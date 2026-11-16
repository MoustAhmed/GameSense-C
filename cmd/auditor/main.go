// Command auditor independently verifies the pipeline end to end:
//
//   - Zero loss: every event the simulator saw acknowledged must appear in the
//     raw topic. It compares the simulator's acked-through sequence numbers
//     (from /stats) with what it actually read from the log.
//   - Duplicates: counts redelivered or retried events in the raw topic.
//   - Detection quality: precision and recall of the anti-cheat worker,
//     scored against the simulator's deterministic ground truth.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"sort"
	"strings"
	"time"

	"gamesense/internal/audit"
	"gamesense/internal/broker"
	"gamesense/internal/event"
	"gamesense/internal/metrics"
	"gamesense/internal/sim"
	"gamesense/internal/svc"
)

var (
	mSeen      = metrics.NewGauge("audit_events_seen", "Raw events read by the auditor.")
	mDups      = metrics.NewGauge("audit_raw_duplicates", "Duplicate events in the raw topic (expected after retries; workers dedupe).")
	mAcked     = metrics.NewGauge("audit_acked_events", "Events acknowledged by ingest as of the checked snapshot.")
	mLost      = metrics.NewGauge("audit_lost_events", "Acknowledged events missing from the log after the grace period. Must be 0.")
	mPrecision = metrics.NewGauge("audit_detection_precision", "Fraction of flagged players who really cheat.")
	mRecall    = metrics.NewGauge("audit_detection_recall", "Fraction of cheaters seen who were flagged.")
	mFlagged   = metrics.NewGauge("audit_flagged_players", "Unique players flagged.")
	mRedeliv   = metrics.NewGauge("audit_detection_redeliveries", "Detections seen more than once (deduped by ID).")
	mAuditLag  = metrics.NewHistogram("audit_read_latency_seconds", "Event age when the auditor reads it.", metrics.LatencyBuckets)
	mWMLag     = metrics.NewGauge("audit_watermark_lag_seconds", "How far behind the log head the auditor is (slowest partition).")
)

type snapshot struct {
	at      time.Time
	servers map[string]*sim.ServerStats
}

func main() {
	brokers := flag.String("brokers", svc.Env("BROKERS", "localhost:19092"), "comma-separated broker list")
	kind := flag.String("broker", "kafka", "broker implementation")
	rawTopic := flag.String("raw", "telemetry.raw", "raw telemetry topic")
	detTopic := flag.String("detections", "detections", "detections topic")
	simStats := flag.String("sim-stats", svc.Env("SIM_STATS", "http://localhost:9101/stats"), "comma-separated simulator /stats URLs")
	grace := flag.Duration("grace", 30*time.Second, "how long an acked event may take to show up before it counts as lost")
	interval := flag.Duration("interval", 10*time.Second, "check interval")
	seed := flag.Int64("seed", 42, "simulator seed (for ground truth)")
	metricsAddr := flag.String("metrics", ":9103", "metrics listen address")
	flag.Parse()

	log := svc.Logger("auditor")
	ctx, stop := svc.SignalContext()
	defer stop()
	svc.ServeMetrics(ctx, *metricsAddr, nil, log)

	f, err := broker.Get(*kind)
	if err != nil {
		log.Error("broker", "err", err)
		os.Exit(1)
	}
	host, _ := os.Hostname()
	mk := func(group, topic string) broker.Consumer {
		c, err := f.NewConsumer(broker.Config{Brokers: svc.SplitList(*brokers), ClientID: "auditor-" + host,
			Group: group, Topics: []string{topic}, MaxPollRecords: 10000})
		if err != nil {
			log.Error("consumer", "err", err)
			os.Exit(1)
		}
		return c
	}
	raw, det := mk("auditor-raw", *rawTopic), mk("auditor-detections", *detTopic)

	tracker := audit.NewTracker()
	wm := audit.NewWatermark()
	scorer := audit.NewScorer(*seed)

	go consume(ctx, raw, log, func(r *broker.Record) {
		wm.Observe(r.Partition, r.Timestamp)
		var evs []event.Event
		if json.Unmarshal(r.Value, &evs) != nil {
			return
		}
		for i := range evs {
			tracker.Observe(evs[i].ServerID, evs[i].Seq)
			scorer.ObservePlayer(evs[i].PlayerID)
		}
		if n := len(evs); n > 0 {
			mAuditLag.With().Observe(time.Since(time.UnixMilli(evs[n-1].TS)).Seconds())
		}
	})
	go consume(ctx, det, log, func(r *broker.Record) {
		var d event.Detection
		if json.Unmarshal(r.Value, &d) == nil {
			scorer.ObserveDetection(d.ID, d.PlayerID)
		}
	})

	urls := svc.SplitList(*simStats)
	client := &http.Client{Timeout: 5 * time.Second}
	var queue []snapshot
	tick := time.NewTicker(*interval)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			raw.Close()
			det.Close()
			return
		case <-tick.C:
		}
		if s, ok := fetch(client, urls, log); ok {
			queue = append(queue, s)
		}
		// Check the newest snapshot that is (a) at least `grace` old and (b)
		// behind the auditor's read watermark on every partition. (b) matters at
		// high volume: if the auditor itself lags, unread events must not be
		// mistaken for lost ones. The 5s slack covers records from different
		// ingest replicas landing slightly out of timestamp order.
		low := wm.Low()
		if !low.IsZero() {
			mWMLag.With().Set(time.Since(low).Seconds())
		}
		var check *snapshot
		for len(queue) > 0 && time.Since(queue[0].at) >= *grace && low.After(queue[0].at.Add(5*time.Second)) {
			check = &queue[0]
			queue = queue[1:]
		}
		if len(queue) > 360 { // auditor hopelessly behind: keep memory bounded, log it
			log.Warn("auditor lagging; dropping old snapshots", "watermark_lag", time.Since(low).String())
			queue = queue[len(queue)-360:]
		}
		seen, dups := tracker.Totals()
		mSeen.With().Set(float64(seen))
		mDups.With().Set(float64(dups))
		sc := scorer.Score()
		mPrecision.With().Set(sc.Precision)
		mRecall.With().Set(sc.Recall)
		mFlagged.With().Set(float64(sc.Flagged))
		mRedeliv.With().Set(float64(sc.Redeliveries))
		if check == nil {
			continue
		}
		var acked, lost, rejected uint64
		for id, st := range check.servers {
			acked += st.AckedThrough
			rejected += st.Rejected
			lost += tracker.Missing(id, st.AckedThrough)
		}
		if lost >= rejected { // permanently rejected (malformed) batches were never accepted
			lost -= rejected
		}
		mAcked.With().Set(float64(acked))
		mLost.With().Set(float64(lost))
		lvl := slog.LevelInfo
		if lost > 0 {
			lvl = slog.LevelError
		}
		log.Log(ctx, lvl, "audit", "acked", acked, "seen", seen, "lost", lost, "raw_duplicates", dups,
			"flagged", sc.Flagged, "precision", round(sc.Precision), "recall", round(sc.Recall))
		if lost > 0 {
			logMissing(ctx, log, tracker, check)
		}
	}
}

// logMissing logs, at ERROR, the servers with missing events (up to 20, most
// missing first), each with its missing count and up to 10 missing seq ranges.
func logMissing(ctx context.Context, log *slog.Logger, tracker *audit.Tracker, check *snapshot) {
	type gap struct {
		id      string
		acked   uint64
		missing uint64
	}
	var gaps []gap
	for id, st := range check.servers {
		if m := tracker.Missing(id, st.AckedThrough); m > 0 {
			gaps = append(gaps, gap{id, st.AckedThrough, m})
		}
	}
	sort.Slice(gaps, func(i, j int) bool {
		if gaps[i].missing != gaps[j].missing {
			return gaps[i].missing > gaps[j].missing
		}
		return gaps[i].id < gaps[j].id
	})
	total := len(gaps)
	if len(gaps) > 20 {
		gaps = gaps[:20]
	}
	for _, g := range gaps {
		var rs []string
		for _, r := range tracker.MissingRanges(g.id, g.acked, 10) {
			rs = append(rs, fmt.Sprintf("%d-%d", r.From, r.To))
		}
		log.Log(ctx, slog.LevelError, "audit missing", "server", g.id, "missing", g.missing,
			"acked_through", g.acked, "ranges", strings.Join(rs, ","), "servers_with_gaps", total)
	}
}

func consume(ctx context.Context, c broker.Consumer, log *slog.Logger, fn func(*broker.Record)) {
	for {
		recs, err := c.Poll(ctx)
		if ctx.Err() != nil || err == broker.ErrClosed {
			return
		}
		if err != nil {
			log.Warn("poll", "err", err)
		}
		for _, r := range recs {
			fn(r)
		}
		if len(recs) > 0 {
			c.Commit(ctx)
		}
	}
}

func fetch(client *http.Client, urls []string, log *slog.Logger) (snapshot, bool) {
	s := snapshot{at: time.Now(), servers: map[string]*sim.ServerStats{}}
	for _, u := range urls {
		resp, err := client.Get(u)
		if err != nil {
			log.Warn("sim stats unavailable", "url", u, "err", err)
			return s, false
		}
		var snap sim.Snapshot
		err = json.NewDecoder(resp.Body).Decode(&snap)
		resp.Body.Close()
		if err != nil {
			return s, false
		}
		for k, v := range snap.Servers {
			s.servers[k] = v
		}
	}
	return s, true
}

func round(f float64) float64 { return float64(int(f*1000+0.5)) / 1000 }
