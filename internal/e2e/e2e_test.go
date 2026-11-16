// Package e2e runs the whole pipeline in one process against the in-memory
// broker: simulator -> HTTP ingest -> log -> detector worker -> detections,
// with the auditor checking zero loss. It also crashes a worker mid-stream.
package e2e

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http/httptest"
	"testing"
	"time"

	"gamesense/internal/audit"
	"gamesense/internal/broker"
	"gamesense/internal/detect"
	"gamesense/internal/event"
	"gamesense/internal/ingest"
	"gamesense/internal/jobs"
	"gamesense/internal/sim"
	"gamesense/internal/worker"
)

const seed = 42

func TestPipelineZeroLossWithWorkerCrash(t *testing.T) {
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	mem := broker.NewMemory(8)
	prod, _ := mem.NewProducer(broker.Config{})
	ts := httptest.NewServer(ingest.New(ingest.DefaultConfig(), prod).Handler())
	defer ts.Close()

	// 1. Simulate 4 game servers for 60 wall-seconds of gameplay (stepped fast).
	sender := sim.NewSender(ts.URL+"/v1/batch", true)
	start := time.Date(2026, 11, 16, 20, 0, 0, 0, time.UTC)
	acked := map[string]uint64{}
	now := time.Now()
	ctx := context.Background()
	servers := []*sim.GameServer{}
	for i := 0; i < 4; i++ {
		servers = append(servers, sim.NewGameServer("gs"+string(rune('0'+i)), "eu-west", 20000, seed, 5, time.Hour))
	}
	for step := 0; step < 600; step++ {
		now = now.Add(100 * time.Millisecond)
		for _, gs := range servers {
			evs := gs.Step(now, start, 0.1, 10, nil)
			if len(evs) == 0 {
				continue
			}
			if rejected, err := sender.Send(ctx, gs.ID, gs.Region, evs); err != nil || rejected {
				t.Fatalf("send: rejected=%v err=%v", rejected, err)
			}
			acked[gs.ID] = evs[len(evs)-1].Seq
		}
	}

	// 2. Detector worker #1 processes part of the log, commits, then "crashes"
	//    in the middle of a batch (context cancelled before commit).
	runUntil := func(stop func() bool) {
		c, _ := mem.NewConsumer(broker.Config{Group: "detector", Topics: []string{"telemetry.raw"}, MaxPollRecords: 200})
		wctx, cancel := context.WithCancel(context.Background())
		done := make(chan struct{})
		go func() {
			_ = worker.Run(wctx, "detector", c, prod, jobs.Detector(detect.New(detect.DefaultRules()), "detections", "telemetry.dlq"), log)
			close(done)
		}()
		deadline := time.Now().Add(2 * time.Minute)
		for !stop() && time.Now().Before(deadline) {
			time.Sleep(5 * time.Millisecond)
		}
		cancel()
		<-done
		c.Close() // anything polled but uncommitted is redelivered
	}
	total := mem.Len("telemetry.raw")
	runUntil(func() bool { return mem.Pending("detector", "telemetry.raw") < total*2/3 })
	if p := mem.Pending("detector", "telemetry.raw"); p == 0 || p == total {
		t.Fatalf("crash should happen mid-stream, pending=%d of %d", p, total)
	}
	// 3. A fresh worker (new process, empty memory) takes over and finishes.
	runUntil(func() bool { return mem.Pending("detector", "telemetry.raw") == 0 })
	if p := mem.Pending("detector", "telemetry.raw"); p != 0 {
		t.Fatalf("worker did not drain the log: %d pending", p)
	}

	// 4. Audit the raw log: every acked event must be there.
	tracker := audit.NewTracker()
	scorer := audit.NewScorer(seed)
	rc, _ := mem.NewConsumer(broker.Config{Group: "audit", Topics: []string{"telemetry.raw"}, MaxPollRecords: 1 << 20})
	pctx, pcancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer pcancel()
	recs, _ := rc.Poll(pctx)
	for _, r := range recs {
		var evs []event.Event
		if err := json.Unmarshal(r.Value, &evs); err != nil {
			t.Fatal(err)
		}
		for _, e := range evs {
			tracker.Observe(e.ServerID, e.Seq)
			scorer.ObservePlayer(e.PlayerID)
		}
	}
	var ackedTotal uint64
	for id, through := range acked {
		ackedTotal += through
		if m := tracker.Missing(id, through); m != 0 {
			t.Errorf("server %s: %d acked events missing", id, m)
		}
	}
	if _, dups := tracker.Totals(); dups != 0 {
		t.Errorf("raw duplicates = %d, want 0 (no retries happened)", dups)
	}

	// 5. Detections: redelivery after the crash may re-emit some, but IDs are
	//    deterministic so deduping by ID yields a clean set with no false positives.
	dc, _ := mem.NewConsumer(broker.Config{Group: "audit", Topics: []string{"detections"}, MaxPollRecords: 1 << 20})
	drecs, _ := dc.Poll(pctx)
	for _, r := range drecs {
		var d event.Detection
		json.Unmarshal(r.Value, &d)
		scorer.ObserveDetection(d.ID, d.PlayerID)
	}
	sc := scorer.Score()
	t.Logf("acked=%d events, records=%d, detections=%d (unique players %d, redeliveries %d), precision=%.2f recall=%.2f",
		ackedTotal, len(recs), len(drecs), sc.Flagged, sc.Redeliveries, sc.Precision, sc.Recall)
	if sc.Flagged == 0 || sc.Precision < 1 {
		t.Errorf("want detections with precision 1.0, got %+v", sc)
	}
}
