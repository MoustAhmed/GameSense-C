// Command loadstat prints live pipeline statistics in your terminal by reading
// the services' /metrics endpoints, with no Prometheus or Grafana needed.
//
//	go run ./cmd/loadstat
//	go run ./cmd/loadstat -ingest http://localhost:8080/metrics -sim http://localhost:9101/metrics
//
// Press Ctrl-C for a summary of the run.
package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"math"
	"net/http"
	"os"
	"os/signal"
	"sort"
	"strconv"
	"strings"
	"time"
)

type sample struct {
	at time.Time
	m  map[string]float64 // full series ("name{labels}") -> value
}

func main() {
	ingestURL := flag.String("ingest", "http://localhost:8080/metrics", "ingest metrics URL")
	simURLs := flag.String("sim", "http://localhost:9101/metrics", "comma-separated simulator metrics URLs")
	every := flag.Duration("every", 2*time.Second, "refresh interval")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	client := &http.Client{Timeout: 3 * time.Second}

	scrape := func() (sample, error) {
		s := sample{at: time.Now(), m: map[string]float64{}}
		if err := fetch(client, *ingestURL, "ingest:", s.m); err != nil {
			return s, err
		}
		for _, u := range strings.Split(*simURLs, ",") {
			if u = strings.TrimSpace(u); u != "" {
				if err := fetch(client, u, "sim:", s.m); err != nil {
					return s, err
				}
			}
		}
		return s, nil
	}

	first, err := scrape()
	for err != nil {
		fmt.Fprintf(os.Stderr, "waiting for services (%v)\n", err)
		select {
		case <-ctx.Done():
			return
		case <-time.After(2 * time.Second):
		}
		first, err = scrape()
	}

	prev := first
	var peakEv, peakReq float64
	var maxHeap, maxCPU float64
	rows := 0
	tick := time.NewTicker(*every)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			summary(first, prev, peakEv, peakReq, maxCPU, maxHeap)
			return
		case <-tick.C:
		}
		cur, err := scrape()
		if err != nil {
			fmt.Fprintf(os.Stderr, "scrape failed: %v\n", err)
			continue
		}
		dt := cur.at.Sub(prev.at).Seconds()
		rate := func(f func(map[string]float64) float64) float64 { return (f(cur.m) - f(prev.m)) / dt }

		gen := rate(sum("sim:sim_events_generated_total"))
		acked := rate(sum("sim:sim_events_acked_total"))
		ingested := rate(one(`ingest:ingest_events_total{result="accepted"}`))
		reqs := rate(sum("ingest:ingest_requests_total"))
		rejected := rate(sum("ingest:ingest_rejected_total"))
		retries := rate(sum("sim:sim_send_retries_total"))
		p50 := quantile(prev.m, cur.m, "ingest:ingest_request_duration_seconds", 0.50)
		p99 := quantile(prev.m, cur.m, "ingest:ingest_request_duration_seconds", 0.99)
		ingCPU, simCPU := math.NaN(), math.NaN()
		if _, ok := cur.m["ingest:process_cpu_seconds_total"]; ok {
			ingCPU = rate(one("ingest:process_cpu_seconds_total")) * 100
		}
		if hasPrefix(cur.m, "sim:process_cpu_seconds_total") {
			simCPU = rate(sum("sim:process_cpu_seconds_total")) * 100
		}
		heap := cur.m["ingest:go_memstats_heap_alloc_bytes"] / (1 << 20)
		upMbps := rate(sum("sim:sim_bytes_sent_total")) * 8 / 1e6
		players := sum("sim:sim_players")(cur.m)
		pending := sum("sim:sim_pending_events")(cur.m)
		simTime := time.Unix(int64(cur.m["sim:sim_time_seconds"]), 0).UTC()

		peakEv = math.Max(peakEv, ingested)
		peakReq = math.Max(peakReq, reqs)
		if !math.IsNaN(ingCPU) {
			maxCPU = math.Max(maxCPU, ingCPU)
		}
		maxHeap = math.Max(maxHeap, heap)

		if rows%15 == 0 {
			fmt.Printf("\n%-15s %7s %9s %9s %9s %7s %7s %7s %8s %8s %8s %8s %7s %8s %7s\n",
				"sim time (UTC)", "players", "gen ev/s", "ingest/s", "acked/s", "req/s", "rej/s", "retry/s",
				"p50", "p99", "ing CPU", "ing heap", "sim CPU", "pending", "up Mbps")
		}
		rows++
		fmt.Printf("%-15s %7.0f %9.0f %9.0f %9.0f %7.0f %7.1f %7.1f %8s %8s %8s %6.0fMB %7s %8.0f %7.1f\n",
			simTime.Format("Mon 15:04"), players, gen, ingested, acked, reqs, rejected, retries,
			ms(p50), ms(p99), pct(ingCPU), heap, pct(simCPU), pending, upMbps)
		prev = cur
	}
}

func summary(first, last sample, peakEv, peakReq, maxCPU, maxHeap float64) {
	dt := last.at.Sub(first.at).Seconds()
	accepted := one(`ingest:ingest_events_total{result="accepted"}`)(last.m) - one(`ingest:ingest_events_total{result="accepted"}`)(first.m)
	reqs := sum("ingest:ingest_requests_total")(last.m) - sum("ingest:ingest_requests_total")(first.m)
	rejected := sum("ingest:ingest_rejected_total")(last.m) - sum("ingest:ingest_rejected_total")(first.m)
	fmt.Printf("\n── run summary (%.0fs) ──────────────────────────────\n", dt)
	fmt.Printf("events accepted:   %.0f  (avg %.0f/s, peak %.0f/s)\n", accepted, accepted/dt, peakEv)
	fmt.Printf("requests:          %.0f  (avg %.0f/s, peak %.0f/s)\n", reqs, reqs/dt, peakReq)
	fmt.Printf("rejected (429/503): %.0f\n", rejected)
	fmt.Printf("events per request: %.0f\n", accepted/math.Max(reqs, 1))
	if maxCPU > 0 {
		fmt.Printf("ingest CPU peak:   %.0f%% (100%% = one core)\n", maxCPU)
	} else {
		fmt.Printf("ingest CPU peak:   n/a (CPU metric not reported)\n")
	}
	fmt.Printf("ingest heap peak:  %.0f MB\n", maxHeap)
}

// fetch parses a Prometheus text-format page into m, prefixing every series.
func fetch(c *http.Client, url, prefix string, m map[string]float64) error {
	resp, err := c.Get(url)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	sc := bufio.NewScanner(resp.Body)
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	for sc.Scan() {
		line := sc.Text()
		if line == "" || line[0] == '#' {
			continue
		}
		i := strings.LastIndexByte(line, ' ')
		if i < 0 {
			continue
		}
		v, err := strconv.ParseFloat(line[i+1:], 64)
		if err != nil {
			continue
		}
		m[prefix+line[:i]] += v // += merges identical series from several simulators
	}
	return sc.Err()
}

func one(series string) func(map[string]float64) float64 {
	return func(m map[string]float64) float64 { return m[series] }
}

// sum adds every series of a metric name, across all label values.
func sum(name string) func(map[string]float64) float64 {
	return func(m map[string]float64) float64 {
		t := 0.0
		for k, v := range m {
			if k == name || strings.HasPrefix(k, name+"{") {
				t += v
			}
		}
		return t
	}
}

// quantile estimates a quantile from the change in histogram buckets between
// two scrapes, interpolating within a bucket the way Prometheus does.
func quantile(prev, cur map[string]float64, hist string, q float64) float64 {
	type b struct{ le, n float64 }
	var bs []b
	pfx := hist + `_bucket{le="`
	for k, v := range cur {
		if !strings.HasPrefix(k, pfx) {
			continue
		}
		leS := strings.TrimSuffix(strings.TrimPrefix(k, pfx), `"}`)
		le := math.Inf(1)
		if leS != "+Inf" {
			le, _ = strconv.ParseFloat(leS, 64)
		}
		bs = append(bs, b{le, v - prev[k]})
	}
	if len(bs) == 0 {
		return math.NaN()
	}
	sort.Slice(bs, func(i, j int) bool { return bs[i].le < bs[j].le })
	total := bs[len(bs)-1].n
	if total <= 0 {
		return math.NaN()
	}
	rank := q * total
	lower, prevN := 0.0, 0.0
	for _, x := range bs {
		if x.n >= rank {
			if math.IsInf(x.le, 1) {
				return lower
			}
			if x.n == prevN {
				return x.le
			}
			return lower + (x.le-lower)*(rank-prevN)/(x.n-prevN)
		}
		lower, prevN = x.le, x.n
	}
	return lower
}

func pct(v float64) string {
	if math.IsNaN(v) {
		return "n/a"
	}
	return fmt.Sprintf("%.0f%%", v)
}

func hasPrefix(m map[string]float64, name string) bool {
	for k := range m {
		if k == name || strings.HasPrefix(k, name+"{") {
			return true
		}
	}
	return false
}

func ms(sec float64) string {
	if math.IsNaN(sec) {
		return "-"
	}
	return fmt.Sprintf("%.1fms", sec*1000)
}
