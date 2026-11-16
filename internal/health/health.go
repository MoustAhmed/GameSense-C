// Package health evaluates the running system against the checklist in
// docs/PLAN.md by querying Prometheus and the simulators.
package health

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"time"
)

// Status of one check.
type Status string

const (
	Pass Status = "PASS"
	Fail Status = "FAIL"
	Wait Status = "WAIT" // not enough data yet
)

// Result is one evaluated check.
type Result struct {
	Name   string
	Status Status
	Detail string
}

// Querier runs an instant PromQL query and returns the first sample, or
// ok=false when the query returned no data.
type Querier interface {
	Query(ctx context.Context, promql string) (v float64, ok bool, err error)
	TargetsUp(ctx context.Context) (up, total int, err error)
}

// Run evaluates every check.
func Run(ctx context.Context, q Querier) []Result {
	var out []Result
	add := func(name string, st Status, f string, a ...any) {
		out = append(out, Result{Name: name, Status: st, Detail: fmt.Sprintf(f, a...)})
	}
	get := func(expr string) (float64, bool) {
		v, ok, err := q.Query(ctx, expr)
		if err != nil {
			return 0, false
		}
		return v, ok
	}

	if up, total, err := q.TargetsUp(ctx); err != nil {
		add("Prometheus reachable", Fail, "%v (is the stack running? try: make up)", err)
		return out
	} else if total == 0 {
		add("Scrape targets up", Wait, "no targets discovered yet")
	} else if up < total {
		add("Scrape targets up", Fail, "%d/%d up (see http://localhost:9090/targets)", up, total)
	} else {
		add("Scrape targets up", Pass, "%d/%d up", up, total)
	}

	if v, ok := get(`max(audit_lost_events)`); !ok {
		add("Lost events = 0", Wait, "auditor hasn't completed a check yet (~40s after start)")
	} else if v > 0 {
		add("Lost events = 0", Fail, "%.0f acknowledged events missing", v)
	} else {
		acked, _ := get(`max(audit_acked_events)`)
		add("Lost events = 0", Pass, "0 lost of %.0f acked", acked)
	}

	gen, ok1 := get(`sum(rate(sim_events_generated_total[2m]))`)
	ing, ok2 := get(`sum(rate(ingest_events_total{result="accepted"}[2m]))`)
	proc, ok3 := get(`sum(rate(worker_events_processed_total[2m]))`)
	switch {
	case !ok1 || !ok2 || gen == 0:
		add("Ingest keeps up with load", Wait, "no traffic yet")
	case ing < gen*0.9:
		add("Ingest keeps up with load", Fail, "generated %.0f/s but ingested %.0f/s (backpressure?)", gen, ing)
	default:
		add("Ingest keeps up with load", Pass, "generated %.0f/s, ingested %.0f/s", gen, ing)
	}
	switch {
	case !ok2 || !ok3 || ing == 0:
		add("Workers keep up with ingest", Wait, "no traffic yet")
	case proc < ing*0.9:
		add("Workers keep up with ingest", Fail, "ingested %.0f/s but processed %.0f/s (lag growing?)", ing, proc)
	default:
		add("Workers keep up with ingest", Pass, "processed %.0f/s", proc)
	}

	if v, ok := get(`sum(rate(ingest_requests_total{code="200"}[5m])) / sum(rate(ingest_requests_total{code!~"4.."}[5m]))`); !ok {
		add("Ingest availability >= 99.9%", Wait, "no requests yet")
	} else if v < 0.999 {
		add("Ingest availability >= 99.9%", Fail, "%.3f%%", v*100)
	} else {
		add("Ingest availability >= 99.9%", Pass, "%.3f%%", v*100)
	}

	latency := func(name, expr string, limit float64) {
		if v, ok := get(expr); !ok || v != v { // NaN when there's no traffic
			add(name, Wait, "no samples yet")
		} else if v > limit {
			add(name, Fail, "%.0f ms (limit %.0f ms)", v*1000, limit*1000)
		} else {
			add(name, Pass, "%.0f ms", v*1000)
		}
	}
	latency("Ingest p99 < 100 ms", `histogram_quantile(0.99, sum by (le) (rate(ingest_request_duration_seconds_bucket[2m])))`, 0.1)
	latency("End-to-end p99 < 1 s", `histogram_quantile(0.99, sum by (le) (rate(worker_e2e_latency_seconds_bucket[2m])))`, 1)

	if v, ok := get(`sum(worker_consumer_lag)`); !ok {
		add("Consumer lag < 100k", Wait, "no data yet")
	} else if v > 100000 {
		add("Consumer lag < 100k", Fail, "%.0f records behind", v)
	} else {
		add("Consumer lag < 100k", Pass, "%.0f records behind", v)
	}

	if v, ok := get(`max(audit_watermark_lag_seconds)`); ok && v > 60 {
		add("Auditor keeping up", Fail, "%.0fs behind; loss checks are delayed", v)
	} else if ok {
		add("Auditor keeping up", Pass, "%.1fs behind", v)
	}

	if v, ok := get(`sum(rate(worker_dlq_total[10m]))`); ok && v > 0 {
		add("Dead-letter queue empty", Fail, "%.2f records/s going to DLQ", v)
	} else {
		add("Dead-letter queue empty", Pass, "no poison records in the last 10m")
	}

	flagged, okF := get(`max(audit_flagged_players)`)
	prec, okP := get(`max(audit_detection_precision)`)
	if !okF || !okP || flagged == 0 {
		add("Detector precision >= 95%", Wait, "no detections yet (cheaters are ~0.7%% of players)")
	} else if prec < 0.95 {
		add("Detector precision >= 95%", Fail, "%.1f%% over %.0f flagged players", prec*100, flagged)
	} else {
		rec, _ := get(`max(audit_detection_recall)`)
		add("Detector precision >= 95%", Pass, "precision %.1f%%, recall %.1f%%, %.0f flagged", prec*100, rec*100, flagged)
	}
	return out
}

// Prom queries a Prometheus server over its HTTP API.
type Prom struct {
	URL    string
	Client *http.Client
}

// NewProm returns a client with a short timeout.
func NewProm(u string) *Prom { return &Prom{URL: u, Client: &http.Client{Timeout: 5 * time.Second}} }

func (p *Prom) get(ctx context.Context, path string, out any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, p.URL+path, nil)
	if err != nil {
		return err
	}
	resp, err := p.Client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("%s: HTTP %d", path, resp.StatusCode)
	}
	return json.NewDecoder(resp.Body).Decode(out)
}

// Query implements Querier.
func (p *Prom) Query(ctx context.Context, promql string) (float64, bool, error) {
	var r struct {
		Data struct {
			Result []struct {
				Value [2]any `json:"value"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := p.get(ctx, "/api/v1/query?query="+url.QueryEscape(promql), &r); err != nil {
		return 0, false, err
	}
	if len(r.Data.Result) == 0 {
		return 0, false, nil
	}
	s, _ := r.Data.Result[0].Value[1].(string)
	v, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return 0, false, err
	}
	return v, true, nil
}

// TargetsUp implements Querier.
func (p *Prom) TargetsUp(ctx context.Context) (int, int, error) {
	var r struct {
		Data struct {
			ActiveTargets []struct {
				Health string `json:"health"`
			} `json:"activeTargets"`
		} `json:"data"`
	}
	if err := p.get(ctx, "/api/v1/targets?state=active", &r); err != nil {
		return 0, 0, err
	}
	up := 0
	for _, t := range r.Data.ActiveTargets {
		if t.Health == "up" {
			up++
		}
	}
	return up, len(r.Data.ActiveTargets), nil
}
