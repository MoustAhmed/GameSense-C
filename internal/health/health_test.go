package health

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

type fake struct {
	vals    map[string]float64
	up, tot int
}

func (f fake) Query(_ context.Context, q string) (float64, bool, error) {
	for k, v := range f.vals {
		if strings.Contains(q, k) {
			return v, true, nil
		}
	}
	return 0, false, nil
}
func (f fake) TargetsUp(context.Context) (int, int, error) { return f.up, f.tot, nil }

func byName(rs []Result) map[string]Result {
	m := map[string]Result{}
	for _, r := range rs {
		m[r.Name] = r
	}
	return m
}

func TestHealthyAndUnhealthy(t *testing.T) {
	healthy := fake{up: 14, tot: 14, vals: map[string]float64{
		"audit_lost_events": 0, "audit_acked_events": 1e6,
		"sim_events_generated_total": 20000, `ingest_events_total{result="accepted"}`: 19900,
		"worker_events_processed_total": 19800, `code="200"`: 1,
		"ingest_request_duration_seconds_bucket": 0.02, "worker_e2e_latency_seconds_bucket": 0.3,
		"worker_consumer_lag": 500, "audit_watermark_lag_seconds": 2,
		"audit_flagged_players": 10, "audit_detection_precision": 1, "audit_detection_recall": 0.9,
	}}
	for _, r := range Run(context.Background(), healthy) {
		if r.Status != Pass {
			t.Errorf("healthy system: %s = %s (%s)", r.Name, r.Status, r.Detail)
		}
	}

	sick := healthy
	sick.vals = map[string]float64{}
	for k, v := range healthy.vals {
		sick.vals[k] = v
	}
	sick.vals["audit_lost_events"] = 12
	sick.vals["worker_events_processed_total"] = 5000
	sick.up = 13
	m := byName(Run(context.Background(), sick))
	for _, name := range []string{"Lost events = 0", "Workers keep up with ingest", "Scrape targets up"} {
		if m[name].Status != Fail {
			t.Errorf("%s = %s, want FAIL", name, m[name].Status)
		}
	}
}

func TestPromClientParsesAPI(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v1/query":
			w.Write([]byte(`{"status":"success","data":{"resultType":"vector","result":[{"metric":{},"value":[1700000000.1,"42.5"]}]}}`))
		case "/api/v1/targets":
			w.Write([]byte(`{"status":"success","data":{"activeTargets":[{"health":"up"},{"health":"down"}]}}`))
		}
	}))
	defer srv.Close()
	p := NewProm(srv.URL)
	v, ok, err := p.Query(context.Background(), "up")
	if err != nil || !ok || v != 42.5 {
		t.Fatalf("v=%v ok=%v err=%v", v, ok, err)
	}
	up, tot, err := p.TargetsUp(context.Background())
	if err != nil || up != 1 || tot != 2 {
		t.Fatalf("up=%d tot=%d err=%v", up, tot, err)
	}
}
