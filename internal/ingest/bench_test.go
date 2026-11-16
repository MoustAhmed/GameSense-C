package ingest

import (
	"bytes"
	"compress/gzip"
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"gamesense/internal/broker"
)

type discardProducer struct{}

func (discardProducer) Produce(context.Context, ...*broker.Record) error { return nil }
func (discardProducer) Close()                                           {}

// BenchmarkIngestBatch measures the CPU cost of ingest itself (decode,
// validate, rate-limit, regroup, re-encode) with the broker stubbed out.
// Use it to see what serialization costs, e.g. before/after switching to protobuf.
func BenchmarkIngestBatch(b *testing.B) {
	cfg := DefaultConfig()
	cfg.PerServerRate, cfg.PerServerBurst = 1e12, 1e12
	h := New(cfg, discardProducer{}).Handler()
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	gz.Write(batch("gs1", 500, 10))
	gz.Close()
	body := buf.Bytes()
	b.SetBytes(int64(len(body)))
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			rr := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodPost, "/v1/batch", bytes.NewReader(body))
			req.Header.Set("Content-Encoding", "gzip")
			h.ServeHTTP(rr, req)
			if rr.Code != http.StatusOK {
				b.Fatalf("code %d", rr.Code)
			}
		}
	})
	b.ReportMetric(float64(b.N*500)/b.Elapsed().Seconds(), "events/s")
}
