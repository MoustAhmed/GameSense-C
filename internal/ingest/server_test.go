package ingest

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"gamesense/internal/broker"
	"gamesense/internal/event"
)

func batch(server string, n int, matches int) []byte {
	b := event.Batch{ServerID: server}
	for i := 0; i < n; i++ {
		b.Events = append(b.Events, event.Event{ServerID: server, Seq: uint64(i + 1), Region: "r",
			MatchID: "m" + string(rune('a'+i%matches)), PlayerID: "p", Type: event.TypeTick})
	}
	out, _ := json.Marshal(b)
	return out
}

func post(h http.Handler, body []byte) *httptest.ResponseRecorder {
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodPost, "/v1/batch", bytes.NewReader(body)))
	return rr
}

func TestAcceptsAndGroupsByMatch(t *testing.T) {
	mem := broker.NewMemory(4)
	p, _ := mem.NewProducer(broker.Config{})
	s := New(DefaultConfig(), p)
	rr := post(s.Handler(), batch("gs1", 30, 3))
	if rr.Code != http.StatusOK {
		t.Fatalf("code %d: %s", rr.Code, rr.Body)
	}
	if n := mem.Len("telemetry.raw"); n != 3 {
		t.Fatalf("records=%d want 3 (one per match)", n)
	}
}

func TestRejectsInvalid(t *testing.T) {
	mem := broker.NewMemory(1)
	p, _ := mem.NewProducer(broker.Config{})
	s := New(DefaultConfig(), p)
	if rr := post(s.Handler(), []byte(`{"server_id":"x","events":[{"seq":0}]}`)); rr.Code != http.StatusBadRequest {
		t.Fatalf("code %d", rr.Code)
	}
}

func TestPerServerRateLimit(t *testing.T) {
	mem := broker.NewMemory(1)
	p, _ := mem.NewProducer(broker.Config{})
	cfg := DefaultConfig()
	cfg.PerServerRate, cfg.PerServerBurst = 1, 50
	s := New(cfg, p)
	h := s.Handler()
	if rr := post(h, batch("noisy", 40, 1)); rr.Code != http.StatusOK {
		t.Fatalf("first batch code %d", rr.Code)
	}
	rr := post(h, batch("noisy", 40, 1))
	if rr.Code != http.StatusTooManyRequests || rr.Header().Get("Retry-After") == "" {
		t.Fatalf("code %d, want 429 with Retry-After", rr.Code)
	}
	if rr := post(h, batch("quiet", 40, 1)); rr.Code != http.StatusOK {
		t.Fatalf("other servers must be unaffected, got %d", rr.Code)
	}
}

// ctxProbeProducer blocks until told to proceed, then reports whether the
// produce context had been cancelled by then.
type ctxProbeProducer struct {
	started, proceed chan struct{}
	ctxErr           chan error
}

func (p *ctxProbeProducer) Produce(ctx context.Context, _ ...*broker.Record) error {
	close(p.started)
	<-p.proceed
	p.ctxErr <- ctx.Err()
	return ctx.Err()
}
func (p *ctxProbeProducer) Close() {}

func TestClientDisconnectDoesNotCancelProduce(t *testing.T) {
	p := &ctxProbeProducer{started: make(chan struct{}), proceed: make(chan struct{}), ctxErr: make(chan error, 1)}
	s := New(DefaultConfig(), p)
	reqCtx, cancelReq := context.WithCancel(context.Background())
	req := httptest.NewRequest(http.MethodPost, "/v1/batch", bytes.NewReader(batch("gs1", 10, 2))).WithContext(reqCtx)
	rr := httptest.NewRecorder()
	done := make(chan struct{})
	go func() { s.Handler().ServeHTTP(rr, req); close(done) }()

	<-p.started // produce is in flight
	cancelReq() // the client goes away
	close(p.proceed)
	if err := <-p.ctxErr; err != nil {
		t.Fatalf("produce context was cancelled by the client disconnect: %v", err)
	}
	<-done
	if rr.Code != http.StatusOK {
		t.Fatalf("code %d, want 200 (the produce succeeded)", rr.Code)
	}
}

type blockingProducer struct{ release chan struct{} }

func (b *blockingProducer) Produce(ctx context.Context, _ ...*broker.Record) error {
	select {
	case <-b.release:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
func (b *blockingProducer) Close() {}

func TestLoadSheddingWhenSaturated(t *testing.T) {
	bp := &blockingProducer{release: make(chan struct{})}
	cfg := DefaultConfig()
	cfg.MaxInFlight = 1
	s := New(cfg, bp)
	h := s.Handler()
	done := make(chan int)
	go func() { done <- post(h, batch("a", 1, 1)).Code }()
	// Wait until the first request holds the only slot.
	for len(s.sem) == 0 {
	}
	if rr := post(h, batch("b", 1, 1)); rr.Code != http.StatusTooManyRequests {
		t.Fatalf("code %d, want 429 while saturated", rr.Code)
	}
	close(bp.release)
	if code := <-done; code != http.StatusOK {
		t.Fatalf("first request code %d", code)
	}
}

type failingProducer struct{}

func (failingProducer) Produce(context.Context, ...*broker.Record) error {
	return errors.New("brokers down")
}
func (failingProducer) Close() {}

func TestBrokerFailureIsRetryable(t *testing.T) {
	s := New(DefaultConfig(), failingProducer{})
	rr := post(s.Handler(), batch("a", 5, 1))
	if rr.Code != http.StatusServiceUnavailable || rr.Header().Get("Retry-After") == "" {
		t.Fatalf("code %d, want 503 + Retry-After so the client retries", rr.Code)
	}
}
