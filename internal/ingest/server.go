// Package ingest is the stateless HTTP front door. It validates batches,
// applies backpressure and per-server rate limits, and acknowledges a batch
// only after the broker has durably replicated it.
package ingest

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"sync/atomic"
	"time"

	"gamesense/internal/broker"
	"gamesense/internal/event"
	"gamesense/internal/metrics"
	"gamesense/internal/ratelimit"
	"gamesense/internal/svc"
)

var (
	mRequests = metrics.NewCounter("ingest_requests_total", "Batch requests by HTTP status code.", "code")
	mEvents   = metrics.NewCounter("ingest_events_total", "Events by result.", "result")
	mRejected = metrics.NewCounter("ingest_rejected_total", "Rejected batches by reason.", "reason")
	mDuration = metrics.NewHistogram("ingest_request_duration_seconds", "End-to-end batch request latency.", metrics.LatencyBuckets)
	mProduce  = metrics.NewHistogram("ingest_produce_duration_seconds", "Time waiting for broker acks.", metrics.LatencyBuckets)
	mInFlight = metrics.NewGauge("ingest_inflight_requests", "Batches currently being processed.")
	mBatchSz  = metrics.NewHistogram("ingest_batch_events", "Events per accepted batch.", []float64{1, 10, 50, 100, 250, 500, 1000, 2500, 5000})
)

// Config tunes the ingest server.
type Config struct {
	Topic             string
	MaxBodyBytes      int64
	MaxEventsPerBatch int
	MaxInFlight       int     // concurrent batches before shedding load with 429
	PerServerRate     float64 // events/sec allowed per game server
	PerServerBurst    float64
	ProduceTimeout    time.Duration
}

// DefaultConfig returns production-ish defaults.
func DefaultConfig() Config {
	return Config{
		// A 500-event gzipped batch is ~25 KB, so 1 MB leaves lots of headroom
		// while bounding worst-case memory: MaxInFlight x 4 x MaxBodyBytes.
		Topic: "telemetry.raw", MaxBodyBytes: 1 << 20, MaxEventsPerBatch: 5000,
		MaxInFlight: 256, PerServerRate: 20000, PerServerBurst: 40000, ProduceTimeout: 5 * time.Second,
	}
}

// Server handles batch ingestion.
type Server struct {
	cfg    Config
	prod   broker.Producer
	sem    chan struct{}
	limits *ratelimit.Keyed
	ready  atomic.Bool
}

// New creates a server. Call SetReady(true) once dependencies are up.
func New(cfg Config, prod broker.Producer) *Server {
	s := &Server{cfg: cfg, prod: prod, sem: make(chan struct{}, cfg.MaxInFlight),
		limits: ratelimit.NewKeyed(cfg.PerServerRate, cfg.PerServerBurst)}
	s.ready.Store(true)
	return s
}

// SetReady toggles the readiness probe. During shutdown we fail readiness first
// so the load balancer stops routing to us before we stop accepting.
func (s *Server) SetReady(v bool) { s.ready.Store(v) }

// Handler returns the HTTP routes.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/batch", s.handleBatch)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { w.Write([]byte("ok")) })
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, _ *http.Request) {
		if !s.ready.Load() {
			http.Error(w, "draining", http.StatusServiceUnavailable)
			return
		}
		w.Write([]byte("ready"))
	})
	mux.Handle("GET /metrics", metrics.Default.Handler())
	svc.MountPprof(mux)
	return mux
}

func (s *Server) handleBatch(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	code := s.process(w, r)
	mRequests.With(strconv.Itoa(code)).Inc()
	mDuration.With().Observe(time.Since(start).Seconds())
}

func (s *Server) process(w http.ResponseWriter, r *http.Request) int {
	// 1. Load shedding: never queue unbounded work. If we're full, tell the
	//    client to back off instead of slowly timing everyone out.
	select {
	case s.sem <- struct{}{}:
		mInFlight.With().Add(1)
		defer func() { <-s.sem; mInFlight.With().Add(-1) }()
	default:
		return reject(w, http.StatusTooManyRequests, "overloaded", 1)
	}

	// 2. Decode with size limits.
	var body io.Reader = http.MaxBytesReader(w, r.Body, s.cfg.MaxBodyBytes)
	if r.Header.Get("Content-Encoding") == "gzip" {
		gz, err := gzip.NewReader(body)
		if err != nil {
			return reject(w, http.StatusBadRequest, "bad_gzip", 0)
		}
		defer gz.Close()
		body = io.LimitReader(gz, 4*s.cfg.MaxBodyBytes) // bound decompression bombs
	}
	var b event.Batch
	if err := json.NewDecoder(body).Decode(&b); err != nil {
		var mbe *http.MaxBytesError
		if errors.As(err, &mbe) {
			return reject(w, http.StatusRequestEntityTooLarge, "too_large", 0)
		}
		return reject(w, http.StatusBadRequest, "bad_json", 0)
	}
	if err := b.Validate(s.cfg.MaxEventsPerBatch); err != nil {
		return reject(w, http.StatusBadRequest, "invalid", 0)
	}

	// 3. Per-server rate limit so one misbehaving server can't starve others.
	if !s.limits.AllowN(b.ServerID, float64(len(b.Events))) {
		mEvents.With("rate_limited").Add(len(b.Events))
		return reject(w, http.StatusTooManyRequests, "rate_limited", 1)
	}

	// 4. Group by match: the match ID is the partition key, so every event of a
	//    match lands on one partition, in order, and a single worker owns that
	//    match's state. One record per match also cuts per-record overhead.
	recs := toRecords(s.cfg.Topic, b.Events)

	// 5. Wait for acks=all before answering. If this fails the client retries;
	//    any duplicates are harmless because (server_id, seq) is an idempotency key.
	//    The produce gets its own context, not r.Context(): if the client (or
	//    nginx, e.g. on a restart) disconnects mid-request, r.Context() is
	//    cancelled, and cancelling records that may already be in flight on the
	//    idempotent producer can force sequence/epoch resets for whole
	//    partitions. The batch then completes or fails on its own; the client
	//    simply retries if it never saw the answer. ProduceTimeout still bounds it.
	ctx, cancel := context.WithTimeout(context.Background(), s.cfg.ProduceTimeout)
	defer cancel()
	pstart := time.Now()
	err := s.prod.Produce(ctx, recs...)
	mProduce.With().Observe(time.Since(pstart).Seconds())
	if err != nil {
		mEvents.With("produce_failed").Add(len(b.Events))
		return reject(w, http.StatusServiceUnavailable, "broker_unavailable", 1)
	}

	mEvents.With("accepted").Add(len(b.Events))
	mBatchSz.With().Observe(float64(len(b.Events)))
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(`{"accepted":` + strconv.Itoa(len(b.Events)) + "}"))
	return http.StatusOK
}

func toRecords(topic string, events []event.Event) []*broker.Record {
	order := []string{}
	groups := map[string][]event.Event{}
	for _, e := range events {
		if _, ok := groups[e.MatchID]; !ok {
			order = append(order, e.MatchID)
		}
		groups[e.MatchID] = append(groups[e.MatchID], e)
	}
	recs := make([]*broker.Record, 0, len(order))
	for _, id := range order {
		v, _ := json.Marshal(groups[id]) // marshalling plain structs cannot fail
		recs = append(recs, &broker.Record{Topic: topic, Key: []byte(id), Value: v})
	}
	return recs
}

func reject(w http.ResponseWriter, code int, reason string, retryAfter int) int {
	mRejected.With(reason).Inc()
	if retryAfter > 0 {
		w.Header().Set("Retry-After", strconv.Itoa(retryAfter))
	}
	http.Error(w, reason, code)
	return code
}
