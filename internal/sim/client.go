package sim

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"math/rand"
	"net/http"
	"strconv"
	"time"

	"gamesense/internal/event"
	"gamesense/internal/metrics"
)

var (
	mBatches   = metrics.NewCounter("sim_batches_total", "Batches sent by result.", "result")
	mRetries   = metrics.NewCounter("sim_send_retries_total", "Batch send retries by reason.", "reason")
	mAcked     = metrics.NewCounter("sim_events_acked_total", "Events acknowledged by ingest.", "region")
	mSendDur   = metrics.NewHistogram("sim_send_duration_seconds", "Time from first attempt to ack, including retries.", metrics.LatencyBuckets)
	mBytesSent = metrics.NewCounter("sim_bytes_sent_total", "Request body bytes sent (after compression).")
)

// Sender delivers batches to the ingest API with retries, exponential backoff
// with full jitter, and respect for Retry-After. It never gives up on a batch
// (that would be silent data loss); it only stops when ctx is cancelled.
type Sender struct {
	URL    string
	Gzip   bool
	Client *http.Client
	Log    *slog.Logger

	BaseBackoff time.Duration
	MaxBackoff  time.Duration
}

// NewSender returns a sender tuned for many concurrent game servers.
func NewSender(url string, gz bool) *Sender {
	return &Sender{
		URL: url, Gzip: gz, Log: slog.Default(),
		Client: &http.Client{
			Timeout:   10 * time.Second,
			Transport: &http.Transport{MaxIdleConns: 512, MaxIdleConnsPerHost: 512, IdleConnTimeout: 90 * time.Second},
		},
		BaseBackoff: 100 * time.Millisecond, MaxBackoff: 5 * time.Second,
	}
}

// Send blocks until the batch is accepted (rejected=false), permanently
// rejected as malformed (rejected=true), or ctx ends (err != nil).
func (s *Sender) Send(ctx context.Context, serverID, region string, events []event.Event) (rejected bool, err error) {
	body, err := s.encode(event.Batch{ServerID: serverID, Events: events})
	if err != nil {
		return false, err
	}
	start := time.Now()
	for attempt := 0; ; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.URL, bytes.NewReader(body))
		if err != nil {
			return false, err
		}
		req.Header.Set("Content-Type", "application/json")
		if s.Gzip {
			req.Header.Set("Content-Encoding", "gzip")
		}
		resp, err := s.Client.Do(req)
		if ctx.Err() != nil {
			return false, ctx.Err()
		}
		wait := s.backoff(attempt)
		reason := "network"
		if err == nil {
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			switch code := resp.StatusCode; {
			case code == http.StatusOK:
				mBatches.With("ok").Inc()
				mAcked.With(region).Add(len(events))
				mBytesSent.With().Add(len(body))
				mSendDur.With().Observe(time.Since(start).Seconds())
				return false, nil
			case code == http.StatusBadRequest || code == http.StatusRequestEntityTooLarge:
				mBatches.With("rejected").Inc()
				s.Log.Error("batch permanently rejected", "server", serverID, "status", code)
				return true, nil
			default:
				reason = strconv.Itoa(code)
				if ra, perr := strconv.Atoi(resp.Header.Get("Retry-After")); perr == nil && ra > 0 {
					wait = time.Duration(ra)*time.Second + time.Duration(rand.Int63n(int64(500*time.Millisecond)))
				}
			}
		}
		mRetries.With(reason).Inc()
		t := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			t.Stop()
			return false, ctx.Err()
		case <-t.C:
		}
	}
}

// backoff implements "full jitter": sleep uniformly in [0, min(cap, base*2^n)].
func (s *Sender) backoff(attempt int) time.Duration {
	d := s.BaseBackoff << min(attempt, 16)
	if d <= 0 || d > s.MaxBackoff {
		d = s.MaxBackoff
	}
	return time.Duration(rand.Int63n(int64(d)) + 1)
}

func (s *Sender) encode(b event.Batch) ([]byte, error) {
	var buf bytes.Buffer
	var w io.Writer = &buf
	var gz *gzip.Writer
	if s.Gzip {
		gz, _ = gzip.NewWriterLevel(&buf, gzip.BestSpeed)
		w = gz
	}
	if err := json.NewEncoder(w).Encode(b); err != nil {
		return nil, fmt.Errorf("encode batch: %w", err)
	}
	if gz != nil {
		if err := gz.Close(); err != nil {
			return nil, err
		}
	}
	return buf.Bytes(), nil
}
