// Package jobs wires domain logic (detectors, aggregators) into worker handlers.
package jobs

import (
	"context"
	"encoding/json"
	"time"

	"gamesense/internal/broker"
	"gamesense/internal/detect"
	"gamesense/internal/event"
	"gamesense/internal/metrics"
	"gamesense/internal/worker"
)

var (
	mE2E        = metrics.NewHistogram("worker_e2e_latency_seconds", "Event age when processed (generation -> worker).", metrics.LatencyBuckets, "job")
	mEvents     = metrics.NewCounter("worker_events_processed_total", "Telemetry events processed.", "job")
	mDetections = metrics.NewCounter("worker_detections_total", "Cheat detections emitted.", "rule")
	mTracked    = metrics.NewGauge("worker_tracked_players", "Player states held in memory.", "job")
)

// Detector returns the anti-cheat handler. Detections are keyed by player ID
// and carry deterministic IDs so downstream consumers can dedupe. Records that
// can't be decoded go to dlqTopic instead of stopping the worker.
func Detector(det *detect.Detector, outTopic, dlqTopic string) worker.Handler {
	lastSweep := time.Now()
	return func(_ context.Context, recs []*broker.Record) ([]*broker.Record, error) {
		var out []*broker.Record
		now := time.Now()
		for _, r := range recs {
			var evs []event.Event
			if err := json.Unmarshal(r.Value, &evs); err != nil {
				out = append(out, deadLetter("detector", "decode", dlqTopic, r, err))
				continue
			}
			if n := len(evs); n > 0 {
				mE2E.With("detector").Observe(now.Sub(time.UnixMilli(evs[n-1].TS)).Seconds())
				mEvents.With("detector").Add(n)
			}
			for _, d := range det.Process(evs) {
				mDetections.With(d.Rule).Inc()
				v, _ := json.Marshal(d)
				out = append(out, &broker.Record{Topic: outTopic, Key: []byte(d.PlayerID), Value: v})
			}
		}
		if now.Sub(lastSweep) > 30*time.Second {
			det.Sweep()
			lastSweep = now
		}
		mTracked.With("detector").Set(float64(det.Tracked()))
		return out, nil
	}
}
