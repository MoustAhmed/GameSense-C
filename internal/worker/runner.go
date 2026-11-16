// Package worker runs the consume -> process -> produce -> commit loop shared
// by every worker type.
package worker

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"gamesense/internal/broker"
	"gamesense/internal/metrics"
)

var (
	mRecords  = metrics.NewCounter("worker_records_processed_total", "Broker records processed.", "job")
	mBatchDur = metrics.NewHistogram("worker_batch_duration_seconds", "Time to process and commit one poll.", metrics.LatencyBuckets, "job")
	mPollErrs = metrics.NewCounter("worker_poll_errors_total", "Non-fatal poll errors.", "job")
	mCommitEr = metrics.NewCounter("worker_commit_errors_total", "Commit failures (records will be redelivered).", "job")
	mLag      = metrics.NewGauge("worker_consumer_lag", "Records behind the log head (partitions in the latest poll).", "job")
)

// Handler processes one poll's records and returns records to produce.
// Returning an error stops the worker without committing, so the records are
// redelivered to whichever worker picks up the partitions next.
type Handler func(ctx context.Context, recs []*broker.Record) ([]*broker.Record, error)

// Run loops until ctx is cancelled or the handler fails.
//
// Order matters for correctness: outputs are produced (and acked) BEFORE the
// input offsets are committed. A crash in between causes reprocessing, never
// loss — that's at-least-once, made safe by idempotent handlers.
func Run(ctx context.Context, job string, cons broker.Consumer, prod broker.Producer, h Handler, log *slog.Logger) error {
	lagr, _ := cons.(broker.LagReporter)
	for {
		recs, err := cons.Poll(ctx)
		if ctx.Err() != nil || errors.Is(err, broker.ErrClosed) {
			return nil
		}
		if err != nil {
			mPollErrs.With(job).Inc()
			log.Warn("poll error", "err", err)
			if len(recs) == 0 {
				time.Sleep(100 * time.Millisecond)
				continue
			}
		}
		if len(recs) == 0 {
			continue
		}
		start := time.Now()
		out, err := h(ctx, recs)
		if err != nil {
			return err
		}
		if len(out) > 0 {
			if err := prod.Produce(ctx, out...); err != nil {
				return err
			}
		}
		if err := cons.Commit(ctx); err != nil {
			mCommitEr.With(job).Inc()
			log.Warn("commit failed; records will be redelivered", "err", err)
		}
		mRecords.With(job).Add(len(recs))
		mBatchDur.With(job).Observe(time.Since(start).Seconds())
		if lagr != nil {
			mLag.With(job).Set(float64(lagr.Lag()))
		}
	}
}
