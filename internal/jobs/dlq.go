package jobs

import (
	"encoding/json"
	"time"

	"gamesense/internal/broker"
	"gamesense/internal/metrics"
)

var mDLQ = metrics.NewCounter("worker_dlq_total", "Records sent to the dead-letter topic, by job and reason.", "job", "reason")

// DeadLetter wraps a record that can never be processed (a "poison message").
//
// Retrying a permanently bad record forever would block its whole partition
// (every later record waits behind it) and crash-loop the worker. Instead we
// park it in a dead-letter topic with enough context to debug or replay it,
// and keep the pipeline flowing. Transient errors (broker down) are NOT
// dead-lettered; those fail the batch so it is retried.
type DeadLetter struct {
	Job         string `json:"job"`
	Reason      string `json:"reason"`
	Error       string `json:"error"`
	SourceTopic string `json:"source_topic"`
	Partition   int32  `json:"partition"`
	Offset      int64  `json:"offset"`
	Key         []byte `json:"key"`   // base64 in JSON
	Value       []byte `json:"value"` // base64 in JSON, the original bytes
	FailedAt    int64  `json:"failed_at"`
}

// deadLetter builds the DLQ record for r. The key is source position, so a
// redelivered poison record overwrites rather than duplicates (on compacted topics).
func deadLetter(job, reason, dlqTopic string, r *broker.Record, err error) *broker.Record {
	mDLQ.With(job, reason).Inc()
	v, _ := json.Marshal(DeadLetter{
		Job: job, Reason: reason, Error: err.Error(), SourceTopic: r.Topic,
		Partition: r.Partition, Offset: r.Offset, Key: r.Key, Value: r.Value,
		FailedAt: time.Now().UnixMilli(),
	})
	return &broker.Record{Topic: dlqTopic, Key: r.Key, Value: v}
}
