// Package broker abstracts the message log (Redpanda/Kafka in production,
// an in-memory log in tests) behind a small interface.
package broker

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"
)

// Record is one message in a topic partition.
type Record struct {
	Topic     string
	Key       []byte
	Value     []byte
	Partition int32
	Offset    int64
	Timestamp time.Time
}

// Producer writes records.
type Producer interface {
	// Produce returns only after every record is durably acknowledged by the
	// broker (acks=all), or with an error. Callers must not report success to
	// their own clients before Produce returns nil.
	Produce(ctx context.Context, recs ...*Record) error
	Close()
}

// Consumer reads records as part of a consumer group.
//
// Contract (at-least-once): call Poll, fully process the records (including
// producing any outputs), then call Commit. Records that were polled but not
// committed are redelivered after a crash or rebalance.
type Consumer interface {
	Poll(ctx context.Context) ([]*Record, error)
	// Commit marks every record returned by Poll so far as processed.
	Commit(ctx context.Context) error
	Close()
}

// LagReporter is optionally implemented by consumers that know how far behind
// the log head they are.
type LagReporter interface {
	// Lag returns the total records between the last polled offset and the
	// high watermark, summed over the partitions seen in the latest Poll.
	Lag() int64
}

// Config configures producers and consumers.
type Config struct {
	Brokers        []string
	ClientID       string
	Group          string
	Topics         []string
	MaxPollRecords int
}

// Factory builds producers and consumers for one broker implementation.
type Factory interface {
	NewProducer(Config) (Producer, error)
	NewConsumer(Config) (Consumer, error)
}

// ErrClosed is returned by Poll after the consumer is closed.
var ErrClosed = errors.New("broker: consumer closed")

var (
	mu        sync.Mutex
	factories = map[string]Factory{}
)

// Register makes a factory available by name.
func Register(name string, f Factory) {
	mu.Lock()
	defer mu.Unlock()
	factories[name] = f
}

// Get returns a registered factory.
func Get(name string) (Factory, error) {
	mu.Lock()
	defer mu.Unlock()
	if f, ok := factories[name]; ok {
		return f, nil
	}
	names := make([]string, 0, len(factories))
	for n := range factories {
		names = append(names, n)
	}
	sort.Strings(names)
	return nil, fmt.Errorf("broker %q not available (built-in: %v); was this binary built with -tags nokafka?", name, names)
}
