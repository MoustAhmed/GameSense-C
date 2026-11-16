//go:build !nokafka

package broker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

func init() { Register("kafka", kafkaFactory{}) }

type kafkaFactory struct{}

// NewProducer builds an idempotent producer that waits for all in-sync
// replicas (on Redpanda: a Raft majority) before acknowledging.
func (kafkaFactory) NewProducer(c Config) (Producer, error) {
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(c.Brokers...),
		kgo.ClientID(c.ClientID),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.ProducerLinger(5*time.Millisecond),
		kgo.ProducerBatchCompression(kgo.Lz4Compression()),
		kgo.ProducerBatchMaxBytes(1<<20),
		kgo.RecordDeliveryTimeout(30*time.Second),
	)
	if err != nil {
		return nil, fmt.Errorf("kafka producer: %w", err)
	}
	return &kafkaProducer{cl: cl}, nil
}

type kafkaProducer struct{ cl *kgo.Client }

func (p *kafkaProducer) Produce(ctx context.Context, recs ...*Record) error {
	krs := make([]*kgo.Record, len(recs))
	for i, r := range recs {
		krs[i] = &kgo.Record{Topic: r.Topic, Key: r.Key, Value: r.Value}
	}
	return p.cl.ProduceSync(ctx, krs...).FirstErr()
}

func (p *kafkaProducer) Close() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = p.cl.Flush(ctx)
	p.cl.Close()
}

// NewConsumer builds a group consumer with manual commits.
//
// BlockRebalanceOnPoll guarantees partitions are not revoked between Poll and
// Commit, so the revoke callback never commits records we have not processed.
func (kafkaFactory) NewConsumer(c Config) (Consumer, error) {
	max := c.MaxPollRecords
	if max <= 0 {
		max = 5000
	}
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(c.Brokers...),
		kgo.ClientID(c.ClientID),
		kgo.ConsumerGroup(c.Group),
		kgo.ConsumeTopics(c.Topics...),
		kgo.DisableAutoCommit(),
		kgo.BlockRebalanceOnPoll(),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.FetchMaxWait(250*time.Millisecond),
		kgo.OnPartitionsRevoked(func(ctx context.Context, cl *kgo.Client, _ map[string][]int32) {
			if err := cl.CommitUncommittedOffsets(ctx); err != nil {
				slog.Warn("commit on revoke failed", "err", err)
			}
		}),
	)
	if err != nil {
		return nil, fmt.Errorf("kafka consumer: %w", err)
	}
	return &kafkaConsumer{cl: cl, max: max}, nil
}

type kafkaConsumer struct {
	cl  *kgo.Client
	max int
	lag atomic.Int64
}

func (c *kafkaConsumer) Poll(ctx context.Context) ([]*Record, error) {
	fetches := c.cl.PollRecords(ctx, c.max)
	if fetches.IsClientClosed() {
		return nil, ErrClosed
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	var out []*Record
	var lag int64
	fetches.EachPartition(func(p kgo.FetchTopicPartition) {
		if n := len(p.Records); n > 0 {
			lag += p.HighWatermark - p.Records[n-1].Offset - 1
		}
		for _, r := range p.Records {
			out = append(out, &Record{Topic: r.Topic, Key: r.Key, Value: r.Value,
				Partition: r.Partition, Offset: r.Offset, Timestamp: r.Timestamp})
		}
	})
	if len(out) > 0 {
		c.lag.Store(lag)
	}
	var errs []error
	fetches.EachError(func(t string, p int32, err error) {
		errs = append(errs, fmt.Errorf("fetch %s[%d]: %w", t, p, err))
	})
	return out, errors.Join(errs...)
}

func (c *kafkaConsumer) Commit(ctx context.Context) error {
	defer c.cl.AllowRebalance()
	return c.cl.CommitUncommittedOffsets(ctx)
}

func (c *kafkaConsumer) Lag() int64 { return c.lag.Load() }

func (c *kafkaConsumer) Close() {
	c.cl.AllowRebalance()
	c.cl.Close()
}
