package broker

import (
	"context"
	"errors"
)

// The discard broker accepts records and throws them away. It exists for
// Phase 1 benchmarking: it measures how fast the ingest API itself is, with no
// queue behind it. It must never be used when data matters.
func init() { Register("discard", discardFactory{}) }

type discardFactory struct{}

func (discardFactory) NewProducer(Config) (Producer, error) { return discardProducer{}, nil }

func (discardFactory) NewConsumer(Config) (Consumer, error) {
	return nil, errors.New("the discard broker has no consumers (it is for ingest-only benchmarks)")
}

type discardProducer struct{}

func (discardProducer) Produce(ctx context.Context, _ ...*Record) error { return ctx.Err() }
func (discardProducer) Close()                                          {}
