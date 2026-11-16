package broker

import (
	"context"
	"hash/fnv"
	"sync"
	"time"
)

// Memory is an in-process partitioned log used by tests. It mimics the parts
// of Kafka semantics the services rely on: key-based partitioning, per-partition
// ordering, and consumer-group committed offsets (so a consumer that is closed
// without committing causes redelivery). Each group should have one member.
type Memory struct {
	mu         sync.Mutex
	partitions int
	topics     map[string][][]*Record
	committed  map[string]map[tp]int64 // group -> next offset to read
	notify     chan struct{}
}

type tp struct {
	topic string
	part  int32
}

// NewMemory creates a log where every topic has the given partition count.
func NewMemory(partitions int) *Memory {
	return &Memory{
		partitions: partitions,
		topics:     map[string][][]*Record{},
		committed:  map[string]map[tp]int64{},
		notify:     make(chan struct{}),
	}
}

func (m *Memory) topic(name string) [][]*Record {
	t, ok := m.topics[name]
	if !ok {
		t = make([][]*Record, m.partitions)
		m.topics[name] = t
	}
	return t
}

// Len returns the number of records in a topic.
func (m *Memory) Len(topic string) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	n := 0
	for _, p := range m.topic(topic) {
		n += len(p)
	}
	return n
}

// Pending returns how many records in topic the group has not committed yet
// (its consumer lag).
func (m *Memory) Pending(group, topic string) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	n := 0
	for part, recs := range m.topic(topic) {
		n += len(recs) - int(m.committed[group][tp{topic, int32(part)}])
	}
	return n
}

func (m *Memory) NewProducer(Config) (Producer, error) { return &memProducer{m}, nil }

func (m *Memory) NewConsumer(c Config) (Consumer, error) {
	max := c.MaxPollRecords
	if max <= 0 {
		max = 1000
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.committed[c.Group] == nil {
		m.committed[c.Group] = map[tp]int64{}
	}
	pos := map[tp]int64{}
	for k, v := range m.committed[c.Group] {
		pos[k] = v
	}
	return &memConsumer{m: m, group: c.Group, topics: c.Topics, pos: pos, max: max}, nil
}

type memProducer struct{ m *Memory }

func (p *memProducer) Produce(ctx context.Context, recs ...*Record) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	m := p.m
	m.mu.Lock()
	for _, r := range recs {
		t := m.topic(r.Topic)
		h := fnv.New32a()
		h.Write(r.Key)
		part := int32(h.Sum32() % uint32(m.partitions))
		cp := *r
		cp.Partition = part
		cp.Offset = int64(len(t[part]))
		cp.Timestamp = time.Now()
		t[part] = append(t[part], &cp)
	}
	close(m.notify)
	m.notify = make(chan struct{})
	m.mu.Unlock()
	return nil
}

func (p *memProducer) Close() {}

type memConsumer struct {
	m      *Memory
	group  string
	topics []string
	pos    map[tp]int64
	max    int
	closed bool
}

func (c *memConsumer) Poll(ctx context.Context) ([]*Record, error) {
	for {
		c.m.mu.Lock()
		if c.closed {
			c.m.mu.Unlock()
			return nil, ErrClosed
		}
		var out []*Record
		for _, name := range c.topics {
			for part, recs := range c.m.topic(name) {
				k := tp{name, int32(part)}
				for off := c.pos[k]; off < int64(len(recs)) && len(out) < c.max; off++ {
					out = append(out, recs[off])
					c.pos[k] = off + 1
				}
			}
		}
		wait := c.m.notify
		c.m.mu.Unlock()
		if len(out) > 0 {
			return out, nil
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-wait:
		case <-time.After(50 * time.Millisecond):
		}
	}
}

func (c *memConsumer) Commit(context.Context) error {
	c.m.mu.Lock()
	defer c.m.mu.Unlock()
	for k, v := range c.pos {
		c.m.committed[c.group][k] = v
	}
	return nil
}

// Close abandons uncommitted progress, like a crashed process would.
func (c *memConsumer) Close() {
	c.m.mu.Lock()
	c.closed = true
	c.m.mu.Unlock()
}
