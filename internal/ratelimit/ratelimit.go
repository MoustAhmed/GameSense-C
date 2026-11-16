// Package ratelimit implements token buckets, optionally keyed per client.
package ratelimit

import (
	"sync"
	"time"
)

// Bucket is a classic token bucket: it refills at rate tokens/sec up to burst.
type Bucket struct {
	mu     sync.Mutex
	rate   float64
	burst  float64
	tokens float64
	last   time.Time
	now    func() time.Time
}

// NewBucket returns a full bucket.
func NewBucket(rate, burst float64) *Bucket {
	return &Bucket{rate: rate, burst: burst, tokens: burst, last: time.Now(), now: time.Now}
}

// AllowN takes n tokens if available.
func (b *Bucket) AllowN(n float64) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	now := b.now()
	b.tokens += now.Sub(b.last).Seconds() * b.rate
	if b.tokens > b.burst {
		b.tokens = b.burst
	}
	b.last = now
	if b.tokens < n {
		return false
	}
	b.tokens -= n
	return true
}

// Keyed holds one bucket per key (e.g. per game server) and evicts idle ones.
type Keyed struct {
	mu      sync.Mutex
	rate    float64
	burst   float64
	buckets map[string]*entry
	sweep   time.Time
}

type entry struct {
	b    *Bucket
	seen time.Time
}

// NewKeyed creates a per-key limiter.
func NewKeyed(rate, burst float64) *Keyed {
	return &Keyed{rate: rate, burst: burst, buckets: map[string]*entry{}, sweep: time.Now()}
}

// AllowN applies the limit for key.
func (k *Keyed) AllowN(key string, n float64) bool {
	now := time.Now()
	k.mu.Lock()
	e := k.buckets[key]
	if e == nil {
		e = &entry{b: NewBucket(k.rate, k.burst)}
		k.buckets[key] = e
	}
	e.seen = now
	if now.Sub(k.sweep) > time.Minute {
		for key, v := range k.buckets {
			if now.Sub(v.seen) > 10*time.Minute {
				delete(k.buckets, key)
			}
		}
		k.sweep = now
	}
	k.mu.Unlock()
	return e.b.AllowN(n)
}
