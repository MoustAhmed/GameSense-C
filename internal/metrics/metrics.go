// Package metrics is a tiny, dependency-free Prometheus text-format exporter.
//
// It supports labeled counters, gauges and histograms, which is all this
// project needs. Swapping in prometheus/client_golang later is a mechanical change.
package metrics

import (
	"fmt"
	"io"
	"math"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
)

// Registry holds metric families and renders them in Prometheus text format.
type Registry struct {
	mu   sync.Mutex
	fams []writer
}

type writer interface{ write(w io.Writer) }

// Default is the process-wide registry used by the New* constructors.
var Default = &Registry{}

func (r *Registry) add(f writer) {
	r.mu.Lock()
	r.fams = append(r.fams, f)
	r.mu.Unlock()
}

// Handler serves the registry at /metrics.
func (r *Registry) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		r.Render(w)
	})
}

// Render renders every family that has at least one child.
func (r *Registry) Render(w io.Writer) {
	r.mu.Lock()
	fams := append([]writer(nil), r.fams...)
	r.mu.Unlock()
	for _, f := range fams {
		f.write(w)
	}
}

// Counter is a monotonically increasing value.
type Counter struct{ v atomic.Uint64 }

func (c *Counter) Inc() { c.v.Add(1) }
func (c *Counter) Add(n int) {
	if n > 0 {
		c.v.Add(uint64(n))
	}
}
func (c *Counter) Value() uint64 { return c.v.Load() }

// Gauge is a value that can go up and down.
type Gauge struct{ bits atomic.Uint64 }

func (g *Gauge) Set(v float64) { g.bits.Store(math.Float64bits(v)) }
func (g *Gauge) Add(d float64) {
	for {
		old := g.bits.Load()
		nv := math.Float64bits(math.Float64frombits(old) + d)
		if g.bits.CompareAndSwap(old, nv) {
			return
		}
	}
}
func (g *Gauge) Value() float64 { return math.Float64frombits(g.bits.Load()) }

// Histogram counts observations into cumulative buckets.
type Histogram struct {
	bounds []float64
	counts []atomic.Uint64 // len(bounds)+1, last is +Inf
	sum    Gauge
	count  atomic.Uint64
}

func (h *Histogram) Observe(v float64) {
	h.counts[sort.SearchFloat64s(h.bounds, v)].Add(1)
	h.sum.Add(v)
	h.count.Add(1)
}
func (h *Histogram) Count() uint64 { return h.count.Load() }

// LatencyBuckets are sensible defaults for request latencies in seconds.
var LatencyBuckets = []float64{.001, .0025, .005, .01, .025, .05, .075, .1, .15, .25, .35, .5, .75, 1, 1.5, 2, 2.5, 3, 4, 5, 7.5, 10, 30}

// Family is a metric name plus its labeled children.
type Family[T any] struct {
	name, help, typ string
	labels          []string
	mk              func() *T
	emit            func(w io.Writer, name, lbls string, m *T)

	mu   sync.RWMutex
	kids map[string]*kid[T]
	keys []string
}

type kid[T any] struct {
	lbls string
	m    *T
}

// With returns the child for the given label values, creating it if needed.
func (f *Family[T]) With(values ...string) *T {
	if len(values) != len(f.labels) {
		panic(fmt.Sprintf("metrics: %s expects %d label values, got %d", f.name, len(f.labels), len(values)))
	}
	key := strings.Join(values, "\xff")
	f.mu.RLock()
	k := f.kids[key]
	f.mu.RUnlock()
	if k != nil {
		return k.m
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if k = f.kids[key]; k == nil {
		k = &kid[T]{lbls: formatLabels(f.labels, values), m: f.mk()}
		f.kids[key] = k
		f.keys = append(f.keys, key)
	}
	return k.m
}

func (f *Family[T]) write(w io.Writer) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	if len(f.keys) == 0 {
		return
	}
	fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s %s\n", f.name, f.help, f.name, f.typ)
	for _, key := range f.keys {
		k := f.kids[key]
		f.emit(w, f.name, k.lbls, k.m)
	}
}

func register[T any](name, help, typ string, labels []string, mk func() *T, emit func(io.Writer, string, string, *T)) *Family[T] {
	f := &Family[T]{name: name, help: help, typ: typ, labels: labels, mk: mk, emit: emit, kids: map[string]*kid[T]{}}
	Default.add(f)
	return f
}

// NewCounter registers a counter family on the Default registry.
func NewCounter(name, help string, labels ...string) *Family[Counter] {
	return register(name, help, "counter", labels, func() *Counter { return &Counter{} },
		func(w io.Writer, n, l string, c *Counter) { fmt.Fprintf(w, "%s%s %d\n", n, braces(l), c.Value()) })
}

// NewGauge registers a gauge family on the Default registry.
func NewGauge(name, help string, labels ...string) *Family[Gauge] {
	return register(name, help, "gauge", labels, func() *Gauge { return &Gauge{} },
		func(w io.Writer, n, l string, g *Gauge) { fmt.Fprintf(w, "%s%s %s\n", n, braces(l), ff(g.Value())) })
}

// NewHistogram registers a histogram family on the Default registry.
func NewHistogram(name, help string, buckets []float64, labels ...string) *Family[Histogram] {
	b := append([]float64(nil), buckets...)
	sort.Float64s(b)
	return register(name, help, "histogram", labels,
		func() *Histogram { return &Histogram{bounds: b, counts: make([]atomic.Uint64, len(b)+1)} },
		func(w io.Writer, n, l string, h *Histogram) {
			sep := ""
			if l != "" {
				sep = ","
			}
			var cum uint64
			for i, ub := range h.bounds {
				cum += h.counts[i].Load()
				fmt.Fprintf(w, "%s_bucket{%s%sle=\"%s\"} %d\n", n, l, sep, ff(ub), cum)
			}
			cum += h.counts[len(h.bounds)].Load()
			fmt.Fprintf(w, "%s_bucket{%s%sle=\"+Inf\"} %d\n", n, l, sep, cum)
			fmt.Fprintf(w, "%s_sum%s %s\n", n, braces(l), ff(h.sum.Value()))
			fmt.Fprintf(w, "%s_count%s %d\n", n, braces(l), h.count.Load())
		})
}

func ff(v float64) string { return strconv.FormatFloat(v, 'g', -1, 64) }

func braces(l string) string {
	if l == "" {
		return ""
	}
	return "{" + l + "}"
}

var labelEscaper = strings.NewReplacer(`\`, `\\`, `"`, `\"`, "\n", `\n`)

func formatLabels(names, values []string) string {
	parts := make([]string, len(names))
	for i := range names {
		parts[i] = names[i] + `="` + labelEscaper.Replace(values[i]) + `"`
	}
	return strings.Join(parts, ",")
}
