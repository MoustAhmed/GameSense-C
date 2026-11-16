package metrics

import (
	"fmt"
	"io"
	"runtime"
)

// Process and Go runtime metrics, exported by every service automatically:
// CPU time (to compute CPU %), heap size, goroutines, and GC activity.
func init() { Default.add(runtimeCollector{}) }

type runtimeCollector struct{}

func (runtimeCollector) write(w io.Writer) {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	emit := func(name, typ, help string, v float64) {
		fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s %s\n%s %s\n", name, help, name, typ, name, ff(v))
	}
	if cpu := cpuSeconds(); cpu >= 0 {
		emit("process_cpu_seconds_total", "counter", "User + system CPU time used by this process.", cpu)
	}
	emit("go_goroutines", "gauge", "Goroutines currently running.", float64(runtime.NumGoroutine()))
	emit("go_memstats_heap_alloc_bytes", "gauge", "Bytes of live heap objects.", float64(m.HeapAlloc))
	emit("go_memstats_heap_inuse_bytes", "gauge", "Bytes in in-use heap spans.", float64(m.HeapInuse))
	emit("go_memstats_sys_bytes", "gauge", "Total bytes obtained from the OS.", float64(m.Sys))
	emit("go_gc_cycles_total", "counter", "Completed GC cycles.", float64(m.NumGC))
	emit("go_gc_pause_seconds_total", "counter", "Total GC stop-the-world pause time.", float64(m.PauseTotalNs)/1e9)
}
