//go:build !unix && !windows && !windows

package metrics

func cpuSeconds() float64 { return -1 } // not available; CPU stats are implemented for Linux, macOS and Windows
