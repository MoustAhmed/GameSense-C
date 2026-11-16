//go:build windows

package metrics

import "syscall"

// cpuSeconds returns kernel + user CPU time for this process via GetProcessTimes.
func cpuSeconds() float64 {
	h, err := syscall.GetCurrentProcess()
	if err != nil {
		return -1
	}
	var created, exited, kernel, user syscall.Filetime
	if err := syscall.GetProcessTimes(h, &created, &exited, &kernel, &user); err != nil {
		return -1
	}
	// FILETIME durations are in 100-nanosecond units.
	ft := func(f syscall.Filetime) float64 {
		return float64(uint64(f.HighDateTime)<<32|uint64(f.LowDateTime)) / 1e7
	}
	return ft(kernel) + ft(user)
}
