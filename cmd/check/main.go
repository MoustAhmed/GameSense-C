// Command check prints a PASS/FAIL/WAIT health report for the running stack.
//
//	make check              # one report
//	make check WATCH=10s    # repeat every 10s
//
// It exits non-zero if any check fails, so scripts (chaos game days, CI) can
// use it as a gate.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"gamesense/internal/health"
)

const (
	green  = "\033[32m"
	red    = "\033[31m"
	yellow = "\033[33m"
	reset  = "\033[0m"
)

func main() {
	prom := flag.String("prom", "http://localhost:9090", "Prometheus URL")
	watch := flag.Duration("watch", 0, "repeat at this interval (0 = run once)")
	flag.Parse()

	q := health.NewProm(*prom)
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		results := health.Run(ctx, q)
		cancel()
		failed := report(results)
		if *watch == 0 {
			if failed {
				os.Exit(1)
			}
			return
		}
		time.Sleep(*watch)
	}
}

func report(rs []health.Result) (failed bool) {
	fmt.Printf("\nGameSense health  %s\n", time.Now().Format("15:04:05"))
	for _, r := range rs {
		c := green
		switch r.Status {
		case health.Fail:
			c, failed = red, true
		case health.Wait:
			c = yellow
		}
		fmt.Printf("  %s%-4s%s  %-30s %s\n", c, r.Status, reset, r.Name, r.Detail)
	}
	return failed
}
