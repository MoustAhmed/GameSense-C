// Command simulator generates realistic, time-varying game telemetry: three
// regions with evening peaks in their own time zones, weekend boosts, and
// scheduled incidents (patch day, outage + reconnect storm, tournament).
package main

import (
	"flag"
	"fmt"
	"math"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"

	"gamesense/internal/sim"
	"gamesense/internal/svc"
)

func main() {
	url := flag.String("ingest", svc.Env("INGEST_URL", "http://localhost:8080/v1/batch"), "ingest batch endpoint")
	scale := flag.Float64("time-scale", 24, "simulated seconds per real second (24 = one day per hour, 1 = real time)")
	start := flag.String("start", "2026-11-16T00:00:00Z", "simulated start time (RFC3339); default is a Monday")
	load := flag.Float64("load", 1, "multiplier on every region's player counts")
	serverScale := flag.Float64("server-scale", 1, "multiplier on the number of game servers (55 at 1). Raise it with -load to keep players per server realistic.")
	eps := flag.Float64("eps", 5, "events per second per player")
	scenarios := flag.String("scenarios", "week", "incident preset: week | none")
	batchMax := flag.Int("batch", 500, "max events per batch")
	flush := flag.Duration("flush", 100*time.Millisecond, "max time an event waits before being sent")
	gz := flag.Bool("gzip", true, "gzip request bodies")
	seed := flag.Int64("seed", 42, "random seed (the auditor must use the same one)")
	shard := flag.Int("shard", 0, "this instance's shard index")
	shards := flag.Int("shards", 1, "total simulator instances (each runs a disjoint subset of game servers)")
	metricsAddr := flag.String("metrics", ":9101", "metrics and /stats listen address")
	runID := flag.String("run-id", strconv.FormatInt(time.Now().UnixMilli()%(36*36*36*36), 36),
		"instance suffix for game server IDs, so a restarted simulator never reuses sequence numbers")
	matchLen := flag.Duration("match-length", 25*time.Minute, "match length in simulated time")
	flag.Parse()

	log := svc.Logger("simulator")
	simStart, err := time.Parse(time.RFC3339, *start)
	if err != nil {
		log.Error("bad -start", "err", err)
		os.Exit(2)
	}
	var sc []sim.Scenario
	if *scenarios == "week" {
		sc = sim.WeekScenarios()
	}

	regions := sim.DefaultRegions(*load)
	for i := range regions {
		regions[i].Servers = max(1, int(math.Round(float64(regions[i].Servers)**serverScale)))
	}
	curve := sim.DefaultCurve(simStart, *seed, sc)
	clock := sim.Clock{SimStart: simStart, WallStart: time.Now(), Scale: *scale}
	stats := sim.NewStats()
	sender := sim.NewSender(*url, *gz)
	cfg := sim.ServerConfig{Tick: 100 * time.Millisecond, BatchMax: *batchMax, FlushEvery: *flush}

	ctx, stop := svc.SignalContext()
	defer stop()
	svc.ServeMetrics(ctx, *metricsAddr, map[string]http.Handler{"GET /stats": stats.Handler()}, log)

	var wg sync.WaitGroup
	idx, running := 0, 0
	for _, r := range regions {
		for i := 0; i < r.Servers; i++ {
			id := fmt.Sprintf("%s-gs%03d-%s", r.Name, i, *runID)
			if idx%*shards == *shard {
				gs := sim.NewGameServer(id, r.Name, r.PeakPlayers*3, *seed, *eps, *matchLen)
				wg.Add(1)
				go func(r sim.Region) {
					defer wg.Done()
					sim.RunServer(ctx, gs, r, curve, clock, sender, stats, cfg)
				}(r)
				running++
			}
			idx++
		}
	}
	peak := 0
	for _, r := range regions {
		peak += r.PeakPlayers
	}
	log.Info("simulating", "servers", running, "time_scale", *scale, "sim_start", simStart,
		"approx_peak_events_per_sec_if_all_regions_peaked", float64(peak)*(*eps))
	<-ctx.Done()
	wg.Wait()
	log.Info("stopped")
}
