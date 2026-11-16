// Command ingest is the stateless HTTP front door for game-server telemetry.
package main

import (
	"context"
	"flag"
	"net/http"
	"os"
	"time"

	"gamesense/internal/broker"
	"gamesense/internal/ingest"
	"gamesense/internal/svc"
)

func main() {
	cfg := ingest.DefaultConfig()
	addr := flag.String("addr", ":8080", "listen address")
	brokers := flag.String("brokers", svc.Env("BROKERS", "localhost:19092"), "comma-separated broker list")
	kind := flag.String("broker", "kafka", "broker implementation: kafka, or discard (Phase 1: accept and drop, for benchmarking ingest alone)")
	drain := flag.Duration("drain", 5*time.Second, "time to fail readiness before shutting down")
	flag.StringVar(&cfg.Topic, "topic", cfg.Topic, "output topic")
	flag.IntVar(&cfg.MaxInFlight, "max-inflight", cfg.MaxInFlight, "concurrent batches before returning 429")
	flag.IntVar(&cfg.MaxEventsPerBatch, "max-events", cfg.MaxEventsPerBatch, "max events per batch")
	flag.Int64Var(&cfg.MaxBodyBytes, "max-body", cfg.MaxBodyBytes, "max request body bytes (compressed); decompressed is capped at 4x")
	flag.Float64Var(&cfg.PerServerRate, "server-rate", cfg.PerServerRate, "events/sec allowed per game server")
	flag.Float64Var(&cfg.PerServerBurst, "server-burst", cfg.PerServerBurst, "burst events per game server")
	flag.DurationVar(&cfg.ProduceTimeout, "produce-timeout", cfg.ProduceTimeout, "max wait for broker acks")
	flag.Parse()

	log := svc.Logger("ingest")
	f, err := broker.Get(*kind)
	if err != nil {
		log.Error("broker", "err", err)
		os.Exit(1)
	}
	host, _ := os.Hostname()
	prod, err := f.NewProducer(broker.Config{Brokers: svc.SplitList(*brokers), ClientID: "ingest-" + host})
	if err != nil {
		log.Error("producer", "err", err)
		os.Exit(1)
	}

	if *kind == "discard" {
		log.Warn("DISCARD BROKER: batches are acknowledged and dropped. Benchmark mode only.")
	}
	s := ingest.New(cfg, prod)
	srv := &http.Server{
		Addr: *addr, Handler: s.Handler(),
		ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 30 * time.Second, IdleTimeout: 120 * time.Second,
	}

	ctx, stop := svc.SignalContext()
	defer stop()
	go func() {
		log.Info("listening", "addr", *addr, "topic", cfg.Topic)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Error("server failed", "err", err)
			os.Exit(1)
		}
	}()
	<-ctx.Done()

	// Graceful shutdown: stop advertising readiness, let the LB notice, finish
	// in-flight batches (they're already being acked), then close the producer.
	log.Info("draining", "for", drain.String())
	s.SetReady(false)
	time.Sleep(*drain)
	sctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := srv.Shutdown(sctx); err != nil {
		log.Warn("shutdown", "err", err)
	}
	prod.Close()
	log.Info("stopped")
}
