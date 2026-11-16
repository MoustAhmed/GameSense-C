// Command worker consumes telemetry from the log and runs a job on it.
// Scale it horizontally: replicas in the same group split the partitions.
package main

import (
	"flag"
	"os"

	"gamesense/internal/broker"
	"gamesense/internal/detect"
	"gamesense/internal/jobs"
	"gamesense/internal/svc"
	"gamesense/internal/worker"
)

func main() {
	job := flag.String("job", "detector", "job to run: detector")
	brokers := flag.String("brokers", svc.Env("BROKERS", "localhost:19092"), "comma-separated broker list")
	kind := flag.String("broker", "kafka", "broker implementation")
	in := flag.String("in", "telemetry.raw", "input topic")
	out := flag.String("out", "detections", "output topic")
	dlq := flag.String("dlq", "telemetry.dlq", "dead-letter topic for records that can never be processed")
	group := flag.String("group", "", "consumer group (defaults to the job name)")
	maxPoll := flag.Int("max-poll", 5000, "max records per poll")
	metricsAddr := flag.String("metrics", ":9102", "metrics listen address")
	flag.Parse()

	log := svc.Logger("worker").With("job", *job)
	if *group == "" {
		*group = *job
	}
	ctx, stop := svc.SignalContext()
	defer stop()
	svc.ServeMetrics(ctx, *metricsAddr, nil, log)

	f, err := broker.Get(*kind)
	if err != nil {
		log.Error("broker", "err", err)
		os.Exit(1)
	}
	host, _ := os.Hostname()
	bc := broker.Config{Brokers: svc.SplitList(*brokers), ClientID: *job + "-" + host, Group: *group, Topics: []string{*in}, MaxPollRecords: *maxPoll}
	cons, err := f.NewConsumer(bc)
	if err != nil {
		log.Error("consumer", "err", err)
		os.Exit(1)
	}
	prod, err := f.NewProducer(bc)
	if err != nil {
		log.Error("producer", "err", err)
		os.Exit(1)
	}

	var h worker.Handler
	switch *job {
	case "detector":
		h = jobs.Detector(detect.New(detect.DefaultRules()), *out, *dlq)
	default:
		log.Error("unknown job")
		os.Exit(2)
	}

	log.Info("starting", "group", *group, "in", *in, "out", *out)
	err = worker.Run(ctx, *job, cons, prod, h, log)
	cons.Close()
	prod.Close()
	if err != nil {
		// Exiting non-zero lets the orchestrator restart us; uncommitted
		// records are redelivered, so nothing is lost.
		log.Error("worker stopped", "err", err)
		os.Exit(1)
	}
	log.Info("stopped")
}
