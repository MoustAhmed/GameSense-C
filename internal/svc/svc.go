// Package svc holds small helpers shared by every binary: logging, signal
// handling, and the metrics endpoint.
package svc

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"net/http/pprof"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"gamesense/internal/metrics"
)

// Logger returns a JSON logger tagged with the service name.
func Logger(service string) *slog.Logger {
	l := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo})).With("svc", service)
	slog.SetDefault(l)
	return l
}

// SignalContext is cancelled on SIGINT or SIGTERM.
func SignalContext() (context.Context, context.CancelFunc) {
	return signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
}

// ServeMetrics exposes /metrics and /healthz on addr until ctx ends.
func ServeMetrics(ctx context.Context, addr string, extra map[string]http.Handler, log *slog.Logger) {
	mux := http.NewServeMux()
	mux.Handle("GET /metrics", metrics.Default.Handler())
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { w.Write([]byte("ok")) })
	MountPprof(mux)
	for p, h := range extra {
		mux.Handle(p, h)
	}
	srv := &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		<-ctx.Done()
		sctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		srv.Shutdown(sctx)
	}()
	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("metrics server failed", "err", err)
		}
	}()
}

// Env returns an environment variable or a default; used as flag defaults so
// the same binary works locally and in containers.
func Env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// SplitList splits a comma-separated list, dropping empty items.
func SplitList(s string) []string {
	var out []string
	for _, p := range strings.Split(s, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

// MountPprof exposes Go's profiler at /debug/pprof/. Example:
//
//	go tool pprof -http=:7000 http://localhost:9102/debug/pprof/profile?seconds=30
func MountPprof(mux *http.ServeMux) {
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
}
