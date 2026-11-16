package sim

import (
	"context"
	"encoding/json"
	"math"
	"net/http"
	"sync"
	"time"

	"gamesense/internal/event"
	"gamesense/internal/metrics"
)

var (
	mPlayers   = metrics.NewGauge("sim_players", "Players currently in matches.", "region", "server")
	mTarget    = metrics.NewGauge("sim_target_players", "Players the traffic curve asks for.", "region")
	mGenerated = metrics.NewCounter("sim_events_generated_total", "Events generated.", "region")
	mPending   = metrics.NewGauge("sim_pending_events", "Events buffered client-side, not yet acked.", "server")
	mSimTime   = metrics.NewGauge("sim_time_seconds", "Current simulated unix time.")
)

// Stats tracks, per game server, the highest sequence number that ingest has
// acknowledged. Because each server sends its batches sequentially and never
// drops one, every seq <= AckedThrough was acknowledged: the auditor can
// demand that each of them shows up in the log.
type Stats struct {
	mu      sync.Mutex
	simTime time.Time
	servers map[string]*ServerStats
}

// ServerStats is one server's progress.
type ServerStats struct {
	AckedThrough uint64 `json:"acked_through"`
	Rejected     uint64 `json:"rejected"`
}

// NewStats creates an empty tracker.
func NewStats() *Stats { return &Stats{servers: map[string]*ServerStats{}} }

func (s *Stats) ack(server string, seq uint64, rejected int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	st := s.servers[server]
	if st == nil {
		st = &ServerStats{}
		s.servers[server] = st
	}
	st.AckedThrough = seq
	st.Rejected += uint64(rejected)
}

// Snapshot is the /stats response.
type Snapshot struct {
	SimTime time.Time               `json:"sim_time"`
	Servers map[string]*ServerStats `json:"servers"`
}

// Snapshot copies the current state.
func (s *Stats) Snapshot() Snapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := Snapshot{SimTime: s.simTime, Servers: make(map[string]*ServerStats, len(s.servers))}
	for k, v := range s.servers {
		c := *v
		out.Servers[k] = &c
	}
	return out
}

// Handler serves the snapshot as JSON.
func (s *Stats) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(s.Snapshot())
	})
}

// ServerConfig controls one simulated game server's send loop.
type ServerConfig struct {
	Tick       time.Duration // world step interval (wall time)
	BatchMax   int
	FlushEvery time.Duration
}

// RunServer drives one game server until ctx ends: step the world, buffer
// events, and flush batches. Sending is synchronous per server, so when ingest
// pushes back the server buffers and slows down instead of dropping data.
func RunServer(ctx context.Context, gs *GameServer, region Region, curve *Curve, clock Clock, sender *Sender, stats *Stats, cfg ServerConfig) {
	ticker := time.NewTicker(cfg.Tick)
	defer ticker.Stop()
	var pending []event.Event
	last := time.Now()
	lastFlush := last
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		now := time.Now()
		dt := now.Sub(last).Seconds()
		last = now
		simNow := clock.At(now)

		players := curve.Players(region, simNow)
		target := int(math.Round(float64(players) / float64(region.Servers) / PlayersPerMatch))
		before := len(pending)
		pending = gs.Step(now, simNow, dt, target, pending)
		mGenerated.With(region.Name).Add(len(pending) - before)
		mPlayers.With(region.Name, gs.ID).Set(float64(gs.ActivePlayers()))
		mTarget.With(region.Name).Set(float64(players))
		mSimTime.With().Set(float64(simNow.Unix()))
		stats.mu.Lock()
		stats.simTime = simNow
		stats.mu.Unlock()

		for len(pending) >= cfg.BatchMax || (len(pending) > 0 && now.Sub(lastFlush) >= cfg.FlushEvery) {
			n := min(len(pending), cfg.BatchMax)
			rejected, err := sender.Send(ctx, gs.ID, region.Name, pending[:n])
			if err != nil {
				return // context cancelled; unsent events were never acked
			}
			r := 0
			if rejected {
				r = n
			}
			stats.ack(gs.ID, pending[n-1].Seq, r)
			pending = append(pending[:0], pending[n:]...)
			lastFlush = now
		}
		mPending.With(gs.ID).Set(float64(len(pending)))
	}
}
