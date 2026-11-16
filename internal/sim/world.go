package sim

import (
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"math"
	"math/rand"
	"time"

	"gamesense/internal/event"
)

// CheatKind is the ground truth for a simulated player.
type CheatKind uint8

const (
	CheatNone CheatKind = iota
	CheatAimbot
	CheatSpeed
)

func (c CheatKind) String() string {
	switch c {
	case CheatAimbot:
		return "aimbot"
	case CheatSpeed:
		return "speedhack"
	}
	return "none"
}

// CheatFor deterministically decides whether a player cheats (~0.7% do).
// The auditor calls the same function to score the detector's precision and
// recall, so the ground truth never travels inside the telemetry.
func CheatFor(seed int64, playerID string) CheatKind {
	h := fnv.New64a()
	var b [8]byte
	binary.LittleEndian.PutUint64(b[:], uint64(seed))
	h.Write(b[:])
	h.Write([]byte(playerID))
	switch v := h.Sum64() % 1000; {
	case v < 4:
		return CheatAimbot
	case v < 7:
		return CheatSpeed
	}
	return CheatNone
}

const (
	PlayersPerMatch = 10
	mapSize         = 500.0 // metres
	humanMaxSpeed   = 6.5   // m/s
	hackMaxSpeed    = 18.0  // m/s
	humanMaxTurn    = 250.0 // deg/s sustained
	humanFlickTurn  = 450.0 // deg/s, rare flicks
)

type player struct {
	id                  string
	cheat               CheatKind
	x, y, vx, vy        float64
	yaw, pitch, yawRate float64
	boost               float64 // remaining speedhack burst, seconds
	sinceLastEvent      float64
}

type match struct {
	id      string
	players []*player
	endsAt  time.Time // simulated time
}

// GameServer hosts matches and turns them into a numbered event stream.
type GameServer struct {
	ID           string
	Region       string
	EventsPerSec float64       // per player
	MatchLength  time.Duration // simulated time
	poolSize     int           // distinct player IDs in the region
	seed         int64
	rng          *rand.Rand
	matches      []*match
	seq          uint64
	matchSeq     uint64
}

// NewGameServer creates a server. poolSize bounds how many distinct players
// exist in the region (players recur across matches, as in a real game).
func NewGameServer(id, region string, poolSize int, seed int64, eventsPerSec float64, matchLength time.Duration) *GameServer {
	h := fnv.New64a()
	h.Write([]byte(id))
	if poolSize < PlayersPerMatch {
		poolSize = PlayersPerMatch
	}
	return &GameServer{
		ID: id, Region: region, EventsPerSec: eventsPerSec, MatchLength: matchLength,
		poolSize: poolSize, seed: seed, rng: rand.New(rand.NewSource(seed ^ int64(h.Sum64()))),
	}
}

// ActivePlayers returns the number of players currently in matches.
func (g *GameServer) ActivePlayers() int { return len(g.matches) * PlayersPerMatch }

// Seq returns the last sequence number assigned.
func (g *GameServer) Seq() uint64 { return g.seq }

// Step advances the world by dt wall seconds, reconciles the number of running
// matches with target, and appends generated events to out.
func (g *GameServer) Step(wallNow, simNow time.Time, dt float64, target int, out []event.Event) []event.Event {
	if dt > 2 {
		dt = 2 // after a long stall (e.g. backpressure) don't generate a flood of stale samples
	}
	kept := g.matches[:0]
	for _, m := range g.matches {
		if simNow.Before(m.endsAt) {
			kept = append(kept, m)
		} else {
			out = g.endMatch(m, wallNow, simNow, out)
		}
	}
	g.matches = kept
	for len(g.matches) > target {
		out = g.endMatch(g.matches[0], wallNow, simNow, out)
		g.matches = g.matches[1:]
	}
	for len(g.matches) < target {
		g.matches = append(g.matches, g.newMatch(simNow))
	}

	interval := 1 / g.EventsPerSec
	for _, m := range g.matches {
		for _, p := range m.players {
			p.sinceLastEvent += dt
			for p.sinceLastEvent >= interval {
				p.sinceLastEvent -= interval
				g.move(p, interval)
				ts := wallNow.Add(-time.Duration(p.sinceLastEvent * float64(time.Second)))
				out = append(out, g.sample(m, p, ts, simNow))
			}
		}
	}
	return out
}

func (g *GameServer) newMatch(simNow time.Time) *match {
	g.matchSeq++
	m := &match{
		id:     fmt.Sprintf("%s-m%d", g.ID, g.matchSeq),
		endsAt: simNow.Add(time.Duration(float64(g.MatchLength) * (0.8 + 0.4*g.rng.Float64()))),
	}
	// Player IDs must be unique within a match: two bodies sharing one ID look
	// like a single player teleporting (the detector caught this bug).
	used := make(map[int]bool, PlayersPerMatch)
	for i := 0; i < PlayersPerMatch; i++ {
		n := g.rng.Intn(g.poolSize)
		for used[n] {
			n = g.rng.Intn(g.poolSize)
		}
		used[n] = true
		id := fmt.Sprintf("%s-p%d", g.Region, n)
		m.players = append(m.players, &player{
			id: id, cheat: CheatFor(g.seed, id),
			x: g.rng.Float64() * mapSize, y: g.rng.Float64() * mapSize,
			yaw: g.rng.Float64() * 360, sinceLastEvent: g.rng.Float64() / g.EventsPerSec,
		})
	}
	return m
}

func (g *GameServer) endMatch(m *match, wallNow, simNow time.Time, out []event.Event) []event.Event {
	for i, p := range m.players {
		e := g.sample(m, p, wallNow, simNow)
		e.Type = event.TypeMatchEnd
		e.Hit = false
		e.Won = i < PlayersPerMatch/2
		out = append(out, e)
	}
	return out
}

func (g *GameServer) move(p *player, dt float64) {
	r := g.rng
	// Movement: a velocity random walk, clamped to what the player can do.
	p.vx += r.NormFloat64() * 8 * dt
	p.vy += r.NormFloat64() * 8 * dt
	maxSpeed := humanMaxSpeed
	if p.cheat == CheatSpeed {
		if p.boost <= 0 && r.Float64() < 0.03 {
			p.boost = 1 + r.Float64()
		}
		if p.boost > 0 {
			p.boost -= dt
			maxSpeed = hackMaxSpeed
			if s := math.Hypot(p.vx, p.vy); s > 0 {
				p.vx, p.vy = p.vx/s*maxSpeed, p.vy/s*maxSpeed
			}
		}
	}
	if s := math.Hypot(p.vx, p.vy); s > maxSpeed {
		p.vx, p.vy = p.vx/s*maxSpeed, p.vy/s*maxSpeed
	}
	p.x, p.vx = reflect(p.x+p.vx*dt, p.vx)
	p.y, p.vy = reflect(p.y+p.vy*dt, p.vy)

	// Aim: smooth turning, rare human flicks, and aimbot snaps.
	p.yawRate += r.NormFloat64() * 300 * dt
	p.yawRate = math.Max(-humanMaxTurn, math.Min(humanMaxTurn, p.yawRate))
	turn := p.yawRate * dt
	switch {
	case p.cheat == CheatAimbot && r.Float64() < 0.04:
		turn = sign(r) * (120 + r.Float64()*50) // instant lock-on, regardless of dt
	case r.Float64() < 0.01:
		turn = sign(r) * (humanMaxTurn + r.Float64()*(humanFlickTurn-humanMaxTurn)) * dt
	}
	p.yaw = math.Mod(p.yaw+turn+360, 360)
	p.pitch = math.Max(-60, math.Min(60, p.pitch+r.NormFloat64()*20*dt))
}

func (g *GameServer) sample(m *match, p *player, ts, simNow time.Time) event.Event {
	g.seq++
	e := event.Event{
		ServerID: g.ID, Seq: g.seq, Region: g.Region, MatchID: m.id, PlayerID: p.id,
		Type: event.TypeTick, TS: ts.UnixMilli(), SimTS: simNow.UnixMilli(),
		X: float32(p.x), Y: float32(p.y), Yaw: float32(p.yaw), Pitch: float32(p.pitch),
	}
	if g.rng.Float64() < 0.1 {
		e.Type = event.TypeShot
		hitP := 0.25
		if p.cheat == CheatAimbot {
			hitP = 0.7
		}
		e.Hit = g.rng.Float64() < hitP
	}
	return e
}

func reflect(pos, v float64) (float64, float64) {
	if pos < 0 {
		return -pos, -v
	}
	if pos > mapSize {
		return 2*mapSize - pos, -v
	}
	return pos, v
}

func sign(r *rand.Rand) float64 {
	if r.Intn(2) == 0 {
		return -1
	}
	return 1
}
