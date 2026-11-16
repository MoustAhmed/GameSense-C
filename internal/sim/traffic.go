// Package sim models a global player population and the game servers that
// stream their telemetry.
package sim

import (
	"encoding/binary"
	"hash/fnv"
	"math"
	"time"
)

// Region is a geographic shard of the player base.
type Region struct {
	Name           string
	UTCOffsetHours float64
	PeakPlayers    int // concurrent players at a normal weekday peak
	Servers        int // game servers hosting this region's matches
}

// Scenario kinds.
const (
	ScenarioPatch      = "patch"      // everyone logs in to try the patch: spike that decays
	ScenarioTournament = "tournament" // sustained plateau in one region
	ScenarioOutage     = "outage"     // region drops to zero, then a reconnect storm
)

// Scenario is a scheduled event layered on top of the daily curve.
type Scenario struct {
	Kind          string
	Region        string  // "" = all regions
	StartHour     float64 // hours after Curve.Start (simulated time)
	DurationHours float64
	Multiplier    float64 // peak multiplier (patch, tournament, reconnect surge)
}

// Curve turns (region, simulated time) into a concurrent player count.
type Curve struct {
	Start         time.Time
	TroughFrac    float64 // load at the quietest hour, as a fraction of peak
	PeakLocalHour float64 // local hour of the evening peak
	Sharpness     float64 // >1 makes the peak narrower
	WeekendBoost  float64 // multiplier on Saturday/Sunday
	Noise         float64 // +/- fraction of smooth random jitter
	Seed          int64
	Scenarios     []Scenario
}

// DefaultCurve is a realistic evening-peak shape.
func DefaultCurve(start time.Time, seed int64, scenarios []Scenario) *Curve {
	return &Curve{
		Start: start, TroughFrac: 0.15, PeakLocalHour: 20.5, Sharpness: 1.6,
		WeekendBoost: 1.35, Noise: 0.06, Seed: seed, Scenarios: scenarios,
	}
}

// Load returns concurrent load as a fraction of PeakPlayers (can exceed 1).
func (c *Curve) Load(r Region, t time.Time) float64 {
	local := t.UTC().Add(time.Duration(r.UTCOffsetHours * float64(time.Hour)))
	h := float64(local.Hour()) + float64(local.Minute())/60 + float64(local.Second())/3600
	diurnal := math.Pow((1+math.Cos(2*math.Pi*(h-c.PeakLocalHour)/24))/2, c.Sharpness)
	load := c.TroughFrac + (1-c.TroughFrac)*diurnal
	if wd := local.Weekday(); wd == time.Saturday || wd == time.Sunday {
		load *= c.WeekendBoost
	}

	elapsed := t.Sub(c.Start).Hours()
	for _, s := range c.Scenarios {
		if s.Region != "" && s.Region != r.Name {
			continue
		}
		since := elapsed - s.StartHour
		switch s.Kind {
		case ScenarioPatch:
			if since >= 0 && since < s.DurationHours {
				load *= 1 + (s.Multiplier-1)*math.Exp(-since/(s.DurationHours/3))
			}
		case ScenarioTournament:
			if since >= 0 && since < s.DurationHours {
				load *= s.Multiplier
			}
		case ScenarioOutage:
			if since >= 0 && since < s.DurationHours {
				return 0
			}
			// Everyone who was kicked reconnects at once, decaying over ~10 sim minutes.
			if after := since - s.DurationHours; after >= 0 && after < 1 {
				load *= 1 + (s.Multiplier-1)*math.Exp(-after/(1.0/6))
			}
		}
	}

	if c.Noise > 0 {
		load *= 1 + c.Noise*c.noise(r.Name, t)
	}
	if load < 0 {
		return 0
	}
	return load
}

// Players returns the target concurrent player count for a region.
func (c *Curve) Players(r Region, t time.Time) int {
	return int(math.Round(float64(r.PeakPlayers) * c.Load(r, t)))
}

// noise is smooth value noise in [-1, 1] with 10-sim-minute knots.
func (c *Curve) noise(region string, t time.Time) float64 {
	m := float64(t.Unix()) / 600
	k := math.Floor(m)
	f := m - k
	f = f * f * (3 - 2*f) // smoothstep
	a, b := hashUnit(c.Seed, region, int64(k)), hashUnit(c.Seed, region, int64(k)+1)
	return a + (b-a)*f
}

func hashUnit(seed int64, s string, k int64) float64 {
	h := fnv.New64a()
	var buf [16]byte
	binary.LittleEndian.PutUint64(buf[:8], uint64(seed))
	binary.LittleEndian.PutUint64(buf[8:], uint64(k))
	h.Write(buf[:])
	h.Write([]byte(s))
	return float64(h.Sum64()%2_000_001)/1_000_000 - 1
}

// Clock maps wall time to simulated time, optionally compressed.
// Scale 24 means one real hour is one simulated day.
type Clock struct {
	SimStart  time.Time
	WallStart time.Time
	Scale     float64
}

// At returns the simulated time for a wall-clock instant.
func (c Clock) At(wall time.Time) time.Time {
	return c.SimStart.Add(time.Duration(float64(wall.Sub(c.WallStart)) * c.Scale))
}

// WeekScenarios is a preset week with one of each incident type, starting on
// a Monday 00:00 UTC.
func WeekScenarios() []Scenario {
	return []Scenario{
		// Tuesday 23:00 UTC (18:00 in NA-East): global patch drop.
		{Kind: ScenarioPatch, StartHour: 24*1 + 23, DurationHours: 4, Multiplier: 2.5},
		// Thursday 11:00 UTC (20:00 in Asia-East): regional outage at peak, 30 min.
		{Kind: ScenarioOutage, Region: "asia-east", StartHour: 24*3 + 11, DurationHours: 0.5, Multiplier: 3},
		// Saturday 17:00 UTC (18:00 in EU-West): tournament finals.
		{Kind: ScenarioTournament, Region: "eu-west", StartHour: 24*5 + 17, DurationHours: 4, Multiplier: 1.8},
	}
}

// DefaultRegions returns three regions with staggered peaks. scale multiplies
// the player counts, so scale=10 is ten times the traffic.
func DefaultRegions(scale float64) []Region {
	r := []Region{
		{Name: "na-east", UTCOffsetHours: -5, PeakPlayers: 2000, Servers: 20},
		{Name: "eu-west", UTCOffsetHours: 1, PeakPlayers: 2500, Servers: 20},
		{Name: "asia-east", UTCOffsetHours: 9, PeakPlayers: 1500, Servers: 15},
	}
	for i := range r {
		r[i].PeakPlayers = int(float64(r[i].PeakPlayers) * scale)
	}
	return r
}
