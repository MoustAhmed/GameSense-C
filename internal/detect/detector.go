// Package detect implements streaming anti-cheat rules over player telemetry.
package detect

import (
	"math"
	"time"

	"gamesense/internal/event"
)

// Rule names.
const (
	RuleAimSnap   = "aim_snap"
	RuleSpeedHack = "speed_hack"
)

// Rules are the detection thresholds.
type Rules struct {
	MaxTurnRate       float64 // deg/s; humans peak around 450
	SnapsToFlag       int
	MaxSpeed          float64 // m/s; humans max out at 6.5
	SpeedEventsToFlag int
	IdleEviction      time.Duration
}

// DefaultRules are tuned so normal players are never flagged.
func DefaultRules() Rules {
	return Rules{MaxTurnRate: 550, SnapsToFlag: 3, MaxSpeed: 9, SpeedEventsToFlag: 5, IdleEviction: 5 * time.Minute}
}

type playerState struct {
	lastTS                int64
	x, y, yaw             float32
	snaps, speeds         int
	peakTurn, peakSpeed   float64
	flaggedAim, flaggedSp bool
	lastSeen              time.Time
}

// Detector keeps per-(match, player) state. It is not safe for concurrent use;
// each worker owns one, and partitioning by match ID guarantees each match is
// handled by exactly one worker at a time.
type Detector struct {
	rules   Rules
	players map[string]*playerState
	now     func() time.Time
}

// New creates a detector.
func New(r Rules) *Detector {
	return &Detector{rules: r, players: map[string]*playerState{}, now: time.Now}
}

// Tracked returns the number of players currently held in memory.
func (d *Detector) Tracked() int { return len(d.players) }

// Process consumes events in order and returns any new detections.
//
// Events whose timestamp is not newer than the player's last one are skipped.
// That makes redelivered duplicates (at-least-once delivery) free to ignore.
func (d *Detector) Process(events []event.Event) []event.Detection {
	var out []event.Detection
	now := d.now()
	for i := range events {
		e := &events[i]
		key := e.MatchID + "/" + e.PlayerID
		st := d.players[key]
		if e.Type == event.TypeMatchEnd {
			delete(d.players, key)
			continue
		}
		if st == nil {
			d.players[key] = &playerState{lastTS: e.TS, x: e.X, y: e.Y, yaw: e.Yaw, lastSeen: now}
			continue
		}
		if e.TS <= st.lastTS {
			continue // duplicate or out-of-order
		}
		dt := float64(e.TS-st.lastTS) / 1000

		if turn := math.Abs(angleDiff(float64(e.Yaw), float64(st.yaw))) / dt; turn > d.rules.MaxTurnRate {
			st.snaps++
			st.peakTurn = math.Max(st.peakTurn, turn)
			if st.snaps >= d.rules.SnapsToFlag && !st.flaggedAim {
				st.flaggedAim = true
				out = append(out, detection(RuleAimSnap, e, st.snaps, st.peakTurn))
			}
		}
		if speed := math.Hypot(float64(e.X-st.x), float64(e.Y-st.y)) / dt; speed > d.rules.MaxSpeed {
			st.speeds++
			st.peakSpeed = math.Max(st.peakSpeed, speed)
			if st.speeds >= d.rules.SpeedEventsToFlag && !st.flaggedSp {
				st.flaggedSp = true
				out = append(out, detection(RuleSpeedHack, e, st.speeds, st.peakSpeed))
			}
		}
		st.lastTS, st.x, st.y, st.yaw, st.lastSeen = e.TS, e.X, e.Y, e.Yaw, now
	}
	return out
}

// Sweep evicts players not seen recently (abandoned matches, or matches whose
// partition moved to another worker after a rebalance). It bounds memory.
func (d *Detector) Sweep() int {
	cutoff := d.now().Add(-d.rules.IdleEviction)
	n := 0
	for k, st := range d.players {
		if st.lastSeen.Before(cutoff) {
			delete(d.players, k)
			n++
		}
	}
	return n
}

func detection(rule string, e *event.Event, count int, peak float64) event.Detection {
	return event.Detection{
		ID: event.DetectionID(rule, e.MatchID, e.PlayerID), Rule: rule,
		PlayerID: e.PlayerID, MatchID: e.MatchID, Region: e.Region,
		Count: count, PeakValue: math.Round(peak*10) / 10, DetectedAt: e.TS,
	}
}

// angleDiff returns the signed smallest difference a-b in degrees, in [-180, 180).
func angleDiff(a, b float64) float64 {
	return math.Mod(a-b+540, 360) - 180
}
