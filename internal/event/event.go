// Package event defines the telemetry schema shared by every service.
package event

import (
	"errors"
	"fmt"
	"strconv"
)

// Event types.
const (
	TypeTick     = "tick"      // periodic player state sample
	TypeShot     = "shot"      // player fired; Hit says whether it landed
	TypeMatchEnd = "match_end" // final event for a player in a match
)

// Event is one telemetry sample from a game server.
//
// (ServerID, Seq) is the idempotency key: every game server numbers its events
// 1, 2, 3, ... so any consumer can detect duplicates and gaps.
type Event struct {
	ServerID string  `json:"server_id"`
	Seq      uint64  `json:"seq"`
	Region   string  `json:"region"`
	MatchID  string  `json:"match_id"`
	PlayerID string  `json:"player_id"`
	Type     string  `json:"type"`
	TS       int64   `json:"ts"`     // wall-clock unix ms; drives physics and latency math
	SimTS    int64   `json:"sim_ts"` // simulated-world unix ms; drives the traffic curve
	X        float32 `json:"x"`
	Y        float32 `json:"y"`
	Yaw      float32 `json:"yaw"` // degrees [0, 360)
	Pitch    float32 `json:"pitch"`
	Hit      bool    `json:"hit,omitempty"`
	Won      bool    `json:"won,omitempty"`
}

// ID returns the globally unique idempotency key.
func (e *Event) ID() string { return e.ServerID + ":" + strconv.FormatUint(e.Seq, 10) }

// Batch is the unit a game server sends to the ingest API.
type Batch struct {
	ServerID string  `json:"server_id"`
	Events   []Event `json:"events"`
}

// Validate checks structural invariants. It does not trust the client.
func (b *Batch) Validate(maxEvents int) error {
	if b.ServerID == "" {
		return errors.New("server_id is required")
	}
	if len(b.Events) == 0 {
		return errors.New("batch has no events")
	}
	if len(b.Events) > maxEvents {
		return fmt.Errorf("batch has %d events, max %d", len(b.Events), maxEvents)
	}
	for i := range b.Events {
		e := &b.Events[i]
		switch {
		case e.ServerID != b.ServerID:
			return fmt.Errorf("event %d: server_id mismatch", i)
		case e.Seq == 0:
			return fmt.Errorf("event %d: seq must be > 0", i)
		case e.MatchID == "" || e.PlayerID == "" || e.Region == "":
			return fmt.Errorf("event %d: match_id, player_id and region are required", i)
		case e.Type != TypeTick && e.Type != TypeShot && e.Type != TypeMatchEnd:
			return fmt.Errorf("event %d: unknown type %q", i, e.Type)
		}
	}
	return nil
}

// Detection is emitted by the anti-cheat worker.
//
// ID is deterministic (rule:match:player), so a redelivered or replayed input
// produces the same ID and downstream sinks can upsert idempotently.
type Detection struct {
	ID         string  `json:"id"`
	Rule       string  `json:"rule"`
	PlayerID   string  `json:"player_id"`
	MatchID    string  `json:"match_id"`
	Region     string  `json:"region"`
	Count      int     `json:"count"`
	PeakValue  float64 `json:"peak_value"`
	DetectedAt int64   `json:"detected_at"`
}

// DetectionID builds the deterministic detection key.
func DetectionID(rule, matchID, playerID string) string {
	return rule + ":" + matchID + ":" + playerID
}
