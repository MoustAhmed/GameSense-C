package detect

import (
	"testing"
	"time"

	"gamesense/internal/event"
	"gamesense/internal/sim"
)

func ev(ts int64, x, y, yaw float32) event.Event {
	return event.Event{MatchID: "m1", PlayerID: "p1", Region: "r", Type: event.TypeTick, TS: ts, X: x, Y: y, Yaw: yaw}
}

func TestAimSnapFlagsAfterThreshold(t *testing.T) {
	d := New(DefaultRules())
	// 200 ms apart, 150 degree jumps = 750 deg/s.
	evs := []event.Event{ev(0, 0, 0, 0), ev(200, 0, 0, 150), ev(400, 0, 0, 300), ev(600, 0, 0, 90)}
	got := d.Process(evs)
	if len(got) != 1 || got[0].Rule != RuleAimSnap || got[0].ID != "aim_snap:m1:p1" {
		t.Fatalf("got %+v", got)
	}
	// Already flagged: more snaps must not emit again.
	if more := d.Process([]event.Event{ev(800, 0, 0, 250)}); len(more) != 0 {
		t.Fatalf("duplicate detection %+v", more)
	}
}

func TestWrapAroundIsNotASnap(t *testing.T) {
	d := New(DefaultRules())
	evs := []event.Event{ev(0, 0, 0, 359), ev(200, 0, 0, 1), ev(400, 0, 0, 358), ev(600, 0, 0, 2)}
	if got := d.Process(evs); len(got) != 0 {
		t.Fatalf("359->1 degrees is a 2 degree turn, got %+v", got)
	}
}

func TestDuplicatesAreIgnored(t *testing.T) {
	d := New(DefaultRules())
	a, b := ev(0, 0, 0, 0), ev(200, 0, 0, 150)
	// Redelivery replays b three times; it must count as one snap.
	if got := d.Process([]event.Event{a, b, b, b}); len(got) != 0 {
		t.Fatalf("duplicates were counted: %+v", got)
	}
}

func TestSpeedHack(t *testing.T) {
	d := New(DefaultRules())
	var evs []event.Event
	for i := 0; i < 7; i++ {
		evs = append(evs, ev(int64(i*200), float32(i)*4, 0, 0)) // 4 m per 0.2 s = 20 m/s
	}
	got := d.Process(evs)
	if len(got) != 1 || got[0].Rule != RuleSpeedHack {
		t.Fatalf("got %+v", got)
	}
}

// Run the real simulator through the detector: humans must never be flagged
// (precision 1.0) and most cheaters should be caught.
func TestDetectorOnSimulatedPlayers(t *testing.T) {
	const seed = 42
	d := New(DefaultRules())
	// Short matches + advancing sim time give lots of match turnover, so we
	// see thousands of distinct players (and dozens of cheaters).
	gs := sim.NewGameServer("gs1", "eu-west", 50000, seed, 5, 90*time.Second)
	now := time.Now()
	simNow := time.Date(2026, 11, 16, 0, 0, 0, 0, time.UTC)
	flagged := map[string]bool{}
	cheaters := map[string]bool{}
	for i := 0; i < 3000; i++ { // 5 wall minutes, 40 concurrent matches
		now = now.Add(100 * time.Millisecond)
		simNow = simNow.Add(250 * time.Millisecond) // a 90s match lasts ~36 wall seconds
		evs := gs.Step(now, simNow, 0.1, 40, nil)
		for _, e := range evs {
			if sim.CheatFor(seed, e.PlayerID) != sim.CheatNone {
				cheaters[e.PlayerID] = true
			}
		}
		for _, det := range d.Process(evs) {
			flagged[det.PlayerID] = true
		}
	}
	for p := range flagged {
		if sim.CheatFor(seed, p) == sim.CheatNone {
			t.Errorf("false positive: %s", p)
		}
	}
	if len(cheaters) == 0 {
		t.Fatal("no cheaters in sample; pick another seed")
	}
	if recall := float64(len(flagged)) / float64(len(cheaters)); recall < 0.7 {
		t.Fatalf("recall %.2f (%d/%d), want >= 0.7", recall, len(flagged), len(cheaters))
	}
}
