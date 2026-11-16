package sim

import (
	"testing"
	"time"

	"gamesense/internal/event"
)

var monday = time.Date(2026, 11, 16, 0, 0, 0, 0, time.UTC)

func TestCurvePeaksInLocalEvening(t *testing.T) {
	c := DefaultCurve(monday, 1, nil)
	c.Noise = 0
	na := Region{Name: "na-east", UTCOffsetHours: -5, PeakPlayers: 1000}
	// 20:30 local in UTC-5 is 01:30 UTC next day; 08:30 local (12h later) is 13:30 UTC.
	peak := c.Players(na, monday.Add(25*time.Hour+30*time.Minute))
	trough := c.Players(na, monday.Add(37*time.Hour+30*time.Minute))
	if peak < 950 || trough > 160 {
		t.Fatalf("peak=%d trough=%d; want peak~1000, trough~150", peak, trough)
	}
}

func TestWeekendBoostAndScenarios(t *testing.T) {
	r := Region{Name: "asia-east", UTCOffsetHours: 9, PeakPlayers: 1000}
	c := DefaultCurve(monday, 1, WeekScenarios())
	c.Noise = 0
	weekday := c.Load(r, monday.Add(11*time.Hour+30*time.Minute)) // Mon 20:30 JST
	saturday := c.Load(r, monday.Add(5*24*time.Hour+11*time.Hour+30*time.Minute))
	if saturday <= weekday*1.3 {
		t.Fatalf("weekend %.2f should be ~1.35x weekday %.2f", saturday, weekday)
	}
	outage := monday.Add((24*3 + 11) * time.Hour).Add(10 * time.Minute)
	if got := c.Load(r, outage); got != 0 {
		t.Fatalf("load during outage = %.2f, want 0", got)
	}
	surge := c.Load(r, outage.Add(21*time.Minute)) // just after the 30 min outage
	if surge < 2 {
		t.Fatalf("reconnect surge = %.2f, want > 2x", surge)
	}
}

func TestGameServerSeqIsContiguousAndRateIsRight(t *testing.T) {
	gs := NewGameServer("gs1", "na-east", 1000, 7, 5, 25*time.Minute)
	now := time.Now()
	var evs []event.Event
	for i := 0; i < 100; i++ { // 10 wall seconds
		now = now.Add(100 * time.Millisecond)
		evs = gs.Step(now, monday, 0.1, 3, evs)
	}
	for i, e := range evs {
		if e.Seq != uint64(i+1) {
			t.Fatalf("event %d has seq %d", i, e.Seq)
		}
	}
	want := 3 * PlayersPerMatch * 5 * 10 // matches * players * eps * seconds
	if n := len(evs); n < want*9/10 || n > want*11/10 {
		t.Fatalf("generated %d events, want ~%d", n, want)
	}
}
