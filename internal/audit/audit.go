// Package audit independently verifies the pipeline: it proves no acknowledged
// event was lost, counts duplicates, and scores detector accuracy against the
// simulator's ground truth.
package audit

import (
	"sort"
	"sync"
	"time"

	"gamesense/internal/sim"
)

// Tracker records which sequence numbers have been seen per game server.
//
// Memory stays small: per server we keep the highest contiguous seq plus a
// sparse set of seqs seen above it (normally empty or tiny).
type Tracker struct {
	mu      sync.Mutex
	servers map[string]*serverTrack
	seen    uint64
	dups    uint64
}

type serverTrack struct {
	contig uint64
	above  map[uint64]struct{}
}

// NewTracker creates an empty tracker.
func NewTracker() *Tracker { return &Tracker{servers: map[string]*serverTrack{}} }

// Observe records one event and reports whether it was a duplicate.
func (t *Tracker) Observe(serverID string, seq uint64) (dup bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	st := t.servers[serverID]
	if st == nil {
		st = &serverTrack{above: map[uint64]struct{}{}}
		t.servers[serverID] = st
	}
	t.seen++
	if seq <= st.contig {
		t.dups++
		return true
	}
	if _, ok := st.above[seq]; ok {
		t.dups++
		return true
	}
	if seq == st.contig+1 {
		st.contig++
		for {
			if _, ok := st.above[st.contig+1]; !ok {
				break
			}
			delete(st.above, st.contig+1)
			st.contig++
		}
	} else {
		st.above[seq] = struct{}{}
	}
	return false
}

// Missing returns how many seqs in [1, ackedThrough] were never observed.
func (t *Tracker) Missing(serverID string, ackedThrough uint64) uint64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	st := t.servers[serverID]
	if st == nil {
		return ackedThrough
	}
	if ackedThrough <= st.contig {
		return 0
	}
	missing := ackedThrough - st.contig
	for s := range st.above {
		if s <= ackedThrough {
			missing--
		}
	}
	return missing
}

// Range is an inclusive span of sequence numbers.
type Range struct{ From, To uint64 }

// MissingRanges returns the gaps in [1, ackedThrough] that were never observed,
// in ascending order, at most limit of them (limit <= 0 means no limit).
func (t *Tracker) MissingRanges(serverID string, ackedThrough uint64, limit int) []Range {
	t.mu.Lock()
	defer t.mu.Unlock()
	st := t.servers[serverID]
	if st == nil {
		if ackedThrough == 0 {
			return nil
		}
		return []Range{{1, ackedThrough}}
	}
	if ackedThrough <= st.contig {
		return nil
	}
	seen := make([]uint64, 0, len(st.above))
	for s := range st.above {
		if s <= ackedThrough {
			seen = append(seen, s)
		}
	}
	sort.Slice(seen, func(i, j int) bool { return seen[i] < seen[j] })
	var out []Range
	next := st.contig + 1 // first seq not yet accounted for
	for _, s := range append(seen, ackedThrough+1) {
		if s > next {
			if limit > 0 && len(out) == limit {
				break
			}
			out = append(out, Range{next, s - 1})
		}
		next = s + 1
	}
	return out
}

// Totals returns events seen and duplicates.
func (t *Tracker) Totals() (seen, dups uint64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.seen, t.dups
}

// Scorer measures detector precision and recall against ground truth.
type Scorer struct {
	mu           sync.Mutex
	seed         int64
	cheatersSeen map[string]struct{} // cheaters observed in raw telemetry
	flagged      map[string]bool     // player -> is actually a cheater
	detectionIDs map[string]struct{}
	redeliveries uint64
}

// NewScorer uses the same seed as the simulator to recompute ground truth.
func NewScorer(seed int64) *Scorer {
	return &Scorer{seed: seed, cheatersSeen: map[string]struct{}{}, flagged: map[string]bool{}, detectionIDs: map[string]struct{}{}}
}

// ObservePlayer notes a player seen in raw telemetry.
func (s *Scorer) ObservePlayer(playerID string) {
	if sim.CheatFor(s.seed, playerID) == sim.CheatNone {
		return
	}
	s.mu.Lock()
	s.cheatersSeen[playerID] = struct{}{}
	s.mu.Unlock()
}

// ObserveDetection notes a detection. Repeated IDs are counted, not re-scored.
func (s *Scorer) ObserveDetection(id, playerID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.detectionIDs[id]; ok {
		s.redeliveries++
		return
	}
	s.detectionIDs[id] = struct{}{}
	s.flagged[playerID] = sim.CheatFor(s.seed, playerID) != sim.CheatNone
}

// Score summarizes detection quality.
type Score struct {
	Flagged, TruePositives, CheatersSeen int
	Precision, Recall                    float64
	Redeliveries                         uint64
}

// Score computes precision (flagged players who really cheat) and recall
// (cheaters seen in telemetry who got flagged).
func (s *Scorer) Score() Score {
	s.mu.Lock()
	defer s.mu.Unlock()
	sc := Score{Flagged: len(s.flagged), CheatersSeen: len(s.cheatersSeen), Redeliveries: s.redeliveries}
	for _, real := range s.flagged {
		if real {
			sc.TruePositives++
		}
	}
	if sc.Flagged > 0 {
		sc.Precision = float64(sc.TruePositives) / float64(sc.Flagged)
	}
	if sc.CheatersSeen > 0 {
		sc.Recall = float64(sc.TruePositives) / float64(sc.CheatersSeen)
	}
	return sc
}

// Watermark tracks, per partition, the newest record timestamp the auditor has
// read. Low() is the instant up to which it has caught up on every partition.
//
// A simulator snapshot taken at time T may only be checked for loss once
// Low() > T: otherwise a lagging auditor would report events it simply hasn't
// read yet as "lost".
type Watermark struct {
	mu    sync.Mutex
	parts map[int32]time.Time
}

// NewWatermark creates an empty watermark.
func NewWatermark() *Watermark { return &Watermark{parts: map[int32]time.Time{}} }

// Observe records a read from a partition.
func (w *Watermark) Observe(partition int32, ts time.Time) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if ts.After(w.parts[partition]) {
		w.parts[partition] = ts
	}
}

// Low returns the minimum per-partition timestamp (zero if nothing read yet).
func (w *Watermark) Low() time.Time {
	w.mu.Lock()
	defer w.mu.Unlock()
	var low time.Time
	for _, t := range w.parts {
		if low.IsZero() || t.Before(low) {
			low = t
		}
	}
	return low
}
