package audit

import (
	"testing"
	"time"
)

func TestTrackerGapsAndDuplicates(t *testing.T) {
	tr := NewTracker()
	for _, s := range []uint64{1, 2, 4, 5, 2, 7} {
		tr.Observe("a", s)
	}
	if m := tr.Missing("a", 7); m != 2 { // 3 and 6
		t.Fatalf("missing=%d want 2", m)
	}
	if m := tr.Missing("a", 2); m != 0 {
		t.Fatalf("missing through 2 = %d want 0", m)
	}
	tr.Observe("a", 3)
	tr.Observe("a", 6)
	if m := tr.Missing("a", 7); m != 0 {
		t.Fatalf("missing=%d after filling gaps", m)
	}
	if seen, dups := tr.Totals(); seen != 8 || dups != 1 {
		t.Fatalf("seen=%d dups=%d", seen, dups)
	}
	if m := tr.Missing("never-seen", 5); m != 5 {
		t.Fatalf("unknown server missing=%d want 5", m)
	}
}

func TestTrackerMissingRanges(t *testing.T) {
	tr := NewTracker()
	// seen: 1-2, 5, 8-9, 12 -> gaps 3-4, 6-7, 10-11, (13-15 if acked through 15)
	for _, s := range []uint64{1, 2, 5, 8, 9, 12, 5} {
		tr.Observe("a", s)
	}
	eq := func(got, want []Range) {
		t.Helper()
		if len(got) != len(want) {
			t.Fatalf("got %v want %v", got, want)
		}
		for i := range got {
			if got[i] != want[i] {
				t.Fatalf("got %v want %v", got, want)
			}
		}
	}
	eq(tr.MissingRanges("a", 15, 0), []Range{{3, 4}, {6, 7}, {10, 11}, {13, 15}})
	eq(tr.MissingRanges("a", 12, 0), []Range{{3, 4}, {6, 7}, {10, 11}})
	eq(tr.MissingRanges("a", 9, 0), []Range{{3, 4}, {6, 7}}) // seqs above ackedThrough are ignored
	eq(tr.MissingRanges("a", 15, 2), []Range{{3, 4}, {6, 7}})
	eq(tr.MissingRanges("a", 2, 0), nil)
	eq(tr.MissingRanges("never-seen", 5, 0), []Range{{1, 5}})
	eq(tr.MissingRanges("never-seen", 0, 0), nil)

	// The ranges must account for exactly what Missing counts.
	var n uint64
	for _, r := range tr.MissingRanges("a", 15, 0) {
		n += r.To - r.From + 1
	}
	if m := tr.Missing("a", 15); m != n {
		t.Fatalf("ranges cover %d, Missing=%d", n, m)
	}
}

func TestWatermarkIsSlowestPartition(t *testing.T) {
	w := NewWatermark()
	base := time.Unix(1000, 0)
	w.Observe(0, base.Add(10*time.Second))
	w.Observe(1, base.Add(3*time.Second))
	w.Observe(1, base.Add(2*time.Second)) // older read never moves it back
	if got := w.Low(); !got.Equal(base.Add(3 * time.Second)) {
		t.Fatalf("low=%v", got)
	}
}
