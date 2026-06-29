package tracker

import "testing"

// A sequence evicted from the reorder window while still unseen must be treated
// as only SUSPECTED lost: an at-least-once (re)delivery arriving later (Pub/Sub
// lease-sweeper redelivery / a nacked message returning below the watermark)
// recovers it as a distinct receive, not a duplicate, and clears the loss.
func TestSuspectedLossRecoveredIsNotLost(t *testing.T) {
	tr := New(4)
	const p = "p1"
	tr.Record(p, 1)
	tr.Record(p, 2)
	tr.Record(p, 3)
	// Jump far ahead so the window slides past seq 4 while it is still unseen.
	tr.Record(p, 9)
	if got := tr.TotalLost(); got != 1 {
		t.Fatalf("expected seq 4 suspected lost (1) after window slide, got %d", got)
	}
	// Late delivery of seq 4 must recover it -> not lost, not a duplicate.
	isDup, _ := tr.Record(p, 4)
	if isDup {
		t.Fatalf("recovered late delivery must not count as a duplicate")
	}
	if got := tr.TotalLost(); got != 0 {
		t.Fatalf("expected 0 loss after recovery, got %d", got)
	}
}

// A sequence evicted unseen and never (re)delivered is genuine loss and must
// still be reported.
func TestGenuineLossStaysLost(t *testing.T) {
	tr := New(4)
	const p = "p1"
	tr.Record(p, 1)
	tr.Record(p, 2)
	tr.Record(p, 3)
	tr.Record(p, 9) // evicts seq 4 unseen; never redelivered
	if got := tr.TotalLost(); got != 1 {
		t.Fatalf("expected genuine loss of 1, got %d", got)
	}
}

// Re-delivery of an already-seen sequence is still a real duplicate.
func TestTrueDuplicateStillCounts(t *testing.T) {
	tr := New(8)
	const p = "p1"
	tr.Record(p, 1)
	isDup, _ := tr.Record(p, 1)
	if !isDup {
		t.Fatalf("re-delivery of a seen seq must be a duplicate")
	}
	if got := tr.TotalLost(); got != 0 {
		t.Fatalf("expected 0 loss, got %d", got)
	}
	if got := tr.TotalDuplicates(); got != 1 {
		t.Fatalf("expected 1 duplicate, got %d", got)
	}
}
