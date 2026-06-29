// Package tracker tracks message sequences per producer (worker-id keyed) using
// a bitset window, detecting gaps (loss), duplicates, and out-of-order delivery.
// Thread-safe for concurrent access from multiple consumer goroutines. Lifted
// 1:1 (transport-agnostic) from the kubemq-aws / kubemq-cloud-events /
// kubemq-amqp-rabbitmq burnin tracker.
//
// Pub/Sub adaptation: the per-message monotonic broker sequence carried in the
// emulator's ack_id (base64(JSON{...,seq,...})) — equivalently the ordered
// position implied by the server-assigned _pubsub_message_id — is the `seq`
// recorded here, and the publishing worker/channel is the `producerID`. Pub/Sub
// is an at-least-once transport: ModifyAckDeadline(0) nacks for redelivery and
// the lease sweeper redelivers overdue messages, so the SAME seq can arrive more
// than once and a late redelivery can arrive AFTER the reorder window has slid
// past it. The pendingLost reconciliation below is exactly what stops such a
// late at-least-once redelivery from being false-counted as loss.
package tracker

import (
	"sync"
	"sync/atomic"
)

// Tracker tracks per-producer sequence integrity within a reorder window.
type Tracker struct {
	mu            sync.Mutex
	reorderWindow int
	producers     map[string]*producerState
}

type producerState struct {
	highContiguous   uint64
	window           []uint64 // bitset of seen offsets above highContiguous
	received         atomic.Uint64
	duplicates       atomic.Uint64
	outOfOrder       atomic.Uint64
	confirmedLost    atomic.Uint64
	pendingLost      map[uint64]struct{}
	lastReportedLost uint64
	lastSeen         uint64
}

// ProducerStats holds aggregate stats for a single producer.
type ProducerStats struct {
	Received      uint64
	Duplicates    uint64
	OutOfOrder    uint64
	ConfirmedLost uint64
}

// New creates a tracker with the given reorder window size (in sequences).
func New(reorderWindow int) *Tracker {
	return &Tracker{
		reorderWindow: reorderWindow,
		producers:     make(map[string]*producerState),
	}
}

// Record records a received sequence for the given producer and returns whether
// it was a duplicate and/or out of order.
func (t *Tracker) Record(producerID string, seq uint64) (isDuplicate bool, isOutOfOrder bool) {
	t.mu.Lock()
	defer t.mu.Unlock()

	ps, ok := t.producers[producerID]
	if !ok {
		ps = &producerState{
			highContiguous: seq - 1,
			window:         make([]uint64, (t.reorderWindow+63)/64),
			lastSeen:       seq,
		}
		t.producers[producerID] = ps
		setBit(ps.window, 0)
		ps.received.Add(1)
		t.advanceContiguous(ps)
		return false, false
	}

	ps.received.Add(1)

	if seq < ps.lastSeen {
		isOutOfOrder = true
		ps.outOfOrder.Add(1)
	}
	ps.lastSeen = seq

	if seq <= ps.highContiguous {
		// A sequence at or below the contiguous watermark is normally a duplicate.
		// But if it was previously evicted from the reorder window as a SUSPECTED
		// loss, this arrival is a late (re)delivery that recovers it: credit it as a
		// distinct receive (not a duplicate) and clear the pending loss.
		if _, pending := ps.pendingLost[seq]; pending {
			delete(ps.pendingLost, seq)
			ps.confirmedLost.Store(uint64(len(ps.pendingLost)))
			return false, isOutOfOrder
		}
		ps.duplicates.Add(1)
		return true, isOutOfOrder
	}

	offset := seq - ps.highContiguous - 1
	if offset >= uint64(t.reorderWindow) {
		t.slideWindow(ps, seq)
		return false, isOutOfOrder
	}

	if getBit(ps.window, int(offset)) {
		ps.duplicates.Add(1)
		return true, isOutOfOrder
	}

	setBit(ps.window, int(offset))
	t.advanceContiguous(ps)
	return false, isOutOfOrder
}

// DetectGaps returns newly confirmed lost counts (deltas) per producer since the
// previous call.
func (t *Tracker) DetectGaps() map[string]uint64 {
	t.mu.Lock()
	defer t.mu.Unlock()

	deltas := make(map[string]uint64)
	for id, ps := range t.producers {
		current := ps.confirmedLost.Load()
		// confirmedLost can decrease when a suspected loss is later recovered, so
		// only stream positive deltas to the live metric (the authoritative final
		// loss is read from TotalLost at snapshot) and guard against underflow.
		if current > ps.lastReportedLost {
			deltas[id] = current - ps.lastReportedLost
		}
		ps.lastReportedLost = current
	}
	return deltas
}

// Reset clears all tracking state (used after warmup).
func (t *Tracker) Reset() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.producers = make(map[string]*producerState)
}

// Stats returns aggregate stats per producer.
func (t *Tracker) Stats() map[string]ProducerStats {
	t.mu.Lock()
	defer t.mu.Unlock()

	result := make(map[string]ProducerStats, len(t.producers))
	for pid, ps := range t.producers {
		result[pid] = ProducerStats{
			Received:      ps.received.Load(),
			Duplicates:    ps.duplicates.Load(),
			OutOfOrder:    ps.outOfOrder.Load(),
			ConfirmedLost: ps.confirmedLost.Load(),
		}
	}
	return result
}

// TotalReceived returns total received across all producers.
func (t *Tracker) TotalReceived() uint64 {
	return t.sum(func(p *producerState) uint64 { return p.received.Load() })
}

// TotalDuplicates returns total duplicates across all producers.
func (t *Tracker) TotalDuplicates() uint64 {
	return t.sum(func(p *producerState) uint64 { return p.duplicates.Load() })
}

// TotalLost returns total confirmed lost across all producers.
func (t *Tracker) TotalLost() uint64 {
	return t.sum(func(p *producerState) uint64 { return p.confirmedLost.Load() })
}

// TotalOutOfOrder returns total out-of-order across all producers.
func (t *Tracker) TotalOutOfOrder() uint64 {
	return t.sum(func(p *producerState) uint64 { return p.outOfOrder.Load() })
}

func (t *Tracker) sum(f func(*producerState) uint64) uint64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	var total uint64
	for _, ps := range t.producers {
		total += f(ps)
	}
	return total
}

func (t *Tracker) advanceContiguous(ps *producerState) {
	for getBit(ps.window, 0) {
		ps.highContiguous++
		shiftLeft(ps.window)
	}
}

func (t *Tracker) slideWindow(ps *producerState, newSeq uint64) {
	newHigh := newSeq - uint64(t.reorderWindow) - 1
	if newHigh <= ps.highContiguous {
		offset := newSeq - ps.highContiguous - 1
		if int(offset) < t.reorderWindow {
			setBit(ps.window, int(offset))
		}
		return
	}

	slideDist := newHigh - ps.highContiguous
	if slideDist > uint64(t.reorderWindow) {
		slideDist = uint64(t.reorderWindow)
	}

	// Sequences evicted from the window without ever being seen are only
	// SUSPECTED lost: at-least-once transports (Pub/Sub redelivery via the lease
	// sweeper / ModifyAckDeadline(0) nacks) may still (re)deliver them later,
	// arriving below the watermark. Record them as pending and reconcile in
	// Record (recovered on late delivery); the surviving pending count is the
	// true loss. This avoids false positives when a slow or redelivering
	// consumer falls more than reorder_window behind the producer.
	for i := uint64(0); i < slideDist; i++ {
		if !getBit(ps.window, int(i)) {
			if ps.pendingLost == nil {
				ps.pendingLost = make(map[uint64]struct{})
			}
			ps.pendingLost[ps.highContiguous+1+i] = struct{}{}
		}
	}
	ps.confirmedLost.Store(uint64(len(ps.pendingLost)))

	for i := uint64(0); i < slideDist; i++ {
		shiftLeft(ps.window)
	}
	ps.highContiguous = newHigh

	offset := newSeq - ps.highContiguous - 1
	if int(offset) < t.reorderWindow {
		setBit(ps.window, int(offset))
	}
	t.advanceContiguous(ps)
}

func setBit(window []uint64, pos int) {
	if pos < 0 || pos >= len(window)*64 {
		return
	}
	window[pos/64] |= 1 << (uint(pos) % 64)
}

func getBit(window []uint64, pos int) bool {
	if pos < 0 || pos >= len(window)*64 {
		return false
	}
	return window[pos/64]&(1<<(uint(pos)%64)) != 0
}

func shiftLeft(window []uint64) {
	for i := 0; i < len(window); i++ {
		window[i] >>= 1
		if i+1 < len(window) {
			window[i] |= (window[i+1] & 1) << 63
		}
	}
}
