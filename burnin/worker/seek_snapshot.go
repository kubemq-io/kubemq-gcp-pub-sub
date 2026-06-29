package worker

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"cloud.google.com/go/pubsub"
	vkit "cloud.google.com/go/pubsub/apiv1"
	"cloud.google.com/go/pubsub/apiv1/pubsubpb"

	"github.com/kubemq-io/kubemq-gcp-pub-sub/burnin/config"
	"github.com/kubemq-io/kubemq-gcp-pub-sub/burnin/payload"
	"github.com/kubemq-io/kubemq-gcp-pub-sub/burnin/transport"
)

// SeekSnapshotWorker (NEW, spec REQ-005/S5.5, opt-in) drives a deterministic
// round structure that asserts seek/snapshot replay (S3.5): each round publishes
// a baseline batch (acked, drained), takes a snapshot (or records t0), publishes
// a replay set R of size seek_replay_count (acked, drained), then
// SeekToSnapshot / SeekToTime and COUNTS how many messages redeliver — expecting
// replayCount within a small tolerance. A replay-count mismatch beyond tolerance
// hard-fails the seek_replay_mismatch gate (verdict.go:162). It cleans up its
// snapshot (DeleteSnapshot) at the end of each round.
//
// CRITICAL (REQ-019/S5.5, I-3): because replay deliberately redelivers acked
// messages, the worker scopes accounting per round using round-scoped
// producerIDs (seek-r<round>-<idx>) — a replayed seq from round N belongs to a
// different producer than round N+1 and never collides as a cross-round
// duplicate/loss. Per-round seqs are monotonic and contiguous (start at 1) so the
// tracker's contiguous watermark is clean and a replayed seq recovers from
// pendingLost rather than false-counting loss. The worker MUST NOT call
// w.trk.Reset(): Reset() wipes ALL producer state on the shared tracker (erasing
// real in-round loss the engine snapshots at drain and data-racing the engine
// goroutine that reads tracker.TotalLost()); it is reserved for the engine's
// warmup boundary (BaseWorker.ResetAfterWarmup, worker.go:576). The throughput
// and dup gates are exempted for this round-structured, at-least-once worker
// (verdict.go:55-57,83-84, config.IsSeekSnapshotWorker); the LOSS gate still
// applies per round-scoped producer.
type SeekSnapshotWorker struct {
	*BaseWorker
	topicID    string
	subID      string
	snapshotID string

	client    *pubsub.Client
	topic     *pubsub.Topic
	sub       *pubsub.Subscription
	subClient *vkit.SubscriberClient
	subPath   string
}

// NewSeekSnapshotWorker creates a seek_snapshot worker.
func NewSeekSnapshotWorker(cfg *config.Config, idx int, logger *slog.Logger) Worker {
	topicID := transport.TopicName(config.WorkerSeekSnapshot, idx)
	subID := transport.SubscriptionName(config.WorkerSeekSnapshot, idx)
	snapshotID := transport.SnapshotName(config.WorkerSeekSnapshot, idx)
	return &SeekSnapshotWorker{
		BaseWorker: NewBaseWorker(config.WorkerSeekSnapshot,
			transport.MappedTopicChannel(topicID), idx, cfg, logger),
		topicID:    topicID,
		subID:      subID,
		snapshotID: snapshotID,
	}
}

// Start builds the Pub/Sub client, provisions the topic + subscription, opens a
// low-level apiv1 subscriber for deterministic round draining, and signals
// consumer-ready. There is NO standing Receive loop: the round driver
// (StartProducers) publishes, drains, seeks, and counts replays itself, so the
// high-level auto-extending Receive loop (which would auto-ack/auto-extend leases
// and blur the replay count) is deliberately avoided — draining uses the
// low-level Pull (the dead_letter.go:172 pattern).
func (w *SeekSnapshotWorker) Start(ctx context.Context) error {
	w.consumerCtx, w.consumerCancel = context.WithCancel(ctx)

	client, err := transport.NewPubSubClient(ctx, w.psCfg)
	if err != nil {
		return fmt.Errorf("build pubsub client: %w", err)
	}
	w.client = client

	if w.topic, err = ensureTopic(ctx, client, w.topicID); err != nil {
		return err
	}
	w.sub, err = ensureSubscription(ctx, client, w.subID, pubsub.SubscriptionConfig{
		Topic:       w.topic,
		AckDeadline: transport.AckDeadline(w.cfg.GCP.AckDeadlineSeconds),
	})
	if err != nil {
		return err
	}

	// Low-level subscriber for deterministic Pull-based draining (the high-level
	// Receive loop auto-extends leases and acks asynchronously, which would make
	// the post-seek replay count nondeterministic).
	w.subClient, err = vkit.NewSubscriberClient(ctx, emulatorClientOptions(w.psCfg.EmulatorHost)...)
	if err != nil {
		return fmt.Errorf("build apiv1 subscriber client: %w", err)
	}
	w.subPath = fmt.Sprintf("projects/%s/subscriptions/%s", w.psCfg.ProjectID, w.subID)

	w.signalReady()
	return nil
}

// StartProducers launches the single round-driver goroutine (measurement
// window). The driver runs publish→snapshot→publish→seek→count-replay rounds
// until producerCtx is cancelled by StopProducers; it is tracked by producerWG.
func (w *SeekSnapshotWorker) StartProducers() {
	w.producerCtx, w.producerCancel = context.WithCancel(context.Background())
	w.producerWG.Add(1)
	go func() {
		defer w.producerWG.Done()
		w.roundLoop(w.producerCtx)
	}()
}

// replayCount returns the configured size of the replay set R (gcp.seek_replay_count,
// default 500 — well below the connector's MaxSeekReplay 1,000,000 cap, S3.5/A3,
// so replay truncation cannot occur).
func (w *SeekSnapshotWorker) replayCount() int {
	n := w.cfg.GCP.SeekReplayCount
	if n < 1 {
		n = 1
	}
	return n
}

// baselineCount is the size of the pre-snapshot baseline batch B. A small batch
// suffices to advance the topic log past the snapshot cursor; the assertion is on
// the post-snapshot replay set R, not B.
func (w *SeekSnapshotWorker) baselineCount() int {
	n := w.replayCount() / 10
	if n < 5 {
		n = 5
	}
	return n
}

// roundLoop runs the deterministic seek/snapshot rounds. Each round uses a fresh
// round-scoped producerID so replayed seqs never collide across rounds (S5.5).
func (w *SeekSnapshotWorker) roundLoop(ctx context.Context) {
	for round := 1; ; round++ {
		if ctx.Err() != nil {
			return
		}
		useTime := round%2 == 0 // alternate SeekToSnapshot / SeekToTime for coverage
		if err := w.runRound(ctx, round, useTime); err != nil {
			if ctx.Err() != nil {
				return
			}
			w.logger.Warn("seek round failed", "round", round, "error", errString(err))
			// Back off briefly so a transient connector error does not spin.
			if !sleepCtx(ctx, reconnectBackoff(w.cfg)) {
				return
			}
		}
	}
}

// runRound executes one publish→snapshot→publish→seek→count-replay cycle.
//
//  1. publish baseline batch B (round-scoped seq 1..B), drain + ack;
//  2. take snapshot S (cursor = topic log's last retained seq = last B msg,
//     subscriber.go:921/snapshotCursor) — or record t0 for SeekToTime;
//  3. publish replay set R (round-scoped seq B+1..B+R), drain + ack — each
//     recordTracked so pendingLost stays correct;
//  4. SeekToSnapshot(S) (or SeekToTime(t0)): the connector purges the sub queue,
//     drops leases, and re-fans the topic log forward from the resolved sequence
//     (seek.go Seek/resolveSeekTarget/refanout);
//  5. drain again and COUNT redeliveries (SeekReplayObserved), expecting R.
func (w *SeekSnapshotWorker) runRound(ctx context.Context, round int, useTime bool) error {
	producerID := fmt.Sprintf("seek-r%d-%d", round, w.channelIndex)
	baseline := w.baselineCount()
	replay := w.replayCount()
	var seq uint64

	// 1. Baseline batch — published, drained, acked. These seqs advance the topic
	// log so the snapshot cursor sits past them.
	for i := 0; i < baseline; i++ {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		seq++
		if err := w.publishOne(ctx, producerID, seq); err != nil {
			return fmt.Errorf("baseline publish: %w", err)
		}
	}
	if _, err := w.drainAck(ctx, producerID, baseline); err != nil {
		return fmt.Errorf("baseline drain: %w", err)
	}

	// 2. Snapshot / time cursor. Wait until producerCtx is still live; record t0
	// just before the replay set so SeekToTime resolves to the replay-set start
	// (the connector clamps a before-window instant — never an error, S3.5).
	var snap *pubsub.SnapshotConfig
	var t0 time.Time
	if useTime {
		t0 = time.Now()
		// A short beat so the replay set's publish timestamps are strictly after t0
		// (the connector resolves the first ES sequence at/after the instant).
		if !sleepCtx(ctx, 50*time.Millisecond) {
			return ctx.Err()
		}
	} else {
		// CreateSnapshot is NOT idempotent (the connector returns AlreadyExists,
		// subscriber.go), so clear any snapshot left by a prior round that failed
		// after create but before its own cleanup (DeleteSnapshot tolerates a miss).
		if derr := transport.DeleteSnapshot(ctx, w.client, w.snapshotID); derr != nil {
			w.logger.Debug("pre-create snapshot cleanup", "snapshot", w.snapshotID, "error", errString(derr))
		}
		cfg, err := transport.CreateSnapshot(ctx, w.sub, w.snapshotID)
		if err != nil {
			return fmt.Errorf("create snapshot: %w", err)
		}
		snap = cfg
		// Incidental control RPC (allowed S1.3 exception): confirm the snapshot is
		// visible before seeking to it. A miss is logged, not fatal.
		if _, gerr := transport.GetSnapshot(ctx, w.client, w.snapshotID); gerr != nil {
			w.logger.Warn("snapshot not yet visible via ListSnapshots", "snapshot", w.snapshotID, "error", errString(gerr))
		}
	}

	// 3. Replay set R — published, drained, acked (each recordTracked so a later
	// replayed seq recovers from pendingLost rather than false-counting loss).
	for i := 0; i < replay; i++ {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		seq++
		if err := w.publishOne(ctx, producerID, seq); err != nil {
			return fmt.Errorf("replay-set publish: %w", err)
		}
	}
	if _, err := w.drainAck(ctx, producerID, replay); err != nil {
		return fmt.Errorf("replay-set drain: %w", err)
	}

	// 4. Seek back to the cursor (snapshot or time).
	if useTime {
		if err := transport.SeekToTime(ctx, w.sub, t0); err != nil {
			return fmt.Errorf("seek to time: %w", err)
		}
	} else {
		if err := transport.SeekToSnapshot(ctx, w.sub, snap.Snapshot); err != nil {
			return fmt.Errorf("seek to snapshot: %w", err)
		}
	}

	// 5. Drain the replay and count it. recordTracked credits each replayed seq as
	// a recovery (below the round producer's watermark) — the dup gate is exempted
	// for this worker (S5.5) so the bounded replay duplication does not fail.
	observed, err := w.drainReplay(ctx, producerID)
	if err != nil {
		return fmt.Errorf("replay drain: %w", err)
	}

	w.recordSeekReplayExpected(uint64(replay))
	w.recordSeekReplayObserved(observed)
	w.assertReplayCount(round, useTime, uint64(replay), observed)

	// Cleanup the snapshot so each round starts fresh and the prefix-scoped
	// snapshot id is reusable (DeleteSnapshot tolerates a missing snapshot).
	if !useTime {
		if derr := transport.DeleteSnapshot(ctx, w.client, w.snapshotID); derr != nil {
			w.logger.Warn("delete snapshot failed", "snapshot", w.snapshotID, "error", errString(derr))
		}
	}
	return nil
}

// assertReplayCount classifies the observed-vs-expected replay count (S5.10,
// REQ-005 — DETERMINISTIC HARD-FAIL). The connector's snapshot cursor is the
// topic log's last RETAINED sequence (subscriber.go snapshotCursor) and the
// replay range is INCLUSIVE of that cursor (array RangeEventsStore: fromSeq=L →
// replays L,L+1,…), so the last baseline message re-delivers alongside the R
// replay-set messages — a deterministic +1 boundary. The tolerance absorbs that
// boundary plus a small at-least-once / reorder slack (boundaryLossPct % of R,
// floored at a few messages). A mismatch BEYOND tolerance is a real replay defect
// and increments SeekReplayMismatch (→ verdict.go:162 fail).
func (w *SeekSnapshotWorker) assertReplayCount(round int, useTime bool, expected, observed uint64) {
	tol := uint64(float64(expected)*seekReplayBoundaryPct/100.0) + 2 // +2 covers the inclusive-cursor boundary message
	var diff uint64
	if observed >= expected {
		diff = observed - expected
	} else {
		diff = expected - observed
	}
	mode := "snapshot"
	if useTime {
		mode = "time"
	}
	if diff > tol {
		w.recordSeekReplayMismatch()
		w.logger.Warn("seek replay count mismatch",
			"round", round, "mode", mode, "expected", expected, "observed", observed, "tolerance", tol)
		return
	}
	w.logger.Debug("seek replay count matched",
		"round", round, "mode", mode, "expected", expected, "observed", observed, "diff", diff)
}

// publishOne publishes a single round-scoped instrumented message and records the
// send. The seq is the per-round monotonic position so the round producer's
// tracker window stays contiguous.
func (w *SeekSnapshotWorker) publishOne(ctx context.Context, producerID string, seq uint64) error {
	if err := w.waitForRate(ctx); err != nil {
		return err
	}
	body, crcHex := payload.Build(w.selectMessageSize())
	start := time.Now()
	if _, err := transport.PublishSync(ctx, w.topic, body, stampAttrs(producerID, seq, crcHex)); err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		w.recordError("publish_failure")
		return err
	}
	metricObserveSend(w.name, time.Since(start))
	w.recordSent(len(body), producerID)
	return nil
}

// drainAck pulls and ACKS up to want messages from the subscription via the
// low-level Pull, recording each as received + tracked + acked. It returns once
// want messages have been acked OR the subscription goes quiescent (a bounded
// number of empty polls). The pre-seek baseline/replay-set drains MUST ack so the
// subscription queue is empty before the seek re-fans the topic log (seek.go
// purges the queue regardless, but draining first keeps the steady-state receive
// counts and pendingLost reconciliation accurate).
func (w *SeekSnapshotWorker) drainAck(ctx context.Context, producerID string, want int) (uint64, error) {
	var got uint64
	emptyPolls := 0
	for got < uint64(want) {
		if ctx.Err() != nil {
			return got, ctx.Err()
		}
		msgs, ackIDs, err := w.pullOnce(ctx)
		if err != nil {
			return got, err
		}
		if len(msgs) == 0 {
			emptyPolls++
			if emptyPolls > drainQuiescentPolls {
				return got, nil
			}
			if !sleepCtx(ctx, drainPollGap) {
				return got, ctx.Err()
			}
			continue
		}
		emptyPolls = 0
		for _, rm := range msgs {
			w.consumeReceived(rm, producerID, "c-drain-"+w.channelName)
			got++
		}
		if err := w.ack(ctx, ackIDs); err != nil {
			return got, err
		}
	}
	return got, nil
}

// drainReplay pulls and ACKS every redelivered message after a seek, counting
// them (SeekReplayObserved). It drains until the subscription goes quiescent
// (the replay is finite and bounded by R + the inclusive-cursor boundary). Each
// replayed seq is recordTracked — below the round producer's contiguous
// watermark it recovers from pendingLost (no false loss); a genuine bounded
// duplicate is exempted from the dup gate for this worker (S5.5).
func (w *SeekSnapshotWorker) drainReplay(ctx context.Context, producerID string) (uint64, error) {
	var observed uint64
	emptyPolls := 0
	for {
		if ctx.Err() != nil {
			return observed, ctx.Err()
		}
		msgs, ackIDs, err := w.pullOnce(ctx)
		if err != nil {
			return observed, err
		}
		if len(msgs) == 0 {
			emptyPolls++
			if emptyPolls > drainQuiescentPolls {
				return observed, nil
			}
			if !sleepCtx(ctx, drainPollGap) {
				return observed, ctx.Err()
			}
			continue
		}
		emptyPolls = 0
		for _, rm := range msgs {
			w.consumeReceived(rm, producerID, "c-replay-"+w.channelName)
			observed++
		}
		if err := w.ack(ctx, ackIDs); err != nil {
			return observed, err
		}
	}
}

// pullOnce issues one low-level Pull and returns the received messages plus their
// ack ids. A context cancellation surfaces as a non-nil error the caller checks
// against ctx.Err(); a transient error is counted and surfaced so the caller can
// back off.
func (w *SeekSnapshotWorker) pullOnce(ctx context.Context) ([]*pubsubpb.ReceivedMessage, []string, error) {
	resp, err := w.subClient.Pull(ctx, &pubsubpb.PullRequest{
		Subscription: w.subPath,
		MaxMessages:  drainPullBatch,
	})
	if err != nil {
		if ctx.Err() != nil {
			return nil, nil, ctx.Err()
		}
		w.recordError("receive_failure")
		return nil, nil, err
	}
	rms := resp.GetReceivedMessages()
	ackIDs := make([]string, 0, len(rms))
	for _, rm := range rms {
		ackIDs = append(ackIDs, rm.GetAckId())
	}
	return rms, ackIDs, nil
}

// consumeReceived records one low-level received message into the metrics +
// tracker, verifying its CRC and reconciling its round-scoped (producerID, seq).
// The producerID stamped in the attributes is the round producer; recordTracked
// against it keeps pendingLost correct for the replay reconciliation.
func (w *SeekSnapshotWorker) consumeReceived(rm *pubsubpb.ReceivedMessage, expectedProducer, consumerID string) {
	msg := rm.GetMessage()
	body := msg.GetData()
	w.recordReceived(len(body), consumerID)

	producerID, seq, crcHex, sentAt, ok := extractMeta(msg.GetAttributes())
	if !ok {
		return
	}
	if crcHex != "" && !payload.VerifyCRC(body, crcHex) {
		w.recordCorrupted()
	}
	if !sentAt.IsZero() {
		w.recordLatency(time.Since(sentAt))
	}
	// Defensive: only reconcile messages from the current round producer. A stray
	// message from a prior round (should not happen — seek purges the queue) is
	// counted as received but not tracked against this round's producer.
	if producerID == expectedProducer {
		w.recordTracked(producerID, seq)
	}
}

// ack acknowledges a batch of ack ids via the low-level Acknowledge and records
// the acks. An empty batch is a no-op. A context cancellation is surfaced; a
// transient ack error is counted and returned so the caller backs off.
func (w *SeekSnapshotWorker) ack(ctx context.Context, ackIDs []string) error {
	if len(ackIDs) == 0 {
		return nil
	}
	err := w.subClient.Acknowledge(ctx, &pubsubpb.AcknowledgeRequest{
		Subscription: w.subPath,
		AckIds:       ackIDs,
	})
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		w.recordError("ack_failure")
		return err
	}
	for range ackIDs {
		w.recordAcked()
	}
	return nil
}

// StopConsumers stops the round driver, closes the low-level subscriber, cleans
// up any lingering snapshot, and closes the Pub/Sub client. (StopProducers has
// already cancelled the round driver and waited on producerWG by the time the
// engine calls StopConsumers, but StopConsumers also drives the BaseWorker
// consumer teardown for symmetry with the other workers.)
func (w *SeekSnapshotWorker) StopConsumers() {
	w.BaseWorker.StopConsumers()
	if w.client != nil {
		// Best-effort: remove the snapshot left by the last snapshot-mode round so a
		// re-run starts clean (the per-round path already deletes its own snapshot).
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		if derr := transport.DeleteSnapshot(cleanupCtx, w.client, w.snapshotID); derr != nil {
			w.logger.Debug("snapshot cleanup at stop", "snapshot", w.snapshotID, "error", errString(derr))
		}
		cancel()
	}
	if w.subClient != nil {
		_ = w.subClient.Close()
	}
	if w.topic != nil {
		w.topic.Stop()
	}
	if w.client != nil {
		_ = w.client.Close()
	}
}

// Draining constants for the deterministic round driver.
const (
	// seekReplayBoundaryPct is the replay-count reorder/at-least-once slack
	// (matching the engine's boundaryLossPct=0.5 reorder tolerance, verdict.go:18),
	// applied as a percentage of the expected replay count and added to the
	// deterministic +2 inclusive-cursor boundary in assertReplayCount.
	seekReplayBoundaryPct = 0.5
	// drainPullBatch is the max messages requested per low-level Pull.
	drainPullBatch = 250
	// drainQuiescentPolls is the number of consecutive empty polls that declares
	// the subscription drained (the queue is finite per round).
	drainQuiescentPolls = 5
	// drainPollGap is the wait between empty polls while draining.
	drainPollGap = 100 * time.Millisecond
)
