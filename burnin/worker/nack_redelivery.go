package worker

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"cloud.google.com/go/pubsub"

	"github.com/kubemq-io/kubemq-gcp-pub-sub/burnin/config"
	"github.com/kubemq-io/kubemq-gcp-pub-sub/burnin/payload"
	"github.com/kubemq-io/kubemq-gcp-pub-sub/burnin/transport"
)

// NackRedeliveryWorker (NEW, spec REQ-001/S5.1) nacks a configurable fraction of
// received messages via transport.Nack (m.Nack() → connector ModifyAckDeadline(0)
// → immediate NAckRange redelivery at the tail with NO backoff, S3.1), acks the
// rest, and on the redelivery of a previously-nacked message acks it — proving
// the tracker reconciles the nacked-then-redelivered message as a recovered
// receive, not loss.
//
// Verdict (S5.1/S5.10): the standard LOSS gate is FULLY RETAINED (a nacked
// message that never redelivers is real loss above boundaryLossPct →
// HARD-FAIL). The DUP gate is EXEMPTED for this worker because explicit
// ModifyAckDeadline(0) nack is at-least-once BY DESIGN — the resulting ≈
// nackFraction duplication is correct behavior, not a defect. The exemption is
// already wired at verdict.go:56 via config.IsNackRedeliveryWorker (NOT a change
// to the global max_duplication_pct, S6.3). A dedicated nack_redelivered metric
// (recordNackRedelivered → metrics.IncNackRedelivered) records confirmed
// redeliveries; WorkerSnapshot.NackRedelivered is informational (no gate).
//
// Tracker invariant (REQ-019): every PUBLISH registers its seq exactly once
// (recordSent). Every RECEIVE — first delivery AND redelivery — calls
// recordTracked so pendingLost reconciles: a redelivery below/at the contiguous
// watermark recovers the seq from pendingLost (no false loss) or counts a
// bounded (exempted) duplicate. A message is acked exactly once on its terminal
// delivery, so the redelivery is the only extra receive (bounded by
// nackFraction). No seq is ever registered twice as a send.
type NackRedeliveryWorker struct {
	*BaseWorker
	topicID string
	subID   string

	client *pubsub.Client
	topic  *pubsub.Topic
	sub    *pubsub.Subscription

	seq          atomic.Uint64
	nackFraction float64

	// nacked tracks per-message ("producerID#seq") nack state under mu so the
	// receive callback (which may run concurrently across consumer goroutines)
	// nacks each seq at most once and recognises its redelivery exactly once.
	// State values: stateNacked = nacked once, awaiting redelivery;
	// stateRedelivered = redelivery seen and acked (terminal).
	mu     sync.Mutex
	nacked map[string]nackState
	rng    *rand.Rand
}

type nackState uint8

const (
	stateNacked      nackState = 1 // nacked once, redelivery pending
	stateRedelivered nackState = 2 // redelivery acked (terminal)
)

// NewNackRedeliveryWorker creates a nack_redelivery worker.
func NewNackRedeliveryWorker(cfg *config.Config, idx int, logger *slog.Logger) Worker {
	topicID := transport.TopicName(config.WorkerNackRedelivery, idx)
	subID := transport.SubscriptionName(config.WorkerNackRedelivery, idx)
	nf := cfg.GCP.NackFraction
	if nf < 0 {
		nf = 0
	}
	if nf > 1 {
		nf = 1
	}
	return &NackRedeliveryWorker{
		BaseWorker: NewBaseWorker(config.WorkerNackRedelivery,
			transport.MappedTopicChannel(topicID), idx, cfg, logger),
		topicID:      topicID,
		subID:        subID,
		nackFraction: nf,
		nacked:       make(map[string]nackState),
		// Per-worker deterministic-seedless RNG: each worker instance gets its own
		// source so the nack draw is independent of (and does not contend on) the
		// global math/rand lock under high receive concurrency.
		rng: rand.New(rand.NewSource(time.Now().UnixNano() + int64(idx))),
	}
}

// Start provisions the topic + a plain (at-least-once) subscription with the
// configured ack deadline and opens the receive loop. The ack deadline governs
// only the normal expiry path; the explicit nack path is immediate (S3.1) and
// independent of it.
func (w *NackRedeliveryWorker) Start(ctx context.Context) error {
	w.consumerCtx, w.consumerCancel = context.WithCancel(ctx)

	client, err := transport.NewPubSubClient(ctx, w.psCfg)
	if err != nil {
		return fmt.Errorf("build pubsub client: %w", err)
	}
	w.client = client

	w.topic, err = ensureTopic(ctx, client, w.topicID)
	if err != nil {
		return err
	}
	w.sub, err = ensureSubscription(ctx, client, w.subID, pubsub.SubscriptionConfig{
		Topic:       w.topic,
		AckDeadline: transport.AckDeadline(w.cfg.GCP.AckDeadlineSeconds),
	})
	if err != nil {
		return err
	}

	transport.ConfigureReceive(w.sub, w.consumerStreams())

	w.consumerWG.Add(1)
	go func() {
		defer w.consumerWG.Done()
		w.receiveLoop(w.consumerCtx)
	}()
	return nil
}

// consumerStreams is the number of concurrent StreamingPull streams for the
// receive loop (ConsumersPerChannel, clamped to ≥ 1).
func (w *NackRedeliveryWorker) consumerStreams() int {
	n := w.workerCfg.ConsumersPerChannel
	if n < 1 {
		n = 1
	}
	return n
}

func (w *NackRedeliveryWorker) receiveLoop(ctx context.Context) {
	gen := w.disconnectGeneration()
	first := true
	for {
		if ctx.Err() != nil {
			return
		}
		if !first {
			w.recordReconnection()
		}
		first = false

		w.signalReady()

		recvCtx, cancel := context.WithCancel(ctx)
		go watchDisconnect(recvCtx, cancel, gen, w.disconnectGeneration)

		err := transport.Receive(recvCtx, w.sub, func(_ context.Context, m *pubsub.Message) {
			w.handleMessage(m, "c-"+w.channelName)
		})
		cancel()
		gen = w.disconnectGeneration()

		if ctx.Err() != nil {
			return
		}
		if err != nil {
			w.recordError("receive_failure")
			if !sleepCtx(ctx, reconnectBackoff(w.cfg)) {
				return
			}
		}
	}
}

// handleMessage records the receive into the tracker (always — first delivery
// AND redelivery, so pendingLost reconciles), then decides ack vs nack:
//   - first receipt, drawn into the nack fraction → transport.Nack (no ack),
//     mark stateNacked; the connector immediately redelivers (S3.1).
//   - redelivery of a previously-nacked seq → transport.Ack + recordNackRedelivered
//     (the recovery this worker proves), mark stateRedelivered (terminal).
//   - any other receipt (not drawn, or a spurious re-receive) → transport.Ack.
//
// A message is acked exactly once on its terminal delivery, so the redelivery is
// the only extra receive — keeping the per-worker duplication bounded by
// nackFraction (exempted at verdict.go:56) and pendingLost accurate.
func (w *NackRedeliveryWorker) handleMessage(m *pubsub.Message, consumerID string) {
	w.recordReceived(len(m.Data), consumerID)

	producerID, seq, crcHex, sentAt, ok := extractMeta(m.Attributes)
	if !ok {
		// Untracked message (no instrumentation): ack and drop so it does not
		// linger and trip the connector's redelivery path.
		transport.Ack(m)
		w.recordAcked()
		return
	}

	if crcHex != "" && !payload.VerifyCRC(m.Data, crcHex) {
		w.recordCorrupted()
	}
	if !sentAt.IsZero() {
		w.recordLatency(time.Since(sentAt))
	}
	// Tracker reconciliation: register EVERY receive (incl. redelivery) so a
	// redelivered seq recovers from pendingLost rather than false-counting loss;
	// recordTracked itself charges the (exempted) duplicate on the redelivery.
	w.recordTracked(producerID, seq)

	key := producerID + "#" + formatUint(seq)

	w.mu.Lock()
	state := w.nacked[key]
	switch {
	case state == stateNacked:
		// This is the redelivery of a message we nacked once: ack it now and
		// count the confirmed recovery. Terminal.
		w.nacked[key] = stateRedelivered
		w.mu.Unlock()
		transport.Ack(m)
		w.recordAcked()
		w.recordNackRedelivered()
		return
	case state == stateRedelivered:
		// Already redelivered+acked once; any further receive is a spurious
		// duplicate — ack to drop it (already charged as a duplicate above).
		w.mu.Unlock()
		transport.Ack(m)
		w.recordAcked()
		return
	}

	// First receipt of this seq: draw the nack fraction.
	doNack := w.rng.Float64() < w.nackFraction
	if doNack {
		w.nacked[key] = stateNacked
		w.mu.Unlock()
		// Explicit nack → ModifyAckDeadline(0) → immediate redelivery, no backoff
		// (S3.1). Do NOT ack: the message must redeliver.
		transport.Nack(m)
		return
	}
	w.mu.Unlock()
	transport.Ack(m)
	w.recordAcked()
}

// StartProducers launches the PublishSync producer loop(s) at the configured
// Rate. Each publish stamps the instrumentation attributes and registers the
// send exactly once.
func (w *NackRedeliveryWorker) StartProducers() {
	w.producerCtx, w.producerCancel = context.WithCancel(context.Background())
	n := w.workerCfg.ProducersPerChannel
	if n < 1 {
		n = 1
	}
	for i := 0; i < n; i++ {
		w.producerWG.Add(1)
		go func(pi int) {
			defer w.producerWG.Done()
			w.produceLoop(w.producerCtx, pi)
		}(i)
	}
}

func (w *NackRedeliveryWorker) produceLoop(ctx context.Context, producerIdx int) {
	producerID := fmt.Sprintf("p-%s-%d", w.channelName, producerIdx)
	for {
		if ctx.Err() != nil {
			return
		}
		if err := w.waitForRate(ctx); err != nil {
			return
		}

		seq := w.seq.Add(1)
		body, crcHex := payload.Build(w.selectMessageSize())

		start := time.Now()
		if _, err := transport.PublishSync(ctx, w.topic, body, stampAttrs(producerID, seq, crcHex)); err != nil {
			if ctx.Err() != nil {
				return
			}
			w.recordError("publish_failure")
			continue
		}
		metricObserveSend(w.name, time.Since(start))
		w.recordSent(len(body), producerID)
	}
}

// StopConsumers stops the receiver, stops the publisher handle, and closes the
// client (S8: every Close()/Stop() error handled with the established pattern).
func (w *NackRedeliveryWorker) StopConsumers() {
	w.BaseWorker.StopConsumers()
	if w.topic != nil {
		w.topic.Stop()
	}
	if w.client != nil {
		_ = w.client.Close()
	}
}
