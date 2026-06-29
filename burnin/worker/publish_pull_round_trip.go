package worker

import (
	"context"
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"

	"cloud.google.com/go/pubsub"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/kubemq-io/kubemq-gcp-pub-sub/burnin/config"
	"github.com/kubemq-io/kubemq-gcp-pub-sub/burnin/payload"
	"github.com/kubemq-io/kubemq-gcp-pub-sub/burnin/transport"
)

// batchProbeProducerID is the reserved producer-id namespace stamped on the
// atomicity-probe batch (spec S5.2). The connector MUST reject the whole batch
// (forbidden goog-prefixed attribute key, S3.2) with codes.InvalidArgument, so
// NO probe-tagged message may ever reach the consumer. A delivered probe message
// is a partial enqueue and hard-fails the batch-atomicity gate. The namespace is
// distinct from the steady-state producerIDs (p-<chan>-<idx>) so probe messages
// are never registered as tracker sends and contribute zero loss.
const batchProbeProducerID = "batch-probe"

// batchProbeGoogAttrKey is the forbidden goog-prefixed attribute key used as the
// PRIMARY atomicity-probe trigger (spec S5.2). The connector's validatePublish
// rejects any goog-prefixed attribute key with errInvalidArgument →
// codes.InvalidArgument (mapper.go:208), invoked per-message inside the publish
// batch loop (fanout.go:95). Unlike an oversize body, this does NOT collide with
// the harness's own message.size_bytes config guard, so the rejection is
// unambiguously connector-side and deterministic (S3.2).
const batchProbeGoogAttrKey = "goog-burnin-atomicity"

// PublishPullRoundTripWorker (worker 1, spec S6.3) stresses the Publish → Pull
// (Receive) → Acknowledge round trip at sustained rate: one or more producers
// publish to a single topic, competing receivers drain the subscription and ack
// each message, verifying at-least-once delivery (no reconciled loss, bounded
// duplicates) under load and forced disconnect. It uses the high-level
// pubsub.Client (StreamingPull under the Receive loop). The connector maps the
// topic to the Events Store log pubsub.{t} and the subscription to the queue
// pubsub.sub.{s} (spec S3.3).
//
// Batch publish atomicity (spec REQ-002/S5.2): producers publish c.GCP.BatchSize
// instrumented messages per round via transport.PublishBatch, awaiting every
// per-message result before counting them sent. When BatchSize == 1 this is
// identical to a single PublishSync (backward compatible). At Start (before the
// steady-state loop) a one-shot atomicity probe (gated by gcp.batch_atomicity_probe)
// publishes a batch containing one message with a forbidden goog-prefixed
// attribute key and asserts the connector rejects the WHOLE batch with
// codes.InvalidArgument and zero partial enqueue — any probe-tagged message that
// later reaches the consumer is a partial enqueue and hard-fails the verdict.
type PublishPullRoundTripWorker struct {
	*BaseWorker
	topicID string
	subID   string

	client *pubsub.Client
	topic  *pubsub.Topic
	sub    *pubsub.Subscription

	seq atomic.Uint64
}

// NewPublishPullRoundTripWorker creates a publish_pull_round_trip worker.
func NewPublishPullRoundTripWorker(cfg *config.Config, idx int, logger *slog.Logger) Worker {
	topicID := transport.TopicName(config.WorkerPublishPullRoundTrip, idx)
	subID := transport.SubscriptionName(config.WorkerPublishPullRoundTrip, idx)
	return &PublishPullRoundTripWorker{
		BaseWorker: NewBaseWorker(config.WorkerPublishPullRoundTrip,
			transport.MappedTopicChannel(topicID), idx, cfg, logger),
		topicID: topicID,
		subID:   subID,
	}
}

// Start builds the Pub/Sub client, provisions the topic + subscription, and
// brings up the competing receiver(s).
func (w *PublishPullRoundTripWorker) Start(ctx context.Context) error {
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

	// One StreamingPull stream per configured consumer; the high-level Receive
	// loop multiplexes competing receivers over the same subscription.
	transport.ConfigureReceive(w.sub, w.workerCfg.ConsumersPerChannel)

	w.consumerWG.Add(1)
	go func() {
		defer w.consumerWG.Done()
		w.receiveLoop(w.consumerCtx)
	}()

	// One-shot batch-atomicity negative-path probe (spec REQ-002/S5.2), run at
	// Start before the steady-state producer loop. Deterministic [HARD-FAIL]
	// classification (S5.10 row 002): the connector MUST reject the whole batch
	// with codes.InvalidArgument and enqueue nothing. Gated by gcp.batch_atomicity_probe.
	if w.cfg.GCP.BatchAtomicityProbe {
		w.runAtomicityProbe(ctx)
	}
	return nil
}

// runAtomicityProbe publishes a single batch containing one message with a
// forbidden goog-prefixed attribute key (spec S5.2 PRIMARY trigger) and asserts
// the connector's whole-batch rejection semantics (S3.2):
//
//  1. PublishBatch MUST return an error whose gRPC status is specifically
//     codes.InvalidArgument (checked via status.Code, NOT a generic transport
//     error — this distinguishes the connector's whole-batch reject from a
//     client-side guard). A success or a non-InvalidArgument error → violation.
//  2. ZERO partial enqueue: no probe-tagged message (batchProbeProducerID
//     namespace) may ever be delivered to the consumer. A delivered probe
//     message is detected in handleMessage and counted as a violation there.
//
// The probe never publishes a tracker-registered send (it uses the reserved
// batch-probe producerID namespace), so a correctly-rejected probe contributes
// zero loss to the steady-state tracker.
func (w *PublishPullRoundTripWorker) runAtomicityProbe(ctx context.Context) {
	body, crcHex := payload.Build(w.selectMessageSize())
	attrs := stampAttrs(batchProbeProducerID, 1, crcHex)
	// The forbidden goog-prefixed attribute key triggers the connector-side
	// whole-batch reject (mapper.go:208). Keep one valid sibling message in the
	// batch so atomicity is exercised: it MUST NOT be enqueued either.
	good := &pubsub.Message{Data: body, Attributes: attrs}
	bad := &pubsub.Message{
		Data: body,
		Attributes: map[string]string{
			payload.AttrWorkerID:    batchProbeProducerID,
			payload.AttrSequence:    "2",
			payload.AttrContentHash: crcHex,
			batchProbeGoogAttrKey:   "x",
		},
	}

	_, err := transport.PublishBatch(ctx, w.topic, []*pubsub.Message{good, bad})
	if err == nil {
		// Whole batch accepted: the connector did not enforce atomicity. The
		// good sibling (and possibly the bad one) was enqueued → partial/full
		// enqueue of an invalid batch.
		w.logger.Error("batch atomicity probe: invalid batch was ACCEPTED (expected codes.InvalidArgument reject)")
		w.recordBatchAtomicityViolation()
		return
	}
	if code := status.Code(err); code != codes.InvalidArgument {
		// Rejected, but not with the connector's whole-batch InvalidArgument
		// status — ambiguous (e.g. a client/SDK-side guard or transport error),
		// which does not prove the connector's atomicity semantics.
		w.logger.Error("batch atomicity probe: reject was not codes.InvalidArgument",
			"code", code.String(), "err", errString(err))
		w.recordBatchAtomicityViolation()
		return
	}
	w.logger.Info("batch atomicity probe: whole-batch rejected with codes.InvalidArgument (zero partial enqueue asserted via consumer)")
}

// receiveLoop runs the StreamingPull Receive loop, re-opening it after a forced
// disconnect or a transient error.
func (w *PublishPullRoundTripWorker) receiveLoop(ctx context.Context) {
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
		// Watch the disconnect generation: a forced disconnect cancels the current
		// Receive so the loop re-establishes the StreamingPull stream.
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

func (w *PublishPullRoundTripWorker) handleMessage(m *pubsub.Message, consumerID string) {
	w.recordReceived(len(m.Data), consumerID)

	producerID, seq, crcHex, sentAt, ok := extractMeta(m.Attributes)

	// Batch-atomicity partial-enqueue detection (spec REQ-002/S5.2): a probe-tagged
	// message MUST never be delivered — the connector rejects the whole probe batch
	// with codes.InvalidArgument and enqueues nothing. Receiving one means a message
	// from a rejected batch leaked through → partial enqueue → [HARD-FAIL]. It is
	// NOT a tracker send (the probe never registered it), so ack it and skip tracking
	// to avoid perturbing the steady-state loss/dup accounting.
	if ok && producerID == batchProbeProducerID {
		w.logger.Error("batch atomicity violation: probe message delivered (partial enqueue of a rejected batch)",
			"seq", seq)
		w.recordBatchAtomicityViolation()
		transport.Ack(m)
		w.recordAcked()
		return
	}

	if ok {
		if crcHex != "" && !payload.VerifyCRC(m.Data, crcHex) {
			w.recordCorrupted()
		}
		if !sentAt.IsZero() {
			w.recordLatency(time.Since(sentAt))
		}
		w.recordTracked(producerID, seq)
	}

	// Round-trip completion: ack the broker seq under its ack-deadline lease.
	transport.Ack(m)
	w.recordAcked()
}

// StartProducers launches the Publish loop(s) (measurement window).
func (w *PublishPullRoundTripWorker) StartProducers() {
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

func (w *PublishPullRoundTripWorker) produceLoop(ctx context.Context, producerIdx int) {
	producerID := fmt.Sprintf("p-%s-%d", w.channelName, producerIdx)

	// Batch publish atomicity (spec REQ-002/S5.2): publish c.GCP.BatchSize
	// instrumented messages per round via transport.PublishBatch, which enqueues
	// all then awaits every per-message result before any is counted sent. With
	// BatchSize == 1 this is a single-message round-trip identical to the prior
	// PublishSync behavior (backward compatible). Config validation pins
	// batch_size to 1..1000 (config.go:608), so clamp defensively to >= 1.
	batchSize := w.cfg.GCP.BatchSize
	if batchSize < 1 {
		batchSize = 1
	}

	for {
		if ctx.Err() != nil {
			return
		}

		msgs := make([]*pubsub.Message, 0, batchSize)
		bodyLens := make([]int, 0, batchSize)
		for i := 0; i < batchSize; i++ {
			if err := w.waitForRate(ctx); err != nil {
				return
			}
			seq := w.seq.Add(1)
			body, crcHex := payload.Build(w.selectMessageSize())
			msgs = append(msgs, &pubsub.Message{
				Data:       body,
				Attributes: stampAttrs(producerID, seq, crcHex),
			})
			bodyLens = append(bodyLens, len(body))
		}

		start := time.Now()
		if _, err := transport.PublishBatch(ctx, w.topic, msgs); err != nil {
			if ctx.Err() != nil {
				return
			}
			// All-or-nothing: a batch error means none of these messages were
			// durably enqueued, so none are counted sent (they remain unregistered
			// in the tracker and contribute zero loss). Count one error per failed
			// batch attempt.
			w.recordError("publish_failure")
			continue
		}
		batchDur := time.Since(start)
		// All per-message results resolved: count each message sent now.
		for _, n := range bodyLens {
			metricObserveSend(w.name, batchDur)
			w.recordSent(n, producerID)
		}
	}
}

// StopConsumers stops the receiver and closes the Pub/Sub client.
func (w *PublishPullRoundTripWorker) StopConsumers() {
	w.BaseWorker.StopConsumers()
	if w.topic != nil {
		w.topic.Stop()
	}
	if w.client != nil {
		_ = w.client.Close()
	}
}
