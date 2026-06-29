package worker

import (
	"context"
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"

	"cloud.google.com/go/pubsub"
	vkit "cloud.google.com/go/pubsub/apiv1"

	"github.com/kubemq-io/kubemq-gcp-pub-sub/burnin/config"
	"github.com/kubemq-io/kubemq-gcp-pub-sub/burnin/payload"
	"github.com/kubemq-io/kubemq-gcp-pub-sub/burnin/transport"
)

// ExactlyOnceWorker (worker 4, spec S6.3) drives an exactly-once subscription
// (EnableExactlyOnceDelivery) and reconciles ack confirmations. Under
// exactly-once, AckWithResult resolves an AcknowledgeStatus: Success confirms the
// ack; InvalidAckID / FailedPrecondition / Other mean the ack id expired or the
// lease was lost — the worker retries the ack once and, on a later success,
// reconciles the earlier failure. The verdict gate is that every ack id is FULLY
// RECONCILED (no surviving invalid/temporary-failed ack ids — spec S6.6).
//
// Exactly-once is NODE-LOCAL: an ack_id minted on one node is invalid on another
// (spec S3.5). The burn-in harness runs against a single connector instance, so
// the ack id is always honoured on the node that minted it; in a cluster a
// subscription's StreamingPull must be pinned to one node.
//
// Hardened partials (spec S5.7, REQ-007/REQ-008):
//   - REQ-007: a one-shot invalid-ack probe (gated by gcp.exactly_once_probe)
//     acknowledges a fabricated ack id via the LOW-LEVEL unary Acknowledge on the
//     exactly-once subscription and asserts the connector returns
//     codes.FailedPrecondition + ErrorInfo reason PERMANENT_FAILURE_INVALID_ACK_ID
//     (S3.7). When the probe runs it sets InvalidAckProbeRan; on the expected
//     status it records ExactlyOnceInvalidAckConfirmed. The verdict gate
//     (InvalidAckProbeRan && ExactlyOnceInvalidAckConfirmed==0 → fail) is
//     deterministic but probe-ran-guarded so a disabled probe cannot false-fail.
//   - REQ-008: every steady-state ack that resolves AcknowledgeStatusSuccess is a
//     confirmed StreamingPull ack (S3.8). Once at least one exactly-once message
//     has been received the worker sets ConfirmationsProbeRan; the verdict gate
//     (ConfirmationsProbeRan && ExactlyOnceConfirmed==0 → fail) is deterministic
//     but guarded so a zero-traffic worker cannot false-fail.
type ExactlyOnceWorker struct {
	*BaseWorker
	topicID string
	subID   string

	client *pubsub.Client
	topic  *pubsub.Topic
	sub    *pubsub.Subscription

	// subClient is the LOW-LEVEL apiv1 subscriber used ONLY by the one-shot
	// invalid-ack probe (REQ-007): the high-level SDK never issues a unary ack on
	// a fabricated id, so the connector's exactly-once FAILED_PRECONDITION reject
	// must be exercised through the bare SubscriberClient.
	subClient *vkit.SubscriberClient
	subPath   string

	seq atomic.Uint64
}

// NewExactlyOnceWorker creates an exactly_once worker.
func NewExactlyOnceWorker(cfg *config.Config, idx int, logger *slog.Logger) Worker {
	topicID := transport.TopicName(config.WorkerExactlyOnce, idx)
	subID := transport.SubscriptionName(config.WorkerExactlyOnce, idx)
	return &ExactlyOnceWorker{
		BaseWorker: NewBaseWorker(config.WorkerExactlyOnce,
			transport.MappedTopicChannel(topicID), idx, cfg, logger),
		topicID: topicID,
		subID:   subID,
	}
}

// Start provisions the topic + an exactly-once subscription and opens the
// receive loop.
func (w *ExactlyOnceWorker) Start(ctx context.Context) error {
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
		Topic:                     w.topic,
		AckDeadline:               transport.AckDeadline(w.cfg.GCP.AckDeadlineSeconds),
		EnableExactlyOnceDelivery: true,
	})
	if err != nil {
		return err
	}

	transport.ConfigureReceive(w.sub, 1)

	// REQ-007: one-shot invalid-ack probe against the exactly-once subscription.
	// Runs at Start (before steady-state traffic) so a disabled probe leaves
	// InvalidAckProbeRan==false and the verdict gate is skipped (no false-fail).
	w.runInvalidAckProbe(ctx)

	w.consumerWG.Add(1)
	go func() {
		defer w.consumerWG.Done()
		w.receiveLoop(w.consumerCtx)
	}()
	return nil
}

// runInvalidAckProbe acknowledges a fabricated ack id via the LOW-LEVEL unary
// Acknowledge on the exactly-once subscription and asserts the connector returns
// codes.FailedPrecondition with ErrorInfo reason PERMANENT_FAILURE_INVALID_ACK_ID
// (spec REQ-007 / S3.7). The SDK surfaces this as AcknowledgeStatusInvalidAckID.
// It is a no-op when gcp.exactly_once_probe is disabled (the gate is then skipped,
// I-1). When the probe runs to completion it sets InvalidAckProbeRan; on the
// expected status it records ExactlyOnceInvalidAckConfirmed.
func (w *ExactlyOnceWorker) runInvalidAckProbe(ctx context.Context) {
	if !w.cfg.GCP.ExactlyOnceProbe {
		return
	}

	subClient, err := transport.NewSubscriberClient(ctx, w.psCfg)
	if err != nil {
		// Could not build the low-level client: do NOT mark the probe as ran, so
		// the never-observed gate stays skipped (the probe did not execute).
		w.recordError("ack_failure")
		w.logger.Warn("exactly-once invalid-ack probe: build subscriber client failed", "err", errString(err))
		return
	}
	w.subClient = subClient
	w.subPath = transport.SubscriptionPath(w.psCfg.ProjectID, w.subID)

	// A fabricated, never-minted ack id: the connector's decodeAckID rejects it
	// and, because the subscription is exactly-once, returns FAILED_PRECONDITION +
	// PERMANENT_FAILURE_INVALID_ACK_ID (connector subscriber.go Acknowledge).
	const fakeAckID = "burnin-exactly-once-invalid-ack-probe"
	status, ackErr := transport.UnaryAckInvalid(ctx, w.subClient, w.subPath, fakeAckID)

	// The probe executed to completion: record it ran so the verdict gate applies.
	w.markInvalidAckProbeRan()

	if status == pubsub.AcknowledgeStatusInvalidAckID ||
		(transport.IsFailedPrecondition(ackErr) && transport.AckErrorReason(ackErr) == transport.InvalidAckIDReason) {
		w.recordExactlyOnceInvalidAckConfirmed()
		w.logger.Info("exactly-once invalid-ack probe confirmed",
			"status", ackStatusString(status), "reason", transport.AckErrorReason(ackErr))
		return
	}

	// Probe ran but the expected FAILED_PRECONDITION/INVALID_ACK_ID was not seen.
	// Leave ExactlyOnceInvalidAckConfirmed==0; the probe-ran-guarded verdict gate
	// (verdict.go) hard-fails on this (deterministic, S5.10 REQ-007).
	w.logger.Warn("exactly-once invalid-ack probe did NOT observe expected status",
		"status", ackStatusString(status), "reason", transport.AckErrorReason(ackErr), "err", errString(ackErr))
}

func (w *ExactlyOnceWorker) receiveLoop(ctx context.Context) {
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

		err := transport.Receive(recvCtx, w.sub, func(cbCtx context.Context, m *pubsub.Message) {
			w.handleMessage(cbCtx, m, "c-"+w.channelName)
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

func (w *ExactlyOnceWorker) handleMessage(ctx context.Context, m *pubsub.Message, consumerID string) {
	w.recordReceived(len(m.Data), consumerID)

	// REQ-008: exactly-once traffic flowed (received > 0), so the StreamingPull
	// ack-confirmation gate now applies. Idempotent; left false on a zero-traffic
	// or disabled worker so the gate is skipped (no false-fail, I-1).
	w.markConfirmationsProbeRan()

	producerID, seq, crcHex, sentAt, ok := extractMeta(m.Attributes)
	if ok {
		if crcHex != "" && !payload.VerifyCRC(m.Data, crcHex) {
			w.recordCorrupted()
		}
		if !sentAt.IsZero() {
			w.recordLatency(time.Since(sentAt))
		}
		w.recordTracked(producerID, seq)
	}

	// Exactly-once ack: AckWithResult resolves to an AcknowledgeStatus.
	w.ackWithReconciliation(ctx, m)
}

// ackWithReconciliation acks the message and reconciles the AcknowledgeStatus.
// Success confirms the ack. A failed status (invalid/expired ack id, failed
// precondition, other) is recorded as unreconciled and retried once; a retry
// success reconciles it (the surviving unreconciled count is the gate failure).
func (w *ExactlyOnceWorker) ackWithReconciliation(ctx context.Context, m *pubsub.Message) {
	status, err := transport.AckExactlyOnce(ctx, m)
	if err == nil && status == pubsub.AcknowledgeStatusSuccess {
		w.recordExactlyOnceAck(true)
		w.recordAcked()
		return
	}

	// Record the failure, then attempt one more ack: if it now succeeds, the lease
	// was simply re-acquired and we reconcile the earlier failure.
	w.recordExactlyOnceAck(false)
	w.logger.Warn("exactly-once ack failed", "status", ackStatusString(status), "err", errString(err))

	retryStatus, retryErr := transport.AckExactlyOnce(ctx, m)
	if retryErr == nil && retryStatus == pubsub.AcknowledgeStatusSuccess {
		w.reconcileExactlyOnce()
		w.recordExactlyOnceAck(true)
		w.recordAcked()
		return
	}
	w.recordError("ack_failure")
}

// StartProducers launches the Publish loop(s).
func (w *ExactlyOnceWorker) StartProducers() {
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

func (w *ExactlyOnceWorker) produceLoop(ctx context.Context, producerIdx int) {
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

// StopConsumers stops the receiver and closes the clients (incl. the low-level
// invalid-ack probe subscriber, REQ-007).
func (w *ExactlyOnceWorker) StopConsumers() {
	w.BaseWorker.StopConsumers()
	if w.topic != nil {
		w.topic.Stop()
	}
	if w.subClient != nil {
		_ = w.subClient.Close()
	}
	if w.client != nil {
		_ = w.client.Close()
	}
}
