package worker

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"sync/atomic"
	"time"

	"cloud.google.com/go/pubsub"
	vkit "cloud.google.com/go/pubsub/apiv1"
	"cloud.google.com/go/pubsub/apiv1/pubsubpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/kubemq-io/kubemq-gcp-pub-sub/burnin/config"
	"github.com/kubemq-io/kubemq-gcp-pub-sub/burnin/payload"
	"github.com/kubemq-io/kubemq-gcp-pub-sub/burnin/transport"
)

// streamCloseSignature is the connector's periodic StreamingPull server-close
// message text (kubemq-server connectors/pubsub/streaming_pull.go:208, spec
// S3.14). A reconnection whose error is codes.Unavailable AND whose message
// contains this string is the connector's StreamCloseSeconds (default 1800)
// force-close — best-effort distinguished from an ordinary transient Unavailable
// (broker bounce / network). Used only to record an observation (REQ-014, NOTE).
const streamCloseSignature = "the stream has been closed"

// connectorMaxOutstandingCeiling is the connector's MaxOutstandingMessages
// flow-control ceiling (CONNECTORS_PUB_SUB_MAX_OUTSTANDING_MESSAGES, default
// 1000, spec S3.9). The flow-control probe requests a ceiling ABOVE this and
// asserts the connector clamps un-acked delivery to no more than this value.
const connectorMaxOutstandingCeiling = 1000

// flowControlProbeRequest is the (deliberately too-high) max_outstanding_messages
// the flow-control probe requests so the connector's server-side clamp to its
// own ceiling (connectorMaxOutstandingCeiling) is observable (spec S5.8).
const flowControlProbeRequest = 5000

// flowControlProbeBacklog is the backlog the flow-control probe publishes; it
// MUST exceed the connector ceiling so the clamp is exercised. A comfortable
// margin (> 1000) lets the probe observe the connector stop delivering at the
// ceiling before any ack.
const flowControlProbeBacklog = 2000

// StreamingPullWorker (worker 2, spec S6.3) exercises the long-lived
// bidirectional StreamingPull stream that most client libraries use by default.
// The connector periodically closes each StreamingPull stream
// (CONNECTORS_PUB_SUB_STREAM_CLOSE_SECONDS, default 1800s) to force a
// transparent SDK reconnect; the worker must lose no messages across it. A
// single Receive stream (NumGoroutines=1) keeps the ordered-receive signal
// clean, and the forced-disconnect injector additionally tears down and
// re-establishes the stream to exercise lease re-acquisition / at-least-once
// redelivery on ack-deadline expiry.
//
// On Start the worker ALSO runs three one-shot hardened-partial probes over
// ISOLATED probe topics/subs (so they never perturb the steady-state tracker),
// each gated by its own config toggle (spec S5.8):
//   - flow-control probe (REQ-009): a low-level StreamingPull requesting a
//     ceiling ABOVE the connector's and asserting the server-side clamp.
//   - lease-extend probe (REQ-011): ModifyAckDeadline(>0) holds a lease past its
//     original deadline (no redelivery) and ModifyAckDeadline(out-of-range) is
//     rejected with INVALID_ARGUMENT.
//   - cross-stream ack probe (REQ-013): a message delivered on one StreamingPull
//     stream is acked via a different path (subscription-owned leases).
//
// It also OBSERVES the connector's periodic StreamingPull server-close in the
// steady-state reconnect loop (REQ-014, NOTE only — StreamCloseSeconds default
// 1800 > a soak window, so not observing it is the expected outcome).
type StreamingPullWorker struct {
	*BaseWorker
	topicID string
	subID   string

	client *pubsub.Client
	topic  *pubsub.Topic
	sub    *pubsub.Subscription

	// Low-level apiv1 subscriber + the isolated probe resources the hardened
	// probes use (spec S5.8). The high-level Receive path cannot prove the
	// connector's server-side clamp / cross-stream lease ownership, so the
	// probes go through the bare SubscriberClient (same transport dead_letter
	// uses). The probe topics are provisioned lazily inside each probe.
	subClient   *vkit.SubscriberClient
	probeTopics []*pubsub.Topic

	seq atomic.Uint64
}

// NewStreamingPullWorker creates a streaming_pull worker.
func NewStreamingPullWorker(cfg *config.Config, idx int, logger *slog.Logger) Worker {
	topicID := transport.TopicName(config.WorkerStreamingPull, idx)
	subID := transport.SubscriptionName(config.WorkerStreamingPull, idx)
	return &StreamingPullWorker{
		BaseWorker: NewBaseWorker(config.WorkerStreamingPull,
			transport.MappedTopicChannel(topicID), idx, cfg, logger),
		topicID: topicID,
		subID:   subID,
	}
}

// Start builds the client, provisions the topic + subscription, and opens the
// StreamingPull receive loop.
func (w *StreamingPullWorker) Start(ctx context.Context) error {
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

	// One StreamingPull stream keeps the delivery signal deterministic; flow
	// control caps outstanding (unacked) messages (server enforces its own
	// MaxOutstandingMessages / MaxInflightPerSubscription safety nets). The byte
	// ceiling (gcp.max_outstanding_bytes, default 0 = unlimited) is forward-looking
	// plumbing only and is NOT part of any asserted gate (spec REQ-009 / M-2); the
	// asserted server-side clamp is the low-level flow-control probe below.
	transport.ConfigureReceiveWithBytes(w.sub, 1, w.cfg.GCP.MaxOutstandingBytes)

	// Low-level apiv1 subscriber for the hardened probes (REQ-009/011/013). It is
	// only needed when at least one probe is enabled; build it unconditionally so
	// the probe runner can use it, and close it in StopConsumers.
	if w.cfg.GCP.FlowControlProbe || w.cfg.GCP.LeaseExtendProbe || w.cfg.GCP.CrossStreamProbe {
		w.subClient, err = transport.NewSubscriberClient(ctx, w.psCfg)
		if err != nil {
			return fmt.Errorf("build apiv1 subscriber client: %w", err)
		}
	}

	w.consumerWG.Add(1)
	go func() {
		defer w.consumerWG.Done()
		w.receiveLoop(w.consumerCtx)
	}()

	// One-shot hardened-partial probes (spec S5.8). They run in their own tracked
	// goroutine over ISOLATED probe topics/subs so they never perturb the
	// steady-state tracker, and are cancelled via consumerCtx on shutdown.
	w.consumerWG.Add(1)
	go func() {
		defer w.consumerWG.Done()
		w.runProbes(w.consumerCtx)
	}()
	return nil
}

func (w *StreamingPullWorker) receiveLoop(ctx context.Context) {
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
			// REQ-014 (NOTE): best-effort detection of the connector's periodic
			// StreamingPull server-close (codes.Unavailable + "the stream has been
			// closed", connector streaming_pull.go:208 / S3.14). The SDK reconnects
			// transparently and the loop already records a reconnection + retains the
			// no-loss gate; this only records the observation. Not observing it is the
			// EXPECTED outcome since StreamCloseSeconds (default 1800) > the run window.
			if isPeriodicClose(err) {
				w.recordPeriodicCloseObserved()
				w.logger.Info("periodic StreamingPull server-close observed", "err", errString(err))
			}
			w.recordError("receive_failure")
			if !sleepCtx(ctx, reconnectBackoff(w.cfg)) {
				return
			}
		}
	}
}

// isPeriodicClose reports whether err is the connector's periodic StreamingPull
// server-initiated close (spec REQ-014 / S3.14): a codes.Unavailable status whose
// message contains the connector's close text. The status-code + message-string
// match best-effort distinguishes the periodic close from an ordinary transient
// Unavailable (broker bounce / network); a false negative is acceptable (NOTE only).
func isPeriodicClose(err error) bool {
	if err == nil {
		return false
	}
	if status.Code(err) != codes.Unavailable {
		return false
	}
	return strings.Contains(err.Error(), streamCloseSignature)
}

func (w *StreamingPullWorker) handleMessage(m *pubsub.Message, consumerID string) {
	w.recordReceived(len(m.Data), consumerID)

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

	transport.Ack(m)
	w.recordAcked()
}

// runProbes executes the one-shot hardened-partial probes (REQ-009/011/013) over
// isolated probe topics/subs, each gated by its config toggle (spec S5.8). The
// probes are independent and run sequentially in a single tracked goroutine; a
// probe that errors logs and is skipped (the verdict gate is guarded by its
// *ProbeRan flag, so a skipped probe cannot false-fail — I-1). The probes never
// touch the steady-state topic/sub or tracker.
func (w *StreamingPullWorker) runProbes(ctx context.Context) {
	if w.subClient == nil {
		return
	}
	if w.cfg.GCP.FlowControlProbe {
		w.flowControlProbe(ctx)
	}
	if w.cfg.GCP.LeaseExtendProbe {
		w.leaseExtendProbe(ctx)
	}
	if w.cfg.GCP.CrossStreamProbe {
		w.crossStreamAckProbe(ctx)
	}
}

// provisionProbe creates an isolated probe topic + a plain (at-least-once)
// subscription with the given suffix, tracked for cleanup in StopConsumers. The
// caller owns the publish; the topic handle is registered so StopConsumers stops
// the publisher. ackDeadlineSeconds sizes the lease for timing-sensitive probes.
func (w *StreamingPullWorker) provisionProbe(ctx context.Context, suffix string, ackDeadlineSeconds int) (*pubsub.Topic, string, error) {
	topicID := transport.TopicName(config.WorkerStreamingPull, w.channelIndex) + "_" + suffix
	subID := transport.SubscriptionName(config.WorkerStreamingPull, w.channelIndex) + "_" + suffix
	topic, err := ensureTopic(ctx, w.client, topicID)
	if err != nil {
		return nil, "", err
	}
	w.probeTopics = append(w.probeTopics, topic)
	if _, err := ensureSubscription(ctx, w.client, subID, pubsub.SubscriptionConfig{
		Topic:       topic,
		AckDeadline: transport.AckDeadline(ackDeadlineSeconds),
	}); err != nil {
		return nil, "", err
	}
	return topic, transport.SubscriptionPath(w.psCfg.ProjectID, subID), nil
}

// flowControlProbe (REQ-009 / S5.8) asserts the connector's SERVER-SIDE flow
// control clamp. It opens a low-level StreamingPull requesting a ceiling ABOVE
// the connector's MaxOutstandingMessages and publishes a backlog larger than the
// ceiling, then receives WITHOUT acking and counts the un-acked messages the
// connector streams. The connector clamps to its ceiling and stops delivering
// until acks arrive, so the peak un-acked count proves the clamp (a callback-
// sampled high-level gauge cannot — the SDK applies its own client-side cap).
// Receiving more than the ceiling un-acked is a deterministic HARD-FAIL.
func (w *StreamingPullWorker) flowControlProbe(ctx context.Context) {
	topic, subPath, err := w.provisionProbe(ctx, "fcprobe", w.cfg.GCP.AckDeadlineSeconds)
	if err != nil {
		w.logger.Warn("flow-control probe provisioning failed (skipped)", "err", errString(err))
		return
	}

	// Publish a backlog larger than the connector ceiling so the clamp is exercised.
	body, crcHex := payload.Build(w.selectMessageSize())
	for i := 0; i < flowControlProbeBacklog; i++ {
		if ctx.Err() != nil {
			return
		}
		if _, err := transport.PublishSync(ctx, topic, body, stampAttrs("fcprobe", uint64(i+1), crcHex)); err != nil {
			if ctx.Err() != nil {
				return
			}
			w.logger.Warn("flow-control probe backlog publish failed (skipped)", "err", errString(err))
			return
		}
	}

	// Request a ceiling ABOVE the connector's; the empty-recv budget lets the probe
	// conclude once the connector stops delivering (clamped) without new messages.
	peakUnacked, err := transport.FlowControlClampProbe(ctx, w.subClient, subPath,
		flowControlProbeRequest, w.cfg.GCP.AckDeadlineSeconds, 5)
	if err != nil {
		if ctx.Err() != nil {
			return
		}
		w.logger.Warn("flow-control probe stream failed (skipped)", "err", errString(err))
		return
	}

	// The probe ran to completion: mark it so the verdict gate is live, and sample
	// the peak un-acked count into the gauge.
	w.markFlowControlProbeRan()
	w.setOutstanding(float64(peakUnacked))

	// [HARD-FAIL] more un-acked delivered than the connector ceiling → the
	// server-side clamp did not hold (REQ-009, deterministic, S5.10).
	if peakUnacked > connectorMaxOutstandingCeiling {
		w.recordFlowControlViolation()
		w.logger.Error("flow-control clamp violation",
			"peak_unacked", peakUnacked, "ceiling", connectorMaxOutstandingCeiling)
		return
	}
	w.logger.Info("flow-control server-side clamp confirmed",
		"peak_unacked", peakUnacked, "ceiling", connectorMaxOutstandingCeiling,
		"requested", flowControlProbeRequest)
}

// leaseExtendProbe (REQ-011 / S5.8) asserts ModifyAckDeadline(>0) extends a
// lease and that an out-of-range deadline is rejected. It publishes one message,
// low-level Pulls it to capture the ack id, extends the lease via
// ModifyAckDeadline(extendSeconds in [10,600]), HOLDS past the ORIGINAL ack
// deadline, then re-Pulls and asserts the message was NOT redelivered in the
// interim (the lease was extended), then acks it. It also issues an out-of-range
// ModifyAckDeadline and asserts codes.InvalidArgument. Both are deterministic
// HARD-FAILs, each guarded by LeaseExtendProbeRan (I-1).
func (w *StreamingPullWorker) leaseExtendProbe(ctx context.Context) {
	ackDeadline := w.cfg.GCP.AckDeadlineSeconds
	if ackDeadline < 10 {
		ackDeadline = 10
	}
	topic, subPath, err := w.provisionProbe(ctx, "leprobe", ackDeadline)
	if err != nil {
		w.logger.Warn("lease-extend probe provisioning failed (skipped)", "err", errString(err))
		return
	}

	body, crcHex := payload.Build(w.selectMessageSize())
	if _, err := transport.PublishSync(ctx, topic, body, stampAttrs("leprobe", 1, crcHex)); err != nil {
		if ctx.Err() != nil {
			return
		}
		w.logger.Warn("lease-extend probe publish failed (skipped)", "err", errString(err))
		return
	}

	ackID, ok := w.pullOneAckID(ctx, subPath)
	if !ok {
		w.logger.Warn("lease-extend probe: no message pulled (skipped)")
		return
	}

	// The probe ran to completion (message pulled, lease in hand): the verdict gate
	// is now live and both confirmations are required.
	w.markLeaseExtendProbeRan()

	// Out-of-range deadline (5s; valid range is 0 nack or [10,600]) MUST be rejected
	// with codes.InvalidArgument (S3.11). Asserted specifically via IsInvalidArgument.
	rejErr := transport.ModifyAckDeadline(ctx, w.subClient, subPath, ackID, 5)
	if transport.IsInvalidArgument(rejErr) {
		w.recordLeaseExtendRejectConfirmed()
	} else {
		w.logger.Error("lease-extend probe: out-of-range deadline NOT rejected with INVALID_ARGUMENT",
			"err", errString(rejErr))
	}

	// Extend the lease well past the original ack deadline.
	extendSeconds := ackDeadline + 30
	if extendSeconds > 600 {
		extendSeconds = 600
	}
	if err := transport.ModifyAckDeadline(ctx, w.subClient, subPath, ackID, extendSeconds); err != nil {
		w.logger.Error("lease-extend probe: extend ModifyAckDeadline failed", "err", errString(err))
		return
	}

	// Hold past the ORIGINAL ack deadline (plus the 250ms sweeper slack). If the
	// lease was truly extended the message must NOT redeliver in this window.
	holdFor := time.Duration(ackDeadline)*time.Second + 3*time.Second
	if !sleepCtx(ctx, holdFor) {
		return
	}

	// Within the extended window the message MUST NOT have redelivered. A no-ack
	// Pull here should NOT return our message; if it does, the lease was not held.
	if redeliveredAckID, got := w.pullOneAckID(ctx, subPath); got {
		w.recordError("receive_failure")
		w.logger.Error("lease-extend probe: message REDELIVERED within extended window (lease not held)")
		// Best-effort drain so the probe sub does not retain a leaked lease.
		_ = transport.CrossStreamAckProbe(ctx, w.subClient, subPath, redeliveredAckID)
		_ = transport.CrossStreamAckProbe(ctx, w.subClient, subPath, ackID)
		return
	}

	// Non-redelivery confirmed: ack the held message to release the lease cleanly.
	if err := transport.CrossStreamAckProbe(ctx, w.subClient, subPath, ackID); err != nil {
		w.logger.Warn("lease-extend probe: final ack failed", "err", errString(err))
	}
	w.recordLeaseExtendConfirmed()
	w.logger.Info("lease-extend non-redelivery confirmed",
		"extend_seconds", extendSeconds, "held_for", holdFor.String())
}

// crossStreamAckProbe (REQ-013 / S5.8) asserts subscription-owned leases enable a
// cross-stream ack: a message delivered on one StreamingPull stream can be acked
// via a DIFFERENT path (here the unary low-level Acknowledge) and is then NOT
// redelivered. It publishes one message, opens a StreamingPull stream (stream A)
// to capture the ack id, acks it via the unary path (stream B), and asserts the
// ack succeeds (codes.OK) and the message is not redelivered. Deterministic
// HARD-FAIL on ack rejection or redelivery, guarded by CrossStreamAckProbeRan.
func (w *StreamingPullWorker) crossStreamAckProbe(ctx context.Context) {
	topic, subPath, err := w.provisionProbe(ctx, "csprobe", w.cfg.GCP.AckDeadlineSeconds)
	if err != nil {
		w.logger.Warn("cross-stream ack probe provisioning failed (skipped)", "err", errString(err))
		return
	}

	body, crcHex := payload.Build(w.selectMessageSize())
	if _, err := transport.PublishSync(ctx, topic, body, stampAttrs("csprobe", 1, crcHex)); err != nil {
		if ctx.Err() != nil {
			return
		}
		w.logger.Warn("cross-stream ack probe publish failed (skipped)", "err", errString(err))
		return
	}

	// Stream A: a low-level StreamingPull receive to capture the message's ack id.
	ackID, ok := w.streamOneAckID(ctx, subPath)
	if !ok {
		w.logger.Warn("cross-stream ack probe: no message received on stream A (skipped)")
		return
	}

	// The probe ran to completion (message delivered on stream A): the gate is live.
	w.markCrossStreamAckProbeRan()

	// Stream B (the unary low-level Acknowledge path): ack the ack id minted by the
	// delivery on stream A. Subscription-owned leases resolve it (codes.OK, S3.13).
	if err := transport.CrossStreamAckProbe(ctx, w.subClient, subPath, ackID); err != nil {
		w.logger.Error("cross-stream ack probe: ack on a different path was REJECTED",
			"err", errString(err))
		return
	}

	// Confirm the message is not redelivered: a follow-up Pull must not return it.
	if redeliveredAckID, got := w.pullOneAckID(ctx, subPath); got {
		w.logger.Error("cross-stream ack probe: message REDELIVERED after a successful cross-stream ack")
		_ = transport.CrossStreamAckProbe(ctx, w.subClient, subPath, redeliveredAckID)
		return
	}

	w.recordCrossStreamAckConfirmed()
	w.logger.Info("cross-stream ack confirmed (subscription-owned lease)")
}

// pullOneAckID issues a single low-level unary Pull for one message and returns
// its ack id. ok is false when no message is available or ctx is cancelled. The
// probe deliberately does not ack here (the caller owns the ack-or-extend).
func (w *StreamingPullWorker) pullOneAckID(ctx context.Context, subPath string) (ackID string, ok bool) {
	resp, err := w.subClient.Pull(ctx, &pubsubpb.PullRequest{
		Subscription: subPath,
		MaxMessages:  1,
	})
	if err != nil {
		if ctx.Err() == nil {
			w.logger.Warn("probe pull failed", "err", errString(err))
		}
		return "", false
	}
	msgs := resp.GetReceivedMessages()
	if len(msgs) == 0 {
		return "", false
	}
	return msgs[0].GetAckId(), true
}

// streamOneAckID opens a low-level StreamingPull stream (stream A for the
// cross-stream probe), receives one message, and returns its ack id WITHOUT
// acking on this stream. The stream is closed before returning so the lease is
// retained (subscription-owned) for the cross-stream ack on a different path.
func (w *StreamingPullWorker) streamOneAckID(ctx context.Context, subPath string) (ackID string, ok bool) {
	streamCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	stream, err := w.subClient.StreamingPull(streamCtx)
	if err != nil {
		w.logger.Warn("probe streaming pull open failed", "err", errString(err))
		return "", false
	}
	if err := stream.Send(&pubsubpb.StreamingPullRequest{
		Subscription:             subPath,
		StreamAckDeadlineSeconds: int32(w.cfg.GCP.AckDeadlineSeconds),
	}); err != nil {
		w.logger.Warn("probe streaming pull send failed", "err", errString(err))
		return "", false
	}
	// A bounded number of empty recvs before giving up (the message may take a poll
	// tick to surface). Receiving WITHOUT acking retains the subscription-owned lease.
	for empty := 0; empty < 10; empty++ {
		if streamCtx.Err() != nil {
			return "", false
		}
		resp, recvErr := stream.Recv()
		if recvErr != nil {
			if recvErr == io.EOF || streamCtx.Err() != nil {
				return "", false
			}
			w.logger.Warn("probe streaming pull recv failed", "err", errString(recvErr))
			return "", false
		}
		msgs := resp.GetReceivedMessages()
		if len(msgs) == 0 {
			continue
		}
		return msgs[0].GetAckId(), true
	}
	return "", false
}

// StartProducers launches the Publish loop(s).
func (w *StreamingPullWorker) StartProducers() {
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

func (w *StreamingPullWorker) produceLoop(ctx context.Context, producerIdx int) {
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

// StopConsumers stops the receiver + probe goroutine, closes the low-level
// subscriber client and the probe topics' publishers, and closes the client.
func (w *StreamingPullWorker) StopConsumers() {
	w.BaseWorker.StopConsumers()
	if w.subClient != nil {
		_ = w.subClient.Close()
	}
	if w.topic != nil {
		w.topic.Stop()
	}
	for _, t := range w.probeTopics {
		t.Stop()
	}
	if w.client != nil {
		_ = w.client.Close()
	}
}
