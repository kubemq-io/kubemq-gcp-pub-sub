package worker

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"cloud.google.com/go/pubsub"
	vkit "cloud.google.com/go/pubsub/apiv1"
	"cloud.google.com/go/pubsub/apiv1/pubsubpb"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/kubemq-io/kubemq-gcp-pub-sub/burnin/config"
	"github.com/kubemq-io/kubemq-gcp-pub-sub/burnin/payload"
	"github.com/kubemq-io/kubemq-gcp-pub-sub/burnin/transport"
)

// deadLetterBudget is the per-message grace period before an un-dead-lettered
// poison message is charged as loss. Dead-lettering is driven by NATURAL
// ack-deadline expiry: with the minimum 10s ack deadline and
// max_delivery_attempts cycles plus the 250ms sweeper, a poison message reaches
// the DLQ within roughly ack_deadline*(attempts+1). The budget bounds that with
// generous slack so an in-flight message mid-cycle is never false-counted as loss.
func deadLetterBudget(cfg *config.Config) time.Duration {
	attempts := cfg.GCP.MaxDeliveryAttempts
	if attempts < 5 {
		attempts = 5
	}
	return time.Duration(cfg.GCP.AckDeadlineSeconds)*time.Second*time.Duration(attempts+2) + 15*time.Second
}

// DeadLetterWorker (worker 5, spec S6.3) exercises connector-level
// dead-lettering. The source subscription has a DeadLetterPolicy (DLQ topic +
// max_delivery_attempts, 5..100) and a tight RetryPolicy. A driver pulls the
// source WITHOUT acking so each lease expires; the 250ms sweeper raises the
// receive count and, once it exceeds max_delivery_attempts, republishes the
// message to the DLQ topic and acks the original (spec S3.5). A DLQ consumer
// drains the dead-letter subscription. The verdict gate is NO dead-letter loss:
// every poison message must surface on the DLQ within its budget (spec S6.6).
// Delivery is at-least-once by design, so the duplication/throughput gates do
// not apply (the engine exempts this worker).
type DeadLetterWorker struct {
	*BaseWorker
	srcTopicID string
	dltTopicID string
	srcSubID   string
	dltSubID   string

	client    *pubsub.Client
	srcTopic  *pubsub.Topic
	dltTopic  *pubsub.Topic
	srcSub    *pubsub.Subscription
	dltSub    *pubsub.Subscription
	subClient *vkit.SubscriberClient
	srcPath   string

	seq atomic.Uint64

	// published tracks each poison message id -> publish time; republished marks
	// the ids confirmed on the DLQ. At drain, a published id not republished and
	// older than the budget is charged as dead-letter loss.
	mu          sync.Mutex
	published   map[string]time.Time
	republished map[string]struct{}

	// Retry-backoff probe (REQ-010, spec S5.9) resources, provisioned only when
	// cfg.GCP.RetryBackoffProbe is set. Kept separate from the source/DLQ state so
	// the probe is fully isolated from the no-loss tracker.
	probeTopicID string
	probeSubID   string
	probeTopic   *pubsub.Topic
}

// NewDeadLetterWorker creates a dead_letter worker.
func NewDeadLetterWorker(cfg *config.Config, idx int, logger *slog.Logger) Worker {
	srcTopicID := transport.TopicName(config.WorkerDeadLetter, idx)
	dltTopicID := transport.DeadLetterTopicName(config.WorkerDeadLetter, idx)
	srcSubID := transport.SubscriptionName(config.WorkerDeadLetter, idx)
	dltSubID := transport.DeadLetterSubscriptionName(config.WorkerDeadLetter, idx)
	return &DeadLetterWorker{
		BaseWorker: NewBaseWorker(config.WorkerDeadLetter,
			transport.MappedTopicChannel(srcTopicID), idx, cfg, logger),
		srcTopicID:   srcTopicID,
		dltTopicID:   dltTopicID,
		srcSubID:     srcSubID,
		dltSubID:     dltSubID,
		probeTopicID: srcTopicID + "_rbprobe",
		probeSubID:   srcSubID + "_rbprobe",
		published:    make(map[string]time.Time),
		republished:  make(map[string]struct{}),
	}
}

// Start provisions both topics, the source sub (with DeadLetterPolicy), the DLQ
// sub, and brings up the no-ack source driver + the DLQ consumer.
func (w *DeadLetterWorker) Start(ctx context.Context) error {
	w.consumerCtx, w.consumerCancel = context.WithCancel(ctx)

	client, err := transport.NewPubSubClient(ctx, w.psCfg)
	if err != nil {
		return fmt.Errorf("build pubsub client: %w", err)
	}
	w.client = client

	if w.srcTopic, err = ensureTopic(ctx, client, w.srcTopicID); err != nil {
		return err
	}
	if w.dltTopic, err = ensureTopic(ctx, client, w.dltTopicID); err != nil {
		return err
	}

	maxAttempts := w.cfg.GCP.MaxDeliveryAttempts
	if maxAttempts < 5 {
		maxAttempts = 5
	}
	// A tight RetryPolicy re-arms each expired lease almost immediately so the
	// expiry cycle is bounded by the 10s minimum ack deadline (the example pattern).
	w.srcSub, err = ensureSubscription(ctx, client, w.srcSubID, pubsub.SubscriptionConfig{
		Topic:       w.srcTopic,
		AckDeadline: transport.AckDeadline(w.cfg.GCP.AckDeadlineSeconds),
		DeadLetterPolicy: &pubsub.DeadLetterPolicy{
			DeadLetterTopic:     fmt.Sprintf("projects/%s/topics/%s", w.psCfg.ProjectID, w.dltTopicID),
			MaxDeliveryAttempts: maxAttempts,
		},
		RetryPolicy: &pubsub.RetryPolicy{
			MinimumBackoff: 10 * time.Millisecond,
			MaximumBackoff: 50 * time.Millisecond,
		},
	})
	if err != nil {
		return err
	}
	w.dltSub, err = ensureSubscription(ctx, client, w.dltSubID, pubsub.SubscriptionConfig{
		Topic:       w.dltTopic,
		AckDeadline: transport.AckDeadline(w.cfg.GCP.AckDeadlineSeconds),
	})
	if err != nil {
		return err
	}

	// Low-level client for the no-ack source pulls (the high-level Receive loop
	// auto-extends leases, which would prevent the natural expiry this depends on).
	w.subClient, err = vkit.NewSubscriberClient(ctx, emulatorClientOptions(w.psCfg.EmulatorHost)...)
	if err != nil {
		return fmt.Errorf("build apiv1 subscriber client: %w", err)
	}
	w.srcPath = fmt.Sprintf("projects/%s/subscriptions/%s", w.psCfg.ProjectID, w.srcSubID)

	// DLQ consumer: drains the dead-letter subscription.
	transport.ConfigureReceive(w.dltSub, 1)
	w.consumerWG.Add(1)
	go func() {
		defer w.consumerWG.Done()
		w.dlqConsumeLoop(w.consumerCtx)
	}()

	// Source driver: pulls without acking so leases expire and receive counts rise.
	w.consumerWG.Add(1)
	go func() {
		defer w.consumerWG.Done()
		w.sourceDriveLoop(w.consumerCtx)
	}()

	// Retry-backoff probe (REQ-010): opt-in (gcp.retry_backoff_probe). Isolated
	// one-shot observation of the exponential expiry-redelivery backoff curve on a
	// dedicated probe topic/sub with a NON-tight RetryPolicy. Off by default so the
	// steady dead-letter cycle (tight RetryPolicy) is unaffected.
	if w.cfg.GCP.RetryBackoffProbe {
		w.consumerWG.Add(1)
		go func() {
			defer w.consumerWG.Done()
			w.retryBackoffProbe(w.consumerCtx)
		}()
	}
	return nil
}

// sourceDriveLoop continuously pulls the source subscription WITHOUT acking, so
// the sweeper expires each lease and raises the receive count toward the DLQ
// threshold. It never acks or nacks — only natural expiry drives the count.
func (w *DeadLetterWorker) sourceDriveLoop(ctx context.Context) {
	w.signalReady()
	for {
		if ctx.Err() != nil {
			return
		}
		resp, err := w.subClient.Pull(ctx, &pubsubpb.PullRequest{
			Subscription: w.srcPath,
			MaxMessages:  100,
		})
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			w.recordError("receive_failure")
			if !sleepCtx(ctx, 200*time.Millisecond) {
				return
			}
			continue
		}
		// Deliberately NO Ack / NO Nack: let each lease expire so the sweeper
		// re-arms it and raises the receive count toward max_delivery_attempts.
		//
		// REQ-012 (spec S5.9 / S3.12): because the source subscription carries a
		// DeadLetterPolicy, the connector surfaces the per-message delivery-attempt
		// count on the LOW-LEVEL ReceivedMessage (mapper.go:149-150 sets it from the
		// broker ReceiveCount ONLY when MaxDeliveryAttempts>0 && DeadLetterTopic!="").
		// It is read here via rm.GetDeliveryAttempt() (int32) — NOT the high-level
		// pubsub.Message.DeliveryAttempt (*int), which is not on this code path. The
		// max observed attempt feeds the DeliveryAttemptMax gauge; the verdict gate
		// HARD-FAILs if a republish was observed yet the attempt count never rose
		// above 0 (the DLQ policy failed to surface attempts).
		rms := resp.GetReceivedMessages()
		for _, rm := range rms {
			if a := rm.GetDeliveryAttempt(); a > 0 {
				w.setDeliveryAttemptMax(uint64(a))
			}
		}
		if len(rms) == 0 {
			if !sleepCtx(ctx, 100*time.Millisecond) {
				return
			}
		}
	}
}

// dlqConsumeLoop drains the dead-letter subscription, acking each republished
// poison message and reconciling it against the published set.
func (w *DeadLetterWorker) dlqConsumeLoop(ctx context.Context) {
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

		recvCtx, cancel := context.WithCancel(ctx)
		go watchDisconnect(recvCtx, cancel, gen, w.disconnectGeneration)

		err := transport.Receive(recvCtx, w.dltSub, func(_ context.Context, m *pubsub.Message) {
			w.handleDLQMessage(m)
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

func (w *DeadLetterWorker) handleDLQMessage(m *pubsub.Message) {
	w.recordReceived(len(m.Data), "c-dlq-"+w.channelName)

	producerID, seq, _, sentAt, ok := extractMeta(m.Attributes)
	if ok {
		if !sentAt.IsZero() {
			w.recordLatency(time.Since(sentAt))
		}
		id := fmt.Sprintf("%s#%d", producerID, seq)
		w.mu.Lock()
		if _, already := w.republished[id]; !already {
			w.republished[id] = struct{}{}
			w.mu.Unlock()
			w.recordDeadLetterRepublished()
		} else {
			w.mu.Unlock()
		}
	}
	transport.Ack(m)
	w.recordAcked()
}

// StartProducers launches the poison-message Publish loop(s).
func (w *DeadLetterWorker) StartProducers() {
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

func (w *DeadLetterWorker) produceLoop(ctx context.Context, producerIdx int) {
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
		id := fmt.Sprintf("%s#%d", producerID, seq)

		start := time.Now()
		if _, err := transport.PublishSync(ctx, w.srcTopic, body, stampAttrs(producerID, seq, crcHex)); err != nil {
			if ctx.Err() != nil {
				return
			}
			w.recordError("publish_failure")
			continue
		}
		w.mu.Lock()
		w.published[id] = time.Now()
		w.mu.Unlock()
		metricObserveSend(w.name, time.Since(start))
		w.recordSent(len(body), producerID)
	}
}

// ResetAfterWarmup drops warmup-era reconciliation state in addition to the
// BaseWorker counter reset, so the no-loss gate measures only the post-warmup
// window: published/republished entries accumulated during warmup must not leak
// into the dead-letter loss accounting (spec warmup isolation).
func (w *DeadLetterWorker) ResetAfterWarmup() {
	w.BaseWorker.ResetAfterWarmup()
	w.mu.Lock()
	w.published = make(map[string]time.Time)
	w.republished = make(map[string]struct{})
	w.mu.Unlock()
}

// StopConsumers stops the driver + DLQ consumer, reconciles dead-letter loss, and
// closes the clients. A published poison message that never surfaced on the DLQ
// and is older than the budget is charged as loss (the no-loss gate).
func (w *DeadLetterWorker) StopConsumers() {
	w.reconcileDeadLetterLoss()
	w.BaseWorker.StopConsumers()
	if w.subClient != nil {
		_ = w.subClient.Close()
	}
	if w.srcTopic != nil {
		w.srcTopic.Stop()
	}
	if w.dltTopic != nil {
		w.dltTopic.Stop()
	}
	if w.probeTopic != nil {
		w.probeTopic.Stop()
	}
	if w.client != nil {
		_ = w.client.Close()
	}
}

func (w *DeadLetterWorker) reconcileDeadLetterLoss() {
	budget := deadLetterBudget(w.cfg)
	now := time.Now()
	var lost uint64
	w.mu.Lock()
	for id, pubTime := range w.published {
		if _, ok := w.republished[id]; ok {
			continue
		}
		// Only charge loss for messages that had enough time to dead-letter; a
		// still-in-flight message at the drain boundary is not loss.
		if now.Sub(pubTime) > budget {
			lost++
		}
	}
	w.mu.Unlock()
	if lost > 0 {
		w.recordDeadLetterLost(lost)
		w.logger.Warn("dead-letter loss", "lost", lost, "budget", budget.String())
	}
}

// retryBackoffProbe (REQ-010, spec S5.9) is a one-shot, tracker-isolated probe
// that observes the connector's EXPONENTIAL ack-deadline-expiry redelivery
// backoff (S3.10: delay = min_ms * 2^(rcv-1), clamped to [min,max]). The steady
// dead_letter cycle uses a TIGHT RetryPolicy (10ms/50ms) to stay bounded, which
// does NOT exhibit a meaningful curve, so the probe provisions a SEPARATE topic
// and subscription with a NON-tight RetryPolicy (Min 1s / Max 8s → ~1s,2s,4s,8s)
// and NO dead-letter policy (the message must keep redelivering via expiry, never
// dead-letter). It publishes one message and drives no-ack low-level Pulls,
// timing each redelivery: a transition from "not currently visible" (lease held)
// to "visible again" marks one expiry+backoff boundary. Two consecutive growing
// inter-redelivery gaps within the bounded window confirm exponential backoff.
//
// Classification: TIMING-NONDETERMINISTIC → NOTE only (spec S5.10). The probe
// NEVER fails the verdict; it sets RetryBackoffObserved=1 on a positive
// observation and otherwise stays silent (the verdict emits the "not conclusively
// observed" NOTE). All resources are torn down in StopConsumers.
func (w *DeadLetterWorker) retryBackoffProbe(ctx context.Context) {
	probeTopic, err := ensureTopic(ctx, w.client, w.probeTopicID)
	if err != nil {
		w.logger.Warn("retry-backoff probe: ensure topic", "err", err)
		return
	}
	w.probeTopic = probeTopic

	// NON-tight RetryPolicy so the expiry-redelivery curve is observable; NO
	// DeadLetterPolicy so the message redelivers indefinitely instead of moving to
	// a DLQ. The 10s minimum ack deadline plus the 250ms sweeper bound each cycle.
	// The handle is unused: the no-ack drive uses the low-level apiv1 Pull path
	// (w.subClient) by subscription path, mirroring sourceDriveLoop. Provisioning
	// is the only thing needed from the high-level call.
	if _, err = ensureSubscription(ctx, w.client, w.probeSubID, pubsub.SubscriptionConfig{
		Topic:       probeTopic,
		AckDeadline: transport.AckDeadline(10),
		RetryPolicy: &pubsub.RetryPolicy{
			MinimumBackoff: 1 * time.Second,
			MaximumBackoff: 8 * time.Second,
		},
	}); err != nil {
		w.logger.Warn("retry-backoff probe: ensure subscription", "err", err)
		return
	}
	probePath := fmt.Sprintf("projects/%s/subscriptions/%s", w.psCfg.ProjectID, w.probeSubID)

	// Publish a single probe message (not part of the no-loss tracker).
	body, crcHex := payload.Build(w.selectMessageSize())
	if _, err := transport.PublishSync(ctx, probeTopic, body, stampAttrs("p-rbprobe-"+w.channelName, 0, crcHex)); err != nil {
		if ctx.Err() == nil {
			w.logger.Warn("retry-backoff probe: publish", "err", err)
		}
		return
	}

	// Bounded observation window: the curve 1s→2s→4s→8s plus the 10s ack deadline
	// per cycle needs roughly a minute to surface three boundaries; cap generously
	// so the probe never blocks the run past the soak window.
	const window = 90 * time.Second
	deadline := time.Now().Add(window)

	var lastSeen time.Time // when the message was last observed visible
	var lastBoundary time.Time
	var gaps []time.Duration
	visible := false

	for time.Now().Before(deadline) {
		if ctx.Err() != nil {
			return
		}
		resp, err := w.subClient.Pull(ctx, &pubsubpb.PullRequest{
			Subscription: probePath,
			MaxMessages:  1,
		})
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			if !sleepCtx(ctx, 200*time.Millisecond) {
				return
			}
			continue
		}

		now := time.Now()
		got := len(resp.GetReceivedMessages()) > 0
		if got {
			// A redelivery boundary: the message became visible again after having
			// been leased (not visible) on the previous polls.
			if !visible {
				if !lastBoundary.IsZero() {
					gaps = append(gaps, now.Sub(lastBoundary))
				}
				lastBoundary = now
			}
			visible = true
			lastSeen = now
			// Deliberately NO Ack / NO Nack — let the lease expire so the next
			// redelivery applies the next step of the backoff curve.
		} else if visible && now.Sub(lastSeen) > 1500*time.Millisecond {
			// The lease is held (message no longer returned by Pull); mark it not
			// visible so the next reappearance counts as a fresh boundary.
			visible = false
		}

		// Two consecutive STRICTLY growing inter-redelivery gaps (with slack for
		// sweeper/scheduling jitter) confirm the exponential curve.
		if growingGaps(gaps) {
			w.recordRetryBackoffObserved()
			w.logger.Info("retry-backoff probe: exponential backoff observed",
				"gaps", fmt.Sprintf("%v", gaps))
			return
		}

		if !sleepCtx(ctx, 150*time.Millisecond) {
			return
		}
	}
	w.logger.Info("retry-backoff probe: backoff not conclusively observed in window",
		"gaps", fmt.Sprintf("%v", gaps))
}

// growingGaps reports whether the observed inter-redelivery gaps contain two
// consecutive strictly-increasing steps consistent with a geometric backoff
// (later gap at least ~1.5x the previous, allowing for the 250ms sweeper
// granularity and scheduling jitter). It is intentionally lenient: a false
// negative only yields the "not conclusively observed" NOTE (never a failure).
func growingGaps(gaps []time.Duration) bool {
	if len(gaps) < 2 {
		return false
	}
	for i := 1; i < len(gaps); i++ {
		if gaps[i] >= gaps[i-1]*3/2 {
			return true
		}
	}
	return false
}

// emulatorClientOptions wires a low-level apiv1 client at the connector exactly
// the way the high-level pubsub.NewClient does when PUBSUB_EMULATOR_HOST is set:
// insecure gRPC, credentials cleared, no Google auth. The bare apiv1 client does
// NOT auto-detect the env var, so the options are wired explicitly.
func emulatorClientOptions(emulatorHost string) []option.ClientOption {
	host := emulatorHost
	if host == "" {
		host = os.Getenv(transport.EmulatorEnv)
	}
	if host == "" {
		return nil
	}
	return []option.ClientOption{
		option.WithEndpoint(host),
		option.WithGRPCDialOption(grpc.WithTransportCredentials(insecure.NewCredentials())),
		option.WithoutAuthentication(),
	}
}
