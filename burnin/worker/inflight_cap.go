package worker

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"cloud.google.com/go/pubsub"
	vkit "cloud.google.com/go/pubsub/apiv1"
	"cloud.google.com/go/pubsub/apiv1/pubsubpb"

	"github.com/kubemq-io/kubemq-gcp-pub-sub/burnin/config"
	"github.com/kubemq-io/kubemq-gcp-pub-sub/burnin/payload"
	"github.com/kubemq-io/kubemq-gcp-pub-sub/burnin/transport"
)

// connectorInflightCap is the connector's default MaxInflightPerSubscription
// (env CONNECTORS_PUB_SUB_MAX_INFLIGHT_PER_SUBSCRIPTION, doc 20-google-pubsub.md
// line 292; spec S3.6): the hard cap on leased (un-acked) messages per
// subscription. The worker's backlog target (gcp.inflight_backlog_target,
// default 25000) is sized ABOVE this so the cap is actually driven. It is used
// only for logging/sanity context — the assertion is no-loss, not a numeric cap
// readout, because the connector enforces the cap SILENTLY on the low-level Pull
// path: when Register returns errTooManyInflight (codes.ResourceExhausted
// internally) the connector NAcks the unleased tail back for redelivery and
// returns a SHORTER Pull response (connectors/pubsub/subscriber.go:433-438,
// nackRemaining 456-473) rather than surfacing ResourceExhausted to the puller.
const connectorInflightCap = 20000

// inflightPullBatch is the per-Pull MaxMessages for the low-level accumulate /
// drain loop. It must stay <= the connector's maxPullMessages guard
// (subscriber.go:410). 100 mirrors dead_letter.go:174 and keeps each Pull RPC
// cheap while still building the backlog quickly at high publish rate.
const inflightPullBatch = 100

// maxAckDeadlineSeconds is the connector's absolute ack-deadline ceiling (S3.11,
// doc line 110: ">0 = extend (10..600s)"). The inflight_cap subscription holds
// many un-acked leases to drive the cap, so it uses the maximum ack deadline to
// keep those leases from expiring-and-redelivering DURING accumulation — the only
// redeliveries the assertion wants are the connector's over-cap NAck-back and the
// deliberate drain release.
const maxAckDeadlineSeconds = 600

// InflightCapWorker (NEW, spec REQ-006/S5.6, opt-in, default-ON only in
// bounded-soak) drives a backlog of un-acked leased messages toward / beyond
// MaxInflightPerSubscription (connector default 20000, S3.6) on a single
// subscription, then asserts the tail beyond the cap is held back / redelivered
// later (NOT lost). It uses a low-level vkit.SubscriberClient no-ack Pull loop
// (pattern from dead_letter.go:138-144,172) to accumulate leases without acking
// during the measurement window; once producers stop, the puller flips to drain
// mode, acks everything it holds and keeps pulling+acking the redelivered tail
// until the published total reconciles.
//
// Tracker invariant (REQ-019/S5.6): every PUBLISH registers its seq exactly once
// (recordSent + a tracked send); the held (un-acked) tail is IN-FLIGHT, not lost;
// on release it redelivers and reconciles via pendingLost. Each terminal
// delivery is recordTracked exactly once. Because the connector NAcks the
// over-cap tail back for redelivery, delivery is at-least-once BY DESIGN, so the
// DUP gate is exempted for this worker (config.IsInflightCapWorker, verdict.go:56)
// — NOT a global max_duplication_pct change. The LOSS gate is fully retained.
// The soak profile sizes message.reorder_window >= gcp.inflight_backlog_target
// (validated in config.go:631) so held seqs are not prematurely evicted into
// pendingLost during accumulation.
type InflightCapWorker struct {
	*BaseWorker
	topicID string
	subID   string

	client  *pubsub.Client
	topic   *pubsub.Topic
	sub     *pubsub.Subscription
	subPath string

	subClient *vkit.SubscriberClient

	seq           atomic.Uint64
	backlogTarget int

	// draining flips false→true once producers stop so the accumulate loop stops
	// holding leases and begins acking (releasing the tail for redelivery).
	draining atomic.Bool
	// released marks the instant the worker switched to drain mode; deliveries
	// observed after it are counted as the redelivered tail.
	released atomic.Int64

	// held collects the ack ids the no-ack accumulate loop is leasing but has not
	// yet acked. On the drain switch they are all acked at once (releasing the
	// cap) and the map is cleared.
	mu   sync.Mutex
	held map[string]struct{}
}

// NewInflightCapWorker creates an inflight_cap worker.
func NewInflightCapWorker(cfg *config.Config, idx int, logger *slog.Logger) Worker {
	topicID := transport.TopicName(config.WorkerInflightCap, idx)
	subID := transport.SubscriptionName(config.WorkerInflightCap, idx)
	target := cfg.GCP.InflightBacklogTarget
	if target < 1 {
		target = connectorInflightCap + 5000
	}
	return &InflightCapWorker{
		BaseWorker: NewBaseWorker(config.WorkerInflightCap,
			transport.MappedTopicChannel(topicID), idx, cfg, logger),
		topicID:       topicID,
		subID:         subID,
		backlogTarget: target,
		held:          make(map[string]struct{}),
	}
}

// Start provisions the topic + subscription and builds the low-level
// SubscriberClient used for the no-ack accumulate / ack-drain loop, then brings
// up that loop. The subscription's ack deadline is set to the connector maximum
// (600s, S3.11) so held leases do NOT expire-and-redeliver DURING accumulation —
// the only redelivery we want is the over-cap tail the connector NAcks back, plus
// the deliberate release at drain — which keeps the no-loss assertion clean.
func (w *InflightCapWorker) Start(ctx context.Context) error {
	w.consumerCtx, w.consumerCancel = context.WithCancel(ctx)

	client, err := transport.NewPubSubClient(ctx, w.psCfg)
	if err != nil {
		return fmt.Errorf("build pubsub client: %w", err)
	}
	w.client = client

	if w.topic, err = ensureTopic(ctx, client, w.topicID); err != nil {
		return err
	}

	// AckDeadline at the connector max keeps leases held long enough to drive the
	// cap without natural expiry redelivery muddying the assertion. ConfigureReceive
	// is not used: this worker pulls via the low-level client (the high-level
	// Receive loop applies its own client-side flow control and auto-extends
	// leases, which would both throttle the backlog below the server cap and hide
	// the over-cap NAck-back the test depends on).
	w.sub, err = ensureSubscription(ctx, client, w.subID, pubsub.SubscriptionConfig{
		Topic:       w.topic,
		AckDeadline: transport.AckDeadline(maxAckDeadlineSeconds),
	})
	if err != nil {
		return err
	}

	w.subClient, err = transport.NewSubscriberClient(ctx, w.psCfg)
	if err != nil {
		return fmt.Errorf("build apiv1 subscriber client: %w", err)
	}
	w.subPath = transport.SubscriptionPath(w.psCfg.ProjectID, w.subID)

	w.consumerWG.Add(1)
	go func() {
		defer w.consumerWG.Done()
		w.pullLoop(w.consumerCtx)
	}()
	return nil
}

// pullLoop is the single low-level Pull driver. While draining==false it
// ACCUMULATES: it leases (Pulls) messages, records each terminal delivery in the
// tracker exactly once, and HOLDS the ack id without acking — building the
// backlog toward / beyond the server cap. The connector NAcks the over-cap tail
// back for redelivery (subscriber.go:437), so the un-acked count this puller
// holds stays bounded by the cap (the cap manifests as a bounded outstanding
// count, NOT a surfaced ResourceExhausted error). Once producers stop, draining
// flips true: the loop acks everything held (releasing the cap) and keeps
// pulling+acking so the redelivered tail lands and reconciles via pendingLost.
func (w *InflightCapWorker) pullLoop(ctx context.Context) {
	w.signalReady()
	for {
		if ctx.Err() != nil {
			return
		}

		resp, err := w.subClient.Pull(ctx, &pubsubpb.PullRequest{
			Subscription: w.subPath,
			MaxMessages:  inflightPullBatch,
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

		msgs := resp.GetReceivedMessages()
		if len(msgs) == 0 {
			// Backlog momentarily empty. In drain mode an empty pull means the tail
			// has fully redelivered + been acked; keep polling briefly so any late
			// redelivery still lands before consumer shutdown.
			if !sleepCtx(ctx, 100*time.Millisecond) {
				return
			}
			continue
		}

		drain := w.draining.Load()
		for _, rm := range msgs {
			w.handlePulled(ctx, rm, drain)
		}

		// Bound the held set to the server cap: if accumulation has reached the
		// cap the connector is already NAcking the tail back, so the held count
		// cannot grow further — sample the outstanding gauge and keep leasing the
		// redelivered churn so producers never block on a full queue.
		if !drain {
			w.mu.Lock()
			n := len(w.held)
			w.mu.Unlock()
			w.setOutstanding(float64(n))
		}
	}
}

// handlePulled records one low-level-pulled message. The seq is tracked exactly
// once on its terminal delivery (recordTracked). In accumulate mode the ack id
// is HELD (no ack) to build the backlog; in drain mode the message is acked
// immediately (releasing the lease) and, if it arrived after the release switch,
// counted as the redelivered tail.
func (w *InflightCapWorker) handlePulled(ctx context.Context, rm *pubsubpb.ReceivedMessage, drain bool) {
	pm := rm.GetMessage()
	ackID := rm.GetAckId()
	w.recordReceived(len(pm.GetData()), "c-"+w.channelName)

	producerID, seq, crcHex, sentAt, ok := extractMeta(pm.GetAttributes())
	if ok {
		if crcHex != "" && !payload.VerifyCRC(pm.GetData(), crcHex) {
			w.recordCorrupted()
		}
		if !sentAt.IsZero() {
			w.recordLatency(time.Since(sentAt))
		}
		// Exactly one tracked send per PUBLISH; a redelivery below/at the
		// contiguous watermark recovers the seq from pendingLost (no false loss)
		// or counts a bounded — DUP-gate-exempt — duplicate.
		w.recordTracked(producerID, seq)
	}

	if !drain {
		// Accumulate: hold the lease (do NOT ack) to build the backlog. The
		// connector caps the leased set per subscription and NAcks the over-cap
		// tail back for redelivery, so this map stays bounded by the server cap.
		w.mu.Lock()
		w.held[ackID] = struct{}{}
		w.mu.Unlock()
		return
	}

	// Drain: ack immediately to release the lease. A delivery seen after the
	// release switch is the redelivered tail the cap held back (the assertion's
	// positive signal: the tail came back, it was not lost).
	w.ackOne(ctx, ackID)
	w.recordAcked()
	if rel := w.released.Load(); rel > 0 && time.Now().UnixNano() >= rel {
		w.recordInflightCapTailRedelivered()
	}
}

// ackOne acks a single ack id via the low-level unary Acknowledge. An ack error
// is non-fatal (the lease will expire and the message redeliver, still
// reconciling via pendingLost); it is recorded so the error-rate gate sees it.
func (w *InflightCapWorker) ackOne(ctx context.Context, ackID string) {
	if ackID == "" {
		return
	}
	if err := w.subClient.Acknowledge(ctx, &pubsubpb.AcknowledgeRequest{
		Subscription: w.subPath,
		AckIds:       []string{ackID},
	}); err != nil {
		if ctx.Err() == nil {
			w.recordError("ack_failure")
		}
	}
}

// StartProducers publishes backlogTarget instrumented messages at the worker's
// configured rate, each registered as a tracked send exactly once. Publishing
// the known total (rather than an open-ended stream) lets the no-loss gate prove
// the FULL backlog — including the over-cap tail the connector NAcks back —
// eventually reconciles. Once the target is reached the loop idles (still under
// producerCtx) so StopProducers stays cheap.
func (w *InflightCapWorker) StartProducers() {
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

func (w *InflightCapWorker) produceLoop(ctx context.Context, producerIdx int) {
	producerID := fmt.Sprintf("p-%s-%d", w.channelName, producerIdx)
	for {
		if ctx.Err() != nil {
			return
		}
		// Publish up to the shared backlog target across all producers, then idle.
		if int(w.seq.Load()) >= w.backlogTarget {
			if !sleepCtx(ctx, 200*time.Millisecond) {
				return
			}
			continue
		}
		if err := w.waitForRate(ctx); err != nil {
			return
		}

		seq := w.seq.Add(1)
		if int(seq) > w.backlogTarget {
			// Another producer already hit the target between the check and Add.
			continue
		}
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

// StopProducers stops the publish loops and then FLIPS the puller to drain mode:
// it acks every lease currently held (releasing the accumulated backlog past the
// cap) and records the release instant so subsequent deliveries are counted as
// the redelivered tail. The puller goroutine continues under consumerCtx, acking
// the redelivered churn until the engine's drain-to-quiescence settles.
func (w *InflightCapWorker) StopProducers() {
	w.BaseWorker.StopProducers()

	// Switch to drain mode BEFORE releasing held leases so any redelivery the
	// release triggers is handled in ack mode (not re-held).
	w.released.Store(time.Now().UnixNano())
	w.draining.Store(true)

	w.mu.Lock()
	heldIDs := make([]string, 0, len(w.held))
	for id := range w.held {
		heldIDs = append(heldIDs, id)
	}
	w.held = make(map[string]struct{})
	w.mu.Unlock()

	ctx := w.consumerCtx
	if ctx == nil {
		ctx = context.Background()
	}
	w.ackMany(ctx, heldIDs)
	w.logger.Info("inflight_cap drain: released held backlog",
		"released_leases", len(heldIDs), "backlog_target", w.backlogTarget,
		"connector_cap", connectorInflightCap)
}

// ackMany acks held ack ids in batches. The connector's Acknowledge accepts many
// ack ids per request, so releasing a large backlog (up to inflight_backlog_target,
// e.g. 25k) at drain is a handful of RPCs instead of tens of thousands of
// sequential unary acks — which otherwise stalled warmup/shutdown for minutes.
func (w *InflightCapWorker) ackMany(ctx context.Context, ackIDs []string) {
	const batch = 1000
	for start := 0; start < len(ackIDs); start += batch {
		end := start + batch
		if end > len(ackIDs) {
			end = len(ackIDs)
		}
		chunk := ackIDs[start:end]
		// Bound each batch: under heavy load the connector's Acknowledge can be slow
		// to respond, and an unbounded ack here stalled shutdown for minutes.
		bctx, cancel := context.WithTimeout(ctx, 10*time.Second)
		err := w.subClient.Acknowledge(bctx, &pubsubpb.AcknowledgeRequest{
			Subscription: w.subPath,
			AckIds:       chunk,
		})
		cancel()
		if err != nil && ctx.Err() == nil {
			w.recordError("ack_failure")
		}
		for range chunk {
			w.recordAcked()
		}
	}
}

// ResetAfterWarmup drops the warmup-era held-lease set in addition to the
// BaseWorker counter/tracker reset, so the no-loss accounting measures only the
// post-warmup window (held ack ids leased during warmup must not leak into the
// measurement-window backlog).
func (w *InflightCapWorker) ResetAfterWarmup() {
	w.BaseWorker.ResetAfterWarmup()
	w.draining.Store(false)
	w.released.Store(0)
	w.seq.Store(0)
	w.mu.Lock()
	w.held = make(map[string]struct{})
	w.mu.Unlock()
}

// StopConsumers stops the pull loop, reconciles the no-loss assertion, and closes
// the clients. The reconciliation reads the tracker's confirmed loss for this
// worker (every published seq must have eventually been received — the over-cap
// tail MUST have redelivered, not vanished) into InflightCapLost; the verdict
// hard-fails when it is > 0 (S5.6, verdict.go:167). The standard tracker loss
// gate (snap.Lost) also applies — InflightCapLost surfaces the same confirmed
// loss under the worker-specific gate for an explicit, named failure.
func (w *InflightCapWorker) StopConsumers() {
	w.BaseWorker.StopConsumers()

	if lost := w.trk.TotalLost(); lost > 0 {
		w.recordInflightCapLost(lost)
		w.logger.Warn("inflight_cap loss: over-cap tail did not redeliver",
			"lost", lost, "sent", w.SentCount(), "received", w.ReceivedCount(),
			"tail_redelivered", w.InflightCapTailRedelivered())
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
