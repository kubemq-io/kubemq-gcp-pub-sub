package worker

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"cloud.google.com/go/pubsub"
	kubemq "github.com/kubemq-io/kubemq-go/v2"

	"github.com/kubemq-io/kubemq-gcp-pub-sub/burnin/config"
	"github.com/kubemq-io/kubemq-gcp-pub-sub/burnin/payload"
	"github.com/kubemq-io/kubemq-gcp-pub-sub/burnin/transport"
)

// interopConvergenceBudget is the per-message grace period before a published
// message that has not yet been consumed natively is charged as divergence. It
// bounds the publish→Events-Store-write→native-receive hop so an in-flight
// message at the drain boundary is not false-counted as diverged.
const interopConvergenceBudget = 10 * time.Second

// CrossProtocolInteropWorker (worker 7, spec S6.3) exercises the connector's
// cross-protocol bridge: the GCP Pub/Sub SDK publishes to a topic, and a NATIVE
// github.com/kubemq-io/kubemq-go/v2 client consumes the topic's Events Store log
// pubsub.{t} (the authoritative, replayable, cross-protocol source — spec S3.3).
//
// Deterministic start-position (spec S6.3): the native Events Store subscription
// is opened with StartFromNewEvents() ("new only") and its subscribe stream is
// confirmed OPEN in Start() — which runs BEFORE the engine starts producers — so
// every published message is in-window for the native consumer (subscribe-before-
// publish, no startup race). The convergence gate matches the native-consumed set
// against the published set: every published message must be consumed natively
// (zero diverged — spec S6.6). The published/consumed sets are reconciled with a
// per-message budget so an in-flight message at drain is not charged as loss.
//
// This is the ONLY worker that uses the native kubemq-go SDK. There is no RPC
// responder — Pub/Sub is pub/sub, not request/reply.
type CrossProtocolInteropWorker struct {
	*BaseWorker
	topicID string
	channel string // the Events Store channel the topic maps to: pubsub.{topicID}

	client    *pubsub.Client
	topic     *pubsub.Topic
	native    *transport.NativeClient
	nativeSub *kubemq.Subscription

	seq atomic.Uint64

	// published tracks each message id -> publish time; consumed marks the ids the
	// native side has read off pubsub.{topicID}. At drain a published id not
	// consumed and older than the budget is charged as interop divergence.
	mu        sync.Mutex
	published map[string]time.Time
	consumed  map[string]struct{}
}

// NewCrossProtocolInteropWorker creates a cross_protocol_interop worker.
func NewCrossProtocolInteropWorker(cfg *config.Config, idx int, logger *slog.Logger) Worker {
	topicID := transport.TopicName(config.WorkerCrossProtocol, idx)
	return &CrossProtocolInteropWorker{
		BaseWorker: NewBaseWorker(config.WorkerCrossProtocol,
			transport.MappedTopicChannel(topicID), idx, cfg, logger),
		topicID:   topicID,
		channel:   transport.MappedTopicChannel(topicID),
		published: make(map[string]time.Time),
		consumed:  make(map[string]struct{}),
	}
}

// Start provisions the topic, dials the native kubemq-go client, and opens the
// native Events Store subscribe stream with the "new only" start policy — BEFORE
// the engine starts producers — so the subscribe-before-publish barrier holds.
func (w *CrossProtocolInteropWorker) Start(ctx context.Context) error {
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

	native, err := transport.DialNative(ctx, w.nativeCfg)
	if err != nil {
		return fmt.Errorf("dial native kubemq: %w", err)
	}
	w.native = native

	// Subscribe FIRST with StartFromNewEvents() ("new only"). SubscribeEventsStoreFromNew
	// returns only after the subscribe stream is established, so a nil error here is
	// the subscribe-before-publish barrier: every message published after this point
	// is in-window for the native consumer (spec S6.3).
	sub, err := w.native.SubscribeEventsStoreFromNew(w.consumerCtx, w.channel,
		w.onNativeEvent,
		func(err error) {
			if w.consumerCtx.Err() == nil {
				w.recordError("interop_failure")
				w.logger.Warn("native subscribe error", "err", errString(err))
			}
		},
	)
	if err != nil {
		return fmt.Errorf("native subscribe %s: %w", w.channel, err)
	}
	w.nativeSub = sub

	// The native subscribe stream is open; signal ready so the engine can start
	// producers (subscribe-before-publish).
	w.signalReady()
	return nil
}

// onNativeEvent reconciles a natively-consumed Events Store event against the
// published set. The event carries the SAME instrumentation tags the GCP publish
// stamped as attributes (the connector maps attributes→tags 1:1, plus the three
// reserved _pubsub_* tags).
func (w *CrossProtocolInteropWorker) onNativeEvent(ev transport.NativeEvent) {
	w.recordReceived(len(ev.Body), "c-native-"+w.channelName)

	producerID, seq, crcHex, sentAt, ok := extractMeta(ev.Tags)
	if !ok {
		return
	}
	if crcHex != "" && !payload.VerifyCRC(ev.Body, crcHex) {
		w.recordCorrupted()
	}
	if !sentAt.IsZero() {
		w.recordLatency(time.Since(sentAt))
	}
	w.recordTracked(producerID, seq)

	id := fmt.Sprintf("%s#%d", producerID, seq)
	w.mu.Lock()
	if _, already := w.consumed[id]; !already {
		w.consumed[id] = struct{}{}
		w.mu.Unlock()
		w.recordInteropConverged()
	} else {
		w.mu.Unlock()
	}
}

// StartProducers launches the GCP Publish loop(s) (measurement window). Every
// publish lands on the topic's Events Store log pubsub.{topicID}, which the
// already-open native subscriber consumes.
func (w *CrossProtocolInteropWorker) StartProducers() {
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

func (w *CrossProtocolInteropWorker) produceLoop(ctx context.Context, producerIdx int) {
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
		if _, err := transport.PublishSync(ctx, w.topic, body, stampAttrs(producerID, seq, crcHex)); err != nil {
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
// BaseWorker counter reset, so the convergence gate measures only the
// post-warmup window: published/consumed entries accumulated during warmup must
// not leak into the divergence accounting (spec warmup isolation).
func (w *CrossProtocolInteropWorker) ResetAfterWarmup() {
	w.BaseWorker.ResetAfterWarmup()
	w.mu.Lock()
	w.published = make(map[string]time.Time)
	w.consumed = make(map[string]struct{})
	w.mu.Unlock()
}

// StopConsumers reconciles interop convergence, cancels the native subscription,
// and closes the clients. A published message not consumed natively within its
// budget is charged as divergence (the convergence gate).
func (w *CrossProtocolInteropWorker) StopConsumers() {
	w.reconcileConvergence()
	if w.nativeSub != nil {
		w.nativeSub.Cancel()
	}
	w.BaseWorker.StopConsumers()
	if w.native != nil {
		_ = w.native.Close()
	}
	if w.topic != nil {
		w.topic.Stop()
	}
	if w.client != nil {
		_ = w.client.Close()
	}
}

func (w *CrossProtocolInteropWorker) reconcileConvergence() {
	now := time.Now()
	var diverged uint64
	w.mu.Lock()
	for id, pubTime := range w.published {
		if _, ok := w.consumed[id]; ok {
			continue
		}
		// Only charge divergence for messages that had enough time to converge; a
		// still-in-flight message at the drain boundary is not divergence.
		if now.Sub(pubTime) > interopConvergenceBudget {
			diverged++
		}
	}
	w.mu.Unlock()
	if diverged > 0 {
		w.recordInteropDiverged(diverged)
		w.logger.Warn("interop divergence", "diverged", diverged, "channel", w.channel)
	}
}
