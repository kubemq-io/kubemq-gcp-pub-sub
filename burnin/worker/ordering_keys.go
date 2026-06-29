package worker

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"cloud.google.com/go/pubsub"

	"github.com/kubemq-io/kubemq-gcp-pub-sub/burnin/config"
	"github.com/kubemq-io/kubemq-gcp-pub-sub/burnin/payload"
	"github.com/kubemq-io/kubemq-gcp-pub-sub/burnin/transport"
)

// orderingKeyCount is the number of distinct ordering keys the worker spreads
// publishes across. Multiple keys exercise the connector's per-key one-in-flight
// cursor and round-robin scheduling under contention (spec S3.5).
const orderingKeyCount = 8

// OrderingKeysWorker (worker 3, spec S6.3) verifies per-ordering-key in-order
// delivery: with EnableMessageOrdering, messages sharing an ordering_key are
// delivered in publish order with AT MOST ONE in flight per key (spec S3.5). The
// worker publishes a per-key monotonic orderingSeq and, on receive, asserts that
// each key's deliveries are non-decreasing in that sequence; any regression is an
// ordering violation. The verdict gate is ZERO ordering violations (spec S6.6).
//
// Publishing must serialize per key (the publisher orders per-key sends), so each
// producer publishes synchronously (Get before the next publish on the same key).
// A single StreamingPull stream (NumGoroutines=1) preserves the per-key ordering
// the connector hands out.
type OrderingKeysWorker struct {
	*BaseWorker
	topicID string
	subID   string

	client *pubsub.Client
	topic  *pubsub.Topic
	sub    *pubsub.Subscription

	seq atomic.Uint64

	// perKey tracks the highest orderingSeq seen per (producer, ordering key) so a
	// lower arrival within a key is flagged as an ordering violation.
	mu     sync.Mutex
	perKey map[string]uint64
}

// NewOrderingKeysWorker creates an ordering_keys worker.
func NewOrderingKeysWorker(cfg *config.Config, idx int, logger *slog.Logger) Worker {
	topicID := transport.TopicName(config.WorkerOrderingKeys, idx)
	subID := transport.SubscriptionName(config.WorkerOrderingKeys, idx)
	return &OrderingKeysWorker{
		BaseWorker: NewBaseWorker(config.WorkerOrderingKeys,
			transport.MappedTopicChannel(topicID), idx, cfg, logger),
		topicID: topicID,
		subID:   subID,
		perKey:  make(map[string]uint64),
	}
}

// Start provisions an ordering-enabled topic + subscription and opens the
// single-stream receiver.
func (w *OrderingKeysWorker) Start(ctx context.Context) error {
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
	// Ordering is a publisher-side flag (it serializes publishes per key); the
	// subscription must also enable it — enabling it on only one side is not enough.
	transport.EnableTopicOrdering(w.topic)

	w.sub, err = ensureSubscription(ctx, client, w.subID,
		transport.OrderingSubscriptionConfig(w.topic, w.cfg.GCP.AckDeadlineSeconds))
	if err != nil {
		return err
	}

	// One stream preserves the per-key delivery order the connector hands out.
	transport.ConfigureReceive(w.sub, 1)

	w.consumerWG.Add(1)
	go func() {
		defer w.consumerWG.Done()
		w.receiveLoop(w.consumerCtx)
	}()
	return nil
}

func (w *OrderingKeysWorker) receiveLoop(ctx context.Context) {
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

func (w *OrderingKeysWorker) handleMessage(m *pubsub.Message, consumerID string) {
	w.recordReceived(len(m.Data), consumerID)

	producerID, seq, crcHex, sentAt, ok := extractMeta(m.Attributes)
	if ok {
		if crcHex != "" && !payload.VerifyCRC(m.Data, crcHex) {
			w.recordCorrupted()
		}
		if !sentAt.IsZero() {
			w.recordLatency(time.Since(sentAt))
		}
		dup := w.recordTracked(producerID, seq)
		// Per-key ordering check: within a key, the per-key orderingSeq must be
		// monotonically non-decreasing. A redelivery (duplicate) of the head is not
		// a violation; only a strictly-lower NEW orderingSeq after a higher one is.
		if !dup {
			w.checkOrdering(producerID, m.OrderingKey, m.Attributes[payload.AttrOrderingSeq])
		}
	}

	transport.Ack(m)
	w.recordAcked()
}

// checkOrdering records an ordering violation if a key's delivery regresses below
// the highest orderingSeq already seen for that key.
func (w *OrderingKeysWorker) checkOrdering(producerID, orderingKey, orderingSeqStr string) {
	if orderingKey == "" || orderingSeqStr == "" {
		return // keyless messages are unordered by design (spec S3.5).
	}
	oseq, err := parseUint(orderingSeqStr)
	if err != nil {
		return
	}
	mapKey := producerID + "|" + orderingKey
	w.mu.Lock()
	last, seen := w.perKey[mapKey]
	if seen && oseq < last {
		w.mu.Unlock()
		w.recordOrderingViolation()
		w.logger.Warn("ordering violation", "key", orderingKey, "got_seq", oseq, "last_seq", last)
		return
	}
	if !seen || oseq > last {
		w.perKey[mapKey] = oseq
	}
	w.mu.Unlock()
}

// ResetAfterWarmup clears the per-key ordering high-water map in addition to the
// base counters. Without this, oseqs observed during warmup are carried into the
// measurement phase, so any warmup-era message still draining into early measure
// is scored against a stale high-water and counted as a spurious ordering
// violation. The ordering assertion must start fresh at the measurement boundary.
func (w *OrderingKeysWorker) ResetAfterWarmup() {
	w.BaseWorker.ResetAfterWarmup()
	w.mu.Lock()
	w.perKey = make(map[string]uint64)
	w.mu.Unlock()
}

// StartProducers launches the per-key serialized Publish loop(s).
func (w *OrderingKeysWorker) StartProducers() {
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

func (w *OrderingKeysWorker) produceLoop(ctx context.Context, producerIdx int) {
	producerID := fmt.Sprintf("p-%s-%d", w.channelName, producerIdx)
	// Per-key monotonic ordering sequence, embedded as an attribute so the receiver
	// can assert per-key order independent of the global tracker sequence.
	keySeq := make([]uint64, orderingKeyCount)
	rr := 0
	for {
		if ctx.Err() != nil {
			return
		}
		if err := w.waitForRate(ctx); err != nil {
			return
		}

		// Round-robin across keys so multiple keys are in contention.
		keyIdx := rr % orderingKeyCount
		rr++
		orderingKey := fmt.Sprintf("%s-k%d", producerID, keyIdx)
		keySeq[keyIdx]++
		oseq := keySeq[keyIdx]

		seq := w.seq.Add(1)
		body, crcHex := payload.Build(w.selectMessageSize())
		attrs := stampAttrs(producerID, seq, crcHex)
		attrs[payload.AttrOrderingSeq] = formatUint(oseq)

		start := time.Now()
		// On the same ordering key the client serializes; PublishOrdered awaits each
		// result so an interleaved publish order across keys is preserved and, on a
		// publish failure, un-pauses ordered delivery on that key (ResumePublish) so
		// the next publish on the key is accepted (mirrors the SDK's contract).
		if _, err := transport.PublishOrdered(ctx, w.topic, orderingKey, body, attrs); err != nil {
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

// StopConsumers stops the receiver and closes the client.
func (w *OrderingKeysWorker) StopConsumers() {
	w.BaseWorker.StopConsumers()
	if w.topic != nil {
		w.topic.Stop()
	}
	if w.client != nil {
		_ = w.client.Close()
	}
}
