package worker

import (
	"context"
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"

	"cloud.google.com/go/pubsub"

	"github.com/kubemq-io/kubemq-gcp-pub-sub/burnin/config"
	"github.com/kubemq-io/kubemq-gcp-pub-sub/burnin/payload"
	"github.com/kubemq-io/kubemq-gcp-pub-sub/burnin/transport"
)

// filterMatchAttr is the attribute the filtering worker stamps on every publish
// to drive the CEL-subset equality filter (spec S3.3). Matching messages carry
// match="yes", non-matching match="no". The filter expression below selects
// only the "yes" messages at fan-out.
const filterMatchAttr = "match"

// filterMatchYes / filterMatchNo are the deterministic attribute values that a
// match / non-match message carries.
const (
	filterMatchYes = "yes"
	filterMatchNo  = "no"
)

// filterExpr is the CEL-subset attribute filter the subscription is created
// with (spec S5.3). It is deterministic and well under the 256-char limit
// (S3.3); the grammar attributes.KEY = "value" is the connector's equality node
// (filter/filter.go:11). Only messages whose match attribute equals "yes" are
// fanned out to the subscription queue; "no" messages are auto-acked / never
// enqueued (S3.3).
const filterExpr = `attributes.` + filterMatchAttr + ` = "` + filterMatchYes + `"`

// FilteringWorker (NEW, spec REQ-003/S5.3) provisions a subscription with a
// CEL-subset attribute Filter (transport.FilteredSubscriptionConfig, S3.3),
// publishes a deterministic mix of matching ("yes") and non-matching ("no")
// messages, and asserts only matching messages are delivered. A delivered
// non-match hard-fails the filter_mismatch gate (verdict.go:146). CRITICAL
// (REQ-019/S5.3): the worker registers tracker sends ONLY for matching messages
// under a dedicated matching producerID — registering a non-match as a send
// would produce guaranteed false loss because the connector auto-acks it at
// fan-out and never delivers it. Non-matches use a separate, untracked
// producerID namespace so the matching producer's sequence stays monotonic and
// contiguous and pendingLost reconciles correctly.
type FilteringWorker struct {
	*BaseWorker
	topicID string
	subID   string

	client *pubsub.Client
	topic  *pubsub.Topic
	sub    *pubsub.Subscription

	// nonMatchSeq is a single worker-wide counter for non-matching publishes. It
	// is never registered with the tracker (those messages are auto-acked at
	// fan-out and never delivered, S3.3), so it only feeds the non-match attribute
	// stamp for debuggability — its contiguity is irrelevant.
	nonMatchSeq atomic.Uint64
}

// NewFilteringWorker creates a filtering worker.
func NewFilteringWorker(cfg *config.Config, idx int, logger *slog.Logger) Worker {
	topicID := transport.TopicName(config.WorkerFiltering, idx)
	subID := transport.SubscriptionName(config.WorkerFiltering, idx)
	return &FilteringWorker{
		BaseWorker: NewBaseWorker(config.WorkerFiltering,
			transport.MappedTopicChannel(topicID), idx, cfg, logger),
		topicID: topicID,
		subID:   subID,
	}
}

// Start builds the Pub/Sub client, provisions the topic and the FILTERED
// subscription (CEL-subset attribute filter compiled once at CreateSubscription,
// immutable — S3.3), and brings up the receiver. The receiver asserts that every
// delivered message satisfies the filter; a delivered non-match is recorded as a
// filter_mismatch (HARD-FAIL).
func (w *FilteringWorker) Start(ctx context.Context) error {
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

	// FilteredSubscriptionConfig sets SubscriptionConfig.Filter = filterExpr. The
	// connector compiles the filter at create-time and applies it per-subscription
	// at publish fan-out (S3.3); a malformed filter would be rejected with
	// codes.InvalidArgument here. filterExpr is a fixed, valid expression.
	w.sub, err = ensureSubscription(ctx, client, w.subID,
		transport.FilteredSubscriptionConfig(w.topic, w.cfg.GCP.AckDeadlineSeconds, filterExpr))
	if err != nil {
		return err
	}

	transport.ConfigureReceive(w.sub, w.workerCfg.ConsumersPerChannel)

	w.consumerWG.Add(1)
	go func() {
		defer w.consumerWG.Done()
		w.receiveLoop(w.consumerCtx)
	}()
	return nil
}

// receiveLoop runs the StreamingPull Receive loop, re-opening it after a forced
// disconnect or a transient error.
func (w *FilteringWorker) receiveLoop(ctx context.Context) {
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

// handleMessage asserts the filter contract on every delivered message:
//   - [HARD-FAIL] the match attribute MUST equal "yes"; a delivered "no" (or a
//     missing match attribute) means the connector fanned out a filtered-out
//     message → recordFilterMismatch (verdict gate FilterMismatch>0 → fail).
//   - a matching delivery is reconciled against the matching producer via
//     recordTracked so the loss gate covers matches (a matching message that
//     never delivers is real loss).
//
// Every message is acked regardless, so a stray non-match does not pile up on
// the subscription lease and skew the run.
func (w *FilteringWorker) handleMessage(m *pubsub.Message, consumerID string) {
	w.recordReceived(len(m.Data), consumerID)

	matchVal := m.Attributes[filterMatchAttr]
	if matchVal != filterMatchYes {
		// The connector delivered a message that should have been filtered out at
		// fan-out (S3.3). This is a deterministic filter-fidelity violation.
		w.recordFilterMismatch()
		w.logger.Error("filter mismatch: filtered-out message delivered",
			"match_attr", matchVal, "message_id", m.ID)
		transport.Ack(m)
		w.recordAcked()
		return
	}

	// Matching delivery: count it and reconcile the matching producer sequence so
	// the loss gate (boundaryLossPct, verdict.go) applies to matches.
	w.recordFilterMatchedDelivered()

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

// StartProducers launches the publish loop(s) that emit a deterministic mix of
// matching and non-matching messages (measurement window).
func (w *FilteringWorker) StartProducers() {
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

// produceLoop publishes an alternating mix of matching / non-matching messages
// at Rate. Matching messages are stamped with match="yes" PLUS the standard
// instrumentation under the MATCHING producerID, and ONLY they are registered as
// tracker sends (recordSent). Non-matching messages carry match="no" under a
// SEPARATE producerID namespace and are NEVER recorded as sends — they are
// auto-acked by the connector at fan-out (S3.3), so registering them would be
// guaranteed false loss (REQ-019/S5.3).
func (w *FilteringWorker) produceLoop(ctx context.Context, producerIdx int) {
	matchProducerID := fmt.Sprintf("p-%s-%d", w.channelName, producerIdx)
	// matchSeq is PER-PRODUCER (goroutine-local): the tracker keys its reorder
	// window by producerID and expects each producer's matching-send stream to be
	// monotonic and CONTIGUOUS (highContiguous = seq-1 on first record). A single
	// worker-wide counter shared across >1 producer goroutines would interleave
	// (1,3,5 / 2,4,6) and false-count the gaps as loss — so each producer owns its
	// own contiguous seq (S5.3/REQ-019).
	var matchSeq uint64
	// tick drives a deterministic alternating ratio per producer: even → matching,
	// odd → non-matching. Half the traffic exercises the auto-ack (filtered-out)
	// path, half exercises delivery + loss reconciliation.
	var tick uint64
	for {
		if ctx.Err() != nil {
			return
		}
		if err := w.waitForRate(ctx); err != nil {
			return
		}

		body, crcHex := payload.Build(w.selectMessageSize())
		matching := tick%2 == 0
		tick++

		if !matching {
			w.publishNonMatch(ctx, body, crcHex)
			continue
		}

		// Reserve the next contiguous matching sequence. Only advance it on a
		// successful publish so the matching send stream stays gap-free.
		seq := matchSeq + 1
		attrs := stampAttrs(matchProducerID, seq, crcHex)
		attrs[filterMatchAttr] = filterMatchYes

		start := time.Now()
		if _, err := transport.PublishSync(ctx, w.topic, body, attrs); err != nil {
			if ctx.Err() != nil {
				return
			}
			w.recordError("publish_failure")
			continue // seq not advanced → no gap in the matching stream
		}
		matchSeq = seq
		metricObserveSend(w.name, time.Since(start))
		// Register the send ONLY for the matching message, under the matching
		// producerID, exactly once.
		w.recordSent(len(body), matchProducerID)
	}
}

// publishNonMatch publishes a non-matching message (match="no") under a separate,
// UNTRACKED producerID namespace. It is intentionally NOT recordSent/recordTracked:
// the connector auto-acks it at fan-out and never delivers it, so it contributes
// zero to the tracker (no false loss, REQ-019). A publish failure is a transport
// error only.
func (w *FilteringWorker) publishNonMatch(ctx context.Context, body []byte, crcHex string) {
	seq := w.nonMatchSeq.Add(1)
	// Distinct producerID namespace ("n-...") so a non-match can never collide with
	// or be mistaken for a tracked matching send.
	attrs := stampAttrs(fmt.Sprintf("n-%s", w.channelName), seq, crcHex)
	attrs[filterMatchAttr] = filterMatchNo

	if _, err := transport.PublishSync(ctx, w.topic, body, attrs); err != nil {
		if ctx.Err() != nil {
			return
		}
		w.recordError("publish_failure")
	}
}

// StopConsumers stops the receiver and closes the Pub/Sub client.
func (w *FilteringWorker) StopConsumers() {
	w.BaseWorker.StopConsumers()
	if w.topic != nil {
		w.topic.Stop()
	}
	if w.client != nil {
		_ = w.client.Close()
	}
}
