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

// FanOutWorker (worker 6, spec S6.3) verifies one-topic→N-subscription fan-out
// completeness: a single Publish writes once to the topic log pubsub.{t} and the
// connector fans out one queue copy per subscription pubsub.sub.{s} (spec S3.3),
// so EVERY confirmed subscription must receive EVERY publish. Each subscriber
// tracks its OWN copy of the producer sequence (tracker keyed by
// (producer, subscription)), so a missing fan-out delivery surfaces as loss on
// that subscriber; the engine folds that into FanoutDropped. The verdict gate is
// ZERO dropped deliveries (spec S6.6).
type FanOutWorker struct {
	*BaseWorker
	topicID string
	subIDs  []string

	client *pubsub.Client
	topic  *pubsub.Topic
	subs   []*pubsub.Subscription

	seq atomic.Uint64
}

// NewFanOutWorker creates a fan_out worker with the configured number of target
// subscriptions.
func NewFanOutWorker(cfg *config.Config, idx int, logger *slog.Logger) Worker {
	topicID := transport.TopicName(config.WorkerFanOut, idx)
	n := cfg.FanoutSubscriptionCount()
	subIDs := make([]string, n)
	for i := 0; i < n; i++ {
		subIDs[i] = transport.FanoutSubscriptionName(config.WorkerFanOut, idx, i)
	}
	return &FanOutWorker{
		BaseWorker: NewBaseWorker(config.WorkerFanOut,
			transport.MappedTopicChannel(topicID), idx, cfg, logger),
		topicID: topicID,
		subIDs:  subIDs,
	}
}

// Start provisions the topic + N subscriptions and brings up a per-subscription
// receiver. Each receiver tracks the producer sequence under its own subscription
// id so a missing fan-out delivery is detected as a gap on that subscriber.
func (w *FanOutWorker) Start(ctx context.Context) error {
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

	for _, subID := range w.subIDs {
		sub, err := ensureSubscription(ctx, client, subID, pubsub.SubscriptionConfig{
			Topic:       w.topic,
			AckDeadline: transport.AckDeadline(w.cfg.GCP.AckDeadlineSeconds),
		})
		if err != nil {
			return err
		}
		transport.ConfigureReceive(sub, 1)
		w.subs = append(w.subs, sub)
	}

	for i := range w.subs {
		sub := w.subs[i]
		subID := w.subIDs[i]
		w.consumerWG.Add(1)
		go func(s *pubsub.Subscription, sid string) {
			defer w.consumerWG.Done()
			w.receiveLoop(w.consumerCtx, s, sid)
		}(sub, subID)
	}
	return nil
}

func (w *FanOutWorker) receiveLoop(ctx context.Context, sub *pubsub.Subscription, subID string) {
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

		err := transport.Receive(recvCtx, sub, func(_ context.Context, m *pubsub.Message) {
			w.handleMessage(m, subID)
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

func (w *FanOutWorker) handleMessage(m *pubsub.Message, subID string) {
	w.recordReceived(len(m.Data), "c-"+subID)
	w.recordFanoutDelivery()

	producerID, seq, crcHex, sentAt, ok := extractMeta(m.Attributes)
	if ok {
		if crcHex != "" && !payload.VerifyCRC(m.Data, crcHex) {
			w.recordCorrupted()
		}
		if !sentAt.IsZero() {
			w.recordLatency(time.Since(sentAt))
		}
		// Key the tracker by (producer, subscription): each confirmed subscriber is
		// expected to receive EVERY publish, so a gap on a subscriber is fan-out
		// loss (the engine folds tracker loss into FanoutDropped for this worker).
		w.recordTracked(producerID+"@"+subID, seq)
	}

	transport.Ack(m)
	w.recordAcked()
}

// StartProducers launches the single-topic Publish loop(s) that fan out to every
// subscription.
func (w *FanOutWorker) StartProducers() {
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

func (w *FanOutWorker) produceLoop(ctx context.Context, producerIdx int) {
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

// StopConsumers stops the per-subscription receivers and closes the client.
func (w *FanOutWorker) StopConsumers() {
	w.BaseWorker.StopConsumers()
	if w.topic != nil {
		w.topic.Stop()
	}
	if w.client != nil {
		_ = w.client.Close()
	}
}
