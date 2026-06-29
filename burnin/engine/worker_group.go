// Package engine drives the burn-in run lifecycle (warmup → measure → drain →
// verdict), grouping the per-channel workers of each of the seven Pub/Sub worker
// types. Mirrors the kubemq-aws burnin engine, recast for the Pub/Sub workers.
package engine

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/kubemq-io/kubemq-gcp-pub-sub/burnin/config"
	"github.com/kubemq-io/kubemq-gcp-pub-sub/burnin/worker"
)

// WorkerGroup holds all channel instances of one worker type.
type WorkerGroup struct {
	name    string
	workers []worker.Worker
}

// NewWorkerGroup builds the channel instances for a worker type. The concrete
// constructors are owned by the worker package; each returns a worker.Worker.
func NewWorkerGroup(name string, cfg *config.Config, logger *slog.Logger) *WorkerGroup {
	numChannels := cfg.GetWorkerChannels(name)
	workers := make([]worker.Worker, 0, numChannels)
	for i := 1; i <= numChannels; i++ {
		switch name {
		case config.WorkerPublishPullRoundTrip:
			workers = append(workers, worker.NewPublishPullRoundTripWorker(cfg, i, logger))
		case config.WorkerStreamingPull:
			workers = append(workers, worker.NewStreamingPullWorker(cfg, i, logger))
		case config.WorkerOrderingKeys:
			workers = append(workers, worker.NewOrderingKeysWorker(cfg, i, logger))
		case config.WorkerExactlyOnce:
			workers = append(workers, worker.NewExactlyOnceWorker(cfg, i, logger))
		case config.WorkerDeadLetter:
			workers = append(workers, worker.NewDeadLetterWorker(cfg, i, logger))
		case config.WorkerFanOut:
			workers = append(workers, worker.NewFanOutWorker(cfg, i, logger))
		case config.WorkerCrossProtocol:
			workers = append(workers, worker.NewCrossProtocolInteropWorker(cfg, i, logger))
		case config.WorkerNackRedelivery:
			workers = append(workers, worker.NewNackRedeliveryWorker(cfg, i, logger))
		case config.WorkerFiltering:
			workers = append(workers, worker.NewFilteringWorker(cfg, i, logger))
		case config.WorkerSchemaEnforce:
			workers = append(workers, worker.NewSchemaEnforceWorker(cfg, i, logger))
		case config.WorkerSeekSnapshot:
			workers = append(workers, worker.NewSeekSnapshotWorker(cfg, i, logger))
		case config.WorkerInflightCap:
			workers = append(workers, worker.NewInflightCapWorker(cfg, i, logger))
		}
	}
	return &WorkerGroup{name: name, workers: workers}
}

// StartConsumers provisions resources and starts the consumer/receiver side of
// every worker.
func (g *WorkerGroup) StartConsumers(ctx context.Context) error {
	for _, w := range g.workers {
		if err := w.Start(ctx); err != nil {
			return fmt.Errorf("start consumer for %s/%s: %w", g.name, w.ChannelName(), err)
		}
	}
	return nil
}

// WaitForConsumerReady blocks until every worker signals ready or times out.
func (g *WorkerGroup) WaitForConsumerReady(timeout time.Duration) error {
	for _, w := range g.workers {
		select {
		case <-w.ConsumerReady():
		case <-time.After(timeout):
			return fmt.Errorf("consumer ready timeout for %s/%s", g.name, w.ChannelName())
		}
	}
	return nil
}

// StartProducers starts the producer side of every worker.
func (g *WorkerGroup) StartProducers() {
	for _, w := range g.workers {
		w.StartProducers()
	}
}

// StopProducers stops the producer side of every worker.
func (g *WorkerGroup) StopProducers() {
	for _, w := range g.workers {
		w.StopProducers()
	}
}

// StopConsumers stops the consumer side of every worker.
func (g *WorkerGroup) StopConsumers() {
	for _, w := range g.workers {
		w.StopConsumers()
	}
}

// DisconnectConsumers force-recreates consumer clients for every worker.
func (g *WorkerGroup) DisconnectConsumers() {
	for _, w := range g.workers {
		w.DisconnectConsumers()
	}
}

// Workers returns the worker slice.
func (g *WorkerGroup) Workers() []worker.Worker { return g.workers }

// Name returns the worker type name.
func (g *WorkerGroup) Name() string { return g.name }
