// Package disconnect injects forced connection churn: on a fixed interval it
// recreates every target's consumer/receiver clients, waits, then lets them
// re-establish. For Pub/Sub this exercises StreamingPull reconnect, lease
// re-acquisition, and at-least-once redelivery on ack-deadline expiry (spec S6
// forced_disconnect / recovery).
package disconnect

import (
	"context"
	"log/slog"
	"time"

	"github.com/kubemq-io/kubemq-gcp-pub-sub/burnin/metrics"
)

// Target is anything whose consumer/receiver clients can be force-closed (every
// worker satisfies this).
type Target interface {
	DisconnectConsumers()
}

// Manager drives the forced-disconnect cycle.
type Manager struct {
	interval time.Duration
	duration time.Duration
	targets  []Target
	logger   *slog.Logger
}

// New creates a forced-disconnect manager.
func New(interval, duration time.Duration, targets []Target, logger *slog.Logger) *Manager {
	return &Manager{interval: interval, duration: duration, targets: targets, logger: logger}
}

// Run blocks until ctx is cancelled, injecting disconnects on each tick.
func (m *Manager) Run(ctx context.Context) {
	if m.interval == 0 {
		return
	}
	ticker := time.NewTicker(m.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			metrics.IncForcedDisconnect()
			m.logger.Info("forced disconnect: recreating consumer clients")
			for _, t := range m.targets {
				t.DisconnectConsumers()
			}
			select {
			case <-ctx.Done():
				return
			case <-time.After(m.duration):
			}
			m.logger.Info("forced disconnect: consumers will re-establish")
		}
	}
}
