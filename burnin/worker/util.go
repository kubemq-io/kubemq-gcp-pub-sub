package worker

import (
	"context"
	"strconv"
	"time"

	"cloud.google.com/go/pubsub"

	"github.com/kubemq-io/kubemq-gcp-pub-sub/burnin/config"
	"github.com/kubemq-io/kubemq-gcp-pub-sub/burnin/metrics"
)

// ackStatusString renders an AcknowledgeStatus for logging.
func ackStatusString(s pubsub.AcknowledgeStatus) string {
	switch s {
	case pubsub.AcknowledgeStatusSuccess:
		return "success"
	case pubsub.AcknowledgeStatusPermissionDenied:
		return "permission_denied"
	case pubsub.AcknowledgeStatusFailedPrecondition:
		return "failed_precondition"
	case pubsub.AcknowledgeStatusInvalidAckID:
		return "invalid_ack_id"
	case pubsub.AcknowledgeStatusOther:
		return "other"
	default:
		return "unknown"
	}
}

// errString renders an error for structured logging (empty string for nil).
func errString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

// parseUint parses a base-10 unsigned integer attribute value.
func parseUint(s string) (uint64, error) {
	return strconv.ParseUint(s, 10, 64)
}

// formatUint formats a uint64 as a base-10 attribute value.
func formatUint(v uint64) string {
	return strconv.FormatUint(v, 10)
}

// sleepCtx sleeps for d unless ctx is cancelled first. Returns false if the
// context was cancelled (caller should stop).
func sleepCtx(ctx context.Context, d time.Duration) bool {
	select {
	case <-ctx.Done():
		return false
	case <-time.After(d):
		return true
	}
}

// metricObserveSend records Publish / native-send round-trip duration.
func metricObserveSend(workerName string, d time.Duration) {
	metrics.ObserveSendDuration(workerName, d)
}

// reconnectBackoff returns the configured reconnect interval (fallback 1s) used
// between StreamingPull re-establish attempts.
func reconnectBackoff(cfg *config.Config) time.Duration {
	if cfg.ReconnectInterval > 0 {
		return cfg.ReconnectInterval
	}
	return time.Second
}

// watchDisconnect cancels an active Receive when the forced-disconnect
// generation advances (so the receive loop rebuilds its StreamingPull stream).
// gen is the generation captured when the current Receive started; genFn reads
// the live generation.
func watchDisconnect(ctx context.Context, cancel context.CancelFunc, gen uint64, genFn func() uint64) {
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if genFn() != gen {
				cancel()
				return
			}
		}
	}
}
