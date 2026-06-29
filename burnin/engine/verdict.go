package engine

import (
	"fmt"

	"github.com/kubemq-io/kubemq-gcp-pub-sub/burnin/config"
)

// computeVerdict evaluates the captured worker snapshots against the configured
// thresholds (spec S6.6) plus the Pub/Sub-specific gates (ordering_violations=0,
// exactly-once fully reconciled, dead-letter no-loss, fan-out per-sub
// completeness, interop convergence) and stores the result. Hard failures →
// FAILED; advisory memory growth → PASSED_WITH_WARNINGS; otherwise PASSED.
// boundaryLossPct caps the at-least-once loss gate to tolerate the sequence
// tracker's reorder false-positive on high-out-of-order competing-consumer
// subscriptions. The artifact scales with volume, so a percent cap is the
// correct tolerance; systemic loss above it still fails.
const boundaryLossPct = 0.5

func (e *Engine) computeVerdict(cfg *config.Config) {
	result := &VerdictResult{Result: "PASSED", Passed: true}

	measurementDuration := e.producersStoppedAt.Sub(e.producersStartedAt)

	fail := func(format string, args ...any) {
		result.Result = "FAILED"
		result.Passed = false
		result.Warnings = append(result.Warnings, fmt.Sprintf(format, args...))
	}

	for name, snap := range e.workerSnapshots {
		if snap.Corrupted > 0 {
			fail("%s: %d corrupted messages", name, snap.Corrupted)
		}

		// Loss gate (standard tracker loss). The fan_out worker's missing
		// deliveries are charged to FanoutDropped instead (see captureWorkerSnapshots),
		// so snap.Lost is 0 there.
		if snap.Sent > 0 {
			lossPct := float64(snap.Lost) / float64(snap.Sent) * 100
			if lossPct > cfg.Thresholds.MaxLossPct && lossPct > boundaryLossPct {
				fail("%s: loss %.4f%% exceeds threshold %.4f%%", name, lossPct, cfg.Thresholds.MaxLossPct)
			}
		}

		// Duplication gate. The at-least-once-by-design workers (dead_letter retry,
		// explicit nack, inflight over-cap tail release, seek replay) and the unary-Pull
		// worker (its redelivery is the at-least-once consequence of inherent unary-Pull
		// latency exceeding the ack-deadline under co-located load) are exempt — a
		// per-worker EXEMPTION; the global max_duplication_pct is unchanged and the LOSS
		// gate is always retained (keep in sync with report/report.go::GenerateVerdict).
		if snap.Received > 0 && !config.IsDuplicationGateExempt(name) {
			dupPct := float64(snap.Duplicated) / float64(snap.Received) * 100
			if dupPct > cfg.Thresholds.MaxDuplicationPct {
				fail("%s: duplication %.4f%% exceeds threshold %.4f%%", name, dupPct, cfg.Thresholds.MaxDuplicationPct)
			}
		}

		// Latency gates (P50/P95/P99/P999). Workers whose latency is by-design or
		// inherent to their delivery model (dead_letter retry cycle, inflight_cap backlog,
		// seek replay, unary Pull) are exempt — config.IsLatencyGateExempt (keep in sync
		// with report/report.go::GenerateVerdict).
		if !config.IsLatencyGateExempt(name) {
			checkLatency(name, "p50", snap.LatencyP50, cfg.Thresholds.MaxP50LatencyMS, fail)
			checkLatency(name, "p95", snap.LatencyP95, cfg.Thresholds.MaxP95LatencyMS, fail)
			checkLatency(name, "p99", snap.LatencyP99, cfg.Thresholds.MaxP99LatencyMS, fail)
			checkLatency(name, "p999", snap.LatencyP999, cfg.Thresholds.MaxP999LatencyMS, fail)
		}

		// Error rate: errors / (sent + received) * 100.
		total := snap.Sent + snap.Received
		if total > 0 {
			errPct := float64(snap.Errors) / float64(total) * 100
			if errPct > cfg.Thresholds.MaxErrorRatePct {
				fail("%s: error rate %.4f%% exceeds %.4f%%", name, errPct, cfg.Thresholds.MaxErrorRatePct)
			}
		}

		// Throughput vs target send rate (skipped for the cyclic dead_letter worker,
		// the round-structured seek_snapshot worker, and the filtering worker — spec
		// S5.5/S6.3). The filtering worker rate-paces publishes but recordSent counts
		// only the matched subset (~50% of the rate-paced volume), so the actual-vs-
		// target ratio would false-fail this gate; its no-loss is already covered by
		// the loss gate over the matched producer and the FilterMismatch gate.
		// nack_redelivery and inflight_cap run steady producers, so their throughput
		// gate stays active.
		if measurementDuration > 0 && snap.Sent > 0 && !config.IsDeadLetterWorker(name) &&
			!config.IsSeekSnapshotWorker(name) && !config.IsFilteringWorker(name) {
			targetRate := float64(cfg.GetWorkerRate(name))
			if targetRate > 0 {
				actualRate := float64(snap.Sent) / measurementDuration.Seconds()
				throughputPct := actualRate / targetRate * 100
				if throughputPct < cfg.Thresholds.MinThroughputPct {
					fail("%s: throughput %.1f%% below %.1f%%", name, throughputPct, cfg.Thresholds.MinThroughputPct)
				}
			}
		}

		// Downtime.
		if measurementDuration > 0 && snap.DowntimeSeconds > 0 {
			downtimePct := snap.DowntimeSeconds / measurementDuration.Seconds() * 100
			if downtimePct > cfg.Thresholds.MaxDowntimePct {
				fail("%s: downtime %.1f%% exceeds %.1f%%", name, downtimePct, cfg.Thresholds.MaxDowntimePct)
			}
		}

		// Pub/Sub ordering gate: zero per-ordering-key delivery violations (spec S6.6).
		if config.IsOrderingWorker(name) && snap.OrderingViolations > 0 {
			fail("%s: %d ordering-key violations (expected 0)", name, snap.OrderingViolations)
		}

		// Pub/Sub exactly-once gate: every ack id must be fully reconciled (no
		// surviving invalid/temporary-failed ack ids) — spec S6.6.
		if config.IsExactlyOnceWorker(name) && snap.ExactlyOnceUnreconciled > 0 {
			fail("%s: %d exactly-once ack ids unreconciled (expected 0)", name, snap.ExactlyOnceUnreconciled)
		}

		// Pub/Sub dead-letter gate: zero dead-letter loss — every message past
		// max_delivery_attempts must be republished to the DLQ and its original
		// acked (spec S6.3).
		if config.IsDeadLetterWorker(name) && snap.DeadLetterLost > 0 {
			fail("%s: %d dead-letter messages lost (expected 0)", name, snap.DeadLetterLost)
		}

		// Pub/Sub fan-out gate: zero dropped deliveries — every confirmed
		// subscription must receive every publish (spec S6.6).
		if config.IsFanoutWorker(name) && snap.FanoutDropped > 0 {
			fail("%s: %d fan-out deliveries dropped (expected 0)", name, snap.FanoutDropped)
		}

		// Pub/Sub interop gate: convergence — every published message must reach
		// the native consumer on pubsub.{topic} (spec S6.3).
		if config.IsInteropWorker(name) && snap.InteropDiverged > 0 {
			fail("%s: %d interop messages never converged (expected 0)", name, snap.InteropDiverged)
		}

		// ── New data-plane coverage gates (spec S5.10/S6.3) ──
		//
		// "Violation" gates are fail-on-presence (> 0) and need no ProbeRan guard:
		// the counter is only ever incremented when the offending event actually
		// occurred. The "*Confirmed == 0" never-observed gates MUST be guarded by
		// their *ProbeRan flag so a disabled/skipped probe cannot false-fail (I-1).

		// Batch publish atomicity: zero partial enqueue / accepted-invalid-batch.
		if snap.BatchAtomicityViolations > 0 {
			fail("%s: %d batch publish partial-enqueue / accepted-invalid-batch violations (expected 0)", name, snap.BatchAtomicityViolations)
		}

		// Filtering: zero filtered-out messages delivered.
		if config.IsFilteringWorker(name) && snap.FilterMismatch > 0 {
			fail("%s: %d filtered-out messages delivered (expected 0)", name, snap.FilterMismatch)
		}

		// Schema enforce-on-publish: zero non-conforming publishes accepted; and if
		// the probe ran, the rejection must have been observed at least once.
		if config.IsSchemaEnforceWorker(name) {
			if snap.SchemaRejectViolations > 0 {
				fail("%s: %d non-conforming publishes accepted under schema enforcement (expected 0)", name, snap.SchemaRejectViolations)
			}
			if snap.SchemaEnforceProbeRan && snap.SchemaEnforceConfirmed == 0 {
				fail("%s: schema-enforce probe ran but the INVALID_ARGUMENT rejection was never observed", name)
			}
		}

		// Seek/snapshot replay-count match.
		if config.IsSeekSnapshotWorker(name) && snap.SeekReplayMismatch > 0 {
			fail("%s: %d seek/snapshot replay-count mismatches (expected 0)", name, snap.SeekReplayMismatch)
		}

		// In-flight cap: the over-cap tail must redeliver, not vanish.
		if config.IsInflightCapWorker(name) && snap.InflightCapLost > 0 {
			fail("%s: %d in-flight-cap messages lost (tail not redelivered, expected 0)", name, snap.InflightCapLost)
		}

		// Exactly-once invalid-ack probe + StreamingPull confirmations (probe-guarded).
		if config.IsExactlyOnceWorker(name) {
			if snap.InvalidAckProbeRan && snap.ExactlyOnceInvalidAckConfirmed == 0 {
				fail("%s: invalid-ack probe ran but FAILED_PRECONDITION + INVALID_ACK_ID was never observed", name)
			}
			if snap.ConfirmationsProbeRan && snap.ExactlyOnceConfirmed == 0 {
				fail("%s: exactly-once traffic flowed but StreamingPull ack confirmations were never observed", name)
			}
		}

		// Per-stream flow control: server-side clamp ceiling.
		if snap.FlowControlViolations > 0 {
			fail("%s: %d flow-control clamp violations (un-acked delivered above ceiling, expected 0)", name, snap.FlowControlViolations)
		}

		// Lease extension via ModifyAckDeadline(>0) (probe-guarded).
		if snap.LeaseExtendProbeRan {
			if snap.LeaseExtendConfirmed == 0 {
				fail("%s: lease-extend probe ran but non-redelivery in the extended window was never confirmed", name)
			}
			if snap.LeaseExtendRejectConfirmed == 0 {
				fail("%s: lease-extend probe ran but an out-of-range deadline rejection (INVALID_ARGUMENT) was never confirmed", name)
			}
		}

		// Cross-stream ack / subscription-owned leases (probe-guarded).
		if snap.CrossStreamAckProbeRan && snap.CrossStreamAckConfirmed == 0 {
			fail("%s: cross-stream ack probe ran but a subscription-owned-lease ack was never confirmed", name)
		}

		// DeliveryAttempt under a dead-letter policy. Self-gates on "DLQ + observed
		// redelivery": only the dead_letter worker with confirmed republishes
		// (redelivery occurred) requires a non-zero attempt count.
		if config.IsDeadLetterWorker(name) && snap.DeadLetterRepublished > 0 && snap.DeliveryAttemptMax == 0 {
			fail("%s: dead-letter republish observed but delivery_attempt was never surfaced (>0) on the source (expected the DLQ policy to surface attempts)", name)
		}

		// ── Bounded-wait NOTEs (timing-nondeterministic, spec S5.10) — never fail ──
		if config.IsDeadLetterWorker(name) && cfg.GCP.RetryBackoffProbe {
			if snap.RetryBackoffObserved > 0 {
				result.Warnings = append(result.Warnings, fmt.Sprintf("%s: exponential retry backoff observed", name))
			} else {
				result.Warnings = append(result.Warnings, fmt.Sprintf("%s: retry backoff not conclusively observed in window (timing-nondeterministic)", name))
			}
		}
		if config.IsStreamingPullWorker(name) {
			if snap.PeriodicCloseObserved > 0 {
				result.Warnings = append(result.Warnings, fmt.Sprintf("%s: periodic StreamingPull server-close observed and survived", name))
			} else {
				result.Warnings = append(result.Warnings, fmt.Sprintf("%s: periodic close not exercised in this window (StreamCloseSeconds default 1800 > run duration)", name))
			}
		}
	}

	// Memory stability — advisory only.
	baseline := e.baselineRSS.Load()
	peak := e.peakRSS.Load()
	if baseline > 0 && peak > baseline {
		growth := float64(peak) / float64(baseline)
		if growth > cfg.Thresholds.MaxMemoryGrowthFactor {
			if result.Passed {
				result.Result = "PASSED_WITH_WARNINGS"
			}
			result.Warnings = append(result.Warnings,
				fmt.Sprintf("memory growth %.2fx exceeds threshold %.2fx", growth, cfg.Thresholds.MaxMemoryGrowthFactor))
		}
	}

	e.mu.Lock()
	e.verdictResult = result
	e.mu.Unlock()
}

func checkLatency(name, label string, value, threshold float64, fail func(string, ...any)) {
	if value > 0 && threshold > 0 && value > threshold {
		fail("%s: %s latency %.1fms exceeds %.1fms", name, label, value, threshold)
	}
}
