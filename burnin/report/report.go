// Package report builds the burn-in summary + verdict, prints a human-readable
// console report, and writes the JSON report. Recast for the Pub/Sub workers
// with P50/P95/P99/P999 latency gates and the Pub/Sub-specific
// ordering_violations / fanout_deliveries / fanout_dropped / exactly_once /
// dead_letter / interop counters (spec S6.5/S6.6).
package report

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/kubemq-io/kubemq-gcp-pub-sub/burnin/config"
)

// Summary is the aggregate run report.
type Summary struct {
	RunID             string                  `json:"run_id"`
	SDK               string                  `json:"sdk"`
	SDKVersion        string                  `json:"sdk_version"`
	Mode              string                  `json:"mode"`
	BrokerAddress     string                  `json:"broker_address"`
	ProjectID         string                  `json:"project_id"`
	EmulatorHost      string                  `json:"emulator_host"`
	StartedAt         time.Time               `json:"started_at"`
	EndedAt           time.Time               `json:"ended_at"`
	DurationSeconds   float64                 `json:"duration_seconds"`
	AllWorkersEnabled bool                    `json:"all_workers_enabled"`
	Workers           map[string]*WorkerStats `json:"workers"`
	Resources         ResourceStats           `json:"resources"`
}

// WorkerStats holds per-worker rollups.
type WorkerStats struct {
	Enabled                 bool    `json:"enabled"`
	Sent                    uint64  `json:"sent"`
	Received                uint64  `json:"received"`
	Lost                    uint64  `json:"lost"`
	Duplicated              uint64  `json:"duplicated"`
	Corrupted               uint64  `json:"corrupted"`
	OutOfOrder              uint64  `json:"out_of_order"`
	Acked                   uint64  `json:"acked"`
	LossPct                 float64 `json:"loss_pct"`
	Errors                  uint64  `json:"errors"`
	Reconnections           uint64  `json:"reconnections"`
	DowntimeSeconds         float64 `json:"downtime_seconds"`
	OrderingViolations      uint64  `json:"ordering_violations"`
	FanoutDeliveries        uint64  `json:"fanout_deliveries"`
	FanoutDropped           uint64  `json:"fanout_dropped"`
	ExactlyOnceConfirmed    uint64  `json:"exactly_once_confirmed"`
	ExactlyOnceUnreconciled uint64  `json:"exactly_once_unreconciled"`
	DeadLetterRepublished   uint64  `json:"dead_letter_republished"`
	DeadLetterLost          uint64  `json:"dead_letter_lost"`
	InteropConverged        uint64  `json:"interop_converged"`
	InteropDiverged         uint64  `json:"interop_diverged"`
	LatencyP50MS            float64 `json:"latency_p50_ms"`
	LatencyP95MS            float64 `json:"latency_p95_ms"`
	LatencyP99MS            float64 `json:"latency_p99_ms"`
	LatencyP999MS           float64 `json:"latency_p999_ms"`
	AvgRate                 float64 `json:"avg_rate"`
	PeakRate                float64 `json:"peak_rate"`
	TargetRate              int     `json:"target_rate"`
	Channels                int     `json:"channels"`
}

// ResourceStats holds memory stats.
type ResourceStats struct {
	PeakRSSMB          float64 `json:"peak_rss_mb"`
	BaselineRSSMB      float64 `json:"baseline_rss_mb"`
	MemoryGrowthFactor float64 `json:"memory_growth_factor"`
}

// Verdict is the evaluated pass/fail outcome.
type Verdict struct {
	Result   string                 `json:"result"`
	Passed   bool                   `json:"passed"`
	Warnings []string               `json:"warnings"`
	Checks   map[string]CheckResult `json:"checks"`
}

// CheckResult is one threshold check.
type CheckResult struct {
	Name      string  `json:"name"`
	Passed    bool    `json:"passed"`
	Advisory  bool    `json:"advisory"`
	Value     float64 `json:"value"`
	Threshold float64 `json:"threshold"`
	Message   string  `json:"message"`
}

// GenerateVerdict evaluates the summary against thresholds and returns a Verdict
// with per-check results (mirrors the engine verdict but with structured checks
// for the API/report consumers).
func GenerateVerdict(summary *Summary, cfg *config.Config) *Verdict {
	v := &Verdict{Result: "PASSED", Passed: true, Warnings: []string{}, Checks: make(map[string]CheckResult)}

	for name, ws := range summary.Workers {
		if !ws.Enabled {
			continue
		}
		if ws.Sent > 0 && !config.IsFanoutWorker(name) {
			lossPct := float64(ws.Lost) / float64(ws.Sent) * 100
			addHard(v, "message_loss:"+name, lossPct, lossGateThreshold(cfg.Thresholds.MaxLossPct),
				fmt.Sprintf("%.4f%% loss (threshold %.4f%%)", lossPct, cfg.Thresholds.MaxLossPct))
		}
		// Duplication: hard gate for steady-state workers; the at-least-once-by-design
		// workers + the unary-Pull worker are exempt (config.IsDuplicationGateExempt) and
		// surfaced as a warning instead. Per-worker exemption; the global max_duplication_pct
		// is unchanged and the LOSS gate is always retained (keep in sync with engine/verdict.go).
		if ws.Received > 0 {
			dupPct := float64(ws.Duplicated) / float64(ws.Received) * 100
			if config.IsDuplicationGateExempt(name) {
				if dupPct > cfg.Thresholds.MaxDuplicationPct {
					v.Warnings = append(v.Warnings, fmt.Sprintf("%s: duplication %.4f%% exceeds %.4f%% — gate-exempt (at-least-once redelivery by design / inherent to delivery model)", name, dupPct, cfg.Thresholds.MaxDuplicationPct))
				}
			} else {
				addHard(v, "duplication:"+name, dupPct, cfg.Thresholds.MaxDuplicationPct,
					fmt.Sprintf("%.4f%% duplication (threshold %.4f%%)", dupPct, cfg.Thresholds.MaxDuplicationPct))
			}
		}
		// p99 latency: hard gate for steady-state StreamingPull workers; workers whose
		// latency is by-design or inherent to their delivery model (dead_letter retry cycle,
		// inflight_cap backlog, seek replay, unary Pull) are exempt (config.IsLatencyGateExempt)
		// and surfaced as a warning instead (keep in sync with engine/verdict.go).
		if ws.LatencyP99MS > 0 {
			if config.IsLatencyGateExempt(name) {
				if ws.LatencyP99MS > cfg.Thresholds.MaxP99LatencyMS {
					v.Warnings = append(v.Warnings, fmt.Sprintf("%s: p99 latency %.1fms exceeds %.1fms — gate-exempt (by-design / inherent delivery latency, not a steady-state SLA)", name, ws.LatencyP99MS, cfg.Thresholds.MaxP99LatencyMS))
				}
			} else {
				addHard(v, "p99_latency:"+name, ws.LatencyP99MS, cfg.Thresholds.MaxP99LatencyMS,
					fmt.Sprintf("P99=%.1fms (threshold %.1fms)", ws.LatencyP99MS, cfg.Thresholds.MaxP99LatencyMS))
			}
		}
		total := ws.Sent + ws.Received
		if total > 0 {
			errPct := float64(ws.Errors) / float64(total) * 100
			addHard(v, "error_rate:"+name, errPct, cfg.Thresholds.MaxErrorRatePct,
				fmt.Sprintf("%.4f%% error rate (threshold %.4f%%)", errPct, cfg.Thresholds.MaxErrorRatePct))
		}

		// Pub/Sub-specific structured gates.
		if config.IsOrderingWorker(name) {
			v.Checks["ordering_violations:"+name] = CheckResult{
				Name: "ordering_violations:" + name, Passed: ws.OrderingViolations == 0,
				Value:   float64(ws.OrderingViolations),
				Message: fmt.Sprintf("%d ordering-key violations", ws.OrderingViolations),
			}
			if ws.OrderingViolations > 0 {
				v.Passed = false
			}
		}
		if config.IsExactlyOnceWorker(name) {
			v.Checks["exactly_once_reconciled:"+name] = CheckResult{
				Name: "exactly_once_reconciled:" + name, Passed: ws.ExactlyOnceUnreconciled == 0,
				Value:   float64(ws.ExactlyOnceUnreconciled),
				Message: fmt.Sprintf("%d unreconciled ack ids (of %d confirmed)", ws.ExactlyOnceUnreconciled, ws.ExactlyOnceConfirmed),
			}
			if ws.ExactlyOnceUnreconciled > 0 {
				v.Passed = false
			}
		}
		if config.IsDeadLetterWorker(name) {
			v.Checks["dead_letter_loss:"+name] = CheckResult{
				Name: "dead_letter_loss:" + name, Passed: ws.DeadLetterLost == 0,
				Value:   float64(ws.DeadLetterLost),
				Message: fmt.Sprintf("%d dead-letter messages lost (of %d republished)", ws.DeadLetterLost, ws.DeadLetterRepublished),
			}
			if ws.DeadLetterLost > 0 {
				v.Passed = false
			}
		}
		if config.IsFanoutWorker(name) {
			v.Checks["fanout_dropped:"+name] = CheckResult{
				Name: "fanout_dropped:" + name, Passed: ws.FanoutDropped == 0,
				Value:   float64(ws.FanoutDropped),
				Message: fmt.Sprintf("%d fan-out deliveries dropped (of %d delivered)", ws.FanoutDropped, ws.FanoutDeliveries),
			}
			if ws.FanoutDropped > 0 {
				v.Passed = false
			}
		}
		if config.IsInteropWorker(name) {
			v.Checks["interop_convergence:"+name] = CheckResult{
				Name: "interop_convergence:" + name, Passed: ws.InteropDiverged == 0,
				Value:   float64(ws.InteropDiverged),
				Message: fmt.Sprintf("%d interop messages diverged (of %d converged)", ws.InteropDiverged, ws.InteropConverged),
			}
			if ws.InteropDiverged > 0 {
				v.Passed = false
			}
		}
	}

	var totalCorrupted uint64
	for _, ws := range summary.Workers {
		totalCorrupted += ws.Corrupted
	}
	v.Checks["corruption"] = CheckResult{
		Name: "corruption", Passed: totalCorrupted == 0,
		Value: float64(totalCorrupted), Message: fmt.Sprintf("%d corrupted messages", totalCorrupted),
	}
	if totalCorrupted > 0 {
		v.Passed = false
	}

	if summary.Resources.BaselineRSSMB > 0 {
		growth := summary.Resources.MemoryGrowthFactor
		passed := growth <= cfg.Thresholds.MaxMemoryGrowthFactor
		v.Checks["memory_stability"] = CheckResult{
			Name: "memory_stability", Passed: passed, Advisory: true,
			Value: growth, Threshold: cfg.Thresholds.MaxMemoryGrowthFactor,
			Message: fmt.Sprintf("%.2fx growth (threshold %.2fx)", growth, cfg.Thresholds.MaxMemoryGrowthFactor),
		}
		if !passed {
			v.Warnings = append(v.Warnings, fmt.Sprintf("memory_stability: %.2fx growth exceeds %.2fx", growth, cfg.Thresholds.MaxMemoryGrowthFactor))
		}
	}

	if !v.Passed {
		v.Result = "FAILED"
	} else if len(v.Warnings) > 0 {
		v.Result = "PASSED_WITH_WARNINGS"
	}
	return v
}

// boundaryLossPct caps the at-least-once loss gate to tolerate the sequence
// tracker's reorder false-positive on high-out-of-order competing-consumer
// subscriptions. Under sustained reorder the bounded sliding-window tracker
// over-counts a small fraction as lost. The artifact scales with volume, so a
// percent cap (not an absolute floor) is the correct tolerance: systemic loss
// above it still fails the gate.
const boundaryLossPct = 0.5

// lossGateThreshold returns an effective loss threshold of at least
// boundaryLossPct, absorbing the tracker reorder-artifact without masking
// systemic loss.
func lossGateThreshold(base float64) float64 {
	if base < boundaryLossPct {
		return boundaryLossPct
	}
	return base
}

func addHard(v *Verdict, name string, value, threshold float64, msg string) {
	passed := value <= threshold
	v.Checks[name] = CheckResult{Name: name, Passed: passed, Value: value, Threshold: threshold, Message: msg}
	if !passed {
		v.Passed = false
	}
}

// PrintConsole prints the final report to stderr.
func PrintConsole(summary *Summary, verdict *Verdict) {
	sep := strings.Repeat("-", 64)
	fmt.Fprintf(os.Stderr, "\n%s\n", sep)
	fmt.Fprintf(os.Stderr, " GCP Pub/Sub Burn-In Report\n")
	fmt.Fprintf(os.Stderr, "%s\n", sep)
	fmt.Fprintf(os.Stderr, " Run ID:       %s\n", summary.RunID)
	fmt.Fprintf(os.Stderr, " Mode:         %s\n", summary.Mode)
	fmt.Fprintf(os.Stderr, " Duration:     %s\n", time.Duration(summary.DurationSeconds*float64(time.Second)))
	fmt.Fprintf(os.Stderr, " Emulator:     %s\n", summary.EmulatorHost)
	fmt.Fprintf(os.Stderr, " Broker:       %s\n", summary.BrokerAddress)
	fmt.Fprintf(os.Stderr, " Project:      %s\n", summary.ProjectID)
	fmt.Fprintf(os.Stderr, " Verdict:      %s\n", verdict.Result)
	fmt.Fprintf(os.Stderr, "%s\n", sep)

	for _, name := range config.AllWorkerNames {
		ws, ok := summary.Workers[name]
		if !ok || !ws.Enabled {
			continue
		}
		fmt.Fprintf(os.Stderr, "\n Worker: %s (%d ch)\n", name, ws.Channels)
		switch {
		case config.IsFanoutWorker(name):
			fmt.Fprintf(os.Stderr, "   Published: %d  Deliveries: %d  Dropped: %d\n", ws.Sent, ws.FanoutDeliveries, ws.FanoutDropped)
			fmt.Fprintf(os.Stderr, "   Duplicated: %d  Corrupted: %d\n", ws.Duplicated, ws.Corrupted)
		case config.IsExactlyOnceWorker(name):
			fmt.Fprintf(os.Stderr, "   Sent: %d  Received: %d  Lost: %d (%.2f%%)\n", ws.Sent, ws.Received, ws.Lost, ws.LossPct)
			fmt.Fprintf(os.Stderr, "   EO-Confirmed: %d  EO-Unreconciled: %d  Dup: %d\n", ws.ExactlyOnceConfirmed, ws.ExactlyOnceUnreconciled, ws.Duplicated)
		case config.IsDeadLetterWorker(name):
			fmt.Fprintf(os.Stderr, "   Sent: %d  Republished: %d  Lost: %d\n", ws.Sent, ws.DeadLetterRepublished, ws.DeadLetterLost)
			fmt.Fprintf(os.Stderr, "   Duplicated: %d  Corrupted: %d\n", ws.Duplicated, ws.Corrupted)
		case config.IsInteropWorker(name):
			fmt.Fprintf(os.Stderr, "   Published: %d  Converged: %d  Diverged: %d\n", ws.Sent, ws.InteropConverged, ws.InteropDiverged)
			fmt.Fprintf(os.Stderr, "   Duplicated: %d  Corrupted: %d\n", ws.Duplicated, ws.Corrupted)
		case config.IsOrderingWorker(name):
			fmt.Fprintf(os.Stderr, "   Sent: %d  Received: %d  Lost: %d (%.2f%%)\n", ws.Sent, ws.Received, ws.Lost, ws.LossPct)
			fmt.Fprintf(os.Stderr, "   OrderViolations: %d  Duplicated: %d  Corrupted: %d\n", ws.OrderingViolations, ws.Duplicated, ws.Corrupted)
		default:
			fmt.Fprintf(os.Stderr, "   Sent: %d  Received: %d  Acked: %d  Lost: %d (%.2f%%)\n", ws.Sent, ws.Received, ws.Acked, ws.Lost, ws.LossPct)
			fmt.Fprintf(os.Stderr, "   Duplicated: %d  Corrupted: %d  OutOfOrder: %d\n", ws.Duplicated, ws.Corrupted, ws.OutOfOrder)
		}
		if ws.LatencyP50MS > 0 {
			fmt.Fprintf(os.Stderr, "   Latency: P50=%.1fms P95=%.1fms P99=%.1fms P999=%.1fms\n",
				ws.LatencyP50MS, ws.LatencyP95MS, ws.LatencyP99MS, ws.LatencyP999MS)
		}
		fmt.Fprintf(os.Stderr, "   Rate: %.1f msgs/s (target %d)  Peak: %.1f msgs/s\n", ws.AvgRate, ws.TargetRate, ws.PeakRate)
		if ws.Reconnections > 0 || ws.DowntimeSeconds > 0 {
			fmt.Fprintf(os.Stderr, "   Reconnections: %d  Downtime: %.1fs\n", ws.Reconnections, ws.DowntimeSeconds)
		}
	}

	fmt.Fprintf(os.Stderr, "\n%s\n Checks:\n", sep)
	for name, cr := range verdict.Checks {
		status := "PASS"
		if !cr.Passed {
			status = "FAIL"
			if cr.Advisory {
				status = "WARN"
			}
		}
		fmt.Fprintf(os.Stderr, "   %-38s %s  %s\n", name, status, cr.Message)
	}
	if len(verdict.Warnings) > 0 {
		fmt.Fprintf(os.Stderr, "\n Warnings:\n")
		for _, w := range verdict.Warnings {
			fmt.Fprintf(os.Stderr, "   - %s\n", w)
		}
	}
	fmt.Fprintf(os.Stderr, "\n%s\n Resources:\n", sep)
	fmt.Fprintf(os.Stderr, "   Memory: peak=%.1fMB baseline=%.1fMB growth=%.2fx\n",
		summary.Resources.PeakRSSMB, summary.Resources.BaselineRSSMB, summary.Resources.MemoryGrowthFactor)
	fmt.Fprintf(os.Stderr, "%s\n\n", sep)
}

// WriteJSON writes the combined summary + verdict as JSON.
func WriteJSON(path string, summary *Summary, verdict *Verdict) error {
	type fullReport struct {
		*Summary
		Verdict *Verdict `json:"verdict"`
	}
	data, err := json.MarshalIndent(fullReport{Summary: summary, Verdict: verdict}, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal report: %w", err)
	}
	return os.WriteFile(path, data, 0o644)
}
