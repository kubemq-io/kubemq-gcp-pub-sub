// Command burnin is the KubeMQ Google Cloud Pub/Sub burn-in soak harness (spec
// S6). It drives the emulator-protocol Pub/Sub connector through seven workers
// (publish_pull_round_trip, streaming_pull, ordering_keys, exactly_once,
// dead_letter, fan_out, cross_protocol_interop), exposes a control API +
// Prometheus metrics, and runs a warmup → measure → drain → verdict lifecycle.
// The transport drives the cloud.google.com/go/pubsub client (connecting via
// PUBSUB_EMULATOR_HOST); the interop worker additionally uses kubemq-go/v2 for
// the native Events Store side. Exit codes: 0 PASSED, 2 PASSED_WITH_WARNINGS,
// 1 FAILED/config-error.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/kubemq-io/kubemq-gcp-pub-sub/burnin/api"
	"github.com/kubemq-io/kubemq-gcp-pub-sub/burnin/config"
	"github.com/kubemq-io/kubemq-gcp-pub-sub/burnin/engine"
	"github.com/kubemq-io/kubemq-gcp-pub-sub/burnin/metrics"
	"github.com/kubemq-io/kubemq-gcp-pub-sub/burnin/report"
	"github.com/kubemq-io/kubemq-gcp-pub-sub/burnin/server"
	"github.com/kubemq-io/kubemq-gcp-pub-sub/burnin/transport"
)

var (
	configPath     = flag.String("config", "", "path to config YAML file")
	validateConfig = flag.Bool("validate-config", false, "validate config and exit")
	runFlag        = flag.Bool("run", false, "start a run immediately from config")
)

func main() {
	flag.Parse()

	cfgPath := *configPath
	if cfgPath == "" {
		cfgPath = os.Getenv("BURNIN_CONFIG_FILE")
	}
	cfgFile := config.FindConfigFile(cfgPath)

	cfg, err := config.Load(cfgFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "ERROR: load config: %v\n", err)
		os.Exit(1)
	}
	// Resolve the effective emulator endpoint once (bare host:port) so it appears
	// in /info, the banner, and the report.
	if cfg.GCP.EmulatorHost == "" {
		cfg.GCP.EmulatorHost = transport.EmulatorHost("", cfg.Broker.Address)
	}

	logger := setupLogger(cfg.Logging)

	if *validateConfig {
		hasErrors := false
		for _, e := range cfg.Validate() {
			if strings.HasPrefix(e.Error(), "WARNING:") {
				logger.Warn(e.Error())
			} else {
				logger.Error(e.Error())
				hasErrors = true
			}
		}
		if hasErrors {
			os.Exit(1)
		}
		logger.Info("config valid")
		os.Exit(0)
	}

	// Fail fast on a non-warning config error before starting anything.
	for _, e := range cfg.Validate() {
		if !strings.HasPrefix(e.Error(), "WARNING:") {
			logger.Error("invalid config", "error", e.Error())
			os.Exit(1)
		}
	}
	cfg.LogResourceWarnings(logger)

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGTERM, syscall.SIGINT)

	metrics.InitMetrics(config.AllWorkerNames)

	eng := engine.New(cfg, logger)
	adapter := &engineAdapter{eng: eng, startupCfg: cfg, logger: logger}

	srv := server.New(cfg.Metrics.Port, adapter, cfg, logger)
	if err := srv.Start(); err != nil {
		logger.Error("failed to start server", "error", err)
		os.Exit(1)
	}

	printBanner(cfg, logger)

	if *runFlag {
		if err := eng.StartRunFromConfig(cfg); err != nil {
			logger.Error("failed to start run", "error", err)
			os.Exit(1)
		}
	}

	// Wait for either a signal or, when -run was given with a bounded duration,
	// the run completing on its own.
	waitForExit(eng, sigCh, cfg, *runFlag)

	logger.Info("shutting down")
	passed := eng.GracefulShutdown()

	summary, verdict := adapter.RunReport()
	if summary != nil && verdict != nil {
		if cfg.Output.ReportFile != "" {
			if err := report.WriteJSON(cfg.Output.ReportFile, summary, verdict); err != nil {
				logger.Error("failed to write report", "error", err)
			}
		}
		report.PrintConsole(summary, verdict)
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = srv.Stop(shutdownCtx)

	if !passed {
		os.Exit(1)
	}
	if eng.HasWarnings() {
		os.Exit(2)
	}
	os.Exit(0)
}

// waitForExit blocks until a termination signal arrives, or — for a bounded
// -run invocation — until the engine reaches a terminal state on its own.
func waitForExit(eng *engine.Engine, sigCh <-chan os.Signal, cfg *config.Config, autoRun bool) {
	if autoRun && cfg.DurationParsed > 0 {
		ticker := time.NewTicker(500 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-sigCh:
				return
			case <-ticker.C:
				switch eng.State() {
				case engine.StateStopped, engine.StateError:
					return
				}
			}
		}
	}
	<-sigCh
}

// engineAdapter bridges *engine.Engine to server.RunController.
type engineAdapter struct {
	eng        *engine.Engine
	startupCfg *config.Config
	logger     *slog.Logger
}

func (a *engineAdapter) State() string { return a.eng.State() }

func (a *engineAdapter) StartRun(rc *api.RunConfig) error {
	cfg, err := rc.ToInternalConfig(a.startupCfg)
	if err != nil {
		return fmt.Errorf("convert run config: %w", err)
	}
	if cfg.GCP.EmulatorHost == "" {
		cfg.GCP.EmulatorHost = transport.EmulatorHost("", cfg.Broker.Address)
	}
	return a.eng.StartRunFromConfig(cfg)
}

func (a *engineAdapter) StopRun() error            { return a.eng.StopRun() }
func (a *engineAdapter) RunID() string             { return a.eng.RunID() }
func (a *engineAdapter) RunConfig() *config.Config { return a.eng.RunConfig() }

func (a *engineAdapter) RunReport() (*report.Summary, *report.Verdict) {
	snapshots := a.eng.GetWorkerSnapshots()
	if snapshots == nil {
		return nil, nil
	}
	cfg := a.eng.RunConfig()
	if cfg == nil {
		cfg = a.startupCfg
	}

	summary := &report.Summary{
		RunID:         a.eng.RunID(),
		SDK:           metrics.SDK(),
		SDKVersion:    cfg.Output.SDKVersion,
		Mode:          cfg.Mode,
		BrokerAddress: cfg.Broker.Address,
		ProjectID:     cfg.GCP.ProjectID,
		EmulatorHost:  cfg.GCP.EmulatorHost,
		StartedAt:     a.eng.RunStartedAt(),
		EndedAt:       a.eng.RunEndedAt(),
		Workers:       make(map[string]*report.WorkerStats),
	}
	if !summary.EndedAt.IsZero() && !summary.StartedAt.IsZero() {
		summary.DurationSeconds = summary.EndedAt.Sub(summary.StartedAt).Seconds()
	}

	enabledCount := 0
	for _, name := range config.AllWorkerNames {
		snap := snapshots[name]
		if snap == nil {
			continue
		}
		wc := cfg.GetWorkerConfig(name)
		if wc == nil || !wc.Enabled {
			continue
		}
		enabledCount++

		ws := &report.WorkerStats{
			Enabled:                 true,
			Sent:                    snap.Sent,
			Received:                snap.Received,
			Lost:                    snap.Lost,
			Duplicated:              snap.Duplicated,
			Corrupted:               snap.Corrupted,
			OutOfOrder:              snap.OutOfOrder,
			Acked:                   snap.Acked,
			Errors:                  snap.Errors,
			Reconnections:           snap.Reconnections,
			DowntimeSeconds:         snap.DowntimeSeconds,
			OrderingViolations:      snap.OrderingViolations,
			FanoutDeliveries:        snap.FanoutDeliveries,
			FanoutDropped:           snap.FanoutDropped,
			ExactlyOnceConfirmed:    snap.ExactlyOnceConfirmed,
			ExactlyOnceUnreconciled: snap.ExactlyOnceUnreconciled,
			DeadLetterRepublished:   snap.DeadLetterRepublished,
			DeadLetterLost:          snap.DeadLetterLost,
			InteropConverged:        snap.InteropConverged,
			InteropDiverged:         snap.InteropDiverged,
			LatencyP50MS:            snap.LatencyP50,
			LatencyP95MS:            snap.LatencyP95,
			LatencyP99MS:            snap.LatencyP99,
			LatencyP999MS:           snap.LatencyP999,
			AvgRate:                 snap.AvgRate,
			PeakRate:                snap.PeakRate,
			TargetRate:              wc.Rate,
			Channels:                wc.Channels,
		}
		if snap.Sent > 0 {
			ws.LossPct = float64(snap.Lost) / float64(snap.Sent) * 100
		}
		summary.Workers[name] = ws
	}
	summary.AllWorkersEnabled = enabledCount == len(config.AllWorkerNames)

	baseline := a.eng.BaselineRSS()
	peak := a.eng.PeakRSS()
	summary.Resources = report.ResourceStats{
		PeakRSSMB:     float64(peak) / 1024 / 1024,
		BaselineRSSMB: float64(baseline) / 1024 / 1024,
	}
	if baseline > 0 {
		summary.Resources.MemoryGrowthFactor = float64(peak) / float64(baseline)
	}

	verdict := report.GenerateVerdict(summary, cfg)
	// Reconcile with the engine's authoritative verdict (it gates on extras like
	// throughput / latency P50-P999 that the report verdict trims).
	if ev := a.eng.Verdict(); ev != nil && !ev.Passed {
		verdict.Passed = false
		verdict.Result = "FAILED"
		verdict.Warnings = append(verdict.Warnings, ev.Warnings...)
	} else if ev != nil && ev.Result == "PASSED_WITH_WARNINGS" && verdict.Result == "PASSED" {
		verdict.Result = "PASSED_WITH_WARNINGS"
		verdict.Warnings = append(verdict.Warnings, ev.Warnings...)
	}
	return summary, verdict
}

func (a *engineAdapter) RunStatus() api.RunStatus {
	status := api.RunStatus{State: a.eng.State(), RunID: a.eng.RunID()}
	if !a.eng.RunStartedAt().IsZero() {
		status.Uptime = time.Since(a.eng.RunStartedAt()).Seconds()
	}
	groups := a.eng.WorkerGroups()
	if groups != nil {
		status.Workers = make(map[string]api.WorkerStatus)
		for name, g := range groups {
			var ws api.WorkerStatus
			for _, w := range g.Workers() {
				ws.Sent += w.SentCount()
				ws.Received += w.ReceivedCount()
				ws.Errors += w.ErrorCount()
				ws.Rate += w.RateWindow().Rate()
			}
			status.Workers[name] = ws
		}
	}
	return status
}

func (a *engineAdapter) RunSnapshot() api.RunSnapshot {
	snap := api.RunSnapshot{State: a.eng.State(), RunID: a.eng.RunID(), Config: a.eng.RunConfig()}
	groups := a.eng.WorkerGroups()
	if groups == nil {
		return snap
	}
	snap.Workers = make(map[string][]api.WorkerSnapshot)
	var totalSent, totalRecv, totalErrors, totalLost, totalCorrupted, totalDup uint64
	var fanoutDeliveries, fanoutDropped uint64
	for name, g := range groups {
		var workers []api.WorkerSnapshot
		for _, w := range g.Workers() {
			workers = append(workers, api.WorkerSnapshot{
				ID:       fmt.Sprintf("%s/%s", w.Name(), w.ChannelName()),
				Worker:   w.Name(),
				Channel:  w.ChannelName(),
				Sent:     w.SentCount(),
				Received: w.ReceivedCount(),
				Errors:   w.ErrorCount(),
				Rate:     w.RateWindow().Rate(),
			})
			totalSent += w.SentCount()
			totalRecv += w.ReceivedCount()
			totalErrors += w.ErrorCount()
			totalLost += w.Tracker().TotalLost()
			totalCorrupted += w.CorruptedCount()
			totalDup += w.DuplicatedCount()
			fanoutDeliveries += w.FanoutDeliveries()
			fanoutDropped += w.FanoutDropped()
		}
		snap.Workers[name] = workers
	}
	var uptime float64
	if !a.eng.RunStartedAt().IsZero() {
		uptime = time.Since(a.eng.RunStartedAt()).Seconds()
	}
	snap.Metrics = api.MetricsSnapshot{
		TotalSent: totalSent, TotalReceived: totalRecv, TotalErrors: totalErrors,
		TotalLost: totalLost, TotalCorrupted: totalCorrupted, TotalDuplicated: totalDup,
		FanoutDeliveries: fanoutDeliveries, FanoutDropped: fanoutDropped,
		UptimeSeconds: uptime,
	}
	return snap
}

func setupLogger(logCfg config.LoggingConfig) *slog.Logger {
	opts := &slog.HandlerOptions{Level: parseLogLevel(logCfg.Level)}
	var handler slog.Handler
	if logCfg.Format == "json" {
		handler = slog.NewJSONHandler(os.Stderr, opts)
	} else {
		handler = slog.NewTextHandler(os.Stderr, opts)
	}
	return slog.New(handler)
}

func parseLogLevel(s string) slog.Level {
	switch strings.ToLower(s) {
	case "debug":
		return slog.LevelDebug
	case "warn", "warning":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

func printBanner(cfg *config.Config, logger *slog.Logger) {
	logger.Info("GCP Pub/Sub burn-in starting",
		"mode", cfg.Mode,
		"emulator", cfg.GCP.EmulatorHost,
		"broker", cfg.Broker.Address,
		"grpc", cfg.Broker.GRPCAddress,
		"project", cfg.GCP.ProjectID,
		"port", cfg.Metrics.Port,
		"duration", cfg.Duration,
		"channels", cfg.TotalChannelCount(),
	)
}
