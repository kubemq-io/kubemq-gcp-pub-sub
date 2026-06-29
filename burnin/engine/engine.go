package engine

import (
	"context"
	"fmt"
	"log/slog"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/kubemq-io/kubemq-gcp-pub-sub/burnin/config"
	"github.com/kubemq-io/kubemq-gcp-pub-sub/burnin/disconnect"
	"github.com/kubemq-io/kubemq-gcp-pub-sub/burnin/metrics"
	"github.com/kubemq-io/kubemq-gcp-pub-sub/burnin/worker"
)

// Engine states.
const (
	StateIdle     = "idle"
	StateStarting = "starting"
	StateRunning  = "running"
	StateStopping = "stopping"
	StateStopped  = "stopped"
	StateError    = "error"
)

// WorkerSnapshot holds frozen counter values captured at producer-stop time.
type WorkerSnapshot struct {
	Sent            uint64
	Received        uint64
	Errors          uint64
	Corrupted       uint64
	Reconnections   uint64
	DowntimeSeconds float64
	Duplicated      uint64
	Lost            uint64
	OutOfOrder      uint64
	Acked           uint64

	// Pub/Sub-specific fidelity counters (spec S6.6).
	OrderingViolations      uint64
	FanoutDeliveries        uint64
	FanoutDropped           uint64
	ExactlyOnceConfirmed    uint64
	ExactlyOnceUnreconciled uint64
	DeadLetterRepublished   uint64
	DeadLetterLost          uint64
	InteropConverged        uint64
	InteropDiverged         uint64

	// New data-plane coverage counters (spec S4/S6.3).
	NackRedelivered                uint64
	BatchAtomicityViolations       uint64
	FilterMismatch                 uint64
	FilterMatchedDelivered         uint64
	SchemaRejectViolations         uint64
	SchemaEnforceConfirmed         uint64
	SeekReplayExpected             uint64
	SeekReplayObserved             uint64
	SeekReplayMismatch             uint64
	InflightCapTailRedelivered     uint64
	InflightCapLost                uint64
	ExactlyOnceInvalidAckConfirmed uint64
	FlowControlViolations          uint64
	RetryBackoffObserved           uint64
	LeaseExtendConfirmed           uint64
	LeaseExtendRejectConfirmed     uint64
	DeliveryAttemptMax             uint64
	CrossStreamAckConfirmed        uint64
	PeriodicCloseObserved          uint64

	// Per-probe "ran" flags (spec S4/S6.3): true iff the probe actually executed,
	// so a disabled or skipped probe cannot false-fail the "never observed" gates.
	FlowControlProbeRan    bool
	InvalidAckProbeRan     bool
	ConfirmationsProbeRan  bool
	CrossStreamAckProbeRan bool
	LeaseExtendProbeRan    bool
	SchemaEnforceProbeRan  bool

	LatencyP50  float64
	LatencyP95  float64
	LatencyP99  float64
	LatencyP999 float64
	PeakRate    float64
	AvgRate     float64
}

// VerdictResult holds the pass/fail outcome and any warnings.
type VerdictResult struct {
	Result   string // "PASSED", "PASSED_WITH_WARNINGS", "FAILED"
	Passed   bool
	Warnings []string
}

// Engine owns the run lifecycle.
type Engine struct {
	startupCfg *config.Config
	logger     *slog.Logger
	bootTime   time.Time

	mu                 sync.Mutex
	state              string
	runCfg             *config.Config
	runID              string
	runStartedAt       time.Time
	runEndedAt         time.Time
	producersStartedAt time.Time
	producersStoppedAt time.Time
	runError           string
	runCancel          context.CancelFunc
	runDone            chan struct{}

	workerGroups    map[string]*WorkerGroup
	workerSnapshots map[string]*WorkerSnapshot

	baselineRSS atomic.Uint64
	peakRSS     atomic.Uint64

	verdictResult *VerdictResult
}

// New creates an idle engine.
func New(cfg *config.Config, logger *slog.Logger) *Engine {
	return &Engine{
		startupCfg: cfg,
		logger:     logger,
		bootTime:   time.Now(),
		state:      StateIdle,
	}
}

// --- Accessors ---

func (e *Engine) State() string {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.state
}

func (e *Engine) RunID() string {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.runID
}

func (e *Engine) RunConfig() *config.Config {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.runCfg
}

func (e *Engine) RunStartedAt() time.Time {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.runStartedAt
}

func (e *Engine) RunEndedAt() time.Time {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.runEndedAt
}

func (e *Engine) WorkerGroups() map[string]*WorkerGroup {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.workerGroups
}

func (e *Engine) GetWorkerSnapshots() map[string]*WorkerSnapshot {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.workerSnapshots
}

func (e *Engine) BaselineRSS() uint64 { return e.baselineRSS.Load() }
func (e *Engine) PeakRSS() uint64     { return e.peakRSS.Load() }

func (e *Engine) Verdict() *VerdictResult {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.verdictResult
}

func (e *Engine) allWorkers() []worker.Worker {
	var workers []worker.Worker
	for _, name := range config.AllWorkerNames {
		if g := e.workerGroups[name]; g != nil {
			workers = append(workers, g.Workers()...)
		}
	}
	return workers
}

// AllWorkers returns every active worker (locked).
func (e *Engine) AllWorkers() []worker.Worker {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.allWorkers()
}

// --- Lifecycle ---

// StartRunFromConfig launches a run in the background.
func (e *Engine) StartRunFromConfig(cfg *config.Config) error {
	e.mu.Lock()
	if e.state != StateIdle && e.state != StateStopped && e.state != StateError {
		state := e.state
		e.mu.Unlock()
		return fmt.Errorf("cannot start run: engine in state %s", state)
	}
	e.state = StateStarting
	e.runCfg = cfg
	e.runID = cfg.RunID
	if e.runID == "" {
		e.runID = config.RandomRunID()
	}
	e.runStartedAt = time.Now()
	e.runEndedAt = time.Time{}
	e.runError = ""
	e.verdictResult = nil
	e.workerSnapshots = nil
	e.baselineRSS.Store(0)
	e.peakRSS.Store(0)

	ctx, cancel := context.WithCancel(context.Background())
	e.runCancel = cancel
	e.runDone = make(chan struct{})
	e.mu.Unlock()

	e.logger.Info("starting run", "run_id", e.runID)
	go e.runLoop(ctx, cfg)
	return nil
}

// StopRun cancels the active run.
func (e *Engine) StopRun() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.state != StateStarting && e.state != StateRunning {
		return fmt.Errorf("cannot stop run: engine in state %s", e.state)
	}
	if e.runCancel != nil {
		e.runCancel()
	}
	return nil
}

// GracefulShutdown cancels any active run, waits, and returns whether it passed.
func (e *Engine) GracefulShutdown() bool {
	e.mu.Lock()
	state := e.state
	cancel := e.runCancel
	done := e.runDone
	e.mu.Unlock()

	if state == StateRunning || state == StateStarting {
		if cancel != nil {
			cancel()
		}
		if done != nil {
			select {
			case <-done:
			case <-time.After(120 * time.Second):
				e.logger.Error("graceful shutdown timed out after 120s")
			}
		}
	}

	e.mu.Lock()
	defer e.mu.Unlock()
	return e.verdictResult != nil && e.verdictResult.Passed
}

// HasWarnings reports whether the verdict was PASSED_WITH_WARNINGS.
func (e *Engine) HasWarnings() bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.verdictResult != nil && e.verdictResult.Result == "PASSED_WITH_WARNINGS"
}

// --- Run loop ---

func (e *Engine) runLoop(ctx context.Context, cfg *config.Config) {
	defer func() {
		e.mu.Lock()
		if e.state != StateError {
			e.state = StateStopped
		}
		e.runEndedAt = time.Now()
		done := e.runDone
		e.mu.Unlock()
		if done != nil {
			close(done)
		}
	}()

	// Build worker groups for each enabled worker type.
	e.mu.Lock()
	e.workerGroups = make(map[string]*WorkerGroup)
	for _, name := range config.AllWorkerNames {
		wc := cfg.GetWorkerConfig(name)
		if wc == nil || !wc.Enabled {
			continue
		}
		e.workerGroups[name] = NewWorkerGroup(name, cfg, e.logger)
	}
	e.mu.Unlock()

	// Start consumers/receivers (also provisions topics/subscriptions). For the
	// interop worker this is where the native Events Store subscribe stream is
	// established BEFORE producers begin (subscribe-before-publish, spec S6.3).
	for _, name := range config.AllWorkerNames {
		g := e.workerGroups[name]
		if g == nil {
			continue
		}
		if err := g.StartConsumers(ctx); err != nil {
			e.setError(fmt.Sprintf("start consumers: %v", err))
			return
		}
	}

	// Wait for all consumers to signal ready.
	readyTimeout := 30 * time.Second
	for _, name := range config.AllWorkerNames {
		g := e.workerGroups[name]
		if g == nil {
			continue
		}
		if err := g.WaitForConsumerReady(readyTimeout); err != nil {
			e.setError(fmt.Sprintf("consumer ready: %v", err))
			return
		}
	}
	if ctx.Err() != nil {
		return
	}

	// Warmup: send producers briefly, then reset counters so warmup traffic
	// does not pollute the measurement window.
	e.logger.Info("starting warmup")
	e.warmup(ctx, cfg)
	if ctx.Err() != nil {
		return
	}

	// T0: producers start — measurement window begins.
	e.mu.Lock()
	e.producersStartedAt = time.Now()
	e.mu.Unlock()
	for _, name := range config.AllWorkerNames {
		g := e.workerGroups[name]
		if g == nil {
			continue
		}
		g.StartProducers()
		metrics.SetTargetRate(name, float64(cfg.GetWorkerRate(name)))
	}

	e.mu.Lock()
	e.state = StateRunning
	e.mu.Unlock()
	e.logger.Info("burn-in running", "run_id", e.runID, "duration", cfg.Duration)

	// Periodic tasks.
	periodicCtx, periodicCancel := context.WithCancel(ctx)
	var periodicWG sync.WaitGroup
	e.startPeriodicTasks(periodicCtx, &periodicWG, cfg)

	// Block until duration or cancellation.
	var durationCh <-chan time.Time
	if cfg.DurationParsed > 0 {
		timer := time.NewTimer(cfg.DurationParsed)
		defer timer.Stop()
		durationCh = timer.C
	}
	select {
	case <-ctx.Done():
		e.logger.Info("run cancelled")
	case <-durationCh:
		e.logger.Info("duration reached", "duration", cfg.Duration)
	}

	periodicCancel()
	periodicWG.Wait()

	e.mu.Lock()
	e.state = StateStopping
	e.mu.Unlock()

	e.shutdownWorkers(cfg)
	e.computeVerdict(cfg)
	e.logFinalSummary()
}

// --- Warmup ---

func (e *Engine) warmup(ctx context.Context, cfg *config.Config) {
	metrics.WarmupActive.WithLabelValues(metrics.SDK()).Set(1)
	defer metrics.WarmupActive.WithLabelValues(metrics.SDK()).Set(0)

	for _, name := range config.AllWorkerNames {
		if g := e.workerGroups[name]; g != nil {
			g.StartProducers()
		}
	}

	warmDur := cfg.WarmupDurationParsed
	if warmDur <= 0 {
		warmDur = 5 * time.Second
	}
	select {
	case <-ctx.Done():
	case <-time.After(warmDur):
	}

	stopAllProducersParallel(e.workerGroups)

	// Give in-flight warmup deliveries a beat to drain, then reset.
	select {
	case <-ctx.Done():
		return
	case <-time.After(time.Second):
	}

	for _, g := range e.workerGroups {
		for _, w := range g.Workers() {
			w.ResetAfterWarmup()
		}
	}
	e.logger.Info("warmup complete")
}

// stopAllProducersParallel stops every group's producers concurrently. Each
// StopProducers is bounded by stopWaitTimeout (a producer blocked in an
// unresponsive publish can take that long to unwind), so stopping sequentially
// would stack the timeouts (N×15s) and balloon both warmup and shutdown.
func stopAllProducersParallel(groups map[string]*WorkerGroup) {
	var wg sync.WaitGroup
	for _, g := range groups {
		if g == nil {
			continue
		}
		wg.Add(1)
		go func(g *WorkerGroup) { defer wg.Done(); g.StopProducers() }(g)
	}
	wg.Wait()
}

// --- Two-phase shutdown ---

func (e *Engine) shutdownWorkers(cfg *config.Config) {
	// Freeze the measurement window at producer-stop.
	e.mu.Lock()
	e.producersStoppedAt = time.Now()
	e.mu.Unlock()

	stopAllProducersParallel(e.workerGroups)
	e.logger.Info("producers stopped, draining", "drain_seconds", cfg.Shutdown.DrainTimeoutSeconds)

	// Drain window: let in-flight deliveries land at the receivers before we
	// measure. Pub/Sub StreamingPull / Pull adds a network hop and lease
	// redelivery can be ack-deadline-granular, so without this the last in-flight
	// messages would be miscounted as loss.
	e.drainToQuiescence(time.Duration(cfg.Shutdown.DrainTimeoutSeconds) * time.Second)

	// Final gap detection so the sequence trackers' TotalLost is current, then
	// snapshot the now-settled counters (including fan-out/ordering fidelity extras).
	for name, g := range e.workerGroups {
		for _, w := range g.Workers() {
			for _, delta := range w.Tracker().DetectGaps() {
				metrics.AddLost(name, delta)
			}
		}
	}

	e.captureWorkerSnapshots()

	// Stop consumers in parallel: each StopConsumers is bounded by stopWaitTimeout,
	// so a sequential loop would stack those timeouts (N×15s) and drag shutdown out.
	var stopWG sync.WaitGroup
	for _, name := range config.AllWorkerNames {
		if g := e.workerGroups[name]; g != nil {
			stopWG.Add(1)
			go func(g *WorkerGroup) { defer stopWG.Done(); g.StopConsumers() }(g)
		}
	}
	stopWG.Wait()

	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	if m.Sys > e.peakRSS.Load() {
		e.peakRSS.Store(m.Sys)
	}
	e.logger.Info("all workers stopped")
}

// drainToQuiescence waits for in-flight deliveries to settle (total received
// count stable across consecutive polls) before the snapshot, bounded by
// maxWait. It replaces a fixed drain sleep: at a hard stop the last at-least-once
// (re)deliveries may still be in flight, and a fixed sleep freezes the snapshot
// at an arbitrary mid-pipeline instant. Polling to quiescence lets those land and
// reconcile against the tracker's pending-loss set, minimising boundary miscounts.
func (e *Engine) drainToQuiescence(maxWait time.Duration) {
	const pollInterval = 250 * time.Millisecond
	const stablePolls = 4 // ~1s with no new deliveries => quiescent
	if maxWait <= 0 {
		maxWait = time.Second
	}
	deadline := time.Now().Add(maxWait)
	prev := e.totalReceived()
	stable := 0
	for time.Now().Before(deadline) {
		time.Sleep(pollInterval)
		// Surface pending-loss recoveries from late deliveries to the live metric.
		for name, g := range e.workerGroups {
			for _, w := range g.Workers() {
				for _, delta := range w.Tracker().DetectGaps() {
					metrics.AddLost(name, delta)
				}
			}
		}
		cur := e.totalReceived()
		if cur == prev {
			if stable++; stable >= stablePolls {
				return
			}
		} else {
			stable = 0
			prev = cur
		}
	}
}

// totalReceived sums received counts across all workers (quiescence signal).
func (e *Engine) totalReceived() uint64 {
	var total uint64
	for _, g := range e.workerGroups {
		for _, w := range g.Workers() {
			total += w.ReceivedCount()
		}
	}
	return total
}

func (e *Engine) captureWorkerSnapshots() {
	e.mu.Lock()
	defer e.mu.Unlock()

	e.workerSnapshots = make(map[string]*WorkerSnapshot)

	for name, g := range e.workerGroups {
		snap := &WorkerSnapshot{}
		var bestLat *metricsLatency
		var bestLatCount int64

		for _, w := range g.Workers() {
			snap.Sent += w.SentCount()
			snap.Received += w.ReceivedCount()
			snap.Errors += w.ErrorCount()
			snap.Corrupted += w.CorruptedCount()
			snap.Reconnections += w.ReconnectionCount()
			snap.DowntimeSeconds += w.DowntimeSeconds()
			snap.Duplicated += w.DuplicatedCount()
			snap.Lost += w.Tracker().TotalLost()
			snap.OutOfOrder += w.Tracker().TotalOutOfOrder()
			snap.Acked += w.AckedCount()
			snap.OrderingViolations += w.OrderingViolations()
			snap.FanoutDeliveries += w.FanoutDeliveries()
			snap.FanoutDropped += w.FanoutDropped()
			snap.ExactlyOnceConfirmed += w.ExactlyOnceConfirmed()
			snap.ExactlyOnceUnreconciled += w.ExactlyOnceUnreconciled()
			snap.DeadLetterRepublished += w.DeadLetterRepublished()
			snap.DeadLetterLost += w.DeadLetterLost()
			snap.InteropConverged += w.InteropConverged()
			snap.InteropDiverged += w.InteropDiverged()

			// New data-plane coverage counters.
			snap.NackRedelivered += w.NackRedelivered()
			snap.BatchAtomicityViolations += w.BatchAtomicityViolations()
			snap.FilterMismatch += w.FilterMismatch()
			snap.FilterMatchedDelivered += w.FilterMatchedDelivered()
			snap.SchemaRejectViolations += w.SchemaRejectViolations()
			snap.SchemaEnforceConfirmed += w.SchemaEnforceConfirmed()
			snap.SeekReplayExpected += w.SeekReplayExpected()
			snap.SeekReplayObserved += w.SeekReplayObserved()
			snap.SeekReplayMismatch += w.SeekReplayMismatch()
			snap.InflightCapTailRedelivered += w.InflightCapTailRedelivered()
			snap.InflightCapLost += w.InflightCapLost()
			snap.ExactlyOnceInvalidAckConfirmed += w.ExactlyOnceInvalidAckConfirmed()
			snap.FlowControlViolations += w.FlowControlViolations()
			snap.RetryBackoffObserved += w.RetryBackoffObserved()
			snap.LeaseExtendConfirmed += w.LeaseExtendConfirmed()
			snap.LeaseExtendRejectConfirmed += w.LeaseExtendRejectConfirmed()
			snap.CrossStreamAckConfirmed += w.CrossStreamAckConfirmed()
			snap.PeriodicCloseObserved += w.PeriodicCloseObserved()

			// DeliveryAttemptMax is a max gauge, not a sum.
			if d := w.DeliveryAttemptMax(); d > snap.DeliveryAttemptMax {
				snap.DeliveryAttemptMax = d
			}

			// Probe-ran flags: OR across the group so the gate is active if any
			// worker in the group ran the probe (spec S6.3).
			snap.FlowControlProbeRan = snap.FlowControlProbeRan || w.FlowControlProbeRan()
			snap.InvalidAckProbeRan = snap.InvalidAckProbeRan || w.InvalidAckProbeRan()
			snap.ConfirmationsProbeRan = snap.ConfirmationsProbeRan || w.ConfirmationsProbeRan()
			snap.CrossStreamAckProbeRan = snap.CrossStreamAckProbeRan || w.CrossStreamAckProbeRan()
			snap.LeaseExtendProbeRan = snap.LeaseExtendProbeRan || w.LeaseExtendProbeRan()
			snap.SchemaEnforceProbeRan = snap.SchemaEnforceProbeRan || w.SchemaEnforceProbeRan()

			if c := w.LatencyAccumulator().Count(); c > bestLatCount {
				bestLatCount = c
				bestLat = wrapLatency(w.LatencyAccumulator())
			}

			if p := w.PeakRate().Peak(); p > snap.PeakRate {
				snap.PeakRate = p
			}
			snap.AvgRate += w.RateWindow().Rate()
		}

		if bestLat != nil {
			snap.LatencyP50, snap.LatencyP95, snap.LatencyP99, snap.LatencyP999 = bestLat.Percentiles()
		}

		// For the fan_out worker, fold the per-(producer, subscription) tracker
		// loss into FanoutDropped for the dedicated per-sub-completeness gate, and
		// DO NOT also charge it as channel loss (which would double-count). For all
		// other workers, the standard tracker loss is the loss gate.
		if config.IsFanoutWorker(name) {
			snap.FanoutDropped += snap.Lost
			snap.Lost = 0
		}

		e.workerSnapshots[name] = snap
	}
}

// --- Periodic tasks ---

func (e *Engine) startPeriodicTasks(ctx context.Context, wg *sync.WaitGroup, cfg *config.Config) {
	if cfg.ReportIntervalParsed > 0 {
		wg.Add(1)
		go func() { defer wg.Done(); e.periodicReporter(ctx, cfg) }()
	}
	wg.Add(1)
	go func() { defer wg.Done(); e.rateAdvancer(ctx) }()
	wg.Add(1)
	go func() { defer wg.Done(); e.uptimeTracker(ctx) }()
	wg.Add(1)
	go func() { defer wg.Done(); e.memoryTracker(ctx) }()
	wg.Add(1)
	go func() { defer wg.Done(); e.gapDetector(ctx) }()

	if cfg.ForcedDisconnInterval > 0 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			dm := disconnect.New(cfg.ForcedDisconnInterval, cfg.ForcedDisconnDuration, e.disconnectTargets(), e.logger)
			dm.Run(ctx)
		}()
	}
}

func (e *Engine) disconnectTargets() []disconnect.Target {
	var targets []disconnect.Target
	for _, w := range e.AllWorkers() {
		targets = append(targets, w)
	}
	return targets
}

func (e *Engine) periodicReporter(ctx context.Context, cfg *config.Config) {
	ticker := time.NewTicker(cfg.ReportIntervalParsed)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			e.logStatus()
		}
	}
}

func (e *Engine) logStatus() {
	e.mu.Lock()
	groups := e.workerGroups
	e.mu.Unlock()

	for _, name := range config.AllWorkerNames {
		g := groups[name]
		if g == nil {
			continue
		}
		var sent, received, errors, dup, lost, dropped, orderViol uint64
		var rate float64
		for _, w := range g.Workers() {
			sent += w.SentCount()
			received += w.ReceivedCount()
			errors += w.ErrorCount()
			dup += w.DuplicatedCount()
			lost += w.Tracker().TotalLost()
			dropped += w.FanoutDropped()
			orderViol += w.OrderingViolations()
			rate += w.RateWindow().Rate()
		}
		switch {
		case config.IsFanoutWorker(name):
			e.logger.Info(name, "pub", sent, "recv", received, "dropped", dropped,
				"dup", dup, "err", errors, "rate", fmt.Sprintf("%.1f/s", rate))
		case config.IsOrderingWorker(name):
			e.logger.Info(name, "sent", sent, "recv", received, "lost", lost,
				"order_viol", orderViol, "dup", dup, "err", errors, "rate", fmt.Sprintf("%.1f/s", rate))
		default:
			e.logger.Info(name, "sent", sent, "recv", received, "lost", lost,
				"dup", dup, "err", errors, "rate", fmt.Sprintf("%.1f/s", rate))
		}
	}
}

func (e *Engine) rateAdvancer(ctx context.Context) {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			e.mu.Lock()
			groups := e.workerGroups
			e.mu.Unlock()
			for name, g := range groups {
				var totalRate float64
				var totalSent, totalRecv uint64
				for _, w := range g.Workers() {
					w.AdvanceRateWindows()
					totalRate += w.RateWindow().Rate()
					totalSent += w.SentCount()
					totalRecv += w.ReceivedCount()
				}
				metrics.SetActualRate(name, totalRate)
				if totalSent > totalRecv {
					metrics.SetConsumerLag(name, float64(totalSent-totalRecv))
				} else {
					metrics.SetConsumerLag(name, 0)
				}
			}
		}
	}
}

func (e *Engine) uptimeTracker(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			metrics.UptimeSeconds.WithLabelValues(metrics.SDK()).Set(time.Since(e.bootTime).Seconds())
		}
	}
}

func (e *Engine) memoryTracker(ctx context.Context) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	start := time.Now()
	baselineSet := false
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			var m runtime.MemStats
			runtime.ReadMemStats(&m)
			if m.Sys > e.peakRSS.Load() {
				e.peakRSS.Store(m.Sys)
			}
			deadline := 5 * time.Minute
			if e.runCfg != nil && e.runCfg.DurationParsed > 0 && e.runCfg.DurationParsed < 5*time.Minute {
				deadline = 10 * time.Second
			}
			if !baselineSet && time.Since(start) >= deadline {
				e.baselineRSS.Store(m.Sys)
				baselineSet = true
				e.logger.Info("memory baseline captured", "rss_mb", m.Sys/1024/1024)
			}
		}
	}
}

func (e *Engine) gapDetector(ctx context.Context) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			e.mu.Lock()
			groups := e.workerGroups
			e.mu.Unlock()
			for name, g := range groups {
				for _, w := range g.Workers() {
					for _, delta := range w.Tracker().DetectGaps() {
						metrics.AddLost(name, delta)
					}
				}
			}
		}
	}
}

// --- Error helper ---

func (e *Engine) setError(msg string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.state = StateError
	e.runError = msg
	e.logger.Error("engine error", "error", msg)
}

func (e *Engine) logFinalSummary() {
	e.mu.Lock()
	verdict := e.verdictResult
	snapshots := e.workerSnapshots
	e.mu.Unlock()
	if verdict == nil {
		return
	}

	sep := "==========================================="
	e.logger.Info(sep)
	e.logger.Info("burn-in verdict", "result", verdict.Result, "passed", verdict.Passed)
	for _, name := range config.AllWorkerNames {
		snap := snapshots[name]
		if snap == nil {
			continue
		}
		switch {
		case config.IsFanoutWorker(name):
			e.logger.Info("  "+name, "pub", snap.Sent, "deliveries", snap.FanoutDeliveries,
				"dropped", snap.FanoutDropped, "dup", snap.Duplicated, "err", snap.Errors)
		case config.IsOrderingWorker(name):
			e.logger.Info("  "+name, "sent", snap.Sent, "recv", snap.Received, "lost", snap.Lost,
				"order_viol", snap.OrderingViolations, "dup", snap.Duplicated, "err", snap.Errors)
		case config.IsExactlyOnceWorker(name):
			e.logger.Info("  "+name, "sent", snap.Sent, "recv", snap.Received, "lost", snap.Lost,
				"eo_confirmed", snap.ExactlyOnceConfirmed, "eo_unreconciled", snap.ExactlyOnceUnreconciled, "err", snap.Errors)
		case config.IsDeadLetterWorker(name):
			e.logger.Info("  "+name, "sent", snap.Sent, "republished", snap.DeadLetterRepublished,
				"dl_lost", snap.DeadLetterLost, "err", snap.Errors)
		case config.IsInteropWorker(name):
			e.logger.Info("  "+name, "sent", snap.Sent, "converged", snap.InteropConverged,
				"diverged", snap.InteropDiverged, "err", snap.Errors)
		default:
			e.logger.Info("  "+name, "sent", snap.Sent, "recv", snap.Received, "lost", snap.Lost,
				"dup", snap.Duplicated, "err", snap.Errors, "acked", snap.Acked)
		}
	}
	for _, wmsg := range verdict.Warnings {
		e.logger.Warn("  " + wmsg)
	}
	e.logger.Info(sep)
}
