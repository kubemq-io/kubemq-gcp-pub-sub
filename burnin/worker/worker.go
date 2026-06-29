// Package worker implements the seven Google Cloud Pub/Sub burn-in workers
// (spec S6.3): publish_pull_round_trip, streaming_pull, ordering_keys,
// exactly_once, dead_letter, fan_out, and cross_protocol_interop. Each drives
// the official cloud.google.com/go/pubsub client against the KubeMQ Pub/Sub
// connector (via PUBSUB_EMULATOR_HOST, zero-auth insecure gRPC) and records
// loss / duplication / latency / throughput plus the per-pattern fidelity
// counters through the shared BaseWorker. The cross_protocol_interop worker
// ALSO drives a native github.com/kubemq-io/kubemq-go/v2 client to consume the
// topic's Events Store log pubsub.{topic} (subscribe-before-publish, "new only").
// There is NO gRPC RPC responder — Pub/Sub is pub/sub, not request/reply.
package worker

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/time/rate"

	"github.com/kubemq-io/kubemq-gcp-pub-sub/burnin/config"
	"github.com/kubemq-io/kubemq-gcp-pub-sub/burnin/metrics"
	"github.com/kubemq-io/kubemq-gcp-pub-sub/burnin/payload"
	"github.com/kubemq-io/kubemq-gcp-pub-sub/burnin/tracker"
	"github.com/kubemq-io/kubemq-gcp-pub-sub/burnin/transport"
)

// Worker is the lifecycle + reporting contract every burn-in worker satisfies.
// The engine drives Start (provision topics/subscriptions + start consumers) →
// wait ConsumerReady → StartProducers (measurement window) → StopProducers →
// drain → StopConsumers. The accessors feed the engine's snapshot/verdict and
// the control-API/report read models.
type Worker interface {
	Name() string
	ChannelName() string
	ChannelIndex() int

	// Start provisions topics/subscriptions and brings up the receiver(s),
	// signalling ConsumerReady once they are pulling. For the interop worker the
	// native Events Store subscribe stream is established here, BEFORE producers
	// begin (subscribe-before-publish, spec S6.3).
	Start(ctx context.Context) error
	StartProducers()
	StopProducers()
	StopConsumers()
	DisconnectConsumers()
	ConsumerReady() <-chan struct{}

	Tracker() *tracker.Tracker
	LatencyAccumulator() *metrics.LatencyAccumulator
	PeakRate() *metrics.PeakRateTracker
	RateWindow() *metrics.SlidingRateWindow

	SentCount() uint64
	ReceivedCount() uint64
	ErrorCount() uint64
	CorruptedCount() uint64
	ReconnectionCount() uint64
	DowntimeSeconds() float64
	DuplicatedCount() uint64
	AckedCount() uint64

	// Pub/Sub-specific fidelity counters (spec S6.6). Workers that do not drive a
	// given dimension inherit the BaseWorker zero getters.
	OrderingViolations() uint64
	FanoutDeliveries() uint64
	FanoutDropped() uint64
	ExactlyOnceConfirmed() uint64
	ExactlyOnceUnreconciled() uint64
	DeadLetterRepublished() uint64
	DeadLetterLost() uint64
	InteropConverged() uint64
	InteropDiverged() uint64

	// New data-plane coverage counters (spec S4/S6.3). Workers that do not drive a
	// given dimension inherit the BaseWorker zero getters.
	NackRedelivered() uint64
	BatchAtomicityViolations() uint64
	FilterMismatch() uint64
	FilterMatchedDelivered() uint64
	SchemaRejectViolations() uint64
	SchemaEnforceConfirmed() uint64
	SeekReplayExpected() uint64
	SeekReplayObserved() uint64
	SeekReplayMismatch() uint64
	InflightCapTailRedelivered() uint64
	InflightCapLost() uint64
	ExactlyOnceInvalidAckConfirmed() uint64
	FlowControlViolations() uint64
	RetryBackoffObserved() uint64
	LeaseExtendConfirmed() uint64
	LeaseExtendRejectConfirmed() uint64
	DeliveryAttemptMax() uint64
	CrossStreamAckConfirmed() uint64
	PeriodicCloseObserved() uint64

	// Per-probe "ran" flags (spec S6.3): true iff the owning worker actually ran
	// the probe to completion. Default-false for workers that do not own the probe.
	FlowControlProbeRan() bool
	InvalidAckProbeRan() bool
	ConfirmationsProbeRan() bool
	CrossStreamAckProbeRan() bool
	LeaseExtendProbeRan() bool
	SchemaEnforceProbeRan() bool

	AdvanceRateWindows()
	ResetAfterWarmup()
}

// BaseWorker holds the shared state and helpers for all Pub/Sub workers.
type BaseWorker struct {
	name         string
	channelName  string
	channelIndex int
	cfg          *config.Config
	workerCfg    *config.WorkerConfig
	logger       *slog.Logger

	psCfg       transport.PubSubConfig
	nativeCfg   transport.NativeConfig
	sizeDistrib *payload.SizeDistribution

	trk        *tracker.Tracker
	latAccum   *metrics.LatencyAccumulator
	peakRate   *metrics.PeakRateTracker
	rateWindow *metrics.SlidingRateWindow

	limiter *rate.Limiter

	producerCtx    context.Context
	producerCancel context.CancelFunc
	consumerCtx    context.Context
	consumerCancel context.CancelFunc
	producerWG     sync.WaitGroup
	consumerWG     sync.WaitGroup
	consumerReady  chan struct{}
	readyOnce      sync.Once

	// disconnectGen is bumped by DisconnectConsumers to signal receiver loops to
	// rebuild their Pub/Sub clients / re-open StreamingPull (forced-churn
	// injection). The default churn-free run never calls this.
	disconnectGen atomic.Uint64

	sent          atomic.Uint64
	received      atomic.Uint64
	errors        atomic.Uint64
	corrupted     atomic.Uint64
	reconnections atomic.Uint64
	downtime      atomic.Uint64 // nanoseconds
	duplicated    atomic.Uint64
	acked         atomic.Uint64

	// Pub/Sub fidelity counters (spec S6.6).
	orderingViolations      atomic.Uint64
	fanoutDeliveries        atomic.Uint64
	fanoutDropped           atomic.Uint64
	exactlyOnceConfirmed    atomic.Uint64
	exactlyOnceUnreconciled atomic.Uint64
	deadLetterRepublished   atomic.Uint64
	deadLetterLost          atomic.Uint64
	interopConverged        atomic.Uint64
	interopDiverged         atomic.Uint64

	// New data-plane coverage counters (spec S4/S6.3).
	nackRedelivered                atomic.Uint64
	batchAtomicityViolations       atomic.Uint64
	filterMismatch                 atomic.Uint64
	filterMatchedDelivered         atomic.Uint64
	schemaRejectViolations         atomic.Uint64
	schemaEnforceConfirmed         atomic.Uint64
	seekReplayExpected             atomic.Uint64
	seekReplayObserved             atomic.Uint64
	seekReplayMismatch             atomic.Uint64
	inflightCapTailRedelivered     atomic.Uint64
	inflightCapLost                atomic.Uint64
	exactlyOnceInvalidAckConfirmed atomic.Uint64
	flowControlViolations          atomic.Uint64
	retryBackoffObserved           atomic.Uint64
	leaseExtendConfirmed           atomic.Uint64
	leaseExtendRejectConfirmed     atomic.Uint64
	deliveryAttemptMax             atomic.Uint64
	crossStreamAckConfirmed        atomic.Uint64
	periodicCloseObserved          atomic.Uint64

	// Per-probe "ran" flags (spec S6.3).
	flowControlProbeRan    atomic.Bool
	invalidAckProbeRan     atomic.Bool
	confirmationsProbeRan  atomic.Bool
	crossStreamAckProbeRan atomic.Bool
	leaseExtendProbeRan    atomic.Bool
	schemaEnforceProbeRan  atomic.Bool
}

// NewBaseWorker constructs the shared worker scaffolding for a channel instance.
func NewBaseWorker(name, channelName string, channelIndex int, cfg *config.Config, logger *slog.Logger) *BaseWorker {
	workerCfg := cfg.GetWorkerConfig(name)

	targetRate := float64(workerCfg.Rate)
	burst := int(targetRate)
	if burst < 1 {
		burst = 1
	}

	var sizeDistrib *payload.SizeDistribution
	if cfg.Message.SizeMode == "distribution" {
		sizeDistrib, _ = payload.ParseDistribution(cfg.Message.SizeDistribution)
	}

	return &BaseWorker{
		name:         name,
		channelName:  channelName,
		channelIndex: channelIndex,
		cfg:          cfg,
		workerCfg:    workerCfg,
		logger:       logger.With("worker", name, "channel", channelName),

		psCfg: transport.PubSubConfig{
			ProjectID:    cfg.GCP.ProjectID,
			EmulatorHost: transport.EmulatorHost(cfg.GCP.EmulatorHost, cfg.Broker.Address),
		},
		nativeCfg: transport.NativeConfig{
			Address:  cfg.Broker.GRPCAddress,
			ClientID: sanitizeClientID(cfg.Broker.ClientIDPrefix + "-" + channelName),
		},
		sizeDistrib: sizeDistrib,

		trk:        tracker.New(cfg.Message.ReorderWindow),
		latAccum:   metrics.NewLatencyAccumulator(),
		peakRate:   metrics.NewPeakRateTracker(),
		rateWindow: metrics.NewSlidingRateWindow(),

		limiter:       rate.NewLimiter(rate.Limit(targetRate), burst),
		consumerReady: make(chan struct{}),
	}
}

// --- Accessors ---

func (b *BaseWorker) Name() string                                    { return b.name }
func (b *BaseWorker) ChannelName() string                             { return b.channelName }
func (b *BaseWorker) ChannelIndex() int                               { return b.channelIndex }
func (b *BaseWorker) ConsumerReady() <-chan struct{}                  { return b.consumerReady }
func (b *BaseWorker) Tracker() *tracker.Tracker                       { return b.trk }
func (b *BaseWorker) LatencyAccumulator() *metrics.LatencyAccumulator { return b.latAccum }
func (b *BaseWorker) PeakRate() *metrics.PeakRateTracker              { return b.peakRate }
func (b *BaseWorker) RateWindow() *metrics.SlidingRateWindow          { return b.rateWindow }

func (b *BaseWorker) SentCount() uint64         { return b.sent.Load() }
func (b *BaseWorker) ReceivedCount() uint64     { return b.received.Load() }
func (b *BaseWorker) ErrorCount() uint64        { return b.errors.Load() }
func (b *BaseWorker) CorruptedCount() uint64    { return b.corrupted.Load() }
func (b *BaseWorker) ReconnectionCount() uint64 { return b.reconnections.Load() }
func (b *BaseWorker) DuplicatedCount() uint64   { return b.duplicated.Load() }
func (b *BaseWorker) AckedCount() uint64        { return b.acked.Load() }

func (b *BaseWorker) OrderingViolations() uint64      { return b.orderingViolations.Load() }
func (b *BaseWorker) FanoutDeliveries() uint64        { return b.fanoutDeliveries.Load() }
func (b *BaseWorker) FanoutDropped() uint64           { return b.fanoutDropped.Load() }
func (b *BaseWorker) ExactlyOnceConfirmed() uint64    { return b.exactlyOnceConfirmed.Load() }
func (b *BaseWorker) ExactlyOnceUnreconciled() uint64 { return b.exactlyOnceUnreconciled.Load() }
func (b *BaseWorker) DeadLetterRepublished() uint64   { return b.deadLetterRepublished.Load() }
func (b *BaseWorker) DeadLetterLost() uint64          { return b.deadLetterLost.Load() }
func (b *BaseWorker) InteropConverged() uint64        { return b.interopConverged.Load() }
func (b *BaseWorker) InteropDiverged() uint64         { return b.interopDiverged.Load() }

func (b *BaseWorker) NackRedelivered() uint64            { return b.nackRedelivered.Load() }
func (b *BaseWorker) BatchAtomicityViolations() uint64   { return b.batchAtomicityViolations.Load() }
func (b *BaseWorker) FilterMismatch() uint64             { return b.filterMismatch.Load() }
func (b *BaseWorker) FilterMatchedDelivered() uint64     { return b.filterMatchedDelivered.Load() }
func (b *BaseWorker) SchemaRejectViolations() uint64     { return b.schemaRejectViolations.Load() }
func (b *BaseWorker) SchemaEnforceConfirmed() uint64     { return b.schemaEnforceConfirmed.Load() }
func (b *BaseWorker) SeekReplayExpected() uint64         { return b.seekReplayExpected.Load() }
func (b *BaseWorker) SeekReplayObserved() uint64         { return b.seekReplayObserved.Load() }
func (b *BaseWorker) SeekReplayMismatch() uint64         { return b.seekReplayMismatch.Load() }
func (b *BaseWorker) InflightCapTailRedelivered() uint64 { return b.inflightCapTailRedelivered.Load() }
func (b *BaseWorker) InflightCapLost() uint64            { return b.inflightCapLost.Load() }
func (b *BaseWorker) ExactlyOnceInvalidAckConfirmed() uint64 {
	return b.exactlyOnceInvalidAckConfirmed.Load()
}
func (b *BaseWorker) FlowControlViolations() uint64      { return b.flowControlViolations.Load() }
func (b *BaseWorker) RetryBackoffObserved() uint64       { return b.retryBackoffObserved.Load() }
func (b *BaseWorker) LeaseExtendConfirmed() uint64       { return b.leaseExtendConfirmed.Load() }
func (b *BaseWorker) LeaseExtendRejectConfirmed() uint64 { return b.leaseExtendRejectConfirmed.Load() }
func (b *BaseWorker) DeliveryAttemptMax() uint64         { return b.deliveryAttemptMax.Load() }
func (b *BaseWorker) CrossStreamAckConfirmed() uint64    { return b.crossStreamAckConfirmed.Load() }
func (b *BaseWorker) PeriodicCloseObserved() uint64      { return b.periodicCloseObserved.Load() }

func (b *BaseWorker) FlowControlProbeRan() bool    { return b.flowControlProbeRan.Load() }
func (b *BaseWorker) InvalidAckProbeRan() bool     { return b.invalidAckProbeRan.Load() }
func (b *BaseWorker) ConfirmationsProbeRan() bool  { return b.confirmationsProbeRan.Load() }
func (b *BaseWorker) CrossStreamAckProbeRan() bool { return b.crossStreamAckProbeRan.Load() }
func (b *BaseWorker) LeaseExtendProbeRan() bool    { return b.leaseExtendProbeRan.Load() }
func (b *BaseWorker) SchemaEnforceProbeRan() bool  { return b.schemaEnforceProbeRan.Load() }

func (b *BaseWorker) DowntimeSeconds() float64 {
	return float64(b.downtime.Load()) / float64(time.Second)
}

// --- Counter helpers (used by concrete workers) ---

func (b *BaseWorker) recordSent(bytes int, producerID string) {
	b.sent.Add(1)
	b.rateWindow.Record()
	b.peakRate.Record()
	metrics.IncSent(b.name, producerID)
	metrics.RecordBytesSent(b.name, bytes)
}

func (b *BaseWorker) recordReceived(bytes int, consumerID string) {
	b.received.Add(1)
	metrics.IncReceived(b.name, consumerID)
	metrics.RecordBytesReceived(b.name, bytes)
}

func (b *BaseWorker) recordAcked() {
	b.acked.Add(1)
	metrics.IncAck(b.name)
}

func (b *BaseWorker) recordError(errType string) {
	b.errors.Add(1)
	metrics.IncError(b.name, errType)
}

func (b *BaseWorker) recordCorrupted() {
	b.corrupted.Add(1)
	metrics.IncCorrupted(b.name)
}

func (b *BaseWorker) recordDuplicated() {
	b.duplicated.Add(1)
	metrics.IncDuplicated(b.name)
}

func (b *BaseWorker) recordReconnection() {
	b.reconnections.Add(1)
	metrics.IncReconnection(b.name)
}

func (b *BaseWorker) recordLatency(d time.Duration) {
	b.latAccum.Record(d)
	metrics.ObserveLatency(b.name, d)
}

// --- Pub/Sub fidelity counter helpers ---

func (b *BaseWorker) recordOrderingViolation() {
	b.orderingViolations.Add(1)
	metrics.IncOrderingViolation(b.name)
}

func (b *BaseWorker) recordFanoutDelivery() {
	b.fanoutDeliveries.Add(1)
	metrics.IncFanoutDelivery(b.name)
}

func (b *BaseWorker) recordFanoutDropped(n uint64) {
	b.fanoutDropped.Add(n)
	metrics.AddFanoutDropped(b.name, n)
}

// recordExactlyOnceAck folds one exactly-once ack confirmation into the counters:
// confirmed=true when the AckResult resolved AcknowledgeStatusSuccess; otherwise
// the ack id ended invalid/temporary-failed and is recorded as unreconciled (the
// gate is zero unreconciled).
func (b *BaseWorker) recordExactlyOnceAck(confirmed bool) {
	if confirmed {
		b.exactlyOnceConfirmed.Add(1)
		metrics.IncExactlyOnceConfirmed(b.name)
		return
	}
	b.exactlyOnceUnreconciled.Add(1)
	metrics.IncExactlyOnceUnreconciled(b.name)
}

// reconcileExactlyOnce removes a previously-recorded unreconciled ack id once a
// retry succeeds (the surviving unreconciled count is the genuine gate failure).
func (b *BaseWorker) reconcileExactlyOnce() {
	for {
		cur := b.exactlyOnceUnreconciled.Load()
		if cur == 0 {
			return
		}
		if b.exactlyOnceUnreconciled.CompareAndSwap(cur, cur-1) {
			return
		}
	}
}

func (b *BaseWorker) recordDeadLetterRepublished() {
	b.deadLetterRepublished.Add(1)
	metrics.IncDeadLetterRepublished(b.name)
}

func (b *BaseWorker) recordDeadLetterLost(n uint64) {
	b.deadLetterLost.Add(n)
	metrics.AddDeadLetterLost(b.name, n)
}

func (b *BaseWorker) recordInteropConverged() {
	b.interopConverged.Add(1)
	metrics.IncInteropConverged(b.name)
}

func (b *BaseWorker) recordInteropDiverged(n uint64) {
	b.interopDiverged.Add(n)
	metrics.AddInteropDiverged(b.name, n)
}

// --- New data-plane coverage counter helpers (spec S4/S6.3) ---

func (b *BaseWorker) recordNackRedelivered() {
	b.nackRedelivered.Add(1)
	metrics.IncNackRedelivered(b.name)
}

func (b *BaseWorker) recordBatchAtomicityViolation() {
	b.batchAtomicityViolations.Add(1)
	metrics.IncBatchAtomicityViolation(b.name)
}

func (b *BaseWorker) recordFilterMismatch() {
	b.filterMismatch.Add(1)
	metrics.IncFilterMismatch(b.name)
}

func (b *BaseWorker) recordFilterMatchedDelivered() {
	b.filterMatchedDelivered.Add(1)
	metrics.IncFilterMatchedDelivered(b.name)
}

func (b *BaseWorker) recordSchemaRejectViolation() {
	b.schemaRejectViolations.Add(1)
	metrics.IncSchemaRejectViolation(b.name)
}

func (b *BaseWorker) recordSchemaEnforceConfirmed() {
	b.schemaEnforceConfirmed.Add(1)
	metrics.IncSchemaEnforceConfirmed(b.name)
}

func (b *BaseWorker) recordSeekReplayExpected(n uint64) {
	b.seekReplayExpected.Add(n)
	metrics.AddSeekReplayExpected(b.name, n)
}

func (b *BaseWorker) recordSeekReplayObserved(n uint64) {
	b.seekReplayObserved.Add(n)
	metrics.AddSeekReplayObserved(b.name, n)
}

func (b *BaseWorker) recordSeekReplayMismatch() {
	b.seekReplayMismatch.Add(1)
	metrics.IncSeekReplayMismatch(b.name)
}

func (b *BaseWorker) recordInflightCapTailRedelivered() {
	b.inflightCapTailRedelivered.Add(1)
	metrics.IncInflightCapTailRedelivered(b.name)
}

func (b *BaseWorker) recordInflightCapLost(n uint64) {
	b.inflightCapLost.Add(n)
	metrics.AddInflightCapLost(b.name, n)
}

func (b *BaseWorker) recordExactlyOnceInvalidAckConfirmed() {
	b.exactlyOnceInvalidAckConfirmed.Add(1)
	metrics.IncExactlyOnceInvalidAckConfirmed(b.name)
}

func (b *BaseWorker) recordFlowControlViolation() {
	b.flowControlViolations.Add(1)
	metrics.IncFlowControlViolation(b.name)
}

func (b *BaseWorker) recordRetryBackoffObserved() {
	b.retryBackoffObserved.Add(1)
	metrics.IncRetryBackoffObserved(b.name)
}

func (b *BaseWorker) recordLeaseExtendConfirmed() {
	b.leaseExtendConfirmed.Add(1)
	metrics.IncLeaseExtendConfirmed(b.name)
}

func (b *BaseWorker) recordLeaseExtendRejectConfirmed() {
	b.leaseExtendRejectConfirmed.Add(1)
	metrics.IncLeaseExtendRejectConfirmed(b.name)
}

// setDeliveryAttemptMax raises the delivery-attempt-max gauge to v if v is
// greater than the current value (monotonic max).
func (b *BaseWorker) setDeliveryAttemptMax(v uint64) {
	for {
		cur := b.deliveryAttemptMax.Load()
		if v <= cur {
			return
		}
		if b.deliveryAttemptMax.CompareAndSwap(cur, v) {
			metrics.SetDeliveryAttemptMax(b.name, float64(v))
			return
		}
	}
}

func (b *BaseWorker) recordCrossStreamAckConfirmed() {
	b.crossStreamAckConfirmed.Add(1)
	metrics.IncCrossStreamAckConfirmed(b.name)
}

func (b *BaseWorker) recordPeriodicCloseObserved() {
	b.periodicCloseObserved.Add(1)
	metrics.IncPeriodicCloseObserved(b.name)
}

// setOutstanding samples the peak un-acked count into the flow-control gauge.
func (b *BaseWorker) setOutstanding(n float64) {
	metrics.SetOutstanding(b.name, n)
}

// --- Per-probe "ran" flag setters (spec S6.3): set ONLY when the probe ran ---

func (b *BaseWorker) markFlowControlProbeRan()    { b.flowControlProbeRan.Store(true) }
func (b *BaseWorker) markInvalidAckProbeRan()     { b.invalidAckProbeRan.Store(true) }
func (b *BaseWorker) markConfirmationsProbeRan()  { b.confirmationsProbeRan.Store(true) }
func (b *BaseWorker) markCrossStreamAckProbeRan() { b.crossStreamAckProbeRan.Store(true) }
func (b *BaseWorker) markLeaseExtendProbeRan()    { b.leaseExtendProbeRan.Store(true) }
func (b *BaseWorker) markSchemaEnforceProbeRan()  { b.schemaEnforceProbeRan.Store(true) }

// recordTracked feeds a received (producer-id, seq) into the tracker and records
// duplicates / out-of-order in metrics.
func (b *BaseWorker) recordTracked(producerID string, seq uint64) (isDuplicate bool) {
	dup, oo := b.trk.Record(producerID, seq)
	if dup {
		b.recordDuplicated()
	}
	if oo {
		metrics.IncOutOfOrder(b.name)
	}
	return dup
}

// --- Rate control ---

func (b *BaseWorker) waitForRate(ctx context.Context) error {
	return b.limiter.Wait(ctx)
}

// --- Message size ---

func (b *BaseWorker) selectMessageSize() int {
	if b.cfg.Message.SizeMode == "distribution" && b.sizeDistrib != nil {
		return b.sizeDistrib.SelectSize()
	}
	return b.cfg.Message.SizeBytes
}

// --- Downtime accounting ---

func (b *BaseWorker) addDowntime(d time.Duration) {
	b.downtime.Add(uint64(d))
	metrics.AddDowntime(b.name, d.Seconds())
}

// --- Rate windows ---

func (b *BaseWorker) AdvanceRateWindows() {
	b.rateWindow.Advance()
	b.peakRate.Advance()
}

// --- Warmup reset ---

func (b *BaseWorker) ResetAfterWarmup() {
	b.trk.Reset()
	b.latAccum.Reset()
	b.peakRate = metrics.NewPeakRateTracker()
	b.rateWindow.Reset()

	b.sent.Store(0)
	b.received.Store(0)
	b.errors.Store(0)
	b.corrupted.Store(0)
	b.reconnections.Store(0)
	b.downtime.Store(0)
	b.duplicated.Store(0)
	b.acked.Store(0)

	b.orderingViolations.Store(0)
	b.fanoutDeliveries.Store(0)
	b.fanoutDropped.Store(0)
	b.exactlyOnceConfirmed.Store(0)
	b.exactlyOnceUnreconciled.Store(0)
	b.deadLetterRepublished.Store(0)
	b.deadLetterLost.Store(0)
	b.interopConverged.Store(0)
	b.interopDiverged.Store(0)

	// New data-plane coverage counters.
	b.nackRedelivered.Store(0)
	b.batchAtomicityViolations.Store(0)
	b.filterMismatch.Store(0)
	b.filterMatchedDelivered.Store(0)
	b.schemaRejectViolations.Store(0)
	b.schemaEnforceConfirmed.Store(0)
	b.seekReplayExpected.Store(0)
	b.seekReplayObserved.Store(0)
	b.seekReplayMismatch.Store(0)
	b.inflightCapTailRedelivered.Store(0)
	b.inflightCapLost.Store(0)
	b.exactlyOnceInvalidAckConfirmed.Store(0)
	b.flowControlViolations.Store(0)
	b.retryBackoffObserved.Store(0)
	b.leaseExtendConfirmed.Store(0)
	b.leaseExtendRejectConfirmed.Store(0)
	b.deliveryAttemptMax.Store(0)
	b.crossStreamAckConfirmed.Store(0)
	b.periodicCloseObserved.Store(0)

	// Reset probe-ran flags so warmup-phase probe execution does not leak into
	// the measurement window (spec S6.3).
	b.flowControlProbeRan.Store(false)
	b.invalidAckProbeRan.Store(false)
	b.confirmationsProbeRan.Store(false)
	b.crossStreamAckProbeRan.Store(false)
	b.leaseExtendProbeRan.Store(false)
	b.schemaEnforceProbeRan.Store(false)
}

// --- Ready signalling ---

func (b *BaseWorker) signalReady() {
	b.readyOnce.Do(func() { close(b.consumerReady) })
}

// --- Forced disconnect ---

// DisconnectConsumers bumps the disconnect generation so receiver loops rebuild
// their Pub/Sub / native clients (re-open StreamingPull, re-acquire leases after
// ack-deadline redelivery). The default churn-free run never calls this.
func (b *BaseWorker) DisconnectConsumers() {
	b.disconnectGen.Add(1)
}

// disconnectGeneration returns the current churn generation; receiver loops
// compare against a captured value to detect a forced disconnect.
func (b *BaseWorker) disconnectGeneration() uint64 {
	return b.disconnectGen.Load()
}

// --- Lifecycle helpers shared by concrete workers ---

// stopWaitTimeout bounds how long Stop{Producers,Consumers} waits for the worker
// goroutines to exit. Under heavy connector load the high-level pubsub
// sub.Receive can be slow to return after its context is cancelled (it flushes
// outstanding acks first). The measurement snapshot is already taken BEFORE
// consumers are stopped (engine.shutdownWorkers), so teardown must never block
// the run from completing and writing its verdict — cap the wait and let any
// straggler goroutine exit on process teardown.
const stopWaitTimeout = 15 * time.Second

func (b *BaseWorker) StopProducers() {
	if b.producerCancel != nil {
		b.producerCancel()
	}
	if !waitWGTimeout(&b.producerWG, stopWaitTimeout) {
		b.logger.Warn("producer stop timed out", "timeout", stopWaitTimeout)
	}
}

func (b *BaseWorker) StopConsumers() {
	if b.consumerCancel != nil {
		b.consumerCancel()
	}
	if !waitWGTimeout(&b.consumerWG, stopWaitTimeout) {
		b.logger.Warn("consumer stop timed out; receive loop will exit on teardown", "timeout", stopWaitTimeout)
	}
}

// waitWGTimeout waits for wg up to d, returning false if it timed out.
func waitWGTimeout(wg *sync.WaitGroup, d time.Duration) bool {
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
		return true
	case <-time.After(d):
		return false
	}
}

// sanitizeClientID makes s a valid KubeMQ client id. The broker requires the
// client id to be alphanumeric plus '_' and '-'. The interop worker derives its
// native client id from the mapped Pub/Sub channel (pubsub.{topic}), which
// contains '.', so any rune outside [A-Za-z0-9_-] is replaced with '-'.
func sanitizeClientID(s string) string {
	b := []byte(s)
	for i := 0; i < len(b); i++ {
		c := b[i]
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9', c == '_', c == '-':
			// valid
		default:
			b[i] = '-'
		}
	}
	return string(b)
}
