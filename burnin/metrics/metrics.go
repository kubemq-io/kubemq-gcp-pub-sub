// Package metrics defines the Prometheus burnin_* metric surface (recast for
// Google Cloud Pub/Sub: worker-labelled, plus the Pub/Sub-specific
// gcp_ordering_violations / gcp_fanout_deliveries / gcp_fanout_dropped /
// gcp_exactly_once_* / gcp_dead_letter_* / gcp_interop_* counters per spec S6.6)
// and the in-memory latency / rate accumulators used to compute verdict
// percentiles. The worker package (owned separately) calls these helpers.
package metrics

import (
	"sync"
	"time"

	"github.com/HdrHistogram/hdrhistogram-go"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

const sdkLabel = "gcp"

var (
	latencyBuckets = []float64{0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10}

	// ── Counters ──

	MessagesSentTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "burnin_messages_sent_total",
		Help: "Total messages sent (Pub/Sub Publish / native send)",
	}, []string{"sdk", "worker", "producerid"})

	MessagesReceivedTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "burnin_messages_received_total",
		Help: "Total messages received by consumers (Pull / StreamingPull / native receive)",
	}, []string{"sdk", "worker", "consumer_id"})

	MessagesLostTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "burnin_messages_lost_total",
		Help: "Confirmed lost messages (reconciled against pendingLost)",
	}, []string{"sdk", "worker"})

	MessagesDuplicatedTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "burnin_messages_duplicated_total",
		Help: "Messages detected as duplicated",
	}, []string{"sdk", "worker"})

	MessagesCorruptedTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "burnin_messages_corrupted_total",
		Help: "Messages with CRC32 hash mismatch",
	}, []string{"sdk", "worker"})

	MessagesOutOfOrderTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "burnin_messages_out_of_order_total",
		Help: "Messages received out of sequence order",
	}, []string{"sdk", "worker"})

	ErrorsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "burnin_errors_total",
		Help: "Errors by type",
	}, []string{"sdk", "worker", "error_type"})

	ReconnectionsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "burnin_reconnections_total",
		Help: "Number of reconnection / client-recreate / StreamingPull re-establish events",
	}, []string{"sdk", "worker"})

	BytesSentTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "burnin_bytes_sent_total",
		Help: "Total bytes sent",
	}, []string{"sdk", "worker"})

	BytesReceivedTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "burnin_bytes_received_total",
		Help: "Total bytes received",
	}, []string{"sdk", "worker"})

	AcksTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "burnin_pubsub_acks_total",
		Help: "Pub/Sub Acknowledge operations (round-trip completion)",
	}, []string{"sdk", "worker"})

	DowntimeSecondsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "burnin_downtime_seconds_total",
		Help: "Cumulative time spent reconnecting",
	}, []string{"sdk", "worker"})

	ForcedDisconnectsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "burnin_forced_disconnects_total",
		Help: "Number of forced disconnect events",
	}, []string{"sdk"})

	// ── Pub/Sub-specific counters (spec S6.6) ──

	OrderingViolationsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "burnin_gcp_ordering_violations_total",
		Help: "Per-ordering-key delivery violations (out-of-order within an ordering key)",
	}, []string{"sdk", "worker"})

	FanoutDeliveriesTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "burnin_gcp_fanout_deliveries_total",
		Help: "Fan-out deliveries received across all target subscriptions",
	}, []string{"sdk", "worker"})

	FanoutDroppedTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "burnin_gcp_fanout_dropped_total",
		Help: "Fan-out deliveries that never arrived at a confirmed subscription",
	}, []string{"sdk", "worker"})

	ExactlyOnceConfirmedTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "burnin_gcp_exactly_once_confirmed_total",
		Help: "Exactly-once ack confirmations that succeeded (AckResponse OK)",
	}, []string{"sdk", "worker"})

	ExactlyOnceUnreconciledTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "burnin_gcp_exactly_once_unreconciled_total",
		Help: "Exactly-once ack ids that ended invalid/temporary-failed and never reconciled",
	}, []string{"sdk", "worker"})

	DeadLetterRepublishedTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "burnin_gcp_dead_letter_republished_total",
		Help: "Messages republished to the dead-letter topic after max delivery attempts",
	}, []string{"sdk", "worker"})

	DeadLetterLostTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "burnin_gcp_dead_letter_lost_total",
		Help: "Dead-letter messages that were neither acked on the source nor republished to the DLQ",
	}, []string{"sdk", "worker"})

	InteropConvergedTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "burnin_gcp_interop_converged_total",
		Help: "Published messages confirmed consumed on pubsub.{topic} by the native kubemq-go side",
	}, []string{"sdk", "worker"})

	InteropDivergedTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "burnin_gcp_interop_diverged_total",
		Help: "Published messages that never converged at the native consumer (interop gate failure)",
	}, []string{"sdk", "worker"})

	// ── New data-plane coverage counters (spec S4/S6.3) ──

	NackRedeliveredTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "burnin_gcp_nack_redelivered_total",
		Help: "Confirmed redeliveries after an explicit nack (ModifyAckDeadline(0))",
	}, []string{"sdk", "worker"})

	BatchAtomicityViolationsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "burnin_gcp_batch_atomicity_violations_total",
		Help: "Batch publish partial enqueue / accepted-invalid-batch violations (expected 0)",
	}, []string{"sdk", "worker"})

	FilterMismatchTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "burnin_gcp_filter_mismatch_total",
		Help: "Filtered-out (non-matching) messages that were nonetheless delivered (expected 0)",
	}, []string{"sdk", "worker"})

	FilterMatchedDeliveredTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "burnin_gcp_filter_matched_delivered_total",
		Help: "Matching messages delivered through the attribute filter",
	}, []string{"sdk", "worker"})

	SchemaRejectViolationsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "burnin_gcp_schema_reject_violations_total",
		Help: "Non-conforming publishes the connector accepted under schema enforcement (expected 0)",
	}, []string{"sdk", "worker"})

	SchemaEnforceConfirmedTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "burnin_gcp_schema_enforce_confirmed_total",
		Help: "Non-conforming publishes the connector correctly rejected (INVALID_ARGUMENT)",
	}, []string{"sdk", "worker"})

	SeekReplayExpectedTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "burnin_gcp_seek_replay_expected_total",
		Help: "Messages expected to replay after a seek/snapshot rewind",
	}, []string{"sdk", "worker"})

	SeekReplayObservedTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "burnin_gcp_seek_replay_observed_total",
		Help: "Messages actually replayed after a seek/snapshot rewind",
	}, []string{"sdk", "worker"})

	SeekReplayMismatchTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "burnin_gcp_seek_replay_mismatch_total",
		Help: "Seek/snapshot replay-count mismatches beyond tolerance (expected 0)",
	}, []string{"sdk", "worker"})

	InflightCapTailRedeliveredTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "burnin_gcp_inflight_cap_tail_redelivered_total",
		Help: "Messages redelivered from the over-cap (unleased) tail after backlog release",
	}, []string{"sdk", "worker"})

	InflightCapLostTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "burnin_gcp_inflight_cap_lost_total",
		Help: "In-flight-cap messages that never redelivered (lost tail, expected 0)",
	}, []string{"sdk", "worker"})

	ExactlyOnceInvalidAckConfirmedTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "burnin_gcp_exactly_once_invalid_ack_confirmed_total",
		Help: "Exactly-once invalid-ack probes that observed FAILED_PRECONDITION + INVALID_ACK_ID",
	}, []string{"sdk", "worker"})

	FlowControlViolationsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "burnin_gcp_flow_control_violations_total",
		Help: "Server-side flow-control clamp violations: un-acked delivered above ceiling (expected 0)",
	}, []string{"sdk", "worker"})

	RetryBackoffObservedTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "burnin_gcp_retry_backoff_observed_total",
		Help: "Times the exponential retry-backoff growing-gap pattern was observed (bounded-wait NOTE)",
	}, []string{"sdk", "worker"})

	LeaseExtendConfirmedTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "burnin_gcp_lease_extend_confirmed_total",
		Help: "Lease-extend probes that confirmed non-redelivery within the extended window",
	}, []string{"sdk", "worker"})

	LeaseExtendRejectConfirmedTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "burnin_gcp_lease_extend_reject_confirmed_total",
		Help: "Lease-extend probes that confirmed an out-of-range deadline was rejected (INVALID_ARGUMENT)",
	}, []string{"sdk", "worker"})

	CrossStreamAckConfirmedTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "burnin_gcp_cross_stream_ack_confirmed_total",
		Help: "Cross-stream ack probes that confirmed a subscription-owned lease ack succeeded",
	}, []string{"sdk", "worker"})

	PeriodicCloseObservedTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "burnin_gcp_periodic_close_observed_total",
		Help: "Times the connector's periodic StreamingPull server-close was observed (bounded-wait NOTE)",
	}, []string{"sdk", "worker"})

	// ── Histograms ──

	MessageLatencySeconds = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "burnin_message_latency_seconds",
		Help:    "End-to-end message latency (publish → receive)",
		Buckets: latencyBuckets,
	}, []string{"sdk", "worker"})

	SendDurationSeconds = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "burnin_send_duration_seconds",
		Help:    "Pub/Sub Publish / native send round-trip time",
		Buckets: latencyBuckets,
	}, []string{"sdk", "worker"})

	// ── Gauges ──

	ActiveConnections = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "burnin_active_connections",
		Help: "Currently active Pub/Sub-SDK / native clients",
	}, []string{"sdk", "worker"})

	UptimeSeconds = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "burnin_uptime_seconds",
		Help: "Burn-in app uptime",
	}, []string{"sdk"})

	TargetRate = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "burnin_target_rate",
		Help: "Configured target rate (msgs/sec)",
	}, []string{"sdk", "worker"})

	ActualRate = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "burnin_actual_rate",
		Help: "Current achieved rate (msgs/sec)",
	}, []string{"sdk", "worker"})

	ConsumerLag = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "burnin_consumer_lag_messages",
		Help: "Sent minus received (producer-consumer lag)",
	}, []string{"sdk", "worker"})

	WarmupActive = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "burnin_warmup_active",
		Help: "1 during warmup, 0 after",
	}, []string{"sdk"})

	OutstandingMessages = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "burnin_gcp_outstanding_messages",
		Help: "Peak un-acked messages delivered by the flow-control probe before any ack",
	}, []string{"sdk", "worker"})

	DeliveryAttemptMax = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "burnin_gcp_delivery_attempt_max",
		Help: "Max ReceivedMessage.delivery_attempt observed under a dead-letter policy",
	}, []string{"sdk", "worker"})
)

// SDK returns the metric SDK label value ("gcp").
func SDK() string { return sdkLabel }

// InitMetrics pre-initializes all metrics to 0 with well-known label values so
// dashboards don't fire absent() alerts.
func InitMetrics(workers []string) {
	errorTypes := []string{
		"send_failure", "receive_failure", "ack_failure", "create_failure",
		"subscribe_failure", "publish_failure", "connect_failure", "interop_failure",
	}
	for _, w := range workers {
		MessagesSentTotal.WithLabelValues(sdkLabel, w, "p-"+w+"-000").Add(0)
		MessagesReceivedTotal.WithLabelValues(sdkLabel, w, "c-"+w+"-000").Add(0)
		MessagesLostTotal.WithLabelValues(sdkLabel, w).Add(0)
		MessagesDuplicatedTotal.WithLabelValues(sdkLabel, w).Add(0)
		MessagesCorruptedTotal.WithLabelValues(sdkLabel, w).Add(0)
		MessagesOutOfOrderTotal.WithLabelValues(sdkLabel, w).Add(0)
		ReconnectionsTotal.WithLabelValues(sdkLabel, w).Add(0)
		BytesSentTotal.WithLabelValues(sdkLabel, w).Add(0)
		BytesReceivedTotal.WithLabelValues(sdkLabel, w).Add(0)
		AcksTotal.WithLabelValues(sdkLabel, w).Add(0)
		DowntimeSecondsTotal.WithLabelValues(sdkLabel, w).Add(0)
		OrderingViolationsTotal.WithLabelValues(sdkLabel, w).Add(0)
		FanoutDeliveriesTotal.WithLabelValues(sdkLabel, w).Add(0)
		FanoutDroppedTotal.WithLabelValues(sdkLabel, w).Add(0)
		ExactlyOnceConfirmedTotal.WithLabelValues(sdkLabel, w).Add(0)
		ExactlyOnceUnreconciledTotal.WithLabelValues(sdkLabel, w).Add(0)
		DeadLetterRepublishedTotal.WithLabelValues(sdkLabel, w).Add(0)
		DeadLetterLostTotal.WithLabelValues(sdkLabel, w).Add(0)
		InteropConvergedTotal.WithLabelValues(sdkLabel, w).Add(0)
		InteropDivergedTotal.WithLabelValues(sdkLabel, w).Add(0)
		NackRedeliveredTotal.WithLabelValues(sdkLabel, w).Add(0)
		BatchAtomicityViolationsTotal.WithLabelValues(sdkLabel, w).Add(0)
		FilterMismatchTotal.WithLabelValues(sdkLabel, w).Add(0)
		FilterMatchedDeliveredTotal.WithLabelValues(sdkLabel, w).Add(0)
		SchemaRejectViolationsTotal.WithLabelValues(sdkLabel, w).Add(0)
		SchemaEnforceConfirmedTotal.WithLabelValues(sdkLabel, w).Add(0)
		SeekReplayExpectedTotal.WithLabelValues(sdkLabel, w).Add(0)
		SeekReplayObservedTotal.WithLabelValues(sdkLabel, w).Add(0)
		SeekReplayMismatchTotal.WithLabelValues(sdkLabel, w).Add(0)
		InflightCapTailRedeliveredTotal.WithLabelValues(sdkLabel, w).Add(0)
		InflightCapLostTotal.WithLabelValues(sdkLabel, w).Add(0)
		ExactlyOnceInvalidAckConfirmedTotal.WithLabelValues(sdkLabel, w).Add(0)
		FlowControlViolationsTotal.WithLabelValues(sdkLabel, w).Add(0)
		RetryBackoffObservedTotal.WithLabelValues(sdkLabel, w).Add(0)
		LeaseExtendConfirmedTotal.WithLabelValues(sdkLabel, w).Add(0)
		LeaseExtendRejectConfirmedTotal.WithLabelValues(sdkLabel, w).Add(0)
		CrossStreamAckConfirmedTotal.WithLabelValues(sdkLabel, w).Add(0)
		PeriodicCloseObservedTotal.WithLabelValues(sdkLabel, w).Add(0)
		OutstandingMessages.WithLabelValues(sdkLabel, w).Set(0)
		DeliveryAttemptMax.WithLabelValues(sdkLabel, w).Set(0)
		for _, et := range errorTypes {
			ErrorsTotal.WithLabelValues(sdkLabel, w, et).Add(0)
		}
		ActiveConnections.WithLabelValues(sdkLabel, w).Set(0)
		TargetRate.WithLabelValues(sdkLabel, w).Set(0)
		ActualRate.WithLabelValues(sdkLabel, w).Set(0)
		ConsumerLag.WithLabelValues(sdkLabel, w).Set(0)
	}
	ForcedDisconnectsTotal.WithLabelValues(sdkLabel).Add(0)
	UptimeSeconds.WithLabelValues(sdkLabel).Set(0)
	WarmupActive.WithLabelValues(sdkLabel).Set(0)
}

// ── Counter helpers ──

func IncSent(worker, producerID string) {
	MessagesSentTotal.WithLabelValues(sdkLabel, worker, producerID).Inc()
}

func IncReceived(worker, consumerID string) {
	MessagesReceivedTotal.WithLabelValues(sdkLabel, worker, consumerID).Inc()
}

func AddLost(worker string, delta uint64) {
	MessagesLostTotal.WithLabelValues(sdkLabel, worker).Add(float64(delta))
}
func IncDuplicated(worker string) { MessagesDuplicatedTotal.WithLabelValues(sdkLabel, worker).Inc() }
func IncCorrupted(worker string)  { MessagesCorruptedTotal.WithLabelValues(sdkLabel, worker).Inc() }
func IncOutOfOrder(worker string) { MessagesOutOfOrderTotal.WithLabelValues(sdkLabel, worker).Inc() }
func IncError(worker, errorType string) {
	ErrorsTotal.WithLabelValues(sdkLabel, worker, errorType).Inc()
}
func IncReconnection(worker string) { ReconnectionsTotal.WithLabelValues(sdkLabel, worker).Inc() }
func IncAck(worker string)          { AcksTotal.WithLabelValues(sdkLabel, worker).Inc() }

func IncOrderingViolation(worker string) {
	OrderingViolationsTotal.WithLabelValues(sdkLabel, worker).Inc()
}
func IncFanoutDelivery(worker string) { FanoutDeliveriesTotal.WithLabelValues(sdkLabel, worker).Inc() }
func AddFanoutDropped(worker string, delta uint64) {
	FanoutDroppedTotal.WithLabelValues(sdkLabel, worker).Add(float64(delta))
}
func IncExactlyOnceConfirmed(worker string) {
	ExactlyOnceConfirmedTotal.WithLabelValues(sdkLabel, worker).Inc()
}
func IncExactlyOnceUnreconciled(worker string) {
	ExactlyOnceUnreconciledTotal.WithLabelValues(sdkLabel, worker).Inc()
}

func IncDeadLetterRepublished(worker string) {
	DeadLetterRepublishedTotal.WithLabelValues(sdkLabel, worker).Inc()
}
func AddDeadLetterLost(worker string, delta uint64) {
	DeadLetterLostTotal.WithLabelValues(sdkLabel, worker).Add(float64(delta))
}

func IncInteropConverged(worker string) {
	InteropConvergedTotal.WithLabelValues(sdkLabel, worker).Inc()
}
func AddInteropDiverged(worker string, delta uint64) {
	InteropDivergedTotal.WithLabelValues(sdkLabel, worker).Add(float64(delta))
}

// ── New data-plane coverage helpers (spec S4/S6.3) ──

func IncNackRedelivered(worker string) {
	NackRedeliveredTotal.WithLabelValues(sdkLabel, worker).Inc()
}
func IncBatchAtomicityViolation(worker string) {
	BatchAtomicityViolationsTotal.WithLabelValues(sdkLabel, worker).Inc()
}
func IncFilterMismatch(worker string) {
	FilterMismatchTotal.WithLabelValues(sdkLabel, worker).Inc()
}
func IncFilterMatchedDelivered(worker string) {
	FilterMatchedDeliveredTotal.WithLabelValues(sdkLabel, worker).Inc()
}
func IncSchemaRejectViolation(worker string) {
	SchemaRejectViolationsTotal.WithLabelValues(sdkLabel, worker).Inc()
}
func IncSchemaEnforceConfirmed(worker string) {
	SchemaEnforceConfirmedTotal.WithLabelValues(sdkLabel, worker).Inc()
}
func AddSeekReplayExpected(worker string, delta uint64) {
	SeekReplayExpectedTotal.WithLabelValues(sdkLabel, worker).Add(float64(delta))
}
func AddSeekReplayObserved(worker string, delta uint64) {
	SeekReplayObservedTotal.WithLabelValues(sdkLabel, worker).Add(float64(delta))
}
func IncSeekReplayMismatch(worker string) {
	SeekReplayMismatchTotal.WithLabelValues(sdkLabel, worker).Inc()
}
func IncInflightCapTailRedelivered(worker string) {
	InflightCapTailRedeliveredTotal.WithLabelValues(sdkLabel, worker).Inc()
}
func AddInflightCapLost(worker string, delta uint64) {
	InflightCapLostTotal.WithLabelValues(sdkLabel, worker).Add(float64(delta))
}
func IncExactlyOnceInvalidAckConfirmed(worker string) {
	ExactlyOnceInvalidAckConfirmedTotal.WithLabelValues(sdkLabel, worker).Inc()
}
func IncFlowControlViolation(worker string) {
	FlowControlViolationsTotal.WithLabelValues(sdkLabel, worker).Inc()
}
func IncRetryBackoffObserved(worker string) {
	RetryBackoffObservedTotal.WithLabelValues(sdkLabel, worker).Inc()
}
func IncLeaseExtendConfirmed(worker string) {
	LeaseExtendConfirmedTotal.WithLabelValues(sdkLabel, worker).Inc()
}
func IncLeaseExtendRejectConfirmed(worker string) {
	LeaseExtendRejectConfirmedTotal.WithLabelValues(sdkLabel, worker).Inc()
}
func IncCrossStreamAckConfirmed(worker string) {
	CrossStreamAckConfirmedTotal.WithLabelValues(sdkLabel, worker).Inc()
}
func IncPeriodicCloseObserved(worker string) {
	PeriodicCloseObservedTotal.WithLabelValues(sdkLabel, worker).Inc()
}

func SetOutstanding(worker string, n float64) {
	OutstandingMessages.WithLabelValues(sdkLabel, worker).Set(n)
}
func SetDeliveryAttemptMax(worker string, n float64) {
	DeliveryAttemptMax.WithLabelValues(sdkLabel, worker).Set(n)
}

func ObserveLatency(worker string, d time.Duration) {
	MessageLatencySeconds.WithLabelValues(sdkLabel, worker).Observe(d.Seconds())
}

func ObserveSendDuration(worker string, d time.Duration) {
	SendDurationSeconds.WithLabelValues(sdkLabel, worker).Observe(d.Seconds())
}

func AddDowntime(worker string, seconds float64) {
	DowntimeSecondsTotal.WithLabelValues(sdkLabel, worker).Add(seconds)
}
func IncForcedDisconnect() { ForcedDisconnectsTotal.WithLabelValues(sdkLabel).Inc() }
func RecordBytesSent(worker string, n int) {
	BytesSentTotal.WithLabelValues(sdkLabel, worker).Add(float64(n))
}

func RecordBytesReceived(worker string, n int) {
	BytesReceivedTotal.WithLabelValues(sdkLabel, worker).Add(float64(n))
}

// ── Gauge helpers ──

func SetActiveConnections(worker string, n float64) {
	ActiveConnections.WithLabelValues(sdkLabel, worker).Set(n)
}
func SetTargetRate(worker string, r float64) { TargetRate.WithLabelValues(sdkLabel, worker).Set(r) }
func SetActualRate(worker string, r float64) { ActualRate.WithLabelValues(sdkLabel, worker).Set(r) }
func SetConsumerLag(worker string, lag float64) {
	ConsumerLag.WithLabelValues(sdkLabel, worker).Set(lag)
}

// ── In-memory accumulators (lifted from aws / cloud-events burnin) ──

// LatencyAccumulator records latency values for percentile computation at
// verdict time using an HdrHistogram (1µs–60s, 3 sig figs).
type LatencyAccumulator struct {
	mu   sync.Mutex
	hist *hdrhistogram.Histogram
}

func NewLatencyAccumulator() *LatencyAccumulator {
	return &LatencyAccumulator{hist: hdrhistogram.New(1, 60_000_000, 3)}
}

func (a *LatencyAccumulator) Record(d time.Duration) {
	a.mu.Lock()
	_ = a.hist.RecordValue(d.Microseconds())
	a.mu.Unlock()
}

// Percentiles returns P50, P95, P99, P99.9 in milliseconds.
func (a *LatencyAccumulator) Percentiles() (p50, p95, p99, p999 float64) {
	a.mu.Lock()
	defer a.mu.Unlock()
	p50 = float64(a.hist.ValueAtQuantile(50)) / 1000.0
	p95 = float64(a.hist.ValueAtQuantile(95)) / 1000.0
	p99 = float64(a.hist.ValueAtQuantile(99)) / 1000.0
	p999 = float64(a.hist.ValueAtQuantile(99.9)) / 1000.0
	return
}

func (a *LatencyAccumulator) Reset() {
	a.mu.Lock()
	a.hist.Reset()
	a.mu.Unlock()
}

func (a *LatencyAccumulator) Count() int64 {
	a.mu.Lock()
	c := a.hist.TotalCount()
	a.mu.Unlock()
	return c
}

const slidingWindowSize = 30

// SlidingRateWindow tracks message rate over a 30-second sliding window.
type SlidingRateWindow struct {
	mu      sync.Mutex
	buckets [slidingWindowSize]int64
	idx     int
	total   int64
	ticks   int
}

func NewSlidingRateWindow() *SlidingRateWindow { return &SlidingRateWindow{} }

func (w *SlidingRateWindow) Record() {
	w.mu.Lock()
	w.buckets[w.idx]++
	w.total++
	w.mu.Unlock()
}

func (w *SlidingRateWindow) Advance() {
	w.mu.Lock()
	w.idx = (w.idx + 1) % slidingWindowSize
	w.total -= w.buckets[w.idx]
	w.buckets[w.idx] = 0
	w.ticks++
	w.mu.Unlock()
}

func (w *SlidingRateWindow) Rate() float64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	window := w.ticks
	if window > slidingWindowSize {
		window = slidingWindowSize
	}
	if window == 0 {
		return 0
	}
	return float64(w.total) / float64(window)
}

func (w *SlidingRateWindow) Reset() {
	w.mu.Lock()
	for i := range w.buckets {
		w.buckets[i] = 0
	}
	w.total = 0
	w.idx = 0
	w.ticks = 0
	w.mu.Unlock()
}

const peakWindowSize = 10

// PeakRateTracker tracks peak throughput over a 10-second sliding window.
type PeakRateTracker struct {
	mu      sync.Mutex
	buckets []int64
	idx     int
	peak    float64
}

func NewPeakRateTracker() *PeakRateTracker {
	return &PeakRateTracker{buckets: make([]int64, peakWindowSize)}
}

func (p *PeakRateTracker) Record() {
	p.mu.Lock()
	p.buckets[p.idx]++
	p.mu.Unlock()
}

func (p *PeakRateTracker) Advance() {
	p.mu.Lock()
	defer p.mu.Unlock()
	var total int64
	for _, b := range p.buckets {
		total += b
	}
	avg := float64(total) / float64(peakWindowSize)
	if avg > p.peak {
		p.peak = avg
	}
	p.idx = (p.idx + 1) % peakWindowSize
	p.buckets[p.idx] = 0
}

func (p *PeakRateTracker) Peak() float64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.peak
}
