// Package config defines the burn-in harness configuration model, defaults,
// loading, validation, and duration parsing. It mirrors the kubemq-aws burnin
// config recast for Google Cloud Pub/Sub (spec S6.5): the aws block is replaced
// by a gcp block, the AWS workers by the seven Pub/Sub workers
// (publish_pull_round_trip, streaming_pull, ordering_keys, exactly_once,
// dead_letter, fan_out, cross_protocol_interop), and the broker address is the
// emulator host (KUBEMQ_BROKER_ADDRESS / PUBSUB_EMULATOR_HOST, default
// localhost:8085). The interop worker additionally uses the native gRPC address
// (KUBEMQ_GRPC_ADDRESS, default localhost:50000) for kubemq-go.
package config

import (
	"bytes"
	cryptorand "crypto/rand"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// ConfigVersion is the required config schema version.
const ConfigVersion = "2"

// Worker name constants (spec S6.3 — one per Pub/Sub pattern).
const (
	WorkerPublishPullRoundTrip = "publish_pull_round_trip"
	WorkerStreamingPull        = "streaming_pull"
	WorkerOrderingKeys         = "ordering_keys"
	WorkerExactlyOnce          = "exactly_once"
	WorkerDeadLetter           = "dead_letter"
	WorkerFanOut               = "fan_out"
	WorkerCrossProtocol        = "cross_protocol_interop"

	// New data-plane workers (spec S6.1).
	WorkerNackRedelivery = "nack_redelivery"
	WorkerFiltering      = "filtering"
	WorkerSchemaEnforce  = "schema_enforce"
	WorkerSeekSnapshot   = "seek_snapshot"
	WorkerInflightCap    = "inflight_cap"
)

// AllWorkerNames lists all Pub/Sub burn-in workers in a stable order.
var AllWorkerNames = []string{
	WorkerPublishPullRoundTrip,
	WorkerStreamingPull,
	WorkerOrderingKeys,
	WorkerExactlyOnce,
	WorkerDeadLetter,
	WorkerFanOut,
	WorkerCrossProtocol,
	WorkerNackRedelivery,
	WorkerFiltering,
	WorkerSchemaEnforce,
	WorkerSeekSnapshot,
	WorkerInflightCap,
}

// IsOrderingWorker reports whether the worker drives ordered (per-key) delivery.
// Only ordering_keys does; its gate is zero ordering violations.
func IsOrderingWorker(name string) bool {
	return name == WorkerOrderingKeys
}

// IsStreamingPullWorker reports whether the worker drives the long-lived
// bidirectional StreamingPull stream. It hosts the flow-control / lease-extend /
// cross-stream probes and observes the periodic server-initiated close (spec S5.8).
func IsStreamingPullWorker(name string) bool {
	return name == WorkerStreamingPull
}

// IsExactlyOnceWorker reports whether the worker drives an exactly-once
// subscription. Its gate is that exactly-once ack confirmations are fully
// reconciled (no unrecovered invalid/temporary-failed ack ids).
func IsExactlyOnceWorker(name string) bool {
	return name == WorkerExactlyOnce
}

// IsDeadLetterWorker reports whether the worker drives dead-letter redelivery.
// Its delivery is at-least-once by design (the message is redelivered up to
// max_delivery_attempts before being republished to the DLQ), so the
// duplication gate does not apply; its gate is the DLQ republish + original-ack
// no-loss invariant.
func IsDeadLetterWorker(name string) bool {
	return name == WorkerDeadLetter
}

// IsFanoutWorker reports whether the worker drives one-topic→N-subscription
// fan-out. Its gate is per-subscription completeness (zero dropped deliveries).
func IsFanoutWorker(name string) bool {
	return name == WorkerFanOut
}

// IsInteropWorker reports whether the worker uses the native kubemq-go side in
// addition to the GCP Pub/Sub SDK (cross-protocol interop). Its gate is
// convergence: every published message reaches the native consumer on
// pubsub.{topic}.
func IsInteropWorker(name string) bool {
	return name == WorkerCrossProtocol
}

// IsFilteringWorker reports whether the worker drives a CEL-subset attribute
// filtered subscription. Its gate is zero filter mismatches (no filtered-out
// message delivered) plus no-loss over the matching producer (spec S5.3).
func IsFilteringWorker(name string) bool { return name == WorkerFiltering }

// IsSchemaEnforceWorker reports whether the worker drives schema
// enforce-on-publish. Its gate is zero schema-reject violations (a
// non-conforming publish that succeeded) — spec S5.4.
func IsSchemaEnforceWorker(name string) bool { return name == WorkerSchemaEnforce }

// IsSeekSnapshotWorker reports whether the worker drives seek/snapshot replay.
// It deliberately redelivers acked messages on replay, so its duplication and
// throughput gates are exempted (round-structured, at-least-once by design,
// spec S5.5); its gate is zero replay-count mismatch.
func IsSeekSnapshotWorker(name string) bool { return name == WorkerSeekSnapshot }

// IsInflightCapWorker reports whether the worker drives a backlog beyond the
// per-subscription in-flight cap. The unleased tail is redelivered (at-least-once
// by design), so its duplication gate is exempted (spec S5.6); its gate is zero
// in-flight-cap loss (the tail must redeliver, not vanish).
func IsInflightCapWorker(name string) bool { return name == WorkerInflightCap }

// IsNackRedeliveryWorker reports whether the worker explicitly nacks a fraction
// of messages via ModifyAckDeadline(0). Redelivery/dup is at-least-once BY
// DESIGN (spec S5.1), so this predicate is added to the per-worker duplication
// exemption alongside IsDeadLetterWorker. This is an exemption, NOT a change to
// the global max_duplication_pct threshold. The LOSS gate is fully retained.
func IsNackRedeliveryWorker(name string) bool { return name == WorkerNackRedelivery }

// IsUnaryPullWorker reports whether the worker consumes via the low-level unary
// Pull RPC (request/response) rather than server-driven StreamingPull. The
// connector team confirmed (2026-06-24) that unary-Pull latency is dominated by
// the client's call cadence plus the broker's 1s-minimum Get — inherent to the
// unary model, NOT a connector-side queue — and is further inflated when the
// client is co-located with the connector/broker. publish_pull_round_trip is the
// unary-Pull worker.
func IsUnaryPullWorker(name string) bool { return name == WorkerPublishPullRoundTrip }

// IsLatencyGateExempt reports whether a worker is exempt from the steady-state
// p99 latency gate because its end-to-end latency is by-design or inherent to its
// delivery model rather than a steady-state SLA signal. Over-threshold latency
// for these is surfaced as a WARNING, not a hard failure (connector-team
// dispositions 2026-06-24):
//   - dead_letter:   the retry-backoff cycle (10→20→40s …) before dead-lettering
//   - inflight_cap:  deliberately holds a large un-acked backlog over the cap
//   - seek_snapshot: bulk seek-replay re-reads
//   - publish_pull:  unary Pull is client-paced + the broker's 1s-min Get
func IsLatencyGateExempt(name string) bool {
	return name == WorkerDeadLetter || name == WorkerInflightCap ||
		name == WorkerSeekSnapshot || name == WorkerPublishPullRoundTrip
}

// IsDuplicationGateExempt reports whether a worker is exempt from the hard
// duplication gate. The first four redeliver at-least-once BY DESIGN; the unary
// Pull worker's duplication is the at-least-once consequence of its inherent
// latency exceeding the ack-deadline under co-located load (not a connector
// defect — see IsUnaryPullWorker). Exempt workers over-threshold are surfaced as
// a WARNING. This is a per-worker exemption, NOT a change to the global
// max_duplication_pct, and the LOSS gate is always retained for every worker.
func IsDuplicationGateExempt(name string) bool {
	return name == WorkerDeadLetter || name == WorkerNackRedelivery ||
		name == WorkerInflightCap || name == WorkerSeekSnapshot ||
		name == WorkerPublishPullRoundTrip
}

// BrokerConfig holds the Pub/Sub emulator host (host:port for the connector
// gRPC listener, consumed via PUBSUB_EMULATOR_HOST) and the native gRPC address
// used by the interop worker.
type BrokerConfig struct {
	Address        string `yaml:"address" json:"address"`
	GRPCAddress    string `yaml:"grpc_address" json:"grpc_address"`
	ClientIDPrefix string `yaml:"client_id_prefix" json:"client_id_prefix"`
}

// GCPConfig is the Google Cloud Pub/Sub knob block (spec S6.5) replacing the
// AWS block. Per-worker overrides (enable_message_ordering, enable_exactly_once,
// filter, dead_letter_topic, max_delivery_attempts, fanout_subscriptions) are
// applied per worker.
type GCPConfig struct {
	ProjectID             string `yaml:"project_id" json:"project_id"`
	EmulatorHost          string `yaml:"emulator_host" json:"emulator_host"`
	AckDeadlineSeconds    int    `yaml:"ack_deadline_seconds" json:"ack_deadline_seconds"`
	EnableMessageOrdering bool   `yaml:"enable_message_ordering" json:"enable_message_ordering"`
	EnableExactlyOnce     bool   `yaml:"enable_exactly_once" json:"enable_exactly_once"`
	Filter                string `yaml:"filter" json:"filter"`
	DeadLetterTopic       string `yaml:"dead_letter_topic" json:"dead_letter_topic"`
	MaxDeliveryAttempts   int    `yaml:"max_delivery_attempts" json:"max_delivery_attempts"`
	FanoutSubscriptions   int    `yaml:"fanout_subscriptions" json:"fanout_subscriptions"`
	BatchSize             int    `yaml:"batch_size" json:"batch_size"`

	// New data-plane worker knobs + probe toggles (spec S6.1).
	SchemaType            string  `yaml:"schema_type" json:"schema_type"`                         // "avro" | "protobuf"; default "avro"
	SchemaEncoding        string  `yaml:"schema_encoding" json:"schema_encoding"`                 // "json" | "binary"; default "json"
	NackFraction          float64 `yaml:"nack_fraction" json:"nack_fraction"`                     // 0..1; default 0.25
	MaxOutstandingBytes   int     `yaml:"max_outstanding_bytes" json:"max_outstanding_bytes"`     // 0 = unlimited; default 0
	InflightBacklogTarget int     `yaml:"inflight_backlog_target" json:"inflight_backlog_target"` // default 25000 (> connector 20000 cap)
	SeekReplayCount       int     `yaml:"seek_replay_count" json:"seek_replay_count"`             // default 500
	ExactlyOnceProbe      bool    `yaml:"exactly_once_probe" json:"exactly_once_probe"`           // default true
	LeaseExtendProbe      bool    `yaml:"lease_extend_probe" json:"lease_extend_probe"`           // default true
	CrossStreamProbe      bool    `yaml:"cross_stream_probe" json:"cross_stream_probe"`           // default true
	FlowControlProbe      bool    `yaml:"flow_control_probe" json:"flow_control_probe"`           // default true (server-side clamp, REQ-009)
	RetryBackoffProbe     bool    `yaml:"retry_backoff_probe" json:"retry_backoff_probe"`         // default false (opt-in, soak)
	BatchAtomicityProbe   bool    `yaml:"batch_atomicity_probe" json:"batch_atomicity_probe"`     // default true
}

// WorkerConfig holds the per-worker concurrency + rate knobs. ProducersPerChannel
// is the number of Pub/Sub publishers per channel; ConsumersPerChannel the number
// of pullers/streaming receivers (spec S6.5). Subscriptions is used by the
// fan_out worker (number of target subscriptions) when > 0, defaulting to
// GCP.FanoutSubscriptions.
type WorkerConfig struct {
	Enabled             bool `yaml:"enabled" json:"enabled"`
	Channels            int  `yaml:"channels" json:"channels"`
	ProducersPerChannel int  `yaml:"producers_per_channel" json:"producers_per_channel"`
	ConsumersPerChannel int  `yaml:"consumers_per_channel" json:"consumers_per_channel"`
	Subscriptions       int  `yaml:"subscriptions" json:"subscriptions"`
	Rate                int  `yaml:"rate" json:"rate"`
}

// WorkersConfig groups the worker blocks.
type WorkersConfig struct {
	PublishPullRoundTrip WorkerConfig `yaml:"publish_pull_round_trip" json:"publish_pull_round_trip"`
	StreamingPull        WorkerConfig `yaml:"streaming_pull" json:"streaming_pull"`
	OrderingKeys         WorkerConfig `yaml:"ordering_keys" json:"ordering_keys"`
	ExactlyOnce          WorkerConfig `yaml:"exactly_once" json:"exactly_once"`
	DeadLetter           WorkerConfig `yaml:"dead_letter" json:"dead_letter"`
	FanOut               WorkerConfig `yaml:"fan_out" json:"fan_out"`
	CrossProtocol        WorkerConfig `yaml:"cross_protocol_interop" json:"cross_protocol_interop"`
	NackRedelivery       WorkerConfig `yaml:"nack_redelivery" json:"nack_redelivery"`
	Filtering            WorkerConfig `yaml:"filtering" json:"filtering"`
	SchemaEnforce        WorkerConfig `yaml:"schema_enforce" json:"schema_enforce"`
	SeekSnapshot         WorkerConfig `yaml:"seek_snapshot" json:"seek_snapshot"`
	InflightCap          WorkerConfig `yaml:"inflight_cap" json:"inflight_cap"`
}

// MessageConfig holds payload sizing knobs (CRC32 + sequence stamped into a
// Pub/Sub message attribute, body padded to size).
type MessageConfig struct {
	SizeMode         string `yaml:"size_mode" json:"size_mode"`
	SizeBytes        int    `yaml:"size_bytes" json:"size_bytes"`
	SizeDistribution string `yaml:"size_distribution" json:"size_distribution"`
	ReorderWindow    int    `yaml:"reorder_window" json:"reorder_window"`
}

// MetricsConfig holds the control HTTP port and report interval.
type MetricsConfig struct {
	Port           int    `yaml:"port" json:"port"`
	ReportInterval string `yaml:"report_interval" json:"report_interval"`
}

// LoggingConfig holds log format and level.
type LoggingConfig struct {
	Format string `yaml:"format" json:"format"`
	Level  string `yaml:"level" json:"level"`
}

// ForcedDisconnConfig drives the connection-churn injector (recreates the GCP
// Pub/Sub clients / native gRPC clients to exercise StreamingPull reconnect and
// at-least-once redelivery on lease expiry).
type ForcedDisconnConfig struct {
	Interval string `yaml:"interval" json:"interval"`
	Duration string `yaml:"duration" json:"duration"`
}

// RecoveryConfig holds reconnect backoff knobs.
type RecoveryConfig struct {
	ReconnectInterval    string  `yaml:"reconnect_interval" json:"reconnect_interval"`
	ReconnectMaxInterval string  `yaml:"reconnect_max_interval" json:"reconnect_max_interval"`
	ReconnectMultiplier  float64 `yaml:"reconnect_multiplier" json:"reconnect_multiplier"`
}

// ShutdownConfig holds the drain timeout.
type ShutdownConfig struct {
	DrainTimeoutSeconds int `yaml:"drain_timeout_seconds" json:"drain_timeout_seconds"`
}

// OutputConfig holds report output knobs.
type OutputConfig struct {
	ReportFile string `yaml:"report_file" json:"report_file"`
	SDKVersion string `yaml:"sdk_version" json:"sdk_version"`
}

// ThresholdsConfig holds pass/fail thresholds (spec S6.6). The Pub/Sub-specific
// ordering_violations=0, exactly_once-reconciliation, dead_letter no-loss, and
// fan_out per-sub completeness gates are derived in the engine verdict, not
// configurable thresholds.
type ThresholdsConfig struct {
	MaxLossPct            float64 `yaml:"max_loss_pct" json:"max_loss_pct"`
	MaxDuplicationPct     float64 `yaml:"max_duplication_pct" json:"max_duplication_pct"`
	MaxP50LatencyMS       float64 `yaml:"max_p50_latency_ms" json:"max_p50_latency_ms"`
	MaxP95LatencyMS       float64 `yaml:"max_p95_latency_ms" json:"max_p95_latency_ms"`
	MaxP99LatencyMS       float64 `yaml:"max_p99_latency_ms" json:"max_p99_latency_ms"`
	MaxP999LatencyMS      float64 `yaml:"max_p999_latency_ms" json:"max_p999_latency_ms"`
	MinThroughputPct      float64 `yaml:"min_throughput_pct" json:"min_throughput_pct"`
	MaxErrorRatePct       float64 `yaml:"max_error_rate_pct" json:"max_error_rate_pct"`
	MaxMemoryGrowthFactor float64 `yaml:"max_memory_growth_factor" json:"max_memory_growth_factor"`
	MaxDowntimePct        float64 `yaml:"max_downtime_pct" json:"max_downtime_pct"`
	MaxDuration           string  `yaml:"max_duration" json:"max_duration"`
}

// WarmupConfig holds warmup parallelism + per-channel timeout.
type WarmupConfig struct {
	MaxParallelChannels int `yaml:"max_parallel_channels" json:"max_parallel_channels"`
	TimeoutPerChannelMs int `yaml:"timeout_per_channel_ms" json:"timeout_per_channel_ms"`
}

// CORSConfig holds the allowed origins for the control API.
type CORSConfig struct {
	Origins string `yaml:"origins" json:"origins"`
}

// Config is the full burn-in configuration.
type Config struct {
	Version          string              `yaml:"version" json:"version"`
	Broker           BrokerConfig        `yaml:"broker" json:"broker"`
	Mode             string              `yaml:"mode" json:"mode"`
	Duration         string              `yaml:"duration" json:"duration"`
	RunID            string              `yaml:"run_id" json:"run_id"`
	WarmupDuration   string              `yaml:"warmup_duration" json:"warmup_duration"`
	GCP              GCPConfig           `yaml:"gcp" json:"gcp"`
	Workers          WorkersConfig       `yaml:"workers" json:"workers"`
	Message          MessageConfig       `yaml:"message" json:"message"`
	Metrics          MetricsConfig       `yaml:"metrics" json:"metrics"`
	Logging          LoggingConfig       `yaml:"logging" json:"logging"`
	ForcedDisconnect ForcedDisconnConfig `yaml:"forced_disconnect" json:"forced_disconnect"`
	Recovery         RecoveryConfig      `yaml:"recovery" json:"recovery"`
	Shutdown         ShutdownConfig      `yaml:"shutdown" json:"shutdown"`
	Output           OutputConfig        `yaml:"output" json:"output"`
	Thresholds       ThresholdsConfig    `yaml:"thresholds" json:"thresholds"`
	Warmup           WarmupConfig        `yaml:"warmup" json:"warmup"`
	CORS             CORSConfig          `yaml:"cors" json:"cors"`

	DurationParsed        time.Duration `yaml:"-" json:"-"`
	WarmupDurationParsed  time.Duration `yaml:"-" json:"-"`
	ReportIntervalParsed  time.Duration `yaml:"-" json:"-"`
	ForcedDisconnInterval time.Duration `yaml:"-" json:"-"`
	ForcedDisconnDuration time.Duration `yaml:"-" json:"-"`
	ReconnectInterval     time.Duration `yaml:"-" json:"-"`
	ReconnectMaxInterval  time.Duration `yaml:"-" json:"-"`
	MaxDurationParsed     time.Duration `yaml:"-" json:"-"`
	Warnings              []string      `yaml:"-" json:"-"`
}

// DefaultConfig returns the built-in default configuration.
func DefaultConfig() *Config {
	c := &Config{}
	c.Version = ConfigVersion
	c.Broker.Address = "localhost:8085"
	c.Broker.GRPCAddress = "localhost:50000"
	c.Broker.ClientIDPrefix = "burnin-gcp"
	c.Mode = "soak"
	c.Duration = "1h"

	c.GCP = GCPConfig{
		ProjectID:             "burnin",
		EmulatorHost:          "",
		AckDeadlineSeconds:    10,
		EnableMessageOrdering: false,
		EnableExactlyOnce:     false,
		Filter:                "",
		DeadLetterTopic:       "",
		MaxDeliveryAttempts:   5,
		FanoutSubscriptions:   3,
		BatchSize:             1,

		SchemaType:            "avro",
		SchemaEncoding:        "json",
		NackFraction:          0.25,
		MaxOutstandingBytes:   0,
		InflightBacklogTarget: 25000,
		SeekReplayCount:       500,
		ExactlyOnceProbe:      true,
		LeaseExtendProbe:      true,
		CrossStreamProbe:      true,
		FlowControlProbe:      true,
		RetryBackoffProbe:     false,
		BatchAtomicityProbe:   true,
	}

	c.Workers = WorkersConfig{
		PublishPullRoundTrip: WorkerConfig{
			Enabled: true, Channels: 1,
			ProducersPerChannel: 1, ConsumersPerChannel: 2,
			Rate: 100,
		},
		StreamingPull: WorkerConfig{
			Enabled: true, Channels: 1,
			ProducersPerChannel: 1, ConsumersPerChannel: 1,
			Rate: 100,
		},
		OrderingKeys: WorkerConfig{
			Enabled: true, Channels: 1,
			ProducersPerChannel: 1, ConsumersPerChannel: 1,
			Rate: 50,
		},
		ExactlyOnce: WorkerConfig{
			Enabled: true, Channels: 1,
			ProducersPerChannel: 1, ConsumersPerChannel: 1,
			Rate: 50,
		},
		DeadLetter: WorkerConfig{
			Enabled: true, Channels: 1,
			ProducersPerChannel: 1, ConsumersPerChannel: 1,
			Rate: 30,
		},
		FanOut: WorkerConfig{
			Enabled: true, Channels: 1,
			ProducersPerChannel: 1, Subscriptions: 3,
			Rate: 30,
		},
		CrossProtocol: WorkerConfig{
			Enabled: true, Channels: 1,
			ProducersPerChannel: 1, ConsumersPerChannel: 1,
			Rate: 50,
		},
		NackRedelivery: WorkerConfig{
			Enabled: true, Channels: 1,
			ProducersPerChannel: 1, ConsumersPerChannel: 1,
			Rate: 50,
		},
		Filtering: WorkerConfig{
			Enabled: true, Channels: 1,
			ProducersPerChannel: 1, ConsumersPerChannel: 1,
			Rate: 50,
		},
		SchemaEnforce: WorkerConfig{
			Enabled: false, Channels: 1,
			ProducersPerChannel: 1, ConsumersPerChannel: 1,
			Rate: 25,
		},
		SeekSnapshot: WorkerConfig{
			Enabled: false, Channels: 1,
			ProducersPerChannel: 1, ConsumersPerChannel: 1,
			Rate: 25,
		},
		InflightCap: WorkerConfig{
			Enabled: false, Channels: 1,
			ProducersPerChannel: 1, ConsumersPerChannel: 1,
			Rate: 100,
		},
	}

	c.Message = MessageConfig{
		SizeMode:         "fixed",
		SizeBytes:        1024,
		SizeDistribution: "256:80,4096:15,65536:5",
		ReorderWindow:    10_000,
	}

	c.Metrics = MetricsConfig{
		Port:           8899,
		ReportInterval: "30s",
	}

	c.Logging = LoggingConfig{Format: "text", Level: "info"}

	c.ForcedDisconnect = ForcedDisconnConfig{
		Interval: "0",
		Duration: "5s",
	}

	c.Recovery = RecoveryConfig{
		ReconnectInterval:    "1s",
		ReconnectMaxInterval: "30s",
		ReconnectMultiplier:  2.0,
	}

	c.Shutdown.DrainTimeoutSeconds = 10

	c.Thresholds = ThresholdsConfig{
		MaxLossPct:            0.0,
		MaxDuplicationPct:     1.0,
		MaxP50LatencyMS:       2000,
		MaxP95LatencyMS:       5000,
		MaxP99LatencyMS:       8000,
		MaxP999LatencyMS:      15000,
		MinThroughputPct:      80,
		MaxErrorRatePct:       1.0,
		MaxMemoryGrowthFactor: 2.0,
		MaxDowntimePct:        10,
		MaxDuration:           "168h",
	}

	c.Warmup = WarmupConfig{
		MaxParallelChannels: 10,
		TimeoutPerChannelMs: 5000,
	}

	c.CORS.Origins = "*"

	return c
}

// GetWorkerConfig returns a pointer to the named worker's config block.
func (c *Config) GetWorkerConfig(name string) *WorkerConfig {
	switch name {
	case WorkerPublishPullRoundTrip:
		return &c.Workers.PublishPullRoundTrip
	case WorkerStreamingPull:
		return &c.Workers.StreamingPull
	case WorkerOrderingKeys:
		return &c.Workers.OrderingKeys
	case WorkerExactlyOnce:
		return &c.Workers.ExactlyOnce
	case WorkerDeadLetter:
		return &c.Workers.DeadLetter
	case WorkerFanOut:
		return &c.Workers.FanOut
	case WorkerCrossProtocol:
		return &c.Workers.CrossProtocol
	case WorkerNackRedelivery:
		return &c.Workers.NackRedelivery
	case WorkerFiltering:
		return &c.Workers.Filtering
	case WorkerSchemaEnforce:
		return &c.Workers.SchemaEnforce
	case WorkerSeekSnapshot:
		return &c.Workers.SeekSnapshot
	case WorkerInflightCap:
		return &c.Workers.InflightCap
	default:
		return nil
	}
}

// GetWorkerRate returns the configured rate for a worker (fallback 100).
func (c *Config) GetWorkerRate(name string) int {
	if wc := c.GetWorkerConfig(name); wc != nil {
		return wc.Rate
	}
	return 100
}

// GetWorkerChannels returns the configured channel count for a worker (min 1).
func (c *Config) GetWorkerChannels(name string) int {
	if wc := c.GetWorkerConfig(name); wc != nil && wc.Channels > 0 {
		return wc.Channels
	}
	return 1
}

// FanoutSubscriptionCount returns the number of target subscriptions for the
// fan_out worker (per-worker Subscriptions, falling back to
// GCP.FanoutSubscriptions).
func (c *Config) FanoutSubscriptionCount() int {
	if c.Workers.FanOut.Subscriptions > 0 {
		return c.Workers.FanOut.Subscriptions
	}
	if c.GCP.FanoutSubscriptions > 0 {
		return c.GCP.FanoutSubscriptions
	}
	return 2
}

// TotalChannelCount sums enabled worker channel counts.
func (c *Config) TotalChannelCount() int {
	total := 0
	for _, name := range AllWorkerNames {
		if wc := c.GetWorkerConfig(name); wc != nil && wc.Enabled {
			total += wc.Channels
		}
	}
	return total
}

// Load reads and parses the config file (or just defaults when path == ""),
// applies env overrides, parses durations, and mints a run ID.
func Load(path string) (*Config, error) {
	c := DefaultConfig()

	if path != "" {
		data, err := os.ReadFile(path)
		if err != nil {
			return nil, fmt.Errorf("read config file %s: %w", path, err)
		}

		decoder := yaml.NewDecoder(bytes.NewReader(data))
		decoder.KnownFields(true)
		if err := decoder.Decode(c); err != nil {
			// Re-parse tolerantly so unknown fields warn rather than fail.
			c2 := DefaultConfig()
			if err2 := yaml.Unmarshal(data, c2); err2 != nil {
				return nil, fmt.Errorf("parse config file %s: %w", path, err2)
			}
			*c = *c2
			c.Warnings = append(c.Warnings, fmt.Sprintf("config has unknown fields: %v", err))
		}
	}

	applyEnvOverrides(c)

	if err := parseDurations(c); err != nil {
		return nil, err
	}

	if c.RunID == "" {
		c.RunID = RandomRunID()
	}

	return c, nil
}

// FindConfigFile resolves the config path search order (spec S6.5):
// CLI flag → ./burnin-config.yaml → /etc/burnin/config.yaml.
func FindConfigFile(cliPath string) string {
	if cliPath != "" {
		return cliPath
	}
	if _, err := os.Stat("./burnin-config.yaml"); err == nil {
		return "./burnin-config.yaml"
	}
	if _, err := os.Stat("/etc/burnin/config.yaml"); err == nil {
		return "/etc/burnin/config.yaml"
	}
	return ""
}

// Validate checks the config and returns a slice of errors. Entries prefixed
// "WARNING:" are advisory and do not fail validation.
func (c *Config) Validate() []error {
	var errs []error

	if c.Version != ConfigVersion {
		errs = append(errs, fmt.Errorf("version must be %q, got %q", ConfigVersion, c.Version))
	}

	if c.Broker.Address == "" {
		errs = append(errs, fmt.Errorf("broker.address is required (Pub/Sub emulator host:port)"))
	}

	if c.GCP.ProjectID == "" {
		errs = append(errs, fmt.Errorf("gcp.project_id is required (segment is parsed but ignored by the connector; the SDK still needs a non-empty id)"))
	}
	if c.GCP.AckDeadlineSeconds < 10 || c.GCP.AckDeadlineSeconds > 600 {
		errs = append(errs, fmt.Errorf("gcp.ack_deadline_seconds: must be 10-600, got %d", c.GCP.AckDeadlineSeconds))
	}
	if c.GCP.MaxDeliveryAttempts != 0 && (c.GCP.MaxDeliveryAttempts < 5 || c.GCP.MaxDeliveryAttempts > 100) {
		errs = append(errs, fmt.Errorf("gcp.max_delivery_attempts: must be 0 (disabled) or 5-100, got %d", c.GCP.MaxDeliveryAttempts))
	}
	if c.GCP.BatchSize < 1 || c.GCP.BatchSize > 1000 {
		errs = append(errs, fmt.Errorf("gcp.batch_size: must be 1-1000, got %d", c.GCP.BatchSize))
	}
	if c.GCP.NackFraction < 0 || c.GCP.NackFraction > 1 {
		errs = append(errs, fmt.Errorf("gcp.nack_fraction: must be 0-1, got %f", c.GCP.NackFraction))
	}
	if c.GCP.SchemaType != "avro" && c.GCP.SchemaType != "protobuf" {
		errs = append(errs, fmt.Errorf("gcp.schema_type: must be 'avro' or 'protobuf', got %q", c.GCP.SchemaType))
	}
	if c.GCP.SchemaEncoding != "json" && c.GCP.SchemaEncoding != "binary" {
		errs = append(errs, fmt.Errorf("gcp.schema_encoding: must be 'json' or 'binary', got %q", c.GCP.SchemaEncoding))
	}
	if c.GCP.InflightBacklogTarget < 1 {
		errs = append(errs, fmt.Errorf("gcp.inflight_backlog_target: must be >= 1, got %d", c.GCP.InflightBacklogTarget))
	}
	if c.GCP.SeekReplayCount < 1 {
		errs = append(errs, fmt.Errorf("gcp.seek_replay_count: must be >= 1, got %d", c.GCP.SeekReplayCount))
	}
	if c.GCP.MaxOutstandingBytes < 0 {
		errs = append(errs, fmt.Errorf("gcp.max_outstanding_bytes: must be >= 0, got %d", c.GCP.MaxOutstandingBytes))
	}
	// When inflight_cap is enabled, the held no-ack backlog must fit in the reorder
	// window or held seqs are prematurely evicted into pendingLost (spec S5.6).
	if c.Workers.InflightCap.Enabled && c.Message.ReorderWindow < c.GCP.InflightBacklogTarget {
		errs = append(errs, fmt.Errorf("WARNING: message.reorder_window (%d) < gcp.inflight_backlog_target (%d) while inflight_cap is enabled -- held seqs may be prematurely evicted (spec S5.6)", c.Message.ReorderWindow, c.GCP.InflightBacklogTarget))
	}

	enabledCount := 0
	totalWorkers := 0

	for _, name := range AllWorkerNames {
		wc := c.GetWorkerConfig(name)
		if wc == nil || !wc.Enabled {
			continue
		}
		enabledCount++

		if wc.Channels < 1 || wc.Channels > 1000 {
			errs = append(errs, fmt.Errorf("%s.channels: must be 1-1000, got %d", name, wc.Channels))
		}
		if wc.Rate <= 0 {
			errs = append(errs, fmt.Errorf("%s.rate: must be > 0, got %d", name, wc.Rate))
		}
		if wc.ProducersPerChannel < 1 {
			errs = append(errs, fmt.Errorf("%s.producers_per_channel: must be >= 1, got %d", name, wc.ProducersPerChannel))
		}

		consumers := wc.ConsumersPerChannel
		if name == WorkerFanOut {
			consumers = wc.Subscriptions
			if wc.Subscriptions < 1 && c.GCP.FanoutSubscriptions < 1 {
				errs = append(errs, fmt.Errorf("%s.subscriptions (or gcp.fanout_subscriptions): must be >= 1", name))
			}
			if consumers < 1 {
				consumers = c.GCP.FanoutSubscriptions
			}
		} else if wc.ConsumersPerChannel < 1 {
			errs = append(errs, fmt.Errorf("%s.consumers_per_channel: must be >= 1, got %d", name, wc.ConsumersPerChannel))
		}
		totalWorkers += wc.Channels * (wc.ProducersPerChannel + consumers)
	}

	if enabledCount == 0 {
		errs = append(errs, fmt.Errorf("at least one worker must be enabled"))
	}

	if c.Message.SizeMode != "fixed" && c.Message.SizeMode != "distribution" {
		errs = append(errs, fmt.Errorf("message.size_mode must be 'fixed' or 'distribution', got %q", c.Message.SizeMode))
	}
	if c.Message.SizeMode == "fixed" && c.Message.SizeBytes < 64 {
		errs = append(errs, fmt.Errorf("message.size_bytes: must be >= 64, got %d", c.Message.SizeBytes))
	}
	// Connector MaxMessageBytes default is 10 MiB (CONNECTORS_PUB_SUB_MAX_MESSAGE_BYTES);
	// cap the fixed body well under it so the gRPC frame is never rejected.
	if c.Message.SizeMode == "fixed" && c.Message.SizeBytes > 10_485_760 {
		errs = append(errs, fmt.Errorf("message.size_bytes: must be <= 10485760 (connector 10 MiB limit), got %d", c.Message.SizeBytes))
	}
	if c.Message.ReorderWindow < 100 {
		errs = append(errs, fmt.Errorf("message.reorder_window: must be >= 100, got %d", c.Message.ReorderWindow))
	}

	if c.Shutdown.DrainTimeoutSeconds <= 0 {
		errs = append(errs, fmt.Errorf("shutdown.drain_timeout_seconds: must be > 0, got %d", c.Shutdown.DrainTimeoutSeconds))
	}
	if c.Metrics.Port < 1 || c.Metrics.Port > 65535 {
		errs = append(errs, fmt.Errorf("metrics.port: must be 1-65535, got %d", c.Metrics.Port))
	}

	if c.Thresholds.MaxLossPct < 0 || c.Thresholds.MaxLossPct > 100 {
		errs = append(errs, fmt.Errorf("thresholds.max_loss_pct: must be 0-100"))
	}
	if c.Thresholds.MaxDuplicationPct < 0 || c.Thresholds.MaxDuplicationPct > 100 {
		errs = append(errs, fmt.Errorf("thresholds.max_duplication_pct: must be 0-100"))
	}
	for label, v := range map[string]float64{
		"max_p50_latency_ms":  c.Thresholds.MaxP50LatencyMS,
		"max_p95_latency_ms":  c.Thresholds.MaxP95LatencyMS,
		"max_p99_latency_ms":  c.Thresholds.MaxP99LatencyMS,
		"max_p999_latency_ms": c.Thresholds.MaxP999LatencyMS,
	} {
		if v <= 0 {
			errs = append(errs, fmt.Errorf("thresholds.%s: must be > 0", label))
		}
	}
	if c.Thresholds.MinThroughputPct <= 0 || c.Thresholds.MinThroughputPct > 100 {
		errs = append(errs, fmt.Errorf("thresholds.min_throughput_pct: must be > 0 and <= 100"))
	}
	if c.Thresholds.MaxErrorRatePct < 0 || c.Thresholds.MaxErrorRatePct > 100 {
		errs = append(errs, fmt.Errorf("thresholds.max_error_rate_pct: must be 0-100"))
	}
	if c.Thresholds.MaxMemoryGrowthFactor < 1.0 {
		errs = append(errs, fmt.Errorf("thresholds.max_memory_growth_factor: must be >= 1.0"))
	}
	if c.Thresholds.MaxDowntimePct < 0 || c.Thresholds.MaxDowntimePct > 100 {
		errs = append(errs, fmt.Errorf("thresholds.max_downtime_pct: must be 0-100"))
	}
	if c.Recovery.ReconnectMultiplier < 1.0 {
		errs = append(errs, fmt.Errorf("recovery.reconnect_multiplier: must be >= 1.0, got %f", c.Recovery.ReconnectMultiplier))
	}

	if totalWorkers > 1000 {
		errs = append(errs, fmt.Errorf("WARNING: high worker count: %d -- may impact system resources", totalWorkers))
	}

	return errs
}

// LogResourceWarnings logs any advisory (WARNING:-prefixed) validation entries.
func (c *Config) LogResourceWarnings(logger *slog.Logger) {
	for _, e := range c.Validate() {
		if strings.HasPrefix(e.Error(), "WARNING:") {
			logger.Warn(e.Error())
		}
	}
}

func applyEnvOverrides(cfg *Config) {
	if v := os.Getenv("KUBEMQ_BROKER_ADDRESS"); v != "" {
		cfg.Broker.Address = v
	}
	if v := os.Getenv("KUBEMQ_GRPC_ADDRESS"); v != "" {
		cfg.Broker.GRPCAddress = v
	}
	// PUBSUB_EMULATOR_HOST is the standard Google env var the SDK honours directly;
	// when set, mirror it into broker.address so /info and the report agree.
	if v := os.Getenv("PUBSUB_EMULATOR_HOST"); v != "" {
		cfg.Broker.Address = v
	}

	envNames := map[string]string{
		WorkerPublishPullRoundTrip: "PUBLISH_PULL_ROUND_TRIP",
		WorkerStreamingPull:        "STREAMING_PULL",
		WorkerOrderingKeys:         "ORDERING_KEYS",
		WorkerExactlyOnce:          "EXACTLY_ONCE",
		WorkerDeadLetter:           "DEAD_LETTER",
		WorkerFanOut:               "FAN_OUT",
		WorkerCrossProtocol:        "CROSS_PROTOCOL_INTEROP",
		WorkerNackRedelivery:       "NACK_REDELIVERY",
		WorkerFiltering:            "FILTERING",
		WorkerSchemaEnforce:        "SCHEMA_ENFORCE",
		WorkerSeekSnapshot:         "SEEK_SNAPSHOT",
		WorkerInflightCap:          "INFLIGHT_CAP",
	}

	for name, env := range envNames {
		wc := cfg.GetWorkerConfig(name)
		if wc == nil {
			continue
		}
		if v := os.Getenv("BURNIN_" + env + "_RATE"); v != "" {
			if n, err := strconv.Atoi(v); err == nil {
				wc.Rate = n
			}
		}
		if v := os.Getenv("BURNIN_" + env + "_CHANNELS"); v != "" {
			if n, err := strconv.Atoi(v); err == nil {
				wc.Channels = n
			}
		}
		if v := os.Getenv("BURNIN_" + env + "_ENABLED"); v != "" {
			wc.Enabled = v == "true" || v == "1"
		}
	}
}

func parseDurations(c *Config) error {
	var err error

	if c.Duration != "" && c.Duration != "0" {
		c.DurationParsed, err = parseDuration(c.Duration)
		if err != nil {
			return fmt.Errorf("invalid duration %q: %w", c.Duration, err)
		}
	}
	if c.WarmupDuration != "" {
		c.WarmupDurationParsed, err = parseDuration(c.WarmupDuration)
		if err != nil {
			return fmt.Errorf("invalid warmup_duration %q: %w", c.WarmupDuration, err)
		}
	}
	if c.Metrics.ReportInterval != "" {
		c.ReportIntervalParsed, err = parseDuration(c.Metrics.ReportInterval)
		if err != nil {
			return fmt.Errorf("invalid metrics.report_interval %q: %w", c.Metrics.ReportInterval, err)
		}
	}
	if c.ForcedDisconnect.Interval != "" && c.ForcedDisconnect.Interval != "0" {
		c.ForcedDisconnInterval, err = parseDuration(c.ForcedDisconnect.Interval)
		if err != nil {
			return fmt.Errorf("invalid forced_disconnect.interval %q: %w", c.ForcedDisconnect.Interval, err)
		}
	}
	if c.ForcedDisconnect.Duration != "" {
		c.ForcedDisconnDuration, err = parseDuration(c.ForcedDisconnect.Duration)
		if err != nil {
			return fmt.Errorf("invalid forced_disconnect.duration %q: %w", c.ForcedDisconnect.Duration, err)
		}
	}
	if c.Recovery.ReconnectInterval != "" {
		c.ReconnectInterval, err = parseDuration(c.Recovery.ReconnectInterval)
		if err != nil {
			return fmt.Errorf("invalid recovery.reconnect_interval %q: %w", c.Recovery.ReconnectInterval, err)
		}
	}
	if c.Recovery.ReconnectMaxInterval != "" {
		c.ReconnectMaxInterval, err = parseDuration(c.Recovery.ReconnectMaxInterval)
		if err != nil {
			return fmt.Errorf("invalid recovery.reconnect_max_interval %q: %w", c.Recovery.ReconnectMaxInterval, err)
		}
	}
	if c.Thresholds.MaxDuration != "" {
		c.MaxDurationParsed, err = parseDuration(c.Thresholds.MaxDuration)
		if err != nil {
			return fmt.Errorf("invalid thresholds.max_duration %q: %w", c.Thresholds.MaxDuration, err)
		}
	}

	return nil
}

func parseDuration(s string) (time.Duration, error) {
	if strings.HasSuffix(s, "d") {
		s = strings.TrimSuffix(s, "d")
		days, err := strconv.Atoi(s)
		if err != nil {
			return 0, fmt.Errorf("invalid day duration: %s", s)
		}
		return time.Duration(days) * 24 * time.Hour, nil
	}
	return time.ParseDuration(s)
}

// RandomRunID returns an 8-hex-char random run identifier.
func RandomRunID() string {
	b := make([]byte, 4)
	_, _ = cryptorand.Read(b)
	return fmt.Sprintf("%08x", b)
}

// ParseDurationsPublic re-parses durations on a config (used after API overlay).
func ParseDurationsPublic(cfg *Config) error {
	return parseDurations(cfg)
}
