// Package api defines the control-API request/response models: the RunConfig
// pointer-overlay for POST /run/start, plus the RunStatus / RunSnapshot /
// MetricsSnapshot read models. Mirrors the kubemq-aws burnin api, recast for the
// Pub/Sub workers.
package api

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/kubemq-io/kubemq-gcp-pub-sub/burnin/config"
)

// RunConfig is the JSON body for POST /run/start. Pointer fields let omitted
// blocks fall through to the base config instead of zeroing it.
type RunConfig struct {
	Duration         string                      `json:"duration,omitempty"`
	Mode             string                      `json:"mode,omitempty"`
	GCP              *config.GCPConfig           `json:"gcp,omitempty"`
	Workers          *config.WorkersConfig       `json:"workers,omitempty"`
	Message          *config.MessageConfig       `json:"message,omitempty"`
	Thresholds       *config.ThresholdsConfig    `json:"thresholds,omitempty"`
	ForcedDisconnect *config.ForcedDisconnConfig `json:"forced_disconnect,omitempty"`
}

// ToInternalConfig overlays this RunConfig onto a copy of base, re-parses
// durations, validates, and returns the merged config.
func (rc *RunConfig) ToInternalConfig(base *config.Config) (*config.Config, error) {
	data, err := json.Marshal(base)
	if err != nil {
		return nil, fmt.Errorf("marshal base config: %w", err)
	}
	var cfg config.Config
	if err := json.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("unmarshal base config: %w", err)
	}

	overlay, err := json.Marshal(rc)
	if err != nil {
		return nil, fmt.Errorf("marshal run config: %w", err)
	}
	if err := json.Unmarshal(overlay, &cfg); err != nil {
		return nil, fmt.Errorf("unmarshal run config overlay: %w", err)
	}

	if err := config.ParseDurationsPublic(&cfg); err != nil {
		return nil, fmt.Errorf("parse durations: %w", err)
	}
	if cfg.RunID == "" {
		cfg.RunID = config.RandomRunID()
	}
	if errs := cfg.Validate(); len(errs) > 0 {
		for _, e := range errs {
			if !strings.HasPrefix(e.Error(), "WARNING:") {
				return nil, fmt.Errorf("config validation: %v", e)
			}
		}
	}
	return &cfg, nil
}

// RunStatus is the GET /run/status response.
type RunStatus struct {
	State   string                  `json:"state"`
	RunID   string                  `json:"run_id,omitempty"`
	Uptime  float64                 `json:"uptime_seconds,omitempty"`
	Workers map[string]WorkerStatus `json:"workers,omitempty"`
}

// WorkerStatus holds per-worker live counters.
type WorkerStatus struct {
	Sent     uint64  `json:"sent"`
	Received uint64  `json:"received"`
	Errors   uint64  `json:"errors"`
	Rate     float64 `json:"actual_rate"`
}

// RunSnapshot is the GET /run response.
type RunSnapshot struct {
	State   string                      `json:"state"`
	RunID   string                      `json:"run_id"`
	Config  *config.Config              `json:"config"`
	Workers map[string][]WorkerSnapshot `json:"workers"`
	Metrics MetricsSnapshot             `json:"metrics"`
}

// WorkerSnapshot holds per-channel-instance live counters.
type WorkerSnapshot struct {
	ID       string  `json:"id"`
	Worker   string  `json:"worker"`
	Channel  string  `json:"channel"`
	Sent     uint64  `json:"sent"`
	Received uint64  `json:"received"`
	Errors   uint64  `json:"errors"`
	Rate     float64 `json:"rate"`
}

// MetricsSnapshot holds run-wide aggregates.
type MetricsSnapshot struct {
	TotalSent        uint64  `json:"total_sent"`
	TotalReceived    uint64  `json:"total_received"`
	TotalErrors      uint64  `json:"total_errors"`
	TotalLost        uint64  `json:"total_lost"`
	TotalCorrupted   uint64  `json:"total_corrupted"`
	TotalDuplicated  uint64  `json:"total_duplicated"`
	FanoutDeliveries uint64  `json:"fanout_deliveries"`
	FanoutDropped    uint64  `json:"fanout_dropped"`
	UptimeSeconds    float64 `json:"uptime_seconds"`
}
