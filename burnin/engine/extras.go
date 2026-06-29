package engine

import (
	"github.com/kubemq-io/kubemq-gcp-pub-sub/burnin/metrics"
)

// metricsLatency is a thin interface over the latency accumulator used to
// extract percentiles for the worker with the most samples.
type metricsLatency struct {
	acc *metrics.LatencyAccumulator
}

func wrapLatency(acc *metrics.LatencyAccumulator) *metricsLatency {
	return &metricsLatency{acc: acc}
}

func (m *metricsLatency) Percentiles() (p50, p95, p99, p999 float64) {
	return m.acc.Percentiles()
}
