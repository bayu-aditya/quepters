// Package metrics defines the per-adapter Prometheus metrics exposed on
// /metrics.
package metrics

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Status values reported by the quepters_adapter_status gauge.
const (
	StatusStopped float64 = 0
	StatusRunning float64 = 1
	StatusError   float64 = 2
)

// Metrics holds the registered metric vectors. One instance is shared by the
// whole process; per-adapter views are obtained via Adapter.
type Metrics struct {
	received  *prometheus.CounterVec
	forwarded *prometheus.CounterVec
	failed    *prometheus.CounterVec
	latency   *prometheus.HistogramVec
	status    *prometheus.GaugeVec
}

// New registers the metric vectors with reg and returns the collection. Passing
// a dedicated registry (rather than the global default) keeps tests isolated.
func New(reg prometheus.Registerer) *Metrics {
	factory := promauto.With(reg)
	return &Metrics{
		received: factory.NewCounterVec(prometheus.CounterOpts{
			Name: "quepters_messages_received_total",
			Help: "Total messages received from the input connector.",
		}, []string{"adapter"}),
		forwarded: factory.NewCounterVec(prometheus.CounterOpts{
			Name: "quepters_messages_forwarded_total",
			Help: "Total messages successfully forwarded and acknowledged.",
		}, []string{"adapter"}),
		failed: factory.NewCounterVec(prometheus.CounterOpts{
			Name: "quepters_messages_failed_total",
			Help: "Total messages that failed forwarding or were not acknowledged.",
		}, []string{"adapter"}),
		latency: factory.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "quepters_processing_latency_seconds",
			Help:    "End-to-end latency of forwarding a single message.",
			Buckets: prometheus.DefBuckets,
		}, []string{"adapter"}),
		status: factory.NewGaugeVec(prometheus.GaugeOpts{
			Name: "quepters_adapter_status",
			Help: "Current adapter status: 0=stopped, 1=running, 2=error.",
		}, []string{"adapter"}),
	}
}

// Adapter returns a view of the metrics scoped to a single adapter name.
func (m *Metrics) Adapter(name string) *AdapterMetrics {
	return &AdapterMetrics{
		received:  m.received.WithLabelValues(name),
		forwarded: m.forwarded.WithLabelValues(name),
		failed:    m.failed.WithLabelValues(name),
		latency:   m.latency.WithLabelValues(name),
		status:    m.status.WithLabelValues(name),
	}
}

// AdapterMetrics records metrics for one adapter. All methods are safe for
// concurrent use.
type AdapterMetrics struct {
	received  prometheus.Counter
	forwarded prometheus.Counter
	failed    prometheus.Counter
	latency   prometheus.Observer
	status    prometheus.Gauge
}

// IncReceived records a received message.
func (a *AdapterMetrics) IncReceived() { a.received.Inc() }

// IncForwarded records a successfully forwarded and acknowledged message.
func (a *AdapterMetrics) IncForwarded() { a.forwarded.Inc() }

// IncFailed records a message that failed or was not acknowledged.
func (a *AdapterMetrics) IncFailed() { a.failed.Inc() }

// ObserveLatency records the time taken to forward a message.
func (a *AdapterMetrics) ObserveLatency(d time.Duration) { a.latency.Observe(d.Seconds()) }

// SetStatus updates the adapter status gauge.
func (a *AdapterMetrics) SetStatus(status float64) { a.status.Set(status) }
