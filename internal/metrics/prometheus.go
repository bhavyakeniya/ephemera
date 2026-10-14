// Package metrics provides a Prometheus-backed implementation of reaper.Metrics.
// It lives outside the engine on purpose: the engine depends only on the tiny
// reaper.Metrics interface, so swapping or dropping Prometheus never touches it.
package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	ctrlmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"

	"github.com/bhavyakeniya/ephemera/internal/reaper"
)

// Prometheus implements reaper.Metrics, registered with controller-runtime's
// registry so the collectors appear on the manager's /metrics endpoint.
type Prometheus struct {
	reaped   prometheus.Counter
	errors   prometheus.Counter
	inflight prometheus.Gauge
}

var _ reaper.Metrics = (*Prometheus)(nil)

// NewPrometheus builds and registers the collectors.
func NewPrometheus() *Prometheus {
	p := &Prometheus{
		reaped: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "ephemera_reaped_total",
			Help: "Total namespaces reaped (includes would-reaps in dry-run).",
		}),
		errors: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "ephemera_reap_errors_total",
			Help: "Total failed reap attempts.",
		}),
		inflight: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "ephemera_reap_in_flight",
			Help: "Reap operations currently in progress.",
		}),
	}
	ctrlmetrics.Registry.MustRegister(p.reaped, p.errors, p.inflight)
	return p
}

func (p *Prometheus) IncReaped()   { p.reaped.Inc() }
func (p *Prometheus) IncErrors()   { p.errors.Inc() }
func (p *Prometheus) IncInFlight() { p.inflight.Inc() }
func (p *Prometheus) DecInFlight() { p.inflight.Dec() }
