package reaper

// Metrics is the engine's view of instrumentation. Keeping it as a tiny
// interface (rather than importing Prometheus here) is deliberate: the engine
// stays dependency-free, and main wires a Prometheus-backed implementation. The
// no-op default means callers that don't care about metrics pay nothing.
type Metrics interface {
	IncReaped()   // a resource was reaped (or would be, in dry-run)
	IncErrors()   // a reap attempt failed
	IncInFlight() // a reap started
	DecInFlight() // a reap finished
}

// nopMetrics is the default when no Metrics is supplied.
type nopMetrics struct{}

func (nopMetrics) IncReaped()   {}
func (nopMetrics) IncErrors()   {}
func (nopMetrics) IncInFlight() {}
func (nopMetrics) DecInFlight() {}
