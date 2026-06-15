// Package health exposes an HTTP health endpoint suitable for Kubernetes
// liveness and readiness probes. It is an http.Handler so it can share a mux
// with the Prometheus /metrics endpoint.
package health

import (
	"net/http"
	"sync/atomic"
)

// Checker reports the process's serving status over HTTP. It responds 200 when
// serving and 503 otherwise. The zero value is not serving; use New.
type Checker struct {
	serving atomic.Bool
}

// New returns a Checker that starts in the serving state.
func New() *Checker {
	c := &Checker{}
	c.serving.Store(true)
	return c
}

// SetServing toggles the reported status.
func (c *Checker) SetServing(serving bool) {
	c.serving.Store(serving)
}

// ServeHTTP implements http.Handler.
func (c *Checker) ServeHTTP(w http.ResponseWriter, _ *http.Request) {
	if c.serving.Load() {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
		return
	}
	w.WriteHeader(http.StatusServiceUnavailable)
	_, _ = w.Write([]byte("not serving"))
}
