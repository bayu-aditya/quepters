// Package adapter contains the transport-agnostic dispatch logic that wires an
// InputConnector to an OutputConnector and records metrics for each message.
package adapter

import (
	"context"
	"sync"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/bayu-aditya/quepters/internal/connector"
	"github.com/bayu-aditya/quepters/internal/metrics"
)

// Adapter forwards every message from its input connector to its output
// connector, acknowledging the input only when the output acknowledges.
type Adapter struct {
	name     string
	input    connector.InputConnector
	output   connector.OutputConnector
	metrics  *metrics.AdapterMetrics
	log      *logrus.Entry
	inflight sync.WaitGroup
}

// New constructs an adapter. The metrics view and logger are expected to be
// pre-scoped to the adapter name by the caller.
func New(name string, in connector.InputConnector, out connector.OutputConnector, m *metrics.AdapterMetrics, log *logrus.Logger) *Adapter {
	return &Adapter{
		name:    name,
		input:   in,
		output:  out,
		metrics: m,
		log:     log.WithField("adapter", name),
	}
}

// Name returns the adapter's configured name.
func (a *Adapter) Name() string { return a.name }

// Run consumes messages until consumeCtx is cancelled, which stops the input
// from pulling new messages. Each message is processed with procCtx so in-flight
// forwarding can complete during graceful shutdown even after consumeCtx is
// cancelled. Run returns when the input connector's Consume loop returns.
func (a *Adapter) Run(consumeCtx, procCtx context.Context) error {
	a.metrics.SetStatus(metrics.StatusRunning)
	a.log.Info("adapter started")

	err := a.input.Consume(consumeCtx, func(_ context.Context, data []byte) (bool, error) {
		a.inflight.Add(1)
		defer a.inflight.Done()
		return a.process(procCtx, data)
	})

	if err != nil && consumeCtx.Err() == nil {
		// An error that is not the result of a normal shutdown.
		a.metrics.SetStatus(metrics.StatusError)
		a.log.WithError(err).Error("adapter stopped with error")
		return err
	}

	a.metrics.SetStatus(metrics.StatusStopped)
	a.log.Info("adapter stopped")
	return nil
}

// process forwards a single message and records metrics. It returns whether the
// message should be acknowledged.
func (a *Adapter) process(ctx context.Context, data []byte) (bool, error) {
	a.metrics.IncReceived()

	start := time.Now()
	ack, err := a.output.Forward(ctx, data)
	a.metrics.ObserveLatency(time.Since(start))

	if err != nil {
		a.metrics.IncFailed()
		a.log.WithError(err).Warn("forwarding failed; message will be redelivered")
		return false, err
	}
	if !ack {
		a.metrics.IncFailed()
		a.log.Warn("output did not acknowledge; message will be redelivered")
		return false, nil
	}

	a.metrics.IncForwarded()
	return true, nil
}

// WaitInflight blocks until all in-flight messages finish or timeout elapses,
// returning true if they all completed in time.
func (a *Adapter) WaitInflight(timeout time.Duration) bool {
	done := make(chan struct{})
	go func() {
		a.inflight.Wait()
		close(done)
	}()
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-done:
		return true
	case <-timer.C:
		return false
	}
}

// Close releases the adapter's connectors.
func (a *Adapter) Close() error {
	inErr := a.input.Close()
	outErr := a.output.Close()
	if inErr != nil {
		return inErr
	}
	return outErr
}
