// Package runtime builds connectors and adapters from configuration, runs them,
// and applies configuration hot reloads.
package runtime

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/bayu-aditya/quepters/internal/adapter"
	"github.com/bayu-aditya/quepters/internal/config"
	"github.com/bayu-aditya/quepters/internal/connector"
	natsin "github.com/bayu-aditya/quepters/internal/connector/input/nats"
	grpcout "github.com/bayu-aditya/quepters/internal/connector/output/grpc"
	"github.com/bayu-aditya/quepters/internal/metrics"
)

// DefaultGracePeriod is the time graceful shutdown waits for in-flight messages.
const DefaultGracePeriod = 10 * time.Second

// Runtime owns the running set of adapters and supports swapping it on reload.
type Runtime struct {
	log     *logrus.Logger
	metrics *metrics.Metrics
	grace   time.Duration

	// procCtx scopes message processing; it stays alive through graceful
	// shutdown so in-flight forwarding can complete.
	procCtx context.Context

	mu  sync.Mutex
	set *adapterSet
}

// adapterSet is one generation of running adapters.
type adapterSet struct {
	adapters      []*adapter.Adapter
	consumeCancel context.CancelFunc
	wg            sync.WaitGroup
}

// New creates a Runtime. A zero grace falls back to DefaultGracePeriod.
func New(log *logrus.Logger, m *metrics.Metrics, grace time.Duration) *Runtime {
	if grace <= 0 {
		grace = DefaultGracePeriod
	}
	return &Runtime{log: log, metrics: m, grace: grace}
}

// Start builds and launches the adapters described by cfg. procCtx scopes
// in-flight message processing for the lifetime of the process.
func (r *Runtime) Start(procCtx context.Context, cfg *config.Config) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.procCtx = procCtx

	adapters, err := r.build(procCtx, cfg)
	if err != nil {
		return err
	}
	r.set = r.launch(adapters)
	r.log.WithField("adapters", len(adapters)).Info("runtime started")
	return nil
}

// Reload swaps in a new adapter set built from cfg. The new set is built before
// the old one is stopped; if the build fails the current set keeps running.
func (r *Runtime) Reload(cfg *config.Config) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.applyLogLevel(cfg)

	adapters, err := r.build(r.procCtx, cfg)
	if err != nil {
		r.log.WithError(err).Error("reload failed; keeping current configuration")
		return
	}

	if r.set != nil {
		r.stop(r.set)
	}
	r.set = r.launch(adapters)
	r.log.WithField("adapters", len(adapters)).Info("runtime reloaded")
}

// Shutdown stops the running adapters, waiting up to the grace period for
// in-flight messages to complete.
func (r *Runtime) Shutdown() {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.set != nil {
		r.stop(r.set)
		r.set = nil
	}
	r.log.Info("runtime stopped")
}

// build constructs (but does not start) the adapters for cfg. On any error the
// connectors created so far are closed before returning.
func (r *Runtime) build(ctx context.Context, cfg *config.Config) ([]*adapter.Adapter, error) {
	var adapters []*adapter.Adapter
	for name, def := range cfg.Adapters {
		in, out, err := r.buildConnectors(ctx, cfg, def)
		if err != nil {
			for _, a := range adapters {
				_ = a.Close()
			}
			return nil, fmt.Errorf("adapter %q: %w", name, err)
		}
		adapters = append(adapters, adapter.New(name, in, out, r.metrics.Adapter(name), r.log))
	}
	return adapters, nil
}

func (r *Runtime) buildConnectors(ctx context.Context, cfg *config.Config, def config.Adapter) (connector.InputConnector, connector.OutputConnector, error) {
	natsConn := cfg.Connectors[def.Input.NATS.ConnectorID].NATS
	in, err := natsin.New(ctx, *natsConn, *def.Input.NATS, r.log)
	if err != nil {
		return nil, nil, fmt.Errorf("build input: %w", err)
	}

	grpcConn := cfg.Connectors[def.Output.GRPC.ConnectorID].GRPC
	out, err := grpcout.New(*grpcConn)
	if err != nil {
		_ = in.Close()
		return nil, nil, fmt.Errorf("build output: %w", err)
	}
	return in, out, nil
}

// launch starts the goroutines for a set of adapters.
func (r *Runtime) launch(adapters []*adapter.Adapter) *adapterSet {
	consumeCtx, cancel := context.WithCancel(context.Background())
	set := &adapterSet{adapters: adapters, consumeCancel: cancel}

	for _, a := range adapters {
		set.wg.Add(1)
		go func(a *adapter.Adapter) {
			defer set.wg.Done()
			if err := a.Run(consumeCtx, r.procCtx); err != nil {
				r.log.WithError(err).WithField("adapter", a.Name()).Error("adapter run failed")
			}
		}(a)
	}
	return set
}

// stop gracefully shuts a set down: it halts new pulls, waits up to the grace
// period for in-flight messages, then closes the connectors.
func (r *Runtime) stop(set *adapterSet) {
	set.consumeCancel()

	if waitGroup(&set.wg, r.grace) {
		r.log.Debug("all adapters drained within grace period")
	} else {
		r.log.Warn("grace period elapsed; closing connectors with messages still in flight")
	}

	for _, a := range set.adapters {
		if err := a.Close(); err != nil {
			r.log.WithError(err).WithField("adapter", a.Name()).Warn("error closing adapter")
		}
	}
}

func (r *Runtime) applyLogLevel(cfg *config.Config) {
	if lvl, err := logrus.ParseLevel(cfg.LogLevel); err == nil {
		r.log.SetLevel(lvl)
	}
}

// waitGroup waits for wg with a timeout, returning true if it finished in time.
func waitGroup(wg *sync.WaitGroup, timeout time.Duration) bool {
	done := make(chan struct{})
	go func() {
		wg.Wait()
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
