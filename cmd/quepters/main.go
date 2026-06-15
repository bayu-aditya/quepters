// Command quepters runs the message broker-to-gRPC bridge.
package main

import (
	"context"
	"flag"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/sirupsen/logrus"

	"github.com/bayu-aditya/quepters/internal/config"
	"github.com/bayu-aditya/quepters/internal/health"
	"github.com/bayu-aditya/quepters/internal/metrics"
	"github.com/bayu-aditya/quepters/internal/runtime"
)

func main() {
	configPath := flag.String("config", "config.yaml", "path to the configuration file")
	flag.Parse()

	log := newLogger()

	if err := run(*configPath, log); err != nil {
		log.WithError(err).Fatal("quepters exited with error")
	}
}

func run(configPath string, log *logrus.Logger) error {
	cfg, err := config.Load(configPath)
	if err != nil {
		return err
	}
	if lvl, err := logrus.ParseLevel(cfg.LogLevel); err == nil {
		log.SetLevel(lvl)
	}

	// procCtx scopes message processing and is cancelled only after graceful
	// shutdown completes, so in-flight messages can drain first.
	procCtx, procCancel := context.WithCancel(context.Background())
	defer procCancel()

	reg := prometheus.NewRegistry()
	m := metrics.New(reg)

	rt := runtime.New(log, m, runtime.DefaultGracePeriod)
	if err := rt.Start(procCtx, cfg); err != nil {
		return err
	}

	// The config watcher runs under serveCtx, which is cancelled on signal so it
	// stops alongside the adapters.
	serveCtx, serveCancel := context.WithCancel(context.Background())
	defer serveCancel()

	// A single HTTP server hosts both /metrics and /health.
	checker := health.New()
	httpSrv := startHTTPServer(cfg.Server.HTTPAddr, reg, checker, log)

	go func() {
		if err := config.Watch(serveCtx, configPath, log, rt.Reload); err != nil {
			log.WithError(err).Error("config watcher stopped")
		}
	}()

	waitForSignal(log)

	// Graceful shutdown: drain adapters first (in-flight messages complete),
	// then stop the server and release the processing context.
	log.Info("shutdown signal received; draining")
	checker.SetServing(false)
	rt.Shutdown()
	serveCancel()
	shutdownHTTPServer(httpSrv, log)
	return nil
}

func newLogger() *logrus.Logger {
	log := logrus.New()
	log.SetFormatter(&logrus.JSONFormatter{})
	log.SetOutput(os.Stdout)
	return log
}

func startHTTPServer(addr string, reg *prometheus.Registry, checker *health.Checker, log *logrus.Logger) *http.Server {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
	mux.Handle("/health", checker)
	srv := &http.Server{Addr: addr, Handler: mux}
	go func() {
		log.WithField("addr", addr).Info("http server listening (/metrics, /health)")
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.WithError(err).Error("http server stopped")
		}
	}()
	return srv
}

func shutdownHTTPServer(srv *http.Server, log *logrus.Logger) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		log.WithError(err).Warn("http server shutdown error")
	}
}

func waitForSignal(log *logrus.Logger) {
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	sig := <-sigCh
	log.WithField("signal", sig.String()).Info("received signal")
}
