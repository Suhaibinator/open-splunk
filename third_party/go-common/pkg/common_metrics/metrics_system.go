// Package common_metrics: System-specific Prometheus metrics registration and methods
package common_metrics

import (
	"context"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// registerSystemMetrics performs its package-specific work in the metrics instrumentation and export flow.
func (h *CommonMetricsHandler) registerSystemMetrics(namespace string) {
	factory := promauto.With(h.registry)
	h.uptimeSeconds = factory.NewCounter(
		prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "uptime_seconds",
			Help:      "Uptime in seconds",
		},
	)

	// Register standard process collector (CPU, memory, FDs, etc.)
	processCollector := collectors.NewProcessCollector(collectors.ProcessCollectorOpts{
		Namespace: namespace,
	})
	h.registry.MustRegister(processCollector)

	// Register standard Go collector (runtime metrics: GC, goroutines, memory, etc.)
	goCollector := collectors.NewGoCollector(
		collectors.WithGoCollectorRuntimeMetrics(collectors.MetricsAll),
	)
	h.registry.MustRegister(goCollector)

	// Start loop to update uptime metric every 10 seconds.
	h.startUptimeTracker()
}

// startUptimeTracker performs its package-specific work in the metrics instrumentation and export flow.
func (h *CommonMetricsHandler) startUptimeTracker() {
	// Create a context that can be cancelled when the handler is closed

	ctx, cancel := context.WithCancel(context.Background())
	h.uptimeCancel = cancel

	ticker := time.NewTicker(10 * time.Second)
	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				h.uptimeSeconds.Add(10)
			case <-ctx.Done():
				return
			}
		}
	}()
}
