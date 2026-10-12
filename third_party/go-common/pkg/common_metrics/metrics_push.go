// Package common_metrics: PushGateway-related Prometheus metrics methods
package common_metrics

import (
	"context"
	"net/http"
	"time"

	"github.com/Suhaibinator/SRouter/pkg/logkeys"
	"github.com/Suhaibinator/go-common/internal/hotpoll"
	"github.com/Suhaibinator/go-common/pkg/config"
	appLogger "github.com/Suhaibinator/go-common/pkg/logger"
	"github.com/prometheus/client_golang/prometheus/push"
	"go.uber.org/zap"
)

// startPushing owns one loop even when export starts disabled, so enabling it is hot.
func (h *CommonMetricsHandler) startPushing() {
	h.pushCancelCtx, h.pushCancel = context.WithCancel(context.Background())
	h.pushDone = make(chan struct{})
	go func() {
		defer close(h.pushDone)
		hotpoll.Run(h.pushCancelCtx, func() time.Duration {
			cfg := h.provider()
			if !cfg.MetricsPushEnabled() {
				return 0
			}
			interval := cfg.MetricsPushInterval()
			if interval < 1 {
				return 15 * time.Second
			}
			return time.Duration(interval) * time.Second
		}, func() { _ = h.pushMetrics() })
	}()
}

// logPushFailure records a retryable export failure. Keeping the classification
// here makes the operational contract independently testable from the ticker.
func (h *CommonMetricsHandler) logPushFailure(cfg config.MetricsView, err error) {
	log := h.logger.Named(commonMetricsLoggerName)
	// The next interval retries automatically. A single failed push is degraded
	// telemetry, not a terminal server failure; sustained Warn volume is what
	// should alert operators.
	log.Warn("Failed to push metrics",
		zap.String(logkeys.Operation, "push_metrics"),
		zap.String(logkeys.Stage, "export"),
		zap.String(appLogger.KeyEndpoint, cfg.MetricsPushEndpoint()),
		appLogger.DurationSeconds(appLogger.KeyIntervalSeconds, time.Duration(cfg.MetricsPushInterval())*time.Second),
		zap.String(logkeys.Fallback, "retry_on_next_interval"),
		zap.Error(err),
	)
}

// pushMetrics pushes metrics to the Prometheus Pushgateway.

// pushMetrics performs its package-specific work in the metrics instrumentation and export flow.
func (h *CommonMetricsHandler) pushMetrics() error {
	cfg := h.provider()
	if !cfg.MetricsPushEnabled() {
		return nil
	}
	pusher := push.New(cfg.MetricsPushEndpoint(), cfg.MetricsPushJobName()).Gatherer(h.registry).
		Client(&http.Client{Timeout: 30 * time.Second})
	if cfg.MetricsPushBasicAuth() {
		pusher.BasicAuth(cfg.MetricsPushUsername(), cfg.MetricsPushPassword().StringValue())
	}
	ctx := h.pushCancelCtx
	if ctx == nil {
		ctx = context.Background()
	}
	if err := pusher.PushContext(ctx); err != nil {
		h.logPushFailure(cfg, err)
		return err
	}

	h.logger.Named(commonMetricsLoggerName).Debug("Successfully pushed metrics to Pushgateway")
	return nil
}
