// Package common_metrics: Rate limiter-specific Prometheus metrics registration and methods
package common_metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Rate limiter metrics - add these fields to CommonMetricsHandler struct
// rateLimitThrottledTotal  *prometheus.CounterVec
// rateLimitAllowedTotal    *prometheus.CounterVec
// rateLimitCheckDuration   *prometheus.HistogramVec

// registerRateLimiterMetrics performs its package-specific work in the metrics instrumentation and export flow.
func (h *CommonMetricsHandler) registerRateLimiterMetrics(namespace string) {
	factory := promauto.With(h.registry)

	h.rateLimitThrottledTotal = factory.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "rate_limit_throttled_total",
			Help:      "Total number of requests throttled by rate limiter",
		},
		[]string{"action", "identifier_type"},
	)

	h.rateLimitAllowedTotal = factory.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "rate_limit_allowed_total",
			Help:      "Total number of requests allowed by rate limiter",
		},
		[]string{"action", "identifier_type"},
	)

	h.rateLimitCheckDuration = factory.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: namespace,
			Name:      "rate_limit_check_duration_seconds",
			Help:      "Duration of rate limit checks in seconds",
			Buckets:   []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0},
		},
		[]string{"action"},
	)
}

// RecordRateLimitCheck performs its package-specific work in the metrics instrumentation and export flow.
func (h *CommonMetricsHandler) RecordRateLimitCheck(action string, identifierType string, allowed bool, duration float64) {
	if h == nil || !h.IsEnabled() {
		return
	}

	// Record the duration of the check
	h.rateLimitCheckDuration.WithLabelValues(action).Observe(duration)

	// Record whether the request was allowed or throttled
	if allowed {
		h.rateLimitAllowedTotal.WithLabelValues(action, identifierType).Inc()
	} else {
		h.rateLimitThrottledTotal.WithLabelValues(action, identifierType).Inc()
	}
}

// RecordRateLimitThrottle performs its package-specific work in the metrics instrumentation and export flow.
func (h *CommonMetricsHandler) RecordRateLimitThrottle(action string, identifierType string) {
	if h == nil || !h.IsEnabled() {
		return
	}

	h.rateLimitThrottledTotal.WithLabelValues(action, identifierType).Inc()
}

// RecordRateLimitAllow is a convenience method for recording allowed requests.
// Use this when you only need to increment the allowed counter without recording

// RecordRateLimitAllow performs its package-specific work in the metrics instrumentation and export flow.
func (h *CommonMetricsHandler) RecordRateLimitAllow(action string, identifierType string) {
	if h == nil || !h.IsEnabled() {
		return
	}

	h.rateLimitAllowedTotal.WithLabelValues(action, identifierType).Inc()
}
