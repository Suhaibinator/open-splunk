// Package common_metrics: Contact form-specific Prometheus metrics registration and methods
package common_metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Contact form metrics - add these fields to CommonMetricsHandler struct
// contactSubmissionsTotal      *prometheus.CounterVec
// contactSpamDetectedTotal     prometheus.Counter
// contactProcessingDuration    prometheus.Histogram

// registerContactMetrics performs its package-specific work in the metrics instrumentation and export flow.
func (h *CommonMetricsHandler) registerContactMetrics(namespace string) {
	factory := promauto.With(h.registry)

	h.contactSubmissionsTotal = factory.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "contact_submissions_total",
			Help:      "Total number of contact form submissions",
		},
		[]string{"status"}, // status: "success", "failure", "rate_limited"
	)

	h.contactSpamDetectedTotal = factory.NewCounter(
		prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "contact_spam_detected_total",
			Help:      "Total number of contact form submissions detected as spam",
		},
	)

	h.contactProcessingDuration = factory.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: namespace,
			Name:      "contact_processing_duration_seconds",
			Help:      "Contact form processing duration in seconds",
			Buckets:   []float64{0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0},
		},
	)
}

// RecordContactSubmission performs its package-specific work in the metrics instrumentation and export flow.
func (h *CommonMetricsHandler) RecordContactSubmission(status string, duration float64) {
	if h == nil || !h.IsEnabled() {
		return
	}

	h.contactSubmissionsTotal.WithLabelValues(status).Inc()
	h.contactProcessingDuration.Observe(duration)
}

// RecordContactSpamDetected increments the counter for contact submissions detected as spam.

// RecordContactSpamDetected performs its package-specific work in the metrics instrumentation and export flow.
func (h *CommonMetricsHandler) RecordContactSpamDetected() {
	if h == nil || !h.IsEnabled() {
		return
	}

	h.contactSpamDetectedTotal.Inc()
}
