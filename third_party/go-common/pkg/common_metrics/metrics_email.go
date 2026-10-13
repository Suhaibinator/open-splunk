// Package common_metrics: Email service-specific Prometheus metrics registration and methods
package common_metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Email metrics - add these fields to CommonMetricsHandler struct
// emailSentTotal         *prometheus.CounterVec
// emailSendDuration      *prometheus.HistogramVec
// emailTemplateErrors    *prometheus.CounterVec

// registerEmailMetrics performs its package-specific work in the metrics instrumentation and export flow.
func (h *CommonMetricsHandler) registerEmailMetrics(namespace string) {
	factory := promauto.With(h.registry)

	h.emailSentTotal = factory.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "email_sent_total",
			Help:      "Total number of emails sent",
		},
		[]string{"provider", "type", "status"}, // provider: "gmail", "postal", "resend"; type: "verification", "password_reset", "notification"; status: "success", "failure"
	)

	h.emailSendDuration = factory.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: namespace,
			Name:      "email_send_duration_seconds",
			Help:      "Time taken to send emails in seconds",
			Buckets:   []float64{0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0},
		},
		[]string{"provider"},
	)

	h.emailTemplateErrors = factory.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "email_template_errors_total",
			Help:      "Total number of email template rendering errors",
		},
		[]string{"category"}, // category: "auth", "notification", "admin"
	)
}

// RecordEmailSent performs its package-specific work in the metrics instrumentation and export flow.
func (h *CommonMetricsHandler) RecordEmailSent(provider string, emailType string, status string, duration float64) {
	if h == nil || !h.IsEnabled() {
		return
	}

	h.emailSentTotal.WithLabelValues(provider, emailType, status).Inc()
	h.emailSendDuration.WithLabelValues(provider).Observe(duration)
}

// RecordEmailTemplateError records template rendering errors.

// RecordEmailTemplateError performs its package-specific work in the metrics instrumentation and export flow.
func (h *CommonMetricsHandler) RecordEmailTemplateError(category string) {
	if h == nil || !h.IsEnabled() {
		return
	}

	h.emailTemplateErrors.WithLabelValues(category).Inc()
}
