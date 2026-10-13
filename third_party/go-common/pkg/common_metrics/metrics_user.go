// Package common_metrics: User-specific Prometheus metrics registration and methods
package common_metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// User metrics - add these fields to CommonMetricsHandler struct
// userProfileUpdatesTotal   *prometheus.CounterVec
// userRegistrationsTotal    *prometheus.CounterVec
// userSettingsUpdatesTotal  prometheus.Counter

// registerUserMetrics performs its package-specific work in the metrics instrumentation and export flow.
func (h *CommonMetricsHandler) registerUserMetrics(namespace string) {
	factory := promauto.With(h.registry)

	h.userProfileUpdatesTotal = factory.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "user_profile_updates_total",
			Help:      "Total number of user profile updates",
		},
		[]string{"category"}, // category: "identity", "preferences", "avatar"
	)

	h.userRegistrationsTotal = factory.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "user_registrations_total",
			Help:      "Total number of user registrations",
		},
		[]string{"method"}, // method: "email", "oauth"
	)

	h.userSettingsUpdatesTotal = factory.NewCounter(
		prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "user_settings_updates_total",
			Help:      "Total number of user settings updates",
		},
	)
}

// RecordUserProfileUpdate performs its package-specific work in the metrics instrumentation and export flow.
func (h *CommonMetricsHandler) RecordUserProfileUpdate(category string) {
	if h == nil || !h.IsEnabled() {
		return
	}

	h.userProfileUpdatesTotal.WithLabelValues(category).Inc()
}

// RecordUserRegistration performs its package-specific work in the metrics instrumentation and export flow.
func (h *CommonMetricsHandler) RecordUserRegistration(method string) {
	if h == nil || !h.IsEnabled() {
		return
	}

	h.userRegistrationsTotal.WithLabelValues(method).Inc()
}

// RecordUserSettingsUpdate increments the counter for user settings updates.

// RecordUserSettingsUpdate performs its package-specific work in the metrics instrumentation and export flow.
func (h *CommonMetricsHandler) RecordUserSettingsUpdate() {
	if h == nil || !h.IsEnabled() {
		return
	}

	h.userSettingsUpdatesTotal.Inc()
}
