// Package common_metrics: Auth-specific Prometheus metrics registration and methods
package common_metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// registerAuthMetrics performs its package-specific work in the metrics instrumentation and export flow.
func (h *CommonMetricsHandler) registerAuthMetrics(namespace string) {
	factory := promauto.With(h.registry)
	h.authAttemptsTotal = factory.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "auth_attempts_total",
			Help:      "Total number of authentication attempts",
		},
		[]string{"provider"},
	)
	h.authSuccessTotal = factory.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "auth_success_total",
			Help:      "Total number of successful authentications",
		},
		[]string{"provider"},
	)
	h.authFailureTotal = factory.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "auth_failure_total",
			Help:      "Total number of failed authentications",
		},
		[]string{"provider", "reason"},
	)
	h.authTokensGenerated = factory.NewCounter(
		prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "auth_tokens_generated",
			Help:      "Total number of authentication tokens generated",
		},
	)
	h.authTokensValidated = factory.NewCounter(
		prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "auth_tokens_validated",
			Help:      "Total number of authentication tokens validated",
		},
	)
	h.authTokensInvalidated = factory.NewCounter(
		prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "auth_tokens_invalidated",
			Help:      "Total number of authentication tokens invalidated",
		},
	)
	h.sessionRefreshesTotal = factory.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "session_refreshes_total",
			Help:      "Total number of probabilistic session token refresh attempts by outcome",
		},
		[]string{"status"},
	)
	h.authCacheHits = factory.NewCounter(
		prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "auth_cache_hits",
			Help:      "Total number of authentication cache hits",
		},
	)
	h.authCacheMisses = factory.NewCounter(
		prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "auth_cache_misses",
			Help:      "Total number of authentication cache misses",
		},
	)

	// Multi-auth metrics
	h.authPasswordAdditionsTotal = factory.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "auth_password_additions_total",
			Help:      "Total number of password additions to OAuth accounts",
		},
		[]string{"provider", "status"},
	)
	h.authMethodQueriesTotal = factory.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "auth_method_queries_total",
			Help:      "Total number of authentication method queries",
		},
		[]string{"has_password", "oauth_providers_count"},
	)
	h.emailVerificationRequestsTotal = factory.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "email_verification_requests_total",
			Help:      "Total number of email verification requests for existing users",
		},
		[]string{"status", "reason"},
	)

	// OTP/2FA metrics
	h.otpSetupAttemptsTotal = factory.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "otp_setup_attempts_total",
			Help:      "Total number of OTP setup attempts",
		},
		[]string{"status"},
	)
	h.otpVerificationAttemptsTotal = factory.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "otp_verification_attempts_total",
			Help:      "Total number of OTP verification attempts",
		},
		[]string{"context", "status"},
	)
	h.otpManagementOperationsTotal = factory.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "otp_management_operations_total",
			Help:      "Total number of OTP management operations",
		},
		[]string{"operation", "status"},
	)
	h.otpSetupDuration = factory.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: namespace,
			Name:      "otp_setup_duration_seconds",
			Help:      "Time taken to complete OTP setup process",
			Buckets:   []float64{0.1, 0.5, 1.0, 2.0, 5.0, 10.0, 30.0, 60.0},
		},
	)
}

// RecordAuthAttempt performs its package-specific work in the metrics instrumentation and export flow.
func (h *CommonMetricsHandler) RecordAuthAttempt(provider string, success bool, failureReason string) {
	h.authAttemptsTotal.WithLabelValues(provider).Inc()
	if success {
		h.authSuccessTotal.WithLabelValues(provider).Inc()
	} else {
		h.authFailureTotal.WithLabelValues(provider, failureReason).Inc()
	}
}

// RecordTokenGeneration performs its package-specific work in the metrics instrumentation and export flow.
func (h *CommonMetricsHandler) RecordTokenGeneration() {
	h.authTokensGenerated.Inc()
}

// RecordTokenValidation performs its package-specific work in the metrics instrumentation and export flow.
func (h *CommonMetricsHandler) RecordTokenValidation(valid bool) {
	h.authTokensValidated.Inc()
	if !valid {
		h.authTokensInvalidated.Inc()
	}
}

// RecordSessionRefresh performs its package-specific work in the metrics instrumentation and export flow.
//
// Parameters:
// - status (string): Refresh outcome; one of "refreshed", "skipped_revoked", or "error".
func (h *CommonMetricsHandler) RecordSessionRefresh(status string) {
	if h.sessionRefreshesTotal != nil {
		h.sessionRefreshesTotal.WithLabelValues(status).Inc()
	}
}

// RecordAuthCacheAccess performs its package-specific work in the metrics instrumentation and export flow.
func (h *CommonMetricsHandler) RecordAuthCacheAccess(hit bool) {
	if hit {
		h.authCacheHits.Inc()
	} else {
		h.authCacheMisses.Inc()
	}
}

// RecordPasswordAddition performs its package-specific work in the metrics instrumentation and export flow.
func (h *CommonMetricsHandler) RecordPasswordAddition(provider string, status string) {
	h.authPasswordAdditionsTotal.WithLabelValues(provider, status).Inc()
}

// RecordAuthMethodQuery performs its package-specific work in the metrics instrumentation and export flow.
func (h *CommonMetricsHandler) RecordAuthMethodQuery(hasPassword string, oauthProvidersCount string) {
	h.authMethodQueriesTotal.WithLabelValues(hasPassword, oauthProvidersCount).Inc()
}

// RecordEmailVerificationRequest performs its package-specific work in the metrics instrumentation and export flow.
func (h *CommonMetricsHandler) RecordEmailVerificationRequest(status string, reason string) {
	h.emailVerificationRequestsTotal.WithLabelValues(status, reason).Inc()
}

// RecordOTPSetupAttempt performs its package-specific work in the metrics instrumentation and export flow.
func (h *CommonMetricsHandler) RecordOTPSetupAttempt(status string) {
	if h.otpSetupAttemptsTotal != nil {
		h.otpSetupAttemptsTotal.WithLabelValues(status).Inc()
	}
}

// RecordOTPVerificationAttempt performs its package-specific work in the metrics instrumentation and export flow.
func (h *CommonMetricsHandler) RecordOTPVerificationAttempt(context string, status string) {
	if h.otpVerificationAttemptsTotal != nil {
		h.otpVerificationAttemptsTotal.WithLabelValues(context, status).Inc()
	}
}

// RecordOTPManagementOperation performs its package-specific work in the metrics instrumentation and export flow.
func (h *CommonMetricsHandler) RecordOTPManagementOperation(operation string, status string) {
	if h.otpManagementOperationsTotal != nil {
		h.otpManagementOperationsTotal.WithLabelValues(operation, status).Inc()
	}
}

// RecordOTPSetupDuration records the time taken to complete OTP setup.

// RecordOTPSetupDuration performs its package-specific work in the metrics instrumentation and export flow.
func (h *CommonMetricsHandler) RecordOTPSetupDuration(duration float64) {
	if h.otpSetupDuration != nil {
		h.otpSetupDuration.Observe(duration)
	}
}
