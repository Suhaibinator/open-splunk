package common_metrics

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

func (h *CommonMetricsHandler) registerNotificationMetrics(namespace string) {
	h.registerNotificationFanoutMetrics(namespace)
	factory := promauto.With(h.registry)
	h.notificationCountCompletions = factory.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace, Name: "notification_count_completions_total",
		Help: "Notification count calls by bounded completion reason",
	}, []string{"reason"})
	h.notificationDeliveriesTotal = factory.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace, Name: "notification_deliveries_total",
		Help: "Notification email delivery attempts by bounded final worker outcome, including lease completion failures",
	}, []string{"outcome"})
	h.notificationAttemptDuration = factory.NewHistogram(prometheus.HistogramOpts{
		Namespace: namespace, Name: "notification_attempt_duration_seconds",
		Help:    "Duration of one notification delivery attempt including its completion write",
		Buckets: []float64{0.01, 0.1, 0.5, 1, 5, 15, 30, 60, 300, 3600},
	})
	h.notificationSendsTotal = factory.NewCounter(prometheus.CounterOpts{
		Namespace: namespace, Name: "notification_email_sends_total",
		Help: "Notification email callbacks that returned success, regardless of subsequent job completion",
	})
	h.notificationEnqueueToSend = factory.NewHistogram(prometheus.HistogramOpts{
		Namespace: namespace, Name: "notification_enqueue_to_send_seconds",
		Help:    "Elapsed time from delivery job creation to successful email callback return",
		Buckets: []float64{1, 5, 15, 30, 60, 300, 900, 3600, 21600, 86400},
	})
	h.notificationSuppressionsTotal = factory.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace, Name: "notification_suppressions_total",
		Help: "Notification interruptions suppressed by bounded reason",
	}, []string{"reason"})
	h.notificationClaimLateness = factory.NewHistogram(prometheus.HistogramOpts{
		Namespace: namespace, Name: "notification_claim_lateness_seconds",
		Help:    "Lateness of claimed notification jobs relative to due time; this does not measure queue backlog",
		Buckets: []float64{0.1, 1, 5, 15, 30, 60, 300, 900, 3600},
	})
	h.notificationEmitBatchRecipients = factory.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: namespace, Name: "notification_emit_batch_recipients",
		Help:    "Candidate inputs passed to one EmitMany call by method result; returned does not imply source transaction commit",
		Buckets: []float64{1, 5, 10, 25, 50, 100, 250, 500, 1000, 5000},
	}, []string{"result"})
	h.notificationInboxCandidatesExamined = factory.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: namespace, Name: "notification_inbox_candidates_examined",
		Help:    "Candidate entries submitted to source evaluation by one inbox method call, including batches attempted before failure",
		Buckets: []float64{1, 10, 50, 100, 250, 500, 1000, 5000, 10000},
	}, []string{"operation", "result"})
	h.notificationDeferralsTotal = factory.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace, Name: "notification_delivery_deferrals_total",
		Help: "Successfully recorded delivery deferrals by bounded reason",
	}, []string{"reason"})
}

// RecordNotificationDelivery records an attempt without payload or recipient labels.
func (h *CommonMetricsHandler) RecordNotificationDelivery(outcome string, latency time.Duration) {
	if h == nil || !h.IsEnabled() {
		return
	}
	switch outcome {
	case "sent", "retry", "deferred", "failed", "cancelled", "finish_failed", "finish_failed_after_send", "send_unknown", "lease_lost":
	default:
		outcome = "other"
	}
	h.notificationDeliveriesTotal.WithLabelValues(outcome).Inc()
	if latency >= 0 {
		h.notificationAttemptDuration.Observe(latency.Seconds())
	}
}

// RecordNotificationEmailSend records external send acceptance separately from
// the job's fenced completion. A provider may still deliver more than once.
func (h *CommonMetricsHandler) RecordNotificationEmailSend(age time.Duration) {
	if h == nil || !h.IsEnabled() {
		return
	}
	h.notificationSendsTotal.Inc()
	if age >= 0 {
		h.notificationEnqueueToSend.Observe(age.Seconds())
	}
}

// RecordNotificationSuppression bounds label cardinality even for application types.
func (h *CommonMetricsHandler) RecordNotificationSuppression(reason string) {
	if h == nil || !h.IsEnabled() {
		return
	}
	switch reason {
	case "read", "resolved", "active", "preferences", "unauthorized", "cooldown", "stale":
	default:
		reason = "other"
	}
	h.notificationSuppressionsTotal.WithLabelValues(reason).Inc()
}

func (h *CommonMetricsHandler) RecordNotificationClaimLateness(age time.Duration) {
	if h == nil || !h.IsEnabled() {
		return
	}
	h.notificationClaimLateness.Observe(max(0, age.Seconds()))
}

// RecordNotificationEmitBatchRecipients observes a method call before any outer
// source transaction commits. The count is the input candidate count.
func (h *CommonMetricsHandler) RecordNotificationEmitBatchRecipients(result string, count int) {
	if h == nil || !h.IsEnabled() {
		return
	}
	if result != "returned" && result != "error" {
		result = "other"
	}
	h.notificationEmitBatchRecipients.WithLabelValues(result).Observe(float64(max(0, count)))
}

// RecordNotificationInboxCandidatesExamined includes work performed before an
// error or deadline. Only these fixed operation/result labels are admitted.
func (h *CommonMetricsHandler) RecordNotificationInboxCandidatesExamined(operation, result string, count int) {
	if h == nil || !h.IsEnabled() {
		return
	}
	if operation != "list" && operation != "count" {
		operation = "other"
	}
	if result != "returned" && result != "error" {
		result = "other"
	}
	h.notificationInboxCandidatesExamined.WithLabelValues(operation, result).Observe(float64(max(0, count)))
}

// RecordNotificationDeferral counts a persisted deferral, not a provider retry.
func (h *CommonMetricsHandler) RecordNotificationDeferral(reason string) {
	if h == nil || !h.IsEnabled() {
		return
	}
	switch reason {
	case "active", "policy_delay", "revision_changed", "disabled", "shutdown", "type_unavailable":
	default:
		reason = "other"
	}
	h.notificationDeferralsTotal.WithLabelValues(reason).Inc()
}

// RecordNotificationCountCompletion records why counting stopped without identity labels.
func (h *CommonMetricsHandler) RecordNotificationCountCompletion(reason string) {
	if h == nil || !h.IsEnabled() {
		return
	}
	switch reason {
	case "exhausted", "count_cap", "candidate_budget", "time_budget", "error":
	default:
		reason = "error"
	}
	h.notificationCountCompletions.WithLabelValues(reason).Inc()
}
