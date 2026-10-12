package common_metrics

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// NotificationFanoutMetrics is optional: custom metrics implementations need
// not add fan-out collectors to continue supporting inline notifications.
type NotificationFanoutMetrics interface {
	RecordNotificationFanoutBacklog(pending int64, oldestPendingAge time.Duration, countSaturated bool)
	RecordNotificationFanoutActivity(active int, paused bool)
	RecordNotificationFanoutBacklogSample(success bool)
	RecordNotificationFanoutEnqueue(outcome string)
	RecordNotificationFanoutStep(stage, outcome string, count int, duration, lateness, completionAge time.Duration)
}

var _ NotificationFanoutMetrics = (*CommonMetricsHandler)(nil)

func (h *CommonMetricsHandler) registerNotificationFanoutMetrics(namespace string) {
	f := promauto.With(h.registry)
	h.notificationFanoutPending = f.NewGauge(prometheus.GaugeOpts{Namespace: namespace, Name: "notification_fanout_pending", Help: "Pending durable emissions across the database, capped at notification_fanout_backlog_count_limit; a saturated sample is a lower bound; aggregate instances with max, not sum"})
	h.notificationFanoutPendingSaturated = f.NewGauge(prometheus.GaugeOpts{Namespace: namespace, Name: "notification_fanout_pending_count_saturated", Help: "Whether the database-wide pending count exceeds its sampling cap; aggregate instances with max, not sum"})
	h.notificationFanoutOldestPendingAge = f.NewGauge(prometheus.GaugeOpts{Namespace: namespace, Name: "notification_fanout_oldest_pending_age_seconds", Help: "Age of oldest pending durable emission across the database; aggregate instances with max, not sum"})
	h.notificationFanoutPaused = f.NewGauge(prometheus.GaugeOpts{Namespace: namespace, Name: "notification_fanout_paused", Help: "Whether application-wide fan-out is paused"})
	h.notificationFanoutActiveSteps = f.NewGauge(prometheus.GaugeOpts{Namespace: namespace, Name: "notification_fanout_active_steps", Help: "Currently active fan-out steps in this process"})
	h.notificationFanoutSampleTimestamp = f.NewGauge(prometheus.GaugeOpts{Namespace: namespace, Name: "notification_fanout_backlog_sample_timestamp_seconds", Help: "Unix time of the last successful backlog sample; zero before the first successful sample"})
	h.notificationFanoutSampleSuccess = f.NewGauge(prometheus.GaugeOpts{Namespace: namespace, Name: "notification_fanout_backlog_sample_success", Help: "Whether the most recent backlog sample succeeded"})
	h.notificationFanoutSampleErrors = f.NewCounter(prometheus.CounterOpts{Namespace: namespace, Name: "notification_fanout_backlog_sample_errors_total", Help: "Failed backlog samples; previous values remain until a successful sample"})
	h.notificationFanoutEnqueues = f.NewCounterVec(prometheus.CounterOpts{Namespace: namespace, Name: "notification_fanout_enqueues_total", Help: "Durable emission enqueue method outcomes; success does not imply ambient transaction commit"}, []string{"outcome"})
	h.notificationFanoutSteps = f.NewCounterVec(prometheus.CounterOpts{Namespace: namespace, Name: "notification_fanout_steps_total", Help: "Durable fan-out steps by bounded stage and outcome"}, []string{"stage", "outcome"})
	h.notificationFanoutStepDuration = f.NewHistogramVec(prometheus.HistogramOpts{Namespace: namespace, Name: "notification_fanout_step_duration_seconds", Help: "Duration of one durable fan-out step", Buckets: []float64{.01, .1, .5, 1, 5, 10, 30, 60}}, []string{"stage"})
	h.notificationFanoutRecipients = f.NewCounter(prometheus.CounterOpts{Namespace: namespace, Name: "notification_fanout_recipients_processed_total", Help: "Committed fan-out recipients processed, including authorization skips"})
	h.notificationFanoutLateness = f.NewHistogram(prometheus.HistogramOpts{Namespace: namespace, Name: "notification_fanout_claim_lateness_seconds", Help: "Claim lateness relative to scheduled durable fan-out step; not a queue size measurement", Buckets: []float64{.1, 1, 5, 30, 60, 300, 900, 3600}})
	h.notificationFanoutCompletion = f.NewHistogram(prometheus.HistogramOpts{Namespace: namespace, Name: "notification_fanout_completion_seconds", Help: "Time from durable enqueue to complete inbox materialization, excluding email delivery", Buckets: []float64{.1, .5, 1, 5, 30, 60, 300, 900, 3600, 86400}})
}

func (h *CommonMetricsHandler) RecordNotificationFanoutBacklog(pending int64, oldestPendingAge time.Duration, countSaturated bool) {
	if h == nil || !h.IsEnabled() {
		return
	}
	h.notificationFanoutPending.Set(float64(max(0, pending)))
	h.notificationFanoutPendingSaturated.Set(boolGaugeValue(countSaturated))
	h.notificationFanoutOldestPendingAge.Set(max(0, oldestPendingAge.Seconds()))
}

// RecordNotificationFanoutActivity records this process's active fan-out steps
// and whether application-wide fan-out is paused.
func (h *CommonMetricsHandler) RecordNotificationFanoutActivity(active int, paused bool) {
	if h == nil || !h.IsEnabled() {
		return
	}
	h.notificationFanoutPaused.Set(boolGaugeValue(paused))
	h.notificationFanoutActiveSteps.Set(float64(max(0, active)))
}

func boolGaugeValue(value bool) float64 {
	if value {
		return 1
	}
	return 0
}

func (h *CommonMetricsHandler) RecordNotificationFanoutBacklogSample(success bool) {
	if h == nil || !h.IsEnabled() {
		return
	}
	if success {
		h.notificationFanoutSampleTimestamp.SetToCurrentTime()
		h.notificationFanoutSampleSuccess.Set(1)
		return
	}
	h.notificationFanoutSampleSuccess.Set(0)
	h.notificationFanoutSampleErrors.Inc()
}

func (h *CommonMetricsHandler) RecordNotificationFanoutEnqueue(outcome string) {
	if h == nil || !h.IsEnabled() {
		return
	}
	switch outcome {
	case "created", "duplicate", "error":
	default:
		outcome = "other"
	}
	h.notificationFanoutEnqueues.WithLabelValues(outcome).Inc()
}

// RecordNotificationFanoutStep records one step outcome. A negative duration
// means no step ran (for example, a job exhausted at claim), so the outcome is
// counted without a step-duration sample. A negative completionAge means the
// emission did not complete.
func (h *CommonMetricsHandler) RecordNotificationFanoutStep(stage, outcome string, count int, duration, lateness, completionAge time.Duration) {
	if h == nil || !h.IsEnabled() {
		return
	}
	switch stage {
	case "snapshot", "batch", "recovery":
	default:
		stage = "other"
	}
	switch outcome {
	case "progress", "completed", "retry", "exhausted", "deferred", "lease_lost", "finish_failed":
	default:
		outcome = "other"
	}
	h.notificationFanoutSteps.WithLabelValues(stage, outcome).Inc()
	if duration >= 0 {
		h.notificationFanoutStepDuration.WithLabelValues(stage).Observe(duration.Seconds())
	}
	h.notificationFanoutLateness.Observe(max(0, lateness.Seconds()))
	if count > 0 {
		h.notificationFanoutRecipients.Add(float64(count))
	}
	if completionAge >= 0 {
		h.notificationFanoutCompletion.Observe(completionAge.Seconds())
	}
}
