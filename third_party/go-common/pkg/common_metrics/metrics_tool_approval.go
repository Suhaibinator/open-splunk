package common_metrics

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// registerToolApprovalMetrics registers bounded, label-free metrics for the approval-expiry worker.
func (h *CommonMetricsHandler) registerToolApprovalMetrics(namespace string) {
	factory := promauto.With(h.registry)

	h.toolApprovalExpirySweepDurationSeconds = factory.NewHistogram(prometheus.HistogramOpts{
		Namespace: namespace,
		Name:      "tool_approval_expiry_sweep_duration_seconds",
		Help:      "Duration of tool-approval expiry sweeps in seconds",
		Buckets:   []float64{0.01, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30},
	})
	h.toolApprovalExpiryCandidatesScanned = factory.NewCounter(prometheus.CounterOpts{
		Namespace: namespace,
		Name:      "tool_approval_expiry_candidates_scanned_total",
		Help:      "Total overdue tool-approval candidates scanned",
	})
	h.toolApprovalExpiryCandidatesResolved = factory.NewCounter(prometheus.CounterOpts{
		Namespace: namespace,
		Name:      "tool_approval_expiry_candidates_resolved_total",
		Help:      "Total overdue tool approvals handled without worker error, including guarded replica no-ops",
	})
	h.toolApprovalExpiryCandidatesFailed = factory.NewCounter(prometheus.CounterOpts{
		Namespace: namespace,
		Name:      "tool_approval_expiry_candidates_failed_total",
		Help:      "Total overdue tool approvals that a sweep could not expire",
	})
	h.toolApprovalExpiryBacklogPresent = factory.NewGauge(prometheus.GaugeOpts{
		Namespace: namespace,
		Name:      "tool_approval_expiry_backlog_present",
		Help:      "Whether overdue tool approvals remained after the most recent sweep (1 or 0)",
	})
	h.toolApprovalExpiryOldestOverdueSeconds = factory.NewGauge(prometheus.GaugeOpts{
		Namespace: namespace,
		Name:      "tool_approval_expiry_oldest_overdue_seconds",
		Help:      "Age in seconds of the oldest overdue tool approval observed by the most recent sweep",
	})
}

// RecordToolApprovalExpirySweep records one completed or interrupted expiry sweep. backlogPresent
// comes from the keyset page's limit+1 probe plus any candidate failures, so it requires no extra
// COUNT query over the backlog.
func (h *CommonMetricsHandler) RecordToolApprovalExpirySweep(
	duration time.Duration,
	scanned int,
	resolved int,
	failed int,
	oldestOverdue time.Duration,
	backlogPresent bool,
) {
	if h == nil || !h.IsEnabled() {
		return
	}
	h.toolApprovalExpirySweepDurationSeconds.Observe(duration.Seconds())
	if scanned > 0 {
		h.toolApprovalExpiryCandidatesScanned.Add(float64(scanned))
	}
	if resolved > 0 {
		h.toolApprovalExpiryCandidatesResolved.Add(float64(resolved))
	}
	if failed > 0 {
		h.toolApprovalExpiryCandidatesFailed.Add(float64(failed))
	}
	if backlogPresent {
		h.toolApprovalExpiryBacklogPresent.Set(1)
	} else {
		h.toolApprovalExpiryBacklogPresent.Set(0)
	}
	if oldestOverdue > 0 {
		h.toolApprovalExpiryOldestOverdueSeconds.Set(oldestOverdue.Seconds())
	} else {
		h.toolApprovalExpiryOldestOverdueSeconds.Set(0)
	}
}
