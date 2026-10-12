package notification

import "github.com/Suhaibinator/go-common/pkg/common_metrics"

// InboxMetrics instruments candidate work independently of count completion.
type InboxMetrics interface{ RecordNotificationInboxCandidatesExamined(string, string, int) }
type CountMetrics interface{ RecordNotificationCountCompletion(string) }
type BatchMetrics interface{ RecordNotificationEmitBatchRecipients(string, int) }
type DeferralMetrics interface{ RecordNotificationDeferral(string) }

// AllMetrics is the complete optional telemetry contract. Dependencies.Metrics
// also accepts nil or any implementation of the four-method Metrics minimum;
// individual optional capabilities can be implemented independently.
type AllMetrics interface {
	Metrics
	InboxMetrics
	CountMetrics
	BatchMetrics
	DeferralMetrics
	common_metrics.NotificationFanoutMetrics
}

var _ AllMetrics = (*common_metrics.CommonMetricsHandler)(nil)

type notificationTelemetry struct {
	inbox    InboxMetrics
	count    CountMetrics
	batch    BatchMetrics
	deferral DeferralMetrics
	fanout   common_metrics.NotificationFanoutMetrics
}

func (h *Handler) setMetrics(metrics Metrics) {
	h.deps.Metrics = metrics
	h.telemetry.inbox, _ = metrics.(InboxMetrics)
	h.telemetry.count, _ = metrics.(CountMetrics)
	h.telemetry.batch, _ = metrics.(BatchMetrics)
	h.telemetry.deferral, _ = metrics.(DeferralMetrics)
	h.telemetry.fanout, _ = metrics.(common_metrics.NotificationFanoutMetrics)
}
