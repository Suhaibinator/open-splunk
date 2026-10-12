package common_metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

func (h *CommonMetricsHandler) registerChatRealtimeMetrics(namespace string) {
	factory := promauto.With(h.registry)
	h.chatRealtimeGrantsActive = factory.NewGauge(prometheus.GaugeOpts{
		Namespace: namespace, Name: "chat_realtime_grants_active",
		Help: "Number of currently authorized chat viewer grants on this instance",
	})
	h.chatRealtimeRenewalsTotal = factory.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace, Name: "chat_realtime_renewals_total",
		Help: "Chat viewer grant renewal decisions by bounded outcome",
	}, []string{"outcome"})
	h.chatRealtimeInvalidationsTotal = factory.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace, Name: "chat_realtime_invalidations_total",
		Help: "Chat realtime access invalidations by bounded scope",
	}, []string{"scope"})
	h.chatRecipientCacheTotal = factory.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace, Name: "chat_recipient_cache_total",
		Help: "Chat participant recipient cache observations by bounded outcome",
	}, []string{"outcome"})
}

// RecordChatRealtimeGrants sets the number of active viewer grants. It is an
// optional extension to MetricsHandlerInterface for chat-aware publishers.
func (h *CommonMetricsHandler) RecordChatRealtimeGrants(active int) {
	if h == nil || !h.IsEnabled() {
		return
	}
	if active < 0 {
		active = 0
	}
	h.chatRealtimeGrantsActive.Set(float64(active))
}

// RecordChatRealtimeRenewal counts allowed, denied, or failed renewals. Unknown
// outcomes are ignored to prevent caller-provided strings growing cardinality.
func (h *CommonMetricsHandler) RecordChatRealtimeRenewal(outcome string) {
	if h == nil || !h.IsEnabled() {
		return
	}
	switch outcome {
	case "allowed", "denied", "error":
		h.chatRealtimeRenewalsTotal.WithLabelValues(outcome).Inc()
	}
}

// RecordChatRealtimeInvalidation counts channel, user, or global invalidations.
// Identifiers must never appear in metric labels.
func (h *CommonMetricsHandler) RecordChatRealtimeInvalidation(scope string) {
	if h == nil || !h.IsEnabled() {
		return
	}
	switch scope {
	case "channel", "user", "all":
		h.chatRealtimeInvalidationsTotal.WithLabelValues(scope).Inc()
	}
}

// RecordChatRecipientCache counts cache hits, misses, and load errors. Unknown
// outcomes are ignored so this optional instrumentation stays bounded.
func (h *CommonMetricsHandler) RecordChatRecipientCache(outcome string) {
	if h == nil || !h.IsEnabled() {
		return
	}
	switch outcome {
	case "hit", "miss", "error":
		h.chatRecipientCacheTotal.WithLabelValues(outcome).Inc()
	}
}
