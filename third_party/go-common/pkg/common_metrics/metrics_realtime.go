// Package common_metrics: Realtime WebSocket-specific Prometheus metrics registration and methods
package common_metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// registerRealtimeMetrics performs its package-specific work in the metrics instrumentation and export flow.
//
// Every label used here is drawn from a closed set the server owns: disconnect reasons and subscribe
// error codes are package constants, and the topic namespace is the *registered* authorizer prefix,
// never the raw topic a client asked for. That keeps realtime cardinality bounded by application
// wiring instead of by traffic.
func (h *CommonMetricsHandler) registerRealtimeMetrics(namespace string) {
	factory := promauto.With(h.registry)
	h.registerChatRealtimeMetrics(namespace)

	h.realtimeConnectionsActive = factory.NewGauge(
		prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "realtime_connections_active",
			Help:      "Number of realtime websocket connections currently served by this instance",
		},
	)

	h.realtimeConnectionsTotal = factory.NewCounter(
		prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "realtime_connections_total",
			Help:      "Total number of realtime websocket connections accepted",
		},
	)

	h.realtimeDisconnectsTotal = factory.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "realtime_disconnects_total",
			Help:      "Total number of realtime websocket disconnects by reason",
		},
		[]string{"reason"}, // reason: "client_eof", "read_error", "backpressure", "pong_timeout", ...
	)

	h.realtimeSubscriptionsActive = factory.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "realtime_subscriptions_active",
			Help:      "Number of live realtime entity-topic subscriptions by registered topic namespace",
		},
		[]string{"namespace"}, // namespace: the registered authorizer prefix, e.g. "grading.submission"
	)

	h.realtimeSubscribeDenialsTotal = factory.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "realtime_subscribe_denials_total",
			Help:      "Total number of denied realtime topic subscribes by error code",
		},
		[]string{"code"}, // code: "invalid_topic", "reserved_topic", "unknown_namespace", "unauthorized_topic", "subscription_limit"
	)

	h.realtimeAuthorizerDuration = factory.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: namespace,
			Name:      "realtime_authorizer_duration_seconds",
			Help:      "Realtime topic authorizer latency in seconds by registered topic namespace",
			Buckets:   []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0},
		},
		[]string{"namespace"},
	)

	h.realtimeEventsPublishedTotal = factory.NewCounter(
		prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "realtime_events_published_total",
			Help:      "Total number of realtime frames published to the hub",
		},
	)

	h.realtimeEventsDeliveredTotal = factory.NewCounter(
		prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "realtime_events_delivered_total",
			Help:      "Total number of realtime frames enqueued onto a connection's send queue",
		},
	)

	h.realtimeEventsDroppedTotal = factory.NewCounter(
		prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "realtime_events_dropped_total",
			Help:      "Total number of realtime frames dropped because the target connection could not accept them",
		},
	)

	h.realtimePublishFailuresTotal = factory.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "realtime_publish_failures_total",
			Help:      "Total number of realtime frames that never reached fan-out, by cause",
		},
		[]string{"cause"}, // cause: "marshal_payload", "marshal_envelope", "marshal_command_result", "hub_closed"
	)
}

// RecordRealtimeConnectionOpened performs its package-specific work in the metrics instrumentation and export flow.
func (h *CommonMetricsHandler) RecordRealtimeConnectionOpened() {
	if h == nil || !h.IsEnabled() {
		return
	}

	h.realtimeConnectionsTotal.Inc()
	h.realtimeConnectionsActive.Inc()
}

// RecordRealtimeConnectionClosed performs its package-specific work in the metrics instrumentation and export flow.
func (h *CommonMetricsHandler) RecordRealtimeConnectionClosed(reason string) {
	if h == nil || !h.IsEnabled() {
		return
	}

	h.realtimeConnectionsActive.Dec()
	h.realtimeDisconnectsTotal.WithLabelValues(reason).Inc()
}

// RecordRealtimeSubscribed performs its package-specific work in the metrics instrumentation and export flow.
func (h *CommonMetricsHandler) RecordRealtimeSubscribed(namespace string) {
	if h == nil || !h.IsEnabled() {
		return
	}

	h.realtimeSubscriptionsActive.WithLabelValues(namespace).Inc()
}

// RecordRealtimeUnsubscribed performs its package-specific work in the metrics instrumentation and export flow.
func (h *CommonMetricsHandler) RecordRealtimeUnsubscribed(namespace string) {
	if h == nil || !h.IsEnabled() {
		return
	}

	h.realtimeSubscriptionsActive.WithLabelValues(namespace).Dec()
}

// RecordRealtimeSubscribeDenied performs its package-specific work in the metrics instrumentation and export flow.
func (h *CommonMetricsHandler) RecordRealtimeSubscribeDenied(errorCode string) {
	if h == nil || !h.IsEnabled() {
		return
	}

	h.realtimeSubscribeDenialsTotal.WithLabelValues(errorCode).Inc()
}

// RecordRealtimeAuthorizerCall performs its package-specific work in the metrics instrumentation and export flow.
func (h *CommonMetricsHandler) RecordRealtimeAuthorizerCall(namespace string, duration float64) {
	if h == nil || !h.IsEnabled() {
		return
	}

	h.realtimeAuthorizerDuration.WithLabelValues(namespace).Observe(duration)
}

// RecordRealtimePublish performs its package-specific work in the metrics instrumentation and export flow.
func (h *CommonMetricsHandler) RecordRealtimePublish(delivered int, dropped int) {
	if h == nil || !h.IsEnabled() {
		return
	}

	h.realtimeEventsPublishedTotal.Inc()
	if delivered > 0 {
		h.realtimeEventsDeliveredTotal.Add(float64(delivered))
	}
	if dropped > 0 {
		h.realtimeEventsDroppedTotal.Add(float64(dropped))
	}
}

// RecordRealtimePublishFailure performs its package-specific work in the metrics instrumentation and export flow.
//
// A failure counted here never became a frame on anyone's queue, so it is deliberately not part of
// the published/delivered/dropped arithmetic above: those describe the fan-out of a frame that was
// built, this describes one that was not.
//
// Parameters:
// - cause (string): Supplies the closed-set reason the frame was not published ("marshal_payload",
// "marshal_envelope", "marshal_command_result", "hub_closed").
func (h *CommonMetricsHandler) RecordRealtimePublishFailure(cause string) {
	if h == nil || !h.IsEnabled() {
		return
	}

	h.realtimePublishFailuresTotal.WithLabelValues(cause).Inc()
}
