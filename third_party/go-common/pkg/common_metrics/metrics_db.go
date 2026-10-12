// Package common_metrics: Database-specific Prometheus metrics registration and methods
package common_metrics

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// registerDBMetrics performs its package-specific work in the metrics instrumentation and export flow.
func (h *CommonMetricsHandler) registerDBMetrics(namespace string) {
	factory := promauto.With(h.registry)
	h.dbConnectionsOpen = factory.NewGauge(
		prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "db_connections_open",
			Help:      "Number of open database connections",
		},
	)
	h.dbConnectionsIdle = factory.NewGauge(
		prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "db_connections_idle",
			Help:      "Number of idle database connections",
		},
	)
	h.dbQueryDuration = factory.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: namespace,
			Name:      "db_query_duration_seconds",
			Help:      "Duration of database queries in seconds",
			Buckets:   prometheus.DefBuckets,
		},
		[]string{"query_type"},
	)
	h.dbQueriesTotal = factory.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "db_queries_total",
			Help:      "Total number of database queries",
		},
		[]string{"query_type", "status"},
	)
	h.dbTransactionsOpen = factory.NewGauge(
		prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "db_transactions_open",
			Help:      "Number of open database transactions",
		},
	)
}

// RecordDBStats performs its package-specific work in the metrics instrumentation and export flow.
func (h *CommonMetricsHandler) RecordDBStats(openConnections, idleConnections int) {
	h.dbConnectionsOpen.Set(float64(openConnections))
	h.dbConnectionsIdle.Set(float64(idleConnections))
}

// RecordDBQuery records metrics for a database query execution.

// RecordDBQuery performs its package-specific work in the metrics instrumentation and export flow.
func (h *CommonMetricsHandler) RecordDBQuery(queryType string, duration time.Duration, success bool) {
	status := "success"
	if !success {
		status = "failure"
	}
	h.dbQueryDuration.WithLabelValues(queryType).Observe(duration.Seconds())
	h.dbQueriesTotal.WithLabelValues(queryType, status).Inc()
}
