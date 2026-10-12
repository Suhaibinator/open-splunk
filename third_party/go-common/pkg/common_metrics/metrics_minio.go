// Package common_metrics: MinIO-specific Prometheus metrics registration and methods
package common_metrics

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// registerMinioMetrics performs its package-specific work in the metrics instrumentation and export flow.
func (h *CommonMetricsHandler) registerMinioMetrics(namespace string) {
	factory := promauto.With(h.registry)
	h.minioOperationsTotal = factory.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "minio_operations_total",
			Help:      "Total number of MinIO operations",
		},
		[]string{"operation", "status"},
	)
	h.minioOperationDuration = factory.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: namespace,
			Name:      "minio_operation_duration_seconds",
			Help:      "Duration of MinIO operations in seconds",
			Buckets:   prometheus.DefBuckets,
		},
		[]string{"operation"},
	)
	h.minioBytesTransferred = factory.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "minio_bytes_transferred",
			Help:      "Number of bytes transferred to/from MinIO",
		},
		[]string{"operation", "direction"},
	)
}

// RecordMinioOperation records metrics for a MinIO object storage operation.

// RecordMinioOperation performs its package-specific work in the metrics instrumentation and export flow.
func (h *CommonMetricsHandler) RecordMinioOperation(operation string, duration time.Duration, bytesTransferred int64, direction string, success bool) {
	status := "success"
	if !success {
		status = "failure"
	}
	h.minioOperationsTotal.WithLabelValues(operation, status).Inc()
	h.minioOperationDuration.WithLabelValues(operation).Observe(duration.Seconds())
	if bytesTransferred > 0 {
		h.minioBytesTransferred.WithLabelValues(operation, direction).Add(float64(bytesTransferred))
	}
}
