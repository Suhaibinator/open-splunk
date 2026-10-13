// Package common_metrics: Attachment-specific Prometheus metrics registration and methods
package common_metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Attachment metrics - add these fields to CommonMetricsHandler struct
// attachmentUploadsTotal         *prometheus.CounterVec
// attachmentProcessingDuration   *prometheus.HistogramVec
// attachmentDeduplicationTotal   *prometheus.CounterVec
// attachmentBytesUploaded        *prometheus.CounterVec
// attachmentValidationFailures   *prometheus.CounterVec

// registerAttachmentMetrics performs its package-specific work in the metrics instrumentation and export flow.
func (h *CommonMetricsHandler) registerAttachmentMetrics(namespace string) {
	factory := promauto.With(h.registry)

	h.attachmentUploadsTotal = factory.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "attachment_uploads_total",
			Help:      "Total number of attachment uploads",
		},
		[]string{"object_type", "status"}, // object_type: "profile-picture", "attachment", "document"; status: "success", "failure", "validation_error"
	)

	h.attachmentProcessingDuration = factory.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: namespace,
			Name:      "attachment_processing_duration_seconds",
			Help:      "Time taken to process attachments in seconds",
			Buckets:   []float64{0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0},
		},
		[]string{"object_type"},
	)

	h.attachmentDeduplicationTotal = factory.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "attachment_deduplication_total",
			Help:      "Total number of deduplicated attachments",
		},
		[]string{"saved"}, // saved: "true", "false"
	)

	h.attachmentBytesUploaded = factory.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "attachment_bytes_uploaded",
			Help:      "Total bytes uploaded for attachments",
		},
		[]string{"object_type"},
	)

	h.attachmentValidationFailures = factory.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "attachment_validation_failures_total",
			Help:      "Total number of attachment validation failures",
		},
		[]string{"object_type", "reason"}, // reason: "too_large", "invalid_type", "virus_detected", "processing_error"
	)
}

// RecordAttachmentUpload performs its package-specific work in the metrics instrumentation and export flow.
func (h *CommonMetricsHandler) RecordAttachmentUpload(objectType string, status string) {
	if h == nil || !h.IsEnabled() {
		return
	}

	h.attachmentUploadsTotal.WithLabelValues(objectType, status).Inc()
}

// RecordAttachmentProcessing performs its package-specific work in the metrics instrumentation and export flow.
func (h *CommonMetricsHandler) RecordAttachmentProcessing(objectType string, duration float64) {
	if h == nil || !h.IsEnabled() {
		return
	}

	h.attachmentProcessingDuration.WithLabelValues(objectType).Observe(duration)
}

// RecordAttachmentDeduplication performs its package-specific work in the metrics instrumentation and export flow.
func (h *CommonMetricsHandler) RecordAttachmentDeduplication(saved bool) {
	if h == nil || !h.IsEnabled() {
		return
	}

	savedStr := "false"
	if saved {
		savedStr = "true"
	}
	h.attachmentDeduplicationTotal.WithLabelValues(savedStr).Inc()
}

// RecordAttachmentBytes performs its package-specific work in the metrics instrumentation and export flow.
func (h *CommonMetricsHandler) RecordAttachmentBytes(objectType string, bytes int64) {
	if h == nil || !h.IsEnabled() {
		return
	}

	h.attachmentBytesUploaded.WithLabelValues(objectType).Add(float64(bytes))
}

// RecordAttachmentValidationFailure tracks validation failures for attachments.

// RecordAttachmentValidationFailure performs its package-specific work in the metrics instrumentation and export flow.
func (h *CommonMetricsHandler) RecordAttachmentValidationFailure(objectType string, reason string) {
	if h == nil || !h.IsEnabled() {
		return
	}

	h.attachmentValidationFailures.WithLabelValues(objectType, reason).Inc()
}
