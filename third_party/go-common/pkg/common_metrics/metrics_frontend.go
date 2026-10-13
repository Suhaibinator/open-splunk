// Package common_metrics: Frontend-specific Prometheus metrics registration and methods
package common_metrics

import (
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// FrontendMetricsHandler defines structured state used in the metrics instrumentation and export flow.
//
// Used in flow:
// - FrontendMetricsHandler is read or populated by backend components in this package to move requests through this stage.
type FrontendMetricsHandler struct {
	// registry is the Prometheus registry for metric registration
	registry *prometheus.Registry

	// Faro measurements metrics
	// faroMeasurementsTotal counts all measurements by type and name
	faroMeasurementsTotal *prometheus.CounterVec
	// faroMeasurementValues records measurement values as a histogram
	faroMeasurementValues *prometheus.HistogramVec

	// Web Vitals specific metrics
	// faroWebVitalsFCP tracks First Contentful Paint timing
	faroWebVitalsFCP *prometheus.HistogramVec
	// faroWebVitalsTTFB tracks Time to First Byte
	faroWebVitalsTTFB *prometheus.HistogramVec
	// faroWebVitalsLCP tracks Largest Contentful Paint timing
	faroWebVitalsLCP *prometheus.HistogramVec
	// faroWebVitalsFID tracks First Input Delay
	faroWebVitalsFID *prometheus.HistogramVec
	// faroWebVitalsCLS tracks Cumulative Layout Shift score
	faroWebVitalsCLS *prometheus.HistogramVec
	// faroWebVitalsINP tracks Interaction to Next Paint timing
	faroWebVitalsINP *prometheus.HistogramVec

	// Resource timing metrics
	// faroResourceLoadDuration tracks how long resources take to load
	faroResourceLoadDuration *prometheus.HistogramVec

	// Faro exceptions metrics
	// faroExceptionsTotal counts JavaScript exceptions by type
	faroExceptionsTotal *prometheus.CounterVec

	// Faro events metrics
	// faroEventsTotal counts user interaction events
	faroEventsTotal *prometheus.CounterVec

	// Faro logs metrics
	// faroLogsTotal counts frontend log messages by level
	faroLogsTotal *prometheus.CounterVec

	// Faro traces metrics
	// faroTracesTotal counts total number of traces
	faroTracesTotal prometheus.Counter
	// faroTraceDuration tracks trace execution time
	faroTraceDuration *prometheus.HistogramVec

	// pagePathMu guards the bounded set of distinct page_path label values.
	pagePathMu sync.Mutex
	// seenPagePaths tracks the distinct page_path values already emitted as labels.
	// The page URL arrives on an unauthenticated endpoint, so once faroMaxPagePaths
	// distinct paths are seen every new path collapses to "other" to bound cardinality.
	seenPagePaths map[string]struct{}
}

// faroMaxPagePaths caps the number of distinct page_path label values emitted over the
// process lifetime. The value derives from an untrusted, unauthenticated page URL, so
// without a cap an attacker could grow the Prometheus series count without bound.
const faroMaxPagePaths = 200

// resourceTimingBuckets defines histogram buckets for resource load times in milliseconds.
// These buckets are optimized for typical web resource loading patterns:
// very fast (50-200ms), fast (200-800ms), moderate (800-2000ms), slow (2000-5000ms), very slow (5000ms+)
var resourceTimingBuckets = []float64{50, 100, 200, 300, 500, 800, 1000, 1500, 2000, 3000, 5000, 10000}

// NewFrontendMetricsHandler performs its package-specific work in the metrics instrumentation and export flow.
func NewFrontendMetricsHandler(metricsHandler MetricsHandlerInterface) *FrontendMetricsHandler {
	// If metricsHandler is nil, return nil
	if metricsHandler == nil {
		return nil
	}

	// Use the same registry as the main metrics handler
	registry := metricsHandler.GetRegistry()

	h := &FrontendMetricsHandler{
		registry: registry,
	}

	// Create and register frontend metrics
	h.registerFrontendMetrics(metricsHandler.GetNamespace())

	return h
}

// BoundPagePath returns pagePath when it is already known or the distinct-path budget
// still has room, otherwise "other". It bounds the cardinality of the page_path label,
// whose value derives from an untrusted, unauthenticated page URL. Safe on a nil
// receiver.
//
// Parameters:
// - pagePath (string): The query/hash-stripped page path derived from the payload.
//
// Returns:
// - result1 (string): pagePath itself, or "other" once the distinct-path cap is reached.
func (h *FrontendMetricsHandler) BoundPagePath(pagePath string) string {
	if h == nil {
		return pagePath
	}

	h.pagePathMu.Lock()
	defer h.pagePathMu.Unlock()

	if _, ok := h.seenPagePaths[pagePath]; ok {
		return pagePath
	}
	if len(h.seenPagePaths) >= faroMaxPagePaths {
		return "other"
	}
	if h.seenPagePaths == nil {
		h.seenPagePaths = make(map[string]struct{}, faroMaxPagePaths)
	}
	h.seenPagePaths[pagePath] = struct{}{}
	return pagePath
}

// registerFrontendMetrics performs its package-specific work in the metrics instrumentation and export flow.
func (h *FrontendMetricsHandler) registerFrontendMetrics(namespace string) {
	factory := promauto.With(h.registry)

	// Faro measurements
	h.faroMeasurementsTotal = factory.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "faro_measurements_total",
			Help:      "Total number of Faro measurements by type and name",
		},
		[]string{"type", "name", "app", "browser"},
	)

	h.faroMeasurementValues = factory.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: namespace,
			Name:      "faro_measurement_values",
			Help:      "Values of Faro measurements",
			Buckets:   prometheus.ExponentialBuckets(0.001, 2, 15), // From 1ms to ~16s
		},
		[]string{"name", "type", "app", "browser", "page_path"},
	)

	// Web Vitals specific metrics with custom buckets
	h.faroWebVitalsFCP = factory.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: namespace,
			Name:      "faro_web_vitals_fcp_ms",
			Help:      "First Contentful Paint in milliseconds",
			Buckets:   []float64{100, 200, 300, 500, 800, 1000, 1500, 2000, 3000, 5000, 8000},
		},
		[]string{"app", "browser", "page_path"},
	)

	h.faroWebVitalsTTFB = factory.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: namespace,
			Name:      "faro_web_vitals_ttfb_ms",
			Help:      "Time to First Byte in milliseconds",
			Buckets:   []float64{50, 100, 200, 300, 500, 800, 1000, 1500, 2000},
		},
		[]string{"app", "browser", "page_path"},
	)

	h.faroWebVitalsLCP = factory.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: namespace,
			Name:      "faro_web_vitals_lcp_ms",
			Help:      "Largest Contentful Paint in milliseconds",
			Buckets:   []float64{500, 1000, 1500, 2000, 2500, 3000, 4000, 5000, 8000, 10000},
		},
		[]string{"app", "browser", "page_path"},
	)

	h.faroWebVitalsFID = factory.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: namespace,
			Name:      "faro_web_vitals_fid_ms",
			Help:      "First Input Delay in milliseconds",
			Buckets:   []float64{5, 10, 50, 100, 200, 300, 500, 800, 1000, 2000, 5000},
		},
		[]string{"app", "browser", "page_path"},
	)

	h.faroWebVitalsCLS = factory.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: namespace,
			Name:      "faro_web_vitals_cls",
			Help:      "Cumulative Layout Shift (unitless)",
			Buckets:   []float64{0.01, 0.05, 0.1, 0.15, 0.2, 0.25, 0.3, 0.5, 1.0},
		},
		[]string{"app", "browser", "page_path"},
	)

	h.faroWebVitalsINP = factory.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: namespace,
			Name:      "faro_web_vitals_inp_ms",
			Help:      "Interaction to Next Paint in milliseconds",
			Buckets:   []float64{50, 100, 200, 300, 500, 800, 1000, 1500, 2000, 3000, 5000},
		},
		[]string{"app", "browser", "page_path"},
	)

	// Resource timings
	h.faroResourceLoadDuration = factory.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: namespace,
			Name:      "faro_resource_load_duration_ms",
			Help:      "Duration of resource loading in milliseconds",
			Buckets:   resourceTimingBuckets,
		},
		[]string{"resource_type", "app", "browser", "page_path"},
	)

	// Faro exceptions
	h.faroExceptionsTotal = factory.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "faro_exceptions_total",
			Help:      "Total number of Faro exceptions",
		},
		[]string{"type", "app", "browser"},
	)

	// Faro events
	h.faroEventsTotal = factory.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "faro_events_total",
			Help:      "Total number of Faro events",
		},
		[]string{"name", "domain", "app"},
	)

	// Faro logs
	h.faroLogsTotal = factory.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "faro_logs_total",
			Help:      "Total number of Faro logs",
		},
		[]string{"level", "app"},
	)

	// Faro traces
	h.faroTracesTotal = factory.NewCounter(
		prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "faro_traces_total",
			Help:      "Total number of Faro traces",
		},
	)

	h.faroTraceDuration = factory.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: namespace,
			Name:      "faro_trace_duration_ms",
			Help:      "Duration of Faro traces in milliseconds",
			Buckets:   prometheus.ExponentialBuckets(1, 2, 10), // 1ms to ~1s
		},
		[]string{"name", "status"},
	)
}

// RecordFaroMeasurement performs its package-specific work in the metrics instrumentation and export flow.
func (h *FrontendMetricsHandler) RecordFaroMeasurement(measurementType, name, appName, browser, pagePath string, value float64) {
	if h == nil {
		return
	}
	h.faroMeasurementsTotal.WithLabelValues(measurementType, name, appName, browser).Inc()
	h.faroMeasurementValues.WithLabelValues(name, measurementType, appName, browser, pagePath).Observe(value)
}

// RecordFaroWebVital performs its package-specific work in the metrics instrumentation and export flow.
func (h *FrontendMetricsHandler) RecordFaroWebVital(vitalName string, appName, browser, pagePath string, value float64) {
	if h == nil {
		return
	}

	// Record in the general measurement metrics too
	h.faroMeasurementsTotal.WithLabelValues("web-vitals", vitalName, appName, browser).Inc()

	// Record in the specific web vital metric
	switch vitalName {
	case "fcp":
		h.faroWebVitalsFCP.WithLabelValues(appName, browser, pagePath).Observe(value)
	case "ttfb":
		h.faroWebVitalsTTFB.WithLabelValues(appName, browser, pagePath).Observe(value)
	case "lcp":
		h.faroWebVitalsLCP.WithLabelValues(appName, browser, pagePath).Observe(value)
	case "fid":
		h.faroWebVitalsFID.WithLabelValues(appName, browser, pagePath).Observe(value)
	case "cls":
		h.faroWebVitalsCLS.WithLabelValues(appName, browser, pagePath).Observe(value)
	case "inp":
		h.faroWebVitalsINP.WithLabelValues(appName, browser, pagePath).Observe(value)
	default:
		// For other web vitals not specifically handled, only use the general measurement metrics
		h.faroMeasurementValues.WithLabelValues(vitalName, "web-vitals", appName, browser, pagePath).Observe(value)
	}
}

// RecordFaroException performs its package-specific work in the metrics instrumentation and export flow.
func (h *FrontendMetricsHandler) RecordFaroException(exceptionType, appName, browser string) {
	if h == nil {
		return
	}
	h.faroExceptionsTotal.WithLabelValues(exceptionType, appName, browser).Inc()
}

// RecordFaroEvent performs its package-specific work in the metrics instrumentation and export flow.
func (h *FrontendMetricsHandler) RecordFaroEvent(eventName, domain, appName string) {
	if h == nil {
		return
	}
	h.faroEventsTotal.WithLabelValues(eventName, domain, appName).Inc()
}

// RecordFaroLog performs its package-specific work in the metrics instrumentation and export flow.
func (h *FrontendMetricsHandler) RecordFaroLog(level, appName string) {
	if h == nil {
		return
	}
	h.faroLogsTotal.WithLabelValues(level, appName).Inc()
}

// RecordFaroTrace performs its package-specific work in the metrics instrumentation and export flow.
func (h *FrontendMetricsHandler) RecordFaroTrace(name, status string, duration float64) {
	if h == nil {
		return
	}
	h.faroTracesTotal.Inc()
	h.faroTraceDuration.WithLabelValues(name, status).Observe(duration)
}

// RecordFaroResourceTiming performs its package-specific work in the metrics instrumentation and export flow.
func (h *FrontendMetricsHandler) RecordFaroResourceTiming(resourceType, appName, browser, pagePath string, duration float64) {
	if h == nil {
		return
	}
	// Assuming duration is already in milliseconds from Faro

	h.faroResourceLoadDuration.WithLabelValues(NormalizeFaroResourceType(resourceType), appName, browser, pagePath).Observe(duration)
}

// NormalizeFaroResourceType bounds browser resource initiator labels to a fixed
// vocabulary, including callers that record metrics without HTTP sanitization.
func NormalizeFaroResourceType(resourceType string) string {
	switch resourceType {
	case "audio", "beacon", "body", "css", "early-hint", "embed", "fetch", "iframe", "icon", "image", "img", "input", "link", "navigation", "object", "ping", "script", "track", "video", "xmlhttprequest", "other", "unknown":
		return resourceType
	default:
		return "other"
	}
}
