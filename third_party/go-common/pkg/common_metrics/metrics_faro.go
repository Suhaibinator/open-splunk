// Package common_metrics: Faro-specific Prometheus telemetry handler
package common_metrics

import (
	"bytes"
	"context"
	json "encoding/json/v2"
	"fmt"
	"net/url"
	"strconv"
	"time"

	"github.com/Suhaibinator/SRouter/pkg/logkeys"
	appLogger "github.com/Suhaibinator/go-common/pkg/logger"
	"go.uber.org/zap"
)

// FaroMetricsHandler defines structured state used in the metrics instrumentation and export flow.
//
// Used in flow:
// - FaroMetricsHandler is read or populated by backend components in this package to move requests through this stage.
type FaroMetricsHandler struct {
	logger          *zap.Logger
	frontendMetrics *FrontendMetricsHandler
}

// NewFaroMetricsHandler performs its package-specific work in the metrics instrumentation and export flow.
func NewFaroMetricsHandler(metricsHandler MetricsHandlerInterface, logger *zap.Logger) *FaroMetricsHandler {
	// Initialize the frontend metrics handler
	frontendMetrics := NewFrontendMetricsHandler(metricsHandler)

	return &FaroMetricsHandler{
		logger:          logger,
		frontendMetrics: frontendMetrics,
	}
}

// FaroPayload defines structured state used in the metrics instrumentation and export flow.
//
// Used in flow:
// - FaroPayload is read or populated by backend components in this package to move requests through this stage.
type FaroPayload struct {
	Measurements []FaroMeasurement `json:"measurements"`
	Logs         []FaroLog         `json:"logs"`
	Exceptions   []FaroException   `json:"exceptions"`
	Events       []FaroEvent       `json:"events"`
	Traces       any               `json:"traces"` // Can vary by Faro/OTel payload shape
	Meta         map[string]any    `json:"meta"`   // Flexible meta structure
}

// FaroMeasurement defines structured state used in the metrics instrumentation and export flow.
//
// Used in flow:
// - FaroMeasurement is read or populated by backend components in this package to move requests through this stage.
type FaroMeasurement struct {
	Type       string         `json:"type"`
	Values     map[string]any `json:"values"`
	Timestamp  FaroTimestamp  `json:"timestamp"` // Decodes int64/float64/ISO-8601-string shapes; see FaroTimestamp
	Attributes map[string]any `json:"attributes,omitempty"`
	Context    map[string]any `json:"context,omitempty"` // Some payloads use context instead of attributes
}

// FaroLog defines structured state used in the metrics instrumentation and export flow.
//
// Used in flow:
// - FaroLog is read or populated by backend components in this package to move requests through this stage.
type FaroLog struct {
	Message   string         `json:"message"`
	Level     string         `json:"level"`
	Context   map[string]any `json:"context"`
	Timestamp FaroTimestamp  `json:"timestamp"` // Decodes int64/float64/ISO-8601-string shapes; see FaroTimestamp
}

// FaroException defines structured state used in the metrics instrumentation and export flow.
//
// Used in flow:
// - FaroException is read or populated by backend components in this package to move requests through this stage.
type FaroException struct {
	Type       string         `json:"type"`
	Message    string         `json:"message,omitempty"` // Some payloads use message
	Value      string         `json:"value,omitempty"`   // Some payloads use value
	Stacktrace any            `json:"stacktrace"`        // Can be string or object
	Timestamp  FaroTimestamp  `json:"timestamp"`         // Decodes int64/float64/ISO-8601-string shapes; see FaroTimestamp
	Attributes map[string]any `json:"attributes,omitempty"`
	Context    map[string]any `json:"context,omitempty"` // Some payloads use context
}

// FaroEvent defines structured state used in the metrics instrumentation and export flow.
//
// Used in flow:
// - FaroEvent is read or populated by backend components in this package to move requests through this stage.
type FaroEvent struct {
	Name       string         `json:"name"`
	Domain     string         `json:"domain"`
	Timestamp  FaroTimestamp  `json:"timestamp"` // Decodes int64/float64/ISO-8601-string shapes; see FaroTimestamp
	Attributes map[string]any `json:"attributes"`
}

type faroMeasurementDebugDetail struct {
	Type        string    `json:"type"`
	Name        string    `json:"name"`
	Value       float64   `json:"value"`
	Timestamp   time.Time `json:"faro_timestamp"`
	App         string    `json:"app"`
	Env         string    `json:"env"`
	User        string    `json:"user"`
	BrowserName string    `json:"browser_name"`
	Path        string    `json:"path"`
}

type faroExceptionDebugDetail struct {
	Type      string    `json:"type"`
	Message   string    `json:"message"`
	Timestamp time.Time `json:"faro_timestamp"`
	App       string    `json:"app"`
	Env       string    `json:"env"`
	User      string    `json:"user"`
}

type faroEventDebugDetail struct {
	Name       string         `json:"name"`
	Domain     string         `json:"domain"`
	Timestamp  time.Time      `json:"faro_timestamp"`
	App        string         `json:"app"`
	Env        string         `json:"env"`
	User       string         `json:"user"`
	Attributes map[string]any `json:"attributes"`
}

type faroLogDebugDetail struct {
	Level     string    `json:"level"`
	Message   string    `json:"message"`
	Timestamp time.Time `json:"faro_timestamp"`
	App       string    `json:"app"`
	User      string    `json:"user"`
}

type faroDerivedMeasurementDebugDetail struct {
	Type        string  `json:"type"`
	Name        string  `json:"name"`
	Value       float64 `json:"value"`
	App         string  `json:"app"`
	BrowserName string  `json:"browser_name"`
	Path        string  `json:"path"`
}

type faroPayloadDebugDetails struct {
	Measurements        []faroMeasurementDebugDetail
	Exceptions          []faroExceptionDebugDetail
	Events              []faroEventDebugDetail
	Logs                []faroLogDebugDetail
	DerivedMeasurements []faroDerivedMeasurementDebugDetail
}

func faroDiagnosticIdentityFields(appName, appEnv, userID, browserName, pagePath, sessionID, refererHost string) []zap.Field {
	fields := []zap.Field{zap.String(logkeys.Operation, "process_faro_payload")}
	for _, field := range []struct {
		key   string
		value string
	}{
		{appLogger.KeyAppName, appName},
		{appLogger.KeyAppEnv, appEnv},
		{appLogger.KeyUser, userID},
		{appLogger.KeyBrowserName, browserName},
		{appLogger.KeyPagePath, pagePath},
		{appLogger.KeySessionID, sessionID},
		{appLogger.KeyReferer, refererHost},
	} {
		if field.value != "" {
			fields = append(fields, zap.String(field.key, field.value))
		}
	}
	return fields
}

func appendFaroDiagnosticFields(identity []zap.Field, event ...zap.Field) []zap.Field {
	fields := make([]zap.Field, 0, len(identity)+len(event))
	fields = append(fields, identity...)
	return append(fields, event...)
}

// ProcessFaroPayload records metrics and logs for a decoded Faro telemetry payload.
// Decoding and body-size enforcement happen at the router/codec layer; the referer
// argument is the raw Referer header value, reduced to its host before logging.
//
// Parameters:
// - ctx (context.Context): Carries the request trace ID for logging.
// - payload (*FaroPayload): The decoded (and sanitized) Faro telemetry payload.
// - referer (string): The raw Referer header value of the collect request.
func (h *FaroMetricsHandler) ProcessFaroPayload(ctx context.Context, payload *FaroPayload, referer string) {
	if payload == nil {
		return
	}

	// Skip processing if frontend metrics handler is not available
	if h.frontendMetrics == nil {
		if appLogger.DebugEnabled(h.logger) {
			appLogger.For(ctx, h.logger, faroLoggerName).Debug("Frontend metrics handler not available, skipping Faro payload processing")
		}
		return
	}

	// Extract app and user information for labels
	var appName, appEnv, userID, browserName, pageURL, pagePath, sessionID string

	// Extract app information
	if app, ok := payload.Meta["app"].(map[string]any); ok {
		if name, ok := app["name"].(string); ok {
			appName = name
		}
		if env, ok := app["environment"].(string); ok {
			appEnv = env
		}
	}

	// Extract user information (may not be present in all payloads)
	if user, ok := payload.Meta["user"].(map[string]any); ok {
		if id, ok := user["id"].(string); ok {
			userID = id
		}
	}

	// Extract browser information
	if browser, ok := payload.Meta["browser"].(map[string]any); ok {
		if name, ok := browser["name"].(string); ok {
			browserName = name
		}
	}

	// Extract page information
	if page, ok := payload.Meta["page"].(map[string]any); ok {
		if urlStr, ok := page["url"].(string); ok {
			pageURL = urlStr
			// Standardize the path by removing query parameters and hash fragments
			pagePath = removeQueryAndHash(pageURL)
		}
	}
	// Bound the page_path label to a capped set of distinct values: it derives from an
	// untrusted URL on an unauthenticated endpoint, so unbounded paths would grow the
	// Prometheus series count without limit.
	pagePath = h.frontendMetrics.BoundPagePath(pagePath)

	if session, ok := payload.Meta["session"].(map[string]any); ok {
		if id, ok := session["id"].(string); ok {
			sessionID = id
		}
	}

	refererHost := hostOnly(referer)
	diagnosticFields := faroDiagnosticIdentityFields(appName, appEnv, userID, browserName, pagePath, sessionID, refererHost)
	// Collection is a high-volume routine ingestion path. Keep the per-payload
	// success record at Debug; aggregate counters carry its production signal.
	operationLogger := appLogger.For(ctx, h.logger, faroLoggerName)
	// One gate for every Debug-only branch below: the summary record, the detail
	// accumulation, and the detail record must agree, or a guarded call could read
	// state that a different guard decided not to build.
	debugEnabled := appLogger.DebugEnabled(h.logger)
	if debugEnabled {
		summaryFields := []zap.Field{
			zap.String(appLogger.KeyAppName, appName),
			zap.String(appLogger.KeyAppEnv, appEnv),
			zap.String(appLogger.KeyBrowserName, browserName),
			// Log the query/hash-stripped path, never the full URL: this endpoint is unauthenticated
			// and page URLs / Referer routinely carry reset/verify/OAuth tokens in query params. The
			// same stripped value is what the metric labels use.
			zap.String(appLogger.KeyPagePath, pagePath),
			zap.String(appLogger.KeySessionID, sessionID),
			zap.Int(appLogger.KeyMeasurementsCount, len(payload.Measurements)),
			zap.Int(appLogger.KeyEventsCount, len(payload.Events)),
			zap.Int(appLogger.KeyLogsCount, len(payload.Logs)),
			zap.Int(appLogger.KeyExceptionsCount, len(payload.Exceptions)),
			zap.Int(appLogger.KeyTraceSize, faroCollectionSize(payload.Traces)),
		}
		if refererHost != "" {
			summaryFields = append(summaryFields, zap.String(appLogger.KeyReferer, refererHost))
		}
		operationLogger.Debug("Processed Faro telemetry payload", summaryFields...)
	}

	var debugDetails *faroPayloadDebugDetails
	if debugEnabled {
		debugDetails = &faroPayloadDebugDetails{
			Measurements:        make([]faroMeasurementDebugDetail, 0),
			Exceptions:          make([]faroExceptionDebugDetail, 0),
			Events:              make([]faroEventDebugDetail, 0),
			Logs:                make([]faroLogDebugDetail, 0),
			DerivedMeasurements: make([]faroDerivedMeasurementDebugDetail, 0),
		}
	}

	// Process measurements
	for _, measurement := range payload.Measurements {
		timestamp := measurement.Timestamp.Time
		if !measurement.Timestamp.Valid {
			timestamp = time.Now().UTC()
			operationLogger.Warn(
				"Failed to parse measurement timestamp",
				appendFaroDiagnosticFields(diagnosticFields,
					zap.String(logkeys.Stage, "parse_measurement_timestamp"),
					zap.String(appLogger.KeyType, measurement.Type),
					zap.String(appLogger.KeyFaroTimestamp, faroTimestampLogDiagnostic(measurement.Timestamp)),
					zap.String(logkeys.Fallback, "use_server_receive_time"),
					zap.String("timestamp_error", "unrecognized timestamp"),
				)...,
			)
		}

		// Record each value as a separate metric
		for name, rawValue := range measurement.Values {
			value, ok := coerceFaroFloat64(rawValue)
			if !ok {
				operationLogger.Warn(
					"Skipping Faro measurement value with unsupported type",
					appendFaroDiagnosticFields(diagnosticFields,
						zap.String(logkeys.Stage, "record_measurement_value"),
						zap.String(appLogger.KeyType, measurement.Type),
						zap.String(appLogger.KeyName, name),
						// Faro sends numeric measurement values; on this unauthenticated
						// endpoint log the offending Go type, not the untrusted value.
						zap.String(appLogger.KeyValueType, fmt.Sprintf("%T", rawValue)),
						zap.String(appLogger.KeyOutcome, "measurement_value_skipped"),
					)...,
				)
				continue
			}

			if debugDetails != nil {
				debugDetails.Measurements = append(debugDetails.Measurements, faroMeasurementDebugDetail{
					Type:        measurement.Type,
					Name:        name,
					Value:       value,
					Timestamp:   timestamp,
					App:         appName,
					Env:         appEnv,
					User:        userID,
					BrowserName: browserName,
					Path:        pagePath,
				})
			}

			// Handle different measurement types
			switch measurement.Type {
			case "web-vitals":
				h.frontendMetrics.RecordFaroWebVital(name, appName, browserName, pagePath, value)
			case "resource":
				// Extract resource details (duration and type)
				var duration float64
				var resourceType = "unknown" // Default type

				if rawDuration, ok := measurement.Values["duration"]; ok {
					parsedDuration, parsed := coerceFaroFloat64(rawDuration)
					if !parsed {
						operationLogger.Warn("Resource measurement has unsupported duration type",
							appendFaroDiagnosticFields(diagnosticFields,
								zap.String(logkeys.Stage, "record_resource_measurement"),
								zap.String(appLogger.KeyType, measurement.Type),
								zap.String(appLogger.KeyName, "duration"),
								zap.String(appLogger.KeyValueType, fmt.Sprintf("%T", rawDuration)),
								zap.String(appLogger.KeyOutcome, "resource_measurement_skipped"),
							)...)
						continue
					}
					duration = parsedDuration
				} else {
					operationLogger.Warn("Resource measurement missing duration value",
						appendFaroDiagnosticFields(diagnosticFields,
							zap.String(logkeys.Stage, "record_resource_measurement"),
							zap.String(appLogger.KeyType, measurement.Type),
							zap.String(logkeys.Expected, "numeric duration value"),
							zap.String(logkeys.Actual, "duration value missing"),
							zap.Int(appLogger.KeyValuesCount, len(measurement.Values)),
							zap.String(appLogger.KeyOutcome, "resource_measurement_skipped"),
						)...)
					continue // Skip if no duration
				}

				// Check attributes first for initiatorType
				if attrs, ok := measurement.Attributes["initiatorType"].(string); ok && attrs != "" {
					resourceType = attrs
				} else if ctx, ok := measurement.Context["initiatorType"].(string); ok && ctx != "" {
					// Fallback to context if attributes not present or empty
					resourceType = ctx
				}

				h.frontendMetrics.RecordFaroResourceTiming(resourceType, appName, browserName, pagePath, duration)

			default:
				// Handle other generic measurement types
				h.frontendMetrics.RecordFaroMeasurement(measurement.Type, name, appName, browserName, pagePath, value)
			}
		}
	}

	// Process exceptions
	for _, exception := range payload.Exceptions {
		timestamp := exception.Timestamp.Time
		if !exception.Timestamp.Valid {
			timestamp = time.Now().UTC()
			operationLogger.Warn(
				"Failed to parse exception timestamp",
				appendFaroDiagnosticFields(diagnosticFields,
					zap.String(logkeys.Stage, "parse_exception_timestamp"),
					zap.String(appLogger.KeyType, exception.Type),
					zap.String(appLogger.KeyFaroTimestamp, faroTimestampLogDiagnostic(exception.Timestamp)),
					zap.String(logkeys.Fallback, "use_server_receive_time"),
					zap.String("timestamp_error", "unrecognized timestamp"),
				)...,
			)
		}

		// Record using frontend metrics handler
		h.frontendMetrics.RecordFaroException(exception.Type, appName, browserName)

		// Get the exception message or value
		exceptionMessage := exception.Message
		if exceptionMessage == "" {
			exceptionMessage = exception.Value
		}

		if debugDetails != nil {
			debugDetails.Exceptions = append(debugDetails.Exceptions, faroExceptionDebugDetail{
				Type:      exception.Type,
				Message:   exceptionMessage,
				Timestamp: timestamp,
				App:       appName,
				Env:       appEnv,
				User:      userID,
			})
		}
	}

	// Process events
	for _, event := range payload.Events {
		timestamp := event.Timestamp.Time
		if !event.Timestamp.Valid {
			timestamp = time.Now().UTC()
			operationLogger.Warn(
				"Failed to parse event timestamp",
				appendFaroDiagnosticFields(diagnosticFields,
					zap.String(logkeys.Stage, "parse_event_timestamp"),
					zap.String(appLogger.KeyName, event.Name),
					zap.String(appLogger.KeyDomain, event.Domain),
					zap.String(appLogger.KeyFaroTimestamp, faroTimestampLogDiagnostic(event.Timestamp)),
					zap.String(logkeys.Fallback, "use_server_receive_time"),
					zap.String("timestamp_error", "unrecognized timestamp"),
				)...,
			)
		}

		// Record using frontend metrics handler
		h.frontendMetrics.RecordFaroEvent(event.Name, event.Domain, appName)
		h.recordDerivedFaroEventMetrics(event, appName, browserName, pagePath, debugDetails)

		if debugDetails != nil {
			debugDetails.Events = append(debugDetails.Events, faroEventDebugDetail{
				Name:       event.Name,
				Domain:     event.Domain,
				Timestamp:  timestamp,
				App:        appName,
				Env:        appEnv,
				User:       userID,
				Attributes: event.Attributes,
			})
		}
	}

	// Process logs (if any)
	for _, logEntry := range payload.Logs {
		// Now that the field is typed, use the decoded instant; fall back to now on a
		// missing/unparseable token (matching the measurement/exception/event paths).
		timestamp := logEntry.Timestamp.Time
		if !logEntry.Timestamp.Valid {
			timestamp = time.Now().UTC()
			operationLogger.Warn(
				"Failed to parse log timestamp",
				appendFaroDiagnosticFields(diagnosticFields,
					zap.String(logkeys.Stage, "parse_log_timestamp"),
					zap.String(appLogger.KeyLevel, logEntry.Level),
					zap.String(appLogger.KeyFaroTimestamp, faroTimestampLogDiagnostic(logEntry.Timestamp)),
					zap.String(logkeys.Fallback, "use_server_receive_time"),
					zap.String("timestamp_error", "unrecognized timestamp"),
				)...,
			)
		}

		// Record using frontend metrics handler
		h.frontendMetrics.RecordFaroLog(logEntry.Level, appName)

		if debugDetails != nil {
			debugDetails.Logs = append(debugDetails.Logs, faroLogDebugDetail{
				Level:     logEntry.Level,
				Message:   logEntry.Message,
				Timestamp: timestamp,
				App:       appName,
				User:      userID,
			})
		}
	}

	if debugDetails != nil {
		operationLogger.Debug("Processed Faro telemetry payload details",
			zap.String(appLogger.KeyAppName, appName),
			zap.String(appLogger.KeyAppEnv, appEnv),
			zap.String(appLogger.KeyUserID, userID),
			zap.String(appLogger.KeyBrowserName, browserName),
			zap.String(appLogger.KeyPageURL, pageURL),
			zap.String(appLogger.KeyPagePath, pagePath),
			zap.String(appLogger.KeySessionID, sessionID),
			zap.Any(appLogger.KeyMeasurements, debugDetails.Measurements),
			zap.Any(appLogger.KeyEvents, debugDetails.Events),
			zap.Any(appLogger.KeyLogs, debugDetails.Logs),
			zap.Any(appLogger.KeyExceptions, debugDetails.Exceptions),
			zap.Any(appLogger.KeyDerivedMeasurements, debugDetails.DerivedMeasurements),
			zap.Int(appLogger.KeyTraceSize, faroCollectionSize(payload.Traces)))
	}
}

func (h *FaroMetricsHandler) recordDerivedFaroEventMetrics(event FaroEvent, appName, browserName, pagePath string, debugDetails *faroPayloadDebugDetails) {
	switch event.Name {
	case "faro.performance.resource":
		h.recordFaroNumericEventAttributes("resource", event.Attributes, appName, browserName, pagePath, debugDetails)

		duration, ok := coerceFaroFloat64(event.Attributes["duration"])
		if !ok {
			return
		}

		h.frontendMetrics.RecordFaroResourceTiming(faroStringAttr(event.Attributes, "initiatorType", "unknown"), appName, browserName, pagePath, duration)
	case "faro.performance.navigation":
		h.recordFaroNumericEventAttributes("navigation", event.Attributes, appName, browserName, pagePath, debugDetails)

		ttfb, ok := coerceFaroFloat64(event.Attributes["ttfb"])
		if !ok {
			return
		}

		h.frontendMetrics.RecordFaroWebVital("ttfb", appName, browserName, pagePath, ttfb)
	}
}

func (h *FaroMetricsHandler) recordFaroNumericEventAttributes(measurementType string, attributes map[string]any, appName, browserName, pagePath string, debugDetails *faroPayloadDebugDetails) {
	for name, rawValue := range attributes {
		value, ok := coerceFaroFloat64(rawValue)
		if !ok {
			continue
		}

		h.frontendMetrics.RecordFaroMeasurement(measurementType, name, appName, browserName, pagePath, value)

		if debugDetails != nil {
			debugDetails.DerivedMeasurements = append(debugDetails.DerivedMeasurements, faroDerivedMeasurementDebugDetail{
				Type:        measurementType,
				Name:        name,
				Value:       value,
				App:         appName,
				BrowserName: browserName,
				Path:        pagePath,
			})
		}
	}
}

// FaroTimestamp is a Faro/OTel event timestamp. Faro emits it in several JSON
// shapes — an RFC3339 string, an epoch-millis string, or a numeric epoch — so the
// field decodes all of them into a single time.Time at the JSON boundary. Decoding
// never fails the enclosing payload: a missing or unrecognized token leaves Valid
// false (recording only a bounded token-shape diagnostic in Raw and a generic Err)
// and callers fall back to time.Now().
type FaroTimestamp struct {
	Time       time.Time // parsed instant; zero when Valid is false
	Raw        string    // bounded token shape and byte count, never raw content
	Valid      bool      // true only when Time was derived from a recognized token
	Err        error     // parse error when a token was present but unrecognized
	tokenKind  string
	tokenBytes int
}

// UnmarshalJSON accepts a JSON string (RFC3339, epoch-millis, or float text) or a
// JSON number. It returns nil for every input so a malformed timestamp degrades to
// Valid == false rather than failing the whole Faro payload decode.
func (t *FaroTimestamp) UnmarshalJSON(data []byte) error {
	data = bytes.TrimSpace(data)
	*t = FaroTimestamp{tokenKind: faroTimestampKind(data), tokenBytes: len(data)}
	t.Raw = faroTimestampLogDiagnostic(*t)
	if len(data) == 0 || bytes.Equal(data, []byte("null")) {
		t.Err = fmt.Errorf("timestamp is nil")
		return nil
	}

	if data[0] == '"' {
		var s string
		if err := json.Unmarshal(data, &s); err != nil {
			t.Err = fmt.Errorf("invalid timestamp JSON string")
			return nil
		}
		if parsed, err := parseFaroTimestampString(s); err == nil {
			t.Time, t.Valid = parsed, true
		} else {
			t.Err = err
		}
		return nil
	}

	var f float64
	if err := json.Unmarshal(data, &f); err != nil {
		t.Err = fmt.Errorf("unsupported timestamp token")
		return nil
	}
	t.Time, t.Valid = time.UnixMilli(int64(f)), true
	return nil
}

func faroTimestampKind(data []byte) string {
	kind := "number"
	if len(data) == 0 {
		kind = "missing"
	} else {
		switch data[0] {
		case '"':
			kind = "string"
		case '{':
			kind = "object"
		case '[':
			kind = "array"
		case 'n':
			kind = "null"
		case 't', 'f':
			kind = "boolean"
		}
	}
	return kind
}

func faroTimestampLogDiagnostic(timestamp FaroTimestamp) string {
	kind, size := timestamp.tokenKind, timestamp.tokenBytes
	if kind == "" {
		kind, size = "unknown", len(timestamp.Raw)
	}
	return fmt.Sprintf("%s (%d bytes)", kind, size)
}

// parseFaroTimestampString interprets the RFC3339 / epoch-millis / float string
// forms Faro sends, preserving the exact semantics of the former parseFaroTimestamp.
func parseFaroTimestampString(v string) (time.Time, error) {
	if parsedTime, err := time.Parse(time.RFC3339Nano, v); err == nil {
		return parsedTime, nil
	}

	if tsInt, err := strconv.ParseInt(v, 10, 64); err == nil {
		return time.UnixMilli(tsInt), nil
	}

	if tsFloat, err := strconv.ParseFloat(v, 64); err == nil {
		return time.UnixMilli(int64(tsFloat)), nil
	}

	return time.Time{}, fmt.Errorf("unsupported timestamp string format")
}

func faroStringAttr(attributes map[string]any, key, fallback string) string {
	value, ok := attributes[key].(string)
	if !ok || value == "" {
		return fallback
	}
	return value
}

func coerceFaroFloat64(raw any) (float64, bool) {
	switch v := raw.(type) {
	case float64:
		return v, true
	case float32:
		return float64(v), true
	case int:
		return float64(v), true
	case int32:
		return float64(v), true
	case int64:
		return float64(v), true
	case string:
		parsed, err := strconv.ParseFloat(v, 64)
		if err != nil {
			return 0, false
		}
		return parsed, true
	default:
		return 0, false
	}
}

func faroCollectionSize(raw any) int {
	switch v := raw.(type) {
	case map[string]any:
		return len(v)
	case []any:
		return len(v)
	default:
		if raw == nil {
			return 0
		}
		return 1
	}
}

// removeQueryAndHash trims everything after the first "?" or "#".

// removeQueryAndHash performs its package-specific work in the metrics instrumentation and export flow.
func removeQueryAndHash(s string) string {
	u, err := url.Parse(s)
	if err != nil || u == nil {
		// The value arrives from an unauthenticated payload; an unparseable URL must not panic
		// the handler.
		return ""
	}
	u.RawQuery, u.Fragment = "", ""
	clean := u.String()
	return clean
}

// hostOnly reduces a Referer value to its host, dropping the path, query, and fragment (which can
// carry reset/verify/OAuth tokens). Returns "" for empty, unparseable, or host-less values so the
// caller can skip the field entirely.
func hostOnly(s string) string {
	if s == "" {
		return ""
	}
	u, err := url.Parse(s)
	if err != nil || u == nil {
		return ""
	}
	return u.Host
}
