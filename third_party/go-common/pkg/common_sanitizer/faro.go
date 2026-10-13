package common_sanitizer

import (
	"context"
	"strings"
	"unicode"

	"github.com/Suhaibinator/go-common/pkg/common_metrics"
)

// Faro payloads arrive on an unauthenticated endpoint and several of their string
// fields end up as Prometheus label values, so every label-bound value is bounded
// and stripped of control characters before the metrics layer sees it.
const (
	// faroMaxItemsPerCategory caps each top-level slice (logs, events, ...) in a batch.
	faroMaxItemsPerCategory = 100
	// faroMaxLabelLength bounds values that become metric labels or log fields.
	faroMaxLabelLength = 128
	// faroMaxTextLength bounds free-text values (log/exception messages) that are
	// only ever emitted to debug logs.
	faroMaxTextLength = 2048
	// faroMaxMapEntries caps attribute/value maps whose keys become metric label values.
	faroMaxMapEntries = 64
)

// faroAllowedLogLevels is the LogLevel enum of @grafana/faro-core; anything else is
// attacker-chosen and gets bucketed as "other" to keep label cardinality bounded.
var faroAllowedLogLevels = map[string]struct{}{
	"trace": {},
	"debug": {},
	"info":  {},
	"log":   {},
	"warn":  {},
	"error": {},
}

// faroBucketOther is the sentinel every allowlist collapses unrecognized values to,
// so a flood of distinct attacker-chosen values can never grow the label cardinality.
const faroBucketOther = "other"

// faroAllowedMeasurementTypes is the set of measurement `type` values the metrics
// layer understands; the switch in ProcessFaroPayload keys off "web-vitals" and
// "resource", and "navigation" is emitted by the derived-event path. Any other type
// is bucketed to "other".
var faroAllowedMeasurementTypes = map[string]struct{}{
	"web-vitals": {},
	"resource":   {},
	"navigation": {},
}

// faroAllowedMeasurementNames enumerates the measurement value / event attribute keys
// that become the `name` metric label. It covers the web-vital names and the known
// Faro navigation/resource timing keys (including the ones ProcessFaroPayload reads by
// name, e.g. "duration" and "initiatorType"). Unknown keys are bucketed to "other".
var faroAllowedMeasurementNames = map[string]struct{}{
	// Web vitals.
	"fcp": {}, "ttfb": {}, "lcp": {}, "fid": {}, "cls": {}, "inp": {},
	// Navigation / resource timing durations.
	"duration": {}, "pageLoadTime": {}, "documentParsingTime": {}, "domProcessingTime": {},
	"domContentLoadHandlerTime": {}, "onLoadTime": {}, "fetchTime": {}, "serviceWorkerTime": {},
	"dnsLookupTime": {}, "tcpHandshakeTime": {}, "tlsNegotiationTime": {}, "redirectTime": {},
	"requestTime": {}, "responseTime": {},
	// Sizes and status.
	"responseStatus": {}, "decodedBodySize": {}, "encodedBodySize": {}, "transferSize": {},
	// Categorical keys read directly or emitted by the metrics layer.
	"initiatorType": {}, "type": {}, "visibilityState": {}, "redirectCount": {},
	"protocol": {}, "cacheHitStatus": {}, "renderBlockingStatus": {},
}

// faroAllowedEventNames is the set of Faro instrumentation event names; unknown names
// are bucketed to "other". The two performance names are load-bearing: the derived
// metrics switch in ProcessFaroPayload keys off them.
var faroAllowedEventNames = map[string]struct{}{
	"faro.performance.navigation":   {},
	"faro.performance.resource":     {},
	"faro.performance.longtask":     {},
	"faro.tracing.xml-http-request": {},
	"faro.tracing.fetch":            {},
	"session_start":                 {},
}

// faroAllowedEventDomains is the set of Faro event `domain` values; unknown domains are
// bucketed to "other".
var faroAllowedEventDomains = map[string]struct{}{
	"browser": {},
}

// SanitizeFaroPayload normalizes a decoded Faro telemetry payload. It is wired as
// the SRouter route sanitizer for the Faro collect endpoint and runs between decode
// and the handler.
//
// Unlike the request sanitizers, this never rejects: telemetry batches bundle many
// independent items, and failing the whole batch over one bad value loses data the
// client will not resend. Bad values are truncated, bucketed, or dropped instead.
// It is safe on a nil receiver and a nil payload.
//
// Parameters:
// - payload (*common_metrics.FaroPayload): The decoded payload to normalize; nil is a no-op.
//
// Returns:
// - result1 (*common_metrics.FaroPayload): The same payload, normalized in place.
// - result2 (error): Always nil; present to satisfy the SRouter sanitizer signature.
func (h *Handler) SanitizeFaroPayload(ctx context.Context, payload *common_metrics.FaroPayload) (*common_metrics.FaroPayload, error) {
	if payload == nil {
		return nil, nil
	}

	if len(payload.Measurements) > faroMaxItemsPerCategory {
		payload.Measurements = payload.Measurements[:faroMaxItemsPerCategory]
	}
	if len(payload.Logs) > faroMaxItemsPerCategory {
		payload.Logs = payload.Logs[:faroMaxItemsPerCategory]
	}
	if len(payload.Exceptions) > faroMaxItemsPerCategory {
		payload.Exceptions = payload.Exceptions[:faroMaxItemsPerCategory]
	}
	if len(payload.Events) > faroMaxItemsPerCategory {
		payload.Events = payload.Events[:faroMaxItemsPerCategory]
	}

	sanitizeFaroMeta(payload.Meta)

	for i := range payload.Measurements {
		m := &payload.Measurements[i]
		m.Type = bucketFaroLabel(sanitizeFaroLabel(m.Type, faroMaxLabelLength), faroAllowedMeasurementTypes)
		m.Values = sanitizeFaroKeyedMap(m.Values)
		m.Attributes = sanitizeFaroKeyedMap(m.Attributes)
		m.Context = sanitizeFaroKeyedMap(m.Context)
		sanitizeFaroResourceType(m.Attributes)
		sanitizeFaroResourceType(m.Context)
	}

	for i := range payload.Logs {
		l := &payload.Logs[i]
		level := strings.ToLower(sanitizeFaroLabel(l.Level, faroMaxLabelLength))
		if _, ok := faroAllowedLogLevels[level]; !ok {
			level = "other"
		}
		l.Level = level
		l.Message = truncateFaroText(l.Message, faroMaxTextLength)
	}

	for i := range payload.Exceptions {
		e := &payload.Exceptions[i]
		e.Type = sanitizeFaroLabel(e.Type, faroMaxLabelLength)
		e.Message = truncateFaroText(e.Message, faroMaxTextLength)
		e.Value = truncateFaroText(e.Value, faroMaxTextLength)
	}

	for i := range payload.Events {
		e := &payload.Events[i]
		e.Name = bucketFaroLabel(sanitizeFaroLabel(e.Name, faroMaxLabelLength), faroAllowedEventNames)
		e.Domain = bucketFaroLabel(sanitizeFaroLabel(e.Domain, faroMaxLabelLength), faroAllowedEventDomains)
		e.Attributes = sanitizeFaroKeyedMap(e.Attributes)
		sanitizeFaroResourceType(e.Attributes)
	}

	return payload, nil
}

func sanitizeFaroResourceType(attributes map[string]any) {
	if value, ok := attributes["initiatorType"].(string); ok {
		attributes["initiatorType"] = common_metrics.NormalizeFaroResourceType(value)
	}
}

// sanitizeFaroMeta normalizes the meta strings that reach metric labels and log
// fields: app name/environment, browser name, and the user/session identifiers.
func sanitizeFaroMeta(meta map[string]any) {
	sanitizeFaroMetaFields(meta, "app", "name", "environment")
	sanitizeFaroMetaFields(meta, "browser", "name")
	sanitizeFaroMetaFields(meta, "user", "id")
	sanitizeFaroMetaFields(meta, "session", "id")
}

func sanitizeFaroMetaFields(meta map[string]any, section string, fields ...string) {
	m, ok := meta[section].(map[string]any)
	if !ok {
		return
	}
	for _, field := range fields {
		if value, ok := m[field].(string); ok {
			m[field] = sanitizeFaroLabel(value, faroMaxLabelLength)
		}
	}
}

// sanitizeFaroKeyedMap bounds a map whose keys can become the `name` metric label:
// at most faroMaxMapEntries input entries are processed, keys are label-sanitized, and
// unrecognized keys are bucketed to "other" so distinct attacker keys cannot inflate
// label cardinality. Bucketing collapses duplicates, so the result may be smaller than
// the input.
func sanitizeFaroKeyedMap(m map[string]any) map[string]any {
	if m == nil {
		return nil
	}
	out := make(map[string]any, min(len(m), faroMaxMapEntries))
	processed := 0
	for key, value := range m {
		if processed >= faroMaxMapEntries {
			break
		}
		processed++
		out[bucketFaroLabel(sanitizeFaroLabel(key, faroMaxLabelLength), faroAllowedMeasurementNames)] = value
	}
	return out
}

// bucketFaroLabel returns value when it is in allowed, otherwise the "other" bucket, so
// only a fixed set of label values can ever reach the metrics layer.
func bucketFaroLabel(value string, allowed map[string]struct{}) string {
	if _, ok := allowed[value]; ok {
		return value
	}
	return faroBucketOther
}

// sanitizeFaroLabel strips control characters, trims whitespace, and truncates the
// value on a rune boundary so it is safe to use as a metric label or log field.
func sanitizeFaroLabel(s string, maxLen int) string {
	s = strings.Map(func(r rune) rune {
		if unicode.IsControl(r) {
			return -1
		}
		return r
	}, s)
	s = strings.TrimSpace(s)
	return truncateFaroText(s, maxLen)
}

// truncateFaroText truncates s to at most maxLen runes.
func truncateFaroText(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	runes := []rune(s)
	if len(runes) <= maxLen {
		return s
	}
	return string(runes[:maxLen])
}
