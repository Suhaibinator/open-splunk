// Package gradethiscorpus defines the canonical synthetic GradeThis fixture
// and the supported SPL investigations that exercise it.
//
// The package is test support, but it deliberately exposes one source of truth
// for compiler, ClickHouse, protocol, and browser acceptance tests. No fixture
// value is copied from a real GradeThis log.
package gradethiscorpus

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"
	"unicode/utf8"
)

const (
	ProfileIdentity = "canonical"
	IndexName       = "gradethis"
	Host            = "gradethis-fixture"
	Source          = "app.log"
	Sourcetype      = "go:zap:json"
	Service         = "gradethis"

	tracePlaceholder = "<trace-id>"
)

var (
	baseTime  = time.Date(2026, time.July, 1, 12, 0, 0, 0, time.UTC)
	indexTime = time.Date(2026, time.July, 1, 12, 20, 0, 123000000, time.UTC)
)

type SearchID string

const (
	SearchFollowTrace       SearchID = "follow-trace"
	SearchErrorsAndWarnings SearchID = "errors-and-warnings"
	SearchRawErrorFragment  SearchID = "raw-error-fragment"
	SearchSeverityCounts    SearchID = "severity-counts"
	SearchFrequentErrors    SearchID = "frequent-errors"
	SearchVolumeBySeverity  SearchID = "volume-by-severity"
	SearchServerErrors      SearchID = "server-errors-by-route"
	SearchResponses         SearchID = "responses-by-route-and-status"
	SearchSlowRoutes        SearchID = "slow-routes"
	SearchTopMessages       SearchID = "top-messages"
)

// Search is one immutable compatibility query template.
type Search struct {
	ID       SearchID
	Name     string
	Template string
}

// Render substitutes the one synthetic trace identifier used by the trace
// investigation. Other searches are returned byte-for-byte.
func (search Search) Render(traceID string) (string, error) {
	if !strings.Contains(search.Template, tracePlaceholder) {
		return strings.Clone(search.Template), nil
	}
	if !validHexID(traceID) {
		return "", errors.New("GradeThis corpus trace ID must contain exactly 32 lowercase hexadecimal characters")
	}
	return strings.ReplaceAll(search.Template, tracePlaceholder, traceID), nil
}

// Searches returns independent copies of the ten canonical searches in their
// documented order.
func Searches() []Search {
	searches := []Search{
		{
			ID:   SearchFollowTrace,
			Name: "follow one request",
			Template: `index=gradethis trace_id="<trace-id>"
| sort _time
| table _time level layer logger message`,
		},
		{
			ID:   SearchErrorsAndWarnings,
			Name: "inspect errors and warnings",
			Template: `index=gradethis (level=ERROR OR level=WARN)
| sort -_time`,
		},
		{
			ID:   SearchRawErrorFragment,
			Name: "find a known error fragment",
			Template: `index=gradethis "connection refused"
| table _time level logger message trace_id`,
		},
		{
			ID:   SearchSeverityCounts,
			Name: "count events by severity",
			Template: `index=gradethis
| stats count by level
| sort -count`,
		},
		{
			ID:   SearchFrequentErrors,
			Name: "find the most frequent errors",
			Template: `index=gradethis level=ERROR
| stats count by logger, message
| sort -count
| head 20`,
		},
		{
			ID:   SearchVolumeBySeverity,
			Name: "chart event volume by severity",
			Template: `index=gradethis
| timechart span=5m count by level`,
		},
		{
			ID:   SearchServerErrors,
			Name: "chart server errors by route",
			Template: `index=gradethis message="Request metrics" status>=500
| timechart span=5m count by path`,
		},
		{
			ID:   SearchResponses,
			Name: "count HTTP responses by route and status",
			Template: `index=gradethis message="Request metrics"
| stats count by path, status
| sort -count`,
		},
		{
			ID:   SearchSlowRoutes,
			Name: "find slow routes",
			Template: `index=gradethis message="Request metrics"
| eval duration_ms=tonumber(replace(duration, "ms$", ""))
| stats count p95(duration_ms) as p95_ms by path
| where p95_ms > 500`,
		},
		{
			ID:   SearchTopMessages,
			Name: "inspect the most common messages",
			Template: `index=gradethis
| top limit=20 message`,
		},
	}
	return slices.Clone(searches)
}

// Event is one synthetic fixture event. RawLine is the authoritative NDJSON
// representation consumed by the collector decoder.
type Event struct {
	ID        string
	Offset    time.Duration
	Level     string
	Layer     string
	Logger    string
	Caller    string
	Message   string
	TraceID   string
	SpanID    string
	Method    string
	Path      string
	Status    int64
	Duration  string
	Bytes     uint64
	IP        string
	UserAgent string
	Error     string
	Request   bool
	// ExplicitNull adds one synthetic dynamic null used to distinguish a
	// present null from an absent field after ClickHouse JSON transport.
	ExplicitNull bool
	RawLine      []byte
}

// Profile contains a detached copy of the canonical fixture.
type Profile struct {
	Identity  string
	BaseTime  time.Time
	IndexTime time.Time
	TraceID   string
	Events    []Event
	NDJSON    []byte
}

// Fixture returns a deterministic, sanitized twenty-event corpus. Its event
// times cover exactly [Profile.BaseTime, Profile.BaseTime+15m).
func Fixture() Profile {
	const sharedTrace = "11111111111111111111111111111111"
	events := []Event{
		event("trace-start", 30*time.Second, "INFO", "api", "api_handler.SRouter", "router/router.go:701", "Request started"),
		event("trace-database-error", 90*time.Second, "ERROR", "persistence", "persistence_handler", "persistence/queries.go:118", "Database request failed"),
		request("assessments-200-a", 2*time.Minute, "/api/assessments", 200, "800ms", 8101),
		event("heartbeat-a", 3*time.Minute, "INFO", "worker", "health", "worker/health.go:42", "Heartbeat"),
		request("assessments-503-a", 3*time.Minute+30*time.Second, "/api/assessments", 503, "800ms", 9111),
		event("dependency-warning-a", 4*time.Minute, "WARN", "worker", "dependency", "worker/retry.go:87", "Dependency retry scheduled"),
		request("assessments-200-b", 5*time.Minute+30*time.Second, "/api/assessments", 200, "800ms", 8102),
		event("heartbeat-b", 6*time.Minute, "INFO", "worker", "health", "worker/health.go:42", "Heartbeat"),
		request("assessments-503-b", 6*time.Minute+30*time.Second, "/api/assessments", 503, "800ms", 9112),
		event("dependency-warning-b", 7*time.Minute, "WARN", "worker", "dependency", "worker/retry.go:87", "Dependency retry scheduled"),
		request("assessments-200-c", 8*time.Minute, "/api/assessments", 200, "800ms", 8103),
		event("heartbeat-c", 9*time.Minute, "INFO", "worker", "health", "worker/health.go:42", "Heartbeat"),
		request("assessments-503-c", 9*time.Minute+30*time.Second, "/api/assessments", 503, "800ms", 9113),
		event("deadline-database-error", 10*time.Minute+30*time.Second, "ERROR", "persistence", "persistence_handler", "persistence/queries.go:126", "Database request failed"),
		request("assessments-200-d", 11*time.Minute, "/api/assessments", 200, "800ms", 8104),
		request("submissions-200-a", 11*time.Minute+30*time.Second, "/api/submissions", 200, "300ms", 4101),
		event("dependency-warning-c", 12*time.Minute, "WARN", "worker", "dependency", "worker/retry.go:87", "Dependency retry scheduled"),
		request("submissions-200-b", 12*time.Minute+30*time.Second, "/api/submissions", 200, "300ms", 4102),
		event("heartbeat-d", 13*time.Minute+30*time.Second, "INFO", "worker", "health", "worker/health.go:42", "Heartbeat"),
		request("submissions-500", 14*time.Minute, "/api/submissions", 500, "300ms", 5101),
	}
	events[0].TraceID, events[0].SpanID = sharedTrace, "1111111111111111"
	events[1].TraceID, events[1].SpanID = sharedTrace, "2222222222222222"
	events[1].Error = "dial tcp 192.0.2.200:5432: connection refused"
	events[5].ExplicitNull = true
	events[13].TraceID, events[13].SpanID = "22222222222222222222222222222222", "3333333333333333"
	events[13].Error = "context deadline exceeded"

	var ndjson bytes.Buffer
	for index := range events {
		events[index].RawLine = encodeEvent(events[index])
		ndjson.Write(events[index].RawLine)
		ndjson.WriteByte('\n')
	}
	return Profile{
		Identity:  ProfileIdentity,
		BaseTime:  baseTime,
		IndexTime: indexTime,
		TraceID:   sharedTrace,
		Events:    cloneEvents(events),
		NDJSON:    bytes.Clone(ndjson.Bytes()),
	}
}

// Validate checks manifest invariants and scans every line for sensitive or
// non-synthetic data.
func Validate(profile Profile) error {
	if profile.Identity != ProfileIdentity || !profile.BaseTime.Equal(baseTime) || !profile.IndexTime.Equal(indexTime) {
		return errors.New("GradeThis corpus profile identity does not match the canonical fixture")
	}
	if len(Searches()) != 10 {
		return errors.New("GradeThis corpus must contain exactly ten searches")
	}
	if !validHexID(profile.TraceID) {
		return errors.New("GradeThis corpus has an invalid trace ID")
	}
	if len(profile.Events) != 20 {
		return fmt.Errorf("GradeThis corpus contains %d events, want 20", len(profile.Events))
	}
	if err := ScanNDJSON(profile.NDJSON); err != nil {
		return fmt.Errorf("GradeThis corpus safety scan: %w", err)
	}
	seen := make(map[string]struct{}, len(profile.Events))
	var ndjson bytes.Buffer
	for index, event := range profile.Events {
		if event.ID == "" {
			return fmt.Errorf("GradeThis corpus event %d has no ID", index)
		}
		if _, duplicate := seen[event.ID]; duplicate {
			return fmt.Errorf("GradeThis corpus event ID %q is duplicated", event.ID)
		}
		seen[event.ID] = struct{}{}
		if event.Offset < 0 || event.Offset >= 15*time.Minute {
			return fmt.Errorf("GradeThis corpus event %q is outside the half-open search range", event.ID)
		}
		if !utf8.Valid(event.RawLine) || bytes.ContainsAny(event.RawLine, "\r\n") {
			return fmt.Errorf("GradeThis corpus event %q is not one UTF-8 NDJSON record", event.ID)
		}
		ndjson.Write(event.RawLine)
		ndjson.WriteByte('\n')
	}
	if !bytes.Equal(ndjson.Bytes(), profile.NDJSON) {
		return errors.New("GradeThis corpus NDJSON does not match its ordered events")
	}
	return nil
}

type rawFixture struct {
	Level        string           `json:"level"`
	Timestamp    string           `json:"timestamp"`
	Logger       string           `json:"logger"`
	Caller       string           `json:"caller"`
	Message      string           `json:"message"`
	Layer        string           `json:"layer"`
	Service      string           `json:"service"`
	Environment  string           `json:"environment"`
	Host         string           `json:"host"`
	Method       string           `json:"method,omitempty"`
	Path         string           `json:"path,omitempty"`
	Status       *int64           `json:"status,omitempty"`
	Duration     string           `json:"duration,omitempty"`
	Bytes        *uint64          `json:"bytes,omitempty"`
	IP           string           `json:"ip,omitempty"`
	UserAgent    string           `json:"user_agent,omitempty"`
	TraceID      string           `json:"trace_id,omitempty"`
	SpanID       string           `json:"span_id,omitempty"`
	Error        string           `json:"error,omitempty"`
	OptionalNote *json.RawMessage `json:"optional_note,omitempty"`
}

func event(id string, offset time.Duration, level, layer, logger, caller, message string) Event {
	return Event{
		ID: id, Offset: offset, Level: level, Layer: layer, Logger: logger,
		Caller: caller, Message: message,
	}
}

func request(id string, offset time.Duration, path string, status int64, duration string, bytes uint64) Event {
	level := "INFO"
	if status >= 500 {
		level = "ERROR"
	}
	ordinal := len(id)
	return Event{
		ID: id, Offset: offset, Level: level, Layer: "api",
		Logger: "api_handler.SRouter", Caller: "router/router.go:735",
		Message: "Request metrics", Method: "POST", Path: path, Status: status,
		Duration: duration, Bytes: bytes, IP: fmt.Sprintf("192.0.2.%d", 10+ordinal),
		UserAgent: "open-splunk-corpus", Request: true,
		TraceID: fmt.Sprintf("%032x", 1000+ordinal+int(offset/time.Second)),
		SpanID:  fmt.Sprintf("%016x", 2000+ordinal+int(offset/time.Second)),
	}
}

func encodeEvent(event Event) []byte {
	fixture := rawFixture{
		Level: event.Level, Timestamp: baseTime.Add(event.Offset).Format(time.RFC3339Nano),
		Logger: event.Logger, Caller: event.Caller, Message: event.Message,
		Layer: event.Layer, Service: Service, Environment: "test", Host: Host,
		Method: event.Method, Path: event.Path, Duration: event.Duration,
		IP: event.IP, UserAgent: event.UserAgent, TraceID: event.TraceID,
		SpanID: event.SpanID, Error: event.Error,
	}
	if event.Request {
		status, size := event.Status, event.Bytes
		fixture.Status, fixture.Bytes = &status, &size
	}
	if event.ExplicitNull {
		value := json.RawMessage("null")
		fixture.OptionalNote = &value
	}
	encoded, err := json.Marshal(fixture)
	if err != nil {
		panic(fmt.Sprintf("encode static GradeThis fixture %q: %v", event.ID, err))
	}
	return encoded
}

func cloneEvents(source []Event) []Event {
	result := slices.Clone(source)
	for index := range result {
		result[index].RawLine = bytes.Clone(result[index].RawLine)
	}
	return result
}

func validHexID(value string) bool {
	if len(value) != 32 {
		return false
	}
	for _, character := range value {
		if (character < '0' || character > '9') && (character < 'a' || character > 'f') {
			return false
		}
	}
	return true
}
