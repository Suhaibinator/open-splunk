package gradethiscorpus

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"
)

const (
	MigrationIdentity   = "current-source"
	MigrationIndexName  = "gradethis"
	MigrationSource     = "gradethis-backend"
	MigrationSourcetype = "go:zap:json"
	MigrationService    = "gradethis"

	migrationTracePlaceholder = "<trace-id>"
	migrationRequestMessage   = "Request summary statistics"
)

var migrationBaseTime = time.Date(2026, time.July, 15, 12, 0, 0, 0, time.UTC)

// MigrationSearchID identifies one investigation representative of the
// current GradeThis/go-common log source. These searches are separate from
// the canonical synthetic corpus.
type MigrationSearchID string

const (
	MigrationSearchFollowTrace    MigrationSearchID = "follow-trace"
	MigrationSearchSeverityCounts MigrationSearchID = "severity-counts"
	MigrationSearchFailedRequests MigrationSearchID = "failed-requests"
	MigrationSearchPathStatus     MigrationSearchID = "path-status"
	MigrationSearchDurationUnits  MigrationSearchID = "duration-units"
	MigrationSearchTopMessages    MigrationSearchID = "top-messages"
)

// MigrationSearch is one immutable current-source investigation template.
type MigrationSearch struct {
	ID           MigrationSearchID
	Name         string
	Template     string
	ExpectedRows uint64
}

// Render substitutes the fixture trace ID without accepting arbitrary SPL.
func (search MigrationSearch) Render(traceID string) (string, error) {
	if !strings.Contains(search.Template, migrationTracePlaceholder) {
		return strings.Clone(search.Template), nil
	}
	if !validHexID(traceID) {
		return "", errors.New("GradeThis migration trace ID must contain exactly 32 lowercase hexadecimal characters")
	}
	return strings.ReplaceAll(search.Template, migrationTracePlaceholder, traceID), nil
}

// MigrationSearches returns the bounded representative investigations for the
// current GradeThis source shape. The exact product-plan searches remain in
// Searches and are not changed by this migration profile.
func MigrationSearches() []MigrationSearch {
	return slices.Clone([]MigrationSearch{
		{
			ID:           MigrationSearchFollowTrace,
			Name:         "follow a current GradeThis trace",
			ExpectedRows: 3,
			Template: `index=gradethis trace_id="<trace-id>"
| sort _time
| table _time level layer logger message`,
		},
		{
			ID:           MigrationSearchSeverityCounts,
			Name:         "count current GradeThis events by severity",
			ExpectedRows: 3,
			Template: `index=gradethis
| stats count by level
| sort -count level`,
		},
		{
			ID:           MigrationSearchFailedRequests,
			Name:         "inspect current GradeThis server failures",
			ExpectedRows: 2,
			Template: `index=gradethis message="Request summary statistics" status>=500
| sort _time
| table _time level path status duration trace_id`,
		},
		{
			ID:           MigrationSearchPathStatus,
			Name:         "count current GradeThis responses by path and status",
			ExpectedRows: 8,
			Template: `index=gradethis message="Request summary statistics"
| stats count by path, status
| sort -count path status`,
		},
		{
			ID:           MigrationSearchDurationUnits,
			Name:         "inspect current GradeThis duration units",
			ExpectedRows: 3,
			Template: `index=gradethis message="Request summary statistics"
| rex field=duration max_match=1 "^(?<duration_value>\d+(?:\.\d+)?)(?<duration_unit>µs|ms|s)$"
| stats count by duration_unit
| sort -count duration_unit`,
		},
		{
			ID:           MigrationSearchTopMessages,
			Name:         "inspect current GradeThis top messages",
			ExpectedRows: 3,
			Template: `index=gradethis
| top limit=3 message`,
		},
	})
}

// MigrationEvent is one generated current-source event plus the semantic
// fields used to derive integration expectations. RawLine never contains
// collector-owned host, service, environment, source, sourcetype, or index
// metadata.
type MigrationEvent struct {
	ID           string
	Offset       time.Duration
	Timestamp    time.Time
	Level        string
	Logger       string
	Caller       string
	Message      string
	Layer        string
	TraceID      string
	Method       string
	Path         string
	Status       int64
	Duration     string
	Bytes        uint64
	IP           string
	Request      bool
	Healthy      bool
	ExplicitNull bool
	Details      bool
	RawLine      []byte
}

// MigrationProfile is a detached, deterministic, scanner-validated current
// GradeThis source corpus.
type MigrationProfile struct {
	Identity string
	BaseTime time.Time
	TraceID  string
	Events   []MigrationEvent
	NDJSON   []byte
}

// MigrationFixture generates a sanitized current GradeThis/go-common profile.
// It models request-summary naming, severity selection, mixed Go duration
// units, RFC3339Nano offsets, sparse root fields, and trusted metadata being
// attached by the collector rather than repeated in raw application JSON.
func MigrationFixture() MigrationProfile {
	return MigrationFixtureAt(migrationBaseTime)
}

// MigrationFixtureAt generates the same deterministic semantic profile at a
// caller-selected UTC instant. Long-lived end-to-end tests use a recent base
// time so ingestion age and logical-retention policies remain part of the
// exercised contract; MigrationFixture remains byte-pinned for compatibility.
func MigrationFixtureAt(baseTime time.Time) MigrationProfile {
	const sharedTrace = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	baseTime = baseTime.UTC()

	events := []MigrationEvent{
		migrationEvent(baseTime, "trace-api", 15*time.Second, "INFO", "api_handler.SRouter",
			"api/handler.go:210", "Request started", "api", sharedTrace),
		migrationEvent(baseTime, "trace-service", 45*time.Second, "INFO", "assessment_service",
			"service/assessment.go:84", "Assessment loaded", "service", sharedTrace),
		migrationEvent(baseTime, "trace-database", 75*time.Second, "ERROR", "persistence_handler",
			"persistence/assessment.go:118", "Database request failed", "persistence", sharedTrace),
		migrationRequest(baseTime, "assessments-fast-microseconds", 2*time.Minute, false,
			"/api/assessments", 200, "800µs", 1),
		migrationEvent(baseTime, "heartbeat-a", 150*time.Second, "INFO", "health",
			"worker/health.go:42", "Heartbeat", "worker", ""),
		migrationRequest(baseTime, "assessments-fast-milliseconds", 3*time.Minute, true,
			"/api/assessments", 200, "250ms", 2),
		migrationEvent(baseTime, "cache-warning-a", 210*time.Second, "WARN", "cache",
			"service/cache.go:71", "Cache refresh delayed", "service", ""),
		migrationRequest(baseTime, "assessments-server-error", 4*time.Minute, false,
			"/api/assessments", 503, "1.2s", 3),
		migrationRequest(baseTime, "assessments-client-warning", 5*time.Minute, true,
			"/api/assessments", 429, "750ms", 4),
		migrationEvent(baseTime, "heartbeat-b", 330*time.Second, "INFO", "health",
			"worker/health.go:42", "Heartbeat", "worker", ""),
		migrationRequest(baseTime, "submissions-fast", 6*time.Minute, false,
			"/api/submissions", 200, "80ms", 5),
		migrationRootEvent(baseTime, "startup", 390*time.Second),
		migrationRequest(baseTime, "submissions-server-error", 7*time.Minute, true,
			"/api/submissions", 500, "2s", 6),
		migrationEvent(baseTime, "cache-warning-b", 450*time.Second, "WARN", "cache",
			"service/cache.go:71", "Cache refresh delayed", "service", ""),
		migrationRequest(baseTime, "submissions-slow-success", 8*time.Minute, false,
			"/api/submissions", 200, "600ms", 7),
		migrationEvent(baseTime, "heartbeat-c", 510*time.Second, "INFO", "health",
			"worker/health.go:42", "Heartbeat", "worker", ""),
		migrationRequest(baseTime, "reports-fast", 9*time.Minute, true,
			"/api/reports", 204, "1.5ms", 8),
		migrationEvent(baseTime, "dependency-error", 570*time.Second, "ERROR", "dependency",
			"service/dependency.go:93", "Dependency unavailable", "service", ""),
		migrationRequest(baseTime, "reports-client-warning", 10*time.Minute, false,
			"/api/reports", 404, "12ms", 9),
		migrationRequest(baseTime, "reports-slow-success", 11*time.Minute, true,
			"/api/reports", 200, "900ms", 10),
	}

	var ndjson bytes.Buffer
	for index := range events {
		events[index].RawLine = encodeMigrationEvent(events[index])
		ndjson.Write(events[index].RawLine)
		ndjson.WriteByte('\n')
	}
	return MigrationProfile{
		Identity: MigrationIdentity, BaseTime: baseTime, TraceID: sharedTrace,
		Events: cloneMigrationEvents(events), NDJSON: bytes.Clone(ndjson.Bytes()),
	}
}

func migrationEvent(
	baseTime time.Time,
	id string,
	offset time.Duration,
	level, logger, caller, message, layer, traceID string,
) MigrationEvent {
	return MigrationEvent{
		ID: id, Offset: offset, Timestamp: baseTime.Add(offset),
		Level: level, Logger: logger, Caller: caller, Message: message,
		Layer: layer, TraceID: traceID,
	}
}

func migrationRequest(
	baseTime time.Time,
	id string,
	offset time.Duration,
	offsetTimestamp bool,
	path string,
	status int64,
	duration string,
	ordinal uint64,
) MigrationEvent {
	parsed, err := time.ParseDuration(duration)
	if err != nil {
		panic("invalid static migration duration: " + err.Error())
	}
	level := "INFO"
	if status >= 500 {
		level = "ERROR"
	} else if status >= 400 || parsed > 500*time.Millisecond {
		level = "WARN"
	}
	timestamp := baseTime.Add(offset)
	if offsetTimestamp {
		timestamp = timestamp.In(time.FixedZone("migration-fixture", -7*60*60))
	}
	return MigrationEvent{
		ID: id, Offset: offset, Timestamp: timestamp, Level: level,
		Logger: "api_handler.SRouter", Caller: "api/handler.go:240",
		Message: migrationRequestMessage, Layer: "api",
		TraceID: fmt.Sprintf("%032x", 0x1000+ordinal),
		Method:  "POST", Path: path, Status: status, Duration: duration,
		Bytes: 1_000 + ordinal, IP: fmt.Sprintf("192.0.2.%d", ordinal),
		Request: true,
	}
}

func migrationRootEvent(baseTime time.Time, id string, offset time.Duration) MigrationEvent {
	return MigrationEvent{
		ID: id, Offset: offset, Timestamp: baseTime.Add(offset),
		Level: "INFO", Caller: "registry/registry.go:88", Message: "Application started",
		Healthy: true, ExplicitNull: true, Details: true,
	}
}

type migrationWireDetails struct {
	Mode    string `json:"mode"`
	Workers int64  `json:"workers"`
}

type migrationWireEvent struct {
	Timestamp    string                `json:"timestamp"`
	Level        string                `json:"level"`
	Logger       *string               `json:"logger,omitempty"`
	Caller       string                `json:"caller"`
	Message      string                `json:"message"`
	Layer        *string               `json:"layer,omitempty"`
	TraceID      *string               `json:"trace_id,omitempty"`
	Method       *string               `json:"method,omitempty"`
	Path         *string               `json:"path,omitempty"`
	Status       *int64                `json:"status,omitempty"`
	Duration     *string               `json:"duration,omitempty"`
	Bytes        *uint64               `json:"bytes,omitempty"`
	IP           *string               `json:"ip,omitempty"`
	Healthy      *bool                 `json:"healthy,omitempty"`
	OptionalNote *json.RawMessage      `json:"optional_note,omitempty"`
	Details      *migrationWireDetails `json:"details,omitempty"`
}

func encodeMigrationEvent(event MigrationEvent) []byte {
	wire := migrationWireEvent{
		Timestamp: event.Timestamp.Format(time.RFC3339Nano),
		Level:     event.Level,
		Caller:    event.Caller,
		Message:   event.Message,
	}
	if event.Logger != "" {
		wire.Logger = new(event.Logger)
	}
	if event.Layer != "" {
		wire.Layer = new(event.Layer)
	}
	if event.TraceID != "" {
		wire.TraceID = new(event.TraceID)
	}
	if event.Request {
		wire.Method = new(event.Method)
		wire.Path = new(event.Path)
		wire.Status = new(event.Status)
		wire.Duration = new(event.Duration)
		wire.Bytes = new(event.Bytes)
		wire.IP = new(event.IP)
	}
	if event.Healthy {
		wire.Healthy = new(true)
	}
	if event.ExplicitNull {
		nullValue := json.RawMessage("null")
		wire.OptionalNote = &nullValue
	}
	if event.Details {
		wire.Details = &migrationWireDetails{Mode: "integration", Workers: 2}
	}
	encoded, err := json.Marshal(wire)
	if err != nil {
		panic("encode static GradeThis migration event: " + err.Error())
	}
	return encoded
}

// ValidateMigration checks identity, source realism, deterministic encoding,
// and the ordinary fixture scanner before the profile can reach a collector.
func ValidateMigration(profile MigrationProfile) error {
	if profile.Identity != MigrationIdentity || profile.BaseTime.IsZero() ||
		profile.BaseTime.Location() != time.UTC {
		return errors.New("GradeThis migration profile identity does not match the current source")
	}
	if !validHexID(profile.TraceID) {
		return errors.New("GradeThis migration profile has an invalid trace ID")
	}
	if len(MigrationSearches()) != 6 {
		return errors.New("GradeThis migration profile must define exactly six investigations")
	}
	if len(profile.Events) != 20 {
		return fmt.Errorf("GradeThis migration profile has %d events, want 20", len(profile.Events))
	}
	if err := ScanNDJSON(profile.NDJSON); err != nil {
		return fmt.Errorf("scan GradeThis migration fixture: %w", err)
	}

	seen := make(map[string]struct{}, len(profile.Events))
	levelCounts := make(map[string]int)
	messageCounts := make(map[string]int)
	durationUnits := make(map[string]int)
	var (
		requests, traceEvents int
		sawUTC, sawOffset     bool
		sawSparseRoot         bool
	)
	var encoded bytes.Buffer
	for _, event := range profile.Events {
		if event.ID == "" {
			return errors.New("GradeThis migration event ID is empty")
		}
		if _, duplicate := seen[event.ID]; duplicate {
			return fmt.Errorf("GradeThis migration event ID %q is duplicated", event.ID)
		}
		seen[event.ID] = struct{}{}
		if !event.Timestamp.Equal(profile.BaseTime.Add(event.Offset)) {
			return fmt.Errorf("GradeThis migration event %q timestamp does not match its offset", event.ID)
		}
		if event.Timestamp.Location() == time.UTC {
			sawUTC = true
		} else {
			sawOffset = true
		}
		if !bytes.Equal(event.RawLine, encodeMigrationEvent(event)) {
			return fmt.Errorf("GradeThis migration event %q raw encoding drifted", event.ID)
		}
		encoded.Write(event.RawLine)
		encoded.WriteByte('\n')
		levelCounts[event.Level]++
		messageCounts[event.Message]++
		if event.TraceID == profile.TraceID {
			traceEvents++
		}
		if !event.Request {
			if event.Logger == "" && event.Layer == "" {
				sawSparseRoot = true
			}
			continue
		}
		requests++
		parsed, err := time.ParseDuration(event.Duration)
		if err != nil {
			return fmt.Errorf("GradeThis migration event %q duration: %w", event.ID, err)
		}
		unit := migrationDurationUnit(event.Duration)
		if unit == "" {
			return fmt.Errorf("GradeThis migration event %q duration unit is unsupported", event.ID)
		}
		durationUnits[unit]++
		wantLevel := "INFO"
		if event.Status >= 500 {
			wantLevel = "ERROR"
		} else if event.Status >= 400 || parsed > 500*time.Millisecond {
			wantLevel = "WARN"
		}
		if event.Level != wantLevel || event.Message != migrationRequestMessage {
			return fmt.Errorf("GradeThis migration request %q has level/message %q/%q, want %q/%q",
				event.ID, event.Level, event.Message, wantLevel, migrationRequestMessage)
		}
	}
	if !bytes.Equal(encoded.Bytes(), profile.NDJSON) {
		return errors.New("GradeThis migration NDJSON does not match its event manifest")
	}
	if !sawUTC || !sawOffset || !sawSparseRoot {
		return fmt.Errorf("GradeThis migration representation coverage = UTC:%t offset:%t sparse-root:%t",
			sawUTC, sawOffset, sawSparseRoot)
	}
	if requests != 10 || traceEvents != 3 ||
		levelCounts["INFO"] != 10 || levelCounts["WARN"] != 6 || levelCounts["ERROR"] != 4 ||
		messageCounts[migrationRequestMessage] != 10 ||
		durationUnits["ms"] != 7 || durationUnits["s"] != 2 || durationUnits["µs"] != 1 {
		return fmt.Errorf(
			"GradeThis migration semantic counts drifted: requests=%d trace=%d levels=%v messages=%v units=%v",
			requests, traceEvents, levelCounts, messageCounts, durationUnits,
		)
	}
	return nil
}

func migrationDurationUnit(value string) string {
	for _, unit := range []string{"µs", "ms", "s"} {
		if strings.HasSuffix(value, unit) {
			return unit
		}
	}
	return ""
}

func cloneMigrationEvents(events []MigrationEvent) []MigrationEvent {
	cloned := slices.Clone(events)
	for index := range cloned {
		cloned[index].RawLine = bytes.Clone(cloned[index].RawLine)
	}
	return cloned
}
