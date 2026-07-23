package searchjobs

import (
	"context"
	"errors"
	"fmt"
	"math"
	"slices"
	"strings"
	"time"
	"unsafe"

	"github.com/Suhaibinator/open-splunk/internal/searchtime"
)

// State is the lifecycle state of one asynchronous search job.
type State uint8

const (
	StateInvalid State = iota
	StateQueued
	StateParsing
	StatePlanning
	StateRunning
	StateCompleted
	StateFailed
	StateCanceled
	StateExpired
)

// String returns the stable lowercase spelling of a state.
func (state State) String() string {
	switch state {
	case StateQueued:
		return "queued"
	case StateParsing:
		return "parsing"
	case StatePlanning:
		return "planning"
	case StateRunning:
		return "running"
	case StateCompleted:
		return "completed"
	case StateFailed:
		return "failed"
	case StateCanceled:
		return "canceled"
	case StateExpired:
		return "expired"
	default:
		return "invalid"
	}
}

// Terminal reports whether no more execution work can change the job state.
func (state State) Terminal() bool { return state.terminal() }

func (state State) terminal() bool {
	return state == StateCompleted || state == StateFailed || state == StateCanceled || state == StateExpired
}

// FailureCode is a stable, safe category for a failed search.
type FailureCode string

const (
	FailureInvalidSPL         FailureCode = "invalid_spl"
	FailureUnsupportedSPL     FailureCode = "unsupported_spl"
	FailureInvalidTimeRange   FailureCode = "invalid_time_range"
	FailureIndexForbidden     FailureCode = "index_forbidden"
	FailureResourceLimit      FailureCode = "resource_limit"
	FailureTimeout            FailureCode = "timeout"
	FailureStorageUnavailable FailureCode = "storage_unavailable"
	FailureExecution          FailureCode = "execution"
	FailureInternal           FailureCode = "internal"
)

// Diagnostic is a source-located SPL parse or planning diagnostic. It contains
// only user-safe compiler information and never generated SQL.
type Diagnostic struct {
	Code        string
	Message     string
	Line        int
	Column      int
	EndLine     int
	EndColumn   int
	Suggestions []string
}

// Failure describes why a job failed without exposing storage errors or SQL.
type Failure struct {
	Code        FailureCode
	Message     string
	Retryable   bool
	Diagnostics []Diagnostic
}

// JobOrigin identifies the authoritative product surface that launched a
// search. IDs are retained separately so history can reopen the originating
// object without trusting a client-selected tenant or owner.
type JobOrigin uint8

const (
	JobOriginInvalid JobOrigin = iota
	JobOriginAdHoc
	JobOriginSavedSearch
	JobOriginHistoryRerun
	JobOriginDashboard
	JobOriginAPI
)

// JobSource is normalized provenance for one search attempt. ObjectID is
// required for saved-search, history-rerun, and dashboard origins and must be
// empty for ad-hoc and API origins.
type JobSource struct {
	Origin   JobOrigin
	ObjectID string
}

// CreateRequest is user intent plus server-resolved authorization. TimeRange
// is an indivisible, resolver-produced value so reusable intent cannot diverge
// from the absolute interval used for execution and audit history. The manager
// resolves IndexTimeCutoff from its clock when the immutable job is created.
type CreateRequest struct {
	SPL               string
	OwnerID           string
	TenantID          string
	AuthorizedIndexes []string
	RequestedIndexes  []string
	TimeRange         searchtime.Range
	AppID             string
	Source            JobSource
}

// AccessScope is the authenticated tenant and owner boundary used by the
// scoped read and cancellation methods. Cross-scope lookups return ErrNotFound
// so job existence is not disclosed.
type AccessScope struct {
	TenantID string
	OwnerID  string
}

// Job is a detached snapshot of job metadata. Every slice, failure, and schema
// returned by Manager is a deep copy and may be changed by the caller.
type Job struct {
	ID               string
	Version          uint64
	OwnerID          string
	SPL              string
	NormalizedSPL    string
	TenantID         string
	RequestedIndexes []string
	EffectiveIndexes []string
	TimeRange        searchtime.Intent
	AppID            string
	Source           JobSource
	Earliest         time.Time
	Latest           time.Time
	IndexTimeCutoff  time.Time
	VisibilityCutoff uint64
	State            State
	Schema           *Schema
	RowCount         uint64
	ResultBytes      uint64
	// ResultsTruncated reports that the retained immutable snapshot contains
	// exactly the configured row limit and the executor attempted to emit at
	// least one additional row. It is never inferred from an executor error.
	ResultsTruncated bool
	Failure          *Failure
	CreatedAt        time.Time
	StartedAt        time.Time
	FinishedAt       time.Time
	ExpiresAt        time.Time
}

// JobJournal durably brackets one asynchronous search attempt. Admit is
// invoked synchronously with the fully constructed queued snapshot before the
// job can become visible or executable. Finalize is invoked exactly once with
// the first completed, failed, or canceled snapshot for every job whose Admit
// returned nil. Implementations must observe context cancellation and should
// make both operations idempotent so process-level recovery can safely retry
// persistence outside this manager. Calls for different jobs may run
// concurrently, so implementations must also be concurrency-safe.
//
// Both snapshots are deep detached copies. Implementations may retain or
// mutate them and may call non-blocking Manager inspection methods without
// deadlocking. They must not call Close from inside a callback because Close
// intentionally waits for active admissions and workers, including the caller.
type JobJournal interface {
	Admit(context.Context, Job) error
	Finalize(context.Context, Job) error
}

// JournalOperation identifies the durable lifecycle operation that failed.
type JournalOperation uint8

const (
	JournalOperationInvalid JournalOperation = iota
	JournalOperationAdmit
	JournalOperationFinalize
)

// String returns the stable lowercase spelling of a journal operation.
func (operation JournalOperation) String() string {
	switch operation {
	case JournalOperationAdmit:
		return "admit"
	case JournalOperationFinalize:
		return "finalize"
	default:
		return "invalid"
	}
}

// JournalError is the trusted operational detail retained when a journal
// callback fails. Create returns only ErrJournalUnavailable for an Admit
// failure so storage details cannot cross the API boundary; operators can
// inspect this value through Config.OnJournalError or Manager.LastJournalError.
type JournalError struct {
	Operation JournalOperation
	JobID     string
	State     State
	Err       error
}

func (journalErr *JournalError) Error() string {
	if journalErr == nil {
		return "search job journal error"
	}
	return fmt.Sprintf("search job journal %s failed for job %q in state %s: %v", journalErr.Operation, journalErr.JobID, journalErr.State, journalErr.Err)
}

// Unwrap exposes the underlying error to trusted observability code.
func (journalErr *JournalError) Unwrap() error {
	if journalErr == nil {
		return nil
	}
	return journalErr.Err
}

// ValueKind distinguishes every result value the search job layer preserves.
// Null is an explicit value, not an absent cell. Mixed is schema-only: it
// describes a dynamic column whose individual cells retain concrete kinds.
type ValueKind uint8

const (
	ValueKindInvalid ValueKind = iota
	ValueKindNull
	ValueKindString
	ValueKindSigned
	ValueKindUnsigned
	ValueKindDouble
	ValueKindBool
	ValueKindBytes
	ValueKindTime
	ValueKindDuration
	ValueKindList
	ValueKindObject
	ValueKindDecimal
	ValueKindMixed
)

// Column is one ordered result column.
type Column struct {
	Name       string
	Kind       ValueKind
	Nullable   bool
	Multivalue bool
}

// Schema is the ordered schema emitted by the executor. Column names must
// exactly match the compiler's public output fields.
type Schema struct {
	Columns []Column
}

// ObjectField is one ordered object member. ObjectValue rejects duplicate and
// empty names so object rendering and hashing never depend on map iteration.
type ObjectField struct {
	Name  string
	Value Value
}

// Value is an immutable typed result value. Payload fields are private so a
// value cannot silently carry multiple kinds or be mutated through shared byte,
// list, or object slices. Constructors and accessors clone recursive payloads.
type Value struct {
	kind          ValueKind
	stringValue   string
	signedValue   int64
	unsignedValue uint64
	doubleValue   float64
	boolValue     bool
	bytesValue    []byte
	timeValue     time.Time
	durationValue time.Duration
	decimalValue  string
	listValue     []Value
	objectValue   []ObjectField
}

// NullValue constructs an explicit null.
func NullValue() Value { return Value{kind: ValueKindNull} }

// StringValue constructs a UTF-8 string value. Arbitrary Go strings are
// retained byte-for-byte; transport adapters may impose stricter UTF-8 rules.
func StringValue(value string) Value {
	return Value{kind: ValueKindString, stringValue: strings.Clone(value)}
}

// SignedValue constructs a signed 64-bit integer value.
func SignedValue(value int64) Value { return Value{kind: ValueKindSigned, signedValue: value} }

// UnsignedValue constructs an unsigned 64-bit integer value.
func UnsignedValue(value uint64) Value {
	return Value{kind: ValueKindUnsigned, unsignedValue: value}
}

// DoubleValue constructs an IEEE-754 double value, preserving NaN and
// infinities when an executor returns them.
func DoubleValue(value float64) Value { return Value{kind: ValueKindDouble, doubleValue: value} }

// BoolValue constructs a boolean value.
func BoolValue(value bool) Value { return Value{kind: ValueKindBool, boolValue: value} }

// BytesValue constructs a byte value without retaining the caller's slice.
func BytesValue(value []byte) Value {
	return Value{kind: ValueKindBytes, bytesValue: slices.Clone(value)}
}

// TimeValue constructs a timestamp normalized to UTC with its monotonic clock
// reading removed.
func TimeValue(value time.Time) Value {
	return Value{kind: ValueKindTime, timeValue: canonicalTime(value)}
}

// DurationValue constructs a signed nanosecond-resolution duration.
func DurationValue(value time.Duration) Value {
	return Value{kind: ValueKindDuration, durationValue: value}
}

// DecimalValue constructs an exact decimal from a validated lexical form. The
// original spelling is preserved so adapters do not round through float64.
func DecimalValue(value string) (Value, error) {
	if !validDecimal(value) {
		return Value{}, errors.New("search result decimal is invalid")
	}
	return Value{kind: ValueKindDecimal, decimalValue: strings.Clone(value)}, nil
}

// ListValue constructs an ordered list without retaining caller-owned slices.
// An invalid or over-depth child produces ValueKindInvalid, which result sinks
// reject; use Kind to detect invalid construction when accepting recursive
// values from an untrusted adapter.
func ListValue(values ...Value) Value {
	candidate := Value{kind: ValueKindList, listValue: values}
	if err := validateValue(candidate, 0); err != nil {
		return Value{}
	}
	candidate.listValue = cloneValues(values)
	return candidate
}

// ObjectValue constructs an ordered object without retaining caller-owned
// recursive payloads.
func ObjectValue(fields ...ObjectField) (Value, error) {
	seen := make(map[string]struct{}, len(fields))
	cloned := make([]ObjectField, len(fields))
	for index, field := range fields {
		if field.Name == "" {
			return Value{}, errors.New("search result object field name is empty")
		}
		if _, exists := seen[field.Name]; exists {
			return Value{}, fmt.Errorf("search result object field %q is duplicated", field.Name)
		}
		seen[field.Name] = struct{}{}
		if err := validateValue(field.Value, 0); err != nil {
			return Value{}, fmt.Errorf("search result object field %q: %w", field.Name, err)
		}
		cloned[index] = ObjectField{Name: strings.Clone(field.Name), Value: cloneValue(field.Value)}
	}
	candidate := Value{kind: ValueKindObject, objectValue: cloned}
	if err := validateValue(candidate, 0); err != nil {
		return Value{}, err
	}
	return candidate, nil
}

// Kind returns the selected value kind.
func (value Value) Kind() ValueKind { return value.kind }

// IsNull reports whether value is an explicit null.
func (value Value) IsNull() bool { return value.kind == ValueKindNull }

// String returns the string payload and whether the kind matches.
func (value Value) String() (string, bool) {
	return value.stringValue, value.kind == ValueKindString
}

// Signed returns the signed integer payload and whether the kind matches.
func (value Value) Signed() (int64, bool) {
	return value.signedValue, value.kind == ValueKindSigned
}

// Unsigned returns the unsigned integer payload and whether the kind matches.
func (value Value) Unsigned() (uint64, bool) {
	return value.unsignedValue, value.kind == ValueKindUnsigned
}

// Double returns the double payload and whether the kind matches.
func (value Value) Double() (float64, bool) {
	return value.doubleValue, value.kind == ValueKindDouble
}

// Bool returns the boolean payload and whether the kind matches.
func (value Value) Bool() (bool, bool) {
	return value.boolValue, value.kind == ValueKindBool
}

// Bytes returns a clone of the byte payload and whether the kind matches.
func (value Value) Bytes() ([]byte, bool) {
	if value.kind != ValueKindBytes {
		return nil, false
	}
	return slices.Clone(value.bytesValue), true
}

// Time returns the UTC timestamp and whether the kind matches.
func (value Value) Time() (time.Time, bool) {
	return value.timeValue, value.kind == ValueKindTime
}

// Duration returns the duration payload and whether the kind matches.
func (value Value) Duration() (time.Duration, bool) {
	return value.durationValue, value.kind == ValueKindDuration
}

// Decimal returns the exact decimal spelling and whether the kind matches.
func (value Value) Decimal() (string, bool) {
	return value.decimalValue, value.kind == ValueKindDecimal
}

// List returns a deep copy of the list and whether the kind matches.
func (value Value) List() ([]Value, bool) {
	if value.kind != ValueKindList {
		return nil, false
	}
	return cloneValues(value.listValue), true
}

// Object returns a deep copy of the ordered object and whether the kind
// matches.
func (value Value) Object() ([]ObjectField, bool) {
	if value.kind != ValueKindObject {
		return nil, false
	}
	return cloneObject(value.objectValue), true
}

// ResultRow is a stable, zero-based row within one completed result snapshot.
type ResultRow struct {
	Ordinal uint64
	Values  []Value

	retainedBytes uint64
}

// PageRequest selects one bounded result page. An empty cursor starts at row
// zero. Limit zero uses the manager's default.
type PageRequest struct {
	Limit  int
	Cursor string
}

// ResultPage is a detached result page. Complete means this page reaches the
// end of the immutable completed result snapshot.
type ResultPage struct {
	Schema     Schema
	Rows       []ResultRow
	NextCursor string
	TotalRows  uint64
	Complete   bool
}

func cloneJob(source Job) Job {
	result := source
	result.RequestedIndexes = cloneStrings(source.RequestedIndexes)
	result.EffectiveIndexes = cloneStrings(source.EffectiveIndexes)
	if source.Schema != nil {
		schema := cloneSchema(*source.Schema)
		result.Schema = &schema
	}
	if source.Failure != nil {
		failure := cloneFailure(*source.Failure)
		result.Failure = &failure
	}
	return result
}

func cloneJobSummary(source Job) Job {
	result := source
	result.SPL = ""
	result.NormalizedSPL = ""
	result.RequestedIndexes = nil
	result.EffectiveIndexes = nil
	result.Schema = nil
	if source.Failure != nil {
		failure := *source.Failure
		failure.Diagnostics = nil
		result.Failure = &failure
	}
	return result
}

func cloneFailure(source Failure) Failure {
	result := source
	result.Diagnostics = make([]Diagnostic, len(source.Diagnostics))
	for index, diagnostic := range source.Diagnostics {
		result.Diagnostics[index] = diagnostic
		result.Diagnostics[index].Suggestions = cloneStrings(diagnostic.Suggestions)
	}
	return result
}

func cloneSchema(source Schema) Schema {
	columns := make([]Column, len(source.Columns))
	for index, column := range source.Columns {
		columns[index] = column
		columns[index].Name = strings.Clone(column.Name)
	}
	return Schema{Columns: columns}
}

func cloneRows(source []ResultRow) []ResultRow {
	result := make([]ResultRow, len(source))
	for index, row := range source {
		result[index] = ResultRow{Ordinal: row.Ordinal, Values: cloneValues(row.Values), retainedBytes: row.retainedBytes}
	}
	return result
}

func cloneValues(source []Value) []Value {
	result := make([]Value, len(source))
	for index, value := range source {
		result[index] = cloneValue(value)
	}
	return result
}

func cloneValue(source Value) Value {
	result := source
	result.stringValue = strings.Clone(source.stringValue)
	result.decimalValue = strings.Clone(source.decimalValue)
	result.bytesValue = slices.Clone(source.bytesValue)
	result.listValue = cloneValues(source.listValue)
	result.objectValue = cloneObject(source.objectValue)
	return result
}

func cloneObject(source []ObjectField) []ObjectField {
	result := make([]ObjectField, len(source))
	for index, field := range source {
		result[index] = ObjectField{Name: strings.Clone(field.Name), Value: cloneValue(field.Value)}
	}
	return result
}

func cloneStrings(source []string) []string {
	result := make([]string, len(source))
	for index, value := range source {
		result[index] = strings.Clone(value)
	}
	return result
}

const (
	retainedValueBase           = uint64(unsafe.Sizeof(Value{}))
	retainedObjectFieldBase     = uint64(unsafe.Sizeof(ObjectField{})) - retainedValueBase
	retainedColumnBase          = uint64(unsafe.Sizeof(Column{}))
	retainedSchemaBase          = uint64(unsafe.Sizeof(Schema{}))
	retainedResultRowBase       = uint64(unsafe.Sizeof(ResultRow{}))
	retainedStringBase          = uint64(unsafe.Sizeof(""))
	retainedStateBase           = uint64(unsafe.Sizeof(StateInvalid))
	metadataContextAllowance    = uint64(1 << 10)
	metadataDiagnosticAllowance = uint64(4 << 10)
)

func retainedJobMetadataReservation(id string, request CreateRequest) (uint64, error) {
	normalizedRequest, err := normalizeCreateRequest(request)
	if err != nil {
		return 0, err
	}
	return retainedNormalizedJobMetadataReservation(id, normalizedRequest)
}

// retainedNormalizedJobMetadataReservation measures a request after its one
// canonicalization pass. Callers admitting a job must not normalize it again.
func retainedNormalizedJobMetadataReservation(id string, request CreateRequest) (uint64, error) {
	var err error
	intent := request.TimeRange.Intent()
	total := uint64(unsafe.Sizeof(jobEntry{})) + metadataContextAllowance + metadataDiagnosticAllowance
	for _, value := range []string{
		id,
		request.OwnerID,
		request.TenantID,
		request.SPL,
		intent.Earliest,
		intent.Latest,
		intent.Timezone,
		request.AppID,
		request.Source.ObjectID,
	} {
		total, err = checkedAdd(total, uint64(len(value)))
		if err != nil {
			return 0, err
		}
	}
	allIndexes := len(request.RequestedIndexes) + len(request.AuthorizedIndexes)
	stringSlots, err := checkedMultiply(uint64(len(request.RequestedIndexes)+len(request.AuthorizedIndexes)+allIndexes), retainedStringBase)
	if err != nil {
		return 0, err
	}
	total, err = checkedAdd(total, stringSlots)
	if err != nil {
		return 0, err
	}
	for _, indexes := range [][]string{request.RequestedIndexes, request.AuthorizedIndexes} {
		for _, index := range indexes {
			// The current scope retains one clone and planning may retain one
			// effective-scope clone of the same name.
			nameBytes, multiplyErr := checkedMultiply(uint64(len(index)), 2)
			if multiplyErr != nil {
				return 0, multiplyErr
			}
			total, err = checkedAdd(total, nameBytes)
			if err != nil {
				return 0, err
			}
		}
	}
	historyBytes, err := checkedMultiply(8, retainedStateBase)
	if err != nil {
		return 0, err
	}
	total, err = checkedAdd(total, historyBytes)
	if err != nil {
		return 0, err
	}
	// A retained parse/planning diagnostic is bounded by the admitted SPL plus
	// a fixed allowance for messages, suggestions, and source ranges.
	diagnosticBytes, err := checkedMultiply(uint64(len(request.SPL)), 4)
	if err != nil {
		return 0, err
	}
	return checkedAdd(total, diagnosticBytes)
}

func validateValue(value Value, depth int) error {
	_, _, err := measureValue(value, depth)
	return err
}

func valuePayloadSize(value Value, depth int) (uint64, error) {
	payload, _, err := measureValue(value, depth)
	return payload, err
}

func retainedValueSize(value Value, depth int) (uint64, error) {
	_, retained, err := measureValue(value, depth)
	return retained, err
}

func measureValue(value Value, depth int) (uint64, uint64, error) {
	if depth > 32 {
		return 0, 0, errors.New("search result value exceeds maximum nesting depth")
	}
	retained := retainedValueBase
	switch value.kind {
	case ValueKindNull:
		return 0, retained, nil
	case ValueKindString:
		retained, _ = checkedAdd(retained, uint64(len(value.stringValue)))
		return uint64(len(value.stringValue)), retained, nil
	case ValueKindSigned, ValueKindUnsigned, ValueKindDouble, ValueKindTime, ValueKindDuration:
		return 8, retained, nil
	case ValueKindBool:
		return 1, retained, nil
	case ValueKindBytes:
		retained, _ = checkedAdd(retained, uint64(len(value.bytesValue)))
		return uint64(len(value.bytesValue)), retained, nil
	case ValueKindDecimal:
		if !validDecimal(value.decimalValue) {
			return 0, 0, errors.New("search result decimal is invalid")
		}
		retained, _ = checkedAdd(retained, uint64(len(value.decimalValue)))
		return uint64(len(value.decimalValue)), retained, nil
	case ValueKindList:
		var payload uint64
		for _, child := range value.listValue {
			childPayload, childRetained, err := measureValue(child, depth+1)
			if err != nil {
				return 0, 0, err
			}
			payload, err = checkedAdd(payload, childPayload)
			if err != nil {
				return 0, 0, err
			}
			retained, err = checkedAdd(retained, childRetained)
			if err != nil {
				return 0, 0, err
			}
		}
		return payload, retained, nil
	case ValueKindObject:
		seen := make(map[string]struct{}, len(value.objectValue))
		var payload uint64
		for _, field := range value.objectValue {
			if field.Name == "" {
				return 0, 0, errors.New("search result object field name is empty")
			}
			if _, exists := seen[field.Name]; exists {
				return 0, 0, fmt.Errorf("search result object field %q is duplicated", field.Name)
			}
			seen[field.Name] = struct{}{}
			childPayload, childRetained, err := measureValue(field.Value, depth+1)
			if err != nil {
				return 0, 0, err
			}
			payload, err = checkedAdd(payload, uint64(len(field.Name)))
			if err != nil {
				return 0, 0, err
			}
			payload, err = checkedAdd(payload, childPayload)
			if err != nil {
				return 0, 0, err
			}
			retained, err = checkedAdd(retained, retainedObjectFieldBase)
			if err != nil {
				return 0, 0, err
			}
			retained, err = checkedAdd(retained, uint64(len(field.Name)))
			if err != nil {
				return 0, 0, err
			}
			retained, err = checkedAdd(retained, childRetained)
			if err != nil {
				return 0, 0, err
			}
		}
		return payload, retained, nil
	default:
		return 0, 0, errors.New("search result value kind is invalid")
	}
}

func retainedSchemaSize(schema Schema) (uint64, error) {
	total := retainedSchemaBase
	for _, column := range schema.Columns {
		var err error
		total, err = checkedAdd(total, retainedColumnBase)
		if err != nil {
			return 0, err
		}
		total, err = checkedAdd(total, uint64(len(column.Name)))
		if err != nil {
			return 0, err
		}
	}
	return total, nil
}

func checkedAdd(left, right uint64) (uint64, error) {
	if math.MaxUint64-left < right {
		return 0, errors.New("search result byte count overflow")
	}
	return left + right, nil
}

func checkedMultiply(left, right uint64) (uint64, error) {
	if left != 0 && right > math.MaxUint64/left {
		return 0, errors.New("search result byte count overflow")
	}
	return left * right, nil
}

func canonicalTime(value time.Time) time.Time {
	if value.IsZero() {
		return time.Time{}
	}
	return value.Round(0).UTC()
}

func validDecimal(value string) bool {
	if value == "" {
		return false
	}
	index := 0
	if value[index] == '+' || value[index] == '-' {
		index++
		if index == len(value) {
			return false
		}
	}
	integerStart := index
	for index < len(value) && value[index] >= '0' && value[index] <= '9' {
		index++
	}
	if index == integerStart {
		return false
	}
	if index < len(value) && value[index] == '.' {
		index++
		fractionStart := index
		for index < len(value) && value[index] >= '0' && value[index] <= '9' {
			index++
		}
		if index == fractionStart {
			return false
		}
	}
	if index < len(value) && (value[index] == 'e' || value[index] == 'E') {
		index++
		if index < len(value) && (value[index] == '+' || value[index] == '-') {
			index++
		}
		exponentStart := index
		for index < len(value) && value[index] >= '0' && value[index] <= '9' {
			index++
		}
		if index == exponentStart {
			return false
		}
	}
	return index == len(value)
}
