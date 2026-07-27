package ingest

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"regexp"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/eventfields"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var decimalPattern = regexp.MustCompile(`^-?(?:0|[1-9][0-9]*)(?:\.[0-9]+)?(?:[eE][+-]?(?:0|[1-9][0-9]*))?$`)

// DurationFitsResultRange reports whether a protobuf duration can round-trip
// through the nanosecond-resolution time.Duration used by search result values.
// Keeping this invariant at ingestion prevents an accepted durable event from
// making a later table search fail during result conversion.
func DurationFitsResultRange(value *durationpb.Duration) bool {
	if value == nil || value.CheckValid() != nil {
		return false
	}
	converted := value.AsDuration()
	roundTrip := durationpb.New(converted)
	return roundTrip.GetSeconds() == value.GetSeconds() && roundTrip.GetNanos() == value.GetNanos()
}

// Validator performs deterministic, storage-independent event validation and
// normalization. It is safe for concurrent use.
type Validator struct {
	limits            Limits
	replacement       string
	depthReplacement  string
	sensitive         map[string]struct{}
	replacementByName map[string]redactionMatch
	ordered           []*Validator
	sequential        []*Validator
	mandatory         bool
	exact             bool
}

func NewValidator(limits Limits, policy RedactionPolicy) (*Validator, error) {
	return newValidator(limits, policy, true, false)
}

// NewSupplementalRedactor constructs an exact-name redactor for only the
// supplied deployment-specific fields. It deliberately omits the mandatory
// built-ins and must be composed with a normal Validator at a trust boundary.
// The collector uses this to preserve each ordered config rule's matching and
// replacement semantics while one shared mandatory validator is authoritative.
func NewSupplementalRedactor(limits Limits, policy RedactionPolicy) (*Validator, error) {
	if len(policy.AdditionalSensitiveFields) == 0 {
		return nil, errors.New("supplemental redaction requires at least one field")
	}
	return newValidator(limits, policy, false, true)
}

// NewCompositeSupplementalRedactor constructs one exact-name redactor for an
// ordered set of deployment-specific policies. Callers may group fields with
// the same replacement. Policy order remains observable for fail-closed
// boundaries, generated-marker cascades, and the embedded-JSON depth limit.
//
// Ordinary marker strings use one traversal for structured data, valid JSON,
// unchanged text, and free-form text whose only observed match belongs to the
// final policy. Any earlier-policy text match, embedded encoded payload, hidden
// assignment inside a later value, or ambiguous fail-closed boundary is
// replayed in policy order: generated quotes can change how a later historical
// pass parses otherwise malformed text.
// Repeated fields, pre-final syntax-bearing markers, and markers equal to a
// later field always retain that ordered implementation because they can
// intentionally create a match for a later policy. These compatibility paths
// favor exact historical output and confidentiality over a misleading
// one-pass claim.
func NewCompositeSupplementalRedactor(
	limits Limits,
	policies []RedactionPolicy,
) (*Validator, error) {
	if len(policies) == 0 {
		return nil, errors.New("composite supplemental redaction requires at least one policy")
	}
	if len(policies) == 1 {
		redactor, err := NewSupplementalRedactor(limits, policies[0])
		if err != nil {
			return nil, fmt.Errorf("supplemental redaction policy 0: %w", err)
		}
		return redactor, nil
	}

	sequential := make([]*Validator, 0, len(policies))
	replacements := make(map[string]redactionMatch)
	useSequential := false
	for order, policy := range policies {
		redactor, err := NewSupplementalRedactor(limits, policy)
		if err != nil {
			return nil, fmt.Errorf("supplemental redaction policy %d: %w", order, err)
		}
		sequential = append(sequential, redactor)
		for name, match := range redactor.replacementByName {
			if _, exists := replacements[name]; exists {
				useSequential = true
			}
			match.order = order
			replacements[name] = match
		}
	}
	for order, redactor := range sequential[:len(sequential)-1] {
		if !compositeReplacementIsOpaque(redactor.replacement) {
			useSequential = true
		}
		if later, markerBecomesLaterField := replacements[redactor.replacement]; markerBecomesLaterField &&
			later.order > order {
			useSequential = true
		}
	}

	result := &Validator{
		limits:            limits,
		replacement:       sequential[0].replacement,
		depthReplacement:  sequential[len(sequential)-1].replacement,
		replacementByName: replacements,
		ordered:           sequential,
		exact:             true,
	}
	if useSequential {
		result.sequential = sequential
	}
	return result, nil
}

func compositeReplacementIsOpaque(replacement string) bool {
	// These bytes can create assignments, quoted/encoded strings, JSON
	// composites, or new text records that a later historical pass would
	// reinterpret. Ordinary visible markers such as [REDACTED], ***, and
	// <MASKED> stay on the single-pass path.
	return !strings.ContainsAny(replacement, "=:;'\"\\{},\r\n\t")
}

func newValidator(limits Limits, policy RedactionPolicy, mandatory, exact bool) (*Validator, error) {
	if err := limits.validate(); err != nil {
		return nil, fmt.Errorf("invalid ingestion limits: %w", err)
	}
	replacement := policy.Replacement
	if replacement == "" {
		replacement = DefaultRedactionReplacement
	}
	if !utf8.ValidString(replacement) {
		return nil, fmt.Errorf("redaction replacement is not valid UTF-8")
	}
	sensitive := sensitiveFieldSet(policy.AdditionalSensitiveFields, mandatory, exact)
	var replacements map[string]redactionMatch
	if exact {
		replacements = make(map[string]redactionMatch, len(sensitive))
		for name := range sensitive {
			replacements[name] = redactionMatch{
				kind:        rawSecretKindForName(name),
				replacement: replacement,
			}
		}
		sensitive = nil
	}
	return &Validator{
		limits:            limits,
		replacement:       replacement,
		depthReplacement:  replacement,
		sensitive:         sensitive,
		replacementByName: replacements,
		mandatory:         mandatory,
		exact:             exact,
	}, nil
}

// RedactEvent returns an independent clone with the validator's mandatory and
// deployment-specific redaction policy applied to structured fields, raw
// bytes, and the canonical message. It performs no validation and preserves
// event identity and provenance. Use it when aliases may exist; collectors can
// use RedactEventInPlace for exclusively owned decoded events.
func (v *Validator) RedactEvent(event *opensplunkv1.LogEvent) *opensplunkv1.LogEvent {
	if event == nil {
		return nil
	}
	cloned := proto.Clone(event).(*opensplunkv1.LogEvent)
	return v.RedactEventInPlace(cloned)
}

// RedactEventInPlace applies the validator's redaction policy to an event owned
// exclusively by the caller. It avoids a full protobuf clone at collector
// durability boundaries, where the decoded or pipeline-produced event has not
// been shared. Call RedactEvent when aliases may exist.
func (v *Validator) RedactEventInPlace(event *opensplunkv1.LogEvent) *opensplunkv1.LogEvent {
	if event == nil {
		return nil
	}
	if len(v.sequential) > 0 {
		for _, redactor := range v.sequential {
			event = redactor.RedactEventInPlace(event)
		}
		return event
	}
	v.redactObject(event.GetFields())
	event.Raw = v.redactEventRaw(event.GetRaw(), event.GetRawEncoding())
	if event.Message != nil {
		redactedMessage := string(v.redactText([]byte(event.GetMessage())))
		event.Message = &redactedMessage
	}
	return event
}

// TopLevelAliasRedaction is one event-specific rename lineage that reached a
// sensitive name. StructuredOnly marks a configured constant: its value
// participates in the processor chain, but same-named raw/message content does
// not share that provenance.
type TopLevelAliasRedaction struct {
	Field          string
	Replacement    string
	StructuredOnly bool
}

// RedactTopLevelAliasesInPlace sanitizes all active top-level rename aliases in
// one structured/raw pass. Exact root-name matching preserves normalized and
// nested lookalikes. Non-JSON raw text and the canonical message share one
// composite exact-name resolver across distinct ordinary replacement markers;
// syntax-bearing compatibility cases retain the historical ordered fallback.
func RedactTopLevelAliasesInPlace(
	event *opensplunkv1.LogEvent,
	policies []TopLevelAliasRedaction,
) *opensplunkv1.LogEvent {
	if event == nil || len(policies) == 0 {
		return event
	}
	structured := make(map[string]string, len(policies))
	raw := make(map[string]string, len(policies))
	type textGroup struct {
		fields      map[string]struct{}
		replacement string
	}
	var groups []textGroup
	groupIndexes := make(map[string]int)
	for _, policy := range policies {
		structured[policy.Field] = policy.Replacement
		if policy.StructuredOnly {
			continue
		}
		raw[policy.Field] = policy.Replacement
		groupIndex, exists := groupIndexes[policy.Replacement]
		if !exists {
			groupIndex = len(groups)
			groupIndexes[policy.Replacement] = groupIndex
			fields := make(map[string]struct{})
			groups = append(groups, textGroup{
				fields:      fields,
				replacement: policy.Replacement,
			})
		}
		groups[groupIndex].fields[policy.Field] = struct{}{}
	}
	redactTopLevelObjectWithReplacements(event.GetFields(), structured)
	if len(raw) == 0 {
		return event
	}
	redactRawAsText := false
	if event.GetRawEncoding() == opensplunkv1.RawEncoding_RAW_ENCODING_UTF8 || utf8.Valid(event.GetRaw()) {
		if redacted, parsed := redactTopLevelJSONWithReplacements(event.GetRaw(), raw); parsed {
			event.Raw = redacted
		} else {
			redactRawAsText = len(event.GetRaw()) > 0
		}
	} else {
		redactRawAsText = len(event.GetRaw()) > 0
	}
	if !redactRawAsText && event.Message == nil {
		return event
	}

	var textRedactor *Validator
	var fallbackRedactors []*Validator
	if len(groups) == 1 {
		textRedactor = newExactRedactorFromSet(
			DefaultLimits(),
			groups[0].fields,
			groups[0].replacement,
		)
	} else if len(groups) > 1 {
		textPolicies := make([]RedactionPolicy, len(groups))
		requiresLiteralReplacement := false
		for index, group := range groups {
			fields := make([]string, 0, len(group.fields))
			for field := range group.fields {
				fields = append(fields, field)
			}
			textPolicies[index] = RedactionPolicy{
				AdditionalSensitiveFields: fields,
				Replacement:               group.replacement,
			}
			requiresLiteralReplacement = requiresLiteralReplacement || group.replacement == ""
		}
		if !requiresLiteralReplacement {
			candidate, err := NewCompositeSupplementalRedactor(DefaultLimits(), textPolicies)
			if err == nil {
				textRedactor = candidate
			}
		}
		if textRedactor == nil {
			// Collector-created aliases already hold validated replacements.
			// Preserve the historical unchecked helper's literal empty/invalid
			// replacement behavior for any other package caller.
			fallbackRedactors = make([]*Validator, 0, len(groups))
			for _, group := range groups {
				fallbackRedactors = append(fallbackRedactors, newExactRedactorFromSet(
					DefaultLimits(),
					group.fields,
					group.replacement,
				))
			}
		}
	}

	if redactRawAsText {
		if textRedactor != nil {
			event.Raw = textRedactor.redactKeyValueText(event.GetRaw())
		} else {
			for _, redactor := range fallbackRedactors {
				event.Raw = redactor.redactKeyValueText(event.GetRaw())
			}
		}
	}
	if event.Message != nil {
		message := []byte(event.GetMessage())
		if textRedactor != nil {
			message = textRedactor.redactText(message)
		} else {
			for _, redactor := range fallbackRedactors {
				message = redactor.redactText(message)
			}
		}
		redactedMessage := string(message)
		event.Message = &redactedMessage
	}
	return event
}

func newExactRedactorFromSet(
	limits Limits,
	fields map[string]struct{},
	replacement string,
) *Validator {
	replacements := make(map[string]redactionMatch, len(fields))
	for name := range fields {
		replacements[name] = redactionMatch{
			kind:        rawSecretKindForName(name),
			replacement: replacement,
		}
	}
	return &Validator{
		limits:            limits,
		replacement:       replacement,
		depthReplacement:  replacement,
		replacementByName: replacements,
		exact:             true,
	}
}

// ValidateAndNormalizeEvent validates a collector event and returns an
// independent, recursively redacted clone with server-derived metadata.
func (v *Validator) ValidateAndNormalizeEvent(event *opensplunkv1.LogEvent, ctx EventContext) (*StoredEvent, *EventError) {
	if event == nil {
		return nil, eventFailure(
			opensplunkv1.EventRejectionCode_EVENT_REJECTION_CODE_VALUE_INVALID,
			"event is required", "event", "required",
		)
	}
	if uint64(proto.Size(event)) > v.limits.MaxEventBytes {
		return nil, eventFailure(
			opensplunkv1.EventRejectionCode_EVENT_REJECTION_CODE_EVENT_TOO_LARGE,
			"event exceeds the maximum encoded size", "event", "event_too_large",
		)
	}
	if !validIdentifier(event.GetEventId(), v.limits.MaxIDBytes) {
		return nil, eventFailure(
			opensplunkv1.EventRejectionCode_EVENT_REJECTION_CODE_INVALID_EVENT_ID,
			"event_id is empty or has an invalid format", "event_id", "invalid_event_id",
		)
	}
	if !validIdentifier(event.GetIndexName(), v.limits.MaxIDBytes) {
		return nil, eventFailure(
			opensplunkv1.EventRejectionCode_EVENT_REJECTION_CODE_INVALID_INDEX,
			"index_name is empty or has an invalid format", "index_name", "invalid_index",
		)
	}
	if ctx.ReceivedAt.IsZero() {
		return nil, eventFailure(
			opensplunkv1.EventRejectionCode_EVENT_REJECTION_CODE_INVALID_TIMESTAMP,
			"server receive time is required", "received_at", "required",
		)
	}
	timestampReference := ctx.TimestampReference
	if timestampReference.IsZero() {
		timestampReference = ctx.ReceivedAt
	}
	if err := v.validateTimestamp(event.GetEventTime(), timestampReference); err != nil {
		return nil, eventFailure(
			opensplunkv1.EventRejectionCode_EVENT_REJECTION_CODE_INVALID_TIMESTAMP,
			"event_time is outside the accepted bounds", "event_time", err.Error(),
		)
	}
	if err := v.validateTimestamp(event.GetCollectedAt(), timestampReference); err != nil {
		return nil, eventFailure(
			opensplunkv1.EventRejectionCode_EVENT_REJECTION_CODE_INVALID_TIMESTAMP,
			"collected_at is outside the accepted bounds", "collected_at", err.Error(),
		)
	}
	if event.GetEventTimeSource() < opensplunkv1.EventTimeSource_EVENT_TIME_SOURCE_PARSED ||
		event.GetEventTimeSource() > opensplunkv1.EventTimeSource_EVENT_TIME_SOURCE_RECEIVED_AT_FALLBACK {
		return nil, eventFailure(
			opensplunkv1.EventRejectionCode_EVENT_REJECTION_CODE_VALUE_INVALID,
			"event_time_source is invalid", "event_time_source", "invalid_enum",
		)
	}
	if event.GetRawEncoding() != opensplunkv1.RawEncoding_RAW_ENCODING_UTF8 &&
		event.GetRawEncoding() != opensplunkv1.RawEncoding_RAW_ENCODING_BINARY {
		return nil, eventFailure(
			opensplunkv1.EventRejectionCode_EVENT_REJECTION_CODE_VALUE_INVALID,
			"raw_encoding is invalid", "raw_encoding", "invalid_enum",
		)
	}
	if event.GetRawEncoding() == opensplunkv1.RawEncoding_RAW_ENCODING_UTF8 && !utf8.Valid(event.GetRaw()) {
		return nil, eventFailure(
			opensplunkv1.EventRejectionCode_EVENT_REJECTION_CODE_VALUE_INVALID,
			"UTF-8 raw data contains invalid bytes", "raw", "invalid_utf8",
		)
	}
	if err := validateEventStrings(event); err != nil {
		return nil, err
	}
	if err := v.validateObject(event.GetFields(), "fields", 1, true, new(uint32)); err != nil {
		return nil, err
	}

	cloned := v.RedactEvent(event)
	if !storedFieldNamesFit(cloned.GetFields()) {
		return nil, eventFailure(
			opensplunkv1.EventRejectionCode_EVENT_REJECTION_CODE_EVENT_TOO_LARGE,
			"flattened field metadata exceeds the durable event limit",
			"fields",
			"field_metadata_too_large",
		)
	}
	if uint64(proto.Size(cloned)) > v.limits.MaxEventBytes {
		return nil, eventFailure(
			opensplunkv1.EventRejectionCode_EVENT_REJECTION_CODE_EVENT_TOO_LARGE,
			"event exceeds the maximum encoded size after mandatory redaction", "event", "event_too_large_after_redaction",
		)
	}
	return &StoredEvent{
		Event:       cloned,
		TenantID:    ctx.TenantID,
		CollectorID: ctx.CollectorID,
		BatchID:     ctx.BatchID,
		IndexTime:   ctx.ReceivedAt.UTC(),
	}, nil
}

func storedFieldNamesFit(object *opensplunkv1.TypedObject) bool {
	remaining := eventfields.MaximumStoredFieldNamesBytes
	return consumeStoredFieldNameBytes(object, 0, &remaining)
}

func consumeStoredFieldNameBytes(
	object *opensplunkv1.TypedObject,
	prefixBytes int,
	remaining *int,
) bool {
	if object == nil {
		return true
	}
	for _, field := range object.GetFields() {
		pathBytes := eventfields.NormalizedDynamicPathBytes(prefixBytes, field.GetName())
		if nested, ok := field.GetValue().GetKind().(*opensplunkv1.TypedValue_ObjectValue); ok &&
			nested.ObjectValue != nil && len(nested.ObjectValue.GetFields()) != 0 {
			if !consumeStoredFieldNameBytes(nested.ObjectValue, pathBytes, remaining) {
				return false
			}
			continue
		}
		if pathBytes > *remaining {
			return false
		}
		*remaining -= pathBytes
	}
	return true
}

func (v *Validator) validateTimestamp(ts *timestamppb.Timestamp, now time.Time) error {
	if ts == nil {
		return errors.New("required")
	}
	if err := ts.CheckValid(); err != nil {
		return errors.New("invalid_protobuf_timestamp")
	}
	value := ts.AsTime()
	if value.Before(now.Add(-v.limits.MaxEventAge)) {
		return errors.New("timestamp_too_old")
	}
	if value.After(now.Add(v.limits.MaxFutureSkew)) {
		return errors.New("timestamp_too_far_in_future")
	}
	return nil
}

func (v *Validator) validateObject(object *opensplunkv1.TypedObject, path string, depth uint32, root bool, count *uint32) *EventError {
	if object == nil {
		return nil
	}
	if depth > v.limits.MaxNestingDepth {
		return eventFailure(
			opensplunkv1.EventRejectionCode_EVENT_REJECTION_CODE_NESTING_TOO_DEEP,
			"typed value nesting exceeds the configured limit", path, "nesting_too_deep",
		)
	}
	seen := make(map[string]struct{}, len(object.GetFields()))
	for i, field := range object.GetFields() {
		fieldPath := fmt.Sprintf("%s[%d]", path, i)
		if field == nil {
			return eventFailure(
				opensplunkv1.EventRejectionCode_EVENT_REJECTION_CODE_VALUE_INVALID,
				"typed object contains a nil field", fieldPath, "required",
			)
		}
		fieldPath = joinFieldPath(path, field.GetName(), i)
		if errCode := validateFieldName(field.GetName(), v.limits.MaxFieldNameBytes); errCode != "" {
			return eventFailure(
				opensplunkv1.EventRejectionCode_EVENT_REJECTION_CODE_FIELD_NAME_INVALID,
				"typed object field name is invalid", fieldPath, errCode,
			)
		}
		if _, duplicate := seen[field.GetName()]; duplicate {
			return eventFailure(
				opensplunkv1.EventRejectionCode_EVENT_REJECTION_CODE_FIELD_NAME_INVALID,
				"duplicate field name in typed object", fieldPath, "duplicate_field_name",
			)
		}
		seen[field.GetName()] = struct{}{}
		if root && eventfields.IsReservedDynamicRoot(field.GetName()) {
			return eventFailure(
				opensplunkv1.EventRejectionCode_EVENT_REJECTION_CODE_FIELD_NAME_INVALID,
				"dynamic field cannot override canonical event metadata", fieldPath, "canonical_field_reserved",
			)
		}
		*count++
		if *count > v.limits.MaxFields {
			return eventFailure(
				opensplunkv1.EventRejectionCode_EVENT_REJECTION_CODE_TOO_MANY_FIELDS,
				"typed object contains too many fields", fieldPath, "too_many_fields",
			)
		}
		if err := v.validateValue(field.GetValue(), fieldPath, depth, count); err != nil {
			return err
		}
	}
	return nil
}

func (v *Validator) validateValue(value *opensplunkv1.TypedValue, path string, depth uint32, count *uint32) *EventError {
	if value == nil || value.GetKind() == nil {
		return eventFailure(
			opensplunkv1.EventRejectionCode_EVENT_REJECTION_CODE_VALUE_INVALID,
			"typed value kind is required", path, "value_kind_required",
		)
	}
	switch kind := value.GetKind().(type) {
	case *opensplunkv1.TypedValue_NullValue:
		if kind.NullValue != opensplunkv1.NullValue_NULL_VALUE_NULL {
			return invalidTypedValue(path, "invalid_null")
		}
	case *opensplunkv1.TypedValue_StringValue:
		if !utf8.ValidString(kind.StringValue) {
			return invalidTypedValue(path, "invalid_utf8")
		}
	case *opensplunkv1.TypedValue_Sint64Value, *opensplunkv1.TypedValue_Uint64Value,
		*opensplunkv1.TypedValue_BoolValue, *opensplunkv1.TypedValue_BytesValue:
		return nil
	case *opensplunkv1.TypedValue_DoubleValue:
		if math.IsNaN(kind.DoubleValue) || math.IsInf(kind.DoubleValue, 0) {
			return invalidTypedValue(path, "non_finite_double")
		}
	case *opensplunkv1.TypedValue_TimestampValue:
		if kind.TimestampValue == nil || kind.TimestampValue.CheckValid() != nil {
			return invalidTypedValue(path, "invalid_timestamp")
		}
	case *opensplunkv1.TypedValue_DurationValue:
		if !DurationFitsResultRange(kind.DurationValue) {
			return invalidTypedValue(path, "invalid_duration")
		}
	case *opensplunkv1.TypedValue_ListValue:
		if kind.ListValue == nil {
			return invalidTypedValue(path, "list_required")
		}
		if depth+1 > v.limits.MaxNestingDepth {
			return eventFailure(
				opensplunkv1.EventRejectionCode_EVENT_REJECTION_CODE_NESTING_TOO_DEEP,
				"typed value nesting exceeds the configured limit", path, "nesting_too_deep",
			)
		}
		for i, item := range kind.ListValue.GetValues() {
			if err := v.validateValue(item, fmt.Sprintf("%s[%d]", path, i), depth+1, count); err != nil {
				return err
			}
		}
	case *opensplunkv1.TypedValue_ObjectValue:
		if kind.ObjectValue == nil {
			return invalidTypedValue(path, "object_required")
		}
		return v.validateObject(kind.ObjectValue, path, depth+1, false, count)
	case *opensplunkv1.TypedValue_DecimalValue:
		if kind.DecimalValue == nil || !decimalPattern.MatchString(kind.DecimalValue.GetValue()) {
			return invalidTypedValue(path, "invalid_decimal")
		}
	case *opensplunkv1.TypedValue_MissingValue:
		return invalidTypedValue(path, "missing_not_storable")
	default:
		return invalidTypedValue(path, "unknown_value_kind")
	}
	return nil
}

func validateEventStrings(event *opensplunkv1.LogEvent) *EventError {
	fields := []struct {
		path  string
		value string
	}{
		{"host", event.GetHost()},
		{"source", event.GetSource()},
		{"sourcetype", event.GetSourcetype()},
		{"service", event.GetService()},
		{"level", event.GetLevel()},
		{"message", event.GetMessage()},
		{"trace_id", event.GetTraceId()},
		{"span_id", event.GetSpanId()},
	}
	for _, field := range fields {
		if !utf8.ValidString(field.value) {
			return eventFailure(
				opensplunkv1.EventRejectionCode_EVENT_REJECTION_CODE_VALUE_INVALID,
				"event string contains invalid UTF-8", field.path, "invalid_utf8",
			)
		}
	}
	if event.GetSeverity() < opensplunkv1.LogSeverity_LOG_SEVERITY_UNSPECIFIED ||
		event.GetSeverity() > opensplunkv1.LogSeverity_LOG_SEVERITY_FATAL {
		return eventFailure(
			opensplunkv1.EventRejectionCode_EVENT_REJECTION_CODE_VALUE_INVALID,
			"severity is invalid", "severity", "invalid_enum",
		)
	}
	if origin := event.GetOrigin(); origin != nil {
		if origin.StartOffset != nil && origin.EndOffset != nil &&
			origin.GetEndOffset() < origin.GetStartOffset() {
			return eventFailure(
				opensplunkv1.EventRejectionCode_EVENT_REJECTION_CODE_VALUE_INVALID,
				"origin end_offset precedes start_offset", "origin.end_offset", "invalid_range",
			)
		}
		if origin.NextLineNumber != nil &&
			(origin.LineNumber == nil || origin.GetLineNumber() == 0 ||
				origin.GetNextLineNumber() <= origin.GetLineNumber() ||
				origin.GetNextLineNumber() == math.MaxUint64) {
			return eventFailure(
				opensplunkv1.EventRejectionCode_EVENT_REJECTION_CODE_VALUE_INVALID,
				"origin next_line_number is invalid", "origin.next_line_number", "invalid_range",
			)
		}
	}
	return nil
}

func validateFieldName(name string, maxBytes uint32) string {
	if name == "" || strings.TrimSpace(name) != name {
		return "empty_or_surrounding_whitespace"
	}
	if !utf8.ValidString(name) {
		return "invalid_utf8"
	}
	if uint32(len(name)) > maxBytes {
		return "field_name_too_long"
	}
	for _, r := range name {
		if unicode.IsControl(r) {
			return "control_character"
		}
	}
	return ""
}

func validIdentifier(value string, maxBytes uint32) bool {
	if value == "" || uint32(len(value)) > maxBytes || !utf8.ValidString(value) {
		return false
	}
	for i, r := range value {
		if r > unicode.MaxASCII || !(unicode.IsLetter(r) || unicode.IsDigit(r) || (i > 0 && strings.ContainsRune("._:-", r))) {
			return false
		}
	}
	return true
}

func eventFailure(code opensplunkv1.EventRejectionCode, message, path, violationCode string) *EventError {
	return &EventError{
		Code:    code,
		Message: message,
		Violations: []*opensplunkv1.FieldViolation{{
			FieldPath: path,
			Code:      violationCode,
			Message:   message,
		}},
	}
}

func invalidTypedValue(path, code string) *EventError {
	return eventFailure(
		opensplunkv1.EventRejectionCode_EVENT_REJECTION_CODE_VALUE_INVALID,
		"typed field value is invalid", path, code,
	)
}

func joinFieldPath(parent, name string, index int) string {
	if name == "" {
		return fmt.Sprintf("%s[%d]", parent, index)
	}
	return parent + "." + name
}

// EventIDDigest implements the collector protocol's length-prefixed SHA-256
// digest over ordered event IDs.
func EventIDDigest(events []*opensplunkv1.LogEvent) []byte {
	h := sha256.New()
	var length [4]byte
	for _, event := range events {
		id := ""
		if event != nil {
			id = event.GetEventId()
		}
		binary.BigEndian.PutUint32(length[:], uint32(len(id)))
		_, _ = h.Write(length[:])
		_, _ = h.Write([]byte(id))
	}
	return h.Sum(nil)
}

// UncompressedEventBytes is the deterministic sum of protobuf-encoded event
// sizes used by EventBatch.uncompressed_size_bytes.
func UncompressedEventBytes(events []*opensplunkv1.LogEvent) uint64 {
	var total uint64
	for _, event := range events {
		size := uint64(proto.Size(event))
		if math.MaxUint64-total < size {
			return math.MaxUint64
		}
		total += size
	}
	return total
}
