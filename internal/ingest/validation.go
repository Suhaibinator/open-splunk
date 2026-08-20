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

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/collectorlimits"
	"github.com/Suhaibinator/open-splunk/internal/eventfields"
	"github.com/Suhaibinator/open-splunk/internal/indexpolicy"
	"github.com/Suhaibinator/open-splunk/internal/protocolid"
	"github.com/Suhaibinator/open-splunk/internal/sha256hex"
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
	orderedOnChange   bool
	mandatory         bool
	exact             bool
}

func NewValidator(limits Limits, policy RedactionPolicy) (*Validator, error) {
	return newValidator(limits, policy, true, false)
}

// withLimits returns a detached validator view which shares only immutable
// redaction lookup tables. Authorization admission constructs these copies
// once per resolved index policy, never in the per-event path.
func (v *Validator) withLimits(limits Limits) Validator {
	cloned := *v
	cloned.limits = limits
	return cloned
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
// later field retain that ordered implementation because they can
// intentionally create a match for a later policy. Each independent typed
// field, raw payload, and message first uses the composite resolver to detect
// a possible change. A directly named typed field starts at its last matching
// policy because that replacement discards every earlier result; affected text
// surfaces replay the complete historical chain. Definite misses and
// duplicate-key-only JSON canonicalization avoid scanning once per policy.
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

	ordered := make([]*Validator, 0, len(policies))
	replacements := make(map[string]redactionMatch)
	orderedOnChange := false
	for order, policy := range policies {
		redactor, err := NewSupplementalRedactor(limits, policy)
		if err != nil {
			return nil, fmt.Errorf("supplemental redaction policy %d: %w", order, err)
		}
		ordered = append(ordered, redactor)
		for name, match := range redactor.replacementByName {
			if _, exists := replacements[name]; exists {
				orderedOnChange = true
			}
			match.order = order
			replacements[name] = match
		}
	}
	for order, redactor := range ordered[:len(ordered)-1] {
		if !compositeReplacementIsOpaque(redactor.replacement) {
			orderedOnChange = true
		}
		if later, markerBecomesLaterField := replacements[redactor.replacement]; markerBecomesLaterField &&
			later.order > order {
			orderedOnChange = true
		}
	}

	result := &Validator{
		limits:            limits,
		replacement:       ordered[0].replacement,
		depthReplacement:  ordered[len(ordered)-1].replacement,
		replacementByName: replacements,
		ordered:           ordered,
		orderedOnChange:   orderedOnChange,
		exact:             true,
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
func (v *Validator) RedactEvent(event *opensplunk.LogEvent) *opensplunk.LogEvent {
	if event == nil {
		return nil
	}
	cloned := proto.Clone(event).(*opensplunk.LogEvent)
	return v.RedactEventInPlace(cloned)
}

// RedactEventInPlace applies the validator's redaction policy to an event owned
// exclusively by the caller. It avoids a full protobuf clone at collector
// durability boundaries, where the decoded or pipeline-produced event has not
// been shared. Call RedactEvent when aliases may exist.
func (v *Validator) RedactEventInPlace(event *opensplunk.LogEvent) *opensplunk.LogEvent {
	if event == nil {
		return nil
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
	event *opensplunk.LogEvent,
	policies []TopLevelAliasRedaction,
) *opensplunk.LogEvent {
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
	if event.GetRawEncoding() == opensplunk.RawEncoding_RAW_ENCODING_UTF8 || utf8.Valid(event.GetRaw()) {
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
func (v *Validator) ValidateAndNormalizeEvent(event *opensplunk.LogEvent, ctx EventContext) (*StoredEvent, *EventError) {
	size, sizeOK := protobufSizeUint64(event)
	if !sizeOK {
		return nil, eventFailure(
			opensplunk.EventRejectionCode_EVENT_REJECTION_CODE_EVENT_TOO_LARGE,
			"event exceeds the maximum encoded size", "event", "event_too_large",
		)
	}
	return v.validateAndNormalizeEventWithSize(event, ctx, size)
}

// validateAndNormalizeEventWithSize is ValidateAndNormalizeEvent for callers
// which already computed the exact serialized event size.
func (v *Validator) validateAndNormalizeEventWithSize(event *opensplunk.LogEvent, ctx EventContext, size uint64) (*StoredEvent, *EventError) {
	if event == nil {
		return nil, eventFailure(
			opensplunk.EventRejectionCode_EVENT_REJECTION_CODE_VALUE_INVALID,
			"event is required", "event", "required",
		)
	}
	if size > v.limits.MaxEventBytes {
		return nil, eventFailure(
			opensplunk.EventRejectionCode_EVENT_REJECTION_CODE_EVENT_TOO_LARGE,
			"event exceeds the maximum encoded size", "event", "event_too_large",
		)
	}
	if !validIdentifier(event.GetEventId(), v.limits.MaxIDBytes) {
		return nil, eventFailure(
			opensplunk.EventRejectionCode_EVENT_REJECTION_CODE_INVALID_EVENT_ID,
			"event_id is empty or has an invalid format", "event_id", "invalid_event_id",
		)
	}
	if !validIndexName(event.GetIndexName()) {
		return nil, eventFailure(
			opensplunk.EventRejectionCode_EVENT_REJECTION_CODE_INVALID_INDEX,
			"index_name is empty or has an invalid format", "index_name", "invalid_index",
		)
	}
	if ctx.ReceivedAt.IsZero() {
		return nil, eventFailure(
			opensplunk.EventRejectionCode_EVENT_REJECTION_CODE_INVALID_TIMESTAMP,
			"server receive time is required", "received_at", "required",
		)
	}
	timestampReference := ctx.TimestampReference
	if timestampReference.IsZero() {
		timestampReference = ctx.ReceivedAt
	}
	if err := v.validateTimestamp(event.GetEventTime(), timestampReference); err != nil {
		return nil, eventFailure(
			opensplunk.EventRejectionCode_EVENT_REJECTION_CODE_INVALID_TIMESTAMP,
			"event_time is outside the accepted bounds", "event_time", err.Error(),
		)
	}
	if err := v.validateTimestamp(event.GetCollectedAt(), timestampReference); err != nil {
		return nil, eventFailure(
			opensplunk.EventRejectionCode_EVENT_REJECTION_CODE_INVALID_TIMESTAMP,
			"collected_at is outside the accepted bounds", "collected_at", err.Error(),
		)
	}
	if event.GetEventTimeSource() < opensplunk.EventTimeSource_EVENT_TIME_SOURCE_PARSED ||
		event.GetEventTimeSource() > opensplunk.EventTimeSource_EVENT_TIME_SOURCE_RECEIVED_AT_FALLBACK {
		return nil, eventFailure(
			opensplunk.EventRejectionCode_EVENT_REJECTION_CODE_VALUE_INVALID,
			"event_time_source is invalid", "event_time_source", "invalid_enum",
		)
	}
	if event.GetRawEncoding() != opensplunk.RawEncoding_RAW_ENCODING_UTF8 &&
		event.GetRawEncoding() != opensplunk.RawEncoding_RAW_ENCODING_BINARY {
		return nil, eventFailure(
			opensplunk.EventRejectionCode_EVENT_REJECTION_CODE_VALUE_INVALID,
			"raw_encoding is invalid", "raw_encoding", "invalid_enum",
		)
	}
	if event.GetRawEncoding() == opensplunk.RawEncoding_RAW_ENCODING_UTF8 && !utf8.Valid(event.GetRaw()) {
		return nil, eventFailure(
			opensplunk.EventRejectionCode_EVENT_REJECTION_CODE_VALUE_INVALID,
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
	if cloned.GetSourcetype() == "" && ctx.DefaultSourcetype != "" {
		cloned.Sourcetype = ctx.DefaultSourcetype
	}
	if !storedFieldNamesFit(cloned.GetFields()) {
		return nil, eventFailure(
			opensplunk.EventRejectionCode_EVENT_REJECTION_CODE_EVENT_TOO_LARGE,
			"flattened field metadata exceeds the durable event limit",
			"fields",
			"field_metadata_too_large",
		)
	}
	size, sizeOK := protobufSizeUint64(cloned)
	if !sizeOK || size > v.limits.MaxEventBytes {
		return nil, eventFailure(
			opensplunk.EventRejectionCode_EVENT_REJECTION_CODE_EVENT_TOO_LARGE,
			"event exceeds the maximum encoded size after mandatory redaction", "event", "event_too_large_after_redaction",
		)
	}
	source := ctx.Source
	if source != (IngestionSource{}) || ctx.CollectorID != "" {
		var sourceErr error
		source, sourceErr = CanonicalIngestionSource(source, ctx.CollectorID)
		if sourceErr != nil {
			return nil, eventFailure(
				opensplunk.EventRejectionCode_EVENT_REJECTION_CODE_VALUE_INVALID,
				"server ingestion source is invalid", "event", "invalid_ingestion_source",
			)
		}
	}
	return &StoredEvent{
		Event:       cloned,
		TenantID:    ctx.TenantID,
		Source:      source,
		CollectorID: ctx.CollectorID,
		BatchID:     ctx.BatchID,
		IndexTime:   ctx.ReceivedAt.UTC(),
	}, nil
}

func storedFieldNamesFit(object *opensplunk.TypedObject) bool {
	remaining := eventfields.MaximumStoredFieldNamesBytes
	return consumeStoredFieldNameBytes(object, 0, &remaining)
}

func consumeStoredFieldNameBytes(
	object *opensplunk.TypedObject,
	prefixBytes int,
	remaining *int,
) bool {
	if object == nil {
		return true
	}
	for _, field := range object.GetFields() {
		pathBytes := eventfields.NormalizedDynamicPathBytes(prefixBytes, field.GetName())
		if nested, ok := field.GetValue().GetKind().(*opensplunk.TypedValue_ObjectValue); ok &&
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

func (v *Validator) validateObject(object *opensplunk.TypedObject, path string, depth uint32, root bool, count *uint32) *EventError {
	if object == nil {
		return nil
	}
	if depth > v.limits.MaxNestingDepth {
		return eventFailure(
			opensplunk.EventRejectionCode_EVENT_REJECTION_CODE_NESTING_TOO_DEEP,
			"typed value nesting exceeds the configured limit", path, "nesting_too_deep",
		)
	}
	seen := make(map[string]struct{}, len(object.GetFields()))
	for i, field := range object.GetFields() {
		if field == nil {
			return eventFailure(
				opensplunk.EventRejectionCode_EVENT_REJECTION_CODE_VALUE_INVALID,
				"typed object contains a nil field", fmt.Sprintf("%s[%d]", path, i), "required",
			)
		}
		fieldPath := joinFieldPath(path, field.GetName(), i)
		if errCode := validateFieldName(field.GetName(), v.limits.MaxFieldNameBytes); errCode != "" {
			return eventFailure(
				opensplunk.EventRejectionCode_EVENT_REJECTION_CODE_FIELD_NAME_INVALID,
				"typed object field name is invalid", fieldPath, errCode,
			)
		}
		if _, duplicate := seen[field.GetName()]; duplicate {
			return eventFailure(
				opensplunk.EventRejectionCode_EVENT_REJECTION_CODE_FIELD_NAME_INVALID,
				"duplicate field name in typed object", fieldPath, "duplicate_field_name",
			)
		}
		seen[field.GetName()] = struct{}{}
		if root && eventfields.IsReservedDynamicRoot(field.GetName()) {
			return eventFailure(
				opensplunk.EventRejectionCode_EVENT_REJECTION_CODE_FIELD_NAME_INVALID,
				"dynamic field cannot override canonical event metadata", fieldPath, "canonical_field_reserved",
			)
		}
		*count++
		if *count > v.limits.MaxFields {
			return eventFailure(
				opensplunk.EventRejectionCode_EVENT_REJECTION_CODE_TOO_MANY_FIELDS,
				"typed object contains too many fields", fieldPath, "too_many_fields",
			)
		}
		if err := v.validateValue(field.GetValue(), fieldPath, depth, count); err != nil {
			return err
		}
	}
	return nil
}

func (v *Validator) validateValue(value *opensplunk.TypedValue, path string, depth uint32, count *uint32) *EventError {
	if value == nil || value.GetKind() == nil {
		return eventFailure(
			opensplunk.EventRejectionCode_EVENT_REJECTION_CODE_VALUE_INVALID,
			"typed value kind is required", path, "value_kind_required",
		)
	}
	switch kind := value.GetKind().(type) {
	case *opensplunk.TypedValue_NullValue:
		if kind.NullValue != opensplunk.NullValue_NULL_VALUE_NULL {
			return invalidTypedValue(path, "invalid_null")
		}
	case *opensplunk.TypedValue_StringValue:
		if !utf8.ValidString(kind.StringValue) {
			return invalidTypedValue(path, "invalid_utf8")
		}
	case *opensplunk.TypedValue_Sint64Value, *opensplunk.TypedValue_Uint64Value,
		*opensplunk.TypedValue_BoolValue, *opensplunk.TypedValue_BytesValue:
		return nil
	case *opensplunk.TypedValue_DoubleValue:
		if math.IsNaN(kind.DoubleValue) || math.IsInf(kind.DoubleValue, 0) {
			return invalidTypedValue(path, "non_finite_double")
		}
	case *opensplunk.TypedValue_TimestampValue:
		if kind.TimestampValue == nil || kind.TimestampValue.CheckValid() != nil {
			return invalidTypedValue(path, "invalid_timestamp")
		}
	case *opensplunk.TypedValue_DurationValue:
		if !DurationFitsResultRange(kind.DurationValue) {
			return invalidTypedValue(path, "invalid_duration")
		}
	case *opensplunk.TypedValue_ListValue:
		if kind.ListValue == nil {
			return invalidTypedValue(path, "list_required")
		}
		if depth+1 > v.limits.MaxNestingDepth {
			return eventFailure(
				opensplunk.EventRejectionCode_EVENT_REJECTION_CODE_NESTING_TOO_DEEP,
				"typed value nesting exceeds the configured limit", path, "nesting_too_deep",
			)
		}
		for i, item := range kind.ListValue.GetValues() {
			if err := v.validateValue(item, fmt.Sprintf("%s[%d]", path, i), depth+1, count); err != nil {
				return err
			}
		}
	case *opensplunk.TypedValue_ObjectValue:
		if kind.ObjectValue == nil {
			return invalidTypedValue(path, "object_required")
		}
		return v.validateObject(kind.ObjectValue, path, depth+1, false, count)
	case *opensplunk.TypedValue_DecimalValue:
		if kind.DecimalValue == nil || !decimalPattern.MatchString(kind.DecimalValue.GetValue()) {
			return invalidTypedValue(path, "invalid_decimal")
		}
	case *opensplunk.TypedValue_MissingValue:
		return invalidTypedValue(path, "missing_not_storable")
	default:
		return invalidTypedValue(path, "unknown_value_kind")
	}
	return nil
}

func validateEventStrings(event *opensplunk.LogEvent) *EventError {
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
				opensplunk.EventRejectionCode_EVENT_REJECTION_CODE_VALUE_INVALID,
				"event string contains invalid UTF-8", field.path, "invalid_utf8",
			)
		}
	}
	if event.GetSeverity() < opensplunk.LogSeverity_LOG_SEVERITY_UNSPECIFIED ||
		event.GetSeverity() > opensplunk.LogSeverity_LOG_SEVERITY_FATAL {
		return eventFailure(
			opensplunk.EventRejectionCode_EVENT_REJECTION_CODE_VALUE_INVALID,
			"severity is invalid", "severity", "invalid_enum",
		)
	}
	if origin := event.GetOrigin(); origin != nil {
		if origin.StartOffset != nil && origin.EndOffset != nil &&
			origin.GetEndOffset() < origin.GetStartOffset() {
			return eventFailure(
				opensplunk.EventRejectionCode_EVENT_REJECTION_CODE_VALUE_INVALID,
				"origin end_offset precedes start_offset", "origin.end_offset", "invalid_range",
			)
		}
		if origin.NextLineNumber != nil &&
			(origin.LineNumber == nil || origin.GetLineNumber() == 0 ||
				origin.GetNextLineNumber() <= origin.GetLineNumber() ||
				origin.GetNextLineNumber() == math.MaxUint64) {
			return eventFailure(
				opensplunk.EventRejectionCode_EVENT_REJECTION_CODE_VALUE_INVALID,
				"origin next_line_number is invalid", "origin.next_line_number", "invalid_range",
			)
		}
		guardFingerprintPresent := origin.CheckpointGuardFingerprint != nil
		guardLengthPresent := origin.CheckpointGuardLength != nil
		if guardFingerprintPresent != guardLengthPresent {
			return eventFailure(
				opensplunk.EventRejectionCode_EVENT_REJECTION_CODE_VALUE_INVALID,
				"origin checkpoint rewrite guard is incomplete", "origin.checkpoint_guard_fingerprint", "required_together",
			)
		}
		if guardFingerprintPresent {
			fingerprint := origin.GetCheckpointGuardFingerprint()
			length := origin.GetCheckpointGuardLength()
			validDigest := sha256hex.Valid(fingerprint)
			if length == 0 || length > collectorlimits.MaximumCheckpointGuardBytes ||
				origin.EndOffset == nil || uint64(length) > origin.GetEndOffset() || !validDigest {
				return eventFailure(
					opensplunk.EventRejectionCode_EVENT_REJECTION_CODE_VALUE_INVALID,
					"origin checkpoint rewrite guard is invalid", "origin.checkpoint_guard_fingerprint", "invalid_rewrite_guard",
				)
			}
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
	length, ok := nonNegativeIntUint64(len(name))
	if !ok || length > uint64(maxBytes) {
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
	return protocolid.ValidWithMaximum(value, maxBytes)
}

func validIndexName(value string) bool {
	return indexpolicy.ValidName(value)
}

func eventFailure(code opensplunk.EventRejectionCode, message, path, violationCode string) *EventError {
	return &EventError{
		Code:    code,
		Message: message,
		Violations: []*opensplunk.FieldViolation{{
			FieldPath: path,
			Code:      violationCode,
			Message:   message,
		}},
	}
}

func invalidTypedValue(path, code string) *EventError {
	return eventFailure(
		opensplunk.EventRejectionCode_EVENT_REJECTION_CODE_VALUE_INVALID,
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
func EventIDDigest(events []*opensplunk.LogEvent) []byte {
	h := sha256.New()
	var length [4]byte
	for _, event := range events {
		id := ""
		if event != nil {
			id = event.GetEventId()
		}
		length64, ok := nonNegativeIntUint64(len(id))
		if !ok || length64 > math.MaxUint32 {
			return nil
		}
		// #nosec G115 -- the explicit math.MaxUint32 check above proves this safe.
		binary.BigEndian.PutUint32(length[:], uint32(len(id)))
		_, _ = h.Write(length[:])
		_, _ = h.Write([]byte(id))
	}
	return h.Sum(nil)
}

// UncompressedEventBytes is the deterministic sum of protobuf-encoded event
// sizes used by EventBatch.uncompressed_size_bytes.
func UncompressedEventBytes(events []*opensplunk.LogEvent) uint64 {
	var total uint64
	for _, event := range events {
		size, ok := protobufSizeUint64(event)
		if !ok {
			return math.MaxUint64
		}
		if math.MaxUint64-total < size {
			return math.MaxUint64
		}
		total += size
	}
	return total
}

func protobufSizeUint64(message proto.Message) (uint64, bool) {
	size := proto.Size(message)
	return nonNegativeIntUint64(size)
}

func nonNegativeIntUint64(value int) (uint64, bool) {
	if value < 0 {
		return 0, false
	}
	// #nosec G115 -- a non-negative Go int is exactly representable as uint64.
	return uint64(value), true
}
