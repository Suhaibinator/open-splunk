package ingest

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"math"
	"testing"
	"time"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var validationTestNow = time.Date(2026, 7, 21, 18, 0, 0, 0, time.UTC)

func TestValidateAndNormalizeEventDoesNotMutateInput(t *testing.T) {
	v := newTestValidator(t, DefaultLimits())
	event := validTestEvent("event-1", "main")
	want := proto.Clone(event).(*opensplunkv1.LogEvent)

	got, rejection := v.ValidateAndNormalizeEvent(event, EventContext{
		ReceivedAt:  validationTestNow,
		TenantID:    "tenant-a",
		CollectorID: "collector-a",
		BatchID:     "batch-a",
	})
	if rejection != nil {
		t.Fatalf("ValidateAndNormalizeEvent() rejection = %v", rejection)
	}
	if !proto.Equal(event, want) {
		t.Fatal("ValidateAndNormalizeEvent() mutated its input")
	}
	if got == nil || got.Event == event {
		t.Fatal("ValidateAndNormalizeEvent() must return an independent event")
	}
	if got.TenantID != "tenant-a" || got.CollectorID != "collector-a" || got.BatchID != "batch-a" {
		t.Fatalf("normalized server metadata = %#v", got)
	}
	if !got.IndexTime.Equal(validationTestNow) {
		t.Fatalf("IndexTime = %v, want %v", got.IndexTime, validationTestNow)
	}
}

func TestValidateAndNormalizeEventRejectsCanonicalFieldInjection(t *testing.T) {
	v := newTestValidator(t, DefaultLimits())
	event := validTestEvent("event-1", "main")
	event.Fields.Fields = append(event.Fields.Fields, stringField("_InDeXtImE", "forged"))

	_, rejection := v.ValidateAndNormalizeEvent(event, EventContext{ReceivedAt: validationTestNow})
	assertEventRejectionCode(t, rejection, opensplunkv1.EventRejectionCode_EVENT_REJECTION_CODE_FIELD_NAME_INVALID)
	if len(rejection.Violations) == 0 || rejection.Violations[0].FieldPath != "fields._InDeXtImE" {
		t.Fatalf("violations = %#v", rejection.Violations)
	}
}

func TestTypedObjectValidation(t *testing.T) {
	tests := []struct {
		name   string
		limits Limits
		fields *opensplunkv1.TypedObject
		code   opensplunkv1.EventRejectionCode
	}{
		{
			name: "duplicate names in nested object",
			fields: object(
				objectField("nested", object(stringField("same", "one"), stringField("same", "two"))),
			),
			code: opensplunkv1.EventRejectionCode_EVENT_REJECTION_CODE_FIELD_NAME_INVALID,
		},
		{
			name: "too many recursively counted fields",
			limits: func() Limits {
				limits := DefaultLimits()
				limits.MaxFields = 2
				return limits
			}(),
			fields: object(stringField("one", "1"), objectField("nested", object(stringField("two", "2")))),
			code:   opensplunkv1.EventRejectionCode_EVENT_REJECTION_CODE_TOO_MANY_FIELDS,
		},
		{
			name: "object nesting too deep",
			limits: func() Limits {
				limits := DefaultLimits()
				limits.MaxNestingDepth = 2
				return limits
			}(),
			fields: object(
				objectField("one", object(
					objectField("two", object(stringField("three", "value"))),
				)),
			),
			code: opensplunkv1.EventRejectionCode_EVENT_REJECTION_CODE_NESTING_TOO_DEEP,
		},
		{
			name:   "unset value kind",
			fields: object(&opensplunkv1.TypedObjectField{Name: "bad", Value: &opensplunkv1.TypedValue{}}),
			code:   opensplunkv1.EventRejectionCode_EVENT_REJECTION_CODE_VALUE_INVALID,
		},
		{
			name: "non finite double",
			fields: object(&opensplunkv1.TypedObjectField{
				Name:  "bad",
				Value: &opensplunkv1.TypedValue{Kind: &opensplunkv1.TypedValue_DoubleValue{DoubleValue: math.Inf(1)}},
			}),
			code: opensplunkv1.EventRejectionCode_EVENT_REJECTION_CODE_VALUE_INVALID,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			limits := tt.limits
			if limits == (Limits{}) {
				limits = DefaultLimits()
			}
			v := newTestValidator(t, limits)
			event := validTestEvent("event-1", "main")
			event.Fields = tt.fields
			_, rejection := v.ValidateAndNormalizeEvent(event, EventContext{ReceivedAt: validationTestNow})
			assertEventRejectionCode(t, rejection, tt.code)
		})
	}
}

func TestValidateAndNormalizeEventRedactsRecursivelyAndSanitizesRawJSON(t *testing.T) {
	v := newTestValidator(t, DefaultLimits())
	event := validTestEvent("event-redact", "main")
	event.Raw = []byte(`{"message":"safe","authorization":"Bearer raw-secret","nested":{"password":"raw-password"},"items":[{"api_key":"raw-key"}],"note":"token=raw-embedded"}`)
	event.Fields = object(
		stringField("authorization", "Bearer typed-secret"),
		objectField("nested", object(stringField("passwordHash", "typed-password"))),
		stringField("note", "token=typed-embedded"),
		stringField("http.request.header.Authorization", "Bearer typed-header"),
		&opensplunkv1.TypedObjectField{
			Name: "items",
			Value: &opensplunkv1.TypedValue{Kind: &opensplunkv1.TypedValue_ListValue{ListValue: &opensplunkv1.TypedValueList{Values: []*opensplunkv1.TypedValue{
				{Kind: &opensplunkv1.TypedValue_ObjectValue{ObjectValue: object(stringField("session-token", "typed-session"))}},
			}}}},
		},
	)

	got, rejection := v.ValidateAndNormalizeEvent(event, EventContext{ReceivedAt: validationTestNow})
	if rejection != nil {
		t.Fatalf("ValidateAndNormalizeEvent() rejection = %v", rejection)
	}

	encoded, err := proto.Marshal(got.Event)
	if err != nil {
		t.Fatal(err)
	}
	for _, secret := range [][]byte{
		[]byte("raw-secret"), []byte("raw-password"), []byte("raw-key"),
		[]byte("raw-embedded"), []byte("typed-secret"), []byte("typed-password"),
		[]byte("typed-embedded"), []byte("typed-header"), []byte("typed-session"),
	} {
		if bytes.Contains(encoded, secret) {
			t.Fatalf("normalized event still contains secret %q", secret)
		}
	}
	if got.Event.Fields.Fields[0].Value.GetStringValue() != DefaultRedactionReplacement {
		t.Fatalf("top-level redaction = %q", got.Event.Fields.Fields[0].Value.GetStringValue())
	}
	if !bytes.Contains(got.Event.Raw, []byte(DefaultRedactionReplacement)) {
		t.Fatalf("sanitized raw = %s", got.Event.Raw)
	}
	if !bytes.Contains(event.Raw, []byte("raw-secret")) {
		t.Fatal("redaction mutated the caller's raw bytes")
	}
}

func TestValidateAndNormalizeEventSanitizesNonJSONRawKeyValues(t *testing.T) {
	v := newTestValidator(t, DefaultLimits())
	event := validTestEvent("event-text", "main")
	event.Raw = []byte(`request failed authorization=Bearer bearer-secret password="plain-secret" safe=value`)

	got, rejection := v.ValidateAndNormalizeEvent(event, EventContext{ReceivedAt: validationTestNow})
	if rejection != nil {
		t.Fatalf("ValidateAndNormalizeEvent() rejection = %v", rejection)
	}
	if bytes.Contains(got.Event.Raw, []byte("bearer-secret")) || bytes.Contains(got.Event.Raw, []byte("plain-secret")) {
		t.Fatalf("sanitized raw still contains a secret: %s", got.Event.Raw)
	}
	if !bytes.Contains(got.Event.Raw, []byte(`authorization="[REDACTED]"`)) ||
		!bytes.Contains(got.Event.Raw, []byte(`password="[REDACTED]"`)) {
		t.Fatalf("sanitized raw = %s", got.Event.Raw)
	}
}

func TestValidateAndNormalizeEventRechecksSizeAfterRedaction(t *testing.T) {
	event := validTestEvent("event-expansion", "main")
	event.Fields = object(stringField("token", "x"))
	limits := DefaultLimits()
	limits.MaxEventBytes = uint64(proto.Size(event) + 32)
	v, err := NewValidator(limits, RedactionPolicy{Replacement: string(bytes.Repeat([]byte("r"), 256))})
	if err != nil {
		t.Fatal(err)
	}

	_, rejection := v.ValidateAndNormalizeEvent(event, EventContext{ReceivedAt: validationTestNow})
	assertEventRejectionCode(t, rejection, opensplunkv1.EventRejectionCode_EVENT_REJECTION_CODE_EVENT_TOO_LARGE)
}

func TestValidateAndNormalizeEventEnforcesTimeAndSizeBounds(t *testing.T) {
	limits := DefaultLimits()
	limits.MaxEventAge = 24 * time.Hour
	limits.MaxFutureSkew = time.Minute
	limits.MaxEventBytes = 512
	v := newTestValidator(t, limits)

	tests := []struct {
		name string
		edit func(*opensplunkv1.LogEvent)
		code opensplunkv1.EventRejectionCode
	}{
		{
			name: "invalid event ID",
			edit: func(e *opensplunkv1.LogEvent) { e.EventId = "event id with spaces" },
			code: opensplunkv1.EventRejectionCode_EVENT_REJECTION_CODE_INVALID_EVENT_ID,
		},
		{
			name: "event timestamp too old",
			edit: func(e *opensplunkv1.LogEvent) { e.EventTime = timestamppb.New(validationTestNow.Add(-25 * time.Hour)) },
			code: opensplunkv1.EventRejectionCode_EVENT_REJECTION_CODE_INVALID_TIMESTAMP,
		},
		{
			name: "collection timestamp in future",
			edit: func(e *opensplunkv1.LogEvent) {
				e.CollectedAt = timestamppb.New(validationTestNow.Add(2 * time.Minute))
			},
			code: opensplunkv1.EventRejectionCode_EVENT_REJECTION_CODE_INVALID_TIMESTAMP,
		},
		{
			name: "invalid protobuf timestamp",
			edit: func(e *opensplunkv1.LogEvent) {
				e.EventTime = &timestamppb.Timestamp{Seconds: math.MaxInt64}
			},
			code: opensplunkv1.EventRejectionCode_EVENT_REJECTION_CODE_INVALID_TIMESTAMP,
		},
		{
			name: "event too large",
			edit: func(e *opensplunkv1.LogEvent) { e.Raw = bytes.Repeat([]byte("x"), 1024) },
			code: opensplunkv1.EventRejectionCode_EVENT_REJECTION_CODE_EVENT_TOO_LARGE,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			event := validTestEvent("event-1", "main")
			tt.edit(event)
			_, rejection := v.ValidateAndNormalizeEvent(event, EventContext{ReceivedAt: validationTestNow})
			assertEventRejectionCode(t, rejection, tt.code)
		})
	}
}

func TestNewValidatorRejectsLimitsAboveHardCeilings(t *testing.T) {
	tests := []struct {
		name string
		edit func(*Limits)
	}{
		{name: "batch events", edit: func(limits *Limits) { limits.MaxBatchEvents = HardMaxBatchEvents + 1 }},
		{name: "batch bytes", edit: func(limits *Limits) { limits.MaxBatchBytes = HardMaxBatchBytes + 1 }},
		{name: "event bytes", edit: func(limits *Limits) { limits.MaxEventBytes = HardMaxEventBytes + 1 }},
		{name: "fields", edit: func(limits *Limits) { limits.MaxFields = HardMaxFields + 1 }},
		{name: "nesting depth", edit: func(limits *Limits) { limits.MaxNestingDepth = HardMaxNestingDepth + 1 }},
		{name: "field name bytes", edit: func(limits *Limits) { limits.MaxFieldNameBytes = HardMaxFieldNameBytes + 1 }},
		{name: "ID bytes", edit: func(limits *Limits) { limits.MaxIDBytes = HardMaxIDBytes + 1 }},
		{name: "event age", edit: func(limits *Limits) { limits.MaxEventAge = HardMaxEventAge + time.Nanosecond }},
		{name: "future skew", edit: func(limits *Limits) { limits.MaxFutureSkew = HardMaxFutureSkew + time.Nanosecond }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			limits := DefaultLimits()
			test.edit(&limits)
			if _, err := NewValidator(limits, RedactionPolicy{}); err == nil {
				t.Fatal("NewValidator() error = nil, want hard-limit rejection")
			}
		})
	}
}

func TestNewServiceRejectsLimitsAboveHardCeiling(t *testing.T) {
	config := testServiceConfig()
	config.Limits.MaxBatchEvents = HardMaxBatchEvents + 1
	if _, err := NewService(config, staticTestAuthorizer(), acceptingStore()); err == nil {
		t.Fatal("NewService() error = nil, want hard-limit rejection")
	}
	config = testServiceConfig()
	config.MaxInFlightBatches = HardMaxInFlightBatches + 1
	if _, err := NewService(config, staticTestAuthorizer(), acceptingStore()); err == nil {
		t.Fatal("NewService() accepted an unsafe in-flight batch limit")
	}
	config = testServiceConfig()
	config.MaxStreamsPerSubject = HardMaxStreamsPerSubject + 1
	if _, err := NewService(config, staticTestAuthorizer(), acceptingStore()); err == nil {
		t.Fatal("NewService() accepted an unsafe per-subject stream limit")
	}
}

func TestEventIDDigestUsesLengthPrefixedEventIDs(t *testing.T) {
	events := []*opensplunkv1.LogEvent{{EventId: "a"}, {EventId: "bc"}}
	h := sha256.New()
	var size [4]byte
	binary.BigEndian.PutUint32(size[:], 1)
	h.Write(size[:])
	h.Write([]byte("a"))
	binary.BigEndian.PutUint32(size[:], 2)
	h.Write(size[:])
	h.Write([]byte("bc"))

	if got, want := EventIDDigest(events), h.Sum(nil); !bytes.Equal(got, want) {
		t.Fatalf("EventIDDigest() = %x, want %x", got, want)
	}
}

func newTestValidator(t *testing.T, limits Limits) *Validator {
	t.Helper()
	v, err := NewValidator(limits, RedactionPolicy{})
	if err != nil {
		t.Fatal(err)
	}
	return v
}

func validTestEvent(id, index string) *opensplunkv1.LogEvent {
	message := "request completed"
	return &opensplunkv1.LogEvent{
		EventId:         id,
		IndexName:       index,
		EventTime:       timestamppb.New(validationTestNow.Add(-time.Minute)),
		CollectedAt:     timestamppb.New(validationTestNow.Add(-30 * time.Second)),
		EventTimeSource: opensplunkv1.EventTimeSource_EVENT_TIME_SOURCE_PARSED,
		Host:            "host-a",
		Source:          "/var/log/app.log",
		Sourcetype:      "json",
		Severity:        opensplunkv1.LogSeverity_LOG_SEVERITY_INFO,
		Message:         &message,
		Raw:             []byte(`{"message":"request completed","status":200}`),
		RawEncoding:     opensplunkv1.RawEncoding_RAW_ENCODING_UTF8,
		Fields:          object(stringField("status", "200")),
	}
}

func object(fields ...*opensplunkv1.TypedObjectField) *opensplunkv1.TypedObject {
	return &opensplunkv1.TypedObject{Fields: fields}
}

func stringField(name, value string) *opensplunkv1.TypedObjectField {
	return &opensplunkv1.TypedObjectField{
		Name: name,
		Value: &opensplunkv1.TypedValue{
			Kind: &opensplunkv1.TypedValue_StringValue{StringValue: value},
		},
	}
}

func objectField(name string, value *opensplunkv1.TypedObject) *opensplunkv1.TypedObjectField {
	return &opensplunkv1.TypedObjectField{
		Name: name,
		Value: &opensplunkv1.TypedValue{
			Kind: &opensplunkv1.TypedValue_ObjectValue{ObjectValue: value},
		},
	}
}

func assertEventRejectionCode(t *testing.T, rejection *EventError, want opensplunkv1.EventRejectionCode) {
	t.Helper()
	if rejection == nil {
		t.Fatalf("rejection = nil, want %v", want)
	}
	if rejection.Code != want {
		t.Fatalf("rejection code = %v, want %v (rejection: %v)", rejection.Code, want, rejection)
	}
}
