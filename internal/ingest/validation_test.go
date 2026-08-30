package ingest

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"
	"slices"
	"strings"
	"testing"
	"time"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/eventfields"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var validationTestNow = time.Date(2026, 7, 21, 18, 0, 0, 0, time.UTC)

// redactionTestFields opts the redaction-focused validator tests into every
// spelling they exercise. Production validators have no implicit field list.
var redactionTestFields = []string{
	"authorization",
	"proxy_authorization",
	"cookie",
	"set_cookie",
	"password",
	"passwd",
	"secret",
	"token",
	"access_token",
	"refresh_token",
	"session_token",
	"auth_token",
	"api_key",
	"apikey",
	"client_secret",
	"private_key",
	"passwordHash",
	"HTTPAuthorization",
	"JWTToken",
	"X-API-Key",
	"http.request.header.api_key",
	"apiKeyValue",
	"tls.private_key",
	"db_password",
	"request_auth_token_value",
	"http_request_header_authorization",
	"dbpassword",
	"passwordhash",
	"authtoken",
	"accesstoken",
	"myapikey",
	"HTTPAPIKey",
	"httpauthorization",
	"mycookie",
	"myprivatekey",
	"tlsprivate_key",
	`password\"safe`,
	strings.Repeat("a", 240) + "_password",
}

func TestValidateAndNormalizeEventDoesNotMutateInput(t *testing.T) {
	v := newTestValidator(t, DefaultLimits())
	event := validTestEvent("event-1", "main")
	want := proto.Clone(event).(*opensplunk.LogEvent)

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

func TestValidateAndNormalizeEventDoesNotRedactWithoutPolicy(t *testing.T) {
	t.Parallel()

	v, err := NewValidator(DefaultLimits(), RedactionPolicy{})
	if err != nil {
		t.Fatal(err)
	}
	event := validTestEvent("event-no-redaction", "main")
	event.Raw = []byte(`{"token":"visible-token","token_id":42,"caller":"session_token_persistence.go:119"}`)
	event.Fields = object(
		stringField("token", "visible-token"),
		stringField("token_type", "authentication"),
	)

	got, rejection := v.ValidateAndNormalizeEvent(event, EventContext{ReceivedAt: validationTestNow})
	if rejection != nil {
		t.Fatalf("ValidateAndNormalizeEvent() rejection = %v", rejection)
	}
	if !bytes.Equal(got.Event.GetRaw(), event.GetRaw()) ||
		got.Event.GetFields().GetFields()[0].GetValue().GetStringValue() != "visible-token" ||
		got.Event.GetFields().GetFields()[1].GetValue().GetStringValue() != "authentication" {
		t.Fatalf("unconfigured event was redacted: raw=%q fields=%+v", got.Event.GetRaw(), got.Event.GetFields())
	}
}

func TestValidateAndNormalizeEventUsesControlPlaneIndexNameBoundary(t *testing.T) {
	t.Parallel()
	validator := newTestValidator(t, DefaultLimits())
	maximumName := strings.Repeat("a", 255)
	event := validTestEvent("event-maximum-index", maximumName)
	if _, rejection := validator.ValidateAndNormalizeEvent(
		event,
		EventContext{ReceivedAt: validationTestNow},
	); rejection != nil {
		t.Fatalf("255-byte canonical index rejected: %v", rejection)
	}

	for name, indexName := range map[string]string{
		"too long":      strings.Repeat("a", 256),
		"not canonical": "Main",
		"protocol ID":   "main:archive",
		"reserved":      "my_kvstore_data",
	} {
		t.Run(name, func(t *testing.T) {
			event := validTestEvent("event-invalid-index", indexName)
			_, rejection := validator.ValidateAndNormalizeEvent(
				event,
				EventContext{ReceivedAt: validationTestNow},
			)
			assertEventRejectionCode(
				t,
				rejection,
				opensplunk.EventRejectionCode_EVENT_REJECTION_CODE_INVALID_INDEX,
			)
		})
	}
}

func TestValidateAndNormalizeEventRejectsCanonicalFieldInjection(t *testing.T) {
	t.Parallel()

	v := newTestValidator(t, DefaultLimits())
	names := append(eventfields.ReservedDynamicRootNames(), "__Os_Private")
	for _, canonical := range names {
		for _, name := range []string{canonical, strings.ToUpper(canonical)} {
			t.Run(name, func(t *testing.T) {
				event := validTestEvent("event-1", "main")
				event.Fields.Fields = append(event.Fields.Fields, stringField(name, "forged"))

				_, rejection := v.ValidateAndNormalizeEvent(event, EventContext{ReceivedAt: validationTestNow})
				assertEventRejectionCode(t, rejection, opensplunk.EventRejectionCode_EVENT_REJECTION_CODE_FIELD_NAME_INVALID)
				if len(rejection.Violations) == 0 || rejection.Violations[0].FieldPath != "fields."+name ||
					rejection.Violations[0].Code != "canonical_field_reserved" {
					t.Fatalf("violations = %#v", rejection.Violations)
				}
			})
		}
	}
}

func TestValidateAndNormalizeEventKeepsCollectorOnlyAliasesDynamic(t *testing.T) {
	t.Parallel()

	v := newTestValidator(t, DefaultLimits())
	event := validTestEvent("event-1", "main")
	event.Fields.Fields = append(event.Fields.Fields,
		stringField("timestamp", "application timestamp"),
		stringField("msg", "application message"),
		stringField("severity_text", "application severity"),
	)

	stored, rejection := v.ValidateAndNormalizeEvent(event, EventContext{ReceivedAt: validationTestNow})
	if rejection != nil {
		t.Fatalf("ValidateAndNormalizeEvent() rejection = %v", rejection)
	}
	if got := len(stored.Event.GetFields().GetFields()); got != len(event.GetFields().GetFields()) {
		t.Fatalf("stored dynamic field count = %d, want %d", got, len(event.GetFields().GetFields()))
	}
}

func TestValidateAndNormalizeEventValidatesNextSourceLine(t *testing.T) {
	t.Parallel()

	validator := newTestValidator(t, DefaultLimits())
	for _, test := range []struct {
		name string
		line *uint64
		next uint64
	}{
		{name: "missing current line", next: 2},
		{name: "zero current line", line: proto.Uint64(0), next: 2},
		{name: "regressing", line: proto.Uint64(3), next: 2},
		{name: "equal", line: proto.Uint64(3), next: 3},
		{name: "exhausted", line: proto.Uint64(math.MaxUint64 - 1), next: math.MaxUint64},
	} {
		t.Run(test.name, func(t *testing.T) {
			event := validTestEvent("event-"+strings.ReplaceAll(test.name, " ", "-"), "main")
			event.Origin = &opensplunk.EventOrigin{
				LineNumber:     test.line,
				NextLineNumber: new(test.next),
			}
			_, rejection := validator.ValidateAndNormalizeEvent(event, EventContext{ReceivedAt: validationTestNow})
			assertEventRejectionCode(
				t,
				rejection,
				opensplunk.EventRejectionCode_EVENT_REJECTION_CODE_VALUE_INVALID,
			)
			if len(rejection.Violations) != 1 ||
				rejection.Violations[0].GetFieldPath() != "origin.next_line_number" {
				t.Fatalf("next-line rejection = %+v", rejection)
			}
		})
	}

	event := validTestEvent("event-valid-next-line", "main")
	event.Origin = &opensplunk.EventOrigin{
		LineNumber:     proto.Uint64(3),
		NextLineNumber: proto.Uint64(5),
	}
	if _, rejection := validator.ValidateAndNormalizeEvent(
		event,
		EventContext{ReceivedAt: validationTestNow},
	); rejection != nil {
		t.Fatalf("valid multiline next-line cursor rejection = %+v", rejection)
	}
}

func TestValidateAndNormalizeEventValidatesCheckpointRewriteGuard(t *testing.T) {
	t.Parallel()
	validator := newTestValidator(t, DefaultLimits())
	digest := strings.Repeat("ab", 32)
	for _, test := range []struct {
		name   string
		origin *opensplunk.EventOrigin
	}{
		{name: "missing length", origin: &opensplunk.EventOrigin{
			EndOffset: proto.Uint64(100), CheckpointGuardFingerprint: new(digest),
		}},
		{name: "missing fingerprint", origin: &opensplunk.EventOrigin{
			EndOffset: proto.Uint64(100), CheckpointGuardLength: proto.Uint32(32),
		}},
		{name: "explicit empty guard", origin: &opensplunk.EventOrigin{
			EndOffset: proto.Uint64(100), CheckpointGuardFingerprint: new(""), CheckpointGuardLength: proto.Uint32(0),
		}},
		{name: "missing end offset", origin: &opensplunk.EventOrigin{
			CheckpointGuardFingerprint: new(digest), CheckpointGuardLength: proto.Uint32(32),
		}},
		{name: "guard exceeds end", origin: &opensplunk.EventOrigin{
			EndOffset: proto.Uint64(31), CheckpointGuardFingerprint: new(digest), CheckpointGuardLength: proto.Uint32(32),
		}},
		{name: "guard exceeds absolute limit", origin: &opensplunk.EventOrigin{
			EndOffset: proto.Uint64(2 << 20), CheckpointGuardFingerprint: new(digest), CheckpointGuardLength: proto.Uint32((1 << 20) + 1),
		}},
		{name: "noncanonical digest", origin: &opensplunk.EventOrigin{
			EndOffset: proto.Uint64(100), CheckpointGuardFingerprint: new(strings.ToUpper(digest)), CheckpointGuardLength: proto.Uint32(32),
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			event := validTestEvent("event-"+strings.ReplaceAll(test.name, " ", "-"), "main")
			event.Origin = test.origin
			_, rejection := validator.ValidateAndNormalizeEvent(event, EventContext{ReceivedAt: validationTestNow})
			assertEventRejectionCode(t, rejection, opensplunk.EventRejectionCode_EVENT_REJECTION_CODE_VALUE_INVALID)
			if len(rejection.Violations) != 1 ||
				rejection.Violations[0].GetFieldPath() != "origin.checkpoint_guard_fingerprint" {
				t.Fatalf("guard rejection = %+v", rejection)
			}
		})
	}

	event := validTestEvent("event-valid-checkpoint-guard", "main")
	event.Origin = &opensplunk.EventOrigin{
		EndOffset: proto.Uint64(100), CheckpointGuardFingerprint: new(digest), CheckpointGuardLength: proto.Uint32(32),
	}
	if _, rejection := validator.ValidateAndNormalizeEvent(event, EventContext{ReceivedAt: validationTestNow}); rejection != nil {
		t.Fatalf("valid checkpoint guard rejection = %+v", rejection)
	}
}

func TestTypedObjectValidation(t *testing.T) {
	tests := []struct {
		name   string
		limits Limits
		fields *opensplunk.TypedObject
		code   opensplunk.EventRejectionCode
	}{
		{
			name: "duplicate names in nested object",
			fields: object(
				objectField("nested", object(stringField("same", "one"), stringField("same", "two"))),
			),
			code: opensplunk.EventRejectionCode_EVENT_REJECTION_CODE_FIELD_NAME_INVALID,
		},
		{
			name: "too many recursively counted fields",
			limits: func() Limits {
				limits := DefaultLimits()
				limits.MaxFields = 2
				return limits
			}(),
			fields: object(stringField("one", "1"), objectField("nested", object(stringField("two", "2")))),
			code:   opensplunk.EventRejectionCode_EVENT_REJECTION_CODE_TOO_MANY_FIELDS,
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
			code: opensplunk.EventRejectionCode_EVENT_REJECTION_CODE_NESTING_TOO_DEEP,
		},
		{
			name:   "unset value kind",
			fields: object(&opensplunk.TypedObjectField{Name: "bad", Value: &opensplunk.TypedValue{}}),
			code:   opensplunk.EventRejectionCode_EVENT_REJECTION_CODE_VALUE_INVALID,
		},
		{
			name: "non finite double",
			fields: object(&opensplunk.TypedObjectField{
				Name:  "bad",
				Value: &opensplunk.TypedValue{Kind: &opensplunk.TypedValue_DoubleValue{DoubleValue: math.Inf(1)}},
			}),
			code: opensplunk.EventRejectionCode_EVENT_REJECTION_CODE_VALUE_INVALID,
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

func TestValidateAndNormalizeEventBoundsDurableFlattenedFieldMetadata(t *testing.T) {
	t.Parallel()

	longParent := strings.Repeat(".", int(HardMaxFieldNameBytes))
	parentNames := make([]string, 15)
	for index := range parentNames {
		parentNames[index] = longParent
	}
	event := validTestEvent("event-field-metadata", "main")
	event.Fields = amplifiedFieldMetadataObject(parentNames, int(HardMaxFields)-len(parentNames))
	if size := uint64(proto.Size(event)); size > HardMaxEventBytes {
		t.Fatalf("amplified event protobuf size = %d, hard limit = %d", size, HardMaxEventBytes)
	}

	validator := newTestValidator(t, DefaultLimits())
	_, rejection := validator.ValidateAndNormalizeEvent(event, EventContext{ReceivedAt: validationTestNow})
	assertEventRejectionCode(
		t,
		rejection,
		opensplunk.EventRejectionCode_EVENT_REJECTION_CODE_EVENT_TOO_LARGE,
	)
	if len(rejection.Violations) != 1 ||
		rejection.Violations[0].Code != "field_metadata_too_large" {
		t.Fatalf("violations = %#v", rejection.Violations)
	}
}

func TestValidateAndNormalizeEventBudgetsFieldMetadataAfterRedaction(t *testing.T) {
	t.Parallel()

	longParent := strings.Repeat(".", int(HardMaxFieldNameBytes))
	parentNames := make([]string, 15)
	parentNames[0] = "password"
	for index := 1; index < len(parentNames); index++ {
		parentNames[index] = longParent
	}
	event := validTestEvent("event-redacted-field-metadata", "main")
	event.Fields = amplifiedFieldMetadataObject(parentNames, int(HardMaxFields)-len(parentNames))
	if size := uint64(proto.Size(event)); size > HardMaxEventBytes {
		t.Fatalf("amplified event protobuf size = %d, hard limit = %d", size, HardMaxEventBytes)
	}

	validator := newTestValidator(t, DefaultLimits())
	stored, rejection := validator.ValidateAndNormalizeEvent(
		event,
		EventContext{ReceivedAt: validationTestNow},
	)
	if rejection != nil {
		t.Fatalf("ValidateAndNormalizeEvent() rejection = %v", rejection)
	}
	fields := stored.Event.GetFields().GetFields()
	if len(fields) != 1 || fields[0].GetName() != "password" ||
		fields[0].GetValue().GetStringValue() != DefaultRedactionReplacement {
		t.Fatalf("durable redacted fields = %#v", fields)
	}
}

func TestDurationFitsResultRangeBoundaries(t *testing.T) {
	t.Parallel()

	for _, value := range []*durationpb.Duration{
		durationpb.New(time.Duration(math.MaxInt64)),
		durationpb.New(time.Duration(math.MinInt64)),
	} {
		if !DurationFitsResultRange(value) {
			t.Fatalf("result-range boundary rejected: %#v", value)
		}
	}
	for _, value := range []*durationpb.Duration{
		{Seconds: 9_223_372_036, Nanos: 854_775_808},
		{Seconds: -9_223_372_036, Nanos: -854_775_809},
		{Seconds: 9_223_372_037},
	} {
		if DurationFitsResultRange(value) {
			t.Fatalf("out-of-range duration accepted: %#v", value)
		}
	}

	v := newTestValidator(t, DefaultLimits())
	event := validTestEvent("event-duration", "main")
	event.Fields = object(&opensplunk.TypedObjectField{
		Name: "too_long",
		Value: &opensplunk.TypedValue{Kind: &opensplunk.TypedValue_DurationValue{
			DurationValue: &durationpb.Duration{Seconds: 9_223_372_037},
		}},
	})
	_, rejection := v.ValidateAndNormalizeEvent(event, EventContext{ReceivedAt: validationTestNow})
	assertEventRejectionCode(t, rejection, opensplunk.EventRejectionCode_EVENT_REJECTION_CODE_VALUE_INVALID)
}

func TestValidateAndNormalizeEventRedactsRecursivelyAndSanitizesRawJSON(t *testing.T) {
	v := newTestValidator(t, DefaultLimits())
	event := validTestEvent("event-redact", "main")
	event.Raw = []byte(`{"message":"safe","authorization":"Bearer raw-secret","nested":{"password":"raw-password"},"items":[{"api_key":"raw-key"}],"headers":{"X-API-Key":"raw-x-api-key"},"tls.private_key":"raw-private-key","note":"token=raw-embedded"}`)
	event.Fields = object(
		stringField("authorization", "Bearer typed-secret"),
		objectField("nested", object(stringField("passwordHash", "typed-password"))),
		stringField("X-API-Key", "typed-x-api-key"),
		stringField("tls.private_key", "typed-private-key"),
		stringField("note", "token=typed-embedded"),
		stringField("http.request.header.Authorization", "Bearer typed-header"),
		&opensplunk.TypedObjectField{
			Name: "items",
			Value: &opensplunk.TypedValue{Kind: &opensplunk.TypedValue_ListValue{ListValue: &opensplunk.TypedValueList{Values: []*opensplunk.TypedValue{
				{Kind: &opensplunk.TypedValue_ObjectValue{ObjectValue: object(stringField("session-token", "typed-session"))}},
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
		[]byte("raw-x-api-key"), []byte("raw-private-key"), []byte("raw-embedded"),
		[]byte("typed-secret"), []byte("typed-password"), []byte("typed-x-api-key"),
		[]byte("typed-private-key"), []byte("typed-embedded"), []byte("typed-header"), []byte("typed-session"),
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

func TestRedactEventInPlaceUsesAdditionalPolicyWithoutCloning(t *testing.T) {
	t.Parallel()

	validator, err := NewValidator(DefaultLimits(), RedactionPolicy{
		SensitiveFields: []string{"customer_credential"},
		Replacement:     "***",
	})
	if err != nil {
		t.Fatal(err)
	}
	event := validTestEvent("event-owned-redaction", "main")
	message := "customer_credential=message-secret safe=message"
	event.Message = &message
	event.Raw = []byte(`{"customer_credential":"raw-secret","safe":"raw"}`)
	event.Fields = object(
		stringField("customerCredential", "typed-secret"),
		stringField("safe", "typed"),
	)

	got := validator.RedactEventInPlace(event)
	if got != event {
		t.Fatal("RedactEventInPlace returned a clone for an exclusively owned event")
	}
	wire, err := proto.MarshalOptions{Deterministic: true}.Marshal(got)
	if err != nil {
		t.Fatal(err)
	}
	for _, secret := range []string{"message-secret", "raw-secret", "typed-secret"} {
		if bytes.Contains(wire, []byte(secret)) {
			t.Fatalf("in-place redaction retained %q", secret)
		}
	}
	if got.GetFields().GetFields()[0].GetValue().GetStringValue() != "***" ||
		!strings.Contains(got.GetMessage(), "***") ||
		!bytes.Contains(got.GetRaw(), []byte(`"customer_credential":"***"`)) ||
		!validator.IsSensitiveField("customerCredential") ||
		validator.IsSensitiveField("ordinary_field") {
		t.Fatalf("in-place redacted event/policy = event:%+v raw:%q", got, got.GetRaw())
	}
}

func TestRedactTopLevelAliasesInPlacePreservesNestedLookalikes(t *testing.T) {
	t.Parallel()

	event := validTestEvent("event-top-level-alias-redaction", "main")
	message := "userID=message-secret safe=message"
	event.Message = &message
	event.Raw = []byte(
		`{"userID":"top-secret","nested":{"userID":"nested-safe"},"safe":"raw-safe"}`,
	)
	event.Fields = object(
		stringField("userID", "top-secret"),
		objectField("nested", object(
			stringField("userID", "nested-safe"),
		)),
		stringField("safe", "typed-safe"),
	)

	got := RedactTopLevelAliasesInPlace(event, []TopLevelAliasRedaction{{
		Field: "userID", Replacement: "MASKED",
	}})
	if got != event {
		t.Fatal("RedactTopLevelAliasesInPlace returned a clone")
	}
	if got.GetFields().GetFields()[0].GetValue().GetStringValue() != "MASKED" ||
		got.GetFields().GetFields()[1].GetValue().GetObjectValue().GetFields()[0].GetValue().GetStringValue() != "nested-safe" ||
		got.GetFields().GetFields()[2].GetValue().GetStringValue() != "typed-safe" ||
		strings.Contains(got.GetMessage(), "message-secret") ||
		!strings.Contains(got.GetMessage(), "MASKED") {
		t.Fatalf("top-level alias structured/message redaction = %+v", got)
	}
	var raw map[string]any
	if err := json.Unmarshal(got.GetRaw(), &raw); err != nil {
		t.Fatal(err)
	}
	nested, ok := raw["nested"].(map[string]any)
	if raw["userID"] != "MASKED" || raw["safe"] != "raw-safe" ||
		!ok || nested["userID"] != "nested-safe" {
		t.Fatalf("top-level alias raw redaction = %s", got.GetRaw())
	}
}

func TestRedactTopLevelAliasesInPlaceKeepsConstantProvenanceStructuredOnly(t *testing.T) {
	t.Parallel()

	event := validTestEvent("event-constant-alias-redaction", "main")
	message := "userID=ordinary-message-data"
	event.Message = &message
	event.Raw = []byte(`{"userID":"ordinary-raw-data"}`)
	event.Fields = object(stringField("userID", "constant-secret"))

	got := RedactTopLevelAliasesInPlace(event, []TopLevelAliasRedaction{{
		Field: "userID", Replacement: "MASKED", StructuredOnly: true,
	}})
	if got.GetFields().GetFields()[0].GetValue().GetStringValue() != "MASKED" ||
		got.GetMessage() != message ||
		string(got.GetRaw()) != `{"userID":"ordinary-raw-data"}` {
		t.Fatalf("structured-only constant alias redaction = %+v", got)
	}
}

func TestRedactTopLevelAliasesInPlaceCanonicalizesShadowedSecret(t *testing.T) {
	t.Parallel()

	event := validTestEvent("event-duplicate-alias-redaction", "main")
	event.Raw = []byte(`{"userID":"shadow-secret","userID":"MASKED","safe":"kept"}`)

	got := RedactTopLevelAliasesInPlace(event, []TopLevelAliasRedaction{{
		Field: "userID", Replacement: "MASKED",
	}})
	if bytes.Contains(got.GetRaw(), []byte("shadow-secret")) ||
		bytes.Count(got.GetRaw(), []byte(`"userID"`)) != 1 ||
		!json.Valid(got.GetRaw()) {
		t.Fatalf("duplicate alias raw was not safely canonicalized: %s", got.GetRaw())
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

func TestValidateAndNormalizeEventRawRedactionCoversConfiguredFieldVariants(t *testing.T) {
	t.Parallel()

	v := newTestValidator(t, DefaultLimits())
	for _, field := range redactionTestFields[:16] {
		for _, spelling := range []string{
			field,
			strings.ReplaceAll(field, "_", "-"),
			strings.ReplaceAll(field, "_", "."),
			strings.ReplaceAll(field, "_", " "),
			camelCaseSensitiveField(field),
		} {
			t.Run(field+"/"+spelling, func(t *testing.T) {
				t.Parallel()
				secret := "secret-for-" + strings.ReplaceAll(field, "_", "-")
				value := `"` + secret + ` value"`
				if field == "authorization" || field == "proxy_authorization" {
					value = "Bearer " + secret
				}
				event := validTestEvent("event-"+strings.ReplaceAll(field, "_", "-"), "main")
				event.Raw = []byte("prefix " + spelling + "=" + value + " safe=value")

				got, rejection := v.ValidateAndNormalizeEvent(event, EventContext{ReceivedAt: validationTestNow})
				if rejection != nil {
					t.Fatalf("ValidateAndNormalizeEvent() rejection = %v", rejection)
				}
				if bytes.Contains(got.Event.Raw, []byte(secret)) || !bytes.Contains(got.Event.Raw, []byte(DefaultRedactionReplacement)) {
					t.Fatalf("configured raw field %q was not redacted: %q", spelling, got.Event.Raw)
				}
			})
		}
	}
}

func TestValidateAndNormalizeEventRedactsWholeCookieAndPrivateKeyLines(t *testing.T) {
	t.Parallel()

	v := newTestValidator(t, DefaultLimits())
	event := validTestEvent("event-sensitive-lines", "main")
	event.Raw = []byte("Cookie: sid=cookie-secret; csrf=csrf-secret\n" +
		"Set-Cookie: session=set-cookie-secret; HttpOnly\n" +
		"private_key=\"-----BEGIN PRIVATE KEY-----\nprivate-key-material\n-----END PRIVATE KEY-----\"\n" +
		"safe=value")

	got, rejection := v.ValidateAndNormalizeEvent(event, EventContext{ReceivedAt: validationTestNow})
	if rejection != nil {
		t.Fatalf("ValidateAndNormalizeEvent() rejection = %v", rejection)
	}
	for _, secret := range []string{"cookie-secret", "csrf-secret", "set-cookie-secret", "private-key-material"} {
		if bytes.Contains(got.Event.Raw, []byte(secret)) {
			t.Fatalf("sensitive line still contains %q: %q", secret, got.Event.Raw)
		}
	}
	if replacementCount := bytes.Count(got.Event.Raw, []byte(DefaultRedactionReplacement)); replacementCount != 3 {
		t.Fatalf("redacted line count = %d, want 3: %q", replacementCount, got.Event.Raw)
	}
	if !bytes.Contains(got.Event.Raw, []byte("safe=value")) {
		t.Fatalf("redaction removed the following safe line: %q", got.Event.Raw)
	}
}

func TestValidateAndNormalizeEventRedactsAdditionalRawField(t *testing.T) {
	t.Parallel()

	v, err := NewValidator(DefaultLimits(), RedactionPolicy{SensitiveFields: []string{"custom_audit_secret"}})
	if err != nil {
		t.Fatal(err)
	}
	event := validTestEvent("event-custom-redaction", "main")
	event.Raw = []byte(`customAuditSecret="custom-secret" safe=value`)
	got, rejection := v.ValidateAndNormalizeEvent(event, EventContext{ReceivedAt: validationTestNow})
	if rejection != nil {
		t.Fatalf("ValidateAndNormalizeEvent() rejection = %v", rejection)
	}
	if bytes.Contains(got.Event.Raw, []byte("custom-secret")) || !bytes.Contains(got.Event.Raw, []byte(DefaultRedactionReplacement)) {
		t.Fatalf("additional raw field was not redacted: %q", got.Event.Raw)
	}
}

func TestValidateAndNormalizeEventRawRedactionMatchesStructuredNameSemantics(t *testing.T) {
	t.Parallel()

	v := newTestValidator(t, DefaultLimits())
	tests := []struct {
		name   string
		key    string
		value  string
		secret string
	}{
		{name: "camel case component", key: "passwordHash", value: "password-hash-secret"},
		{name: "acronym authorization", key: "HTTPAuthorization", value: "Digest response=acronym-auth-secret"},
		{name: "acronym token", key: "JWTToken", value: "acronym-token-secret"},
		{name: "prefixed API key", key: "X-API-Key", value: "prefixed-api-key-secret"},
		{name: "nested API key", key: "http.request.header.api_key", value: "nested-api-key-secret"},
		{name: "suffixed API key", key: "apiKeyValue", value: "suffixed-api-key-secret"},
		{name: "prefixed private key", key: "tls.private_key", value: "prefixed-private-key-secret"},
		{name: "snake case component", key: "db_password", value: "database-password-secret"},
		{name: "component before suffix", key: "request_auth_token_value", value: "auth-token-secret"},
		{name: "compound authorization", key: "http_request_header_authorization", value: "Digest username=user response=digest-secret"},
		{name: "fused password suffix", key: "dbpassword", value: "fused-password-suffix-secret"},
		{name: "fused password prefix", key: "passwordhash", value: "fused-password-prefix-secret"},
		{name: "fused auth token", key: "authtoken", value: "fused-auth-token-secret"},
		{name: "fused access token", key: "accesstoken", value: "fused-access-token-secret"},
		{name: "fused API key", key: "myapikey", value: "fused-api-key-secret"},
		{name: "acronym fused API key", key: "HTTPAPIKey", value: "acronym-fused-api-key-secret"},
		{name: "fused authorization", key: "httpauthorization", value: "Digest username=u, response=fused-auth-response-secret", secret: "fused-auth-response-secret"},
		{name: "fused cookie", key: "mycookie", value: "sid=a; csrf=fused-cookie-tail-secret", secret: "fused-cookie-tail-secret"},
		{name: "fused private key", key: "myprivatekey", value: "-----BEGIN PRIVATE KEY-----\nfused-private-key-secret\n-----END PRIVATE KEY-----", secret: "fused-private-key-secret"},
		{name: "prefixed private key pair", key: "tlsprivate_key", value: "-----BEGIN PRIVATE KEY-----\nprefixed-private-pair-secret\n-----END PRIVATE KEY-----", secret: "prefixed-private-pair-secret"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			secret := test.secret
			if secret == "" {
				secret = test.value
			}
			event := validTestEvent("event-"+strings.ReplaceAll(test.name, " ", "-"), "main")
			event.Raw = []byte(test.key + "=" + test.value + "\nsafe=value")
			event.Fields = object(stringField(test.key, test.value))
			message := test.key + "=" + test.value
			event.Message = &message

			got, rejection := v.ValidateAndNormalizeEvent(event, EventContext{ReceivedAt: validationTestNow})
			if rejection != nil {
				t.Fatalf("ValidateAndNormalizeEvent() rejection = %v", rejection)
			}
			if bytes.Contains(got.Event.Raw, []byte(secret)) || !bytes.Contains(got.Event.Raw, []byte(DefaultRedactionReplacement)) {
				t.Fatalf("compound raw field %q was not redacted: %q", test.key, got.Event.Raw)
			}
			encoded, err := proto.Marshal(got.Event)
			if err != nil {
				t.Fatal(err)
			}
			if bytes.Contains(encoded, []byte(secret)) {
				t.Fatalf("compound structured field %q was not redacted", test.key)
			}
			if !bytes.Contains(got.Event.Raw, []byte("safe=value")) {
				t.Fatalf("redaction removed the following safe line: %q", got.Event.Raw)
			}
		})
	}
}

func TestValidateAndNormalizeEventRawRedactionNormalizesConfiguredSeparators(t *testing.T) {
	t.Parallel()

	v, err := NewValidator(DefaultLimits(), RedactionPolicy{SensitiveFields: []string{"customer_credential"}})
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"customer/credential", "customer\tcredential", "customer@credential", "customer—credential"} {
		t.Run(key, func(t *testing.T) {
			t.Parallel()
			event := validTestEvent("event-separator", "main")
			event.Raw = []byte(key + `="configured-secret" safe=value`)
			got, rejection := v.ValidateAndNormalizeEvent(event, EventContext{ReceivedAt: validationTestNow})
			if rejection != nil {
				t.Fatalf("ValidateAndNormalizeEvent() rejection = %v", rejection)
			}
			if bytes.Contains(got.Event.Raw, []byte("configured-secret")) || !bytes.Contains(got.Event.Raw, []byte("safe=value")) {
				t.Fatalf("configured raw field %q was not safely redacted: %q", key, got.Event.Raw)
			}
		})
	}
}

func TestValidateAndNormalizeEventRedactsComplexAuthorizationAndFoldedCookies(t *testing.T) {
	t.Parallel()

	v := newTestValidator(t, DefaultLimits())
	event := validTestEvent("event-complex-headers", "main")
	event.Raw = []byte("Authorization: Digest username=user, realm=example, response=digest-response-secret\n" +
		"Proxy-Authorization=AWS4-HMAC-SHA256 Credential=aws-access-secret/date, SignedHeaders=host, Signature=aws-signature-secret\n" +
		"Authorization: Bearer bearer-secret password=password-secret\n" +
		"Authorization: Bearer folded-bearer-secret\r\n folded-bearer-tail\r\n" +
		"Cookie: sid=cookie-secret\r\n\tcsrf=folded-cookie-secret\r\n" +
		"safe=value")

	got, rejection := v.ValidateAndNormalizeEvent(event, EventContext{ReceivedAt: validationTestNow})
	if rejection != nil {
		t.Fatalf("ValidateAndNormalizeEvent() rejection = %v", rejection)
	}
	for _, secret := range []string{
		"digest-response-secret", "aws-access-secret", "aws-signature-secret",
		"bearer-secret", "password-secret", "folded-bearer-secret", "folded-bearer-tail",
		"cookie-secret", "folded-cookie-secret",
	} {
		if bytes.Contains(got.Event.Raw, []byte(secret)) {
			t.Fatalf("complex header still contains %q: %q", secret, got.Event.Raw)
		}
	}
	if !bytes.Contains(got.Event.Raw, []byte("safe=value")) {
		t.Fatalf("header redaction removed the following safe line: %q", got.Event.Raw)
	}
}

func TestValidateAndNormalizeEventPrivateKeyRedactionIsFailClosed(t *testing.T) {
	t.Parallel()

	v := newTestValidator(t, DefaultLimits())
	t.Run("terminated PEM preserves following data", func(t *testing.T) {
		event := validTestEvent("event-terminated-pem", "main")
		event.Raw = []byte("private_key='PEM follows: -----BEGIN RSA PRIVATE KEY-----\npem-body-secret\n-----END RSA PRIVATE KEY-----' safe=value")
		got, rejection := v.ValidateAndNormalizeEvent(event, EventContext{ReceivedAt: validationTestNow})
		if rejection != nil {
			t.Fatalf("ValidateAndNormalizeEvent() rejection = %v", rejection)
		}
		if bytes.Contains(got.Event.Raw, []byte("pem-body-secret")) || !bytes.Contains(got.Event.Raw, []byte("safe=value")) {
			t.Fatalf("terminated PEM redaction = %q", got.Event.Raw)
		}
	})
	t.Run("quoted PEM consumes trailing value text", func(t *testing.T) {
		event := validTestEvent("event-quoted-pem-trailer", "main")
		event.Raw = []byte("private_key='-----BEGIN PRIVATE KEY-----\npem-body-secret\n-----END PRIVATE KEY----- trailing-private-secret' safe=value")
		got, rejection := v.ValidateAndNormalizeEvent(event, EventContext{ReceivedAt: validationTestNow})
		if rejection != nil {
			t.Fatalf("ValidateAndNormalizeEvent() rejection = %v", rejection)
		}
		if bytes.Contains(got.Event.Raw, []byte("private-secret")) || !bytes.Contains(got.Event.Raw, []byte("safe=value")) {
			t.Fatalf("quoted PEM redaction = %q", got.Event.Raw)
		}
	})
	t.Run("unterminated PEM consumes event remainder", func(t *testing.T) {
		event := validTestEvent("event-unterminated-pem", "main")
		event.Raw = []byte("prefix private_key=\n\n-----BEGIN PRIVATE KEY-----\npem-body-secret\nunsafe-trailer")
		got, rejection := v.ValidateAndNormalizeEvent(event, EventContext{ReceivedAt: validationTestNow})
		if rejection != nil {
			t.Fatalf("ValidateAndNormalizeEvent() rejection = %v", rejection)
		}
		if bytes.Contains(got.Event.Raw, []byte("pem-body-secret")) || bytes.Contains(got.Event.Raw, []byte("unsafe-trailer")) {
			t.Fatalf("unterminated PEM redaction = %q", got.Event.Raw)
		}
	})
	t.Run("PEM after preamble and blank lines", func(t *testing.T) {
		event := validTestEvent("event-pem-preamble", "main")
		event.Raw = []byte("private_key=PEM follows:\n\n-----BEGIN PRIVATE KEY-----\npreamble-pem-secret\n-----END PRIVATE KEY-----\nsafe=value")
		got, rejection := v.ValidateAndNormalizeEvent(event, EventContext{ReceivedAt: validationTestNow})
		if rejection != nil {
			t.Fatalf("ValidateAndNormalizeEvent() rejection = %v", rejection)
		}
		if bytes.Contains(got.Event.Raw, []byte("preamble-pem-secret")) || !bytes.Contains(got.Event.Raw, []byte("safe=value")) {
			t.Fatalf("preamble PEM redaction = %q", got.Event.Raw)
		}
	})
}

func TestValidateAndNormalizeEventRawRedactionDoesNotTreatPriorProseAsKey(t *testing.T) {
	t.Parallel()

	v := newTestValidator(t, DefaultLimits())
	event := validTestEvent("event-safe-prose", "main")
	event.Raw = []byte("message=password reset complete status=ok")
	got, rejection := v.ValidateAndNormalizeEvent(event, EventContext{ReceivedAt: validationTestNow})
	if rejection != nil {
		t.Fatalf("ValidateAndNormalizeEvent() rejection = %v", rejection)
	}
	if !bytes.Equal(got.Event.Raw, event.Raw) {
		t.Fatalf("safe prose was over-redacted: got %q, want %q", got.Event.Raw, event.Raw)
	}
}

func TestValidateAndNormalizeEventRawRedactionHandlesEscapedQuotes(t *testing.T) {
	t.Parallel()

	v := newTestValidator(t, DefaultLimits())
	for _, raw := range []string{
		`password="head\"double-quote-tail" safe=value`,
		`password='head\'single-quote-tail' safe=value`,
	} {
		event := validTestEvent("event-escaped-quote", "main")
		event.Raw = []byte(raw)
		got, rejection := v.ValidateAndNormalizeEvent(event, EventContext{ReceivedAt: validationTestNow})
		if rejection != nil {
			t.Fatalf("ValidateAndNormalizeEvent() rejection = %v", rejection)
		}
		if bytes.Contains(got.Event.Raw, []byte("quote-tail")) || !bytes.Contains(got.Event.Raw, []byte("safe=value")) {
			t.Fatalf("escaped quoted value was not safely redacted: %q", got.Event.Raw)
		}
	}

	embedded := `password="head\"json-tail" safe=value`
	rawJSON, err := json.Marshal(map[string]string{"note": embedded, "visible": "yes"})
	if err != nil {
		t.Fatal(err)
	}
	event := validTestEvent("event-json-escaped-quote", "main")
	event.Raw = rawJSON
	got, rejection := v.ValidateAndNormalizeEvent(event, EventContext{ReceivedAt: validationTestNow})
	if rejection != nil {
		t.Fatalf("ValidateAndNormalizeEvent() rejection = %v", rejection)
	}
	if bytes.Contains(got.Event.Raw, []byte("json-tail")) || !json.Valid(got.Event.Raw) || !bytes.Contains(got.Event.Raw, []byte(`"visible":"yes"`)) {
		t.Fatalf("JSON embedded escaped value was not safely redacted: %q", got.Event.Raw)
	}
}

func TestValidateAndNormalizeEventRedactsBinaryRawBytes(t *testing.T) {
	t.Parallel()

	v := newTestValidator(t, DefaultLimits())
	event := validTestEvent("event-binary-secret", "main")
	event.RawEncoding = opensplunk.RawEncoding_RAW_ENCODING_BINARY
	event.Raw = append([]byte{0xff, 0x00, ' '}, []byte(`password="binary-secret" safe=value`)...)

	got, rejection := v.ValidateAndNormalizeEvent(event, EventContext{ReceivedAt: validationTestNow})
	if rejection != nil {
		t.Fatalf("ValidateAndNormalizeEvent() rejection = %v", rejection)
	}
	if bytes.Contains(got.Event.Raw, []byte("binary-secret")) || !bytes.Contains(got.Event.Raw, []byte(DefaultRedactionReplacement)) {
		t.Fatalf("binary raw was not redacted: %q", got.Event.Raw)
	}
	if len(got.Event.Raw) < 2 || got.Event.Raw[0] != 0xff || got.Event.Raw[1] != 0x00 || !bytes.Contains(got.Event.Raw, []byte("safe=value")) {
		t.Fatalf("binary redaction changed unrelated bytes: %q", got.Event.Raw)
	}
}

func TestValidateAndNormalizeEventRedactsUTF8JSONLabeledBinary(t *testing.T) {
	t.Parallel()

	v := newTestValidator(t, DefaultLimits())
	event := validTestEvent("event-binary-json-secret", "main")
	event.RawEncoding = opensplunk.RawEncoding_RAW_ENCODING_BINARY
	event.Raw = []byte(`{"pass\u0077ord":"binary-json-secret","safe":true}`)

	got, rejection := v.ValidateAndNormalizeEvent(event, EventContext{ReceivedAt: validationTestNow})
	if rejection != nil {
		t.Fatalf("ValidateAndNormalizeEvent() rejection = %v", rejection)
	}
	if bytes.Contains(got.Event.Raw, []byte("binary-json-secret")) || !bytes.Contains(got.Event.Raw, []byte(DefaultRedactionReplacement)) {
		t.Fatalf("UTF-8 JSON labeled binary was not redacted: %s", got.Event.Raw)
	}
}

func TestValidateAndNormalizeEventRedactsQuotedKeysInTextAndJSONStrings(t *testing.T) {
	t.Parallel()

	v := newTestValidator(t, DefaultLimits())
	for _, test := range []struct {
		raw    string
		secret string
	}{
		{raw: `prefix {"password":"quoted-text-secret"} trailing`, secret: "quoted-text-secret"},
		{raw: `prefix {'password':'single-quoted-secret'} trailing`, secret: "single-quoted-secret"},
		{raw: "prefix {\"password\"\n:\n\"multiline-quoted-secret\"} trailing", secret: "multiline-quoted-secret"},
		{raw: `prefix {"password":[["head"],["nested-tail-secret"]]} trailing`, secret: "nested-tail-secret"},
		{raw: `prefix {"password":[["unterminated-tail-secret"] trailing`, secret: "unterminated-tail-secret"},
		{raw: "prefix {\"authorization\": {\n\"credential\":\"auth-object-secret\"\n}} trailing", secret: "auth-object-secret"},
		{raw: "prefix {\"cookie\": [\n\"cookie-array-secret\"\n]} trailing", secret: "cookie-array-secret"},
		{raw: "prefix {\"private_key\": {\n\"material\":\"private-object-secret\"\n}} trailing", secret: "private-object-secret"},
	} {
		event := validTestEvent("event-quoted-key", "main")
		event.Raw = []byte(test.raw)
		got, rejection := v.ValidateAndNormalizeEvent(event, EventContext{ReceivedAt: validationTestNow})
		if rejection != nil {
			t.Fatalf("ValidateAndNormalizeEvent() rejection = %v", rejection)
		}
		if bytes.Contains(got.Event.Raw, []byte(test.secret)) {
			t.Fatalf("quoted raw key leaked a secret: %q", got.Event.Raw)
		}
	}

	embedded, err := json.Marshal(map[string]string{"note": "{\"password\"\n:\n\"embedded-json-string-secret\"}", "safe": "yes"})
	if err != nil {
		t.Fatal(err)
	}
	event := validTestEvent("event-quoted-key-json-string", "main")
	event.Raw = embedded
	got, rejection := v.ValidateAndNormalizeEvent(event, EventContext{ReceivedAt: validationTestNow})
	if rejection != nil {
		t.Fatalf("ValidateAndNormalizeEvent() rejection = %v", rejection)
	}
	if bytes.Contains(got.Event.Raw, []byte("embedded-json-string-secret")) || !json.Valid(got.Event.Raw) {
		t.Fatalf("JSON string quoted key leaked a secret: %s", got.Event.Raw)
	}
}

func TestValidateAndNormalizeEventRedactsRecursivelyEncodedJSONStrings(t *testing.T) {
	t.Parallel()

	v := newTestValidator(t, DefaultLimits())
	encodedObject, err := json.Marshal(`{"password":"encoded-string-secret"}`)
	if err != nil {
		t.Fatal(err)
	}
	outer, err := json.Marshal(map[string]string{"note": string(encodedObject), "safe": "yes"})
	if err != nil {
		t.Fatal(err)
	}
	deep := `{"password":"deep-encoded-secret"}`
	for range maxEmbeddedJSONRedactionDepth + 2 {
		encoded, marshalErr := json.Marshal(deep)
		if marshalErr != nil {
			t.Fatal(marshalErr)
		}
		deep = string(encoded)
	}
	event := validTestEvent("event-recursive-json-string", "main")
	event.Raw = outer
	event.Fields = object(stringField("encoded_payload", string(encodedObject)), stringField("deep_payload", deep))

	got, rejection := v.ValidateAndNormalizeEvent(event, EventContext{ReceivedAt: validationTestNow})
	if rejection != nil {
		t.Fatalf("ValidateAndNormalizeEvent() rejection = %v", rejection)
	}
	encodedEvent, err := proto.Marshal(got.Event)
	if err != nil {
		t.Fatal(err)
	}
	for _, secret := range []string{"encoded-string-secret", "deep-encoded-secret"} {
		if bytes.Contains(encodedEvent, []byte(secret)) {
			t.Fatalf("recursively encoded JSON leaked %q", secret)
		}
	}
}

func TestValidateAndNormalizeEventRedactsProseWrappedEncodedJSON(t *testing.T) {
	t.Parallel()

	v := newTestValidator(t, DefaultLimits())
	prettyInner := "{\"password\"\n:\"pretty-encoded-secret\"}"
	prettyEncoded, err := json.Marshal(prettyInner)
	if err != nil {
		t.Fatal(err)
	}
	prettyEncodedTwice, err := json.Marshal(string(prettyEncoded))
	if err != nil {
		t.Fatal(err)
	}
	longWhitespaceInner := "{\"password\"" + strings.Repeat("\n", int(DefaultLimits().MaxFieldNameBytes)) + ":\"bounded-whitespace-secret\"}"
	longWhitespaceEncoded, err := json.Marshal(longWhitespaceInner)
	if err != nil {
		t.Fatal(err)
	}
	unicodeWhitespaceNote := `failed payload="{\"password\"\u0020:\"unicode-space-secret\"}"`
	unicodeQuoteNote := `failed payload="{\u0022password\u0022:\u0022unicode-quote-secret\u0022}"`
	longEncodedKeyNote := `failed payload="{\"` + strings.Repeat(`\u0061`, 240) + `_password\":\"long-key-secret\"}"`
	repeatedUnicodeKeyInner := `{"\u0070assword":"repeated-unicode-key-secret"}`
	repeatedUnicodeKeyOnce, err := json.Marshal(repeatedUnicodeKeyInner)
	if err != nil {
		t.Fatal(err)
	}
	repeatedUnicodeKeyTwice, err := json.Marshal(string(repeatedUnicodeKeyOnce))
	if err != nil {
		t.Fatal(err)
	}
	strippedRepeatedUnicodeKey := string(repeatedUnicodeKeyTwice[1 : len(repeatedUnicodeKeyTwice)-1])
	for _, test := range []struct {
		name   string
		note   string
		secret string
	}{
		{
			name:   "single encoding",
			note:   `failed payload="{\"password\":\"prose-wrapped-secret\"}"`,
			secret: "prose-wrapped-secret",
		},
		{
			name:   "encoded JSON whitespace",
			note:   "failed payload=" + string(prettyEncoded),
			secret: "pretty-encoded-secret",
		},
		{
			name:   "repeated encoding",
			note:   "failed payload=" + string(prettyEncodedTwice),
			secret: "pretty-encoded-secret",
		},
		{
			name:   "encoded whitespace exceeds scan bound",
			note:   "failed payload=" + string(longWhitespaceEncoded),
			secret: "bounded-whitespace-secret",
		},
		{
			name:   "Unicode-encoded JSON whitespace",
			note:   unicodeWhitespaceNote,
			secret: "unicode-space-secret",
		},
		{
			name:   "Unicode-encoded quote delimiters",
			note:   unicodeQuoteNote,
			secret: "unicode-quote-secret",
		},
		{
			name:   "unmatched quote before encoded delimiters",
			note:   `prefix "safe payload="{\u0022password\u0022:\u0022unmatched-quote-secret\u0022}"`,
			secret: "unmatched-quote-secret",
		},
		{
			name:   "escaped quote inside encoded key",
			note:   `failed payload={\"password\\\"safe\":\"embedded-quote-secret\"}`,
			secret: "embedded-quote-secret",
		},
		{
			name:   "encoded key ending in escaped backslash",
			note:   `failed payload={\"password\\\\\":\"trailing-backslash-key-secret\"}`,
			secret: "trailing-backslash-key-secret",
		},
		{
			name:   "long Unicode-encoded key",
			note:   longEncodedKeyNote,
			secret: "long-key-secret",
		},
		{
			name:   "twice-encoded Unicode key",
			note:   "failed payload=" + string(repeatedUnicodeKeyTwice),
			secret: "repeated-unicode-key-secret",
		},
		{
			name:   "stripped twice-encoded Unicode key",
			note:   "failed payload=" + strippedRepeatedUnicodeKey,
			secret: "repeated-unicode-key-secret",
		},
		{
			name:   "encoded fragment followed by key-value secret",
			note:   `payload="{\"password\":\"fragment-secret\"}" password=trailing-secret`,
			secret: "secret",
		},
		{
			name:   "encoded quoted password value",
			note:   `password=\"escaped-opening-secret\" safe=value`,
			secret: "escaped-opening-secret",
		},
		{
			name:   "encoded quoted authorization credential",
			note:   `Authorization=Bearer \"credential with spaces\" safe=value`,
			secret: "credential with spaces",
		},
		{
			name:   "unquoted multiword password",
			note:   `password=correct horse battery staple`,
			secret: "horse battery staple",
		},
		{
			name:   "unquoted comma password",
			note:   `password=abc,def safe=value`,
			secret: "def",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			outer, marshalErr := json.Marshal(map[string]string{"note": test.note, "safe": "yes"})
			if marshalErr != nil {
				t.Fatal(marshalErr)
			}
			event := validTestEvent("event-prose-wrapped-json", "main")
			event.Raw = outer

			got, rejection := v.ValidateAndNormalizeEvent(event, EventContext{ReceivedAt: validationTestNow})
			if rejection != nil {
				t.Fatalf("ValidateAndNormalizeEvent() rejection = %v", rejection)
			}
			if bytes.Contains(got.Event.Raw, []byte(test.secret)) || !json.Valid(got.Event.Raw) {
				t.Fatalf("prose-wrapped encoded JSON leaked a secret: %s", got.Event.Raw)
			}
			if !bytes.Contains(got.Event.Raw, []byte(`"safe":"yes"`)) {
				t.Fatalf("redaction removed an unrelated JSON member: %s", got.Event.Raw)
			}

			direct := validTestEvent("event-direct-prose-wrapped-json", "main")
			direct.Raw = []byte(test.note)
			message := test.note
			direct.Message = &message
			direct.Fields = object(stringField("note", test.note))
			directGot, directRejection := v.ValidateAndNormalizeEvent(direct, EventContext{ReceivedAt: validationTestNow})
			if directRejection != nil {
				t.Fatalf("direct ValidateAndNormalizeEvent() rejection = %v", directRejection)
			}
			encodedEvent, encodeErr := proto.Marshal(directGot.Event)
			if encodeErr != nil {
				t.Fatal(encodeErr)
			}
			if bytes.Contains(encodedEvent, []byte(test.secret)) {
				t.Fatalf("direct raw/message/typed string leaked %q", test.secret)
			}
		})
	}
}

func TestValidateAndNormalizeEventBoundsEscapedQuoteFallbackWork(t *testing.T) {
	t.Parallel()

	v := newTestValidator(t, DefaultLimits())
	raw := bytes.Repeat([]byte{'\\', '"', ':'}, 10_000)
	event := validTestEvent("event-escaped-quote-run", "main")
	event.Raw = raw

	got, rejection := v.ValidateAndNormalizeEvent(event, EventContext{ReceivedAt: validationTestNow})
	if rejection != nil {
		t.Fatalf("ValidateAndNormalizeEvent() rejection = %v", rejection)
	}
	if !bytes.Equal(got.Event.Raw, raw) {
		t.Fatalf("safe escaped-quote run changed unexpectedly")
	}
}

func TestValidateAndNormalizeEventFailsClosedAtEncodedJSONDepthBound(t *testing.T) {
	t.Parallel()

	v := newTestValidator(t, DefaultLimits())
	deep := `prefix {\"password\":\"depth-bound-secret\"}`
	for range maxEmbeddedJSONRedactionDepth {
		encoded, err := json.Marshal(deep)
		if err != nil {
			t.Fatal(err)
		}
		deep = string(encoded)
	}
	outer, err := json.Marshal(map[string]string{"note": deep, "safe": "yes"})
	if err != nil {
		t.Fatal(err)
	}
	event := validTestEvent("event-depth-bound-json", "main")
	event.Raw = outer

	got, rejection := v.ValidateAndNormalizeEvent(event, EventContext{ReceivedAt: validationTestNow})
	if rejection != nil {
		t.Fatalf("ValidateAndNormalizeEvent() rejection = %v", rejection)
	}
	if bytes.Contains(got.Event.Raw, []byte("depth-bound-secret")) || !json.Valid(got.Event.Raw) {
		t.Fatalf("depth-bound encoded JSON leaked a secret: %s", got.Event.Raw)
	}
}

func TestValidateAndNormalizeEventBoundsKeyNameNotDelimiterWhitespace(t *testing.T) {
	t.Parallel()

	v := newTestValidator(t, DefaultLimits())
	event := validTestEvent("event-spaced-delimiter", "main")
	event.Raw = []byte("password" + strings.Repeat(" ", int(DefaultLimits().MaxFieldNameBytes)+1) + "=spaced-delimiter-secret safe=value")
	got, rejection := v.ValidateAndNormalizeEvent(event, EventContext{ReceivedAt: validationTestNow})
	if rejection != nil {
		t.Fatalf("ValidateAndNormalizeEvent() rejection = %v", rejection)
	}
	if bytes.Contains(got.Event.Raw, []byte("spaced-delimiter-secret")) || !bytes.Contains(got.Event.Raw, []byte("safe=value")) {
		t.Fatalf("delimiter whitespace bypassed redaction: %q", got.Event.Raw)
	}
}

func TestValidateAndNormalizeEventPreservesLineAfterEmptyUnquotedSecret(t *testing.T) {
	t.Parallel()

	v := newTestValidator(t, DefaultLimits())
	event := validTestEvent("event-empty-secret", "main")
	event.Raw = []byte("password=\nmessage=safe")
	got, rejection := v.ValidateAndNormalizeEvent(event, EventContext{ReceivedAt: validationTestNow})
	if rejection != nil {
		t.Fatalf("ValidateAndNormalizeEvent() rejection = %v", rejection)
	}
	if !bytes.Equal(got.Event.Raw, event.Raw) {
		t.Fatalf("empty unquoted secret consumed the next line: got %q, want %q", got.Event.Raw, event.Raw)
	}
}

func TestValidateAndNormalizeEventCanonicalizesDuplicateJSONKeys(t *testing.T) {
	t.Parallel()

	v := newTestValidator(t, DefaultLimits())
	tests := []struct {
		name string
		raw  string
	}{
		{
			name: "shadowed sensitive child",
			raw:  `{"outer":{"password":"shadow-secret"},"outer":"safe","visible":1}`,
		},
		{
			name: "duplicate sensitive key",
			raw:  `{"password":"first-secret","password":"second-secret","visible":1}`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			event := validTestEvent("event-duplicate-json", "main")
			event.Raw = []byte(test.raw)
			got, rejection := v.ValidateAndNormalizeEvent(event, EventContext{ReceivedAt: validationTestNow})
			if rejection != nil {
				t.Fatalf("ValidateAndNormalizeEvent() rejection = %v", rejection)
			}
			for _, secret := range []string{"shadow-secret", "first-secret", "second-secret"} {
				if bytes.Contains(got.Event.Raw, []byte(secret)) {
					t.Fatalf("duplicate-key JSON leaked %q: %s", secret, got.Event.Raw)
				}
			}
			if !json.Valid(got.Event.Raw) || !bytes.Contains(got.Event.Raw, []byte(`"visible":1`)) {
				t.Fatalf("duplicate-key JSON was not safely canonicalized: %s", got.Event.Raw)
			}
		})
	}
}

func TestRedactJSONReportsValidUnchangedInput(t *testing.T) {
	t.Parallel()

	v := newTestValidator(t, DefaultLimits())
	raw := []byte("{ \n  \"message\": \"safe\", \"status\": 200\n}\n")
	redacted, parsed := v.redactJSON(raw)
	if !parsed {
		t.Fatal("redactJSON() parsed = false for valid JSON")
	}
	if !bytes.Equal(redacted, raw) {
		t.Fatalf("redactJSON() changed safe JSON:\n got: %q\nwant: %q", redacted, raw)
	}
	if got := v.redactText(raw); !bytes.Equal(got, raw) {
		t.Fatalf("redactText() changed safe JSON:\n got: %q\nwant: %q", got, raw)
	}
}

func camelCaseSensitiveField(field string) string {
	parts := strings.Split(field, "_")
	for index := 1; index < len(parts); index++ {
		if parts[index] != "" {
			parts[index] = strings.ToUpper(parts[index][:1]) + parts[index][1:]
		}
	}
	return strings.Join(parts, "")
}

func TestValidateAndNormalizeEventRechecksSizeAfterRedaction(t *testing.T) {
	event := validTestEvent("event-expansion", "main")
	event.Fields = object(stringField("token", "x"))
	limits := DefaultLimits()
	limits.MaxEventBytes = uint64(proto.Size(event) + 32)
	v, err := NewValidator(limits, RedactionPolicy{
		SensitiveFields: []string{"token"},
		Replacement:     string(bytes.Repeat([]byte("r"), 256)),
	})
	if err != nil {
		t.Fatal(err)
	}

	_, rejection := v.ValidateAndNormalizeEvent(event, EventContext{ReceivedAt: validationTestNow})
	assertEventRejectionCode(t, rejection, opensplunk.EventRejectionCode_EVENT_REJECTION_CODE_EVENT_TOO_LARGE)
}

func TestValidateAndNormalizeEventEnforcesTimeAndSizeBounds(t *testing.T) {
	limits := DefaultLimits()
	limits.MaxEventAge = 24 * time.Hour
	limits.MaxFutureSkew = time.Minute
	limits.MaxEventBytes = 512
	v := newTestValidator(t, limits)

	tests := []struct {
		name string
		edit func(*opensplunk.LogEvent)
		code opensplunk.EventRejectionCode
	}{
		{
			name: "invalid event ID",
			edit: func(e *opensplunk.LogEvent) { e.EventId = "event id with spaces" },
			code: opensplunk.EventRejectionCode_EVENT_REJECTION_CODE_INVALID_EVENT_ID,
		},
		{
			name: "event timestamp too old",
			edit: func(e *opensplunk.LogEvent) { e.EventTime = timestamppb.New(validationTestNow.Add(-25 * time.Hour)) },
			code: opensplunk.EventRejectionCode_EVENT_REJECTION_CODE_INVALID_TIMESTAMP,
		},
		{
			name: "collection timestamp in future",
			edit: func(e *opensplunk.LogEvent) {
				e.CollectedAt = timestamppb.New(validationTestNow.Add(2 * time.Minute))
			},
			code: opensplunk.EventRejectionCode_EVENT_REJECTION_CODE_INVALID_TIMESTAMP,
		},
		{
			name: "invalid protobuf timestamp",
			edit: func(e *opensplunk.LogEvent) {
				e.EventTime = &timestamppb.Timestamp{Seconds: math.MaxInt64}
			},
			code: opensplunk.EventRejectionCode_EVENT_REJECTION_CODE_INVALID_TIMESTAMP,
		},
		{
			name: "event too large",
			edit: func(e *opensplunk.LogEvent) { e.Raw = bytes.Repeat([]byte("x"), 1024) },
			code: opensplunk.EventRejectionCode_EVENT_REJECTION_CODE_EVENT_TOO_LARGE,
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
	tests := []struct {
		name    string
		edit    func(*Config)
		message string
	}{
		{
			name: "batch events",
			edit: func(config *Config) {
				config.Limits.MaxBatchEvents = HardMaxBatchEvents + 1
			},
			message: "max batch events cannot exceed hard limit",
		},
		{
			name: "in-flight batches",
			edit: func(config *Config) {
				config.MaxInFlightBatches = HardMaxInFlightBatches + 1
			},
			message: "max in-flight batches cannot exceed hard limit",
		},
		{
			name: "per-subject streams",
			edit: func(config *Config) {
				config.MaxStreamsPerSubject = HardMaxStreamsPerSubject + 1
			},
			message: "max streams per subject cannot exceed hard limit",
		},
	}
	authorizer := staticTestAuthorizer()
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			config := withTestSessionManager(testServiceConfig(), authorizer)
			test.edit(&config)
			_, err := NewService(config, authorizer, acceptingStore())
			if err == nil || !strings.Contains(err.Error(), test.message) {
				t.Fatalf(
					"NewService() error = %v, want %q hard-limit rejection",
					err,
					test.message,
				)
			}
		})
	}
}

func TestEventIDDigestUsesLengthPrefixedEventIDs(t *testing.T) {
	events := []*opensplunk.LogEvent{{EventId: "a"}, {EventId: "bc"}}
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
	v, err := NewValidator(limits, RedactionPolicy{SensitiveFields: redactionTestFields})
	if err != nil {
		t.Fatal(err)
	}
	return v
}

func validTestEvent(id, index string) *opensplunk.LogEvent {
	message := "request completed"
	return &opensplunk.LogEvent{
		EventId:         id,
		IndexName:       index,
		EventTime:       timestamppb.New(validationTestNow.Add(-time.Minute)),
		CollectedAt:     timestamppb.New(validationTestNow.Add(-30 * time.Second)),
		EventTimeSource: opensplunk.EventTimeSource_EVENT_TIME_SOURCE_PARSED,
		Host:            "host-a",
		Source:          "/var/log/app.log",
		Sourcetype:      "json",
		Severity:        opensplunk.LogSeverity_LOG_SEVERITY_INFO,
		Message:         &message,
		Raw:             []byte(`{"message":"request completed","status":200}`),
		RawEncoding:     opensplunk.RawEncoding_RAW_ENCODING_UTF8,
		Fields:          object(stringField("status", "200")),
	}
}

func object(fields ...*opensplunk.TypedObjectField) *opensplunk.TypedObject {
	return &opensplunk.TypedObject{Fields: fields}
}

func stringField(name, value string) *opensplunk.TypedObjectField {
	return &opensplunk.TypedObjectField{
		Name: name,
		Value: &opensplunk.TypedValue{
			Kind: &opensplunk.TypedValue_StringValue{StringValue: value},
		},
	}
}

func objectField(name string, value *opensplunk.TypedObject) *opensplunk.TypedObjectField {
	return &opensplunk.TypedObjectField{
		Name: name,
		Value: &opensplunk.TypedValue{
			Kind: &opensplunk.TypedValue_ObjectValue{ObjectValue: value},
		},
	}
}

func amplifiedFieldMetadataObject(parentNames []string, leafCount int) *opensplunk.TypedObject {
	leaves := make([]*opensplunk.TypedObjectField, leafCount)
	for index := range leaves {
		leaves[index] = stringField(fmt.Sprintf("f%04d", index), "value")
	}
	nested := object(leaves...)
	for _, v := range slices.Backward(parentNames) {
		nested = object(objectField(v, nested))
	}
	return nested
}

func assertEventRejectionCode(t *testing.T, rejection *EventError, want opensplunk.EventRejectionCode) {
	t.Helper()
	if rejection == nil {
		t.Fatalf("rejection = nil, want %v", want)
	}
	if rejection.Code != want {
		t.Fatalf("rejection code = %v, want %v (rejection: %v)", rejection.Code, want, rejection)
	}
}
