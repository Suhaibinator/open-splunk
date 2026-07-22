package collector

import (
	"bytes"
	"testing"
	"time"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
)

func TestNDJSONDecoderExtractsCanonicalFieldsAndPreservesTypes(t *testing.T) {
	t.Parallel()

	decoder := newTestDecoder(t, DecodeConfig{Format: InputFormatNDJSON})
	raw := []byte(`{"level":"INFO","timestamp":"2026-06-29T19:09:12.496713446Z","message":"Request summary statistics","method":"POST","path":"/api/v1/search/jobs/create","status":200,"duration":"645.046µs","bytes":20,"ok":true,"ratio":0.5,"tax":0.1,"nothing":null,"trace_id":"019f13e47d16735a9b5a2b6307ccb0e9","nested":{"attempt":2},"tags":["api",3]}`)
	collectedAt := time.Date(2026, time.June, 29, 19, 10, 0, 0, time.UTC)

	event, err := decoder.Decode(raw, SourcePosition{
		FileIdentity: "dev=1;ino=2", StartOffset: 10, EndOffset: 10 + uint64(len(raw)), LineNumber: 7,
	}, collectedAt)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}

	if got, want := event.GetEventTime().AsTime(), time.Date(2026, time.June, 29, 19, 9, 12, 496_713_446, time.UTC); !got.Equal(want) {
		t.Fatalf("event time = %s, want %s", got, want)
	}
	if got := event.GetEventTimeSource(); got != opensplunkv1.EventTimeSource_EVENT_TIME_SOURCE_PARSED {
		t.Fatalf("event time source = %v", got)
	}
	if got := event.GetSeverity(); got != opensplunkv1.LogSeverity_LOG_SEVERITY_INFO {
		t.Fatalf("severity = %v", got)
	}
	if got := event.GetLevel(); got != "INFO" {
		t.Fatalf("level = %q", got)
	}
	if got := event.GetMessage(); got != "Request summary statistics" {
		t.Fatalf("message = %q", got)
	}
	if got := event.GetTraceId(); got != "019f13e47d16735a9b5a2b6307ccb0e9" {
		t.Fatalf("trace ID = %q", got)
	}
	if !bytes.Equal(event.GetRaw(), raw) {
		t.Fatal("raw payload was not preserved byte-for-byte")
	}
	if event.GetRawEncoding() != opensplunkv1.RawEncoding_RAW_ENCODING_UTF8 {
		t.Fatalf("raw encoding = %v", event.GetRawEncoding())
	}
	if event.GetIndexName() != "gradethis" || event.GetHost() != "fixture-host" || event.GetSource() != "app.log" {
		t.Fatalf("configured metadata not applied: %#v", event)
	}

	fields := objectFields(event.GetFields())
	assertStringValue(t, fields["method"], "POST")
	assertSignedValue(t, fields["status"], 200)
	assertSignedValue(t, fields["bytes"], 20)
	assertBoolValue(t, fields["ok"], true)
	assertDoubleValue(t, fields["ratio"], 0.5)
	assertDecimalValue(t, fields["tax"], "0.1")
	assertNullValue(t, fields["nothing"])
	if fields["nested"].GetObjectValue() == nil || fields["tags"].GetListValue() == nil {
		t.Fatalf("nested values not preserved: %#v", fields)
	}
	for _, canonical := range []string{"level", "timestamp", "message", "trace_id"} {
		if _, exists := fields[canonical]; exists {
			t.Fatalf("canonical field %q was also emitted dynamically", canonical)
		}
	}
	if got := event.GetOrigin().GetLineNumber(); got != 7 {
		t.Fatalf("origin line = %d, want 7", got)
	}
}

func TestDecoderUsesFallbackTimeAndStableContentAddressedEventID(t *testing.T) {
	t.Parallel()

	decoder := newTestDecoder(t, DecodeConfig{Format: InputFormatNDJSON})
	collectedAt := time.Date(2026, time.July, 1, 2, 3, 4, 5, time.UTC)
	position := SourcePosition{FileIdentity: "id", StartOffset: 12, EndOffset: 22, LineNumber: 2}

	first, err := decoder.Decode([]byte(`{"message":"one"}`), position, collectedAt)
	if err != nil {
		t.Fatalf("Decode(first): %v", err)
	}
	second, err := decoder.Decode([]byte(`{"message":"one"}`), position, collectedAt.Add(time.Hour))
	if err != nil {
		t.Fatalf("Decode(second): %v", err)
	}
	changedBody, err := decoder.Decode([]byte(`{"message":"two"}`), position, collectedAt)
	if err != nil {
		t.Fatalf("Decode(changed body): %v", err)
	}
	changedPosition, err := decoder.Decode([]byte(`{"message":"one"}`), SourcePosition{FileIdentity: "id", StartOffset: 13, EndOffset: 23}, collectedAt)
	if err != nil {
		t.Fatalf("Decode(changed position): %v", err)
	}

	if first.GetEventId() != second.GetEventId() {
		t.Fatalf("stable source event ID changed: %q != %q", first.GetEventId(), second.GetEventId())
	}
	if first.GetEventId() == changedBody.GetEventId() || first.GetEventId() == changedPosition.GetEventId() {
		t.Fatal("event ID did not bind both source position and raw content")
	}
	if !first.GetEventTime().AsTime().Equal(collectedAt) || first.GetEventTimeSource() != opensplunkv1.EventTimeSource_EVENT_TIME_SOURCE_COLLECTED_AT_FALLBACK {
		t.Fatalf("fallback timestamp = %s (%v)", first.GetEventTime().AsTime(), first.GetEventTimeSource())
	}
}

func TestDecoderPreventsPayloadMetadataOverrideAndConstantsWin(t *testing.T) {
	t.Parallel()

	constant := &opensplunkv1.TypedObject{Fields: []*opensplunkv1.TypedObjectField{
		{Name: "environment", Value: stringValue("production")},
		{Name: "region", Value: stringValue("us-west-2")},
	}}
	decoder := newTestDecoder(t, DecodeConfig{Format: InputFormatNDJSON, ConstantFields: constant})
	event, err := decoder.Decode(
		[]byte(`{"timestamp":"2026-01-01T00:00:00Z","index":"attacker","index_name":"attacker","tenant_id":"attacker","host":"attacker","source":"attacker","sourcetype":"attacker","environment":"development","region":"elsewhere"}`),
		SourcePosition{FileIdentity: "id"},
		time.Date(2026, time.January, 1, 0, 0, 1, 0, time.UTC),
	)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if event.GetIndexName() != "gradethis" || event.GetHost() != "fixture-host" || event.GetSource() != "app.log" || event.GetSourcetype() != "go:zap:json" {
		t.Fatalf("payload overrode configured metadata: %#v", event)
	}
	fields := objectFields(event.GetFields())
	assertStringValue(t, fields["environment"], "production")
	assertStringValue(t, fields["region"], "us-west-2")
	for _, reserved := range []string{"index", "index_name", "tenant_id", "host", "source", "sourcetype"} {
		if _, exists := fields[reserved]; exists {
			t.Fatalf("reserved field %q leaked into dynamic fields", reserved)
		}
	}
}

func TestDecoderRejectsMalformedAmbiguousOrInvalidCanonicalJSON(t *testing.T) {
	t.Parallel()

	decoder := newTestDecoder(t, DecodeConfig{Format: InputFormatNDJSON})
	now := time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC)
	for _, raw := range [][]byte{
		[]byte(`{"a":1,"a":2}`),
		[]byte(`{"nested":{"a":1,"a":2}}`),
		[]byte(`[1,2,3]`),
		[]byte(`{"timestamp":"not-a-time"}`),
		[]byte(`{"message":{"not":"text"}}`),
		[]byte(`{"level":["INFO"]}`),
		[]byte(`{"a":1} trailing`),
	} {
		if _, err := decoder.Decode(raw, SourcePosition{FileIdentity: "id"}, now); err == nil {
			t.Fatalf("Decode(%q) unexpectedly succeeded", raw)
		}
	}
}

func TestRawDecoderPreservesBinaryWithoutInventingTextMessage(t *testing.T) {
	t.Parallel()

	decoder := newTestDecoder(t, DecodeConfig{Format: InputFormatRaw})
	raw := []byte{0xff, 0x00, 'x'}
	event, err := decoder.Decode(raw, SourcePosition{FileIdentity: "id"}, time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if event.GetRawEncoding() != opensplunkv1.RawEncoding_RAW_ENCODING_BINARY {
		t.Fatalf("raw encoding = %v", event.GetRawEncoding())
	}
	if event.Message != nil {
		t.Fatalf("binary event got message %q", event.GetMessage())
	}
}

func TestDecoderParsesExactNumericTimestampOutsideNanosecondDurationRange(t *testing.T) {
	t.Parallel()

	decoder := newTestDecoder(t, DecodeConfig{Format: InputFormatNDJSON})
	event, err := decoder.Decode(
		[]byte(`{"time":32503680000.000000001,"message":"year 3000"}`),
		SourcePosition{FileIdentity: "id"},
		time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC),
	)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	want := time.Date(3000, time.January, 1, 0, 0, 0, 1, time.UTC)
	if got := event.GetEventTime().AsTime(); !got.Equal(want) {
		t.Fatalf("event time = %s, want %s", got, want)
	}
}

func TestDecoderBoundsInputAndExtremeDecimalWork(t *testing.T) {
	t.Parallel()

	decoder, err := NewDecoder(DecodeConfig{
		Format:        InputFormatNDJSON,
		InputID:       "input",
		IndexName:     "index",
		Source:        "source",
		Sourcetype:    "json",
		Host:          "host",
		MaxLineBytes:  64,
		MaxJSONDepth:  2,
		MaxJSONFields: 2,
	})
	if err != nil {
		t.Fatalf("NewDecoder: %v", err)
	}
	now := time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC)
	for _, raw := range [][]byte{
		bytes.Repeat([]byte{'x'}, 65),
		[]byte(`{"a":{"b":1}}`),
		[]byte(`{"a":1,"b":2,"c":3}`),
	} {
		if _, err := decoder.Decode(raw, SourcePosition{}, now); err == nil {
			t.Fatalf("Decode(%q) unexpectedly succeeded", raw)
		}
	}

	wideDecoder := newTestDecoder(t, DecodeConfig{Format: InputFormatNDJSON})
	event, err := wideDecoder.Decode([]byte(`{"value":1e-2147483647}`), SourcePosition{}, now)
	if err != nil {
		t.Fatalf("Decode extreme decimal: %v", err)
	}
	assertDecimalValue(t, objectFields(event.GetFields())["value"], "1e-2147483647")
}

func FuzzDecoderDoesNotPanic(f *testing.F) {
	decoder, err := NewDecoder(DecodeConfig{
		Format:        InputFormatNDJSON,
		InputID:       "input",
		IndexName:     "index",
		Source:        "source",
		Sourcetype:    "json",
		Host:          "host",
		MaxLineBytes:  4 << 10,
		MaxJSONDepth:  8,
		MaxJSONFields: 64,
	})
	if err != nil {
		f.Fatalf("NewDecoder: %v", err)
	}
	for _, seed := range [][]byte{
		[]byte(`{"timestamp":"2026-01-01T00:00:00Z","value":1}`),
		[]byte(`{"a":{"b":[true,null,0.1]}}`),
		[]byte(`{"a":1,"a":2}`),
		{0xff, 0x00, '{'},
	} {
		f.Add(seed)
	}
	now := time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC)
	f.Fuzz(func(t *testing.T, raw []byte) {
		_, _ = decoder.Decode(raw, SourcePosition{}, now)
	})
}

func newTestDecoder(t *testing.T, override DecodeConfig) *Decoder {
	t.Helper()
	cfg := DecodeConfig{
		Format:         override.Format,
		InputID:        "gradethis-backend",
		IndexName:      "gradethis",
		Source:         "app.log",
		Sourcetype:     "go:zap:json",
		Host:           "fixture-host",
		Service:        "gradethis",
		ConstantFields: override.ConstantFields,
	}
	decoder, err := NewDecoder(cfg)
	if err != nil {
		t.Fatalf("NewDecoder: %v", err)
	}
	return decoder
}

func objectFields(object *opensplunkv1.TypedObject) map[string]*opensplunkv1.TypedValue {
	result := make(map[string]*opensplunkv1.TypedValue, len(object.GetFields()))
	for _, field := range object.GetFields() {
		result[field.GetName()] = field.GetValue()
	}
	return result
}

func stringValue(value string) *opensplunkv1.TypedValue {
	return &opensplunkv1.TypedValue{Kind: &opensplunkv1.TypedValue_StringValue{StringValue: value}}
}

func assertStringValue(t *testing.T, value *opensplunkv1.TypedValue, want string) {
	t.Helper()
	if value == nil || value.GetStringValue() != want {
		t.Fatalf("string value = %#v, want %q", value, want)
	}
}

func assertSignedValue(t *testing.T, value *opensplunkv1.TypedValue, want int64) {
	t.Helper()
	if value == nil || value.GetSint64Value() != want {
		t.Fatalf("signed value = %#v, want %d", value, want)
	}
}

func assertBoolValue(t *testing.T, value *opensplunkv1.TypedValue, want bool) {
	t.Helper()
	if value == nil || value.GetBoolValue() != want {
		t.Fatalf("bool value = %#v, want %t", value, want)
	}
}

func assertDoubleValue(t *testing.T, value *opensplunkv1.TypedValue, want float64) {
	t.Helper()
	if value == nil || value.GetDoubleValue() != want {
		t.Fatalf("double value = %#v, want %v", value, want)
	}
}

func assertDecimalValue(t *testing.T, value *opensplunkv1.TypedValue, want string) {
	t.Helper()
	if value == nil || value.GetDecimalValue().GetValue() != want {
		t.Fatalf("decimal value = %#v, want %q", value, want)
	}
}

func assertNullValue(t *testing.T, value *opensplunkv1.TypedValue) {
	t.Helper()
	if value == nil || value.GetNullValue() != opensplunkv1.NullValue_NULL_VALUE_NULL {
		t.Fatalf("null value = %#v", value)
	}
}
