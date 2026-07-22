package server

import (
	"context"
	"math"
	"reflect"
	"testing"
	"time"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
)

func TestValueToProtoPreservesEverySupportedKind(t *testing.T) {
	decimal, err := searchjobs.DecimalValue("-12345678901234567890.00100")
	if err != nil {
		t.Fatal(err)
	}
	object, err := searchjobs.ObjectValue(searchjobs.ObjectField{Name: "nested", Value: searchjobs.SignedValue(-2)})
	if err != nil {
		t.Fatal(err)
	}
	timestamp := time.Date(2026, 7, 22, 9, 8, 7, 654_321_000, time.FixedZone("offset", -7*60*60))
	tests := []struct {
		name     string
		value    searchjobs.Value
		kindType any
		check    func(*testing.T, *opensplunkv1.TypedValue)
	}{
		{name: "null", value: searchjobs.NullValue(), kindType: (*opensplunkv1.TypedValue_NullValue)(nil)},
		{name: "string", value: searchjobs.StringValue("hello"), kindType: (*opensplunkv1.TypedValue_StringValue)(nil), check: func(t *testing.T, value *opensplunkv1.TypedValue) {
			if value.GetStringValue() != "hello" {
				t.Fatalf("string = %q", value.GetStringValue())
			}
		}},
		{name: "signed", value: searchjobs.SignedValue(-9), kindType: (*opensplunkv1.TypedValue_Sint64Value)(nil), check: func(t *testing.T, value *opensplunkv1.TypedValue) {
			if value.GetSint64Value() != -9 {
				t.Fatalf("signed = %d", value.GetSint64Value())
			}
		}},
		{name: "unsigned", value: searchjobs.UnsignedValue(math.MaxUint64), kindType: (*opensplunkv1.TypedValue_Uint64Value)(nil), check: func(t *testing.T, value *opensplunkv1.TypedValue) {
			if value.GetUint64Value() != math.MaxUint64 {
				t.Fatalf("unsigned = %d", value.GetUint64Value())
			}
		}},
		{name: "double", value: searchjobs.DoubleValue(math.Inf(1)), kindType: (*opensplunkv1.TypedValue_DoubleValue)(nil), check: func(t *testing.T, value *opensplunkv1.TypedValue) {
			if !math.IsInf(value.GetDoubleValue(), 1) {
				t.Fatalf("double = %v", value.GetDoubleValue())
			}
		}},
		{name: "bool", value: searchjobs.BoolValue(true), kindType: (*opensplunkv1.TypedValue_BoolValue)(nil), check: func(t *testing.T, value *opensplunkv1.TypedValue) {
			if !value.GetBoolValue() {
				t.Fatal("bool = false")
			}
		}},
		{name: "bytes", value: searchjobs.BytesValue([]byte{0, 1, 2}), kindType: (*opensplunkv1.TypedValue_BytesValue)(nil), check: func(t *testing.T, value *opensplunkv1.TypedValue) {
			if !reflect.DeepEqual(value.GetBytesValue(), []byte{0, 1, 2}) {
				t.Fatalf("bytes = %v", value.GetBytesValue())
			}
		}},
		{name: "timestamp", value: searchjobs.TimeValue(timestamp), kindType: (*opensplunkv1.TypedValue_TimestampValue)(nil), check: func(t *testing.T, value *opensplunkv1.TypedValue) {
			if !value.GetTimestampValue().AsTime().Equal(timestamp) {
				t.Fatalf("timestamp = %s", value.GetTimestampValue().AsTime())
			}
		}},
		{name: "duration", value: searchjobs.DurationValue(-1500 * time.Millisecond), kindType: (*opensplunkv1.TypedValue_DurationValue)(nil), check: func(t *testing.T, value *opensplunkv1.TypedValue) {
			if value.GetDurationValue().AsDuration() != -1500*time.Millisecond {
				t.Fatalf("duration = %s", value.GetDurationValue().AsDuration())
			}
		}},
		{name: "decimal", value: decimal, kindType: (*opensplunkv1.TypedValue_DecimalValue)(nil), check: func(t *testing.T, value *opensplunkv1.TypedValue) {
			if value.GetDecimalValue().GetValue() != "-12345678901234567890.001" {
				t.Fatalf("decimal = %q", value.GetDecimalValue().GetValue())
			}
		}},
		{name: "list", value: searchjobs.ListValue(searchjobs.StringValue("one"), searchjobs.NullValue()), kindType: (*opensplunkv1.TypedValue_ListValue)(nil), check: func(t *testing.T, value *opensplunkv1.TypedValue) {
			if len(value.GetListValue().GetValues()) != 2 {
				t.Fatalf("list = %+v", value.GetListValue())
			}
		}},
		{name: "object", value: object, kindType: (*opensplunkv1.TypedValue_ObjectValue)(nil), check: func(t *testing.T, value *opensplunkv1.TypedValue) {
			if value.GetObjectValue().GetFields()[0].GetValue().GetSint64Value() != -2 {
				t.Fatalf("object = %+v", value.GetObjectValue())
			}
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			converted, err := valueToProto(context.Background(), test.value)
			if err != nil {
				t.Fatalf("valueToProto: %v", err)
			}
			if reflect.TypeOf(converted.GetKind()) != reflect.TypeOf(test.kindType) {
				t.Fatalf("kind = %T, want %T", converted.GetKind(), test.kindType)
			}
			if test.check != nil {
				test.check(t, converted)
			}
		})
	}
}

func TestValueToProtoRejectsInvalidUTF8(t *testing.T) {
	if _, err := valueToProto(context.Background(), searchjobs.StringValue(string([]byte{0xff}))); err == nil {
		t.Fatal("invalid UTF-8 string was accepted")
	}
	object, err := searchjobs.ObjectValue(searchjobs.ObjectField{
		Name:  string([]byte{0xff}),
		Value: searchjobs.StringValue("value"),
	})
	if err != nil {
		t.Fatalf("ObjectValue: %v", err)
	}
	if _, err := valueToProto(context.Background(), object); err == nil {
		t.Fatal("invalid UTF-8 object field name was accepted")
	}
}

func TestValueToProtoAcceptsProtobufMinimumTimestamp(t *testing.T) {
	converted, err := valueToProto(context.Background(), searchjobs.TimeValue(time.Time{}))
	if err != nil {
		t.Fatalf("valueToProto(minimum timestamp): %v", err)
	}
	timestamp := converted.GetTimestampValue()
	if timestamp == nil || timestamp.CheckValid() != nil || timestamp.GetSeconds() != -62_135_596_800 || timestamp.GetNanos() != 0 {
		t.Fatalf("minimum timestamp = %#v", timestamp)
	}
	if _, err := validTimestamp(time.Time{}); err == nil {
		t.Fatal("required metadata accepted a zero timestamp")
	}
}

func TestValueToProtoCanonicalizesDecimalLexicalForm(t *testing.T) {
	decimal, err := searchjobs.DecimalValue("+0012.3400E+002")
	if err != nil {
		t.Fatal(err)
	}
	converted, err := valueToProto(context.Background(), decimal)
	if err != nil {
		t.Fatalf("valueToProto: %v", err)
	}
	if got := converted.GetDecimalValue().GetValue(); got != "1234" {
		t.Fatalf("canonical decimal = %q, want %q", got, "1234")
	}
	for _, source := range []string{"1234.0", "1.234e3", "+000123400e-2"} {
		decimal, err := searchjobs.DecimalValue(source)
		if err != nil {
			t.Fatal(err)
		}
		converted, err := valueToProto(context.Background(), decimal)
		if err != nil {
			t.Fatalf("valueToProto(%q): %v", source, err)
		}
		if got := converted.GetDecimalValue().GetValue(); got != "1234" {
			t.Fatalf("canonical decimal for %q = %q, want 1234", source, got)
		}
	}
	for source, want := range map[string]string{
		"-0.000e999": "0",
		"0.0000001":  "1e-7",
		"1e21":       "1e21",
	} {
		decimal, err := searchjobs.DecimalValue(source)
		if err != nil {
			t.Fatal(err)
		}
		converted, err := valueToProto(context.Background(), decimal)
		if err != nil {
			t.Fatalf("valueToProto(%q): %v", source, err)
		}
		if got := converted.GetDecimalValue().GetValue(); got != want {
			t.Fatalf("canonical decimal for %q = %q, want %q", source, got, want)
		}
	}
}

func TestMixedSchemaRetainsConcreteCellType(t *testing.T) {
	page, err := resultPageToProto(context.Background(), "job", searchjobs.ResultPage{
		Schema:   searchjobs.Schema{Columns: []searchjobs.Column{{Name: "_raw", Kind: searchjobs.ValueKindMixed, Nullable: true}}},
		Rows:     []searchjobs.ResultRow{{Ordinal: 0, Values: []searchjobs.Value{searchjobs.BytesValue([]byte{0xff})}}},
		Complete: true,
	}, opensplunkv1.ResultSetKind_RESULT_SET_KIND_EVENTS, false)
	if err != nil {
		t.Fatalf("resultPageToProto: %v", err)
	}
	if page.GetSchema().GetColumns()[0].GetValueType() != opensplunkv1.ValueType_VALUE_TYPE_MIXED {
		t.Fatalf("schema kind = %v", page.GetSchema().GetColumns()[0].GetValueType())
	}
	if got := page.GetRows()[0].GetCells()[0].GetBytesValue(); !reflect.DeepEqual(got, []byte{0xff}) {
		t.Fatalf("cell = %v", got)
	}
}

func TestResultKindForSPLRecognizesTransformingCommands(t *testing.T) {
	tests := map[string]opensplunkv1.ResultSetKind{
		"index=main":                                                opensplunkv1.ResultSetKind_RESULT_SET_KIND_EVENTS,
		"index=main | table level count":                            opensplunkv1.ResultSetKind_RESULT_SET_KIND_STATISTICS,
		"index=main | stats count by level":                         opensplunkv1.ResultSetKind_RESULT_SET_KIND_STATISTICS,
		"index=main | stats count by level | sort -count | head 20": opensplunkv1.ResultSetKind_RESULT_SET_KIND_STATISTICS,
		"index=main | top limit=20 message":                         opensplunkv1.ResultSetKind_RESULT_SET_KIND_STATISTICS,
		`index=main | eval duration_ms=tonumber(replace(duration, "ms$", "")) | stats count p95(duration_ms) AS p95_ms BY path | where p95_ms>500`: opensplunkv1.ResultSetKind_RESULT_SET_KIND_STATISTICS,
		"index=main | unsupported": opensplunkv1.ResultSetKind_RESULT_SET_KIND_UNSPECIFIED,
	}
	for source, want := range tests {
		if got := resultKindForSPL(source); got != want {
			t.Errorf("resultKindForSPL(%q) = %v, want %v", source, got, want)
		}
	}
}
