package searchjobproto

import (
	"context"
	"errors"
	"math"
	"reflect"
	"strings"
	"testing"
	"time"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
)

func TestValuePreservesEverySupportedKindAndNestedShape(t *testing.T) {
	t.Parallel()

	decimal, err := searchjobs.DecimalValue("+0012.3400E+002")
	if err != nil {
		t.Fatal(err)
	}
	object, err := searchjobs.ObjectValue(
		searchjobs.ObjectField{Name: "nested", Value: searchjobs.ListValue(searchjobs.SignedValue(-2), decimal)},
		searchjobs.ObjectField{Name: "raw", Value: searchjobs.BytesValue([]byte{0, 0xff})},
	)
	if err != nil {
		t.Fatal(err)
	}
	timestamp := time.Date(2026, 7, 22, 9, 8, 7, 654_321_000, time.FixedZone("offset", -7*60*60))
	tests := []struct {
		name     string
		value    searchjobs.Value
		kindType any
		check    func(*testing.T, *opensplunk.TypedValue)
	}{
		{name: "null", value: searchjobs.NullValue(), kindType: (*opensplunk.TypedValue_NullValue)(nil)},
		{name: "missing", value: searchjobs.MissingValue(), kindType: (*opensplunk.TypedValue_MissingValue)(nil), check: func(t *testing.T, value *opensplunk.TypedValue) {
			if value.GetMissingValue() != opensplunk.MissingValue_MISSING_VALUE_MISSING {
				t.Fatalf("missing = %v", value.GetMissingValue())
			}
		}},
		{name: "string", value: searchjobs.StringValue("hello"), kindType: (*opensplunk.TypedValue_StringValue)(nil), check: func(t *testing.T, value *opensplunk.TypedValue) {
			if value.GetStringValue() != "hello" {
				t.Fatalf("string = %q", value.GetStringValue())
			}
		}},
		{name: "signed", value: searchjobs.SignedValue(-9), kindType: (*opensplunk.TypedValue_Sint64Value)(nil)},
		{name: "unsigned", value: searchjobs.UnsignedValue(math.MaxUint64), kindType: (*opensplunk.TypedValue_Uint64Value)(nil)},
		{name: "double", value: searchjobs.DoubleValue(math.Inf(1)), kindType: (*opensplunk.TypedValue_DoubleValue)(nil), check: func(t *testing.T, value *opensplunk.TypedValue) {
			if !math.IsInf(value.GetDoubleValue(), 1) {
				t.Fatalf("double = %v", value.GetDoubleValue())
			}
		}},
		{name: "bool", value: searchjobs.BoolValue(true), kindType: (*opensplunk.TypedValue_BoolValue)(nil)},
		{name: "bytes", value: searchjobs.BytesValue([]byte{0, 1, 2}), kindType: (*opensplunk.TypedValue_BytesValue)(nil)},
		{name: "timestamp", value: searchjobs.TimeValue(timestamp), kindType: (*opensplunk.TypedValue_TimestampValue)(nil), check: func(t *testing.T, value *opensplunk.TypedValue) {
			if !value.GetTimestampValue().AsTime().Equal(timestamp) {
				t.Fatalf("timestamp = %s", value.GetTimestampValue().AsTime())
			}
		}},
		{name: "duration", value: searchjobs.DurationValue(-1500 * time.Millisecond), kindType: (*opensplunk.TypedValue_DurationValue)(nil)},
		{name: "decimal", value: decimal, kindType: (*opensplunk.TypedValue_DecimalValue)(nil), check: func(t *testing.T, value *opensplunk.TypedValue) {
			if value.GetDecimalValue().GetValue() != "1234" {
				t.Fatalf("decimal = %q", value.GetDecimalValue().GetValue())
			}
		}},
		{name: "list", value: searchjobs.ListValue(searchjobs.StringValue("one"), searchjobs.NullValue()), kindType: (*opensplunk.TypedValue_ListValue)(nil)},
		{name: "object", value: object, kindType: (*opensplunk.TypedValue_ObjectValue)(nil), check: func(t *testing.T, value *opensplunk.TypedValue) {
			fields := value.GetObjectValue().GetFields()
			if len(fields) != 2 || fields[0].GetName() != "nested" || fields[1].GetName() != "raw" {
				t.Fatalf("object fields = %+v", fields)
			}
			nested := fields[0].GetValue().GetListValue().GetValues()
			if len(nested) != 2 || nested[0].GetSint64Value() != -2 ||
				nested[1].GetDecimalValue().GetValue() != "1234" ||
				!reflect.DeepEqual(fields[1].GetValue().GetBytesValue(), []byte{0, 0xff}) {
				t.Fatalf("nested object = %+v", value.GetObjectValue())
			}
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			converted, err := Value(context.Background(), test.value)
			if err != nil {
				t.Fatal(err)
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

func TestValueRejectsInvalidUTF8AndHonorsContext(t *testing.T) {
	t.Parallel()
	var nilContext context.Context

	if _, err := Value(nilContext, searchjobs.NullValue()); err == nil {
		t.Fatal("nil context was accepted")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := Value(ctx, searchjobs.ListValue(searchjobs.StringValue("value"))); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled conversion error = %v, want %v", err, context.Canceled)
	}
	if _, err := Value(context.Background(), searchjobs.StringValue(string([]byte{0xff}))); err == nil {
		t.Fatal("invalid UTF-8 string was accepted")
	}
	object, err := searchjobs.ObjectValue(searchjobs.ObjectField{
		Name:  string([]byte{0xff}),
		Value: searchjobs.StringValue("value"),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Value(context.Background(), object); err == nil {
		t.Fatal("invalid UTF-8 object field name was accepted")
	}
	if _, err := Value(context.Background(), searchjobs.TimeValue(time.Date(10_000, 1, 1, 0, 0, 0, 0, time.UTC))); err == nil {
		t.Fatal("out-of-range timestamp was accepted")
	}
}

func TestSchemaPreservesOrderKindsAndSemantics(t *testing.T) {
	t.Parallel()

	schema := searchjobs.Schema{Columns: []searchjobs.Column{
		{Name: "_time", Kind: searchjobs.ValueKindTime},
		{Name: "ERROR", Kind: searchjobs.ValueKindUnsigned, Nullable: true},
		{Name: "body", Kind: searchjobs.ValueKindMixed, Multivalue: true},
	}}
	converted, err := Schema("schema-1", schema, ResultShape{Kind: opensplunk.ResultSetKind_RESULT_SET_KIND_TIME_SERIES, RuntimeNamedColumns: true})
	if err != nil {
		t.Fatal(err)
	}
	if converted.GetSchemaId() != "schema-1" || converted.GetRevision() != 1 ||
		converted.GetResultKind() != opensplunk.ResultSetKind_RESULT_SET_KIND_TIME_SERIES ||
		len(converted.GetColumns()) != len(schema.Columns) {
		t.Fatalf("schema = %+v", converted)
	}
	if converted.GetColumns()[0].GetFieldName() != "_time" ||
		converted.GetColumns()[0].GetSemanticType() != opensplunk.ColumnSemanticType_COLUMN_SEMANTIC_TYPE_EVENT_TIME {
		t.Fatalf("time column = %+v", converted.GetColumns()[0])
	}
	for index := 1; index < len(converted.GetColumns()); index++ {
		if converted.GetColumns()[index].GetSemanticType() != opensplunk.ColumnSemanticType_COLUMN_SEMANTIC_TYPE_METRIC {
			t.Fatalf("wide time-series column %d = %+v", index, converted.GetColumns()[index])
		}
	}
	if !converted.GetColumns()[1].GetNullable() ||
		converted.GetColumns()[2].GetValueType() != opensplunk.ValueType_VALUE_TYPE_MIXED ||
		!converted.GetColumns()[2].GetMultivalue() {
		t.Fatalf("column flags/types = %+v", converted.GetColumns())
	}
}

func TestSchemaMarksStaticTimeSeriesCountAsMetric(t *testing.T) {
	t.Parallel()

	converted, err := Schema("schema-static-timechart", searchjobs.Schema{
		Columns: []searchjobs.Column{
			{Name: "_time", Kind: searchjobs.ValueKindTime},
			{Name: "count", Kind: searchjobs.ValueKindUnsigned},
		},
	}, ResultShape{
		Kind:                opensplunk.ResultSetKind_RESULT_SET_KIND_TIME_SERIES,
		RuntimeNamedColumns: false,
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := converted.GetColumns()[1].GetSemanticType(); got !=
		opensplunk.ColumnSemanticType_COLUMN_SEMANTIC_TYPE_METRIC {
		t.Fatalf("count semantic = %v, want METRIC", got)
	}
}

func TestSchemaMapsExplicitMissingKind(t *testing.T) {
	t.Parallel()

	converted, err := Schema("schema-missing", searchjobs.Schema{Columns: []searchjobs.Column{
		{Name: "optional", Kind: searchjobs.ValueKindMissing, Nullable: true},
	}}, ResultShape{Kind: opensplunk.ResultSetKind_RESULT_SET_KIND_EVENTS})
	if err != nil {
		t.Fatal(err)
	}
	if len(converted.GetColumns()) != 1 ||
		converted.GetColumns()[0].GetValueType() != opensplunk.ValueType_VALUE_TYPE_MISSING ||
		!converted.GetColumns()[0].GetNullable() {
		t.Fatalf("missing schema column = %+v", converted.GetColumns())
	}
}

func TestSchemaPreservesPresenceSensitiveFlatMultivalueDelimiter(t *testing.T) {
	t.Parallel()

	converted, err := Schema("schema-delimiter", searchjobs.Schema{
		Columns: []searchjobs.Column{
			{
				Name:                       "users",
				Kind:                       searchjobs.ValueKindList,
				Multivalue:                 true,
				HasFlatMultivalueDelimiter: true,
			},
			{Name: "hosts", Kind: searchjobs.ValueKindList, Multivalue: true},
			{Name: "trend", Kind: searchjobs.ValueKindList, Multivalue: true, StatsSparkline: true},
		},
	}, ResultShape{Kind: opensplunk.ResultSetKind_RESULT_SET_KIND_STATISTICS})
	if err != nil {
		t.Fatal(err)
	}
	if converted.Columns[0].FlatMultivalueDelimiter == nil ||
		converted.Columns[0].GetFlatMultivalueDelimiter() != "" ||
		converted.Columns[1].FlatMultivalueDelimiter != nil {
		t.Fatalf("delimiter transport = %+v", converted.Columns)
	}
	if !converted.Columns[2].GetStatsSparkline() {
		t.Fatalf("sparkline transport = %+v", converted.Columns)
	}
}

func TestSchemaRejectsMalformedColumns(t *testing.T) {
	t.Parallel()

	tests := []searchjobs.Schema{
		{Columns: []searchjobs.Column{{Kind: searchjobs.ValueKindString}}},
		{Columns: []searchjobs.Column{{Name: string([]byte{0xff}), Kind: searchjobs.ValueKindString}}},
		{Columns: []searchjobs.Column{{Name: "field", Kind: searchjobs.ValueKindString}, {Name: "field", Kind: searchjobs.ValueKindString}}},
		{Columns: []searchjobs.Column{{Name: "field", Kind: searchjobs.ValueKindInvalid}}},
		{Columns: []searchjobs.Column{{Name: "field", Kind: searchjobs.ValueKindString, FlatMultivalueDelimiter: ",", HasFlatMultivalueDelimiter: true}}},
		{Columns: []searchjobs.Column{{Name: "field", Kind: searchjobs.ValueKindList, Multivalue: true, FlatMultivalueDelimiter: string([]byte{0xff}), HasFlatMultivalueDelimiter: true}}},
	}
	for _, schema := range tests {
		if _, err := Schema("schema", schema, ResultShape{Kind: opensplunk.ResultSetKind_RESULT_SET_KIND_EVENTS}); err == nil {
			t.Fatalf("Schema(%+v) error = nil", schema)
		}
	}
}

func TestRowsEnforcesBoundSchemaAndStableIDs(t *testing.T) {
	t.Parallel()

	schema := searchjobs.Schema{Columns: []searchjobs.Column{
		{Name: "count", Kind: searchjobs.ValueKindUnsigned},
		{Name: "details", Kind: searchjobs.ValueKindObject},
	}}
	details, err := searchjobs.ObjectValue(searchjobs.ObjectField{Name: "ok", Value: searchjobs.BoolValue(true)})
	if err != nil {
		t.Fatal(err)
	}
	input := []searchjobs.ResultRow{
		{Ordinal: 7, Values: []searchjobs.Value{searchjobs.UnsignedValue(2), details}},
		{Ordinal: 11, Values: []searchjobs.Value{searchjobs.UnsignedValue(3), searchjobs.NullValue()}},
	}
	converted, err := Rows(context.Background(), "job:opaque", schema, input, len(input))
	if err != nil {
		t.Fatal(err)
	}
	if len(converted) != 2 || converted[0].GetRowId() != "job:opaque:7" ||
		converted[0].GetOrdinal() != 7 || converted[1].GetRowId() != "job:opaque:11" ||
		converted[1].GetOrdinal() != 11 {
		t.Fatalf("rows = %+v", converted)
	}

	empty, err := Rows(context.Background(), "job", schema, nil, 0)
	if err != nil || len(empty) != 0 {
		t.Fatalf("zero-row conversion = (%+v, %v)", empty, err)
	}
	if _, err := Rows(context.Background(), "job", schema, input, 1); err == nil ||
		!strings.Contains(err.Error(), "limit") {
		t.Fatalf("over-limit error = %v", err)
	}
	if _, err := Rows(context.Background(), "job", schema, nil, -1); err == nil {
		t.Fatal("negative row limit was accepted")
	}
	if _, err := Rows(context.Background(), "job", schema, []searchjobs.ResultRow{
		{Values: []searchjobs.Value{searchjobs.UnsignedValue(1)}},
	}, 1); err == nil || !strings.Contains(err.Error(), "schema") {
		t.Fatalf("schema-mismatch error = %v", err)
	}
}

func TestResultPagePreservesHTTPPagingAndCompleteness(t *testing.T) {
	t.Parallel()

	page, err := ResultPage(context.Background(), "job", searchjobs.ResultPage{
		Schema:     searchjobs.Schema{Columns: []searchjobs.Column{{Name: "_raw", Kind: searchjobs.ValueKindBytes}}},
		Rows:       []searchjobs.ResultRow{{Ordinal: 9, Values: []searchjobs.Value{searchjobs.BytesValue([]byte{0xff})}}},
		NextCursor: "next",
		TotalRows:  10,
		Complete:   false,
	}, ResultShape{Kind: opensplunk.ResultSetKind_RESULT_SET_KIND_EVENTS}, true, true)
	if err != nil {
		t.Fatal(err)
	}
	if page.GetRows()[0].GetRowId() != "job:9" ||
		!reflect.DeepEqual(page.GetRows()[0].GetCells()[0].GetBytesValue(), []byte{0xff}) ||
		page.GetPage().GetNextPageToken() != "next" || page.GetPage().GetTotalSize() != 10 ||
		page.GetPage().GetTotalSizeExact() || page.GetSnapshotComplete() {
		t.Fatalf("page = %+v", page)
	}
}
