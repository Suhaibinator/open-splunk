package export

import (
	"bytes"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
)

func TestCSVExactQuotingAndMandatoryFormulaDefense(t *testing.T) {
	t.Parallel()
	schema := searchjobs.Schema{Columns: []searchjobs.Column{
		{Name: "=name", Kind: searchjobs.ValueKindString},
		{Name: "quoted", Kind: searchjobs.ValueKindString},
		{Name: "tab", Kind: searchjobs.ValueKindString},
		{Name: "line", Kind: searchjobs.ValueKindString},
		{Name: "negative", Kind: searchjobs.ValueKindSigned},
		{Name: "binary", Kind: searchjobs.ValueKindBytes},
	}}
	selection, err := selectColumns(schema, nil)
	if err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	serializer, err := newCSVSerializer(&output, selection, CSVOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := serializer.WriteRow(searchjobs.ResultRow{Values: []searchjobs.Value{
		searchjobs.StringValue("=2+2"),
		searchjobs.StringValue("a,\"b\nc"),
		searchjobs.StringValue("\tcmd"),
		searchjobs.StringValue("\ncmd"),
		searchjobs.SignedValue(-7),
		searchjobs.BytesValue([]byte{0xfb}),
	}}); err != nil {
		t.Fatal(err)
	}
	if err := serializer.Close(); err != nil {
		t.Fatal(err)
	}
	want := "'=name,quoted,tab,line,negative,binary\n" +
		"'=2+2,\"a,\"\"b\nc\",'\tcmd,\"'\ncmd\",-7,'+w==\n"
	if output.String() != want {
		t.Fatalf("CSV output:\n%q\nwant:\n%q", output.String(), want)
	}
}

func TestJSONLinesAllValueKindsAreDeterministicAndLossless(t *testing.T) {
	t.Parallel()
	decimalValue, err := searchjobs.DecimalValue("-1234567890.0001")
	if err != nil {
		t.Fatal(err)
	}
	objectValue, err := searchjobs.ObjectValue(
		searchjobs.ObjectField{Name: "z", Value: searchjobs.BoolValue(true)},
		searchjobs.ObjectField{Name: "a", Value: searchjobs.UnsignedValue(math.MaxUint64)},
	)
	if err != nil {
		t.Fatal(err)
	}
	values := []searchjobs.Value{
		searchjobs.NullValue(),
		searchjobs.StringValue("hello"),
		searchjobs.SignedValue(-9_007_199_254_740_992),
		searchjobs.UnsignedValue(math.MaxUint64),
		searchjobs.DoubleValue(1.5),
		searchjobs.DoubleValue(math.Inf(1)),
		searchjobs.BoolValue(true),
		searchjobs.BytesValue([]byte{0, 1, 2}),
		searchjobs.TimeValue(time.Date(2026, 7, 22, 8, 9, 10, 123, time.FixedZone("offset", -7*60*60))),
		searchjobs.DurationValue(-1500 * time.Millisecond),
		decimalValue,
		searchjobs.ListValue(searchjobs.SignedValue(2), searchjobs.StringValue("x")),
		objectValue,
	}
	columns := make([]searchjobs.Column, len(values))
	for index, value := range values {
		columns[index] = searchjobs.Column{Name: string(rune('a' + index)), Kind: value.Kind()}
	}
	selection, err := selectColumns(searchjobs.Schema{Columns: columns}, nil)
	if err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	serializer, err := newJSONLinesSerializer(&output, selection, JSONLinesOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := serializer.WriteRow(searchjobs.ResultRow{Values: values}); err != nil {
		t.Fatal(err)
	}
	if err := serializer.Close(); err != nil {
		t.Fatal(err)
	}
	want := `{"a":null,"b":"hello","c":"-9007199254740992","d":"18446744073709551615","e":1.5,"f":"+Inf","g":true,"h":"AAEC","i":"2026-07-22T15:09:10.000000123Z","j":"-1.5s","k":"-1234567890.0001","l":[2,"x"],"m":{"z":true,"a":"18446744073709551615"}}` + "\n"
	if output.String() != want {
		t.Fatalf("JSONL output:\n%s\nwant:\n%s", output.String(), want)
	}

	output.Reset()
	stringIntegers, err := newJSONLinesSerializer(&output, selection, JSONLinesOptions{IntegerEncoding: JSONIntegerString})
	if err != nil {
		t.Fatal(err)
	}
	if err := stringIntegers.WriteRow(searchjobs.ResultRow{Values: values}); err != nil {
		t.Fatal(err)
	}
	if err := stringIntegers.Close(); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(output.String(), `"l":["2","x"]`) {
		t.Fatalf("string-integer JSONL = %s", output.String())
	}
}

func TestJSONLinesRecursiveTypeMetadata(t *testing.T) {
	t.Parallel()
	object, err := searchjobs.ObjectValue(searchjobs.ObjectField{
		Name: "items",
		Value: searchjobs.ListValue(
			searchjobs.UnsignedValue(math.MaxUint64),
			searchjobs.BytesValue([]byte("x")),
		),
	})
	if err != nil {
		t.Fatal(err)
	}
	selection, err := selectColumns(searchjobs.Schema{Columns: []searchjobs.Column{{Name: "payload", Kind: searchjobs.ValueKindObject}}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	serializer, err := newJSONLinesSerializer(&output, selection, JSONLinesOptions{IncludeTypeMetadata: true})
	if err != nil {
		t.Fatal(err)
	}
	if err := serializer.WriteRow(searchjobs.ResultRow{Values: []searchjobs.Value{object}}); err != nil {
		t.Fatal(err)
	}
	if err := serializer.Close(); err != nil {
		t.Fatal(err)
	}
	want := `{"payload":{"$type":"object","$value":{"items":{"$type":"list","$value":[{"$type":"unsigned","$value":"18446744073709551615"},{"$type":"bytes","$value":"eA=="}]}}}}` + "\n"
	if output.String() != want {
		t.Fatalf("typed JSONL = %s, want %s", output.String(), want)
	}
}

func TestJSONLinesStreamingEscapesAreDeterministic(t *testing.T) {
	t.Parallel()
	selection, err := selectColumns(searchjobs.Schema{Columns: []searchjobs.Column{{Name: "text", Kind: searchjobs.ValueKindString}}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	serializer, err := newJSONLinesSerializer(&output, selection, JSONLinesOptions{})
	if err != nil {
		t.Fatal(err)
	}
	value := "\"\\\b\f\n\r\t\x01<>&\u2028\u2029é"
	if err := serializer.WriteRow(searchjobs.ResultRow{Values: []searchjobs.Value{searchjobs.StringValue(value)}}); err != nil {
		t.Fatal(err)
	}
	if err := serializer.Close(); err != nil {
		t.Fatal(err)
	}
	want := "{\"text\":\"\\\"\\\\\\b\\f\\n\\r\\t\\u0001\\u003c\\u003e\\u0026\\u2028\\u2029é\"}\n"
	if output.String() != want {
		t.Fatalf("escaped JSONL = %q, want %q", output.String(), want)
	}
}

func TestJSONLinesBuffersSmallRowsAndEnforcesExactLimitOnClose(t *testing.T) {
	t.Parallel()
	selection, err := selectColumns(searchjobs.Schema{Columns: []searchjobs.Column{{Name: "x", Kind: searchjobs.ValueKindString}}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	limited := &exactLimitWriter{output: &output, limit: 4}
	serializer, err := newJSONLinesSerializer(limited, selection, JSONLinesOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := serializer.WriteRow(searchjobs.ResultRow{Values: []searchjobs.Value{searchjobs.StringValue("value")}}); err != nil {
		t.Fatalf("WriteRow() = %v; small rows should remain buffered", err)
	}
	if output.Len() != 0 || limited.written != 0 {
		t.Fatalf("WriteRow flushed early: output=%d written=%d", output.Len(), limited.written)
	}
	if err := serializer.Close(); !errorsIs(err, ErrByteLimit) {
		t.Fatalf("Close() = %v, want ErrByteLimit", err)
	}
	if output.Len() != 0 || limited.written != 0 {
		t.Fatalf("over-limit close wrote bytes: output=%d written=%d", output.Len(), limited.written)
	}
}

func TestColumnSelectionOrderAndValidation(t *testing.T) {
	t.Parallel()
	schema := searchjobs.Schema{Columns: []searchjobs.Column{
		{Name: "a", Kind: searchjobs.ValueKindString},
		{Name: "b", Kind: searchjobs.ValueKindSigned},
		{Name: "c", Kind: searchjobs.ValueKindBool},
	}}
	selection, err := selectColumns(schema, []string{"c", "a"})
	if err != nil {
		t.Fatal(err)
	}
	if got := []string{selection.columns[0].Name, selection.columns[1].Name}; strings.Join(got, ",") != "c,a" {
		t.Fatalf("selected columns = %v", got)
	}
	for _, requested := range [][]string{{"missing"}, {"a", "a"}, {""}} {
		if _, err := selectColumns(schema, requested); !errorsIs(err, ErrInvalidColumns) {
			t.Fatalf("selectColumns(%v) error = %v, want ErrInvalidColumns", requested, err)
		}
	}
}

func TestCSVNestedEncodingIsMemoryBounded(t *testing.T) {
	t.Parallel()
	selection, err := selectColumns(searchjobs.Schema{Columns: []searchjobs.Column{{Name: "values", Kind: searchjobs.ValueKindList}}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	serializer, err := newCSVSerializerWithLimit(&output, selection, CSVOptions{HeaderMode: CSVHeaderNone}, 16)
	if err != nil {
		t.Fatal(err)
	}
	value := searchjobs.ListValue(searchjobs.StringValue(strings.Repeat("x", 64)))
	if err := serializer.WriteRow(searchjobs.ResultRow{Values: []searchjobs.Value{value}}); !errorsIs(err, ErrByteLimit) {
		t.Fatalf("WriteRow(oversized nested cell) = %v, want ErrByteLimit", err)
	}
	if output.Len() != 0 {
		t.Fatalf("oversized nested cell wrote %d bytes", output.Len())
	}
}

// Kept local so this test file's imports remain intentionally small.
func errorsIs(err, target error) bool {
	for err != nil {
		if err == target {
			return true
		}
		type unwrapper interface{ Unwrap() error }
		unwrapped, ok := err.(unwrapper)
		if !ok {
			return false
		}
		err = unwrapped.Unwrap()
	}
	return false
}
