package searchartifacts

import (
	"bytes"
	"encoding/json"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
)

func TestStoredValuePreservesArtifactJSONContract(t *testing.T) {
	t.Parallel()
	decimal, err := searchjobs.DecimalValue("+0012.3400e-2")
	if err != nil {
		t.Fatal(err)
	}
	emptyObject, err := searchjobs.ObjectValue()
	if err != nil {
		t.Fatal(err)
	}
	timestamp := time.Unix(-123, 456).UTC()
	tests := []struct {
		name  string
		value searchjobs.Value
		want  storedValue
	}{
		{"null", searchjobs.NullValue(), storedValue{Kind: searchjobs.ValueKindNull}},
		{"missing", searchjobs.MissingValue(), storedValue{Kind: searchjobs.ValueKindMissing}},
		{"string", searchjobs.StringValue("<event>\n\x00 café"), storedValue{Kind: searchjobs.ValueKindString, String: "<event>\n\x00 café"}},
		{"empty_string", searchjobs.StringValue(""), storedValue{Kind: searchjobs.ValueKindString}},
		{"signed", searchjobs.SignedValue(math.MinInt64), storedValue{Kind: searchjobs.ValueKindSigned, Signed: math.MinInt64}},
		{"unsigned", searchjobs.UnsignedValue(math.MaxUint64), storedValue{Kind: searchjobs.ValueKindUnsigned, Unsigned: math.MaxUint64}},
		{"double", searchjobs.DoubleValue(1.25), storedValue{Kind: searchjobs.ValueKindDouble, FloatBits: math.Float64bits(1.25)}},
		{"zero", searchjobs.DoubleValue(0), storedValue{Kind: searchjobs.ValueKindDouble}},
		{"negative_zero", searchjobs.DoubleValue(math.Copysign(0, -1)), storedValue{Kind: searchjobs.ValueKindDouble, FloatBits: 1 << 63}},
		{"nan_payload", searchjobs.DoubleValue(math.Float64frombits(0x7ff8000000000042)), storedValue{Kind: searchjobs.ValueKindDouble, FloatBits: 0x7ff8000000000042}},
		{"positive_infinity", searchjobs.DoubleValue(math.Inf(1)), storedValue{Kind: searchjobs.ValueKindDouble, FloatBits: 0x7ff0000000000000}},
		{"negative_infinity", searchjobs.DoubleValue(math.Inf(-1)), storedValue{Kind: searchjobs.ValueKindDouble, FloatBits: 0xfff0000000000000}},
		{"false", searchjobs.BoolValue(false), storedValue{Kind: searchjobs.ValueKindBool}},
		{"true", searchjobs.BoolValue(true), storedValue{Kind: searchjobs.ValueKindBool, Bool: true}},
		{"bytes", searchjobs.BytesValue([]byte{0, 1, 254, 255}), storedValue{Kind: searchjobs.ValueKindBytes, Bytes: []byte{0, 1, 254, 255}}},
		{"empty_bytes", searchjobs.BytesValue(nil), storedValue{Kind: searchjobs.ValueKindBytes}},
		{"time", searchjobs.TimeValue(timestamp), storedValue{Kind: searchjobs.ValueKindTime, UnixNano: timestamp.UnixNano()}},
		{"duration", searchjobs.DurationValue(-123 * time.Nanosecond), storedValue{Kind: searchjobs.ValueKindDuration, Duration: -123}},
		{"decimal", decimal, storedValue{Kind: searchjobs.ValueKindDecimal, Decimal: "+0012.3400e-2"}},
		{"empty_list", searchjobs.ListValue(), storedValue{Kind: searchjobs.ValueKindList}},
		{"empty_object", emptyObject, storedValue{Kind: searchjobs.ValueKindObject}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			// The same persisted scalar must be byte-identical both at the root
			// and inside alternating ordered objects and lists.
			value, want := test.value, test.want
			for depth := range 33 {
				if depth > 0 {
					if depth%2 == 0 {
						value = searchjobs.ListValue(value, searchjobs.MissingValue())
						want = storedValue{Kind: searchjobs.ValueKindList, List: []storedValue{want, {Kind: searchjobs.ValueKindMissing}}}
					} else {
						value, err = searchjobs.ObjectValue(
							searchjobs.ObjectField{Name: "z", Value: value},
							searchjobs.ObjectField{Name: "a", Value: searchjobs.NullValue()},
						)
						if err != nil {
							t.Fatal(err)
						}
						want = storedValue{Kind: searchjobs.ValueKindObject, Object: []storedObjectField{
							{Name: "z", Value: want}, {Name: "a", Value: storedValue{Kind: searchjobs.ValueKindNull}},
						}}
					}
				}
				stored, err := storeValue(value)
				if err != nil {
					t.Fatal(err)
				}
				gotJSON, err := json.Marshal(stored)
				if err != nil {
					t.Fatal(err)
				}
				wantJSON, err := json.Marshal(want)
				if err != nil {
					t.Fatal(err)
				}
				if !bytes.Equal(gotJSON, wantJSON) {
					t.Fatalf("depth %d: stored JSON = %s, want %s", depth, gotJSON, wantJSON)
				}
				restored, err := restoreValue(stored)
				if err != nil {
					t.Fatal(err)
				}
				restoredStored, err := storeValue(restored)
				if err != nil {
					t.Fatal(err)
				}
				restoredJSON, err := json.Marshal(restoredStored)
				if err != nil || !bytes.Equal(restoredJSON, wantJSON) {
					t.Fatalf("depth %d: restored JSON = %s, error %v, want %s", depth, restoredJSON, err, wantJSON)
				}
			}
		})
	}
}

func TestStoredNestedBytesRemainDetached(t *testing.T) {
	t.Parallel()
	value, err := searchjobs.ObjectValue(searchjobs.ObjectField{
		Name: "payload", Value: searchjobs.ListValue(searchjobs.BytesValue([]byte{1, 2, 3})),
	})
	if err != nil {
		t.Fatal(err)
	}
	stored, err := storeValue(value)
	if err != nil {
		t.Fatal(err)
	}
	stored.Object[0].Value.List[0].Bytes[0] = 9
	second, err := storeValue(value)
	if err != nil {
		t.Fatal(err)
	}
	if got := second.Object[0].Value.List[0].Bytes; !bytes.Equal(got, []byte{1, 2, 3}) {
		t.Fatalf("stored bytes mutated the source value: %v", got)
	}
}

func BenchmarkStoredRowJSON(b *testing.B) {
	for _, test := range artifactEncodingBenchmarkRows(b) {
		b.Run(test.name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				stored, err := storedRow(test.row)
				if err != nil {
					b.Fatal(err)
				}
				if _, err := json.Marshal(stored); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

type artifactEncodingBenchmarkRow struct {
	name string
	row  searchjobs.ResultRow
}

func artifactEncodingBenchmarkRows(b *testing.B) []artifactEncodingBenchmarkRow {
	b.Helper()
	flat := []searchjobs.Value{
		searchjobs.StringValue(strings.Repeat("raw event payload ", 64)),
		searchjobs.TimeValue(time.Unix(1_700_000_000, 123)),
		searchjobs.StringValue("server-1"), searchjobs.StringValue("main"),
		searchjobs.SignedValue(200), searchjobs.BoolValue(true),
	}
	multivalue := make([]searchjobs.Value, 32)
	for index := range multivalue {
		multivalue[index] = searchjobs.StringValue(strings.Repeat("value", 20))
	}
	rows := []artifactEncodingBenchmarkRow{
		{name: "flat", row: searchjobs.ResultRow{Values: flat}},
		{name: "multivalue", row: searchjobs.ResultRow{Values: []searchjobs.Value{searchjobs.ListValue(multivalue...)}}},
	}
	for _, depth := range []int{8, 24} {
		value := searchjobs.BytesValue(bytes.Repeat([]byte{1, 2, 3, 4}, 256))
		for range depth {
			var err error
			value, err = searchjobs.ObjectValue(searchjobs.ObjectField{Name: "child", Value: value})
			if err != nil {
				b.Fatal(err)
			}
		}
		name := "nested_8"
		if depth == 24 {
			name = "nested_24"
		}
		rows = append(rows, artifactEncodingBenchmarkRow{name: name, row: searchjobs.ResultRow{Values: []searchjobs.Value{value}}})
	}
	return rows
}
