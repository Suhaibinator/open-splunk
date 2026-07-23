package queryexec

import (
	"context"
	"errors"
	"io"
	"math"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	clickhousedriver "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
	"github.com/Suhaibinator/open-splunk/internal/eventfields"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
)

func TestExecutorExecuteFieldSummaryReturnsCanonicalExactSummary(t *testing.T) {
	t.Parallel()
	rows := fieldSummaryFakeRows(
		fieldSummaryHeaderRow(
			"mixed",
			[]uint8{
				uint8(eventfields.StoredValueTypeNull),
				uint8(eventfields.StoredValueTypeString),
				uint8(eventfields.StoredValueTypeSint64),
				uint8(eventfields.StoredValueTypeDouble),
				uint8(eventfields.StoredValueTypeTimestamp),
				uint8(eventfields.StoredValueTypeDecimal),
			},
			12,
			1,
			1,
			13,
		),
		fieldSummaryGroup(eventfields.StoredValueTypeString, "z", 3),
		fieldSummaryGroup(eventfields.StoredValueTypeSint64, "2", 2),
		fieldSummaryGroup(eventfields.StoredValueTypeDouble, "-0", 1),
		fieldSummaryGroup(eventfields.StoredValueTypeDouble, "0", 1),
		fieldSummaryGroup(eventfields.StoredValueTypeTimestamp, "1969-12-31T19:00:00-05:00", 1),
		fieldSummaryGroup(eventfields.StoredValueTypeTimestamp, "1970-01-01T00:00:00Z", 1),
		fieldSummaryGroup(eventfields.StoredValueTypeDecimal, "1.0", 1),
		fieldSummaryGroup(eventfields.StoredValueTypeDecimal, "1.00", 1),
	)
	connection := &fakeQueryConnection{rows: rows}
	query := validCompiledFieldSummary(5, 32, clickhouse.MaximumFieldSummaryValueBytes)
	query.Spec.FieldName = "mixed"
	query.Args = []any{"tenant-a", uint64(44)}

	got, err := mustExecutor(t, connection).ExecuteFieldSummary(context.Background(), query)
	if err != nil {
		t.Fatalf("ExecuteFieldSummary() error = %v", err)
	}
	if !rows.closed {
		t.Fatal("field-summary rows were not closed")
	}
	if connection.query != query.SQL || !reflect.DeepEqual(connection.args, query.Args) {
		t.Fatalf("query/args = %q %#v, want %q %#v", connection.query, connection.args, query.SQL, query.Args)
	}
	wantTypes := []eventfields.StoredValueType{
		eventfields.StoredValueTypeNull,
		eventfields.StoredValueTypeString,
		eventfields.StoredValueTypeSint64,
		eventfields.StoredValueTypeDouble,
		eventfields.StoredValueTypeTimestamp,
		eventfields.StoredValueTypeDecimal,
	}
	if got.FieldName != "mixed" || !slices.Equal(got.ObservedTypes, wantTypes) ||
		got.EventCount != 12 || got.NullCount != 1 || got.MissingCount != 1 ||
		got.DistinctCount != 5 || len(got.TopValues) != 5 {
		t.Fatalf("summary = %#v", got)
	}
	wantKinds := []searchjobs.ValueKind{
		searchjobs.ValueKindString,
		searchjobs.ValueKindDouble,
		searchjobs.ValueKindDecimal,
		searchjobs.ValueKindTime,
		searchjobs.ValueKindSigned,
	}
	for index, wantKind := range wantKinds {
		if got.TopValues[index].Value.Kind() != wantKind {
			t.Fatalf("top[%d] kind = %v, want %v; summary=%#v", index, got.TopValues[index].Value.Kind(), wantKind, got)
		}
		wantCount := uint64(2)
		if index == 0 {
			wantCount = 3
		}
		if got.TopValues[index].Count != wantCount {
			t.Fatalf("top[%d] count = %d, want %d", index, got.TopValues[index].Count, wantCount)
		}
	}
	if value, ok := got.TopValues[0].Value.String(); !ok || value != "z" {
		t.Fatalf("string top value = (%q, %v)", value, ok)
	}
	if value, ok := got.TopValues[1].Value.Double(); !ok || value != 0 || math.Signbit(value) {
		t.Fatalf("double top value = (%v, %v), want positive zero", value, ok)
	}
	if value, ok := got.TopValues[2].Value.Decimal(); !ok || value != "1" {
		t.Fatalf("decimal top value = (%q, %v), want canonical 1", value, ok)
	}
	if value, ok := got.TopValues[3].Value.Time(); !ok ||
		!value.Equal(time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)) ||
		value.Location() != time.UTC {
		t.Fatalf("timestamp top value = (%v, %v)", value, ok)
	}
	if value, ok := got.TopValues[4].Value.Signed(); !ok || value != 2 {
		t.Fatalf("signed top value = (%d, %v)", value, ok)
	}
}

func TestExecutorExecuteFieldSummaryUsesStableDisplayThenKindOrdering(t *testing.T) {
	t.Parallel()
	rows := fieldSummaryFakeRows(
		fieldSummaryHeaderRow(
			"value",
			[]uint8{
				uint8(eventfields.StoredValueTypeSint64),
				uint8(eventfields.StoredValueTypeUint64),
				uint8(eventfields.StoredValueTypeDouble),
				uint8(eventfields.StoredValueTypeDecimal),
			},
			4,
			0,
			0,
			4,
		),
		fieldSummaryGroup(eventfields.StoredValueTypeSint64, "1", 1),
		fieldSummaryGroup(eventfields.StoredValueTypeUint64, "1", 1),
		fieldSummaryGroup(eventfields.StoredValueTypeDouble, "1", 1),
		fieldSummaryGroup(eventfields.StoredValueTypeDecimal, "1", 1),
	)
	got, err := mustExecutor(t, &fakeQueryConnection{rows: rows}).ExecuteFieldSummary(
		context.Background(),
		validCompiledFieldSummary(4, 4, 8),
	)
	if err != nil {
		t.Fatal(err)
	}
	wantKinds := []searchjobs.ValueKind{
		searchjobs.ValueKindSigned,
		searchjobs.ValueKindUnsigned,
		searchjobs.ValueKindDouble,
		searchjobs.ValueKindDecimal,
	}
	for index := range wantKinds {
		if got.TopValues[index].Value.Kind() != wantKinds[index] {
			t.Fatalf("top kinds = %#v, want %#v", fieldSummaryKinds(got.TopValues), wantKinds)
		}
	}
}

func TestExecutorExecuteFieldSummaryAcceptsKnownEmptyAndRejectsAbsentDynamic(t *testing.T) {
	t.Parallel()
	t.Run("known empty", func(t *testing.T) {
		query := validCompiledFieldSummary(1, 1, 8)
		rows := fieldSummaryFakeRows(fieldSummaryHeaderRow(query.Spec.FieldName, nil, 0, 0, 7, 7))
		got, err := mustExecutor(t, &fakeQueryConnection{rows: rows}).ExecuteFieldSummary(context.Background(), query)
		if err != nil {
			t.Fatal(err)
		}
		if got.FieldName != query.Spec.FieldName || got.EventCount != 0 || got.MissingCount != 7 ||
			got.DistinctCount != 0 || got.ObservedTypes == nil || got.TopValues == nil ||
			len(got.ObservedTypes) != 0 || len(got.TopValues) != 0 {
			t.Fatalf("known empty summary = %#v", got)
		}
	})
	t.Run("absent dynamic", func(t *testing.T) {
		query := validCompiledFieldSummary(1, 1, 8)
		query.FieldKnown = false
		rows := fieldSummaryFakeRows(fieldSummaryHeaderRow(query.Spec.FieldName, nil, 0, 0, 7, 7))
		got, err := mustExecutor(t, &fakeQueryConnection{rows: rows}).ExecuteFieldSummary(context.Background(), query)
		assertFieldSummaryError(t, got, err, clickhouse.ErrFieldSummaryNotFound)
	})
}

func TestExecutorExecuteFieldSummaryMapsHeaderControlsAfterFullConsumption(t *testing.T) {
	tests := []struct {
		name  string
		index int
		want  error
	}{
		{name: "metadata invalid", index: 10, want: ErrFieldMetadataUnavailable},
		{name: "unsupported", index: 11, want: searchjobs.ErrUnsupportedValue},
		{name: "oversized", index: 12, want: searchjobs.ErrExecutionLimit},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			header := fieldSummaryHeaderRow("value", []uint8{uint8(eventfields.StoredValueTypeString)}, 1, 0, 0, 1)
			header[test.index] = uint8(1)
			rows := fieldSummaryFakeRows(
				header,
				fieldSummaryGroup(eventfields.StoredValueTypeString, "secret", 1),
			)
			got, err := mustExecutor(t, &fakeQueryConnection{rows: rows}).ExecuteFieldSummary(
				context.Background(),
				validCompiledFieldSummary(1, 1, 32),
			)
			assertFieldSummaryError(t, got, err, test.want)
			if !rows.closed || rows.nextCalls != 2 {
				t.Fatalf("controlled stream closed=%v, nextCalls=%d; want fully consumed", rows.closed, rows.nextCalls)
			}
			if strings.Contains(err.Error(), "secret") {
				t.Fatalf("control error leaked encoded value: %v", err)
			}
		})
	}
}

func TestExecutorExecuteFieldSummaryRejectsMalformedSchemaAtomically(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*fakeRows)
	}{
		{name: "wrong column", mutate: func(rows *fakeRows) { rows.columns[0] = "wrong" }},
		{name: "reordered columns", mutate: func(rows *fakeRows) {
			rows.columns[0], rows.columns[1] = rows.columns[1], rows.columns[0]
		}},
		{name: "extra column", mutate: func(rows *fakeRows) {
			rows.columns = append(rows.columns, "extra")
			rows.types = append(rows.types, fakeColumnType{name: "extra", databaseType: "UInt8", scanType: reflect.TypeOf(uint8(0))})
		}},
		{name: "missing column type", mutate: func(rows *fakeRows) { rows.types = rows.types[:12] }},
		{name: "typed nil column type", mutate: func(rows *fakeRows) {
			var columnType *fakeColumnType
			rows.types[2] = columnType
		}},
		{name: "type name mismatch", mutate: func(rows *fakeRows) {
			rows.types[1] = fakeColumnType{name: "wrong", databaseType: "String", scanType: reflect.TypeOf("")}
		}},
		{name: "nullable", mutate: func(rows *fakeRows) {
			rows.types[4] = fakeColumnType{
				name: clickhouse.FieldSummaryNullCountColumn, databaseType: "UInt64",
				scanType: reflect.TypeOf(uint64(0)), nullable: true,
			}
		}},
		{name: "wrapped nullable", mutate: func(rows *fakeRows) {
			rows.types[4] = fakeColumnType{
				name: clickhouse.FieldSummaryNullCountColumn, databaseType: "Nullable(UInt64)",
				scanType: reflect.TypeOf(uint64(0)),
			}
		}},
		{name: "wrong physical type", mutate: func(rows *fakeRows) {
			rows.types[2] = fakeColumnType{
				name: clickhouse.FieldSummaryObservedTypesColumn, databaseType: "Array(UInt16)",
				scanType: reflect.TypeOf([]uint16{}),
			}
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			rows := fieldSummaryFakeRows(
				fieldSummaryHeaderRow("value", []uint8{uint8(eventfields.StoredValueTypeString)}, 1, 0, 0, 1),
				fieldSummaryGroup(eventfields.StoredValueTypeString, "a", 1),
			)
			test.mutate(rows)
			got, err := mustExecutor(t, &fakeQueryConnection{rows: rows}).ExecuteFieldSummary(
				context.Background(),
				validCompiledFieldSummary(1, 1, 8),
			)
			assertFieldSummaryError(t, got, err, searchjobs.ErrInvalidResult)
			if !rows.closed {
				t.Fatal("rows were not closed after schema rejection")
			}
		})
	}
}

func TestExecutorExecuteFieldSummaryRejectsMalformedRowsAtomically(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*fakeRows)
	}{
		{name: "missing header", mutate: func(rows *fakeRows) { rows.data = nil }},
		{name: "value before header", mutate: func(rows *fakeRows) { rows.data = rows.data[1:] }},
		{name: "second header", mutate: func(rows *fakeRows) {
			rows.data = append(rows.data, fieldSummaryHeaderRow("value", []uint8{2}, 1, 0, 0, 1))
		}},
		{name: "unknown row kind", mutate: func(rows *fakeRows) { rows.data[1][0] = uint8(2) }},
		{name: "header value type", mutate: func(rows *fakeRows) { rows.data[0][7] = uint8(2) }},
		{name: "header encoded value", mutate: func(rows *fakeRows) { rows.data[0][8] = "a" }},
		{name: "header value count", mutate: func(rows *fakeRows) { rows.data[0][9] = uint64(1) }},
		{name: "header control outside boolean", mutate: func(rows *fakeRows) { rows.data[0][10] = uint8(2) }},
		{name: "value field name", mutate: func(rows *fakeRows) { rows.data[1][1] = "value" }},
		{name: "value observed types", mutate: func(rows *fakeRows) { rows.data[1][2] = []uint8{2} }},
		{name: "value profile count", mutate: func(rows *fakeRows) { rows.data[1][3] = uint64(1) }},
		{name: "value control", mutate: func(rows *fakeRows) { rows.data[1][11] = uint8(1) }},
		{name: "zero value count", mutate: func(rows *fakeRows) { rows.data[1][9] = uint64(0) }},
		{name: "wrong field identity", mutate: func(rows *fakeRows) { rows.data[0][1] = "other" }},
		{name: "null exceeds event", mutate: func(rows *fakeRows) { rows.data[0][4] = uint64(2) }},
		{name: "event exceeds total", mutate: func(rows *fakeRows) { rows.data[0][6] = uint64(0) }},
		{name: "missing mismatch", mutate: func(rows *fakeRows) { rows.data[0][5] = uint64(1) }},
		{name: "empty types for present field", mutate: func(rows *fakeRows) { rows.data[0][2] = []uint8{} }},
		{name: "types for absent field", mutate: func(rows *fakeRows) {
			rows.data = rows.data[:1]
			rows.data[0][2] = []uint8{2}
			rows.data[0][3] = uint64(0)
			rows.data[0][5] = uint64(1)
			rows.data[0][6] = uint64(1)
		}},
		{name: "unspecified type", mutate: func(rows *fakeRows) { rows.data[0][2] = []uint8{0} }},
		{name: "out of range type", mutate: func(rows *fakeRows) { rows.data[0][2] = []uint8{13} }},
		{name: "unsorted types", mutate: func(rows *fakeRows) { rows.data[0][2] = []uint8{3, 2} }},
		{name: "duplicate types", mutate: func(rows *fakeRows) { rows.data[0][2] = []uint8{2, 2} }},
		{name: "null type without nulls", mutate: func(rows *fakeRows) { rows.data[0][2] = []uint8{1, 2} }},
		{name: "nulls without null type", mutate: func(rows *fakeRows) {
			rows.data[0][4] = uint64(1)
			rows.data[0][3] = uint64(2)
			rows.data[0][6] = uint64(2)
		}},
		{name: "unsupported type not flagged", mutate: func(rows *fakeRows) {
			rows.data = rows.data[:1]
			rows.data[0][2] = []uint8{1, 10}
			rows.data[0][3] = uint64(1)
			rows.data[0][4] = uint64(1)
		}},
		{name: "group type not observed", mutate: func(rows *fakeRows) {
			rows.data[1] = fieldSummaryGroup(eventfields.StoredValueTypeSint64, "1", 1)
		}},
		{name: "observed type has no group", mutate: func(rows *fakeRows) {
			rows.data[0][2] = []uint8{2, 3}
		}},
		{name: "group total short", mutate: func(rows *fakeRows) { rows.data[1][9] = uint64(0) }},
		{name: "group total excessive", mutate: func(rows *fakeRows) { rows.data[1][9] = uint64(2) }},
		{name: "invalid encoded value", mutate: func(rows *fakeRows) {
			rows.data[0][2] = []uint8{3}
			rows.data[1] = fieldSummaryGroup(eventfields.StoredValueTypeSint64, "+1", 1)
		}},
		{name: "null value group", mutate: func(rows *fakeRows) {
			rows.data[0][2] = []uint8{1}
			rows.data[0][4] = uint64(1)
			rows.data[1] = fieldSummaryGroup(eventfields.StoredValueTypeNull, "", 1)
		}},
		{name: "list value group", mutate: func(rows *fakeRows) {
			rows.data[0][2] = []uint8{10}
			rows.data[1] = fieldSummaryGroup(eventfields.StoredValueTypeList, "[]", 1)
		}},
		{name: "unsorted groups", mutate: func(rows *fakeRows) {
			rows.data[0][3] = uint64(2)
			rows.data[0][6] = uint64(2)
			rows.data = append(rows.data, fieldSummaryGroup(eventfields.StoredValueTypeString, "0", 1))
		}},
		{name: "duplicate raw group", mutate: func(rows *fakeRows) {
			rows.data[0][3] = uint64(2)
			rows.data[0][6] = uint64(2)
			rows.data = append(rows.data, fieldSummaryGroup(eventfields.StoredValueTypeString, "a", 1))
		}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			rows := fieldSummaryFakeRows(
				fieldSummaryHeaderRow("value", []uint8{uint8(eventfields.StoredValueTypeString)}, 1, 0, 0, 1),
				fieldSummaryGroup(eventfields.StoredValueTypeString, "a", 1),
			)
			test.mutate(rows)
			got, err := mustExecutor(t, &fakeQueryConnection{rows: rows}).ExecuteFieldSummary(
				context.Background(),
				validCompiledFieldSummary(4, 4, 32),
			)
			assertFieldSummaryError(t, got, err, searchjobs.ErrInvalidResult)
			if !rows.closed {
				t.Fatal("rows were not closed after row rejection")
			}
		})
	}
}

func TestExecutorExecuteFieldSummaryDetectsResourceAndCountOverflows(t *testing.T) {
	t.Run("distinct groups", func(t *testing.T) {
		rows := fieldSummaryFakeRows(
			fieldSummaryHeaderRow("value", []uint8{2}, 2, 0, 0, 2),
			fieldSummaryGroup(eventfields.StoredValueTypeString, "a", 1),
			fieldSummaryGroup(eventfields.StoredValueTypeString, "b", 1),
		)
		got, err := mustExecutor(t, &fakeQueryConnection{rows: rows}).ExecuteFieldSummary(
			context.Background(),
			validCompiledFieldSummary(1, 1, 8),
		)
		assertFieldSummaryError(t, got, err, searchjobs.ErrExecutionLimit)
	})
	t.Run("encoded value bytes", func(t *testing.T) {
		rows := fieldSummaryFakeRows(
			fieldSummaryHeaderRow("value", []uint8{2}, 1, 0, 0, 1),
			fieldSummaryGroup(eventfields.StoredValueTypeString, "ab", 1),
		)
		got, err := mustExecutor(t, &fakeQueryConnection{rows: rows}).ExecuteFieldSummary(
			context.Background(),
			validCompiledFieldSummary(1, 1, 1),
		)
		assertFieldSummaryError(t, got, err, searchjobs.ErrExecutionLimit)
	})
	t.Run("canonical value expansion", func(t *testing.T) {
		encoded := "1234567890123456789012"
		rows := fieldSummaryFakeRows(
			fieldSummaryHeaderRow("value", []uint8{12}, 1, 0, 0, 1),
			fieldSummaryGroup(eventfields.StoredValueTypeDecimal, encoded, 1),
		)
		got, err := mustExecutor(t, &fakeQueryConnection{rows: rows}).ExecuteFieldSummary(
			context.Background(),
			validCompiledFieldSummary(1, 1, uint32(len(encoded))),
		)
		assertFieldSummaryError(t, got, err, searchjobs.ErrExecutionLimit)
	})
	t.Run("canonical count", func(t *testing.T) {
		rows := fieldSummaryFakeRows(
			fieldSummaryHeaderRow("value", []uint8{12}, math.MaxUint64, 0, 0, math.MaxUint64),
			fieldSummaryGroup(eventfields.StoredValueTypeDecimal, "1.0", math.MaxUint64),
			fieldSummaryGroup(eventfields.StoredValueTypeDecimal, "1.00", 1),
		)
		got, err := mustExecutor(t, &fakeQueryConnection{rows: rows}).ExecuteFieldSummary(
			context.Background(),
			validCompiledFieldSummary(2, 2, 8),
		)
		assertFieldSummaryError(t, got, err, searchjobs.ErrInvalidResult)
	})
}

func TestFieldSummaryRetainedValueBytesAccountsImmutablePayloadCopies(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		valueType eventfields.StoredValueType
		encoded   string
		want      uint64
	}{
		{name: "string", valueType: eventfields.StoredValueTypeString, encoded: "abc", want: 6},
		{name: "decimal", valueType: eventfields.StoredValueTypeDecimal, encoded: "+001.2300e+2", want: 6},
		{name: "bytes", valueType: eventfields.StoredValueTypeBytes, encoded: "AAH/", want: 7},
		{name: "inline scalar", valueType: eventfields.StoredValueTypeSint64, encoded: "-1", want: 2},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			decoded, err := decodeFieldSummaryValue(test.valueType, test.encoded)
			if err != nil {
				t.Fatal(err)
			}
			if got := fieldSummaryRetainedValueBytes(decoded, test.encoded); got != test.want {
				t.Fatalf("fieldSummaryRetainedValueBytes() = %d, want %d", got, test.want)
			}
		})
	}
}

func TestDecodeFieldSummaryValueAcceptsEveryScalarKind(t *testing.T) {
	tests := []struct {
		name      string
		valueType eventfields.StoredValueType
		encoded   string
		kind      searchjobs.ValueKind
		canonical string
		check     func(*testing.T, searchjobs.Value)
	}{
		{
			name: "string", valueType: eventfields.StoredValueTypeString, encoded: "東京",
			kind: searchjobs.ValueKindString, canonical: "東京",
			check: func(t *testing.T, value searchjobs.Value) {
				if got, ok := value.String(); !ok || got != "東京" {
					t.Fatalf("String() = (%q, %v)", got, ok)
				}
			},
		},
		{
			name: "signed", valueType: eventfields.StoredValueTypeSint64, encoded: "-9223372036854775808",
			kind: searchjobs.ValueKindSigned, canonical: "-9223372036854775808",
			check: func(t *testing.T, value searchjobs.Value) {
				if got, ok := value.Signed(); !ok || got != math.MinInt64 {
					t.Fatalf("Signed() = (%d, %v)", got, ok)
				}
			},
		},
		{
			name: "unsigned", valueType: eventfields.StoredValueTypeUint64, encoded: "18446744073709551615",
			kind: searchjobs.ValueKindUnsigned, canonical: "18446744073709551615",
			check: func(t *testing.T, value searchjobs.Value) {
				if got, ok := value.Unsigned(); !ok || got != math.MaxUint64 {
					t.Fatalf("Unsigned() = (%d, %v)", got, ok)
				}
			},
		},
		{
			name: "double", valueType: eventfields.StoredValueTypeDouble, encoded: "1.25e+2",
			kind: searchjobs.ValueKindDouble, canonical: "125",
			check: func(t *testing.T, value searchjobs.Value) {
				if got, ok := value.Double(); !ok || got != 125 {
					t.Fatalf("Double() = (%v, %v)", got, ok)
				}
			},
		},
		{
			name: "negative zero", valueType: eventfields.StoredValueTypeDouble, encoded: "-0",
			kind: searchjobs.ValueKindDouble, canonical: "0",
			check: func(t *testing.T, value searchjobs.Value) {
				if got, ok := value.Double(); !ok || got != 0 || math.Signbit(got) {
					t.Fatalf("Double() = (%v, %v), want positive zero", got, ok)
				}
			},
		},
		{
			name: "boolean", valueType: eventfields.StoredValueTypeBool, encoded: "true",
			kind: searchjobs.ValueKindBool, canonical: "true",
			check: func(t *testing.T, value searchjobs.Value) {
				if got, ok := value.Bool(); !ok || !got {
					t.Fatalf("Bool() = (%v, %v)", got, ok)
				}
			},
		},
		{
			name: "bytes", valueType: eventfields.StoredValueTypeBytes, encoded: "AAH/",
			kind: searchjobs.ValueKindBytes, canonical: "AAH/",
			check: func(t *testing.T, value searchjobs.Value) {
				if got, ok := value.Bytes(); !ok || !slices.Equal(got, []byte{0, 1, 255}) {
					t.Fatalf("Bytes() = (%v, %v)", got, ok)
				}
			},
		},
		{
			name: "timestamp offset", valueType: eventfields.StoredValueTypeTimestamp,
			encoded: "1969-12-31T19:00:00-05:00",
			kind:    searchjobs.ValueKindTime, canonical: "1970-01-01T00:00:00Z",
			check: func(t *testing.T, value searchjobs.Value) {
				got, ok := value.Time()
				if !ok || !got.Equal(time.Unix(0, 0)) || got.Location() != time.UTC {
					t.Fatalf("Time() = (%v, %v)", got, ok)
				}
			},
		},
		{
			name: "year one timestamp", valueType: eventfields.StoredValueTypeTimestamp,
			encoded: "0001-01-01T00:00:00Z",
			kind:    searchjobs.ValueKindTime, canonical: "0001-01-01T00:00:00Z",
			check: func(t *testing.T, value searchjobs.Value) {
				got, ok := value.Time()
				if !ok || got.Year() != 1 {
					t.Fatalf("Time() = (%v, %v)", got, ok)
				}
			},
		},
		{
			name: "duration", valueType: eventfields.StoredValueTypeDuration, encoded: "-1:-2",
			kind: searchjobs.ValueKindDuration, canonical: "-1:-2",
			check: func(t *testing.T, value searchjobs.Value) {
				if got, ok := value.Duration(); !ok || got != -time.Second-2 {
					t.Fatalf("Duration() = (%v, %v)", got, ok)
				}
			},
		},
		{
			name: "decimal", valueType: eventfields.StoredValueTypeDecimal, encoded: "+001.2300e+2",
			kind: searchjobs.ValueKindDecimal, canonical: "123",
			check: func(t *testing.T, value searchjobs.Value) {
				if got, ok := value.Decimal(); !ok || got != "123" {
					t.Fatalf("Decimal() = (%q, %v)", got, ok)
				}
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := decodeFieldSummaryValue(test.valueType, test.encoded)
			if err != nil {
				t.Fatalf("decodeFieldSummaryValue(%d, %q) error = %v", test.valueType, test.encoded, err)
			}
			if got.kind != test.kind || got.canonical != test.canonical {
				t.Fatalf("decoded = %#v, want kind=%v canonical=%q", got, test.kind, test.canonical)
			}
			test.check(t, got.value)
		})
	}
}

func TestDecodeFieldSummaryValueRejectsMalformedAndCompositeValues(t *testing.T) {
	tests := []struct {
		name      string
		valueType eventfields.StoredValueType
		encoded   string
	}{
		{name: "invalid type", valueType: 0, encoded: ""},
		{name: "null", valueType: eventfields.StoredValueTypeNull, encoded: ""},
		{name: "list", valueType: eventfields.StoredValueTypeList, encoded: "[]"},
		{name: "object", valueType: eventfields.StoredValueTypeObject, encoded: "{}"},
		{name: "invalid UTF-8", valueType: eventfields.StoredValueTypeString, encoded: string([]byte{0xff})},
		{name: "signed plus", valueType: eventfields.StoredValueTypeSint64, encoded: "+1"},
		{name: "signed leading zero", valueType: eventfields.StoredValueTypeSint64, encoded: "01"},
		{name: "signed overflow", valueType: eventfields.StoredValueTypeSint64, encoded: "9223372036854775808"},
		{name: "unsigned negative", valueType: eventfields.StoredValueTypeUint64, encoded: "-1"},
		{name: "unsigned leading zero", valueType: eventfields.StoredValueTypeUint64, encoded: "01"},
		{name: "unsigned overflow", valueType: eventfields.StoredValueTypeUint64, encoded: "18446744073709551616"},
		{name: "double empty", valueType: eventfields.StoredValueTypeDouble, encoded: ""},
		{name: "double leading plus", valueType: eventfields.StoredValueTypeDouble, encoded: "+1"},
		{name: "double leading zero", valueType: eventfields.StoredValueTypeDouble, encoded: "01"},
		{name: "double hex", valueType: eventfields.StoredValueTypeDouble, encoded: "0x1p0"},
		{name: "double NaN", valueType: eventfields.StoredValueTypeDouble, encoded: "NaN"},
		{name: "double infinity", valueType: eventfields.StoredValueTypeDouble, encoded: "Inf"},
		{name: "boolean numeric", valueType: eventfields.StoredValueTypeBool, encoded: "1"},
		{name: "boolean title case", valueType: eventfields.StoredValueTypeBool, encoded: "True"},
		{name: "bytes padded", valueType: eventfields.StoredValueTypeBytes, encoded: "YQ=="},
		{name: "bytes URL alphabet", valueType: eventfields.StoredValueTypeBytes, encoded: "AAH_"},
		{name: "bytes malformed", valueType: eventfields.StoredValueTypeBytes, encoded: "a"},
		{name: "timestamp date only", valueType: eventfields.StoredValueTypeTimestamp, encoded: "1970-01-01"},
		{name: "timestamp impossible", valueType: eventfields.StoredValueTypeTimestamp, encoded: "2026-02-29T00:00:00Z"},
		{name: "timestamp comma fraction", valueType: eventfields.StoredValueTypeTimestamp, encoded: "1970-01-01T00:00:00,1Z"},
		{name: "timestamp excess fraction", valueType: eventfields.StoredValueTypeTimestamp, encoded: "1970-01-01T00:00:00.1234567890Z"},
		{name: "timestamp zone hour", valueType: eventfields.StoredValueTypeTimestamp, encoded: "1970-01-01T00:00:00+24:00"},
		{name: "timestamp zone minute", valueType: eventfields.StoredValueTypeTimestamp, encoded: "1970-01-01T00:00:00+23:60"},
		{
			name: "timestamp before UTC year one", valueType: eventfields.StoredValueTypeTimestamp,
			encoded: "0001-01-01T00:00:00+14:00",
		},
		{
			name: "timestamp after UTC year 9999", valueType: eventfields.StoredValueTypeTimestamp,
			encoded: "9999-12-31T23:59:59-14:00",
		},
		{name: "duration missing colon", valueType: eventfields.StoredValueTypeDuration, encoded: "1"},
		{name: "duration leading zero", valueType: eventfields.StoredValueTypeDuration, encoded: "01:0"},
		{name: "duration mixed sign", valueType: eventfields.StoredValueTypeDuration, encoded: "1:-1"},
		{name: "duration nanos range", valueType: eventfields.StoredValueTypeDuration, encoded: "0:1000000000"},
		{name: "duration overflow", valueType: eventfields.StoredValueTypeDuration, encoded: "9223372037:0"},
		{name: "decimal empty", valueType: eventfields.StoredValueTypeDecimal, encoded: ""},
		{name: "decimal no integer", valueType: eventfields.StoredValueTypeDecimal, encoded: ".1"},
		{name: "decimal special", valueType: eventfields.StoredValueTypeDecimal, encoded: "NaN"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := decodeFieldSummaryValue(test.valueType, test.encoded)
			if err == nil || !reflect.DeepEqual(got, canonicalFieldSummaryValue{}) {
				t.Fatalf("decodeFieldSummaryValue(%d, %q) = (%#v, %v), want zero and error", test.valueType, test.encoded, got, err)
			}
		})
	}
}

func TestSettingsForFieldSummaryClonesAndTightensEveryOutputCap(t *testing.T) {
	t.Parallel()
	base, err := querySettings(Config{
		MaxResultRows: 50_000, MaxResultBytes: 64 << 20, MaxRowsToGroupBy: 20_000,
	})
	if err != nil {
		t.Fatal(err)
	}
	before := cloneFieldSummarySettings(base)
	spec := validCompiledFieldSummary(5, 73, 256).Spec
	got, err := settingsForFieldSummary(base, spec)
	if err != nil {
		t.Fatal(err)
	}
	if got["max_result_rows"] != uint64(75) ||
		got["max_result_bytes"] != maximumFieldSummaryResultBytes ||
		got["max_rows_to_group_by"] != uint64(73) ||
		got["readonly"] != uint8(2) {
		t.Fatalf("field-summary settings = %#v", got)
	}
	if !reflect.DeepEqual(base, before) {
		t.Fatalf("base settings mutated: got %#v, want %#v", base, before)
	}
	for key, value := range before {
		if key == "max_result_rows" || key == "max_result_bytes" || key == "max_rows_to_group_by" {
			continue
		}
		if !reflect.DeepEqual(got[key], value) {
			t.Fatalf("setting %q changed from %#v to %#v", key, value, got[key])
		}
	}

	base["max_result_rows"] = uint64(7)
	base["max_result_bytes"] = uint64(1024)
	base["max_rows_to_group_by"] = uint64(5)
	strict, err := settingsForFieldSummary(base, spec)
	if err != nil {
		t.Fatal(err)
	}
	if strict["max_result_rows"] != uint64(7) ||
		strict["max_result_bytes"] != uint64(1024) ||
		strict["max_rows_to_group_by"] != uint64(5) {
		t.Fatalf("stricter base caps were raised: %#v", strict)
	}
}

func TestSettingsForFieldSummaryRejectsInvalidBaseAndSummarySettings(t *testing.T) {
	base, err := querySettings(Config{})
	if err != nil {
		t.Fatal(err)
	}
	spec := validCompiledFieldSummary(1, 1, 8).Spec
	for _, name := range []string{
		"max_execution_time",
		"max_memory_usage",
		"max_rows_to_read",
		"max_bytes_to_read",
		"max_result_rows",
		"max_result_bytes",
		"max_rows_to_group_by",
		"max_threads",
		"max_query_size",
	} {
		t.Run(name+" missing", func(t *testing.T) {
			malformed := cloneFieldSummarySettings(base)
			delete(malformed, name)
			if _, err := settingsForFieldSummary(malformed, spec); err == nil {
				t.Fatalf("missing %s unexpectedly accepted", name)
			}
		})
		t.Run(name+" zero", func(t *testing.T) {
			malformed := cloneFieldSummarySettings(base)
			malformed[name] = uint64(0)
			if _, err := settingsForFieldSummary(malformed, spec); err == nil {
				t.Fatalf("zero %s unexpectedly accepted", name)
			}
		})
		t.Run(name+" wrong type", func(t *testing.T) {
			malformed := cloneFieldSummarySettings(base)
			malformed[name] = "1"
			if _, err := settingsForFieldSummary(malformed, spec); err == nil {
				t.Fatalf("wrong-type %s unexpectedly accepted", name)
			}
		})
	}
	for _, name := range []string{
		"timeout_overflow_mode",
		"read_overflow_mode",
		"result_overflow_mode",
		"group_by_overflow_mode",
	} {
		t.Run(name, func(t *testing.T) {
			malformed := cloneFieldSummarySettings(base)
			malformed[name] = "break"
			if _, err := settingsForFieldSummary(malformed, spec); err == nil {
				t.Fatalf("unsafe %s unexpectedly accepted", name)
			}
		})
	}
	for _, malformed := range []clickhousedriver.Settings{nil, cloneFieldSummarySettings(base)} {
		if malformed != nil {
			malformed["readonly"] = uint8(1)
		}
		if _, err := settingsForFieldSummary(malformed, spec); err == nil {
			t.Fatalf("settingsForFieldSummary(%#v) unexpectedly succeeded", malformed)
		}
	}

	invalidSpecs := []clickhouse.FieldSummarySpec{
		{},
		{FieldName: "value", MaximumValues: 0, MaximumDistinctValues: 1, MaximumValueBytes: 1},
		{
			FieldName: "value", MaximumValues: clickhouse.MaximumFieldSummaryValues + 1,
			MaximumDistinctValues: clickhouse.MaximumFieldSummaryDistinctValues, MaximumValueBytes: 1,
		},
		{FieldName: "value", MaximumValues: 2, MaximumDistinctValues: 1, MaximumValueBytes: 1},
		{
			FieldName: "value", MaximumValues: 1,
			MaximumDistinctValues: clickhouse.MaximumFieldSummaryDistinctValues + 1, MaximumValueBytes: 1,
		},
		{FieldName: "value", MaximumValues: 1, MaximumDistinctValues: 1, MaximumValueBytes: 0},
		{
			FieldName: "value", MaximumValues: 1, MaximumDistinctValues: 1,
			MaximumValueBytes: clickhouse.MaximumFieldSummaryValueBytes + 1,
		},
	}
	for index, invalid := range invalidSpecs {
		if _, err := settingsForFieldSummary(base, invalid); err == nil {
			t.Fatalf("invalid spec %d unexpectedly accepted: %#v", index, invalid)
		}
	}
}

func TestSettingsForFieldSummaryIsRaceSafeForConcurrentReaders(t *testing.T) {
	base, err := querySettings(Config{})
	if err != nil {
		t.Fatal(err)
	}
	const workers = 32
	var wait sync.WaitGroup
	wait.Add(workers)
	for index := range workers {
		go func() {
			defer wait.Done()
			query := validCompiledFieldSummary(1, uint32(index+1), 8)
			got, err := settingsForFieldSummary(base, query.Spec)
			if err != nil {
				t.Errorf("settingsForFieldSummary() error = %v", err)
				return
			}
			if got["max_result_rows"] != uint64(index+3) ||
				got["max_rows_to_group_by"] != uint64(index+1) {
				t.Errorf("settingsForFieldSummary(%d) = %#v", index+1, got)
			}
		}()
	}
	wait.Wait()
}

func TestExecutorExecuteFieldSummaryValidatesStateAndCompiledContractBeforeQuery(t *testing.T) {
	var typedNilConnection *fakeQueryConnection
	valid := validCompiledFieldSummary(1, 1, 8)
	tests := []struct {
		name     string
		executor *Executor
		query    clickhouse.CompiledFieldSummary
	}{
		{name: "nil receiver", query: valid},
		{name: "nil connection", executor: &Executor{}, query: valid},
		{name: "typed nil connection", executor: &Executor{connection: typedNilConnection}, query: valid},
		{name: "blank SQL", executor: mustExecutor(t, &fakeQueryConnection{}), query: func() clickhouse.CompiledFieldSummary {
			query := valid
			query.SQL = " \n"
			return query
		}()},
		{name: "empty field", executor: mustExecutor(t, &fakeQueryConnection{}), query: func() clickhouse.CompiledFieldSummary {
			query := valid
			query.Spec.FieldName = ""
			return query
		}()},
		{name: "invalid UTF-8 field", executor: mustExecutor(t, &fakeQueryConnection{}), query: func() clickhouse.CompiledFieldSummary {
			query := valid
			query.Spec.FieldName = string([]byte{0xff})
			return query
		}()},
		{name: "oversized field", executor: mustExecutor(t, &fakeQueryConnection{}), query: func() clickhouse.CompiledFieldSummary {
			query := valid
			query.Spec.FieldName = strings.Repeat("x", eventfields.MaximumNormalizedFieldNameBytes+1)
			return query
		}()},
		{name: "zero maximum values", executor: mustExecutor(t, &fakeQueryConnection{}), query: func() clickhouse.CompiledFieldSummary {
			query := valid
			query.Spec.MaximumValues = 0
			return query
		}()},
		{name: "maximum exceeds distinct", executor: mustExecutor(t, &fakeQueryConnection{}), query: func() clickhouse.CompiledFieldSummary {
			query := valid
			query.Spec.MaximumValues = 2
			return query
		}()},
		{name: "zero value bytes", executor: mustExecutor(t, &fakeQueryConnection{}), query: func() clickhouse.CompiledFieldSummary {
			query := valid
			query.Spec.MaximumValueBytes = 0
			return query
		}()},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := test.executor.ExecuteFieldSummary(context.Background(), test.query)
			if err == nil || !reflect.DeepEqual(got, FieldSummaryResult{}) {
				t.Fatalf("ExecuteFieldSummary() = (%#v, %v), want zero and error", got, err)
			}
		})
	}

	t.Run("nil query ID generator", func(t *testing.T) {
		connection := &fakeQueryConnection{}
		executor := mustExecutor(t, connection)
		executor.newQueryID = nil
		got, err := executor.ExecuteFieldSummary(context.Background(), valid)
		if err == nil || !reflect.DeepEqual(got, FieldSummaryResult{}) || connection.query != "" {
			t.Fatalf("ExecuteFieldSummary() = (%#v, %v), query=%q", got, err, connection.query)
		}
	})
	for _, test := range []struct {
		name string
		fn   func() (string, error)
	}{
		{name: "empty query ID", fn: func() (string, error) { return "", nil }},
		{name: "query ID failure", fn: func() (string, error) { return "", io.ErrUnexpectedEOF }},
	} {
		t.Run(test.name, func(t *testing.T) {
			connection := &fakeQueryConnection{}
			executor := mustExecutor(t, connection)
			executor.newQueryID = test.fn
			got, err := executor.ExecuteFieldSummary(context.Background(), valid)
			if err == nil || !reflect.DeepEqual(got, FieldSummaryResult{}) || connection.query != "" {
				t.Fatalf("ExecuteFieldSummary() = (%#v, %v), query=%q", got, err, connection.query)
			}
		})
	}
}

func TestExecutorExecuteFieldSummaryHonorsContextAtEveryBoundary(t *testing.T) {
	valid := validCompiledFieldSummary(1, 1, 8)
	t.Run("nil", func(t *testing.T) {
		connection := &fakeQueryConnection{rows: fieldSummaryEmptyRows()}
		got, err := mustExecutor(t, connection).ExecuteFieldSummary(nil, valid)
		if err == nil || !reflect.DeepEqual(got, FieldSummaryResult{}) || connection.query != "" {
			t.Fatalf("ExecuteFieldSummary(nil) = (%#v, %v), query=%q", got, err, connection.query)
		}
	})
	t.Run("pre canceled", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		connection := &fakeQueryConnection{rows: fieldSummaryEmptyRows()}
		got, err := mustExecutor(t, connection).ExecuteFieldSummary(ctx, valid)
		assertFieldSummaryError(t, got, err, context.Canceled)
		if connection.query != "" {
			t.Fatalf("pre-canceled query issued: %q", connection.query)
		}
	})
	t.Run("canceled by query ID", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		connection := &fakeQueryConnection{rows: fieldSummaryEmptyRows()}
		executor := mustExecutor(t, connection)
		executor.newQueryID = func() (string, error) {
			cancel()
			return "field-summary-query", nil
		}
		got, err := executor.ExecuteFieldSummary(ctx, valid)
		assertFieldSummaryError(t, got, err, context.Canceled)
		if connection.query != "" {
			t.Fatalf("query issued after cancellation: %q", connection.query)
		}
	})
	t.Run("canceled after query", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		base := fieldSummaryEmptyRows()
		connection := &cancelingFieldSummaryConnection{rows: base, cancel: cancel}
		got, err := mustExecutor(t, connection).ExecuteFieldSummary(ctx, valid)
		assertFieldSummaryError(t, got, err, context.Canceled)
		if !base.closed {
			t.Fatal("rows were not closed after query cancellation")
		}
	})
	t.Run("canceled during scan", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		base := fieldSummaryEmptyRows()
		rows := &cancelAfterFieldSummaryScanRows{fakeRows: base, cancel: cancel}
		got, err := mustExecutor(t, &fakeQueryConnection{rows: rows}).ExecuteFieldSummary(ctx, valid)
		assertFieldSummaryError(t, got, err, context.Canceled)
		if !base.closed || rows.scanCalls != 1 {
			t.Fatalf("closed=%v scanCalls=%d", base.closed, rows.scanCalls)
		}
	})
	t.Run("canceled during close", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		base := fieldSummaryEmptyRows()
		rows := &cancelOnFieldSummaryCloseRows{fakeRows: base, cancel: cancel}
		got, err := mustExecutor(t, &fakeQueryConnection{rows: rows}).ExecuteFieldSummary(ctx, valid)
		assertFieldSummaryError(t, got, err, context.Canceled)
		if rows.closeCalls != 1 {
			t.Fatalf("closeCalls=%d, want 1", rows.closeCalls)
		}
	})
}

func TestExecutorExecuteFieldSummaryClassifiesDriverFailuresAtomically(t *testing.T) {
	tests := []struct {
		name       string
		connection queryConnection
	}{
		{name: "query", connection: &fakeQueryConnection{err: io.ErrUnexpectedEOF}},
		{
			name: "scan",
			connection: &fakeQueryConnection{
				rows: &fieldSummaryScanErrorRows{fakeRows: fieldSummaryEmptyRows()},
			},
		},
		{name: "iteration", connection: func() queryConnection {
			rows := fieldSummaryEmptyRows()
			rows.err = io.ErrUnexpectedEOF
			return &fakeQueryConnection{rows: rows}
		}()},
		{name: "close", connection: func() queryConnection {
			rows := fieldSummaryEmptyRows()
			rows.closeErr = io.ErrUnexpectedEOF
			return &fakeQueryConnection{rows: rows}
		}()},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := mustExecutor(t, test.connection).ExecuteFieldSummary(
				context.Background(),
				validCompiledFieldSummary(1, 1, 8),
			)
			assertFieldSummaryError(t, got, err, searchjobs.ErrStorageUnavailable)
		})
	}

	t.Run("resource exception", func(t *testing.T) {
		connection := &fakeQueryConnection{err: &clickhousedriver.Exception{Code: 241, Name: "MEMORY_LIMIT_EXCEEDED"}}
		got, err := mustExecutor(t, connection).ExecuteFieldSummary(
			context.Background(),
			validCompiledFieldSummary(1, 1, 8),
		)
		assertFieldSummaryError(t, got, err, searchjobs.ErrExecutionLimit)
	})
}

func TestExecutorExecuteFieldSummaryRejectsNilResultStreams(t *testing.T) {
	t.Parallel()
	var typedNilRows *fakeRows
	for _, rows := range []driver.Rows{nil, typedNilRows} {
		got, err := mustExecutor(t, &fakeQueryConnection{rows: rows}).ExecuteFieldSummary(
			context.Background(),
			validCompiledFieldSummary(1, 1, 8),
		)
		assertFieldSummaryError(t, got, err, searchjobs.ErrInvalidResult)
	}
}

func validCompiledFieldSummary(
	maximumValues uint32,
	maximumDistinctValues uint32,
	maximumValueBytes uint32,
) clickhouse.CompiledFieldSummary {
	return clickhouse.CompiledFieldSummary{
		SQL: "SELECT bounded_field_summary",
		Spec: clickhouse.FieldSummarySpec{
			FieldName:             "value",
			MaximumValues:         maximumValues,
			MaximumDistinctValues: maximumDistinctValues,
			MaximumValueBytes:     maximumValueBytes,
		},
		FieldKnown: true,
	}
}

func fieldSummaryFakeRows(rows ...[]any) *fakeRows {
	columns := []string{
		clickhouse.FieldSummaryRowKindColumn,
		clickhouse.FieldSummaryFieldNameColumn,
		clickhouse.FieldSummaryObservedTypesColumn,
		clickhouse.FieldSummaryEventCountColumn,
		clickhouse.FieldSummaryNullCountColumn,
		clickhouse.FieldSummaryMissingCountColumn,
		clickhouse.FieldSummaryTotalEventCountColumn,
		clickhouse.FieldSummaryValueTypeColumn,
		clickhouse.FieldSummaryEncodedValueColumn,
		clickhouse.FieldSummaryValueCountColumn,
		clickhouse.FieldSummaryMetadataInvalidColumn,
		clickhouse.FieldSummaryUnsupportedColumn,
		clickhouse.FieldSummaryOversizedColumn,
	}
	databaseTypes := []string{
		"UInt8",
		"String",
		"Array(UInt8)",
		"UInt64",
		"UInt64",
		"UInt64",
		"UInt64",
		"UInt8",
		"String",
		"UInt64",
		"UInt8",
		"UInt8",
		"UInt8",
	}
	scanTypes := []reflect.Type{
		reflect.TypeOf(uint8(0)),
		reflect.TypeOf(""),
		reflect.TypeOf([]uint8{}),
		reflect.TypeOf(uint64(0)),
		reflect.TypeOf(uint64(0)),
		reflect.TypeOf(uint64(0)),
		reflect.TypeOf(uint64(0)),
		reflect.TypeOf(uint8(0)),
		reflect.TypeOf(""),
		reflect.TypeOf(uint64(0)),
		reflect.TypeOf(uint8(0)),
		reflect.TypeOf(uint8(0)),
		reflect.TypeOf(uint8(0)),
	}
	columnTypes := make([]driver.ColumnType, len(columns))
	for index := range columns {
		columnTypes[index] = fakeColumnType{
			name: columns[index], databaseType: databaseTypes[index], scanType: scanTypes[index],
		}
	}
	return &fakeRows{columns: columns, types: columnTypes, data: rows}
}

func fieldSummaryEmptyRows() *fakeRows {
	return fieldSummaryFakeRows(fieldSummaryHeaderRow("value", nil, 0, 0, 0, 0))
}

func fieldSummaryHeaderRow(
	fieldName string,
	observedTypes []uint8,
	eventCount uint64,
	nullCount uint64,
	missingCount uint64,
	totalEventCount uint64,
) []any {
	return []any{
		uint8(0),
		fieldName,
		slices.Clone(observedTypes),
		eventCount,
		nullCount,
		missingCount,
		totalEventCount,
		uint8(0),
		"",
		uint64(0),
		uint8(0),
		uint8(0),
		uint8(0),
	}
}

func fieldSummaryGroup(
	valueType eventfields.StoredValueType,
	encodedValue string,
	count uint64,
) []any {
	return []any{
		uint8(1),
		"",
		[]uint8{},
		uint64(0),
		uint64(0),
		uint64(0),
		uint64(0),
		uint8(valueType),
		encodedValue,
		count,
		uint8(0),
		uint8(0),
		uint8(0),
	}
}

func fieldSummaryKinds(rows []FieldValueCountRow) []searchjobs.ValueKind {
	kinds := make([]searchjobs.ValueKind, len(rows))
	for index, row := range rows {
		kinds[index] = row.Value.Kind()
	}
	return kinds
}

func assertFieldSummaryError(t *testing.T, got FieldSummaryResult, err, want error) {
	t.Helper()
	if !errors.Is(err, want) || !reflect.DeepEqual(got, FieldSummaryResult{}) {
		t.Fatalf("ExecuteFieldSummary() = (%#v, %v), want zero result and %v", got, err, want)
	}
}

func cloneFieldSummarySettings(settings clickhousedriver.Settings) clickhousedriver.Settings {
	cloned := clickhousedriver.Settings{}
	for key, value := range settings {
		cloned[key] = value
	}
	return cloned
}

type cancelingFieldSummaryConnection struct {
	rows   driver.Rows
	cancel context.CancelFunc
}

func (connection *cancelingFieldSummaryConnection) Query(
	context.Context,
	string,
	...any,
) (driver.Rows, error) {
	connection.cancel()
	return connection.rows, nil
}

type cancelAfterFieldSummaryScanRows struct {
	*fakeRows
	cancel    context.CancelFunc
	scanCalls int
}

func (rows *cancelAfterFieldSummaryScanRows) Scan(destinations ...any) error {
	rows.scanCalls++
	err := rows.fakeRows.Scan(destinations...)
	rows.cancel()
	return err
}

type cancelOnFieldSummaryCloseRows struct {
	*fakeRows
	cancel     context.CancelFunc
	closeCalls int
}

func (rows *cancelOnFieldSummaryCloseRows) Close() error {
	rows.closeCalls++
	err := rows.fakeRows.Close()
	rows.cancel()
	return err
}

type fieldSummaryScanErrorRows struct {
	*fakeRows
}

func (rows *fieldSummaryScanErrorRows) Scan(...any) error {
	return io.ErrUnexpectedEOF
}
