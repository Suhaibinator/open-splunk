package queryexec

import (
	"context"
	"errors"
	"io"
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

func TestExecutorExecuteFieldCatalogReturnsCompleteCatalog(t *testing.T) {
	t.Parallel()
	rows := fieldCatalogFakeRows(10,
		fieldCatalogProfile("_time", []uint8{uint8(eventfields.StoredValueTypeTimestamp)}, 10, 0, 0, 10),
		fieldCatalogProfile("answer", []uint8{uint8(eventfields.StoredValueTypeNull), uint8(eventfields.StoredValueTypeSint64)}, 7, 2, 3, 10),
		fieldCatalogProfile("empty", nil, 0, 0, 10, 10),
	)
	connection := &fakeQueryConnection{rows: rows}
	query := validCompiledFieldCatalog(3)
	query.Args = []any{"tenant-a", uint64(44)}

	got, err := mustExecutor(t, connection).ExecuteFieldCatalog(context.Background(), query)
	if err != nil {
		t.Fatalf("ExecuteFieldCatalog() error = %v", err)
	}
	if !rows.closed {
		t.Fatal("field catalog rows were not closed")
	}
	if connection.query != query.SQL || !reflect.DeepEqual(connection.args, query.Args) {
		t.Fatalf("query/args = %q %#v, want %q %#v", connection.query, connection.args, query.SQL, query.Args)
	}
	want := FieldCatalogResult{
		TotalEvents: 10,
		Fields: []FieldProfileRow{
			{FieldName: "_time", ObservedTypes: []eventfields.StoredValueType{eventfields.StoredValueTypeTimestamp}, EventCount: 10},
			{FieldName: "answer", ObservedTypes: []eventfields.StoredValueType{eventfields.StoredValueTypeNull, eventfields.StoredValueTypeSint64}, EventCount: 7, NullCount: 2, MissingCount: 3},
			{FieldName: "empty", ObservedTypes: []eventfields.StoredValueType{}, MissingCount: 10},
		},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("ExecuteFieldCatalog() = %#v, want %#v", got, want)
	}
}

func TestExecutorExecuteFieldCatalogAcceptsEmptySearchAndCatalog(t *testing.T) {
	t.Parallel()
	rows := fieldCatalogFakeRows(0)
	got, err := mustExecutor(t, &fakeQueryConnection{rows: rows}).ExecuteFieldCatalog(context.Background(), validCompiledFieldCatalog(1))
	if err != nil {
		t.Fatal(err)
	}
	if got.TotalEvents != 0 || got.Fields == nil || len(got.Fields) != 0 {
		t.Fatalf("empty catalog = %#v, want non-nil empty fields", got)
	}
}

func TestExecutorExecuteFieldCatalogRejectsMalformedSchemaAtomically(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*fakeRows)
	}{
		{name: "wrong column", mutate: func(rows *fakeRows) { rows.columns[0] = "wrong" }},
		{name: "reordered columns", mutate: func(rows *fakeRows) { rows.columns[0], rows.columns[1] = rows.columns[1], rows.columns[0] }},
		{name: "extra column", mutate: func(rows *fakeRows) {
			rows.columns = append(rows.columns, "extra")
			rows.types = append(rows.types, fakeColumnType{name: "extra", databaseType: "UInt8", scanType: reflect.TypeOf(uint8(0))})
		}},
		{name: "missing column type", mutate: func(rows *fakeRows) { rows.types = rows.types[:7] }},
		{name: "typed nil column type", mutate: func(rows *fakeRows) {
			var columnType *fakeColumnType
			rows.types[2] = columnType
		}},
		{name: "type name mismatch", mutate: func(rows *fakeRows) {
			rows.types[1] = fakeColumnType{name: "wrong", databaseType: "String", scanType: reflect.TypeOf("")}
		}},
		{name: "nullable", mutate: func(rows *fakeRows) {
			rows.types[4] = fakeColumnType{name: clickhouse.FieldCatalogNullCountColumn, databaseType: "UInt64", scanType: reflect.TypeOf(uint64(0)), nullable: true}
		}},
		{name: "wrapped nullable", mutate: func(rows *fakeRows) {
			rows.types[4] = fakeColumnType{name: clickhouse.FieldCatalogNullCountColumn, databaseType: "Nullable(UInt64)", scanType: reflect.TypeOf(uint64(0))}
		}},
		{name: "wrong physical type", mutate: func(rows *fakeRows) {
			rows.types[2] = fakeColumnType{name: clickhouse.FieldCatalogObservedTypesColumn, databaseType: "Array(UInt16)", scanType: reflect.TypeOf([]uint16{})}
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			rows := fieldCatalogFakeRows(1, fieldCatalogProfile("a", []uint8{2}, 1, 0, 0, 1))
			test.mutate(rows)
			got, err := mustExecutor(t, &fakeQueryConnection{rows: rows}).ExecuteFieldCatalog(context.Background(), validCompiledFieldCatalog(1))
			assertFieldCatalogError(t, got, err, searchjobs.ErrInvalidResult)
			if !rows.closed {
				t.Fatal("rows were not closed after schema rejection")
			}
		})
	}
}

func TestExecutorExecuteFieldCatalogRejectsMalformedRowsAtomically(t *testing.T) {
	validProfile := func() []any { return fieldCatalogProfile("b", []uint8{2}, 3, 0, 2, 5) }
	tests := []struct {
		name   string
		mutate func(*fakeRows)
	}{
		{name: "missing header", mutate: func(rows *fakeRows) { rows.data = nil }},
		{name: "profile before header", mutate: func(rows *fakeRows) { rows.data = rows.data[1:] }},
		{name: "header name", mutate: func(rows *fakeRows) { rows.data[0][1] = "header" }},
		{name: "header types", mutate: func(rows *fakeRows) { rows.data[0][2] = []uint8{2} }},
		{name: "header event count", mutate: func(rows *fakeRows) { rows.data[0][3] = uint64(1) }},
		{name: "header null count", mutate: func(rows *fakeRows) { rows.data[0][4] = uint64(1) }},
		{name: "header missing count", mutate: func(rows *fakeRows) { rows.data[0][5] = uint64(1) }},
		{name: "header invalid flag", mutate: func(rows *fakeRows) { rows.data[0][7] = uint8(2) }},
		{name: "second header", mutate: func(rows *fakeRows) { rows.data[1][0] = uint8(0) }},
		{name: "unknown row kind", mutate: func(rows *fakeRows) { rows.data[1][0] = uint8(2) }},
		{name: "profile invalid flag", mutate: func(rows *fakeRows) { rows.data[1][7] = uint8(1) }},
		{name: "profile total mismatch", mutate: func(rows *fakeRows) { rows.data[1][6] = uint64(4) }},
		{name: "empty field name", mutate: func(rows *fakeRows) { rows.data[1][1] = "" }},
		{name: "invalid UTF-8 field name", mutate: func(rows *fakeRows) { rows.data[1][1] = string([]byte{0xff}) }},
		{name: "oversized field name", mutate: func(rows *fakeRows) { rows.data[1][1] = strings.Repeat("x", maximumFieldCatalogNameBytes+1) }},
		{name: "descending names", mutate: func(rows *fakeRows) {
			rows.data = append(rows.data, fieldCatalogProfile("a", []uint8{2}, 1, 0, 4, 5))
		}},
		{name: "duplicate names", mutate: func(rows *fakeRows) {
			rows.data = append(rows.data, fieldCatalogProfile("b", []uint8{2}, 1, 0, 4, 5))
		}},
		{name: "null exceeds event", mutate: func(rows *fakeRows) { rows.data[1][4] = uint64(4) }},
		{name: "event exceeds total", mutate: func(rows *fakeRows) { rows.data[1][3] = uint64(6) }},
		{name: "missing mismatch", mutate: func(rows *fakeRows) { rows.data[1][5] = uint64(1) }},
		{name: "empty types for present field", mutate: func(rows *fakeRows) { rows.data[1][2] = []uint8{} }},
		{name: "types for absent field", mutate: func(rows *fakeRows) {
			rows.data[1][2] = []uint8{2}
			rows.data[1][3] = uint64(0)
			rows.data[1][5] = uint64(5)
		}},
		{name: "unspecified type", mutate: func(rows *fakeRows) { rows.data[1][2] = []uint8{0} }},
		{name: "out of range type", mutate: func(rows *fakeRows) { rows.data[1][2] = []uint8{13} }},
		{name: "unsorted types", mutate: func(rows *fakeRows) { rows.data[1][2] = []uint8{3, 2} }},
		{name: "duplicate types", mutate: func(rows *fakeRows) { rows.data[1][2] = []uint8{2, 2} }},
		{name: "null type without nulls", mutate: func(rows *fakeRows) { rows.data[1][2] = []uint8{1, 2} }},
		{name: "nulls without null type", mutate: func(rows *fakeRows) {
			rows.data[1][4] = uint64(1)
			rows.data[1][2] = []uint8{2}
		}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			rows := fieldCatalogFakeRows(5, validProfile())
			test.mutate(rows)
			got, err := mustExecutor(t, &fakeQueryConnection{rows: rows}).ExecuteFieldCatalog(context.Background(), validCompiledFieldCatalog(3))
			assertFieldCatalogError(t, got, err, searchjobs.ErrInvalidResult)
			if !rows.closed {
				t.Fatal("rows were not closed after row rejection")
			}
		})
	}
}

func TestExecutorExecuteFieldCatalogDetectsOverflowAtomically(t *testing.T) {
	t.Parallel()
	profiles := [][]any{
		fieldCatalogProfile("a", []uint8{2}, 1, 0, 0, 1),
		fieldCatalogProfile("b", []uint8{2}, 1, 0, 0, 1),
		fieldCatalogProfile("c", []uint8{2}, 1, 0, 0, 1),
	}
	got, err := mustExecutor(t, &fakeQueryConnection{rows: fieldCatalogFakeRows(1, profiles...)}).ExecuteFieldCatalog(context.Background(), validCompiledFieldCatalog(2))
	assertFieldCatalogError(t, got, err, searchjobs.ErrExecutionLimit)

	profiles = append(profiles, fieldCatalogProfile("d", []uint8{2}, 1, 0, 0, 1))
	got, err = mustExecutor(t, &fakeQueryConnection{rows: fieldCatalogFakeRows(1, profiles...)}).ExecuteFieldCatalog(context.Background(), validCompiledFieldCatalog(2))
	assertFieldCatalogError(t, got, err, searchjobs.ErrInvalidResult)
}

func TestExecutorExecuteFieldCatalogReturnsMetadataUnavailableAtomically(t *testing.T) {
	t.Parallel()
	rows := fieldCatalogFakeRows(2,
		fieldCatalogProfile("legacy", []uint8{0, 255}, 2, 0, 0, 2),
	)
	rows.data[0][7] = uint8(1)
	got, err := mustExecutor(t, &fakeQueryConnection{rows: rows}).ExecuteFieldCatalog(context.Background(), validCompiledFieldCatalog(2))
	assertFieldCatalogError(t, got, err, ErrFieldMetadataUnavailable)
	if !rows.closed || rows.nextCalls != 2 {
		t.Fatalf("metadata-invalid stream closed=%v, nextCalls=%d; want fully consumed", rows.closed, rows.nextCalls)
	}
}

func TestExecutorExecuteFieldCatalogPrefersMetadataUnavailableThroughOverflowSentinel(t *testing.T) {
	t.Parallel()

	const maximumFields = uint32(2)
	profiles := [][]any{
		fieldCatalogProfile("a", []uint8{2}, 1, 0, 0, 1),
		fieldCatalogProfile("b", []uint8{2}, 1, 0, 0, 1),
		fieldCatalogProfile("c", []uint8{2}, 1, 0, 0, 1),
	}
	rows := fieldCatalogFakeRows(1, profiles...)
	rows.data[0][7] = uint8(1)
	got, err := mustExecutor(t, &fakeQueryConnection{rows: rows}).ExecuteFieldCatalog(
		context.Background(),
		validCompiledFieldCatalog(maximumFields),
	)
	assertFieldCatalogError(t, got, err, ErrFieldMetadataUnavailable)
	if !rows.closed || rows.nextCalls != len(profiles)+1 {
		t.Fatalf(
			"corrupt N+1 stream closed=%v nextCalls=%d, want complete atomic consumption of %d rows",
			rows.closed,
			rows.nextCalls,
			len(profiles)+1,
		)
	}
}

func TestExecutorExecuteFieldCatalogHonorsContextAtEveryBoundary(t *testing.T) {
	t.Run("nil", func(t *testing.T) {
		connection := &fakeQueryConnection{rows: fieldCatalogFakeRows(0)}
		//nolint:staticcheck // This case explicitly verifies the nil-context guard.
		got, err := mustExecutor(t, connection).ExecuteFieldCatalog(nil, validCompiledFieldCatalog(1))
		if err == nil || !reflect.DeepEqual(got, FieldCatalogResult{}) || connection.query != "" {
			t.Fatalf("ExecuteFieldCatalog(nil) = (%#v, %v), query=%q", got, err, connection.query)
		}
	})
	t.Run("pre canceled", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		connection := &fakeQueryConnection{rows: fieldCatalogFakeRows(0)}
		got, err := mustExecutor(t, connection).ExecuteFieldCatalog(ctx, validCompiledFieldCatalog(1))
		assertFieldCatalogError(t, got, err, context.Canceled)
		if connection.query != "" {
			t.Fatalf("pre-canceled query issued: %q", connection.query)
		}
	})
	t.Run("canceled by query ID", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		connection := &fakeQueryConnection{rows: fieldCatalogFakeRows(0)}
		executor := mustExecutor(t, connection)
		executor.newQueryID = func() (string, error) { cancel(); return "field-query", nil }
		got, err := executor.ExecuteFieldCatalog(ctx, validCompiledFieldCatalog(1))
		assertFieldCatalogError(t, got, err, context.Canceled)
		if connection.query != "" {
			t.Fatalf("query issued after cancellation: %q", connection.query)
		}
	})
	t.Run("canceled after query", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		base := fieldCatalogFakeRows(0)
		connection := &cancelingFieldCatalogConnection{rows: base, cancel: cancel}
		got, err := mustExecutor(t, connection).ExecuteFieldCatalog(ctx, validCompiledFieldCatalog(1))
		assertFieldCatalogError(t, got, err, context.Canceled)
		if !base.closed {
			t.Fatal("rows were not closed after query cancellation")
		}
	})
	t.Run("canceled during scan", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		base := fieldCatalogFakeRows(0)
		rows := &cancelAfterTimelineScanRows{fakeRows: base, cancel: cancel}
		got, err := mustExecutor(t, &fakeQueryConnection{rows: rows}).ExecuteFieldCatalog(ctx, validCompiledFieldCatalog(1))
		assertFieldCatalogError(t, got, err, context.Canceled)
		if !base.closed || rows.scanCalls != 1 {
			t.Fatalf("closed=%v scanCalls=%d", base.closed, rows.scanCalls)
		}
	})
	t.Run("canceled during close", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		base := fieldCatalogFakeRows(0)
		rows := &cancelOnTimelineCloseRows{fakeRows: base, cancel: cancel}
		got, err := mustExecutor(t, &fakeQueryConnection{rows: rows}).ExecuteFieldCatalog(ctx, validCompiledFieldCatalog(1))
		assertFieldCatalogError(t, got, err, context.Canceled)
		if rows.closeCalls != 1 {
			t.Fatalf("closeCalls=%d, want 1", rows.closeCalls)
		}
	})
}

func TestExecutorExecuteFieldCatalogClassifiesDriverFailuresAtomically(t *testing.T) {
	tests := []struct {
		name       string
		connection queryConnection
	}{
		{name: "query", connection: &fakeQueryConnection{err: io.ErrUnexpectedEOF}},
		{name: "scan", connection: &fakeQueryConnection{rows: &timelineScanErrorRows{fakeRows: fieldCatalogFakeRows(0)}}},
		{name: "iteration", connection: func() queryConnection {
			rows := fieldCatalogFakeRows(0)
			rows.err = io.ErrUnexpectedEOF
			return &fakeQueryConnection{rows: rows}
		}()},
		{name: "close", connection: func() queryConnection {
			rows := fieldCatalogFakeRows(0)
			rows.closeErr = io.ErrUnexpectedEOF
			return &fakeQueryConnection{rows: rows}
		}()},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := mustExecutor(t, test.connection).ExecuteFieldCatalog(context.Background(), validCompiledFieldCatalog(1))
			assertFieldCatalogError(t, got, err, searchjobs.ErrStorageUnavailable)
		})
	}
}

func TestExecutorExecuteFieldCatalogValidatesStateAndCompiledQueryBeforeExecution(t *testing.T) {
	var typedNilConnection *fakeQueryConnection
	tests := []struct {
		name     string
		executor *Executor
		query    clickhouse.CompiledFieldCatalog
	}{
		{name: "nil receiver", query: validCompiledFieldCatalog(1)},
		{name: "nil connection", executor: &Executor{}, query: validCompiledFieldCatalog(1)},
		{name: "typed nil connection", executor: &Executor{connection: typedNilConnection}, query: validCompiledFieldCatalog(1)},
		{name: "blank SQL", executor: mustExecutor(t, &fakeQueryConnection{rows: fieldCatalogFakeRows(0)}), query: clickhouse.CompiledFieldCatalog{SQL: " \n", Spec: clickhouse.FieldCatalogSpec{MaximumFields: 1}}},
		{name: "zero maximum", executor: mustExecutor(t, &fakeQueryConnection{rows: fieldCatalogFakeRows(0)}), query: validCompiledFieldCatalog(0)},
		{name: "oversized maximum", executor: mustExecutor(t, &fakeQueryConnection{rows: fieldCatalogFakeRows(0)}), query: validCompiledFieldCatalog(clickhouse.MaximumFieldCatalogFields + 1)},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := test.executor.ExecuteFieldCatalog(context.Background(), test.query)
			if err == nil || !reflect.DeepEqual(got, FieldCatalogResult{}) {
				t.Fatalf("ExecuteFieldCatalog() = (%#v, %v), want zero and error", got, err)
			}
		})
	}

	t.Run("nil query ID generator", func(t *testing.T) {
		executor := mustExecutor(t, &fakeQueryConnection{rows: fieldCatalogFakeRows(0)})
		executor.newQueryID = nil
		got, err := executor.ExecuteFieldCatalog(context.Background(), validCompiledFieldCatalog(1))
		if err == nil || !reflect.DeepEqual(got, FieldCatalogResult{}) {
			t.Fatalf("ExecuteFieldCatalog() = (%#v, %v)", got, err)
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
			connection := &fakeQueryConnection{rows: fieldCatalogFakeRows(0)}
			executor := mustExecutor(t, connection)
			executor.newQueryID = test.fn
			got, err := executor.ExecuteFieldCatalog(context.Background(), validCompiledFieldCatalog(1))
			if err == nil || !reflect.DeepEqual(got, FieldCatalogResult{}) || connection.query != "" {
				t.Fatalf("ExecuteFieldCatalog() = (%#v, %v), query=%q", got, err, connection.query)
			}
		})
	}
}

func TestSettingsForFieldCatalogClonesAndTightensEveryResourceCap(t *testing.T) {
	t.Parallel()
	base, err := querySettings(Config{
		MaxExecutionTime: 2 * maximumFieldCatalogExecutionTime,
		MaxMemoryBytes:   2 * maximumFieldCatalogMemoryBytes,
		MaxRowsToRead:    2 * maximumFieldCatalogRowsToRead,
		MaxBytesToRead:   2 * maximumFieldCatalogBytesToRead,
		MaxResultRows:    50_000,
		MaxResultBytes:   64 << 20,
		MaxRowsToGroupBy: 20_000,
		MaxThreads:       8,
	})
	if err != nil {
		t.Fatal(err)
	}
	base["use_query_cache"] = uint8(1)
	before := clickhousedriver.Settings{}
	for key, value := range base {
		before[key] = value
	}
	got, err := settingsForFieldCatalog(base, 73, 0)
	if err != nil {
		t.Fatal(err)
	}
	if got["max_execution_time"] != uint64(maximumFieldCatalogExecutionTime/time.Second) ||
		got["max_memory_usage"] != maximumFieldCatalogMemoryBytes ||
		got["max_rows_to_read"] != maximumFieldCatalogRowsToRead ||
		got["max_bytes_to_read"] != maximumFieldCatalogBytesToRead ||
		got["max_result_rows"] != uint64(75) ||
		got["max_result_bytes"] != maximumFieldCatalogBytes ||
		got["max_rows_to_group_by"] != uint64(75) ||
		got["max_threads"] != maximumFieldCatalogThreads ||
		got["use_query_cache"] != uint8(0) ||
		got["readonly"] != uint8(2) {
		t.Fatalf("field catalog settings = %#v", got)
	}
	if !reflect.DeepEqual(base, before) {
		t.Fatalf("base settings mutated: got %#v, want %#v", base, before)
	}
	if maximumFieldCatalogMemoryBytes != uint64(384<<20) ||
		MaximumFieldCatalogMemoryBytes != uint64(448<<20) {
		t.Fatalf(
			"field catalog memory tiers = standard %d maximum %d, want %d/%d",
			maximumFieldCatalogMemoryBytes,
			MaximumFieldCatalogMemoryBytes,
			uint64(384<<20),
			uint64(448<<20),
		)
	}
	for _, test := range []struct {
		name                     string
		knowledgeGeneratedFields uint32
		wantMemory               uint64
	}{
		{name: "empty knowledge", knowledgeGeneratedFields: 0, wantMemory: maximumFieldCatalogMemoryBytes},
		{name: "standard boundary", knowledgeGeneratedFields: 13, wantMemory: maximumFieldCatalogMemoryBytes},
		{name: "elevated start", knowledgeGeneratedFields: 14, wantMemory: MaximumFieldCatalogMemoryBytes},
		{name: "physical boundary", knowledgeGeneratedFields: clickhouse.MaximumClickHouseKnowledgeGeneratedFields, wantMemory: MaximumFieldCatalogMemoryBytes},
	} {
		t.Run(test.name, func(t *testing.T) {
			settings, err := settingsForFieldCatalog(base, 73, test.knowledgeGeneratedFields)
			if err != nil {
				t.Fatal(err)
			}
			if settings["max_memory_usage"] != test.wantMemory {
				t.Fatalf("max_memory_usage = %#v, want %d", settings["max_memory_usage"], test.wantMemory)
			}
		})
	}
	tightened := map[string]struct{}{
		"max_execution_time": {}, "max_memory_usage": {}, "max_rows_to_read": {},
		"max_bytes_to_read": {}, "max_result_rows": {}, "max_result_bytes": {},
		"max_rows_to_group_by": {}, "max_threads": {}, "use_query_cache": {},
	}
	for key, value := range before {
		if _, changed := tightened[key]; changed {
			continue
		}
		if !reflect.DeepEqual(got[key], value) {
			t.Fatalf("setting %q changed from %#v to %#v", key, value, got[key])
		}
	}

	base["max_execution_time"] = uint64(3)
	base["max_memory_usage"] = uint64(64 << 20)
	base["max_rows_to_read"] = uint64(1_000)
	base["max_bytes_to_read"] = uint64(2 << 20)
	base["max_result_rows"] = uint64(7)
	base["max_result_bytes"] = uint64(1024)
	base["max_rows_to_group_by"] = uint64(5)
	base["max_threads"] = uint64(1)
	strict, err := settingsForFieldCatalog(base, 73, clickhouse.MaximumClickHouseKnowledgeGeneratedFields)
	if err != nil {
		t.Fatal(err)
	}
	if strict["max_execution_time"] != uint64(3) ||
		strict["max_memory_usage"] != uint64(64<<20) ||
		strict["max_rows_to_read"] != uint64(1_000) ||
		strict["max_bytes_to_read"] != uint64(2<<20) ||
		strict["max_result_rows"] != uint64(7) ||
		strict["max_result_bytes"] != uint64(1024) ||
		strict["max_rows_to_group_by"] != uint64(5) ||
		strict["max_threads"] != uint64(1) {
		t.Fatalf("stricter base caps were raised: %#v", strict)
	}
}

func TestSettingsForFieldCatalogRejectsInvalidBaseSettings(t *testing.T) {
	base, err := querySettings(Config{})
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{
		"max_execution_time", "max_memory_usage", "max_rows_to_read", "max_bytes_to_read",
		"max_result_rows", "max_result_bytes", "max_rows_to_group_by", "max_threads", "max_query_size",
		"max_subquery_depth",
	} {
		t.Run(name+" missing", func(t *testing.T) {
			malformed := cloneFieldCatalogSettings(base)
			delete(malformed, name)
			if _, err := settingsForFieldCatalog(malformed, 1, 0); err == nil {
				t.Fatalf("missing %s unexpectedly accepted", name)
			}
		})
		t.Run(name+" zero", func(t *testing.T) {
			malformed := cloneFieldCatalogSettings(base)
			malformed[name] = uint64(0)
			if _, err := settingsForFieldCatalog(malformed, 1, 0); err == nil {
				t.Fatalf("zero %s unexpectedly accepted", name)
			}
		})
		t.Run(name+" wrong type", func(t *testing.T) {
			malformed := cloneFieldCatalogSettings(base)
			malformed[name] = "1"
			if _, err := settingsForFieldCatalog(malformed, 1, 0); err == nil {
				t.Fatalf("wrong type %s unexpectedly accepted", name)
			}
		})
	}
	for _, name := range []string{"timeout_overflow_mode", "read_overflow_mode", "result_overflow_mode", "group_by_overflow_mode"} {
		t.Run(name, func(t *testing.T) {
			malformed := cloneFieldCatalogSettings(base)
			malformed[name] = "break"
			if _, err := settingsForFieldCatalog(malformed, 1, 0); err == nil {
				t.Fatalf("unsafe %s unexpectedly accepted", name)
			}
		})
	}
	for _, test := range []struct {
		name  string
		value any
	}{
		{name: "enable_materialized_cte", value: uint8(0)},
		{name: "short_circuit_function_evaluation", value: "disable"},
		{name: "async_insert", value: uint8(1)},
	} {
		t.Run(test.name, func(t *testing.T) {
			malformed := cloneFieldCatalogSettings(base)
			malformed[test.name] = test.value
			if _, err := settingsForFieldCatalog(malformed, 1, 0); err == nil {
				t.Fatalf("unsafe %s unexpectedly accepted", test.name)
			}
		})
	}
	for _, malformed := range []clickhousedriver.Settings{nil, cloneFieldCatalogSettings(base)} {
		if malformed != nil {
			malformed["readonly"] = uint8(1)
		}
		if _, err := settingsForFieldCatalog(malformed, 1, 0); err == nil {
			t.Fatalf("settingsForFieldCatalog(%#v) unexpectedly succeeded", malformed)
		}
	}
	if _, err := settingsForFieldCatalog(base, 0, 0); err == nil {
		t.Fatal("zero maximum fields unexpectedly accepted")
	}
	if _, err := settingsForFieldCatalog(base, clickhouse.MaximumFieldCatalogFields+1, 0); err == nil {
		t.Fatal("oversized maximum fields unexpectedly accepted")
	}
	if _, err := settingsForFieldCatalog(
		base,
		1,
		clickhouse.MaximumClickHouseKnowledgeGeneratedFields+1,
	); err == nil {
		t.Fatal("oversized generated-field resource evidence unexpectedly accepted")
	}
}

func TestSettingsForFieldCatalogIsRaceSafeForConcurrentReaders(t *testing.T) {
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
			knowledgeGeneratedFields := uint32(index % 17)
			got, err := settingsForFieldCatalog(base, uint32(index+1), knowledgeGeneratedFields)
			if err != nil {
				t.Errorf("settingsForFieldCatalog() error = %v", err)
				return
			}
			wantMemory := maximumFieldCatalogMemoryBytes
			if knowledgeGeneratedFields > maximumStandardFieldCatalogKnowledgeFields {
				wantMemory = MaximumFieldCatalogMemoryBytes
			}
			if got["max_result_rows"] != uint64(index+3) || got["max_rows_to_group_by"] != uint64(index+3) {
				t.Errorf("settingsForFieldCatalog(%d) = %#v", index+1, got)
			}
			if got["max_memory_usage"] != wantMemory {
				t.Errorf("settingsForFieldCatalog(%d) memory = %#v, want %d", index+1, got["max_memory_usage"], wantMemory)
			}
		}()
	}
	wait.Wait()
}

func validCompiledFieldCatalog(maximum uint32) clickhouse.CompiledFieldCatalog {
	return clickhouse.CompiledFieldCatalog{
		SQL:  "SELECT bounded_field_catalog",
		Spec: clickhouse.FieldCatalogSpec{MaximumFields: maximum},
	}
}

func fieldCatalogFakeRows(totalEvents uint64, profiles ...[]any) *fakeRows {
	data := make([][]any, 0, len(profiles)+1)
	data = append(data, []any{uint8(0), "", []uint8{}, uint64(0), uint64(0), uint64(0), totalEvents, uint8(0)})
	data = append(data, profiles...)
	columns := []string{
		clickhouse.FieldCatalogRowKindColumn,
		clickhouse.FieldCatalogNameColumn,
		clickhouse.FieldCatalogObservedTypesColumn,
		clickhouse.FieldCatalogEventCountColumn,
		clickhouse.FieldCatalogNullCountColumn,
		clickhouse.FieldCatalogMissingCountColumn,
		clickhouse.FieldCatalogTotalEventsColumn,
		clickhouse.FieldCatalogInvalidColumn,
	}
	databaseTypes := []string{"UInt8", "String", "Array(UInt8)", "UInt64", "UInt64", "UInt64", "UInt64", "UInt8"}
	scanTypes := []reflect.Type{
		reflect.TypeOf(uint8(0)), reflect.TypeOf(""), reflect.TypeOf([]uint8{}),
		reflect.TypeOf(uint64(0)), reflect.TypeOf(uint64(0)), reflect.TypeOf(uint64(0)), reflect.TypeOf(uint64(0)),
		reflect.TypeOf(uint8(0)),
	}
	types := make([]driver.ColumnType, len(columns))
	for index := range columns {
		types[index] = fakeColumnType{name: columns[index], databaseType: databaseTypes[index], scanType: scanTypes[index]}
	}
	return &fakeRows{columns: columns, types: types, data: data}
}

func fieldCatalogProfile(name string, types []uint8, eventCount, nullCount, missingCount, totalEvents uint64) []any {
	return []any{uint8(1), name, slices.Clone(types), eventCount, nullCount, missingCount, totalEvents, uint8(0)}
}

func assertFieldCatalogError(t *testing.T, got FieldCatalogResult, err, want error) {
	t.Helper()
	if !errors.Is(err, want) || !reflect.DeepEqual(got, FieldCatalogResult{}) {
		t.Fatalf("ExecuteFieldCatalog() = (%#v, %v), want zero result and %v", got, err, want)
	}
}

func cloneFieldCatalogSettings(settings clickhousedriver.Settings) clickhousedriver.Settings {
	cloned := clickhousedriver.Settings{}
	for key, value := range settings {
		cloned[key] = value
	}
	return cloned
}

type cancelingFieldCatalogConnection struct {
	rows   driver.Rows
	cancel context.CancelFunc
}

func (connection *cancelingFieldCatalogConnection) Query(context.Context, string, ...any) (driver.Rows, error) {
	connection.cancel()
	return connection.rows, nil
}
