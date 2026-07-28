package queryexec

import (
	"context"
	"errors"
	"io"
	"reflect"
	"strings"
	"sync"
	"testing"

	clickhousedriver "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
	"github.com/Suhaibinator/open-splunk/internal/eventfields"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
)

func TestExecutorExecuteFieldSuggestionsReturnsOnlyCompleteNames(t *testing.T) {
	t.Parallel()

	rows := fieldSuggestionFakeRows(0, "status", "status_code")
	connection := &fakeQueryConnection{rows: rows}
	query := validCompiledFieldSuggestions("sta", 2)
	query.Args = []any{"tenant-a", "sta", uint64(3)}

	got, err := mustExecutor(t, connection).ExecuteFieldSuggestions(context.Background(), query)
	if err != nil {
		t.Fatalf("ExecuteFieldSuggestions() error = %v", err)
	}
	if !rows.closed {
		t.Fatal("field suggestion rows were not closed")
	}
	if connection.query != query.SQL || !reflect.DeepEqual(connection.args, query.Args) {
		t.Fatalf("query/args = %q %#v, want %q %#v", connection.query, connection.args, query.SQL, query.Args)
	}
	want := FieldSuggestionResult{FieldNames: []string{"status", "status_code"}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("ExecuteFieldSuggestions() = %#v, want %#v", got, want)
	}
}

func TestExecutorExecuteFieldSuggestionsAcceptsEmptyResult(t *testing.T) {
	t.Parallel()

	got, err := mustExecutor(
		t,
		&fakeQueryConnection{rows: fieldSuggestionFakeRows(0)},
	).ExecuteFieldSuggestions(context.Background(), validCompiledFieldSuggestions("missing", 1))
	if err != nil {
		t.Fatal(err)
	}
	if got.Truncated || got.FieldNames == nil || len(got.FieldNames) != 0 {
		t.Fatalf("empty suggestions = %#v, want non-nil empty names", got)
	}
}

func TestExecutorExecuteFieldSuggestionsAcceptsCanonicalEscapes(t *testing.T) {
	t.Parallel()

	got, err := mustExecutor(
		t,
		&fakeQueryConnection{rows: fieldSuggestionFakeRows(0, `a\.b`, `a\\b`)},
	).ExecuteFieldSuggestions(context.Background(), validCompiledFieldSuggestions("a", 2))
	if err != nil {
		t.Fatal(err)
	}
	want := FieldSuggestionResult{FieldNames: []string{`a\.b`, `a\\b`}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("escaped suggestions = %#v, want %#v", got, want)
	}
}

func TestExecutorExecuteFieldSuggestionsAcceptsSeventeenSegmentQueryField(t *testing.T) {
	t.Parallel()

	fieldName := strings.Repeat("segment.", 16) + "leaf"
	got, err := mustExecutor(
		t,
		&fakeQueryConnection{rows: fieldSuggestionFakeRows(0, fieldName)},
	).ExecuteFieldSuggestions(
		context.Background(),
		validCompiledFieldSuggestions("segment.", 1),
	)
	if err != nil {
		t.Fatal(err)
	}
	want := FieldSuggestionResult{FieldNames: []string{fieldName}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("seventeen-segment suggestions = %#v, want %#v", got, want)
	}
}

func TestExecutorExecuteFieldSuggestionsAcceptsVisibleParentAndChild(t *testing.T) {
	t.Parallel()

	got, err := mustExecutor(
		t,
		&fakeQueryConnection{rows: fieldSuggestionFakeRows(
			0,
			"edge_obj",
			"edge_obj.child",
		)},
	).ExecuteFieldSuggestions(
		context.Background(),
		validCompiledFieldSuggestions("edge_", 2),
	)
	if err != nil {
		t.Fatal(err)
	}
	want := FieldSuggestionResult{FieldNames: []string{"edge_obj", "edge_obj.child"}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("parent/child suggestions = %#v, want %#v", got, want)
	}
}

func TestExecutorExecuteFieldSuggestionsAcceptsASCIIFoldedExactOrder(t *testing.T) {
	t.Parallel()

	got, err := mustExecutor(
		t,
		&fakeQueryConnection{rows: fieldSuggestionFakeRows(
			0,
			"mixaardvark",
			"mixAlpha",
			"mixalpha",
			"mixValid",
		)},
	).ExecuteFieldSuggestions(
		context.Background(),
		validCompiledFieldSuggestions("mix", 4),
	)
	if err != nil {
		t.Fatal(err)
	}
	want := FieldSuggestionResult{FieldNames: []string{
		"mixaardvark",
		"mixAlpha",
		"mixalpha",
		"mixValid",
	}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("mixed-case suggestions = %#v, want %#v", got, want)
	}
}

func TestExecutorExecuteFieldSuggestionsUsesOverflowSentinelForTruncation(t *testing.T) {
	t.Parallel()

	got, err := mustExecutor(
		t,
		&fakeQueryConnection{rows: fieldSuggestionFakeRows(0, "a", "b", "c")},
	).ExecuteFieldSuggestions(context.Background(), validCompiledFieldSuggestions("", 2))
	if err != nil {
		t.Fatal(err)
	}
	want := FieldSuggestionResult{FieldNames: []string{"a", "b"}, Truncated: true}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("truncated suggestions = %#v, want %#v", got, want)
	}

	result, err := mustExecutor(
		t,
		&fakeQueryConnection{rows: fieldSuggestionFakeRows(0, "a", "b", "c", "d")},
	).ExecuteFieldSuggestions(context.Background(), validCompiledFieldSuggestions("", 2))
	assertFieldSuggestionError(t, result, err, searchjobs.ErrInvalidResult)
}

func TestExecutorExecuteFieldSuggestionsRejectsMalformedSchemaAtomically(t *testing.T) {
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
			rows.types = append(rows.types, fakeColumnType{
				name: "extra", databaseType: "UInt8", scanType: reflect.TypeOf(uint8(0)),
			})
		}},
		{name: "missing column type", mutate: func(rows *fakeRows) { rows.types = rows.types[:2] }},
		{name: "typed nil column type", mutate: func(rows *fakeRows) {
			var columnType *fakeColumnType
			rows.types[1] = columnType
		}},
		{name: "type name mismatch", mutate: func(rows *fakeRows) {
			rows.types[1] = fakeColumnType{
				name: "wrong", databaseType: "String", scanType: reflect.TypeOf(""),
			}
		}},
		{name: "nullable", mutate: func(rows *fakeRows) {
			rows.types[2] = fakeColumnType{
				name: clickhouse.FieldSuggestionInvalidColumn, databaseType: "UInt8",
				scanType: reflect.TypeOf(uint8(0)), nullable: true,
			}
		}},
		{name: "wrapped nullable", mutate: func(rows *fakeRows) {
			rows.types[2] = fakeColumnType{
				name: clickhouse.FieldSuggestionInvalidColumn, databaseType: "Nullable(UInt8)",
				scanType: reflect.TypeOf(uint8(0)),
			}
		}},
		{name: "wrong physical type", mutate: func(rows *fakeRows) {
			rows.types[1] = fakeColumnType{
				name: clickhouse.FieldSuggestionNameColumn, databaseType: "FixedString(8)",
				scanType: reflect.TypeOf(""),
			}
		}},
		{name: "wrong scan type", mutate: func(rows *fakeRows) {
			rows.types[1] = fakeColumnType{
				name: clickhouse.FieldSuggestionNameColumn, databaseType: "String",
				scanType: reflect.TypeOf([]byte{}),
			}
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			rows := fieldSuggestionFakeRows(0, "a")
			test.mutate(rows)
			got, err := mustExecutor(
				t,
				&fakeQueryConnection{rows: rows},
			).ExecuteFieldSuggestions(context.Background(), validCompiledFieldSuggestions("", 1))
			assertFieldSuggestionError(t, got, err, searchjobs.ErrInvalidResult)
			if !rows.closed {
				t.Fatal("rows were not closed after schema rejection")
			}
		})
	}
}

func TestExecutorExecuteFieldSuggestionsRejectsMalformedRowsAtomically(t *testing.T) {
	tests := []struct {
		name   string
		prefix string
		mutate func(*fakeRows)
	}{
		{name: "missing header", mutate: func(rows *fakeRows) { rows.data = nil }},
		{name: "name before header", mutate: func(rows *fakeRows) { rows.data = rows.data[1:] }},
		{name: "header name", mutate: func(rows *fakeRows) { rows.data[0][1] = "header" }},
		{name: "header invalid flag", mutate: func(rows *fakeRows) { rows.data[0][2] = uint8(2) }},
		{name: "second header", mutate: func(rows *fakeRows) { rows.data[1][0] = uint8(0) }},
		{name: "unknown row kind", mutate: func(rows *fakeRows) { rows.data[1][0] = uint8(2) }},
		{name: "name invalid flag", mutate: func(rows *fakeRows) { rows.data[1][2] = uint8(1) }},
		{name: "empty field name", mutate: func(rows *fakeRows) { rows.data[1][1] = "" }},
		{name: "invalid UTF-8 field name", mutate: func(rows *fakeRows) {
			rows.data[1][1] = string([]byte{0xff})
		}},
		{name: "NUL field name", mutate: func(rows *fakeRows) {
			rows.data[1][1] = "sta\x00tus"
		}},
		{name: "control field name", mutate: func(rows *fakeRows) {
			rows.data[1][1] = "sta\x01tus"
		}},
		{name: "C1 control field name", mutate: func(rows *fakeRows) {
			rows.data[1][1] = "sta\u0085tus"
		}},
		{name: "invalid escape", mutate: func(rows *fakeRows) {
			rows.data[1][1] = `a\q`
		}},
		{name: "trailing escape", mutate: func(rows *fakeRows) {
			rows.data[1][1] = `a\`
		}},
		{name: "empty path segment", mutate: func(rows *fakeRows) {
			rows.data[1][1] = "a..b"
		}},
		{name: "oversized path segment", mutate: func(rows *fakeRows) {
			rows.data[1][1] = strings.Repeat(
				"x",
				eventfields.MaximumDynamicPathSegmentBytes+1,
			)
		}},
		{name: "too many query path segments", mutate: func(rows *fakeRows) {
			rows.data[1][1] = strings.Repeat(
				"a.",
				eventfields.MaximumDynamicPathSegments+1,
			) + "a"
		}},
		{name: "oversized field name", mutate: func(rows *fakeRows) {
			rows.data[1][1] = strings.Repeat("x", eventfields.MaximumNormalizedFieldNameBytes+1)
		}},
		{name: "leading plus", mutate: func(rows *fakeRows) { rows.data[1][1] = "+field" }},
		{name: "leading minus", mutate: func(rows *fakeRows) { rows.data[1][1] = "-field" }},
		{name: "whitespace", mutate: func(rows *fakeRows) { rows.data[1][1] = "bad field" }},
		{name: "operator delimiter", mutate: func(rows *fakeRows) { rows.data[1][1] = "bad|field" }},
		{name: "wildcard", mutate: func(rows *fakeRows) { rows.data[1][1] = "bad*field" }},
		{name: "private namespace", mutate: func(rows *fakeRows) { rows.data[1][1] = "__OS_private" }},
		{name: "Unicode format", mutate: func(rows *fakeRows) { rows.data[1][1] = "bad\u200bfield" }},
		{name: "Unicode private use", mutate: func(rows *fakeRows) { rows.data[1][1] = "bad\uE000field" }},
		{name: "descending names", mutate: func(rows *fakeRows) {
			rows.data = append(rows.data, fieldSuggestionName("a"))
		}},
		{name: "bytewise ascending but folded descending", mutate: func(rows *fakeRows) {
			rows.data[1][1] = "hoZ"
			rows.data = append(rows.data, fieldSuggestionName("hoa"))
		}},
		{name: "duplicate names", mutate: func(rows *fakeRows) {
			rows.data = append(rows.data, fieldSuggestionName("b"))
		}},
		{name: "prefix mismatch", prefix: "B", mutate: func(*fakeRows) {}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			rows := fieldSuggestionFakeRows(0, "b")
			test.mutate(rows)
			got, err := mustExecutor(
				t,
				&fakeQueryConnection{rows: rows},
			).ExecuteFieldSuggestions(
				context.Background(),
				validCompiledFieldSuggestions(test.prefix, 3),
			)
			assertFieldSuggestionError(t, got, err, searchjobs.ErrInvalidResult)
			if !rows.closed {
				t.Fatal("rows were not closed after row rejection")
			}
		})
	}
}

func TestExecutorExecuteFieldSuggestionsReturnsMetadataUnavailableAtomically(t *testing.T) {
	t.Parallel()

	rows := fieldSuggestionFakeRows(1, string([]byte{0xff}), "ignored")
	got, err := mustExecutor(
		t,
		&fakeQueryConnection{rows: rows},
	).ExecuteFieldSuggestions(context.Background(), validCompiledFieldSuggestions("", 2))
	assertFieldSuggestionError(t, got, err, ErrFieldMetadataUnavailable)
	if !rows.closed || rows.nextCalls != 3 {
		t.Fatalf("metadata-invalid stream closed=%v, nextCalls=%d; want fully consumed", rows.closed, rows.nextCalls)
	}
}

func TestExecutorExecuteFieldSuggestionsHonorsContextAtEveryBoundary(t *testing.T) {
	t.Run("nil", func(t *testing.T) {
		connection := &fakeQueryConnection{rows: fieldSuggestionFakeRows(0)}
		//nolint:staticcheck // This case explicitly verifies the nil-context guard.
		got, err := mustExecutor(t, connection).ExecuteFieldSuggestions(
			nil,
			validCompiledFieldSuggestions("", 1),
		)
		if err == nil || !reflect.DeepEqual(got, FieldSuggestionResult{}) || connection.query != "" {
			t.Fatalf("ExecuteFieldSuggestions(nil) = (%#v, %v), query=%q", got, err, connection.query)
		}
	})
	t.Run("pre canceled", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		connection := &fakeQueryConnection{rows: fieldSuggestionFakeRows(0)}
		got, err := mustExecutor(t, connection).ExecuteFieldSuggestions(
			ctx,
			validCompiledFieldSuggestions("", 1),
		)
		assertFieldSuggestionError(t, got, err, context.Canceled)
		if connection.query != "" {
			t.Fatalf("pre-canceled query issued: %q", connection.query)
		}
	})
	t.Run("canceled by query ID", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		connection := &fakeQueryConnection{rows: fieldSuggestionFakeRows(0)}
		executor := mustExecutor(t, connection)
		executor.newQueryID = func() (string, error) {
			cancel()
			return "field-suggestion-query", nil
		}
		got, err := executor.ExecuteFieldSuggestions(ctx, validCompiledFieldSuggestions("", 1))
		assertFieldSuggestionError(t, got, err, context.Canceled)
		if connection.query != "" {
			t.Fatalf("query issued after cancellation: %q", connection.query)
		}
	})
	t.Run("canceled after query", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		base := fieldSuggestionFakeRows(0)
		connection := &cancelingFieldCatalogConnection{rows: base, cancel: cancel}
		got, err := mustExecutor(t, connection).ExecuteFieldSuggestions(
			ctx,
			validCompiledFieldSuggestions("", 1),
		)
		assertFieldSuggestionError(t, got, err, context.Canceled)
		if !base.closed {
			t.Fatal("rows were not closed after query cancellation")
		}
	})
	t.Run("canceled during scan", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		base := fieldSuggestionFakeRows(0)
		rows := &cancelAfterTimelineScanRows{fakeRows: base, cancel: cancel}
		got, err := mustExecutor(t, &fakeQueryConnection{rows: rows}).ExecuteFieldSuggestions(
			ctx,
			validCompiledFieldSuggestions("", 1),
		)
		assertFieldSuggestionError(t, got, err, context.Canceled)
		if !base.closed || rows.scanCalls != 1 {
			t.Fatalf("closed=%v scanCalls=%d", base.closed, rows.scanCalls)
		}
	})
	t.Run("canceled during close", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		base := fieldSuggestionFakeRows(0)
		rows := &cancelOnTimelineCloseRows{fakeRows: base, cancel: cancel}
		got, err := mustExecutor(t, &fakeQueryConnection{rows: rows}).ExecuteFieldSuggestions(
			ctx,
			validCompiledFieldSuggestions("", 1),
		)
		assertFieldSuggestionError(t, got, err, context.Canceled)
		if rows.closeCalls != 1 {
			t.Fatalf("closeCalls=%d, want 1", rows.closeCalls)
		}
	})
}

func TestExecutorExecuteFieldSuggestionsClassifiesDriverFailuresAtomically(t *testing.T) {
	tests := []struct {
		name       string
		connection queryConnection
	}{
		{name: "query", connection: &fakeQueryConnection{err: io.ErrUnexpectedEOF}},
		{name: "scan", connection: &fakeQueryConnection{
			rows: &timelineScanErrorRows{fakeRows: fieldSuggestionFakeRows(0)},
		}},
		{name: "iteration", connection: func() queryConnection {
			rows := fieldSuggestionFakeRows(0)
			rows.err = io.ErrUnexpectedEOF
			return &fakeQueryConnection{rows: rows}
		}()},
		{name: "close", connection: func() queryConnection {
			rows := fieldSuggestionFakeRows(0)
			rows.closeErr = io.ErrUnexpectedEOF
			return &fakeQueryConnection{rows: rows}
		}()},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := mustExecutor(t, test.connection).ExecuteFieldSuggestions(
				context.Background(),
				validCompiledFieldSuggestions("", 1),
			)
			assertFieldSuggestionError(t, got, err, searchjobs.ErrStorageUnavailable)
		})
	}
}

func TestExecutorExecuteFieldSuggestionsValidatesStateAndQueryBeforeExecution(t *testing.T) {
	var typedNilConnection *fakeQueryConnection
	tests := []struct {
		name     string
		executor *Executor
		query    clickhouse.CompiledFieldSuggestions
	}{
		{name: "nil receiver", query: validCompiledFieldSuggestions("", 1)},
		{name: "nil connection", executor: &Executor{}, query: validCompiledFieldSuggestions("", 1)},
		{name: "typed nil connection", executor: &Executor{
			connection: typedNilConnection,
		}, query: validCompiledFieldSuggestions("", 1)},
		{name: "blank SQL", executor: mustExecutor(t, &fakeQueryConnection{}), query: clickhouse.CompiledFieldSuggestions{
			SQL: " \n", Spec: clickhouse.FieldSuggestionSpec{MaximumFields: 1},
		}},
		{name: "zero maximum", executor: mustExecutor(t, &fakeQueryConnection{}), query: validCompiledFieldSuggestions("", 0)},
		{name: "oversized maximum", executor: mustExecutor(t, &fakeQueryConnection{}), query: validCompiledFieldSuggestions(
			"", clickhouse.MaximumFieldSuggestions+1,
		)},
		{name: "invalid UTF-8 prefix", executor: mustExecutor(t, &fakeQueryConnection{}), query: validCompiledFieldSuggestions(
			string([]byte{0xff}), 1,
		)},
		{name: "NUL prefix", executor: mustExecutor(t, &fakeQueryConnection{}), query: validCompiledFieldSuggestions(
			"sta\x00tus", 1,
		)},
		{name: "control prefix", executor: mustExecutor(t, &fakeQueryConnection{}), query: validCompiledFieldSuggestions(
			"sta\x01tus", 1,
		)},
		{name: "C1 control prefix", executor: mustExecutor(t, &fakeQueryConnection{}), query: validCompiledFieldSuggestions(
			"sta\u0085tus", 1,
		)},
		{name: "oversized prefix", executor: mustExecutor(t, &fakeQueryConnection{}), query: validCompiledFieldSuggestions(
			strings.Repeat("x", eventfields.MaximumNormalizedFieldNameBytes+1), 1,
		)},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := test.executor.ExecuteFieldSuggestions(context.Background(), test.query)
			if err == nil || !reflect.DeepEqual(got, FieldSuggestionResult{}) {
				t.Fatalf("ExecuteFieldSuggestions() = (%#v, %v), want zero and error", got, err)
			}
		})
	}

	t.Run("nil query ID generator", func(t *testing.T) {
		executor := mustExecutor(t, &fakeQueryConnection{})
		executor.newQueryID = nil
		got, err := executor.ExecuteFieldSuggestions(
			context.Background(),
			validCompiledFieldSuggestions("", 1),
		)
		if err == nil || !reflect.DeepEqual(got, FieldSuggestionResult{}) {
			t.Fatalf("ExecuteFieldSuggestions() = (%#v, %v)", got, err)
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
			got, err := executor.ExecuteFieldSuggestions(
				context.Background(),
				validCompiledFieldSuggestions("", 1),
			)
			if err == nil || !reflect.DeepEqual(got, FieldSuggestionResult{}) || connection.query != "" {
				t.Fatalf("ExecuteFieldSuggestions() = (%#v, %v), query=%q", got, err, connection.query)
			}
		})
	}

	t.Run("typed nil rows", func(t *testing.T) {
		var rows *fakeRows
		got, err := mustExecutor(
			t,
			&fakeQueryConnection{rows: rows},
		).ExecuteFieldSuggestions(context.Background(), validCompiledFieldSuggestions("", 1))
		assertFieldSuggestionError(t, got, err, searchjobs.ErrInvalidResult)
	})
}

func TestSettingsForFieldSuggestionsClonesAndTightensCaps(t *testing.T) {
	t.Parallel()

	base, err := querySettings(Config{
		MaxExecutionTime: 2 * maximumFieldSuggestionExecutionTime,
		MaxMemoryBytes:   2 * maximumFieldSuggestionMemoryBytes,
		MaxRowsToRead:    2 * maximumFieldSuggestionRowsToRead,
		MaxBytesToRead:   2 * maximumFieldSuggestionBytesToRead,
		MaxResultRows:    50_000,
		MaxResultBytes:   64 << 20,
		MaxRowsToGroupBy: 50_000,
		MaxThreads:       8,
	})
	if err != nil {
		t.Fatal(err)
	}
	before := cloneFieldCatalogSettings(base)
	got, err := settingsForFieldSuggestions(base, 73)
	if err != nil {
		t.Fatal(err)
	}
	if got["max_execution_time"] != uint64(maximumFieldSuggestionExecutionTime.Seconds()) ||
		got["max_memory_usage"] != maximumFieldSuggestionMemoryBytes ||
		got["max_rows_to_read"] != maximumFieldSuggestionRowsToRead ||
		got["max_bytes_to_read"] != maximumFieldSuggestionBytesToRead ||
		got["max_result_rows"] != uint64(75) ||
		got["max_result_bytes"] != maximumFieldSuggestionResultBytes ||
		got["max_rows_to_group_by"] != maximumFieldSuggestionGroups ||
		got["max_threads"] != maximumFieldSuggestionThreads ||
		got["readonly"] != uint8(2) {
		t.Fatalf("field suggestion settings = %#v", got)
	}
	if !reflect.DeepEqual(base, before) {
		t.Fatalf("base settings mutated: got %#v, want %#v", base, before)
	}

	base["max_result_rows"] = uint64(7)
	base["max_result_bytes"] = uint64(1024)
	base["max_rows_to_group_by"] = uint64(5)
	strict, err := settingsForFieldSuggestions(base, 73)
	if err != nil {
		t.Fatal(err)
	}
	if strict["max_result_rows"] != uint64(7) ||
		strict["max_result_bytes"] != uint64(1024) ||
		strict["max_rows_to_group_by"] != uint64(5) {
		t.Fatalf("stricter base caps were raised: %#v", strict)
	}
}

func TestSettingsForFieldSuggestionsRejectsUnsafeBaseSettings(t *testing.T) {
	base, err := querySettings(Config{})
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{
		"max_execution_time", "max_memory_usage", "max_rows_to_read", "max_bytes_to_read",
		"max_result_rows", "max_result_bytes", "max_rows_to_group_by", "max_threads",
		"max_query_size", "max_subquery_depth",
	} {
		t.Run(name+" missing", func(t *testing.T) {
			malformed := cloneFieldCatalogSettings(base)
			delete(malformed, name)
			if _, err := settingsForFieldSuggestions(malformed, 1); err == nil {
				t.Fatalf("missing %s unexpectedly accepted", name)
			}
		})
		t.Run(name+" zero", func(t *testing.T) {
			malformed := cloneFieldCatalogSettings(base)
			malformed[name] = uint64(0)
			if _, err := settingsForFieldSuggestions(malformed, 1); err == nil {
				t.Fatalf("zero %s unexpectedly accepted", name)
			}
		})
		t.Run(name+" wrong type", func(t *testing.T) {
			malformed := cloneFieldCatalogSettings(base)
			malformed[name] = "1"
			if _, err := settingsForFieldSuggestions(malformed, 1); err == nil {
				t.Fatalf("wrong type %s unexpectedly accepted", name)
			}
		})
	}
	for _, name := range []string{
		"timeout_overflow_mode", "read_overflow_mode", "result_overflow_mode",
		"group_by_overflow_mode",
	} {
		t.Run(name, func(t *testing.T) {
			malformed := cloneFieldCatalogSettings(base)
			malformed[name] = "break"
			if _, err := settingsForFieldSuggestions(malformed, 1); err == nil {
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
			if _, err := settingsForFieldSuggestions(malformed, 1); err == nil {
				t.Fatalf("unsafe %s unexpectedly accepted", test.name)
			}
		})
	}
	for _, malformed := range []clickhousedriver.Settings{nil, cloneFieldCatalogSettings(base)} {
		if malformed != nil {
			malformed["readonly"] = uint8(1)
		}
		if _, err := settingsForFieldSuggestions(malformed, 1); err == nil {
			t.Fatalf("settingsForFieldSuggestions(%#v) unexpectedly succeeded", malformed)
		}
	}
	if _, err := settingsForFieldSuggestions(base, 0); err == nil {
		t.Fatal("zero maximum fields unexpectedly accepted")
	}
	if _, err := settingsForFieldSuggestions(base, clickhouse.MaximumFieldSuggestions+1); err == nil {
		t.Fatal("oversized maximum fields unexpectedly accepted")
	}
}

func TestSettingsForFieldSuggestionsIsRaceSafeForConcurrentReaders(t *testing.T) {
	t.Parallel()

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
			got, err := settingsForFieldSuggestions(base, uint32(index+1))
			if err != nil {
				t.Errorf("settingsForFieldSuggestions() error = %v", err)
				return
			}
			if got["max_result_rows"] != uint64(index+3) {
				t.Errorf("settingsForFieldSuggestions(%d) = %#v", index+1, got)
			}
		}()
	}
	wait.Wait()
}

func validCompiledFieldSuggestions(prefix string, maximum uint32) clickhouse.CompiledFieldSuggestions {
	return clickhouse.CompiledFieldSuggestions{
		SQL: "SELECT bounded_field_suggestions",
		Spec: clickhouse.FieldSuggestionSpec{
			Prefix:        prefix,
			MaximumFields: maximum,
		},
	}
}

func fieldSuggestionFakeRows(metadataInvalid uint8, names ...string) *fakeRows {
	data := make([][]any, 0, len(names)+1)
	data = append(data, []any{uint8(0), "", metadataInvalid})
	for _, name := range names {
		data = append(data, fieldSuggestionName(name))
	}
	columns := []string{
		clickhouse.FieldSuggestionRowKindColumn,
		clickhouse.FieldSuggestionNameColumn,
		clickhouse.FieldSuggestionInvalidColumn,
	}
	databaseTypes := []string{"UInt8", "String", "UInt8"}
	scanTypes := []reflect.Type{
		reflect.TypeOf(uint8(0)),
		reflect.TypeOf(""),
		reflect.TypeOf(uint8(0)),
	}
	types := make([]driver.ColumnType, len(columns))
	for index := range columns {
		types[index] = fakeColumnType{
			name: columns[index], databaseType: databaseTypes[index], scanType: scanTypes[index],
		}
	}
	return &fakeRows{columns: columns, types: types, data: data}
}

func fieldSuggestionName(name string) []any {
	return []any{uint8(1), name, uint8(0)}
}

func assertFieldSuggestionError(
	t *testing.T,
	got FieldSuggestionResult,
	err error,
	want error,
) {
	t.Helper()
	if !errors.Is(err, want) || !reflect.DeepEqual(got, FieldSuggestionResult{}) {
		t.Fatalf("ExecuteFieldSuggestions() = (%#v, %v), want zero result and %v", got, err, want)
	}
}
