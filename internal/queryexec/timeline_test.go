package queryexec

import (
	"context"
	"errors"
	"io"
	"math"
	"reflect"
	"testing"
	"time"

	clickhousedriver "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
)

func TestExecutorExecuteTimelineReturnsCompleteZeroFilledGrid(t *testing.T) {
	t.Parallel()
	first := time.Date(2026, time.July, 22, 12, 0, 0, 0, time.UTC)
	rows := timelineFakeRows(first, time.Minute, []uint64{0, 7, 0})
	connection := &fakeQueryConnection{rows: rows}
	executor := mustExecutor(t, connection)
	query := validCompiledTimeline(first, 3)
	query.Args = []any{"tenant-a", uint64(91)}

	got, err := executor.ExecuteTimeline(context.Background(), query)
	if err != nil {
		t.Fatalf("ExecuteTimeline() error = %v", err)
	}
	if !rows.closed {
		t.Fatal("timeline rows were not closed")
	}
	if connection.query != query.SQL || !reflect.DeepEqual(connection.args, query.Args) {
		t.Fatalf("query/args = %q %#v, want %q %#v", connection.query, connection.args, query.SQL, query.Args)
	}
	want := []TimelineBucket{
		{AlignedStart: first, Count: 0},
		{AlignedStart: first.Add(time.Minute), Count: 7},
		{AlignedStart: first.Add(2 * time.Minute), Count: 0},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("ExecuteTimeline() = %#v, want %#v", got, want)
	}
}

func TestExecutorExecuteTimelineRejectsMalformedResultsAtomically(t *testing.T) {
	first := time.Date(2026, time.July, 22, 12, 0, 0, 0, time.UTC)
	tests := []struct {
		name   string
		mutate func(*fakeRows)
	}{
		{name: "wrong columns", mutate: func(rows *fakeRows) { rows.columns[0] = "wrong" }},
		{name: "reversed columns", mutate: func(rows *fakeRows) { rows.columns[0], rows.columns[1] = rows.columns[1], rows.columns[0] }},
		{name: "extra column", mutate: func(rows *fakeRows) {
			rows.columns = append(rows.columns, "extra")
			rows.types = append(rows.types, fakeColumnType{name: "extra", databaseType: "UInt8", scanType: reflect.TypeOf(uint8(0))})
		}},
		{name: "missing type", mutate: func(rows *fakeRows) { rows.types = rows.types[:1] }},
		{name: "typed nil type", mutate: func(rows *fakeRows) {
			var columnType *fakeColumnType
			rows.types[0] = columnType
		}},
		{name: "type name differs from column", mutate: func(rows *fakeRows) {
			rows.types[0] = fakeColumnType{name: "wrong", databaseType: timelineOrdinalDatabaseType, scanType: reflect.TypeOf(uint64(0))}
		}},
		{name: "nullable ordinal", mutate: func(rows *fakeRows) {
			rows.types[0] = fakeColumnType{name: clickhouse.TimelineOrdinalColumn, databaseType: timelineOrdinalDatabaseType, scanType: reflect.TypeOf(uint64(0)), nullable: true}
		}},
		{name: "wrapped ordinal", mutate: func(rows *fakeRows) {
			rows.types[0] = fakeColumnType{name: clickhouse.TimelineOrdinalColumn, databaseType: "Nullable(" + timelineOrdinalDatabaseType + ")", scanType: reflect.TypeOf(uint64(0))}
		}},
		{name: "wrong ordinal width", mutate: func(rows *fakeRows) {
			rows.types[0] = fakeColumnType{name: clickhouse.TimelineOrdinalColumn, databaseType: "UInt32", scanType: reflect.TypeOf(uint32(0))}
		}},
		{name: "nullable count", mutate: func(rows *fakeRows) {
			rows.types[1] = fakeColumnType{name: clickhouse.TimelineCountColumn, databaseType: "Nullable(UInt64)", scanType: reflect.TypeOf(uint64(0)), nullable: true}
		}},
		{name: "wrong count width", mutate: func(rows *fakeRows) {
			rows.types[1] = fakeColumnType{name: clickhouse.TimelineCountColumn, databaseType: "UInt32", scanType: reflect.TypeOf(uint32(0))}
		}},
		{name: "short grid", mutate: func(rows *fakeRows) { rows.data = rows.data[:2] }},
		{name: "long grid", mutate: func(rows *fakeRows) {
			rows.data = append(rows.data, []any{uint64(3), uint64(4)})
		}},
		{name: "wrong first ordinal", mutate: func(rows *fakeRows) { rows.data[0][0] = uint64(1) }},
		{name: "gap", mutate: func(rows *fakeRows) { rows.data[1][0] = uint64(2) }},
		{name: "duplicate", mutate: func(rows *fakeRows) { rows.data[1][0] = uint64(0) }},
		{name: "descending", mutate: func(rows *fakeRows) {
			rows.data[0][0], rows.data[1][0] = rows.data[1][0], rows.data[0][0]
		}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			rows := timelineFakeRows(first, time.Minute, []uint64{1, 2, 3})
			test.mutate(rows)
			got, err := mustExecutor(t, &fakeQueryConnection{rows: rows}).ExecuteTimeline(context.Background(), validCompiledTimeline(first, 3))
			if !errors.Is(err, searchjobs.ErrInvalidResult) {
				t.Fatalf("ExecuteTimeline() = (%#v, %v), want ErrInvalidResult", got, err)
			}
			if got != nil {
				t.Fatalf("malformed timeline was returned partially: %#v", got)
			}
			if !rows.closed {
				t.Fatal("timeline rows were not closed after validation failure")
			}
		})
	}
}

func TestExecutorExecuteTimelineRejectsInvalidCompiledSpecificationsBeforeQuery(t *testing.T) {
	first := time.Date(2026, time.July, 22, 12, 0, 0, 0, time.UTC)
	tests := []struct {
		name   string
		mutate func(*clickhouse.CompiledTimeline)
	}{
		{name: "blank SQL", mutate: func(query *clickhouse.CompiledTimeline) { query.SQL = " \n\t" }},
		{name: "zero first bucket", mutate: func(query *clickhouse.CompiledTimeline) { query.Spec.FirstBucket = time.Time{} }},
		{name: "non UTC first bucket", mutate: func(query *clickhouse.CompiledTimeline) {
			query.Spec.FirstBucket = query.Spec.FirstBucket.In(time.FixedZone("UTC-like", 0))
		}},
		{name: "fractional first bucket", mutate: func(query *clickhouse.CompiledTimeline) {
			query.Spec.FirstBucket = query.Spec.FirstBucket.Add(time.Nanosecond)
		}},
		{name: "zero span", mutate: func(query *clickhouse.CompiledTimeline) { query.Spec.SpanSeconds = 0 }},
		{name: "negative span", mutate: func(query *clickhouse.CompiledTimeline) { query.Spec.SpanSeconds = -1 }},
		{name: "span nanoseconds overflow", mutate: func(query *clickhouse.CompiledTimeline) { query.Spec.SpanSeconds = maximumTimelineSpanSeconds + 1 }},
		{name: "misaligned origin", mutate: func(query *clickhouse.CompiledTimeline) {
			query.Spec.FirstBucket = query.Spec.FirstBucket.Add(time.Second)
		}},
		{name: "zero buckets", mutate: func(query *clickhouse.CompiledTimeline) { query.Spec.BucketCount = 0 }},
		{name: "too many buckets", mutate: func(query *clickhouse.CompiledTimeline) { query.Spec.BucketCount = maximumTimelineResultBuckets + 1 }},
		{name: "zero earliest", mutate: func(query *clickhouse.CompiledTimeline) { query.Spec.Earliest = time.Time{} }},
		{name: "zero latest", mutate: func(query *clickhouse.CompiledTimeline) { query.Spec.Latest = time.Time{} }},
		{name: "non UTC earliest", mutate: func(query *clickhouse.CompiledTimeline) {
			query.Spec.Earliest = query.Spec.Earliest.In(time.FixedZone("UTC-like", 0))
		}},
		{name: "non UTC latest", mutate: func(query *clickhouse.CompiledTimeline) {
			query.Spec.Latest = query.Spec.Latest.In(time.FixedZone("UTC-like", 0))
		}},
		{name: "earliest before DateTime64 minimum", mutate: func(query *clickhouse.CompiledTimeline) {
			query.Spec.Earliest = clickhouse.MinimumSearchTime().Add(-time.Nanosecond)
		}},
		{name: "latest after DateTime64 maximum", mutate: func(query *clickhouse.CompiledTimeline) {
			query.Spec.Latest = clickhouse.MaximumSearchTime().Add(time.Nanosecond)
		}},
		{name: "empty range", mutate: func(query *clickhouse.CompiledTimeline) { query.Spec.Latest = query.Spec.Earliest }},
		{name: "reversed range", mutate: func(query *clickhouse.CompiledTimeline) {
			query.Spec.Latest = query.Spec.Earliest.Add(-time.Nanosecond)
		}},
		{name: "earliest before cover", mutate: func(query *clickhouse.CompiledTimeline) {
			query.Spec.Earliest = query.Spec.FirstBucket.Add(-time.Nanosecond)
		}},
		{name: "nonminimal first bucket", mutate: func(query *clickhouse.CompiledTimeline) {
			query.Spec.Earliest = query.Spec.FirstBucket.Add(time.Minute)
		}},
		{name: "nonminimal last bucket", mutate: func(query *clickhouse.CompiledTimeline) {
			query.Spec.Latest = query.Spec.FirstBucket.Add(2 * time.Minute)
		}},
		{name: "latest after cover", mutate: func(query *clickhouse.CompiledTimeline) {
			query.Spec.Latest = query.Spec.FirstBucket.Add(3*time.Minute + time.Nanosecond)
		}},
		{name: "boundary addition overflow", mutate: func(query *clickhouse.CompiledTimeline) {
			query.Spec = clickhouse.TimelineSpec{
				FirstBucket: time.Unix(math.MaxInt64-1, 0).UTC(), SpanSeconds: 2, BucketCount: 2,
				Earliest: time.Unix(math.MaxInt64-1, 0).UTC(), Latest: time.Unix(math.MaxInt64, 0).UTC(),
			}
		}},
		{name: "boundary multiplication overflow", mutate: func(query *clickhouse.CompiledTimeline) {
			query.Spec = clickhouse.TimelineSpec{
				FirstBucket: time.Unix(0, 0).UTC(), SpanSeconds: math.MaxInt64, BucketCount: 2,
				Earliest: time.Unix(0, 0).UTC(), Latest: time.Unix(1, 0).UTC(),
			}
		}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			query := validCompiledTimeline(first, 3)
			test.mutate(&query)
			connection := &fakeQueryConnection{rows: timelineFakeRows(first, time.Minute, []uint64{1, 2, 3})}
			got, err := mustExecutor(t, connection).ExecuteTimeline(context.Background(), query)
			if !errors.Is(err, searchjobs.ErrInvalidResult) {
				t.Fatalf("ExecuteTimeline() = (%#v, %v), want ErrInvalidResult", got, err)
			}
			if got != nil || connection.query != "" {
				t.Fatalf("invalid compiled timeline returned %#v and issued query %q", got, connection.query)
			}
		})
	}
}

func TestValidateCompiledTimelineAcceptsMaximumFractionalCover(t *testing.T) {
	t.Parallel()
	first := time.Unix(0, 0).UTC()
	query := clickhouse.CompiledTimeline{
		SQL: "SELECT bounded_timeline",
		Spec: clickhouse.TimelineSpec{
			FirstBucket: first,
			SpanSeconds: 1,
			BucketCount: maximumTimelineResultBuckets,
			Earliest:    first.Add(time.Nanosecond),
			Latest:      first.Add(time.Duration(maximumTimelineResultBuckets) * time.Second),
		},
	}
	if err := validateCompiledTimeline(query); err != nil {
		t.Fatalf("validateCompiledTimeline() error = %v", err)
	}
}

func TestExecutorExecuteTimelineHonorsContextAtEveryBoundary(t *testing.T) {
	first := time.Unix(0, 0).UTC()
	t.Run("nil", func(t *testing.T) {
		connection := &fakeQueryConnection{rows: timelineFakeRows(first, time.Minute, []uint64{1})}
		got, err := mustExecutor(t, connection).ExecuteTimeline(nil, validCompiledTimeline(first, 1))
		if err == nil || got != nil || connection.query != "" {
			t.Fatalf("ExecuteTimeline(nil) = (%#v, %v), query=%q", got, err, connection.query)
		}
	})
	t.Run("pre canceled", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		connection := &fakeQueryConnection{rows: timelineFakeRows(first, time.Minute, []uint64{1})}
		got, err := mustExecutor(t, connection).ExecuteTimeline(ctx, validCompiledTimeline(first, 1))
		if !errors.Is(err, context.Canceled) || got != nil || connection.query != "" {
			t.Fatalf("ExecuteTimeline() = (%#v, %v), query=%q", got, err, connection.query)
		}
	})
	t.Run("canceled by query ID generation", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		connection := &fakeQueryConnection{rows: timelineFakeRows(first, time.Minute, []uint64{1})}
		executor := mustExecutor(t, connection)
		executor.newQueryID = func() (string, error) { cancel(); return "timeline-query", nil }
		got, err := executor.ExecuteTimeline(ctx, validCompiledTimeline(first, 1))
		if !errors.Is(err, context.Canceled) || got != nil || connection.query != "" {
			t.Fatalf("ExecuteTimeline() = (%#v, %v), query=%q", got, err, connection.query)
		}
	})
	t.Run("canceled during scan", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		base := timelineFakeRows(first, time.Minute, []uint64{1, 2})
		rows := &cancelAfterTimelineScanRows{fakeRows: base, cancel: cancel}
		got, err := mustExecutor(t, &fakeQueryConnection{rows: rows}).ExecuteTimeline(ctx, validCompiledTimeline(first, 2))
		if !errors.Is(err, context.Canceled) || got != nil || !base.closed || rows.scanCalls != 1 {
			t.Fatalf("ExecuteTimeline() = (%#v, %v), closed=%v scanCalls=%d", got, err, base.closed, rows.scanCalls)
		}
	})
	t.Run("canceled during close", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		base := timelineFakeRows(first, time.Minute, []uint64{1})
		rows := &cancelOnTimelineCloseRows{fakeRows: base, cancel: cancel}
		got, err := mustExecutor(t, &fakeQueryConnection{rows: rows}).ExecuteTimeline(ctx, validCompiledTimeline(first, 1))
		if !errors.Is(err, context.Canceled) || got != nil || rows.closeCalls != 1 {
			t.Fatalf("ExecuteTimeline() = (%#v, %v), closeCalls=%d", got, err, rows.closeCalls)
		}
	})
}

func TestExecutorExecuteTimelineClassifiesDriverFailuresAtomically(t *testing.T) {
	first := time.Unix(0, 0).UTC()
	tests := []struct {
		name       string
		connection queryConnection
		want       error
	}{
		{
			name: "query", connection: &fakeQueryConnection{err: io.ErrUnexpectedEOF}, want: searchjobs.ErrStorageUnavailable,
		},
		{
			name: "scan", connection: func() queryConnection {
				base := timelineFakeRows(first, time.Minute, []uint64{1})
				return &fakeQueryConnection{rows: &timelineScanErrorRows{fakeRows: base}}
			}(), want: searchjobs.ErrStorageUnavailable,
		},
		{
			name: "iteration", connection: func() queryConnection {
				rows := timelineFakeRows(first, time.Minute, []uint64{1})
				rows.err = io.ErrUnexpectedEOF
				return &fakeQueryConnection{rows: rows}
			}(), want: searchjobs.ErrStorageUnavailable,
		},
		{
			name: "close", connection: func() queryConnection {
				rows := timelineFakeRows(first, time.Minute, []uint64{1})
				rows.closeErr = io.ErrUnexpectedEOF
				return &fakeQueryConnection{rows: rows}
			}(), want: searchjobs.ErrStorageUnavailable,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := mustExecutor(t, test.connection).ExecuteTimeline(context.Background(), validCompiledTimeline(first, 1))
			if !errors.Is(err, test.want) || got != nil {
				t.Fatalf("ExecuteTimeline() = (%#v, %v), want nil, %v", got, err, test.want)
			}
		})
	}
}

func TestExecutorExecuteTimelineValidatesExecutorStateAndQueryID(t *testing.T) {
	first := time.Unix(0, 0).UTC()
	query := validCompiledTimeline(first, 1)
	var nilConnection *fakeQueryConnection
	tests := []struct {
		name     string
		executor *Executor
	}{
		{name: "nil receiver"},
		{name: "nil connection", executor: &Executor{}},
		{name: "typed nil connection", executor: &Executor{connection: nilConnection}},
		{name: "nil query ID generator", executor: func() *Executor {
			executor := mustExecutor(t, &fakeQueryConnection{rows: timelineFakeRows(first, time.Minute, []uint64{1})})
			executor.newQueryID = nil
			return executor
		}()},
		{name: "nil settings", executor: func() *Executor {
			executor := mustExecutor(t, &fakeQueryConnection{rows: timelineFakeRows(first, time.Minute, []uint64{1})})
			executor.settings = nil
			return executor
		}()},
		{name: "non readonly settings", executor: func() *Executor {
			executor := mustExecutor(t, &fakeQueryConnection{rows: timelineFakeRows(first, time.Minute, []uint64{1})})
			executor.settings["readonly"] = uint8(1)
			return executor
		}()},
		{name: "empty query ID", executor: func() *Executor {
			executor := mustExecutor(t, &fakeQueryConnection{rows: timelineFakeRows(first, time.Minute, []uint64{1})})
			executor.newQueryID = func() (string, error) { return "", nil }
			return executor
		}()},
		{name: "query ID failure", executor: func() *Executor {
			executor := mustExecutor(t, &fakeQueryConnection{rows: timelineFakeRows(first, time.Minute, []uint64{1})})
			executor.newQueryID = func() (string, error) { return "", io.ErrUnexpectedEOF }
			return executor
		}()},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := test.executor.ExecuteTimeline(context.Background(), query)
			if err == nil || got != nil {
				t.Fatalf("ExecuteTimeline() = (%#v, %v), want nil and error", got, err)
			}
		})
	}
}

func TestSettingsForTimelineClonesAndTightensOnlyResultCaps(t *testing.T) {
	t.Parallel()
	base, err := querySettings(Config{MaxResultRows: 500, MaxResultBytes: 4 << 20})
	if err != nil {
		t.Fatal(err)
	}
	before := clickhousedriver.Settings{}
	for key, value := range base {
		before[key] = value
	}
	got, err := settingsForTimeline(base, 73)
	if err != nil {
		t.Fatalf("settingsForTimeline() error = %v", err)
	}
	if got["max_result_rows"] != uint64(73) || got["max_result_bytes"] != maximumTimelineResultBytes || got["readonly"] != uint8(2) {
		t.Fatalf("timeline settings = %#v", got)
	}
	if !reflect.DeepEqual(base, before) {
		t.Fatalf("base settings mutated: got %#v, want %#v", base, before)
	}
	for key, value := range before {
		if key == "max_result_rows" || key == "max_result_bytes" {
			continue
		}
		if !reflect.DeepEqual(got[key], value) {
			t.Fatalf("setting %q changed from %#v to %#v", key, value, got[key])
		}
	}

	base["max_result_rows"] = uint64(17)
	base["max_result_bytes"] = uint64(64 << 10)
	strict, err := settingsForTimeline(base, 73)
	if err != nil {
		t.Fatal(err)
	}
	if strict["max_result_rows"] != uint64(17) || strict["max_result_bytes"] != uint64(64<<10) {
		t.Fatalf("stricter base caps were raised: %#v", strict)
	}

	for _, malformed := range []clickhousedriver.Settings{
		nil,
		{"readonly": uint8(2), "max_result_rows": uint64(1)},
		{"readonly": uint8(2), "max_result_rows": "1", "max_result_bytes": uint64(1)},
		{"readonly": uint8(2), "max_result_rows": uint64(0), "max_result_bytes": uint64(1)},
		{"readonly": uint8(2), "max_result_rows": uint64(1), "max_result_bytes": uint64(0)},
	} {
		if _, err := settingsForTimeline(malformed, 1); err == nil {
			t.Fatalf("settingsForTimeline(%#v) unexpectedly succeeded", malformed)
		}
	}
	if _, err := settingsForTimeline(base, 0); err == nil {
		t.Fatal("zero timeline bucket count unexpectedly succeeded")
	}
	if _, err := settingsForTimeline(base, maximumTimelineResultBuckets+1); err == nil {
		t.Fatal("oversized timeline bucket count unexpectedly succeeded")
	}
}

func validCompiledTimeline(first time.Time, bucketCount uint64) clickhouse.CompiledTimeline {
	const span = time.Minute
	return clickhouse.CompiledTimeline{
		SQL: "SELECT bounded_timeline",
		Spec: clickhouse.TimelineSpec{
			FirstBucket: first,
			SpanSeconds: int64(span / time.Second),
			BucketCount: bucketCount,
			Earliest:    first.Add(15 * time.Second),
			Latest:      first.Add(time.Duration(bucketCount-1)*span + 30*time.Second),
		},
	}
}

func timelineFakeRows(_ time.Time, _ time.Duration, counts []uint64) *fakeRows {
	data := make([][]any, len(counts))
	for index, count := range counts {
		data[index] = []any{uint64(index), count}
	}
	return &fakeRows{
		columns: []string{clickhouse.TimelineOrdinalColumn, clickhouse.TimelineCountColumn},
		types: []driver.ColumnType{
			fakeColumnType{name: clickhouse.TimelineOrdinalColumn, databaseType: timelineOrdinalDatabaseType, scanType: reflect.TypeOf(uint64(0))},
			fakeColumnType{name: clickhouse.TimelineCountColumn, databaseType: timelineCountDatabaseType, scanType: reflect.TypeOf(uint64(0))},
		},
		data: data,
	}
}

type cancelAfterTimelineScanRows struct {
	*fakeRows
	cancel    context.CancelFunc
	scanCalls int
}

func (rows *cancelAfterTimelineScanRows) Scan(destinations ...any) error {
	rows.scanCalls++
	err := rows.fakeRows.Scan(destinations...)
	rows.cancel()
	return err
}

type cancelOnTimelineCloseRows struct {
	*fakeRows
	cancel     context.CancelFunc
	closeCalls int
}

func (rows *cancelOnTimelineCloseRows) Close() error {
	rows.closeCalls++
	err := rows.fakeRows.Close()
	rows.cancel()
	return err
}

type timelineScanErrorRows struct {
	*fakeRows
}

func (rows *timelineScanErrorRows) Scan(...any) error {
	return io.ErrUnexpectedEOF
}
