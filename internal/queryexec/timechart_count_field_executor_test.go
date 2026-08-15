package queryexec

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
)

func TestExecutorPublishesAllZeroFixedCountFieldGridWhenInputRowsExist(t *testing.T) {
	t.Parallel()

	first := time.Date(2026, time.July, 22, 12, 0, 0, 0, time.UTC)
	rows := fixedCountFieldTimechartRows(
		[]uint64{0, 0, 0},
		[]uint8{1, 1, 1},
	)
	sink := &fakeSink{}
	if err := mustExecutor(t, &fakeQueryConnection{rows: rows}).Execute(
		context.Background(),
		fixedCountFieldTimechartQuery(first, 3, "eligible_values"),
		sink,
	); err != nil {
		t.Fatalf("Execute: %v", err)
	}

	wantSchema := searchjobs.Schema{Columns: []searchjobs.Column{
		{Name: "_time", Kind: searchjobs.ValueKindTime},
		{Name: "eligible_values", Kind: searchjobs.ValueKindUnsigned},
	}}
	if sink.setCalls != 1 || !reflect.DeepEqual(sink.schema, wantSchema) ||
		len(sink.rows) != 3 || !rows.closed {
		t.Fatalf(
			"schema=%#v calls=%d rows=%d closed=%v",
			sink.schema,
			sink.setCalls,
			len(sink.rows),
			rows.closed,
		)
	}
	for ordinal, row := range sink.rows {
		bucket, bucketOK := row[0].Time()
		count, countOK := row[1].Unsigned()
		if !bucketOK || !bucket.Equal(first.Add(time.Duration(ordinal)*5*time.Minute)) ||
			!countOK || count != 0 {
			t.Fatalf("row %d = %#v, want bucket and zero count", ordinal, row)
		}
	}
}

func TestExecutorSuppressesEmptyFixedCountFieldGridButKeepsAliasSchema(t *testing.T) {
	t.Parallel()

	first := time.Date(2026, time.July, 22, 12, 0, 0, 0, time.UTC)
	rows := fixedCountFieldTimechartRows(
		[]uint64{0, 0, 0},
		[]uint8{0, 0, 0},
	)
	sink := &fakeSink{}
	if err := mustExecutor(t, &fakeQueryConnection{rows: rows}).Execute(
		context.Background(),
		fixedCountFieldTimechartQuery(first, 3, "eligible_values"),
		sink,
	); err != nil {
		t.Fatalf("Execute: %v", err)
	}

	wantSchema := searchjobs.Schema{Columns: []searchjobs.Column{
		{Name: "_time", Kind: searchjobs.ValueKindTime},
		{Name: "eligible_values", Kind: searchjobs.ValueKindUnsigned},
	}}
	if sink.setCalls != 1 || !reflect.DeepEqual(sink.schema, wantSchema) ||
		len(sink.rows) != 0 || !rows.closed {
		t.Fatalf(
			"empty schema=%#v calls=%d rows=%d closed=%v",
			sink.schema,
			sink.setCalls,
			len(sink.rows),
			rows.closed,
		)
	}
}

func TestExecutorDistinguishesFieldCountNamedCountFromRowCount(t *testing.T) {
	t.Parallel()

	first := time.Date(2026, time.July, 22, 12, 0, 0, 0, time.UTC)
	query := fixedCountFieldTimechartQuery(first, 2, "count")
	rows := fixedCountFieldTimechartRows([]uint64{0, 0}, []uint8{1, 1})
	sink := &fakeSink{}
	if err := mustExecutor(t, &fakeQueryConnection{rows: rows}).Execute(
		context.Background(),
		query,
		sink,
	); err != nil {
		t.Fatalf("Execute field count named count: %v", err)
	}
	if len(sink.rows) != 2 || sink.schema.Columns[1].Name != "count" {
		t.Fatalf("field count named count schema=%#v rows=%d", sink.schema, len(sink.rows))
	}

	query.Timechart.Mode = clickhouse.TimechartModeFixedCount
	connection := &fakeQueryConnection{
		rows: fixedCountFieldTimechartRows([]uint64{0, 0}, []uint8{1, 1}),
	}
	sink = &fakeSink{}
	err := mustExecutor(t, connection).Execute(context.Background(), query, sink)
	if !errors.Is(err, searchjobs.ErrInvalidResult) || connection.query != "" ||
		sink.setCalls != 0 || len(sink.rows) != 0 {
		t.Fatalf(
			"ambiguous row-count mode error=%v query=%q schema=%d rows=%d",
			err,
			connection.query,
			sink.setCalls,
			len(sink.rows),
		)
	}

	// A forged tuple that is internally valid as legacy row-count metadata
	// reaches the database, but the legacy two-column reader must still reject
	// the field-count SQL's private presence column before publishing anything.
	query.Timechart.ValueField = ""
	connection = &fakeQueryConnection{
		rows: fixedCountFieldTimechartRows([]uint64{0, 0}, []uint8{1, 1}),
	}
	sink = &fakeSink{}
	err = mustExecutor(t, connection).Execute(context.Background(), query, sink)
	if !errors.Is(err, searchjobs.ErrInvalidResult) || connection.query == "" ||
		sink.setCalls != 0 || len(sink.rows) != 0 {
		t.Fatalf(
			"self-consistent forged row-count mode error=%v query=%q schema=%d rows=%d",
			err,
			connection.query,
			sink.setCalls,
			len(sink.rows),
		)
	}
}

func TestExecutorRejectsMalformedFixedCountFieldTransportAtomically(t *testing.T) {
	first := time.Date(2026, time.July, 22, 12, 0, 0, 0, time.UTC)
	for _, test := range []struct {
		name        string
		mutate      func(*fakeRows, *clickhouse.CompiledQuery)
		queryIssued bool
	}{
		{
			name: "wrong presence column",
			mutate: func(rows *fakeRows, _ *clickhouse.CompiledQuery) {
				rows.columns[2] = "attacker_presence"
			},
			queryIssued: true,
		},
		{
			name: "nullable presence",
			mutate: func(rows *fakeRows, _ *clickhouse.CompiledQuery) {
				rows.types[2] = fakeColumnType{
					name:         clickhouse.TimechartInputPresentColumn,
					databaseType: "UInt8",
					scanType:     reflect.TypeFor[uint8](),
					nullable:     true,
				}
			},
			queryIssued: true,
		},
		{
			name: "wrong presence width",
			mutate: func(rows *fakeRows, _ *clickhouse.CompiledQuery) {
				rows.types[2] = fakeColumnType{
					name:         clickhouse.TimechartInputPresentColumn,
					databaseType: "UInt64",
					scanType:     reflect.TypeFor[uint64](),
				}
				rows.data[0][2] = uint64(1)
				rows.data[1][2] = uint64(1)
			},
			queryIssued: true,
		},
		{
			name: "presence changes",
			mutate: func(rows *fakeRows, _ *clickhouse.CompiledQuery) {
				rows.data[1][2] = uint8(0)
			},
			queryIssued: true,
		},
		{
			name: "invalid presence flag",
			mutate: func(rows *fakeRows, _ *clickhouse.CompiledQuery) {
				rows.data[0][2] = uint8(2)
				rows.data[1][2] = uint8(2)
			},
			queryIssued: true,
		},
		{
			name: "empty input carries occurrence",
			mutate: func(rows *fakeRows, _ *clickhouse.CompiledQuery) {
				rows.data[0][2] = uint8(0)
				rows.data[1][2] = uint8(0)
			},
			queryIssued: true,
		},
		{
			name: "public alias mismatch",
			mutate: func(_ *fakeRows, query *clickhouse.CompiledQuery) {
				query.OutputFields[1] = "attacker"
			},
		},
		{
			name: "missing alias metadata",
			mutate: func(_ *fakeRows, query *clickhouse.CompiledQuery) {
				query.Timechart.ValueField = ""
			},
		},
		{
			name: "nullable-value policy on count transport",
			mutate: func(_ *fakeRows, query *clickhouse.CompiledQuery) {
				query.Timechart.ValueKind = clickhouse.TimechartValueKindSum
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			rows := fixedCountFieldTimechartRows(
				[]uint64{1, 0},
				[]uint8{1, 1},
			)
			query := fixedCountFieldTimechartQuery(first, 2, "eligible_values")
			test.mutate(rows, &query)
			connection := &fakeQueryConnection{rows: rows}
			sink := &fakeSink{}
			err := mustExecutor(t, connection).Execute(
				context.Background(),
				query,
				sink,
			)
			if !errors.Is(err, searchjobs.ErrInvalidResult) {
				t.Fatalf("Execute error = %v, want ErrInvalidResult", err)
			}
			if got := connection.query != ""; got != test.queryIssued {
				t.Fatalf("query issued = %v, want %v", got, test.queryIssued)
			}
			if sink.setCalls != 0 || len(sink.rows) != 0 {
				t.Fatalf(
					"malformed transport published schema=%d rows=%d",
					sink.setCalls,
					len(sink.rows),
				)
			}
		})
	}
}

func fixedCountFieldTimechartQuery(
	first time.Time,
	bucketCount uint64,
	output string,
) clickhouse.CompiledQuery {
	return clickhouse.CompiledQuery{
		SQL:          "SELECT bounded_fixed_count_field_timechart",
		OutputFields: []string{"_time", output},
		Timechart: &clickhouse.TimechartOutput{
			Mode:          clickhouse.TimechartModeFixedFieldCount,
			FirstBucket:   first,
			Span:          5 * time.Minute,
			BucketCount:   bucketCount,
			MaxSeries:     1,
			MaxLabelBytes: 0,
			ValueField:    output,
			ValueKind:     clickhouse.TimechartValueKindInvalid,
		},
	}
}

func fixedCountFieldTimechartRows(counts []uint64, presence []uint8) *fakeRows {
	if len(counts) != len(presence) {
		panic("fixed count(field) fixture lengths differ")
	}
	rows := &fakeRows{
		columns: []string{
			clickhouse.TimechartOrdinalColumn,
			clickhouse.TimechartCountColumn,
			clickhouse.TimechartInputPresentColumn,
		},
		types: []driver.ColumnType{
			fakeColumnType{
				name:         clickhouse.TimechartOrdinalColumn,
				databaseType: "UInt64",
				scanType:     reflect.TypeFor[uint64](),
			},
			fakeColumnType{
				name:         clickhouse.TimechartCountColumn,
				databaseType: "UInt64",
				scanType:     reflect.TypeFor[uint64](),
			},
			fakeColumnType{
				name:         clickhouse.TimechartInputPresentColumn,
				databaseType: "UInt8",
				scanType:     reflect.TypeFor[uint8](),
			},
		},
		data: make([][]any, len(counts)),
	}
	for index, count := range counts {
		rows.data[index] = []any{uint64(index), count, presence[index]}
	}
	return rows
}
