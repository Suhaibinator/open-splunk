package queryexec

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
)

func TestExecutorPublishesStatsDelimiterPresentationWithoutFlatteningCell(t *testing.T) {
	t.Parallel()
	if clickhouse.MaximumResultFieldFlatDelimiterBytes !=
		searchjobs.MaximumFlatMultivalueDelimiterBytes {
		t.Fatal("compiler and search-job delimiter bounds diverged")
	}

	rows := &fakeRows{
		columns: []string{"users"},
		types: []driver.ColumnType{fakeColumnType{
			name:         "users",
			databaseType: "Array(String)",
			scanType:     reflect.TypeFor[[]string](),
		}},
		data: [][]any{{[]string{"alice", "bob"}}},
	}
	query := clickhouse.CompiledQuery{
		SQL:          "SELECT users",
		OutputFields: []string{"users"},
		OutputPresentations: []clickhouse.ResultFieldPresentation{{
			HasFlatMultivalueDelimiter: true,
		}},
	}
	sink := &fakeSink{}
	if err := mustExecutor(t, &fakeQueryConnection{rows: rows}).Execute(
		context.Background(), query, sink,
	); err != nil {
		t.Fatal(err)
	}
	if len(sink.schema.Columns) != 1 ||
		!sink.schema.Columns[0].HasFlatMultivalueDelimiter ||
		sink.schema.Columns[0].FlatMultivalueDelimiter != "" {
		t.Fatalf("schema = %#v", sink.schema)
	}
	if len(sink.rows) != 1 || len(sink.rows[0]) != 1 ||
		sink.rows[0][0].Kind() != searchjobs.ValueKindList {
		t.Fatalf("typed rows = %#v", sink.rows)
	}
}

func TestExecutorPublishesAuthenticatedStatsSparklinePresentation(t *testing.T) {
	t.Parallel()
	rows := &fakeRows{
		columns: []string{"trend"},
		types: []driver.ColumnType{fakeColumnType{
			name: "trend", databaseType: "Array(String)", scanType: reflect.TypeFor[[]string](),
		}},
		data: [][]any{{[]string{"##__SPARKLINE__##", "1", "2"}}},
	}
	query := clickhouse.CompiledQuery{
		SQL: "SELECT trend", OutputFields: []string{"trend"},
		OutputPresentations: []clickhouse.ResultFieldPresentation{{StatsSparkline: true}},
	}
	sink := &fakeSink{}
	if err := mustExecutor(t, &fakeQueryConnection{rows: rows}).Execute(
		context.Background(), query, sink,
	); err != nil {
		t.Fatal(err)
	}
	if len(sink.schema.Columns) != 1 || !sink.schema.Columns[0].StatsSparkline ||
		sink.schema.Columns[0].HasFlatMultivalueDelimiter {
		t.Fatalf("schema = %#v", sink.schema)
	}
}

func TestExecutorRejectsStatsDelimiterPresentationTampering(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name  string
		query clickhouse.CompiledQuery
		rows  *fakeRows
	}{
		{
			name: "ordinal length",
			query: clickhouse.CompiledQuery{
				SQL:                 "SELECT users",
				OutputFields:        []string{"users"},
				OutputPresentations: []clickhouse.ResultFieldPresentation{{}, {}},
			},
		},
		{
			name: "absent with payload",
			query: clickhouse.CompiledQuery{
				SQL:          "SELECT users",
				OutputFields: []string{"users"},
				OutputPresentations: []clickhouse.ResultFieldPresentation{{
					FlatMultivalueDelimiter: ",",
				}},
			},
		},
		{
			name: "non multivalue column",
			query: clickhouse.CompiledQuery{
				SQL:          "SELECT users",
				OutputFields: []string{"users"},
				OutputPresentations: []clickhouse.ResultFieldPresentation{{
					FlatMultivalueDelimiter:    ",",
					HasFlatMultivalueDelimiter: true,
				}},
			},
			rows: &fakeRows{
				columns: []string{"users"},
				types: []driver.ColumnType{fakeColumnType{
					name:         "users",
					databaseType: "String",
					scanType:     reflect.TypeFor[string](),
				}},
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			connection := &fakeQueryConnection{rows: test.rows}
			err := mustExecutor(t, connection).Execute(
				context.Background(), test.query, &fakeSink{},
			)
			if !errors.Is(err, searchjobs.ErrInvalidResult) {
				t.Fatalf("Execute() error = %v, want ErrInvalidResult", err)
			}
		})
	}
}
