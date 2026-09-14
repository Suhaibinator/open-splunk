package queryexec

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
	"github.com/Suhaibinator/open-splunk/internal/plan"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
	"github.com/Suhaibinator/open-splunk/internal/searchlimits"
	"github.com/Suhaibinator/open-splunk/internal/spl"
)

func compileSingleStageRowFixture(t *testing.T, source string, buckets int) clickhouse.CompiledQuery {
	t.Helper()
	first := time.Date(2026, time.September, 1, 0, 0, 0, 0, time.UTC)
	parsed, err := spl.Parse("index=target | " + source)
	if err != nil {
		t.Fatal(err)
	}
	visibility := uint64(1)
	logical, err := plan.Build(parsed, plan.Scope{TenantID: "tenant", AuthorizedIndexes: []string{"target"}, Earliest: first, Latest: first.Add(time.Duration(buckets) * time.Second), SearchStart: first, IndexTimeCutoff: first, VisibilityCutoff: &visibility, SearchTimezone: "UTC"})
	if err != nil {
		t.Fatal(err)
	}
	query, err := (clickhouse.Compiler{}).Compile(logical)
	if err != nil {
		t.Fatal(err)
	}
	if query.HasContinuation() || !query.HasTimechartStage() || !query.RequiresAtomicResult() {
		t.Fatal("fixture must be sealed atomic single-stage timechart output")
	}
	return query
}

func singleStageAggregateRows(query clickhouse.CompiledQuery, buckets int) *fakeRows {
	counts := make([]uint64, buckets)
	present := make([]uint8, buckets)
	values := make([]any, buckets)
	wideCounts := make([][]uint64, buckets)
	wideValues := make([][]float64, buckets)
	widePresent := make([][]uint8, buckets)
	for i := range buckets {
		counts[i] = 1
		present[i] = 1
		values[i] = float64(i + 1)
		wideCounts[i] = []uint64{1}
		wideValues[i] = []float64{float64(i + 1)}
		widePresent[i] = []uint8{1}
	}
	switch query.Timechart.Mode {
	case clickhouse.TimechartModeFixedCount:
		return fixedTimechartOrdinalRows(counts)
	case clickhouse.TimechartModeFixedFieldCount:
		return fixedCountFieldTimechartRows(counts, present)
	case clickhouse.TimechartModeFixedValue:
		return fixedValueTimechartRows(values, present)
	case clickhouse.TimechartModeRuntimeWide:
		return timechartOrdinalRows([]string{"0:api"}, wideCounts)
	default:
		return splitValueTimechartRows([]string{"0:api"}, wideValues, widePresent)
	}
}

func TestSingleStageTimechartAdmitsExactPublicRows(t *testing.T) {
	for _, measure := range []string{"count", "count(source)", "sum(value)", "avg(value)", "p95(value)"} {
		for _, split := range []string{"", " BY host"} {
			for _, count := range []int{3, 4} {
				t.Run(fmt.Sprintf("%s%s/%d", measure, split, count), func(t *testing.T) {
					query := compileSingleStageRowFixture(t, "timechart span=1s "+measure+split, count)
					rows := singleStageAggregateRows(query, count)
					connection := &terminalTimechartQueueConnection{rows: []driver.Rows{rows}}
					executor := mustExecutor(t, connection)
					if err := executor.Reconfigure(Config{MaxResultRows: 1}); err != nil {
						t.Fatal(err)
					}
					policy := searchlimits.Default()
					policy.MaxResultRows = 3
					sink := &terminalTimechartSink{}
					err := executor.Execute(searchlimits.WithPolicy(context.Background(), policy), query, sink)
					assertSingleStageRows(t, sink, err, count)
				})
			}
		}
	}
}

func assertSingleStageRows(t *testing.T, sink *terminalTimechartSink, err error, count int) {
	t.Helper()
	if count > 3 {
		if !errors.Is(err, searchjobs.ErrExecutionLimit) {
			t.Fatalf("overflow error=%v", err)
		}
		if sink.compiledCalls != 0 || sink.setCalls != 0 || len(sink.rows) != 0 || len(sink.bounds) != 0 {
			t.Fatalf("overflow published descriptor=%d schema=%d rows=%d bounds=%d", sink.compiledCalls, sink.setCalls, len(sink.rows), len(sink.bounds))
		}
	} else {
		if err != nil {
			t.Fatal(err)
		}
		if sink.setCalls != 1 || len(sink.rows) != count {
			t.Fatalf("schema=%d rows=%d,want%d", sink.setCalls, len(sink.rows), count)
		}
	}
}

func TestSingleStageStaticSuffixRowsAndOrdinaryStreaming(t *testing.T) {
	for _, timechart := range []bool{false, true} {
		for _, admitted := range []bool{false, true} {
			for _, count := range []int{3, 4} {
				t.Run(fmt.Sprintf("timechart_%t/admitted_%t/rows_%d", timechart, admitted, count), func(t *testing.T) {
					query := clickhouse.CompiledQuery{SQL: "SELECT count FROM diagnostic", OutputFields: []string{"count"}}
					if timechart {
						query = compileSingleStageRowFixture(t, "timechart span=1s count | fields - _time", count)
					}
					rows := &fakeRows{columns: []string{"count"}, types: []driver.ColumnType{fakeColumnType{name: "count", databaseType: "UInt64", scanType: reflect.TypeFor[uint64]()}}}
					for range count {
						rows.data = append(rows.data, []any{uint64(1)})
					}
					executor := mustExecutor(t, &terminalTimechartQueueConnection{rows: []driver.Rows{rows}})
					if err := executor.Reconfigure(Config{MaxResultRows: 3}); err != nil {
						t.Fatal(err)
					}
					ctx := context.Background()
					if admitted {
						policy := searchlimits.Default()
						policy.MaxResultRows = 3
						ctx = searchlimits.WithPolicy(ctx, policy)
					}
					sink := &terminalTimechartSink{}
					err := executor.Execute(ctx, query, sink)
					if timechart {
						assertSingleStageRows(t, sink, err, count)
					} else if err != nil || len(sink.rows) != count {
						t.Fatalf("ordinary streaming changed: err=%v rows=%d", err, len(sink.rows))
					}
				})
			}
		}
	}
}

func TestTimechartGridRowLimitCountsOnlyPresentedBuckets(t *testing.T) {
	first := time.Date(2026, time.March, 7, 8, 0, 0, 0, time.UTC)
	// The uneven intervals also exercise calendar boundaries independently of
	// their elapsed width. Admission and publication must include the same rows.
	boundaries := []time.Time{first, first.Add(24 * time.Hour), first.Add(47 * time.Hour), first.Add(71 * time.Hour), first.Add(95 * time.Hour)}
	for _, sparse := range []bool{false, true} {
		for _, partial := range []bool{false, true} {
			t.Run(fmt.Sprintf("sparse_%t/partial_%t", sparse, partial), func(t *testing.T) {
				target := &exactGridTestSink{}
				sink := &timechartGridSink{ResultSink: target, output: clickhouse.TimechartOutput{ExactGrid: true, Calendar: true, Boundaries: boundaries, BucketCount: 4, Continuous: !sparse, IncludePartial: partial, SearchEarliest: first.Add(time.Hour), SearchLatest: boundaries[4].Add(-time.Hour)}, occupancy: &timechartGridRows{present: []uint8{1, 0, 1, 0}}}
				for _, bucket := range boundaries[:4] {
					if err := sink.AddRow([]searchjobs.Value{searchjobs.TimeValue(bucket), searchjobs.UnsignedValue(1)}); err != nil {
						t.Fatal(err)
					}
				}
				visible := uint64(len(target.rows))
				if err := sink.validateRowLimit(context.Background(), 4, visible); err != nil {
					t.Fatalf("exact visible cap %d: %v", visible, err)
				}
				if visible > 1 && !errors.Is(sink.validateRowLimit(context.Background(), 4, visible-1), searchjobs.ErrExecutionLimit) {
					t.Fatal("visible overflow accepted")
				}
			})
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := (&timechartGridSink{}).validateRowLimit(ctx, 0, 3); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled empty check=%v", err)
	}
}

func TestSingleStageTimechartWithoutSeriesPublishesNoRows(t *testing.T) {
	for _, measure := range []string{"count", "sum(value)", "avg(value)", "p95(value)"} {
		t.Run(measure, func(t *testing.T) {
			query := compileSingleStageRowFixture(t, "timechart span=1s "+measure+" BY host", 4)
			var rows *fakeRows
			if query.Timechart.Mode == clickhouse.TimechartModeRuntimeWide {
				rows = timechartOrdinalRows(nil, [][]uint64{{}, {}, {}, {}})
			} else {
				rows = splitValueTimechartRows(nil, [][]float64{{}, {}, {}, {}}, [][]uint8{{}, {}, {}, {}})
			}
			executor := mustExecutor(t, &terminalTimechartQueueConnection{rows: []driver.Rows{rows}})
			policy := searchlimits.Default()
			policy.MaxResultRows = 3
			sink := &terminalTimechartSink{}
			assertSingleStageRows(t, sink, executor.Execute(searchlimits.WithPolicy(context.Background(), policy), query, sink), 0)
		})
	}
}
