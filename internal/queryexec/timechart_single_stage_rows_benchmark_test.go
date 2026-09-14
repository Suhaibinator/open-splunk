package queryexec

import (
	"fmt"
	"reflect"
	"slices"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
	"github.com/Suhaibinator/open-splunk/internal/indexread"
	"github.com/Suhaibinator/open-splunk/internal/plan"
	"github.com/Suhaibinator/open-splunk/internal/searchlimits"
	"github.com/Suhaibinator/open-splunk/internal/spl"
)

// BenchmarkTimechartSingleStageRowLimit measures admitted row validation on a
// sealed terminal timechart. Compilation and fixture construction stay outside
// the timed region. The sparse cases retain a dense native grid while
// cont=false publishes one row per ten buckets.
func BenchmarkTimechartSingleStageRowLimit(b *testing.B) {
	for _, bucketCount := range []int{100, 1_000} {
		for _, sparse := range []bool{false, true} {
			mode := "dense"
			source := "timechart span=1s count BY host limit=10"
			if sparse {
				mode = "cont-false-sparse"
				source = "timechart span=1s cont=false count BY host limit=10"
			}
			name := fmt.Sprintf("buckets-%05d/%s", bucketCount, mode)
			query := compileSingleStageRowLimitBenchmark(b, source, bucketCount)
			rows, visibleRows := singleStageRowLimitBenchmarkRows(b, query, bucketCount, sparse)
			connection := &benchmarkRowsConnection{template: rows}
			settings, err := querySettings(Config{})
			if err != nil {
				b.Fatalf("create %s query settings: %v", name, err)
			}
			executor := &Executor{
				connection:                connection,
				settings:                  mustValidatedSettings(b, settings),
				expandTimechartGroupLimit: true,
				newQueryID:                func() (string, error) { return "timechart-row-limit-benchmark", nil },
				readAdmission:             indexread.UnfencedAdmission{},
			}
			policy := searchlimits.Default()
			policy.MaxResultRows = uint64(visibleRows)

			b.Run(name, func(b *testing.B) {
				ctx := searchlimits.WithPolicy(b.Context(), policy)
				b.ReportAllocs()
				b.ResetTimer()
				for range b.N {
					sink := &benchmarkTimechartSink{wantColumns: 2, wantRows: visibleRows}
					if executeErr := executor.Execute(ctx, query, sink); executeErr != nil {
						b.Fatalf("execute: %v", executeErr)
					}
					if sink.schemaCalls != 1 || sink.rows != visibleRows {
						b.Fatalf("published schema/rows = %d/%d, want 1/%d", sink.schemaCalls, sink.rows, visibleRows)
					}
				}
				b.StopTimer()
				b.ReportMetric(float64(bucketCount), "native-rows/op")
				b.ReportMetric(float64(visibleRows), "published-rows/op")
			})
		}
	}
}

func compileSingleStageRowLimitBenchmark(b *testing.B, source string, bucketCount int) clickhouse.CompiledQuery {
	b.Helper()
	first := time.Date(2026, time.September, 1, 0, 0, 0, 0, time.UTC)
	parsed, err := spl.Parse("index=target | " + source)
	if err != nil {
		b.Fatal(err)
	}
	visibility := uint64(1)
	logical, err := plan.Build(parsed, plan.Scope{
		TenantID:          "tenant",
		AuthorizedIndexes: []string{"target"},
		Earliest:          first,
		Latest:            first.Add(time.Duration(bucketCount) * time.Second),
		SearchStart:       first,
		IndexTimeCutoff:   first,
		VisibilityCutoff:  &visibility,
		SearchTimezone:    "UTC",
	})
	if err != nil {
		b.Fatal(err)
	}
	query, err := (clickhouse.Compiler{}).Compile(logical)
	if err != nil {
		b.Fatal(err)
	}
	if query.HasContinuation() || !query.HasTimechartStage() || !query.HasValidExecutionSeal() || !query.RequiresAtomicResult() {
		b.Fatal("benchmark requires a sealed atomic single-stage timechart")
	}
	return query
}

func singleStageRowLimitBenchmarkRows(
	b *testing.B,
	query clickhouse.CompiledQuery,
	bucketCount int,
	sparse bool,
) (*fakeRows, int) {
	b.Helper()
	if query.Timechart == nil {
		b.Fatal("compiled benchmark has no timechart descriptor")
	}
	counts := make([][]uint64, bucketCount)
	visibleRows := 0
	for bucket := range bucketCount {
		present := !sparse || bucket%10 == 0
		if present {
			counts[bucket] = []uint64{1}
			visibleRows++
		} else {
			counts[bucket] = []uint64{0}
		}
	}
	rows := timechartOrdinalRows([]string{"0:api"}, counts)
	if !query.Timechart.ExactGrid {
		if sparse {
			b.Fatal("cont=false benchmark must use the exact grid transport")
		}
		return rows, visibleRows
	}
	if len(query.Timechart.Boundaries) != bucketCount+1 {
		b.Fatalf("sealed boundaries = %d, want %d", len(query.Timechart.Boundaries), bucketCount+1)
	}
	rows.columns = slices.Insert(rows.columns, 1,
		clickhouse.TimechartBucketColumn,
		clickhouse.TimechartBucketPresentColumn,
	)
	rows.types = slices.Insert(rows.types, 1,
		driver.ColumnType(fakeColumnType{name: clickhouse.TimechartBucketColumn, databaseType: "DateTime64(9, 'UTC')", scanType: reflect.TypeFor[time.Time]()}),
		driver.ColumnType(fakeColumnType{name: clickhouse.TimechartBucketPresentColumn, databaseType: "UInt8", scanType: reflect.TypeFor[uint8]()}),
	)
	for bucket := range bucketCount {
		present := uint8(0)
		if !sparse || bucket%10 == 0 {
			present = 1
		}
		rows.data[bucket] = slices.Insert(
			rows.data[bucket],
			1,
			any(query.Timechart.Boundaries[bucket]),
			any(present),
		)
	}
	return rows, visibleRows
}
