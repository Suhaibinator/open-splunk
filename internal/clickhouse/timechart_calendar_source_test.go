package clickhouse

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

func TestCalendarSourceGridIsBoundedSealedAndDetached(t *testing.T) {
	for _, count := range []int{100, 10000} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			scope := testChartScope()
			scope.Earliest = time.Date(1990, 1, 1, 0, 0, 0, 0, time.UTC)
			scope.Latest = scope.Earliest.AddDate(0, 0, count)
			scope.SearchTimezone = "UTC"
			compiled := compileSPLWithScope(t, `index=gradethis | timechart span=1d count`, scope)
			if compiled.Timechart.BucketCount != uint64(count) || !compiled.Timechart.ExactGrid {
				t.Fatalf("grid=%+v", compiled.Timechart)
			}
			if strings.Count(compiled.SQL, "ASOF LEFT JOIN") != 1 || strings.Contains(compiled.SQL, "arrayLastIndex") || strings.Contains(compiled.SQL, "arrayFirstIndex") || strings.Count(compiled.SQL, `FROM "open_splunk"."events"`) != 1 {
				t.Fatalf("unbounded or repeated source SQL: %s", compiled.SQL)
			}
			var gridArgs []int
			for i, arg := range compiled.Args {
				if ticks, ok := arg.([]int64); ok {
					if len(ticks) != count {
						t.Fatalf("grid arg length=%d want=%d", len(ticks), count)
					}
					gridArgs = append(gridArgs, i)
				}
			}
			if len(gridArgs) != 2 {
				t.Fatalf("lookup and output grids=%v", gridArgs)
			}
			retained, ok := compiled.RetainedBytes()
			if !ok || retained < uint64(len(compiled.SQL)+count*16) {
				t.Fatalf("grid args not charged: %d valid=%v", retained, ok)
			}
			clone, ok := compiled.CloneForExecution()
			if !ok {
				t.Fatal("clone failed")
			}
			clone.Args[gridArgs[0]].([]int64)[0]++
			if clone.HasValidExecutionSeal() || !compiled.HasValidExecutionSeal() || compiled.Args[gridArgs[0]].([]int64)[0] != scope.Earliest.UnixNano() {
				t.Fatal("lookup argument is unsealed or aliases original")
			}
		})
	}
}

func BenchmarkTimechartCalendarCompileGrid(b *testing.B) {
	for _, count := range []int{100, 10000} {
		b.Run(fmt.Sprint(count), func(b *testing.B) {
			scope := testChartScope()
			scope.Earliest = time.Date(1990, 1, 1, 0, 0, 0, 0, time.UTC)
			scope.Latest = scope.Earliest.AddDate(0, 0, count)
			scope.SearchTimezone = "UTC"
			logical := buildPlanWithScope(b, `index=gradethis | timechart span=1d count`, scope)
			b.ReportAllocs()
			for b.Loop() {
				compiled, err := (Compiler{}).Compile(logical)
				if err != nil {
					b.Fatal(err)
				}
				b.ReportMetric(float64(len(compiled.SQL)), "SQL-bytes")
			}
		})
	}
}
