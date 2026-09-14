package clickhouse

import (
	"strings"
	"testing"
)

// BenchmarkTimechartCompile is the deterministic before/after compiler
// benchmark for the timechart enhancement. Every case is accepted by the
// baseline and exercises a distinct existing output shape, so the same command
// can be run at the baseline commit and after the enhanced grid, composition,
// series, and result-bound contracts are integrated.
//
// Run a fixed paired sample with:
//
//	go test ./internal/clickhouse -run '^$' \
//	  -bench '^BenchmarkTimechartCompile$' -benchtime=100x -count=5 -benchmem
//
// Parsing and planning happen outside the timed region. The benchmark reports
// the compiled SQL size and asserts that lowering retains one physical events
// source reference; Docker integration tests separately verify ClickHouse's
// physical plan and runtime read counters.
func BenchmarkTimechartCompile(b *testing.B) {
	b.ReportAllocs()
	for _, test := range []struct {
		name   string
		source string
	}{
		{
			name:   "fixed-count",
			source: `index=gradethis | timechart span=5m count`,
		},
		{
			name:   "automatic-count",
			source: `index=gradethis | timechart count`,
		},
		{
			name:   "calendar-count",
			source: `index=gradethis | timechart span=1d count`,
		},
		{
			name:   "split-count",
			source: `index=gradethis | timechart span=5m count BY service`,
		},
		{
			name:   "split-average",
			source: `index=gradethis | timechart span=5m avg(duration_ms) BY service`,
		},
	} {
		logical := buildPlan(b, test.source)
		verified, err := (Compiler{}).Compile(logical)
		if err != nil {
			b.Fatalf("compile %s verification fixture: %v", test.name, err)
		}
		if verified.Timechart == nil || len(verified.SQL) == 0 {
			b.Fatalf("compile %s omitted timechart output", test.name)
		}
		if scans := strings.Count(verified.SQL, `FROM "open_splunk"."events"`); scans != 1 {
			b.Fatalf("compile %s events source references = %d, want 1", test.name, scans)
		}

		b.Run(test.name, func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				compiled, compileErr := (Compiler{}).Compile(logical)
				if compileErr != nil {
					b.Fatalf("compile: %v", compileErr)
				}
				if len(compiled.SQL) != len(verified.SQL) {
					b.Fatalf(
						"nondeterministic SQL size = %d, want %d",
						len(compiled.SQL),
						len(verified.SQL),
					)
				}
			}
			b.StopTimer()
			b.ReportMetric(float64(len(verified.SQL)), "sql-B")
			b.ReportMetric(1, "event-sources")
		})
	}
}
