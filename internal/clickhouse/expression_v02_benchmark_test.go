package clickhouse

import (
	"fmt"
	"strings"
	"testing"
)

// BenchmarkExpressionV02Compile is the deterministic compiler-side scaling
// gate for the public expression ceilings. Parsing and planning happen outside
// the timed region so this benchmark isolates lowering, validation, sealing,
// and SQL construction. The companion SPL benchmark measures parsing.
//
// Run the fixed diagnostic sample with:
//
//	go test ./internal/clickhouse -run '^$' \
//	  -bench '^BenchmarkExpressionV02Compile$' -benchtime=10x -count=3 -benchmem
func BenchmarkExpressionV02Compile(b *testing.B) {
	b.ReportAllocs()
	for _, family := range []struct {
		name   string
		counts []int
		source func(int) string
	}{
		{
			name:   "fixed-arithmetic",
			counts: []int{1, 32, 128, 256},
			source: func(operators int) string {
				return expressionV02ArithmeticBenchmarkSource("severity", operators)
			},
		},
		{
			name:   "dynamic-arithmetic",
			counts: []int{1, 32, 128, 256},
			source: func(operators int) string {
				return expressionV02ArithmeticBenchmarkSource("duration_ms", operators)
			},
		},
		{
			name:   "membership",
			counts: []int{1, 8, 16, 32},
			source: expressionV02MembershipBenchmarkSource,
		},
	} {
		for _, count := range family.counts {
			name := fmt.Sprintf("%s/%03d", family.name, count)
			logical := buildPlan(b, family.source(count))
			compiled, err := (Compiler{}).Compile(logical)
			if err != nil {
				b.Fatalf("compile %s verification fixture: %v", name, err)
			}
			if !compiled.RequiresAtomicResult() || len(compiled.SQL) == 0 {
				b.Fatalf("compile %s omitted expression execution authority", name)
			}
			b.Run(name, func(b *testing.B) {
				b.ReportAllocs()
				b.ResetTimer()
				for range b.N {
					result, compileErr := (Compiler{}).Compile(logical)
					if compileErr != nil {
						b.Fatalf("compile: %v", compileErr)
					}
					if len(result.SQL) != len(compiled.SQL) {
						b.Fatalf("nondeterministic SQL size = %d, want %d", len(result.SQL), len(compiled.SQL))
					}
				}
				b.StopTimer()
				b.ReportMetric(float64(len(compiled.SQL)), "sql-B")
			})
		}
	}
}

func expressionV02ArithmeticBenchmarkSource(field string, operators int) string {
	var expression strings.Builder
	expression.WriteString(field)
	for index := 0; index < operators; index++ {
		expression.WriteString("+1")
	}
	return "index=gradethis | eval result=" + expression.String() + " | table result"
}

func expressionV02MembershipBenchmarkSource(candidates int) string {
	var source strings.Builder
	source.WriteString("index=gradethis | where status IN (")
	for index := 0; index < candidates; index++ {
		if index > 0 {
			source.WriteByte(',')
		}
		fmt.Fprintf(&source, "%d", index)
	}
	source.WriteString(") | table status")
	return source.String()
}
