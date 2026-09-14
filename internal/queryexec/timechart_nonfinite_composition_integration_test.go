package queryexec

import (
	"fmt"
	"math"
	"os"
	"testing"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
	"github.com/Suhaibinator/open-splunk/internal/plan"
	"github.com/Suhaibinator/open-splunk/internal/spl"
)

func TestTimechartNonfiniteCompositionAgainstClickHouse(t *testing.T) {
	if os.Getenv("OPEN_SPLUNK_CLICKHOUSE_INTEGRATION") != "1" {
		t.Skip("set OPEN_SPLUNK_CLICKHOUSE_INTEGRATION=1")
	}
	first := time.Date(2026, 8, 12, 20, 0, 0, 0, time.UTC)
	latest := first.Add(time.Second)
	indexTime := latest.Add(time.Hour)
	var events []semanticBytesLineageEvent
	for _, source := range []string{"positive", "negative", "nan"} {
		hosts := []string{"api"}
		if source == "nan" {
			hosts = []string{"alpha", "beta", "gamma"}
		}
		for _, host := range hosts {
			for i := range 2 {
				events = append(events, semanticBytesLineageEvent{id: fmt.Sprintf("%s-%s-%d", source, host, i), at: first.Add(time.Duration(i+1) * 100 * time.Millisecond), host: host, source: source, raw: []byte("x")})
			}
		}
	}
	ctx, executor := semanticBytesLineageStartClickHouse(t, indexTime, events)
	compile := func(t *testing.T, source string) clickhouse.CompiledQuery {
		t.Helper()
		query, err := spl.Parse("index=" + semanticBytesLineageIndex + " " + source)
		if err != nil {
			t.Fatal(err)
		}
		visibility := uint64(1)
		logical, err := plan.Build(query, plan.Scope{TenantID: "tenant", AuthorizedIndexes: []string{semanticBytesLineageIndex}, Earliest: first, Latest: latest, SearchStart: indexTime, IndexTimeCutoff: indexTime, VisibilityCutoff: &visibility, SearchTimezone: "UTC"})
		if err != nil {
			t.Fatal(err)
		}
		compiled, err := (clickhouse.Compiler{}).Compile(logical)
		if err != nil {
			t.Fatal(err)
		}
		return compiled
	}
	for _, measure := range []string{"sum", "avg"} {
		for _, split := range []bool{false, true} {
			field, clause := "total", ""
			if split {
				field, clause = "api", " BY host limit=0"
			}
			for _, sign := range []int{-1, 1} {
				selection, literal := "positive", "1e308"
				if sign < 0 {
					selection, literal = "negative", "-1e308"
				}
				base := fmt.Sprintf(`source="%s" | eval metric=%s | timechart span=1s %s(metric) AS total%s`, selection, literal, measure, clause)
				for _, suffix := range []string{"", " | head 1", " | table _time " + field, " | fields _time " + field, " | where isnotnull(" + field + ") | head 1"} {
					t.Run(base+suffix, func(t *testing.T) {
						sink := &compositionSink{}
						if err := executor.Execute(ctx, compile(t, base+suffix), sink); err != nil {
							t.Fatal(err)
						}
						if len(sink.rows) != 1 || len(sink.rows[0]) != 2 {
							t.Fatalf("rows=%v", sink.rows)
						}
						if at, ok := sink.rows[0][0].Time(); !ok || !at.Equal(first) || len(sink.bounds) != 1 {
							t.Fatalf("identity changed timestamp or bounds: at=%v valid=%v bounds=%v", at, ok, sink.bounds)
						}
						got, ok := sink.rows[0][1].Double()
						if !ok || !math.IsInf(got, sign) {
							t.Fatalf("value=%v valid=%v want Inf(%d)", got, ok, sign)
						}
					})
				}
				for _, suffix := range []string{" | where " + field + "=0 | head 1", " | timechart span=1s count"} {
					t.Run(base+suffix, func(t *testing.T) {
						sink := &compositionSink{}
						if err := executor.Execute(ctx, compile(t, base+suffix), sink); err != nil {
							t.Fatal(err)
						}
						if suffix == " | timechart span=1s count" {
							if len(sink.rows) != 1 {
								t.Fatalf("rows=%d", len(sink.rows))
							}
							count, ok := sink.rows[0][1].Unsigned()
							if !ok || count != 1 {
								t.Fatalf("count=%d valid=%v", count, ok)
							}
						} else if len(sink.rows) != 0 {
							t.Fatalf("filtered rows=%d", len(sink.rows))
						}
					})
				}
			}
		}
		// Two overflowed groups collapse into OTHER as +Inf + -Inf, a
		// legitimate NaN result; SQL arithmetic need not preserve its payload.
		base := fmt.Sprintf(`source="nan" | eval metric=if(host="gamma",-1e308,1e308) | timechart span=1s %s(metric) BY host limit=1`, measure)
		for _, suffix := range []string{"", " | head 1", " | table _time OTHER"} {
			t.Run(base+suffix, func(t *testing.T) {
				sink := &compositionSink{}
				if err := executor.Execute(ctx, compile(t, base+suffix), sink); err != nil {
					t.Fatal(err)
				}
				if len(sink.rows) != 1 {
					t.Fatalf("rows=%d", len(sink.rows))
				}
				index := -1
				for i, column := range sink.schema.Columns {
					if column.Name == "OTHER" {
						index = i
					}
				}
				if index < 0 {
					t.Fatalf("schema=%+v", sink.schema)
				}
				got, ok := sink.rows[0][index].Double()
				if !ok || !math.IsNaN(got) {
					t.Fatalf("OTHER=%v valid=%v, want NaN", got, ok)
				}
			})
		}
	}
}
