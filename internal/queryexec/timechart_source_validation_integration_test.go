package queryexec

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
	"github.com/Suhaibinator/open-splunk/internal/plan"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
	"github.com/Suhaibinator/open-splunk/internal/spl"
)

func TestTimechartSplitValidationOutsideGridAgainstClickHouse(t *testing.T) {
	if os.Getenv("OPEN_SPLUNK_CLICKHOUSE_INTEGRATION") != "1" {
		t.Skip("set OPEN_SPLUNK_CLICKHOUSE_INTEGRATION=1")
	}
	earliest := time.Date(2026, 8, 12, 20, 0, 0, 0, time.UTC)
	latest := earliest.Add(time.Hour)
	indexTime := latest.Add(time.Hour)
	ctx, executor := semanticBytesLineageStartClickHouse(t, indexTime, []semanticBytesLineageEvent{
		{id: "invalid", at: earliest.Add(time.Second), host: "NULL", source: "invalid", raw: []byte("x")},
		{id: "collision-1", at: earliest.Add(2 * time.Second), host: "_service", source: "collision", raw: []byte("x")},
		{id: "collision-2", at: earliest.Add(3 * time.Second), host: "VALUE_service", source: "collision", raw: []byte("x")},
		{id: "valid", at: earliest.Add(4 * time.Second), host: "api", source: "valid", raw: []byte("x")},
	})
	compile := func(t *testing.T, source string) clickhouse.CompiledQuery {
		t.Helper()
		parsed, err := spl.Parse(source)
		if err != nil {
			t.Fatal(err)
		}
		visibility := uint64(1)
		logical, err := plan.Build(parsed, plan.Scope{TenantID: "tenant", AuthorizedIndexes: []string{semanticBytesLineageIndex}, Earliest: earliest, Latest: latest, SearchStart: indexTime, IndexTimeCutoff: indexTime, VisibilityCutoff: &visibility, SearchTimezone: "UTC"})
		if err != nil {
			t.Fatal(err)
		}
		compiled, err := (clickhouse.Compiler{}).Compile(logical)
		if err != nil {
			t.Fatal(err)
		}
		return compiled
	}
	for _, span := range []string{"1m", "1d"} {
		for _, kind := range []string{"invalid", "collision"} {
			for _, measure := range []string{"count", "count(missing)", "sum(value)", "avg(value)", "p95(value)", "sum(missing)", "avg(missing)", "p95(missing)"} {
				controls := []string{"cont=false partial=false fixedrange=true"}
				if measure == "count" || measure == "count(missing)" {
					controls = nil
					for _, cont := range []bool{false, true} {
						for _, partial := range []bool{false, true} {
							for _, fixed := range []bool{false, true} {
								controls = append(controls, fmt.Sprintf("cont=%t partial=%t fixedrange=%t", cont, partial, fixed))
							}
						}
					}
				}
				for _, control := range controls {
					t.Run(span+"/"+kind+"/"+measure+"/"+control, func(t *testing.T) {
						source := fmt.Sprintf(`index=%s source="%s" | table _time host | bin _time span=1w | eval value=1 | timechart span=%s %s %s BY host limit=0 | head 1`, semanticBytesLineageIndex, kind, span, control, measure)
						compiled := compile(t, source)
						sink := &compositionSink{}
						callCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
						defer cancel()
						err := executor.Execute(callCtx, compiled, sink)
						if !errors.Is(err, searchjobs.ErrUnsupportedValue) || sink.setCalls != 0 || len(sink.rows) != 0 {
							t.Fatalf("clipped invalid source published: err=%v schemas=%d rows=%d", err, sink.setCalls, len(sink.rows))
						}
						if kind == "invalid" && measure == "count" && control == "cont=false partial=false fixedrange=true" {
							assertTimechartWorkOneRead(t, ctx, executor, compiled)
						}
					})
				}
			}
		}
		for _, selection := range []string{"valid", "absent"} {
			t.Run(span+"/valid-or-empty/"+selection, func(t *testing.T) {
				source := fmt.Sprintf(`index=%s source="%s" | table _time host | bin _time span=1w | timechart span=%s cont=false partial=false count BY host limit=0 | head 1`, semanticBytesLineageIndex, selection, span)
				sink := &compositionSink{}
				if err := executor.Execute(ctx, compile(t, source), sink); err != nil || len(sink.rows) != 0 {
					t.Fatalf("valid clipped/empty input: err=%v rows=%d", err, len(sink.rows))
				}
			})
		}
	}
}
