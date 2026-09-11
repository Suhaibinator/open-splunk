package queryexec

import (
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
	"github.com/Suhaibinator/open-splunk/internal/plan"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
	"github.com/Suhaibinator/open-splunk/internal/spl"
)

func TestTimechartCumulativeMVExpandWorkAgainstClickHouse(t *testing.T) {
	if os.Getenv("OPEN_SPLUNK_CLICKHOUSE_INTEGRATION") != "1" {
		t.Skip("set OPEN_SPLUNK_CLICKHOUSE_INTEGRATION=1")
	}
	earliest := time.Date(2026, 8, 12, 20, 0, 0, 0, time.UTC)
	events := make([]semanticBytesLineageEvent, 8)
	for i := range events {
		events[i] = semanticBytesLineageEvent{id: fmt.Sprintf("work-%d", i), at: earliest.Add(time.Duration(i) * time.Second), host: "api", raw: []byte("x")}
	}
	ctx, executor := semanticBytesLineageStartClickHouse(t, earliest.Add(time.Hour), events)
	payload := strings.TrimSuffix(strings.Repeat("x,", 1000), ",")
	for _, chart := range []string{`timechart span=1s count BY host`, `timechart span=1s fixedrange=false count BY host`, `timechart span=1s count`} {
		for _, limit := range []int{875, 1000} {
			t.Run(fmt.Sprintf("%s/%d", chart, limit), func(t *testing.T) {
				source := fmt.Sprintf(`index=%s | eval payload="%s" | makemv delim="," payload | mvexpand payload | %s | eval payload="%s" | makemv delim="," payload | mvexpand payload limit=%d | head 1`, semanticBytesLineageIndex, payload, chart, payload, limit)
				parsed, err := spl.Parse(source)
				if err != nil {
					t.Fatal(err)
				}
				visibility := uint64(1)
				logical, err := plan.Build(parsed, plan.Scope{TenantID: "tenant", AuthorizedIndexes: []string{semanticBytesLineageIndex}, Earliest: earliest, Latest: earliest.Add(8 * time.Second), SearchStart: earliest.Add(time.Hour), IndexTimeCutoff: earliest.Add(time.Hour), VisibilityCutoff: &visibility, SearchTimezone: "UTC"})
				if err != nil {
					t.Fatal(err)
				}
				compiled, err := (clickhouse.Compiler{}).Compile(logical)
				if err != nil {
					t.Fatal(err)
				}
				sink := &compositionSink{}
				err = executor.Execute(ctx, compiled, sink)
				if limit == 1000 {
					if !errors.Is(err, searchjobs.ErrExecutionLimit) {
						t.Fatalf("8000+8000 expansion work: error=%v rows=%d", err, len(sink.rows))
					}
					if len(sink.rows) != 0 || len(sink.schema.Columns) != 0 {
						t.Fatal("over-budget expansion published results")
					}
				} else if err != nil || len(sink.rows) != 1 {
					t.Fatalf("8000+7000 work exact boundary: error=%v rows=%d", err, len(sink.rows))
				}
			})
		}
	}
}
