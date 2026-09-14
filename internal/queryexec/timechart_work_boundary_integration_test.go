package queryexec

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	clickhousedriver "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
	"github.com/Suhaibinator/open-splunk/internal/plan"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
	"github.com/Suhaibinator/open-splunk/internal/spl"
)

func TestTimechartWorkBoundaryAndEmptyInputsAgainstClickHouse(t *testing.T) {
	if os.Getenv("OPEN_SPLUNK_CLICKHOUSE_INTEGRATION") != "1" {
		t.Skip("set OPEN_SPLUNK_CLICKHOUSE_INTEGRATION=1")
	}
	first := time.Date(2026, 8, 12, 20, 0, 0, 0, time.UTC)
	events := make([]semanticBytesLineageEvent, 9)
	for i := range 8 {
		events[i] = semanticBytesLineageEvent{id: fmt.Sprintf("boundary-%d", i), at: first.Add(time.Duration(i) * time.Second), host: "api", raw: []byte("x")}
	}
	// One extra expanded member shares an existing bucket. The second expansion
	// therefore contributes the same 8*875 rows at both query-wide boundaries.
	events[8] = semanticBytesLineageEvent{id: "boundary-extra", at: first, host: "tiny", raw: []byte("x")}
	ctx, executor := semanticBytesLineageStartClickHouse(t, first.Add(time.Hour), events)
	payload := strings.TrimSuffix(strings.Repeat("x,", 1000), ",")
	for _, chart := range []string{
		`timechart span=1s count`,
		`timechart span=1s count BY host`,
		`timechart span=1s fixedrange=false count`,
		`timechart span=1s fixedrange=false count BY host`,
	} {
		for _, over := range []bool{false, true} {
			t.Run(fmt.Sprintf("boundary/%s/over=%t", chart, over), func(t *testing.T) {
				selection := ` | search host!="tiny"`
				if over {
					selection = ""
				}
				source := fmt.Sprintf(`index=%s%s | eval payload=if(host="tiny","x","%s") | makemv delim="," payload | mvexpand payload | %s | eval payload="%s" | makemv delim="," payload | mvexpand payload limit=875 | head 1`, semanticBytesLineageIndex, selection, payload, chart, payload)
				compiled := compileTimechartWorkBoundary(t, source, first)
				sink := &compositionSink{}
				err := executor.Execute(ctx, compiled, sink)
				if over {
					if !errors.Is(err, searchjobs.ErrExecutionLimit) || sink.setCalls != 0 || len(sink.rows) != 0 {
						t.Fatalf("15001 rows must fail atomically: err=%v schema=%d rows=%d", err, sink.setCalls, len(sink.rows))
					}
				} else {
					if err != nil || len(sink.rows) != 1 {
						t.Fatalf("15000 rows must succeed: err=%v rows=%d", err, len(sink.rows))
					}
					assertTimechartWorkOneRead(t, ctx, executor, compiled)
				}
			})
		}
	}
	for _, testCase := range []struct {
		name, before, chart string
		work                uint64
		emptySource         bool
	}{
		{"filtered static", ` | where host="absent"`, `timechart span=1s cont=false count`, 8000, false},
		{"filtered split", ` | where host="absent"`, `timechart span=1s cont=false count BY host`, 8000, false},
		{"filtered observed static", ` | where host="absent"`, `timechart span=1s fixedrange=false count`, 8000, false},
		{"filtered observed split", ` | where host="absent"`, `timechart span=1s fixedrange=false count BY host`, 8000, false},
		{"partial static", "", `timechart span=1h partial=false count`, 8000, false},
		{"partial split", "", `timechart span=1h partial=false count BY host`, 8000, false},
		{"partial observed static", "", `timechart span=1h partial=false fixedrange=false count`, 8000, false},
		{"partial observed split", "", `timechart span=1h partial=false fixedrange=false count BY host`, 8000, false},
		{"empty observed source", "", `timechart span=1s fixedrange=false count`, 0, true},
		{"empty observed split source", "", `timechart span=1s fixedrange=false count BY host`, 0, true},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			predicate := `host!="tiny"`
			if testCase.emptySource {
				predicate = `host="absent"`
			}
			source := fmt.Sprintf(`index=%s | search %s | eval payload="%s" | makemv delim="," payload | mvexpand payload%s | %s`, semanticBytesLineageIndex, predicate, payload, testCase.before, testCase.chart)
			compiled := compileTimechartWorkBoundary(t, source, first)
			sink := &timechartWorkReceiptSink{}
			if err := executor.Execute(ctx, compiled, sink); err != nil {
				t.Fatal(err)
			}
			if len(sink.rows) != 0 || sink.workCalls != 1 || sink.work != testCase.work {
				t.Fatalf("empty output lost completed work: rows=%d receipt calls=%d work=%d want=%d", len(sink.rows), sink.workCalls, sink.work, testCase.work)
			}
			assertTimechartWorkOneRead(t, ctx, executor, compiled)
		})
	}
}

type timechartWorkReceiptSink struct {
	compositionSink
	work      uint64
	workCalls int
}

func (sink *timechartWorkReceiptSink) SetTimechartWork(work uint64) error {
	sink.work, sink.workCalls = work, sink.workCalls+1
	return nil
}

func compileTimechartWorkBoundary(t *testing.T, source string, first time.Time) clickhouse.CompiledQuery {
	t.Helper()
	parsed, err := spl.Parse(source)
	if err != nil {
		t.Fatal(err)
	}
	visibility := uint64(1)
	logical, err := plan.Build(parsed, plan.Scope{TenantID: "tenant", AuthorizedIndexes: []string{semanticBytesLineageIndex}, Earliest: first, Latest: first.Add(8 * time.Second), SearchStart: first.Add(time.Hour), IndexTimeCutoff: first.Add(time.Hour), VisibilityCutoff: &visibility, SearchTimezone: "UTC"})
	if err != nil {
		t.Fatal(err)
	}
	compiled, err := (clickhouse.Compiler{}).Compile(logical)
	if err != nil {
		t.Fatal(err)
	}
	return compiled
}

func assertTimechartWorkOneRead(t *testing.T, ctx context.Context, executor *Executor, compiled clickhouse.CompiledQuery) {
	t.Helper()
	settings, err := executor.settingsForContext(ctx, compiled)
	if err != nil {
		t.Fatal(err)
	}
	rows, err := executor.connection.Query(clickhousedriver.Context(ctx, clickhousedriver.WithSettings(settings)), "EXPLAIN PLAN json=1,description=0,indexes=1,actions=0,header=1 "+compiled.SQL, compiled.Args...)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rows.Close() }()
	var parts []string
	for rows.Next() {
		var part string
		if err := rows.Scan(&part); err != nil {
			t.Fatal(err)
		}
		parts = append(parts, part)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if err := rows.Close(); err != nil {
		t.Fatal(err)
	}
	physical, err := parseExplainPlanText(ctx, strings.Join(parts, "\n"))
	if err != nil {
		t.Fatal(err)
	}
	if len(physical.Reads) != 1 {
		t.Fatalf("work receipt must reuse materialized input: physical reads=%d want=1", len(physical.Reads))
	}
}
