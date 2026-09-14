package queryexec

import (
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
	"github.com/Suhaibinator/open-splunk/internal/plan"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
	"github.com/Suhaibinator/open-splunk/internal/searchlimits"
	"github.com/Suhaibinator/open-splunk/internal/spl"
)

func TestTimechartAdmittedResultRowsAgainstClickHouse(t *testing.T) {
	if os.Getenv("OPEN_SPLUNK_CLICKHOUSE_INTEGRATION") != "1" {
		t.Skip("set OPEN_SPLUNK_CLICKHOUSE_INTEGRATION=1")
	}
	earliest := time.Date(2026, time.September, 1, 0, 0, 0, 0, time.UTC)
	latest := earliest.Add(3 * time.Second)
	indexTime := latest.Add(time.Hour)
	events := make([]semanticBytesLineageEvent, 0, 7)
	for index := range 5 {
		events = append(events, semanticBytesLineageEvent{
			id:     fmt.Sprintf("admitted-burst-%d", index),
			at:     earliest.Add(time.Duration(index+1) * 100 * time.Millisecond),
			host:   "api",
			source: "burst",
			raw:    []byte("x"),
		})
	}
	for index := 1; index <= 2; index++ {
		events = append(events, semanticBytesLineageEvent{
			id:     fmt.Sprintf("admitted-range-%d", index),
			at:     earliest.Add(time.Duration(index)*time.Second + 100*time.Millisecond),
			host:   "api",
			source: "range",
			raw:    []byte("x"),
		})
	}
	baseContext, executor := semanticBytesLineageStartClickHouse(t, indexTime, events)
	policy := searchlimits.Default()
	policy.MaxResultRows = 3
	admittedContext := searchlimits.WithPolicy(baseContext, policy)

	for _, chart := range []struct {
		name   string
		clause string
	}{
		{name: "fixed", clause: `timechart span=1s count BY host`},
		{name: "observed", clause: `timechart span=1s fixedrange=false cont=false count BY host`},
	} {
		t.Run(chart.name, func(t *testing.T) {
			t.Run("exact admitted cap ignores lower live cap", func(t *testing.T) {
				if err := executor.Reconfigure(Config{MaxResultRows: 1}); err != nil {
					t.Fatal(err)
				}
				compiled := compileAdmittedRowsTimechart(
					t,
					chart.clause+` | head 1 | eval member=split("a,b,c", ",") | mvexpand member | table member`,
					earliest,
					latest,
					indexTime,
				)
				sink := &compositionSink{}
				if err := executor.Execute(admittedContext, compiled, sink); err != nil {
					t.Fatalf("execute exact admitted cap: %v", err)
				}
				if sink.setCalls != 1 || len(sink.rows) != int(policy.MaxResultRows) {
					t.Fatalf("published schema/rows = %d/%d, want 1/%d", sink.setCalls, len(sink.rows), policy.MaxResultRows)
				}
			})

			t.Run("one over admitted cap ignores higher live cap", func(t *testing.T) {
				if err := executor.Reconfigure(Config{MaxResultRows: policy.MaxResultRows + 1}); err != nil {
					t.Fatal(err)
				}
				compiled := compileAdmittedRowsTimechart(
					t,
					chart.clause+` | head 1 | eval member=split("a,b,c,d", ",") | mvexpand member | table member`,
					earliest,
					latest,
					indexTime,
				)
				sink := &compositionSink{}
				err := executor.Execute(admittedContext, compiled, sink)
				if !errors.Is(err, searchjobs.ErrExecutionLimit) {
					t.Fatalf("Execute error = %v, want ErrExecutionLimit", err)
				}
				if sink.setCalls != 0 || len(sink.rows) != 0 {
					t.Fatalf("over-limit result published schema=%d rows=%d", sink.setCalls, len(sink.rows))
				}
			})
		})
	}

	t.Run("observed discovery can exceed public row cap", func(t *testing.T) {
		if err := executor.Reconfigure(Config{MaxResultRows: 1}); err != nil {
			t.Fatal(err)
		}
		compiled := compileAdmittedRowsTimechart(
			t,
			`search source="burst" | timechart span=1s fixedrange=false cont=false count | head 1`,
			earliest,
			latest,
			indexTime,
		)
		sink := &compositionSink{}
		if err := executor.Execute(admittedContext, compiled, sink); err != nil {
			t.Fatalf("execute five-row discovery: %v", err)
		}
		if sink.setCalls != 1 || len(sink.rows) != 1 {
			t.Fatalf("published schema/rows = %d/%d, want 1/1", sink.setCalls, len(sink.rows))
		}
		count, ok := sink.rows[0][1].Unsigned()
		if !ok || count != 5 {
			t.Fatalf("observed count = %#v, want 5", sink.rows[0][1])
		}
	})
}

func compileAdmittedRowsTimechart(
	t *testing.T,
	suffix string,
	earliest time.Time,
	latest time.Time,
	indexTime time.Time,
) clickhouse.CompiledQuery {
	t.Helper()
	parsed, err := spl.Parse(fmt.Sprintf("index=%s | %s", semanticBytesLineageIndex, suffix))
	if err != nil {
		t.Fatal(err)
	}
	visibility := uint64(1)
	logical, err := plan.Build(parsed, plan.Scope{
		TenantID:          "tenant",
		AuthorizedIndexes: []string{semanticBytesLineageIndex},
		Earliest:          earliest,
		Latest:            latest,
		SearchStart:       indexTime,
		IndexTimeCutoff:   indexTime,
		VisibilityCutoff:  &visibility,
		SearchTimezone:    "UTC",
	})
	if err != nil {
		t.Fatal(err)
	}
	compiled, err := (clickhouse.Compiler{}).Compile(logical)
	if err != nil {
		t.Fatal(err)
	}
	if !compiled.HasContinuation() || !compiled.HasValidExecutionSeal() ||
		!compiled.RequiresAtomicResult() {
		t.Fatalf("fixture lacks a sealed atomic continuation: %#v", compiled)
	}
	return compiled
}
