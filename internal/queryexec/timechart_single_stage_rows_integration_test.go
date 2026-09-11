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
	"github.com/Suhaibinator/open-splunk/internal/searchlimits"
	"github.com/Suhaibinator/open-splunk/internal/spl"
)

func TestTimechartSingleStageAdmittedRowsAgainstClickHouse(t *testing.T) {
	if os.Getenv("OPEN_SPLUNK_CLICKHOUSE_INTEGRATION") != "1" {
		t.Skip("set OPEN_SPLUNK_CLICKHOUSE_INTEGRATION=1")
	}
	earliest := time.Date(2026, time.September, 1, 0, 0, 0, 0, time.UTC)
	indexTime := earliest.Add(10 * 24 * time.Hour)
	events := singleStageRowsEvents(earliest)
	baseContext, executor := semanticBytesLineageStartClickHouse(t, indexTime, events)
	policy := searchlimits.Default()
	policy.MaxResultRows = 3
	admittedContext := searchlimits.WithPolicy(baseContext, policy)

	for _, split := range []struct {
		name, suffix string
	}{
		{name: "unsplit"},
		{name: "split", suffix: " BY host limit=0"},
	} {
		for _, aggregate := range []string{
			"count",
			"count(source)",
			"sum(metric)",
			"avg(metric)",
			"p95(metric)",
		} {
			name := split.name + "/" + aggregate
			t.Run(name+"/exact", func(t *testing.T) {
				compiled := compileSingleStageRowsTimechart(
					t,
					"eval metric=2 | timechart span=1s "+aggregate+split.suffix,
					earliest,
					earliest.Add(3*time.Second),
					indexTime,
				)
				requireSingleStageTimechart(t, compiled)
				assertSingleStageRowsSuccess(t, admittedContext, executor, compiled, 3, true)
			})
			t.Run(name+"/one-over", func(t *testing.T) {
				compiled := compileSingleStageRowsTimechart(
					t,
					"eval metric=2 | timechart span=1s "+aggregate+split.suffix,
					earliest,
					earliest.Add(4*time.Second),
					indexTime,
				)
				requireSingleStageTimechart(t, compiled)
				assertSingleStageRowsLimit(t, admittedContext, executor, compiled)
			})
		}
	}

	for _, test := range []struct {
		name, source string
		latest       time.Time
		wantRows     int
		wantBounds   bool
	}{
		{
			name:       "expanded exact",
			source:     `timechart span=4s count | eval member=split("a,b,c", ",") | mvexpand member | table member`,
			latest:     earliest.Add(4 * time.Second),
			wantRows:   3,
			wantBounds: false,
		},
		{
			name:       "missing time exact",
			source:     `timechart span=1s count | table count`,
			latest:     earliest.Add(3 * time.Second),
			wantRows:   3,
			wantBounds: false,
		},
		{
			name:       "downstream chart contracts dense input",
			source:     `search source!="chart" | timechart span=1s count | eval bucket_count=count | chart count BY bucket_count`,
			latest:     earliest.Add(4 * time.Second),
			wantRows:   3,
			wantBounds: false,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			compiled := compileSingleStageRowsTimechart(
				t,
				test.source,
				earliest,
				test.latest,
				indexTime,
			)
			requireSingleStageTimechart(t, compiled)
			assertSingleStageRowsSuccess(
				t,
				admittedContext,
				executor,
				compiled,
				test.wantRows,
				test.wantBounds,
			)
		})
	}
	for _, test := range []struct {
		name, source string
	}{
		{
			name:   "expanded one over",
			source: `timechart span=4s count | eval member=split("a,b,c,d", ",") | mvexpand member | table member`,
		},
		{
			name:   "missing time one over",
			source: `timechart span=1s count | table count`,
		},
		{
			name:   "downstream chart one over",
			source: `search source="chart" | timechart span=1s count | eval bucket_count=count | chart count BY bucket_count`,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			compiled := compileSingleStageRowsTimechart(
				t,
				test.source,
				earliest,
				earliest.Add(4*time.Second),
				indexTime,
			)
			requireSingleStageTimechart(t, compiled)
			assertSingleStageRowsLimit(t, admittedContext, executor, compiled)
		})
	}

	for _, test := range []struct {
		name, source string
		start        time.Time
		latest       time.Time
		wantRows     int
	}{
		{
			name:     "elapsed sparse unsplit",
			source:   `search source="burst" | timechart span=1s cont=false partial=false count`,
			latest:   earliest.Add(6 * time.Second),
			wantRows: 1,
		},
		{
			name:     "elapsed sparse split",
			source:   `search source="burst" | timechart span=1s cont=false partial=false count BY host limit=0`,
			latest:   earliest.Add(6 * time.Second),
			wantRows: 1,
		},
		{
			name:     "partial trims dense edges",
			source:   `timechart span=1s partial=false count`,
			start:    earliest.Add(500 * time.Millisecond),
			latest:   earliest.Add(4500 * time.Millisecond),
			wantRows: 3,
		},
		{
			name:     "calendar exact",
			source:   `timechart span=1d count`,
			latest:   earliest.AddDate(0, 0, 3),
			wantRows: 3,
		},
		{
			name:     "calendar sparse controls",
			source:   `search source="burst" | timechart span=1d cont=false partial=false count`,
			latest:   earliest.AddDate(0, 0, 6),
			wantRows: 1,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			start := test.start
			if start.IsZero() {
				start = earliest
			}
			compiled := compileSingleStageRowsTimechart(
				t,
				test.source,
				start,
				test.latest,
				indexTime,
			)
			requireSingleStageTimechart(t, compiled)
			assertSingleStageRowsSuccess(t, admittedContext, executor, compiled, test.wantRows, true)
		})
	}
	t.Run("calendar one over", func(t *testing.T) {
		compiled := compileSingleStageRowsTimechart(
			t,
			`timechart span=1d count`,
			earliest,
			earliest.AddDate(0, 0, 4),
			indexTime,
		)
		requireSingleStageTimechart(t, compiled)
		assertSingleStageRowsLimit(t, admittedContext, executor, compiled)
	})

	for _, test := range []struct {
		name, source string
	}{
		{
			name:   "empty unsplit",
			source: `search source="absent" | timechart span=1s cont=false count`,
		},
		{
			name:   "empty split domain",
			source: `timechart span=1s cont=false count BY service usenull=false limit=0`,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			compiled := compileSingleStageRowsTimechart(
				t,
				test.source,
				earliest,
				earliest.Add(6*time.Second),
				indexTime,
			)
			requireSingleStageTimechart(t, compiled)
			assertSingleStageRowsSuccess(t, admittedContext, executor, compiled, 0, false)
		})
	}

	t.Run("observed discovery retains raw row allowance", func(t *testing.T) {
		compiled := compileSingleStageRowsTimechart(
			t,
			`search source="burst" | timechart span=1s fixedrange=false cont=false count | head 1`,
			earliest,
			earliest.Add(3*time.Second),
			indexTime,
		)
		if !compiled.HasContinuation() {
			t.Fatal("observed fixture does not require discovery")
		}
		assertSingleStageRowsSuccess(t, admittedContext, executor, compiled, 1, true)
	})
}

func singleStageRowsEvents(first time.Time) []semanticBytesLineageEvent {
	events := make([]semanticBytesLineageEvent, 0, 17)
	for index := range 5 {
		events = append(events, semanticBytesLineageEvent{
			id:     fmt.Sprintf("single-stage-burst-%d", index),
			at:     first.Add(time.Duration(index+1) * 100 * time.Millisecond),
			host:   "api",
			source: "burst",
			raw:    []byte("x"),
		})
	}
	for bucket := 1; bucket <= 2; bucket++ {
		events = append(events, semanticBytesLineageEvent{
			id:     fmt.Sprintf("single-stage-range-%d", bucket),
			at:     first.Add(time.Duration(bucket)*time.Second + 100*time.Millisecond),
			host:   "api",
			source: "range",
			raw:    []byte("x"),
		})
	}
	for bucket := 0; bucket < 4; bucket++ {
		for member := 0; member <= bucket; member++ {
			events = append(events, semanticBytesLineageEvent{
				id:     fmt.Sprintf("single-stage-chart-%d-%d", bucket, member),
				at:     first.Add(time.Duration(bucket)*time.Second + time.Duration(member+1)*10*time.Millisecond),
				host:   "api",
				source: "chart",
				raw:    []byte("x"),
			})
		}
	}
	return events
}

func compileSingleStageRowsTimechart(
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
	return compiled
}

func requireSingleStageTimechart(t *testing.T, compiled clickhouse.CompiledQuery) {
	t.Helper()
	if compiled.HasContinuation() || !compiled.HasValidExecutionSeal() ||
		!compiled.RequiresAtomicResult() {
		t.Fatalf("fixture is not one sealed atomic stage: %#v", compiled)
	}
}

func assertSingleStageRowsSuccess(
	t *testing.T,
	ctx context.Context,
	executor *Executor,
	compiled clickhouse.CompiledQuery,
	wantRows int,
	wantBounds bool,
) {
	t.Helper()
	sink := &compositionSink{}
	if err := executor.Execute(ctx, compiled, sink); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if sink.setCalls != 1 || len(sink.rows) != wantRows {
		t.Fatalf("published schema/rows = %d/%d, want 1/%d", sink.setCalls, len(sink.rows), wantRows)
	}
	if wantBounds && len(sink.bounds) != wantRows {
		t.Fatalf("published bounds = %d, want %d", len(sink.bounds), wantRows)
	}
	if !wantBounds && len(sink.bounds) != 0 {
		t.Fatalf("published bounds = %d, want 0", len(sink.bounds))
	}
}

func assertSingleStageRowsLimit(
	t *testing.T,
	ctx context.Context,
	executor *Executor,
	compiled clickhouse.CompiledQuery,
) {
	t.Helper()
	sink := &compositionSink{}
	err := executor.Execute(ctx, compiled, sink)
	if !errors.Is(err, searchjobs.ErrExecutionLimit) {
		t.Fatalf("Execute error = %v, want ErrExecutionLimit", err)
	}
	if sink.setCalls != 0 || len(sink.rows) != 0 || len(sink.bounds) != 0 {
		t.Fatalf(
			"over-limit result published schema=%d rows=%d bounds=%d",
			sink.setCalls,
			len(sink.rows),
			len(sink.bounds),
		)
	}
}
