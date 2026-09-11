package queryexec

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
	"github.com/Suhaibinator/open-splunk/internal/plan"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
	"github.com/Suhaibinator/open-splunk/internal/searchlimits"
	"github.com/Suhaibinator/open-splunk/internal/spl"
)

func TestTimechartTerminalStagePublicationIsAtomic(t *testing.T) {
	first := time.Date(2026, time.September, 1, 0, 0, 0, 0, time.UTC)

	t.Run("late resource failure publishes no prefix", func(t *testing.T) {
		const bucketCount = 3600
		compiled := compileTerminalTimechartFixture(t, first, first.Add(time.Hour))
		counts := make([]uint64, bucketCount)
		counts[0] = 1
		connection := &terminalTimechartQueueConnection{rows: []driver.Rows{
			timechartOrdinalRows([]string{"0:api"}, [][]uint64{{1}}),
			fixedTimechartOrdinalRows(counts),
		}}
		sink := &terminalTimechartSink{}
		policy := searchlimits.Default()
		policy.MaxResultBytes = 1 << 20

		err := mustExecutor(t, connection).Execute(
			searchlimits.WithPolicy(context.Background(), policy),
			compiled,
			sink,
		)
		if !errors.Is(err, searchjobs.ErrExecutionLimit) {
			t.Fatalf("terminal staged resource error = %v", err)
		}
		if connection.calls != 2 {
			t.Fatalf("terminal staged query calls = %d, want 2", connection.calls)
		}
		if sink.compiledCalls != 0 || sink.setCalls != 0 || len(sink.schema.Columns) != 0 ||
			len(sink.rows) != 0 || len(sink.bounds) != 0 {
			t.Fatalf(
				"terminal staged failure published a prefix: descriptor calls=%d schema calls=%d schema=%#v rows=%d bounds=%d",
				sink.compiledCalls,
				sink.setCalls,
				sink.schema,
				len(sink.rows),
				len(sink.bounds),
			)
		}
	})

	t.Run("complete small result publishes exact bounds", func(t *testing.T) {
		const bucketCount = 3
		compiled := compileTerminalTimechartFixture(t, first, first.Add(bucketCount*time.Second))
		connection := &terminalTimechartQueueConnection{rows: []driver.Rows{
			timechartOrdinalRows([]string{"0:api"}, [][]uint64{{1}}),
			fixedTimechartOrdinalRows([]uint64{1, 0, 0}),
		}}
		sink := &terminalTimechartSink{}

		if err := mustExecutor(t, connection).Execute(context.Background(), compiled, sink); err != nil {
			t.Fatal(err)
		}
		if connection.calls != 2 || sink.compiledCalls != 1 || sink.setCalls != 1 ||
			len(sink.rows) != bucketCount ||
			len(sink.bounds) != bucketCount {
			t.Fatalf(
				"small terminal staged result: queries=%d descriptor calls=%d schema calls=%d rows=%d bounds=%d",
				connection.calls,
				sink.compiledCalls,
				sink.setCalls,
				len(sink.rows),
				len(sink.bounds),
			)
		}
		if !sink.compiled.HasValidExecutionSeal() || !sink.compiled.IsContinuationOf(compiled) {
			t.Fatal("small terminal staged result published an invalid final descriptor")
		}
		for index, bounds := range sink.bounds {
			wantEarliest := first.Add(time.Duration(index) * time.Second).Format(time.RFC3339Nano)
			wantLatest := first.Add(time.Duration(index+1) * time.Second).Format(time.RFC3339Nano)
			if bounds.Earliest != wantEarliest || bounds.Latest != wantLatest {
				t.Fatalf("small terminal staged bounds %d = %#v, want %s..%s", index, bounds, wantEarliest, wantLatest)
			}
		}
	})
}

type terminalTimechartSink struct {
	compositionSink
	compiled      clickhouse.CompiledQuery
	compiledCalls int
}

func (sink *terminalTimechartSink) SetCompiledQuery(compiled clickhouse.CompiledQuery) error {
	sink.compiledCalls++
	sink.compiled = compiled
	return nil
}

func compileTerminalTimechartFixture(
	t *testing.T,
	earliest time.Time,
	latest time.Time,
) clickhouse.CompiledQuery {
	t.Helper()
	parsed, err := spl.Parse(
		`index=target | timechart span=1h count BY host | timechart span=1s count`,
	)
	if err != nil {
		t.Fatal(err)
	}
	visibility := uint64(1)
	logical, err := plan.Build(parsed, plan.Scope{
		TenantID:          "tenant",
		AuthorizedIndexes: []string{"target"},
		Earliest:          earliest,
		Latest:            latest,
		SearchStart:       latest,
		IndexTimeCutoff:   latest,
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
	if !compiled.HasValidExecutionSeal() || !compiled.HasContinuation() {
		t.Fatal("terminal timechart fixture lacks a sealed continuation")
	}
	return compiled
}

type terminalTimechartQueueConnection struct {
	rows  []driver.Rows
	calls int
}

func (connection *terminalTimechartQueueConnection) Query(
	context.Context,
	string,
	...any,
) (driver.Rows, error) {
	if connection.calls >= len(connection.rows) {
		return nil, fmt.Errorf("unexpected terminal timechart query %d", connection.calls+1)
	}
	rows := connection.rows[connection.calls]
	connection.calls++
	return rows, nil
}
