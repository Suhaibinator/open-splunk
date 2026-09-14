package queryexec

import (
	"context"
	"errors"
	"math"
	"testing"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
)

type stagePollingContext struct {
	context.Context
	checks, cancelAt int
}

func (ctx *stagePollingContext) Err() error {
	ctx.checks++
	if ctx.checks >= ctx.cancelAt {
		return context.Canceled
	}
	return nil
}

func TestStageDynamicValuePollsLargeTraversalAndFinalPhase(t *testing.T) {
	values := make([]searchjobs.Value, 4096)
	for index := range values {
		values[index] = searchjobs.UnsignedValue(uint64(index))
	}
	value := searchjobs.ListValue(values...)
	baseline := &stagePollingContext{Context: context.Background(), cancelAt: math.MaxInt}
	if _, err := stageDynamicValue(baseline, value); err != nil {
		t.Fatal(err)
	}
	for _, at := range []int{1, 8, baseline.checks} {
		ctx := &stagePollingContext{Context: context.Background(), cancelAt: at}
		result, err := stageDynamicValue(ctx, value)
		if !errors.Is(err, context.Canceled) || result != nil {
			t.Fatalf("cancel at %d: result=%T err=%v", at, result, err)
		}
	}
}

func TestStageSinkCancellationStopsBeforeRetainingLargeValue(t *testing.T) {
	values := make([]searchjobs.Value, 4096)
	for index := range values {
		values[index] = searchjobs.UnsignedValue(uint64(index))
	}
	ctx := &stagePollingContext{Context: context.Background(), cancelAt: 8}
	sink := &timechartStageSink{
		ctx: ctx, stageBudget: &stageBudget{maxRetained: 64 << 20},
		columns: []clickhouse.RelationColumn{{Name: "list", Type: "Dynamic"}}, maxResultRows: 100,
	}
	err := sink.AddRow([]searchjobs.Value{searchjobs.ListValue(values...)})
	if !errors.Is(err, context.Canceled) || len(sink.rows) != 0 {
		t.Fatalf("rows=%d err=%v", len(sink.rows), err)
	}
}

type publicationBoundsSink struct {
	fakeSink
	bounds []searchjobs.TimeBucketBounds
}

func (sink *publicationBoundsSink) AddRowWithTimeBucket(values []searchjobs.Value, bounds searchjobs.TimeBucketBounds) error {
	sink.bounds = append(sink.bounds, bounds)
	return sink.AddRow(values)
}

func TestTerminalTransactionPreservesVariedRowsAndOptionalBounds(t *testing.T) {
	sink := &stagedFinalSink{ctx: context.Background(), stageBudget: &stageBudget{maxRetained: 64 << 20}, maxRows: 1024}
	schema := searchjobs.Schema{Columns: []searchjobs.Column{{Name: "_time", Kind: searchjobs.ValueKindTime}, {Name: "count", Kind: searchjobs.ValueKindUnsigned}}}
	if err := sink.SetSchema(schema); err != nil {
		t.Fatal(err)
	}
	first := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	for index := range 600 {
		start := first.Add(time.Duration(index) * time.Second)
		values := []searchjobs.Value{searchjobs.TimeValue(start), searchjobs.UnsignedValue(uint64(index))}
		var err error
		if index%2 == 0 {
			err = sink.AddRowWithTimeBucket(values, searchjobs.TimeBucketBounds{Earliest: start.Format(time.RFC3339Nano), Latest: start.Add(time.Second).Format(time.RFC3339Nano)})
		} else {
			err = sink.AddRow(values)
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	recipient := &publicationBoundsSink{}
	if err := sink.publish(recipient); err != nil {
		t.Fatal(err)
	}
	if recipient.setCalls != 1 || len(recipient.rows) != 600 || len(recipient.bounds) != 300 {
		t.Fatalf("schema=%d rows=%d bounds=%d", recipient.setCalls, len(recipient.rows), len(recipient.bounds))
	}
	for index, row := range recipient.rows {
		count, _ := row[1].Unsigned()
		at, _ := row[0].Time()
		if count != uint64(index) || !at.Equal(first.Add(time.Duration(index)*time.Second)) {
			t.Fatalf("row %d reused or changed", index)
		}
		if index%2 == 0 && recipient.bounds[index/2].Earliest != at.Format(time.RFC3339Nano) {
			t.Fatalf("bounds %d changed", index)
		}
	}
}

func TestTerminalTransactionCancellationBeforePublicationIsPrivate(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	transaction := &stagedFinalSink{ctx: ctx, stageBudget: &stageBudget{maxRetained: 1 << 20}, maxRows: 10}
	if err := transaction.SetSchema(searchjobs.Schema{Columns: []searchjobs.Column{{Name: "value", Kind: searchjobs.ValueKindUnsigned}}}); err != nil {
		t.Fatal(err)
	}
	if err := transaction.AddRow([]searchjobs.Value{searchjobs.UnsignedValue(1)}); err != nil {
		t.Fatal(err)
	}
	cancel()
	recipient := &fakeSink{}
	if err := transaction.publish(recipient); !errors.Is(err, context.Canceled) {
		t.Fatalf("publish canceled transaction: %v", err)
	}
	if recipient.setCalls != 0 || len(recipient.rows) != 0 {
		t.Fatal("canceled transaction published")
	}
}

func TestTerminalTransactionPreflightsCompactExpansionThroughWrappers(t *testing.T) {
	transaction := &stagedFinalSink{ctx: context.Background(), stageBudget: &stageBudget{maxRetained: 1024}}
	wrapped := &timechartGridSink{ResultSink: timechartWorkSink{ResultSink: transaction}}
	if err := preflightStagedPublicationRow(wrapped, 4096); !errors.Is(err, searchjobs.ErrExecutionLimit) {
		t.Fatalf("expanded row escaped preflight: %v", err)
	}
	if transaction.retained != 0 || transaction.rows.first != nil {
		t.Fatal("preflight allocated or retained rows")
	}
}
