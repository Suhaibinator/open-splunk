package queryexec

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
)

func TestExecutorRejectsPreCanceledContextBeforeQuery(t *testing.T) {
	first := time.Unix(0, 0).UTC()
	rows := timechartFakeRows(first, 5*time.Minute, []string{"0:ERROR"}, [][]uint64{{1}})
	connection := &fakeQueryConnection{rows: rows}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := mustExecutor(t, connection).Execute(ctx, timechartQuery(first, 1), &fakeSink{})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Execute error = %v, want context.Canceled", err)
	}
	if connection.query != "" {
		t.Fatalf("pre-canceled execution issued query %q", connection.query)
	}
}

func TestExecutorTimechartCancellationOnCloseIsAtomic(t *testing.T) {
	first := time.Unix(0, 0).UTC()
	baseRows := timechartFakeRows(first, 5*time.Minute, []string{"0:ERROR"}, [][]uint64{{1}, {2}})
	ctx, cancel := context.WithCancel(context.Background())
	rows := &cancelOnCloseTimechartRows{fakeRows: baseRows, cancel: cancel}
	sink := &fakeSink{}

	err := mustExecutor(t, &fakeQueryConnection{rows: rows}).Execute(ctx, timechartQuery(first, 2), sink)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Execute error = %v, want context.Canceled", err)
	}
	if rows.closeCalls != 1 || !baseRows.closed {
		t.Fatalf("rows close calls = %d, closed = %v; want one successful close", rows.closeCalls, baseRows.closed)
	}
	if sink.setCalls != 0 || len(sink.rows) != 0 {
		t.Fatalf("canceled timechart was partially published: schema calls=%d rows=%d", sink.setCalls, len(sink.rows))
	}
}

func TestExecutorTimechartCancellationDuringAddRowStopsBeforeNextRow(t *testing.T) {
	first := time.Unix(0, 0).UTC()
	rows := timechartFakeRows(first, 5*time.Minute, []string{"0:ERROR"}, [][]uint64{{1}, {2}, {3}})
	ctx, cancel := context.WithCancel(context.Background())
	sink := &cancelOnFirstAddSink{cancel: cancel}

	err := mustExecutor(t, &fakeQueryConnection{rows: rows}).Execute(ctx, timechartQuery(first, 3), sink)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Execute error = %v, want context.Canceled", err)
	}
	if sink.setCalls != 1 {
		t.Fatalf("schema calls = %d, want 1", sink.setCalls)
	}
	if sink.addCalls != 1 || len(sink.rows) != 1 {
		t.Fatalf("published rows = %d (recorded %d), want exactly 1", sink.addCalls, len(sink.rows))
	}
}

type cancelOnCloseTimechartRows struct {
	*fakeRows
	cancel     context.CancelFunc
	closeCalls int
}

func (rows *cancelOnCloseTimechartRows) Close() error {
	rows.closeCalls++
	err := rows.fakeRows.Close()
	rows.cancel()
	return err
}

type cancelOnFirstAddSink struct {
	fakeSink
	cancel   context.CancelFunc
	addCalls int
}

func (sink *cancelOnFirstAddSink) AddRow(values []searchjobs.Value) error {
	sink.addCalls++
	if err := sink.fakeSink.AddRow(values); err != nil {
		return err
	}
	if sink.addCalls == 1 {
		sink.cancel()
	}
	return nil
}
