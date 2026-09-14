package queryexec

import (
	"context"
	"errors"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
	"github.com/Suhaibinator/open-splunk/internal/indexread"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
)

func TestRepeatedTimechartStagesHoldOneReadLease(t *testing.T) {
	query := compileReadAdmissionQuery(t, `index=target | timechart span=1h count BY host | eval stage="api" | timechart span=1h count BY stage | timechart span=1h count`)
	admission := &recordingReadAdmission{}
	connection := &terminalTimechartQueueConnection{rows: []driver.Rows{
		timechartOrdinalRows([]string{"0:api"}, [][]uint64{{1}}),
		timechartOrdinalRows([]string{"0:api"}, [][]uint64{{1}}),
		fixedTimechartOrdinalRows([]uint64{1}),
	}}
	executor := mustExecutor(t, connection)
	executor.readAdmission = admission
	sink := &terminalTimechartSink{}
	if err := executor.Execute(context.Background(), query, sink); err != nil {
		t.Fatal(err)
	}
	if connection.calls != 3 || admission.acquireCalls.Load() != 1 || admission.releaseCalls.Load() != 1 {
		t.Fatalf("queries=%d acquire=%d release=%d", connection.calls, admission.acquireCalls.Load(), admission.releaseCalls.Load())
	}
	if sink.setCalls != 1 || len(sink.rows) != 1 || len(sink.bounds) != 1 {
		t.Fatalf("publishedschema=%d rows=%d bounds=%d", sink.setCalls, len(sink.rows), len(sink.bounds))
	}
}

type stageReleaseAdmission struct {
	recordingReadAdmission
	cancel context.CancelCauseFunc
}

func (admission *stageReleaseAdmission) Acquire(ctx context.Context, tenant string, indexes []string) (context.Context, func(), error) {
	ctx, release, err := admission.recordingReadAdmission.Acquire(ctx, tenant, indexes)
	ctx, admission.cancel = context.WithCancelCause(ctx)
	return ctx, func() {
		// Release cleanup may cancel its context. Only a cause observed before
		// release is allowed to replace the operation's successful outcome.
		admission.cancel(indexread.ErrUnavailable)
		release()
	}, err
}

type stageHandoffCancellationSink struct {
	terminalTimechartSink
	cancel context.CancelCauseFunc
}

func (sink *stageHandoffCancellationSink) SetCompiledQuery(query clickhouse.CompiledQuery) error {
	if sink.cancel != nil {
		sink.cancel(indexread.ErrUnavailable)
	}
	return sink.terminalTimechartSink.SetCompiledQuery(query)
}

func TestStagedReadCausePrecedesReleaseCleanup(t *testing.T) {
	for _, retireAtHandoff := range []bool{false, true} {
		name := "normal release does not replace success"
		if retireAtHandoff {
			name = "retirement at descriptor handoff precedes publication"
		}
		t.Run(name, func(t *testing.T) {
			query := compileReadAdmissionQuery(t, `index=target | timechart span=1h count BY host | timechart span=1h count`)
			admission := &stageReleaseAdmission{}
			connection := &terminalTimechartQueueConnection{rows: []driver.Rows{
				timechartOrdinalRows([]string{"0:api"}, [][]uint64{{1}}),
				fixedTimechartOrdinalRows([]uint64{1}),
			}}
			executor := mustExecutor(t, connection)
			executor.readAdmission = admission
			sink := &stageHandoffCancellationSink{}
			if retireAtHandoff {
				sink.cancel = func(cause error) { admission.cancel(cause) }
			}
			err := executor.Execute(context.Background(), query, sink)
			if retireAtHandoff {
				if !errors.Is(err, indexread.ErrUnavailable) || !errors.Is(err, searchjobs.ErrStorageUnavailable) || sink.setCalls != 0 || len(sink.rows) != 0 {
					t.Fatalf("handoff retirement: err=%v schema=%d rows=%d", err, sink.setCalls, len(sink.rows))
				}
			} else if err != nil || sink.setCalls != 1 || len(sink.rows) != 1 {
				t.Fatalf("normal release: err=%v schema=%d rows=%d", err, sink.setCalls, len(sink.rows))
			}
			if admission.acquireCalls.Load() != 1 || admission.releaseCalls.Load() != 1 {
				t.Fatalf("acquire=%d release=%d", admission.acquireCalls.Load(), admission.releaseCalls.Load())
			}
		})
	}
}

func TestAdmittedStageRetainsReadScopeAndNativeValidators(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*clickhouse.CompiledQuery)
		rows   func() *fakeRows
	}{
		{"read scope", func(query *clickhouse.CompiledQuery) { query.SQL += " -- tampered" }, func() *fakeRows { return timechartOrdinalRows([]string{"0:api"}, [][]uint64{{1}}) }},
		{"native row", func(*clickhouse.CompiledQuery) {}, func() *fakeRows { return timechartOrdinalRows([]string{"0:api"}, [][]uint64{{1}, {2}}) }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			query := compileReadAdmissionQuery(t, `index=target | timechart span=1h count BY host | head 1`)
			detached, valid, err := query.CloneForExecutionContext(context.Background())
			if err != nil || !valid {
				t.Fatalf("clone=%v valid=%t", err, valid)
			}
			admission := &recordingReadAdmission{}
			connection := &terminalTimechartQueueConnection{rows: []driver.Rows{test.rows()}}
			executor := mustExecutor(t, connection)
			executor.readAdmission = admission
			ctx, release, err := executor.acquireRead(context.Background(), detached, "test staged read")
			if err != nil {
				t.Fatal(err)
			}
			defer release()
			test.mutate(&detached)
			sink := &terminalTimechartSink{}
			err = executor.executeAdmittedStage(ctx, detached, sink)
			if !errors.Is(err, searchjobs.ErrInvalidResult) {
				t.Fatalf("stage error=%v", err)
			}
			if admission.acquireCalls.Load() != 1 || sink.setCalls != 0 || len(sink.rows) != 0 {
				t.Fatalf("acquire=%d schema=%d rows=%d", admission.acquireCalls.Load(), sink.setCalls, len(sink.rows))
			}
		})
	}
}
