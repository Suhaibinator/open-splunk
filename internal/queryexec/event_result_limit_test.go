package queryexec

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
	"github.com/Suhaibinator/open-splunk/internal/indexread"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
	"github.com/Suhaibinator/open-splunk/internal/searchlimits"
)

func eventLimitExecutorFixture(t *testing.T, rowCount int) (*Executor, *fakeQueryConnection, clickhouse.CompiledQuery) {
	t.Helper()
	stamp := time.Date(2026, 8, 11, 12, 0, 0, 0, time.UTC)
	query := queryIntegrationCompileSearchRange(t, `index=main | table event_id`, stamp, stamp.Add(-time.Hour), stamp)
	rows := &fakeRows{
		columns: []string{"event_id"},
		types: []driver.ColumnType{
			fakeColumnType{name: "event_id", databaseType: "String", scanType: reflect.TypeFor[string]()},
		},
	}
	for row := range rowCount {
		rows.data = append(rows.data, []any{fmt.Sprintf("event-%d", row)})
	}
	connection := &fakeQueryConnection{rows: rows}
	executor := mustExecutor(t, connection)
	executor.readAdmission = indexread.UnfencedAdmission{}
	return executor, connection, query
}

func TestExecutorUsesAdmittedEventLimitAndPreservesCanonicalExportSource(t *testing.T) {
	t.Parallel()
	for _, rowCount := range []int{0, 1, 2, 3} {
		t.Run(fmt.Sprint(rowCount), func(t *testing.T) {
			t.Parallel()
			executor, connection, canonical := eventLimitExecutorFixture(t, rowCount)
			sql := canonical.SQL
			digest, _ := canonical.ExecutionAuthorityDigest()
			policy := searchlimits.Default()
			policy.MaxResultRows = 2
			ctx := searchlimits.WithPolicy(context.Background(), policy)
			sink := &fakeSink{}
			if err := executor.Execute(ctx, canonical, sink); err != nil {
				t.Fatalf("execute bounded query: %v", err)
			}
			if connection.query != sql+" LIMIT 3" || len(sink.rows) != rowCount {
				t.Fatalf("bounded driver/sink = SQL %s, rows %d, want %d", connection.query, len(sink.rows), rowCount)
			}
			after, valid := canonical.ExecutionAuthorityDigest()
			if !valid || after != digest || canonical.SQL != sql {
				t.Fatal("retained source changed during execution")
			}
		})
	}
	// Export reexecution uses the same retained canonical authority without a
	// search policy; its own larger row envelope must not inherit search LIMIT.
	executor, connection, canonical := eventLimitExecutorFixture(t, 10_002)
	policy := searchlimits.Default()
	if _, _, err := executor.eventExecutionSurfaceContext(searchlimits.WithPolicy(context.Background(), policy), canonical); err != nil {
		t.Fatalf("derive admitted search: %v", err)
	}
	sink := &fakeSink{}
	if err := executor.Execute(context.Background(), canonical, sink); err != nil {
		t.Fatalf("execute retained authority without search policy: %v", err)
	}
	if connection.query != canonical.SQL || len(sink.rows) != 10_002 {
		t.Fatalf("export inherited search limit: rows %d SQL %s", len(sink.rows), connection.query)
	}
}

func TestExecutorEventLimitStillValidatesOverflowRow(t *testing.T) {
	t.Parallel()
	executor, _, canonical := eventLimitExecutorFixture(t, 3)
	policy := searchlimits.Default()
	policy.MaxResultRows = 2
	ctx := searchlimits.WithPolicy(context.Background(), policy)
	overflow := errors.New("retained row ceiling reached")
	sink := &eventLimitOverflowSink{maximum: 2, overflow: overflow}
	if err := executor.Execute(ctx, canonical, sink); !errors.Is(err, overflow) {
		t.Fatalf("overflow error = %v, want sink ceiling", err)
	}
	if sink.calls != 3 || len(sink.rows) != 2 {
		t.Fatalf("overflow sentinel = calls %d, retained %d", sink.calls, len(sink.rows))
	}

	executor, connection, canonical := eventLimitExecutorFixture(t, 3)
	connection.rows.(*fakeRows).data[2][0] = int64(7)
	sink = &eventLimitOverflowSink{maximum: 2, overflow: overflow}
	if err := executor.Execute(ctx, canonical, sink); err == nil || errors.Is(err, overflow) || !strings.Contains(err.Error(), "fake scan type mismatch") {
		t.Fatalf("invalid overflow row = %v, want native scan failure", err)
	}
	if sink.calls != 2 {
		t.Fatalf("invalid overflow row reached sink: %d calls", sink.calls)
	}
}

type eventLimitOverflowSink struct {
	fakeSink
	maximum  int
	calls    int
	overflow error
}

func (sink *eventLimitOverflowSink) AddRow(values []searchjobs.Value) error {
	sink.calls++
	if len(sink.rows) >= sink.maximum {
		return sink.overflow
	}
	return sink.fakeSink.AddRow(values)
}

func TestExecutorEventLimitRejectsSourceMutationAndLeavesAtomicQueryUnlimited(t *testing.T) {
	t.Parallel()
	executor, _, canonical := eventLimitExecutorFixture(t, 0)
	policy := searchlimits.Default()
	ctx := searchlimits.WithPolicy(context.Background(), policy)
	canonical.Args[0] = "other-tenant"
	if _, _, err := executor.eventExecutionSurfaceContext(ctx, canonical); !errors.Is(err, searchjobs.ErrInvalidResult) {
		t.Fatalf("tampered source = %v", err)
	}
	stamp := time.Date(2026, 8, 11, 12, 0, 0, 0, time.UTC)
	for _, source := range []string{
		`index=main | where severity+1 > 2 | table event_id`,
		`index=main | eventstats count AS total | table event_id`,
		`index=main | stats count`,
	} {
		query := queryIntegrationCompileSearchRange(t, source, stamp, stamp.Add(-time.Hour), stamp)
		sql, _, err := executor.eventExecutionSurfaceContext(ctx, query)
		if err != nil || sql != query.SQL || strings.HasSuffix(sql, " LIMIT 10001") {
			t.Fatalf("validation query bounded: %v\n%s", err, sql)
		}
	}
}
