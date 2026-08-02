package main

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	internalclickhouse "github.com/Suhaibinator/open-splunk/internal/clickhouse"
	"github.com/Suhaibinator/open-splunk/internal/indexread"
	"github.com/Suhaibinator/open-splunk/internal/plan"
	"github.com/Suhaibinator/open-splunk/internal/queryexec"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
	"github.com/Suhaibinator/open-splunk/internal/spl"
)

func TestRuntimeReadConstructorsRequireConcreteAdmission(t *testing.T) {
	t.Parallel()

	var typedNil *runtimeReadAdmission
	for _, test := range []struct {
		name      string
		admission indexread.Admission
	}{
		{name: "nil"},
		{name: "typed nil", admission: typedNil},
	} {
		t.Run(test.name, func(t *testing.T) {
			connection := &runtimeReadConnection{}
			if executor, err := newRuntimeQueryExecutor(
				connection,
				queryexec.Config{},
				test.admission,
			); err == nil || executor != nil {
				t.Fatalf(
					"newRuntimeQueryExecutor() = (%v, %v), want nil and error",
					executor,
					err,
				)
			}
			if reader, err := newRuntimeIndexStatisticsReader(
				connection,
				internalclickhouse.IndexStatisticsConfig{},
				test.admission,
			); err == nil || reader != nil {
				t.Fatalf(
					"newRuntimeIndexStatisticsReader() = (%v, %v), want nil and error",
					reader,
					err,
				)
			}
		})
	}
}

func TestRuntimeQueryExecutorForcesSharedAdmissionBeforeNativeQuery(t *testing.T) {
	t.Parallel()

	registry := retiredRuntimeReadRegistry(t)
	connection := &runtimeReadConnection{}
	executor, err := newRuntimeQueryExecutor(
		connection,
		queryexec.Config{ReadAdmission: indexread.NewRegistry()},
		registry,
	)
	if err != nil {
		t.Fatalf("newRuntimeQueryExecutor(): %v", err)
	}
	err = executor.Execute(
		context.Background(),
		compileRuntimeReadQuery(t),
		runtimeReadSink{},
	)
	if !errors.Is(err, indexread.ErrUnavailable) {
		t.Fatalf("Execute() error = %v, want ErrUnavailable", err)
	}
	if connection.queryCalls != 0 {
		t.Fatalf("native Query calls = %d, want 0", connection.queryCalls)
	}
}

func TestRuntimeIndexStatisticsReaderForcesSharedAdmissionBeforeNativeQueryRow(
	t *testing.T,
) {
	t.Parallel()

	registry := retiredRuntimeReadRegistry(t)
	connection := &runtimeReadConnection{}
	reader, err := newRuntimeIndexStatisticsReader(
		connection,
		internalclickhouse.IndexStatisticsConfig{
			ReadAdmission: indexread.NewRegistry(),
		},
		registry,
	)
	if err != nil {
		t.Fatalf("newRuntimeIndexStatisticsReader(): %v", err)
	}
	_, err = reader.GetIndexStatistics(
		context.Background(),
		internalclickhouse.IndexStatisticsRequest{
			TenantID:         "tenant-read",
			IndexID:          "idx_0123456789abcdef012345",
			IndexName:        "target",
			MeasuredAt:       time.Date(2026, 8, 2, 12, 0, 0, 0, time.UTC),
			VisibilityCutoff: 1,
		},
	)
	if !errors.Is(err, indexread.ErrUnavailable) {
		t.Fatalf("GetIndexStatistics() error = %v, want ErrUnavailable", err)
	}
	if connection.queryRowCalls != 0 {
		t.Fatalf("native QueryRow calls = %d, want 0", connection.queryRowCalls)
	}
}

type runtimeReadAdmission struct{}

func (*runtimeReadAdmission) Acquire(
	ctx context.Context,
	_ string,
	_ []string,
) (context.Context, func(), error) {
	return ctx, func() {}, nil
}

type runtimeReadConnection struct {
	driver.Conn
	queryCalls    int
	queryRowCalls int
}

func (connection *runtimeReadConnection) Query(
	context.Context,
	string,
	...any,
) (driver.Rows, error) {
	connection.queryCalls++
	return nil, errors.New("native Query must not be called")
}

func (connection *runtimeReadConnection) QueryRow(
	context.Context,
	string,
	...any,
) driver.Row {
	connection.queryRowCalls++
	return nil
}

type runtimeReadSink struct{}

func (runtimeReadSink) SetSchema(searchjobs.Schema) error { return nil }
func (runtimeReadSink) AddRow([]searchjobs.Value) error   { return nil }

func retiredRuntimeReadRegistry(t *testing.T) *indexread.Registry {
	t.Helper()
	registry := indexread.NewRegistry()
	if err := registry.Retire(
		context.Background(),
		"tenant-read",
		"target",
	); err != nil {
		t.Fatalf("Retire(): %v", err)
	}
	return registry
}

func compileRuntimeReadQuery(t *testing.T) internalclickhouse.CompiledQuery {
	t.Helper()
	parsed, err := spl.Parse(`index=target | stats count`)
	if err != nil {
		t.Fatalf("Parse(): %v", err)
	}
	start := time.Date(2026, 8, 2, 10, 0, 0, 0, time.UTC)
	visibilityCutoff := uint64(1)
	logical, err := plan.Build(parsed, plan.Scope{
		TenantID:          "tenant-read",
		AuthorizedIndexes: []string{"target"},
		Earliest:          start,
		Latest:            start.Add(time.Hour),
		SearchStart:       start.Add(2 * time.Hour),
		SearchTimezone:    "UTC",
		IndexTimeCutoff:   start.Add(2 * time.Hour),
		VisibilityCutoff:  &visibilityCutoff,
	})
	if err != nil {
		t.Fatalf("Build(): %v", err)
	}
	compiled, err := (internalclickhouse.Compiler{}).Compile(logical)
	if err != nil {
		t.Fatalf("Compile(): %v", err)
	}
	return compiled
}
