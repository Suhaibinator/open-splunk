package main

import (
	"context"
	"errors"
	"fmt"

	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
	"github.com/Suhaibinator/open-splunk/internal/plan"
	"github.com/Suhaibinator/open-splunk/internal/queryexec"
	"github.com/Suhaibinator/open-splunk/internal/searchanalysis"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
	"github.com/Suhaibinator/open-splunk/internal/server"
)

// runtimeSearchAnalysis groups the derived-search services that share the
// runtime's search-job manager, compiler, executor, and ClickHouse connection.
// Timeline analysis is synchronous and owns no resources. Field analysis owns
// detached workers and a cache, so this object must be closed after HTTP request
// drain and before the shared search/ClickHouse dependencies are closed.
type runtimeSearchAnalysis struct {
	timelines *searchanalysis.Service
	fields    *searchanalysis.FieldService
}

type runtimeAnalysisSearches interface {
	CompletedExecutionSnapshotFor(context.Context, searchjobs.AccessScope, string) (searchjobs.ExecutionSnapshot, error)
}

type runtimeAnalysisCompiler interface {
	CompileTimeline(*plan.Query, clickhouse.TimelineSpec) (clickhouse.CompiledTimeline, error)
	CompileFieldCatalog(*plan.Query, clickhouse.FieldCatalogSpec) (clickhouse.CompiledFieldCatalog, error)
	CompileFieldSummary(*plan.Query, clickhouse.FieldSummarySpec) (clickhouse.CompiledFieldSummary, error)
}

type runtimeAnalysisExecutor interface {
	ExecuteTimeline(context.Context, clickhouse.CompiledTimeline) ([]queryexec.TimelineBucket, error)
	ExecuteFieldCatalog(context.Context, clickhouse.CompiledFieldCatalog) (queryexec.FieldCatalogResult, error)
	ExecuteFieldSummary(context.Context, clickhouse.CompiledFieldSummary) (queryexec.FieldSummaryResult, error)
}

// runtimeSearchAnalysisConfig names each borrowed dependency once. Timeline
// and field analysis must never drift onto different search snapshots,
// compilers, or ClickHouse executors inside one runtime.
type runtimeSearchAnalysisConfig struct {
	Searches         runtimeAnalysisSearches
	Compiler         runtimeAnalysisCompiler
	Executor         runtimeAnalysisExecutor
	FieldCursorScope string
}

func newRuntimeSearchAnalysis(config runtimeSearchAnalysisConfig) (*runtimeSearchAnalysis, error) {
	timelines, err := searchanalysis.New(searchanalysis.Config{
		Searches: config.Searches,
		Compiler: config.Compiler,
		Executor: config.Executor,
	})
	if err != nil {
		return nil, fmt.Errorf("compose search analysis runtime: %w", err)
	}
	fields, err := searchanalysis.NewFieldService(searchanalysis.FieldConfig{
		Searches:    config.Searches,
		Compiler:    config.Compiler,
		Executor:    config.Executor,
		CursorScope: config.FieldCursorScope,
	})
	if err != nil {
		return nil, fmt.Errorf("compose search analysis runtime: %w", err)
	}
	return &runtimeSearchAnalysis{timelines: timelines, fields: fields}, nil
}

// Close stops field admission, cancels detached catalog workers, invalidates
// cached cursors, and waits without a deadline. Returning while a worker still
// uses the borrowed search manager or ClickHouse executor would make later
// runtime cleanup unsafe. The process restores default second-signal behavior
// before shutdown, which remains the hard escape hatch for a stuck dependency.
func (analysis *runtimeSearchAnalysis) Close() error {
	if analysis == nil || analysis.fields == nil {
		return nil
	}
	return analysis.fields.Close(context.Background())
}

// newRuntimeHTTPHandler attaches the enforcing analysis services to the browser
// handler without transferring ownership. server.Handler.Close remains limited
// to upgraded WebSocket connections because HTTP shutdown invokes it before
// ordinary request drain.
func newRuntimeHTTPHandler(config server.Config, analysis *runtimeSearchAnalysis) (*server.Handler, error) {
	if analysis == nil || analysis.timelines == nil || analysis.fields == nil {
		return nil, errors.New("compose HTTP runtime: search analysis services are required")
	}
	if config.SearchTimelines != nil {
		return nil, errors.New("compose HTTP runtime: search timeline service is already configured")
	}
	if config.SearchFields != nil {
		return nil, errors.New("compose HTTP runtime: search field service is already configured")
	}
	config.SearchTimelines = analysis.timelines
	config.SearchFields = analysis.fields
	return server.NewHandler(config)
}
