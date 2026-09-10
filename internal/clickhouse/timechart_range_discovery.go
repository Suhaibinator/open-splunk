package clickhouse

import (
	"context"
	"errors"
	"slices"
	"strings"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/plan"
)

type compiledTimechartRangeDiscovery struct {
	operator     plan.Timechart
	scan         plan.Scan
	searchStart  time.Time
	timezone     string
	compiler     Compiler
	continuation *compiledTimechartContinuation
}

func compileTimechartRangeSource(relation compiledRelation, state compileState, args []any, operator *plan.Timechart, scan *plan.Scan, stage int) (CompiledQuery, error) {
	fields := []plan.FieldRef{operator.Time}
	if operator.Measure.Input.Name != "" {
		fields = append(fields, operator.Measure.Input)
	}
	if operator.Split != nil {
		fields = append(fields, operator.Split.Field)
	}
	projection, next, prefix, err := compileProjection(&plan.Project{Mode: plan.ProjectModeTable, Fields: fields, Range: operator.Range}, state, "", stage)
	if err != nil {
		return CompiledQuery{}, err
	}
	projected := "SELECT " + strings.Join(projection, ", ") + " FROM (" + relation.sql + ")"
	relation = relation.selectFrom(projected, operator.Range)
	return finalizeOrdinaryQuery(relation, next, prependArguments(prefix, args), scan, stage)
}

func newTimechartRangeDiscovery(operator *plan.Timechart, scan *plan.Scan, query *plan.Query, compiler Compiler, continuation *compiledTimechartContinuation) *compiledTimechartRangeDiscovery {
	copied := *operator
	copied.GridBoundaries = slices.Clone(operator.GridBoundaries)
	if operator.Split != nil {
		split := *operator.Split
		copied.Split = &split
	}
	copiedScan := *scan
	copiedScan.Indexes = slices.Clone(scan.Indexes)
	compiler.relationInput = nil
	return &compiledTimechartRangeDiscovery{operator: copied, scan: copiedScan, searchStart: query.SearchStart, timezone: query.SearchTimezone, compiler: compiler, continuation: continuation}
}

func (compiled CompiledQuery) continueObservedTimechart(ctx context.Context, input *compiledRelationInput) (CompiledQuery, error) {
	discovery := compiled.rangeDiscovery
	operator := discovery.operator
	var earliest, latest time.Time
	for _, row := range input.rows {
		timestamp, ok := row[0].(time.Time)
		if !ok {
			return CompiledQuery{}, errors.New("timechart input extent requires timestamp _time")
		}
		if earliest.IsZero() || timestamp.Before(earliest) {
			earliest = timestamp
		}
		if latest.IsZero() || timestamp.After(latest) {
			latest = timestamp
		}
	}
	if earliest.IsZero() {
		earliest = discovery.scan.Earliest
		latest = earliest
	}
	latest = latest.Add(time.Nanosecond)
	if err := plan.ResolveTimechartGrid(&operator, earliest, latest, discovery.timezone); err != nil {
		return CompiledQuery{}, err
	}
	fields, dynamic := timechartStageOutput(&operator)
	query := &plan.Query{Operators: []plan.Operator{&discovery.scan, &operator}, OutputFields: fields, DynamicOutput: dynamic, SearchStart: discovery.searchStart, SearchTimezone: discovery.timezone, EffectiveIndexes: slices.Clone(discovery.scan.Indexes)}
	compiler := discovery.compiler
	compiler.relationInput = input
	result, err := compiler.CompileContext(ctx, query)
	if err != nil {
		return CompiledQuery{}, err
	}
	result.continuation = discovery.continuation
	result.atomicResult = true
	return result, nil
}

// RequiresTimechartInputDiscovery distinguishes the bounded input capture from a result pivot.
func (compiled CompiledQuery) RequiresTimechartInputDiscovery() bool {
	return compiled.rangeDiscovery != nil
}
