package clickhouse

import (
	"context"
	"errors"
	"fmt"

	"github.com/Suhaibinator/open-splunk/internal/plan"
)

// WithDeferredLookupResolutionsContext pins the remaining authored lookup
// assets for a parser-owned timechart suffix. Compilation checks their exact
// ordered contracts before any prefix can execute.
func (compiler Compiler) WithDeferredLookupResolutionsContext(ctx context.Context, resolutions []LookupResolution) (Compiler, error) {
	all := append(append([]LookupResolution(nil), compiler.lookupResolutions...), resolutions...)
	validated, err := compiler.WithLookupResolutionsContext(ctx, all)
	if err != nil {
		return Compiler{}, err
	}
	var cells, keys uint64
	for _, resolution := range validated.lookupResolutions {
		selected, err := lookupSelectedCellCountContext(ctx, &resolution.contract, resolution)
		if err != nil {
			return Compiler{}, err
		}
		if selected > MaximumLookupSelectedCellsPerQuery-cells || uint64(len(resolution.contract.Keys)) > uint64(MaximumLookupMatchKeyComponentsPerEvent)-keys {
			return Compiler{}, errors.New("configure deferred lookup: cumulative work exceeds query budget")
		}
		cells += selected
		keys += uint64(len(resolution.contract.Keys))
	}
	compiler.deferredLookupResolutions = validated.lookupResolutions[len(compiler.lookupResolutions):]
	return compiler, nil
}

func (compiler Compiler) prepareTimechartContinuation(ctx context.Context, continuation plan.TimechartContinuation, scan *plan.Scan, prepared preparedLookupCompilation, consumed int, state *compileContext) (*compiledTimechartContinuation, error) {
	contracts, err := continuation.LookupContracts()
	if err != nil {
		return nil, err
	}
	resolutions := make([]LookupResolution, 0, len(prepared.stages)-consumed+len(compiler.deferredLookupResolutions))
	for _, stage := range prepared.stages[consumed:] {
		resolutions = append(resolutions, stage.resolution)
	}
	resolutions = append(resolutions, compiler.deferredLookupResolutions...)
	operators := []plan.Operator{scan}
	for i := range contracts {
		operators = append(operators, &contracts[i])
	}
	suffixPreparation, err := prepareLookupCompilationContext(ctx, &plan.Query{Operators: operators}, scan, resolutions)
	if err != nil {
		return nil, err
	}
	tables := make([]compiledLookupExternalTable, len(contracts))
	for i, stage := range suffixPreparation.stages {
		names := make([]string, len(stage.selectedColumns))
		for j := range names {
			names[j] = fmt.Sprintf("__os_lookup_%d_column_%d", i, j)
		}
		tables[i], err = newCompiledLookupExternalTableContext(ctx, fmt.Sprintf("__os_lookup_table_%d", i), fmt.Sprintf("__os_lookup_matched_%d", i), stage, names)
		if err != nil {
			return nil, err
		}
	}
	compiler.relationInput = nil
	compiler.lookupResolutions = nil
	compiler.deferredLookupResolutions = nil
	compiler.continuationBudget = timechartContinuationBudget(state)
	return &compiledTimechartContinuation{plan: continuation, compiler: compiler, lookups: tables}, nil
}

func (continuation *compiledTimechartContinuation) compilerForQuery(ctx context.Context, query *plan.Query) (Compiler, error) {
	contracts, err := continuation.plan.LookupContracts()
	if err != nil {
		return Compiler{}, err
	}
	if len(contracts) != len(continuation.lookups) {
		return Compiler{}, errors.New("continue timechart: incomplete lookup authority")
	}
	resolutions := make([]LookupResolution, len(contracts))
	for i, contract := range contracts {
		table := continuation.lookups[i]
		resolutions[i], err = lookupResolutionFromRetainedTable(ctx, contract, table.logicalID, table)
		if err != nil {
			return Compiler{}, err
		}
	}
	count := 0
	for _, operator := range query.Operators {
		if _, ok := operator.(*plan.Lookup); ok {
			count++
		}
	}
	if count > len(resolutions) {
		return Compiler{}, errors.New("continue timechart: lookup stage mismatch")
	}
	compiler, err := continuation.compiler.WithLookupResolutionsContext(ctx, resolutions[:count])
	if err != nil {
		return Compiler{}, err
	}
	return compiler.WithDeferredLookupResolutionsContext(ctx, resolutions[count:])
}

func (compiled CompiledQuery) deferredLookupTables() []compiledLookupExternalTable {
	if compiled.continuation != nil {
		return compiled.continuation.lookups
	}
	if compiled.rangeDiscovery != nil && compiled.rangeDiscovery.continuation != nil {
		return compiled.rangeDiscovery.continuation.lookups
	}
	return nil
}

func cloneTimechartContinuation(source *compiledTimechartContinuation) *compiledTimechartContinuation {
	if source == nil {
		return nil
	}
	clone := *source
	clone.lookups = cloneCompiledLookupExternalTables(source.lookups)
	return &clone
}
