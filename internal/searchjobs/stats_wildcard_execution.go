package searchjobs

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
	"github.com/Suhaibinator/open-splunk/internal/nilcheck"
	"github.com/Suhaibinator/open-splunk/internal/plan"
	"github.com/Suhaibinator/open-splunk/internal/searchlimits"
	"github.com/Suhaibinator/open-splunk/internal/spl"
)

// prepareAndCompileStatsWildcard returns an ordinary full plan when no runtime
// field inventory is required. Otherwise it executes ordered compiler-sealed
// inventories for stats and convert, sharing one runtime budget and retaining
// the complete replay evidence chain.
func (manager *Manager) prepareAndCompileStatsWildcard(
	ctx context.Context,
	parsed *spl.Query,
	scope plan.Scope,
	limits searchlimits.Policy,
) (*plan.Query, clickhouse.CompiledQuery, plan.StatsWildcardExpansion, time.Duration, error) {
	remainingRuntime := limits.MaxRuntime
	preparation, err := plan.PrepareStatsWildcard(parsed, scope)
	if err != nil {
		return nil, clickhouse.CompiledQuery{}, plan.StatsWildcardExpansion{}, 0, err
	}
	discoveryContext := ctx
	if manager.limitSource != nil {
		discoveryContext = searchlimits.WithPolicy(discoveryContext, limits)
	}
	var expansion plan.StatsWildcardExpansion
	inventories := []clickhouse.CompiledStatsWildcardInventory{}
	for preparation.FullPlan() == nil {
		request, prefix := preparation.Request(), preparation.Prefix()
		if request.IsZero() || prefix == nil {
			return nil, clickhouse.CompiledQuery{}, plan.StatsWildcardExpansion{}, 0, errors.New("compile search query: wildcard preparation is incomplete")
		}
		var inventory clickhouse.CompiledStatsWildcardInventory
		var consumed time.Duration
		expansion, inventory, consumed, err = manager.executeStatsWildcardInventory(discoveryContext, manager.compiler, prefix, request, remainingRuntime)
		if err != nil {
			return nil, clickhouse.CompiledQuery{}, plan.StatsWildcardExpansion{}, 0, err
		}
		var ok bool
		remainingRuntime, ok = remainingStatsWildcardRuntime(remainingRuntime, consumed)
		if !ok {
			return nil, clickhouse.CompiledQuery{}, plan.StatsWildcardExpansion{}, 0, context.DeadlineExceeded
		}
		inventories = append(inventories, inventory)
		preparation, err = plan.PrepareNextStatsWildcard(parsed, scope, expansion)
		if err != nil {
			return nil, clickhouse.CompiledQuery{}, plan.StatsWildcardExpansion{}, 0, err
		}
	}
	logical := preparation.FullPlan()
	compiled, err := manager.compiler.CompileContext(ctx, logical)
	if err != nil {
		return nil, clickhouse.CompiledQuery{}, plan.StatsWildcardExpansion{}, 0, err
	}
	for _, inventory := range inventories {
		sameReadScope, scopeErr := inventory.SameReadScopeContext(ctx, compiled)
		if scopeErr != nil {
			return nil, clickhouse.CompiledQuery{}, plan.StatsWildcardExpansion{}, 0, scopeErr
		}
		if !sameReadScope {
			return nil, clickhouse.CompiledQuery{}, plan.StatsWildcardExpansion{}, 0, fmt.Errorf("%w: wildcard read scope changed", ErrInvalidResult)
		}
	}
	return logical, compiled, expansion.Clone(), remainingRuntime, nil
}

func (manager *Manager) executeStatsWildcardInventory(
	ctx context.Context,
	compiler clickhouse.Compiler,
	prefix *plan.Query,
	request plan.StatsWildcardRequest,
	maximumRuntime time.Duration,
) (
	plan.StatsWildcardExpansion,
	clickhouse.CompiledStatsWildcardInventory,
	time.Duration,
	error,
) {
	if ctx == nil {
		return plan.StatsWildcardExpansion{}, clickhouse.CompiledStatsWildcardInventory{}, 0, errors.New("execute stats wildcard inventory: context is nil")
	}
	if err := ctx.Err(); err != nil {
		return plan.StatsWildcardExpansion{}, clickhouse.CompiledStatsWildcardInventory{}, 0, err
	}
	if maximumRuntime <= 0 {
		return plan.StatsWildcardExpansion{}, clickhouse.CompiledStatsWildcardInventory{}, 0,
			context.DeadlineExceeded
	}
	capability, ok := manager.executor.(StatsWildcardInventoryExecutor)
	if !ok || nilcheck.IsNil(capability) {
		return plan.StatsWildcardExpansion{}, clickhouse.CompiledStatsWildcardInventory{}, 0, ErrUnsupportedSPL
	}
	compiled, err := compiler.CompileStatsWildcardInventoryContext(ctx, prefix, request)
	if err != nil {
		return plan.StatsWildcardExpansion{}, clickhouse.CompiledStatsWildcardInventory{}, 0, err
	}
	pristine, ok, cloneErr := compiled.CloneForExecutionContext(ctx)
	if cloneErr != nil {
		return plan.StatsWildcardExpansion{}, clickhouse.CompiledStatsWildcardInventory{}, 0, cloneErr
	}
	equal, equalErr := compiled.EqualForExecutionContext(ctx, pristine)
	if equalErr != nil {
		return plan.StatsWildcardExpansion{}, clickhouse.CompiledStatsWildcardInventory{}, 0, equalErr
	}
	if !ok || !equal {
		return plan.StatsWildcardExpansion{}, clickhouse.CompiledStatsWildcardInventory{}, 0, ErrInvalidResult
	}
	executable, ok, cloneErr := pristine.CloneForExecutionContext(ctx)
	if cloneErr != nil {
		return plan.StatsWildcardExpansion{}, clickhouse.CompiledStatsWildcardInventory{}, 0, cloneErr
	}
	equal, equalErr = executable.EqualForExecutionContext(ctx, pristine)
	if equalErr != nil {
		return plan.StatsWildcardExpansion{}, clickhouse.CompiledStatsWildcardInventory{}, 0, equalErr
	}
	if !ok || !equal {
		return plan.StatsWildcardExpansion{}, clickhouse.CompiledStatsWildcardInventory{}, 0, ErrInvalidResult
	}
	// Compilation and seal validation are planning work, matching the ordinary
	// query path. Arm MaxRuntime only at the storage-execution boundary so
	// scheduler or compiler latency cannot consume an unaccounted budget.
	inventoryStarted := time.Now()
	executionContext, cancelExecution := context.WithTimeout(ctx, maximumRuntime)
	defer cancelExecution()
	expansion, err := capability.ExecuteStatsWildcardInventory(executionContext, executable)
	inventoryRuntime := time.Since(inventoryStarted)
	executionContextErr := executionContext.Err()
	cancelExecution()
	equal, equalErr = executable.EqualForExecutionContext(ctx, pristine)
	if equalErr != nil {
		return plan.StatsWildcardExpansion{}, clickhouse.CompiledStatsWildcardInventory{}, inventoryRuntime, equalErr
	}
	if !equal {
		return plan.StatsWildcardExpansion{}, clickhouse.CompiledStatsWildcardInventory{}, inventoryRuntime, ErrInvalidResult
	}
	if executionContextErr != nil {
		return plan.StatsWildcardExpansion{}, clickhouse.CompiledStatsWildcardInventory{}, inventoryRuntime, executionContextErr
	}
	if contextErr := ctx.Err(); contextErr != nil {
		return plan.StatsWildcardExpansion{}, clickhouse.CompiledStatsWildcardInventory{}, inventoryRuntime, contextErr
	}
	if err != nil {
		return plan.StatsWildcardExpansion{}, clickhouse.CompiledStatsWildcardInventory{}, inventoryRuntime, err
	}
	if !expansion.ValidForRequest(request) {
		return plan.StatsWildcardExpansion{}, clickhouse.CompiledStatsWildcardInventory{}, inventoryRuntime, ErrInvalidResult
	}
	return expansion.Clone(), pristine, inventoryRuntime, nil
}

func remainingStatsWildcardRuntime(maximum, consumed time.Duration) (time.Duration, bool) {
	if maximum <= 0 || consumed < 0 || consumed >= maximum {
		return 0, false
	}
	return maximum - consumed, true
}

// retainStatsWildcardExpansion accounts and attaches replay evidence while the
// job is still in planning. Terminal transitions retain metadata until normal
// job cleanup, matching every other immutable execution authority.
func (manager *Manager) retainStatsWildcardExpansion(
	entry *jobEntry,
	expansion plan.StatsWildcardExpansion,
) error {
	if entry == nil || expansion.IsZero() {
		return ErrInvalidResult
	}
	retainedBytes, ok := expansion.RetainedBytes()
	if !ok || retainedBytes == 0 {
		return ErrInvalidResult
	}
	if err := manager.reserveMetadataWithCleanup(retainedBytes); err != nil {
		return err
	}
	committed := false
	defer func() {
		if !committed {
			manager.releaseMetadata(retainedBytes)
		}
	}()
	entry.mu.Lock()
	defer entry.mu.Unlock()
	if entry.job.State != StatePlanning || entry.ctx.Err() != nil ||
		!entry.statsWildcardExpansion.IsZero() {
		return ErrInvalidResult
	}
	entry.statsWildcardExpansion = expansion.Clone()
	next, err := checkedAdd(entry.metadataBytes, retainedBytes)
	if err != nil {
		entry.statsWildcardExpansion = plan.StatsWildcardExpansion{}
		return ErrCapacity
	}
	entry.metadataBytes = next
	committed = true
	return nil
}
