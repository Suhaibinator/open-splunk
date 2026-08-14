package searchjobs

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
	"github.com/Suhaibinator/open-splunk/internal/plan"
	"github.com/Suhaibinator/open-splunk/internal/spl"
)

// prepareAndCompileStatsWildcard returns an ordinary full plan when no runtime
// field inventory is required. When the first wildcard stats stage sees an
// open schema, it executes one compiler-sealed, bounded inventory query and
// rebuilds the original AST with opaque expansion evidence.
func (manager *Manager) prepareAndCompileStatsWildcard(
	ctx context.Context,
	parsed *spl.Query,
	scope plan.Scope,
) (*plan.Query, clickhouse.CompiledQuery, plan.StatsWildcardExpansion, time.Duration, error) {
	preparation, err := plan.PrepareStatsWildcard(parsed, scope)
	if err != nil {
		return nil, clickhouse.CompiledQuery{}, plan.StatsWildcardExpansion{}, 0, err
	}
	if full := preparation.FullPlan(); full != nil {
		compiled, compileErr := manager.compiler.CompileContext(ctx, full)
		return full, compiled, plan.StatsWildcardExpansion{}, manager.maxRuntime, compileErr
	}
	request := preparation.Request()
	prefix := preparation.Prefix()
	if request.IsZero() || prefix == nil {
		return nil, clickhouse.CompiledQuery{}, plan.StatsWildcardExpansion{}, 0,
			errors.New("compile search query: wildcard preparation is incomplete")
	}
	discoveryContext, cancelDiscovery := context.WithTimeout(ctx, manager.maxRuntime)
	expansion, inventory, inventoryRuntime, err := manager.executeStatsWildcardInventory(
		discoveryContext,
		manager.compiler,
		prefix,
		request,
	)
	cancelDiscovery()
	if err != nil {
		return nil, clickhouse.CompiledQuery{}, plan.StatsWildcardExpansion{}, 0, err
	}
	remainingRuntime, ok := remainingStatsWildcardRuntime(manager.maxRuntime, inventoryRuntime)
	if !ok {
		return nil, clickhouse.CompiledQuery{}, plan.StatsWildcardExpansion{}, 0,
			context.DeadlineExceeded
	}
	logical, err := plan.BuildWithStatsWildcardExpansion(parsed, scope, expansion)
	if err != nil {
		return nil, clickhouse.CompiledQuery{}, plan.StatsWildcardExpansion{}, 0, err
	}
	compiled, err := manager.compiler.CompileContext(ctx, logical)
	if err != nil {
		return nil, clickhouse.CompiledQuery{}, plan.StatsWildcardExpansion{}, 0, err
	}
	sameReadScope, scopeErr := inventory.SameReadScopeContext(ctx, compiled)
	if scopeErr != nil {
		return nil, clickhouse.CompiledQuery{}, plan.StatsWildcardExpansion{}, 0,
			scopeErr
	}
	if !sameReadScope {
		return nil, clickhouse.CompiledQuery{}, plan.StatsWildcardExpansion{}, 0,
			fmt.Errorf("%w: stats wildcard read scope changed", ErrInvalidResult)
	}
	return logical, compiled, expansion.Clone(), remainingRuntime, nil
}

func (manager *Manager) executeStatsWildcardInventory(
	ctx context.Context,
	compiler clickhouse.Compiler,
	prefix *plan.Query,
	request plan.StatsWildcardRequest,
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
	capability, ok := manager.executor.(StatsWildcardInventoryExecutor)
	if !ok || isNilRequiredDependency(capability) {
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
	inventoryStarted := time.Now()
	expansion, err := capability.ExecuteStatsWildcardInventory(ctx, executable)
	inventoryRuntime := time.Since(inventoryStarted)
	equal, equalErr = executable.EqualForExecutionContext(ctx, pristine)
	if equalErr != nil {
		return plan.StatsWildcardExpansion{}, clickhouse.CompiledStatsWildcardInventory{}, inventoryRuntime, equalErr
	}
	if !equal {
		return plan.StatsWildcardExpansion{}, clickhouse.CompiledStatsWildcardInventory{}, inventoryRuntime, ErrInvalidResult
	}
	if contextErr := ctx.Err(); contextErr != nil {
		return plan.StatsWildcardExpansion{}, clickhouse.CompiledStatsWildcardInventory{}, inventoryRuntime, contextErr
	}
	if err != nil {
		return plan.StatsWildcardExpansion{}, clickhouse.CompiledStatsWildcardInventory{}, inventoryRuntime, err
	}
	if expansion.IsZero() {
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
