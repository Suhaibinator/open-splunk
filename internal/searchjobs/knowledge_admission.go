package searchjobs

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"slices"
	"time"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/knowledgecatalog"
	"github.com/Suhaibinator/open-splunk/internal/knowledgeprogram"
	"github.com/Suhaibinator/open-splunk/internal/knowledgesnapshot"
	"github.com/Suhaibinator/open-splunk/internal/plan"
	"github.com/Suhaibinator/open-splunk/internal/spl"
)

type preparedKnowledgeAdmission struct {
	compiled          clickhouse.CompiledQuery
	snapshot          knowledgesnapshot.Snapshot
	wildcardExpansion plan.StatsWildcardExpansion
	summary           *opensplunkv1.KnowledgeSnapshotSummary
	effective         []string
	metadataBytes     uint64
	remainingRuntime  time.Duration
}

var errSearchJobIDRequired = errors.New("search job ID is required before addinfo compilation")

func normalizedKnowledgeResolver(resolver KnowledgeResolver) KnowledgeResolver {
	if isNilRequiredDependency(resolver) {
		return nil
	}
	return resolver
}

func (manager *Manager) knowledgeAdmissionEnabled(request CreateRequest) bool {
	return manager.knowledgeResolver != nil && request.AppID != ""
}

// prepareKnowledgeAdmission parses, plans, resolves, compiles, and seals one
// configured request before an ID exists or any durable/public side effect is
// possible. The shared fail-fast planning gate prevents configured admissions
// from bypassing the existing synchronous CPU bound.
func (manager *Manager) prepareKnowledgeAdmission(
	ctx context.Context,
	request CreateRequest,
	visibilityCutoff uint64,
	searchStart time.Time,
) (preparedKnowledgeAdmission, error) {
	return manager.prepareKnowledgeAdmissionForJob(
		ctx,
		request,
		"",
		visibilityCutoff,
		searchStart,
	)
}

func (manager *Manager) prepareKnowledgeAdmissionForJob(
	ctx context.Context,
	request CreateRequest,
	searchJobID string,
	visibilityCutoff uint64,
	searchStart time.Time,
) (preparedKnowledgeAdmission, error) {
	select {
	case manager.validationGate <- struct{}{}:
		defer func() { <-manager.validationGate }()
	default:
		if err := manager.operationContextError(ctx); err != nil {
			return preparedKnowledgeAdmission{}, err
		}
		return preparedKnowledgeAdmission{}, ErrCapacity
	}

	admissionContext, cancelAdmission := context.WithCancel(ctx)
	stopManagerCancellation := context.AfterFunc(manager.ctx, cancelAdmission)
	defer func() {
		stopManagerCancellation()
		cancelAdmission()
	}()

	parsed, err := parseSPLQuery(admissionContext, request.SPL)
	if err != nil {
		return preparedKnowledgeAdmission{}, manager.safeSPLAdmissionError(ctx, err, true)
	}
	if searchJobID == "" && parsedContainsAddInfo(parsed) {
		return preparedKnowledgeAdmission{}, errSearchJobIDRequired
	}
	scope := plan.Scope{
		TenantID:          request.TenantID,
		AuthorizedIndexes: cloneStrings(request.AuthorizedIndexes),
		RequestedIndexes:  cloneStrings(request.RequestedIndexes),
		SearchJobID:       searchJobID,
		Earliest:          request.TimeRange.Earliest(),
		Latest:            request.TimeRange.Latest(),
		SearchStart:       searchStart,
		SearchTimezone:    request.TimeRange.Intent().Timezone,
		IndexTimeCutoff:   searchStart,
		VisibilityCutoff:  &visibilityCutoff,
	}
	wildcardPreparation, err := plan.PrepareStatsWildcard(parsed, scope)
	if err != nil {
		return preparedKnowledgeAdmission{}, manager.safeSPLAdmissionError(ctx, err, false)
	}
	logical := wildcardPreparation.FullPlan()
	prefix := wildcardPreparation.Prefix()
	wildcardRequest := wildcardPreparation.Request()
	if logical == nil && (prefix == nil || wildcardRequest.IsZero()) {
		return preparedKnowledgeAdmission{}, ErrKnowledgeUnavailable
	}
	resolutionPlan := logical
	if resolutionPlan == nil {
		resolutionPlan = prefix
	}
	if err := manager.operationContextError(admissionContext); err != nil {
		return preparedKnowledgeAdmission{}, manager.safeKnowledgeAdmissionError(ctx, err)
	}

	expectedResolutionScope := knowledgecatalog.ResolutionScope{
		TenantID:                   request.TenantID,
		PrincipalID:                request.OwnerID,
		AppID:                      request.AppID,
		EffectiveAuthorizedIndexes: cloneStrings(resolutionPlan.EffectiveIndexes),
	}
	resolverInput := expectedResolutionScope
	resolverInput.EffectiveAuthorizedIndexes = cloneStrings(expectedResolutionScope.EffectiveAuthorizedIndexes)
	resolution, err := manager.knowledgeResolver.Resolve(admissionContext, resolverInput)
	if err != nil {
		return preparedKnowledgeAdmission{}, manager.safeKnowledgeAdmissionError(ctx, err)
	}
	if err := manager.operationContextError(admissionContext); err != nil {
		return preparedKnowledgeAdmission{}, manager.safeKnowledgeAdmissionError(ctx, err)
	}
	if err := exactKnowledgeResolution(resolution, expectedResolutionScope); err != nil {
		return preparedKnowledgeAdmission{}, ErrKnowledgeUnavailable
	}
	lookupResolutions, automaticLookups, err := manager.resolveLookupAdmission(
		admissionContext,
		request,
		parsed,
	)
	if err != nil {
		return preparedKnowledgeAdmission{}, manager.safeKnowledgeAdmissionError(ctx, err)
	}
	if err := manager.operationContextError(admissionContext); err != nil {
		return preparedKnowledgeAdmission{}, manager.safeKnowledgeAdmissionError(ctx, err)
	}
	prelude := resolution.Prelude()
	var wildcardExpansion plan.StatsWildcardExpansion
	var compiled clickhouse.CompiledQuery
	remainingRuntime := manager.maxRuntime
	if logical == nil {
		inventoryPrefix, injectErr := plan.InjectKnowledgePrelude(prefix, prelude)
		if injectErr != nil {
			return preparedKnowledgeAdmission{}, ErrKnowledgeUnavailable
		}
		inventoryPrefix, inventoryCompiler, compilerErr := configureResolvedPlanLookups(
			admissionContext,
			manager.compiler,
			inventoryPrefix,
			automaticLookups,
			lookupResolutions,
		)
		if compilerErr != nil {
			return preparedKnowledgeAdmission{}, manager.safeKnowledgeAdmissionError(ctx, compilerErr)
		}
		discoveryContext, cancelDiscovery := context.WithTimeout(
			admissionContext,
			manager.maxRuntime,
		)
		var inventory clickhouse.CompiledStatsWildcardInventory
		var inventoryRuntime time.Duration
		wildcardExpansion, inventory, inventoryRuntime, err = manager.executeStatsWildcardInventory(
			discoveryContext,
			inventoryCompiler,
			inventoryPrefix,
			wildcardRequest,
		)
		cancelDiscovery()
		if err != nil {
			var diagnostic *plan.Diagnostic
			if errors.As(err, &diagnostic) {
				return preparedKnowledgeAdmission{}, manager.safeSPLAdmissionError(ctx, err, false)
			}
			return preparedKnowledgeAdmission{}, manager.safeKnowledgeAdmissionError(ctx, err)
		}
		var remainingOK bool
		remainingRuntime, remainingOK = remainingStatsWildcardRuntime(
			manager.maxRuntime,
			inventoryRuntime,
		)
		if !remainingOK {
			return preparedKnowledgeAdmission{}, manager.safeKnowledgeAdmissionError(
				ctx,
				context.DeadlineExceeded,
			)
		}
		logical, err = plan.BuildWithStatsWildcardExpansion(parsed, scope, wildcardExpansion)
		if err != nil {
			return preparedKnowledgeAdmission{}, manager.safeSPLAdmissionError(ctx, err, false)
		}
		logical, err = plan.InjectKnowledgePrelude(logical, prelude)
		if err != nil {
			return preparedKnowledgeAdmission{}, ErrKnowledgeUnavailable
		}
		logical, fullCompiler, compilerErr := configureResolvedPlanLookups(
			admissionContext,
			manager.compiler,
			logical,
			automaticLookups,
			lookupResolutions,
		)
		if compilerErr != nil {
			return preparedKnowledgeAdmission{}, manager.safeKnowledgeAdmissionError(ctx, compilerErr)
		}
		compiledCandidate, compileErr := fullCompiler.CompileContext(
			admissionContext,
			logical,
		)
		if compileErr != nil {
			return preparedKnowledgeAdmission{}, manager.safeKnowledgeAdmissionError(ctx, compileErr)
		}
		sameReadScope, scopeErr := inventory.SameReadScopeContext(
			admissionContext,
			compiledCandidate,
		)
		if scopeErr != nil {
			return preparedKnowledgeAdmission{}, manager.safeKnowledgeAdmissionError(ctx, scopeErr)
		}
		if !sameReadScope {
			return preparedKnowledgeAdmission{}, ErrKnowledgeUnavailable
		}
		compiled = compiledCandidate
	} else {
		logical, err = plan.InjectKnowledgePrelude(logical, prelude)
	}
	if err != nil {
		return preparedKnowledgeAdmission{}, ErrKnowledgeUnavailable
	}

	if wildcardExpansion.IsZero() {
		logical, resolvedCompiler, compilerErr := configureResolvedPlanLookups(
			admissionContext,
			manager.compiler,
			logical,
			automaticLookups,
			lookupResolutions,
		)
		if compilerErr != nil {
			return preparedKnowledgeAdmission{}, manager.safeKnowledgeAdmissionError(ctx, compilerErr)
		}
		compiled, err = resolvedCompiler.CompileContext(admissionContext, logical)
		if err != nil {
			return preparedKnowledgeAdmission{}, manager.safeKnowledgeAdmissionError(ctx, err)
		}
	}
	if err := manager.operationContextError(admissionContext); err != nil {
		return preparedKnowledgeAdmission{}, manager.safeKnowledgeAdmissionError(ctx, err)
	}
	detachedCompiled, ok, detachErr := compiled.CloneForExecutionContext(admissionContext)
	if detachErr != nil {
		return preparedKnowledgeAdmission{}, manager.safeKnowledgeAdmissionError(ctx, detachErr)
	}
	if !ok {
		return preparedKnowledgeAdmission{}, ErrKnowledgeUnavailable
	}
	compiledBytes, ok, retainedErr := detachedCompiled.RetainedBytesContext(admissionContext)
	if retainedErr != nil {
		return preparedKnowledgeAdmission{}, manager.safeKnowledgeAdmissionError(ctx, retainedErr)
	}
	if !ok {
		return preparedKnowledgeAdmission{}, ErrCapacity
	}
	snapshot, err := resolution.Finalize(detachedCompiled)
	if err != nil || snapshot.IsZero() {
		return preparedKnowledgeAdmission{}, manager.safeKnowledgeAdmissionError(ctx, err)
	}
	summary := snapshot.Summary()
	if err := knowledgesnapshot.ValidateSummary(summary); err != nil {
		return preparedKnowledgeAdmission{}, ErrKnowledgeUnavailable
	}
	metadataBytes, err := retainedKnowledgeAdmissionMetadata(snapshot, compiledBytes)
	if err != nil {
		return preparedKnowledgeAdmission{}, ErrCapacity
	}
	if !wildcardExpansion.IsZero() {
		wildcardBytes, ok := wildcardExpansion.RetainedBytes()
		if !ok {
			return preparedKnowledgeAdmission{}, ErrCapacity
		}
		metadataBytes, err = checkedAdd(metadataBytes, wildcardBytes)
		if err != nil {
			return preparedKnowledgeAdmission{}, ErrCapacity
		}
	}
	if err := manager.operationContextError(admissionContext); err != nil {
		return preparedKnowledgeAdmission{}, manager.safeKnowledgeAdmissionError(ctx, err)
	}
	return preparedKnowledgeAdmission{
		compiled:          detachedCompiled,
		snapshot:          snapshot,
		wildcardExpansion: wildcardExpansion.Clone(),
		summary:           summary,
		effective:         cloneStrings(logical.EffectiveIndexes),
		metadataBytes:     metadataBytes,
		remainingRuntime:  remainingRuntime,
	}, nil
}

func parsedContainsAddInfo(query *spl.Query) bool {
	if query == nil {
		return false
	}
	for _, command := range query.Commands {
		if _, ok := command.(*spl.AddInfoCommand); ok {
			return true
		}
	}
	return false
}

// exactKnowledgeResolution treats the resolver as a detached dependency, not
// as a trusted alias to caller-owned scope. Every scalar and bounded count is
// cross-checked before compilation can mint final evidence.
func exactKnowledgeResolution(
	resolution knowledgecatalog.Resolution,
	scope knowledgecatalog.ResolutionScope,
) error {
	if resolution.IsZero() {
		return ErrKnowledgeUnavailable
	}
	summary := resolution.Summary()
	objects := resolution.ObjectSummaries()
	dependencies := resolution.Dependencies()
	shadows := resolution.Shadows()
	prelude := resolution.Prelude()
	static := resolution.StaticCharges()
	if summary.TenantID != scope.TenantID ||
		summary.PrincipalID != scope.PrincipalID ||
		summary.AppID != scope.AppID ||
		!slices.Equal(summary.EffectiveAuthorizedIndexes, scope.EffectiveAuthorizedIndexes) ||
		summary.ExecutableObjects != uint32(len(objects)) || // #nosec G115 -- resolver authority collections are resource-bounded.
		summary.Dependencies != uint32(len(dependencies)) || // #nosec G115 -- resolver authority collections are resource-bounded.
		summary.Shadows != uint32(len(shadows)) || // #nosec G115 -- resolver authority collections are resource-bounded.
		len(summary.TenantCatalogStateToken) != sha256.Size || prelude.IsZero() ||
		prelude.ObjectCount() != summary.ExecutableObjects ||
		prelude.IsEmpty() != (summary.ExecutableObjects == 0) ||
		!knowledgePreludeChargesMatch(prelude.Charges(), static) {
		return ErrKnowledgeUnavailable
	}
	// A second detached read must be exact. This detects a dependency that
	// incorrectly exposes mutable aliases even when its first values matched.
	again := resolution.Summary()
	if again.TenantID != summary.TenantID ||
		again.PrincipalID != summary.PrincipalID ||
		again.AppID != summary.AppID ||
		again.TenantCatalogRevision != summary.TenantCatalogRevision ||
		!bytes.Equal(again.TenantCatalogStateToken, summary.TenantCatalogStateToken) ||
		!equalOptionalUint64(again.AppCatalogRevision, summary.AppCatalogRevision) ||
		!slices.Equal(again.EffectiveAuthorizedIndexes, summary.EffectiveAuthorizedIndexes) ||
		again.ExecutableObjects != summary.ExecutableObjects ||
		again.Dependencies != summary.Dependencies ||
		again.Shadows != summary.Shadows {
		return ErrKnowledgeUnavailable
	}
	if !prelude.Equal(resolution.Prelude()) {
		return ErrKnowledgeUnavailable
	}
	return nil
}

func knowledgePreludeChargesMatch(
	charges knowledgeprogram.Charges,
	static knowledgecatalog.ResolutionStaticCharges,
) bool {
	return charges.GeneratedFields == static.GeneratedFields &&
		charges.RegexPrograms == static.ExtractionRegexPrograms &&
		charges.RegexWorkUnits == static.ExtractionRegexWorkUnits &&
		charges.ExtractionOutputs == static.ExtractionOutputs &&
		charges.JSONEvaluationWork == static.JSONEvaluationWorkUnits &&
		charges.ScalarExpressions == static.ScalarExpressions &&
		charges.ScalarExpressionNodes == static.ScalarExpressionNodes &&
		charges.ScalarPredicates == static.ScalarPredicates
}

func equalOptionalUint64(left, right *uint64) bool {
	return left == nil && right == nil || left != nil && right != nil && *left == *right
}

func (manager *Manager) safeSPLAdmissionError(caller context.Context, err error, parsing bool) error {
	if operationErr := manager.operationContextError(caller); operationErr != nil {
		return operationErr
	}
	failure := planningFailure(err)
	if parsing {
		failure = parseFailure(err)
	}
	if failure.Code == FailureUnsupportedSPL {
		return ErrUnsupportedSPL
	}
	return ErrInvalidSPL
}

func (manager *Manager) safeKnowledgeAdmissionError(caller context.Context, err error) error {
	if operationErr := manager.operationContextError(caller); operationErr != nil {
		return operationErr
	}
	if errors.Is(err, ErrCapacity) ||
		errors.Is(err, control.ErrCapacityExceeded) ||
		errors.Is(err, knowledgesnapshot.ErrResourceLimit) {
		return ErrCapacity
	}
	return ErrKnowledgeUnavailable
}
