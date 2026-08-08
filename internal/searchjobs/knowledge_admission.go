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
	"github.com/Suhaibinator/open-splunk/internal/knowledgesnapshot"
	"github.com/Suhaibinator/open-splunk/internal/plan"
)

type preparedKnowledgeAdmission struct {
	compiled      clickhouse.CompiledQuery
	snapshot      knowledgesnapshot.Snapshot
	summary       *opensplunkv1.KnowledgeSnapshotSummary
	effective     []string
	metadataBytes uint64
}

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
	scope := plan.Scope{
		TenantID:          request.TenantID,
		AuthorizedIndexes: cloneStrings(request.AuthorizedIndexes),
		RequestedIndexes:  cloneStrings(request.RequestedIndexes),
		Earliest:          request.TimeRange.Earliest(),
		Latest:            request.TimeRange.Latest(),
		SearchStart:       searchStart,
		SearchTimezone:    request.TimeRange.Intent().Timezone,
		IndexTimeCutoff:   searchStart,
		VisibilityCutoff:  &visibilityCutoff,
	}
	logical, err := plan.Build(parsed, scope)
	if err != nil {
		return preparedKnowledgeAdmission{}, manager.safeSPLAdmissionError(ctx, err, false)
	}
	if err := manager.operationContextError(admissionContext); err != nil {
		return preparedKnowledgeAdmission{}, manager.safeKnowledgeAdmissionError(ctx, err)
	}

	expectedResolutionScope := knowledgecatalog.ResolutionScope{
		TenantID:                   request.TenantID,
		PrincipalID:                request.OwnerID,
		AppID:                      request.AppID,
		EffectiveAuthorizedIndexes: cloneStrings(logical.EffectiveIndexes),
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

	compiled, err := manager.compiler.Compile(logical)
	if err != nil {
		return preparedKnowledgeAdmission{}, manager.safeKnowledgeAdmissionError(ctx, err)
	}
	if err := manager.operationContextError(admissionContext); err != nil {
		return preparedKnowledgeAdmission{}, manager.safeKnowledgeAdmissionError(ctx, err)
	}
	detachedCompiled, ok := compiled.CloneForExecution()
	if !ok {
		return preparedKnowledgeAdmission{}, ErrKnowledgeUnavailable
	}
	compiledBytes, ok := detachedCompiled.RetainedBytes()
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
	if err := manager.operationContextError(admissionContext); err != nil {
		return preparedKnowledgeAdmission{}, manager.safeKnowledgeAdmissionError(ctx, err)
	}
	return preparedKnowledgeAdmission{
		compiled:      detachedCompiled,
		snapshot:      snapshot,
		summary:       summary,
		effective:     cloneStrings(logical.EffectiveIndexes),
		metadataBytes: metadataBytes,
	}, nil
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
	if summary.TenantID != scope.TenantID ||
		summary.PrincipalID != scope.PrincipalID ||
		summary.AppID != scope.AppID ||
		!slices.Equal(summary.EffectiveAuthorizedIndexes, scope.EffectiveAuthorizedIndexes) ||
		summary.ExecutableObjects != uint32(len(objects)) ||
		summary.Dependencies != uint32(len(dependencies)) ||
		summary.Shadows != uint32(len(shadows)) ||
		len(summary.TenantCatalogStateToken) != sha256.Size {
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
	return nil
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
	if errors.Is(err, control.ErrCapacityExceeded) || errors.Is(err, knowledgesnapshot.ErrResourceLimit) {
		return ErrCapacity
	}
	return ErrKnowledgeUnavailable
}
