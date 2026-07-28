package searchjobs

import (
	"context"
	"errors"
	"strings"
)

// SnapshotAnalysisScope captures one real storage visibility boundary for a
// synchronous derived analysis without creating or retaining a search job.
// The caller supplies trusted authorization and a previously resolved time
// range; malformed scope fails before storage is consulted.
func (manager *Manager) SnapshotAnalysisScope(
	ctx context.Context,
	request AnalysisScopeRequest,
) (AnalysisScopeSnapshot, error) {
	if manager == nil {
		return AnalysisScopeSnapshot{}, errors.New("snapshot analysis scope: manager is nil")
	}
	if ctx == nil {
		return AnalysisScopeSnapshot{}, errors.New("snapshot analysis scope: context is nil")
	}
	if err := ctx.Err(); err != nil {
		return AnalysisScopeSnapshot{}, err
	}
	if err := manager.validatePlanningRequestSize(
		"",
		request.TenantID,
		request.AuthorizedIndexes,
		request.RequestedIndexes,
		request.TimeRange,
	); err != nil {
		return AnalysisScopeSnapshot{}, err
	}
	if !request.TimeRange.Valid() {
		return AnalysisScopeSnapshot{}, errors.New("snapshot analysis scope: resolved time range is required")
	}
	if !validAccessIdentity(request.TenantID) {
		return AnalysisScopeSnapshot{}, errors.New("snapshot analysis scope: tenant identity is invalid")
	}
	if err := validateAnalysisIndexes(request.AuthorizedIndexes, request.RequestedIndexes); err != nil {
		return AnalysisScopeSnapshot{}, err
	}
	if err := manager.beginSynchronousOperation(ctx); err != nil {
		return AnalysisScopeSnapshot{}, err
	}
	defer manager.endSynchronousOperation()

	// Clone only after bounded synchronous admission. Callers may reuse or
	// mutate their request as soon as this method returns.
	tenantID := strings.Clone(request.TenantID)
	authorizedIndexes := cloneStrings(request.AuthorizedIndexes)
	requestedIndexes := cloneStrings(request.RequestedIndexes)
	resolvedRange := request.TimeRange

	visibilityCutoff, err := manager.captureVisibility(ctx)
	if err != nil {
		return AnalysisScopeSnapshot{}, err
	}
	if err := manager.operationContextError(ctx); err != nil {
		return AnalysisScopeSnapshot{}, err
	}
	anchor := manager.nowUTC()
	if anchor.IsZero() {
		return AnalysisScopeSnapshot{}, errors.New("snapshot analysis scope: manager clock returned a zero time")
	}
	if err := manager.operationContextError(ctx); err != nil {
		return AnalysisScopeSnapshot{}, err
	}
	return AnalysisScopeSnapshot{
		TenantID:          tenantID,
		AuthorizedIndexes: authorizedIndexes,
		RequestedIndexes:  requestedIndexes,
		TimeRange:         resolvedRange,
		SearchStart:       anchor,
		IndexTimeCutoff:   anchor,
		VisibilityCutoff:  visibilityCutoff,
	}, nil
}

func validateAnalysisIndexes(authorizedIndexes, requestedIndexes []string) error {
	if len(authorizedIndexes) == 0 {
		return errors.New("snapshot analysis scope: at least one authorized index is required")
	}
	authorized := make(map[string]struct{}, len(authorizedIndexes))
	for _, index := range authorizedIndexes {
		if !validAccessIdentity(index) {
			return errors.New("snapshot analysis scope: authorized index is invalid")
		}
		authorized[index] = struct{}{}
	}
	for _, index := range requestedIndexes {
		if !validAccessIdentity(index) {
			return errors.New("snapshot analysis scope: requested index is invalid")
		}
		if _, ok := authorized[index]; !ok {
			return errors.New("snapshot analysis scope: requested index is outside the authorized scope")
		}
	}
	return nil
}
