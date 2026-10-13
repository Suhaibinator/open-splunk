package main

import (
	"context"
	"strconv"

	"github.com/Suhaibinator/open-splunk/internal/authorization"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
)

// Planning-only analysis still carries a current user and selected organization.
// It must not turn a caller's cached index-name slice into search authority.
type runtimeAnalysisAuthority struct {
	jobs     *searchjobs.Manager
	identity *runtimeIdentity
}

func (authority runtimeAnalysisAuthority) CompletedExecutionSnapshotFor(ctx context.Context, access searchjobs.AccessScope, id string) (searchjobs.ExecutionSnapshot, error) {
	job, err := authority.jobs.GetForContext(ctx, access, id)
	if err != nil {
		return searchjobs.ExecutionSnapshot{}, err
	}
	if err := authority.identity.authorizeJob(ctx, access, job); err != nil {
		return searchjobs.ExecutionSnapshot{}, err
	}
	return authority.jobs.CompletedExecutionSnapshotFor(ctx, access, id)
}
func (authority runtimeAnalysisAuthority) Validate(ctx context.Context, request searchjobs.ValidateRequest) (searchjobs.ValidationResult, error) {
	if err := authority.authorize(ctx, request.TenantID, request.AuthorizedIndexes); err != nil {
		return searchjobs.ValidationResult{}, err
	}
	return authority.jobs.Validate(ctx, request)
}
func (authority runtimeAnalysisAuthority) SnapshotAnalysisScope(ctx context.Context, request searchjobs.AnalysisScopeRequest) (searchjobs.AnalysisScopeSnapshot, error) {
	if err := authority.authorize(ctx, request.TenantID, request.AuthorizedIndexes); err != nil {
		return searchjobs.AnalysisScopeSnapshot{}, err
	}
	return authority.jobs.SnapshotAnalysisScope(ctx, request)
}
func (authority runtimeAnalysisAuthority) authorize(ctx context.Context, tenant string, names []string) error {
	scope, ok := authorization.FromContext(ctx)
	if !ok || tenant != authority.identity.tenantID || scope.PhysicalTenant != tenant || len(names) == 0 {
		return authorization.ErrForbidden
	}
	_, subject, err := authority.identity.freshAccessAndSubject(ctx, scope.Subject.OwnerID(), strconv.FormatInt(scope.OrganizationID, 10))
	if err != nil {
		return err
	}
	indexes, err := authority.identity.indexes.GetIndexesByNames(ctx, names)
	if err != nil {
		return err
	}
	if len(indexes) != len(names) {
		return authorization.ErrForbidden
	}
	for i, index := range indexes {
		if index.Definition.Name != names[i] || index.State != control.IndexStateActive || !index.Definition.SearchEnabled {
			return authorization.ErrForbidden
		}
		if err := authority.identity.authorization.AuthorizeIndex(ctx, subject, scope.OrganizationID, index.ID, authorization.Search); err != nil {
			return err
		}
	}
	return nil
}
