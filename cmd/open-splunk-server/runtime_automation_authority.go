package main

import (
	"context"

	"github.com/Suhaibinator/open-splunk/internal/alerts"
	"github.com/Suhaibinator/open-splunk/internal/savedobjects"
)

// Source privileges come from the current run-as account, even when a schedule
// was configured before a promotion, demotion or membership revocation.
func (runtime *runtimeIdentity) scheduledSourceScope(ctx context.Context, scope savedobjects.AccessScope) (savedobjects.AccessScope, error) {
	access, err := runtime.freshAccess(ctx, scope.OwnerID, scope.OrganizationID)
	if err != nil {
		return savedobjects.AccessScope{}, err
	}
	return savedobjects.AccessScope{OwnerID: access.OwnerID, OrganizationID: access.OrganizationID, SiteAdministrator: access.SiteAdministrator}, nil
}
func (runtime *runtimeIdentity) alertSourceScope(ctx context.Context, scope alerts.AccessScope) (alerts.AccessScope, error) {
	access, err := runtime.freshAccess(ctx, scope.OwnerID, scope.OrganizationID)
	if err != nil {
		return alerts.AccessScope{}, err
	}
	return alerts.AccessScope{OwnerID: access.OwnerID, OrganizationID: access.OrganizationID, SiteAdministrator: access.SiteAdministrator}, nil
}
