package server

import (
	"context"
	"errors"
	"strconv"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/authorization"
	"github.com/Suhaibinator/open-splunk/internal/searchartifacts"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
)

// RetentionUsageReader reports the shared capture/export quota after checking
// the current user's selected organization membership. Private in-flight
// reservations are included, so usage is accurate during streaming writes.
type RetentionUsageReader func(context.Context, searchjobs.AccessScope) (searchartifacts.RetentionUsage, error)

func (handler *apiHandler) populateRetentionUsage(ctx context.Context, response *opensplunk.GetSystemBootstrapResponse) error {
	if handler.retentionUsage == nil {
		return nil
	}
	scope, ok := authorization.FromContext(ctx)
	if !ok || scope.OrganizationID <= 0 {
		return nil
	}
	access := searchjobs.AccessScope{TenantID: scope.PhysicalTenant, OwnerID: scope.Subject.OwnerID(), OrganizationID: strconv.FormatInt(scope.OrganizationID, 10), SiteAdministrator: scope.Subject.IsSiteAdmin()}
	usage, err := handler.retentionUsage(ctx, access)
	if err != nil {
		return err
	}
	if usage.OrganizationLimit == 0 || usage.GlobalLimit < usage.OrganizationLimit || usage.GlobalBytes < usage.OrganizationBytes {
		return errors.New("retained storage usage contract is invalid")
	}
	response.RetainedStorageUsage = &opensplunk.RetainedStorageUsage{OrganizationId: access.OrganizationID, OrganizationUsedBytes: usage.OrganizationBytes, GlobalUsedBytes: usage.GlobalBytes, OrganizationLimitBytes: usage.OrganizationLimit, GlobalLimitBytes: usage.GlobalLimit}
	return nil
}
