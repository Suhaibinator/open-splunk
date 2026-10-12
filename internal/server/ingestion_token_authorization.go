package server

import (
	"context"
	"errors"
	"net/http"
	"strconv"

	"github.com/Suhaibinator/open-splunk/internal/auth"
	"github.com/Suhaibinator/open-splunk/internal/authorization"
	"github.com/Suhaibinator/open-splunk/internal/collectorfleet"
	"github.com/Suhaibinator/open-splunk/internal/control"
)

func (handler *apiHandler) authorizeIngestionTokenAdministrator(ctx context.Context) error {
	if handler.authorization == nil {
		return nil
	}
	scope, ok := authorization.FromContext(ctx)
	if !ok {
		return mapAuthorizationError(authorization.ErrUnauthorized)
	}
	if scope.OrganizationID <= 0 {
		return badRequestError("select an organization to administer ingestion credentials")
	}
	return mapAuthorizationError(handler.authorization.AuthorizeOrganization(ctx, scope.Subject, scope.OrganizationID, authorization.AdminRole))
}

// An unenrolled collector ID may receive its first credential. An existing
// collector must already be visible through the caller's organization policy.
// The bounded unrestricted lookup distinguishes those cases without returning
// another organization's collector data.
func (handler *apiHandler) authorizeIngestionTokenCollectorBinding(request *http.Request, collectorID string) error {
	if collectorID == "" || handler.authorization == nil {
		return nil
	}
	subjectScope, ok := authorization.FromContext(request.Context())
	if !ok {
		return mapAuthorizationError(authorization.ErrUnauthorized)
	}
	if subjectScope.Subject.IsSiteAdmin() {
		return nil
	}
	scope, err := handler.collectorAdministrationAccess(request)
	if err != nil {
		return err
	}
	if handler.collectorAdmin == nil {
		return unavailableError("collector binding authorization is unavailable")
	}
	_, err = handler.collectorAdmin.Get(request.Context(), scope, collectorID)
	if err == nil {
		return nil
	}
	if !errors.Is(err, control.ErrNotFound) {
		return mapCollectorAdministrationCallError(request.Context(), err, false)
	}
	_, err = handler.collectorAdmin.Get(request.Context(), collectorfleet.Scope{TenantID: scope.TenantID}, collectorID)
	if errors.Is(err, control.ErrNotFound) {
		return nil
	}
	if err != nil {
		return mapCollectorAdministrationCallError(request.Context(), err, false)
	}
	return forbiddenError("collector binding access is denied")
}

func (handler *apiHandler) authorizeIngestionTokenIndexes(ctx context.Context, names []string) error {
	if err := handler.authorizeIngestionTokenAdministrator(ctx); err != nil {
		return err
	}
	if handler.authorization == nil {
		return nil
	}
	scope, _ := authorization.FromContext(ctx)
	if len(names) == 0 {
		return forbiddenError("token index scope is unavailable")
	}
	for _, name := range names {
		index, err := handler.indexes.GetIndexByName(ctx, name)
		if err != nil {
			return forbiddenError("token index access is denied")
		}
		organizationID, err := handler.authorization.OrganizationForIndex(ctx, index.ID)
		if err != nil {
			return mapAuthorizationError(err)
		}
		// A credential is always restricted to one selected organization, including
		// credentials issued by site administrators.
		if organizationID != scope.OrganizationID {
			return forbiddenError("token indexes must belong to the selected organization")
		}
		for _, capability := range []authorization.Capability{authorization.Grants, authorization.Ingest} {
			if err := handler.authorizeIndexCapability(ctx, index.ID, capability); err != nil {
				return mapAuthorizationError(err)
			}
		}
	}
	return nil
}

func (handler *apiHandler) authorizeIngestionTokenRecord(ctx context.Context, record auth.CollectorToken) error {
	if err := handler.authorizeIngestionTokenAdministrator(ctx); err != nil {
		return err
	}
	if handler.authorization != nil {
		scope, _ := authorization.FromContext(ctx)
		if record.OrganizationID <= 0 || record.OrganizationID != scope.OrganizationID {
			return forbiddenError("ingestion credential access is denied")
		}
	}
	return nil
}

func ingestionTokenScopeFingerprint(ctx context.Context) string {
	if scope, ok := authorization.FromContext(ctx); ok {
		return scope.PhysicalTenant + ":" + scope.Subject.OwnerID() + ":" + strconv.FormatInt(scope.OrganizationID, 10) + ":" + strconv.FormatInt(int64(scope.Subject.UserType), 10)
	}
	return ""
}
