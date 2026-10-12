package server

import (
	"context"
	"errors"
	"net/http"
	"slices"
	"strconv"
	"strings"

	"github.com/Suhaibinator/SRouter/pkg/router"
	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/audit"
	"github.com/Suhaibinator/open-splunk/internal/auth"
	"github.com/Suhaibinator/open-splunk/internal/authorization"
	"github.com/Suhaibinator/open-splunk/internal/resourcepolicy"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
)

type IdentityAuthenticator interface {
	AuthenticateRequest(*http.Request) (*http.Request, authorization.Subject, error)
}

func (handler *apiHandler) authenticateIdentity(response http.ResponseWriter, request *http.Request) (*http.Request, bool) {
	authenticated, subject, err := handler.identityAuthenticator.AuthenticateRequest(request)
	if err != nil || authenticated == nil || !subject.Valid() {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			writeAPIError(response, http.StatusRequestTimeout, "authentication timed out")
		} else if errors.Is(err, authorization.ErrUnauthorized) {
			writeAPIError(response, http.StatusUnauthorized, "sign in is required")
		} else if errors.Is(err, authorization.ErrForbidden) {
			writeAPIError(response, http.StatusForbidden, "resource access is denied")
		} else {
			writeAPIError(response, http.StatusServiceUnavailable, "user authentication is unavailable")
		}
		return request, false
	}
	organizationID := int64(0)
	values := request.Header.Values("X-OpenSplunk-Organization-ID")
	if len(values) == 0 && (request.URL.Path == searchWebSocketPath || request.URL.Path == exportDownloadPath) {
		values = request.URL.Query()["organization_id"]
	}
	if len(values) > 0 {
		if len(values) != 1 {
			writeAPIError(response, http.StatusBadRequest, "organization selection is invalid")
			return request, false
		}
		organizationID, err = strconv.ParseInt(values[0], 10, 64)
		if err != nil || organizationID <= 0 || strconv.FormatInt(organizationID, 10) != values[0] {
			writeAPIError(response, http.StatusBadRequest, "organization selection is invalid")
			return request, false
		}
	}
	role := auth.BrowserRoleUser
	if subject.IsSiteAdmin() {
		role = auth.BrowserRoleAdministrator
	} else if organizationID > 0 {
		orgRole, lookupErr := handler.authorization.OrganizationRole(authenticated.Context(), subject, organizationID)
		if lookupErr != nil {
			writeAPIError(response, http.StatusForbidden, "organization access is required")
			return request, false
		}
		if orgRole >= authorization.AdminRole {
			role = auth.BrowserRoleAdministrator
		}
	}
	principal, err := auth.NewBrowserPrincipal(handler.tenantID, subject.OwnerID(), role)
	if err != nil {
		writeAPIError(response, http.StatusServiceUnavailable, "authentication is unavailable")
		return request, false
	}
	ctx, err := audit.WithActor(authenticated.Context(), audit.Actor{Kind: audit.ActorKindBrowser, ID: subject.OwnerID(), Role: audit.ActorRole(role.String())})
	if err != nil {
		writeAPIError(response, http.StatusServiceUnavailable, "authentication is unavailable")
		return request, false
	}
	ctx = authorization.WithScope(ctx, authorization.Scope{Subject: subject, OrganizationID: organizationID, PhysicalTenant: handler.tenantID})
	ctx = context.WithValue(ctx, browserPrincipalContextKey{}, principal)
	ctx = resourcepolicy.WithProvider(ctx, handler.metadataReadSnapshot)
	authenticated = authenticated.Clone(ctx)
	for name := range authenticated.Header {
		if strings.EqualFold(name, "Authorization") {
			delete(authenticated.Header, name)
		}
	}
	return authenticated, true
}

func (handler *apiHandler) requestOwner(contexts ...context.Context) string {
	if len(contexts) > 0 {
		if scope, ok := authorization.FromContext(contexts[0]); ok {
			return scope.Subject.OwnerID()
		}
	}
	if handler.identityAuthenticator != nil {
		return ""
	}
	return handler.ownerID
}
func (handler *apiHandler) requireSiteAdministrator(request *http.Request) bool {
	scope, ok := authorization.FromContext(request.Context())
	return ok && scope.Subject.IsSiteAdmin()
}

func (handler *apiHandler) requestOrganization(ctx context.Context) string {
	if scope, ok := authorization.FromContext(ctx); ok && scope.OrganizationID > 0 {
		return strconv.FormatInt(scope.OrganizationID, 10)
	}
	return ""
}

func mapAuthorizationError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, authorization.ErrUnauthorized) {
		return router.NewHTTPError(http.StatusUnauthorized, "sign in is required")
	}
	if errors.Is(err, authorization.ErrForbidden) || errors.Is(err, authorization.ErrUnassigned) {
		return forbiddenError("resource access is denied")
	}
	if errors.Is(err, authorization.ErrConflict) {
		return router.NewHTTPError(http.StatusConflict, "permission policy changed; reload and retry")
	}
	if errors.Is(err, authorization.ErrInvalidPolicy) {
		return badRequestError("permission policy is invalid")
	}
	return unavailableError("authorization is unavailable")
}

type capabilityContextKey struct{}

const metadataCapability authorization.Capability = "metadata"

func indexRouteCapability(path string) authorization.Capability {
	if path == "/api/indexes/list" || path == "/api/indexes/get" {
		return metadataCapability
	}
	for _, suffix := range []string{"/update", "/state/set", "/delete", "/data/delete"} {
		if strings.HasSuffix(path, suffix) {
			return authorization.Manage
		}
	}
	return authorization.Search
}
func siteAdministratorRoute(path string) bool {
	for _, prefix := range []string{"/api/server/", "/api/audit/", "/api/hec/", "/api/search/attempts/"} {
		if strings.HasPrefix(path, prefix) {
			return true
		}
	}
	return false
}

func (handler *apiHandler) authorizeIndexCapability(ctx context.Context, indexID string, capability authorization.Capability) error {
	if handler.authorization == nil {
		if handler.identityAuthenticator != nil {
			return authorization.ErrForbidden
		}
		return nil
	}
	scope, ok := authorization.FromContext(ctx)
	if !ok {
		return authorization.ErrUnauthorized
	}
	if capability == metadataCapability {
		return handler.authorization.AuthorizeIndexMetadata(ctx, scope.Subject, scope.OrganizationID, indexID)
	}
	return handler.authorization.AuthorizeIndex(ctx, scope.Subject, scope.OrganizationID, indexID, capability)
}

func (handler *apiHandler) authorizeJobSources(ctx context.Context, job searchjobs.Job) error {
	if handler.authorization == nil {
		return nil
	}
	scope, ok := authorization.FromContext(ctx)
	if !ok {
		return mapAuthorizationError(authorization.ErrUnauthorized)
	}
	if scope.Subject.IsSiteAdmin() {
		return nil
	}
	if job.TenantID != handler.tenantID || job.OrganizationID == "" || job.OrganizationID != handler.requestOrganization(ctx) {
		return forbiddenError("search organization access is denied")
	}
	if err := handler.authorizeSearchApp(ctx, job.AppID); err != nil {
		return err
	}
	indexes := job.EffectiveIndexes
	if len(indexes) == 0 {
		indexes = job.RequestedIndexes
	}
	if len(indexes) == 0 {
		return forbiddenError("search index access is denied")
	}
	for _, name := range indexes {
		record, err := handler.indexes.GetIndexByName(ctx, name)
		if err != nil {
			return forbiddenError("search index access is denied")
		}
		if err := handler.authorizeIndexCapability(ctx, record.ID, authorization.Search); err != nil {
			return mapAuthorizationError(err)
		}
	}
	return nil
}

func (handler *apiHandler) authorizeStoredSearchDefinition(ctx context.Context, definition *opensplunk.SearchDefinition) error {
	if handler.authorization == nil {
		return nil
	}
	if definition == nil {
		return forbiddenError("search source access is denied")
	}
	if scope, ok := authorization.FromContext(ctx); ok && scope.Subject.IsSiteAdmin() {
		return nil
	}
	snapshot, present, err := resourcepolicy.FromContext(ctx)
	if err != nil {
		return mapAuthorizationError(err)
	}
	if present {
		sources, sourceErr := resourcepolicy.SourceNames(definition)
		if sourceErr != nil || !resourcepolicy.AllowsSource(snapshot, sources, definition.GetAppId()) {
			return forbiddenError("search source access is denied")
		}
	}
	if err := handler.authorizeSearchApp(ctx, definition.GetAppId()); err != nil {
		return err
	}
	if _, err := handler.resolveAuthorizedSearchIndexes(ctx, definition.GetIndexScope()); err != nil {
		return err
	}
	return nil
}

func (handler *apiHandler) authorizedIndexList(ctx context.Context) ([]string, string, error) {
	if handler.authorization == nil {
		return nil, "", nil
	}
	scope, ok := authorization.FromContext(ctx)
	if !ok {
		return nil, "", authorization.ErrUnauthorized
	}
	fingerprint := scope.Subject.OwnerID() + ":" + strconv.FormatInt(scope.OrganizationID, 10) + ":" + strconv.FormatInt(int64(scope.Subject.UserType), 10)
	if scope.Subject.IsSiteAdmin() {
		return nil, fingerprint, nil
	}
	indexes, err := handler.indexes.ListIndexes(ctx)
	if err != nil {
		return nil, "", err
	}
	ids := make([]string, 0, len(indexes))
	for _, index := range indexes {
		err := handler.authorizeIndexCapability(ctx, index.ID, metadataCapability)
		if err == nil {
			ids = append(ids, index.ID)
		} else if !errors.Is(err, authorization.ErrForbidden) && !errors.Is(err, authorization.ErrUnassigned) {
			return nil, "", err
		}
	}
	slices.Sort(ids)
	return ids, fingerprint + ":" + strings.Join(ids, ","), nil
}
