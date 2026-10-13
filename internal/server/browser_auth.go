package server

import (
	"net/http"

	"github.com/Suhaibinator/open-splunk/internal/auth"
	"github.com/Suhaibinator/open-splunk/internal/authorization"
)

type browserPrincipalContextKey struct{}

func (handler *apiHandler) administratorPrincipal(request *http.Request) (auth.BrowserPrincipal, error) {
	if handler == nil || request == nil {
		return auth.BrowserPrincipal{}, forbiddenError("administrator access is required")
	}
	principal, ok := browserPrincipalFromRequest(request)
	if !ok || !principal.IsAdministrator() {
		return auth.BrowserPrincipal{}, forbiddenError("administrator access is required")
	}
	scope, valid := authorization.FromContext(request.Context())
	if !valid || !scope.Subject.Valid() || scope.Subject.OwnerID() != principal.OwnerID() || scope.PhysicalTenant != handler.tenantID || principal.TenantID() != scope.PhysicalTenant || (!scope.Subject.IsSiteAdmin() && scope.OrganizationID <= 0) {
		return auth.BrowserPrincipal{}, forbiddenError("authenticated administrator scope is required")
	}
	return principal, nil
}
func (handler *apiHandler) authenticateBrowser(response http.ResponseWriter, request *http.Request) (*http.Request, auth.BrowserPrincipal, bool) {
	if handler == nil || isNilDependency(handler.identityAuthenticator) {
		writeAPIError(response, http.StatusServiceUnavailable, "user authentication is unavailable")
		return request, auth.BrowserPrincipal{}, false
	}
	if principal, ok := browserPrincipalFromRequest(request); ok {
		return request, principal, true
	}
	authenticated, ok := handler.authenticateIdentity(response, request)
	if !ok {
		return request, auth.BrowserPrincipal{}, false
	}
	principal, valid := browserPrincipalFromRequest(authenticated)
	return authenticated, principal, valid
}
func browserPrincipalFromRequest(request *http.Request) (auth.BrowserPrincipal, bool) {
	if request == nil {
		return auth.BrowserPrincipal{}, false
	}
	principal, ok := request.Context().Value(browserPrincipalContextKey{}).(auth.BrowserPrincipal)
	return principal, ok && principal.Valid()
}
