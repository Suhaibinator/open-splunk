package server

import (
	"context"
	"errors"
	"net/http"
	"strings"

	"github.com/Suhaibinator/open-splunk/internal/audit"
	"github.com/Suhaibinator/open-splunk/internal/auth"
)

const (
	administratorAuthorizationScheme = "Bearer "
	administratorAuthenticationRealm = `Bearer realm="open-splunk-admin"`
)

type browserPrincipalContextKey struct{}

func (handler *apiHandler) authorizeBrowserAdministrator(
	response http.ResponseWriter,
	request *http.Request,
) (*http.Request, bool) {
	authenticatedRequest, principal, authenticated := handler.authenticateBrowser(
		response,
		request,
	)
	if !authenticated {
		return request, false
	}
	if !principal.IsAdministrator() ||
		principal.TenantID() != handler.tenantID ||
		principal.OwnerID() != handler.ownerID {
		writeAPIError(
			response,
			http.StatusForbidden,
			"administrator access is required",
		)
		return request, false
	}
	return authenticatedRequest, true
}

func (handler *apiHandler) authenticateBrowser(
	response http.ResponseWriter,
	request *http.Request,
) (*http.Request, auth.BrowserPrincipal, bool) {
	token, ok := browserAuthorizationToken(request)
	if !ok {
		writeAdministratorUnauthorized(response)
		return request, auth.BrowserPrincipal{}, false
	}
	defer clear(token)

	if handler == nil || isNilDependency(handler.browserAuthenticator) {
		writeAPIError(
			response,
			http.StatusServiceUnavailable,
			"administrator authentication is unavailable",
		)
		return request, auth.BrowserPrincipal{}, false
	}
	principal, err := handler.browserAuthenticator.Authenticate(
		request.Context(),
		token,
	)
	if err != nil {
		switch {
		case errors.Is(err, context.Canceled),
			errors.Is(err, context.DeadlineExceeded):
			writeAPIError(
				response,
				http.StatusRequestTimeout,
				"administrator authentication timed out",
			)
		case errors.Is(err, auth.ErrBrowserUnauthorized):
			writeAdministratorUnauthorized(response)
		default:
			writeAPIError(
				response,
				http.StatusServiceUnavailable,
				"administrator authentication is unavailable",
			)
		}
		return request, auth.BrowserPrincipal{}, false
	}
	if !principal.Valid() {
		writeAPIError(
			response,
			http.StatusServiceUnavailable,
			"administrator authentication is unavailable",
		)
		return request, auth.BrowserPrincipal{}, false
	}

	auditContext, err := audit.WithActor(request.Context(), audit.Actor{
		Kind: audit.ActorKindBrowser,
		ID:   principal.OwnerID(),
		Role: audit.ActorRole(principal.Role().String()),
	})
	if err != nil {
		writeAPIError(
			response,
			http.StatusServiceUnavailable,
			"administrator authentication is unavailable",
		)
		return request, auth.BrowserPrincipal{}, false
	}
	ctx := context.WithValue(
		auditContext,
		browserPrincipalContextKey{},
		principal,
	)
	authenticatedRequest := request.Clone(ctx)
	// Route middleware and handlers consume the detached principal, never the
	// reusable credential.
	for name := range authenticatedRequest.Header {
		if strings.EqualFold(name, "Authorization") {
			delete(authenticatedRequest.Header, name)
		}
	}
	return authenticatedRequest, principal, true
}

func browserAuthorizationToken(request *http.Request) ([]byte, bool) {
	if request == nil {
		return nil, false
	}
	values, ok := exactAuthorizationHeaderValues(request.Header)
	if !ok || len(values) != 1 ||
		len(values[0]) >
			len(administratorAuthorizationScheme)+
				auth.MaximumBrowserBearerTokenBytes {
		return nil, false
	}
	token, ok := strictBearerToken(values)
	if !ok {
		return nil, false
	}
	detached := []byte(token)
	if err := auth.ValidateBrowserBearerToken(detached); err != nil {
		clear(detached)
		return nil, false
	}
	return detached, true
}

func exactAuthorizationHeaderValues(
	header http.Header,
) ([]string, bool) {
	var result []string
	for name, values := range header {
		if !strings.EqualFold(name, "Authorization") {
			continue
		}
		if len(values) != 1 || result != nil {
			return nil, false
		}
		result = values
	}
	return result, true
}

func writeAdministratorUnauthorized(response http.ResponseWriter) {
	response.Header().Set(
		"WWW-Authenticate",
		administratorAuthenticationRealm,
	)
	writeAPIError(
		response,
		http.StatusUnauthorized,
		"administrator authentication is required",
	)
}

func browserPrincipalFromRequest(
	request *http.Request,
) (auth.BrowserPrincipal, bool) {
	if request == nil {
		return auth.BrowserPrincipal{}, false
	}
	principal, ok := request.Context().Value(
		browserPrincipalContextKey{},
	).(auth.BrowserPrincipal)
	return principal, ok && principal.Valid()
}
