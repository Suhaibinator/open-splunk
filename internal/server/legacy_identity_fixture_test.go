package server

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"unicode"

	"github.com/Suhaibinator/open-splunk/internal/auth"
	"github.com/Suhaibinator/open-splunk/internal/authorization"
	"github.com/Suhaibinator/open-splunk/internal/testsupport"
)

type testBearerAuthenticator struct {
	digest    [32]byte
	principal auth.BrowserPrincipal
}

func newTestBearerTokenAuthenticator(token []byte, tenant, owner string, role auth.BrowserRole) (fixtureBrowserAuthenticator, error) {
	if validateFixtureBearerToken(token) != nil {
		return nil, auth.ErrInvalidBrowserAuthenticator
	}
	principal, err := auth.NewBrowserPrincipal(tenant, owner, role)
	if err != nil {
		return nil, err
	}
	return &testBearerAuthenticator{digest: sha256.Sum256(token), principal: principal}, nil
}
func (s *testBearerAuthenticator) Authenticate(ctx context.Context, token []byte) (auth.BrowserPrincipal, error) {
	if err := ctx.Err(); err != nil {
		return auth.BrowserPrincipal{}, err
	}
	digest := sha256.Sum256(token)
	if subtle.ConstantTimeCompare(digest[:], s.digest[:]) != 1 {
		return auth.BrowserPrincipal{}, auth.ErrBrowserUnauthorized
	}
	return s.principal, nil
}

type testBrowserIdentityAdapter struct {
	legacy              fixtureBrowserAuthenticator
	tenant              string
	defaultOrganization bool
}

func adaptTestBrowserAuthenticator(authenticator fixtureBrowserAuthenticator) IdentityAuthenticator {
	if isNilDependency(authenticator) {
		return nil
	}
	return &testBrowserIdentityAdapter{legacy: authenticator}
}
func (s *testBrowserIdentityAdapter) AuthenticateRequest(request *http.Request) (*http.Request, authorization.Subject, error) {
	if s.defaultOrganization && request.Header.Get("X-OpenSplunk-Organization-ID") == "" && request.URL.Query().Get("organization_id") == "" {
		request.Header.Set("X-OpenSplunk-Organization-ID", "1")
	}
	token, ok := browserAuthorizationToken(request)
	if !ok {
		return request, authorization.Subject{}, authorization.ErrUnauthorized
	}
	defer clear(token)
	principal, err := s.legacy.Authenticate(request.Context(), token)
	if errors.Is(err, auth.ErrBrowserUnauthorized) {
		err = authorization.ErrUnauthorized
	}
	if err != nil {
		return request, authorization.Subject{}, err
	}
	if !principal.Valid() {
		return request, authorization.Subject{}, errors.New("invalid fixture principal")
	}
	if s.tenant != "" && principal.TenantID() != s.tenant {
		return request, authorization.Subject{}, authorization.ErrForbidden
	}
	userID, err := strconv.ParseUint(principal.OwnerID(), 10, 64)
	if err != nil {
		return request, authorization.Subject{}, err
	}
	userType := int32(12)
	if principal.IsAdministrator() {
		userType = authorization.SiteAdminUserType
	}
	return request, authorization.Subject{UserID: userID, UserType: userType}, nil
}
func newAuthenticatedTestHandler(config Config) (*Handler, error) {
	if config.IdentityAuthenticator == nil {
		owner := config.OwnerID
		if owner == "" {
			owner = "1030"
		}
		id, err := strconv.ParseUint(owner, 10, 64)
		if err != nil {
			return nil, err
		}
		session, _ := testsupport.NewSessionAuthenticator(id, 44, nil)
		config.IdentityAuthenticator = fixtureOrganizationIdentity{session}
	}
	if adapter, ok := config.IdentityAuthenticator.(*testBrowserIdentityAdapter); ok {
		adapter.defaultOrganization = true
		adapter.tenant = config.TenantID
		if adapter.tenant == "" {
			adapter.tenant = "default"
		}
	}
	if config.Authorization == nil {
		config.Authorization = testsupport.AuthorizationFixture(config.TenantID)
	}
	return NewHandler(config)
}

const administratorAuthorizationScheme = "Bearer "

// Google session authentication has no HTTP bearer challenge.
const administratorAuthenticationRealm = ""

func browserAuthorizationToken(request *http.Request) ([]byte, bool) {
	if request == nil {
		return nil, false
	}
	values, ok := exactAuthorizationHeaderValues(request.Header)
	if !ok || len(values) != 1 || len(values[0]) > len(administratorAuthorizationScheme)+maximumFixtureBearerTokenBytes {
		return nil, false
	}
	token, ok := strictBearerToken(values)
	if !ok {
		return nil, false
	}
	detached := []byte(token)
	if validateFixtureBearerToken(detached) != nil {
		clear(detached)
		return nil, false
	}
	return detached, true
}
func exactAuthorizationHeaderValues(header http.Header) ([]string, bool) {
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

func strictBearerToken(values []string) (string, bool) {
	if len(values) != 1 {
		return "", false
	}
	value := values[0]
	if strings.Count(value, " ") != 1 || strings.Contains(value, ",") {
		return "", false
	}
	scheme, token, found := strings.Cut(value, " ")
	if !found || !strings.EqualFold(scheme, "Bearer") || token == "" ||
		strings.IndexFunc(token, unicode.IsSpace) >= 0 {
		return "", false
	}
	return token, true
}

const (
	minimumFixtureBearerTokenBytes = 32
	maximumFixtureBearerTokenBytes = 512
)

type fixtureBrowserAuthenticator interface {
	Authenticate(context.Context, []byte) (auth.BrowserPrincipal, error)
}

func validateFixtureBearerToken(token []byte) error {
	if len(token) < minimumFixtureBearerTokenBytes || len(token) > maximumFixtureBearerTokenBytes {
		return auth.ErrBrowserUnauthorized
	}
	padding := false
	for i, c := range token {
		if c == '=' {
			if i == 0 {
				return auth.ErrBrowserUnauthorized
			}
			padding = true
			continue
		}
		if padding {
			return auth.ErrBrowserUnauthorized
		}
		if c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || strings.ContainsRune("-._~+/", rune(c)) {
			continue
		}
		return auth.ErrBrowserUnauthorized
	}
	return nil
}

type fixtureOrganizationIdentity struct{ IdentityAuthenticator }

func (s fixtureOrganizationIdentity) AuthenticateRequest(r *http.Request) (*http.Request, authorization.Subject, error) {
	if r.Header.Get("X-OpenSplunk-Organization-ID") == "" && r.URL.Query().Get("organization_id") == "" {
		r.Header.Set("X-OpenSplunk-Organization-ID", "1")
	}
	return s.IdentityAuthenticator.AuthenticateRequest(r)
}
