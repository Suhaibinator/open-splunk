package main

import (
	"net/http"
	"strconv"

	"github.com/Suhaibinator/open-splunk/internal/auth"
	"github.com/Suhaibinator/open-splunk/internal/authorization"
	"github.com/Suhaibinator/open-splunk/internal/server"
	"github.com/Suhaibinator/open-splunk/internal/testsupport"
)

type runtimeSessionFixture struct{ server.IdentityAuthenticator }

func (f runtimeSessionFixture) AuthenticateRequest(request *http.Request) (*http.Request, authorization.Subject, error) {
	if request.Header.Get("X-OpenSplunk-Organization-ID") == "" {
		request.Header.Set("X-OpenSplunk-Organization-ID", "1")
	}
	return f.IdentityAuthenticator.AuthenticateRequest(request)
}
func newRuntimeSessionAuthenticator(credential []byte, _ string, owner string, role auth.BrowserRole) (server.IdentityAuthenticator, error) {
	userID, err := strconv.ParseUint(owner, 10, 64)
	if err != nil || userID == 0 {
		userID = 1
	}
	userType := int32(12)
	if role == auth.BrowserRoleAdministrator {
		userType = authorization.SiteAdminUserType
	}
	authenticator, err := testsupport.NewSessionAuthenticator(userID, userType, credential)
	if err != nil {
		return nil, err
	}
	return runtimeSessionFixture{authenticator}, nil
}
func newRuntimeTestHandler(config server.Config) (*server.Handler, error) {
	if config.IdentityAuthenticator == nil {
		config.IdentityAuthenticator, _ = newRuntimeSessionAuthenticator(nil, config.TenantID, config.OwnerID, auth.BrowserRoleAdministrator)
	}
	if config.Authorization == nil {
		config.Authorization = testsupport.AuthorizationFixture(config.TenantID)
	}
	return server.NewHandler(config)
}

const runtimeSessionCredentialBytes = 32
