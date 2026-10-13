//go:build !windows

package integration_test

import (
	"net/http"
	"net/url"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/testsupport/identityfixture"
)

type nativeSessionProbe struct{ requests []*http.Request }

func (probe *nativeSessionProbe) RoundTrip(request *http.Request) (*http.Response, error) {
	probe.requests = append(probe.requests, request)
	return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: http.NoBody, Request: request}, nil
}

func TestNativeSessionTransportKeepsCookieJarSecure(t *testing.T) {
	for _, scheme := range []string{"http", "https"} {
		t.Run(scheme, func(t *testing.T) {
			origin := scheme + "://127.0.0.1:41237"
			fixture := &identityfixture.Fixture{Session: "native-session-fixture", OrganizationID: "1"}
			registerNativeBrowserFixture(t, fixture, origin)
			probe := &nativeSessionProbe{}
			client := &http.Client{Transport: probe}
			configureNativeWorkspaceClient(t, client, origin)
			parsed, err := url.Parse(origin)
			if err != nil {
				t.Fatal(err)
			}
			cookies := client.Jar.Cookies(parsed)
			if (scheme == "http" && len(cookies) != 0) || (scheme == "https" && (len(cookies) != 1 || cookies[0].Value != fixture.Session)) {
				t.Fatal("session cookie jar did not enforce HTTPS")
			}
			request, err := http.NewRequestWithContext(t.Context(), http.MethodGet, origin+"/api/identity/session", nil)
			if err != nil {
				t.Fatal(err)
			}
			response, err := client.Do(request)
			if err != nil {
				t.Fatal(err)
			}
			if err := response.Body.Close(); err != nil {
				t.Fatal(err)
			}
			cookie, err := probe.requests[0].Cookie(identityfixture.CookieName)
			if err != nil || cookie.Value != fixture.Session || probe.requests[0].Header.Get("X-OpenSplunk-Organization-ID") != "1" {
				t.Fatal("scoped native API transport lost session or organization authority")
			}
		})
	}
}
