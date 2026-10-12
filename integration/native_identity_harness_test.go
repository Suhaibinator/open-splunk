//go:build !windows

package integration_test

import (
	"net/http"
	"net/http/cookiejar"
	"net/url"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/testsupport/identityfixture"
)

var nativeIdentities = struct {
	sync.Mutex
	byDSN     map[string]*identityfixture.Fixture
	bySession map[string]*identityfixture.Fixture
	byOrigin  map[string]*identityfixture.Fixture
}{byDSN: map[string]*identityfixture.Fixture{}, bySession: map[string]*identityfixture.Fixture{}, byOrigin: map[string]*identityfixture.Fixture{}}

func provisionGoogleSession(t *testing.T, directory string, dsn ...string) (*identityfixture.Fixture, string) {
	t.Helper()
	options := identityfixture.Options{Directory: directory}
	if len(dsn) != 0 {
		options.PostgresDSN = dsn[0]
	}
	fixture := identityfixture.New(t, options)
	nativeIdentities.Lock()
	nativeIdentities.byDSN[fixture.PostgresDSN], nativeIdentities.bySession[fixture.Session] = fixture, fixture
	nativeIdentities.Unlock()
	t.Cleanup(func() {
		nativeIdentities.Lock()
		defer nativeIdentities.Unlock()
		delete(nativeIdentities.byDSN, fixture.PostgresDSN)
		delete(nativeIdentities.bySession, fixture.Session)
		for origin, candidate := range nativeIdentities.byOrigin {
			if candidate == fixture {
				delete(nativeIdentities.byOrigin, origin)
			}
		}
	})
	return fixture, fixture.Session
}

func fixtureForURL(endpoint string, session string) *identityfixture.Fixture {
	nativeIdentities.Lock()
	defer nativeIdentities.Unlock()
	if fixture := nativeIdentities.bySession[session]; fixture != nil {
		return fixture
	}
	parsed, err := url.Parse(endpoint)
	if err != nil {
		return nil
	}
	if parsed.Scheme == "ws" {
		parsed.Scheme = "http"
	}
	if parsed.Scheme == "wss" {
		parsed.Scheme = "https"
	}
	return nativeIdentities.byOrigin[parsed.Scheme+"://"+parsed.Host]
}

func applyGoogleSession(request *http.Request, session string) {
	fixture := fixtureForURL(request.URL.String(), session)
	if fixture == nil {
		return
	}
	if _, err := request.Cookie(identityfixture.CookieName); err != nil {
		request.AddCookie(&http.Cookie{Name: identityfixture.CookieName, Value: fixture.Session, Secure: true, HttpOnly: true, SameSite: http.SameSiteLaxMode})
	}
	request.Header.Set("X-OpenSplunk-Organization-ID", fixture.OrganizationID)
	if request.Header.Get("Origin") == "" {
		request.Header.Set("Origin", request.URL.Scheme+"://"+request.URL.Host)
	}
}

type nativeWorkspaceTransport struct{ base http.RoundTripper }

func (transport nativeWorkspaceTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	if strings.HasPrefix(request.URL.Path, "/api/") {
		request = request.Clone(request.Context())
		applyGoogleSession(request, "")
	}
	return transport.base.RoundTrip(request)
}

func configureNativeWorkspaceClient(t *testing.T, client *http.Client, origin string) {
	t.Helper()
	fixture := fixtureForURL(origin, "")
	if fixture == nil {
		return
	}
	if _, configured := client.Transport.(nativeWorkspaceTransport); !configured {
		base := client.Transport
		if base == nil {
			base = http.DefaultTransport
		}
		client.Transport = nativeWorkspaceTransport{base: base}
	}
	if client.Jar == nil {
		jar, err := cookiejar.New(nil)
		if err != nil {
			t.Fatal(err)
		}
		client.Jar = jar
	}
	parsed, err := url.Parse(origin)
	if err != nil {
		t.Fatal(err)
	}
	cookie := &http.Cookie{Name: identityfixture.CookieName, Value: fixture.Session, Path: "/", HttpOnly: true, Secure: true, SameSite: http.SameSiteLaxMode}
	if parsed.Scheme == "https" {
		client.Jar.SetCookies(parsed, []*http.Cookie{cookie})
	}
	// Explicit local HTTP fixtures authenticate through the scoped test
	// transport above; the cookie jar never exposes their session over HTTP.
}

// Native process fixtures use the same credential and storage inputs as the
// shipped server. Seeding is finished and its ownership lease released before
// launching that process; there is no administrator-token argument or route.
func nativeProcessEnvironment(t *testing.T, arguments, environment []string) ([]string, []string) {
	t.Helper()
	value := func(name string) string {
		for _, argument := range arguments[1:] {
			if suffix, ok := strings.CutPrefix(argument, "-"+name+"="); ok {
				return suffix
			}
		}
		return ""
	}
	dsn := value("postgres-dsn")
	if dsn == "" {
		return arguments, environment
	}
	nativeIdentities.Lock()
	fixture := nativeIdentities.byDSN[dsn]
	nativeIdentities.Unlock()
	if fixture == nil {
		return arguments, environment
	}
	if err := fixture.Close(); err != nil {
		t.Fatal("release native Google seed ownership before server startup:", err)
	}
	scheme := "http"
	if value("http-tls-certificate-file") != "" {
		scheme = "https"
	}
	origin := scheme + "://" + value("http-listen-address")
	nativeIdentities.Lock()
	nativeIdentities.byOrigin[origin] = fixture
	nativeIdentities.Unlock()
	environment = append([]string(nil), environment...)
	for name, setting := range map[string]string{
		"OPEN_SPLUNK_SERVER_PUBLIC_BASE_URL":      origin,
		"OPEN_SPLUNK_SERVER_GOOGLE_CLIENT_ID":     "integration-google-client",
		"OPEN_SPLUNK_SERVER_GOOGLE_CLIENT_SECRET": "integration-google-secret",
		"OPEN_SPLUNK_SERVER_MINIO_ENDPOINT":       fixture.MinioEndpoint,
		"OPEN_SPLUNK_SERVER_MINIO_ACCESS_KEY":     fixture.MinioAccessKey,
		"OPEN_SPLUNK_SERVER_MINIO_SECRET_KEY":     fixture.MinioSecretKey,
		"OPEN_SPLUNK_SERVER_MINIO_BUCKET":         fixture.MinioBucket,
		"OPEN_SPLUNK_SERVER_MINIO_USE_TLS":        "false",
	} {
		environment = environmentWithValue(environment, name, setting)
	}
	arguments = append([]string(nil), arguments...)
	for name, path := range map[string]string{"server-lock-file": "server.lock", "search-artifact-directory": "search-artifacts", "export-artifact-directory": "exports"} {
		if value(name) == "" {
			arguments = append(arguments, "-"+name+"="+filepath.Join(filepath.Dir(fixture.MasterKeyPath), path))
		}
	}
	return arguments, environment
}

func nativeBrowserEnvironment(environment []string, baseURL string) []string {
	fixture := fixtureForURL(baseURL, "")
	if fixture == nil {
		return environment
	}
	environment = environmentWithValue(environment, "OPEN_SPLUNK_BROWSER_SESSION_COOKIE", fixture.Session)
	return environmentWithValue(environment, "OPEN_SPLUNK_BROWSER_ORGANIZATION_ID", fixture.OrganizationID)
}

func registerNativeBrowserFixture(t *testing.T, fixture *identityfixture.Fixture, origin string) {
	t.Helper()
	nativeIdentities.Lock()
	nativeIdentities.byOrigin[origin], nativeIdentities.bySession[fixture.Session] = fixture, fixture
	nativeIdentities.Unlock()
	t.Cleanup(func() {
		nativeIdentities.Lock()
		defer nativeIdentities.Unlock()
		delete(nativeIdentities.byOrigin, origin)
		delete(nativeIdentities.bySession, fixture.Session)
	})
}

var _ http.RoundTripper = nativeWorkspaceTransport{}

func underlyingNativeTransport(transport http.RoundTripper) http.RoundTripper {
	if wrapped, ok := transport.(nativeWorkspaceTransport); ok {
		return wrapped.base
	}
	if transport == nil {
		return http.DefaultTransport
	}
	return transport
}
