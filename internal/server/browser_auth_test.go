package server

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/auth"
	"github.com/Suhaibinator/open-splunk/internal/collectorfleet"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"google.golang.org/protobuf/proto"
)

const (
	browserGateTenantID = "tenant-exact"
	browserGateOwnerID  = "owner-exact"
)

var browserGateAdministratorPaths = []string{
	"/api/v1/indexes/create",
	"/api/v1/indexes/get",
	"/api/v1/indexes/list",
	"/api/v1/indexes/update",
	"/api/v1/indexes/state/set",
	"/api/v1/indexes/delete",
	"/api/v1/ingestion-tokens/create",
	"/api/v1/ingestion-tokens/get",
	"/api/v1/ingestion-tokens/list",
	"/api/v1/ingestion-tokens/update",
	"/api/v1/ingestion-tokens/revoke",
	"/api/v1/collectors/get",
	"/api/v1/collectors/list",
	"/api/v1/collectors/update",
	"/api/v1/collectors/state/set",
	"/api/v1/apps/create",
	"/api/v1/apps/get",
	"/api/v1/apps/list",
	"/api/v1/apps/update",
	"/api/v1/apps/state/set",
	"/api/v1/apps/delete",
	searchInspectionPath,
}

type recordingBrowserAuthenticator struct {
	mu         sync.Mutex
	calls      int
	tokenAlias []byte
	fn         func(context.Context, []byte) (auth.BrowserPrincipal, error)
}

func (authenticator *recordingBrowserAuthenticator) Authenticate(
	ctx context.Context,
	token []byte,
) (auth.BrowserPrincipal, error) {
	authenticator.mu.Lock()
	authenticator.calls++
	authenticator.tokenAlias = token
	fn := authenticator.fn
	authenticator.mu.Unlock()
	if fn == nil {
		return auth.BrowserPrincipal{}, errors.New("unexpected authentication")
	}
	return fn(ctx, token)
}

func (authenticator *recordingBrowserAuthenticator) callCount() int {
	authenticator.mu.Lock()
	defer authenticator.mu.Unlock()
	return authenticator.calls
}

func (authenticator *recordingBrowserAuthenticator) aliasedTokenWasCleared() bool {
	authenticator.mu.Lock()
	defer authenticator.mu.Unlock()
	if len(authenticator.tokenAlias) == 0 {
		return false
	}
	for _, character := range authenticator.tokenAlias {
		if character != 0 {
			return false
		}
	}
	return true
}

type browserGateIndexAdministration struct {
	mu          sync.Mutex
	calls       int
	listContext context.Context
}

func (administration *browserGateIndexAdministration) record(
	ctx context.Context,
) {
	administration.mu.Lock()
	administration.calls++
	administration.listContext = ctx
	administration.mu.Unlock()
}

func (administration *browserGateIndexAdministration) callCount() int {
	administration.mu.Lock()
	defer administration.mu.Unlock()
	return administration.calls
}

func (administration *browserGateIndexAdministration) capturedContext() context.Context {
	administration.mu.Lock()
	defer administration.mu.Unlock()
	return administration.listContext
}

func (administration *browserGateIndexAdministration) CreateIndex(
	ctx context.Context,
	_ control.IndexDefinition,
) (control.Index, error) {
	administration.record(ctx)
	return control.Index{}, errors.New("unexpected index creation")
}

func (administration *browserGateIndexAdministration) GetIndex(
	ctx context.Context,
	_ string,
) (control.Index, error) {
	administration.record(ctx)
	return control.Index{}, control.ErrNotFound
}

func (administration *browserGateIndexAdministration) GetIndexByName(
	ctx context.Context,
	_ string,
) (control.Index, error) {
	administration.record(ctx)
	return control.Index{}, control.ErrNotFound
}

func (administration *browserGateIndexAdministration) ListIndexes(
	ctx context.Context,
) ([]control.Index, error) {
	administration.record(ctx)
	return nil, nil
}

func (administration *browserGateIndexAdministration) UpdateIndex(
	ctx context.Context,
	_ string,
	_ uint64,
	_ control.IndexDefinition,
) (control.Index, error) {
	administration.record(ctx)
	return control.Index{}, errors.New("unexpected index update")
}

func (administration *browserGateIndexAdministration) SetIndexState(
	ctx context.Context,
	_ string,
	_ uint64,
	_ control.IndexState,
) (control.Index, error) {
	administration.record(ctx)
	return control.Index{}, errors.New("unexpected index state update")
}

func (administration *browserGateIndexAdministration) DeleteIndex(
	ctx context.Context,
	_ string,
	_ uint64,
	_ string,
) (string, error) {
	administration.record(ctx)
	return "", errors.New("unexpected index deletion")
}

type browserGateTokenAdministration struct {
	mu    sync.Mutex
	calls int
}

type browserGateCollectorAdministration struct{}

func (browserGateCollectorAdministration) Get(
	context.Context,
	collectorfleet.Scope,
	string,
) (collectorfleet.CatalogEntry, error) {
	return collectorfleet.CatalogEntry{}, errors.New(
		"collector administration must not run before browser authorization",
	)
}

func (browserGateCollectorAdministration) List(
	context.Context,
	collectorfleet.Scope,
	collectorfleet.ListRequest,
) (collectorfleet.ListResult, error) {
	return collectorfleet.ListResult{}, errors.New(
		"collector administration must not run before browser authorization",
	)
}

func (browserGateCollectorAdministration) UpdateDisplayName(
	context.Context,
	collectorfleet.Scope,
	string,
	uint64,
	*string,
	time.Time,
) (collectorfleet.AdministrationSnapshot, error) {
	return collectorfleet.AdministrationSnapshot{}, errors.New(
		"collector administration must not run before browser authorization",
	)
}

func (browserGateCollectorAdministration) SetAdministrativeState(
	context.Context,
	collectorfleet.Scope,
	string,
	uint64,
	collectorfleet.AdministrativeState,
	time.Time,
) (collectorfleet.AdministrationSnapshot, error) {
	return collectorfleet.AdministrationSnapshot{}, errors.New(
		"collector administration must not run before browser authorization",
	)
}

func (administration *browserGateTokenAdministration) record() {
	administration.mu.Lock()
	administration.calls++
	administration.mu.Unlock()
}

func (administration *browserGateTokenAdministration) callCount() int {
	administration.mu.Lock()
	defer administration.mu.Unlock()
	return administration.calls
}

func (administration *browserGateTokenAdministration) CreateCollectorToken(
	context.Context,
	auth.CreateCollectorTokenRequest,
) (auth.IssuedCollectorToken, error) {
	administration.record()
	return auth.IssuedCollectorToken{}, errors.New("unexpected token creation")
}

func (administration *browserGateTokenAdministration) GetCollectorToken(
	context.Context,
	string,
) (auth.CollectorToken, error) {
	administration.record()
	return auth.CollectorToken{}, control.ErrNotFound
}

func (administration *browserGateTokenAdministration) ListCollectorTokens(
	context.Context,
) ([]auth.CollectorToken, error) {
	administration.record()
	return nil, nil
}

func (administration *browserGateTokenAdministration) UpdateCollectorToken(
	context.Context,
	string,
	uint64,
	auth.UpdateCollectorTokenRequest,
) (auth.CollectorToken, error) {
	administration.record()
	return auth.CollectorToken{}, errors.New("unexpected token update")
}

func (administration *browserGateTokenAdministration) RevokeCollectorToken(
	context.Context,
	string,
	uint64,
) (auth.CollectorToken, error) {
	administration.record()
	return auth.CollectorToken{}, errors.New("unexpected token revocation")
}

type observedRequestBody struct {
	reads int
}

func (body *observedRequestBody) Read([]byte) (int, error) {
	body.reads++
	return 0, io.EOF
}

func (*observedRequestBody) Close() error {
	return nil
}

func TestHandlerRequiresBrowserAuthenticationForAdministrativeServices(t *testing.T) {
	t.Parallel()

	indexes := &browserGateIndexAdministration{}
	base := Config{
		SearchJobs:    &fakeSearchJobs{},
		Indexes:       indexes,
		IndexAdmin:    indexes,
		SavedSearches: &fakeSavedSearches{},
		WebUI:         testUI(),
	}

	for _, test := range []struct {
		name          string
		authenticator auth.BrowserAuthenticator
	}{
		{name: "missing"},
		{name: "typed nil", authenticator: (*recordingBrowserAuthenticator)(nil)},
	} {
		t.Run(test.name, func(t *testing.T) {
			config := base
			config.BrowserAuthenticator = test.authenticator
			_, err := NewHandler(config)
			if err == nil ||
				!strings.Contains(err.Error(), "administrative services require browser authentication") {
				t.Fatalf("NewHandler error = %v", err)
			}
		})
	}

	if _, err := NewHandler(Config{
		SearchJobs:    &fakeSearchJobs{},
		Indexes:       fakeIndexCatalog{},
		SavedSearches: &fakeSavedSearches{},
		WebUI:         testUI(),
	}); err != nil {
		t.Fatalf("NewHandler without administrative services: %v", err)
	}
}

func TestEveryAdministratorRouteRequiresBrowserAuthenticationBeforeAdmission(t *testing.T) {
	t.Parallel()

	principal := browserGatePrincipal(
		t,
		browserGateTenantID,
		browserGateOwnerID,
		auth.BrowserRoleAdministrator,
	)
	authenticator := &recordingBrowserAuthenticator{
		fn: func(context.Context, []byte) (auth.BrowserPrincipal, error) {
			return principal, nil
		},
	}
	handler, indexes, tokens := newBrowserGateHandler(
		t,
		authenticator,
		defaultRouteTimeout,
	)

	for _, path := range browserGateAdministratorPaths {
		t.Run(path, func(t *testing.T) {
			body := &observedRequestBody{}
			request := httptest.NewRequestWithContext(
				context.Background(),
				http.MethodPost,
				path,
				nil,
			)
			request.Body = body
			request.Header.Set("Content-Type", "text/plain")
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, request)

			if response.Code != http.StatusUnauthorized {
				t.Fatalf(
					"status = %d, want %d; body = %s",
					response.Code,
					http.StatusUnauthorized,
					response.Body.String(),
				)
			}
			if response.Header().Get("WWW-Authenticate") !=
				administratorAuthenticationRealm {
				t.Fatalf(
					"WWW-Authenticate = %q",
					response.Header().Get("WWW-Authenticate"),
				)
			}
			if body.reads != 0 {
				t.Fatalf("request body reads = %d, want 0", body.reads)
			}
		})
	}
	if authenticator.callCount() != 0 {
		t.Fatalf("authenticator calls = %d, want 0", authenticator.callCount())
	}
	if indexes.callCount() != 0 || tokens.callCount() != 0 {
		t.Fatalf(
			"administrative service calls = indexes %d, tokens %d",
			indexes.callCount(),
			tokens.callCount(),
		)
	}
}

func TestBrowserAuthenticationIsScopedAndOrderedAfterExactRouteAndOriginChecks(t *testing.T) {
	t.Parallel()

	authenticator := &recordingBrowserAuthenticator{
		fn: func(context.Context, []byte) (auth.BrowserPrincipal, error) {
			return auth.BrowserPrincipal{}, errors.New("must not authenticate")
		},
	}
	handler, _, _ := newBrowserGateHandler(
		t,
		authenticator,
		defaultRouteTimeout,
	)

	ordinary := postProto(
		t,
		handler,
		"/api/v1/system/bootstrap",
		&opensplunkv1.GetSystemBootstrapRequest{},
	)
	if ordinary.Code != http.StatusOK {
		t.Fatalf(
			"ordinary route status = %d, body = %s",
			ordinary.Code,
			ordinary.Body.String(),
		)
	}

	tests := []struct {
		name   string
		method string
		path   string
		host   string
		origin string
		status int
	}{
		{
			name: "unknown route", method: http.MethodPost,
			path: "/api/v1/indexes/unknown", status: http.StatusNotFound,
		},
		{
			name: "wrong method", method: http.MethodGet,
			path: "/api/v1/indexes/list", host: "attacker.example",
			status: http.StatusMethodNotAllowed,
		},
		{
			name: "untrusted host", method: http.MethodPost,
			path: "/api/v1/indexes/list", host: "attacker.example",
			status: http.StatusForbidden,
		},
		{
			name: "foreign origin", method: http.MethodPost,
			path: "/api/v1/indexes/list", host: "example.com",
			origin: "http://attacker.example", status: http.StatusForbidden,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			request := httptest.NewRequestWithContext(
				context.Background(),
				test.method,
				test.path,
				nil,
			)
			if test.host != "" {
				request.Host = test.host
			}
			if test.origin != "" {
				request.Header.Set("Origin", test.origin)
			}
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, request)
			if response.Code != test.status {
				t.Fatalf(
					"status = %d, want %d; body = %s",
					response.Code,
					test.status,
					response.Body.String(),
				)
			}
		})
	}
	if authenticator.callCount() != 0 {
		t.Fatalf("authenticator calls = %d, want 0", authenticator.callCount())
	}
}

func TestAdministratorAuthorizationHeaderParsingFailsClosedBeforeWork(t *testing.T) {
	t.Parallel()

	validHeader := "Bearer " + adminIntegrationBearerToken
	tests := []struct {
		name   string
		values []string
		mutate func(http.Header)
	}{
		{name: "missing"},
		{name: "basic", values: []string{"Basic " + adminIntegrationBearerToken}},
		{name: "empty bearer", values: []string{"Bearer "}},
		{name: "missing separator", values: []string{"Bearer" + adminIntegrationBearerToken}},
		{name: "double separator", values: []string{"Bearer  " + adminIntegrationBearerToken}},
		{name: "tab separator", values: []string{"Bearer\t" + adminIntegrationBearerToken}},
		{name: "trailing whitespace", values: []string{validHeader + " "}},
		{name: "comma joined", values: []string{validHeader + "," + validHeader}},
		{name: "short token", values: []string{"Bearer short"}},
		{name: "invalid token68 character", values: []string{"Bearer " + strings.Repeat("a", 31) + ":"}},
		{name: "NUL token character", values: []string{"Bearer " + strings.Repeat("a", 31) + "\x00"}},
		{name: "DEL token character", values: []string{"Bearer " + strings.Repeat("a", 31) + "\x7f"}},
		{name: "non-ASCII token character", values: []string{"Bearer " + strings.Repeat("a", 31) + "é"}},
		{name: "interior padding", values: []string{"Bearer " + strings.Repeat("a", 31) + "=a"}},
		{name: "oversized", values: []string{"Bearer " + strings.Repeat("a", auth.MaximumBrowserBearerTokenBytes+1)}},
		{name: "duplicate values", values: []string{validHeader, validHeader}},
		{
			name: "case variant duplicate",
			mutate: func(header http.Header) {
				header["Authorization"] = []string{validHeader}
				header["authorization"] = []string{validHeader}
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			authenticator := &recordingBrowserAuthenticator{
				fn: func(context.Context, []byte) (auth.BrowserPrincipal, error) {
					return auth.BrowserPrincipal{}, errors.New("malformed header reached authenticator")
				},
			}
			handler, indexes, tokens := newBrowserGateHandler(
				t,
				authenticator,
				defaultRouteTimeout,
			)
			body := &observedRequestBody{}
			request := httptest.NewRequestWithContext(
				context.Background(),
				http.MethodPost,
				"/api/v1/indexes/list",
				nil,
			)
			request.Body = body
			request.Header["Authorization"] = test.values
			if test.mutate != nil {
				test.mutate(request.Header)
			}
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, request)

			if response.Code != http.StatusUnauthorized {
				t.Fatalf(
					"status = %d, want %d; body = %s",
					response.Code,
					http.StatusUnauthorized,
					response.Body.String(),
				)
			}
			if response.Header().Get("WWW-Authenticate") !=
				administratorAuthenticationRealm {
				t.Fatalf(
					"WWW-Authenticate = %q",
					response.Header().Get("WWW-Authenticate"),
				)
			}
			if authenticator.callCount() != 0 {
				t.Fatalf(
					"authenticator calls = %d, want 0",
					authenticator.callCount(),
				)
			}
			if body.reads != 0 ||
				indexes.callCount() != 0 ||
				tokens.callCount() != 0 {
				t.Fatalf(
					"rejected work = body reads %d, indexes %d, tokens %d",
					body.reads,
					indexes.callCount(),
					tokens.callCount(),
				)
			}
		})
	}
}

func TestAdministratorAuthenticationAndAuthorizationOutcomesAreSafe(t *testing.T) {
	t.Parallel()

	validToken := adminIntegrationBearerToken
	secretBackendError := "database password secret-auth-backend"
	tests := []struct {
		name          string
		principal     auth.BrowserPrincipal
		err           error
		status        int
		wantChallenge bool
	}{
		{
			name: "invalid credential", err: auth.ErrBrowserUnauthorized,
			status: http.StatusUnauthorized, wantChallenge: true,
		},
		{
			name: "ordinary user",
			principal: browserGatePrincipal(
				t,
				browserGateTenantID,
				browserGateOwnerID,
				auth.BrowserRoleUser,
			),
			status: http.StatusForbidden,
		},
		{
			name: "tenant mismatch",
			principal: browserGatePrincipal(
				t,
				"other-tenant",
				browserGateOwnerID,
				auth.BrowserRoleAdministrator,
			),
			status: http.StatusForbidden,
		},
		{
			name: "owner mismatch",
			principal: browserGatePrincipal(
				t,
				browserGateTenantID,
				"other-owner",
				auth.BrowserRoleAdministrator,
			),
			status: http.StatusForbidden,
		},
		{
			name:   "corrupt principal",
			status: http.StatusServiceUnavailable,
		},
		{
			name: "backend failure",
			err:  errors.New(secretBackendError), status: http.StatusServiceUnavailable,
		},
		{
			name: "authentication canceled",
			err:  context.Canceled, status: http.StatusRequestTimeout,
		},
		{
			name: "authentication deadline",
			err:  context.DeadlineExceeded, status: http.StatusRequestTimeout,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			authenticator := &recordingBrowserAuthenticator{
				fn: func(context.Context, []byte) (auth.BrowserPrincipal, error) {
					return test.principal, test.err
				},
			}
			handler, indexes, tokens := newBrowserGateHandler(
				t,
				authenticator,
				defaultRouteTimeout,
			)
			body := &observedRequestBody{}
			request := httptest.NewRequestWithContext(
				context.Background(),
				http.MethodPost,
				"/api/v1/indexes/list",
				nil,
			)
			request.Body = body
			request.Header.Set("Authorization", "Bearer "+validToken)
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, request)

			if response.Code != test.status {
				t.Fatalf(
					"status = %d, want %d; body = %s",
					response.Code,
					test.status,
					response.Body.String(),
				)
			}
			wantAuthenticate := ""
			if test.wantChallenge {
				wantAuthenticate = administratorAuthenticationRealm
			}
			if got := response.Header().Get("WWW-Authenticate"); got != wantAuthenticate {
				t.Fatalf(
					"WWW-Authenticate = %q, want %q",
					got,
					wantAuthenticate,
				)
			}
			if authenticator.callCount() != 1 {
				t.Fatalf(
					"authenticator calls = %d, want 1",
					authenticator.callCount(),
				)
			}
			if body.reads != 0 ||
				indexes.callCount() != 0 ||
				tokens.callCount() != 0 {
				t.Fatalf(
					"rejected work = body reads %d, indexes %d, tokens %d",
					body.reads,
					indexes.callCount(),
					tokens.callCount(),
				)
			}
			errorMessage := ""
			if test.err != nil {
				errorMessage = test.err.Error()
			}
			for _, forbidden := range []string{
				validToken,
				secretBackendError,
				errorMessage,
			} {
				if forbidden != "" &&
					strings.Contains(response.Body.String(), forbidden) {
					t.Fatalf(
						"response leaked %q: %s",
						forbidden,
						response.Body.String(),
					)
				}
			}
		})
	}
}

func TestAdministratorAuthenticationUsesRequestCancellationAndRouteDeadline(t *testing.T) {
	t.Parallel()

	t.Run("canceled request", func(t *testing.T) {
		authenticator := &recordingBrowserAuthenticator{
			fn: func(ctx context.Context, _ []byte) (auth.BrowserPrincipal, error) {
				return auth.BrowserPrincipal{}, ctx.Err()
			},
		}
		handler, _, _ := newBrowserGateHandler(
			t,
			authenticator,
			defaultRouteTimeout,
		)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		response := serveBrowserGateRequest(
			handler,
			httptest.NewRequestWithContext(
				ctx,
				http.MethodPost,
				"/api/v1/indexes/list",
				nil,
			),
			adminIntegrationBearerToken,
		)
		if response.Code != http.StatusRequestTimeout {
			t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
		}
	})

	t.Run("route deadline wraps authenticator", func(t *testing.T) {
		authenticator := &recordingBrowserAuthenticator{
			fn: func(ctx context.Context, _ []byte) (auth.BrowserPrincipal, error) {
				<-ctx.Done()
				return auth.BrowserPrincipal{}, ctx.Err()
			},
		}
		handler, _, _ := newBrowserGateHandler(
			t,
			authenticator,
			5*time.Millisecond,
		)
		response := serveBrowserGateRequest(
			handler,
			httptest.NewRequestWithContext(
				context.Background(),
				http.MethodPost,
				"/api/v1/indexes/list",
				nil,
			),
			adminIntegrationBearerToken,
		)
		if response.Code != http.StatusRequestTimeout {
			t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
		}
		if authenticator.callCount() != 1 {
			t.Fatalf(
				"authenticator calls = %d, want 1",
				authenticator.callCount(),
			)
		}
	})
}

func TestAdministratorAuthenticationPrecedesProtobufDecoding(t *testing.T) {
	t.Parallel()

	principal := browserGatePrincipal(
		t,
		browserGateTenantID,
		browserGateOwnerID,
		auth.BrowserRoleAdministrator,
	)
	tests := []struct {
		name        string
		principal   auth.BrowserPrincipal
		err         error
		status      int
		wantService int
	}{
		{
			name: "invalid credential hides malformed protobuf",
			err:  auth.ErrBrowserUnauthorized, status: http.StatusUnauthorized,
		},
		{
			name:      "valid credential exposes malformed protobuf",
			principal: principal, status: http.StatusBadRequest,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			authenticator := &recordingBrowserAuthenticator{
				fn: func(context.Context, []byte) (auth.BrowserPrincipal, error) {
					return test.principal, test.err
				},
			}
			handler, indexes, _ := newBrowserGateHandler(
				t,
				authenticator,
				defaultRouteTimeout,
			)
			request := httptest.NewRequestWithContext(
				context.Background(),
				http.MethodPost,
				"/api/v1/indexes/list",
				bytes.NewReader([]byte{0xff}),
			)
			request.Header.Set("Content-Type", "application/x-protobuf")
			request.Header.Set(
				"Authorization",
				"Bearer "+adminIntegrationBearerToken,
			)
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, request)
			if response.Code != test.status {
				t.Fatalf(
					"status = %d, want %d; body = %s",
					response.Code,
					test.status,
					response.Body.String(),
				)
			}
			if indexes.callCount() != test.wantService {
				t.Fatalf(
					"index service calls = %d, want %d",
					indexes.callCount(),
					test.wantService,
				)
			}
		})
	}
}

func TestValidAdministratorPrincipalIsExactAndCredentialBufferIsCleared(t *testing.T) {
	t.Parallel()

	principal := browserGatePrincipal(
		t,
		browserGateTenantID,
		browserGateOwnerID,
		auth.BrowserRoleAdministrator,
	)
	authenticator := &recordingBrowserAuthenticator{
		fn: func(context.Context, []byte) (auth.BrowserPrincipal, error) {
			return principal, nil
		},
	}
	handler, indexes, tokens := newBrowserGateHandler(
		t,
		authenticator,
		defaultRouteTimeout,
	)
	payload, err := proto.Marshal(&opensplunkv1.ListIndexesRequest{})
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequestWithContext(
		context.Background(),
		http.MethodPost,
		"/api/v1/indexes/list",
		bytes.NewReader(payload),
	)
	request.Header.Set("Content-Type", "application/x-protobuf")
	request.Header.Set(
		"Authorization",
		"bEaReR "+adminIntegrationBearerToken,
	)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)

	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
	}
	if authenticator.callCount() != 1 ||
		indexes.callCount() != 1 ||
		tokens.callCount() != 0 {
		t.Fatalf(
			"calls = authenticator %d, indexes %d, tokens %d",
			authenticator.callCount(),
			indexes.callCount(),
			tokens.callCount(),
		)
	}
	if !authenticator.aliasedTokenWasCleared() {
		t.Fatal("transport-owned credential buffer was not cleared")
	}
	capturedContext := indexes.capturedContext()
	captured, ok := capturedContext.Value(
		browserPrincipalContextKey{},
	).(auth.BrowserPrincipal)
	if !ok ||
		!captured.Valid() ||
		captured.TenantID() != browserGateTenantID ||
		captured.OwnerID() != browserGateOwnerID ||
		captured.Role() != auth.BrowserRoleAdministrator {
		t.Fatalf(
			"captured principal = tenant %q owner %q role %v valid %t",
			captured.TenantID(),
			captured.OwnerID(),
			captured.Role(),
			captured.Valid(),
		)
	}
	if _, ok := request.Context().Value(
		browserPrincipalContextKey{},
	).(auth.BrowserPrincipal); ok {
		t.Fatal("original request context was mutated")
	}
	if request.Header.Get("Authorization") !=
		"bEaReR "+adminIntegrationBearerToken {
		t.Fatal("caller-owned Authorization header was mutated")
	}
	if strings.Contains(response.Body.String(), adminIntegrationBearerToken) {
		t.Fatal("successful response leaked administrator credential")
	}
}

func TestAuthorizedDownstreamRequestStripsEveryAuthorizationKeyVariant(t *testing.T) {
	t.Parallel()

	principal := browserGatePrincipal(
		t,
		browserGateTenantID,
		browserGateOwnerID,
		auth.BrowserRoleAdministrator,
	)
	for _, headerName := range []string{
		"Authorization",
		"authorization",
		"aUtHoRiZaTiOn",
	} {
		t.Run(headerName, func(t *testing.T) {
			authenticator := &recordingBrowserAuthenticator{
				fn: func(context.Context, []byte) (auth.BrowserPrincipal, error) {
					return principal, nil
				},
			}
			api := &apiHandler{
				browserAuthenticator: authenticator,
				administratorRoutes: map[string]struct{}{
					"/api/v1/indexes/list": {},
				},
				tenantID:     browserGateTenantID,
				ownerID:      browserGateOwnerID,
				routeTimeout: defaultRouteTimeout,
				browserAllowedHosts: map[string]struct{}{
					"example.com": {},
				},
			}
			var downstream *http.Request
			protected := api.protectBrowserAPIRoutes(http.HandlerFunc(
				func(response http.ResponseWriter, request *http.Request) {
					downstream = request
					response.WriteHeader(http.StatusNoContent)
				},
			))
			request := httptest.NewRequestWithContext(
				context.Background(),
				http.MethodPost,
				"/api/v1/indexes/list",
				nil,
			)
			request.Header[headerName] = []string{
				"Bearer " + adminIntegrationBearerToken,
			}
			response := httptest.NewRecorder()
			protected.ServeHTTP(response, request)

			if response.Code != http.StatusNoContent {
				t.Fatalf(
					"status = %d, body = %s",
					response.Code,
					response.Body.String(),
				)
			}
			if downstream == nil {
				t.Fatal("authorized request did not reach downstream")
			}
			for name := range downstream.Header {
				if strings.EqualFold(name, "Authorization") {
					t.Fatalf(
						"downstream retained Authorization key %q",
						name,
					)
				}
			}
			if got := request.Header[headerName]; len(got) != 1 ||
				got[0] != "Bearer "+adminIntegrationBearerToken {
				t.Fatalf(
					"caller-owned header %q was mutated: %q",
					headerName,
					got,
				)
			}
			captured, ok := browserPrincipalFromRequest(downstream)
			if !ok ||
				captured.TenantID() != browserGateTenantID ||
				captured.OwnerID() != browserGateOwnerID ||
				captured.Role() != auth.BrowserRoleAdministrator {
				t.Fatalf(
					"downstream principal = tenant %q owner %q role %v, present %t",
					captured.TenantID(),
					captured.OwnerID(),
					captured.Role(),
					ok,
				)
			}
		})
	}
}

func newBrowserGateHandler(
	t *testing.T,
	authenticator auth.BrowserAuthenticator,
	routeTimeout time.Duration,
) (*Handler, *browserGateIndexAdministration, *browserGateTokenAdministration) {
	t.Helper()
	indexes := &browserGateIndexAdministration{}
	tokens := &browserGateTokenAdministration{}
	handler, err := NewHandler(Config{
		SearchJobs:                 &fakeSearchJobs{},
		Indexes:                    indexes,
		IndexAdmin:                 indexes,
		IngestionTokens:            tokens,
		CollectorAdmin:             browserGateCollectorAdministration{},
		AppAdmin:                   &fakeAppAdministration{},
		AppCursorKey:               appAdministrationCursorKey,
		SearchInspections:          &fakeSearchInspections{},
		SavedSearches:              &fakeSavedSearches{},
		BrowserAuthenticator:       authenticator,
		WebUI:                      testUI(),
		TenantID:                   browserGateTenantID,
		OwnerID:                    browserGateOwnerID,
		RouteTimeout:               routeTimeout,
		AdministrativeAllowedHosts: []string{"example.com"},
	})
	if err != nil {
		t.Fatalf("NewHandler: %v", err)
	}
	return handler, indexes, tokens
}

func browserGatePrincipal(
	t *testing.T,
	tenantID string,
	ownerID string,
	role auth.BrowserRole,
) auth.BrowserPrincipal {
	t.Helper()
	authenticator, err := auth.NewBearerTokenAuthenticator(
		[]byte(adminIntegrationBearerToken),
		tenantID,
		ownerID,
		role,
	)
	if err != nil {
		t.Fatalf("NewBearerTokenAuthenticator: %v", err)
	}
	principal, err := authenticator.Authenticate(
		context.Background(),
		[]byte(adminIntegrationBearerToken),
	)
	if err != nil {
		t.Fatalf("Authenticate test principal: %v", err)
	}
	return principal
}

func serveBrowserGateRequest(
	handler http.Handler,
	request *http.Request,
	token string,
) *httptest.ResponseRecorder {
	request.Header.Set("Authorization", "Bearer "+token)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	return response
}
