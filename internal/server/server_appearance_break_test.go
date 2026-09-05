package server

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/auth"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/uipalette"
	"google.golang.org/protobuf/proto"
)

// countingServerSettings records how many times the store-side appearance
// update ran, so a wire-level rejection can be proven to stop at the
// sanitizer rather than reaching the store.
type countingServerSettings struct {
	*fakeServerSettings
	mu          sync.Mutex
	updateCalls int
}

func (settings *countingServerSettings) UpdateAppearance(
	ctx context.Context,
	expectedVersion uint64,
	palette uipalette.Palette,
) (control.ServerAppearanceSettings, error) {
	settings.mu.Lock()
	settings.updateCalls++
	settings.mu.Unlock()
	return settings.fakeServerSettings.UpdateAppearance(ctx, expectedVersion, palette)
}

func (settings *countingServerSettings) updateCallCount() int {
	settings.mu.Lock()
	defer settings.mu.Unlock()
	return settings.updateCalls
}

func newAppearanceGateHandler(t *testing.T, authenticator auth.BrowserAuthenticator, settings Settings) *Handler {
	t.Helper()
	handler, err := NewHandler(Config{
		SearchJobs:                 &fakeSearchJobs{},
		Indexes:                    fakeIndexCatalog{},
		IndexAdmin:                 &browserGateIndexAdministration{},
		ServerSettings:             settings,
		SavedSearches:              &fakeSavedSearches{},
		BrowserAuthenticator:       authenticator,
		WebUI:                      testUI(),
		TenantID:                   browserGateTenantID,
		OwnerID:                    browserGateOwnerID,
		RouteTimeout:               defaultRouteTimeout,
		AdministrativeAllowedHosts: []string{"example.com"},
	})
	if err != nil {
		t.Fatalf("NewHandler: %v", err)
	}
	return handler
}

func postAppearanceProto(
	t *testing.T,
	handler http.Handler,
	path string,
	message proto.Message,
	bearer string,
) *httptest.ResponseRecorder {
	t.Helper()
	headers := map[string]string{}
	if bearer != "" {
		headers["Authorization"] = "Bearer " + bearer
	}
	return postProtoHeaders(t, handler, path, message, headers)
}

func decodeAppearanceProto(t *testing.T, response *httptest.ResponseRecorder, message proto.Message) {
	t.Helper()
	if err := proto.Unmarshal(response.Body.Bytes(), message); err != nil {
		t.Fatalf("decode %T: %v (body %q)", message, err, response.Body.String())
	}
}

// TestAppearanceRoutesGateEveryPrincipalAtTheWire sends real HTTP requests
// through the router: no credential is challenged with 401 before the body
// is read, an ordinary user is refused with 403, and only an administrator
// reaches the handler on both appearance routes.
func TestAppearanceRoutesGateEveryPrincipalAtTheWire(t *testing.T) {
	t.Parallel()
	const userToken = "open-splunk-ordinary-user-test-token-9876543210"
	administrator := browserGatePrincipal(t, browserGateTenantID, browserGateOwnerID, auth.BrowserRoleAdministrator)
	user := browserGatePrincipal(t, browserGateTenantID, browserGateOwnerID, auth.BrowserRoleUser)
	authenticator := &recordingBrowserAuthenticator{
		fn: func(_ context.Context, token []byte) (auth.BrowserPrincipal, error) {
			switch string(token) {
			case adminIntegrationBearerToken:
				return administrator, nil
			case userToken:
				return user, nil
			default:
				return auth.BrowserPrincipal{}, auth.ErrBrowserUnauthorized
			}
		},
	}
	settings := &countingServerSettings{fakeServerSettings: newFakeServerSettings()}
	handler := newAppearanceGateHandler(t, authenticator, settings)

	routes := []struct {
		path    string
		message proto.Message
	}{
		{path: "/api/server/appearance/get", message: &opensplunk.GetServerAppearanceRequest{}},
		{
			path: "/api/server/appearance/update",
			message: &opensplunk.UpdateServerAppearanceRequest{
				ExpectedVersion: 0, Palette: opensplunk.UiPalette_UI_PALETTE_OCEAN,
			},
		},
	}
	for _, route := range routes {
		t.Run("unauthenticated "+route.path, func(t *testing.T) {
			payload, err := proto.Marshal(route.message)
			if err != nil {
				t.Fatal(err)
			}
			body := &observedRequestBody{}
			request := httptest.NewRequestWithContext(context.Background(), http.MethodPost, route.path, nil)
			request.Body = body
			request.ContentLength = int64(len(payload))
			request.Header.Set("Content-Type", "application/x-protobuf")
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, request)
			if response.Code != http.StatusUnauthorized ||
				response.Header().Get("WWW-Authenticate") != administratorAuthenticationRealm {
				t.Fatalf("status = %d, WWW-Authenticate = %q, body = %s",
					response.Code, response.Header().Get("WWW-Authenticate"), response.Body.String())
			}
			if body.reads != 0 {
				t.Fatalf("request body reads = %d, want 0", body.reads)
			}
		})
		t.Run("garbage token "+route.path, func(t *testing.T) {
			response := postAppearanceProto(t, handler, route.path, route.message, "not-a-real-token")
			if response.Code != http.StatusUnauthorized {
				t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
			}
		})
		t.Run("ordinary user "+route.path, func(t *testing.T) {
			response := postAppearanceProto(t, handler, route.path, route.message, userToken)
			if response.Code != http.StatusForbidden ||
				!strings.Contains(response.Body.String(), "administrator access is required") {
				t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
			}
		})
	}
	if settings.updateCallCount() != 0 || settings.CurrentAppearance().Version != 0 {
		t.Fatalf("rejected principals reached the store: calls %d, appearance %+v",
			settings.updateCallCount(), settings.CurrentAppearance())
	}

	get := postAppearanceProto(t, handler, "/api/server/appearance/get",
		&opensplunk.GetServerAppearanceRequest{}, adminIntegrationBearerToken)
	if get.Code != http.StatusOK {
		t.Fatalf("administrator get status = %d, body = %s", get.Code, get.Body.String())
	}
	var getResponse opensplunk.GetServerAppearanceResponse
	decodeAppearanceProto(t, get, &getResponse)
	if getResponse.GetCurrent().GetVersion() != 0 ||
		getResponse.GetCurrent().GetPalette() != opensplunk.UiPalette_UI_PALETTE_CLASSIC ||
		getResponse.GetCurrent().GetUpdatedAt() != nil ||
		getResponse.GetDefaultPalette() != opensplunk.UiPalette_UI_PALETTE_CLASSIC {
		t.Fatalf("administrator get = %+v", &getResponse)
	}
	update := postAppearanceProto(t, handler, "/api/server/appearance/update",
		&opensplunk.UpdateServerAppearanceRequest{ExpectedVersion: 0, Palette: opensplunk.UiPalette_UI_PALETTE_OCEAN},
		adminIntegrationBearerToken)
	if update.Code != http.StatusOK {
		t.Fatalf("administrator update status = %d, body = %s", update.Code, update.Body.String())
	}
	var updateResponse opensplunk.UpdateServerAppearanceResponse
	decodeAppearanceProto(t, update, &updateResponse)
	if updateResponse.GetCurrent().GetVersion() != 1 ||
		updateResponse.GetCurrent().GetPalette() != opensplunk.UiPalette_UI_PALETTE_OCEAN ||
		updateResponse.GetCurrent().GetUpdatedAt() == nil ||
		updateResponse.GetDefaultPalette() != opensplunk.UiPalette_UI_PALETTE_CLASSIC {
		t.Fatalf("administrator update = %+v", &updateResponse)
	}
	if settings.updateCallCount() != 1 || settings.CurrentAppearance().Palette != uipalette.Ocean {
		t.Fatalf("store after administrator update: calls %d, appearance %+v",
			settings.updateCallCount(), settings.CurrentAppearance())
	}
	if strings.Contains(update.Body.String(), adminIntegrationBearerToken) ||
		strings.Contains(get.Body.String(), adminIntegrationBearerToken) {
		t.Fatal("response leaked the administrator credential")
	}
}

// TestAppearanceUpdateRejectsUnlistedPalettesBeforeTheStore proves the
// sanitizer stops UNSPECIFIED, out-of-range, and negative enum numbers with
// 400 at the wire, that the store never sees them, and that a stale version
// after a successful save is a 409 that leaves the stored palette intact.
func TestAppearanceUpdateRejectsUnlistedPalettesBeforeTheStore(t *testing.T) {
	t.Parallel()
	administrator := browserGatePrincipal(t, browserGateTenantID, browserGateOwnerID, auth.BrowserRoleAdministrator)
	authenticator := &recordingBrowserAuthenticator{
		fn: func(context.Context, []byte) (auth.BrowserPrincipal, error) { return administrator, nil },
	}
	settings := &countingServerSettings{fakeServerSettings: newFakeServerSettings()}
	handler := newAppearanceGateHandler(t, authenticator, settings)
	update := func(expectedVersion uint64, palette opensplunk.UiPalette) *httptest.ResponseRecorder {
		t.Helper()
		return postAppearanceProto(t, handler, "/api/server/appearance/update",
			&opensplunk.UpdateServerAppearanceRequest{ExpectedVersion: expectedVersion, Palette: palette},
			adminIntegrationBearerToken)
	}

	for _, palette := range []opensplunk.UiPalette{
		opensplunk.UiPalette_UI_PALETTE_UNSPECIFIED, 7, 99, -1, 1 << 30, -(1 << 30),
	} {
		response := update(0, palette)
		if response.Code != http.StatusBadRequest ||
			!strings.Contains(response.Body.String(), "ui palette is invalid") {
			t.Fatalf("palette %d status = %d, body = %s", palette, response.Code, response.Body.String())
		}
	}
	// An empty body decodes as UNSPECIFIED with version 0: still a 400.
	empty := postProtoHeaders(t, handler, "/api/server/appearance/update",
		&opensplunk.UpdateServerAppearanceRequest{},
		map[string]string{"Authorization": "Bearer " + adminIntegrationBearerToken})
	if empty.Code != http.StatusBadRequest {
		t.Fatalf("empty update status = %d, body = %s", empty.Code, empty.Body.String())
	}
	if settings.updateCallCount() != 0 {
		t.Fatalf("invalid palettes reached the store %d times", settings.updateCallCount())
	}
	if settings.CurrentAppearance().Version != 0 || settings.CurrentAppearance().Palette != uipalette.Classic {
		t.Fatalf("invalid palettes changed the appearance: %+v", settings.CurrentAppearance())
	}

	// A raw enum number that is not a listed value but also not a known Go
	// constant reaches the sanitizer as an open enum and is rejected there.
	rawPayload := []byte{0x08, 0x00, 0x10, 0x2a} // expected_version = 0, palette = 42
	rawRequest := httptest.NewRequestWithContext(context.Background(), http.MethodPost,
		"/api/server/appearance/update", bytes.NewReader(rawPayload))
	rawRequest.Header.Set("Content-Type", "application/x-protobuf")
	rawRequest.Header.Set("Authorization", "Bearer "+adminIntegrationBearerToken)
	rawResponse := httptest.NewRecorder()
	handler.ServeHTTP(rawResponse, rawRequest)
	if rawResponse.Code != http.StatusBadRequest {
		t.Fatalf("raw enum 42 status = %d, body = %s", rawResponse.Code, rawResponse.Body.String())
	}
	if settings.updateCallCount() != 0 {
		t.Fatalf("raw enum reached the store %d times", settings.updateCallCount())
	}

	first := update(0, opensplunk.UiPalette_UI_PALETTE_GRAPHITE)
	if first.Code != http.StatusOK {
		t.Fatalf("first update status = %d, body = %s", first.Code, first.Body.String())
	}
	for _, stale := range []uint64{0, 2, 1 << 62} {
		response := update(stale, opensplunk.UiPalette_UI_PALETTE_EMBER)
		if response.Code != http.StatusConflict ||
			!strings.Contains(response.Body.String(), "reload and try again") {
			t.Fatalf("stale version %d status = %d, body = %s", stale, response.Code, response.Body.String())
		}
	}
	if settings.updateCallCount() != 4 {
		t.Fatalf("store update calls = %d, want 4 (one commit, three conflicts)", settings.updateCallCount())
	}
	current := settings.CurrentAppearance()
	if current.Version != 1 || current.Palette != uipalette.Graphite {
		t.Fatalf("stale updates changed the appearance: %+v", current)
	}
	get := postAppearanceProto(t, handler, "/api/server/appearance/get",
		&opensplunk.GetServerAppearanceRequest{}, adminIntegrationBearerToken)
	var getResponse opensplunk.GetServerAppearanceResponse
	decodeAppearanceProto(t, get, &getResponse)
	if get.Code != http.StatusOK || getResponse.GetCurrent().GetVersion() != 1 ||
		getResponse.GetCurrent().GetPalette() != opensplunk.UiPalette_UI_PALETTE_GRAPHITE {
		t.Fatalf("get after conflicts = %d %+v", get.Code, &getResponse)
	}
}

// TestBootstrapServesTheLivePaletteWithoutAuthenticationOrRestart proves
// the unauthenticated bootstrap route (what the sign-in page calls) reflects
// each administrator save on the very next request from the same handler,
// and reports UNSPECIFIED when no settings service is configured.
func TestBootstrapServesTheLivePaletteWithoutAuthenticationOrRestart(t *testing.T) {
	t.Parallel()
	administrator := browserGatePrincipal(t, browserGateTenantID, browserGateOwnerID, auth.BrowserRoleAdministrator)
	authenticator := &recordingBrowserAuthenticator{
		fn: func(context.Context, []byte) (auth.BrowserPrincipal, error) { return administrator, nil },
	}
	settings := &countingServerSettings{fakeServerSettings: newFakeServerSettings()}
	handler := newAppearanceGateHandler(t, authenticator, settings)
	bootstrap := func() opensplunk.UiPalette {
		t.Helper()
		response := postProto(t, handler, "/api/system/bootstrap", &opensplunk.GetSystemBootstrapRequest{})
		if response.Code != http.StatusOK {
			t.Fatalf("bootstrap status = %d, body = %s", response.Code, response.Body.String())
		}
		var decoded opensplunk.GetSystemBootstrapResponse
		decodeAppearanceProto(t, response, &decoded)
		return decoded.GetUiPalette()
	}
	if got := bootstrap(); got != opensplunk.UiPalette_UI_PALETTE_CLASSIC {
		t.Fatalf("initial bootstrap palette = %v, want CLASSIC", got)
	}
	expectedVersion := uint64(0)
	for _, palette := range []opensplunk.UiPalette{
		opensplunk.UiPalette_UI_PALETTE_TERMINAL,
		opensplunk.UiPalette_UI_PALETTE_GLASS,
		opensplunk.UiPalette_UI_PALETTE_CLASSIC,
		opensplunk.UiPalette_UI_PALETTE_EMBER,
	} {
		response := postAppearanceProto(t, handler, "/api/server/appearance/update",
			&opensplunk.UpdateServerAppearanceRequest{ExpectedVersion: expectedVersion, Palette: palette},
			adminIntegrationBearerToken)
		if response.Code != http.StatusOK {
			t.Fatalf("update to %v status = %d, body = %s", palette, response.Code, response.Body.String())
		}
		expectedVersion++
		if got := bootstrap(); got != palette {
			t.Fatalf("bootstrap palette after update = %v, want %v", got, palette)
		}
	}
	// A rejected save leaves bootstrap on the last committed palette.
	rejected := postAppearanceProto(t, handler, "/api/server/appearance/update",
		&opensplunk.UpdateServerAppearanceRequest{ExpectedVersion: 0, Palette: opensplunk.UiPalette_UI_PALETTE_OCEAN},
		adminIntegrationBearerToken)
	if rejected.Code != http.StatusConflict {
		t.Fatalf("stale update status = %d", rejected.Code)
	}
	if got := bootstrap(); got != opensplunk.UiPalette_UI_PALETTE_EMBER {
		t.Fatalf("bootstrap palette after rejected update = %v, want EMBER", got)
	}
	if authenticator.callCount() != 5 {
		t.Fatalf("authenticator calls = %d, want 5 (bootstrap never authenticates)", authenticator.callCount())
	}

	withoutSettings, err := NewHandler(Config{
		SearchJobs:                 &fakeSearchJobs{},
		Indexes:                    fakeIndexCatalog{},
		SavedSearches:              &fakeSavedSearches{},
		WebUI:                      testUI(),
		AdministrativeAllowedHosts: []string{"example.com"},
	})
	if err != nil {
		t.Fatal(err)
	}
	response := postProto(t, withoutSettings, "/api/system/bootstrap", &opensplunk.GetSystemBootstrapRequest{})
	if response.Code != http.StatusOK {
		t.Fatalf("bootstrap without settings status = %d, body = %s", response.Code, response.Body.String())
	}
	var decoded opensplunk.GetSystemBootstrapResponse
	decodeAppearanceProto(t, response, &decoded)
	if decoded.GetUiPalette() != opensplunk.UiPalette_UI_PALETTE_UNSPECIFIED {
		t.Fatalf("bootstrap without settings palette = %v, want UNSPECIFIED", decoded.GetUiPalette())
	}
	if containsFeature(decoded.GetFeatures(), opensplunk.ServerFeature_SERVER_FEATURE_SERVER_SETTINGS_ADMIN) {
		t.Fatal("handler without a settings service advertised the settings admin feature")
	}
	for _, path := range []string{"/api/server/appearance/get", "/api/server/appearance/update"} {
		missing := postProto(t, withoutSettings, path, &opensplunk.GetServerAppearanceRequest{})
		if missing.Code != http.StatusNotFound {
			t.Fatalf("%s without settings status = %d, want 404", path, missing.Code)
		}
	}
}
