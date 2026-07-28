package server

import (
	"context"
	"errors"
	"net/http"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
)

type fakeBootstrapAppCatalog struct {
	mu sync.Mutex

	result  AppCatalogResult
	err     error
	fn      func(context.Context, string, uint32) (AppCatalogResult, error)
	calls   int
	tenant  string
	maximum uint32
}

func (catalog *fakeBootstrapAppCatalog) ListActiveApps(
	ctx context.Context,
	tenantID string,
	maximum uint32,
) (AppCatalogResult, error) {
	catalog.mu.Lock()
	catalog.calls++
	catalog.tenant = tenantID
	catalog.maximum = maximum
	fn := catalog.fn
	result, err := catalog.result, catalog.err
	catalog.mu.Unlock()
	if fn != nil {
		return fn(ctx, tenantID, maximum)
	}
	return result, err
}

func (catalog *fakeBootstrapAppCatalog) setResult(result AppCatalogResult) {
	catalog.mu.Lock()
	defer catalog.mu.Unlock()
	catalog.result = result
	catalog.err = nil
}

func (catalog *fakeBootstrapAppCatalog) captured() (int, string, uint32) {
	catalog.mu.Lock()
	defer catalog.mu.Unlock()
	return catalog.calls, catalog.tenant, catalog.maximum
}

func TestBootstrapAppCatalogIsLiveTenantScopedAndActiveOnly(t *testing.T) {
	catalog := &fakeBootstrapAppCatalog{}
	catalog.setResult(AppCatalogResult{
		Complete: true,
		Apps: []AppCatalogSummary{
			validBootstrapCatalogApp("app_zeta", "zeta", "Zeta", "main"),
			validBootstrapCatalogApp(
				"app_alpha",
				"alpha",
				"Alpha",
				"archive",
				"main",
			),
		},
	})
	handler := newTestHandler(t, Config{
		SearchJobs: &fakeSearchJobs{},
		Indexes:    fakeIndexCatalog{},
		AppCatalog: catalog,
		WebUI:      testUI(),
		TenantID:   "tenant-bootstrap",
		Bootstrap: BootstrapConfig{
			SelectedAppID: "app_zeta",
		},
	})

	bootstrap := readBootstrap(
		t,
		handler,
		&opensplunkv1.GetSystemBootstrapRequest{
			PreferredAppId: stringPointer("app_archived"),
		},
	)
	if got := bootstrap.GetApps(); len(got) != 2 ||
		got[0].GetAppId() != "app_alpha" ||
		got[1].GetAppId() != "app_zeta" {
		t.Fatalf("unordered live apps were not canonicalized = %+v", got)
	}
	if bootstrap.GetSelectedAppId() != "app_zeta" {
		t.Fatalf(
			"missing/archived preferred selection = %q, want configured active app",
			bootstrap.GetSelectedAppId(),
		)
	}
	alpha := bootstrap.GetApps()[0]
	if alpha.GetSlug() != "alpha" ||
		alpha.GetDisplayName() != "Alpha" ||
		!slices.Equal(
			alpha.GetDefaultIndexNames(),
			[]string{"archive", "main"},
		) ||
		alpha.GetState() != opensplunkv1.AppState_APP_STATE_ACTIVE {
		t.Fatalf("active app projection = %+v", alpha)
	}
	calls, tenantID, maximum := catalog.captured()
	if calls != 1 ||
		tenantID != "tenant-bootstrap" ||
		maximum != uint32(maximumBootstrapApps) {
		t.Fatalf(
			"catalog call = (%d, %q, %d)",
			calls,
			tenantID,
			maximum,
		)
	}

	bootstrap = readBootstrap(
		t,
		handler,
		&opensplunkv1.GetSystemBootstrapRequest{
			PreferredAppId: stringPointer("app_missing"),
		},
	)
	if bootstrap.GetSelectedAppId() != "app_zeta" {
		t.Fatalf(
			"missing preferred selection = %q, want configured active app",
			bootstrap.GetSelectedAppId(),
		)
	}

	bootstrap = readBootstrap(
		t,
		handler,
		&opensplunkv1.GetSystemBootstrapRequest{
			PreferredAppId: stringPointer("app_zeta"),
		},
	)
	if bootstrap.GetSelectedAppId() != "app_zeta" {
		t.Fatalf(
			"active preferred selection = %q",
			bootstrap.GetSelectedAppId(),
		)
	}

	// Archiving alpha is represented by its immediate removal from the
	// read-only active result; no handler reconstruction is needed.
	catalog.setResult(AppCatalogResult{
		Complete: true,
		Apps: []AppCatalogSummary{
			validBootstrapCatalogApp("app_zeta", "zeta", "Zeta", "main"),
		},
	})
	bootstrap = readBootstrap(
		t,
		handler,
		&opensplunkv1.GetSystemBootstrapRequest{
			PreferredAppId: stringPointer("app_alpha"),
		},
	)
	if got := bootstrap.GetApps(); len(got) != 1 ||
		got[0].GetAppId() != "app_zeta" ||
		bootstrap.GetSelectedAppId() != "app_zeta" {
		t.Fatalf("archived app remained visible/selectable = %+v", bootstrap)
	}

	// A subsequently created active app is equally visible on the next call.
	catalog.setResult(AppCatalogResult{
		Complete: true,
		Apps: []AppCatalogSummary{
			validBootstrapCatalogApp("app_zeta", "zeta", "Zeta", "main"),
			validBootstrapCatalogApp("app_beta", "beta", "Beta", "main"),
		},
	})
	bootstrap = readBootstrap(
		t,
		handler,
		&opensplunkv1.GetSystemBootstrapRequest{},
	)
	if got := bootstrap.GetApps(); len(got) != 2 ||
		got[0].GetAppId() != "app_beta" ||
		got[1].GetAppId() != "app_zeta" {
		t.Fatalf("created active app was not visible = %+v", got)
	}
}

func TestBootstrapAppCatalogOutputIsDetached(t *testing.T) {
	indexes := []string{"archive", "main"}
	catalog := AppCatalogResult{
		Complete: true,
		Apps: []AppCatalogSummary{{
			AppID:             "app_alpha",
			Slug:              "alpha",
			DisplayName:       "Alpha",
			DefaultIndexNames: indexes,
		}},
	}
	apps, err := appCatalogSummariesToProto(catalog)
	if err != nil {
		t.Fatalf("appCatalogSummariesToProto: %v", err)
	}
	indexes[0] = "mutated"
	catalog.Apps[0].DefaultIndexNames[1] = "also-mutated"
	if got := apps[0].GetDefaultIndexNames(); !slices.Equal(
		got,
		[]string{"archive", "main"},
	) {
		t.Fatalf("catalog result alias leaked = %v", got)
	}
}

func TestBootstrapAppCatalogRejectsCorruptIncompleteAndDuplicateOutput(
	t *testing.T,
) {
	oversized := make([]AppCatalogSummary, maximumBootstrapApps+1)
	for index := range oversized {
		suffix := leftPadDecimal(index, 3)
		oversized[index] = validBootstrapCatalogApp(
			"app_"+suffix,
			"app-"+suffix,
			"App "+suffix,
		)
	}
	validAlpha := validBootstrapCatalogApp(
		"app_alpha",
		"alpha",
		"Alpha",
		"main",
	)
	validBeta := validBootstrapCatalogApp(
		"app_beta",
		"beta",
		"Beta",
		"main",
	)
	tests := []struct {
		name   string
		result AppCatalogResult
	}{
		{
			name: "incomplete",
			result: AppCatalogResult{
				Complete: false,
				Apps:     []AppCatalogSummary{validAlpha},
			},
		},
		{
			name: "oversized",
			result: AppCatalogResult{
				Complete: true,
				Apps:     oversized,
			},
		},
		{
			name: "empty ID",
			result: AppCatalogResult{
				Complete: true,
				Apps: []AppCatalogSummary{{
					Slug:        "alpha",
					DisplayName: "Alpha",
				}},
			},
		},
		{
			name: "padded ID",
			result: AppCatalogResult{
				Complete: true,
				Apps: []AppCatalogSummary{{
					AppID:       " app_alpha",
					Slug:        "alpha",
					DisplayName: "Alpha",
				}},
			},
		},
		{
			name: "noncanonical slug",
			result: AppCatalogResult{
				Complete: true,
				Apps: []AppCatalogSummary{{
					AppID:       "app_alpha",
					Slug:        "Alpha",
					DisplayName: "Alpha",
				}},
			},
		},
		{
			name: "noncanonical display name",
			result: AppCatalogResult{
				Complete: true,
				Apps: []AppCatalogSummary{{
					AppID:       "app_alpha",
					Slug:        "alpha",
					DisplayName: " Alpha",
				}},
			},
		},
		{
			name: "noncanonical indexes",
			result: AppCatalogResult{
				Complete: true,
				Apps: []AppCatalogSummary{{
					AppID:             "app_alpha",
					Slug:              "alpha",
					DisplayName:       "Alpha",
					DefaultIndexNames: []string{"zeta", "alpha"},
				}},
			},
		},
		{
			name: "duplicate ID",
			result: AppCatalogResult{
				Complete: true,
				Apps: []AppCatalogSummary{
					validAlpha,
					{
						AppID:       validAlpha.AppID,
						Slug:        validBeta.Slug,
						DisplayName: validBeta.DisplayName,
					},
				},
			},
		},
		{
			name: "duplicate slug",
			result: AppCatalogResult{
				Complete: true,
				Apps: []AppCatalogSummary{
					validAlpha,
					{
						AppID:       validBeta.AppID,
						Slug:        validAlpha.Slug,
						DisplayName: validBeta.DisplayName,
					},
				},
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			handler := newTestHandler(t, Config{
				SearchJobs: &fakeSearchJobs{},
				Indexes:    fakeIndexCatalog{},
				AppCatalog: &fakeBootstrapAppCatalog{result: test.result},
				WebUI:      testUI(),
			})
			response := postProto(
				t,
				handler,
				"/api/v1/system/bootstrap",
				&opensplunkv1.GetSystemBootstrapRequest{},
			)
			if response.Code != http.StatusInternalServerError ||
				!strings.Contains(
					response.Body.String(),
					"internal server error",
				) ||
				strings.Contains(response.Body.String(), "catalog") {
				t.Fatalf(
					"corrupt catalog response = %d, %s",
					response.Code,
					response.Body,
				)
			}
		})
	}
}

func TestBootstrapAppCatalogMapsCancellationAndStorageFailure(t *testing.T) {
	t.Run("storage failure", func(t *testing.T) {
		handler := newTestHandler(t, Config{
			SearchJobs: &fakeSearchJobs{},
			Indexes:    fakeIndexCatalog{},
			AppCatalog: &fakeBootstrapAppCatalog{
				err: errors.New("database password leaked"),
			},
			WebUI: testUI(),
		})
		response := postProto(
			t,
			handler,
			"/api/v1/system/bootstrap",
			&opensplunkv1.GetSystemBootstrapRequest{},
		)
		if response.Code != http.StatusServiceUnavailable ||
			strings.Contains(response.Body.String(), "password") {
			t.Fatalf(
				"storage failure response = %d, %s",
				response.Code,
				response.Body,
			)
		}
	})

	t.Run("operation cancellation", func(t *testing.T) {
		handler := newTestHandler(t, Config{
			SearchJobs: &fakeSearchJobs{},
			Indexes:    fakeIndexCatalog{},
			AppCatalog: &fakeBootstrapAppCatalog{err: context.Canceled},
			WebUI:      testUI(),
		})
		response := postProto(
			t,
			handler,
			"/api/v1/system/bootstrap",
			&opensplunkv1.GetSystemBootstrapRequest{},
		)
		if response.Code != http.StatusRequestTimeout {
			t.Fatalf(
				"canceled catalog response = %d, %s",
				response.Code,
				response.Body,
			)
		}
	})

	t.Run("route deadline reaches storage", func(t *testing.T) {
		contextCanceled := make(chan struct{})
		catalog := &fakeBootstrapAppCatalog{
			fn: func(
				ctx context.Context,
				_ string,
				_ uint32,
			) (AppCatalogResult, error) {
				<-ctx.Done()
				close(contextCanceled)
				return AppCatalogResult{}, ctx.Err()
			},
		}
		handler := newTestHandler(t, Config{
			SearchJobs:   &fakeSearchJobs{},
			Indexes:      fakeIndexCatalog{},
			AppCatalog:   catalog,
			WebUI:        testUI(),
			RouteTimeout: time.Millisecond,
		})
		response := postProto(
			t,
			handler,
			"/api/v1/system/bootstrap",
			&opensplunkv1.GetSystemBootstrapRequest{},
		)
		if response.Code != http.StatusRequestTimeout {
			t.Fatalf(
				"deadline catalog response = %d, %s",
				response.Code,
				response.Body,
			)
		}
		select {
		case <-contextCanceled:
		default:
			t.Fatal("catalog did not observe route cancellation")
		}
	})
}

func TestBootstrapAppCatalogTypedNilAndSourceConflict(t *testing.T) {
	staticApp := &opensplunkv1.AppSummary{
		AppId:       "static-app",
		Slug:        "static",
		DisplayName: "Static",
	}
	var typedNil *fakeBootstrapAppCatalog
	handler := newTestHandler(t, Config{
		SearchJobs: &fakeSearchJobs{},
		Indexes:    fakeIndexCatalog{},
		AppCatalog: typedNil,
		WebUI:      testUI(),
		Bootstrap: BootstrapConfig{
			Apps: []*opensplunkv1.AppSummary{staticApp},
		},
	})
	bootstrap := readBootstrap(
		t,
		handler,
		&opensplunkv1.GetSystemBootstrapRequest{},
	)
	if got := bootstrap.GetApps(); len(got) != 1 ||
		got[0].GetAppId() != "static-app" {
		t.Fatalf("typed-nil catalog disabled static compatibility = %+v", got)
	}

	_, err := NewHandler(Config{
		SearchJobs:    &fakeSearchJobs{},
		Indexes:       fakeIndexCatalog{},
		SavedSearches: &fakeSavedSearches{},
		AppCatalog: &fakeBootstrapAppCatalog{
			result: AppCatalogResult{Complete: true},
		},
		WebUI: testUI(),
		Bootstrap: BootstrapConfig{
			Apps: []*opensplunkv1.AppSummary{staticApp},
		},
	})
	if err == nil || !strings.Contains(
		err.Error(),
		"live app catalog and static bootstrap apps cannot both be configured",
	) {
		t.Fatalf("app bootstrap source conflict error = %v", err)
	}
}

func TestBootstrapAppCatalogDoesNotRequireAdministratorBearer(t *testing.T) {
	authenticator := &recordingBrowserAuthenticator{}
	catalog := &fakeBootstrapAppCatalog{
		result: AppCatalogResult{
			Complete: true,
			Apps: []AppCatalogSummary{
				validBootstrapCatalogApp(
					"app_alpha",
					"alpha",
					"Alpha",
				),
			},
		},
	}
	handler := newTestHandler(t, Config{
		SearchJobs:           &fakeSearchJobs{},
		Indexes:              fakeIndexCatalog{},
		AppAdmin:             &fakeAppAdministration{},
		AppCatalog:           catalog,
		BrowserAuthenticator: authenticator,
		AppCursorKey:         []byte(strings.Repeat("k", 32)),
		WebUI:                testUI(),
	})
	response := postProto(
		t,
		handler,
		"/api/v1/system/bootstrap",
		&opensplunkv1.GetSystemBootstrapRequest{},
	)
	if response.Code != http.StatusOK {
		t.Fatalf(
			"ordinary bootstrap status = %d, %s",
			response.Code,
			response.Body,
		)
	}
	if calls := authenticator.callCount(); calls != 0 {
		t.Fatalf("ordinary bootstrap authenticated %d times", calls)
	}
}

func validBootstrapCatalogApp(
	appID string,
	slug string,
	displayName string,
	defaultIndexes ...string,
) AppCatalogSummary {
	return AppCatalogSummary{
		AppID:             appID,
		Slug:              slug,
		DisplayName:       displayName,
		DefaultIndexNames: slices.Clone(defaultIndexes),
	}
}

func readBootstrap(
	t *testing.T,
	handler http.Handler,
	request *opensplunkv1.GetSystemBootstrapRequest,
) *opensplunkv1.GetSystemBootstrapResponse {
	t.Helper()
	response := postProto(
		t,
		handler,
		"/api/v1/system/bootstrap",
		request,
	)
	if response.Code != http.StatusOK {
		t.Fatalf(
			"bootstrap response = %d, %s",
			response.Code,
			response.Body,
		)
	}
	bootstrap := &opensplunkv1.GetSystemBootstrapResponse{}
	unmarshalResponse(t, response, bootstrap)
	return bootstrap
}

func leftPadDecimal(value int, width int) string {
	result := ""
	for value > 0 {
		result = string(rune('0'+value%10)) + result
		value /= 10
	}
	if result == "" {
		result = "0"
	}
	for len(result) < width {
		result = "0" + result
	}
	return result
}
