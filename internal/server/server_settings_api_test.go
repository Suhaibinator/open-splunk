package server

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/searchlimits"
	"github.com/Suhaibinator/open-splunk/internal/uipalette"
)

type fakeServerSettings struct {
	mu         sync.Mutex
	current    control.ServerSearchSettings
	appearance control.ServerAppearanceSettings
	err        error
}

func newFakeServerSettings() *fakeServerSettings {
	return &fakeServerSettings{
		current:    control.ServerSearchSettings{Limits: searchlimits.Default()},
		appearance: control.ServerAppearanceSettings{Palette: uipalette.Default()},
	}
}

func (settings *fakeServerSettings) GetAppearance(context.Context) (control.ServerAppearanceSettings, error) {
	settings.mu.Lock()
	defer settings.mu.Unlock()
	return settings.appearance, settings.err
}

func (settings *fakeServerSettings) CurrentAppearance() control.ServerAppearanceSettings {
	settings.mu.Lock()
	defer settings.mu.Unlock()
	return settings.appearance
}

func (settings *fakeServerSettings) UpdateAppearance(
	_ context.Context,
	expectedVersion uint64,
	palette uipalette.Palette,
) (control.ServerAppearanceSettings, error) {
	settings.mu.Lock()
	defer settings.mu.Unlock()
	if settings.err != nil {
		return control.ServerAppearanceSettings{}, settings.err
	}
	if err := uipalette.Validate(palette); err != nil {
		return control.ServerAppearanceSettings{}, control.ErrInvalidArgument
	}
	if expectedVersion != settings.appearance.Version {
		return control.ServerAppearanceSettings{}, control.ErrVersionConflict
	}
	settings.appearance = control.ServerAppearanceSettings{
		Version: expectedVersion + 1, Palette: palette,
		UpdatedAt: time.Date(2026, 9, 4, 12, 0, 0, 0, time.UTC),
	}
	return settings.appearance, nil
}

func (settings *fakeServerSettings) Get(context.Context) (control.ServerSearchSettings, error) {
	settings.mu.Lock()
	defer settings.mu.Unlock()
	return settings.current, settings.err
}

func (settings *fakeServerSettings) Current() control.ServerSearchSettings {
	settings.mu.Lock()
	defer settings.mu.Unlock()
	return settings.current
}

func (settings *fakeServerSettings) Update(
	_ context.Context,
	expectedVersion uint64,
	limits searchlimits.Policy,
) (control.ServerSearchSettings, error) {
	settings.mu.Lock()
	defer settings.mu.Unlock()
	if settings.err != nil {
		return control.ServerSearchSettings{}, settings.err
	}
	if expectedVersion != settings.current.Version {
		return control.ServerSearchSettings{}, control.ErrVersionConflict
	}
	settings.current = control.ServerSearchSettings{
		Version: expectedVersion + 1, Limits: limits,
		UpdatedAt: time.Date(2026, 8, 29, 12, 0, 0, 0, time.UTC),
	}
	return settings.current, nil
}

func TestServerSettingsAPIEnvelopeUpdateConflictAndValidation(t *testing.T) {
	t.Parallel()
	settings := newFakeServerSettings()
	handler := &apiHandler{serverSettings: settings}
	request := httptest.NewRequestWithContext(context.Background(), "POST", "/api/server/settings/get", nil)
	get, err := handler.getServerSettings(request, &opensplunk.GetServerSettingsRequest{})
	if err != nil || get.GetCurrent().GetVersion() != 0 ||
		get.GetCurrent().GetLimits().GetMaximumRuntime().AsDuration() != 2*time.Minute ||
		get.GetMinimums().GetMaximumRuntime().AsDuration() != 10*time.Second ||
		get.GetMaximums().GetMaximumRuntime().AsDuration() != 24*time.Hour {
		t.Fatalf("getServerSettings() = (%+v, %v)", get, err)
	}
	updatedLimits := searchlimits.Default()
	updatedLimits.MaxRuntime = 4 * time.Minute
	updated, err := handler.updateServerSettings(request, &opensplunk.UpdateServerSettingsRequest{
		ExpectedVersion: 0,
		Limits:          searchLimitsToProto(updatedLimits),
	})
	if err != nil || updated.GetCurrent().GetVersion() != 1 ||
		updated.GetCurrent().GetLimits().GetMaximumRuntime().AsDuration() != 4*time.Minute {
		t.Fatalf("updateServerSettings() = (%+v, %v)", updated, err)
	}
	if _, err := handler.updateServerSettings(request, &opensplunk.UpdateServerSettingsRequest{
		ExpectedVersion: 0, Limits: searchLimitsToProto(updatedLimits),
	}); err == nil {
		t.Fatal("stale update succeeded")
	}
	invalid := searchLimitsToProto(searchlimits.Default())
	invalid.MaximumTotalResultBytes = invalid.MaximumResultBytes - 1
	if _, err := handler.updateServerSettings(request, &opensplunk.UpdateServerSettingsRequest{Limits: invalid}); err == nil {
		t.Fatal("relationship-invalid update succeeded")
	}
	settings.err = errors.New("storage unavailable")
	if _, err := handler.getServerSettings(request, &opensplunk.GetServerSettingsRequest{}); err == nil {
		t.Fatal("storage failure was not returned")
	}
}

func TestBootstrapUsesLiveServerSettingsTimeoutAndRetention(t *testing.T) {
	t.Parallel()
	settings := newFakeServerSettings()
	settings.current.Limits.MaxRuntime = 9 * time.Minute
	settings.current.Limits.ResultRetention = 2 * time.Hour
	handler := &apiHandler{
		indexes: fakeIndexCatalog{}, serverSettings: settings,
		bootstrap:       BootstrapConfig{DefaultSearchTimeout: time.Second, SearchResultRetention: time.Minute},
		maximumPageSize: 100, now: time.Now,
	}
	response, err := handler.getSystemBootstrap(
		httptest.NewRequestWithContext(context.Background(), "POST", "/api/system/bootstrap", nil),
		&opensplunk.GetSystemBootstrapRequest{},
	)
	if err != nil {
		t.Fatal(err)
	}
	if response.GetLimits().GetDefaultSearchTimeout().AsDuration() != 9*time.Minute ||
		response.GetLimits().GetSearchResultRetention().AsDuration() != 2*time.Hour {
		t.Fatalf("bootstrap limits = %+v", response.GetLimits())
	}
}

func TestServerAppearanceAPIEnvelopeUpdateConflictAndValidation(t *testing.T) {
	t.Parallel()
	settings := newFakeServerSettings()
	handler := &apiHandler{serverSettings: settings}
	request := httptest.NewRequestWithContext(context.Background(), "POST", "/api/server/appearance/get", nil)
	get, err := handler.getServerAppearance(request, &opensplunk.GetServerAppearanceRequest{})
	if err != nil || get.GetCurrent().GetVersion() != 0 ||
		get.GetCurrent().GetPalette() != opensplunk.UiPalette_UI_PALETTE_CLASSIC ||
		get.GetCurrent().UpdatedAt != nil ||
		get.GetDefaultPalette() != opensplunk.UiPalette_UI_PALETTE_CLASSIC {
		t.Fatalf("getServerAppearance() = (%+v, %v)", get, err)
	}
	updated, err := handler.updateServerAppearance(request, &opensplunk.UpdateServerAppearanceRequest{
		ExpectedVersion: 0, Palette: opensplunk.UiPalette_UI_PALETTE_OCEAN,
	})
	if err != nil || updated.GetCurrent().GetVersion() != 1 ||
		updated.GetCurrent().GetPalette() != opensplunk.UiPalette_UI_PALETTE_OCEAN ||
		updated.GetCurrent().GetUpdatedAt().AsTime() != time.Date(2026, 9, 4, 12, 0, 0, 0, time.UTC) ||
		updated.GetDefaultPalette() != opensplunk.UiPalette_UI_PALETTE_CLASSIC {
		t.Fatalf("updateServerAppearance() = (%+v, %v)", updated, err)
	}
	if settings.CurrentAppearance().Palette != uipalette.Ocean {
		t.Fatalf("live appearance = %+v", settings.CurrentAppearance())
	}
	_, err = handler.updateServerAppearance(request, &opensplunk.UpdateServerAppearanceRequest{
		ExpectedVersion: 0, Palette: opensplunk.UiPalette_UI_PALETTE_EMBER,
	})
	assertHTTPErrorStatus(t, err, http.StatusConflict)
	for _, palette := range []opensplunk.UiPalette{opensplunk.UiPalette_UI_PALETTE_UNSPECIFIED, opensplunk.UiPalette(99)} {
		_, err = handler.updateServerAppearance(request, &opensplunk.UpdateServerAppearanceRequest{
			ExpectedVersion: 1, Palette: palette,
		})
		assertHTTPErrorStatus(t, err, http.StatusBadRequest)
	}
	if settings.CurrentAppearance().Version != 1 || settings.CurrentAppearance().Palette != uipalette.Ocean {
		t.Fatalf("rejected updates changed appearance: %+v", settings.CurrentAppearance())
	}
	settings.err = control.ErrInvalidArgument
	_, err = handler.updateServerAppearance(request, &opensplunk.UpdateServerAppearanceRequest{
		ExpectedVersion: 1, Palette: opensplunk.UiPalette_UI_PALETTE_GLASS,
	})
	assertHTTPErrorStatus(t, err, http.StatusBadRequest)
	settings.err = errors.New("storage unavailable")
	_, err = handler.updateServerAppearance(request, &opensplunk.UpdateServerAppearanceRequest{
		ExpectedVersion: 1, Palette: opensplunk.UiPalette_UI_PALETTE_GLASS,
	})
	assertHTTPErrorStatus(t, err, http.StatusServiceUnavailable)
	_, err = handler.getServerAppearance(request, &opensplunk.GetServerAppearanceRequest{})
	assertHTTPErrorStatus(t, err, http.StatusServiceUnavailable)
}

func TestUiPaletteProtoMappingIsExactAndRejectsUnlistedValues(t *testing.T) {
	t.Parallel()
	wire := map[uipalette.Palette]opensplunk.UiPalette{
		uipalette.Classic:  opensplunk.UiPalette_UI_PALETTE_CLASSIC,
		uipalette.Ocean:    opensplunk.UiPalette_UI_PALETTE_OCEAN,
		uipalette.Ember:    opensplunk.UiPalette_UI_PALETTE_EMBER,
		uipalette.Graphite: opensplunk.UiPalette_UI_PALETTE_GRAPHITE,
		uipalette.Glass:    opensplunk.UiPalette_UI_PALETTE_GLASS,
		uipalette.Terminal: opensplunk.UiPalette_UI_PALETTE_TERMINAL,
	}
	if len(wire) != len(uipalette.All()) {
		t.Fatalf("mapping covers %d palettes, enum lists %d", len(wire), len(uipalette.All()))
	}
	for _, palette := range uipalette.All() {
		got := uiPaletteToProto(palette)
		if got != wire[palette] {
			t.Fatalf("uiPaletteToProto(%q) = %v, want %v", palette, got, wire[palette])
		}
		back, err := uiPaletteFromProto(got)
		if err != nil || back != palette {
			t.Fatalf("uiPaletteFromProto(%v) = (%q, %v), want %q", got, back, err, palette)
		}
	}
	if got := uiPaletteToProto(uipalette.Palette("sepia")); got != opensplunk.UiPalette_UI_PALETTE_UNSPECIFIED {
		t.Fatalf("uiPaletteToProto(sepia) = %v, want UNSPECIFIED", got)
	}
	for _, value := range []opensplunk.UiPalette{opensplunk.UiPalette_UI_PALETTE_UNSPECIFIED, 7, 99, -1} {
		if palette, err := uiPaletteFromProto(value); err == nil {
			t.Fatalf("uiPaletteFromProto(%d) = %q, want an error", value, palette)
		}
	}
}

func TestBootstrapCarriesLiveUiPalette(t *testing.T) {
	t.Parallel()
	settings := newFakeServerSettings()
	settings.appearance = control.ServerAppearanceSettings{Version: 3, Palette: uipalette.Terminal}
	handler := &apiHandler{
		indexes: fakeIndexCatalog{}, serverSettings: settings,
		bootstrap:       BootstrapConfig{DefaultSearchTimeout: time.Second, SearchResultRetention: time.Minute},
		maximumPageSize: 100, now: time.Now,
	}
	bootstrapRequest := func() *http.Request {
		return httptest.NewRequestWithContext(context.Background(), "POST", "/api/system/bootstrap", nil)
	}
	response, err := handler.getSystemBootstrap(bootstrapRequest(), &opensplunk.GetSystemBootstrapRequest{})
	if err != nil || response.GetUiPalette() != opensplunk.UiPalette_UI_PALETTE_TERMINAL {
		t.Fatalf("bootstrap palette = (%v, %v), want TERMINAL", response.GetUiPalette(), err)
	}
	if _, err := settings.UpdateAppearance(context.Background(), 3, uipalette.Graphite); err != nil {
		t.Fatal(err)
	}
	response, err = handler.getSystemBootstrap(bootstrapRequest(), &opensplunk.GetSystemBootstrapRequest{})
	if err != nil || response.GetUiPalette() != opensplunk.UiPalette_UI_PALETTE_GRAPHITE {
		t.Fatalf("bootstrap palette after update = (%v, %v), want GRAPHITE", response.GetUiPalette(), err)
	}
	withoutSettings := &apiHandler{
		indexes: fakeIndexCatalog{},
		bootstrap: BootstrapConfig{
			DefaultSearchTimeout: time.Second, SearchResultRetention: time.Minute,
		},
		maximumPageSize: 100, now: time.Now,
	}
	response, err = withoutSettings.getSystemBootstrap(bootstrapRequest(), &opensplunk.GetSystemBootstrapRequest{})
	if err != nil || response.GetUiPalette() != opensplunk.UiPalette_UI_PALETTE_UNSPECIFIED {
		t.Fatalf("bootstrap palette without settings = (%v, %v), want UNSPECIFIED", response.GetUiPalette(), err)
	}
}

func TestServerSettingsCapabilityTracksCompleteService(t *testing.T) {
	t.Parallel()
	feature := opensplunk.ServerFeature_SERVER_FEATURE_SERVER_SETTINGS_ADMIN
	available := featuresForServices(nil, serviceCapabilities{serverSettings: true})
	if !containsFeature(available, feature) {
		t.Fatal("configured server-settings service did not advertise its capability")
	}
	unavailable := featuresForServices([]opensplunk.ServerFeature{feature}, serviceCapabilities{})
	if containsFeature(unavailable, feature) {
		t.Fatal("incomplete server-settings service retained a caller-supplied capability")
	}
}
