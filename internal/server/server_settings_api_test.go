package server

import (
	"context"
	"errors"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/searchlimits"
)

type fakeServerSettings struct {
	mu      sync.Mutex
	current control.ServerSearchSettings
	err     error
}

func newFakeServerSettings() *fakeServerSettings {
	return &fakeServerSettings{current: control.ServerSearchSettings{Limits: searchlimits.Default()}}
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
	request := httptest.NewRequest("POST", "/api/server/settings/get", nil)
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
		httptest.NewRequest("POST", "/api/system/bootstrap", nil),
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
