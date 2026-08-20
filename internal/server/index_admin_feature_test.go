package server

import (
	"context"
	"net/http"
	"testing"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/auth"
	"github.com/Suhaibinator/open-splunk/internal/control"
)

func TestIndexAdministrationFeatureRequiresCompleteRouteFamily(t *testing.T) {
	t.Parallel()

	administration := &browserGateIndexAdministration{}
	statistics := &recordingIndexStatistics{}
	snapshotter := &recordingIndexStatisticsSnapshotter{}
	fields := &recordingIndexFields{
		maximumFields: 100,
		maximumPage:   10,
	}
	admission := indexDataDeletionAdmissionFunc(
		func(
			context.Context,
			control.IndexDataDeletionScope,
			string,
			uint64,
			string,
		) (control.IndexDeletionOperation, error) {
			return control.IndexDeletionOperation{}, control.ErrNotFound
		},
	)
	waker := &indexDataDeletionWakeRecorder{}
	authenticator := indexStatisticsAuthenticator(
		t,
		browserGateTenantID,
		auth.BrowserRoleAdministrator,
	)

	tests := []struct {
		name       string
		statistics bool
		fields     bool
		deletion   bool
		want       int
	}{
		{name: "administration only"},
		{name: "statistics only", statistics: true},
		{name: "fields only", fields: true},
		{name: "physical deletion only", deletion: true},
		{name: "statistics and fields", statistics: true, fields: true},
		{name: "statistics and physical deletion", statistics: true, deletion: true},
		{name: "fields and physical deletion", fields: true, deletion: true},
		{
			name:       "complete route family",
			statistics: true,
			fields:     true,
			deletion:   true,
			want:       1,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			config := Config{
				SearchJobs:           &fakeSearchJobs{},
				Indexes:              administration,
				IndexAdmin:           administration,
				SavedSearches:        &fakeSavedSearches{},
				BrowserAuthenticator: authenticator,
				WebUI:                testUI(),
				TenantID:             browserGateTenantID,
				OwnerID:              browserGateOwnerID,
				AdministrativeAllowedHosts: []string{
					"example.com",
				},
				Bootstrap: BootstrapConfig{
					Features: []opensplunk.ServerFeature{
						opensplunk.ServerFeature_SERVER_FEATURE_SEARCH,
						opensplunk.ServerFeature_SERVER_FEATURE_INDEX_ADMIN,
						opensplunk.ServerFeature_SERVER_FEATURE_INDEX_ADMIN,
					},
				},
			}
			if test.statistics {
				config.IndexStatistics = statistics
				config.IndexStatisticsSnapshotter = snapshotter
			}
			if test.fields {
				config.IndexFields = fields
			}
			if test.deletion {
				config.IndexDataDeletionAdmission = admission
				config.IndexDataDeletionWaker = waker
			}

			handler, err := NewHandler(config)
			if err != nil {
				t.Fatalf("NewHandler: %v", err)
			}
			response := postProto(
				t,
				handler,
				"/api/system/bootstrap",
				&opensplunk.GetSystemBootstrapRequest{},
			)
			if response.Code != http.StatusOK {
				t.Fatalf(
					"bootstrap status = %d, body = %s",
					response.Code,
					response.Body.String(),
				)
			}
			var bootstrap opensplunk.GetSystemBootstrapResponse
			unmarshalResponse(t, response, &bootstrap)
			if got := countServerFeature(
				bootstrap.GetFeatures(),
				opensplunk.ServerFeature_SERVER_FEATURE_INDEX_ADMIN,
			); got != test.want {
				t.Fatalf(
					"index administration feature count = %d, want %d; features = %v",
					got,
					test.want,
					bootstrap.GetFeatures(),
				)
			}
		})
	}
}
