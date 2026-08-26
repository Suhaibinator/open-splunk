package server

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/dashboards"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type fakeDashboards struct {
	createFn func(context.Context, dashboards.AccessScope, *opensplunk.DashboardDefinition) (*opensplunk.Dashboard, error)
	getFn    func(context.Context, dashboards.AccessScope, string) (*opensplunk.Dashboard, error)
	listFn   func(context.Context, dashboards.AccessScope, *string) ([]*opensplunk.Dashboard, error)
	updateFn func(context.Context, dashboards.AccessScope, string, uint64, *opensplunk.DashboardDefinition) (*opensplunk.Dashboard, error)
	deleteFn func(context.Context, dashboards.AccessScope, string, uint64) error
}

func (fake *fakeDashboards) Create(ctx context.Context, scope dashboards.AccessScope, definition *opensplunk.DashboardDefinition) (*opensplunk.Dashboard, error) {
	if fake.createFn == nil {
		return nil, errors.New("unexpected dashboard create")
	}
	return fake.createFn(ctx, scope, definition)
}

func (fake *fakeDashboards) Get(ctx context.Context, scope dashboards.AccessScope, id string) (*opensplunk.Dashboard, error) {
	if fake.getFn == nil {
		return nil, errors.New("unexpected dashboard get")
	}
	return fake.getFn(ctx, scope, id)
}

func (fake *fakeDashboards) List(ctx context.Context, scope dashboards.AccessScope, appID *string) ([]*opensplunk.Dashboard, error) {
	if fake.listFn == nil {
		return nil, errors.New("unexpected dashboard list")
	}
	return fake.listFn(ctx, scope, appID)
}

func (fake *fakeDashboards) Update(ctx context.Context, scope dashboards.AccessScope, id string, version uint64, definition *opensplunk.DashboardDefinition) (*opensplunk.Dashboard, error) {
	if fake.updateFn == nil {
		return nil, errors.New("unexpected dashboard update")
	}
	return fake.updateFn(ctx, scope, id, version, definition)
}

func (fake *fakeDashboards) Delete(ctx context.Context, scope dashboards.AccessScope, id string, version uint64) error {
	if fake.deleteFn == nil {
		return errors.New("unexpected dashboard delete")
	}
	return fake.deleteFn(ctx, scope, id, version)
}

func dashboardAPITestRecord(ownerID, appID string) *opensplunk.Dashboard {
	earliest, latest, timezone := "-24h", "now", "UTC"
	return &opensplunk.Dashboard{
		DashboardId: "dash-1", Version: 1,
		Definition: &opensplunk.DashboardDefinition{
			Name: "Operations", AppId: appID, OwnerId: &ownerID,
			SharingScope: opensplunk.SharingScope_SHARING_SCOPE_PRIVATE,
			Panels: []*opensplunk.DashboardPanel{{
				PanelId: "panel-1", Title: "Errors", Width: 12, Height: 4,
				Search: &opensplunk.SearchDefinition{
					Spl:   "index=main level=ERROR | stats count by service",
					AppId: &appID, IndexScope: []string{"main"},
					TimeRange: &opensplunk.TimeRangeSpec{Earliest: &earliest, Latest: &latest, Timezone: &timezone},
				},
			}},
		},
		CreatedAt: timestamppb.New(testNow.Add(-time.Hour)), UpdatedAt: timestamppb.New(testNow.Add(-time.Minute)),
	}
}

func TestRunDashboardPanelUsesOnlyStoredDefinitionAndSealsProvenance(t *testing.T) {
	ownerID, tenantID, appID := "owner-1", "tenant-1", "app-main"
	record := dashboardAPITestRecord(ownerID, appID)
	store := &fakeDashboards{getFn: func(_ context.Context, scope dashboards.AccessScope, id string) (*opensplunk.Dashboard, error) {
		if scope.OwnerID != ownerID || id != record.GetDashboardId() {
			t.Fatalf("dashboard lookup = %+v %q", scope, id)
		}
		return record, nil
	}}
	created := completeJobForApp("job-dashboard", appID)
	created.OwnerID = ownerID
	created.TenantID = tenantID
	created.SPL = record.GetDefinition().GetPanels()[0].GetSearch().GetSpl()
	created.Source = searchjobs.JobSource{Origin: searchjobs.JobOriginDashboard, ObjectID: record.GetDashboardId()}
	jobs := &fakeSearchJobs{createJob: created}
	handler := newTestHandler(t, Config{
		SearchJobs: jobs, Dashboards: store,
		Indexes: fakeIndexCatalog{indexes: []control.Index{{
			ID: "idx-main", State: control.IndexStateActive,
			Definition: control.IndexDefinition{Name: "main", DisplayName: "Main", SearchEnabled: true},
		}}},
		OwnerID: ownerID, TenantID: tenantID, WebUI: testUI(), Now: func() time.Time { return testNow },
		Bootstrap: BootstrapConfig{Apps: []*opensplunk.AppSummary{{AppId: appID, Slug: "main", DisplayName: "Main", State: opensplunk.AppState_APP_STATE_ACTIVE}}},
	})

	response := postProto(t, handler, "/api/dashboards/panels/run", &opensplunk.RunDashboardPanelRequest{
		DashboardId: record.GetDashboardId(), PanelId: "panel-1",
	})
	if response.Code != http.StatusOK {
		t.Fatalf("run status = %d, body = %s", response.Code, response.Body.String())
	}
	if jobs.createCalls != 1 || jobs.createRequest.SPL != record.GetDefinition().GetPanels()[0].GetSearch().GetSpl() ||
		jobs.createRequest.AppID != appID || jobs.createRequest.Source.Origin != searchjobs.JobOriginDashboard ||
		jobs.createRequest.Source.ObjectID != record.GetDashboardId() || len(jobs.createRequest.RequestedIndexes) != 1 || jobs.createRequest.RequestedIndexes[0] != "main" {
		t.Fatalf("trusted create request = %+v", jobs.createRequest)
	}
	var decoded opensplunk.RunDashboardPanelResponse
	unmarshalResponse(t, response, &decoded)
	if decoded.GetSearchJob().GetSource().GetOrigin() != opensplunk.SearchJobOrigin_SEARCH_JOB_ORIGIN_DASHBOARD ||
		decoded.GetSearchJob().GetSource().GetDashboardId() != record.GetDashboardId() {
		t.Fatalf("dashboard provenance = %+v", decoded.GetSearchJob().GetSource())
	}
}

func TestRunDashboardPanelRejectsUnknownPanelBeforeCreatingJob(t *testing.T) {
	record := dashboardAPITestRecord("owner-1", "app-main")
	jobs := &fakeSearchJobs{createJob: completeJob("must-not-create")}
	handler := newTestHandler(t, Config{
		SearchJobs: jobs,
		Dashboards: &fakeDashboards{getFn: func(context.Context, dashboards.AccessScope, string) (*opensplunk.Dashboard, error) {
			return record, nil
		}},
		Indexes: fakeIndexCatalog{}, OwnerID: "owner-1", WebUI: testUI(),
	})
	response := postProto(t, handler, "/api/dashboards/panels/run", &opensplunk.RunDashboardPanelRequest{DashboardId: "dash-1", PanelId: "missing"})
	if response.Code != http.StatusNotFound || jobs.createCalls != 0 {
		t.Fatalf("unknown panel = status %d calls %d body %s", response.Code, jobs.createCalls, response.Body.String())
	}
}

func TestCreateDashboardRejectsUnavailablePanelIndexBeforePersistence(t *testing.T) {
	ownerID, appID := "owner-1", "app-main"
	handler := newTestHandler(t, Config{
		SearchJobs: &fakeSearchJobs{createJob: completeJob("unused")},
		Dashboards: &fakeDashboards{createFn: func(context.Context, dashboards.AccessScope, *opensplunk.DashboardDefinition) (*opensplunk.Dashboard, error) {
			t.Fatal("dashboard persistence was reached for an unavailable index")
			return nil, nil
		}},
		Indexes: fakeIndexCatalog{}, OwnerID: ownerID, WebUI: testUI(),
		Bootstrap: BootstrapConfig{Apps: []*opensplunk.AppSummary{{AppId: appID, Slug: "main", DisplayName: "Main", State: opensplunk.AppState_APP_STATE_ACTIVE}}},
	})
	definition := dashboardAPITestRecord(ownerID, appID).GetDefinition()
	response := postProto(t, handler, "/api/dashboards/create", &opensplunk.CreateDashboardRequest{Definition: definition})
	if response.Code != http.StatusForbidden {
		t.Fatalf("create status = %d, body = %s", response.Code, response.Body.String())
	}
}
