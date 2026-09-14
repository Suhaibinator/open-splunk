package server

import (
	"context"
	"net/http"
	"reflect"
	"testing"
	"time"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/dashboards"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
)

// Presentation never participates in admission: every visualization reaches
// the same backend with identical SPL, time, indexes, and trusted provenance.
func TestDashboardVisualizationPreservesSearchAdmissionAndRoundTrip(t *testing.T) {
	ownerID, tenantID, appID := "owner-1", "tenant-1", "app-main"
	record := dashboardAPITestRecord(ownerID)
	storedSearch := proto.Clone(record.Definition.Panels[0].Search).(*opensplunk.SearchDefinition)
	created := completeJobForApp("job-dashboard", appID)
	created.OwnerID, created.TenantID = ownerID, tenantID
	created.SPL = storedSearch.Spl
	created.Source = searchjobs.JobSource{Origin: searchjobs.JobOriginDashboard, ObjectID: record.DashboardId}
	jobs := &fakeSearchJobs{createJob: created}
	store := &fakeDashboards{
		getFn: func(context.Context, dashboards.AccessScope, string) (*opensplunk.Dashboard, error) {
			return proto.Clone(record).(*opensplunk.Dashboard), nil
		},
		updateFn: func(_ context.Context, _ dashboards.AccessScope, id string, version uint64, definition *opensplunk.DashboardDefinition) (*opensplunk.Dashboard, error) {
			if id != record.DashboardId || version != record.Version {
				t.Fatalf("update identity = %q v%d", id, version)
			}
			record.Definition = proto.Clone(definition).(*opensplunk.DashboardDefinition)
			record.Version++
			return proto.Clone(record).(*opensplunk.Dashboard), nil
		},
	}
	handler := newTestHandler(t, Config{
		SearchJobs: jobs, Dashboards: store,
		Indexes: fakeIndexCatalog{indexes: []control.Index{{
			ID: "idx-main", State: control.IndexStateActive,
			Definition: control.IndexDefinition{Name: "main", DisplayName: "Main", SearchEnabled: true},
		}}},
		OwnerID: ownerID, TenantID: tenantID, WebUI: testUI(), Now: func() time.Time { return testNow },
		Bootstrap: BootstrapConfig{Apps: []*opensplunk.AppSummary{{AppId: appID, Slug: "main", DisplayName: "Main", State: opensplunk.AppState_APP_STATE_ACTIVE}}},
	})

	var baseline *searchjobs.CreateRequest
	for _, visualType := range []opensplunk.VisualizationType{
		opensplunk.VisualizationType_VISUALIZATION_TYPE_UNSPECIFIED, // Legacy absent spec.
		opensplunk.VisualizationType_VISUALIZATION_TYPE_TABLE,
		opensplunk.VisualizationType_VISUALIZATION_TYPE_LINE,
		opensplunk.VisualizationType_VISUALIZATION_TYPE_AREA,
		opensplunk.VisualizationType_VISUALIZATION_TYPE_COLUMN,
		opensplunk.VisualizationType_VISUALIZATION_TYPE_BAR,
		opensplunk.VisualizationType_VISUALIZATION_TYPE_PIE,
		opensplunk.VisualizationType_VISUALIZATION_TYPE_SINGLE_VALUE,
		opensplunk.VisualizationType_VISUALIZATION_TYPE_SCATTER,
	} {
		t.Run(visualType.String(), func(t *testing.T) {
			definition := proto.Clone(record.Definition).(*opensplunk.DashboardDefinition)
			search := proto.Clone(storedSearch).(*opensplunk.SearchDefinition)
			if visualType != opensplunk.VisualizationType_VISUALIZATION_TYPE_UNSPECIFIED {
				search.Visualization = &opensplunk.VisualizationSpec{
					Type: visualType, Title: new("Exact display title"), XField: new("service"),
					YFields: []string{"p95", "count", "p50"}, SeriesField: new("region"),
					StackMode:  opensplunk.VisualizationStackMode_VISUALIZATION_STACK_MODE_STACKED_100_PERCENT,
					ShowLegend: true, ShowDataLabels: true,
					TimeBucketWidth: durationpb.New(1500 * time.Millisecond),
				}
			}
			definition.Panels[0].Search = search
			response := postProto(t, handler, "/api/dashboards/update", &opensplunk.UpdateDashboardRequest{
				DashboardId: record.DashboardId, ExpectedVersion: record.Version, Definition: definition,
			})
			if response.Code != http.StatusOK {
				t.Fatalf("update status = %d, body = %s", response.Code, response.Body.String())
			}
			var updated opensplunk.UpdateDashboardResponse
			unmarshalResponse(t, response, &updated)
			if !proto.Equal(updated.GetDashboard().GetDefinition().GetPanels()[0].GetSearch(), search) {
				t.Fatalf("visualization update changed the search or its presentation fields: got %v, want %v", updated.GetDashboard().GetDefinition().GetPanels()[0].GetSearch(), search)
			}
			response = postProto(t, handler, "/api/dashboards/panels/run", &opensplunk.RunDashboardPanelRequest{
				DashboardId: record.DashboardId, PanelId: "panel-1",
			})
			if response.Code != http.StatusOK {
				t.Fatalf("run status = %d, body = %s", response.Code, response.Body.String())
			}
			if baseline == nil {
				request := jobs.createRequest
				baseline = &request
			} else if !reflect.DeepEqual(*baseline, jobs.createRequest) {
				t.Fatalf("visualization changed search admission: got %+v, want %+v", jobs.createRequest, *baseline)
			}
			if !proto.Equal(record.Definition.Panels[0].Search, search) {
				t.Fatal("running a panel mutated its stored search")
			}
		})
	}
}
