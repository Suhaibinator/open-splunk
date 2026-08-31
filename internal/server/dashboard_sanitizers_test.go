package server

import (
	"errors"
	"net/http"
	"strings"
	"testing"

	"github.com/Suhaibinator/SRouter/pkg/router"
	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
)

const dashboardSanitizerOwnerID = "owner-dashboard-sanitizer"

func dashboardSanitizerHandler() *apiHandler {
	return &apiHandler{ownerID: dashboardSanitizerOwnerID}
}

func assertDashboardSanitizerRejection(t *testing.T, err error, message string) {
	t.Helper()
	var httpErr *router.HTTPError
	if !errors.As(err, &httpErr) ||
		httpErr.StatusCode != http.StatusBadRequest ||
		httpErr.Message != message {
		t.Fatalf("error = %T %v, want bad request %q", err, err, message)
	}
}

func dashboardSanitizerDefinition() *opensplunk.DashboardDefinition {
	earliest, latest := "-24h", "now"
	return &opensplunk.DashboardDefinition{
		Name:  "Operations",
		AppId: "app-main",
		Panels: []*opensplunk.DashboardPanel{{
			PanelId: "panel-1",
			Search: &opensplunk.SearchDefinition{
				Spl:       "index=main | stats count",
				TimeRange: &opensplunk.TimeRangeSpec{Earliest: &earliest, Latest: &latest},
			},
		}},
	}
}

func TestSanitizeGetDashboardRequestBoundsTheIdentifier(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		id      string
		message string
	}{
		"canonical":      {id: "dash-1"},
		"empty":          {id: "", message: "dashboard ID is invalid"},
		"leading space":  {id: " dash-1", message: "dashboard ID is invalid"},
		"trailing space": {id: "dash-1 ", message: "dashboard ID is invalid"},
		"control byte":   {id: "dash\n1", message: "dashboard ID is invalid"},
		"oversized": {
			id:      strings.Repeat("d", 129),
			message: "dashboard ID is invalid",
		},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			request := &opensplunk.GetDashboardRequest{DashboardId: test.id}
			got, err := sanitizeGetDashboardRequest(t.Context(), request)
			if got != request {
				t.Fatalf("sanitizer returned %p, want %p", got, request)
			}
			if test.message != "" {
				assertDashboardSanitizerRejection(t, err, test.message)
				return
			}
			if err != nil {
				t.Fatalf("sanitize = %v", err)
			}
			if got.GetDashboardId() != test.id {
				t.Fatalf("dashboard ID = %q, want %q", got.GetDashboardId(), test.id)
			}
		})
	}
}

func TestSanitizeGetDashboardRequestDiscardsUnknownFields(t *testing.T) {
	t.Parallel()

	request := &opensplunk.GetDashboardRequest{DashboardId: "dash-1"}
	request.ProtoReflect().SetUnknown(futureProtobufField("future-dashboard"))
	got, err := sanitizeGetDashboardRequest(t.Context(), request)
	if err != nil {
		t.Fatalf("sanitize = %v", err)
	}
	if len(got.ProtoReflect().GetUnknown()) != 0 {
		t.Fatalf("unknown fields survived: %x", got.ProtoReflect().GetUnknown())
	}
}

func TestSanitizeListDashboardsRequestBoundsTheAppFilter(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		filter  *string
		message string
	}{
		"absent":     {filter: nil},
		"canonical":  {filter: new("app-main")},
		"empty":      {filter: new(""), message: "dashboard app ID filter is invalid"},
		"untrimmed":  {filter: new(" app-main "), message: "dashboard app ID filter is invalid"},
		"control":    {filter: new("app\tmain"), message: "dashboard app ID filter is invalid"},
		"oversized":  {filter: new(strings.Repeat("a", maximumSavedSearchAppIDBytes+1)), message: "dashboard app ID filter is invalid"},
		"whitespace": {filter: new("   "), message: "dashboard app ID filter is invalid"},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			request := &opensplunk.ListDashboardsRequest{AppIdFilter: test.filter}
			got, err := sanitizeListDashboardsRequest(t.Context(), request)
			if test.message != "" {
				assertDashboardSanitizerRejection(t, err, test.message)
				return
			}
			if err != nil {
				t.Fatalf("sanitize = %v", err)
			}
			if test.filter == nil {
				if got.AppIdFilter != nil {
					t.Fatalf("app filter = %q, want absent", got.GetAppIdFilter())
				}
				return
			}
			if got.GetAppIdFilter() != *test.filter {
				t.Fatalf("app filter = %q, want %q", got.GetAppIdFilter(), *test.filter)
			}
		})
	}
}

func TestSanitizeCreateDashboardRequestBoundsTheDefinition(t *testing.T) {
	t.Parallel()

	otherOwner := "someone-else"
	ownDefinition := dashboardSanitizerDefinition()
	ownDefinition.OwnerId = new(dashboardSanitizerOwnerID)
	blankOwner := dashboardSanitizerDefinition()
	blankOwner.OwnerId = new("")
	foreignOwner := dashboardSanitizerDefinition()
	foreignOwner.OwnerId = &otherOwner
	oversized := dashboardSanitizerDefinition()
	oversized.Name = strings.Repeat("n", maximumDashboardDefinitionBytes)
	missingApp := dashboardSanitizerDefinition()
	missingApp.AppId = ""
	untrimmedApp := dashboardSanitizerDefinition()
	untrimmedApp.AppId = " app-main "
	panelWithoutSearch := dashboardSanitizerDefinition()
	panelWithoutSearch.Panels[0].Search = nil
	nilPanel := dashboardSanitizerDefinition()
	nilPanel.Panels = append(nilPanel.Panels, nil)

	tests := map[string]struct {
		definition *opensplunk.DashboardDefinition
		message    string
	}{
		"canonical":       {definition: dashboardSanitizerDefinition()},
		"own owner":       {definition: ownDefinition},
		"blank owner":     {definition: blankOwner},
		"absent":          {message: "dashboard definition is required"},
		"oversized":       {definition: oversized, message: "dashboard definition is too large"},
		"foreign owner":   {definition: foreignOwner, message: "dashboard owner must match the authenticated owner"},
		"missing app":     {definition: missingApp, message: "dashboard app ID is invalid"},
		"untrimmed app":   {definition: untrimmedApp, message: "dashboard app ID is invalid"},
		"panel no search": {definition: panelWithoutSearch, message: "every dashboard panel requires a search definition"},
		"nil panel":       {definition: nilPanel, message: "every dashboard panel requires a search definition"},
	}
	handler := dashboardSanitizerHandler()
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			request := &opensplunk.CreateDashboardRequest{Definition: test.definition}
			got, err := handler.sanitizeCreateDashboardRequest(t.Context(), request)
			if got != request {
				t.Fatalf("sanitizer returned %p, want %p", got, request)
			}
			if test.message != "" {
				assertDashboardSanitizerRejection(t, err, test.message)
				return
			}
			if err != nil {
				t.Fatalf("sanitize = %v", err)
			}
		})
	}
}

func TestSanitizeCreateDashboardRequestGivesEveryPanelTheDashboardApp(t *testing.T) {
	t.Parallel()

	definition := dashboardSanitizerDefinition()
	definition.Panels = append(
		definition.Panels,
		&opensplunk.DashboardPanel{
			PanelId: "panel-2",
			Search: &opensplunk.SearchDefinition{
				Spl:   "index=main | head 1",
				AppId: new("   "),
			},
		},
		&opensplunk.DashboardPanel{
			PanelId: "panel-3",
			Search: &opensplunk.SearchDefinition{
				Spl:   "index=main | head 2",
				AppId: new("app-other"),
			},
		},
	)
	request := &opensplunk.CreateDashboardRequest{Definition: definition}
	got, err := dashboardSanitizerHandler().sanitizeCreateDashboardRequest(
		t.Context(),
		request,
	)
	if err != nil {
		t.Fatalf("sanitize = %v", err)
	}
	panels := got.GetDefinition().GetPanels()
	if panels[0].GetSearch().GetAppId() != "app-main" ||
		panels[1].GetSearch().GetAppId() != "app-main" ||
		panels[2].GetSearch().GetAppId() != "app-other" {
		t.Fatalf(
			"panel app IDs = %q, %q, %q",
			panels[0].GetSearch().GetAppId(),
			panels[1].GetSearch().GetAppId(),
			panels[2].GetSearch().GetAppId(),
		)
	}
}

func TestSanitizeUpdateDashboardRequestBoundsIdentityAndVersion(t *testing.T) {
	t.Parallel()

	handler := dashboardSanitizerHandler()
	tests := map[string]struct {
		request *opensplunk.UpdateDashboardRequest
		message string
	}{
		"canonical": {request: &opensplunk.UpdateDashboardRequest{
			DashboardId:     "dash-1",
			ExpectedVersion: 3,
			Definition:      dashboardSanitizerDefinition(),
		}},
		"invalid ID": {
			request: &opensplunk.UpdateDashboardRequest{
				DashboardId:     " dash-1",
				ExpectedVersion: 3,
				Definition:      dashboardSanitizerDefinition(),
			},
			message: "dashboard ID is invalid",
		},
		"zero version": {
			request: &opensplunk.UpdateDashboardRequest{
				DashboardId: "dash-1",
				Definition:  dashboardSanitizerDefinition(),
			},
			message: "expected version is invalid",
		},
		"absent definition": {
			request: &opensplunk.UpdateDashboardRequest{
				DashboardId:     "dash-1",
				ExpectedVersion: 3,
			},
			message: "dashboard definition is required",
		},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			got, err := handler.sanitizeUpdateDashboardRequest(t.Context(), test.request)
			if got != test.request {
				t.Fatalf("sanitizer returned %p, want %p", got, test.request)
			}
			if test.message != "" {
				assertDashboardSanitizerRejection(t, err, test.message)
				return
			}
			if err != nil {
				t.Fatalf("sanitize = %v", err)
			}
		})
	}
}

func TestSanitizeDeleteDashboardRequestBoundsIdentityAndVersion(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		request *opensplunk.DeleteDashboardRequest
		message string
	}{
		"canonical": {request: &opensplunk.DeleteDashboardRequest{
			DashboardId:     "dash-1",
			ExpectedVersion: 1,
		}},
		"invalid ID": {
			request: &opensplunk.DeleteDashboardRequest{
				DashboardId:     "",
				ExpectedVersion: 1,
			},
			message: "dashboard ID is invalid",
		},
		"zero version": {
			request: &opensplunk.DeleteDashboardRequest{DashboardId: "dash-1"},
			message: "expected version is invalid",
		},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			_, err := sanitizeDeleteDashboardRequest(t.Context(), test.request)
			if test.message != "" {
				assertDashboardSanitizerRejection(t, err, test.message)
				return
			}
			if err != nil {
				t.Fatalf("sanitize = %v", err)
			}
		})
	}
}

func TestSanitizeRunDashboardPanelRequestBoundsBothIdentifiers(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		dashboardID string
		panelID     string
		message     string
	}{
		"canonical":       {dashboardID: "dash-1", panelID: "panel-1"},
		"invalid board":   {dashboardID: " dash-1", panelID: "panel-1", message: "dashboard ID is invalid"},
		"missing panel":   {dashboardID: "dash-1", message: "dashboard panel ID is invalid"},
		"untrimmed panel": {dashboardID: "dash-1", panelID: "panel-1 ", message: "dashboard panel ID is invalid"},
		"oversized panel": {
			dashboardID: "dash-1",
			panelID:     strings.Repeat("p", 129),
			message:     "dashboard panel ID is invalid",
		},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			request := &opensplunk.RunDashboardPanelRequest{
				DashboardId: test.dashboardID,
				PanelId:     test.panelID,
			}
			got, err := sanitizeRunDashboardPanelRequest(t.Context(), request)
			if test.message != "" {
				assertDashboardSanitizerRejection(t, err, test.message)
				return
			}
			if err != nil {
				t.Fatalf("sanitize = %v", err)
			}
			if got.GetDashboardId() != test.dashboardID ||
				got.GetPanelId() != test.panelID {
				t.Fatalf(
					"identifiers = %q/%q",
					got.GetDashboardId(),
					got.GetPanelId(),
				)
			}
		})
	}
}
