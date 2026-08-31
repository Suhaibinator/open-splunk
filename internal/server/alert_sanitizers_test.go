package server

import (
	"strings"
	"testing"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/fieldmaskpb"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/alerts"
)

func TestSanitizeAlertIdentifierRoutesTrimAndRequireTheAlertID(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		sanitize func(string) (string, error)
	}{
		{name: "get", sanitize: func(id string) (string, error) {
			request, err := sanitizeGetAlertRequest(t.Context(), &opensplunk.GetAlertRequest{AlertId: id})
			return request.GetAlertId(), err
		}},
		{name: "set enabled", sanitize: func(id string) (string, error) {
			request, err := sanitizeSetAlertEnabledRequest(t.Context(), &opensplunk.SetAlertEnabledRequest{AlertId: id})
			return request.GetAlertId(), err
		}},
		{name: "delete", sanitize: func(id string) (string, error) {
			request, err := sanitizeDeleteAlertRequest(t.Context(), &opensplunk.DeleteAlertRequest{AlertId: id})
			return request.GetAlertId(), err
		}},
		{name: "run", sanitize: func(id string) (string, error) {
			request, err := sanitizeRunAlertRequest(t.Context(), &opensplunk.RunAlertRequest{AlertId: id})
			return request.GetAlertId(), err
		}},
		{name: "webhook test", sanitize: func(id string) (string, error) {
			request, err := sanitizeTestAlertWebhookRequest(t.Context(), &opensplunk.TestAlertWebhookRequest{AlertId: id})
			return request.GetAlertId(), err
		}},
		{name: "secret rotate", sanitize: func(id string) (string, error) {
			request, err := sanitizeRotateAlertSecretRequest(t.Context(), &opensplunk.RotateAlertSecretRequest{AlertId: id})
			return request.GetAlertId(), err
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			id, err := test.sanitize(" alert-1\t")
			if err != nil || id != "alert-1" {
				t.Fatalf("sanitized alert ID = %q, %v", id, err)
			}
			for _, blank := range []string{"", "   "} {
				if _, err := test.sanitize(blank); err == nil || !strings.Contains(err.Error(), "alert ID is required") {
					t.Fatalf("sanitize(%q) error = %v, want an alert ID requirement", blank, err)
				}
			}
		})
	}
}

func TestSanitizeListAlertsRequestNormalizesFiltersAndBoundsThePage(t *testing.T) {
	t.Parallel()
	handler := &apiHandler{maximumPageSize: 25}

	sanitized, err := handler.sanitizeListAlertsRequest(t.Context(), &opensplunk.ListAlertsRequest{
		AppIdFilter: new("  search "), TextFilter: new("  Database Errors  "),
	})
	if err != nil {
		t.Fatalf("sanitizeListAlertsRequest() error = %v", err)
	}
	if sanitized.GetAppIdFilter() != "search" || sanitized.GetTextFilter() != "database errors" {
		t.Fatalf("normalized filters = %q, %q", sanitized.GetAppIdFilter(), sanitized.GetTextFilter())
	}
	if sanitized.GetPage().GetPageSize() != 25 || sanitized.GetPage().PageToken != nil || sanitized.GetPage().GetIncludeTotalSize() {
		t.Fatalf("resolved page = %+v", sanitized.GetPage())
	}

	blank, err := handler.sanitizeListAlertsRequest(t.Context(), &opensplunk.ListAlertsRequest{
		AppIdFilter: new("   "), TextFilter: new(""),
	})
	if err != nil || blank.AppIdFilter != nil || blank.TextFilter != nil {
		t.Fatalf("blank filters = %+v, %v", blank, err)
	}

	for _, test := range []struct {
		name    string
		request *opensplunk.ListAlertsRequest
	}{
		{name: "app filter", request: &opensplunk.ListAlertsRequest{
			AppIdFilter: new(strings.Repeat("a", maximumAlertAppFilterBytes+1)),
		}},
		{name: "text filter", request: &opensplunk.ListAlertsRequest{
			TextFilter: new(strings.Repeat("t", maximumAlertTextFilterBytes+1)),
		}},
	} {
		if _, err := handler.sanitizeListAlertsRequest(t.Context(), test.request); err == nil ||
			!strings.Contains(err.Error(), "alert filters are too long") {
			t.Fatalf("oversized %s error = %v", test.name, err)
		}
	}

	atBound, err := handler.sanitizeListAlertsRequest(t.Context(), &opensplunk.ListAlertsRequest{
		AppIdFilter: new(strings.Repeat("a", maximumAlertAppFilterBytes)),
		TextFilter:  new(strings.Repeat("t", maximumAlertTextFilterBytes)),
	})
	if err != nil || len(atBound.GetAppIdFilter()) != maximumAlertAppFilterBytes {
		t.Fatalf("filters at the bound = %v", err)
	}

	oversize := uint32(26)
	if _, err := handler.sanitizeListAlertsRequest(t.Context(), &opensplunk.ListAlertsRequest{
		Page: &opensplunk.PageRequest{PageSize: &oversize},
	}); err == nil || !strings.Contains(err.Error(), "alert page size is invalid") {
		t.Fatalf("oversized page size error = %v", err)
	}
}

func TestSanitizeListAlertRunsRequestResolvesAStablePage(t *testing.T) {
	t.Parallel()
	handler := &apiHandler{maximumPageSize: 1_000}

	sanitized, err := handler.sanitizeListAlertRunsRequest(t.Context(), &opensplunk.ListAlertRunsRequest{
		AlertId: " alert-1 ", Page: &opensplunk.PageRequest{IncludeTotalSize: true},
	})
	if err != nil {
		t.Fatalf("sanitizeListAlertRunsRequest() error = %v", err)
	}
	if sanitized.GetAlertId() != "alert-1" || sanitized.GetPage().GetPageSize() != defaultAlertPageSize ||
		!sanitized.GetPage().GetIncludeTotalSize() {
		t.Fatalf("sanitized run list = %+v", sanitized)
	}

	// A second pass must not reject the page it just resolved.
	again, err := handler.sanitizeListAlertRunsRequest(t.Context(), sanitized)
	if err != nil || again.GetPage().GetPageSize() != defaultAlertPageSize {
		t.Fatalf("re-sanitized run list = %+v, %v", again, err)
	}

	if _, err := handler.sanitizeListAlertRunsRequest(t.Context(), &opensplunk.ListAlertRunsRequest{}); err == nil ||
		!strings.Contains(err.Error(), "alert ID is required") {
		t.Fatalf("missing alert ID error = %v", err)
	}

	serviceBound := uint32(alerts.MaximumRunHistory + 1)
	if _, err := handler.sanitizeListAlertRunsRequest(t.Context(), &opensplunk.ListAlertRunsRequest{
		AlertId: "alert-1", Page: &opensplunk.PageRequest{PageSize: &serviceBound},
	}); err == nil || !strings.Contains(err.Error(), "alert run page size is invalid") {
		t.Fatalf("page size above the run history bound = %v", err)
	}
}

func TestSanitizeUpdateAlertRequestRejectsPartialUpdates(t *testing.T) {
	t.Parallel()
	_, err := sanitizeUpdateAlertRequest(t.Context(), &opensplunk.UpdateAlertRequest{
		AlertId:    "alert-1",
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"definition.name"}},
		Definition: alertAPITestDefinition("Errors", "https://hooks.example.com/alerts"),
	})
	if err == nil || !strings.Contains(err.Error(), "partial alert updates are not supported") {
		t.Fatalf("update mask error = %v", err)
	}

	sanitized, err := sanitizeUpdateAlertRequest(t.Context(), &opensplunk.UpdateAlertRequest{
		AlertId:    " alert-1 ",
		UpdateMask: &fieldmaskpb.FieldMask{},
		Definition: alertAPITestDefinition("Errors", "https://hooks.example.com/alerts"),
	})
	if err != nil || sanitized.GetAlertId() != "alert-1" {
		t.Fatalf("sanitizeUpdateAlertRequest() = %+v, %v", sanitized, err)
	}
}

func TestSanitizeCreateAlertRequestRequiresAWebhookURL(t *testing.T) {
	t.Parallel()
	withoutURL := alertAPITestDefinition("Errors", "https://hooks.example.com/alerts")
	withoutURL.Webhook.Url = nil
	if _, err := sanitizeCreateAlertRequest(t.Context(), &opensplunk.CreateAlertRequest{Definition: withoutURL}); err == nil ||
		!strings.Contains(err.Error(), "webhook URL is required") {
		t.Fatalf("absent webhook URL error = %v", err)
	}
	// An update reuses the stored URL, so the same definition is acceptable there.
	if _, err := sanitizeUpdateAlertRequest(t.Context(), &opensplunk.UpdateAlertRequest{
		AlertId: "alert-1", Definition: withoutURL,
	}); err != nil {
		t.Fatalf("sanitizeUpdateAlertRequest() rejected an absent webhook URL: %v", err)
	}
	if _, err := sanitizeCreateAlertRequest(t.Context(), &opensplunk.CreateAlertRequest{
		Definition: alertAPITestDefinition("Errors", "https://hooks.example.com/alerts"),
	}); err != nil {
		t.Fatalf("sanitizeCreateAlertRequest() rejected a complete definition: %v", err)
	}
}

func TestSanitizeAlertDefinitionRejectsEveryUnconvertibleShape(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		mutate    func(*opensplunk.AlertDefinition)
		wantError string
	}{
		{name: "absent search", mutate: func(definition *opensplunk.AlertDefinition) {
			definition.Search = nil
		}, wantError: "complete alert definition is required"},
		{name: "absent time range", mutate: func(definition *opensplunk.AlertDefinition) {
			definition.Search.TimeRange = nil
		}, wantError: "complete alert definition is required"},
		{name: "absent condition", mutate: func(definition *opensplunk.AlertDefinition) {
			definition.Condition = nil
		}, wantError: "complete alert definition is required"},
		{name: "absent webhook", mutate: func(definition *opensplunk.AlertDefinition) {
			definition.Webhook = nil
		}, wantError: "complete alert definition is required"},
		{name: "unknown result tab", mutate: func(definition *opensplunk.AlertDefinition) {
			definition.Search.PreferredResultTab = opensplunk.SearchResultTab(127)
		}, wantError: "alert preferred result tab is invalid"},
		{name: "unspecified operator", mutate: func(definition *opensplunk.AlertDefinition) {
			definition.Condition.Operator = opensplunk.AlertConditionOperator_ALERT_CONDITION_OPERATOR_UNSPECIFIED
		}, wantError: "alert condition operator is invalid"},
		{name: "empty index scope", mutate: func(definition *opensplunk.AlertDefinition) {
			definition.Search.IndexScope = nil
		}, wantError: "alert index scope must be nonempty and canonical"},
		{name: "uncanonical index scope", mutate: func(definition *opensplunk.AlertDefinition) {
			definition.Search.IndexScope = []string{"Main"}
		}, wantError: "alert index scope must be nonempty and canonical"},
		{name: "duplicated index scope", mutate: func(definition *opensplunk.AlertDefinition) {
			definition.Search.IndexScope = []string{"main", "main"}
		}, wantError: "alert index scope must be nonempty and canonical"},
		{name: "uncanonical application", mutate: func(definition *opensplunk.AlertDefinition) {
			definition.Search.AppId = new(" search ")
		}, wantError: "alert application is invalid"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			definition := alertAPITestDefinition("Errors", "https://hooks.example.com/alerts")
			test.mutate(definition)
			err := sanitizeAlertDefinition(definition, true)
			if err == nil || !strings.Contains(err.Error(), test.wantError) {
				t.Fatalf("sanitizeAlertDefinition() error = %v, want %q", err, test.wantError)
			}
		})
	}

	if err := sanitizeAlertDefinition(nil, false); err == nil ||
		!strings.Contains(err.Error(), "complete alert definition is required") {
		t.Fatalf("absent definition error = %v", err)
	}
}

func TestSanitizeAlertDefinitionLeavesAnAcceptedDefinitionUnchanged(t *testing.T) {
	t.Parallel()
	definition := alertAPITestDefinition("Errors", "https://hooks.example.com/alerts")
	definition.Search.Visualization = &opensplunk.VisualizationSpec{}
	before := proto.Clone(definition)
	if err := sanitizeAlertDefinition(definition, true); err != nil {
		t.Fatalf("sanitizeAlertDefinition() error = %v", err)
	}
	if !proto.Equal(before, definition) {
		t.Fatalf("sanitizer rewrote an accepted definition: %+v", definition)
	}
	converted, webhookURL := alertDefinitionFromProto(definition)
	if webhookURL != "https://hooks.example.com/alerts" || converted.Application != "search" ||
		len(converted.IndexScope) != 1 || converted.IndexScope[0] != "main" {
		t.Fatalf("converted definition = %+v, %q", converted, webhookURL)
	}
}

func TestSanitizeAlertRequestsTolerateUnknownFields(t *testing.T) {
	t.Parallel()
	unknown := futureProtobufField("future-alert-field")
	request := &opensplunk.GetAlertRequest{AlertId: "alert-1"}
	request.ProtoReflect().SetUnknown(unknown)
	sanitized, err := sanitizeGetAlertRequest(t.Context(), request)
	if err != nil {
		t.Fatalf("sanitizeGetAlertRequest() error = %v", err)
	}
	if sanitized.GetAlertId() != "alert-1" {
		t.Fatalf("alert ID = %q, want %q", sanitized.GetAlertId(), "alert-1")
	}
	assertUnknownFieldTolerated(t, sanitized, unknown)
}
