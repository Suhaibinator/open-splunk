package server

import (
	"context"
	"net/http"
	"strings"
	"testing"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
	"github.com/Suhaibinator/open-splunk/internal/searchanalysis"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
)

func TestSearchFieldServiceLimitsAndTypedNilAreValidated(t *testing.T) {
	tests := []struct {
		name                string
		maximumFields       uint32
		maximumPage         uint32
		maximumSummary      uint32
		forceSummaryMaximum bool
		browserPage         uint32
		want                string
	}{
		{name: "zero fields", maximumPage: 1, want: "maximum fields"},
		{name: "too many fields", maximumFields: clickhouse.MaximumFieldCatalogFields + 1, maximumPage: 1, want: "maximum fields"},
		{name: "zero page", maximumFields: 10, want: "maximum page size"},
		{name: "page above fields", maximumFields: 10, maximumPage: 11, want: "maximum page size"},
		{name: "page above transport", maximumFields: 2_000, maximumPage: maximumSearchFieldPageSize + 1, want: "maximum page size"},
		{name: "page above browser limit", maximumFields: 100, maximumPage: 100, browserPage: 50, want: "browser maximum page size"},
		{name: "zero summary values", maximumFields: 10, maximumPage: 10, forceSummaryMaximum: true, want: "summary maximum values"},
		{name: "too many summary values", maximumFields: 10, maximumPage: 10, maximumSummary: clickhouse.MaximumFieldSummaryValues + 1, want: "summary maximum values"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := NewHandler(Config{
				SearchJobs: &fakeSearchJobs{}, Indexes: fakeIndexCatalog{}, SavedSearches: &fakeSavedSearches{},
				SearchFields: &fakeSearchFields{
					maximumFields: test.maximumFields, maximumPage: test.maximumPage,
					maximumSummary: test.maximumSummary, forceSummaryMaximum: test.forceSummaryMaximum,
				}, WebUI: testUI(),
				MaximumPageSize: test.browserPage,
			})
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("NewHandler error = %v, want %q", err, test.want)
			}
		})
	}

	service := &fakeSearchFields{
		maximumFields: 100,
		maximumPage:   100,
		listFn: func(_ context.Context, _ searchjobs.AccessScope, _ searchanalysis.ListFieldsRequest) (searchanalysis.FieldPage, error) {
			return searchanalysis.FieldPage{}, nil
		},
	}
	bounded := newTestHandler(t, Config{
		SearchJobs: &fakeSearchJobs{}, Indexes: fakeIndexCatalog{}, SearchFields: service,
		MaximumPageSize: 1_000, WebUI: testUI(),
	})
	bootstrapResponse := postProto(t, bounded, "/api/v1/system/bootstrap", &opensplunkv1.GetSystemBootstrapRequest{})
	var bootstrap opensplunkv1.GetSystemBootstrapResponse
	unmarshalResponse(t, bootstrapResponse, &bootstrap)
	if bootstrap.GetLimits().GetMaximumPageSize() != 1_000 {
		t.Fatalf("advertised page size = %d, want unrelated browser limit 1000", bootstrap.GetLimits().GetMaximumPageSize())
	}
	if bootstrap.GetLimits().GetMaximumFieldSummaryValues() != service.MaximumSummaryValues() {
		t.Fatalf("advertised summary values = %d, want %d", bootstrap.GetLimits().GetMaximumFieldSummaryValues(), service.MaximumSummaryValues())
	}

	var typedNil *fakeSearchFields
	handler, err := NewHandler(Config{
		SearchJobs: &fakeSearchJobs{}, Indexes: fakeIndexCatalog{}, SavedSearches: &fakeSavedSearches{},
		SearchFields: typedNil, WebUI: testUI(),
	})
	if err != nil {
		t.Fatalf("typed-nil field service: %v", err)
	}
	response := postProto(t, handler, searchFieldsListPath, &opensplunkv1.ListSearchFieldsRequest{SearchJobId: "job"})
	if response.Code != http.StatusNotFound {
		t.Fatalf("typed-nil route status = %d, body = %s", response.Code, response.Body.String())
	}
	response = postProto(t, handler, searchFieldSummaryPath, &opensplunkv1.GetSearchFieldSummaryRequest{SearchJobId: "job", FieldName: "message"})
	if response.Code != http.StatusNotFound {
		t.Fatalf("typed-nil summary route status = %d, body = %s", response.Code, response.Body.String())
	}
}
