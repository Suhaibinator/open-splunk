package server

import (
	"net/http"
	"testing"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
)

func TestRetainedResultTruncationFlowsThroughSearchRoutes(t *testing.T) {
	job := completeJob("job-truncated")
	job.ResultsTruncated = true
	job.RowCount = 10_000
	jobs := &fakeSearchJobs{
		getJob: job,
		resultsPage: searchjobs.ResultPage{
			Schema: searchjobs.Schema{Columns: []searchjobs.Column{{Name: "message", Kind: searchjobs.ValueKindString}}},
			Rows: []searchjobs.ResultRow{{Ordinal: 9_999, Values: []searchjobs.Value{
				searchjobs.StringValue("last retained row"),
			}}},
			TotalRows: 10_000,
			Complete:  true,
		},
	}
	handler := newTestHandler(t, Config{SearchJobs: jobs, Indexes: fakeIndexCatalog{}, WebUI: testUI()})

	response := postProto(t, handler, "/api/v1/search/jobs/get", &opensplunkv1.GetSearchJobRequest{SearchJobId: job.ID})
	if response.Code != http.StatusOK {
		t.Fatalf("get status = %d, body = %s", response.Code, response.Body.String())
	}
	var got opensplunkv1.GetSearchJobResponse
	unmarshalResponse(t, response, &got)
	if !got.GetSearchJob().GetResultsTruncated() || len(got.GetSearchJob().GetWarnings()) != 1 ||
		got.GetSearchJob().GetWarnings()[0].GetCode() != "RESULTS_TRUNCATED" {
		t.Fatalf("truncated search job = %+v", got.GetSearchJob())
	}

	response = postProto(t, handler, "/api/v1/search/jobs/results", &opensplunkv1.GetSearchResultsRequest{
		SearchJobId: job.ID,
		Page:        &opensplunkv1.PageRequest{IncludeTotalSize: true},
	})
	if response.Code != http.StatusOK {
		t.Fatalf("results status = %d, body = %s", response.Code, response.Body.String())
	}
	var results opensplunkv1.GetSearchResultsResponse
	unmarshalResponse(t, response, &results)
	page := results.GetResultPage()
	if page.GetSnapshotComplete() || page.GetPage().GetTotalSizeExact() || page.GetPage().GetTotalSize() != 10_000 {
		t.Fatalf("truncated page = %+v", page)
	}
}
