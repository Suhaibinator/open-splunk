package server

import (
	"net/http"
	"testing"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/patterns"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
)

func TestPatternsRoutesShareCompletedResultSnapshotAuthority(t *testing.T) {
	job := completeJob("retained-route")
	jobs := &fakeSearchJobs{getJob: job, resultsPage: searchjobs.ResultPage{Schema: searchjobs.Schema{Columns: []searchjobs.Column{{Name: "_raw", Kind: searchjobs.ValueKindString}}}, Complete: true, Generation: 37}}
	service := &patternAPIFake{result: patterns.ListResult{Generation: 37, SnapshotComplete: true, TotalSize: new(uint64(0)), TotalSizeExact: true}}
	handler := newTestHandler(t, Config{SearchJobs: jobs, SearchPatterns: service, Indexes: fakeIndexCatalog{}, WebUI: testUI(), OwnerID: "owner-1", TenantID: "tenant-1"})
	results := postProto(t, handler, "/api/search/jobs/results", &opensplunk.GetSearchResultsRequest{SearchJobId: job.ID})
	if results.Code != http.StatusOK {
		t.Fatalf("results status %d: %s", results.Code, results.Body.String())
	}
	decoded := &opensplunk.GetSearchResultsResponse{}
	unmarshalResponse(t, results, decoded)
	ref := decoded.GetResultPage().GetSnapshotRef()
	if ref == "" {
		t.Fatal("final result omitted snapshot authority")
	}
	response := postProto(t, handler, apiPathPrefix+searchPatternsListRoute, &opensplunk.ListSearchPatternsRequest{SearchJobId: job.ID, SnapshotRef: ref, Sensitivity: opensplunk.PatternSensitivity_PATTERN_SENSITIVITY_BALANCED, Page: &opensplunk.PageRequest{IncludeTotalSize: true}})
	if response.Code != http.StatusOK || service.calls != 1 || service.query.Generation != 37 {
		t.Fatalf("pattern route = %d/%s, service %+v", response.Code, response.Body.String(), service)
	}
	members := postProto(t, handler, apiPathPrefix+searchPatternMembersRoute, &opensplunk.ListSearchPatternMembersRequest{SearchJobId: job.ID, SnapshotRef: "invalid", PatternId: "member", Sensitivity: opensplunk.PatternSensitivity_PATTERN_SENSITIVITY_BALANCED})
	if members.Code != http.StatusBadRequest {
		t.Fatalf("member route status = %d, body %s", members.Code, members.Body.String())
	}
}

func TestPatternsRoutesAreAbsentWithoutConfiguredService(t *testing.T) {
	handler := newTestHandler(t, Config{SearchJobs: &fakeSearchJobs{}, Indexes: fakeIndexCatalog{}, WebUI: testUI()})
	response := postProto(t, handler, apiPathPrefix+searchPatternsListRoute, &opensplunk.ListSearchPatternsRequest{})
	if response.Code != http.StatusNotFound {
		t.Fatalf("unconfigured pattern route status = %d", response.Code)
	}
}
