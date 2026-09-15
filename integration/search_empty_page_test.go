//go:build !windows

package integration_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"google.golang.org/protobuf/proto"
)

func TestCompletedSearchPagingAcceptsExactEmptyResult(t *testing.T) {
	t.Parallel()
	response := &opensplunk.GetSearchResultsResponse{
		SearchJobId: "empty-job",
		ResultPage: &opensplunk.ResultPage{
			Schema:           &opensplunk.ResultSchema{},
			SnapshotComplete: true,
			Page:             &opensplunk.PageResponse{TotalSize: new(uint64(0)), TotalSizeExact: true},
		},
	}
	wire, err := proto.Marshal(response)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/search/jobs/results" {
			t.Errorf("unexpected path %s", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/x-protobuf")
		if _, err := w.Write(wire); err != nil {
			t.Error(err)
		}
	}))
	defer server.Close()
	result := fetchAllCompletedSearchResults(t, context.Background(), server.Client(), server.URL, "empty-job", 0, 2)
	if result.schema == nil || len(result.rows) != 0 || len(result.responseWire) != 1 {
		t.Fatalf("empty result=%+v", result)
	}
}
