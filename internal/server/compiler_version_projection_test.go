package server

import (
	"net/http"
	"strings"
	"testing"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	exportjobs "github.com/Suhaibinator/open-splunk/internal/export"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
	"github.com/Suhaibinator/open-splunk/internal/spl"
)

func TestSearchJobCompilerVersionProjectionIsExactForGetAndList(t *testing.T) {
	t.Parallel()

	version := spl.CompatibilityVersion
	job := completeJob("compiler-version-get")
	job.CompilerVersion = version
	projected, err := searchJobToProto(job, testNow)
	if err != nil {
		t.Fatalf("searchJobToProto() error = %v", err)
	}
	if projected.GetCompilerVersion() != version {
		t.Fatalf("get projection compiler version = %q, want %q", projected.GetCompilerVersion(), version)
	}
	projected.CompilerVersion = "caller-mutated-protobuf"
	fresh, err := searchJobToProto(job, testNow)
	if err != nil || fresh.GetCompilerVersion() != version {
		t.Fatalf("fresh get projection = (%q, %v), want %q", fresh.GetCompilerVersion(), err, version)
	}

	listJob := listSearchJob("compiler-version-list", testNow)
	listJob.CompilerVersion = version
	item := listItem(listJob)
	if item.CompilerVersion != version {
		t.Fatalf("list fixture lost compiler version: %#v", item)
	}
	handler := newSearchJobListTestHandler(t, &fakeSearchJobs{
		listPage: searchjobs.JobListPage{Jobs: []searchjobs.JobListItem{item}},
	}, Config{})
	response := postProto(t, handler, searchJobsListPath, &opensplunkv1.ListSearchJobsRequest{})
	if response.Code != http.StatusOK {
		t.Fatalf("list status = %d, body = %s", response.Code, response.Body.String())
	}
	var decoded opensplunkv1.ListSearchJobsResponse
	unmarshalResponse(t, response, &decoded)
	if len(decoded.GetSearchJobs()) != 1 || decoded.GetSearchJobs()[0].GetCompilerVersion() != version {
		t.Fatalf("list compiler version projection = %+v", decoded.GetSearchJobs())
	}
}

func TestSearchJobCompilerVersionProjectionFailsClosed(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name    string
		version string
	}{
		{name: "empty"},
		{name: "surrounding whitespace", version: " " + spl.CompatibilityVersion + " "},
		{name: "control", version: spl.CompatibilityVersion + "\nforged"},
		{name: "invalid UTF-8", version: string([]byte{0xff})},
		{name: "oversized", version: strings.Repeat("v", searchjobs.MaximumCompilerVersionBytes+1)},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			job := completeJob("invalid-compiler-version")
			job.CompilerVersion = test.version
			projected, err := searchJobToProto(job, testNow)
			if err == nil || projected != nil {
				t.Fatalf("searchJobToProto(CompilerVersion=%q) = (%+v, %v), want fail-closed", test.version, projected, err)
			}
		})
	}
}

func TestExportCompilerVersionProjectionIsExactAndCanonical(t *testing.T) {
	t.Parallel()

	version := spl.CompatibilityVersion
	job := validListExportJob("compiler-version-export", exportjobs.StateCompleted, testNow)
	job.CompilerVersion = version
	projected, err := exportJobToProto(job, testNow)
	if err != nil {
		t.Fatalf("exportJobToProto() error = %v", err)
	}
	if projected.GetCompilerVersion() != version {
		t.Fatalf("export compiler version = %q, want %q", projected.GetCompilerVersion(), version)
	}
	projected.CompilerVersion = "caller-mutated-protobuf"
	fresh, err := exportJobToProto(job, testNow)
	if err != nil || fresh.GetCompilerVersion() != version {
		t.Fatalf("fresh export projection = (%q, %v), want %q", fresh.GetCompilerVersion(), err, version)
	}

	for _, invalid := range []string{
		" " + spl.CompatibilityVersion,
		spl.CompatibilityVersion + " ",
		spl.CompatibilityVersion + "\nforged",
		string([]byte{0xff}),
		strings.Repeat("v", searchjobs.MaximumCompilerVersionBytes+1),
	} {
		job.CompilerVersion = invalid
		if projected, err := exportJobToProto(job, testNow); err == nil || projected != nil {
			t.Fatalf("exportJobToProto(CompilerVersion=%q) = (%+v, %v), want fail-closed", invalid, projected, err)
		}
	}
}
