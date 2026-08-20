package server

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	exportjobs "github.com/Suhaibinator/open-splunk/internal/export"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
	"google.golang.org/protobuf/proto"
)

func TestExportListRouteRoundTripsScopeCanonicalFiltersAndAllStates(t *testing.T) {
	states := []exportjobs.State{
		exportjobs.StateQueued,
		exportjobs.StateRunning,
		exportjobs.StateCompleted,
		exportjobs.StateFailed,
		exportjobs.StateCanceled,
		exportjobs.StateExpired,
	}
	jobs := make([]exportjobs.Job, len(states))
	for index, state := range states {
		jobs[index] = validListExportJob(
			"export-"+string(rune('f'-index)),
			state,
			testNow.Add(-time.Duration(index)*time.Minute),
		)
	}
	total := uint64(19)
	service := &fakeExports{listFn: func(
		_ context.Context,
		scope searchjobs.AccessScope,
		_ exportjobs.ListRequest,
	) (exportjobs.ListPage, error) {
		assertExportScope(t, scope, "tenant-list", "owner-list")
		return exportjobs.ListPage{
			Jobs:           scopedExportListItems("tenant-list", "owner-list", jobs...),
			NextPageToken:  "next-export-page",
			TotalSize:      &total,
			TotalSizeExact: true,
		}, nil
	}}
	handler := newExportListTestHandler(t, service, Config{
		OwnerID: "owner-list", TenantID: "tenant-list",
	})
	pageSize := uint32(len(jobs))
	pageToken, searchJobID := "current-export-page", "search-1"
	request := &opensplunk.ListExportJobsRequest{
		Page: &opensplunk.PageRequest{
			PageSize:         &pageSize,
			PageToken:        &pageToken,
			IncludeTotalSize: true,
		},
		StateFilters: []opensplunk.ExportJobState{
			opensplunk.ExportJobState_EXPORT_JOB_STATE_EXPIRED,
			opensplunk.ExportJobState_EXPORT_JOB_STATE_FAILED,
			opensplunk.ExportJobState_EXPORT_JOB_STATE_QUEUED,
			opensplunk.ExportJobState_EXPORT_JOB_STATE_COMPLETED,
			opensplunk.ExportJobState_EXPORT_JOB_STATE_RUNNING,
			opensplunk.ExportJobState_EXPORT_JOB_STATE_CANCELED,
			opensplunk.ExportJobState_EXPORT_JOB_STATE_FAILED,
		},
		SearchJobIdFilter: &searchJobID,
	}
	request.ProtoReflect().SetUnknown(futureProtobufField("future-export-list"))
	request.Page.ProtoReflect().SetUnknown(futureProtobufField("future-export-page"))
	response := postProto(t, handler, exportJobsListPath, request)
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
	}
	if got := response.Header().Get("Content-Type"); got != "application/x-protobuf" {
		t.Fatalf("Content-Type = %q", got)
	}

	service.mu.Lock()
	capturedScope := service.listScope
	captured := service.listRequest
	calls := service.listCalls
	service.mu.Unlock()
	if calls != 1 {
		t.Fatalf("list calls = %d, want 1", calls)
	}
	if capturedScope != (searchjobs.AccessScope{OwnerID: "owner-list", TenantID: "tenant-list"}) {
		t.Fatalf("scope = %+v", capturedScope)
	}
	if captured.PageSize != len(jobs) || captured.PageToken != pageToken || !captured.IncludeTotal {
		t.Fatalf("page request = %+v", captured)
	}
	if !slices.Equal(captured.StateFilters, states) {
		t.Fatalf("state filters = %v, want %v", captured.StateFilters, states)
	}
	if captured.SearchJobIDFilter == nil || *captured.SearchJobIDFilter != "search-1" {
		t.Fatalf("search job filter = %#v", captured.SearchJobIDFilter)
	}

	var decoded opensplunk.ListExportJobsResponse
	unmarshalResponse(t, response, &decoded)
	if len(decoded.GetExportJobs()) != len(states) {
		t.Fatalf("jobs = %d, want %d", len(decoded.GetExportJobs()), len(states))
	}
	for index, want := range []opensplunk.ExportJobState{
		opensplunk.ExportJobState_EXPORT_JOB_STATE_QUEUED,
		opensplunk.ExportJobState_EXPORT_JOB_STATE_RUNNING,
		opensplunk.ExportJobState_EXPORT_JOB_STATE_COMPLETED,
		opensplunk.ExportJobState_EXPORT_JOB_STATE_FAILED,
		opensplunk.ExportJobState_EXPORT_JOB_STATE_CANCELED,
		opensplunk.ExportJobState_EXPORT_JOB_STATE_EXPIRED,
	} {
		if got := decoded.GetExportJobs()[index].GetState(); got != want {
			t.Fatalf("job %d state = %v, want %v", index, got, want)
		}
	}
	if decoded.GetPage().GetNextPageToken() != "next-export-page" ||
		decoded.GetPage().GetTotalSize() != total ||
		!decoded.GetPage().GetTotalSizeExact() {
		t.Fatalf("page = %+v", decoded.GetPage())
	}
}

func TestExportListDefaultsAndCapsPageSizeAtFifteen(t *testing.T) {
	for _, test := range []struct {
		name   string
		config Config
		want   int
	}{
		{name: "default", want: maximumExportListRows},
		{
			name:   "configured transport maximum",
			config: Config{MaximumPageSize: 7},
			want:   7,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			service := &fakeExports{}
			handler := newExportListTestHandler(t, service, test.config)
			response := postProto(t, handler, exportJobsListPath, &opensplunk.ListExportJobsRequest{})
			if response.Code != http.StatusOK {
				t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
			}
			service.mu.Lock()
			captured := service.listRequest
			service.mu.Unlock()
			if captured.PageSize != test.want {
				t.Fatalf("page size = %d, want %d", captured.PageSize, test.want)
			}
		})
	}
}

func TestExportListAllowsPageSizeAndTotalOptionToChangeOnContinuation(t *testing.T) {
	var (
		mu       sync.Mutex
		requests []exportjobs.ListRequest
	)
	service := &fakeExports{listFn: func(
		_ context.Context,
		_ searchjobs.AccessScope,
		request exportjobs.ListRequest,
	) (exportjobs.ListPage, error) {
		mu.Lock()
		requests = append(requests, request)
		mu.Unlock()
		if request.IncludeTotal {
			total := uint64(0)
			return exportjobs.ListPage{
				TotalSize:      &total,
				TotalSizeExact: true,
			}, nil
		}
		return exportjobs.ListPage{}, nil
	}}
	handler := newExportListTestHandler(t, service, Config{})
	token, searchJobID := "opaque-continuation", "search-1"
	firstSize, secondSize := uint32(1), uint32(7)
	stateFilters := []opensplunk.ExportJobState{
		opensplunk.ExportJobState_EXPORT_JOB_STATE_RUNNING,
	}
	for _, request := range []*opensplunk.ListExportJobsRequest{
		{
			Page: &opensplunk.PageRequest{
				PageSize:  &firstSize,
				PageToken: &token,
			},
			StateFilters:      stateFilters,
			SearchJobIdFilter: &searchJobID,
		},
		{
			Page: &opensplunk.PageRequest{
				PageSize:         &secondSize,
				PageToken:        &token,
				IncludeTotalSize: true,
			},
			StateFilters:      stateFilters,
			SearchJobIdFilter: &searchJobID,
		},
	} {
		response := postProto(t, handler, exportJobsListPath, request)
		if response.Code != http.StatusOK {
			t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
		}
	}
	mu.Lock()
	defer mu.Unlock()
	if len(requests) != 2 ||
		requests[0].PageSize != 1 ||
		requests[0].IncludeTotal ||
		requests[1].PageSize != 7 ||
		!requests[1].IncludeTotal ||
		requests[0].PageToken != token ||
		requests[1].PageToken != token ||
		!slices.Equal(requests[0].StateFilters, requests[1].StateFilters) ||
		requests[0].SearchJobIDFilter == nil ||
		requests[1].SearchJobIDFilter == nil ||
		*requests[0].SearchJobIDFilter != searchJobID ||
		*requests[1].SearchJobIDFilter != searchJobID {
		t.Fatalf("continuation requests = %+v", requests)
	}
}

func TestExportListRejectsInvalidRequestBeforeService(t *testing.T) {
	invalidUTF8SearchID := string([]byte{0xff})
	if _, err := exportListSearchJobIDFilter(&invalidUTF8SearchID); err == nil {
		t.Fatal("invalid UTF-8 search job filter was accepted")
	}

	zero, aboveMaximum := uint32(0), uint32(maximumExportListRows+1)
	oversizedToken := strings.Repeat("t", maximumExportListPageTokenBytes+1)
	paddedToken := " signed-token "
	controlToken := "signed\x00token"
	tooManyStates := make(
		[]opensplunk.ExportJobState,
		maximumExportListStateFilters+1,
	)
	for index := range tooManyStates {
		tooManyStates[index] = opensplunk.ExportJobState_EXPORT_JOB_STATE_QUEUED
	}
	emptySearchID := " \t "
	paddedSearchID := " search-1 "
	oversizedSearchID := strings.Repeat(
		"s",
		exportjobs.MaximumSearchJobIDBytes+1,
	)
	controlSearchID := "search\nid"
	tests := []struct {
		name    string
		request *opensplunk.ListExportJobsRequest
	}{
		{
			name: "explicit zero page size",
			request: &opensplunk.ListExportJobsRequest{
				Page: &opensplunk.PageRequest{PageSize: &zero},
			},
		},
		{
			name: "page size above endpoint maximum",
			request: &opensplunk.ListExportJobsRequest{
				Page: &opensplunk.PageRequest{PageSize: &aboveMaximum},
			},
		},
		{
			name: "oversized token",
			request: &opensplunk.ListExportJobsRequest{
				Page: &opensplunk.PageRequest{PageToken: &oversizedToken},
			},
		},
		{
			name: "padded token",
			request: &opensplunk.ListExportJobsRequest{
				Page: &opensplunk.PageRequest{PageToken: &paddedToken},
			},
		},
		{
			name: "control token",
			request: &opensplunk.ListExportJobsRequest{
				Page: &opensplunk.PageRequest{PageToken: &controlToken},
			},
		},
		{name: "too many raw states", request: &opensplunk.ListExportJobsRequest{
			StateFilters: tooManyStates,
		}},
		{name: "unspecified state", request: &opensplunk.ListExportJobsRequest{
			StateFilters: []opensplunk.ExportJobState{
				opensplunk.ExportJobState_EXPORT_JOB_STATE_UNSPECIFIED,
			},
		}},
		{name: "unknown state", request: &opensplunk.ListExportJobsRequest{
			StateFilters: []opensplunk.ExportJobState{opensplunk.ExportJobState(99)},
		}},
		{name: "empty search job filter", request: &opensplunk.ListExportJobsRequest{
			SearchJobIdFilter: &emptySearchID,
		}},
		{name: "padded search job filter", request: &opensplunk.ListExportJobsRequest{
			SearchJobIdFilter: &paddedSearchID,
		}},
		{name: "oversized search job filter", request: &opensplunk.ListExportJobsRequest{
			SearchJobIdFilter: &oversizedSearchID,
		}},
		{name: "control search job filter", request: &opensplunk.ListExportJobsRequest{
			SearchJobIdFilter: &controlSearchID,
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			service := &fakeExports{}
			handler := newExportListTestHandler(t, service, Config{})
			response := postProto(t, handler, exportJobsListPath, test.request)
			if response.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
			}
			service.mu.Lock()
			calls := service.listCalls
			service.mu.Unlock()
			if calls != 0 {
				t.Fatalf("service calls = %d, want 0", calls)
			}
		})
	}
}

func TestExportListRejectsCorruptServicePages(t *testing.T) {
	base := func(id string, createdAt time.Time) exportjobs.Job {
		return validListExportJob(id, exportjobs.StateQueued, createdAt)
	}
	tests := []struct {
		name    string
		request *opensplunk.ListExportJobsRequest
		page    func() exportjobs.ListPage
	}{
		{
			name:    "cross owner",
			request: &opensplunk.ListExportJobsRequest{},
			page: func() exportjobs.ListPage {
				items := exportListItems(base("export-a", testNow))
				items[0].OwnerID = "other-owner"
				return exportjobs.ListPage{Jobs: items}
			},
		},
		{
			name:    "cross tenant",
			request: &opensplunk.ListExportJobsRequest{},
			page: func() exportjobs.ListPage {
				items := exportListItems(base("export-a", testNow))
				items[0].TenantID = "other-tenant"
				return exportjobs.ListPage{Jobs: items}
			},
		},
		{
			name:    "invalid state",
			request: &opensplunk.ListExportJobsRequest{},
			page: func() exportjobs.ListPage {
				job := base("export-a", testNow)
				job.State = exportjobs.StateInvalid
				return exportjobs.ListPage{Jobs: exportListItems(job)}
			},
		},
		{
			name:    "empty ID",
			request: &opensplunk.ListExportJobsRequest{},
			page: func() exportjobs.ListPage {
				return exportjobs.ListPage{Jobs: exportListItems(
					base("", testNow),
				)}
			},
		},
		{
			name:    "padded ID",
			request: &opensplunk.ListExportJobsRequest{},
			page: func() exportjobs.ListPage {
				return exportjobs.ListPage{Jobs: exportListItems(
					base(" export-a ", testNow),
				)}
			},
		},
		{
			name:    "zero version",
			request: &opensplunk.ListExportJobsRequest{},
			page: func() exportjobs.ListPage {
				job := base("export-a", testNow)
				job.Version = 0
				return exportjobs.ListPage{Jobs: exportListItems(job)}
			},
		},
		{
			name:    "invalid format",
			request: &opensplunk.ListExportJobsRequest{},
			page: func() exportjobs.ListPage {
				job := base("export-a", testNow)
				job.Format = exportjobs.FormatInvalid
				return exportjobs.ListPage{Jobs: exportListItems(job)}
			},
		},
		{
			name:    "duplicate selected column",
			request: &opensplunk.ListExportJobsRequest{},
			page: func() exportjobs.ListPage {
				job := base("export-a", testNow)
				job.Columns = []string{"message", "message"}
				return exportjobs.ListPage{Jobs: exportListItems(job)}
			},
		},
		{
			name:    "empty selected column",
			request: &opensplunk.ListExportJobsRequest{},
			page: func() exportjobs.ListPage {
				job := base("export-a", testNow)
				job.Columns = []string{""}
				return exportjobs.ListPage{Jobs: exportListItems(job)}
			},
		},
		{
			name:    "control byte in selected column",
			request: &opensplunk.ListExportJobsRequest{},
			page: func() exportjobs.ListPage {
				job := base("export-a", testNow)
				job.Columns = []string{"message\ninjected"}
				return exportjobs.ListPage{Jobs: exportListItems(job)}
			},
		},
		{
			name:    "queued state has progress",
			request: &opensplunk.ListExportJobsRequest{},
			page: func() exportjobs.ListPage {
				job := base("export-a", testNow)
				job.Progress.RowsWritten = 1
				return exportjobs.ListPage{Jobs: exportListItems(job)}
			},
		},
		{
			name:    "progress timestamp precedes creation",
			request: &opensplunk.ListExportJobsRequest{},
			page: func() exportjobs.ListPage {
				job := base("export-a", testNow)
				job.Progress.UpdatedAt = job.CreatedAt.Add(-time.Second)
				return exportjobs.ListPage{Jobs: exportListItems(job)}
			},
		},
		{
			name:    "started timestamp precedes creation",
			request: &opensplunk.ListExportJobsRequest{},
			page: func() exportjobs.ListPage {
				job := validListExportJob(
					"export-a",
					exportjobs.StateRunning,
					testNow,
				)
				job.StartedAt = job.CreatedAt.Add(-time.Second)
				return exportjobs.ListPage{Jobs: exportListItems(job)}
			},
		},
		{
			name:    "progress timestamp precedes start",
			request: &opensplunk.ListExportJobsRequest{},
			page: func() exportjobs.ListPage {
				job := validListExportJob(
					"export-a",
					exportjobs.StateRunning,
					testNow,
				)
				job.Progress.UpdatedAt = job.StartedAt.Add(-time.Second)
				return exportjobs.ListPage{Jobs: exportListItems(job)}
			},
		},
		{
			name:    "finished timestamp precedes start",
			request: &opensplunk.ListExportJobsRequest{},
			page: func() exportjobs.ListPage {
				job := validListExportJob(
					"export-a",
					exportjobs.StateCompleted,
					testNow.Add(-time.Minute),
				)
				job.FinishedAt = job.StartedAt.Add(-time.Second)
				return exportjobs.ListPage{Jobs: exportListItems(job)}
			},
		},
		{
			name:    "expiration precedes finish",
			request: &opensplunk.ListExportJobsRequest{},
			page: func() exportjobs.ListPage {
				job := validListExportJob(
					"export-a",
					exportjobs.StateCompleted,
					testNow.Add(-time.Minute),
				)
				job.ExpiresAt = job.FinishedAt.Add(-time.Second)
				job.Artifact.ExpiresAt = job.ExpiresAt
				return exportjobs.ListPage{Jobs: exportListItems(job)}
			},
		},
		{
			name:    "failed state missing failure",
			request: &opensplunk.ListExportJobsRequest{},
			page: func() exportjobs.ListPage {
				job := validListExportJob(
					"export-a",
					exportjobs.StateFailed,
					testNow.Add(-time.Minute),
				)
				job.Failure = nil
				return exportjobs.ListPage{Jobs: exportListItems(job)}
			},
		},
		{
			name:    "queued state carries failure",
			request: &opensplunk.ListExportJobsRequest{},
			page: func() exportjobs.ListPage {
				job := base("export-a", testNow)
				job.Failure = &exportjobs.Failure{
					Code:    exportjobs.FailureInternal,
					Message: "must not be present",
				}
				return exportjobs.ListPage{Jobs: exportListItems(job)}
			},
		},
		{
			name:    "invalid failure code",
			request: &opensplunk.ListExportJobsRequest{},
			page: func() exportjobs.ListPage {
				job := validListExportJob(
					"export-a",
					exportjobs.StateFailed,
					testNow.Add(-time.Minute),
				)
				job.Failure.Code = exportjobs.FailureInvalid
				return exportjobs.ListPage{Jobs: exportListItems(job)}
			},
		},
		{
			name:    "failure leaks dependency detail",
			request: &opensplunk.ListExportJobsRequest{},
			page: func() exportjobs.ListPage {
				job := validListExportJob(
					"export-a",
					exportjobs.StateFailed,
					testNow.Add(-time.Minute),
				)
				job.Failure.Message = "/private/export/session/results.csv"
				return exportjobs.ListPage{Jobs: exportListItems(job)}
			},
		},
		{
			name:    "failure retryability disagrees with code",
			request: &opensplunk.ListExportJobsRequest{},
			page: func() exportjobs.ListPage {
				job := validListExportJob(
					"export-a",
					exportjobs.StateFailed,
					testNow.Add(-time.Minute),
				)
				job.Failure.Retryable = true
				return exportjobs.ListPage{Jobs: exportListItems(job)}
			},
		},
		{
			name:    "completed state missing artifact",
			request: &opensplunk.ListExportJobsRequest{},
			page: func() exportjobs.ListPage {
				job := validListExportJob(
					"export-a",
					exportjobs.StateCompleted,
					testNow.Add(-time.Minute),
				)
				job.Artifact = nil
				return exportjobs.ListPage{Jobs: exportListItems(job)}
			},
		},
		{
			name:    "artifact counters disagree with progress",
			request: &opensplunk.ListExportJobsRequest{},
			page: func() exportjobs.ListPage {
				job := validListExportJob(
					"export-a",
					exportjobs.StateCompleted,
					testNow.Add(-time.Minute),
				)
				job.Artifact.RowCount++
				return exportjobs.ListPage{Jobs: exportListItems(job)}
			},
		},
		{
			name:    "artifact filename disagrees with ID",
			request: &opensplunk.ListExportJobsRequest{},
			page: func() exportjobs.ListPage {
				job := validListExportJob(
					"export-a",
					exportjobs.StateCompleted,
					testNow.Add(-time.Minute),
				)
				job.Artifact.FileName = "other.csv"
				return exportjobs.ListPage{Jobs: exportListItems(job)}
			},
		},
		{
			name:    "artifact media type disagrees with format",
			request: &opensplunk.ListExportJobsRequest{},
			page: func() exportjobs.ListPage {
				job := validListExportJob(
					"export-a",
					exportjobs.StateCompleted,
					testNow.Add(-time.Minute),
				)
				job.Artifact.MediaType = "application/octet-stream"
				return exportjobs.ListPage{Jobs: exportListItems(job)}
			},
		},
		{
			name: "state filter mismatch",
			request: &opensplunk.ListExportJobsRequest{
				StateFilters: []opensplunk.ExportJobState{
					opensplunk.ExportJobState_EXPORT_JOB_STATE_FAILED,
				},
			},
			page: func() exportjobs.ListPage {
				return exportjobs.ListPage{Jobs: exportListItems(base("export-a", testNow))}
			},
		},
		{
			name: "search job filter mismatch",
			request: func() *opensplunk.ListExportJobsRequest {
				filter := "search-other"
				return &opensplunk.ListExportJobsRequest{SearchJobIdFilter: &filter}
			}(),
			page: func() exportjobs.ListPage {
				return exportjobs.ListPage{Jobs: exportListItems(base("export-a", testNow))}
			},
		},
		{
			name:    "duplicate ID",
			request: &opensplunk.ListExportJobsRequest{},
			page: func() exportjobs.ListPage {
				return exportjobs.ListPage{Jobs: exportListItems(
					base("export-a", testNow),
					base("export-a", testNow.Add(-time.Second)),
				)}
			},
		},
		{
			name:    "ascending creation time",
			request: &opensplunk.ListExportJobsRequest{},
			page: func() exportjobs.ListPage {
				return exportjobs.ListPage{Jobs: exportListItems(
					base("export-b", testNow.Add(-time.Second)),
					base("export-a", testNow),
				)}
			},
		},
		{
			name:    "ascending ID tie break",
			request: &opensplunk.ListExportJobsRequest{},
			page: func() exportjobs.ListPage {
				return exportjobs.ListPage{Jobs: exportListItems(
					base("export-a", testNow),
					base("export-b", testNow),
				)}
			},
		},
		{
			name: "more than requested",
			request: func() *opensplunk.ListExportJobsRequest {
				size := uint32(1)
				return &opensplunk.ListExportJobsRequest{
					Page: &opensplunk.PageRequest{PageSize: &size},
				}
			}(),
			page: func() exportjobs.ListPage {
				return exportjobs.ListPage{Jobs: exportListItems(
					base("export-b", testNow),
					base("export-a", testNow.Add(-time.Second)),
				)}
			},
		},
		{
			name:    "token on short page",
			request: &opensplunk.ListExportJobsRequest{},
			page: func() exportjobs.ListPage {
				return exportjobs.ListPage{
					Jobs:          exportListItems(base("export-a", testNow)),
					NextPageToken: "unexpected",
				}
			},
		},
		{
			name: "replayed token",
			request: func() *opensplunk.ListExportJobsRequest {
				size := uint32(1)
				token := "same-token"
				return &opensplunk.ListExportJobsRequest{Page: &opensplunk.PageRequest{
					PageSize: &size, PageToken: &token,
				}}
			}(),
			page: func() exportjobs.ListPage {
				return exportjobs.ListPage{
					Jobs:          exportListItems(base("export-a", testNow)),
					NextPageToken: "same-token",
				}
			},
		},
		{
			name: "invalid response token",
			request: func() *opensplunk.ListExportJobsRequest {
				size := uint32(1)
				return &opensplunk.ListExportJobsRequest{
					Page: &opensplunk.PageRequest{PageSize: &size},
				}
			}(),
			page: func() exportjobs.ListPage {
				return exportjobs.ListPage{
					Jobs:          exportListItems(base("export-a", testNow)),
					NextPageToken: " bad ",
				}
			},
		},
		{
			name: "oversized response token",
			request: func() *opensplunk.ListExportJobsRequest {
				size := uint32(1)
				return &opensplunk.ListExportJobsRequest{
					Page: &opensplunk.PageRequest{PageSize: &size},
				}
			}(),
			page: func() exportjobs.ListPage {
				return exportjobs.ListPage{
					Jobs: exportListItems(base("export-a", testNow)),
					NextPageToken: strings.Repeat(
						"x",
						maximumExportListPageTokenBytes+1,
					),
				}
			},
		},
		{
			name:    "unexpected total",
			request: &opensplunk.ListExportJobsRequest{},
			page: func() exportjobs.ListPage {
				total := uint64(1)
				return exportjobs.ListPage{
					Jobs:           exportListItems(base("export-a", testNow)),
					TotalSize:      &total,
					TotalSizeExact: true,
				}
			},
		},
		{
			name:    "exact flag without total",
			request: &opensplunk.ListExportJobsRequest{},
			page: func() exportjobs.ListPage {
				return exportjobs.ListPage{
					Jobs:           exportListItems(base("export-a", testNow)),
					TotalSizeExact: true,
				}
			},
		},
		{
			name: "missing requested total",
			request: &opensplunk.ListExportJobsRequest{Page: &opensplunk.PageRequest{
				IncludeTotalSize: true,
			}},
			page: func() exportjobs.ListPage {
				return exportjobs.ListPage{Jobs: exportListItems(base("export-a", testNow))}
			},
		},
		{
			name: "total smaller than page",
			request: &opensplunk.ListExportJobsRequest{Page: &opensplunk.PageRequest{
				IncludeTotalSize: true,
			}},
			page: func() exportjobs.ListPage {
				total := uint64(1)
				return exportjobs.ListPage{
					Jobs: exportListItems(
						base("export-b", testNow),
						base("export-a", testNow.Add(-time.Second)),
					),
					TotalSize:      &total,
					TotalSizeExact: true,
				}
			},
		},
		{
			name: "first terminal page total mismatch",
			request: &opensplunk.ListExportJobsRequest{Page: &opensplunk.PageRequest{
				IncludeTotalSize: true,
			}},
			page: func() exportjobs.ListPage {
				total := uint64(2)
				return exportjobs.ListPage{
					Jobs:           exportListItems(base("export-a", testNow)),
					TotalSize:      &total,
					TotalSizeExact: true,
				}
			},
		},
		{
			name: "continued page total has no remaining item",
			request: func() *opensplunk.ListExportJobsRequest {
				size := uint32(1)
				return &opensplunk.ListExportJobsRequest{Page: &opensplunk.PageRequest{
					PageSize: &size, IncludeTotalSize: true,
				}}
			}(),
			page: func() exportjobs.ListPage {
				total := uint64(1)
				return exportjobs.ListPage{
					Jobs:           exportListItems(base("export-a", testNow)),
					NextPageToken:  "next-page",
					TotalSize:      &total,
					TotalSizeExact: true,
				}
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			service := &fakeExports{listFn: func(
				context.Context,
				searchjobs.AccessScope,
				exportjobs.ListRequest,
			) (exportjobs.ListPage, error) {
				return test.page(), nil
			}}
			handler := newExportListTestHandler(t, service, Config{})
			response := postProto(t, handler, exportJobsListPath, test.request)
			if response.Code != http.StatusInternalServerError {
				t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
			}
			if response.Body.String() != "{\"error\":{\"message\":\"internal server error\"}}\n" {
				t.Fatalf("error body = %q", response.Body.String())
			}
		})
	}
}

func TestExportListAcceptsInternallyConsistentServiceClockSkew(t *testing.T) {
	createdAt := testNow.Add(time.Minute)
	job := validListExportJob(
		"export-a",
		exportjobs.StateCompleted,
		createdAt,
	)
	job.ExpiresAt = createdAt.Add(time.Hour)
	job.Artifact.ExpiresAt = job.ExpiresAt
	service := &fakeExports{listFn: func(
		context.Context,
		searchjobs.AccessScope,
		exportjobs.ListRequest,
	) (exportjobs.ListPage, error) {
		return exportjobs.ListPage{Jobs: exportListItems(job)}, nil
	}}
	handler := newExportListTestHandler(t, service, Config{})
	response := postProto(
		t,
		handler,
		exportJobsListPath,
		&opensplunk.ListExportJobsRequest{},
	)
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
	}
}

func TestExportListMapsErrorsWithoutLeakingDetails(t *testing.T) {
	tests := []struct {
		name    string
		err     error
		status  int
		message string
	}{
		{
			name: "invalid filter",
			err: errors.Join(
				exportjobs.ErrInvalidListFilter,
				errors.New("secret state filter detail"),
			),
			status:  http.StatusBadRequest,
			message: "export list filter is invalid",
		},
		{
			name: "invalid cursor",
			err: errors.Join(
				exportjobs.ErrInvalidCursor,
				errors.New("secret cursor signature"),
			),
			status:  http.StatusBadRequest,
			message: "export page token is invalid",
		},
		{
			name:   "closed",
			err:    exportjobs.ErrClosed,
			status: http.StatusServiceUnavailable,
		},
		{
			name:    "list capacity",
			err:     exportjobs.ErrListCapacity,
			status:  http.StatusTooManyRequests,
			message: "export list capacity is exhausted",
		},
		{
			name:   "canceled",
			err:    context.Canceled,
			status: http.StatusRequestTimeout,
		},
		{
			name:   "deadline",
			err:    context.DeadlineExceeded,
			status: http.StatusRequestTimeout,
		},
		{
			name:   "internal",
			err:    errors.New("SELECT secret_path FROM exports"),
			status: http.StatusInternalServerError,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			service := &fakeExports{listFn: func(
				context.Context,
				searchjobs.AccessScope,
				exportjobs.ListRequest,
			) (exportjobs.ListPage, error) {
				return exportjobs.ListPage{}, test.err
			}}
			handler := newExportListTestHandler(t, service, Config{})
			response := postProto(
				t,
				handler,
				exportJobsListPath,
				&opensplunk.ListExportJobsRequest{},
			)
			if response.Code != test.status {
				t.Fatalf(
					"status = %d, want %d, body = %s",
					response.Code,
					test.status,
					response.Body.String(),
				)
			}
			if test.message != "" && !strings.Contains(response.Body.String(), test.message) {
				t.Fatalf("body = %q, want message %q", response.Body.String(), test.message)
			}
			if strings.Contains(response.Body.String(), "secret") ||
				strings.Contains(response.Body.String(), "SELECT") {
				t.Fatalf("dependency detail leaked: %q", response.Body.String())
			}
		})
	}
}

func TestExportListAcquiresSerializationPermitBeforeService(t *testing.T) {
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	service := &fakeExports{listFn: func(
		context.Context,
		searchjobs.AccessScope,
		exportjobs.ListRequest,
	) (exportjobs.ListPage, error) {
		entered <- struct{}{}
		<-release
		return exportjobs.ListPage{}, nil
	}}
	handler := newExportListTestHandler(
		t,
		service,
		Config{MaximumConcurrentResponses: 1},
	)
	payload, err := proto.Marshal(&opensplunk.ListExportJobsRequest{})
	if err != nil {
		t.Fatal(err)
	}
	serve := func() int {
		request := httptest.NewRequestWithContext(
			context.Background(),
			http.MethodPost,
			exportJobsListPath,
			bytes.NewReader(payload),
		)
		request.Header.Set("Content-Type", "application/x-protobuf")
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		return response.Code
	}
	firstDone := make(chan int, 1)
	go func() { firstDone <- serve() }()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("first list request did not enter service")
	}
	if status := serve(); status != http.StatusServiceUnavailable {
		t.Fatalf("second list status = %d, want %d", status, http.StatusServiceUnavailable)
	}
	select {
	case <-entered:
		t.Fatal("rejected list request entered service")
	default:
	}
	close(release)
	select {
	case status := <-firstDone:
		if status != http.StatusOK {
			t.Fatalf("first list status = %d", status)
		}
	case <-time.After(time.Second):
		t.Fatal("first list request did not finish")
	}
}

func TestExportListCancellationPreventsProtobufTransfer(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	service := &fakeExports{listFn: func(
		context.Context,
		searchjobs.AccessScope,
		exportjobs.ListRequest,
	) (exportjobs.ListPage, error) {
		cancel()
		return exportjobs.ListPage{}, nil
	}}
	handler := newExportListTestHandler(t, service, Config{})
	payload, err := proto.Marshal(&opensplunk.ListExportJobsRequest{})
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequestWithContext(
		ctx,
		http.MethodPost,
		exportJobsListPath,
		bytes.NewReader(payload),
	)
	request.Header.Set("Content-Type", "application/x-protobuf")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusRequestTimeout {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
	}
	if response.Header().Get("Content-Type") == "application/x-protobuf" {
		t.Fatal("canceled response was transferred as protobuf")
	}
}

func TestExportListRejectsOversizedServiceProjection(t *testing.T) {
	job := validListExportJob("export-a", exportjobs.StateQueued, testNow)
	job.Columns = []string{strings.Repeat("x", maximumExportListResponseBytes+1)}
	service := &fakeExports{listFn: func(
		context.Context,
		searchjobs.AccessScope,
		exportjobs.ListRequest,
	) (exportjobs.ListPage, error) {
		return exportjobs.ListPage{Jobs: exportListItems(job)}, nil
	}}
	handler := newExportListTestHandler(t, service, Config{})
	response := postProto(
		t,
		handler,
		exportJobsListPath,
		&opensplunk.ListExportJobsRequest{},
	)
	if response.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
	}
	if response.Body.Len() >= maximumExportListResponseBytes {
		t.Fatalf("error response length = %d", response.Body.Len())
	}
}

func TestExportListRouteIsExactAndPostOnly(t *testing.T) {
	handler := newExportListTestHandler(t, &fakeExports{}, Config{})
	request := httptest.NewRequestWithContext(
		context.Background(),
		http.MethodGet,
		exportJobsListPath,
		nil,
	)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusMethodNotAllowed ||
		response.Header().Get("Allow") != http.MethodPost {
		t.Fatalf(
			"GET response = %d, Allow %q",
			response.Code,
			response.Header().Get("Allow"),
		)
	}
	response = postProto(
		t,
		handler,
		exportJobsListPath+"/extra",
		&opensplunk.ListExportJobsRequest{},
	)
	if response.Code != http.StatusNotFound {
		t.Fatalf("suffix status = %d, body = %s", response.Code, response.Body.String())
	}
}

func newExportListTestHandler(
	t *testing.T,
	exports Exports,
	overrides Config,
) *Handler {
	t.Helper()
	overrides.SearchJobs = &fakeSearchJobs{}
	overrides.Indexes = fakeIndexCatalog{}
	overrides.Exports = exports
	overrides.WebUI = testUI()
	if overrides.OwnerID == "" {
		overrides.OwnerID = "owner-list"
	}
	if overrides.TenantID == "" {
		overrides.TenantID = "tenant-list"
	}
	overrides.Now = func() time.Time { return testNow }
	return newTestHandler(t, overrides)
}

func validListExportJob(
	id string,
	state exportjobs.State,
	createdAt time.Time,
) exportjobs.Job {
	job := testExportJob(id, exportjobs.FormatCSV, state)
	job.CreatedAt = createdAt
	job.Progress.UpdatedAt = createdAt
	job.StartedAt = time.Time{}
	job.FinishedAt = time.Time{}
	job.ExpiresAt = time.Time{}
	job.Artifact = nil
	job.Failure = nil
	switch state {
	case exportjobs.StateQueued:
		job.Progress.RowsWritten = 0
		job.Progress.BytesWritten = 0
	case exportjobs.StateRunning:
		job.StartedAt = createdAt.Add(time.Second)
		job.Progress.UpdatedAt = job.StartedAt
	case exportjobs.StateCompleted:
		job.StartedAt = createdAt.Add(time.Second)
		job.FinishedAt = createdAt.Add(2 * time.Second)
		job.ExpiresAt = testNow.Add(time.Hour)
		job.Progress.UpdatedAt = job.FinishedAt
		job.Artifact = &exportjobs.Artifact{
			FileName:  id + ".csv",
			MediaType: "text/csv; charset=utf-8",
			SizeBytes: job.Progress.BytesWritten,
			RowCount:  job.Progress.RowsWritten,
			ExpiresAt: job.ExpiresAt,
		}
	case exportjobs.StateFailed:
		job.StartedAt = createdAt.Add(time.Second)
		job.FinishedAt = createdAt.Add(2 * time.Second)
		job.ExpiresAt = testNow.Add(time.Hour)
		job.Progress.UpdatedAt = job.FinishedAt
		job.Failure = &exportjobs.Failure{
			Code:    exportjobs.FailureInternal,
			Message: "export serialization failed",
		}
	case exportjobs.StateCanceled:
		job.FinishedAt = createdAt.Add(time.Second)
		job.ExpiresAt = testNow.Add(time.Hour)
		job.Progress.UpdatedAt = job.FinishedAt
	case exportjobs.StateExpired:
		job.StartedAt = createdAt.Add(time.Second)
		job.FinishedAt = createdAt.Add(2 * time.Second)
		job.ExpiresAt = testNow.Add(-time.Minute)
		job.Progress.UpdatedAt = testNow
	}
	return job
}

func exportListItems(jobs ...exportjobs.Job) []exportjobs.ListItem {
	return scopedExportListItems("tenant-list", "owner-list", jobs...)
}

func scopedExportListItems(
	tenantID string,
	ownerID string,
	jobs ...exportjobs.Job,
) []exportjobs.ListItem {
	items := make([]exportjobs.ListItem, len(jobs))
	for index, job := range jobs {
		items[index] = exportjobs.ListItem{
			Job:      job,
			TenantID: tenantID,
			OwnerID:  ownerID,
		}
	}
	return items
}
