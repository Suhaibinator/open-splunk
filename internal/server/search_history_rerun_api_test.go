package server

import (
	"context"
	"errors"
	"net/http"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/searchhistory"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type historyRerunIndexCatalog struct {
	mu sync.Mutex
	fakeIndexCatalog
	calls []string
}

func (catalog *historyRerunIndexCatalog) ListIndexes(ctx context.Context) ([]control.Index, error) {
	return catalog.fakeIndexCatalog.ListIndexes(ctx)
}

func (catalog *historyRerunIndexCatalog) GetIndexByName(
	ctx context.Context,
	name string,
) (control.Index, error) {
	catalog.mu.Lock()
	defer catalog.mu.Unlock()

	catalog.calls = append(catalog.calls, name)
	return catalog.fakeIndexCatalog.GetIndexByName(ctx, name)
}

func (catalog *historyRerunIndexCatalog) capturedCalls() []string {
	catalog.mu.Lock()
	defer catalog.mu.Unlock()
	return append([]string(nil), catalog.calls...)
}

func TestCreateSearchHistoryRerunUsesTrustedSnapshotAndFreshAdmission(t *testing.T) {
	t.Parallel()

	const (
		historyID = "history-original"
		ownerID   = "owner-1"
		tenantID  = "tenant-1"
		appID     = "app-main"
	)
	admissionTime := time.Date(2026, time.July, 24, 18, 30, 0, 0, time.UTC)

	for _, finalState := range []opensplunkv1.SearchJobState{
		opensplunkv1.SearchJobState_SEARCH_JOB_STATE_COMPLETED,
		opensplunkv1.SearchJobState_SEARCH_JOB_STATE_FAILED,
		opensplunkv1.SearchJobState_SEARCH_JOB_STATE_CANCELED,
		opensplunkv1.SearchJobState_SEARCH_JOB_STATE_EXPIRED,
	} {
		finalState := finalState
		t.Run(finalState.String(), func(t *testing.T) {
			t.Parallel()

			entry := historyRerunEntry(historyID, finalState)
			history := &fakeSearchHistory{getFn: func(
				_ context.Context,
				scope searchhistory.AccessScope,
				id string,
			) (*opensplunkv1.SearchHistoryEntry, error) {
				assertHistoryScope(t, scope, tenantID, ownerID)
				if id != historyID {
					t.Fatalf("history lookup ID = %q, want %q", id, historyID)
				}
				return entry, nil
			}}
			apps := activeHistoryRerunAppCatalog()
			indexes := activeHistoryRerunIndexCatalog("main")
			jobs := &fakeSearchJobs{createJob: completeJobForApp("history-rerun-new", appID)}
			handler := newTestHandler(t, Config{
				SearchJobs:    jobs,
				Indexes:       indexes,
				AppCatalog:    apps,
				SearchHistory: history,
				WebUI:         testUI(),
				OwnerID:       ownerID,
				TenantID:      tenantID,
				Now:           func() time.Time { return admissionTime },
			})

			response := postProto(
				t,
				handler,
				"/api/v1/search/jobs/create",
				historyRerunRequest("  "+historyID+"  "),
			)
			if response.Code != http.StatusOK {
				t.Fatalf(
					"history rerun status = %d, body = %s",
					response.Code,
					response.Body.String(),
				)
			}

			jobs.mu.Lock()
			captured := jobs.createRequest
			createCalls := jobs.createCalls
			jobs.mu.Unlock()
			if createCalls != 1 {
				t.Fatalf("history rerun created %d jobs, want 1", createCalls)
			}
			if captured.OwnerID != ownerID || captured.TenantID != tenantID {
				t.Fatalf("history rerun scope = owner %q tenant %q", captured.OwnerID, captured.TenantID)
			}
			if captured.SPL != entry.GetDefinition().GetSpl() ||
				captured.AppID != appID ||
				len(captured.RequestedIndexes) != 1 ||
				captured.RequestedIndexes[0] != "main" ||
				len(captured.AuthorizedIndexes) != 1 ||
				captured.AuthorizedIndexes[0] != "main" {
				t.Fatalf("history rerun did not use the trusted definition: %+v", captured)
			}
			if captured.Source.Origin != searchjobs.JobOriginHistoryRerun ||
				captured.Source.ObjectID != historyID {
				t.Fatalf("history rerun provenance = %+v", captured.Source)
			}

			intent := captured.TimeRange.Intent()
			if intent.Earliest != "-2h" || intent.Latest != "now" ||
				intent.Timezone != "UTC" || !intent.TimezoneSpecified ||
				!captured.TimeRange.Earliest().Equal(admissionTime.Add(-2*time.Hour)) ||
				!captured.TimeRange.Latest().Equal(admissionTime) {
				t.Fatalf(
					"history rerun time = intent %+v resolved [%s, %s)",
					intent,
					captured.TimeRange.Earliest(),
					captured.TimeRange.Latest(),
				)
			}
			if captured.TimeRange.Latest().Equal(entry.GetResolvedTimeRange().GetLatest().AsTime()) {
				t.Fatal("history rerun reused the original absolute time range")
			}

			if history.callCount() != 1 {
				t.Fatalf("history rerun performed %d history calls, want 1", history.callCount())
			}
			appCalls, appTenant, appMaximum := apps.captured()
			if appCalls != 1 || appTenant != tenantID || appMaximum != uint32(maximumBootstrapApps) {
				t.Fatalf(
					"history rerun app authorization = calls %d tenant %q maximum %d",
					appCalls,
					appTenant,
					appMaximum,
				)
			}
			if calls := indexes.capturedCalls(); len(calls) != 1 || calls[0] != "main" {
				t.Fatalf("history rerun index authorization calls = %v", calls)
			}
		})
	}
}

func TestCreateSearchHistoryRerunRejectsClientDefinition(t *testing.T) {
	t.Parallel()

	history := &fakeSearchHistory{getFn: func(
		context.Context,
		searchhistory.AccessScope,
		string,
	) (*opensplunkv1.SearchHistoryEntry, error) {
		t.Fatal("rejected client definition reached history storage")
		return nil, nil
	}}
	jobs := &fakeSearchJobs{createJob: completeJob("must-not-create")}
	handler := newTestHandler(t, Config{
		SearchJobs:    jobs,
		Indexes:       activeHistoryRerunIndexCatalog("main"),
		AppCatalog:    activeHistoryRerunAppCatalog(),
		SearchHistory: history,
		WebUI:         testUI(),
		OwnerID:       "owner-1",
		TenantID:      "tenant-1",
	})
	request := historyRerunRequest("history-original")
	request.Definition = createRequest("-30d", "now", "forged-index").Definition
	request.Definition.Spl = "index=forged-index | delete everything"
	request.Definition.AppId = new("forged-app")

	response := postProto(t, handler, "/api/v1/search/jobs/create", request)
	if response.Code != http.StatusBadRequest ||
		!strings.Contains(response.Body.String(), "history rerun cannot include a client search definition") {
		t.Fatalf(
			"forged history rerun status = %d, body = %s",
			response.Code,
			response.Body.String(),
		)
	}
	if history.callCount() != 0 {
		t.Fatalf("forged history rerun performed %d history calls", history.callCount())
	}
	assertHistoryRerunCreatedNoJobs(t, jobs)
}

func TestCreateSearchHistoryRerunUsesHistoryIDBoundary(t *testing.T) {
	t.Parallel()

	t.Run("exactly 256 bytes", func(t *testing.T) {
		t.Parallel()

		historyID := strings.Repeat("h", maximumHistorySearchJobIDBytes)
		history := &fakeSearchHistory{getFn: func(
			_ context.Context,
			scope searchhistory.AccessScope,
			id string,
		) (*opensplunkv1.SearchHistoryEntry, error) {
			assertHistoryScope(t, scope, "tenant-1", "owner-1")
			if id != historyID {
				t.Fatalf("history lookup ID has %d bytes, want %d", len(id), len(historyID))
			}
			return historyRerunEntry(historyID, opensplunkv1.SearchJobState_SEARCH_JOB_STATE_COMPLETED), nil
		}}
		jobs := &fakeSearchJobs{createJob: completeJobForApp("history-rerun-max-id", "app-main")}
		handler := newTestHandler(t, Config{
			SearchJobs:    jobs,
			Indexes:       activeHistoryRerunIndexCatalog("main"),
			AppCatalog:    activeHistoryRerunAppCatalog(),
			SearchHistory: history,
			WebUI:         testUI(),
			OwnerID:       "owner-1",
			TenantID:      "tenant-1",
		})

		response := postProto(
			t,
			handler,
			"/api/v1/search/jobs/create",
			historyRerunRequest(historyID),
		)
		if response.Code != http.StatusOK {
			t.Fatalf(
				"maximum history ID rerun status = %d, body = %s",
				response.Code,
				response.Body.String(),
			)
		}
		jobs.mu.Lock()
		captured := jobs.createRequest.Source
		jobs.mu.Unlock()
		if captured.Origin != searchjobs.JobOriginHistoryRerun || captured.ObjectID != historyID {
			t.Fatalf("maximum history ID provenance = %+v", captured)
		}
	})

	t.Run("257 bytes", func(t *testing.T) {
		t.Parallel()

		history := &fakeSearchHistory{getFn: func(
			context.Context,
			searchhistory.AccessScope,
			string,
		) (*opensplunkv1.SearchHistoryEntry, error) {
			t.Fatal("oversized history ID reached history storage")
			return nil, nil
		}}
		jobs := &fakeSearchJobs{createJob: completeJob("must-not-create")}
		handler := newTestHandler(t, Config{
			SearchJobs:    jobs,
			Indexes:       activeHistoryRerunIndexCatalog("main"),
			AppCatalog:    activeHistoryRerunAppCatalog(),
			SearchHistory: history,
			WebUI:         testUI(),
			OwnerID:       "owner-1",
			TenantID:      "tenant-1",
		})

		response := postProto(
			t,
			handler,
			"/api/v1/search/jobs/create",
			historyRerunRequest(strings.Repeat("h", maximumHistorySearchJobIDBytes+1)),
		)
		if response.Code != http.StatusBadRequest ||
			!strings.Contains(response.Body.String(), "search job ID is invalid") {
			t.Fatalf(
				"oversized history ID status = %d, body = %s",
				response.Code,
				response.Body.String(),
			)
		}
		if history.callCount() != 0 {
			t.Fatalf("oversized history ID performed %d history calls", history.callCount())
		}
		assertHistoryRerunCreatedNoJobs(t, jobs)
	})
}

func TestCreateSearchHistoryRerunUsesEffectiveScopeWithDefinitionFallback(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name            string
		spl             string
		definitionScope []string
		effectiveScope  []string
		wantScope       []string
	}{
		{
			name:            "effective scope is trusted over wider definition scope",
			spl:             "index=main | head 7",
			definitionScope: []string{"main", "stale-unused"},
			effectiveScope:  []string{"main"},
			wantScope:       []string{"main"},
		},
		{
			name:            "empty effective scope falls back to definition scope",
			spl:             "index=fallback | head 7",
			definitionScope: []string{"fallback"},
			wantScope:       []string{"fallback"},
		},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			entry := historyRerunEntry(
				"history-original",
				opensplunkv1.SearchJobState_SEARCH_JOB_STATE_COMPLETED,
			)
			entry.Definition.Spl = test.spl
			entry.Definition.IndexScope = slices.Clone(test.definitionScope)
			entry.EffectiveIndexScope = slices.Clone(test.effectiveScope)
			history := &fakeSearchHistory{getFn: func(
				context.Context,
				searchhistory.AccessScope,
				string,
			) (*opensplunkv1.SearchHistoryEntry, error) {
				return entry, nil
			}}
			indexes := activeHistoryRerunIndexCatalog(test.wantScope...)
			jobs := &fakeSearchJobs{createJob: completeJobForApp("history-rerun-scope", "app-main")}
			handler := newTestHandler(t, Config{
				SearchJobs:    jobs,
				Indexes:       indexes,
				AppCatalog:    activeHistoryRerunAppCatalog(),
				SearchHistory: history,
				WebUI:         testUI(),
				OwnerID:       "owner-1",
				TenantID:      "tenant-1",
			})

			response := postProto(
				t,
				handler,
				"/api/v1/search/jobs/create",
				historyRerunRequest("history-original"),
			)
			if response.Code != http.StatusOK {
				t.Fatalf(
					"history rerun scope status = %d, body = %s",
					response.Code,
					response.Body.String(),
				)
			}
			jobs.mu.Lock()
			captured := jobs.createRequest
			jobs.mu.Unlock()
			if !slices.Equal(captured.RequestedIndexes, test.wantScope) ||
				!slices.Equal(captured.AuthorizedIndexes, test.wantScope) {
				t.Fatalf(
					"history rerun scope = requested %v authorized %v, want %v",
					captured.RequestedIndexes,
					captured.AuthorizedIndexes,
					test.wantScope,
				)
			}
			if calls := indexes.capturedCalls(); !slices.Equal(calls, test.wantScope) {
				t.Fatalf("history rerun index authorization calls = %v, want %v", calls, test.wantScope)
			}
		})
	}
}

func TestCreateSearchHistoryRerunLookupFailuresCreateNoJob(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name          string
		get           func(context.Context, searchhistory.AccessScope, string) (*opensplunkv1.SearchHistoryEntry, error)
		wantStatus    int
		wantMessage   string
		forbiddenText string
	}{
		{
			name: "missing",
			get: func(context.Context, searchhistory.AccessScope, string) (*opensplunkv1.SearchHistoryEntry, error) {
				return nil, control.ErrNotFound
			},
			wantStatus:  http.StatusNotFound,
			wantMessage: "search history entry not found",
		},
		{
			name: "corrupt",
			get: func(context.Context, searchhistory.AccessScope, string) (*opensplunkv1.SearchHistoryEntry, error) {
				return nil, errors.New("checksum mismatch at secret storage path")
			},
			wantStatus:    http.StatusServiceUnavailable,
			wantMessage:   "search history service is unavailable",
			forbiddenText: "checksum mismatch",
		},
		{
			name: "canceled lookup",
			get: func(context.Context, searchhistory.AccessScope, string) (*opensplunkv1.SearchHistoryEntry, error) {
				return nil, context.Canceled
			},
			wantStatus:  http.StatusRequestTimeout,
			wantMessage: "history rerun request was canceled",
		},
		{
			name: "malformed service result",
			get: func(context.Context, searchhistory.AccessScope, string) (*opensplunkv1.SearchHistoryEntry, error) {
				entry := historyRerunEntry("different-history", opensplunkv1.SearchJobState_SEARCH_JOB_STATE_COMPLETED)
				return entry, nil
			},
			wantStatus:  http.StatusInternalServerError,
			wantMessage: "internal server error",
		},
		{
			name: "corrupt stored definition",
			get: func(context.Context, searchhistory.AccessScope, string) (*opensplunkv1.SearchHistoryEntry, error) {
				entry := historyRerunEntry("history-original", opensplunkv1.SearchJobState_SEARCH_JOB_STATE_COMPLETED)
				entry.Definition = nil
				return entry, nil
			},
			wantStatus:  http.StatusInternalServerError,
			wantMessage: "internal server error",
		},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			history := &fakeSearchHistory{getFn: func(
				ctx context.Context,
				scope searchhistory.AccessScope,
				id string,
			) (*opensplunkv1.SearchHistoryEntry, error) {
				assertHistoryScope(t, scope, "tenant-1", "owner-1")
				if id != "history-original" {
					t.Fatalf("history lookup ID = %q", id)
				}
				return test.get(ctx, scope, id)
			}}
			jobs := &fakeSearchJobs{createJob: completeJob("must-not-create")}
			handler := newTestHandler(t, Config{
				SearchJobs:    jobs,
				Indexes:       activeHistoryRerunIndexCatalog("main"),
				AppCatalog:    activeHistoryRerunAppCatalog(),
				SearchHistory: history,
				WebUI:         testUI(),
				OwnerID:       "owner-1",
				TenantID:      "tenant-1",
			})

			response := postProto(
				t,
				handler,
				"/api/v1/search/jobs/create",
				historyRerunRequest("history-original"),
			)
			if response.Code != test.wantStatus ||
				!strings.Contains(response.Body.String(), test.wantMessage) {
				t.Fatalf(
					"history lookup failure status = %d, body = %s",
					response.Code,
					response.Body.String(),
				)
			}
			if test.forbiddenText != "" && strings.Contains(response.Body.String(), test.forbiddenText) {
				t.Fatalf("history lookup leaked storage detail: %s", response.Body.String())
			}
			if history.callCount() != 1 {
				t.Fatalf("history lookup calls = %d, want 1", history.callCount())
			}
			assertHistoryRerunCreatedNoJobs(t, jobs)
		})
	}
}

func TestCreateSearchHistoryRerunReauthorizesAppAndIndexes(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name    string
		apps    *fakeBootstrapAppCatalog
		indexes *historyRerunIndexCatalog
	}{
		{
			name: "app is no longer active",
			apps: &fakeBootstrapAppCatalog{result: AppCatalogResult{
				Complete: true,
			}},
			indexes: activeHistoryRerunIndexCatalog("main"),
		},
		{
			name:    "index was deleted",
			apps:    activeHistoryRerunAppCatalog(),
			indexes: &historyRerunIndexCatalog{},
		},
		{
			name: "index is no longer searchable",
			apps: activeHistoryRerunAppCatalog(),
			indexes: &historyRerunIndexCatalog{fakeIndexCatalog: fakeIndexCatalog{indexes: []control.Index{
				{
					ID: "index-main",
					Definition: control.IndexDefinition{
						Name:          "main",
						DisplayName:   "Main",
						SearchEnabled: false,
					},
					State: control.IndexStateActive,
				},
			}}},
		},
		{
			name: "index was archived",
			apps: activeHistoryRerunAppCatalog(),
			indexes: &historyRerunIndexCatalog{fakeIndexCatalog: fakeIndexCatalog{indexes: []control.Index{
				{
					ID: "index-main",
					Definition: control.IndexDefinition{
						Name:          "main",
						DisplayName:   "Main",
						SearchEnabled: true,
					},
					State: control.IndexStateArchived,
				},
			}}},
		},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			history := &fakeSearchHistory{getFn: func(
				context.Context,
				searchhistory.AccessScope,
				string,
			) (*opensplunkv1.SearchHistoryEntry, error) {
				return historyRerunEntry(
					"history-original",
					opensplunkv1.SearchJobState_SEARCH_JOB_STATE_COMPLETED,
				), nil
			}}
			jobs := &fakeSearchJobs{createJob: completeJob("must-not-create")}
			handler := newTestHandler(t, Config{
				SearchJobs:    jobs,
				Indexes:       test.indexes,
				AppCatalog:    test.apps,
				SearchHistory: history,
				WebUI:         testUI(),
				OwnerID:       "owner-1",
				TenantID:      "tenant-1",
			})

			response := postProto(
				t,
				handler,
				"/api/v1/search/jobs/create",
				historyRerunRequest("history-original"),
			)
			if response.Code != http.StatusForbidden {
				t.Fatalf(
					"stale history authorization status = %d, body = %s",
					response.Code,
					response.Body.String(),
				)
			}
			assertHistoryRerunCreatedNoJobs(t, jobs)
		})
	}
}

func historyRerunRequest(historyID string) *opensplunkv1.CreateSearchJobRequest {
	return &opensplunkv1.CreateSearchJobRequest{Source: &opensplunkv1.SearchJobSource{
		Origin:          opensplunkv1.SearchJobOrigin_SEARCH_JOB_ORIGIN_HISTORY_RERUN,
		HistorySearchId: new(historyID),
	}}
}

func historyRerunEntry(
	historyID string,
	finalState opensplunkv1.SearchJobState,
) *opensplunkv1.SearchHistoryEntry {
	appID := "app-main"
	timezone := "UTC"
	savedSearchID := "saved-original"
	originalLatest := time.Date(2026, time.July, 20, 12, 0, 0, 0, time.UTC)
	originalCreated := originalLatest.Add(-time.Minute)
	return &opensplunkv1.SearchHistoryEntry{
		SearchJobId: historyID,
		Definition: &opensplunkv1.SearchDefinition{
			Spl: "  index=main ERROR | head 7\n",
			TimeRange: &opensplunkv1.TimeRangeSpec{
				Earliest: new("-2h"),
				Latest:   new("now"),
				Timezone: &timezone,
			},
			AppId:      &appID,
			IndexScope: []string{"main"},
		},
		Source: &opensplunkv1.SearchJobSource{
			Origin:        opensplunkv1.SearchJobOrigin_SEARCH_JOB_ORIGIN_SAVED_SEARCH,
			SavedSearchId: &savedSearchID,
		},
		EffectiveIndexScope: []string{"main"},
		ResolvedTimeRange: &opensplunkv1.ResolvedTimeRange{
			Earliest: timestamppb.New(originalLatest.Add(-2 * time.Hour)),
			Latest:   timestamppb.New(originalLatest),
			Timezone: timezone,
		},
		FinalState:      finalState,
		MatchedEvents:   7,
		ScannedRows:     11,
		ScannedBytes:    1_024,
		ProducedRows:    7,
		Duration:        durationpb.New(3 * time.Second),
		CompilerVersion: "test",
		CreatedAt:       timestamppb.New(originalCreated),
		StartedAt:       timestamppb.New(originalCreated.Add(time.Second)),
		FinishedAt:      timestamppb.New(originalCreated.Add(4 * time.Second)),
	}
}

func activeHistoryRerunAppCatalog() *fakeBootstrapAppCatalog {
	return &fakeBootstrapAppCatalog{result: AppCatalogResult{
		Complete: true,
		Apps: []AppCatalogSummary{
			validBootstrapCatalogApp("app-main", "main", "Main", "main"),
		},
	}}
}

func activeHistoryRerunIndexCatalog(names ...string) *historyRerunIndexCatalog {
	indexes := make([]control.Index, 0, len(names))
	for _, name := range names {
		indexes = append(indexes, validationTestIndex(name))
	}
	return &historyRerunIndexCatalog{fakeIndexCatalog: fakeIndexCatalog{indexes: indexes}}
}

func assertHistoryRerunCreatedNoJobs(t *testing.T, jobs *fakeSearchJobs) {
	t.Helper()
	jobs.mu.Lock()
	defer jobs.mu.Unlock()
	if jobs.createCalls != 0 {
		t.Fatalf("failed history rerun created %d jobs", jobs.createCalls)
	}
}
