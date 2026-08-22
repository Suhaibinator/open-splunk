package server

import (
	"context"
	"errors"
	"net/http"
	"slices"
	"strings"
	"testing"
	"time"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/searchhistory"
)

func TestCreateSearchHistoryRerunRejectsMalformedSourceShapes(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name        string
		request     func() *opensplunk.CreateSearchJobRequest
		wantMessage string
	}{
		{
			name: "missing history ID",
			request: func() *opensplunk.CreateSearchJobRequest {
				return &opensplunk.CreateSearchJobRequest{Source: &opensplunk.SearchJobSource{
					Origin: opensplunk.SearchJobOrigin_SEARCH_JOB_ORIGIN_HISTORY_RERUN,
				}}
			},
			wantMessage: "history-rerun origin requires a history search ID",
		},
		{
			name: "empty history ID",
			request: func() *opensplunk.CreateSearchJobRequest {
				return historyRerunRequest("")
			},
			wantMessage: "search job ID is invalid",
		},
		{
			name: "saved-search ID also supplied",
			request: func() *opensplunk.CreateSearchJobRequest {
				request := historyRerunRequest("history-original")
				request.Source.SavedSearchId = new("saved-forged")
				return request
			},
			wantMessage: "history-rerun origin requires a history search ID",
		},
		{
			name: "dashboard ID also supplied",
			request: func() *opensplunk.CreateSearchJobRequest {
				request := historyRerunRequest("history-original")
				request.Source.DashboardId = new("dashboard-forged")
				return request
			},
			wantMessage: "history-rerun origin requires a history search ID",
		},
		{
			name: "ad-hoc origin with history ID",
			request: func() *opensplunk.CreateSearchJobRequest {
				request := createRequest("-1h", "now", "main")
				request.Source = &opensplunk.SearchJobSource{
					Origin:          opensplunk.SearchJobOrigin_SEARCH_JOB_ORIGIN_AD_HOC,
					HistorySearchId: new("history-forged"),
				}
				return request
			},
			wantMessage: "ad-hoc search source cannot include an object ID",
		},
		{
			name: "saved-search origin with history ID",
			request: func() *opensplunk.CreateSearchJobRequest {
				request := createRequest("-1h", "now", "main")
				request.Source = &opensplunk.SearchJobSource{
					Origin:          opensplunk.SearchJobOrigin_SEARCH_JOB_ORIGIN_SAVED_SEARCH,
					HistorySearchId: new("history-forged"),
				}
				return request
			},
			wantMessage: "search job source metadata is invalid or unsupported",
		},
		{
			name: "unspecified origin with history ID",
			request: func() *opensplunk.CreateSearchJobRequest {
				request := createRequest("-1h", "now", "main")
				request.Source = &opensplunk.SearchJobSource{
					HistorySearchId: new("history-forged"),
				}
				return request
			},
			wantMessage: "search job source metadata is invalid or unsupported",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			history := &fakeSearchHistory{getFn: func(
				context.Context,
				searchhistory.AccessScope,
				string,
			) (*opensplunk.SearchHistoryEntry, error) {
				t.Fatal("malformed source reached history storage")
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
				"/api/search/jobs/create",
				test.request(),
			)
			if response.Code != http.StatusBadRequest ||
				!strings.Contains(response.Body.String(), test.wantMessage) {
				t.Fatalf(
					"malformed source status = %d, body = %s",
					response.Code,
					response.Body.String(),
				)
			}
			if history.callCount() != 0 {
				t.Fatalf("malformed source performed %d history calls", history.callCount())
			}
			assertHistoryRerunCreatedNoJobs(t, jobs)
		})
	}
}

func TestCreateSearchHistoryRerunRejectsRequestOptionsBeforeLookup(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name        string
		mutate      func(*opensplunk.CreateSearchJobRequest)
		wantMessage string
	}{
		{
			name: "client request ID",
			mutate: func(request *opensplunk.CreateSearchJobRequest) {
				request.ClientRequestId = new("client-1")
			},
			wantMessage: "client request idempotency is not supported",
		},
		{
			name: "eager field discovery",
			mutate: func(request *opensplunk.CreateSearchJobRequest) {
				request.Options = &opensplunk.SearchJobOptions{EnableFieldDiscovery: true}
			},
			wantMessage: "eager field discovery and timeline options are not supported",
		},
		{
			name: "eager timeline",
			mutate: func(request *opensplunk.CreateSearchJobRequest) {
				request.Options = &opensplunk.SearchJobOptions{EnableTimeline: true}
			},
			wantMessage: "eager field discovery and timeline options are not supported",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			history := &fakeSearchHistory{getFn: func(
				context.Context,
				searchhistory.AccessScope,
				string,
			) (*opensplunk.SearchHistoryEntry, error) {
				t.Fatal("forbidden request option reached history storage")
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
			test.mutate(request)

			response := postProto(
				t,
				handler,
				"/api/search/jobs/create",
				request,
			)
			if response.Code != http.StatusBadRequest ||
				!strings.Contains(response.Body.String(), test.wantMessage) {
				t.Fatalf(
					"forbidden option status = %d, body = %s",
					response.Code,
					response.Body.String(),
				)
			}
			if history.callCount() != 0 {
				t.Fatalf("forbidden option performed %d history calls", history.callCount())
			}
			assertHistoryRerunCreatedNoJobs(t, jobs)
		})
	}
}

func TestCreateSearchHistoryRerunWithoutHistoryServiceIsUnsupported(t *testing.T) {
	t.Parallel()

	jobs := &fakeSearchJobs{createJob: completeJob("must-not-create")}
	handler := newTestHandler(t, Config{
		SearchJobs: jobs,
		Indexes:    activeHistoryRerunIndexCatalog("main"),
		AppCatalog: activeHistoryRerunAppCatalog(),
		WebUI:      testUI(),
		OwnerID:    "owner-1",
		TenantID:   "tenant-1",
	})

	response := postProto(
		t,
		handler,
		"/api/search/jobs/create",
		historyRerunRequest("history-original"),
	)
	if response.Code != http.StatusBadRequest ||
		!strings.Contains(response.Body.String(), "history rerun is not supported") {
		t.Fatalf(
			"history rerun without service status = %d, body = %s",
			response.Code,
			response.Body.String(),
		)
	}
	assertHistoryRerunCreatedNoJobs(t, jobs)
}

func TestCreateSearchHistoryRerunAuthorizesLegacyAndStaticApps(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name       string
		mutate     func(*opensplunk.SearchHistoryEntry)
		bootstrap  BootstrapConfig
		wantStatus int
		wantAppID  string
	}{
		{
			name: "legacy absent app",
			mutate: func(entry *opensplunk.SearchHistoryEntry) {
				entry.Definition.AppId = nil
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "legacy explicit empty app",
			mutate: func(entry *opensplunk.SearchHistoryEntry) {
				entry.Definition.AppId = new("")
			},
			wantStatus: http.StatusOK,
		},
		{
			name:   "matching static app",
			mutate: func(*opensplunk.SearchHistoryEntry) {},
			bootstrap: BootstrapConfig{Apps: []*opensplunk.AppSummary{{
				AppId:       "app-main",
				Slug:        "main",
				DisplayName: "Main",
				State:       opensplunk.AppState_APP_STATE_ACTIVE,
			}}},
			wantStatus: http.StatusOK,
			wantAppID:  "app-main",
		},
		{
			name:   "missing static app",
			mutate: func(*opensplunk.SearchHistoryEntry) {},
			bootstrap: BootstrapConfig{Apps: []*opensplunk.AppSummary{{
				AppId:       "app-other",
				Slug:        "other",
				DisplayName: "Other",
				State:       opensplunk.AppState_APP_STATE_ACTIVE,
			}}},
			wantStatus: http.StatusForbidden,
		},
		{
			name:   "matching archived static app",
			mutate: func(*opensplunk.SearchHistoryEntry) {},
			bootstrap: BootstrapConfig{Apps: []*opensplunk.AppSummary{{
				AppId:       "app-main",
				Slug:        "main",
				DisplayName: "Main",
				State:       opensplunk.AppState_APP_STATE_ARCHIVED,
			}}},
			wantStatus: http.StatusForbidden,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			entry := historyRerunEntry(
				"history-original",
				opensplunk.SearchJobState_SEARCH_JOB_STATE_COMPLETED,
			)
			test.mutate(entry)
			history := &fakeSearchHistory{getFn: func(
				context.Context,
				searchhistory.AccessScope,
				string,
			) (*opensplunk.SearchHistoryEntry, error) {
				return entry, nil
			}}
			jobs := &fakeSearchJobs{createJob: completeJobForApp("history-rerun-app", test.wantAppID)}
			handler := newTestHandler(t, Config{
				SearchJobs:    jobs,
				Indexes:       activeHistoryRerunIndexCatalog("main"),
				SearchHistory: history,
				WebUI:         testUI(),
				Bootstrap:     test.bootstrap,
				OwnerID:       "owner-1",
				TenantID:      "tenant-1",
			})

			response := postProto(
				t,
				handler,
				"/api/search/jobs/create",
				historyRerunRequest("history-original"),
			)
			if response.Code != test.wantStatus {
				t.Fatalf(
					"legacy/static app status = %d, want %d, body = %s",
					response.Code,
					test.wantStatus,
					response.Body.String(),
				)
			}
			if test.wantStatus != http.StatusOK {
				assertHistoryRerunCreatedNoJobs(t, jobs)
				return
			}
			jobs.mu.Lock()
			createdAppID := jobs.createRequest.AppID
			createCalls := jobs.createCalls
			jobs.mu.Unlock()
			if createCalls != 1 || createdAppID != test.wantAppID {
				t.Fatalf(
					"legacy/static app create = calls %d app %q, want app %q",
					createCalls,
					createdAppID,
					test.wantAppID,
				)
			}
		})
	}
}

func TestCreateSearchHistoryRerunMapsLiveAppCatalogFailures(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name          string
		catalog       *fakeBootstrapAppCatalog
		wantStatus    int
		wantMessage   string
		forbiddenText string
	}{
		{
			name: "storage error",
			catalog: &fakeBootstrapAppCatalog{
				err: errors.New("SELECT secret FROM app_workspaces"),
			},
			wantStatus:    http.StatusServiceUnavailable,
			wantMessage:   "control plane is unavailable",
			forbiddenText: "SELECT secret",
		},
		{
			name: "incomplete catalog",
			catalog: &fakeBootstrapAppCatalog{result: AppCatalogResult{
				Complete: false,
				Apps: []AppCatalogSummary{{
					AppID: "app-main", Slug: "main", DisplayName: "Main",
				}},
			}},
			wantStatus:  http.StatusServiceUnavailable,
			wantMessage: "control plane is unavailable",
		},
		{
			name: "malformed catalog",
			catalog: &fakeBootstrapAppCatalog{result: AppCatalogResult{
				Complete: true,
				Apps: []AppCatalogSummary{{
					AppID: " app-main ", Slug: "main", DisplayName: "Main",
				}},
			}},
			wantStatus:  http.StatusInternalServerError,
			wantMessage: "internal server error",
		},
		{
			name:        "canceled catalog",
			catalog:     &fakeBootstrapAppCatalog{err: context.Canceled},
			wantStatus:  http.StatusRequestTimeout,
			wantMessage: "history rerun request was canceled",
		},
		{
			name:        "deadline-exceeded catalog",
			catalog:     &fakeBootstrapAppCatalog{err: context.DeadlineExceeded},
			wantStatus:  http.StatusRequestTimeout,
			wantMessage: "history rerun request was canceled",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			history := &fakeSearchHistory{getFn: func(
				context.Context,
				searchhistory.AccessScope,
				string,
			) (*opensplunk.SearchHistoryEntry, error) {
				return historyRerunEntry(
					"history-original",
					opensplunk.SearchJobState_SEARCH_JOB_STATE_COMPLETED,
				), nil
			}}
			indexes := activeHistoryRerunIndexCatalog("main")
			jobs := &fakeSearchJobs{createJob: completeJob("must-not-create")}
			handler := newTestHandler(t, Config{
				SearchJobs:    jobs,
				Indexes:       indexes,
				AppCatalog:    test.catalog,
				SearchHistory: history,
				WebUI:         testUI(),
				OwnerID:       "owner-1",
				TenantID:      "tenant-1",
			})

			response := postProto(
				t,
				handler,
				"/api/search/jobs/create",
				historyRerunRequest("history-original"),
			)
			if response.Code != test.wantStatus ||
				!strings.Contains(response.Body.String(), test.wantMessage) {
				t.Fatalf(
					"app catalog failure status = %d, body = %s",
					response.Code,
					response.Body.String(),
				)
			}
			if test.forbiddenText != "" && strings.Contains(response.Body.String(), test.forbiddenText) {
				t.Fatalf("app catalog failure leaked storage detail: %s", response.Body.String())
			}
			if calls, tenant, maximum := test.catalog.captured(); calls != 1 || tenant != "tenant-1" || maximum != uint32(maximumBootstrapApps) {
				t.Fatalf(
					"app catalog calls = %d tenant %q maximum %d",
					calls,
					tenant,
					maximum,
				)
			}
			assertHistoryRerunCreatedNoJobs(t, jobs)
		})
	}
}

func TestCreateSearchHistoryRerunRejectsMalformedTrustedDefinition(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name   string
		mutate func(*opensplunk.SearchHistoryEntry)
	}{
		{
			name: "missing time intent",
			mutate: func(entry *opensplunk.SearchHistoryEntry) {
				entry.Definition.TimeRange = nil
			},
		},
		{
			name: "missing earliest time intent",
			mutate: func(entry *opensplunk.SearchHistoryEntry) {
				entry.Definition.TimeRange.Earliest = nil
			},
		},
		{
			name: "padded time intent",
			mutate: func(entry *opensplunk.SearchHistoryEntry) {
				entry.Definition.TimeRange.Earliest = new(" -2h ")
			},
		},
		{
			name: "invalid canonical time expression",
			mutate: func(entry *opensplunk.SearchHistoryEntry) {
				entry.Definition.TimeRange.Earliest = new("yesterday-ish")
			},
		},
		{
			name: "padded timezone",
			mutate: func(entry *opensplunk.SearchHistoryEntry) {
				entry.Definition.TimeRange.Timezone = new(" UTC ")
			},
		},
		{
			name: "invalid canonical timezone",
			mutate: func(entry *opensplunk.SearchHistoryEntry) {
				entry.Definition.TimeRange.Timezone = new("Mars/Olympus")
			},
		},
		{
			name: "padded app ID",
			mutate: func(entry *opensplunk.SearchHistoryEntry) {
				entry.Definition.AppId = new(" app-main ")
			},
		},
		{
			name: "duplicate requested index",
			mutate: func(entry *opensplunk.SearchHistoryEntry) {
				entry.Definition.IndexScope = []string{"main", "main"}
			},
		},
		{
			name: "duplicate effective index",
			mutate: func(entry *opensplunk.SearchHistoryEntry) {
				entry.EffectiveIndexScope = []string{"main", "main"}
			},
		},
		{
			name: "effective scope widens requested scope",
			mutate: func(entry *opensplunk.SearchHistoryEntry) {
				entry.Definition.IndexScope = []string{"main"}
				entry.EffectiveIndexScope = []string{"main", "other"}
			},
		},
		{
			name: "empty reusable index scope",
			mutate: func(entry *opensplunk.SearchHistoryEntry) {
				entry.Definition.IndexScope = nil
				entry.EffectiveIndexScope = nil
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			entry := historyRerunEntry(
				"history-original",
				opensplunk.SearchJobState_SEARCH_JOB_STATE_COMPLETED,
			)
			test.mutate(entry)
			history := &fakeSearchHistory{getFn: func(
				context.Context,
				searchhistory.AccessScope,
				string,
			) (*opensplunk.SearchHistoryEntry, error) {
				return entry, nil
			}}
			indexes := activeHistoryRerunIndexCatalog("main", "other")
			apps := activeHistoryRerunAppCatalog()
			jobs := &fakeSearchJobs{createJob: completeJob("must-not-create")}
			handler := newTestHandler(t, Config{
				SearchJobs:    jobs,
				Indexes:       indexes,
				AppCatalog:    apps,
				SearchHistory: history,
				WebUI:         testUI(),
				OwnerID:       "owner-1",
				TenantID:      "tenant-1",
			})

			response := postProto(
				t,
				handler,
				"/api/search/jobs/create",
				historyRerunRequest("history-original"),
			)
			if response.Code != http.StatusInternalServerError ||
				!strings.Contains(response.Body.String(), "internal server error") {
				t.Fatalf(
					"malformed trusted definition status = %d, body = %s",
					response.Code,
					response.Body.String(),
				)
			}
			assertHistoryRerunCreatedNoJobs(t, jobs)
		})
	}
}

func TestCreateSearchHistoryRerunMapsIndexCatalogFailures(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name          string
		err           error
		wantStatus    int
		wantMessage   string
		forbiddenText string
	}{
		{
			name:          "storage error",
			err:           errors.New("SELECT secret FROM indexes"),
			wantStatus:    http.StatusServiceUnavailable,
			wantMessage:   "control plane is unavailable",
			forbiddenText: "SELECT secret",
		},
		{
			name:        "canceled catalog",
			err:         context.Canceled,
			wantStatus:  http.StatusRequestTimeout,
			wantMessage: "history rerun request was canceled",
		},
		{
			name:        "deadline-exceeded catalog",
			err:         context.DeadlineExceeded,
			wantStatus:  http.StatusRequestTimeout,
			wantMessage: "history rerun request was canceled",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			history := &fakeSearchHistory{getFn: func(
				context.Context,
				searchhistory.AccessScope,
				string,
			) (*opensplunk.SearchHistoryEntry, error) {
				return historyRerunEntry(
					"history-original",
					opensplunk.SearchJobState_SEARCH_JOB_STATE_COMPLETED,
				), nil
			}}
			indexes := &historyRerunIndexCatalog{
				indexes: slices.Clone(
					activeHistoryRerunIndexCatalog("main").indexes,
				),
				err: test.err,
			}
			jobs := &fakeSearchJobs{createJob: completeJob("must-not-create")}
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
				"/api/search/jobs/create",
				historyRerunRequest("history-original"),
			)
			if response.Code != test.wantStatus ||
				(test.wantMessage != "" && !strings.Contains(response.Body.String(), test.wantMessage)) {
				t.Fatalf(
					"index catalog failure status = %d, body = %s",
					response.Code,
					response.Body.String(),
				)
			}
			if test.forbiddenText != "" && strings.Contains(response.Body.String(), test.forbiddenText) {
				t.Fatalf("index catalog failure leaked storage detail: %s", response.Body.String())
			}
			if calls := indexes.capturedCalls(); !slices.Equal(calls, []string{"main"}) {
				t.Fatalf("index catalog calls = %v, want [main]", calls)
			}
			assertHistoryRerunCreatedNoJobs(t, jobs)
		})
	}
}

func TestCreateSearchHistoryRerunMapsJobAdmissionCancellation(t *testing.T) {
	t.Parallel()

	for _, operationErr := range []error{
		context.Canceled,
		context.DeadlineExceeded,
	} {
		t.Run(operationErr.Error(), func(t *testing.T) {
			t.Parallel()

			history := &fakeSearchHistory{getFn: func(
				context.Context,
				searchhistory.AccessScope,
				string,
			) (*opensplunk.SearchHistoryEntry, error) {
				return historyRerunEntry(
					"history-original",
					opensplunk.SearchJobState_SEARCH_JOB_STATE_COMPLETED,
				), nil
			}}
			jobs := &fakeSearchJobs{createErr: operationErr}
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
				"/api/search/jobs/create",
				historyRerunRequest("history-original"),
			)
			if response.Code != http.StatusRequestTimeout ||
				!strings.Contains(response.Body.String(), "history rerun request was canceled") {
				t.Fatalf(
					"job admission cancellation status = %d, body = %s",
					response.Code,
					response.Body.String(),
				)
			}
			jobs.mu.Lock()
			createCalls := jobs.createCalls
			jobs.mu.Unlock()
			if createCalls != 1 {
				t.Fatalf("job admission cancellation reached Create %d times, want 1", createCalls)
			}
		})
	}
}

func TestCreateSearchHistoryRerunResolvesNamedZoneDayIntentAtFreshAdmission(t *testing.T) {
	t.Parallel()

	location, err := time.LoadLocation("America/Los_Angeles")
	if err != nil {
		t.Fatal(err)
	}
	admissionTime := time.Date(2026, time.July, 24, 18, 30, 0, 0, time.UTC)
	entry := historyRerunEntry(
		"history-original",
		opensplunk.SearchJobState_SEARCH_JOB_STATE_COMPLETED,
	)
	entry.Definition.TimeRange.Earliest = new("-1d@d")
	entry.Definition.TimeRange.Latest = new("@d")
	entry.Definition.TimeRange.Timezone = new("America/Los_Angeles")
	history := &fakeSearchHistory{getFn: func(
		context.Context,
		searchhistory.AccessScope,
		string,
	) (*opensplunk.SearchHistoryEntry, error) {
		return entry, nil
	}}
	jobs := &fakeSearchJobs{createJob: completeJobForApp("history-rerun-named-zone", "app-main")}
	handler := newTestHandler(t, Config{
		SearchJobs:    jobs,
		Indexes:       activeHistoryRerunIndexCatalog("main"),
		AppCatalog:    activeHistoryRerunAppCatalog(),
		SearchHistory: history,
		WebUI:         testUI(),
		OwnerID:       "owner-1",
		TenantID:      "tenant-1",
		Now:           func() time.Time { return admissionTime },
	})

	response := postProto(
		t,
		handler,
		"/api/search/jobs/create",
		historyRerunRequest("history-original"),
	)
	if response.Code != http.StatusOK {
		t.Fatalf(
			"named-zone history rerun status = %d, body = %s",
			response.Code,
			response.Body.String(),
		)
	}
	jobs.mu.Lock()
	resolved := jobs.createRequest.TimeRange
	jobs.mu.Unlock()
	intent := resolved.Intent()
	wantEarliest := time.Date(2026, time.July, 23, 0, 0, 0, 0, location).UTC()
	wantLatest := time.Date(2026, time.July, 24, 0, 0, 0, 0, location).UTC()
	if intent.Earliest != "-1d@d" || intent.Latest != "@d" ||
		intent.Timezone != "America/Los_Angeles" || !intent.TimezoneSpecified ||
		!resolved.Earliest().Equal(wantEarliest) || !resolved.Latest().Equal(wantLatest) {
		t.Fatalf(
			"named-zone rerun = intent %+v resolved [%s, %s), want [%s, %s)",
			intent,
			resolved.Earliest(),
			resolved.Latest(),
			wantEarliest,
			wantLatest,
		)
	}
	if resolved.Latest().Equal(entry.GetResolvedTimeRange().GetLatest().AsTime()) {
		t.Fatal("named-zone history rerun reused the original absolute range")
	}
}

func TestCreateSearchHistoryRerunRejectsTimeIntentThatExpiredAgainstFreshClock(t *testing.T) {
	t.Parallel()

	entry := historyRerunEntry(
		"history-original",
		opensplunk.SearchJobState_SEARCH_JOB_STATE_COMPLETED,
	)
	entry.Definition.TimeRange.Earliest = new("now")
	entry.Definition.TimeRange.Latest = new("2026-07-24T18:00:00Z")
	history := &fakeSearchHistory{getFn: func(
		context.Context,
		searchhistory.AccessScope,
		string,
	) (*opensplunk.SearchHistoryEntry, error) {
		return entry, nil
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
		Now: func() time.Time {
			return time.Date(2026, time.July, 24, 18, 30, 0, 0, time.UTC)
		},
	})

	response := postProto(
		t,
		handler,
		"/api/search/jobs/create",
		historyRerunRequest("history-original"),
	)
	if response.Code != http.StatusConflict ||
		!strings.Contains(
			response.Body.String(),
			"retained search time range is not executable at the current server time",
		) {
		t.Fatalf(
			"expired retained time intent status = %d, body = %s",
			response.Code,
			response.Body.String(),
		)
	}
	assertHistoryRerunCreatedNoJobs(t, jobs)
}
