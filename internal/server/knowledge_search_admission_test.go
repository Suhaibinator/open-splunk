package server

import (
	"bytes"
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
	"google.golang.org/protobuf/proto"
)

type knowledgeAdmissionSearchJobs struct {
	*fakeSearchJobs
	enabled          bool
	executionEnabled bool
}

func (jobs *knowledgeAdmissionSearchJobs) KnowledgeAdmissionEnabled() bool {
	return jobs != nil && jobs.enabled
}

func (jobs *knowledgeAdmissionSearchJobs) KnowledgeExecutionEnabled() bool {
	return jobs != nil && jobs.executionEnabled
}

func TestKnowledgeSearchAdmissionRequiresLiveAppCatalog(t *testing.T) {
	t.Parallel()

	jobs := &knowledgeAdmissionSearchJobs{
		fakeSearchJobs: &fakeSearchJobs{},
		enabled:        true,
	}
	_, err := NewHandler(Config{
		SearchJobs:    jobs,
		Indexes:       fakeIndexCatalog{},
		SavedSearches: &fakeSavedSearches{},
		WebUI:         testUI(),
	})
	if err == nil || !strings.Contains(err.Error(), "requires a live app catalog") {
		t.Fatalf("NewHandler(knowledge admission without live apps) error = %v", err)
	}

	jobs.enabled = false
	if handler, err := NewHandler(Config{
		SearchJobs:    jobs,
		Indexes:       fakeIndexCatalog{},
		SavedSearches: &fakeSavedSearches{},
		WebUI:         testUI(),
	}); err != nil || handler == nil {
		t.Fatalf("NewHandler(disabled knowledge admission) = (%v, %v)", handler, err)
	}
}

func TestKnowledgeSearchAdmissionAuthorizesCurrentAppBeforeIndexesAndCreate(t *testing.T) {
	t.Parallel()

	const appID = "app-main"
	job := completeJob("knowledge-admission-job")
	job.AppID = appID
	job.KnowledgeSnapshot = enabledEmptyKnowledgeSnapshotSummary()
	jobs := &knowledgeAdmissionSearchJobs{
		fakeSearchJobs: &fakeSearchJobs{createJob: job},
		enabled:        true,
	}
	apps := activeHistoryRerunAppCatalog(appID)
	indexes := activeHistoryRerunIndexCatalog("main")
	handler := newTestHandler(t, Config{
		SearchJobs: jobs,
		Indexes:    indexes,
		AppCatalog: apps,
		WebUI:      testUI(),
		OwnerID:    "owner-1",
		TenantID:   "tenant-1",
	})
	request := createRequest("-1h", "now", "main")
	request.Definition.AppId = stringPointer(appID)
	response := postProto(t, handler, "/api/v1/search/jobs/create", request)
	if response.Code != http.StatusOK {
		t.Fatalf("knowledge admission status = %d, body = %s", response.Code, response.Body.String())
	}

	appCalls, tenantID, maximum := apps.captured()
	if appCalls != 1 || tenantID != "tenant-1" || maximum != uint32(maximumBootstrapApps) {
		t.Fatalf("app authorization = calls %d tenant %q maximum %d", appCalls, tenantID, maximum)
	}
	if calls := indexes.capturedCalls(); len(calls) != 1 || calls[0] != "main" {
		t.Fatalf("index authorization calls = %v", calls)
	}
	jobs.mu.Lock()
	createCalls := jobs.createCalls
	captured := jobs.createRequest
	jobs.mu.Unlock()
	if createCalls != 1 || captured.AppID != appID || captured.OwnerID != "owner-1" || captured.TenantID != "tenant-1" {
		t.Fatalf("Create calls/request = %d/%+v", createCalls, captured)
	}
}

func TestKnowledgeSearchAdmissionAppAuthorizationFailsClosedBeforeIndexes(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name       string
		catalog    *fakeBootstrapAppCatalog
		wantStatus int
	}{
		{
			name: "app absent",
			catalog: &fakeBootstrapAppCatalog{result: AppCatalogResult{
				Complete: true,
			}},
			wantStatus: http.StatusForbidden,
		},
		{
			name:       "catalog incomplete",
			catalog:    &fakeBootstrapAppCatalog{result: AppCatalogResult{Complete: false}},
			wantStatus: http.StatusServiceUnavailable,
		},
		{
			name:       "catalog unavailable",
			catalog:    &fakeBootstrapAppCatalog{err: errors.New("secret app database detail")},
			wantStatus: http.StatusServiceUnavailable,
		},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			jobs := &knowledgeAdmissionSearchJobs{
				fakeSearchJobs: &fakeSearchJobs{createJob: completeJob("must-not-create")},
				enabled:        true,
			}
			indexes := activeHistoryRerunIndexCatalog("main")
			handler := newTestHandler(t, Config{
				SearchJobs: jobs,
				Indexes:    indexes,
				AppCatalog: test.catalog,
				WebUI:      testUI(),
				OwnerID:    "owner-1",
				TenantID:   "tenant-1",
			})
			request := createRequest("-1h", "now", "main")
			request.Definition.AppId = stringPointer("app-main")
			response := postProto(t, handler, "/api/v1/search/jobs/create", request)
			if response.Code != test.wantStatus || strings.Contains(response.Body.String(), "secret") {
				t.Fatalf("status/body = %d/%s", response.Code, response.Body.String())
			}
			if calls := indexes.capturedCalls(); len(calls) != 0 {
				t.Fatalf("rejected app reached index authorization: %v", calls)
			}
			jobs.mu.Lock()
			createCalls := jobs.createCalls
			jobs.mu.Unlock()
			if createCalls != 0 {
				t.Fatalf("rejected app created %d jobs", createCalls)
			}
		})
	}
}

func TestKnowledgeSearchAdmissionPreservesAppLessLegacyCreate(t *testing.T) {
	t.Parallel()

	job := completeJob("app-less-job")
	jobs := &knowledgeAdmissionSearchJobs{
		fakeSearchJobs: &fakeSearchJobs{createJob: job},
		enabled:        true,
	}
	apps := activeHistoryRerunAppCatalog("app-main")
	handler := newTestHandler(t, Config{
		SearchJobs: jobs,
		Indexes:    activeHistoryRerunIndexCatalog("main"),
		AppCatalog: apps,
		WebUI:      testUI(),
	})
	response := postProto(t, handler, "/api/v1/search/jobs/create", createRequest("-1h", "now", "main"))
	if response.Code != http.StatusOK {
		t.Fatalf("app-less create status = %d, body = %s", response.Code, response.Body.String())
	}
	if calls, _, _ := apps.captured(); calls != 0 {
		t.Fatalf("app-less create performed %d app catalog calls", calls)
	}
}

func TestKnowledgeDisabledSearchRejectsInventedSnapshot(t *testing.T) {
	t.Parallel()

	job := completeJob("disabled-invented-snapshot")
	job.KnowledgeSnapshot = enabledEmptyKnowledgeSnapshotSummary()
	handler := newTestHandler(t, Config{
		SearchJobs: &fakeSearchJobs{createJob: job},
		Indexes:    activeHistoryRerunIndexCatalog("main"),
		WebUI:      testUI(),
	})
	response := postProto(t, handler, "/api/v1/search/jobs/create", createRequest("-1h", "now", "main"))
	if response.Code != http.StatusInternalServerError ||
		!strings.Contains(response.Body.String(), "internal server error") {
		t.Fatalf("status/body = %d/%s", response.Code, response.Body.String())
	}
}

func TestKnowledgeSearchAdmissionRejectsMismatchedDependencyOutcome(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name       string
		requestApp string
		jobApp     string
		summary    *opensplunkv1.KnowledgeSnapshotSummary
	}{
		{name: "scoped result omitted snapshot", requestApp: "app-main", jobApp: "app-main"},
		{name: "scoped result changed app", requestApp: "app-main", jobApp: "app-other", summary: enabledEmptyKnowledgeSnapshotSummary()},
		{name: "app-less result invented snapshot", summary: enabledEmptyKnowledgeSnapshotSummary()},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			job := completeJob("mismatched-knowledge-outcome")
			job.AppID = test.jobApp
			job.KnowledgeSnapshot = test.summary
			jobs := &knowledgeAdmissionSearchJobs{
				fakeSearchJobs: &fakeSearchJobs{createJob: job},
				enabled:        true,
			}
			handler := newTestHandler(t, Config{
				SearchJobs: jobs,
				Indexes:    activeHistoryRerunIndexCatalog("main"),
				AppCatalog: activeHistoryRerunAppCatalog("app-main"),
				WebUI:      testUI(),
			})
			request := createRequest("-1h", "now", "main")
			if test.requestApp != "" {
				request.Definition.AppId = stringPointer(test.requestApp)
			}
			response := postProto(t, handler, "/api/v1/search/jobs/create", request)
			if response.Code != http.StatusInternalServerError ||
				!strings.Contains(response.Body.String(), "internal server error") {
				t.Fatalf("status/body = %d/%s", response.Code, response.Body.String())
			}
		})
	}
}

func TestKnowledgeSearchAdmissionRejectsMismatchedLaterLifecycleProjections(t *testing.T) {
	t.Parallel()

	states := []searchjobs.State{
		searchjobs.StateQueued,
		searchjobs.StateParsing,
		searchjobs.StatePlanning,
		searchjobs.StateRunning,
		searchjobs.StateCompleted,
		searchjobs.StateFailed,
		searchjobs.StateCanceled,
		searchjobs.StateExpired,
	}
	probe := &apiHandler{knowledgeSearchAdmission: true}
	disabledProbe := &apiHandler{}
	for _, state := range states {
		state := state
		t.Run(state.String(), func(t *testing.T) {
			appScoped := completeJob("projection-invariant")
			appScoped.State = state
			appScoped.AppID = "app-main"
			if probe.validKnowledgeSearchJobProjection(appScoped) {
				t.Fatal("app-scoped job without a snapshot passed projection validation")
			}
			appLess := completeJob("projection-invariant")
			appLess.State = state
			appLess.KnowledgeSnapshot = enabledEmptyKnowledgeSnapshotSummary()
			if probe.validKnowledgeSearchJobProjection(appLess) {
				t.Fatal("app-less job with an invented snapshot passed projection validation")
			}
			if disabledProbe.validKnowledgeSearchJobProjection(appLess) {
				t.Fatal("knowledge-disabled projection accepted an invented snapshot")
			}
		})
	}

	for _, route := range []struct {
		name    string
		path    string
		request proto.Message
		prepare func(*fakeSearchJobs, searchjobs.Job)
	}{
		{
			name:    "get",
			path:    "/api/v1/search/jobs/get",
			request: &opensplunkv1.GetSearchJobRequest{SearchJobId: "projection-mismatch"},
			prepare: func(jobs *fakeSearchJobs, job searchjobs.Job) { jobs.getJob = job },
		},
		{
			name:    "cancel",
			path:    "/api/v1/search/jobs/cancel",
			request: &opensplunkv1.CancelSearchJobRequest{SearchJobId: "projection-mismatch"},
			prepare: func(jobs *fakeSearchJobs, job searchjobs.Job) { jobs.getJob = job },
		},
		{
			name:    "list",
			path:    searchJobsListPath,
			request: &opensplunkv1.ListSearchJobsRequest{},
			prepare: func(jobs *fakeSearchJobs, job searchjobs.Job) {
				jobs.listPage = searchjobs.JobListPage{Jobs: []searchjobs.JobListItem{listItem(job)}}
			},
		},
	} {
		route := route
		t.Run(route.name, func(t *testing.T) {
			t.Parallel()
			job := listSearchJob("projection-mismatch", time.Now().UTC())
			job.AppID = "app-main"
			job.KnowledgeSnapshot = nil
			jobs := &fakeSearchJobs{}
			route.prepare(jobs, job)
			configured := &knowledgeAdmissionSearchJobs{fakeSearchJobs: jobs, enabled: true}
			handler := newTestHandler(t, Config{
				SearchJobs: configured,
				Indexes:    fakeIndexCatalog{},
				AppCatalog: activeHistoryRerunAppCatalog("app-main"),
				WebUI:      testUI(),
				OwnerID:    "owner-1",
				TenantID:   "tenant-1",
			})
			response := postProto(t, handler, route.path, route.request)
			if response.Code != http.StatusInternalServerError ||
				!strings.Contains(response.Body.String(), "internal server error") {
				t.Fatalf("status/body = %d/%s", response.Code, response.Body.String())
			}
		})
	}
}

func TestKnowledgeSearchAdmissionListProjectsDetachedRedactedSnapshot(t *testing.T) {
	t.Parallel()

	job := listSearchJob("knowledge-list-projection", testNow)
	job.AppID = "app-main"
	job.KnowledgeSnapshot = serverKnowledgeSnapshotSummary()
	jobs := &knowledgeAdmissionSearchJobs{
		fakeSearchJobs: &fakeSearchJobs{listPage: listPage(job)},
		enabled:        true,
	}
	handler := newTestHandler(t, Config{
		SearchJobs: jobs,
		Indexes:    fakeIndexCatalog{},
		AppCatalog: activeHistoryRerunAppCatalog("app-main"),
		WebUI:      testUI(),
		OwnerID:    "owner-1",
		TenantID:   "tenant-1",
	})
	response := postProto(t, handler, searchJobsListPath, &opensplunkv1.ListSearchJobsRequest{})
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
	}
	var decoded opensplunkv1.ListSearchJobsResponse
	unmarshalResponse(t, response, &decoded)
	if len(decoded.GetSearchJobs()) != 1 {
		t.Fatalf("jobs = %d, want 1", len(decoded.GetSearchJobs()))
	}
	projected := decoded.GetSearchJobs()[0].GetKnowledgeSnapshot()
	assertRedactedKnowledgeSnapshotSummary(t, projected, job.KnowledgeSnapshot.GetRef())
	projected.Ref.SnapshotSha256[0] ^= 0xff
	if job.KnowledgeSnapshot.GetRef().GetSnapshotSha256()[0] != 0x42 {
		t.Fatal("list response mutation changed dependency-owned snapshot")
	}
}

func TestKnowledgeSearchAdmissionMapsSynchronousFailuresWithoutDetails(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name       string
		err        error
		wantStatus int
	}{
		{name: "invalid SPL", err: searchjobs.ErrInvalidSPL, wantStatus: http.StatusBadRequest},
		{name: "unsupported SPL", err: searchjobs.ErrUnsupportedSPL, wantStatus: http.StatusUnprocessableEntity},
		{name: "knowledge unavailable", err: searchjobs.ErrKnowledgeUnavailable, wantStatus: http.StatusServiceUnavailable},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			jobs := &knowledgeAdmissionSearchJobs{
				fakeSearchJobs: &fakeSearchJobs{createErr: errors.Join(test.err, errors.New("secret compiler detail"))},
				enabled:        true,
			}
			handler := newTestHandler(t, Config{
				SearchJobs: jobs,
				Indexes:    activeHistoryRerunIndexCatalog("main"),
				AppCatalog: activeHistoryRerunAppCatalog("app-main"),
				WebUI:      testUI(),
			})
			request := createRequest("-1h", "now", "main")
			request.Definition.AppId = stringPointer("app-main")
			response := postProto(t, handler, "/api/v1/search/jobs/create", request)
			if response.Code != test.wantStatus || strings.Contains(response.Body.String(), "secret") {
				t.Fatalf("status/body = %d/%s", response.Code, response.Body.String())
			}
		})
	}
}

func enabledEmptyKnowledgeSnapshotSummary() *opensplunkv1.KnowledgeSnapshotSummary {
	return &opensplunkv1.KnowledgeSnapshotSummary{Ref: &opensplunkv1.KnowledgeSnapshotRef{
		SnapshotSha256:               bytes.Repeat([]byte{0x42}, 32),
		TenantCatalogStateToken:      bytes.Repeat([]byte{0x73}, 32),
		CompilerCompatibilityVersion: "0.1",
	}}
}

var _ SearchJobs = (*knowledgeAdmissionSearchJobs)(nil)
var _ knowledgeSearchAdmission = (*knowledgeAdmissionSearchJobs)(nil)
