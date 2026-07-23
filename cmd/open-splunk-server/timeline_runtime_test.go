package main

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
	"testing/fstest"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/plan"
	"github.com/Suhaibinator/open-splunk/internal/queryexec"
	"github.com/Suhaibinator/open-splunk/internal/savedobjects"
	"github.com/Suhaibinator/open-splunk/internal/searchanalysis"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
	"github.com/Suhaibinator/open-splunk/internal/server"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

type runtimeCompletedSearches struct{}

func (runtimeCompletedSearches) CompletedExecutionSnapshotFor(context.Context, searchjobs.AccessScope, string) (searchjobs.ExecutionSnapshot, error) {
	return searchjobs.ExecutionSnapshot{}, searchjobs.ErrNotFound
}

type runtimeTimelineCompiler struct{}

func (runtimeTimelineCompiler) CompileTimeline(*plan.Query, clickhouse.TimelineSpec) (clickhouse.CompiledTimeline, error) {
	return clickhouse.CompiledTimeline{}, nil
}

type runtimeTimelineExecutor struct{}

func (runtimeTimelineExecutor) ExecuteTimeline(context.Context, clickhouse.CompiledTimeline) ([]queryexec.TimelineBucket, error) {
	return nil, nil
}

type runtimeSearchJobs struct{}

func (runtimeSearchJobs) Create(context.Context, searchjobs.CreateRequest) (searchjobs.Job, error) {
	return searchjobs.Job{}, nil
}

func (runtimeSearchJobs) GetFor(searchjobs.AccessScope, string) (searchjobs.Job, error) {
	return searchjobs.Job{}, searchjobs.ErrNotFound
}

func (runtimeSearchJobs) ResultsFor(searchjobs.AccessScope, string, searchjobs.PageRequest) (searchjobs.ResultPage, error) {
	return searchjobs.ResultPage{}, searchjobs.ErrNotFound
}

func (runtimeSearchJobs) CancelFor(searchjobs.AccessScope, string) error {
	return searchjobs.ErrNotFound
}

type runtimeIndexCatalog struct{}

func (runtimeIndexCatalog) ListIndexes(context.Context) ([]control.Index, error) {
	return nil, nil
}

func (runtimeIndexCatalog) GetIndexByName(context.Context, string) (control.Index, error) {
	return control.Index{}, control.ErrNotFound
}

type runtimeSavedSearches struct{}

func (runtimeSavedSearches) Create(context.Context, savedobjects.AccessScope, *opensplunkv1.SavedSearchDefinition) (*opensplunkv1.SavedSearch, error) {
	return nil, control.ErrNotFound
}

func (runtimeSavedSearches) Get(context.Context, savedobjects.AccessScope, string) (*opensplunkv1.SavedSearch, error) {
	return nil, control.ErrNotFound
}

func (runtimeSavedSearches) List(context.Context, savedobjects.AccessScope, savedobjects.ListRequest) (savedobjects.ListResult, error) {
	return savedobjects.ListResult{}, nil
}

func (runtimeSavedSearches) Update(context.Context, savedobjects.AccessScope, string, uint64, *opensplunkv1.SavedSearchDefinition, *fieldmaskpb.FieldMask) (*opensplunkv1.SavedSearch, error) {
	return nil, control.ErrNotFound
}

func (runtimeSavedSearches) Duplicate(context.Context, savedobjects.AccessScope, string, string, *string) (*opensplunkv1.SavedSearch, error) {
	return nil, control.ErrNotFound
}

func (runtimeSavedSearches) Delete(context.Context, savedobjects.AccessScope, string, uint64) error {
	return control.ErrNotFound
}

func TestRuntimeHTTPHandlerAdvertisesEnforcedTimelineService(t *testing.T) {
	handler, err := newRuntimeHTTPHandler(runtimeServerConfig(), searchanalysis.Config{
		Searches: runtimeCompletedSearches{},
		Compiler: runtimeTimelineCompiler{},
		Executor: runtimeTimelineExecutor{},
	})
	if err != nil {
		t.Fatalf("newRuntimeHTTPHandler: %v", err)
	}

	payload, err := proto.Marshal(&opensplunkv1.GetSystemBootstrapRequest{})
	if err != nil {
		t.Fatalf("marshal bootstrap request: %v", err)
	}
	request := httptest.NewRequest(http.MethodPost, "http://127.0.0.1/api/v1/system/bootstrap", bytes.NewReader(payload))
	request.Header.Set("Content-Type", "application/x-protobuf")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("bootstrap status = %d, body = %s", response.Code, response.Body.String())
	}
	var decoded opensplunkv1.GetSystemBootstrapResponse
	if err := proto.Unmarshal(response.Body.Bytes(), &decoded); err != nil {
		t.Fatalf("unmarshal bootstrap response: %v", err)
	}
	if !slices.Contains(decoded.GetFeatures(), opensplunkv1.ServerFeature_SERVER_FEATURE_TIMELINE) {
		t.Fatalf("bootstrap features = %v, want timeline", decoded.GetFeatures())
	}
	if decoded.GetLimits().GetMaximumTimelineBuckets() != 1_000 {
		t.Fatalf("maximum timeline buckets = %d, want enforcing service default 1000", decoded.GetLimits().GetMaximumTimelineBuckets())
	}
}

func TestRuntimeHTTPHandlerFailsClosedWithoutTimelineDependencies(t *testing.T) {
	_, err := newRuntimeHTTPHandler(runtimeServerConfig(), searchanalysis.Config{})
	if err == nil || !strings.Contains(err.Error(), "completed search snapshots are required") {
		t.Fatalf("newRuntimeHTTPHandler error = %v", err)
	}
}

func TestRuntimeHTTPHandlerRejectsPreconfiguredTimelineService(t *testing.T) {
	config := runtimeServerConfig()
	config.SearchTimelines = &runtimeConfiguredTimelines{}
	_, err := newRuntimeHTTPHandler(config, searchanalysis.Config{
		Searches: runtimeCompletedSearches{},
		Compiler: runtimeTimelineCompiler{},
		Executor: runtimeTimelineExecutor{},
	})
	if err == nil || !strings.Contains(err.Error(), "already configured") {
		t.Fatalf("newRuntimeHTTPHandler error = %v", err)
	}
}

type runtimeConfiguredTimelines struct{}

func (*runtimeConfiguredTimelines) MaximumBuckets() uint32 { return 1 }

func (*runtimeConfiguredTimelines) Get(context.Context, searchjobs.AccessScope, searchanalysis.Request) (searchanalysis.Result, error) {
	return searchanalysis.Result{}, nil
}

func runtimeServerConfig() server.Config {
	return server.Config{
		SearchJobs:    runtimeSearchJobs{},
		Indexes:       runtimeIndexCatalog{},
		SavedSearches: runtimeSavedSearches{},
		WebUI: fstest.MapFS{
			"index.html": &fstest.MapFile{Data: []byte("<html>runtime</html>")},
		},
	}
}
