package main

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
	"testing/fstest"
	"time"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/eventfields"
	"github.com/Suhaibinator/open-splunk/internal/plan"
	"github.com/Suhaibinator/open-splunk/internal/queryexec"
	"github.com/Suhaibinator/open-splunk/internal/savedobjects"
	"github.com/Suhaibinator/open-splunk/internal/searchanalysis"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
	"github.com/Suhaibinator/open-splunk/internal/searchsuggestions"
	"github.com/Suhaibinator/open-splunk/internal/searchtime"
	"github.com/Suhaibinator/open-splunk/internal/server"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

type runtimeCompletedSearches struct{}

func (runtimeCompletedSearches) CompletedExecutionSnapshotFor(context.Context, searchjobs.AccessScope, string) (searchjobs.ExecutionSnapshot, error) {
	return searchjobs.ExecutionSnapshot{}, searchjobs.ErrNotFound
}

func (runtimeCompletedSearches) Validate(
	context.Context,
	searchjobs.ValidateRequest,
) (searchjobs.ValidationResult, error) {
	return searchjobs.ValidationResult{}, searchjobs.ErrClosed
}

func (runtimeCompletedSearches) SnapshotAnalysisScope(
	context.Context,
	searchjobs.AnalysisScopeRequest,
) (searchjobs.AnalysisScopeSnapshot, error) {
	return searchjobs.AnalysisScopeSnapshot{}, searchjobs.ErrClosed
}

type runtimeTimelineCompiler struct{}

func (runtimeTimelineCompiler) CompileTimeline(*plan.Query, clickhouse.TimelineSpec) (clickhouse.CompiledTimeline, error) {
	return clickhouse.CompiledTimeline{}, nil
}

func (runtimeTimelineCompiler) CompileFieldCatalog(_ *plan.Query, spec clickhouse.FieldCatalogSpec) (clickhouse.CompiledFieldCatalog, error) {
	return clickhouse.CompiledFieldCatalog{SQL: "SELECT field catalog", Spec: spec}, nil
}

func (runtimeTimelineCompiler) CompileFieldSummary(_ *plan.Query, spec clickhouse.FieldSummarySpec) (clickhouse.CompiledFieldSummary, error) {
	return clickhouse.CompiledFieldSummary{SQL: "SELECT field summary", Spec: spec}, nil
}

func (runtimeTimelineCompiler) CompileFieldSuggestions(
	_ *plan.Query,
	spec clickhouse.FieldSuggestionSpec,
) (clickhouse.CompiledFieldSuggestions, error) {
	return clickhouse.CompiledFieldSuggestions{
		SQL:  "SELECT field suggestions",
		Spec: spec,
	}, nil
}

type runtimeTimelineExecutor struct{}

func (runtimeTimelineExecutor) ExecuteTimeline(context.Context, clickhouse.CompiledTimeline) ([]queryexec.TimelineBucket, error) {
	return nil, nil
}

func (runtimeTimelineExecutor) ExecuteFieldCatalog(context.Context, clickhouse.CompiledFieldCatalog) (queryexec.FieldCatalogResult, error) {
	return queryexec.FieldCatalogResult{}, nil
}

func (runtimeTimelineExecutor) ExecuteFieldSummary(context.Context, clickhouse.CompiledFieldSummary) (queryexec.FieldSummaryResult, error) {
	return queryexec.FieldSummaryResult{}, nil
}

func (runtimeTimelineExecutor) ExecuteFieldSuggestions(
	context.Context,
	clickhouse.CompiledFieldSuggestions,
) (queryexec.FieldSuggestionResult, error) {
	return queryexec.FieldSuggestionResult{}, nil
}

type runtimeSearchJobs struct{}

func (runtimeSearchJobs) Create(context.Context, searchjobs.CreateRequest) (searchjobs.Job, error) {
	return searchjobs.Job{}, nil
}

func (runtimeSearchJobs) Validate(context.Context, searchjobs.ValidateRequest) (searchjobs.ValidationResult, error) {
	return searchjobs.ValidationResult{}, nil
}

func (runtimeSearchJobs) GetFor(searchjobs.AccessScope, string) (searchjobs.Job, error) {
	return searchjobs.Job{}, searchjobs.ErrNotFound
}

func (runtimeSearchJobs) PreviewFor(searchjobs.AccessScope, string, int) (searchjobs.PreviewSnapshot, error) {
	return searchjobs.PreviewSnapshot{}, searchjobs.ErrNotFound
}

func (jobs runtimeSearchJobs) PreviewForBytes(scope searchjobs.AccessScope, id string, limit int, _ uint64) (searchjobs.PreviewSnapshot, error) {
	return jobs.PreviewFor(scope, id, limit)
}

func (runtimeSearchJobs) MaximumPreviewRows() uint32 { return 100 }

func (runtimeSearchJobs) ListPageFor(context.Context, searchjobs.AccessScope, searchjobs.JobListRequest) (searchjobs.JobListPage, error) {
	return searchjobs.JobListPage{}, nil
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

type runtimeSuggestionSearches struct {
	runtimeSearchJobs

	mu          sync.Mutex
	createCalls int
}

func (searches *runtimeSuggestionSearches) Create(
	context.Context,
	searchjobs.CreateRequest,
) (searchjobs.Job, error) {
	searches.mu.Lock()
	defer searches.mu.Unlock()
	searches.createCalls++
	return searchjobs.Job{}, nil
}

func (*runtimeSuggestionSearches) Validate(
	_ context.Context,
	request searchjobs.ValidateRequest,
) (searchjobs.ValidationResult, error) {
	return searchjobs.ValidationResult{
		Valid:               true,
		NormalizedSPL:       strings.TrimSpace(request.SPL),
		ReferencedIndexes:   []string{"main"},
		PredictedResultKind: searchjobs.ValidationResultKindEvents,
	}, nil
}

func (*runtimeSuggestionSearches) CompletedExecutionSnapshotFor(
	context.Context,
	searchjobs.AccessScope,
	string,
) (searchjobs.ExecutionSnapshot, error) {
	return searchjobs.ExecutionSnapshot{}, searchjobs.ErrNotFound
}

func (*runtimeSuggestionSearches) SnapshotAnalysisScope(
	context.Context,
	searchjobs.AnalysisScopeRequest,
) (searchjobs.AnalysisScopeSnapshot, error) {
	return searchjobs.AnalysisScopeSnapshot{}, errors.New("static suggestion unexpectedly requested storage scope")
}

func (searches *runtimeSuggestionSearches) createCallCount() int {
	searches.mu.Lock()
	defer searches.mu.Unlock()
	return searches.createCalls
}

type runtimeSuggestionIndexes struct{ runtimeIndexCatalog }

func (runtimeSuggestionIndexes) GetIndexByName(
	_ context.Context,
	name string,
) (control.Index, error) {
	if name != "main" {
		return control.Index{}, control.ErrNotFound
	}
	return control.Index{
		Definition: control.IndexDefinition{
			Name:          name,
			SearchEnabled: true,
		},
		State: control.IndexStateActive,
	}, nil
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
	analysis := newRuntimeSearchAnalysisForTest(t)
	handler, err := newRuntimeHTTPHandler(runtimeServerConfig(), analysis)
	if err != nil {
		t.Fatalf("newRuntimeHTTPHandler: %v", err)
	}

	payload, err := proto.Marshal(&opensplunkv1.GetSystemBootstrapRequest{})
	if err != nil {
		t.Fatalf("marshal bootstrap request: %v", err)
	}
	request := httptest.NewRequestWithContext(context.Background(), http.MethodPost, "http://127.0.0.1/api/v1/system/bootstrap", bytes.NewReader(payload))
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
	if !slices.Contains(decoded.GetFeatures(), opensplunkv1.ServerFeature_SERVER_FEATURE_FIELD_DISCOVERY) {
		t.Fatalf("bootstrap features = %v, want field discovery", decoded.GetFeatures())
	}
	if decoded.GetLimits().GetMaximumFieldSummaryValues() != clickhouse.MaximumFieldSummaryValues {
		t.Fatalf(
			"maximum field summary values = %d, want enforcing service default %d",
			decoded.GetLimits().GetMaximumFieldSummaryValues(),
			clickhouse.MaximumFieldSummaryValues,
		)
	}
}

func TestRuntimeHTTPHandlerServesComposedSearchSuggestionsWithoutCreatingJob(t *testing.T) {
	searches := &runtimeSuggestionSearches{}
	analysis, err := newRuntimeSearchAnalysis(runtimeSearchAnalysisConfig{
		Searches: searches,
		Compiler: runtimeTimelineCompiler{},
		Executor: runtimeTimelineExecutor{},
	})
	if err != nil {
		t.Fatalf("newRuntimeSearchAnalysis: %v", err)
	}
	t.Cleanup(func() {
		if err := analysis.Close(); err != nil {
			t.Errorf("analysis.Close: %v", err)
		}
	})

	config := runtimeServerConfig()
	config.SearchJobs = searches
	config.Indexes = runtimeSuggestionIndexes{}
	config.TenantID = "tenant"
	config.Now = func() time.Time {
		return time.Date(2026, time.July, 22, 12, 0, 0, 0, time.UTC)
	}
	handler, err := newRuntimeHTTPHandler(config, analysis)
	if err != nil {
		t.Fatalf("newRuntimeHTTPHandler: %v", err)
	}

	source := "index=main | head"
	cursor := len("index=main | he")
	earliest, latest, timezone := "-24h", "now", "UTC"
	payload, err := proto.Marshal(&opensplunkv1.GetSearchSuggestionsRequest{
		Spl:              source,
		CursorByteOffset: uint64(cursor),
		TimeRange: &opensplunkv1.TimeRangeSpec{
			Earliest: &earliest,
			Latest:   &latest,
			Timezone: &timezone,
		},
		IndexScope: []string{"main"},
	})
	if err != nil {
		t.Fatalf("marshal suggestion request: %v", err)
	}
	request := httptest.NewRequestWithContext(
		context.Background(),
		http.MethodPost,
		"http://127.0.0.1/api/v1/search/suggestions",
		bytes.NewReader(payload),
	)
	request.Header.Set("Content-Type", "application/x-protobuf")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("suggestion status = %d, body = %s", response.Code, response.Body.String())
	}
	var decoded opensplunkv1.GetSearchSuggestionsResponse
	if err := proto.Unmarshal(response.Body.Bytes(), &decoded); err != nil {
		t.Fatalf("unmarshal suggestion response: %v", err)
	}
	foundHead := false
	for _, suggestion := range decoded.GetSuggestions() {
		if suggestion.GetKind() == opensplunkv1.SearchSuggestionKind_SEARCH_SUGGESTION_KIND_COMMAND &&
			suggestion.GetLabel() == "head" {
			foundHead = true
			break
		}
	}
	if !foundHead {
		t.Fatalf("suggestions = %+v, want command head", decoded.GetSuggestions())
	}
	if calls := searches.createCallCount(); calls != 0 {
		t.Fatalf("search job Create calls = %d, want 0", calls)
	}
}

func TestRuntimeHTTPHandlerServesConfiguredFieldCatalog(t *testing.T) {
	snapshot := runtimeFieldExecutionSnapshot()
	analysis, err := newRuntimeSearchAnalysis(runtimeSearchAnalysisConfig{
		Searches: runtimeSnapshotSearches{snapshot: snapshot}, Compiler: runtimeTimelineCompiler{}, Executor: runtimeTimelineExecutor{},
	})
	if err != nil {
		t.Fatalf("newRuntimeSearchAnalysis: %v", err)
	}
	t.Cleanup(func() {
		if err := analysis.Close(); err != nil {
			t.Errorf("analysis.Close: %v", err)
		}
	})
	config := runtimeServerConfig()
	config.OwnerID = snapshot.OwnerID
	config.TenantID = snapshot.TenantID
	handler, err := newRuntimeHTTPHandler(config, analysis)
	if err != nil {
		t.Fatalf("newRuntimeHTTPHandler: %v", err)
	}

	payload, err := proto.Marshal(&opensplunkv1.ListSearchFieldsRequest{
		SearchJobId: snapshot.ID,
		Page:        &opensplunkv1.PageRequest{IncludeTotalSize: true},
	})
	if err != nil {
		t.Fatalf("marshal field request: %v", err)
	}
	request := httptest.NewRequestWithContext(context.Background(), http.MethodPost, "http://127.0.0.1/api/v1/search/jobs/fields/list", bytes.NewReader(payload))
	request.Header.Set("Content-Type", "application/x-protobuf")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("field-list status = %d, body = %s", response.Code, response.Body.String())
	}
	var decoded opensplunkv1.ListSearchFieldsResponse
	if err := proto.Unmarshal(response.Body.Bytes(), &decoded); err != nil {
		t.Fatalf("unmarshal field response: %v", err)
	}
	if len(decoded.GetFields()) != 0 || decoded.GetPage() == nil || decoded.GetPage().TotalSize == nil ||
		decoded.GetPage().GetTotalSize() != 0 || !decoded.GetPage().GetTotalSizeExact() {
		t.Fatalf("field-list response = %+v", &decoded)
	}
}

func TestRuntimeHTTPHandlerServesConfiguredFieldSummary(t *testing.T) {
	snapshot := runtimeFieldExecutionSnapshot()
	analysis, err := newRuntimeSearchAnalysis(runtimeSearchAnalysisConfig{
		Searches: runtimeSnapshotSearches{snapshot: snapshot},
		Compiler: runtimeFieldSummaryCompiler{},
		Executor: runtimeFieldSummaryExecutor{},
	})
	if err != nil {
		t.Fatalf("newRuntimeSearchAnalysis: %v", err)
	}
	t.Cleanup(func() {
		if err := analysis.Close(); err != nil {
			t.Errorf("analysis.Close: %v", err)
		}
	})
	config := runtimeServerConfig()
	config.OwnerID = snapshot.OwnerID
	config.TenantID = snapshot.TenantID
	handler, err := newRuntimeHTTPHandler(config, analysis)
	if err != nil {
		t.Fatalf("newRuntimeHTTPHandler: %v", err)
	}

	maximumValues := uint32(2)
	payload, err := proto.Marshal(&opensplunkv1.GetSearchFieldSummaryRequest{
		SearchJobId: snapshot.ID,
		FieldName:   "level",
		MaxValues:   &maximumValues,
	})
	if err != nil {
		t.Fatalf("marshal field summary request: %v", err)
	}
	request := httptest.NewRequestWithContext(context.Background(), http.MethodPost, "http://127.0.0.1/api/v1/search/jobs/field-summary", bytes.NewReader(payload))
	request.Header.Set("Content-Type", "application/x-protobuf")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("field-summary status = %d, body = %s", response.Code, response.Body.String())
	}
	var decoded opensplunkv1.GetSearchFieldSummaryResponse
	if err := proto.Unmarshal(response.Body.Bytes(), &decoded); err != nil {
		t.Fatalf("unmarshal field summary response: %v", err)
	}
	summary := decoded.GetFieldSummary()
	if summary == nil || summary.GetProfile().GetFieldName() != "level" ||
		summary.GetProfile().GetDistinctCount() != 2 || len(summary.GetTopValues()) != 2 ||
		summary.GetTopValues()[0].GetValue().GetStringValue() != "error" ||
		summary.GetTopValues()[0].GetCount() != 2 ||
		summary.GetTopValues()[1].GetValue().GetStringValue() != "info" ||
		summary.GetTopValues()[1].GetCount() != 1 {
		t.Fatalf("field-summary response = %+v", summary)
	}
}

func TestRuntimeSearchAnalysisFailsClosedWithoutDependencies(t *testing.T) {
	tests := []struct {
		name   string
		config runtimeSearchAnalysisConfig
		want   string
	}{
		{
			name: "searches", want: "completed search snapshots are required",
			config: runtimeSearchAnalysisConfig{Compiler: runtimeTimelineCompiler{}, Executor: runtimeTimelineExecutor{}},
		},
		{
			name: "compiler", want: "timeline compiler is required",
			config: runtimeSearchAnalysisConfig{Searches: runtimeCompletedSearches{}, Executor: runtimeTimelineExecutor{}},
		},
		{
			name: "executor", want: "timeline executor is required",
			config: runtimeSearchAnalysisConfig{Searches: runtimeCompletedSearches{}, Compiler: runtimeTimelineCompiler{}},
		},
		{
			name: "field options", want: "cursor scope is invalid",
			config: runtimeSearchAnalysisConfig{
				Searches: runtimeCompletedSearches{}, Compiler: runtimeTimelineCompiler{}, Executor: runtimeTimelineExecutor{},
				FieldCursorScope: strings.Repeat("x", 257),
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			analysis, err := newRuntimeSearchAnalysis(test.config)
			if err == nil || analysis != nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("newRuntimeSearchAnalysis = (%v, %v), want %q error", analysis, err, test.want)
			}
		})
	}
}

func TestRuntimeHTTPHandlerRejectsPreconfiguredTimelineService(t *testing.T) {
	analysis := newRuntimeSearchAnalysisForTest(t)
	config := runtimeServerConfig()
	config.SearchTimelines = &runtimeConfiguredTimelines{}
	_, err := newRuntimeHTTPHandler(config, analysis)
	if err == nil || !strings.Contains(err.Error(), "already configured") {
		t.Fatalf("newRuntimeHTTPHandler error = %v", err)
	}
}

func TestRuntimeHTTPHandlerRejectsPreconfiguredFieldServiceAndMissingAnalysis(t *testing.T) {
	analysis := newRuntimeSearchAnalysisForTest(t)
	config := runtimeServerConfig()
	config.SearchFields = &runtimeConfiguredFields{}
	if _, err := newRuntimeHTTPHandler(config, analysis); err == nil || !strings.Contains(err.Error(), "already configured") {
		t.Fatalf("preconfigured field error = %v", err)
	}
	config = runtimeServerConfig()
	config.SearchSuggestions = &runtimeConfiguredSuggestions{}
	if _, err := newRuntimeHTTPHandler(config, analysis); err == nil ||
		!strings.Contains(err.Error(), "already configured") {
		t.Fatalf("preconfigured suggestion error = %v", err)
	}
	if _, err := newRuntimeHTTPHandler(runtimeServerConfig(), nil); err == nil || !strings.Contains(err.Error(), "services are required") {
		t.Fatalf("nil analysis error = %v", err)
	}
}

func TestRuntimeSearchAnalysisRejectsTypedNilDependencies(t *testing.T) {
	var searches *runtimeCompletedSearches
	var compiler *runtimeTimelineCompiler
	var executor *runtimeTimelineExecutor
	for _, test := range []struct {
		name   string
		config runtimeSearchAnalysisConfig
	}{
		{
			name: "searches",
			config: runtimeSearchAnalysisConfig{
				Searches: searches, Compiler: runtimeTimelineCompiler{}, Executor: runtimeTimelineExecutor{},
			},
		},
		{
			name: "compiler",
			config: runtimeSearchAnalysisConfig{
				Searches: runtimeCompletedSearches{}, Compiler: compiler, Executor: runtimeTimelineExecutor{},
			},
		},
		{
			name: "executor",
			config: runtimeSearchAnalysisConfig{
				Searches: runtimeCompletedSearches{}, Compiler: runtimeTimelineCompiler{}, Executor: executor,
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			if analysis, err := newRuntimeSearchAnalysis(test.config); err == nil || analysis != nil {
				t.Fatalf("newRuntimeSearchAnalysis = (%v, %v), want typed-nil rejection", analysis, err)
			}
		})
	}
}

func TestRuntimeSearchAnalysisCloseIsIdempotent(t *testing.T) {
	analysis := newRuntimeSearchAnalysisForTest(t)
	if err := analysis.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}
	if err := analysis.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
	if err := (*runtimeSearchAnalysis)(nil).Close(); err != nil {
		t.Fatalf("nil Close: %v", err)
	}
	_, err := analysis.fields.ListFields(context.Background(), searchjobs.AccessScope{
		TenantID: "tenant", OwnerID: "owner",
	}, searchanalysis.ListFieldsRequest{SearchJobID: "job"})
	if !errors.Is(err, searchjobs.ErrClosed) {
		t.Fatalf("ListFields after Close error = %v, want ErrClosed", err)
	}
}

func TestRuntimeSearchAnalysisCloseWaitsForBlockedFieldWorker(t *testing.T) {
	snapshot := runtimeFieldExecutionSnapshot()
	executor := &runtimeBlockingFieldExecutor{
		entered: make(chan struct{}),
		exited:  make(chan struct{}),
	}
	analysis, err := newRuntimeSearchAnalysis(runtimeSearchAnalysisConfig{
		Searches: runtimeSnapshotSearches{snapshot: snapshot},
		Compiler: runtimeTimelineCompiler{},
		Executor: executor,
	})
	if err != nil {
		t.Fatalf("newRuntimeSearchAnalysis: %v", err)
	}
	t.Cleanup(func() {
		if err := analysis.Close(); err != nil {
			t.Errorf("analysis.Close cleanup: %v", err)
		}
	})

	listResult := make(chan error, 1)
	go func() {
		_, err := analysis.fields.ListFields(context.Background(), searchjobs.AccessScope{
			TenantID: snapshot.TenantID, OwnerID: snapshot.OwnerID,
		}, searchanalysis.ListFieldsRequest{SearchJobID: snapshot.ID})
		listResult <- err
	}()
	select {
	case <-executor.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("field worker did not enter the executor")
	}

	closeResult := make(chan error, 1)
	go func() { closeResult <- analysis.Close() }()
	select {
	case err := <-closeResult:
		if err != nil {
			t.Fatalf("Close: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Close did not wait for the cancellation-aware field worker")
	}
	select {
	case <-executor.exited:
	default:
		t.Fatal("Close returned before the field worker exited")
	}
	select {
	case err := <-listResult:
		if !errors.Is(err, searchjobs.ErrClosed) {
			t.Fatalf("ListFields error = %v, want ErrClosed", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("ListFields did not return after analysis close")
	}
}

func TestRuntimeSearchAnalysisCloseWaitsForBlockedSuggestion(t *testing.T) {
	searches := &runtimeBlockingSuggestionSearches{
		entered: make(chan struct{}),
		exited:  make(chan struct{}),
	}
	analysis, err := newRuntimeSearchAnalysis(runtimeSearchAnalysisConfig{
		Searches: searches,
		Compiler: runtimeTimelineCompiler{},
		Executor: runtimeTimelineExecutor{},
	})
	if err != nil {
		t.Fatalf("newRuntimeSearchAnalysis: %v", err)
	}
	t.Cleanup(func() {
		if err := analysis.Close(); err != nil {
			t.Errorf("analysis.Close cleanup: %v", err)
		}
	})

	resolvedRange, err := searchtime.NewAbsoluteRange(
		time.Date(2026, 7, 22, 1, 0, 0, 0, time.UTC),
		time.Date(2026, 7, 22, 2, 0, 0, 0, time.UTC),
	)
	if err != nil {
		t.Fatalf("resolve suggestion range: %v", err)
	}
	suggestResult := make(chan error, 1)
	go func() {
		_, err := analysis.suggestions.Suggest(
			context.Background(),
			searchsuggestions.Request{
				SPL:                       "index=main | he",
				CursorByteOffset:          len("index=main | he"),
				TenantID:                  "tenant",
				AuthorizedIndexes:         []string{"main"},
				RequestedIndexes:          []string{"main"},
				TimeRange:                 resolvedRange,
				AuthorizedIndexCandidates: []string{"main"},
			},
		)
		suggestResult <- err
	}()
	select {
	case <-searches.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("suggestion did not enter validation")
	}

	closeResult := make(chan error, 1)
	go func() { closeResult <- analysis.Close() }()
	select {
	case err := <-closeResult:
		if err != nil {
			t.Fatalf("Close: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Close did not wait for the cancellation-aware suggestion")
	}
	select {
	case <-searches.exited:
	default:
		t.Fatal("Close returned before suggestion validation exited")
	}
	select {
	case err := <-suggestResult:
		if !errors.Is(err, searchjobs.ErrClosed) {
			t.Fatalf("Suggest error = %v, want ErrClosed", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Suggest did not return after analysis close")
	}
}

func TestRuntimeSearchAnalysisCloseWaitsForBlockedFieldSummaryWorker(t *testing.T) {
	snapshot := runtimeFieldExecutionSnapshot()
	executor := &runtimeBlockingFieldSummaryExecutor{
		entered: make(chan struct{}),
		exited:  make(chan struct{}),
	}
	analysis, err := newRuntimeSearchAnalysis(runtimeSearchAnalysisConfig{
		Searches: runtimeSnapshotSearches{snapshot: snapshot},
		Compiler: runtimeFieldSummaryCompiler{},
		Executor: executor,
	})
	if err != nil {
		t.Fatalf("newRuntimeSearchAnalysis: %v", err)
	}
	t.Cleanup(func() {
		if err := analysis.Close(); err != nil {
			t.Errorf("analysis.Close cleanup: %v", err)
		}
	})

	summaryResult := make(chan error, 1)
	go func() {
		_, err := analysis.fields.GetFieldSummary(context.Background(), searchjobs.AccessScope{
			TenantID: snapshot.TenantID, OwnerID: snapshot.OwnerID,
		}, searchanalysis.GetFieldSummaryRequest{SearchJobID: snapshot.ID, FieldName: "level"})
		summaryResult <- err
	}()
	select {
	case <-executor.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("field-summary worker did not enter the executor")
	}

	closeResult := make(chan error, 1)
	go func() { closeResult <- analysis.Close() }()
	select {
	case err := <-closeResult:
		if err != nil {
			t.Fatalf("Close: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Close did not wait for the cancellation-aware field-summary worker")
	}
	select {
	case <-executor.exited:
	default:
		t.Fatal("Close returned before the field-summary worker exited")
	}
	select {
	case err := <-summaryResult:
		if !errors.Is(err, searchjobs.ErrClosed) {
			t.Fatalf("GetFieldSummary error = %v, want ErrClosed", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("GetFieldSummary did not return after analysis close")
	}
}

type runtimeConfiguredTimelines struct{}

func (*runtimeConfiguredTimelines) MaximumBuckets() uint32 { return 1 }

func (*runtimeConfiguredTimelines) Get(context.Context, searchjobs.AccessScope, searchanalysis.Request) (searchanalysis.Result, error) {
	return searchanalysis.Result{}, nil
}

type runtimeConfiguredFields struct{}

func (*runtimeConfiguredFields) MaximumFields() uint32        { return 1 }
func (*runtimeConfiguredFields) MaximumPageSize() uint32      { return 1 }
func (*runtimeConfiguredFields) MaximumSummaryValues() uint32 { return 1 }

func (*runtimeConfiguredFields) ListFields(context.Context, searchjobs.AccessScope, searchanalysis.ListFieldsRequest) (searchanalysis.FieldPage, error) {
	return searchanalysis.FieldPage{}, nil
}

func (*runtimeConfiguredFields) GetFieldSummary(context.Context, searchjobs.AccessScope, searchanalysis.GetFieldSummaryRequest) (searchanalysis.FieldSummary, error) {
	return searchanalysis.FieldSummary{}, nil
}

type runtimeConfiguredSuggestions struct{}

func (*runtimeConfiguredSuggestions) MaximumSuggestions() uint32 { return 1 }

func (*runtimeConfiguredSuggestions) Suggest(
	context.Context,
	searchsuggestions.Request,
) (searchsuggestions.Result, error) {
	return searchsuggestions.Result{}, nil
}

type runtimeSnapshotSearches struct {
	runtimeCompletedSearches
	snapshot searchjobs.ExecutionSnapshot
}

func runtimeFieldExecutionSnapshot() searchjobs.ExecutionSnapshot {
	return searchjobs.ExecutionSnapshot{
		ID: "job", TenantID: "tenant", OwnerID: "owner", SPL: "index=main level=error",
		EffectiveIndexes: []string{"main"},
		Earliest:         time.Date(2026, 7, 22, 1, 0, 0, 0, time.UTC),
		Latest:           time.Date(2026, 7, 22, 2, 0, 0, 0, time.UTC),
		SearchStart:      time.Date(2026, 7, 22, 2, 0, 59, 0, time.UTC),
		SearchTimezone:   "UTC",
		IndexTimeCutoff:  time.Date(2026, 7, 22, 2, 1, 0, 0, time.UTC),
		VisibilityCutoff: 1,
		FinishedAt:       time.Date(2026, 7, 22, 2, 2, 0, 0, time.UTC),
		ExpiresAt:        time.Date(2099, 7, 22, 2, 2, 0, 0, time.UTC),
	}
}

func (searches runtimeSnapshotSearches) CompletedExecutionSnapshotFor(_ context.Context, _ searchjobs.AccessScope, _ string) (searchjobs.ExecutionSnapshot, error) {
	return searches.snapshot, nil
}

type runtimeBlockingFieldExecutor struct {
	runtimeTimelineExecutor
	entered chan struct{}
	exited  chan struct{}
	once    sync.Once
}

func (executor *runtimeBlockingFieldExecutor) ExecuteFieldCatalog(ctx context.Context, _ clickhouse.CompiledFieldCatalog) (queryexec.FieldCatalogResult, error) {
	executor.once.Do(func() { close(executor.entered) })
	<-ctx.Done()
	close(executor.exited)
	return queryexec.FieldCatalogResult{}, ctx.Err()
}

type runtimeBlockingSuggestionSearches struct {
	runtimeCompletedSearches
	entered chan struct{}
	exited  chan struct{}
	once    sync.Once
}

func (searches *runtimeBlockingSuggestionSearches) Validate(
	ctx context.Context,
	_ searchjobs.ValidateRequest,
) (searchjobs.ValidationResult, error) {
	searches.once.Do(func() { close(searches.entered) })
	<-ctx.Done()
	close(searches.exited)
	return searchjobs.ValidationResult{}, ctx.Err()
}

type runtimeFieldSummaryCompiler struct{ runtimeTimelineCompiler }

func (runtimeFieldSummaryCompiler) CompileFieldSummary(_ *plan.Query, spec clickhouse.FieldSummarySpec) (clickhouse.CompiledFieldSummary, error) {
	return clickhouse.CompiledFieldSummary{SQL: "SELECT field summary", Spec: spec, FieldKnown: true}, nil
}

type runtimeFieldSummaryExecutor struct{ runtimeTimelineExecutor }

func (runtimeFieldSummaryExecutor) ExecuteFieldSummary(_ context.Context, compiled clickhouse.CompiledFieldSummary) (queryexec.FieldSummaryResult, error) {
	return queryexec.FieldSummaryResult{
		FieldName:     compiled.Spec.FieldName,
		ObservedTypes: []eventfields.StoredValueType{eventfields.StoredValueTypeString},
		EventCount:    3,
		DistinctCount: 2,
		TopValues: []queryexec.FieldValueCountRow{
			{Value: searchjobs.StringValue("error"), Count: 2},
			{Value: searchjobs.StringValue("info"), Count: 1},
		},
	}, nil
}

type runtimeBlockingFieldSummaryExecutor struct {
	runtimeTimelineExecutor
	entered chan struct{}
	exited  chan struct{}
	once    sync.Once
}

func (executor *runtimeBlockingFieldSummaryExecutor) ExecuteFieldSummary(ctx context.Context, _ clickhouse.CompiledFieldSummary) (queryexec.FieldSummaryResult, error) {
	executor.once.Do(func() { close(executor.entered) })
	<-ctx.Done()
	close(executor.exited)
	return queryexec.FieldSummaryResult{}, ctx.Err()
}

func newRuntimeSearchAnalysisForTest(t *testing.T) *runtimeSearchAnalysis {
	t.Helper()
	analysis, err := newRuntimeSearchAnalysis(runtimeSearchAnalysisConfig{
		Searches: runtimeCompletedSearches{}, Compiler: runtimeTimelineCompiler{}, Executor: runtimeTimelineExecutor{},
	})
	if err != nil {
		t.Fatalf("newRuntimeSearchAnalysis: %v", err)
	}
	t.Cleanup(func() {
		if err := analysis.Close(); err != nil {
			t.Errorf("analysis.Close: %v", err)
		}
	})
	return analysis
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
