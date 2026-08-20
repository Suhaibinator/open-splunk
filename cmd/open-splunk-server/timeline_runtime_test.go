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

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/auth"
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

const runtimeAdministratorToken = "open-splunk-runtime-administrator-test-token"

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

func (runtimeTimelineCompiler) CompileTimelineContext(context.Context, *plan.Query, clickhouse.TimelineSpec) (clickhouse.CompiledTimeline, error) {
	return clickhouse.CompiledTimeline{}, nil
}

func (runtimeTimelineCompiler) CompileFieldCatalogContext(_ context.Context, _ *plan.Query, spec clickhouse.FieldCatalogSpec) (clickhouse.CompiledFieldCatalog, error) {
	return clickhouse.CompiledFieldCatalog{SQL: "SELECT field catalog", Spec: spec}, nil
}

func (runtimeTimelineCompiler) CompileFieldSummaryContext(_ context.Context, _ *plan.Query, spec clickhouse.FieldSummarySpec) (clickhouse.CompiledFieldSummary, error) {
	return clickhouse.CompiledFieldSummary{SQL: "SELECT field summary", Spec: spec}, nil
}

func (runtimeTimelineCompiler) CompileFieldSuggestionsContext(
	_ context.Context,
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

func (jobs runtimeSearchJobs) GetForContext(
	ctx context.Context,
	scope searchjobs.AccessScope,
	id string,
) (searchjobs.Job, error) {
	if err := ctx.Err(); err != nil {
		return searchjobs.Job{}, err
	}
	return jobs.GetFor(scope, id)
}

func (runtimeSearchJobs) PreviewFor(searchjobs.AccessScope, string, int) (searchjobs.PreviewSnapshot, error) {
	return searchjobs.PreviewSnapshot{}, searchjobs.ErrNotFound
}

func (jobs runtimeSearchJobs) PreviewForBytes(scope searchjobs.AccessScope, id string, limit int, _ uint64) (searchjobs.PreviewSnapshot, error) {
	return jobs.PreviewFor(scope, id, limit)
}

func (jobs runtimeSearchJobs) PreviewForBytesContext(
	ctx context.Context,
	scope searchjobs.AccessScope,
	id string,
	limit int,
	maximumBytes uint64,
) (searchjobs.PreviewSnapshot, error) {
	if err := ctx.Err(); err != nil {
		return searchjobs.PreviewSnapshot{}, err
	}
	return jobs.PreviewForBytes(scope, id, limit, maximumBytes)
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

type runtimeIndexAdministration struct{}

func (runtimeIndexAdministration) CreateIndex(
	context.Context,
	control.IndexDefinition,
) (control.Index, error) {
	return control.Index{}, errors.New("unexpected index creation")
}

func (runtimeIndexAdministration) GetIndex(
	_ context.Context,
	id string,
) (control.Index, error) {
	record := runtimeIndexRecord()
	if id != record.ID {
		return control.Index{}, control.ErrNotFound
	}
	return record, nil
}

func (runtimeIndexAdministration) GetIndexByName(
	_ context.Context,
	name string,
) (control.Index, error) {
	record := runtimeIndexRecord()
	if name != record.Definition.Name {
		return control.Index{}, control.ErrNotFound
	}
	return record, nil
}

func (runtimeIndexAdministration) ListIndexPage(
	context.Context,
	control.IndexListRequest,
) (control.IndexListResult, error) {
	return control.IndexListResult{
		Indexes:         []control.Index{runtimeIndexRecord()},
		CatalogRevision: 1,
	}, nil
}

func (runtimeIndexAdministration) UpdateIndex(
	context.Context,
	string,
	uint64,
	control.IndexDefinition,
) (control.Index, error) {
	return control.Index{}, errors.New("unexpected index update")
}

func (runtimeIndexAdministration) SetIndexState(
	context.Context,
	string,
	uint64,
	control.IndexState,
) (control.Index, error) {
	return control.Index{}, errors.New("unexpected index state update")
}

func (runtimeIndexAdministration) DeleteIndex(
	context.Context,
	string,
	uint64,
	string,
) (string, error) {
	return "", errors.New("unexpected index deletion")
}

func runtimeIndexRecord() control.Index {
	return control.Index{
		ID:      "idx_runtime_main",
		Version: 1,
		Definition: control.IndexDefinition{
			Name:             "main",
			DisplayName:      "Main",
			IngestionEnabled: true,
			SearchEnabled:    true,
		},
		State:     control.IndexStateActive,
		CreatedAt: time.Date(2026, 7, 22, 0, 0, 0, 0, time.UTC),
		UpdatedAt: time.Date(2026, 7, 22, 0, 0, 0, 0, time.UTC),
	}
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

func (runtimeSavedSearches) Create(context.Context, savedobjects.AccessScope, *opensplunk.SavedSearchDefinition) (*opensplunk.SavedSearch, error) {
	return nil, control.ErrNotFound
}

func (runtimeSavedSearches) Get(context.Context, savedobjects.AccessScope, string) (*opensplunk.SavedSearch, error) {
	return nil, control.ErrNotFound
}

func (runtimeSavedSearches) List(context.Context, savedobjects.AccessScope, savedobjects.ListRequest) (savedobjects.ListResult, error) {
	return savedobjects.ListResult{}, nil
}

func (runtimeSavedSearches) Update(context.Context, savedobjects.AccessScope, string, uint64, *opensplunk.SavedSearchDefinition, *fieldmaskpb.FieldMask) (*opensplunk.SavedSearch, error) {
	return nil, control.ErrNotFound
}

func (runtimeSavedSearches) Duplicate(context.Context, savedobjects.AccessScope, string, string, *string) (*opensplunk.SavedSearch, error) {
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

	payload, err := proto.Marshal(&opensplunk.GetSystemBootstrapRequest{})
	if err != nil {
		t.Fatalf("marshal bootstrap request: %v", err)
	}
	request := httptest.NewRequestWithContext(context.Background(), http.MethodPost, "http://127.0.0.1/api/system/bootstrap", bytes.NewReader(payload))
	request.Header.Set("Content-Type", "application/x-protobuf")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("bootstrap status = %d, body = %s", response.Code, response.Body.String())
	}
	var decoded opensplunk.GetSystemBootstrapResponse
	if err := proto.Unmarshal(response.Body.Bytes(), &decoded); err != nil {
		t.Fatalf("unmarshal bootstrap response: %v", err)
	}
	if !slices.Contains(decoded.GetFeatures(), opensplunk.ServerFeature_SERVER_FEATURE_TIMELINE) {
		t.Fatalf("bootstrap features = %v, want timeline", decoded.GetFeatures())
	}
	if decoded.GetLimits().GetMaximumTimelineBuckets() != 1_000 {
		t.Fatalf("maximum timeline buckets = %d, want enforcing service default 1000", decoded.GetLimits().GetMaximumTimelineBuckets())
	}
	if !slices.Contains(decoded.GetFeatures(), opensplunk.ServerFeature_SERVER_FEATURE_FIELD_DISCOVERY) {
		t.Fatalf("bootstrap features = %v, want field discovery", decoded.GetFeatures())
	}
	if slices.Contains(decoded.GetFeatures(), opensplunk.ServerFeature_SERVER_FEATURE_INDEX_ADMIN) {
		t.Fatalf(
			"partial index administration advertised as complete: %v",
			decoded.GetFeatures(),
		)
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
	payload, err := proto.Marshal(&opensplunk.GetSearchSuggestionsRequest{
		Spl:              source,
		CursorByteOffset: uint64(cursor),
		TimeRange: &opensplunk.TimeRangeSpec{
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
		"http://127.0.0.1/api/search/suggestions",
		bytes.NewReader(payload),
	)
	request.Header.Set("Content-Type", "application/x-protobuf")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("suggestion status = %d, body = %s", response.Code, response.Body.String())
	}
	var decoded opensplunk.GetSearchSuggestionsResponse
	if err := proto.Unmarshal(response.Body.Bytes(), &decoded); err != nil {
		t.Fatalf("unmarshal suggestion response: %v", err)
	}
	foundHead := false
	for _, suggestion := range decoded.GetSuggestions() {
		if suggestion.GetKind() == opensplunk.SearchSuggestionKind_SEARCH_SUGGESTION_KIND_COMMAND &&
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
	snapshot := runtimeFieldExecutionSnapshot(t)
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

	payload, err := proto.Marshal(&opensplunk.ListSearchFieldsRequest{
		SearchJobId: snapshot.ID,
		Page:        &opensplunk.PageRequest{IncludeTotalSize: true},
	})
	if err != nil {
		t.Fatalf("marshal field request: %v", err)
	}
	request := httptest.NewRequestWithContext(context.Background(), http.MethodPost, "http://127.0.0.1/api/search/jobs/fields/list", bytes.NewReader(payload))
	request.Header.Set("Content-Type", "application/x-protobuf")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("field-list status = %d, body = %s", response.Code, response.Body.String())
	}
	var decoded opensplunk.ListSearchFieldsResponse
	if err := proto.Unmarshal(response.Body.Bytes(), &decoded); err != nil {
		t.Fatalf("unmarshal field response: %v", err)
	}
	if len(decoded.GetFields()) != 0 || decoded.GetPage() == nil || decoded.GetPage().TotalSize == nil ||
		decoded.GetPage().GetTotalSize() != 0 || !decoded.GetPage().GetTotalSizeExact() {
		t.Fatalf("field-list response = %+v", &decoded)
	}
}

func TestRuntimeHTTPHandlerServesConfiguredIndexFieldCatalog(t *testing.T) {
	snapshot := runtimeFieldExecutionSnapshot(t)
	analysis, err := newRuntimeSearchAnalysis(runtimeSearchAnalysisConfig{
		Searches: runtimeSnapshotSearches{snapshot: snapshot},
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
	config.OwnerID = snapshot.OwnerID
	config.TenantID = snapshot.TenantID
	handler, err := newRuntimeHTTPHandler(config, analysis)
	if err != nil {
		t.Fatalf("newRuntimeHTTPHandler: %v", err)
	}

	earliest := "2026-07-22T01:00:00Z"
	latest := "2026-07-22T02:00:00Z"
	payload, err := proto.Marshal(&opensplunk.ListIndexFieldsRequest{
		Selector: &opensplunk.IndexSelector{
			Selector: &opensplunk.IndexSelector_IndexName{
				IndexName: "main",
			},
		},
		TimeRange: &opensplunk.TimeRangeSpec{
			Earliest: &earliest,
			Latest:   &latest,
		},
		Page: &opensplunk.PageRequest{IncludeTotalSize: true},
	})
	if err != nil {
		t.Fatalf("marshal index field request: %v", err)
	}
	request := httptest.NewRequestWithContext(
		context.Background(),
		http.MethodPost,
		"http://127.0.0.1/api/indexes/fields/list",
		bytes.NewReader(payload),
	)
	request.Header.Set("Content-Type", "application/x-protobuf")
	request.Header.Set(
		"Authorization",
		"Bearer "+runtimeAdministratorToken,
	)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf(
			"index field-list status = %d, body = %s",
			response.Code,
			response.Body.String(),
		)
	}
	var decoded opensplunk.ListIndexFieldsResponse
	if err := proto.Unmarshal(response.Body.Bytes(), &decoded); err != nil {
		t.Fatalf("unmarshal index field response: %v", err)
	}
	if len(decoded.GetFields()) != 0 ||
		decoded.GetPage() == nil ||
		decoded.GetPage().TotalSize == nil ||
		decoded.GetPage().GetTotalSize() != 0 ||
		!decoded.GetPage().GetTotalSizeExact() {
		t.Fatalf("index field-list response = %+v", &decoded)
	}
}

func TestRuntimeHTTPHandlerServesConfiguredFieldSummary(t *testing.T) {
	snapshot := runtimeFieldExecutionSnapshot(t)
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
	payload, err := proto.Marshal(&opensplunk.GetSearchFieldSummaryRequest{
		SearchJobId: snapshot.ID,
		FieldName:   "level",
		MaxValues:   &maximumValues,
	})
	if err != nil {
		t.Fatalf("marshal field summary request: %v", err)
	}
	request := httptest.NewRequestWithContext(context.Background(), http.MethodPost, "http://127.0.0.1/api/search/jobs/field-summary", bytes.NewReader(payload))
	request.Header.Set("Content-Type", "application/x-protobuf")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("field-summary status = %d, body = %s", response.Code, response.Body.String())
	}
	var decoded opensplunk.GetSearchFieldSummaryResponse
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
	config.IndexFields = &runtimeConfiguredFields{}
	if _, err := newRuntimeHTTPHandler(config, analysis); err == nil ||
		!strings.Contains(err.Error(), "already configured") {
		t.Fatalf("preconfigured index field error = %v", err)
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

func TestRuntimeFieldCatalogMemoryContract(t *testing.T) {
	if runtimeFieldAnalysisMaxConcurrent != 2 ||
		queryexec.MaximumFieldCatalogMemoryBytes != uint64(448<<20) ||
		runtimeFieldCatalogMemoryEnvelopeBytes != uint64(1<<30) ||
		!runtimeFieldCatalogMemoryContractValid() {
		t.Fatalf(
			"runtime field-catalog memory contract = shared concurrency %d cap %d envelope %d valid %t",
			runtimeFieldAnalysisMaxConcurrent,
			queryexec.MaximumFieldCatalogMemoryBytes,
			runtimeFieldCatalogMemoryEnvelopeBytes,
			runtimeFieldCatalogMemoryContractValid(),
		)
	}
	twoQueries := uint64(2) * queryexec.MaximumFieldCatalogMemoryBytes
	threeQueries := uint64(3) * queryexec.MaximumFieldCatalogMemoryBytes
	if twoQueries > runtimeFieldCatalogMemoryEnvelopeBytes ||
		threeQueries <= runtimeFieldCatalogMemoryEnvelopeBytes {
		t.Fatalf(
			"runtime field-catalog aggregate = two %d three %d envelope %d",
			twoQueries,
			threeQueries,
			runtimeFieldCatalogMemoryEnvelopeBytes,
		)
	}
}

func TestRuntimeFieldAnalysisAdmitsTwoCatalogsAndRejectsThirdWithoutWaiting(t *testing.T) {
	jobIDs := []string{"field-capacity-0", "field-capacity-1", "field-capacity-third"}
	snapshots := make(map[string]searchjobs.ExecutionSnapshot, len(jobIDs))
	for _, jobID := range jobIDs {
		snapshots[jobID] = runtimeFieldExecutionSnapshotWithID(t, jobID)
	}
	snapshot := snapshots[jobIDs[0]]
	searches := runtimeCapacityFieldSearches{snapshots: snapshots}
	executor := &runtimeCapacityFieldExecutor{
		entered: make(chan struct{}, runtimeFieldAnalysisMaxConcurrent),
		release: make(chan struct{}),
	}
	analysis, err := newRuntimeSearchAnalysis(runtimeSearchAnalysisConfig{
		Searches: searches,
		Compiler: runtimeTimelineCompiler{},
		Executor: executor,
	})
	if err != nil {
		t.Fatalf("newRuntimeSearchAnalysis: %v", err)
	}
	released := false
	t.Cleanup(func() {
		if !released {
			close(executor.release)
		}
		if err := analysis.Close(); err != nil {
			t.Errorf("analysis.Close cleanup: %v", err)
		}
	})

	access := searchjobs.AccessScope{TenantID: snapshot.TenantID, OwnerID: snapshot.OwnerID}
	results := make(chan error, runtimeFieldAnalysisMaxConcurrent)
	for index := range runtimeFieldAnalysisMaxConcurrent {
		jobID := jobIDs[index]
		go func() {
			_, err := analysis.fields.ListFields(
				context.Background(),
				access,
				searchanalysis.ListFieldsRequest{SearchJobID: jobID},
			)
			results <- err
		}()
	}
	for range runtimeFieldAnalysisMaxConcurrent {
		select {
		case <-executor.entered:
		case err := <-results:
			t.Fatalf("runtime field analysis returned before executor admission: %v", err)
		case <-time.After(5 * time.Second):
			t.Fatal("runtime field analysis did not admit two workers")
		}
	}

	started := time.Now()
	_, err = analysis.fields.ListFields(
		context.Background(),
		access,
		searchanalysis.ListFieldsRequest{SearchJobID: jobIDs[2]},
	)
	if !errors.Is(err, searchanalysis.ErrFieldAnalysisCapacity) {
		t.Fatalf("third field analysis error = %v, want capacity", err)
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("third field analysis waited %v instead of failing fast", elapsed)
	}

	close(executor.release)
	released = true
	for index := range runtimeFieldAnalysisMaxConcurrent {
		select {
		case err := <-results:
			if err != nil {
				t.Fatalf("admitted field analysis %d error = %v", index, err)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("admitted field analysis %d did not complete", index)
		}
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
	snapshot := runtimeFieldExecutionSnapshot(t)
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
	snapshot := runtimeFieldExecutionSnapshot(t)
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

func (*runtimeConfiguredFields) ListIndexFields(
	context.Context,
	searchjobs.AccessScope,
	searchanalysis.ListIndexFieldsRequest,
) (searchanalysis.FieldPage, error) {
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

func runtimeFieldExecutionSnapshot(t *testing.T) searchjobs.ExecutionSnapshot {
	return runtimeFieldExecutionSnapshotWithID(t, "job")
}

func runtimeFieldExecutionSnapshotWithID(
	t *testing.T,
	jobID string,
) searchjobs.ExecutionSnapshot {
	t.Helper()

	const (
		tenantID = "tenant"
		ownerID  = "owner"
		source   = "index=main level=error"
	)
	searchStart := time.Date(2026, 7, 22, 2, 2, 0, 0, time.UTC)
	expiresAt := time.Date(2099, 7, 22, 2, 2, 0, 0, time.UTC)
	earliest := time.Date(2026, 7, 22, 1, 0, 0, 0, time.UTC)
	latest := time.Date(2026, 7, 22, 2, 0, 0, 0, time.UTC)
	resolvedRange, err := searchtime.NewAbsoluteRange(earliest, latest)
	if err != nil {
		t.Fatalf("resolve field execution range: %v", err)
	}
	manager, err := searchjobs.New(searchjobs.Config{
		Executor:          runtimeFieldExecutionExecutor{},
		Snapshotter:       runtimeFieldExecutionSnapshotter{},
		Compiler:          clickhouse.Compiler{Database: "open_splunk", Table: "events"},
		KnowledgeResolver: nil,
		MaxConcurrent:     1,
		RetentionTTL:      expiresAt.Sub(searchStart),
		CleanupInterval:   -1,
		Now:               func() time.Time { return searchStart },
		NewID:             func() string { return jobID },
	})
	if err != nil {
		t.Fatalf("create field execution manager: %v", err)
	}
	t.Cleanup(func() {
		if err := manager.Close(); err != nil {
			t.Errorf("close field execution manager: %v", err)
		}
	})
	created, err := manager.Create(context.Background(), searchjobs.CreateRequest{
		SPL:               source,
		OwnerID:           ownerID,
		TenantID:          tenantID,
		AuthorizedIndexes: []string{"main"},
		RequestedIndexes:  []string{"main"},
		TimeRange:         resolvedRange,
		AppID:             "",
	})
	if err != nil {
		t.Fatalf("create field execution: %v", err)
	}
	waitForRuntimeKnowledgeJobState(t, manager, created.ID)
	snapshot, err := manager.CompletedExecutionSnapshotFor(
		context.Background(),
		searchjobs.AccessScope{TenantID: tenantID, OwnerID: ownerID},
		created.ID,
	)
	if err != nil {
		t.Fatalf("read completed field execution: %v", err)
	}
	if snapshot.ID != jobID || snapshot.TenantID != tenantID || snapshot.OwnerID != ownerID ||
		snapshot.AppID != "" || snapshot.SPL != source ||
		!slices.Equal(snapshot.EffectiveIndexes, []string{"main"}) ||
		!snapshot.Earliest.Equal(earliest) || !snapshot.Latest.Equal(latest) ||
		!snapshot.SearchStart.Equal(searchStart) || snapshot.SearchTimezone != "UTC" ||
		!snapshot.IndexTimeCutoff.Equal(searchStart) || snapshot.VisibilityCutoff != 1 ||
		!snapshot.FinishedAt.Equal(searchStart) || !snapshot.ExpiresAt.Equal(expiresAt) {
		t.Fatalf("completed field execution scope = %#v", snapshot)
	}
	if snapshot.CompiledQuery != nil || !snapshot.KnowledgeSnapshot.IsZero() {
		t.Fatalf(
			"legacy field execution retained knowledge authority = compiled:%#v snapshot-zero:%t",
			snapshot.CompiledQuery,
			snapshot.KnowledgeSnapshot.IsZero(),
		)
	}
	if authority, validateErr := snapshot.ValidateRetainedKnowledgeAuthority(); validateErr != nil || authority != (searchjobs.RetainedKnowledgeAuthorityDigests{}) {
		t.Fatalf(
			"ValidateRetainedKnowledgeAuthority(legacy) = (%#v, %v), want Present=false",
			authority,
			validateErr,
		)
	}
	if err := manager.Close(); err != nil {
		t.Fatalf("close field execution manager before returning snapshot: %v", err)
	}
	if authority, validateErr := snapshot.ValidateRetainedKnowledgeAuthority(); validateErr != nil || authority != (searchjobs.RetainedKnowledgeAuthorityDigests{}) {
		t.Fatalf(
			"ValidateRetainedKnowledgeAuthority(after manager close) = (%#v, %v), want Present=false",
			authority,
			validateErr,
		)
	}
	return snapshot
}

type runtimeFieldExecutionSnapshotter struct{}

func (runtimeFieldExecutionSnapshotter) VisibilityCutoff(ctx context.Context) (uint64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	return 1, nil
}

type runtimeFieldExecutionExecutor struct{}

func (runtimeFieldExecutionExecutor) Execute(
	ctx context.Context,
	compiled clickhouse.CompiledQuery,
	sink searchjobs.ResultSink,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	columns := make([]searchjobs.Column, len(compiled.OutputFields))
	for index, field := range compiled.OutputFields {
		columns[index] = searchjobs.Column{
			Name:     field,
			Kind:     searchjobs.ValueKindMixed,
			Nullable: true,
		}
	}
	return sink.SetSchema(searchjobs.Schema{Columns: columns})
}

func (searches runtimeSnapshotSearches) CompletedExecutionSnapshotFor(_ context.Context, _ searchjobs.AccessScope, _ string) (searchjobs.ExecutionSnapshot, error) {
	return searches.snapshot, nil
}

func (searches runtimeSnapshotSearches) SnapshotAnalysisScope(
	_ context.Context,
	request searchjobs.AnalysisScopeRequest,
) (searchjobs.AnalysisScopeSnapshot, error) {
	anchor := searches.snapshot.IndexTimeCutoff
	return searchjobs.AnalysisScopeSnapshot{
		TenantID:          request.TenantID,
		AuthorizedIndexes: slices.Clone(request.AuthorizedIndexes),
		RequestedIndexes:  slices.Clone(request.RequestedIndexes),
		TimeRange:         request.TimeRange,
		SearchStart:       anchor,
		IndexTimeCutoff:   anchor,
		VisibilityCutoff:  searches.snapshot.VisibilityCutoff,
	}, nil
}

type runtimeBlockingFieldExecutor struct {
	runtimeTimelineExecutor
	entered chan struct{}
	exited  chan struct{}
	once    sync.Once
}

type runtimeCapacityFieldSearches struct {
	runtimeCompletedSearches
	snapshots map[string]searchjobs.ExecutionSnapshot
}

func (searches runtimeCapacityFieldSearches) CompletedExecutionSnapshotFor(
	_ context.Context,
	access searchjobs.AccessScope,
	jobID string,
) (searchjobs.ExecutionSnapshot, error) {
	snapshot, ok := searches.snapshots[jobID]
	if !ok || snapshot.TenantID != access.TenantID || snapshot.OwnerID != access.OwnerID {
		return searchjobs.ExecutionSnapshot{}, searchjobs.ErrNotFound
	}
	return snapshot, nil
}

type runtimeCapacityFieldExecutor struct {
	runtimeTimelineExecutor
	entered chan struct{}
	release chan struct{}
}

func (executor *runtimeCapacityFieldExecutor) ExecuteFieldCatalog(
	ctx context.Context,
	_ clickhouse.CompiledFieldCatalog,
) (queryexec.FieldCatalogResult, error) {
	select {
	case executor.entered <- struct{}{}:
	case <-ctx.Done():
		return queryexec.FieldCatalogResult{}, ctx.Err()
	}
	select {
	case <-executor.release:
		return queryexec.FieldCatalogResult{Fields: []queryexec.FieldProfileRow{}}, nil
	case <-ctx.Done():
		return queryexec.FieldCatalogResult{}, ctx.Err()
	}
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

func (runtimeFieldSummaryCompiler) CompileFieldSummaryContext(_ context.Context, _ *plan.Query, spec clickhouse.FieldSummarySpec) (clickhouse.CompiledFieldSummary, error) {
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
	authenticator, err := auth.NewBearerTokenAuthenticator(
		[]byte(runtimeAdministratorToken),
		"tenant",
		"owner",
		auth.BrowserRoleAdministrator,
	)
	if err != nil {
		panic("construct runtime test browser authenticator: " + err.Error())
	}
	return server.Config{
		SearchJobs:           runtimeSearchJobs{},
		Indexes:              runtimeIndexCatalog{},
		IndexAdmin:           runtimeIndexAdministration{},
		SavedSearches:        runtimeSavedSearches{},
		BrowserAuthenticator: authenticator,
		TenantID:             "tenant",
		OwnerID:              "owner",
		WebUI: fstest.MapFS{
			"index.html": &fstest.MapFile{Data: []byte("<html>runtime</html>")},
		},
	}
}
