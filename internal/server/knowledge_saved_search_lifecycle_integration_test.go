package server

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"path/filepath"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/audit"
	"github.com/Suhaibinator/open-splunk/internal/auth"
	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
	"github.com/Suhaibinator/open-splunk/internal/control"
	exportjobs "github.com/Suhaibinator/open-splunk/internal/export"
	"github.com/Suhaibinator/open-splunk/internal/knowledgecatalog"
	"github.com/Suhaibinator/open-splunk/internal/knowledgeprogram"
	"github.com/Suhaibinator/open-splunk/internal/savedobjects"
	"github.com/Suhaibinator/open-splunk/internal/searchhistory"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

const (
	knowledgeSavedSearchTenantID = "knowledge-saved-search-tenant"
	knowledgeSavedSearchOwnerID  = "knowledge-saved-search-owner"
	knowledgeSavedSearchAppID    = "app_000000000900000000001Q"
	knowledgeSavedSearchID       = "saved-knowledge-lifecycle"
	knowledgeSavedSearchObjectID = "ko_saved_search_lifecycle"
	knowledgeSavedSearchField    = "saved_lifecycle_kind"
	knowledgeSavedSearchScore    = "saved_lifecycle_score"
	knowledgeSavedSearchV1       = "alpha"
	knowledgeSavedSearchV2       = "beta"
	knowledgeSavedSearchSPL      = "index=main | eval saved_lifecycle_score=len(saved_lifecycle_kind)+1 | where saved_lifecycle_score IN (5, 6) | table saved_lifecycle_kind saved_lifecycle_score"
)

var knowledgeSavedSearchCursorKey = []byte(
	"knowledge-saved-search-cursor-key-at-least-32-bytes",
)

type knowledgeSavedSearchExecutor struct {
	firstStarted chan clickhouse.CompiledQuery
	releaseFirst chan struct{}
	releaseOnce  sync.Once
	ordinal      atomic.Int32
	mu           sync.Mutex
	queries      []clickhouse.CompiledQuery
}

func newKnowledgeSavedSearchExecutor() *knowledgeSavedSearchExecutor {
	return &knowledgeSavedSearchExecutor{
		firstStarted: make(chan clickhouse.CompiledQuery, 1),
		releaseFirst: make(chan struct{}),
	}
}

func (executor *knowledgeSavedSearchExecutor) Execute(
	ctx context.Context,
	compiled clickhouse.CompiledQuery,
	sink searchjobs.ResultSink,
) error {
	detached, ok := compiled.CloneForExecution()
	if !ok {
		return errors.New("knowledge saved-search executor received invalid compiler authority")
	}
	executor.mu.Lock()
	executor.queries = append(executor.queries, detached)
	executor.mu.Unlock()
	if executor.ordinal.Add(1) == 1 {
		select {
		case executor.firstStarted <- detached:
		case <-ctx.Done():
			return ctx.Err()
		}
		select {
		case <-executor.releaseFirst:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if err := sink.SetSchema(searchjobs.Schema{Columns: []searchjobs.Column{
		{Name: knowledgeSavedSearchField, Kind: searchjobs.ValueKindMixed, Nullable: true},
		{Name: knowledgeSavedSearchScore, Kind: searchjobs.ValueKindDouble, Nullable: true},
	}}); err != nil {
		return err
	}
	return sink.AddRow([]searchjobs.Value{
		searchjobs.StringValue("retained"),
		searchjobs.DoubleValue(6),
	})
}

func (executor *knowledgeSavedSearchExecutor) Release() {
	executor.releaseOnce.Do(func() { close(executor.releaseFirst) })
}

func (executor *knowledgeSavedSearchExecutor) Queries() []clickhouse.CompiledQuery {
	executor.mu.Lock()
	defer executor.mu.Unlock()
	result := make([]clickhouse.CompiledQuery, len(executor.queries))
	for index := range executor.queries {
		result[index], _ = executor.queries[index].CloneForExecution()
	}
	return result
}

type knowledgeSavedSearchAppCatalog struct {
	catalog *control.AppCatalog
}

func (catalog knowledgeSavedSearchAppCatalog) ListActiveApps(
	ctx context.Context,
	tenantID string,
	maximum uint32,
) (AppCatalogResult, error) {
	result, err := catalog.catalog.ListApps(ctx, control.AppAccessScope{TenantID: tenantID}, control.AppListRequest{
		PageSize:     maximum,
		StateFilters: []control.AppState{control.AppStateActive},
		SortBy:       control.AppSortByDisplayName,
		Direction:    control.AppSortAscending,
	})
	if err != nil {
		return AppCatalogResult{}, err
	}
	apps := make([]AppCatalogSummary, len(result.Apps))
	for index, app := range result.Apps {
		apps[index] = AppCatalogSummary{
			AppID:             app.ID,
			Slug:              app.Definition.Slug,
			DisplayName:       app.Definition.DisplayName,
			DefaultIndexNames: slices.Clone(app.Definition.DefaultIndexes),
		}
	}
	return AppCatalogResult{Apps: apps, Complete: result.NextPageToken == nil}, nil
}

func TestSavedSearchAndHistoryRerunResolveCurrentKnowledgeWhileExportRetainsOriginal(t *testing.T) {
	ctx := t.Context()
	anchor := time.Date(2026, time.August, 10, 12, 0, 0, 0, time.UTC)
	database, err := control.Open(ctx, filepath.Join(t.TempDir(), "control.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if closeErr := database.Close(); closeErr != nil {
			t.Errorf("close control database: %v", closeErr)
		}
	})
	if _, err := database.CreateIndex(ctx, control.IndexDefinition{
		Name: "main", IngestionEnabled: true, SearchEnabled: true,
	}); err != nil {
		t.Fatal(err)
	}
	appCatalog, err := control.NewAppCatalog(database, control.AppCatalogOptions{
		CursorKey: knowledgeSavedSearchCursorKey,
		Clock:     func() time.Time { return anchor },
		IDGenerator: func() (string, error) {
			return knowledgeSavedSearchAppID, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := appCatalog.CreateApp(ctx, control.AppAccessScope{TenantID: knowledgeSavedSearchTenantID}, control.AppDefinition{
		Slug: "knowledge-saved-search", DisplayName: "Knowledge Saved Search",
	}); err != nil {
		t.Fatal(err)
	}

	auditStore, err := audit.NewStore(database, audit.StoreOptions{CursorKey: knowledgeSavedSearchCursorKey})
	if err != nil {
		t.Fatal(err)
	}
	knowledgeStore, err := knowledgecatalog.New(database, knowledgecatalog.Options{CursorKey: knowledgeSavedSearchCursorKey})
	if err != nil {
		t.Fatal(err)
	}
	resolver, err := knowledgeStore.NewResolver(knowledgecatalog.ResolverOptions{})
	if err != nil {
		t.Fatal(err)
	}
	writer, err := knowledgecatalog.NewWriter(database, auditStore, knowledgecatalog.WriterOptions{
		IDGenerator: func() (string, error) { return knowledgeSavedSearchObjectID, nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	actor, err := audit.WithActor(ctx, audit.Actor{
		Kind: audit.ActorKindBrowser, ID: knowledgeSavedSearchOwnerID, Role: audit.ActorRoleAdministrator,
	})
	if err != nil {
		t.Fatal(err)
	}
	writeScope := knowledgecatalog.WriteScope{
		TenantID:       knowledgeSavedSearchTenantID,
		OwnerID:        knowledgeSavedSearchOwnerID,
		WritableAppIDs: []string{knowledgeSavedSearchAppID},
	}
	createdObject, err := writer.Create(actor, writeScope, &opensplunk.CreateKnowledgeObjectRequest{
		Definition:      knowledgeSavedSearchDefinition(knowledgeSavedSearchV1),
		InitialState:    opensplunk.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_ACTIVE,
		ClientRequestId: "knowledge-saved-search-v1",
	})
	if err != nil {
		t.Fatalf("publish saved-search knowledge v1: %v", err)
	}
	objectV1 := createdObject.GetKnowledgeObject()
	if objectV1.GetKnowledgeObjectId() != knowledgeSavedSearchObjectID || objectV1.GetVersion() != 1 {
		t.Fatalf("saved-search knowledge v1 = %v", objectV1)
	}

	savedSearches, err := savedobjects.New(database, savedobjects.Options{
		Clock:       func() time.Time { return anchor },
		IDGenerator: func() (string, error) { return knowledgeSavedSearchID, nil },
		CursorKey:   knowledgeSavedSearchCursorKey,
	})
	if err != nil {
		t.Fatal(err)
	}
	savedDefinition := savedSearchDefinition(
		knowledgeSavedSearchOwnerID,
		knowledgeSavedSearchAppID,
		"Knowledge lifecycle",
	)
	savedDefinition.Search.Spl = knowledgeSavedSearchSPL
	savedDefinition.Search.IndexScope = []string{"main"}
	timezone := "UTC"
	savedDefinition.Search.TimeRange = &opensplunk.TimeRangeSpec{
		Earliest: new("-1h"), Latest: new("now"), Timezone: &timezone,
	}
	saved, err := savedSearches.Create(
		ctx,
		savedobjects.AccessScope{OwnerID: knowledgeSavedSearchOwnerID},
		savedDefinition,
	)
	if err != nil {
		t.Fatal(err)
	}

	history, err := searchhistory.New(database, searchhistory.Options{
		Clock: func() time.Time { return anchor }, CursorKey: knowledgeSavedSearchCursorKey,
	})
	if err != nil {
		t.Fatal(err)
	}
	journal, err := searchhistory.NewJobJournal(history)
	if err != nil {
		t.Fatal(err)
	}
	executor := newKnowledgeSavedSearchExecutor()
	jobIDs := []string{"knowledge-saved-v1", "knowledge-saved-v2", "knowledge-history-v2"}
	var nextJob atomic.Int32
	manager, err := searchjobs.New(searchjobs.Config{
		Executor:          executor,
		Snapshotter:       provenanceIntegrationSnapshotter(17),
		Journal:           journal,
		KnowledgeResolver: resolver,
		Compiler:          clickhouse.Compiler{Database: "open_splunk", Table: "events"},
		MaxConcurrent:     1,
		MaxQueued:         3,
		MaxRows:           10,
		MaxBytes:          1 << 20,
		RetentionTTL:      time.Hour,
		CleanupInterval:   -1,
		Now:               func() time.Time { return anchor },
		NewID: func() string {
			return jobIDs[nextJob.Add(1)-1]
		},
		CursorKey: knowledgeSavedSearchCursorKey,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		executor.Release()
		if closeErr := manager.Close(); closeErr != nil {
			t.Errorf("close search manager: %v", closeErr)
		}
	})
	if !manager.KnowledgeAdmissionEnabled() || manager.LookupAdmissionEnabled() {
		t.Fatal("saved-search lifecycle manager reported incorrect knowledge or lookup capabilities")
	}
	principalFactory, err := auth.NewBearerTokenAuthenticator(
		[]byte("knowledge-saved-search-browser-token"),
		knowledgeSavedSearchTenantID,
		knowledgeSavedSearchOwnerID,
		auth.BrowserRoleAdministrator,
	)
	if err != nil {
		t.Fatal(err)
	}
	principal, err := principalFactory.Authenticate(
		ctx,
		[]byte("knowledge-saved-search-browser-token"),
	)
	if err != nil {
		t.Fatal(err)
	}
	handler := newTestHandler(t, Config{
		SearchJobs:           manager,
		Indexes:              database,
		SavedSearches:        savedSearches,
		SearchHistory:        history,
		AppCatalog:           knowledgeSavedSearchAppCatalog{catalog: appCatalog},
		BrowserAuthenticator: &knowledgeBoundaryAuthenticator{principal: principal},
		WebUI:                testUI(),
		OwnerID:              knowledgeSavedSearchOwnerID,
		TenantID:             knowledgeSavedSearchTenantID,
		Now:                  func() time.Time { return anchor },
	})

	savedRunDefinition := proto.Clone(saved.GetDefinition().GetSearch()).(*opensplunk.SearchDefinition)
	// Search creation consumes execution intent only. Saved-search presentation
	// defaults remain owned by the saved object and are not job input.
	savedRunDefinition.PreferredResultTab = opensplunk.SearchResultTab_SEARCH_RESULT_TAB_UNSPECIFIED
	savedRunDefinition.SelectedFields = nil
	savedRunDefinition.Visualization = nil
	savedRun := &opensplunk.CreateSearchJobRequest{
		Definition: savedRunDefinition,
		Source: &opensplunk.SearchJobSource{
			Origin:        opensplunk.SearchJobOrigin_SEARCH_JOB_ORIGIN_SAVED_SEARCH,
			SavedSearchId: new(saved.GetSavedSearchId()),
		},
	}
	original := createKnowledgeSavedSearchJob(t, handler, manager, savedRun, jobIDs[0])
	requireKnowledgeSavedSearchSummary(t, original.KnowledgeSnapshot, 1)
	savedSource := searchjobs.JobSource{
		Origin: searchjobs.JobOriginSavedSearch, ObjectID: knowledgeSavedSearchID,
	}
	if original.Source != savedSource {
		t.Fatalf("original saved-search source = %#v, want %#v", original.Source, savedSource)
	}
	var observedV1 clickhouse.CompiledQuery
	select {
	case observedV1 = <-executor.firstStarted:
	case <-time.After(3 * time.Second):
		t.Fatal("saved-search v1 did not reach executor")
	}

	definitionV2 := proto.Clone(objectV1.GetDefinition()).(*opensplunk.KnowledgeObjectDefinition)
	definitionV2.GetFieldExtraction().GetRegex().Pattern = knowledgeSavedSearchPattern(knowledgeSavedSearchV2)
	updatedObject, err := writer.Update(actor, writeScope, &opensplunk.UpdateKnowledgeObjectRequest{
		KnowledgeObjectId: objectV1.GetKnowledgeObjectId(),
		ExpectedVersion:   1,
		Definition:        definitionV2,
		UpdateMask:        &fieldmaskpb.FieldMask{Paths: []string{"field_extraction"}},
		ClientRequestId:   "knowledge-saved-search-v2",
	})
	if err != nil {
		t.Fatalf("publish saved-search knowledge v2: %v", err)
	}
	if updatedObject.GetKnowledgeObject().GetVersion() != 2 {
		t.Fatalf("saved-search knowledge v2 = %v", updatedObject.GetKnowledgeObject())
	}
	retainedWhileRunning, err := manager.GetForContext(ctx, searchjobs.AccessScope{
		TenantID: knowledgeSavedSearchTenantID, OwnerID: knowledgeSavedSearchOwnerID,
	}, original.ID)
	if err != nil {
		t.Fatal(err)
	}
	requireKnowledgeSavedSearchSummary(t, retainedWhileRunning.KnowledgeSnapshot, 1)

	freshSaved := createKnowledgeSavedSearchJob(t, handler, manager, savedRun, jobIDs[1])
	requireKnowledgeSavedSearchSummary(t, freshSaved.KnowledgeSnapshot, 2)
	if freshSaved.Source != savedSource {
		t.Fatalf("fresh saved-search source = %#v, want %#v", freshSaved.Source, savedSource)
	}
	if bytes.Equal(
		original.KnowledgeSnapshot.GetRef().GetSnapshotSha256(),
		freshSaved.KnowledgeSnapshot.GetRef().GetSnapshotSha256(),
	) {
		t.Fatal("saved-search rerun retained the stale v1 snapshot digest")
	}
	executor.Release()
	access := searchjobs.AccessScope{
		TenantID: knowledgeSavedSearchTenantID, OwnerID: knowledgeSavedSearchOwnerID,
	}
	original = waitKnowledgeSavedSearchJob(t, manager, access, original.ID)
	freshSaved = waitKnowledgeSavedSearchJob(t, manager, access, freshSaved.ID)
	originalHistory := waitKnowledgeSavedSearchHistory(t, history, original.ID)
	requireKnowledgeSavedSearchSummary(t, originalHistory.GetKnowledgeSnapshot(), 1)
	if originalHistory.GetSource().GetOrigin() !=
		opensplunk.SearchJobOrigin_SEARCH_JOB_ORIGIN_SAVED_SEARCH ||
		originalHistory.GetSource().GetSavedSearchId() != knowledgeSavedSearchID ||
		!sameKnowledgeSavedSearchDigest(
			originalHistory.GetKnowledgeSnapshot(),
			original.KnowledgeSnapshot,
		) {
		t.Fatal("durable original history did not retain exact saved-search v1 authority")
	}

	historyRerun := createKnowledgeSavedSearchJob(
		t,
		handler,
		manager,
		historyRerunRequest(original.ID),
		jobIDs[2],
	)
	requireKnowledgeSavedSearchSummary(t, historyRerun.KnowledgeSnapshot, 2)
	historySource := searchjobs.JobSource{
		Origin: searchjobs.JobOriginHistoryRerun, ObjectID: original.ID,
	}
	if historyRerun.Source != historySource ||
		!sameKnowledgeSavedSearchDigest(
			historyRerun.KnowledgeSnapshot,
			freshSaved.KnowledgeSnapshot,
		) {
		t.Fatal("history rerun did not admit the exact current saved-search v2 authority")
	}
	historyRerun = waitKnowledgeSavedSearchJob(t, manager, access, historyRerun.ID)
	rererunHistory := waitKnowledgeSavedSearchHistory(t, history, historyRerun.ID)
	requireKnowledgeSavedSearchSummary(t, rererunHistory.GetKnowledgeSnapshot(), 2)

	executionV1 := knowledgeSavedSearchExecution(t, manager, access, original.ID)
	executionSavedV2 := knowledgeSavedSearchExecution(t, manager, access, freshSaved.ID)
	executionHistoryV2 := knowledgeSavedSearchExecution(t, manager, access, historyRerun.ID)
	retainedV1 := requireKnowledgeSavedSearchExecution(t, executionV1, 1, knowledgeSavedSearchV1)
	retainedSavedV2 := requireKnowledgeSavedSearchExecution(t, executionSavedV2, 2, knowledgeSavedSearchV2)
	retainedHistoryV2 := requireKnowledgeSavedSearchExecution(t, executionHistoryV2, 2, knowledgeSavedSearchV2)
	if !sameKnowledgeSavedSearchDigest(retainedV1.KnowledgeSummary, original.KnowledgeSnapshot) ||
		!sameKnowledgeSavedSearchDigest(retainedSavedV2.KnowledgeSummary, freshSaved.KnowledgeSnapshot) ||
		!sameKnowledgeSavedSearchDigest(retainedHistoryV2.KnowledgeSummary, historyRerun.KnowledgeSnapshot) ||
		!sameKnowledgeSavedSearchDigest(retainedSavedV2.KnowledgeSummary, retainedHistoryV2.KnowledgeSummary) ||
		!retainedSavedV2.KnowledgePrelude.Equal(retainedHistoryV2.KnowledgePrelude) ||
		!retainedSavedV2.CompiledQuery.EqualForExecution(retainedHistoryV2.CompiledQuery) {
		t.Fatal("saved-search and history reruns did not share the exact current v2 authority")
	}
	for _, test := range []struct {
		name     string
		retained *searchjobs.RetainedKnowledgeExecution
	}{
		{name: "saved v1", retained: retainedV1},
		{name: "saved v2", retained: retainedSavedV2},
		{name: "history v2", retained: retainedHistoryV2},
	} {
		if !slices.Equal(test.retained.CompiledQuery.OutputFields, []string{
			knowledgeSavedSearchField,
			knowledgeSavedSearchScore,
		}) {
			t.Fatalf("%s output fields = %v, want knowledge-prelude/expression composition outputs", test.name, test.retained.CompiledQuery.OutputFields)
		}
	}
	if retainedV1.KnowledgePrelude.Equal(retainedSavedV2.KnowledgePrelude) ||
		retainedV1.CompiledQuery.EqualForExecution(retainedSavedV2.CompiledQuery) ||
		!retainedV1.CompiledQuery.EqualForExecution(observedV1) {
		t.Fatal("the retained program did not rotate with the knowledge-object revision")
	}

	reexecution, err := exportjobs.NewReexecutionSource(exportjobs.ReexecutionSourceConfig{
		Searches: manager,
		Executor: executor,
		Compiler: clickhouse.Compiler{Database: "retained_export_recompile_forbidden"},
	})
	if err != nil {
		t.Fatal(err)
	}
	exports, err := exportjobs.New(exportjobs.Config{
		Source:          reexecution,
		ArtifactDir:     t.TempDir(),
		MaxWorkers:      1,
		MaxQueued:       1,
		CleanupInterval: -1,
		NewID:           func() string { return "knowledge-saved-export-v1" },
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if closeErr := exports.Close(); closeErr != nil {
			t.Errorf("close export manager: %v", closeErr)
		}
	})
	exportJob, err := exports.Create(ctx, access, exportjobs.CreateRequest{
		SearchJobID: original.ID,
		Format:      exportjobs.FormatCSV,
		Columns:     []string{knowledgeSavedSearchField},
		RowLimit:    10,
		ByteLimit:   1 << 20,
		CSV:         exportjobs.CSVOptions{HeaderMode: exportjobs.CSVHeaderFieldNames},
	})
	if err != nil {
		t.Fatal(err)
	}
	exportJob = waitKnowledgeSavedSearchExport(t, exports, access, exportJob.ID)
	requireKnowledgeSavedSearchSummary(t, exportJob.KnowledgeSnapshot, 1)
	if !bytes.Equal(
		exportJob.KnowledgeSnapshot.GetRef().GetSnapshotSha256(),
		original.KnowledgeSnapshot.GetRef().GetSnapshotSha256(),
	) {
		t.Fatal("export did not preserve the original saved-search v1 digest")
	}
	queries := executor.Queries()
	if len(queries) != 4 ||
		!queries[0].EqualForExecution(retainedV1.CompiledQuery) ||
		!queries[1].EqualForExecution(retainedSavedV2.CompiledQuery) ||
		!queries[2].EqualForExecution(retainedHistoryV2.CompiledQuery) ||
		!queries[3].EqualForExecution(retainedV1.CompiledQuery) {
		t.Fatalf("saved-search lifecycle executor authorities = %d calls", len(queries))
	}
}

func knowledgeSavedSearchDefinition(value string) *opensplunk.KnowledgeObjectDefinition {
	return &opensplunk.KnowledgeObjectDefinition{
		AppId:        knowledgeSavedSearchAppID,
		Name:         "saved-search-lifecycle-extraction",
		SharingScope: opensplunk.SharingScope_SHARING_SCOPE_PRIVATE,
		Selector: &opensplunk.KnowledgeSelector{IndexPatterns: []*opensplunk.KnowledgeSelectorPattern{{
			MatchKind: opensplunk.KnowledgeSelectorMatchKind_KNOWLEDGE_SELECTOR_MATCH_KIND_EXACT,
			Value:     "main",
		}}},
		Body: &opensplunk.KnowledgeObjectDefinition_FieldExtraction{
			FieldExtraction: &opensplunk.FieldExtractionDefinition{
				InputField:        "_raw",
				OverwriteBehavior: opensplunk.KnowledgeOverwriteBehavior_KNOWLEDGE_OVERWRITE_BEHAVIOR_REPLACE_EXISTING,
				Extraction: &opensplunk.FieldExtractionDefinition_Regex{
					Regex: &opensplunk.RegexFieldExtractionDefinition{
						Pattern:      knowledgeSavedSearchPattern(value),
						OutputFields: []string{knowledgeSavedSearchField},
					},
				},
			},
		},
	}
}

func knowledgeSavedSearchPattern(value string) string {
	return `"kind":"(?P<` + knowledgeSavedSearchField + `>` + value + `)"`
}

func createKnowledgeSavedSearchJob(
	t *testing.T,
	handler *Handler,
	manager *searchjobs.Manager,
	request *opensplunk.CreateSearchJobRequest,
	wantID string,
) searchjobs.Job {
	t.Helper()
	response := postProto(t, handler, "/api/search/jobs/create", request)
	if response.Code != http.StatusOK {
		t.Fatalf("create knowledge saved-search job status=%d body=%q", response.Code, response.Body.String())
	}
	var decoded opensplunk.CreateSearchJobResponse
	unmarshalResponse(t, response, &decoded)
	if decoded.GetSearchJob().GetSearchJobId() != wantID {
		t.Fatalf("created knowledge saved-search job ID=%q, want %q", decoded.GetSearchJob().GetSearchJobId(), wantID)
	}
	job, err := manager.GetForContext(t.Context(), searchjobs.AccessScope{
		TenantID: knowledgeSavedSearchTenantID, OwnerID: knowledgeSavedSearchOwnerID,
	}, wantID)
	if err != nil {
		t.Fatalf("read created knowledge saved-search job: %v", err)
	}
	return job
}

func waitKnowledgeSavedSearchJob(
	t *testing.T,
	manager *searchjobs.Manager,
	access searchjobs.AccessScope,
	id string,
) searchjobs.Job {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		job, err := manager.GetForContext(t.Context(), access, id)
		if err != nil {
			t.Fatalf("read saved-search lifecycle job %q: %v", id, err)
		}
		if job.State.Terminal() {
			if job.State != searchjobs.StateCompleted || job.Failure != nil {
				t.Fatalf("saved-search lifecycle job %q state=%s failure=%#v", id, job.State, job.Failure)
			}
			return job
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("saved-search lifecycle job %q did not complete", id)
	return searchjobs.Job{}
}

func waitKnowledgeSavedSearchHistory(
	t *testing.T,
	history *searchhistory.Store,
	id string,
) *opensplunk.SearchHistoryEntry {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		entry, err := history.Get(t.Context(), searchhistory.AccessScope{
			TenantID: knowledgeSavedSearchTenantID, OwnerID: knowledgeSavedSearchOwnerID,
		}, id)
		if err == nil {
			return entry
		}
		if !errors.Is(err, control.ErrNotFound) {
			t.Fatalf("read saved-search lifecycle history %q: %v", id, err)
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("saved-search lifecycle history %q was not finalized", id)
	return nil
}

func knowledgeSavedSearchExecution(
	t *testing.T,
	manager *searchjobs.Manager,
	access searchjobs.AccessScope,
	id string,
) searchjobs.ExecutionSnapshot {
	t.Helper()
	execution, err := manager.CompletedExecutionSnapshotFor(t.Context(), access, id)
	if err != nil {
		t.Fatalf("open saved-search lifecycle execution %q: %v", id, err)
	}
	return execution
}

func requireKnowledgeSavedSearchExecution(
	t *testing.T,
	execution searchjobs.ExecutionSnapshot,
	version uint64,
	value string,
) *searchjobs.RetainedKnowledgeExecution {
	t.Helper()
	retained, err := execution.OpenRetainedKnowledgeExecution()
	if err != nil || retained == nil {
		t.Fatalf("open saved-search lifecycle v%d execution: retained=%#v error=%v", version, retained, err)
	}
	requireKnowledgeSavedSearchSummary(t, retained.KnowledgeSummary, version)
	requireKnowledgeSavedSearchProgram(t, retained.KnowledgePrelude, value)
	if execution.CompiledQuery == nil || !execution.CompiledQuery.EqualForExecution(retained.CompiledQuery) {
		t.Fatalf("saved-search lifecycle v%d compiler authority changed", version)
	}
	return retained
}

func requireKnowledgeSavedSearchSummary(
	t *testing.T,
	summary *opensplunk.KnowledgeSnapshotSummary,
	version uint64,
) {
	t.Helper()
	if summary == nil || summary.GetRef() == nil || summary.GetRef().GetObjectCount() != 1 ||
		len(summary.GetRef().GetSnapshotSha256()) != 32 || len(summary.GetObjects()) != 1 ||
		summary.GetObjects()[0].GetAuthorizedObject().GetKnowledgeObjectId() != knowledgeSavedSearchObjectID ||
		summary.GetObjects()[0].GetAuthorizedObject().GetVersion() != version {
		t.Fatalf("saved-search lifecycle v%d summary = %v", version, summary)
	}
}

func sameKnowledgeSavedSearchDigest(
	left *opensplunk.KnowledgeSnapshotSummary,
	right *opensplunk.KnowledgeSnapshotSummary,
) bool {
	return left != nil && right != nil &&
		bytes.Equal(
			left.GetRef().GetSnapshotSha256(),
			right.GetRef().GetSnapshotSha256(),
		)
}

func requireKnowledgeSavedSearchProgram(
	t *testing.T,
	program knowledgeprogram.Program,
	value string,
) {
	t.Helper()
	extractions := program.RegexExtractions()
	var captures []knowledgeprogram.Capture
	if len(extractions) == 1 {
		captures = extractions[0].Captures()
	}
	if program.IsZero() || program.ObjectCount() != 1 || len(extractions) != 1 ||
		extractions[0].Pattern() != "(?-s)"+knowledgeSavedSearchPattern(value) ||
		len(captures) != 1 || captures[0].Name() != knowledgeSavedSearchField || captures[0].Group() != 1 {
		t.Fatalf("saved-search lifecycle %s program = zero:%t objects:%d extractions:%#v", value, program.IsZero(), program.ObjectCount(), extractions)
	}
}

func waitKnowledgeSavedSearchExport(
	t *testing.T,
	manager *exportjobs.Manager,
	access searchjobs.AccessScope,
	id string,
) exportjobs.Job {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		job, err := manager.Get(t.Context(), access, id)
		if err != nil {
			t.Fatalf("read saved-search lifecycle export %q: %v", id, err)
		}
		switch job.State {
		case exportjobs.StateCompleted:
			return job
		case exportjobs.StateFailed, exportjobs.StateCanceled, exportjobs.StateExpired:
			t.Fatalf("saved-search lifecycle export %q state=%s failure=%#v", id, job.State, job.Failure)
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("saved-search lifecycle export %q did not complete", id)
	return exportjobs.Job{}
}
