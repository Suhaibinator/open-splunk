package main

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/audit"
	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/knowledgecatalog"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
	"github.com/Suhaibinator/open-splunk/internal/searchtime"
	"github.com/Suhaibinator/open-splunk/internal/server"
	"google.golang.org/protobuf/proto"
)

const (
	runtimeKnowledgeTestTenant = "runtime-knowledge-tenant"
	runtimeKnowledgeTestOwner  = "runtime-knowledge-owner"
	runtimeKnowledgeTestApp    = "app_000000000800000000001A"
)

func TestDeriveKnowledgeCatalogCursorKeyIsStableAndPurposeSeparated(
	t *testing.T,
) {
	t.Parallel()

	master := bytes.Repeat([]byte{0x6d}, masterKeyBytes)
	first, err := deriveKnowledgeCatalogCursorKey(master)
	if err != nil {
		t.Fatal(err)
	}
	second, err := deriveKnowledgeCatalogCursorKey(master)
	if err != nil {
		t.Fatal(err)
	}
	appCatalogKey, appAdministrationKey, err := deriveAppCursorKeys(master)
	if err != nil {
		t.Fatal(err)
	}
	otherPurposes := []string{
		"saved-search-cursors",
		collectorCatalogCursorKeyPurpose,
		"collector-token-digests",
		"search-history-cursors",
		auditCursorKeyPurpose,
		searchAttemptAuditCursorKeyPurpose,
	}
	if len(first) != 32 || !bytes.Equal(first, second) ||
		bytes.Equal(first, appCatalogKey) ||
		bytes.Equal(first, appAdministrationKey) {
		t.Fatal("knowledge cursor key is not stable and app-purpose-separated")
	}
	for _, purpose := range otherPurposes {
		key, deriveErr := deriveServerKey(master, purpose)
		if deriveErr != nil {
			t.Fatalf("deriveServerKey(%q): %v", purpose, deriveErr)
		}
		if bytes.Equal(first, key) {
			t.Fatalf("knowledge cursor key collides with purpose %q", purpose)
		}
	}
	if _, err := deriveKnowledgeCatalogCursorKey(master[:len(master)-1]); err == nil {
		t.Fatal("knowledge cursor key accepted an invalid master key")
	}
}

func TestRuntimeKnowledgeManagementCursorContinuesAcrossReopen(t *testing.T) {
	ctx := t.Context()
	directory := t.TempDir()
	databasePath := filepath.Join(directory, "control.db")
	masterKeyPath := filepath.Join(directory, "server.key")

	firstDatabase, err := control.Open(ctx, databasePath)
	if err != nil {
		t.Fatal(err)
	}
	firstAudit := newRuntimeKnowledgeTestAudit(
		t,
		firstDatabase,
		masterKeyPath,
	)
	createRuntimeKnowledgeTestApp(t, firstDatabase)
	firstRuntime, err := newRuntimeKnowledgeManagement(
		ctx,
		firstDatabase,
		masterKeyPath,
		firstAudit,
	)
	if err != nil {
		_ = firstDatabase.Close()
		t.Fatal(err)
	}
	if firstRuntime.catalog == nil || firstRuntime.writer == nil ||
		firstRuntime.resolver == nil || firstRuntime.attempts == nil {
		_ = firstDatabase.Close()
		t.Fatalf("runtime knowledge dependencies = %#v", firstRuntime)
	}
	if !firstRuntime.writer.ReadyForManagement() {
		_ = firstDatabase.Close()
		t.Fatal("runtime writer is not ready for management")
	}

	actorContext, err := audit.WithActor(ctx, audit.Actor{
		Kind: audit.ActorKindBrowser,
		ID:   "runtime-knowledge-administrator",
		Role: audit.ActorRoleAdministrator,
	})
	if err != nil {
		_ = firstDatabase.Close()
		t.Fatal(err)
	}
	writeScope := knowledgecatalog.WriteScope{
		TenantID:       runtimeKnowledgeTestTenant,
		OwnerID:        runtimeKnowledgeTestOwner,
		WritableAppIDs: []string{runtimeKnowledgeTestApp},
	}
	for index, name := range []string{"alpha", "bravo"} {
		request := runtimeKnowledgeTestCreateRequest(
			name,
			[]string{"runtime-knowledge-create-alpha", "runtime-knowledge-create-bravo"}[index],
		)
		if _, err := firstRuntime.writer.Create(
			actorContext,
			writeScope,
			request,
		); err != nil {
			_ = firstDatabase.Close()
			t.Fatalf("create draft %q: %v", name, err)
		}
	}
	readScope := knowledgecatalog.ReadScope{
		TenantID:       runtimeKnowledgeTestTenant,
		OwnerID:        runtimeKnowledgeTestOwner,
		ReadableAppIDs: []string{runtimeKnowledgeTestApp},
	}
	request := knowledgecatalog.ListRequest{
		PageSize:      1,
		SortBy:        knowledgecatalog.SortByName,
		SortDirection: knowledgecatalog.SortAscending,
	}
	firstPage, err := firstRuntime.catalog.List(ctx, readScope, request)
	if err != nil {
		_ = firstDatabase.Close()
		t.Fatal(err)
	}
	if len(firstPage.Objects) != 1 || firstPage.Objects[0].Name != "alpha" ||
		firstPage.NextPageToken == "" {
		_ = firstDatabase.Close()
		t.Fatalf("first runtime knowledge page = %#v", firstPage)
	}
	request.PageToken = firstPage.NextPageToken
	if err := firstDatabase.Close(); err != nil {
		t.Fatal(err)
	}

	secondDatabase, err := control.Open(ctx, databasePath)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := secondDatabase.Close(); err != nil {
			t.Errorf("close reopened control database: %v", err)
		}
	}()
	secondAudit := newRuntimeKnowledgeTestAudit(
		t,
		secondDatabase,
		masterKeyPath,
	)
	secondRuntime, err := newRuntimeKnowledgeManagement(
		ctx,
		secondDatabase,
		masterKeyPath,
		secondAudit,
	)
	if err != nil {
		t.Fatal(err)
	}
	secondPage, err := secondRuntime.catalog.List(ctx, readScope, request)
	if err != nil {
		t.Fatalf("continue runtime knowledge cursor after reopen: %v", err)
	}
	if len(secondPage.Objects) != 1 || secondPage.Objects[0].Name != "bravo" ||
		secondPage.NextPageToken != "" ||
		secondPage.CatalogRevision != firstPage.CatalogRevision {
		t.Fatalf("continued runtime knowledge page = %#v", secondPage)
	}
}

func TestConfigureRuntimeKnowledgeManagementIsAtomicAndNarrow(t *testing.T) {
	t.Parallel()

	runtime, database := newRuntimeKnowledgeTestRuntime(t)
	defer func() { _ = database.Close() }()
	apps := &runtimeAppCatalog{catalog: &stubControlAppCatalog{}}
	var typedNilAppBackend *control.AppCatalog
	config := server.Config{Bootstrap: server.BootstrapConfig{
		Features: []opensplunkv1.ServerFeature{
			opensplunkv1.ServerFeature_SERVER_FEATURE_SEARCH,
		},
		SelectedAppID: "unchanged-app",
	}}
	if err := configureRuntimeKnowledgeManagement(&config, runtime, apps); err != nil {
		t.Fatal(err)
	}
	if config.KnowledgeCatalog != runtime.catalog ||
		config.KnowledgeWriter != runtime.writer ||
		config.KnowledgeApps != apps ||
		config.KnowledgeAttempts != runtime.attempts {
		t.Fatalf("configured knowledge dependencies = %#v", config)
	}
	if !slices.Equal(config.Bootstrap.Features, []opensplunkv1.ServerFeature{
		opensplunkv1.ServerFeature_SERVER_FEATURE_SEARCH,
	}) || config.Bootstrap.SelectedAppID != "unchanged-app" {
		t.Fatalf("knowledge composition changed bootstrap capability = %#v", config.Bootstrap)
	}

	missing := []struct {
		name    string
		runtime runtimeKnowledgeManagement
		apps    *runtimeAppCatalog
	}{
		{name: "catalog", runtime: runtimeKnowledgeManagement{resolver: runtime.resolver, writer: runtime.writer, attempts: runtime.attempts}, apps: apps},
		{name: "resolver", runtime: runtimeKnowledgeManagement{catalog: runtime.catalog, writer: runtime.writer, attempts: runtime.attempts}, apps: apps},
		{name: "writer", runtime: runtimeKnowledgeManagement{catalog: runtime.catalog, resolver: runtime.resolver, attempts: runtime.attempts}, apps: apps},
		{name: "unready writer", runtime: runtimeKnowledgeManagement{catalog: runtime.catalog, resolver: runtime.resolver, writer: &knowledgecatalog.Writer{}, attempts: runtime.attempts}, apps: apps},
		{name: "attempts", runtime: runtimeKnowledgeManagement{catalog: runtime.catalog, resolver: runtime.resolver, writer: runtime.writer}, apps: apps},
		{name: "apps", runtime: runtime, apps: nil},
		{name: "app backend", runtime: runtime, apps: &runtimeAppCatalog{}},
		{name: "typed nil app backend", runtime: runtime, apps: &runtimeAppCatalog{catalog: typedNilAppBackend}},
	}
	for _, test := range missing {
		t.Run("missing "+test.name, func(t *testing.T) {
			candidate := server.Config{}
			if err := configureRuntimeKnowledgeManagement(
				&candidate,
				test.runtime,
				test.apps,
			); err == nil || candidate.KnowledgeCatalog != nil ||
				candidate.KnowledgeWriter != nil || candidate.KnowledgeApps != nil ||
				candidate.KnowledgeAttempts != nil {
				t.Fatalf("partial configuration = (%#v, %v)", candidate, err)
			}
		})
	}
	if err := configureRuntimeKnowledgeManagement(nil, runtime, apps); err == nil {
		t.Fatal("nil server config was accepted")
	}

	var typedNilCatalog *knowledgecatalog.Store
	preconfigured := server.Config{KnowledgeCatalog: typedNilCatalog}
	before := preconfigured.KnowledgeCatalog
	if err := configureRuntimeKnowledgeManagement(
		&preconfigured,
		runtime,
		apps,
	); err == nil || preconfigured.KnowledgeCatalog != before ||
		preconfigured.KnowledgeWriter != nil || preconfigured.KnowledgeApps != nil ||
		preconfigured.KnowledgeAttempts != nil {
		t.Fatalf("typed-nil preconfiguration was overwritten = (%#v, %v)", preconfigured, err)
	}
}

func TestRuntimeKnowledgeCompositionLeavesSearchAndCapabilityGatesOff(
	t *testing.T,
) {
	runtime, database := newRuntimeKnowledgeTestRuntime(t)
	defer func() { _ = database.Close() }()
	apps := &runtimeAppCatalog{catalog: &stubControlAppCatalog{}}
	config := runtimeServerConfig()
	originalSearchJobs := config.SearchJobs
	if err := configureRuntimeKnowledgeManagement(&config, runtime, apps); err != nil {
		t.Fatal(err)
	}
	if config.SearchJobs != originalSearchJobs {
		t.Fatal("knowledge management composition replaced search admission")
	}
	analysis := newRuntimeSearchAnalysisForTest(t)
	handler, err := newRuntimeHTTPHandler(config, analysis)
	if err != nil {
		t.Fatal(err)
	}
	payload, err := proto.Marshal(&opensplunkv1.GetSystemBootstrapRequest{})
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequestWithContext(
		t.Context(),
		http.MethodPost,
		"http://127.0.0.1/api/v1/system/bootstrap",
		bytes.NewReader(payload),
	)
	request.Header.Set("Content-Type", "application/x-protobuf")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("bootstrap status = %d, body = %s", response.Code, response.Body.String())
	}
	var decoded opensplunkv1.GetSystemBootstrapResponse
	if err := proto.Unmarshal(response.Body.Bytes(), &decoded); err != nil {
		t.Fatal(err)
	}
	if slices.Contains(
		decoded.GetFeatures(),
		opensplunkv1.ServerFeature_SERVER_FEATURE_KNOWLEDGE_FIELD_OBJECTS,
	) {
		t.Fatalf("management-only runtime advertised knowledge execution: %v", decoded.GetFeatures())
	}
}

func TestRuntimeKnowledgeResolverFailsClosedForWriterPublishedActiveObject(
	t *testing.T,
) {
	runtime, database := newRuntimeKnowledgeTestRuntime(t)
	defer func() { _ = database.Close() }()
	createRuntimeKnowledgeTestApp(t, database)
	createRuntimeKnowledgeTestIndex(t, database)

	actorContext, err := audit.WithActor(t.Context(), audit.Actor{
		Kind: audit.ActorKindBrowser,
		ID:   "runtime-knowledge-admission-administrator",
		Role: audit.ActorRoleAdministrator,
	})
	if err != nil {
		t.Fatal(err)
	}
	activeRequest := runtimeKnowledgeTestCreateRequest(
		"active-alias",
		"runtime-knowledge-active-create-0001",
	)
	activeRequest.InitialState = opensplunkv1.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_ACTIVE
	activeRequest.Definition.Selector = &opensplunkv1.KnowledgeSelector{
		IndexPatterns: []*opensplunkv1.KnowledgeSelectorPattern{{Value: "main"}},
	}
	published, err := runtime.writer.Create(
		actorContext,
		knowledgecatalog.WriteScope{
			TenantID:       runtimeKnowledgeTestTenant,
			OwnerID:        runtimeKnowledgeTestOwner,
			WritableAppIDs: []string{runtimeKnowledgeTestApp},
		},
		activeRequest,
	)
	if err != nil {
		t.Fatalf("publish ACTIVE knowledge object: %v", err)
	}
	if published.GetKnowledgeObject().GetState() !=
		opensplunkv1.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_ACTIVE {
		t.Fatalf("published knowledge object = %v", published.GetKnowledgeObject())
	}

	resolution, err := runtime.resolver.Resolve(
		t.Context(),
		knowledgecatalog.ResolutionScope{
			TenantID:                   runtimeKnowledgeTestTenant,
			PrincipalID:                runtimeKnowledgeTestOwner,
			AppID:                      runtimeKnowledgeTestApp,
			EffectiveAuthorizedIndexes: []string{"main"},
		},
	)
	if err != nil {
		t.Fatalf("resolve Writer-published ACTIVE knowledge: %v", err)
	}
	if summary := resolution.Summary(); summary.ExecutableObjects != 1 ||
		resolution.Prelude().ObjectCount() != 1 {
		t.Fatalf(
			"resolved ACTIVE authority = (%#v, objects=%d)",
			summary,
			resolution.Prelude().ObjectCount(),
		)
	}

	counters := &runtimeKnowledgeAdmissionCounters{}
	manager := newRuntimeKnowledgeAdmissionManager(t, runtime.resolver, counters)
	defer func() {
		if err := manager.Close(); err != nil {
			t.Errorf("close knowledge admission manager: %v", err)
		}
	}()
	if !manager.KnowledgeAdmissionEnabled() {
		t.Fatal("concrete runtime resolver did not enable test-only knowledge admission")
	}
	job, err := manager.Create(t.Context(), runtimeKnowledgeSearchRequest(t))
	if !errors.Is(err, searchjobs.ErrKnowledgeUnavailable) ||
		err.Error() != searchjobs.ErrKnowledgeUnavailable.Error() {
		t.Fatalf("Create(nonempty ACTIVE authority) = (%#v, %v)", job, err)
	}
	if counters.snapshots.Load() != 1 || counters.ids.Load() != 0 ||
		counters.journalAdmissions.Load() != 0 ||
		counters.journalFinalizations.Load() != 0 ||
		counters.executions.Load() != 0 || len(manager.List()) != 0 {
		t.Fatalf(
			"failed nonempty admission side effects: snapshots=%d ids=%d journal=(%d,%d) executions=%d jobs=%d",
			counters.snapshots.Load(),
			counters.ids.Load(),
			counters.journalAdmissions.Load(),
			counters.journalFinalizations.Load(),
			counters.executions.Load(),
			len(manager.List()),
		)
	}
}

func TestRuntimeKnowledgeResolverEnabledEmptyAdmissionSucceeds(t *testing.T) {
	runtime, database := newRuntimeKnowledgeTestRuntime(t)
	defer func() { _ = database.Close() }()
	createRuntimeKnowledgeTestApp(t, database)
	createRuntimeKnowledgeTestIndex(t, database)

	resolution, err := runtime.resolver.Resolve(
		t.Context(),
		knowledgecatalog.ResolutionScope{
			TenantID:                   runtimeKnowledgeTestTenant,
			PrincipalID:                runtimeKnowledgeTestOwner,
			AppID:                      runtimeKnowledgeTestApp,
			EffectiveAuthorizedIndexes: []string{"main"},
		},
	)
	if err != nil {
		t.Fatalf("resolve empty runtime knowledge catalog: %v", err)
	}
	if summary := resolution.Summary(); summary.ExecutableObjects != 0 ||
		resolution.Prelude().ObjectCount() != 0 || resolution.IsZero() {
		t.Fatalf(
			"resolved empty authority = (%#v, objects=%d, zero=%t)",
			summary,
			resolution.Prelude().ObjectCount(),
			resolution.IsZero(),
		)
	}

	counters := &runtimeKnowledgeAdmissionCounters{
		finalized: make(chan struct{}, 1),
	}
	manager := newRuntimeKnowledgeAdmissionManager(t, runtime.resolver, counters)
	defer func() {
		if err := manager.Close(); err != nil {
			t.Errorf("close knowledge admission manager: %v", err)
		}
	}()
	created, err := manager.Create(t.Context(), runtimeKnowledgeSearchRequest(t))
	if err != nil {
		t.Fatalf("Create(enabled empty authority): %v", err)
	}
	if created.ID != "runtime-knowledge-search-0001" ||
		created.KnowledgeSnapshot == nil || created.KnowledgeSnapshot.GetRef() == nil ||
		created.KnowledgeSnapshot.GetRef().GetObjectCount() != 0 {
		t.Fatalf("created empty-authority job = %#v", created)
	}
	completed := waitForRuntimeKnowledgeJobState(
		t,
		manager,
		created.ID,
		searchjobs.StateCompleted,
	)
	select {
	case <-counters.finalized:
	case <-time.After(3 * time.Second):
		t.Fatal("completed empty-authority job was not finalized in the journal")
	}
	if completed.Failure != nil || counters.snapshots.Load() != 1 ||
		counters.ids.Load() != 1 || counters.journalAdmissions.Load() != 1 ||
		counters.journalFinalizations.Load() != 1 || counters.executions.Load() != 1 ||
		len(manager.List()) != 1 {
		t.Fatalf(
			"empty admission outcome=%#v counters=(snapshots=%d ids=%d journal=%d/%d executions=%d jobs=%d)",
			completed,
			counters.snapshots.Load(),
			counters.ids.Load(),
			counters.journalAdmissions.Load(),
			counters.journalFinalizations.Load(),
			counters.executions.Load(),
			len(manager.List()),
		)
	}
}

func newRuntimeKnowledgeTestRuntime(
	t *testing.T,
) (runtimeKnowledgeManagement, *control.DB) {
	t.Helper()
	directory := t.TempDir()
	database, err := control.Open(
		t.Context(),
		filepath.Join(directory, "control.db"),
	)
	if err != nil {
		t.Fatal(err)
	}
	masterKeyPath := filepath.Join(directory, "server.key")
	auditStore := newRuntimeKnowledgeTestAudit(t, database, masterKeyPath)
	runtime, err := newRuntimeKnowledgeManagement(
		t.Context(),
		database,
		masterKeyPath,
		auditStore,
	)
	if err != nil {
		_ = database.Close()
		t.Fatal(err)
	}
	return runtime, database
}

func newRuntimeKnowledgeTestAudit(
	t *testing.T,
	database *control.DB,
	masterKeyPath string,
) *audit.Store {
	t.Helper()
	masterKey, err := loadVerifiedMasterKey(t.Context(), database, masterKeyPath)
	if err != nil {
		t.Fatal(err)
	}
	defer clear(masterKey)
	cursorKey, err := deriveServerKey(masterKey, auditCursorKeyPurpose)
	if err != nil {
		t.Fatal(err)
	}
	defer clear(cursorKey)
	store, err := audit.NewStoreWithContext(
		t.Context(),
		database,
		audit.StoreOptions{CursorKey: cursorKey},
	)
	if err != nil {
		t.Fatal(err)
	}
	return store
}

func createRuntimeKnowledgeTestApp(t *testing.T, database *control.DB) {
	t.Helper()
	catalog, err := control.NewAppCatalog(database, control.AppCatalogOptions{
		CursorKey: bytes.Repeat([]byte{0x41}, 32),
		Clock:     func() time.Time { return time.UnixMicro(10_000).UTC() },
		IDGenerator: func() (string, error) {
			return runtimeKnowledgeTestApp, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := catalog.CreateApp(
		t.Context(),
		control.AppAccessScope{TenantID: runtimeKnowledgeTestTenant},
		control.AppDefinition{
			Slug:        "runtime-knowledge",
			DisplayName: "Runtime Knowledge",
		},
	); err != nil {
		t.Fatal(err)
	}
}

func createRuntimeKnowledgeTestIndex(t *testing.T, database *control.DB) {
	t.Helper()
	if _, err := database.CreateIndex(t.Context(), control.IndexDefinition{
		Name:             "main",
		DisplayName:      "Main",
		IngestionEnabled: true,
		SearchEnabled:    true,
	}); err != nil {
		t.Fatalf("create runtime knowledge test index: %v", err)
	}
}

func runtimeKnowledgeTestCreateRequest(
	name string,
	requestID string,
) *opensplunkv1.CreateKnowledgeObjectRequest {
	return &opensplunkv1.CreateKnowledgeObjectRequest{
		Definition: &opensplunkv1.KnowledgeObjectDefinition{
			AppId:        runtimeKnowledgeTestApp,
			Name:         name,
			SharingScope: opensplunkv1.SharingScope_SHARING_SCOPE_PRIVATE,
			Body: &opensplunkv1.KnowledgeObjectDefinition_FieldAlias{
				FieldAlias: &opensplunkv1.FieldAliasDefinition{
					SourceField:       "source_field",
					DestinationField:  "destination_" + name,
					OverwriteBehavior: opensplunkv1.KnowledgeOverwriteBehavior_KNOWLEDGE_OVERWRITE_BEHAVIOR_PRESERVE_EXISTING,
				},
			},
		},
		InitialState:    opensplunkv1.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_DRAFT,
		ClientRequestId: requestID,
	}
}

func runtimeKnowledgeSearchRequest(t *testing.T) searchjobs.CreateRequest {
	t.Helper()
	timeRange, err := searchtime.NewAbsoluteRange(
		time.Date(2026, time.August, 8, 10, 0, 0, 0, time.UTC),
		time.Date(2026, time.August, 8, 11, 0, 0, 0, time.UTC),
	)
	if err != nil {
		t.Fatal(err)
	}
	return searchjobs.CreateRequest{
		SPL:               "index=main | table message",
		OwnerID:           runtimeKnowledgeTestOwner,
		TenantID:          runtimeKnowledgeTestTenant,
		AuthorizedIndexes: []string{"main"},
		RequestedIndexes:  []string{"main"},
		TimeRange:         timeRange,
		AppID:             runtimeKnowledgeTestApp,
	}
}

type runtimeKnowledgeAdmissionCounters struct {
	snapshots            atomic.Int32
	ids                  atomic.Int32
	journalAdmissions    atomic.Int32
	journalFinalizations atomic.Int32
	executions           atomic.Int32
	finalized            chan struct{}
}

type runtimeKnowledgeAdmissionExecutor struct {
	counters *runtimeKnowledgeAdmissionCounters
}

func (executor runtimeKnowledgeAdmissionExecutor) Execute(
	_ context.Context,
	_ clickhouse.CompiledQuery,
	sink searchjobs.ResultSink,
) error {
	executor.counters.executions.Add(1)
	return sink.SetSchema(searchjobs.Schema{Columns: []searchjobs.Column{{
		Name: "message",
		Kind: searchjobs.ValueKindString,
	}}})
}

type runtimeKnowledgeAdmissionSnapshotter struct {
	counters *runtimeKnowledgeAdmissionCounters
}

func (snapshotter runtimeKnowledgeAdmissionSnapshotter) VisibilityCutoff(
	context.Context,
) (uint64, error) {
	snapshotter.counters.snapshots.Add(1)
	return 41, nil
}

type runtimeKnowledgeAdmissionJournal struct {
	counters *runtimeKnowledgeAdmissionCounters
}

func (journal runtimeKnowledgeAdmissionJournal) Admit(
	context.Context,
	searchjobs.Job,
) error {
	journal.counters.journalAdmissions.Add(1)
	return nil
}

func (journal runtimeKnowledgeAdmissionJournal) Finalize(
	context.Context,
	searchjobs.Job,
) error {
	journal.counters.journalFinalizations.Add(1)
	if journal.counters.finalized != nil {
		select {
		case journal.counters.finalized <- struct{}{}:
		default:
		}
	}
	return nil
}

func newRuntimeKnowledgeAdmissionManager(
	t *testing.T,
	resolver *knowledgecatalog.Resolver,
	counters *runtimeKnowledgeAdmissionCounters,
) *searchjobs.Manager {
	t.Helper()
	manager, err := searchjobs.New(searchjobs.Config{
		Executor:          runtimeKnowledgeAdmissionExecutor{counters: counters},
		Snapshotter:       runtimeKnowledgeAdmissionSnapshotter{counters: counters},
		Journal:           runtimeKnowledgeAdmissionJournal{counters: counters},
		Compiler:          clickhouse.Compiler{Database: "open_splunk", Table: "events"},
		KnowledgeResolver: resolver,
		MaxConcurrent:     1,
		CleanupInterval:   -1,
		NewID: func() string {
			counters.ids.Add(1)
			return "runtime-knowledge-search-0001"
		},
		Now: func() time.Time {
			return time.Date(2026, time.August, 8, 12, 0, 0, 0, time.UTC)
		},
	})
	if err != nil {
		t.Fatalf("create test-only knowledge admission manager: %v", err)
	}
	return manager
}

func waitForRuntimeKnowledgeJobState(
	t *testing.T,
	manager *searchjobs.Manager,
	jobID string,
	want searchjobs.State,
) searchjobs.Job {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for {
		job, err := manager.Get(jobID)
		if err != nil {
			t.Fatalf("Get(%q): %v", jobID, err)
		}
		if job.State == want {
			return job
		}
		if job.State == searchjobs.StateCompleted ||
			job.State == searchjobs.StateFailed ||
			job.State == searchjobs.StateCanceled {
			t.Fatalf(
				"job %q reached %s, want %s; failure=%#v",
				jobID,
				job.State,
				want,
				job.Failure,
			)
		}
		if time.Now().After(deadline) {
			t.Fatalf("job %q state = %s, want %s", jobID, job.State, want)
		}
		time.Sleep(time.Millisecond)
	}
}

func TestNewRuntimeKnowledgeManagementRejectsMissingAuditAppender(t *testing.T) {
	t.Parallel()

	directory := t.TempDir()
	database, err := control.Open(
		context.Background(),
		filepath.Join(directory, "control.db"),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = database.Close() }()
	keyPath := filepath.Join(directory, "server.key")
	var typedNilAudit *audit.Store
	for _, appender := range []audit.TransactionAppender{nil, typedNilAudit} {
		_, err = newRuntimeKnowledgeManagement(
			t.Context(),
			database,
			keyPath,
			appender,
		)
		if err == nil || !errors.Is(err, control.ErrInvalidArgument) {
			t.Fatalf("missing audit appender error = %v", err)
		}
		if _, statErr := os.Stat(keyPath); !errors.Is(statErr, os.ErrNotExist) {
			t.Fatalf("invalid dependency reached master key material: %v", statErr)
		}
	}
}
