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
	"testing"
	"time"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/audit"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/knowledgecatalog"
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
		firstRuntime.attempts == nil {
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
		{name: "catalog", runtime: runtimeKnowledgeManagement{writer: runtime.writer, attempts: runtime.attempts}, apps: apps},
		{name: "writer", runtime: runtimeKnowledgeManagement{catalog: runtime.catalog, attempts: runtime.attempts}, apps: apps},
		{name: "unready writer", runtime: runtimeKnowledgeManagement{catalog: runtime.catalog, writer: &knowledgecatalog.Writer{}, attempts: runtime.attempts}, apps: apps},
		{name: "attempts", runtime: runtimeKnowledgeManagement{catalog: runtime.catalog, writer: runtime.writer}, apps: apps},
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
