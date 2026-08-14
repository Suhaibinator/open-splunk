package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/audit"
	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/knowledgecatalog"
	"github.com/Suhaibinator/open-splunk/internal/knowledgepreview"
	"github.com/Suhaibinator/open-splunk/internal/lookupservice"
	"github.com/Suhaibinator/open-splunk/internal/plan"
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
		lookupManagementCursorKeyPurpose,
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

func TestDeriveLookupManagementCursorKeyIsStableAndPurposeSeparated(
	t *testing.T,
) {
	t.Parallel()

	master := bytes.Repeat([]byte{0x71}, masterKeyBytes)
	first, err := deriveLookupManagementCursorKey(master)
	if err != nil {
		t.Fatal(err)
	}
	second, err := deriveLookupManagementCursorKey(master)
	if err != nil {
		t.Fatal(err)
	}
	knowledgeKey, err := deriveKnowledgeCatalogCursorKey(master)
	if err != nil {
		t.Fatal(err)
	}
	if len(first) != 32 || !bytes.Equal(first, second) ||
		bytes.Equal(first, knowledgeKey) {
		t.Fatal("lookup cursor key is not stable and purpose-separated")
	}
	if _, err := deriveLookupManagementCursorKey(master[:len(master)-1]); err == nil {
		t.Fatal("lookup cursor key accepted an invalid master key")
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
		firstRuntime.resolver == nil || firstRuntime.attempts == nil ||
		firstRuntime.lookupAssets == nil || firstRuntime.lookupCatalog == nil ||
		firstRuntime.lookupManagement == nil || !firstRuntime.lookupManagement.Ready() ||
		firstRuntime.lookupResolver == nil {
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

func TestRuntimeLookupManagementAndSearchResolutionShareOneCatalog(t *testing.T) {
	runtime, database := newRuntimeKnowledgeTestRuntime(t)
	defer func() { _ = database.Close() }()
	createRuntimeKnowledgeTestApp(t, database)
	createRuntimeKnowledgeTestIndex(t, database)

	created, err := runtime.lookupManagement.Create(
		t.Context(),
		lookupservice.Scope{
			TenantID: runtimeKnowledgeTestTenant,
			OwnerID:  runtimeKnowledgeTestOwner,
		},
		&opensplunkv1.CreateLookupRequest{
			Definition: &opensplunkv1.LookupDefinition{
				AppId:        runtimeKnowledgeTestApp,
				Name:         "service_owners",
				SharingScope: opensplunkv1.SharingScope_SHARING_SCOPE_APP,
				Automatic:    true,
				KeyMappings: []*opensplunkv1.LookupFieldMapping{{
					LookupField: "service_id",
					EventField:  "service_id",
				}},
				OutputMappings: []*opensplunkv1.LookupFieldMapping{{
					LookupField: "owner",
					EventField:  "service_owner",
				}},
				OverwriteBehavior: opensplunkv1.KnowledgeOverwriteBehavior_KNOWLEDGE_OVERWRITE_BEHAVIOR_PRESERVE_EXISTING,
			},
			CsvData: []byte("service_id,owner\napi,platform\n"),
		},
	)
	if err != nil {
		t.Fatalf("create runtime lookup: %v", err)
	}
	if created.GetLookup() == nil || created.GetLookup().GetVersion() != 1 {
		t.Fatalf("created runtime lookup = %#v", created)
	}

	resolved, err := runtime.lookupResolver.ResolveLookups(
		t.Context(),
		searchjobs.LookupResolutionScope{
			TenantID:    runtimeKnowledgeTestTenant,
			PrincipalID: runtimeKnowledgeTestOwner,
			AppID:       runtimeKnowledgeTestApp,
			Names:       []string{"service_owners"},
		},
	)
	if err != nil {
		t.Fatalf("resolve runtime lookup: %v", err)
	}
	if len(resolved) != 1 ||
		resolved[0].TenantID() != runtimeKnowledgeTestTenant ||
		resolved[0].DefinitionName() != "service_owners" ||
		resolved[0].LogicalID() != created.GetLookup().GetLookupId() ||
		resolved[0].LogicalVersion() != created.GetLookup().GetVersion() ||
		resolved[0].Version() != 1 ||
		!slices.Equal(resolved[0].Headers(), []string{"service_id", "owner"}) ||
		!reflect.DeepEqual(resolved[0].Rows(), [][]string{{"api", "platform"}}) {
		t.Fatalf("resolved runtime lookup = %#v", resolved)
	}
	contract, contractSet := resolved[0].LogicalContract()
	if !contractSet || contract.DefinitionName != "service_owners" ||
		contract.WriteMode != plan.LookupWriteModePreserveExisting ||
		len(contract.Keys) != 1 || len(contract.Outputs) != 1 {
		t.Fatalf("resolved runtime lookup contract = (%#v, %t)", contract, contractSet)
	}
	automatic, err := runtime.lookupResolver.ResolveAutomaticLookups(
		t.Context(),
		searchjobs.AutomaticLookupResolutionScope{
			TenantID:    runtimeKnowledgeTestTenant,
			PrincipalID: runtimeKnowledgeTestOwner,
			AppID:       runtimeKnowledgeTestApp,
		},
	)
	if err != nil {
		t.Fatalf("resolve runtime automatic lookups: %v", err)
	}
	if len(automatic) != 1 ||
		automatic[0].StableID != created.GetLookup().GetLookupId() ||
		automatic[0].Lookup.DefinitionName != "service_owners" ||
		automatic[0].Selector == nil ||
		automatic[0].Resolution.LogicalID() != created.GetLookup().GetLookupId() ||
		automatic[0].Resolution.LogicalVersion() != created.GetLookup().GetVersion() ||
		automatic[0].Resolution.ObjectID() == "" {
		t.Fatalf("resolved runtime automatic lookup = %#v", automatic)
	}
	admission, err := runtime.lookupResolver.ResolveLookupAdmission(
		t.Context(),
		searchjobs.LookupAdmissionResolutionScope{
			TenantID:    runtimeKnowledgeTestTenant,
			PrincipalID: runtimeKnowledgeTestOwner,
			AppID:       runtimeKnowledgeTestApp,
			Names:       []string{"service_owners"},
		},
	)
	if err != nil || len(admission.Explicit) != 1 || len(admission.Automatic) != 1 ||
		admission.Explicit[0].LogicalID() != created.GetLookup().GetLookupId() ||
		admission.Automatic[0].StableID != created.GetLookup().GetLookupId() {
		t.Fatalf("combined runtime lookup admission = %#v, %v", admission, err)
	}

	counters := &runtimeKnowledgeAdmissionCounters{}
	manager := newRuntimeKnowledgeAdmissionManager(t, runtime, counters)
	defer func() {
		if err := manager.Close(); err != nil {
			t.Errorf("close lookup admission manager: %v", err)
		}
	}()
	request := runtimeKnowledgeSearchRequest(t)
	request.SPL = "index=main | lookup service_owners service_id AS service_id OUTPUTNEW owner AS service_owner | table message"
	job, err := manager.Create(t.Context(), request)
	if err != nil {
		t.Fatalf("admit runtime lookup search: %v", err)
	}
	completed := waitForRuntimeKnowledgeJobState(
		t,
		manager,
		job.ID,
		searchjobs.StateCompleted,
	)
	if completed.Failure != nil || counters.executions.Load() != 1 {
		t.Fatalf("runtime lookup execution = (%#v, count=%d)", completed, counters.executions.Load())
	}
	firstProvenance := completed.KnowledgeSnapshot.GetLookupAssets()
	if len(firstProvenance) != 1 ||
		firstProvenance[0].GetLookupId() != created.GetLookup().GetLookupId() ||
		firstProvenance[0].GetLookupVersion() != 1 {
		t.Fatalf("first runtime lookup provenance = %#v", firstProvenance)
	}

	replaced, err := runtime.lookupManagement.Replace(
		t.Context(),
		lookupservice.Scope{
			TenantID: runtimeKnowledgeTestTenant,
			OwnerID:  runtimeKnowledgeTestOwner,
		},
		&opensplunkv1.ReplaceLookupRequest{
			LookupId:        created.GetLookup().GetLookupId(),
			ExpectedVersion: 1,
			Definition: &opensplunkv1.LookupDefinition{
				AppId:        runtimeKnowledgeTestApp,
				Name:         "service_owners",
				SharingScope: opensplunkv1.SharingScope_SHARING_SCOPE_APP,
				Automatic:    true,
				KeyMappings: []*opensplunkv1.LookupFieldMapping{{
					LookupField: "service_id",
					EventField:  "service_id",
				}},
				OutputMappings: []*opensplunkv1.LookupFieldMapping{{
					LookupField: "owner",
					EventField:  "team_owner",
				}},
				OverwriteBehavior: opensplunkv1.KnowledgeOverwriteBehavior_KNOWLEDGE_OVERWRITE_BEHAVIOR_REPLACE_EXISTING,
			},
		},
	)
	if err != nil {
		t.Fatalf("metadata-only replace runtime lookup: %v", err)
	}
	if replaced.GetLookup().GetVersion() != 2 {
		t.Fatalf("metadata-only replacement = %#v", replaced)
	}
	replacedResolution, err := runtime.lookupResolver.ResolveLookups(
		t.Context(),
		searchjobs.LookupResolutionScope{
			TenantID:    runtimeKnowledgeTestTenant,
			PrincipalID: runtimeKnowledgeTestOwner,
			AppID:       runtimeKnowledgeTestApp,
			Names:       []string{"service_owners"},
		},
	)
	if err != nil {
		t.Fatalf("resolve metadata-only replacement: %v", err)
	}
	if len(replacedResolution) != 1 ||
		replacedResolution[0].LogicalID() != resolved[0].LogicalID() ||
		replacedResolution[0].LogicalVersion() != 2 ||
		replacedResolution[0].ObjectID() != resolved[0].ObjectID() ||
		replacedResolution[0].Version() != resolved[0].Version() ||
		replacedResolution[0].SizeBytes() != resolved[0].SizeBytes() ||
		replacedResolution[0].ContentSHA256() != resolved[0].ContentSHA256() {
		t.Fatalf("metadata-only replacement resolution = %#v", replacedResolution)
	}
	replacedContract, replacedContractSet := replacedResolution[0].LogicalContract()
	if !replacedContractSet ||
		replacedContract.WriteMode != plan.LookupWriteModeOverwrite ||
		len(replacedContract.Outputs) != 1 ||
		replacedContract.Outputs[0].EventField.Name != "team_owner" {
		t.Fatalf("metadata-only replacement contract = (%#v, %t)", replacedContract, replacedContractSet)
	}

	request = runtimeKnowledgeSearchRequest(t)
	request.SPL = "index=main | lookup service_owners service_id AS service_id OUTPUT owner AS team_owner | table message"
	secondJob, err := manager.Create(t.Context(), request)
	if err != nil {
		t.Fatalf("admit replaced runtime lookup search: %v", err)
	}
	secondCompleted := waitForRuntimeKnowledgeJobState(
		t,
		manager,
		secondJob.ID,
		searchjobs.StateCompleted,
	)
	if secondCompleted.Failure != nil || counters.executions.Load() != 2 {
		t.Fatalf(
			"replaced runtime lookup execution = (%#v, count=%d)",
			secondCompleted,
			counters.executions.Load(),
		)
	}
	secondProvenance := secondCompleted.KnowledgeSnapshot.GetLookupAssets()
	if len(secondProvenance) != 1 ||
		secondProvenance[0].GetLookupId() != firstProvenance[0].GetLookupId() ||
		secondProvenance[0].GetLookupVersion() != 2 ||
		secondProvenance[0].GetAsset().GetLookupAssetId() != firstProvenance[0].GetAsset().GetLookupAssetId() ||
		secondProvenance[0].GetAsset().GetVersion() != firstProvenance[0].GetAsset().GetVersion() ||
		secondProvenance[0].GetAsset().GetSizeBytes() != firstProvenance[0].GetAsset().GetSizeBytes() ||
		!bytes.Equal(
			secondProvenance[0].GetAsset().GetContentSha256(),
			firstProvenance[0].GetAsset().GetContentSha256(),
		) ||
		bytes.Equal(
			secondCompleted.KnowledgeSnapshot.GetRef().GetSnapshotSha256(),
			completed.KnowledgeSnapshot.GetRef().GetSnapshotSha256(),
		) {
		t.Fatalf(
			"metadata-only runtime lookup provenance = %#v / %#v",
			firstProvenance,
			secondProvenance,
		)
	}
}

func TestConfigureRuntimeKnowledgeManagementIsAtomicAndNarrow(t *testing.T) {
	t.Parallel()

	runtime, database := newRuntimeKnowledgeTestRuntime(t)
	defer func() { _ = database.Close() }()
	apps := &runtimeAppCatalog{catalog: &stubControlAppCatalog{}}
	preview := newRuntimeKnowledgePreviewForTest(t, runtime)
	var typedNilAppBackend *control.AppCatalog
	config := server.Config{Bootstrap: server.BootstrapConfig{
		Features: []opensplunkv1.ServerFeature{
			opensplunkv1.ServerFeature_SERVER_FEATURE_SEARCH,
		},
		SelectedAppID: "unchanged-app",
	}}
	if err := configureRuntimeKnowledgeManagement(&config, runtime, apps, preview); err != nil {
		t.Fatal(err)
	}
	if config.KnowledgeCatalog != runtime.catalog ||
		config.KnowledgeWriter != runtime.writer ||
		config.KnowledgeApps != apps ||
		config.KnowledgeAttempts != runtime.attempts ||
		config.KnowledgePreview != preview ||
		config.LookupManagement != runtime.lookupManagement {
		t.Fatalf("configured knowledge dependencies = %#v", config)
	}
	if !slices.Equal(config.Bootstrap.Features, []opensplunkv1.ServerFeature{
		opensplunkv1.ServerFeature_SERVER_FEATURE_SEARCH,
	}) || config.Bootstrap.SelectedAppID != "unchanged-app" {
		t.Fatalf("knowledge composition changed bootstrap capability = %#v", config.Bootstrap)
	}

	without := func(clearField func(*runtimeKnowledgeManagement)) runtimeKnowledgeManagement {
		candidate := runtime
		clearField(&candidate)
		return candidate
	}
	missing := []struct {
		name    string
		runtime runtimeKnowledgeManagement
		apps    *runtimeAppCatalog
		preview *knowledgepreview.Service
	}{
		{name: "catalog", runtime: without(func(value *runtimeKnowledgeManagement) { value.catalog = nil }), apps: apps, preview: preview},
		{name: "resolver", runtime: without(func(value *runtimeKnowledgeManagement) { value.resolver = nil }), apps: apps, preview: preview},
		{name: "writer", runtime: without(func(value *runtimeKnowledgeManagement) { value.writer = nil }), apps: apps, preview: preview},
		{name: "unready writer", runtime: without(func(value *runtimeKnowledgeManagement) { value.writer = &knowledgecatalog.Writer{} }), apps: apps, preview: preview},
		{name: "attempts", runtime: without(func(value *runtimeKnowledgeManagement) { value.attempts = nil }), apps: apps, preview: preview},
		{name: "lookup assets", runtime: without(func(value *runtimeKnowledgeManagement) { value.lookupAssets = nil }), apps: apps, preview: preview},
		{name: "lookup catalog", runtime: without(func(value *runtimeKnowledgeManagement) { value.lookupCatalog = nil }), apps: apps, preview: preview},
		{name: "lookup management", runtime: without(func(value *runtimeKnowledgeManagement) { value.lookupManagement = nil }), apps: apps, preview: preview},
		{name: "lookup resolver", runtime: without(func(value *runtimeKnowledgeManagement) { value.lookupResolver = nil }), apps: apps, preview: preview},
		{name: "unready lookup resolver", runtime: without(func(value *runtimeKnowledgeManagement) { value.lookupResolver = &runtimeLookupSearchResolver{} }), apps: apps, preview: preview},
		{name: "apps", runtime: runtime, preview: preview},
		{name: "app backend", runtime: runtime, apps: &runtimeAppCatalog{}, preview: preview},
		{name: "typed nil app backend", runtime: runtime, apps: &runtimeAppCatalog{catalog: typedNilAppBackend}, preview: preview},
		{name: "preview", runtime: runtime, apps: apps},
	}
	for _, test := range missing {
		t.Run("missing "+test.name, func(t *testing.T) {
			candidate := server.Config{}
			if err := configureRuntimeKnowledgeManagement(
				&candidate,
				test.runtime,
				test.apps,
				test.preview,
			); err == nil || candidate.KnowledgeCatalog != nil ||
				candidate.KnowledgeWriter != nil || candidate.KnowledgeApps != nil ||
				candidate.KnowledgeAttempts != nil || candidate.KnowledgePreview != nil ||
				candidate.LookupManagement != nil {
				t.Fatalf("partial configuration = (%#v, %v)", candidate, err)
			}
		})
	}
	if err := configureRuntimeKnowledgeManagement(nil, runtime, apps, preview); err == nil {
		t.Fatal("nil server config was accepted")
	}

	var typedNilCatalog *knowledgecatalog.Store
	preconfigured := server.Config{KnowledgeCatalog: typedNilCatalog}
	before := preconfigured.KnowledgeCatalog
	if err := configureRuntimeKnowledgeManagement(
		&preconfigured,
		runtime,
		apps,
		preview,
	); err == nil || preconfigured.KnowledgeCatalog != before ||
		preconfigured.KnowledgeWriter != nil || preconfigured.KnowledgeApps != nil ||
		preconfigured.KnowledgeAttempts != nil || preconfigured.KnowledgePreview != nil ||
		preconfigured.LookupManagement != nil {
		t.Fatalf("typed-nil preconfiguration was overwritten = (%#v, %v)", preconfigured, err)
	}

	preconfigured = server.Config{LookupManagement: runtime.lookupManagement}
	if err := configureRuntimeKnowledgeManagement(
		&preconfigured,
		runtime,
		apps,
		preview,
	); err == nil || preconfigured.LookupManagement != runtime.lookupManagement ||
		preconfigured.KnowledgeCatalog != nil || preconfigured.KnowledgeWriter != nil ||
		preconfigured.KnowledgeApps != nil || preconfigured.KnowledgeAttempts != nil ||
		preconfigured.KnowledgePreview != nil {
		t.Fatalf("lookup preconfiguration was overwritten = (%#v, %v)", preconfigured, err)
	}
}

func TestRuntimeKnowledgeCompositionDoesNotAdvertiseWithoutSearchAdmission(
	t *testing.T,
) {
	runtime, database := newRuntimeKnowledgeTestRuntime(t)
	defer func() { _ = database.Close() }()
	apps := &runtimeAppCatalog{catalog: &stubControlAppCatalog{}}
	preview := newRuntimeKnowledgePreviewForTest(t, runtime)
	config := runtimeServerConfig()
	originalSearchJobs := config.SearchJobs
	if err := configureRuntimeKnowledgeManagement(&config, runtime, apps, preview); err != nil {
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
	manager := newRuntimeKnowledgeAdmissionManager(t, runtime, counters)
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
	runtime runtimeKnowledgeManagement,
	counters *runtimeKnowledgeAdmissionCounters,
) *searchjobs.Manager {
	t.Helper()
	config := runtimeKnowledgeAdmissionManagerConfig(runtime.resolver, counters)
	config.LookupResolver = runtime.lookupResolver
	manager, err := searchjobs.New(config)
	if err != nil {
		t.Fatalf("create knowledge admission manager: %v", err)
	}
	return manager
}

func newRuntimeKnowledgePreviewForTest(
	t *testing.T,
	runtime runtimeKnowledgeManagement,
) *knowledgepreview.Service {
	t.Helper()
	counters := &runtimeKnowledgeAdmissionCounters{}
	manager := newRuntimeKnowledgeAdmissionManager(t, runtime, counters)
	t.Cleanup(func() {
		if err := manager.Close(); err != nil {
			t.Errorf("close Preview search manager: %v", err)
		}
	})
	executor := runtimeKnowledgeAdmissionExecutor{counters: counters}
	preview, err := knowledgepreview.NewService(knowledgepreview.Config{
		Searches: manager,
		Writer:   runtime.writer,
		Compiler: knowledgepreview.ProductionCompilerAdapter{Compiler: clickhouse.Compiler{
			Database: "open_splunk",
			Table:    "events",
		}},
		Executor: executor,
	})
	if err != nil {
		t.Fatalf("create knowledge Preview service: %v", err)
	}
	return preview
}

func runtimeKnowledgeAdmissionManagerConfig(
	resolver *knowledgecatalog.Resolver,
	counters *runtimeKnowledgeAdmissionCounters,
) searchjobs.Config {
	return searchjobs.Config{
		Executor:          runtimeKnowledgeAdmissionExecutor{counters: counters},
		Snapshotter:       runtimeKnowledgeAdmissionSnapshotter{counters: counters},
		Journal:           runtimeKnowledgeAdmissionJournal{counters: counters},
		Compiler:          clickhouse.Compiler{Database: "open_splunk", Table: "events"},
		KnowledgeResolver: resolver,
		MaxConcurrent:     1,
		CleanupInterval:   -1,
		NewID: func() string {
			sequence := counters.ids.Add(1)
			return fmt.Sprintf("runtime-knowledge-search-%04d", sequence)
		},
		Now: func() time.Time {
			return time.Date(2026, time.August, 8, 12, 0, 0, 0, time.UTC)
		},
	}
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
