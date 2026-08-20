package main

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"testing"
	"time"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/audit"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/knowledge"
	"github.com/Suhaibinator/open-splunk/internal/knowledgecatalog"
	"github.com/Suhaibinator/open-splunk/internal/knowledgedefinition"
)

const (
	runtimeIndexAdmissionTenantID = "deployment-index-administrator"
	runtimeIndexAdmissionOwnerID  = "runtime-index-admission-owner"
	runtimeIndexAdmissionSafeApp  = "app_000000000200000000001A"
	runtimeIndexAdmissionRiskyApp = "app_000000000200000000002A"
)

var runtimeIndexAdmissionCursorKey = []byte(
	"runtime-index-admission-cursor-key-at-least-32-bytes",
)

func TestRuntimeIndexAdministrationUsesKnowledgeValidatorForSafeACTIVEObjects(
	t *testing.T,
) {
	database := newRuntimeIndexAdmissionDatabase(t)
	createRuntimeIndexAdmissionPhysicalIndex(t, database, "stage-prod")
	createRuntimeIndexAdmissionApp(
		t,
		database,
		"tenant-safe-active",
		runtimeIndexAdmissionSafeApp,
		"safe-active",
	)
	insertRuntimeIndexAdmissionACTIVEObject(
		t,
		database,
		"tenant-safe-active",
		"ko-runtime-safe-active",
		runtimeIndexAdmissionSafeApp,
		"safe_active_output",
		"stage-prod",
		"safe_output",
		20_001,
	)

	candidate := runtimeIndexAdmissionControlDefinition("main-dev")
	if _, err := database.CreateIndex(t.Context(), candidate); !errors.Is(
		err,
		control.ErrDependencyConflict,
	) {
		t.Fatalf("raw nil-validator create error = %v, want dependency conflict", err)
	}
	if _, err := database.GetIndexByName(t.Context(), candidate.Name); !errors.Is(
		err,
		control.ErrNotFound,
	) {
		t.Fatalf("raw rejected candidate lookup error = %v, want not found", err)
	}

	auditStore := newRuntimeIndexAdmissionAuditStore(t, database)
	administration, err := newRuntimeIndexAdministration(
		database,
		runtimeIndexAdmissionTenantID,
		auditStore,
	)
	if err != nil {
		t.Fatalf("newRuntimeIndexAdministration(): %v", err)
	}
	created, err := administration.CreateIndex(
		newRuntimeIndexAdmissionActorContext(t),
		candidate,
	)
	if err != nil {
		t.Fatalf("runtime CreateIndex with safe ACTIVE object: %v", err)
	}
	if created.Definition.Name != candidate.Name || created.Version != 1 {
		t.Fatalf("runtime-created index = %+v", created)
	}
	if got := runtimeIndexAdmissionRowCount(t, database, "audit_events"); got != 1 {
		t.Fatalf("runtime index audit rows = %d, want 1", got)
	}
}

func TestRuntimeIndexAdministrationRejectsConflictInAnyKnowledgeTenantAtomically(
	t *testing.T,
) {
	database := newRuntimeIndexAdmissionDatabase(t)
	createRuntimeIndexAdmissionPhysicalIndex(t, database, "stage-prod")
	createRuntimeIndexAdmissionApp(
		t,
		database,
		"tenant-b-runtime-safe",
		runtimeIndexAdmissionSafeApp,
		"runtime-safe",
	)
	createRuntimeIndexAdmissionApp(
		t,
		database,
		"tenant-z-runtime-conflict",
		runtimeIndexAdmissionRiskyApp,
		"runtime-conflict",
	)
	insertRuntimeIndexAdmissionACTIVEObject(
		t,
		database,
		"tenant-b-runtime-safe",
		"ko-runtime-safe-first",
		runtimeIndexAdmissionSafeApp,
		"safe_first",
		"stage-prod",
		"safe_output",
		20_001,
	)
	for index, fixture := range []struct {
		name    string
		pattern string
	}{
		{name: "existing_output", pattern: "*prod"},
		{name: "new_output", pattern: "main*"},
	} {
		insertRuntimeIndexAdmissionACTIVEObject(
			t,
			database,
			"tenant-z-runtime-conflict",
			[]string{
				"ko-runtime-conflict-existing",
				"ko-runtime-conflict-new",
			}[index],
			runtimeIndexAdmissionRiskyApp,
			fixture.name,
			fixture.pattern,
			"shared_runtime_output",
			int64(20_010+index),
		)
	}

	auditStore := newRuntimeIndexAdmissionAuditStore(t, database)
	administration, err := newRuntimeIndexAdministration(
		database,
		runtimeIndexAdmissionTenantID,
		auditStore,
	)
	if err != nil {
		t.Fatalf("newRuntimeIndexAdministration(): %v", err)
	}
	before := readRuntimeIndexAdmissionMutationSnapshot(t, database)
	_, err = administration.CreateIndex(
		newRuntimeIndexAdmissionActorContext(t),
		runtimeIndexAdmissionControlDefinition("main-dev"),
	)
	if !errors.Is(err, control.ErrDependencyConflict) {
		t.Fatalf("foreign knowledge-tenant conflict error = %v, want dependency conflict", err)
	}
	after := readRuntimeIndexAdmissionMutationSnapshot(t, database)
	if after != before {
		t.Fatalf("rejected runtime index mutation changed authority: before=%+v after=%+v", before, after)
	}
	if _, err := database.GetIndexByName(t.Context(), "main-dev"); !errors.Is(
		err,
		control.ErrNotFound,
	) {
		t.Fatalf("rejected runtime candidate lookup error = %v, want not found", err)
	}
}

func newRuntimeIndexAdmissionDatabase(t *testing.T) *control.DB {
	t.Helper()
	database, err := control.Open(
		t.Context(),
		filepath.Join(t.TempDir(), "control.db"),
	)
	if err != nil {
		t.Fatalf("control.Open(): %v", err)
	}
	t.Cleanup(func() {
		if err := database.Close(); err != nil {
			t.Errorf("close control database: %v", err)
		}
	})
	return database
}

func newRuntimeIndexAdmissionAuditStore(
	t *testing.T,
	database *control.DB,
) *audit.Store {
	t.Helper()
	store, err := audit.NewStore(
		database,
		audit.StoreOptions{CursorKey: runtimeIndexAdmissionCursorKey},
	)
	if err != nil {
		t.Fatalf("audit.NewStore(): %v", err)
	}
	return store
}

func newRuntimeIndexAdmissionActorContext(t *testing.T) context.Context {
	t.Helper()
	ctx, err := audit.WithActor(t.Context(), audit.Actor{
		Kind: audit.ActorKindBrowser,
		ID:   runtimeIndexAdmissionOwnerID,
		Role: audit.ActorRoleAdministrator,
	})
	if err != nil {
		t.Fatalf("audit.WithActor(): %v", err)
	}
	return ctx
}

func createRuntimeIndexAdmissionPhysicalIndex(
	t *testing.T,
	database *control.DB,
	name string,
) {
	t.Helper()
	if _, err := database.CreateIndex(
		t.Context(),
		runtimeIndexAdmissionControlDefinition(name),
	); err != nil {
		t.Fatalf("create existing physical index %q: %v", name, err)
	}
}

func runtimeIndexAdmissionControlDefinition(name string) control.IndexDefinition {
	return control.IndexDefinition{
		Name:             name,
		DisplayName:      name,
		IngestionEnabled: true,
		SearchEnabled:    true,
	}
}

func createRuntimeIndexAdmissionApp(
	t *testing.T,
	database *control.DB,
	tenantID string,
	appID string,
	slug string,
) {
	t.Helper()
	catalog, err := control.NewAppCatalog(database, control.AppCatalogOptions{
		CursorKey: runtimeIndexAdmissionCursorKey,
		Clock:     func() time.Time { return time.UnixMicro(10_000).UTC() },
		IDGenerator: func() (string, error) {
			return appID, nil
		},
	})
	if err != nil {
		t.Fatalf("construct app catalog for %s: %v", tenantID, err)
	}
	if _, err := catalog.CreateApp(
		t.Context(),
		control.AppAccessScope{TenantID: tenantID},
		control.AppDefinition{
			Slug:        slug,
			DisplayName: slug,
			DefaultTimeRange: &control.AppTimeRange{
				Earliest: new("-24h"),
				Latest:   new("now"),
			},
		},
	); err != nil {
		t.Fatalf("create app for %s: %v", tenantID, err)
	}
}

func insertRuntimeIndexAdmissionACTIVEObject(
	t *testing.T,
	database *control.DB,
	tenantID string,
	objectID string,
	appID string,
	name string,
	indexPattern string,
	outputField string,
	timestamp int64,
) {
	t.Helper()
	definition := &opensplunk.KnowledgeObjectDefinition{
		AppId:        appID,
		Name:         name,
		SharingScope: opensplunk.SharingScope_SHARING_SCOPE_APP,
		Selector: &opensplunk.KnowledgeSelector{
			IndexPatterns: []*opensplunk.KnowledgeSelectorPattern{{
				Value: indexPattern,
			}},
		},
		Body: &opensplunk.KnowledgeObjectDefinition_FieldExtraction{
			FieldExtraction: &opensplunk.FieldExtractionDefinition{
				InputField: "_raw",
				Extraction: &opensplunk.FieldExtractionDefinition_Json{
					Json: &opensplunk.JsonFieldExtractionDefinition{
						Path:        "payload.value",
						OutputField: outputField,
					},
				},
			},
		},
	}
	normalized, err := knowledgedefinition.Normalize(definition)
	if err != nil {
		t.Fatalf("normalize %s/%s: %v", tenantID, objectID, err)
	}

	tx, err := database.SQLDB().BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatalf("begin %s/%s ACTIVE fixture: %v", tenantID, objectID, err)
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.ExecContext(t.Context(), `INSERT INTO knowledge_definition_blobs (
		tenant_id, definition_digest, definition_proto, definition_bytes, created_at_unix_micro
	) VALUES (?, ?, ?, ?, ?)`,
		tenantID, normalized.Digest[:], normalized.Bytes, len(normalized.Bytes), timestamp,
	); err != nil {
		t.Fatalf("insert %s/%s definition: %v", tenantID, objectID, err)
	}
	if _, err := tx.ExecContext(t.Context(), `INSERT INTO knowledge_object_versions (
		tenant_id, knowledge_object_id, object_version, app_id, owner_id, object_type, name,
		sharing_scope, state, definition_digest, dependency_count, mutation_kind,
		quarantine_reason, created_at_unix_micro
	) VALUES (?, ?, 1, ?, ?, ?, ?, ?, 'active', ?, 0, 'create', NULL, ?)`,
		tenantID,
		objectID,
		appID,
		runtimeIndexAdmissionOwnerID,
		string(knowledgecatalog.ObjectTypeFieldExtraction),
		name,
		string(knowledgecatalog.SharingScopeApp),
		normalized.Digest[:],
		timestamp,
	); err != nil {
		t.Fatalf("insert %s/%s version: %v", tenantID, objectID, err)
	}
	if _, err := tx.ExecContext(t.Context(), `INSERT INTO knowledge_object_dependency_seals (
		tenant_id, knowledge_object_id, object_version, dependency_count
	) VALUES (?, ?, 1, 0)`, tenantID, objectID); err != nil {
		t.Fatalf("seal %s/%s dependencies: %v", tenantID, objectID, err)
	}

	dimensions := []knowledge.Dimension{
		knowledge.DimensionIndex,
		knowledge.DimensionHost,
		knowledge.DimensionSource,
		knowledge.DimensionSourcetype,
	}
	selectorCounts := [4]int{}
	selectorValueBytes := 0
	for index, dimension := range dimensions {
		patterns := normalized.Selector.Patterns(dimension)
		selectorCounts[index] = len(patterns)
		for _, pattern := range patterns {
			selectorValueBytes += len(pattern)
		}
	}
	canonicalSelectorBytes := len(normalized.Selector.CanonicalBytes())
	if _, err := tx.ExecContext(t.Context(), `INSERT INTO knowledge_object_list_projections (
		tenant_id, knowledge_object_id, object_version, app_id, owner_id, object_type, name,
		sharing_scope, state, description_present, description, index_selector_count,
		host_selector_count, source_selector_count, sourcetype_selector_count,
		selector_value_bytes, canonical_selector_bytes
	) VALUES (?, ?, 1, ?, ?, ?, ?, ?, 'active', 0, '', ?, ?, ?, ?, ?, ?)`,
		tenantID,
		objectID,
		appID,
		runtimeIndexAdmissionOwnerID,
		string(knowledgecatalog.ObjectTypeFieldExtraction),
		name,
		string(knowledgecatalog.SharingScopeApp),
		selectorCounts[0],
		selectorCounts[1],
		selectorCounts[2],
		selectorCounts[3],
		selectorValueBytes,
		canonicalSelectorBytes,
	); err != nil {
		t.Fatalf("insert %s/%s projection: %v", tenantID, objectID, err)
	}
	insertRuntimeIndexAdmissionSelectorRows(t, tx, tenantID, objectID, normalized)
	if _, err := tx.ExecContext(t.Context(), `INSERT INTO knowledge_object_list_projection_seals (
		tenant_id, knowledge_object_id, object_version, projection_bytes, canonical_selector_bytes
	) VALUES (?, ?, 1, ?, ?)`,
		tenantID,
		objectID,
		selectorValueBytes,
		canonicalSelectorBytes,
	); err != nil {
		t.Fatalf("seal %s/%s projection: %v", tenantID, objectID, err)
	}
	if _, err := tx.ExecContext(t.Context(), `INSERT INTO knowledge_objects (
		tenant_id, knowledge_object_id, current_version, app_id, owner_id, object_type, name,
		sharing_scope, state, definition_digest, created_at_unix_micro, updated_at_unix_micro,
		disabled_at_unix_micro, quarantined_at_unix_micro, deleted_at_unix_micro, quarantine_reason
	) VALUES (?, ?, 1, ?, ?, ?, ?, ?, 'active', ?, ?, ?, NULL, NULL, NULL, NULL)`,
		tenantID,
		objectID,
		appID,
		runtimeIndexAdmissionOwnerID,
		string(knowledgecatalog.ObjectTypeFieldExtraction),
		name,
		string(knowledgecatalog.SharingScopeApp),
		normalized.Digest[:],
		timestamp,
		timestamp,
	); err != nil {
		t.Fatalf("insert %s/%s registry: %v", tenantID, objectID, err)
	}
	if _, err := tx.ExecContext(t.Context(), `UPDATE knowledge_catalog_tenants
		SET catalog_revision = catalog_revision + 1 WHERE tenant_id = ?`, tenantID); err != nil {
		t.Fatalf("advance %s catalog revision: %v", tenantID, err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit %s/%s ACTIVE fixture: %v", tenantID, objectID, err)
	}
}

func insertRuntimeIndexAdmissionSelectorRows(
	t *testing.T,
	tx *sql.Tx,
	tenantID string,
	objectID string,
	normalized knowledgedefinition.Normalized,
) {
	t.Helper()
	dimensions := []struct {
		name      string
		dimension knowledge.Dimension
	}{
		{name: "index", dimension: knowledge.DimensionIndex},
		{name: "host", dimension: knowledge.DimensionHost},
		{name: "source", dimension: knowledge.DimensionSource},
		{name: "sourcetype", dimension: knowledge.DimensionSourcetype},
	}
	for _, dimension := range dimensions {
		for ordinal, value := range normalized.Selector.Patterns(dimension.dimension) {
			pattern, err := knowledge.NormalizePattern(value)
			if err != nil {
				t.Fatalf("normalize %s/%s selector: %v", tenantID, objectID, err)
			}
			matchKind := "wildcard"
			if pattern.IsLiteral() {
				matchKind = "exact"
			}
			if _, err := tx.ExecContext(t.Context(), `INSERT INTO knowledge_object_list_selector_patterns (
				tenant_id, knowledge_object_id, object_version, dimension, ordinal, match_kind, value
			) VALUES (?, ?, 1, ?, ?, ?, ?)`,
				tenantID,
				objectID,
				dimension.name,
				ordinal,
				matchKind,
				value,
			); err != nil {
				t.Fatalf("insert %s/%s selector: %v", tenantID, objectID, err)
			}
		}
	}
}

type runtimeIndexAdmissionMutationSnapshot struct {
	revision        int64
	physicalCount   int64
	indexRows       int64
	auditRows       int64
	auditTenantRows int64
}

func readRuntimeIndexAdmissionMutationSnapshot(
	t *testing.T,
	database *control.DB,
) runtimeIndexAdmissionMutationSnapshot {
	t.Helper()
	var snapshot runtimeIndexAdmissionMutationSnapshot
	if err := database.SQLDB().QueryRowContext(t.Context(), `
		SELECT revision, physical_count FROM index_catalog_state WHERE singleton_id = 1
	`).Scan(&snapshot.revision, &snapshot.physicalCount); err != nil {
		t.Fatalf("read index catalog state: %v", err)
	}
	snapshot.indexRows = runtimeIndexAdmissionRowCount(t, database, "indexes")
	snapshot.auditRows = runtimeIndexAdmissionRowCount(t, database, "audit_events")
	snapshot.auditTenantRows = runtimeIndexAdmissionRowCount(t, database, "audit_tenant_state")
	return snapshot
}

func runtimeIndexAdmissionRowCount(
	t *testing.T,
	database *control.DB,
	table string,
) int64 {
	t.Helper()
	var count int64
	query := "SELECT COUNT(*) FROM " + table // #nosec G202 -- table is a fixed test constant.
	if err := database.SQLDB().QueryRowContext(t.Context(), query).Scan(&count); err != nil {
		t.Fatalf("count %s: %v", table, err)
	}
	return count
}
