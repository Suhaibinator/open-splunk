package control

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Suhaibinator/open-splunk/migrations"
	"gorm.io/gorm"
)

func TestAppLifecycleIsTenantScopedVersionedAndReferentiallySafe(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db := openTestDB(t)
	catalog := newTestAppCatalog(t, db)
	mainIndex := mustCreateIndex(t, db, enabledIndex("main"))
	audit := mustCreateIndex(t, db, enabledIndex("audit"))

	scope := AppAccessScope{TenantID: "tenant-a"}
	created, err := catalog.CreateApp(ctx, scope, AppDefinition{
		Slug:        "  GradeThis_Prod ",
		DisplayName: " GradeThis Production ",
		Description: " production workspace ",
		DefaultIndexes: []string{
			" MAIN ",
			"audit",
			"main",
		},
		DefaultTimeRange: &AppTimeRange{
			Earliest: stringPointerForAppTest(" -24h "),
			Latest:   stringPointerForAppTest(" now "),
			Timezone: stringPointerForAppTest(" America/Los_Angeles "),
		},
	})
	if err != nil {
		t.Fatalf("CreateApp() error = %v", err)
	}
	if created.ID == "" || created.Version != 1 || created.State != AppStateActive {
		t.Fatalf("CreateApp() identity = %#v", created)
	}
	if created.Definition.Slug != "gradethis_prod" ||
		created.Definition.DisplayName != "GradeThis Production" ||
		created.Definition.Description != "production workspace" ||
		!slices.Equal(created.Definition.DefaultIndexes, []string{"audit", "main"}) ||
		!reflect.DeepEqual(created.Definition.DefaultTimeRange, &AppTimeRange{
			Earliest: stringPointerForAppTest("-24h"),
			Latest:   stringPointerForAppTest("now"),
			Timezone: stringPointerForAppTest("America/Los_Angeles"),
		}) {
		t.Fatalf("CreateApp() normalization = %#v", created.Definition)
	}
	if created.CreatedAt.IsZero() || !created.CreatedAt.Equal(created.UpdatedAt) || created.ArchivedAt != nil {
		t.Fatalf("CreateApp() timestamps = created %v updated %v archived %v", created.CreatedAt, created.UpdatedAt, created.ArchivedAt)
	}

	for name, selector := range map[string]AppSelector{
		"id":   {AppID: created.ID},
		"slug": {Slug: " GRADETHIS_PROD "},
	} {
		t.Run("get by "+name, func(t *testing.T) {
			got, getErr := catalog.GetApp(ctx, scope, selector)
			if getErr != nil {
				t.Fatalf("GetApp() error = %v", getErr)
			}
			if !reflect.DeepEqual(got, created) {
				t.Fatalf("GetApp() = %#v, want %#v", got, created)
			}
		})
	}
	if _, err := catalog.GetApp(ctx, AppAccessScope{TenantID: "tenant-b"}, AppSelector{AppID: created.ID}); !errors.Is(err, ErrNotFound) {
		t.Fatalf("cross-tenant GetApp() error = %v, want ErrNotFound", err)
	}

	replacement := created.Definition
	replacement.Slug = "GRADETHIS_PROD"
	replacement.DisplayName = "Application Logs"
	replacement.Description = ""
	replacement.DefaultIndexes = []string{"main"}
	replacement.DefaultTimeRange = nil
	updated, err := catalog.UpdateApp(ctx, scope, AppSelector{Slug: created.Definition.Slug}, created.Version, replacement)
	if err != nil {
		t.Fatalf("UpdateApp() error = %v", err)
	}
	if updated.Version != 2 ||
		updated.Definition.Slug != created.Definition.Slug ||
		updated.Definition.DisplayName != "Application Logs" ||
		updated.Definition.Description != "" ||
		!slices.Equal(updated.Definition.DefaultIndexes, []string{"main"}) ||
		updated.Definition.DefaultTimeRange != nil ||
		!updated.UpdatedAt.After(created.UpdatedAt) {
		t.Fatalf("UpdateApp() = %#v", updated)
	}
	if _, err := catalog.UpdateApp(ctx, scope, AppSelector{AppID: created.ID}, created.Version, replacement); !errors.Is(err, ErrVersionConflict) {
		t.Fatalf("stale UpdateApp() error = %v, want ErrVersionConflict", err)
	}
	rename := replacement
	rename.Slug = "renamed"
	if _, err := catalog.UpdateApp(ctx, scope, AppSelector{AppID: created.ID}, updated.Version, rename); !errors.Is(err, ErrImmutableSlug) {
		t.Fatalf("renaming UpdateApp() error = %v, want ErrImmutableSlug", err)
	}

	if _, err := catalog.DeleteApp(ctx, scope, AppSelector{AppID: created.ID}, updated.Version, created.Definition.Slug); !errors.Is(err, ErrDependencyConflict) {
		t.Fatalf("DeleteApp(active) error = %v, want ErrDependencyConflict", err)
	}
	if _, err := db.SQLDB().ExecContext(ctx, `
		DELETE FROM app_workspaces WHERE app_id = ?`,
		created.ID,
	); err == nil {
		t.Fatal("direct active app deletion unexpectedly succeeded")
	}
	archived, err := catalog.SetAppState(ctx, scope, AppSelector{AppID: created.ID}, updated.Version, AppStateArchived)
	if err != nil {
		t.Fatalf("SetAppState(archived) error = %v", err)
	}
	if archived.Version != 3 || archived.State != AppStateArchived || archived.ArchivedAt == nil ||
		archived.ArchivedAt.Before(archived.CreatedAt) || archived.ArchivedAt.After(archived.UpdatedAt) {
		t.Fatalf("SetAppState(archived) = %#v", archived)
	}
	if _, err := catalog.DeleteApp(
		ctx,
		scope,
		AppSelector{AppID: created.ID},
		archived.Version,
		"wrong-slug",
	); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("DeleteApp(wrong confirmation) error = %v, want ErrInvalidArgument", err)
	}
	if _, err := catalog.DeleteApp(
		ctx,
		scope,
		AppSelector{AppID: created.ID},
		archived.Version,
		" GRADETHIS_PROD ",
	); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("DeleteApp(noncanonical confirmation) error = %v, want ErrInvalidArgument", err)
	}

	if _, err := db.SQLDB().ExecContext(ctx, `
		INSERT INTO saved_searches (
			saved_search_id, version, name, app_id, owner_id, sharing_scope,
			definition_proto, created_at_unix_micro, updated_at_unix_micro
		) VALUES ('saved_app_dependency', 1, 'dependency', ?, 'owner', 1, X'01', 1, 1)`,
		created.ID,
	); err != nil {
		t.Fatalf("insert saved-search dependency: %v", err)
	}
	if _, err := catalog.DeleteApp(ctx, scope, AppSelector{Slug: created.Definition.Slug}, archived.Version, created.Definition.Slug); !errors.Is(err, ErrDependencyConflict) {
		t.Fatalf("DeleteApp(referenced) error = %v, want ErrDependencyConflict", err)
	}
	if _, err := db.SQLDB().ExecContext(ctx, `DELETE FROM app_workspaces WHERE app_id = ?`, created.ID); err == nil {
		t.Fatal("direct referenced app deletion unexpectedly succeeded")
	}
	if _, err := db.SQLDB().ExecContext(ctx, `DELETE FROM saved_searches WHERE saved_search_id = 'saved_app_dependency'`); err != nil {
		t.Fatalf("delete saved-search dependency: %v", err)
	}
	deletedID, err := catalog.DeleteApp(
		ctx,
		scope,
		AppSelector{AppID: created.ID},
		archived.Version,
		created.Definition.Slug,
	)
	if err != nil {
		t.Fatalf("DeleteApp() error = %v", err)
	}
	if deletedID != created.ID {
		t.Fatalf("DeleteApp() ID = %q, want %q", deletedID, created.ID)
	}
	if _, err := catalog.GetApp(ctx, scope, AppSelector{AppID: created.ID}); !errors.Is(err, ErrNotFound) {
		t.Fatalf("GetApp(after delete) error = %v, want ErrNotFound", err)
	}
	if _, err := db.SQLDB().ExecContext(ctx, `
		INSERT INTO saved_searches (
			saved_search_id, version, name, app_id, owner_id, sharing_scope,
			definition_proto, created_at_unix_micro, updated_at_unix_micro
		) VALUES ('saved_after_app_delete', 1, 'late dependency', ?, 'owner', 1, X'01', 1, 1)`,
		created.ID,
	); err == nil {
		t.Fatal("canonical saved-search reference created after app deletion")
	}

	if mainIndex.State != IndexStateActive || audit.State != IndexStateActive {
		t.Fatalf("test index states changed: main=%s audit=%s", mainIndex.State, audit.State)
	}
}

func TestAppMigrationGrandfathersLegacyLabelsButGuardsCanonicalIDs(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "legacy-app-upgrade.sqlite")
	raw, err := sql.Open("sqlite", sqliteDSN(path))
	if err != nil {
		t.Fatalf("open legacy SQLite: %v", err)
	}
	defer raw.Close()
	if err := ApplyMigrations(ctx, raw, migrationsBefore(t, "0010_")); err != nil {
		t.Fatalf("apply pre-app migrations: %v", err)
	}
	const grandfatheredID = "app_000000000000000000000A"
	if _, err := raw.ExecContext(ctx, `
		INSERT INTO saved_searches (
			saved_search_id, version, name, app_id, owner_id, sharing_scope,
			definition_proto, created_at_unix_micro, updated_at_unix_micro
		) VALUES ('legacy-canonical', 1, 'legacy', ?, 'owner', 1, X'01', 1, 1)`,
		grandfatheredID,
	); err != nil {
		t.Fatalf("insert pre-migration canonical-looking namespace: %v", err)
	}
	if err := ApplyMigrations(ctx, raw, migrations.SQLite()); err != nil {
		t.Fatalf("apply app migration: %v", err)
	}
	if _, err := raw.ExecContext(ctx, `
		INSERT INTO app_workspaces (
			app_id, tenant_id, version, slug, display_name, description,
			default_time_range_present, state,
			created_at_unix_micro, updated_at_unix_micro
		) VALUES (?, 'tenant', 1, 'legacy-adoption', 'Legacy adoption', '', 0, 'active', 1, 1)`,
		grandfatheredID,
	); err == nil {
		t.Fatal("app row adopted a grandfathered saved-search namespace")
	}
	const noncanonicalTailID = "app_000000000000000000000B"
	if validCanonicalAppID(noncanonicalTailID) {
		t.Fatal("code accepted a noncanonical base64url tail")
	}
	if _, err := raw.ExecContext(ctx, `
		INSERT INTO app_workspaces (
			app_id, tenant_id, version, slug, display_name, description,
			default_time_range_present, state,
			created_at_unix_micro, updated_at_unix_micro
		) VALUES (?, 'tenant', 1, 'bad-tail', 'Bad tail', '', 0, 'active', 1, 1)`,
		noncanonicalTailID,
	); err == nil {
		t.Fatal("SQL schema accepted a noncanonical base64url tail")
	}
	if _, err := raw.ExecContext(ctx, `
		INSERT INTO saved_searches (
			saved_search_id, version, name, app_id, owner_id, sharing_scope,
			definition_proto, created_at_unix_micro, updated_at_unix_micro
		) VALUES ('legacy-canonical-second', 1, 'legacy second', ?, 'owner', 1, X'01', 2, 2)`,
		grandfatheredID,
	); err != nil {
		t.Fatalf("grandfathered canonical namespace insert: %v", err)
	}
	if _, err := raw.ExecContext(ctx, `
		INSERT INTO saved_searches (
			saved_search_id, version, name, app_id, owner_id, sharing_scope,
			definition_proto, created_at_unix_micro, updated_at_unix_micro
		) VALUES ('legacy-label', 1, 'legacy label', 'app-main', 'owner', 1, X'01', 3, 3)`); err != nil {
		t.Fatalf("legacy slug-like namespace insert: %v", err)
	}
	const missingCanonicalID = "app_000000000000000000000Q"
	if _, err := raw.ExecContext(ctx, `
		INSERT INTO saved_searches (
			saved_search_id, version, name, app_id, owner_id, sharing_scope,
			definition_proto, created_at_unix_micro, updated_at_unix_micro
		) VALUES ('missing-canonical', 1, 'missing', ?, 'owner', 1, X'01', 4, 4)`,
		missingCanonicalID,
	); err == nil {
		t.Fatal("new missing canonical namespace unexpectedly succeeded")
	}
	if _, err := raw.ExecContext(ctx, `
		UPDATE saved_searches SET app_id = ?
		WHERE saved_search_id = 'legacy-label'`,
		missingCanonicalID,
	); err == nil {
		t.Fatal("update to missing canonical namespace unexpectedly succeeded")
	}
	if err := raw.Close(); err != nil {
		t.Fatalf("close upgraded raw database: %v", err)
	}
	database, err := Open(ctx, path)
	if err != nil {
		t.Fatalf("open upgraded control database: %v", err)
	}
	defer database.Close()
	var idCalls int
	catalog, err := NewAppCatalog(database, AppCatalogOptions{
		CursorKey: []byte("app-catalog-test-cursor-key-32-bytes-minimum"),
		IDGenerator: func() (string, error) {
			idCalls++
			if idCalls == 1 {
				return grandfatheredID, nil
			}
			return "app_000000000000000000000g", nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	created, err := catalog.CreateApp(
		ctx,
		AppAccessScope{TenantID: "tenant"},
		validAppDefinition("collision-safe"),
	)
	if err != nil {
		t.Fatalf("CreateApp(after legacy ID collision): %v", err)
	}
	if idCalls != 2 || created.ID != "app_000000000000000000000g" {
		t.Fatalf("legacy namespace collision calls/ID = %d/%q", idCalls, created.ID)
	}
}

func TestAppDeletionSerializesAgainstPostCommitCanonicalReference(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db := openTestDB(t)
	catalog := newTestAppCatalog(t, db)
	scope := AppAccessScope{TenantID: "tenant"}
	created, err := catalog.CreateApp(ctx, scope, validAppDefinition("delete-race"))
	if err != nil {
		t.Fatal(err)
	}
	archived, err := catalog.SetAppState(
		ctx,
		scope,
		AppSelector{AppID: created.ID},
		created.Version,
		AppStateArchived,
	)
	if err != nil {
		t.Fatal(err)
	}

	deleteConnection, err := db.SQLDB().Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer deleteConnection.Close()
	if _, err := deleteConnection.ExecContext(ctx, `BEGIN IMMEDIATE`); err != nil {
		t.Fatalf("begin delete transaction: %v", err)
	}
	committed := false
	defer func() {
		if !committed {
			_, _ = deleteConnection.ExecContext(context.Background(), `ROLLBACK`)
		}
	}()
	if _, err := deleteConnection.ExecContext(ctx, `
		DELETE FROM app_workspaces
		WHERE tenant_id = ? AND app_id = ? AND version = ? AND state = 'archived'`,
		scope.TenantID,
		created.ID,
		archived.Version,
	); err != nil {
		t.Fatalf("delete app inside transaction: %v", err)
	}

	insertDone := make(chan error, 1)
	go func() {
		_, insertErr := db.SQLDB().ExecContext(ctx, `
			INSERT INTO saved_searches (
				saved_search_id, version, name, app_id, owner_id, sharing_scope,
				definition_proto, created_at_unix_micro, updated_at_unix_micro
			) VALUES ('racing-reference', 1, 'race', ?, 'owner', 1, X'01', 1, 1)`,
			created.ID,
		)
		insertDone <- insertErr
	}()
	select {
	case insertErr := <-insertDone:
		t.Fatalf("saved-search insert did not wait for delete transaction: %v", insertErr)
	case <-time.After(25 * time.Millisecond):
	}
	if _, err := deleteConnection.ExecContext(ctx, `COMMIT`); err != nil {
		t.Fatalf("commit app deletion: %v", err)
	}
	committed = true
	select {
	case insertErr := <-insertDone:
		if insertErr == nil {
			t.Fatal("saved-search insert succeeded after app deletion committed")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("saved-search insert remained blocked after app deletion committed")
	}
}

func TestAppDefaultIndexesMustExistAndRemainSearchable(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db := openTestDB(t)
	catalog := newTestAppCatalog(t, db)
	queryable := mustCreateIndex(t, db, enabledIndex("queryable"))
	ingestionOnlyDefinition := enabledIndex("ingestion-only")
	ingestionOnlyDefinition.SearchEnabled = false
	ingestionOnly := mustCreateIndex(t, db, ingestionOnlyDefinition)
	deleting := mustCreateIndex(t, db, enabledIndex("deleting"))
	deleting, err := db.SetIndexState(
		ctx,
		deleting.ID,
		deleting.Version,
		IndexStateArchived,
	)
	if err != nil {
		t.Fatalf("archive deleting index: %v", err)
	}
	if _, err := db.BeginIndexDataDeletion(
		ctx,
		deleting.ID,
		deleting.Version,
		deleting.Definition.Name,
	); err != nil {
		t.Fatalf("begin deleting index operation: %v", err)
	}
	archived := mustCreateIndex(t, db, enabledIndex("archived"))
	archivedState, err := db.SetIndexState(ctx, archived.ID, archived.Version, IndexStateArchived)
	if err != nil {
		t.Fatalf("archive index: %v", err)
	}
	archived = archivedState
	scope := AppAccessScope{TenantID: "tenant"}

	for name, indexName := range map[string]string{
		"missing":        "missing",
		"ingestion only": ingestionOnly.Definition.Name,
		"deleting":       deleting.Definition.Name,
		"archived":       archived.Definition.Name,
	} {
		t.Run(name, func(t *testing.T) {
			definition := validAppDefinition("invalid-" + strings.ReplaceAll(name, " ", "-"))
			definition.DefaultIndexes = []string{indexName}
			_, createErr := catalog.CreateApp(ctx, scope, definition)
			if !errors.Is(createErr, ErrInvalidArgument) {
				t.Fatalf("CreateApp() error = %v, want ErrInvalidArgument", createErr)
			}
			var count int64
			if err := db.GORMDB().Model(&appRecord{}).Where("tenant_id = ?", scope.TenantID).Count(&count).Error; err != nil {
				t.Fatalf("count rolled-back apps: %v", err)
			}
			if count != 0 {
				t.Fatalf("failed CreateApp() left %d app rows", count)
			}
		})
	}

	created, err := catalog.CreateApp(ctx, scope, appDefinitionWithIndexes("eligible", queryable.Definition.Name))
	if err != nil {
		t.Fatalf("CreateApp(queryable) error = %v", err)
	}
	replacement := created.Definition
	replacement.DefaultIndexes = []string{ingestionOnly.Definition.Name}
	if _, err := catalog.UpdateApp(ctx, scope, AppSelector{AppID: created.ID}, created.Version, replacement); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("UpdateApp(ineligible) error = %v, want ErrInvalidArgument", err)
	}
	unchanged, err := catalog.GetApp(ctx, scope, AppSelector{AppID: created.ID})
	if err != nil {
		t.Fatalf("GetApp(after failed update): %v", err)
	}
	if unchanged.Version != created.Version || !slices.Equal(unchanged.Definition.DefaultIndexes, created.Definition.DefaultIndexes) {
		t.Fatalf("failed UpdateApp() was not atomic: %#v", unchanged)
	}

	queryableUpdate := queryable.Definition
	queryableUpdate.SearchEnabled = false
	if _, err := db.UpdateIndex(ctx, queryable.ID, queryable.Version, queryableUpdate); !errors.Is(err, ErrDependencyConflict) {
		t.Fatalf("disabling an app-default index error = %v, want ErrDependencyConflict", err)
	}
	if _, err := db.SetIndexState(ctx, queryable.ID, queryable.Version, IndexStateArchived); !errors.Is(err, ErrDependencyConflict) {
		t.Fatalf("archiving an app-default index error = %v, want ErrDependencyConflict", err)
	}
	if _, err := db.SetIndexState(ctx, queryable.ID, queryable.Version+1, IndexStateArchived); !errors.Is(err, ErrVersionConflict) {
		t.Fatalf("stale app-default index state error = %v, want ErrVersionConflict", err)
	}
	archivedApp, err := catalog.SetAppState(
		ctx,
		scope,
		AppSelector{AppID: created.ID},
		created.Version,
		AppStateArchived,
	)
	if err != nil {
		t.Fatalf("archive dependent app: %v", err)
	}
	queryable, err = db.SetIndexState(
		ctx,
		queryable.ID,
		queryable.Version,
		IndexStateArchived,
	)
	if err != nil {
		t.Fatalf("archive index referenced only by archived app: %v", err)
	}
	activeTarget, err := catalog.CreateApp(
		ctx,
		scope,
		validAppDefinition("active-target"),
	)
	if err != nil {
		t.Fatalf("create active reassignment target: %v", err)
	}
	if _, err := db.SQLDB().ExecContext(ctx, `
		UPDATE app_default_indexes
		SET app_id = ?
		WHERE tenant_id = ? AND app_id = ? AND index_id = ?`,
		activeTarget.ID,
		scope.TenantID,
		created.ID,
		queryable.ID,
	); err == nil {
		t.Fatal("inactive index membership reassignment to active app unexpectedly succeeded")
	}
	if _, err := catalog.SetAppState(
		ctx,
		scope,
		AppSelector{AppID: created.ID},
		archivedApp.Version,
		AppStateActive,
	); !errors.Is(err, ErrDependencyConflict) {
		t.Fatalf("reactivate app with archived index error = %v, want ErrDependencyConflict", err)
	}
	if _, err := catalog.UpdateApp(
		ctx,
		scope,
		AppSelector{AppID: created.ID},
		archivedApp.Version,
		archivedApp.Definition,
	); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("update app with archived index error = %v, want ErrInvalidArgument", err)
	}
}

func TestAppSlugUniquenessIsPerTenantAndErrorsDoNotEchoInput(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db := openTestDB(t)
	catalog := newTestAppCatalog(t, db)
	sentinel := "private-app-sentinel"
	first, err := catalog.CreateApp(ctx, AppAccessScope{TenantID: "tenant-a"}, validAppDefinition(sentinel))
	if err != nil {
		t.Fatal(err)
	}
	_, err = catalog.CreateApp(ctx, AppAccessScope{TenantID: "tenant-a"}, validAppDefinition(" "+strings.ToUpper(sentinel)+" "))
	if !errors.Is(err, ErrAlreadyExists) {
		t.Fatalf("duplicate CreateApp() error = %v, want ErrAlreadyExists", err)
	}
	if strings.Contains(err.Error(), sentinel) {
		t.Fatalf("duplicate error echoed app slug: %v", err)
	}
	second, err := catalog.CreateApp(ctx, AppAccessScope{TenantID: "tenant-b"}, validAppDefinition(sentinel))
	if err != nil {
		t.Fatalf("same slug in another tenant: %v", err)
	}
	if second.ID == first.ID {
		t.Fatalf("generated app IDs are not globally unique: %q", first.ID)
	}
}

func TestNewAppCatalogValidatesDependenciesAndClonesCursorKey(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	validKey := []byte("app-catalog-test-cursor-key-32-bytes-minimum")
	for name, test := range map[string]struct {
		database *DB
		key      []byte
	}{
		"nil database":  {database: nil, key: validKey},
		"short key":     {database: db, key: make([]byte, minimumAppCursorKeyBytes-1)},
		"oversized key": {database: db, key: make([]byte, maximumAppCursorKeyBytes+1)},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := NewAppCatalog(test.database, AppCatalogOptions{CursorKey: test.key}); !errors.Is(err, ErrInvalidArgument) {
				t.Fatalf("NewAppCatalog() error = %v, want ErrInvalidArgument", err)
			}
		})
	}
	key := slices.Clone(validKey)
	catalog, err := NewAppCatalog(db, AppCatalogOptions{CursorKey: key})
	if err != nil {
		t.Fatal(err)
	}
	key[0] ^= 0xff
	if catalog.cursorKey[0] == key[0] {
		t.Fatal("NewAppCatalog retained caller cursor-key storage")
	}

	invalidGenerator, err := NewAppCatalog(db, AppCatalogOptions{
		CursorKey: validKey,
		IDGenerator: func() (string, error) {
			return "app_not-an-exact-canonical-id", nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := invalidGenerator.CreateApp(
		context.Background(),
		AppAccessScope{TenantID: "tenant"},
		validAppDefinition("invalid-generator"),
	); err == nil || strings.Contains(err.Error(), "app_not-an-exact-canonical-id") {
		t.Fatalf("invalid generator error = %v, want safe failure", err)
	}
}

func TestAppDefinitionPresenceAndCallerStorageAreDetached(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db := openTestDB(t)
	catalog := newTestAppCatalog(t, db)
	index := mustCreateIndex(t, db, enabledIndex("main"))
	scope := AppAccessScope{TenantID: "tenant"}
	definition := AppDefinition{
		Slug:             "presence",
		DisplayName:      " Presence ",
		Description:      "   ",
		DefaultIndexes:   []string{index.Definition.Name},
		DefaultTimeRange: &AppTimeRange{},
	}
	created, err := catalog.CreateApp(ctx, scope, definition)
	if err != nil {
		t.Fatalf("CreateApp(): %v", err)
	}
	if created.Definition.Description != "" ||
		created.Definition.DefaultTimeRange == nil ||
		created.Definition.DefaultTimeRange.Earliest != nil ||
		created.Definition.DefaultTimeRange.Latest != nil ||
		created.Definition.DefaultTimeRange.Timezone != nil {
		t.Fatalf("canonical optional definition = %#v", created.Definition)
	}
	var present int64
	var earliest, latest, timezone sql.NullString
	if err := db.SQLDB().QueryRowContext(ctx, `
		SELECT default_time_range_present, default_earliest, default_latest, default_timezone
		FROM app_workspaces WHERE app_id = ?`,
		created.ID,
	).Scan(&present, &earliest, &latest, &timezone); err != nil {
		t.Fatal(err)
	}
	if present != 1 || earliest.Valid || latest.Valid || timezone.Valid {
		t.Fatalf("stored present-empty time range = %d/%#v/%#v/%#v", present, earliest, latest, timezone)
	}

	definition.DefaultIndexes[0] = "mutated"
	definition.DefaultTimeRange.Earliest = stringPointerForAppTest("0")
	created.Definition.DefaultIndexes[0] = "mutated-result"
	created.Definition.DefaultTimeRange.Latest = stringPointerForAppTest("0")
	got, err := catalog.GetApp(ctx, scope, AppSelector{AppID: created.ID})
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(got.Definition.DefaultIndexes, []string{"main"}) ||
		got.Definition.DefaultTimeRange == nil ||
		got.Definition.DefaultTimeRange.Earliest != nil ||
		got.Definition.DefaultTimeRange.Latest != nil {
		t.Fatalf("persisted app aliased caller storage: %#v", got.Definition)
	}

	nilRange, err := catalog.CreateApp(ctx, scope, AppDefinition{Slug: "nil-range"})
	if err != nil {
		t.Fatal(err)
	}
	if nilRange.Definition.DefaultTimeRange != nil {
		t.Fatalf("nil range materialized as %#v", nilRange.Definition.DefaultTimeRange)
	}
	if err := db.SQLDB().QueryRowContext(ctx, `
		SELECT default_time_range_present
		FROM app_workspaces WHERE app_id = ?`,
		nilRange.ID,
	).Scan(&present); err != nil {
		t.Fatal(err)
	}
	if present != 0 {
		t.Fatalf("stored nil time range presence = %d, want 0", present)
	}
}

func TestAppCatalogEnforcesPerTenantCapacity(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db := openTestDB(t)
	catalog := newTestAppCatalog(t, db)
	scope := AppAccessScope{TenantID: "tenant-full"}
	for index := 0; index < maximumAppsPerTenant; index++ {
		if _, err := catalog.CreateApp(
			ctx,
			scope,
			validAppDefinition(fmt.Sprintf("app-%03d", index)),
		); err != nil {
			t.Fatalf("CreateApp(%d): %v", index, err)
		}
	}
	if _, err := catalog.CreateApp(
		ctx,
		scope,
		validAppDefinition("app-000"),
	); !errors.Is(err, ErrAlreadyExists) {
		t.Fatalf("CreateApp(duplicate at capacity) error = %v, want ErrAlreadyExists", err)
	}
	if _, err := catalog.CreateApp(
		ctx,
		scope,
		validAppDefinition("one-too-many"),
	); !errors.Is(err, ErrCapacityExceeded) {
		t.Fatalf("CreateApp(over capacity) error = %v, want ErrCapacityExceeded", err)
	}
	if _, err := catalog.CreateApp(
		ctx,
		AppAccessScope{TenantID: "tenant-other"},
		validAppDefinition("independent"),
	); err != nil {
		t.Fatalf("another tenant incorrectly shared capacity: %v", err)
	}
}

func TestListAppsUsesBoundedDeterministicKeysetPages(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db := openTestDB(t)
	catalog := newTestAppCatalog(t, db)
	scope := AppAccessScope{TenantID: "tenant-list"}
	otherScope := AppAccessScope{TenantID: "tenant-other"}
	for _, item := range []struct {
		slug, display string
		state         AppState
	}{
		{slug: "charlie", display: "Same", state: AppStateActive},
		{slug: "alpha", display: "Alpha", state: AppStateActive},
		{slug: "bravo", display: "Same", state: AppStateArchived},
		{slug: "delta", display: "Zulu", state: AppStateActive},
	} {
		definition := validAppDefinition(item.slug)
		definition.DisplayName = item.display
		created, err := catalog.CreateApp(ctx, scope, definition)
		if err != nil {
			t.Fatalf("CreateApp(%s): %v", item.slug, err)
		}
		if item.state == AppStateArchived {
			if _, err := catalog.SetAppState(ctx, scope, AppSelector{AppID: created.ID}, created.Version, item.state); err != nil {
				t.Fatalf("archive %s: %v", item.slug, err)
			}
		}
	}
	if _, err := catalog.CreateApp(ctx, otherScope, validAppDefinition("alpha")); err != nil {
		t.Fatalf("CreateApp(other tenant): %v", err)
	}

	first, err := catalog.ListApps(ctx, scope, AppListRequest{
		PageSize:     2,
		IncludeTotal: true,
		SortBy:       AppSortByDisplayName,
		Direction:    AppSortAscending,
	})
	if err != nil {
		t.Fatalf("ListApps(first) error = %v", err)
	}
	if got := appSlugs(first.Apps); !slices.Equal(got, []string{"alpha", "charlie"}) {
		t.Fatalf("first page = %v, want [alpha charlie]", got)
	}
	if first.NextPageToken == nil || first.TotalSize == nil || *first.TotalSize != 4 || !first.TotalSizeExact {
		t.Fatalf("first page metadata = next %#v total %#v exact %t", first.NextPageToken, first.TotalSize, first.TotalSizeExact)
	}
	second, err := catalog.ListApps(ctx, scope, AppListRequest{
		PageSize:     2,
		IncludeTotal: true,
		PageToken: func() string {
			return *first.NextPageToken
		}(),
		SortBy:    AppSortByDisplayName,
		Direction: AppSortAscending,
	})
	if err != nil {
		t.Fatalf("ListApps(second) error = %v", err)
	}
	if got := appSlugs(second.Apps); !slices.Equal(got, []string{"bravo", "delta"}) || second.NextPageToken != nil {
		t.Fatalf("second page = %v next %#v, want [bravo delta] and nil", got, second.NextPageToken)
	}

	text := "same"
	filtered, err := catalog.ListApps(ctx, scope, AppListRequest{
		PageSize:   10,
		TextFilter: &text,
		StateFilters: []AppState{
			AppStateArchived,
			AppStateArchived,
		},
		SortBy:    AppSortByUpdatedAt,
		Direction: AppSortDescending,
	})
	if err != nil {
		t.Fatalf("ListApps(filtered) error = %v", err)
	}
	if got := appSlugs(filtered.Apps); !slices.Equal(got, []string{"bravo"}) {
		t.Fatalf("filtered page = %v, want [bravo]", got)
	}

	for _, sortBy := range []AppSortBy{AppSortByCreatedAt, AppSortByUpdatedAt} {
		listed, err := catalog.ListApps(ctx, scope, AppListRequest{
			PageSize:  10,
			SortBy:    sortBy,
			Direction: AppSortDescending,
		})
		if err != nil {
			t.Fatalf("ListApps(%s) error = %v", sortBy, err)
		}
		if len(listed.Apps) != 4 {
			t.Fatalf("ListApps(%s) count = %d, want 4", sortBy, len(listed.Apps))
		}
	}
}

func TestAppListCursorIsAuthenticatedBoundAndRestartStable(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db := openTestDB(t)
	catalog := newTestAppCatalog(t, db)
	scope := AppAccessScope{TenantID: "tenant"}
	for _, slug := range []string{"alpha", "bravo", "charlie"} {
		if _, err := catalog.CreateApp(ctx, scope, validAppDefinition(slug)); err != nil {
			t.Fatal(err)
		}
	}
	first, err := catalog.ListApps(ctx, scope, AppListRequest{
		PageSize:  1,
		SortBy:    AppSortByDisplayName,
		Direction: AppSortAscending,
	})
	if err != nil || first.NextPageToken == nil {
		t.Fatalf("ListApps(first) = (%#v, %v)", first, err)
	}

	reopenedCatalog := newTestAppCatalog(t, db)
	requiredRevision := first.CatalogRevision
	second, err := reopenedCatalog.ListApps(ctx, scope, AppListRequest{
		PageSize:                1,
		PageToken:               *first.NextPageToken,
		RequiredCatalogRevision: &requiredRevision,
		SortBy:                  AppSortByDisplayName,
		Direction:               AppSortAscending,
	})
	if err != nil || !slices.Equal(appSlugs(second.Apps), []string{"bravo"}) {
		t.Fatalf("restart-stable continuation = (%v, %v)", appSlugs(second.Apps), err)
	}

	token := *first.NextPageToken
	tampered := token[:len(token)-1] + map[bool]string{true: "A", false: "B"}[token[len(token)-1] != 'A']
	text := "a"
	mismatches := []struct {
		name    string
		scope   AppAccessScope
		request AppListRequest
	}{
		{
			name:  "tampered",
			scope: scope,
			request: AppListRequest{
				PageSize: 1, PageToken: tampered,
				SortBy: AppSortByDisplayName, Direction: AppSortAscending,
			},
		},
		{
			name:  "tenant",
			scope: AppAccessScope{TenantID: "other"},
			request: AppListRequest{
				PageSize: 1, PageToken: token,
				SortBy: AppSortByDisplayName, Direction: AppSortAscending,
			},
		},
		{
			name:  "page size",
			scope: scope,
			request: AppListRequest{
				PageSize: 2, PageToken: token,
				SortBy: AppSortByDisplayName, Direction: AppSortAscending,
			},
		},
		{
			name:  "filter",
			scope: scope,
			request: AppListRequest{
				PageSize: 1, PageToken: token, TextFilter: &text,
				SortBy: AppSortByDisplayName, Direction: AppSortAscending,
			},
		},
		{
			name:  "sort",
			scope: scope,
			request: AppListRequest{
				PageSize: 1, PageToken: token,
				SortBy: AppSortByCreatedAt, Direction: AppSortAscending,
			},
		},
		{
			name:  "revision",
			scope: scope,
			request: AppListRequest{
				PageSize: 1, PageToken: token,
				RequiredCatalogRevision: func() *uint64 {
					value := first.CatalogRevision + 1
					return &value
				}(),
				SortBy: AppSortByDisplayName, Direction: AppSortAscending,
			},
		},
	}
	for _, mismatch := range mismatches {
		t.Run(mismatch.name, func(t *testing.T) {
			if _, err := catalog.ListApps(ctx, mismatch.scope, mismatch.request); !errors.Is(err, ErrInvalidArgument) {
				t.Fatalf("ListApps() error = %v, want ErrInvalidArgument", err)
			}
		})
	}
}

func TestAppListCursorRejectsEveryCatalogMutation(t *testing.T) {
	t.Parallel()

	for _, mutation := range []string{"update", "state", "delete"} {
		t.Run(mutation, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			db := openTestDB(t)
			catalog := newTestAppCatalog(t, db)
			scope := AppAccessScope{TenantID: "tenant"}
			apps := make(map[string]AppWorkspace)
			for _, slug := range []string{"alpha", "bravo", "charlie"} {
				created, err := catalog.CreateApp(ctx, scope, validAppDefinition(slug))
				if err != nil {
					t.Fatal(err)
				}
				apps[slug] = created
			}
			if mutation == "delete" {
				archived, err := catalog.SetAppState(
					ctx,
					scope,
					AppSelector{AppID: apps["charlie"].ID},
					apps["charlie"].Version,
					AppStateArchived,
				)
				if err != nil {
					t.Fatal(err)
				}
				apps["charlie"] = archived
			}
			first, err := catalog.ListApps(ctx, scope, AppListRequest{
				PageSize:  1,
				SortBy:    AppSortByDisplayName,
				Direction: AppSortAscending,
			})
			if err != nil || first.NextPageToken == nil {
				t.Fatalf("ListApps(first) = (%#v, %v)", first, err)
			}
			switch mutation {
			case "update":
				replacement := apps["bravo"].Definition
				replacement.DisplayName = "Moved"
				_, err = catalog.UpdateApp(
					ctx,
					scope,
					AppSelector{AppID: apps["bravo"].ID},
					apps["bravo"].Version,
					replacement,
				)
			case "state":
				_, err = catalog.SetAppState(
					ctx,
					scope,
					AppSelector{AppID: apps["bravo"].ID},
					apps["bravo"].Version,
					AppStateArchived,
				)
			case "delete":
				_, err = catalog.DeleteApp(
					ctx,
					scope,
					AppSelector{AppID: apps["charlie"].ID},
					apps["charlie"].Version,
					apps["charlie"].Definition.Slug,
				)
			}
			if err != nil {
				t.Fatalf("%s mutation: %v", mutation, err)
			}
			_, err = catalog.ListApps(ctx, scope, AppListRequest{
				PageSize:  1,
				PageToken: *first.NextPageToken,
				SortBy:    AppSortByDisplayName,
				Direction: AppSortAscending,
			})
			if !errors.Is(err, ErrPageInvalidated) {
				t.Fatalf("continuation after %s error = %v, want ErrPageInvalidated", mutation, err)
			}
		})
	}
}

func TestAppsPersistAcrossReopenAndRejectCorruptRows(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "control.sqlite")
	db, err := Open(ctx, path)
	if err != nil {
		t.Fatalf("Open(): %v", err)
	}
	catalog := newTestAppCatalog(t, db)
	index := mustCreateIndex(t, db, enabledIndex("persistent"))
	scope := AppAccessScope{TenantID: "tenant"}
	created, err := catalog.CreateApp(ctx, scope, appDefinitionWithIndexes("persistent", index.Definition.Name))
	if err != nil {
		t.Fatalf("CreateApp(): %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close(): %v", err)
	}
	db, err = Open(ctx, path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	catalog = newTestAppCatalog(t, db)
	t.Cleanup(func() { _ = db.Close() })
	reopened, err := catalog.GetApp(ctx, scope, AppSelector{AppID: created.ID})
	if err != nil {
		t.Fatalf("GetApp(after reopen): %v", err)
	}
	if !reflect.DeepEqual(reopened, created) {
		t.Fatalf("GetApp(after reopen) = %#v, want %#v", reopened, created)
	}

	connection, err := db.SQLDB().Conn(ctx)
	if err != nil {
		t.Fatalf("acquire corruption connection: %v", err)
	}
	defer connection.Close()
	if _, err := connection.ExecContext(ctx, `PRAGMA ignore_check_constraints = ON`); err != nil {
		t.Fatalf("disable test-only check constraints: %v", err)
	}
	if _, err := connection.ExecContext(ctx, `
		UPDATE app_workspaces
		SET default_time_range_present = 2, description = 'private-description-sentinel'
		WHERE app_id = ?`, created.ID); err != nil {
		t.Fatalf("corrupt app record: %v", err)
	}
	_, err = catalog.GetApp(ctx, scope, AppSelector{AppID: created.ID})
	if err == nil || !strings.Contains(err.Error(), "invalid app record in control-plane database") {
		t.Fatalf("GetApp(corrupt) error = %v, want invalid-record error", err)
	}
	if strings.Contains(err.Error(), "private-description-sentinel") {
		t.Fatalf("corrupt-row error leaked persisted data: %v", err)
	}
}

func TestActiveAppReadsTreatUnavailablePersistedIndexAsInternalCorruption(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db := openTestDB(t)
	catalog := newTestAppCatalog(t, db)
	index := mustCreateIndex(t, db, enabledIndex("corrupt-membership"))
	scope := AppAccessScope{TenantID: "tenant"}
	created, err := catalog.CreateApp(
		ctx,
		scope,
		appDefinitionWithIndexes("corrupt-membership", index.Definition.Name),
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.SQLDB().ExecContext(ctx, `
		DROP TRIGGER active_app_default_indexes_remain_searchable;
		UPDATE indexes SET state = 'archived' WHERE index_id = ?`,
		index.ID,
	); err != nil {
		t.Fatalf("create test-only persisted dependency corruption: %v", err)
	}
	_, err = catalog.GetApp(ctx, scope, AppSelector{AppID: created.ID})
	if err == nil ||
		errors.Is(err, ErrInvalidArgument) ||
		!strings.Contains(err.Error(), "unavailable in control-plane database") {
		t.Fatalf("GetApp(corrupt dependency) error = %v", err)
	}
	_, err = catalog.ListApps(ctx, scope, AppListRequest{})
	if err == nil ||
		errors.Is(err, ErrInvalidArgument) ||
		!strings.Contains(err.Error(), "unavailable in control-plane database") {
		t.Fatalf("ListApps(corrupt dependency) error = %v", err)
	}
}

func TestAppListRejectsCorruptCatalogRevision(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db := openTestDB(t)
	catalog := newTestAppCatalog(t, db)
	scope := AppAccessScope{TenantID: "tenant"}
	if _, err := catalog.CreateApp(ctx, scope, validAppDefinition("revision")); err != nil {
		t.Fatal(err)
	}
	connection, err := db.SQLDB().Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	if _, err := connection.ExecContext(ctx, `PRAGMA ignore_check_constraints = ON`); err != nil {
		t.Fatal(err)
	}
	if _, err := connection.ExecContext(ctx, `
		UPDATE app_catalog_revisions SET revision = 0 WHERE tenant_id = ?`,
		scope.TenantID,
	); err != nil {
		t.Fatal(err)
	}
	if _, err := catalog.ListApps(ctx, scope, AppListRequest{}); err == nil ||
		!strings.Contains(err.Error(), "invalid app catalog revision") {
		t.Fatalf("ListApps(corrupt revision) error = %v", err)
	}
	if _, err := connection.ExecContext(ctx, `
		DELETE FROM app_catalog_revisions WHERE tenant_id = ?`,
		scope.TenantID,
	); err != nil {
		t.Fatal(err)
	}
	if _, err := catalog.ListApps(ctx, scope, AppListRequest{}); err == nil ||
		!strings.Contains(err.Error(), "invalid app catalog revision") {
		t.Fatalf("ListApps(missing revision) error = %v", err)
	}
}

func TestConcurrentAppUpdatesAllowOneOptimisticWinner(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db := openTestDB(t)
	catalog := newTestAppCatalog(t, db)
	scope := AppAccessScope{TenantID: "tenant"}
	created, err := catalog.CreateApp(ctx, scope, validAppDefinition("concurrent"))
	if err != nil {
		t.Fatal(err)
	}

	start := make(chan struct{})
	results := make(chan error, 2)
	var wait sync.WaitGroup
	for _, display := range []string{"winner-a", "winner-b"} {
		wait.Add(1)
		go func(display string) {
			defer wait.Done()
			definition := created.Definition
			definition.DisplayName = display
			<-start
			_, updateErr := catalog.UpdateApp(ctx, scope, AppSelector{AppID: created.ID}, created.Version, definition)
			results <- updateErr
		}(display)
	}
	close(start)
	wait.Wait()
	close(results)

	var successes, conflicts int
	for err := range results {
		switch {
		case err == nil:
			successes++
		case errors.Is(err, ErrVersionConflict):
			conflicts++
		default:
			t.Fatalf("concurrent UpdateApp() unexpected error = %v", err)
		}
	}
	if successes != 1 || conflicts != 1 {
		t.Fatalf("concurrent results = %d successes/%d conflicts, want 1/1", successes, conflicts)
	}
}

func TestAppValidationIsCanonicalAndBounded(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db := openTestDB(t)
	catalog := newTestAppCatalog(t, db)
	scope := AppAccessScope{TenantID: "tenant"}
	valid := validAppDefinition("valid")
	invalidUTF8 := string([]byte{0xff})
	tests := []struct {
		name   string
		scope  AppAccessScope
		mutate func(*AppDefinition)
	}{
		{name: "empty tenant", scope: AppAccessScope{}},
		{name: "padded tenant", scope: AppAccessScope{TenantID: " tenant "}},
		{name: "control tenant", scope: AppAccessScope{TenantID: "ten\nant"}},
		{name: "oversized tenant", scope: AppAccessScope{TenantID: strings.Repeat("t", maximumAppTenantIDBytes+1)}},
		{name: "invalid tenant utf8", scope: AppAccessScope{TenantID: invalidUTF8}},
		{name: "empty slug", scope: scope, mutate: func(definition *AppDefinition) { definition.Slug = " " }},
		{name: "invalid slug", scope: scope, mutate: func(definition *AppDefinition) { definition.Slug = "_private" }},
		{name: "oversized slug", scope: scope, mutate: func(definition *AppDefinition) {
			definition.Slug = strings.Repeat("a", maximumAppSlugBytes+1)
		}},
		{name: "oversized display", scope: scope, mutate: func(definition *AppDefinition) {
			definition.DisplayName = strings.Repeat("d", maximumAppDisplayNameBytes+1)
		}},
		{name: "control display", scope: scope, mutate: func(definition *AppDefinition) {
			definition.DisplayName = "bad\nname"
		}},
		{name: "oversized description", scope: scope, mutate: func(definition *AppDefinition) {
			definition.Description = strings.Repeat("d", maximumAppDescriptionBytes+1)
		}},
		{name: "too many indexes", scope: scope, mutate: func(definition *AppDefinition) {
			definition.DefaultIndexes = make([]string, maximumAppDefaultIndexes+1)
		}},
		{name: "invalid earliest", scope: scope, mutate: func(definition *AppDefinition) {
			definition.DefaultTimeRange = &AppTimeRange{
				Earliest: stringPointerForAppTest("tomorrow"),
				Latest:   stringPointerForAppTest("now"),
			}
		}},
		{name: "invalid timezone", scope: scope, mutate: func(definition *AppDefinition) {
			definition.DefaultTimeRange = &AppTimeRange{
				Earliest: stringPointerForAppTest("-24h"),
				Latest:   stringPointerForAppTest("now"),
				Timezone: stringPointerForAppTest("Local"),
			}
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			definition := valid
			definition.DefaultIndexes = slices.Clone(valid.DefaultIndexes)
			if test.mutate != nil {
				test.mutate(&definition)
			}
			_, err := catalog.CreateApp(ctx, test.scope, definition)
			if !errors.Is(err, ErrInvalidArgument) {
				t.Fatalf("CreateApp() error = %v, want ErrInvalidArgument", err)
			}
		})
	}

	for _, selector := range []AppSelector{{}, {AppID: "one", Slug: "two"}, {AppID: " padded "}} {
		if _, err := catalog.GetApp(ctx, scope, selector); !errors.Is(err, ErrInvalidArgument) {
			t.Fatalf("GetApp(%#v) error = %v, want ErrInvalidArgument", selector, err)
		}
	}
	for _, version := range []uint64{0, ^uint64(0)} {
		if _, err := catalog.UpdateApp(ctx, scope, AppSelector{AppID: "app_valid"}, version, valid); !errors.Is(err, ErrInvalidArgument) {
			t.Fatalf("UpdateApp(version %d) error = %v, want ErrInvalidArgument", version, err)
		}
	}
	if _, err := catalog.ListApps(ctx, scope, AppListRequest{PageSize: maximumAppListPageSize + 1}); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("ListApps(oversized page) error = %v, want ErrInvalidArgument", err)
	}
}

func TestAppOperationsPreserveCanceledContextIdentity(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	catalog := newTestAppCatalog(t, db)
	scope := AppAccessScope{TenantID: "tenant"}
	created, err := catalog.CreateApp(context.Background(), scope, validAppDefinition("canceled"))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	operations := map[string]func() error{
		"create": func() error {
			_, operationErr := catalog.CreateApp(ctx, scope, validAppDefinition("new"))
			return operationErr
		},
		"get": func() error {
			_, operationErr := catalog.GetApp(ctx, scope, AppSelector{AppID: created.ID})
			return operationErr
		},
		"list": func() error {
			_, operationErr := catalog.ListApps(ctx, scope, AppListRequest{})
			return operationErr
		},
		"update": func() error {
			_, operationErr := catalog.UpdateApp(ctx, scope, AppSelector{AppID: created.ID}, created.Version, created.Definition)
			return operationErr
		},
		"state": func() error {
			_, operationErr := catalog.SetAppState(ctx, scope, AppSelector{AppID: created.ID}, created.Version, AppStateArchived)
			return operationErr
		},
		"delete": func() error {
			_, operationErr := catalog.DeleteApp(
				ctx,
				scope,
				AppSelector{AppID: created.ID},
				created.Version,
				created.Definition.Slug,
			)
			return operationErr
		},
	}
	for name, operation := range operations {
		t.Run(name, func(t *testing.T) {
			if operationErr := operation(); !errors.Is(operationErr, context.Canceled) {
				t.Fatalf("operation error = %v, want context.Canceled", operationErr)
			}
		})
	}
}

func TestAppGORMModelsMatchMigratedSQLiteColumns(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db := openTestDB(t)
	tests := []struct {
		table string
		model any
	}{
		{table: "app_workspaces", model: &appRecord{}},
		{table: "app_default_indexes", model: &appDefaultIndexRecord{}},
		{table: "app_catalog_revisions", model: &appCatalogRevisionRecord{}},
	}
	for _, test := range tests {
		t.Run(test.table, func(t *testing.T) {
			statement := &gorm.Statement{DB: db.GORMDB()}
			if err := statement.Parse(test.model); err != nil {
				t.Fatalf("parse GORM model: %v", err)
			}
			rows, err := db.SQLDB().QueryContext(ctx, fmt.Sprintf(`SELECT name FROM pragma_table_info('%s') ORDER BY cid`, test.table))
			if err != nil {
				t.Fatalf("read migrated columns: %v", err)
			}
			defer rows.Close()
			var migrated []string
			for rows.Next() {
				var name string
				if err := rows.Scan(&name); err != nil {
					t.Fatal(err)
				}
				migrated = append(migrated, name)
			}
			if err := rows.Err(); err != nil {
				t.Fatal(err)
			}
			if !slices.Equal(statement.Schema.DBNames, migrated) {
				t.Fatalf("GORM columns = %v, migrated columns = %v", statement.Schema.DBNames, migrated)
			}
			if test.table == "app_workspaces" {
				assertAppListIndexesMatchModel(t, db, statement)
			}
		})
	}
}

func assertAppListIndexesMatchModel(t *testing.T, db *DB, statement *gorm.Statement) {
	t.Helper()
	expected := map[string][]string{
		"app_workspaces_tenant_display_id_idx": {"tenant_id", "display_name", "app_id"},
		"app_workspaces_tenant_created_id_idx": {"tenant_id", "created_at_unix_micro", "app_id"},
		"app_workspaces_tenant_updated_id_idx": {"tenant_id", "updated_at_unix_micro", "app_id"},
	}
	modelIndexes := make(map[string][]string)
	for _, index := range statement.Schema.ParseIndexes() {
		fields := make([]string, len(index.Fields))
		for fieldIndex, option := range index.Fields {
			fields[fieldIndex] = option.DBName
		}
		modelIndexes[index.Name] = fields
	}
	for name, want := range expected {
		if got := modelIndexes[name]; !slices.Equal(got, want) {
			t.Errorf("GORM index %s columns = %v, want %v", name, got, want)
		}
		rows, err := db.SQLDB().QueryContext(
			context.Background(),
			fmt.Sprintf(`SELECT name FROM pragma_index_info('%s') ORDER BY seqno`, name),
		)
		if err != nil {
			t.Fatalf("read migrated index %s: %v", name, err)
		}
		var migrated []string
		for rows.Next() {
			var column string
			if err := rows.Scan(&column); err != nil {
				_ = rows.Close()
				t.Fatal(err)
			}
			migrated = append(migrated, column)
		}
		if err := rows.Close(); err != nil {
			t.Fatal(err)
		}
		if !slices.Equal(migrated, want) {
			t.Errorf("migrated index %s columns = %v, want %v", name, migrated, want)
		}
	}
}

func validAppDefinition(slug string) AppDefinition {
	return AppDefinition{
		Slug:        slug,
		DisplayName: slug,
		DefaultTimeRange: &AppTimeRange{
			Earliest: stringPointerForAppTest("-24h"),
			Latest:   stringPointerForAppTest("now"),
		},
	}
}

func appDefinitionWithIndexes(slug string, indexes ...string) AppDefinition {
	definition := validAppDefinition(slug)
	definition.DefaultIndexes = indexes
	return definition
}

func mustCreateIndex(t *testing.T, db *DB, definition IndexDefinition) Index {
	t.Helper()
	created, err := db.CreateIndex(context.Background(), definition)
	if err != nil {
		t.Fatalf("CreateIndex(%s): %v", definition.Name, err)
	}
	return created
}

func appSlugs(apps []AppWorkspace) []string {
	result := make([]string, len(apps))
	for index, app := range apps {
		result[index] = app.Definition.Slug
	}
	return result
}

var appCatalogTestSequence atomic.Uint64

func newTestAppCatalog(t *testing.T, db *DB) *AppCatalog {
	t.Helper()
	sequence := appCatalogTestSequence.Add(1)
	var idSequence atomic.Uint64
	base := time.Date(2026, time.July, 28, 12, 0, 0, int(sequence), time.UTC)
	var clockSequence atomic.Int64
	catalog, err := NewAppCatalog(db, AppCatalogOptions{
		CursorKey: []byte("app-catalog-test-cursor-key-32-bytes-minimum"),
		Clock: func() time.Time {
			return base.Add(time.Duration(clockSequence.Add(1)) * time.Microsecond)
		},
		IDGenerator: func() (string, error) {
			return fmt.Sprintf("app_%010d%011dA", sequence, idSequence.Add(1)), nil
		},
	})
	if err != nil {
		t.Fatalf("NewAppCatalog() error = %v", err)
	}
	return catalog
}

func stringPointerForAppTest(value string) *string {
	return &value
}
