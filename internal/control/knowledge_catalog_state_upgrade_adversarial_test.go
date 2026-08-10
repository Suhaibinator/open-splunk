package control

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/binary"
	"io/fs"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Suhaibinator/open-splunk/migrations"
)

func TestKnowledgeCatalogStateMigrationPreflightsPhysicalVersionOverCap(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name     string
		tenantID string
	}{
		{name: "registered tenant", tenantID: "tenant-a"},
		{name: "orphan physical tenant", tenantID: "tenant-orphan"},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			raw := openKnowledgeMigrationTestDB(t, "knowledge-state-physical-over-cap.sqlite")
			if err := ApplyMigrations(context.Background(), raw, migrationsBefore(t, "0029_")); err != nil {
				t.Fatalf("apply through migration 0028: %v", err)
			}
			seedKnowledgeStatePrerequisites(t, raw)
			raw.SetMaxOpenConns(1)
			raw.SetMaxIdleConns(1)
			conn, err := raw.Conn(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			if _, err := conn.ExecContext(context.Background(), `
		PRAGMA foreign_keys = OFF;
		DROP TRIGGER knowledge_object_insert_requires_sealed_list_projection;
		DROP TRIGGER knowledge_object_version_capacity_is_available;
		DROP TRIGGER knowledge_object_version_identity_collision_is_forbidden;
		DROP TRIGGER knowledge_object_version_is_contiguous;
		DROP TRIGGER knowledge_object_version_after_insert`); err != nil {
				_ = conn.Close()
				t.Fatalf("prepare corrupt physical history: %v", err)
			}
			if _, err := conn.ExecContext(context.Background(), `
		WITH RECURSIVE sequence(object_version) AS (
			VALUES (1)
			UNION ALL
			SELECT object_version + 1
			FROM sequence
			WHERE object_version < 65537
		)
		INSERT INTO knowledge_object_versions (
			tenant_id, knowledge_object_id, object_version,
			app_id, owner_id, object_type, name, sharing_scope, state,
			definition_digest, dependency_count, mutation_kind,
			quarantine_reason, created_at_unix_micro
		)
		SELECT ?, 'ko-physical-over-cap', object_version,
		       ?, 'owner-a', 'field_extraction', 'ko-physical-over-cap',
		       'private', 'active', zeroblob(32), 0,
		       CASE object_version WHEN 1 THEN 'create' ELSE 'update' END,
		       NULL, object_version
		FROM sequence`, test.tenantID, knowledgeMigrationTestAppID); err != nil {
				_ = conn.Close()
				t.Fatalf("inject corrupt physical history: %v", err)
			}
			if test.tenantID == "tenant-a" {
				if _, err := conn.ExecContext(context.Background(), `
			INSERT INTO knowledge_objects (
				tenant_id, knowledge_object_id, current_version,
				app_id, owner_id, object_type, name, sharing_scope, state,
				definition_digest, created_at_unix_micro, updated_at_unix_micro
			) VALUES (
				'tenant-a', 'ko-physical-over-cap', 65537,
				?, 'owner-a', 'field_extraction', 'ko-physical-over-cap',
				'private', 'active', zeroblob(32), 1, 65537
			)`, knowledgeMigrationTestAppID); err != nil {
					_ = conn.Close()
					t.Fatalf("inject over-cap registry authority: %v", err)
				}
			}
			if _, err := conn.ExecContext(context.Background(), `PRAGMA foreign_keys = ON`); err != nil {
				_ = conn.Close()
				t.Fatalf("restore foreign-key enforcement: %v", err)
			}
			var foreignKeys int
			if err := conn.QueryRowContext(context.Background(), `PRAGMA foreign_keys`).Scan(&foreignKeys); err != nil {
				_ = conn.Close()
				t.Fatal(err)
			}
			if foreignKeys != 1 {
				_ = conn.Close()
				t.Fatalf("foreign_keys = %d after injection, want 1", foreignKeys)
			}
			if err := conn.Close(); err != nil {
				t.Fatal(err)
			}
			assertIntegerQuery(t, raw, 65537, `
		SELECT count(*) FROM knowledge_object_versions
		WHERE tenant_id = ?`, test.tenantID)
			assertIntegerQuery(t, raw, 65537, `
		SELECT count(*) FROM (
			SELECT 1 FROM knowledge_object_versions
			WHERE tenant_id = ?
			LIMIT 65537
		)`, test.tenantID)

			migrationContext, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			err = ApplyMigrations(migrationContext, raw, migrations.SQLite())
			if migrationContext.Err() != nil {
				t.Fatalf("physical over-cap preflight did not make bounded progress: %v", migrationContext.Err())
			}
			if err == nil {
				t.Fatal("migration 0029 accepted more than 65,536 physical tenant versions")
			}
			assertIntegerQuery(t, raw, 28, `SELECT count(*) FROM schema_migrations`)
			assertIntegerQuery(t, raw, 0, `
		SELECT count(*) FROM sqlite_schema
		WHERE name IN (
			'knowledge_catalog_revision_heads',
			'knowledge_object_version_lifecycle',
			'knowledge_object_list_order_keys'
		)`)
			assertIntegerQuery(t, raw, 65537, `
		SELECT count(*) FROM knowledge_object_versions
		WHERE tenant_id = ?`, test.tenantID)
			var integrity string
			if err := raw.QueryRow(`PRAGMA integrity_check(1)`).Scan(&integrity); err != nil {
				t.Fatal(err)
			}
			if integrity != "ok" {
				t.Fatalf("structural integrity after rejected migration = %q", integrity)
			}
		})
	}
}

func TestKnowledgeCatalogStateMigrationBackfillsLongDisabledHistoryInBoundedTime(t *testing.T) {
	const (
		// Publication reserves the final 4,096 tenant-version slots for
		// quarantine closure, so 61,440 is the largest valid ordinary history.
		versionCount         = 61440
		enableVersion        = versionCount/2 + 1
		secondDisableVersion = enableVersion + 1
		updateVersionCount   = versionCount - 4
	)

	raw := openKnowledgeMigrationTestDB(t, "knowledge-state-long-disabled.sqlite")
	if err := ApplyMigrations(context.Background(), raw, migrationsBefore(t, "0029_")); err != nil {
		t.Fatalf("apply through migration 0028: %v", err)
	}
	seedKnowledgeStatePrerequisites(t, raw)

	tx, err := raw.Begin()
	if err != nil {
		t.Fatal(err)
	}
	rollback := func(format string, args ...any) {
		_ = tx.Rollback()
		t.Fatalf(format, args...)
	}
	if _, err := tx.Exec(`
		INSERT INTO knowledge_object_list_projections (
			tenant_id, knowledge_object_id, object_version,
			app_id, owner_id, object_type, name, sharing_scope, state,
			description_present, description,
			index_selector_count, host_selector_count,
			source_selector_count, sourcetype_selector_count,
			selector_value_bytes, canonical_selector_bytes
		) VALUES (
			'tenant-a', 'ko-long-disabled', ?, ?, 'owner-a',
			'field_extraction', 'ko-long-disabled', 'private', 'disabled',
			0, '', 0, 0, 0, 0, 0, 46
		);
		INSERT INTO knowledge_object_list_projection_seals (
			tenant_id, knowledge_object_id, object_version,
			projection_bytes, canonical_selector_bytes
		) SELECT tenant_id, knowledge_object_id, object_version,
		         projection_bytes, canonical_selector_bytes
		    FROM knowledge_object_list_projections
		   WHERE tenant_id = 'tenant-a'
		     AND knowledge_object_id = 'ko-long-disabled'
		     AND object_version = ?`,
		versionCount, knowledgeMigrationTestAppID, versionCount,
	); err != nil {
		rollback("stage long-history projection: %v", err)
	}
	blobInsert, err := tx.Prepare(`
		INSERT INTO knowledge_definition_blobs (
			tenant_id, definition_digest, definition_proto,
			definition_bytes, created_at_unix_micro
		) VALUES ('tenant-a', ?, ?, ?, ?)`)
	if err != nil {
		rollback("prepare long-history definition insert: %v", err)
	}
	versionInsert, err := tx.Prepare(`
		INSERT INTO knowledge_object_versions (
			tenant_id, knowledge_object_id, object_version,
			app_id, owner_id, object_type, name, sharing_scope, state,
			definition_digest, dependency_count, mutation_kind,
			quarantine_reason, created_at_unix_micro
		) VALUES (
			'tenant-a', 'ko-long-disabled', ?, ?, 'owner-a',
			'field_extraction', 'ko-long-disabled', 'private', ?, ?, 0, ?, NULL, ?
		)`)
	if err != nil {
		_ = blobInsert.Close()
		rollback("prepare long-history version insert: %v", err)
	}
	previousDigest := make([]byte, sha256.Size)
	for objectVersion := 1; objectVersion <= versionCount; objectVersion++ {
		state := "disabled"
		mutation := "update"
		switch objectVersion {
		case 1:
			state = "active"
			mutation = "create"
		case 2:
			mutation = "disable"
		case enableVersion:
			state = "active"
			mutation = "enable"
		case secondDisableVersion:
			mutation = "disable"
		}

		digest := previousDigest
		if mutation == "update" {
			var body [8]byte
			binary.BigEndian.PutUint64(body[:], uint64(objectVersion))
			hash := sha256.Sum256(body[:])
			if _, err := blobInsert.Exec(hash[:], body[:], len(body), objectVersion); err != nil {
				_ = versionInsert.Close()
				_ = blobInsert.Close()
				rollback("stage long-history definition %d: %v", objectVersion, err)
			}
			digest = hash[:]
		}
		if _, err := versionInsert.Exec(
			objectVersion, knowledgeMigrationTestAppID, state, digest, mutation, objectVersion,
		); err != nil {
			_ = versionInsert.Close()
			_ = blobInsert.Close()
			rollback("stage long immutable history version %d: %v", objectVersion, err)
		}
		previousDigest = append(previousDigest[:0], digest...)
	}
	if err := versionInsert.Close(); err != nil {
		_ = blobInsert.Close()
		rollback("close long-history version insert: %v", err)
	}
	if err := blobInsert.Close(); err != nil {
		rollback("close long-history definition insert: %v", err)
	}
	if _, err := tx.Exec(`
		INSERT INTO knowledge_object_dependency_seals (
			tenant_id, knowledge_object_id, object_version, dependency_count
		)
		SELECT tenant_id, knowledge_object_id, object_version, 0
		FROM knowledge_object_versions
		WHERE tenant_id = 'tenant-a'
		  AND knowledge_object_id = 'ko-long-disabled'
		ORDER BY object_version;

		INSERT INTO knowledge_objects (
			tenant_id, knowledge_object_id, current_version,
			app_id, owner_id, object_type, name, sharing_scope, state,
			definition_digest, created_at_unix_micro, updated_at_unix_micro,
			disabled_at_unix_micro
		) VALUES (
			'tenant-a', 'ko-long-disabled', ?, ?, 'owner-a',
			'field_extraction', 'ko-long-disabled', 'private', 'disabled',
			?, 1, ?, ?
		)`,
		versionCount, knowledgeMigrationTestAppID,
		previousDigest, versionCount, secondDisableVersion,
	); err != nil {
		rollback("stage long-history registry and seals: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit long disabled history: %v", err)
	}
	assertIntegerQuery(t, raw, versionCount, `
		SELECT version_count FROM knowledge_catalog_tenants
		WHERE tenant_id = 'tenant-a'`)
	assertIntegerQuery(t, raw, updateVersionCount+1, `
		SELECT count(*) FROM knowledge_definition_blobs
		WHERE tenant_id = 'tenant-a'`)
	assertIntegerQuery(t, raw, 1+8*updateVersionCount, `
		SELECT definition_body_bytes FROM knowledge_catalog_tenants
		WHERE tenant_id = 'tenant-a'`)

	migrationContext, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	migrationStarted := time.Now()
	if err := ApplyMigrations(migrationContext, raw, migrations.SQLite()); err != nil {
		if migrationContext.Err() != nil {
			t.Fatalf("migration 0029 did not make bounded progress through %d versions: %v",
				versionCount, migrationContext.Err())
		}
		t.Fatalf("apply migration 0029 to long valid history: %v", err)
	}
	t.Logf("migration 0029 backfilled %d versions in %s", versionCount, time.Since(migrationStarted))
	assertIntegerQuery(t, raw, 36, `SELECT count(*) FROM schema_migrations`)
	assertIntegerQuery(t, raw, versionCount, `
		SELECT count(*) FROM knowledge_object_version_lifecycle
		WHERE tenant_id = 'tenant-a' AND knowledge_object_id = 'ko-long-disabled'`)
	assertIntegerQuery(t, raw, enableVersion-2, `
		SELECT count(*) FROM knowledge_object_version_lifecycle
		WHERE tenant_id = 'tenant-a' AND knowledge_object_id = 'ko-long-disabled'
		  AND object_version BETWEEN 2 AND ?
		  AND state = 'disabled' AND disabled_at_unix_micro = 2`, enableVersion-1)
	assertIntegerQuery(t, raw, versionCount-secondDisableVersion+1, `
		SELECT count(*) FROM knowledge_object_version_lifecycle
		WHERE tenant_id = 'tenant-a' AND knowledge_object_id = 'ko-long-disabled'
		  AND object_version BETWEEN ? AND ?
		  AND state = 'disabled' AND disabled_at_unix_micro = ?`,
		secondDisableVersion, versionCount, secondDisableVersion)
	assertIntegerQuery(t, raw, 2, `
		SELECT count(*) FROM knowledge_object_version_lifecycle
		WHERE tenant_id = 'tenant-a' AND knowledge_object_id = 'ko-long-disabled'
		  AND state = 'active' AND disabled_at_unix_micro IS NULL`)
	assertKnowledgeLifecycleBackfillPlanIsLinear(t, raw)
	assertNoForeignKeyViolations(t, raw)
}

func assertKnowledgeLifecycleBackfillPlanIsLinear(t *testing.T, db *sql.DB) {
	t.Helper()
	contents, err := fs.ReadFile(
		migrations.SQLite(),
		"0029_knowledge_catalog_state_and_ordering.sql",
	)
	if err != nil {
		t.Fatalf("read embedded migration 0029: %v", err)
	}
	migrationSQL := string(contents)
	start := strings.Index(migrationSQL, "WITH lifecycle_source AS (")
	const terminator = "FROM lifecycle_source;"
	if start < 0 {
		t.Fatal("migration 0029 lifecycle backfill is not sourced from a windowed history")
	}
	endOffset := strings.Index(migrationSQL[start:], terminator)
	if endOffset < 0 {
		t.Fatal("migration 0029 lifecycle backfill statement is incomplete")
	}
	statement := migrationSQL[start : start+endOffset+len(terminator)]
	rows, err := db.Query(`EXPLAIN QUERY PLAN ` + statement)
	if err != nil {
		t.Fatalf("explain migration 0029 lifecycle backfill: %v", err)
	}
	defer rows.Close()
	var plan []string
	for rows.Next() {
		var id, parent, unused int
		var detail string
		if err := rows.Scan(&id, &parent, &unused, &detail); err != nil {
			t.Fatalf("scan migration 0029 lifecycle backfill plan: %v", err)
		}
		plan = append(plan, detail)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate migration 0029 lifecycle backfill plan: %v", err)
	}
	joined := strings.ToUpper(strings.Join(plan, "\n"))
	if strings.Contains(joined, "CORRELATED") {
		t.Fatalf("migration 0029 lifecycle backfill contains per-version correlated work:\n%s", joined)
	}
}

func TestKnowledgeCatalogStateMigrationRejectsCurrentVersionTupleDrift(t *testing.T) {
	t.Parallel()

	alternateDigest := bytes.Repeat([]byte{1}, 32)
	tests := []struct {
		name       string
		assignment string
		value      any
	}{
		{name: "app", assignment: "app_id = ?", value: "app_BBBBBBBBBBBBBBBBBBBBBA"},
		{name: "owner", assignment: "owner_id = ?", value: "owner-b"},
		{name: "object type", assignment: "object_type = ?", value: "field_alias"},
		{name: "name", assignment: "name = ?", value: "ko-current-tuple-drifted"},
		{name: "sharing scope", assignment: "sharing_scope = ?", value: "app"},
		{name: "state", assignment: "state = ?", value: "draft"},
		{name: "definition digest", assignment: "definition_digest = ?", value: alternateDigest},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			raw := openKnowledgeMigrationTestDB(t, "knowledge-state-current-tuple-drift.sqlite")
			if err := ApplyMigrations(context.Background(), raw, migrationsBefore(t, "0029_")); err != nil {
				t.Fatalf("apply through migration 0028: %v", err)
			}
			seedKnowledgeStatePrerequisites(t, raw)
			if _, err := raw.Exec(`
				INSERT INTO app_workspaces (
					app_id, tenant_id, version, slug, display_name, description,
					default_time_range_present, state,
					created_at_unix_micro, updated_at_unix_micro
				) VALUES (?, 'tenant-a', 1, 'knowledge-migration-alternate',
				          'Knowledge migration alternate', '', 0, 'active', 2, 2)`,
				"app_BBBBBBBBBBBBBBBBBBBBBA",
			); err != nil {
				t.Fatalf("seed alternate app authority: %v", err)
			}
			if _, err := raw.Exec(`
				INSERT INTO knowledge_definition_blobs (
					tenant_id, definition_digest, definition_proto,
					definition_bytes, created_at_unix_micro
				) VALUES ('tenant-a', ?, X'02', 1, 2)`, alternateDigest,
			); err != nil {
				t.Fatalf("seed alternate definition authority: %v", err)
			}
			insertKnowledgeStateVersion(t, raw, stateVersionFixture{
				objectID: "ko-current-tuple", version: 1,
				state: "active", mutation: "create", timestamp: 10,
			})

			raw.SetMaxOpenConns(1)
			raw.SetMaxIdleConns(1)
			conn, err := raw.Conn(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			if _, err := conn.ExecContext(context.Background(), `PRAGMA foreign_keys = OFF`); err != nil {
				_ = conn.Close()
				t.Fatalf("disable foreign keys for legacy corruption fixture: %v", err)
			}
			if _, err := conn.ExecContext(context.Background(),
				`DROP TRIGGER knowledge_object_version_update_is_forbidden`); err != nil {
				_ = conn.Close()
				t.Fatalf("drop legacy immutability trigger for corruption fixture: %v", err)
			}
			if _, err := conn.ExecContext(context.Background(), `
				UPDATE knowledge_object_versions SET `+test.assignment+`
				WHERE tenant_id = 'tenant-a'
				  AND knowledge_object_id = 'ko-current-tuple'
				  AND object_version = 1`, test.value); err != nil {
				_ = conn.Close()
				t.Fatalf("inject %s current-version tuple drift: %v", test.name, err)
			}
			if _, err := conn.ExecContext(context.Background(), `
				CREATE TRIGGER knowledge_object_version_update_is_forbidden
				BEFORE UPDATE ON knowledge_object_versions
				BEGIN
					SELECT RAISE(ABORT, 'knowledge object version is immutable');
				END`); err != nil {
				_ = conn.Close()
				t.Fatalf("restore legacy immutability trigger after corruption fixture: %v", err)
			}
			if _, err := conn.ExecContext(context.Background(), `PRAGMA foreign_keys = ON`); err != nil {
				_ = conn.Close()
				t.Fatalf("restore foreign keys after legacy corruption fixture: %v", err)
			}
			if err := conn.Close(); err != nil {
				t.Fatal(err)
			}
			assertIntegerQuery(t, raw, 1, `
				SELECT count(*)
				FROM knowledge_objects AS object
				JOIN knowledge_object_versions AS version
				  ON version.tenant_id = object.tenant_id
				 AND version.knowledge_object_id = object.knowledge_object_id
				 AND version.object_version = object.current_version
				WHERE object.tenant_id = 'tenant-a'
				  AND object.knowledge_object_id = 'ko-current-tuple'
				  AND (
				      version.app_id <> object.app_id
				      OR version.owner_id <> object.owner_id
				      OR version.object_type <> object.object_type
				      OR version.name <> object.name
				      OR version.sharing_scope <> object.sharing_scope
				      OR version.state <> object.state
				      OR version.definition_digest IS NOT object.definition_digest
				  )`)

			migrationContext, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			err = ApplyMigrations(migrationContext, raw, migrations.SQLite())
			if migrationContext.Err() != nil {
				t.Fatalf("exact current-version tuple preflight did not make bounded progress: %v",
					migrationContext.Err())
			}
			if err == nil || !strings.Contains(err.Error(), "CHECK constraint failed: invalid") {
				t.Fatalf("%s current-version tuple drift upgrade error = %v", test.name, err)
			}
			assertKnowledgeStateUpgradeRolledBack(t, raw)
		})
	}
}

func TestKnowledgeCatalogStateAuthoritiesSurviveBackupRestoreExactly(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	source, err := Open(ctx, filepath.Join(t.TempDir(), "knowledge-state-source.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = source.Close() })
	seedKnowledgeStatePrerequisites(t, source.SQLDB())
	for _, fixture := range []stateVersionFixture{
		{objectID: "ko-restore", version: 1, state: "active", mutation: "create", timestamp: 10},
		{objectID: "ko-restore", version: 2, state: "disabled", mutation: "disable", timestamp: 20,
			disabledAt: sql.NullInt64{Int64: 20, Valid: true}},
		{objectID: "ko-restore", version: 3, state: "disabled", mutation: "scope_change", timestamp: 30,
			disabledAt: sql.NullInt64{Int64: 20, Valid: true}, sharingScope: "app"},
	} {
		insertKnowledgeStateVersion(t, source.SQLDB(), fixture)
	}
	if _, err := source.SQLDB().ExecContext(ctx, `
		UPDATE knowledge_catalog_tenants
		SET catalog_revision = catalog_revision + 1
		WHERE tenant_id = 'tenant-a'`); err != nil {
		t.Fatalf("advance source revision: %v", err)
	}
	sourceToken := readKnowledgeRevisionToken(t, source.SQLDB(), "tenant-a", 1)

	restoredPath := filepath.Join(t.TempDir(), "knowledge-state-restored.sqlite")
	if err := source.BackupTo(ctx, restoredPath); err != nil {
		t.Fatalf("backup state authorities: %v", err)
	}
	restored, err := Open(ctx, restoredPath)
	if err != nil {
		t.Fatalf("open restored catalog: %v", err)
	}
	t.Cleanup(func() { _ = restored.Close() })
	if err := restored.VerifyIntegrity(ctx); err != nil {
		t.Fatalf("verify restored catalog: %v", err)
	}
	if got := readKnowledgeRevisionToken(t, restored.SQLDB(), "tenant-a", 1); !bytes.Equal(got, sourceToken) {
		t.Fatal("restored catalog changed its exact revision state token")
	}
	assertIntegerQuery(t, restored.SQLDB(), 2, `
		SELECT count(*) FROM knowledge_object_version_lifecycle
		WHERE tenant_id = 'tenant-a' AND knowledge_object_id = 'ko-restore'
		  AND object_version IN (2, 3) AND state = 'disabled'
		  AND disabled_at_unix_micro = 20
		  AND quarantined_at_unix_micro IS NULL
		  AND deleted_at_unix_micro IS NULL
		  AND quarantine_reason IS NULL`)
	assertIntegerQuery(t, restored.SQLDB(), 1, `
		SELECT count(*) FROM knowledge_object_list_order_keys
		WHERE tenant_id = 'tenant-a' AND knowledge_object_id = 'ko-restore'
		  AND object_version = 3
		  AND created_at_unix_micro = 10
		  AND updated_at_unix_micro = 30`)
	assertSQLFailsContaining(t, restored.SQLDB(), "immutable", `
		UPDATE knowledge_object_version_lifecycle
		SET disabled_at_unix_micro = 30
		WHERE tenant_id = 'tenant-a' AND knowledge_object_id = 'ko-restore'
		  AND object_version = 3`)
	assertSQLFailsContaining(t, restored.SQLDB(), "immutable", `
		UPDATE knowledge_object_list_order_keys
		SET updated_at_unix_micro = 29
		WHERE tenant_id = 'tenant-a' AND knowledge_object_id = 'ko-restore'
		  AND object_version = 3`)
	if _, err := restored.SQLDB().ExecContext(ctx, `
		UPDATE knowledge_catalog_tenants
		SET catalog_revision = catalog_revision + 1
		WHERE tenant_id = 'tenant-a'`); err != nil {
		t.Fatalf("advance restored revision: %v", err)
	}
	if got := readKnowledgeRevisionToken(t, restored.SQLDB(), "tenant-a", 2); bytes.Equal(got, sourceToken) {
		t.Fatal("restored catalog did not rotate its state token on a new revision")
	}
	assertNoForeignKeyViolations(t, restored.SQLDB())
}

func TestKnowledgeCatalogOrderKeySupportsPermittedPublicationOrders(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name            string
		projectionFirst bool
	}{
		{name: "version before projection"},
		{name: "deferred projection before version", projectionFirst: true},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			raw := openKnowledgeMigrationTestDB(t, "knowledge-order-publication.sqlite")
			if err := ApplyMigrations(context.Background(), raw, migrations.SQLite()); err != nil {
				t.Fatal(err)
			}
			seedProjectionPrerequisites(t, raw)
			fixture := projectionFixture{ObjectID: "ko-publication", Version: 1, Name: "Publication"}
			tx, err := raw.Begin()
			if err != nil {
				t.Fatal(err)
			}
			if test.projectionFirst {
				insertProjectionParent(t, tx, fixture)
				insertKnowledgeVersion(t, tx, fixture.ObjectID, fixture.Version, fixture.Name, "create", 10)
				sealKnowledgeStateProjection(t, tx, fixture.ObjectID, fixture.Version)
			} else {
				insertKnowledgeVersion(t, tx, fixture.ObjectID, fixture.Version, fixture.Name, "create", 10)
				insertProjectionRows(t, tx, fixture)
			}
			if _, err := tx.Exec(`
				INSERT INTO knowledge_objects (
					tenant_id, knowledge_object_id, current_version,
					app_id, owner_id, object_type, name, sharing_scope, state,
					definition_digest, created_at_unix_micro, updated_at_unix_micro
				) VALUES (
					'tenant-a', 'ko-publication', 1, ?, 'owner-a',
					'field_extraction', 'Publication', 'private', 'active',
					zeroblob(32), 10, 10
				)`, knowledgeMigrationTestAppID); err != nil {
				_ = tx.Rollback()
				t.Fatalf("insert current registry: %v", err)
			}
			if err := tx.Commit(); err != nil {
				t.Fatalf("commit publication: %v", err)
			}
			assertIntegerQuery(t, raw, 1, `
				SELECT count(*) FROM knowledge_object_list_order_keys
				WHERE tenant_id = 'tenant-a'
				  AND knowledge_object_id = 'ko-publication'
				  AND object_version = 1
				  AND created_at_unix_micro = 10
				  AND updated_at_unix_micro = 10`)
			assertIntegerQuery(t, raw, 1, `
				SELECT count(*) FROM knowledge_object_version_lifecycle
				WHERE tenant_id = 'tenant-a'
				  AND knowledge_object_id = 'ko-publication'
				  AND object_version = 1 AND state = 'active'`)
			assertNoForeignKeyViolations(t, raw)
		})
	}
}

func TestKnowledgeCatalogOrderKeySupportsDeferredUpdatePublication(t *testing.T) {
	t.Parallel()

	raw := openKnowledgeMigrationTestDB(t, "knowledge-order-deferred-update.sqlite")
	if err := ApplyMigrations(context.Background(), raw, migrations.SQLite()); err != nil {
		t.Fatal(err)
	}
	seedProjectionPrerequisites(t, raw)
	insertProjectedKnowledgeObject(t, raw, projectionFixture{
		ObjectID: "ko-deferred-update", Version: 1, Name: "Deferred Update",
	}, 10)

	tx, err := raw.Begin()
	if err != nil {
		t.Fatal(err)
	}
	fixture := projectionFixture{ObjectID: "ko-deferred-update", Version: 2, Name: "Deferred Update"}
	definitionBody := []byte("knowledge-order-fixture/ko-deferred-update/2")
	definitionDigest := sha256.Sum256(definitionBody)
	if _, err := tx.Exec(`
		INSERT INTO knowledge_definition_blobs (
			tenant_id, definition_digest, definition_proto,
			definition_bytes, created_at_unix_micro
		) VALUES ('tenant-a', ?, ?, ?, 20)`,
		definitionDigest[:], definitionBody, len(definitionBody),
	); err != nil {
		_ = tx.Rollback()
		t.Fatalf("insert deferred update definition: %v", err)
	}
	insertProjectionParent(t, tx, fixture)
	insertKnowledgeVersionWithDigest(
		t,
		tx,
		fixture.ObjectID,
		fixture.Version,
		fixture.Name,
		"update",
		20,
		definitionDigest[:],
	)
	sealKnowledgeStateProjection(t, tx, fixture.ObjectID, fixture.Version)
	if _, err := tx.Exec(`
		UPDATE knowledge_objects
		SET current_version = 2, definition_digest = ?, updated_at_unix_micro = 20
		WHERE tenant_id = 'tenant-a'
		  AND knowledge_object_id = 'ko-deferred-update'`, definitionDigest[:]); err != nil {
		_ = tx.Rollback()
		t.Fatalf("publish deferred update: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit deferred update: %v", err)
	}
	assertIntegerQuery(t, raw, 1, `
		SELECT count(*) FROM knowledge_object_list_order_keys
		WHERE tenant_id = 'tenant-a'
		  AND knowledge_object_id = 'ko-deferred-update'
		  AND object_version = 2
		  AND created_at_unix_micro = 10
		  AND updated_at_unix_micro = 20`)
	assertIntegerQuery(t, raw, 0, `
		SELECT count(*) FROM knowledge_object_list_order_keys
		WHERE tenant_id = 'tenant-a'
		  AND knowledge_object_id = 'ko-deferred-update'
		  AND object_version = 1`)
	assertIntegerQuery(t, raw, 2, `
		SELECT count(*) FROM knowledge_object_version_lifecycle
		WHERE tenant_id = 'tenant-a'
		  AND knowledge_object_id = 'ko-deferred-update'`)
	assertNoForeignKeyViolations(t, raw)
}

func sealKnowledgeStateProjection(t *testing.T, exec projectionExecer, objectID string, version int) {
	t.Helper()
	if _, err := exec.Exec(`
		INSERT INTO knowledge_object_list_projection_seals (
			tenant_id, knowledge_object_id, object_version,
			projection_bytes, canonical_selector_bytes
		) SELECT tenant_id, knowledge_object_id, object_version,
		         projection_bytes, canonical_selector_bytes
		    FROM knowledge_object_list_projections
		   WHERE tenant_id = 'tenant-a' AND knowledge_object_id = ?
		     AND object_version = ?`, objectID, version); err != nil {
		t.Fatalf("seal projection %s/%d: %v", objectID, version, err)
	}
}

func stageKnowledgeStateHistory(
	t *testing.T,
	db *sql.DB,
	fixtures []stateVersionFixture,
	registryCreatedAt int64,
	registryUpdatedAt int64,
	registryReason string,
) {
	t.Helper()
	if len(fixtures) == 0 {
		t.Fatal("stage knowledge state history requires at least one version")
	}
	objectID := fixtures[0].objectID
	for index, fixture := range fixtures {
		if fixture.objectID != objectID || fixture.version != index+1 {
			t.Fatalf("invalid staged fixture identity/version at index %d: %+v", index, fixture)
		}
	}
	current := fixtures[len(fixtures)-1]
	if registryCreatedAt == 0 {
		registryCreatedAt = fixtures[0].timestamp
	}
	if registryUpdatedAt == 0 {
		registryUpdatedAt = current.timestamp
	}
	if registryReason == "" {
		registryReason = current.quarantineReason
	}

	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	rollback := func(format string, args ...any) {
		_ = tx.Rollback()
		t.Fatalf(format, args...)
	}
	var currentDigest any
	for _, fixture := range fixtures {
		digest := knowledgeStateFixtureDefinitionDigest(t, tx, fixture)
		currentDigest = digest
		reason := any(nil)
		if fixture.quarantineReason != "" {
			reason = fixture.quarantineReason
		}
		sharingScope := fixture.sharingScope
		if sharingScope == "" {
			sharingScope = "private"
		}
		if _, err := tx.Exec(`
			INSERT INTO knowledge_object_versions (
				tenant_id, knowledge_object_id, object_version,
				app_id, owner_id, object_type, name, sharing_scope, state,
				definition_digest, dependency_count, mutation_kind,
				quarantine_reason, created_at_unix_micro
			) VALUES (
				'tenant-a', ?, ?, ?, 'owner-a', 'field_extraction', ?,
				?, ?, ?, 0, ?, ?, ?
			);
			INSERT INTO knowledge_object_dependency_seals (
				tenant_id, knowledge_object_id, object_version, dependency_count
			) VALUES ('tenant-a', ?, ?, 0)`,
			fixture.objectID, fixture.version, knowledgeMigrationTestAppID, fixture.objectID, sharingScope,
			fixture.state, digest, fixture.mutation, reason, fixture.timestamp,
			fixture.objectID, fixture.version,
		); err != nil {
			rollback("stage version %s/%d: %v", fixture.objectID, fixture.version, err)
		}
	}

	canonicalSelectorBytes := 46
	if current.state == "quarantined" {
		canonicalSelectorBytes = 0
	}
	currentSharingScope := current.sharingScope
	if currentSharingScope == "" {
		currentSharingScope = "private"
	}
	if _, err := tx.Exec(`
		INSERT INTO knowledge_object_list_projections (
			tenant_id, knowledge_object_id, object_version,
			app_id, owner_id, object_type, name, sharing_scope, state,
			description_present, description,
			index_selector_count, host_selector_count,
			source_selector_count, sourcetype_selector_count,
			selector_value_bytes, canonical_selector_bytes
		) VALUES (
			'tenant-a', ?, ?, ?, 'owner-a', 'field_extraction', ?,
			?, ?, 0, '', 0, 0, 0, 0, 0, ?
		);
		INSERT INTO knowledge_object_list_projection_seals (
			tenant_id, knowledge_object_id, object_version,
			projection_bytes, canonical_selector_bytes
		) SELECT tenant_id, knowledge_object_id, object_version,
		         projection_bytes, canonical_selector_bytes
		    FROM knowledge_object_list_projections
		   WHERE tenant_id = 'tenant-a' AND knowledge_object_id = ?
		     AND object_version = ?`,
		current.objectID, current.version, knowledgeMigrationTestAppID, current.objectID, currentSharingScope,
		current.state, canonicalSelectorBytes, current.objectID, current.version,
	); err != nil {
		rollback("stage current projection %s/%d: %v", current.objectID, current.version, err)
	}

	reason := any(nil)
	if registryReason != "" {
		reason = registryReason
	}
	if _, err := tx.Exec(`
		INSERT INTO knowledge_objects (
			tenant_id, knowledge_object_id, current_version,
			app_id, owner_id, object_type, name, sharing_scope, state,
			definition_digest, created_at_unix_micro, updated_at_unix_micro,
			disabled_at_unix_micro, quarantined_at_unix_micro,
			deleted_at_unix_micro, quarantine_reason
		) VALUES (
			'tenant-a', ?, ?, ?, 'owner-a', 'field_extraction', ?,
			?, ?, ?, ?, ?, ?, ?, ?, ?
		)`, current.objectID, current.version, knowledgeMigrationTestAppID, current.objectID,
		currentSharingScope, current.state, currentDigest, registryCreatedAt, registryUpdatedAt,
		nullInt64Value(current.disabledAt), nullInt64Value(current.quarantinedAt),
		nullInt64Value(current.deletedAt), reason,
	); err != nil {
		rollback("stage current registry %s/%d: %v", current.objectID, current.version, err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit staged history %s: %v", objectID, err)
	}
}

func assertKnowledgeStateVersionInsertRejected(
	t *testing.T,
	db *sql.DB,
	fixture stateVersionFixture,
) {
	t.Helper()
	digest := any(make([]byte, 32))
	if fixture.state == "quarantined" {
		digest = nil
	}
	reason := any(nil)
	if fixture.quarantineReason != "" {
		reason = fixture.quarantineReason
	}
	if _, err := db.Exec(`
		INSERT INTO knowledge_object_versions (
			tenant_id, knowledge_object_id, object_version,
			app_id, owner_id, object_type, name, sharing_scope, state,
			definition_digest, dependency_count, mutation_kind,
			quarantine_reason, created_at_unix_micro
		) VALUES (
			'tenant-a', ?, ?, ?, 'owner-a', 'field_extraction', ?,
			'private', ?, ?, 0, ?, ?, ?
		)`, fixture.objectID, fixture.version, knowledgeMigrationTestAppID, fixture.objectID,
		fixture.state, digest, fixture.mutation, reason, fixture.timestamp,
	); err == nil {
		t.Fatal("immutable version insertion accepted a transition rejected by the runtime chronology contract")
	} else if !strings.Contains(err.Error(), "knowledge object version transition is invalid") {
		t.Fatalf("invalid immutable version was rejected by the wrong authority: %v", err)
	}
}

func assertKnowledgeStateUpgradeRolledBack(t *testing.T, db *sql.DB) {
	t.Helper()
	assertIntegerQuery(t, db, 28, `SELECT count(*) FROM schema_migrations`)
	assertIntegerQuery(t, db, 0, `
		SELECT count(*) FROM sqlite_schema
		WHERE name IN (
			'knowledge_catalog_revision_heads',
			'knowledge_object_version_lifecycle',
			'knowledge_object_list_order_keys'
		)`)
}

// Every history accepted by the 0029 backfill must also satisfy the chronology
// contract enforced by the catalog reader. Otherwise a successful upgrade can
// turn a previously openable catalog into one whose objects are all reported as
// corrupt, with no rollback path after the migration ledger is committed.
func TestKnowledgeCatalogStateMigrationRejectsUnreadableLegacyChronologies(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		fixtures []stateVersionFixture
	}{
		{
			name: "disable while already disabled",
			fixtures: []stateVersionFixture{
				{objectID: "ko-repeat-disable", version: 1, state: "active", mutation: "create", timestamp: 10},
				{objectID: "ko-repeat-disable", version: 2, state: "disabled", mutation: "disable", timestamp: 20,
					disabledAt: sql.NullInt64{Int64: 20, Valid: true}},
				{objectID: "ko-repeat-disable", version: 3, state: "disabled", mutation: "disable", timestamp: 30,
					disabledAt: sql.NullInt64{Int64: 30, Valid: true}},
			},
		},
		{
			name: "enable while already active",
			fixtures: []stateVersionFixture{
				{objectID: "ko-repeat-enable", version: 1, state: "active", mutation: "create", timestamp: 10},
				{objectID: "ko-repeat-enable", version: 2, state: "active", mutation: "enable", timestamp: 20},
			},
		},
		{
			name: "update changes state",
			fixtures: []stateVersionFixture{
				{objectID: "ko-update-state", version: 1, state: "active", mutation: "create", timestamp: 10},
				{objectID: "ko-update-state", version: 2, state: "draft", mutation: "update", timestamp: 20},
			},
		},
		{
			name: "scope change changes state",
			fixtures: []stateVersionFixture{
				{objectID: "ko-scope-state", version: 1, state: "active", mutation: "create", timestamp: 10},
				{objectID: "ko-scope-state", version: 2, state: "draft", mutation: "scope_change", timestamp: 20},
			},
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			raw := openKnowledgeMigrationTestDB(t, "knowledge-state-unreadable-upgrade.sqlite")
			if err := ApplyMigrations(context.Background(), raw, migrationsBefore(t, "0029_")); err != nil {
				t.Fatalf("apply through migration 0028: %v", err)
			}
			seedKnowledgeStatePrerequisites(t, raw)
			for _, fixture := range test.fixtures {
				insertKnowledgeStateVersion(t, raw, fixture)
			}

			if err := ApplyMigrations(context.Background(), raw, migrations.SQLite()); err == nil {
				t.Error("migration 0029 accepted a legacy chronology rejected by the runtime reader")
				assertIntegerQuery(t, raw, 29, `SELECT count(*) FROM schema_migrations`)
				assertIntegerQuery(t, raw, len(test.fixtures), `
					SELECT count(*) FROM knowledge_object_version_lifecycle
					WHERE tenant_id = 'tenant-a'`)
				return
			}
			assertKnowledgeStateUpgradeRolledBack(t, raw)
			assertIntegerQuery(t, raw, len(test.fixtures), `
				SELECT count(*) FROM knowledge_object_versions
				WHERE tenant_id = 'tenant-a'`)
			assertNoForeignKeyViolations(t, raw)
		})
	}
}

func TestKnowledgeCatalogStateMigrationRejectsDeferredMalformedHistories(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name              string
		fixtures          []stateVersionFixture
		registryCreatedAt int64
		registryUpdatedAt int64
		registryReason    string
	}{
		{
			name: "intermediate timestamp regression",
			fixtures: []stateVersionFixture{
				{objectID: "ko-time-regression", version: 1, state: "active", mutation: "create", timestamp: 10},
				{objectID: "ko-time-regression", version: 2, state: "active", mutation: "update", timestamp: 30},
				{objectID: "ko-time-regression", version: 3, state: "active", mutation: "update", timestamp: 20},
				{objectID: "ko-time-regression", version: 4, state: "active", mutation: "update", timestamp: 40},
			},
		},
		{
			name: "version follows terminal delete",
			fixtures: []stateVersionFixture{
				{objectID: "ko-after-delete", version: 1, state: "active", mutation: "create", timestamp: 10},
				{objectID: "ko-after-delete", version: 2, state: "deleted", mutation: "delete", timestamp: 20},
				{objectID: "ko-after-delete", version: 3, state: "active", mutation: "update", timestamp: 30},
			},
		},
		{
			name: "registry creation timestamp differs from version one",
			fixtures: []stateVersionFixture{
				{objectID: "ko-created-drift", version: 1, state: "active", mutation: "create", timestamp: 10},
				{objectID: "ko-created-drift", version: 2, state: "active", mutation: "update", timestamp: 20},
			},
			registryCreatedAt: 11,
		},
		{
			name: "registry update timestamp differs from current version",
			fixtures: []stateVersionFixture{
				{objectID: "ko-updated-drift", version: 1, state: "active", mutation: "create", timestamp: 10},
				{objectID: "ko-updated-drift", version: 2, state: "active", mutation: "update", timestamp: 20},
			},
			registryUpdatedAt: 21,
		},
		{
			name: "registry quarantine reason differs from terminal version",
			fixtures: []stateVersionFixture{
				{objectID: "ko-reason-drift", version: 1, state: "active", mutation: "create", timestamp: 10},
				{objectID: "ko-reason-drift", version: 2, state: "quarantined", mutation: "quarantine", timestamp: 20,
					quarantinedAt: sql.NullInt64{Int64: 20, Valid: true}, quarantineReason: "root_corruption"},
			},
			registryReason: "dependency_recovery",
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			raw := openKnowledgeMigrationTestDB(t, "knowledge-state-deferred-malformed.sqlite")
			if err := ApplyMigrations(context.Background(), raw, migrationsBefore(t, "0029_")); err != nil {
				t.Fatalf("apply through migration 0028: %v", err)
			}
			seedKnowledgeStatePrerequisites(t, raw)
			stageKnowledgeStateHistory(
				t,
				raw,
				test.fixtures,
				test.registryCreatedAt,
				test.registryUpdatedAt,
				test.registryReason,
			)

			if err := ApplyMigrations(context.Background(), raw, migrations.SQLite()); err == nil {
				t.Error("migration 0029 accepted a deferred legacy history rejected by the runtime reader")
				assertIntegerQuery(t, raw, 29, `SELECT count(*) FROM schema_migrations`)
				return
			}
			assertKnowledgeStateUpgradeRolledBack(t, raw)
			assertIntegerQuery(t, raw, len(test.fixtures), `
				SELECT count(*) FROM knowledge_object_versions
				WHERE tenant_id = 'tenant-a'`)
			assertNoForeignKeyViolations(t, raw)
		})
	}
}

func TestKnowledgeObjectVersionTransitionTriggerRejectsInvalidFreshHistory(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		committed []stateVersionFixture
		attempted stateVersionFixture
	}{
		{
			name:      "version one is not create",
			attempted: stateVersionFixture{objectID: "ko-v1-update", version: 1, state: "active", mutation: "update", timestamp: 10},
		},
		{
			name:      "version one create is disabled",
			attempted: stateVersionFixture{objectID: "ko-v1-disabled", version: 1, state: "disabled", mutation: "create", timestamp: 10},
		},
		{
			name: "enable while active",
			committed: []stateVersionFixture{
				{objectID: "ko-enable-active", version: 1, state: "active", mutation: "create", timestamp: 10},
			},
			attempted: stateVersionFixture{objectID: "ko-enable-active", version: 2, state: "active", mutation: "enable", timestamp: 20},
		},
		{
			name: "update changes state",
			committed: []stateVersionFixture{
				{objectID: "ko-update-state-fresh", version: 1, state: "active", mutation: "create", timestamp: 10},
			},
			attempted: stateVersionFixture{objectID: "ko-update-state-fresh", version: 2, state: "draft", mutation: "update", timestamp: 20},
		},
		{
			name: "scope change changes state",
			committed: []stateVersionFixture{
				{objectID: "ko-scope-state-fresh", version: 1, state: "active", mutation: "create", timestamp: 10},
			},
			attempted: stateVersionFixture{objectID: "ko-scope-state-fresh", version: 2, state: "draft", mutation: "scope_change", timestamp: 20},
		},
		{
			name: "disable while disabled",
			committed: []stateVersionFixture{
				{objectID: "ko-disable-disabled", version: 1, state: "active", mutation: "create", timestamp: 10},
				{objectID: "ko-disable-disabled", version: 2, state: "disabled", mutation: "disable", timestamp: 20,
					disabledAt: sql.NullInt64{Int64: 20, Valid: true}},
			},
			attempted: stateVersionFixture{objectID: "ko-disable-disabled", version: 3, state: "disabled", mutation: "disable", timestamp: 30},
		},
		{
			name: "timestamp regresses",
			committed: []stateVersionFixture{
				{objectID: "ko-time-regression-fresh", version: 1, state: "active", mutation: "create", timestamp: 10},
			},
			attempted: stateVersionFixture{objectID: "ko-time-regression-fresh", version: 2, state: "active", mutation: "update", timestamp: 9},
		},
		{
			name: "version follows terminal delete",
			committed: []stateVersionFixture{
				{objectID: "ko-after-delete-fresh", version: 1, state: "active", mutation: "create", timestamp: 10},
				{objectID: "ko-after-delete-fresh", version: 2, state: "deleted", mutation: "delete", timestamp: 20,
					deletedAt: sql.NullInt64{Int64: 20, Valid: true}},
			},
			attempted: stateVersionFixture{objectID: "ko-after-delete-fresh", version: 3, state: "active", mutation: "update", timestamp: 30},
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			raw := openKnowledgeMigrationTestDB(t, "knowledge-state-fresh-invalid.sqlite")
			if err := ApplyMigrations(context.Background(), raw, migrations.SQLite()); err != nil {
				t.Fatal(err)
			}
			seedKnowledgeStatePrerequisites(t, raw)
			for _, fixture := range test.committed {
				insertKnowledgeStateVersion(t, raw, fixture)
			}
			assertKnowledgeStateVersionInsertRejected(t, raw, test.attempted)
			assertIntegerQuery(t, raw, len(test.committed), `
				SELECT count(*) FROM knowledge_object_versions
				WHERE tenant_id = 'tenant-a' AND knowledge_object_id = ?`,
				test.attempted.objectID,
			)
			assertIntegerQuery(t, raw, len(test.committed), `
				SELECT count(*) FROM knowledge_object_version_lifecycle
				WHERE tenant_id = 'tenant-a' AND knowledge_object_id = ?`,
				test.attempted.objectID,
			)
			assertNoForeignKeyViolations(t, raw)
		})
	}
}

func TestKnowledgeObjectVersionTransitionTriggerAcceptsValidFreshMatrix(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		fixtures []stateVersionFixture
	}{
		{
			name: "active disabled enable quarantine",
			fixtures: []stateVersionFixture{
				{objectID: "ko-valid-main", version: 1, state: "active", mutation: "create", timestamp: 10},
				{objectID: "ko-valid-main", version: 2, state: "active", mutation: "update", timestamp: 20},
				{objectID: "ko-valid-main", version: 3, state: "active", mutation: "scope_change", timestamp: 30, sharingScope: "app"},
				{objectID: "ko-valid-main", version: 4, state: "disabled", mutation: "disable", timestamp: 40,
					disabledAt: sql.NullInt64{Int64: 40, Valid: true}, sharingScope: "app"},
				{objectID: "ko-valid-main", version: 5, state: "disabled", mutation: "update", timestamp: 50,
					disabledAt: sql.NullInt64{Int64: 40, Valid: true}, sharingScope: "app"},
				{objectID: "ko-valid-main", version: 6, state: "disabled", mutation: "scope_change", timestamp: 60,
					disabledAt: sql.NullInt64{Int64: 40, Valid: true}},
				{objectID: "ko-valid-main", version: 7, state: "active", mutation: "enable", timestamp: 70},
				{objectID: "ko-valid-main", version: 8, state: "quarantined", mutation: "quarantine", timestamp: 80,
					quarantinedAt: sql.NullInt64{Int64: 80, Valid: true}, quarantineReason: "root_corruption"},
			},
		},
		{
			name: "draft update scope disable enable delete",
			fixtures: []stateVersionFixture{
				{objectID: "ko-valid-draft", version: 1, state: "draft", mutation: "create", timestamp: 10},
				{objectID: "ko-valid-draft", version: 2, state: "draft", mutation: "update", timestamp: 20},
				{objectID: "ko-valid-draft", version: 3, state: "draft", mutation: "scope_change", timestamp: 30, sharingScope: "app"},
				{objectID: "ko-valid-draft", version: 4, state: "disabled", mutation: "disable", timestamp: 40,
					disabledAt: sql.NullInt64{Int64: 40, Valid: true}, sharingScope: "app"},
				{objectID: "ko-valid-draft", version: 5, state: "active", mutation: "enable", timestamp: 50, sharingScope: "app"},
				{objectID: "ko-valid-draft", version: 6, state: "deleted", mutation: "delete", timestamp: 60,
					deletedAt: sql.NullInt64{Int64: 60, Valid: true}, sharingScope: "app"},
			},
		},
		{
			name: "draft enable",
			fixtures: []stateVersionFixture{
				{objectID: "ko-valid-draft-enable", version: 1, state: "draft", mutation: "create", timestamp: 10},
				{objectID: "ko-valid-draft-enable", version: 2, state: "active", mutation: "enable", timestamp: 20},
			},
		},
		{
			name: "draft quarantine",
			fixtures: []stateVersionFixture{
				{objectID: "ko-valid-draft-quarantine", version: 1, state: "draft", mutation: "create", timestamp: 10},
				{objectID: "ko-valid-draft-quarantine", version: 2, state: "quarantined", mutation: "quarantine", timestamp: 20,
					quarantinedAt: sql.NullInt64{Int64: 20, Valid: true}, quarantineReason: "dependency_recovery"},
			},
		},
		{
			name: "disabled delete",
			fixtures: []stateVersionFixture{
				{objectID: "ko-valid-disabled-delete", version: 1, state: "active", mutation: "create", timestamp: 10},
				{objectID: "ko-valid-disabled-delete", version: 2, state: "disabled", mutation: "disable", timestamp: 20,
					disabledAt: sql.NullInt64{Int64: 20, Valid: true}},
				{objectID: "ko-valid-disabled-delete", version: 3, state: "deleted", mutation: "delete", timestamp: 30,
					deletedAt: sql.NullInt64{Int64: 30, Valid: true}},
			},
		},
		{
			name: "disabled quarantine",
			fixtures: []stateVersionFixture{
				{objectID: "ko-valid-disabled-quarantine", version: 1, state: "active", mutation: "create", timestamp: 10},
				{objectID: "ko-valid-disabled-quarantine", version: 2, state: "disabled", mutation: "disable", timestamp: 20,
					disabledAt: sql.NullInt64{Int64: 20, Valid: true}},
				{objectID: "ko-valid-disabled-quarantine", version: 3, state: "quarantined", mutation: "quarantine", timestamp: 30,
					quarantinedAt: sql.NullInt64{Int64: 30, Valid: true}, quarantineReason: "root_corruption"},
			},
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			raw := openKnowledgeMigrationTestDB(t, "knowledge-state-fresh-valid.sqlite")
			if err := ApplyMigrations(context.Background(), raw, migrations.SQLite()); err != nil {
				t.Fatal(err)
			}
			seedKnowledgeStatePrerequisites(t, raw)
			for _, fixture := range test.fixtures {
				insertKnowledgeStateVersion(t, raw, fixture)
			}
			current := test.fixtures[len(test.fixtures)-1]
			assertIntegerQuery(t, raw, len(test.fixtures), `
				SELECT count(*) FROM knowledge_object_versions
				WHERE tenant_id = 'tenant-a' AND knowledge_object_id = ?`, current.objectID)
			assertIntegerQuery(t, raw, len(test.fixtures), `
				SELECT count(*) FROM knowledge_object_version_lifecycle
				WHERE tenant_id = 'tenant-a' AND knowledge_object_id = ?`, current.objectID)
			assertIntegerQuery(t, raw, 1, `
				SELECT count(*) FROM knowledge_objects
				WHERE tenant_id = 'tenant-a' AND knowledge_object_id = ?
				  AND current_version = ? AND state = ?`,
				current.objectID, current.version, current.state,
			)
			assertNoForeignKeyViolations(t, raw)
		})
	}
}

func TestKnowledgeCatalogStateMigrationRejectsCurrentLifecycleMarkerDrift(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		fixtures []stateVersionFixture
	}{
		{
			name: "disabled marker differs from latest disable",
			fixtures: []stateVersionFixture{
				{objectID: "ko-disabled-marker", version: 1, state: "active", mutation: "create", timestamp: 10},
				{objectID: "ko-disabled-marker", version: 2, state: "disabled", mutation: "disable", timestamp: 20,
					disabledAt: sql.NullInt64{Int64: 20, Valid: true}},
				{objectID: "ko-disabled-marker", version: 3, state: "disabled", mutation: "scope_change", timestamp: 30,
					disabledAt: sql.NullInt64{Int64: 30, Valid: true}},
			},
		},
		{
			name: "quarantine marker differs from terminal version",
			fixtures: []stateVersionFixture{
				{objectID: "ko-quarantine-marker", version: 1, state: "active", mutation: "create", timestamp: 10},
				{objectID: "ko-quarantine-marker", version: 2, state: "quarantined", mutation: "quarantine", timestamp: 20,
					quarantinedAt: sql.NullInt64{Int64: 15, Valid: true}, quarantineReason: "root_corruption"},
			},
		},
		{
			name: "delete marker differs from terminal version",
			fixtures: []stateVersionFixture{
				{objectID: "ko-delete-marker", version: 1, state: "active", mutation: "create", timestamp: 10},
				{objectID: "ko-delete-marker", version: 2, state: "deleted", mutation: "delete", timestamp: 20,
					deletedAt: sql.NullInt64{Int64: 15, Valid: true}},
			},
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			raw := openKnowledgeMigrationTestDB(t, "knowledge-state-marker-drift.sqlite")
			if err := ApplyMigrations(context.Background(), raw, migrationsBefore(t, "0029_")); err != nil {
				t.Fatalf("apply through migration 0028: %v", err)
			}
			seedKnowledgeStatePrerequisites(t, raw)
			for _, fixture := range test.fixtures {
				insertKnowledgeStateVersion(t, raw, fixture)
			}

			if err := ApplyMigrations(context.Background(), raw, migrations.SQLite()); err == nil {
				t.Error("migration 0029 accepted a current lifecycle marker that disagrees with immutable history")
				assertIntegerQuery(t, raw, 29, `SELECT count(*) FROM schema_migrations`)
				return
			}
			assertKnowledgeStateUpgradeRolledBack(t, raw)
			assertIntegerQuery(t, raw, len(test.fixtures), `
				SELECT count(*) FROM knowledge_object_versions
				WHERE tenant_id = 'tenant-a'`)
			assertNoForeignKeyViolations(t, raw)
		})
	}
}
