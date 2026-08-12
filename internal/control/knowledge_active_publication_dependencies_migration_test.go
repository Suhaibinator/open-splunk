package control

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"fmt"
	"io/fs"
	"strings"
	"testing"

	"github.com/Suhaibinator/open-splunk/migrations"
)

const knowledgeActivePublicationDependenciesMigration = "0033_knowledge_active_publication_dependencies.sql"
const knowledgeActivePublicationDependenciesMigrationSHA256 = "171c5b390d1033a48405ab5953131c312a2fed5ba09a22bd7b8a58f62cba9f7f"

func TestKnowledgeActivePublicationDependenciesMigrationPinsSchemaAndAccessPath(t *testing.T) {
	t.Parallel()

	raw := openKnowledgeMigrationTestDB(t, "knowledge-active-publication-schema.sqlite")
	if err := ApplyMigrations(context.Background(), raw, migrations.SQLite()); err != nil {
		t.Fatalf("apply migrations: %v", err)
	}

	contents, err := fs.ReadFile(migrations.SQLite(), knowledgeActivePublicationDependenciesMigration)
	if err != nil {
		t.Fatalf("read embedded migration: %v", err)
	}
	wantChecksum := sha256.Sum256(contents)
	if got := fmt.Sprintf("%x", wantChecksum); got != knowledgeActivePublicationDependenciesMigrationSHA256 {
		t.Fatalf("migration SHA-256 = %s, want %s", got, knowledgeActivePublicationDependenciesMigrationSHA256)
	}
	var version int
	var name string
	var checksum []byte
	if err := raw.QueryRowContext(t.Context(), `
		SELECT version, name, checksum
		FROM schema_migrations
		WHERE version = 33`).Scan(&version, &name, &checksum); err != nil {
		t.Fatalf("read migration authority: %v", err)
	}
	if version != 33 || name != knowledgeActivePublicationDependenciesMigration ||
		!bytes.Equal(checksum, wantChecksum[:]) {
		t.Fatalf("migration authority = (%d, %q, %x), want (33, %q, %x)",
			version, name, checksum, knowledgeActivePublicationDependenciesMigration, wantChecksum)
	}
	assertIntegerQuery(t, raw, 36, `SELECT count(*) FROM schema_migrations`)

	triggerSQL := knowledge0033SchemaSQL(
		t,
		raw,
		"trigger",
		"knowledge_active_dependency_target_version_advance_is_blocked",
	)
	for _, fragment := range []string{
		"BEFORE UPDATE OF current_version ON knowledge_objects",
		"OLD.state = 'active'",
		"NEW.state = 'active'",
		"NEW.current_version <> OLD.current_version",
		"INDEXED BY knowledge_objects_resolution_idx",
		"CROSS JOIN knowledge_object_dependencies AS dependency",
		"INDEXED BY knowledge_object_dependencies_source_target_idx",
		"dependent.tenant_id = OLD.tenant_id",
		"dependent.state = 'active'",
		"dependency.tenant_id = dependent.tenant_id",
		"dependency.source_object_id = dependent.knowledge_object_id",
		"dependency.source_object_version = dependent.current_version",
		"dependency.target_object_id = OLD.knowledge_object_id",
		"dependency.target_object_version <> NEW.current_version",
		"LIMIT 1",
	} {
		if !strings.Contains(triggerSQL, fragment) {
			t.Errorf("target-version trigger lacks %q", fragment)
		}
	}
	assertKnowledge0033IndexColumns(t, raw, "knowledge_objects_resolution_idx", []string{
		"tenant_id", "state", "sharing_scope", "app_id", "owner_id",
		"object_type", "name", "knowledge_object_id",
	})
	assertKnowledge0033IndexColumns(t, raw, "knowledge_object_dependencies_source_target_idx", []string{
		"tenant_id", "source_object_id", "source_object_version",
		"target_kind", "target_object_id", "target_object_version",
	})
	assertKnowledge0033GuardPlanUsesBoundedIndexes(t, raw)
	assertNoForeignKeyViolations(t, raw)
}

func TestKnowledgeActivePublicationDependenciesMigrationAllowsEnableRederivation(t *testing.T) {
	t.Parallel()

	raw := openKnowledgeMigrationTestDB(t, "knowledge-active-publication-enable.sqlite")
	if err := ApplyMigrations(context.Background(), raw, migrations.SQLite()); err != nil {
		t.Fatalf("apply migrations: %v", err)
	}
	seedKnowledgeStatePrerequisites(t, raw)
	insertKnowledgeStateVersion(t, raw, stateVersionFixture{
		objectID: "ko-enable-target", version: 1, state: "active",
		mutation: "create", timestamp: 1,
	})
	insertKnowledgeStateVersion(t, raw, stateVersionFixture{
		objectID: "ko-enable-source", version: 1, state: "draft",
		mutation: "create", timestamp: 10,
	})

	insertKnowledgeWriterDependencyVersion(
		t, raw, "ko-enable-source", 2, "active", "enable",
		"ko-enable-target", 20,
	)

	assertIntegerQuery(t, raw, 1, `
		SELECT count(*)
		FROM knowledge_objects AS object
		JOIN knowledge_object_versions AS version
		  ON version.tenant_id = object.tenant_id
		 AND version.knowledge_object_id = object.knowledge_object_id
		 AND version.object_version = object.current_version
		JOIN knowledge_object_dependency_seals AS seal
		  ON seal.tenant_id = version.tenant_id
		 AND seal.knowledge_object_id = version.knowledge_object_id
		 AND seal.object_version = version.object_version
		WHERE object.tenant_id = 'tenant-a'
		  AND object.knowledge_object_id = 'ko-enable-source'
		  AND object.current_version = 2 AND object.state = 'active'
		  AND version.mutation_kind = 'enable'
		  AND version.dependency_count = 1
		  AND seal.dependency_count = 1`)
	assertIntegerQuery(t, raw, 1, `
		SELECT count(*) FROM knowledge_object_dependencies
		WHERE tenant_id = 'tenant-a'
		  AND source_object_id = 'ko-enable-source'
		  AND source_object_version = 2 AND ordinal = 0
		  AND target_kind = 'object'
		  AND target_object_id = 'ko-enable-target'
		  AND target_object_version = 1
		  AND dependency_role = 'field_input'`)
	assertIntegerQuery(t, raw, 0, `
		SELECT dependency_count FROM knowledge_object_dependency_seals
		WHERE tenant_id = 'tenant-a'
		  AND knowledge_object_id = 'ko-enable-source'
		  AND object_version = 1`)
	assertNoForeignKeyViolations(t, raw)
}

func TestKnowledgeActivePublicationDependenciesMigrationKeepsStateOnlyEdgesExact(t *testing.T) {
	t.Parallel()

	for _, mutation := range []string{"disable", "delete"} {
		mutation := mutation
		t.Run(mutation, func(t *testing.T) {
			t.Parallel()
			raw := openKnowledgeMigrationTestDB(t, "knowledge-active-publication-"+mutation+".sqlite")
			if err := ApplyMigrations(context.Background(), raw, migrations.SQLite()); err != nil {
				t.Fatalf("apply migrations: %v", err)
			}
			seedKnowledgeStatePrerequisites(t, raw)
			for index, targetID := range []string{"ko-state-target-a", "ko-state-target-b"} {
				insertKnowledgeStateVersion(t, raw, stateVersionFixture{
					objectID: targetID, version: 1, state: "active",
					mutation: "create", timestamp: int64(index + 1),
				})
			}
			insertKnowledgeWriterDependencyVersion(
				t, raw, "ko-state-source", 1, "active", "create",
				"ko-state-target-a", 10,
			)

			exact, err := raw.BeginTx(t.Context(), nil)
			if err != nil {
				t.Fatalf("begin exact %s: %v", mutation, err)
			}
			if err := stageKnowledge0033StateOnlyDependency(
				exact, mutation, "ko-state-target-a",
			); err != nil {
				_ = exact.Rollback()
				t.Fatalf("stage exact %s dependency: %v", mutation, err)
			}
			if err := exact.Rollback(); err != nil {
				t.Fatalf("roll back exact %s probe: %v", mutation, err)
			}

			drifted, err := raw.BeginTx(t.Context(), nil)
			if err != nil {
				t.Fatalf("begin drifted %s: %v", mutation, err)
			}
			err = stageKnowledge0033StateOnlyDependency(
				drifted, mutation, "ko-state-target-b",
			)
			if err == nil || !strings.Contains(err.Error(), "state-only dependencies are invalid") {
				_ = drifted.Rollback()
				t.Fatalf("drifted %s dependency error = %v", mutation, err)
			}
			if err := drifted.Rollback(); err != nil {
				t.Fatalf("roll back drifted %s probe: %v", mutation, err)
			}
			assertIntegerQuery(t, raw, 1, `
				SELECT count(*) FROM knowledge_object_versions
				WHERE tenant_id = 'tenant-a'
				  AND knowledge_object_id = 'ko-state-source'`)
			assertNoForeignKeyViolations(t, raw)
		})
	}
}

func TestKnowledgeActivePublicationDependenciesMigrationGuardsTargetVersionAdvance(t *testing.T) {
	t.Parallel()

	t.Run("current active old pin blocks", func(t *testing.T) {
		t.Parallel()
		raw := newKnowledge0033DependencyDB(t, "knowledge-active-publication-blocked.sqlite")
		seedKnowledge0033ActiveDependency(t, raw)

		tx, err := raw.BeginTx(t.Context(), nil)
		if err != nil {
			t.Fatalf("begin blocked advance: %v", err)
		}
		digest, err := stageKnowledge0033ActiveUpdate(tx, "ko-version-target", 2, 20, "", 0)
		if err != nil {
			_ = tx.Rollback()
			t.Fatalf("stage target update: %v", err)
		}
		err = publishKnowledge0033ActiveUpdate(tx, "ko-version-target", 2, 20, digest)
		if err == nil || !strings.Contains(err.Error(), "pins a prior target version") {
			_ = tx.Rollback()
			t.Fatalf("blocked target advance error = %v", err)
		}
		if err := tx.Rollback(); err != nil {
			t.Fatalf("roll back blocked advance: %v", err)
		}
		assertIntegerQuery(t, raw, 1, `
			SELECT current_version FROM knowledge_objects
			WHERE tenant_id = 'tenant-a' AND knowledge_object_id = 'ko-version-target'`)
		assertIntegerQuery(t, raw, 0, `
			SELECT count(*) FROM knowledge_object_versions
			WHERE tenant_id = 'tenant-a' AND knowledge_object_id = 'ko-version-target'
			  AND object_version = 2`)
	})

	t.Run("atomic disable advance enable cascade ignores historical old pins", func(t *testing.T) {
		t.Parallel()
		raw := newKnowledge0033DependencyDB(t, "knowledge-active-publication-repin.sqlite")
		seedKnowledge0033ActiveDependency(t, raw)

		tx, err := raw.BeginTx(t.Context(), nil)
		if err != nil {
			t.Fatalf("begin atomic repin: %v", err)
		}
		if err := stageKnowledge0033RetainedDefinitionPublication(
			tx, "ko-version-dependent", 2, "disabled", "disable", 20,
			"ko-version-target", 1,
		); err != nil {
			_ = tx.Rollback()
			t.Fatalf("stage dependent disable: %v", err)
		}
		if err := publishKnowledge0033RetainedDefinitionVersion(
			tx, "ko-version-dependent", 2, "disabled", 20,
		); err != nil {
			_ = tx.Rollback()
			t.Fatalf("publish dependent disable: %v", err)
		}
		targetDigest, err := stageKnowledge0033ActiveUpdate(tx, "ko-version-target", 2, 21, "", 0)
		if err != nil {
			_ = tx.Rollback()
			t.Fatalf("stage target update: %v", err)
		}
		if err := publishKnowledge0033ActiveUpdate(
			tx, "ko-version-target", 2, 21, targetDigest,
		); err != nil {
			_ = tx.Rollback()
			t.Fatalf("publish target while dependent is disabled: %v", err)
		}
		if err := stageKnowledge0033RetainedDefinitionPublication(
			tx, "ko-version-dependent", 3, "active", "enable", 22,
			"ko-version-target", 2,
		); err != nil {
			_ = tx.Rollback()
			t.Fatalf("stage dependent enable: %v", err)
		}
		if err := publishKnowledge0033RetainedDefinitionVersion(
			tx, "ko-version-dependent", 3, "active", 22,
		); err != nil {
			_ = tx.Rollback()
			t.Fatalf("publish dependent enable: %v", err)
		}
		if err := tx.Commit(); err != nil {
			t.Fatalf("commit atomic repin: %v", err)
		}
		assertIntegerQuery(t, raw, 2, `
			SELECT current_version FROM knowledge_objects
			WHERE tenant_id = 'tenant-a' AND knowledge_object_id = 'ko-version-target'`)
		assertIntegerQuery(t, raw, 3, `
			SELECT current_version FROM knowledge_objects
			WHERE tenant_id = 'tenant-a' AND knowledge_object_id = 'ko-version-dependent'`)
		assertIntegerQuery(t, raw, 3, `
			SELECT count(*) FROM knowledge_object_dependencies
			WHERE tenant_id = 'tenant-a'
			  AND source_object_id = 'ko-version-dependent'
			  AND target_object_id = 'ko-version-target'
			  AND (
			      (source_object_version IN (1, 2) AND target_object_version = 1)
			      OR (source_object_version = 3 AND target_object_version = 2)
			  )`)
		assertNoForeignKeyViolations(t, raw)
	})

	t.Run("historical pin does not block", func(t *testing.T) {
		t.Parallel()
		raw := newKnowledge0033DependencyDB(t, "knowledge-active-publication-historical.sqlite")
		seedKnowledge0033ActiveDependency(t, raw)

		tx, err := raw.BeginTx(t.Context(), nil)
		if err != nil {
			t.Fatalf("begin historical probe: %v", err)
		}
		targetDigest, err := stageKnowledge0033ActiveUpdate(tx, "ko-version-target", 2, 20, "", 0)
		if err != nil {
			_ = tx.Rollback()
			t.Fatalf("stage target update: %v", err)
		}
		dependentDigest, err := stageKnowledge0033ActiveUpdate(tx, "ko-version-dependent", 2, 21, "", 0)
		if err != nil {
			_ = tx.Rollback()
			t.Fatalf("stage dependency-free current version: %v", err)
		}
		if err := publishKnowledge0033ActiveUpdate(
			tx, "ko-version-dependent", 2, 21, dependentDigest,
		); err != nil {
			_ = tx.Rollback()
			t.Fatalf("publish dependency-free current version: %v", err)
		}
		if err := publishKnowledge0033ActiveUpdate(
			tx, "ko-version-target", 2, 20, targetDigest,
		); err != nil {
			_ = tx.Rollback()
			t.Fatalf("historical pin blocked target advance: %v", err)
		}
		if err := tx.Commit(); err != nil {
			t.Fatalf("commit historical probe: %v", err)
		}
		assertIntegerQuery(t, raw, 1, `
			SELECT count(*) FROM knowledge_object_dependencies
			WHERE tenant_id = 'tenant-a'
			  AND source_object_id = 'ko-version-dependent'
			  AND source_object_version = 1
			  AND target_object_id = 'ko-version-target'
			  AND target_object_version = 1`)
		assertIntegerQuery(t, raw, 0, `
			SELECT count(*) FROM knowledge_object_dependencies
			WHERE tenant_id = 'tenant-a'
			  AND source_object_id = 'ko-version-dependent'
			  AND source_object_version = 2`)
	})

	t.Run("nonactive current pin does not block", func(t *testing.T) {
		t.Parallel()
		raw := newKnowledge0033DependencyDB(t, "knowledge-active-publication-nonactive.sqlite")
		seedKnowledge0033ActiveDependency(t, raw)
		insertKnowledgeWriterDependencyVersion(
			t, raw, "ko-version-dependent", 2, "disabled", "disable",
			"ko-version-target", 15,
		)

		tx, err := raw.BeginTx(t.Context(), nil)
		if err != nil {
			t.Fatalf("begin nonactive probe: %v", err)
		}
		targetDigest, err := stageKnowledge0033ActiveUpdate(tx, "ko-version-target", 2, 20, "", 0)
		if err != nil {
			_ = tx.Rollback()
			t.Fatalf("stage target update: %v", err)
		}
		if err := publishKnowledge0033ActiveUpdate(
			tx, "ko-version-target", 2, 20, targetDigest,
		); err != nil {
			_ = tx.Rollback()
			t.Fatalf("nonactive pin blocked target advance: %v", err)
		}
		if err := tx.Commit(); err != nil {
			t.Fatalf("commit nonactive probe: %v", err)
		}
		assertIntegerQuery(t, raw, 1, `
			SELECT count(*) FROM knowledge_objects
			WHERE tenant_id = 'tenant-a'
			  AND knowledge_object_id = 'ko-version-dependent'
			  AND current_version = 2 AND state = 'disabled'`)
		assertIntegerQuery(t, raw, 2, `
			SELECT current_version FROM knowledge_objects
			WHERE tenant_id = 'tenant-a' AND knowledge_object_id = 'ko-version-target'`)
		assertNoForeignKeyViolations(t, raw)
	})
}

func TestKnowledgeActivePublicationDependenciesMigrationRejectsStaleUpgradeAtomically(t *testing.T) {
	t.Parallel()

	raw := openKnowledgeMigrationTestDB(t, "knowledge-active-publication-stale-upgrade.sqlite")
	if err := ApplyMigrations(context.Background(), raw, migrationsBefore(t, "0033_")); err != nil {
		t.Fatalf("apply through migration 0032: %v", err)
	}
	seedKnowledgeStatePrerequisites(t, raw)
	insertKnowledgeStateVersion(t, raw, stateVersionFixture{
		objectID: "ko-stale-target", version: 1, state: "active",
		mutation: "create", timestamp: 1,
	})
	insertKnowledgeWriterDependencyVersion(
		t, raw, "ko-stale-dependent", 1, "active", "create",
		"ko-stale-target", 2,
	)
	insertKnowledgeStateVersion(t, raw, stateVersionFixture{
		objectID: "ko-stale-target", version: 2, state: "active",
		mutation: "update", timestamp: 3,
	})

	err := ApplyMigrations(context.Background(), raw, migrations.SQLite())
	if err == nil || !strings.Contains(err.Error(), "CHECK constraint failed") {
		t.Fatalf("stale upgrade error = %v", err)
	}
	assertIntegerQuery(t, raw, 32, `SELECT count(*) FROM schema_migrations`)
	assertIntegerQuery(t, raw, 0, `
		SELECT count(*) FROM sqlite_schema
		WHERE name = 'knowledge_active_publication_0033_upgrade_guard'`)
	assertIntegerQuery(t, raw, 0, `
		SELECT count(*) FROM sqlite_schema
		WHERE name = 'knowledge_active_dependency_target_version_advance_is_blocked'`)
	oldWriterTrigger := knowledge0033SchemaSQL(
		t, raw, "trigger", "knowledge_object_version_writer_semantics_are_exact",
	)
	if !strings.Contains(oldWriterTrigger, "NEW.dependency_count = previous.dependency_count") {
		t.Fatal("failed upgrade did not retain the migration 0030 writer trigger")
	}
	assertKnowledge0033IndexColumns(t, raw, "knowledge_object_dependencies_source_target_idx", []string{
		"tenant_id", "source_object_id", "source_object_version",
		"target_kind", "target_object_id",
	})
	assertIntegerQuery(t, raw, 1, `
		SELECT count(*)
		FROM knowledge_objects AS dependent
		JOIN knowledge_object_dependencies AS dependency
		  ON dependency.tenant_id = dependent.tenant_id
		 AND dependency.source_object_id = dependent.knowledge_object_id
		 AND dependency.source_object_version = dependent.current_version
		JOIN knowledge_objects AS target
		  ON target.tenant_id = dependency.tenant_id
		 AND target.knowledge_object_id = dependency.target_object_id
		WHERE dependent.tenant_id = 'tenant-a'
		  AND dependent.knowledge_object_id = 'ko-stale-dependent'
		  AND dependent.state = 'active'
		  AND target.current_version = 2
		  AND dependency.target_object_version = 1`)
	assertNoForeignKeyViolations(t, raw)
}

func newKnowledge0033DependencyDB(t *testing.T, name string) *sql.DB {
	t.Helper()
	raw := openKnowledgeMigrationTestDB(t, name)
	if err := ApplyMigrations(context.Background(), raw, migrations.SQLite()); err != nil {
		t.Fatalf("apply migrations: %v", err)
	}
	seedKnowledgeStatePrerequisites(t, raw)
	return raw
}

func seedKnowledge0033ActiveDependency(t *testing.T, raw *sql.DB) {
	t.Helper()
	insertKnowledgeStateVersion(t, raw, stateVersionFixture{
		objectID: "ko-version-target", version: 1, state: "active",
		mutation: "create", timestamp: 1,
	})
	insertKnowledgeWriterDependencyVersion(
		t, raw, "ko-version-dependent", 1, "active", "create",
		"ko-version-target", 2,
	)
}

func stageKnowledge0033StateOnlyDependency(tx *sql.Tx, mutation string, targetObjectID string) error {
	state := "disabled"
	if mutation == "delete" {
		state = "deleted"
	}
	if _, err := tx.ExecContext(context.Background(), `
		INSERT INTO knowledge_object_versions (
			tenant_id, knowledge_object_id, object_version,
			app_id, owner_id, object_type, name, sharing_scope, state,
			definition_digest, dependency_count, mutation_kind,
			quarantine_reason, created_at_unix_micro
		) VALUES (
			'tenant-a', 'ko-state-source', 2, ?, 'owner-a',
			'field_extraction', 'ko-state-source', 'private', ?,
			zeroblob(32), 1, ?, NULL, 20
		)`, knowledgeMigrationTestAppID, state, mutation); err != nil {
		return fmt.Errorf("insert state-only version: %w", err)
	}
	if _, err := tx.ExecContext(context.Background(), `
		INSERT INTO knowledge_object_dependencies (
			tenant_id, source_object_id, source_object_version, ordinal,
			target_kind, target_object_id, target_object_version, dependency_role
		) VALUES (
			'tenant-a', 'ko-state-source', 2, 0,
			'object', ?, 1, 'field_input'
		)`, targetObjectID); err != nil {
		return fmt.Errorf("insert state-only edge: %w", err)
	}
	if _, err := tx.ExecContext(context.Background(), `
		INSERT INTO knowledge_object_dependency_seals (
			tenant_id, knowledge_object_id, object_version, dependency_count
		) VALUES ('tenant-a', 'ko-state-source', 2, 1)`); err != nil {
		return fmt.Errorf("seal state-only edge: %w", err)
	}
	return nil
}

func stageKnowledge0033RetainedDefinitionPublication(
	tx *sql.Tx,
	objectID string,
	version int,
	state string,
	mutation string,
	timestamp int64,
	targetObjectID string,
	targetVersion int,
) error {
	dependencyCount := 0
	if targetObjectID != "" {
		dependencyCount = 1
	}
	if _, err := tx.ExecContext(context.Background(), `
		INSERT INTO knowledge_object_versions (
			tenant_id, knowledge_object_id, object_version,
			app_id, owner_id, object_type, name, sharing_scope, state,
			definition_digest, dependency_count, mutation_kind,
			quarantine_reason, created_at_unix_micro
		) VALUES (
			'tenant-a', ?, ?, ?, 'owner-a', 'field_extraction', ?,
			'private', ?, zeroblob(32), ?, ?, NULL, ?
		)`, objectID, version, knowledgeMigrationTestAppID, objectID,
		state, dependencyCount, mutation, timestamp); err != nil {
		return fmt.Errorf("insert retained-definition version: %w", err)
	}
	if targetObjectID != "" {
		if _, err := tx.ExecContext(context.Background(), `
			INSERT INTO knowledge_object_dependencies (
				tenant_id, source_object_id, source_object_version, ordinal,
				target_kind, target_object_id, target_object_version, dependency_role
			) VALUES (
				'tenant-a', ?, ?, 0, 'object', ?, ?, 'field_input'
			)`, objectID, version, targetObjectID, targetVersion); err != nil {
			return fmt.Errorf("insert retained-definition dependency: %w", err)
		}
	}
	if _, err := tx.ExecContext(context.Background(), `
		INSERT INTO knowledge_object_dependency_seals (
			tenant_id, knowledge_object_id, object_version, dependency_count
		) VALUES ('tenant-a', ?, ?, ?)`, objectID, version, dependencyCount); err != nil {
		return fmt.Errorf("seal retained-definition dependencies: %w", err)
	}
	if _, err := tx.ExecContext(context.Background(), `
		INSERT INTO knowledge_object_list_projections (
			tenant_id, knowledge_object_id, object_version,
			app_id, owner_id, object_type, name, sharing_scope, state,
			description_present, description,
			index_selector_count, host_selector_count,
			source_selector_count, sourcetype_selector_count,
			selector_value_bytes, canonical_selector_bytes
		) VALUES (
			'tenant-a', ?, ?, ?, 'owner-a', 'field_extraction', ?,
			'private', ?, 0, '', 0, 0, 0, 0, 0, 46
		);
		INSERT INTO knowledge_object_list_projection_seals (
			tenant_id, knowledge_object_id, object_version,
			projection_bytes, canonical_selector_bytes
		) SELECT tenant_id, knowledge_object_id, object_version,
		         projection_bytes, canonical_selector_bytes
		    FROM knowledge_object_list_projections
		   WHERE tenant_id = 'tenant-a'
		     AND knowledge_object_id = ? AND object_version = ?`,
		objectID, version, knowledgeMigrationTestAppID, objectID, state,
		objectID, version); err != nil {
		return fmt.Errorf("stage retained-definition projection: %w", err)
	}
	return nil
}

func publishKnowledge0033RetainedDefinitionVersion(
	tx *sql.Tx,
	objectID string,
	version int,
	state string,
	timestamp int64,
) error {
	disabledAt := any(nil)
	if state == "disabled" {
		disabledAt = timestamp
	}
	result, err := tx.ExecContext(context.Background(), `
		UPDATE knowledge_objects
		SET current_version = ?, state = ?, updated_at_unix_micro = ?,
		    disabled_at_unix_micro = ?
		WHERE tenant_id = 'tenant-a' AND knowledge_object_id = ?
		  AND current_version = ?`,
		version, state, timestamp, disabledAt, objectID, version-1)
	if err != nil {
		return err
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("read retained-definition rows affected: %w", err)
	}
	if affected != 1 {
		return fmt.Errorf("publish retained-definition version affected %d rows", affected)
	}
	return nil
}

func stageKnowledge0033ActiveUpdate(
	tx *sql.Tx,
	objectID string,
	version int,
	timestamp int64,
	targetObjectID string,
	targetVersion int,
) ([]byte, error) {
	body := []byte(fmt.Sprintf("knowledge-0033/%s/%d", objectID, version))
	digest := sha256.Sum256(body)
	if _, err := tx.ExecContext(context.Background(), `
		INSERT INTO knowledge_definition_blobs (
			tenant_id, definition_digest, definition_proto,
			definition_bytes, created_at_unix_micro
		) VALUES ('tenant-a', ?, ?, ?, ?)`, digest[:], body, len(body), timestamp); err != nil {
		return nil, fmt.Errorf("insert update definition: %w", err)
	}
	dependencyCount := 0
	if targetObjectID != "" {
		dependencyCount = 1
	}
	if _, err := tx.ExecContext(context.Background(), `
		INSERT INTO knowledge_object_versions (
			tenant_id, knowledge_object_id, object_version,
			app_id, owner_id, object_type, name, sharing_scope, state,
			definition_digest, dependency_count, mutation_kind,
			quarantine_reason, created_at_unix_micro
		) VALUES (
			'tenant-a', ?, ?, ?, 'owner-a', 'field_extraction', ?,
			'private', 'active', ?, ?, 'update', NULL, ?
		)`, objectID, version, knowledgeMigrationTestAppID, objectID,
		digest[:], dependencyCount, timestamp); err != nil {
		return nil, fmt.Errorf("insert active update version: %w", err)
	}
	if targetObjectID != "" {
		if _, err := tx.ExecContext(context.Background(), `
			INSERT INTO knowledge_object_dependencies (
				tenant_id, source_object_id, source_object_version, ordinal,
				target_kind, target_object_id, target_object_version, dependency_role
			) VALUES (
				'tenant-a', ?, ?, 0, 'object', ?, ?, 'field_input'
			)`, objectID, version, targetObjectID, targetVersion); err != nil {
			return nil, fmt.Errorf("insert active update dependency: %w", err)
		}
	}
	if _, err := tx.ExecContext(context.Background(), `
		INSERT INTO knowledge_object_dependency_seals (
			tenant_id, knowledge_object_id, object_version, dependency_count
		) VALUES ('tenant-a', ?, ?, ?)`, objectID, version, dependencyCount); err != nil {
		return nil, fmt.Errorf("seal active update dependencies: %w", err)
	}
	if _, err := tx.ExecContext(context.Background(), `
		INSERT INTO knowledge_object_list_projections (
			tenant_id, knowledge_object_id, object_version,
			app_id, owner_id, object_type, name, sharing_scope, state,
			description_present, description,
			index_selector_count, host_selector_count,
			source_selector_count, sourcetype_selector_count,
			selector_value_bytes, canonical_selector_bytes
		) VALUES (
			'tenant-a', ?, ?, ?, 'owner-a', 'field_extraction', ?,
			'private', 'active', 0, '', 0, 0, 0, 0, 0, 46
		);
		INSERT INTO knowledge_object_list_projection_seals (
			tenant_id, knowledge_object_id, object_version,
			projection_bytes, canonical_selector_bytes
		) SELECT tenant_id, knowledge_object_id, object_version,
		         projection_bytes, canonical_selector_bytes
		    FROM knowledge_object_list_projections
		   WHERE tenant_id = 'tenant-a'
		     AND knowledge_object_id = ? AND object_version = ?`,
		objectID, version, knowledgeMigrationTestAppID, objectID, objectID, version); err != nil {
		return nil, fmt.Errorf("stage active update projection: %w", err)
	}
	return digest[:], nil
}

func publishKnowledge0033ActiveUpdate(
	tx *sql.Tx,
	objectID string,
	version int,
	timestamp int64,
	digest []byte,
) error {
	result, err := tx.ExecContext(context.Background(), `
		UPDATE knowledge_objects
		SET current_version = ?, definition_digest = ?, updated_at_unix_micro = ?
		WHERE tenant_id = 'tenant-a' AND knowledge_object_id = ?
		  AND current_version = ?`, version, digest, timestamp, objectID, version-1)
	if err != nil {
		return err
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("read active update rows affected: %w", err)
	}
	if affected != 1 {
		return fmt.Errorf("publish active update affected %d rows", affected)
	}
	return nil
}

func knowledge0033SchemaSQL(t *testing.T, db *sql.DB, objectType string, name string) string {
	t.Helper()
	var statement string
	if err := db.QueryRowContext(t.Context(), `
		SELECT sql FROM sqlite_schema
		WHERE type = ? AND name = ?`, objectType, name).Scan(&statement); err != nil {
		t.Fatalf("read %s %s SQL: %v", objectType, name, err)
	}
	return statement
}

func assertKnowledge0033IndexColumns(t *testing.T, db *sql.DB, indexName string, want []string) {
	t.Helper()
	rows, err := db.QueryContext(t.Context(), `SELECT name FROM pragma_index_info(?) ORDER BY seqno`, indexName)
	if err != nil {
		t.Fatalf("read index %s columns: %v", indexName, err)
	}
	defer rows.Close()
	var got []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			t.Fatalf("scan index %s column: %v", indexName, err)
		}
		got = append(got, name)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate index %s columns: %v", indexName, err)
	}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("index %s columns = %v, want %v", indexName, got, want)
	}
}

func assertKnowledge0033GuardPlanUsesBoundedIndexes(t *testing.T, db *sql.DB) {
	t.Helper()
	rows, err := db.QueryContext(t.Context(), `EXPLAIN QUERY PLAN
		SELECT 1
		FROM knowledge_objects AS dependent
		    INDEXED BY knowledge_objects_resolution_idx
		CROSS JOIN knowledge_object_dependencies AS dependency
		    INDEXED BY knowledge_object_dependencies_source_target_idx
		WHERE dependent.tenant_id = 'tenant-a'
		  AND dependent.state = 'active'
		  AND dependency.tenant_id = dependent.tenant_id
		  AND dependency.source_object_id = dependent.knowledge_object_id
		  AND dependency.source_object_version = dependent.current_version
		  AND dependency.target_kind = 'object'
		  AND dependency.target_object_id = 'ko-target'
		  AND dependency.target_object_version <> 2
		LIMIT 1`)
	if err != nil {
		t.Fatalf("explain target-version guard: %v", err)
	}
	defer rows.Close()
	var details []string
	for rows.Next() {
		var id, parent, unused int
		var detail string
		if err := rows.Scan(&id, &parent, &unused, &detail); err != nil {
			t.Fatalf("scan target-version guard plan: %v", err)
		}
		details = append(details, detail)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate target-version guard plan: %v", err)
	}
	joined := strings.Join(details, "\n")
	for _, search := range []string{
		"SEARCH dependent USING INDEX knowledge_objects_resolution_idx (tenant_id=? AND state=?)",
		"SEARCH dependency USING COVERING INDEX knowledge_object_dependencies_source_target_idx (tenant_id=? AND source_object_id=? AND source_object_version=? AND target_kind=? AND target_object_id=?)",
	} {
		if !strings.Contains(joined, search) {
			t.Errorf("target-version guard plan lacks %q:\n%s", search, joined)
		}
	}
	if strings.Contains(joined, "SCAN dependency") {
		t.Fatalf("target-version guard scans dependency rows:\n%s", joined)
	}
}
