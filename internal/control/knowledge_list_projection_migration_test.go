package control

import (
	"context"
	"database/sql"
	"strings"
	"testing"
)

func TestKnowledgeListProjectionMigrationFreshAndUpgrade(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name    string
		upgrade bool
	}{
		{name: "fresh"},
		{name: "upgrade_from_empty_0024", upgrade: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			raw := openKnowledgeMigrationTestDB(t, "knowledge-list-"+test.name+".sqlite")
			if test.upgrade {
				if err := ApplyMigrations(context.Background(), raw, migrationsBefore(t, "0025_")); err != nil {
					t.Fatalf("apply through migration 0024: %v", err)
				}
			}
			if err := ApplyMigrations(context.Background(), raw, migrationsBefore(t, "0026_")); err != nil {
				t.Fatalf("apply through projection migration: %v", err)
			}

			seedProjectionPrerequisites(t, raw)
			insertProjectedKnowledgeObject(t, raw, projectionFixture{
				ObjectID: "ko-" + test.name,
				Version:  1,
				Name:     "Projected",
			}, 10)

			assertIntegerQuery(t, raw, 25, `SELECT count(*) FROM schema_migrations`)
			for _, table := range []string{
				"knowledge_projection_tenant_ledgers",
				"knowledge_object_list_projections",
				"knowledge_object_list_selector_patterns",
				"knowledge_object_list_projection_seals",
			} {
				var withoutRowID, strict int
				if err := raw.QueryRow(`
					SELECT wr, strict FROM pragma_table_list WHERE name = ?`, table,
				).Scan(&withoutRowID, &strict); err != nil {
					t.Fatalf("inspect %s: %v", table, err)
				}
				if withoutRowID != 1 || strict != 1 {
					t.Fatalf("%s flags = WITHOUT ROWID %d STRICT %d", table, withoutRowID, strict)
				}
			}
			assertNoForeignKeyViolations(t, raw)
		})
	}
}

func TestKnowledgeListProjectionMigrationRejectsUnprojectedLegacyCatalog(t *testing.T) {
	t.Parallel()

	raw := openKnowledgeMigrationTestDB(t, "knowledge-list-reject-legacy.sqlite")
	if err := ApplyMigrations(context.Background(), raw, migrationsBefore(t, "0025_")); err != nil {
		t.Fatalf("apply through migration 0024: %v", err)
	}
	seedKnowledgeMigrationApp(t, raw)
	if _, err := raw.Exec(`
		INSERT INTO knowledge_catalog_tenants (tenant_id) VALUES ('tenant-a');
		INSERT INTO knowledge_definition_blobs (
			tenant_id, definition_digest, definition_proto,
			definition_bytes, created_at_unix_micro
		) VALUES ('tenant-a', zeroblob(32), X'01', 1, 10)`); err != nil {
		t.Fatalf("seed legacy catalog: %v", err)
	}
	insertKnowledgeMigrationObject(t, raw, "ko-legacy", "Legacy", "private", "active", 10)

	err := ApplyMigrations(context.Background(), raw, migrationsBefore(t, "0026_"))
	if err == nil || !strings.Contains(err.Error(), "requires empty catalog") {
		t.Fatalf("projection upgrade error = %v", err)
	}
	assertIntegerQuery(t, raw, 24, `SELECT count(*) FROM schema_migrations`)
	assertIntegerQuery(t, raw, 1, `
		SELECT count(*) FROM knowledge_objects
		WHERE tenant_id = 'tenant-a' AND knowledge_object_id = 'ko-legacy'`)
	assertIntegerQuery(t, raw, 0, `
		SELECT count(*) FROM sqlite_schema
		WHERE name IN (
			'knowledge_object_list_projections',
			'knowledge_projection_tenant_ledgers',
			'knowledge_objects_current_projection_identity_idx'
		)`)
	assertNoForeignKeyViolations(t, raw)
}

func TestKnowledgeListProjectionCompletenessImmutabilityAndFiltering(t *testing.T) {
	t.Parallel()

	raw := openProjectionMigrationDatabase(t, "projection-invariants.sqlite")
	enableRecursiveProjectionTriggers(t, raw)
	seedProjectionPrerequisites(t, raw)
	insertProjectedKnowledgeObject(t, raw, projectionFixture{
		ObjectID:    "ko-alpha",
		Version:     1,
		Name:        "Alpha",
		Description: "Alpha description",
		Selectors: []projectionSelector{
			{Dimension: "index", Ordinal: 0, MatchKind: "wildcard", Value: "audit*"},
			{Dimension: "index", Ordinal: 1, MatchKind: "exact", Value: "main"},
			{Dimension: "host", Ordinal: 0, MatchKind: "exact", Value: "web-01"},
		},
	}, 10)
	insertProjectedKnowledgeObject(t, raw, projectionFixture{
		ObjectID: "ko-beta",
		Version:  1,
		Name:     "Beta",
	}, 11)
	insertProjectedKnowledgeObject(t, raw, projectionFixture{
		ObjectID:    "ko-gamma",
		Version:     1,
		Name:        "Gamma",
		Description: "Gamma needle description",
		Selectors: []projectionSelector{
			{Dimension: "index", Ordinal: 0, MatchKind: "exact", Value: "main"},
			{Dimension: "source", Ordinal: 0, MatchKind: "wildcard", Value: "/srv/*"},
		},
	}, 12)

	var ledgerBefore int
	if err := raw.QueryRow(`
		SELECT projection_bytes FROM knowledge_projection_tenant_ledgers
		WHERE tenant_id = 'tenant-a'`).Scan(&ledgerBefore); err != nil {
		t.Fatalf("read projection ledger: %v", err)
	}
	assertProjectionPublicationGuards(t, raw, ledgerBefore)
	assertProjectionTriggerContains(t, raw,
		"knowledge_list_projection_seal_is_complete",
		"AS selector_aggregate")
	assertProjectionTriggerFragmentCount(t, raw,
		"knowledge_list_projection_seal_is_complete",
		"FROM knowledge_object_list_selector_patterns", 2)

	// The deferred current-tuple FK rejects a future version at autocommit and
	// rolls its byte-ledger increment back with it.
	assertSQLFailsContaining(t, raw, "FOREIGN KEY", `
		INSERT INTO knowledge_object_list_projections (
			tenant_id, knowledge_object_id, object_version,
			app_id, owner_id, object_type, name, sharing_scope, state,
			description_present, description,
			index_selector_count, host_selector_count,
			source_selector_count, sourcetype_selector_count,
			selector_value_bytes, canonical_selector_bytes
		) VALUES (
			'tenant-a', 'ko-alpha', 2, 'app_AAAAAAAAAAAAAAAAAAAAAA',
			'owner-a', 'field_extraction', 'Alpha', 'private', 'active',
			0, '', 0, 0, 0, 0, 0, 46
		)`)
	assertIntegerQuery(t, raw, ledgerBefore, `
		SELECT projection_bytes FROM knowledge_projection_tenant_ledgers
		WHERE tenant_id = 'tenant-a'`)

	// Declared counts and bytes must agree before the projection can be sealed.
	incomplete, err := raw.Begin()
	if err != nil {
		t.Fatalf("begin incomplete projection: %v", err)
	}
	if _, err := incomplete.Exec(`
		INSERT INTO knowledge_object_list_projections (
			tenant_id, knowledge_object_id, object_version,
			app_id, owner_id, object_type, name, sharing_scope, state,
			description_present, description,
			index_selector_count, host_selector_count,
			source_selector_count, sourcetype_selector_count,
			selector_value_bytes, canonical_selector_bytes
		) VALUES (
			'tenant-a', 'ko-alpha', 2, 'app_AAAAAAAAAAAAAAAAAAAAAA',
			'owner-a', 'field_extraction', 'Alpha', 'private', 'active',
			0, '', 2, 0, 0, 0, 9, 63
		);
		INSERT INTO knowledge_object_list_selector_patterns (
			tenant_id, knowledge_object_id, object_version,
			dimension, ordinal, match_kind, value
		) VALUES ('tenant-a', 'ko-alpha', 2, 'index', 0, 'exact', 'main');
		INSERT INTO knowledge_object_list_projection_seals (
			tenant_id, knowledge_object_id, object_version,
			projection_bytes, canonical_selector_bytes
		) SELECT tenant_id, knowledge_object_id, object_version,
		         projection_bytes, canonical_selector_bytes
		    FROM knowledge_object_list_projections
		   WHERE tenant_id = 'tenant-a' AND knowledge_object_id = 'ko-alpha'
		     AND object_version = 2`); err == nil || !strings.Contains(err.Error(), "incomplete") {
		_ = incomplete.Rollback()
		t.Fatalf("incomplete projection seal error = %v", err)
	}
	if err := incomplete.Rollback(); err != nil {
		t.Fatalf("roll back incomplete projection: %v", err)
	}

	assertUnsealedProjectionRejectsDuplicateAndUnorderedSelectors(t, raw)

	var generatedBytes, descriptionBytes, selectorBytes, canonicalBytes int
	if err := raw.QueryRow(`
		SELECT projection_bytes, length(CAST(description AS BLOB)),
		       selector_value_bytes, canonical_selector_bytes
		FROM knowledge_object_list_projections
		WHERE tenant_id = 'tenant-a' AND knowledge_object_id = 'ko-alpha'`,
	).Scan(&generatedBytes, &descriptionBytes, &selectorBytes, &canonicalBytes); err != nil {
		t.Fatalf("read exact projection accounting: %v", err)
	}
	if generatedBytes != descriptionBytes+selectorBytes {
		t.Fatalf("projection bytes = %d, want description %d + selector values %d", generatedBytes, descriptionBytes, selectorBytes)
	}
	if canonicalBytes != 46+4*3+selectorBytes || canonicalBytes > 8192 {
		t.Fatalf("canonical selector bytes = %d, selector value bytes = %d", canonicalBytes, selectorBytes)
	}
	assertIntegerQuery(t, raw, ledgerBefore, `
		SELECT sum(projection_bytes) FROM knowledge_object_list_projections
		WHERE tenant_id = 'tenant-a'`)
	assertIntegerQuery(t, raw, 0, `
		SELECT projection_bytes FROM knowledge_object_list_projections
		WHERE tenant_id = 'tenant-a' AND knowledge_object_id = 'ko-beta'`)

	assertSQLFailsContaining(t, raw, "immutable", `
		UPDATE knowledge_object_list_projections SET description = 'changed'
		WHERE tenant_id = 'tenant-a' AND knowledge_object_id = 'ko-alpha'`)
	assertSQLFailsContaining(t, raw, "immutable", `
		UPDATE knowledge_object_list_selector_patterns SET value = 'changed'
		WHERE tenant_id = 'tenant-a' AND knowledge_object_id = 'ko-alpha'`)
	assertSQLFailsContaining(t, raw, "immutable", `
		UPDATE knowledge_object_list_projection_seals SET projection_bytes = 1
		WHERE tenant_id = 'tenant-a' AND knowledge_object_id = 'ko-alpha'`)
	assertSQLFailsContaining(t, raw, "sealed", `
		DELETE FROM knowledge_object_list_selector_patterns
		WHERE tenant_id = 'tenant-a' AND knowledge_object_id = 'ko-alpha'`)
	assertSQLFailsContaining(t, raw, "unsealed", `
		DELETE FROM knowledge_object_list_projections
		WHERE tenant_id = 'tenant-a' AND knowledge_object_id = 'ko-alpha'`)
	assertSQLFailsContaining(t, raw, "identity already exists", `
		INSERT OR REPLACE INTO knowledge_object_list_projections (
			tenant_id, knowledge_object_id, object_version,
			app_id, owner_id, object_type, name, sharing_scope, state,
			description_present, description,
			index_selector_count, host_selector_count,
			source_selector_count, sourcetype_selector_count,
			selector_value_bytes, canonical_selector_bytes
		) SELECT tenant_id, knowledge_object_id, object_version,
		         app_id, owner_id, object_type, name, sharing_scope, state,
		         description_present, description,
		         index_selector_count, host_selector_count,
		         source_selector_count, sourcetype_selector_count,
		         selector_value_bytes, canonical_selector_bytes
		    FROM knowledge_object_list_projections
		   WHERE tenant_id = 'tenant-a' AND knowledge_object_id = 'ko-alpha'`)
	assertSQLFailsContaining(t, raw, "sealed", `
		INSERT OR REPLACE INTO knowledge_object_list_selector_patterns (
			tenant_id, knowledge_object_id, object_version,
			dimension, ordinal, match_kind, value
		) VALUES ('tenant-a', 'ko-alpha', 1, 'index', 0, 'exact', 'other')`)
	assertSQLFails(t, raw, `
		INSERT OR REPLACE INTO knowledge_object_list_projection_seals (
			tenant_id, knowledge_object_id, object_version,
			projection_bytes, canonical_selector_bytes
		) VALUES ('tenant-a', 'ko-alpha', 1, 1, 1)`)
	assertIntegerQuery(t, raw, 1, `
		SELECT count(*) FROM knowledge_object_list_projection_seals
		WHERE tenant_id = 'tenant-a' AND knowledge_object_id = 'ko-alpha'
		  AND object_version = 1`)
	assertSQLFails(t, raw, `
		INSERT OR REPLACE INTO knowledge_projection_tenant_ledgers (tenant_id)
		VALUES ('tenant-a')`)
	assertIntegerQuery(t, raw, ledgerBefore, `
		SELECT projection_bytes FROM knowledge_projection_tenant_ledgers
		WHERE tenant_id = 'tenant-a'`)

	assertSQLFailsContaining(t, raw, "CHECK constraint", `
		INSERT INTO knowledge_object_list_projections (
			tenant_id, knowledge_object_id, object_version,
			app_id, owner_id, object_type, name, sharing_scope, state,
			description_present, description,
			index_selector_count, host_selector_count,
			source_selector_count, sourcetype_selector_count,
			selector_value_bytes, canonical_selector_bytes
		) VALUES (
			'tenant-a', 'missing', 1, 'app_AAAAAAAAAAAAAAAAAAAAAA',
			'owner-a', 'field_extraction', 'Missing', 'private', 'draft',
			0, 'not-canonical-absence', 0, 0, 0, 0, 0, 46
		)`)
	assertSQLFailsContaining(t, raw, "CHECK constraint", `
		INSERT INTO knowledge_object_list_projections (
			tenant_id, knowledge_object_id, object_version,
			app_id, owner_id, object_type, name, sharing_scope, state,
			description_present, description,
			index_selector_count, host_selector_count,
			source_selector_count, sourcetype_selector_count,
			selector_value_bytes, canonical_selector_bytes
		) VALUES (
			'tenant-a', 'missing', 1, 'app_AAAAAAAAAAAAAAAAAAAAAA',
			'owner-a', 'field_extraction', 'Missing', 'private', 'draft',
			1, '', 0, 0, 0, 0, 0, 46
		)`)
	// Canonical charges above 8 KiB remain forbidden even when the separately
	// bounded selector values and declared pattern counts fit their own limits.
	assertSQLFailsContaining(t, raw, "CHECK constraint", `
		INSERT INTO knowledge_object_list_projections (
			tenant_id, knowledge_object_id, object_version,
			app_id, owner_id, object_type, name, sharing_scope, state,
			description_present, description,
			index_selector_count, host_selector_count,
			source_selector_count, sourcetype_selector_count,
			selector_value_bytes, canonical_selector_bytes
		) VALUES (
			'tenant-a', 'missing', 1, 'app_AAAAAAAAAAAAAAAAAAAAAA',
			'owner-a', 'field_extraction', 'Missing', 'private', 'draft',
			0, '', 16, 16, 16, 16, 64, 8193
		)`)
	assertSQLFailsContaining(t, raw, "CHECK constraint", `
		INSERT INTO knowledge_object_list_projections (
			tenant_id, knowledge_object_id, object_version,
			app_id, owner_id, object_type, name, sharing_scope, state,
			description_present, description,
			index_selector_count, host_selector_count,
			source_selector_count, sourcetype_selector_count,
			selector_value_bytes, canonical_selector_bytes
		) VALUES (
			'tenant-a', 'missing', 1, 'app_AAAAAAAAAAAAAAAAAAAAAA',
			'owner-a', 'field_extraction', 'Missing', 'private', 'draft',
			0, '', 16, 16, 16, 16, 8193, 8193
		)`)
	// Even 8 KiB of selector values is invalid when it omits the fixed and
	// per-pattern canonical framing charge.
	assertSQLFailsContaining(t, raw, "CHECK constraint", `
		INSERT INTO knowledge_object_list_projections (
			tenant_id, knowledge_object_id, object_version,
			app_id, owner_id, object_type, name, sharing_scope, state,
			description_present, description,
			index_selector_count, host_selector_count,
			source_selector_count, sourcetype_selector_count,
			selector_value_bytes, canonical_selector_bytes
		) VALUES (
			'tenant-a', 'missing', 1, 'app_AAAAAAAAAAAAAAAAAAAAAA',
			'owner-a', 'field_extraction', 'Missing', 'private', 'draft',
			0, '', 16, 16, 16, 16, 8192, 8192
		)`)

	// The current registry join and all filters occur before LIMIT; BINARY
	// matching remains case-sensitive. Alpha sorts before Beta, so this also
	// catches an implementation that retrieves one row and filters afterward.
	var filtered string
	if err := raw.QueryRow(`
		SELECT projection.name
		FROM knowledge_object_list_projections AS projection
		JOIN knowledge_object_list_projection_seals AS seal
		  USING (tenant_id, knowledge_object_id, object_version)
		JOIN knowledge_objects AS object
		  ON object.tenant_id = projection.tenant_id
		 AND object.knowledge_object_id = projection.knowledge_object_id
		 AND object.current_version = projection.object_version
		WHERE projection.tenant_id = 'tenant-a'
		  AND instr(projection.name, 'Bet') > 0
		ORDER BY projection.name COLLATE BINARY, projection.knowledge_object_id
		LIMIT 1`).Scan(&filtered); err != nil {
		t.Fatalf("query filtered projection: %v", err)
	}
	if filtered != "Beta" {
		t.Fatalf("filtered projection = %q", filtered)
	}
	assertIntegerQuery(t, raw, 0, `
		SELECT count(*) FROM knowledge_object_list_projections
		WHERE tenant_id = 'tenant-a' AND instr(name, 'beta') > 0`)

	if err := raw.QueryRow(`
		SELECT projection.name
		FROM knowledge_object_list_projections AS projection
		JOIN knowledge_object_list_projection_seals AS seal
		  USING (tenant_id, knowledge_object_id, object_version)
		WHERE projection.tenant_id = 'tenant-a'
		  AND projection.description_present = 1
		  AND instr(projection.description, 'needle') > 0
		ORDER BY projection.name COLLATE BINARY
		LIMIT 1`).Scan(&filtered); err != nil {
		t.Fatalf("query description-filtered projection: %v", err)
	}
	if filtered != "Gamma" {
		t.Fatalf("description-filtered projection = %q", filtered)
	}

	// Selector text is matched against one child value at a time. It must never
	// be synthesized by concatenating adjacent patterns such as audit* + main.
	assertIntegerQuery(t, raw, 0, `
		SELECT count(*)
		FROM knowledge_object_list_projections AS projection
		WHERE projection.tenant_id = 'tenant-a'
		  AND projection.knowledge_object_id = 'ko-alpha'
		  AND EXISTS (
			SELECT 1 FROM knowledge_object_list_selector_patterns AS pattern
			WHERE pattern.tenant_id = projection.tenant_id
			  AND pattern.knowledge_object_id = projection.knowledge_object_id
			  AND pattern.object_version = projection.object_version
			  AND pattern.value = 'audit*main'
		  )`)
	assertIntegerQuery(t, raw, 2, `
		SELECT count(*)
		FROM knowledge_object_list_projections AS projection
		WHERE projection.tenant_id = 'tenant-a'
		  AND EXISTS (
			SELECT 1 FROM knowledge_object_list_selector_patterns AS pattern
			WHERE pattern.tenant_id = projection.tenant_id
			  AND pattern.knowledge_object_id = projection.knowledge_object_id
			  AND pattern.object_version = projection.object_version
			  AND pattern.value = 'main'
		  )`)

	// EXISTS keeps one parent row even when several child patterns match Alpha;
	// deduplication therefore happens before ordering and LIMIT.
	rows, err := raw.Query(`
		SELECT projection.name
		FROM knowledge_object_list_projections AS projection
		JOIN knowledge_object_list_projection_seals AS seal
		  USING (tenant_id, knowledge_object_id, object_version)
		WHERE projection.tenant_id = 'tenant-a'
		  AND EXISTS (
			SELECT 1 FROM knowledge_object_list_selector_patterns AS pattern
			WHERE pattern.tenant_id = projection.tenant_id
			  AND pattern.knowledge_object_id = projection.knowledge_object_id
			  AND pattern.object_version = projection.object_version
			  AND pattern.dimension = 'index'
		  )
		ORDER BY projection.name COLLATE BINARY, projection.knowledge_object_id
		LIMIT 2`)
	if err != nil {
		t.Fatalf("query selector-filtered projections: %v", err)
	}
	var selectorFiltered []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			_ = rows.Close()
			t.Fatalf("scan selector-filtered projection: %v", err)
		}
		selectorFiltered = append(selectorFiltered, name)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		t.Fatalf("iterate selector-filtered projections: %v", err)
	}
	if err := rows.Close(); err != nil {
		t.Fatalf("close selector-filtered projections: %v", err)
	}
	if len(selectorFiltered) != 2 || selectorFiltered[0] != "Alpha" || selectorFiltered[1] != "Gamma" {
		t.Fatalf("selector-filtered projections = %v", selectorFiltered)
	}

	if err := raw.QueryRow(`
		SELECT projection.name
		FROM knowledge_object_list_projections AS projection
		JOIN knowledge_object_list_projection_seals AS seal
		  USING (tenant_id, knowledge_object_id, object_version)
		JOIN knowledge_objects AS object
		  ON object.tenant_id = projection.tenant_id
		 AND object.knowledge_object_id = projection.knowledge_object_id
		 AND object.current_version = projection.object_version
		WHERE projection.tenant_id = 'tenant-a'
		  AND projection.state = 'active'
		  AND projection.object_type = 'field_extraction'
		  AND projection.sharing_scope = 'private'
		  AND (
			instr(projection.name, 'Alp') > 0
			OR instr(projection.description, 'needle') > 0
		  )
		  AND EXISTS (
			SELECT 1 FROM knowledge_object_list_selector_patterns AS pattern
			WHERE pattern.tenant_id = projection.tenant_id
			  AND pattern.knowledge_object_id = projection.knowledge_object_id
			  AND pattern.object_version = projection.object_version
			  AND pattern.dimension = 'host'
			  AND pattern.value = 'web-01'
		  )
		ORDER BY projection.name COLLATE BINARY
		LIMIT 1`).Scan(&filtered); err != nil {
		t.Fatalf("query combined-filter projection: %v", err)
	}
	if filtered != "Alpha" {
		t.Fatalf("combined-filter projection = %q", filtered)
	}
	assertNoForeignKeyViolations(t, raw)
}

func TestKnowledgeListProjectionCapacityLedgerAndLifecycle(t *testing.T) {
	t.Parallel()

	raw := openProjectionMigrationDatabase(t, "projection-lifecycle.sqlite")
	enableRecursiveProjectionTriggers(t, raw)
	seedProjectionPrerequisites(t, raw)
	insertProjectedKnowledgeObject(t, raw, projectionFixture{
		ObjectID:    "ko-life",
		Version:     1,
		Name:        "Life",
		Description: "first",
		Selectors: []projectionSelector{
			{Dimension: "source", Ordinal: 0, MatchKind: "wildcard", Value: "/var/*"},
		},
	}, 10)

	var firstBytes int
	if err := raw.QueryRow(`
		SELECT projection_bytes FROM knowledge_projection_tenant_ledgers
		WHERE tenant_id = 'tenant-a'`).Scan(&firstBytes); err != nil {
		t.Fatalf("read initial ledger: %v", err)
	}
	assertSQLFailsContaining(t, raw, "current knowledge list projection seal", `
		DELETE FROM knowledge_object_list_projection_seals
		WHERE tenant_id = 'tenant-a' AND knowledge_object_id = 'ko-life'`)
	assertSQLFailsContaining(t, raw, "ledger transition is invalid", `
		UPDATE knowledge_projection_tenant_ledgers
		SET projection_bytes = projection_bytes + 1
		WHERE tenant_id = 'tenant-a'`)
	assertSQLFailsContaining(t, raw, "cannot be deleted", `
		DELETE FROM knowledge_projection_tenant_ledgers WHERE tenant_id = 'tenant-a'`)
	assertProjectionTriggerContains(t, raw,
		"knowledge_list_projection_capacity_is_available",
		"projection_bytes <= 268435456 - NEW.projection_bytes")

	// Stage the immutable snapshot first, then publish it by moving the current
	// registry tuple. The AFTER trigger removes version 1 in dependency order.
	lifecycle, err := raw.Begin()
	if err != nil {
		t.Fatalf("begin projection lifecycle: %v", err)
	}
	insertKnowledgeVersion(t, lifecycle, "ko-life", 2, "Life", "update", 20)
	insertProjectionRows(t, lifecycle, projectionFixture{
		ObjectID:    "ko-life",
		Version:     2,
		Name:        "Life",
		Description: "second",
	})
	if _, err := lifecycle.Exec(`
		UPDATE knowledge_objects
		SET current_version = 2, updated_at_unix_micro = 20
		WHERE tenant_id = 'tenant-a' AND knowledge_object_id = 'ko-life'`); err != nil {
		_ = lifecycle.Rollback()
		t.Fatalf("publish replacement projection: %v", err)
	}
	if err := lifecycle.Commit(); err != nil {
		t.Fatalf("commit current projection replacement: %v", err)
	}
	assertIntegerQuery(t, raw, 1, `
		SELECT count(*) FROM knowledge_object_list_projections
		WHERE tenant_id = 'tenant-a' AND knowledge_object_id = 'ko-life'
		  AND object_version = 2 AND description = 'second'`)
	assertIntegerQuery(t, raw, 0, `
		SELECT count(*) FROM knowledge_object_list_projections
		WHERE tenant_id = 'tenant-a' AND knowledge_object_id = 'ko-life'
		  AND object_version = 1`)

	var secondBytes int
	if err := raw.QueryRow(`
		SELECT projection_bytes FROM knowledge_projection_tenant_ledgers
		WHERE tenant_id = 'tenant-a'`).Scan(&secondBytes); err != nil {
		t.Fatalf("read replacement ledger: %v", err)
	}
	if secondBytes != len("second") || secondBytes == firstBytes {
		t.Fatalf("replacement ledger bytes = %d, initial %d", secondBytes, firstBytes)
	}
	assertIntegerQuery(t, raw, secondBytes, `
		SELECT sum(projection_bytes) FROM knowledge_object_list_projections
		WHERE tenant_id = 'tenant-a'`)

	// Publishing a version whose projection has not been sealed fails before
	// the current tuple can move; the transaction rollback removes all staging.
	missingSeal, err := raw.Begin()
	if err != nil {
		t.Fatalf("begin missing-seal lifecycle: %v", err)
	}
	insertKnowledgeVersion(t, missingSeal, "ko-life", 3, "Life", "update", 30)
	insertProjectionParent(t, missingSeal, projectionFixture{
		ObjectID: "ko-life", Version: 3, Name: "Life", Description: "third",
	})
	if _, err := missingSeal.Exec(`
		UPDATE knowledge_objects
		SET current_version = 3, updated_at_unix_micro = 30
		WHERE tenant_id = 'tenant-a' AND knowledge_object_id = 'ko-life'`); err == nil || !strings.Contains(err.Error(), "requires exact sealed") {
		_ = missingSeal.Rollback()
		t.Fatalf("missing projection seal error = %v", err)
	}
	if err := missingSeal.Rollback(); err != nil {
		t.Fatalf("roll back missing-seal lifecycle: %v", err)
	}
	assertIntegerQuery(t, raw, 2, `
		SELECT current_version FROM knowledge_objects
		WHERE tenant_id = 'tenant-a' AND knowledge_object_id = 'ko-life'`)
	assertIntegerQuery(t, raw, 0, `
		SELECT count(*) FROM knowledge_object_versions
		WHERE tenant_id = 'tenant-a' AND knowledge_object_id = 'ko-life'
		  AND object_version = 3`)
	assertIntegerQuery(t, raw, secondBytes, `
		SELECT projection_bytes FROM knowledge_projection_tenant_ledgers
		WHERE tenant_id = 'tenant-a'`)

	// A later injected failure also restores the old sealed projection and the
	// exact ledger, despite a complete replacement having been staged.
	rollback, err := raw.Begin()
	if err != nil {
		t.Fatalf("begin rollback lifecycle: %v", err)
	}
	insertKnowledgeVersion(t, rollback, "ko-life", 3, "Life", "update", 30)
	insertProjectionRows(t, rollback, projectionFixture{
		ObjectID: "ko-life", Version: 3, Name: "Life", Description: "third",
	})
	if _, err := rollback.Exec(`
		UPDATE knowledge_projection_tenant_ledgers
		SET projection_bytes = projection_bytes + 1
		WHERE tenant_id = 'tenant-a'`); err == nil || !strings.Contains(err.Error(), "ledger transition is invalid") {
		_ = rollback.Rollback()
		t.Fatalf("injected lifecycle error = %v", err)
	}
	if err := rollback.Rollback(); err != nil {
		t.Fatalf("roll back lifecycle: %v", err)
	}
	assertIntegerQuery(t, raw, 1, `
		SELECT count(*)
		FROM knowledge_object_list_projections AS projection
		JOIN knowledge_object_list_projection_seals AS seal
		  USING (tenant_id, knowledge_object_id, object_version)
		WHERE projection.tenant_id = 'tenant-a'
		  AND projection.knowledge_object_id = 'ko-life'
		  AND projection.object_version = 2`)
	assertIntegerQuery(t, raw, secondBytes, `
		SELECT projection_bytes FROM knowledge_projection_tenant_ledgers
		WHERE tenant_id = 'tenant-a'`)
	assertNoForeignKeyViolations(t, raw)
}

func assertUnsealedProjectionRejectsDuplicateAndUnorderedSelectors(t *testing.T, db *sql.DB) {
	t.Helper()

	duplicate, err := db.Begin()
	if err != nil {
		t.Fatalf("begin duplicate-selector projection: %v", err)
	}
	insertProjectionParent(t, duplicate, projectionFixture{
		ObjectID: "ko-alpha", Version: 2, Name: "Alpha",
		Selectors: []projectionSelector{
			{Dimension: "index", Ordinal: 0, MatchKind: "exact", Value: "same"},
			{Dimension: "index", Ordinal: 1, MatchKind: "wildcard", Value: "same"},
		},
	})
	if _, err := duplicate.Exec(`
		INSERT INTO knowledge_object_list_selector_patterns (
			tenant_id, knowledge_object_id, object_version,
			dimension, ordinal, match_kind, value
		) VALUES
			('tenant-a', 'ko-alpha', 2, 'index', 0, 'exact', 'same'),
			('tenant-a', 'ko-alpha', 2, 'index', 1, 'wildcard', 'same')`); err == nil || !strings.Contains(err.Error(), "identity already exists") {
		_ = duplicate.Rollback()
		t.Fatalf("duplicate selector error = %v", err)
	}
	if err := duplicate.Rollback(); err != nil {
		t.Fatalf("roll back duplicate selector: %v", err)
	}

	unordered, err := db.Begin()
	if err != nil {
		t.Fatalf("begin unordered-selector projection: %v", err)
	}
	fixture := projectionFixture{
		ObjectID: "ko-alpha", Version: 2, Name: "Alpha",
		Selectors: []projectionSelector{
			{Dimension: "index", Ordinal: 0, MatchKind: "exact", Value: "z"},
			{Dimension: "index", Ordinal: 1, MatchKind: "exact", Value: "a"},
		},
	}
	insertProjectionParent(t, unordered, fixture)
	for _, selector := range fixture.Selectors {
		insertProjectionSelector(t, unordered, fixture, selector)
	}
	if _, err := unordered.Exec(`
		INSERT INTO knowledge_object_list_projection_seals (
			tenant_id, knowledge_object_id, object_version,
			projection_bytes, canonical_selector_bytes
		) SELECT tenant_id, knowledge_object_id, object_version,
		         projection_bytes, canonical_selector_bytes
		    FROM knowledge_object_list_projections
		   WHERE tenant_id = 'tenant-a' AND knowledge_object_id = 'ko-alpha'
		     AND object_version = 2`); err == nil || !strings.Contains(err.Error(), "incomplete") {
		_ = unordered.Rollback()
		t.Fatalf("unordered selector error = %v", err)
	}
	if err := unordered.Rollback(); err != nil {
		t.Fatalf("roll back unordered selector: %v", err)
	}

	invalid, err := db.Begin()
	if err != nil {
		t.Fatalf("begin invalid-selector projection: %v", err)
	}
	insertProjectionParent(t, invalid, projectionFixture{
		ObjectID: "ko-alpha", Version: 2, Name: "Alpha",
		Selectors: []projectionSelector{{Dimension: "host", Ordinal: 0, MatchKind: "exact", Value: "valid"}},
	})
	for _, value := range []string{" padded", "bad\nvalue"} {
		if _, err := invalid.Exec(`
			INSERT INTO knowledge_object_list_selector_patterns (
				tenant_id, knowledge_object_id, object_version,
				dimension, ordinal, match_kind, value
			) VALUES ('tenant-a', 'ko-alpha', 2, 'host', 0, 'exact', ?)`, value); err == nil {
			_ = invalid.Rollback()
			t.Fatalf("invalid selector %q unexpectedly succeeded", value)
		}
	}
	for _, query := range []string{
		`INSERT INTO knowledge_object_list_selector_patterns (
			tenant_id, knowledge_object_id, object_version,
			dimension, ordinal, match_kind, value
		) VALUES ('tenant-a', 'ko-alpha', 2, 'invalid', 0, 'exact', 'valid')`,
		`INSERT INTO knowledge_object_list_selector_patterns (
			tenant_id, knowledge_object_id, object_version,
			dimension, ordinal, match_kind, value
		) VALUES ('tenant-a', 'ko-alpha', 2, 'host', 16, 'exact', 'valid')`,
		`INSERT INTO knowledge_object_list_selector_patterns (
			tenant_id, knowledge_object_id, object_version,
			dimension, ordinal, match_kind, value
		) VALUES ('tenant-a', 'ko-alpha', 2, 'host', 0, 'regex', 'valid')`,
	} {
		if _, err := invalid.Exec(query); err == nil {
			_ = invalid.Rollback()
			t.Fatalf("invalid selector shape unexpectedly succeeded: %s", query)
		}
	}
	if err := invalid.Rollback(); err != nil {
		t.Fatalf("roll back invalid selector: %v", err)
	}
}

func assertProjectionPublicationGuards(t *testing.T, db *sql.DB, ledgerBefore int) {
	t.Helper()

	missing, err := db.Begin()
	if err != nil {
		t.Fatalf("begin missing-projection publication: %v", err)
	}
	insertKnowledgeVersion(t, missing, "ko-registry-missing", 1, "Missing", "create", 13)
	if _, err := missing.Exec(`
		INSERT INTO knowledge_objects (
			tenant_id, knowledge_object_id, current_version,
			app_id, owner_id, object_type, name, sharing_scope, state,
			definition_digest, created_at_unix_micro, updated_at_unix_micro
		) VALUES (
			'tenant-a', 'ko-registry-missing', 1, ?, 'owner-a',
			'field_extraction', 'Missing', 'private', 'active',
			zeroblob(32), 13, 13
		)`, knowledgeMigrationTestAppID); err == nil || !strings.Contains(err.Error(), "requires exact sealed") {
		_ = missing.Rollback()
		t.Fatalf("registry insert without projection error = %v", err)
	}
	if err := missing.Rollback(); err != nil {
		t.Fatalf("roll back missing-projection publication: %v", err)
	}

	mismatch, err := db.Begin()
	if err != nil {
		t.Fatalf("begin mismatched-projection publication: %v", err)
	}
	insertKnowledgeVersion(t, mismatch, "ko-registry-mismatch", 1, "Registry", "create", 14)
	insertProjectionRows(t, mismatch, projectionFixture{
		ObjectID: "ko-registry-mismatch", Version: 1, Name: "Projection",
	})
	if _, err := mismatch.Exec(`
		INSERT INTO knowledge_objects (
			tenant_id, knowledge_object_id, current_version,
			app_id, owner_id, object_type, name, sharing_scope, state,
			definition_digest, created_at_unix_micro, updated_at_unix_micro
		) VALUES (
			'tenant-a', 'ko-registry-mismatch', 1, ?, 'owner-a',
			'field_extraction', 'Registry', 'private', 'active',
			zeroblob(32), 14, 14
		)`, knowledgeMigrationTestAppID); err == nil || !strings.Contains(err.Error(), "requires exact sealed") {
		_ = mismatch.Rollback()
		t.Fatalf("registry insert with mismatched projection error = %v", err)
	}
	if err := mismatch.Rollback(); err != nil {
		t.Fatalf("roll back mismatched-projection publication: %v", err)
	}

	orphan, err := db.Begin()
	if err != nil {
		t.Fatalf("begin orphaned sealed projection: %v", err)
	}
	insertProjectionRows(t, orphan, projectionFixture{
		ObjectID: "ko-orphan-projection", Version: 1, Name: "Orphan",
		Description: "temporary charge",
	})
	if err := orphan.Commit(); err == nil || !strings.Contains(err.Error(), "FOREIGN KEY") {
		_ = orphan.Rollback()
		t.Fatalf("orphaned sealed projection commit error = %v", err)
	}
	_ = orphan.Rollback()

	for _, objectID := range []string{
		"ko-registry-missing", "ko-registry-mismatch", "ko-orphan-projection",
	} {
		assertIntegerQuery(t, db, 0, `
			SELECT count(*) FROM knowledge_object_list_projections
			WHERE tenant_id = 'tenant-a' AND knowledge_object_id = ?`, objectID)
		assertIntegerQuery(t, db, 0, `
			SELECT count(*) FROM knowledge_objects
			WHERE tenant_id = 'tenant-a' AND knowledge_object_id = ?`, objectID)
	}
	assertIntegerQuery(t, db, ledgerBefore, `
		SELECT projection_bytes FROM knowledge_projection_tenant_ledgers
		WHERE tenant_id = 'tenant-a'`)
}

type projectionFixture struct {
	ObjectID    string
	Version     int
	Name        string
	Description string
	Selectors   []projectionSelector
}

type projectionSelector struct {
	Dimension string
	Ordinal   int
	MatchKind string
	Value     string
}

type projectionExecer interface {
	Exec(query string, args ...any) (sql.Result, error)
}

func openProjectionMigrationDatabase(t *testing.T, name string) *sql.DB {
	t.Helper()
	raw := openKnowledgeMigrationTestDB(t, name)
	if err := ApplyMigrations(context.Background(), raw, migrationsBefore(t, "0026_")); err != nil {
		t.Fatalf("apply through projection migration: %v", err)
	}
	return raw
}

func enableRecursiveProjectionTriggers(t *testing.T, db *sql.DB) {
	t.Helper()
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	if _, err := db.Exec(`PRAGMA recursive_triggers = ON`); err != nil {
		t.Fatalf("enable recursive projection triggers: %v", err)
	}
	assertIntegerQuery(t, db, 1, `PRAGMA recursive_triggers`)
}

func seedProjectionPrerequisites(t *testing.T, db *sql.DB) {
	t.Helper()
	seedKnowledgeMigrationApp(t, db)
	if _, err := db.Exec(`
		INSERT INTO knowledge_catalog_tenants (tenant_id) VALUES ('tenant-a');
		INSERT INTO knowledge_definition_blobs (
			tenant_id, definition_digest, definition_proto,
			definition_bytes, created_at_unix_micro
		) VALUES ('tenant-a', zeroblob(32), X'01', 1, 10);
		INSERT INTO knowledge_projection_tenant_ledgers (tenant_id)
		VALUES ('tenant-a')`); err != nil {
		t.Fatalf("seed projection prerequisites: %v", err)
	}
}

func insertProjectedKnowledgeObject(t *testing.T, db *sql.DB, fixture projectionFixture, timestamp int64) {
	t.Helper()
	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("begin projected object insert: %v", err)
	}
	insertKnowledgeVersion(t, tx, fixture.ObjectID, fixture.Version, fixture.Name, "create", timestamp)
	insertProjectionRows(t, tx, fixture)
	if _, err := tx.Exec(`
		INSERT INTO knowledge_objects (
			tenant_id, knowledge_object_id, current_version,
			app_id, owner_id, object_type, name, sharing_scope, state,
			definition_digest, created_at_unix_micro, updated_at_unix_micro
		) VALUES (
			'tenant-a', ?, ?, ?, 'owner-a', 'field_extraction', ?,
			'private', 'active', zeroblob(32), ?, ?
		)`, fixture.ObjectID, fixture.Version, knowledgeMigrationTestAppID,
		fixture.Name, timestamp, timestamp); err != nil {
		_ = tx.Rollback()
		t.Fatalf("insert projected object %s: %v", fixture.ObjectID, err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit projected object %s: %v", fixture.ObjectID, err)
	}
}

func insertKnowledgeVersion(t *testing.T, exec projectionExecer, objectID string, version int, name, mutation string, timestamp int64) {
	t.Helper()
	if _, err := exec.Exec(`
		INSERT INTO knowledge_object_versions (
			tenant_id, knowledge_object_id, object_version,
			app_id, owner_id, object_type, name, sharing_scope, state,
			definition_digest, dependency_count, mutation_kind,
			created_at_unix_micro
		) VALUES (
			'tenant-a', ?, ?, ?, 'owner-a', 'field_extraction', ?,
			'private', 'active', zeroblob(32), 0, ?, ?
		);
		INSERT INTO knowledge_object_dependency_seals (
			tenant_id, knowledge_object_id, object_version, dependency_count
		) VALUES ('tenant-a', ?, ?, 0)`,
		objectID, version, knowledgeMigrationTestAppID, name, mutation, timestamp,
		objectID, version); err != nil {
		t.Fatalf("insert object version %s/%d: %v", objectID, version, err)
	}
}

func insertProjectionRows(t *testing.T, exec projectionExecer, fixture projectionFixture) {
	t.Helper()
	insertProjectionParent(t, exec, fixture)
	for _, selector := range fixture.Selectors {
		insertProjectionSelector(t, exec, fixture, selector)
	}
	if _, err := exec.Exec(`
		INSERT INTO knowledge_object_list_projection_seals (
			tenant_id, knowledge_object_id, object_version,
			projection_bytes, canonical_selector_bytes
		) SELECT tenant_id, knowledge_object_id, object_version,
		         projection_bytes, canonical_selector_bytes
		    FROM knowledge_object_list_projections
		   WHERE tenant_id = 'tenant-a' AND knowledge_object_id = ?
		     AND object_version = ?`, fixture.ObjectID, fixture.Version); err != nil {
		t.Fatalf("seal projection %s/%d: %v", fixture.ObjectID, fixture.Version, err)
	}
}

func insertProjectionParent(t *testing.T, exec projectionExecer, fixture projectionFixture) {
	t.Helper()
	counts := map[string]int{"index": 0, "host": 0, "source": 0, "sourcetype": 0}
	selectorBytes := 0
	for _, selector := range fixture.Selectors {
		counts[selector.Dimension]++
		selectorBytes += len([]byte(selector.Value))
	}
	canonicalSelectorBytes := 46 + 4*len(fixture.Selectors) + selectorBytes
	descriptionPresent := 0
	if fixture.Description != "" {
		descriptionPresent = 1
	}
	if _, err := exec.Exec(`
		INSERT INTO knowledge_object_list_projections (
			tenant_id, knowledge_object_id, object_version,
			app_id, owner_id, object_type, name, sharing_scope, state,
			description_present, description,
			index_selector_count, host_selector_count,
			source_selector_count, sourcetype_selector_count,
			selector_value_bytes, canonical_selector_bytes
		) VALUES (
			'tenant-a', ?, ?, ?, 'owner-a', 'field_extraction', ?,
			'private', 'active', ?, ?, ?, ?, ?, ?, ?, ?
		)`,
		fixture.ObjectID, fixture.Version, knowledgeMigrationTestAppID, fixture.Name,
		descriptionPresent, fixture.Description,
		counts["index"], counts["host"], counts["source"], counts["sourcetype"],
		selectorBytes, canonicalSelectorBytes); err != nil {
		t.Fatalf("insert projection %s/%d: %v", fixture.ObjectID, fixture.Version, err)
	}
}

func insertProjectionSelector(t *testing.T, exec projectionExecer, fixture projectionFixture, selector projectionSelector) {
	t.Helper()
	if _, err := exec.Exec(`
		INSERT INTO knowledge_object_list_selector_patterns (
			tenant_id, knowledge_object_id, object_version,
			dimension, ordinal, match_kind, value
		) VALUES ('tenant-a', ?, ?, ?, ?, ?, ?)`,
		fixture.ObjectID, fixture.Version, selector.Dimension, selector.Ordinal,
		selector.MatchKind, selector.Value); err != nil {
		t.Fatalf("insert selector for %s/%d: %v", fixture.ObjectID, fixture.Version, err)
	}
}

func assertProjectionTriggerContains(t *testing.T, db *sql.DB, trigger, fragment string) {
	t.Helper()
	var sqlText string
	if err := db.QueryRow(`
		SELECT sql FROM sqlite_schema WHERE type = 'trigger' AND name = ?`, trigger,
	).Scan(&sqlText); err != nil {
		t.Fatalf("read trigger %s: %v", trigger, err)
	}
	if !strings.Contains(sqlText, fragment) {
		t.Fatalf("trigger %s does not contain %q: %s", trigger, fragment, sqlText)
	}
}

func assertProjectionTriggerFragmentCount(
	t *testing.T,
	db *sql.DB,
	trigger string,
	fragment string,
	want int,
) {
	t.Helper()
	var sqlText string
	if err := db.QueryRow(`
		SELECT sql FROM sqlite_schema WHERE type = 'trigger' AND name = ?`, trigger,
	).Scan(&sqlText); err != nil {
		t.Fatalf("read trigger %s: %v", trigger, err)
	}
	if got := strings.Count(sqlText, fragment); got != want {
		t.Fatalf(
			"trigger %s contains %q %d times, want %d: %s",
			trigger,
			fragment,
			got,
			want,
			sqlText,
		)
	}
}
