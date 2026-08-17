package control

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"io/fs"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Suhaibinator/open-splunk/migrations"
	moderncsqlite "modernc.org/sqlite"
)

const knowledgeIndexAdmissionDriversMigration = "0034_knowledge_index_admission_drivers.sql"
const knowledgeIndexAdmissionDriversMigrationSHA256 = "300b31fe8f6b2d13023c5c0acf1c989c1b92f565c26296d10272d9d1fbd6e056"

const knowledge0034DriverVisitFunction = "ko_test_0034_driver_visit"

var (
	knowledge0034DriverVisitCounters sync.Map
	knowledge0034DriverVisitToken    atomic.Uint64
)

func init() {
	moderncsqlite.MustRegisterScalarFunction(
		knowledge0034DriverVisitFunction,
		2,
		func(_ *moderncsqlite.FunctionContext, arguments []driver.Value) (driver.Value, error) {
			if len(arguments) != 2 {
				return nil, fmt.Errorf(
					"%s received %d arguments, want 2",
					knowledge0034DriverVisitFunction,
					len(arguments),
				)
			}
			token, ok := arguments[0].(string)
			if !ok || token == "" {
				return nil, fmt.Errorf("%s received an invalid token", knowledge0034DriverVisitFunction)
			}
			value, found := knowledge0034DriverVisitCounters.Load(token)
			if !found {
				return nil, fmt.Errorf("%s received an unknown token", knowledge0034DriverVisitFunction)
			}
			counter, ok := value.(*atomic.Int64)
			if !ok || counter == nil {
				return nil, fmt.Errorf("%s counter authority is invalid", knowledge0034DriverVisitFunction)
			}
			counter.Add(1)
			return int64(1), nil
		},
	)
}

func TestKnowledgeIndexAdmissionDriversMigrationPinsFreshSchemaAndChecksum(t *testing.T) {
	t.Parallel()

	raw := openKnowledgeMigrationTestDB(t, "knowledge-index-admission-fresh.sqlite")
	if err := ApplyMigrations(context.Background(), raw, migrations.SQLite()); err != nil {
		t.Fatalf("apply migrations: %v", err)
	}

	contents, err := fs.ReadFile(migrations.SQLite(), knowledgeIndexAdmissionDriversMigration)
	if err != nil {
		t.Fatalf("read embedded migration: %v", err)
	}
	wantChecksum := sha256.Sum256(contents)
	if got := fmt.Sprintf("%x", wantChecksum); got != knowledgeIndexAdmissionDriversMigrationSHA256 {
		t.Fatalf("migration SHA-256 = %s, want %s", got, knowledgeIndexAdmissionDriversMigrationSHA256)
	}
	var version int
	var name string
	var checksum []byte
	if err := raw.QueryRowContext(t.Context(), `
		SELECT version, name, checksum
		FROM schema_migrations
		WHERE version = 34`).Scan(&version, &name, &checksum); err != nil {
		t.Fatalf("read migration authority: %v", err)
	}
	if version != 34 || name != knowledgeIndexAdmissionDriversMigration ||
		!bytes.Equal(checksum, wantChecksum[:]) {
		t.Fatalf(
			"migration authority = (%d, %q, %x), want (34, %q, %x)",
			version,
			name,
			checksum,
			knowledgeIndexAdmissionDriversMigration,
			wantChecksum,
		)
	}
	assertIntegerQuery(t, raw, 39, `SELECT count(*) FROM schema_migrations`)

	indexes := []struct {
		table     string
		name      string
		columns   []string
		predicate string
	}{
		{
			table:     "knowledge_catalog_tenants",
			name:      "knowledge_catalog_tenants_nonempty_active_idx",
			columns:   []string{"tenant_id", "catalog_revision", "active_object_count"},
			predicate: "WHERE active_object_count > 0",
		},
		{
			table:     "knowledge_objects",
			name:      "knowledge_objects_active_tenant_idx",
			columns:   []string{"tenant_id", "knowledge_object_id", "current_version"},
			predicate: "WHERE state = 'active'",
		},
	}
	for _, index := range indexes {
		t.Run(index.name, func(t *testing.T) {
			var unique, partial int
			if err := raw.QueryRowContext(t.Context(), `
				SELECT "unique", partial
				FROM pragma_index_list(?)
				WHERE name = ?`, index.table, index.name).Scan(&unique, &partial); err != nil {
				t.Fatalf("read index flags: %v", err)
			}
			if unique != 0 || partial != 1 {
				t.Fatalf("index flags = unique:%d partial:%d, want 0/1", unique, partial)
			}
			assertKnowledge0034IndexColumns(t, raw, index.name, index.columns)

			var statement string
			if err := raw.QueryRowContext(t.Context(), `
				SELECT sql FROM sqlite_schema
				WHERE type = 'index' AND name = ?`, index.name).Scan(&statement); err != nil {
				t.Fatalf("read index SQL: %v", err)
			}
			if !strings.Contains(statement, index.predicate) {
				t.Fatalf("index SQL lacks exact predicate %q:\n%s", index.predicate, statement)
			}
		})
	}
	assertKnowledge0034DriverPlans(t, raw)
	assertNoForeignKeyViolations(t, raw)
}

func TestKnowledgeIndexAdmissionDriversMigrationUpgradesWithoutChangingCatalogState(t *testing.T) {
	t.Parallel()

	raw := openKnowledgeMigrationTestDB(t, "knowledge-index-admission-upgrade.sqlite")
	if err := ApplyMigrations(context.Background(), raw, migrationsBefore(t, "0034_")); err != nil {
		t.Fatalf("apply through migration 0033: %v", err)
	}
	seedKnowledgeStatePrerequisites(t, raw)
	insertKnowledgeStateVersion(t, raw, stateVersionFixture{
		objectID: "ko-0034-active", version: 1, state: "active", mutation: "create", timestamp: 10,
	})
	insertKnowledgeStateVersion(t, raw, stateVersionFixture{
		objectID: "ko-0034-draft", version: 1, state: "draft", mutation: "create", timestamp: 20,
	})
	if _, err := raw.ExecContext(t.Context(), `INSERT INTO knowledge_catalog_tenants (tenant_id) VALUES ('tenant-empty')`); err != nil {
		t.Fatalf("seed empty tenant ledger: %v", err)
	}
	activeToken := readKnowledgeRevisionToken(t, raw, "tenant-a", 0)
	emptyToken := readKnowledgeRevisionToken(t, raw, "tenant-empty", 0)
	assertIntegerQuery(t, raw, 33, `SELECT count(*) FROM schema_migrations`)
	assertIntegerQuery(t, raw, 2, `SELECT count(*) FROM knowledge_objects`)
	assertIntegerQuery(t, raw, 1, `
		SELECT active_object_count FROM knowledge_catalog_tenants
		WHERE tenant_id = 'tenant-a'`)

	if err := ApplyMigrations(context.Background(), raw, migrations.SQLite()); err != nil {
		t.Fatalf("apply migration 0034: %v", err)
	}
	assertIntegerQuery(t, raw, 39, `SELECT count(*) FROM schema_migrations`)
	assertIntegerQuery(t, raw, 1, `
		SELECT count(*)
		FROM knowledge_catalog_tenants INDEXED BY knowledge_catalog_tenants_nonempty_active_idx
		WHERE active_object_count > 0`)
	assertIntegerQuery(t, raw, 1, `
		SELECT count(*)
		FROM knowledge_objects INDEXED BY knowledge_objects_active_tenant_idx
		WHERE state = 'active'`)
	assertIntegerQuery(t, raw, 1, `
		SELECT count(*) FROM knowledge_objects
		WHERE knowledge_object_id = 'ko-0034-draft' AND state = 'draft'`)
	if got := readKnowledgeRevisionToken(t, raw, "tenant-a", 0); !bytes.Equal(got, activeToken) {
		t.Fatal("migration changed the nonempty tenant revision token")
	}
	if got := readKnowledgeRevisionToken(t, raw, "tenant-empty", 0); !bytes.Equal(got, emptyToken) {
		t.Fatal("migration changed the empty tenant revision token")
	}
	assertKnowledge0034DriverPlans(t, raw)
	assertNoForeignKeyViolations(t, raw)
}

func TestKnowledgeIndexAdmissionDriversMigrationRollsBackBothIndexes(t *testing.T) {
	t.Parallel()

	raw := openKnowledgeMigrationTestDB(t, "knowledge-index-admission-rollback.sqlite")
	if err := ApplyMigrations(context.Background(), raw, migrationsBefore(t, "0034_")); err != nil {
		t.Fatalf("apply through migration 0033: %v", err)
	}
	if _, err := raw.ExecContext(t.Context(), `
		CREATE VIEW knowledge_objects_active_tenant_idx AS
		SELECT tenant_id, knowledge_object_id, current_version
		FROM knowledge_objects WHERE 0`); err != nil {
		t.Fatalf("create migration conflict: %v", err)
	}

	err := ApplyMigrations(context.Background(), raw, migrations.SQLite())
	if err == nil || !strings.Contains(err.Error(), "knowledge_objects_active_tenant_idx") {
		t.Fatalf("conflicting migration error = %v", err)
	}
	assertIntegerQuery(t, raw, 33, `SELECT count(*) FROM schema_migrations`)
	assertIntegerQuery(t, raw, 0, `
		SELECT count(*) FROM sqlite_schema
		WHERE type = 'index' AND name IN (
			'knowledge_catalog_tenants_nonempty_active_idx',
			'knowledge_objects_active_tenant_idx'
		)`)
	assertIntegerQuery(t, raw, 1, `
		SELECT count(*) FROM sqlite_schema
		WHERE type = 'view' AND name = 'knowledge_objects_active_tenant_idx'`)

	if _, err := raw.ExecContext(t.Context(), `DROP VIEW knowledge_objects_active_tenant_idx`); err != nil {
		t.Fatalf("drop migration conflict: %v", err)
	}
	if err := ApplyMigrations(context.Background(), raw, migrations.SQLite()); err != nil {
		t.Fatalf("apply migration after removing conflict: %v", err)
	}
	assertIntegerQuery(t, raw, 39, `SELECT count(*) FROM schema_migrations`)
	assertIntegerQuery(t, raw, 2, `
		SELECT count(*) FROM sqlite_schema
		WHERE type = 'index' AND name IN (
			'knowledge_catalog_tenants_nonempty_active_idx',
			'knowledge_objects_active_tenant_idx'
		)`)
}

func TestKnowledgeIndexAdmissionDriversIgnoreEmptyAndInactiveDecoys(t *testing.T) {
	raw := openKnowledgeMigrationTestDB(t, "knowledge-index-admission-progress.sqlite")
	if err := ApplyMigrations(context.Background(), raw, migrations.SQLite()); err != nil {
		t.Fatalf("apply migrations: %v", err)
	}
	seedKnowledgeStatePrerequisites(t, raw)
	insertKnowledgeStateVersion(t, raw, stateVersionFixture{
		objectID: "ko-0034-progress-active", version: 1, state: "active", mutation: "create", timestamp: 10,
	})

	const decoyCount = 4096
	if _, err := raw.ExecContext(t.Context(), `
		WITH RECURSIVE sequence(n) AS (
			VALUES (1)
			UNION ALL
			SELECT n + 1 FROM sequence WHERE n < ?
		)
		INSERT INTO knowledge_catalog_tenants (tenant_id)
		SELECT 'zz-empty-' || printf('%05d', n) FROM sequence`, decoyCount); err != nil {
		t.Fatalf("insert empty tenant decoys: %v", err)
	}
	assertIntegerQuery(t, raw, decoyCount, `
		SELECT count(*) FROM knowledge_catalog_tenants
		WHERE tenant_id LIKE 'zz-empty-%' AND active_object_count = 0`)

	raw.SetMaxOpenConns(1)
	raw.SetMaxIdleConns(1)
	conn, err := raw.Conn(context.Background())
	if err != nil {
		t.Fatalf("acquire progress connection: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	if _, err := conn.ExecContext(context.Background(), `PRAGMA foreign_keys = OFF`); err != nil {
		t.Fatalf("disable foreign keys for inactive corruption fixture: %v", err)
	}
	dropKnowledge0034RegistryTriggers(t, conn)
	if _, err := conn.ExecContext(context.Background(), `
		WITH RECURSIVE sequence(n) AS (
			VALUES (1)
			UNION ALL
			SELECT n + 1 FROM sequence WHERE n < ?
		)
		INSERT INTO knowledge_objects (
			tenant_id, knowledge_object_id, current_version,
			app_id, owner_id, object_type, name, sharing_scope, state,
			definition_digest, created_at_unix_micro, updated_at_unix_micro,
			disabled_at_unix_micro
		)
		SELECT
			'zz-inactive-' || printf('%05d', n),
			'ko-inactive-' || printf('%05d', n),
			1, ?, 'owner-decoy', 'field_extraction', 'inactive-decoy',
			'private', 'disabled', zeroblob(32), 1, 1, 1
		FROM sequence`, decoyCount, knowledgeMigrationTestAppID); err != nil {
		t.Fatalf("insert inactive registry decoys: %v", err)
	}
	assertIntegerQueryOnConn(t, conn, decoyCount, `
		SELECT count(*) FROM knowledge_objects WHERE state = 'disabled'`)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	tenantToken, tenantVisits := newKnowledge0034DriverVisitCounter(t)
	rows, err := conn.QueryContext(ctx, `
		SELECT `+knowledge0034DriverVisitFunction+`(?, tenant_id),
		       tenant_id, catalog_revision, active_object_count
		FROM knowledge_catalog_tenants
		     INDEXED BY knowledge_catalog_tenants_nonempty_active_idx
		WHERE active_object_count > 0
		ORDER BY tenant_id
		LIMIT 4097`, tenantToken)
	if err != nil {
		t.Fatalf("query nonempty tenant driver: %v", err)
	}
	var tenantRows int
	for rows.Next() {
		var visited, revision, activeCount int64
		var tenantID string
		if err := rows.Scan(&visited, &tenantID, &revision, &activeCount); err != nil {
			_ = rows.Close()
			t.Fatalf("scan nonempty tenant driver: %v", err)
		}
		if visited != 1 || tenantID != "tenant-a" || revision != 0 || activeCount != 1 {
			_ = rows.Close()
			t.Fatalf(
				"nonempty tenant row = (%d, %q, %d, %d)",
				visited,
				tenantID,
				revision,
				activeCount,
			)
		}
		tenantRows++
	}
	if err := rows.Close(); err != nil {
		t.Fatalf("close nonempty tenant driver: %v", err)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate nonempty tenant driver: %v", err)
	}
	if tenantRows != 1 || tenantVisits.Load() != 1 {
		t.Fatalf("nonempty tenant driver rows/visits = %d/%d, want 1/1", tenantRows, tenantVisits.Load())
	}

	objectToken, objectVisits := newKnowledge0034DriverVisitCounter(t)
	rows, err = conn.QueryContext(ctx, `
		SELECT `+knowledge0034DriverVisitFunction+`(?, knowledge_object_id),
		       tenant_id, knowledge_object_id, current_version
		FROM knowledge_objects INDEXED BY knowledge_objects_active_tenant_idx
		WHERE state = 'active'
		ORDER BY tenant_id, knowledge_object_id
		LIMIT 4097`, objectToken)
	if err != nil {
		t.Fatalf("query ACTIVE registry driver: %v", err)
	}
	var objectRows int
	for rows.Next() {
		var visited, version int64
		var tenantID, objectID string
		if err := rows.Scan(&visited, &tenantID, &objectID, &version); err != nil {
			_ = rows.Close()
			t.Fatalf("scan ACTIVE registry driver: %v", err)
		}
		if visited != 1 || tenantID != "tenant-a" ||
			objectID != "ko-0034-progress-active" || version != 1 {
			_ = rows.Close()
			t.Fatalf(
				"ACTIVE registry row = (%d, %q, %q, %d)",
				visited,
				tenantID,
				objectID,
				version,
			)
		}
		objectRows++
	}
	if err := rows.Close(); err != nil {
		t.Fatalf("close ACTIVE registry driver: %v", err)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate ACTIVE registry driver: %v", err)
	}
	if objectRows != 1 || objectVisits.Load() != 1 {
		t.Fatalf("ACTIVE registry driver rows/visits = %d/%d, want 1/1", objectRows, objectVisits.Load())
	}
	if ctx.Err() != nil {
		t.Fatalf("cross-tenant admission drivers did not make bounded progress: %v", ctx.Err())
	}
}

func assertKnowledge0034DriverPlans(t *testing.T, db *sql.DB) {
	t.Helper()
	tests := []struct {
		name      string
		query     string
		required  string
		forbidden []string
	}{
		{
			name: "nonempty tenant ledger",
			query: `EXPLAIN QUERY PLAN
				SELECT tenant_id, catalog_revision, active_object_count
				FROM knowledge_catalog_tenants
				     INDEXED BY knowledge_catalog_tenants_nonempty_active_idx
				WHERE active_object_count > 0
				ORDER BY tenant_id
				LIMIT 4097`,
			required: "USING COVERING INDEX knowledge_catalog_tenants_nonempty_active_idx",
			forbidden: []string{
				"SCAN knowledge_catalog_tenants\n",
				"USE TEMP B-TREE",
			},
		},
		{
			name: "ACTIVE registry",
			query: `EXPLAIN QUERY PLAN
				SELECT tenant_id, knowledge_object_id, current_version
				FROM knowledge_objects INDEXED BY knowledge_objects_active_tenant_idx
				WHERE state = 'active'
				ORDER BY tenant_id, knowledge_object_id
				LIMIT 4097`,
			required: "USING COVERING INDEX knowledge_objects_active_tenant_idx",
			forbidden: []string{
				"SCAN knowledge_objects\n",
				"USE TEMP B-TREE",
			},
		},
	}
	for _, test := range tests {
		t.Run("query plan/"+test.name, func(t *testing.T) {
			details := knowledge0034ExplainDetails(t, db, test.query)
			joined := strings.Join(details, "\n")
			if !strings.Contains(joined, test.required) {
				t.Fatalf("query plan lacks %q:\n%s", test.required, joined)
			}
			for _, forbidden := range test.forbidden {
				if strings.Contains(joined, forbidden) {
					t.Fatalf("query plan contains forbidden %q:\n%s", forbidden, joined)
				}
			}
		})
	}
}

func knowledge0034ExplainDetails(t *testing.T, db *sql.DB, query string) []string {
	t.Helper()
	rows, err := db.QueryContext(t.Context(), query)
	if err != nil {
		t.Fatalf("explain query: %v", err)
	}
	defer rows.Close()
	var details []string
	for rows.Next() {
		var id, parent, unused int
		var detail string
		if err := rows.Scan(&id, &parent, &unused, &detail); err != nil {
			t.Fatalf("scan query plan: %v", err)
		}
		details = append(details, detail)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate query plan: %v", err)
	}
	return details
}

func assertKnowledge0034IndexColumns(t *testing.T, db *sql.DB, name string, want []string) {
	t.Helper()
	rows, err := db.QueryContext(t.Context(), `SELECT name FROM pragma_index_info(?) ORDER BY seqno`, name)
	if err != nil {
		t.Fatalf("read index %s columns: %v", name, err)
	}
	defer rows.Close()
	var got []string
	for rows.Next() {
		var column string
		if err := rows.Scan(&column); err != nil {
			t.Fatalf("scan index %s column: %v", name, err)
		}
		got = append(got, column)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate index %s columns: %v", name, err)
	}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("index %s columns = %v, want %v", name, got, want)
	}
}

func newKnowledge0034DriverVisitCounter(t *testing.T) (string, *atomic.Int64) {
	t.Helper()
	token := fmt.Sprintf("knowledge-0034-driver-%d", knowledge0034DriverVisitToken.Add(1))
	counter := &atomic.Int64{}
	knowledge0034DriverVisitCounters.Store(token, counter)
	t.Cleanup(func() { knowledge0034DriverVisitCounters.Delete(token) })
	return token, counter
}

func dropKnowledge0034RegistryTriggers(t *testing.T, conn *sql.Conn) {
	t.Helper()
	rows, err := conn.QueryContext(context.Background(), `
		SELECT name FROM sqlite_schema
		WHERE type = 'trigger' AND tbl_name = 'knowledge_objects'`)
	if err != nil {
		t.Fatalf("list knowledge registry triggers: %v", err)
	}
	var names []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			_ = rows.Close()
			t.Fatalf("scan knowledge registry trigger: %v", err)
		}
		names = append(names, name)
	}
	if err := rows.Close(); err != nil {
		t.Fatalf("close knowledge registry trigger list: %v", err)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate knowledge registry triggers: %v", err)
	}
	if len(names) == 0 {
		t.Fatal("knowledge registry trigger list is empty")
	}
	for _, name := range names {
		quoted := `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
		if _, err := conn.ExecContext(context.Background(), "DROP TRIGGER "+quoted); err != nil {
			t.Fatalf("drop knowledge registry trigger %s: %v", name, err)
		}
	}
}

func assertIntegerQueryOnConn(
	t *testing.T,
	conn *sql.Conn,
	want int,
	query string,
	args ...any,
) {
	t.Helper()
	var got int
	if err := conn.QueryRowContext(context.Background(), query, args...).Scan(&got); err != nil {
		t.Fatalf("integer connection query: %v", err)
	}
	if got != want {
		t.Fatalf("integer connection query = %d, want %d", got, want)
	}
}
