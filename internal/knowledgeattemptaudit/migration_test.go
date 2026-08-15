package knowledgeattemptaudit

import (
	"context"
	"database/sql"
	"path/filepath"
	"reflect"
	"regexp"
	"sort"
	"strings"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/testsupport"
	"github.com/Suhaibinator/open-splunk/migrations"
	_ "modernc.org/sqlite"
)

func TestMigrationFreshSchemaIsExactAndBounded(t *testing.T) {
	t.Parallel()
	database := openTestDatabase(t)

	var version int
	if err := database.SQLDB().QueryRowContext(t.Context(), `SELECT MAX(version) FROM schema_migrations`).Scan(&version); err != nil {
		t.Fatal(err)
	}
	if version < 28 {
		t.Fatalf("latest migration = %d, want at least 28", version)
	}
	rows, err := database.SQLDB().QueryContext(t.Context(), `
		SELECT name FROM pragma_table_info('knowledge_attempt_audit_events') ORDER BY cid
	`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var columns []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			t.Fatal(err)
		}
		columns = append(columns, name)
	}
	want := []string{
		"tenant_id", "sequence", "occurred_at_unix_micro", "actor_kind",
		"actor_id", "actor_role", "action", "result", "reason",
		"app_id", "knowledge_object_id", "object_type", "object_version",
		"sharing_scope",
	}
	if strings.Join(columns, ",") != strings.Join(want, ",") {
		t.Fatalf("columns = %v, want %v", columns, want)
	}
	var strict, withoutRowID int
	if err := database.SQLDB().QueryRowContext(t.Context(), `
		SELECT strict, wr FROM pragma_table_list
		WHERE schema = 'main' AND name = 'knowledge_attempt_audit_events'
	`).Scan(&strict, &withoutRowID); err != nil {
		t.Fatal(err)
	}
	if strict != 1 || withoutRowID != 1 {
		t.Fatalf("event table shape = strict %d without-rowid %d", strict, withoutRowID)
	}
	var tableSQL string
	if err := database.SQLDB().QueryRowContext(t.Context(), `
		SELECT sql FROM sqlite_schema
		WHERE type = 'table' AND name = 'knowledge_attempt_audit_events'
	`).Scan(&tableSQL); err != nil {
		t.Fatal(err)
	}
	for _, action := range []string{"'get'", "'list'", "'dependencies'", "'dependents'"} {
		if !strings.Contains(tableSQL, action) {
			t.Errorf("event table CHECK does not contain action %s", action)
		}
	}
	var triggerCount int
	if err := database.SQLDB().QueryRowContext(t.Context(), `
		SELECT COUNT(*) FROM sqlite_schema
		WHERE type = 'trigger' AND name LIKE 'knowledge_attempt_audit_%'
	`).Scan(&triggerCount); err != nil {
		t.Fatal(err)
	}
	if triggerCount != 10 {
		t.Fatalf("trigger count = %d, want 10", triggerCount)
	}
	if _, err := database.SQLDB().ExecContext(t.Context(), `
		INSERT INTO knowledge_attempt_audit_tenant_state (
			tenant_id, first_sequence, next_sequence, retained_count
		) VALUES ('bad-count', 1, 100003, 100002)
	`); err == nil {
		t.Fatal("over-cap retained count was accepted")
	}
}

func TestMigrationUpgradesVersion26Database(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	raw := openRawMigrationDB(t, filepath.Join(t.TempDir(), "upgrade.db"))
	before := testsupport.SQLiteMigrationsBefore(t, "0027_")
	if err := control.ApplyMigrations(ctx, raw, before); err != nil {
		t.Fatalf("ApplyMigrations(before 0027): %v", err)
	}
	if _, err := raw.ExecContext(t.Context(), `INSERT INTO app_catalog_revisions (tenant_id, revision) VALUES ('tenant-existing', 1)`); err != nil {
		t.Fatalf("seed legacy control state: %v", err)
	}
	if err := control.ApplyMigrations(ctx, raw, migrations.SQLite()); err != nil {
		t.Fatalf("ApplyMigrations(upgrade): %v", err)
	}
	var existingRevision, latest int
	if err := raw.QueryRowContext(t.Context(), `SELECT revision FROM app_catalog_revisions WHERE tenant_id = 'tenant-existing'`).Scan(&existingRevision); err != nil {
		t.Fatal(err)
	}
	if err := raw.QueryRowContext(t.Context(), `SELECT MAX(version) FROM schema_migrations`).Scan(&latest); err != nil {
		t.Fatal(err)
	}
	if existingRevision != 1 || latest < 28 {
		t.Fatalf("upgrade state = revision %d, latest %d", existingRevision, latest)
	}
	if _, err := raw.ExecContext(t.Context(), `
		INSERT INTO knowledge_attempt_audit_tenant_state (
			tenant_id, first_sequence, next_sequence, retained_count
		) VALUES ('tenant-new', 1, 1, 0)
	`); err != nil {
		t.Fatalf("new schema is unusable: %v", err)
	}
}

func TestMigrationUpgradesVersion27JournalWithoutChangingRetainedRows(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	raw := openRawMigrationDB(t, filepath.Join(t.TempDir(), "read-actions-upgrade.db"))
	before := testsupport.SQLiteMigrationsBefore(t, "0028_")
	if err := control.ApplyMigrations(ctx, raw, before); err != nil {
		t.Fatalf("ApplyMigrations(before 0028): %v", err)
	}
	if _, err := raw.ExecContext(t.Context(), `
		INSERT INTO knowledge_attempt_audit_tenant_state (
			tenant_id, first_sequence, next_sequence, retained_count
		) VALUES ('tenant-existing', 1, 1, 0);
		INSERT INTO knowledge_attempt_audit_events (
			tenant_id, sequence, occurred_at_unix_micro,
			actor_kind, actor_id, actor_role, action, result, reason,
			app_id, knowledge_object_id, object_type, object_version,
			sharing_scope
		) VALUES (
			'tenant-existing', 1, 1000001,
			'browser', 'administrator', 'administrator', 'validate',
			'rejected', 'invalid_definition',
			NULL, NULL, NULL, NULL, NULL
		);
		INSERT INTO knowledge_attempt_audit_events (
			tenant_id, sequence, occurred_at_unix_micro,
			actor_kind, actor_id, actor_role, action, result, reason,
			app_id, knowledge_object_id, object_type, object_version,
			sharing_scope
		) VALUES (
			'tenant-existing', 2, 1000002,
			'browser', 'administrator', 'administrator', 'update',
			'rejected', 'version_conflict',
			'app_012345678901234567890A', 'ko-existing', 'field_alias', 7,
			'app'
		);
		INSERT INTO knowledge_attempt_audit_events (
			tenant_id, sequence, occurred_at_unix_micro,
			actor_kind, actor_id, actor_role, action, result, reason,
			app_id, knowledge_object_id, object_type, object_version,
			sharing_scope
		) VALUES (
			'tenant-existing', 3, 1000003,
			'browser', 'administrator', 'administrator', 'create',
			'rejected', 'invalid_definition',
			'app_012345678901234567890A', NULL, NULL, NULL, NULL
		);
		INSERT INTO knowledge_attempt_audit_events (
			tenant_id, sequence, occurred_at_unix_micro,
			actor_kind, actor_id, actor_role, action, result, reason,
			app_id, knowledge_object_id, object_type, object_version,
			sharing_scope
		) VALUES (
			'tenant-existing', 4, 1000004,
			'browser', 'ordinary-user', 'user', 'preview',
			'rejected', 'not_administrator',
			NULL, NULL, NULL, NULL, NULL
		)
	`); err != nil {
		t.Fatalf("seed version 27 journal: %v", err)
	}
	beforeSchema := readKnowledgeAttemptAuditSchema(t, raw)
	beforeRows := readRawKnowledgeAttemptAuditRows(t, raw, "tenant-existing")

	if err := control.ApplyMigrations(ctx, raw, migrations.SQLite()); err != nil {
		t.Fatalf("ApplyMigrations(upgrade): %v", err)
	}
	afterSchema := readKnowledgeAttemptAuditSchema(t, raw)
	assertKnowledgeAttemptAuditSchemaEquivalent(t, beforeSchema, afterSchema)
	afterRows := readRawKnowledgeAttemptAuditRows(t, raw, "tenant-existing")
	if !reflect.DeepEqual(afterRows, beforeRows) {
		t.Fatalf("all-column retained rows changed across upgrade:\nbefore=%#v\nafter=%#v", beforeRows, afterRows)
	}

	var state tenantStateRecord
	if err := raw.QueryRowContext(t.Context(), `
		SELECT tenant_id, first_sequence, next_sequence, retained_count
		FROM knowledge_attempt_audit_tenant_state
		WHERE tenant_id = 'tenant-existing'
	`).Scan(
		&state.TenantID,
		&state.FirstSequence,
		&state.NextSequence,
		&state.RetainedCount,
	); err != nil {
		t.Fatal(err)
	}
	if state.FirstSequence != 1 || state.NextSequence != 5 || state.RetainedCount != 4 {
		t.Fatalf("upgraded tenant state = %+v", state)
	}

	for sequence, action := range []string{"get", "list", "dependencies", "dependents"} {
		if _, err := raw.ExecContext(t.Context(), `
			INSERT INTO knowledge_attempt_audit_events (
				tenant_id, sequence, occurred_at_unix_micro,
				actor_kind, actor_id, actor_role, action, result, reason,
				app_id, knowledge_object_id, object_type, object_version,
				sharing_scope
			) VALUES (
				'tenant-existing', ?, ?,
				'browser', 'administrator', 'administrator', ?,
				'rejected', 'service_unavailable',
				NULL, NULL, NULL, NULL, NULL
			)
		`, sequence+5, 1000005+sequence, action); err != nil {
			t.Fatalf("append %q after upgrade: %v", action, err)
		}
	}
	if err := raw.QueryRowContext(t.Context(), `
		SELECT first_sequence, next_sequence, retained_count
		FROM knowledge_attempt_audit_tenant_state
		WHERE tenant_id = 'tenant-existing'
	`).Scan(&state.FirstSequence, &state.NextSequence, &state.RetainedCount); err != nil {
		t.Fatal(err)
	}
	if state.FirstSequence != 1 || state.NextSequence != 9 || state.RetainedCount != 8 {
		t.Fatalf("post-upgrade append state = %+v", state)
	}

	if _, err := raw.ExecContext(t.Context(), `
		INSERT INTO knowledge_attempt_audit_events (
			tenant_id, sequence, occurred_at_unix_micro,
			actor_kind, actor_id, actor_role, action, result, reason,
			app_id, knowledge_object_id, object_type, object_version,
			sharing_scope
		) VALUES (
			'tenant-existing', 9, 1000009,
			'browser', 'administrator', 'administrator', 'future',
			'rejected', 'service_unavailable',
			NULL, NULL, NULL, NULL, NULL
		)
	`); err == nil {
		t.Fatal("unknown post-upgrade action was accepted")
	}
	if err := raw.QueryRowContext(t.Context(), `
		SELECT next_sequence, retained_count
		FROM knowledge_attempt_audit_tenant_state
		WHERE tenant_id = 'tenant-existing'
	`).Scan(&state.NextSequence, &state.RetainedCount); err != nil {
		t.Fatal(err)
	}
	if state.NextSequence != 9 || state.RetainedCount != 8 {
		t.Fatalf("invalid action advanced state = %+v", state)
	}

	foreignKeys, err := raw.QueryContext(t.Context(), `PRAGMA foreign_key_check`)
	if err != nil {
		t.Fatal(err)
	}
	defer foreignKeys.Close()
	if foreignKeys.Next() {
		t.Fatal("upgraded journal has a foreign-key violation")
	}
	var temporaryTables int
	if err := raw.QueryRowContext(t.Context(), `
		SELECT COUNT(*) FROM sqlite_schema
		WHERE name = 'knowledge_attempt_audit_events_before_read_actions'
	`).Scan(&temporaryTables); err != nil {
		t.Fatal(err)
	}
	if temporaryTables != 0 {
		t.Fatal("upgrade retained the temporary event table")
	}
}

func TestFailedReadActionMigrationRollsBackSchemaRowsAndLedger(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	raw := openRawMigrationDB(t, filepath.Join(t.TempDir(), "read-actions-rollback.db"))
	before := testsupport.SQLiteMigrationsBefore(t, "0028_")
	if err := control.ApplyMigrations(ctx, raw, before); err != nil {
		t.Fatalf("ApplyMigrations(before): %v", err)
	}
	if _, err := raw.ExecContext(t.Context(), `
		INSERT INTO knowledge_attempt_audit_tenant_state (
			tenant_id, first_sequence, next_sequence, retained_count
		) VALUES ('tenant-rollback', 1, 1, 0);
		INSERT INTO knowledge_attempt_audit_events (
			tenant_id, sequence, occurred_at_unix_micro,
			actor_kind, actor_id, actor_role, action, result, reason,
			app_id, knowledge_object_id, object_type, object_version,
			sharing_scope
		) VALUES (
			'tenant-rollback', 1, 1000001,
			'browser', 'administrator', 'administrator', 'preview',
			'rejected', 'resource_limit',
			NULL, NULL, NULL, NULL, NULL
		)
	`); err != nil {
		t.Fatal(err)
	}

	broken := testsupport.FilterEmbeddedFS(
		t,
		migrations.SQLite(),
		func(name string) bool { return name <= "0028_knowledge_attempt_audit_read_actions.sql" },
	)
	item := broken["0028_knowledge_attempt_audit_read_actions.sql"]
	item.Data = append(
		append([]byte(nil), item.Data...),
		[]byte("\nINSERT INTO table_that_does_not_exist VALUES (1);\n")...,
	)
	broken["0028_knowledge_attempt_audit_read_actions.sql"] = item
	if err := control.ApplyMigrations(ctx, raw, broken); err == nil {
		t.Fatal("broken read-action migration unexpectedly succeeded")
	}

	var latest, rows, triggers int
	var action string
	if err := raw.QueryRowContext(t.Context(), `SELECT MAX(version) FROM schema_migrations`).Scan(&latest); err != nil {
		t.Fatal(err)
	}
	if err := raw.QueryRowContext(t.Context(), `
		SELECT COUNT(*), MIN(action)
		FROM knowledge_attempt_audit_events
		WHERE tenant_id = 'tenant-rollback'
	`).Scan(&rows, &action); err != nil {
		t.Fatal(err)
	}
	if err := raw.QueryRowContext(t.Context(), `
		SELECT COUNT(*) FROM sqlite_schema
		WHERE type = 'trigger' AND name LIKE 'knowledge_attempt_audit_%'
	`).Scan(&triggers); err != nil {
		t.Fatal(err)
	}
	if latest != 27 || rows != 1 || action != "preview" || triggers != 10 {
		t.Fatalf(
			"rolled-back migration state = latest %d rows %d action %q triggers %d",
			latest,
			rows,
			action,
			triggers,
		)
	}
	if _, err := raw.ExecContext(t.Context(), `
		INSERT INTO knowledge_attempt_audit_events (
			tenant_id, sequence, occurred_at_unix_micro,
			actor_kind, actor_id, actor_role, action, result, reason,
			app_id, knowledge_object_id, object_type, object_version,
			sharing_scope
		) VALUES (
			'tenant-rollback', 2, 1000002,
			'browser', 'administrator', 'administrator', 'get',
			'rejected', 'service_unavailable',
			NULL, NULL, NULL, NULL, NULL
		)
	`); err == nil {
		t.Fatal("rolled-back version 27 schema accepted a read action")
	}
}

func TestFailedMigrationRollsBackEntireVersion(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	raw := openRawMigrationDB(t, filepath.Join(t.TempDir(), "rollback.db"))
	before := testsupport.SQLiteMigrationsBefore(t, "0027_")
	if err := control.ApplyMigrations(ctx, raw, before); err != nil {
		t.Fatalf("ApplyMigrations(before): %v", err)
	}
	broken := testsupport.FilterEmbeddedFS(
		t,
		migrations.SQLite(),
		func(name string) bool { return name <= "0027_knowledge_attempt_audit.sql" },
	)
	item := broken["0027_knowledge_attempt_audit.sql"]
	item.Data = append(append([]byte(nil), item.Data...), []byte("\nINSERT INTO table_that_does_not_exist VALUES (1);\n")...)
	broken["0027_knowledge_attempt_audit.sql"] = item
	if err := control.ApplyMigrations(ctx, raw, broken); err == nil {
		t.Fatal("broken migration unexpectedly succeeded")
	}
	var tables, ledger int
	if err := raw.QueryRowContext(t.Context(), `
		SELECT COUNT(*) FROM sqlite_schema
		WHERE name IN (
			'knowledge_attempt_audit_tenant_state',
			'knowledge_attempt_audit_events'
		)
	`).Scan(&tables); err != nil {
		t.Fatal(err)
	}
	if err := raw.QueryRowContext(t.Context(), `SELECT coalesce(MAX(version), 0) FROM schema_migrations`).Scan(&ledger); err != nil {
		t.Fatal(err)
	}
	if tables != 0 || ledger != 26 {
		t.Fatalf("failed migration left tables=%d ledger=%d", tables, ledger)
	}
}

func openRawMigrationDB(t *testing.T, path string) *sql.DB {
	t.Helper()
	raw, err := sql.Open("sqlite", path+"?_pragma=foreign_keys(1)&_txlock=immediate")
	if err != nil {
		t.Fatal(err)
	}
	raw.SetMaxOpenConns(4)
	if err := raw.PingContext(t.Context()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := raw.Close(); err != nil {
			t.Errorf("close raw migration DB: %v", err)
		}
	})
	return raw
}

type knowledgeAttemptAuditColumn struct {
	CID          int
	Name         string
	Type         string
	NotNull      int
	DefaultValue sql.NullString
	PrimaryKey   int
	Hidden       int
}

type knowledgeAttemptAuditForeignKey struct {
	ID       int
	Sequence int
	Table    string
	From     string
	To       string
	OnUpdate string
	OnDelete string
	Match    string
}

type knowledgeAttemptAuditIndexColumn struct {
	SequenceNumber int
	ColumnID       int
	Name           sql.NullString
	Descending     int
	Collation      sql.NullString
	Key            int
}

type knowledgeAttemptAuditIndex struct {
	Sequence int
	Name     string
	Unique   int
	Origin   string
	Partial  int
	Columns  []knowledgeAttemptAuditIndexColumn
}

type knowledgeAttemptAuditSchemaObject struct {
	Type      string
	Name      string
	TableName string
	SQL       string
}

type knowledgeAttemptAuditSchema struct {
	Columns      []knowledgeAttemptAuditColumn
	Strict       int
	WithoutRowID int
	ForeignKeys  []knowledgeAttemptAuditForeignKey
	Indexes      []knowledgeAttemptAuditIndex
	Objects      []knowledgeAttemptAuditSchemaObject
}

func readKnowledgeAttemptAuditSchema(t *testing.T, raw *sql.DB) knowledgeAttemptAuditSchema {
	t.Helper()
	var snapshot knowledgeAttemptAuditSchema

	columnRows, err := raw.QueryContext(t.Context(), `
		SELECT cid, name, type, "notnull", dflt_value, pk, hidden
		FROM pragma_table_xinfo('knowledge_attempt_audit_events')
		ORDER BY cid
	`)
	if err != nil {
		t.Fatal(err)
	}
	for columnRows.Next() {
		var column knowledgeAttemptAuditColumn
		if err := columnRows.Scan(
			&column.CID,
			&column.Name,
			&column.Type,
			&column.NotNull,
			&column.DefaultValue,
			&column.PrimaryKey,
			&column.Hidden,
		); err != nil {
			if closeErr := columnRows.Close(); closeErr != nil {
				t.Errorf("close column rows after scan failure: %v", closeErr)
			}
			t.Fatal(err)
		}
		snapshot.Columns = append(snapshot.Columns, column)
	}
	if err := columnRows.Err(); err != nil {
		if closeErr := columnRows.Close(); closeErr != nil {
			t.Errorf("close column rows after iteration failure: %v", closeErr)
		}
		t.Fatal(err)
	}
	if err := columnRows.Close(); err != nil {
		t.Fatal(err)
	}

	if err := raw.QueryRowContext(t.Context(), `
		SELECT strict, wr
		FROM pragma_table_list
		WHERE schema = 'main' AND name = 'knowledge_attempt_audit_events'
	`).Scan(&snapshot.Strict, &snapshot.WithoutRowID); err != nil {
		t.Fatal(err)
	}

	foreignKeyRows, err := raw.QueryContext(t.Context(), `
		SELECT id, seq, "table", "from", "to", on_update, on_delete, match
		FROM pragma_foreign_key_list('knowledge_attempt_audit_events')
		ORDER BY id, seq
	`)
	if err != nil {
		t.Fatal(err)
	}
	for foreignKeyRows.Next() {
		var foreignKey knowledgeAttemptAuditForeignKey
		if err := foreignKeyRows.Scan(
			&foreignKey.ID,
			&foreignKey.Sequence,
			&foreignKey.Table,
			&foreignKey.From,
			&foreignKey.To,
			&foreignKey.OnUpdate,
			&foreignKey.OnDelete,
			&foreignKey.Match,
		); err != nil {
			if closeErr := foreignKeyRows.Close(); closeErr != nil {
				t.Errorf("close foreign-key rows after scan failure: %v", closeErr)
			}
			t.Fatal(err)
		}
		snapshot.ForeignKeys = append(snapshot.ForeignKeys, foreignKey)
	}
	if err := foreignKeyRows.Err(); err != nil {
		if closeErr := foreignKeyRows.Close(); closeErr != nil {
			t.Errorf("close foreign-key rows after iteration failure: %v", closeErr)
		}
		t.Fatal(err)
	}
	if err := foreignKeyRows.Close(); err != nil {
		t.Fatal(err)
	}

	indexRows, err := raw.QueryContext(t.Context(), `
		SELECT seq, name, "unique", origin, partial
		FROM pragma_index_list('knowledge_attempt_audit_events')
		ORDER BY seq
	`)
	if err != nil {
		t.Fatal(err)
	}
	for indexRows.Next() {
		var index knowledgeAttemptAuditIndex
		if err := indexRows.Scan(
			&index.Sequence,
			&index.Name,
			&index.Unique,
			&index.Origin,
			&index.Partial,
		); err != nil {
			if closeErr := indexRows.Close(); closeErr != nil {
				t.Errorf("close index rows after scan failure: %v", closeErr)
			}
			t.Fatal(err)
		}
		snapshot.Indexes = append(snapshot.Indexes, index)
	}
	if err := indexRows.Err(); err != nil {
		if closeErr := indexRows.Close(); closeErr != nil {
			t.Errorf("close index rows after iteration failure: %v", closeErr)
		}
		t.Fatal(err)
	}
	if err := indexRows.Close(); err != nil {
		t.Fatal(err)
	}
	for indexNumber := range snapshot.Indexes {
		index := &snapshot.Indexes[indexNumber]
		indexColumnRows, err := raw.QueryContext(t.Context(), `
			SELECT seqno, cid, name, "desc", coll, "key"
			FROM pragma_index_xinfo(?)
			ORDER BY seqno
		`, index.Name)
		if err != nil {
			t.Fatal(err)
		}
		for indexColumnRows.Next() {
			var column knowledgeAttemptAuditIndexColumn
			if err := indexColumnRows.Scan(
				&column.SequenceNumber,
				&column.ColumnID,
				&column.Name,
				&column.Descending,
				&column.Collation,
				&column.Key,
			); err != nil {
				if closeErr := indexColumnRows.Close(); closeErr != nil {
					t.Errorf("close index-column rows after scan failure: %v", closeErr)
				}
				t.Fatal(err)
			}
			index.Columns = append(index.Columns, column)
		}
		if err := indexColumnRows.Err(); err != nil {
			if closeErr := indexColumnRows.Close(); closeErr != nil {
				t.Errorf("close index-column rows after iteration failure: %v", closeErr)
			}
			t.Fatal(err)
		}
		if err := indexColumnRows.Close(); err != nil {
			t.Fatal(err)
		}
	}

	objectRows, err := raw.QueryContext(t.Context(), `
		SELECT type, name, tbl_name, coalesce(sql, '')
		FROM sqlite_schema
		WHERE name GLOB 'knowledge_attempt_audit_*'
		   OR tbl_name IN (
			'knowledge_attempt_audit_tenant_state',
			'knowledge_attempt_audit_events'
		   )
		ORDER BY type, name
	`)
	if err != nil {
		t.Fatal(err)
	}
	for objectRows.Next() {
		var object knowledgeAttemptAuditSchemaObject
		var schemaSQL string
		if err := objectRows.Scan(
			&object.Type,
			&object.Name,
			&object.TableName,
			&schemaSQL,
		); err != nil {
			if closeErr := objectRows.Close(); closeErr != nil {
				t.Errorf("close schema-object rows after scan failure: %v", closeErr)
			}
			t.Fatal(err)
		}
		object.SQL = normalizeKnowledgeAttemptAuditSchemaSQL(schemaSQL)
		snapshot.Objects = append(snapshot.Objects, object)
	}
	if err := objectRows.Err(); err != nil {
		if closeErr := objectRows.Close(); closeErr != nil {
			t.Errorf("close schema-object rows after iteration failure: %v", closeErr)
		}
		t.Fatal(err)
	}
	if err := objectRows.Close(); err != nil {
		t.Fatal(err)
	}
	return snapshot
}

var (
	knowledgeAttemptAuditActionCheckPattern = regexp.MustCompile(
		`action IN \(\s*((?:'[^']+'\s*,\s*)*'[^']+')\s*\)`,
	)
	knowledgeAttemptAuditQuotedValuePattern = regexp.MustCompile(`'([^']+)'`)
	knowledgeAttemptAuditObjectRulePattern  = regexp.MustCompile(
		`action (?:<> 'create'|NOT IN \('create', 'list'\)) OR knowledge_object_id IS NULL`,
	)
)

func assertKnowledgeAttemptAuditSchemaEquivalent(
	t *testing.T,
	before knowledgeAttemptAuditSchema,
	after knowledgeAttemptAuditSchema,
) {
	t.Helper()
	assertKnowledgeAttemptAuditObjectNames(t, "version 27", before)
	assertKnowledgeAttemptAuditObjectNames(t, "version 28", after)

	beforeTableSQL := knowledgeAttemptAuditObjectSQL(t, before, "knowledge_attempt_audit_events")
	afterTableSQL := knowledgeAttemptAuditObjectSQL(t, after, "knowledge_attempt_audit_events")
	assertKnowledgeAttemptAuditActions(t, "version 27", beforeTableSQL, []string{
		"create", "update", "scope_change", "enable", "disable",
		"quarantine", "delete", "validate", "preview",
	})
	assertKnowledgeAttemptAuditActions(t, "version 28", afterTableSQL, []string{
		"create", "get", "list", "update", "scope_change", "enable",
		"disable", "quarantine", "delete", "validate", "dependencies",
		"dependents", "preview",
	})
	const beforeObjectRule = "action <> 'create' OR knowledge_object_id IS NULL"
	const afterObjectRule = "action NOT IN ('create', 'list') OR knowledge_object_id IS NULL"
	if strings.Count(beforeTableSQL, beforeObjectRule) != 1 {
		t.Fatalf("version 27 event table does not contain exactly one expected object rule: %s", beforeTableSQL)
	}
	if strings.Count(afterTableSQL, afterObjectRule) != 1 {
		t.Fatalf("version 28 event table does not contain exactly one expected object rule: %s", afterTableSQL)
	}

	before = scrubKnowledgeAttemptAuditIntendedDeltas(t, before)
	after = scrubKnowledgeAttemptAuditIntendedDeltas(t, after)
	if !reflect.DeepEqual(after, before) {
		t.Fatalf(
			"knowledge-attempt audit schema changed beyond the intended action CHECK and list rule:\nbefore=%#v\nafter=%#v",
			before,
			after,
		)
	}
}

func assertKnowledgeAttemptAuditObjectNames(
	t *testing.T,
	version string,
	snapshot knowledgeAttemptAuditSchema,
) {
	t.Helper()
	var names []string
	for _, object := range snapshot.Objects {
		if strings.HasPrefix(object.Name, "knowledge_attempt_audit_") {
			names = append(names, object.Type+":"+object.Name)
		}
	}
	sort.Strings(names)
	want := []string{
		"index:knowledge_attempt_audit_tenant_actor_sequence_idx",
		"index:knowledge_attempt_audit_tenant_reason_sequence_idx",
		"table:knowledge_attempt_audit_events",
		"table:knowledge_attempt_audit_tenant_state",
		"trigger:knowledge_attempt_audit_event_advances_and_prunes",
		"trigger:knowledge_attempt_audit_event_delete_requires_rolling_prune",
		"trigger:knowledge_attempt_audit_event_identity_collision_is_forbidden",
		"trigger:knowledge_attempt_audit_event_insert_requires_current_state",
		"trigger:knowledge_attempt_audit_event_prune_advances_state",
		"trigger:knowledge_attempt_audit_event_update_is_forbidden",
		"trigger:knowledge_attempt_audit_state_delete_is_forbidden",
		"trigger:knowledge_attempt_audit_state_identity_collision_is_forbidden",
		"trigger:knowledge_attempt_audit_state_initial_shape_is_valid",
		"trigger:knowledge_attempt_audit_state_transition_is_valid",
	}
	if !reflect.DeepEqual(names, want) {
		t.Fatalf("%s explicit schema objects = %v, want %v", version, names, want)
	}
}

func assertKnowledgeAttemptAuditActions(
	t *testing.T,
	version string,
	tableSQL string,
	want []string,
) {
	t.Helper()
	matches := knowledgeAttemptAuditActionCheckPattern.FindAllStringSubmatch(tableSQL, -1)
	if len(matches) != 1 {
		t.Fatalf("%s action CHECK match count = %d, want 1: %s", version, len(matches), tableSQL)
	}
	var actions []string
	for _, match := range knowledgeAttemptAuditQuotedValuePattern.FindAllStringSubmatch(matches[0][1], -1) {
		actions = append(actions, match[1])
	}
	if !reflect.DeepEqual(actions, want) {
		t.Fatalf("%s action CHECK = %v, want %v", version, actions, want)
	}
}

func knowledgeAttemptAuditObjectSQL(
	t *testing.T,
	snapshot knowledgeAttemptAuditSchema,
	name string,
) string {
	t.Helper()
	for _, object := range snapshot.Objects {
		if object.Name == name {
			return object.SQL
		}
	}
	t.Fatalf("schema object %q not found", name)
	return ""
}

func scrubKnowledgeAttemptAuditIntendedDeltas(
	t *testing.T,
	snapshot knowledgeAttemptAuditSchema,
) knowledgeAttemptAuditSchema {
	t.Helper()
	for objectNumber := range snapshot.Objects {
		object := &snapshot.Objects[objectNumber]
		if object.Name != "knowledge_attempt_audit_events" {
			continue
		}
		if len(knowledgeAttemptAuditActionCheckPattern.FindAllString(object.SQL, -1)) != 1 {
			t.Fatalf("event table does not have exactly one action CHECK: %s", object.SQL)
		}
		if len(knowledgeAttemptAuditObjectRulePattern.FindAllString(object.SQL, -1)) != 1 {
			t.Fatalf("event table does not have exactly one recognized action/object rule: %s", object.SQL)
		}
		object.SQL = knowledgeAttemptAuditActionCheckPattern.ReplaceAllString(
			object.SQL,
			"action IN (<intended-action-taxonomy-delta>)",
		)
		object.SQL = knowledgeAttemptAuditObjectRulePattern.ReplaceAllString(
			object.SQL,
			"action <intended-object-shape-delta> OR knowledge_object_id IS NULL",
		)
		return snapshot
	}
	t.Fatal("event table schema object not found")
	return knowledgeAttemptAuditSchema{}
}

func normalizeKnowledgeAttemptAuditSchemaSQL(schemaSQL string) string {
	var tokens []string
	for line := range strings.SplitSeq(schemaSQL, "\n") {
		if beforeComment, _, found := strings.Cut(line, "--"); found {
			line = beforeComment
		}
		tokens = append(tokens, strings.Fields(line)...)
	}
	return strings.Join(tokens, " ")
}

type rawKnowledgeAttemptAuditText struct {
	Bytes   []byte
	Present bool
}

type rawKnowledgeAttemptAuditRow struct {
	TenantID            rawKnowledgeAttemptAuditText
	Sequence            int64
	OccurredAtUnixMicro int64
	ActorKind           rawKnowledgeAttemptAuditText
	ActorID             rawKnowledgeAttemptAuditText
	ActorRole           rawKnowledgeAttemptAuditText
	Action              rawKnowledgeAttemptAuditText
	Result              rawKnowledgeAttemptAuditText
	Reason              rawKnowledgeAttemptAuditText
	AppID               rawKnowledgeAttemptAuditText
	KnowledgeObjectID   rawKnowledgeAttemptAuditText
	ObjectType          rawKnowledgeAttemptAuditText
	ObjectVersion       sql.NullInt64
	SharingScope        rawKnowledgeAttemptAuditText
}

func readRawKnowledgeAttemptAuditRows(
	t *testing.T,
	raw *sql.DB,
	tenantID string,
) []rawKnowledgeAttemptAuditRow {
	t.Helper()
	rows, err := raw.QueryContext(t.Context(), `
		SELECT
			CAST(tenant_id AS BLOB), sequence, occurred_at_unix_micro,
			CAST(actor_kind AS BLOB), CAST(actor_id AS BLOB),
			CAST(actor_role AS BLOB), CAST(action AS BLOB),
			CAST(result AS BLOB), CAST(reason AS BLOB), CAST(app_id AS BLOB),
			CAST(knowledge_object_id AS BLOB), CAST(object_type AS BLOB),
			object_version, CAST(sharing_scope AS BLOB)
		FROM knowledge_attempt_audit_events
		WHERE tenant_id = ?
		ORDER BY sequence
	`, tenantID)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	var result []rawKnowledgeAttemptAuditRow
	for rows.Next() {
		var row rawKnowledgeAttemptAuditRow
		var tenantIDValue, actorKind, actorID, actorRole sql.NullString
		var action, auditResult, reason, appID sql.NullString
		var knowledgeObjectID, objectType, sharingScope sql.NullString
		if err := rows.Scan(
			&tenantIDValue,
			&row.Sequence,
			&row.OccurredAtUnixMicro,
			&actorKind,
			&actorID,
			&actorRole,
			&action,
			&auditResult,
			&reason,
			&appID,
			&knowledgeObjectID,
			&objectType,
			&row.ObjectVersion,
			&sharingScope,
		); err != nil {
			t.Fatal(err)
		}
		row.TenantID = rawKnowledgeAttemptAuditTextValue(tenantIDValue)
		row.ActorKind = rawKnowledgeAttemptAuditTextValue(actorKind)
		row.ActorID = rawKnowledgeAttemptAuditTextValue(actorID)
		row.ActorRole = rawKnowledgeAttemptAuditTextValue(actorRole)
		row.Action = rawKnowledgeAttemptAuditTextValue(action)
		row.Result = rawKnowledgeAttemptAuditTextValue(auditResult)
		row.Reason = rawKnowledgeAttemptAuditTextValue(reason)
		row.AppID = rawKnowledgeAttemptAuditTextValue(appID)
		row.KnowledgeObjectID = rawKnowledgeAttemptAuditTextValue(knowledgeObjectID)
		row.ObjectType = rawKnowledgeAttemptAuditTextValue(objectType)
		row.SharingScope = rawKnowledgeAttemptAuditTextValue(sharingScope)
		result = append(result, row)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return result
}

func rawKnowledgeAttemptAuditTextValue(value sql.NullString) rawKnowledgeAttemptAuditText {
	return rawKnowledgeAttemptAuditText{
		Bytes:   []byte(value.String),
		Present: value.Valid,
	}
}
