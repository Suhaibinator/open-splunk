package knowledgeattemptaudit

import (
	"strings"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/migrations"
)

func TestBaselineSchemaIsExactBoundedAndRetrySafe(t *testing.T) {
	t.Parallel()
	database := openTestDatabase(t)
	ctx := t.Context()

	var version, ledgerRows int
	var name string
	if err := database.SQLDB().QueryRowContext(ctx, `
		SELECT version, name FROM schema_migrations
	`).Scan(&version, &name); err != nil {
		t.Fatal(err)
	}
	if err := database.SQLDB().QueryRowContext(ctx, `
		SELECT COUNT(*) FROM schema_migrations
	`).Scan(&ledgerRows); err != nil {
		t.Fatal(err)
	}
	if version != 1 || name != "0001_baseline.sql" || ledgerRows != 1 {
		t.Fatalf("migration ledger = version %d name %q rows %d", version, name, ledgerRows)
	}

	rows, err := database.SQLDB().QueryContext(ctx, `
		SELECT name FROM pragma_table_info('knowledge_attempt_audit_events') ORDER BY cid
	`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var columns []string
	for rows.Next() {
		var column string
		if err := rows.Scan(&column); err != nil {
			t.Fatal(err)
		}
		columns = append(columns, column)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	wantColumns := []string{
		"tenant_id", "sequence", "occurred_at_unix_micro", "actor_kind",
		"actor_id", "actor_role", "action", "result", "reason",
		"app_id", "knowledge_object_id", "object_type", "object_version",
		"sharing_scope",
	}
	if strings.Join(columns, ",") != strings.Join(wantColumns, ",") {
		t.Fatalf("columns = %v, want %v", columns, wantColumns)
	}

	var strict, withoutRowID int
	if err := database.SQLDB().QueryRowContext(ctx, `
		SELECT strict, wr FROM pragma_table_list
		WHERE schema = 'main' AND name = 'knowledge_attempt_audit_events'
	`).Scan(&strict, &withoutRowID); err != nil {
		t.Fatal(err)
	}
	if strict != 1 || withoutRowID != 1 {
		t.Fatalf("event table shape = strict %d without-rowid %d", strict, withoutRowID)
	}

	var tableSQL string
	if err := database.SQLDB().QueryRowContext(ctx, `
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
	if err := database.SQLDB().QueryRowContext(ctx, `
		SELECT COUNT(*) FROM sqlite_schema
		WHERE type = 'trigger' AND name LIKE 'knowledge_attempt_audit_%'
	`).Scan(&triggerCount); err != nil {
		t.Fatal(err)
	}
	if triggerCount != 10 {
		t.Fatalf("trigger count = %d, want 10", triggerCount)
	}
	if _, err := database.SQLDB().ExecContext(ctx, `
		INSERT INTO knowledge_attempt_audit_tenant_state (
			tenant_id, first_sequence, next_sequence, retained_count
		) VALUES ('bad-count', 1, 100003, 100002)
	`); err == nil {
		t.Fatal("over-cap retained count was accepted")
	}

	if err := control.ApplyMigrations(ctx, database.SQLDB(), migrations.SQLite()); err != nil {
		t.Fatalf("reapply baseline: %v", err)
	}
	if err := database.SQLDB().QueryRowContext(ctx, `
		SELECT COUNT(*) FROM schema_migrations
	`).Scan(&ledgerRows); err != nil {
		t.Fatal(err)
	}
	if ledgerRows != 1 {
		t.Fatalf("migration retry changed ledger row count to %d", ledgerRows)
	}
}
