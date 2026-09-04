package knowledgeattemptaudit

import (
	"strings"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/migrations"
)

func TestCurrentSchemaIsExactBoundedAndRetrySafe(t *testing.T) {
	t.Parallel()
	database := openTestDatabase(t)
	ctx := t.Context()

	rows, err := database.SQLDB().QueryContext(ctx, `
		SELECT version, name, length(checksum), applied_at_unix_micro
		FROM schema_migrations
		ORDER BY version
	`)
	if err != nil {
		t.Fatal(err)
	}
	var ledger []string
	for rows.Next() {
		var version int
		var name string
		var checksumBytes int
		var appliedAt int64
		if err := rows.Scan(&version, &name, &checksumBytes, &appliedAt); err != nil {
			t.Fatal(err)
		}
		ledger = append(ledger, name)
		if version != len(ledger) {
			t.Fatalf("migration ledger version %d is not contiguous at row %d", version, len(ledger))
		}
		if checksumBytes != 32 || appliedAt <= 0 {
			t.Fatalf("migration ledger row %d has checksum bytes %d and applied time %d", version, checksumBytes, appliedAt)
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if err := rows.Close(); err != nil {
		t.Fatal(err)
	}
	wantLedger := []string{
		"0001_baseline.sql",
		"0002_server_search_settings.sql",
		"0003_durable_search_jobs.sql",
		"0004_saved_search_schedules.sql",
		"0005_alerts.sql",
		"0006_feature_operation_audit.sql",
		"0007_lookup_mutation_audit.sql",
		"0008_rolling_feature_operation_audit.sql",
		"0009_ingest_reservation_accounting.sql",
		"0010_ingest_write_groups.sql",
	}
	if strings.Join(ledger, ",") != strings.Join(wantLedger, ",") {
		t.Fatalf("migration ledger = %v, want %v", ledger, wantLedger)
	}

	rows, err = database.SQLDB().QueryContext(ctx, `
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
	var settingsStrict, settingsWithoutRowID int
	if err := database.SQLDB().QueryRowContext(ctx, `
		SELECT strict, wr FROM pragma_table_list
		WHERE schema = 'main' AND name = 'server_search_settings'
	`).Scan(&settingsStrict, &settingsWithoutRowID); err != nil {
		t.Fatal(err)
	}
	if settingsStrict != 1 || settingsWithoutRowID != 1 {
		t.Fatalf("server settings table shape = strict %d without-rowid %d", settingsStrict, settingsWithoutRowID)
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
	var auditTableSQL string
	if err := database.SQLDB().QueryRowContext(ctx, `
		SELECT sql FROM sqlite_schema
		WHERE type = 'table' AND name = 'audit_events'
	`).Scan(&auditTableSQL); err != nil {
		t.Fatal(err)
	}
	for _, marker := range []string{"'server_settings.update'", "'server_settings'"} {
		if !strings.Contains(auditTableSQL, marker) {
			t.Errorf("current audit event table does not contain %s", marker)
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
		t.Fatalf("reapply migrations: %v", err)
	}
	var ledgerRows int
	if err := database.SQLDB().QueryRowContext(ctx, `
		SELECT COUNT(*) FROM schema_migrations
	`).Scan(&ledgerRows); err != nil {
		t.Fatal(err)
	}
	if ledgerRows != len(wantLedger) {
		t.Fatalf("migration retry changed ledger row count to %d", ledgerRows)
	}
}
