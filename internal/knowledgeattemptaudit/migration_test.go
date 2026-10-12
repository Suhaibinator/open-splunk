package knowledgeattemptaudit

import (
	"io/fs"
	"strings"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/pgtest"
	"github.com/Suhaibinator/open-splunk/migrations"
)

func TestCurrentSchemaIsExactBoundedAndRetrySafe(t *testing.T) {
	t.Parallel()
	database := openTestDatabase(t)
	ctx := t.Context()

	rows, err := database.SQLDB().QueryContext(ctx, `
		SELECT version, name, length(checksum), applied_at_unix_micro
		FROM open_splunk_schema_migrations
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
	wantLedger, err := fs.Glob(migrations.PostgreSQL(), "*.sql")
	if err != nil {
		t.Fatal(err)
	}

	if strings.Join(ledger, ",") != strings.Join(wantLedger, ",") {
		t.Fatalf("migration ledger = %v, want %v", ledger, wantLedger)
	}

	rows, err = database.SQLDB().QueryContext(ctx, `
		SELECT column_name FROM information_schema.columns WHERE table_schema=current_schema() AND table_name='knowledge_attempt_audit_events' ORDER BY ordinal_position
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

	for _, table := range []string{"knowledge_attempt_audit_events", "server_search_settings"} {
		var primaryKey int
		if err := database.SQLDB().QueryRowContext(ctx, `SELECT count(*) FROM pg_constraint WHERE connamespace=current_schema()::regnamespace AND conrelid=$1::regclass AND contype='p'`, table).Scan(&primaryKey); err != nil {
			t.Fatal(err)
		}
		if primaryKey != 1 {
			t.Fatalf("%s native primary key count %d", table, primaryKey)
		}
		var unsupported int
		if err := database.SQLDB().QueryRowContext(ctx, `SELECT count(*) FROM pg_attribute WHERE attrelid=$1::regclass AND attnum>0 AND NOT attisdropped AND atttypid NOT IN ('text'::regtype,'int8'::regtype)`, table).Scan(&unsupported); err != nil {
			t.Fatal(err)
		}
		if unsupported != 0 {
			t.Fatalf("%s native typed columns: %d unsupported", table, unsupported)
		}
	}

	var tableSQL string
	if err := database.SQLDB().QueryRowContext(ctx, `
		SELECT string_agg(pg_get_constraintdef(oid),' ') FROM pg_constraint WHERE connamespace=current_schema()::regnamespace AND conrelid='knowledge_attempt_audit_events'::regclass AND contype='c'
	`).Scan(&tableSQL); err != nil {
		t.Fatal(err)
	}
	for _, action := range []string{"'get'", "'list'", "'dependencies'", "'dependents'"} {
		if !pgtest.ContainsCheck(tableSQL, action) {
			t.Errorf("event table CHECK does not contain action %s", action)
		}
	}
	var auditTableSQL string
	if err := database.SQLDB().QueryRowContext(ctx, `
		SELECT string_agg(pg_get_constraintdef(oid),' ') FROM pg_constraint WHERE connamespace=current_schema()::regnamespace AND conrelid='audit_events'::regclass AND contype='c'
	`).Scan(&auditTableSQL); err != nil {
		t.Fatal(err)
	}
	for _, marker := range []string{"'server_settings.update'", "'server_settings'"} {
		if !pgtest.ContainsCheck(auditTableSQL, marker) {
			t.Errorf("current audit event table does not contain %s", marker)
		}
	}

	for _, name := range []string{"knowledge_attempt_audit_state_initial_shape_is_valid", "knowledge_attempt_audit_state_delete_is_forbidden", "knowledge_attempt_audit_state_transition_is_valid", "knowledge_attempt_audit_event_insert_requires_current_state", "knowledge_attempt_audit_event_advances_and_prunes", "knowledge_attempt_audit_event_update_is_forbidden", "knowledge_attempt_audit_event_delete_requires_rolling_prune", "knowledge_attempt_audit_event_prune_advances_state"} {
		var present bool
		if err := database.SQLDB().QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM pg_trigger WHERE tgname=$1 AND tgrelid IN ('knowledge_attempt_audit_events'::regclass,'knowledge_attempt_audit_tenant_state'::regclass) AND NOT tgisinternal)`, name).Scan(&present); err != nil {
			t.Fatal(err)
		}
		if !present {
			t.Fatalf("required rolling integrity trigger %s absent", name)
		}
	}

	if _, err := database.SQLDB().ExecContext(ctx, `
		INSERT INTO knowledge_attempt_audit_tenant_state (
			tenant_id, first_sequence, next_sequence, retained_count
		) VALUES ('bad-count', 1, 100003, 100002)
	`); err == nil {
		t.Fatal("over-cap retained count was accepted")
	}

	if err := control.ApplyMigrations(ctx, database.SQLDB(), migrations.PostgreSQL()); err != nil {
		t.Fatalf("reapply migrations: %v", err)
	}
	var ledgerRows int
	if err := database.SQLDB().QueryRowContext(ctx, `
		SELECT COUNT(*) FROM open_splunk_schema_migrations
	`).Scan(&ledgerRows); err != nil {
		t.Fatal(err)
	}
	if ledgerRows != len(wantLedger) {
		t.Fatalf("migration retry changed ledger row count to %d", ledgerRows)
	}
}
