package knowledgeattemptaudit

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"path/filepath"
	"testing"
	"time"

	"net/url"
	"os"

	"github.com/Suhaibinator/open-splunk/internal/audit"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/pgtest"
)

func TestMigrationRejectsDirectInvalidTaxonomyAndPrivacyShapes(t *testing.T) {
	t.Parallel()
	database := openTestDatabase(t)
	if _, err := database.SQLDB().ExecContext(t.Context(), `
		INSERT INTO knowledge_attempt_audit_tenant_state (
			tenant_id, first_sequence, next_sequence, retained_count
		) VALUES ('tenant-direct', 1, 1, 0)
	`); err != nil {
		t.Fatal(err)
	}
	valid := []any{
		"tenant-direct", 1, testTime.UnixMicro(), "browser", "administrator",
		"administrator", "validate", "rejected", "invalid_definition",
		nil, nil, nil, nil, nil,
	}
	tests := []struct {
		name  string
		index int
		value any
	}{
		{"system actor", 3, "system"},
		{"unknown role", 5, "system"},
		{"unknown action", 6, "future"},
		{"success result", 7, "success"},
		{"unknown reason", 8, "future"},
		{"admin not-administrator reason", 8, "not_administrator"},
		{"invalid app value", 9, ""},
		{"partial object value", 10, "ko-attacker"},
		{"version conflict without object", 8, "version_conflict"},
	}
	statement := `
		INSERT INTO knowledge_attempt_audit_events (
			tenant_id, sequence, occurred_at_unix_micro,
			actor_kind, actor_id, actor_role, action, result, reason,
			app_id, knowledge_object_id, object_type, object_version, sharing_scope
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
	`
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			values := append([]any(nil), valid...)
			values[test.index] = test.value
			if _, err := database.SQLDB().ExecContext(t.Context(), statement, values...); err == nil {
				t.Fatal("invalid direct insert unexpectedly succeeded")
			}
		})
	}
	listWithObject := append([]any(nil), valid...)
	listWithObject[6] = "list"
	listWithObject[9] = "app_012345678901234567890A"
	listWithObject[10] = "ko-authorized"
	listWithObject[11] = "field_alias"
	listWithObject[12] = int64(1)
	listWithObject[13] = "app"
	if _, err := database.SQLDB().ExecContext(t.Context(), statement, listWithObject...); err == nil {
		t.Fatal("list attempt retained object metadata")
	}
	userWithApp := append([]any(nil), valid...)
	userWithApp[5] = "user"
	userWithApp[8] = "not_administrator"
	userWithApp[9] = "app_012345678901234567890A"
	if _, err := database.SQLDB().ExecContext(t.Context(), statement, userWithApp...); err == nil {
		t.Fatal("non-administrator attempt retained app metadata")
	}
	var state tenantStateRecord
	if err := database.GORMDB().Where("tenant_id = ?", "tenant-direct").Take(&state).Error; err != nil {
		t.Fatal(err)
	}
	if state.NextSequence != 1 || state.RetainedCount != 0 {
		t.Fatalf("failed inserts advanced state: %+v", state)
	}
}

func TestRollingCapEvictsOldestAcrossNativeInserts(t *testing.T) {
	database := openTestDatabase(t)
	store := newTestStore(t, database)
	admin := actorContext(t, audit.ActorRoleAdministrator, "administrator")
	ctx := context.Background()
	conn, err := database.SQLDB().Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	connectionClosed := false
	defer func() {
		if !connectionClosed {
			_ = conn.Close()
		}
	}()
	committed := false
	defer func() {
		if !committed {
			_, _ = conn.ExecContext(context.Background(), `ROLLBACK`)
		}
	}()
	if _, err := conn.ExecContext(ctx, `
		INSERT INTO knowledge_attempt_audit_tenant_state (
			tenant_id, first_sequence, next_sequence, retained_count
		) VALUES ('tenant-cap', 1, 1, 0)
	`); err != nil {
		t.Fatal(err)
	}
	// PostgreSQL runs AFTER ROW accounting after the whole INSERT statement;
	// seed one guarded statement per sequence to exercise the real rolling journal.
	// Committed chunks avoid a fixture-only 100,000-version tenant-state HOT chain;
	// production also commits individual appends. Boundary mutations below share
	// an explicit transaction with every accounting and rolling-prune guard active.
	if _, err := conn.ExecContext(ctx, `DO $$ DECLARE value BIGINT; BEGIN
 FOR value IN 1..100000 LOOP
 INSERT INTO knowledge_attempt_audit_events (
 tenant_id,sequence,occurred_at_unix_micro,actor_kind,actor_id,actor_role,
 action,result,reason,app_id,knowledge_object_id,object_type,object_version,sharing_scope
 ) VALUES ('tenant-cap',value,1000000+value,'browser','administrator','administrator',
 'validate','rejected','invalid_definition',NULL,NULL,NULL,NULL,NULL);
 IF value % 512 = 0 THEN COMMIT; END IF;
 END LOOP; END $$`); err != nil {
		t.Fatalf("seed cap: %v", err)
	}

	if _, err := conn.ExecContext(ctx, `BEGIN`); err != nil {
		t.Fatal(err)
	}
	assertRawState(t, conn, "tenant-cap", 1, 100001, 100000)

	insertDirectAttempt(t, conn, "tenant-cap", 100001)
	assertRawState(t, conn, "tenant-cap", 2, 100002, 100000)
	assertRawSequenceAbsent(t, conn, "tenant-cap", 1)

	insertDirectAttempt(t, conn, "tenant-cap", 100002)
	assertRawState(t, conn, "tenant-cap", 3, 100003, 100000)
	assertRawSequenceAbsent(t, conn, "tenant-cap", 2)
	if _, err := conn.ExecContext(ctx, `COMMIT`); err != nil {
		t.Fatal(err)
	}
	committed = true

	if err := validateAllTenantIntegrity(database.GORMDB()); err != nil {
		t.Fatalf("validateAllTenantIntegrity: %v", err)
	}
	if err := conn.Close(); err != nil {
		t.Fatal(err)
	}
	connectionClosed = true
	before := readRawTenantState(t, database, "tenant-cap")
	corruptEventForAppendTest(t, database, `
		UPDATE knowledge_attempt_audit_events
		SET actor_role = 'administratoX'
		WHERE tenant_id = 'tenant-cap' AND sequence = 3
	`)
	if err := store.AppendRejected(
		admin,
		"tenant-cap",
		adminDefinition(ActionPreview, ReasonResourceLimit, time.Microsecond),
	); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("AppendRejected(malformed oldest at cap) = %v, want ErrCorrupt", err)
	}
	after := readRawTenantState(t, database, "tenant-cap")
	if after != before {
		t.Fatalf("state changed: before=%+v after=%+v", before, after)
	}
	assertRawEventWindow(t, database, "tenant-cap", MaximumRetainedAttempts, 3, 100002)
	assertRawEventSequenceCount(t, database, "tenant-cap", 3, 1)
	assertRawEventSequenceCount(t, database, "tenant-cap", 100003, 0)
}

func TestNativeUpsertCannotBypassImmutability(t *testing.T) {
	t.Parallel()
	database := openTestDatabase(t)
	store := newTestStore(t, database)
	admin := actorContext(t, audit.ActorRoleAdministrator, "administrator")
	if err := store.AppendRejected(admin, "tenant-replace", adminDefinition(ActionValidate, ReasonInvalidDefinition, 0)); err != nil {
		t.Fatal(err)
	}
	conn, err := database.SQLDB().Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	for attempt := range 2 {
		if _, err := conn.ExecContext(context.Background(), `
			INSERT INTO knowledge_attempt_audit_tenant_state (
				tenant_id, first_sequence, next_sequence, retained_count
			) VALUES ('tenant-replace', 1, 2, 1)
 ON CONFLICT (tenant_id) DO UPDATE SET first_sequence=EXCLUDED.first_sequence,next_sequence=EXCLUDED.next_sequence,retained_count=EXCLUDED.retained_count
		`); err == nil {
			t.Fatalf("native state upsert succeeded on attempt %d", attempt)
		}
		if _, err := conn.ExecContext(context.Background(), `
			INSERT INTO knowledge_attempt_audit_events (
				tenant_id, sequence, occurred_at_unix_micro,
				actor_kind, actor_id, actor_role, action, result, reason,
				app_id, knowledge_object_id, object_type, object_version, sharing_scope
			) VALUES (
				'tenant-replace', 1, 1, 'browser', 'attacker', 'administrator',
				'validate', 'rejected', 'invalid_definition',
				NULL, NULL, NULL, NULL, NULL
			) ON CONFLICT (tenant_id,sequence) DO UPDATE SET actor_id=EXCLUDED.actor_id
		`); err == nil {
			t.Fatalf("native event upsert succeeded on attempt %d", attempt)
		}
		if _, err := conn.ExecContext(context.Background(), `
			UPDATE knowledge_attempt_audit_events SET actor_id = 'attacker'
			WHERE tenant_id = 'tenant-replace' AND sequence = 1
		`); err == nil {
			t.Fatalf("event update succeeded on attempt %d", attempt)
		}
		if _, err := conn.ExecContext(context.Background(), `
			DELETE FROM knowledge_attempt_audit_events
			WHERE tenant_id = 'tenant-replace' AND sequence = 1
		`); err == nil {
			t.Fatalf("event delete succeeded on attempt %d", attempt)
		}
	}
	events := readEvents(t, database, "tenant-replace")
	if len(events) != 1 || events[0].Actor.ID != "administrator" {
		t.Fatalf("event was replaced: %+v", events)
	}
}

func TestSequenceExhaustionFailsClosedWithoutWrapping(t *testing.T) {
	t.Parallel()
	database := openTestDatabase(t)
	store := newTestStore(t, database)
	admin := actorContext(t, audit.ActorRoleAdministrator, "administrator")
	if _, err := database.SQLDB().ExecContext(t.Context(), `
		INSERT INTO knowledge_attempt_audit_tenant_state (
			tenant_id, first_sequence, next_sequence, retained_count
		) VALUES ('tenant-exhausted', 1, 1, 0)
	`); err != nil {
		t.Fatal(err)
	}
	if _, err := database.SQLDB().ExecContext(t.Context(), `DROP TRIGGER knowledge_attempt_audit_state_transition_is_valid ON knowledge_attempt_audit_tenant_state`); err != nil {
		t.Fatal(err)
	}
	if _, err := database.SQLDB().ExecContext(t.Context(), `
		UPDATE knowledge_attempt_audit_tenant_state
		SET first_sequence = $1, next_sequence = $2
		WHERE tenant_id = 'tenant-exhausted'
	`, int64(math.MaxInt64), int64(math.MaxInt64)); err != nil {
		t.Fatal(err)
	}
	if err := store.AppendRejected(admin, "tenant-exhausted", adminDefinition(ActionValidate, ReasonResourceLimit, 0)); !errors.Is(err, control.ErrCapacityExceeded) {
		t.Fatalf("AppendRejected(exhausted) = %v, want ErrCapacityExceeded", err)
	}
	var state tenantStateRecord
	if err := database.GORMDB().Where("tenant_id = ?", "tenant-exhausted").Take(&state).Error; err != nil {
		t.Fatal(err)
	}
	if state.FirstSequence != math.MaxInt64 || state.NextSequence != math.MaxInt64 || state.RetainedCount != 0 {
		t.Fatalf("exhausted state changed: %+v", state)
	}
}

func TestStartupDetectsTamperingWithoutReturningPartialState(t *testing.T) {
	t.Parallel()
	database := openTestDatabase(t)
	store := newTestStore(t, database)
	admin := actorContext(t, audit.ActorRoleAdministrator, "administrator")
	for index := range 3 {
		if err := store.AppendRejected(admin, "tenant-tamper", adminDefinition(ActionValidate, ReasonInvalidDefinition, time.Duration(index))); err != nil {
			t.Fatal(err)
		}
	}
	conn, err := database.SQLDB().Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pgtest.Checks(context.Background(), conn, false); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.ExecContext(context.Background(), `DROP TRIGGER knowledge_attempt_audit_event_update_is_forbidden ON knowledge_attempt_audit_events`); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.ExecContext(context.Background(), `UPDATE knowledge_attempt_audit_events SET actor_kind = 'system' WHERE tenant_id = 'tenant-tamper' AND sequence = 2`); err != nil {
		t.Fatal(err)
	}
	if err := conn.Close(); err != nil {
		t.Fatal(err)
	}
	if reopened, err := New(database); reopened != nil || !errors.Is(err, ErrCorrupt) {
		t.Fatalf("New(tampered) = (%v, %v), want ErrCorrupt", reopened, err)
	}
}

func TestBackupRestorePreservesExactWindowAndIntegrity(t *testing.T) {
	database := openTestDatabase(t)
	store := newTestStore(t, database)
	admin := actorContext(t, audit.ActorRoleAdministrator, "administrator")
	for index := range 4 {
		if err := store.AppendRejected(admin, "tenant-backup", adminDefinition(ActionPreview, ReasonResourceLimit, time.Duration(index)*time.Microsecond)); err != nil {
			t.Fatal(err)
		}
	}
	destination := filepath.Join(t.TempDir(), "backup.pgdump")
	if err := database.BackupTo(context.Background(), destination); err != nil {
		t.Fatalf("BackupTo: %v", err)
	}
	var sourceDatabase, sourceSchema string
	if err := database.SQLDB().QueryRowContext(t.Context(), "SELECT current_database(),current_schema()").Scan(&sourceDatabase, &sourceSchema); err != nil {
		t.Fatal(err)
	}
	source, err := url.Parse(os.Getenv("OPEN_SPLUNK_TEST_POSTGRES_DSN"))
	if err != nil {
		t.Fatal(err)
	}
	source.Path = "/" + sourceDatabase
	parameters := source.Query()
	parameters.Set("search_path", sourceSchema)
	source.RawQuery = parameters.Encode()
	target := pgtest.EmptyCopy(t, source.String())
	if err := control.RestoreFrom(t.Context(), target, destination); err != nil {
		t.Fatal(err)
	}
	restored, err := control.OpenReadOnly(context.Background(), target)
	if err != nil {
		t.Fatalf("OpenReadOnly: %v", err)
	}
	defer restored.Close()
	if _, err := New(restored); err != nil {
		t.Fatalf("New(restored): %v", err)
	}
	originalEvents := readEvents(t, database, "tenant-backup")
	restoredEvents := readEvents(t, restored, "tenant-backup")
	if fmt.Sprint(originalEvents) != fmt.Sprint(restoredEvents) {
		t.Fatalf("restored events differ:\noriginal=%+v\nrestored=%+v", originalEvents, restoredEvents)
	}
}

func insertDirectAttempt(t *testing.T, conn *sql.Conn, tenantID string, sequence int64) {
	t.Helper()
	if _, err := conn.ExecContext(context.Background(), `
		INSERT INTO knowledge_attempt_audit_events (
			tenant_id, sequence, occurred_at_unix_micro,
			actor_kind, actor_id, actor_role, action, result, reason,
			app_id, knowledge_object_id, object_type, object_version, sharing_scope
		) VALUES ($1, $2, $3, 'browser', 'administrator', 'administrator',
		          'validate', 'rejected', 'invalid_definition',
		          NULL, NULL, NULL, NULL, NULL)
	`, tenantID, sequence, sequence+1_000_000); err != nil {
		t.Fatalf("insert sequence %d: %v", sequence, err)
	}
}

func assertRawState(t *testing.T, conn *sql.Conn, tenantID string, first, next, count int64) {
	t.Helper()
	var got tenantStateRecord
	if err := conn.QueryRowContext(context.Background(), `
		SELECT tenant_id, first_sequence, next_sequence, retained_count
		FROM knowledge_attempt_audit_tenant_state WHERE tenant_id = $1
	`, tenantID).Scan(&got.TenantID, &got.FirstSequence, &got.NextSequence,
		&got.RetainedCount); err != nil {
		t.Fatal(err)
	}
	if got.FirstSequence != first || got.NextSequence != next ||
		got.RetainedCount != count {
		t.Fatalf("state = %+v, want first=%d next=%d count=%d", got, first, next, count)
	}
}

func assertRawSequenceAbsent(t *testing.T, conn *sql.Conn, tenantID string, sequence int64) {
	t.Helper()
	var count int64
	if err := conn.QueryRowContext(context.Background(), `
		SELECT COUNT(*) FROM knowledge_attempt_audit_events
		WHERE tenant_id = $1 AND sequence = $2
	`, tenantID, sequence).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("sequence %d was not evicted", sequence)
	}
}
