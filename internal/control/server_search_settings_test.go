package control

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/searchlimits"
	"gorm.io/gorm"
)

type serverSettingsAuditAppenderFunc func(context.Context, *gorm.DB, string, ServerSettingsMutationAuditEvent) error

func (appender serverSettingsAuditAppenderFunc) AppendServerSettingsMutationInTransaction(
	ctx context.Context,
	tx *gorm.DB,
	tenantID string,
	event ServerSettingsMutationAuditEvent,
) error {
	return appender(ctx, tx, tenantID, event)
}

func TestServerSearchSettingsDefaultPersistenceConflictAndRollback(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "control.sqlite")
	database, err := Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	var auditObjects int
	if err := database.SQLDB().QueryRowContext(ctx, `
		SELECT count(*) FROM sqlite_schema
		WHERE name IN (
			'audit_event_identity_collision_is_forbidden',
			'audit_event_insert_requires_current_tenant_state',
			'audit_event_advances_tenant_state',
			'audit_event_update_is_forbidden',
			'audit_event_delete_is_forbidden',
			'audit_events_tenant_action_sequence_idx',
			'audit_events_tenant_actor_sequence_idx',
			'audit_events_tenant_target_sequence_idx'
		)`).Scan(&auditObjects); err != nil {
		t.Fatal(err)
	}
	if auditObjects != 8 {
		t.Fatalf("post-migration audit objects = %d, want 8", auditObjects)
	}
	var staleReferences int
	if err := database.SQLDB().QueryRowContext(ctx, `
		SELECT count(*) FROM sqlite_schema
		WHERE sql LIKE '%audit_events_with_server_settings%'
		   OR sql LIKE '%audit_events_before_server_settings%'`).Scan(&staleReferences); err != nil {
		t.Fatal(err)
	}
	if staleReferences != 0 {
		rows, queryErr := database.SQLDB().QueryContext(ctx, `SELECT name FROM sqlite_schema WHERE sql LIKE '%audit_events_before_server_settings%'`)
		if queryErr != nil {
			t.Fatal(queryErr)
		}
		defer rows.Close()
		var names []string
		for rows.Next() {
			var name string
			if scanErr := rows.Scan(&name); scanErr != nil {
				t.Fatal(scanErr)
			}
			names = append(names, name)
		}
		t.Fatalf("post-migration schema retains %d temporary audit references: %v", staleReferences, names)
	}
	foreignKeyRows, err := database.SQLDB().QueryContext(ctx, `PRAGMA foreign_key_check`)
	if err != nil {
		t.Fatal(err)
	}
	defer foreignKeyRows.Close()
	if foreignKeyRows.Next() {
		var table string
		var rowID int64
		var parent string
		var foreignKeyID int
		if err := foreignKeyRows.Scan(&table, &rowID, &parent, &foreignKeyID); err != nil {
			t.Fatal(err)
		}
		t.Fatalf("post-migration foreign key violation: table=%s row=%d parent=%s constraint=%d", table, rowID, parent, foreignKeyID)
	}
	if err := foreignKeyRows.Err(); err != nil {
		t.Fatal(err)
	}
	auditCalls := 0
	appender := serverSettingsAuditAppenderFunc(func(_ context.Context, tx *gorm.DB, tenant string, event ServerSettingsMutationAuditEvent) error {
		auditCalls++
		if tx == nil || tenant != "tenant" || event.Version == 0 || event.Target != ServerSettingsTargetSearchLimits {
			return errors.New("invalid audit projection")
		}
		return nil
	})
	store, err := NewServerSearchSettingsStore(database, "tenant", appender)
	if err != nil {
		t.Fatal(err)
	}
	initial, err := store.Get(ctx)
	if err != nil || initial.Version != 0 || initial.Limits != searchlimits.Default() {
		t.Fatalf("Get(default) = (%+v, %v)", initial, err)
	}
	updatedLimits := searchlimits.Default()
	updatedLimits.MaxRuntime = 5 * time.Minute
	updated, err := store.Update(ctx, 0, updatedLimits)
	if err != nil || updated.Version != 1 || updated.Limits != updatedLimits || auditCalls != 1 {
		t.Fatalf("Update() = (%+v, %v), audit calls %d", updated, err, auditCalls)
	}
	if _, err := store.Update(ctx, 0, searchlimits.Default()); !errors.Is(err, ErrVersionConflict) {
		t.Fatalf("stale Update() error = %v", err)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	restarted, err := NewServerSearchSettingsStore(reopened, "tenant", appender)
	if err != nil {
		t.Fatal(err)
	}
	persisted, err := restarted.Get(ctx)
	if err != nil || persisted.Version != 1 || persisted.Limits != updatedLimits {
		t.Fatalf("Get(restarted) = (%+v, %v)", persisted, err)
	}

	failing, err := NewServerSearchSettingsStore(reopened, "tenant", serverSettingsAuditAppenderFunc(
		func(context.Context, *gorm.DB, string, ServerSettingsMutationAuditEvent) error {
			return errors.New("audit unavailable")
		},
	))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := failing.Update(ctx, 1, searchlimits.Default()); err == nil {
		t.Fatal("Update() succeeded with failed audit write")
	}
	afterFailure, err := restarted.Get(ctx)
	if err != nil || afterFailure.Version != 1 || afterFailure.Limits != updatedLimits {
		t.Fatalf("failed audit changed settings: (%+v, %v)", afterFailure, err)
	}
}

func TestServerSearchSettingsRecordRejectsUnsignedOverflow(t *testing.T) {
	tests := []struct {
		name string
		set  func(*searchlimits.Policy)
	}{
		{"maximum memory bytes", func(policy *searchlimits.Policy) { policy.MaxMemoryBytes = ^uint64(0) }},
		{"maximum rows to read", func(policy *searchlimits.Policy) { policy.MaxRowsToRead = ^uint64(0) }},
		{"maximum bytes to read", func(policy *searchlimits.Policy) { policy.MaxBytesToRead = ^uint64(0) }},
		{"maximum grouped rows", func(policy *searchlimits.Policy) { policy.MaxGroupedRows = ^uint64(0) }},
		{"maximum threads", func(policy *searchlimits.Policy) { policy.MaxThreads = ^uint64(0) }},
		{"maximum result rows", func(policy *searchlimits.Policy) { policy.MaxResultRows = ^uint64(0) }},
		{"maximum result bytes", func(policy *searchlimits.Policy) { policy.MaxResultBytes = ^uint64(0) }},
		{"maximum total result bytes", func(policy *searchlimits.Policy) { policy.MaxTotalResultBytes = ^uint64(0) }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			limits := searchlimits.Default()
			test.set(&limits)
			if _, err := serverSearchSettingsRecordFrom(1, limits, time.Unix(1, 0)); !errors.Is(err, ErrInvalidArgument) {
				t.Fatalf("serverSearchSettingsRecordFrom() error = %v, want ErrInvalidArgument", err)
			}
		})
	}
}
