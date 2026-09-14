package auth

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/control"
)

func TestSetCollectorTokenEnabledReportsBeginContentionWithoutMutation(t *testing.T) {
	ctx := context.Background()
	database := openControlDB(t)
	if _, err := database.CreateIndex(ctx, activeIndex("main")); err != nil {
		t.Fatalf("CreateIndex(main): %v", err)
	}
	store, err := NewStore(
		database,
		[]byte("collector-token-state-contention-key"),
	)
	if err != nil {
		t.Fatalf("NewStore(): %v", err)
	}
	issued, err := store.CreateCollectorToken(ctx, CreateCollectorTokenRequest{
		Name:              "contended state token",
		AllowedIndexNames: []string{"main"},
		BoundCollectorID:  testCollectorID,
	})
	if err != nil {
		t.Fatalf("CreateCollectorToken(): %v", err)
	}

	raw := database.SQLDB()
	raw.SetMaxOpenConns(2)
	blocker, err := raw.Conn(ctx)
	if err != nil {
		t.Fatalf("acquire state mutation blocker: %v", err)
	}
	blockerOpen := true
	t.Cleanup(func() {
		if !blockerOpen {
			return
		}
		_, _ = blocker.ExecContext(context.Background(), `ROLLBACK`)
		_ = blocker.Close()
	})
	if _, err := blocker.ExecContext(ctx, `BEGIN IMMEDIATE`); err != nil {
		t.Fatalf("reserve state mutation writer: %v", err)
	}
	contender, err := raw.Conn(ctx)
	if err != nil {
		t.Fatalf("acquire state mutation contender: %v", err)
	}
	if _, err := contender.ExecContext(ctx, `PRAGMA busy_timeout = 1`); err != nil {
		_ = contender.Close()
		t.Fatalf("shorten state mutation busy timeout: %v", err)
	}
	if err := contender.Close(); err != nil {
		t.Fatalf("release configured state mutation contender: %v", err)
	}

	_, err = store.SetCollectorTokenEnabled(
		ctx,
		issued.Token.ID,
		issued.Token.Version,
		false,
	)
	if err == nil || !strings.HasPrefix(err.Error(), "begin collector token state update:") ||
		!control.IsDatabaseContention(err) {
		t.Fatalf(
			"SetCollectorTokenEnabled(contended) error = %v, want begin contention",
			err,
		)
	}
	var databaseErr interface{ Code() int }
	if !errors.As(err, &databaseErr) || databaseErr.Code()&0xff != 5 {
		t.Fatalf(
			"SetCollectorTokenEnabled(contended) database error = %v, want SQLITE_BUSY (5)",
			err,
		)
	}
	stored, err := store.GetCollectorToken(ctx, issued.Token.ID)
	if err != nil {
		t.Fatalf("GetCollectorToken(after contention): %v", err)
	}
	if stored.Version != issued.Token.Version || stored.State != CollectorTokenStateActive {
		t.Fatalf("contended state mutation changed token = %#v", stored)
	}
	var auditCount int
	if err := raw.QueryRowContext(
		ctx,
		`SELECT count(*) FROM audit_events WHERE target_id = ?`,
		issued.Token.ID,
	).Scan(&auditCount); err != nil {
		t.Fatalf("count audit events after contention: %v", err)
	}
	if auditCount != 1 {
		t.Fatalf("audit event count after contention = %d, want creation event only", auditCount)
	}

	if _, err := blocker.ExecContext(ctx, `ROLLBACK`); err != nil {
		t.Fatalf("release state mutation writer: %v", err)
	}
	if err := blocker.Close(); err != nil {
		t.Fatalf("close state mutation blocker: %v", err)
	}
	blockerOpen = false
	if _, err := store.SetCollectorTokenEnabled(
		ctx,
		issued.Token.ID,
		issued.Token.Version,
		false,
	); err != nil {
		t.Fatalf("SetCollectorTokenEnabled(after contention): %v", err)
	}
}

func TestSetCollectorTokenEnabledTransitionsAndFencesAuthentication(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	database := openControlDB(t)
	if _, err := database.CreateIndex(ctx, activeIndex("main")); err != nil {
		t.Fatalf("CreateIndex(main): %v", err)
	}
	store, err := NewStore(
		database,
		[]byte("collector-token-state-transition-key"),
	)
	if err != nil {
		t.Fatalf("NewStore(): %v", err)
	}
	anchor := time.Date(2026, time.August, 10, 9, 30, 0, 123456000, time.UTC)
	store.now = func() time.Time { return anchor }
	issued, err := store.CreateCollectorToken(ctx, CreateCollectorTokenRequest{
		Name:              "stateful native token",
		AllowedIndexNames: []string{"main"},
		BoundCollectorID:  testCollectorID,
		ExpiresAt:         anchor.Add(time.Hour),
	})
	if err != nil {
		t.Fatalf("CreateCollectorToken(): %v", err)
	}
	plaintext := issued.Secret.Plaintext()
	if _, err := store.Authenticate(ctx, plaintext); err != nil {
		t.Fatalf("Authenticate(active): %v", err)
	}

	disabled, err := store.SetCollectorTokenEnabled(
		ctx,
		issued.Token.ID,
		issued.Token.Version,
		false,
	)
	if err != nil {
		t.Fatalf("SetCollectorTokenEnabled(disable): %v", err)
	}
	if disabled.Version != 2 ||
		disabled.State != CollectorTokenStateDisabled ||
		!disabled.RevokedAt.IsZero() ||
		!disabled.UpdatedAt.Equal(anchor) {
		t.Fatalf("disabled token = %#v", disabled)
	}
	if _, err := store.Authenticate(ctx, plaintext); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("Authenticate(disabled) error = %v, want ErrUnauthorized", err)
	}
	if _, err := store.SetCollectorTokenEnabled(
		ctx,
		disabled.ID,
		issued.Token.Version,
		true,
	); !errors.Is(err, control.ErrVersionConflict) {
		t.Fatalf("SetCollectorTokenEnabled(stale) error = %v, want ErrVersionConflict", err)
	}

	enabled, err := store.SetCollectorTokenEnabled(
		ctx,
		disabled.ID,
		disabled.Version,
		true,
	)
	if err != nil {
		t.Fatalf("SetCollectorTokenEnabled(enable): %v", err)
	}
	if enabled.Version != 3 ||
		enabled.State != CollectorTokenStateActive ||
		!enabled.RevokedAt.IsZero() {
		t.Fatalf("enabled token = %#v", enabled)
	}
	if _, err := store.Authenticate(ctx, plaintext); err != nil {
		t.Fatalf("Authenticate(re-enabled): %v", err)
	}

	disabled, err = store.SetCollectorTokenEnabled(
		ctx,
		enabled.ID,
		enabled.Version,
		false,
	)
	if err != nil {
		t.Fatalf("SetCollectorTokenEnabled(disable before expiry): %v", err)
	}
	rows, err := database.SQLDB().QueryContext(ctx, `
		SELECT action, target_version
		FROM audit_events
		WHERE target_id = ? AND sequence > 1
		ORDER BY sequence`, issued.Token.ID)
	if err != nil {
		t.Fatalf("query token state audit events: %v", err)
	}
	for wantVersion := uint64(2); wantVersion <= 4; wantVersion++ {
		if !rows.Next() {
			t.Fatalf("missing audit event for token version %d", wantVersion)
		}
		var action string
		var targetVersion uint64
		if err := rows.Scan(&action, &targetVersion); err != nil {
			t.Fatalf("scan token state audit event: %v", err)
		}
		if action != "ingestion_token.update" || targetVersion != wantVersion {
			t.Fatalf(
				"token state audit event = %q/version %d, want update/version %d",
				action,
				targetVersion,
				wantVersion,
			)
		}
	}
	if rows.Next() {
		t.Fatal("unexpected extra token state audit event")
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate token state audit events: %v", err)
	}
	if err := rows.Close(); err != nil {
		t.Fatalf("close token state audit events: %v", err)
	}
	store.now = func() time.Time { return anchor.Add(time.Hour) }
	if _, err := store.SetCollectorTokenEnabled(
		ctx,
		disabled.ID,
		disabled.Version,
		true,
	); !errors.Is(err, ErrInactiveToken) {
		t.Fatalf("SetCollectorTokenEnabled(expired) error = %v, want ErrInactiveToken", err)
	}
	stored, err := store.GetCollectorToken(ctx, disabled.ID)
	if err != nil {
		t.Fatalf("GetCollectorToken(expired disabled): %v", err)
	}
	if stored.Version != disabled.Version || stored.State != CollectorTokenStateDisabled {
		t.Fatalf("expired disabled token mutated = %#v", stored)
	}
	var auditCount int
	if err := database.SQLDB().QueryRowContext(
		ctx,
		`SELECT count(*) FROM audit_events`,
	).Scan(&auditCount); err != nil {
		t.Fatalf("count audit events after expired transition: %v", err)
	}
	if auditCount != 4 {
		t.Fatalf("audit event count after expired transition = %d, want 4", auditCount)
	}
}

func TestSetCollectorTokenEnabledCannotReactivateRevokedToken(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	database := openControlDB(t)
	if _, err := database.CreateIndex(ctx, activeIndex("main")); err != nil {
		t.Fatalf("CreateIndex(main): %v", err)
	}
	store, err := NewStore(
		database,
		[]byte("collector-token-revoked-state-key"),
	)
	if err != nil {
		t.Fatalf("NewStore(): %v", err)
	}
	issued, err := store.CreateCollectorToken(ctx, CreateCollectorTokenRequest{
		Name:              "revoked state token",
		AllowedIndexNames: []string{"main"},
		BoundCollectorID:  testCollectorID,
	})
	if err != nil {
		t.Fatalf("CreateCollectorToken(): %v", err)
	}
	revoked, err := store.RevokeCollectorToken(
		ctx,
		issued.Token.ID,
		issued.Token.Version,
	)
	if err != nil {
		t.Fatalf("RevokeCollectorToken(): %v", err)
	}
	if _, err := store.SetCollectorTokenEnabled(
		ctx,
		revoked.ID,
		revoked.Version,
		true,
	); !errors.Is(err, ErrInactiveToken) {
		t.Fatalf("SetCollectorTokenEnabled(revoked) error = %v, want ErrInactiveToken", err)
	}
	stored, err := store.GetCollectorToken(ctx, revoked.ID)
	if err != nil {
		t.Fatalf("GetCollectorToken(revoked): %v", err)
	}
	if stored.Version != revoked.Version ||
		stored.State != CollectorTokenStateRevoked ||
		stored.RevokedAt.IsZero() {
		t.Fatalf("revoked token mutated = %#v", stored)
	}
}

func TestSetCollectorTokenEnabledRollsBackWithoutAuditEvent(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	database := openControlDB(t)
	if _, err := database.CreateIndex(ctx, activeIndex("main")); err != nil {
		t.Fatalf("CreateIndex(main): %v", err)
	}
	store, err := NewStore(
		database,
		[]byte("collector-token-state-audit-key-material"),
	)
	if err != nil {
		t.Fatalf("NewStore(): %v", err)
	}
	issued, err := store.CreateCollectorToken(ctx, CreateCollectorTokenRequest{
		Name:              "audited state token",
		AllowedIndexNames: []string{"main"},
		BoundCollectorID:  testCollectorID,
	})
	if err != nil {
		t.Fatalf("CreateCollectorToken(): %v", err)
	}
	if _, err := database.SQLDB().ExecContext(ctx, `
		CREATE TRIGGER test_force_token_state_audit_failure
		BEFORE INSERT ON audit_events
		BEGIN
			SELECT RAISE(ABORT, 'forced token state audit failure');
		END`); err != nil {
		t.Fatalf("create audit failure trigger: %v", err)
	}
	if _, err := store.SetCollectorTokenEnabled(
		ctx,
		issued.Token.ID,
		issued.Token.Version,
		false,
	); err == nil {
		t.Fatal("SetCollectorTokenEnabled() succeeded without its audit event")
	}
	stored, err := store.GetCollectorToken(ctx, issued.Token.ID)
	if err != nil {
		t.Fatalf("GetCollectorToken(): %v", err)
	}
	if stored.Version != issued.Token.Version || stored.State != CollectorTokenStateActive {
		t.Fatalf("state transition survived audit rollback = %#v", stored)
	}
	var auditCount int
	if err := database.SQLDB().QueryRowContext(
		ctx,
		`SELECT count(*) FROM audit_events`,
	).Scan(&auditCount); err != nil {
		t.Fatalf("count audit events: %v", err)
	}
	if auditCount != 1 {
		t.Fatalf("audit event count = %d, want only creation event", auditCount)
	}
}
