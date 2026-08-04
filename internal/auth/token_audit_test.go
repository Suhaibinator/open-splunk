package auth

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/audit"
	"github.com/Suhaibinator/open-splunk/internal/control"
)

func TestCollectorTokenMutationsAppendAtomicSecretFreeAuditEvents(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	database := openControlDB(t)
	if _, err := database.CreateIndex(ctx, activeIndex("main")); err != nil {
		t.Fatalf("CreateIndex(main): %v", err)
	}
	store, err := NewStore(
		database,
		[]byte("collector-token-audit-digest-key"),
	)
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	anchor := time.Date(2026, time.August, 3, 19, 20, 21, 654321000, time.UTC)
	store.now = func() time.Time { return anchor }

	issued, err := store.CreateCollectorToken(ctx, CreateCollectorTokenRequest{
		Name:              "audited collector",
		Description:       "safe mutable metadata",
		AllowedIndexNames: []string{"main"},
		BoundCollectorID:  testCollectorID,
	})
	if err != nil {
		t.Fatalf("CreateCollectorToken: %v", err)
	}
	plaintext := issued.Secret.Plaintext()
	updated, err := store.UpdateCollectorToken(
		ctx,
		issued.Token.ID,
		issued.Token.Version,
		UpdateCollectorTokenRequest{
			Name:              "renamed collector",
			Description:       "replacement metadata",
			AllowedIndexNames: []string{"main"},
			BoundCollectorID:  testCollectorID,
		},
	)
	if err != nil {
		t.Fatalf("UpdateCollectorToken: %v", err)
	}
	revoked, err := store.RevokeCollectorToken(
		ctx,
		updated.ID,
		updated.Version,
	)
	if err != nil {
		t.Fatalf("RevokeCollectorToken: %v", err)
	}

	type auditRow struct {
		sequence, targetVersion                 uint64
		tenantID, actorKind, actorID, actorRole string
		action, targetKind, targetID            string
		occurredAtUnixMicro                     int64
	}
	rows, err := database.SQLDB().QueryContext(ctx, `
		SELECT sequence, tenant_id, occurred_at_unix_micro,
		       actor_kind, actor_id, actor_role, action,
		       target_kind, target_id, target_version
		FROM audit_events
		ORDER BY tenant_id, sequence`)
	if err != nil {
		t.Fatalf("query token audit events: %v", err)
	}
	defer rows.Close()
	var got []auditRow
	for rows.Next() {
		var row auditRow
		if err := rows.Scan(
			&row.sequence,
			&row.tenantID,
			&row.occurredAtUnixMicro,
			&row.actorKind,
			&row.actorID,
			&row.actorRole,
			&row.action,
			&row.targetKind,
			&row.targetID,
			&row.targetVersion,
		); err != nil {
			t.Fatalf("scan token audit event: %v", err)
		}
		got = append(got, row)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate token audit events: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("token audit event count = %d, want 3: %#v", len(got), got)
	}
	wantActions := []string{
		"ingestion_token.create",
		"ingestion_token.update",
		"ingestion_token.revoke",
	}
	for index, row := range got {
		if row.sequence != uint64(index+1) ||
			row.tenantID != "default" ||
			row.occurredAtUnixMicro != anchor.UnixMicro() ||
			row.actorKind != "system" ||
			row.actorID == "" ||
			row.actorRole != "system" ||
			row.action != wantActions[index] ||
			row.targetKind != "ingestion_token" ||
			row.targetID != issued.Token.ID ||
			row.targetVersion != uint64(index+1) {
			t.Fatalf("token audit event %d = %#v", index, row)
		}
		serialized := strings.Join([]string{
			row.tenantID,
			row.actorKind,
			row.actorID,
			row.actorRole,
			row.action,
			row.targetKind,
			row.targetID,
		}, "\x00")
		if strings.Contains(serialized, plaintext) {
			t.Fatalf("token audit event %d contains plaintext credential", index)
		}
	}
	if revoked.Version != 3 {
		t.Fatalf("revoked version = %d, want 3", revoked.Version)
	}

	before := len(got)
	_, err = store.UpdateCollectorToken(
		ctx,
		issued.Token.ID,
		1,
		UpdateCollectorTokenRequest{
			Name:              "stale update",
			AllowedIndexNames: []string{"main"},
			BoundCollectorID:  testCollectorID,
		},
	)
	if !errors.Is(err, control.ErrVersionConflict) {
		t.Fatalf("stale UpdateCollectorToken error = %v, want ErrVersionConflict", err)
	}
	var after int
	if err := database.SQLDB().QueryRowContext(ctx, `SELECT count(*) FROM audit_events`).Scan(&after); err != nil {
		t.Fatalf("count audit events after rejected update: %v", err)
	}
	if after != before {
		t.Fatalf("rejected update appended audit event: before %d after %d", before, after)
	}

	columnRows, err := database.SQLDB().QueryContext(ctx, `
		SELECT name FROM pragma_table_info('audit_events') ORDER BY cid`)
	if err != nil {
		t.Fatalf("inspect audit columns: %v", err)
	}
	defer columnRows.Close()
	var columns []string
	for columnRows.Next() {
		var column string
		if err := columnRows.Scan(&column); err != nil {
			t.Fatalf("scan audit column: %v", err)
		}
		columns = append(columns, column)
	}
	if err := columnRows.Err(); err != nil {
		t.Fatalf("iterate audit columns: %v", err)
	}
	wantColumns := []string{
		"tenant_id",
		"sequence",
		"occurred_at_unix_micro",
		"actor_kind",
		"actor_id",
		"actor_role",
		"action",
		"target_kind",
		"target_id",
		"target_version",
	}
	if !slices.Equal(columns, wantColumns) {
		t.Fatalf("audit columns = %v, want exact secret-free shape %v", columns, wantColumns)
	}
}

func TestCollectorTokenCreationRollsBackWhenAuditAppendFails(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	database := openControlDB(t)
	if _, err := database.CreateIndex(ctx, activeIndex("main")); err != nil {
		t.Fatalf("CreateIndex(main): %v", err)
	}
	if _, err := database.SQLDB().ExecContext(ctx, `
		CREATE TRIGGER test_force_audit_insert_failure
		BEFORE INSERT ON audit_events
		BEGIN
			SELECT RAISE(ABORT, 'forced audit persistence failure');
		END`); err != nil {
		t.Fatalf("create audit failure trigger: %v", err)
	}
	store, err := NewStore(
		database,
		[]byte("collector-token-audit-failure-key"),
	)
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	_, err = store.CreateCollectorToken(ctx, CreateCollectorTokenRequest{
		Name:              "must roll back",
		AllowedIndexNames: []string{"main"},
		BoundCollectorID:  testCollectorID,
	})
	if err == nil {
		t.Fatal("CreateCollectorToken succeeded without its audit event")
	}
	if strings.Contains(strings.ToLower(err.Error()), "digest") ||
		strings.Contains(err.Error(), collectorTokenPrefix) {
		t.Fatalf("audit failure exposed credential material: %v", err)
	}
	for table, want := range map[string]int{
		"ingestion_tokens": 0,
		"audit_events":     0,
	} {
		var count int
		query := `SELECT count(*) FROM ` + table
		if err := database.SQLDB().QueryRowContext(ctx, query).Scan(&count); err != nil {
			t.Fatalf("count %s after rollback: %v", table, err)
		}
		if count != want {
			t.Fatalf("%s count after audit failure = %d, want %d", table, count, want)
		}
	}
}

func TestCollectorTokenUpdateAndRevocationRollBackWhenAuditAppendFails(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	database := openControlDB(t)
	if _, err := database.CreateIndex(ctx, activeIndex("main")); err != nil {
		t.Fatalf("CreateIndex(main): %v", err)
	}
	if _, err := database.CreateIndex(ctx, activeIndex("other")); err != nil {
		t.Fatalf("CreateIndex(other): %v", err)
	}
	store, err := NewStore(
		database,
		[]byte("collector-token-audit-rollback-key"),
	)
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	issued, err := store.CreateCollectorToken(ctx, CreateCollectorTokenRequest{
		Name:               "original",
		Description:        "unchanged",
		AllowedIndexNames:  []string{"main"},
		AllowedHostRegexes: []string{"^before$"},
		BoundCollectorID:   testCollectorID,
	})
	if err != nil {
		t.Fatalf("CreateCollectorToken: %v", err)
	}

	installFailure := func() {
		t.Helper()
		if _, err := database.SQLDB().ExecContext(ctx, `
			CREATE TRIGGER test_force_later_audit_insert_failure
			BEFORE INSERT ON audit_events
			BEGIN
				SELECT RAISE(ABORT, 'forced later audit persistence failure');
			END`); err != nil {
			t.Fatalf("create audit failure trigger: %v", err)
		}
	}
	dropFailure := func() {
		t.Helper()
		if _, err := database.SQLDB().ExecContext(
			ctx,
			`DROP TRIGGER test_force_later_audit_insert_failure`,
		); err != nil {
			t.Fatalf("drop audit failure trigger: %v", err)
		}
	}
	assertStored := func(
		wantName string,
		wantVersion uint64,
		wantState CollectorTokenState,
		wantIndex string,
		wantHostRegex string,
	) {
		t.Helper()
		stored, err := store.GetCollectorToken(ctx, issued.Token.ID)
		if err != nil {
			t.Fatalf("GetCollectorToken: %v", err)
		}
		if stored.Name != wantName || stored.Version != wantVersion ||
			stored.State != wantState ||
			!slices.Equal(stored.AllowedIndexNames, []string{wantIndex}) ||
			!slices.Equal(stored.AllowedHostRegexes, []string{wantHostRegex}) {
			t.Fatalf("stored token = %+v, want name/version/state %q/%d/%q", stored, wantName, wantVersion, wantState)
		}
		var eventCount int
		if err := database.SQLDB().QueryRowContext(
			ctx,
			`SELECT count(*) FROM audit_events`,
		).Scan(&eventCount); err != nil {
			t.Fatalf("count audit events: %v", err)
		}
		if eventCount != int(wantVersion) {
			t.Fatalf("audit event count = %d, want %d", eventCount, wantVersion)
		}
	}

	installFailure()
	updateRequest := UpdateCollectorTokenRequest{
		Name:               "updated",
		Description:        "replacement",
		AllowedIndexNames:  []string{"other"},
		AllowedHostRegexes: []string{"^after$"},
		BoundCollectorID:   testCollectorID,
	}
	if _, err := store.UpdateCollectorToken(
		ctx,
		issued.Token.ID,
		issued.Token.Version,
		updateRequest,
	); err == nil {
		t.Fatal("UpdateCollectorToken succeeded without its audit event")
	}
	assertStored("original", 1, CollectorTokenStateActive, "main", "^before$")
	dropFailure()

	updated, err := store.UpdateCollectorToken(
		ctx,
		issued.Token.ID,
		issued.Token.Version,
		updateRequest,
	)
	if err != nil {
		t.Fatalf("UpdateCollectorToken after restoring audit: %v", err)
	}
	assertStored("updated", 2, CollectorTokenStateActive, "other", "^after$")

	installFailure()
	if _, err := store.RevokeCollectorToken(
		ctx,
		updated.ID,
		updated.Version,
	); err == nil {
		t.Fatal("RevokeCollectorToken succeeded without its audit event")
	}
	assertStored("updated", 2, CollectorTokenStateActive, "other", "^after$")
}

func TestCollectorTokenRevocationPruningRollsBackWhenAuditAppendFails(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	database := openControlDB(t)
	if _, err := database.CreateIndex(ctx, activeIndex("main")); err != nil {
		t.Fatalf("CreateIndex(main): %v", err)
	}
	store, err := NewStoreWithOptions(
		database,
		[]byte("collector-token-audit-pruning-key"),
		StoreOptions{RetainedRevokedTokenLimit: 2, TotalTokenRecordLimit: 4},
	)
	if err != nil {
		t.Fatalf("NewStoreWithOptions: %v", err)
	}
	issued := make([]IssuedCollectorToken, 3)
	for index := range issued {
		issued[index], err = store.CreateCollectorToken(ctx, CreateCollectorTokenRequest{
			Name:              "pruning audit " + string(rune('a'+index)),
			AllowedIndexNames: []string{"main"},
			BoundCollectorID:  testCollectorID,
		})
		if err != nil {
			t.Fatalf("CreateCollectorToken(%d): %v", index, err)
		}
	}
	for index := 0; index < 2; index++ {
		if _, err := store.RevokeCollectorToken(
			ctx,
			issued[index].Token.ID,
			issued[index].Token.Version,
		); err != nil {
			t.Fatalf("RevokeCollectorToken(%d): %v", index, err)
		}
	}
	if _, err := database.SQLDB().ExecContext(ctx, `
		CREATE TRIGGER test_force_pruning_audit_insert_failure
		BEFORE INSERT ON audit_events
		BEGIN
			SELECT RAISE(ABORT, 'forced pruning audit persistence failure');
		END`); err != nil {
		t.Fatalf("create audit failure trigger: %v", err)
	}
	if _, err := store.RevokeCollectorToken(
		ctx,
		issued[2].Token.ID,
		issued[2].Token.Version,
	); err == nil {
		t.Fatal("RevokeCollectorToken succeeded after audit persistence failed")
	}

	for index, want := range []struct {
		state   CollectorTokenState
		version uint64
	}{
		{state: CollectorTokenStateRevoked, version: 2},
		{state: CollectorTokenStateRevoked, version: 2},
		{state: CollectorTokenStateActive, version: 1},
	} {
		stored, err := store.GetCollectorToken(ctx, issued[index].Token.ID)
		if err != nil {
			t.Fatalf("GetCollectorToken(%d): %v", index, err)
		}
		if stored.State != want.state || stored.Version != want.version {
			t.Fatalf("stored token %d = %+v, want state/version %q/%d", index, stored, want.state, want.version)
		}
	}
	var eventCount int
	if err := database.SQLDB().QueryRowContext(
		ctx,
		`SELECT count(*) FROM audit_events`,
	).Scan(&eventCount); err != nil {
		t.Fatalf("count audit events: %v", err)
	}
	if eventCount != 5 {
		t.Fatalf("audit event count = %d, want 5", eventCount)
	}
}

func TestCollectorTokenAuditRequiresExplicitAdministratorAndScopesTenant(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	database := openControlDB(t)
	if _, err := database.CreateIndex(ctx, activeIndex("main")); err != nil {
		t.Fatalf("CreateIndex(main): %v", err)
	}
	auditStore, err := audit.NewStore(database, audit.StoreOptions{
		CursorKey: []byte("collector-token-audit-list-key-32b"),
	})
	if err != nil {
		t.Fatalf("audit.NewStore: %v", err)
	}
	store, err := NewStoreWithOptions(
		database,
		[]byte("collector-token-explicit-actor-key"),
		StoreOptions{
			AuditAppender:             auditStore,
			AuditTenantID:             "tenant-west",
			RequireExplicitAuditActor: true,
		},
	)
	if err != nil {
		t.Fatalf("NewStoreWithOptions: %v", err)
	}
	request := CreateCollectorTokenRequest{
		Name:              "administrator owned",
		AllowedIndexNames: []string{"main"},
		BoundCollectorID:  testCollectorID,
	}

	if _, err := store.CreateCollectorToken(ctx, request); !errors.Is(
		err,
		ErrAuditActorUnavailable,
	) {
		t.Fatalf("CreateCollectorToken(missing actor) error = %v", err)
	}
	userContext, err := audit.WithActor(ctx, audit.Actor{
		Kind: audit.ActorKindBrowser,
		ID:   "browser-user",
		Role: audit.ActorRoleUser,
	})
	if err != nil {
		t.Fatalf("audit.WithActor(user): %v", err)
	}
	if _, err := store.CreateCollectorToken(userContext, request); !errors.Is(
		err,
		control.ErrInvalidArgument,
	) {
		t.Fatalf("CreateCollectorToken(user actor) error = %v", err)
	}

	administrator := audit.Actor{
		Kind: audit.ActorKindBrowser,
		ID:   "browser-administrator",
		Role: audit.ActorRoleAdministrator,
	}
	administratorContext, err := audit.WithActor(ctx, administrator)
	if err != nil {
		t.Fatalf("audit.WithActor(administrator): %v", err)
	}
	issued, err := store.CreateCollectorToken(administratorContext, request)
	if err != nil {
		t.Fatalf("CreateCollectorToken(administrator): %v", err)
	}

	page, err := auditStore.List(ctx, "tenant-west", audit.ListRequest{
		IncludeTotal: true,
	})
	if err != nil {
		t.Fatalf("audit List(tenant-west): %v", err)
	}
	if len(page.Events) != 1 ||
		page.Events[0].Actor != administrator ||
		page.Events[0].TargetID != issued.Token.ID {
		t.Fatalf("tenant audit events = %+v", page.Events)
	}
	defaultPage, err := auditStore.List(ctx, "default", audit.ListRequest{})
	if err != nil {
		t.Fatalf("audit List(default): %v", err)
	}
	if len(defaultPage.Events) != 0 {
		t.Fatalf("default tenant received scoped events: %+v", defaultPage.Events)
	}

	var tokenCount int
	if err := database.SQLDB().QueryRowContext(
		ctx,
		`SELECT count(*) FROM ingestion_tokens`,
	).Scan(&tokenCount); err != nil {
		t.Fatalf("count ingestion tokens: %v", err)
	}
	if tokenCount != 1 {
		t.Fatalf("ingestion token count = %d, want only administrator mutation", tokenCount)
	}
}

func TestCollectorTokenAuditConfigurationRejectsInvalidDependencies(t *testing.T) {
	t.Parallel()

	database := openControlDB(t)
	key := []byte("collector-token-audit-config-key")
	if store, err := NewStoreWithOptions(database, key, StoreOptions{
		AuditTenantID: " tenant",
	}); store != nil || !errors.Is(err, control.ErrInvalidArgument) {
		t.Fatalf("NewStoreWithOptions(invalid tenant) = (%v, %v)", store, err)
	}
	var nilAppender *audit.Store
	if store, err := NewStoreWithOptions(database, key, StoreOptions{
		AuditAppender: nilAppender,
	}); store != nil || !errors.Is(err, control.ErrInvalidArgument) {
		t.Fatalf("NewStoreWithOptions(typed nil appender) = (%v, %v)", store, err)
	}
}
