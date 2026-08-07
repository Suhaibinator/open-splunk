package knowledgeattemptaudit

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/audit"
	"github.com/Suhaibinator/open-splunk/internal/control"
)

var testTime = time.Date(2026, time.August, 6, 19, 20, 21, 987654321, time.FixedZone("fixture", -7*60*60))

func openTestDatabase(t *testing.T) (string, *control.DB) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "control.db")
	database, err := control.Open(context.Background(), path)
	if err != nil {
		t.Fatalf("control.Open: %v", err)
	}
	t.Cleanup(func() {
		if database != nil {
			if err := database.Close(); err != nil {
				t.Errorf("close control database: %v", err)
			}
		}
	})
	return path, database
}

func newTestStore(t *testing.T, database *control.DB) *Store {
	t.Helper()
	store, err := New(database)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return store
}

func actorContext(t *testing.T, role audit.ActorRole, id string) context.Context {
	t.Helper()
	ctx, err := audit.WithActor(context.Background(), audit.Actor{
		Kind: audit.ActorKindBrowser,
		ID:   id,
		Role: role,
	})
	if err != nil {
		t.Fatalf("audit.WithActor: %v", err)
	}
	return ctx
}

func adminDefinition(action Action, reason Reason, offset time.Duration) Definition {
	return Definition{OccurredAt: testTime.Add(offset), Action: action, Reason: reason}
}

func readEvents(t *testing.T, database *control.DB, tenantID string) []Event {
	t.Helper()
	var records []eventRecord
	if err := database.GORMDB().Where("tenant_id = ?", tenantID).
		Order("sequence ASC").Find(&records).Error; err != nil {
		t.Fatalf("read event records: %v", err)
	}
	events := make([]Event, len(records))
	for index, record := range records {
		event, err := eventFromRecord(record)
		if err != nil {
			t.Fatalf("eventFromRecord(%+v): %v", record, err)
		}
		events[index] = event
	}
	return events
}

func TestAppendRejectedStoresOnlyTrustedBoundedProjection(t *testing.T) {
	t.Parallel()
	_, database := openTestDatabase(t)
	store := newTestStore(t, database)
	admin := actorContext(t, audit.ActorRoleAdministrator, "administrator-a")
	user := actorContext(t, audit.ActorRoleUser, "user-a")

	if err := store.AppendRejected(user, "tenant-a", Definition{
		OccurredAt: testTime,
		Action:     ActionValidate,
		Reason:     ReasonNotAdministrator,
	}); err != nil {
		t.Fatalf("AppendRejected(user): %v", err)
	}
	definition := adminDefinition(ActionUpdate, ReasonVersionConflict, time.Microsecond)
	definition.AuthorizedContext = &AuthorizedContext{
		AppID: "app_012345678901234567890A",
		Object: &AuthorizedObject{
			KnowledgeObjectID: "ko-authorized",
			ObjectType:        ObjectTypeFieldAlias,
			Version:           7,
			SharingScope:      SharingScopeApp,
		},
	}
	if err := store.AppendRejected(admin, "tenant-a", definition); err != nil {
		t.Fatalf("AppendRejected(admin): %v", err)
	}
	appOnly := adminDefinition(ActionCreate, ReasonInvalidDefinition, 2*time.Microsecond)
	appOnly.AuthorizedContext = &AuthorizedContext{AppID: "app_012345678901234567890A"}
	if err := store.AppendRejected(admin, "tenant-a", appOnly); err != nil {
		t.Fatalf("AppendRejected(app-only): %v", err)
	}

	events := readEvents(t, database, "tenant-a")
	if len(events) != 3 || events[0].Sequence != 1 ||
		events[0].Actor.Role != audit.ActorRoleUser ||
		events[0].AuthorizedContext != nil || events[1].Sequence != 2 ||
		events[1].Action != ActionUpdate || events[1].Result != ResultRejected ||
		events[1].Reason != ReasonVersionConflict ||
		events[1].AuthorizedContext == nil ||
		events[1].AuthorizedContext.Object == nil ||
		events[1].AuthorizedContext.Object.KnowledgeObjectID != "ko-authorized" ||
		events[1].AuthorizedContext.Object.Version != 7 ||
		events[2].AuthorizedContext == nil || events[2].AuthorizedContext.Object != nil ||
		events[2].AuthorizedContext.AppID != "app_012345678901234567890A" {
		t.Fatalf("events = %+v", events)
	}
	wantTime := time.UnixMicro(testTime.Add(time.Microsecond).UnixMicro()).UTC()
	if !events[1].OccurredAt.Equal(wantTime) {
		t.Fatalf("occurred_at = %v, want %v", events[1].OccurredAt, wantTime)
	}
	for _, event := range events {
		if err := event.ValidateForTenant("tenant-a"); err != nil {
			t.Fatalf("ValidateForTenant(%+v): %v", event, err)
		}
	}
}

func TestAppendInputTaxonomyAndPrivacyShapes(t *testing.T) {
	t.Parallel()
	_, database := openTestDatabase(t)
	store := newTestStore(t, database)
	admin := actorContext(t, audit.ActorRoleAdministrator, "administrator")
	user := actorContext(t, audit.ActorRoleUser, "user")
	app := &AuthorizedContext{AppID: "app_012345678901234567890A"}
	object := &AuthorizedContext{
		AppID: "app_012345678901234567890A",
		Object: &AuthorizedObject{
			KnowledgeObjectID: "ko-a", ObjectType: ObjectTypeCalculatedField,
			Version: 2, SharingScope: SharingScopePrivate,
		},
	}
	tests := []struct {
		name       string
		ctx        context.Context
		definition Definition
	}{
		{"missing actor", context.Background(), adminDefinition(ActionValidate, ReasonInvalidDefinition, 0)},
		{"system actor", func() context.Context {
			ctx, _ := audit.WithActor(context.Background(), audit.Actor{Kind: audit.ActorKindSystem, ID: "server", Role: audit.ActorRoleSystem})
			return ctx
		}(), adminDefinition(ActionValidate, ReasonInvalidDefinition, 0)},
		{"user detailed reason", user, adminDefinition(ActionValidate, ReasonInvalidDefinition, 0)},
		{"admin not administrator", admin, adminDefinition(ActionValidate, ReasonNotAdministrator, 0)},
		{"unknown action", admin, adminDefinition(Action("future"), ReasonInvalidDefinition, 0)},
		{"unknown reason", admin, adminDefinition(ActionValidate, Reason("future"), 0)},
		{"not admin app metadata", user, Definition{OccurredAt: testTime, Action: ActionValidate, Reason: ReasonNotAdministrator, AuthorizedContext: app}},
		{"not found object metadata", admin, Definition{OccurredAt: testTime, Action: ActionUpdate, Reason: ReasonNotFoundOrForbidden, AuthorizedContext: object}},
		{"create object metadata", admin, Definition{OccurredAt: testTime, Action: ActionCreate, Reason: ReasonInvalidDefinition, AuthorizedContext: object}},
		{"version without object", admin, adminDefinition(ActionUpdate, ReasonVersionConflict, 0)},
		{"empty app", admin, Definition{OccurredAt: testTime, Action: ActionValidate, Reason: ReasonInvalidDefinition, AuthorizedContext: &AuthorizedContext{}}},
		{"unknown type", admin, Definition{OccurredAt: testTime, Action: ActionUpdate, Reason: ReasonInvalidDefinition, AuthorizedContext: &AuthorizedContext{AppID: object.AppID, Object: &AuthorizedObject{KnowledgeObjectID: "ko", ObjectType: ObjectType("future"), Version: 1, SharingScope: SharingScopeApp}}}},
		{"zero version", admin, Definition{OccurredAt: testTime, Action: ActionUpdate, Reason: ReasonInvalidDefinition, AuthorizedContext: &AuthorizedContext{AppID: object.AppID, Object: &AuthorizedObject{KnowledgeObjectID: "ko", ObjectType: ObjectTypeFieldAlias, SharingScope: SharingScopeApp}}}},
		{"unknown scope", admin, Definition{OccurredAt: testTime, Action: ActionUpdate, Reason: ReasonInvalidDefinition, AuthorizedContext: &AuthorizedContext{AppID: object.AppID, Object: &AuthorizedObject{KnowledgeObjectID: "ko", ObjectType: ObjectTypeFieldAlias, Version: 1, SharingScope: SharingScope("future")}}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if err := store.AppendRejected(test.ctx, "tenant-invalid", test.definition); !errors.Is(err, control.ErrInvalidArgument) {
				t.Fatalf("AppendRejected() error = %v, want ErrInvalidArgument", err)
			}
		})
	}
	var states int64
	if err := database.GORMDB().Model(&tenantStateRecord{}).
		Where("tenant_id = ?", "tenant-invalid").Count(&states).Error; err != nil || states != 0 {
		t.Fatalf("invalid attempts created %d tenant states: %v", states, err)
	}
}

func TestTransactionRollbackCancellationAndForeignHandle(t *testing.T) {
	t.Parallel()
	_, database := openTestDatabase(t)
	store := newTestStore(t, database)
	admin := actorContext(t, audit.ActorRoleAdministrator, "administrator")
	definition := adminDefinition(ActionPreview, ReasonResourceLimit, 0)

	tx := database.GORMDB().WithContext(admin).Begin()
	if tx.Error != nil {
		t.Fatal(tx.Error)
	}
	if err := store.AppendRejectedInTransaction(admin, tx, "tenant-rollback", definition); err != nil {
		t.Fatalf("AppendRejectedInTransaction: %v", err)
	}
	if err := tx.Rollback().Error; err != nil {
		t.Fatalf("rollback: %v", err)
	}
	if got := readEvents(t, database, "tenant-rollback"); len(got) != 0 {
		t.Fatalf("rolled-back events = %+v", got)
	}
	var states int64
	if err := database.GORMDB().Model(&tenantStateRecord{}).
		Where("tenant_id = ?", "tenant-rollback").Count(&states).Error; err != nil || states != 0 {
		t.Fatalf("rolled-back states = %d, %v", states, err)
	}

	canceled, cancel := context.WithCancel(admin)
	cancel()
	if err := store.AppendRejected(canceled, "tenant-canceled", definition); !errors.Is(err, context.Canceled) {
		t.Fatalf("AppendRejected(canceled) = %v", err)
	}
	if err := store.AppendRejectedInTransaction(admin, nil, "tenant", definition); !errors.Is(err, control.ErrInvalidArgument) {
		t.Fatalf("AppendRejectedInTransaction(nil) = %v", err)
	}

	_, foreign := openTestDatabase(t)
	foreignTx := foreign.GORMDB().WithContext(admin).Begin()
	if foreignTx.Error != nil {
		t.Fatal(foreignTx.Error)
	}
	defer foreignTx.Rollback()
	if err := store.AppendRejectedInTransaction(admin, foreignTx, "tenant", definition); !errors.Is(err, control.ErrInvalidArgument) {
		t.Fatalf("AppendRejectedInTransaction(foreign) = %v", err)
	}
}

func TestConcurrentAppendsAllocateDenseUniqueSequence(t *testing.T) {
	_, database := openTestDatabase(t)
	store := newTestStore(t, database)
	const attempts = 48
	errorsByAttempt := make(chan error, attempts)
	var group sync.WaitGroup
	for index := range attempts {
		group.Add(1)
		go func(index int) {
			defer group.Done()
			ctx := actorContext(t, audit.ActorRoleAdministrator, fmt.Sprintf("admin-%02d", index))
			errorsByAttempt <- store.AppendRejected(
				ctx,
				"tenant-concurrent",
				adminDefinition(ActionValidate, ReasonInvalidDefinition, time.Duration(index)*time.Microsecond),
			)
		}(index)
	}
	group.Wait()
	close(errorsByAttempt)
	for err := range errorsByAttempt {
		if err != nil {
			t.Fatalf("concurrent AppendRejected: %v", err)
		}
	}
	events := readEvents(t, database, "tenant-concurrent")
	sequences := make([]uint64, len(events))
	for index, event := range events {
		sequences[index] = event.Sequence
	}
	want := make([]uint64, attempts)
	for index := range want {
		want[index] = uint64(index + 1)
	}
	if !slices.Equal(sequences, want) {
		t.Fatalf("sequences = %v", sequences)
	}
	var state tenantStateRecord
	if err := database.GORMDB().Where("tenant_id = ?", "tenant-concurrent").Take(&state).Error; err != nil {
		t.Fatal(err)
	}
	if state.FirstSequence != 1 || state.NextSequence != attempts+1 ||
		state.RetainedCount != attempts {
		t.Fatalf("state = %+v", state)
	}
}

func TestAppendRejectsMalformedNewestWithoutMutation(t *testing.T) {
	t.Parallel()
	_, database := openTestDatabase(t)
	store := newTestStore(t, database)
	admin := actorContext(t, audit.ActorRoleAdministrator, "administrator")
	for index := 0; index < 2; index++ {
		if err := store.AppendRejected(
			admin,
			"tenant-malformed-newest",
			adminDefinition(
				ActionValidate,
				ReasonInvalidDefinition,
				time.Duration(index)*time.Microsecond,
			),
		); err != nil {
			t.Fatal(err)
		}
	}

	before := readRawTenantState(t, database, "tenant-malformed-newest")
	corruptEventForAppendTest(t, database, `
		UPDATE knowledge_attempt_audit_events
		SET actor_id = ?
		WHERE tenant_id = 'tenant-malformed-newest' AND sequence = 2
	`, fmt.Sprintf("%0256d", 0))

	err := store.AppendRejected(
		admin,
		"tenant-malformed-newest",
		adminDefinition(ActionPreview, ReasonResourceLimit, 2*time.Microsecond),
	)
	if !errors.Is(err, ErrCorrupt) {
		t.Fatalf("AppendRejected(malformed newest) = %v, want ErrCorrupt", err)
	}
	after := readRawTenantState(t, database, "tenant-malformed-newest")
	if after != before {
		t.Fatalf("state changed: before=%+v after=%+v", before, after)
	}
	assertRawEventWindow(t, database, "tenant-malformed-newest", 2, 1, 2)
	assertRawEventSequenceCount(t, database, "tenant-malformed-newest", 3, 0)
}

func TestAppendAfterDatabaseCloseFailsClosed(t *testing.T) {
	path := filepath.Join(t.TempDir(), "control.db")
	database, err := control.Open(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	store := newTestStore(t, database)
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	ctx := actorContext(t, audit.ActorRoleAdministrator, "administrator")
	if err := store.AppendRejected(ctx, "tenant", adminDefinition(ActionValidate, ReasonServiceUnavailable, 0)); err == nil {
		t.Fatal("AppendRejected unexpectedly succeeded against closed database")
	}
}

func TestModelTablesAreMigrationOwned(t *testing.T) {
	if got := (tenantStateRecord{}).TableName(); got != "knowledge_attempt_audit_tenant_state" {
		t.Fatal(got)
	}
	if got := (eventRecord{}).TableName(); got != "knowledge_attempt_audit_events" {
		t.Fatal(got)
	}
}

func corruptEventForAppendTest(
	t *testing.T,
	database *control.DB,
	statement string,
	arguments ...any,
) {
	t.Helper()
	conn, err := database.SQLDB().Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if _, err := conn.ExecContext(
			context.Background(),
			`PRAGMA ignore_check_constraints = OFF`,
		); err != nil {
			t.Errorf("restore check constraints: %v", err)
		}
		if err := conn.Close(); err != nil {
			t.Errorf("close corruption connection: %v", err)
		}
	}()
	if _, err := conn.ExecContext(
		context.Background(),
		`PRAGMA ignore_check_constraints = ON`,
	); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.ExecContext(
		context.Background(),
		`DROP TRIGGER knowledge_attempt_audit_event_update_is_forbidden`,
	); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.ExecContext(context.Background(), statement, arguments...); err != nil {
		t.Fatalf("corrupt retained event: %v", err)
	}
}

func readRawTenantState(
	t *testing.T,
	database *control.DB,
	tenantID string,
) tenantStateRecord {
	t.Helper()
	var state tenantStateRecord
	if err := database.GORMDB().Where("tenant_id = ?", tenantID).Take(&state).Error; err != nil {
		t.Fatalf("read raw tenant state: %v", err)
	}
	return state
}

func assertRawEventWindow(
	t *testing.T,
	database *control.DB,
	tenantID string,
	wantCount int,
	wantFirst int,
	wantLast int,
) {
	t.Helper()
	var got struct {
		Count int `gorm:"column:retained_count"`
		First int `gorm:"column:first_sequence"`
		Last  int `gorm:"column:last_sequence"`
	}
	if err := database.GORMDB().Raw(`
		SELECT COUNT(*) AS retained_count,
		       MIN(sequence) AS first_sequence,
		       MAX(sequence) AS last_sequence
		FROM knowledge_attempt_audit_events
		WHERE tenant_id = ?
	`, tenantID).Scan(&got).Error; err != nil {
		t.Fatalf("read raw event window: %v", err)
	}
	if got.Count != wantCount || got.First != wantFirst || got.Last != wantLast {
		t.Fatalf(
			"raw event window = %+v, want count=%d first=%d last=%d",
			got,
			wantCount,
			wantFirst,
			wantLast,
		)
	}
}

func assertRawEventSequenceCount(
	t *testing.T,
	database *control.DB,
	tenantID string,
	sequence int,
	want int64,
) {
	t.Helper()
	var count int64
	if err := database.GORMDB().Model(&eventRecord{}).
		Where("tenant_id = ? AND sequence = ?", tenantID, sequence).
		Count(&count).Error; err != nil {
		t.Fatalf("count raw event sequence: %v", err)
	}
	if count != want {
		t.Fatalf("sequence %d count = %d, want %d", sequence, count, want)
	}
}
