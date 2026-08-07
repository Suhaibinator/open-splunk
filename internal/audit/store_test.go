package audit

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/control"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

var auditTestTime = time.Date(2026, 8, 3, 12, 34, 56, 789_123_456, time.FixedZone("fixture", -7*60*60))

func auditTestCursorKey() []byte { return bytes.Repeat([]byte{0x5a}, minimumCursorKeyBytes) }

func openAuditTestDatabase(t *testing.T) (string, *control.DB) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "control.db")
	database, err := control.Open(context.Background(), path)
	if err != nil {
		t.Fatalf("control.Open: %v", err)
	}
	t.Cleanup(func() {
		if closeErr := database.Close(); closeErr != nil {
			t.Errorf("close control database: %v", closeErr)
		}
	})
	return path, database
}

func newAuditTestStore(t *testing.T, database *control.DB, key []byte) *Store {
	t.Helper()
	store, err := NewStore(database, StoreOptions{
		CursorKey: key,
	})
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	return store
}

func auditTestDefinition(action Action, targetID string, version uint64) SuccessfulEvent {
	return SuccessfulEvent{
		OccurredAt:    auditTestTime,
		Action:        action,
		TargetKind:    TargetKindIngestionToken,
		TargetID:      targetID,
		TargetVersion: version,
	}
}

func populateCanonicalAuditJournal(
	t *testing.T,
	database *control.DB,
	tenantID string,
	eventCount int,
) {
	t.Helper()
	err := database.GORMDB().WithContext(context.Background()).Transaction(func(tx *gorm.DB) error {
		if err := tx.Create(&auditTenantStateRecord{
			TenantID:     tenantID,
			NextSequence: 1,
			EventCount:   0,
		}).Error; err != nil {
			return err
		}
		if eventCount == 0 {
			return nil
		}
		return tx.Exec(`
			WITH RECURSIVE audit_sequence(sequence) AS (
				SELECT 1
				UNION ALL
				SELECT sequence + 1
				FROM audit_sequence
				WHERE sequence < ?
			)
			INSERT INTO audit_events (
				tenant_id,
				sequence,
				occurred_at_unix_micro,
				actor_kind,
				actor_id,
				actor_role,
				action,
				target_kind,
				target_id,
				target_version
			)
			SELECT
				?,
				sequence,
				?,
				?,
				?,
				?,
				?,
				?,
				printf('token-%06d', sequence),
				1
			FROM audit_sequence
			ORDER BY sequence
		`,
			eventCount,
			tenantID,
			auditTestTime.UnixMicro(),
			ActorKindSystem,
			defaultSystemActorID,
			ActorRoleSystem,
			ActionIngestionTokenCreate,
			TargetKindIngestionToken,
		).Error
	})
	if err != nil {
		t.Fatalf("populate canonical audit journal: %v", err)
	}
}

type auditStatementCounter struct {
	mutex sync.Mutex
	count int
}

func (counter *auditStatementCounter) add() {
	counter.mutex.Lock()
	defer counter.mutex.Unlock()
	counter.count++
}

func (counter *auditStatementCounter) reset() {
	counter.mutex.Lock()
	defer counter.mutex.Unlock()
	counter.count = 0
}

func (counter *auditStatementCounter) value() int {
	counter.mutex.Lock()
	defer counter.mutex.Unlock()
	return counter.count
}

type countingAuditLogger struct {
	counter *auditStatementCounter
}

func (logging countingAuditLogger) LogMode(logger.LogLevel) logger.Interface { return logging }

func (countingAuditLogger) Info(context.Context, string, ...any)  {}
func (countingAuditLogger) Warn(context.Context, string, ...any)  {}
func (countingAuditLogger) Error(context.Context, string, ...any) {}

func (logging countingAuditLogger) Trace(
	context.Context,
	time.Time,
	func() (string, int64),
	error,
) {
	logging.counter.add()
}

func TestStoreAppendUsesSystemDefaultAndBrowserOverride(t *testing.T) {
	t.Parallel()
	_, database := openAuditTestDatabase(t)
	store := newAuditTestStore(t, database, auditTestCursorKey())
	ctx := context.Background()

	created, err := store.Append(
		ctx,
		"tenant-a",
		auditTestDefinition(ActionIngestionTokenCreate, "token-a", 1),
	)
	if err != nil {
		t.Fatalf("Append(system): %v", err)
	}
	wantTime := time.UnixMicro(auditTestTime.UnixMicro()).UTC()
	if created.Sequence != 1 || created.TenantID != "tenant-a" ||
		!created.OccurredAt.Equal(wantTime) ||
		created.Actor != (Actor{Kind: ActorKindSystem, ID: defaultSystemActorID, Role: ActorRoleSystem}) ||
		created.Action != ActionIngestionTokenCreate ||
		created.TargetKind != TargetKindIngestionToken ||
		created.TargetID != "token-a" || created.TargetVersion != 1 {
		t.Fatalf("system event = %+v", created)
	}

	browser := Actor{
		Kind: ActorKindBrowser,
		ID:   "admin-a",
		Role: ActorRoleAdministrator,
	}
	browserContext, err := WithActor(ctx, browser)
	if err != nil {
		t.Fatalf("WithActor: %v", err)
	}
	updated, err := store.Append(
		browserContext,
		"tenant-a",
		auditTestDefinition(ActionIngestionTokenUpdate, "token-a", 2),
	)
	if err != nil {
		t.Fatalf("Append(browser): %v", err)
	}
	if updated.Sequence != 2 || updated.Actor != browser ||
		updated.Action != ActionIngestionTokenUpdate || updated.TargetVersion != 2 {
		t.Fatalf("browser event = %+v", updated)
	}

	page, err := store.List(ctx, "tenant-a", ListRequest{IncludeTotal: true})
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(page.Events) != 2 || page.Events[0] != updated || page.Events[1] != created ||
		page.TotalSize == nil || *page.TotalSize != 2 || !page.TotalSizeExact {
		t.Fatalf("page = %+v", page)
	}
}

func TestStoreAppendOnlyConstructionAndConfigurationValidation(t *testing.T) {
	t.Parallel()
	_, database := openAuditTestDatabase(t)

	if store, err := NewStore(nil, StoreOptions{}); store != nil ||
		!errors.Is(err, control.ErrInvalidArgument) {
		t.Fatalf("NewStore(nil) = (%v, %v)", store, err)
	}
	//nolint:staticcheck // Explicitly verifies the exported nil-context guard.
	if store, err := NewStoreWithContext(nil, database, StoreOptions{}); store != nil ||
		!errors.Is(err, control.ErrInvalidArgument) {
		t.Fatalf("NewStoreWithContext(nil) = (%v, %v)", store, err)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if store, err := NewStoreWithContext(canceled, database, StoreOptions{}); store != nil ||
		!errors.Is(err, context.Canceled) {
		t.Fatalf("NewStoreWithContext(canceled) = (%v, %v)", store, err)
	}
	for _, key := range [][]byte{
		{1},
		bytes.Repeat([]byte{1}, maximumCursorKeyBytes+1),
	} {
		if store, err := NewStore(database, StoreOptions{CursorKey: key}); store != nil ||
			!errors.Is(err, control.ErrInvalidArgument) {
			t.Fatalf("NewStore(key length %d) = (%v, %v)", len(key), store, err)
		}
	}

	store := newAuditTestStore(t, database, nil)
	if _, err := store.Append(
		context.Background(),
		"append-only",
		auditTestDefinition(ActionIngestionTokenCreate, "token", 1),
	); err != nil {
		t.Fatalf("append-only Append: %v", err)
	}
	if page, err := store.List(context.Background(), "append-only", ListRequest{}); len(page.Events) != 0 ||
		!errors.Is(err, control.ErrInvalidArgument) {
		t.Fatalf("append-only List = (%+v, %v), want invalid", page, err)
	}

	key := auditTestCursorKey()
	detached, err := NewStore(database, StoreOptions{CursorKey: key})
	if err != nil {
		t.Fatalf("NewStore(detached key): %v", err)
	}
	key[0] ^= 0xff
	if bytes.Equal(detached.cursorKey, key) {
		t.Fatal("store retained caller cursor-key storage")
	}
}

func TestAppendInTransactionRollsBackWithCaller(t *testing.T) {
	t.Parallel()
	_, database := openAuditTestDatabase(t)
	store := newAuditTestStore(t, database, auditTestCursorKey())
	ctx := context.Background()

	tx := database.GORMDB().WithContext(ctx).Begin()
	if tx.Error != nil {
		t.Fatalf("Begin: %v", tx.Error)
	}
	provisional, err := store.AppendInTransaction(
		ctx,
		tx,
		"tenant-rollback",
		auditTestDefinition(ActionIngestionTokenCreate, "rolled-back", 1),
	)
	if err != nil {
		_ = tx.Rollback().Error
		t.Fatalf("AppendInTransaction: %v", err)
	}
	if provisional.Sequence != 1 {
		_ = tx.Rollback().Error
		t.Fatalf("provisional sequence = %d", provisional.Sequence)
	}
	if err := tx.Rollback().Error; err != nil {
		t.Fatalf("Rollback: %v", err)
	}

	page, err := store.List(ctx, "tenant-rollback", ListRequest{IncludeTotal: true})
	if err != nil {
		t.Fatalf("List after rollback: %v", err)
	}
	if len(page.Events) != 0 || page.TotalSize == nil || *page.TotalSize != 0 {
		t.Fatalf("events survived caller rollback: %+v", page)
	}
	var states int64
	if err := database.GORMDB().Model(&auditTenantStateRecord{}).
		Where("tenant_id = ?", "tenant-rollback").
		Count(&states).Error; err != nil || states != 0 {
		t.Fatalf("state after rollback = %d, %v", states, err)
	}
}

func TestAppendInTransactionRejectsAutocommitAndForeignHandles(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	_, database := openAuditTestDatabase(t)
	store := newAuditTestStore(t, database, auditTestCursorKey())
	definition := auditTestDefinition(ActionIngestionTokenCreate, "token", 1)

	if event, err := store.AppendInTransaction(
		ctx,
		database.GORMDB(),
		"tenant-root",
		definition,
	); event != (Event{}) || !errors.Is(err, control.ErrInvalidArgument) {
		t.Fatalf("AppendInTransaction(root) = (%+v, %v)", event, err)
	}

	_, foreign := openAuditTestDatabase(t)
	foreignTx := foreign.GORMDB().WithContext(ctx).Begin()
	if foreignTx.Error != nil {
		t.Fatalf("begin foreign transaction: %v", foreignTx.Error)
	}
	defer func() { _ = foreignTx.Rollback().Error }()
	if event, err := store.AppendInTransaction(
		ctx,
		foreignTx,
		"tenant-foreign",
		definition,
	); event != (Event{}) || !errors.Is(err, control.ErrInvalidArgument) {
		t.Fatalf("AppendInTransaction(foreign) = (%+v, %v)", event, err)
	}

	for _, finish := range []struct {
		name string
		call func(*gorm.DB) error
	}{
		{name: "committed", call: func(tx *gorm.DB) error { return tx.Commit().Error }},
		{name: "rolled back", call: func(tx *gorm.DB) error { return tx.Rollback().Error }},
	} {
		t.Run(finish.name, func(t *testing.T) {
			tx := database.GORMDB().WithContext(ctx).Begin()
			if tx.Error != nil {
				t.Fatalf("Begin: %v", tx.Error)
			}
			if err := finish.call(tx); err != nil {
				t.Fatalf("finish transaction: %v", err)
			}
			if event, err := store.AppendInTransaction(
				ctx,
				tx,
				"tenant-closed",
				definition,
			); event != (Event{}) || err == nil {
				t.Fatalf("AppendInTransaction(%s) = (%+v, %v)", finish.name, event, err)
			}
		})
	}
	for _, candidate := range []*control.DB{database, foreign} {
		var eventCount, stateCount int64
		if err := candidate.GORMDB().Model(&auditEventRecord{}).Count(&eventCount).Error; err != nil {
			t.Fatalf("count rejected events: %v", err)
		}
		if err := candidate.GORMDB().Model(&auditTenantStateRecord{}).Count(&stateCount).Error; err != nil {
			t.Fatalf("count rejected states: %v", err)
		}
		if eventCount != 0 || stateCount != 0 {
			t.Fatalf("rejected append persisted events=%d states=%d", eventCount, stateCount)
		}
	}
}

func TestAppendCapacityFailsClosedAtCanonicalLimit(t *testing.T) {
	_, database := openAuditTestDatabase(t)
	populateCanonicalAuditJournal(
		t,
		database,
		"tenant-full",
		MaximumEventsPerTenant,
	)
	store := newAuditTestStore(t, database, auditTestCursorKey())
	ctx := context.Background()
	if event, err := store.Append(
		ctx,
		"tenant-full",
		auditTestDefinition(ActionIngestionTokenCreate, "not-written", 1),
	); event != (Event{}) || !errors.Is(err, control.ErrCapacityExceeded) {
		t.Fatalf("Append(capacity) = (%+v, %v)", event, err)
	}
	var count int64
	if err := database.GORMDB().Model(&auditEventRecord{}).
		Where("tenant_id = ?", "tenant-full").
		Count(&count).Error; err != nil || count != MaximumEventsPerTenant {
		t.Fatalf("event count after capacity failure = %d, %v", count, err)
	}
}

func TestAppendAndFirstListPageStatementCountsDoNotGrowWithJournal(t *testing.T) {
	_, database := openAuditTestDatabase(t)
	populateCanonicalAuditJournal(t, database, "tenant-small", 1)
	populateCanonicalAuditJournal(t, database, "tenant-large", 2_048)
	store := newAuditTestStore(t, database, auditTestCursorKey())
	counter := &auditStatementCounter{}
	store.orm = database.GORMDB().Session(&gorm.Session{
		Logger: countingAuditLogger{counter: counter},
	})
	ctx := context.Background()

	appendStatements := func(tenantID, targetID string) int {
		t.Helper()
		counter.reset()
		if _, err := store.Append(
			ctx,
			tenantID,
			auditTestDefinition(ActionIngestionTokenCreate, targetID, 1),
		); err != nil {
			t.Fatalf("Append(%s): %v", tenantID, err)
		}
		return counter.value()
	}
	listStatements := func(tenantID string) int {
		t.Helper()
		counter.reset()
		page, err := store.List(ctx, tenantID, ListRequest{PageSize: 1})
		if err != nil || len(page.Events) != 1 || page.NextPageToken == "" {
			t.Fatalf("List(%s) = (%+v, %v)", tenantID, page, err)
		}
		return counter.value()
	}

	smallAppend := appendStatements("tenant-small", "small-final")
	largeAppend := appendStatements("tenant-large", "large-final")
	if smallAppend != largeAppend || smallAppend < 1 || smallAppend > 8 {
		t.Fatalf(
			"append statement counts small=%d large=%d, want equal bounded counts",
			smallAppend,
			largeAppend,
		)
	}
	smallList := listStatements("tenant-small")
	largeList := listStatements("tenant-large")
	if smallList != largeList || smallList < 1 || smallList > 8 {
		t.Fatalf(
			"first-list statement counts small=%d large=%d, want equal bounded counts",
			smallList,
			largeList,
		)
	}
}

func TestConcurrentAppendAllocatesDenseUniqueTenantSequences(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	_, database := openAuditTestDatabase(t)
	store := newAuditTestStore(t, database, auditTestCursorKey())
	const appendCount = 16

	start := make(chan struct{})
	results := make(chan Event, appendCount)
	errorsChannel := make(chan error, appendCount)
	var workers sync.WaitGroup
	workers.Add(appendCount)
	for index := range appendCount {
		go func() {
			defer workers.Done()
			<-start
			event, err := store.Append(
				ctx,
				"tenant-concurrent",
				auditTestDefinition(
					ActionIngestionTokenCreate,
					fmt.Sprintf("token-%02d", index),
					1,
				),
			)
			if err != nil {
				errorsChannel <- err
				return
			}
			results <- event
		}()
	}
	close(start)
	workers.Wait()
	close(results)
	close(errorsChannel)
	for err := range errorsChannel {
		t.Errorf("concurrent Append: %v", err)
	}
	seen := make([]bool, appendCount+1)
	for event := range results {
		if event.Sequence < 1 || event.Sequence > appendCount || seen[event.Sequence] {
			t.Fatalf("invalid concurrent sequence %d", event.Sequence)
		}
		seen[event.Sequence] = true
	}
	for sequence := 1; sequence <= appendCount; sequence++ {
		if !seen[sequence] {
			t.Fatalf("missing concurrent sequence %d", sequence)
		}
	}

	page, err := store.List(ctx, "tenant-concurrent", ListRequest{PageSize: appendCount})
	if err != nil {
		t.Fatalf("List concurrent events: %v", err)
	}
	if len(page.Events) != appendCount {
		t.Fatalf("concurrent event count = %d, want %d", len(page.Events), appendCount)
	}
	for index, event := range page.Events {
		if event.Sequence != uint64(appendCount-index) {
			t.Fatalf("event %d sequence = %d", index, event.Sequence)
		}
	}
}

func TestAuditBackupExcludesUncommittedEventAndResumesDenseSequence(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	directory := t.TempDir()
	database, err := control.Open(ctx, filepath.Join(directory, "control.db"))
	if err != nil {
		t.Fatalf("control.Open: %v", err)
	}
	defer func() { _ = database.Close() }()
	store := newAuditTestStore(t, database, auditTestCursorKey())
	committed := appendAuditTestEvent(
		t,
		store,
		ctx,
		"tenant-backup",
		ActionIngestionTokenCreate,
		"token",
		1,
	)

	tx := database.GORMDB().WithContext(ctx).Begin()
	if tx.Error != nil {
		t.Fatalf("Begin: %v", tx.Error)
	}
	provisional, err := store.AppendInTransaction(
		ctx,
		tx,
		"tenant-backup",
		auditTestDefinition(ActionIngestionTokenUpdate, "token", 2),
	)
	if err != nil {
		_ = tx.Rollback().Error
		t.Fatalf("AppendInTransaction: %v", err)
	}
	if provisional.Sequence != 2 {
		_ = tx.Rollback().Error
		t.Fatalf("provisional sequence = %d", provisional.Sequence)
	}
	backupPath := filepath.Join(directory, "backup.db")
	if err := database.BackupTo(ctx, backupPath); err != nil {
		_ = tx.Rollback().Error
		t.Fatalf("BackupTo: %v", err)
	}
	if err := tx.Rollback().Error; err != nil {
		t.Fatalf("Rollback provisional event: %v", err)
	}

	restored, err := control.Open(ctx, backupPath)
	if err != nil {
		t.Fatalf("control.Open(backup): %v", err)
	}
	defer func() { _ = restored.Close() }()
	restoredStore := newAuditTestStore(t, restored, auditTestCursorKey())
	page, err := restoredStore.List(ctx, "tenant-backup", ListRequest{})
	if err != nil || len(page.Events) != 1 || page.Events[0] != committed {
		t.Fatalf("restored audit page = (%+v, %v)", page, err)
	}
	next := appendAuditTestEvent(
		t,
		restoredStore,
		ctx,
		"tenant-backup",
		ActionIngestionTokenUpdate,
		"token",
		2,
	)
	if next.Sequence != 2 {
		t.Fatalf("post-restore sequence = %d, want 2", next.Sequence)
	}
}

func TestAppendValidationFailsBeforeWriting(t *testing.T) {
	t.Parallel()
	_, database := openAuditTestDatabase(t)
	store := newAuditTestStore(t, database, auditTestCursorKey())
	ctx := context.Background()
	invalidUTF8 := string([]byte{0xff})

	tests := []struct {
		name       string
		tenantID   string
		definition SuccessfulEvent
	}{
		{name: "empty tenant", definition: auditTestDefinition(ActionIngestionTokenCreate, "token", 1)},
		{name: "padded tenant", tenantID: " tenant", definition: auditTestDefinition(ActionIngestionTokenCreate, "token", 1)},
		{name: "invalid tenant UTF-8", tenantID: invalidUTF8, definition: auditTestDefinition(ActionIngestionTokenCreate, "token", 1)},
		{name: "unknown action", tenantID: "tenant", definition: auditTestDefinition(Action("unknown"), "token", 1)},
		{name: "unknown target", tenantID: "tenant", definition: SuccessfulEvent{Action: ActionIngestionTokenCreate, TargetKind: TargetKind("other"), TargetID: "token", TargetVersion: 1}},
		{name: "empty target", tenantID: "tenant", definition: auditTestDefinition(ActionIngestionTokenCreate, "", 1)},
		{name: "control target", tenantID: "tenant", definition: auditTestDefinition(ActionIngestionTokenCreate, "token\n", 1)},
		{name: "long target", tenantID: "tenant", definition: auditTestDefinition(ActionIngestionTokenCreate, strings.Repeat("x", maximumTargetIDBytes+1), 1)},
		{name: "zero version", tenantID: "tenant", definition: auditTestDefinition(ActionIngestionTokenCreate, "token", 0)},
		{name: "create version two", tenantID: "tenant", definition: auditTestDefinition(ActionIngestionTokenCreate, "token", 2)},
		{name: "update version one", tenantID: "tenant", definition: auditTestDefinition(ActionIngestionTokenUpdate, "token", 1)},
		{name: "revoke version one", tenantID: "tenant", definition: auditTestDefinition(ActionIngestionTokenRevoke, "token", 1)},
		{name: "large version", tenantID: "tenant", definition: auditTestDefinition(ActionIngestionTokenCreate, "token", uint64(math.MaxInt64)+1)},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if event, err := store.Append(ctx, test.tenantID, test.definition); event != (Event{}) ||
				!errors.Is(err, control.ErrInvalidArgument) {
				t.Fatalf("Append = (%+v, %v), want invalid", event, err)
			}
		})
	}
	//nolint:staticcheck // Explicitly verifies the exported nil-context guard.
	if event, err := store.Append(nil, "tenant", auditTestDefinition(ActionIngestionTokenCreate, "token", 1)); event != (Event{}) || !errors.Is(err, control.ErrInvalidArgument) {
		t.Fatalf("Append(nil context) = (%+v, %v)", event, err)
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if event, err := store.Append(canceled, "tenant", auditTestDefinition(ActionIngestionTokenCreate, "token", 1)); event != (Event{}) || !errors.Is(err, context.Canceled) {
		t.Fatalf("Append(canceled) = (%+v, %v)", event, err)
	}
	if event, err := store.AppendInTransaction(ctx, nil, "tenant", auditTestDefinition(ActionIngestionTokenCreate, "token", 1)); event != (Event{}) || !errors.Is(err, control.ErrInvalidArgument) {
		t.Fatalf("AppendInTransaction(nil) = (%+v, %v)", event, err)
	}
	userContext, err := WithActor(ctx, Actor{
		Kind: ActorKindBrowser, ID: "ordinary-user", Role: ActorRoleUser,
	})
	if err != nil {
		t.Fatalf("WithActor(browser user): %v", err)
	}
	if event, err := store.Append(
		userContext,
		"tenant",
		auditTestDefinition(ActionIngestionTokenCreate, "token", 1),
	); event != (Event{}) || !errors.Is(err, control.ErrInvalidArgument) {
		t.Fatalf("Append(browser user) = (%+v, %v)", event, err)
	}

	if event, err := store.Append(ctx, "clock-tenant", SuccessfulEvent{
		Action:        ActionIngestionTokenCreate,
		TargetKind:    TargetKindIngestionToken,
		TargetID:      "token",
		TargetVersion: 1,
	}); event != (Event{}) || !errors.Is(err, control.ErrInvalidArgument) {
		t.Fatalf("Append(missing event time) = (%+v, %v)", event, err)
	}
}

func TestAuditMigrationRejectsForgedAccountingAndTaxonomy(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	_, database := openAuditTestDatabase(t)
	store := newAuditTestStore(t, database, auditTestCursorKey())
	event := appendAuditTestEvent(
		t,
		store,
		ctx,
		"tenant-schema",
		ActionIngestionTokenCreate,
		"token",
		1,
	)

	accountingAttacks := []struct {
		name string
		sql  string
		args []any
	}{
		{
			name: "forged nonempty initial state",
			sql: `INSERT INTO audit_tenant_state
				(tenant_id, next_sequence, event_count) VALUES (?, 2, 1)`,
			args: []any{"forged-tenant"},
		},
		{
			name: "duplicate state replacement",
			sql: `INSERT OR REPLACE INTO audit_tenant_state
				(tenant_id, next_sequence, event_count) VALUES (?, 2, 1)`,
			args: []any{event.TenantID},
		},
		{
			name: "state advance without terminal event",
			sql: `UPDATE audit_tenant_state
				SET next_sequence = 3, event_count = 2 WHERE tenant_id = ?`,
			args: []any{event.TenantID},
		},
		{
			name: "state moves backwards",
			sql: `UPDATE audit_tenant_state
				SET next_sequence = 1, event_count = 0 WHERE tenant_id = ?`,
			args: []any{event.TenantID},
		},
	}
	for _, attack := range accountingAttacks {
		t.Run(attack.name, func(t *testing.T) {
			if _, err := database.SQLDB().ExecContext(ctx, attack.sql, attack.args...); err == nil {
				t.Fatal("forged audit accounting succeeded")
			}
		})
	}

	insertEvent := `INSERT INTO audit_events (
		tenant_id, sequence, occurred_at_unix_micro,
		actor_kind, actor_id, actor_role, action,
		target_kind, target_id, target_version
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
	eventAttacks := []struct {
		name   string
		values []any
	}{
		{
			name: "out of sequence",
			values: []any{event.TenantID, 3, auditTestTime.UnixMicro(), "system",
				defaultSystemActorID, "system", ActionIngestionTokenUpdate,
				TargetKindIngestionToken, "token", 2},
		},
		{
			name: "browser user actor",
			values: []any{event.TenantID, 2, auditTestTime.UnixMicro(), "browser",
				"user", "user", ActionIngestionTokenUpdate,
				TargetKindIngestionToken, "token", 2},
		},
		{
			name: "unknown action",
			values: []any{event.TenantID, 2, auditTestTime.UnixMicro(), "system",
				defaultSystemActorID, "system", "ingestion_token.rotate",
				TargetKindIngestionToken, "token", 2},
		},
		{
			name: "create version two",
			values: []any{event.TenantID, 2, auditTestTime.UnixMicro(), "system",
				defaultSystemActorID, "system", ActionIngestionTokenCreate,
				TargetKindIngestionToken, "token", 2},
		},
		{
			name: "update version one",
			values: []any{event.TenantID, 2, auditTestTime.UnixMicro(), "system",
				defaultSystemActorID, "system", ActionIngestionTokenUpdate,
				TargetKindIngestionToken, "token", 1},
		},
		{
			name: "above maximum sequence",
			values: []any{event.TenantID, MaximumEventsPerTenant + 1,
				auditTestTime.UnixMicro(), "system", defaultSystemActorID, "system",
				ActionIngestionTokenUpdate, TargetKindIngestionToken, "token", 2},
		},
	}
	for _, attack := range eventAttacks {
		t.Run(attack.name, func(t *testing.T) {
			if _, err := database.SQLDB().ExecContext(ctx, insertEvent, attack.values...); err == nil {
				t.Fatal("forged audit event succeeded")
			}
		})
	}

	var nextSequence, eventCount int64
	if err := database.SQLDB().QueryRowContext(ctx, `
		SELECT next_sequence, event_count
		FROM audit_tenant_state
		WHERE tenant_id = ?`, event.TenantID).Scan(&nextSequence, &eventCount); err != nil {
		t.Fatalf("read accounting after attacks: %v", err)
	}
	if nextSequence != 2 || eventCount != 1 {
		t.Fatalf("accounting after attacks = next %d count %d", nextSequence, eventCount)
	}
}

func TestNewStoreFailsClosedOnPreexistingInteriorJournalGap(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	_, database := openAuditTestDatabase(t)
	writer := newAuditTestStore(t, database, auditTestCursorKey())
	appendAuditTestEvent(t, writer, ctx, "tenant-gap", ActionIngestionTokenCreate, "token", 1)
	appendAuditTestEvent(t, writer, ctx, "tenant-gap", ActionIngestionTokenUpdate, "token", 2)
	appendAuditTestEvent(t, writer, ctx, "tenant-gap", ActionIngestionTokenRevoke, "token", 3)

	if err := database.GORMDB().Exec(
		"DROP TRIGGER audit_event_delete_is_forbidden",
	).Error; err != nil {
		t.Fatalf("drop event delete trigger: %v", err)
	}
	if err := database.GORMDB().Exec(`
		DELETE FROM audit_events
		WHERE tenant_id = ? AND sequence = 2`, "tenant-gap").Error; err != nil {
		t.Fatalf("create interior gap: %v", err)
	}
	if store, err := NewStore(database, StoreOptions{
		CursorKey: auditTestCursorKey(),
	}); store != nil || !errors.Is(err, ErrCorrupt) {
		t.Fatalf("NewStore(interior gap) = (%v, %v)", store, err)
	}
	var count int64
	if err := database.GORMDB().Model(&auditEventRecord{}).
		Where("tenant_id = ?", "tenant-gap").
		Count(&count).Error; err != nil || count != 2 {
		t.Fatalf("event count after rejected startup = %d, %v", count, err)
	}
}

func TestNewStoreFailsClosedOnPreexistingMalformedInteriorJournalRow(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	_, database := openAuditTestDatabase(t)
	writer := newAuditTestStore(t, database, auditTestCursorKey())
	appendAuditTestEvent(t, writer, ctx, "tenant-malformed", ActionIngestionTokenCreate, "token", 1)
	appendAuditTestEvent(t, writer, ctx, "tenant-malformed", ActionIngestionTokenUpdate, "token", 2)
	appendAuditTestEvent(t, writer, ctx, "tenant-malformed", ActionIngestionTokenRevoke, "token", 3)

	database.SQLDB().SetMaxOpenConns(1)
	connection, err := database.SQLDB().Conn(ctx)
	if err != nil {
		t.Fatalf("acquire corruption connection: %v", err)
	}
	if _, err := connection.ExecContext(ctx, "DROP TRIGGER audit_event_update_is_forbidden"); err != nil {
		_ = connection.Close()
		t.Fatalf("drop event update trigger: %v", err)
	}
	if _, err := connection.ExecContext(ctx, "PRAGMA ignore_check_constraints = ON"); err != nil {
		_ = connection.Close()
		t.Fatalf("ignore fixture constraints: %v", err)
	}
	if _, err := connection.ExecContext(ctx, `
		UPDATE audit_events
		SET actor_role = 'user'
		WHERE tenant_id = ? AND sequence = 2`, "tenant-malformed"); err != nil {
		_ = connection.Close()
		t.Fatalf("corrupt interior event: %v", err)
	}
	if err := connection.Close(); err != nil {
		t.Fatalf("close corruption connection: %v", err)
	}

	if store, err := NewStore(database, StoreOptions{
		CursorKey: auditTestCursorKey(),
	}); store != nil || !errors.Is(err, ErrCorrupt) {
		t.Fatalf("NewStore(malformed interior) = (%v, %v)", store, err)
	}
}

func TestAppendFailsClosedOnRuntimeTailCorruption(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	_, database := openAuditTestDatabase(t)
	store := newAuditTestStore(t, database, auditTestCursorKey())
	appendAuditTestEvent(t, store, ctx, "tenant-tail", ActionIngestionTokenCreate, "token", 1)
	appendAuditTestEvent(t, store, ctx, "tenant-tail", ActionIngestionTokenUpdate, "token", 2)

	database.SQLDB().SetMaxOpenConns(1)
	connection, err := database.SQLDB().Conn(ctx)
	if err != nil {
		t.Fatalf("acquire corruption connection: %v", err)
	}
	if _, err := connection.ExecContext(ctx, "DROP TRIGGER audit_event_update_is_forbidden"); err != nil {
		_ = connection.Close()
		t.Fatalf("drop event update trigger: %v", err)
	}
	if _, err := connection.ExecContext(ctx, "PRAGMA ignore_check_constraints = ON"); err != nil {
		_ = connection.Close()
		t.Fatalf("ignore fixture constraints: %v", err)
	}
	if _, err := connection.ExecContext(ctx, `
		UPDATE audit_events
		SET actor_role = 'user'
		WHERE tenant_id = ? AND sequence = 2`, "tenant-tail"); err != nil {
		_ = connection.Close()
		t.Fatalf("corrupt tail event: %v", err)
	}
	if err := connection.Close(); err != nil {
		t.Fatalf("close corruption connection: %v", err)
	}

	if event, err := store.Append(
		ctx,
		"tenant-tail",
		auditTestDefinition(ActionIngestionTokenUpdate, "token", 3),
	); event != (Event{}) || !errors.Is(err, ErrCorrupt) {
		t.Fatalf("Append(corrupt tail) = (%+v, %v)", event, err)
	}
}

func TestAppendChecksCorruptFullStateBeforeCapacity(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	_, database := openAuditTestDatabase(t)
	store := newAuditTestStore(t, database, auditTestCursorKey())

	if err := database.GORMDB().Exec(
		"DROP TRIGGER audit_tenant_state_initial_shape_is_valid",
	).Error; err != nil {
		t.Fatalf("remove initial-state fixture guard: %v", err)
	}
	if err := database.GORMDB().Create(&auditTenantStateRecord{
		TenantID:     "tenant-forged-full",
		NextSequence: MaximumEventsPerTenant + 1,
		EventCount:   MaximumEventsPerTenant,
	}).Error; err != nil {
		t.Fatalf("install corrupt full state: %v", err)
	}

	event, err := store.Append(
		ctx,
		"tenant-forged-full",
		auditTestDefinition(ActionIngestionTokenCreate, "not-written", 1),
	)
	if event != (Event{}) || !errors.Is(err, ErrCorrupt) ||
		errors.Is(err, control.ErrCapacityExceeded) {
		t.Fatalf("Append(corrupt full state) = (%+v, %v)", event, err)
	}
}

func TestAuditRowsAndStateAreImmutable(t *testing.T) {
	t.Parallel()
	_, database := openAuditTestDatabase(t)
	store := newAuditTestStore(t, database, auditTestCursorKey())
	ctx := context.Background()
	event, err := store.Append(ctx, "tenant-immutable", auditTestDefinition(ActionIngestionTokenCreate, "token", 1))
	if err != nil {
		t.Fatalf("Append: %v", err)
	}

	if err := database.GORMDB().Model(&auditEventRecord{}).
		Where("tenant_id = ? AND sequence = ?", event.TenantID, event.Sequence).
		Update("target_version", 2).Error; err == nil {
		t.Fatal("audit event update succeeded")
	}
	if err := database.GORMDB().
		Where("tenant_id = ? AND sequence = ?", event.TenantID, event.Sequence).
		Delete(&auditEventRecord{}).Error; err == nil {
		t.Fatal("audit event delete succeeded")
	}
	if err := database.GORMDB().
		Where("tenant_id = ?", event.TenantID).
		Delete(&auditTenantStateRecord{}).Error; err == nil {
		t.Fatal("audit tenant state delete succeeded")
	}

	replacement := auditEventRecord{
		Sequence:            int64(event.Sequence),
		TenantID:            event.TenantID,
		OccurredAtUnixMicro: event.OccurredAt.UnixMicro(),
		ActorKind:           ActorKindSystem,
		ActorID:             defaultSystemActorID,
		ActorRole:           ActorRoleSystem,
		Action:              ActionIngestionTokenUpdate,
		TargetKind:          TargetKindIngestionToken,
		TargetID:            event.TargetID,
		TargetVersion:       2,
	}
	if err := database.GORMDB().Exec(`
		INSERT OR REPLACE INTO audit_events (
			sequence, tenant_id, occurred_at_unix_micro,
			actor_kind, actor_id, actor_role, action,
			target_kind, target_id, target_version
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`,
		replacement.Sequence,
		replacement.TenantID,
		replacement.OccurredAtUnixMicro,
		replacement.ActorKind,
		replacement.ActorID,
		replacement.ActorRole,
		replacement.Action,
		replacement.TargetKind,
		replacement.TargetID,
		replacement.TargetVersion,
	).Error; err == nil {
		t.Fatal("audit event INSERT OR REPLACE succeeded")
	}

	page, err := store.List(ctx, event.TenantID, ListRequest{})
	if err != nil || len(page.Events) != 1 || page.Events[0] != event {
		t.Fatalf("immutable event after attacks = (%+v, %v)", page, err)
	}
}

func TestExplicitGORMModelsMatchMigratedAuditColumns(t *testing.T) {
	t.Parallel()
	_, database := openAuditTestDatabase(t)

	stateStatement := &gorm.Statement{DB: database.GORMDB()}
	if err := stateStatement.Parse(&auditTenantStateRecord{}); err != nil {
		t.Fatalf("parse auditTenantStateRecord: %v", err)
	}
	if stateStatement.Schema.Table != "audit_tenant_state" ||
		!slices.Equal(
			stateStatement.Schema.DBNames,
			[]string{"tenant_id", "next_sequence", "event_count"},
		) || len(stateStatement.Schema.PrimaryFields) != 1 ||
		stateStatement.Schema.PrimaryFields[0].DBName != "tenant_id" {
		t.Fatalf("tenant-state GORM schema = %#v", stateStatement.Schema)
	}

	statement := &gorm.Statement{DB: database.GORMDB()}
	if err := statement.Parse(&auditEventRecord{}); err != nil {
		t.Fatalf("parse auditEventRecord: %v", err)
	}
	if statement.Schema.Table != "audit_events" || len(statement.Schema.Fields) != 13 {
		t.Fatalf("event GORM schema = table %q fields %d", statement.Schema.Table, len(statement.Schema.Fields))
	}
	want := []string{
		"tenant_id", "sequence", "occurred_at_unix_micro", "actor_kind",
		"actor_id", "actor_role", "action", "target_kind", "target_id",
		"target_version", "app_id", "object_type", "sharing_scope",
	}
	if !slices.Equal(statement.Schema.DBNames, want) {
		t.Fatalf("GORM columns = %v, want %v", statement.Schema.DBNames, want)
	}
	for index, field := range statement.Schema.Fields {
		wantNotNull := index < 10
		if field.NotNull != wantNotNull {
			t.Fatalf("GORM field %q NotNull = %t, want %t", field.DBName, field.NotNull, wantNotNull)
		}
	}
	primaryKey := make([]string, len(statement.Schema.PrimaryFields))
	for index, field := range statement.Schema.PrimaryFields {
		primaryKey[index] = field.DBName
	}
	if !slices.Equal(primaryKey, []string{"tenant_id", "sequence"}) {
		t.Fatalf("GORM primary key = %v", primaryKey)
	}
	rows, err := database.SQLDB().QueryContext(context.Background(), `
		SELECT name
		FROM pragma_table_info('audit_events')
		WHERE pk > 0
		ORDER BY pk`)
	if err != nil {
		t.Fatalf("read migrated primary key: %v", err)
	}
	var migratedPrimaryKey []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			_ = rows.Close()
			t.Fatalf("scan migrated primary key: %v", err)
		}
		migratedPrimaryKey = append(migratedPrimaryKey, name)
	}
	if err := rows.Close(); err != nil {
		t.Fatalf("close migrated primary key: %v", err)
	}
	if !slices.Equal(migratedPrimaryKey, primaryKey) {
		t.Fatalf("migrated primary key = %v, GORM = %v", migratedPrimaryKey, primaryKey)
	}
	columns, err := database.GORMDB().Migrator().ColumnTypes(&auditEventRecord{})
	if err != nil {
		t.Fatalf("ColumnTypes(audit events): %v", err)
	}
	if len(columns) != len(want) {
		t.Fatalf("migrated audit columns = %d, want %d", len(columns), len(want))
	}
	for index, column := range columns {
		if column.Name() != want[index] {
			t.Fatalf("column %d = %q, want %q", index, column.Name(), want[index])
		}
		wantType := "TEXT"
		if index == 1 || index == 2 || index == 9 {
			wantType = "INTEGER"
		}
		if column.DatabaseTypeName() != wantType {
			t.Fatalf("column %q type = %q, want %q", column.Name(), column.DatabaseTypeName(), wantType)
		}
	}
	columnRows, err := database.SQLDB().QueryContext(context.Background(), `
		SELECT name, type, "notnull"
		FROM pragma_table_info('audit_events')
		ORDER BY cid`)
	if err != nil {
		t.Fatalf("read migrated column definitions: %v", err)
	}
	for index := 0; columnRows.Next(); index++ {
		var name, dataType string
		var notNull int
		if err := columnRows.Scan(&name, &dataType, &notNull); err != nil {
			_ = columnRows.Close()
			t.Fatalf("scan migrated column definition: %v", err)
		}
		wantType := "TEXT"
		if index == 1 || index == 2 || index == 9 {
			wantType = "INTEGER"
		}
		wantNotNull := 1
		if index >= 10 {
			wantNotNull = 0
		}
		if index >= len(want) || name != want[index] || dataType != wantType || notNull != wantNotNull {
			t.Fatalf(
				"migrated column %d = (%q, %q, %d), want (%q, %q, %d)",
				index, name, dataType, notNull, want[index], wantType, wantNotNull,
			)
		}
	}
	if err := columnRows.Close(); err != nil {
		t.Fatalf("close migrated column definitions: %v", err)
	}

	type indexField struct {
		name string
		desc bool
	}
	wantIndexes := map[string][]indexField{
		"audit_events_tenant_action_sequence_idx": {
			{name: "tenant_id"}, {name: "action"}, {name: "sequence", desc: true},
		},
		"audit_events_tenant_actor_sequence_idx": {
			{name: "tenant_id"}, {name: "actor_id"}, {name: "sequence", desc: true},
		},
		"audit_events_tenant_target_sequence_idx": {
			{name: "tenant_id"}, {name: "target_kind"}, {name: "sequence", desc: true},
		},
	}
	modelIndexes := make(map[string][]indexField, len(wantIndexes))
	for _, index := range statement.Schema.ParseIndexes() {
		fields := make([]indexField, len(index.Fields))
		for fieldIndex, field := range index.Fields {
			fields[fieldIndex] = indexField{
				name: field.DBName,
				desc: strings.EqualFold(field.Sort, "desc"),
			}
		}
		modelIndexes[index.Name] = fields
	}
	for name, expected := range wantIndexes {
		if actual := modelIndexes[name]; !slices.Equal(actual, expected) {
			t.Errorf("GORM index %s = %v, want %v", name, actual, expected)
		}
		rows, err := database.SQLDB().QueryContext(
			context.Background(),
			fmt.Sprintf(
				"SELECT name, desc FROM pragma_index_xinfo('%s') WHERE key = 1 ORDER BY seqno",
				name,
			),
		)
		if err != nil {
			t.Fatalf("read migrated index %s: %v", name, err)
		}
		var migrated []indexField
		for rows.Next() {
			var field indexField
			if err := rows.Scan(&field.name, &field.desc); err != nil {
				_ = rows.Close()
				t.Fatalf("scan migrated index %s: %v", name, err)
			}
			migrated = append(migrated, field)
		}
		if err := rows.Close(); err != nil {
			t.Fatalf("close migrated index %s: %v", name, err)
		}
		if !slices.Equal(migrated, expected) {
			t.Errorf("migrated index %s = %v, want %v", name, migrated, expected)
		}
	}
}
