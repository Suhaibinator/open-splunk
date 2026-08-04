package searchaudit

import (
	"bytes"
	"context"
	"errors"
	"path/filepath"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/audit"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/searchhistory"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

var searchAuditTestTime = time.Date(
	2026,
	time.August,
	4,
	16,
	17,
	18,
	987_654_321,
	time.FixedZone("fixture", -7*60*60),
)

func searchAuditTestCursorKey() []byte {
	return bytes.Repeat([]byte{0x73}, minimumCursorKeyBytes)
}

func openSearchAuditTestDatabase(t *testing.T) (string, *control.DB) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "control.db")
	database, err := control.Open(context.Background(), path)
	if err != nil {
		t.Fatalf("control.Open: %v", err)
	}
	t.Cleanup(func() {
		if database != nil {
			if closeErr := database.Close(); closeErr != nil {
				t.Errorf("close control database: %v", closeErr)
			}
		}
	})
	return path, database
}

func newSearchAuditTestStore(
	t *testing.T,
	database *control.DB,
	key []byte,
	maximum uint32,
) *Store {
	t.Helper()
	store, err := New(database, Options{
		CursorKey:               key,
		MaximumRetainedAttempts: maximum,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return store
}

func searchAuditTestDefinition(ownerID, jobID string, offset time.Duration) searchhistory.SearchAttemptAuditEvent {
	return searchhistory.SearchAttemptAuditEvent{
		OccurredAt:  searchAuditTestTime.Add(offset),
		SearchJobID: jobID,
		OwnerID:     ownerID,
	}
}

func appendSearchAuditTestEvent(
	t *testing.T,
	store *Store,
	database *control.DB,
	ctx context.Context,
	tenantID string,
	definition searchhistory.SearchAttemptAuditEvent,
) {
	t.Helper()
	tx := database.GORMDB().WithContext(ctx).Begin()
	if tx.Error != nil {
		t.Fatalf("begin append transaction: %v", tx.Error)
	}
	if err := store.AppendSearchAttemptInTransaction(ctx, tx, tenantID, definition); err != nil {
		_ = tx.Rollback().Error
		t.Fatalf("AppendSearchAttemptInTransaction: %v", err)
	}
	if err := tx.Commit().Error; err != nil {
		t.Fatalf("commit append transaction: %v", err)
	}
}

type searchAuditStatementCounter struct {
	mutex sync.Mutex
	count int
}

func (counter *searchAuditStatementCounter) add() {
	counter.mutex.Lock()
	defer counter.mutex.Unlock()
	counter.count++
}

func (counter *searchAuditStatementCounter) reset() {
	counter.mutex.Lock()
	defer counter.mutex.Unlock()
	counter.count = 0
}

func (counter *searchAuditStatementCounter) value() int {
	counter.mutex.Lock()
	defer counter.mutex.Unlock()
	return counter.count
}

type countingSearchAuditLogger struct {
	counter *searchAuditStatementCounter
}

func (logging countingSearchAuditLogger) LogMode(logger.LogLevel) logger.Interface {
	return logging
}

func (countingSearchAuditLogger) Info(context.Context, string, ...any)  {}
func (countingSearchAuditLogger) Warn(context.Context, string, ...any)  {}
func (countingSearchAuditLogger) Error(context.Context, string, ...any) {}

func (logging countingSearchAuditLogger) Trace(
	context.Context,
	time.Time,
	func() (string, int64),
	error,
) {
	logging.counter.add()
}

func TestAppendUsesCallerTransactionAndSafeActorProjection(t *testing.T) {
	t.Parallel()
	_, database := openSearchAuditTestDatabase(t)
	store := newSearchAuditTestStore(t, database, searchAuditTestCursorKey(), 3)
	ctx := context.Background()

	tx := database.GORMDB().WithContext(ctx).Begin()
	if tx.Error != nil {
		t.Fatal(tx.Error)
	}
	definition := searchAuditTestDefinition("owner-a", "job-a", 0)
	if err := store.AppendSearchAttemptInTransaction(ctx, tx, "tenant-a", definition); err != nil {
		_ = tx.Rollback().Error
		t.Fatalf("AppendSearchAttemptInTransaction(system): %v", err)
	}
	if err := tx.Rollback().Error; err != nil {
		t.Fatalf("rollback: %v", err)
	}
	page, err := store.List(ctx, "tenant-a", ListRequest{IncludeTotal: true})
	if err != nil {
		t.Fatalf("List(after rollback): %v", err)
	}
	if len(page.Events) != 0 || page.TotalSize == nil || *page.TotalSize != 0 {
		t.Fatalf("rolled-back event survived: %+v", page)
	}

	appendSearchAuditTestEvent(t, store, database, ctx, "tenant-a", definition)
	browser := audit.Actor{
		Kind: audit.ActorKindBrowser,
		ID:   "browser-user-a",
		Role: audit.ActorRoleUser,
	}
	browserContext, err := audit.WithActor(ctx, browser)
	if err != nil {
		t.Fatalf("audit.WithActor: %v", err)
	}
	appendSearchAuditTestEvent(
		t,
		store,
		database,
		browserContext,
		"tenant-a",
		searchAuditTestDefinition("owner-a", "job-b", time.Microsecond),
	)

	page, err = store.List(ctx, "tenant-a", ListRequest{IncludeTotal: true})
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	wantTime := time.UnixMicro(searchAuditTestTime.Add(time.Microsecond).UnixMicro()).UTC()
	if len(page.Events) != 2 || page.TotalSize == nil || *page.TotalSize != 2 ||
		!page.TotalSizeExact || page.Events[0].Sequence != 2 ||
		page.Events[0].TenantID != "tenant-a" || page.Events[0].OwnerID != "owner-a" ||
		page.Events[0].SearchJobID != "job-b" || !page.Events[0].OccurredAt.Equal(wantTime) ||
		page.Events[0].Actor != browser || page.Events[1].Sequence != 1 ||
		page.Events[1].Actor != (audit.Actor{
			Kind: audit.ActorKindSystem,
			ID:   "open-splunk-server",
			Role: audit.ActorRoleSystem,
		}) {
		t.Fatalf("page = %+v", page)
	}
	for _, event := range page.Events {
		if err := event.ValidateForTenant("tenant-a"); err != nil {
			t.Fatalf("ValidateForTenant(%+v): %v", event, err)
		}
	}
}

func TestRollingCapPrunesExactlyTheOldestAndKeepsMonotonicSequences(t *testing.T) {
	t.Parallel()
	_, database := openSearchAuditTestDatabase(t)
	store := newSearchAuditTestStore(t, database, searchAuditTestCursorKey(), 3)
	ctx := context.Background()

	for index := 1; index <= 5; index++ {
		appendSearchAuditTestEvent(
			t,
			store,
			database,
			ctx,
			"tenant-roll",
			searchAuditTestDefinition("owner", "job-"+string(rune('0'+index)), time.Duration(index)*time.Microsecond),
		)
		var state searchAttemptTenantStateRecord
		if err := database.GORMDB().Where("tenant_id = ?", "tenant-roll").Take(&state).Error; err != nil {
			t.Fatalf("read state after append %d: %v", index, err)
		}
		wantFirst := int64(1)
		if index > 3 {
			wantFirst = int64(index - 2)
		}
		wantCount := int64(index)
		if wantCount > 3 {
			wantCount = 3
		}
		if state.FirstSequence != wantFirst || state.NextSequence != int64(index+1) ||
			state.RetainedCount != wantCount || state.MaximumRetainedAttempts != 3 {
			t.Fatalf("state after append %d = %+v", index, state)
		}
	}

	page, err := store.List(ctx, "tenant-roll", ListRequest{IncludeTotal: true})
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if got := searchAuditSequences(page.Events); !slices.Equal(got, []uint64{5, 4, 3}) ||
		page.TotalSize == nil || *page.TotalSize != 3 {
		t.Fatalf("retained page = %+v, sequences=%v", page, got)
	}
	var pruned int64
	if err := database.GORMDB().Raw(`
		SELECT COUNT(*)
		FROM search_attempt_audit_events
		WHERE tenant_id = ? AND sequence < 3
	`, "tenant-roll").Scan(&pruned).Error; err != nil || pruned != 0 {
		t.Fatalf("pruned rows = %d, %v", pruned, err)
	}
}

func TestExistingTenantAppendUsesBoundedStatementBudget(t *testing.T) {
	_, database := openSearchAuditTestDatabase(t)
	store := newSearchAuditTestStore(t, database, searchAuditTestCursorKey(), 5)
	ctx := context.Background()
	appendSearchAuditTestEvent(
		t,
		store,
		database,
		ctx,
		"tenant-hot-path",
		searchAuditTestDefinition("owner", "job-1", 0),
	)

	counter := &searchAuditStatementCounter{}
	tx := database.GORMDB().WithContext(ctx).Session(&gorm.Session{
		Logger: countingSearchAuditLogger{counter: counter},
	}).Begin()
	if tx.Error != nil {
		t.Fatal(tx.Error)
	}
	counter.reset()
	if err := store.AppendSearchAttemptInTransaction(
		ctx,
		tx,
		"tenant-hot-path",
		searchAuditTestDefinition("owner", "job-2", time.Microsecond),
	); err != nil {
		_ = tx.Rollback().Error
		t.Fatalf("AppendSearchAttemptInTransaction: %v", err)
	}
	const maximumExistingTenantAppendStatements = 5
	if got := counter.value(); got < 1 || got > maximumExistingTenantAppendStatements {
		_ = tx.Rollback().Error
		t.Fatalf(
			"existing-tenant append statements = %d, want 1..%d",
			got,
			maximumExistingTenantAppendStatements,
		)
	}
	if err := tx.Commit().Error; err != nil {
		t.Fatal(err)
	}
}

func TestAppendRejectsMissingRetainedBoundaryBeforeMutation(t *testing.T) {
	for _, test := range []struct {
		name     string
		sequence int64
	}{
		{name: "first", sequence: 1},
		{name: "tail", sequence: 3},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			_, database := openSearchAuditTestDatabase(t)
			store := newSearchAuditTestStore(t, database, searchAuditTestCursorKey(), 5)
			for index := 1; index <= 3; index++ {
				appendSearchAuditTestEvent(
					t,
					store,
					database,
					ctx,
					"tenant-corrupt-boundary",
					searchAuditTestDefinition(
						"owner",
						"job-"+string(rune('0'+index)),
						time.Duration(index)*time.Microsecond,
					),
				)
			}
			for _, trigger := range []string{
				"search_attempt_audit_event_delete_requires_rolling_prune",
				"search_attempt_audit_event_prune_advances_state",
			} {
				if err := database.GORMDB().Exec("DROP TRIGGER " + trigger).Error; err != nil {
					t.Fatalf("drop %s: %v", trigger, err)
				}
			}
			if err := database.GORMDB().Exec(`
				DELETE FROM search_attempt_audit_events
				WHERE tenant_id = ? AND sequence = ?
			`, "tenant-corrupt-boundary", test.sequence).Error; err != nil {
				t.Fatal(err)
			}

			tx := database.GORMDB().WithContext(ctx).Begin()
			if tx.Error != nil {
				t.Fatal(tx.Error)
			}
			err := store.AppendSearchAttemptInTransaction(
				ctx,
				tx,
				"tenant-corrupt-boundary",
				searchAuditTestDefinition("owner", "job-new", 4*time.Microsecond),
			)
			if !errors.Is(err, ErrCorrupt) {
				_ = tx.Rollback().Error
				t.Fatalf("AppendSearchAttemptInTransaction(corrupt boundary) error = %v", err)
			}
			if err := tx.Rollback().Error; err != nil {
				t.Fatal(err)
			}
			assertSearchAuditFailedAppendDidNotAdvance(
				t,
				database,
				"tenant-corrupt-boundary",
				1,
				4,
				3,
				2,
			)
		})
	}
}

func TestAppendRejectsRowsBehindEmptyStateBeforeMutation(t *testing.T) {
	ctx := context.Background()
	_, database := openSearchAuditTestDatabase(t)
	store := newSearchAuditTestStore(t, database, searchAuditTestCursorKey(), 5)
	if err := database.GORMDB().Create(&searchAttemptTenantStateRecord{
		TenantID:                "tenant-corrupt-empty",
		FirstSequence:           1,
		NextSequence:            1,
		RetainedCount:           0,
		MaximumRetainedAttempts: 5,
	}).Error; err != nil {
		t.Fatal(err)
	}

	triggerNames := []string{
		"search_attempt_audit_event_insert_requires_current_state",
		"search_attempt_audit_event_advances_and_prunes",
	}
	triggerSQL := make([]string, len(triggerNames))
	for index, trigger := range triggerNames {
		if err := database.GORMDB().Raw(`
			SELECT sql
			FROM sqlite_schema
			WHERE type = 'trigger' AND name = ?
		`, trigger).Scan(&triggerSQL[index]).Error; err != nil || triggerSQL[index] == "" {
			t.Fatalf("read %s SQL: %q, %v", trigger, triggerSQL[index], err)
		}
		if err := database.GORMDB().Exec("DROP TRIGGER " + trigger).Error; err != nil {
			t.Fatalf("drop %s: %v", trigger, err)
		}
	}
	if err := database.GORMDB().Create(&searchAttemptEventRecord{
		TenantID:            "tenant-corrupt-empty",
		Sequence:            2,
		OccurredAtUnixMicro: searchAuditTestTime.UnixMicro(),
		ActorKind:           audit.ActorKindSystem,
		ActorID:             defaultSystemActorID,
		ActorRole:           audit.ActorRoleSystem,
		OwnerID:             "owner",
		SearchJobID:         "job-forged",
	}).Error; err != nil {
		t.Fatal(err)
	}
	for index, sql := range triggerSQL {
		if err := database.GORMDB().Exec(sql).Error; err != nil {
			t.Fatalf("restore %s: %v", triggerNames[index], err)
		}
	}

	tx := database.GORMDB().WithContext(ctx).Begin()
	if tx.Error != nil {
		t.Fatal(tx.Error)
	}
	err := store.AppendSearchAttemptInTransaction(
		ctx,
		tx,
		"tenant-corrupt-empty",
		searchAuditTestDefinition("owner", "job-new", time.Microsecond),
	)
	if !errors.Is(err, ErrCorrupt) {
		_ = tx.Rollback().Error
		t.Fatalf("AppendSearchAttemptInTransaction(corrupt empty state) error = %v", err)
	}
	if err := tx.Rollback().Error; err != nil {
		t.Fatal(err)
	}
	assertSearchAuditFailedAppendDidNotAdvance(
		t,
		database,
		"tenant-corrupt-empty",
		1,
		1,
		0,
		1,
	)
}

func assertSearchAuditFailedAppendDidNotAdvance(
	t *testing.T,
	database *control.DB,
	tenantID string,
	wantFirst int64,
	wantNext int64,
	wantRetained int64,
	wantRows int64,
) {
	t.Helper()
	var state searchAttemptTenantStateRecord
	if err := database.GORMDB().Where("tenant_id = ?", tenantID).Take(&state).Error; err != nil {
		t.Fatal(err)
	}
	if state.FirstSequence != wantFirst || state.NextSequence != wantNext ||
		state.RetainedCount != wantRetained {
		t.Fatalf("state advanced after rejected append: %+v", state)
	}
	var rows, newJob int64
	if err := database.GORMDB().Model(&searchAttemptEventRecord{}).
		Where("tenant_id = ?", tenantID).Count(&rows).Error; err != nil {
		t.Fatal(err)
	}
	if err := database.GORMDB().Model(&searchAttemptEventRecord{}).
		Where("tenant_id = ? AND search_job_id = ?", tenantID, "job-new").
		Count(&newJob).Error; err != nil {
		t.Fatal(err)
	}
	if rows != wantRows || newJob != 0 {
		t.Fatalf("rows after rejected append = %d, new job rows = %d", rows, newJob)
	}
}

func TestAppendRejectsInvalidAndForeignTransactionInputs(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	_, database := openSearchAuditTestDatabase(t)
	store := newSearchAuditTestStore(t, database, nil, 2)
	definition := searchAuditTestDefinition("owner", "job", 0)

	if err := store.AppendSearchAttemptInTransaction(
		ctx,
		database.GORMDB(),
		"tenant",
		definition,
	); !errors.Is(err, control.ErrInvalidArgument) {
		t.Fatalf("autocommit append error = %v", err)
	}
	_, foreign := openSearchAuditTestDatabase(t)
	foreignTx := foreign.GORMDB().WithContext(ctx).Begin()
	if foreignTx.Error != nil {
		t.Fatal(foreignTx.Error)
	}
	if err := store.AppendSearchAttemptInTransaction(ctx, foreignTx, "tenant", definition); !errors.Is(err, control.ErrInvalidArgument) {
		_ = foreignTx.Rollback().Error
		t.Fatalf("foreign append error = %v", err)
	}
	_ = foreignTx.Rollback().Error

	tx := database.GORMDB().WithContext(ctx).Begin()
	if tx.Error != nil {
		t.Fatal(tx.Error)
	}
	badDefinitions := []searchhistory.SearchAttemptAuditEvent{
		searchAuditTestDefinition(" owner", "job", 0),
		searchAuditTestDefinition("owner", " job", 0),
		searchAuditTestDefinition("owner", "job", 0),
	}
	badDefinitions[2].OccurredAt = time.Time{}
	for _, bad := range badDefinitions {
		if err := store.AppendSearchAttemptInTransaction(ctx, tx, "tenant", bad); !errors.Is(err, control.ErrInvalidArgument) {
			_ = tx.Rollback().Error
			t.Fatalf("invalid definition %+v error = %v", bad, err)
		}
	}
	_ = tx.Rollback().Error
}

func TestConstructionValidatesConfigurationAndDetachesCursorKey(t *testing.T) {
	t.Parallel()
	_, database := openSearchAuditTestDatabase(t)

	if store, err := New(nil, Options{}); store != nil || !errors.Is(err, control.ErrInvalidArgument) {
		t.Fatalf("New(nil) = (%v, %v)", store, err)
	}
	//nolint:staticcheck // Explicitly verifies the exported nil-context guard.
	if store, err := NewWithContext(nil, database, Options{}); store != nil ||
		!errors.Is(err, control.ErrInvalidArgument) {
		t.Fatalf("NewWithContext(nil) = (%v, %v)", store, err)
	}
	for _, options := range []Options{
		{CursorKey: []byte{1}},
		{CursorKey: bytes.Repeat([]byte{1}, maximumCursorKeyBytes+1)},
		{MaximumRetainedAttempts: MaximumRetainedAttempts + 1},
	} {
		if store, err := New(database, options); store != nil ||
			!errors.Is(err, control.ErrInvalidArgument) {
			t.Fatalf("New(%+v) = (%v, %v)", options, store, err)
		}
	}

	key := searchAuditTestCursorKey()
	store, err := New(database, Options{CursorKey: key})
	if err != nil {
		t.Fatalf("New(default maximum): %v", err)
	}
	if store.maximumRetainedAttempts != DefaultMaximumRetainedAttempts {
		t.Fatalf("default maximum = %d", store.maximumRetainedAttempts)
	}
	key[0] ^= 0xff
	if bytes.Equal(store.cursorKey, key) {
		t.Fatal("store retained caller cursor-key storage")
	}
	appendSearchAuditTestEvent(
		t,
		store,
		database,
		context.Background(),
		"default-cap",
		searchAuditTestDefinition("owner", "job", 0),
	)
	var state searchAttemptTenantStateRecord
	if err := database.GORMDB().Where("tenant_id = ?", "default-cap").Take(&state).Error; err != nil {
		t.Fatal(err)
	}
	if state.MaximumRetainedAttempts != DefaultMaximumRetainedAttempts {
		t.Fatalf("persisted default maximum = %d", state.MaximumRetainedAttempts)
	}
}

func searchAuditSequences(events []Event) []uint64 {
	result := make([]uint64, len(events))
	for index, event := range events {
		result[index] = event.Sequence
	}
	return result
}
