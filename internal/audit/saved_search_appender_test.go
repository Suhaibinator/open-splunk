package audit

import (
	"context"
	"errors"
	"math"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/savedobjects"
)

func TestSavedSearchMutationAppenderMapsAllActionsAndActorsInOneTransaction(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	database := openAuditTestDatabase(t)
	store := newAuditTestStore(t, database, auditTestCursorKey())
	ordinaryUser := Actor{
		Kind: ActorKindBrowser,
		ID:   "saved-search-user",
		Role: ActorRoleUser,
	}
	administrator := Actor{
		Kind: ActorKindBrowser,
		ID:   "saved-search-administrator",
		Role: ActorRoleAdministrator,
	}
	tests := []struct {
		ctx         context.Context
		event       savedobjects.SavedSearchMutationAuditEvent
		auditAction Action
		actor       Actor
	}{
		{
			ctx: ctx,
			event: savedobjects.SavedSearchMutationAuditEvent{
				OccurredAt: auditTestTime, Action: savedobjects.SavedSearchMutationAuditActionCreate,
				SavedSearchID: "saved-search-a", SavedSearchVersion: 1,
			},
			auditAction: ActionSavedSearchCreate,
			actor: Actor{
				Kind: ActorKindSystem,
				ID:   defaultSystemActorID,
				Role: ActorRoleSystem,
			},
		},
		{
			ctx: actorContext(t, ordinaryUser),
			event: savedobjects.SavedSearchMutationAuditEvent{
				OccurredAt: auditTestTime, Action: savedobjects.SavedSearchMutationAuditActionUpdate,
				SavedSearchID: "saved-search-a", SavedSearchVersion: 2,
			},
			auditAction: ActionSavedSearchUpdate,
			actor:       ordinaryUser,
		},
		{
			ctx: actorContext(t, administrator),
			event: savedobjects.SavedSearchMutationAuditEvent{
				OccurredAt: auditTestTime, Action: savedobjects.SavedSearchMutationAuditActionDuplicate,
				SavedSearchID: "saved-search-copy", SavedSearchVersion: 1,
			},
			auditAction: ActionSavedSearchDuplicate,
			actor:       administrator,
		},
		{
			ctx: ctx,
			event: savedobjects.SavedSearchMutationAuditEvent{
				OccurredAt: auditTestTime, Action: savedobjects.SavedSearchMutationAuditActionDelete,
				SavedSearchID: "saved-search-copy", SavedSearchVersion: 1,
			},
			auditAction: ActionSavedSearchDelete,
			actor: Actor{
				Kind: ActorKindSystem,
				ID:   defaultSystemActorID,
				Role: ActorRoleSystem,
			},
		},
	}

	tx := database.GORMDB().WithContext(ctx).Begin()
	if tx.Error != nil {
		t.Fatalf("begin shared transaction: %v", tx.Error)
	}
	finished := false
	defer func() {
		if !finished {
			_ = tx.Rollback().Error
		}
	}()
	for _, testCase := range tests {
		if err := store.AppendSavedSearchMutationInTransaction(
			testCase.ctx,
			tx,
			"tenant-saved-search-adapter",
			testCase.event,
		); err != nil {
			t.Fatalf("AppendSavedSearchMutationInTransaction(%s): %v", testCase.event.Action, err)
		}
	}

	var transactionEventCount int64
	if err := tx.Model(&auditEventRecord{}).
		Where("tenant_id = ?", "tenant-saved-search-adapter").
		Count(&transactionEventCount).Error; err != nil {
		t.Fatalf("count transaction-local events: %v", err)
	}
	if transactionEventCount != int64(len(tests)) {
		t.Fatalf("transaction-local event count = %d, want %d", transactionEventCount, len(tests))
	}
	var state auditTenantStateRecord
	if err := tx.Where("tenant_id = ?", "tenant-saved-search-adapter").Take(&state).Error; err != nil {
		t.Fatalf("read transaction-local tenant state: %v", err)
	}
	if state.EventCount != int64(len(tests)) || state.NextSequence != int64(len(tests)+1) {
		t.Fatalf("transaction-local tenant state = %+v", state)
	}
	if err := tx.Commit().Error; err != nil {
		t.Fatalf("commit shared transaction: %v", err)
	}
	finished = true

	page, err := store.List(ctx, "tenant-saved-search-adapter", ListRequest{
		PageSize:     uint32(len(tests)),
		TargetKind:   new(TargetKindSavedSearch),
		IncludeTotal: true,
	})
	if err != nil {
		t.Fatalf("List(saved-search adapter events): %v", err)
	}
	if len(page.Events) != len(tests) || page.TotalSize == nil ||
		*page.TotalSize != uint64(len(tests)) || !page.TotalSizeExact {
		t.Fatalf("saved-search adapter page = %+v", page)
	}
	wantOccurredAt, ok := CanonicalOccurrenceTime(auditTestTime)
	if !ok {
		t.Fatal("audit fixture time is invalid")
	}
	for index, event := range page.Events {
		want := tests[len(tests)-1-index]
		if event.Sequence != uint64(len(tests)-index) ||
			event.TenantID != "tenant-saved-search-adapter" ||
			!event.OccurredAt.Equal(wantOccurredAt) ||
			event.Actor != want.actor ||
			event.Action != want.auditAction ||
			event.TargetKind != TargetKindSavedSearch ||
			event.TargetID != want.event.SavedSearchID ||
			event.TargetVersion != want.event.SavedSearchVersion {
			t.Fatalf("event[%d] = %+v, want mapping %+v", index, event, want)
		}
	}
}

func TestSavedSearchMutationAppenderRejectsUnknownAndInvalidEventsBeforeWriting(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	database := openAuditTestDatabase(t)
	store := newAuditTestStore(t, database, auditTestCursorKey())
	tests := []struct {
		name  string
		event savedobjects.SavedSearchMutationAuditEvent
	}{
		{
			name: "unknown action",
			event: savedobjects.SavedSearchMutationAuditEvent{
				OccurredAt: auditTestTime, Action: savedobjects.SavedSearchMutationAuditAction("saved_search.publish"),
				SavedSearchID: "saved-search", SavedSearchVersion: 1,
			},
		},
		{name: "zero typed event", event: savedobjects.SavedSearchMutationAuditEvent{}},
		{
			name: "missing timestamp",
			event: savedobjects.SavedSearchMutationAuditEvent{
				Action:        savedobjects.SavedSearchMutationAuditActionCreate,
				SavedSearchID: "saved-search", SavedSearchVersion: 1,
			},
		},
		{
			name: "empty ID",
			event: savedobjects.SavedSearchMutationAuditEvent{
				OccurredAt: auditTestTime, Action: savedobjects.SavedSearchMutationAuditActionCreate,
				SavedSearchVersion: 1,
			},
		},
		{
			name: "create version two",
			event: savedobjects.SavedSearchMutationAuditEvent{
				OccurredAt: auditTestTime, Action: savedobjects.SavedSearchMutationAuditActionCreate,
				SavedSearchID: "saved-search", SavedSearchVersion: 2,
			},
		},
		{
			name: "update version one",
			event: savedobjects.SavedSearchMutationAuditEvent{
				OccurredAt: auditTestTime, Action: savedobjects.SavedSearchMutationAuditActionUpdate,
				SavedSearchID: "saved-search", SavedSearchVersion: 1,
			},
		},
		{
			name: "duplicate version two",
			event: savedobjects.SavedSearchMutationAuditEvent{
				OccurredAt: auditTestTime, Action: savedobjects.SavedSearchMutationAuditActionDuplicate,
				SavedSearchID: "saved-search-copy", SavedSearchVersion: 2,
			},
		},
		{
			name: "delete version zero",
			event: savedobjects.SavedSearchMutationAuditEvent{
				OccurredAt: auditTestTime, Action: savedobjects.SavedSearchMutationAuditActionDelete,
				SavedSearchID: "saved-search", SavedSearchVersion: 0,
			},
		},
		{
			name: "version outside SQLite range",
			event: savedobjects.SavedSearchMutationAuditEvent{
				OccurredAt: auditTestTime, Action: savedobjects.SavedSearchMutationAuditActionDelete,
				SavedSearchID: "saved-search", SavedSearchVersion: uint64(math.MaxInt64) + 1,
			},
		},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			tx := database.GORMDB().WithContext(ctx).Begin()
			if tx.Error != nil {
				t.Fatalf("begin: %v", tx.Error)
			}
			defer func() { _ = tx.Rollback().Error }()
			err := store.AppendSavedSearchMutationInTransaction(
				ctx,
				tx,
				"tenant-saved-search-rejected",
				testCase.event,
			)
			if !errors.Is(err, control.ErrInvalidArgument) {
				t.Fatalf("AppendSavedSearchMutationInTransaction = %v, want invalid argument", err)
			}
			var eventCount, stateCount int64
			if err := tx.Model(&auditEventRecord{}).Count(&eventCount).Error; err != nil {
				t.Fatalf("count events after rejected event: %v", err)
			}
			if err := tx.Model(&auditTenantStateRecord{}).Count(&stateCount).Error; err != nil {
				t.Fatalf("count states after rejected event: %v", err)
			}
			if eventCount != 0 || stateCount != 0 {
				t.Fatalf("rejected event wrote events=%d states=%d", eventCount, stateCount)
			}
		})
	}
}

func TestSavedSearchMutationAppenderRejectsForeignAndAutocommitTransactions(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	database := openAuditTestDatabase(t)
	store := newAuditTestStore(t, database, auditTestCursorKey())
	event := savedobjects.SavedSearchMutationAuditEvent{
		OccurredAt:         auditTestTime,
		Action:             savedobjects.SavedSearchMutationAuditActionCreate,
		SavedSearchID:      "saved-search",
		SavedSearchVersion: 1,
	}

	if err := store.AppendSavedSearchMutationInTransaction(
		ctx,
		database.GORMDB(),
		"tenant-saved-search-autocommit",
		event,
	); !errors.Is(err, control.ErrInvalidArgument) {
		t.Fatalf("autocommit append error = %v, want invalid argument", err)
	}
	if err := store.AppendSavedSearchMutationInTransaction(
		ctx,
		nil,
		"tenant-saved-search-nil-transaction",
		event,
	); !errors.Is(err, control.ErrInvalidArgument) {
		t.Fatalf("nil transaction append error = %v, want invalid argument", err)
	}

	foreignDatabase := openAuditTestDatabase(t)
	foreignTx := foreignDatabase.GORMDB().WithContext(ctx).Begin()
	if foreignTx.Error != nil {
		t.Fatalf("begin foreign transaction: %v", foreignTx.Error)
	}
	defer func() { _ = foreignTx.Rollback().Error }()
	if err := store.AppendSavedSearchMutationInTransaction(
		ctx,
		foreignTx,
		"tenant-saved-search-foreign",
		event,
	); !errors.Is(err, control.ErrInvalidArgument) {
		t.Fatalf("foreign transaction append error = %v, want invalid argument", err)
	}

	for _, candidate := range []*control.DB{database, foreignDatabase} {
		var eventCount, stateCount int64
		if err := candidate.GORMDB().Model(&auditEventRecord{}).Count(&eventCount).Error; err != nil {
			t.Fatalf("count rejected transaction events: %v", err)
		}
		if err := candidate.GORMDB().Model(&auditTenantStateRecord{}).Count(&stateCount).Error; err != nil {
			t.Fatalf("count rejected transaction states: %v", err)
		}
		if eventCount != 0 || stateCount != 0 {
			t.Fatalf("rejected transaction wrote events=%d states=%d", eventCount, stateCount)
		}
	}
}

func TestSavedSearchMutationAppenderRollbackRemainsProvisional(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	database := openAuditTestDatabase(t)
	store := newAuditTestStore(t, database, auditTestCursorKey())
	event := savedobjects.SavedSearchMutationAuditEvent{
		OccurredAt:         auditTestTime,
		Action:             savedobjects.SavedSearchMutationAuditActionCreate,
		SavedSearchID:      "saved-search-rollback",
		SavedSearchVersion: 1,
	}

	tx := database.GORMDB().WithContext(ctx).Begin()
	if tx.Error != nil {
		t.Fatalf("begin provisional transaction: %v", tx.Error)
	}
	if err := store.AppendSavedSearchMutationInTransaction(
		ctx,
		tx,
		"tenant-saved-search-rollback",
		event,
	); err != nil {
		_ = tx.Rollback().Error
		t.Fatalf("append provisional saved-search event: %v", err)
	}
	var provisional auditEventRecord
	if err := tx.Where(
		"tenant_id = ? AND sequence = ?",
		"tenant-saved-search-rollback",
		1,
	).Take(&provisional).Error; err != nil {
		_ = tx.Rollback().Error
		t.Fatalf("read provisional saved-search event: %v", err)
	}
	if provisional.Action != ActionSavedSearchCreate ||
		provisional.TargetKind != TargetKindSavedSearch ||
		provisional.TargetID != event.SavedSearchID ||
		provisional.TargetVersion != 1 {
		_ = tx.Rollback().Error
		t.Fatalf("provisional event = %+v", provisional)
	}
	if err := tx.Rollback().Error; err != nil {
		t.Fatalf("rollback provisional transaction: %v", err)
	}

	page, err := store.List(ctx, "tenant-saved-search-rollback", ListRequest{IncludeTotal: true})
	if err != nil {
		t.Fatalf("List after rollback: %v", err)
	}
	if len(page.Events) != 0 || page.TotalSize == nil || *page.TotalSize != 0 {
		t.Fatalf("events survived caller rollback: %+v", page)
	}
	var stateCount int64
	if err := database.GORMDB().Model(&auditTenantStateRecord{}).
		Where("tenant_id = ?", "tenant-saved-search-rollback").
		Count(&stateCount).Error; err != nil {
		t.Fatalf("count tenant state after rollback: %v", err)
	}
	if stateCount != 0 {
		t.Fatalf("tenant state survived caller rollback: %d", stateCount)
	}

	committedTx := database.GORMDB().WithContext(ctx).Begin()
	if committedTx.Error != nil {
		t.Fatalf("begin committed transaction: %v", committedTx.Error)
	}
	if err := store.AppendSavedSearchMutationInTransaction(
		ctx,
		committedTx,
		"tenant-saved-search-rollback",
		event,
	); err != nil {
		_ = committedTx.Rollback().Error
		t.Fatalf("append after rollback: %v", err)
	}
	if err := committedTx.Commit().Error; err != nil {
		t.Fatalf("commit after rollback: %v", err)
	}
	page, err = store.List(ctx, "tenant-saved-search-rollback", ListRequest{})
	if err != nil || len(page.Events) != 1 || page.Events[0].Sequence != 1 {
		t.Fatalf("events after rollback retry = (%+v, %v)", page, err)
	}
}
