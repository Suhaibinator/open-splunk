package audit

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/control"
)

func TestIndexMutationAppenderRequiresExplicitSuccessfulActor(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	database := openAuditTestDatabase(t)
	store := newAuditTestStore(t, database, auditTestCursorKey())
	event := control.IndexMutationAuditEvent{
		OccurredAt:   auditTestTime,
		Action:       control.IndexMutationAuditActionCreate,
		IndexID:      "events",
		IndexVersion: 1,
	}

	tests := []struct {
		name string
		ctx  context.Context
	}{
		{name: "missing", ctx: ctx},
		{name: "browser user", ctx: actorContext(t, Actor{
			Kind: ActorKindBrowser, ID: "user", Role: ActorRoleUser,
		})},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			tx := database.GORMDB().WithContext(testCase.ctx).Begin()
			if tx.Error != nil {
				t.Fatalf("begin: %v", tx.Error)
			}
			err := store.AppendIndexMutationInTransaction(
				testCase.ctx,
				tx,
				"tenant-index-actor",
				event,
			)
			if rollbackErr := tx.Rollback().Error; rollbackErr != nil {
				t.Fatalf("rollback: %v", rollbackErr)
			}
			if !errors.Is(err, control.ErrInvalidArgument) {
				t.Fatalf("AppendIndexMutationInTransaction = %v, want invalid", err)
			}
			if strings.Contains(err.Error(), "ingestion-token") {
				t.Fatalf("actor error is object-specific: %v", err)
			}
		})
	}

	page, err := store.List(ctx, "tenant-index-actor", ListRequest{})
	if err != nil || len(page.Events) != 0 {
		t.Fatalf("events after rejected actors = (%+v, %v)", page, err)
	}
}

func TestIndexMutationAppenderMapsAllControlActions(t *testing.T) {
	t.Parallel()

	ctx := actorContext(t, Actor{
		Kind: ActorKindSystem, ID: "index-controller", Role: ActorRoleSystem,
	})
	database := openAuditTestDatabase(t)
	store := newAuditTestStore(t, database, auditTestCursorKey())
	tests := []struct {
		controlAction control.IndexMutationAuditAction
		auditAction   Action
		version       uint64
	}{
		{control.IndexMutationAuditActionCreate, ActionIndexCreate, 1},
		{control.IndexMutationAuditActionUpdate, ActionIndexUpdate, 2},
		{control.IndexMutationAuditActionActivate, ActionIndexActivate, 3},
		{control.IndexMutationAuditActionArchive, ActionIndexArchive, 4},
		{control.IndexMutationAuditActionDeleteKeepData, ActionIndexDeleteKeepData, 5},
		{control.IndexMutationAuditActionDeleteData, ActionIndexDeleteData, 6},
	}
	for _, testCase := range tests {
		tx := database.GORMDB().WithContext(ctx).Begin()
		if tx.Error != nil {
			t.Fatalf("begin %s: %v", testCase.controlAction, tx.Error)
		}
		err := store.AppendIndexMutationInTransaction(ctx, tx, "tenant-index-adapter", control.IndexMutationAuditEvent{
			OccurredAt:   auditTestTime,
			Action:       testCase.controlAction,
			IndexID:      "events",
			IndexVersion: testCase.version,
		})
		if err != nil {
			_ = tx.Rollback()
			t.Fatalf("AppendIndexMutationInTransaction(%s): %v", testCase.controlAction, err)
		}
		if err := tx.Commit().Error; err != nil {
			t.Fatalf("commit %s: %v", testCase.controlAction, err)
		}
	}

	page, err := store.List(ctx, "tenant-index-adapter", ListRequest{
		PageSize:   uint32(len(tests)),
		TargetKind: new(TargetKindIndex),
	})
	if err != nil {
		t.Fatalf("List(index adapter events): %v", err)
	}
	if len(page.Events) != len(tests) {
		t.Fatalf("listed events = %d, want %d", len(page.Events), len(tests))
	}
	for index, event := range page.Events {
		want := tests[len(tests)-1-index]
		if event.Action != want.auditAction || event.TargetKind != TargetKindIndex ||
			event.TargetID != "events" || event.TargetVersion != want.version ||
			event.Actor.ID != "index-controller" {
			t.Fatalf("event[%d] = %+v, want action %s/version %d", index, event, want.auditAction, want.version)
		}
	}

	tx := database.GORMDB().WithContext(ctx).Begin()
	if tx.Error != nil {
		t.Fatalf("begin unknown action: %v", tx.Error)
	}
	err = store.AppendIndexMutationInTransaction(ctx, tx, "tenant-index-adapter", control.IndexMutationAuditEvent{
		OccurredAt:   auditTestTime,
		Action:       control.IndexMutationAuditAction("index.unknown"),
		IndexID:      "events",
		IndexVersion: 7,
	})
	if rollbackErr := tx.Rollback().Error; rollbackErr != nil {
		t.Fatalf("rollback unknown action: %v", rollbackErr)
	}
	if !errors.Is(err, control.ErrInvalidArgument) {
		t.Fatalf("unknown control action error = %v, want invalid", err)
	}
}

func actorContext(t *testing.T, actor Actor) context.Context {
	t.Helper()
	ctx, err := WithActor(context.Background(), actor)
	if err != nil {
		t.Fatalf("WithActor(%+v): %v", actor, err)
	}
	return ctx
}
