package audit

import (
	"context"
	"errors"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/control"
)

func TestAppMutationAppenderRequiresExplicitSuccessfulActor(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	database := openAuditTestDatabase(t)
	store := newAuditTestStore(t, database, auditTestCursorKey())
	event := control.AppMutationAuditEvent{
		OccurredAt: auditTestTime,
		Action:     control.AppMutationAuditActionCreate,
		AppID:      "app-observability",
		AppVersion: 1,
	}

	for _, testCase := range []struct {
		name string
		ctx  context.Context
	}{
		{name: "missing", ctx: ctx},
		{name: "browser user", ctx: actorContext(t, Actor{
			Kind: ActorKindBrowser, ID: "user", Role: ActorRoleUser,
		})},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			tx := database.GORMDB().WithContext(testCase.ctx).Begin()
			if tx.Error != nil {
				t.Fatalf("begin: %v", tx.Error)
			}
			err := store.AppendAppMutationInTransaction(
				testCase.ctx,
				tx,
				"tenant-app-actor",
				event,
			)
			if rollbackErr := tx.Rollback().Error; rollbackErr != nil {
				t.Fatalf("rollback: %v", rollbackErr)
			}
			if !errors.Is(err, control.ErrInvalidArgument) {
				t.Fatalf("AppendAppMutationInTransaction = %v, want invalid", err)
			}
		})
	}

	page, err := store.List(ctx, "tenant-app-actor", ListRequest{})
	if err != nil || len(page.Events) != 0 {
		t.Fatalf("events after rejected actors = (%+v, %v)", page, err)
	}
}

func TestAppMutationAppenderMapsAllControlActions(t *testing.T) {
	t.Parallel()

	actors := []Actor{
		{Kind: ActorKindSystem, ID: "app-controller", Role: ActorRoleSystem},
		{Kind: ActorKindBrowser, ID: "administrator", Role: ActorRoleAdministrator},
	}
	for _, actor := range actors {
		actor := actor
		t.Run(string(actor.Kind), func(t *testing.T) {
			t.Parallel()
			ctx := actorContext(t, actor)
			database := openAuditTestDatabase(t)
			store := newAuditTestStore(t, database, auditTestCursorKey())
			tests := []struct {
				controlAction control.AppMutationAuditAction
				auditAction   Action
				version       uint64
			}{
				{control.AppMutationAuditActionCreate, ActionAppCreate, 1},
				{control.AppMutationAuditActionUpdate, ActionAppUpdate, 2},
				{control.AppMutationAuditActionActivate, ActionAppActivate, 3},
				{control.AppMutationAuditActionArchive, ActionAppArchive, 4},
				{control.AppMutationAuditActionDelete, ActionAppDelete, 4},
			}
			for _, testCase := range tests {
				tx := database.GORMDB().WithContext(ctx).Begin()
				if tx.Error != nil {
					t.Fatalf("begin %s: %v", testCase.controlAction, tx.Error)
				}
				err := store.AppendAppMutationInTransaction(ctx, tx, "tenant-app-adapter", control.AppMutationAuditEvent{
					OccurredAt: auditTestTime,
					Action:     testCase.controlAction,
					AppID:      "app-observability",
					AppVersion: testCase.version,
				})
				if err != nil {
					_ = tx.Rollback()
					t.Fatalf("AppendAppMutationInTransaction(%s): %v", testCase.controlAction, err)
				}
				if err := tx.Commit().Error; err != nil {
					t.Fatalf("commit %s: %v", testCase.controlAction, err)
				}
			}

			page, err := store.List(ctx, "tenant-app-adapter", ListRequest{
				PageSize:   uint32(len(tests)),
				TargetKind: new(TargetKindApp),
			})
			if err != nil {
				t.Fatalf("List(app adapter events): %v", err)
			}
			if len(page.Events) != len(tests) {
				t.Fatalf("listed events = %d, want %d", len(page.Events), len(tests))
			}
			for index, event := range page.Events {
				want := tests[len(tests)-1-index]
				if event.Action != want.auditAction || event.TargetKind != TargetKindApp ||
					event.TargetID != "app-observability" || event.TargetVersion != want.version ||
					event.Actor.ID != actor.ID {
					t.Fatalf("event[%d] = %+v, want action %s/version %d", index, event, want.auditAction, want.version)
				}
			}

			tx := database.GORMDB().WithContext(ctx).Begin()
			if tx.Error != nil {
				t.Fatalf("begin unknown action: %v", tx.Error)
			}
			err = store.AppendAppMutationInTransaction(ctx, tx, "tenant-app-adapter", control.AppMutationAuditEvent{
				OccurredAt: auditTestTime,
				Action:     control.AppMutationAuditAction("app.unknown"),
				AppID:      "app-observability",
				AppVersion: 5,
			})
			if rollbackErr := tx.Rollback().Error; rollbackErr != nil {
				t.Fatalf("rollback unknown action: %v", rollbackErr)
			}
			if !errors.Is(err, control.ErrInvalidArgument) {
				t.Fatalf("unknown control action error = %v, want invalid", err)
			}
		})
	}
}
