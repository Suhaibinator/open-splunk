package audit

import (
	"context"
	"testing"
)

func TestAppAuditTaxonomyValidatesActionTargetAndVersion(t *testing.T) {
	t.Parallel()

	valid := []struct {
		action  Action
		version uint64
	}{
		{ActionAppCreate, 1},
		{ActionAppUpdate, 2},
		{ActionAppActivate, 2},
		{ActionAppArchive, 2},
		{ActionAppDelete, 2},
	}
	for _, testCase := range valid {
		t.Run(string(testCase.action), func(t *testing.T) {
			t.Parallel()
			event := SuccessfulEvent{
				OccurredAt:    auditTestTime,
				Action:        testCase.action,
				TargetKind:    TargetKindApp,
				TargetID:      "app-observability",
				TargetVersion: testCase.version,
			}
			if !event.valid() {
				t.Fatalf("valid app event rejected: %+v", event)
			}
			for _, otherKind := range []TargetKind{
				TargetKindIngestionToken,
				TargetKindIndex,
			} {
				forged := event
				forged.TargetKind = otherKind
				if forged.valid() {
					t.Fatalf("cross-family action/target accepted: %+v", forged)
				}
			}
		})
	}

	for _, testCase := range []struct {
		action  Action
		version uint64
	}{
		{ActionAppCreate, 2},
		{ActionAppUpdate, 1},
		{ActionAppActivate, 1},
		{ActionAppArchive, 1},
		{ActionAppDelete, 1},
	} {
		event := SuccessfulEvent{
			OccurredAt:    auditTestTime,
			Action:        testCase.action,
			TargetKind:    TargetKindApp,
			TargetID:      "app-observability",
			TargetVersion: testCase.version,
		}
		if event.valid() {
			t.Fatalf("invalid app version accepted: %+v", event)
		}
	}
}

func TestAppAuditEventsPersistAndSQLiteRejectsTaxonomyForgeries(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	_, database := openAuditTestDatabase(t)
	store := newAuditTestStore(t, database, auditTestCursorKey())
	events := []SuccessfulEvent{
		{OccurredAt: auditTestTime, Action: ActionAppCreate, TargetKind: TargetKindApp, TargetID: "app-observability", TargetVersion: 1},
		{OccurredAt: auditTestTime, Action: ActionAppUpdate, TargetKind: TargetKindApp, TargetID: "app-observability", TargetVersion: 2},
		{OccurredAt: auditTestTime, Action: ActionAppArchive, TargetKind: TargetKindApp, TargetID: "app-observability", TargetVersion: 3},
		{OccurredAt: auditTestTime, Action: ActionAppActivate, TargetKind: TargetKindApp, TargetID: "app-observability", TargetVersion: 4},
		{OccurredAt: auditTestTime, Action: ActionAppArchive, TargetKind: TargetKindApp, TargetID: "app-observability", TargetVersion: 5},
		{OccurredAt: auditTestTime, Action: ActionAppDelete, TargetKind: TargetKindApp, TargetID: "app-observability", TargetVersion: 5},
	}
	for index, definition := range events {
		persisted, err := store.Append(ctx, "tenant-app", definition)
		if err != nil {
			t.Fatalf("Append(%s): %v", definition.Action, err)
		}
		if persisted.Sequence != uint64(index+1) ||
			persisted.Action != definition.Action ||
			persisted.TargetKind != TargetKindApp ||
			persisted.TargetID != definition.TargetID ||
			persisted.TargetVersion != definition.TargetVersion {
			t.Fatalf("persisted event = %+v, definition = %+v", persisted, definition)
		}
	}
	page, err := store.List(ctx, "tenant-app", ListRequest{
		PageSize:      uint32(len(events)),
		ActionFilters: allKnownAuditActions(),
		TargetKind:    targetKindPointer(TargetKindApp),
	})
	if err != nil {
		t.Fatalf("List(app events): %v", err)
	}
	if len(page.Events) != len(events) {
		t.Fatalf("listed event count = %d, want %d", len(page.Events), len(events))
	}

	created, err := store.Append(ctx, "tenant-app-schema", events[0])
	if err != nil {
		t.Fatalf("Append(schema anchor): %v", err)
	}
	insertEvent := `INSERT INTO audit_events (
		tenant_id, sequence, occurred_at_unix_micro,
		actor_kind, actor_id, actor_role, action,
		target_kind, target_id, target_version
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
	for _, forgery := range []struct {
		name       string
		action     Action
		targetKind TargetKind
		version    uint64
	}{
		{"token action with app target", ActionIngestionTokenUpdate, TargetKindApp, 2},
		{"index action with app target", ActionIndexUpdate, TargetKindApp, 2},
		{"app action with token target", ActionAppUpdate, TargetKindIngestionToken, 2},
		{"app action with index target", ActionAppUpdate, TargetKindIndex, 2},
		{"app create version two", ActionAppCreate, TargetKindApp, 2},
		{"app update version one", ActionAppUpdate, TargetKindApp, 1},
		{"app activate version one", ActionAppActivate, TargetKindApp, 1},
		{"app archive version one", ActionAppArchive, TargetKindApp, 1},
		{"app delete version one", ActionAppDelete, TargetKindApp, 1},
	} {
		t.Run(forgery.name, func(t *testing.T) {
			_, err := database.SQLDB().ExecContext(
				ctx,
				insertEvent,
				created.TenantID,
				2,
				auditTestTime.UnixMicro(),
				"system",
				defaultSystemActorID,
				"system",
				forgery.action,
				forgery.targetKind,
				"app-observability",
				forgery.version,
			)
			if err == nil {
				t.Fatal("forged app audit event succeeded")
			}
		})
	}
}
