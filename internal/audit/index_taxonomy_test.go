package audit

import (
	"context"
	"testing"
)

func TestIndexAuditTaxonomyValidatesActionTargetAndVersion(t *testing.T) {
	t.Parallel()

	valid := []struct {
		action  Action
		version uint64
	}{
		{ActionIndexCreate, 1},
		{ActionIndexUpdate, 2},
		{ActionIndexActivate, 2},
		{ActionIndexArchive, 2},
		{ActionIndexDeleteKeepData, 2},
		{ActionIndexDeleteData, 3},
	}
	for _, testCase := range valid {
		t.Run(string(testCase.action), func(t *testing.T) {
			t.Parallel()
			event := SuccessfulEvent{
				OccurredAt:    auditTestTime,
				Action:        testCase.action,
				TargetKind:    TargetKindIndex,
				TargetID:      "events",
				TargetVersion: testCase.version,
			}
			if !event.valid() {
				t.Fatalf("valid index event rejected: %+v", event)
			}
			event.TargetKind = TargetKindIngestionToken
			if event.valid() {
				t.Fatalf("cross-family action/target accepted: %+v", event)
			}
		})
	}

	invalidVersions := []struct {
		action  Action
		version uint64
	}{
		{ActionIndexCreate, 2},
		{ActionIndexUpdate, 1},
		{ActionIndexActivate, 1},
		{ActionIndexArchive, 1},
		{ActionIndexDeleteKeepData, 1},
		{ActionIndexDeleteData, 2},
	}
	for _, testCase := range invalidVersions {
		event := SuccessfulEvent{
			OccurredAt:    auditTestTime,
			Action:        testCase.action,
			TargetKind:    TargetKindIndex,
			TargetID:      "events",
			TargetVersion: testCase.version,
		}
		if event.valid() {
			t.Fatalf("invalid version accepted: %+v", event)
		}
	}

	tokenEvent := auditTestDefinition(ActionIngestionTokenCreate, "token", 1)
	tokenEvent.TargetKind = TargetKindIndex
	if tokenEvent.valid() {
		t.Fatalf("token action with index target accepted: %+v", tokenEvent)
	}
}

func TestAuditListAcceptsAllKnownActionFilters(t *testing.T) {
	t.Parallel()

	actions := []Action{
		ActionIngestionTokenCreate,
		ActionIngestionTokenUpdate,
		ActionIngestionTokenRevoke,
		ActionIndexCreate,
		ActionIndexUpdate,
		ActionIndexActivate,
		ActionIndexArchive,
		ActionIndexDeleteKeepData,
		ActionIndexDeleteData,
	}
	normalized, err := normalizeListRequest("tenant", ListRequest{ActionFilters: actions})
	if err != nil {
		t.Fatalf("normalizeListRequest(all known actions): %v", err)
	}
	if len(normalized.actionFilters) != len(actions) {
		t.Fatalf("normalized action count = %d, want %d", len(normalized.actionFilters), len(actions))
	}
}

func TestIndexAuditEventsPersistAndSQLiteRejectsTaxonomyForgeries(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	database := openAuditTestDatabase(t)
	store := newAuditTestStore(t, database, auditTestCursorKey())
	events := []SuccessfulEvent{
		{OccurredAt: auditTestTime, Action: ActionIndexCreate, TargetKind: TargetKindIndex, TargetID: "events", TargetVersion: 1},
		{OccurredAt: auditTestTime, Action: ActionIndexUpdate, TargetKind: TargetKindIndex, TargetID: "events", TargetVersion: 2},
		{OccurredAt: auditTestTime, Action: ActionIndexActivate, TargetKind: TargetKindIndex, TargetID: "events", TargetVersion: 3},
		{OccurredAt: auditTestTime, Action: ActionIndexArchive, TargetKind: TargetKindIndex, TargetID: "events", TargetVersion: 4},
		{OccurredAt: auditTestTime, Action: ActionIndexDeleteKeepData, TargetKind: TargetKindIndex, TargetID: "events", TargetVersion: 5},
		{OccurredAt: auditTestTime, Action: ActionIndexDeleteData, TargetKind: TargetKindIndex, TargetID: "events", TargetVersion: 6},
	}
	for index, definition := range events {
		persisted, err := store.Append(ctx, "tenant-index", definition)
		if err != nil {
			t.Fatalf("Append(%s): %v", definition.Action, err)
		}
		if persisted.Sequence != uint64(index+1) ||
			persisted.Action != definition.Action ||
			persisted.TargetKind != TargetKindIndex ||
			persisted.TargetID != definition.TargetID ||
			persisted.TargetVersion != definition.TargetVersion {
			t.Fatalf("persisted event = %+v, definition = %+v", persisted, definition)
		}
	}
	page, err := store.List(ctx, "tenant-index", ListRequest{
		PageSize:      uint32(len(events)),
		ActionFilters: allKnownAuditActions(),
		TargetKind:    new(TargetKindIndex),
	})
	if err != nil {
		t.Fatalf("List(index events): %v", err)
	}
	if len(page.Events) != len(events) {
		t.Fatalf("listed event count = %d, want %d", len(page.Events), len(events))
	}

	created, err := store.Append(ctx, "tenant-index-schema", events[0])
	if err != nil {
		t.Fatalf("Append(schema anchor): %v", err)
	}
	insertEvent := `INSERT INTO audit_events (
		tenant_id, sequence, occurred_at_unix_micro,
		actor_kind, actor_id, actor_role, action,
		target_kind, target_id, target_version
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
	forgeries := []struct {
		name       string
		action     Action
		targetKind TargetKind
		version    uint64
	}{
		{"token action with index target", ActionIngestionTokenUpdate, TargetKindIndex, 2},
		{"index action with token target", ActionIndexUpdate, TargetKindIngestionToken, 2},
		{"index create version two", ActionIndexCreate, TargetKindIndex, 2},
		{"index update version one", ActionIndexUpdate, TargetKindIndex, 1},
		{"index activate version one", ActionIndexActivate, TargetKindIndex, 1},
		{"index archive version one", ActionIndexArchive, TargetKindIndex, 1},
		{"index delete keep data version one", ActionIndexDeleteKeepData, TargetKindIndex, 1},
		{"index delete data version two", ActionIndexDeleteData, TargetKindIndex, 2},
	}
	for _, forgery := range forgeries {
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
				"events",
				forgery.version,
			)
			if err == nil {
				t.Fatal("forged index audit event succeeded")
			}
		})
	}
}

func allKnownAuditActions() []Action {
	return []Action{
		ActionIngestionTokenCreate,
		ActionIngestionTokenUpdate,
		ActionIngestionTokenRevoke,
		ActionIndexCreate,
		ActionIndexUpdate,
		ActionIndexActivate,
		ActionIndexArchive,
		ActionIndexDeleteKeepData,
		ActionIndexDeleteData,
		ActionAppCreate,
		ActionAppUpdate,
		ActionAppActivate,
		ActionAppArchive,
		ActionAppDelete,
		ActionSavedSearchCreate,
		ActionSavedSearchUpdate,
		ActionSavedSearchDuplicate,
		ActionSavedSearchDelete,
		ActionKnowledgeObjectCreate,
		ActionKnowledgeObjectUpdate,
		ActionKnowledgeObjectScopeChange,
		ActionKnowledgeObjectEnable,
		ActionKnowledgeObjectDisable,
		ActionKnowledgeObjectDelete,
	}
}
