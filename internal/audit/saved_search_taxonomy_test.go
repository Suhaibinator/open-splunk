package audit

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/control"
)

func TestSavedSearchAuditTaxonomyValidatesActionTargetAndVersion(t *testing.T) {
	t.Parallel()

	valid := []struct {
		name    string
		action  Action
		version uint64
	}{
		{"create", ActionSavedSearchCreate, 1},
		{"update minimum", ActionSavedSearchUpdate, 2},
		{"update later", ActionSavedSearchUpdate, 19},
		{"duplicate", ActionSavedSearchDuplicate, 1},
		{"delete version one", ActionSavedSearchDelete, 1},
		{"delete later version", ActionSavedSearchDelete, 19},
	}
	for _, testCase := range valid {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			event := SuccessfulEvent{
				OccurredAt:    auditTestTime,
				Action:        testCase.action,
				TargetKind:    TargetKindSavedSearch,
				TargetID:      "saved-search-observability",
				TargetVersion: testCase.version,
			}
			if !event.valid() {
				t.Fatalf("valid saved-search event rejected: %+v", event)
			}
			for _, otherKind := range []TargetKind{
				TargetKindIngestionToken,
				TargetKindIndex,
				TargetKindApp,
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
		name    string
		action  Action
		version uint64
	}{
		{"create zero", ActionSavedSearchCreate, 0},
		{"create version two", ActionSavedSearchCreate, 2},
		{"update zero", ActionSavedSearchUpdate, 0},
		{"update version one", ActionSavedSearchUpdate, 1},
		{"duplicate zero", ActionSavedSearchDuplicate, 0},
		{"duplicate version two", ActionSavedSearchDuplicate, 2},
		{"delete zero", ActionSavedSearchDelete, 0},
	} {
		event := SuccessfulEvent{
			OccurredAt:    auditTestTime,
			Action:        testCase.action,
			TargetKind:    TargetKindSavedSearch,
			TargetID:      "saved-search-observability",
			TargetVersion: testCase.version,
		}
		if event.valid() {
			t.Fatalf("%s accepted invalid version: %+v", testCase.name, event)
		}
	}
}

func TestSavedSearchAuditActorPolicyIsActionSpecific(t *testing.T) {
	t.Parallel()

	occurredAt := time.UnixMicro(auditTestTime.UnixMicro()).UTC()
	actors := []struct {
		name  string
		actor Actor
	}{
		{
			name: "system",
			actor: Actor{
				Kind: ActorKindSystem,
				ID:   defaultSystemActorID,
				Role: ActorRoleSystem,
			},
		},
		{
			name: "browser administrator",
			actor: Actor{
				Kind: ActorKindBrowser,
				ID:   "administrator",
				Role: ActorRoleAdministrator,
			},
		},
		{
			name: "browser user",
			actor: Actor{
				Kind: ActorKindBrowser,
				ID:   "single-user",
				Role: ActorRoleUser,
			},
		},
	}
	for _, actorCase := range actors {
		t.Run(actorCase.name, func(t *testing.T) {
			t.Parallel()
			for _, eventCase := range []struct {
				action  Action
				version uint64
			}{
				{ActionSavedSearchCreate, 1},
				{ActionSavedSearchUpdate, 2},
				{ActionSavedSearchDuplicate, 1},
				{ActionSavedSearchDelete, 1},
			} {
				event := Event{
					Sequence:      1,
					TenantID:      "tenant",
					OccurredAt:    occurredAt,
					Actor:         actorCase.actor,
					Action:        eventCase.action,
					TargetKind:    TargetKindSavedSearch,
					TargetID:      "saved-search-observability",
					TargetVersion: eventCase.version,
				}
				if err := event.ValidateForTenant("tenant"); err != nil {
					t.Fatalf("ValidateForTenant(%s, %s): %v", actorCase.name, eventCase.action, err)
				}
			}
		})
	}

	ordinaryUser := Actor{
		Kind: ActorKindBrowser,
		ID:   "single-user",
		Role: ActorRoleUser,
	}
	for _, event := range []Event{
		{
			Sequence: 1, TenantID: "tenant", OccurredAt: occurredAt, Actor: ordinaryUser,
			Action: ActionIngestionTokenUpdate, TargetKind: TargetKindIngestionToken,
			TargetID: "token", TargetVersion: 2,
		},
		{
			Sequence: 1, TenantID: "tenant", OccurredAt: occurredAt, Actor: ordinaryUser,
			Action: ActionIndexUpdate, TargetKind: TargetKindIndex,
			TargetID: "events", TargetVersion: 2,
		},
		{
			Sequence: 1, TenantID: "tenant", OccurredAt: occurredAt, Actor: ordinaryUser,
			Action: ActionAppUpdate, TargetKind: TargetKindApp,
			TargetID: "app-observability", TargetVersion: 2,
		},
	} {
		if err := event.ValidateForTenant("tenant"); !errors.Is(err, control.ErrInvalidArgument) {
			t.Fatalf("ordinary user administrative event error = %v, want invalid argument: %+v", err, event)
		}
	}
}

func TestSavedSearchAuditEventsPersistAndSQLiteRejectsTaxonomyForgeries(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	database := openAuditTestDatabase(t)
	store := newAuditTestStore(t, database, auditTestCursorKey())
	administrator := Actor{
		Kind: ActorKindBrowser,
		ID:   "administrator",
		Role: ActorRoleAdministrator,
	}
	administratorContext, err := WithActor(ctx, administrator)
	if err != nil {
		t.Fatalf("WithActor(administrator): %v", err)
	}
	ordinaryUser := Actor{
		Kind: ActorKindBrowser,
		ID:   "single-user",
		Role: ActorRoleUser,
	}
	userContext, err := WithActor(ctx, ordinaryUser)
	if err != nil {
		t.Fatalf("WithActor(user): %v", err)
	}

	events := []struct {
		ctx        context.Context
		definition SuccessfulEvent
		actor      Actor
	}{
		{
			ctx: ctx,
			definition: SuccessfulEvent{
				OccurredAt: auditTestTime, Action: ActionSavedSearchCreate,
				TargetKind: TargetKindSavedSearch, TargetID: "saved-search-a", TargetVersion: 1,
			},
			actor: Actor{Kind: ActorKindSystem, ID: defaultSystemActorID, Role: ActorRoleSystem},
		},
		{
			ctx: administratorContext,
			definition: SuccessfulEvent{
				OccurredAt: auditTestTime, Action: ActionSavedSearchUpdate,
				TargetKind: TargetKindSavedSearch, TargetID: "saved-search-a", TargetVersion: 2,
			},
			actor: administrator,
		},
		{
			ctx: userContext,
			definition: SuccessfulEvent{
				OccurredAt: auditTestTime, Action: ActionSavedSearchDuplicate,
				TargetKind: TargetKindSavedSearch, TargetID: "saved-search-b", TargetVersion: 1,
			},
			actor: ordinaryUser,
		},
		{
			ctx: userContext,
			definition: SuccessfulEvent{
				OccurredAt: auditTestTime, Action: ActionSavedSearchDelete,
				TargetKind: TargetKindSavedSearch, TargetID: "saved-search-b", TargetVersion: 1,
			},
			actor: ordinaryUser,
		},
	}
	for index, testCase := range events {
		persisted, appendErr := store.Append(testCase.ctx, "tenant-saved-search", testCase.definition)
		if appendErr != nil {
			t.Fatalf("Append(%s): %v", testCase.definition.Action, appendErr)
		}
		if persisted.Sequence != uint64(index+1) ||
			persisted.Actor != testCase.actor ||
			persisted.Action != testCase.definition.Action ||
			persisted.TargetKind != TargetKindSavedSearch ||
			persisted.TargetID != testCase.definition.TargetID ||
			persisted.TargetVersion != testCase.definition.TargetVersion {
			t.Fatalf("persisted event = %+v, definition = %+v", persisted, testCase.definition)
		}
	}
	page, err := store.List(ctx, "tenant-saved-search", ListRequest{
		PageSize:      uint32(len(events)),
		ActionFilters: allKnownAuditActions(),
		TargetKind:    new(TargetKindSavedSearch),
	})
	if err != nil {
		t.Fatalf("List(saved-search events): %v", err)
	}
	if len(page.Events) != len(events) {
		t.Fatalf("listed event count = %d, want %d", len(page.Events), len(events))
	}

	created, err := store.Append(ctx, "tenant-saved-search-schema", SuccessfulEvent{
		OccurredAt: auditTestTime, Action: ActionSavedSearchCreate,
		TargetKind: TargetKindSavedSearch, TargetID: "saved-search-schema", TargetVersion: 1,
	})
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
		actorKind  ActorKind
		actorID    string
		actorRole  ActorRole
		action     Action
		targetKind TargetKind
		version    uint64
	}{
		{"saved-search action with token target", ActorKindSystem, defaultSystemActorID, ActorRoleSystem, ActionSavedSearchUpdate, TargetKindIngestionToken, 2},
		{"saved-search action with index target", ActorKindSystem, defaultSystemActorID, ActorRoleSystem, ActionSavedSearchUpdate, TargetKindIndex, 2},
		{"saved-search action with app target", ActorKindSystem, defaultSystemActorID, ActorRoleSystem, ActionSavedSearchUpdate, TargetKindApp, 2},
		{"administrative action with saved-search target", ActorKindSystem, defaultSystemActorID, ActorRoleSystem, ActionIndexUpdate, TargetKindSavedSearch, 2},
		{"saved-search create version two", ActorKindSystem, defaultSystemActorID, ActorRoleSystem, ActionSavedSearchCreate, TargetKindSavedSearch, 2},
		{"saved-search update version one", ActorKindSystem, defaultSystemActorID, ActorRoleSystem, ActionSavedSearchUpdate, TargetKindSavedSearch, 1},
		{"saved-search duplicate version two", ActorKindSystem, defaultSystemActorID, ActorRoleSystem, ActionSavedSearchDuplicate, TargetKindSavedSearch, 2},
		{"saved-search delete version zero", ActorKindSystem, defaultSystemActorID, ActorRoleSystem, ActionSavedSearchDelete, TargetKindSavedSearch, 0},
		{"browser user administrative action", ActorKindBrowser, "single-user", ActorRoleUser, ActionIndexUpdate, TargetKindIndex, 2},
	} {
		t.Run(forgery.name, func(t *testing.T) {
			_, insertErr := database.SQLDB().ExecContext(
				ctx,
				insertEvent,
				created.TenantID,
				created.Sequence+1,
				auditTestTime.UnixMicro(),
				forgery.actorKind,
				forgery.actorID,
				forgery.actorRole,
				forgery.action,
				forgery.targetKind,
				"saved-search-schema",
				forgery.version,
			)
			if insertErr == nil {
				t.Fatal("forged saved-search audit event succeeded")
			}
		})
	}
}

func TestSavedSearchAuditCompleteActionFilterParity(t *testing.T) {
	t.Parallel()

	actions := allKnownAuditActions()
	if len(actions) != MaximumActionFilters {
		t.Fatalf("complete action count = %d, MaximumActionFilters = %d", len(actions), MaximumActionFilters)
	}
	normalized, err := normalizeListRequest("tenant", ListRequest{ActionFilters: actions})
	if err != nil {
		t.Fatalf("normalizeListRequest(complete action taxonomy): %v", err)
	}
	if len(normalized.actionFilters) != len(actions) {
		t.Fatalf("normalized action count = %d, want %d", len(normalized.actionFilters), len(actions))
	}
}
