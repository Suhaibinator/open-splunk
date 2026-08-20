package audit

import (
	"context"
	"errors"
	"strings"
	"testing"
)

func TestKnowledgeAuditTaxonomyValidatesActionTargetVersionAndActor(t *testing.T) {
	t.Parallel()

	valid := []struct {
		action  Action
		version uint64
	}{
		{ActionKnowledgeObjectCreate, 1},
		{ActionKnowledgeObjectUpdate, 2},
		{ActionKnowledgeObjectScopeChange, 2},
		{ActionKnowledgeObjectEnable, 2},
		{ActionKnowledgeObjectDisable, 2},
		{ActionKnowledgeObjectDelete, 2},
	}
	for _, testCase := range valid {
		t.Run(string(testCase.action), func(t *testing.T) {
			t.Parallel()
			event := SuccessfulEvent{
				OccurredAt:      auditTestTime,
				Action:          testCase.action,
				TargetKind:      TargetKindKnowledgeObject,
				TargetID:        "ko_AAAAAAAAAAAAAAAAAAAAAA",
				TargetVersion:   testCase.version,
				KnowledgeObject: knowledgeAuditTestMetadata(),
			}
			if !event.valid() {
				t.Fatalf("valid knowledge-object event rejected: %+v", event)
			}
			event.TargetKind = TargetKindSavedSearch
			if event.valid() {
				t.Fatalf("cross-family action/target accepted: %+v", event)
			}
		})
	}

	for _, testCase := range []struct {
		action  Action
		version uint64
	}{
		{ActionKnowledgeObjectCreate, 0},
		{ActionKnowledgeObjectCreate, 2},
		{ActionKnowledgeObjectUpdate, 1},
		{ActionKnowledgeObjectScopeChange, 1},
		{ActionKnowledgeObjectEnable, 1},
		{ActionKnowledgeObjectDisable, 1},
		{ActionKnowledgeObjectDelete, 1},
	} {
		event := SuccessfulEvent{
			OccurredAt:      auditTestTime,
			Action:          testCase.action,
			TargetKind:      TargetKindKnowledgeObject,
			TargetID:        "ko_AAAAAAAAAAAAAAAAAAAAAA",
			TargetVersion:   testCase.version,
			KnowledgeObject: knowledgeAuditTestMetadata(),
		}
		if event.valid() {
			t.Fatalf("invalid knowledge-object version accepted: %+v", event)
		}
	}

	ordinaryUser := Event{
		Sequence:   1,
		TenantID:   "tenant",
		OccurredAt: auditTestTime.Round(0).UTC().Truncate(1_000),
		Actor: Actor{
			Kind: ActorKindBrowser,
			ID:   "ordinary-user",
			Role: ActorRoleUser,
		},
		Action:          ActionKnowledgeObjectUpdate,
		TargetKind:      TargetKindKnowledgeObject,
		TargetID:        "ko_AAAAAAAAAAAAAAAAAAAAAA",
		TargetVersion:   2,
		KnowledgeObject: knowledgeAuditTestMetadata(),
	}
	if err := ordinaryUser.ValidateForTenant("tenant"); err == nil {
		t.Fatal("ordinary browser user knowledge-object audit event was accepted")
	}

	validKnowledge := SuccessfulEvent{
		OccurredAt:      auditTestTime,
		Action:          ActionKnowledgeObjectUpdate,
		TargetKind:      TargetKindKnowledgeObject,
		TargetID:        "ko_AAAAAAAAAAAAAAAAAAAAAA",
		TargetVersion:   2,
		KnowledgeObject: knowledgeAuditTestMetadata(),
	}
	for _, metadata := range []KnowledgeObjectMetadata{
		{},
		{AppID: "app", ObjectType: KnowledgeObjectType("future"), SharingScope: KnowledgeSharingScopeApp},
		{AppID: "app", ObjectType: KnowledgeObjectTypeFieldAlias, SharingScope: KnowledgeSharingScope("future")},
		{AppID: strings.Repeat("a", maximumKnowledgeAppIDBytes+1), ObjectType: KnowledgeObjectTypeFieldAlias, SharingScope: KnowledgeSharingScopePrivate},
	} {
		candidate := validKnowledge
		candidate.KnowledgeObject = metadata
		if candidate.valid() {
			t.Fatalf("invalid knowledge metadata accepted: %+v", metadata)
		}
	}
	legacyWithMetadata := validKnowledge
	legacyWithMetadata.Action = ActionSavedSearchUpdate
	legacyWithMetadata.TargetKind = TargetKindSavedSearch
	if legacyWithMetadata.valid() {
		t.Fatal("legacy audit target accepted knowledge-only metadata")
	}
}

func TestKnowledgeAuditEventsPersistAndSQLiteRejectsTaxonomyForgeries(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	database := openAuditTestDatabase(t)
	store := newAuditTestStore(t, database, auditTestCursorKey())
	events := []SuccessfulEvent{
		{OccurredAt: auditTestTime, Action: ActionKnowledgeObjectCreate, TargetKind: TargetKindKnowledgeObject, TargetID: "ko-a", TargetVersion: 1, KnowledgeObject: knowledgeAuditTestMetadata()},
		{OccurredAt: auditTestTime, Action: ActionKnowledgeObjectUpdate, TargetKind: TargetKindKnowledgeObject, TargetID: "ko-a", TargetVersion: 2, KnowledgeObject: knowledgeAuditTestMetadata()},
		{OccurredAt: auditTestTime, Action: ActionKnowledgeObjectScopeChange, TargetKind: TargetKindKnowledgeObject, TargetID: "ko-a", TargetVersion: 3, KnowledgeObject: knowledgeAuditTestMetadata()},
		{OccurredAt: auditTestTime, Action: ActionKnowledgeObjectEnable, TargetKind: TargetKindKnowledgeObject, TargetID: "ko-a", TargetVersion: 4, KnowledgeObject: knowledgeAuditTestMetadata()},
		{OccurredAt: auditTestTime, Action: ActionKnowledgeObjectDisable, TargetKind: TargetKindKnowledgeObject, TargetID: "ko-a", TargetVersion: 5, KnowledgeObject: knowledgeAuditTestMetadata()},
		{OccurredAt: auditTestTime, Action: ActionKnowledgeObjectDelete, TargetKind: TargetKindKnowledgeObject, TargetID: "ko-a", TargetVersion: 6, KnowledgeObject: knowledgeAuditTestMetadata()},
	}
	for index, definition := range events {
		persisted, err := store.Append(ctx, "tenant-knowledge", definition)
		if err != nil {
			t.Fatalf("Append(%s): %v", definition.Action, err)
		}
		if persisted.Sequence != uint64(index+1) ||
			persisted.Action != definition.Action ||
			persisted.TargetKind != TargetKindKnowledgeObject ||
			persisted.TargetVersion != definition.TargetVersion ||
			persisted.KnowledgeObject != definition.KnowledgeObject {
			t.Fatalf("persisted event = %+v, definition = %+v", persisted, definition)
		}
	}
	page, err := store.List(ctx, "tenant-knowledge", ListRequest{
		PageSize:      uint32(len(events)),
		ActionFilters: allKnownAuditActions(),
		TargetKind:    new(TargetKindKnowledgeObject),
	})
	if err != nil {
		t.Fatalf("List(knowledge-object events): %v", err)
	}
	if len(page.Events) != len(events) {
		t.Fatalf("listed event count = %d, want %d", len(page.Events), len(events))
	}
	for _, event := range page.Events {
		if event.KnowledgeObject != knowledgeAuditTestMetadata() {
			t.Fatalf("listed knowledge metadata = %+v", event.KnowledgeObject)
		}
	}

	created, err := store.Append(ctx, "tenant-knowledge-schema", events[0])
	if err != nil {
		t.Fatalf("Append(schema anchor): %v", err)
	}
	insertEvent := `INSERT INTO audit_events (
		tenant_id, sequence, occurred_at_unix_micro,
		actor_kind, actor_id, actor_role, action,
		target_kind, target_id, target_version,
		app_id, object_type, sharing_scope
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
	for _, forgery := range []struct {
		name       string
		action     Action
		targetKind TargetKind
		version    uint64
	}{
		{"knowledge action with saved-search target", ActionKnowledgeObjectUpdate, TargetKindSavedSearch, 2},
		{"saved-search action with knowledge target", ActionSavedSearchUpdate, TargetKindKnowledgeObject, 2},
		{"knowledge create version two", ActionKnowledgeObjectCreate, TargetKindKnowledgeObject, 2},
		{"knowledge update version one", ActionKnowledgeObjectUpdate, TargetKindKnowledgeObject, 1},
		{"knowledge scope-change version one", ActionKnowledgeObjectScopeChange, TargetKindKnowledgeObject, 1},
		{"knowledge enable version one", ActionKnowledgeObjectEnable, TargetKindKnowledgeObject, 1},
		{"knowledge disable version one", ActionKnowledgeObjectDisable, TargetKindKnowledgeObject, 1},
		{"knowledge delete version one", ActionKnowledgeObjectDelete, TargetKindKnowledgeObject, 1},
	} {
		t.Run(forgery.name, func(t *testing.T) {
			_, insertErr := database.SQLDB().ExecContext(
				ctx,
				insertEvent,
				created.TenantID,
				created.Sequence+1,
				auditTestTime.UnixMicro(),
				"system",
				defaultSystemActorID,
				"system",
				forgery.action,
				forgery.targetKind,
				"ko-forged",
				forgery.version,
				"app_AAAAAAAAAAAAAAAAAAAAAA",
				"field_extraction",
				"app",
			)
			if insertErr == nil {
				t.Fatal("forged knowledge-object audit event succeeded")
			}
		})
	}
	for _, forgery := range []string{
		`INSERT INTO audit_events (
			tenant_id, sequence, occurred_at_unix_micro,
			actor_kind, actor_id, actor_role, action,
			target_kind, target_id, target_version
		) VALUES (
			'tenant-knowledge-schema', 2, 2,
			'system', 'open-splunk-server', 'system',
			'knowledge.object.update', 'knowledge_object', 'ko-forged', 2
		)`,
		`INSERT INTO audit_events (
			tenant_id, sequence, occurred_at_unix_micro,
			actor_kind, actor_id, actor_role, action,
			target_kind, target_id, target_version,
			app_id, object_type, sharing_scope
		) VALUES (
			'tenant-knowledge-schema', 2, 2,
			'system', 'open-splunk-server', 'system',
			'knowledge.object.update', 'knowledge_object', 'ko-forged', 2,
			'app_AAAAAAAAAAAAAAAAAAAAAA', 'future_type', 'app'
		)`,
		`INSERT INTO audit_events (
			tenant_id, sequence, occurred_at_unix_micro,
			actor_kind, actor_id, actor_role, action,
			target_kind, target_id, target_version,
			app_id, object_type, sharing_scope
		) VALUES (
			'tenant-knowledge-schema', 2, 2,
			'system', 'open-splunk-server', 'system',
			'saved_search.update', 'saved_search', 'saved-search', 2,
			'app_AAAAAAAAAAAAAAAAAAAAAA', 'field_alias', 'private'
		)`,
		`INSERT INTO audit_events (
			tenant_id, sequence, occurred_at_unix_micro,
			actor_kind, actor_id, actor_role, action,
			target_kind, target_id, target_version,
			app_id, object_type, sharing_scope
		) VALUES (
			'tenant-knowledge-schema', 2, 2,
			'system', 'open-splunk-server', 'system',
			'knowledge.object.update', 'knowledge_object', 'ko-forged', 2,
			'app_AAAAAAAAAAAAAAAAAAAAAA', 'field_alias', 'future_scope'
		)`,
		`INSERT INTO audit_events (
			tenant_id, sequence, occurred_at_unix_micro,
			actor_kind, actor_id, actor_role, action,
			target_kind, target_id, target_version,
			app_id, object_type, sharing_scope
		) VALUES (
			'tenant-knowledge-schema', 2, 2,
			'system', 'open-splunk-server', 'system',
			'knowledge.object.update', 'knowledge_object', 'ko-forged', 2,
			replace(hex(zeroblob(129)), '00', 'a'), 'field_alias', 'private'
		)`,
	} {
		if _, err := database.SQLDB().ExecContext(ctx, forgery); err == nil {
			t.Fatalf("forged knowledge metadata succeeded: %s", forgery)
		}
	}
}

func TestKnowledgeAuditMetadataCorruptionFailsStartupListAndContinuation(t *testing.T) {
	t.Parallel()

	for _, testCase := range []struct {
		name       string
		assignment string
	}{
		{"app ID", "app_id = replace(hex(zeroblob(129)), '00', 'a')"},
		{"object type", "object_type = replace(hex(zeroblob(17)), '00', 'a')"},
		{"sharing scope", "sharing_scope = replace(hex(zeroblob(8)), '00', 'a')"},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			database := openAuditTestDatabase(t)
			store := newAuditTestStore(t, database, auditTestCursorKey())
			for _, definition := range []SuccessfulEvent{
				{
					OccurredAt: auditTestTime, Action: ActionKnowledgeObjectCreate,
					TargetKind: TargetKindKnowledgeObject, TargetID: "ko-corrupt",
					TargetVersion: 1, KnowledgeObject: knowledgeAuditTestMetadata(),
				},
				{
					OccurredAt: auditTestTime, Action: ActionKnowledgeObjectUpdate,
					TargetKind: TargetKindKnowledgeObject, TargetID: "ko-corrupt",
					TargetVersion: 2, KnowledgeObject: knowledgeAuditTestMetadata(),
				},
			} {
				if _, err := store.Append(ctx, "tenant-metadata-corrupt", definition); err != nil {
					t.Fatalf("append knowledge audit fixture: %v", err)
				}
			}
			first, err := store.List(ctx, "tenant-metadata-corrupt", ListRequest{PageSize: 1})
			if err != nil || first.NextPageToken == "" {
				t.Fatalf("List(first) = (%+v, %v)", first, err)
			}

			database.SQLDB().SetMaxOpenConns(1)
			connection, err := database.SQLDB().Conn(ctx)
			if err != nil {
				t.Fatalf("acquire corruption connection: %v", err)
			}
			if _, err := connection.ExecContext(ctx, "DROP TRIGGER audit_event_update_is_forbidden"); err != nil {
				_ = connection.Close()
				t.Fatalf("drop audit update trigger: %v", err)
			}
			if _, err := connection.ExecContext(ctx, "PRAGMA ignore_check_constraints = ON"); err != nil {
				_ = connection.Close()
				t.Fatalf("ignore fixture constraints: %v", err)
			}
			// #nosec G202 -- assignment is selected from the fixed corruption-test table above.
			if _, err := connection.ExecContext(ctx, `
				UPDATE audit_events
				SET `+testCase.assignment+`
				WHERE tenant_id = 'tenant-metadata-corrupt' AND sequence = 2`); err != nil {
				_ = connection.Close()
				t.Fatalf("corrupt knowledge metadata: %v", err)
			}
			if _, err := connection.ExecContext(ctx, "PRAGMA ignore_check_constraints = OFF"); err != nil {
				_ = connection.Close()
				t.Fatalf("restore fixture constraints: %v", err)
			}
			if err := connection.Close(); err != nil {
				t.Fatalf("close corruption connection: %v", err)
			}

			if page, err := store.List(ctx, "tenant-metadata-corrupt", ListRequest{}); len(page.Events) != 0 || !errors.Is(err, ErrCorrupt) {
				t.Fatalf("List(corrupt metadata) = (%+v, %v)", page, err)
			}
			if page, err := store.List(ctx, "tenant-metadata-corrupt", ListRequest{
				PageSize: 1, PageToken: first.NextPageToken,
			}); len(page.Events) != 0 ||
				(!errors.Is(err, ErrCorrupt) && !errors.Is(err, ErrInvalidCursor)) {
				t.Fatalf("List(corrupt continuation) = (%+v, %v)", page, err)
			}
			if candidate, err := NewStore(database, StoreOptions{CursorKey: auditTestCursorKey()}); candidate != nil || !errors.Is(err, ErrCorrupt) {
				t.Fatalf("NewStore(corrupt metadata) = (%v, %v)", candidate, err)
			}
		})
	}
}

func TestAuditEventDigestBindsKnowledgeMetadataPresenceAndValues(t *testing.T) {
	t.Parallel()

	appID := "app-a"
	objectType := KnowledgeObjectTypeFieldExtraction
	sharingScope := KnowledgeSharingScopePrivate
	record := auditEventRecord{
		TenantID: "tenant", Sequence: 1, OccurredAtUnixMicro: 1,
		ActorKind: ActorKindSystem, ActorID: defaultSystemActorID,
		ActorRole: ActorRoleSystem, Action: ActionKnowledgeObjectCreate,
		TargetKind: TargetKindKnowledgeObject, TargetID: "ko-a", TargetVersion: 1,
		AppID: &appID, ObjectType: &objectType, SharingScope: &sharingScope,
	}
	want, err := auditEventDigest(record)
	if err != nil {
		t.Fatalf("auditEventDigest: %v", err)
	}
	mutations := []func(*auditEventRecord){
		func(candidate *auditEventRecord) { candidate.AppID = nil },
		func(candidate *auditEventRecord) { value := "app-b"; candidate.AppID = &value },
		func(candidate *auditEventRecord) {
			value := KnowledgeObjectTypeFieldAlias
			candidate.ObjectType = &value
		},
		func(candidate *auditEventRecord) {
			value := KnowledgeSharingScopeGlobal
			candidate.SharingScope = &value
		},
	}
	for index, mutate := range mutations {
		candidate := record
		mutate(&candidate)
		got, digestErr := auditEventDigest(candidate)
		if digestErr != nil {
			t.Fatalf("auditEventDigest(mutation %d): %v", index, digestErr)
		}
		if got == want {
			t.Fatalf("knowledge metadata mutation %d did not change digest", index)
		}
	}
}

func TestKnowledgeAuditContinuationRejectsValidMetadataRewrite(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	database := openAuditTestDatabase(t)
	store := newAuditTestStore(t, database, auditTestCursorKey())
	for _, definition := range []SuccessfulEvent{
		{
			OccurredAt: auditTestTime, Action: ActionKnowledgeObjectCreate,
			TargetKind: TargetKindKnowledgeObject, TargetID: "ko-cursor",
			TargetVersion: 1, KnowledgeObject: knowledgeAuditTestMetadata(),
		},
		{
			OccurredAt: auditTestTime, Action: ActionKnowledgeObjectUpdate,
			TargetKind: TargetKindKnowledgeObject, TargetID: "ko-cursor",
			TargetVersion: 2, KnowledgeObject: knowledgeAuditTestMetadata(),
		},
	} {
		if _, err := store.Append(ctx, "tenant-metadata-cursor", definition); err != nil {
			t.Fatalf("append knowledge cursor fixture: %v", err)
		}
	}
	first, err := store.List(ctx, "tenant-metadata-cursor", ListRequest{PageSize: 1})
	if err != nil || first.NextPageToken == "" {
		t.Fatalf("List(first) = (%+v, %v)", first, err)
	}
	if _, err := database.SQLDB().ExecContext(ctx, `
		DROP TRIGGER audit_event_update_is_forbidden;
		UPDATE audit_events
		SET app_id = 'app_BBBBBBBBBBBBBBBBBBBBBB'
		WHERE tenant_id = 'tenant-metadata-cursor' AND sequence = 2
	`); err != nil {
		t.Fatalf("rewrite valid knowledge metadata: %v", err)
	}
	page, err := store.List(ctx, "tenant-metadata-cursor", ListRequest{
		PageSize: 1, PageToken: first.NextPageToken,
	})
	if len(page.Events) != 0 || !errors.Is(err, ErrInvalidCursor) {
		t.Fatalf("List(rewritten high-water) = (%+v, %v)", page, err)
	}
}

func knowledgeAuditTestMetadata() KnowledgeObjectMetadata {
	return KnowledgeObjectMetadata{
		AppID:        "app_AAAAAAAAAAAAAAAAAAAAAA",
		ObjectType:   KnowledgeObjectTypeFieldExtraction,
		SharingScope: KnowledgeSharingScopeApp,
	}
}
