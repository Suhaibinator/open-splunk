package audit

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/testsupport"
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
	_, database := openAuditTestDatabase(t)
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
		TargetKind:    targetKindPointer(TargetKindKnowledgeObject),
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

func TestKnowledgeAuditTaxonomyMigrationPreservesRowsAndSchema(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	raw := openKnowledgeAuditMigrationDB(t, "upgrade.sqlite")
	if err := control.ApplyMigrations(
		ctx,
		raw,
		testsupport.SQLiteMigrationsBefore(t, "0026_"),
	); err != nil {
		t.Fatalf("apply pre-knowledge-audit migrations: %v", err)
	}
	insertLegacyAuditFixture(t, raw, "tenant-upgrade")

	if err := control.ApplyMigrations(
		ctx,
		raw,
		testsupport.SQLiteMigrationsBefore(t, "0027_"),
	); err != nil {
		t.Fatalf("apply knowledge-audit migration: %v", err)
	}

	var action, targetKind, targetID string
	var sequence, targetVersion int64
	var legacyMetadataNull int
	if err := raw.QueryRowContext(ctx, `
		SELECT sequence, action, target_kind, target_id, target_version,
		       app_id IS NULL AND object_type IS NULL AND sharing_scope IS NULL
		FROM audit_events
		WHERE tenant_id = 'tenant-upgrade'`).Scan(
		&sequence, &action, &targetKind, &targetID, &targetVersion,
		&legacyMetadataNull,
	); err != nil {
		t.Fatalf("read preserved legacy event: %v", err)
	}
	if sequence != 1 || action != "saved_search.create" ||
		targetKind != "saved_search" || targetID != "saved-search-a" ||
		targetVersion != 1 || legacyMetadataNull != 1 {
		t.Fatalf("preserved legacy event = %d/%q/%q/%q/%d", sequence, action, targetKind, targetID, targetVersion)
	}
	assertAuditTenantAccounting(t, raw, "tenant-upgrade", 2, 1)

	if _, err := raw.ExecContext(ctx, `
		INSERT INTO audit_events (
			tenant_id, sequence, occurred_at_unix_micro,
			actor_kind, actor_id, actor_role, action,
			target_kind, target_id, target_version,
			app_id, object_type, sharing_scope
		) VALUES (
			'tenant-upgrade', 2, 2,
			'browser', 'administrator', 'administrator',
			'knowledge.object.update', 'knowledge_object', 'ko-a', 2,
			'app_AAAAAAAAAAAAAAAAAAAAAA', 'field_extraction', 'app'
		)`); err != nil {
		t.Fatalf("append post-upgrade knowledge event: %v", err)
	}
	assertAuditTenantAccounting(t, raw, "tenant-upgrade", 3, 2)
	var appID, objectType, sharingScope string
	if err := raw.QueryRowContext(ctx, `
		SELECT app_id, object_type, sharing_scope
		FROM audit_events
		WHERE tenant_id = 'tenant-upgrade' AND sequence = 2
	`).Scan(&appID, &objectType, &sharingScope); err != nil {
		t.Fatalf("read knowledge metadata after upgrade: %v", err)
	}
	if appID != "app_AAAAAAAAAAAAAAAAAAAAAA" ||
		objectType != "field_extraction" || sharingScope != "app" {
		t.Fatalf("knowledge metadata = %q/%q/%q", appID, objectType, sharingScope)
	}

	for _, statement := range []string{
		`UPDATE audit_events SET target_id = 'forged' WHERE tenant_id = 'tenant-upgrade' AND sequence = 1`,
		`DELETE FROM audit_events WHERE tenant_id = 'tenant-upgrade' AND sequence = 1`,
	} {
		if _, err := raw.ExecContext(ctx, statement); err == nil {
			t.Fatalf("immutable audit statement succeeded: %s", statement)
		}
	}

	var withoutRowID, strict int
	if err := raw.QueryRowContext(ctx, `
		SELECT wr, strict FROM pragma_table_list WHERE name = 'audit_events'
	`).Scan(&withoutRowID, &strict); err != nil {
		t.Fatalf("read rebuilt table flags: %v", err)
	}
	if withoutRowID != 1 || strict != 1 {
		t.Fatalf("audit_events flags = WITHOUT ROWID %d STRICT %d", withoutRowID, strict)
	}

	wantIndexes := []string{
		"audit_events_tenant_action_sequence_idx",
		"audit_events_tenant_actor_sequence_idx",
		"audit_events_tenant_target_sequence_idx",
	}
	if got := auditSchemaNames(t, raw, "index", "audit_events_%"); !slices.Equal(got, wantIndexes) {
		t.Fatalf("audit event indexes = %v, want %v", got, wantIndexes)
	}
	wantTriggers := []string{
		"audit_event_advances_tenant_state",
		"audit_event_delete_is_forbidden",
		"audit_event_identity_collision_is_forbidden",
		"audit_event_insert_requires_current_tenant_state",
		"audit_event_update_is_forbidden",
	}
	if got := auditSchemaNames(t, raw, "trigger", "audit_event_%"); !slices.Equal(got, wantTriggers) {
		t.Fatalf("audit event triggers = %v, want %v", got, wantTriggers)
	}
	if legacy := auditSchemaNames(t, raw, "table", "audit_events_before_%"); len(legacy) != 0 {
		t.Fatalf("legacy audit tables remain after migration: %v", legacy)
	}
	var foreignKeyViolations int
	if err := raw.QueryRowContext(ctx, `SELECT count(*) FROM pragma_foreign_key_check`).Scan(
		&foreignKeyViolations,
	); err != nil {
		t.Fatalf("check upgraded audit foreign keys: %v", err)
	}
	if foreignKeyViolations != 0 {
		t.Fatalf("upgraded audit foreign-key violations = %d", foreignKeyViolations)
	}
}

func TestKnowledgeAuditTaxonomyMigrationRollsBackFailedCopy(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	raw := openKnowledgeAuditMigrationDB(t, "rollback.sqlite")
	if err := control.ApplyMigrations(
		ctx,
		raw,
		testsupport.SQLiteMigrationsBefore(t, "0026_"),
	); err != nil {
		t.Fatalf("apply pre-knowledge-audit migrations: %v", err)
	}
	insertLegacyAuditFixture(t, raw, "tenant-rollback")
	if _, err := raw.ExecContext(ctx, `PRAGMA ignore_check_constraints = ON`); err != nil {
		t.Fatalf("enable legacy corruption fixture: %v", err)
	}
	if _, err := raw.ExecContext(ctx, `
		INSERT INTO audit_events (
			tenant_id, sequence, occurred_at_unix_micro,
			actor_kind, actor_id, actor_role, action,
			target_kind, target_id, target_version
		) VALUES (
			'tenant-rollback', 2, 2,
			'system', 'open-splunk-server', 'system',
			'future.action', 'future_target', 'future-id', 2
		)`); err != nil {
		t.Fatalf("insert corrupt legacy event: %v", err)
	}
	if _, err := raw.ExecContext(ctx, `PRAGMA ignore_check_constraints = OFF`); err != nil {
		t.Fatalf("restore CHECK enforcement: %v", err)
	}

	err := control.ApplyMigrations(
		ctx,
		raw,
		testsupport.SQLiteMigrationsBefore(t, "0027_"),
	)
	if err == nil || !strings.Contains(err.Error(), "0026_knowledge_audit_taxonomy.sql") {
		t.Fatalf("migration error = %v, want 0026 copy failure", err)
	}

	var rowCount int
	if err := raw.QueryRowContext(ctx, `SELECT count(*) FROM audit_events`).Scan(&rowCount); err != nil {
		t.Fatalf("read rolled-back audit table: %v", err)
	}
	if rowCount != 2 {
		t.Fatalf("rolled-back audit row count = %d, want 2", rowCount)
	}
	assertAuditTenantAccounting(t, raw, "tenant-rollback", 3, 2)
	if legacy := auditSchemaNames(t, raw, "table", "audit_events_before_%"); len(legacy) != 0 {
		t.Fatalf("legacy table escaped failed migration transaction: %v", legacy)
	}
	if got := auditSchemaNames(t, raw, "trigger", "audit_event_%"); len(got) != 5 {
		t.Fatalf("rolled-back audit trigger count = %d, want 5: %v", len(got), got)
	}
	var applied int
	if err := raw.QueryRowContext(ctx, `
		SELECT count(*) FROM schema_migrations WHERE version = 26
	`).Scan(&applied); err != nil {
		t.Fatalf("read migration ledger: %v", err)
	}
	if applied != 0 {
		t.Fatalf("failed migration ledger rows = %d, want 0", applied)
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
			_, database := openAuditTestDatabase(t)
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
	_, database := openAuditTestDatabase(t)
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

func openKnowledgeAuditMigrationDB(t *testing.T, name string) *sql.DB {
	t.Helper()
	raw, err := sql.Open(
		"sqlite",
		filepath.Join(t.TempDir(), name)+"?_txlock=immediate&_pragma=foreign_keys(1)",
	)
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	raw.SetMaxOpenConns(1)
	t.Cleanup(func() {
		if closeErr := raw.Close(); closeErr != nil {
			t.Errorf("close migration database: %v", closeErr)
		}
	})
	return raw
}

func insertLegacyAuditFixture(t *testing.T, raw *sql.DB, tenantID string) {
	t.Helper()
	if _, err := raw.ExecContext(context.Background(), `
		INSERT INTO audit_tenant_state (tenant_id, next_sequence, event_count)
		VALUES (?, 1, 0);
		INSERT INTO audit_events (
			tenant_id, sequence, occurred_at_unix_micro,
			actor_kind, actor_id, actor_role, action,
			target_kind, target_id, target_version
		) VALUES (
			?, 1, 1,
			'browser', 'ordinary-user', 'user',
			'saved_search.create', 'saved_search', 'saved-search-a', 1
		)`, tenantID, tenantID); err != nil {
		t.Fatalf("insert legacy audit fixture: %v", err)
	}
	assertAuditTenantAccounting(t, raw, tenantID, 2, 1)
}

func assertAuditTenantAccounting(
	t *testing.T,
	raw *sql.DB,
	tenantID string,
	wantNextSequence int64,
	wantEventCount int64,
) {
	t.Helper()
	var nextSequence, eventCount int64
	if err := raw.QueryRowContext(context.Background(), `
		SELECT next_sequence, event_count
		FROM audit_tenant_state
		WHERE tenant_id = ?`, tenantID).Scan(&nextSequence, &eventCount); err != nil {
		t.Fatalf("read audit tenant accounting: %v", err)
	}
	if nextSequence != wantNextSequence || eventCount != wantEventCount {
		t.Fatalf(
			"audit tenant accounting = next %d/count %d, want next %d/count %d",
			nextSequence, eventCount, wantNextSequence, wantEventCount,
		)
	}
}

func auditSchemaNames(t *testing.T, raw *sql.DB, schemaType, pattern string) []string {
	t.Helper()
	rows, err := raw.QueryContext(context.Background(), `
		SELECT name
		FROM sqlite_schema
		WHERE type = ? AND name LIKE ?
		ORDER BY name`, schemaType, pattern)
	if err != nil {
		t.Fatalf("read %s schema names: %v", schemaType, err)
	}
	defer rows.Close()
	var names []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			t.Fatalf("scan %s schema name: %v", schemaType, err)
		}
		names = append(names, name)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate %s schema names: %v", schemaType, err)
	}
	return names
}

func knowledgeAuditTestMetadata() KnowledgeObjectMetadata {
	return KnowledgeObjectMetadata{
		AppID:        "app_AAAAAAAAAAAAAAAAAAAAAA",
		ObjectType:   KnowledgeObjectTypeFieldExtraction,
		SharingScope: KnowledgeSharingScopeApp,
	}
}
