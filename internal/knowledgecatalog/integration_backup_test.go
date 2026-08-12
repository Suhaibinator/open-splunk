package knowledgecatalog

import (
	"context"
	"errors"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/audit"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/knowledgeattemptaudit"
)

func TestIntegrationBackupPreservesCommittedCatalogAndRejectedReadAuditSnapshot(t *testing.T) {
	database, store := newCatalogTestStore(t)
	oldDescription := "committed before backup"
	insertFixtureObject(t, database, fixtureObject{
		id:    "ko-backup-atomic",
		owner: testOwner,
		versions: []fixtureVersion{{
			definition: aliasDefinition(testApp, "backup-alpha", SharingScopePrivate, &oldDescription, "backup-old-*"),
			state:      StateActive,
			mutation:   "create",
			timestamp:  10,
		}},
	})
	insertFixtureObject(t, database, fixtureObject{
		id:    "ko-backup-zulu",
		owner: testOwner,
		versions: []fixtureVersion{{
			definition: aliasDefinition(testApp, "backup-zulu", SharingScopePrivate, nil, "backup-zulu-*"),
			state:      StateActive,
			mutation:   "create",
			timestamp:  11,
		}},
	})

	attempts, err := knowledgeattemptaudit.New(database)
	if err != nil {
		t.Fatalf("knowledgeattemptaudit.New(source): %v", err)
	}
	actorContext, err := audit.WithActor(context.Background(), audit.Actor{
		Kind: audit.ActorKindBrowser,
		ID:   "backup-administrator",
		Role: audit.ActorRoleAdministrator,
	})
	if err != nil {
		t.Fatalf("audit.WithActor: %v", err)
	}
	readContext := &knowledgeattemptaudit.AuthorizedContext{
		AppID: testApp,
		Object: &knowledgeattemptaudit.AuthorizedObject{
			KnowledgeObjectID: "ko-backup-atomic",
			ObjectType:        knowledgeattemptaudit.ObjectTypeFieldAlias,
			Version:           1,
			SharingScope:      knowledgeattemptaudit.SharingScopePrivate,
		},
	}
	if err := attempts.AppendRejected(actorContext, testTenant, knowledgeattemptaudit.Definition{
		OccurredAt:        time.UnixMicro(50).UTC(),
		Action:            knowledgeattemptaudit.ActionGet,
		Reason:            knowledgeattemptaudit.ReasonServiceUnavailable,
		AuthorizedContext: readContext,
	}); err != nil {
		t.Fatalf("AppendRejected(get): %v", err)
	}
	if err := attempts.AppendRejected(actorContext, testTenant, knowledgeattemptaudit.Definition{
		OccurredAt: time.UnixMicro(51).UTC(),
		Action:     knowledgeattemptaudit.ActionList,
		Reason:     knowledgeattemptaudit.ReasonResourceLimit,
	}); err != nil {
		t.Fatalf("AppendRejected(list): %v", err)
	}

	request := ListRequest{PageSize: 1, IncludeTotal: true}
	baseline, err := store.List(context.Background(), testReadScope(), request)
	if err != nil {
		t.Fatalf("List(source baseline): %v", err)
	}
	if !slices.Equal(names(baseline.Objects), []string{"backup-alpha"}) ||
		baseline.NextPageToken == "" || baseline.CatalogRevision == 0 {
		t.Fatalf("source baseline = %#v", baseline)
	}
	continuation := request
	continuation.PageToken = baseline.NextPageToken

	newDescription := "staged but absent from backup"
	staged, _ := stageIntegrationKnownPublication(
		t,
		database,
		"ko-backup-atomic",
		aliasDefinition(testApp, "backup-charlie", SharingScopePrivate, &newDescription, "backup-new-*"),
		StateActive,
		"update",
		20,
	)
	committedDefinitionAuthorities := readBackupDefinitionAuthorities(t, database)
	if len(committedDefinitionAuthorities) != 2 {
		t.Fatalf("committed definition authorities = %d, want 2", len(committedDefinitionAuthorities))
	}

	backupPath := filepath.Join(t.TempDir(), "catalog-backup.sqlite")
	backupContext, cancelBackup := context.WithTimeout(context.Background(), 10*time.Second)
	err = database.BackupTo(backupContext, backupPath)
	cancelBackup()
	if err != nil {
		t.Fatalf("BackupTo(with uncommitted publication): %v", err)
	}
	if err := staged.Commit(); err != nil {
		t.Fatalf("commit source publication after backup: %v", err)
	}

	if _, err := store.List(context.Background(), testReadScope(), continuation); !errors.Is(err, control.ErrPageInvalidated) {
		t.Fatalf("List(source with pre-backup cursor) error = %v, want ErrPageInvalidated", err)
	}
	sourceCurrent, err := store.Get(context.Background(), testReadScope(), "ko-backup-atomic", nil)
	if err != nil || sourceCurrent.Version != 2 || sourceCurrent.Name != "backup-charlie" {
		t.Fatalf("Get(source after commit) = %#v, %v", sourceCurrent, err)
	}

	restored, err := control.OpenReadOnly(context.Background(), backupPath)
	if err != nil {
		t.Fatalf("control.OpenReadOnly(backup): %v", err)
	}
	t.Cleanup(func() {
		if err := restored.Close(); err != nil {
			t.Errorf("close restored database: %v", err)
		}
	})
	restoredStore, err := New(restored, Options{CursorKey: testCursorKey})
	if err != nil {
		t.Fatalf("knowledgecatalog.New(backup): %v", err)
	}
	if _, err := knowledgeattemptaudit.NewWithContext(context.Background(), restored); err != nil {
		t.Fatalf("knowledgeattemptaudit.NewWithContext(backup): %v", err)
	}
	assertBackupDefinitionAuthoritiesEqual(
		t,
		committedDefinitionAuthorities,
		readBackupDefinitionAuthorities(t, restored),
	)

	restoredCurrent, err := restoredStore.Get(context.Background(), testReadScope(), "ko-backup-atomic", nil)
	if err != nil || restoredCurrent.Version != 1 || restoredCurrent.Name != "backup-alpha" ||
		restoredCurrent.Definition.GetDescription() != oldDescription {
		t.Fatalf("Get(restored committed snapshot) = %#v, %v", restoredCurrent, err)
	}
	restoredSecond, err := restoredStore.List(context.Background(), testReadScope(), continuation)
	if err != nil {
		t.Fatalf("List(restored with pre-backup cursor): %v", err)
	}
	if !slices.Equal(names(restoredSecond.Objects), []string{"backup-zulu"}) ||
		restoredSecond.CatalogRevision != baseline.CatalogRevision || restoredSecond.NextPageToken != "" {
		t.Fatalf("restored continuation = %#v", restoredSecond)
	}

	var integrity string
	if err := restored.SQLDB().QueryRowContext(context.Background(), `PRAGMA integrity_check`).Scan(&integrity); err != nil {
		t.Fatalf("restored integrity_check: %v", err)
	}
	if integrity != "ok" {
		t.Fatalf("restored integrity_check = %q", integrity)
	}
	foreignKeys, err := restored.SQLDB().QueryContext(context.Background(), `PRAGMA foreign_key_check`)
	if err != nil {
		t.Fatalf("restored foreign_key_check: %v", err)
	}
	if foreignKeys.Next() {
		_ = foreignKeys.Close()
		t.Fatal("restored database has a foreign-key violation")
	}
	if err := foreignKeys.Close(); err != nil {
		t.Fatalf("close foreign_key_check: %v", err)
	}

	rows, err := restored.SQLDB().QueryContext(context.Background(), `SELECT sequence, action, reason,
		coalesce(knowledge_object_id, '') FROM knowledge_attempt_audit_events
		WHERE tenant_id = ? ORDER BY sequence`, testTenant)
	if err != nil {
		t.Fatalf("read restored rejected attempts: %v", err)
	}
	defer rows.Close()
	type restoredAttempt struct {
		sequence int64
		action   string
		reason   string
		objectID string
	}
	var gotAttempts []restoredAttempt
	for rows.Next() {
		var event restoredAttempt
		if err := rows.Scan(&event.sequence, &event.action, &event.reason, &event.objectID); err != nil {
			t.Fatalf("scan restored rejected attempt: %v", err)
		}
		gotAttempts = append(gotAttempts, event)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate restored rejected attempts: %v", err)
	}
	wantAttempts := []restoredAttempt{
		{1, string(knowledgeattemptaudit.ActionGet), string(knowledgeattemptaudit.ReasonServiceUnavailable), "ko-backup-atomic"},
		{2, string(knowledgeattemptaudit.ActionList), string(knowledgeattemptaudit.ReasonResourceLimit), ""},
	}
	if !slices.Equal(gotAttempts, wantAttempts) {
		t.Fatalf("restored rejected attempts = %#v, want %#v", gotAttempts, wantAttempts)
	}
}

type backupDefinitionAuthority struct {
	objectID        string
	version         int64
	versionDigest   []byte
	blobDigest      []byte
	definitionProto []byte
	definitionBytes int64
}

func readBackupDefinitionAuthorities(t *testing.T, database *control.DB) []backupDefinitionAuthority {
	t.Helper()
	rows, err := database.SQLDB().QueryContext(t.Context(), `
		SELECT version.knowledge_object_id, version.object_version,
		       version.definition_digest, blob.definition_digest,
		       blob.definition_proto, blob.definition_bytes
		FROM knowledge_object_versions AS version
		JOIN knowledge_definition_blobs AS blob
		  ON blob.tenant_id = version.tenant_id
		 AND blob.definition_digest = version.definition_digest
		WHERE version.tenant_id = ?
		ORDER BY version.knowledge_object_id, version.object_version`, testTenant)
	if err != nil {
		t.Fatalf("read backup definition authorities: %v", err)
	}
	defer rows.Close()

	var authorities []backupDefinitionAuthority
	for rows.Next() {
		var authority backupDefinitionAuthority
		if err := rows.Scan(
			&authority.objectID,
			&authority.version,
			&authority.versionDigest,
			&authority.blobDigest,
			&authority.definitionProto,
			&authority.definitionBytes,
		); err != nil {
			t.Fatalf("scan backup definition authority: %v", err)
		}
		authority.versionDigest = slices.Clone(authority.versionDigest)
		authority.blobDigest = slices.Clone(authority.blobDigest)
		authority.definitionProto = slices.Clone(authority.definitionProto)
		if !slices.Equal(authority.versionDigest, authority.blobDigest) {
			t.Fatalf(
				"definition digest authority differs for %q v%d: version=%x blob=%x",
				authority.objectID,
				authority.version,
				authority.versionDigest,
				authority.blobDigest,
			)
		}
		if authority.definitionBytes != int64(len(authority.definitionProto)) {
			t.Fatalf(
				"definition byte authority differs for %q v%d: declared=%d actual=%d",
				authority.objectID,
				authority.version,
				authority.definitionBytes,
				len(authority.definitionProto),
			)
		}
		authorities = append(authorities, authority)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate backup definition authorities: %v", err)
	}
	return authorities
}

func assertBackupDefinitionAuthoritiesEqual(
	t *testing.T,
	want, got []backupDefinitionAuthority,
) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("restored definition authority count = %d, want %d", len(got), len(want))
	}
	for index := range want {
		wantAuthority := want[index]
		gotAuthority := got[index]
		if gotAuthority.objectID != wantAuthority.objectID ||
			gotAuthority.version != wantAuthority.version ||
			gotAuthority.definitionBytes != wantAuthority.definitionBytes ||
			!slices.Equal(gotAuthority.versionDigest, wantAuthority.versionDigest) ||
			!slices.Equal(gotAuthority.blobDigest, wantAuthority.blobDigest) ||
			!slices.Equal(gotAuthority.definitionProto, wantAuthority.definitionProto) {
			t.Fatalf(
				"restored definition authority[%d] = %#v, want exact raw authority %#v",
				index,
				gotAuthority,
				wantAuthority,
			)
		}
	}
}
