package audit

import (
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/pgmeta"
)

func TestDelegatedIndexManagementPreservesBrowserUserAuditRole(t *testing.T) {
	database := openAuditTestDatabase(t)
	store := newAuditTestStore(t, database, auditTestCursorKey())
	ctx := actorContext(t, Actor{Kind: ActorKindBrowser, ID: "91", Role: ActorRoleUser})
	tx := pgmeta.Begin(database.GORMDB().WithContext(ctx))
	if tx.Error != nil {
		t.Fatal(tx.Error)
	}
	defer tx.Rollback()
	for _, action := range []control.IndexMutationAuditAction{control.IndexMutationAuditActionUpdate, control.IndexMutationAuditActionActivate, control.IndexMutationAuditActionArchive, control.IndexMutationAuditActionDeleteKeepData, control.IndexMutationAuditActionDeleteData} {
		version := uint64(2)
		if action == control.IndexMutationAuditActionDeleteData {
			version = 3
		}
		if err := store.AppendIndexMutationInTransaction(ctx, tx, "delegated-index", control.IndexMutationAuditEvent{OccurredAt: auditTestTime, Action: action, IndexID: "index", IndexVersion: version}); err != nil {
			t.Fatalf("delegated %s: %v", action, err)
		}
	}
	if err := tx.Commit().Error; err != nil {
		t.Fatal(err)
	}
	page, err := store.List(ctx, "delegated-index", ListRequest{})
	if err != nil || len(page.Events) != 5 {
		t.Fatalf("delegated audit: %+v %v", page, err)
	}
	for _, event := range page.Events {
		if event.Actor.Role != ActorRoleUser || event.Actor.ID != "91" {
			t.Fatalf("delegated role relabeled: %+v", event.Actor)
		}
	}
}
