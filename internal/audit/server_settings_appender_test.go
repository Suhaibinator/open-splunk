package audit

import (
	"context"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/searchlimits"
)

func TestServerSettingsUpdateCommitsWithRealAuditEvent(t *testing.T) {
	t.Parallel()
	database := openAuditTestDatabase(t)
	auditStore := newAuditTestStore(t, database, auditTestCursorKey())
	settings, err := control.NewServerSearchSettingsStore(database, "tenant-settings", auditStore)
	if err != nil {
		t.Fatal(err)
	}
	administratorContext, err := WithActor(context.Background(), Actor{
		Kind: ActorKindBrowser, ID: "administrator", Role: ActorRoleAdministrator,
	})
	if err != nil {
		t.Fatal(err)
	}
	updated, err := settings.Update(administratorContext, 0, searchlimits.Default())
	if err != nil || updated.Version != 1 {
		t.Fatalf("Update() = (%+v, %v)", updated, err)
	}
	page, err := auditStore.List(context.Background(), "tenant-settings", ListRequest{})
	if err != nil || len(page.Events) != 1 {
		t.Fatalf("List() = (%+v, %v)", page, err)
	}
	event := page.Events[0]
	if event.Action != ActionServerSettingsUpdate ||
		event.TargetKind != TargetKindServerSettings ||
		event.TargetID != "search-limits" || event.TargetVersion != 1 ||
		event.Actor.ID != "administrator" {
		t.Fatalf("audit event = %+v", event)
	}
	if _, err := settings.Update(context.Background(), 1, searchlimits.Default()); err == nil {
		t.Fatal("unauthenticated settings update succeeded")
	}
	current, err := settings.Get(context.Background())
	if err != nil || current.Version != 1 {
		t.Fatalf("failed audit changed settings: (%+v, %v)", current, err)
	}
}
