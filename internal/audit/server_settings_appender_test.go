package audit

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/searchlimits"
	"github.com/Suhaibinator/open-splunk/internal/uipalette"
	"gorm.io/gorm"
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

func TestServerAppearanceUpdateCommitsWithSeparateAuditTarget(t *testing.T) {
	t.Parallel()
	database := openAuditTestDatabase(t)
	auditStore := newAuditTestStore(t, database, auditTestCursorKey())
	limits, err := control.NewServerSearchSettingsStore(database, "tenant-appearance", auditStore)
	if err != nil {
		t.Fatal(err)
	}
	appearance, err := control.NewServerAppearanceSettingsStore(database, "tenant-appearance", auditStore)
	if err != nil {
		t.Fatal(err)
	}
	administratorContext, err := WithActor(context.Background(), Actor{
		Kind: ActorKindBrowser, ID: "administrator", Role: ActorRoleAdministrator,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := limits.Update(administratorContext, 0, searchlimits.Default()); err != nil {
		t.Fatal(err)
	}
	updated, err := appearance.Update(administratorContext, 0, uipalette.Ocean)
	if err != nil || updated.Version != 1 || updated.Palette != uipalette.Ocean {
		t.Fatalf("Update() = (%+v, %v)", updated, err)
	}
	if _, err := appearance.Update(administratorContext, 1, uipalette.Ember); err != nil {
		t.Fatal(err)
	}
	page, err := auditStore.List(context.Background(), "tenant-appearance", ListRequest{})
	if err != nil || len(page.Events) != 3 {
		t.Fatalf("List() = (%+v, %v)", page, err)
	}
	// Newest first: the second palette save, the first palette save, then the
	// search-limits save. Each singleton keeps its own version line.
	wantTargets := []struct {
		id      string
		version uint64
	}{{"ui-palette", 2}, {"ui-palette", 1}, {"search-limits", 1}}
	for index, event := range page.Events {
		want := wantTargets[index]
		if event.Action != ActionServerSettingsUpdate ||
			event.TargetKind != TargetKindServerSettings ||
			event.TargetID != want.id || event.TargetVersion != want.version ||
			event.Actor.ID != "administrator" {
			t.Fatalf("audit event %d = %+v, want target %+v", index, event, want)
		}
	}
	currentLimits, err := limits.Get(context.Background())
	if err != nil || currentLimits.Version != 1 {
		t.Fatalf("palette saves changed the search-limits version: (%+v, %v)", currentLimits, err)
	}
	if _, err := appearance.Update(context.Background(), 2, uipalette.Glass); err == nil {
		t.Fatal("unauthenticated appearance update succeeded")
	}
	current, err := appearance.Get(context.Background())
	if err != nil || current.Version != 2 || current.Palette != uipalette.Ember {
		t.Fatalf("failed audit changed appearance: (%+v, %v)", current, err)
	}
}

func TestServerSettingsAppenderRejectsUnknownTarget(t *testing.T) {
	t.Parallel()
	database := openAuditTestDatabase(t)
	auditStore := newAuditTestStore(t, database, auditTestCursorKey())
	administratorContext, err := WithActor(context.Background(), Actor{
		Kind: ActorKindBrowser, ID: "administrator", Role: ActorRoleAdministrator,
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, target := range []control.ServerSettingsTarget{"", "sepia", "UI-PALETTE"} {
		err := database.GORMDB().WithContext(administratorContext).Transaction(func(tx *gorm.DB) error {
			return auditStore.AppendServerSettingsMutationInTransaction(
				administratorContext, tx, "tenant-target",
				control.ServerSettingsMutationAuditEvent{
					OccurredAt: time.Date(2026, 9, 4, 12, 0, 0, 0, time.UTC), Target: target, Version: 1,
				},
			)
		})
		if !errors.Is(err, control.ErrInvalidArgument) {
			t.Fatalf("target %q error = %v, want ErrInvalidArgument", target, err)
		}
	}
	page, err := auditStore.List(context.Background(), "tenant-target", ListRequest{})
	if err != nil || len(page.Events) != 0 {
		t.Fatalf("List() after rejected targets = (%+v, %v)", page, err)
	}
}
