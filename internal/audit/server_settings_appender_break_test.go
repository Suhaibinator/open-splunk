package audit

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/control"
	"gorm.io/gorm"
)

// TestServerSettingsAppenderKeepsTargetsExactAndAtomic drives the appender
// directly with every near-miss spelling of a known target, proves the
// rejection rolls back a transaction that already carried a valid row, and
// then proves both known targets land with their own target_id and version.
func TestServerSettingsAppenderKeepsTargetsExactAndAtomic(t *testing.T) {
	t.Parallel()
	database := openAuditTestDatabase(t)
	auditStore := newAuditTestStore(t, database, auditTestCursorKey())
	administratorContext, err := WithActor(context.Background(), Actor{
		Kind: ActorKindBrowser, ID: "administrator", Role: ActorRoleAdministrator,
	})
	if err != nil {
		t.Fatal(err)
	}
	const tenant = "tenant-exact-targets"
	occurredAt := time.Date(2026, 9, 5, 8, 0, 0, 0, time.UTC)
	appendTarget := func(tx *gorm.DB, target control.ServerSettingsTarget, version uint64) error {
		return auditStore.AppendServerSettingsMutationInTransaction(
			administratorContext, tx, tenant,
			control.ServerSettingsMutationAuditEvent{OccurredAt: occurredAt, Target: target, Version: version},
		)
	}

	for _, target := range []control.ServerSettingsTarget{
		"", " ", "ui-palette ", " ui-palette", "ui-palette\n", "ui_palette", "UI-PALETTE", "Ui-Palette",
		"search-limits\x00", "search_limits", "Search-Limits", "palette", "appearance", "server-settings",
	} {
		err := database.GORMDB().WithContext(administratorContext).Transaction(func(tx *gorm.DB) error {
			// A valid row first: the unknown target must sink the whole transaction.
			if err := appendTarget(tx, control.ServerSettingsTargetSearchLimits, 1); err != nil {
				return err
			}
			return appendTarget(tx, target, 1)
		})
		if !errors.Is(err, control.ErrInvalidArgument) {
			t.Fatalf("target %q error = %v, want ErrInvalidArgument", target, err)
		}
	}
	page, err := auditStore.List(context.Background(), tenant, ListRequest{})
	if err != nil || len(page.Events) != 0 {
		t.Fatalf("List() after rejected targets = (%+v, %v), want an empty ledger", page, err)
	}

	err = database.GORMDB().WithContext(administratorContext).Transaction(func(tx *gorm.DB) error {
		if err := appendTarget(tx, control.ServerSettingsTargetSearchLimits, 4); err != nil {
			return err
		}
		return appendTarget(tx, control.ServerSettingsTargetUIPalette, 2)
	})
	if err != nil {
		t.Fatal(err)
	}
	page, err = auditStore.List(context.Background(), tenant, ListRequest{})
	if err != nil || len(page.Events) != 2 {
		t.Fatalf("List() = (%+v, %v), want two events", page, err)
	}
	seen := map[string]uint64{}
	for _, event := range page.Events {
		if event.Action != ActionServerSettingsUpdate || event.TargetKind != TargetKindServerSettings ||
			event.Actor.ID != "administrator" || !event.OccurredAt.Equal(occurredAt) {
			t.Fatalf("event = %+v", event)
		}
		seen[event.TargetID] = event.TargetVersion
	}
	if seen["search-limits"] != 4 || seen["ui-palette"] != 2 || len(seen) != 2 {
		t.Fatalf("target ids and versions = %v, want search-limits:4 ui-palette:2", seen)
	}

	// A valid target without an explicit administrative actor is refused
	// before any row is written; the ledger stays at two.
	err = database.GORMDB().Transaction(func(tx *gorm.DB) error {
		return auditStore.AppendServerSettingsMutationInTransaction(
			context.Background(), tx, tenant,
			control.ServerSettingsMutationAuditEvent{
				OccurredAt: occurredAt, Target: control.ServerSettingsTargetUIPalette, Version: 3,
			},
		)
	})
	if !errors.Is(err, control.ErrInvalidArgument) {
		t.Fatalf("actor-less append error = %v, want ErrInvalidArgument", err)
	}
	userContext, err := WithActor(context.Background(), Actor{Kind: ActorKindBrowser, ID: "user", Role: ActorRoleUser})
	if err != nil {
		t.Fatal(err)
	}
	err = database.GORMDB().WithContext(userContext).Transaction(func(tx *gorm.DB) error {
		return auditStore.AppendServerSettingsMutationInTransaction(
			userContext, tx, tenant,
			control.ServerSettingsMutationAuditEvent{
				OccurredAt: occurredAt, Target: control.ServerSettingsTargetUIPalette, Version: 3,
			},
		)
	})
	if !errors.Is(err, control.ErrInvalidArgument) {
		t.Fatalf("ordinary-user append error = %v, want ErrInvalidArgument", err)
	}
	page, err = auditStore.List(context.Background(), tenant, ListRequest{})
	if err != nil || len(page.Events) != 2 {
		t.Fatalf("List() after actor rejections = (%+v, %v), want two events", page, err)
	}
}
