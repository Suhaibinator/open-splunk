package main

import (
	"bytes"
	"context"
	"errors"
	"path/filepath"
	"sync"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/audit"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/uipalette"
)

// TestRuntimeAppearanceSerializesWritersAndPublishesOnlyCommittedPalettes
// drives the runtime object bootstrap reads with a real control database
// and real audit ledger: concurrent saves against one expected version admit
// exactly one writer, the live snapshot only ever shows a committed palette,
// and a save whose audit row is refused leaves the snapshot untouched.
func TestRuntimeAppearanceSerializesWritersAndPublishesOnlyCommittedPalettes(t *testing.T) {
	ctx := context.Background()
	database, err := control.Open(ctx, filepath.Join(t.TempDir(), "control.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if closeErr := database.Close(); closeErr != nil {
			t.Error(closeErr)
		}
	})
	auditStore, err := audit.NewStore(database, audit.StoreOptions{CursorKey: bytes.Repeat([]byte("a"), 32)})
	if err != nil {
		t.Fatal(err)
	}
	const tenant = "tenant-appearance-runtime"
	appearanceStore, err := control.NewServerAppearanceSettingsStore(database, tenant, auditStore)
	if err != nil {
		t.Fatal(err)
	}
	initial, err := appearanceStore.Get(ctx)
	if err != nil {
		t.Fatal(err)
	}
	settings := &runtimeServerSettings{appearanceStore: appearanceStore, appearance: initial}
	administratorContext, err := audit.WithActor(ctx, audit.Actor{
		Kind: audit.ActorKindBrowser, ID: "administrator", Role: audit.ActorRoleAdministrator,
	})
	if err != nil {
		t.Fatal(err)
	}

	if got := settings.CurrentAppearance(); got.Version != 0 || got.Palette != uipalette.Classic {
		t.Fatalf("initial snapshot = %+v", got)
	}
	var nilContext context.Context
	if _, err := settings.GetAppearance(nilContext); !errors.Is(err, control.ErrInvalidArgument) {
		t.Fatalf("GetAppearance(nil) error = %v, want ErrInvalidArgument", err)
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := settings.GetAppearance(canceled); !errors.Is(err, context.Canceled) {
		t.Fatalf("GetAppearance(canceled) error = %v, want context.Canceled", err)
	}

	// Without an administrative actor the audit append fails inside the
	// transaction: nothing is persisted and nothing is published.
	if _, err := settings.UpdateAppearance(ctx, 0, uipalette.Ocean); err == nil {
		t.Fatal("actor-less UpdateAppearance succeeded")
	}
	if got := settings.CurrentAppearance(); got.Version != 0 || got.Palette != uipalette.Classic {
		t.Fatalf("snapshot after refused audit = %+v, want untouched default", got)
	}
	if persisted, err := appearanceStore.Get(ctx); err != nil || persisted.Version != 0 {
		t.Fatalf("store after refused audit = (%+v, %v)", persisted, err)
	}
	for _, invalid := range []uipalette.Palette{"", "sepia", "Ocean", "ocean "} {
		if _, err := settings.UpdateAppearance(administratorContext, 0, invalid); !errors.Is(err, control.ErrInvalidArgument) {
			t.Fatalf("UpdateAppearance(%q) error = %v, want ErrInvalidArgument", invalid, err)
		}
	}
	if page, err := auditStore.List(ctx, tenant, audit.ListRequest{}); err != nil || len(page.Events) != 0 {
		t.Fatalf("audit ledger after rejected saves = (%+v, %v), want empty", page, err)
	}

	palettes := uipalette.All()
	results := make([]error, len(palettes))
	snapshots := make([]control.ServerAppearanceSettings, len(palettes))
	start := make(chan struct{})
	var wg sync.WaitGroup
	for index, palette := range palettes {
		wg.Go(func() {
			<-start
			snapshots[index], results[index] = settings.UpdateAppearance(administratorContext, 0, palette)
		})
	}
	close(start)
	wg.Wait()
	winners, conflicts := 0, 0
	var winner uipalette.Palette
	for index, err := range results {
		switch {
		case err == nil:
			winners++
			winner = palettes[index]
			if snapshots[index].Version != 1 || snapshots[index].Palette != winner {
				t.Fatalf("winner snapshot = %+v", snapshots[index])
			}
		case errors.Is(err, control.ErrVersionConflict):
			conflicts++
		default:
			t.Fatalf("writer %q error = %v, want ErrVersionConflict", palettes[index], err)
		}
	}
	if winners != 1 || conflicts != len(palettes)-1 {
		t.Fatalf("winners = %d, conflicts = %d", winners, conflicts)
	}
	live := settings.CurrentAppearance()
	if live.Version != 1 || live.Palette != winner {
		t.Fatalf("live snapshot = %+v, want version 1 palette %q", live, winner)
	}
	persisted, err := appearanceStore.Get(ctx)
	if err != nil || persisted != live {
		t.Fatalf("persisted = (%+v, %v), live = %+v", persisted, err, live)
	}
	fromGet, err := settings.GetAppearance(ctx)
	if err != nil || fromGet != live {
		t.Fatalf("GetAppearance() = (%+v, %v), want %+v", fromGet, err, live)
	}
	page, err := auditStore.List(ctx, tenant, audit.ListRequest{})
	if err != nil || len(page.Events) != 1 {
		t.Fatalf("audit ledger after the race = (%+v, %v), want exactly one row", page, err)
	}
	if event := page.Events[0]; event.TargetID != "ui-palette" || event.TargetVersion != 1 ||
		event.Action != audit.ActionServerSettingsUpdate || event.TargetKind != audit.TargetKindServerSettings ||
		event.Actor.ID != "administrator" {
		t.Fatalf("audit row = %+v", event)
	}

	// A stale writer after the race is a conflict and the snapshot holds.
	if _, err := settings.UpdateAppearance(administratorContext, 0, uipalette.Classic); !errors.Is(err, control.ErrVersionConflict) {
		t.Fatalf("stale UpdateAppearance error = %v, want ErrVersionConflict", err)
	}
	if got := settings.CurrentAppearance(); got != live {
		t.Fatalf("snapshot after stale write = %+v, want %+v", got, live)
	}
	// The next honest write advances the line and the snapshot together.
	next := uipalette.Classic
	if winner == uipalette.Classic {
		next = uipalette.Ocean
	}
	second, err := settings.UpdateAppearance(administratorContext, 1, next)
	if err != nil || second.Version != 2 || second.Palette != next {
		t.Fatalf("second UpdateAppearance = (%+v, %v)", second, err)
	}
	if got := settings.CurrentAppearance(); got != second {
		t.Fatalf("snapshot after second write = %+v, want %+v", got, second)
	}
	if page, err := auditStore.List(ctx, tenant, audit.ListRequest{}); err != nil || len(page.Events) != 2 {
		t.Fatalf("audit ledger after second write = (%+v, %v), want two rows", page, err)
	}
}
