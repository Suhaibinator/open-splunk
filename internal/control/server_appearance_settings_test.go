package control

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/uipalette"
	"gorm.io/gorm"
)

func TestServerAppearanceSettingsDefaultPersistenceConflictAndRollback(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "control.sqlite")
	database, err := Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	var settingsStrict, settingsWithoutRowID int
	if err := database.SQLDB().QueryRowContext(ctx, `
		SELECT strict, wr FROM pragma_table_list
		WHERE schema = 'main' AND name = 'server_appearance_settings'
	`).Scan(&settingsStrict, &settingsWithoutRowID); err != nil {
		t.Fatal(err)
	}
	if settingsStrict != 1 || settingsWithoutRowID != 1 {
		t.Fatalf("server appearance settings table = strict %d without-rowid %d", settingsStrict, settingsWithoutRowID)
	}
	if _, err := database.SQLDB().ExecContext(ctx, `
		INSERT INTO server_appearance_settings (singleton_id, version, palette, updated_at_unix_micro)
		VALUES (1, 1, 'sepia', 1)
	`); err == nil {
		t.Fatal("raw INSERT of an unknown palette passed the CHECK constraint")
	}
	var events []ServerSettingsMutationAuditEvent
	appender := serverSettingsAuditAppenderFunc(func(_ context.Context, tx *gorm.DB, tenant string, event ServerSettingsMutationAuditEvent) error {
		events = append(events, event)
		if tx == nil || tenant != "tenant" || event.Version == 0 {
			return errors.New("invalid audit projection")
		}
		return nil
	})
	store, err := NewServerAppearanceSettingsStore(database, "tenant", appender)
	if err != nil {
		t.Fatal(err)
	}
	initial, err := store.Get(ctx)
	if err != nil || initial.Version != 0 || initial.Palette != uipalette.Default() || !initial.UpdatedAt.IsZero() {
		t.Fatalf("Get(default) = (%+v, %v)", initial, err)
	}
	updated, err := store.Update(ctx, 0, uipalette.Ocean)
	if err != nil || updated.Version != 1 || updated.Palette != uipalette.Ocean || updated.UpdatedAt.IsZero() {
		t.Fatalf("Update() = (%+v, %v)", updated, err)
	}
	if len(events) != 1 || events[0].Target != ServerSettingsTargetUIPalette || events[0].Version != 1 ||
		!events[0].OccurredAt.Equal(updated.UpdatedAt) {
		t.Fatalf("audit events = %+v", events)
	}
	if _, err := store.Update(ctx, 0, uipalette.Ember); !errors.Is(err, ErrVersionConflict) {
		t.Fatalf("stale Update() error = %v", err)
	}
	if _, err := store.Update(ctx, 1, uipalette.Palette("sepia")); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("invalid palette Update() error = %v", err)
	}
	if _, err := store.Update(ctx, 1, uipalette.Palette("")); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("empty palette Update() error = %v", err)
	}
	if _, err := store.Update(ctx, maximumServerSettingsVersion, uipalette.Classic); !errors.Is(err, ErrVersionConflict) {
		t.Fatalf("overflow Update() error = %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("rejected updates reached the audit appender: %+v", events)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	restarted, err := NewServerAppearanceSettingsStore(reopened, "tenant", appender)
	if err != nil {
		t.Fatal(err)
	}
	persisted, err := restarted.Get(ctx)
	if err != nil || persisted.Version != 1 || persisted.Palette != uipalette.Ocean ||
		!persisted.UpdatedAt.Equal(updated.UpdatedAt) {
		t.Fatalf("Get(restarted) = (%+v, %v)", persisted, err)
	}
	second, err := restarted.Update(ctx, 1, uipalette.Terminal)
	if err != nil || second.Version != 2 || second.Palette != uipalette.Terminal {
		t.Fatalf("second Update() = (%+v, %v)", second, err)
	}

	failing, err := NewServerAppearanceSettingsStore(reopened, "tenant", serverSettingsAuditAppenderFunc(
		func(context.Context, *gorm.DB, string, ServerSettingsMutationAuditEvent) error {
			return errors.New("audit unavailable")
		},
	))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := failing.Update(ctx, 2, uipalette.Glass); err == nil {
		t.Fatal("Update() succeeded with failed audit write")
	}
	afterFailure, err := restarted.Get(ctx)
	if err != nil || afterFailure.Version != 2 || afterFailure.Palette != uipalette.Terminal {
		t.Fatalf("failed audit changed settings: (%+v, %v)", afterFailure, err)
	}
	var searchVersion int
	if err := reopened.SQLDB().QueryRowContext(ctx, `SELECT count(*) FROM server_search_settings`).Scan(&searchVersion); err != nil {
		t.Fatal(err)
	}
	if searchVersion != 0 {
		t.Fatalf("palette saves touched server_search_settings: %d rows", searchVersion)
	}
}

func TestServerAppearanceSettingsStoreRejectsMissingDependencies(t *testing.T) {
	ctx := context.Background()
	database, err := Open(ctx, filepath.Join(t.TempDir(), "control.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	appender := serverSettingsAuditAppenderFunc(func(context.Context, *gorm.DB, string, ServerSettingsMutationAuditEvent) error {
		return nil
	})
	if _, err := NewServerAppearanceSettingsStore(nil, "tenant", appender); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("nil database error = %v", err)
	}
	if _, err := NewServerAppearanceSettingsStore(database, "", appender); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("empty tenant error = %v", err)
	}
	if _, err := NewServerAppearanceSettingsStore(database, "tenant", nil); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("nil appender error = %v", err)
	}
	var nilStore *ServerAppearanceSettingsStore
	if _, err := nilStore.Get(ctx); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("nil store Get() error = %v", err)
	}
	if _, err := nilStore.Update(ctx, 0, uipalette.Classic); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("nil store Update() error = %v", err)
	}
}

func TestServerAppearanceSettingsFromRecordRejectsCorruptRows(t *testing.T) {
	t.Parallel()
	valid := serverAppearanceSettingsRecord{SingletonID: 1, Version: 3, Palette: "graphite", UpdatedAtUnixMicro: 5}
	settings, err := serverAppearanceSettingsFromRecord(valid)
	if err != nil || settings.Version != 3 || settings.Palette != uipalette.Graphite || settings.UpdatedAt.UnixMicro() != 5 {
		t.Fatalf("serverAppearanceSettingsFromRecord(valid) = (%+v, %v)", settings, err)
	}
	for name, mutate := range map[string]func(*serverAppearanceSettingsRecord){
		"singleton": func(record *serverAppearanceSettingsRecord) { record.SingletonID = 2 },
		"version":   func(record *serverAppearanceSettingsRecord) { record.Version = 0 },
		"palette":   func(record *serverAppearanceSettingsRecord) { record.Palette = "Graphite" },
		"time zero": func(record *serverAppearanceSettingsRecord) { record.UpdatedAtUnixMicro = 0 },
		"time out of range": func(record *serverAppearanceSettingsRecord) {
			record.UpdatedAtUnixMicro = maximumControlTimestampUnixMicro + 1
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			record := valid
			mutate(&record)
			if _, err := serverAppearanceSettingsFromRecord(record); err == nil {
				t.Fatalf("serverAppearanceSettingsFromRecord(%+v) succeeded", record)
			}
		})
	}
}

func TestServerSettingsTargetValidity(t *testing.T) {
	t.Parallel()
	for _, target := range []ServerSettingsTarget{ServerSettingsTargetSearchLimits, ServerSettingsTargetUIPalette} {
		if !target.Valid() {
			t.Fatalf("%q is not valid", target)
		}
	}
	for _, target := range []ServerSettingsTarget{"", "search_limits", "palette", "UI-PALETTE"} {
		if target.Valid() {
			t.Fatalf("%q is valid", target)
		}
	}
}
