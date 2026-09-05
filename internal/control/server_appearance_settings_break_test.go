package control

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/uipalette"
	"gorm.io/gorm"
)

// openAppearanceBreakDatabase opens a fresh control database for one test and
// closes it on cleanup so the SQLite handle never outlives the test.
func openAppearanceBreakDatabase(t *testing.T) *DB {
	t.Helper()
	database, err := Open(context.Background(), filepath.Join(t.TempDir(), "control.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if closeErr := database.Close(); closeErr != nil {
			t.Error(closeErr)
		}
	})
	return database
}

// TestServerAppearanceSettingsConcurrentUpdatesAdmitExactlyOneWriter races
// every palette against the same expected version. The immediate
// transaction lock serializes the writers, so exactly one commits, every
// loser sees ErrVersionConflict (not a driver "database is locked" error),
// and the audit appender observes exactly one event inside the winning
// transaction with the new row already visible.
func TestServerAppearanceSettingsConcurrentUpdatesAdmitExactlyOneWriter(t *testing.T) {
	ctx := context.Background()
	database := openAppearanceBreakDatabase(t)
	var mu sync.Mutex
	var events []ServerSettingsMutationAuditEvent
	appender := serverSettingsAuditAppenderFunc(func(
		_ context.Context, tx *gorm.DB, tenant string, event ServerSettingsMutationAuditEvent,
	) error {
		var visible int64
		if err := tx.Raw(
			`SELECT count(*) FROM server_appearance_settings WHERE singleton_id = 1 AND version = ?`,
			event.Version,
		).Scan(&visible).Error; err != nil {
			return err
		}
		if visible != 1 || tenant != "tenant" {
			return fmt.Errorf("audit ran outside the writing transaction: visible %d tenant %q", visible, tenant)
		}
		mu.Lock()
		events = append(events, event)
		mu.Unlock()
		return nil
	})
	store, err := NewServerAppearanceSettingsStore(database, "tenant", appender)
	if err != nil {
		t.Fatal(err)
	}

	race := func(expectedVersion uint64) uipalette.Palette {
		t.Helper()
		palettes := uipalette.All()
		type outcome struct {
			settings ServerAppearanceSettings
			err      error
		}
		outcomes := make([]outcome, len(palettes))
		start := make(chan struct{})
		var wg sync.WaitGroup
		for index, palette := range palettes {
			wg.Go(func() {
				<-start
				settings, err := store.Update(ctx, expectedVersion, palette)
				outcomes[index] = outcome{settings: settings, err: err}
			})
		}
		close(start)
		wg.Wait()
		var winner uipalette.Palette
		winners, conflicts := 0, 0
		for index, result := range outcomes {
			switch {
			case result.err == nil:
				winners++
				winner = result.settings.Palette
				if result.settings.Version != expectedVersion+1 || result.settings.Palette != palettes[index] {
					t.Fatalf("winner %q = %+v", palettes[index], result.settings)
				}
			case errors.Is(result.err, ErrVersionConflict):
				conflicts++
				if result.settings != (ServerAppearanceSettings{}) {
					t.Fatalf("loser %q returned settings %+v", palettes[index], result.settings)
				}
			default:
				t.Fatalf("writer %q failed with %v, want ErrVersionConflict", palettes[index], result.err)
			}
		}
		if winners != 1 || conflicts != len(palettes)-1 {
			t.Fatalf("winners = %d, conflicts = %d, want 1 and %d", winners, conflicts, len(palettes)-1)
		}
		return winner
	}

	first := race(0)
	mu.Lock()
	if len(events) != 1 || events[0].Target != ServerSettingsTargetUIPalette || events[0].Version != 1 {
		t.Fatalf("audit events after first race = %+v", events)
	}
	mu.Unlock()
	stored, err := store.Get(ctx)
	if err != nil || stored.Version != 1 || stored.Palette != first {
		t.Fatalf("Get() after first race = (%+v, %v), want version 1 palette %q", stored, err, first)
	}
	var rows int
	if err := database.SQLDB().QueryRowContext(ctx, `SELECT count(*) FROM server_appearance_settings`).Scan(&rows); err != nil {
		t.Fatal(err)
	}
	if rows != 1 {
		t.Fatalf("server_appearance_settings rows = %d, want 1", rows)
	}

	// Losers retrying with the version they were told about must fail again
	// until they reload: a stale retry cannot overwrite the winner.
	if _, err := store.Update(ctx, 0, uipalette.Classic); !errors.Is(err, ErrVersionConflict) {
		t.Fatalf("stale retry error = %v, want ErrVersionConflict", err)
	}
	if _, err := store.Update(ctx, 2, uipalette.Classic); !errors.Is(err, ErrVersionConflict) {
		t.Fatalf("future-version update error = %v, want ErrVersionConflict", err)
	}
	stored, err = store.Get(ctx)
	if err != nil || stored.Version != 1 || stored.Palette != first {
		t.Fatalf("Get() after stale retries = (%+v, %v)", stored, err)
	}

	second := race(1)
	mu.Lock()
	if len(events) != 2 || events[1].Version != 2 || events[1].Target != ServerSettingsTargetUIPalette {
		t.Fatalf("audit events after second race = %+v", events)
	}
	mu.Unlock()
	stored, err = store.Get(ctx)
	if err != nil || stored.Version != 2 || stored.Palette != second {
		t.Fatalf("Get() after second race = (%+v, %v)", stored, err)
	}
}

// TestServerAppearanceSettingsSchemaRejectsEveryOutOfContractRow drives the
// CHECK and STRICT constraints directly, bypassing the store, so a future
// writer cannot persist a row Get would refuse.
func TestServerAppearanceSettingsSchemaRejectsEveryOutOfContractRow(t *testing.T) {
	ctx := context.Background()
	database := openAppearanceBreakDatabase(t)
	const insert = `
		INSERT INTO server_appearance_settings (singleton_id, version, palette, updated_at_unix_micro)
		VALUES (?, ?, ?, ?)
	`
	for _, row := range []struct {
		name      string
		singleton any
		version   any
		palette   any
		updatedAt any
	}{
		{name: "version zero", singleton: 1, version: 0, palette: "classic", updatedAt: 1},
		{name: "version negative", singleton: 1, version: -1, palette: "classic", updatedAt: 1},
		{name: "unknown palette", singleton: 1, version: 1, palette: "sepia", updatedAt: 1},
		{name: "capitalised palette", singleton: 1, version: 1, palette: "Classic", updatedAt: 1},
		{name: "padded palette", singleton: 1, version: 1, palette: "ocean ", updatedAt: 1},
		{name: "upper-case palette", singleton: 1, version: 1, palette: "OCEAN", updatedAt: 1},
		{name: "empty palette", singleton: 1, version: 1, palette: "", updatedAt: 1},
		{name: "null palette", singleton: 1, version: 1, palette: nil, updatedAt: 1},
		{name: "palette as integer", singleton: 1, version: 1, palette: 2, updatedAt: 1},
		{name: "second singleton", singleton: 2, version: 1, palette: "classic", updatedAt: 1},
		{name: "zero singleton", singleton: 0, version: 1, palette: "classic", updatedAt: 1},
		{name: "timestamp zero", singleton: 1, version: 1, palette: "classic", updatedAt: 0},
		{name: "timestamp negative", singleton: 1, version: 1, palette: "classic", updatedAt: -1},
		{
			name: "timestamp past year 9999", singleton: 1, version: 1, palette: "classic",
			updatedAt: maximumControlTimestampUnixMicro + 1,
		},
		{name: "timestamp as real", singleton: 1, version: 1, palette: "classic", updatedAt: 1.5},
	} {
		t.Run(row.name, func(t *testing.T) {
			if _, err := database.SQLDB().ExecContext(
				ctx, insert, row.singleton, row.version, row.palette, row.updatedAt,
			); err == nil {
				t.Fatalf("raw INSERT %+v passed the table constraints", row)
			}
		})
	}
	var rows int
	if err := database.SQLDB().QueryRowContext(ctx, `SELECT count(*) FROM server_appearance_settings`).Scan(&rows); err != nil {
		t.Fatal(err)
	}
	if rows != 0 {
		t.Fatalf("rejected inserts left %d rows", rows)
	}
	// The boundary values themselves are accepted, so the CHECKs are exact.
	if _, err := database.SQLDB().ExecContext(
		ctx, insert, 1, 9223372036854775807, "terminal", maximumControlTimestampUnixMicro,
	); err != nil {
		t.Fatalf("boundary INSERT rejected: %v", err)
	}
	appender := serverSettingsAuditAppenderFunc(func(context.Context, *gorm.DB, string, ServerSettingsMutationAuditEvent) error {
		return nil
	})
	store, err := NewServerAppearanceSettingsStore(database, "tenant", appender)
	if err != nil {
		t.Fatal(err)
	}
	stored, err := store.Get(ctx)
	if err != nil || stored.Version != 9223372036854775807 || stored.Palette != uipalette.Terminal ||
		stored.UpdatedAt.UnixMicro() != maximumControlTimestampUnixMicro {
		t.Fatalf("Get(boundary row) = (%+v, %v)", stored, err)
	}
	// The version line cannot be advanced past the signed range: the store
	// reports a conflict rather than wrapping or tripping the CHECK.
	if _, err := store.Update(ctx, 9223372036854775807, uipalette.Classic); !errors.Is(err, ErrVersionConflict) {
		t.Fatalf("Update(maximum version) error = %v, want ErrVersionConflict", err)
	}
}

// TestServerAppearanceSettingsGetRefusesCorruptRowsInsteadOfDefaulting
// plants rows the CHECK constraints would normally refuse (via
// ignore_check_constraints on a dedicated connection) and proves Get
// surfaces corruption instead of silently painting classic, which would hide
// a damaged control database from the operator.
func TestServerAppearanceSettingsGetRefusesCorruptRowsInsteadOfDefaulting(t *testing.T) {
	ctx := context.Background()
	for _, corrupt := range []struct {
		name      string
		version   int64
		palette   string
		updatedAt int64
	}{
		{name: "timestamp zero", version: 1, palette: "ocean", updatedAt: 0},
		{name: "timestamp negative", version: 3, palette: "glass", updatedAt: -5},
		{name: "timestamp past year 9999", version: 2, palette: "ember", updatedAt: maximumControlTimestampUnixMicro + 1},
		{name: "version zero", version: 0, palette: "terminal", updatedAt: 10},
		{name: "unknown palette", version: 1, palette: "sepia", updatedAt: 10},
	} {
		t.Run(corrupt.name, func(t *testing.T) {
			database := openAppearanceBreakDatabase(t)
			connection, err := database.SQLDB().Conn(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := connection.ExecContext(ctx, `PRAGMA ignore_check_constraints = ON`); err != nil {
				t.Fatal(err)
			}
			if _, err := connection.ExecContext(ctx, `
				INSERT INTO server_appearance_settings (singleton_id, version, palette, updated_at_unix_micro)
				VALUES (1, ?, ?, ?)
			`, corrupt.version, corrupt.palette, corrupt.updatedAt); err != nil {
				t.Fatalf("plant corrupt row: %v", err)
			}
			if _, err := connection.ExecContext(ctx, `PRAGMA ignore_check_constraints = OFF`); err != nil {
				t.Fatal(err)
			}
			if err := connection.Close(); err != nil {
				t.Fatal(err)
			}
			var calls int
			appender := serverSettingsAuditAppenderFunc(func(context.Context, *gorm.DB, string, ServerSettingsMutationAuditEvent) error {
				calls++
				return nil
			})
			store, err := NewServerAppearanceSettingsStore(database, "tenant", appender)
			if err != nil {
				t.Fatal(err)
			}
			settings, err := store.Get(ctx)
			if err == nil {
				t.Fatalf("Get() returned %+v for a corrupt row", settings)
			}
			if !strings.Contains(err.Error(), "corrupt") {
				t.Fatalf("Get() error = %v, want a corruption error", err)
			}
			if errors.Is(err, gorm.ErrRecordNotFound) {
				t.Fatalf("Get() reported not-found for a present row: %v", err)
			}
			if settings != (ServerAppearanceSettings{}) {
				t.Fatalf("Get() leaked a partial value alongside the error: %+v", settings)
			}
			// A stale writer that believes the table is empty must not be able
			// to overwrite the damaged row and hide the evidence.
			if _, err := store.Update(ctx, 0, uipalette.Classic); !errors.Is(err, ErrVersionConflict) {
				t.Fatalf("Update(0) over corrupt row error = %v, want ErrVersionConflict", err)
			}
			if calls != 0 {
				t.Fatalf("rejected update reached the audit appender %d times", calls)
			}
		})
	}
}

// TestServerAppearanceSettingsUpdateRejectsAnUnusableClock pins the clock
// guard: a store whose clock reports epoch zero or a time past the
// persisted range never writes and never audits.
func TestServerAppearanceSettingsUpdateRejectsAnUnusableClock(t *testing.T) {
	ctx := context.Background()
	database := openAppearanceBreakDatabase(t)
	var calls int
	appender := serverSettingsAuditAppenderFunc(func(context.Context, *gorm.DB, string, ServerSettingsMutationAuditEvent) error {
		calls++
		return nil
	})
	store, err := NewServerAppearanceSettingsStore(database, "tenant", appender)
	if err != nil {
		t.Fatal(err)
	}
	for name, clock := range map[string]func() time.Time{
		"epoch":        func() time.Time { return time.Unix(0, 0) },
		"before epoch": func() time.Time { return time.Unix(-1, 0) },
		"past the range": func() time.Time {
			return time.UnixMicro(maximumControlTimestampUnixMicro).Add(time.Microsecond)
		},
	} {
		store.now = clock
		if _, err := store.Update(ctx, 0, uipalette.Ocean); !errors.Is(err, ErrInvalidArgument) {
			t.Fatalf("%s clock Update() error = %v, want ErrInvalidArgument", name, err)
		}
	}
	if calls != 0 {
		t.Fatalf("clock-rejected updates reached the audit appender %d times", calls)
	}
	stored, err := store.Get(ctx)
	if err != nil || stored.Version != 0 || stored.Palette != uipalette.Default() {
		t.Fatalf("Get() after clock rejections = (%+v, %v)", stored, err)
	}
	// The last representable microsecond is accepted, and sub-microsecond
	// precision is truncated so the persisted and returned times agree.
	store.now = func() time.Time { return time.UnixMicro(maximumControlTimestampUnixMicro).Add(999 * time.Nanosecond) }
	updated, err := store.Update(ctx, 0, uipalette.Ocean)
	if err != nil || updated.UpdatedAt.UnixMicro() != maximumControlTimestampUnixMicro {
		t.Fatalf("Update(maximum clock) = (%+v, %v)", updated, err)
	}
	stored, err = store.Get(ctx)
	if err != nil || !stored.UpdatedAt.Equal(updated.UpdatedAt) || stored.UpdatedAt.Location() != time.UTC {
		t.Fatalf("Get() after maximum clock = (%+v, %v), want %v in UTC", stored, err, updated.UpdatedAt)
	}
}
