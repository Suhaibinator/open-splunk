//go:build !windows

package integration_test

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/audit"
	"github.com/Suhaibinator/open-splunk/internal/auth"
	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/savedobjects"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
	"github.com/Suhaibinator/open-splunk/internal/searchlimits"
	"github.com/Suhaibinator/open-splunk/internal/server"
	"github.com/Suhaibinator/open-splunk/internal/uipalette"
)

// paletteSmokeFlag gates the instance-palette browser smoke. It needs the
// pinned Chromium build and a frontend build, but no Docker, ClickHouse, or
// compiled server binary: the real browser API handler, SQLite control plane,
// audit journal, and static export run in this process on a loopback port.
const (
	paletteSmokeFlag   = "OPEN_SPLUNK_PALETTE_SMOKE"
	paletteSmokeOwner  = "palette-smoke-owner"
	paletteSmokeTenant = "palette-smoke-tenant"
)

// paletteSmokeServerSettings is the in-process twin of the server command's
// runtimeServerSettings (cmd/open-splunk-server/server_settings_runtime.go),
// which lives in package main and cannot be imported. Both stores are the
// real control-plane singletons over the real SQLite file and the real audit
// appender; only the wiring is restated here, with the same mutex discipline:
// write durably first, publish the snapshot bootstrap serves second.
type paletteSmokeServerSettings struct {
	mu              sync.RWMutex
	store           *control.ServerSearchSettingsStore
	source          *searchlimits.Source
	jobs            *searchjobs.Manager
	current         control.ServerSearchSettings
	appearanceStore *control.ServerAppearanceSettingsStore
	appearance      control.ServerAppearanceSettings
}

func (settings *paletteSmokeServerSettings) Get(ctx context.Context) (control.ServerSearchSettings, error) {
	if err := ctx.Err(); err != nil {
		return control.ServerSearchSettings{}, err
	}
	return settings.Current(), nil
}

func (settings *paletteSmokeServerSettings) Current() control.ServerSearchSettings {
	settings.mu.RLock()
	defer settings.mu.RUnlock()
	return settings.current
}

func (settings *paletteSmokeServerSettings) Update(
	ctx context.Context,
	expectedVersion uint64,
	limits searchlimits.Policy,
) (control.ServerSearchSettings, error) {
	settings.mu.Lock()
	defer settings.mu.Unlock()
	if err := searchlimits.Validate(limits); err != nil {
		return control.ServerSearchSettings{}, err
	}
	updated, err := settings.store.Update(ctx, expectedVersion, limits)
	if err != nil {
		return control.ServerSearchSettings{}, err
	}
	if err := settings.source.Store(limits); err != nil {
		return control.ServerSearchSettings{}, err
	}
	settings.current = updated
	settings.jobs.LimitsChanged()
	return updated, nil
}

func (settings *paletteSmokeServerSettings) GetAppearance(ctx context.Context) (control.ServerAppearanceSettings, error) {
	if err := ctx.Err(); err != nil {
		return control.ServerAppearanceSettings{}, err
	}
	return settings.CurrentAppearance(), nil
}

func (settings *paletteSmokeServerSettings) CurrentAppearance() control.ServerAppearanceSettings {
	settings.mu.RLock()
	defer settings.mu.RUnlock()
	return settings.appearance
}

func (settings *paletteSmokeServerSettings) UpdateAppearance(
	ctx context.Context,
	expectedVersion uint64,
	palette uipalette.Palette,
) (control.ServerAppearanceSettings, error) {
	settings.mu.Lock()
	defer settings.mu.Unlock()
	if err := uipalette.Validate(palette); err != nil {
		return control.ServerAppearanceSettings{}, fmt.Errorf("%w: %w", control.ErrInvalidArgument, err)
	}
	updated, err := settings.appearanceStore.Update(ctx, expectedVersion, palette)
	if err != nil {
		return control.ServerAppearanceSettings{}, err
	}
	settings.appearance = updated
	return updated, nil
}

// paletteSmokeSnapshotter and paletteSmokeExecutor stand in for ClickHouse.
// The smoke never runs a search; the search-job manager exists because the
// handler requires one, and because the search workspace, whose user menu
// the browser opens, is the page that mounts a `.floating-menu`.
type paletteSmokeSnapshotter struct{}

func (paletteSmokeSnapshotter) VisibilityCutoff(context.Context) (uint64, error) {
	return 1, nil
}

type paletteSmokeExecutor struct{}

func (paletteSmokeExecutor) Execute(
	_ context.Context,
	_ clickhouse.CompiledQuery,
	sink searchjobs.ResultSink,
) error {
	return sink.SetSchema(searchjobs.Schema{Columns: []searchjobs.Column{{
		Name: "message",
		Kind: searchjobs.ValueKindString,
	}}})
}

func TestBrowserInstancePaletteSmoke(t *testing.T) {
	if os.Getenv(paletteSmokeFlag) != "1" {
		t.Skip("set " + paletteSmokeFlag + "=1 to run the instance-palette browser smoke")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	repository := repositoryRoot(t)
	stagedRepository := buildBackendFrontend(t, ctx, repository)

	controlDB, err := control.Open(ctx, filepath.Join(t.TempDir(), "control.sqlite"))
	if err != nil {
		t.Fatalf("open palette smoke control database: %v", err)
	}
	t.Cleanup(func() {
		if err := controlDB.Close(); err != nil {
			t.Errorf("close palette smoke control database: %v", err)
		}
	})
	if _, err := controlDB.CreateIndex(ctx, control.IndexDefinition{
		Name:             "main",
		DisplayName:      "Palette smoke",
		RetentionPeriod:  time.Hour,
		IngestionEnabled: true,
		SearchEnabled:    true,
	}); err != nil {
		t.Fatalf("create palette smoke index: %v", err)
	}
	auditEvents, err := audit.NewStoreWithContext(ctx, controlDB, audit.StoreOptions{
		CursorKey: []byte("palette-smoke-audit-cursor-key!!"),
	})
	if err != nil {
		t.Fatalf("create palette smoke audit store: %v", err)
	}
	savedSearches, err := savedobjects.New(controlDB, savedobjects.Options{
		CursorKey: []byte("palette-smoke-saved-cursor-key!!"),
	})
	if err != nil {
		t.Fatalf("create palette smoke saved-search store: %v", err)
	}
	settings := newPaletteSmokeServerSettings(t, ctx, controlDB, auditEvents)
	if got := settings.CurrentAppearance(); got.Version != 0 || got.Palette != uipalette.Classic {
		t.Fatalf("fresh appearance = %+v, want version 0 classic", got)
	}

	token := mintPaletteSmokeAdministratorToken(t)
	authenticator, err := auth.NewBearerTokenAuthenticator(
		[]byte(token), paletteSmokeTenant, paletteSmokeOwner, auth.BrowserRoleAdministrator,
	)
	if err != nil {
		t.Fatalf("create palette smoke administrator authenticator: %v", err)
	}
	handler, err := server.NewHandler(server.Config{
		SearchJobs:           settings.jobs,
		Indexes:              browserSearchOnlyCatalog(controlDB),
		SavedSearches:        savedSearches,
		ServerSettings:       settings,
		BrowserAuthenticator: authenticator,
		WebUI:                os.DirFS(filepath.Join(stagedRepository, "out")),
		OwnerID:              paletteSmokeOwner,
		TenantID:             paletteSmokeTenant,
		MaximumPageSize:      100,
		Bootstrap: server.BootstrapConfig{
			Features: []opensplunk.ServerFeature{
				opensplunk.ServerFeature_SERVER_FEATURE_SEARCH,
			},
		},
	})
	if err != nil {
		t.Fatalf("create palette smoke HTTP handler: %v", err)
	}
	testServer := newIPv4LoopbackTestServer(t, handler)
	t.Cleanup(func() {
		testServer.Close()
		closeContext, closeCancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer closeCancel()
		if err := handler.Close(closeContext); err != nil {
			t.Errorf("close palette smoke HTTP handler: %v", err)
		}
	})

	runPaletteSmokeSpec(t, ctx, repository, testServer.URL, token)

	// The browser saw terminal (version 1) and glass (version 2), then a
	// rejected out-of-range enum. The durable singleton and the audit journal
	// must agree with what the page painted.
	final, err := settings.appearanceStore.Get(ctx)
	if err != nil {
		t.Fatalf("read final appearance: %v", err)
	}
	if final.Version != 2 || final.Palette != uipalette.Glass {
		t.Fatalf("final durable appearance = %+v, want version 2 glass", final)
	}
	if live := settings.CurrentAppearance(); live != final {
		t.Fatalf("live appearance %+v differs from durable %+v", live, final)
	}
	var auditedUpdates, latestAuditedVersion int
	if err := controlDB.SQLDB().QueryRowContext(
		ctx,
		`SELECT COUNT(*), COALESCE(MAX(target_version), 0) FROM audit_events
		 WHERE action = 'server_settings.update' AND target_id = ?`,
		string(control.ServerSettingsTargetUIPalette),
	).Scan(&auditedUpdates, &latestAuditedVersion); err != nil {
		t.Fatalf("count palette audit events: %v", err)
	}
	if auditedUpdates != 2 || latestAuditedVersion != 2 {
		t.Fatalf("palette audit events = %d (latest version %d), want 2 at version 2", auditedUpdates, latestAuditedVersion)
	}
}

func newPaletteSmokeServerSettings(
	t *testing.T,
	ctx context.Context,
	controlDB *control.DB,
	appender control.ServerSettingsMutationAuditAppender,
) *paletteSmokeServerSettings {
	t.Helper()
	settingsStore, err := control.NewServerSearchSettingsStore(controlDB, paletteSmokeTenant, appender)
	if err != nil {
		t.Fatalf("create palette smoke server settings store: %v", err)
	}
	initialSettings, err := settingsStore.Get(ctx)
	if err != nil {
		t.Fatalf("load palette smoke server settings: %v", err)
	}
	appearanceStore, err := control.NewServerAppearanceSettingsStore(controlDB, paletteSmokeTenant, appender)
	if err != nil {
		t.Fatalf("create palette smoke appearance store: %v", err)
	}
	initialAppearance, err := appearanceStore.Get(ctx)
	if err != nil {
		t.Fatalf("load palette smoke appearance settings: %v", err)
	}
	limitSource, err := searchlimits.NewSource(initialSettings.Limits)
	if err != nil {
		t.Fatalf("create palette smoke limit source: %v", err)
	}
	manager, err := searchjobs.New(searchjobs.Config{
		Executor:        paletteSmokeExecutor{},
		Snapshotter:     paletteSmokeSnapshotter{},
		Compiler:        clickhouse.Compiler{Database: "open_splunk", Table: "events"},
		MaxConcurrent:   1,
		MaxRows:         100,
		MaxBytes:        1 << 20,
		MaxPageBytes:    1 << 20,
		DefaultPageSize: 100,
		MaxPageSize:     100,
		RetentionTTL:    time.Hour,
		CleanupInterval: -1,
		LimitSource:     limitSource,
		CursorKey:       []byte("palette-smoke-job-cursor-key!!!!"),
	})
	if err != nil {
		t.Fatalf("create palette smoke search manager: %v", err)
	}
	t.Cleanup(func() {
		if err := manager.Close(); err != nil {
			t.Errorf("close palette smoke search manager: %v", err)
		}
	})
	return &paletteSmokeServerSettings{
		store: settingsStore, source: limitSource, jobs: manager, current: initialSettings,
		appearanceStore: appearanceStore, appearance: initialAppearance,
	}
}

func mintPaletteSmokeAdministratorToken(t *testing.T) string {
	t.Helper()
	random := make([]byte, 32)
	if _, err := rand.Read(random); err != nil {
		t.Fatalf("generate palette smoke administrator token: %v", err)
	}
	token := base64.RawURLEncoding.EncodeToString(random)
	clear(random)
	return token
}

func runPaletteSmokeSpec(t *testing.T, ctx context.Context, repository, baseURL, token string) {
	t.Helper()
	browserContext, cancel := context.WithTimeout(ctx, 3*time.Minute)
	defer cancel()
	command := exec.CommandContext(
		browserContext,
		filepath.Join(repository, "node_modules", ".bin", "playwright"),
		"test",
		"--config=playwright.palette-smoke.config.ts",
		"--workers=1",
		"--reporter=line",
		"--output="+filepath.Join(repository, "test-results", "palette-smoke"),
	)
	configureProcessGroup(command)
	command.Dir = repository
	environment := environmentWithValue(os.Environ(), "OPEN_SPLUNK_PALETTE_SMOKE_BASE_URL", baseURL)
	environment = environmentWithValue(environment, "OPEN_SPLUNK_PALETTE_SMOKE_ADMINISTRATOR_TOKEN", token)
	command.Env = environment
	logs, truncated, runErr := runCommandWithBoundedOutput(command, maximumHarnessOutputBytes)
	if runErr != nil {
		t.Fatalf(
			"verify instance palette in the browser: %v\n%s",
			runErr,
			redactForFailure(formatBoundedCommandOutput(logs, truncated, maximumHarnessOutputBytes), token),
		)
	}
	if truncated {
		t.Fatalf("instance palette browser logs exceeded %d bytes", maximumHarnessOutputBytes)
	}
	// The line reporter's closing summary ("1 passed (…)") is the evidence that
	// the browser flow ran, so keep it in the verbose output on success too.
	t.Logf("instance palette browser run: %s", strings.TrimSpace(redactForFailure(logs, token)))
}

var _ server.Settings = (*paletteSmokeServerSettings)(nil)
var _ searchjobs.Executor = paletteSmokeExecutor{}
var _ searchjobs.Snapshotter = paletteSmokeSnapshotter{}
