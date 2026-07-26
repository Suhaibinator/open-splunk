//go:build !windows

package integration_test

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
	"github.com/Suhaibinator/open-splunk/internal/control"
	exportjobs "github.com/Suhaibinator/open-splunk/internal/export"
	"github.com/Suhaibinator/open-splunk/internal/savedobjects"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
	"github.com/Suhaibinator/open-splunk/internal/searchws"
	"github.com/Suhaibinator/open-splunk/internal/server"
)

const (
	browserSequenceExpiredJobID        = "browser-sequence-expired"
	browserSequenceExpiredIndex        = "main"
	browserSequenceExpiredOwner        = "browser-recovery-owner"
	browserSequenceExpiredTenant       = "browser-recovery-tenant"
	browserSequenceExpiredInitialRow   = "preview before sequence expiration"
	browserSequenceExpiredRecoveredRow = "preview after sequence recovery"
	browserRecoveryControlTokenHeader  = "X-Open-Splunk-Test-Token"
)

type browserRecoverySnapshotter uint64

func (snapshotter browserRecoverySnapshotter) VisibilityCutoff(context.Context) (uint64, error) {
	return uint64(snapshotter), nil
}

type browserRecoveryExportSnapshots struct{}

func (browserRecoveryExportSnapshots) Get(
	context.Context,
	searchjobs.AccessScope,
	string,
) (exportjobs.Job, error) {
	return exportjobs.Job{}, exportjobs.ErrNotFound
}

type browserRecoveryCommandKind uint8

const (
	browserRecoveryCommandProgress browserRecoveryCommandKind = iota + 1
	browserRecoveryCommandAppend
	browserRecoveryCommandComplete
)

type browserRecoveryCommand struct {
	kind  browserRecoveryCommandKind
	reply chan error
}

type browserRecoveryExecutor struct {
	calls    atomic.Uint32
	exits    atomic.Uint32
	commands chan browserRecoveryCommand
}

func newBrowserRecoveryExecutor() *browserRecoveryExecutor {
	return &browserRecoveryExecutor{
		commands: make(chan browserRecoveryCommand),
	}
}

func (executor *browserRecoveryExecutor) Execute(
	ctx context.Context,
	_ clickhouse.CompiledQuery,
	sink searchjobs.ResultSink,
) error {
	executor.calls.Add(1)
	defer executor.exits.Add(1)
	if err := sink.SetSchema(searchjobs.Schema{Columns: []searchjobs.Column{{
		Name: "message",
		Kind: searchjobs.ValueKindString,
	}}}); err != nil {
		return err
	}
	if err := sink.AddRow([]searchjobs.Value{
		searchjobs.StringValue(browserSequenceExpiredInitialRow),
	}); err != nil {
		return err
	}
	progress, ok := sink.(searchjobs.ProgressSink)
	if !ok {
		return errors.New("browser recovery result sink does not report progress")
	}
	progressSteps := 0
	rowAppended := false
	for {
		select {
		case command := <-executor.commands:
			var commandErr error
			complete := false
			switch command.kind {
			case browserRecoveryCommandProgress:
				if progressSteps >= 5 || rowAppended {
					commandErr = errors.New("browser recovery progress command is out of order")
					break
				}
				commandErr = progress.ReportProgress(searchjobs.ExecutionProgressDelta{
					ScannedRows:  1,
					ScannedBytes: 10,
				})
				if commandErr == nil {
					progressSteps++
				}
			case browserRecoveryCommandAppend:
				if progressSteps != 5 || rowAppended {
					commandErr = errors.New("browser recovery append command is out of order")
					break
				}
				commandErr = sink.AddRow([]searchjobs.Value{
					searchjobs.StringValue(browserSequenceExpiredRecoveredRow),
				})
				if commandErr == nil {
					rowAppended = true
				}
			case browserRecoveryCommandComplete:
				if progressSteps != 5 || !rowAppended {
					commandErr = errors.New("browser recovery completion command is out of order")
					break
				}
				complete = true
			default:
				commandErr = errors.New("browser recovery command is invalid")
			}
			command.reply <- commandErr
			if commandErr != nil {
				return commandErr
			}
			if complete {
				return nil
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (executor *browserRecoveryExecutor) command(
	ctx context.Context,
	kind browserRecoveryCommandKind,
) error {
	command := browserRecoveryCommand{kind: kind, reply: make(chan error, 1)}
	select {
	case executor.commands <- command:
	case <-ctx.Done():
		return ctx.Err()
	}
	select {
	case err := <-command.reply:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

type browserRecoveryController struct {
	token     string
	executor  *browserRecoveryExecutor
	total     atomic.Uint32
	progress  atomic.Uint32
	appendRow atomic.Uint32
	complete  atomic.Uint32
}

func (controller *browserRecoveryController) ServeHTTP(
	response http.ResponseWriter,
	request *http.Request,
) {
	if request.Method != http.MethodPost ||
		request.Header.Get(browserRecoveryControlTokenHeader) != controller.token ||
		request.ContentLength > 0 {
		http.Error(response, http.StatusText(http.StatusNotFound), http.StatusNotFound)
		return
	}
	var kind browserRecoveryCommandKind
	switch request.URL.Path {
	case "/progress":
		kind = browserRecoveryCommandProgress
		controller.progress.Add(1)
	case "/append":
		kind = browserRecoveryCommandAppend
		controller.appendRow.Add(1)
	case "/complete":
		kind = browserRecoveryCommandComplete
		controller.complete.Add(1)
	default:
		http.Error(response, http.StatusText(http.StatusNotFound), http.StatusNotFound)
		return
	}
	controller.total.Add(1)
	ctx, cancel := context.WithTimeout(request.Context(), 3*time.Second)
	defer cancel()
	if err := controller.executor.command(ctx, kind); err != nil {
		http.Error(response, "browser recovery command failed", http.StatusConflict)
		return
	}
	response.Header().Set("Content-Type", "application/json")
	response.WriteHeader(http.StatusOK)
	_, _ = response.Write([]byte("{}\n"))
}

func TestBrowserSequenceExpiredRecovery(t *testing.T) {
	if os.Getenv(backendIntegrationFlag) != "1" {
		t.Skip("set " + backendIntegrationFlag + "=1 to run the browser recovery integration test")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	repository := repositoryRoot(t)
	stagedBackendRepository := buildBackendFrontend(t, ctx, repository)

	controlDB, err := control.Open(ctx, filepath.Join(t.TempDir(), "control.sqlite"))
	if err != nil {
		t.Fatalf("open browser recovery control database: %v", err)
	}
	t.Cleanup(func() {
		if err := controlDB.Close(); err != nil {
			t.Errorf("close browser recovery control database: %v", err)
		}
	})
	if _, err := controlDB.CreateIndex(ctx, control.IndexDefinition{
		Name:             browserSequenceExpiredIndex,
		DisplayName:      "Browser sequence recovery",
		RetentionPeriod:  time.Hour,
		IngestionEnabled: true,
		SearchEnabled:    true,
	}); err != nil {
		t.Fatalf("create browser recovery index: %v", err)
	}
	savedSearches, err := savedobjects.New(controlDB, savedobjects.Options{
		CursorKey: []byte("0123456789abcdef0123456789abcdef"),
	})
	if err != nil {
		t.Fatalf("create browser recovery saved-search store: %v", err)
	}

	anchor := time.Date(2026, time.July, 25, 18, 0, 0, 0, time.UTC)
	executor := newBrowserRecoveryExecutor()
	manager, err := searchjobs.New(searchjobs.Config{
		Executor:        executor,
		Snapshotter:     browserRecoverySnapshotter(17),
		Compiler:        clickhouse.Compiler{Database: "open_splunk", Table: "events"},
		MaxConcurrent:   1,
		RetentionTTL:    time.Hour,
		CleanupInterval: -1,
		Now:             func() time.Time { return anchor },
		NewID:           func() string { return browserSequenceExpiredJobID },
		CursorKey:       []byte("abcdef0123456789abcdef0123456789"),
	})
	if err != nil {
		t.Fatalf("create browser recovery search manager: %v", err)
	}
	t.Cleanup(func() {
		if err := manager.Close(); err != nil {
			t.Errorf("close browser recovery search manager: %v", err)
		}
	})

	searchSocket, err := searchws.New(searchws.Config{
		Searches:             manager,
		Exports:              browserRecoveryExportSnapshots{},
		Access:               searchjobs.AccessScope{TenantID: browserSequenceExpiredTenant, OwnerID: browserSequenceExpiredOwner},
		MaximumSubscriptions: 8,
		MaximumFrameBytes:    64 << 10,
		MaximumReplayEvents:  1,
		PollInterval:         10 * time.Millisecond,
		PingInterval:         3 * time.Second,
		PongTimeout:          5 * time.Second,
		Now:                  func() time.Time { return anchor },
	})
	if err != nil {
		t.Fatalf("create browser recovery WebSocket: %v", err)
	}
	handler, err := server.NewHandler(server.Config{
		SearchJobs:      manager,
		SearchWebSocket: searchSocket,
		Indexes:         controlDB,
		SavedSearches:   savedSearches,
		WebUI:           os.DirFS(filepath.Join(stagedBackendRepository, "out")),
		OwnerID:         browserSequenceExpiredOwner,
		TenantID:        browserSequenceExpiredTenant,
		Now:             func() time.Time { return anchor },
		Bootstrap: server.BootstrapConfig{
			ServerVersion:           "browser-recovery-test",
			APIVersion:              "v1",
			SPLCompatibilityVersion: splCompatibilityVersionForTest,
		},
	})
	if err != nil {
		closeContext, closeCancel := context.WithTimeout(context.Background(), 2*time.Second)
		_ = searchSocket.Close(closeContext)
		closeCancel()
		t.Fatalf("create browser recovery HTTP handler: %v", err)
	}
	var applicationServer *httptest.Server
	t.Cleanup(func() {
		closeContext, closeCancel := context.WithTimeout(context.Background(), 2*time.Second)
		if err := handler.Close(closeContext); err != nil {
			t.Errorf("close browser recovery HTTP handler: %v", err)
		}
		closeCancel()
		if applicationServer != nil {
			applicationServer.Close()
		}
	})
	applicationServer = newIPv4LoopbackTestServer(t, handler)

	tokenBytes := make([]byte, 32)
	if _, err := rand.Read(tokenBytes); err != nil {
		t.Fatalf("generate browser recovery control token: %v", err)
	}
	controller := &browserRecoveryController{
		token:    hex.EncodeToString(tokenBytes),
		executor: executor,
	}
	controlServer := newIPv4LoopbackTestServer(t, controller)
	t.Cleanup(controlServer.Close)

	runBrowserSequenceExpiredSpec(
		t,
		ctx,
		repository,
		applicationServer.URL,
		controlServer.URL,
		controller.token,
		anchor,
	)
	waitForAtomicValue(t, &executor.exits, 1, "browser recovery executor exit")
	if calls := executor.calls.Load(); calls != 1 {
		t.Fatalf("browser recovery executor calls = %d, want 1", calls)
	}
	if total := controller.total.Load(); total != 7 ||
		controller.progress.Load() != 5 ||
		controller.appendRow.Load() != 1 ||
		controller.complete.Load() != 1 {
		t.Fatalf(
			"browser recovery control calls = total:%d progress:%d append:%d complete:%d",
			total,
			controller.progress.Load(),
			controller.appendRow.Load(),
			controller.complete.Load(),
		)
	}
}

func runBrowserSequenceExpiredSpec(
	t *testing.T,
	ctx context.Context,
	repository, baseURL, controlURL, controlToken string,
	anchor time.Time,
) {
	t.Helper()
	browserContext, cancel := context.WithTimeout(ctx, 90*time.Second)
	defer cancel()
	command := exec.CommandContext(
		browserContext,
		filepath.Join(repository, "node_modules", ".bin", "playwright"),
		"test",
		"integration/browser_vertical.spec.ts",
		"--workers=1",
		"--reporter=line",
		"--grep=real sequence expiration",
		"--output="+filepath.Join(repository, "test-results", "browser-sequence-expired"),
	)
	configureProcessGroup(command)
	command.Dir = repository
	environment := os.Environ()
	for name, value := range map[string]string{
		"OPEN_SPLUNK_E2E_BASE_URL":                 baseURL,
		"OPEN_SPLUNK_E2E_SPL":                      "index=main | table message",
		"OPEN_SPLUNK_E2E_EARLIEST":                 anchor.Add(-time.Hour).Format(time.RFC3339Nano),
		"OPEN_SPLUNK_E2E_LATEST":                   anchor.Format(time.RFC3339Nano),
		"OPEN_SPLUNK_E2E_EXPECTED_TEXT":            browserSequenceExpiredRecoveredRow,
		"OPEN_SPLUNK_E2E_EXPECTED_ROWS":            "2",
		"OPEN_SPLUNK_E2E_RECOVERY_CONTROL_URL":     controlURL,
		"OPEN_SPLUNK_E2E_RECOVERY_CONTROL_TOKEN":   controlToken,
		"OPEN_SPLUNK_E2E_RECOVERY_INITIAL_TEXT":    browserSequenceExpiredInitialRow,
		"OPEN_SPLUNK_E2E_SEQUENCE_EXPIRATION_TEST": "1",
	} {
		environment = environmentWithValue(environment, name, value)
	}
	command.Env = environment
	logs := &lockedBuffer{maximum: 1 << 20}
	command.Stdout = logs
	command.Stderr = logs
	if err := command.Run(); err != nil {
		t.Fatalf("verify browser sequence-expiration recovery: %v\n%s", err, logs.String())
	}
}

func waitForAtomicValue(
	t *testing.T,
	value *atomic.Uint32,
	want uint32,
	description string,
) {
	t.Helper()
	deadline := time.NewTimer(3 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		if got := value.Load(); got == want {
			return
		} else if got > want {
			t.Fatalf("%s = %d, want %d", description, got, want)
		}
		select {
		case <-deadline.C:
			t.Fatalf("timed out waiting for %s = %d; got %d", description, want, value.Load())
		case <-ticker.C:
		}
	}
}

func newIPv4LoopbackTestServer(t *testing.T, handler http.Handler) *httptest.Server {
	t.Helper()
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen for loopback test server: %v", err)
	}
	testServer := httptest.NewUnstartedServer(handler)
	testServer.Listener = listener
	testServer.Start()
	return testServer
}

var _ searchjobs.Executor = (*browserRecoveryExecutor)(nil)
var _ searchjobs.Snapshotter = browserRecoverySnapshotter(0)
var _ searchws.ExportSnapshots = browserRecoveryExportSnapshots{}
