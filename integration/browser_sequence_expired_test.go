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
	"path/filepath"
	"strconv"
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

func (snapshots browserRecoveryExportSnapshots) Snapshot(
	ctx context.Context,
	scope searchjobs.AccessScope,
	id string,
) (exportjobs.Job, error) {
	return snapshots.Get(ctx, scope, id)
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
	appendBeforeCompletion bool
	calls                  atomic.Uint32
	exits                  atomic.Uint32
	canceledExits          atomic.Uint32
	commands               chan browserRecoveryCommand
}

func newBrowserRecoveryExecutor(appendBeforeCompletion bool) *browserRecoveryExecutor {
	return &browserRecoveryExecutor{
		appendBeforeCompletion: appendBeforeCompletion,
		commands:               make(chan browserRecoveryCommand),
	}
}

func (executor *browserRecoveryExecutor) Execute(
	ctx context.Context,
	_ clickhouse.CompiledQuery,
	sink searchjobs.ResultSink,
) (returnedErr error) {
	executor.calls.Add(1)
	defer func() {
		if errors.Is(returnedErr, context.Canceled) {
			executor.canceledExits.Add(1)
		}
		executor.exits.Add(1)
	}()
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
				if progressSteps != 5 || rowAppended || !executor.appendBeforeCompletion {
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
				if progressSteps != 5 || rowAppended != executor.appendBeforeCompletion {
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
	searches  *searchjobs.Manager
	total     atomic.Uint32
	progress  atomic.Uint32
	appendRow atomic.Uint32
	complete  atomic.Uint32
}

func (controller *browserRecoveryController) waitForCompletion(ctx context.Context) error {
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for {
		job, err := controller.searches.Get(browserSequenceExpiredJobID)
		if err != nil {
			return err
		}
		if job.State == searchjobs.StateCompleted {
			return nil
		}
		if job.State.Terminal() {
			return errors.New("browser recovery job reached unexpected state " + job.State.String())
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
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
	if kind == browserRecoveryCommandComplete {
		if err := controller.waitForCompletion(ctx); err != nil {
			http.Error(response, "browser recovery completion failed", http.StatusConflict)
			return
		}
	}
	response.Header().Set("Content-Type", "application/json")
	response.WriteHeader(http.StatusOK)
	_, _ = response.Write([]byte("{}\n"))
}

type browserRecoveryBrowserSpec struct {
	grepPattern            string
	outputDirectory        string
	environmentFlag        string
	failureName            string
	outcome                browserRecoveryOutcome
	appendBeforeCompletion bool
	expectedText           string
	expectedRows           int
}

type browserRecoveryOutcome uint8

const (
	browserRecoveryOutcomeComplete browserRecoveryOutcome = iota + 1
	browserRecoveryOutcomeCanceled
)

func TestBrowserSequenceExpiredRecovery(t *testing.T) {
	runBrowserRecoveryFixture(t, 1, browserRecoveryBrowserSpec{
		grepPattern:            "real sequence expiration",
		outputDirectory:        "browser-sequence-expired",
		environmentFlag:        "OPEN_SPLUNK_E2E_SEQUENCE_EXPIRATION_TEST",
		failureName:            "sequence-expiration",
		outcome:                browserRecoveryOutcomeComplete,
		appendBeforeCompletion: true,
		expectedText:           browserSequenceExpiredRecoveredRow,
		expectedRows:           2,
	})
}

func TestBrowserSequenceGapRecovery(t *testing.T) {
	runBrowserRecoveryFixture(t, 8, browserRecoveryBrowserSpec{
		grepPattern:            "live preview recovers from a real sequence gap",
		outputDirectory:        "browser-sequence-gap",
		environmentFlag:        "OPEN_SPLUNK_E2E_SEQUENCE_GAP_TEST",
		failureName:            "sequence-gap",
		outcome:                browserRecoveryOutcomeComplete,
		appendBeforeCompletion: false,
		expectedText:           browserSequenceExpiredInitialRow,
		expectedRows:           1,
	})
}

func TestBrowserSequenceGapRESTTerminalRecovery(t *testing.T) {
	runBrowserRecoveryFixture(t, 8, browserRecoveryBrowserSpec{
		grepPattern:            "live preview recovers through REST-only completion after a real sequence gap",
		outputDirectory:        "browser-sequence-gap-rest-terminal",
		environmentFlag:        "OPEN_SPLUNK_E2E_SEQUENCE_GAP_REST_TERMINAL_TEST",
		failureName:            "sequence-gap REST-terminal",
		outcome:                browserRecoveryOutcomeComplete,
		appendBeforeCompletion: false,
		expectedText:           browserSequenceExpiredInitialRow,
		expectedRows:           1,
	})
}

func TestBrowserSequenceGapRESTFirstProgressRecovery(t *testing.T) {
	runBrowserRecoveryFixture(t, 8, browserRecoveryBrowserSpec{
		grepPattern:            "live progress preserves a REST-first snapshot across retained replay",
		outputDirectory:        "browser-sequence-gap-rest-first-progress",
		environmentFlag:        "OPEN_SPLUNK_E2E_SEQUENCE_GAP_REST_FIRST_PROGRESS_TEST",
		failureName:            "sequence-gap REST-first progress",
		outcome:                browserRecoveryOutcomeComplete,
		appendBeforeCompletion: false,
		expectedText:           browserSequenceExpiredInitialRow,
		expectedRows:           1,
	})
}

func TestBrowserSearchCancellation(t *testing.T) {
	runBrowserRecoveryFixture(t, 8, browserRecoveryBrowserSpec{
		grepPattern:            "browser cancellation is authoritative and does not reconnect",
		outputDirectory:        "browser-search-cancellation",
		environmentFlag:        "OPEN_SPLUNK_E2E_CANCELLATION_TEST",
		failureName:            "search cancellation",
		outcome:                browserRecoveryOutcomeCanceled,
		appendBeforeCompletion: false,
		expectedText:           browserSequenceExpiredInitialRow,
		expectedRows:           1,
	})
}

func runBrowserRecoveryFixture(
	t *testing.T,
	maximumReplayEvents int,
	spec browserRecoveryBrowserSpec,
) {
	t.Helper()
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
	executor := newBrowserRecoveryExecutor(spec.appendBeforeCompletion)
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
		MaximumReplayEvents:  maximumReplayEvents,
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
		Indexes:         browserSearchOnlyCatalog(controlDB),
		SavedSearches:   savedSearches,
		WebUI:           os.DirFS(filepath.Join(stagedBackendRepository, "out")),
		OwnerID:         browserSequenceExpiredOwner,
		TenantID:        browserSequenceExpiredTenant,
		Now:             func() time.Time { return anchor },
	})
	if err != nil {
		closeContext, closeCancel := context.WithTimeout(context.Background(), 2*time.Second)
		_ = searchSocket.Close(closeContext)
		closeCancel()
		t.Fatalf("create browser recovery HTTP handler: %v", err)
	}
	var (
		applicationServer *httptest.Server
		serverCreateCalls atomic.Uint32
		serverCancelCalls atomic.Uint32
	)
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
	applicationServer = newIPv4LoopbackTestServer(t, http.HandlerFunc(
		func(response http.ResponseWriter, request *http.Request) {
			if request.Method == http.MethodPost {
				switch request.URL.Path {
				case "/api/search/jobs/create":
					serverCreateCalls.Add(1)
				case "/api/search/jobs/cancel":
					serverCancelCalls.Add(1)
				}
			}
			handler.ServeHTTP(response, request)
		},
	))

	tokenBytes := make([]byte, 32)
	if _, err := rand.Read(tokenBytes); err != nil {
		t.Fatalf("generate browser recovery control token: %v", err)
	}
	controller := &browserRecoveryController{
		token:    hex.EncodeToString(tokenBytes),
		executor: executor,
		searches: manager,
	}
	controlServer := newIPv4LoopbackTestServer(t, controller)
	t.Cleanup(controlServer.Close)

	runBrowserRecoverySpec(
		t,
		ctx,
		repository,
		applicationServer.URL,
		controlServer.URL,
		controller.token,
		anchor,
		spec,
	)
	waitForAtomicValue(t, &executor.exits, 1, "browser recovery executor exit")
	if calls := executor.calls.Load(); calls != 1 {
		t.Fatalf("browser recovery executor calls = %d, want 1", calls)
	}
	if calls := serverCreateCalls.Load(); calls != 1 {
		t.Fatalf("browser recovery server create calls = %d, want 1", calls)
	}
	switch spec.outcome {
	case browserRecoveryOutcomeComplete:
		expectedControlCalls := uint32(6)
		expectedAppendCalls := uint32(0)
		if spec.appendBeforeCompletion {
			expectedControlCalls++
			expectedAppendCalls++
		}
		if total := controller.total.Load(); total != expectedControlCalls ||
			controller.progress.Load() != 5 ||
			controller.appendRow.Load() != expectedAppendCalls ||
			controller.complete.Load() != 1 ||
			executor.canceledExits.Load() != 0 ||
			serverCancelCalls.Load() != 0 {
			t.Fatalf(
				"completed browser recovery controls = total:%d progress:%d append:%d complete:%d canceled_exits:%d cancel_requests:%d",
				total,
				controller.progress.Load(),
				controller.appendRow.Load(),
				controller.complete.Load(),
				executor.canceledExits.Load(),
				serverCancelCalls.Load(),
			)
		}
	case browserRecoveryOutcomeCanceled:
		if total := controller.total.Load(); total != 0 ||
			controller.progress.Load() != 0 ||
			controller.appendRow.Load() != 0 ||
			controller.complete.Load() != 0 ||
			executor.canceledExits.Load() != 1 ||
			serverCancelCalls.Load() != 1 {
			t.Fatalf(
				"canceled browser recovery controls = total:%d progress:%d append:%d complete:%d canceled_exits:%d cancel_requests:%d",
				total,
				controller.progress.Load(),
				controller.appendRow.Load(),
				controller.complete.Load(),
				executor.canceledExits.Load(),
				serverCancelCalls.Load(),
			)
		}
		job, err := manager.Get(browserSequenceExpiredJobID)
		if err != nil {
			t.Fatalf("get canceled browser search: %v", err)
		}
		if job.State != searchjobs.StateCanceled ||
			job.Version == 0 ||
			job.Failure != nil ||
			job.FinishedAt.IsZero() {
			t.Fatalf("authoritative canceled browser search = %+v", job)
		}
	default:
		t.Fatalf("browser recovery outcome = %d", spec.outcome)
	}
}

func runBrowserRecoverySpec(
	t *testing.T,
	ctx context.Context,
	repository, baseURL, controlURL, controlToken string,
	anchor time.Time,
	spec browserRecoveryBrowserSpec,
) {
	t.Helper()
	runBrowserVerticalSpec(t, ctx, repository, browserVerticalSpecConfig{
		grepPattern:        spec.grepPattern,
		outputDirectory:    spec.outputDirectory,
		failureDescription: "verify browser " + spec.failureName + " recovery",
		environment: map[string]string{
			"OPEN_SPLUNK_E2E_BASE_URL":               baseURL,
			"OPEN_SPLUNK_E2E_SPL":                    "index=main | table message",
			"OPEN_SPLUNK_E2E_EARLIEST":               anchor.Add(-time.Hour).Format(time.RFC3339Nano),
			"OPEN_SPLUNK_E2E_LATEST":                 anchor.Format(time.RFC3339Nano),
			"OPEN_SPLUNK_E2E_EXPECTED_TEXT":          spec.expectedText,
			"OPEN_SPLUNK_E2E_EXPECTED_ROWS":          strconv.Itoa(spec.expectedRows),
			"OPEN_SPLUNK_E2E_RECOVERY_CONTROL_URL":   controlURL,
			"OPEN_SPLUNK_E2E_RECOVERY_CONTROL_TOKEN": controlToken,
			"OPEN_SPLUNK_E2E_RECOVERY_INITIAL_TEXT":  browserSequenceExpiredInitialRow,
			spec.environmentFlag:                     "1",
		},
	})
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
	listener, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp4", "127.0.0.1:0")
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
