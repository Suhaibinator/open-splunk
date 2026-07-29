//go:build !windows

package integration_test

import (
	"context"
	"errors"
	"fmt"
	"math"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	clickhousedriver "github.com/ClickHouse/clickhouse-go/v2"
	clickhousequery "github.com/Suhaibinator/open-splunk/internal/clickhouse"
	"github.com/Suhaibinator/open-splunk/internal/testsupport"
)

const (
	backendLoadIntegrationFlag     = "OPEN_SPLUNK_BACKEND_LOAD"
	backendLoadWarmSeed            = int64(2026072601)
	backendLoadMainSeed            = int64(2026072602)
	backendLoadRecoverySeed        = int64(2026072603)
	backendLoadMaximumExactDC      = uint64(clickhousequery.MaximumStatsDistinctValuesPerGroup)
	backendLoadRecoveryLimit       = 15 * time.Second
	backendLoadConcurrentJobs      = 8
	backendLoadMaximumSearchWaves  = 3
	backendLoadSearchRetryInterval = 100 * time.Millisecond
)

type backendLoadPlan struct {
	TenantID      string
	IndexName     string
	WarmEvents    uint64
	MainEvents    uint64
	OfflineEvents uint64
	SpoolHeadroom uint64
	Cardinality   uint64
	Rate          uint64
	FlushEvents   uint64
}

func defaultBackendLoadPlan() backendLoadPlan {
	return backendLoadPlan{
		TenantID:      "backend-load-tenant",
		IndexName:     "backend-load",
		WarmEvents:    5_000,
		MainEvents:    25_000,
		OfflineEvents: 5_000,
		SpoolHeadroom: 1_000,
		Cardinality:   10_000,
		Rate:          1_000,
		FlushEvents:   100,
	}
}

func (plan backendLoadPlan) eventCount() uint64 {
	return plan.WarmEvents + plan.MainEvents
}

func (plan backendLoadPlan) interval() time.Duration {
	if plan.Rate == 0 || plan.Rate > uint64(time.Second) {
		return 0
	}
	return time.Duration(uint64(time.Second) / plan.Rate)
}

func (plan backendLoadPlan) offlineGenerationEvents() uint64 {
	return plan.OfflineEvents + plan.SpoolHeadroom
}

func (plan backendLoadPlan) recoveryEvents() uint64 {
	return plan.MainEvents - plan.offlineGenerationEvents()
}

func (plan backendLoadPlan) validate() error {
	if strings.TrimSpace(plan.TenantID) == "" || strings.TrimSpace(plan.IndexName) == "" {
		return errors.New("load tenant and index are required")
	}
	if plan.WarmEvents < 2 || plan.MainEvents < 2 {
		return errors.New("load warm and main event counts must each be at least two")
	}
	if plan.WarmEvents > ^uint64(0)-plan.MainEvents {
		return errors.New("load warm and main event counts overflow uint64")
	}
	eventCount := plan.eventCount()
	if eventCount > backendLoadMaximumExactDC {
		return fmt.Errorf(
			"load event count %d exceeds the exact SPL distinct-count limit %d",
			eventCount,
			backendLoadMaximumExactDC,
		)
	}
	if plan.Cardinality == 0 || plan.FlushEvents == 0 {
		return errors.New("load cardinality and flush event count must be positive")
	}
	if plan.Rate == 0 {
		return errors.New("load rate must be positive")
	}
	if plan.Rate > uint64(time.Second) || uint64(time.Second)%plan.Rate != 0 {
		return fmt.Errorf(
			"load rate %d must produce a positive whole-nanosecond interval",
			plan.Rate,
		)
	}
	if plan.FlushEvents > plan.WarmEvents || plan.FlushEvents > plan.MainEvents ||
		plan.OfflineEvents == 0 || plan.SpoolHeadroom == 0 ||
		plan.OfflineEvents > plan.MainEvents ||
		plan.SpoolHeadroom > plan.MainEvents-plan.OfflineEvents ||
		plan.recoveryEvents() < plan.WarmEvents {
		return errors.New("load outage window must leave online work before and after the outage")
	}
	return nil
}

func (plan backendLoadPlan) scheduledDuration() time.Duration {
	if plan.WarmEvents > ^uint64(0)-plan.MainEvents {
		return 0
	}
	eventCount := plan.eventCount()
	interval := plan.interval()
	if eventCount < 3 || interval <= 0 {
		return 0
	}
	pacedIntervals := eventCount - 3
	if pacedIntervals > uint64(math.MaxInt64)/uint64(interval) {
		return 0
	}
	return time.Duration(pacedIntervals * uint64(interval))
}

func (plan backendLoadPlan) recoveryTimeout() time.Duration {
	interval := plan.interval()
	if plan.OfflineEvents > plan.MainEvents ||
		plan.SpoolHeadroom > plan.MainEvents-plan.OfflineEvents {
		return 0
	}
	events := plan.recoveryEvents()
	if events < 2 || interval <= 0 {
		return 0
	}
	if events-1 > uint64(math.MaxInt64)/uint64(interval) {
		return 0
	}
	activeWindow := time.Duration(events-1) * interval
	if activeWindow <= time.Second {
		return activeWindow
	}
	return min(backendLoadRecoveryLimit, activeWindow-time.Second)
}

func TestBackendLoadPlanPinsSustainedOutageWindow(t *testing.T) {
	t.Parallel()
	plan := defaultBackendLoadPlan()
	if err := plan.validate(); err != nil {
		t.Fatal(err)
	}
	if plan.eventCount() != 30_000 ||
		plan.WarmEvents != 5_000 ||
		plan.MainEvents != 25_000 ||
		plan.OfflineEvents != 5_000 ||
		plan.SpoolHeadroom != 1_000 ||
		plan.Cardinality != 10_000 ||
		plan.Rate != 1_000 ||
		plan.interval() != time.Millisecond ||
		plan.offlineGenerationEvents() != 6_000 ||
		plan.recoveryEvents() != 19_000 ||
		plan.recoveryTimeout() != backendLoadRecoveryLimit ||
		plan.FlushEvents != 100 {
		t.Fatalf("default load plan = %+v", plan)
	}
	if got, want := plan.scheduledDuration(), 29_997*time.Millisecond; got != want {
		t.Fatalf("scheduled duration = %s, want %s", got, want)
	}
	if plan.recoveryEvents() < plan.WarmEvents {
		t.Fatalf("load plan leaves no post-recovery generation: %+v", plan)
	}
	if got, want := backendLoadSearchProgressTimeout(plan, 11_200), 17_799*time.Millisecond; got != want {
		t.Fatalf("search progress timeout = %s, want %s", got, want)
	}
	if got := backendLoadSearchProgressTimeout(plan, plan.eventCount()-plan.FlushEvents); got != 0 {
		t.Fatalf("near-EOF search progress timeout = %s, want 0", got)
	}
}

func TestBackendLoadPlanRejectsWrappedCountsAndInexactDistinctCounts(t *testing.T) {
	t.Parallel()
	overflow := defaultBackendLoadPlan()
	overflow.WarmEvents = ^uint64(0)
	overflow.MainEvents = 5
	if err := overflow.validate(); err == nil {
		t.Fatal("overflowing warm/main event counts unexpectedly validated")
	}

	tooMany := defaultBackendLoadPlan()
	tooMany.WarmEvents = 2
	tooMany.MainEvents = backendLoadMaximumExactDC - 1
	if err := tooMany.validate(); err == nil {
		t.Fatal("event count above the exact SPL distinct-count limit unexpectedly validated")
	}
}

func TestBackendLoadDurableWALAppendedEvents(t *testing.T) {
	t.Parallel()
	logs := strings.Join([]string{
		`time=now level=INFO msg="collector starting"`,
		`time=now level=DEBUG msg="collector: batch appended" batch_sequence=11 events=250 bytes=1000`,
		`time=now level=DEBUG msg="collector: batch appended" batch_sequence=12 events=500 bytes=2000`,
	}, "\n")
	if got, err := backendLoadDurableWALAppendedEvents(logs); err != nil || got != 750 {
		t.Fatalf("backendLoadDurableWALAppendedEvents() = %d, %v; want 750", got, err)
	}
	if _, err := backendLoadDurableWALAppendedEvents(
		`level=DEBUG msg="collector: batch appended" batch_sequence=11 bytes=1000`,
	); err == nil {
		t.Fatal("WAL append log without an event count unexpectedly parsed")
	}
}

func TestBackendSustainedLoad(t *testing.T) {
	if os.Getenv(backendLoadIntegrationFlag) != "1" {
		t.Skip("set " + backendLoadIntegrationFlag + "=1 to run the sustained backend load integration test")
	}
	runBackendSustainedLoad(t, defaultBackendLoadPlan())
}

type backendLoadRecoveryObservation struct {
	RecoveredAt time.Time
	Source      backendLoadSourceProgress
	StoredRows  uint64
}

func runBackendSustainedLoad(t *testing.T, plan backendLoadPlan) {
	t.Helper()
	if err := plan.validate(); err != nil {
		t.Fatal(err)
	}
	if _, err := exec.LookPath("docker"); err != nil {
		t.Fatalf("docker CLI is required when %s=1: %v", backendLoadIntegrationFlag, err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	repository := repositoryRoot(t)
	work := t.TempDir()
	buildDir := t.TempDir()
	serverRuntimeDir := t.TempDir()
	stagedBackendRepository := buildBackendFrontend(t, ctx, repository)

	image := os.Getenv("OPEN_SPLUNK_CLICKHOUSE_TEST_IMAGE")
	clickhouse, err := testsupport.StartClickHouse(ctx, image)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cleanupCancel()
		if err := clickhouse.Close(cleanupCtx); err != nil {
			t.Errorf("ClickHouse cleanup: %v", err)
		}
	})

	serverBinary := filepath.Join(buildDir, "open-splunk-server")
	collectorBinary := filepath.Join(buildDir, "open-splunk-collector")
	loggenBinary := filepath.Join(buildDir, "open-splunk-loggen")
	buildBinary(t, ctx, stagedBackendRepository, serverBinary, "./cmd/open-splunk-server")
	buildBinary(t, ctx, repository, collectorBinary, "./cmd/open-splunk-collector")
	buildBinary(t, ctx, repository, loggenBinary, "./cmd/open-splunk-loggen")

	httpAddress := unusedLoopbackAddress(t)
	collectorAddress := unusedLoopbackAddress(t)
	controlDBPath := filepath.Join(work, "control.sqlite")
	administratorTokenPath, administratorToken := provisionAdministratorToken(
		t,
		work,
	)
	assertEmptyDirectory(t, serverRuntimeDir)
	serverEnvironment := environmentWithValue(
		os.Environ(),
		"OPEN_SPLUNK_CLICKHOUSE_PASSWORD",
		clickhouse.Password,
	)
	serverEnvironment = environmentWithValue(
		serverEnvironment,
		"PATH",
		filepath.Join(serverRuntimeDir, "no-external-runtime"),
	)
	serverArguments := []string{
		serverBinary,
		"-http-address=" + httpAddress,
		"-control-db=" + controlDBPath,
		"-master-key=" + filepath.Join(work, "server.key"),
		"-administrator-token-file=" + administratorTokenPath,
		"-clickhouse-address=" + clickhouse.Address,
		"-clickhouse-database=" + clickhouse.Database,
		"-clickhouse-username=" + clickhouse.Username,
		"-collector-grpc-address=" + collectorAddress,
		"-collector-grpc-insecure",
		"-tenant-id=" + plan.TenantID,
	}
	var serverProcesses []*managedProcess
	startServer := func() *managedProcess {
		process := startProcess(t, serverRuntimeDir, serverArguments, serverEnvironment)
		serverProcesses = append(serverProcesses, process)
		return process
	}
	serverProcess := startServer()
	baseURL := "http://" + httpAddress
	httpClient := &http.Client{Timeout: 10 * time.Second}
	waitForHealth(t, ctx, httpClient, baseURL, serverProcess)

	createVerticalIndex(
		t,
		ctx,
		httpClient,
		baseURL,
		administratorToken,
		plan.IndexName,
		"Sustained backend load",
	)
	collectorStateDir := filepath.Join(work, "collector-state")
	collectorID := "backend-sustained-load-collector"
	writeCollectorIdentity(t, collectorStateDir, collectorID)
	plaintextToken := createBackendLoadToken(
		t,
		ctx,
		httpClient,
		baseURL,
		administratorToken,
		plan.IndexName,
		collectorID,
	)

	fixtureStart := time.Now().UTC().Add(-5 * time.Minute).Truncate(time.Millisecond)
	logPath := filepath.Join(work, "backend-load.ndjson")
	createEmptyFixture(t, logPath)
	tokenPath := filepath.Join(work, "collector.token")
	writePrivateFile(t, tokenPath, []byte(plaintextToken+"\n"))
	collectorConfig := filepath.Join(work, "collector.yaml")
	writePrivateFile(t, collectorConfig, []byte(backendLoadCollectorYAML(
		collectorAddress,
		tokenPath,
		collectorStateDir,
		logPath,
		plan.IndexName,
	)))
	collectorEnvironment := os.Environ()
	validateBackendLoadCollectorConfiguration(
		t,
		ctx,
		repository,
		collectorBinary,
		collectorConfig,
		collectorEnvironment,
		plaintextToken,
	)
	collectorArguments := []string{
		collectorBinary,
		"run",
		"-config", collectorConfig,
		"-log-level", "debug",
	}
	var collectorProcesses []*managedProcess
	startCollector := func() *managedProcess {
		process := startProcess(t, repository, collectorArguments, collectorEnvironment)
		collectorProcesses = append(collectorProcesses, process)
		return process
	}
	collectorProcess := startCollector()
	waitForCollectorDiscovery(t, ctx, collectorStateDir, logPath, collectorProcess, plaintextToken)

	storage, err := clickhousedriver.Open(&clickhousedriver.Options{
		Addr: []string{clickhouse.Address},
		Auth: clickhousedriver.Auth{
			Database: clickhouse.Database,
			Username: clickhouse.Username,
			Password: clickhouse.Password,
		},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = storage.Close() })

	sourceTracker := newBackendLoadSourceTracker(logPath)
	warmStartedAt := time.Now()
	warmLoggen := startProcess(
		t,
		repository,
		backendLoadLoggenArguments(
			loggenBinary,
			logPath,
			plan,
			plan.WarmEvents,
			backendLoadWarmSeed,
			fixtureStart,
		),
		os.Environ(),
	)
	if err := warmLoggen.Wait(backendLoadLegTimeout(plan.WarmEvents, plan)); err != nil {
		t.Fatalf("warm load generator: %v\nlogs:\n%s", err, warmLoggen.Logs())
	}
	warmFinishedAt := time.Now()
	warmDuration := warmFinishedAt.Sub(warmStartedAt)
	requireBackendLoadPacedDuration(t, "warm", plan.WarmEvents, plan.interval(), warmDuration)
	if _, err := sourceTracker.Finalize(plan.WarmEvents); err != nil {
		t.Fatalf("finalize warm backend load source: %v", err)
	}
	waitForBackendLoadStorage(
		t,
		ctx,
		storage,
		collectorProcess,
		plan.TenantID,
		plan.IndexName,
		plan.WarmEvents,
		45*time.Second,
		plaintextToken,
	)
	waitForCollectorCheckpoint(t, ctx, collectorStateDir, logPath, collectorProcess, plaintextToken)
	waitForCollectorWALAcknowledgedThroughCurrent(
		t,
		ctx,
		collectorStateDir,
		collectorProcess,
		plaintextToken,
	)
	warmOffset := uint64(mustFileSize(t, logPath))
	assertCollectorCheckpoint(t, collectorStateDir, warmOffset, plan.WarmEvents)
	assertBackendLoadDeadLetterEmpty(t, collectorStateDir)

	t.Logf("stopping backend server after %d acknowledged warm events", plan.WarmEvents)
	if err := serverProcess.Kill(10 * time.Second); err != nil {
		t.Fatalf("stop backend server for sustained outage: %v", err)
	}
	if !serverProcess.Exited() {
		t.Fatal("backend server did not exit before sustained outage")
	}
	httpClient.CloseIdleConnections()
	outageStartedAt := time.Now()
	outageCollectorLogOffset := len(collectorProcess.Logs())

	offlineStartedAt := time.Now()
	offlineLoggen := startProcess(
		t,
		repository,
		backendLoadLoggenArguments(
			loggenBinary,
			logPath,
			plan,
			plan.offlineGenerationEvents(),
			backendLoadMainSeed,
			fixtureStart.Add(time.Duration(plan.WarmEvents)*plan.interval()),
		),
		os.Environ(),
	)
	if err := offlineLoggen.Wait(backendLoadLegTimeout(plan.offlineGenerationEvents(), plan)); err != nil {
		t.Fatalf("offline load generator: %v\nlogs:\n%s", err, offlineLoggen.Logs())
	}
	offlineFinishedAt := time.Now()
	offlineDuration := offlineFinishedAt.Sub(offlineStartedAt)
	requireBackendLoadPacedDuration(
		t,
		"offline",
		plan.offlineGenerationEvents(),
		plan.interval(),
		offlineDuration,
	)
	offlineTarget := plan.WarmEvents + plan.offlineGenerationEvents()
	offlineSource, err := sourceTracker.Finalize(offlineTarget)
	if err != nil {
		t.Fatalf("finalize offline backend load source: %v", err)
	}
	offlineQueryContext, offlineQueryCancel := context.WithTimeout(ctx, 5*time.Second)
	offlineStorage, err := readBackendLoadStorageState(
		offlineQueryContext,
		storage,
		plan.TenantID,
		plan.IndexName,
	)
	offlineQueryCancel()
	if err != nil {
		t.Fatalf("read storage during sustained outage: %v", err)
	}
	if done, classifyErr := offlineStorage.classify(plan.WarmEvents); classifyErr != nil || !done {
		t.Fatalf(
			"storage advanced while backend server was down: state=%+v error=%v",
			offlineStorage,
			classifyErr,
		)
	}
	assertCollectorCheckpoint(t, collectorStateDir, warmOffset, plan.WarmEvents)
	queuedBeforeCrash := waitForBackendLoadDurableWALQueuedEvents(
		t,
		ctx,
		collectorProcess,
		outageCollectorLogOffset,
		plan.OfflineEvents,
		30*time.Second,
		plaintextToken,
	)
	assertBackendLoadDeadLetterEmpty(t, collectorStateDir)

	if err := collectorProcess.Kill(10 * time.Second); err != nil {
		t.Fatalf(
			"crash-stop collector with offline WAL backlog: %v\nlogs:\n%s",
			err,
			redactForFailure(collectorProcess.Logs(), plaintextToken),
		)
	}
	pendingWAL := assertBackendLoadPendingWAL(
		t,
		collectorStateDir,
		plan.OfflineEvents,
	)
	t.Logf(
		"backend load durable outage backlog: source_records=%d observed_appended_events=%d queued_batches=%d queued_events=%d queued_bytes=%d",
		offlineSource.Records-plan.WarmEvents,
		queuedBeforeCrash,
		pendingWAL.QueuedBatches,
		pendingWAL.QueuedEvents,
		pendingWAL.QueuedBytes,
	)

	serverProcess = startServer()
	waitForHealth(t, ctx, httpClient, baseURL, serverProcess)
	serverHealthyAt := time.Now()
	healthQueryContext, healthQueryCancel := context.WithTimeout(ctx, 5*time.Second)
	storageAtHealth, err := readBackendLoadStorageState(
		healthQueryContext,
		storage,
		plan.TenantID,
		plan.IndexName,
	)
	healthQueryCancel()
	if err != nil {
		t.Fatalf("read storage at backend recovery: %v", err)
	}
	sourceAtHealth, err := sourceTracker.Poll()
	if err != nil {
		t.Fatalf("read source progress at backend recovery: %v", err)
	}
	if done, classifyErr := storageAtHealth.classify(plan.WarmEvents); classifyErr != nil || !done {
		t.Fatalf(
			"storage advanced while both server and collector were stopped: state=%+v error=%v",
			storageAtHealth,
			classifyErr,
		)
	}
	recoveryStartedAt := time.Now()
	recoveryLoggen := startProcess(
		t,
		repository,
		backendLoadLoggenArguments(
			loggenBinary,
			logPath,
			plan,
			plan.recoveryEvents(),
			backendLoadRecoverySeed,
			fixtureStart.Add(time.Duration(offlineTarget)*plan.interval()),
		),
		os.Environ(),
	)
	collectorProcess = startCollector()
	recovery := waitForBackendLoadRecoveryWhileGenerating(
		t,
		ctx,
		storage,
		sourceTracker,
		collectorProcess,
		recoveryLoggen,
		plan,
		plan.recoveryTimeout(),
		plaintextToken,
	)
	concurrentSearchWindow := backendLoadConcurrentSearchHarness{
		storage:       storage,
		sourceTracker: sourceTracker,
		collector:     collectorProcess,
		generator:     recoveryLoggen,
		client:        httpClient,
		baseURL:       baseURL,
		plan:          plan,
		fixtureStart:  fixtureStart,
		secret:        plaintextToken,
	}.run(t, ctx)

	if err := recoveryLoggen.Wait(backendLoadLegTimeout(plan.recoveryEvents(), plan)); err != nil {
		t.Fatalf("recovery load generator: %v\nlogs:\n%s", err, recoveryLoggen.Logs())
	}
	mainFinishedAt := time.Now()
	recoveryDuration := mainFinishedAt.Sub(recoveryStartedAt)
	requireBackendLoadPacedDuration(
		t,
		"recovery",
		plan.recoveryEvents(),
		plan.interval(),
		recoveryDuration,
	)
	mainDuration := offlineDuration + recoveryDuration
	if _, err := sourceTracker.Finalize(plan.eventCount()); err != nil {
		t.Fatalf("finalize backend load source: %v", err)
	}
	source, err := readBackendLoadSourceCorpus(logPath, fixtureStart, plan)
	if err != nil {
		t.Fatalf("read backend load source corpus: %v", err)
	}
	assertBackendLoadSourceCardinality(t, plan, source)

	waitForBackendLoadStorage(
		t,
		ctx,
		storage,
		collectorProcess,
		plan.TenantID,
		plan.IndexName,
		plan.eventCount(),
		2*time.Minute,
		plaintextToken,
	)
	finalStoredAt := time.Now()
	waitForCollectorCheckpoint(t, ctx, collectorStateDir, logPath, collectorProcess, plaintextToken)
	waitForCollectorWALAcknowledgedThroughCurrent(
		t,
		ctx,
		collectorStateDir,
		collectorProcess,
		plaintextToken,
	)
	if err := collectorProcess.Interrupt(20 * time.Second); err != nil {
		t.Fatalf(
			"stop sustained-load collector: %v\nlogs:\n%s",
			err,
			redactForFailure(collectorProcess.Logs(), plaintextToken),
		)
	}
	assertDurableCollectorState(t, collectorStateDir, source.FileBytes, plan.eventCount())
	assertBackendLoadCheckpointDetails(t, collectorStateDir, logPath, plan.eventCount())
	assertBackendLoadDeadLetterEmpty(t, collectorStateDir)
	for index, process := range collectorProcesses {
		assertBackendLoadCollectorDiagnostics(t, process.Logs(), plaintextToken)
		assertManagedProcessLogsComplete(
			t,
			fmt.Sprintf("collector process %d", index+1),
			process,
			plaintextToken,
		)
	}

	firstSearch := runBackendLoadSearch(
		t,
		ctx,
		httpClient,
		baseURL,
		plan,
		fixtureStart,
		source,
	)
	repeatedSearch := runBackendLoadSearch(
		t,
		ctx,
		httpClient,
		baseURL,
		plan,
		fixtureStart,
		source,
	)
	metricsContext, metricsCancel := context.WithTimeout(ctx, 15*time.Second)
	metrics, err := readBackendLoadStorageMetrics(
		metricsContext,
		storage,
		plan.TenantID,
		plan.IndexName,
	)
	metricsCancel()
	if err != nil {
		t.Fatal(err)
	}
	assertBackendLoadStorageMetrics(t, plan, source, metrics)
	rawContext, rawCancel := context.WithTimeout(ctx, 30*time.Second)
	err = verifyBackendLoadRawRows(
		rawContext,
		storage,
		plan.TenantID,
		plan.IndexName,
		source.RawRecords,
	)
	rawCancel()
	if err != nil {
		t.Fatalf("verify backend load raw rows: %v", err)
	}

	if err := serverProcess.Interrupt(20 * time.Second); err != nil {
		t.Fatalf(
			"stop sustained-load server: %v\nlogs:\n%s",
			err,
			redactForFailure(
				serverProcess.Logs(),
				administratorToken,
				plaintextToken,
			),
		)
	}
	for index, process := range serverProcesses {
		assertManagedProcessLogsComplete(
			t,
			fmt.Sprintf("server process %d", index+1),
			process,
			administratorToken,
			plaintextToken,
		)
		assertProcessLogsDoNotLeak(
			t,
			process.Logs(),
			administratorToken,
			plaintextToken,
		)
	}

	activeGeneration := warmDuration + mainDuration
	acceptedWindow := finalStoredAt.Sub(warmStartedAt)
	outageDuration := serverHealthyAt.Sub(outageStartedAt)
	backlogAtHealth := backendLoadBacklog(sourceAtHealth.Records, storageAtHealth.Rows)
	backlogAtFirstRecovery := backendLoadBacklog(recovery.Source.Records, recovery.StoredRows)
	postGenerationDrain := max(time.Duration(0), finalStoredAt.Sub(mainFinishedAt))
	t.Logf(
		"backend load generation: events=%d source_bytes=%d active_duration=%s generated_eps=%.1f source_bytes_per_second=%.1f accepted_window=%s accepted_eps=%.1f",
		plan.eventCount(),
		source.FileBytes,
		activeGeneration,
		float64(plan.eventCount())/activeGeneration.Seconds(),
		float64(source.FileBytes)/activeGeneration.Seconds(),
		acceptedWindow,
		float64(plan.eventCount())/acceptedWindow.Seconds(),
	)
	t.Logf(
		"backend load outage/recovery: outage=%s backlog_at_health=%d backlog_at_first_recovery=%d first_recovery_after_health=%s post_generation_drain=%s",
		outageDuration,
		backlogAtHealth,
		backlogAtFirstRecovery,
		recovery.RecoveredAt.Sub(serverHealthyAt),
		postGenerationDrain,
	)
	t.Logf(
		"backend load storage: active_parts=%d part_rows=%d compressed_bytes=%d uncompressed_bytes=%d bytes_on_disk=%d compression_ratio=%.3f",
		metrics.ActiveParts,
		metrics.PartRows,
		metrics.CompressedBytes,
		metrics.UncompressedBytes,
		metrics.BytesOnDisk,
		float64(metrics.CompressedBytes)/float64(metrics.UncompressedBytes),
	)
	logBackendLoadSearchObservation(t, "first", firstSearch)
	logBackendLoadSearchObservation(t, "repeated", repeatedSearch)
	logBackendLoadConcurrentSearchWindow(t, concurrentSearchWindow)
}

func createBackendLoadToken(
	t *testing.T,
	ctx context.Context,
	client *http.Client,
	baseURL string,
	administratorToken string,
	indexName string,
	collectorID string,
) string {
	t.Helper()
	return createIndexScopedIngestionToken(
		t,
		ctx,
		client,
		baseURL,
		administratorToken,
		"backend-sustained-load-collector",
		indexName,
		collectorID,
	)
}

func backendLoadLoggenArguments(
	binary, output string,
	plan backendLoadPlan,
	count uint64,
	seed int64,
	start time.Time,
) []string {
	return []string{
		binary,
		"-count=" + strconv.FormatUint(count, 10),
		"-format=cardinality-json",
		"-seed=" + strconv.FormatInt(seed, 10),
		"-start=" + start.Format(time.RFC3339Nano),
		"-interval=" + plan.interval().String(),
		"-rate=" + strconv.FormatUint(plan.Rate, 10),
		"-output=" + output,
		"-append",
		"-flush-events=" + strconv.FormatUint(plan.FlushEvents, 10),
		"-service=backend-load",
		"-environment=integration",
		"-host=backend-load-host",
		"-cardinality=" + strconv.FormatUint(plan.Cardinality, 10),
	}
}

func backendLoadLegTimeout(count uint64, plan backendLoadPlan) time.Duration {
	return time.Duration(count-1)*plan.interval() + 30*time.Second
}

func requireBackendLoadPacedDuration(
	t *testing.T,
	name string,
	count uint64,
	interval, elapsed time.Duration,
) {
	t.Helper()
	minimum := time.Duration(count-1) * interval * 9 / 10
	if elapsed < minimum {
		t.Fatalf(
			"%s load generator completed in %s, want at least %s to prove pacing remained active",
			name,
			elapsed,
			minimum,
		)
	}
}

func waitForBackendLoadDurableWALQueuedEvents(
	t *testing.T,
	ctx context.Context,
	process *managedProcess,
	logOffset int,
	minimumQueuedEvents uint64,
	timeout time.Duration,
	plaintextToken string,
) uint64 {
	t.Helper()
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	var (
		last    uint64
		lastErr error
	)
	for {
		logs := process.Logs()
		if len(logs) < logOffset {
			lastErr = errors.New("collector log capture shrank")
		} else {
			last, lastErr = backendLoadDurableWALAppendedEvents(logs[logOffset:])
		}
		if lastErr == nil && last >= minimumQueuedEvents {
			return last
		}
		if process.Exited() {
			t.Fatalf(
				"collector exited before durably appending %d offline events: %v appended=%d error=%v\nlogs:\n%s",
				minimumQueuedEvents,
				process.Err(),
				last,
				lastErr,
				redactForFailure(process.Logs(), plaintextToken),
			)
		}
		select {
		case <-ctx.Done():
			t.Fatalf(
				"wait for durable backend load WAL backlog: %v appended=%d want=%d error=%v",
				ctx.Err(),
				last,
				minimumQueuedEvents,
				lastErr,
			)
		case <-deadline.C:
			t.Fatalf(
				"wait for durable backend load WAL backlog: timed out after %s appended=%d want=%d error=%v\nlogs:\n%s",
				timeout,
				last,
				minimumQueuedEvents,
				lastErr,
				redactForFailure(process.Logs(), plaintextToken),
			)
		case <-ticker.C:
		}
	}
}

func backendLoadDurableWALAppendedEvents(logs string) (uint64, error) {
	const (
		message = `level=DEBUG msg="collector: batch appended"`
		marker  = " events="
	)
	var total uint64
	for _, line := range strings.Split(logs, "\n") {
		if !strings.Contains(line, message) {
			continue
		}
		_, value, found := strings.Cut(line, marker)
		if !found {
			return 0, fmt.Errorf("backend load WAL append log lacks events: %q", line)
		}
		value, _, _ = strings.Cut(value, " ")
		events, err := strconv.ParseUint(value, 10, 64)
		if err != nil {
			return 0, fmt.Errorf("parse backend load WAL append events %q: %w", value, err)
		}
		if total > ^uint64(0)-events {
			return 0, errors.New("backend load WAL appended event count overflow")
		}
		total += events
	}
	return total, nil
}

func waitForBackendLoadRecoveryWhileGenerating(
	t *testing.T,
	ctx context.Context,
	connection clickhousedriver.Conn,
	tracker *backendLoadSourceTracker,
	collector, generator *managedProcess,
	plan backendLoadPlan,
	timeout time.Duration,
	plaintextToken string,
) backendLoadRecoveryObservation {
	t.Helper()
	waitContext, waitCancel := context.WithTimeout(ctx, timeout)
	defer waitCancel()
	expiresAt, _ := waitContext.Deadline()
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()
	var (
		source     backendLoadSourceProgress
		storedRows uint64
		lastErr    error
	)
	for {
		queryTimeout := min(5*time.Second, time.Until(expiresAt))
		queryContext, queryCancel := context.WithTimeout(waitContext, queryTimeout)
		storedRows, lastErr = readBackendLoadRowCount(
			queryContext,
			connection,
			plan.TenantID,
			plan.IndexName,
		)
		queryCancel()
		if lastErr == nil {
			if storedRows > plan.eventCount() {
				t.Fatalf(
					"backend load storage rows = %d during recovery, want at most %d",
					storedRows,
					plan.eventCount(),
				)
			}
			source, lastErr = tracker.Poll()
		}
		if generator.Exited() {
			t.Fatalf(
				"main load generator exited before stored rows recovered while generation was active: %v source=%+v stored_rows=%d error=%v\nlogs:\n%s",
				generator.Err(),
				source,
				storedRows,
				lastErr,
				generator.Logs(),
			)
		}
		if lastErr == nil && storedRows > plan.WarmEvents {
			if source.Records >= plan.eventCount() {
				t.Fatalf(
					"backend load recovery was first observed after source generation completed: source=%+v stored_rows=%d",
					source,
					storedRows,
				)
			}
			return backendLoadRecoveryObservation{
				RecoveredAt: time.Now(),
				Source:      source,
				StoredRows:  storedRows,
			}
		}
		if collector.Exited() {
			t.Fatalf(
				"collector exited before backend load recovery: %v source=%+v stored_rows=%d error=%v\nlogs:\n%s",
				collector.Err(),
				source,
				storedRows,
				lastErr,
				redactForFailure(collector.Logs(), plaintextToken),
			)
		}
		select {
		case <-waitContext.Done():
			t.Fatalf(
				"wait for backend load recovery after %s: %v source=%+v stored_rows=%d error=%v",
				timeout,
				waitContext.Err(),
				source,
				storedRows,
				lastErr,
			)
		case <-ticker.C:
		}
	}
}

func assertBackendLoadSourceCardinality(
	t *testing.T,
	plan backendLoadPlan,
	source backendLoadSourceCorpus,
) {
	t.Helper()
	if len(source.RequestIDs) != int(plan.eventCount()) {
		t.Fatalf(
			"backend load request cardinality = %d, want %d unique request IDs",
			len(source.RequestIDs),
			plan.eventCount(),
		)
	}
	minimumUsers := min(plan.Cardinality, plan.eventCount()) * 9 / 10
	if got := uint64(len(source.UserIDs)); got < minimumUsers || got > plan.Cardinality {
		t.Fatalf(
			"backend load user cardinality = %d, want [%d,%d]",
			got,
			minimumUsers,
			plan.Cardinality,
		)
	}
}

func backendLoadBacklog(sourceRecords, storedRows uint64) uint64 {
	if sourceRecords <= storedRows {
		return 0
	}
	return sourceRecords - storedRows
}

type backendLoadConcurrentSearchHarness struct {
	storage       clickhousedriver.Conn
	sourceTracker *backendLoadSourceTracker
	collector     *managedProcess
	generator     *managedProcess
	client        *http.Client
	baseURL       string
	plan          backendLoadPlan
	fixtureStart  time.Time
	secret        string
}

func (harness backendLoadConcurrentSearchHarness) run(
	t *testing.T,
	ctx context.Context,
) backendLoadConcurrentSearchWindow {
	t.Helper()
	requireBackendLoadProcessRunning(t, "recovery load generator", harness.generator)
	requireBackendLoadProcessRunning(
		t,
		"collector before concurrent searches",
		harness.collector,
		harness.secret,
	)
	storedBefore, sourceBefore := harness.sample(t, ctx, "before concurrent searches")
	if sourceBefore.Records >= harness.plan.eventCount() ||
		storedBefore <= harness.plan.WarmEvents ||
		storedBefore > sourceBefore.Records {
		t.Fatalf(
			"backend load was not actively recovering before concurrent searches: source=%+v stored_rows=%d total_events=%d",
			sourceBefore,
			storedBefore,
			harness.plan.eventCount(),
		)
	}

	window := backendLoadConcurrentSearchWindow{
		SourceBefore: sourceBefore,
		StoredBefore: storedBefore,
	}
	first := runBackendLoadConcurrentSearches(
		t,
		ctx,
		harness.client,
		harness.baseURL,
		harness.plan,
		harness.fixtureStart,
	)
	window.Cohorts = append(window.Cohorts, first)
	previousVisible := maximumBackendLoadCohortEvents(first)
	baselineVisible := previousVisible
	candidateStored := storedBefore
	ticker := time.NewTicker(backendLoadSearchRetryInterval)
	defer ticker.Stop()

	for len(window.Cohorts) < backendLoadMaximumSearchWaves {
		storedCandidate := harness.waitForCandidateProgress(
			t,
			ctx,
			ticker,
			sourceBefore,
			storedBefore,
			candidateStored,
		)
		candidateStored = storedCandidate
		next := runBackendLoadConcurrentSearches(
			t,
			ctx,
			harness.client,
			harness.baseURL,
			harness.plan,
			harness.fixtureStart,
		)
		nextVisible, err := validateBackendLoadCohortVisibility(previousVisible, next)
		if err != nil {
			t.Fatal(err)
		}
		window.Cohorts = append(window.Cohorts, next)
		storedAfter, sourceAfter := harness.sample(t, ctx, "after concurrent search cohort")
		window.StoredAfter = storedAfter
		window.SourceAfter = sourceAfter
		harness.requireActiveState(t, sourceBefore, storedBefore, sourceAfter, storedAfter)
		for wave, cohort := range window.Cohorts {
			for slot, search := range cohort.Searches {
				if search.Events > storedAfter {
					t.Fatalf(
						"concurrent backend load search %d/%d snapshot events = %d, want at most later physical rows %d",
						wave,
						slot,
						search.Events,
						storedAfter,
					)
				}
			}
		}
		if sourceAfter.Records > sourceBefore.Records && nextVisible > baselineVisible {
			return window
		}
		previousVisible = nextVisible
		candidateStored = max(candidateStored, storedAfter)
	}
	t.Fatalf(
		"concurrent backend load searches exhausted %d bounded waves without visible progress: source=%d->%d visible=%d->%d stored=%d->%d",
		backendLoadMaximumSearchWaves,
		sourceBefore.Records,
		window.SourceAfter.Records,
		baselineVisible,
		previousVisible,
		storedBefore,
		window.StoredAfter,
	)
	return backendLoadConcurrentSearchWindow{}
}

func backendLoadSearchProgressTimeout(plan backendLoadPlan, sourceRecords uint64) time.Duration {
	if sourceRecords >= plan.eventCount() {
		return 0
	}
	remaining := plan.eventCount() - sourceRecords
	interval := plan.interval()
	if remaining < 2 || interval <= 0 ||
		remaining-1 > uint64(math.MaxInt64)/uint64(interval) {
		return 0
	}
	activeWindow := time.Duration(remaining-1) * interval
	headroomEvents := plan.FlushEvents
	if headroomEvents > uint64(math.MaxInt64)/uint64(interval) {
		return 0
	}
	headroom := max(time.Second, time.Duration(headroomEvents)*interval)
	if activeWindow <= headroom {
		return 0
	}
	return activeWindow - headroom
}

func (harness backendLoadConcurrentSearchHarness) waitForCandidateProgress(
	t *testing.T,
	ctx context.Context,
	ticker *time.Ticker,
	sourceBefore backendLoadSourceProgress,
	storedBefore, candidateStored uint64,
) uint64 {
	t.Helper()
	stored, source := harness.sample(t, ctx, "before waiting for concurrent-search progress")
	harness.requireActiveState(t, sourceBefore, storedBefore, source, stored)
	if source.Records > sourceBefore.Records && stored > candidateStored {
		return stored
	}
	progressTimeout := backendLoadSearchProgressTimeout(harness.plan, source.Records)
	if progressTimeout <= 0 {
		t.Fatalf(
			"backend load has insufficient active source window for concurrent-search progress: source=%+v total_events=%d",
			source,
			harness.plan.eventCount(),
		)
	}
	progressContext, progressCancel := context.WithTimeout(ctx, progressTimeout)
	defer progressCancel()
	for {
		select {
		case <-progressContext.Done():
			t.Fatalf(
				"concurrent backend load search progress did not arrive before the active source window closed: source=%d->%d stored=%d->%d timeout=%s: %v",
				sourceBefore.Records,
				source.Records,
				storedBefore,
				stored,
				progressTimeout,
				progressContext.Err(),
			)
		case <-ticker.C:
		}
		stored, source = harness.sample(
			t,
			progressContext,
			"while waiting for concurrent-search progress",
		)
		harness.requireActiveState(t, sourceBefore, storedBefore, source, stored)
		if source.Records > sourceBefore.Records && stored > candidateStored {
			return stored
		}
	}
}

func (harness backendLoadConcurrentSearchHarness) requireActiveState(
	t *testing.T,
	sourceBefore backendLoadSourceProgress,
	storedBefore uint64,
	sourceAfter backendLoadSourceProgress,
	storedAfter uint64,
) {
	t.Helper()
	requireBackendLoadProcessRunning(t, "recovery load generator", harness.generator)
	requireBackendLoadProcessRunning(
		t,
		"collector during concurrent searches",
		harness.collector,
		harness.secret,
	)
	if sourceAfter.Records < sourceBefore.Records ||
		sourceAfter.Records >= harness.plan.eventCount() ||
		storedAfter < storedBefore ||
		storedAfter > sourceAfter.Records {
		t.Fatalf(
			"backend load did not remain active through concurrent searches: source_before=%+v source_after=%+v stored_before=%d stored_after=%d total_events=%d",
			sourceBefore,
			sourceAfter,
			storedBefore,
			storedAfter,
			harness.plan.eventCount(),
		)
	}
}

func (harness backendLoadConcurrentSearchHarness) sample(
	t *testing.T,
	ctx context.Context,
	phase string,
) (uint64, backendLoadSourceProgress) {
	t.Helper()
	queryContext, queryCancel := context.WithTimeout(ctx, 5*time.Second)
	stored, err := readBackendLoadRowCount(
		queryContext,
		harness.storage,
		harness.plan.TenantID,
		harness.plan.IndexName,
	)
	queryCancel()
	if err != nil {
		t.Fatalf("read storage %s: %v", phase, err)
	}
	source, err := harness.sourceTracker.Poll()
	if err != nil {
		t.Fatalf("read source %s: %v", phase, err)
	}
	return stored, source
}

func maximumBackendLoadCohortEvents(cohort backendLoadSearchCohort) uint64 {
	var maximum uint64
	for _, search := range cohort.Searches {
		maximum = max(maximum, search.Events)
	}
	return maximum
}

func validateBackendLoadCohortVisibility(
	previousMaximum uint64,
	cohort backendLoadSearchCohort,
) (uint64, error) {
	if len(cohort.Searches) == 0 {
		return 0, errors.New("concurrent backend load cohort has no searches")
	}
	minimum := cohort.Searches[0].Events
	maximum := minimum
	for _, search := range cohort.Searches[1:] {
		minimum = min(minimum, search.Events)
		maximum = max(maximum, search.Events)
	}
	if minimum < previousMaximum {
		return 0, fmt.Errorf(
			"concurrent backend load visibility regressed from %d to cohort range [%d,%d]",
			previousMaximum,
			minimum,
			maximum,
		)
	}
	return maximum, nil
}

func logBackendLoadSearchObservation(
	t *testing.T,
	name string,
	observation backendLoadSearchObservation,
) {
	t.Helper()
	t.Logf(
		"backend load %s search: job_id=%s lifecycle=%s elapsed=%s queue_wait=%s events=%d event_ids=%d request_ids=%d user_ids=%d event_times=%d first_event_at=%s last_event_at=%s scanned_rows=%d scanned_bytes=%d",
		name,
		observation.JobID,
		observation.LifecycleTime,
		observation.Elapsed,
		observation.QueueWait,
		observation.Events,
		observation.EventIDs,
		observation.RequestIDs,
		observation.UserIDs,
		observation.EventTimes,
		observation.FirstEventAt,
		observation.LastEventAt,
		observation.ScannedRows,
		observation.ScannedBytes,
	)
}

func requireBackendLoadProcessRunning(
	t *testing.T,
	name string,
	process *managedProcess,
	secrets ...string,
) {
	t.Helper()
	if !process.Exited() {
		return
	}
	t.Fatalf(
		"%s exited unexpectedly: %v\nlogs:\n%s",
		name,
		process.Err(),
		redactForFailure(process.Logs(), secrets...),
	)
}
