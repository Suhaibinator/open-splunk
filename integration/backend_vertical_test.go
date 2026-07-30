//go:build !windows

package integration_test

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	clickhousedriver "github.com/ClickHouse/clickhouse-go/v2"
	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/collector/input"
	"github.com/Suhaibinator/open-splunk/internal/collector/wal"
	"github.com/Suhaibinator/open-splunk/internal/loggen"
	"github.com/Suhaibinator/open-splunk/internal/testsupport"
	"github.com/Suhaibinator/open-splunk/internal/testsupport/gradethiscorpus"
	"github.com/gorilla/websocket"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
)

const (
	backendIntegrationFlag         = "OPEN_SPLUNK_BACKEND_INTEGRATION"
	verticalIndexName              = "vertical"
	verticalTenantID               = "vertical-tenant"
	verticalEventCount             = uint64(4)
	verticalTimelineMaximumBuckets = uint32(1_000)
	bulkIndexName                  = "vertical-bulk"
	bulkEventCount                 = uint64(10_001)
	redactionAPIKeySentinel        = "vertical-api-key-must-not-survive"
	redactionCookieSentinel        = "vertical-cookie-must-not-survive"
	redactionPrivateKeySentinel    = "vertical-private-key-must-not-survive"
	redactionCredentialSentinel    = "vertical-customer-credential-must-not-survive"
	redactionPINSentinel           = "vertical-customer-pin-must-not-survive"
	redactionCredentialMarker      = "[CREDENTIAL-MASKED]"
	redactionPINMarker             = "[PIN-MASKED]"
	verticalSentinelMessage        = "typed redaction sentinel"
	verticalSearchSPL              = " \nindex=vertical | dedup event_id | table _time message status duration_ms api_key customer_credential customer_pin _raw\t"
	browserVerticalSearchSPL       = "index=vertical | dedup event_id"
	bulkSearchSPL                  = "index=vertical-bulk | table event_id"
	splCompatibilityVersionForTest = "tier-1-dev"
	clickHouseEventInsertSQL       = "INSERT INTO open_splunk.events (event_id, tenant_id, index_name, event_time, index_time, " +
		"collected_at, event_time_source, host, source, sourcetype, service, severity, level, body, raw, " +
		"raw_encoding, trace_id, span_id, fields, field_names, collector_id, batch_id, batch_sequence, " +
		"expires_at, visibility_seq)"
)

// TestBackendVertical exercises the deployed backend boundary rather than a
// collection of in-process components:
//
//	HTTP protobuf provisioning -> collector file/WAL/gRPC -> ClickHouse ->
//	SPL job creation -> binary protobuf WebSocket progress/terminal events ->
//	opaque HTTP protobuf result pages -> compiled browser UI -> bounded export
//	re-execution -> one-time raw artifact download.
//
// It is opt-in because it builds the frontend and two binaries, starts a pinned
// Docker image, and launches a real browser.
func TestBackendVertical(t *testing.T) {
	if os.Getenv(backendIntegrationFlag) != "1" {
		t.Skip("set " + backendIntegrationFlag + "=1 to run the backend vertical integration test")
	}
	if _, err := exec.LookPath("docker"); err != nil {
		t.Fatalf("docker CLI is required when %s=1: %v", backendIntegrationFlag, err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 7*time.Minute)
	defer cancel()
	repository := repositoryRoot(t)
	work := t.TempDir()
	buildDir := t.TempDir()
	serverRuntimeDir := t.TempDir()
	stagedBackendRepository := buildBackendFrontend(t, ctx, repository)

	image := os.Getenv("OPEN_SPLUNK_CLICKHOUSE_TEST_IMAGE")
	clickhouse, err := testsupport.StartClickHouseWithServicePrincipals(ctx, image)
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
	buildBinary(t, ctx, stagedBackendRepository, serverBinary, "./cmd/open-splunk-server")
	buildBinary(t, ctx, repository, collectorBinary, "./cmd/open-splunk-collector")

	httpAddress := unusedLoopbackAddress(t)
	collectorAddress := unusedLoopbackAddress(t)
	controlDBPath := filepath.Join(work, "control.sqlite")
	administratorTokenPath, administratorToken := provisionAdministratorToken(
		t,
		work,
	)
	assertEmptyDirectory(t, serverRuntimeDir)
	serverEnvironment := clickHouseServerEnvironment(
		os.Environ(),
		clickhouse,
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
		"-collector-grpc-address=" + collectorAddress,
		"-collector-grpc-insecure",
		"-tenant-id=" + verticalTenantID,
	}
	serverArguments = append(
		serverArguments,
		clickHouseServerArguments(clickhouse)...,
	)
	serverProcess := startProcess(t, serverRuntimeDir, serverArguments, serverEnvironment)
	serverProcesses := []*managedProcess{serverProcess}
	baseURL := "http://" + httpAddress
	httpClient := &http.Client{Timeout: 10 * time.Second}
	waitForHealth(t, ctx, httpClient, baseURL, serverProcess)
	assertStandaloneServerSurface(t, ctx, httpClient, baseURL)

	var createdIndex opensplunkv1.CreateIndexResponse
	postAdministratorProto(
		t,
		ctx,
		httpClient,
		baseURL+"/api/v1/indexes/create",
		administratorToken,
		&opensplunkv1.CreateIndexRequest{
			Definition: &opensplunkv1.IndexDefinition{
				Name:            verticalIndexName,
				DisplayName:     "Backend vertical integration",
				RetentionPeriod: durationpb.New(24 * time.Hour),
				IngestionAccess: opensplunkv1.IndexAccessState_INDEX_ACCESS_STATE_ENABLED,
				SearchAccess:    opensplunkv1.IndexAccessState_INDEX_ACCESS_STATE_ENABLED,
			},
		},
		&createdIndex,
	)
	if createdIndex.GetIndex().GetVersion() != 1 || createdIndex.GetIndex().GetDefinition().GetName() != verticalIndexName {
		t.Fatalf("created index = %+v", createdIndex.GetIndex())
	}

	collectorStateDir := filepath.Join(work, "collector-state")
	collectorID := "backend-vertical-collector"
	writeCollectorIdentity(t, collectorStateDir, collectorID)
	plaintextToken := createIndexScopedIngestionToken(
		t,
		ctx,
		httpClient,
		baseURL,
		administratorToken,
		"backend-vertical-collector",
		verticalIndexName,
		collectorID,
	)
	serverSecrets := []string{
		administratorToken,
		plaintextToken,
		redactionAPIKeySentinel,
		redactionCookieSentinel,
		redactionPrivateKeySentinel,
		redactionCredentialSentinel,
		redactionPINSentinel,
	}

	fixtureStart := time.Now().UTC().Add(-2 * time.Minute).Truncate(time.Second)
	logPath := filepath.Join(work, "app.log")
	createEmptyFixture(t, logPath)
	tokenPath := filepath.Join(work, "collector.token")
	writePrivateFile(t, tokenPath, []byte(plaintextToken+"\n"))
	collectorConfig := filepath.Join(work, "collector.yaml")
	writePrivateFile(t, collectorConfig, []byte(collectorYAML(collectorAddress, tokenPath, collectorStateDir, logPath)))

	collectorArguments := []string{
		collectorBinary, "run", "-config", collectorConfig,
	}
	collectorProcess := startProcess(t, repository, collectorArguments, os.Environ())
	collectorProcesses := []*managedProcess{collectorProcess}
	waitForCollectorDiscovery(t, ctx, collectorStateDir, logPath, collectorProcess, plaintextToken)
	appendPrimerFixture(t, logPath, fixtureStart)

	storage, err := clickhousedriver.Open(&clickhousedriver.Options{
		Addr: []string{clickhouse.Address},
		Auth: clickhousedriver.Auth{
			Database: clickhouse.Database,
			Username: clickhouse.RuntimeUsername,
			Password: clickhouse.RuntimePassword,
		},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = storage.Close() })
	waitForStoredEventCount(t, ctx, storage, collectorProcess, plaintextToken, 1)
	waitForCollectorCheckpoint(t, ctx, collectorStateDir, logPath, collectorProcess, plaintextToken)
	primerOffset := uint64(mustFileSize(t, logPath))

	// Crash after the first durable acknowledgment, then reuse the same
	// checkpoint/WAL directory. The restarted collector must neither replay the
	// acknowledged line nor skip bytes appended after the restart.
	t.Log("crash-restarting collector after acknowledged primer")
	if err := collectorProcess.Kill(10 * time.Second); err != nil {
		t.Fatalf("crash collector after primer: %v", err)
	}
	assertCrashSafeAcknowledgedCollectorState(t, collectorStateDir, logPath, primerOffset)
	collectorProcess = startProcess(t, repository, collectorArguments, os.Environ())
	collectorProcesses = append(collectorProcesses, collectorProcess)

	appendGeneratedFixture(t, logPath, fixtureStart)
	waitForStoredEventCount(t, ctx, storage, collectorProcess, plaintextToken, verticalEventCount-1)
	waitForCollectorCheckpoint(t, ctx, collectorStateDir, logPath, collectorProcess, plaintextToken)
	waitForCollectorWALAcknowledgedThroughCurrent(
		t,
		ctx,
		collectorStateDir,
		collectorProcess,
		plaintextToken,
	)
	preServerRestartOffset := uint64(mustFileSize(t, logPath))

	// Crash the server only after three events are acknowledged. Append the
	// final event while it is down, wait until that event is durable in the
	// collector WAL, then crash the collector too. Restarting both processes
	// must retain the three acknowledged rows and deliver the one unacknowledged
	// batch exactly once through the stable batch/event IDs.
	t.Log("crash-restarting server with one offline collector batch")
	if err := serverProcess.Kill(10 * time.Second); err != nil {
		t.Fatalf("crash server: %v", err)
	}
	walBefore := snapshotCollectorWAL(t, collectorStateDir)
	appendSentinelFixture(t, logPath, fixtureStart)
	waitForCollectorWALAppend(t, ctx, collectorStateDir, walBefore, collectorProcess, plaintextToken)
	if err := collectorProcess.Kill(10 * time.Second); err != nil {
		t.Fatalf("crash collector with pending batch: %v", err)
	}
	assertPendingCollectorState(t, collectorStateDir, logPath, preServerRestartOffset)

	serverProcess = startProcess(t, serverRuntimeDir, serverArguments, serverEnvironment)
	serverProcesses = append(serverProcesses, serverProcess)
	waitForHealth(t, ctx, httpClient, baseURL, serverProcess)
	assertStandaloneServerSurface(t, ctx, httpClient, baseURL)

	collectorProcess = startProcess(t, repository, collectorArguments, os.Environ())
	collectorProcesses = append(collectorProcesses, collectorProcess)
	waitForDistinctStoredEventCount(
		t,
		ctx,
		storage,
		collectorProcess,
		verticalIndexName,
		verticalEventCount,
		1,
		plaintextToken,
		redactionAPIKeySentinel,
		redactionCookieSentinel,
		redactionPrivateKeySentinel,
		redactionCredentialSentinel,
		redactionPINSentinel,
	)
	waitForCollectorCheckpoint(t, ctx, collectorStateDir, logPath, collectorProcess, plaintextToken)

	if err := collectorProcess.Interrupt(15 * time.Second); err != nil {
		t.Fatalf("stop collector: %v\nlogs:\n%s", err, redactForFailure(
			collectorProcess.Logs(), plaintextToken,
			redactionAPIKeySentinel, redactionCookieSentinel, redactionPrivateKeySentinel,
			redactionCredentialSentinel, redactionPINSentinel,
		))
	}
	assertDurableCollectorState(t, collectorStateDir, uint64(mustFileSize(t, logPath)), verticalEventCount)
	visibilityCutoff := assertStoredEventBounds(t, ctx, storage, fixtureStart)
	assertRestartDeliveryAccounting(t, ctx, storage)
	assertBackendIndexStatistics(
		t,
		ctx,
		httpClient,
		baseURL,
		administratorToken,
		storage,
		createdIndex.GetIndex().GetIndexId(),
		visibilityCutoff,
	)
	assertBackendIndexFields(
		t,
		ctx,
		httpClient,
		baseURL,
		administratorToken,
		storage,
		createdIndex.GetIndex().GetIndexId(),
		fixtureStart,
	)
	for _, process := range collectorProcesses {
		assertProcessLogsDoNotLeak(t, process.Logs(), plaintextToken,
			redactionAPIKeySentinel, redactionCookieSentinel, redactionPrivateKeySentinel,
			redactionCredentialSentinel, redactionPINSentinel)
	}
	insertTimelineExclusiveBoundaryEvent(t, ctx, storage, fixtureStart.Add(5500*time.Millisecond), visibilityCutoff)

	search := runSearch(t, ctx, httpClient, baseURL, fixtureStart)
	assertCompletedTimeline(t, ctx, httpClient, baseURL, search.jobID, fixtureStart)
	assertTypedRedactedResults(t, search.results)
	assertBrowserVisibleResults(t, ctx, repository, baseURL, fixtureStart)
	for pageIndex, wire := range search.results.responseWire {
		for _, sentinel := range []string{
			redactionAPIKeySentinel,
			redactionCookieSentinel,
			redactionPrivateKeySentinel,
			redactionCredentialSentinel,
			redactionPINSentinel,
		} {
			if bytes.Contains(wire, []byte(sentinel)) {
				t.Fatalf("HTTP protobuf search response page %d leaked sentinel %q", pageIndex+1, sentinel)
			}
		}
	}
	completedExport, artifact, downloadToken := exportAndDownloadJSONLines(t, ctx, httpClient, baseURL, search.jobID,
		[]string{
			"message", "status", "duration_ms", "api_key",
			"customer_credential", "customer_pin", "_raw",
		},
		verticalEventCount,
	)
	serverSecrets = append(serverSecrets, downloadToken)
	assertDownloadedRedactedResults(t, completedExport, artifact)

	serverSecrets = append(serverSecrets, assertCurrentGradeThisMigration(
		t,
		ctx,
		repository,
		collectorBinary,
		collectorAddress,
		work,
		httpClient,
		baseURL,
		storage,
		administratorToken,
	))

	createBackendIndex(
		t,
		ctx,
		httpClient,
		baseURL,
		administratorToken,
		bulkIndexName,
		"Backend vertical bulk export",
	)
	bulkStart := insertBulkEvents(t, ctx, storage, visibilityCutoff)
	serverSecrets = append(serverSecrets, assertTruncatedPreviewExportsAllRows(
		t, ctx, httpClient, storage, baseURL, bulkStart, visibilityCutoff,
	))

	if err := serverProcess.Interrupt(20 * time.Second); err != nil {
		t.Fatalf("stop server: %v\nlogs:\n%s", err, redactForFailure(serverProcess.Logs(), serverSecrets...))
	}
	for _, process := range serverProcesses {
		assertProcessLogsDoNotLeak(t, process.Logs(), serverSecrets...)
	}
}

func waitForDistinctStoredEventCount(
	t *testing.T,
	ctx context.Context,
	connection clickhousedriver.Conn,
	process *managedProcess,
	indexName string,
	wantDistinct, maximumReplays uint64,
	secrets ...string,
) {
	t.Helper()
	deadline := time.NewTimer(30 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	var lastCount, lastDistinct uint64
	for {
		err := connection.QueryRow(ctx,
			`SELECT count(), uniqExact(event_id)
			 FROM open_splunk.events WHERE tenant_id = ? AND index_name = ?`,
			verticalTenantID, indexName,
		).Scan(&lastCount, &lastDistinct)
		maximumCount := wantDistinct + maximumReplays
		if err == nil && lastDistinct == wantDistinct &&
			lastCount >= wantDistinct && lastCount <= maximumCount {
			return
		}
		if err == nil && (lastDistinct > wantDistinct || lastCount > maximumCount ||
			lastCount < lastDistinct || lastCount-lastDistinct > maximumReplays) {
			t.Fatalf(
				"stored %q events = %d distinct=%d, want %d distinct and at most %d replay(s)",
				indexName,
				lastCount,
				lastDistinct,
				wantDistinct,
				maximumReplays,
			)
		}
		if process.Exited() {
			t.Fatalf(
				"collector exited before %q ingestion completed: %v, count=%d distinct=%d\nlogs:\n%s",
				indexName,
				process.Err(),
				lastCount,
				lastDistinct,
				redactForFailure(process.Logs(), secrets...),
			)
		}
		select {
		case <-ctx.Done():
			t.Fatalf(
				"wait for distinct %q events: %v, count=%d distinct=%d",
				indexName,
				ctx.Err(),
				lastCount,
				lastDistinct,
			)
		case <-deadline.C:
			t.Fatalf(
				"wait for distinct %q events: timed out, count=%d distinct=%d\ncollector logs:\n%s",
				indexName,
				lastCount,
				lastDistinct,
				redactForFailure(process.Logs(), secrets...),
			)
		case <-ticker.C:
		}
	}
}

type completedSearch struct {
	jobID   string
	results *collectedVerticalSearchResults
}

type collectedVerticalSearchResults struct {
	schema       *opensplunkv1.ResultSchema
	rows         []*opensplunkv1.ResultRow
	responseWire [][]byte
}

func assertEmptyDirectory(t *testing.T, directory string) {
	t.Helper()
	entries, err := os.ReadDir(directory)
	if err != nil {
		t.Fatalf("read standalone server working directory: %v", err)
	}
	if len(entries) != 0 {
		names := make([]string, len(entries))
		for index, entry := range entries {
			names[index] = entry.Name()
		}
		t.Fatalf("standalone server working directory is not empty: %v", names)
	}
}

func assertBrowserVisibleResults(
	t *testing.T,
	ctx context.Context,
	repository, baseURL string,
	fixtureStart time.Time,
) {
	t.Helper()
	browserContext, cancel := context.WithTimeout(ctx, 90*time.Second)
	defer cancel()
	// #nosec G204 -- the executable is the repository-owned Playwright binary,
	// and every argument is a fixed test path or flag; no shell is involved.
	command := exec.CommandContext(
		browserContext,
		filepath.Join(repository, "node_modules", ".bin", "playwright"),
		"test",
		"integration/browser_vertical.spec.ts",
		"--workers=1",
		"--reporter=line",
		"--output="+filepath.Join(repository, "test-results", "backend-vertical"),
	)
	configureProcessGroup(command)
	command.Dir = repository
	environment := os.Environ()
	for name, value := range map[string]string{
		"OPEN_SPLUNK_E2E_BASE_URL":      baseURL,
		"OPEN_SPLUNK_E2E_SPL":           browserVerticalSearchSPL,
		"OPEN_SPLUNK_E2E_EARLIEST":      fixtureStart.Format(time.RFC3339Nano),
		"OPEN_SPLUNK_E2E_LATEST":        fixtureStart.Add(4 * time.Second).Format(time.RFC3339Nano),
		"OPEN_SPLUNK_E2E_EXPECTED_TEXT": verticalSentinelMessage,
		"OPEN_SPLUNK_E2E_EXPECTED_ROWS": strconv.FormatUint(verticalEventCount, 10),
	} {
		environment = environmentWithValue(environment, name, value)
	}
	command.Env = environment
	logs := &lockedBuffer{maximum: 1 << 20}
	command.Stdout = logs
	command.Stderr = logs
	err := command.Run()
	if err != nil {
		t.Fatalf("verify browser-visible backend result: %v\n%s", err, logs.String())
	}
}

func assertStandaloneServerSurface(t *testing.T, ctx context.Context, client *http.Client, baseURL string) {
	t.Helper()
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, baseURL+"/", nil)
	if err != nil {
		t.Fatal(err)
	}
	response, err := client.Do(request)
	if err != nil {
		t.Fatalf("load embedded UI: %v", err)
	}
	const maximumHTMLBytes = int64(2 << 20)
	body, readErr := io.ReadAll(io.LimitReader(response.Body, maximumHTMLBytes+1))
	closeErr := response.Body.Close()
	if readErr != nil {
		t.Fatalf("read embedded UI: %v", readErr)
	}
	if closeErr != nil {
		t.Fatalf("close embedded UI response: %v", closeErr)
	}
	bodyPreview := body[:min(len(body), 512)]
	if response.StatusCode != http.StatusOK {
		t.Fatalf("embedded UI status = %d body prefix = %q", response.StatusCode, bodyPreview)
	}
	if int64(len(body)) > maximumHTMLBytes {
		t.Fatalf("embedded UI exceeded %d bytes", maximumHTMLBytes)
	}
	mediaType, _, err := mime.ParseMediaType(response.Header.Get("Content-Type"))
	if err != nil || mediaType != "text/html" {
		t.Fatalf("embedded UI Content-Type = %q (parse error %v)", response.Header.Get("Content-Type"), err)
	}
	if !bytes.Contains(body, []byte("<title>Home | Open Splunk</title>")) {
		t.Fatalf("embedded UI did not contain the expected title; body prefix = %q", bodyPreview)
	}
	if !bytes.Contains(body, []byte("Backend mode selected")) ||
		bytes.Contains(body, []byte("Demo workspace ready")) {
		t.Fatal("embedded release UI is not configured for backend data")
	}

	var bootstrap opensplunkv1.GetSystemBootstrapResponse
	postProto(t, ctx, client, baseURL+"/api/v1/system/bootstrap", &opensplunkv1.GetSystemBootstrapRequest{}, &bootstrap)
	limits := bootstrap.GetLimits()
	timelineFeatures := 0
	previewFeatures := 0
	for _, feature := range bootstrap.GetFeatures() {
		switch feature {
		case opensplunkv1.ServerFeature_SERVER_FEATURE_TIMELINE:
			timelineFeatures++
		case opensplunkv1.ServerFeature_SERVER_FEATURE_SEARCH_PREVIEW:
			previewFeatures++
		}
	}
	build := bootstrap.GetBuild()
	if bootstrap.GetServerVersion() != integrationApplicationVersion+" ("+integrationSourceRevision+")" ||
		bootstrap.GetApiVersion() != "v1" ||
		bootstrap.GetSplCompatibilityVersion() != splCompatibilityVersionForTest ||
		bootstrap.GetSearchWebsocketPath() != "/api/v1/search/ws" ||
		build.GetApplicationVersion() != integrationApplicationVersion ||
		build.GetSourceRevision() != integrationSourceRevision ||
		build.GetUiBuildId() != integrationUIBuildID ||
		build.GetUiSha256() == "" || build.GetProtobufSchemaSha256() == "" ||
		build.GetSqliteMigrationsSha256() == "" || build.GetSqliteMigrationVersion() == 0 ||
		build.GetClickhouseMigrationsSha256() == "" || build.GetClickhouseMigrationVersion() == 0 ||
		build.GetAssetManifestFormatVersion() != 1 ||
		limits.GetMaximumPreviewRows() == 0 || limits.GetMaximumWebsocketSubscriptions() == 0 ||
		limits.GetMaximumWebsocketFrameBytes() < 1<<10 || limits.GetMaximumWebsocketFrameBytes() > 1<<20 ||
		timelineFeatures != 1 || previewFeatures != 1 ||
		limits.GetMaximumTimelineBuckets() != verticalTimelineMaximumBuckets {
		t.Fatalf("standalone bootstrap response = %+v", &bootstrap)
	}
}

func waitForHealth(
	t *testing.T,
	ctx context.Context,
	client *http.Client,
	baseURL string,
	process *managedProcess,
	protectedValues ...string,
) {
	t.Helper()
	deadline := time.NewTimer(45 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	var last error
	for {
		request, err := http.NewRequestWithContext(ctx, http.MethodGet, baseURL+"/healthz", nil)
		if err != nil {
			t.Fatal(err)
		}
		response, err := client.Do(request)
		if err == nil {
			body, readErr := io.ReadAll(io.LimitReader(response.Body, 64))
			_ = response.Body.Close()
			if readErr == nil && response.StatusCode == http.StatusOK && string(body) == "ok\n" {
				return
			}
			last = fmt.Errorf("status %d body %q read error %w", response.StatusCode, body, readErr)
		} else {
			last = err
		}
		if process.Exited() {
			t.Fatalf(
				"server exited before health check: %v (last: %s)\nlogs:\n%s",
				process.Err(),
				redactForFailure(fmt.Sprint(last), protectedValues...),
				redactForFailure(process.Logs(), protectedValues...),
			)
		}
		select {
		case <-ctx.Done():
			t.Fatalf(
				"wait for server health: %v (last: %s)\nlogs:\n%s",
				ctx.Err(),
				redactForFailure(fmt.Sprint(last), protectedValues...),
				redactForFailure(process.Logs(), protectedValues...),
			)
		case <-deadline.C:
			t.Fatalf(
				"wait for server health: timed out (last: %s)\nlogs:\n%s",
				redactForFailure(fmt.Sprint(last), protectedValues...),
				redactForFailure(process.Logs(), protectedValues...),
			)
		case <-ticker.C:
		}
	}
}

func createIndexScopedIngestionToken(
	t *testing.T,
	ctx context.Context,
	client *http.Client,
	baseURL string,
	administratorToken string,
	name string,
	indexName string,
	collectorID string,
) string {
	t.Helper()
	var created opensplunkv1.CreateIngestionTokenResponse
	postAdministratorProto(
		t,
		ctx,
		client,
		baseURL+"/api/v1/ingestion-tokens/create",
		administratorToken,
		&opensplunkv1.CreateIngestionTokenRequest{
			Definition: &opensplunkv1.IngestionTokenDefinition{
				Name: name,
				Constraints: &opensplunkv1.IngestionTokenConstraints{
					AllowedIndexNames: []string{indexName},
					BoundCollectorId:  &collectorID,
				},
			},
		},
		&created,
	)
	plaintext := created.GetPlaintextToken()
	metadata := created.GetIngestionToken()
	if plaintext == "" || metadata.GetVersion() != 1 ||
		metadata.GetConstraints().GetBoundCollectorId() != collectorID ||
		!strings.HasPrefix(plaintext, metadata.GetTokenPrefix()) {
		t.Fatalf(
			"created ingestion token %q metadata = %+v, plaintext length = %d",
			name,
			metadata,
			len(plaintext),
		)
	}
	return plaintext
}

func assertCurrentGradeThisMigration(
	t *testing.T,
	ctx context.Context,
	repository, collectorBinary, collectorAddress, work string,
	client *http.Client,
	baseURL string,
	connection clickhousedriver.Conn,
	administratorToken string,
) string {
	t.Helper()

	profile := gradethiscorpus.MigrationFixtureAt(
		time.Now().UTC().Add(-15 * time.Minute).Truncate(time.Second),
	)
	if err := gradethiscorpus.ValidateMigration(profile); err != nil {
		t.Fatalf("validate current GradeThis migration fixture: %v", err)
	}
	createBackendIndex(
		t,
		ctx,
		client,
		baseURL,
		administratorToken,
		gradethiscorpus.MigrationIndexName,
		"Current GradeThis collector migration",
	)

	stateDir := filepath.Join(work, "gradethis-current-state")
	collectorID := "gradethis-current-migration"
	writeCollectorIdentity(t, stateDir, collectorID)
	plaintextToken := createIndexScopedIngestionToken(
		t,
		ctx,
		client,
		baseURL,
		administratorToken,
		"gradethis-current-migration",
		gradethiscorpus.MigrationIndexName,
		collectorID,
	)

	logPath := filepath.Join(work, "gradethis-current.log")
	createEmptyFixture(t, logPath)
	tokenPath := filepath.Join(work, "gradethis-current.token")
	writePrivateFile(t, tokenPath, []byte(plaintextToken+"\n"))
	configPath := filepath.Join(repository, "configs", "examples", "collector.yaml")
	environment := os.Environ()
	for name, value := range map[string]string{
		"OPEN_SPLUNK_SERVER_GRPC_ADDRESS":       collectorAddress,
		"OPEN_SPLUNK_COLLECTOR_TOKEN_FILE":      tokenPath,
		"OPEN_SPLUNK_COLLECTOR_STATE_DIRECTORY": stateDir,
		"GRADETHIS_LOG_PATH":                    logPath,
		"GRADETHIS_HOST":                        "gradethis-integration-host",
		"GRADETHIS_ENVIRONMENT":                 "integration",
	} {
		environment = environmentWithValue(environment, name, value)
	}

	validateCollectorConfiguration(
		t,
		ctx,
		repository,
		collectorBinary,
		configPath,
		environment,
		plaintextToken,
	)
	process := startProcess(
		t,
		repository,
		[]string{collectorBinary, "run", "-config", configPath},
		environment,
	)
	waitForCollectorDiscovery(t, ctx, stateDir, logPath, process, plaintextToken)
	appendSyncedFixture(t, logPath, profile.NDJSON)

	wantRows := uint64(len(profile.Events))
	waitForDistinctStoredEventCount(
		t,
		ctx,
		connection,
		process,
		gradethiscorpus.MigrationIndexName,
		wantRows,
		0,
		plaintextToken,
	)
	waitForCollectorCheckpoint(t, ctx, stateDir, logPath, process, plaintextToken)
	waitForCollectorWALAcknowledgedThroughCurrent(t, ctx, stateDir, process, plaintextToken)
	if err := process.Interrupt(15 * time.Second); err != nil {
		t.Fatalf(
			"stop current GradeThis collector: %v\nlogs:\n%s",
			err,
			redactForFailure(process.Logs(), plaintextToken),
		)
	}
	assertDurableCollectorState(t, stateDir, uint64(mustFileSize(t, logPath)), wantRows)
	assertCurrentGradeThisStoredMetadata(t, ctx, connection, wantRows)
	assertProcessLogsDoNotLeak(t, process.Logs(), plaintextToken)
	assertCurrentGradeThisSearches(t, ctx, client, baseURL, profile)

	return plaintextToken
}

func validateCollectorConfiguration(
	t *testing.T,
	ctx context.Context,
	repository, collectorBinary, configPath string,
	environment []string,
	plaintextToken string,
) {
	t.Helper()
	validateCollectorConfigurationWithInput(
		t,
		ctx,
		repository,
		collectorBinary,
		configPath,
		environment,
		plaintextToken,
		"shipped GradeThis",
		"gradethis-backend",
	)
}

func validateCollectorConfigurationWithInput(
	t *testing.T,
	ctx context.Context,
	repository, collectorBinary, configPath string,
	environment []string,
	plaintextToken, description, expectedInputID string,
) {
	t.Helper()
	command := exec.CommandContext(ctx, collectorBinary, "validate", "-config", configPath)
	configureProcessGroup(command)
	command.Dir = repository
	command.Env = environment
	logs := &lockedBuffer{maximum: 1 << 20}
	command.Stdout = logs
	command.Stderr = logs
	if err := command.Run(); err != nil {
		t.Fatalf(
			"validate %s collector configuration: %v\n%s",
			description,
			err,
			redactForFailure(logs.String(), plaintextToken),
		)
	}
	output := logs.String()
	if logs.Truncated() {
		t.Fatalf(
			"%s collector validation output exceeded the capture limit:\n%s",
			description,
			redactForFailure(output, plaintextToken),
		)
	}
	if !strings.Contains(output, "configuration "+configPath+" is valid") ||
		!strings.Contains(output, expectedInputID+": 1 file(s)") {
		t.Fatalf(
			"collector validation output did not prove one %s source:\n%s",
			description,
			redactForFailure(output, plaintextToken),
		)
	}
	assertProcessLogsDoNotLeak(t, output, plaintextToken)
}

func appendSyncedFixture(t *testing.T, path string, contents []byte) {
	t.Helper()
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatalf("open collector fixture for append: %v", err)
	}
	written, writeErr := file.Write(contents)
	if writeErr != nil || written != len(contents) {
		_ = file.Close()
		t.Fatalf("append collector fixture: wrote %d of %d bytes: %v", written, len(contents), writeErr)
	}
	syncAndCloseFixture(t, file)
}

func assertCurrentGradeThisStoredMetadata(
	t *testing.T,
	ctx context.Context,
	connection clickhousedriver.Conn,
	wantRows uint64,
) {
	t.Helper()
	var count, distinct, canonicalMismatches, environmentMismatches, rawMetadata uint64
	err := connection.QueryRow(
		ctx,
		`SELECT
			count(),
			uniqExact(event_id),
			countIf(
				host != ?
				OR source != ?
				OR sourcetype != ?
				OR ifNull(service, '') != ?
			),
			countIf(
				NOT has(field_names, 'environment')
				OR dynamicType(fields.environment) != 'String'
				OR dynamicElement(fields.environment, 'String') != ?
			),
			countIf(
				position(raw, '"index":') > 0
				OR position(raw, '"host":') > 0
				OR position(raw, '"source":') > 0
				OR position(raw, '"sourcetype":') > 0
				OR position(raw, '"service":') > 0
				OR position(raw, '"environment":') > 0
			)
		 FROM open_splunk.events
		 WHERE tenant_id = ? AND index_name = ?`,
		"gradethis-integration-host",
		gradethiscorpus.MigrationSource,
		gradethiscorpus.MigrationSourcetype,
		gradethiscorpus.MigrationService,
		"integration",
		verticalTenantID,
		gradethiscorpus.MigrationIndexName,
	).Scan(&count, &distinct, &canonicalMismatches, &environmentMismatches, &rawMetadata)
	if err != nil {
		t.Fatalf("query current GradeThis stored metadata: %v", err)
	}
	if count != wantRows || distinct != wantRows || canonicalMismatches != 0 ||
		environmentMismatches != 0 || rawMetadata != 0 {
		t.Fatalf(
			"current GradeThis storage = count:%d distinct:%d canonical_mismatches:%d "+
				"environment_mismatches:%d raw_metadata:%d, want %d/%d/0/0/0",
			count,
			distinct,
			canonicalMismatches,
			environmentMismatches,
			rawMetadata,
			wantRows,
			wantRows,
		)
	}
}

func assertCurrentGradeThisSearches(
	t *testing.T,
	ctx context.Context,
	client *http.Client,
	baseURL string,
	profile gradethiscorpus.MigrationProfile,
) {
	t.Helper()
	for _, search := range gradethiscorpus.MigrationSearches() {
		search := search
		t.Run("current-gradethis-"+string(search.ID), func(t *testing.T) {
			source, err := search.Render(profile.TraceID)
			if err != nil {
				t.Fatal(err)
			}
			results := runCurrentGradeThisSearch(
				t,
				ctx,
				client,
				baseURL,
				profile.BaseTime,
				source,
				search.ExpectedRows,
			)
			assertCurrentGradeThisSearchResults(t, search.ID, profile, results)
		})
	}
}

func runCurrentGradeThisSearch(
	t *testing.T,
	ctx context.Context,
	client *http.Client,
	baseURL string,
	baseTime time.Time,
	source string,
	wantRows uint64,
) *collectedVerticalSearchResults {
	t.Helper()
	earliest := baseTime.Format(time.RFC3339Nano)
	latestTime := baseTime.Add(12 * time.Minute)
	latest := latestTime.Format(time.RFC3339Nano)
	timezone := "UTC"
	var created opensplunkv1.CreateSearchJobResponse
	postProto(t, ctx, client, baseURL+"/api/v1/search/jobs/create", &opensplunkv1.CreateSearchJobRequest{
		Definition: &opensplunkv1.SearchDefinition{
			Spl: source,
			TimeRange: &opensplunkv1.TimeRangeSpec{
				Earliest: &earliest,
				Latest:   &latest,
				Timezone: &timezone,
			},
			IndexScope: []string{gradethiscorpus.MigrationIndexName},
		},
	}, &created)
	jobID := created.GetSearchJob().GetSearchJobId()
	if jobID == "" {
		t.Fatalf("created current GradeThis search job = %+v", created.GetSearchJob())
	}
	completed := waitForCompletedSearch(t, ctx, client, baseURL, jobID, 30*time.Second)
	resolved := completed.GetResolvedTimeRange()
	if completed.GetDefinition().GetSpl() != source ||
		completed.GetProgress().GetProducedRows() != wantRows ||
		completed.GetResultsTruncated() ||
		len(completed.GetEffectiveIndexScope()) != 1 ||
		completed.GetEffectiveIndexScope()[0] != gradethiscorpus.MigrationIndexName ||
		resolved == nil ||
		resolved.GetEarliest() == nil ||
		resolved.GetLatest() == nil ||
		!resolved.GetEarliest().AsTime().Equal(baseTime) ||
		!resolved.GetLatest().AsTime().Equal(latestTime) ||
		resolved.GetTimezone() != timezone {
		t.Fatalf("completed current GradeThis search = %+v", completed)
	}
	return fetchAllCompletedSearchResults(t, ctx, client, baseURL, jobID, wantRows, 3)
}

func exportAndDownloadJSONLines(
	t *testing.T,
	ctx context.Context,
	client *http.Client,
	baseURL, searchJobID string,
	columns []string,
	expectedRows uint64,
) (*opensplunkv1.ExportJob, []byte, string) {
	t.Helper()
	// Leave headroom so a broken snapshot predicate cannot hide extra rows
	// behind a limit equal to the expected cardinality.
	rowLimit := expectedRows + 32
	byteLimit := uint64(16 << 20)
	var created opensplunkv1.CreateExportJobResponse
	postProto(t, ctx, client, baseURL+"/api/v1/search/exports/create", &opensplunkv1.CreateExportJobRequest{
		Definition: &opensplunkv1.ExportDefinition{
			SearchJobId: searchJobID,
			Columns:     append([]string(nil), columns...),
			RowLimit:    &rowLimit,
			ByteLimit:   &byteLimit,
			FormatOptions: &opensplunkv1.ExportDefinition_JsonLines{JsonLines: &opensplunkv1.JsonLinesExportOptions{
				IntegerEncoding: opensplunkv1.JsonIntegerEncoding_JSON_INTEGER_ENCODING_NUMBER_WHEN_SAFE,
			}},
		},
	}, &created)
	exportID := created.GetExportJob().GetExportJobId()
	if exportID == "" || created.GetExportJob().GetDefinition().GetSearchJobId() != searchJobID ||
		created.GetExportJob().GetFormat() != opensplunkv1.ExportFormat_EXPORT_FORMAT_JSON_LINES {
		t.Fatalf("created export job = %+v", created.GetExportJob())
	}

	completed := waitForCompletedExport(t, ctx, client, baseURL, exportID)
	if completed.GetArtifact().GetRowCount() != expectedRows || completed.GetProgress().GetRowsWritten() != expectedRows ||
		completed.GetArtifact().GetSizeBytes() == 0 || completed.GetProgress().GetBytesWritten() != completed.GetArtifact().GetSizeBytes() {
		t.Fatalf("completed export job = %+v", completed)
	}

	var granted opensplunkv1.GetExportJobResponse
	postProto(t, ctx, client, baseURL+"/api/v1/search/exports/get", &opensplunkv1.GetExportJobRequest{
		ExportJobId:        exportID,
		IssueDownloadGrant: true,
	}, &granted)
	grant := granted.GetDownloadGrant()
	if granted.GetExportJob().GetState() != opensplunkv1.ExportJobState_EXPORT_JOB_STATE_COMPLETED ||
		grant.GetDownloadPath() != "/api/v1/search/exports/download" || grant.GetDownloadToken() == "" ||
		grant.GetExpiresAt() == nil || !grant.GetExpiresAt().AsTime().After(time.Now().UTC()) {
		t.Fatalf("granted completed export = %+v", granted.GetExportJob())
	}
	artifact := downloadGrantedArtifact(t, ctx, client, baseURL, granted.GetExportJob().GetArtifact(), grant)
	return granted.GetExportJob(), artifact, grant.GetDownloadToken()
}

func waitForCompletedExport(t *testing.T, ctx context.Context, client *http.Client, baseURL, exportID string) *opensplunkv1.ExportJob {
	t.Helper()
	deadline := time.NewTimer(90 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		var got opensplunkv1.GetExportJobResponse
		postProto(t, ctx, client, baseURL+"/api/v1/search/exports/get", &opensplunkv1.GetExportJobRequest{ExportJobId: exportID}, &got)
		job := got.GetExportJob()
		switch job.GetState() {
		case opensplunkv1.ExportJobState_EXPORT_JOB_STATE_COMPLETED:
			return job
		case opensplunkv1.ExportJobState_EXPORT_JOB_STATE_FAILED,
			opensplunkv1.ExportJobState_EXPORT_JOB_STATE_CANCELED,
			opensplunkv1.ExportJobState_EXPORT_JOB_STATE_EXPIRED:
			t.Fatalf("export job terminated in %s: %+v", job.GetState(), job.GetFailure())
		}
		select {
		case <-ctx.Done():
			t.Fatalf("wait for export: %v", ctx.Err())
		case <-deadline.C:
			t.Fatal("wait for export: timed out")
		case <-ticker.C:
		}
	}
}

func downloadGrantedArtifact(
	t *testing.T,
	ctx context.Context,
	client *http.Client,
	baseURL string,
	artifact *opensplunkv1.ExportArtifact,
	grant *opensplunkv1.ExportDownloadGrant,
) []byte {
	t.Helper()
	if artifact == nil || grant == nil || artifact.GetSizeBytes() > 16<<20 {
		t.Fatalf("invalid downloadable artifact: artifact present=%t grant present=%t size=%d",
			artifact != nil, grant != nil, artifact.GetSizeBytes())
	}
	if artifact.GetMediaType() != "application/x-ndjson; charset=utf-8" ||
		!strings.HasSuffix(artifact.GetFileName(), ".jsonl") {
		t.Fatalf("JSON Lines artifact metadata = type %q filename %q", artifact.GetMediaType(), artifact.GetFileName())
	}
	downloadURL := baseURL + grant.GetDownloadPath()
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, downloadURL, nil)
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Authorization", "Bearer "+grant.GetDownloadToken())
	response, err := client.Do(request)
	if err != nil {
		t.Fatalf("download export artifact: %v", err)
	}
	body, readErr := io.ReadAll(io.LimitReader(response.Body, int64(artifact.GetSizeBytes())+1))
	closeErr := response.Body.Close()
	if readErr != nil || closeErr != nil {
		t.Fatalf("read export artifact: read=%v close=%v", readErr, closeErr)
	}
	if response.StatusCode != http.StatusOK {
		t.Fatalf("download status = %d, body = %q", response.StatusCode, body)
	}
	if uint64(len(body)) != artifact.GetSizeBytes() || response.ContentLength != int64(artifact.GetSizeBytes()) {
		t.Fatalf("download size = body %d header %d, want %d", len(body), response.ContentLength, artifact.GetSizeBytes())
	}
	if contentType := response.Header.Get("Content-Type"); contentType != "application/x-ndjson; charset=utf-8" {
		t.Fatalf("download content type = %q", contentType)
	}
	disposition, parameters, err := mime.ParseMediaType(response.Header.Get("Content-Disposition"))
	if err != nil || disposition != "attachment" || parameters["filename"] != artifact.GetFileName() {
		t.Fatalf("download content disposition = %q (%v, %v)", response.Header.Get("Content-Disposition"), parameters, err)
	}
	if !strings.HasSuffix(parameters["filename"], ".jsonl") {
		t.Fatalf("download filename = %q", parameters["filename"])
	}
	if response.Header.Get("Cache-Control") != "no-store" || response.Header.Get("Pragma") != "no-cache" ||
		response.Header.Get("X-Content-Type-Options") != "nosniff" || response.Header.Get("Accept-Ranges") != "" ||
		response.Header.Get("Content-Security-Policy") != "sandbox" ||
		response.Header.Get("Cross-Origin-Resource-Policy") != "same-origin" {
		t.Fatalf("download safety headers = %+v", response.Header)
	}

	replay, err := http.NewRequestWithContext(ctx, http.MethodGet, downloadURL, nil)
	if err != nil {
		t.Fatal(err)
	}
	replay.Header.Set("Authorization", "Bearer "+grant.GetDownloadToken())
	replayed, err := client.Do(replay)
	if err != nil {
		t.Fatalf("replay export grant: %v", err)
	}
	replayBody, replayReadErr := io.ReadAll(io.LimitReader(replayed.Body, 1<<20))
	_ = replayed.Body.Close()
	if replayReadErr != nil {
		t.Fatalf("read replay rejection: %v", replayReadErr)
	}
	if replayed.StatusCode != http.StatusUnauthorized || replayed.Header.Get("WWW-Authenticate") == "" ||
		bytes.Contains(replayBody, []byte(grant.GetDownloadToken())) || bytes.Contains(replayBody, []byte(artifact.GetFileName())) {
		t.Fatalf("grant replay response = status %d headers %+v body %q", replayed.StatusCode, replayed.Header, replayBody)
	}
	return body
}

func assertDownloadedRedactedResults(t *testing.T, completed *opensplunkv1.ExportJob, artifact []byte) {
	t.Helper()
	if completed.GetArtifact().GetRowCount() != verticalEventCount {
		t.Fatalf("downloaded export metadata = %+v", completed.GetArtifact())
	}
	for _, sentinel := range []string{
		redactionAPIKeySentinel,
		redactionCookieSentinel,
		redactionPrivateKeySentinel,
		redactionCredentialSentinel,
		redactionPINSentinel,
	} {
		if bytes.Contains(artifact, []byte(sentinel)) {
			t.Fatalf("downloaded export leaked sentinel %q", sentinel)
		}
	}
	var (
		rowCount uint64
		found    bool
	)
	expectedColumns := []string{
		"message",
		"status",
		"duration_ms",
		"api_key",
		"customer_credential",
		"customer_pin",
		"_raw",
	}
	forEachJSONLine(t, artifact, func(line int, row map[string]any) {
		if len(row) != len(expectedColumns) {
			t.Fatalf("JSON Lines row %d columns = %#v", line, row)
		}
		for _, column := range expectedColumns {
			if _, exists := row[column]; !exists {
				t.Fatalf("JSON Lines row %d is missing column %q: %#v", line, column, row)
			}
		}
		rowCount++
		if row["message"] != verticalSentinelMessage {
			return
		}
		found = true
		status, statusOK := row["status"].(json.Number)
		duration, durationOK := row["duration_ms"].(json.Number)
		raw, rawOK := row["_raw"].(string)
		if !statusOK || status.String() != "201" || !durationOK || duration.String() != "12.5" ||
			row["api_key"] != "[REDACTED]" ||
			row["customer_credential"] != redactionCredentialMarker ||
			row["customer_pin"] != redactionPINMarker ||
			!rawOK ||
			strings.Count(raw, "[REDACTED]") < 3 ||
			!strings.Contains(raw, `"customer_credential":"`+redactionCredentialMarker+`"`) ||
			!strings.Contains(raw, `"customer_pin":"`+redactionPINMarker+`"`) {
			t.Fatalf("downloaded typed redaction row = %#v", row)
		}
	})
	if rowCount != verticalEventCount || !found {
		t.Fatalf("downloaded rows = %d, sentinel found = %t", rowCount, found)
	}
}

func createEmptyFixture(t *testing.T, path string) {
	t.Helper()
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
}

func appendPrimerFixture(t *testing.T, path string, start time.Time) {
	t.Helper()
	file, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	config := verticalLogGeneratorConfig(start, 20260722)
	if err := loggen.Generate(context.Background(), file, config, 1); err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	syncAndCloseFixture(t, file)
}

func appendGeneratedFixture(t *testing.T, path string, start time.Time) {
	t.Helper()
	file, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	config := verticalLogGeneratorConfig(start.Add(time.Second), 20260723)
	if err := loggen.Generate(context.Background(), file, config, verticalEventCount-2); err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	syncAndCloseFixture(t, file)
}

func appendSentinelFixture(t *testing.T, path string, start time.Time) {
	t.Helper()
	file, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	sentinelLine := fmt.Sprintf(
		`{"timestamp":%q,"level":"INFO","message":%q,"status":201,"duration_ms":12.5,`+
			`"api_key":%q,"customer_credential":%q,"customer_pin":%q,"note_one":%q,"note_two":%q}`+"\n",
		start.Add(3*time.Second).Format(time.RFC3339Nano), verticalSentinelMessage, redactionAPIKeySentinel,
		redactionCredentialSentinel, redactionPINSentinel,
		"Cookie: sid="+redactionCookieSentinel+"; csrf="+redactionCookieSentinel,
		"private_key=-----BEGIN PRIVATE KEY----- "+redactionPrivateKeySentinel,
	)
	if _, err := io.WriteString(file, sentinelLine); err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	syncAndCloseFixture(t, file)
}

func verticalLogGeneratorConfig(start time.Time, seed int64) loggen.Config {
	config := loggen.DefaultConfig()
	config.Format = loggen.FormatZapJSON
	config.Seed = seed
	config.Start = start
	config.Interval = time.Second
	config.Service = "vertical-service"
	config.Environment = "integration"
	config.Host = "vertical-host"
	return config
}

func syncAndCloseFixture(t *testing.T, file *os.File) {
	t.Helper()
	if err := file.Sync(); err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
}

func writePrivateFile(t *testing.T, path string, contents []byte) {
	t.Helper()
	if err := os.WriteFile(path, contents, 0o600); err != nil {
		t.Fatal(err)
	}
}

func writeCollectorIdentity(t *testing.T, stateDir, collectorID string) {
	t.Helper()
	if err := os.MkdirAll(stateDir, 0o700); err != nil {
		t.Fatal(err)
	}
	writePrivateFile(t, filepath.Join(stateDir, "collector_id"), []byte(collectorID+"\n"))
}

func collectorYAML(address, tokenPath, statePath, logPath string) string {
	return fmt.Sprintf(`server:
  address: %q
  transport: grpc
  token_file: %q
  compression: gzip
  tls:
    enabled: false
state:
  directory: %q
  max_queue_bytes: 16MiB
inputs:
  - id: vertical-app-log
    type: file
    include:
      - %q
    format: ndjson
    start_at: beginning
    index: %s
    source: app.log
    sourcetype: json
    host: vertical-host
    poll_interval: 20ms
    fields:
      environment: integration
      service: vertical-service
processors:
  - type: redact
    fields: [customer_credential]
    replacement: %q
  - type: redact
    fields: [customer_pin]
    replacement: %q
`, address, tokenPath, statePath, logPath, verticalIndexName,
		redactionCredentialMarker, redactionPINMarker)
}

func waitForStoredEventCount(
	t *testing.T,
	ctx context.Context,
	connection clickhousedriver.Conn,
	process *managedProcess,
	plaintextToken string,
	wantCount uint64,
) {
	t.Helper()
	deadline := time.NewTimer(30 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	var lastCount uint64
	for {
		err := connection.QueryRow(ctx,
			"SELECT count() FROM open_splunk.events WHERE tenant_id = ? AND index_name = ?",
			verticalTenantID, verticalIndexName,
		).Scan(&lastCount)
		if err == nil && lastCount == wantCount {
			return
		}
		if err == nil && lastCount > wantCount {
			t.Fatalf("stored event count = %d, want %d", lastCount, wantCount)
		}
		if process.Exited() {
			t.Fatalf("collector exited before ingestion completed: %v, count=%d\nlogs:\n%s", process.Err(), lastCount,
				redactForFailure(process.Logs(), plaintextToken,
					redactionAPIKeySentinel, redactionCookieSentinel, redactionPrivateKeySentinel,
					redactionCredentialSentinel, redactionPINSentinel))
		}
		select {
		case <-ctx.Done():
			t.Fatalf("wait for stored events: %v, count=%d\ncollector logs:\n%s", ctx.Err(), lastCount,
				redactForFailure(process.Logs(), plaintextToken,
					redactionAPIKeySentinel, redactionCookieSentinel, redactionPrivateKeySentinel,
					redactionCredentialSentinel, redactionPINSentinel))
		case <-deadline.C:
			t.Fatalf("wait for stored events: timed out, count=%d\ncollector logs:\n%s", lastCount,
				redactForFailure(process.Logs(), plaintextToken,
					redactionAPIKeySentinel, redactionCookieSentinel, redactionPrivateKeySentinel,
					redactionCredentialSentinel, redactionPINSentinel))
		case <-ticker.C:
		}
	}
}

func waitForCollectorCheckpoint(
	t *testing.T,
	ctx context.Context,
	stateDir, logPath string,
	process *managedProcess,
	plaintextToken string,
) {
	t.Helper()
	info, err := os.Stat(logPath)
	if err != nil {
		t.Fatalf("stat collector fixture: %v", err)
	}
	if info.Size() <= 0 {
		t.Fatalf("collector fixture size = %d", info.Size())
	}
	wantOffset := uint64(info.Size())
	deadline := time.NewTimer(30 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	var (
		lastOffset uint64
		lastCount  int
		lastErr    error
	)
	for {
		list, readErr := readCollectorCheckpoints(stateDir)
		lastErr = readErr
		if readErr == nil {
			lastCount = len(list)
			if len(list) == 1 {
				lastOffset = list[0].Offset
				if lastOffset == wantOffset {
					return
				}
				if lastOffset > wantOffset {
					t.Fatalf("durable collector checkpoint offset = %d, want %d", lastOffset, wantOffset)
				}
			}
		}
		if process.Exited() {
			t.Fatalf("collector exited before durable acknowledgment: %v, checkpoints=%d offset=%d error=%v\nlogs:\n%s",
				process.Err(), lastCount, lastOffset, lastErr,
				redactForFailure(process.Logs(), plaintextToken,
					redactionAPIKeySentinel, redactionCookieSentinel, redactionPrivateKeySentinel,
					redactionCredentialSentinel, redactionPINSentinel))
		}
		select {
		case <-ctx.Done():
			t.Fatalf("wait for durable collector acknowledgment: %v, checkpoints=%d offset=%d error=%v",
				ctx.Err(), lastCount, lastOffset, lastErr)
		case <-deadline.C:
			t.Fatalf("wait for durable collector acknowledgment: timed out, checkpoints=%d offset=%d want=%d error=%v",
				lastCount, lastOffset, wantOffset, lastErr)
		case <-ticker.C:
		}
	}
}

type collectorWALMeta struct {
	NextBatchSequence      uint64 `json:"next_batch_sequence"`
	LastAckedBatchSequence uint64 `json:"last_acked_batch_sequence"`
}

// waitForCollectorWALAcknowledgedThroughCurrent closes the narrow transaction
// window after source-checkpoint persistence but before the local WAL
// high-water advances. Once it returns, any subsequent segment growth after the
// server stops can only be the deliberately appended offline sentinel batch.
func waitForCollectorWALAcknowledgedThroughCurrent(
	t *testing.T,
	ctx context.Context,
	stateDir string,
	process *managedProcess,
	plaintextToken string,
) {
	t.Helper()
	deadline := time.NewTimer(15 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()
	var (
		last    collectorWALMeta
		lastErr error
	)
	for {
		last, lastErr = readCollectorWALMeta(stateDir)
		if lastErr == nil &&
			last.LastAckedBatchSequence > 0 &&
			last.LastAckedBatchSequence < ^uint64(0) &&
			last.NextBatchSequence == last.LastAckedBatchSequence+1 {
			return
		}
		if process.Exited() {
			t.Fatalf(
				"collector exited before WAL acknowledgment settled: %v meta=%+v error=%v\nlogs:\n%s",
				process.Err(),
				last,
				lastErr,
				redactForFailure(
					process.Logs(),
					plaintextToken,
					redactionAPIKeySentinel,
					redactionCookieSentinel,
					redactionPrivateKeySentinel,
				),
			)
		}
		select {
		case <-ctx.Done():
			t.Fatalf("wait for collector WAL acknowledgment: %v meta=%+v error=%v", ctx.Err(), last, lastErr)
		case <-deadline.C:
			t.Fatalf(
				"wait for collector WAL acknowledgment: timed out meta=%+v error=%v\nlogs:\n%s",
				last,
				lastErr,
				redactForFailure(
					process.Logs(),
					plaintextToken,
					redactionAPIKeySentinel,
					redactionCookieSentinel,
					redactionPrivateKeySentinel,
				),
			)
		case <-ticker.C:
		}
	}
}

func readCollectorWALMeta(stateDir string) (collectorWALMeta, error) {
	data, err := os.ReadFile(filepath.Join(stateDir, "wal", "meta.json"))
	if err != nil {
		return collectorWALMeta{}, err
	}
	var meta collectorWALMeta
	if err := json.Unmarshal(data, &meta); err != nil {
		return collectorWALMeta{}, err
	}
	return meta, nil
}

func snapshotCollectorWAL(t *testing.T, stateDir string) map[string]int64 {
	t.Helper()
	snapshot, err := readCollectorWALSnapshot(stateDir)
	if err != nil {
		t.Fatalf("snapshot collector WAL: %v", err)
	}
	return snapshot
}

func readCollectorWALSnapshot(stateDir string) (map[string]int64, error) {
	entries, err := os.ReadDir(filepath.Join(stateDir, "wal"))
	if err != nil {
		return nil, err
	}
	snapshot := make(map[string]int64, len(entries))
	for _, entry := range entries {
		if !strings.HasPrefix(entry.Name(), "segment-") || !strings.HasSuffix(entry.Name(), ".wal") {
			continue
		}
		info, infoErr := entry.Info()
		if infoErr != nil {
			return nil, infoErr
		}
		if info.Mode().IsRegular() {
			snapshot[entry.Name()] = info.Size()
		}
	}
	return snapshot, nil
}

func collectorWALAdvanced(before, after map[string]int64) bool {
	for name, size := range after {
		previous, existed := before[name]
		if (!existed && size > 0) || (existed && size > previous) {
			return true
		}
	}
	return false
}

func waitForCollectorWALAppend(
	t *testing.T,
	ctx context.Context,
	stateDir string,
	before map[string]int64,
	process *managedProcess,
	plaintextToken string,
) {
	t.Helper()
	deadline := time.NewTimer(30 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()
	var (
		last    map[string]int64
		lastErr error
	)
	for {
		last, lastErr = readCollectorWALSnapshot(stateDir)
		if lastErr == nil && collectorWALAdvanced(before, last) {
			syncErr := syncCollectorWALGrowth(stateDir, before, last)
			if syncErr == nil {
				return
			}
			lastErr = syncErr
		}
		if process.Exited() {
			t.Fatalf("collector exited before offline WAL append: %v snapshot=%v error=%v\nlogs:\n%s",
				process.Err(), last, lastErr, redactForFailure(process.Logs(), plaintextToken,
					redactionAPIKeySentinel, redactionCookieSentinel, redactionPrivateKeySentinel,
					redactionCredentialSentinel, redactionPINSentinel))
		}
		select {
		case <-ctx.Done():
			t.Fatalf("wait for offline collector WAL append: %v snapshot=%v error=%v", ctx.Err(), last, lastErr)
		case <-deadline.C:
			t.Fatalf("wait for offline collector WAL append: timed out snapshot=%v error=%v\nlogs:\n%s",
				last, lastErr, redactForFailure(process.Logs(), plaintextToken,
					redactionAPIKeySentinel, redactionCookieSentinel, redactionPrivateKeySentinel,
					redactionCredentialSentinel, redactionPINSentinel))
		case <-ticker.C:
		}
	}
}

// syncCollectorWALGrowth establishes the test's hard-crash durability barrier.
// Queue.Append writes one complete record before its SyncAlways fsync. Once the
// size growth is visible, syncing that exact segment from a second descriptor
// guarantees the observed record bytes are stable before the test sends SIGKILL
// rather than merely racing the collector's own Sync call.
func syncCollectorWALGrowth(
	stateDir string,
	before, after map[string]int64,
) error {
	walDir := filepath.Join(stateDir, "wal")
	for name, size := range after {
		previous, existed := before[name]
		if (existed && size <= previous) || (!existed && size == 0) {
			continue
		}
		file, err := os.OpenFile(filepath.Join(walDir, name), os.O_RDWR, 0)
		if err != nil {
			return fmt.Errorf("open advanced WAL segment %s: %w", name, err)
		}
		info, statErr := file.Stat()
		if statErr == nil && info.Size() < size {
			statErr = fmt.Errorf("WAL segment %s shrank from observed size %d to %d", name, size, info.Size())
		}
		syncErr := file.Sync()
		closeErr := file.Close()
		if err := errors.Join(statErr, syncErr, closeErr); err != nil {
			return fmt.Errorf("sync advanced WAL segment %s: %w", name, err)
		}
	}
	directory, err := os.Open(walDir)
	if err != nil {
		return fmt.Errorf("open WAL directory for sync: %w", err)
	}
	return errors.Join(directory.Sync(), directory.Close())
}

func assertCrashSafeAcknowledgedCollectorState(
	t *testing.T,
	stateDir, logPath string,
	wantOffset uint64,
) {
	t.Helper()
	assertCollectorCheckpoint(t, stateDir, wantOffset, 1)

	queue, err := wal.Open(wal.Options{
		Dir:  filepath.Join(stateDir, "wal"),
		Sync: wal.SyncAlways,
	})
	if err != nil {
		t.Fatalf("reopen crash-safe collector WAL: %v", err)
	}
	defer func() {
		if closeErr := queue.Close(); closeErr != nil {
			t.Errorf("close crash-safe collector WAL: %v", closeErr)
		}
	}()
	stats := queue.Stats()
	if stats.QueuedBatches == 0 && stats.QueuedEvents == 0 && stats.QueuedBytes == 0 &&
		stats.LastAckedBatchSequence > 0 {
		return
	}
	// A crash may land after the server acknowledgment advanced the durable
	// source checkpoint but before the collector advances its own WAL high-water
	// mark. Retaining that batch is the other valid state: the restart replays
	// its stable event ID and reconciles it through the server's durable prefix.
	if stats.QueuedBatches != 1 || stats.QueuedEvents != 1 || stats.QueuedBytes == 0 ||
		stats.LastAckedBatchSequence != 0 {
		t.Fatalf("crash-safe collector WAL state = %+v, want drained or one replayable acknowledged batch", stats)
	}
	nextContext, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	batch, err := queue.NextBatch(nextContext)
	if err != nil {
		t.Fatalf("read replayable acknowledged collector batch: %v", err)
	}
	if len(batch.GetEvents()) != 1 {
		t.Fatalf("replayable acknowledged collector batch = %+v, want one event", batch)
	}
	origin := batch.GetEvents()[0].GetOrigin()
	if origin.GetSourcePath() != logPath || origin.GetStartOffset() != 0 ||
		origin.GetEndOffset() != wantOffset || origin.GetLineNumber() != 1 ||
		origin.GetNextLineNumber() != 2 {
		t.Fatalf("replayable acknowledged collector event origin = %+v", origin)
	}
}

func waitForCollectorDiscovery(
	t *testing.T,
	ctx context.Context,
	stateDir, logPath string,
	process *managedProcess,
	plaintextToken string,
) {
	t.Helper()
	deadline := time.NewTimer(15 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	var (
		lastCount int
		lastErr   error
	)
	for {
		list, readErr := readCollectorCheckpoints(stateDir)
		lastErr = readErr
		if readErr == nil {
			lastCount = len(list)
			if len(list) == 1 {
				checkpoint := list[0]
				if checkpoint.Path != logPath || checkpoint.Offset != 0 ||
					checkpoint.LineNumber != 0 || checkpoint.NextLineNumber != 1 ||
					checkpoint.Identity.FingerprintLength != 0 {
					t.Fatalf("empty collector discovery checkpoint = %+v, want %s at offset zero", checkpoint, logPath)
				}
				return
			}
		}
		if process.Exited() {
			t.Fatalf("collector exited before empty-file discovery: %v, checkpoints=%d error=%v\nlogs:\n%s",
				process.Err(), lastCount, lastErr,
				redactForFailure(process.Logs(), plaintextToken,
					redactionAPIKeySentinel, redactionCookieSentinel, redactionPrivateKeySentinel,
					redactionCredentialSentinel, redactionPINSentinel))
		}
		select {
		case <-ctx.Done():
			t.Fatalf("wait for empty-file discovery: %v, checkpoints=%d error=%v", ctx.Err(), lastCount, lastErr)
		case <-deadline.C:
			t.Fatalf("wait for empty-file discovery: timed out, checkpoints=%d error=%v", lastCount, lastErr)
		case <-ticker.C:
		}
	}
}

func readCollectorCheckpoints(stateDir string) ([]input.Checkpoint, error) {
	checkpoints, err := input.NewCheckpointStore(filepath.Join(stateDir, "checkpoints"))
	if err != nil {
		return nil, err
	}
	list, listErr := checkpoints.List()
	return list, errors.Join(listErr, checkpoints.Close())
}

func assertDurableCollectorState(t *testing.T, stateDir string, wantOffset, wantLine uint64) {
	t.Helper()
	assertCollectorCheckpoint(t, stateDir, wantOffset, wantLine)

	queue, err := wal.Open(wal.Options{
		Dir:  filepath.Join(stateDir, "wal"),
		Sync: wal.SyncAlways,
	})
	if err != nil {
		t.Fatalf("reopen collector WAL: %v", err)
	}
	stats := queue.Stats()
	if err := queue.Close(); err != nil {
		t.Fatalf("close reopened collector WAL: %v", err)
	}
	if stats.QueuedBatches != 0 || stats.QueuedEvents != 0 || stats.QueuedBytes != 0 ||
		stats.OldestEventAge != 0 ||
		stats.LastAckedBatchSequence == 0 ||
		stats.LastAckedBatchSequence == ^uint64(0) ||
		stats.NextBatchSequence != stats.LastAckedBatchSequence+1 ||
		stats.QuarantinedSegments != 0 {
		t.Fatalf("durable collector WAL state = %+v, want drained queue with a terminal acknowledgment", stats)
	}
	entries, err := os.ReadDir(filepath.Join(stateDir, "wal"))
	if err != nil {
		t.Fatalf("read reopened collector WAL directory: %v", err)
	}
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), "segment-") &&
			strings.Contains(entry.Name(), ".wal.corrupt") {
			t.Fatalf("durable collector WAL retained quarantined artifact %q", entry.Name())
		}
	}
}

func assertCollectorCheckpoint(t *testing.T, stateDir string, wantOffset, wantLine uint64) {
	t.Helper()
	list, err := readCollectorCheckpoints(stateDir)
	if err != nil {
		t.Fatalf("read durable collector checkpoints: %v", err)
	}
	if len(list) != 1 || list[0].Offset != wantOffset ||
		list[0].LineNumber != wantLine || list[0].NextLineNumber != wantLine+1 {
		t.Fatalf(
			"durable collector checkpoints = %+v, want one checkpoint at offset %d lines [%d,%d)",
			list,
			wantOffset,
			wantLine,
			wantLine+1,
		)
	}
}

func assertPendingCollectorState(t *testing.T, stateDir, logPath string, wantOffset uint64) {
	t.Helper()
	list, err := readCollectorCheckpoints(stateDir)
	if err != nil {
		t.Fatalf("read pending collector checkpoints: %v", err)
	}
	if len(list) != 1 || list[0].Offset != wantOffset ||
		list[0].LineNumber != verticalEventCount-1 ||
		list[0].NextLineNumber != verticalEventCount {
		t.Fatalf(
			"pending collector checkpoints = %+v, want one checkpoint at offset %d lines [%d,%d)",
			list,
			wantOffset,
			verticalEventCount-1,
			verticalEventCount,
		)
	}

	queue, err := wal.Open(wal.Options{
		Dir:  filepath.Join(stateDir, "wal"),
		Sync: wal.SyncAlways,
	})
	if err != nil {
		t.Fatalf("reopen pending collector WAL: %v", err)
	}
	defer func() {
		if closeErr := queue.Close(); closeErr != nil {
			t.Errorf("close pending collector WAL: %v", closeErr)
		}
	}()
	stats := queue.Stats()
	if stats.QueuedBatches != 1 || stats.QueuedEvents != 1 || stats.QueuedBytes == 0 ||
		stats.LastAckedBatchSequence == 0 {
		t.Fatalf("pending collector WAL state = %+v, want one queued event after an acknowledged prefix", stats)
	}
	nextContext, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	batch, err := queue.NextBatch(nextContext)
	if err != nil {
		t.Fatalf("read pending collector WAL batch: %v", err)
	}
	if batch.GetBatchSequence() <= stats.LastAckedBatchSequence || len(batch.GetEvents()) != 1 {
		t.Fatalf("pending collector batch = %+v, acknowledged through %d", batch, stats.LastAckedBatchSequence)
	}
	event := batch.GetEvents()[0]
	origin := event.GetOrigin()
	if event.GetMessage() != verticalSentinelMessage || event.GetIndexName() != verticalIndexName ||
		origin.GetSourcePath() != logPath || origin.GetStartOffset() != wantOffset ||
		origin.GetEndOffset() != uint64(mustFileSize(t, logPath)) ||
		origin.GetLineNumber() != verticalEventCount ||
		origin.GetNextLineNumber() != verticalEventCount+1 {
		t.Fatalf("pending collector event = %+v", event)
	}
}

func mustFileSize(t *testing.T, path string) int64 {
	t.Helper()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat %s: %v", path, err)
	}
	return info.Size()
}

func assertStoredEventBounds(t *testing.T, ctx context.Context, connection clickhousedriver.Conn, fixtureStart time.Time) uint64 {
	t.Helper()
	var (
		earliest, latest                   time.Time
		earliestIndexTime, latestIndexTime time.Time
		minimumVisibility                  uint64
		maximumVisibility                  uint64
	)
	if err := connection.QueryRow(ctx, `
		SELECT min(event_time), max(event_time), min(index_time), max(index_time),
		       min(visibility_seq), max(visibility_seq)
		FROM open_splunk.events
		WHERE tenant_id = ? AND index_name = ?`, verticalTenantID, verticalIndexName,
	).Scan(&earliest, &latest, &earliestIndexTime, &latestIndexTime, &minimumVisibility, &maximumVisibility); err != nil {
		t.Fatalf("read stored event bounds: %v", err)
	}
	if !earliest.Equal(fixtureStart) || !latest.Equal(fixtureStart.Add(3*time.Second)) {
		t.Fatalf("stored event time bounds = [%s,%s], want [%s,%s]",
			earliest.Format(time.RFC3339Nano), latest.Format(time.RFC3339Nano),
			fixtureStart.Format(time.RFC3339Nano), fixtureStart.Add(3*time.Second).Format(time.RFC3339Nano))
	}
	if earliestIndexTime.IsZero() || latestIndexTime.Before(earliestIndexTime) ||
		minimumVisibility == 0 || maximumVisibility < minimumVisibility {
		t.Fatalf("stored commit bounds: index_time=[%s,%s] visibility=[%d,%d]",
			earliestIndexTime.Format(time.RFC3339Nano), latestIndexTime.Format(time.RFC3339Nano),
			minimumVisibility, maximumVisibility)
	}
	t.Logf("stored event bounds: event_time=[%s,%s] index_time=[%s,%s] visibility=[%d,%d]",
		earliest.Format(time.RFC3339Nano), latest.Format(time.RFC3339Nano),
		earliestIndexTime.Format(time.RFC3339Nano), latestIndexTime.Format(time.RFC3339Nano),
		minimumVisibility, maximumVisibility)
	return maximumVisibility
}

func assertRestartDeliveryAccounting(t *testing.T, ctx context.Context, connection clickhousedriver.Conn) {
	t.Helper()
	var total, distinct, sentinelRows, sentinelEventIDs, sentinelBatchIDs uint64
	if err := connection.QueryRow(ctx, `
		SELECT count(), uniqExact(event_id), countIf(body = ?),
		       uniqExactIf(event_id, body = ?), uniqExactIf(batch_id, body = ?)
		FROM open_splunk.events
		WHERE tenant_id = ? AND index_name = ?`,
		verticalSentinelMessage,
		verticalSentinelMessage,
		verticalSentinelMessage,
		verticalTenantID,
		verticalIndexName,
	).Scan(&total, &distinct, &sentinelRows, &sentinelEventIDs, &sentinelBatchIDs); err != nil {
		t.Fatalf("read restart delivery accounting: %v", err)
	}
	if distinct != verticalEventCount || total < distinct || total > distinct+1 ||
		sentinelRows != total-distinct+1 || sentinelEventIDs != 1 ||
		sentinelBatchIDs != sentinelRows {
		t.Fatalf(
			"restart delivery accounting = total:%d distinct:%d sentinel_rows:%d sentinel_event_ids:%d sentinel_batch_ids:%d",
			total,
			distinct,
			sentinelRows,
			sentinelEventIDs,
			sentinelBatchIDs,
		)
	}
	t.Logf("restart delivery accounting: stored=%d distinct_event_ids=%d replayed=%d", total, distinct, total-distinct)
}

func insertTimelineExclusiveBoundaryEvent(
	t *testing.T,
	ctx context.Context,
	connection clickhousedriver.Conn,
	eventTime time.Time,
	visibilityCutoff uint64,
) {
	t.Helper()
	if eventTime.Nanosecond() == 0 || visibilityCutoff == 0 {
		t.Fatalf("invalid timeline boundary fixture: event_time=%s visibility=%d", eventTime, visibilityCutoff)
	}
	const eventID = "vertical-timeline-exclusive-boundary"
	message := "timeline latest-exclusive boundary event"
	document := clickhousedriver.NewJSON()
	batch, err := connection.PrepareBatch(ctx, clickHouseEventInsertSQL)
	if err != nil {
		t.Fatalf("prepare timeline boundary event: %v", err)
	}
	if err := batch.Append(
		eventID, verticalTenantID, verticalIndexName, eventTime, eventTime,
		nil, uint8(1), "vertical-host", "timeline-boundary.log", "integration", nil, uint8(1), nil,
		&message, []byte(message), uint8(1), nil, nil, document, []string(nil), "integration-direct",
		"vertical-timeline-boundary", uint64(1), eventTime.Add(24*time.Hour), visibilityCutoff,
	); err != nil {
		t.Fatalf("append timeline boundary event: %v", err)
	}
	if err := batch.Send(); err != nil {
		t.Fatalf("insert timeline boundary event: %v", err)
	}
	var storedTime time.Time
	if err := connection.QueryRow(ctx,
		"SELECT event_time FROM open_splunk.events WHERE tenant_id = ? AND event_id = ?",
		verticalTenantID, eventID,
	).Scan(&storedTime); err != nil {
		t.Fatalf("read timeline boundary event: %v", err)
	}
	if !storedTime.Equal(eventTime) {
		t.Fatalf("stored timeline boundary event time = %s, want %s", storedTime, eventTime)
	}
}

func assertBackendIndexStatistics(
	t *testing.T,
	ctx context.Context,
	client *http.Client,
	baseURL string,
	administratorToken string,
	connection clickhousedriver.Conn,
	indexID string,
	visibilityCutoff uint64,
) {
	t.Helper()
	if indexID == "" || visibilityCutoff == 0 {
		t.Fatalf(
			"invalid index statistics fixture identity: index=%q visibility=%d",
			indexID,
			visibilityCutoff,
		)
	}
	var response opensplunkv1.GetIndexStatsResponse
	postAdministratorProto(
		t,
		ctx,
		client,
		baseURL+"/api/v1/indexes/stats/get",
		administratorToken,
		&opensplunkv1.GetIndexStatsRequest{
			Selector: &opensplunkv1.IndexSelector{
				Selector: &opensplunkv1.IndexSelector_IndexId{
					IndexId: indexID,
				},
			},
		},
		&response,
	)
	assertBackendIndexStatisticsValue(
		t,
		ctx,
		connection,
		"get",
		response.GetStats(),
		indexID,
		visibilityCutoff,
	)
	assertBackendIndexListStatistics(
		t,
		ctx,
		client,
		baseURL,
		administratorToken,
		connection,
		indexID,
		visibilityCutoff,
	)
}

func assertBackendIndexFields(
	t *testing.T,
	ctx context.Context,
	client *http.Client,
	baseURL string,
	administratorToken string,
	connection clickhousedriver.Conn,
	indexID string,
	fixtureStart time.Time,
) {
	t.Helper()
	if indexID == "" || fixtureStart.IsZero() {
		t.Fatalf(
			"invalid index field fixture identity: index=%q start=%s",
			indexID,
			fixtureStart,
		)
	}

	earliest := fixtureStart.Format(time.RFC3339Nano)
	latest := fixtureStart.Add(4 * time.Second).Format(time.RFC3339Nano)
	timezone := "UTC"
	nameFilter := "customer_"
	pageSize := uint32(1)
	includeTotal := true
	request := &opensplunkv1.ListIndexFieldsRequest{
		Selector: &opensplunkv1.IndexSelector{
			Selector: &opensplunkv1.IndexSelector_IndexId{IndexId: indexID},
		},
		TimeRange: &opensplunkv1.TimeRangeSpec{
			Earliest: &earliest,
			Latest:   &latest,
			Timezone: &timezone,
		},
		Page: &opensplunkv1.PageRequest{
			PageSize:         &pageSize,
			IncludeTotalSize: includeTotal,
		},
		NameFilter: &nameFilter,
	}
	var first opensplunkv1.ListIndexFieldsResponse
	postAdministratorProto(
		t,
		ctx,
		client,
		baseURL+"/api/v1/indexes/fields/list",
		administratorToken,
		request,
		&first,
	)
	if len(first.GetFields()) != 1 ||
		first.GetFields()[0].GetFieldName() != "customer_credential" ||
		first.GetPage().GetNextPageToken() == "" ||
		first.GetPage().TotalSize == nil ||
		first.GetPage().GetTotalSize() != 2 ||
		!first.GetPage().GetTotalSizeExact() {
		t.Fatalf("first backend index-field page = %+v", &first)
	}

	token := first.GetPage().GetNextPageToken()
	secondPageSize := uint32(2)
	request.Selector = &opensplunkv1.IndexSelector{
		Selector: &opensplunkv1.IndexSelector_IndexName{
			IndexName: strings.ToUpper(verticalIndexName),
		},
	}
	request.Page = &opensplunkv1.PageRequest{
		PageSize:  &secondPageSize,
		PageToken: &token,
	}
	var second opensplunkv1.ListIndexFieldsResponse
	postAdministratorProto(
		t,
		ctx,
		client,
		baseURL+"/api/v1/indexes/fields/list",
		administratorToken,
		request,
		&second,
	)
	if len(second.GetFields()) != 1 ||
		second.GetFields()[0].GetFieldName() != "customer_pin" ||
		second.GetPage().NextPageToken != nil ||
		second.GetPage().TotalSize != nil ||
		second.GetPage().GetTotalSizeExact() {
		t.Fatalf("second backend index-field page = %+v", &second)
	}

	var totalEvents, credentialEvents, pinEvents uint64
	if err := connection.QueryRow(
		ctx,
		`SELECT count(),
		        countIf(has(field_names, 'customer_credential')),
		        countIf(has(field_names, 'customer_pin'))
		   FROM open_splunk.events
		  PREWHERE tenant_id = ? AND index_name = ?
		  WHERE event_time >= ? AND event_time < ?`,
		verticalTenantID,
		verticalIndexName,
		fixtureStart,
		fixtureStart.Add(4*time.Second),
	).Scan(&totalEvents, &credentialEvents, &pinEvents); err != nil {
		t.Fatalf("read backend index-field reference counts: %v", err)
	}
	if totalEvents == 0 || credentialEvents == 0 || credentialEvents != pinEvents ||
		credentialEvents > totalEvents {
		t.Fatalf(
			"backend index-field reference counts = total:%d credential:%d pin:%d",
			totalEvents,
			credentialEvents,
			pinEvents,
		)
	}
	assertBackendIndexFieldProfile(
		t,
		first.GetFields()[0],
		"customer_credential",
		totalEvents,
		credentialEvents,
	)
	assertBackendIndexFieldProfile(
		t,
		second.GetFields()[0],
		"customer_pin",
		totalEvents,
		pinEvents,
	)
}

func assertBackendIndexFieldProfile(
	t *testing.T,
	profile *opensplunkv1.FieldProfile,
	name string,
	totalEvents, eventCount uint64,
) {
	t.Helper()
	interestingThreshold := totalEvents / 5
	if totalEvents%5 != 0 {
		interestingThreshold++
	}
	if profile == nil ||
		profile.GetFieldName() != name ||
		profile.GetDisplayName() != name ||
		profile.GetValueType() != opensplunkv1.ValueType_VALUE_TYPE_STRING ||
		len(profile.GetObservedValueTypes()) != 1 ||
		profile.GetObservedValueTypes()[0] != opensplunkv1.ValueType_VALUE_TYPE_STRING ||
		profile.GetEventCount() != eventCount ||
		profile.GetNullCount() != 0 ||
		profile.GetMissingCount() != totalEvents-eventCount ||
		profile.DistinctCount != nil ||
		profile.GetDistinctCountIsApproximate() ||
		profile.GetSelected() ||
		profile.GetInteresting() != (eventCount >= interestingThreshold) {
		t.Fatalf(
			"backend index field %q = %+v, total=%d present=%d",
			name,
			profile,
			totalEvents,
			eventCount,
		)
	}
}

func assertBackendIndexListStatistics(
	t *testing.T,
	ctx context.Context,
	client *http.Client,
	baseURL string,
	administratorToken string,
	connection clickhousedriver.Conn,
	indexID string,
	visibilityCutoff uint64,
) {
	t.Helper()
	pageSize := uint32(64)
	textFilter := verticalIndexName
	var response opensplunkv1.ListIndexesResponse
	postAdministratorProto(
		t,
		ctx,
		client,
		baseURL+"/api/v1/indexes/list",
		administratorToken,
		&opensplunkv1.ListIndexesRequest{
			Page:         &opensplunkv1.PageRequest{PageSize: &pageSize},
			TextFilter:   &textFilter,
			IncludeStats: true,
		},
		&response,
	)

	var matchingItem *opensplunkv1.IndexListItem
	for _, item := range response.GetIndexes() {
		if item.GetIndex().GetIndexId() != indexID {
			continue
		}
		if matchingItem != nil {
			t.Fatalf("backend index list returned duplicate index ID %q", indexID)
		}
		matchingItem = item
	}
	if matchingItem == nil ||
		matchingItem.GetIndex().GetDefinition().GetName() != verticalIndexName {
		t.Fatalf(
			"backend index list did not return index %q (%q): %#v",
			verticalIndexName,
			indexID,
			response.GetIndexes(),
		)
	}
	assertBackendIndexStatisticsValue(
		t,
		ctx,
		connection,
		"list",
		matchingItem.GetStats(),
		indexID,
		visibilityCutoff,
	)
}

func assertBackendIndexStatisticsValue(
	t *testing.T,
	ctx context.Context,
	connection clickhousedriver.Conn,
	source string,
	statistics *opensplunkv1.IndexStats,
	indexID string,
	visibilityCutoff uint64,
) {
	t.Helper()
	if statistics == nil ||
		statistics.GetIndexId() != indexID ||
		statistics.GetMeasuredAt() == nil ||
		statistics.GetMeasuredAt().CheckValid() != nil ||
		!statistics.GetEstimates() {
		t.Fatalf("backend index %s statistics metadata = %#v", source, statistics)
	}
	measuredAt := statistics.GetMeasuredAt().AsTime().UTC()
	cutoffText := measuredAt.Format("2006-01-02 15:04:05.000")
	var expectedCount uint64
	var expectedEarliest, expectedLatest *time.Time
	if err := connection.QueryRow(
		ctx,
		`SELECT count(), minOrNull(event_time), maxOrNull(event_time)
		   FROM open_splunk.events
		  PREWHERE tenant_id = ? AND index_name = ?
		  WHERE expires_at > parseDateTime64BestEffort(?, 3, 'UTC')
		    AND index_time <= parseDateTime64BestEffort(?, 3, 'UTC')
		    AND visibility_seq <= ?`,
		verticalTenantID,
		verticalIndexName,
		cutoffText,
		cutoffText,
		visibilityCutoff,
	).Scan(
		&expectedCount,
		&expectedEarliest,
		&expectedLatest,
	); err != nil {
		t.Fatalf("read expected backend index %s statistics: %v", source, err)
	}
	if expectedCount == 0 ||
		expectedEarliest == nil ||
		expectedLatest == nil ||
		statistics.GetEventCount() != expectedCount ||
		statistics.GetEarliestEventTime() == nil ||
		statistics.GetLatestEventTime() == nil ||
		!statistics.GetEarliestEventTime().AsTime().Equal(*expectedEarliest) ||
		!statistics.GetLatestEventTime().AsTime().Equal(*expectedLatest) {
		t.Fatalf(
			"backend index %s statistics = %#v, want count=%d bounds=%v..%v",
			source,
			statistics,
			expectedCount,
			expectedEarliest,
			expectedLatest,
		)
	}
	var activeTableBytes uint64
	if err := connection.QueryRow(
		ctx,
		`SELECT coalesce(sum(bytes_on_disk), toUInt64(0))
		   FROM system.parts
		  WHERE database = ? AND table = ? AND active = 1`,
		"open_splunk",
		"events",
	).Scan(&activeTableBytes); err != nil {
		t.Fatalf("read backend index %s statistics storage bound: %v", source, err)
	}
	if statistics.GetStorageBytes() == 0 ||
		statistics.GetStorageBytes() > activeTableBytes {
		t.Fatalf(
			"backend index %s storage estimate = %d, want 0 < estimate <= %d",
			source,
			statistics.GetStorageBytes(),
			activeTableBytes,
		)
	}
}

func insertBulkEvents(t *testing.T, ctx context.Context, connection clickhousedriver.Conn, visibilityCutoff uint64) time.Time {
	t.Helper()
	if visibilityCutoff == 0 {
		t.Fatal("bulk fixture visibility cutoff must be positive")
	}
	batch, err := connection.PrepareBatch(ctx, clickHouseEventInsertSQL)
	if err != nil {
		t.Fatalf("prepare bulk integration events: %v", err)
	}
	start := time.Now().UTC().Add(-time.Minute).Truncate(time.Second)
	indexTime := start
	expiresAt := start.Add(24 * time.Hour)
	for index := uint64(0); index < bulkEventCount; index++ {
		eventID := fmt.Sprintf("vertical-bulk-%05d", index)
		message := "bulk export " + eventID
		document := clickhousedriver.NewJSON()
		if err := batch.Append(
			eventID, verticalTenantID, bulkIndexName, start.Add(time.Duration(index)*time.Microsecond), indexTime,
			nil, uint8(1), "vertical-host", "bulk.log", "integration", nil, uint8(1), nil, &message, []byte(message),
			uint8(1), nil, nil, document, []string(nil), "integration-direct", "vertical-bulk-batch", uint64(1),
			expiresAt, visibilityCutoff,
		); err != nil {
			t.Fatalf("append bulk integration event %d: %v", index, err)
		}
	}
	if err := batch.Send(); err != nil {
		t.Fatalf("insert bulk integration events: %v", err)
	}
	var stored uint64
	if err := connection.QueryRow(ctx,
		"SELECT count() FROM open_splunk.events WHERE tenant_id = ? AND index_name = ?",
		verticalTenantID, bulkIndexName,
	).Scan(&stored); err != nil {
		t.Fatalf("count bulk integration events: %v", err)
	}
	if stored != bulkEventCount {
		t.Fatalf("stored bulk event count = %d, want %d", stored, bulkEventCount)
	}
	return start
}

func insertBulkSnapshotDecoys(
	t *testing.T,
	ctx context.Context,
	connection clickhousedriver.Conn,
	fixtureStart, indexTimeCutoff time.Time,
	visibilityCutoff uint64,
) {
	t.Helper()
	if indexTimeCutoff.IsZero() || !indexTimeCutoff.After(fixtureStart) {
		t.Fatalf("bulk search index-time cutoff = %s, fixture start = %s", indexTimeCutoff, fixtureStart)
	}
	if visibilityCutoff == 0 {
		t.Fatalf("bulk search visibility cutoff = %d", visibilityCutoff)
	}
	type decoy struct {
		id            string
		tenant        string
		index         string
		eventTime     time.Time
		indexTime     time.Time
		visibilitySeq uint64
	}
	decoys := []decoy{
		{id: "vertical-decoy-late-visibility", tenant: verticalTenantID, index: bulkIndexName,
			eventTime: fixtureStart.Add(time.Second), indexTime: fixtureStart, visibilitySeq: ^uint64(0)},
		{id: "vertical-decoy-late-index-time", tenant: verticalTenantID, index: bulkIndexName,
			eventTime: fixtureStart.Add(time.Second), indexTime: indexTimeCutoff.Add(time.Minute), visibilitySeq: visibilityCutoff},
		{id: "vertical-decoy-foreign-tenant", tenant: "vertical-foreign-tenant", index: bulkIndexName,
			eventTime: fixtureStart.Add(time.Second), indexTime: fixtureStart, visibilitySeq: visibilityCutoff},
		{id: "vertical-decoy-wrong-index", tenant: verticalTenantID, index: verticalIndexName,
			eventTime: fixtureStart.Add(time.Second), indexTime: fixtureStart, visibilitySeq: visibilityCutoff},
		{id: "vertical-decoy-out-of-range", tenant: verticalTenantID, index: bulkIndexName,
			eventTime: fixtureStart.Add(3 * time.Minute), indexTime: fixtureStart, visibilitySeq: visibilityCutoff},
	}

	batch, err := connection.PrepareBatch(ctx, clickHouseEventInsertSQL)
	if err != nil {
		t.Fatalf("prepare snapshot decoys: %v", err)
	}
	expiresAt := indexTimeCutoff.Add(24 * time.Hour)
	for index, decoy := range decoys {
		message := "snapshot predicate decoy " + decoy.id
		document := clickhousedriver.NewJSON()
		if err := batch.Append(
			decoy.id, decoy.tenant, decoy.index, decoy.eventTime, decoy.indexTime,
			nil, uint8(1), "vertical-host", "snapshot-decoys.log", "integration", nil, uint8(1), nil,
			&message, []byte(message), uint8(1), nil, nil, document, []string(nil), "integration-direct",
			"vertical-decoy-batch", uint64(index+1), expiresAt, decoy.visibilitySeq,
		); err != nil {
			t.Fatalf("append snapshot decoy %q: %v", decoy.id, err)
		}
	}
	if err := batch.Send(); err != nil {
		t.Fatalf("insert snapshot decoys: %v", err)
	}
	var stored uint64
	if err := connection.QueryRow(ctx,
		"SELECT count() FROM open_splunk.events WHERE event_id LIKE 'vertical-decoy-%'",
	).Scan(&stored); err != nil {
		t.Fatalf("count snapshot decoys: %v", err)
	}
	if stored != uint64(len(decoys)) {
		t.Fatalf("stored snapshot decoys = %d, want %d", stored, len(decoys))
	}
}

func assertTruncatedPreviewExportsAllRows(
	t *testing.T,
	ctx context.Context,
	client *http.Client,
	connection clickhousedriver.Conn,
	baseURL string,
	fixtureStart time.Time,
	visibilityCutoff uint64,
) string {
	t.Helper()
	earliest := fixtureStart.Add(-time.Minute).Format(time.RFC3339Nano)
	latest := fixtureStart.Add(2 * time.Minute).Format(time.RFC3339Nano)
	timezone := "UTC"
	var created opensplunkv1.CreateSearchJobResponse
	postProto(t, ctx, client, baseURL+"/api/v1/search/jobs/create", &opensplunkv1.CreateSearchJobRequest{
		Definition: &opensplunkv1.SearchDefinition{
			Spl: bulkSearchSPL,
			TimeRange: &opensplunkv1.TimeRangeSpec{
				Earliest: &earliest,
				Latest:   &latest,
				Timezone: &timezone,
			},
			IndexScope: []string{bulkIndexName},
		},
	}, &created)
	jobID := created.GetSearchJob().GetSearchJobId()
	if jobID == "" {
		t.Fatalf("created bulk search job = %+v", created.GetSearchJob())
	}
	completed := waitForCompletedSearch(t, ctx, client, baseURL, jobID, 60*time.Second)
	if !completed.GetResultsTruncated() || completed.GetProgress().GetProducedRows() != 10_000 ||
		len(completed.GetEffectiveIndexScope()) != 1 || completed.GetEffectiveIndexScope()[0] != bulkIndexName {
		t.Fatalf("completed bulk search = %+v", completed)
	}
	foundTruncationWarning := false
	for _, warning := range completed.GetWarnings() {
		if warning.GetCode() == "RESULTS_TRUNCATED" {
			foundTruncationWarning = true
		}
	}
	if !foundTruncationWarning {
		t.Fatalf("completed bulk search warnings = %+v", completed.GetWarnings())
	}
	if completed.GetIndexTimeCutoff() == nil {
		t.Fatalf("completed bulk search lacks an index-time cutoff: %+v", completed)
	}

	pageSize := uint32(128)
	var preview opensplunkv1.GetSearchResultsResponse
	postProto(t, ctx, client, baseURL+"/api/v1/search/jobs/results", &opensplunkv1.GetSearchResultsRequest{
		SearchJobId: jobID,
		Page:        &opensplunkv1.PageRequest{PageSize: &pageSize, IncludeTotalSize: true},
	}, &preview)
	page := preview.GetResultPage()
	if page.GetSnapshotComplete() || page.GetPage().GetTotalSizeExact() || page.GetPage().GetTotalSize() != 10_000 ||
		len(page.GetRows()) != int(pageSize) {
		t.Fatalf("truncated bulk preview = %+v", page)
	}

	// Insert one decoy for every replay predicate only after the search has
	// captured its immutable cutoffs. Export must not admit any of them.
	insertBulkSnapshotDecoys(t, ctx, connection, fixtureStart, completed.GetIndexTimeCutoff().AsTime(), visibilityCutoff)
	exported, artifact, downloadToken := exportAndDownloadJSONLines(t, ctx, client, baseURL, jobID, []string{"event_id"}, bulkEventCount)
	if exported.GetArtifact().GetRowCount() != bulkEventCount {
		t.Fatalf("bulk export artifact = %+v", exported.GetArtifact())
	}
	assertCompleteBulkArtifact(t, artifact)
	return downloadToken
}

func waitForCompletedSearch(
	t *testing.T,
	ctx context.Context,
	client *http.Client,
	baseURL, jobID string,
	timeout time.Duration,
) *opensplunkv1.SearchJob {
	t.Helper()
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		var got opensplunkv1.GetSearchJobResponse
		postProto(t, ctx, client, baseURL+"/api/v1/search/jobs/get", &opensplunkv1.GetSearchJobRequest{SearchJobId: jobID}, &got)
		job := got.GetSearchJob()
		switch job.GetState() {
		case opensplunkv1.SearchJobState_SEARCH_JOB_STATE_COMPLETED:
			return job
		case opensplunkv1.SearchJobState_SEARCH_JOB_STATE_FAILED,
			opensplunkv1.SearchJobState_SEARCH_JOB_STATE_CANCELED,
			opensplunkv1.SearchJobState_SEARCH_JOB_STATE_EXPIRED:
			t.Fatalf("search job terminated in %s: %+v", job.GetState(), job.GetFailure())
		}
		select {
		case <-ctx.Done():
			t.Fatalf("wait for search: %v", ctx.Err())
		case <-deadline.C:
			t.Fatalf("wait for search %q: timed out", jobID)
		case <-ticker.C:
		}
	}
}

func assertCompleteBulkArtifact(t *testing.T, artifact []byte) {
	t.Helper()
	seen := make(map[uint64]struct{}, bulkEventCount)
	forEachJSONLine(t, artifact, func(_ int, row map[string]any) {
		if len(row) != 1 {
			t.Fatalf("bulk JSON Lines row = %#v", row)
		}
		eventID, ok := row["event_id"].(string)
		if !ok || !strings.HasPrefix(eventID, "vertical-bulk-") {
			t.Fatalf("bulk event ID = %#v", row["event_id"])
		}
		sequence, err := strconv.ParseUint(strings.TrimPrefix(eventID, "vertical-bulk-"), 10, 64)
		if err != nil || sequence >= bulkEventCount {
			t.Fatalf("bulk event ID = %q", eventID)
		}
		if _, duplicate := seen[sequence]; duplicate {
			t.Fatalf("bulk export duplicated event ID %q", eventID)
		}
		seen[sequence] = struct{}{}
	})
	if uint64(len(seen)) != bulkEventCount {
		t.Fatalf("bulk exported row count = %d, want %d", len(seen), bulkEventCount)
	}
}

func forEachJSONLine(t *testing.T, artifact []byte, visit func(int, map[string]any)) {
	t.Helper()
	if len(artifact) == 0 || artifact[len(artifact)-1] != '\n' {
		t.Fatal("JSON Lines artifact is empty or lacks its final newline")
	}
	scanner := bufio.NewScanner(bytes.NewReader(artifact))
	scanner.Buffer(make([]byte, 64<<10), 1<<20)
	line := 0
	for scanner.Scan() {
		line++
		if len(scanner.Bytes()) == 0 {
			t.Fatalf("JSON Lines artifact contains an empty line at %d", line)
		}
		decoder := json.NewDecoder(bytes.NewReader(scanner.Bytes()))
		decoder.UseNumber()
		var row map[string]any
		if err := decoder.Decode(&row); err != nil {
			t.Fatalf("decode JSON Lines row %d: %v", line, err)
		}
		var trailing any
		if err := decoder.Decode(&trailing); err != io.EOF {
			t.Fatalf("JSON Lines row %d contains trailing data: %v", line, err)
		}
		visit(line, row)
	}
	if err := scanner.Err(); err != nil {
		t.Fatalf("scan JSON Lines artifact: %v", err)
	}
}

func runSearch(t *testing.T, ctx context.Context, client *http.Client, baseURL string, fixtureStart time.Time) completedSearch {
	t.Helper()
	earliest := fixtureStart.Format(time.RFC3339Nano)
	latest := fixtureStart.Add(5500 * time.Millisecond).Format(time.RFC3339Nano)
	timezone := "UTC"
	var created opensplunkv1.CreateSearchJobResponse
	postProto(t, ctx, client, baseURL+"/api/v1/search/jobs/create", &opensplunkv1.CreateSearchJobRequest{
		Definition: &opensplunkv1.SearchDefinition{
			Spl: verticalSearchSPL,
			TimeRange: &opensplunkv1.TimeRangeSpec{
				Earliest: &earliest,
				Latest:   &latest,
				Timezone: &timezone,
			},
			IndexScope: []string{verticalIndexName},
		},
	}, &created)
	jobID := created.GetSearchJob().GetSearchJobId()
	if jobID == "" {
		t.Fatalf("created search job = %+v", created.GetSearchJob())
	}
	terminal := observeCompletedSearchWebSocket(t, ctx, baseURL, jobID)

	// WebSocket events are sequenced notifications, not the source of truth.
	// Read the authoritative terminal snapshot and full results over HTTP.
	var got opensplunkv1.GetSearchJobResponse
	postProto(t, ctx, client, baseURL+"/api/v1/search/jobs/get", &opensplunkv1.GetSearchJobRequest{SearchJobId: jobID}, &got)
	job := got.GetSearchJob()
	if job.GetState() != opensplunkv1.SearchJobState_SEARCH_JOB_STATE_COMPLETED ||
		job.GetStateVersion() != terminal.GetStateVersion() ||
		job.GetProgress().GetStateVersion() != job.GetStateVersion() ||
		terminal.GetFinalProgress().GetStateVersion() != terminal.GetStateVersion() ||
		job.GetProgress().GetProducedRows() != terminal.GetFinalProgress().GetProducedRows() {
		t.Fatalf("authoritative search job = %+v, websocket terminal = %+v", job, terminal)
	}
	t.Logf("completed search scope: indexes=%v range=%v cutoff=%v rows=%d",
		job.GetEffectiveIndexScope(), job.GetResolvedTimeRange(), job.GetIndexTimeCutoff(), job.GetProgress().GetProducedRows())

	results := fetchAllVerticalSearchResults(t, ctx, client, baseURL, jobID)
	waitForTerminalHistory(t, ctx, client, baseURL, jobID)
	return completedSearch{jobID: jobID, results: results}
}

func fetchAllVerticalSearchResults(
	t *testing.T,
	ctx context.Context,
	client *http.Client,
	baseURL, jobID string,
) *collectedVerticalSearchResults {
	t.Helper()
	return fetchAllCompletedSearchResults(t, ctx, client, baseURL, jobID, verticalEventCount, 2)
}

func fetchAllCompletedSearchResults(
	t *testing.T,
	ctx context.Context,
	client *http.Client,
	baseURL, jobID string,
	expectedRows uint64,
	pageSize uint32,
) *collectedVerticalSearchResults {
	t.Helper()
	if expectedRows == 0 || pageSize == 0 {
		t.Fatalf("completed result paging requires positive rows/page size, got %d/%d", expectedRows, pageSize)
	}
	expectedPages := (expectedRows + uint64(pageSize) - 1) / uint64(pageSize)
	var (
		schema     *opensplunkv1.ResultSchema
		rows       []*opensplunkv1.ResultRow
		nextToken  string
		pageCount  int
		seenTokens = make(map[string]struct{})
		seenRowIDs = make(map[string]struct{})
		pageWire   [][]byte
	)
	for {
		requestPage := &opensplunkv1.PageRequest{
			PageSize:         &pageSize,
			IncludeTotalSize: true,
		}
		if nextToken != "" {
			requestPage.PageToken = &nextToken
		}
		var current opensplunkv1.GetSearchResultsResponse
		wire := postProto(t, ctx, client, baseURL+"/api/v1/search/jobs/results", &opensplunkv1.GetSearchResultsRequest{
			SearchJobId: jobID,
			Page:        requestPage,
		}, &current)
		pageWire = append(pageWire, bytes.Clone(wire))
		pageCount++
		page := current.GetResultPage()
		if current.GetSearchJobId() != jobID || page == nil || page.GetSchema() == nil || page.GetPage() == nil {
			t.Fatalf("search result page %d = %+v", pageCount, &current)
		}
		if !page.GetSnapshotComplete() ||
			!page.GetPage().GetTotalSizeExact() || page.GetPage().GetTotalSize() != expectedRows ||
			len(page.GetRows()) == 0 || len(page.GetRows()) > int(pageSize) {
			t.Fatalf("search result page %d metadata = %+v", pageCount, page)
		}
		if schema == nil {
			schema = proto.Clone(page.GetSchema()).(*opensplunkv1.ResultSchema)
		} else if !proto.Equal(schema, page.GetSchema()) {
			t.Fatalf("search result schema changed on page %d: first=%+v current=%+v", pageCount, schema, page.GetSchema())
		}
		for _, row := range page.GetRows() {
			if row.GetRowId() == "" {
				t.Fatalf("search result page %d contains an empty row ID", pageCount)
			}
			if _, duplicate := seenRowIDs[row.GetRowId()]; duplicate {
				t.Fatalf("search result row ID %q repeated across cursor pages", row.GetRowId())
			}
			if row.GetOrdinal() != uint64(len(rows)) {
				t.Fatalf("search result row %q ordinal = %d, want %d", row.GetRowId(), row.GetOrdinal(), len(rows))
			}
			seenRowIDs[row.GetRowId()] = struct{}{}
			rows = append(rows, proto.Clone(row).(*opensplunkv1.ResultRow))
		}

		returnedToken := page.GetPage().GetNextPageToken()
		if returnedToken == "" {
			break
		}
		if _, repeated := seenTokens[returnedToken]; repeated {
			t.Fatalf("search result cursor repeated on page %d", pageCount)
		}
		seenTokens[returnedToken] = struct{}{}
		nextToken = returnedToken
		if uint64(pageCount) >= expectedPages {
			t.Fatalf("search result cursor exceeded %d bounded pages", expectedPages)
		}
	}
	if uint64(pageCount) != expectedPages || uint64(len(rows)) != expectedRows {
		t.Fatalf(
			"collected search results: pages=%d rows=%d, want pages=%d rows=%d",
			pageCount,
			len(rows),
			expectedPages,
			expectedRows,
		)
	}
	return &collectedVerticalSearchResults{schema: schema, rows: rows, responseWire: pageWire}
}

func assertCurrentGradeThisSearchResults(
	t *testing.T,
	searchID gradethiscorpus.MigrationSearchID,
	profile gradethiscorpus.MigrationProfile,
	results *collectedVerticalSearchResults,
) {
	t.Helper()
	switch searchID {
	case gradethiscorpus.MigrationSearchFollowTrace:
		assertCurrentGradeThisColumns(t, results,
			[]string{"_time", "level", "layer", "logger", "message"},
			[]opensplunkv1.ValueType{
				opensplunkv1.ValueType_VALUE_TYPE_TIMESTAMP,
				opensplunkv1.ValueType_VALUE_TYPE_STRING,
				opensplunkv1.ValueType_VALUE_TYPE_MIXED,
				opensplunkv1.ValueType_VALUE_TYPE_MIXED,
				opensplunkv1.ValueType_VALUE_TYPE_STRING,
			},
		)
		for rowIndex, eventID := range []string{"trace-api", "trace-service", "trace-database"} {
			event := currentGradeThisEvent(t, profile, eventID)
			cells := results.rows[rowIndex].GetCells()
			requireCurrentGradeThisTime(t, cells[0], event.Timestamp)
			requireCurrentGradeThisString(t, cells[1], event.Level)
			requireCurrentGradeThisString(t, cells[2], event.Layer)
			requireCurrentGradeThisString(t, cells[3], event.Logger)
			requireCurrentGradeThisString(t, cells[4], event.Message)
		}

	case gradethiscorpus.MigrationSearchSeverityCounts:
		assertCurrentGradeThisColumns(t, results,
			[]string{"level", "count"},
			[]opensplunkv1.ValueType{
				opensplunkv1.ValueType_VALUE_TYPE_STRING,
				opensplunkv1.ValueType_VALUE_TYPE_UINT64,
			},
		)
		for rowIndex, expected := range []struct {
			level string
			count uint64
		}{
			{level: "INFO", count: 10},
			{level: "WARN", count: 6},
			{level: "ERROR", count: 4},
		} {
			cells := results.rows[rowIndex].GetCells()
			requireCurrentGradeThisString(t, cells[0], expected.level)
			requireCurrentGradeThisUnsigned(t, cells[1], expected.count)
		}

	case gradethiscorpus.MigrationSearchFailedRequests:
		assertCurrentGradeThisColumns(t, results,
			[]string{"_time", "level", "path", "status", "duration", "trace_id"},
			[]opensplunkv1.ValueType{
				opensplunkv1.ValueType_VALUE_TYPE_TIMESTAMP,
				opensplunkv1.ValueType_VALUE_TYPE_STRING,
				opensplunkv1.ValueType_VALUE_TYPE_MIXED,
				opensplunkv1.ValueType_VALUE_TYPE_MIXED,
				opensplunkv1.ValueType_VALUE_TYPE_MIXED,
				opensplunkv1.ValueType_VALUE_TYPE_STRING,
			},
		)
		for rowIndex, eventID := range []string{"assessments-server-error", "submissions-server-error"} {
			event := currentGradeThisEvent(t, profile, eventID)
			cells := results.rows[rowIndex].GetCells()
			requireCurrentGradeThisTime(t, cells[0], event.Timestamp)
			requireCurrentGradeThisString(t, cells[1], event.Level)
			requireCurrentGradeThisString(t, cells[2], event.Path)
			requireCurrentGradeThisSigned(t, cells[3], event.Status)
			requireCurrentGradeThisString(t, cells[4], event.Duration)
			requireCurrentGradeThisString(t, cells[5], event.TraceID)
		}

	case gradethiscorpus.MigrationSearchPathStatus:
		assertCurrentGradeThisColumns(t, results,
			[]string{"path", "status", "count"},
			[]opensplunkv1.ValueType{
				opensplunkv1.ValueType_VALUE_TYPE_STRING,
				opensplunkv1.ValueType_VALUE_TYPE_STRING,
				opensplunkv1.ValueType_VALUE_TYPE_UINT64,
			},
		)
		for rowIndex, expected := range []struct {
			path   string
			status string
			count  uint64
		}{
			{path: "/api/v1/assessments", status: "200", count: 2},
			{path: "/api/v1/submissions", status: "200", count: 2},
			{path: "/api/v1/assessments", status: "429", count: 1},
			{path: "/api/v1/assessments", status: "503", count: 1},
			{path: "/api/v1/reports", status: "200", count: 1},
			{path: "/api/v1/reports", status: "204", count: 1},
			{path: "/api/v1/reports", status: "404", count: 1},
			{path: "/api/v1/submissions", status: "500", count: 1},
		} {
			cells := results.rows[rowIndex].GetCells()
			requireCurrentGradeThisString(t, cells[0], expected.path)
			requireCurrentGradeThisString(t, cells[1], expected.status)
			requireCurrentGradeThisUnsigned(t, cells[2], expected.count)
		}

	case gradethiscorpus.MigrationSearchDurationUnits:
		assertCurrentGradeThisColumns(t, results,
			[]string{"duration_unit", "count"},
			[]opensplunkv1.ValueType{
				opensplunkv1.ValueType_VALUE_TYPE_STRING,
				opensplunkv1.ValueType_VALUE_TYPE_UINT64,
			},
		)
		for rowIndex, expected := range []struct {
			unit  string
			count uint64
		}{
			{unit: "ms", count: 7},
			{unit: "s", count: 2},
			{unit: "µs", count: 1},
		} {
			cells := results.rows[rowIndex].GetCells()
			requireCurrentGradeThisString(t, cells[0], expected.unit)
			requireCurrentGradeThisUnsigned(t, cells[1], expected.count)
		}

	case gradethiscorpus.MigrationSearchTopMessages:
		assertCurrentGradeThisColumns(t, results,
			[]string{"message", "count", "percent"},
			[]opensplunkv1.ValueType{
				opensplunkv1.ValueType_VALUE_TYPE_STRING,
				opensplunkv1.ValueType_VALUE_TYPE_UINT64,
				opensplunkv1.ValueType_VALUE_TYPE_DOUBLE,
			},
		)
		for rowIndex, expected := range []struct {
			message string
			count   uint64
			percent float64
		}{
			{message: "Request summary statistics", count: 10, percent: 50},
			{message: "Heartbeat", count: 3, percent: 15},
			{message: "Cache refresh delayed", count: 2, percent: 10},
		} {
			cells := results.rows[rowIndex].GetCells()
			requireCurrentGradeThisString(t, cells[0], expected.message)
			requireCurrentGradeThisUnsigned(t, cells[1], expected.count)
			requireCurrentGradeThisDouble(t, cells[2], expected.percent)
		}

	default:
		t.Fatalf("unknown current GradeThis search %q", searchID)
	}
}

func assertCurrentGradeThisColumns(
	t *testing.T,
	results *collectedVerticalSearchResults,
	names []string,
	types []opensplunkv1.ValueType,
) {
	t.Helper()
	if results == nil || results.schema == nil || len(names) != len(types) ||
		len(results.schema.GetColumns()) != len(names) {
		t.Fatalf("current GradeThis result schema = %+v, want names=%v types=%v", results, names, types)
	}
	for index, column := range results.schema.GetColumns() {
		if column.GetFieldName() != names[index] || column.GetValueType() != types[index] {
			t.Fatalf(
				"current GradeThis result column %d = %+v, want name=%q type=%s",
				index,
				column,
				names[index],
				types[index],
			)
		}
	}
	for rowIndex, row := range results.rows {
		if len(row.GetCells()) != len(names) {
			t.Fatalf(
				"current GradeThis result row %d has %d cells, want %d",
				rowIndex,
				len(row.GetCells()),
				len(names),
			)
		}
	}
}

func currentGradeThisEvent(
	t *testing.T,
	profile gradethiscorpus.MigrationProfile,
	eventID string,
) gradethiscorpus.MigrationEvent {
	t.Helper()
	for _, event := range profile.Events {
		if event.ID == eventID {
			return event
		}
	}
	t.Fatalf("current GradeThis profile has no event %q", eventID)
	return gradethiscorpus.MigrationEvent{}
}

func requireCurrentGradeThisString(t *testing.T, value *opensplunkv1.TypedValue, want string) {
	t.Helper()
	if _, ok := value.GetKind().(*opensplunkv1.TypedValue_StringValue); !ok ||
		value.GetStringValue() != want {
		t.Fatalf("current GradeThis cell = %+v, want string(%q)", value, want)
	}
}

func requireCurrentGradeThisSigned(t *testing.T, value *opensplunkv1.TypedValue, want int64) {
	t.Helper()
	if _, ok := value.GetKind().(*opensplunkv1.TypedValue_Sint64Value); !ok ||
		value.GetSint64Value() != want {
		t.Fatalf("current GradeThis cell = %+v, want sint64(%d)", value, want)
	}
}

func requireCurrentGradeThisUnsigned(t *testing.T, value *opensplunkv1.TypedValue, want uint64) {
	t.Helper()
	if _, ok := value.GetKind().(*opensplunkv1.TypedValue_Uint64Value); !ok ||
		value.GetUint64Value() != want {
		t.Fatalf("current GradeThis cell = %+v, want uint64(%d)", value, want)
	}
}

func requireCurrentGradeThisDouble(t *testing.T, value *opensplunkv1.TypedValue, want float64) {
	t.Helper()
	if _, ok := value.GetKind().(*opensplunkv1.TypedValue_DoubleValue); !ok ||
		value.GetDoubleValue() != want {
		t.Fatalf("current GradeThis cell = %+v, want double(%v)", value, want)
	}
}

func requireCurrentGradeThisTime(t *testing.T, value *opensplunkv1.TypedValue, want time.Time) {
	t.Helper()
	timestamp := value.GetTimestampValue()
	if _, ok := value.GetKind().(*opensplunkv1.TypedValue_TimestampValue); !ok ||
		timestamp == nil ||
		timestamp.CheckValid() != nil ||
		!timestamp.AsTime().Equal(want) {
		t.Fatalf("current GradeThis cell = %+v, want timestamp(%s)", value, want.Format(time.RFC3339Nano))
	}
}

func assertCompletedTimeline(
	t *testing.T,
	ctx context.Context,
	client *http.Client,
	baseURL, searchJobID string,
	fixtureStart time.Time,
) {
	t.Helper()
	maximumBuckets := uint32(10)
	var timeline opensplunkv1.GetSearchTimelineResponse
	postProto(t, ctx, client, baseURL+"/api/v1/search/jobs/timeline", &opensplunkv1.GetSearchTimelineRequest{
		SearchJobId:          searchJobID,
		MaxBuckets:           &maximumBuckets,
		PreferredBucketWidth: durationpb.New(time.Second),
	}, &timeline)

	if !timeline.GetComplete() || timeline.GetBucketWidth() == nil ||
		timeline.GetBucketWidth().AsDuration() != time.Second {
		t.Fatalf("completed timeline metadata = %+v", &timeline)
	}
	wantCounts := []uint64{1, 1, 1, 1, 0, 0}
	buckets := timeline.GetBuckets()
	if len(buckets) != len(wantCounts) {
		t.Fatalf("completed timeline bucket count = %d, want %d: %+v", len(buckets), len(wantCounts), &timeline)
	}
	wantSearchLatest := fixtureStart.Add(5500 * time.Millisecond)
	var total uint64
	for index, bucket := range buckets {
		if bucket.GetEarliest() == nil || bucket.GetLatest() == nil {
			t.Fatalf("timeline bucket %d is missing a boundary: %+v", index, bucket)
		}
		if err := bucket.GetEarliest().CheckValid(); err != nil {
			t.Fatalf("timeline bucket %d earliest is invalid: %v", index, err)
		}
		if err := bucket.GetLatest().CheckValid(); err != nil {
			t.Fatalf("timeline bucket %d latest is invalid: %v", index, err)
		}
		wantEarliest := fixtureStart.Add(time.Duration(index) * time.Second)
		wantLatest := wantEarliest.Add(time.Second)
		wantPartial := false
		if wantLatest.After(wantSearchLatest) {
			wantLatest = wantSearchLatest
			wantPartial = true
		}
		if !bucket.GetEarliest().AsTime().Equal(wantEarliest) ||
			!bucket.GetLatest().AsTime().Equal(wantLatest) ||
			bucket.GetEventCount() != wantCounts[index] || bucket.GetPartial() != wantPartial {
			t.Fatalf("timeline bucket %d = %+v, want [%s,%s) count=%d partial=%t", index, bucket,
				wantEarliest.Format(time.RFC3339Nano), wantLatest.Format(time.RFC3339Nano), wantCounts[index], wantPartial)
		}
		total += bucket.GetEventCount()
	}
	// The fixture has a stored event exactly at each search boundary. The first
	// is counted and the direct latest-boundary event is excluded, proving the
	// search's half-open [earliest, latest) contract through real SQL execution.
	if total != verticalEventCount || buckets[0].GetEventCount() != 1 || buckets[len(buckets)-1].GetEventCount() != 0 {
		t.Fatalf("completed timeline total = %d, first=%d last=%d, want %d/1/0",
			total, buckets[0].GetEventCount(), buckets[len(buckets)-1].GetEventCount(), verticalEventCount)
	}
}

func observeCompletedSearchWebSocket(
	t *testing.T,
	ctx context.Context,
	baseURL, jobID string,
) *opensplunkv1.SearchJobTerminal {
	t.Helper()
	watchContext, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()

	endpoint, err := url.Parse(baseURL)
	if err != nil {
		t.Fatalf("parse backend URL for search websocket: %v", err)
	}
	switch endpoint.Scheme {
	case "http":
		endpoint.Scheme = "ws"
	case "https":
		endpoint.Scheme = "wss"
	default:
		t.Fatalf("unsupported backend URL scheme %q", endpoint.Scheme)
	}
	endpoint.Path = "/api/v1/search/ws"
	endpoint.RawPath = ""
	endpoint.RawQuery = ""
	endpoint.Fragment = ""

	dialer := websocket.Dialer{HandshakeTimeout: 5 * time.Second}
	connection, response, err := dialer.DialContext(watchContext, endpoint.String(), http.Header{
		"Origin":         []string{baseURL},
		"Sec-Fetch-Site": []string{"same-origin"},
	})
	if err != nil {
		status := 0
		if response != nil {
			status = response.StatusCode
			_ = response.Body.Close()
		}
		t.Fatalf("connect search websocket: %v (status %d)", err, status)
	}
	if response == nil || response.StatusCode != http.StatusSwitchingProtocols {
		if response != nil {
			_ = response.Body.Close()
		}
		_ = connection.Close()
		t.Fatalf("search websocket upgrade response = %#v", response)
	}
	_ = response.Body.Close()
	defer func() {
		_ = connection.WriteControl(
			websocket.CloseMessage,
			websocket.FormatCloseMessage(websocket.CloseNormalClosure, "integration observation complete"),
			time.Now().Add(time.Second),
		)
		_ = connection.Close()
	}()
	connection.SetReadLimit(1 << 20)

	deadline, ok := watchContext.Deadline()
	if !ok {
		t.Fatal("search websocket observation has no deadline")
	}
	if err := connection.SetWriteDeadline(deadline); err != nil {
		t.Fatalf("set search websocket write deadline: %v", err)
	}
	previewRowLimit := uint32(2)
	command := &opensplunkv1.SearchWebSocketCommand{
		RequestId: "backend-vertical-subscribe",
		Payload: &opensplunkv1.SearchWebSocketCommand_Subscribe{Subscribe: &opensplunkv1.SubscribeSearchJobsCommand{
			Subscriptions: []*opensplunkv1.SearchSubscription{{
				SubscriptionId: "backend-vertical-search",
				Target: &opensplunkv1.JobTarget{Target: &opensplunkv1.JobTarget_SearchJobId{
					SearchJobId: jobID,
				}},
				// Zero deliberately accepts the current terminal snapshot when this
				// four-row query finishes before the upgrade completes.
				AfterSequence:   0,
				IncludePreviews: true,
				PreviewRowLimit: &previewRowLimit,
			}},
		}},
	}
	wire, err := proto.MarshalOptions{Deterministic: true}.Marshal(command)
	if err != nil {
		t.Fatalf("marshal search websocket subscription: %v", err)
	}
	if err := connection.WriteMessage(websocket.BinaryMessage, wire); err != nil {
		t.Fatalf("write search websocket subscription: %v", err)
	}
	if err := connection.SetReadDeadline(deadline); err != nil {
		t.Fatalf("set search websocket read deadline: %v", err)
	}

	first := readBackendSearchWebSocketEvent(t, connection)
	acknowledgment := first.GetSubscriptionAcknowledged()
	if acknowledgment == nil {
		t.Fatalf("first search websocket event is not an explicit subscription acknowledgment: %+v", first)
	}
	if first.GetSequence() != 0 || acknowledgment.GetRequestId() != command.GetRequestId() ||
		acknowledgment.GetSubscriptionId() != "backend-vertical-search" ||
		acknowledgment.GetTarget().GetSearchJobId() != jobID {
		t.Fatalf("search websocket acknowledgment = %+v, sequence = %d", acknowledgment, first.GetSequence())
	}

	var (
		lastSequence        uint64
		lastStateVersion    uint64
		lastProgressVersion uint64
		lastProducedRows    uint64
		lastResultBytes     uint64
		lastPreview         uint64
		schemaID            string
		schemaColumns       int
		sawState            bool
		sawProgress         bool
		sawSchema           bool
		sawPreview          bool
	)
	for frame := 0; frame < 256; frame++ {
		event := readBackendSearchWebSocketEvent(t, connection)
		if event.GetSubscriptionAcknowledged() != nil {
			t.Fatalf("duplicate search websocket acknowledgment: %+v", event)
		}
		if event.GetProtocolError() != nil {
			t.Fatalf("search websocket protocol error: %+v", event.GetProtocolError())
		}
		if event.GetResynchronizationRequired() != nil {
			t.Fatalf("fresh search websocket subscription required resynchronization: %+v", event.GetResynchronizationRequired())
		}
		if event.GetSubscriptionId() != "backend-vertical-search" || event.GetTarget().GetSearchJobId() != jobID {
			t.Fatalf("search websocket target routing = %+v", event)
		}
		if event.GetSequence() == 0 || event.GetSequence() <= lastSequence {
			t.Fatalf("search websocket target sequence = %d after %d", event.GetSequence(), lastSequence)
		}
		lastSequence = event.GetSequence()
		if event.GetOccurredAt() == nil || event.GetOccurredAt().CheckValid() != nil {
			t.Fatalf("search websocket event has invalid occurrence time: %+v", event)
		}

		switch {
		case event.GetResultPreview() != nil:
			preview := event.GetResultPreview()
			if !sawSchema || preview.GetSearchJobId() != jobID || preview.GetSchemaId() != schemaID ||
				preview.GetPreviewRevision() == 0 || preview.GetPreviewRevision() < lastPreview ||
				preview.GetUpdateMode() != opensplunkv1.PreviewUpdateMode_PREVIEW_UPDATE_MODE_RESET ||
				len(preview.GetRows()) == 0 || len(preview.GetRows()) > int(previewRowLimit) ||
				!preview.GetTruncated() {
				t.Fatalf("search websocket preview = %+v (schema=%q columns=%d revision=%d)",
					preview, schemaID, schemaColumns, lastPreview)
			}
			for rowIndex, row := range preview.GetRows() {
				if row.GetRowId() == "" || len(row.GetCells()) != schemaColumns {
					t.Fatalf("search websocket preview row %d = %+v, schema columns = %d",
						rowIndex, row, schemaColumns)
				}
				rowWire, err := proto.MarshalOptions{Deterministic: true}.Marshal(row)
				if err != nil {
					t.Fatalf("marshal search websocket preview row %d: %v", rowIndex, err)
				}
				for _, sentinel := range []string{
					redactionAPIKeySentinel,
					redactionCookieSentinel,
					redactionPrivateKeySentinel,
					redactionCredentialSentinel,
					redactionPINSentinel,
				} {
					if bytes.Contains(rowWire, []byte(sentinel)) {
						t.Fatalf("search websocket preview row %d leaked protected input", rowIndex)
					}
				}
			}
			lastPreview = preview.GetPreviewRevision()
			sawPreview = true
		case event.GetSearchStateChanged() != nil:
			state := event.GetSearchStateChanged()
			if state.GetSearchJobId() != jobID || state.GetState() == opensplunkv1.SearchJobState_SEARCH_JOB_STATE_UNSPECIFIED ||
				state.GetStateVersion() == 0 || state.GetStateVersion() < lastStateVersion {
				t.Fatalf("search websocket state event = %+v after version %d", state, lastStateVersion)
			}
			switch state.GetState() {
			case opensplunkv1.SearchJobState_SEARCH_JOB_STATE_FAILED,
				opensplunkv1.SearchJobState_SEARCH_JOB_STATE_CANCELED,
				opensplunkv1.SearchJobState_SEARCH_JOB_STATE_EXPIRED:
				t.Fatalf("search websocket reported terminal state %s before successful completion", state.GetState())
			}
			lastStateVersion = state.GetStateVersion()
			sawState = true
		case event.GetSearchProgress() != nil:
			progress := event.GetSearchProgress()
			if progress.GetPhase() == opensplunkv1.SearchExecutionPhase_SEARCH_EXECUTION_PHASE_UNSPECIFIED ||
				progress.GetUpdatedAt() == nil || progress.GetUpdatedAt().CheckValid() != nil ||
				progress.GetStateVersion() == 0 ||
				progress.GetStateVersion() < lastProgressVersion ||
				progress.GetStateVersion() < lastStateVersion ||
				progress.GetProducedRows() < lastProducedRows || progress.GetResultBytes() < lastResultBytes {
				t.Fatalf("search websocket progress event = %+v after version=%d state_version=%d rows=%d bytes=%d",
					progress, lastProgressVersion, lastStateVersion, lastProducedRows, lastResultBytes)
			}
			lastProgressVersion = progress.GetStateVersion()
			lastProducedRows = progress.GetProducedRows()
			lastResultBytes = progress.GetResultBytes()
			sawProgress = true
		case event.GetSearchTerminal() != nil:
			terminal := event.GetSearchTerminal()
			if !sawState || !sawProgress || !sawSchema || !sawPreview || terminal.GetSearchJobId() != jobID ||
				terminal.GetState() != opensplunkv1.SearchJobState_SEARCH_JOB_STATE_COMPLETED ||
				terminal.GetStateVersion() == 0 || terminal.GetStateVersion() != lastStateVersion ||
				terminal.GetFinalProgress().GetPhase() != opensplunkv1.SearchExecutionPhase_SEARCH_EXECUTION_PHASE_COMPLETE ||
				terminal.GetFinalProgress().GetStateVersion() != terminal.GetStateVersion() ||
				terminal.GetFinalProgress().GetStateVersion() != lastProgressVersion ||
				terminal.GetFinalProgress().GetProducedRows() != verticalEventCount ||
				terminal.GetFinalProgress().GetProducedRows() != lastProducedRows ||
				terminal.GetFinalProgress().GetResultBytes() != lastResultBytes || terminal.GetFailure() != nil ||
				terminal.GetResultsExpireAt() == nil || terminal.GetResultsExpireAt().CheckValid() != nil {
				t.Fatalf("search websocket terminal event = %+v (state=%t progress=%t schema=%t preview=%t last version=%d)",
					terminal, sawState, sawProgress, sawSchema, sawPreview, lastStateVersion)
			}
			return terminal
		case event.GetResultSchemaAvailable() != nil:
			available := event.GetResultSchemaAvailable()
			schema := available.GetSchema()
			if available.GetSearchJobId() != jobID || schema.GetSchemaId() == "" ||
				schema.GetRevision() == 0 || len(schema.GetColumns()) == 0 {
				t.Fatalf("search websocket result schema = %+v", available)
			}
			schemaID = schema.GetSchemaId()
			schemaColumns = len(schema.GetColumns())
			sawSchema = true
		case event.GetWarning() != nil:
			// Warnings may appear between progress and terminal.
		default:
			t.Fatalf("unexpected search websocket event: %+v", event)
		}
	}
	t.Fatal("search websocket exceeded the bounded event count before completion")
	return nil
}

func readBackendSearchWebSocketEvent(t *testing.T, connection *websocket.Conn) *opensplunkv1.SearchWebSocketEvent {
	t.Helper()
	messageType, wire, err := connection.ReadMessage()
	if err != nil {
		t.Fatalf("read search websocket event: %v", err)
	}
	if messageType != websocket.BinaryMessage {
		t.Fatalf("search websocket message type = %d, want binary", messageType)
	}
	event := new(opensplunkv1.SearchWebSocketEvent)
	if err := proto.Unmarshal(wire, event); err != nil {
		t.Fatalf("decode search websocket event: %v", err)
	}
	return event
}

func waitForTerminalHistory(t *testing.T, ctx context.Context, client *http.Client, baseURL, jobID string) {
	t.Helper()
	payload, err := proto.Marshal(&opensplunkv1.GetSearchHistoryEntryRequest{SearchJobId: jobID})
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		request, err := http.NewRequestWithContext(ctx, http.MethodPost, baseURL+"/api/v1/search/history/get", bytes.NewReader(payload))
		if err != nil {
			t.Fatal(err)
		}
		request.Header.Set("Content-Type", "application/x-protobuf")
		response, err := client.Do(request)
		if err != nil {
			t.Fatalf("get search history: %v", err)
		}
		body, readErr := io.ReadAll(io.LimitReader(response.Body, 1<<20))
		_ = response.Body.Close()
		if readErr != nil {
			t.Fatalf("read search history: %v", readErr)
		}
		if response.StatusCode == http.StatusOK {
			var got opensplunkv1.GetSearchHistoryEntryResponse
			if err := proto.Unmarshal(body, &got); err != nil {
				t.Fatalf("decode search history: %v", err)
			}
			entry := got.GetHistoryEntry()
			if entry.GetSearchJobId() != jobID || entry.GetDefinition().GetSpl() != verticalSearchSPL ||
				entry.GetFinalState() != opensplunkv1.SearchJobState_SEARCH_JOB_STATE_COMPLETED ||
				entry.GetProducedRows() != verticalEventCount || entry.GetCompilerVersion() != splCompatibilityVersionForTest ||
				len(entry.GetEffectiveIndexScope()) != 1 || entry.GetEffectiveIndexScope()[0] != verticalIndexName {
				t.Fatalf("terminal search history = %+v", entry)
			}
			return
		}
		if response.StatusCode != http.StatusNotFound {
			t.Fatalf("get search history status = %d, body = %q", response.StatusCode, body)
		}
		select {
		case <-ctx.Done():
			t.Fatalf("wait for search history: %v", ctx.Err())
		case <-deadline.C:
			t.Fatal("wait for search history: timed out")
		case <-ticker.C:
		}
	}
}

func assertTypedRedactedResults(t *testing.T, results *collectedVerticalSearchResults) {
	t.Helper()
	if results == nil || results.schema == nil || uint64(len(results.rows)) != verticalEventCount {
		t.Fatalf("collected typed results = %+v", results)
	}
	columns := make(map[string]int, len(results.schema.GetColumns()))
	for index, column := range results.schema.GetColumns() {
		columns[column.GetFieldName()] = index
	}
	for _, name := range []string{
		"message",
		"status",
		"duration_ms",
		"api_key",
		"customer_credential",
		"customer_pin",
		"_raw",
	} {
		if _, ok := columns[name]; !ok {
			t.Fatalf("result schema is missing %q: %+v", name, results.schema)
		}
	}
	if results.schema.GetColumns()[columns["status"]].GetValueType() != opensplunkv1.ValueType_VALUE_TYPE_MIXED ||
		results.schema.GetColumns()[columns["duration_ms"]].GetValueType() != opensplunkv1.ValueType_VALUE_TYPE_MIXED {
		t.Fatalf("dynamic numeric schema did not retain mixed typing: %+v", results.schema)
	}
	var sentinel *opensplunkv1.ResultRow
	for _, row := range results.rows {
		if row.GetCells()[columns["message"]].GetStringValue() == verticalSentinelMessage {
			sentinel = row
			break
		}
	}
	if sentinel == nil {
		t.Fatalf("%q row was not returned", verticalSentinelMessage)
	}
	status := sentinel.GetCells()[columns["status"]]
	if _, ok := status.GetKind().(*opensplunkv1.TypedValue_Sint64Value); !ok || status.GetSint64Value() != 201 {
		t.Fatalf("status cell = %+v, want typed sint64(201)", status)
	}
	duration := sentinel.GetCells()[columns["duration_ms"]]
	if _, ok := duration.GetKind().(*opensplunkv1.TypedValue_DoubleValue); !ok || duration.GetDoubleValue() != 12.5 {
		t.Fatalf("duration_ms cell = %+v, want typed double(12.5)", duration)
	}
	redacted := sentinel.GetCells()[columns["api_key"]]
	if _, ok := redacted.GetKind().(*opensplunkv1.TypedValue_StringValue); !ok || redacted.GetStringValue() != "[REDACTED]" {
		t.Fatalf("api_key cell = %+v, want redacted string", redacted)
	}
	credential := sentinel.GetCells()[columns["customer_credential"]]
	if _, ok := credential.GetKind().(*opensplunkv1.TypedValue_StringValue); !ok ||
		credential.GetStringValue() != redactionCredentialMarker {
		t.Fatalf("customer_credential cell = %+v, want %q", credential, redactionCredentialMarker)
	}
	pin := sentinel.GetCells()[columns["customer_pin"]]
	if _, ok := pin.GetKind().(*opensplunkv1.TypedValue_StringValue); !ok ||
		pin.GetStringValue() != redactionPINMarker {
		t.Fatalf("customer_pin cell = %+v, want %q", pin, redactionPINMarker)
	}
	raw := sentinel.GetCells()[columns["_raw"]]
	rawText := raw.GetStringValue()
	if rawText == "" {
		rawText = string(raw.GetBytesValue())
	}
	for _, sentinel := range []string{
		redactionAPIKeySentinel,
		redactionCookieSentinel,
		redactionPrivateKeySentinel,
		redactionCredentialSentinel,
		redactionPINSentinel,
	} {
		if strings.Contains(rawText, sentinel) {
			t.Fatalf("raw search-result cell leaked sentinel %q", sentinel)
		}
	}
	if !strings.Contains(rawText, `"api_key":"[REDACTED]"`) {
		t.Fatalf("raw cell was not mandatorily redacted: %q", rawText)
	}
	if strings.Count(rawText, "[REDACTED]") < 3 {
		t.Fatalf("raw cell did not redact the structured key plus embedded cookie/private-key values: %q", rawText)
	}
	if !strings.Contains(rawText, `"customer_credential":"`+redactionCredentialMarker+`"`) ||
		!strings.Contains(rawText, `"customer_pin":"`+redactionPINMarker+`"`) {
		t.Fatalf("raw cell did not retain both configured replacement markers: %q", rawText)
	}
}

func assertProcessLogsDoNotLeak(t *testing.T, logs string, secrets ...string) {
	t.Helper()
	for _, secret := range secrets {
		if secret != "" && strings.Contains(logs, secret) {
			t.Fatal("process logs leaked a protected test value")
		}
	}
}
