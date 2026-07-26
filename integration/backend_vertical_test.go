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
	verticalSentinelMessage        = "typed redaction sentinel"
	verticalSearchSPL              = " \nindex=vertical | dedup event_id | table _time message status duration_ms api_key _raw\t"
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
	buildBackendFrontend(t, ctx, repository)

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
	buildBinary(t, ctx, repository, serverBinary, "./cmd/open-splunk-server")
	buildBinary(t, ctx, repository, collectorBinary, "./cmd/open-splunk-collector")

	httpAddress := unusedLoopbackAddress(t)
	collectorAddress := unusedLoopbackAddress(t)
	controlDBPath := filepath.Join(work, "control.sqlite")
	assertEmptyDirectory(t, serverRuntimeDir)
	serverEnvironment := environmentWithValue(os.Environ(), "OPEN_SPLUNK_CLICKHOUSE_PASSWORD", clickhouse.Password)
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
		"-clickhouse-address=" + clickhouse.Address,
		"-clickhouse-database=" + clickhouse.Database,
		"-clickhouse-username=" + clickhouse.Username,
		"-collector-grpc-address=" + collectorAddress,
		"-collector-grpc-insecure",
		"-tenant-id=" + verticalTenantID,
	}
	serverProcess := startProcess(t, serverRuntimeDir, serverArguments, serverEnvironment)
	serverProcesses := []*managedProcess{serverProcess}
	baseURL := "http://" + httpAddress
	httpClient := &http.Client{Timeout: 10 * time.Second}
	waitForHealth(t, ctx, httpClient, baseURL, serverProcess)
	assertStandaloneServerSurface(t, ctx, httpClient, baseURL)

	var createdIndex opensplunkv1.CreateIndexResponse
	postProto(t, ctx, httpClient, baseURL+"/api/v1/indexes/create", &opensplunkv1.CreateIndexRequest{
		Definition: &opensplunkv1.IndexDefinition{
			Name:            verticalIndexName,
			DisplayName:     "Backend vertical integration",
			RetentionPeriod: durationpb.New(24 * time.Hour),
			IngestionAccess: opensplunkv1.IndexAccessState_INDEX_ACCESS_STATE_ENABLED,
			SearchAccess:    opensplunkv1.IndexAccessState_INDEX_ACCESS_STATE_ENABLED,
		},
	}, &createdIndex)
	if createdIndex.GetIndex().GetVersion() != 1 || createdIndex.GetIndex().GetDefinition().GetName() != verticalIndexName {
		t.Fatalf("created index = %+v", createdIndex.GetIndex())
	}

	var createdToken opensplunkv1.CreateIngestionTokenResponse
	postProto(t, ctx, httpClient, baseURL+"/api/v1/ingestion-tokens/create", &opensplunkv1.CreateIngestionTokenRequest{
		Definition: &opensplunkv1.IngestionTokenDefinition{
			Name: "backend-vertical-collector",
			Constraints: &opensplunkv1.IngestionTokenConstraints{
				AllowedIndexNames: []string{verticalIndexName},
			},
		},
	}, &createdToken)
	plaintextToken := createdToken.GetPlaintextToken()
	if plaintextToken == "" || createdToken.GetIngestionToken().GetVersion() != 1 ||
		!strings.HasPrefix(plaintextToken, createdToken.GetIngestionToken().GetTokenPrefix()) {
		t.Fatalf("created ingestion token metadata = %+v, plaintext length = %d", createdToken.GetIngestionToken(), len(plaintextToken))
	}
	serverSecrets := []string{
		plaintextToken,
		redactionAPIKeySentinel,
		redactionCookieSentinel,
		redactionPrivateKeySentinel,
	}

	fixtureStart := time.Now().UTC().Add(-2 * time.Minute).Truncate(time.Second)
	logPath := filepath.Join(work, "app.log")
	createEmptyFixture(t, logPath)
	tokenPath := filepath.Join(work, "collector.token")
	writePrivateFile(t, tokenPath, []byte(plaintextToken+"\n"))
	collectorStateDir := filepath.Join(work, "collector-state")
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
			Username: clickhouse.Username,
			Password: clickhouse.Password,
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
	waitForDistinctStoredEventCount(t, ctx, storage, collectorProcess, plaintextToken, verticalEventCount)
	waitForCollectorCheckpoint(t, ctx, collectorStateDir, logPath, collectorProcess, plaintextToken)

	if err := collectorProcess.Interrupt(15 * time.Second); err != nil {
		t.Fatalf("stop collector: %v\nlogs:\n%s", err, redactForFailure(
			collectorProcess.Logs(), plaintextToken,
			redactionAPIKeySentinel, redactionCookieSentinel, redactionPrivateKeySentinel,
		))
	}
	assertDurableCollectorState(t, collectorStateDir, uint64(mustFileSize(t, logPath)), verticalEventCount)
	visibilityCutoff := assertStoredEventBounds(t, ctx, storage, fixtureStart)
	assertRestartDeliveryAccounting(t, ctx, storage)
	for _, process := range collectorProcesses {
		assertProcessLogsDoNotLeak(t, process.Logs(), plaintextToken,
			redactionAPIKeySentinel, redactionCookieSentinel, redactionPrivateKeySentinel)
	}
	insertTimelineExclusiveBoundaryEvent(t, ctx, storage, fixtureStart.Add(5500*time.Millisecond), visibilityCutoff)

	search := runSearch(t, ctx, httpClient, baseURL, fixtureStart)
	assertCompletedTimeline(t, ctx, httpClient, baseURL, search.jobID, fixtureStart)
	assertTypedRedactedResults(t, search.results)
	assertBrowserVisibleResults(t, ctx, repository, baseURL, fixtureStart)
	for pageIndex, wire := range search.results.responseWire {
		for _, sentinel := range []string{redactionAPIKeySentinel, redactionCookieSentinel, redactionPrivateKeySentinel} {
			if bytes.Contains(wire, []byte(sentinel)) {
				t.Fatalf("HTTP protobuf search response page %d leaked sentinel %q", pageIndex+1, sentinel)
			}
		}
	}
	completedExport, artifact, downloadToken := exportAndDownloadJSONLines(t, ctx, httpClient, baseURL, search.jobID,
		[]string{"message", "status", "duration_ms", "api_key", "_raw"}, verticalEventCount)
	serverSecrets = append(serverSecrets, downloadToken)
	assertDownloadedRedactedResults(t, completedExport, artifact)

	createVerticalIndex(t, ctx, httpClient, baseURL, bulkIndexName, "Backend vertical bulk export")
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
	plaintextToken string,
	wantCount uint64,
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
			verticalTenantID, verticalIndexName,
		).Scan(&lastCount, &lastDistinct)
		if err == nil && lastDistinct == wantCount &&
			lastCount >= wantCount && lastCount <= wantCount+1 {
			return
		}
		if err == nil && (lastDistinct > wantCount || lastCount > wantCount+1) {
			t.Fatalf("stored restart events = %d distinct=%d, want %d distinct and at most one replay", lastCount, lastDistinct, wantCount)
		}
		if process.Exited() {
			t.Fatalf("collector exited before restart ingestion completed: %v, count=%d distinct=%d\nlogs:\n%s",
				process.Err(), lastCount, lastDistinct,
				redactForFailure(process.Logs(), plaintextToken,
					redactionAPIKeySentinel, redactionCookieSentinel, redactionPrivateKeySentinel))
		}
		select {
		case <-ctx.Done():
			t.Fatalf("wait for distinct stored events: %v, count=%d distinct=%d", ctx.Err(), lastCount, lastDistinct)
		case <-deadline.C:
			t.Fatalf("wait for distinct stored events: timed out, count=%d distinct=%d\ncollector logs:\n%s",
				lastCount, lastDistinct,
				redactForFailure(process.Logs(), plaintextToken,
					redactionAPIKeySentinel, redactionCookieSentinel, redactionPrivateKeySentinel))
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
	if bootstrap.GetServerVersion() != "dev" || bootstrap.GetApiVersion() != "v1" ||
		bootstrap.GetSplCompatibilityVersion() != splCompatibilityVersionForTest ||
		bootstrap.GetSearchWebsocketPath() != "/api/v1/search/ws" ||
		limits.GetMaximumPreviewRows() == 0 || limits.GetMaximumWebsocketSubscriptions() == 0 ||
		limits.GetMaximumWebsocketFrameBytes() < 1<<10 || limits.GetMaximumWebsocketFrameBytes() > 1<<20 ||
		timelineFeatures != 1 || previewFeatures != 1 ||
		limits.GetMaximumTimelineBuckets() != verticalTimelineMaximumBuckets {
		t.Fatalf("standalone bootstrap response = %+v", &bootstrap)
	}
}

func waitForHealth(t *testing.T, ctx context.Context, client *http.Client, baseURL string, process *managedProcess) {
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
			last = fmt.Errorf("status %d body %q read error %v", response.StatusCode, body, readErr)
		} else {
			last = err
		}
		if process.Exited() {
			t.Fatalf("server exited before health check: %v\nlogs:\n%s", process.Err(), process.Logs())
		}
		select {
		case <-ctx.Done():
			t.Fatalf("wait for server health: %v (last: %v)\nlogs:\n%s", ctx.Err(), last, process.Logs())
		case <-deadline.C:
			t.Fatalf("wait for server health: timed out (last: %v)\nlogs:\n%s", last, process.Logs())
		case <-ticker.C:
		}
	}
}

func postProto(t *testing.T, ctx context.Context, client *http.Client, url string, input, output proto.Message) []byte {
	t.Helper()
	payload, err := proto.Marshal(input)
	if err != nil {
		t.Fatal(err)
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(payload))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", "application/x-protobuf")
	response, err := client.Do(request)
	if err != nil {
		t.Fatalf("POST %s: %v", url, err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(io.LimitReader(response.Body, 16<<20))
	if err != nil {
		t.Fatalf("read POST %s: %v", url, err)
	}
	if response.StatusCode != http.StatusOK {
		t.Fatalf("POST %s status = %d, body = %q", url, response.StatusCode, body)
	}
	if contentType := response.Header.Get("Content-Type"); contentType != "application/x-protobuf" {
		t.Fatalf("POST %s content type = %q", url, contentType)
	}
	if err := proto.Unmarshal(body, output); err != nil {
		t.Fatalf("decode POST %s: %v", url, err)
	}
	return body
}

func createVerticalIndex(t *testing.T, ctx context.Context, client *http.Client, baseURL, name, displayName string) {
	t.Helper()
	var created opensplunkv1.CreateIndexResponse
	postProto(t, ctx, client, baseURL+"/api/v1/indexes/create", &opensplunkv1.CreateIndexRequest{
		Definition: &opensplunkv1.IndexDefinition{
			Name:            name,
			DisplayName:     displayName,
			RetentionPeriod: durationpb.New(24 * time.Hour),
			IngestionAccess: opensplunkv1.IndexAccessState_INDEX_ACCESS_STATE_ENABLED,
			SearchAccess:    opensplunkv1.IndexAccessState_INDEX_ACCESS_STATE_ENABLED,
		},
	}, &created)
	if created.GetIndex().GetVersion() != 1 || created.GetIndex().GetDefinition().GetName() != name {
		t.Fatalf("created index %q = %+v", name, created.GetIndex())
	}
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
	for _, sentinel := range []string{redactionAPIKeySentinel, redactionCookieSentinel, redactionPrivateKeySentinel} {
		if bytes.Contains(artifact, []byte(sentinel)) {
			t.Fatalf("downloaded export leaked sentinel %q", sentinel)
		}
	}
	var (
		rowCount uint64
		found    bool
	)
	expectedColumns := []string{"message", "status", "duration_ms", "api_key", "_raw"}
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
			row["api_key"] != "[REDACTED]" || !rawOK || strings.Count(raw, "[REDACTED]") < 3 {
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
		`{"timestamp":%q,"level":"INFO","message":%q,"status":201,"duration_ms":12.5,"api_key":%q,"note_one":%q,"note_two":%q}`+"\n",
		start.Add(3*time.Second).Format(time.RFC3339Nano), verticalSentinelMessage, redactionAPIKeySentinel,
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
`, address, tokenPath, statePath, logPath, verticalIndexName)
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
					redactionAPIKeySentinel, redactionCookieSentinel, redactionPrivateKeySentinel))
		}
		select {
		case <-ctx.Done():
			t.Fatalf("wait for stored events: %v, count=%d\ncollector logs:\n%s", ctx.Err(), lastCount,
				redactForFailure(process.Logs(), plaintextToken,
					redactionAPIKeySentinel, redactionCookieSentinel, redactionPrivateKeySentinel))
		case <-deadline.C:
			t.Fatalf("wait for stored events: timed out, count=%d\ncollector logs:\n%s", lastCount,
				redactForFailure(process.Logs(), plaintextToken,
					redactionAPIKeySentinel, redactionCookieSentinel, redactionPrivateKeySentinel))
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
					redactionAPIKeySentinel, redactionCookieSentinel, redactionPrivateKeySentinel))
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
		data, readErr := os.ReadFile(filepath.Join(stateDir, "wal", "meta.json"))
		lastErr = readErr
		if readErr == nil {
			lastErr = json.Unmarshal(data, &last)
			if lastErr == nil &&
				last.LastAckedBatchSequence > 0 &&
				last.LastAckedBatchSequence < ^uint64(0) &&
				last.NextBatchSequence == last.LastAckedBatchSequence+1 {
				return
			}
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
			if syncErr := syncCollectorWALGrowth(stateDir, before, last); syncErr == nil {
				return
			} else {
				lastErr = syncErr
			}
		}
		if process.Exited() {
			t.Fatalf("collector exited before offline WAL append: %v snapshot=%v error=%v\nlogs:\n%s",
				process.Err(), last, lastErr, redactForFailure(process.Logs(), plaintextToken,
					redactionAPIKeySentinel, redactionCookieSentinel, redactionPrivateKeySentinel))
		}
		select {
		case <-ctx.Done():
			t.Fatalf("wait for offline collector WAL append: %v snapshot=%v error=%v", ctx.Err(), last, lastErr)
		case <-deadline.C:
			t.Fatalf("wait for offline collector WAL append: timed out snapshot=%v error=%v\nlogs:\n%s",
				last, lastErr, redactForFailure(process.Logs(), plaintextToken,
					redactionAPIKeySentinel, redactionCookieSentinel, redactionPrivateKeySentinel))
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
					redactionAPIKeySentinel, redactionCookieSentinel, redactionPrivateKeySentinel))
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
		stats.LastAckedBatchSequence == 0 {
		t.Fatalf("durable collector WAL state = %+v, want drained queue with a terminal acknowledgment", stats)
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
	pageSize := uint32(2)
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
			!page.GetPage().GetTotalSizeExact() || page.GetPage().GetTotalSize() != verticalEventCount ||
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
		if pageCount > int(verticalEventCount) {
			t.Fatalf("search result cursor exceeded %d bounded pages", verticalEventCount)
		}
	}
	if pageCount != 2 || uint64(len(rows)) != verticalEventCount {
		t.Fatalf("collected search results: pages=%d rows=%d", pageCount, len(rows))
	}
	return &collectedVerticalSearchResults{schema: schema, rows: rows, responseWire: pageWire}
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
		lastSequence     uint64
		lastStateVersion uint64
		lastProducedRows uint64
		lastResultBytes  uint64
		lastPreview      uint64
		schemaID         string
		schemaColumns    int
		sawState         bool
		sawProgress      bool
		sawSchema        bool
		sawPreview       bool
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
				for _, sentinel := range []string{redactionAPIKeySentinel, redactionCookieSentinel, redactionPrivateKeySentinel} {
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
				progress.GetProducedRows() < lastProducedRows || progress.GetResultBytes() < lastResultBytes {
				t.Fatalf("search websocket progress event = %+v after rows=%d bytes=%d",
					progress, lastProducedRows, lastResultBytes)
			}
			lastProducedRows = progress.GetProducedRows()
			lastResultBytes = progress.GetResultBytes()
			sawProgress = true
		case event.GetSearchTerminal() != nil:
			terminal := event.GetSearchTerminal()
			if !sawState || !sawProgress || !sawSchema || !sawPreview || terminal.GetSearchJobId() != jobID ||
				terminal.GetState() != opensplunkv1.SearchJobState_SEARCH_JOB_STATE_COMPLETED ||
				terminal.GetStateVersion() == 0 || terminal.GetStateVersion() != lastStateVersion ||
				terminal.GetFinalProgress().GetPhase() != opensplunkv1.SearchExecutionPhase_SEARCH_EXECUTION_PHASE_COMPLETE ||
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
	for _, name := range []string{"message", "status", "duration_ms", "api_key", "_raw"} {
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
	raw := sentinel.GetCells()[columns["_raw"]]
	rawText := raw.GetStringValue()
	if rawText == "" {
		rawText = string(raw.GetBytesValue())
	}
	for _, sentinel := range []string{redactionAPIKeySentinel, redactionCookieSentinel, redactionPrivateKeySentinel} {
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
}

func assertProcessLogsDoNotLeak(t *testing.T, logs string, secrets ...string) {
	t.Helper()
	for _, secret := range secrets {
		if secret != "" && strings.Contains(logs, secret) {
			t.Fatal("process logs leaked a protected test value")
		}
	}
}
