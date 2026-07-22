//go:build !windows

package integration_test

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"mime"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	clickhousedriver "github.com/ClickHouse/clickhouse-go/v2"
	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/loggen"
	"github.com/Suhaibinator/open-splunk/internal/testsupport"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
)

const (
	backendIntegrationFlag         = "OPEN_SPLUNK_BACKEND_INTEGRATION"
	verticalIndexName              = "vertical"
	verticalTenantID               = "vertical-tenant"
	verticalEventCount             = uint64(4)
	bulkIndexName                  = "vertical-bulk"
	bulkEventCount                 = uint64(10_001)
	redactionAPIKeySentinel        = "vertical-api-key-must-not-survive"
	redactionCookieSentinel        = "vertical-cookie-must-not-survive"
	redactionPrivateKeySentinel    = "vertical-private-key-must-not-survive"
	verticalSearchSPL              = " \nindex=vertical | table message status duration_ms api_key _raw\t"
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
//	SPL job execution -> HTTP protobuf typed results -> bounded export
//	re-execution -> one-time raw artifact download.
//
// It is opt-in because it builds two binaries and starts a pinned Docker image.
func TestBackendVertical(t *testing.T) {
	if os.Getenv(backendIntegrationFlag) != "1" {
		t.Skip("set " + backendIntegrationFlag + "=1 to run the backend vertical integration test")
	}
	if _, err := exec.LookPath("docker"); err != nil {
		t.Skipf("docker CLI is unavailable: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 7*time.Minute)
	defer cancel()
	repository := repositoryRoot(t)
	work := t.TempDir()

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

	serverBinary := filepath.Join(work, "open-splunk-server")
	collectorBinary := filepath.Join(work, "open-splunk-collector")
	buildBinary(t, ctx, repository, serverBinary, "./cmd/open-splunk-server")
	buildBinary(t, ctx, repository, collectorBinary, "./cmd/open-splunk-collector")

	httpAddress := unusedLoopbackAddress(t)
	collectorAddress := unusedLoopbackAddress(t)
	controlDBPath := filepath.Join(work, "control.sqlite")
	serverProcess := startProcess(t, repository, []string{
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
	}, append(os.Environ(), "OPEN_SPLUNK_CLICKHOUSE_PASSWORD="+clickhouse.Password))
	baseURL := "http://" + httpAddress
	httpClient := &http.Client{Timeout: 10 * time.Second}
	waitForHealth(t, ctx, httpClient, baseURL, serverProcess)

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
	writeFixture(t, logPath, fixtureStart)
	tokenPath := filepath.Join(work, "collector.token")
	writePrivateFile(t, tokenPath, []byte(plaintextToken+"\n"))
	collectorConfig := filepath.Join(work, "collector.yaml")
	writePrivateFile(t, collectorConfig, []byte(collectorYAML(collectorAddress, tokenPath, filepath.Join(work, "collector-state"), logPath)))

	collectorProcess := startProcess(t, repository, []string{
		collectorBinary, "run", "-config", collectorConfig,
	}, os.Environ())

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
	waitForStoredEvents(t, ctx, storage, collectorProcess, plaintextToken)
	visibilityCutoff := assertStoredEventBounds(t, ctx, storage, fixtureStart)

	if err := collectorProcess.Interrupt(15 * time.Second); err != nil {
		t.Fatalf("stop collector: %v\nlogs:\n%s", err, redactForFailure(
			collectorProcess.Logs(), plaintextToken,
			redactionAPIKeySentinel, redactionCookieSentinel, redactionPrivateKeySentinel,
		))
	}
	assertProcessLogsDoNotLeak(t, collectorProcess.Logs(), plaintextToken,
		redactionAPIKeySentinel, redactionCookieSentinel, redactionPrivateKeySentinel)

	search := runSearch(t, ctx, httpClient, baseURL, fixtureStart)
	assertTypedRedactedResults(t, search.results)
	wire, err := proto.MarshalOptions{Deterministic: true}.Marshal(search.results)
	if err != nil {
		t.Fatal(err)
	}
	for _, sentinel := range []string{redactionAPIKeySentinel, redactionCookieSentinel, redactionPrivateKeySentinel} {
		if bytes.Contains(wire, []byte(sentinel)) {
			t.Fatalf("HTTP protobuf search response leaked sentinel %q", sentinel)
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
	assertProcessLogsDoNotLeak(t, serverProcess.Logs(), serverSecrets...)
}

type completedSearch struct {
	jobID   string
	results *opensplunkv1.GetSearchResultsResponse
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
		if row["message"] != "typed redaction sentinel" {
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

func writeFixture(t *testing.T, path string, start time.Time) {
	t.Helper()
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	config := loggen.DefaultConfig()
	config.Format = loggen.FormatZapJSON
	config.Seed = 20260722
	config.Start = start
	config.Interval = time.Second
	config.Service = "vertical-service"
	config.Environment = "integration"
	config.Host = "vertical-host"
	if err := loggen.Generate(context.Background(), file, config, verticalEventCount-1); err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	sentinelLine := fmt.Sprintf(
		`{"timestamp":%q,"level":"INFO","message":"typed redaction sentinel","status":201,"duration_ms":12.5,"api_key":%q,"note_one":%q,"note_two":%q}`+"\n",
		start.Add(3*time.Second).Format(time.RFC3339Nano), redactionAPIKeySentinel,
		"Cookie: sid="+redactionCookieSentinel+"; csrf="+redactionCookieSentinel,
		"private_key=-----BEGIN PRIVATE KEY----- "+redactionPrivateKeySentinel,
	)
	if _, err := io.WriteString(file, sentinelLine); err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
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

func waitForStoredEvents(t *testing.T, ctx context.Context, connection clickhousedriver.Conn, process *managedProcess, plaintextToken string) {
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
		if err == nil && lastCount == verticalEventCount {
			return
		}
		if err == nil && lastCount > verticalEventCount {
			t.Fatalf("stored event count = %d, want %d", lastCount, verticalEventCount)
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
	earliest := fixtureStart.Add(-time.Minute).Format(time.RFC3339Nano)
	latest := time.Now().UTC().Add(time.Minute).Format(time.RFC3339Nano)
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
	deadline := time.NewTimer(20 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		var got opensplunkv1.GetSearchJobResponse
		postProto(t, ctx, client, baseURL+"/api/v1/search/jobs/get", &opensplunkv1.GetSearchJobRequest{SearchJobId: jobID}, &got)
		state := got.GetSearchJob().GetState()
		switch state {
		case opensplunkv1.SearchJobState_SEARCH_JOB_STATE_COMPLETED:
			t.Logf("completed search scope: indexes=%v range=%v cutoff=%v rows=%d",
				got.GetSearchJob().GetEffectiveIndexScope(), got.GetSearchJob().GetResolvedTimeRange(),
				got.GetSearchJob().GetIndexTimeCutoff(), got.GetSearchJob().GetProgress().GetProducedRows())
			pageSize := uint32(100)
			var results opensplunkv1.GetSearchResultsResponse
			postProto(t, ctx, client, baseURL+"/api/v1/search/jobs/results", &opensplunkv1.GetSearchResultsRequest{
				SearchJobId: jobID,
				Page:        &opensplunkv1.PageRequest{PageSize: &pageSize, IncludeTotalSize: true},
			}, &results)
			waitForTerminalHistory(t, ctx, client, baseURL, jobID)
			return completedSearch{jobID: jobID, results: &results}
		case opensplunkv1.SearchJobState_SEARCH_JOB_STATE_FAILED,
			opensplunkv1.SearchJobState_SEARCH_JOB_STATE_CANCELED,
			opensplunkv1.SearchJobState_SEARCH_JOB_STATE_EXPIRED:
			t.Fatalf("search job terminated in %s: %+v", state, got.GetSearchJob().GetFailure())
		}
		select {
		case <-ctx.Done():
			t.Fatalf("wait for search: %v", ctx.Err())
		case <-deadline.C:
			t.Fatal("wait for search: timed out")
		case <-ticker.C:
		}
	}
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

func assertTypedRedactedResults(t *testing.T, response *opensplunkv1.GetSearchResultsResponse) {
	t.Helper()
	page := response.GetResultPage()
	if page == nil || !page.GetSnapshotComplete() || page.GetPage().GetTotalSize() != verticalEventCount ||
		uint64(len(page.GetRows())) != verticalEventCount {
		t.Fatalf("result page = %+v", page)
	}
	columns := make(map[string]int, len(page.GetSchema().GetColumns()))
	for index, column := range page.GetSchema().GetColumns() {
		columns[column.GetFieldName()] = index
	}
	for _, name := range []string{"message", "status", "duration_ms", "api_key", "_raw"} {
		if _, ok := columns[name]; !ok {
			t.Fatalf("result schema is missing %q: %+v", name, page.GetSchema())
		}
	}
	if page.GetSchema().GetColumns()[columns["status"]].GetValueType() != opensplunkv1.ValueType_VALUE_TYPE_MIXED ||
		page.GetSchema().GetColumns()[columns["duration_ms"]].GetValueType() != opensplunkv1.ValueType_VALUE_TYPE_MIXED {
		t.Fatalf("dynamic numeric schema did not retain mixed typing: %+v", page.GetSchema())
	}
	var sentinel *opensplunkv1.ResultRow
	for _, row := range page.GetRows() {
		if row.GetCells()[columns["message"]].GetStringValue() == "typed redaction sentinel" {
			sentinel = row
			break
		}
	}
	if sentinel == nil {
		t.Fatal("typed redaction sentinel row was not returned")
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
