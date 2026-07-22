//go:build !windows

package integration_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
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
	backendIntegrationFlag = "OPEN_SPLUNK_BACKEND_INTEGRATION"
	verticalIndexName      = "vertical"
	verticalTenantID       = "vertical-tenant"
	verticalEventCount     = uint64(4)
	redactionSentinel      = "vertical-secret-must-not-survive"
)

// TestBackendVertical exercises the deployed backend boundary rather than a
// collection of in-process components:
//
//	HTTP protobuf provisioning -> collector file/WAL/gRPC -> ClickHouse ->
//	SPL job execution -> HTTP protobuf typed results.
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
	assertStoredEventBounds(t, ctx, storage, fixtureStart)

	if err := collectorProcess.Interrupt(15 * time.Second); err != nil {
		t.Fatalf("stop collector: %v\nlogs:\n%s", err, redactForFailure(collectorProcess.Logs(), plaintextToken, redactionSentinel))
	}
	assertProcessLogsDoNotLeak(t, collectorProcess.Logs(), plaintextToken, redactionSentinel)

	response := runSearch(t, ctx, httpClient, baseURL, fixtureStart)
	assertTypedRedactedResults(t, response)
	wire, err := proto.MarshalOptions{Deterministic: true}.Marshal(response)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(wire, []byte(redactionSentinel)) {
		t.Fatal("HTTP protobuf search response leaked the redaction sentinel")
	}

	if err := serverProcess.Interrupt(20 * time.Second); err != nil {
		t.Fatalf("stop server: %v\nlogs:\n%s", err, redactForFailure(serverProcess.Logs(), plaintextToken, redactionSentinel))
	}
	assertProcessLogsDoNotLeak(t, serverProcess.Logs(), plaintextToken, redactionSentinel)
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
		`{"timestamp":%q,"level":"INFO","message":"typed redaction sentinel","status":201,"duration_ms":12.5,"api_key":%q}`+"\n",
		start.Add(3*time.Second).Format(time.RFC3339Nano), redactionSentinel,
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
				redactForFailure(process.Logs(), plaintextToken, redactionSentinel))
		}
		select {
		case <-ctx.Done():
			t.Fatalf("wait for stored events: %v, count=%d\ncollector logs:\n%s", ctx.Err(), lastCount,
				redactForFailure(process.Logs(), plaintextToken, redactionSentinel))
		case <-deadline.C:
			t.Fatalf("wait for stored events: timed out, count=%d\ncollector logs:\n%s", lastCount,
				redactForFailure(process.Logs(), plaintextToken, redactionSentinel))
		case <-ticker.C:
		}
	}
}

func assertStoredEventBounds(t *testing.T, ctx context.Context, connection clickhousedriver.Conn, fixtureStart time.Time) {
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
	if earliestIndexTime.IsZero() || !earliestIndexTime.Equal(latestIndexTime) || minimumVisibility != 1 || maximumVisibility != 1 {
		t.Fatalf("stored commit bounds: index_time=[%s,%s] visibility=[%d,%d]",
			earliestIndexTime.Format(time.RFC3339Nano), latestIndexTime.Format(time.RFC3339Nano),
			minimumVisibility, maximumVisibility)
	}
	t.Logf("stored event bounds: event_time=[%s,%s] index_time=[%s,%s] visibility=[%d,%d]",
		earliest.Format(time.RFC3339Nano), latest.Format(time.RFC3339Nano),
		earliestIndexTime.Format(time.RFC3339Nano), latestIndexTime.Format(time.RFC3339Nano),
		minimumVisibility, maximumVisibility)
}

func runSearch(t *testing.T, ctx context.Context, client *http.Client, baseURL string, fixtureStart time.Time) *opensplunkv1.GetSearchResultsResponse {
	t.Helper()
	earliest := fixtureStart.Add(-time.Minute).Format(time.RFC3339Nano)
	latest := time.Now().UTC().Add(time.Minute).Format(time.RFC3339Nano)
	timezone := "UTC"
	var created opensplunkv1.CreateSearchJobResponse
	postProto(t, ctx, client, baseURL+"/api/v1/search/jobs/create", &opensplunkv1.CreateSearchJobRequest{
		Definition: &opensplunkv1.SearchDefinition{
			Spl: "index=vertical | table message status duration_ms api_key _raw",
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
			return &results
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
	if strings.Contains(rawText, redactionSentinel) {
		t.Fatal("raw search-result cell leaked the redaction sentinel")
	}
	if !strings.Contains(rawText, `"api_key":"[REDACTED]"`) {
		t.Fatalf("raw cell was not mandatorily redacted: %q", rawText)
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
