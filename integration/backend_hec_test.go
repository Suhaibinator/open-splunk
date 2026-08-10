//go:build !windows

package integration_test

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"maps"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	clickhousedriver "github.com/ClickHouse/clickhouse-go/v2"
	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/eventfields"
	"github.com/Suhaibinator/open-splunk/internal/ingest"
	"github.com/Suhaibinator/open-splunk/internal/testsupport"
	"google.golang.org/protobuf/types/known/durationpb"
)

const (
	backendHECIndexName         = "hec-vertical"
	backendHECTenantID          = "hec-vertical-tenant"
	backendHECJSONChannel       = "123e4567-e89b-42d3-a456-426614174001"
	backendHECRawChannel        = "123e4567-e89b-42d3-a456-426614174002"
	backendHECDefaultHost       = "hec-default-host-private"
	backendHECDefaultSource     = "hec-default-source-private"
	backendHECDefaultSourcetype = "hec-default-sourcetype-private"
	backendHECJSONPayload       = "hec-json-payload-private"
	backendHECObjectPayload     = "hec-object-payload-private"
	backendHECRawPayloadOne     = "hec-raw-payload-one-private"
	backendHECRawPayloadTwo     = "hec-raw-payload-two-private"
	backendHECMaximumAckID      = int64(1<<53 - 1)
)

// TestBackendHECVertical proves the selected HEC v0.1 boundary through the
// shipped process rather than through in-process adapters:
//
//	Admin protobuf token provisioning -> TLS HEC JSON/raw -> durable ACK ->
//	ClickHouse projection/provenance -> public SPL search -> crash restart ->
//	retained ACK/retry.
//
// It shares the backend vertical's opt-in flag and pinned disposable
// ClickHouse harness. Pausing the owned ClickHouse container makes PENDING
// externally observable without adding a test seam to production code.
func TestBackendHECVertical(t *testing.T) {
	if os.Getenv(backendIntegrationFlag) != "1" {
		t.Skip("set " + backendIntegrationFlag + "=1 to run the backend HEC vertical integration test")
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

	image, err := testsupport.ResolvePinnedClickHouseImage(
		os.Getenv("OPEN_SPLUNK_CLICKHOUSE_TEST_IMAGE"),
	)
	if err != nil {
		t.Fatal(err)
	}
	clickHouse, err := testsupport.StartClickHouseWithServicePrincipals(ctx, image)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cleanupCancel()
		if err := clickHouse.Close(cleanupCtx); err != nil {
			t.Errorf("ClickHouse cleanup: %v", err)
		}
	})

	serverBinary := filepath.Join(buildDir, "open-splunk-server")
	buildBinary(t, ctx, stagedBackendRepository, serverBinary, "./cmd/open-splunk-server")

	httpAddress := unusedLoopbackAddress(t)
	collectorAddress := unusedLoopbackAddress(t)
	httpTLSIdentity, err := testsupport.WriteServerTLSIdentity(
		filepath.Join(work, "http-tls"),
		"127.0.0.1",
	)
	if err != nil {
		t.Fatal(err)
	}
	controlDBPath := filepath.Join(work, "control.sqlite")
	administratorTokenPath, administratorToken := provisionAdministratorToken(t, work)
	assertEmptyDirectory(t, serverRuntimeDir)
	serverEnvironment := clickHouseServerEnvironment(os.Environ(), clickHouse)
	serverEnvironment = environmentWithValue(
		serverEnvironment,
		"PATH",
		filepath.Join(serverRuntimeDir, "no-external-runtime"),
	)
	serverArguments := []string{
		serverBinary,
		"-http-address=" + httpAddress,
		"-http-tls-cert=" + httpTLSIdentity.CertificateFile,
		"-http-tls-key=" + httpTLSIdentity.PrivateKeyFile,
		"-control-db=" + controlDBPath,
		"-master-key=" + filepath.Join(work, "server.key"),
		"-administrator-token-file=" + administratorTokenPath,
		"-collector-grpc-address=" + collectorAddress,
		"-collector-grpc-insecure",
		"-tenant-id=" + backendHECTenantID,
		"-hec-enabled=true",
	}
	serverArguments = append(serverArguments, clickHouseServerArguments(clickHouse)...)

	serverProcess := startProcess(t, serverRuntimeDir, serverArguments, serverEnvironment)
	serverProcesses := []*managedProcess{serverProcess}
	baseURL := "https://" + httpAddress
	httpTransport := http.DefaultTransport.(*http.Transport).Clone()
	httpTransport.TLSClientConfig = &tls.Config{
		MinVersion: tls.VersionTLS12,
		RootCAs:    httpTLSIdentity.RootCAs,
	}
	httpClient := &http.Client{Transport: httpTransport, Timeout: 10 * time.Second}
	waitForHealth(
		t,
		ctx,
		httpClient,
		baseURL,
		serverProcess,
		administratorToken,
		clickHouse.MigrationPassword,
		clickHouse.RuntimePassword,
		clickHouse.DeletionPassword,
	)
	backendHECAssertHealth(t, ctx, httpClient, baseURL)
	backendHECAssertAdvertised(t, ctx, httpClient, baseURL)
	assertPlaintextCannotReachHTTPSHealth(t, ctx, httpAddress)

	var createdIndex opensplunkv1.CreateIndexResponse
	postAdministratorProto(
		t,
		ctx,
		httpClient,
		baseURL+"/api/v1/indexes/create",
		administratorToken,
		&opensplunkv1.CreateIndexRequest{Definition: &opensplunkv1.IndexDefinition{
			Name:            backendHECIndexName,
			DisplayName:     "Backend HEC vertical integration",
			RetentionPeriod: durationpb.New(24 * time.Hour),
			IngestionAccess: opensplunkv1.IndexAccessState_INDEX_ACCESS_STATE_ENABLED,
			SearchAccess:    opensplunkv1.IndexAccessState_INDEX_ACCESS_STATE_ENABLED,
		}},
		&createdIndex,
	)
	if createdIndex.GetIndex().GetVersion() != 1 ||
		createdIndex.GetIndex().GetDefinition().GetName() != backendHECIndexName {
		t.Fatalf("created HEC index = %+v", createdIndex.GetIndex())
	}

	plaintextToken, tokenMetadata := backendHECCreateToken(
		t,
		ctx,
		httpClient,
		baseURL,
		administratorToken,
	)
	protectedValues := []string{
		administratorToken,
		plaintextToken,
		tokenMetadata.GetTokenPrefix(),
		clickHouse.MigrationPassword,
		clickHouse.RuntimePassword,
		clickHouse.DeletionPassword,
		backendHECJSONChannel,
		backendHECRawChannel,
		backendHECDefaultHost,
		backendHECDefaultSource,
		backendHECDefaultSourcetype,
		backendHECJSONPayload,
		backendHECObjectPayload,
		backendHECRawPayloadOne,
		backendHECRawPayloadTwo,
	}

	storage, err := clickhousedriver.Open(&clickhousedriver.Options{
		Addr: []string{clickHouse.Address},
		Auth: clickhousedriver.Auth{
			Database: clickHouse.Database,
			Username: clickHouse.RuntimeUsername,
			Password: clickHouse.RuntimePassword,
		},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = storage.Close() })
	if err := storage.Ping(ctx); err != nil {
		t.Fatalf("ping ClickHouse HEC inspection connection: %v", err)
	}

	eventTime := time.Now().UTC().Add(-time.Minute).Truncate(time.Nanosecond)
	epoch := backendHECEpochSeconds(eventTime)
	jsonBody := []byte(
		`{"time":` + epoch +
			`,"host":"json-host","source":"json-source","sourcetype":"json-type",` +
			`"event":"` + backendHECJSONPayload + `","fields":{` +
			`"text":"exact","signed":-9223372036854775808,` +
			`"unsigned":18446744073709551615,"decimal":1.2300E+004,` +
			`"flag":true,"nothing":null}}` +
			`{"event":{"kind":"` + backendHECObjectPayload + `","n":1.00}}`,
	)

	clickHousePaused := false
	backendHECDocker(t, ctx, "pause", clickHouse.Name)
	clickHousePaused = true
	t.Cleanup(func() {
		if !clickHousePaused {
			return
		}
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cleanupCancel()
		if err := backendHECDockerError(cleanupCtx, "unpause", clickHouse.Name); err != nil {
			t.Errorf("unpause ClickHouse during cleanup: %v", err)
		}
	})

	jsonAck := backendHECIngest(
		t,
		ctx,
		httpClient,
		baseURL+"/services/collector/event",
		plaintextToken,
		backendHECJSONChannel,
		"application/json; charset=utf-8",
		jsonBody,
	)
	if jsonAck < 1 || jsonAck > backendHECMaximumAckID {
		t.Fatalf("first HEC acknowledgment ID = %d, want an exact positive JSON integer", jsonAck)
	}
	if got := backendHECQueryAcknowledgments(
		t,
		ctx,
		httpClient,
		baseURL,
		plaintextToken,
		backendHECJSONChannel,
		jsonAck,
	)[jsonAck]; got {
		t.Fatal("HEC acknowledgment became indexed while ClickHouse was paused")
	}

	backendHECDocker(t, ctx, "unpause", clickHouse.Name)
	clickHousePaused = false
	backendHECWaitForAcknowledgment(
		t,
		ctx,
		httpClient,
		baseURL,
		plaintextToken,
		backendHECJSONChannel,
		jsonAck,
		serverProcess,
	)

	rawValues := url.Values{
		"time":       []string{epoch},
		"host":       []string{"raw-host"},
		"source":     []string{"raw-source"},
		"sourcetype": []string{"raw-type"},
		"index":      []string{backendHECIndexName},
		"channel":    []string{backendHECRawChannel},
	}
	rawAck := backendHECIngest(
		t,
		ctx,
		httpClient,
		baseURL+"/services/collector/raw?"+rawValues.Encode(),
		plaintextToken,
		"",
		"text/plain; charset=utf-8",
		[]byte(backendHECRawPayloadOne+"\r\n\n"+backendHECRawPayloadTwo),
	)
	if rawAck < 1 || rawAck > backendHECMaximumAckID {
		t.Fatalf("first HEC acknowledgment ID on independent raw channel = %d, want an exact positive JSON integer", rawAck)
	}
	backendHECWaitForAcknowledgment(
		t,
		ctx,
		httpClient,
		baseURL,
		plaintextToken,
		backendHECRawChannel,
		rawAck,
		serverProcess,
	)
	backendHECAssertStoredProjection(t, ctx, storage, tokenMetadata.GetIngestionTokenId(), eventTime)
	backendHECAssertStoredCardinality(t, ctx, storage, 4, 2, 1)
	backendHECAssertProductSearch(
		t,
		ctx,
		httpClient,
		baseURL,
		eventTime,
		time.Now().UTC().Add(5*time.Second),
	)

	if err := serverProcess.Kill(10 * time.Second); err != nil {
		t.Fatalf("crash HEC server after indexed acknowledgments: %v", err)
	}
	assertProcessLogsDoNotLeak(t, serverProcess.Logs(), protectedValues...)

	serverProcess = startProcess(t, serverRuntimeDir, serverArguments, serverEnvironment)
	serverProcesses = append(serverProcesses, serverProcess)
	waitForHealth(t, ctx, httpClient, baseURL, serverProcess, protectedValues...)
	retained := backendHECQueryAcknowledgments(
		t,
		ctx,
		httpClient,
		baseURL,
		plaintextToken,
		backendHECJSONChannel,
		jsonAck,
	)
	if !retained[jsonAck] {
		t.Fatalf("HEC acknowledgment %d was not retained across restart", jsonAck)
	}
	retained = backendHECQueryAcknowledgments(
		t,
		ctx,
		httpClient,
		baseURL,
		plaintextToken,
		backendHECRawChannel,
		rawAck,
	)
	if !retained[rawAck] {
		t.Fatalf("raw HEC acknowledgment %d was not retained across restart", rawAck)
	}

	retryAck := backendHECIngest(
		t,
		ctx,
		httpClient,
		baseURL+"/services/collector/event/1.0",
		plaintextToken,
		backendHECJSONChannel,
		"application/json",
		jsonBody,
	)
	if retryAck < 1 || retryAck > backendHECMaximumAckID || retryAck == jsonAck {
		t.Fatalf("independent HEC retry acknowledgment ID = %d, want an exact opaque ID distinct from %d", retryAck, jsonAck)
	}
	backendHECWaitForAcknowledgment(
		t,
		ctx,
		httpClient,
		baseURL,
		plaintextToken,
		backendHECJSONChannel,
		retryAck,
		serverProcess,
	)
	backendHECAssertStoredCardinality(t, ctx, storage, 6, 3, 2)
	backendHECAssertAuditRedaction(
		t,
		ctx,
		httpClient,
		baseURL,
		administratorToken,
		plaintextToken,
		protectedValues,
	)

	if err := serverProcess.Interrupt(20 * time.Second); err != nil {
		t.Fatalf(
			"stop HEC server: %v\nlogs:\n%s",
			err,
			redactForFailure(serverProcess.Logs(), protectedValues...),
		)
	}
	for _, process := range serverProcesses {
		assertProcessLogsDoNotLeak(t, process.Logs(), protectedValues...)
	}
}

func backendHECAssertProductSearch(
	t *testing.T,
	ctx context.Context,
	client *http.Client,
	baseURL string,
	earliestEvent time.Time,
	latestEvent time.Time,
) {
	t.Helper()
	earliest := earliestEvent.Add(-time.Nanosecond)
	if !latestEvent.After(earliest) {
		t.Fatalf("HEC search range = [%s,%s), want a positive range", earliest, latestEvent)
	}

	t.Run("base index time source and typed fields", func(t *testing.T) {
		const spl = `index="hec-vertical" source="json-source" signed=-9223372036854775808 unsigned=18446744073709551615 flag=true | table _time index host source sourcetype text signed unsigned flag`
		_, results := backendHECRunProductSearch(
			t,
			ctx,
			client,
			baseURL,
			spl,
			earliest,
			latestEvent,
			1,
		)
		backendHECRequireSearchColumns(t, results, []string{
			"_time",
			"index",
			"host",
			"source",
			"sourcetype",
			"text",
			"signed",
			"unsigned",
			"flag",
		})
		cells := results.rows[0].GetCells()
		backendHECRequireSearchTime(t, cells[0], earliestEvent)
		backendHECRequireSearchString(t, cells[1], backendHECIndexName)
		backendHECRequireSearchString(t, cells[2], "json-host")
		backendHECRequireSearchString(t, cells[3], "json-source")
		backendHECRequireSearchString(t, cells[4], "json-type")
		backendHECRequireSearchString(t, cells[5], "exact")
		backendHECRequireSearchSigned(t, cells[6], -9223372036854775808)
		backendHECRequireSearchUnsigned(t, cells[7], ^uint64(0))
		backendHECRequireSearchBool(t, cells[8], true)
	})

	t.Run("stats by source", func(t *testing.T) {
		const spl = `index="hec-vertical" | stats count AS events BY source`
		_, results := backendHECRunProductSearch(
			t,
			ctx,
			client,
			baseURL,
			spl,
			earliest,
			latestEvent,
			3,
		)
		backendHECRequireSearchColumns(t, results, []string{"source", "events"})
		backendHECRequireSearchColumnTypes(t, results, []opensplunkv1.ValueType{
			opensplunkv1.ValueType_VALUE_TYPE_STRING,
			opensplunkv1.ValueType_VALUE_TYPE_UINT64,
		})
		counts := make(map[string]uint64, len(results.rows))
		for _, row := range results.rows {
			cells := row.GetCells()
			source := backendHECSearchString(t, cells[0])
			if _, duplicate := counts[source]; duplicate {
				t.Fatalf("HEC stats returned duplicate source %q", source)
			}
			counts[source] = backendHECSearchUnsigned(t, cells[1])
		}
		want := map[string]uint64{
			backendHECDefaultSource: 1,
			"json-source":           1,
			"raw-source":            2,
		}
		if !maps.Equal(counts, want) {
			t.Fatalf("HEC stats source counts = %#v, want %#v", counts, want)
		}
	})

	t.Run("timechart count", func(t *testing.T) {
		const spl = `index="hec-vertical" | timechart span=1m count`
		job, results := backendHECRunProductSearch(
			t,
			ctx,
			client,
			baseURL,
			spl,
			earliest,
			latestEvent,
			0,
		)
		backendHECRequireSearchColumns(t, results, []string{"_time", "count"})
		backendHECRequireSearchColumnTypes(t, results, []opensplunkv1.ValueType{
			opensplunkv1.ValueType_VALUE_TYPE_TIMESTAMP,
			opensplunkv1.ValueType_VALUE_TYPE_UINT64,
		})
		var total uint64
		var previous time.Time
		for _, row := range results.rows {
			cells := row.GetCells()
			bucket := backendHECSearchTime(t, cells[0])
			if bucket.Nanosecond() != 0 || bucket.Second() != 0 ||
				(!previous.IsZero() && !bucket.After(previous)) {
				t.Fatalf("HEC timechart bucket %s after %s is not an ordered minute boundary", bucket, previous)
			}
			previous = bucket
			total += backendHECSearchUnsigned(t, cells[1])
		}
		if total != 4 || job.GetProgress().GetProducedRows() != uint64(len(results.rows)) {
			t.Fatalf(
				"HEC timechart rows/total = %d/%d, want nonempty rows totaling 4",
				len(results.rows),
				total,
			)
		}
	})
}

func backendHECRunProductSearch(
	t *testing.T,
	ctx context.Context,
	client *http.Client,
	baseURL string,
	spl string,
	earliest time.Time,
	latest time.Time,
	expectedRows uint64,
) (*opensplunkv1.SearchJob, *collectedVerticalSearchResults) {
	t.Helper()
	earliestText := earliest.Format(time.RFC3339Nano)
	latestText := latest.Format(time.RFC3339Nano)
	timezone := "UTC"
	var created opensplunkv1.CreateSearchJobResponse
	postProto(
		t,
		ctx,
		client,
		baseURL+"/api/v1/search/jobs/create",
		&opensplunkv1.CreateSearchJobRequest{Definition: &opensplunkv1.SearchDefinition{
			Spl: spl,
			TimeRange: &opensplunkv1.TimeRangeSpec{
				Earliest: &earliestText,
				Latest:   &latestText,
				Timezone: &timezone,
			},
			IndexScope: []string{backendHECIndexName},
		}},
		&created,
	)
	jobID := created.GetSearchJob().GetSearchJobId()
	if jobID == "" {
		t.Fatalf("created HEC product search = %+v", created.GetSearchJob())
	}
	job := waitForCompletedSearch(t, ctx, client, baseURL, jobID, 60*time.Second)
	resolved := job.GetResolvedTimeRange()
	if job.GetDefinition().GetSpl() != spl ||
		job.GetResultsTruncated() ||
		!slices.Equal(job.GetEffectiveIndexScope(), []string{backendHECIndexName}) ||
		resolved == nil ||
		resolved.GetEarliest() == nil || resolved.GetEarliest().CheckValid() != nil ||
		resolved.GetLatest() == nil || resolved.GetLatest().CheckValid() != nil ||
		!resolved.GetEarliest().AsTime().Equal(earliest) ||
		!resolved.GetLatest().AsTime().Equal(latest) ||
		resolved.GetTimezone() != timezone {
		t.Fatalf("completed HEC product search = %+v", job)
	}
	if expectedRows == 0 {
		expectedRows = job.GetProgress().GetProducedRows()
	}
	if expectedRows == 0 || job.GetProgress().GetProducedRows() != expectedRows {
		t.Fatalf(
			"HEC product search %q produced %d rows, want %d",
			spl,
			job.GetProgress().GetProducedRows(),
			expectedRows,
		)
	}
	return job, fetchAllCompletedSearchResults(
		t,
		ctx,
		client,
		baseURL,
		jobID,
		expectedRows,
		16,
	)
}

func backendHECRequireSearchColumns(
	t *testing.T,
	results *collectedVerticalSearchResults,
	want []string,
) {
	t.Helper()
	if results == nil || results.schema == nil || len(results.schema.GetColumns()) != len(want) {
		t.Fatalf("HEC product search schema = %+v, want columns %v", results, want)
	}
	for columnIndex, column := range results.schema.GetColumns() {
		if column.GetFieldName() != want[columnIndex] {
			t.Fatalf(
				"HEC product search column %d = %q, want %q",
				columnIndex,
				column.GetFieldName(),
				want[columnIndex],
			)
		}
	}
	for rowIndex, row := range results.rows {
		if len(row.GetCells()) != len(want) {
			t.Fatalf("HEC product search row %d has %d cells, want %d", rowIndex, len(row.GetCells()), len(want))
		}
	}
}

func backendHECRequireSearchColumnTypes(
	t *testing.T,
	results *collectedVerticalSearchResults,
	want []opensplunkv1.ValueType,
) {
	t.Helper()
	if results == nil || results.schema == nil || len(results.schema.GetColumns()) != len(want) {
		t.Fatalf("HEC product search schema = %+v, want value types %v", results, want)
	}
	for columnIndex, column := range results.schema.GetColumns() {
		if column.GetValueType() != want[columnIndex] {
			t.Fatalf(
				"HEC product search column %q type = %s, want %s",
				column.GetFieldName(),
				column.GetValueType(),
				want[columnIndex],
			)
		}
	}
}

func backendHECSearchString(t *testing.T, value *opensplunkv1.TypedValue) string {
	t.Helper()
	if _, ok := value.GetKind().(*opensplunkv1.TypedValue_StringValue); !ok {
		t.Fatalf("HEC product search cell = %+v, want string", value)
	}
	return value.GetStringValue()
}

func backendHECRequireSearchString(t *testing.T, value *opensplunkv1.TypedValue, want string) {
	t.Helper()
	if got := backendHECSearchString(t, value); got != want {
		t.Fatalf("HEC product search string = %q, want %q", got, want)
	}
}

func backendHECSearchUnsigned(t *testing.T, value *opensplunkv1.TypedValue) uint64 {
	t.Helper()
	if _, ok := value.GetKind().(*opensplunkv1.TypedValue_Uint64Value); !ok {
		t.Fatalf("HEC product search cell = %+v, want uint64", value)
	}
	return value.GetUint64Value()
}

func backendHECRequireSearchUnsigned(t *testing.T, value *opensplunkv1.TypedValue, want uint64) {
	t.Helper()
	if got := backendHECSearchUnsigned(t, value); got != want {
		t.Fatalf("HEC product search uint64 = %d, want %d", got, want)
	}
}

func backendHECRequireSearchSigned(t *testing.T, value *opensplunkv1.TypedValue, want int64) {
	t.Helper()
	if _, ok := value.GetKind().(*opensplunkv1.TypedValue_Sint64Value); !ok ||
		value.GetSint64Value() != want {
		t.Fatalf("HEC product search cell = %+v, want sint64(%d)", value, want)
	}
}

func backendHECRequireSearchBool(t *testing.T, value *opensplunkv1.TypedValue, want bool) {
	t.Helper()
	if _, ok := value.GetKind().(*opensplunkv1.TypedValue_BoolValue); !ok ||
		value.GetBoolValue() != want {
		t.Fatalf("HEC product search cell = %+v, want bool(%t)", value, want)
	}
}

func backendHECSearchTime(t *testing.T, value *opensplunkv1.TypedValue) time.Time {
	t.Helper()
	timestamp := value.GetTimestampValue()
	if _, ok := value.GetKind().(*opensplunkv1.TypedValue_TimestampValue); !ok ||
		timestamp == nil || timestamp.CheckValid() != nil {
		t.Fatalf("HEC product search cell = %+v, want valid timestamp", value)
	}
	return timestamp.AsTime()
}

func backendHECRequireSearchTime(t *testing.T, value *opensplunkv1.TypedValue, want time.Time) {
	t.Helper()
	if got := backendHECSearchTime(t, value); !got.Equal(want) {
		t.Fatalf("HEC product search timestamp = %s, want %s", got, want)
	}
}

func backendHECCreateToken(
	t *testing.T,
	ctx context.Context,
	client *http.Client,
	baseURL string,
	administratorToken string,
) (string, *opensplunkv1.IngestionToken) {
	t.Helper()
	defaultIndex := backendHECIndexName
	defaultHost := backendHECDefaultHost
	defaultSource := backendHECDefaultSource
	defaultSourcetype := backendHECDefaultSourcetype
	var created opensplunkv1.CreateIngestionTokenResponse
	postAdministratorProto(
		t,
		ctx,
		client,
		baseURL+"/api/v1/ingestion-tokens/create",
		administratorToken,
		&opensplunkv1.CreateIngestionTokenRequest{
			Definition: &opensplunkv1.IngestionTokenDefinition{
				Name:    "Backend HEC vertical token",
				Purpose: opensplunkv1.IngestionTokenPurpose_INGESTION_TOKEN_PURPOSE_HEC,
				Constraints: &opensplunkv1.IngestionTokenConstraints{
					AllowedIndexNames:  []string{backendHECIndexName},
					AllowedHostRegexes: []string{`^(hec-default-host-private|json-host|raw-host)$`},
					AllowedSourceRegexes: []string{
						`^(hec-default-source-private|json-source|raw-source)$`,
					},
				},
				HecProfile: &opensplunkv1.IngestionTokenHecProfile{
					DefaultIndexName:      &defaultIndex,
					DefaultHost:           &defaultHost,
					DefaultSource:         &defaultSource,
					DefaultSourcetype:     &defaultSourcetype,
					IndexerAcknowledgment: true,
				},
			},
		},
		&created,
	)
	plaintext := created.GetPlaintextToken()
	metadata := created.GetIngestionToken()
	if plaintext == "" || metadata.GetIngestionTokenId() == "" || metadata.GetVersion() != 1 ||
		metadata.GetPurpose() != opensplunkv1.IngestionTokenPurpose_INGESTION_TOKEN_PURPOSE_HEC ||
		metadata.GetConstraints().BoundCollectorId != nil ||
		!slices.Equal(metadata.GetConstraints().GetAllowedIndexNames(), []string{backendHECIndexName}) ||
		metadata.GetHecProfile().GetDefaultIndexName() != backendHECIndexName ||
		!metadata.GetHecProfile().GetIndexerAcknowledgment() ||
		!strings.HasPrefix(plaintext, metadata.GetTokenPrefix()) {
		t.Fatalf(
			"created HEC token metadata is invalid (plaintext length %d)",
			len(plaintext),
		)
	}

	var got opensplunkv1.GetIngestionTokenResponse
	getWire := postAdministratorProto(
		t,
		ctx,
		client,
		baseURL+"/api/v1/ingestion-tokens/get",
		administratorToken,
		&opensplunkv1.GetIngestionTokenRequest{IngestionTokenId: metadata.GetIngestionTokenId()},
		&got,
	)
	if bytes.Contains(getWire, []byte(plaintext)) ||
		got.GetIngestionToken().GetIngestionTokenId() != metadata.GetIngestionTokenId() ||
		got.GetIngestionToken().GetPurpose() != metadata.GetPurpose() {
		t.Fatal("HEC token readback disclosed plaintext or changed token identity")
	}
	var listed opensplunkv1.ListIngestionTokensResponse
	listWire := postAdministratorProto(
		t,
		ctx,
		client,
		baseURL+"/api/v1/ingestion-tokens/list",
		administratorToken,
		&opensplunkv1.ListIngestionTokensRequest{},
		&listed,
	)
	if bytes.Contains(listWire, []byte(plaintext)) {
		t.Fatal("HEC token list disclosed plaintext")
	}
	found := false
	for _, candidate := range listed.GetIngestionTokens() {
		if candidate.GetIngestionTokenId() == metadata.GetIngestionTokenId() {
			found = candidate.GetPurpose() == metadata.GetPurpose()
		}
	}
	if !found {
		t.Fatal("HEC token was not returned by administrative list")
	}
	return plaintext, metadata
}

type backendHECResponse struct {
	Text  string `json:"text"`
	Code  int    `json:"code"`
	AckID *int64 `json:"ackId,omitempty"`
}

func backendHECAssertAdvertised(
	t *testing.T,
	ctx context.Context,
	client *http.Client,
	baseURL string,
) {
	t.Helper()
	var bootstrap opensplunkv1.GetSystemBootstrapResponse
	postProto(
		t,
		ctx,
		client,
		baseURL+"/api/v1/system/bootstrap",
		&opensplunkv1.GetSystemBootstrapRequest{},
		&bootstrap,
	)
	features := 0
	for _, feature := range bootstrap.GetFeatures() {
		if feature == opensplunkv1.ServerFeature_SERVER_FEATURE_HEC_INGESTION {
			features++
		}
	}
	if features != 1 {
		t.Fatalf("HEC bootstrap feature count = %d, want 1", features)
	}
}

func backendHECAssertHealth(
	t *testing.T,
	ctx context.Context,
	client *http.Client,
	baseURL string,
) {
	t.Helper()
	request, err := http.NewRequestWithContext(
		ctx,
		http.MethodGet,
		baseURL+"/services/collector/health/1.0",
		nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	response, err := client.Do(request)
	if err != nil {
		t.Fatalf("GET HEC health: %v", err)
	}
	body := backendHECReadResponse(t, response)
	if response.StatusCode != http.StatusOK || string(body) != `{"text":"HEC is healthy","code":17}` {
		t.Fatalf("HEC health = status %d body %q", response.StatusCode, body)
	}
}

func backendHECIngest(
	t *testing.T,
	ctx context.Context,
	client *http.Client,
	endpoint string,
	plaintextToken string,
	channel string,
	contentType string,
	body []byte,
) int64 {
	t.Helper()
	wire := backendHECPost(t, ctx, client, endpoint, plaintextToken, channel, contentType, body)
	var response backendHECResponse
	if err := json.Unmarshal(wire, &response); err != nil {
		t.Fatalf("decode HEC ingestion response: %v", err)
	}
	if response.Text != "Success" || response.Code != 0 || response.AckID == nil ||
		*response.AckID < 1 || *response.AckID > backendHECMaximumAckID {
		t.Fatalf("HEC ingestion response = text %q code %d ack present %t", response.Text, response.Code, response.AckID != nil)
	}
	wantWire := fmt.Sprintf(`{"text":"Success","code":0,"ackId":%d}`, *response.AckID)
	if string(wire) != wantWire {
		t.Fatalf("HEC ingestion response bytes = %q, want %q", wire, wantWire)
	}
	return *response.AckID
}

func backendHECPost(
	t *testing.T,
	ctx context.Context,
	client *http.Client,
	endpoint string,
	plaintextToken string,
	channel string,
	contentType string,
	body []byte,
) []byte {
	t.Helper()
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Authorization", "Splunk "+plaintextToken)
	if channel != "" {
		request.Header.Set("X-Splunk-Request-Channel", channel)
	}
	if contentType != "" {
		request.Header.Set("Content-Type", contentType)
	}
	response, err := client.Do(request)
	if err != nil {
		t.Fatalf("POST HEC endpoint: %v", err)
	}
	wire := backendHECReadResponse(t, response)
	if response.StatusCode != http.StatusOK {
		t.Fatalf("HEC endpoint status = %d body = %q", response.StatusCode, wire)
	}
	return wire
}

func backendHECReadResponse(t *testing.T, response *http.Response) []byte {
	t.Helper()
	defer response.Body.Close()
	const maximum = 1 << 20
	body, err := io.ReadAll(io.LimitReader(response.Body, maximum+1))
	if err != nil {
		t.Fatalf("read HEC response: %v", err)
	}
	if len(body) > maximum {
		t.Fatal("HEC response exceeded integration-test bound")
	}
	if response.Header.Get("Content-Type") != "application/json; charset=utf-8" ||
		response.Header.Get("X-Content-Type-Options") != "nosniff" ||
		response.Header.Get("Cache-Control") != "no-store" {
		t.Fatalf(
			"HEC response security headers = content-type %q nosniff %q cache %q",
			response.Header.Get("Content-Type"),
			response.Header.Get("X-Content-Type-Options"),
			response.Header.Get("Cache-Control"),
		)
	}
	if response.ContentLength >= 0 && response.ContentLength != int64(len(body)) {
		t.Fatalf("HEC response Content-Length = %d, body bytes = %d", response.ContentLength, len(body))
	}
	return body
}

func backendHECQueryAcknowledgments(
	t *testing.T,
	ctx context.Context,
	client *http.Client,
	baseURL string,
	plaintextToken string,
	channel string,
	ids ...int64,
) map[int64]bool {
	t.Helper()
	body, err := json.Marshal(struct {
		Acknowledgments []int64 `json:"acks"`
	}{Acknowledgments: ids})
	if err != nil {
		t.Fatal(err)
	}
	wire := backendHECPost(
		t,
		ctx,
		client,
		baseURL+"/services/collector/ack",
		plaintextToken,
		channel,
		"application/json",
		body,
	)
	var decoded struct {
		Acknowledgments map[string]bool `json:"acks"`
	}
	if err := json.Unmarshal(wire, &decoded); err != nil {
		t.Fatalf("decode HEC acknowledgment response: %v", err)
	}
	if len(decoded.Acknowledgments) != len(ids) {
		t.Fatalf("HEC acknowledgment response count = %d, want %d", len(decoded.Acknowledgments), len(ids))
	}
	result := make(map[int64]bool, len(ids))
	var wantWire strings.Builder
	wantWire.WriteString(`{"acks":{`)
	for _, id := range ids {
		value, exists := decoded.Acknowledgments[strconv.FormatInt(id, 10)]
		if !exists {
			t.Fatalf("HEC acknowledgment response omitted ID %d", id)
		}
		result[id] = value
		if wantWire.Len() > len(`{"acks":{`) {
			wantWire.WriteByte(',')
		}
		_, _ = fmt.Fprintf(&wantWire, "%q:%t", strconv.FormatInt(id, 10), value)
	}
	wantWire.WriteString("}}")
	if string(wire) != wantWire.String() {
		t.Fatalf("HEC acknowledgment response bytes = %q, want %q", wire, wantWire.String())
	}
	return result
}

func backendHECWaitForAcknowledgment(
	t *testing.T,
	ctx context.Context,
	client *http.Client,
	baseURL string,
	plaintextToken string,
	channel string,
	id int64,
	process *managedProcess,
) {
	t.Helper()
	deadline := time.NewTimer(45 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		if backendHECQueryAcknowledgments(
			t,
			ctx,
			client,
			baseURL,
			plaintextToken,
			channel,
			id,
		)[id] {
			return
		}
		if process.Exited() {
			t.Fatalf("server exited before HEC acknowledgment %d indexed: %v", id, process.Err())
		}
		select {
		case <-ctx.Done():
			t.Fatalf("wait for HEC acknowledgment %d: %v", id, ctx.Err())
		case <-deadline.C:
			t.Fatalf("wait for HEC acknowledgment %d: timed out", id)
		case <-ticker.C:
		}
	}
}

func backendHECAssertStoredProjection(
	t *testing.T,
	ctx context.Context,
	storage clickhousedriver.Conn,
	tokenID string,
	eventTime time.Time,
) {
	t.Helper()
	var (
		eventID, batchID, body, host, source, sourcetype string
		raw, collectorID, sourceID                       []byte
		storedEventTime                                  time.Time
		timeSource, sourceKind, metadataVersion          uint8
		fieldNames                                       []string
		fieldTypes                                       []uint8
		textType, textValue                              string
		signedType                                       string
		signedValue                                      int64
		unsignedType                                     string
		unsignedValue                                    uint64
		flagType                                         string
		flagValue                                        bool
		nullType, decimalType, decimalKind, decimalValue string
	)
	err := storage.QueryRow(ctx, `
		SELECT event_id, batch_id, raw, ifNull(body, ''), host, source, sourcetype,
		       event_time, event_time_source, collector_id,
		       ingest_source_kind, ingest_source_id,
		       field_names, field_types, field_metadata_version,
		       dynamicType(fields.text), dynamicElement(fields.text, 'String'),
		       dynamicType(fields.signed), dynamicElement(fields.signed, 'Int64'),
		       dynamicType(fields.unsigned), dynamicElement(fields.unsigned, 'UInt64'),
		       dynamicType(fields.flag), dynamicElement(fields.flag, 'Bool'),
		       dynamicType(fields.nothing), dynamicType(fields.decimal),
		       dynamicElement(fields.decimal, 'Map(String, String)')[concat(char(0), 'open_splunk_type')],
		       dynamicElement(fields.decimal, 'Map(String, String)')[concat(char(0), 'open_splunk_value')]
		FROM open_splunk.events
		WHERE tenant_id = ? AND index_name = ? AND raw = ?
		ORDER BY index_time
		LIMIT 1`,
		backendHECTenantID,
		backendHECIndexName,
		backendHECJSONPayload,
	).Scan(
		&eventID,
		&batchID,
		&raw,
		&body,
		&host,
		&source,
		&sourcetype,
		&storedEventTime,
		&timeSource,
		&collectorID,
		&sourceKind,
		&sourceID,
		&fieldNames,
		&fieldTypes,
		&metadataVersion,
		&textType,
		&textValue,
		&signedType,
		&signedValue,
		&unsignedType,
		&unsignedValue,
		&flagType,
		&flagValue,
		&nullType,
		&decimalType,
		&decimalKind,
		&decimalValue,
	)
	if err != nil {
		t.Fatalf("query stored HEC JSON projection: %v", err)
	}
	if string(raw) != backendHECJSONPayload || body != backendHECJSONPayload ||
		host != "json-host" || source != "json-source" || sourcetype != "json-type" ||
		!storedEventTime.Equal(eventTime) ||
		timeSource != uint8(opensplunkv1.EventTimeSource_EVENT_TIME_SOURCE_PARSED) {
		t.Fatalf(
			"stored HEC JSON projection mismatch = raw %t body %t metadata %q/%q/%q time %v source %d",
			string(raw) == backendHECJSONPayload,
			body == backendHECJSONPayload,
			host,
			source,
			sourcetype,
			storedEventTime,
			timeSource,
		)
	}
	if string(collectorID) != "" || sourceKind != uint8(ingest.IngestionSourceKindHEC) ||
		string(sourceID) != tokenID || batchID == "" || eventID != batchID+"-0" {
		t.Fatalf(
			"stored HEC provenance = event %q batch present %t collector empty %t source %d/%q",
			eventID,
			batchID != "",
			len(collectorID) == 0,
			sourceKind,
			sourceID,
		)
	}
	wantNames := []string{"decimal", "flag", "nothing", "signed", "text", "unsigned"}
	wantTypes := []uint8{
		uint8(eventfields.StoredValueTypeDecimal),
		uint8(eventfields.StoredValueTypeBool),
		uint8(eventfields.StoredValueTypeNull),
		uint8(eventfields.StoredValueTypeSint64),
		uint8(eventfields.StoredValueTypeString),
		uint8(eventfields.StoredValueTypeUint64),
	}
	if !slices.Equal(fieldNames, wantNames) || !slices.Equal(fieldTypes, wantTypes) ||
		metadataVersion != eventfields.CurrentFieldMetadataVersion ||
		textType != "String" || textValue != "exact" ||
		signedType != "Int64" || signedValue != -9223372036854775808 ||
		unsignedType != "UInt64" || unsignedValue != ^uint64(0) ||
		flagType != "Bool" || !flagValue || nullType != "None" ||
		decimalType != "Map(String, String)" || decimalKind != "decimal/v1" ||
		decimalValue != "1.2300e4" {
		t.Fatalf(
			"stored HEC typed projection = names %#v types %#v version %d dynamic %q/%q/%q/%q/%q decimal %q/%q",
			fieldNames,
			fieldTypes,
			metadataVersion,
			textType,
			signedType,
			unsignedType,
			flagType,
			nullType,
			decimalKind,
			decimalValue,
		)
	}
	var (
		objectEventID, objectBatchID, objectRaw    string
		objectHost, objectSource, objectSourcetype string
		objectTimeSource, objectSourceKind         uint8
		objectBodyMissing                          bool
		objectCollectorID, objectSourceID          []byte
		objectFieldNames                           []string
		objectFieldTypes                           []uint8
	)
	if err := storage.QueryRow(ctx, `
		SELECT event_id, batch_id, raw, isNull(body), host, source, sourcetype,
		       event_time_source, collector_id, ingest_source_kind, ingest_source_id,
		       field_names, field_types
		FROM open_splunk.events
		WHERE tenant_id = ? AND index_name = ? AND raw = ?
		LIMIT 1`,
		backendHECTenantID,
		backendHECIndexName,
		`{"kind":"`+backendHECObjectPayload+`","n":1.00}`,
	).Scan(
		&objectEventID,
		&objectBatchID,
		&objectRaw,
		&objectBodyMissing,
		&objectHost,
		&objectSource,
		&objectSourcetype,
		&objectTimeSource,
		&objectCollectorID,
		&objectSourceKind,
		&objectSourceID,
		&objectFieldNames,
		&objectFieldTypes,
	); err != nil {
		t.Fatalf("query stored HEC object projection: %v", err)
	}
	wantObjectRaw := `{"kind":"` + backendHECObjectPayload + `","n":1.00}`
	if objectRaw != wantObjectRaw || !objectBodyMissing || objectBatchID != batchID ||
		objectEventID != batchID+"-1" || objectHost != backendHECDefaultHost ||
		objectSource != backendHECDefaultSource || objectSourcetype != backendHECDefaultSourcetype ||
		objectTimeSource != uint8(opensplunkv1.EventTimeSource_EVENT_TIME_SOURCE_RECEIVED_AT_FALLBACK) ||
		len(objectCollectorID) != 0 || objectSourceKind != uint8(ingest.IngestionSourceKindHEC) ||
		string(objectSourceID) != tokenID || len(objectFieldNames) != 0 || len(objectFieldTypes) != 0 {
		t.Fatal("stored HEC object/default-metadata projection is not exact")
	}

	rows, err := storage.Query(ctx, `
		SELECT event_id, batch_id, raw, ifNull(body, ''), host, source, sourcetype,
		       event_time, collector_id, ingest_source_kind, ingest_source_id,
		       field_names, field_types
		FROM open_splunk.events
		WHERE tenant_id = ? AND index_name = ? AND raw IN (?, ?)
		ORDER BY event_id`,
		backendHECTenantID,
		backendHECIndexName,
		backendHECRawPayloadOne,
		backendHECRawPayloadTwo,
	)
	if err != nil {
		t.Fatalf("query stored HEC raw projections: %v", err)
	}
	defer rows.Close()
	rawEvents := 0
	var rawBatchID string
	for rows.Next() {
		var (
			rawEventID, candidateBatchID, candidateBody, candidateHost string
			candidateSource, candidateSourcetype                       string
			candidateRaw, candidateCollector, candidateSourceID        []byte
			candidateTime                                              time.Time
			candidateKind                                              uint8
			candidateNames                                             []string
			candidateTypes                                             []uint8
		)
		if err := rows.Scan(
			&rawEventID,
			&candidateBatchID,
			&candidateRaw,
			&candidateBody,
			&candidateHost,
			&candidateSource,
			&candidateSourcetype,
			&candidateTime,
			&candidateCollector,
			&candidateKind,
			&candidateSourceID,
			&candidateNames,
			&candidateTypes,
		); err != nil {
			t.Fatalf("scan stored HEC raw projection: %v", err)
		}
		if rawBatchID == "" {
			rawBatchID = candidateBatchID
		}
		wantRaw := backendHECRawPayloadOne
		if rawEvents == 1 {
			wantRaw = backendHECRawPayloadTwo
		}
		if string(candidateRaw) != wantRaw || candidateBody != wantRaw ||
			candidateBatchID != rawBatchID || rawEventID != rawBatchID+"-"+strconv.Itoa(rawEvents) ||
			candidateHost != "raw-host" || candidateSource != "raw-source" ||
			candidateSourcetype != "raw-type" || !candidateTime.Equal(eventTime) ||
			len(candidateCollector) != 0 || candidateKind != uint8(ingest.IngestionSourceKindHEC) ||
			string(candidateSourceID) != tokenID || len(candidateNames) != 0 || len(candidateTypes) != 0 {
			t.Fatalf("stored HEC raw projection %d is not exact", rawEvents)
		}
		rawEvents++
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate stored HEC raw projections: %v", err)
	}
	if rawEvents != 2 {
		t.Fatalf("stored HEC raw projection count = %d, want 2", rawEvents)
	}
}

func backendHECAssertStoredCardinality(
	t *testing.T,
	ctx context.Context,
	storage clickhousedriver.Conn,
	wantEvents uint64,
	wantBatches uint64,
	wantJSONPayloadCopies uint64,
) {
	t.Helper()
	var events, distinctEvents, batches, jsonCopies uint64
	if err := storage.QueryRow(ctx, `
		SELECT count(), uniqExact(event_id), uniqExact(batch_id), countIf(raw = ?)
		FROM open_splunk.events
		WHERE tenant_id = ? AND index_name = ?`,
		backendHECJSONPayload,
		backendHECTenantID,
		backendHECIndexName,
	).Scan(&events, &distinctEvents, &batches, &jsonCopies); err != nil {
		t.Fatalf("query HEC stored cardinality: %v", err)
	}
	if events != wantEvents || distinctEvents != wantEvents || batches != wantBatches ||
		jsonCopies != wantJSONPayloadCopies {
		t.Fatalf(
			"HEC stored cardinality = events %d distinct %d batches %d JSON copies %d, want %d/%d/%d/%d",
			events,
			distinctEvents,
			batches,
			jsonCopies,
			wantEvents,
			wantEvents,
			wantBatches,
			wantJSONPayloadCopies,
		)
	}
}

func backendHECAssertAuditRedaction(
	t *testing.T,
	ctx context.Context,
	client *http.Client,
	baseURL string,
	administratorToken string,
	plaintextToken string,
	protectedValues []string,
) {
	t.Helper()
	var listed opensplunkv1.ListAuditEventsResponse
	wire := postAdministratorProto(
		t,
		ctx,
		client,
		baseURL+"/api/v1/audit/events/list",
		administratorToken,
		&opensplunkv1.ListAuditEventsRequest{
			Page: &opensplunkv1.PageRequest{IncludeTotalSize: true},
		},
		&listed,
	)
	for _, protected := range protectedValues {
		if protected != "" && bytes.Contains(wire, []byte(protected)) {
			t.Fatal("administrative audit response leaked protected HEC material")
		}
	}
	if bytes.Contains(wire, []byte(plaintextToken)) {
		t.Fatal("administrative audit response leaked HEC plaintext token")
	}
	foundTokenCreation := false
	for _, event := range listed.GetAuditEvents() {
		if event.GetAction() == opensplunkv1.AuditAction_AUDIT_ACTION_INGESTION_TOKEN_CREATE &&
			event.GetTargetKind() == opensplunkv1.AuditTargetKind_AUDIT_TARGET_KIND_INGESTION_TOKEN {
			foundTokenCreation = true
		}
	}
	if !foundTokenCreation {
		t.Fatal("administrative audit response omitted HEC token creation")
	}
}

func backendHECEpochSeconds(value time.Time) string {
	value = value.UTC()
	if value.Nanosecond() == 0 {
		return strconv.FormatInt(value.Unix(), 10)
	}
	return fmt.Sprintf("%d.%09d", value.Unix(), value.Nanosecond())
}

func backendHECDocker(t *testing.T, ctx context.Context, arguments ...string) {
	t.Helper()
	if err := backendHECDockerError(ctx, arguments...); err != nil {
		t.Fatalf("docker %s: %v", strings.Join(arguments, " "), err)
	}
}

func backendHECDockerError(ctx context.Context, arguments ...string) error {
	command := exec.CommandContext(ctx, "docker", arguments...)
	output, truncated, err := runCommandWithBoundedOutput(command, maximumHarnessOutputBytes)
	if err != nil {
		return fmt.Errorf(
			"%w: %s",
			err,
			formatBoundedCommandOutput(output, truncated, maximumHarnessOutputBytes),
		)
	}
	if truncated {
		return fmt.Errorf("output exceeded %d bytes", maximumHarnessOutputBytes)
	}
	return nil
}
