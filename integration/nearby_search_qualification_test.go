//go:build !windows

package integration_test

// Controlled qualification (Docker and two already-built release binaries):
//
// OPEN_SPLUNK_NEARBY_SEARCH_QUALIFICATION=1 \
// OPEN_SPLUNK_NEARBY_BASELINE_SERVER=/absolute/baseline/open-splunk-server \
// OPEN_SPLUNK_NEARBY_CANDIDATE_SERVER=/absolute/candidate/open-splunk-server \
// OPEN_SPLUNK_NEARBY_CANDIDATE_REVISION=<exact-40-hex-candidate-commit> \
// go test ./integration -run '^TestNearbyOrdinarySearchQualification$' -count=1 -v -timeout=15m
//
// Build baseline da8415f3 and candidate with the same release toolchain before
// entering the controlled idle slot. This test does not build either binary.
// It emits separate NEARBY_SEARCH_QUALIFICATION JSON reports for unkeyed and
// browser-behavior lanes, each with seven alternating pairs and exact parity.
// Baseline rejects keys; the browser lane keys only fresh candidate admissions.
// Compile/helper validation without Docker: go test ./integration -run '^TestNearbyQualification' -count=1

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"hash"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	clickhousedriver "github.com/ClickHouse/clickhouse-go/v2"
	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/testsupport"
	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	nearbyQualificationFlag                 = "OPEN_SPLUNK_NEARBY_SEARCH_QUALIFICATION"
	nearbyQualificationBaselineBinaryEnv    = "OPEN_SPLUNK_NEARBY_BASELINE_SERVER"
	nearbyQualificationCandidateBinaryEnv   = "OPEN_SPLUNK_NEARBY_CANDIDATE_SERVER"
	nearbyQualificationCandidateRevisionEnv = "OPEN_SPLUNK_NEARBY_CANDIDATE_REVISION"
	nearbyQualificationBaselineRevision     = "da8415f3bf4ad115da0a0b3941e5c333392ae3b9"
	nearbyQualificationTenant               = "nearby-qualification-tenant"
	nearbyQualificationIndex                = "nearby-qualification"
	nearbyQualificationSource               = "qualification.log"
	nearbyQualificationRows                 = uint64(10_001)
	nearbyQualificationRetainedRows         = uint64(10_000)
	nearbyQualificationPairs                = 7
	nearbyQualificationRegressionLimit      = 1.10
)

const nearbyQualificationInsertSQL = "INSERT INTO open_splunk.events (event_id, tenant_id, index_name, event_time, index_time, " +
	"collected_at, event_time_source, host, source, sourcetype, service, severity, level, body, raw, " +
	"raw_encoding, trace_id, span_id, fields, field_names, field_types, field_metadata_version, collector_id, " +
	"ingest_source_kind, ingest_source_id, batch_id, batch_sequence, expires_at, visibility_seq)"

var (
	nearbyQualificationFixtureStart = time.Date(2026, time.September, 12, 12, 0, 0, 123456789, time.UTC)
	// Fixed ingest time precedes both servers' current index-time cutoffs.
	nearbyQualificationIndexTime = time.Date(2020, time.January, 1, 0, 0, 0, 0, time.UTC)
)

type nearbyQualificationServer struct {
	name       string
	baseURL    string
	adminToken string
	client     *http.Client
	process    *managedProcess
	expectsRef bool
}

type nearbyQualificationSearch struct {
	name         string
	spl          string
	expectedRows uint64
	truncated    bool
}

type nearbyQualificationLane struct {
	name        string
	newKey      func() string
	keySequence uint64
}

func (lane *nearbyQualificationLane) requestKey(serverName string) *string {
	if lane.newKey == nil || serverName != "candidate" {
		return nil
	}
	lane.keySequence++
	return new(lane.newKey())
}

type nearbyQualificationObservation struct {
	clientRequestID string
	duration        time.Duration
	digest          string
	rows            uint64
	truncated       bool
}

type nearbyQualificationPairReport struct {
	BaselineResultSHA256     string   `json:"baseline_result_sha256"`
	CandidateResultSHA256    string   `json:"candidate_result_sha256"`
	CandidateClientRequestID string   `json:"candidate_client_request_id,omitempty"`
	Pair                     int      `json:"pair"`
	Order                    []string `json:"order"`
	BaselineNS               int64    `json:"baseline_ns"`
	CandidateNS              int64    `json:"candidate_ns"`
}

type nearbyQualificationReport struct {
	Lane                  string                          `json:"lane"`
	BaselineAdmission     string                          `json:"baseline_admission"`
	CandidateAdmission    string                          `json:"candidate_admission"`
	WarmupPairs           int                             `json:"warmup_pairs"`
	Completed             bool                            `json:"completed"`
	BaselineBinarySHA256  string                          `json:"baseline_binary_sha256"`
	CandidateBinarySHA256 string                          `json:"candidate_binary_sha256"`
	NumCPU                int                             `json:"num_cpu"`
	FormatVersion         int                             `json:"format_version"`
	BaselineRevision      string                          `json:"baseline_revision"`
	CandidateRevision     string                          `json:"candidate_revision"`
	ClickHouseImage       string                          `json:"clickhouse_image"`
	ClickHouseVersion     string                          `json:"clickhouse_version"`
	FixtureSHA256         string                          `json:"fixture_sha256"`
	FixtureRows           uint64                          `json:"fixture_rows"`
	RetainedRows          uint64                          `json:"retained_rows"`
	Pairs                 []nearbyQualificationPairReport `json:"pairs"`
	ParitySHA256          string                          `json:"parity_sha256"`
	ErrorSHA256           string                          `json:"error_sha256"`
	BaselineMedianNS      int64                           `json:"baseline_median_ns"`
	CandidateMedianNS     int64                           `json:"candidate_median_ns"`
	MedianRatio           float64                         `json:"median_ratio"`
	BaselineP95NS         int64                           `json:"baseline_p95_ns"`
	CandidateP95NS        int64                           `json:"candidate_p95_ns"`
	P95Ratio              float64                         `json:"p95_ratio"`
	RegressionLimit       float64                         `json:"regression_limit"`
	GoVersion             string                          `json:"go_version"`
	GOOS                  string                          `json:"goos"`
	GOARCH                string                          `json:"goarch"`
	GOMAXPROCS            int                             `json:"gomaxprocs"`
	Passed                bool                            `json:"passed"`
}

func TestNearbyQualificationStatistics(t *testing.T) {
	t.Parallel()
	samples := []time.Duration{70, 10, 40, 20, 60, 30, 50}
	if got := nearbyQualificationPercentile(samples, 50); got != 40 {
		t.Fatalf("median = %s, want 40ns", got)
	}
	if got := nearbyQualificationPercentile(samples, 95); got != 70 {
		t.Fatalf("p95 = %s, want 70ns", got)
	}
	if !nearbyQualificationWithinLimit(100, 110) || nearbyQualificationWithinLimit(100, 111) {
		t.Fatal("10% qualification boundary is not exact")
	}
}

func TestNearbyQualificationResultDigestBindsTypesOrderAndTruncation(t *testing.T) {
	t.Parallel()
	schema := &opensplunk.ResultSchema{Columns: []*opensplunk.ResultColumn{{
		FieldName: "value", ValueType: opensplunk.ValueType_VALUE_TYPE_STRING,
	}}}
	rows := []*opensplunk.ResultRow{{
		RowId: "job-a:0", Ordinal: 0,
		Cells: []*opensplunk.TypedValue{{Kind: &opensplunk.TypedValue_StringValue{StringValue: "x"}}},
	}}
	first, err := nearbyQualificationResultDigest(schema, rows, 1, true, true)
	if err != nil {
		t.Fatal(err)
	}
	rows[0].RowId = "job-b:0"
	second, err := nearbyQualificationResultDigest(schema, rows, 1, true, true)
	if err != nil || first != second {
		t.Fatalf("job-bound row ID changed canonical result digest: %q/%q, %v", first, second, err)
	}
	rows[0].Cells[0] = &opensplunk.TypedValue{Kind: &opensplunk.TypedValue_Uint64Value{Uint64Value: 1}}
	typed, err := nearbyQualificationResultDigest(schema, rows, 1, true, true)
	if err != nil || typed == first {
		t.Fatalf("typed value did not change result digest: %q/%q, %v", first, typed, err)
	}
	rows[0].Cells[0] = &opensplunk.TypedValue{Kind: &opensplunk.TypedValue_StringValue{StringValue: "x"}}
	notTruncated, err := nearbyQualificationResultDigest(schema, rows, 1, true, false)
	if err != nil || notTruncated == first {
		t.Fatalf("truncation did not change result digest: %q/%q, %v", first, notTruncated, err)
	}
}

func TestNearbyOrdinarySearchQualification(t *testing.T) {
	if os.Getenv(nearbyQualificationFlag) != "1" {
		t.Skip("set " + nearbyQualificationFlag + "=1 to run baseline/candidate qualification")
	}
	if _, err := exec.LookPath("docker"); err != nil {
		t.Fatalf("docker CLI is required when %s=1: %v", nearbyQualificationFlag, err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 11*time.Minute)
	defer cancel()
	candidateRevision := strings.TrimSpace(os.Getenv(nearbyQualificationCandidateRevisionEnv))
	if !nearbyQualificationRevision(candidateRevision) || candidateRevision == nearbyQualificationBaselineRevision {
		t.Fatalf("%s must be the candidate's distinct exact 40-hex revision", nearbyQualificationCandidateRevisionEnv)
	}
	baselineBinary := nearbyQualificationBinary(t, nearbyQualificationBaselineBinaryEnv)
	candidateBinary := nearbyQualificationBinary(t, nearbyQualificationCandidateBinaryEnv)
	if baselineBinary == candidateBinary {
		t.Fatal("baseline and candidate server binaries must be distinct files")
	}
	nearbyQualificationRequireBinaryRevision(t, ctx, baselineBinary, nearbyQualificationBaselineRevision)
	nearbyQualificationRequireBinaryRevision(t, ctx, candidateBinary, candidateRevision)
	baselineBinaryDigest := nearbyQualificationBinaryDigest(t, baselineBinary)
	candidateBinaryDigest := nearbyQualificationBinaryDigest(t, candidateBinary)

	image, err := testsupport.ResolvePinnedClickHouseImage(os.Getenv("OPEN_SPLUNK_CLICKHOUSE_TEST_IMAGE"))
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
	// Fixture identity belongs to the comparison, independently of the runtime
	// resources created from the configured binaries and ClickHouse image.
	baseline := &nearbyQualificationServer{name: "baseline"}
	candidate := &nearbyQualificationServer{name: "candidate", expectsRef: true}
	nearbyQualificationStartServer(t, ctx, clickHouse, baseline, baselineBinary)
	nearbyQualificationStartServer(t, ctx, clickHouse, candidate, candidateBinary)

	connection, err := clickhousedriver.Open(&clickhousedriver.Options{
		Addr: []string{clickHouse.Address},
		Auth: clickhousedriver.Auth{
			Database: clickHouse.Database, Username: clickHouse.Username, Password: clickHouse.Password,
		},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = connection.Close() })
	if err := connection.Ping(ctx); err != nil {
		t.Fatalf("ping qualification ClickHouse: %v", err)
	}
	fixtureDigest := nearbyQualificationInsertFixture(t, ctx, connection)
	nearbyQualificationAdvanceVisibility(t, ctx, baseline)
	nearbyQualificationAdvanceVisibility(t, ctx, candidate)
	if err := connection.Exec(ctx, "OPTIMIZE TABLE open_splunk.events FINAL"); err != nil {
		t.Fatalf("merge qualification fixture: %v", err)
	}
	nearbyQualificationVerifyFixture(t, ctx, connection)
	var clickHouseVersion string
	if err := connection.QueryRow(ctx, "SELECT version()").Scan(&clickHouseVersion); err != nil {
		t.Fatalf("read qualification ClickHouse version: %v", err)
	}

	metadata := nearbyQualificationReport{
		BaselineBinarySHA256:  baselineBinaryDigest,
		CandidateBinarySHA256: candidateBinaryDigest,
		NumCPU:                runtime.NumCPU(), FormatVersion: 2,
		BaselineRevision:  nearbyQualificationBaselineRevision,
		CandidateRevision: candidateRevision,
		ClickHouseImage:   clickHouse.Image, ClickHouseVersion: clickHouseVersion,
		FixtureSHA256: fixtureDigest, FixtureRows: nearbyQualificationRows,
		RetainedRows:    nearbyQualificationRetainedRows,
		RegressionLimit: nearbyQualificationRegressionLimit,
		GoVersion:       runtime.Version(), GOOS: runtime.GOOS, GOARCH: runtime.GOARCH,
		GOMAXPROCS: runtime.GOMAXPROCS(0),
	}
	for _, lane := range []*nearbyQualificationLane{
		{name: "unkeyed"},
		{name: "browser_behavior", newKey: uuid.NewString},
	} {
		// A failed lane must not suppress the other lane's measurements/report.
		t.Run(lane.name, func(t *testing.T) {
			nearbyQualificationRunLane(t, ctx, baseline, candidate, lane, metadata)
		})
	}
}

func nearbyQualificationRunLane(
	t *testing.T,
	ctx context.Context,
	baseline, candidate *nearbyQualificationServer,
	lane *nearbyQualificationLane,
	report nearbyQualificationReport,
) {
	t.Helper()
	report.Lane = lane.name
	report.BaselineAdmission = "unkeyed"
	report.CandidateAdmission = "unkeyed"
	if lane.newKey != nil {
		report.CandidateAdmission = "fresh_unique_client_request_id"
	}
	defer func() {
		// Preserve partial observations even if a parity/admission check fails.
		report.Passed = report.Passed && !t.Failed()
		encoded, err := json.Marshal(report)
		if err != nil {
			t.Errorf("encode %s qualification report: %v", lane.name, err)
			return
		}
		t.Logf("NEARBY_SEARCH_QUALIFICATION %s", encoded)
	}()
	paritySearches := []nearbyQualificationSearch{
		{
			name:         "bounded ordinary events",
			spl:          `index=nearby-qualification source="qualification.log" | sort 0 +event_id | head 257 | table _time index host source event_id message severity`,
			expectedRows: 257,
		},
		{
			name:         "ordinary statistics",
			spl:          `index=nearby-qualification source="qualification.log" | stats count AS events dc(event_id) AS event_ids min(_time) AS first max(_time) AS last`,
			expectedRows: 1,
		},
	}
	parity := sha256.New()
	for _, search := range paritySearches {
		baselineObservation := nearbyQualificationRunSearch(t, ctx, baseline, lane, search)
		candidateObservation := nearbyQualificationRunSearch(t, ctx, candidate, lane, search)
		nearbyQualificationRequireParity(t, search.name, baselineObservation, candidateObservation)
		nearbyQualificationWritePart(parity, search.name, []byte(baselineObservation.digest))
	}
	report.ErrorSHA256 = nearbyQualificationRequireErrorParity(t, ctx, baseline, candidate, lane)

	performance := nearbyQualificationSearch{
		name:         "truncated ordinary events",
		spl:          `index=nearby-qualification source="qualification.log" | sort 0 +event_id | table _time index host source event_id message severity`,
		expectedRows: nearbyQualificationRetainedRows,
		truncated:    true,
	}
	baselineWarm := nearbyQualificationRunSearch(t, ctx, baseline, lane, performance)
	candidateWarm := nearbyQualificationRunSearch(t, ctx, candidate, lane, performance)
	nearbyQualificationRequireParity(t, "performance warmup", baselineWarm, candidateWarm)
	nearbyQualificationWritePart(parity, performance.name, []byte(baselineWarm.digest))
	report.WarmupPairs = 1
	report.ParitySHA256 = hex.EncodeToString(parity.Sum(nil))

	baselineSamples := make([]time.Duration, 0, nearbyQualificationPairs)
	candidateSamples := make([]time.Duration, 0, nearbyQualificationPairs)
	report.Pairs = make([]nearbyQualificationPairReport, 0, nearbyQualificationPairs)
	for pair := range nearbyQualificationPairs {
		order := []*nearbyQualificationServer{baseline, candidate}
		if pair%2 == 1 {
			order = []*nearbyQualificationServer{candidate, baseline}
		}
		observations := make(map[string]nearbyQualificationObservation, 2)
		for _, server := range order {
			observations[server.name] = nearbyQualificationRunSearch(t, ctx, server, lane, performance)
		}
		baselineObservation := observations[baseline.name]
		candidateObservation := observations[candidate.name]
		report.Pairs = append(report.Pairs, nearbyQualificationPairReport{
			CandidateClientRequestID: candidateObservation.clientRequestID,
			BaselineResultSHA256:     baselineObservation.digest,
			CandidateResultSHA256:    candidateObservation.digest,
			Pair:                     pair + 1,
			Order:                    []string{order[0].name, order[1].name},
			BaselineNS:               baselineObservation.duration.Nanoseconds(),
			CandidateNS:              candidateObservation.duration.Nanoseconds(),
		})
		nearbyQualificationRequireParity(t, fmt.Sprintf("performance pair %d", pair+1), baselineObservation, candidateObservation)
		if baselineObservation.digest != baselineWarm.digest {
			t.Fatalf("performance pair %d changed immutable fixture result digest", pair+1)
		}
		baselineSamples = append(baselineSamples, baselineObservation.duration)
		candidateSamples = append(candidateSamples, candidateObservation.duration)

	}

	baselineMedian := nearbyQualificationPercentile(baselineSamples, 50)
	candidateMedian := nearbyQualificationPercentile(candidateSamples, 50)
	baselineP95 := nearbyQualificationPercentile(baselineSamples, 95)
	candidateP95 := nearbyQualificationPercentile(candidateSamples, 95)
	medianPass := nearbyQualificationWithinLimit(baselineMedian, candidateMedian)
	p95Pass := nearbyQualificationWithinLimit(baselineP95, candidateP95)
	report.BaselineMedianNS = baselineMedian.Nanoseconds()
	report.CandidateMedianNS = candidateMedian.Nanoseconds()
	report.MedianRatio = nearbyQualificationRatio(candidateMedian, baselineMedian)
	report.BaselineP95NS = baselineP95.Nanoseconds()
	report.CandidateP95NS = candidateP95.Nanoseconds()
	report.P95Ratio = nearbyQualificationRatio(candidateP95, baselineP95)
	report.Completed = true
	report.Passed = medianPass && p95Pass
	if !report.Passed {
		t.Fatalf(
			"candidate regression exceeds %.0f%%: median %.4fx, p95 %.4fx",
			(nearbyQualificationRegressionLimit-1)*100,
			report.MedianRatio,
			report.P95Ratio,
		)
	}
}

func nearbyQualificationStartServer(
	t *testing.T,
	ctx context.Context,
	clickHouse *testsupport.ClickHouseContainer,
	server *nearbyQualificationServer,
	binary string,
) {
	t.Helper()
	work := t.TempDir()
	runtimeDirectory := filepath.Join(work, "runtime")
	if err := os.Mkdir(runtimeDirectory, 0o700); err != nil {
		t.Fatalf("create %s runtime directory: %v", server.name, err)
	}
	administratorTokenPath, administratorToken := provisionAdministratorToken(t, work)
	httpAddress, collectorAddress := unusedLoopbackAddressPair(t)
	environment := clickHouseServerEnvironment(os.Environ(), clickHouse)
	environment = environmentWithValue(environment, "PATH", filepath.Join(runtimeDirectory, "no-external-runtime"))
	arguments := []string{
		binary,
		"-http-listen-address=" + httpAddress,
		"-control-database-file=" + filepath.Join(work, "control.sqlite"),
		"-master-key-file=" + filepath.Join(work, "server.key"),
		"-administrator-token-file=" + administratorTokenPath,
		"-collector-grpc-listen-address=" + collectorAddress,
		"-collector-grpc-plaintext-enabled",
		"-tenant-id=" + nearbyQualificationTenant,
		"-hec-enabled=true",
	}
	arguments = append(arguments, clickHouseServerArguments(clickHouse)...)
	process := startProcess(t, runtimeDirectory, arguments, environment)
	server.baseURL = "http://" + httpAddress
	server.adminToken = administratorToken
	server.client = &http.Client{Timeout: 15 * time.Second}
	server.process = process
	waitForHealth(t, ctx, server.client, server.baseURL, process, administratorToken)
	createBackendIndex(
		t, ctx, server.client, server.baseURL, administratorToken,
		nearbyQualificationIndex, "Nearby qualification fixture",
	)
}

func nearbyQualificationInsertFixture(
	t *testing.T,
	ctx context.Context,
	connection clickhousedriver.Conn,
) string {
	t.Helper()
	batch, err := connection.PrepareBatch(ctx, nearbyQualificationInsertSQL)
	if err != nil {
		t.Fatalf("prepare nearby qualification fixture: %v", err)
	}
	digest := sha256.New()
	nearbyQualificationWritePart(digest, "columns", []byte(nearbyQualificationInsertSQL))
	nearbyQualificationWritePart(digest, "tenant", []byte(nearbyQualificationTenant))
	nearbyQualificationWritePart(digest, "index_time", []byte(nearbyQualificationIndexTime.Format(time.RFC3339Nano)))
	for ordinal := range nearbyQualificationRows {
		eventID := fmt.Sprintf("nearby-qualification-%05d", ordinal)
		eventTime := nearbyQualificationFixtureStart.Add(time.Duration(ordinal) * time.Microsecond)
		host := fmt.Sprintf("qualification-host-%02d", ordinal%8)
		message := fmt.Sprintf("nearby ordinary search fixture %05d", ordinal)
		severity := uint8(ordinal % 8)
		for _, part := range []string{
			eventID, eventTime.Format(time.RFC3339Nano), nearbyQualificationIndex,
			host, nearbyQualificationSource, message, strconv.FormatUint(uint64(severity), 10),
		} {
			nearbyQualificationWritePart(digest, "fixture", []byte(part))
		}
		document := clickhousedriver.NewJSON()
		if err := batch.Append(
			eventID, nearbyQualificationTenant, nearbyQualificationIndex,
			eventTime, nearbyQualificationIndexTime,
			nil, uint8(1), host, nearbyQualificationSource, "qualification",
			nil, severity, nil, &message, []byte(message), uint8(1), nil, nil,
			document, []string(nil), []uint8(nil), uint8(1),
			"nearby-qualification", uint8(1), "nearby-qualification",
			"nearby-qualification-batch", uint64(1),
			time.Date(2100, time.January, 1, 0, 0, 0, 0, time.UTC), uint64(1),
		); err != nil {
			t.Fatalf("append nearby qualification row %d: %v", ordinal, err)
		}
	}
	if err := batch.Send(); err != nil {
		t.Fatalf("insert nearby qualification fixture: %v", err)
	}
	return hex.EncodeToString(digest.Sum(nil))
}

func nearbyQualificationAdvanceVisibility(
	t *testing.T,
	ctx context.Context,
	server *nearbyQualificationServer,
) {
	t.Helper()
	defaultIndex := nearbyQualificationIndex
	defaultHost := "qualification-primer-host"
	defaultSource := "visibility-primer.log"
	defaultSourcetype := "qualification"
	var created opensplunk.CreateIngestionTokenResponse
	postAdministratorProto(
		t, ctx, server.client, server.baseURL+"/api/ingestion-tokens/create", server.adminToken,
		&opensplunk.CreateIngestionTokenRequest{Definition: &opensplunk.IngestionTokenDefinition{
			Name:        "Nearby qualification visibility primer",
			Purpose:     opensplunk.IngestionTokenPurpose_INGESTION_TOKEN_PURPOSE_HEC,
			Constraints: &opensplunk.IngestionTokenConstraints{AllowedIndexNames: []string{nearbyQualificationIndex}},
			HecProfile: &opensplunk.IngestionTokenHecProfile{
				DefaultIndexName: &defaultIndex, DefaultHost: &defaultHost,
				DefaultSource: &defaultSource, DefaultSourcetype: &defaultSourcetype,
				IndexerAcknowledgment: true,
			},
		}},
		&created,
	)
	token := created.GetPlaintextToken()
	if token == "" {
		t.Fatalf("%s visibility primer token is empty", server.name)
	}
	channel := "11111111-1111-4111-8111-111111111111"
	if server.name == "candidate" {
		channel = "22222222-2222-4222-8222-222222222222"
	}
	body := []byte(fmt.Sprintf(
		`{"time":%d,"event":"%s visibility primer"}`,
		nearbyQualificationFixtureStart.Add(-time.Hour).Unix(), server.name,
	))
	ack := backendHECIngest(
		t, ctx, server.client, server.baseURL+"/services/collector/event",
		token, channel, "application/json", body,
	)
	backendHECWaitForAcknowledgment(
		t, ctx, server.client, server.baseURL, token, channel, ack, server.process,
	)
}

func nearbyQualificationVerifyFixture(
	t *testing.T,
	ctx context.Context,
	connection clickhousedriver.Conn,
) {
	t.Helper()
	var count uint64
	var earliest, latest time.Time
	if err := connection.QueryRow(ctx, `
		SELECT count(), min(event_time), max(event_time)
		FROM open_splunk.events
		WHERE tenant_id = ? AND index_name = ? AND source = ?`,
		nearbyQualificationTenant, nearbyQualificationIndex, nearbyQualificationSource,
	).Scan(&count, &earliest, &latest); err != nil {
		t.Fatalf("read nearby qualification fixture: %v", err)
	}
	wantLatest := nearbyQualificationFixtureStart.Add(time.Duration(nearbyQualificationRows-1) * time.Microsecond)
	if count != nearbyQualificationRows || !earliest.Equal(nearbyQualificationFixtureStart) || !latest.Equal(wantLatest) {
		t.Fatalf(
			"nearby qualification fixture = count %d range [%s,%s], want %d [%s,%s]",
			count, earliest.Format(time.RFC3339Nano), latest.Format(time.RFC3339Nano),
			nearbyQualificationRows, nearbyQualificationFixtureStart.Format(time.RFC3339Nano),
			wantLatest.Format(time.RFC3339Nano),
		)
	}
}

func nearbyQualificationPrepareAdmission(
	serverName string,
	lane *nearbyQualificationLane,
	search nearbyQualificationSearch,
	now func() time.Time,
) (*opensplunk.CreateSearchJobRequest, time.Time) {
	earliest := nearbyQualificationFixtureStart.Format(time.RFC3339Nano)
	latest := nearbyQualificationFixtureStart.Add(time.Duration(nearbyQualificationRows) * time.Microsecond).Format(time.RFC3339Nano)
	timezone := "UTC"
	request := &opensplunk.CreateSearchJobRequest{Definition: &opensplunk.SearchDefinition{
		Spl:        search.spl,
		TimeRange:  &opensplunk.TimeRangeSpec{Earliest: &earliest, Latest: &latest, Timezone: &timezone},
		IndexScope: []string{nearbyQualificationIndex},
	}}
	request.ClientRequestId = lane.requestKey(serverName)
	// Key generation and request construction never contribute to measured latency.
	return request, now()
}

func nearbyQualificationFreshAdmission(response *opensplunk.CreateSearchJobResponse) (string, error) {
	if response.GetReplayed() {
		return "", fmt.Errorf("fresh qualification admission unexpectedly replayed a receipt")
	}
	jobID := response.GetSearchJob().GetSearchJobId()
	if jobID == "" {
		return "", fmt.Errorf("fresh qualification admission has no job ID")
	}
	return jobID, nil
}

func nearbyQualificationRunSearch(
	t *testing.T,
	ctx context.Context,
	server *nearbyQualificationServer,
	lane *nearbyQualificationLane,
	search nearbyQualificationSearch,
) nearbyQualificationObservation {
	t.Helper()
	request, started := nearbyQualificationPrepareAdmission(server.name, lane, search, time.Now)
	var created opensplunk.CreateSearchJobResponse
	if _, err := postProtoRequest(
		ctx, server.client, server.baseURL+"/api/search/jobs/create", request, &created,
	); err != nil {
		t.Fatalf("%s create %s: %v", server.name, search.name, err)
	}
	jobID, err := nearbyQualificationFreshAdmission(&created)
	if err != nil {
		t.Fatalf("%s create %s: %v", server.name, search.name, err)
	}
	job := nearbyQualificationWaitSearch(t, ctx, server, jobID, search.name)
	results := nearbyQualificationFetchResults(t, ctx, server, jobID, search)
	duration := time.Since(started)
	// Canonicalization is qualification work, outside the measured search path.
	digest, err := nearbyQualificationResultDigest(
		results.schema, results.rows, results.total, results.totalExact, job.GetResultsTruncated(),
	)
	if err != nil {
		t.Fatalf("%s digest %s: %v", server.name, search.name, err)
	}
	rows := uint64(len(results.rows))
	if rows != search.expectedRows || job.GetResultsTruncated() != search.truncated {
		t.Fatalf(
			"%s %s = rows %d truncated %t, want %d/%t",
			server.name, search.name, rows, job.GetResultsTruncated(), search.expectedRows, search.truncated,
		)
	}
	return nearbyQualificationObservation{
		clientRequestID: request.GetClientRequestId(),
		duration:        duration, digest: digest, rows: rows, truncated: job.GetResultsTruncated(),
	}
}

func nearbyQualificationWaitSearch(
	t *testing.T,
	ctx context.Context,
	server *nearbyQualificationServer,
	jobID, name string,
) *opensplunk.SearchJob {
	t.Helper()
	job, err := nearbyQualificationWaitTerminal(ctx, server, jobID, false)
	if err != nil {
		t.Fatalf("%s wait for %s: %v", server.name, name, err)
	}
	if job.GetState() != opensplunk.SearchJobState_SEARCH_JOB_STATE_COMPLETED {
		t.Fatalf("%s %s terminated in %s: %+v", server.name, name, job.GetState(), job.GetFailure())
	}
	return job
}

func nearbyQualificationWaitTerminal(
	ctx context.Context,
	server *nearbyQualificationServer,
	jobID string,
	retained bool,
) (*opensplunk.SearchJob, error) {
	ctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		job, err := nearbyQualificationReadJob(ctx, server, jobID, retained)
		if err != nil {
			return nil, err
		}
		if job.GetSearchJobId() != jobID {
			return nil, fmt.Errorf("polled job identity = %q, want %q", job.GetSearchJobId(), jobID)
		}
		switch job.GetState() {
		case opensplunk.SearchJobState_SEARCH_JOB_STATE_COMPLETED,
			opensplunk.SearchJobState_SEARCH_JOB_STATE_FAILED,
			opensplunk.SearchJobState_SEARCH_JOB_STATE_CANCELED,
			opensplunk.SearchJobState_SEARCH_JOB_STATE_EXPIRED:
			return job, nil
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-ticker.C:
		}
	}
}

func nearbyQualificationReadJob(ctx context.Context, server *nearbyQualificationServer, jobID string, retained bool) (*opensplunk.SearchJob, error) {
	if retained {
		// Settings reads the durable record directly; ordinary get overlays live
		// manager state and can expose FAILED before journal finalization.
		var response opensplunk.GetSearchJobSettingsResponse
		_, err := postProtoRequest(ctx, server.client, server.baseURL+"/api/search/jobs/settings/get",
			&opensplunk.GetSearchJobSettingsRequest{SearchJobId: jobID}, &response)
		return response.GetSearchJob(), err
	}
	var response opensplunk.GetSearchJobResponse
	_, err := postProtoRequest(ctx, server.client, server.baseURL+"/api/search/jobs/get",
		&opensplunk.GetSearchJobRequest{SearchJobId: jobID}, &response)
	return response.GetSearchJob(), err
}

type nearbyQualificationResults struct {
	schema     *opensplunk.ResultSchema
	rows       []*opensplunk.ResultRow
	total      uint64
	totalExact bool
}

func nearbyQualificationFetchResults(
	t *testing.T,
	ctx context.Context,
	server *nearbyQualificationServer,
	jobID string,
	search nearbyQualificationSearch,
) nearbyQualificationResults {
	t.Helper()
	const pageSize = uint32(1_000)
	var (
		schema      *opensplunk.ResultSchema
		rows        []*opensplunk.ResultRow
		nextToken   string
		total       uint64
		totalExact  bool
		snapshotRef string
		seenTokens  = make(map[string]struct{})
	)
	for pageNumber := 1; ; pageNumber++ {
		requestPage := &opensplunk.PageRequest{PageSize: new(pageSize), IncludeTotalSize: true}
		if nextToken != "" {
			requestPage.PageToken = new(nextToken)
		}
		var response opensplunk.GetSearchResultsResponse
		if _, err := postProtoRequest(
			ctx, server.client, server.baseURL+"/api/search/jobs/results",
			&opensplunk.GetSearchResultsRequest{SearchJobId: jobID, Page: requestPage}, &response,
		); err != nil {
			t.Fatalf("%s fetch %s page %d: %v", server.name, search.name, pageNumber, err)
		}
		page := response.GetResultPage()
		if response.GetSearchJobId() != jobID || !nearbyQualificationPageMetadata(page, search.truncated) {
			t.Fatalf("%s %s page %d metadata = %+v", server.name, search.name, pageNumber, page)
		}
		if server.expectsRef {
			if page.GetSnapshotRef() == "" || snapshotRef != "" && page.GetSnapshotRef() != snapshotRef {
				t.Fatalf("%s %s page %d has an invalid snapshot ref", server.name, search.name, pageNumber)
			}
			snapshotRef = page.GetSnapshotRef()
		} else if page.GetSnapshotRef() != "" {
			t.Fatalf("%s baseline unexpectedly returned candidate snapshot metadata", search.name)
		}
		if schema == nil {
			schema = page.GetSchema()
			total = page.GetPage().GetTotalSize()
			totalExact = page.GetPage().GetTotalSizeExact()
		} else if !proto.Equal(schema, page.GetSchema()) || total != page.GetPage().GetTotalSize() ||
			totalExact != page.GetPage().GetTotalSizeExact() {
			t.Fatalf("%s %s result metadata changed on page %d", server.name, search.name, pageNumber)
		}
		for _, row := range page.GetRows() {
			if row.GetOrdinal() != uint64(len(rows)) || row.GetRowId() != fmt.Sprintf("%s:%d", jobID, row.GetOrdinal()) {
				t.Fatalf("%s %s row %d identity = %q/%d", server.name, search.name, len(rows), row.GetRowId(), row.GetOrdinal())
			}
			rows = append(rows, row)
		}
		returnedToken := page.GetPage().GetNextPageToken()
		if returnedToken == "" {
			break
		}
		if _, duplicate := seenTokens[returnedToken]; duplicate {
			t.Fatalf("%s %s repeated page token", server.name, search.name)
		}
		seenTokens[returnedToken] = struct{}{}
		nextToken = returnedToken
		if pageNumber > 11 {
			t.Fatalf("%s %s exceeded bounded result pages", server.name, search.name)
		}
	}
	if total != uint64(len(rows)) || totalExact == search.truncated {
		t.Fatalf("%s %s total = %d/%t, rows %d", server.name, search.name, total, totalExact, len(rows))
	}
	return nearbyQualificationResults{schema: schema, rows: rows, total: total, totalExact: totalExact}
}

func nearbyQualificationPageMetadata(page *opensplunk.ResultPage, truncated bool) bool {
	return page != nil && page.GetSchema() != nil && page.GetPage() != nil &&
		page.GetPage().TotalSize != nil && page.GetSnapshotComplete() != truncated &&
		page.GetPage().GetTotalSizeExact() != truncated
}

func nearbyQualificationResultDigest(
	schema *opensplunk.ResultSchema,
	rows []*opensplunk.ResultRow,
	total uint64,
	totalExact, truncated bool,
) (string, error) {
	if schema == nil {
		return "", fmt.Errorf("result schema is required")
	}
	digest := sha256.New()
	options := proto.MarshalOptions{Deterministic: true}
	normalizedSchema := proto.Clone(schema).(*opensplunk.ResultSchema)
	normalizedSchema.SchemaId = "" // Job identity differs across otherwise exact executions.
	encoded, err := options.Marshal(normalizedSchema)
	if err != nil {
		return "", err
	}
	nearbyQualificationWritePart(digest, "schema", encoded)
	for _, row := range rows {
		if row == nil {
			return "", fmt.Errorf("result row is nil")
		}
		normalized := proto.Clone(row).(*opensplunk.ResultRow)
		normalized.RowId = ""
		encoded, err = options.Marshal(normalized)
		if err != nil {
			return "", err
		}
		nearbyQualificationWritePart(digest, "row", encoded)
	}
	var metadata [10]byte
	binary.BigEndian.PutUint64(metadata[:8], total)
	if totalExact {
		metadata[8] = 1
	}
	if truncated {
		metadata[9] = 1
	}
	nearbyQualificationWritePart(digest, "metadata", metadata[:])
	return hex.EncodeToString(digest.Sum(nil)), nil
}

func nearbyQualificationRequireErrorParity(
	t *testing.T,
	ctx context.Context,
	baseline, candidate *nearbyQualificationServer,
	lane *nearbyQualificationLane,
) string {
	t.Helper()
	baselineDigest, err := nearbyQualificationObserveError(ctx, baseline, lane)
	if err != nil {
		t.Fatalf("baseline malformed search: %v", err)
	}
	candidateDigest, err := nearbyQualificationObserveError(ctx, candidate, lane)
	if err != nil {
		t.Fatalf("candidate malformed search: %v", err)
	}
	if baselineDigest != candidateDigest {
		t.Fatalf("malformed search error parity differs: baseline=%s candidate=%s", baselineDigest, candidateDigest)
	}
	return baselineDigest
}

// App-less malformed SPL is admitted before asynchronous parsing. An immediate
// rejection, replay, or different terminal failure is a contract regression.
func nearbyQualificationObserveError(
	ctx context.Context,
	server *nearbyQualificationServer,
	lane *nearbyQualificationLane,
) (string, error) {
	request, _ := nearbyQualificationPrepareAdmission(server.name, lane,
		nearbyQualificationSearch{spl: `index=nearby-qualification | where (`}, time.Now)
	var created opensplunk.CreateSearchJobResponse
	if _, err := postProtoRequest(ctx, server.client, server.baseURL+"/api/search/jobs/create", request, &created); err != nil {
		return "", err
	}
	jobID, err := nearbyQualificationFreshAdmission(&created)
	if err != nil {
		return "", err
	}
	job, err := nearbyQualificationWaitTerminal(ctx, server, jobID, false)
	if err != nil {
		return "", err
	}
	if job.GetState() != opensplunk.SearchJobState_SEARCH_JOB_STATE_FAILED ||
		job.GetFailure().GetCode() != opensplunk.SearchFailureCode_SEARCH_FAILURE_CODE_INVALID_SPL {
		return "", fmt.Errorf("live malformed search state/failure = %s/%v", job.GetState(), job.GetFailure())
	}
	retained, err := nearbyQualificationWaitTerminal(ctx, server, jobID, true)
	if err != nil {
		return "", err
	}
	if retained.GetState() != opensplunk.SearchJobState_SEARCH_JOB_STATE_FAILED || !proto.Equal(job.GetFailure(), retained.GetFailure()) {
		return "", fmt.Errorf("durable failure differs from live failure: live=%v durable=%s/%v", job.GetFailure(), retained.GetState(), retained.GetFailure())
	}
	results, err := performProtoRequestWithBearer(ctx, server.client, server.baseURL+"/api/search/jobs/results", "",
		&opensplunk.GetSearchResultsRequest{SearchJobId: jobID, Page: &opensplunk.PageRequest{PageSize: new(uint32(1))}})
	if err != nil {
		return "", err
	}
	return nearbyQualificationFailureDigest(retained, results)
}

func nearbyQualificationFailureDigest(job *opensplunk.SearchJob, results protoHTTPResponse) (string, error) {
	if job.GetState() != opensplunk.SearchJobState_SEARCH_JOB_STATE_FAILED ||
		job.GetFailure().GetCode() != opensplunk.SearchFailureCode_SEARCH_FAILURE_CODE_INVALID_SPL {
		return "", fmt.Errorf("malformed search state/failure = %s/%v", job.GetState(), job.GetFailure())
	}
	if job.GetResultSchema() != nil || job.GetResultsTruncated() ||
		job.GetProgress().GetProducedRows() != 0 || job.GetProgress().GetResultBytes() != 0 ||
		job.GetRetainedResultStatus() == opensplunk.RetainedResultStatus_RETAINED_RESULT_STATUS_AVAILABLE ||
		results.statusCode != http.StatusConflict {
		return "", fmt.Errorf("malformed search exposed results: schema=%v progress=%v status=%d body=%q",
			job.GetResultSchema(), job.GetProgress(), results.statusCode, results.body)
	}
	// Bind logical failure, diagnostics and result availability, excluding only
	// per-job identity, publication versions, and wall-clock measurements.
	projection := &opensplunk.SearchJob{
		State: job.GetState(), Failure: job.GetFailure(), Diagnostics: job.GetDiagnostics(),
		Warnings: job.GetWarnings(), ResultKind: job.GetResultKind(),
		ResultSchema: job.GetResultSchema(), ResultsTruncated: job.GetResultsTruncated(),
		RetainedResultStatus: job.GetRetainedResultStatus(),
	}
	if job.GetProgress() != nil {
		projection.Progress = proto.CloneOf(job.GetProgress())
		projection.Progress.Elapsed = nil
		projection.Progress.QueueWait = nil
		projection.Progress.UpdatedAt = nil
		projection.Progress.StateVersion = 0
	}
	encoded, err := (proto.MarshalOptions{Deterministic: true}).Marshal(projection)
	if err != nil {
		return "", err
	}
	digest := sha256.New()
	nearbyQualificationWritePart(digest, "terminal-failure", encoded)
	nearbyQualificationWritePart(digest, "results-status", []byte(strconv.Itoa(results.statusCode)))
	nearbyQualificationWritePart(digest, "results-content-type", []byte(results.contentType))
	nearbyQualificationWritePart(digest, "results-body", results.body)
	return hex.EncodeToString(digest.Sum(nil)), nil
}

func nearbyQualificationRequireParity(
	t *testing.T,
	name string,
	baseline, candidate nearbyQualificationObservation,
) {
	t.Helper()
	if baseline.digest != candidate.digest || baseline.rows != candidate.rows ||
		baseline.truncated != candidate.truncated {
		t.Fatalf(
			"%s parity differs: baseline=%s/%d/%t candidate=%s/%d/%t",
			name, baseline.digest, baseline.rows, baseline.truncated,
			candidate.digest, candidate.rows, candidate.truncated,
		)
	}
}

func nearbyQualificationPercentile(samples []time.Duration, percentile int) time.Duration {
	if len(samples) == 0 || percentile < 1 || percentile > 100 {
		return 0
	}
	ordered := slices.Clone(samples)
	slices.Sort(ordered)
	index := (len(ordered)*percentile + 99) / 100
	return ordered[index-1]
}

func nearbyQualificationWithinLimit(baseline, candidate time.Duration) bool {
	return baseline > 0 && candidate > 0 &&
		candidate.Nanoseconds()*100 <= baseline.Nanoseconds()*110
}

func nearbyQualificationRatio(candidate, baseline time.Duration) float64 {
	if baseline <= 0 {
		return 0
	}
	return float64(candidate) / float64(baseline)
}

func nearbyQualificationWritePart(destination hash.Hash, label string, value []byte) {
	var length [8]byte
	binary.BigEndian.PutUint64(length[:], uint64(len(label)))
	_, _ = destination.Write(length[:])
	_, _ = destination.Write([]byte(label))
	binary.BigEndian.PutUint64(length[:], uint64(len(value)))
	_, _ = destination.Write(length[:])
	_, _ = destination.Write(value)
}

func nearbyQualificationBinary(t *testing.T, environmentName string) string {
	t.Helper()
	value := strings.TrimSpace(os.Getenv(environmentName))
	if value == "" || !filepath.IsAbs(value) {
		t.Fatalf("%s must name an absolute server binary path", environmentName)
	}
	resolved, err := filepath.EvalSymlinks(value)
	if err != nil {
		t.Fatalf("resolve %s: %v", environmentName, err)
	}
	info, err := os.Stat(resolved)
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0o111 == 0 {
		t.Fatalf("%s is not a regular executable file: %v", environmentName, err)
	}
	return resolved
}

func nearbyQualificationRequireBinaryRevision(
	t *testing.T,
	ctx context.Context,
	binary, revision string,
) {
	t.Helper()
	commandContext, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	output, err := exec.CommandContext(commandContext, binary, "version").Output()
	if err != nil {
		t.Fatalf("read server binary revision from %s: %v", binary, err)
	}
	lines := strings.Split(strings.TrimSpace(string(output)), "\n")
	want := "source_revision=" + revision
	if len(lines) == 0 || lines[0] != want {
		t.Fatalf("server binary %s identity = %q, want first line %q", binary, output, want)
	}
}

func nearbyQualificationRevision(value string) bool {
	return len(value) == 40 && strings.Trim(value, "0123456789abcdef") == ""
}

func TestNearbyQualificationIgnoresJobSchemaIdentityButBindsColumnMetadata(t *testing.T) {
	first := &opensplunk.ResultSchema{SchemaId: "baseline-job", Revision: 1, Columns: []*opensplunk.ResultColumn{{FieldName: "value", ValueType: opensplunk.ValueType_VALUE_TYPE_UINT64}}}
	second := proto.Clone(first).(*opensplunk.ResultSchema)
	second.SchemaId = "candidate-job"
	left, err := nearbyQualificationResultDigest(first, nil, 0, true, false)
	if err != nil {
		t.Fatal(err)
	}
	right, err := nearbyQualificationResultDigest(second, nil, 0, true, false)
	if err != nil || left != right {
		t.Fatalf("job-specific schema ID changed parity: %s/%s, %v", left, right, err)
	}
	second.Columns[0].Nullable = true
	right, err = nearbyQualificationResultDigest(second, nil, 0, true, false)
	if err != nil || left == right {
		t.Fatalf("column nullability was not bound: %s/%s, %v", left, right, err)
	}
	if first.SchemaId != "baseline-job" {
		t.Fatal("digest mutated source schema")
	}
}

func TestNearbyQualificationAcceptsAuthoritativeTruncatedPageMetadata(t *testing.T) {
	for _, truncated := range []bool{false, true} {
		page := &opensplunk.ResultPage{Schema: &opensplunk.ResultSchema{}, SnapshotComplete: !truncated, Page: &opensplunk.PageResponse{TotalSize: new(uint64(10)), TotalSizeExact: !truncated}}
		if !nearbyQualificationPageMetadata(page, truncated) {
			t.Fatalf("valid truncated=%t metadata rejected", truncated)
		}
		page.SnapshotComplete = truncated
		if nearbyQualificationPageMetadata(page, truncated) {
			t.Fatal("contradictory snapshot completion accepted")
		}
		page.SnapshotComplete = !truncated
		page.Page.TotalSizeExact = truncated
		if nearbyQualificationPageMetadata(page, truncated) {
			t.Fatal("contradictory total exactness accepted")
		}
	}
}

func nearbyQualificationBinaryDigest(t *testing.T, path string) string {
	t.Helper()
	file, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.New()
	_, copyErr := io.Copy(digest, file)
	closeErr := file.Close()
	if copyErr != nil || closeErr != nil {
		t.Fatalf("hash server binary: %v/%v", copyErr, closeErr)
	}
	return hex.EncodeToString(digest.Sum(nil))
}

func TestNearbyQualificationResultDigestBindsOrderAndBucketMetadata(t *testing.T) {
	schema := &opensplunk.ResultSchema{Columns: []*opensplunk.ResultColumn{{FieldName: "value", ValueType: opensplunk.ValueType_VALUE_TYPE_STRING}}}
	rows := []*opensplunk.ResultRow{
		{Ordinal: 0, Cells: []*opensplunk.TypedValue{{Kind: &opensplunk.TypedValue_StringValue{StringValue: "first"}}}},
		{Ordinal: 1, Cells: []*opensplunk.TypedValue{{Kind: &opensplunk.TypedValue_StringValue{StringValue: "second"}}}},
	}
	first, err := nearbyQualificationResultDigest(schema, rows, 2, true, false)
	if err != nil {
		t.Fatal(err)
	}
	reversed, err := nearbyQualificationResultDigest(schema, []*opensplunk.ResultRow{rows[1], rows[0]}, 2, true, false)
	if err != nil || first == reversed {
		t.Fatalf("row order was not bound: %s/%s, %v", first, reversed, err)
	}
	rows[0].TimeBucket = &opensplunk.TimeBucketBounds{Earliest: "2026-09-12T12:00:00.123456789Z", Latest: "2026-09-12T12:01:00.123456789Z"}
	bucketed, err := nearbyQualificationResultDigest(schema, rows, 2, true, false)
	if err != nil || first == bucketed {
		t.Fatalf("bucket metadata was not bound: %s/%s, %v", first, bucketed, err)
	}
}

func TestNearbyQualificationLaneKeysAreFreshAndPreparedBeforeTiming(t *testing.T) {
	t.Parallel()
	search := nearbyQualificationSearch{spl: "index=nearby-qualification"}
	unkeyed := &nearbyQualificationLane{name: "unkeyed"}
	generated := uint64(0)
	browser := &nearbyQualificationLane{name: "browser_behavior", newKey: func() string {
		generated++
		return uuid.NewString()
	}}
	clockValue := time.Date(2026, time.September, 12, 12, 0, 0, 0, time.UTC)
	for _, server := range []string{"baseline", "candidate"} {
		request, started := nearbyQualificationPrepareAdmission(server, unkeyed, search, func() time.Time { return clockValue })
		if request.ClientRequestId != nil || !started.Equal(clockValue) {
			t.Fatalf("unkeyed %s admission = %v at %s", server, request, started)
		}
	}
	seen := make(map[string]struct{})
	for attempt := uint64(1); attempt <= nearbyQualificationPairs+4; attempt++ {
		baseline, _ := nearbyQualificationPrepareAdmission("baseline", browser, search, func() time.Time { return clockValue })
		if baseline.ClientRequestId != nil || browser.keySequence != attempt-1 {
			t.Fatal("browser baseline received a key or consumed candidate key authority")
		}
		candidate, started := nearbyQualificationPrepareAdmission("candidate", browser, search, func() time.Time {
			// The measured interval must start after fresh-key allocation completes.
			if browser.keySequence != attempt || generated != attempt {
				t.Fatal("measurement clock started before candidate key was prepared")
			}
			return clockValue
		})
		key := candidate.GetClientRequestId()
		parsedKey, parseErr := uuid.Parse(key)
		if parseErr != nil || parsedKey.Version() != 4 || parsedKey.String() != key || !started.Equal(clockValue) {
			t.Fatalf("browser candidate admission key/timing = %q/%s", key, started)
		}
		if _, duplicate := seen[key]; duplicate {
			t.Fatalf("candidate reused admission key %q", key)
		}
		seen[key] = struct{}{}
		candidate.ClientRequestId = nil
		if !proto.Equal(baseline, candidate) {
			t.Fatal("browser lane changed search intent in addition to the supported request-key difference")
		}
	}
}

func TestNearbyQualificationRejectsReplayBeforeCollectingLatency(t *testing.T) {
	t.Parallel()
	for _, response := range []*opensplunk.CreateSearchJobResponse{
		nil,
		{},
		{SearchJob: &opensplunk.SearchJob{SearchJobId: "old-job"}, Replayed: true},
	} {
		if jobID, err := nearbyQualificationFreshAdmission(response); err == nil || jobID != "" {
			t.Fatalf("invalid/replayed admission entered measurement: %q, %v", jobID, err)
		}
	}
	response := &opensplunk.CreateSearchJobResponse{SearchJob: &opensplunk.SearchJob{SearchJobId: "fresh-job"}}
	if jobID, err := nearbyQualificationFreshAdmission(response); err != nil || jobID != "fresh-job" {
		t.Fatalf("fresh admission rejected: %q, %v", jobID, err)
	}
}

func nearbyQualificationFailedJob(id string) *opensplunk.SearchJob {
	return &opensplunk.SearchJob{
		SearchJobId: id, State: opensplunk.SearchJobState_SEARCH_JOB_STATE_FAILED,
		Failure: &opensplunk.SearchFailure{
			Code:    opensplunk.SearchFailureCode_SEARCH_FAILURE_CODE_INVALID_SPL,
			Message: "expected expression", Diagnostics: []*opensplunk.Diagnostic{{Code: "PARSE_EXPRESSION"}},
		},
		Progress: &opensplunk.SearchProgress{},
	}
}

func TestNearbyQualificationFailureDigestBindsLogicalFailureAndUnavailableResults(t *testing.T) {
	t.Parallel()
	job := nearbyQualificationFailedJob("baseline-job")
	results := protoHTTPResponse{statusCode: http.StatusConflict, contentType: "application/json", body: []byte(`{"error":"search results are not ready"}`)}
	want, err := nearbyQualificationFailureDigest(job, results)
	if err != nil {
		t.Fatal(err)
	}
	candidate := proto.CloneOf(job)
	candidate.SearchJobId = "candidate-job"
	candidate.StateVersion = 900
	candidate.CreatedAt = timestamppb.Now()
	candidate.FinishedAt = timestamppb.Now()
	candidate.Progress.UpdatedAt = timestamppb.Now()
	candidate.Progress.StateVersion = 900
	if got, err := nearbyQualificationFailureDigest(candidate, results); err != nil || got != want {
		t.Fatalf("per-job identity/time affected logical error parity: %s, %v", got, err)
	}
	for name, mutate := range map[string]func(*opensplunk.SearchJob, *protoHTTPResponse){
		"failure details":      func(job *opensplunk.SearchJob, _ *protoHTTPResponse) { job.Failure.Message += " changed" },
		"failure diagnostic":   func(job *opensplunk.SearchJob, _ *protoHTTPResponse) { job.Failure.Diagnostics[0].Code += "_CHANGED" },
		"failure retryability": func(job *opensplunk.SearchJob, _ *protoHTTPResponse) { job.Failure.Retryable = true },
		"wrong failure code": func(job *opensplunk.SearchJob, _ *protoHTTPResponse) {
			job.Failure.Code = opensplunk.SearchFailureCode_SEARCH_FAILURE_CODE_INTERNAL
		},
		"completed": func(job *opensplunk.SearchJob, _ *protoHTTPResponse) {
			job.State = opensplunk.SearchJobState_SEARCH_JOB_STATE_COMPLETED
		},
		"result schema": func(job *opensplunk.SearchJob, _ *protoHTTPResponse) { job.ResultSchema = &opensplunk.ResultSchema{} },
		"result rows":   func(job *opensplunk.SearchJob, _ *protoHTTPResponse) { job.Progress.ProducedRows = 1 },
		"truncation":    func(job *opensplunk.SearchJob, _ *protoHTTPResponse) { job.ResultsTruncated = true },
		"availability": func(job *opensplunk.SearchJob, _ *protoHTTPResponse) {
			job.RetainedResultStatus = opensplunk.RetainedResultStatus_RETAINED_RESULT_STATUS_AVAILABLE
		},
		"result status": func(_ *opensplunk.SearchJob, response *protoHTTPResponse) { response.statusCode = http.StatusOK },
		"result body": func(_ *opensplunk.SearchJob, response *protoHTTPResponse) {
			response.body = []byte(`{"error":"changed"}`)
		},
		"result content type": func(_ *opensplunk.SearchJob, response *protoHTTPResponse) { response.contentType = "text/plain" },
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			changed, response := proto.CloneOf(job), results
			mutate(changed, &response)
			if got, err := nearbyQualificationFailureDigest(changed, response); err == nil && got == want {
				t.Fatal("changed failure/result contract retained the baseline digest")
			}
		})
	}
}

func TestNearbyQualificationAsyncErrorRequiresFreshAdmissionAndTerminalFailure(t *testing.T) {
	t.Parallel()
	var baselineDigest string
	for _, serverName := range []string{"baseline", "candidate"} {
		lane := &nearbyQualificationLane{name: "browser_behavior", newKey: uuid.NewString}
		server, calls := nearbyQualificationErrorFixture(t, serverName, http.StatusOK, false, false)
		digest, err := nearbyQualificationObserveError(t.Context(), server, lane)
		if err != nil {
			t.Fatalf("%s asynchronous error contract rejected: %v", serverName, err)
		}
		wantCalls := []string{"/api/search/jobs/create", "/api/search/jobs/get", "/api/search/jobs/get",
			"/api/search/jobs/settings/get", "/api/search/jobs/settings/get", "/api/search/jobs/results"}
		if !slices.Equal(*calls, wantCalls) {
			t.Fatalf("%s observation paths = %v", serverName, *calls)
		}
		if baselineDigest != "" && baselineDigest != digest {
			t.Fatal("matching asynchronous failures differed after excluding per-job identity")
		}
		baselineDigest = digest
	}
	for name, fixture := range map[string]struct {
		status             int
		replayed, wrongJob bool
	}{
		"immediate rejection": {status: http.StatusBadRequest},
		"receipt replay":      {status: http.StatusOK, replayed: true},
		"wrong polled job":    {status: http.StatusOK, wrongJob: true},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			server, calls := nearbyQualificationErrorFixture(t, "candidate", fixture.status, fixture.replayed, fixture.wrongJob)
			if _, err := nearbyQualificationObserveError(t.Context(), server,
				&nearbyQualificationLane{name: "browser_behavior", newKey: uuid.NewString}); err == nil {
				t.Fatal("different admission/identity contract was accepted as an asynchronous parse failure")
			}
			if slices.Contains(*calls, "/api/search/jobs/results") {
				t.Fatal("invalid admission/identity reached result parity collection")
			}
		})
	}
}

func nearbyQualificationErrorFixture(
	t *testing.T,
	name string,
	createStatus int,
	replayed, wrongJob bool,
) (*nearbyQualificationServer, *[]string) {
	t.Helper()
	calls := []string{}
	jobID := name + "-job"
	polls, durablePolls := 0, 0
	client := &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		calls = append(calls, request.URL.Path)
		status, contentType := http.StatusOK, "application/x-protobuf"
		var message proto.Message
		var body []byte
		switch request.URL.Path {
		case "/api/search/jobs/create":
			payload, err := io.ReadAll(request.Body)
			if err != nil {
				return nil, err
			}
			var input opensplunk.CreateSearchJobRequest
			if err := proto.Unmarshal(payload, &input); err != nil {
				return nil, err
			}
			if input.GetDefinition().GetAppId() != "" || input.GetDefinition().GetSpl() != `index=nearby-qualification | where (` ||
				(name == "candidate") != (input.ClientRequestId != nil) {
				return nil, fmt.Errorf("unexpected malformed search admission: %v", &input)
			}
			status = createStatus
			message = &opensplunk.CreateSearchJobResponse{SearchJob: &opensplunk.SearchJob{SearchJobId: jobID}, Replayed: replayed}
		case "/api/search/jobs/get":
			polls++
			job := nearbyQualificationFailedJob(jobID)
			if polls == 1 {
				job.State = opensplunk.SearchJobState_SEARCH_JOB_STATE_RUNNING
				job.Failure = nil
			}
			if wrongJob {
				job.SearchJobId = "different-job"
			}
			message = &opensplunk.GetSearchJobResponse{SearchJob: job}
		case "/api/search/jobs/settings/get":
			durablePolls++
			job := nearbyQualificationFailedJob(jobID)
			if durablePolls == 1 {
				job.State = opensplunk.SearchJobState_SEARCH_JOB_STATE_RUNNING
				job.Failure = nil
			}
			message = &opensplunk.GetSearchJobSettingsResponse{SearchJob: job}
		case "/api/search/jobs/results":
			if durablePolls < 2 {
				return nil, fmt.Errorf("results read before durable failure publication")
			}
			status, contentType = http.StatusConflict, "application/json"
			body = []byte(`{"error":"search results are not ready"}`)
		default:
			return nil, fmt.Errorf("unexpected qualification path %q", request.URL.Path)
		}
		if message != nil {
			var err error
			body, err = proto.Marshal(message)
			if err != nil {
				return nil, err
			}
		}
		return &http.Response{StatusCode: status, Header: http.Header{"Content-Type": []string{contentType}},
			Body: io.NopCloser(bytes.NewReader(body)), Request: request}, nil
	})}
	return &nearbyQualificationServer{name: name, baseURL: "http://qualification.invalid", client: client}, &calls
}

func TestNearbyQualificationDurableFailureBarrierRejectsChangesAndCancellation(t *testing.T) {
	t.Parallel()
	for _, scenario := range []string{"changed failure", "completed", "cancellation"} {
		t.Run(scenario, func(t *testing.T) {
			t.Parallel()
			server, calls := nearbyQualificationErrorFixture(t, "candidate", http.StatusOK, false, false)
			transport := server.client.Transport
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			server.client.Transport = roundTripFunc(func(request *http.Request) (*http.Response, error) {
				response, err := transport.RoundTrip(request)
				if err != nil || request.URL.Path != "/api/search/jobs/settings/get" {
					return response, err
				}
				if scenario == "cancellation" {
					cancel()
					return response, nil
				}
				if err := response.Body.Close(); err != nil {
					return nil, err
				}
				job := nearbyQualificationFailedJob("candidate-job")
				if scenario == "changed failure" {
					job.Failure.Message += " changed after publication"
				} else {
					job.State = opensplunk.SearchJobState_SEARCH_JOB_STATE_COMPLETED
				}
				body, err := proto.Marshal(&opensplunk.GetSearchJobSettingsResponse{SearchJob: job})
				response.Body = io.NopCloser(bytes.NewReader(body))
				return response, err
			})
			if _, err := nearbyQualificationObserveError(ctx, server,
				&nearbyQualificationLane{name: "browser_behavior", newKey: uuid.NewString}); err == nil {
				t.Fatal("changed or canceled durable publication was accepted")
			}
			if slices.Contains(*calls, "/api/search/jobs/results") {
				t.Fatal("unsettled or inconsistent durable publication reached results")
			}
		})
	}
}
