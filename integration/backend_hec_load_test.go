//go:build !windows

package integration_test

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	clickhousedriver "github.com/ClickHouse/clickhouse-go/v2"
	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/testsupport"
)

const (
	backendHECLoadFlag              = "OPEN_SPLUNK_HEC_LOAD"
	backendHECLoadDurationEnv       = "OPEN_SPLUNK_HEC_LOAD_DURATION"
	backendHECLoadEventRateEnv      = "OPEN_SPLUNK_HEC_LOAD_EVENTS_PER_SECOND"
	backendHECLoadOutageAfterEnv    = "OPEN_SPLUNK_HEC_LOAD_OUTAGE_AFTER"
	backendHECLoadOutageDurationEnv = "OPEN_SPLUNK_HEC_LOAD_OUTAGE_DURATION"
	backendHECLoadProfileEnv        = "OPEN_SPLUNK_HEC_LOAD_PROFILE"

	backendHECLoadSmallChannel = "123e4567-e89b-42d3-a456-426614174011"
	backendHECLoadFullChannel  = "123e4567-e89b-42d3-a456-426614174012"
	backendHECLoadSmallCanary  = "hec-load-small-event-canary"
	backendHECLoadFullCanary   = "hec-load-full-event-canary"

	backendHECLoadWorkers              = 16
	backendHECLoadJobCapacity          = 128
	backendHECLoadFullEvents           = 1_000
	backendHECLoadMaximumPending       = 64
	backendHECLoadMaximumPendingBytes  = 256 << 20
	backendHECLoadMaximumRuntimeHeapMB = 512
	backendHECLoadMaximumResidentMB    = 768
	backendHECLoadMaximumGoroutines    = 512
	backendHECLoadMaximumThreads       = 128
	backendHECLoadMaximumPlannedRows   = 96_000
	backendHECLoadMaximumScheduleLag   = 2 * time.Second
	backendHECLoadSoakSmallInterval    = time.Second
)

type backendHECLoadProfile string

const (
	backendHECLoadProfileMixed     backendHECLoadProfile = "mixed"
	backendHECLoadProfileSmallOnly backendHECLoadProfile = "small-only"
	backendHECLoadProfileBatchOnly backendHECLoadProfile = "batch-only"
	backendHECLoadProfileSoak      backendHECLoadProfile = "soak"
)

type backendHECLoadPlan struct {
	Duration        time.Duration
	EventsPerSecond uint64
	OutageAfter     time.Duration
	OutageDuration  time.Duration
	NativeRate      uint64
	Profile         backendHECLoadProfile
}

func defaultBackendHECLoadPlan() backendHECLoadPlan {
	return backendHECLoadPlan{
		Duration:        30 * time.Second,
		EventsPerSecond: 1_000,
		OutageAfter:     10 * time.Second,
		OutageDuration:  5 * time.Second,
		NativeRate:      100,
		Profile:         backendHECLoadProfileMixed,
	}
}

func backendHECLoadPlanFromEnvironment() (backendHECLoadPlan, error) {
	plan := defaultBackendHECLoadPlan()
	var err error
	if plan.Duration, err = backendHECLoadDurationValue(
		backendHECLoadDurationEnv,
		plan.Duration,
	); err != nil {
		return backendHECLoadPlan{}, err
	}
	if plan.OutageAfter, err = backendHECLoadDurationValue(
		backendHECLoadOutageAfterEnv,
		plan.OutageAfter,
	); err != nil {
		return backendHECLoadPlan{}, err
	}
	if plan.OutageDuration, err = backendHECLoadDurationValue(
		backendHECLoadOutageDurationEnv,
		plan.OutageDuration,
	); err != nil {
		return backendHECLoadPlan{}, err
	}
	eventRateConfigured := false
	if value := os.Getenv(backendHECLoadEventRateEnv); value != "" {
		eventRateConfigured = true
		plan.EventsPerSecond, err = strconv.ParseUint(value, 10, 64)
		if err != nil {
			return backendHECLoadPlan{}, fmt.Errorf("parse %s: %w", backendHECLoadEventRateEnv, err)
		}
	}
	if value := os.Getenv(backendHECLoadProfileEnv); value != "" {
		plan.Profile = backendHECLoadProfile(value)
	}
	if plan.longRunning() && plan.Profile == backendHECLoadProfileSoak && !eventRateConfigured {
		// The 1,000 event/s target belongs to the short throughput gate. Keep the
		// default 24-hour lifecycle soak's final exact-uniqueness query bounded.
		plan.EventsPerSecond = 10
	}
	if plan.longRunning() {
		plan.NativeRate = 1
	} else {
		plan.NativeRate = min(uint64(100), max(uint64(1), plan.EventsPerSecond/10))
	}
	if err := plan.validate(); err != nil {
		return backendHECLoadPlan{}, err
	}
	return plan, nil
}

func backendHECLoadDurationValue(name string, fallback time.Duration) (time.Duration, error) {
	value := os.Getenv(name)
	if value == "" {
		return fallback, nil
	}
	parsed, err := time.ParseDuration(value)
	if err != nil {
		return 0, fmt.Errorf("parse %s: %w", name, err)
	}
	return parsed, nil
}

func (plan backendHECLoadPlan) validate() error {
	if plan.Duration < 6*time.Second || plan.Duration > 24*time.Hour {
		return errors.New("HEC load duration must be from 6s through 24h")
	}
	if plan.EventsPerSecond < 2 || plan.EventsPerSecond > 5_000 {
		return errors.New("HEC load event rate must be from 2 through 5,000 events/second")
	}
	if plan.OutageAfter < time.Second || plan.OutageDuration < time.Second ||
		plan.OutageAfter+plan.OutageDuration+time.Second > plan.Duration {
		return errors.New("HEC load outage must leave at least one online second before and after it")
	}
	if plan.NativeRate == 0 || plan.NativeRate > 100 {
		return errors.New("HEC load native rate must be from 1 through 100 events/second")
	}
	if plan.Profile != backendHECLoadProfileMixed &&
		plan.Profile != backendHECLoadProfileSmallOnly &&
		plan.Profile != backendHECLoadProfileBatchOnly &&
		plan.Profile != backendHECLoadProfileSoak {
		return fmt.Errorf(
			"%s must be one of mixed, small-only, batch-only, or soak",
			backendHECLoadProfileEnv,
		)
	}
	if plan.longRunning() && plan.Profile != backendHECLoadProfileSoak {
		return fmt.Errorf(
			"HEC load durations over 30m require %s=%s so retained request rows remain bounded",
			backendHECLoadProfileEnv,
			backendHECLoadProfileSoak,
		)
	}
	for _, schedule := range plan.schedules() {
		if schedule.interval <= 0 {
			return errors.New("HEC load rate does not produce representable pacing intervals")
		}
	}
	plannedRequests := plan.plannedRequests()
	if plannedRequests == 0 || plannedRequests > backendHECLoadMaximumPlannedRows {
		return fmt.Errorf(
			"HEC load profile plans %d retained request/ACK rows; want from 1 through %d below the per-token 100,000-row cap",
			plannedRequests,
			backendHECLoadMaximumPlannedRows,
		)
	}
	return nil
}

type backendHECLoadSchedule struct {
	shape              backendHECLoadShape
	events             uint64
	interval           time.Duration
	targetEventsPerSec float64
}

func (plan backendHECLoadPlan) schedules() []backendHECLoadSchedule {
	smallRate := min(uint64(50), max(uint64(1), plan.EventsPerSecond/2))
	fullRate := plan.EventsPerSecond - smallRate
	switch plan.Profile {
	case backendHECLoadProfileSmallOnly:
		return []backendHECLoadSchedule{{
			shape:              backendHECLoadShapeSmall,
			events:             1,
			interval:           backendHECLoadInterval(1, plan.EventsPerSecond),
			targetEventsPerSec: float64(plan.EventsPerSecond),
		}}
	case backendHECLoadProfileBatchOnly:
		return []backendHECLoadSchedule{{
			shape:              backendHECLoadShapeFull,
			events:             backendHECLoadFullEvents,
			interval:           backendHECLoadInterval(backendHECLoadFullEvents, plan.EventsPerSecond),
			targetEventsPerSec: float64(plan.EventsPerSecond),
		}}
	case backendHECLoadProfileSoak:
		// One small request each second retains continuous small-shape coverage
		// while the lower-rate lifecycle-soak default stays below the fixed
		// 100,000 request/ACK rows per token. Batched traffic supplies the rest.
		fullRate = plan.EventsPerSecond - 1
		return []backendHECLoadSchedule{
			{
				shape:              backendHECLoadShapeSmall,
				events:             1,
				interval:           backendHECLoadSoakSmallInterval,
				targetEventsPerSec: 1,
			},
			{
				shape:              backendHECLoadShapeFull,
				events:             backendHECLoadFullEvents,
				interval:           backendHECLoadInterval(backendHECLoadFullEvents, fullRate),
				targetEventsPerSec: float64(fullRate),
			},
		}
	default:
		return []backendHECLoadSchedule{
			{
				shape:              backendHECLoadShapeSmall,
				events:             1,
				interval:           backendHECLoadInterval(1, smallRate),
				targetEventsPerSec: float64(smallRate),
			},
			{
				shape:              backendHECLoadShapeFull,
				events:             backendHECLoadFullEvents,
				interval:           backendHECLoadInterval(backendHECLoadFullEvents, fullRate),
				targetEventsPerSec: float64(fullRate),
			},
		}
	}
}

func (plan backendHECLoadPlan) plannedRequests() uint64 {
	var result uint64
	for _, schedule := range plan.schedules() {
		result += backendHECLoadScheduledCount(plan.Duration, schedule.interval)
	}
	return result
}

func (plan backendHECLoadPlan) longRunning() bool {
	return plan.Duration > 30*time.Minute
}

func (plan backendHECLoadPlan) controlInterval() time.Duration {
	if plan.longRunning() {
		return time.Minute
	}
	return 100 * time.Millisecond
}

func (plan backendHECLoadPlan) monitorInterval() time.Duration {
	if plan.longRunning() {
		return 30 * time.Second
	}
	return 100 * time.Millisecond
}

func (plan backendHECLoadPlan) runtimeTraceInterval() time.Duration {
	if plan.longRunning() {
		return 30 * time.Minute
	}
	return 5 * time.Second
}

func (plan backendHECLoadPlan) indexRetention() time.Duration {
	// The exact final count runs after the load and durable drain. Keep its
	// earliest rows in ClickHouse beyond that boundary instead of racing the
	// general integration fixture's 24-hour TTL.
	return max(24*time.Hour, plan.Duration+time.Hour)
}

func (plan backendHECLoadPlan) requiresGCSamples() bool {
	return !plan.longRunning()
}

func backendHECLoadGODEBUG(plan backendHECLoadPlan) string {
	parts := make([]string, 0, 3)
	if plan.requiresGCSamples() {
		parts = append(parts, "gctrace=1")
	}
	parts = append(
		parts,
		"schedtrace="+strconv.FormatInt(plan.runtimeTraceInterval().Milliseconds(), 10),
		"scheddetail=1",
	)
	return strings.Join(parts, ",")
}

func backendHECLoadScheduledCount(duration, interval time.Duration) uint64 {
	if duration <= 0 || interval <= 0 {
		return 0
	}
	return uint64((duration-1)/interval) + 1
}

func backendHECLoadInterval(events, rate uint64) time.Duration {
	return backendHECLoadRationalInterval(events, rate, 1)
}

func backendHECLoadRationalInterval(events, rateNumerator, rateDenominator uint64) time.Duration {
	if events == 0 || rateNumerator == 0 || rateDenominator == 0 ||
		events > math.MaxInt64/uint64(time.Second) {
		return 0
	}
	numerator := events * uint64(time.Second)
	if numerator > math.MaxInt64/rateDenominator {
		return 0
	}
	numerator *= rateDenominator
	value := numerator / rateNumerator
	if numerator%rateNumerator != 0 {
		value++
	}
	if value == 0 || value > math.MaxInt64 {
		return 0
	}
	return time.Duration(value)
}

func TestBackendHECLoadPlanPinsDurablePressure(t *testing.T) {
	t.Parallel()
	plan := defaultBackendHECLoadPlan()
	if err := plan.validate(); err != nil {
		t.Fatal(err)
	}
	if plan.Duration != 30*time.Second || plan.EventsPerSecond != 1_000 ||
		plan.OutageAfter != 10*time.Second || plan.OutageDuration != 5*time.Second ||
		plan.NativeRate != 100 || plan.Profile != backendHECLoadProfileMixed ||
		len(plan.schedules()) != 2 ||
		plan.schedules()[0].interval != 20*time.Millisecond ||
		plan.schedules()[0].targetEventsPerSec != 50 ||
		plan.schedules()[1].interval != 1_052_631_579*time.Nanosecond ||
		plan.schedules()[1].targetEventsPerSec != 950 ||
		plan.plannedRequests() != 1_529 {
		t.Fatalf("default HEC load plan = %+v", plan)
	}
}

func TestBackendHECLoadPlanPinsRetentionSafeSoak(t *testing.T) {
	t.Parallel()
	plan := defaultBackendHECLoadPlan()
	plan.Duration = 24 * time.Hour
	plan.EventsPerSecond = 10
	plan.NativeRate = 1
	plan.Profile = backendHECLoadProfileSoak
	if err := plan.validate(); err != nil {
		t.Fatal(err)
	}
	schedules := plan.schedules()
	if len(schedules) != 2 || schedules[0].interval != time.Second ||
		schedules[1].interval != 111_111_111_112*time.Nanosecond ||
		plan.plannedRequests() != 87_178 || plan.controlInterval() != time.Minute ||
		plan.monitorInterval() != 30*time.Second ||
		plan.runtimeTraceInterval() != 30*time.Minute || plan.requiresGCSamples() ||
		plan.indexRetention() != 25*time.Hour {
		t.Fatalf("24-hour HEC soak plan = %+v schedules=%+v", plan, schedules)
	}
}

func TestBackendHECLoadPlanRejectsUnboundedLongMixedProfile(t *testing.T) {
	t.Parallel()
	plan := defaultBackendHECLoadPlan()
	plan.Duration = 24 * time.Hour
	plan.NativeRate = 1
	if err := plan.validate(); err == nil || !strings.Contains(err.Error(), backendHECLoadProfileEnv) {
		t.Fatalf("long mixed plan validation error = %v", err)
	}
}

func TestBackendHECLoadEnvironmentDefaultsLongSoakToLifecycleRate(t *testing.T) {
	t.Setenv(backendHECLoadDurationEnv, "24h")
	t.Setenv(backendHECLoadProfileEnv, string(backendHECLoadProfileSoak))
	t.Setenv(backendHECLoadEventRateEnv, "")
	t.Setenv(backendHECLoadOutageAfterEnv, "")
	t.Setenv(backendHECLoadOutageDurationEnv, "")
	plan, err := backendHECLoadPlanFromEnvironment()
	if err != nil {
		t.Fatal(err)
	}
	if plan.EventsPerSecond != 10 || plan.NativeRate != 1 ||
		plan.plannedRequests() != 87_178 {
		t.Fatalf("environment-derived 24-hour HEC soak plan = %+v", plan)
	}
}

func TestBackendHECLoadPlanRejectsUnboundedSoakRate(t *testing.T) {
	t.Parallel()
	plan := defaultBackendHECLoadPlan()
	plan.Duration = 24 * time.Hour
	plan.NativeRate = 1
	plan.Profile = backendHECLoadProfileSoak
	if err := plan.validate(); err == nil || !strings.Contains(err.Error(), "100,000-row cap") {
		t.Fatalf("unbounded 24-hour HEC soak validation error = %v", err)
	}
}

func TestBackendHECLoadWarmAcceptanceUsesCompletionTime(t *testing.T) {
	t.Parallel()
	outageStart := time.Unix(1_700_000_000, 0)
	accumulator := &backendHECLoadAccumulator{outageStart: outageStart}
	job := backendHECLoadJob{
		shape:       backendHECLoadShapeSmall,
		events:      1,
		scheduledAt: outageStart.Add(-time.Second),
	}
	if err := accumulator.record(job, backendHECLoadHTTPResponse{
		status:         http.StatusOK,
		code:           0,
		acknowledgment: 1,
		completedAt:    outageStart.Add(time.Nanosecond),
	}); err != nil {
		t.Fatal(err)
	}
	if accumulator.result.small.warmAcceptedEvents != 0 {
		t.Fatalf("late completion credited as warm: %+v", accumulator.result.small)
	}
	if err := accumulator.record(job, backendHECLoadHTTPResponse{
		status:         http.StatusOK,
		code:           0,
		acknowledgment: 2,
		completedAt:    outageStart.Add(-time.Nanosecond),
	}); err != nil {
		t.Fatal(err)
	}
	if accumulator.result.small.warmAcceptedRequests != 1 ||
		accumulator.result.small.warmAcceptedEvents != 1 {
		t.Fatalf("warm completion accounting = %+v", accumulator.result.small)
	}
}

func TestBackendHECLoadSoakRateRequiresSustainedTailAndPacing(t *testing.T) {
	t.Parallel()
	plan := defaultBackendHECLoadPlan()
	plan.Duration = 24 * time.Hour
	plan.EventsPerSecond = 10
	plan.NativeRate = 1
	plan.Profile = backendHECLoadProfileSoak
	started := time.Unix(1_700_000_000, 0)
	load := backendHECLoadResult{
		startedAt: started,
		small: backendHECLoadShapeResult{
			scheduledEvents:      86_400,
			acceptedRequests:     10,
			acceptedEvents:       10,
			warmAcceptedRequests: 10,
			warmAcceptedEvents:   10,
			lastAcceptedAt:       started.Add(10 * time.Second),
			maximumScheduleLag:   3 * time.Second,
		},
		full: backendHECLoadShapeResult{
			scheduledEvents:      778_000,
			acceptedRequests:     1,
			acceptedEvents:       1_000,
			warmAcceptedRequests: 1,
			warmAcceptedEvents:   1_000,
			lastAcceptedAt:       started,
			maximumScheduleLag:   time.Millisecond,
		},
	}
	failures := strings.Join(backendHECLoadRateFailures(plan, load), "\n")
	for _, want := range []string{
		"small maximum schedule lag",
		"small sustained accepted events",
		"full sustained accepted events",
		"small had no accepted completion",
		"full had no accepted completion",
	} {
		if !strings.Contains(failures, want) {
			t.Fatalf("soak rate failures did not contain %q:\n%s", want, failures)
		}
	}

	load.small.acceptedRequests = 86_400
	load.small.acceptedEvents = 86_400
	load.small.lastAcceptedAt = started.Add(plan.Duration - time.Second)
	load.small.maximumScheduleLag = time.Millisecond
	load.full.acceptedRequests = 778
	load.full.acceptedEvents = 778_000
	load.full.lastAcceptedAt = started.Add(plan.Duration - time.Minute)
	if failures := backendHECLoadRateFailures(plan, load); len(failures) != 0 {
		t.Fatalf("healthy soak rate failures = %v", failures)
	}
}

// TestBackendHECDurableLoad runs the shipped server and collector against the
// pinned ClickHouse image. It combines paced one-event and 1,000-event HTTPS
// HEC traffic, native collector traffic, administrator writes, an actual
// ClickHouse pause/unpause, bounded operational snapshots, and product-row
// convergence. It is opt-in because it builds binaries and owns Docker state.
func TestBackendHECDurableLoad(t *testing.T) {
	if os.Getenv(backendHECLoadFlag) != "1" {
		t.Skip("set " + backendHECLoadFlag + "=1 to run the durable HEC load integration test")
	}
	if _, err := exec.LookPath("docker"); err != nil {
		t.Fatalf("docker CLI is required when %s=1: %v", backendHECLoadFlag, err)
	}
	plan, err := backendHECLoadPlanFromEnvironment()
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), plan.Duration+8*time.Minute)
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
	collectorBinary := filepath.Join(buildDir, "open-splunk-collector")
	loggenBinary := filepath.Join(buildDir, "open-splunk-loggen")
	buildBinary(t, ctx, stagedBackendRepository, serverBinary, "./cmd/open-splunk-server")
	buildBinary(t, ctx, repository, collectorBinary, "./cmd/open-splunk-collector")
	buildBinary(t, ctx, repository, loggenBinary, "./cmd/open-splunk-loggen")

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
	serverEnvironment = environmentWithValue(
		serverEnvironment,
		"GODEBUG",
		backendHECLoadGODEBUG(plan),
	)
	serverEnvironment = environmentWithValue(
		serverEnvironment,
		"GOMEMLIMIT",
		strconv.FormatUint(backendHECLoadMaximumRuntimeHeapMB, 10)+"MiB",
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
	baseURL := "https://" + httpAddress
	httpTransport := http.DefaultTransport.(*http.Transport).Clone()
	httpTransport.ForceAttemptHTTP2 = true
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
	// Keep the earliest load rows outside ClickHouse's TTL horizon through the
	// final drain and exact-cardinality query. The soak can run for a full day,
	// so the general 24-hour integration fixture retention is not sufficient.
	createBackendIndexWithRetention(
		t,
		ctx,
		httpClient,
		baseURL,
		administratorToken,
		backendHECIndexName,
		"Durable HEC load integration",
		plan.indexRetention(),
	)
	hecToken, hecMetadata := backendHECCreateToken(
		t,
		ctx,
		httpClient,
		baseURL,
		administratorToken,
	)
	pressureSecret, pressureToken := backendHECLoadCreatePressureToken(
		t,
		ctx,
		httpClient,
		baseURL,
		administratorToken,
	)

	collectorStateDir := filepath.Join(work, "collector-state")
	logPath := filepath.Join(work, "backend-hec-load-native.ndjson")
	createEmptyFixture(t, logPath)
	nativeTokenPath := filepath.Join(work, "collector.token")
	collectorConfig := filepath.Join(work, "collector.yaml")
	writePrivateFile(t, collectorConfig, []byte(backendLoadCollectorYAML(
		collectorAddress,
		nativeTokenPath,
		collectorStateDir,
		logPath,
		backendHECIndexName,
	)))
	collectorEnvironment := os.Environ()
	collectorID := initializeCollectorIdentity(
		t,
		ctx,
		repository,
		collectorBinary,
		collectorConfig,
		collectorEnvironment,
		collectorStateDir,
	)
	nativeToken := createBackendLoadToken(
		t,
		ctx,
		httpClient,
		baseURL,
		administratorToken,
		backendHECIndexName,
		collectorID,
	)
	writePrivateFile(t, nativeTokenPath, []byte(nativeToken+"\n"))
	validateBackendLoadCollectorConfiguration(
		t,
		ctx,
		repository,
		collectorBinary,
		collectorConfig,
		collectorEnvironment,
		nativeToken,
	)
	collectorProcess := startProcess(
		t,
		repository,
		[]string{collectorBinary, "run", "-config", collectorConfig, "-log-level", "debug"},
		collectorEnvironment,
	)
	waitForCollectorDiscovery(
		t,
		ctx,
		collectorStateDir,
		logPath,
		collectorProcess,
		nativeToken,
	)

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
		t.Fatalf("ping ClickHouse HEC load inspection connection: %v", err)
	}

	protectedValues := []string{
		administratorToken,
		hecToken,
		hecMetadata.GetTokenPrefix(),
		pressureSecret,
		pressureToken.GetTokenPrefix(),
		nativeToken,
		clickHouse.MigrationPassword,
		clickHouse.RuntimePassword,
		clickHouse.DeletionPassword,
		backendHECLoadSmallChannel,
		backendHECLoadFullChannel,
		backendHECLoadSmallCanary,
		backendHECLoadFullCanary,
	}

	nativeCount := max(uint64(2), uint64(plan.Duration.Seconds()*float64(plan.NativeRate)))
	nativePlan := backendLoadPlan{
		TenantID:    backendHECTenantID,
		IndexName:   backendHECIndexName,
		Cardinality: min(nativeCount, uint64(10_000)),
		Rate:        plan.NativeRate,
		FlushEvents: min(nativeCount, uint64(25)),
	}
	nativeGenerator := startProcess(
		t,
		repository,
		backendLoadLoggenArguments(
			loggenBinary,
			logPath,
			nativePlan,
			nativeCount,
			backendLoadMainSeed+101,
			time.Now().UTC().Add(-5*time.Minute).Truncate(time.Millisecond),
		),
		os.Environ(),
	)

	pressureContext, stopPressure := context.WithCancel(ctx)
	pressureDone := make(chan backendHECLoadControlResult, 1)
	go func() {
		pressureDone <- runBackendHECLoadControlPressure(
			pressureContext,
			httpClient,
			baseURL,
			administratorToken,
			pressureToken.GetIngestionTokenId(),
			pressureToken.GetVersion(),
			plan.controlInterval(),
		)
	}()
	monitorContext, stopMonitor := context.WithCancel(ctx)
	monitorDone := make(chan backendHECLoadOperationalObservation, 1)
	go func() {
		monitorDone <- monitorBackendHECLoadOperations(
			monitorContext,
			httpClient,
			baseURL,
			administratorToken,
			serverProcess,
			plan.monitorInterval(),
			plan.longRunning(),
		)
	}()
	loadStarted := time.Now()
	loadDone := make(chan backendHECLoadResult, 1)
	go func() {
		loadDone <- runBackendHECLoadTraffic(
			ctx,
			httpClient,
			baseURL,
			hecToken,
			loadStarted,
			plan,
		)
	}()

	if err := waitUntilBackendHECLoad(ctx, loadStarted.Add(plan.OutageAfter)); err != nil {
		t.Fatal(err)
	}
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
			t.Errorf("unpause ClickHouse during HEC load cleanup: %v", err)
		}
	})
	if err := waitUntilBackendHECLoad(ctx, loadStarted.Add(plan.OutageAfter+plan.OutageDuration)); err != nil {
		t.Fatal(err)
	}
	outageSnapshot, err := readBackendHECLoadOperations(
		ctx,
		httpClient,
		baseURL,
		administratorToken,
	)
	if err != nil {
		t.Fatalf("read HEC operations during ClickHouse outage: %v", err)
	}
	backendHECLoadRequireBoundedBacklog(t, outageSnapshot)

	recoveryStarted := time.Now()
	backendHECDocker(t, ctx, "unpause", clickHouse.Name)
	clickHousePaused = false
	waitForBackendHECLoadClickHouse(t, ctx, storage, serverProcess, protectedValues...)

	load := <-loadDone
	if load.err != nil {
		t.Fatalf("durable HEC traffic: %v", load.err)
	}
	if err := nativeGenerator.Wait(plan.Duration + 30*time.Second); err != nil {
		t.Fatalf("native HEC-load generator: %v", err)
	}
	stopPressure()
	control := <-pressureDone
	if control.err != nil {
		t.Fatalf("HEC load control-plane pressure: %v", control.err)
	}
	if control.mutations == 0 {
		t.Fatal("HEC load control-plane pressure completed no mutations")
	}

	finalOperations := waitForBackendHECLoadDrain(
		t,
		ctx,
		httpClient,
		baseURL,
		administratorToken,
		serverProcess,
		protectedValues...,
	)
	drainedAt := time.Now()
	stopMonitor()
	operational := <-monitorDone
	if operational.err != nil && !errors.Is(operational.err, context.Canceled) {
		t.Fatalf("monitor HEC operational load state: %v", operational.err)
	}
	operational.observe(outageSnapshot)
	if operational.snapshots == 0 || operational.maximumPending == 0 ||
		operational.maximumPending > backendHECLoadMaximumPending ||
		operational.maximumPendingBytes == 0 ||
		operational.maximumPendingBytes > backendHECLoadMaximumPendingBytes ||
		operational.maximumPendingAcknowledgments > backendHECLoadMaximumPending ||
		operational.maximumRetainedRequests > 100_000 {
		t.Fatalf("HEC operational high-water observation = %+v", operational)
	}
	if plan.longRunning() && (operational.resourceSamples == 0 ||
		operational.maximumResidentMB == 0 ||
		operational.maximumResidentMB > backendHECLoadMaximumResidentMB ||
		operational.maximumProcessThreads == 0 ||
		operational.maximumProcessThreads > backendHECLoadMaximumThreads) {
		t.Fatalf("HEC process resource high-water observation = %+v", operational)
	}

	expectedRows := nativeCount + load.acceptedEvents()
	waitForBackendLoadStorage(
		t,
		ctx,
		storage,
		collectorProcess,
		backendHECTenantID,
		backendHECIndexName,
		expectedRows,
		2*time.Minute,
		protectedValues...,
	)
	waitForCollectorCheckpoint(t, ctx, collectorStateDir, logPath, collectorProcess, nativeToken)
	waitForCollectorWALAcknowledgedThroughCurrent(
		t,
		ctx,
		collectorStateDir,
		collectorProcess,
		nativeToken,
	)
	assertBackendLoadDeadLetterEmpty(t, collectorStateDir)
	for _, acknowledgment := range []struct {
		channel string
		id      int64
	}{
		{channel: backendHECLoadSmallChannel, id: load.small.lastAcknowledgment},
		{channel: backendHECLoadFullChannel, id: load.full.lastAcknowledgment},
	} {
		if acknowledgment.id == 0 {
			continue
		}
		if !backendHECQueryAcknowledgments(
			t,
			ctx,
			httpClient,
			baseURL,
			hecToken,
			acknowledgment.channel,
			acknowledgment.id,
		)[acknowledgment.id] {
			t.Fatalf("HEC load acknowledgment %d remained pending after drain", acknowledgment.id)
		}
	}
	if finalOperations.GetRequest().GetAcceptedRequests() != load.acceptedRequests() ||
		finalOperations.GetRequest().GetEvents() != load.acceptedEvents() ||
		finalOperations.GetDurable().GetPendingOutboxReservations() != 0 ||
		finalOperations.GetAcknowledgments().GetPendingRows() != 0 ||
		!finalOperations.GetDurable().GetQueueAvailable() ||
		!finalOperations.GetDurable().GetRequestCapacityAvailable() ||
		!finalOperations.GetReconciliation().GetAvailable() ||
		!finalOperations.GetAcknowledgments().GetAvailable() {
		t.Fatalf("final HEC operational snapshot = %+v", finalOperations)
	}

	if err := collectorProcess.Interrupt(20 * time.Second); err != nil {
		t.Fatalf(
			"stop HEC-load collector: %v\nlogs:\n%s",
			err,
			redactForFailure(collectorProcess.Logs(), protectedValues...),
		)
	}
	if err := serverProcess.Interrupt(20 * time.Second); err != nil {
		t.Fatalf(
			"stop HEC-load server: %v\nlogs:\n%s",
			err,
			redactForFailure(serverProcess.Logs(), protectedValues...),
		)
	}
	assertManagedProcessLogsComplete(t, "HEC-load server", serverProcess, protectedValues...)
	assertManagedProcessLogsComplete(t, "HEC-load collector", collectorProcess, protectedValues...)
	assertProcessLogsDoNotLeak(t, serverProcess.Logs(), protectedValues...)
	assertProcessLogsDoNotLeak(t, collectorProcess.Logs(), protectedValues...)
	runtimeSignals, err := parseBackendHECLoadRuntimeSignalsWithGCRequirement(
		serverProcess.Logs(),
		plan.requiresGCSamples(),
	)
	if err != nil {
		t.Fatal(err)
	}
	if runtimeSignals.maximumHeapMB > backendHECLoadMaximumRuntimeHeapMB ||
		runtimeSignals.maximumGoroutines > backendHECLoadMaximumGoroutines ||
		runtimeSignals.maximumThreads > backendHECLoadMaximumThreads {
		t.Fatalf("HEC load runtime signals exceeded bounds: %+v", runtimeSignals)
	}

	elapsed := time.Since(loadStarted)
	t.Logf(
		"durable HEC load: profile=%s configured_duration=%s elapsed=%s target_eps=%d planned_request_rows=%d small=%s full=%s native_events=%d control_mutations=%d rows=%d",
		plan.Profile,
		plan.Duration,
		elapsed.Round(time.Millisecond),
		plan.EventsPerSecond,
		plan.plannedRequests(),
		load.small.summary(plan.Duration),
		load.full.summary(plan.Duration),
		nativeCount,
		control.mutations,
		expectedRows,
	)
	t.Logf(
		"durable HEC outage/recovery: outage=%s recovery=%s max_pending=%d max_pending_bytes=%d max_pending_acks=%d max_oldest_pending=%s retries=%d successes=%d",
		plan.OutageDuration,
		drainedAt.Sub(recoveryStarted).Round(time.Millisecond),
		operational.maximumPending,
		operational.maximumPendingBytes,
		operational.maximumPendingAcknowledgments,
		operational.maximumOldestPending,
		finalOperations.GetReconciliation().GetRetries(),
		finalOperations.GetReconciliation().GetSuccesses(),
	)
	t.Logf(
		"durable HEC runtime bounds: gc_samples=%d max_heap_mb=%d scheduler_samples=%d max_goroutines=%d last_goroutines=%d max_threads=%d process_samples=%d max_resident_mb=%d max_process_threads=%d",
		runtimeSignals.gcSamples,
		runtimeSignals.maximumHeapMB,
		runtimeSignals.schedulerSamples,
		runtimeSignals.maximumGoroutines,
		runtimeSignals.lastGoroutines,
		runtimeSignals.maximumThreads,
		operational.resourceSamples,
		operational.maximumResidentMB,
		operational.maximumProcessThreads,
	)
	backendHECLoadRequireRate(t, plan, load)
}

func backendHECLoadCreatePressureToken(
	t *testing.T,
	ctx context.Context,
	client *http.Client,
	baseURL string,
	administratorToken string,
) (string, *opensplunkv1.IngestionToken) {
	t.Helper()
	defaultIndex := backendHECIndexName
	defaultHost := "hec-load-control-host"
	defaultSource := "hec-load-control-source"
	defaultSourcetype := "hec-load-control-type"
	var response opensplunkv1.CreateIngestionTokenResponse
	postAdministratorProto(
		t,
		ctx,
		client,
		baseURL+"/api/v1/ingestion-tokens/create",
		administratorToken,
		&opensplunkv1.CreateIngestionTokenRequest{Definition: &opensplunkv1.IngestionTokenDefinition{
			Name:    "HEC load control-plane pressure",
			Purpose: opensplunkv1.IngestionTokenPurpose_INGESTION_TOKEN_PURPOSE_HEC,
			Constraints: &opensplunkv1.IngestionTokenConstraints{
				AllowedIndexNames: []string{backendHECIndexName},
			},
			HecProfile: &opensplunkv1.IngestionTokenHecProfile{
				DefaultIndexName:  &defaultIndex,
				DefaultHost:       &defaultHost,
				DefaultSource:     &defaultSource,
				DefaultSourcetype: &defaultSourcetype,
			},
		}},
		&response,
	)
	secret := response.GetPlaintextToken()
	metadata := response.GetIngestionToken()
	if secret == "" || metadata.GetIngestionTokenId() == "" || metadata.GetVersion() != 1 ||
		metadata.GetPurpose() != opensplunkv1.IngestionTokenPurpose_INGESTION_TOKEN_PURPOSE_HEC {
		t.Fatalf("created HEC load pressure token metadata is invalid (secret length %d)", len(secret))
	}
	return secret, metadata
}

type backendHECLoadShape uint8

const (
	backendHECLoadShapeSmall backendHECLoadShape = iota + 1
	backendHECLoadShapeFull
)

type backendHECLoadJob struct {
	shape       backendHECLoadShape
	events      uint64
	scheduledAt time.Time
	body        []byte
	channel     string
}

type backendHECLoadShapeResult struct {
	scheduledRequests    uint64
	scheduledEvents      uint64
	warmScheduledEvents  uint64
	acceptedRequests     uint64
	acceptedEvents       uint64
	warmAcceptedRequests uint64
	warmAcceptedEvents   uint64
	busyRequests         uint64
	capacityRequests     uint64
	firstAcknowledgment  int64
	lastAcknowledgment   int64
	lastAcceptedAt       time.Time
	maximumScheduleLag   time.Duration
}

func (result backendHECLoadShapeResult) summary(duration time.Duration) string {
	return fmt.Sprintf(
		"scheduled_requests=%d scheduled_events=%d offered_eps=%.1f accepted_requests=%d accepted_rps=%.1f accepted_events=%d accepted_eps=%.1f warm_accepted_requests=%d warm_accepted_events=%d busy=%d capacity=%d max_schedule_lag=%s",
		result.scheduledRequests,
		result.scheduledEvents,
		float64(result.scheduledEvents)/duration.Seconds(),
		result.acceptedRequests,
		float64(result.acceptedRequests)/duration.Seconds(),
		result.acceptedEvents,
		float64(result.acceptedEvents)/duration.Seconds(),
		result.warmAcceptedRequests,
		result.warmAcceptedEvents,
		result.busyRequests,
		result.capacityRequests,
		result.maximumScheduleLag,
	)
}

type backendHECLoadResult struct {
	small     backendHECLoadShapeResult
	full      backendHECLoadShapeResult
	startedAt time.Time
	err       error
}

func (result backendHECLoadResult) acceptedRequests() uint64 {
	return result.small.acceptedRequests + result.full.acceptedRequests
}

func (result backendHECLoadResult) acceptedEvents() uint64 {
	return result.small.acceptedEvents + result.full.acceptedEvents
}

type backendHECLoadAccumulator struct {
	mu          sync.Mutex
	result      backendHECLoadResult
	outageStart time.Time
}

func (accumulator *backendHECLoadAccumulator) scheduled(job backendHECLoadJob) {
	accumulator.mu.Lock()
	defer accumulator.mu.Unlock()
	result := accumulator.shape(job.shape)
	result.scheduledRequests++
	result.scheduledEvents += job.events
	if job.scheduledAt.Before(accumulator.outageStart) {
		result.warmScheduledEvents += job.events
	}
	result.maximumScheduleLag = max(result.maximumScheduleLag, time.Since(job.scheduledAt))
}

func (accumulator *backendHECLoadAccumulator) record(
	job backendHECLoadJob,
	response backendHECLoadHTTPResponse,
) error {
	accumulator.mu.Lock()
	defer accumulator.mu.Unlock()
	result := accumulator.shape(job.shape)
	switch {
	case response.status == http.StatusOK && response.code == 0 && response.acknowledgment > 0:
		result.acceptedRequests++
		result.acceptedEvents += job.events
		if response.completedAt.Before(accumulator.outageStart) {
			result.warmAcceptedRequests++
			result.warmAcceptedEvents += job.events
		}
		if result.firstAcknowledgment == 0 {
			result.firstAcknowledgment = response.acknowledgment
		}
		result.lastAcknowledgment = response.acknowledgment
		result.lastAcceptedAt = response.completedAt
		return nil
	case (response.status == http.StatusServiceUnavailable ||
		response.status == http.StatusTooManyRequests) && response.code == 9:
		result.busyRequests++
		return nil
	case response.status == http.StatusTooManyRequests && response.code == 26:
		result.capacityRequests++
		return nil
	default:
		return fmt.Errorf(
			"HEC load %s response status/code/ack = %d/%d/%d",
			job.shape,
			response.status,
			response.code,
			response.acknowledgment,
		)
	}
}

func (accumulator *backendHECLoadAccumulator) shape(
	shape backendHECLoadShape,
) *backendHECLoadShapeResult {
	if shape == backendHECLoadShapeSmall {
		return &accumulator.result.small
	}
	return &accumulator.result.full
}

func (shape backendHECLoadShape) String() string {
	if shape == backendHECLoadShapeSmall {
		return "small"
	}
	return "full"
}

func runBackendHECLoadTraffic(
	ctx context.Context,
	client *http.Client,
	baseURL string,
	credential string,
	started time.Time,
	plan backendHECLoadPlan,
) backendHECLoadResult {
	trafficContext, cancelTraffic := context.WithCancel(ctx)
	defer cancelTraffic()
	fullBody := backendHECLoadFullBody()
	jobs := make(chan backendHECLoadJob, backendHECLoadJobCapacity)
	accumulator := &backendHECLoadAccumulator{outageStart: started.Add(plan.OutageAfter)}
	accumulator.result.startedAt = started
	var firstError sync.Once
	recordError := func(err error) {
		if err == nil {
			return
		}
		firstError.Do(func() {
			accumulator.mu.Lock()
			accumulator.result.err = err
			accumulator.mu.Unlock()
			cancelTraffic()
		})
	}

	var workers sync.WaitGroup
	for range backendHECLoadWorkers {
		workers.Go(func() {
			for job := range jobs {
				response, err := backendHECLoadPost(
					trafficContext,
					client,
					baseURL+"/services/collector/event",
					credential,
					job.channel,
					job.body,
				)
				if err != nil {
					if trafficContext.Err() == nil {
						recordError(err)
					}
					return
				}
				if err := accumulator.record(job, response); err != nil {
					recordError(err)
					return
				}
			}
		})
	}

	schedules := plan.schedules()
	var schedulers sync.WaitGroup
	for _, schedule := range schedules {
		schedulers.Go(func() {
			interval := schedule.interval
			channel := backendHECLoadFullChannel
			body := fullBody
			if schedule.shape == backendHECLoadShapeSmall {
				channel = backendHECLoadSmallChannel
				body = []byte(`{"event":"` + backendHECLoadSmallCanary + `"}`)
			}
			finished := started.Add(plan.Duration)
			for ordinal := uint64(0); ; ordinal++ {
				if ordinal > math.MaxInt64/uint64(interval) {
					recordError(errors.New("HEC load schedule ordinal overflow"))
					return
				}
				deadline := started.Add(time.Duration(ordinal * uint64(interval)))
				if !deadline.Before(finished) {
					return
				}
				if err := waitUntilBackendHECLoad(trafficContext, deadline); err != nil {
					return
				}
				job := backendHECLoadJob{
					shape:       schedule.shape,
					events:      schedule.events,
					scheduledAt: deadline,
					body:        body,
					channel:     channel,
				}
				remaining := time.Until(finished)
				if remaining <= 0 {
					return
				}
				timer := time.NewTimer(remaining)
				select {
				case jobs <- job:
					if !timer.Stop() {
						select {
						case <-timer.C:
						default:
						}
					}
					accumulator.scheduled(job)
				case <-timer.C:
					return
				case <-trafficContext.Done():
					if !timer.Stop() {
						select {
						case <-timer.C:
						default:
						}
					}
					return
				}
			}
		})
	}
	schedulers.Wait()
	close(jobs)
	workers.Wait()
	accumulator.mu.Lock()
	defer accumulator.mu.Unlock()
	return accumulator.result
}

func backendHECLoadFullBody() []byte {
	result := make([]byte, 0, 96*backendHECLoadFullEvents)
	for ordinal := range backendHECLoadFullEvents {
		result = append(result, `{"event":"`...)
		result = append(result, backendHECLoadFullCanary...)
		result = append(result, `","fields":{"ordinal":`...)
		result = strconv.AppendInt(result, int64(ordinal), 10)
		result = append(result, "}}"...)
	}
	return result
}

type backendHECLoadHTTPResponse struct {
	status         int
	code           int
	acknowledgment int64
	completedAt    time.Time
}

func backendHECLoadPost(
	ctx context.Context,
	client *http.Client,
	endpoint string,
	credential string,
	channel string,
	body []byte,
) (backendHECLoadHTTPResponse, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return backendHECLoadHTTPResponse{}, err
	}
	request.Header.Set("Authorization", "Splunk "+credential)
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("X-Splunk-Request-Channel", channel)
	response, err := client.Do(request)
	if err != nil {
		return backendHECLoadHTTPResponse{}, err
	}
	defer response.Body.Close()
	wire, err := io.ReadAll(io.LimitReader(response.Body, (1<<20)+1))
	if err != nil {
		return backendHECLoadHTTPResponse{}, err
	}
	if len(wire) > 1<<20 {
		return backendHECLoadHTTPResponse{}, errors.New("HEC load response exceeded 1 MiB")
	}
	if response.ProtoMajor != 2 {
		return backendHECLoadHTTPResponse{}, fmt.Errorf("HEC load negotiated HTTP/%d", response.ProtoMajor)
	}
	if response.Header.Get("Content-Type") != "application/json; charset=utf-8" ||
		response.Header.Get("X-Content-Type-Options") != "nosniff" ||
		response.Header.Get("Cache-Control") != "no-store" {
		return backendHECLoadHTTPResponse{}, errors.New("HEC load response security headers are invalid")
	}
	var decoded backendHECResponse
	if err := json.Unmarshal(wire, &decoded); err != nil {
		return backendHECLoadHTTPResponse{}, fmt.Errorf("decode HEC load response: %w", err)
	}
	result := backendHECLoadHTTPResponse{
		status:      response.StatusCode,
		code:        decoded.Code,
		completedAt: time.Now(),
	}
	if decoded.AckID != nil {
		result.acknowledgment = *decoded.AckID
	}
	return result, nil
}

func waitUntilBackendHECLoad(ctx context.Context, deadline time.Time) error {
	delay := time.Until(deadline)
	if delay <= 0 {
		return ctx.Err()
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func backendHECLoadRequireRate(
	t *testing.T,
	plan backendHECLoadPlan,
	load backendHECLoadResult,
) {
	t.Helper()
	failures := backendHECLoadRateFailures(plan, load)
	if len(failures) > 0 {
		t.Fatalf(
			"HEC %s profile rate gate failed: %s; small={%s}; full={%s}",
			plan.Profile,
			strings.Join(failures, "; "),
			load.small.summary(plan.Duration),
			load.full.summary(plan.Duration),
		)
	}
}

func backendHECLoadRateFailures(
	plan backendHECLoadPlan,
	load backendHECLoadResult,
) []string {
	shapeResults := map[backendHECLoadShape]backendHECLoadShapeResult{
		backendHECLoadShapeSmall: load.small,
		backendHECLoadShapeFull:  load.full,
	}
	var failures []string
	var warmAccepted uint64
	var warmTarget float64
	for _, schedule := range plan.schedules() {
		result := shapeResults[schedule.shape]
		minimumScheduled := backendHECLoadMinimumEvents(
			schedule.targetEventsPerSec,
			plan.Duration,
			0.90,
		)
		if result.scheduledEvents < minimumScheduled {
			failures = append(failures, fmt.Sprintf(
				"%s scheduled events %d < %d (90%% of %.1f events/s budget)",
				schedule.shape,
				result.scheduledEvents,
				minimumScheduled,
				schedule.targetEventsPerSec,
			))
		}
		if result.maximumScheduleLag > backendHECLoadMaximumScheduleLag {
			failures = append(failures, fmt.Sprintf(
				"%s maximum schedule lag %s > %s",
				schedule.shape,
				result.maximumScheduleLag,
				backendHECLoadMaximumScheduleLag,
			))
		}
		if plan.Profile == backendHECLoadProfileSmallOnly {
			if result.acceptedRequests == 0 {
				failures = append(failures, "small-only profile accepted no requests")
			}
			continue
		}
		minimumWarmAccepted := backendHECLoadMinimumEvents(
			schedule.targetEventsPerSec,
			plan.OutageAfter,
			0.80,
		)
		if result.warmAcceptedEvents < minimumWarmAccepted {
			failures = append(failures, fmt.Sprintf(
				"%s completion-time warm accepted events %d < %d (80%% of %.1f events/s budget)",
				schedule.shape,
				result.warmAcceptedEvents,
				minimumWarmAccepted,
				schedule.targetEventsPerSec,
			))
		}
		if plan.longRunning() {
			minimumSustained := uint64(math.Ceil(float64(result.scheduledEvents) * 0.90))
			if result.acceptedEvents < minimumSustained {
				failures = append(failures, fmt.Sprintf(
					"%s sustained accepted events %d < %d (90%% of scheduled events)",
					schedule.shape,
					result.acceptedEvents,
					minimumSustained,
				))
			}
			tailTolerance := max(time.Minute, 2*schedule.interval)
			tailBoundary := load.startedAt.Add(plan.Duration - tailTolerance)
			if result.lastAcceptedAt.IsZero() || result.lastAcceptedAt.Before(tailBoundary) {
				failures = append(failures, fmt.Sprintf(
					"%s had no accepted completion within %s of the planned end",
					schedule.shape,
					tailTolerance,
				))
			}
		}
		warmAccepted += result.warmAcceptedEvents
		warmTarget += schedule.targetEventsPerSec * plan.OutageAfter.Seconds()
	}
	if plan.Profile != backendHECLoadProfileSmallOnly {
		minimumCombinedWarm := uint64(math.Ceil(warmTarget * 0.90))
		if warmAccepted < minimumCombinedWarm {
			failures = append(failures, fmt.Sprintf(
				"combined completion-time warm accepted events %d < %d (90%% of %.1f-event aggregate budget)",
				warmAccepted,
				minimumCombinedWarm,
				warmTarget,
			))
		}
	}
	return failures
}

func backendHECLoadMinimumEvents(rate float64, duration time.Duration, fraction float64) uint64 {
	return uint64(math.Ceil(rate * duration.Seconds() * fraction))
}

type backendHECLoadControlResult struct {
	mutations uint64
	version   uint64
	err       error
}

func runBackendHECLoadControlPressure(
	ctx context.Context,
	client *http.Client,
	baseURL string,
	administratorToken string,
	tokenID string,
	version uint64,
	interval time.Duration,
) backendHECLoadControlResult {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	enabled := true
	result := backendHECLoadControlResult{version: version}
	for {
		select {
		case <-ctx.Done():
			return result
		case <-ticker.C:
		}
		enabled = !enabled
		var response opensplunkv1.SetIngestionTokenEnabledResponse
		_, err := postProtoRequestWithBearer(
			ctx,
			client,
			baseURL+"/api/v1/ingestion-tokens/state/set",
			administratorToken,
			&opensplunkv1.SetIngestionTokenEnabledRequest{
				IngestionTokenId: tokenID,
				ExpectedVersion:  result.version,
				Enabled:          enabled,
			},
			&response,
		)
		if err != nil {
			if ctx.Err() != nil {
				return result
			}
			result.err = err
			return result
		}
		metadata := response.GetIngestionToken()
		wantState := opensplunkv1.IngestionTokenState_INGESTION_TOKEN_STATE_DISABLED
		if enabled {
			wantState = opensplunkv1.IngestionTokenState_INGESTION_TOKEN_STATE_ACTIVE
		}
		if metadata.GetIngestionTokenId() != tokenID ||
			metadata.GetVersion() != result.version+1 || metadata.GetState() != wantState {
			result.err = errors.New("control-plane pressure returned inconsistent token state")
			return result
		}
		result.version = metadata.GetVersion()
		result.mutations++
	}
}

type backendHECLoadOperationalObservation struct {
	snapshots                     uint64
	maximumPending                uint64
	maximumPendingBytes           uint64
	maximumPendingAcknowledgments uint64
	maximumRetainedRequests       uint64
	maximumOldestPending          time.Duration
	resourceSamples               uint64
	maximumResidentMB             uint64
	maximumProcessThreads         uint64
	err                           error
}

func monitorBackendHECLoadOperations(
	ctx context.Context,
	client *http.Client,
	baseURL string,
	administratorToken string,
	server *managedProcess,
	interval time.Duration,
	observeProcessResources bool,
) backendHECLoadOperationalObservation {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	result := backendHECLoadOperationalObservation{}
	for {
		snapshot, err := readBackendHECLoadOperations(
			ctx,
			client,
			baseURL,
			administratorToken,
		)
		if err != nil {
			if ctx.Err() != nil {
				return result
			}
			result.err = err
			return result
		}
		result.observe(snapshot)
		if observeProcessResources {
			residentMB, threads, err := readBackendHECLoadProcessResources(ctx, server)
			if err != nil {
				if ctx.Err() != nil {
					return result
				}
				result.err = err
				return result
			}
			result.resourceSamples++
			result.maximumResidentMB = max(result.maximumResidentMB, residentMB)
			result.maximumProcessThreads = max(result.maximumProcessThreads, threads)
		}
		select {
		case <-ctx.Done():
			return result
		case <-ticker.C:
		}
	}
}

func (observation *backendHECLoadOperationalObservation) observe(
	snapshot *opensplunkv1.GetHECOperationalSnapshotResponse,
) {
	if snapshot == nil {
		return
	}
	observation.snapshots++
	observation.maximumPending = max(
		observation.maximumPending,
		snapshot.GetDurable().GetPendingOutboxReservations(),
	)
	observation.maximumPendingBytes = max(
		observation.maximumPendingBytes,
		snapshot.GetDurable().GetPendingOutboxBytes(),
	)
	observation.maximumPendingAcknowledgments = max(
		observation.maximumPendingAcknowledgments,
		snapshot.GetAcknowledgments().GetPendingRows(),
	)
	observation.maximumRetainedRequests = max(
		observation.maximumRetainedRequests,
		snapshot.GetDurable().GetRetainedRequests(),
	)
	if age := snapshot.GetDurable().GetOldestPendingOutboxAge(); age != nil {
		observation.maximumOldestPending = max(
			observation.maximumOldestPending,
			age.AsDuration(),
		)
	}
}

func readBackendHECLoadProcessResources(
	ctx context.Context,
	process *managedProcess,
) (residentMB uint64, threads uint64, resultErr error) {
	if process == nil || process.command == nil || process.command.Process == nil {
		return 0, 0, errors.New("HEC load server process is unavailable for resource sampling")
	}
	pid := strconv.Itoa(process.command.Process.Pid)
	arguments := []string{"-o", "rss=", "-o", "nlwp=", "-p", pid}
	if runtime.GOOS == "darwin" {
		arguments = []string{"-o", "rss=", "-p", pid}
	}
	wire, err := exec.CommandContext(ctx, "ps", arguments...).Output()
	if err != nil {
		return 0, 0, fmt.Errorf("sample HEC load server resources: %w", err)
	}
	fields := strings.Fields(string(wire))
	wantFields := 2
	if runtime.GOOS == "darwin" {
		wantFields = 1
	}
	if len(fields) != wantFields {
		return 0, 0, fmt.Errorf("sample HEC load server resources returned %d fields", len(fields))
	}
	residentKiB, err := strconv.ParseUint(fields[0], 10, 64)
	if err != nil {
		return 0, 0, fmt.Errorf("parse HEC load resident memory: %w", err)
	}
	if runtime.GOOS == "darwin" {
		threadWire, err := exec.CommandContext(ctx, "ps", "-M", "-p", pid).Output()
		if err != nil {
			return 0, 0, fmt.Errorf("sample HEC load server threads: %w", err)
		}
		lines := strings.Split(strings.TrimSpace(string(threadWire)), "\n")
		if len(lines) < 2 {
			return 0, 0, errors.New("sample HEC load server threads returned no thread rows")
		}
		threads = uint64(len(lines) - 1)
	} else {
		threads, err = strconv.ParseUint(fields[1], 10, 64)
		if err != nil {
			return 0, 0, fmt.Errorf("parse HEC load process threads: %w", err)
		}
	}
	if residentKiB == 0 || threads == 0 {
		return 0, 0, errors.New("HEC load server resource sample was zero")
	}
	return (residentKiB + 1023) / 1024, threads, nil
}

func TestReadBackendHECLoadProcessResources(t *testing.T) {
	t.Parallel()
	osProcess, err := os.FindProcess(os.Getpid())
	if err != nil {
		t.Fatal(err)
	}
	process := &managedProcess{command: &exec.Cmd{Process: osProcess}}
	residentMB, threads, err := readBackendHECLoadProcessResources(
		context.Background(),
		process,
	)
	if err != nil {
		t.Fatal(err)
	}
	if residentMB == 0 || threads == 0 {
		t.Fatalf("current process resource sample = %d MiB/%d threads", residentMB, threads)
	}
}

func readBackendHECLoadOperations(
	ctx context.Context,
	client *http.Client,
	baseURL string,
	administratorToken string,
) (*opensplunkv1.GetHECOperationalSnapshotResponse, error) {
	var response opensplunkv1.GetHECOperationalSnapshotResponse
	_, err := postProtoRequestWithBearer(
		ctx,
		client,
		baseURL+"/api/v1/hec/operations/get",
		administratorToken,
		&opensplunkv1.GetHECOperationalSnapshotRequest{},
		&response,
	)
	if err != nil {
		return nil, err
	}
	return &response, nil
}

func backendHECLoadRequireBoundedBacklog(
	t *testing.T,
	snapshot *opensplunkv1.GetHECOperationalSnapshotResponse,
) {
	t.Helper()
	durable := snapshot.GetDurable()
	acknowledgments := snapshot.GetAcknowledgments()
	if durable.GetPendingOutboxReservations() == 0 ||
		durable.GetPendingOutboxReservations() > backendHECLoadMaximumPending ||
		durable.GetPendingOutboxBytes() == 0 ||
		durable.GetPendingOutboxBytes() > backendHECLoadMaximumPendingBytes ||
		acknowledgments.GetPendingRows() == 0 ||
		durable.GetRetainedRequests() > 100_000 {
		t.Fatalf("HEC outage backlog is outside bounds: %+v", snapshot)
	}
}

func waitForBackendHECLoadClickHouse(
	t *testing.T,
	ctx context.Context,
	storage clickhousedriver.Conn,
	server *managedProcess,
	protectedValues ...string,
) {
	t.Helper()
	waitContext, cancel := context.WithTimeout(ctx, 45*time.Second)
	defer cancel()
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()
	var lastErr error
	for {
		pingContext, pingCancel := context.WithTimeout(waitContext, 3*time.Second)
		lastErr = storage.Ping(pingContext)
		pingCancel()
		if lastErr == nil {
			return
		}
		if server.Exited() {
			t.Fatalf(
				"server exited while ClickHouse resumed: %v\nlogs:\n%s",
				server.Err(),
				redactForFailure(server.Logs(), protectedValues...),
			)
		}
		select {
		case <-waitContext.Done():
			t.Fatalf("wait for resumed ClickHouse: %v (last %v)", waitContext.Err(), lastErr)
		case <-ticker.C:
		}
	}
}

func waitForBackendHECLoadDrain(
	t *testing.T,
	ctx context.Context,
	client *http.Client,
	baseURL string,
	administratorToken string,
	server *managedProcess,
	protectedValues ...string,
) *opensplunkv1.GetHECOperationalSnapshotResponse {
	t.Helper()
	waitContext, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	var last *opensplunkv1.GetHECOperationalSnapshotResponse
	var lastErr error
	for {
		last, lastErr = readBackendHECLoadOperations(
			waitContext,
			client,
			baseURL,
			administratorToken,
		)
		if lastErr == nil && last.GetDurable().GetPendingOutboxReservations() == 0 &&
			last.GetAcknowledgments().GetPendingRows() == 0 &&
			last.GetDurable().GetQueueAvailable() &&
			last.GetDurable().GetRequestCapacityAvailable() &&
			last.GetReconciliation().GetAvailable() &&
			last.GetAcknowledgments().GetAvailable() {
			return last
		}
		if server.Exited() {
			t.Fatalf(
				"server exited before HEC load drained: %v\nlogs:\n%s",
				server.Err(),
				redactForFailure(server.Logs(), protectedValues...),
			)
		}
		select {
		case <-waitContext.Done():
			t.Fatalf(
				"wait for HEC load drain: %v (last error %v, snapshot %+v)",
				waitContext.Err(),
				lastErr,
				last,
			)
		case <-ticker.C:
		}
	}
}

type backendHECLoadRuntimeSignals struct {
	gcSamples         uint64
	maximumHeapMB     uint64
	schedulerSamples  uint64
	maximumGoroutines uint64
	lastGoroutines    uint64
	maximumThreads    uint64
}

var backendHECLoadGCHeapPattern = regexp.MustCompile(
	` ([0-9]+)->([0-9]+)->([0-9]+) MB, [0-9]+ MB goal`,
)

var backendHECLoadThreadsPattern = regexp.MustCompile(`(?:^| )threads=([0-9]+)(?: |$)`)

var backendHECLoadGoroutinePattern = regexp.MustCompile(`^\s+G[0-9]+:`)

func parseBackendHECLoadRuntimeSignals(logs string) (backendHECLoadRuntimeSignals, error) {
	return parseBackendHECLoadRuntimeSignalsWithGCRequirement(logs, true)
}

func parseBackendHECLoadRuntimeSignalsWithGCRequirement(
	logs string,
	requireGC bool,
) (backendHECLoadRuntimeSignals, error) {
	result := backendHECLoadRuntimeSignals{}
	currentGoroutines := uint64(0)
	inSchedulerSnapshot := false
	finishSchedulerSnapshot := func() {
		if !inSchedulerSnapshot {
			return
		}
		result.schedulerSamples++
		result.maximumGoroutines = max(result.maximumGoroutines, currentGoroutines)
		result.lastGoroutines = currentGoroutines
	}
	for line := range strings.SplitSeq(logs, "\n") {
		if match := backendHECLoadGCHeapPattern.FindStringSubmatch(line); len(match) == 4 {
			result.gcSamples++
			for _, value := range match[1:] {
				megabytes, err := strconv.ParseUint(value, 10, 64)
				if err != nil {
					return backendHECLoadRuntimeSignals{}, fmt.Errorf("parse HEC load GC heap: %w", err)
				}
				result.maximumHeapMB = max(result.maximumHeapMB, megabytes)
			}
		}
		if strings.HasPrefix(line, "SCHED ") {
			finishSchedulerSnapshot()
			inSchedulerSnapshot = true
			currentGoroutines = 0
			if match := backendHECLoadThreadsPattern.FindStringSubmatch(line); len(match) == 2 {
				threads, err := strconv.ParseUint(match[1], 10, 64)
				if err != nil {
					return backendHECLoadRuntimeSignals{}, fmt.Errorf("parse HEC load scheduler threads: %w", err)
				}
				result.maximumThreads = max(result.maximumThreads, threads)
			}
			continue
		}
		// Detailed scheduler output includes _Gdead records (status=6) in its
		// inventory. They are reusable runtime descriptors, not live goroutines
		// consuming request resources, so exclude them from the live ceiling.
		if inSchedulerSnapshot && backendHECLoadGoroutinePattern.MatchString(line) &&
			!strings.Contains(line, "status=6") {
			currentGoroutines++
		}
	}
	finishSchedulerSnapshot()
	if (requireGC && (result.gcSamples == 0 || result.maximumHeapMB == 0)) ||
		result.schedulerSamples == 0 || result.maximumGoroutines == 0 ||
		result.maximumThreads == 0 {
		return backendHECLoadRuntimeSignals{}, fmt.Errorf(
			"HEC load runtime trace is incomplete: %+v",
			result,
		)
	}
	return result, nil
}

func TestParseBackendHECLoadRuntimeSignals(t *testing.T) {
	t.Parallel()
	logs := strings.Join([]string{
		"gc 1 @0.1s 1%: 0.1+0.2+0.1 ms clock, 1+0/1/0+1 ms cpu, 4->7->3 MB, 8 MB goal, 1 MB stacks, 0 MB globals, 8 P",
		"SCHED 5000ms: gomaxprocs=8 idleprocs=7 threads=12 spinningthreads=0 needspinning=0 idlethreads=6 runqueue=0 gcwaiting=false",
		"  G1: status=4(chan receive) m=nil lockedm=nil",
		"  G2: status=4(select) m=nil lockedm=nil",
		"  G3: status=6() m=nil lockedm=nil",
		"gc 2 @5.1s 1%: 0.1+0.2+0.1 ms clock, 1+0/1/0+1 ms cpu, 6->9->4 MB, 10 MB goal, 1 MB stacks, 0 MB globals, 8 P",
		"SCHED 10000ms: gomaxprocs=8 idleprocs=8 threads=10 spinningthreads=0 needspinning=0 idlethreads=7 runqueue=0 gcwaiting=false",
		"  G1: status=4(chan receive) m=nil lockedm=nil",
	}, "\n")
	got, err := parseBackendHECLoadRuntimeSignals(logs)
	if err != nil {
		t.Fatal(err)
	}
	if got.gcSamples != 2 || got.maximumHeapMB != 9 || got.schedulerSamples != 2 ||
		got.maximumGoroutines != 2 || got.lastGoroutines != 1 || got.maximumThreads != 12 {
		t.Fatalf("parsed HEC load runtime signals = %+v", got)
	}
}

func TestParseBackendHECLoadRuntimeSignalsAllowsSparseLongTrace(t *testing.T) {
	t.Parallel()
	logs := strings.Join([]string{
		"SCHED 1800000ms: gomaxprocs=8 idleprocs=7 threads=12 spinningthreads=0 needspinning=0 idlethreads=6 runqueue=0 gcwaiting=false",
		"  G1: status=4(chan receive) m=nil lockedm=nil",
		"  G2: status=6() m=nil lockedm=nil",
	}, "\n")
	got, err := parseBackendHECLoadRuntimeSignalsWithGCRequirement(logs, false)
	if err != nil {
		t.Fatal(err)
	}
	if got.gcSamples != 0 || got.maximumHeapMB != 0 || got.schedulerSamples != 1 ||
		got.maximumGoroutines != 1 || got.maximumThreads != 12 {
		t.Fatalf("parsed sparse long HEC runtime signals = %+v", got)
	}
}
