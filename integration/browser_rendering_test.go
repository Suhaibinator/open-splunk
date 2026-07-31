//go:build !windows

package integration_test

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/savedobjects"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
	"github.com/Suhaibinator/open-splunk/internal/server"
)

const (
	browserRenderingJobID    = "browser-fixed-result-rendering"
	browserRenderingRowCount = 1_000
	browserRenderingColumns  = 64
)

type browserRenderingSnapshotter struct{}

func (browserRenderingSnapshotter) VisibilityCutoff(context.Context) (uint64, error) {
	return 23, nil
}

type browserRenderingExecutor struct {
	calls atomic.Uint32
}

func (executor *browserRenderingExecutor) Execute(
	ctx context.Context,
	_ clickhouse.CompiledQuery,
	sink searchjobs.ResultSink,
) error {
	executor.calls.Add(1)
	columns := make([]searchjobs.Column, browserRenderingColumns)
	columns[0] = searchjobs.Column{Name: "group", Kind: searchjobs.ValueKindString}
	for columnIndex := 1; columnIndex < browserRenderingColumns; columnIndex++ {
		name := "count"
		if columnIndex > 1 {
			name = fmt.Sprintf("metric_%02d", columnIndex)
		}
		columns[columnIndex] = searchjobs.Column{
			Name: name,
			Kind: searchjobs.ValueKindUnsigned,
		}
	}
	if err := sink.SetSchema(searchjobs.Schema{Columns: columns}); err != nil {
		return err
	}
	for ordinal := 0; ordinal < browserRenderingRowCount; ordinal++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		cells := make([]searchjobs.Value, browserRenderingColumns)
		cells[0] = searchjobs.StringValue(fmt.Sprintf("render-row-%04d", ordinal))
		for columnIndex := 1; columnIndex < browserRenderingColumns; columnIndex++ {
			cells[columnIndex] = searchjobs.UnsignedValue(
				uint64(ordinal*browserRenderingColumns + columnIndex),
			)
		}
		if err := sink.AddRow(cells); err != nil {
			return err
		}
	}
	if progress, ok := sink.(searchjobs.ProgressSink); ok {
		return progress.ReportProgress(searchjobs.ExecutionProgressDelta{
			ScannedRows:  browserRenderingRowCount,
			ScannedBytes: browserRenderingRowCount * 64,
		})
	}
	return nil
}

type browserRenderingMetrics struct {
	Version                  int      `json:"version"`
	RowCount                 int      `json:"rowCount"`
	ColumnCount              int      `json:"columnCount"`
	ResponseBytes            int      `json:"responseBytes"`
	ResponseSHA256           string   `json:"responseSHA256"`
	MaterializedRows         int      `json:"materializedRows"`
	SpacerRows               int      `json:"spacerRows"`
	TableBodyRows            int      `json:"tableBodyRows"`
	MaterializedCells        int      `json:"materializedCells"`
	MaximumMaterializedRows  int      `json:"maximumMaterializedRows"`
	MaximumTableBodyRows     int      `json:"maximumTableBodyRows"`
	TableScrollWidth         int      `json:"tableScrollWidth"`
	StableRenderMilliseconds *float64 `json:"stableRenderMilliseconds"`
	BottomStableMilliseconds *float64 `json:"bottomStableMilliseconds"`
	MutationCallbacks        int      `json:"mutationCallbacks"`
	StabilityRetries         *int     `json:"stabilityRetries"`
	BrowserVersion           string   `json:"browserVersion"`
}

func TestBrowserFixedResultRendering(t *testing.T) {
	if os.Getenv(backendIntegrationFlag) != "1" {
		t.Skip("set " + backendIntegrationFlag + "=1 to run the fixed-result browser rendering test")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	repository := repositoryRoot(t)
	stagedBackendRepository := buildBackendFrontend(t, ctx, repository)

	controlDB, err := control.Open(ctx, filepath.Join(t.TempDir(), "control.sqlite"))
	if err != nil {
		t.Fatalf("open browser rendering control database: %v", err)
	}
	t.Cleanup(func() {
		if err := controlDB.Close(); err != nil {
			t.Errorf("close browser rendering control database: %v", err)
		}
	})
	if _, err := controlDB.CreateIndex(ctx, control.IndexDefinition{
		Name:             "main",
		DisplayName:      "Browser rendering",
		RetentionPeriod:  time.Hour,
		IngestionEnabled: true,
		SearchEnabled:    true,
	}); err != nil {
		t.Fatalf("create browser rendering index: %v", err)
	}
	savedSearches, err := savedobjects.New(controlDB, savedobjects.Options{
		CursorKey: []byte("123456789abcdef0123456789abcdef0"),
	})
	if err != nil {
		t.Fatalf("create browser rendering saved-search store: %v", err)
	}

	anchor := time.Date(2026, time.July, 26, 18, 0, 0, 0, time.UTC)
	executor := &browserRenderingExecutor{}
	manager, err := searchjobs.New(searchjobs.Config{
		Executor:        executor,
		Snapshotter:     browserRenderingSnapshotter{},
		Compiler:        clickhouse.Compiler{Database: "open_splunk", Table: "events"},
		MaxConcurrent:   1,
		MaxRows:         browserRenderingRowCount,
		MaxBytes:        64 << 20,
		MaxPageBytes:    64 << 20,
		DefaultPageSize: browserRenderingRowCount,
		MaxPageSize:     browserRenderingRowCount,
		RetentionTTL:    time.Hour,
		CleanupInterval: -1,
		Now:             func() time.Time { return anchor },
		NewID:           func() string { return browserRenderingJobID },
		CursorKey:       []byte("abcdef0123456789abcdef0123456789"),
	})
	if err != nil {
		t.Fatalf("create browser rendering search manager: %v", err)
	}
	t.Cleanup(func() {
		if err := manager.Close(); err != nil {
			t.Errorf("close browser rendering search manager: %v", err)
		}
	})

	handler, err := server.NewHandler(server.Config{
		SearchJobs:      manager,
		Indexes:         browserSearchOnlyCatalog(controlDB),
		SavedSearches:   savedSearches,
		WebUI:           os.DirFS(filepath.Join(stagedBackendRepository, "out")),
		OwnerID:         "browser-rendering-owner",
		TenantID:        "browser-rendering-tenant",
		MaximumPageSize: browserRenderingRowCount,
		Now:             func() time.Time { return anchor },
		Bootstrap: server.BootstrapConfig{
			ServerVersion:           "browser-rendering-test",
			APIVersion:              "v1",
			SPLCompatibilityVersion: splCompatibilityVersionForTest,
			Features: []opensplunkv1.ServerFeature{
				opensplunkv1.ServerFeature_SERVER_FEATURE_SEARCH,
			},
		},
	})
	if err != nil {
		t.Fatalf("create browser rendering HTTP handler: %v", err)
	}
	var (
		createCalls atomic.Uint32
		getCalls    atomic.Uint32
		resultCalls atomic.Uint32
	)
	testServer := newIPv4LoopbackTestServer(t, http.HandlerFunc(
		func(response http.ResponseWriter, request *http.Request) {
			if request.Method == http.MethodPost {
				switch request.URL.Path {
				case "/api/v1/search/jobs/create":
					createCalls.Add(1)
				case "/api/v1/search/jobs/get":
					getCalls.Add(1)
				case "/api/v1/search/jobs/results":
					resultCalls.Add(1)
				}
			}
			handler.ServeHTTP(response, request)
		},
	))
	t.Cleanup(func() {
		testServer.Close()
		closeContext, closeCancel := context.WithTimeout(context.Background(), 2*time.Second)
		if err := handler.Close(closeContext); err != nil {
			t.Errorf("close browser rendering HTTP handler: %v", err)
		}
		closeCancel()
	})

	artifactDirectory := filepath.Join(
		repository,
		"test-results",
		"browser-fixed-result-rendering",
		"visual",
	)
	metricsPath := filepath.Join(artifactDirectory, "metrics.json")
	runBrowserRenderingSpec(
		t,
		ctx,
		repository,
		testServer.URL,
		metricsPath,
		artifactDirectory,
		anchor,
	)
	if calls := executor.calls.Load(); calls != 1 {
		t.Fatalf("browser rendering executor calls = %d, want 1", calls)
	}
	if calls := createCalls.Load(); calls != 1 {
		t.Fatalf("browser rendering create requests = %d, want 1", calls)
	}
	if calls := resultCalls.Load(); calls != 1 {
		t.Fatalf("browser rendering result requests = %d, want 1", calls)
	}
	if calls := getCalls.Load(); calls > 4 {
		t.Fatalf("browser rendering job GET requests = %d, want no more than 4", calls)
	}
	metricsInfo, err := os.Stat(metricsPath)
	if err != nil {
		t.Fatalf("stat browser rendering metrics: %v", err)
	}
	if metricsInfo.Size() <= 0 || metricsInfo.Size() > 64<<10 {
		t.Fatalf("browser rendering metrics bytes = %d, want 1..65536", metricsInfo.Size())
	}
	metricsPayload, err := os.ReadFile(metricsPath)
	if err != nil {
		t.Fatalf("read browser rendering metrics: %v", err)
	}
	var metrics browserRenderingMetrics
	if err := json.Unmarshal(metricsPayload, &metrics); err != nil {
		t.Fatalf("decode browser rendering metrics: %v", err)
	}
	if metrics.Version != 1 ||
		metrics.RowCount != browserRenderingRowCount ||
		metrics.ColumnCount != browserRenderingColumns ||
		metrics.ResponseBytes <= 0 ||
		len(metrics.ResponseSHA256) != 64 ||
		metrics.MaterializedRows <= 0 ||
		metrics.MaterializedRows > 32 ||
		metrics.SpacerRows <= 0 ||
		metrics.SpacerRows > 2 ||
		metrics.TableBodyRows != metrics.MaterializedRows+metrics.SpacerRows ||
		metrics.TableBodyRows > 34 ||
		metrics.MaterializedCells <= 0 ||
		metrics.MaterializedCells > metrics.ColumnCount*33 ||
		metrics.MaximumMaterializedRows <= 0 ||
		metrics.MaximumMaterializedRows > 32 ||
		metrics.MaximumTableBodyRows < metrics.MaximumMaterializedRows ||
		metrics.MaximumTableBodyRows < metrics.TableBodyRows ||
		metrics.MaximumTableBodyRows > 34 ||
		metrics.TableScrollWidth <= 0 ||
		metrics.StableRenderMilliseconds == nil ||
		*metrics.StableRenderMilliseconds < 0 ||
		math.IsNaN(*metrics.StableRenderMilliseconds) ||
		math.IsInf(*metrics.StableRenderMilliseconds, 0) ||
		metrics.BottomStableMilliseconds == nil ||
		*metrics.BottomStableMilliseconds < 0 ||
		math.IsNaN(*metrics.BottomStableMilliseconds) ||
		math.IsInf(*metrics.BottomStableMilliseconds, 0) ||
		metrics.MutationCallbacks <= 0 ||
		metrics.StabilityRetries == nil ||
		*metrics.StabilityRetries < 0 ||
		metrics.BrowserVersion == "" {
		t.Fatalf("invalid browser rendering metrics: %+v", metrics)
	}
	t.Logf(
		"browser rendering metrics: stable_ms=%.3f bottom_stable_ms=%.3f response_bytes=%d materialized_rows=%d peak_rows=%d peak_dom_rows=%d mutation_callbacks=%d retries=%d browser=%q",
		*metrics.StableRenderMilliseconds,
		*metrics.BottomStableMilliseconds,
		metrics.ResponseBytes,
		metrics.MaterializedRows,
		metrics.MaximumMaterializedRows,
		metrics.MaximumTableBodyRows,
		metrics.MutationCallbacks,
		*metrics.StabilityRetries,
		metrics.BrowserVersion,
	)
}

func runBrowserRenderingSpec(
	t *testing.T,
	ctx context.Context,
	repository, baseURL, metricsPath, artifactDirectory string,
	anchor time.Time,
) {
	t.Helper()
	runBrowserVerticalSpec(t, ctx, repository, browserVerticalSpecConfig{
		grepPattern:        "fixed 1,000-row statistics result",
		outputDirectory:    "browser-fixed-result-rendering",
		failureDescription: "verify fixed-result browser rendering",
		environment: map[string]string{
			"OPEN_SPLUNK_E2E_BASE_URL":                     baseURL,
			"OPEN_SPLUNK_E2E_SPL":                          browserRenderingSPL(),
			"OPEN_SPLUNK_E2E_EARLIEST":                     anchor.Add(-time.Hour).Format(time.RFC3339Nano),
			"OPEN_SPLUNK_E2E_LATEST":                       anchor.Format(time.RFC3339Nano),
			"OPEN_SPLUNK_E2E_EXPECTED_TEXT":                "render-row-0999",
			"OPEN_SPLUNK_E2E_EXPECTED_ROWS":                strconv.Itoa(browserRenderingRowCount),
			"OPEN_SPLUNK_E2E_RENDERING_ARTIFACT_DIRECTORY": artifactDirectory,
			"OPEN_SPLUNK_E2E_RENDERING_METRICS_PATH":       metricsPath,
			"OPEN_SPLUNK_E2E_RENDERING_TEST":               "1",
		},
	})
}

func browserRenderingSPL() string {
	fields := make([]string, browserRenderingColumns)
	fields[0] = "group"
	fields[1] = "count"
	for columnIndex := 2; columnIndex < browserRenderingColumns; columnIndex++ {
		fields[columnIndex] = fmt.Sprintf("metric_%02d", columnIndex)
	}
	return "index=main | table " + strings.Join(fields, ", ")
}

var _ searchjobs.Executor = (*browserRenderingExecutor)(nil)
var _ searchjobs.Snapshotter = browserRenderingSnapshotter{}
