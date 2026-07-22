package server

import (
	"bytes"
	"context"
	"errors"
	"io"
	"mime"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	exportjobs "github.com/Suhaibinator/open-splunk/internal/export"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
	"google.golang.org/protobuf/proto"
)

type fakeExports struct {
	mu sync.Mutex

	createFn func(context.Context, searchjobs.AccessScope, exportjobs.CreateRequest) (exportjobs.Job, error)
	getFn    func(context.Context, searchjobs.AccessScope, string) (exportjobs.Job, error)
	cancelFn func(context.Context, searchjobs.AccessScope, string) (exportjobs.Job, error)
	grantFn  func(context.Context, searchjobs.AccessScope, string) (exportjobs.DownloadGrant, error)
	redeemFn func(context.Context, string) (exportjobs.ArtifactDownload, error)

	createCalls int
	getCalls    int
	cancelCalls int
	grantCalls  int
	redeemCalls int
}

func (service *fakeExports) Create(ctx context.Context, scope searchjobs.AccessScope, request exportjobs.CreateRequest) (exportjobs.Job, error) {
	service.mu.Lock()
	service.createCalls++
	fn := service.createFn
	service.mu.Unlock()
	if fn == nil {
		return exportjobs.Job{}, errors.New("unexpected export create")
	}
	return fn(ctx, scope, request)
}

func (service *fakeExports) Get(ctx context.Context, scope searchjobs.AccessScope, id string) (exportjobs.Job, error) {
	service.mu.Lock()
	service.getCalls++
	fn := service.getFn
	service.mu.Unlock()
	if fn == nil {
		return exportjobs.Job{}, errors.New("unexpected export get")
	}
	return fn(ctx, scope, id)
}

func (service *fakeExports) Cancel(ctx context.Context, scope searchjobs.AccessScope, id string) (exportjobs.Job, error) {
	service.mu.Lock()
	service.cancelCalls++
	fn := service.cancelFn
	service.mu.Unlock()
	if fn == nil {
		return exportjobs.Job{}, errors.New("unexpected export cancel")
	}
	return fn(ctx, scope, id)
}

func (service *fakeExports) CreateDownloadGrant(ctx context.Context, scope searchjobs.AccessScope, id string) (exportjobs.DownloadGrant, error) {
	service.mu.Lock()
	service.grantCalls++
	fn := service.grantFn
	service.mu.Unlock()
	if fn == nil {
		return exportjobs.DownloadGrant{}, errors.New("unexpected export download grant")
	}
	return fn(ctx, scope, id)
}

func (service *fakeExports) RedeemDownload(ctx context.Context, token string) (exportjobs.ArtifactDownload, error) {
	service.mu.Lock()
	service.redeemCalls++
	fn := service.redeemFn
	service.mu.Unlock()
	if fn == nil {
		return nil, errors.New("unexpected export download redemption")
	}
	return fn(ctx, token)
}

func (service *fakeExports) callCounts() (create, get, cancel, grant, redeem int) {
	service.mu.Lock()
	defer service.mu.Unlock()
	return service.createCalls, service.getCalls, service.cancelCalls, service.grantCalls, service.redeemCalls
}

type memoryArtifactDownload struct {
	mu       sync.Mutex
	reader   io.Reader
	artifact exportjobs.Artifact
	closed   int
}

type deadlineResponseRecorder struct {
	*httptest.ResponseRecorder
	deadlines []time.Time
}

type flushResponseRecorder struct {
	*httptest.ResponseRecorder
	flushCalls int
	onFlush    func()
}

func (recorder *flushResponseRecorder) FlushError() error {
	recorder.flushCalls++
	if recorder.onFlush != nil {
		recorder.onFlush()
	}
	return nil
}

func (recorder *flushResponseRecorder) Flush() {
	_ = recorder.FlushError()
}

func (recorder *deadlineResponseRecorder) SetWriteDeadline(deadline time.Time) error {
	recorder.deadlines = append(recorder.deadlines, deadline)
	return nil
}

func (download *memoryArtifactDownload) Read(destination []byte) (int, error) {
	download.mu.Lock()
	defer download.mu.Unlock()
	return download.reader.Read(destination)
}

func (download *memoryArtifactDownload) Close() error {
	download.mu.Lock()
	defer download.mu.Unlock()
	download.closed++
	return nil
}

func (download *memoryArtifactDownload) Artifact() exportjobs.Artifact {
	return download.artifact
}

func (download *memoryArtifactDownload) closeCount() int {
	download.mu.Lock()
	defer download.mu.Unlock()
	return download.closed
}

func TestExportRoutesRoundTripProtobufAndScope(t *testing.T) {
	ownerID, tenantID := "owner-export", "tenant-export"
	queued := testExportJob("export-create", exportjobs.FormatCSV, exportjobs.StateQueued)
	completed := testExportJob("export-get", exportjobs.FormatJSONLines, exportjobs.StateCompleted)
	completed.SearchJobID = "search-get"
	completed.JSONLines = exportjobs.JSONLinesOptions{IntegerEncoding: exportjobs.JSONIntegerString, IncludeTypeMetadata: true}
	completed.Artifact = &exportjobs.Artifact{
		FileName: "search-get.jsonl", MediaType: "application/x-ndjson", SizeBytes: 321, RowCount: 7,
		ExpiresAt: testNow.Add(10 * time.Minute),
	}
	canceled := testExportJob("export-cancel", exportjobs.FormatCSV, exportjobs.StateCanceled)
	grantExpiresAt := testNow.Add(30 * time.Second)

	service := &fakeExports{}
	service.createFn = func(_ context.Context, scope searchjobs.AccessScope, request exportjobs.CreateRequest) (exportjobs.Job, error) {
		assertExportScope(t, scope, tenantID, ownerID)
		if request.SearchJobID != "search-create" || request.Format != exportjobs.FormatCSV ||
			request.RowLimit != 77 || request.ByteLimit != 8_192 || request.CSV.HeaderMode != exportjobs.CSVHeaderDisplayNames {
			t.Fatalf("create request = %+v", request)
		}
		if !equalStrings(request.Columns, []string{"_time", "message"}) {
			t.Fatalf("create columns = %v", request.Columns)
		}
		return queued, nil
	}
	service.getFn = func(_ context.Context, scope searchjobs.AccessScope, id string) (exportjobs.Job, error) {
		assertExportScope(t, scope, tenantID, ownerID)
		if id != "export-get" {
			t.Fatalf("get ID = %q", id)
		}
		return completed, nil
	}
	service.grantFn = func(_ context.Context, scope searchjobs.AccessScope, id string) (exportjobs.DownloadGrant, error) {
		assertExportScope(t, scope, tenantID, ownerID)
		if id != "export-get" {
			t.Fatalf("grant ID = %q", id)
		}
		return exportjobs.DownloadGrant{Token: "one-time-secret", ExpiresAt: grantExpiresAt}, nil
	}
	service.cancelFn = func(_ context.Context, scope searchjobs.AccessScope, id string) (exportjobs.Job, error) {
		assertExportScope(t, scope, tenantID, ownerID)
		if id != "export-cancel" {
			t.Fatalf("cancel ID = %q", id)
		}
		return canceled, nil
	}

	handler := newTestHandler(t, Config{
		SearchJobs: &fakeSearchJobs{}, Indexes: fakeIndexCatalog{}, Exports: service,
		WebUI: testUI(), OwnerID: ownerID, TenantID: tenantID, Now: func() time.Time { return testNow },
	})
	rowLimit, byteLimit := uint64(77), uint64(8_192)
	response := postProto(t, handler, "/api/v1/search/exports/create", &opensplunkv1.CreateExportJobRequest{
		Definition: &opensplunkv1.ExportDefinition{
			SearchJobId: " search-create ", Columns: []string{"_time", "message"}, RowLimit: &rowLimit, ByteLimit: &byteLimit,
			FormatOptions: &opensplunkv1.ExportDefinition_Csv{Csv: &opensplunkv1.CsvExportOptions{
				HeaderMode: opensplunkv1.CsvHeaderMode_CSV_HEADER_MODE_DISPLAY_NAMES,
			}},
		},
	})
	if response.Code != http.StatusOK {
		t.Fatalf("create status = %d, body = %s", response.Code, response.Body.String())
	}
	var created opensplunkv1.CreateExportJobResponse
	unmarshalResponse(t, response, &created)
	if created.GetExportJob().GetExportJobId() != queued.ID ||
		created.GetExportJob().GetFormat() != opensplunkv1.ExportFormat_EXPORT_FORMAT_CSV ||
		created.GetExportJob().GetState() != opensplunkv1.ExportJobState_EXPORT_JOB_STATE_QUEUED {
		t.Fatalf("created job = %+v", created.GetExportJob())
	}

	response = postProto(t, handler, "/api/v1/search/exports/get", &opensplunkv1.GetExportJobRequest{
		ExportJobId: " export-get ", IssueDownloadGrant: true,
	})
	if response.Code != http.StatusOK {
		t.Fatalf("get status = %d, body = %s", response.Code, response.Body.String())
	}
	var got opensplunkv1.GetExportJobResponse
	unmarshalResponse(t, response, &got)
	if got.GetExportJob().GetExportJobId() != completed.ID ||
		got.GetExportJob().GetDefinition().GetJsonLines().GetIntegerEncoding() != opensplunkv1.JsonIntegerEncoding_JSON_INTEGER_ENCODING_STRING ||
		!got.GetExportJob().GetDefinition().GetJsonLines().GetIncludeTypeMetadata() ||
		got.GetExportJob().GetArtifact().GetSizeBytes() != 321 || got.GetExportJob().GetArtifact().GetRowCount() != 7 {
		t.Fatalf("get job = %+v", got.GetExportJob())
	}
	if got.GetDownloadGrant().GetDownloadPath() != exportDownloadPath ||
		got.GetDownloadGrant().GetDownloadToken() != "one-time-secret" ||
		!got.GetDownloadGrant().GetExpiresAt().AsTime().Equal(grantExpiresAt) {
		t.Fatalf("download grant = %+v", got.GetDownloadGrant())
	}

	response = postProto(t, handler, "/api/v1/search/exports/cancel", &opensplunkv1.CancelExportJobRequest{ExportJobId: " export-cancel "})
	if response.Code != http.StatusOK {
		t.Fatalf("cancel status = %d, body = %s", response.Code, response.Body.String())
	}
	var cancelResponse opensplunkv1.CancelExportJobResponse
	unmarshalResponse(t, response, &cancelResponse)
	if cancelResponse.GetExportJob().GetExportJobId() != canceled.ID ||
		cancelResponse.GetExportJob().GetState() != opensplunkv1.ExportJobState_EXPORT_JOB_STATE_CANCELED {
		t.Fatalf("canceled job = %+v", cancelResponse.GetExportJob())
	}

	createCalls, getCalls, cancelCalls, grantCalls, redeemCalls := service.callCounts()
	if createCalls != 1 || getCalls != 1 || cancelCalls != 1 || grantCalls != 1 || redeemCalls != 0 {
		t.Fatalf("service calls = create %d get %d cancel %d grant %d redeem %d", createCalls, getCalls, cancelCalls, grantCalls, redeemCalls)
	}
}

func TestExportRoutesValidateBeforeCallingService(t *testing.T) {
	for name, definition := range map[string]*opensplunkv1.ExportDefinition{
		"nil CSV options":        {SearchJobId: "search-1", FormatOptions: &opensplunkv1.ExportDefinition_Csv{}},
		"nil JSON Lines options": {SearchJobId: "search-1", FormatOptions: &opensplunkv1.ExportDefinition_JsonLines{}},
	} {
		t.Run(name+" in memory", func(t *testing.T) {
			if _, err := exportRequestFromProto(definition); err == nil {
				t.Fatal("nil nested format options were accepted")
			}
		})
	}

	service := &fakeExports{}
	handler := newTestHandler(t, Config{
		SearchJobs: &fakeSearchJobs{}, Indexes: fakeIndexCatalog{}, Exports: service, WebUI: testUI(),
	})
	zero := uint64(0)
	clientID := ""
	tests := []struct {
		name    string
		path    string
		request proto.Message
	}{
		{name: "missing definition", path: "/api/v1/search/exports/create", request: &opensplunkv1.CreateExportJobRequest{}},
		{name: "present client request ID", path: "/api/v1/search/exports/create", request: &opensplunkv1.CreateExportJobRequest{ClientRequestId: &clientID, Definition: csvExportDefinition("search-1")}},
		{name: "missing search ID", path: "/api/v1/search/exports/create", request: &opensplunkv1.CreateExportJobRequest{Definition: csvExportDefinition(" ")}},
		{name: "missing format", path: "/api/v1/search/exports/create", request: &opensplunkv1.CreateExportJobRequest{Definition: &opensplunkv1.ExportDefinition{SearchJobId: "search-1"}}},
		{name: "zero row limit", path: "/api/v1/search/exports/create", request: &opensplunkv1.CreateExportJobRequest{Definition: func() *opensplunkv1.ExportDefinition {
			definition := csvExportDefinition("search-1")
			definition.RowLimit = &zero
			return definition
		}()}},
		{name: "zero byte limit", path: "/api/v1/search/exports/create", request: &opensplunkv1.CreateExportJobRequest{Definition: func() *opensplunkv1.ExportDefinition {
			definition := csvExportDefinition("search-1")
			definition.ByteLimit = &zero
			return definition
		}()}},
		{name: "invalid CSV header", path: "/api/v1/search/exports/create", request: &opensplunkv1.CreateExportJobRequest{Definition: &opensplunkv1.ExportDefinition{SearchJobId: "search-1", FormatOptions: &opensplunkv1.ExportDefinition_Csv{Csv: &opensplunkv1.CsvExportOptions{HeaderMode: 99}}}}},
		{name: "invalid JSON integer mode", path: "/api/v1/search/exports/create", request: &opensplunkv1.CreateExportJobRequest{Definition: &opensplunkv1.ExportDefinition{SearchJobId: "search-1", FormatOptions: &opensplunkv1.ExportDefinition_JsonLines{JsonLines: &opensplunkv1.JsonLinesExportOptions{IntegerEncoding: 99}}}}},
		{name: "missing get ID", path: "/api/v1/search/exports/get", request: &opensplunkv1.GetExportJobRequest{ExportJobId: " "}},
		{name: "missing cancel ID", path: "/api/v1/search/exports/cancel", request: &opensplunkv1.CancelExportJobRequest{ExportJobId: " "}},
		{name: "unsupported cancel reason", path: "/api/v1/search/exports/cancel", request: &opensplunkv1.CancelExportJobRequest{ExportJobId: "export-1", Reason: stringPointer("because")}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			response := postProto(t, handler, test.path, test.request)
			if response.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
			}
		})
	}
	createCalls, getCalls, cancelCalls, grantCalls, redeemCalls := service.callCounts()
	if createCalls != 0 || getCalls != 0 || cancelCalls != 0 || grantCalls != 0 || redeemCalls != 0 {
		t.Fatalf("invalid requests reached service: %d %d %d %d %d", createCalls, getCalls, cancelCalls, grantCalls, redeemCalls)
	}
}

func TestExportErrorsMapToStableHTTPStatuses(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want int
	}{
		{name: "invalid request", err: exportjobs.ErrInvalidRequest, want: http.StatusBadRequest},
		{name: "invalid columns", err: exportjobs.ErrInvalidColumns, want: http.StatusBadRequest},
		{name: "not found", err: exportjobs.ErrNotFound, want: http.StatusNotFound},
		{name: "expired", err: exportjobs.ErrSourceExpired, want: http.StatusGone},
		{name: "not ready", err: exportjobs.ErrSourceNotReady, want: http.StatusConflict},
		{name: "truncated source", err: exportjobs.ErrSourceTruncated, want: http.StatusConflict},
		{name: "source unavailable", err: exportjobs.ErrSourceUnavailable, want: http.StatusConflict},
		{name: "not cancelable", err: exportjobs.ErrNotCancelable, want: http.StatusConflict},
		{name: "queue full", err: exportjobs.ErrQueueFull, want: http.StatusTooManyRequests},
		{name: "capacity", err: exportjobs.ErrCapacity, want: http.StatusTooManyRequests},
		{name: "grant capacity", err: exportjobs.ErrDownloadGrantCapacity, want: http.StatusTooManyRequests},
		{name: "row limit", err: exportjobs.ErrRowLimit, want: http.StatusUnprocessableEntity},
		{name: "byte limit", err: exportjobs.ErrByteLimit, want: http.StatusUnprocessableEntity},
		{name: "closed", err: exportjobs.ErrClosed, want: http.StatusServiceUnavailable},
		{name: "artifact unavailable", err: exportjobs.ErrArtifactUnavailable, want: http.StatusServiceUnavailable},
		{name: "canceled", err: context.Canceled, want: http.StatusRequestTimeout},
		{name: "internal detail", err: errors.New("SELECT secret_path FROM exports"), want: http.StatusInternalServerError},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			service := &fakeExports{getFn: func(context.Context, searchjobs.AccessScope, string) (exportjobs.Job, error) {
				return exportjobs.Job{}, test.err
			}}
			handler := newTestHandler(t, Config{
				SearchJobs: &fakeSearchJobs{}, Indexes: fakeIndexCatalog{}, Exports: service, WebUI: testUI(),
			})
			response := postProto(t, handler, "/api/v1/search/exports/get", &opensplunkv1.GetExportJobRequest{ExportJobId: "export-1"})
			if response.Code != test.want {
				t.Fatalf("status = %d, want %d, body = %s", response.Code, test.want, response.Body.String())
			}
			if strings.Contains(response.Body.String(), "SELECT") || strings.Contains(response.Body.String(), "secret_path") {
				t.Fatalf("internal detail leaked: %s", response.Body.String())
			}
		})
	}
}

func TestExportDownloadGrantFailureDoesNotReturnPartialJob(t *testing.T) {
	service := &fakeExports{
		getFn: func(context.Context, searchjobs.AccessScope, string) (exportjobs.Job, error) {
			job := testExportJob("export-1", exportjobs.FormatCSV, exportjobs.StateCompleted)
			job.Artifact = &exportjobs.Artifact{
				FileName: "results.csv", MediaType: "text/csv; charset=utf-8", SizeBytes: 12, RowCount: 1,
				ExpiresAt: testNow.Add(time.Minute),
			}
			return job, nil
		},
		grantFn: func(context.Context, searchjobs.AccessScope, string) (exportjobs.DownloadGrant, error) {
			return exportjobs.DownloadGrant{}, exportjobs.ErrDownloadGrantCapacity
		},
	}
	handler := newTestHandler(t, Config{SearchJobs: &fakeSearchJobs{}, Indexes: fakeIndexCatalog{}, Exports: service, WebUI: testUI()})
	response := postProto(t, handler, "/api/v1/search/exports/get", &opensplunkv1.GetExportJobRequest{ExportJobId: "export-1", IssueDownloadGrant: true})
	if response.Code != http.StatusTooManyRequests {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
	}
	if strings.Contains(response.Body.String(), "export-1") {
		t.Fatalf("partial job leaked in error response: %s", response.Body.String())
	}
}

func TestExportDownloadGrantStateErrorsDescribeArtifacts(t *testing.T) {
	for _, test := range []struct {
		name string
		err  error
		want int
	}{
		{name: "not ready", err: exportjobs.ErrSourceNotReady, want: http.StatusConflict},
		{name: "expired", err: exportjobs.ErrSourceExpired, want: http.StatusGone},
		{name: "unavailable", err: exportjobs.ErrSourceUnavailable, want: http.StatusConflict},
	} {
		t.Run(test.name, func(t *testing.T) {
			service := &fakeExports{
				getFn: func(context.Context, searchjobs.AccessScope, string) (exportjobs.Job, error) {
					return testExportJob("export-1", exportjobs.FormatCSV, exportjobs.StateQueued), nil
				},
				grantFn: func(context.Context, searchjobs.AccessScope, string) (exportjobs.DownloadGrant, error) {
					return exportjobs.DownloadGrant{}, test.err
				},
			}
			handler := newTestHandler(t, Config{SearchJobs: &fakeSearchJobs{}, Indexes: fakeIndexCatalog{}, Exports: service, WebUI: testUI()})
			response := postProto(t, handler, "/api/v1/search/exports/get", &opensplunkv1.GetExportJobRequest{
				ExportJobId: "export-1", IssueDownloadGrant: true,
			})
			if response.Code != test.want || !strings.Contains(response.Body.String(), "export artifact") || strings.Contains(response.Body.String(), "search result") {
				t.Fatalf("response = %d %s", response.Code, response.Body.String())
			}
		})
	}
}

func TestCommittedExportSuccessWinsContextCancellationRace(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := mapExportCallError(ctx, nil); err != nil {
		t.Fatalf("committed export operation mapped to error = %v", err)
	}
}

func TestGetExportDoesNotIssueUnrequestedGrant(t *testing.T) {
	service := &fakeExports{
		getFn: func(context.Context, searchjobs.AccessScope, string) (exportjobs.Job, error) {
			return testExportJob("export-1", exportjobs.FormatCSV, exportjobs.StateQueued), nil
		},
		grantFn: func(context.Context, searchjobs.AccessScope, string) (exportjobs.DownloadGrant, error) {
			t.Fatal("unrequested download grant was issued")
			return exportjobs.DownloadGrant{}, errors.New("unreachable")
		},
	}
	handler := newTestHandler(t, Config{SearchJobs: &fakeSearchJobs{}, Indexes: fakeIndexCatalog{}, Exports: service, WebUI: testUI()})
	response := postProto(t, handler, "/api/v1/search/exports/get", &opensplunkv1.GetExportJobRequest{ExportJobId: "export-1"})
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
	}
	var decoded opensplunkv1.GetExportJobResponse
	unmarshalResponse(t, response, &decoded)
	if decoded.GetDownloadGrant() != nil {
		t.Fatalf("unrequested grant = %+v", decoded.GetDownloadGrant())
	}
	_, getCalls, _, grantCalls, _ := service.callCounts()
	if getCalls != 1 || grantCalls != 0 {
		t.Fatalf("get/grant calls = %d/%d", getCalls, grantCalls)
	}
}

func TestExportRoutesAreExactConditionalAndAdvertised(t *testing.T) {
	service := &fakeExports{}
	handler := newTestHandler(t, Config{SearchJobs: &fakeSearchJobs{}, Indexes: fakeIndexCatalog{}, Exports: service, WebUI: testUI()})

	request := httptest.NewRequest(http.MethodGet, "/api/v1/search/exports/create", nil)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusMethodNotAllowed || response.Header().Get("Allow") != http.MethodPost {
		t.Fatalf("typed method status/allow = %d / %q", response.Code, response.Header().Get("Allow"))
	}
	request = httptest.NewRequest(http.MethodPost, exportDownloadPath, nil)
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusMethodNotAllowed || response.Header().Get("Allow") != http.MethodGet {
		t.Fatalf("download method status/allow = %d / %q", response.Code, response.Header().Get("Allow"))
	}
	request = httptest.NewRequest(http.MethodGet, exportDownloadPath+"/extra", nil)
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusNotFound {
		t.Fatalf("download suffix status = %d", response.Code)
	}

	bootstrapResponse := postProto(t, handler, "/api/v1/system/bootstrap", &opensplunkv1.GetSystemBootstrapRequest{})
	if bootstrapResponse.Code != http.StatusOK {
		t.Fatalf("enabled bootstrap status = %d, body = %s", bootstrapResponse.Code, bootstrapResponse.Body.String())
	}
	var enabled opensplunkv1.GetSystemBootstrapResponse
	unmarshalResponse(t, bootstrapResponse, &enabled)
	if countServerFeature(enabled.GetFeatures(), opensplunkv1.ServerFeature_SERVER_FEATURE_EXPORT_CSV) != 1 ||
		countServerFeature(enabled.GetFeatures(), opensplunkv1.ServerFeature_SERVER_FEATURE_EXPORT_JSON_LINES) != 1 {
		t.Fatalf("enabled features = %v", enabled.GetFeatures())
	}

	disabled := newTestHandler(t, Config{
		SearchJobs: &fakeSearchJobs{}, Indexes: fakeIndexCatalog{}, WebUI: testUI(),
		Bootstrap: BootstrapConfig{Features: []opensplunkv1.ServerFeature{
			opensplunkv1.ServerFeature_SERVER_FEATURE_SEARCH,
			opensplunkv1.ServerFeature_SERVER_FEATURE_EXPORT_CSV,
			opensplunkv1.ServerFeature_SERVER_FEATURE_EXPORT_JSON_LINES,
		}},
	})
	response = postProto(t, disabled, "/api/v1/search/exports/get", &opensplunkv1.GetExportJobRequest{ExportJobId: "export-1"})
	if response.Code != http.StatusNotFound {
		t.Fatalf("disabled typed route status = %d", response.Code)
	}
	request = httptest.NewRequest(http.MethodGet, exportDownloadPath, nil)
	response = httptest.NewRecorder()
	disabled.ServeHTTP(response, request)
	if response.Code != http.StatusNotFound {
		t.Fatalf("disabled download route status = %d", response.Code)
	}
	bootstrapResponse = postProto(t, disabled, "/api/v1/system/bootstrap", &opensplunkv1.GetSystemBootstrapRequest{})
	var disabledBootstrap opensplunkv1.GetSystemBootstrapResponse
	unmarshalResponse(t, bootstrapResponse, &disabledBootstrap)
	if countServerFeature(disabledBootstrap.GetFeatures(), opensplunkv1.ServerFeature_SERVER_FEATURE_EXPORT_CSV) != 0 ||
		countServerFeature(disabledBootstrap.GetFeatures(), opensplunkv1.ServerFeature_SERVER_FEATURE_EXPORT_JSON_LINES) != 0 {
		t.Fatalf("disabled features = %v", disabledBootstrap.GetFeatures())
	}

	var typedNil *fakeExports
	if _, err := NewHandler(Config{
		SearchJobs: &fakeSearchJobs{}, Indexes: fakeIndexCatalog{}, SavedSearches: &fakeSavedSearches{}, Exports: typedNil, WebUI: testUI(),
	}); err != nil {
		t.Fatalf("typed-nil optional exports: %v", err)
	}
}

func TestExportDownloadReturnsRawArtifactWithSecureHeaders(t *testing.T) {
	payload := []byte("_time,message\n2026-07-22T12:00:00Z,hello\n")
	download := &memoryArtifactDownload{
		reader: bytes.NewReader(payload),
		artifact: exportjobs.Artifact{
			FileName: "search results.csv", MediaType: "text/csv; charset=utf-8", SizeBytes: uint64(len(payload)), RowCount: 1,
			ExpiresAt: testNow.Add(time.Minute),
		},
	}
	service := &fakeExports{redeemFn: func(ctx context.Context, token string) (exportjobs.ArtifactDownload, error) {
		if token != "download-token" {
			t.Fatalf("token = %q", token)
		}
		deadline, hasDeadline := ctx.Deadline()
		if !hasDeadline || time.Until(deadline) < time.Minute {
			t.Fatalf("raw download deadline = %s, want independent long transfer deadline", deadline)
		}
		return download, nil
	}}
	handler := newTestHandler(t, Config{
		SearchJobs: &fakeSearchJobs{}, Indexes: fakeIndexCatalog{}, Exports: service, WebUI: testUI(), RouteTimeout: time.Millisecond,
	})
	request := httptest.NewRequest(http.MethodGet, exportDownloadPath, nil)
	request.Header.Set("Authorization", "Bearer download-token")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)

	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
	}
	if !bytes.Equal(response.Body.Bytes(), payload) {
		t.Fatalf("body = %q", response.Body.Bytes())
	}
	if response.Header().Get("Content-Type") != download.artifact.MediaType || response.Header().Get("Content-Length") != strconv.Itoa(len(payload)) {
		t.Fatalf("representation headers = %v", response.Header())
	}
	disposition, parameters, err := mime.ParseMediaType(response.Header().Get("Content-Disposition"))
	if err != nil || disposition != "attachment" || parameters["filename"] != download.artifact.FileName {
		t.Fatalf("content disposition = %q (%q, %v, %v)", response.Header().Get("Content-Disposition"), disposition, parameters, err)
	}
	if response.Header().Get("Cache-Control") != "no-store" || response.Header().Get("Pragma") != "no-cache" ||
		response.Header().Get("X-Content-Type-Options") != "nosniff" || response.Header().Get("Accept-Ranges") != "" {
		t.Fatalf("security headers = %v", response.Header())
	}
	if download.closeCount() != 1 {
		t.Fatalf("download close count = %d", download.closeCount())
	}
	_, _, _, _, redeemCalls := service.callCounts()
	if redeemCalls != 1 {
		t.Fatalf("redeem calls = %d", redeemCalls)
	}
}

func TestExportDownloadKeepsDeadlineThroughServerFinalFlush(t *testing.T) {
	payload := []byte("x\n")
	service := &fakeExports{redeemFn: func(context.Context, string) (exportjobs.ArtifactDownload, error) {
		return &memoryArtifactDownload{
			reader: bytes.NewReader(payload),
			artifact: exportjobs.Artifact{
				FileName: "result.jsonl", MediaType: "application/x-ndjson", SizeBytes: uint64(len(payload)),
				ExpiresAt: testNow.Add(time.Minute),
			},
		}, nil
	}}
	handler := newTestHandler(t, Config{
		SearchJobs: &fakeSearchJobs{}, Indexes: fakeIndexCatalog{}, Exports: service, WebUI: testUI(),
	})
	request := httptest.NewRequest(http.MethodGet, exportDownloadPath, nil)
	request.Header.Set("Authorization", "Bearer download-token")
	response := &deadlineResponseRecorder{ResponseRecorder: httptest.NewRecorder()}
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK || !bytes.Equal(response.Body.Bytes(), payload) {
		t.Fatalf("response = %d %q", response.Code, response.Body.Bytes())
	}
	if len(response.deadlines) != 1 || response.deadlines[0].IsZero() {
		t.Fatalf("write deadlines = %v, want one nonzero deadline left for net/http final flush", response.deadlines)
	}
}

func TestExportDownloadFlushesBeforeReleasingArtifactLease(t *testing.T) {
	payload := []byte("buffered")
	download := &memoryArtifactDownload{
		reader: bytes.NewReader(payload),
		artifact: exportjobs.Artifact{
			FileName: "result.csv", MediaType: "text/csv", SizeBytes: uint64(len(payload)),
			ExpiresAt: testNow.Add(time.Minute),
		},
	}
	service := &fakeExports{redeemFn: func(context.Context, string) (exportjobs.ArtifactDownload, error) {
		return download, nil
	}}
	handler := newTestHandler(t, Config{
		SearchJobs: &fakeSearchJobs{}, Indexes: fakeIndexCatalog{}, Exports: service, WebUI: testUI(),
		MaximumConcurrentDownloads: 1,
	})
	request := httptest.NewRequest(http.MethodGet, exportDownloadPath, nil)
	request.Header.Set("Authorization", "Bearer download-token")
	response := &flushResponseRecorder{ResponseRecorder: httptest.NewRecorder()}
	response.onFlush = func() {
		if download.closeCount() != 0 {
			t.Fatal("artifact lease closed before response flush")
		}
	}
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK || !bytes.Equal(response.Body.Bytes(), payload) {
		t.Fatalf("response = %d %q", response.Code, response.Body.Bytes())
	}
	if response.flushCalls != 1 || download.closeCount() != 1 {
		t.Fatalf("flush calls = %d, close calls = %d", response.flushCalls, download.closeCount())
	}
}

func TestExportAPIRoutesRejectDNSRebindingBeforeServiceOrGrantUse(t *testing.T) {
	payload := []byte("{\"message\":\"safe\"}\n")
	service := &fakeExports{
		createFn: func(context.Context, searchjobs.AccessScope, exportjobs.CreateRequest) (exportjobs.Job, error) {
			return testExportJob("export-created", exportjobs.FormatCSV, exportjobs.StateQueued), nil
		},
		redeemFn: func(_ context.Context, token string) (exportjobs.ArtifactDownload, error) {
			if token != "still-valid" {
				t.Fatalf("token = %q", token)
			}
			return &memoryArtifactDownload{
				reader: bytes.NewReader(payload),
				artifact: exportjobs.Artifact{
					FileName: "results.jsonl", MediaType: "application/x-ndjson", SizeBytes: uint64(len(payload)),
					ExpiresAt: testNow.Add(time.Minute),
				},
			}, nil
		},
	}
	handler := newTestHandler(t, Config{
		SearchJobs: &fakeSearchJobs{}, Indexes: fakeIndexCatalog{}, Exports: service, WebUI: testUI(),
		AdministrativeAllowedHosts: []string{"example.com"},
	})

	hostileHeaders := map[string]string{"Host": "attacker.example", "Origin": "http://attacker.example"}
	typed := postProtoHeaders(t, handler, "/api/v1/search/exports/create", &opensplunkv1.CreateExportJobRequest{
		Definition: csvExportDefinition("search-1"),
	}, hostileHeaders)
	if typed.Code != http.StatusForbidden {
		t.Fatalf("hostile typed status = %d, body = %s", typed.Code, typed.Body.String())
	}
	createCalls, _, _, _, redeemCalls := service.callCounts()
	if createCalls != 0 || redeemCalls != 0 {
		t.Fatalf("hostile typed request reached service: create = %d redeem = %d", createCalls, redeemCalls)
	}

	rawRequest := httptest.NewRequest(http.MethodGet, exportDownloadPath, nil)
	rawRequest.Host = "attacker.example"
	rawRequest.Header.Set("Origin", "http://attacker.example")
	rawRequest.Header.Set("Authorization", "Bearer still-valid")
	rawResponse := httptest.NewRecorder()
	handler.ServeHTTP(rawResponse, rawRequest)
	if rawResponse.Code != http.StatusForbidden {
		t.Fatalf("hostile raw status = %d, body = %s", rawResponse.Code, rawResponse.Body.String())
	}
	_, _, _, _, redeemCalls = service.callCounts()
	if redeemCalls != 0 {
		t.Fatalf("hostile raw request consumed grant: redeem = %d", redeemCalls)
	}

	allowedTyped := postProtoHeaders(t, handler, "/api/v1/search/exports/create", &opensplunkv1.CreateExportJobRequest{
		Definition: csvExportDefinition("search-1"),
	}, map[string]string{"Host": "example.com", "Origin": "http://example.com", "Sec-Fetch-Site": "same-origin"})
	if allowedTyped.Code != http.StatusOK {
		t.Fatalf("allowed typed status = %d, body = %s", allowedTyped.Code, allowedTyped.Body.String())
	}

	rawRequest = httptest.NewRequest(http.MethodGet, exportDownloadPath, nil)
	rawRequest.Host = "example.com"
	rawRequest.Header.Set("Origin", "http://example.com")
	rawRequest.Header.Set("Sec-Fetch-Site", "same-origin")
	rawRequest.Header.Set("Authorization", "Bearer still-valid")
	rawResponse = httptest.NewRecorder()
	handler.ServeHTTP(rawResponse, rawRequest)
	if rawResponse.Code != http.StatusOK || !bytes.Equal(rawResponse.Body.Bytes(), payload) {
		t.Fatalf("allowed raw response = %d %q", rawResponse.Code, rawResponse.Body.Bytes())
	}
	createCalls, _, _, _, redeemCalls = service.callCounts()
	if createCalls != 1 || redeemCalls != 1 {
		t.Fatalf("allowed requests service calls = create %d redeem %d", createCalls, redeemCalls)
	}
}

func TestSlowDownloadsCannotStarveTypedAPICapacity(t *testing.T) {
	reader, writer := io.Pipe()
	defer reader.Close()
	defer writer.Close()
	redeemed := make(chan struct{})
	service := &fakeExports{redeemFn: func(_ context.Context, token string) (exportjobs.ArtifactDownload, error) {
		if token != "first-token" {
			t.Errorf("unexpected redemption token %q", token)
			return nil, exportjobs.ErrInvalidDownloadGrant
		}
		close(redeemed)
		return &memoryArtifactDownload{
			reader: reader,
			artifact: exportjobs.Artifact{
				FileName: "result.csv", MediaType: "text/csv", SizeBytes: 1, ExpiresAt: testNow.Add(time.Minute),
			},
		}, nil
	}}
	handler := newTestHandler(t, Config{
		SearchJobs: &fakeSearchJobs{}, Indexes: fakeIndexCatalog{}, Exports: service, WebUI: testUI(),
		MaximumConcurrentRequests: 1, MaximumConcurrentDownloads: 1,
	})

	firstResult := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		request := httptest.NewRequest(http.MethodGet, exportDownloadPath, nil)
		request.Header.Set("Authorization", "Bearer first-token")
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		firstResult <- response
	}()
	select {
	case <-redeemed:
	case <-time.After(5 * time.Second):
		t.Fatal("first download did not reach its blocking reader")
	}

	// Typed work has an independent permit even while the download holds its
	// own gate for the full response lifetime.
	bootstrap := postProto(t, handler, "/api/v1/system/bootstrap", &opensplunkv1.GetSystemBootstrapRequest{})
	if bootstrap.Code != http.StatusOK {
		t.Fatalf("typed API was starved by slow download: %d %s", bootstrap.Code, bootstrap.Body.String())
	}

	secondRequest := httptest.NewRequest(http.MethodGet, exportDownloadPath, nil)
	secondRequest.Header.Set("Authorization", "Bearer second-token")
	secondResponse := httptest.NewRecorder()
	handler.ServeHTTP(secondResponse, secondRequest)
	if secondResponse.Code != http.StatusServiceUnavailable || secondResponse.Header().Get("Retry-After") != "1" {
		t.Fatalf("second download capacity response = %d headers %v body %s", secondResponse.Code, secondResponse.Header(), secondResponse.Body.String())
	}
	_, _, _, _, redeemCalls := service.callCounts()
	if redeemCalls != 1 {
		t.Fatalf("download gate consumed another grant: redemption calls = %d", redeemCalls)
	}

	if _, err := writer.Write([]byte("x")); err != nil {
		t.Fatal(err)
	}
	select {
	case response := <-firstResult:
		if response.Code != http.StatusOK || response.Body.String() != "x" {
			t.Fatalf("first download response = %d %q", response.Code, response.Body.String())
		}
	case <-time.After(5 * time.Second):
		t.Fatal("first download did not complete")
	}
}

func TestExportDownloadRejectsMalformedRequestsBeforeRedemption(t *testing.T) {
	service := &fakeExports{redeemFn: func(context.Context, string) (exportjobs.ArtifactDownload, error) {
		return nil, errors.New("redemption must not be called")
	}}
	handler := newTestHandler(t, Config{SearchJobs: &fakeSearchJobs{}, Indexes: fakeIndexCatalog{}, Exports: service, WebUI: testUI()})
	tests := []struct {
		name       string
		configure  func(*http.Request)
		wantStatus int
	}{
		{name: "missing authorization", configure: func(*http.Request) {}, wantStatus: http.StatusUnauthorized},
		{name: "basic authorization", configure: func(request *http.Request) { request.Header.Set("Authorization", "Basic abc") }, wantStatus: http.StatusUnauthorized},
		{name: "empty bearer", configure: func(request *http.Request) { request.Header.Set("Authorization", "Bearer ") }, wantStatus: http.StatusUnauthorized},
		{name: "bearer with whitespace", configure: func(request *http.Request) { request.Header.Set("Authorization", "Bearer token extra") }, wantStatus: http.StatusUnauthorized},
		{name: "duplicate authorization", configure: func(request *http.Request) {
			request.Header["Authorization"] = []string{"Bearer token", "Bearer other"}
		}, wantStatus: http.StatusUnauthorized},
		{name: "query string", configure: func(request *http.Request) { request.URL.RawQuery = "token=secret" }},
		{name: "range", configure: func(request *http.Request) { request.Header.Set("Range", "bytes=0-1") }},
		{name: "request body", configure: func(request *http.Request) {
			request.Body = io.NopCloser(strings.NewReader("x"))
			request.ContentLength = 1
		}},
		{name: "transfer encoding", configure: func(request *http.Request) {
			request.TransferEncoding = []string{"chunked"}
			request.Body = io.NopCloser(strings.NewReader(""))
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			request := httptest.NewRequest(http.MethodGet, exportDownloadPath, nil)
			if test.wantStatus == 0 {
				request.Header.Set("Authorization", "Bearer valid-token")
			}
			test.configure(request)
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, request)
			if test.wantStatus != 0 && response.Code != test.wantStatus {
				t.Fatalf("status = %d, want %d, body = %s", response.Code, test.wantStatus, response.Body.String())
			}
			if test.wantStatus == 0 && response.Code >= 200 && response.Code < 300 {
				t.Fatalf("malformed request succeeded: %d", response.Code)
			}
		})
	}
	_, _, _, _, redeemCalls := service.callCounts()
	if redeemCalls != 0 {
		t.Fatalf("malformed requests consumed %d grants", redeemCalls)
	}
}

func TestExportDownloadGrantIsOneTimeAndFailuresAreNonDisclosing(t *testing.T) {
	payload := []byte("{}\n")
	service := &fakeExports{}
	consumed := false
	service.redeemFn = func(_ context.Context, token string) (exportjobs.ArtifactDownload, error) {
		if token == "unavailable-token" {
			return nil, exportjobs.ErrArtifactUnavailable
		}
		if token != "single-use-token" || consumed {
			return nil, exportjobs.ErrInvalidDownloadGrant
		}
		consumed = true
		return &memoryArtifactDownload{
			reader:   bytes.NewReader(payload),
			artifact: exportjobs.Artifact{FileName: "results.jsonl", MediaType: "application/x-ndjson", SizeBytes: uint64(len(payload)), ExpiresAt: testNow.Add(time.Minute)},
		}, nil
	}
	handler := newTestHandler(t, Config{SearchJobs: &fakeSearchJobs{}, Indexes: fakeIndexCatalog{}, Exports: service, WebUI: testUI()})

	download := func(token string) *httptest.ResponseRecorder {
		request := httptest.NewRequest(http.MethodGet, exportDownloadPath, nil)
		request.Header.Set("Authorization", "Bearer "+token)
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		return response
	}
	if first := download("single-use-token"); first.Code != http.StatusOK || !bytes.Equal(first.Body.Bytes(), payload) {
		t.Fatalf("first redemption = %d %q", first.Code, first.Body.String())
	}
	for _, token := range []string{"single-use-token", "unknown-secret"} {
		response := download(token)
		if response.Code != http.StatusUnauthorized {
			t.Fatalf("invalid token %q status = %d, body = %s", token, response.Code, response.Body.String())
		}
		if response.Header().Get("WWW-Authenticate") == "" {
			t.Fatalf("invalid token %q omitted WWW-Authenticate", token)
		}
		if strings.Contains(response.Body.String(), token) || strings.Contains(response.Body.String(), "replay") || strings.Contains(response.Body.String(), "unknown") {
			t.Fatalf("invalid-token detail leaked: %s", response.Body.String())
		}
	}
	unavailable := download("unavailable-token")
	if unavailable.Code != http.StatusServiceUnavailable {
		t.Fatalf("unavailable status = %d, body = %s", unavailable.Code, unavailable.Body.String())
	}
	if strings.Contains(unavailable.Body.String(), "unavailable-token") {
		t.Fatalf("token leaked in unavailable response: %s", unavailable.Body.String())
	}
}

func TestExportDownloadRejectsUnsafeArtifactMetadata(t *testing.T) {
	tests := []struct {
		name     string
		artifact exportjobs.Artifact
	}{
		{name: "path", artifact: exportjobs.Artifact{FileName: "../secret.csv", MediaType: "text/csv", SizeBytes: 1}},
		{name: "header injection", artifact: exportjobs.Artifact{FileName: "safe.csv\r\nX-Injected: yes", MediaType: "text/csv", SizeBytes: 1}},
		{name: "invalid media type", artifact: exportjobs.Artifact{FileName: "safe.csv", MediaType: "text/csv\r\nX-Injected: yes", SizeBytes: 1}},
		{name: "unrepresentable length", artifact: exportjobs.Artifact{FileName: "safe.csv", MediaType: "text/csv", SizeBytes: ^uint64(0)}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			test.artifact.ExpiresAt = testNow.Add(time.Minute)
			download := &memoryArtifactDownload{reader: strings.NewReader("x"), artifact: test.artifact}
			service := &fakeExports{redeemFn: func(context.Context, string) (exportjobs.ArtifactDownload, error) {
				return download, nil
			}}
			handler := newTestHandler(t, Config{SearchJobs: &fakeSearchJobs{}, Indexes: fakeIndexCatalog{}, Exports: service, WebUI: testUI()})
			request := httptest.NewRequest(http.MethodGet, exportDownloadPath, nil)
			request.Header.Set("Authorization", "Bearer one-time-token")
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, request)
			if response.Code != http.StatusServiceUnavailable {
				t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
			}
			if response.Header().Get("Content-Disposition") != "" || response.Header().Get("X-Injected") != "" {
				t.Fatalf("unsafe artifact headers = %v", response.Header())
			}
			if download.closeCount() != 1 {
				t.Fatalf("download close count = %d", download.closeCount())
			}
		})
	}
}

func testExportJob(id string, format exportjobs.Format, state exportjobs.State) exportjobs.Job {
	job := exportjobs.Job{
		ID: id, Version: 3, SearchJobID: "search-1", Format: format, Columns: []string{"_time", "message"},
		RowLimit: 77, ByteLimit: 8_192, State: state,
		Progress:  exportjobs.Progress{RowsWritten: 7, BytesWritten: 321, UpdatedAt: testNow.Add(-2 * time.Second)},
		CreatedAt: testNow.Add(-time.Minute), ExpiresAt: testNow.Add(10 * time.Minute),
	}
	if format == exportjobs.FormatCSV {
		job.CSV.HeaderMode = exportjobs.CSVHeaderDisplayNames
	} else {
		job.JSONLines.IntegerEncoding = exportjobs.JSONIntegerNumberWhenSafe
	}
	if state != exportjobs.StateQueued {
		job.StartedAt = testNow.Add(-30 * time.Second)
	}
	if state == exportjobs.StateCompleted || state == exportjobs.StateFailed || state == exportjobs.StateCanceled || state == exportjobs.StateExpired {
		job.FinishedAt = testNow.Add(-time.Second)
	}
	return job
}

func csvExportDefinition(searchJobID string) *opensplunkv1.ExportDefinition {
	return &opensplunkv1.ExportDefinition{
		SearchJobId: searchJobID,
		FormatOptions: &opensplunkv1.ExportDefinition_Csv{Csv: &opensplunkv1.CsvExportOptions{
			HeaderMode: opensplunkv1.CsvHeaderMode_CSV_HEADER_MODE_FIELD_NAMES,
		}},
	}
}

func assertExportScope(t *testing.T, scope searchjobs.AccessScope, tenantID, ownerID string) {
	t.Helper()
	if scope.TenantID != tenantID || scope.OwnerID != ownerID {
		t.Fatalf("scope = %+v, want tenant %q owner %q", scope, tenantID, ownerID)
	}
}

func equalStrings(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}

func countServerFeature(features []opensplunkv1.ServerFeature, target opensplunkv1.ServerFeature) int {
	count := 0
	for _, feature := range features {
		if feature == target {
			count++
		}
	}
	return count
}
