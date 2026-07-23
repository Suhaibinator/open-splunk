package server

import (
	"bytes"
	"context"
	"errors"
	"io"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"testing"
	"testing/fstest"
	"time"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/savedobjects"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
)

var testNow = time.Date(2026, 7, 22, 12, 0, 0, 123_000_000, time.UTC)

type fakeSearchJobs struct {
	mu sync.Mutex

	createRequest  searchjobs.CreateRequest
	createJob      searchjobs.Job
	createErr      error
	createCalls    int
	getJob         searchjobs.Job
	getErr         error
	getScope       searchjobs.AccessScope
	getID          string
	resultsPage    searchjobs.ResultPage
	resultsErr     error
	resultsScope   searchjobs.AccessScope
	resultsID      string
	resultsRequest searchjobs.PageRequest
	cancelErr      error
	cancelScope    searchjobs.AccessScope
	cancelID       string
}

// cancelOnSuccessfulSearchJobs simulates the request being canceled in the
// narrow window after a mutation committed but before its handler returned.
// Successful mutations remain authoritative even though the response write
// may subsequently fail at the transport boundary.
type cancelOnSuccessfulSearchJobs struct {
	*fakeSearchJobs
	cancelCreate context.CancelFunc
	cancelJob    context.CancelFunc
}

func (jobs *cancelOnSuccessfulSearchJobs) Create(ctx context.Context, request searchjobs.CreateRequest) (searchjobs.Job, error) {
	job, err := jobs.fakeSearchJobs.Create(ctx, request)
	if err == nil && jobs.cancelCreate != nil {
		jobs.cancelCreate()
	}
	return job, err
}

func (jobs *cancelOnSuccessfulSearchJobs) CancelFor(scope searchjobs.AccessScope, id string) error {
	err := jobs.fakeSearchJobs.CancelFor(scope, id)
	if err == nil && jobs.cancelJob != nil {
		jobs.cancelJob()
	}
	return err
}

func (jobs *fakeSearchJobs) Create(_ context.Context, request searchjobs.CreateRequest) (searchjobs.Job, error) {
	jobs.mu.Lock()
	defer jobs.mu.Unlock()
	jobs.createCalls++
	jobs.createRequest = request
	return jobs.createJob, jobs.createErr
}

func (jobs *fakeSearchJobs) GetFor(scope searchjobs.AccessScope, id string) (searchjobs.Job, error) {
	jobs.mu.Lock()
	defer jobs.mu.Unlock()
	jobs.getScope = scope
	jobs.getID = id
	return jobs.getJob, jobs.getErr
}

func (jobs *fakeSearchJobs) ResultsFor(scope searchjobs.AccessScope, id string, request searchjobs.PageRequest) (searchjobs.ResultPage, error) {
	jobs.mu.Lock()
	defer jobs.mu.Unlock()
	jobs.resultsScope = scope
	jobs.resultsID = id
	jobs.resultsRequest = request
	return jobs.resultsPage, jobs.resultsErr
}

func (jobs *fakeSearchJobs) CancelFor(scope searchjobs.AccessScope, id string) error {
	jobs.mu.Lock()
	defer jobs.mu.Unlock()
	jobs.cancelScope = scope
	jobs.cancelID = id
	return jobs.cancelErr
}

type fakeIndexCatalog struct {
	indexes []control.Index
	err     error
}

type fakeSearchWebSocket struct {
	mu                   sync.Mutex
	calls                int
	maximumSubscriptions uint32
	maximumFrameBytes    uint64
	closed               bool
}

func (socket *fakeSearchWebSocket) ServeHTTP(response http.ResponseWriter, _ *http.Request) {
	socket.mu.Lock()
	socket.calls++
	socket.mu.Unlock()
	response.WriteHeader(http.StatusNoContent)
}

func (socket *fakeSearchWebSocket) MaximumSubscriptions() uint32 {
	return socket.maximumSubscriptions
}

func (socket *fakeSearchWebSocket) MaximumFrameBytes() uint64 { return socket.maximumFrameBytes }

func (socket *fakeSearchWebSocket) callCount() int {
	socket.mu.Lock()
	defer socket.mu.Unlock()
	return socket.calls
}

func (socket *fakeSearchWebSocket) Close(context.Context) error {
	socket.mu.Lock()
	socket.closed = true
	socket.mu.Unlock()
	return nil
}

func (socket *fakeSearchWebSocket) wasClosed() bool {
	socket.mu.Lock()
	defer socket.mu.Unlock()
	return socket.closed
}

type blockingResultJobs struct {
	*fakeSearchJobs
	entered chan struct{}
	release chan struct{}
}

type delayedResultJobs struct {
	*fakeSearchJobs
	delay    time.Duration
	finished chan struct{}
}

func (jobs *delayedResultJobs) ResultsFor(scope searchjobs.AccessScope, id string, request searchjobs.PageRequest) (searchjobs.ResultPage, error) {
	time.Sleep(jobs.delay)
	close(jobs.finished)
	return jobs.fakeSearchJobs.ResultsFor(scope, id, request)
}

type blockingRequestBody struct {
	started chan struct{}
	release chan struct{}
}

type oneByteReader struct {
	data []byte
}

func (reader *oneByteReader) Read(destination []byte) (int, error) {
	if len(reader.data) == 0 {
		return 0, io.EOF
	}
	if len(destination) == 0 {
		return 0, nil
	}
	destination[0] = reader.data[0]
	reader.data = reader.data[1:]
	return 1, nil
}

func (body *blockingRequestBody) Read([]byte) (int, error) {
	close(body.started)
	<-body.release
	return 0, io.EOF
}

type deadlineIndexCatalog struct{}

func (deadlineIndexCatalog) ListIndexes(ctx context.Context) ([]control.Index, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

func (deadlineIndexCatalog) GetIndexByName(ctx context.Context, _ string) (control.Index, error) {
	<-ctx.Done()
	return control.Index{}, ctx.Err()
}

func (jobs *blockingResultJobs) ResultsFor(scope searchjobs.AccessScope, id string, request searchjobs.PageRequest) (searchjobs.ResultPage, error) {
	jobs.entered <- struct{}{}
	<-jobs.release
	return jobs.fakeSearchJobs.ResultsFor(scope, id, request)
}

func (catalog fakeIndexCatalog) ListIndexes(context.Context) ([]control.Index, error) {
	return append([]control.Index(nil), catalog.indexes...), catalog.err
}

func (catalog fakeIndexCatalog) GetIndexByName(_ context.Context, name string) (control.Index, error) {
	if catalog.err != nil {
		return control.Index{}, catalog.err
	}
	for _, index := range catalog.indexes {
		if index.Definition.Name == name {
			return index, nil
		}
	}
	return control.Index{}, control.ErrNotFound
}

func TestBootstrapUsesProtobufAndLiveIndexes(t *testing.T) {
	app := &opensplunkv1.AppSummary{AppId: "app-1", Slug: "main", DisplayName: "Main"}
	handler := newTestHandler(t, Config{
		SearchJobs: &fakeSearchJobs{},
		Indexes: fakeIndexCatalog{indexes: []control.Index{{
			ID: "idx-1", Definition: control.IndexDefinition{Name: "main", DisplayName: "Main", SearchEnabled: true}, State: control.IndexStateActive,
		}}},
		WebUI:     testUI(),
		Bootstrap: BootstrapConfig{ServerVersion: "test", Apps: []*opensplunkv1.AppSummary{app}},
		Now:       func() time.Time { return testNow },
	})

	response := postProto(t, handler, "/api/v1/system/bootstrap", &opensplunkv1.GetSystemBootstrapRequest{})
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
	}
	if got := response.Header().Get("Content-Type"); got != "application/x-protobuf" {
		t.Fatalf("Content-Type = %q", got)
	}
	var decoded opensplunkv1.GetSystemBootstrapResponse
	unmarshalResponse(t, response, &decoded)
	if decoded.GetServerVersion() != "test" || decoded.GetLimits().GetMaximumPageSize() != defaultMaximumPageSize {
		t.Fatalf("bootstrap = %+v", &decoded)
	}
	if !slices.Contains(decoded.GetFeatures(), opensplunkv1.ServerFeature_SERVER_FEATURE_SAVED_SEARCHES) {
		t.Fatalf("bootstrap features = %v, want saved searches", decoded.GetFeatures())
	}
	if len(decoded.GetIndexes()) != 1 || decoded.GetIndexes()[0].GetName() != "main" {
		t.Fatalf("indexes = %+v", decoded.GetIndexes())
	}
	if decoded.GetSelectedAppId() != "app-1" {
		t.Fatalf("selected app = %q", decoded.GetSelectedAppId())
	}
	if !decoded.GetServerTime().AsTime().Equal(testNow) {
		t.Fatalf("server time = %s", decoded.GetServerTime().AsTime())
	}
	// Constructor input is detached.
	app.DisplayName = "mutated"
	response = postProto(t, handler, "/api/v1/system/bootstrap", &opensplunkv1.GetSystemBootstrapRequest{})
	unmarshalResponse(t, response, &decoded)
	if decoded.GetApps()[0].GetDisplayName() != "Main" {
		t.Fatalf("app alias leaked: %+v", decoded.GetApps()[0])
	}
}

func TestSearchWebSocketRouteAndBootstrapUseServiceLimits(t *testing.T) {
	socket := &fakeSearchWebSocket{maximumSubscriptions: 32, maximumFrameBytes: 64 << 10}
	handler := newTestHandler(t, Config{
		SearchJobs:      &fakeSearchJobs{},
		SearchWebSocket: socket,
		Indexes:         fakeIndexCatalog{},
		WebUI:           testUI(),
		Bootstrap: BootstrapConfig{Features: []opensplunkv1.ServerFeature{
			opensplunkv1.ServerFeature_SERVER_FEATURE_SEARCH,
			opensplunkv1.ServerFeature_SERVER_FEATURE_SEARCH_PREVIEW,
			opensplunkv1.ServerFeature_SERVER_FEATURE_FIELD_DISCOVERY,
			opensplunkv1.ServerFeature_SERVER_FEATURE_TIMELINE,
			opensplunkv1.ServerFeature_SERVER_FEATURE_PLAN_INSPECTION,
		}},
	})

	bootstrapResponse := postProto(t, handler, "/api/v1/system/bootstrap", &opensplunkv1.GetSystemBootstrapRequest{})
	if bootstrapResponse.Code != http.StatusOK {
		t.Fatalf("bootstrap status = %d, body = %s", bootstrapResponse.Code, bootstrapResponse.Body.String())
	}
	var bootstrap opensplunkv1.GetSystemBootstrapResponse
	unmarshalResponse(t, bootstrapResponse, &bootstrap)
	if bootstrap.GetSearchWebsocketPath() != searchWebSocketPath {
		t.Fatalf("websocket path = %q, want %q", bootstrap.GetSearchWebsocketPath(), searchWebSocketPath)
	}
	if bootstrap.GetLimits().GetMaximumWebsocketSubscriptions() != socket.MaximumSubscriptions() ||
		bootstrap.GetLimits().GetMaximumWebsocketFrameBytes() != socket.MaximumFrameBytes() {
		t.Fatalf("websocket limits = %+v", bootstrap.GetLimits())
	}
	if bootstrap.GetLimits().GetMaximumPreviewRows() != 0 {
		t.Fatalf("unsupported preview was advertised: %+v", &bootstrap)
	}
	for _, unsupported := range []opensplunkv1.ServerFeature{
		opensplunkv1.ServerFeature_SERVER_FEATURE_SEARCH_PREVIEW,
		opensplunkv1.ServerFeature_SERVER_FEATURE_FIELD_DISCOVERY,
		opensplunkv1.ServerFeature_SERVER_FEATURE_TIMELINE,
		opensplunkv1.ServerFeature_SERVER_FEATURE_PLAN_INSPECTION,
	} {
		if slices.Contains(bootstrap.GetFeatures(), unsupported) {
			t.Fatalf("unsupported feature %s was advertised", unsupported)
		}
	}

	request := httptest.NewRequest(http.MethodGet, searchWebSocketPath, nil)
	request.Host = "example.com"
	request.Header.Set("Origin", "http://example.com")
	request.Header.Set("Sec-Fetch-Site", "same-origin")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusNoContent || socket.callCount() != 1 {
		t.Fatalf("websocket response = %d calls = %d", response.Code, socket.callCount())
	}
	// This fake raw handler returns an ordinary HTTP response, so the assertion
	// covers the route's non-upgrade cache boundary. Gorilla writes a successful
	// 101 after hijacking; 101 responses are not cacheable and do not promise
	// these ordinary-response headers.
	if response.Header().Get("Cache-Control") != "no-store" {
		t.Fatalf("websocket cache policy = %q", response.Header().Get("Cache-Control"))
	}

	for name, authority := range map[string]string{
		"foreign origin": "http://attacker.example",
		"empty query":    "http://example.com?",
		"empty fragment": "http://example.com#",
	} {
		t.Run(name, func(t *testing.T) {
			request := httptest.NewRequest(http.MethodGet, searchWebSocketPath, nil)
			request.Host = "example.com"
			request.Header.Set("Origin", authority)
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, request)
			if response.Code != http.StatusForbidden || socket.callCount() != 1 {
				t.Fatalf("untrusted-origin response = %d calls = %d", response.Code, socket.callCount())
			}
		})
	}
	request = httptest.NewRequest(http.MethodGet, searchWebSocketPath, nil)
	request.Host = "example.com:"
	request.Header.Set("Origin", "http://example.com:")
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusForbidden || socket.callCount() != 1 {
		t.Fatalf("empty-port response = %d calls = %d", response.Code, socket.callCount())
	}

	request = httptest.NewRequest(http.MethodPost, searchWebSocketPath, nil)
	request.Host = "example.com"
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusMethodNotAllowed || response.Header().Get("Allow") != http.MethodGet || socket.callCount() != 1 {
		t.Fatalf("wrong-method response = %d allow = %q calls = %d", response.Code, response.Header().Get("Allow"), socket.callCount())
	}
	if err := handler.Close(context.Background()); err != nil || !socket.wasClosed() {
		t.Fatalf("handler close error = %v, socket closed = %v", err, socket.wasClosed())
	}
}

func TestSearchWebSocketLimitsAreValidated(t *testing.T) {
	tests := []struct {
		name   string
		socket *fakeSearchWebSocket
		want   string
	}{
		{name: "zero subscriptions", socket: &fakeSearchWebSocket{maximumFrameBytes: 1}, want: "websocket subscriptions"},
		{name: "too many subscriptions", socket: &fakeSearchWebSocket{maximumSubscriptions: maximumWebSocketSubscriptions + 1, maximumFrameBytes: 1}, want: "websocket subscriptions"},
		{name: "zero frame bytes", socket: &fakeSearchWebSocket{maximumSubscriptions: 1}, want: "websocket frame bytes"},
		{name: "unusable frame bytes", socket: &fakeSearchWebSocket{maximumSubscriptions: 1, maximumFrameBytes: minimumWebSocketFrameBytes - 1}, want: "websocket frame bytes"},
		{name: "too many frame bytes", socket: &fakeSearchWebSocket{maximumSubscriptions: 1, maximumFrameBytes: maximumWebSocketFrameBytes + 1}, want: "websocket frame bytes"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := NewHandler(Config{
				SearchJobs: &fakeSearchJobs{}, SearchWebSocket: test.socket, Indexes: fakeIndexCatalog{},
				SavedSearches: &fakeSavedSearches{}, WebUI: testUI(), AdministrativeAllowedHosts: []string{"example.com"},
			})
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("NewHandler error = %v, want %q", err, test.want)
			}
		})
	}

	var typedNil *fakeSearchWebSocket
	if _, err := NewHandler(Config{
		SearchJobs: &fakeSearchJobs{}, SearchWebSocket: typedNil, Indexes: fakeIndexCatalog{},
		SavedSearches: &fakeSavedSearches{}, WebUI: testUI(),
	}); err != nil {
		t.Fatalf("typed-nil websocket should be disabled: %v", err)
	}
}

func TestSearchWebSocketIsNotAdvertisedOrRoutedWithoutService(t *testing.T) {
	handler := newTestHandler(t, Config{SearchJobs: &fakeSearchJobs{}, Indexes: fakeIndexCatalog{}, WebUI: testUI()})
	bootstrapResponse := postProto(t, handler, "/api/v1/system/bootstrap", &opensplunkv1.GetSystemBootstrapRequest{})
	var bootstrap opensplunkv1.GetSystemBootstrapResponse
	unmarshalResponse(t, bootstrapResponse, &bootstrap)
	if bootstrap.GetSearchWebsocketPath() != "" || bootstrap.GetLimits().GetMaximumWebsocketSubscriptions() != 0 ||
		bootstrap.GetLimits().GetMaximumWebsocketFrameBytes() != 0 {
		t.Fatalf("disabled websocket was advertised: %+v", &bootstrap)
	}

	request := httptest.NewRequest(http.MethodGet, searchWebSocketPath, nil)
	request.Host = "example.com"
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusNotFound {
		t.Fatalf("disabled websocket status = %d, body = %s", response.Code, response.Body.String())
	}
}

func TestBrowserAPIRoutesDefaultToLoopbackWithoutAdministration(t *testing.T) {
	handler, err := NewHandler(Config{
		SearchJobs: &fakeSearchJobs{}, Indexes: fakeIndexCatalog{}, SavedSearches: &fakeSavedSearches{}, WebUI: testUI(),
	})
	if err != nil {
		t.Fatalf("NewHandler: %v", err)
	}

	response := postProtoHeaders(t, handler, "/api/v1/system/bootstrap", &opensplunkv1.GetSystemBootstrapRequest{}, map[string]string{
		"Host": "attacker.example", "Origin": "http://attacker.example",
	})
	if response.Code != http.StatusForbidden {
		t.Fatalf("non-loopback default status = %d, body = %s", response.Code, response.Body.String())
	}

	response = postProtoHeaders(t, handler, "/api/v1/system/bootstrap", &opensplunkv1.GetSystemBootstrapRequest{}, map[string]string{
		"Host": "[::1]:8080", "Origin": "http://[::1]:8080", "Sec-Fetch-Site": "same-origin",
	})
	if response.Code != http.StatusOK {
		t.Fatalf("IPv6 loopback status = %d, body = %s", response.Code, response.Body.String())
	}

	response = postProtoHeaders(t, handler, "/api/v1/system/bootstrap", &opensplunkv1.GetSystemBootstrapRequest{}, map[string]string{
		"Host": "localhost:8080",
	})
	if response.Code != http.StatusOK {
		t.Fatalf("origin-less loopback status = %d, body = %s", response.Code, response.Body.String())
	}

	_, err = NewHandler(Config{
		SearchJobs: &fakeSearchJobs{}, Indexes: fakeIndexCatalog{}, SavedSearches: &fakeSavedSearches{}, WebUI: testUI(),
		AdministrativeAllowedHosts: []string{"invalid.example/path"},
	})
	if err == nil || !strings.Contains(err.Error(), "browser allowed host is invalid") {
		t.Fatalf("invalid allowed host error = %v", err)
	}
}

func TestProtobufMediaTypeMethodAndBodyLimit(t *testing.T) {
	handler := newTestHandler(t, Config{
		SearchJobs:          &fakeSearchJobs{},
		Indexes:             fakeIndexCatalog{},
		WebUI:               testUI(),
		MaximumRequestBytes: 32,
	})

	request := httptest.NewRequest(http.MethodPost, "/api/v1/system/bootstrap", bytes.NewReader(nil))
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusUnsupportedMediaType {
		t.Fatalf("wrong media status = %d", response.Code)
	}

	request = httptest.NewRequest(http.MethodGet, "/api/v1/system/bootstrap", nil)
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusMethodNotAllowed {
		t.Fatalf("wrong method status = %d, body = %s", response.Code, response.Body.String())
	}
	if response.Header().Get("Content-Type") != "application/json; charset=utf-8" || response.Header().Get("Allow") != http.MethodPost {
		t.Fatalf("method error headers = %v", response.Header())
	}

	large := &opensplunkv1.GetSystemBootstrapRequest{PreferredAppId: stringPointer(strings.Repeat("a", 128))}
	response = postProto(t, handler, "/api/v1/system/bootstrap", large)
	if response.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("large body status = %d, body = %s", response.Code, response.Body.String())
	}

	exact := &opensplunkv1.GetSystemBootstrapRequest{PreferredAppId: stringPointer(strings.Repeat("a", 30))}
	if size := proto.Size(exact); size != 32 {
		t.Fatalf("exact request size = %d, want 32", size)
	}
	response = postProto(t, handler, "/api/v1/system/bootstrap", exact)
	if response.Code != http.StatusOK {
		t.Fatalf("exact-limit body status = %d, body = %s", response.Code, response.Body.String())
	}

	payload, err := proto.Marshal(large)
	if err != nil {
		t.Fatal(err)
	}
	request = httptest.NewRequest(http.MethodPost, "/api/v1/system/bootstrap", &oneByteReader{data: payload})
	request.Header.Set("Content-Type", "application/x-protobuf")
	if request.ContentLength != -1 {
		t.Fatalf("chunked request ContentLength = %d, want -1", request.ContentLength)
	}
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("chunked large body status = %d, body = %s", response.Code, response.Body.String())
	}
}

func TestProtobufUnknownFieldsRemainForwardCompatible(t *testing.T) {
	handler := newTestHandler(t, Config{
		SearchJobs: &fakeSearchJobs{}, Indexes: fakeIndexCatalog{}, WebUI: testUI(),
	})
	payload := protowire.AppendTag(nil, 2_047, protowire.BytesType)
	payload = protowire.AppendString(payload, "future-client-field")
	request := httptest.NewRequest(http.MethodPost, "/api/v1/system/bootstrap", bytes.NewReader(payload))
	request.Header.Set("Content-Type", "application/x-protobuf")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
	}
}

func TestCreateSearchResolvesTimeIntentAndValidatesIndexScope(t *testing.T) {
	created := completeJob("job-1")
	jobs := &fakeSearchJobs{createJob: created}
	handler := newTestHandler(t, Config{
		SearchJobs: jobs,
		Indexes: fakeIndexCatalog{indexes: []control.Index{
			{ID: "idx-main", Definition: control.IndexDefinition{Name: "main", DisplayName: "Main", SearchEnabled: true}, State: control.IndexStateActive},
			{ID: "idx-disabled", Definition: control.IndexDefinition{Name: "disabled", DisplayName: "Disabled", SearchEnabled: false}, State: control.IndexStateActive},
		}},
		WebUI:    testUI(),
		OwnerID:  "owner-1",
		TenantID: "tenant-1",
		Now:      func() time.Time { return testNow },
	})

	valid := createRequest("2026-07-22T10:00:00-02:00", "2026-07-22T13:00:00Z", "MAIN")
	valid.Definition.Spl = " \nindex=main | head 10\t"
	response := postProto(t, handler, "/api/v1/search/jobs/create", valid)
	if response.Code != http.StatusOK {
		t.Fatalf("create status = %d, body = %s", response.Code, response.Body.String())
	}
	jobs.mu.Lock()
	captured := jobs.createRequest
	jobs.mu.Unlock()
	if captured.OwnerID != "owner-1" || captured.TenantID != "tenant-1" {
		t.Fatalf("scope = owner %q tenant %q", captured.OwnerID, captured.TenantID)
	}
	if len(captured.RequestedIndexes) != 1 || captured.RequestedIndexes[0] != "main" {
		t.Fatalf("requested indexes = %v", captured.RequestedIndexes)
	}
	if captured.SPL != valid.Definition.Spl {
		t.Fatalf("captured SPL = %q, want original %q", captured.SPL, valid.Definition.Spl)
	}
	if !captured.TimeRange.Earliest().Equal(time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC)) {
		t.Fatalf("earliest = %s", captured.TimeRange.Earliest())
	}

	relative := createRequest(" -24h ", " now ", "main")
	response = postProto(t, handler, "/api/v1/search/jobs/create", relative)
	if response.Code != http.StatusOK {
		t.Fatalf("relative create status = %d, body = %s", response.Code, response.Body.String())
	}
	jobs.mu.Lock()
	captured = jobs.createRequest
	jobs.mu.Unlock()
	intent := captured.TimeRange.Intent()
	if intent.Earliest != "-24h" || intent.Latest != "now" ||
		intent.Timezone != "UTC" || !intent.TimezoneSpecified ||
		!captured.TimeRange.Earliest().Equal(testNow.Add(-24*time.Hour)) || !captured.TimeRange.Latest().Equal(testNow) {
		t.Fatalf("relative request = intent %+v resolved [%s, %s)", intent, captured.TimeRange.Earliest(), captured.TimeRange.Latest())
	}

	tests := []struct {
		name       string
		request    *opensplunkv1.CreateSearchJobRequest
		wantStatus int
	}{
		{name: "inverted time", request: createRequest("2026-07-22T14:00:00Z", "2026-07-22T13:00:00Z", "main"), wantStatus: http.StatusBadRequest},
		{name: "outside storage range", request: createRequest("1500-01-01T00:00:00Z", "2026-07-22T13:00:00Z", "main"), wantStatus: http.StatusBadRequest},
		{name: "no index", request: createRequest("2026-07-22T12:00:00Z", "2026-07-22T13:00:00Z"), wantStatus: http.StatusBadRequest},
		{name: "disabled index", request: createRequest("2026-07-22T12:00:00Z", "2026-07-22T13:00:00Z", "disabled"), wantStatus: http.StatusForbidden},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			response := postProto(t, handler, "/api/v1/search/jobs/create", test.request)
			if response.Code != test.wantStatus {
				t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
			}
		})
	}
}

func TestCreateSearchPreservesScopedSavedSearchProvenance(t *testing.T) {
	t.Parallel()

	const (
		ownerID = "owner-1"
		appID   = "app-main"
		savedID = "saved-1"
	)
	store := &fakeSavedSearches{getFn: func(_ context.Context, scope savedobjects.AccessScope, id string) (*opensplunkv1.SavedSearch, error) {
		if scope.OwnerID != ownerID || id != savedID {
			t.Fatalf("saved-search lookup = scope %+v ID %q", scope, id)
		}
		return savedSearchRecord(savedID, 1, ownerID, appID, "Errors"), nil
	}}
	jobs := &fakeSearchJobs{createJob: completeJob("job-saved")}
	handler := newTestHandler(t, Config{
		SearchJobs: jobs,
		Indexes: fakeIndexCatalog{indexes: []control.Index{{
			ID: "idx-main", Definition: control.IndexDefinition{Name: "main", SearchEnabled: true}, State: control.IndexStateActive,
		}}},
		SavedSearches: store,
		WebUI:         testUI(),
		OwnerID:       ownerID,
		TenantID:      "tenant-1",
		Now:           func() time.Time { return testNow },
	})

	request := createRequest("-24h", "now", "main")
	request.Source = &opensplunkv1.SearchJobSource{
		Origin:        opensplunkv1.SearchJobOrigin_SEARCH_JOB_ORIGIN_SAVED_SEARCH,
		SavedSearchId: stringPointer(savedID),
	}
	response := postProto(t, handler, "/api/v1/search/jobs/create", request)
	if response.Code != http.StatusOK {
		t.Fatalf("saved-source create status = %d, body = %s", response.Code, response.Body.String())
	}
	jobs.mu.Lock()
	captured := jobs.createRequest
	createCalls := jobs.createCalls
	jobs.mu.Unlock()
	intent := captured.TimeRange.Intent()
	if createCalls != 1 || captured.AppID != appID || captured.Source.Origin != searchjobs.JobOriginSavedSearch ||
		captured.Source.ObjectID != savedID || intent.Earliest != "-24h" || intent.Latest != "now" {
		t.Fatalf("captured saved-source request = %+v", captured)
	}

	mismatch := createRequest("-24h", "now", "main")
	mismatch.Definition.AppId = stringPointer("other-app")
	mismatch.Source = request.Source
	response = postProto(t, handler, "/api/v1/search/jobs/create", mismatch)
	if response.Code != http.StatusBadRequest {
		t.Fatalf("cross-app source status = %d, body = %s", response.Code, response.Body.String())
	}
	jobs.mu.Lock()
	createCalls = jobs.createCalls
	jobs.mu.Unlock()
	if createCalls != 1 {
		t.Fatalf("cross-app source created %d jobs, want 1 prior successful call", createCalls)
	}
}

func TestCreateSearchRejectsNoncanonicalSavedSearchProvenance(t *testing.T) {
	t.Parallel()

	record := savedSearchRecord("saved-1", 1, "owner-1", "app-main", "Errors")
	record.Definition.Search.AppId = stringPointer(" app-main ")
	store := &fakeSavedSearches{getFn: func(context.Context, savedobjects.AccessScope, string) (*opensplunkv1.SavedSearch, error) {
		return record, nil
	}}
	jobs := &fakeSearchJobs{createJob: completeJob("job-never-created")}
	handler := newTestHandler(t, Config{
		SearchJobs: jobs,
		Indexes: fakeIndexCatalog{indexes: []control.Index{{
			ID: "idx-main", Definition: control.IndexDefinition{Name: "main", SearchEnabled: true}, State: control.IndexStateActive,
		}}},
		SavedSearches: store,
		WebUI:         testUI(),
		OwnerID:       "owner-1",
		TenantID:      "tenant-1",
		Now:           func() time.Time { return testNow },
	})
	request := createRequest("-24h", "now", "main")
	request.Source = &opensplunkv1.SearchJobSource{
		Origin:        opensplunkv1.SearchJobOrigin_SEARCH_JOB_ORIGIN_SAVED_SEARCH,
		SavedSearchId: stringPointer("saved-1"),
	}
	response := postProto(t, handler, "/api/v1/search/jobs/create", request)
	if response.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
	}
	jobs.mu.Lock()
	createCalls := jobs.createCalls
	jobs.mu.Unlock()
	if createCalls != 0 {
		t.Fatalf("malformed provenance created %d jobs", createCalls)
	}
}

func TestCreateSearchRejectsUnsupportedSemanticsBeforeCreatingJob(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*opensplunkv1.CreateSearchJobRequest)
	}{
		{name: "client request ID", mutate: func(request *opensplunkv1.CreateSearchJobRequest) { request.ClientRequestId = stringPointer("") }},
		{name: "source ID without origin", mutate: func(request *opensplunkv1.CreateSearchJobRequest) {
			request.Source = &opensplunkv1.SearchJobSource{SavedSearchId: stringPointer("saved-1")}
		}},
		{name: "saved search without ID", mutate: func(request *opensplunkv1.CreateSearchJobRequest) {
			request.Source = &opensplunkv1.SearchJobSource{Origin: opensplunkv1.SearchJobOrigin_SEARCH_JOB_ORIGIN_SAVED_SEARCH}
		}},
		{name: "history source", mutate: func(request *opensplunkv1.CreateSearchJobRequest) {
			request.Source = &opensplunkv1.SearchJobSource{HistorySearchId: stringPointer("")}
		}},
		{name: "dashboard source", mutate: func(request *opensplunkv1.CreateSearchJobRequest) {
			request.Source = &opensplunkv1.SearchJobSource{DashboardId: stringPointer("")}
		}},
		{name: "preview", mutate: func(request *opensplunkv1.CreateSearchJobRequest) {
			request.Options = &opensplunkv1.SearchJobOptions{EnablePreview: true}
		}},
		{name: "field discovery", mutate: func(request *opensplunkv1.CreateSearchJobRequest) {
			request.Options = &opensplunkv1.SearchJobOptions{EnableFieldDiscovery: true}
		}},
		{name: "timeline", mutate: func(request *opensplunkv1.CreateSearchJobRequest) {
			request.Options = &opensplunkv1.SearchJobOptions{EnableTimeline: true}
		}},
		{name: "preview row limit", mutate: func(request *opensplunkv1.CreateSearchJobRequest) {
			limit := uint32(0)
			request.Options = &opensplunkv1.SearchJobOptions{PreviewRowLimit: &limit}
		}},
		{name: "preferred result tab", mutate: func(request *opensplunkv1.CreateSearchJobRequest) {
			request.Definition.PreferredResultTab = opensplunkv1.SearchResultTab_SEARCH_RESULT_TAB_EVENTS
		}},
		{name: "selected fields", mutate: func(request *opensplunkv1.CreateSearchJobRequest) {
			request.Definition.SelectedFields = []string{"message"}
		}},
		{name: "visualization", mutate: func(request *opensplunkv1.CreateSearchJobRequest) {
			request.Definition.Visualization = &opensplunkv1.VisualizationSpec{}
		}},
		{name: "SPL NUL", mutate: func(request *opensplunkv1.CreateSearchJobRequest) {
			request.Definition.Spl = "index=main\x00 | head 1"
		}},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			jobs := &fakeSearchJobs{createJob: completeJob("job-1")}
			handler := newTestHandler(t, Config{
				SearchJobs: jobs,
				Indexes: fakeIndexCatalog{indexes: []control.Index{{
					ID: "idx-main", Definition: control.IndexDefinition{Name: "main", SearchEnabled: true}, State: control.IndexStateActive,
				}}},
				WebUI: testUI(),
			})
			request := createRequest("2026-07-22T12:00:00Z", "2026-07-22T13:00:00Z", "main")
			test.mutate(request)
			response := postProto(t, handler, "/api/v1/search/jobs/create", request)
			if response.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
			}
			jobs.mu.Lock()
			calls := jobs.createCalls
			jobs.mu.Unlock()
			if calls != 0 {
				t.Fatalf("Create calls = %d, want 0", calls)
			}
		})
	}
}

func TestSearchErrorMappingDoesNotExposeStorageDetails(t *testing.T) {
	jobs := &fakeSearchJobs{getErr: errors.New("SELECT password FROM sqlite_secret")}
	handler := newTestHandler(t, Config{SearchJobs: jobs, Indexes: fakeIndexCatalog{}, WebUI: testUI()})
	response := postProto(t, handler, "/api/v1/search/jobs/get", &opensplunkv1.GetSearchJobRequest{SearchJobId: "job-1"})
	if response.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d", response.Code)
	}
	if strings.Contains(response.Body.String(), "SELECT") || strings.Contains(response.Body.String(), "password") {
		t.Fatalf("response exposed storage detail: %s", response.Body.String())
	}

	statusTests := []struct {
		err  error
		want int
	}{
		{searchjobs.ErrNotFound, http.StatusNotFound},
		{searchjobs.ErrExpired, http.StatusGone},
		{searchjobs.ErrResultsNotReady, http.StatusConflict},
		{searchjobs.ErrInvalidCursor, http.StatusBadRequest},
		{searchjobs.ErrClosed, http.StatusServiceUnavailable},
		{searchjobs.ErrJournalUnavailable, http.StatusServiceUnavailable},
	}
	for _, test := range statusTests {
		jobs.getErr = test.err
		response = postProto(t, handler, "/api/v1/search/jobs/get", &opensplunkv1.GetSearchJobRequest{SearchJobId: "job-1"})
		if response.Code != test.want {
			t.Fatalf("error %v status = %d, want %d", test.err, response.Code, test.want)
		}
	}
}

func TestResultsAreBoundedAndTyped(t *testing.T) {
	decimal, err := searchjobs.DecimalValue("1234567890.012300")
	if err != nil {
		t.Fatal(err)
	}
	object, err := searchjobs.ObjectValue(searchjobs.ObjectField{Name: "ok", Value: searchjobs.BoolValue(true)})
	if err != nil {
		t.Fatal(err)
	}
	job := completeJob("job-1")
	job.SPL = "index=main | table message count decimal payload"
	jobs := &fakeSearchJobs{getJob: job, resultsPage: searchjobs.ResultPage{
		Schema: searchjobs.Schema{Columns: []searchjobs.Column{
			{Name: "message", Kind: searchjobs.ValueKindString},
			{Name: "count", Kind: searchjobs.ValueKindUnsigned},
			{Name: "decimal", Kind: searchjobs.ValueKindDecimal},
			{Name: "payload", Kind: searchjobs.ValueKindObject},
		}},
		Rows: []searchjobs.ResultRow{{Ordinal: 7, Values: []searchjobs.Value{
			searchjobs.StringValue("hello"), searchjobs.UnsignedValue(9), decimal, object,
		}}},
		NextCursor: "next-token",
		TotalRows:  11,
	}}
	handler := newTestHandler(t, Config{SearchJobs: jobs, Indexes: fakeIndexCatalog{}, WebUI: testUI(), MaximumPageSize: 50})
	pageSize := uint32(10)
	response := postProto(t, handler, "/api/v1/search/jobs/results", &opensplunkv1.GetSearchResultsRequest{
		SearchJobId: "job-1",
		Page:        &opensplunkv1.PageRequest{PageSize: &pageSize, IncludeTotalSize: true},
	})
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
	}
	var decoded opensplunkv1.GetSearchResultsResponse
	unmarshalResponse(t, response, &decoded)
	page := decoded.GetResultPage()
	if len(page.GetSchema().GetColumns()) != 4 || page.GetSchema().GetColumns()[0].GetFieldName() != "message" {
		t.Fatalf("schema = %+v", page.GetSchema())
	}
	if page.GetSchema().GetResultKind() != opensplunkv1.ResultSetKind_RESULT_SET_KIND_STATISTICS {
		t.Fatalf("result kind = %v, want statistics", page.GetSchema().GetResultKind())
	}
	if got := page.GetRows()[0].GetCells()[1].GetUint64Value(); got != 9 {
		t.Fatalf("uint cell = %d", got)
	}
	if got := page.GetRows()[0].GetCells()[3].GetObjectValue().GetFields()[0].GetName(); got != "ok" {
		t.Fatalf("object cell field = %q", got)
	}
	if page.GetPage().GetTotalSize() != 11 || page.GetPage().GetNextPageToken() != "next-token" {
		t.Fatalf("page metadata = %+v", page.GetPage())
	}
	if !page.GetSnapshotComplete() {
		t.Fatal("completed snapshot was marked partial on a nonfinal page")
	}
	jobs.mu.Lock()
	request := jobs.resultsRequest
	jobs.mu.Unlock()
	if request.Limit != 10 {
		t.Fatalf("manager page limit = %d", request.Limit)
	}

	tooLarge := uint32(51)
	response = postProto(t, handler, "/api/v1/search/jobs/results", &opensplunkv1.GetSearchResultsRequest{
		SearchJobId: "job-1", Page: &opensplunkv1.PageRequest{PageSize: &tooLarge},
	})
	if response.Code != http.StatusBadRequest {
		t.Fatalf("oversized page status = %d", response.Code)
	}

	response = postProto(t, handler, "/api/v1/search/jobs/results", &opensplunkv1.GetSearchResultsRequest{
		SearchJobId: "job-1", Columns: []string{"missing"},
	})
	if response.Code != http.StatusBadRequest {
		t.Fatalf("unknown column status = %d", response.Code)
	}
}

func TestCancelUsesScopedJobAndReturnsSnapshot(t *testing.T) {
	jobs := &fakeSearchJobs{getJob: completeJob("job-1")}
	handler := newTestHandler(t, Config{SearchJobs: jobs, Indexes: fakeIndexCatalog{}, WebUI: testUI(), OwnerID: "owner", TenantID: "tenant"})
	response := postProto(t, handler, "/api/v1/search/jobs/cancel", &opensplunkv1.CancelSearchJobRequest{SearchJobId: "job-1"})
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
	}
	jobs.mu.Lock()
	defer jobs.mu.Unlock()
	if jobs.cancelID != "job-1" || jobs.cancelScope != (searchjobs.AccessScope{OwnerID: "owner", TenantID: "tenant"}) {
		t.Fatalf("cancel = %q %+v", jobs.cancelID, jobs.cancelScope)
	}
}

func TestCommittedSearchMutationsWinContextCancellationRace(t *testing.T) {
	t.Run("create", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		jobs := &cancelOnSuccessfulSearchJobs{
			fakeSearchJobs: &fakeSearchJobs{createJob: completeJob("job-create")},
			cancelCreate:   cancel,
		}
		handler := &apiHandler{
			jobs: jobs,
			indexes: fakeIndexCatalog{indexes: []control.Index{{
				ID: "idx-main", Definition: control.IndexDefinition{Name: "main", SearchEnabled: true}, State: control.IndexStateActive,
			}}},
			ownerID:  "owner-1",
			tenantID: "tenant-1",
			now:      func() time.Time { return testNow },
		}
		request := httptest.NewRequest(http.MethodPost, "/api/v1/search/jobs/create", nil).WithContext(ctx)
		response, err := handler.createSearchJob(request, createRequest(
			"2026-07-22T11:00:00Z", "2026-07-22T12:00:00Z", "main",
		))
		if !errors.Is(ctx.Err(), context.Canceled) {
			t.Fatalf("request context error = %v, want context.Canceled", ctx.Err())
		}
		if err != nil {
			t.Fatalf("committed create returned error = %v", err)
		}
		if response.GetSearchJob().GetSearchJobId() != "job-create" {
			t.Fatalf("created response = %+v", response)
		}
	})

	t.Run("cancel", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		jobs := &cancelOnSuccessfulSearchJobs{
			fakeSearchJobs: &fakeSearchJobs{getJob: completeJob("job-cancel")},
			cancelJob:      cancel,
		}
		handler := &apiHandler{
			jobs:     jobs,
			ownerID:  "owner-1",
			tenantID: "tenant-1",
			now:      func() time.Time { return testNow },
		}
		request := httptest.NewRequest(http.MethodPost, "/api/v1/search/jobs/cancel", nil).WithContext(ctx)
		response, err := handler.cancelSearchJob(request, &opensplunkv1.CancelSearchJobRequest{SearchJobId: "job-cancel"})
		if !errors.Is(ctx.Err(), context.Canceled) {
			t.Fatalf("request context error = %v, want context.Canceled", ctx.Err())
		}
		if err != nil {
			t.Fatalf("committed cancellation returned error = %v", err)
		}
		if response.GetSearchJob().GetSearchJobId() != "job-cancel" {
			t.Fatalf("canceled response = %+v", response)
		}
	})
}

func TestSearchCancellationRejectsAlreadyCanceledRequestBeforeMutation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	jobs := &fakeSearchJobs{getJob: completeJob("job-cancel")}
	handler := &apiHandler{
		jobs:     jobs,
		ownerID:  "owner-1",
		tenantID: "tenant-1",
		now:      func() time.Time { return testNow },
	}
	request := httptest.NewRequest(http.MethodPost, "/api/v1/search/jobs/cancel", nil).WithContext(ctx)
	response, err := handler.cancelSearchJob(request, &opensplunkv1.CancelSearchJobRequest{SearchJobId: "job-cancel"})
	if !errors.Is(err, context.Canceled) || response != nil {
		t.Fatalf("cancel response/error = %+v / %v, want context.Canceled", response, err)
	}
	jobs.mu.Lock()
	canceledID := jobs.cancelID
	jobs.mu.Unlock()
	if canceledID != "" {
		t.Fatalf("already-canceled request mutated job %q", canceledID)
	}
}

func TestResultSerializationConcurrencyIsBounded(t *testing.T) {
	base := &fakeSearchJobs{resultsPage: searchjobs.ResultPage{
		Schema:   searchjobs.Schema{Columns: []searchjobs.Column{{Name: "message", Kind: searchjobs.ValueKindString}}},
		Rows:     []searchjobs.ResultRow{{Values: []searchjobs.Value{searchjobs.StringValue("ok")}}},
		Complete: true,
	}}
	jobs := &blockingResultJobs{fakeSearchJobs: base, entered: make(chan struct{}, 2), release: make(chan struct{})}
	handler := newTestHandler(t, Config{
		SearchJobs: jobs, Indexes: fakeIndexCatalog{}, WebUI: testUI(),
		MaximumConcurrentResponses: 1, RouteTimeout: time.Second,
	})
	payload, err := proto.Marshal(&opensplunkv1.GetSearchResultsRequest{SearchJobId: "job-1"})
	if err != nil {
		t.Fatal(err)
	}
	serve := func() int {
		request := httptest.NewRequest(http.MethodPost, "/api/v1/search/jobs/results", bytes.NewReader(payload))
		request.Header.Set("Content-Type", "application/x-protobuf")
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		return response.Code
	}
	firstDone := make(chan int, 1)
	go func() { firstDone <- serve() }()
	select {
	case <-jobs.entered:
	case <-time.After(time.Second):
		t.Fatal("first result request did not enter the service")
	}
	secondDone := make(chan int, 1)
	go func() { secondDone <- serve() }()
	select {
	case status := <-secondDone:
		if status != http.StatusServiceUnavailable {
			t.Fatalf("second result status = %d, want %d", status, http.StatusServiceUnavailable)
		}
	case <-time.After(time.Second):
		t.Fatal("second result request was not rejected promptly")
	}
	select {
	case <-jobs.entered:
		t.Fatal("rejected result request entered the service")
	default:
	}
	close(jobs.release)
	select {
	case status := <-firstDone:
		if status != http.StatusOK {
			t.Fatalf("first request status = %d", status)
		}
	case <-time.After(time.Second):
		t.Fatal("first request did not finish")
	}
}

func TestSlowResultUploadDoesNotConsumeSerializationCapacity(t *testing.T) {
	base := &fakeSearchJobs{getJob: completeJob("job-1"), resultsPage: searchjobs.ResultPage{
		Schema:   searchjobs.Schema{Columns: []searchjobs.Column{{Name: "message", Kind: searchjobs.ValueKindString}}},
		Rows:     []searchjobs.ResultRow{{Values: []searchjobs.Value{searchjobs.StringValue("ok")}}},
		Complete: true,
	}}
	handler := newTestHandler(t, Config{
		SearchJobs: base, Indexes: fakeIndexCatalog{}, WebUI: testUI(),
		MaximumConcurrentResponses: 1,
	})
	body := &blockingRequestBody{started: make(chan struct{}), release: make(chan struct{})}
	request := httptest.NewRequest(http.MethodPost, "/api/v1/search/jobs/results", body)
	request.Header.Set("Content-Type", "application/x-protobuf")
	slowDone := make(chan int, 1)
	go func() {
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		slowDone <- response.Code
	}()
	select {
	case <-body.started:
	case <-time.After(time.Second):
		t.Fatal("slow body was not read")
	}

	response := postProto(t, handler, "/api/v1/search/jobs/results", &opensplunkv1.GetSearchResultsRequest{SearchJobId: "job-1"})
	if response.Code != http.StatusOK {
		t.Fatalf("normal result status = %d, body = %s", response.Code, response.Body.String())
	}
	close(body.release)
	select {
	case status := <-slowDone:
		if status != http.StatusBadRequest {
			t.Fatalf("released slow request status = %d", status)
		}
	case <-time.After(time.Second):
		t.Fatal("released slow request did not finish")
	}
}

func TestSynchronousDeadlinePreventsLateSuccess(t *testing.T) {
	delay := 30 * time.Millisecond
	base := &fakeSearchJobs{getJob: completeJob("job-1"), resultsPage: searchjobs.ResultPage{
		Schema:   searchjobs.Schema{Columns: []searchjobs.Column{{Name: "message", Kind: searchjobs.ValueKindString}}},
		Rows:     []searchjobs.ResultRow{{Values: []searchjobs.Value{searchjobs.StringValue("late")}}},
		Complete: true,
	}}
	jobs := &delayedResultJobs{fakeSearchJobs: base, delay: delay, finished: make(chan struct{})}
	handler := newTestHandler(t, Config{
		SearchJobs: jobs, Indexes: fakeIndexCatalog{}, WebUI: testUI(), RouteTimeout: 5 * time.Millisecond,
	})
	started := time.Now()
	response := postProto(t, handler, "/api/v1/search/jobs/results", &opensplunkv1.GetSearchResultsRequest{SearchJobId: "job-1"})
	if response.Code != http.StatusRequestTimeout {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
	}
	if elapsed := time.Since(started); elapsed < delay {
		t.Fatalf("handler returned after %s before synchronous work finished at %s", elapsed, delay)
	}
	select {
	case <-jobs.finished:
	default:
		t.Fatal("timeout response raced ahead of the service operation")
	}
}

func TestControlPlaneDeadlineMapsToRequestTimeout(t *testing.T) {
	handler := newTestHandler(t, Config{
		SearchJobs: &fakeSearchJobs{}, Indexes: deadlineIndexCatalog{}, WebUI: testUI(), RouteTimeout: 5 * time.Millisecond,
	})
	response := postProto(t, handler, "/api/v1/system/bootstrap", &opensplunkv1.GetSystemBootstrapRequest{})
	if response.Code != http.StatusRequestTimeout {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
	}
}

func TestSPAFallbackNeverShadowsAPI(t *testing.T) {
	handler := newTestHandler(t, Config{SearchJobs: &fakeSearchJobs{}, Indexes: fakeIndexCatalog{}, WebUI: testUI()})

	request := httptest.NewRequest(http.MethodGet, "/search/jobs/job-1", nil)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK || response.Body.String() != "<html>ui-marker</html>" {
		t.Fatalf("SPA response = %d %q", response.Code, response.Body.String())
	}
	if response.Header().Get("Cache-Control") != "no-cache" {
		t.Fatalf("SPA cache = %q", response.Header().Get("Cache-Control"))
	}

	request = httptest.NewRequest(http.MethodGet, "/api/v1/not-a-route", nil)
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code == http.StatusOK || strings.Contains(response.Body.String(), "ui-marker") {
		t.Fatalf("unknown API fell through to SPA: %d %q", response.Code, response.Body.String())
	}
	if response.Header().Get("Content-Type") != "application/json; charset=utf-8" || !strings.Contains(response.Body.String(), `"error"`) {
		t.Fatalf("unknown API error envelope = headers %v body %q", response.Header(), response.Body.String())
	}

	request = httptest.NewRequest(http.MethodGet, "/_next/static/app.abcdef12.js", nil)
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK || !strings.Contains(response.Header().Get("Cache-Control"), "immutable") {
		t.Fatalf("asset response = %d cache %q", response.Code, response.Header().Get("Cache-Control"))
	}

	request = httptest.NewRequest(http.MethodGet, "/missing.js", nil)
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusNotFound {
		t.Fatalf("missing asset status = %d", response.Code)
	}
}

func newTestHandler(t *testing.T, config Config) *Handler {
	t.Helper()
	if config.SavedSearches == nil {
		config.SavedSearches = &fakeSavedSearches{}
	}
	if config.AdministrativeAllowedHosts == nil {
		config.AdministrativeAllowedHosts = []string{"example.com"}
	}
	handler, err := NewHandler(config)
	if err != nil {
		t.Fatalf("NewHandler: %v", err)
	}
	return handler
}

func testUI() fs.FS {
	return fstest.MapFS{
		"index.html":                   &fstest.MapFile{Data: []byte("<html>ui-marker</html>")},
		"_next/static/app.abcdef12.js": &fstest.MapFile{Data: []byte("console.log('ok')")},
	}
}

func postProto(t *testing.T, handler http.Handler, path string, message proto.Message) *httptest.ResponseRecorder {
	t.Helper()
	payload, err := proto.Marshal(message)
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}
	request := httptest.NewRequest(http.MethodPost, path, bytes.NewReader(payload))
	request.Header.Set("Content-Type", "application/x-protobuf")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	return response
}

func unmarshalResponse(t *testing.T, response *httptest.ResponseRecorder, message proto.Message) {
	t.Helper()
	if err := proto.Unmarshal(response.Body.Bytes(), message); err != nil {
		t.Fatalf("unmarshal response: %v (body %q)", err, response.Body.String())
	}
}

func createRequest(earliest, latest string, indexes ...string) *opensplunkv1.CreateSearchJobRequest {
	timezone := "UTC"
	return &opensplunkv1.CreateSearchJobRequest{Definition: &opensplunkv1.SearchDefinition{
		Spl: "index=main | head 10",
		TimeRange: &opensplunkv1.TimeRangeSpec{
			Earliest: stringPointer(earliest), Latest: stringPointer(latest), Timezone: &timezone,
		},
		IndexScope: indexes,
	}}
}

func completeJob(id string) searchjobs.Job {
	return searchjobs.Job{
		ID:               id,
		Version:          1,
		OwnerID:          "owner-1",
		TenantID:         "tenant-1",
		SPL:              "index=main | head 10",
		NormalizedSPL:    "index=main | head 10",
		RequestedIndexes: []string{"main"},
		EffectiveIndexes: []string{"main"},
		Earliest:         testNow.Add(-time.Hour),
		Latest:           testNow,
		IndexTimeCutoff:  testNow,
		State:            searchjobs.StateCompleted,
		CreatedAt:        testNow.Add(-time.Minute),
		StartedAt:        testNow.Add(-30 * time.Second),
		FinishedAt:       testNow.Add(-time.Second),
		ExpiresAt:        testNow.Add(15 * time.Minute),
	}
}
