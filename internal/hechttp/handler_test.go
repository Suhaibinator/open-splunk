package hechttp

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/auth"
	"github.com/Suhaibinator/open-splunk/internal/hec"
	"github.com/Suhaibinator/open-splunk/internal/ingest"
	"github.com/Suhaibinator/open-splunk/internal/visibility"
)

const (
	testTenantID  = "tenant-hec-test"
	testRequestID = "0123456789abcdef0123456789abcdef"
	testChannel   = "123e4567-e89b-42d3-a456-426614174000"
)

var testReceivedAt = time.Date(2026, time.August, 10, 18, 19, 20, 987654321, time.UTC)

type fakeAuthenticator struct {
	mu    sync.Mutex
	calls []string
	fn    func(context.Context, string) (auth.Authentication, error)
}

func (fake *fakeAuthenticator) AuthenticateHEC(
	ctx context.Context,
	credential string,
) (auth.Authentication, error) {
	fake.mu.Lock()
	fake.calls = append(fake.calls, credential)
	function := fake.fn
	fake.mu.Unlock()
	if function == nil {
		return testAuthentication("token-record-id", false), nil
	}
	return function(ctx, credential)
}

func (fake *fakeAuthenticator) callCount() int {
	fake.mu.Lock()
	defer fake.mu.Unlock()
	return len(fake.calls)
}

type fakeAdmissionStager struct {
	mu    sync.Mutex
	calls []ingest.AdmissionRequest
	fn    func(context.Context, ingest.AdmissionRequest) (ingest.StageResult, error)
}

func (fake *fakeAdmissionStager) Stage(
	ctx context.Context,
	request ingest.AdmissionRequest,
) (ingest.StageResult, error) {
	fake.mu.Lock()
	fake.calls = append(fake.calls, request)
	function := fake.fn
	fake.mu.Unlock()
	if function != nil {
		return function(ctx, request)
	}
	result := ingest.StageResult{
		VisibilitySequence: 11,
		State:              ingest.StoredBatchPending,
		AcceptedEvents:     uint32(len(request.Events)), // #nosec G115 -- HEC event count is bounded.
		UncompressedBytes:  testAdmissionBytes(request.Events),
		HECRequestSequence: 7,
	}
	if request.HECAdmission != nil && request.HECAdmission.AcknowledgmentEnabled {
		result.HECAcknowledgmentID = 41
	}
	return result, nil
}

func (fake *fakeAdmissionStager) snapshot() []ingest.AdmissionRequest {
	fake.mu.Lock()
	defer fake.mu.Unlock()
	return append([]ingest.AdmissionRequest(nil), fake.calls...)
}

type acknowledgmentCall struct {
	tenantID string
	tokenID  string
	channel  string
	ids      []uint64
}

type fakeAcknowledgmentReader struct {
	mu       sync.Mutex
	calls    []acknowledgmentCall
	statuses map[uint64]bool
	err      error
}

func (fake *fakeAcknowledgmentReader) LookupHECAcknowledgments(
	_ context.Context,
	tenantID string,
	tokenID string,
	channel string,
	ids []uint64,
) (map[uint64]bool, error) {
	fake.mu.Lock()
	defer fake.mu.Unlock()
	fake.calls = append(fake.calls, acknowledgmentCall{
		tenantID: tenantID,
		tokenID:  tokenID,
		channel:  channel,
		ids:      append([]uint64(nil), ids...),
	})
	return fake.statuses, fake.err
}

func (fake *fakeAcknowledgmentReader) snapshot() []acknowledgmentCall {
	fake.mu.Lock()
	defer fake.mu.Unlock()
	result := make([]acknowledgmentCall, len(fake.calls))
	for index, call := range fake.calls {
		result[index] = call
		result[index].ids = append([]uint64(nil), call.ids...)
	}
	return result
}

type fakeHealthChecker struct {
	mu       sync.Mutex
	calls    int
	snapshot HealthSnapshot
	err      error
}

func (fake *fakeHealthChecker) HECHealth(context.Context) (HealthSnapshot, error) {
	fake.mu.Lock()
	defer fake.mu.Unlock()
	fake.calls++
	return fake.snapshot, fake.err
}

func (fake *fakeHealthChecker) callCount() int {
	fake.mu.Lock()
	defer fake.mu.Unlock()
	return fake.calls
}

type handlerHarness struct {
	handler *Handler
	auth    *fakeAuthenticator
	stage   *fakeAdmissionStager
	acks    *fakeAcknowledgmentReader
	health  *fakeHealthChecker
	metrics *Metrics
	next    *recordingHandler
}

type recordingHandler struct {
	mu       sync.Mutex
	calls    int
	method   string
	path     string
	response string
}

func (handler *recordingHandler) ServeHTTP(response http.ResponseWriter, request *http.Request) {
	handler.mu.Lock()
	handler.calls++
	handler.method = request.Method
	handler.path = request.URL.Path
	body := handler.response
	handler.mu.Unlock()
	response.Header().Set("X-Next-Handler", "true")
	response.WriteHeader(299)
	_, _ = io.WriteString(response, body) // #nosec G705 -- test handler deliberately echoes configured fixture data.
}

func (handler *recordingHandler) snapshot() (int, string, string) {
	handler.mu.Lock()
	defer handler.mu.Unlock()
	return handler.calls, handler.method, handler.path
}

func newHandlerHarness(t *testing.T, configure func(*Config, *handlerHarness)) *handlerHarness {
	t.Helper()
	harness := &handlerHarness{
		auth:    &fakeAuthenticator{},
		stage:   &fakeAdmissionStager{},
		acks:    &fakeAcknowledgmentReader{},
		health:  &fakeHealthChecker{snapshot: HealthSnapshot{QueueAvailable: true, AcknowledgmentAvailable: true}},
		metrics: NewMetrics(),
		next:    &recordingHandler{response: "delegated"},
	}
	config := Config{
		Next:                              harness.next,
		Authenticator:                     harness.auth,
		Admission:                         harness.stage,
		Acknowledgments:                   harness.acks,
		Health:                            harness.health,
		Metrics:                           harness.metrics,
		TenantID:                          testTenantID,
		Now:                               func() time.Time { return testReceivedAt },
		NewRequestID:                      func() (string, error) { return testRequestID, nil },
		MaximumConcurrentRequests:         8,
		MaximumConcurrentRequestsPerToken: 4,
	}
	if configure != nil {
		configure(&config, harness)
	}
	handler, err := New(config)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	harness.handler = handler
	return harness
}

func testAuthentication(tokenID string, acknowledgment bool) auth.Authentication {
	return auth.Authentication{
		TokenID:      tokenID,
		TokenVersion: 7,
		Purpose:      auth.IngestionTokenPurposeHEC,
		HECProfile: auth.HECTokenProfile{
			DefaultIndexName:      "main",
			DefaultHost:           "token-host",
			DefaultSource:         "token-source",
			DefaultSourcetype:     "token-type",
			IndexerAcknowledgment: acknowledgment,
		},
		AuthorizedIndexes: []auth.AuthorizedIndexPolicy{{Name: "main", Version: 3}},
	}
}

func hecRequest(method, target, contentType, credential, channel, body string) *http.Request {
	request := httptest.NewRequestWithContext(context.Background(), method, target, strings.NewReader(body))
	if contentType != "" {
		request.Header.Set("Content-Type", contentType)
	}
	if credential != "" {
		request.Header.Set("Authorization", credential)
	}
	if channel != "" {
		request.Header.Set("X-Splunk-Request-Channel", channel)
	}
	return request
}

func perform(handler http.Handler, request *http.Request) *httptest.ResponseRecorder {
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	return response
}

func assertHECResponse(
	t *testing.T,
	response *httptest.ResponseRecorder,
	status int,
	body string,
	extraHeaders map[string]string,
) {
	t.Helper()
	if response.Code != status {
		t.Errorf("status = %d, want %d", response.Code, status)
	}
	if got := response.Body.String(); got != body {
		t.Errorf("body = %q, want %q", got, body)
	}
	want := http.Header{
		"Content-Type":           []string{"application/json; charset=utf-8"},
		"X-Content-Type-Options": []string{"nosniff"},
		"Cache-Control":          []string{"no-store"},
		"Pragma":                 []string{"no-cache"},
	}
	for name, value := range extraHeaders {
		want[http.CanonicalHeaderKey(name)] = []string{value}
	}
	if got := response.Header(); !reflect.DeepEqual(got, want) {
		t.Errorf("headers = %#v, want %#v", got, want)
	}
}

func TestHandlerKeepsHECRoutesIsolatedAndEnforcesMethods(t *testing.T) {
	t.Parallel()
	harness := newHandlerHarness(t, nil)

	outside := perform(harness.handler, hecRequest(http.MethodPost, "/services/collectors/event", "", "", "", "outside"))
	if outside.Code != 299 || outside.Body.String() != "delegated" || outside.Header().Get("X-Next-Handler") != "true" {
		t.Fatalf("outside response = status %d headers %#v body %q", outside.Code, outside.Header(), outside.Body.String())
	}
	if calls, method, path := harness.next.snapshot(); calls != 1 || method != http.MethodPost || path != "/services/collectors/event" {
		t.Fatalf("next calls = %d method %q path %q", calls, method, path)
	}

	tests := []struct {
		name    string
		method  string
		target  string
		status  int
		body    string
		headers map[string]string
	}{
		{
			name:   "unknown namespace path",
			method: http.MethodPost,
			target: "/services/collector/not-a-route",
			status: http.StatusNotFound,
			body:   `{"text":"Invalid data format","code":6}`,
		},
		{
			name:   "percent-encoded route segment",
			method: http.MethodPost,
			target: "/services/collector/%65vent",
			status: http.StatusNotFound,
			body:   `{"text":"Invalid data format","code":6}`,
		},
		{
			name:   "percent-encoded namespace slash",
			method: http.MethodPost,
			target: "/services%2fcollector/event",
			status: http.StatusNotFound,
			body:   `{"text":"Invalid data format","code":6}`,
		},
		{
			name:    "event method",
			method:  http.MethodGet,
			target:  "/services/collector/event",
			status:  http.StatusMethodNotAllowed,
			body:    `{"text":"Invalid data format","code":6}`,
			headers: map[string]string{"Allow": http.MethodPost},
		},
		{
			name:    "health method",
			method:  http.MethodPost,
			target:  "/services/collector/health",
			status:  http.StatusMethodNotAllowed,
			body:    `{"text":"Invalid data format","code":6}`,
			headers: map[string]string{"Allow": http.MethodGet},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			response := perform(harness.handler, hecRequest(test.method, test.target, "", "", "", ""))
			assertHECResponse(t, response, test.status, test.body, test.headers)
		})
	}
	if calls, _, _ := harness.next.snapshot(); calls != 1 {
		t.Fatalf("next call count after HEC requests = %d, want 1", calls)
	}
	if harness.auth.callCount() != 0 || len(harness.stage.snapshot()) != 0 {
		t.Fatal("routing failures reached protected dependencies")
	}
}

type trackingBody struct {
	reader strings.Reader
	reads  atomic.Int64
}

func newTrackingBody(body string) *trackingBody {
	tracked := &trackingBody{}
	tracked.reader.Reset(body)
	return tracked
}

func (body *trackingBody) Read(destination []byte) (int, error) {
	body.reads.Add(1)
	return body.reader.Read(destination)
}

func (*trackingBody) Close() error { return nil }

func TestHandlerAuthenticatesBeforeReadingBodyAndMapsTokenFailures(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name          string
		header        string
		authError     error
		wantStatus    int
		wantBody      string
		wantAuthCalls int
	}{
		{
			name:       "missing",
			wantStatus: http.StatusUnauthorized,
			wantBody:   `{"text":"Token is required","code":2}`,
		},
		{
			name:       "malformed scheme",
			header:     "splunk private-secret",
			wantStatus: http.StatusUnauthorized,
			wantBody:   `{"text":"Invalid authorization","code":3}`,
		},
		{
			name:          "invalid token",
			header:        "Splunk private-secret",
			authError:     auth.ErrUnauthorized,
			wantStatus:    http.StatusForbidden,
			wantBody:      `{"text":"Invalid token","code":4}`,
			wantAuthCalls: 1,
		},
		{
			name:          "disabled token",
			header:        "Splunk private-secret",
			authError:     auth.ErrInactiveToken,
			wantStatus:    http.StatusForbidden,
			wantBody:      `{"text":"Token disabled","code":1}`,
			wantAuthCalls: 1,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			harness := newHandlerHarness(t, func(_ *Config, harness *handlerHarness) {
				harness.auth.fn = func(context.Context, string) (auth.Authentication, error) {
					return auth.Authentication{}, test.authError
				}
			})
			body := newTrackingBody(`{"event":`)
			request := httptest.NewRequestWithContext(
				context.Background(),
				http.MethodPost,
				"/services/collector/event",
				body,
			)
			request.Header.Set("Content-Type", "application/json")
			if test.header != "" {
				request.Header.Set("Authorization", test.header)
			}
			response := perform(harness.handler, request)
			assertHECResponse(t, response, test.wantStatus, test.wantBody, nil)
			if got := body.reads.Load(); got != 0 {
				t.Errorf("body reads = %d, want 0", got)
			}
			if got := harness.auth.callCount(); got != test.wantAuthCalls {
				t.Errorf("authentication calls = %d, want %d", got, test.wantAuthCalls)
			}
			if len(harness.stage.snapshot()) != 0 {
				t.Error("authentication failure reached admission")
			}
			if got := request.Header.Get("Authorization"); got != "" {
				t.Errorf("reusable request retained Authorization %q", got)
			}
		})
	}
}

func TestHandlerRemovesAuthorizationBeforeAdmission(t *testing.T) {
	t.Parallel()
	harness := newHandlerHarness(t, nil)
	request := hecRequest(
		http.MethodPost,
		"/services/collector/event",
		"application/json",
		"Splunk private-reusable-header-secret",
		"",
		`{"event":"header cleanup"}`,
	)
	response := perform(harness.handler, request)
	assertHECResponse(t, response, http.StatusOK, `{"text":"Success","code":0}`, nil)
	if got := request.Header.Get("Authorization"); got != "" {
		t.Fatalf("request retained Authorization after admission: %q", got)
	}
}

func TestHandlerJSONSuccessBuildsAtomicAdmissionAndReturnsAck(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name           string
		acknowledgment bool
		channel        string
		wantBody       string
	}{
		{
			name:     "without acknowledgment",
			wantBody: `{"text":"Success","code":0}`,
		},
		{
			name:           "with acknowledgment",
			acknowledgment: true,
			channel:        testChannel,
			wantBody:       `{"text":"Success","code":0,"ackId":41}`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			harness := newHandlerHarness(t, func(_ *Config, harness *handlerHarness) {
				harness.auth.fn = func(context.Context, string) (auth.Authentication, error) {
					return testAuthentication("json-token-id", test.acknowledgment), nil
				}
			})
			body := `{"event":"one"}` +
				`{"index":"main","host":"event-host","event":{"n":2}}`
			response := perform(harness.handler, hecRequest(
				http.MethodPost,
				"/services/collector/event/1.0",
				"application/json; charset=UTF-8",
				"Splunk private-json-secret",
				test.channel,
				body,
			))
			assertHECResponse(t, response, http.StatusOK, test.wantBody, nil)

			calls := harness.stage.snapshot()
			if len(calls) != 1 {
				t.Fatalf("admission calls = %d, want 1", len(calls))
			}
			got := calls[0]
			if got.Authorization.SubjectID != "json-token-id" || got.Authorization.TenantID != testTenantID ||
				got.Authorization.CollectorID != "" || got.Source.Kind != ingest.IngestionSourceKindHEC ||
				got.Source.ID != "json-token-id" || got.CollectorID != "" || got.BatchID != testRequestID ||
				got.BatchSequence != 1 || got.SourceBatchSHA256 == ([32]byte{}) ||
				!got.ReceivedAt.Equal(testReceivedAt) || !got.QuotaEvaluatedAt.Equal(testReceivedAt) {
				t.Errorf("admission identity = %#v", got)
			}
			if got.HECAdmission == nil {
				t.Fatal("HEC admission is nil")
			}
			wantAdmissionChannel := ""
			if test.acknowledgment {
				wantAdmissionChannel = test.channel
			}
			if got.HECAdmission.TokenID != "json-token-id" || got.HECAdmission.TokenVersion != 7 ||
				got.HECAdmission.RequestID != testRequestID ||
				got.HECAdmission.AcknowledgmentEnabled != test.acknowledgment ||
				got.HECAdmission.Channel != wantAdmissionChannel ||
				!got.HECAdmission.CreatedAt.Equal(testReceivedAt) {
				t.Errorf("HEC admission = %#v", got.HECAdmission)
			}
			if len(got.Events) != 2 {
				t.Fatalf("events = %d, want 2", len(got.Events))
			}
			first, second := got.Events[0], got.Events[1]
			if first.Event.GetEventId() != testRequestID+"-0" || first.Event.GetMessage() != "one" ||
				string(first.Event.GetRaw()) != "one" || first.Event.GetIndexName() != "main" ||
				first.Event.GetHost() != "token-host" || first.Event.GetSource() != "token-source" ||
				first.Event.GetSourcetype() != "token-type" || first.UncompressedBytes == 0 {
				t.Errorf("first event = %#v", first)
			}
			if second.Event.GetEventId() != testRequestID+"-1" || second.Event.Message != nil ||
				string(second.Event.GetRaw()) != `{"n":2}` || second.Event.GetHost() != "event-host" ||
				second.UncompressedBytes == 0 {
				t.Errorf("second event = %#v", second)
			}
		})
	}
}

func TestHandlerCapturesReceiveBoundaryBeforeBodyDecode(t *testing.T) {
	t.Parallel()
	body := newTrackingBody(`{"event":"boundary"}`)
	var capturedAfterRead atomic.Bool
	harness := newHandlerHarness(t, func(config *Config, harness *handlerHarness) {
		config.Now = func() time.Time {
			if body.reads.Load() != 0 {
				capturedAfterRead.Store(true)
			}
			return testReceivedAt
		}
		harness.auth.fn = func(context.Context, string) (auth.Authentication, error) {
			return testAuthentication("boundary-token-id", false), nil
		}
	})
	request := httptest.NewRequestWithContext(
		context.Background(),
		http.MethodPost,
		"/services/collector/event",
		body,
	)
	request.Header.Set("Authorization", "Splunk private-boundary-secret")
	request.Header.Set("Content-Type", "application/json")
	response := perform(harness.handler, request)
	assertHECResponse(t, response, http.StatusOK, `{"text":"Success","code":0}`, nil)
	if capturedAfterRead.Load() {
		t.Fatal("request receive boundary was captured after body decoding began")
	}
	calls := harness.stage.snapshot()
	if len(calls) != 1 || !calls[0].ReceivedAt.Equal(testReceivedAt) ||
		!calls[0].QuotaEvaluatedAt.Equal(testReceivedAt) {
		t.Fatalf("captured admission boundary = %#v", calls)
	}
}

func TestHandlerRawQueryChannelAndLineBreakerSuccess(t *testing.T) {
	t.Parallel()
	harness := newHandlerHarness(t, func(_ *Config, harness *handlerHarness) {
		harness.auth.fn = func(context.Context, string) (auth.Authentication, error) {
			return testAuthentication("raw-token-id", false), nil
		}
	})
	target := "/services/collector/raw/1.0?time=1700000000.25&host=raw-host" +
		"&source=raw-source&sourcetype=raw-type&index=main&channel=" + testChannel
	response := perform(harness.handler, hecRequest(
		http.MethodPost,
		target,
		"text/plain; charset=utf-8",
		"Splunk private-raw-secret",
		"",
		"first\r\n\nsecond",
	))
	assertHECResponse(t, response, http.StatusOK, `{"text":"Success","code":0}`, nil)

	calls := harness.stage.snapshot()
	if len(calls) != 1 || len(calls[0].Events) != 2 {
		t.Fatalf("admission calls/events = %d/%d, want 1/2", len(calls), eventCount(calls))
	}
	got := calls[0]
	for ordinal, wantRaw := range []string{"first", "second"} {
		event := got.Events[ordinal].Event
		if string(event.GetRaw()) != wantRaw || event.GetMessage() != wantRaw ||
			event.GetEventId() != fmt.Sprintf("%s-%d", testRequestID, ordinal) ||
			event.GetIndexName() != "main" || event.GetHost() != "raw-host" ||
			event.GetSource() != "raw-source" || event.GetSourcetype() != "raw-type" ||
			!event.GetEventTime().AsTime().Equal(time.Unix(1700000000, 250_000_000).UTC()) {
			t.Errorf("raw event %d = %#v", ordinal, event)
		}
	}
	if got.HECAdmission == nil || got.HECAdmission.AcknowledgmentEnabled || got.HECAdmission.Channel != "" {
		t.Errorf("raw HEC admission = %#v", got.HECAdmission)
	}
}

func eventCount(calls []ingest.AdmissionRequest) int {
	if len(calls) == 0 {
		return 0
	}
	return len(calls[0].Events)
}

func testAdmissionBytes(events []ingest.AdmissionEvent) uint64 {
	var total uint64
	for _, event := range events {
		total += event.UncompressedBytes
	}
	return total
}

func TestHandlerAcknowledgmentLookupPreservesOrderAndScope(t *testing.T) {
	t.Parallel()
	const authoredChannel = "ABCDEFAB-CDEF-ABCD-EFAB-CDEFABCDEFAB"
	harness := newHandlerHarness(t, func(_ *Config, harness *handlerHarness) {
		harness.auth.fn = func(context.Context, string) (auth.Authentication, error) {
			return testAuthentication("current-token-id", true), nil
		}
		harness.acks.statuses = map[uint64]bool{2: true, 9: false}
	})
	response := perform(harness.handler, hecRequest(
		http.MethodPost,
		"/services/collector/ack",
		"application/json",
		"Splunk private-ack-secret",
		authoredChannel,
		`{"acks":[7,2,9]}`,
	))
	assertHECResponse(t, response, http.StatusOK, `{"acks":{"7":false,"2":true,"9":false}}`, nil)

	calls := harness.acks.snapshot()
	want := []acknowledgmentCall{{
		tenantID: testTenantID,
		tokenID:  "current-token-id",
		channel:  authoredChannel,
		ids:      []uint64{7, 2, 9},
	}}
	if !reflect.DeepEqual(calls, want) {
		t.Fatalf("acknowledgment calls = %#v, want %#v", calls, want)
	}
}

func TestHandlerHealthShallowAuthenticatedAndUnhealthyResults(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name            string
		target          string
		credential      string
		authentication  auth.Authentication
		authError       error
		health          HealthSnapshot
		healthError     error
		wantStatus      int
		wantBody        string
		wantHealthCalls int
		wantFailureCode hec.ResultCode
	}{
		{
			name:            "shallow healthy",
			health:          HealthSnapshot{QueueAvailable: true},
			wantStatus:      http.StatusOK,
			wantBody:        `{"text":"HEC is healthy","code":17}`,
			wantHealthCalls: 1,
		},
		{
			name:            "invalid authenticated health",
			credential:      "Splunk private-invalid-health-secret",
			authError:       auth.ErrUnauthorized,
			wantStatus:      http.StatusBadRequest,
			wantBody:        `{"text":"Invalid token","code":21}`,
			wantFailureCode: hec.ResultHealthInvalidToken,
		},
		{
			name:            "disabled authenticated health",
			credential:      "Splunk private-disabled-health-secret",
			authError:       auth.ErrInactiveToken,
			wantStatus:      http.StatusBadRequest,
			wantBody:        `{"text":"Token disabled","code":22}`,
			wantFailureCode: hec.ResultHealthTokenDisabled,
		},
		{
			name:            "queue unavailable",
			credential:      "Splunk private-health-secret",
			authentication:  testAuthentication("health-token-id", false),
			health:          HealthSnapshot{AcknowledgmentAvailable: true},
			wantStatus:      http.StatusServiceUnavailable,
			wantBody:        `{"text":"HEC is unhealthy, queues are full","code":18}`,
			wantHealthCalls: 1,
			wantFailureCode: hec.ResultUnhealthyQueuesFull,
		},
		{
			name:            "ack unavailable when shallow query requests it",
			target:          "/services/collector/health?ack=true",
			health:          HealthSnapshot{QueueAvailable: true},
			wantStatus:      http.StatusServiceUnavailable,
			wantBody:        `{"text":"HEC is unhealthy, ack service unavailable","code":19}`,
			wantHealthCalls: 1,
			wantFailureCode: hec.ResultUnhealthyAcknowledgment,
		},
		{
			name:            "both unavailable when shallow query requests ack",
			target:          "/services/collector/health?ack=1",
			health:          HealthSnapshot{},
			wantStatus:      http.StatusServiceUnavailable,
			wantBody:        `{"text":"HEC is unhealthy, queues are full, ack service unavailable","code":20}`,
			wantHealthCalls: 1,
			wantFailureCode: hec.ResultUnhealthyQueuesAndAck,
		},
		{
			name:            "token ack mode alone does not request ack health",
			credential:      "Splunk private-health-secret",
			authentication:  testAuthentication("health-token-id", true),
			health:          HealthSnapshot{QueueAvailable: true},
			wantStatus:      http.StatusOK,
			wantBody:        `{"text":"HEC is healthy","code":17}`,
			wantHealthCalls: 1,
		},
		{
			name:            "health dependency failure is closed",
			health:          HealthSnapshot{QueueAvailable: true, AcknowledgmentAvailable: true},
			healthError:     errors.New("storage address and details must stay private"),
			wantStatus:      http.StatusServiceUnavailable,
			wantBody:        `{"text":"HEC is unhealthy, queues are full","code":18}`,
			wantHealthCalls: 1,
			wantFailureCode: hec.ResultUnhealthyQueuesFull,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			harness := newHandlerHarness(t, func(_ *Config, harness *handlerHarness) {
				harness.health.snapshot = test.health
				harness.health.err = test.healthError
				harness.auth.fn = func(context.Context, string) (auth.Authentication, error) {
					return test.authentication, test.authError
				}
			})
			target := test.target
			if target == "" {
				target = "/services/collector/health/1.0"
			}
			response := perform(harness.handler, hecRequest(
				http.MethodGet,
				target,
				"",
				test.credential,
				"",
				"",
			))
			assertHECResponse(t, response, test.wantStatus, test.wantBody, nil)
			if got := harness.health.callCount(); got != test.wantHealthCalls {
				t.Errorf("health calls = %d, want %d", got, test.wantHealthCalls)
			}
			metrics := harness.metrics.Snapshot()
			var failureCount uint64
			for _, count := range metrics.ProtocolFailures {
				failureCount += count
			}
			if test.wantFailureCode == hec.ResultSuccess {
				if failureCount != 0 {
					t.Errorf("healthy protocol failure counters = %#v", metrics.ProtocolFailures)
				}
			} else if metrics.ProtocolFailures[test.wantFailureCode] != 1 || failureCount != 1 {
				t.Errorf("protocol failure counters = %#v, want code %d", metrics.ProtocolFailures, test.wantFailureCode)
			}
		})
	}
}

func TestHandlerHealthAuthPrecedenceNeverReadsBody(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name          string
		target        string
		authorization string
		authError     error
		wantBody      string
		wantAuthCalls int
	}{
		{
			name:     "query credential precedes body",
			target:   "/services/collector/health?token=private-query-secret",
			wantBody: `{"text":"Query string authorization is not enabled","code":16}`,
		},
		{
			name:          "invalid token precedes body",
			target:        "/services/collector/health",
			authorization: "Splunk private-invalid-health-secret",
			authError:     auth.ErrUnauthorized,
			wantBody:      `{"text":"Invalid token","code":21}`,
			wantAuthCalls: 1,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			body := newTrackingBody("private-body-diagnostic")
			harness := newHandlerHarness(t, func(_ *Config, harness *handlerHarness) {
				harness.auth.fn = func(context.Context, string) (auth.Authentication, error) {
					return auth.Authentication{}, test.authError
				}
			})
			request := httptest.NewRequestWithContext(context.Background(), http.MethodGet, test.target, body)
			if test.authorization != "" {
				request.Header.Set("Authorization", test.authorization)
			}
			response := perform(harness.handler, request)
			assertHECResponse(t, response, http.StatusBadRequest, test.wantBody, nil)
			if got := body.reads.Load(); got != 0 {
				t.Fatalf("health body reads = %d, want zero", got)
			}
			if got := harness.auth.callCount(); got != test.wantAuthCalls {
				t.Fatalf("health auth calls = %d, want %d", got, test.wantAuthCalls)
			}
		})
	}
}

func TestHandlerMapsRateLimitAndDurableCapacityErrorsExactly(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		err        error
		wantStatus int
		wantBody   string
		retryAfter string
		wantRate   uint64
		wantPolicy uint64
	}{
		{
			name: "rate limited",
			err: &ingest.TransientStoreError{
				Err:        errors.New("private quota internals"),
				Reason:     opensplunkv1.RetryBatchReason_RETRY_BATCH_REASON_RATE_LIMITED,
				RetryAfter: 1500 * time.Millisecond,
			},
			wantStatus: http.StatusTooManyRequests,
			wantBody:   `{"text":"Server is busy","code":9}`,
			retryAfter: "2",
			wantRate:   1,
		},
		{
			name:       "event policy",
			err:        &ingest.AdmissionFailure{EventIndex: 0},
			wantStatus: http.StatusBadRequest,
			wantBody:   `{"text":"Invalid data format","code":6,"invalid-event-number":0}`,
			wantPolicy: 1,
		},
		{
			name:       "normalized admission bytes",
			err:        fmt.Errorf("private normalized details: %w", ingest.ErrAdmissionRequestTooLarge),
			wantStatus: http.StatusRequestEntityTooLarge,
			wantBody:   `{"text":"Invalid data format","code":6}`,
		},
		{
			name:       "queue capacity",
			err:        visibility.ErrPendingCapacity,
			wantStatus: http.StatusTooManyRequests,
			wantBody:   `{"text":"HEC queue is at capacity and cannot process any more requests","code":26}`,
		},
		{
			name:       "ack capacity",
			err:        visibility.ErrHECAcknowledgmentCapacity,
			wantStatus: http.StatusTooManyRequests,
			wantBody:   `{"text":"HEC ACK channel is at capacity and cannot process any more requests","code":27}`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			harness := newHandlerHarness(t, func(_ *Config, harness *handlerHarness) {
				harness.stage.fn = func(context.Context, ingest.AdmissionRequest) (ingest.StageResult, error) {
					return ingest.StageResult{}, test.err
				}
			})
			response := perform(harness.handler, hecRequest(
				http.MethodPost,
				"/services/collector/event",
				"application/json",
				"Splunk private-capacity-secret",
				"",
				`{"event":"capacity"}`,
			))
			headers := map[string]string(nil)
			if test.retryAfter != "" {
				headers = map[string]string{"Retry-After": test.retryAfter}
			}
			assertHECResponse(t, response, test.wantStatus, test.wantBody, headers)
			snapshot := harness.metrics.Snapshot()
			if snapshot.StagingFailures != 1 ||
				snapshot.RateLimitedRequests != test.wantRate ||
				snapshot.EventPolicyFailures != test.wantPolicy {
				t.Fatalf("staging failure metrics = %+v", snapshot)
			}
		})
	}
}

func TestHandlerConcurrencyGatesGloballyAndPerToken(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name          string
		globalLimit   int
		perTokenLimit int
		firstSecret   string
		secondSecret  string
		wantAuthCalls int
	}{
		{
			name:          "global",
			globalLimit:   1,
			perTokenLimit: 1,
			firstSecret:   "token-a",
			secondSecret:  "token-b",
			wantAuthCalls: 1,
		},
		{
			name:          "per token",
			globalLimit:   2,
			perTokenLimit: 1,
			firstSecret:   "token-a",
			secondSecret:  "token-a",
			wantAuthCalls: 2,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			entered := make(chan struct{}, 1)
			release := make(chan struct{})
			harness := newHandlerHarness(t, func(config *Config, harness *handlerHarness) {
				config.MaximumConcurrentRequests = test.globalLimit
				config.MaximumConcurrentRequestsPerToken = test.perTokenLimit
				harness.auth.fn = func(_ context.Context, credential string) (auth.Authentication, error) {
					return testAuthentication("id-"+credential, false), nil
				}
				harness.stage.fn = func(ctx context.Context, _ ingest.AdmissionRequest) (ingest.StageResult, error) {
					select {
					case entered <- struct{}{}:
					default:
					}
					select {
					case <-release:
						return ingest.StageResult{VisibilitySequence: 1, State: ingest.StoredBatchPending, HECRequestSequence: 1}, nil
					case <-ctx.Done():
						return ingest.StageResult{}, ctx.Err()
					}
				}
			})

			firstDone := make(chan *httptest.ResponseRecorder, 1)
			go func() {
				firstDone <- perform(harness.handler, hecRequest(
					http.MethodPost,
					"/services/collector/event",
					"application/json",
					"Splunk "+test.firstSecret,
					"",
					`{"event":"first"}`,
				))
			}()
			select {
			case <-entered:
			case <-time.After(2 * time.Second):
				t.Fatal("first request did not enter staging")
			}

			secondBody := newTrackingBody(`{"event":"second"}`)
			secondRequest := httptest.NewRequestWithContext(
				context.Background(),
				http.MethodPost,
				"/services/collector/event",
				secondBody,
			)
			secondRequest.Header.Set("Content-Type", "application/json")
			secondRequest.Header.Set("Authorization", "Splunk "+test.secondSecret)
			second := perform(harness.handler, secondRequest)
			assertHECResponse(t, second, http.StatusServiceUnavailable, `{"text":"Server is busy","code":9}`, nil)
			if got := secondBody.reads.Load(); got != 0 {
				t.Errorf("rejected request body reads = %d, want 0", got)
			}
			close(release)
			select {
			case first := <-firstDone:
				assertHECResponse(t, first, http.StatusOK, `{"text":"Success","code":0}`, nil)
			case <-time.After(2 * time.Second):
				t.Fatal("first request did not finish")
			}
			if got := len(harness.stage.snapshot()); got != 1 {
				t.Errorf("staging calls = %d, want 1", got)
			}
			if got := harness.auth.callCount(); got != test.wantAuthCalls {
				t.Errorf("authentication calls = %d, want %d", got, test.wantAuthCalls)
			}
		})
	}
}

func TestHandlerShutdownRejectsNewWorkAndWaitsForInflight(t *testing.T) {
	t.Parallel()
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	harness := newHandlerHarness(t, func(_ *Config, harness *handlerHarness) {
		harness.stage.fn = func(ctx context.Context, _ ingest.AdmissionRequest) (ingest.StageResult, error) {
			entered <- struct{}{}
			select {
			case <-release:
				return ingest.StageResult{VisibilitySequence: 1, State: ingest.StoredBatchPending, HECRequestSequence: 1}, nil
			case <-ctx.Done():
				return ingest.StageResult{}, ctx.Err()
			}
		}
	})

	activeDone := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		activeDone <- perform(harness.handler, hecRequest(
			http.MethodPost,
			"/services/collector/event",
			"application/json",
			"Splunk active-secret",
			"",
			`{"event":"active"}`,
		))
	}()
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("request did not enter staging")
	}

	harness.handler.BeginShutdown()
	harness.handler.BeginShutdown()
	rejected := perform(harness.handler, hecRequest(
		http.MethodPost,
		"/services/collector/event",
		"application/json",
		"Splunk rejected-secret",
		"",
		`{"event":"rejected"}`,
	))
	assertHECResponse(t, rejected, http.StatusServiceUnavailable, `{"text":"Server is shutting down","code":23}`, nil)
	health := perform(harness.handler, hecRequest(http.MethodGet, "/services/collector/health", "", "", "", ""))
	assertHECResponse(t, health, http.StatusServiceUnavailable, `{"text":"HEC is unhealthy, queues are full","code":18}`, nil)

	short, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if err := harness.handler.Shutdown(short); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Shutdown(short) error = %v, want deadline exceeded", err)
	}
	select {
	case response := <-activeDone:
		assertHECResponse(t, response, http.StatusServiceUnavailable, `{"text":"Server is busy","code":9}`, nil)
	case <-time.After(2 * time.Second):
		t.Fatal("active request did not observe shutdown cancellation")
	}
	close(release)
	if err := harness.handler.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown() error = %v", err)
	}
	if got := len(harness.stage.snapshot()); got != 1 {
		t.Fatalf("staging calls = %d, want only the active request", got)
	}
	if got := harness.metrics.Snapshot().ShutdownRejections; got != 1 {
		t.Errorf("shutdown rejections = %d, want 1", got)
	}
}

func TestHandlerMetricsAreBoundedAggregateAndContainNoRequestIdentity(t *testing.T) {
	t.Parallel()
	var stageCalls atomic.Int64
	harness := newHandlerHarness(t, func(_ *Config, harness *handlerHarness) {
		harness.auth.fn = func(_ context.Context, credential string) (auth.Authentication, error) {
			switch credential {
			case "private-invalid-secret":
				return auth.Authentication{}, auth.ErrUnauthorized
			case "private-ack-secret":
				return testAuthentication("private-ack-token-id", true), nil
			default:
				return testAuthentication("private-event-token-id", false), nil
			}
		}
		harness.stage.fn = func(_ context.Context, request ingest.AdmissionRequest) (ingest.StageResult, error) {
			stageCalls.Add(1)
			if len(request.Events) == 1 && string(request.Events[0].Event.GetRaw()) == "stage-error-private-payload" {
				return ingest.StageResult{}, errors.New("private staging details")
			}
			return ingest.StageResult{
				VisibilitySequence: 1,
				State:              ingest.StoredBatchPending,
				AcceptedEvents:     uint32(len(request.Events)), // #nosec G115 -- HEC events are bounded.
				UncompressedBytes:  testAdmissionBytes(request.Events),
				HECRequestSequence: 1,
			}, nil
		}
		harness.acks.statuses = map[uint64]bool{1: true}
	})

	accepted := perform(harness.handler, hecRequest(
		http.MethodPost, "/services/collector/event", "application/json", "Splunk private-good-secret", "",
		`{"event":"private-payload-a"}{"event":"private-payload-b"}`,
	))
	assertHECResponse(t, accepted, http.StatusOK, `{"text":"Success","code":0}`, nil)
	acceptedRequest := harness.stage.snapshot()[0]
	wantBytes := testAdmissionBytes(acceptedRequest.Events)

	invalid := perform(harness.handler, hecRequest(
		http.MethodPost, "/services/collector/event", "application/json", "Splunk private-invalid-secret", "",
		`{"event":"unread-private-payload"}`,
	))
	assertHECResponse(t, invalid, http.StatusForbidden, `{"text":"Invalid token","code":4}`, nil)
	malformed := perform(harness.handler, hecRequest(
		http.MethodPost, "/services/collector/event", "application/json", "Splunk private-good-secret", "",
		`{"event":"unterminated`,
	))
	assertHECResponse(t, malformed, http.StatusBadRequest, `{"text":"Invalid data format","code":6,"invalid-event-number":0}`, nil)
	stagingFailure := perform(harness.handler, hecRequest(
		http.MethodPost, "/services/collector/event", "application/json", "Splunk private-good-secret", "",
		`{"event":"stage-error-private-payload"}`,
	))
	assertHECResponse(t, stagingFailure, http.StatusInternalServerError, `{"text":"Internal server error","code":8}`, nil)
	ack := perform(harness.handler, hecRequest(
		http.MethodPost, "/services/collector/ack", "application/json", "Splunk private-ack-secret", testChannel,
		`{"acks":[1,2]}`,
	))
	assertHECResponse(t, ack, http.StatusOK, `{"acks":{"1":true,"2":false}}`, nil)

	snapshot := harness.metrics.Snapshot()
	if snapshot.Requests != 5 || snapshot.AcceptedRequests != 1 || snapshot.Events != 2 ||
		snapshot.UncompressedBytes != wantBytes || snapshot.AuthenticationFailures != 1 ||
		snapshot.DecodeFailures != 1 || snapshot.StagingFailures != 1 ||
		snapshot.AcknowledgmentQueries != 1 || snapshot.AcknowledgmentIDsQueried != 2 ||
		snapshot.AcknowledgmentMisses != 1 || snapshot.EventPolicyFailures != 0 ||
		snapshot.RateLimitedRequests != 0 ||
		stageCalls.Load() != 2 {
		t.Errorf("metrics snapshot = %#v, stage calls %d", snapshot, stageCalls.Load())
	}
	if snapshot.ProtocolFailures[hec.ResultInvalidToken] != 1 ||
		snapshot.ProtocolFailures[hec.ResultInvalidDataFormat] != 1 ||
		snapshot.ProtocolFailures[hec.ResultInternalServerError] != 1 {
		t.Errorf("protocol failure counters = %#v", snapshot.ProtocolFailures)
	}
	privateValues := []string{
		"private-good-secret", "private-event-token-id", testChannel,
		"private-payload-a", "private staging details",
	}
	formatted := fmt.Sprintf("%#v", snapshot)
	for _, private := range privateValues {
		if strings.Contains(formatted, private) {
			t.Errorf("metrics snapshot contains private request value %q", private)
		}
	}
	assertAggregateMetricShape(t, reflect.TypeOf(snapshot))
}

func assertAggregateMetricShape(t *testing.T, metricType reflect.Type) {
	t.Helper()
	for index := 0; index < metricType.NumField(); index++ {
		field := metricType.Field(index)
		switch field.Type.Kind() {
		case reflect.Uint64, reflect.Int64:
		case reflect.Array:
			if field.Type.Elem().Kind() != reflect.Uint64 {
				t.Errorf("metric field %s has non-counter array type %s", field.Name, field.Type)
			}
		default:
			t.Errorf("metric field %s has identity-capable type %s", field.Name, field.Type)
		}
	}
}
