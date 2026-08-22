package hechttp

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/http/httptrace"
	"os"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/auth"
	"github.com/Suhaibinator/open-splunk/internal/hec"
	"github.com/Suhaibinator/open-splunk/internal/ingest"
)

const (
	liveTransportSoakFlag        = "OPEN_SPLUNK_HEC_SOAK"
	liveTransportSoakDurationEnv = "OPEN_SPLUNK_HEC_SOAK_DURATION"
	liveTransportSoakDefault     = 24 * time.Hour

	liveGlobalLimit   = 8
	livePerTokenLimit = 4
	liveLoadWorkers   = 48
	liveShortRequests = 384
	liveHeapPeakLimit = 128 << 20
	liveHeapPostLimit = 32 << 20

	liveSoakTargetRequestsPerSecond = 100
	liveSoakBurstInterval           = time.Second * liveLoadWorkers / liveSoakTargetRequestsPerSecond
	liveSoakStagingDelay            = 50 * time.Millisecond
	liveSoakMaximumLivenessGap      = 5 * time.Second
	liveSoakProgressInterval        = time.Hour
)

var liveCredentials = map[string]struct {
	tokenID        string
	acknowledgment bool
}{
	"transport-a": {tokenID: "transport-token-a"},
	"transport-b": {tokenID: "transport-token-b", acknowledgment: true},
	"transport-c": {tokenID: "transport-token-c"},
}

// TestHandlerLiveHTTP2Backpressure negotiates HTTP/2 over TLS and holds one
// stream inside durable staging. A second stream on the same connection must
// not bypass either the process-wide or token-scoped admission schedule.
func TestHandlerLiveHTTP2Backpressure(t *testing.T) {
	tests := []struct {
		name             string
		globalLimit      int
		perTokenLimit    int
		firstCredential  string
		secondCredential string
		wantAuthCalls    uint64
	}{
		{
			name:             "process-wide gate",
			globalLimit:      1,
			perTokenLimit:    1,
			firstCredential:  "transport-a",
			secondCredential: "transport-b",
			wantAuthCalls:    1,
		},
		{
			name:             "per-token gate",
			globalLimit:      2,
			perTokenLimit:    1,
			firstCredential:  "transport-a",
			secondCredential: "transport-a",
			wantAuthCalls:    2,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			release := make(chan struct{})
			stager := &liveAdmissionStager{block: release, entered: make(chan struct{}, 1)}
			authenticator := &liveAuthenticator{}
			handler := newLiveHandler(t, liveHandlerConfig{
				authenticator: authenticator,
				stager:        stager,
				globalLimit:   test.globalLimit,
				perTokenLimit: test.perTokenLimit,
			})
			server, client := startLiveTLSServer(t, handler, true)
			defer closeLiveTLSServer(server, client)

			firstConnection := make(chan net.Conn, 1)
			firstDone := make(chan liveHTTPResult, 1)
			go func() {
				firstDone <- performLiveRequest(
					context.Background(),
					client,
					server.URL+"/services/collector/event",
					test.firstCredential,
					"",
					"application/json",
					"",
					[]byte(`{"event":"first-stream"}`),
					firstConnection,
				)
			}()
			select {
			case <-stager.entered:
			case <-time.After(5 * time.Second):
				t.Fatal("first HTTP/2 stream did not reach staging")
			}

			secondConnection := make(chan net.Conn, 1)
			second := performLiveRequest(
				t.Context(),
				client,
				server.URL+"/services/collector/event",
				test.secondCredential,
				acknowledgmentChannel(test.secondCredential),
				"application/json",
				"",
				[]byte(`{"event":"rejected-stream"}`),
				secondConnection,
			)
			assertLiveHECResult(t, second, http.StatusServiceUnavailable, 9, 2)
			if first := <-firstConnection; first != <-secondConnection {
				t.Fatal("concurrent HTTP/2 streams did not reuse one negotiated connection")
			}

			close(release)
			select {
			case first := <-firstDone:
				assertLiveHECResult(t, first, http.StatusOK, 0, 2)
			case <-time.After(5 * time.Second):
				t.Fatal("first HTTP/2 stream did not finish")
			}
			if got := authenticator.calls.Load(); got != test.wantAuthCalls {
				t.Fatalf("authentication calls = %d, want %d", got, test.wantAuthCalls)
			}
			if got := stager.accepted.Load(); got != 1 {
				t.Fatalf("staged requests = %d, want 1", got)
			}
			if got := stager.maximum.Load(); got != 1 {
				t.Fatalf("maximum concurrent staging calls = %d, want 1", got)
			}
		})
	}
}

// TestHandlerLiveHTTP1SlowChunkedBackpressure proves that an incomplete
// chunked request occupies only its one configured slot. The rejected request
// uses Expect: 100-continue so a zero-read assertion crosses the real client,
// transport, server, and handler boundary rather than a ResponseRecorder seam.
func TestHandlerLiveHTTP1SlowChunkedBackpressure(t *testing.T) {
	authenticator := &liveAuthenticator{}
	stager := &liveAdmissionStager{}
	observed := make(chan liveServerRequest, 4)
	handler := newLiveHandler(t, liveHandlerConfig{
		authenticator: authenticator,
		stager:        stager,
		globalLimit:   1,
		perTokenLimit: 1,
	})
	serverHandler := http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if marker := request.Header.Get("X-Open-Splunk-Test-Request"); marker != "" {
			select {
			case observed <- liveServerRequest{
				marker:   marker,
				protocol: request.ProtoMajor,
				chunked:  containsString(request.TransferEncoding, "chunked"),
			}:
			default:
			}
		}
		handler.ServeHTTP(response, request)
	})
	server, client := startLiveTLSServer(t, serverHandler, false)
	var closeOnce sync.Once
	closeResources := func() {
		closeOnce.Do(func() { closeLiveTLSServer(server, client) })
	}
	defer closeResources()
	warmRequest, err := http.NewRequestWithContext(
		t.Context(),
		http.MethodGet,
		server.URL+"/services/collector/health",
		nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	warm := doLiveRequest(client, warmRequest, nil)
	assertLiveHECResult(t, warm, http.StatusOK, 17, 1)
	runtime.GC()
	baselineHeap := currentHeapBytes()
	baselineGoroutines := runtime.NumGoroutine()

	slowReader, slowWriter := io.Pipe()
	slowRequest, err := http.NewRequestWithContext(
		context.Background(),
		http.MethodPost,
		server.URL+"/services/collector/event",
		slowReader,
	)
	if err != nil {
		t.Fatal(err)
	}
	slowRequest.ContentLength = -1
	slowRequest.Header.Set("Authorization", "Splunk transport-a")
	slowRequest.Header.Set("Content-Type", "application/json")
	slowRequest.Header.Set("X-Open-Splunk-Test-Request", "slow")
	slowDone := make(chan liveHTTPResult, 1)
	go func() { slowDone <- doLiveRequest(client, slowRequest, nil) }()
	if _, err := io.WriteString(slowWriter, `{"event":"slow`); err != nil {
		t.Fatalf("write first slow chunk: %v", err)
	}
	waitForAtomicAtLeast(t, &authenticator.calls, 1, "slow request authentication")
	if heldHeap := currentHeapBytes(); heldHeap > baselineHeap+(8<<20) {
		t.Fatalf(
			"one held slow-client heap = %d (baseline %d), growth exceeds 8 MiB",
			heldHeap,
			baselineHeap,
		)
	}
	if heldGoroutines := runtime.NumGoroutine(); heldGoroutines > baselineGoroutines+8 {
		t.Fatalf(
			"one held slow-client goroutines = %d (baseline %d), want at most %d",
			heldGoroutines,
			baselineGoroutines,
			baselineGoroutines+8,
		)
	}

	rejectedBody := &liveTrackingBody{source: strings.NewReader(`{"event":"must-not-be-read"}`)}
	rejectedRequest, err := http.NewRequestWithContext(
		t.Context(),
		http.MethodPost,
		server.URL+"/services/collector/event",
		rejectedBody,
	)
	if err != nil {
		t.Fatal(err)
	}
	rejectedRequest.ContentLength = int64(rejectedBody.source.Len())
	rejectedRequest.Header.Set("Authorization", "Splunk transport-b")
	rejectedRequest.Header.Set("Content-Type", "application/json")
	rejectedRequest.Header.Set("X-Open-Splunk-Test-Request", "rejected")
	rejectedRequest.Header.Set("Expect", "100-continue")
	rejected := doLiveRequest(client, rejectedRequest, nil)
	assertLiveHECResult(t, rejected, http.StatusServiceUnavailable, 9, 1)
	if got := rejectedBody.reads.Load(); got != 0 {
		t.Fatalf("backpressure-rejected live request body reads = %d, want 0", got)
	}

	if _, err := io.WriteString(slowWriter, `-client"}`); err != nil {
		t.Fatalf("finish slow chunked body: %v", err)
	}
	if err := slowWriter.Close(); err != nil {
		t.Fatalf("close slow chunked body: %v", err)
	}
	select {
	case slow := <-slowDone:
		assertLiveHECResult(t, slow, http.StatusOK, 0, 1)
	case <-time.After(5 * time.Second):
		t.Fatal("slow chunked request did not finish")
	}

	seenSlow := false
	seenRejected := false
	for range 2 {
		select {
		case request := <-observed:
			switch request.marker {
			case "slow":
				seenSlow = request.protocol == 1 && request.chunked
			case "rejected":
				seenRejected = request.protocol == 1 && !request.chunked
			}
		case <-time.After(5 * time.Second):
			t.Fatal("live server did not observe both HTTP/1.1 requests")
		}
	}
	if !seenSlow || !seenRejected {
		t.Fatalf("live HTTP/1.1 observations = slow %t rejected %t", seenSlow, seenRejected)
	}
	if got := stager.accepted.Load(); got != 1 {
		t.Fatalf("staged slow requests = %d, want 1", got)
	}

	closeResources()
	waitForGoroutineCeiling(t, baselineGoroutines+12)
	runtime.GC()
	if postHeap := currentHeapBytes(); postHeap > baselineHeap+(8<<20) {
		t.Fatalf(
			"post-slow-client heap = %d (baseline %d), retained growth exceeds 8 MiB",
			postHeap,
			baselineHeap,
		)
	}
}

// TestHandlerLiveTransportBoundedLoad is a short always-on proof that the
// reusable soak workload negotiates HTTP/2, reaches both success and explicit
// backpressure, and returns to a bounded resource envelope after mixed request
// shapes have been released.
func TestHandlerLiveTransportBoundedLoad(t *testing.T) {
	observation := runLiveTransportLoad(t, liveLoadRun{requests: liveShortRequests})
	if observation.requests != liveShortRequests ||
		observation.successes+observation.busy != observation.requests ||
		observation.successes == 0 || observation.busy == 0 {
		t.Fatalf(
			"live load requests/success/busy = %d/%d/%d, want %d classified requests and both paths exercised",
			observation.requests,
			observation.successes,
			observation.busy,
			liveShortRequests,
		)
	}
	t.Logf(
		"HEC live transport load: requests=%d success=%d busy=%d events=%d duration=%s peak_heap_growth=%d peak_goroutine_growth=%d",
		observation.requests,
		observation.successes,
		observation.busy,
		observation.events,
		observation.duration.Round(time.Millisecond),
		observation.peakHeapGrowth,
		observation.peakGoroutineGrowth,
	)
}

// TestHandlerLiveTransportSoak runs synchronized 48-request bursts at a
// declared 100 requests/second for 24 hours by default. A shorter duration is
// accepted for local harness validation, but is not release evidence for the
// plan's 24-hour soak gate.
func TestHandlerLiveTransportSoak(t *testing.T) {
	if os.Getenv(liveTransportSoakFlag) != "1" {
		t.Skip("set " + liveTransportSoakFlag + "=1 to run the HEC live transport soak")
	}
	duration := liveTransportSoakDefault
	if value := os.Getenv(liveTransportSoakDurationEnv); value != "" {
		parsed, err := time.ParseDuration(value)
		if err != nil || parsed < time.Second || parsed > 7*24*time.Hour {
			t.Fatalf("%s must be a duration from 1s through 168h", liveTransportSoakDurationEnv)
		}
		duration = parsed
	}
	plan, err := newLiveTransportSoakPlan(duration)
	if err != nil {
		t.Fatal(err)
	}
	observation := runLiveTransportLoad(t, liveLoadRun{duration: duration, soak: &plan})
	if err := validateLiveTransportSoak(plan, observation); err != nil {
		t.Fatal(err)
	}
	t.Logf(
		"HEC live transport soak: target_requests_per_second=%d bursts=%d mixed_bursts=%d requests=%d minimum_requests=%d success=%d busy=%d events=%d duration=%s events_per_second=%.1f maximum_schedule_lag=%s maximum_liveness_gap=%s tail_activity_gap=%s peak_heap_growth=%d peak_goroutine_growth=%d",
		liveSoakTargetRequestsPerSecond,
		observation.bursts,
		observation.mixedBursts,
		observation.requests,
		plan.minimumRequests,
		observation.successes,
		observation.busy,
		observation.events,
		observation.duration.Round(time.Millisecond),
		float64(observation.events)/observation.duration.Seconds(),
		observation.maximumScheduleLag.Round(time.Millisecond),
		observation.maximumLivenessGap.Round(time.Millisecond),
		observation.tailActivityGap.Round(time.Millisecond),
		observation.peakHeapGrowth,
		observation.peakGoroutineGrowth,
	)
}

func TestLiveTransportSoakPlan(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name            string
		duration        time.Duration
		minimumBursts   uint64
		minimumRequests uint64
	}{
		{name: "one second validation", duration: time.Second, minimumBursts: 2, minimumRequests: 96},
		{name: "two minute validation", duration: 2 * time.Minute, minimumBursts: 250, minimumRequests: 12_000},
		{name: "release duration", duration: 24 * time.Hour, minimumBursts: 180_000, minimumRequests: 8_640_000},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			plan, err := newLiveTransportSoakPlan(test.duration)
			if err != nil {
				t.Fatal(err)
			}
			if plan.burstInterval != 480*time.Millisecond ||
				plan.burstSize != liveLoadWorkers ||
				plan.minimumBursts != test.minimumBursts ||
				plan.minimumRequests != test.minimumRequests ||
				plan.offeredRequestsPerSecond() != liveSoakTargetRequestsPerSecond {
				t.Fatalf("live transport soak plan = %+v", plan)
			}
		})
	}
}

func TestValidateLiveTransportSoak(t *testing.T) {
	t.Parallel()
	plan, err := newLiveTransportSoakPlan(time.Second)
	if err != nil {
		t.Fatal(err)
	}
	valid := liveLoadObservation{
		requests:           144,
		successes:          24,
		busy:               120,
		events:             1_000,
		duration:           time.Second,
		bursts:             3,
		mixedBursts:        3,
		maximumScheduleLag: 10 * time.Millisecond,
		maximumLivenessGap: liveSoakBurstInterval,
		tailActivityGap:    100 * time.Millisecond,
	}
	if err := validateLiveTransportSoak(plan, valid); err != nil {
		t.Fatalf("valid live transport soak rejected: %v", err)
	}
	tests := []struct {
		name   string
		mutate func(*liveLoadObservation)
		want   string
	}{
		{name: "short duration", mutate: func(value *liveLoadObservation) { value.duration -= time.Nanosecond }, want: "duration"},
		{name: "too few bursts", mutate: func(value *liveLoadObservation) { value.bursts = 1 }, want: "bursts"},
		{name: "too little work", mutate: func(value *liveLoadObservation) { value.requests = 95 }, want: "minimum"},
		{name: "unclassified request", mutate: func(value *liveLoadObservation) { value.busy-- }, want: "classified"},
		{name: "missing repeated backpressure", mutate: func(value *liveLoadObservation) { value.mixedBursts-- }, want: "mixed"},
		{name: "too few successes", mutate: func(value *liveLoadObservation) { value.successes = 2; value.busy = 142 }, want: "successful work"},
		{name: "too few busy responses", mutate: func(value *liveLoadObservation) { value.successes = 142; value.busy = 2 }, want: "backpressure work"},
		{name: "too few events", mutate: func(value *liveLoadObservation) { value.events = 2 }, want: "event work"},
		{name: "schedule lag", mutate: func(value *liveLoadObservation) { value.maximumScheduleLag = 5*time.Second + time.Nanosecond }, want: "schedule lag"},
		{name: "liveness gap", mutate: func(value *liveLoadObservation) { value.maximumLivenessGap = 5*time.Second + time.Nanosecond }, want: "liveness gap"},
		{name: "tail gap", mutate: func(value *liveLoadObservation) { value.tailActivityGap = 2*liveSoakBurstInterval + time.Nanosecond }, want: "tail activity"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			candidate := valid
			test.mutate(&candidate)
			if err := validateLiveTransportSoak(plan, candidate); err == nil ||
				!strings.Contains(err.Error(), test.want) {
				t.Fatalf("validate live transport soak error = %v, want %q", err, test.want)
			}
		})
	}
}

func TestLiveTransportSoakLivenessDetectsWallSuspension(t *testing.T) {
	t.Parallel()
	started := time.Unix(1_700_000_000, 0)
	tracker := newLiveSoakLivenessTracker(started, liveSoakMaximumLivenessGap)
	if err := tracker.observe(started.Add(liveSoakBurstInterval)); err != nil {
		t.Fatalf("healthy live soak interval: %v", err)
	}
	if tracker.maximumGap != liveSoakBurstInterval {
		t.Fatalf("healthy live soak maximum gap = %s", tracker.maximumGap)
	}
	if err := tracker.observe(started.Add(6 * time.Second)); err == nil ||
		!strings.Contains(err.Error(), "liveness gap") {
		t.Fatalf("forward suspension error = %v", err)
	}

	tracker = newLiveSoakLivenessTracker(started, liveSoakMaximumLivenessGap)
	if err := tracker.observe(started.Add(-6 * time.Second)); err == nil ||
		!strings.Contains(err.Error(), "wall clock") {
		t.Fatalf("backward wall-clock discontinuity error = %v", err)
	}
}

type liveHandlerConfig struct {
	authenticator *liveAuthenticator
	stager        *liveAdmissionStager
	globalLimit   int
	perTokenLimit int
}

func newLiveHandler(t *testing.T, config liveHandlerConfig) *Handler {
	t.Helper()
	if config.authenticator == nil {
		config.authenticator = &liveAuthenticator{}
	}
	if config.stager == nil {
		config.stager = &liveAdmissionStager{}
	}
	if config.globalLimit == 0 {
		config.globalLimit = liveGlobalLimit
	}
	if config.perTokenLimit == 0 {
		config.perTokenLimit = livePerTokenLimit
	}
	var requestID atomic.Uint64
	handler, err := New(Config{
		Next:            http.NotFoundHandler(),
		Authenticator:   config.authenticator,
		Admission:       config.stager,
		Acknowledgments: liveAcknowledgmentReader{},
		Health: HealthCheckerFunc(func(context.Context) (HealthSnapshot, error) {
			return HealthSnapshot{QueueAvailable: true, AcknowledgmentAvailable: true}, nil
		}),
		Metrics:                           NewMetrics(),
		TenantID:                          "live-transport-tenant",
		MaximumConcurrentRequests:         config.globalLimit,
		MaximumConcurrentRequestsPerToken: config.perTokenLimit,
		NewRequestID: func() (string, error) {
			return fmt.Sprintf("%032x", requestID.Add(1)), nil
		},
	})
	if err != nil {
		t.Fatalf("construct live HEC handler: %v", err)
	}
	return handler
}

type liveAuthenticator struct{ calls atomic.Uint64 }

func (authenticator *liveAuthenticator) AuthenticateHEC(
	_ context.Context,
	credential string,
) (auth.Authentication, error) {
	authenticator.calls.Add(1)
	profile, exists := liveCredentials[credential]
	if !exists {
		return auth.Authentication{}, auth.ErrUnauthorized
	}
	result := testAuthentication(profile.tokenID, profile.acknowledgment)
	return result, nil
}

type liveAdmissionStager struct {
	delay   time.Duration
	block   <-chan struct{}
	entered chan struct{}

	active   atomic.Int64
	maximum  atomic.Int64
	accepted atomic.Uint64
	events   atomic.Uint64
	sequence atomic.Uint64

	mu             sync.Mutex
	activeByToken  map[string]int64
	maximumByToken map[string]int64
}

func (stager *liveAdmissionStager) Stage(
	ctx context.Context,
	request ingest.AdmissionRequest,
) (ingest.StageResult, error) {
	active := stager.active.Add(1)
	updateAtomicMaximum(&stager.maximum, active)
	tokenID := request.Source.ID
	stager.mu.Lock()
	if stager.activeByToken == nil {
		stager.activeByToken = make(map[string]int64)
		stager.maximumByToken = make(map[string]int64)
	}
	stager.activeByToken[tokenID]++
	if stager.activeByToken[tokenID] > stager.maximumByToken[tokenID] {
		stager.maximumByToken[tokenID] = stager.activeByToken[tokenID]
	}
	stager.mu.Unlock()
	defer func() {
		stager.active.Add(-1)
		stager.mu.Lock()
		stager.activeByToken[tokenID]--
		stager.mu.Unlock()
	}()
	if stager.entered != nil {
		select {
		case stager.entered <- struct{}{}:
		default:
		}
	}
	if stager.block != nil {
		select {
		case <-stager.block:
		case <-ctx.Done():
			return ingest.StageResult{}, ctx.Err()
		}
	}
	if stager.delay > 0 {
		timer := time.NewTimer(stager.delay)
		defer timer.Stop()
		select {
		case <-timer.C:
		case <-ctx.Done():
			return ingest.StageResult{}, ctx.Err()
		}
	}
	sequence := stager.sequence.Add(1)
	events := uint64(len(request.Events))
	stager.accepted.Add(1)
	stager.events.Add(events)
	result := ingest.StageResult{
		VisibilitySequence: sequence,
		State:              ingest.StoredBatchPending,
		AcceptedEvents:     uint32(events),
		UncompressedBytes:  admissionSourceBytes(request.Events),
		HECRequestSequence: sequence,
	}
	if request.HECAdmission != nil && request.HECAdmission.AcknowledgmentEnabled {
		result.HECAcknowledgmentID = sequence
	}
	return result, nil
}

func (stager *liveAdmissionStager) maximumTokenConcurrency() int64 {
	stager.mu.Lock()
	defer stager.mu.Unlock()
	var maximum int64
	for _, value := range stager.maximumByToken {
		maximum = max(maximum, value)
	}
	return maximum
}

func admissionSourceBytes(events []ingest.AdmissionEvent) uint64 {
	var total uint64
	for _, event := range events {
		total += event.UncompressedBytes
	}
	return total
}

type liveAcknowledgmentReader struct{}

func (liveAcknowledgmentReader) LookupHECAcknowledgments(
	context.Context,
	string,
	string,
	string,
	[]uint64,
) (map[uint64]bool, error) {
	return nil, nil
}

type liveServerRequest struct {
	marker   string
	protocol int
	chunked  bool
}

type liveTrackingBody struct {
	source *strings.Reader
	reads  atomic.Uint64
}

func (body *liveTrackingBody) Read(destination []byte) (int, error) {
	body.reads.Add(1)
	return body.source.Read(destination)
}

func (*liveTrackingBody) Close() error { return nil }

type liveHTTPResult struct {
	status        int
	protocolMajor int
	code          int
	body          []byte
	err           error
}

func performLiveRequest(
	ctx context.Context,
	client *http.Client,
	endpoint string,
	credential string,
	channel string,
	contentType string,
	contentEncoding string,
	body []byte,
	connection chan<- net.Conn,
) liveHTTPResult {
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return liveHTTPResult{err: err}
	}
	request.Header.Set("Authorization", "Splunk "+credential)
	request.Header.Set("Content-Type", contentType)
	if channel != "" {
		request.Header.Set("X-Splunk-Request-Channel", channel)
	}
	if contentEncoding != "" {
		request.Header.Set("Content-Encoding", contentEncoding)
	}
	return doLiveRequest(client, request, connection)
}

func doLiveRequest(
	client *http.Client,
	request *http.Request,
	connection chan<- net.Conn,
) liveHTTPResult {
	if connection != nil {
		request = request.WithContext(httptrace.WithClientTrace(request.Context(), &httptrace.ClientTrace{
			GotConn: func(info httptrace.GotConnInfo) {
				select {
				case connection <- info.Conn:
				default:
				}
			},
		}))
	}
	// Every caller supplies a request targeting the loopback httptest server created by this test.
	response, err := client.Do(request)
	if err != nil {
		return liveHTTPResult{err: err}
	}
	defer response.Body.Close()
	body, readErr := io.ReadAll(io.LimitReader(response.Body, hec.HardMaximumResponseBytes+1))
	if readErr != nil {
		return liveHTTPResult{err: readErr}
	}
	result := liveHTTPResult{
		status:        response.StatusCode,
		protocolMajor: response.ProtoMajor,
		body:          body,
	}
	var public struct {
		Code int `json:"code"`
	}
	if err := json.Unmarshal(body, &public); err == nil {
		result.code = public.Code
	}
	return result
}

func assertLiveHECResult(
	t *testing.T,
	result liveHTTPResult,
	wantStatus int,
	wantCode int,
	wantProtocol int,
) {
	t.Helper()
	if result.err != nil {
		t.Fatalf("live HEC request: %v", result.err)
	}
	if result.status != wantStatus || result.code != wantCode || result.protocolMajor != wantProtocol {
		t.Fatalf(
			"live HEC result = status %d code %d HTTP/%d, want %d/%d over HTTP/%d",
			result.status,
			result.code,
			result.protocolMajor,
			wantStatus,
			wantCode,
			wantProtocol,
		)
	}
}

func acknowledgmentChannel(credential string) string {
	if liveCredentials[credential].acknowledgment {
		return testChannel
	}
	return ""
}

func startLiveTLSServer(
	t *testing.T,
	handler http.Handler,
	http2 bool,
) (*httptest.Server, *http.Client) {
	t.Helper()
	server := httptest.NewUnstartedServer(handler)
	server.EnableHTTP2 = http2
	server.StartTLS()
	transport := server.Client().Transport.(*http.Transport).Clone()
	transport.ForceAttemptHTTP2 = http2
	if !http2 {
		// A non-nil empty map disables automatic HTTP/2 negotiation for this
		// test client while retaining the generated TLS trust root.
		transport.TLSNextProto = make(map[string]func(string, *tls.Conn) http.RoundTripper)
		// Keep an Expect: 100-continue request body client-side long enough to
		// prove that the handler's pre-body backpressure response does not ask
		// the transport to send it.
		transport.ExpectContinueTimeout = 30 * time.Second
	}
	return server, &http.Client{Transport: transport, Timeout: 10 * time.Second}
}

func closeLiveTLSServer(server *httptest.Server, client *http.Client) {
	if client != nil {
		client.CloseIdleConnections()
	}
	if server != nil {
		server.Close()
	}
}

func waitForAtomicAtLeast(
	t *testing.T,
	value *atomic.Uint64,
	want uint64,
	description string,
) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for value.Load() < want {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", description)
		}
		runtime.Gosched()
	}
}

func waitForGoroutineCeiling(t *testing.T, maximum int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		runtime.GC()
		if got := runtime.NumGoroutine(); got <= maximum {
			return
		} else if time.Now().After(deadline) {
			t.Fatalf("post-transport goroutines = %d, want at most %d", got, maximum)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func containsString(values []string, want string) bool {
	return slices.Contains(values, want)
}

func updateAtomicMaximum(target *atomic.Int64, candidate int64) {
	for current := target.Load(); candidate > current; current = target.Load() {
		if target.CompareAndSwap(current, candidate) {
			return
		}
	}
}

type liveLoadRun struct {
	requests uint64
	duration time.Duration
	soak     *liveTransportSoakPlan
}

type liveLoadObservation struct {
	requests            uint64
	successes           uint64
	busy                uint64
	events              uint64
	duration            time.Duration
	peakHeapGrowth      uint64
	peakGoroutineGrowth int
	bursts              uint64
	mixedBursts         uint64
	maximumScheduleLag  time.Duration
	maximumLivenessGap  time.Duration
	tailActivityGap     time.Duration
}

type liveLoadCounters struct {
	requests  atomic.Uint64
	successes atomic.Uint64
	busy      atomic.Uint64
}

type liveLoadResourceSample struct {
	peakHeap       uint64
	peakGoroutines int
}

type liveTransportSoakPlan struct {
	duration        time.Duration
	burstInterval   time.Duration
	burstSize       uint64
	minimumBursts   uint64
	minimumRequests uint64
}

func newLiveTransportSoakPlan(duration time.Duration) (liveTransportSoakPlan, error) {
	if duration < time.Second || duration > 7*24*time.Hour {
		return liveTransportSoakPlan{}, errors.New("live transport soak duration must be from 1s through 168h")
	}
	minimumBursts := uint64(duration / liveSoakBurstInterval)
	if minimumBursts == 0 {
		minimumBursts = 1
	}
	return liveTransportSoakPlan{
		duration:        duration,
		burstInterval:   liveSoakBurstInterval,
		burstSize:       liveLoadWorkers,
		minimumBursts:   minimumBursts,
		minimumRequests: minimumBursts * liveLoadWorkers,
	}, nil
}

func (plan liveTransportSoakPlan) offeredRequestsPerSecond() float64 {
	return float64(plan.burstSize) / plan.burstInterval.Seconds()
}

func validateLiveTransportSoak(
	plan liveTransportSoakPlan,
	observation liveLoadObservation,
) error {
	if observation.duration < plan.duration {
		return fmt.Errorf(
			"live transport soak duration = %s, want at least %s",
			observation.duration,
			plan.duration,
		)
	}
	if observation.duration > plan.duration+liveSoakMaximumLivenessGap {
		return fmt.Errorf(
			"live transport soak duration exceeded its bounded finish: %s, want at most %s",
			observation.duration,
			plan.duration+liveSoakMaximumLivenessGap,
		)
	}
	if observation.bursts < plan.minimumBursts {
		return fmt.Errorf(
			"live transport soak bursts = %d, want at least %d",
			observation.bursts,
			plan.minimumBursts,
		)
	}
	if observation.requests < plan.minimumRequests {
		return fmt.Errorf(
			"live transport soak requests = %d, want minimum %d",
			observation.requests,
			plan.minimumRequests,
		)
	}
	if observation.successes+observation.busy != observation.requests {
		return fmt.Errorf(
			"live transport soak classified requests = %d, want %d",
			observation.successes+observation.busy,
			observation.requests,
		)
	}
	if observation.requests != observation.bursts*plan.burstSize {
		return fmt.Errorf(
			"live transport soak burst requests = %d, want %d",
			observation.requests,
			observation.bursts*plan.burstSize,
		)
	}
	if observation.mixedBursts != observation.bursts {
		return fmt.Errorf(
			"live transport soak mixed bursts = %d, want %d",
			observation.mixedBursts,
			observation.bursts,
		)
	}
	if observation.successes < observation.bursts {
		return fmt.Errorf(
			"live transport soak successful work = %d, want at least one per burst (%d)",
			observation.successes,
			observation.bursts,
		)
	}
	if observation.busy < observation.bursts {
		return fmt.Errorf(
			"live transport soak backpressure work = %d, want at least one per burst (%d)",
			observation.busy,
			observation.bursts,
		)
	}
	if observation.events < observation.bursts {
		return fmt.Errorf(
			"live transport soak event work = %d, want at least one per burst (%d)",
			observation.events,
			observation.bursts,
		)
	}
	if observation.maximumScheduleLag > liveSoakMaximumLivenessGap {
		return fmt.Errorf(
			"live transport soak maximum schedule lag = %s, want at most %s",
			observation.maximumScheduleLag,
			liveSoakMaximumLivenessGap,
		)
	}
	if observation.maximumLivenessGap > liveSoakMaximumLivenessGap {
		return fmt.Errorf(
			"live transport soak maximum liveness gap = %s, want at most %s",
			observation.maximumLivenessGap,
			liveSoakMaximumLivenessGap,
		)
	}
	maximumTailGap := 2 * plan.burstInterval
	if observation.tailActivityGap > maximumTailGap {
		return fmt.Errorf(
			"live transport soak tail activity gap = %s, want at most %s",
			observation.tailActivityGap,
			maximumTailGap,
		)
	}
	return nil
}

type liveSoakLivenessTracker struct {
	lastWallNanoseconds int64
	maximumGap          time.Duration
	maximumAllowedGap   time.Duration
}

func newLiveSoakLivenessTracker(
	started time.Time,
	maximumAllowedGap time.Duration,
) liveSoakLivenessTracker {
	return liveSoakLivenessTracker{
		lastWallNanoseconds: started.UnixNano(),
		maximumAllowedGap:   maximumAllowedGap,
	}
}

func (tracker *liveSoakLivenessTracker) observe(now time.Time) error {
	nowNanoseconds := now.UnixNano()
	delta := nowNanoseconds - tracker.lastWallNanoseconds
	tracker.lastWallNanoseconds = nowNanoseconds
	if delta < 0 {
		gap := time.Duration(-delta)
		tracker.maximumGap = max(tracker.maximumGap, gap)
		if gap > tracker.maximumAllowedGap {
			return fmt.Errorf(
				"live transport soak wall clock moved backward by %s, limit %s",
				gap,
				tracker.maximumAllowedGap,
			)
		}
		return nil
	}
	gap := time.Duration(delta)
	tracker.maximumGap = max(tracker.maximumGap, gap)
	if gap > tracker.maximumAllowedGap {
		return fmt.Errorf(
			"live transport soak liveness gap = %s, limit %s",
			gap,
			tracker.maximumAllowedGap,
		)
	}
	return nil
}

type liveSoakPacingObservation struct {
	bursts             uint64
	mixedBursts        uint64
	maximumScheduleLag time.Duration
	maximumLivenessGap time.Duration
	tailActivityGap    time.Duration
}

func runLiveTransportLoad(t *testing.T, run liveLoadRun) liveLoadObservation {
	t.Helper()
	if (run.requests == 0) == (run.duration == 0) {
		t.Fatal("live load requires exactly one request or duration bound")
	}
	if (run.soak == nil) != (run.duration == 0) ||
		run.soak != nil && run.soak.duration != run.duration {
		t.Fatal("duration-bounded live load requires its exact soak plan")
	}
	stagingDelay := time.Millisecond
	if run.soak != nil {
		stagingDelay = liveSoakStagingDelay
	}
	stager := &liveAdmissionStager{delay: stagingDelay}
	handler := newLiveHandler(t, liveHandlerConfig{
		stager:        stager,
		globalLimit:   liveGlobalLimit,
		perTokenLimit: livePerTokenLimit,
	})
	server, client := startLiveTLSServer(t, handler, true)
	var closeOnce sync.Once
	closeResources := func() {
		closeOnce.Do(func() { closeLiveTLSServer(server, client) })
	}
	defer closeResources()

	// Establish TLS/HTTP2 and initialize decoder caches before taking the
	// baseline used for retained-resource assertions.
	warm := performLiveRequest(
		t.Context(),
		client,
		server.URL+"/services/collector/event",
		"transport-a",
		"",
		"application/json",
		"",
		[]byte(`{"event":"warmup"}`),
		nil,
	)
	assertLiveHECResult(t, warm, http.StatusOK, 0, 2)
	runtime.GC()
	baselineHeap := currentHeapBytes()
	baselineGoroutines := runtime.NumGoroutine()

	workload := newLiveLoadWorkload(t)
	counters := &liveLoadCounters{}
	stopSamples := make(chan struct{})
	samplesDone := make(chan liveLoadResourceSample, 1)
	sampleInterval := 5 * time.Millisecond
	if run.duration >= time.Minute {
		sampleInterval = time.Second
	}
	go sampleLiveLoadResources(stopSamples, samplesDone, sampleInterval)

	started := time.Now()
	deadline := time.Time{}
	if run.duration > 0 {
		deadline = started.Add(run.duration)
	}
	var next atomic.Uint64
	var workers sync.WaitGroup
	var loadErr error
	var loadErrMu sync.Mutex
	var failed atomic.Bool
	var firstError sync.Once
	recordError := func(err error) {
		firstError.Do(func() {
			loadErrMu.Lock()
			loadErr = err
			loadErrMu.Unlock()
			failed.Store(true)
		})
	}
	execute := func(index uint64) {
		if failed.Load() {
			return
		}
		item := workload[index%uint64(len(workload))]
		requestContext, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		result := performLiveRequest(
			requestContext,
			client,
			server.URL+item.path,
			item.credential,
			item.channel,
			item.contentType,
			item.contentEncoding,
			item.body,
			nil,
		)
		cancel()
		if result.err != nil {
			recordError(result.err)
			return
		}
		if result.protocolMajor != 2 {
			recordError(fmt.Errorf("live load negotiated HTTP/%d", result.protocolMajor))
			return
		}
		counters.requests.Add(1)
		switch {
		case result.status == http.StatusOK:
			counters.successes.Add(1)
		case result.status == http.StatusServiceUnavailable && result.code == 9:
			counters.busy.Add(1)
		default:
			recordError(fmt.Errorf("live load response status/code = %d/%d", result.status, result.code))
		}
	}

	var pacing liveSoakPacingObservation
	if run.soak != nil {
		var pacingErr error
		pacing, pacingErr = runPacedLiveTransportSoak(
			t,
			*run.soak,
			started,
			deadline,
			&next,
			counters,
			&failed,
			execute,
		)
		if pacingErr != nil {
			recordError(pacingErr)
		}
	} else {
		for range liveLoadWorkers {
			workers.Go(func() {
				for {
					index := next.Add(1) - 1
					if index >= run.requests || failed.Load() {
						return
					}
					execute(index)
				}
			})
		}
		workers.Wait()
	}
	duration := time.Since(started)
	close(stopSamples)
	sample := <-samplesDone
	loadErrMu.Lock()
	err := loadErr
	loadErrMu.Unlock()
	if err != nil {
		closeResources()
		t.Fatal(err)
	}

	if got := stager.active.Load(); got != 0 {
		closeResources()
		t.Fatalf("active staging calls after load = %d, want 0", got)
	}
	if got := stager.maximum.Load(); got > liveGlobalLimit {
		closeResources()
		t.Fatalf("maximum concurrent staging calls = %d, limit %d", got, liveGlobalLimit)
	}
	if got := stager.maximumTokenConcurrency(); got > livePerTokenLimit {
		closeResources()
		t.Fatalf("maximum per-token staging calls = %d, limit %d", got, livePerTokenLimit)
	}
	if sample.peakHeap > baselineHeap+liveHeapPeakLimit {
		closeResources()
		t.Fatalf(
			"live load peak heap = %d (baseline %d), growth exceeds %d",
			sample.peakHeap,
			baselineHeap,
			liveHeapPeakLimit,
		)
	}
	maximumGoroutines := baselineGoroutines + 3*liveLoadWorkers + 64
	if sample.peakGoroutines > maximumGoroutines {
		closeResources()
		t.Fatalf(
			"live load peak goroutines = %d (baseline %d), want at most %d",
			sample.peakGoroutines,
			baselineGoroutines,
			maximumGoroutines,
		)
	}

	client.CloseIdleConnections()
	runtime.GC()
	postHeap := currentHeapBytes()
	postGoroutines := runtime.NumGoroutine()
	if postHeap > baselineHeap+liveHeapPostLimit {
		closeResources()
		t.Fatalf(
			"post-load heap = %d (baseline %d), retained growth exceeds %d",
			postHeap,
			baselineHeap,
			liveHeapPostLimit,
		)
	}
	if postGoroutines > baselineGoroutines+16 {
		closeResources()
		t.Fatalf(
			"post-load goroutines = %d (baseline %d), want at most %d",
			postGoroutines,
			baselineGoroutines,
			baselineGoroutines+16,
		)
	}
	closeResources()

	peakHeapGrowth := uint64(0)
	if sample.peakHeap > baselineHeap {
		peakHeapGrowth = sample.peakHeap - baselineHeap
	}
	peakGoroutineGrowth := 0
	if sample.peakGoroutines > baselineGoroutines {
		peakGoroutineGrowth = sample.peakGoroutines - baselineGoroutines
	}
	return liveLoadObservation{
		requests:            counters.requests.Load(),
		successes:           counters.successes.Load(),
		busy:                counters.busy.Load(),
		events:              stager.events.Load(),
		duration:            duration,
		peakHeapGrowth:      peakHeapGrowth,
		peakGoroutineGrowth: peakGoroutineGrowth,
		bursts:              pacing.bursts,
		mixedBursts:         pacing.mixedBursts,
		maximumScheduleLag:  pacing.maximumScheduleLag,
		maximumLivenessGap:  pacing.maximumLivenessGap,
		tailActivityGap:     pacing.tailActivityGap,
	}
}

func runPacedLiveTransportSoak(
	t *testing.T,
	plan liveTransportSoakPlan,
	started time.Time,
	deadline time.Time,
	next *atomic.Uint64,
	counters *liveLoadCounters,
	failed *atomic.Bool,
	execute func(uint64),
) (liveSoakPacingObservation, error) {
	t.Helper()
	observation := liveSoakPacingObservation{}
	liveness := newLiveSoakLivenessTracker(started, liveSoakMaximumLivenessGap)
	nextProgress := started.Add(liveSoakProgressInterval)
	lastCompletionWall := int64(0)
	for {
		scheduled := started.Add(time.Duration(observation.bursts) * plan.burstInterval)
		if !scheduled.Before(deadline) {
			break
		}
		if err := waitForLiveSoakSchedule(t.Context(), scheduled); err != nil {
			return observation, fmt.Errorf("wait for live transport soak burst: %w", err)
		}
		actualStart := time.Now()
		if err := liveness.observe(actualStart); err != nil {
			return observation, err
		}
		scheduleLag := max(actualStart.Sub(scheduled), 0)
		observation.maximumScheduleLag = max(observation.maximumScheduleLag, scheduleLag)
		if scheduleLag > liveSoakMaximumLivenessGap {
			return observation, fmt.Errorf(
				"live transport soak schedule lag = %s, limit %s",
				scheduleLag,
				liveSoakMaximumLivenessGap,
			)
		}

		beforeSuccesses := counters.successes.Load()
		beforeBusy := counters.busy.Load()
		base := next.Add(plan.burstSize) - plan.burstSize
		startGate := make(chan struct{})
		var workers sync.WaitGroup
		workers.Add(int(plan.burstSize))
		for offset := range plan.burstSize {
			go func(index uint64) {
				defer workers.Done()
				<-startGate
				execute(index)
			}(base + offset)
		}
		close(startGate)
		workers.Wait()
		completed := time.Now()
		if err := liveness.observe(completed); err != nil {
			return observation, err
		}
		lastCompletionWall = completed.UnixNano()
		observation.bursts++
		if failed.Load() {
			return observation, nil
		}
		burstSuccesses := counters.successes.Load() - beforeSuccesses
		burstBusy := counters.busy.Load() - beforeBusy
		if burstSuccesses == 0 || burstBusy == 0 {
			return observation, fmt.Errorf(
				"live transport soak burst %d classified success/busy = %d/%d, want both paths",
				observation.bursts,
				burstSuccesses,
				burstBusy,
			)
		}
		observation.mixedBursts++
		if !completed.Before(nextProgress) {
			t.Logf(
				"HEC live transport soak progress: elapsed=%s bursts=%d requests=%d success=%d busy=%d maximum_schedule_lag=%s maximum_liveness_gap=%s",
				completed.Sub(started).Round(time.Second),
				observation.bursts,
				counters.requests.Load(),
				counters.successes.Load(),
				counters.busy.Load(),
				observation.maximumScheduleLag.Round(time.Millisecond),
				liveness.maximumGap.Round(time.Millisecond),
			)
			for !nextProgress.After(completed) {
				nextProgress = nextProgress.Add(liveSoakProgressInterval)
			}
		}
	}
	if err := waitForLiveSoakSchedule(t.Context(), deadline); err != nil {
		return observation, fmt.Errorf("wait for live transport soak duration: %w", err)
	}
	finished := time.Now()
	if err := liveness.observe(finished); err != nil {
		return observation, err
	}
	observation.maximumLivenessGap = liveness.maximumGap
	if lastCompletionWall == 0 {
		return observation, errors.New("live transport soak completed no bursts")
	}
	if tailNanoseconds := finished.UnixNano() - lastCompletionWall; tailNanoseconds > 0 {
		observation.tailActivityGap = time.Duration(tailNanoseconds)
	}
	return observation, nil
}

func waitForLiveSoakSchedule(ctx context.Context, scheduled time.Time) error {
	delay := time.Until(scheduled)
	if delay <= 0 {
		return nil
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

type liveLoadWorkItem struct {
	path            string
	credential      string
	channel         string
	contentType     string
	contentEncoding string
	body            []byte
}

func newLiveLoadWorkload(t *testing.T) []liveLoadWorkItem {
	t.Helper()
	bulk := make([]byte, 0, 32_000)
	for index := range hec.HardMaximumEvents {
		bulk = strconv.AppendQuote(append(bulk, `{"event":`...), "bulk-"+strconv.Itoa(index))
		bulk = append(bulk, '}')
	}
	largeEvent := []byte(`{"event":"` + strings.Repeat("x", 256<<10) + `"}`)
	compressedLarge := gzipForLiveLoad(t, largeEvent)
	ackIDs := make([]byte, 0, 8_000)
	ackIDs = append(ackIDs, `{"acks":[`...)
	for index := 1; index <= hec.HardMaximumAcknowledgmentIDs; index++ {
		if index > 1 {
			ackIDs = append(ackIDs, ',')
		}
		ackIDs = strconv.AppendInt(ackIDs, int64(index), 10)
	}
	ackIDs = append(ackIDs, "]}"...)
	return []liveLoadWorkItem{
		{
			path:        "/services/collector/event",
			credential:  "transport-a",
			contentType: "application/json",
			body:        []byte(`{"event":"single"}`),
		},
		{
			path:        "/services/collector/event",
			credential:  "transport-b",
			channel:     testChannel,
			contentType: "application/json",
			body:        []byte(`{"event":{"kind":"exact"},"fields":{"n":1.2300e+4,"flag":true}}`),
		},
		{
			path:        "/services/collector/raw?source=transport-load",
			credential:  "transport-c",
			channel:     testChannel,
			contentType: "text/plain",
			body:        []byte("raw-one\nraw-two\n"),
		},
		{
			path:            "/services/collector/event",
			credential:      "transport-a",
			contentType:     "application/json",
			contentEncoding: "gzip",
			body:            compressedLarge,
		},
		{
			path:        "/services/collector/event",
			credential:  "transport-a",
			contentType: "application/json",
			body:        bulk,
		},
		{
			path:        "/services/collector/ack",
			credential:  "transport-b",
			channel:     testChannel,
			contentType: "application/json",
			body:        ackIDs,
		},
	}
}

func gzipForLiveLoad(t *testing.T, body []byte) []byte {
	t.Helper()
	var compressed bytes.Buffer
	writer := gzip.NewWriter(&compressed)
	if _, err := writer.Write(body); err != nil {
		t.Fatalf("compress live HEC workload: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("close live HEC workload gzip member: %v", err)
	}
	return compressed.Bytes()
}

func sampleLiveLoadResources(
	stop <-chan struct{},
	done chan<- liveLoadResourceSample,
	interval time.Duration,
) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	sample := liveLoadResourceSample{}
	observe := func() {
		sample.peakHeap = max(sample.peakHeap, currentHeapBytes())
		sample.peakGoroutines = max(sample.peakGoroutines, runtime.NumGoroutine())
	}
	observe()
	for {
		select {
		case <-ticker.C:
			observe()
		case <-stop:
			observe()
			done <- sample
			return
		}
	}
}

func currentHeapBytes() uint64 {
	var statistics runtime.MemStats
	runtime.ReadMemStats(&statistics)
	return statistics.HeapAlloc
}
