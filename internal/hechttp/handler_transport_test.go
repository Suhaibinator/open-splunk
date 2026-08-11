package hechttp

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/ingest"
)

func TestHandlerHTTPProtocolVersionsShareOneDurableBoundary(t *testing.T) {
	t.Parallel()
	for _, protocol := range []struct {
		name  string
		text  string
		major int
		minor int
	}{
		{name: "HTTP 1.1", text: "HTTP/1.1", major: 1, minor: 1},
		{name: "HTTP 2", text: "HTTP/2.0", major: 2, minor: 0},
	} {
		t.Run(protocol.name, func(t *testing.T) {
			t.Parallel()
			harness := newHandlerHarness(t, nil)
			request := hecRequest(
				http.MethodPost,
				"/services/collector/event/1.0",
				"application/json",
				"Splunk private-protocol-secret",
				"",
				`{"event":"protocol-neutral"}`,
			)
			request.Proto = protocol.text
			request.ProtoMajor = protocol.major
			request.ProtoMinor = protocol.minor
			response := perform(harness.handler, request)
			assertHECResponse(t, response, http.StatusOK, `{"text":"Success","code":0}`, nil)
			if got := len(harness.stage.snapshot()); got != 1 {
				t.Fatalf("durable staging calls = %d, want 1", got)
			}
		})
	}
}

type failingResponseWriter struct {
	header http.Header
	status int
	writes int
}

func (writer *failingResponseWriter) Header() http.Header {
	if writer.header == nil {
		writer.header = make(http.Header)
	}
	return writer.header
}

func (writer *failingResponseWriter) WriteHeader(status int) { writer.status = status }

func (writer *failingResponseWriter) Write([]byte) (int, error) {
	writer.writes++
	return 0, errors.New("simulated disconnected response writer")
}

func TestHandlerResponseFailureAfterStagingDoesNotUndoAcceptance(t *testing.T) {
	t.Parallel()
	harness := newHandlerHarness(t, nil)
	response := &failingResponseWriter{}
	harness.handler.ServeHTTP(response, hecRequest(
		http.MethodPost,
		"/services/collector/event",
		"application/json",
		"Splunk private-disconnected-secret",
		"",
		`{"event":"durably-accepted-before-write"}`,
	))
	if response.status != http.StatusOK || response.writes != 1 {
		t.Fatalf("failed response writer = status %d, writes %d", response.status, response.writes)
	}
	if got := len(harness.stage.snapshot()); got != 1 {
		t.Fatalf("durable staging calls = %d, want committed call retained", got)
	}
	metrics := harness.metrics.Snapshot()
	if metrics.AcceptedRequests != 1 || metrics.StagingFailures != 0 {
		t.Fatalf("post-commit response failure metrics = %#v", metrics)
	}
}

func TestHandlerCancellationBeforeDurableCommitCannotReportSuccess(t *testing.T) {
	t.Parallel()
	harness := newHandlerHarness(t, func(_ *Config, harness *handlerHarness) {
		harness.stage.fn = func(ctx context.Context, _ ingest.AdmissionRequest) (ingest.StageResult, error) {
			<-ctx.Done()
			return ingest.StageResult{}, ctx.Err()
		}
	})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	request := hecRequest(
		http.MethodPost,
		"/services/collector/event",
		"application/json",
		"Splunk private-canceled-secret",
		"",
		`{"event":"must-not-report-success"}`,
	).WithContext(ctx)
	response := httptest.NewRecorder()
	harness.handler.ServeHTTP(response, request)
	assertHECResponse(t, response, http.StatusServiceUnavailable, `{"text":"Server is busy","code":9}`, nil)
	metrics := harness.metrics.Snapshot()
	if metrics.AcceptedRequests != 0 || metrics.StagingFailures != 1 {
		t.Fatalf("pre-commit cancellation metrics = %#v", metrics)
	}
}

func TestHealthRejectsChunkedBodyFramingWithoutReading(t *testing.T) {
	t.Parallel()
	body := newTrackingBody("private-slow-health-body")
	harness := newHandlerHarness(t, nil)
	request := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/services/collector/health", body)
	request.ContentLength = -1
	request.TransferEncoding = []string{"chunked"}
	response := perform(harness.handler, request)
	assertHECResponse(t, response, http.StatusBadRequest, `{"text":"Invalid data format","code":6}`, nil)
	if got := body.reads.Load(); got != 0 {
		t.Fatalf("chunked health body reads = %d, want zero", got)
	}
}
