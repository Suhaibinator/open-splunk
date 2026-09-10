package hechttp

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/auth"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/ingest"
)

func TestHandlerPendingAuthenticationDoesNotConsumeProtectedAdmission(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		path        string
		contentType string
		body        string
		wantBody    string
	}{
		{"/services/collector", "application/json", `{"event":"accepted"}`, `{"text":"Success","code":0,"ackId":41}`},
		{"/services/collector/event", "application/json", `{"event":"accepted"}`, `{"text":"Success","code":0,"ackId":41}`},
		{"/services/collector/raw", "text/plain", "accepted", `{"text":"Success","code":0,"ackId":41}`},
		{"/services/collector/ack", "application/json", `{"acks":[41]}`, `{"acks":{"41":false}}`},
	} {
		t.Run(test.path, func(t *testing.T) {
			t.Parallel()
			authEntered := make(chan struct{})
			stageEntered := make(chan struct{})
			release := make(chan struct{})
			defer close(release)
			harness := newHandlerHarness(t, func(config *Config, harness *handlerHarness) {
				config.MaximumConcurrentRequests = 2
				config.MaximumConcurrentRequestsPerToken = 1
				harness.auth.fn = func(ctx context.Context, credential string) (auth.Authentication, error) {
					if credential == "unknown" {
						close(authEntered)
						select {
						case <-release:
						case <-ctx.Done():
						}
						return auth.Authentication{}, auth.ErrUnauthorized
					}
					return testAuthentication(credential, true), nil
				}
				harness.stage.fn = func(ctx context.Context, request ingest.AdmissionRequest) (ingest.StageResult, error) {
					if request.Source.ID == "inflight" {
						close(stageEntered)
						select {
						case <-release:
						case <-ctx.Done():
						}
					}
					return ingest.StageResult{VisibilitySequence: 1, State: ingest.StoredBatchPending, HECRequestSequence: 1, HECAcknowledgmentID: 41}, nil
				}
			})
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			unknownBody := newTrackingBody(test.body)
			unknown := httptest.NewRequestWithContext(ctx, http.MethodPost, test.path, unknownBody)
			unknown.Header.Set("Content-Type", test.contentType)
			unknown.Header.Set("Authorization", "Splunk unknown")
			unknown.Header.Set("X-Splunk-Request-Channel", testChannel)
			unknownDone := make(chan *httptest.ResponseRecorder, 1)
			go func() { unknownDone <- perform(harness.handler, unknown) }()
			select {
			case <-authEntered:
			case <-time.After(2 * time.Second):
				t.Fatal("unknown credential did not reach authentication")
			}
			inflightDone := make(chan *httptest.ResponseRecorder, 1)
			go func() {
				inflightDone <- perform(harness.handler, hecRequest(http.MethodPost, "/services/collector/event", "application/json", "Splunk inflight", testChannel, `{"event":"inflight"}`).WithContext(ctx))
			}()
			select {
			case <-stageEntered:
			case <-time.After(2 * time.Second):
				t.Fatal("authenticated request did not enter staging")
			}

			// One protected request and one unresolved credential must leave a
			// protected slot for a second verified principal on every POST route.
			accepted := perform(harness.handler, hecRequest(http.MethodPost, test.path, test.contentType, "Splunk accepted", testChannel, test.body))
			assertHECResponse(t, accepted, http.StatusOK, test.wantBody, nil)
			cancel()
			select {
			case rejected := <-unknownDone:
				assertHECResponse(t, rejected, http.StatusForbidden, `{"text":"Invalid token","code":4}`, nil)
			case <-time.After(2 * time.Second):
				t.Fatal("canceled authentication did not finish")
			}
			select {
			case <-inflightDone:
			case <-time.After(2 * time.Second):
				t.Fatal("inflight staging did not finish")
			}
			if reads := unknownBody.reads.Load(); reads != 0 {
				t.Fatalf("unknown credential caused %d body reads", reads)
			}
		})
	}
}

func TestHandlerReleasesAdmissionWhenFinalAuthenticationFails(t *testing.T) {
	t.Parallel()
	for _, health := range []bool{false, true} {
		method, path, contentType, body := http.MethodPost, "/services/collector/event", "application/json", `{"event":"accepted"}`
		failureStatus, failureBody := http.StatusForbidden, `{"text":"Invalid token","code":4}`
		successBody := `{"text":"Success","code":0}`
		if health {
			method, path, contentType, body = http.MethodGet, "/services/collector/health", "", ""
			failureStatus, failureBody = http.StatusBadRequest, `{"text":"Invalid token","code":21}`
			successBody = `{"text":"HEC is healthy","code":17}`
		}
		t.Run(path, func(t *testing.T) {
			t.Parallel()
			var completions atomic.Int32
			harness := newHandlerHarness(t, func(config *Config, harness *handlerHarness) {
				config.MaximumConcurrentRequests = 1
				config.MaximumConcurrentRequestsPerToken = 1
				harness.auth.afterAdmission = func(context.Context) error {
					if completions.Add(1) == 1 {
						return auth.ErrUnauthorized
					}
					return nil
				}
			})
			rejected := perform(harness.handler, hecRequest(method, path, contentType, "Splunk token", "", body))
			assertHECResponse(t, rejected, failureStatus, failureBody, nil)
			accepted := perform(harness.handler, hecRequest(method, path, contentType, "Splunk token", "", body))
			assertHECResponse(t, accepted, http.StatusOK, successBody, nil)
			if len(harness.handler.globalSlots) != 0 || len(harness.handler.authSlots) != 0 {
				t.Fatal("final authentication retained capacity")
			}
		})
	}
}

func TestHandlerRejectsCapacityBeforeRecordingTokenUse(t *testing.T) {
	t.Parallel()
	for _, gate := range []string{"global", "token", "health token"} {
		t.Run(gate, func(t *testing.T) {
			t.Parallel()
			ctx := t.Context()
			db, err := control.Open(ctx, t.TempDir()+"/control.sqlite")
			if err != nil {
				t.Fatalf("open control database: %v", err)
			}
			t.Cleanup(func() { _ = db.Close() })
			if _, err := db.CreateIndex(ctx, control.IndexDefinition{Name: "main", DisplayName: "Main", IngestionEnabled: true, SearchEnabled: true}); err != nil {
				t.Fatalf("create index: %v", err)
			}
			store, err := auth.NewStore(db, []byte("0123456789abcdef0123456789abcdef"))
			if err != nil {
				t.Fatalf("create token store: %v", err)
			}
			issued, err := store.CreateCollectorToken(ctx, auth.CreateCollectorTokenRequest{
				Name: "admission-before-use", Purpose: auth.IngestionTokenPurposeHEC,
				AllowedIndexNames: []string{"main"}, HECProfile: auth.HECTokenProfile{DefaultIndexName: "main"},
			})
			if err != nil {
				t.Fatalf("create HEC token: %v", err)
			}
			harness := newHandlerHarness(t, func(config *Config, _ *handlerHarness) {
				config.Authenticator = store
				config.MaximumConcurrentRequests = 1
				config.MaximumConcurrentRequestsPerToken = 1
			})
			var release func()
			if gate == "global" {
				release, err = harness.handler.beginGlobal()
				if err != nil {
					t.Fatalf("reserve global capacity: %v", err)
				}
			} else {
				if !harness.handler.tokenSlots.acquire(issued.Token.ID, 1) {
					t.Fatal("reserve token capacity")
				}
				release = func() { harness.handler.tokenSlots.release(issued.Token.ID) }
			}
			method, path, contentType, body := http.MethodPost, "/services/collector/event", "application/json", `{"event":"accepted"}`
			busyBody, successBody := `{"text":"Server is busy","code":9}`, `{"text":"Success","code":0}`
			if gate == "health token" {
				method, path, contentType, body = http.MethodGet, "/services/collector/health", "", ""
				busyBody, successBody = `{"text":"HEC is unhealthy, queues are full","code":18}`, `{"text":"HEC is healthy","code":17}`
			}
			response := perform(harness.handler, hecRequest(method, path, contentType, "Splunk "+issued.Secret.Plaintext(), "", body))
			assertHECResponse(t, response, http.StatusServiceUnavailable, busyBody, nil)
			current, err := store.GetCollectorToken(ctx, issued.Token.ID)
			if err != nil || !current.LastUsedAt.IsZero() {
				t.Fatalf("capacity-rejected request recorded token use: %v, %v", current.LastUsedAt, err)
			}
			release()
			response = perform(harness.handler, hecRequest(method, path, contentType, "Splunk "+issued.Secret.Plaintext(), "", body))
			assertHECResponse(t, response, http.StatusOK, successBody, nil)
			current, err = store.GetCollectorToken(ctx, issued.Token.ID)
			if err != nil || current.LastUsedAt.IsZero() {
				t.Fatalf("admitted request did not record token use: %v, %v", current.LastUsedAt, err)
			}
		})
	}
}

func TestHandlerBoundsAuthenticationAcrossRoutesAndReleasesCanceledWork(t *testing.T) {
	t.Parallel()
	entered := make(chan struct{})
	harness := newHandlerHarness(t, func(config *Config, harness *handlerHarness) {
		config.MaximumConcurrentRequests = 1
		config.MaximumConcurrentRequestsPerToken = 1
		harness.auth.fn = func(ctx context.Context, credential string) (auth.Authentication, error) {
			if credential == "pending" {
				close(entered)
				<-ctx.Done()
				return auth.Authentication{}, auth.ErrUnauthorized
			}
			return testAuthentication(credential, false), nil
		}
	})
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		done <- perform(harness.handler, hecRequest(http.MethodPost, "/services/collector/event", "application/json", "Splunk pending", "", `{"event":"pending"}`).WithContext(ctx))
	}()
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("authentication did not enter the bounded gate")
	}
	for _, path := range []string{"/services/collector", "/services/collector/event", "/services/collector/raw", "/services/collector/ack", "/services/collector/health"} {
		t.Run(path, func(t *testing.T) {
			method, contentType := http.MethodPost, "application/json"
			wantStatus, wantBody := http.StatusServiceUnavailable, `{"text":"Server is busy","code":9}`
			if path == "/services/collector/raw" {
				contentType = "text/plain"
			}
			if path == "/services/collector/health" {
				method, contentType = http.MethodGet, ""
				wantStatus, wantBody = http.StatusServiceUnavailable, `{"text":"HEC is unhealthy, queues are full","code":18}`
			}
			body := newTrackingBody("must remain unread")
			request := httptest.NewRequestWithContext(t.Context(), method, path, body)
			request.Header.Set("Content-Type", contentType)
			request.Header.Set("Authorization", "Splunk other")
			response := perform(harness.handler, request)
			assertHECResponse(t, response, wantStatus, wantBody, nil)
			if body.reads.Load() != 0 || request.Header.Get("Authorization") != "" {
				t.Fatal("authentication backpressure read the body or retained authorization")
			}
		})
	}
	if harness.auth.callCount() != 1 || len(harness.handler.globalSlots) != 0 {
		t.Fatal("pending authentication exceeded its bound or acquired protected admission")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("canceled authentication did not leave the gate")
	}
	accepted := perform(harness.handler, hecRequest(http.MethodPost, "/services/collector/event", "application/json", "Splunk accepted", "", `{"event":"accepted"}`))
	assertHECResponse(t, accepted, http.StatusOK, `{"text":"Success","code":0}`, nil)
	health := perform(harness.handler, hecRequest(http.MethodGet, "/services/collector/health", "", "Splunk accepted", "", ""))
	assertHECResponse(t, health, http.StatusOK, `{"text":"HEC is healthy","code":17}`, nil)
	if len(harness.handler.authSlots) != 0 || len(harness.handler.globalSlots) != 0 {
		t.Fatal("completed authentication or admission retained capacity")
	}
}
