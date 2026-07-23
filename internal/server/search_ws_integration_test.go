package server

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	exportjobs "github.com/Suhaibinator/open-splunk/internal/export"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
	"github.com/Suhaibinator/open-splunk/internal/searchws"
	"github.com/gorilla/websocket"
	"google.golang.org/protobuf/proto"
)

func TestSearchWebSocketFullServerIntegration(t *testing.T) {
	const (
		ownerID              = "owner-1"
		tenantID             = "tenant-1"
		jobID                = "search-ws-integration"
		inheritedHTTPTimeout = 400 * time.Millisecond
		clientTimeout        = 3 * time.Second
	)

	job := completeJob(jobID)
	job.State = searchjobs.StateRunning
	job.Version = 7
	job.RowCount = 42
	job.ResultBytes = 4_096
	job.Schema = &searchjobs.Schema{Columns: []searchjobs.Column{{Name: "message", Kind: searchjobs.ValueKindString}}}
	job.FinishedAt = time.Time{}
	job.ExpiresAt = time.Time{}
	searches := &fakeSearchJobs{
		getJob: job,
		resultsPage: searchjobs.ResultPage{
			Schema: *job.Schema,
			Rows:   []searchjobs.ResultRow{{Ordinal: 0, Values: []searchjobs.Value{searchjobs.StringValue("typed preview")}}},
		},
	}
	exports := &fakeExports{getFn: func(context.Context, searchjobs.AccessScope, string) (exportjobs.Job, error) {
		return exportjobs.Job{}, exportjobs.ErrNotFound
	}}

	socketService, err := searchws.New(searchws.Config{
		Searches: searches,
		Exports:  exports,
		Access:   searchjobs.AccessScope{TenantID: tenantID, OwnerID: ownerID},
		// Deliberately permit every origin at the transport. Rejection assertions
		// below therefore prove that the server's outer Host/Origin boundary runs
		// before Gorilla can upgrade the connection.
		CheckOrigin:          func(*http.Request) bool { return true },
		MaximumSubscriptions: 8,
		MaximumFrameBytes:    64 << 10,
		PollInterval:         10 * time.Millisecond,
		PingInterval:         3 * time.Second,
		PongTimeout:          5 * time.Second,
	})
	if err != nil {
		t.Fatalf("searchws.New: %v", err)
	}
	handler, err := NewHandler(Config{
		SearchJobs:                 searches,
		SearchWebSocket:            socketService,
		Indexes:                    fakeIndexCatalog{},
		SavedSearches:              &fakeSavedSearches{},
		WebUI:                      testUI(),
		OwnerID:                    ownerID,
		TenantID:                   tenantID,
		AdministrativeAllowedHosts: []string{"127.0.0.1"},
		Now:                        func() time.Time { return testNow },
	})
	if err != nil {
		t.Fatalf("NewHandler: %v", err)
	}
	httpServer := httptest.NewUnstartedServer(handler)
	// Upgraded connections are hijacked from net/http and must replace the
	// request-scoped deadlines with the WebSocket service's own liveness
	// deadlines. Keeping these deliberately short catches deadline inheritance.
	httpServer.Config.ReadTimeout = inheritedHTTPTimeout
	httpServer.Config.ReadHeaderTimeout = inheritedHTTPTimeout
	httpServer.Config.WriteTimeout = inheritedHTTPTimeout
	httpServer.Start()
	httpClient := httpServer.Client()
	httpClient.Timeout = clientTimeout
	dialer := &websocket.Dialer{HandshakeTimeout: clientTimeout}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if err := handler.Close(ctx); err != nil {
			t.Errorf("Handler.Close cleanup: %v", err)
		}
		httpServer.Close()
	})

	assertSearchWebSocketBootstrapLimits(t, httpClient, httpServer.URL, socketService)
	assertRejectedSearchWebSocketOrigins(t, dialer, httpServer.URL)
	assertSearchWebSocketWrongMethod(t, httpClient, httpServer.URL)

	connection, response, err := dialer.Dial(
		webSocketURL(httpServer.URL, searchWebSocketPath),
		http.Header{
			"Origin":         []string{httpServer.URL},
			"Sec-Fetch-Site": []string{"same-origin"},
		},
	)
	if err != nil {
		status := 0
		if response != nil {
			status = response.StatusCode
			_ = response.Body.Close()
		}
		t.Fatalf("websocket dial: %v (status %d)", err, status)
	}
	if response == nil || response.StatusCode != http.StatusSwitchingProtocols {
		if response != nil {
			_ = response.Body.Close()
		}
		_ = connection.Close()
		t.Fatalf("upgrade response = %#v", response)
	}
	_ = response.Body.Close()
	defer connection.Close()

	command := &opensplunkv1.SearchWebSocketCommand{
		RequestId: "request-1",
		Payload: &opensplunkv1.SearchWebSocketCommand_Subscribe{Subscribe: &opensplunkv1.SubscribeSearchJobsCommand{
			Subscriptions: []*opensplunkv1.SearchSubscription{{
				SubscriptionId: "subscription-1",
				Target: &opensplunkv1.JobTarget{Target: &opensplunkv1.JobTarget_SearchJobId{
					SearchJobId: jobID,
				}}, IncludePreviews: true, PreviewRowLimit: uint32Pointer(1),
			}},
		}},
	}
	writeSearchWebSocketCommand(t, connection, command)

	firstEvent := readSearchWebSocketEvent(t, connection)
	acknowledgment := firstEvent.GetSubscriptionAcknowledged()
	if acknowledgment == nil {
		t.Fatalf("first application event was not a subscription acknowledgment: %+v", firstEvent)
	}
	if firstEvent.GetSequence() != 0 || acknowledgment.GetRequestId() != command.GetRequestId() ||
		acknowledgment.GetSubscriptionId() != "subscription-1" || acknowledgment.GetTarget().GetSearchJobId() != jobID {
		t.Fatalf("subscription acknowledgment = %+v event sequence = %d", acknowledgment, firstEvent.GetSequence())
	}

	sawState := false
	sawProgress := false
	sawSchema := false
	sawPreview := false
	var lastTargetSequence uint64
	for frames := 0; frames < 11 && (!sawState || !sawProgress || !sawSchema || !sawPreview); frames++ {
		event := readSearchWebSocketEvent(t, connection)
		if event.GetSubscriptionAcknowledged() != nil {
			t.Fatalf("duplicate subscription acknowledgment: %+v", event)
		}
		if event.GetTarget().GetSearchJobId() != jobID || event.GetSubscriptionId() != "subscription-1" {
			t.Fatalf("target event routing = %+v", event)
		}
		if event.GetSequence() == 0 || event.GetSequence() <= lastTargetSequence {
			t.Fatalf("target sequence = %d after %d", event.GetSequence(), lastTargetSequence)
		}
		lastTargetSequence = event.GetSequence()
		if state := event.GetSearchStateChanged(); state != nil {
			if state.GetSearchJobId() != jobID ||
				state.GetState() != opensplunkv1.SearchJobState_SEARCH_JOB_STATE_RUNNING ||
				state.GetStateVersion() != job.Version {
				t.Fatalf("search state event = %+v", state)
			}
			sawState = true
		}
		if progress := event.GetSearchProgress(); progress != nil {
			if progress.GetProducedRows() != job.RowCount || progress.GetResultBytes() != job.ResultBytes {
				t.Fatalf("search progress event = %+v", progress)
			}
			sawProgress = true
		}
		if schema := event.GetResultSchemaAvailable(); schema != nil {
			if schema.GetSchema().GetSchemaId() != jobID || len(schema.GetSchema().GetColumns()) != 1 {
				t.Fatalf("search schema event = %+v", schema)
			}
			sawSchema = true
		}
		if preview := event.GetResultPreview(); preview != nil {
			if !sawSchema || preview.GetUpdateMode() != opensplunkv1.PreviewUpdateMode_PREVIEW_UPDATE_MODE_RESET ||
				len(preview.GetRows()) != 1 || preview.GetRows()[0].GetCells()[0].GetStringValue() != "typed preview" {
				t.Fatalf("search preview event = %+v (schema seen %t)", preview, sawSchema)
			}
			sawPreview = true
		}
	}
	if !sawState || !sawProgress || !sawSchema || !sawPreview {
		t.Fatalf("current events received: state=%v progress=%v schema=%v preview=%v", sawState, sawProgress, sawSchema, sawPreview)
	}
	searches.mu.Lock()
	gotScope, gotID := searches.getScope, searches.getID
	searches.mu.Unlock()
	if gotScope != (searchjobs.AccessScope{TenantID: tenantID, OwnerID: ownerID}) || gotID != jobID {
		t.Fatalf("scoped websocket lookup = %+v %q", gotScope, gotID)
	}

	// Stay idle past both net/http transport deadlines, then exercise the
	// application protocol in both directions. If the upgraded socket retained
	// either inherited deadline, this ping/pong exchange fails.
	time.Sleep(4 * inheritedHTTPTimeout)
	const pingNonce = "survived-http-deadlines"
	writeSearchWebSocketCommand(t, connection, &opensplunkv1.SearchWebSocketCommand{
		RequestId: "request-ping",
		Payload: &opensplunkv1.SearchWebSocketCommand_Ping{Ping: &opensplunkv1.SearchWebSocketPing{
			Nonce: pingNonce,
		}},
	})
	pongEvent := readSearchWebSocketEvent(t, connection)
	if pongEvent.GetSequence() != 0 || pongEvent.GetSubscriptionId() != "" || pongEvent.GetTarget() != nil ||
		pongEvent.GetPong().GetNonce() != pingNonce || pongEvent.GetPong().GetServerTime() == nil {
		t.Fatalf("application pong event = %+v", pongEvent)
	}

	completed := job
	completed.State = searchjobs.StateCompleted
	completed.Version++
	completed.FinishedAt = testNow.Add(-time.Second)
	completed.ExpiresAt = testNow.Add(15 * time.Minute)
	searches.mu.Lock()
	searches.getJob = completed
	searches.mu.Unlock()

	sawCompletedState := false
	sawCompletedProgress := false
	sawTerminal := false
	for frames := 0; frames < 16 && (!sawCompletedState || !sawCompletedProgress || !sawTerminal); frames++ {
		event := readSearchWebSocketEvent(t, connection)
		if event.GetTarget().GetSearchJobId() != jobID || event.GetSubscriptionId() != "subscription-1" {
			t.Fatalf("completed target event routing = %+v", event)
		}
		if event.GetSequence() == 0 || event.GetSequence() <= lastTargetSequence {
			t.Fatalf("completed target sequence = %d after %d", event.GetSequence(), lastTargetSequence)
		}
		lastTargetSequence = event.GetSequence()

		switch {
		case event.GetSearchStateChanged() != nil:
			state := event.GetSearchStateChanged()
			if state.GetSearchJobId() != jobID ||
				state.GetState() != opensplunkv1.SearchJobState_SEARCH_JOB_STATE_COMPLETED ||
				state.GetStateVersion() != completed.Version {
				t.Fatalf("completed search state event = %+v", state)
			}
			sawCompletedState = true
		case event.GetSearchProgress() != nil:
			progress := event.GetSearchProgress()
			if progress.GetProducedRows() != completed.RowCount || progress.GetResultBytes() != completed.ResultBytes {
				t.Fatalf("completed search progress event = %+v", progress)
			}
			sawCompletedProgress = true
		case event.GetSearchTerminal() != nil:
			terminal := event.GetSearchTerminal()
			if terminal.GetSearchJobId() != jobID ||
				terminal.GetState() != opensplunkv1.SearchJobState_SEARCH_JOB_STATE_COMPLETED ||
				terminal.GetStateVersion() != completed.Version || terminal.GetFailure() != nil ||
				terminal.GetFinalProgress().GetProducedRows() != completed.RowCount ||
				terminal.GetFinalProgress().GetResultBytes() != completed.ResultBytes ||
				terminal.GetResultsExpireAt() == nil ||
				!terminal.GetResultsExpireAt().AsTime().Equal(completed.ExpiresAt) {
				t.Fatalf("completed search terminal event = %+v", terminal)
			}
			sawTerminal = true
		case event.GetResultPreview() != nil:
			t.Fatalf("unchanged completed preview was redundantly republished: %+v", event.GetResultPreview())
		default:
			t.Fatalf("unexpected completed target event = %+v", event)
		}
	}
	if !sawCompletedState || !sawCompletedProgress || !sawTerminal {
		t.Fatalf("completed events received: state=%v progress=%v terminal=%v", sawCompletedState, sawCompletedProgress, sawTerminal)
	}

	closeContext, cancelClose := context.WithTimeout(context.Background(), 2*time.Second)
	if err := handler.Close(closeContext); err != nil {
		cancelClose()
		t.Fatalf("Handler.Close: %v", err)
	}
	cancelClose()
	if err := connection.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatalf("set post-close deadline: %v", err)
	}
	if _, _, err := connection.ReadMessage(); err == nil {
		t.Fatal("open websocket remained readable after Handler.Close")
	}
}

func writeSearchWebSocketCommand(t *testing.T, connection *websocket.Conn, command *opensplunkv1.SearchWebSocketCommand) {
	t.Helper()
	payload, err := proto.Marshal(command)
	if err != nil {
		t.Fatalf("marshal websocket command: %v", err)
	}
	if err := connection.SetWriteDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatalf("set write deadline: %v", err)
	}
	if err := connection.WriteMessage(websocket.BinaryMessage, payload); err != nil {
		t.Fatalf("write websocket command: %v", err)
	}
}

func readSearchWebSocketEvent(t *testing.T, connection *websocket.Conn) *opensplunkv1.SearchWebSocketEvent {
	t.Helper()
	if err := connection.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatalf("set read deadline: %v", err)
	}
	messageType, frame, err := connection.ReadMessage()
	if err != nil {
		t.Fatalf("read websocket event: %v", err)
	}
	if messageType != websocket.BinaryMessage {
		t.Fatalf("websocket message type = %d, want binary", messageType)
	}
	event := new(opensplunkv1.SearchWebSocketEvent)
	if err := proto.Unmarshal(frame, event); err != nil {
		t.Fatalf("unmarshal websocket event: %v", err)
	}
	return event
}

func assertSearchWebSocketBootstrapLimits(t *testing.T, client *http.Client, serverURL string, service *searchws.Service) {
	t.Helper()
	payload, err := proto.Marshal(&opensplunkv1.GetSystemBootstrapRequest{})
	if err != nil {
		t.Fatalf("marshal bootstrap request: %v", err)
	}
	request, err := http.NewRequest(http.MethodPost, serverURL+"/api/v1/system/bootstrap", bytes.NewReader(payload))
	if err != nil {
		t.Fatalf("create bootstrap request: %v", err)
	}
	request.Header.Set("Content-Type", "application/x-protobuf")
	request.Header.Set("Origin", serverURL)
	request.Header.Set("Sec-Fetch-Site", "same-origin")
	response, err := client.Do(request)
	if err != nil {
		t.Fatalf("bootstrap request: %v", err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatalf("read bootstrap response: %v", err)
	}
	if response.StatusCode != http.StatusOK {
		t.Fatalf("bootstrap status = %d body = %q", response.StatusCode, body)
	}
	var bootstrap opensplunkv1.GetSystemBootstrapResponse
	if err := proto.Unmarshal(body, &bootstrap); err != nil {
		t.Fatalf("unmarshal bootstrap response: %v", err)
	}
	if bootstrap.GetSearchWebsocketPath() != searchWebSocketPath ||
		bootstrap.GetLimits().GetMaximumWebsocketSubscriptions() != service.MaximumSubscriptions() ||
		bootstrap.GetLimits().GetMaximumPreviewRows() != service.MaximumPreviewRows() ||
		bootstrap.GetLimits().GetMaximumWebsocketFrameBytes() != service.MaximumFrameBytes() {
		t.Fatalf("bootstrap websocket metadata = %+v service limits = %+v", &bootstrap, service.Limits())
	}
}

func assertRejectedSearchWebSocketOrigins(t *testing.T, dialer *websocket.Dialer, serverURL string) {
	t.Helper()
	for name, origin := range map[string]string{
		"hostile host":    "http://attacker.example",
		"mismatched port": "http://127.0.0.1:1",
	} {
		t.Run(name, func(t *testing.T) {
			connection, response, err := dialer.Dial(
				webSocketURL(serverURL, searchWebSocketPath),
				http.Header{"Origin": []string{origin}, "Sec-Fetch-Site": []string{"same-origin"}},
			)
			if connection != nil {
				_ = connection.Close()
				t.Fatal("hostile origin was upgraded")
			}
			if err == nil || !errors.Is(err, websocket.ErrBadHandshake) {
				t.Fatalf("hostile-origin dial error = %v", err)
			}
			if response == nil {
				t.Fatal("hostile-origin response is nil")
			}
			defer response.Body.Close()
			if response.StatusCode != http.StatusForbidden {
				body, _ := io.ReadAll(response.Body)
				t.Fatalf("hostile-origin status = %d body = %q", response.StatusCode, body)
			}
			if response.Header.Get("Cache-Control") != "no-store" {
				t.Fatalf("hostile-origin cache policy = %q", response.Header.Get("Cache-Control"))
			}
		})
	}
}

func assertSearchWebSocketWrongMethod(t *testing.T, client *http.Client, serverURL string) {
	t.Helper()
	request, err := http.NewRequest(http.MethodPost, serverURL+searchWebSocketPath, nil)
	if err != nil {
		t.Fatalf("create wrong-method request: %v", err)
	}
	response, err := client.Do(request)
	if err != nil {
		t.Fatalf("wrong-method request: %v", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusMethodNotAllowed || response.Header.Get("Allow") != http.MethodGet {
		t.Fatalf("wrong-method response = %d allow = %q", response.StatusCode, response.Header.Get("Allow"))
	}
	if response.Header.Get("Cache-Control") != "no-store" {
		t.Fatalf("wrong-method cache policy = %q", response.Header.Get("Cache-Control"))
	}
}

func webSocketURL(serverURL, path string) string {
	return "ws" + strings.TrimPrefix(serverURL, "http") + path
}
