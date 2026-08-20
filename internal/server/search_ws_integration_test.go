package server

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
	"github.com/Suhaibinator/open-splunk/internal/control"
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

	connection := dialSearchWebSocket(
		t,
		dialer,
		webSocketURL(httpServer.URL),
		httpServer.URL,
	)
	defer connection.Close()

	command := &opensplunk.SearchWebSocketCommand{
		RequestId: "request-1",
		Payload: &opensplunk.SearchWebSocketCommand_Subscribe{Subscribe: &opensplunk.SubscribeSearchJobsCommand{
			Subscriptions: []*opensplunk.SearchSubscription{{
				SubscriptionId: "subscription-1",
				Target: &opensplunk.JobTarget{Target: &opensplunk.JobTarget_SearchJobId{
					SearchJobId: jobID,
				}}, IncludePreviews: true, PreviewRowLimit: new(uint32(1)),
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
				state.GetState() != opensplunk.SearchJobState_SEARCH_JOB_STATE_RUNNING ||
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
			if !sawSchema || preview.GetUpdateMode() != opensplunk.PreviewUpdateMode_PREVIEW_UPDATE_MODE_RESET ||
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
	writeSearchWebSocketCommand(t, connection, &opensplunk.SearchWebSocketCommand{
		RequestId: "request-ping",
		Payload: &opensplunk.SearchWebSocketCommand_Ping{Ping: &opensplunk.SearchWebSocketPing{
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
				state.GetState() != opensplunk.SearchJobState_SEARCH_JOB_STATE_COMPLETED ||
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
				terminal.GetState() != opensplunk.SearchJobState_SEARCH_JOB_STATE_COMPLETED ||
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

type realSearchWebSocketExecutor struct {
	calls   atomic.Uint32
	exits   atomic.Uint32
	entered chan struct{}
	steps   chan realSearchWebSocketProgressStep
	rows    chan realSearchWebSocketRowStep
	finish  chan chan error
	exited  chan error
}

type realSearchWebSocketProgressStep struct {
	delta searchjobs.ExecutionProgressDelta
	reply chan error
}

type realSearchWebSocketRowStep struct {
	values []searchjobs.Value
	reply  chan error
}

func newRealSearchWebSocketExecutor() *realSearchWebSocketExecutor {
	return &realSearchWebSocketExecutor{
		entered: make(chan struct{}, 1),
		steps:   make(chan realSearchWebSocketProgressStep),
		rows:    make(chan realSearchWebSocketRowStep),
		finish:  make(chan chan error),
		exited:  make(chan error, 1),
	}
}

func (executor *realSearchWebSocketExecutor) Execute(
	ctx context.Context,
	_ clickhouse.CompiledQuery,
	sink searchjobs.ResultSink,
) (returnedErr error) {
	executor.calls.Add(1)
	defer func() { executor.recordExit(returnedErr) }()
	if err := sink.SetSchema(searchjobs.Schema{Columns: []searchjobs.Column{{
		Name: "message",
		Kind: searchjobs.ValueKindString,
	}}}); err != nil {
		return err
	}
	select {
	case executor.entered <- struct{}{}:
	case <-ctx.Done():
		return ctx.Err()
	}
	progress, ok := sink.(searchjobs.ProgressSink)
	if !ok {
		return errors.New("real websocket integration result sink does not report progress")
	}
	for {
		select {
		case step := <-executor.steps:
			err := progress.ReportProgress(step.delta)
			select {
			case step.reply <- err:
			case <-ctx.Done():
				err = ctx.Err()
			}
			if err != nil {
				return err
			}
		case step := <-executor.rows:
			err := sink.AddRow(step.values)
			select {
			case step.reply <- err:
			case <-ctx.Done():
				err = ctx.Err()
			}
			if err != nil {
				return err
			}
		case reply := <-executor.finish:
			reply <- nil
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (executor *realSearchWebSocketExecutor) recordExit(err error) {
	executor.exits.Add(1)
	select {
	case executor.exited <- err:
	default:
	}
}

func (executor *realSearchWebSocketExecutor) step(t *testing.T, delta searchjobs.ExecutionProgressDelta) {
	t.Helper()
	step := realSearchWebSocketProgressStep{delta: delta, reply: make(chan error, 1)}
	sendRealSearchWebSocketExecutorCommand(t, executor.steps, step, step.reply, "executor progress step")
}

func (executor *realSearchWebSocketExecutor) addRow(t *testing.T, values ...searchjobs.Value) {
	t.Helper()
	step := realSearchWebSocketRowStep{values: values, reply: make(chan error, 1)}
	sendRealSearchWebSocketExecutorCommand(t, executor.rows, step, step.reply, "executor result row")
}

func (executor *realSearchWebSocketExecutor) complete(t *testing.T) {
	t.Helper()
	reply := make(chan error, 1)
	sendRealSearchWebSocketExecutorCommand(t, executor.finish, reply, reply, "executor completion")
}

func sendRealSearchWebSocketExecutorCommand[T any](
	t *testing.T,
	commands chan<- T,
	command T,
	reply <-chan error,
	label string,
) {
	t.Helper()
	timer := time.NewTimer(2 * time.Second)
	defer timer.Stop()
	select {
	case commands <- command:
	case <-timer.C:
		t.Fatalf("timed out sending %s", label)
	}
	select {
	case err := <-reply:
		if err != nil {
			t.Fatalf("%s: %v", label, err)
		}
	case <-timer.C:
		t.Fatalf("timed out waiting for %s", label)
	}
}

type realSearchWebSocketFixture struct {
	t         *testing.T
	jobID     string
	anchor    time.Time
	executor  *realSearchWebSocketExecutor
	server    *httptest.Server
	client    *http.Client
	dialer    *websocket.Dialer
	socketURL string
}

func newRealSearchWebSocketFixture(t *testing.T, jobID string, maximumReplayEvents int) *realSearchWebSocketFixture {
	t.Helper()
	return newRealSearchWebSocketFixtureWithOptions(
		t,
		jobID,
		maximumReplayEvents,
		realSearchWebSocketFixtureOptions{},
	)
}

type realSearchWebSocketFixtureOptions struct {
	now                func() time.Time
	wait               func(context.Context, time.Duration)
	configureWebSocket func(*searchws.Config)
	wrapListener       func(net.Listener) net.Listener
}

func newRealSearchWebSocketFixtureWithOptions(
	t *testing.T,
	jobID string,
	maximumReplayEvents int,
	options realSearchWebSocketFixtureOptions,
) *realSearchWebSocketFixture {
	t.Helper()
	const (
		ownerID       = "owner-real-websocket"
		tenantID      = "tenant-real-websocket"
		clientTimeout = 3 * time.Second
	)
	anchor := time.Date(2026, time.July, 25, 12, 0, 0, 0, time.UTC)
	clock := &searchJobListIntegrationClock{now: anchor}
	now := clock.Now
	if options.now != nil {
		now = options.now
	}
	executor := newRealSearchWebSocketExecutor()
	manager, err := searchjobs.New(searchjobs.Config{
		Executor:        executor,
		Snapshotter:     searchJobListIntegrationSnapshotter(17),
		MaxConcurrent:   1,
		RetentionTTL:    time.Hour,
		CleanupInterval: -1,
		Now:             now,
		NewID:           func() string { return jobID },
		CursorKey:       []byte("0123456789abcdef0123456789abcdef"),
	})
	if err != nil {
		t.Fatalf("searchjobs.New: %v", err)
	}
	t.Cleanup(func() {
		if err := manager.Close(); err != nil {
			t.Errorf("close real websocket search manager: %v", err)
		}
	})

	exports := &fakeExports{getFn: func(context.Context, searchjobs.AccessScope, string) (exportjobs.Job, error) {
		return exportjobs.Job{}, exportjobs.ErrNotFound
	}}
	socketConfig := searchws.Config{
		Searches: manager,
		Exports:  exports,
		Access:   searchjobs.AccessScope{TenantID: tenantID, OwnerID: ownerID},
		CheckOrigin: func(*http.Request) bool {
			return true
		},
		MaximumSubscriptions: 8,
		MaximumFrameBytes:    64 << 10,
		MaximumReplayEvents:  maximumReplayEvents,
		PollInterval:         10 * time.Millisecond,
		PingInterval:         3 * time.Second,
		PongTimeout:          5 * time.Second,
		Now:                  now,
		Wait:                 options.wait,
	}
	if options.configureWebSocket != nil {
		options.configureWebSocket(&socketConfig)
	}
	socketService, err := searchws.New(socketConfig)
	if err != nil {
		t.Fatalf("searchws.New: %v", err)
	}
	handler, err := NewHandler(Config{
		SearchJobs:      manager,
		SearchWebSocket: socketService,
		Indexes: fakeIndexCatalog{indexes: []control.Index{{
			ID: "index-main",
			Definition: control.IndexDefinition{
				Name:          "main",
				DisplayName:   "Main",
				SearchEnabled: true,
			},
			State: control.IndexStateActive,
		}}},
		SavedSearches:              &fakeSavedSearches{},
		WebUI:                      testUI(),
		OwnerID:                    ownerID,
		TenantID:                   tenantID,
		AdministrativeAllowedHosts: []string{"127.0.0.1"},
		Now:                        now,
	})
	if err != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		_ = socketService.Close(ctx)
		cancel()
		t.Fatalf("NewHandler: %v", err)
	}
	server := httptest.NewUnstartedServer(handler)
	if options.wrapListener != nil {
		server.Listener = options.wrapListener(server.Listener)
	}
	server.Start()
	client := server.Client()
	client.Timeout = clientTimeout
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		if err := handler.Close(ctx); err != nil {
			t.Errorf("close real websocket handler: %v", err)
		}
		cancel()
		server.Close()
	})
	return &realSearchWebSocketFixture{
		t: t, jobID: jobID, anchor: anchor, executor: executor, server: server, client: client,
		dialer:    &websocket.Dialer{HandshakeTimeout: clientTimeout},
		socketURL: webSocketURL(server.URL),
	}
}

func (fixture *realSearchWebSocketFixture) post(path string, input, output proto.Message) {
	fixture.t.Helper()
	payload, err := proto.Marshal(input)
	if err != nil {
		fixture.t.Fatalf("marshal POST %s: %v", path, err)
	}
	request, err := http.NewRequestWithContext(context.Background(), http.MethodPost, fixture.server.URL+path, bytes.NewReader(payload))
	if err != nil {
		fixture.t.Fatalf("create POST %s: %v", path, err)
	}
	request.Header.Set("Content-Type", "application/x-protobuf")
	request.Header.Set("Origin", fixture.server.URL)
	request.Header.Set("Sec-Fetch-Site", "same-origin")
	response, err := fixture.client.Do(request)
	if err != nil {
		fixture.t.Fatalf("POST %s: %v", path, err)
	}
	defer response.Body.Close()
	const maximumResponseBytes = int64(1 << 20)
	body, err := io.ReadAll(io.LimitReader(response.Body, maximumResponseBytes+1))
	if err != nil {
		fixture.t.Fatalf("read POST %s: %v", path, err)
	}
	if int64(len(body)) > maximumResponseBytes {
		fixture.t.Fatalf("POST %s response exceeded %d bytes", path, maximumResponseBytes)
	}
	if response.StatusCode != http.StatusOK {
		fixture.t.Fatalf("POST %s status = %d body = %q", path, response.StatusCode, body)
	}
	if contentType := response.Header.Get("Content-Type"); contentType != "application/x-protobuf" {
		fixture.t.Fatalf("POST %s content type = %q", path, contentType)
	}
	if err := proto.Unmarshal(body, output); err != nil {
		fixture.t.Fatalf("unmarshal POST %s: %v", path, err)
	}
}

func (fixture *realSearchWebSocketFixture) createRunningSearch() {
	fixture.t.Helper()
	request := createRequest(
		fixture.anchor.Add(-time.Hour).Format(time.RFC3339Nano),
		fixture.anchor.Format(time.RFC3339Nano),
		"main",
	)
	request.Definition.Spl = "index=main | table message"
	var response opensplunk.CreateSearchJobResponse
	fixture.post("/api/search/jobs/create", request, &response)
	if response.GetSearchJob().GetSearchJobId() != fixture.jobID {
		fixture.t.Fatalf("created search = %+v", response.GetSearchJob())
	}
	timer := time.NewTimer(2 * time.Second)
	defer timer.Stop()
	select {
	case <-fixture.executor.entered:
	case <-timer.C:
		fixture.t.Fatal("timed out waiting for real search executor")
	}
	if calls := fixture.executor.calls.Load(); calls != 1 {
		fixture.t.Fatalf("executor calls = %d, want 1", calls)
	}
}

func (fixture *realSearchWebSocketFixture) dial() *websocket.Conn {
	fixture.t.Helper()
	return dialSearchWebSocket(
		fixture.t,
		fixture.dialer,
		fixture.socketURL,
		fixture.server.URL,
	)
}

func dialSearchWebSocket(t *testing.T, dialer *websocket.Dialer, socketURL, origin string) *websocket.Conn {
	t.Helper()
	connection, response, err := dialer.Dial(socketURL, http.Header{
		"Origin":         []string{origin},
		"Sec-Fetch-Site": []string{"same-origin"},
	})
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
	return connection
}

func searchWebSocketSubscribeCommand(requestID, subscriptionID, jobID string, afterSequence uint64) *opensplunk.SearchWebSocketCommand {
	return searchWebSocketTargetSubscribeCommand(
		requestID,
		subscriptionID,
		&opensplunk.JobTarget{
			Target: &opensplunk.JobTarget_SearchJobId{SearchJobId: jobID},
		},
		afterSequence,
		false,
		nil,
	)
}

func searchWebSocketTargetSubscribeCommand(
	requestID string,
	subscriptionID string,
	target *opensplunk.JobTarget,
	afterSequence uint64,
	includePreviews bool,
	previewRowLimit *uint32,
) *opensplunk.SearchWebSocketCommand {
	return &opensplunk.SearchWebSocketCommand{
		RequestId: requestID,
		Payload: &opensplunk.SearchWebSocketCommand_Subscribe{Subscribe: &opensplunk.SubscribeSearchJobsCommand{
			Subscriptions: []*opensplunk.SearchSubscription{{
				SubscriptionId:  subscriptionID,
				Target:          target,
				AfterSequence:   afterSequence,
				IncludePreviews: includePreviews,
				PreviewRowLimit: previewRowLimit,
			}},
		}},
	}
}

func establishRealSearchWebSocketEpoch(
	t *testing.T,
	connection *websocket.Conn,
	requestID, subscriptionID, jobID string,
) (uint64, uint64) {
	t.Helper()
	writeSearchWebSocketCommand(t, connection, searchWebSocketSubscribeCommand(requestID, subscriptionID, jobID, 0))
	ackEvent := readSearchWebSocketEvent(t, connection)
	ack := ackEvent.GetSubscriptionAcknowledged()
	if ack == nil || ack.GetRequestId() != requestID || ack.GetSubscriptionId() != subscriptionID ||
		ack.GetTarget().GetSearchJobId() != jobID || ack.GetReplayWillFollow() {
		t.Fatalf("initial subscription acknowledgment = %+v", ackEvent)
	}
	state := readSearchWebSocketEvent(t, connection)
	progress := readSearchWebSocketEvent(t, connection)
	schema := readSearchWebSocketEvent(t, connection)
	if state.GetSearchStateChanged().GetState() != opensplunk.SearchJobState_SEARCH_JOB_STATE_RUNNING ||
		progress.GetSearchProgress() == nil || schema.GetResultSchemaAvailable() == nil ||
		state.GetSubscriptionId() != subscriptionID || progress.GetSubscriptionId() != subscriptionID ||
		schema.GetSubscriptionId() != subscriptionID ||
		state.GetSequence() == 0 || progress.GetSequence() != state.GetSequence()+1 ||
		schema.GetSequence() != progress.GetSequence()+1 {
		t.Fatalf("initial websocket projection = (%+v, %+v, %+v)", state, progress, schema)
	}
	if ack.GetEarliestAvailableSequence() != state.GetSequence() ||
		ack.GetLatestSequence() != schema.GetSequence() {
		t.Fatalf("initial replay bounds = [%d,%d], want [%d,%d]",
			ack.GetEarliestAvailableSequence(), ack.GetLatestSequence(), state.GetSequence(), schema.GetSequence())
	}
	return state.GetSequence(), schema.GetSequence()
}

func TestSearchWebSocketRealManagerReplaysOneEventThenExpiresSequence(t *testing.T) {
	const (
		jobID          = "search-ws-real-replay"
		subscriptionID = "search-ws-real-subscription"
	)
	fixture := newRealSearchWebSocketFixture(t, jobID, 1)
	fixture.createRunningSearch()

	initial := fixture.dial()
	_, checkpoint := establishRealSearchWebSocketEpoch(t, initial, "initial", subscriptionID, jobID)
	fixture.executor.step(t, searchjobs.ExecutionProgressDelta{ScannedRows: 10, ScannedBytes: 100})
	firstProgress := readSearchWebSocketEvent(t, initial)
	if firstProgress.GetSearchProgress().GetScannedRows() != 10 ||
		firstProgress.GetSearchProgress().GetScannedBytes() != 100 ||
		firstProgress.GetSearchProgress().GetStateVersion() == 0 ||
		firstProgress.GetSequence() != checkpoint+1 {
		t.Fatalf("first progress = %+v", firstProgress)
	}
	if err := initial.Close(); err != nil {
		t.Fatalf("close interrupted websocket: %v", err)
	}

	replay := fixture.dial()
	defer replay.Close()
	writeSearchWebSocketCommand(t, replay, searchWebSocketSubscribeCommand("replay", subscriptionID, jobID, checkpoint))
	replayAckEvent := readSearchWebSocketEvent(t, replay)
	replayAck := replayAckEvent.GetSubscriptionAcknowledged()
	if replayAck == nil || !replayAck.GetReplayWillFollow() ||
		replayAck.GetEarliestAvailableSequence() != checkpoint ||
		replayAck.GetLatestSequence() != firstProgress.GetSequence() {
		t.Fatalf("replay acknowledgment = %+v", replayAckEvent)
	}
	replayed := readSearchWebSocketEvent(t, replay)
	if replayed.GetSearchProgress().GetScannedRows() != 10 ||
		replayed.GetSearchProgress().GetScannedBytes() != 100 ||
		replayed.GetSequence() != firstProgress.GetSequence() ||
		replayed.GetSubscriptionId() != subscriptionID ||
		!proto.Equal(replayed.GetSearchProgress(), firstProgress.GetSearchProgress()) {
		t.Fatalf("replayed progress = %+v", replayed)
	}

	fixture.executor.step(t, searchjobs.ExecutionProgressDelta{ScannedRows: 5, ScannedBytes: 50})
	secondProgress := readSearchWebSocketEvent(t, replay)
	if secondProgress.GetSearchProgress().GetScannedRows() != 15 ||
		secondProgress.GetSearchProgress().GetScannedBytes() != 150 ||
		secondProgress.GetSearchProgress().GetStateVersion() <= firstProgress.GetSearchProgress().GetStateVersion() ||
		secondProgress.GetSequence() != replayed.GetSequence()+1 {
		t.Fatalf("second progress projection = %+v", secondProgress)
	}

	expired := fixture.dial()
	defer expired.Close()
	writeSearchWebSocketCommand(t, expired, searchWebSocketSubscribeCommand("expired", "expired-subscription", jobID, checkpoint))
	expiredAckEvent := readSearchWebSocketEvent(t, expired)
	expiredAck := expiredAckEvent.GetSubscriptionAcknowledged()
	if expiredAck == nil || expiredAck.GetReplayWillFollow() ||
		expiredAck.GetEarliestAvailableSequence() != secondProgress.GetSequence() ||
		expiredAck.GetLatestSequence() != secondProgress.GetSequence() {
		t.Fatalf("expired acknowledgment = %+v", expiredAckEvent)
	}
	resynchronization := readSearchWebSocketEvent(t, expired).GetResynchronizationRequired()
	if resynchronization == nil ||
		resynchronization.GetReason() != opensplunk.ResynchronizationReason_RESYNCHRONIZATION_REASON_SEQUENCE_EXPIRED ||
		resynchronization.GetEarliestAvailableSequence() != secondProgress.GetSequence() ||
		resynchronization.GetLatestSequence() != secondProgress.GetSequence() {
		t.Fatalf("expired resynchronization = %+v", resynchronization)
	}

	var authoritative opensplunk.GetSearchJobResponse
	fixture.post("/api/search/jobs/get", &opensplunk.GetSearchJobRequest{SearchJobId: jobID}, &authoritative)
	if authoritative.GetSearchJob().GetState() != opensplunk.SearchJobState_SEARCH_JOB_STATE_RUNNING ||
		authoritative.GetSearchJob().GetProgress().GetScannedRows() != 15 ||
		authoritative.GetSearchJob().GetProgress().GetScannedBytes() != 150 ||
		authoritative.GetSearchJob().GetProgress().GetStateVersion() != secondProgress.GetSearchProgress().GetStateVersion() ||
		authoritative.GetSearchJob().GetStateVersion() != secondProgress.GetSearchProgress().GetStateVersion() ||
		fixture.executor.calls.Load() != 1 {
		t.Fatalf("authoritative running search = %+v calls=%d", authoritative.GetSearchJob(), fixture.executor.calls.Load())
	}
}

func TestSearchWebSocketRealManagerReplaysCancellationAfterOfflineSocket(t *testing.T) {
	const (
		jobID          = "search-ws-real-cancel"
		subscriptionID = "search-ws-cancel-subscription"
	)
	fixture := newRealSearchWebSocketFixture(t, jobID, 8)
	fixture.createRunningSearch()

	offline := fixture.dial()
	earliest, checkpoint := establishRealSearchWebSocketEpoch(t, offline, "initial", subscriptionID, jobID)
	keeper := fixture.dial()
	defer keeper.Close()
	writeSearchWebSocketCommand(
		t,
		keeper,
		searchWebSocketSubscribeCommand("keeper", "cancellation-journal-keeper", jobID, checkpoint),
	)
	keeperAckEvent := readSearchWebSocketEvent(t, keeper)
	keeperAck := keeperAckEvent.GetSubscriptionAcknowledged()
	if keeperAck == nil || keeperAck.GetRequestId() != "keeper" ||
		keeperAck.GetSubscriptionId() != "cancellation-journal-keeper" ||
		keeperAck.GetTarget().GetSearchJobId() != jobID ||
		keeperAck.GetEarliestAvailableSequence() != earliest ||
		keeperAck.GetLatestSequence() != checkpoint || keeperAck.GetReplayWillFollow() {
		t.Fatalf("cancellation keeper acknowledgment = %+v", keeperAckEvent)
	}
	if err := offline.Close(); err != nil {
		t.Fatalf("close offline websocket: %v", err)
	}

	var canceled opensplunk.CancelSearchJobResponse
	fixture.post("/api/search/jobs/cancel", &opensplunk.CancelSearchJobRequest{SearchJobId: jobID}, &canceled)
	cancellationVersion := canceled.GetSearchJob().GetStateVersion()
	if canceled.GetSearchJob().GetState() != opensplunk.SearchJobState_SEARCH_JOB_STATE_CANCELED ||
		cancellationVersion == 0 ||
		canceled.GetSearchJob().GetProgress().GetStateVersion() != cancellationVersion {
		t.Fatalf("cancel response = %+v", canceled.GetSearchJob())
	}
	timer := time.NewTimer(2 * time.Second)
	defer timer.Stop()
	select {
	case err := <-fixture.executor.exited:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("executor exit = %v, want context.Canceled", err)
		}
	case <-timer.C:
		t.Fatal("timed out waiting for canceled executor")
	}

	publishedState := readSearchWebSocketEvent(t, keeper)
	publishedProgress := readSearchWebSocketEvent(t, keeper)
	publishedTerminal := readSearchWebSocketEvent(t, keeper)
	if publishedState.GetSearchStateChanged().GetState() != opensplunk.SearchJobState_SEARCH_JOB_STATE_CANCELED ||
		publishedState.GetSearchStateChanged().GetStateVersion() != cancellationVersion ||
		publishedProgress.GetSearchProgress().GetPhase() != opensplunk.SearchExecutionPhase_SEARCH_EXECUTION_PHASE_COMPLETE ||
		publishedProgress.GetSearchProgress().GetStateVersion() != cancellationVersion ||
		publishedTerminal.GetSearchTerminal().GetState() != opensplunk.SearchJobState_SEARCH_JOB_STATE_CANCELED ||
		publishedTerminal.GetSearchTerminal().GetStateVersion() != cancellationVersion ||
		publishedTerminal.GetSearchTerminal().GetFinalProgress().GetStateVersion() != cancellationVersion ||
		!proto.Equal(publishedTerminal.GetSearchTerminal().GetFinalProgress(), publishedProgress.GetSearchProgress()) ||
		publishedTerminal.GetSearchTerminal().GetFailure() != nil ||
		publishedTerminal.GetSearchTerminal().GetResultsExpireAt() == nil ||
		publishedState.GetSequence() != checkpoint+1 ||
		publishedProgress.GetSequence() != publishedState.GetSequence()+1 ||
		publishedTerminal.GetSequence() != publishedProgress.GetSequence()+1 {
		t.Fatalf(
			"published canceled websocket projection = (%+v, %+v, %+v)",
			publishedState, publishedProgress, publishedTerminal,
		)
	}
	if err := keeper.Close(); err != nil {
		t.Fatalf("close cancellation journal keeper: %v", err)
	}

	reconnected := fixture.dial()
	defer reconnected.Close()
	writeSearchWebSocketCommand(t, reconnected, searchWebSocketSubscribeCommand("resume", subscriptionID, jobID, checkpoint))
	ackEvent := readSearchWebSocketEvent(t, reconnected)
	ack := ackEvent.GetSubscriptionAcknowledged()
	if ack == nil || ack.GetRequestId() != "resume" || ack.GetSubscriptionId() != subscriptionID ||
		ack.GetTarget().GetSearchJobId() != jobID ||
		ack.GetEarliestAvailableSequence() != earliest ||
		ack.GetLatestSequence() != publishedTerminal.GetSequence() || !ack.GetReplayWillFollow() {
		t.Fatalf("cancellation resume acknowledgment = %+v", ackEvent)
	}
	state := readSearchWebSocketEvent(t, reconnected)
	progress := readSearchWebSocketEvent(t, reconnected)
	terminal := readSearchWebSocketEvent(t, reconnected)
	if state.GetSearchStateChanged().GetState() != opensplunk.SearchJobState_SEARCH_JOB_STATE_CANCELED ||
		progress.GetSearchProgress().GetPhase() != opensplunk.SearchExecutionPhase_SEARCH_EXECUTION_PHASE_COMPLETE ||
		terminal.GetSearchTerminal().GetState() != opensplunk.SearchJobState_SEARCH_JOB_STATE_CANCELED ||
		terminal.GetSearchTerminal().GetFailure() != nil ||
		terminal.GetSearchTerminal().GetResultsExpireAt() == nil ||
		state.GetSequence() != checkpoint+1 ||
		progress.GetSequence() != state.GetSequence()+1 ||
		terminal.GetSequence() != progress.GetSequence()+1 ||
		!proto.Equal(state.GetSearchStateChanged(), publishedState.GetSearchStateChanged()) ||
		!proto.Equal(progress.GetSearchProgress(), publishedProgress.GetSearchProgress()) ||
		!proto.Equal(terminal.GetSearchTerminal(), publishedTerminal.GetSearchTerminal()) {
		t.Fatalf("canceled websocket projection = (%+v, %+v, %+v)", state, progress, terminal)
	}

	var authoritative opensplunk.GetSearchJobResponse
	fixture.post("/api/search/jobs/get", &opensplunk.GetSearchJobRequest{SearchJobId: jobID}, &authoritative)
	if authoritative.GetSearchJob().GetState() != opensplunk.SearchJobState_SEARCH_JOB_STATE_CANCELED ||
		authoritative.GetSearchJob().GetStateVersion() != cancellationVersion ||
		authoritative.GetSearchJob().GetProgress().GetStateVersion() != cancellationVersion ||
		terminal.GetSearchTerminal().GetStateVersion() != cancellationVersion ||
		!proto.Equal(authoritative.GetSearchJob().GetProgress(), publishedProgress.GetSearchProgress()) ||
		fixture.executor.calls.Load() != 1 || fixture.executor.exits.Load() != 1 {
		t.Fatalf(
			"authoritative canceled search = %+v cancel=%+v terminal=%+v calls=%d exits=%d",
			authoritative.GetSearchJob(), canceled.GetSearchJob(), terminal.GetSearchTerminal(),
			fixture.executor.calls.Load(), fixture.executor.exits.Load(),
		)
	}
}

func writeSearchWebSocketCommand(t *testing.T, connection *websocket.Conn, command *opensplunk.SearchWebSocketCommand) {
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

func readSearchWebSocketEvent(t *testing.T, connection *websocket.Conn) *opensplunk.SearchWebSocketEvent {
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
	event := new(opensplunk.SearchWebSocketEvent)
	if err := proto.Unmarshal(frame, event); err != nil {
		t.Fatalf("unmarshal websocket event: %v", err)
	}
	return event
}

func assertSearchWebSocketBootstrapLimits(t *testing.T, client *http.Client, serverURL string, service *searchws.Service) {
	t.Helper()
	payload, err := proto.Marshal(&opensplunk.GetSystemBootstrapRequest{})
	if err != nil {
		t.Fatalf("marshal bootstrap request: %v", err)
	}
	request, err := http.NewRequestWithContext(context.Background(), http.MethodPost, serverURL+"/api/system/bootstrap", bytes.NewReader(payload))
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
	var bootstrap opensplunk.GetSystemBootstrapResponse
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
				webSocketURL(serverURL),
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
	request, err := http.NewRequestWithContext(context.Background(), http.MethodPost, serverURL+searchWebSocketPath, nil)
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

func webSocketURL(serverURL string) string {
	return "ws" + strings.TrimPrefix(serverURL, "http") + searchWebSocketPath
}
