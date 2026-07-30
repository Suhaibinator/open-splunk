package main

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	exportjobs "github.com/Suhaibinator/open-splunk/internal/export"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
	"github.com/Suhaibinator/open-splunk/internal/searchws"
	"github.com/gorilla/websocket"
	"google.golang.org/protobuf/proto"
)

func TestTrackedHandlerRejectsNewWorkAndWaitsForActiveWork(t *testing.T) {
	t.Parallel()
	entered := make(chan struct{})
	release := make(chan struct{})
	tracked := newTrackedHandler(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		close(entered)
		<-release
	}))

	firstDone := make(chan struct{})
	go func() {
		defer close(firstDone)
		tracked.ServeHTTP(
			httptest.NewRecorder(),
			httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/", nil),
		)
	}()
	<-entered
	tracked.stopAccepting()

	response := httptest.NewRecorder()
	tracked.ServeHTTP(
		response,
		httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/", nil),
	)
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("new request status = %d, want 503", response.Code)
	}

	waitDone := make(chan struct{})
	go func() {
		defer close(waitDone)
		tracked.wait()
	}()
	select {
	case <-waitDone:
		t.Fatal("wait returned while the handler was active")
	case <-time.After(10 * time.Millisecond):
	}
	close(release)
	<-firstDone
	<-waitDone
}

func TestShutdownDrainsDeletionAdmissionWakeBeforeRuntimeClose(t *testing.T) {
	t.Parallel()

	wakeEntered := make(chan struct{})
	releaseWake := make(chan struct{})
	wakeReturned := make(chan struct{})
	var releaseOnce sync.Once
	releaseAdmission := func() {
		releaseOnce.Do(func() {
			close(releaseWake)
		})
	}
	store := &runtimeDeletionStore{
		close: func() error {
			select {
			case <-wakeReturned:
				return nil
			default:
				return errors.New(
					"deletion Store closed before admitted request returned from Wake",
				)
			}
		},
	}
	runtime, err := newIndexDataDeletionRuntime(
		&runtimeDeletionControl{},
		store,
		"tenant-a",
		nil,
	)
	if err != nil {
		t.Fatalf("newIndexDataDeletionRuntime(): %v", err)
	}
	t.Cleanup(func() {
		closeErr := runtime.Close(context.Background())
		if closeErr != nil {
			t.Errorf("Close(): %v", closeErr)
		}
	})
	t.Cleanup(releaseAdmission)
	tracked := newTrackedHandler(http.HandlerFunc(
		func(http.ResponseWriter, *http.Request) {
			close(wakeEntered)
			<-releaseWake
			runtime.Wake()
			close(wakeReturned)
		},
	))
	handlerDone := make(chan struct{})
	go func() {
		defer close(handlerDone)
		tracked.ServeHTTP(
			httptest.NewRecorder(),
			httptest.NewRequestWithContext(
				context.Background(),
				http.MethodPost,
				"/api/v1/indexes/delete",
				nil,
			),
		)
	}()
	select {
	case <-wakeEntered:
	case <-time.After(time.Second):
		t.Fatal("deletion admission handler did not reach postcommit Wake")
	}

	lifecycleDone := make(chan error, 1)
	shutdownServer := &immediateShutdownServer{}
	go func() {
		shutdownErr := shutdownHTTPServer(
			shutdownServer,
			tracked,
			&fakeWebSocketShutdown{},
			time.Second,
		)
		if shutdownErr != nil {
			lifecycleDone <- shutdownErr
			return
		}
		lifecycleDone <- runtime.Close(context.Background())
	}()
	select {
	case <-tracked.drainStarted:
	case <-time.After(time.Second):
		t.Fatal("HTTP shutdown did not reach the active-request drain")
	}
	if calls := store.closeCalls.Load(); calls != 0 {
		t.Fatalf("Store.Close calls during active admission = %d, want 0", calls)
	}

	releaseAdmission()
	select {
	case <-handlerDone:
	case <-time.After(time.Second):
		t.Fatal("deletion admission handler did not return")
	}
	select {
	case lifecycleErr := <-lifecycleDone:
		if lifecycleErr != nil {
			t.Fatalf("shutdown lifecycle: %v", lifecycleErr)
		}
	case <-time.After(time.Second):
		t.Fatal("shutdown lifecycle did not close deletion runtime")
	}
	if calls := store.closeCalls.Load(); calls != 1 {
		t.Fatalf("Store.Close calls after request drain = %d, want 1", calls)
	}
}

func TestShutdownHTTPServerForceClosesThenWaitsForHandlers(t *testing.T) {
	t.Parallel()
	entered := make(chan struct{})
	release := make(chan struct{})
	tracked := newTrackedHandler(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		close(entered)
		<-release
	}))
	go tracked.ServeHTTP(
		httptest.NewRecorder(),
		httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/", nil),
	)
	<-entered

	server := &fakeShutdownServer{closed: make(chan struct{})}
	webSockets := &fakeWebSocketShutdown{}
	shutdownDone := make(chan error, 1)
	go func() { shutdownDone <- shutdownHTTPServer(server, tracked, webSockets, 5*time.Millisecond) }()
	select {
	case <-server.closed:
	case <-time.After(time.Second):
		t.Fatal("server was not force-closed after its shutdown deadline")
	}
	select {
	case <-shutdownDone:
		t.Fatal("shutdown returned before the active handler completed")
	default:
	}
	close(release)
	if err := <-shutdownDone; !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("shutdown error = %v, want context deadline exceeded", err)
	}
}

func TestShutdownHTTPServerClosesWebSocketsBeforeHTTPServer(t *testing.T) {
	t.Parallel()
	webSockets := &fakeWebSocketShutdown{}
	server := &orderedShutdownServer{webSockets: webSockets}
	tracked := newTrackedHandler(http.NotFoundHandler())
	if err := shutdownHTTPServer(server, tracked, webSockets, time.Second); err != nil {
		t.Fatal(err)
	}
	if !webSockets.wasClosed() || !server.wasShutdown() {
		t.Fatalf("shutdown state: websocket=%v HTTP=%v", webSockets.wasClosed(), server.wasShutdown())
	}

	closeErr := errors.New("websocket close failed")
	webSockets = &fakeWebSocketShutdown{err: closeErr}
	server = &orderedShutdownServer{webSockets: webSockets}
	tracked = newTrackedHandler(http.NotFoundHandler())
	err := shutdownHTTPServer(server, tracked, webSockets, time.Second)
	if !errors.Is(err, closeErr) || !server.wasShutdown() {
		t.Fatalf("shutdown error = %v, HTTP shutdown=%v", err, server.wasShutdown())
	}
}

func TestShutdownHTTPServerUnblocksActiveWebSocketHandlerEvenOnCloseError(t *testing.T) {
	t.Parallel()
	closeErr := errors.New("graceful websocket close timed out")
	webSockets := &activeWebSocketShutdown{
		entered: make(chan struct{}),
		closed:  make(chan struct{}),
		err:     closeErr,
	}
	tracked := newTrackedHandler(webSockets)
	handlerDone := make(chan struct{})
	go func() {
		defer close(handlerDone)
		tracked.ServeHTTP(
			httptest.NewRecorder(),
			httptest.NewRequestWithContext(
				context.Background(),
				http.MethodGet,
				"/api/v1/search/ws",
				nil,
			),
		)
	}()
	select {
	case <-webSockets.entered:
	case <-time.After(time.Second):
		t.Fatal("websocket handler did not start")
	}

	server := &immediateShutdownServer{}
	shutdownDone := make(chan error, 1)
	go func() { shutdownDone <- shutdownHTTPServer(server, tracked, webSockets, time.Second) }()
	select {
	case err := <-shutdownDone:
		if !errors.Is(err, closeErr) {
			t.Fatalf("shutdown error = %v, want close error", err)
		}
	case <-time.After(time.Second):
		t.Fatal("shutdown remained blocked on the active websocket handler")
	}
	select {
	case <-handlerDone:
	default:
		t.Fatal("active websocket handler did not return")
	}
}

func TestShutdownHTTPServerClosesActualUpgradedWebSocket(t *testing.T) {
	blockingSearches := &shutdownBlockingSearchSnapshots{
		started: make(chan struct{}),
		exited:  make(chan struct{}),
	}
	webSockets, err := searchws.New(searchws.Config{
		Searches: blockingSearches,
		Exports:  shutdownExportSnapshots{},
		Access: searchjobs.AccessScope{
			TenantID: "shutdown-tenant",
			OwnerID:  "shutdown-owner",
		},
		CheckOrigin:  func(*http.Request) bool { return true },
		PingInterval: time.Second,
		PongTimeout:  2 * time.Second,
	})
	if err != nil {
		t.Fatalf("searchws.New: %v", err)
	}
	requests := newTrackedHandler(webSockets)
	httpServer := httptest.NewUnstartedServer(requests)
	httpServer.Start()

	var connection *websocket.Conn
	t.Cleanup(func() {
		if connection != nil {
			_ = connection.Close()
		}
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if err := webSockets.Close(ctx); err != nil {
			t.Errorf("close websocket service cleanup: %v", err)
		}
		httpServer.Close()
	})

	dialer := websocket.Dialer{HandshakeTimeout: time.Second}
	connection, response, err := dialer.Dial(
		"ws"+strings.TrimPrefix(httpServer.URL, "http")+"/api/v1/search/ws",
		nil,
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
		t.Fatalf("upgrade response = %#v", response)
	}
	_ = response.Body.Close()

	command := &opensplunkv1.SearchWebSocketCommand{
		RequestId: "shutdown-blocked-provider",
		Payload: &opensplunkv1.SearchWebSocketCommand_Subscribe{
			Subscribe: &opensplunkv1.SubscribeSearchJobsCommand{
				Subscriptions: []*opensplunkv1.SearchSubscription{{
					SubscriptionId: "shutdown-subscription",
					Target: &opensplunkv1.JobTarget{
						Target: &opensplunkv1.JobTarget_SearchJobId{
							SearchJobId: "shutdown-search",
						},
					},
				}},
			},
		},
	}
	payload, err := proto.MarshalOptions{Deterministic: true}.Marshal(command)
	if err != nil {
		t.Fatalf("marshal subscribe command: %v", err)
	}
	if err := connection.WriteMessage(websocket.BinaryMessage, payload); err != nil {
		t.Fatalf("write subscribe command: %v", err)
	}
	select {
	case <-blockingSearches.started:
	case <-time.After(time.Second):
		t.Fatal("websocket subscription did not enter the snapshot provider")
	}

	shutdownDone := make(chan error, 1)
	go func() {
		shutdownDone <- shutdownHTTPServer(httpServer.Config, requests, webSockets, 2*time.Second)
	}()
	select {
	case err := <-shutdownDone:
		if err != nil {
			t.Fatalf("shutdownHTTPServer: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("shutdownHTTPServer remained blocked on the upgraded websocket handler")
	}

	if err := connection.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatalf("set post-shutdown read deadline: %v", err)
	}
	if _, _, err := connection.ReadMessage(); err == nil {
		t.Fatal("upgraded websocket remained readable after shutdownHTTPServer returned")
	}
	select {
	case <-blockingSearches.exited:
	default:
		t.Fatal("runtime shutdown returned before the snapshot provider exited")
	}
}

type shutdownBlockingSearchSnapshots struct {
	started chan struct{}
	exited  chan struct{}
	once    sync.Once
}

func (snapshots *shutdownBlockingSearchSnapshots) GetForContext(
	ctx context.Context,
	_ searchjobs.AccessScope,
	_ string,
) (searchjobs.Job, error) {
	snapshots.once.Do(func() { close(snapshots.started) })
	<-ctx.Done()
	close(snapshots.exited)
	return searchjobs.Job{}, ctx.Err()
}

func (*shutdownBlockingSearchSnapshots) PreviewForBytesContext(
	ctx context.Context,
	_ searchjobs.AccessScope,
	_ string,
	_ int,
	_ uint64,
) (searchjobs.PreviewSnapshot, error) {
	if err := ctx.Err(); err != nil {
		return searchjobs.PreviewSnapshot{}, err
	}
	return searchjobs.PreviewSnapshot{}, searchjobs.ErrResultsNotReady
}

func (*shutdownBlockingSearchSnapshots) MaximumPreviewRows() uint32 { return 100 }

type shutdownExportSnapshots struct{}

func (shutdownExportSnapshots) Get(context.Context, searchjobs.AccessScope, string) (exportjobs.Job, error) {
	return exportjobs.Job{}, exportjobs.ErrNotFound
}

func (jobs shutdownExportSnapshots) Snapshot(
	ctx context.Context,
	scope searchjobs.AccessScope,
	id string,
) (exportjobs.Job, error) {
	return jobs.Get(ctx, scope, id)
}

type activeWebSocketShutdown struct {
	entered   chan struct{}
	closed    chan struct{}
	enterOnce sync.Once
	closeOnce sync.Once
	err       error
}

func (service *activeWebSocketShutdown) ServeHTTP(http.ResponseWriter, *http.Request) {
	service.enterOnce.Do(func() { close(service.entered) })
	<-service.closed
}

func (service *activeWebSocketShutdown) Close(context.Context) error {
	service.closeOnce.Do(func() { close(service.closed) })
	return service.err
}

type immediateShutdownServer struct {
	mu       sync.Mutex
	shutdown bool
}

func (server *immediateShutdownServer) Shutdown(context.Context) error {
	server.mu.Lock()
	server.shutdown = true
	server.mu.Unlock()
	return nil
}

func (*immediateShutdownServer) Close() error { return nil }

type fakeWebSocketShutdown struct {
	mu     sync.Mutex
	closed bool
	err    error
}

func (service *fakeWebSocketShutdown) Close(context.Context) error {
	service.mu.Lock()
	service.closed = true
	service.mu.Unlock()
	return service.err
}

func (service *fakeWebSocketShutdown) wasClosed() bool {
	service.mu.Lock()
	defer service.mu.Unlock()
	return service.closed
}

type orderedShutdownServer struct {
	mu         sync.Mutex
	webSockets *fakeWebSocketShutdown
	shutdown   bool
}

func (server *orderedShutdownServer) Shutdown(context.Context) error {
	if !server.webSockets.wasClosed() {
		return errors.New("HTTP shutdown ran before websocket shutdown")
	}
	server.mu.Lock()
	server.shutdown = true
	server.mu.Unlock()
	return nil
}

func (*orderedShutdownServer) Close() error { return nil }

func (server *orderedShutdownServer) wasShutdown() bool {
	server.mu.Lock()
	defer server.mu.Unlock()
	return server.shutdown
}

type fakeShutdownServer struct {
	closeOnce sync.Once
	closed    chan struct{}
}

func (*fakeShutdownServer) Shutdown(ctx context.Context) error {
	<-ctx.Done()
	return ctx.Err()
}

func (server *fakeShutdownServer) Close() error {
	server.closeOnce.Do(func() { close(server.closed) })
	return nil
}
