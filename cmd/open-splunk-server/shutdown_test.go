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

	exportjobs "github.com/Suhaibinator/open-splunk/internal/export"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
	"github.com/Suhaibinator/open-splunk/internal/searchws"
	"github.com/gorilla/websocket"
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
		tracked.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/", nil))
	}()
	<-entered
	tracked.stopAccepting()

	response := httptest.NewRecorder()
	tracked.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/", nil))
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

func TestShutdownHTTPServerForceClosesThenWaitsForHandlers(t *testing.T) {
	t.Parallel()
	entered := make(chan struct{})
	release := make(chan struct{})
	tracked := newTrackedHandler(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		close(entered)
		<-release
	}))
	go tracked.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/", nil))
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
		tracked.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/api/v1/search/ws", nil))
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
	webSockets, err := searchws.New(searchws.Config{
		Searches: shutdownSearchSnapshots{},
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
}

type shutdownSearchSnapshots struct{}

func (shutdownSearchSnapshots) GetFor(searchjobs.AccessScope, string) (searchjobs.Job, error) {
	return searchjobs.Job{}, searchjobs.ErrNotFound
}

type shutdownExportSnapshots struct{}

func (shutdownExportSnapshots) Get(context.Context, searchjobs.AccessScope, string) (exportjobs.Job, error) {
	return exportjobs.Job{}, exportjobs.ErrNotFound
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
