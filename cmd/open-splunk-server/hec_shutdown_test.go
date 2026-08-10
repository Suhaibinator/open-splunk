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
)

type runtimeHECTestLifecycle struct {
	mu            sync.Mutex
	beginCalls    int
	shutdownCalls int
	shutdown      func(context.Context, int) error
	events        *[]string
}

func (lifecycle *runtimeHECTestLifecycle) BeginShutdown() {
	lifecycle.mu.Lock()
	defer lifecycle.mu.Unlock()
	lifecycle.beginCalls++
	if lifecycle.events != nil {
		*lifecycle.events = append(*lifecycle.events, "begin-admission")
	}
}

func (lifecycle *runtimeHECTestLifecycle) Shutdown(ctx context.Context) error {
	lifecycle.mu.Lock()
	lifecycle.shutdownCalls++
	call := lifecycle.shutdownCalls
	if lifecycle.events != nil {
		*lifecycle.events = append(*lifecycle.events, "drain-admission")
	}
	shutdown := lifecycle.shutdown
	lifecycle.mu.Unlock()
	if shutdown == nil {
		return nil
	}
	return shutdown(ctx, call)
}

func (lifecycle *runtimeHECTestLifecycle) snapshot() (int, int) {
	lifecycle.mu.Lock()
	defer lifecycle.mu.Unlock()
	return lifecycle.beginCalls, lifecycle.shutdownCalls
}

type orderedHECTestWebSockets struct {
	mu     *sync.Mutex
	events *[]string
}

func (webSockets *orderedHECTestWebSockets) Close(context.Context) error {
	webSockets.mu.Lock()
	defer webSockets.mu.Unlock()
	*webSockets.events = append(*webSockets.events, "close-websockets")
	return nil
}

type orderedHECTestShutdownServer struct {
	mu       *sync.Mutex
	events   *[]string
	requests *trackedHandler
	closed   bool
}

func (server *orderedHECTestShutdownServer) Shutdown(context.Context) error {
	server.requests.mu.Lock()
	stopping := server.requests.stopping
	server.requests.mu.Unlock()
	server.mu.Lock()
	defer server.mu.Unlock()
	if !stopping {
		return errors.New("HTTP tracker still accepted work when server shutdown began")
	}
	*server.events = append(*server.events, "shutdown-http")
	return nil
}

func (server *orderedHECTestShutdownServer) Close() error {
	server.mu.Lock()
	defer server.mu.Unlock()
	server.closed = true
	*server.events = append(*server.events, "close-http")
	return nil
}

func TestShutdownHTTPServerClosesHECAdmissionBeforeTransportDrain(t *testing.T) {
	var mu sync.Mutex
	events := make([]string, 0, 4)
	requests := newTrackedHandler(http.NotFoundHandler())
	lifecycle := &runtimeHECTestLifecycle{events: &events}
	server := &orderedHECTestShutdownServer{
		mu:       &mu,
		events:   &events,
		requests: requests,
	}
	webSockets := &orderedHECTestWebSockets{mu: &mu, events: &events}

	if err := shutdownHTTPServer(
		server,
		requests,
		webSockets,
		time.Second,
		lifecycle,
	); err != nil {
		t.Fatalf("shutdownHTTPServer(): %v", err)
	}
	beginCalls, shutdownCalls := lifecycle.snapshot()
	if beginCalls != 1 || shutdownCalls != 1 {
		t.Fatalf("admission lifecycle calls = (begin %d, shutdown %d), want (1, 1)", beginCalls, shutdownCalls)
	}
	mu.Lock()
	gotEvents := strings.Join(events, ",")
	mu.Unlock()
	if gotEvents != "begin-admission,close-websockets,shutdown-http,drain-admission" {
		t.Fatalf("shutdown order = %q", gotEvents)
	}

	response := httptest.NewRecorder()
	requests.ServeHTTP(
		response,
		httptest.NewRequest(http.MethodGet, "/after-shutdown", nil),
	)
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("post-shutdown tracker status = %d, want 503", response.Code)
	}
}

type deadlineHECTestShutdownServer struct {
	mu         sync.Mutex
	closeCalls int
}

func (server *deadlineHECTestShutdownServer) Shutdown(ctx context.Context) error {
	<-ctx.Done()
	return ctx.Err()
}

func (server *deadlineHECTestShutdownServer) Close() error {
	server.mu.Lock()
	defer server.mu.Unlock()
	server.closeCalls++
	return nil
}

func (server *deadlineHECTestShutdownServer) snapshot() int {
	server.mu.Lock()
	defer server.mu.Unlock()
	return server.closeCalls
}

func TestShutdownHTTPServerCancelsHECDrainAtDeadlineWithoutUnboundedRetry(t *testing.T) {
	server := &deadlineHECTestShutdownServer{}
	lifecycle := &runtimeHECTestLifecycle{}
	lifecycle.shutdown = func(ctx context.Context, call int) error {
		switch call {
		case 1:
			if ctx.Err() == nil {
				return errors.New("first HEC drain did not inherit the expired shutdown deadline")
			}
			return ctx.Err()
		default:
			return errors.New("unexpected additional HEC drain")
		}
	}

	err := shutdownHTTPServer(
		server,
		newTrackedHandler(http.NotFoundHandler()),
		&fakeWebSocketShutdown{},
		5*time.Millisecond,
		lifecycle,
	)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("shutdownHTTPServer() error = %v, want deadline exceeded", err)
	}
	if beginCalls, shutdownCalls := lifecycle.snapshot(); beginCalls != 1 || shutdownCalls != 1 {
		t.Fatalf("deadline lifecycle calls = (begin %d, shutdown %d), want (1, 1)", beginCalls, shutdownCalls)
	}
	if closeCalls := server.snapshot(); closeCalls != 1 {
		t.Fatalf("forced HTTP close calls = %d, want 1", closeCalls)
	}
}

func TestServeRuntimePropagatesHECAdmissionLifecycle(t *testing.T) {
	httpServer := newFakeRuntimeHTTPServer()
	webSockets := &fakeWebSocketShutdown{}
	lifecycle := &runtimeHECTestLifecycle{}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- serveRuntime(
			ctx,
			httpServer,
			newTrackedHandler(http.NotFoundHandler()),
			webSockets,
			nil,
			nil,
			time.Second,
			time.Second,
			lifecycle,
		)
	}()
	select {
	case <-httpServer.started:
	case <-time.After(time.Second):
		t.Fatal("HTTP runtime did not start")
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("serveRuntime(): %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("serveRuntime did not finish after cancellation")
	}
	if beginCalls, shutdownCalls := lifecycle.snapshot(); beginCalls != 1 || shutdownCalls != 1 {
		t.Fatalf("serve runtime admission calls = (begin %d, shutdown %d), want (1, 1)", beginCalls, shutdownCalls)
	}
}
