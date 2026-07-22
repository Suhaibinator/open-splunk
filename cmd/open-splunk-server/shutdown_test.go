package main

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
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
	shutdownDone := make(chan error, 1)
	go func() { shutdownDone <- shutdownHTTPServer(server, tracked, 5*time.Millisecond) }()
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
