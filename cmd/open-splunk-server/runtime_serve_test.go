package main

import (
	"context"
	"errors"
	"net"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestServeRuntimeCancellationStopsBothTransports(t *testing.T) {
	t.Parallel()
	httpServer := newFakeRuntimeHTTPServer()
	collectorServer := newFakeRuntimeGRPCServer()
	webSockets := &fakeWebSocketShutdown{}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- serveRuntime(ctx, httpServer, newTrackedHandler(http.NotFoundHandler()), webSockets, collectorServer, fakeRuntimeListener{}, time.Second)
	}()
	<-httpServer.started
	<-collectorServer.started
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if !webSockets.wasClosed() || !httpServer.wasShutdown() || !collectorServer.wasGracefullyStopped() {
		t.Fatal("cancellation did not stop both transports")
	}
}

func TestServeRuntimeHTTPFailureStopsCollector(t *testing.T) {
	t.Parallel()
	httpServer := newFakeRuntimeHTTPServer()
	httpServer.serveErr = errors.New("bind failed")
	collectorServer := newFakeRuntimeGRPCServer()
	webSockets := &fakeWebSocketShutdown{}
	err := serveRuntime(context.Background(), httpServer, newTrackedHandler(http.NotFoundHandler()), webSockets, collectorServer, fakeRuntimeListener{}, time.Second)
	if !errors.Is(err, httpServer.serveErr) {
		t.Fatalf("serveRuntime error = %v, want HTTP failure", err)
	}
	if !webSockets.wasClosed() || !collectorServer.wasGracefullyStopped() {
		t.Fatal("HTTP failure did not stop websocket and collector transports")
	}
}

func TestServeRuntimeCollectorFailureStopsHTTP(t *testing.T) {
	t.Parallel()
	httpServer := newFakeRuntimeHTTPServer()
	collectorServer := newFakeRuntimeGRPCServer()
	collectorServer.serveErr = errors.New("accept failed")
	webSockets := &fakeWebSocketShutdown{}
	err := serveRuntime(context.Background(), httpServer, newTrackedHandler(http.NotFoundHandler()), webSockets, collectorServer, fakeRuntimeListener{}, time.Second)
	if !errors.Is(err, collectorServer.serveErr) {
		t.Fatalf("serveRuntime error = %v, want collector failure", err)
	}
	if !webSockets.wasClosed() || !httpServer.wasShutdown() {
		t.Fatal("collector failure did not stop websocket and HTTP transports")
	}
}

func TestServeRuntimeRejectsNilWebSocketService(t *testing.T) {
	t.Parallel()
	httpServer := newFakeRuntimeHTTPServer()
	err := serveRuntime(context.Background(), httpServer, newTrackedHandler(http.NotFoundHandler()), nil, nil, nil, time.Second)
	if err == nil || !strings.Contains(err.Error(), "websocket service") {
		t.Fatalf("nil websocket error = %v", err)
	}
	var typedNil *fakeWebSocketShutdown
	err = serveRuntime(context.Background(), httpServer, newTrackedHandler(http.NotFoundHandler()), typedNil, nil, nil, time.Second)
	if err == nil || !strings.Contains(err.Error(), "websocket service") {
		t.Fatalf("typed-nil websocket error = %v", err)
	}
}

type fakeRuntimeHTTPServer struct {
	started   chan struct{}
	stopped   chan struct{}
	serveErr  error
	startOnce sync.Once
	stopOnce  sync.Once
	mu        sync.Mutex
	shutdown  bool
}

func newFakeRuntimeHTTPServer() *fakeRuntimeHTTPServer {
	return &fakeRuntimeHTTPServer{started: make(chan struct{}), stopped: make(chan struct{})}
}

func (server *fakeRuntimeHTTPServer) ListenAndServe() error {
	server.startOnce.Do(func() { close(server.started) })
	if server.serveErr != nil {
		return server.serveErr
	}
	<-server.stopped
	return http.ErrServerClosed
}

func (server *fakeRuntimeHTTPServer) Shutdown(context.Context) error {
	server.mu.Lock()
	server.shutdown = true
	server.mu.Unlock()
	server.stopOnce.Do(func() { close(server.stopped) })
	return nil
}

func (server *fakeRuntimeHTTPServer) Close() error {
	server.stopOnce.Do(func() { close(server.stopped) })
	return nil
}

func (server *fakeRuntimeHTTPServer) wasShutdown() bool {
	server.mu.Lock()
	defer server.mu.Unlock()
	return server.shutdown
}

type fakeRuntimeGRPCServer struct {
	started      chan struct{}
	stopped      chan struct{}
	serveErr     error
	startOnce    sync.Once
	stopOnce     sync.Once
	mu           sync.Mutex
	gracefulStop bool
}

func newFakeRuntimeGRPCServer() *fakeRuntimeGRPCServer {
	return &fakeRuntimeGRPCServer{started: make(chan struct{}), stopped: make(chan struct{})}
}

func (server *fakeRuntimeGRPCServer) Serve(net.Listener) error {
	server.startOnce.Do(func() { close(server.started) })
	if server.serveErr != nil {
		return server.serveErr
	}
	<-server.stopped
	return nil
}

func (server *fakeRuntimeGRPCServer) GracefulStop() {
	server.mu.Lock()
	server.gracefulStop = true
	server.mu.Unlock()
	server.stopOnce.Do(func() { close(server.stopped) })
}

func (server *fakeRuntimeGRPCServer) Stop() {
	server.stopOnce.Do(func() { close(server.stopped) })
}

func (server *fakeRuntimeGRPCServer) wasGracefullyStopped() bool {
	server.mu.Lock()
	defer server.mu.Unlock()
	return server.gracefulStop
}

type fakeRuntimeListener struct{}

func (fakeRuntimeListener) Accept() (net.Conn, error) { return nil, errors.New("not implemented") }
func (fakeRuntimeListener) Close() error              { return nil }
func (fakeRuntimeListener) Addr() net.Addr            { return fakeRuntimeAddress("collector") }

type fakeRuntimeAddress string

func (address fakeRuntimeAddress) Network() string { return "test" }
func (address fakeRuntimeAddress) String() string  { return string(address) }
