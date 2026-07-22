package main

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func TestOpenCollectorServerAllowsUnconfiguredListener(t *testing.T) {
	t.Parallel()
	server, listener, err := openCollectorServer(collectorServerConfig{}, nil)
	if err != nil || server != nil || listener != nil {
		t.Fatalf("openCollectorServer = (%v, %v, %v), want (nil, nil, nil)", server, listener, err)
	}
}

func TestCollectorGRPCServerOptionsRequireExplicitSafeTransport(t *testing.T) {
	t.Parallel()
	for name, config := range map[string]collectorServerConfig{
		"implicit plaintext": {
			Address: "127.0.0.1:8443",
		},
		"remote plaintext": {
			Address: "192.0.2.10:8443", Insecure: true,
		},
		"plaintext with certificate": {
			Address: "127.0.0.1:8443", Insecure: true, TLSCertFile: "server.crt",
		},
		"certificate without key": {
			Address: "127.0.0.1:8443", TLSCertFile: "server.crt",
		},
	} {
		config := config
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			if _, err := collectorGRPCServerOptions(config); err == nil {
				t.Fatal("collectorGRPCServerOptions succeeded")
			}
		})
	}
	if _, err := collectorGRPCServerOptions(collectorServerConfig{
		Address: "localhost:8443", Insecure: true,
	}); err != nil {
		t.Fatalf("explicit loopback plaintext rejected: %v", err)
	}
}

func TestOpenCollectorServerRejectsIncompleteConfigurationBeforeListening(t *testing.T) {
	t.Parallel()
	service := &unimplementedCollectorService{}
	for name, config := range map[string]collectorServerConfig{
		"transport without address": {Insecure: true},
		"invalid address":           {Address: "not-an-address", Insecure: true},
		"missing service":           {Address: "127.0.0.1:0", Insecure: true},
	} {
		config := config
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			candidate := opensplunkv1.CollectorIngestServiceServer(service)
			if name == "missing service" {
				candidate = nil
			}
			if _, _, err := openCollectorServer(config, candidate); err == nil {
				t.Fatal("openCollectorServer succeeded")
			}
		})
	}
}

func TestShutdownGRPCServerStopsAtDeadline(t *testing.T) {
	t.Parallel()
	server := newFakeGracefulGRPCServer()
	err := shutdownGRPCServer(server, 5*time.Millisecond)
	if err == nil {
		t.Fatal("shutdownGRPCServer returned nil")
	}
	select {
	case <-server.stopped:
	default:
		t.Fatal("shutdown did not force-stop the server")
	}
}

func TestShutdownGRPCServerReturnsAfterGracefulStop(t *testing.T) {
	t.Parallel()
	server := newFakeGracefulGRPCServer()
	close(server.release)
	if err := shutdownGRPCServer(server, time.Second); err != nil {
		t.Fatal(err)
	}
	select {
	case <-server.stopped:
		t.Fatal("graceful shutdown unnecessarily called Stop")
	default:
	}
}

func TestConcurrentStreamLimitRejectsExcessAndReleasesCapacity(t *testing.T) {
	t.Parallel()
	interceptor := concurrentStreamLimit(1)
	entered := make(chan struct{})
	release := make(chan struct{})
	firstDone := make(chan error, 1)
	go func() {
		firstDone <- interceptor(nil, fakeCollectorServerStream{}, nil, func(any, grpc.ServerStream) error {
			close(entered)
			<-release
			return nil
		})
	}()
	<-entered
	err := interceptor(nil, fakeCollectorServerStream{}, nil, func(any, grpc.ServerStream) error { return nil })
	if status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("excess stream error = %v, want ResourceExhausted", err)
	}
	close(release)
	if err := <-firstDone; err != nil {
		t.Fatal(err)
	}
	if err := interceptor(nil, fakeCollectorServerStream{}, nil, func(any, grpc.ServerStream) error { return nil }); err != nil {
		t.Fatalf("released stream capacity was not reusable: %v", err)
	}
}

func TestConnectionLimitedListenerReleasesSlotOnClose(t *testing.T) {
	t.Parallel()
	serverConnection, clientConnection := net.Pipe()
	t.Cleanup(func() { _ = clientConnection.Close() })
	underlying := &singleConnectionListener{connection: serverConnection}
	limited := newConnectionLimitedListener(underlying, 1).(*connectionLimitedListener)
	accepted, err := limited.Accept()
	if err != nil {
		t.Fatal(err)
	}
	if len(limited.slots) != 1 {
		t.Fatalf("occupied connection slots = %d, want 1", len(limited.slots))
	}
	if err := accepted.Close(); err != nil {
		t.Fatal(err)
	}
	if len(limited.slots) != 0 {
		t.Fatalf("occupied connection slots after close = %d, want 0", len(limited.slots))
	}
	// The important property of a second close is that capacity release remains
	// idempotent, regardless of the underlying connection's returned error.
	_ = accepted.Close()
	if len(limited.slots) != 0 {
		t.Fatal("double close released connection capacity twice")
	}
}

type unimplementedCollectorService struct {
	opensplunkv1.UnimplementedCollectorIngestServiceServer
}

type fakeCollectorServerStream struct{}

func (fakeCollectorServerStream) SetHeader(metadata.MD) error  { return nil }
func (fakeCollectorServerStream) SendHeader(metadata.MD) error { return nil }
func (fakeCollectorServerStream) SetTrailer(metadata.MD)       {}
func (fakeCollectorServerStream) Context() context.Context     { return context.Background() }
func (fakeCollectorServerStream) SendMsg(any) error            { return nil }
func (fakeCollectorServerStream) RecvMsg(any) error            { return nil }

type singleConnectionListener struct {
	connection net.Conn
	accepted   bool
}

func (listener *singleConnectionListener) Accept() (net.Conn, error) {
	if listener.accepted {
		return nil, errors.New("no more connections")
	}
	listener.accepted = true
	return listener.connection, nil
}

func (listener *singleConnectionListener) Close() error   { return nil }
func (listener *singleConnectionListener) Addr() net.Addr { return fakeCollectorAddress("single") }

type fakeCollectorAddress string

func (address fakeCollectorAddress) Network() string { return "test" }
func (address fakeCollectorAddress) String() string  { return string(address) }

type fakeGracefulGRPCServer struct {
	release chan struct{}
	stopped chan struct{}
}

func newFakeGracefulGRPCServer() *fakeGracefulGRPCServer {
	return &fakeGracefulGRPCServer{release: make(chan struct{}), stopped: make(chan struct{})}
}

func (server *fakeGracefulGRPCServer) GracefulStop() {
	<-server.release
}

func (server *fakeGracefulGRPCServer) Stop() {
	select {
	case <-server.stopped:
		return
	default:
		close(server.stopped)
		close(server.release)
	}
}
