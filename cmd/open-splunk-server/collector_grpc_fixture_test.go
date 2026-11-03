package main

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

func newRuntimeCollectorConnection(
	t *testing.T,
	service opensplunk.CollectorIngestServiceServer,
) *grpc.ClientConn {
	t.Helper()
	listener := bufconn.Listen(1 << 20)
	grpcServer := grpc.NewServer()
	opensplunk.RegisterCollectorIngestServiceServer(grpcServer, service)
	serveDone := make(chan error, 1)
	go func() {
		serveDone <- grpcServer.Serve(listener)
	}()
	t.Cleanup(func() {
		grpcServer.Stop()
		if err := listener.Close(); err != nil {
			t.Error(err)
		}
		select {
		case err := <-serveDone:
			if err != nil && !errors.Is(err, grpc.ErrServerStopped) {
				t.Errorf("serve collector: %v", err)
			}
		case <-time.After(time.Second):
			t.Error("collector gRPC server did not stop")
		}
	})

	connection, err := grpc.NewClient(
		"passthrough:///runtime-collector-fixture",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return listener.Dial()
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := connection.Close(); err != nil {
			t.Error(err)
		}
	})
	return connection
}
