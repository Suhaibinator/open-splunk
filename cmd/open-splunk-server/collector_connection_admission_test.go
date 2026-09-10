package main

import (
	"context"
	"crypto/tls"
	"errors"
	"io"
	"net"
	"path/filepath"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/auth"
	"github.com/Suhaibinator/open-splunk/internal/collectoradmission"
	"github.com/Suhaibinator/open-splunk/internal/collectorfleet"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/ingest"
	"github.com/Suhaibinator/open-splunk/internal/testsupport"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/stats"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestCollectorConnectionRequiresServerReadyBeforeDeadline(t *testing.T) {
	ready := &opensplunk.CollectResponse{
		Payload: &opensplunk.CollectResponse_Ready{Ready: &opensplunk.CollectorReady{}},
	}
	for name, event := range map[string]stats.RPCStats{
		"no RPC":               &stats.Begin{},
		"RPC headers":          &stats.InHeader{},
		"incoming payload":     &stats.InPayload{Payload: ready},
		"client Ready":         &stats.OutPayload{Client: true, Payload: ready},
		"other response":       &stats.OutPayload{Payload: &opensplunk.CollectResponse{}},
		"authentication error": &stats.End{Error: status.Error(codes.Unauthenticated, "invalid token")},
	} {
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				listener, connection := newCollectorDeadlineTestConnection(t)
				ctx := collectorDeadlineTestContext(listener, connection.RemoteAddr())
				// Transport setup normally clears the socket deadline. It must
				// not clear the independent accept-to-Ready budget.
				if err := connection.SetDeadline(time.Time{}); err != nil {
					t.Fatal(err)
				}
				time.Sleep(collectorReadyTimeout - time.Second)
				listener.HandleRPC(ctx, event)
				if len(listener.slots) != 1 {
					t.Fatal("connection lost its setup grace period")
				}
				time.Sleep(time.Second)
				synctest.Wait()
				if len(listener.slots) != 0 || len(listener.connections) != 0 {
					t.Fatal("connection without Ready retained capacity after its deadline")
				}
				listener.HandleRPC(ctx, &stats.OutPayload{Payload: ready})
				if connection.ready {
					t.Fatal("late Ready revived an expired connection")
				}
			})
		})
	}
}

func TestCollectorConnectionReadyPreservesEstablishedLifetime(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		listener, connection := newCollectorDeadlineTestConnection(t)
		ctx := collectorDeadlineTestContext(listener, connection.RemoteAddr())
		time.Sleep(collectorReadyTimeout - time.Second)
		listener.HandleRPC(ctx, &stats.OutPayload{Payload: &opensplunk.CollectResponse{
			Payload: &opensplunk.CollectResponse_Ready{Ready: &opensplunk.CollectorReady{}},
		}})
		time.Sleep(2 * collectorReadyTimeout)
		synctest.Wait()
		if !connection.ready || connection.closed || len(listener.slots) != 1 {
			t.Fatal("successful Ready did not preserve the established connection")
		}
		_ = connection.Close()
		_ = connection.Close()
		if len(listener.slots) != 0 || len(listener.connections) != 0 {
			t.Fatal("established connection close did not release capacity exactly once")
		}
	})
}

func TestCollectorConnectionReadyCannotPromoteAnotherMethodOrAddressGeneration(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		listener, connection := newCollectorDeadlineTestConnection(t)
		address := connection.RemoteAddr().(*net.TCPAddr)
		oldAddress := *address
		for _, ctx := range []context.Context{
			collectorDeadlineTestContext(listener, &oldAddress),
			listener.TagRPC(collectorDeadlineTestContext(listener, address), &stats.RPCTagInfo{
				FullMethodName: "/other.Service/Collect",
			}),
		} {
			listener.HandleRPC(ctx, &stats.OutPayload{Payload: &opensplunk.CollectResponse{
				Payload: &opensplunk.CollectResponse_Ready{Ready: &opensplunk.CollectorReady{}},
			}})
		}
		time.Sleep(collectorReadyTimeout)
		synctest.Wait()
		if connection.ready || len(listener.slots) != 0 {
			t.Fatal("unrelated Ready promoted the pending connection")
		}
	})
}

func TestCollectorConnectionCloseReadyAndTimeoutReleaseOnce(t *testing.T) {
	t.Parallel()
	listener, connection := newCollectorDeadlineTestConnection(t)
	var done sync.WaitGroup
	for _, action := range []func(){
		connection.markReady,
		connection.expireBeforeReady,
		func() { _ = connection.Close() },
	} {
		done.Go(action)
	}
	done.Wait()
	if len(listener.slots) != 0 || len(listener.connections) != 0 {
		t.Fatal("concurrent lifecycle transitions leaked connection capacity")
	}
}

func newCollectorDeadlineTestConnection(t *testing.T) (*connectionLimitedListener, *limitedConnection) {
	t.Helper()
	server, client := net.Pipe()
	t.Cleanup(func() { _ = client.Close() })
	listener := newConnectionLimitedListener(&singleConnectionListener{
		connection: &collectorDeadlineTestConnection{Conn: server, remote: &net.TCPAddr{Port: 1234}},
	}, 1)
	accepted, err := listener.Accept()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = accepted.Close() })
	return listener, accepted.(*limitedConnection)
}

type collectorDeadlineTestConnection struct {
	net.Conn
	remote *net.TCPAddr
}

func (connection *collectorDeadlineTestConnection) RemoteAddr() net.Addr { return connection.remote }

func collectorDeadlineTestContext(listener *connectionLimitedListener, address net.Addr) context.Context {
	ctx := listener.TagConn(context.Background(), &stats.ConnTagInfo{RemoteAddr: address})
	return listener.TagRPC(ctx, &stats.RPCTagInfo{
		FullMethodName: opensplunk.CollectorIngestService_Collect_FullMethodName,
	})
}

func TestOpenCollectorServerExpiresPendingConnectionAndPreservesAuthenticatedStream(t *testing.T) {
	for _, secure := range []bool{false, true} {
		name := "plaintext"
		if secure {
			name = "TLS"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			service, token := newCollectorDeadlineTestService(t)
			config := collectorServerConfig{Address: "127.0.0.1:0", Insecure: !secure}
			clientCredentials := insecure.NewCredentials()
			if secure {
				identity, err := testsupport.WriteServerTLSIdentity(t.TempDir(), "127.0.0.1")
				if err != nil {
					t.Fatal(err)
				}
				config.TLSCertFile, config.TLSKeyFile = identity.CertificateFile, identity.PrivateKeyFile
				clientCredentials = credentials.NewTLS(&tls.Config{
					RootCAs: identity.RootCAs, MinVersion: tls.VersionTLS12,
				})
			}
			server, rawListener, err := openCollectorServer(context.Background(), config, service)
			if err != nil {
				t.Fatal(err)
			}
			listener := rawListener.(*connectionLimitedListener)
			listener.slots = make(chan struct{}, 1)
			listener.readyTimeout = 2 * time.Second
			go func() { _ = server.Serve(listener) }()
			t.Cleanup(server.Stop)
			client, err := grpc.NewClient(listener.Addr().String(),
				grpc.WithTransportCredentials(clientCredentials),
				grpc.WithContextDialer(func(ctx context.Context, address string) (net.Conn, error) {
					connection, err := (&net.Dialer{}).DialContext(ctx, "tcp", address)
					if err == nil {
						// Leave part of the setup budget for a slow TLS/HTTP2 handshake.
						time.Sleep(listener.readyTimeout / 4)
					}
					return connection, err
				}),
			)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = client.Close() })
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			client.Connect()
			for state := client.GetState(); state != connectivity.Ready; state = client.GetState() {
				if !client.WaitForStateChange(ctx, state) {
					t.Fatal("transport did not become ready")
				}
			}
			// A normal gRPC connection without an RPC must relinquish the sole
			// provisional slot even after TLS and HTTP/2 setup have completed.
			if !client.WaitForStateChange(ctx, connectivity.Ready) {
				t.Fatal("pending transport was not closed at its deadline")
			}
			waitForCollectorDeadlineSlots(t, ctx, listener, 0)
			invalidContext := metadata.NewOutgoingContext(ctx, metadata.Pairs("authorization", "Bearer invalid-token"))
			invalidStream, err := opensplunk.NewCollectorIngestServiceClient(client).Collect(invalidContext)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := invalidStream.Recv(); status.Code(err) != codes.Unauthenticated {
				t.Fatalf("invalid authentication = %v, want Unauthenticated", err)
			}
			if !client.WaitForStateChange(ctx, connectivity.Ready) {
				t.Fatal("rejected authentication retained its transport")
			}
			waitForCollectorDeadlineSlots(t, ctx, listener, 0)
			validContext := metadata.NewOutgoingContext(ctx, metadata.Pairs("authorization", "Bearer "+token))
			stream, err := opensplunk.NewCollectorIngestServiceClient(client).Collect(validContext)
			if err != nil {
				t.Fatal(err)
			}
			// Delay Hello within the setup budget to cover legitimate slow setup.
			time.Sleep(listener.readyTimeout / 4)
			now := time.Now().UTC()
			if err := stream.Send(&opensplunk.CollectRequest{
				StreamSequence: 1, SentAt: timestamppb.New(now),
				Payload: &opensplunk.CollectRequest_Hello{Hello: &opensplunk.CollectorHello{
					CollectorId: "deadline-collector", InstanceId: "deadline-instance",
					SourceRevision: "development", Hostname: "deadline-host", StartedAt: timestamppb.New(now),
				}},
			}); err != nil {
				t.Fatal(err)
			}
			if response, err := stream.Recv(); err != nil || response.GetReady() == nil {
				t.Fatalf("authenticated Ready = (%v, %v)", response, err)
			}
			time.Sleep(listener.readyTimeout + time.Second/4)
			now = time.Now().UTC()
			if err := stream.Send(&opensplunk.CollectRequest{
				StreamSequence: 2, SentAt: timestamppb.New(now),
				Payload: &opensplunk.CollectRequest_Heartbeat{Heartbeat: &opensplunk.CollectorHeartbeat{
					CollectorId: "deadline-collector", InstanceId: "deadline-instance",
					ObservedAt: timestamppb.New(now), Queue: &opensplunk.CollectorQueueStats{},
				}},
			}); err != nil {
				t.Fatal(err)
			}
			if err := stream.Send(&opensplunk.CollectRequest{
				StreamSequence: 3, SentAt: timestamppb.New(now),
				Payload: &opensplunk.CollectRequest_Goodbye{Goodbye: &opensplunk.CollectorGoodbye{
					Reason: opensplunk.CollectorGoodbyeReason_COLLECTOR_GOODBYE_REASON_SHUTDOWN,
				}},
			}); err != nil {
				t.Fatal(err)
			}
			if response, err := stream.Recv(); response != nil || !errors.Is(err, io.EOF) {
				t.Fatalf("heartbeat and Goodbye after setup deadline = (%v, %v), want EOF", response, err)
			}
			cancel()
			_ = client.Close()
			server.Stop()
			listener.mu.Lock()
			defer listener.mu.Unlock()
			if len(listener.slots) != 0 || len(listener.connections) != 0 {
				t.Fatal("shutdown leaked connection capacity")
			}
		})
	}
}

func waitForCollectorDeadlineSlots(t *testing.T, ctx context.Context, listener *connectionLimitedListener, want int) {
	t.Helper()
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for len(listener.slots) != want {
		select {
		case <-ctx.Done():
			t.Fatalf("connection slots = %d, want %d: %v", len(listener.slots), want, ctx.Err())
		case <-ticker.C:
		}
	}
}

func newCollectorDeadlineTestService(t *testing.T) (*ingest.Service, string) {
	t.Helper()
	ctx := context.Background()
	database, err := control.Open(ctx, filepath.Join(t.TempDir(), "deadline.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	if _, err := database.CreateIndex(ctx, control.IndexDefinition{
		Name: "main", DisplayName: "Main", RetentionPeriod: time.Hour, IngestionEnabled: true,
	}); err != nil {
		t.Fatal(err)
	}
	tokens, err := auth.NewStore(database, []byte("collector-deadline-digest-key-32b"))
	if err != nil {
		t.Fatal(err)
	}
	issued, err := tokens.CreateCollectorToken(ctx, auth.CreateCollectorTokenRequest{
		Name: "deadline collector", AllowedIndexNames: []string{"main"}, BoundCollectorID: "deadline-collector",
	})
	if err != nil {
		t.Fatal(err)
	}
	fleet, err := collectorfleet.New(database)
	if err != nil {
		t.Fatal(err)
	}
	admissions, err := collectoradmission.New(database, tokens, "deadline-tenant")
	if err != nil {
		t.Fatal(err)
	}
	config := ingest.DefaultConfig()
	config.SessionManager = collectorSessionManager{
		admission: admissions, fleet: fleet,
		heartbeats: newCommandHeartbeatRuntime(t, fleet, config.HeartbeatInterval),
	}
	service, err := ingest.NewService(config, collectorAuthorizer{store: tokens, tenantID: "deadline-tenant"},
		ingest.EventStoreFunc(func(context.Context, ingest.StoreBatch) (ingest.StoreResult, error) {
			return ingest.StoreResult{}, nil
		}),
	)
	if err != nil {
		t.Fatal(err)
	}
	return service, issued.Secret.Plaintext()
}
