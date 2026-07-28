package main

import (
	"bytes"
	"context"
	"errors"
	"net"
	"net/http"
	"path/filepath"
	"testing"
	"time"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/auth"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/ingest"
	"github.com/Suhaibinator/open-splunk/internal/server"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestRuntimeIngestionTokenLastUseSurvivesHTTPGRPCReopen(t *testing.T) {
	ctx := context.Background()
	directory := t.TempDir()
	dbPath := filepath.Join(directory, "control.db")
	keyPath := filepath.Join(directory, "server.key")
	administratorToken := bytes.Repeat(
		[]byte("a"),
		auth.MinimumBrowserBearerTokenBytes,
	)
	authenticator, err := auth.NewBearerTokenAuthenticator(
		administratorToken,
		"tenant-a",
		"administrator",
		auth.BrowserRoleAdministrator,
	)
	if err != nil {
		t.Fatal(err)
	}

	firstDB, err := control.Open(ctx, dbPath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := firstDB.CreateIndex(ctx, control.IndexDefinition{
		Name:             "main",
		DisplayName:      "Main",
		IngestionEnabled: true,
		SearchEnabled:    true,
	}); err != nil {
		_ = firstDB.Close()
		t.Fatal(err)
	}
	_, firstTokens, err := openSecurityStores(ctx, firstDB, keyPath)
	if err != nil {
		_ = firstDB.Close()
		t.Fatal(err)
	}
	firstHandler := newRuntimeTokenHTTPHandlerForTest(
		t,
		firstDB,
		firstTokens,
		authenticator,
	)

	createResponse := postRuntimeAppProto(
		t,
		firstHandler,
		"/api/v1/ingestion-tokens/create",
		&opensplunkv1.CreateIngestionTokenRequest{
			Definition: &opensplunkv1.IngestionTokenDefinition{
				Name: "native collector",
				Constraints: &opensplunkv1.IngestionTokenConstraints{
					AllowedIndexNames: []string{"main"},
				},
			},
		},
		administratorToken,
	)
	if createResponse.Code != http.StatusOK {
		_ = firstHandler.Close(ctx)
		_ = firstDB.Close()
		t.Fatalf(
			"create token status = %d, body = %s",
			createResponse.Code,
			createResponse.Body.String(),
		)
	}
	var created opensplunkv1.CreateIngestionTokenResponse
	unmarshalRuntimeAppResponse(t, createResponse, &created)
	if created.GetPlaintextToken() == "" ||
		created.GetIngestionToken().GetCreatedAt() == nil ||
		created.GetIngestionToken().GetUpdatedAt() == nil ||
		created.GetIngestionToken().GetLastUsedAt() != nil {
		_ = firstHandler.Close(ctx)
		_ = firstDB.Close()
		t.Fatalf("created token = %#v", &created)
	}
	tokenID := created.GetIngestionToken().GetIngestionTokenId()
	initialVersion := created.GetIngestionToken().GetVersion()
	initialUpdatedAt := created.GetIngestionToken().GetUpdatedAt().AsTime()
	acceptedAt := created.GetIngestionToken().
		GetCreatedAt().
		AsTime().
		Add(time.Minute).
		Truncate(time.Microsecond).
		UTC()

	admitRuntimeCollectorStream(
		t,
		firstTokens,
		created.GetPlaintextToken(),
		acceptedAt,
	)

	firstGet := getRuntimeIngestionToken(
		t,
		firstHandler,
		administratorToken,
		tokenID,
	)
	assertRuntimeTokenLastUse(
		t,
		firstGet,
		acceptedAt,
		initialVersion,
		initialUpdatedAt,
	)
	firstList := listRuntimeIngestionTokens(
		t,
		firstHandler,
		administratorToken,
	)
	if len(firstList.GetIngestionTokens()) != 1 {
		t.Fatalf("first token list = %#v", &firstList)
	}
	assertRuntimeTokenLastUse(
		t,
		firstList.GetIngestionTokens()[0],
		acceptedAt,
		initialVersion,
		initialUpdatedAt,
	)
	if err := firstHandler.Close(ctx); err != nil {
		_ = firstDB.Close()
		t.Fatal(err)
	}
	if err := firstDB.Close(); err != nil {
		t.Fatal(err)
	}

	secondDB, err := control.Open(ctx, dbPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := secondDB.Close(); err != nil {
			t.Error(err)
		}
	})
	_, secondTokens, err := openSecurityStores(ctx, secondDB, keyPath)
	if err != nil {
		t.Fatal(err)
	}
	secondHandler := newRuntimeTokenHTTPHandlerForTest(
		t,
		secondDB,
		secondTokens,
		authenticator,
	)
	t.Cleanup(func() {
		if err := secondHandler.Close(context.Background()); err != nil {
			t.Error(err)
		}
	})

	secondGet := getRuntimeIngestionToken(
		t,
		secondHandler,
		administratorToken,
		tokenID,
	)
	assertRuntimeTokenLastUse(
		t,
		secondGet,
		acceptedAt,
		initialVersion,
		initialUpdatedAt,
	)
	secondList := listRuntimeIngestionTokens(
		t,
		secondHandler,
		administratorToken,
	)
	if len(secondList.GetIngestionTokens()) != 1 {
		t.Fatalf("reopened token list = %#v", &secondList)
	}
	assertRuntimeTokenLastUse(
		t,
		secondList.GetIngestionTokens()[0],
		acceptedAt,
		initialVersion,
		initialUpdatedAt,
	)
}

func newRuntimeTokenHTTPHandlerForTest(
	t *testing.T,
	db *control.DB,
	tokens *auth.Store,
	authenticator auth.BrowserAuthenticator,
) *server.Handler {
	t.Helper()
	config := runtimeServerConfig()
	config.Indexes = db
	config.IngestionTokens = tokens
	config.BrowserAuthenticator = authenticator
	config.AdministrativeAllowedHosts = []string{"127.0.0.1"}
	config.TenantID = "tenant-a"
	config.OwnerID = "administrator"
	handler, err := server.NewHandler(config)
	if err != nil {
		t.Fatalf("NewHandler: %v", err)
	}
	return handler
}

func admitRuntimeCollectorStream(
	t *testing.T,
	tokens *auth.Store,
	plaintextToken string,
	acceptedAt time.Time,
) {
	t.Helper()
	config := ingest.DefaultConfig()
	config.Clock = func() time.Time { return acceptedAt }
	config.NewStreamID = func() string { return "stream-runtime-test" }
	config.ServerInstanceID = "server-runtime-test"
	config.ServerVersion = "runtime-test"
	config.TokenUseRecorder = collectorTokenUseRecorder{store: tokens}
	service, err := ingest.NewService(
		config,
		collectorAuthorizer{store: tokens, tenantID: "tenant-a"},
		ingest.EventStoreFunc(func(
			context.Context,
			ingest.StoreBatch,
		) (ingest.StoreResult, error) {
			return ingest.StoreResult{}, nil
		}),
	)
	if err != nil {
		t.Fatal(err)
	}

	listener := bufconn.Listen(1 << 20)
	grpcServer := grpc.NewServer()
	opensplunkv1.RegisterCollectorIngestServiceServer(grpcServer, service)
	serveDone := make(chan error, 1)
	go func() {
		serveDone <- grpcServer.Serve(listener)
	}()
	connection, err := grpc.NewClient(
		"passthrough:///runtime-token-use",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return listener.Dial()
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		grpcServer.Stop()
		_ = listener.Close()
		t.Fatal(err)
	}
	streamContext, cancel := context.WithCancel(
		metadata.NewOutgoingContext(
			context.Background(),
			metadata.Pairs("authorization", "Bearer "+plaintextToken),
		),
	)
	stream, err := opensplunkv1.NewCollectorIngestServiceClient(connection).
		Collect(streamContext)
	if err != nil {
		cancel()
		_ = connection.Close()
		grpcServer.Stop()
		_ = listener.Close()
		t.Fatal(err)
	}
	if err := stream.Send(&opensplunkv1.CollectRequest{
		StreamSequence: 1,
		SentAt:         timestamppb.New(acceptedAt),
		Payload: &opensplunkv1.CollectRequest_Hello{
			Hello: &opensplunkv1.CollectorHello{
				CollectorId:      "collector-runtime-test",
				InstanceId:       "instance-runtime-test",
				ProtocolMajor:    1,
				ProtocolMinor:    0,
				CollectorVersion: "runtime-test",
				Hostname:         "runtime-host",
				StartedAt:        timestamppb.New(acceptedAt.Add(-time.Hour)),
			},
		},
	}); err != nil {
		cancel()
		_ = connection.Close()
		grpcServer.Stop()
		_ = listener.Close()
		t.Fatal(err)
	}
	response, err := stream.Recv()
	if err != nil || response.GetReady() == nil {
		cancel()
		_ = connection.Close()
		grpcServer.Stop()
		_ = listener.Close()
		t.Fatalf("collector Ready = %#v, %v", response, err)
	}
	cancel()
	if err := connection.Close(); err != nil {
		t.Error(err)
	}
	grpcServer.Stop()
	if err := listener.Close(); err != nil {
		t.Error(err)
	}
	if err := <-serveDone; err != nil && !errors.Is(err, grpc.ErrServerStopped) {
		t.Fatalf("serve collector: %v", err)
	}
}

func getRuntimeIngestionToken(
	t *testing.T,
	handler http.Handler,
	administratorToken []byte,
	tokenID string,
) *opensplunkv1.IngestionToken {
	t.Helper()
	response := postRuntimeAppProto(
		t,
		handler,
		"/api/v1/ingestion-tokens/get",
		&opensplunkv1.GetIngestionTokenRequest{
			IngestionTokenId: tokenID,
		},
		administratorToken,
	)
	if response.Code != http.StatusOK {
		t.Fatalf(
			"get token status = %d, body = %s",
			response.Code,
			response.Body.String(),
		)
	}
	var decoded opensplunkv1.GetIngestionTokenResponse
	unmarshalRuntimeAppResponse(t, response, &decoded)
	return proto.Clone(decoded.GetIngestionToken()).(*opensplunkv1.IngestionToken)
}

func listRuntimeIngestionTokens(
	t *testing.T,
	handler http.Handler,
	administratorToken []byte,
) *opensplunkv1.ListIngestionTokensResponse {
	t.Helper()
	response := postRuntimeAppProto(
		t,
		handler,
		"/api/v1/ingestion-tokens/list",
		&opensplunkv1.ListIngestionTokensRequest{
			SortBy:        opensplunkv1.IngestionTokenSortBy_INGESTION_TOKEN_SORT_BY_LAST_USED_AT,
			SortDirection: opensplunkv1.SortDirection_SORT_DIRECTION_ASCENDING,
		},
		administratorToken,
	)
	if response.Code != http.StatusOK {
		t.Fatalf(
			"list tokens status = %d, body = %s",
			response.Code,
			response.Body.String(),
		)
	}
	var decoded opensplunkv1.ListIngestionTokensResponse
	unmarshalRuntimeAppResponse(t, response, &decoded)
	return &decoded
}

func assertRuntimeTokenLastUse(
	t *testing.T,
	token *opensplunkv1.IngestionToken,
	acceptedAt time.Time,
	initialVersion uint64,
	initialUpdatedAt time.Time,
) {
	t.Helper()
	if token == nil ||
		token.GetLastUsedAt() == nil ||
		!token.GetLastUsedAt().AsTime().Equal(acceptedAt) ||
		token.GetVersion() != initialVersion ||
		!token.GetUpdatedAt().AsTime().Equal(initialUpdatedAt) {
		t.Fatalf(
			"token last use = %#v, want at:%v version:%d updated:%v",
			token,
			acceptedAt,
			initialVersion,
			initialUpdatedAt,
		)
	}
}
