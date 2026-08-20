package main

import (
	"bytes"
	"context"
	"errors"
	"net"
	"net/http"
	"path/filepath"
	"strings"
	"testing"
	"time"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/auth"
	"github.com/Suhaibinator/open-splunk/internal/collectoradmission"
	"github.com/Suhaibinator/open-splunk/internal/collectorfleet"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/ingest"
	"github.com/Suhaibinator/open-splunk/internal/server"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
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
	firstTokens, err := openSecurityStores(ctx, firstDB, keyPath)
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
	collectorID := "collector-runtime-test"

	createResponse := postRuntimeAppProto(
		t,
		firstHandler,
		"/api/ingestion-tokens/create",
		&opensplunk.CreateIngestionTokenRequest{
			Definition: &opensplunk.IngestionTokenDefinition{
				Name: "native collector",
				Constraints: &opensplunk.IngestionTokenConstraints{
					AllowedIndexNames: []string{"main"},
					BoundCollectorId:  &collectorID,
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
	var created opensplunk.CreateIngestionTokenResponse
	unmarshalRuntimeAppResponse(t, createResponse, &created)
	if created.GetPlaintextToken() == "" ||
		created.GetIngestionToken().GetCreatedAt() == nil ||
		created.GetIngestionToken().GetUpdatedAt() == nil ||
		created.GetIngestionToken().GetLastUsedAt() != nil ||
		created.GetIngestionToken().GetConstraints().GetBoundCollectorId() != collectorID {
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

	mismatchedCollectorID := "collector-runtime-attacker"
	rejected, rejectionErr := runtimeCollectorHello(
		t,
		firstDB,
		firstTokens,
		created.GetPlaintextToken(),
		mismatchedCollectorID,
		acceptedAt,
	)
	if rejected != nil || status.Code(rejectionErr) != codes.PermissionDenied ||
		strings.Contains(rejectionErr.Error(), collectorID) ||
		strings.Contains(rejectionErr.Error(), mismatchedCollectorID) {
		t.Fatalf(
			"mismatched collector admission = (%#v, %v), want sanitized PermissionDenied",
			rejected,
			rejectionErr,
		)
	}
	rejectedGet := getRuntimeIngestionToken(
		t,
		firstHandler,
		administratorToken,
		tokenID,
	)
	if rejectedGet.GetLastUsedAt() != nil {
		t.Fatalf("identity-rejected stream recorded token use: %#v", rejectedGet)
	}

	admitRuntimeCollectorStream(
		t,
		firstDB,
		firstTokens,
		created.GetPlaintextToken(),
		collectorID,
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
	secondTokens, err := openSecurityStores(ctx, secondDB, keyPath)
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
	db *control.DB,
	tokens *auth.Store,
	plaintextToken string,
	collectorID string,
	acceptedAt time.Time,
) {
	t.Helper()
	response, err := runtimeCollectorHello(
		t,
		db,
		tokens,
		plaintextToken,
		collectorID,
		acceptedAt,
	)
	if err != nil || response.GetReady() == nil {
		t.Fatalf("collector Ready = %#v, %v", response, err)
	}
}

func runtimeCollectorHello(
	t *testing.T,
	db *control.DB,
	tokens *auth.Store,
	plaintextToken string,
	collectorID string,
	acceptedAt time.Time,
) (*opensplunk.CollectResponse, error) {
	t.Helper()
	config := ingest.DefaultConfig()
	config.Clock = func() time.Time { return acceptedAt }
	config.NewStreamID = func() string { return "stream-runtime-test" }
	config.ServerInstanceID = "server-runtime-test"
	fleet, err := collectorfleet.New(db)
	if err != nil {
		t.Fatal(err)
	}
	admissions, err := collectoradmission.New(db, tokens, "tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	heartbeatRuntime := newCommandHeartbeatRuntime(
		t,
		fleet,
		config.HeartbeatInterval,
	)
	config.SessionManager = collectorSessionManager{
		admission:  admissions,
		fleet:      fleet,
		heartbeats: heartbeatRuntime,
	}
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
	opensplunk.RegisterCollectorIngestServiceServer(grpcServer, service)
	serveDone := make(chan error, 1)
	go func() {
		serveDone <- grpcServer.Serve(listener)
	}()
	shutdownCollector := func() {
		if err := shutdownGRPCServer(grpcServer, time.Second); err != nil {
			t.Error(err)
		}
	}
	connection, err := grpc.NewClient(
		"passthrough:///runtime-token-use",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return listener.Dial()
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		shutdownCollector()
		_ = listener.Close()
		t.Fatal(err)
	}
	streamContext, cancel := context.WithCancel(
		metadata.NewOutgoingContext(
			context.Background(),
			metadata.Pairs("authorization", "Bearer "+plaintextToken),
		),
	)
	stream, err := opensplunk.NewCollectorIngestServiceClient(connection).
		Collect(streamContext)
	if err != nil {
		cancel()
		_ = connection.Close()
		shutdownCollector()
		_ = listener.Close()
		t.Fatal(err)
	}
	if err := stream.Send(&opensplunk.CollectRequest{
		StreamSequence: 1,
		SentAt:         timestamppb.New(acceptedAt),
		Payload: &opensplunk.CollectRequest_Hello{
			Hello: &opensplunk.CollectorHello{
				CollectorId:    collectorID,
				InstanceId:     "instance-runtime-test",
				SourceRevision: "development",
				Hostname:       "runtime-host",
				StartedAt:      timestamppb.New(acceptedAt.Add(-time.Hour)),
			},
		},
	}); err != nil {
		cancel()
		_ = connection.Close()
		shutdownCollector()
		_ = listener.Close()
		t.Fatal(err)
	}
	response, err := stream.Recv()
	cancel()
	if err := connection.Close(); err != nil {
		t.Error(err)
	}
	shutdownCollector()
	if err := listener.Close(); err != nil {
		t.Error(err)
	}
	if err := <-serveDone; err != nil && !errors.Is(err, grpc.ErrServerStopped) {
		t.Fatalf("serve collector: %v", err)
	}
	closeCommandHeartbeatRuntime(t, heartbeatRuntime)
	return response, err
}

func getRuntimeIngestionToken(
	t *testing.T,
	handler http.Handler,
	administratorToken []byte,
	tokenID string,
) *opensplunk.IngestionToken {
	t.Helper()
	response := postRuntimeAppProto(
		t,
		handler,
		"/api/ingestion-tokens/get",
		&opensplunk.GetIngestionTokenRequest{
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
	var decoded opensplunk.GetIngestionTokenResponse
	unmarshalRuntimeAppResponse(t, response, &decoded)
	return proto.Clone(decoded.GetIngestionToken()).(*opensplunk.IngestionToken)
}

func listRuntimeIngestionTokens(
	t *testing.T,
	handler http.Handler,
	administratorToken []byte,
) *opensplunk.ListIngestionTokensResponse {
	t.Helper()
	response := postRuntimeAppProto(
		t,
		handler,
		"/api/ingestion-tokens/list",
		&opensplunk.ListIngestionTokensRequest{
			SortBy:        opensplunk.IngestionTokenSortBy_INGESTION_TOKEN_SORT_BY_LAST_USED_AT,
			SortDirection: opensplunk.SortDirection_SORT_DIRECTION_ASCENDING,
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
	var decoded opensplunk.ListIngestionTokensResponse
	unmarshalRuntimeAppResponse(t, response, &decoded)
	return &decoded
}

func assertRuntimeTokenLastUse(
	t *testing.T,
	token *opensplunk.IngestionToken,
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
