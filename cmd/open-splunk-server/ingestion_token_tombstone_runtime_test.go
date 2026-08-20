package main

import (
	"context"
	"errors"
	"net"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/auth"
	"github.com/Suhaibinator/open-splunk/internal/collectoradmission"
	"github.com/Suhaibinator/open-splunk/internal/collectorfleet"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/ingest"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestRuntimePrunedRevokedCollectorTokenRemainsUnauthorized(t *testing.T) {
	ctx := context.Background()
	database, err := control.Open(
		ctx,
		filepath.Join(t.TempDir(), "pruned-token-runtime.db"),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := database.Close(); err != nil {
			t.Error(err)
		}
	})
	if _, err := database.CreateIndex(ctx, control.IndexDefinition{
		Name:             "main",
		DisplayName:      "Main",
		IngestionEnabled: true,
		SearchEnabled:    true,
	}); err != nil {
		t.Fatal(err)
	}

	const (
		tenantID          = "tenant-pruned-token"
		prunedCollectorID = "collector-pruned-token"
		retainedCollector = "collector-retained-token"
	)
	tokens, err := auth.NewStoreWithOptions(
		database,
		[]byte("pruned-token-runtime-digest-key-32b"),
		auth.StoreOptions{RetainedRevokedTokenLimit: 1},
	)
	if err != nil {
		t.Fatal(err)
	}
	pruned, err := tokens.CreateCollectorToken(
		ctx,
		auth.CreateCollectorTokenRequest{
			Name:              "token pruned after revocation",
			AllowedIndexNames: []string{"main"},
			BoundCollectorID:  prunedCollectorID,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tokens.RevokeCollectorToken(
		ctx,
		pruned.Token.ID,
		pruned.Token.Version,
	); err != nil {
		t.Fatal(err)
	}

	retained, err := tokens.CreateCollectorToken(
		ctx,
		auth.CreateCollectorTokenRequest{
			Name:              "most recently revoked token",
			AllowedIndexNames: []string{"main"},
			BoundCollectorID:  retainedCollector,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tokens.RevokeCollectorToken(
		ctx,
		retained.Token.ID,
		retained.Token.Version,
	); err != nil {
		t.Fatal(err)
	}

	var prunedRows int64
	query := database.GORMDB().
		WithContext(ctx).
		Table("ingestion_tokens").
		Where("ingestion_token_id = ?", pruned.Token.ID).
		Count(&prunedRows)
	if query.Error != nil {
		t.Fatal(query.Error)
	}
	if prunedRows != 0 {
		t.Fatalf(
			"physically pruned ingestion token rows = %d, want 0",
			prunedRows,
		)
	}
	var retainedRows int64
	query = database.GORMDB().
		WithContext(ctx).
		Table("ingestion_tokens").
		Where("ingestion_token_id = ?", retained.Token.ID).
		Count(&retainedRows)
	if query.Error != nil {
		t.Fatal(query.Error)
	}
	if retainedRows != 1 {
		t.Fatalf(
			"most recently revoked ingestion token rows = %d, want 1",
			retainedRows,
		)
	}
	if _, err := tokens.Authenticate(
		ctx,
		pruned.Secret.Plaintext(),
	); !errors.Is(err, auth.ErrUnauthorized) {
		t.Fatalf(
			"Authenticate(physically pruned token) error = %v, want ErrUnauthorized",
			err,
		)
	}

	fleet, err := collectorfleet.New(database)
	if err != nil {
		t.Fatal(err)
	}
	admissions, err := collectoradmission.New(database, tokens, tenantID)
	if err != nil {
		t.Fatal(err)
	}
	acceptedAt := retained.Token.CreatedAt.
		Add(time.Second).
		Truncate(time.Microsecond).
		UTC()
	config := ingest.DefaultConfig()
	config.Clock = func() time.Time { return acceptedAt }
	config.NewStreamID = func() string { return "stream-pruned-token" }
	config.ServerInstanceID = "server-pruned-token"
	config.SessionManager = collectorSessionManager{
		admission: admissions,
		fleet:     fleet,
		heartbeats: newCommandHeartbeatRuntime(
			t,
			fleet,
			config.HeartbeatInterval,
		),
	}
	authorizationStarted := make(chan struct{})
	releaseAuthorization := make(chan struct{})
	var startAuthorizationOnce sync.Once
	var releaseAuthorizationOnce sync.Once
	unblockAuthorization := func() {
		releaseAuthorizationOnce.Do(func() {
			close(releaseAuthorization)
		})
	}
	t.Cleanup(unblockAuthorization)
	runtimeAuthorizer := collectorAuthorizer{
		store:    tokens,
		tenantID: tenantID,
	}
	var storeCalls atomic.Uint32
	service, err := ingest.NewService(
		config,
		ingest.AuthorizerFunc(func(
			ctx context.Context,
			plaintext string,
		) (ingest.Authorization, error) {
			startAuthorizationOnce.Do(func() {
				close(authorizationStarted)
			})
			select {
			case <-releaseAuthorization:
			case <-ctx.Done():
				return ingest.Authorization{}, ctx.Err()
			}
			return runtimeAuthorizer.Authorize(ctx, plaintext)
		}),
		ingest.EventStoreFunc(func(
			_ context.Context,
			batch ingest.StoreBatch,
		) (ingest.StoreResult, error) {
			storeCalls.Add(1)
			acknowledged := batch.BatchSequence
			return ingest.StoreResult{
				Accepted:            uint32(len(batch.Events)),
				AcknowledgedThrough: &acknowledged,
				CommittedAt:         acceptedAt,
				OriginalEventCount:  batch.OriginalEventCount,
			}, nil
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
		"passthrough:///runtime-pruned-token",
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
	streamContext, cancelStream := context.WithTimeout(
		metadata.NewOutgoingContext(
			context.Background(),
			metadata.Pairs(
				"authorization",
				"Bearer "+pruned.Secret.Plaintext(),
			),
		),
		5*time.Second,
	)
	t.Cleanup(cancelStream)
	stream, err := opensplunk.NewCollectorIngestServiceClient(connection).
		Collect(streamContext)
	if err != nil {
		t.Fatal(err)
	}
	message := "physically pruned credentials must not store this event"
	event := &opensplunk.LogEvent{
		EventId:     "event-pruned-token",
		IndexName:   "main",
		EventTime:   timestamppb.New(acceptedAt.Add(-time.Second)),
		CollectedAt: timestamppb.New(acceptedAt),
		EventTimeSource: opensplunk.
			EventTimeSource_EVENT_TIME_SOURCE_PARSED,
		Host:        "runtime-pruned-token-host",
		Source:      "/var/log/runtime-pruned-token.log",
		Sourcetype:  "runtime:pruned-token",
		Severity:    opensplunk.LogSeverity_LOG_SEVERITY_INFO,
		Message:     &message,
		Raw:         []byte(message),
		RawEncoding: opensplunk.RawEncoding_RAW_ENCODING_UTF8,
	}
	batch := &opensplunk.EventBatch{
		CollectorId:   prunedCollectorID,
		BatchId:       "batch-pruned-token",
		BatchSequence: 1,
		CreatedAt:     timestamppb.New(acceptedAt),
		Events:        []*opensplunk.LogEvent{event},
		UncompressedSizeBytes: ingest.UncompressedEventBytes(
			[]*opensplunk.LogEvent{event},
		),
		EventIdsSha256: ingest.EventIDDigest(
			[]*opensplunk.LogEvent{event},
		),
	}
	if err := stream.Send(&opensplunk.CollectRequest{
		StreamSequence: 1,
		SentAt:         timestamppb.New(acceptedAt),
		Payload: &opensplunk.CollectRequest_Hello{
			Hello: &opensplunk.CollectorHello{
				CollectorId:     prunedCollectorID,
				InstanceId:      "instance-pruned-token",
				SourceRevision:  "development",
				Hostname:        "runtime-pruned-token-host",
				OperatingSystem: "linux",
				Architecture:    "amd64",
				StartedAt:       timestamppb.New(acceptedAt.Add(-time.Minute)),
				Inputs: []*opensplunk.CollectorInputRegistration{{
					InputId: "input-pruned-token",
					InputType: opensplunk.
						CollectorInputType_COLLECTOR_INPUT_TYPE_FILE,
					IndexName: "main",
				}},
			},
		},
	}); err != nil {
		t.Fatal(err)
	}
	if err := stream.Send(&opensplunk.CollectRequest{
		StreamSequence: 2,
		SentAt:         timestamppb.New(acceptedAt),
		Payload: &opensplunk.CollectRequest_Batch{
			Batch: batch,
		},
	}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-authorizationStarted:
	case <-streamContext.Done():
		t.Fatalf(
			"collector authorization did not start after valid requests were queued: %v",
			streamContext.Err(),
		)
	}
	unblockAuthorization()
	response, err := stream.Recv()
	if response != nil || status.Code(err) != codes.Unauthenticated {
		t.Fatalf(
			"physically pruned token admission = (%#v, %v), want nil/Unauthenticated",
			response,
			err,
		)
	}
	for _, sensitive := range []string{
		pruned.Token.ID,
		pruned.Secret.Plaintext(),
	} {
		if strings.Contains(err.Error(), sensitive) {
			t.Fatalf(
				"physically pruned token admission disclosed credential material: %v",
				err,
			)
		}
	}
	if calls := storeCalls.Load(); calls != 0 {
		t.Fatalf(
			"event store calls for physically pruned credential = %d, want 0",
			calls,
		)
	}
	if _, err := fleet.Get(
		ctx,
		collectorfleet.Scope{TenantID: tenantID},
		prunedCollectorID,
	); !errors.Is(err, control.ErrNotFound) {
		t.Fatalf(
			"Get(collector rejected with physically pruned token) error = %v, want ErrNotFound",
			err,
		)
	}
}
