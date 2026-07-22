package ingest

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestCollectAuthenticatesBearerTokenAndNegotiatesReady(t *testing.T) {
	var gotToken string
	authorizer := AuthorizerFunc(func(_ context.Context, token string) (Authorization, error) {
		gotToken = token
		return Authorization{
			SubjectID:         "token-1",
			TenantID:          "tenant-a",
			AuthorizedIndexes: []string{"z-last", "main"},
		}, nil
	})
	harness := newServiceHarness(t, testServiceConfig(), authorizer, acceptingStore())
	stream := harness.stream(t, "Bearer token-value")

	sendHello(t, stream, 1, 1, 0)
	response := recvResponse(t, stream)
	ready := response.GetReady()
	if ready == nil {
		t.Fatalf("response = %T, want CollectorReady", response.GetPayload())
	}
	if gotToken != "token-value" {
		t.Fatalf("authorizer token = %q", gotToken)
	}
	if response.GetStreamSequence() != 1 || ready.GetStreamId() != "stream-test" {
		t.Fatalf("ready response = %#v", response)
	}
	if got, want := ready.GetAuthorizedIndexes(), []string{"main", "z-last"}; !equalStrings(got, want) {
		t.Fatalf("authorized indexes = %v, want %v", got, want)
	}
	if ready.GetAcknowledgmentDurability() != opensplunkv1.AckDurability_ACK_DURABILITY_CLICKHOUSE_COMMITTED {
		t.Fatalf("ack durability = %v", ready.GetAcknowledgmentDurability())
	}
}

func TestCollectRejectsMissingOrInvalidAuthentication(t *testing.T) {
	authorizer := AuthorizerFunc(func(context.Context, string) (Authorization, error) {
		return Authorization{}, errors.New("bad token")
	})
	harness := newServiceHarness(t, testServiceConfig(), authorizer, acceptingStore())

	for _, authHeader := range []string{"", "Basic abc", "Bearer rejected"} {
		t.Run(authHeader, func(t *testing.T) {
			stream := harness.stream(t, authHeader)
			_, err := stream.Recv()
			if status.Code(err) != codes.Unauthenticated {
				t.Fatalf("Recv() error = %v, want Unauthenticated", err)
			}
		})
	}
}

func TestCollectEnforcesHelloFirstProtocolAndStreamSequence(t *testing.T) {
	authorizer := staticTestAuthorizer()
	harness := newServiceHarness(t, testServiceConfig(), authorizer, acceptingStore())

	tests := []struct {
		name string
		send func(opensplunkv1.CollectorIngestService_CollectClient) error
		code codes.Code
	}{
		{
			name: "batch before hello",
			send: func(stream opensplunkv1.CollectorIngestService_CollectClient) error {
				batch := validTestBatch("collector-a", "batch-a", 1, validTestEvent("event-a", "main"))
				return stream.Send(batchRequest(1, batch))
			},
			code: codes.InvalidArgument,
		},
		{
			name: "initial sequence is not one",
			send: func(stream opensplunkv1.CollectorIngestService_CollectClient) error {
				return stream.Send(helloRequest(2, 1, 0))
			},
			code: codes.InvalidArgument,
		},
		{
			name: "unsupported major",
			send: func(stream opensplunkv1.CollectorIngestService_CollectClient) error {
				return stream.Send(helloRequest(1, 2, 0))
			},
			code: codes.FailedPrecondition,
		},
		{
			name: "unsupported future minor",
			send: func(stream opensplunkv1.CollectorIngestService_CollectClient) error {
				return stream.Send(helloRequest(1, 1, 1))
			},
			code: codes.FailedPrecondition,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stream := harness.stream(t, "Bearer good-token")
			if err := tt.send(stream); err != nil {
				t.Fatal(err)
			}
			_, err := stream.Recv()
			if status.Code(err) != tt.code {
				t.Fatalf("Recv() error = %v, want %v", err, tt.code)
			}
		})
	}

	t.Run("sequence gap after hello", func(t *testing.T) {
		stream := harness.stream(t, "Bearer good-token")
		sendHello(t, stream, 1, 1, 0)
		_ = recvResponse(t, stream)
		heartbeat := &opensplunkv1.CollectorHeartbeat{
			CollectorId: "collector-a",
			InstanceId:  "instance-a",
			ObservedAt:  timestamppb.New(validationTestNow),
		}
		if err := stream.Send(&opensplunkv1.CollectRequest{
			StreamSequence: 3,
			SentAt:         timestamppb.New(validationTestNow),
			Payload:        &opensplunkv1.CollectRequest_Heartbeat{Heartbeat: heartbeat},
		}); err != nil {
			t.Fatal(err)
		}
		_, err := stream.Recv()
		if status.Code(err) != codes.InvalidArgument {
			t.Fatalf("Recv() error = %v, want InvalidArgument", err)
		}
	})
}

func TestCollectEnforcesTokenAndPayloadCollectorIdentity(t *testing.T) {
	authorizer := AuthorizerFunc(func(context.Context, string) (Authorization, error) {
		return Authorization{
			TenantID:          "tenant-a",
			CollectorID:       "bound-collector",
			AuthorizedIndexes: []string{"main"},
		}, nil
	})
	harness := newServiceHarness(t, testServiceConfig(), authorizer, acceptingStore())

	t.Run("hello must match token binding", func(t *testing.T) {
		stream := harness.stream(t, "Bearer good-token")
		if err := stream.Send(helloRequestFor(1, "different-collector", "instance-a", 1, 0)); err != nil {
			t.Fatal(err)
		}
		_, err := stream.Recv()
		if status.Code(err) != codes.PermissionDenied {
			t.Fatalf("Recv() error = %v, want PermissionDenied", err)
		}
	})

	t.Run("batch collector must match hello", func(t *testing.T) {
		stream := harness.stream(t, "Bearer good-token")
		if err := stream.Send(helloRequestFor(1, "bound-collector", "instance-a", 1, 0)); err != nil {
			t.Fatal(err)
		}
		_ = recvResponse(t, stream)
		batch := validTestBatch("other-collector", "batch-a", 1, validTestEvent("event-a", "main"))
		if err := stream.Send(batchRequest(2, batch)); err != nil {
			t.Fatal(err)
		}
		response := recvResponse(t, stream)
		if response.GetBatchReject().GetCode() != opensplunkv1.BatchRejectionCode_BATCH_REJECTION_CODE_COLLECTOR_ID_MISMATCH {
			t.Fatalf("batch rejection = %#v", response.GetBatchReject())
		}
	})
}

func TestCollectPartiallyRejectsEventsAndStoresOnlyNormalizedAuthorizedEvents(t *testing.T) {
	var stored StoreBatch
	store := EventStoreFunc(func(_ context.Context, batch StoreBatch) (StoreResult, error) {
		stored = batch
		return StoreResult{Accepted: uint32(len(batch.Events)), CommittedAt: validationTestNow.Add(time.Second)}, nil
	})
	harness := newServiceHarness(t, testServiceConfig(), staticTestAuthorizer(), store)
	stream := harness.stream(t, "Bearer good-token")
	sendHello(t, stream, 1, 1, 0)
	_ = recvResponse(t, stream)

	accepted := validTestEvent("event-accepted", "main")
	accepted.Fields = object(stringField("password", "must-not-reach-store"))
	unauthorized := validTestEvent("event-unauthorized", "forbidden")
	invalid := validTestEvent("event-invalid", "main")
	invalid.Fields = object(stringField("_raw", "forged"))
	batch := validTestBatch("collector-a", "batch-partial", 1, accepted, unauthorized, invalid)
	if err := stream.Send(batchRequest(2, batch)); err != nil {
		t.Fatal(err)
	}
	response := recvResponse(t, stream)
	ack := response.GetBatchAck()
	if ack == nil {
		t.Fatalf("response = %T, want BatchAck", response.GetPayload())
	}
	if ack.GetAcceptedEventCount() != 1 || ack.GetDuplicateEventCount() != 0 || len(ack.GetRejectedEvents()) != 2 {
		t.Fatalf("ack = %#v", ack)
	}
	if len(stored.Events) != 1 || stored.Events[0].Event.GetEventId() != "event-accepted" {
		t.Fatalf("stored events = %#v", stored.Events)
	}
	if stored.Events[0].Event.Fields.Fields[0].Value.GetStringValue() != DefaultRedactionReplacement {
		t.Fatalf("stored secret = %q", stored.Events[0].Event.Fields.Fields[0].Value.GetStringValue())
	}
	if got := ack.GetRejectedEvents()[0]; got.GetCode() != opensplunkv1.EventRejectionCode_EVENT_REJECTION_CODE_UNAUTHORIZED_INDEX || got.GetEventIndex() != 1 {
		t.Fatalf("first rejection = %#v", got)
	}
	if got := ack.GetRejectedEvents()[1]; got.GetCode() != opensplunkv1.EventRejectionCode_EVENT_REJECTION_CODE_FIELD_NAME_INVALID || got.GetEventIndex() != 2 {
		t.Fatalf("second rejection = %#v", got)
	}
}

func TestCollectRetriesTransientStoreFailureThenAcknowledgesDuplicateOutcome(t *testing.T) {
	var mu sync.Mutex
	calls := 0
	store := EventStoreFunc(func(_ context.Context, batch StoreBatch) (StoreResult, error) {
		mu.Lock()
		defer mu.Unlock()
		calls++
		if calls == 1 {
			return StoreResult{}, &TransientStoreError{
				Err:        errors.New("clickhouse unavailable"),
				Reason:     opensplunkv1.RetryBatchReason_RETRY_BATCH_REASON_STORAGE_UNAVAILABLE,
				RetryAfter: 250 * time.Millisecond,
			}
		}
		return StoreResult{Duplicate: uint32(len(batch.Events)), CommittedAt: validationTestNow.Add(time.Second)}, nil
	})
	harness := newServiceHarness(t, testServiceConfig(), staticTestAuthorizer(), store)
	stream := harness.stream(t, "Bearer good-token")
	sendHello(t, stream, 1, 1, 0)
	_ = recvResponse(t, stream)

	batch := validTestBatch("collector-a", "batch-retry", 1, validTestEvent("event-a", "main"))
	if err := stream.Send(batchRequest(2, batch)); err != nil {
		t.Fatal(err)
	}
	response := recvResponse(t, stream)
	retry := response.GetRetryBatch()
	if retry == nil || retry.GetReason() != opensplunkv1.RetryBatchReason_RETRY_BATCH_REASON_STORAGE_UNAVAILABLE {
		t.Fatalf("retry response = %#v", response)
	}
	if retry.GetRetryAfter().AsDuration() != 250*time.Millisecond {
		t.Fatalf("retry after = %v", retry.GetRetryAfter())
	}

	// A retry has a new connection-local stream sequence but preserves the
	// durable batch identity and sequence exactly.
	if err := stream.Send(batchRequest(3, batch)); err != nil {
		t.Fatal(err)
	}
	response = recvResponse(t, stream)
	ack := response.GetBatchAck()
	if ack == nil || ack.GetAcceptedEventCount() != 0 || ack.GetDuplicateEventCount() != 1 {
		t.Fatalf("ack = %#v", ack)
	}
	if calls != 2 {
		t.Fatalf("store calls = %d, want 2", calls)
	}
}

func TestCollectDoesNotAcknowledgePermanentStoreFailure(t *testing.T) {
	store := EventStoreFunc(func(context.Context, StoreBatch) (StoreResult, error) {
		return StoreResult{}, errors.New("corrupt storage contract")
	})
	harness := newServiceHarness(t, testServiceConfig(), staticTestAuthorizer(), store)
	stream := harness.stream(t, "Bearer good-token")
	sendHello(t, stream, 1, 1, 0)
	_ = recvResponse(t, stream)
	batch := validTestBatch("collector-a", "batch-failure", 1, validTestEvent("event-a", "main"))
	if err := stream.Send(batchRequest(2, batch)); err != nil {
		t.Fatal(err)
	}
	response, err := stream.Recv()
	if response != nil || status.Code(err) != codes.Internal {
		t.Fatalf("Recv() = (%#v, %v), want nil/Internal", response, err)
	}
}

func TestCollectRejectsInvalidBatchEnvelopesBeforeStorage(t *testing.T) {
	var storeCalls int
	store := EventStoreFunc(func(_ context.Context, batch StoreBatch) (StoreResult, error) {
		storeCalls++
		return StoreResult{Accepted: uint32(len(batch.Events))}, nil
	})
	harness := newServiceHarness(t, testServiceConfig(), staticTestAuthorizer(), store)

	tests := []struct {
		name string
		edit func(*opensplunkv1.EventBatch)
		code opensplunkv1.BatchRejectionCode
	}{
		{
			name: "invalid batch ID",
			edit: func(batch *opensplunkv1.EventBatch) { batch.BatchId = "bad id" },
			code: opensplunkv1.BatchRejectionCode_BATCH_REJECTION_CODE_INVALID_BATCH_ID,
		},
		{
			name: "digest mismatch",
			edit: func(batch *opensplunkv1.EventBatch) { batch.EventIdsSha256[0] ^= 0xff },
			code: opensplunkv1.BatchRejectionCode_BATCH_REJECTION_CODE_EVENT_ID_DIGEST_MISMATCH,
		},
		{
			name: "declared size mismatch",
			edit: func(batch *opensplunkv1.EventBatch) { batch.UncompressedSizeBytes++ },
			code: opensplunkv1.BatchRejectionCode_BATCH_REJECTION_CODE_BATCH_TOO_LARGE,
		},
		{
			name: "protocol mismatch",
			edit: func(batch *opensplunkv1.EventBatch) { batch.ProtocolMinor++ },
			code: opensplunkv1.BatchRejectionCode_BATCH_REJECTION_CODE_PROTOCOL_VIOLATION,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stream := harness.stream(t, "Bearer good-token")
			sendHello(t, stream, 1, 1, 0)
			_ = recvResponse(t, stream)
			batch := validTestBatch("collector-a", "batch-a", 1, validTestEvent("event-a", "main"))
			tt.edit(batch)
			if err := stream.Send(batchRequest(2, batch)); err != nil {
				t.Fatal(err)
			}
			response := recvResponse(t, stream)
			if got := response.GetBatchReject().GetCode(); got != tt.code {
				t.Fatalf("batch rejection code = %v, want %v", got, tt.code)
			}
		})
	}
	if storeCalls != 0 {
		t.Fatalf("store calls = %d, want 0", storeCalls)
	}
}

func TestCollectRejectsBatchSequenceConflict(t *testing.T) {
	harness := newServiceHarness(t, testServiceConfig(), staticTestAuthorizer(), acceptingStore())
	stream := harness.stream(t, "Bearer good-token")
	sendHello(t, stream, 1, 1, 0)
	_ = recvResponse(t, stream)

	first := validTestBatch("collector-a", "batch-one", 1, validTestEvent("event-one", "main"))
	if err := stream.Send(batchRequest(2, first)); err != nil {
		t.Fatal(err)
	}
	_ = recvResponse(t, stream)
	conflict := validTestBatch("collector-a", "batch-two", 1, validTestEvent("event-two", "main"))
	if err := stream.Send(batchRequest(3, conflict)); err != nil {
		t.Fatal(err)
	}
	response := recvResponse(t, stream)
	if response.GetBatchReject().GetCode() != opensplunkv1.BatchRejectionCode_BATCH_REJECTION_CODE_SEQUENCE_CONFLICT {
		t.Fatalf("response = %#v", response)
	}
}

func TestCollectClosesCleanlyAfterGoodbye(t *testing.T) {
	harness := newServiceHarness(t, testServiceConfig(), staticTestAuthorizer(), acceptingStore())
	stream := harness.stream(t, "Bearer good-token")
	sendHello(t, stream, 1, 1, 0)
	_ = recvResponse(t, stream)
	if err := stream.Send(&opensplunkv1.CollectRequest{
		StreamSequence: 2,
		SentAt:         timestamppb.New(validationTestNow),
		Payload: &opensplunkv1.CollectRequest_Goodbye{Goodbye: &opensplunkv1.CollectorGoodbye{
			Reason: opensplunkv1.CollectorGoodbyeReason_COLLECTOR_GOODBYE_REASON_SHUTDOWN,
		}},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := stream.Recv(); !errors.Is(err, io.EOF) {
		t.Fatalf("Recv() error = %v, want EOF", err)
	}
}

type serviceHarness struct {
	client opensplunkv1.CollectorIngestServiceClient
}

func newServiceHarness(t *testing.T, cfg Config, authorizer Authorizer, store EventStore) *serviceHarness {
	t.Helper()
	service, err := NewService(cfg, authorizer, store)
	if err != nil {
		t.Fatal(err)
	}
	listener := bufconn.Listen(1 << 20)
	server := grpc.NewServer()
	opensplunkv1.RegisterCollectorIngestServiceServer(server, service)
	go func() {
		_ = server.Serve(listener)
	}()
	t.Cleanup(server.Stop)

	dialer := func(context.Context, string) (net.Conn, error) { return listener.Dial() }
	conn, err := grpc.NewClient(
		"passthrough:///bufnet",
		grpc.WithContextDialer(dialer),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return &serviceHarness{client: opensplunkv1.NewCollectorIngestServiceClient(conn)}
}

func (h *serviceHarness) stream(t *testing.T, authorization string) opensplunkv1.CollectorIngestService_CollectClient {
	t.Helper()
	ctx := context.Background()
	if authorization != "" {
		ctx = metadata.NewOutgoingContext(ctx, metadata.Pairs("authorization", authorization))
	}
	stream, err := h.client.Collect(ctx)
	if err != nil {
		t.Fatal(err)
	}
	return stream
}

func testServiceConfig() Config {
	cfg := DefaultConfig()
	cfg.Clock = func() time.Time { return validationTestNow }
	cfg.NewStreamID = func() string { return "stream-test" }
	cfg.ServerInstanceID = "server-test"
	cfg.ServerVersion = "test-version"
	cfg.ProtocolMajor = 1
	cfg.ProtocolMinor = 0
	return cfg
}

func staticTestAuthorizer() Authorizer {
	return AuthorizerFunc(func(_ context.Context, token string) (Authorization, error) {
		if token != "good-token" {
			return Authorization{}, errors.New("invalid token")
		}
		return Authorization{
			SubjectID:         "token-1",
			TenantID:          "tenant-a",
			AuthorizedIndexes: []string{"main"},
		}, nil
	})
}

func acceptingStore() EventStore {
	return EventStoreFunc(func(_ context.Context, batch StoreBatch) (StoreResult, error) {
		return StoreResult{Accepted: uint32(len(batch.Events)), CommittedAt: validationTestNow}, nil
	})
}

func sendHello(t *testing.T, stream opensplunkv1.CollectorIngestService_CollectClient, sequence uint64, major, minor uint32) {
	t.Helper()
	if err := stream.Send(helloRequest(sequence, major, minor)); err != nil {
		t.Fatal(err)
	}
}

func helloRequest(sequence uint64, major, minor uint32) *opensplunkv1.CollectRequest {
	return helloRequestFor(sequence, "collector-a", "instance-a", major, minor)
}

func helloRequestFor(sequence uint64, collectorID, instanceID string, major, minor uint32) *opensplunkv1.CollectRequest {
	return &opensplunkv1.CollectRequest{
		StreamSequence: sequence,
		SentAt:         timestamppb.New(validationTestNow),
		Payload: &opensplunkv1.CollectRequest_Hello{Hello: &opensplunkv1.CollectorHello{
			CollectorId:      collectorID,
			InstanceId:       instanceID,
			ProtocolMajor:    major,
			ProtocolMinor:    minor,
			CollectorVersion: "test-collector",
			Hostname:         "host-a",
			StartedAt:        timestamppb.New(validationTestNow.Add(-time.Hour)),
		}},
	}
}

func batchRequest(streamSequence uint64, batch *opensplunkv1.EventBatch) *opensplunkv1.CollectRequest {
	return &opensplunkv1.CollectRequest{
		StreamSequence: streamSequence,
		SentAt:         timestamppb.New(validationTestNow),
		Payload:        &opensplunkv1.CollectRequest_Batch{Batch: batch},
	}
}

func validTestBatch(collectorID, batchID string, batchSequence uint64, events ...*opensplunkv1.LogEvent) *opensplunkv1.EventBatch {
	return &opensplunkv1.EventBatch{
		CollectorId:           collectorID,
		BatchId:               batchID,
		BatchSequence:         batchSequence,
		CreatedAt:             timestamppb.New(validationTestNow),
		Events:                events,
		UncompressedSizeBytes: UncompressedEventBytes(events),
		EventIdsSha256:        EventIDDigest(events),
		ProtocolMajor:         1,
		ProtocolMinor:         0,
	}
}

func recvResponse(t *testing.T, stream opensplunkv1.CollectorIngestService_CollectClient) *opensplunkv1.CollectResponse {
	t.Helper()
	response, err := stream.Recv()
	if err != nil {
		t.Fatal(err)
	}
	return response
}

func equalStrings(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}
