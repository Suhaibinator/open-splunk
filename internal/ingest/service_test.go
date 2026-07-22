package ingest

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
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

func TestCollectReauthorizesEveryBatch(t *testing.T) {
	authorizerCalls := 0
	authorizer := AuthorizerFunc(func(context.Context, string) (Authorization, error) {
		authorizerCalls++
		if authorizerCalls > 1 {
			return Authorization{}, errors.New("token revoked")
		}
		return Authorization{
			SubjectID:         "token-1",
			TenantID:          "tenant-a",
			AuthorizedIndexes: []string{"main"},
		}, nil
	})
	storeCalls := 0
	store := EventStoreFunc(func(context.Context, StoreBatch) (StoreResult, error) {
		storeCalls++
		return StoreResult{Accepted: 1}, nil
	})
	harness := newServiceHarness(t, testServiceConfig(), authorizer, store)
	stream := harness.stream(t, "Bearer token-that-will-be-revoked")
	sendHello(t, stream, 1, 1, 0)
	_ = recvResponse(t, stream)
	batch := validTestBatch("collector-a", "batch-after-revocation", 1, validTestEvent("event-a", "main"))
	if err := stream.Send(batchRequest(2, batch)); err != nil {
		t.Fatal(err)
	}
	response, err := stream.Recv()
	if response != nil || status.Code(err) != codes.Unauthenticated {
		t.Fatalf("Recv() = (%#v, %v), want nil/Unauthenticated", response, err)
	}
	if authorizerCalls != 2 || storeCalls != 0 {
		t.Fatalf("authorizer calls = %d, store calls = %d", authorizerCalls, storeCalls)
	}
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
	wireIdentity, err := batchFingerprint(batch)
	if err != nil {
		t.Fatalf("batchFingerprint: %v", err)
	}
	if stored.SourceBatchSHA256 != wireIdentity.contentHash {
		t.Fatal("store batch did not retain the original collector payload hash")
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

func TestCollectLostAckUsesOriginalDispositionAfterAuthorizationExpansion(t *testing.T) {
	var authorizationCalls int
	authorizer := AuthorizerFunc(func(context.Context, string) (Authorization, error) {
		authorizationCalls++
		indexes := []string{"main"}
		if authorizationCalls > 2 {
			indexes = []string{"audit", "main"}
		}
		return Authorization{
			SubjectID: "token-1", TenantID: "tenant-a", AuthorizedIndexes: indexes,
		}, nil
	})
	store := &recoverableTestStore{}
	harness := newServiceHarness(t, testServiceConfig(), authorizer, store)
	mainEvent := validTestEvent("event-main", "main")
	auditEvent := validTestEvent("event-audit", "audit")
	batch := validTestBatch("collector-a", "batch-lost-ack", 1, mainEvent, auditEvent)

	first := harness.stream(t, "Bearer good-token")
	sendHello(t, first, 1, 1, 0)
	_ = recvResponse(t, first)
	if err := first.Send(batchRequest(2, batch)); err != nil {
		t.Fatal(err)
	}
	// Receive and discard the first response to model an ACK lost after the
	// durable server decision but before collector checkpoint advancement.
	_ = recvResponse(t, first)
	_ = first.CloseSend()

	second := harness.stream(t, "Bearer good-token")
	sendHello(t, second, 1, 1, 0)
	_ = recvResponse(t, second)
	if err := second.Send(batchRequest(2, batch)); err != nil {
		t.Fatal(err)
	}
	ack := recvResponse(t, second).GetBatchAck()
	if ack == nil || ack.GetAcceptedEventCount() != 0 || ack.GetDuplicateEventCount() != 1 ||
		len(ack.GetRejectedEvents()) != 1 {
		t.Fatalf("retried ack = %#v", ack)
	}
	if rejection := ack.GetRejectedEvents()[0]; rejection.GetEventIndex() != 1 ||
		rejection.GetEventId() != "event-audit" ||
		rejection.GetCode() != opensplunkv1.EventRejectionCode_EVENT_REJECTION_CODE_UNAUTHORIZED_INDEX {
		t.Fatalf("retried rejection = %#v, want original unauthorized decision", rejection)
	}
	if store.storeCalls != 1 || store.lookupCalls != 2 {
		t.Fatalf("durable store calls: Store=%d Lookup=%d", store.storeCalls, store.lookupCalls)
	}
	if len(store.first.Events) != 1 || store.first.Events[0].Event.GetEventId() != "event-main" {
		t.Fatalf("first stored block = %#v", store.first.Events)
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

func TestCollectRetryMustPreserveExactDurableBatch(t *testing.T) {
	storeCalls := 0
	store := EventStoreFunc(func(context.Context, StoreBatch) (StoreResult, error) {
		storeCalls++
		return StoreResult{}, &TransientStoreError{Err: errors.New("retry")}
	})
	harness := newServiceHarness(t, testServiceConfig(), staticTestAuthorizer(), store)
	stream := harness.stream(t, "Bearer good-token")
	sendHello(t, stream, 1, 1, 0)
	_ = recvResponse(t, stream)

	batch := validTestBatch("collector-a", "batch-retry", 1, validTestEvent("event-a", "main"))
	if err := stream.Send(batchRequest(2, batch)); err != nil {
		t.Fatal(err)
	}
	if retry := recvResponse(t, stream).GetRetryBatch(); retry == nil {
		t.Fatal("first response is not RetryBatch")
	}

	// The event-ID digest and encoded size deliberately remain unchanged. The
	// server must still detect that the durable batch body changed.
	batch.Events[0].Raw = bytes.Replace(batch.Events[0].Raw, []byte("200"), []byte("201"), 1)
	if got, want := UncompressedEventBytes(batch.Events), batch.UncompressedSizeBytes; got != want {
		t.Fatalf("test mutation changed encoded size: got %d, want %d", got, want)
	}
	if err := stream.Send(batchRequest(3, batch)); err != nil {
		t.Fatal(err)
	}
	response := recvResponse(t, stream)
	if response.GetBatchReject().GetCode() != opensplunkv1.BatchRejectionCode_BATCH_REJECTION_CODE_SEQUENCE_CONFLICT {
		t.Fatalf("response = %#v", response)
	}
	if storeCalls != 1 {
		t.Fatalf("store calls = %d, want 1", storeCalls)
	}
}

func TestCollectRetryReusesFirstServerReceiveTime(t *testing.T) {
	var mu sync.Mutex
	clockCalls := 0
	cfg := testServiceConfig()
	cfg.Clock = func() time.Time {
		mu.Lock()
		defer mu.Unlock()
		clockCalls++
		return validationTestNow.Add(time.Duration(clockCalls) * time.Second)
	}
	var received []time.Time
	var indexTimes []time.Time
	store := EventStoreFunc(func(_ context.Context, batch StoreBatch) (StoreResult, error) {
		received = append(received, batch.ReceivedAt)
		indexTimes = append(indexTimes, batch.Events[0].IndexTime)
		if len(received) == 1 {
			return StoreResult{}, &TransientStoreError{Err: errors.New("retry")}
		}
		return StoreResult{Duplicate: 1}, nil
	})
	harness := newServiceHarness(t, cfg, staticTestAuthorizer(), store)
	stream := harness.stream(t, "Bearer good-token")
	sendHello(t, stream, 1, 1, 0)
	_ = recvResponse(t, stream)
	batch := validTestBatch("collector-a", "batch-retry-time", 1, validTestEvent("event-a", "main"))
	if err := stream.Send(batchRequest(2, batch)); err != nil {
		t.Fatal(err)
	}
	_ = recvResponse(t, stream)
	if err := stream.Send(batchRequest(3, batch)); err != nil {
		t.Fatal(err)
	}
	_ = recvResponse(t, stream)

	if len(received) != 2 || !received[0].Equal(received[1]) {
		t.Fatalf("store receive times = %v, want identical", received)
	}
	if len(indexTimes) != 2 || !indexTimes[0].Equal(indexTimes[1]) {
		t.Fatalf("event index times = %v, want identical", indexTimes)
	}
}

func TestProcessBatchCommittedRetryBypassesMutablePolicy(t *testing.T) {
	store := &recoverableTestStore{}
	firstConfig := testServiceConfig()
	first, err := NewService(firstConfig, staticTestAuthorizer(), store)
	if err != nil {
		t.Fatal(err)
	}
	batch := validTestBatch(
		"collector-a",
		"batch-policy-retry",
		1,
		validTestEvent("event-one", "main"),
		validTestEvent("event-two", "main"),
	)
	firstResponse, err := first.processBatch(context.Background(), batch, testBatchStreamState())
	if err != nil {
		t.Fatal(err)
	}
	if ack := firstResponse.GetBatchAck(); ack == nil || ack.GetAcceptedEventCount() != 2 {
		t.Fatalf("first response = %#v", firstResponse)
	}

	// Both the configured event-count policy and the wall-clock timestamp
	// policy now reject this source batch. Its exact durable identity must be
	// looked up before either mutable check is applied.
	retryConfig := testServiceConfig()
	retryConfig.Limits.MaxBatchEvents = 1
	retryConfig.Clock = func() time.Time { return validationTestNow.Add(HardMaxEventAge + time.Hour) }
	retry, err := NewService(retryConfig, staticTestAuthorizer(), store)
	if err != nil {
		t.Fatal(err)
	}
	retryResponse, err := retry.processBatch(context.Background(), batch, testBatchStreamState())
	if err != nil {
		t.Fatal(err)
	}
	ack := retryResponse.GetBatchAck()
	if ack == nil || ack.GetAcceptedEventCount() != 0 || ack.GetDuplicateEventCount() != 2 {
		t.Fatalf("retried response = %#v, want the persisted acknowledgment", retryResponse)
	}
	if store.storeCalls != 1 || store.lookupCalls != 2 {
		t.Fatalf("durable store calls: Store=%d Lookup=%d", store.storeCalls, store.lookupCalls)
	}
}

func TestProcessBatchAllowsEarlierPendingRetryAfterLaterSuccess(t *testing.T) {
	cfg := testServiceConfig()
	cfg.MaxInFlightBatches = 2
	var calls []uint64
	sequenceOneCalls := 0
	store := EventStoreFunc(func(_ context.Context, batch StoreBatch) (StoreResult, error) {
		calls = append(calls, batch.BatchSequence)
		if batch.BatchSequence == 1 {
			sequenceOneCalls++
			if sequenceOneCalls == 1 {
				return StoreResult{}, &TransientStoreError{Err: errors.New("pre-reservation failure")}
			}
			return StoreResult{Duplicate: uint32(len(batch.Events))}, nil
		}
		return StoreResult{Accepted: uint32(len(batch.Events))}, nil
	})
	service, err := NewService(cfg, staticTestAuthorizer(), store)
	if err != nil {
		t.Fatal(err)
	}
	state := testBatchStreamState()
	first := validTestBatch("collector-a", "batch-one", 1, validTestEvent("event-one", "main"))
	second := validTestBatch("collector-a", "batch-two", 2, validTestEvent("event-two", "main"))

	response, err := service.processBatch(context.Background(), first, state)
	if err != nil || response.GetRetryBatch() == nil {
		t.Fatalf("first response = (%#v, %v), want RetryBatch", response, err)
	}
	response, err = service.processBatch(context.Background(), second, state)
	if err != nil || response.GetBatchAck() == nil {
		t.Fatalf("second response = (%#v, %v), want BatchAck", response, err)
	}
	response, err = service.processBatch(context.Background(), first, state)
	if err != nil || response.GetBatchAck() == nil {
		t.Fatalf("first retry response = (%#v, %v), want BatchAck", response, err)
	}
	if got, want := calls, []uint64{1, 2, 1}; !equalUint64s(got, want) {
		t.Fatalf("store sequences = %v, want %v", got, want)
	}
	if len(state.pendingBatches) != 0 {
		t.Fatalf("pending batches = %d, want 0", len(state.pendingBatches))
	}
}

func TestProcessBatchCapacityRetriesWithoutConsumingIdentity(t *testing.T) {
	cfg := testServiceConfig()
	cfg.MaxInFlightBatches = 1
	var calls []uint64
	sequenceOneCalls := 0
	store := EventStoreFunc(func(_ context.Context, batch StoreBatch) (StoreResult, error) {
		calls = append(calls, batch.BatchSequence)
		if batch.BatchSequence == 1 {
			sequenceOneCalls++
			if sequenceOneCalls == 1 {
				return StoreResult{}, &TransientStoreError{Err: errors.New("storage unavailable")}
			}
		}
		return StoreResult{Accepted: uint32(len(batch.Events))}, nil
	})
	service, err := NewService(cfg, staticTestAuthorizer(), store)
	if err != nil {
		t.Fatal(err)
	}
	state := testBatchStreamState()
	first := validTestBatch("collector-a", "batch-one", 1, validTestEvent("event-one", "main"))
	second := validTestBatch("collector-a", "batch-two", 2, validTestEvent("event-two", "main"))

	response, err := service.processBatch(context.Background(), first, state)
	if err != nil || response.GetRetryBatch() == nil {
		t.Fatalf("first response = (%#v, %v), want RetryBatch", response, err)
	}
	response, err = service.processBatch(context.Background(), second, state)
	if err != nil {
		t.Fatal(err)
	}
	retry := response.GetRetryBatch()
	if retry == nil || retry.GetReason() != opensplunkv1.RetryBatchReason_RETRY_BATCH_REASON_SERVER_BUSY {
		t.Fatalf("capacity response = %#v, want SERVER_BUSY RetryBatch", response)
	}
	if state.highestBatchSequence != 1 {
		t.Fatalf("highest sequence = %d, want 1", state.highestBatchSequence)
	}

	// Completing the original pending identity frees capacity. Because the
	// rejected capacity attempt did not consume sequence two, it can be sent
	// again without a sequence conflict or data loss.
	response, err = service.processBatch(context.Background(), first, state)
	if err != nil || response.GetBatchAck() == nil {
		t.Fatalf("first retry response = (%#v, %v), want BatchAck", response, err)
	}
	response, err = service.processBatch(context.Background(), second, state)
	if err != nil || response.GetBatchAck() == nil {
		t.Fatalf("second retry response = (%#v, %v), want BatchAck", response, err)
	}
	if got, want := calls, []uint64{1, 1, 2}; !equalUint64s(got, want) {
		t.Fatalf("store sequences = %v, want %v", got, want)
	}
}

func TestProcessBatchMapsDurableIdentityConflictToBatchReject(t *testing.T) {
	for _, conflictOnLookup := range []bool{true, false} {
		name := "store"
		if conflictOnLookup {
			name = "lookup"
		}
		t.Run(name, func(t *testing.T) {
			store := &durableIdentityConflictTestStore{conflictOnLookup: conflictOnLookup}
			service, err := NewService(testServiceConfig(), staticTestAuthorizer(), store)
			if err != nil {
				t.Fatal(err)
			}
			state := testBatchStreamState()
			batch := validTestBatch("collector-a", "batch-conflict", 1, validTestEvent("event-one", "main"))
			response, err := service.processBatch(context.Background(), batch, state)
			if err != nil {
				t.Fatal(err)
			}
			rejection := response.GetBatchReject()
			if rejection == nil || rejection.GetCode() != opensplunkv1.BatchRejectionCode_BATCH_REJECTION_CODE_SEQUENCE_CONFLICT {
				t.Fatalf("response = %#v, want SEQUENCE_CONFLICT", response)
			}
			if len(state.pendingBatches) != 0 {
				t.Fatalf("pending batches = %d, want terminal conflict released", len(state.pendingBatches))
			}
		})
	}
}

func TestProcessBatchTerminallyRejectsExpandedDurableOutbox(t *testing.T) {
	config := testServiceConfig()
	config.Redaction.Replacement = strings.Repeat("r", 100)
	storeCalls := 0
	store := EventStoreFunc(func(context.Context, StoreBatch) (StoreResult, error) {
		storeCalls++
		return StoreResult{}, nil
	})
	service, err := NewService(config, staticTestAuthorizer(), store)
	if err != nil {
		t.Fatal(err)
	}
	events := make([]*opensplunkv1.LogEvent, 20)
	for i := range events {
		events[i] = validTestEvent(fmt.Sprintf("event-%d", i), "main")
		events[i].Raw = []byte(strings.Repeat("token=x ", 8_000))
	}
	batch := validTestBatch("collector-a", "expanded-redaction", 1, events...)
	response, err := service.processBatch(context.Background(), batch, testBatchStreamState())
	if err != nil {
		t.Fatal(err)
	}
	if rejection := response.GetBatchReject(); rejection == nil ||
		rejection.GetCode() != opensplunkv1.BatchRejectionCode_BATCH_REJECTION_CODE_BATCH_TOO_LARGE {
		t.Fatalf("response = %#v, want terminal BATCH_TOO_LARGE", response)
	}
	if storeCalls != 0 {
		t.Fatalf("store calls = %d, want 0", storeCalls)
	}
}

func TestProcessBatchTerminallyRejectsOversizedDurableOutcome(t *testing.T) {
	storeCalls := 0
	store := EventStoreFunc(func(context.Context, StoreBatch) (StoreResult, error) {
		storeCalls++
		return StoreResult{}, nil
	})
	service, err := NewService(testServiceConfig(), staticTestAuthorizer(), store)
	if err != nil {
		t.Fatal(err)
	}
	const rejectedEvents = 280
	events := make([]*opensplunkv1.LogEvent, 0, rejectedEvents+1)
	longName := strings.Repeat("n", int(HardMaxFieldNameBytes))
	for i := 0; i < rejectedEvents; i++ {
		nested := object(stringField(longName, "value"))
		for range HardMaxNestingDepth {
			nested = object(objectField(longName, nested))
		}
		event := validTestEvent(fmt.Sprintf("invalid-%d", i), "main")
		event.Fields = nested
		events = append(events, event)
	}
	events = append(events, validTestEvent("accepted", "main"))
	batch := validTestBatch("collector-a", "expanded-outcome", 1, events...)
	response, err := service.processBatch(context.Background(), batch, testBatchStreamState())
	if err != nil {
		t.Fatal(err)
	}
	if rejection := response.GetBatchReject(); rejection == nil ||
		rejection.GetCode() != opensplunkv1.BatchRejectionCode_BATCH_REJECTION_CODE_BATCH_TOO_LARGE {
		t.Fatalf("response = %#v, want terminal BATCH_TOO_LARGE", response)
	}
	if storeCalls != 0 {
		t.Fatalf("store calls = %d, want 0", storeCalls)
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

func TestCollectEnforcesBatchEventCountAndEncodedByteLimits(t *testing.T) {
	t.Run("event count", func(t *testing.T) {
		cfg := testServiceConfig()
		cfg.Limits.MaxBatchEvents = 1
		harness := newServiceHarness(t, cfg, staticTestAuthorizer(), acceptingStore())
		stream := harness.stream(t, "Bearer good-token")
		sendHello(t, stream, 1, 1, 0)
		_ = recvResponse(t, stream)
		batch := validTestBatch(
			"collector-a", "batch-count", 1,
			validTestEvent("event-one", "main"),
			validTestEvent("event-two", "main"),
		)
		if err := stream.Send(batchRequest(2, batch)); err != nil {
			t.Fatal(err)
		}
		if got := recvResponse(t, stream).GetBatchReject().GetCode(); got != opensplunkv1.BatchRejectionCode_BATCH_REJECTION_CODE_TOO_MANY_EVENTS {
			t.Fatalf("batch rejection = %v", got)
		}
	})

	t.Run("encoded bytes use actual events rather than trusting declared size", func(t *testing.T) {
		first := validTestEvent("event-one", "main")
		second := validTestEvent("event-two", "main")
		oneEventBytes := UncompressedEventBytes([]*opensplunkv1.LogEvent{first})
		allEventBytes := UncompressedEventBytes([]*opensplunkv1.LogEvent{first, second})
		cfg := testServiceConfig()
		cfg.Limits.MaxEventBytes = oneEventBytes
		cfg.Limits.MaxBatchBytes = allEventBytes - 1
		harness := newServiceHarness(t, cfg, staticTestAuthorizer(), acceptingStore())
		stream := harness.stream(t, "Bearer good-token")
		sendHello(t, stream, 1, 1, 0)
		_ = recvResponse(t, stream)
		batch := validTestBatch("collector-a", "batch-bytes", 1, first, second)
		if err := stream.Send(batchRequest(2, batch)); err != nil {
			t.Fatal(err)
		}
		if got := recvResponse(t, stream).GetBatchReject().GetCode(); got != opensplunkv1.BatchRejectionCode_BATCH_REJECTION_CODE_BATCH_TOO_LARGE {
			t.Fatalf("batch rejection = %v", got)
		}
	})
}

func TestCollectPartiallyRejectsOversizedEvent(t *testing.T) {
	small := validTestEvent("event-small", "main")
	large := validTestEvent("event-large", "main")
	smallBytes := UncompressedEventBytes([]*opensplunkv1.LogEvent{small})
	large.Raw = append(large.Raw, bytes.Repeat([]byte("x"), 128)...)
	cfg := testServiceConfig()
	cfg.Limits.MaxEventBytes = smallBytes + 16
	var stored StoreBatch
	store := EventStoreFunc(func(_ context.Context, batch StoreBatch) (StoreResult, error) {
		stored = batch
		return StoreResult{Accepted: uint32(len(batch.Events))}, nil
	})
	harness := newServiceHarness(t, cfg, staticTestAuthorizer(), store)
	stream := harness.stream(t, "Bearer good-token")
	sendHello(t, stream, 1, 1, 0)
	_ = recvResponse(t, stream)
	batch := validTestBatch("collector-a", "batch-event-size", 1, small, large)
	if err := stream.Send(batchRequest(2, batch)); err != nil {
		t.Fatal(err)
	}
	ack := recvResponse(t, stream).GetBatchAck()
	if ack == nil || ack.GetAcceptedEventCount() != 1 || len(ack.GetRejectedEvents()) != 1 {
		t.Fatalf("ack = %#v", ack)
	}
	if ack.GetRejectedEvents()[0].GetCode() != opensplunkv1.EventRejectionCode_EVENT_REJECTION_CODE_EVENT_TOO_LARGE {
		t.Fatalf("event rejection = %#v", ack.GetRejectedEvents()[0])
	}
	if len(stored.Events) != 1 || stored.Events[0].Event.GetEventId() != "event-small" {
		t.Fatalf("stored events = %#v", stored.Events)
	}
}

func TestCollectRejectsInconsistentStoreAccountingWithoutAck(t *testing.T) {
	store := EventStoreFunc(func(context.Context, StoreBatch) (StoreResult, error) {
		return StoreResult{}, nil
	})
	harness := newServiceHarness(t, testServiceConfig(), staticTestAuthorizer(), store)
	stream := harness.stream(t, "Bearer good-token")
	sendHello(t, stream, 1, 1, 0)
	_ = recvResponse(t, stream)
	batch := validTestBatch("collector-a", "batch-accounting", 1, validTestEvent("event-a", "main"))
	if err := stream.Send(batchRequest(2, batch)); err != nil {
		t.Fatal(err)
	}
	response, err := stream.Recv()
	if response != nil || status.Code(err) != codes.Internal {
		t.Fatalf("Recv() = (%#v, %v), want nil/Internal", response, err)
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

type recoverableTestStore struct {
	first       StoreBatch
	identity    StoreBatchIdentity
	result      StoreResult
	storeCalls  int
	lookupCalls int
}

type durableIdentityConflictTestStore struct {
	conflictOnLookup bool
}

func (*durableIdentityConflictTestStore) Store(context.Context, StoreBatch) (StoreResult, error) {
	return StoreResult{}, &DurableIdentityConflictError{Err: errors.New("identity reused")}
}

func (store *durableIdentityConflictTestStore) LookupBatch(context.Context, StoreBatchIdentity) (StoredBatchState, StoreResult, error) {
	if store.conflictOnLookup {
		return StoredBatchNotFound, StoreResult{}, &DurableIdentityConflictError{Err: errors.New("identity reused")}
	}
	return StoredBatchNotFound, StoreResult{}, nil
}

func (*durableIdentityConflictTestStore) ResumeBatch(context.Context, StoreBatchIdentity) (StoreResult, error) {
	return StoreResult{}, errors.New("unexpected pending resume")
}

func (store *recoverableTestStore) Store(_ context.Context, batch StoreBatch) (StoreResult, error) {
	store.storeCalls++
	store.first = batch
	store.identity = StoreBatchIdentity{
		TenantID: batch.TenantID, CollectorID: batch.CollectorID, BatchID: batch.BatchID,
		BatchSequence: batch.BatchSequence, SourceBatchSHA256: batch.SourceBatchSHA256,
	}
	acknowledged := batch.BatchSequence
	store.result = StoreResult{
		Accepted:            uint32(len(batch.Events)),
		AcknowledgedThrough: &acknowledged,
		CommittedAt:         validationTestNow.Add(time.Second),
		OriginalEventCount:  batch.OriginalEventCount,
		RejectedEvents:      batch.RejectedEvents,
	}
	return store.result, nil
}

func (store *recoverableTestStore) LookupBatch(_ context.Context, identity StoreBatchIdentity) (StoredBatchState, StoreResult, error) {
	store.lookupCalls++
	if store.storeCalls == 0 || identity != store.identity {
		return StoredBatchNotFound, StoreResult{}, nil
	}
	result := store.result
	result.Duplicate = result.Accepted
	result.Accepted = 0
	return StoredBatchCommitted, result, nil
}

func (*recoverableTestStore) ResumeBatch(context.Context, StoreBatchIdentity) (StoreResult, error) {
	return StoreResult{}, errors.New("unexpected pending resume")
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

func equalUint64s(left, right []uint64) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}

func testBatchStreamState() *streamState {
	return &streamState{
		collectorID:   "collector-a",
		protocolMajor: 1,
		protocolMinor: 0,
		authorization: Authorization{TenantID: "tenant-a"},
		authorizedIndexes: map[string]struct{}{
			"main": {},
		},
	}
}
