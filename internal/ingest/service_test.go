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
	"github.com/Suhaibinator/open-splunk/internal/buildinfo"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestCollectAuthenticatesBearerTokenAndNegotiatesReady(t *testing.T) {
	var gotToken string
	authorizer := AuthorizerFunc(func(_ context.Context, token string) (Authorization, error) {
		gotToken = token
		return Authorization{
			SubjectID:         "token-1",
			TenantID:          "tenant-a",
			CollectorID:       "collector-a",
			AuthorizedIndexes: testIndexPolicies("z-last", "main"),
		}, nil
	})
	config := testServiceConfig()
	build := validServiceBuildMetadata(t)
	config.Build = build
	config.ServerVersion = ""
	expectedServerVersion := buildinfo.Identity{
		ApplicationVersion: build.GetApplicationVersion(),
		SourceRevision:     build.GetSourceRevision(),
	}.DisplayVersion()
	harness := newServiceHarness(t, config, authorizer, acceptingStore())
	build.SourceRevision = "mutated"
	stream := harness.stream(t, "Bearer token-value")

	sendHello(t, stream, 1)
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
	if ready.GetBuild().GetApplicationVersion() != "1.2.3" ||
		ready.GetBuild().GetSourceRevision() != strings.Repeat("a", 40) {
		t.Fatalf("ready build = %+v", ready.GetBuild())
	}
	if ready.GetServerVersion() != expectedServerVersion ||
		ready.GetBuild().GetAssetManifestFormatVersion() != 1 {
		t.Fatalf("ready release identity = %q %+v", ready.GetServerVersion(), ready.GetBuild())
	}
	if got, want := ready.GetAuthorizedIndexes(), []string{"main", "z-last"}; !equalStrings(got, want) {
		t.Fatalf("authorized indexes = %v, want %v", got, want)
	}
	if ready.GetAcknowledgmentDurability() != opensplunkv1.AckDurability_ACK_DURABILITY_CLICKHOUSE_COMMITTED {
		t.Fatalf("ack durability = %v", ready.GetAcknowledgmentDurability())
	}
}

func TestCollectSupportsMaximumControlPlaneIndexNameEndToEnd(t *testing.T) {
	t.Parallel()
	indexName := strings.Repeat("a", 255)
	authorization := Authorization{
		SubjectID:         "token-maximum-index",
		TenantID:          "tenant-a",
		CollectorID:       "collector-a",
		AuthorizedIndexes: testIndexPolicies(indexName),
	}
	authorizer := AuthorizerFunc(func(context.Context, string) (Authorization, error) {
		return authorization, nil
	})
	var stored StoreBatch
	store := EventStoreFunc(func(_ context.Context, batch StoreBatch) (StoreResult, error) {
		stored = batch
		return StoreResult{
			Accepted:    uint32(len(batch.Events)),
			CommittedAt: validationTestNow,
		}, nil
	})
	harness := newServiceHarness(t, testServiceConfig(), authorizer, store)
	stream := harness.stream(t, "Bearer maximum-index-token")
	if err := stream.Send(&opensplunkv1.CollectRequest{
		StreamSequence: 1,
		SentAt:         timestamppb.New(validationTestNow),
		Payload: &opensplunkv1.CollectRequest_Hello{
			Hello: &opensplunkv1.CollectorHello{
				CollectorId:      "collector-a",
				InstanceId:       "instance-a",
				ProtocolMajor:    1,
				CollectorVersion: "collector-test",
				StartedAt:        timestamppb.New(validationTestNow.Add(-time.Hour)),
				Inputs: []*opensplunkv1.CollectorInputRegistration{{
					InputId:   "input-a",
					InputType: opensplunkv1.CollectorInputType_COLLECTOR_INPUT_TYPE_FILE,
					IndexName: indexName,
				}},
			},
		},
	}); err != nil {
		t.Fatal(err)
	}
	ready := recvResponse(t, stream).GetReady()
	if ready == nil ||
		len(ready.GetAuthorizedIndexes()) != 1 ||
		ready.GetAuthorizedIndexes()[0] != indexName {
		t.Fatalf("Ready authorized indexes = %#v", ready.GetAuthorizedIndexes())
	}
	batch := validTestBatch(
		"collector-a",
		"batch-maximum-index",
		1,
		validTestEvent("event-maximum-index", indexName),
	)
	if err := stream.Send(batchRequest(2, batch)); err != nil {
		t.Fatal(err)
	}
	ack := recvResponse(t, stream).GetBatchAck()
	if ack == nil || ack.GetAcceptedEventCount() != 1 {
		t.Fatalf("batch acknowledgment = %#v", ack)
	}
	if len(stored.Events) != 1 ||
		stored.Events[0].Event.GetIndexName() != indexName {
		t.Fatalf("stored maximum index batch = %#v", stored)
	}
}

func TestNewServiceRejectsContradictoryOrMalformedBuildMetadata(t *testing.T) {
	t.Parallel()

	config := testServiceConfig()
	config.Build = validServiceBuildMetadata(t)
	if _, err := NewService(config, staticTestAuthorizer(), acceptingStore()); err == nil ||
		!strings.Contains(err.Error(), "does not match") {
		t.Fatalf("contradictory server version error = %v", err)
	}

	config = testServiceConfig()
	config.ServerVersion = ""
	config.Build = validServiceBuildMetadata(t)
	config.Build.ProtobufSchemaSha256 = "malformed"
	if _, err := NewService(config, staticTestAuthorizer(), acceptingStore()); err == nil ||
		!strings.Contains(err.Error(), "protobuf schema") {
		t.Fatalf("malformed build metadata error = %v", err)
	}
}

func validServiceBuildMetadata(t *testing.T) *opensplunkv1.BuildMetadata {
	t.Helper()
	identity, err := buildinfo.Parse("1.2.3", strings.Repeat("a", 40))
	if err != nil {
		t.Fatal(err)
	}
	uiBuildID, err := identity.UIBuildID()
	if err != nil {
		t.Fatal(err)
	}
	return &opensplunkv1.BuildMetadata{
		ApplicationVersion:         identity.ApplicationVersion,
		SourceRevision:             identity.SourceRevision,
		UiBuildId:                  uiBuildID,
		UiSha256:                   strings.Repeat("1", 64),
		ProtobufSchemaSha256:       strings.Repeat("2", 64),
		SqliteMigrationsSha256:     strings.Repeat("3", 64),
		SqliteMigrationVersion:     2,
		ClickhouseMigrationsSha256: strings.Repeat("4", 64),
		ClickhouseMigrationVersion: 1,
		AssetManifestFormatVersion: 1,
	}
}

func TestCollectRejectsMissingOrInvalidAuthentication(t *testing.T) {
	authorizer := AuthorizerFunc(func(context.Context, string) (Authorization, error) {
		return Authorization{}, ErrUnauthorized
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

func TestCollectReportsAuthenticationBackendFailureAsUnavailable(t *testing.T) {
	t.Parallel()
	backendErr := errors.New("database unavailable")
	authorizer := AuthorizerFunc(func(context.Context, string) (Authorization, error) {
		return Authorization{}, backendErr
	})
	harness := newServiceHarness(t, testServiceConfig(), authorizer, acceptingStore())
	stream := harness.stream(t, "Bearer syntactically-valid")
	if _, err := stream.Recv(); status.Code(err) != codes.Unavailable {
		t.Fatalf("Recv() error = %v, want Unavailable", err)
	}
}

func TestCollectLimitsConcurrentPreHelloStreamsPerCredential(t *testing.T) {
	t.Parallel()
	config := testServiceConfig()
	config.MaxStreamsPerSubject = 1
	authorizer := mappedCollectorAuthorizer(map[string]Authorization{
		"collector-a-token": boundTestAuthorization("shared-subject", "tenant-a", "collector-a"),
		"collector-b-token": boundTestAuthorization("shared-subject", "tenant-a", "collector-b"),
	})
	harness := newServiceHarness(t, config, authorizer, acceptingStore())

	streams := []opensplunkv1.CollectorIngestService_CollectClient{
		harness.stream(t, "Bearer collector-a-token"),
		harness.stream(t, "Bearer collector-b-token"),
	}
	results := make(chan error, len(streams))
	for _, stream := range streams {
		go func() {
			_, err := stream.Recv()
			results <- err
		}()
	}
	select {
	case err := <-results:
		if status.Code(err) != codes.ResourceExhausted {
			t.Fatalf("quota loser error = %v, want ResourceExhausted", err)
		}
	case <-time.After(time.Second):
		t.Fatal("distinct pre-Hello admission did not hit subject quota")
	}
	for _, stream := range streams {
		if err := stream.CloseSend(); err != nil {
			t.Fatal(err)
		}
	}
	select {
	case err := <-results:
		if status.Code(err) != codes.InvalidArgument {
			t.Fatalf("quota winner close error = %v, want InvalidArgument", err)
		}
	case <-time.After(time.Second):
		t.Fatal("quota winner did not exit after CloseSend")
	}

	third := harness.stream(t, "Bearer collector-a-token")
	sendHello(t, third, 1)
	if ready := recvResponse(t, third).GetReady(); ready == nil {
		t.Fatal("released per-credential stream capacity was not reusable")
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
		sendHello(t, stream, 1)
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
			SubjectID:         "token-1",
			TenantID:          "tenant-a",
			CollectorID:       "bound-collector",
			AuthorizedIndexes: testIndexPolicies("main"),
		}, nil
	})
	harness := newServiceHarness(t, testServiceConfig(), authorizer, acceptingStore())

	t.Run("hello must match token binding", func(t *testing.T) {
		stream := harness.stream(t, "Bearer good-token")
		if err := stream.Send(helloRequestFor(1, "different-collector", 1, 0)); err != nil {
			t.Fatal(err)
		}
		_, err := stream.Recv()
		if status.Code(err) != codes.PermissionDenied {
			t.Fatalf("Recv() error = %v, want PermissionDenied", err)
		}
	})

	t.Run("batch collector must match hello", func(t *testing.T) {
		stream := harness.stream(t, "Bearer good-token")
		if err := stream.Send(helloRequestFor(1, "bound-collector", 1, 0)); err != nil {
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
			return Authorization{}, ErrUnauthorized
		}
		return Authorization{
			SubjectID:         "token-1",
			TenantID:          "tenant-a",
			CollectorID:       "collector-a",
			AuthorizedIndexes: testIndexPolicies("main"),
		}, nil
	})
	storeCalls := 0
	store := EventStoreFunc(func(context.Context, StoreBatch) (StoreResult, error) {
		storeCalls++
		return StoreResult{Accepted: 1}, nil
	})
	harness := newServiceHarness(t, testServiceConfig(), authorizer, store)
	stream := harness.stream(t, "Bearer token-that-will-be-revoked")
	sendHello(t, stream, 1)
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
	sendHello(t, stream, 1)
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
			SubjectID: "token-1", TenantID: "tenant-a", CollectorID: "collector-a",
			AuthorizedIndexes: testIndexPolicies(indexes...),
		}, nil
	})
	store := &recoverableTestStore{}
	harness := newServiceHarness(t, testServiceConfig(), authorizer, store)
	mainEvent := validTestEvent("event-main", "main")
	auditEvent := validTestEvent("event-audit", "audit")
	batch := validTestBatch("collector-a", "batch-lost-ack", 1, mainEvent, auditEvent)

	first := harness.stream(t, "Bearer good-token")
	sendHello(t, first, 1)
	_ = recvResponse(t, first)
	if err := first.Send(batchRequest(2, batch)); err != nil {
		t.Fatal(err)
	}
	// Receive and discard the first response to model an ACK lost after the
	// durable server decision but before collector checkpoint advancement.
	_ = recvResponse(t, first)
	_ = first.CloseSend()

	second := harness.stream(t, "Bearer good-token")
	sendHello(t, second, 1)
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
	sendHello(t, stream, 1)
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

func TestCollectRetriesChangedStreamLocalPendingIdentity(t *testing.T) {
	storeCalls := 0
	store := EventStoreFunc(func(context.Context, StoreBatch) (StoreResult, error) {
		storeCalls++
		return StoreResult{}, &TransientStoreError{Err: errors.New("retry")}
	})
	harness := newServiceHarness(t, testServiceConfig(), staticTestAuthorizer(), store)
	stream := harness.stream(t, "Bearer good-token")
	sendHello(t, stream, 1)
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
	if retry := response.GetRetryBatch(); retry == nil ||
		retry.GetReason() != opensplunkv1.RetryBatchReason_RETRY_BATCH_REASON_SERVER_BUSY {
		t.Fatalf("response = %#v, want non-terminal SERVER_BUSY retry", response)
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
	sendHello(t, stream, 1)
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
	firstConfig = withTestSessionManager(firstConfig, staticTestAuthorizer())
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
	firstResponse, err := first.processBatch(
		context.Background(), batch, testBatchStreamState(first), first.config.Clock().UTC(),
	)
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
	retryConfig = withTestSessionManager(retryConfig, staticTestAuthorizer())
	retry, err := NewService(retryConfig, staticTestAuthorizer(), store)
	if err != nil {
		t.Fatal(err)
	}
	retryAuthority, ok := retry.resolveAuthorizedIndexPolicies([]IndexPolicy{{
		Name: "main", Version: 2, RetentionPeriod: time.Hour,
		Limits: IndexLimits{MaxEventBytes: 1},
	}}, retry.config.Clock().UTC())
	if !ok {
		t.Fatal("resolve restrictive retry policy")
	}
	retryState := testBatchStreamState(retry)
	retryState.indexPolicies = retryAuthority.byName
	retryResponse, err := retry.processBatch(
		context.Background(), batch, retryState, retry.config.Clock().UTC(),
	)
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

func TestProcessBatchPersistsAndReplaysConfiguredBatchRejection(t *testing.T) {
	config := testServiceConfig()
	config.Limits.MaxBatchEvents = 1
	config = withTestSessionManager(config, staticTestAuthorizer())
	store := &recoverableTestStore{}
	service, err := NewService(config, staticTestAuthorizer(), store)
	if err != nil {
		t.Fatal(err)
	}
	batch := validTestBatch(
		"collector-a",
		"batch-durable-policy-rejection",
		7,
		validTestEvent("event-one", "main"),
		validTestEvent("event-two", "main"),
	)
	receivedAt := validationTestNow.Add(3 * time.Second)

	first, err := service.processBatch(
		context.Background(), batch, testBatchStreamState(service), receivedAt,
	)
	if err != nil {
		t.Fatal(err)
	}
	if rejection := first.GetBatchReject(); rejection == nil ||
		rejection.GetCode() != opensplunkv1.BatchRejectionCode_BATCH_REJECTION_CODE_TOO_MANY_EVENTS {
		t.Fatalf("first response = %#v", first)
	}
	identity, err := batchFingerprint(batch)
	if err != nil {
		t.Fatal(err)
	}
	wantIdentity := StoreBatchIdentity{
		TenantID:          "tenant-a",
		CollectorID:       "collector-a",
		BatchID:           batch.GetBatchId(),
		BatchSequence:     batch.GetBatchSequence(),
		SourceBatchSHA256: identity.contentHash,
	}
	if store.rejectCalls != 1 || store.storeCalls != 0 ||
		store.rejection.Identity != wantIdentity ||
		!store.rejection.ReceivedAt.Equal(receivedAt) ||
		!proto.Equal(store.rejection.Rejection, first.GetBatchReject()) {
		t.Fatalf("durable rejection call = %+v, response = %#v", store.rejection, first)
	}
	if store.rejection.Rejection != store.result.BatchRejection {
		t.Fatal("RejectBatch did not receive the exact rejection used as its durable result")
	}

	// A later process with permissive mutable policy must replay the original
	// terminal decision instead of revalidating and accepting the same bytes.
	retryConfig := testServiceConfig()
	retryConfig = withTestSessionManager(retryConfig, staticTestAuthorizer())
	retry, err := NewService(retryConfig, staticTestAuthorizer(), store)
	if err != nil {
		t.Fatal(err)
	}
	replayed, err := retry.processBatch(
		context.Background(), batch, testBatchStreamState(retry), receivedAt.Add(time.Hour),
	)
	if err != nil {
		t.Fatal(err)
	}
	if !proto.Equal(replayed.GetBatchReject(), first.GetBatchReject()) {
		t.Fatalf("replayed rejection = %#v, want %#v", replayed, first)
	}
	if replayed.GetBatchReject() == store.result.BatchRejection {
		t.Fatal("replayed response aliases store-owned rejection")
	}
	if store.lookupCalls != 2 || store.rejectCalls != 1 || store.storeCalls != 0 {
		t.Fatalf(
			"store calls = lookup:%d reject:%d store:%d, want 2/1/0",
			store.lookupCalls,
			store.rejectCalls,
			store.storeCalls,
		)
	}
}

func TestProcessBatchRejectsDurableStateDispositionMismatch(t *testing.T) {
	config := withTestSessionManager(testServiceConfig(), staticTestAuthorizer())
	batch := validTestBatch(
		"collector-a", "batch-state-mismatch", 1, validTestEvent("event-one", "main"),
	)
	identity, err := batchFingerprint(batch)
	if err != nil {
		t.Fatal(err)
	}
	durableIdentity := StoreBatchIdentity{
		TenantID:          "tenant-a",
		CollectorID:       "collector-a",
		BatchID:           batch.GetBatchId(),
		BatchSequence:     batch.GetBatchSequence(),
		SourceBatchSHA256: identity.contentHash,
	}
	tests := []struct {
		name   string
		state  StoredBatchState
		result StoreResult
	}{
		{
			name:  "committed state with rejection",
			state: StoredBatchCommitted,
			result: StoreResult{BatchRejection: batchRejection(
				batch,
				opensplunkv1.BatchRejectionCode_BATCH_REJECTION_CODE_NO_AUTHORIZED_EVENTS,
				"stored rejection",
				"events",
				"invalid",
			)},
		},
		{
			name:   "rejected state with acknowledgment",
			state:  StoredBatchRejected,
			result: StoreResult{Accepted: 1, OriginalEventCount: 1, CommittedAt: validationTestNow},
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			store := &recoverableTestStore{
				identity: durableIdentity, result: test.result, storedState: test.state,
			}
			service, err := NewService(config, staticTestAuthorizer(), store)
			if err != nil {
				t.Fatal(err)
			}
			response, processErr := service.processBatch(
				context.Background(), batch, testBatchStreamState(service), validationTestNow,
			)
			if response != nil || status.Code(processErr) != codes.Internal {
				t.Fatalf("processBatch = (%#v, %v), want nil/Internal", response, processErr)
			}
			if store.lookupCalls != 1 || store.rejectCalls != 0 || store.storeCalls != 0 {
				t.Fatalf("store calls = lookup:%d reject:%d store:%d", store.lookupCalls, store.rejectCalls, store.storeCalls)
			}
		})
	}
}

func TestProcessBatchHandlesRejectionWhenPendingBatchResumes(t *testing.T) {
	config := withTestSessionManager(testServiceConfig(), staticTestAuthorizer())
	batch := validTestBatch(
		"collector-a", "batch-pending-rejection-transition", 1,
		validTestEvent("event-one", "main"),
	)
	validRejection := batchRejection(
		batch,
		opensplunkv1.BatchRejectionCode_BATCH_REJECTION_CODE_NO_AUTHORIZED_EVENTS,
		"concurrent rejection won the durable outcome",
		"events",
		"invalid",
	)
	tests := []struct {
		name       string
		result     StoreResult
		wantReject *opensplunkv1.BatchReject
		wantCode   codes.Code
	}{
		{
			name:       "valid terminal rejection",
			result:     StoreResult{BatchRejection: validRejection},
			wantReject: validRejection,
		},
		{
			name: "malformed terminal rejection",
			result: StoreResult{BatchRejection: &opensplunkv1.BatchReject{
				BatchId:       "different-batch",
				BatchSequence: batch.GetBatchSequence(),
				Code:          opensplunkv1.BatchRejectionCode_BATCH_REJECTION_CODE_NO_AUTHORIZED_EVENTS,
				Message:       "mismatched durable identity",
			}},
			wantCode: codes.Internal,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store := &pendingAuthorityTestStore{result: test.result}
			service, err := NewService(config, staticTestAuthorizer(), store)
			if err != nil {
				t.Fatal(err)
			}
			response, processErr := service.processBatch(
				context.Background(), batch, testBatchStreamState(service), validationTestNow,
			)
			if test.wantReject != nil {
				if processErr != nil || !proto.Equal(response.GetBatchReject(), test.wantReject) {
					t.Fatalf("processBatch = (%#v, %v), want rejection %#v", response, processErr, test.wantReject)
				}
			} else if response != nil || status.Code(processErr) != test.wantCode {
				t.Fatalf("processBatch = (%#v, %v), want nil/%v", response, processErr, test.wantCode)
			}
			if store.lookupCalls != 1 || store.resumeCalls != 1 ||
				store.storeCalls != 0 || store.rejectCalls != 0 {
				t.Fatalf(
					"store calls = lookup:%d resume:%d store:%d reject:%d",
					store.lookupCalls,
					store.resumeCalls,
					store.storeCalls,
					store.rejectCalls,
				)
			}
		})
	}
}

func TestProcessBatchPendingReservationGoneContinuesFreshInSameCall(t *testing.T) {
	config := withTestSessionManager(testServiceConfig(), staticTestAuthorizer())
	store := &goneResumeTestStore{resumeErr: storedBatchGoneTestError()}
	service, err := NewService(config, staticTestAuthorizer(), store)
	if err != nil {
		t.Fatal(err)
	}
	state := testBatchStreamState(service)
	batch := validTestBatch(
		"collector-a", "batch-pending-gone-fresh", 1,
		validTestEvent("event-one", "main"),
	)

	response, processErr := service.processBatch(
		context.Background(), batch, state, validationTestNow,
	)
	if processErr != nil {
		t.Fatal(processErr)
	}
	if ack := response.GetBatchAck(); ack == nil || ack.GetAcceptedEventCount() != 1 {
		t.Fatalf("processBatch response = %#v, want one accepted event", response)
	}
	if store.lookupCalls != 1 || store.resumeCalls != 1 ||
		store.storeCalls != 1 || store.rejectCalls != 0 {
		t.Fatalf(
			"store calls = lookup:%d resume:%d store:%d reject:%d, want 1/1/1/0",
			store.lookupCalls,
			store.resumeCalls,
			store.storeCalls,
			store.rejectCalls,
		)
	}
	if !state.hasHighestBatchSequence || state.highestBatchSequence != batch.GetBatchSequence() ||
		len(state.pendingBatches) != 0 {
		t.Fatalf("stream state after fresh progress = %+v", state)
	}
}

func TestProcessBatchPendingReservationGoneDefersToIndexAuthority(t *testing.T) {
	config := withTestSessionManager(testServiceConfig(), staticTestAuthorizer())
	store := &goneResumeTestStore{resumeErr: storedBatchGoneTestError()}
	service, err := NewService(config, staticTestAuthorizer(), store)
	if err != nil {
		t.Fatal(err)
	}
	state := testBatchStreamState(service)
	batch := validTestBatch(
		"collector-a", "batch-pending-gone-authority", 1,
		validTestEvent("event-one", "main"),
	)

	response, processErr := service.processBatchWithDeferredAuthority(
		context.Background(), batch, state, validationTestNow, ErrNoActiveIndexAuthority,
	)
	if response != nil || status.Code(processErr) != codes.Unauthenticated {
		t.Fatalf("processBatch = (%#v, %v), want nil/Unauthenticated", response, processErr)
	}
	if store.lookupCalls != 1 || store.resumeCalls != 1 ||
		store.storeCalls != 0 || store.rejectCalls != 0 {
		t.Fatalf(
			"store calls = lookup:%d resume:%d store:%d reject:%d, want 1/1/0/0",
			store.lookupCalls,
			store.resumeCalls,
			store.storeCalls,
			store.rejectCalls,
		)
	}
	if state.hasHighestBatchSequence || len(state.pendingBatches) != 0 {
		t.Fatalf("gone reservation poisoned stream state: %+v", state)
	}
}

func TestProcessBatchPendingResumeTransientThenDisappearsContinuesFreshOnRetry(t *testing.T) {
	config := withTestSessionManager(testServiceConfig(), staticTestAuthorizer())
	store := &goneResumeTestStore{
		lookupStates: []StoredBatchState{StoredBatchPending, StoredBatchNotFound},
		resumeErr: &TransientStoreError{
			Err:        errors.New("pending attempt is still owned"),
			Reason:     opensplunkv1.RetryBatchReason_RETRY_BATCH_REASON_SERVER_BUSY,
			RetryAfter: time.Millisecond,
		},
	}
	service, err := NewService(config, staticTestAuthorizer(), store)
	if err != nil {
		t.Fatal(err)
	}
	state := testBatchStreamState(service)
	batch := validTestBatch(
		"collector-a", "batch-pending-transient-then-gone", 1,
		validTestEvent("event-one", "main"),
	)
	firstBoundary := validationTestNow
	secondBoundary := firstBoundary.Add(time.Minute)

	response, processErr := service.processBatch(
		context.Background(), batch, state, firstBoundary,
	)
	if processErr != nil ||
		response.GetRetryBatch().GetReason() != opensplunkv1.RetryBatchReason_RETRY_BATCH_REASON_SERVER_BUSY {
		t.Fatalf("first processBatch = (%#v, %v), want SERVER_BUSY retry", response, processErr)
	}
	pending, ok := state.pendingBatches[batch.GetBatchSequence()]
	if !ok || !pending.receivedAt.Equal(firstBoundary) {
		t.Fatalf("remembered pending identity = %+v found=%v, want boundary %v", pending, ok, firstBoundary)
	}

	response, processErr = service.processBatch(
		context.Background(), batch, state, secondBoundary,
	)
	if processErr != nil {
		t.Fatal(processErr)
	}
	if ack := response.GetBatchAck(); ack == nil || ack.GetAcceptedEventCount() != 1 {
		t.Fatalf("second processBatch response = %#v, want one accepted event", response)
	}
	if store.lookupCalls != 2 || store.resumeCalls != 1 ||
		store.storeCalls != 1 || store.rejectCalls != 0 {
		t.Fatalf(
			"store calls = lookup:%d resume:%d store:%d reject:%d, want 2/1/1/0",
			store.lookupCalls,
			store.resumeCalls,
			store.storeCalls,
			store.rejectCalls,
		)
	}
	if len(store.storedBatches) != 1 || !store.storedBatches[0].ReceivedAt.Equal(firstBoundary) {
		t.Fatalf("fresh Store batches = %+v, want stable first boundary %v", store.storedBatches, firstBoundary)
	}
	if state.highestBatchSequence != batch.GetBatchSequence() || len(state.pendingBatches) != 0 {
		t.Fatalf("stream state after recovered progress = %+v", state)
	}
}

func TestProcessBatchDurablePendingRecoveryBypassesSoftCapacityAndHigherSequence(t *testing.T) {
	config := testServiceConfig()
	config.MaxInFlightBatches = 1
	config = withTestSessionManager(config, staticTestAuthorizer())
	store := &goneResumeTestStore{
		lookupStates: []StoredBatchState{StoredBatchPending, StoredBatchNotFound},
		resumeErr: &TransientStoreError{
			Err:    errors.New("pending attempt is still owned"),
			Reason: opensplunkv1.RetryBatchReason_RETRY_BATCH_REASON_SERVER_BUSY,
		},
	}
	service, err := NewService(config, staticTestAuthorizer(), store)
	if err != nil {
		t.Fatal(err)
	}
	state := testBatchStreamState(service)
	later := validTestBatch(
		"collector-a", "batch-later-local-pending", 9,
		validTestEvent("event-nine", "main"),
	)
	laterIdentity, err := batchFingerprint(later)
	if err != nil {
		t.Fatal(err)
	}
	if _, rejection, atCapacity := recordBatchIdentity(
		state,
		later.GetBatchSequence(),
		laterIdentity,
		validationTestNow,
		config.MaxInFlightBatches,
	); rejection != nil || atCapacity {
		t.Fatalf("seed later pending identity = rejection %#v capacity=%v", rejection, atCapacity)
	}
	target := validTestBatch(
		"collector-a", "batch-earlier-durable-pending", 7,
		validTestEvent("event-seven", "main"),
	)
	firstBoundary := validationTestNow.Add(time.Second)

	response, processErr := service.processBatch(
		context.Background(), target, state, firstBoundary,
	)
	if processErr != nil || response.GetRetryBatch() == nil {
		t.Fatalf("first processBatch = (%#v, %v), want retry", response, processErr)
	}
	if state.highestBatchSequence != later.GetBatchSequence() || len(state.pendingBatches) != 2 {
		t.Fatalf("stream state after durable pending proof = %+v, want high-water 9 and two pending", state)
	}
	if pending := state.pendingBatches[target.GetBatchSequence()]; !pending.receivedAt.Equal(firstBoundary) {
		t.Fatalf("earlier durable pending identity = %+v, want boundary %v", pending, firstBoundary)
	}

	response, processErr = service.processBatch(
		context.Background(), target, state, firstBoundary.Add(time.Minute),
	)
	if processErr != nil || response.GetBatchAck().GetAcceptedEventCount() != 1 {
		t.Fatalf("second processBatch = (%#v, %v), want accepted progress", response, processErr)
	}
	if len(store.storedBatches) != 1 || !store.storedBatches[0].ReceivedAt.Equal(firstBoundary) {
		t.Fatalf("fresh Store batches = %+v, want stable first boundary %v", store.storedBatches, firstBoundary)
	}
	if state.highestBatchSequence != later.GetBatchSequence() || len(state.pendingBatches) != 1 {
		t.Fatalf("stream state after earlier recovery = %+v, want only later pending identity", state)
	}
	if _, ok := state.pendingBatches[later.GetBatchSequence()]; !ok {
		t.Fatalf("later pending identity was lost: %+v", state.pendingBatches)
	}
}

func TestProcessBatchDurablePendingRecoveryHardBoundForcesReconnect(t *testing.T) {
	tests := []struct {
		name       string
		resumeErr  error
		wantedCode codes.Code
	}{
		{
			name: "transient forces reconnect",
			resumeErr: &TransientStoreError{
				Err:    errors.New("pending attempt is still owned"),
				Reason: opensplunkv1.RetryBatchReason_RETRY_BATCH_REASON_SERVER_BUSY,
			},
			wantedCode: codes.Unavailable,
		},
		{
			name:       "canceled remains canceled",
			resumeErr:  context.Canceled,
			wantedCode: codes.Canceled,
		},
		{
			name:       "deadline remains deadline",
			resumeErr:  context.DeadlineExceeded,
			wantedCode: codes.DeadlineExceeded,
		},
		{
			name: "transient wrapped cancellation remains canceled",
			resumeErr: &TransientStoreError{
				Err:    context.Canceled,
				Reason: opensplunkv1.RetryBatchReason_RETRY_BATCH_REASON_SERVER_BUSY,
			},
			wantedCode: codes.Canceled,
		},
		{
			name: "transient wrapped deadline remains deadline",
			resumeErr: &TransientStoreError{
				Err:    context.DeadlineExceeded,
				Reason: opensplunkv1.RetryBatchReason_RETRY_BATCH_REASON_SERVER_BUSY,
			},
			wantedCode: codes.DeadlineExceeded,
		},
		{
			name:       "permanent remains internal",
			resumeErr:  errors.New("durable pending metadata is corrupt"),
			wantedCode: codes.Internal,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			config := withTestSessionManager(testServiceConfig(), staticTestAuthorizer())
			store := &goneResumeTestStore{
				lookupStates: []StoredBatchState{StoredBatchPending},
				resumeErr:    test.resumeErr,
			}
			service, err := NewService(config, staticTestAuthorizer(), store)
			if err != nil {
				t.Fatal(err)
			}
			state := testBatchStreamState(service)
			for offset := range HardMaxInFlightBatches {
				sequence := uint64(offset) + 1
				batch := validTestBatch(
					"collector-a",
					fmt.Sprintf("batch-hard-pending-%d", sequence),
					sequence,
					validTestEvent(fmt.Sprintf("event-%d", sequence), "main"),
				)
				identity, fingerprintErr := batchFingerprint(batch)
				if fingerprintErr != nil {
					t.Fatal(fingerprintErr)
				}
				if !rememberDurablePendingBatchIdentity(
					state,
					sequence,
					identity,
					validationTestNow,
				) {
					t.Fatalf("failed to seed durable pending identity %d", sequence)
				}
			}
			targetSequence := uint64(HardMaxInFlightBatches) + 1
			target := validTestBatch(
				"collector-a", "batch-hard-pending-overflow", targetSequence,
				validTestEvent("event-overflow", "main"),
			)

			response, processErr := service.processBatch(
				context.Background(), target, state, validationTestNow.Add(time.Second),
			)
			if response != nil || status.Code(processErr) != test.wantedCode {
				t.Fatalf(
					"processBatch = (%#v, %v), want nil/%s",
					response,
					processErr,
					test.wantedCode,
				)
			}
			if len(state.pendingBatches) != int(HardMaxInFlightBatches) ||
				state.highestBatchSequence != uint64(HardMaxInFlightBatches) {
				t.Fatalf("hard-bound stream state mutated on overflow: %+v", state)
			}
			if store.lookupCalls != 1 || store.resumeCalls != 1 ||
				store.storeCalls != 0 || store.rejectCalls != 0 {
				t.Fatalf(
					"store calls = lookup:%d resume:%d store:%d reject:%d, want 1/1/0/0",
					store.lookupCalls,
					store.resumeCalls,
					store.storeCalls,
					store.rejectCalls,
				)
			}
		})
	}
}

func TestProcessBatchPersistsAllEventsInvalidRejection(t *testing.T) {
	config := withTestSessionManager(testServiceConfig(), staticTestAuthorizer())
	store := &recoverableTestStore{}
	service, err := NewService(config, staticTestAuthorizer(), store)
	if err != nil {
		t.Fatal(err)
	}
	batch := validTestBatch(
		"collector-a",
		"batch-durable-invalid-rejection",
		9,
		validTestEvent("event-forbidden", "forbidden"),
	)
	response, err := service.processBatch(
		context.Background(), batch, testBatchStreamState(service), validationTestNow,
	)
	if err != nil {
		t.Fatal(err)
	}
	rejection := response.GetBatchReject()
	if rejection == nil ||
		rejection.GetCode() != opensplunkv1.BatchRejectionCode_BATCH_REJECTION_CODE_NO_AUTHORIZED_EVENTS {
		t.Fatalf("response = %#v", response)
	}
	if store.rejectCalls != 1 || store.storeCalls != 0 ||
		!proto.Equal(store.rejection.Rejection, rejection) {
		t.Fatalf("store = %+v, response = %#v", store, response)
	}
}

func TestProcessBatchFailsClosedWithoutRecoverableRejectionStore(t *testing.T) {
	config := testServiceConfig()
	config.Limits.MaxBatchEvents = 1
	config = withTestSessionManager(config, staticTestAuthorizer())
	service, err := NewService(config, staticTestAuthorizer(), acceptingStore())
	if err != nil {
		t.Fatal(err)
	}
	batch := validTestBatch(
		"collector-a",
		"batch-requires-durable-rejection",
		1,
		validTestEvent("event-one", "main"),
		validTestEvent("event-two", "main"),
	)
	response, err := service.processBatch(
		context.Background(), batch, testBatchStreamState(service), validationTestNow,
	)
	if response != nil || status.Code(err) != codes.Internal {
		t.Fatalf("processBatch = (%#v, %v), want nil/Internal", response, err)
	}
}

func TestProcessBatchRejectPersistenceFailurePreservesStorePrecedence(t *testing.T) {
	tests := []struct {
		name        string
		err         error
		wantRetry   opensplunkv1.RetryBatchReason
		wantCode    codes.Code
		wantReject  opensplunkv1.BatchRejectionCode
		wantPending int
	}{
		{
			name: "transient",
			err: &TransientStoreError{
				Err:    errors.New("terminal outcome store unavailable"),
				Reason: opensplunkv1.RetryBatchReason_RETRY_BATCH_REASON_STORAGE_UNAVAILABLE,
			},
			wantRetry:   opensplunkv1.RetryBatchReason_RETRY_BATCH_REASON_STORAGE_UNAVAILABLE,
			wantPending: 1,
		},
		{name: "canceled", err: context.Canceled, wantCode: codes.Canceled, wantPending: 1},
		{name: "permanent", err: errors.New("corrupt rejection store"), wantCode: codes.Internal, wantPending: 1},
		{
			name:        "durable identity conflict",
			err:         &DurableIdentityConflictError{Err: errors.New("identity reused")},
			wantReject:  opensplunkv1.BatchRejectionCode_BATCH_REJECTION_CODE_SEQUENCE_CONFLICT,
			wantPending: 0,
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			config := testServiceConfig()
			config.Limits.MaxBatchEvents = 1
			config = withTestSessionManager(config, staticTestAuthorizer())
			store := &recoverableTestStore{rejectErr: test.err}
			service, err := NewService(config, staticTestAuthorizer(), store)
			if err != nil {
				t.Fatal(err)
			}
			state := testBatchStreamState(service)
			batch := validTestBatch(
				"collector-a",
				"batch-rejection-store-failure",
				1,
				validTestEvent("event-one", "main"),
				validTestEvent("event-two", "main"),
			)
			response, processErr := service.processBatch(
				context.Background(), batch, state, validationTestNow,
			)
			switch {
			case test.wantRetry != opensplunkv1.RetryBatchReason_RETRY_BATCH_REASON_UNSPECIFIED:
				if processErr != nil || response.GetRetryBatch().GetReason() != test.wantRetry {
					t.Fatalf("processBatch = (%#v, %v), want retry %v", response, processErr, test.wantRetry)
				}
			case test.wantReject != opensplunkv1.BatchRejectionCode_BATCH_REJECTION_CODE_UNSPECIFIED:
				if processErr != nil || response.GetBatchReject().GetCode() != test.wantReject {
					t.Fatalf("processBatch = (%#v, %v), want rejection %v", response, processErr, test.wantReject)
				}
			default:
				if response != nil || status.Code(processErr) != test.wantCode {
					t.Fatalf("processBatch = (%#v, %v), want nil/%v", response, processErr, test.wantCode)
				}
			}
			if store.rejectCalls != 1 || len(state.pendingBatches) != test.wantPending {
				t.Fatalf(
					"reject calls = %d, pending = %d, want 1/%d",
					store.rejectCalls,
					len(state.pendingBatches),
					test.wantPending,
				)
			}
		})
	}
}

func TestProcessBatchConcurrentAcceptedOutcomeWinsRejectionReservation(t *testing.T) {
	committedAt := validationTestNow.Add(time.Second)
	accepted := StoreResult{Duplicate: 2, OriginalEventCount: 2, CommittedAt: committedAt}
	store := &recoverableTestStore{rejectResult: &accepted}
	config := testServiceConfig()
	config.Limits.MaxBatchEvents = 1
	config = withTestSessionManager(config, staticTestAuthorizer())
	service, err := NewService(config, staticTestAuthorizer(), store)
	if err != nil {
		t.Fatal(err)
	}
	state := testBatchStreamState(service)
	batch := validTestBatch(
		"collector-a",
		"batch-concurrent-accepted",
		1,
		validTestEvent("event-one", "main"),
		validTestEvent("event-two", "main"),
	)
	response, err := service.processBatch(context.Background(), batch, state, validationTestNow)
	if err != nil {
		t.Fatal(err)
	}
	ack := response.GetBatchAck()
	if ack == nil || ack.GetDuplicateEventCount() != 2 ||
		!ack.GetCommittedAt().AsTime().Equal(committedAt) {
		t.Fatalf("response = %#v, want concurrent accepted acknowledgment", response)
	}
	if store.rejectCalls != 1 || len(state.pendingBatches) != 0 {
		t.Fatalf("reject calls = %d, pending = %d, want 1/0", store.rejectCalls, len(state.pendingBatches))
	}
}

func TestProcessBatchAllowsEarlierPendingRetryAfterLaterSuccess(t *testing.T) {
	cfg := testServiceConfig()
	cfg.MaxInFlightBatches = 2
	cfg = withTestSessionManager(cfg, staticTestAuthorizer())
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
	state := testBatchStreamState(service)
	first := validTestBatch("collector-a", "batch-one", 1, validTestEvent("event-one", "main"))
	second := validTestBatch("collector-a", "batch-two", 2, validTestEvent("event-two", "main"))

	response, err := service.processBatch(context.Background(), first, state, service.config.Clock().UTC())
	if err != nil || response.GetRetryBatch() == nil {
		t.Fatalf("first response = (%#v, %v), want RetryBatch", response, err)
	}
	response, err = service.processBatch(context.Background(), second, state, service.config.Clock().UTC())
	if err != nil || response.GetBatchAck() == nil {
		t.Fatalf("second response = (%#v, %v), want BatchAck", response, err)
	}
	response, err = service.processBatch(context.Background(), first, state, service.config.Clock().UTC())
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
	cfg = withTestSessionManager(cfg, staticTestAuthorizer())
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
	state := testBatchStreamState(service)
	first := validTestBatch("collector-a", "batch-one", 1, validTestEvent("event-one", "main"))
	second := validTestBatch("collector-a", "batch-two", 2, validTestEvent("event-two", "main"))

	response, err := service.processBatch(context.Background(), first, state, service.config.Clock().UTC())
	if err != nil || response.GetRetryBatch() == nil {
		t.Fatalf("first response = (%#v, %v), want RetryBatch", response, err)
	}
	response, err = service.processBatch(context.Background(), second, state, service.config.Clock().UTC())
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
	response, err = service.processBatch(context.Background(), first, state, service.config.Clock().UTC())
	if err != nil || response.GetBatchAck() == nil {
		t.Fatalf("first retry response = (%#v, %v), want BatchAck", response, err)
	}
	response, err = service.processBatch(context.Background(), second, state, service.config.Clock().UTC())
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
			config := withTestSessionManager(testServiceConfig(), staticTestAuthorizer())
			service, err := NewService(config, staticTestAuthorizer(), store)
			if err != nil {
				t.Fatal(err)
			}
			state := testBatchStreamState(service)
			batch := validTestBatch("collector-a", "batch-conflict", 1, validTestEvent("event-one", "main"))
			response, err := service.processBatch(
				context.Background(), batch, state, service.config.Clock().UTC(),
			)
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
	// Keep each redacted event below the 1 MiB event ceiling while expanding the
	// complete batch past the 16 MiB durable-outbox ceiling. A large replacement
	// with fewer matches preserves that boundary without making race/coverage
	// instrumentation perform hundreds of thousands of replacement operations.
	config.Redaction.Replacement = strings.Repeat("r", 8_192)
	config = withTestSessionManager(config, staticTestAuthorizer())
	store := &recoverableTestStore{}
	service, err := NewService(config, staticTestAuthorizer(), store)
	if err != nil {
		t.Fatal(err)
	}
	events := make([]*opensplunkv1.LogEvent, 20)
	for i := range events {
		events[i] = validTestEvent(fmt.Sprintf("event-%d", i), "main")
		events[i].Raw = []byte(strings.Repeat("token=x ", 105))
	}
	batch := validTestBatch("collector-a", "expanded-redaction", 1, events...)
	response, err := service.processBatch(
		context.Background(), batch, testBatchStreamState(service), service.config.Clock().UTC(),
	)
	if err != nil {
		t.Fatal(err)
	}
	if rejection := response.GetBatchReject(); rejection == nil ||
		rejection.GetCode() != opensplunkv1.BatchRejectionCode_BATCH_REJECTION_CODE_BATCH_TOO_LARGE {
		t.Fatalf("response = %#v, want terminal BATCH_TOO_LARGE", response)
	}
	if store.storeCalls != 0 || store.rejectCalls != 1 {
		t.Fatalf("store calls = %d, reject calls = %d, want 0/1", store.storeCalls, store.rejectCalls)
	}
}

func TestProcessBatchTerminallyRejectsOversizedDurableOutcome(t *testing.T) {
	store := &recoverableTestStore{}
	config := withTestSessionManager(testServiceConfig(), staticTestAuthorizer())
	service, err := NewService(config, staticTestAuthorizer(), store)
	if err != nil {
		t.Fatal(err)
	}
	const rejectedEvents = 280
	events := make([]*opensplunkv1.LogEvent, 0, rejectedEvents+1)
	longName := strings.Repeat("n", int(HardMaxFieldNameBytes))
	for i := range rejectedEvents {
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
	response, err := service.processBatch(
		context.Background(), batch, testBatchStreamState(service), service.config.Clock().UTC(),
	)
	if err != nil {
		t.Fatal(err)
	}
	if rejection := response.GetBatchReject(); rejection == nil ||
		rejection.GetCode() != opensplunkv1.BatchRejectionCode_BATCH_REJECTION_CODE_BATCH_TOO_LARGE {
		t.Fatalf("response = %#v, want terminal BATCH_TOO_LARGE", response)
	}
	if store.storeCalls != 0 || store.rejectCalls != 1 {
		t.Fatalf("store calls = %d, reject calls = %d, want 0/1", store.storeCalls, store.rejectCalls)
	}
}

func TestCollectDoesNotAcknowledgePermanentStoreFailure(t *testing.T) {
	store := EventStoreFunc(func(context.Context, StoreBatch) (StoreResult, error) {
		return StoreResult{}, errors.New("corrupt storage contract")
	})
	harness := newServiceHarness(t, testServiceConfig(), staticTestAuthorizer(), store)
	stream := harness.stream(t, "Bearer good-token")
	sendHello(t, stream, 1)
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
			sendHello(t, stream, 1)
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
		store := &recoverableTestStore{}
		harness := newServiceHarness(t, cfg, staticTestAuthorizer(), store)
		stream := harness.stream(t, "Bearer good-token")
		sendHello(t, stream, 1)
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
		if store.rejectCalls != 1 {
			t.Fatalf("RejectBatch calls = %d, want 1", store.rejectCalls)
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
		store := &recoverableTestStore{}
		harness := newServiceHarness(t, cfg, staticTestAuthorizer(), store)
		stream := harness.stream(t, "Bearer good-token")
		sendHello(t, stream, 1)
		_ = recvResponse(t, stream)
		batch := validTestBatch("collector-a", "batch-bytes", 1, first, second)
		if err := stream.Send(batchRequest(2, batch)); err != nil {
			t.Fatal(err)
		}
		if got := recvResponse(t, stream).GetBatchReject().GetCode(); got != opensplunkv1.BatchRejectionCode_BATCH_REJECTION_CODE_BATCH_TOO_LARGE {
			t.Fatalf("batch rejection = %v", got)
		}
		if store.rejectCalls != 1 {
			t.Fatalf("RejectBatch calls = %d, want 1", store.rejectCalls)
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
	sendHello(t, stream, 1)
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
	sendHello(t, stream, 1)
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

func TestCollectRetriesStreamLocalBatchSequenceConflict(t *testing.T) {
	harness := newServiceHarness(t, testServiceConfig(), staticTestAuthorizer(), acceptingStore())
	stream := harness.stream(t, "Bearer good-token")
	sendHello(t, stream, 1)
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
	if retry := response.GetRetryBatch(); retry == nil ||
		retry.GetReason() != opensplunkv1.RetryBatchReason_RETRY_BATCH_REASON_SERVER_BUSY {
		t.Fatalf("response = %#v, want non-terminal SERVER_BUSY retry", response)
	}
}

func TestCollectClosesCleanlyAfterGoodbye(t *testing.T) {
	harness := newServiceHarness(t, testServiceConfig(), staticTestAuthorizer(), acceptingStore())
	stream := harness.stream(t, "Bearer good-token")
	sendHello(t, stream, 1)
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
	server *Service
}

func newServiceHarness(t *testing.T, cfg Config, authorizer Authorizer, store EventStore) *serviceHarness {
	t.Helper()
	if cfg.SessionManager == nil {
		manager := newTestCollectorSessionManager(authorizer)
		cfg.SessionManager = manager
		authorizer = manager.preliminaryAuthorizer()
	}
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
	return &serviceHarness{
		client: opensplunkv1.NewCollectorIngestServiceClient(conn),
		server: service,
	}
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

func withTestSessionManager(config Config, authorizer Authorizer) Config {
	config.SessionManager = newTestCollectorSessionManager(authorizer)
	return config
}

func staticTestAuthorizer() Authorizer {
	return AuthorizerFunc(func(_ context.Context, token string) (Authorization, error) {
		if token != "good-token" {
			return Authorization{}, ErrUnauthorized
		}
		return Authorization{
			SubjectID:         "token-1",
			TenantID:          "tenant-a",
			CollectorID:       "collector-a",
			AuthorizedIndexes: testIndexPolicies("main"),
		}, nil
	})
}

func acceptingStore() EventStore {
	return EventStoreFunc(func(_ context.Context, batch StoreBatch) (StoreResult, error) {
		return StoreResult{Accepted: uint32(len(batch.Events)), CommittedAt: validationTestNow}, nil
	})
}

type recoverableTestStore struct {
	first        StoreBatch
	rejection    StoreBatchRejection
	identity     StoreBatchIdentity
	result       StoreResult
	rejectResult *StoreResult
	rejectErr    error
	storedState  StoredBatchState
	storeCalls   int
	lookupCalls  int
	rejectCalls  int
}

type pendingAuthorityTestStore struct {
	result      StoreResult
	storeCalls  int
	lookupCalls int
	resumeCalls int
	rejectCalls int
}

type goneResumeTestStore struct {
	lookupStates  []StoredBatchState
	resumeErr     error
	storedBatches []StoreBatch
	storeCalls    int
	lookupCalls   int
	resumeCalls   int
	rejectCalls   int
}

type deferredAuthorityTestStore struct {
	lookupState StoredBatchState
	lookupErr   error
	resumeErr   error
	storeCalls  int
	lookupCalls int
	resumeCalls int
	rejectCalls int
}

func (store *deferredAuthorityTestStore) Store(context.Context, StoreBatch) (StoreResult, error) {
	store.storeCalls++
	return StoreResult{}, errors.New("fresh Store must not run with deferred index authority")
}

func (store *deferredAuthorityTestStore) LookupBatch(
	context.Context,
	StoreBatchIdentity,
) (StoredBatchState, StoreResult, error) {
	store.lookupCalls++
	return store.lookupState, StoreResult{}, store.lookupErr
}

func (store *deferredAuthorityTestStore) ResumeBatch(
	context.Context,
	StoreBatchIdentity,
) (StoreResult, error) {
	store.resumeCalls++
	return StoreResult{}, store.resumeErr
}

func (store *deferredAuthorityTestStore) RejectBatch(
	context.Context,
	StoreBatchRejection,
) (StoreResult, error) {
	store.rejectCalls++
	return StoreResult{}, errors.New("fresh RejectBatch must not run with deferred index authority")
}

func (store *pendingAuthorityTestStore) Store(context.Context, StoreBatch) (StoreResult, error) {
	store.storeCalls++
	return StoreResult{}, errors.New("fresh Store must not run for a pending durable batch")
}

func (store *pendingAuthorityTestStore) LookupBatch(
	context.Context,
	StoreBatchIdentity,
) (StoredBatchState, StoreResult, error) {
	store.lookupCalls++
	return StoredBatchPending, StoreResult{}, nil
}

func (store *pendingAuthorityTestStore) ResumeBatch(
	context.Context,
	StoreBatchIdentity,
) (StoreResult, error) {
	store.resumeCalls++
	return store.result, nil
}

func (store *pendingAuthorityTestStore) RejectBatch(
	context.Context,
	StoreBatchRejection,
) (StoreResult, error) {
	store.rejectCalls++
	return StoreResult{}, errors.New("fresh RejectBatch must not run for a pending durable batch")
}

func (store *goneResumeTestStore) Store(_ context.Context, batch StoreBatch) (StoreResult, error) {
	store.storeCalls++
	store.storedBatches = append(store.storedBatches, batch)
	return StoreResult{
		Accepted:           uint32(len(batch.Events)),
		CommittedAt:        validationTestNow,
		OriginalEventCount: batch.OriginalEventCount,
		RejectedEvents:     batch.RejectedEvents,
	}, nil
}

func (store *goneResumeTestStore) LookupBatch(
	context.Context,
	StoreBatchIdentity,
) (StoredBatchState, StoreResult, error) {
	state := StoredBatchPending
	if store.lookupCalls < len(store.lookupStates) {
		state = store.lookupStates[store.lookupCalls]
	}
	store.lookupCalls++
	return state, StoreResult{}, nil
}

func (store *goneResumeTestStore) ResumeBatch(
	context.Context,
	StoreBatchIdentity,
) (StoreResult, error) {
	store.resumeCalls++
	return StoreResult{}, store.resumeErr
}

func (store *goneResumeTestStore) RejectBatch(
	context.Context,
	StoreBatchRejection,
) (StoreResult, error) {
	store.rejectCalls++
	return StoreResult{}, errors.New("fresh rejection must not run after a gone accepted reservation")
}

func storedBatchGoneTestError() error {
	return &StoredBatchGoneError{
		Err: errors.New("pending disposition was abandoned before resume"),
	}
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

func (*durableIdentityConflictTestStore) RejectBatch(context.Context, StoreBatchRejection) (StoreResult, error) {
	return StoreResult{}, &DurableIdentityConflictError{Err: errors.New("identity reused")}
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
	store.storedState = StoredBatchCommitted
	return store.result, nil
}

func (store *recoverableTestStore) LookupBatch(_ context.Context, identity StoreBatchIdentity) (StoredBatchState, StoreResult, error) {
	store.lookupCalls++
	if store.storedState == StoredBatchNotFound || identity != store.identity {
		return StoredBatchNotFound, StoreResult{}, nil
	}
	result := store.result
	if store.storedState == StoredBatchCommitted {
		result.Duplicate = result.Accepted
		result.Accepted = 0
	}
	return store.storedState, result, nil
}

func (*recoverableTestStore) ResumeBatch(context.Context, StoreBatchIdentity) (StoreResult, error) {
	return StoreResult{}, errors.New("unexpected pending resume")
}

func (store *recoverableTestStore) RejectBatch(
	_ context.Context,
	rejection StoreBatchRejection,
) (StoreResult, error) {
	store.rejectCalls++
	store.rejection = rejection
	if store.rejectErr != nil {
		return StoreResult{}, store.rejectErr
	}
	store.identity = rejection.Identity
	if store.rejectResult != nil {
		store.result = *store.rejectResult
	} else {
		store.result = StoreResult{BatchRejection: rejection.Rejection}
	}
	if store.result.BatchRejection != nil {
		store.storedState = StoredBatchRejected
	} else {
		store.storedState = StoredBatchCommitted
	}
	return store.result, nil
}

func sendHello(t *testing.T, stream opensplunkv1.CollectorIngestService_CollectClient, major uint32) {
	t.Helper()
	if err := stream.Send(helloRequest(1, major, 0)); err != nil {
		t.Fatal(err)
	}
}

func helloRequest(sequence uint64, major, minor uint32) *opensplunkv1.CollectRequest {
	return helloRequestFor(sequence, "collector-a", major, minor)
}

func helloRequestFor(sequence uint64, collectorID string, major, minor uint32) *opensplunkv1.CollectRequest {
	return &opensplunkv1.CollectRequest{
		StreamSequence: sequence,
		SentAt:         timestamppb.New(validationTestNow),
		Payload: &opensplunkv1.CollectRequest_Hello{Hello: &opensplunkv1.CollectorHello{
			CollectorId:      collectorID,
			InstanceId:       "instance-a",
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

func testBatchStreamState(service *Service) *streamState {
	resolved, _ := service.resolveAuthorizedIndexPolicies(
		testIndexPolicies("main"),
		service.config.Clock().UTC(),
	)
	return &streamState{
		collectorID:   "collector-a",
		protocolMajor: 1,
		protocolMinor: 0,
		authorization: Authorization{
			TenantID:          "tenant-a",
			AuthorizedIndexes: resolved.policies,
		},
		indexPolicies: resolved.byName,
	}
}

func testIndexPolicies(names ...string) []IndexPolicy {
	policies := make([]IndexPolicy, len(names))
	for index, name := range names {
		policies[index] = IndexPolicy{Name: name, Version: 1}
	}
	return policies
}
