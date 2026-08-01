package ingest

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestCollectRejectsUnboundOrMalformedTrustedIdentityBeforeHello(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		authorization Authorization
		wantCode      codes.Code
		wantMessage   string
	}{
		{
			name: "unbound credential",
			authorization: Authorization{
				SubjectID: "token-private-id",
				TenantID:  "tenant-private-id",
			},
			wantCode:    codes.Unauthenticated,
			wantMessage: "collector authentication failed",
		},
		{
			name: "missing tenant",
			authorization: Authorization{
				SubjectID:   "token-private-id",
				CollectorID: "collector-private-id",
			},
			wantCode:    codes.Unavailable,
			wantMessage: "collector authentication service is unavailable",
		},
		{
			name: "missing subject",
			authorization: Authorization{
				TenantID:    "tenant-private-id",
				CollectorID: "collector-private-id",
			},
			wantCode:    codes.Unavailable,
			wantMessage: "collector authentication service is unavailable",
		},
		{
			name: "malformed trusted binding",
			authorization: Authorization{
				SubjectID:   "token-private-id",
				TenantID:    "tenant-private-id",
				CollectorID: "private collector id",
			},
			wantCode:    codes.Unavailable,
			wantMessage: "collector authentication service is unavailable",
		},
		{
			name: "oversized tenant",
			authorization: Authorization{
				SubjectID:   "token-private-id",
				TenantID:    strings.Repeat("t", maximumTrustedAuthorizationIdentityBytes+1),
				CollectorID: "collector-a",
			},
			wantCode:    codes.Unavailable,
			wantMessage: "collector authentication service is unavailable",
		},
		{
			name: "NUL subject",
			authorization: Authorization{
				SubjectID:   "token\x00private-id",
				TenantID:    "tenant-a",
				CollectorID: "collector-a",
			},
			wantCode:    codes.Unavailable,
			wantMessage: "collector authentication service is unavailable",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			authorizer := AuthorizerFunc(func(context.Context, string) (Authorization, error) {
				return test.authorization, nil
			})
			harness := newServiceHarness(t, testServiceConfig(), authorizer, acceptingStore())
			stream := harness.stream(t, "Bearer private-token")
			response, err := stream.Recv()
			if response != nil || status.Code(err) != test.wantCode {
				t.Fatalf("Recv() = (%#v, %v), want nil/%v", response, err, test.wantCode)
			}
			if got := status.Convert(err).Message(); got != test.wantMessage {
				t.Fatalf("status message = %q, want %q", got, test.wantMessage)
			}
			for _, private := range []string{"private-token", "token-private-id", "tenant-private-id", "collector-private-id"} {
				if strings.Contains(err.Error(), private) {
					t.Fatalf("status leaked trusted identity %q: %v", private, err)
				}
			}
		})
	}
}

func TestCollectAcceptsDurableTrustedTenantIdentityBeyondProtocolIDLimit(t *testing.T) {
	t.Parallel()

	tenantID := "tenant/" + strings.Repeat("t", 192)
	subjectID := "token/" + strings.Repeat("s", 192)
	if len(tenantID) <= int(HardMaxIDBytes) || len(tenantID) > maximumTrustedAuthorizationIdentityBytes {
		t.Fatalf("tenant regression fixture has %d bytes", len(tenantID))
	}
	authorization := boundTestAuthorization(subjectID, tenantID, "collector-a")
	harness := newServiceHarness(
		t,
		testServiceConfig(),
		mappedCollectorAuthorizer(map[string]Authorization{"good-token": authorization}),
		acceptingStore(),
	)
	stream := harness.stream(t, "Bearer good-token")
	sendHello(t, stream, 1, 1, 0)
	if ready := recvResponse(t, stream).GetReady(); ready == nil {
		t.Fatal("response was not Ready")
	}
	sendGoodbye(t, stream, 2)
	if _, err := stream.Recv(); !errors.Is(err, io.EOF) {
		t.Fatalf("goodbye error = %v, want EOF", err)
	}
}

func TestCollectLastValidHelloSupersedesIdleStreamAcrossTokens(t *testing.T) {
	t.Parallel()

	authorizer := mappedCollectorAuthorizer(map[string]Authorization{
		"first-token":  boundTestAuthorization("subject-a", "tenant-a", "collector-a"),
		"second-token": boundTestAuthorization("subject-b", "tenant-a", "collector-a"),
	})
	config := testServiceConfigWithUniqueStreamIDs()
	harness := newServiceHarness(t, config, authorizer, acceptingStore())

	first := harness.stream(t, "Bearer first-token")
	sendHello(t, first, 1, 1, 0)
	firstReady := recvResponse(t, first).GetReady()
	if firstReady == nil {
		t.Fatal("first response was not Ready")
	}

	second := harness.stream(t, "Bearer second-token")
	sendHello(t, second, 1, 1, 0)
	secondReady := recvResponse(t, second).GetReady()
	if secondReady == nil {
		t.Fatal("second response was not Ready")
	}
	if firstReady.GetStreamId() == secondReady.GetStreamId() {
		t.Fatalf("successive stream IDs are equal: %q", firstReady.GetStreamId())
	}

	firstResult := make(chan error, 1)
	go func() {
		_, err := first.Recv()
		firstResult <- err
	}()
	select {
	case err := <-firstResult:
		if status.Code(err) != codes.Aborted ||
			status.Convert(err).Message() != "collector stream was superseded" {
			t.Fatalf("superseded stream error = %v, want sanitized Aborted", err)
		}
	case <-time.After(time.Second):
		t.Fatal("idle superseded handler was not woken promptly")
	}

	sendGoodbye(t, second, 2)
	if _, err := second.Recv(); !errors.Is(err, io.EOF) {
		t.Fatalf("current stream goodbye error = %v, want EOF", err)
	}
}

func TestCollectSameTokenTakeoverBypassesSteadyStateLimit(t *testing.T) {
	t.Parallel()

	config := testServiceConfigWithUniqueStreamIDs()
	config.MaxStreamsPerSubject = 1
	harness := newServiceHarness(t, config, staticTestAuthorizer(), acceptingStore())

	first := harness.stream(t, "Bearer good-token")
	sendHello(t, first, 1, 1, 0)
	_ = recvResponse(t, first)

	second := harness.stream(t, "Bearer good-token")
	sendHello(t, second, 1, 1, 0)
	if ready := recvResponse(t, second).GetReady(); ready == nil {
		t.Fatal("replacement response was not Ready")
	}
	if _, err := first.Recv(); status.Code(err) != codes.Aborted {
		t.Fatalf("superseded stream error = %v, want Aborted", err)
	}

	sendGoodbye(t, second, 2)
	if _, err := second.Recv(); !errors.Is(err, io.EOF) {
		t.Fatalf("replacement goodbye error = %v, want EOF", err)
	}
}

func TestCollectNewestPreHelloAttemptSupersedesOlderSameIdentity(t *testing.T) {
	t.Parallel()

	config := testServiceConfigWithUniqueStreamIDs()
	config.MaxStreamsPerSubject = 1
	harness := newServiceHarness(t, config, staticTestAuthorizer(), acceptingStore())

	first := harness.stream(t, "Bearer good-token")
	firstResult := make(chan error, 1)
	go func() {
		_, err := first.Recv()
		firstResult <- err
	}()
	waitForStreamAdmission(
		t,
		harness.server,
		authorizationSubjectKey("token-1"),
		CollectorStreamKey{TenantID: "tenant-a", CollectorID: "collector-a"},
	)

	second := harness.stream(t, "Bearer good-token")
	sendHello(t, second, 1, 1, 0)
	if ready := recvResponse(t, second).GetReady(); ready == nil {
		t.Fatal("newest pre-Hello attempt response was not Ready")
	}
	select {
	case err := <-firstResult:
		if status.Code(err) != codes.Aborted {
			t.Fatalf("older pre-Hello attempt error = %v, want Aborted", err)
		}
	case <-time.After(time.Second):
		t.Fatal("newest pre-Hello attempt did not wake its predecessor")
	}

	sendGoodbye(t, second, 2)
	if _, err := second.Recv(); !errors.Is(err, io.EOF) {
		t.Fatalf("newest attempt goodbye error = %v, want EOF", err)
	}
}

func TestCollectPreHelloTakeoverCancelsTokenUseRecording(t *testing.T) {
	t.Parallel()

	recordingStarted := make(chan struct{})
	var recorderCalls atomic.Uint32
	config := testServiceConfigWithUniqueStreamIDs()
	config.MaxStreamsPerSubject = 1
	manager := newTestCollectorSessionManager(staticTestAuthorizer())
	manager.admitFunc = func(
		ctx context.Context,
		_ string,
		request CollectorSessionAdmissionRequest,
	) (CollectorSessionAdmission, error) {
		if recorderCalls.Add(1) == 1 {
			close(recordingStarted)
			<-ctx.Done()
			return CollectorSessionAdmission{}, ctx.Err()
		}
		return manager.admissionFor(
			boundTestAuthorization("token-1", "tenant-a", "collector-a"),
			request,
		), nil
	}
	config.SessionManager = manager
	harness := newServiceHarness(t, config, staticTestAuthorizer(), acceptingStore())

	first := harness.stream(t, "Bearer good-token")
	sendHello(t, first, 1, 1, 0)
	select {
	case <-recordingStarted:
	case <-time.After(time.Second):
		t.Fatal("first attempt did not enter token-use recording")
	}

	second := harness.stream(t, "Bearer good-token")
	sendHello(t, second, 1, 1, 0)
	if ready := recvResponse(t, second).GetReady(); ready == nil {
		t.Fatal("replacement response was not Ready")
	}
	if _, err := first.Recv(); status.Code(err) != codes.Aborted {
		t.Fatalf("superseded admission error = %v, want Aborted", err)
	}
	if got := recorderCalls.Load(); got != 2 {
		t.Fatalf("token-use recorder calls = %d, want 2", got)
	}

	sendGoodbye(t, second, 2)
	if _, err := second.Recv(); !errors.Is(err, io.EOF) {
		t.Fatalf("replacement goodbye error = %v, want EOF", err)
	}
}

func TestCollectInvalidHelloDoesNotSupersedeCurrentLease(t *testing.T) {
	t.Parallel()

	authorizer := mappedCollectorAuthorizer(map[string]Authorization{
		"first-token":  boundTestAuthorization("subject-a", "tenant-a", "collector-a"),
		"second-token": boundTestAuthorization("subject-b", "tenant-a", "collector-a"),
	})
	harness := newServiceHarness(
		t,
		testServiceConfigWithUniqueStreamIDs(),
		authorizer,
		acceptingStore(),
	)

	first := harness.stream(t, "Bearer first-token")
	sendHello(t, first, 1, 1, 0)
	_ = recvResponse(t, first)

	invalid := harness.stream(t, "Bearer second-token")
	sendHello(t, invalid, 1, 2, 0)
	if _, err := invalid.Recv(); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("invalid successor error = %v, want FailedPrecondition", err)
	}

	sendGoodbye(t, first, 2)
	if _, err := first.Recv(); !errors.Is(err, io.EOF) {
		t.Fatalf("original stream was not retained after invalid Hello: %v", err)
	}
}

func TestCollectDoesNotFenceDifferentTrustedCollectorKeys(t *testing.T) {
	t.Parallel()

	authorizations := map[string]Authorization{
		"tenant-a-collector-a": boundTestAuthorization("subject-a", "tenant-a", "collector-a"),
		"tenant-a-collector-b": boundTestAuthorization("subject-b", "tenant-a", "collector-b"),
		"tenant-b-collector-a": boundTestAuthorization("subject-c", "tenant-b", "collector-a"),
	}
	harness := newServiceHarness(
		t,
		testServiceConfigWithUniqueStreamIDs(),
		mappedCollectorAuthorizer(authorizations),
		acceptingStore(),
	)
	type activeStream struct {
		token  string
		stream opensplunkv1.CollectorIngestService_CollectClient
	}
	streams := make([]activeStream, 0, len(authorizations))
	for token, authorization := range authorizations {
		stream := harness.stream(t, "Bearer "+token)
		if err := stream.Send(helloRequestFor(1, authorization.CollectorID, "instance-a", 1, 0)); err != nil {
			t.Fatal(err)
		}
		if ready := recvResponse(t, stream).GetReady(); ready == nil {
			t.Fatalf("%s response was not Ready", token)
		}
		streams = append(streams, activeStream{token: token, stream: stream})
	}
	for _, active := range streams {
		sendGoodbye(t, active.stream, 2)
		if _, err := active.stream.Recv(); !errors.Is(err, io.EOF) {
			t.Fatalf("%s goodbye error = %v, want EOF", active.token, err)
		}
	}
}

func TestCollectRejectsPostReadyAuthorizationScopeChanges(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name   string
		mutate func(*Authorization)
	}{
		{
			name: "subject",
			mutate: func(authorization *Authorization) {
				authorization.SubjectID = "subject-private-replacement"
			},
		},
		{
			name: "tenant",
			mutate: func(authorization *Authorization) {
				authorization.TenantID = "tenant-private-replacement"
			},
		},
		{
			name: "collector binding",
			mutate: func(authorization *Authorization) {
				authorization.CollectorID = "collector-private-replacement"
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			var calls atomic.Uint32
			authorizer := AuthorizerFunc(func(context.Context, string) (Authorization, error) {
				authorization := boundTestAuthorization("subject-a", "tenant-a", "collector-a")
				if calls.Add(1) > 1 {
					test.mutate(&authorization)
				}
				return authorization, nil
			})
			harness := newServiceHarness(t, testServiceConfig(), authorizer, acceptingStore())
			stream := harness.stream(t, "Bearer private-token")
			sendHello(t, stream, 1, 1, 0)
			_ = recvResponse(t, stream)
			if err := stream.Send(&opensplunkv1.CollectRequest{
				StreamSequence: 2,
				SentAt:         timestamppb.New(validationTestNow),
				Payload: &opensplunkv1.CollectRequest_Heartbeat{Heartbeat: &opensplunkv1.CollectorHeartbeat{
					CollectorId: "collector-a",
					InstanceId:  "instance-a",
					ObservedAt:  timestamppb.New(validationTestNow),
				}},
			}); err != nil {
				t.Fatal(err)
			}
			response, err := stream.Recv()
			if response != nil || status.Code(err) != codes.PermissionDenied {
				t.Fatalf("Recv() = (%#v, %v), want nil/PermissionDenied", response, err)
			}
			for _, private := range []string{
				"subject-private-replacement",
				"tenant-private-replacement",
				"collector-private-replacement",
			} {
				if strings.Contains(err.Error(), private) {
					t.Fatalf("status leaked replacement scope %q: %v", private, err)
				}
			}
		})
	}
}

func TestCollectReauthorizesHeartbeat(t *testing.T) {
	t.Parallel()

	var calls atomic.Uint32
	authorizer := AuthorizerFunc(func(context.Context, string) (Authorization, error) {
		if calls.Add(1) > 1 {
			return Authorization{}, fmt.Errorf("private revocation detail: %w", ErrUnauthorized)
		}
		return boundTestAuthorization("subject-a", "tenant-a", "collector-a"), nil
	})
	harness := newServiceHarness(t, testServiceConfig(), authorizer, acceptingStore())
	stream := harness.stream(t, "Bearer private-token")
	sendHello(t, stream, 1, 1, 0)
	_ = recvResponse(t, stream)
	if err := stream.Send(&opensplunkv1.CollectRequest{
		StreamSequence: 2,
		SentAt:         timestamppb.New(validationTestNow),
		Payload: &opensplunkv1.CollectRequest_Heartbeat{Heartbeat: &opensplunkv1.CollectorHeartbeat{
			CollectorId: "collector-a",
			InstanceId:  "instance-a",
			ObservedAt:  timestamppb.New(validationTestNow),
		}},
	}); err != nil {
		t.Fatal(err)
	}
	response, err := stream.Recv()
	if response != nil || status.Code(err) != codes.Unauthenticated {
		t.Fatalf("Recv() = (%#v, %v), want nil/Unauthenticated", response, err)
	}
	if strings.Contains(err.Error(), "private") || strings.Contains(err.Error(), "revocation") {
		t.Fatalf("status leaked authorization detail: %v", err)
	}
	if got := calls.Load(); got != 2 {
		t.Fatalf("authorization calls = %d, want 2", got)
	}
}

func TestCollectGoodbyeChecksLeaseWithoutReauthorization(t *testing.T) {
	t.Parallel()

	var calls atomic.Uint32
	authorizer := AuthorizerFunc(func(context.Context, string) (Authorization, error) {
		if calls.Add(1) > 1 {
			return Authorization{}, errors.New("authentication backend should not be called for goodbye")
		}
		return boundTestAuthorization("subject-a", "tenant-a", "collector-a"), nil
	})
	harness := newServiceHarness(t, testServiceConfig(), authorizer, acceptingStore())
	stream := harness.stream(t, "Bearer private-token")
	sendHello(t, stream, 1, 1, 0)
	_ = recvResponse(t, stream)
	sendGoodbye(t, stream, 2)
	if _, err := stream.Recv(); !errors.Is(err, io.EOF) {
		t.Fatalf("goodbye error = %v, want EOF", err)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("authorization calls = %d, want initial admission only", got)
	}
}

func TestCollectValidatesEnvelopeBeforeReauthorization(t *testing.T) {
	t.Parallel()

	var calls atomic.Uint32
	authorizer := AuthorizerFunc(func(context.Context, string) (Authorization, error) {
		calls.Add(1)
		return boundTestAuthorization("subject-a", "tenant-a", "collector-a"), nil
	})
	harness := newServiceHarness(t, testServiceConfig(), authorizer, acceptingStore())
	stream := harness.stream(t, "Bearer private-token")
	sendHello(t, stream, 1, 1, 0)
	_ = recvResponse(t, stream)

	if err := stream.Send(&opensplunkv1.CollectRequest{
		StreamSequence: 3,
		SentAt:         timestamppb.New(validationTestNow),
		Payload: &opensplunkv1.CollectRequest_Heartbeat{Heartbeat: &opensplunkv1.CollectorHeartbeat{
			CollectorId: "collector-a",
			InstanceId:  "instance-a",
			ObservedAt:  timestamppb.New(validationTestNow),
		}},
	}); err != nil {
		t.Fatal(err)
	}
	response, err := stream.Recv()
	if response != nil || status.Code(err) != codes.InvalidArgument {
		t.Fatalf("Recv() = (%#v, %v), want nil/InvalidArgument", response, err)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("authorization calls = %d, want only initial admission", got)
	}
}

func TestCollectTakeoverCancelsBlockedAuthorizationRefresh(t *testing.T) {
	t.Parallel()

	refreshStarted := make(chan struct{})
	var firstCalls atomic.Uint32
	authorizer := AuthorizerFunc(func(ctx context.Context, token string) (Authorization, error) {
		switch token {
		case "first-token":
			if firstCalls.Add(1) > 1 {
				close(refreshStarted)
				<-ctx.Done()
				return Authorization{}, ctx.Err()
			}
			return boundTestAuthorization("subject-a", "tenant-a", "collector-a"), nil
		case "second-token":
			return boundTestAuthorization("subject-b", "tenant-a", "collector-a"), nil
		default:
			return Authorization{}, ErrUnauthorized
		}
	})
	var storeCalls atomic.Uint32
	store := EventStoreFunc(func(context.Context, StoreBatch) (StoreResult, error) {
		storeCalls.Add(1)
		return StoreResult{Accepted: 1, CommittedAt: validationTestNow}, nil
	})
	harness := newServiceHarness(t, testServiceConfigWithUniqueStreamIDs(), authorizer, store)

	first := harness.stream(t, "Bearer first-token")
	sendHello(t, first, 1, 1, 0)
	_ = recvResponse(t, first)
	batch := validTestBatch("collector-a", "blocked-refresh-batch", 1, validTestEvent("event-a", "main"))
	if err := first.Send(batchRequest(2, batch)); err != nil {
		t.Fatal(err)
	}
	select {
	case <-refreshStarted:
	case <-time.After(time.Second):
		t.Fatal("first stream did not enter request reauthorization")
	}

	second := harness.stream(t, "Bearer second-token")
	sendHello(t, second, 1, 1, 0)
	_ = recvResponse(t, second)

	firstResult := make(chan error, 1)
	go func() {
		_, err := first.Recv()
		firstResult <- err
	}()
	select {
	case err := <-firstResult:
		if status.Code(err) != codes.Aborted {
			t.Fatalf("superseded stream error = %v, want Aborted", err)
		}
	case <-time.After(time.Second):
		t.Fatal("takeover did not cancel context-aware reauthorization")
	}
	if got := storeCalls.Load(); got != 0 {
		t.Fatalf("durable store calls = %d, want 0", got)
	}

	sendGoodbye(t, second, 2)
	if _, err := second.Recv(); !errors.Is(err, io.EOF) {
		t.Fatalf("successor goodbye error = %v, want EOF", err)
	}
}

func TestCollectStaleRequestDoesNotStartDurableBatchWork(t *testing.T) {
	t.Parallel()

	refreshStarted := make(chan struct{})
	allowRefresh := make(chan struct{})
	var firstCalls atomic.Uint32
	authorizer := AuthorizerFunc(func(_ context.Context, token string) (Authorization, error) {
		switch token {
		case "first-token":
			if firstCalls.Add(1) == 2 {
				close(refreshStarted)
				<-allowRefresh
			}
			return boundTestAuthorization("subject-a", "tenant-a", "collector-a"), nil
		case "second-token":
			return boundTestAuthorization("subject-b", "tenant-a", "collector-a"), nil
		default:
			return Authorization{}, ErrUnauthorized
		}
	})
	var storeCalls atomic.Uint32
	store := EventStoreFunc(func(context.Context, StoreBatch) (StoreResult, error) {
		storeCalls.Add(1)
		return StoreResult{Accepted: 1, CommittedAt: validationTestNow}, nil
	})
	harness := newServiceHarness(t, testServiceConfigWithUniqueStreamIDs(), authorizer, store)

	first := harness.stream(t, "Bearer first-token")
	sendHello(t, first, 1, 1, 0)
	_ = recvResponse(t, first)
	batch := validTestBatch("collector-a", "stale-batch", 1, validTestEvent("event-a", "main"))
	if err := first.Send(batchRequest(2, batch)); err != nil {
		t.Fatal(err)
	}
	select {
	case <-refreshStarted:
	case <-time.After(time.Second):
		t.Fatal("first stream did not enter request reauthorization")
	}

	second := harness.stream(t, "Bearer second-token")
	sendHello(t, second, 1, 1, 0)
	_ = recvResponse(t, second)
	close(allowRefresh)

	response, err := first.Recv()
	if response != nil || status.Code(err) != codes.Aborted {
		t.Fatalf("stale Recv() = (%#v, %v), want nil/Aborted", response, err)
	}
	if got := storeCalls.Load(); got != 0 {
		t.Fatalf("durable store calls = %d, want 0", got)
	}
	sendGoodbye(t, second, 2)
	if _, err := second.Recv(); !errors.Is(err, io.EOF) {
		t.Fatalf("successor goodbye error = %v, want EOF", err)
	}
}

func TestCollectBatchLinearizedBeforeTakeoverMayFinish(t *testing.T) {
	t.Parallel()

	storeStarted := make(chan struct{})
	allowStore := make(chan struct{})
	var storeCalls atomic.Uint32
	store := EventStoreFunc(func(context.Context, StoreBatch) (StoreResult, error) {
		storeCalls.Add(1)
		close(storeStarted)
		<-allowStore
		return StoreResult{Accepted: 1, CommittedAt: validationTestNow}, nil
	})
	authorizer := mappedCollectorAuthorizer(map[string]Authorization{
		"first-token":  boundTestAuthorization("subject-a", "tenant-a", "collector-a"),
		"second-token": boundTestAuthorization("subject-b", "tenant-a", "collector-a"),
	})
	harness := newServiceHarness(t, testServiceConfigWithUniqueStreamIDs(), authorizer, store)

	first := harness.stream(t, "Bearer first-token")
	sendHello(t, first, 1, 1, 0)
	_ = recvResponse(t, first)
	batch := validTestBatch("collector-a", "admitted-batch", 1, validTestEvent("event-a", "main"))
	if err := first.Send(batchRequest(2, batch)); err != nil {
		t.Fatal(err)
	}
	select {
	case <-storeStarted:
	case <-time.After(time.Second):
		t.Fatal("first batch did not cross the durable admission boundary")
	}

	second := harness.stream(t, "Bearer second-token")
	sendHello(t, second, 1, 1, 0)
	_ = recvResponse(t, second)
	close(allowStore)

	if ack := recvResponse(t, first).GetBatchAck(); ack == nil || ack.GetAcceptedEventCount() != 1 {
		t.Fatalf("admitted batch acknowledgment = %#v", ack)
	}
	if _, err := first.Recv(); status.Code(err) != codes.Aborted {
		t.Fatalf("old stream terminal error = %v, want Aborted", err)
	}
	if got := storeCalls.Load(); got != 1 {
		t.Fatalf("durable store calls = %d, want 1", got)
	}
	sendGoodbye(t, second, 2)
	if _, err := second.Recv(); !errors.Is(err, io.EOF) {
		t.Fatalf("successor goodbye error = %v, want EOF", err)
	}
}

func boundTestAuthorization(subjectID, tenantID, collectorID string) Authorization {
	return Authorization{
		SubjectID:         subjectID,
		TenantID:          tenantID,
		CollectorID:       collectorID,
		AuthorizedIndexes: testIndexPolicies("main"),
	}
}

func mappedCollectorAuthorizer(authorizations map[string]Authorization) Authorizer {
	return AuthorizerFunc(func(_ context.Context, token string) (Authorization, error) {
		authorization, ok := authorizations[token]
		if !ok {
			return Authorization{}, ErrUnauthorized
		}
		return authorization, nil
	})
}

func testServiceConfigWithUniqueStreamIDs() Config {
	config := testServiceConfig()
	var sequence atomic.Uint64
	config.NewStreamID = func() string {
		return fmt.Sprintf("stream-%d", sequence.Add(1))
	}
	return config
}

func sendGoodbye(
	t *testing.T,
	stream opensplunkv1.CollectorIngestService_CollectClient,
	sequence uint64,
) {
	t.Helper()
	if err := stream.Send(&opensplunkv1.CollectRequest{
		StreamSequence: sequence,
		SentAt:         timestamppb.New(validationTestNow),
		Payload: &opensplunkv1.CollectRequest_Goodbye{Goodbye: &opensplunkv1.CollectorGoodbye{
			Reason: opensplunkv1.CollectorGoodbyeReason_COLLECTOR_GOODBYE_REASON_SHUTDOWN,
		}},
	}); err != nil {
		t.Fatal(err)
	}
}

func waitForStreamAdmission(
	t *testing.T,
	service *Service,
	subjectKey string,
	collectorKey CollectorStreamKey,
) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for {
		service.admissionMu.Lock()
		_, admitted := service.admissions[subjectKey][collectorKey]
		service.admissionMu.Unlock()
		if admitted {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("collector stream admission was not registered")
		}
		time.Sleep(time.Millisecond)
	}
}
