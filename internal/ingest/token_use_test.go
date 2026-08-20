package ingest

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type tokenUseCall struct {
	tokenID    string
	acceptedAt time.Time
}

func TestCollectRecordsTokenUseOncePerValidStreamAdmission(t *testing.T) {
	t.Parallel()

	trace := make(chan string, 8)
	authorizer := AuthorizerFunc(func(_ context.Context, token string) (Authorization, error) {
		if token != "one-time-secret" {
			return Authorization{}, ErrUnauthorized
		}
		trace <- "authorize"
		return Authorization{
			SubjectID:         "token-safe-id",
			TenantID:          "tenant-a",
			CollectorID:       "collector-a",
			AuthorizedIndexes: testIndexPolicies("main"),
		}, nil
	})

	var (
		callsMu sync.Mutex
		calls   []tokenUseCall
	)
	config := testServiceConfig()
	manager := newTestCollectorSessionManager(authorizer)
	manager.admitFunc = func(
		_ context.Context,
		bearer string,
		request CollectorSessionAdmissionRequest,
	) (CollectorSessionAdmission, error) {
		if bearer != "one-time-secret" {
			return CollectorSessionAdmission{}, ErrUnauthorized
		}
		authorization := Authorization{
			SubjectID:         "token-safe-id",
			TenantID:          "tenant-a",
			CollectorID:       "collector-a",
			AuthorizedIndexes: testIndexPolicies("main"),
		}
		callsMu.Lock()
		calls = append(calls, tokenUseCall{
			tokenID:    authorization.SubjectID,
			acceptedAt: request.AcceptedAt,
		})
		callsMu.Unlock()
		trace <- "record"
		return manager.admissionFor(authorization, request), nil
	}
	config.SessionManager = manager

	harness := newServiceHarness(t, config, authorizer, acceptingStore())
	stream := harness.stream(t, "Bearer one-time-secret")
	sendHello(t, stream)
	response := recvResponse(t, stream)
	if response.GetReady() == nil {
		t.Fatalf("response payload = %T, want CollectorReady", response.GetPayload())
	}
	if got := []string{<-trace, <-trace}; !equalStrings(got, []string{"authorize", "record"}) {
		t.Fatalf("admission order = %v, want authorize then record before ready", got)
	}
	if got := response.GetSentAt().AsTime(); !got.Equal(validationTestNow) {
		t.Fatalf("ready sent_at = %v, want admission time %v", got, validationTestNow)
	}
	if got := response.GetReady().GetServerTime().AsTime(); !got.Equal(validationTestNow) {
		t.Fatalf("ready server_time = %v, want admission time %v", got, validationTestNow)
	}
	assertTokenUseCalls(t, &callsMu, &calls, []tokenUseCall{{
		tokenID:    "token-safe-id",
		acceptedAt: validationTestNow,
	}})

	if err := stream.Send(&opensplunk.CollectRequest{
		StreamSequence: 2,
		SentAt:         timestamppb.New(validationTestNow),
		Payload: &opensplunk.CollectRequest_Heartbeat{Heartbeat: &opensplunk.CollectorHeartbeat{
			CollectorId: "collector-a",
			InstanceId:  "instance-a",
			ObservedAt:  timestamppb.New(validationTestNow),
		}},
	}); err != nil {
		t.Fatal(err)
	}
	for _, batchCase := range []struct {
		sequence uint64
		batchID  string
	}{
		{sequence: 3, batchID: "batch-one"},
		{sequence: 4, batchID: "batch-two"},
	} {
		sequence := batchCase.sequence
		batchID := batchCase.batchID
		batchSequence := sequence - 2
		batch := validTestBatch(
			"collector-a",
			batchID,
			batchSequence,
			validTestEvent("event-"+batchID, "main"),
		)
		if err := stream.Send(batchRequest(sequence, batch)); err != nil {
			t.Fatal(err)
		}
		if got := recvResponse(t, stream).GetBatchAck(); got == nil {
			t.Fatalf("request %d response was not BatchAck", sequence)
		}
	}
	assertTokenUseCalls(t, &callsMu, &calls, []tokenUseCall{{
		tokenID:    "token-safe-id",
		acceptedAt: validationTestNow,
	}})

	if err := stream.Send(&opensplunk.CollectRequest{
		StreamSequence: 5,
		SentAt:         timestamppb.New(validationTestNow),
		Payload: &opensplunk.CollectRequest_Goodbye{Goodbye: &opensplunk.CollectorGoodbye{
			Reason: opensplunk.CollectorGoodbyeReason_COLLECTOR_GOODBYE_REASON_SHUTDOWN,
		}},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := stream.Recv(); !errors.Is(err, io.EOF) {
		t.Fatalf("Recv() after goodbye = %v, want EOF", err)
	}

	second := harness.stream(t, "Bearer one-time-secret")
	sendHello(t, second)
	if got := recvResponse(t, second).GetReady(); got == nil {
		t.Fatal("second stream response was not CollectorReady")
	}
	assertTokenUseCalls(t, &callsMu, &calls, []tokenUseCall{
		{tokenID: "token-safe-id", acceptedAt: validationTestNow},
		{tokenID: "token-safe-id", acceptedAt: validationTestNow},
	})
}

func TestCollectDoesNotRecordRejectedStreamAdmission(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		authorizer Authorizer
		configure  func(*Config)
		send       func(*testing.T, opensplunk.CollectorIngestService_CollectClient)
		wantCode   codes.Code
	}{
		{
			name: "invalid authentication",
			authorizer: AuthorizerFunc(func(context.Context, string) (Authorization, error) {
				return Authorization{}, ErrUnauthorized
			}),
			send:     func(*testing.T, opensplunk.CollectorIngestService_CollectClient) {},
			wantCode: codes.Unauthenticated,
		},
		{
			name:       "invalid hello",
			authorizer: staticTestAuthorizer(),
			send: func(t *testing.T, stream opensplunk.CollectorIngestService_CollectClient) {
				t.Helper()
				sendInvalidHello(t, stream)
			},
			wantCode: codes.InvalidArgument,
		},
		{
			name:       "invalid allocated stream ID",
			authorizer: staticTestAuthorizer(),
			configure: func(config *Config) {
				config.NewStreamID = func() string { return "" }
			},
			send: func(t *testing.T, stream opensplunk.CollectorIngestService_CollectClient) {
				t.Helper()
				sendHello(t, stream)
			},
			wantCode: codes.Internal,
		},
		{
			name: "missing safe token ID",
			authorizer: AuthorizerFunc(func(context.Context, string) (Authorization, error) {
				return Authorization{
					TenantID:          "tenant-a",
					CollectorID:       "collector-a",
					AuthorizedIndexes: testIndexPolicies("main"),
				}, nil
			}),
			// Trusted authorization identity validation precedes the first request,
			// so receive the terminal status without racing an unnecessary hello send.
			send:     func(*testing.T, opensplunk.CollectorIngestService_CollectClient) {},
			wantCode: codes.Unavailable,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var calls atomic.Uint32
			config := testServiceConfig()
			manager := newTestCollectorSessionManager(tt.authorizer)
			manager.admitFunc = func(
				context.Context,
				string,
				CollectorSessionAdmissionRequest,
			) (CollectorSessionAdmission, error) {
				calls.Add(1)
				return CollectorSessionAdmission{}, errors.New(
					"unexpected collector admission",
				)
			}
			config.SessionManager = manager
			if tt.configure != nil {
				tt.configure(&config)
			}
			harness := newServiceHarness(t, config, tt.authorizer, acceptingStore())
			stream := harness.stream(t, "Bearer good-token")
			tt.send(t, stream)
			if _, err := stream.Recv(); status.Code(err) != tt.wantCode {
				t.Fatalf("Recv() error = %v, want %v", err, tt.wantCode)
			}
			if got := calls.Load(); got != 0 {
				t.Fatalf("token-use recorder calls = %d, want 0", got)
			}
		})
	}
}

func TestCollectMapsSessionAdmissionFailuresBeforeReady(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		err      error
		wantCode codes.Code
		wantText string
	}{
		{
			name:     "inactive token",
			err:      fmt.Errorf("private revoked detail: %w", ErrUnauthorized),
			wantCode: codes.Unauthenticated,
			wantText: "collector authentication is no longer valid",
		},
		{
			name:     "canceled",
			err:      fmt.Errorf("private cancellation detail: %w", context.Canceled),
			wantCode: codes.Canceled,
			wantText: "context canceled",
		},
		{
			name:     "deadline",
			err:      fmt.Errorf("private deadline detail: %w", context.DeadlineExceeded),
			wantCode: codes.DeadlineExceeded,
			wantText: "context deadline exceeded",
		},
		{
			name:     "collector capacity",
			err:      fmt.Errorf("private capacity detail: %w", ErrCollectorSessionCapacity),
			wantCode: codes.ResourceExhausted,
			wantText: "collector capacity is exhausted",
		},
		{
			name:     "backend unavailable",
			err:      errors.New("private sqlite path and key"),
			wantCode: codes.Unavailable,
			wantText: "collector admission service is unavailable",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var calls atomic.Uint32
			config := testServiceConfig()
			manager := newTestCollectorSessionManager(staticTestAuthorizer())
			manager.admitFunc = func(
				context.Context,
				string,
				CollectorSessionAdmissionRequest,
			) (CollectorSessionAdmission, error) {
				calls.Add(1)
				return CollectorSessionAdmission{}, tt.err
			}
			config.SessionManager = manager
			harness := newServiceHarness(t, config, staticTestAuthorizer(), acceptingStore())
			stream := harness.stream(t, "Bearer good-token")
			sendHello(t, stream)
			response, err := stream.Recv()
			if response != nil || status.Code(err) != tt.wantCode {
				t.Fatalf("Recv() = (%#v, %v), want nil/%v", response, err, tt.wantCode)
			}
			if got := status.Convert(err).Message(); got != tt.wantText {
				t.Fatalf("status message = %q, want %q", got, tt.wantText)
			}
			if strings.Contains(err.Error(), "private") ||
				strings.Contains(err.Error(), "sqlite") ||
				strings.Contains(err.Error(), "key") {
				t.Fatalf("status leaked recorder detail: %v", err)
			}
			if got := calls.Load(); got != 1 {
				t.Fatalf("token-use recorder calls = %d, want 1", got)
			}
		})
	}
}

func assertTokenUseCalls(
	t *testing.T,
	mu *sync.Mutex,
	calls *[]tokenUseCall,
	want []tokenUseCall,
) {
	t.Helper()
	mu.Lock()
	defer mu.Unlock()
	if len(*calls) != len(want) {
		t.Fatalf("token-use calls = %#v, want %#v", *calls, want)
	}
	for index := range want {
		if (*calls)[index] != want[index] {
			t.Fatalf("token-use calls = %#v, want %#v", *calls, want)
		}
	}
}
