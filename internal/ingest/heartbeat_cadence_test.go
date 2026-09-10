package ingest

import (
	"context"
	"errors"
	"io"
	"math"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/collectorfleet"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestCollectHeartbeatCadenceSkipsEarlyWorkAndPreservesSequence(t *testing.T) {
	t.Parallel()
	for _, interval := range []time.Duration{15 * time.Second, time.Nanosecond} {
		t.Run(interval.String(), func(t *testing.T) {
			t.Parallel()
			minimum := interval - interval/2
			// A fixed wall clock cannot authorize cadence; only these monotonic
			// arrival times can. Repeated early arrivals must not slide the window.
			arrivals := []time.Duration{0, 0, minimum - 1, minimum - 1, minimum, 2*minimum + 1}
			config := testServiceConfig()
			config.HeartbeatInterval = interval
			config.MonotonicNow = heartbeatArrivalClock(t, arrivals)
			probe := newHeartbeatCadenceProbe(t, config)
			stream := probe.stream(t)
			for index := range arrivals {
				request := cadenceHeartbeatRequest(uint64(index + 2))
				if index > 0 && index < 4 {
					// This nested value fails snapshot conversion if it is reached.
					request.GetHeartbeat().Queue = &opensplunk.CollectorQueueStats{
						OldestEventAge: &durationpb.Duration{Seconds: math.MaxInt64},
					}
				}
				if err := stream.Send(request); err != nil {
					t.Fatal(err)
				}
			}
			if err := stream.Send(batchRequest(8, validTestBatch(
				"collector-a", "after-early-heartbeats", 1, validTestEvent("event-a", "main"),
			))); err != nil {
				t.Fatal(err)
			}
			if ack := recvResponse(t, stream).GetBatchAck(); ack == nil || ack.GetAcceptedEventCount() != 1 {
				t.Fatalf("batch acknowledgment = %#v", ack)
			}
			finishCadenceStream(t, stream, 9)
			if got := probe.authorizationCalls.Load(); got != 4 {
				t.Fatalf("authorization calls = %d, want three admitted heartbeats plus one batch", got)
			}
			probe.mu.Lock()
			defer probe.mu.Unlock()
			if len(probe.heartbeats) != 3 {
				t.Fatalf("recorded heartbeats = %d, want 3", len(probe.heartbeats))
			}
			for index, sequence := range []uint64{2, 6, 7} {
				if got := probe.heartbeats[index]; got.ObservationSequence != sequence || !got.ReceivedAt.Equal(validationTestNow) {
					t.Fatalf("heartbeat %d = %+v, want sequence %d and unchanged wall time", index, got, sequence)
				}
			}
		})
	}
}

func TestCollectHeartbeatCadenceStillRejectsInvalidEarlyFrames(t *testing.T) {
	t.Parallel()
	for name, mutate := range map[string]func(*opensplunk.CollectRequest){
		"sequence":    func(request *opensplunk.CollectRequest) { request.StreamSequence++ },
		"sent at":     func(request *opensplunk.CollectRequest) { request.SentAt = nil },
		"collector":   func(request *opensplunk.CollectRequest) { request.GetHeartbeat().CollectorId = "other" },
		"instance":    func(request *opensplunk.CollectRequest) { request.GetHeartbeat().InstanceId = "other" },
		"observed at": func(request *opensplunk.CollectRequest) { request.GetHeartbeat().ObservedAt = nil },
		"CPU":         func(request *opensplunk.CollectRequest) { request.GetHeartbeat().ProcessCpuPercent = math.NaN() },
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			config := testServiceConfig()
			config.MonotonicNow = func() time.Time { return validationTestNow }
			probe := newHeartbeatCadenceProbe(t, config)
			stream := probe.stream(t)
			if err := stream.Send(cadenceHeartbeatRequest(2)); err != nil {
				t.Fatal(err)
			}
			request := cadenceHeartbeatRequest(3)
			mutate(request)
			if err := stream.Send(request); err != nil {
				t.Fatal(err)
			}
			if response, err := stream.Recv(); response != nil || status.Code(err) != codes.InvalidArgument {
				t.Fatalf("invalid early frame = (%#v, %v), want InvalidArgument", response, err)
			}
			if got := probe.authorizationCalls.Load(); got != 1 {
				t.Fatalf("authorization calls = %d, want 1", got)
			}
		})
	}
}

func TestCollectHeartbeatCadenceIgnoresWallClockAdvancement(t *testing.T) {
	t.Parallel()
	var wallCalls atomic.Int64
	config := testServiceConfig()
	config.HeartbeatInterval = time.Nanosecond
	config.Clock = func() time.Time {
		return validationTestNow.Add(time.Duration(wallCalls.Add(1)) * time.Second)
	}
	config.MonotonicNow = func() time.Time { return validationTestNow }
	probe := newHeartbeatCadenceProbe(t, config)
	stream := probe.stream(t)
	for sequence := uint64(2); sequence <= 3; sequence++ {
		if err := stream.Send(cadenceHeartbeatRequest(sequence)); err != nil {
			t.Fatal(err)
		}
	}
	finishCadenceStream(t, stream, 4)
	if got := probe.authorizationCalls.Load(); got != 1 {
		t.Fatalf("authorization calls = %d, want only the first heartbeat despite advancing wall time", got)
	}
}

func TestCollectHeartbeatCadenceReauthorizesAcceptedBoundaries(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name string
		err  error
		code codes.Code
	}{
		{name: "revoked token", err: ErrUnauthorized, code: codes.Unauthenticated},
		{name: "disabled lease", err: ErrCollectorLeaseNotCurrent, code: codes.Aborted},
		{name: "removed index", err: ErrNoActiveIndexAuthority, code: codes.Unauthenticated},
		{name: "invalid index", err: ErrInvalidIndexAuthority, code: codes.Unavailable},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			config := testServiceConfig()
			config.MonotonicNow = heartbeatArrivalClock(t, []time.Duration{0, time.Second, config.HeartbeatInterval / 2})
			probe := newHeartbeatCadenceProbe(t, config)
			probe.authorizationError = test.err
			stream := probe.stream(t)
			for sequence := uint64(2); sequence <= 4; sequence++ {
				if err := stream.Send(cadenceHeartbeatRequest(sequence)); err != nil {
					t.Fatal(err)
				}
			}
			if response, err := stream.Recv(); response != nil || status.Code(err) != test.code {
				t.Fatalf("accepted heartbeat = (%#v, %v), want %v", response, err, test.code)
			}
			if got := probe.authorizationCalls.Load(); got != 2 {
				t.Fatalf("authorization calls = %d, want first and next eligible heartbeat", got)
			}
			probe.mu.Lock()
			defer probe.mu.Unlock()
			if len(probe.heartbeats) != 1 {
				t.Fatalf("recorded heartbeats = %d, want only the first", len(probe.heartbeats))
			}
		})
	}
}

func TestCollectHeartbeatCadenceDoesNotGateBatchAuthorization(t *testing.T) {
	t.Parallel()
	for _, repack := range []bool{false, true} {
		name := "batch"
		if repack {
			name = "repack"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			config := testServiceConfig()
			config.MonotonicNow = func() time.Time { return validationTestNow }
			probe := newHeartbeatCadenceProbe(t, config)
			probe.authorizationError = ErrUnauthorized
			stream := probe.stream(t)
			for sequence := uint64(2); sequence <= 3; sequence++ {
				if err := stream.Send(cadenceHeartbeatRequest(sequence)); err != nil {
					t.Fatal(err)
				}
			}
			request := batchRequest(4, validTestBatch("collector-a", "batch-a", 1, validTestEvent("event-a", "main")))
			if repack {
				request.Payload = &opensplunk.CollectRequest_RepackBatch{RepackBatch: request.GetBatch()}
			}
			if err := stream.Send(request); err != nil {
				t.Fatal(err)
			}
			if response, err := stream.Recv(); response != nil || status.Code(err) != codes.Unauthenticated {
				t.Fatalf("batch authorization = (%#v, %v), want Unauthenticated", response, err)
			}
			if got := probe.authorizationCalls.Load(); got != 2 {
				t.Fatalf("authorization calls = %d, want first heartbeat and batch", got)
			}
		})
	}
}

func TestCollectHeartbeatCadenceSurvivesTokenInstanceAndStreamReplacement(t *testing.T) {
	t.Parallel()
	authorizer := mappedCollectorAuthorizer(map[string]Authorization{
		"first-token":  boundTestAuthorization("subject-a", "tenant-a", "collector-a"),
		"second-token": boundTestAuthorization("subject-b", "tenant-a", "collector-a"),
	})
	manager := newTestCollectorSessionManager(authorizer)
	var authorizationCalls, heartbeatCalls atomic.Uint32
	manager.authorizeFunc = func(ctx context.Context, bearer string, _ collectorfleet.Lease, _ time.Time) (Authorization, error) {
		authorizationCalls.Add(1)
		return authorizer.Authorize(ctx, bearer)
	}
	manager.heartbeatFunc = func(context.Context, collectorfleet.Lease, collectorfleet.Heartbeat) (bool, error) {
		heartbeatCalls.Add(1)
		return true, nil
	}
	config := testServiceConfigWithUniqueStreamIDs()
	config.MaxStreamsPerSubject = 1
	config.SessionManager = manager
	config.MonotonicNow = heartbeatArrivalClock(t, []time.Duration{0, 0, 0, config.HeartbeatInterval})
	harness := newServiceHarness(t, config, manager.preliminaryAuthorizer(), acceptingStore())
	var previous opensplunk.CollectorIngestService_CollectClient
	for index, test := range []struct {
		bearer         string
		instance       string
		wantHeartbeats uint32
	}{
		{bearer: "first-token", instance: "instance-a", wantHeartbeats: 1},
		{bearer: "first-token", instance: "instance-a", wantHeartbeats: 1},
		{bearer: "second-token", instance: "instance-b", wantHeartbeats: 1},
		{bearer: "second-token", instance: "instance-c", wantHeartbeats: 2},
	} {
		stream := openCadenceStream(t, harness, test.bearer, test.instance)
		if previous != nil && index < 3 {
			if _, err := previous.Recv(); status.Code(err) != codes.Aborted {
				t.Fatalf("superseded stream error = %v, want Aborted", err)
			}
		}
		heartbeat := cadenceHeartbeatRequest(2)
		heartbeat.GetHeartbeat().InstanceId = test.instance
		if err := stream.Send(heartbeat); err != nil {
			t.Fatal(err)
		}
		if err := stream.Send(batchRequest(3, validTestBatch(
			"collector-a", "batch-after-replacement", 1, validTestEvent("event-a", "main"),
		))); err != nil {
			t.Fatal(err)
		}
		if ack := recvResponse(t, stream).GetBatchAck(); ack == nil || ack.GetAcceptedEventCount() != 1 {
			t.Fatalf("replacement batch acknowledgment = %#v", ack)
		}
		if got := heartbeatCalls.Load(); got != test.wantHeartbeats {
			t.Fatalf("stream %d recorded %d heartbeats, want %d", index, got, test.wantHeartbeats)
		}
		if got, want := authorizationCalls.Load(), uint32(index+1)+test.wantHeartbeats; got != want {
			t.Fatalf("stream %d authorization calls = %d, want %d", index, got, want)
		}
		if index >= 2 {
			finishCadenceStream(t, stream, 4)
		}
		previous = stream
	}
}

type heartbeatCadenceProbe struct {
	harness            *serviceHarness
	authorizationCalls atomic.Uint32
	// Set before opening the stream; returned after the first authorization.
	authorizationError error
	mu                 sync.Mutex
	heartbeats         []collectorfleet.Heartbeat
}

func newHeartbeatCadenceProbe(t *testing.T, config Config) *heartbeatCadenceProbe {
	t.Helper()
	probe := &heartbeatCadenceProbe{}
	authorization := boundTestAuthorization("subject-a", "tenant-a", "collector-a")
	authorizer := AuthorizerFunc(func(context.Context, string) (Authorization, error) {
		return authorization, nil
	})
	manager := newTestCollectorSessionManager(authorizer)
	manager.authorizeFunc = func(context.Context, string, collectorfleet.Lease, time.Time) (Authorization, error) {
		if probe.authorizationCalls.Add(1) > 1 && probe.authorizationError != nil {
			partial := cloneAuthorization(authorization)
			partial.AuthorizedIndexes = nil
			return partial, probe.authorizationError
		}
		return authorization, nil
	}
	manager.heartbeatFunc = func(_ context.Context, _ collectorfleet.Lease, heartbeat collectorfleet.Heartbeat) (bool, error) {
		probe.mu.Lock()
		defer probe.mu.Unlock()
		probe.heartbeats = append(probe.heartbeats, heartbeat)
		return true, nil
	}
	config.SessionManager = manager
	probe.harness = newServiceHarness(t, config, manager.preliminaryAuthorizer(), acceptingStore())
	return probe
}

func (probe *heartbeatCadenceProbe) stream(t *testing.T) opensplunk.CollectorIngestService_CollectClient {
	t.Helper()
	return openCadenceStream(t, probe.harness, "good-token", "instance-a")
}

func openCadenceStream(t *testing.T, harness *serviceHarness, bearer, instance string) opensplunk.CollectorIngestService_CollectClient {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)
	ctx = metadata.NewOutgoingContext(ctx, metadata.Pairs("authorization", "Bearer "+bearer))
	stream, err := harness.client.Collect(ctx)
	if err != nil {
		t.Fatal(err)
	}
	hello := helloRequest(1)
	hello.GetHello().InstanceId = instance
	if err := stream.Send(hello); err != nil {
		t.Fatal(err)
	}
	if ready := recvResponse(t, stream).GetReady(); ready == nil {
		t.Fatal("missing Ready")
	}
	return stream
}

func heartbeatArrivalClock(t *testing.T, offsets []time.Duration) func() time.Time {
	t.Helper()
	start := time.Now()
	var index atomic.Uint32
	return func() time.Time {
		position := int(index.Add(1)) - 1
		if position >= len(offsets) {
			t.Errorf("unexpected heartbeat clock call %d", position+1)
			return start
		}
		return start.Add(offsets[position])
	}
}

func cadenceHeartbeatRequest(sequence uint64) *opensplunk.CollectRequest {
	return &opensplunk.CollectRequest{
		StreamSequence: sequence,
		SentAt:         timestamppb.New(validationTestNow),
		Payload: &opensplunk.CollectRequest_Heartbeat{Heartbeat: &opensplunk.CollectorHeartbeat{
			CollectorId: "collector-a", InstanceId: "instance-a", ObservedAt: timestamppb.New(validationTestNow),
		}},
	}
}

func finishCadenceStream(t *testing.T, stream opensplunk.CollectorIngestService_CollectClient, sequence uint64) {
	t.Helper()
	if err := stream.Send(&opensplunk.CollectRequest{
		StreamSequence: sequence,
		SentAt:         timestamppb.New(validationTestNow),
		Payload: &opensplunk.CollectRequest_Goodbye{Goodbye: &opensplunk.CollectorGoodbye{
			Reason: opensplunk.CollectorGoodbyeReason_COLLECTOR_GOODBYE_REASON_SHUTDOWN,
		}},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := stream.Recv(); !errors.Is(err, io.EOF) {
		t.Fatalf("Goodbye error = %v, want EOF", err)
	}
}
