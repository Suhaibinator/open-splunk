package ingest

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/collectorfleet"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestNewServiceRequiresCollectorSessionManager(t *testing.T) {
	t.Parallel()
	config := testServiceConfig()
	if _, err := NewService(
		config,
		staticTestAuthorizer(),
		acceptingStore(),
	); err == nil || err.Error() != "ingest collector session manager is required" {
		t.Fatalf("NewService() error = %v", err)
	}
}

func TestNewServiceDefaultsAndValidatesSessionCleanupTimeout(t *testing.T) {
	t.Parallel()
	authorizer := staticTestAuthorizer()
	config := testServiceConfig()
	config.SessionManager = newTestCollectorSessionManager(authorizer)
	config.SessionCleanupTimeout = 0
	service, err := NewService(config, authorizer, acceptingStore())
	if err != nil {
		t.Fatal(err)
	}
	if service.config.SessionCleanupTimeout !=
		defaultCollectorSessionCleanupTimeout {
		t.Fatalf(
			"default session cleanup timeout = %v, want %v",
			service.config.SessionCleanupTimeout,
			defaultCollectorSessionCleanupTimeout,
		)
	}

	config.SessionCleanupTimeout = -time.Nanosecond
	if _, err := NewService(
		config,
		authorizer,
		acceptingStore(),
	); err == nil ||
		err.Error() != "collector session cleanup timeout must be positive" {
		t.Fatalf("negative session cleanup timeout error = %v", err)
	}
}

func TestCollectActivatesExactLeaseWhileProcessPromotionIsCurrentAndFinalized(t *testing.T) {
	t.Parallel()

	authorization := boundTestAuthorization(
		"subject-a",
		"tenant-a",
		"collector-a",
	)
	authorizer := AuthorizerFunc(func(context.Context, string) (Authorization, error) {
		return authorization, nil
	})
	manager := newTestCollectorSessionManager(authorizer)
	manager.admitFunc = func(
		_ context.Context,
		_ string,
		request CollectorSessionAdmissionRequest,
	) (CollectorSessionAdmission, error) {
		return manager.admissionFor(authorization, request), nil
	}
	registry := NewInMemoryCollectorStreamRegistry()
	type activationObservation struct {
		lease               collectorfleet.Lease
		processLeaseCurrent bool
		finalizationLocked  bool
	}
	activated := make(chan activationObservation, 1)
	var servicePointer atomic.Pointer[Service]
	manager.activateFunc = func(lease collectorfleet.Lease) error {
		service := servicePointer.Load()
		if service == nil {
			activated <- activationObservation{lease: lease}
			return nil
		}

		key := streamKey(lease)
		registry.mu.RLock()
		entry, exists := registry.entries[key]
		processLeaseCurrent := exists &&
			entry.active &&
			entry.lease.Lease == lease
		registry.mu.RUnlock()

		service.finalizersMu.Lock()
		finalizer := service.finalizers[CollectorStreamKey{
			TenantID:    lease.TenantID,
			CollectorID: lease.CollectorID,
		}]
		finalizationLocked := finalizer != nil && !finalizer.mu.TryLock()
		if finalizer != nil && !finalizationLocked {
			finalizer.mu.Unlock()
		}
		service.finalizersMu.Unlock()

		activated <- activationObservation{
			lease:               lease,
			processLeaseCurrent: processLeaseCurrent,
			finalizationLocked:  finalizationLocked,
		}
		return nil
	}
	config := testServiceConfig()
	config.SessionManager = manager
	config.StreamRegistry = registry
	harness := newServiceHarness(t, config, authorizer, acceptingStore())
	servicePointer.Store(harness.server)

	stream := harness.stream(t, "Bearer good-token")
	sendHello(t, stream, 1)
	ready := recvResponse(t, stream).GetReady()
	if ready == nil {
		t.Fatal("first response was not Ready")
	}
	select {
	case observation := <-activated:
		if observation.lease.TenantID != "tenant-a" ||
			observation.lease.CollectorID != "collector-a" ||
			observation.lease.BootEpoch != config.ServerInstanceID ||
			observation.lease.StreamID != ready.GetStreamId() ||
			observation.lease.Generation == 0 {
			t.Fatalf("activated lease = %+v", observation.lease)
		}
		if !observation.processLeaseCurrent {
			t.Fatal("heartbeat activation ran before exact process promotion")
		}
		if !observation.finalizationLocked {
			t.Fatal("heartbeat activation ran outside the collector finalization lock")
		}
	default:
		t.Fatal("Ready was sent without heartbeat runtime activation")
	}

	sendGoodbye(t, stream)
	if _, err := stream.Recv(); !errors.Is(err, io.EOF) {
		t.Fatalf("stream goodbye error = %v, want EOF", err)
	}
}

func TestCollectMapsHeartbeatActivationFailuresBeforeReadyAndCleansExactLease(
	t *testing.T,
) {
	t.Parallel()

	tests := []struct {
		name     string
		err      error
		wantCode codes.Code
		wantText string
	}{
		{
			name:     "stale lease",
			err:      fmt.Errorf("private predecessor detail: %w", ErrCollectorLeaseNotCurrent),
			wantCode: codes.Aborted,
			wantText: "collector stream was superseded",
		},
		{
			name:     "runtime capacity",
			err:      fmt.Errorf("private capacity detail: %w", ErrCollectorSessionCapacity),
			wantCode: codes.ResourceExhausted,
			wantText: "collector heartbeat capacity is exhausted",
		},
		{
			name:     "runtime unavailable",
			err:      errors.New("private runtime topology detail"),
			wantCode: codes.Unavailable,
			wantText: "collector heartbeat runtime is unavailable",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			authorization := boundTestAuthorization(
				"subject-a",
				"tenant-a",
				"collector-a",
			)
			authorizer := AuthorizerFunc(func(
				context.Context,
				string,
			) (Authorization, error) {
				return authorization, nil
			})
			manager := newTestCollectorSessionManager(authorizer)
			manager.admitFunc = func(
				_ context.Context,
				_ string,
				request CollectorSessionAdmissionRequest,
			) (CollectorSessionAdmission, error) {
				return manager.admissionFor(authorization, request), nil
			}
			activationStarted := make(chan collectorfleet.Lease, 1)
			finishActivation := make(chan struct{})
			manager.activateFunc = func(lease collectorfleet.Lease) error {
				activationStarted <- lease
				<-finishActivation
				return tt.err
			}
			registry := NewInMemoryCollectorStreamRegistry()
			type disconnectObservation struct {
				lease                collectorfleet.Lease
				exactProcessReleased bool
			}
			disconnected := make(chan disconnectObservation, 1)
			manager.disconnectFunc = func(
				_ context.Context,
				lease collectorfleet.Lease,
				_ time.Time,
			) (bool, error) {
				key := streamKey(lease)
				registry.mu.RLock()
				processEntry, exists := registry.entries[key]
				exactProcessReleased := exists &&
					processEntry.lease.Lease == lease &&
					!processEntry.active
				registry.mu.RUnlock()
				disconnected <- disconnectObservation{
					lease:                lease,
					exactProcessReleased: exactProcessReleased,
				}
				return true, nil
			}
			var storeCalls atomic.Uint32
			store := EventStoreFunc(func(
				context.Context,
				StoreBatch,
			) (StoreResult, error) {
				storeCalls.Add(1)
				return StoreResult{}, nil
			})
			config := testServiceConfig()
			config.SessionManager = manager
			config.StreamRegistry = registry
			harness := newServiceHarness(t, config, authorizer, store)
			stream := harness.stream(t, "Bearer good-token")

			sendHello(t, stream, 1)
			var activatedLease collectorfleet.Lease
			select {
			case activatedLease = <-activationStarted:
			case <-time.After(time.Second):
				t.Fatal("heartbeat activation was not called")
			}
			batch := validTestBatch(
				"collector-a",
				"batch-before-ready",
				1,
				validTestEvent("event-a", "main"),
			)
			if err := stream.Send(batchRequest(2, batch)); err != nil {
				t.Fatal(err)
			}
			close(finishActivation)

			response, err := stream.Recv()
			if response != nil || status.Code(err) != tt.wantCode {
				t.Fatalf(
					"Recv() = (%#v, %v), want nil/%v",
					response,
					err,
					tt.wantCode,
				)
			}
			if got := status.Convert(err).Message(); got != tt.wantText {
				t.Fatalf("status message = %q, want %q", got, tt.wantText)
			}
			if got := storeCalls.Load(); got != 0 {
				t.Fatalf("event store calls = %d, want 0", got)
			}

			select {
			case observation := <-disconnected:
				if observation.lease != activatedLease {
					t.Fatalf(
						"disconnected lease = %+v, want activated %+v",
						observation.lease,
						activatedLease,
					)
				}
				if !observation.exactProcessReleased {
					t.Fatal("durable disconnect ran before exact process release")
				}
			case <-time.After(time.Second):
				t.Fatal("activation failure leaked its durable lease")
			}
			key := streamKey(activatedLease)
			registry.mu.RLock()
			processEntry, exists := registry.entries[key]
			registry.mu.RUnlock()
			if !exists ||
				processEntry.lease.Lease != activatedLease ||
				processEntry.active {
				t.Fatalf(
					"process lease after activation failure = %+v, exists %t",
					processEntry,
					exists,
				)
			}
		})
	}
}

func TestCollectUsesFreshAdmissionAuthorityAndDetachedExactCleanup(t *testing.T) {
	t.Parallel()
	preliminary := boundTestAuthorization(
		"subject-a",
		"tenant-a",
		"collector-a",
	)
	fresh := cloneAuthorization(preliminary)
	fresh.AuthorizedIndexes = testIndexPolicies("audit")
	authorizer := AuthorizerFunc(func(context.Context, string) (Authorization, error) {
		return preliminary, nil
	})
	manager := newTestCollectorSessionManager(authorizer)
	manager.admitFunc = func(
		_ context.Context,
		_ string,
		request CollectorSessionAdmissionRequest,
	) (CollectorSessionAdmission, error) {
		return manager.admissionFor(fresh, request), nil
	}
	type disconnectObservation struct {
		lease       collectorfleet.Lease
		contextErr  error
		hasDeadline bool
	}
	disconnected := make(chan disconnectObservation, 1)
	manager.disconnectFunc = func(
		ctx context.Context,
		lease collectorfleet.Lease,
		_ time.Time,
	) (bool, error) {
		_, hasDeadline := ctx.Deadline()
		disconnected <- disconnectObservation{
			lease:       lease,
			contextErr:  ctx.Err(),
			hasDeadline: hasDeadline,
		}
		return true, nil
	}
	config := testServiceConfig()
	config.SessionManager = manager
	harness := newServiceHarness(t, config, authorizer, acceptingStore())
	stream := harness.stream(t, "Bearer good-token")
	sendHello(t, stream, 1)
	ready := recvResponse(t, stream).GetReady()
	if ready == nil ||
		len(ready.GetAuthorizedIndexes()) != 1 ||
		ready.GetAuthorizedIndexes()[0] != "audit" {
		t.Fatalf("Ready authority = %#v", ready.GetAuthorizedIndexes())
	}
	if err := stream.CloseSend(); err != nil {
		t.Fatal(err)
	}
	if _, err := stream.Recv(); !errors.Is(err, io.EOF) {
		t.Fatalf("stream close error = %v, want EOF", err)
	}
	select {
	case observation := <-disconnected:
		if observation.contextErr != nil || !observation.hasDeadline {
			t.Fatalf("cleanup context = err:%v deadline:%v", observation.contextErr, observation.hasDeadline)
		}
		if observation.lease.TenantID != "tenant-a" ||
			observation.lease.CollectorID != "collector-a" ||
			observation.lease.BootEpoch != config.ServerInstanceID ||
			observation.lease.StreamID != ready.GetStreamId() ||
			observation.lease.Generation == 0 {
			t.Fatalf("cleanup lease = %+v", observation.lease)
		}
	case <-time.After(time.Second):
		t.Fatal("durable collector cleanup was not called")
	}
}

func TestCollectCleansCommittedLeaseWhenAdmissionResultIsInvalid(t *testing.T) {
	t.Parallel()
	authorization := boundTestAuthorization(
		"subject-a",
		"tenant-a",
		"collector-a",
	)
	authorizer := AuthorizerFunc(func(context.Context, string) (Authorization, error) {
		return authorization, nil
	})
	manager := newTestCollectorSessionManager(authorizer)
	committed := collectorfleet.Lease{
		Scope:       collectorfleet.Scope{TenantID: "tenant-a"},
		CollectorID: "collector-a",
		BootEpoch:   "server-test",
		StreamID:    "wrong-stream",
		Generation:  1,
	}
	manager.admitFunc = func(
		context.Context,
		string,
		CollectorSessionAdmissionRequest,
	) (CollectorSessionAdmission, error) {
		return CollectorSessionAdmission{
			Authorization: authorization,
			Lease:         committed,
		}, nil
	}
	disconnected := make(chan collectorfleet.Lease, 1)
	manager.disconnectFunc = func(
		_ context.Context,
		lease collectorfleet.Lease,
		_ time.Time,
	) (bool, error) {
		disconnected <- lease
		return true, nil
	}
	config := testServiceConfig()
	config.SessionManager = manager
	harness := newServiceHarness(t, config, authorizer, acceptingStore())
	stream := harness.stream(t, "Bearer good-token")
	sendHello(t, stream, 1)
	response, err := stream.Recv()
	if response != nil || status.Code(err) != codes.Unavailable {
		t.Fatalf("Recv() = (%#v, %v), want nil/Unavailable", response, err)
	}
	select {
	case lease := <-disconnected:
		if lease != committed {
			t.Fatalf("cleanup lease = %+v, want %+v", lease, committed)
		}
	case <-time.After(time.Second):
		t.Fatal("invalid post-commit result leaked its durable lease")
	}
}

func TestDisconnectCollectorSessionRetriesTransientFailure(t *testing.T) {
	t.Parallel()
	authorization := boundTestAuthorization(
		"subject-a",
		"tenant-a",
		"collector-a",
	)
	authorizer := AuthorizerFunc(func(context.Context, string) (Authorization, error) {
		return authorization, nil
	})
	manager := newTestCollectorSessionManager(authorizer)
	lease := collectorfleet.Lease{
		Scope:       collectorfleet.Scope{TenantID: "tenant-a"},
		CollectorID: "collector-a",
		BootEpoch:   "server-test",
		StreamID:    "stream-a",
		Generation:  1,
	}
	var calls atomic.Uint32
	var observedTimes []time.Time
	var observedMu sync.Mutex
	manager.disconnectFunc = func(
		_ context.Context,
		got collectorfleet.Lease,
		disconnectedAt time.Time,
	) (bool, error) {
		if got != lease {
			return false, errors.New("unexpected lease")
		}
		observedMu.Lock()
		observedTimes = append(observedTimes, disconnectedAt)
		observedMu.Unlock()
		if calls.Add(1) == 1 {
			return false, errors.New("database is temporarily busy")
		}
		return true, nil
	}
	var reported atomic.Uint32
	config := testServiceConfig()
	config.SessionManager = manager
	config.SessionErrorHandler = func(error) {
		reported.Add(1)
	}
	service, err := NewService(config, authorizer, acceptingStore())
	if err != nil {
		t.Fatal(err)
	}
	service.disconnectCollectorSession(lease)
	if calls.Load() != 2 {
		t.Fatalf("disconnect attempts = %d, want 2", calls.Load())
	}
	if reported.Load() != 0 {
		t.Fatalf("reported cleanup errors = %d, want 0", reported.Load())
	}
	observedMu.Lock()
	defer observedMu.Unlock()
	if len(observedTimes) != 2 ||
		!observedTimes[0].Equal(validationTestNow) ||
		!observedTimes[1].Equal(validationTestNow) {
		t.Fatalf("disconnect times = %v, want stable request time", observedTimes)
	}
}

func TestDisconnectCollectorSessionUsesConfiguredTimeout(t *testing.T) {
	t.Parallel()
	authorization := boundTestAuthorization(
		"subject-a",
		"tenant-a",
		"collector-a",
	)
	authorizer := AuthorizerFunc(func(context.Context, string) (Authorization, error) {
		return authorization, nil
	})
	manager := newTestCollectorSessionManager(authorizer)
	lease := collectorfleet.Lease{
		Scope:       collectorfleet.Scope{TenantID: "tenant-a"},
		CollectorID: "collector-a",
		BootEpoch:   "server-test",
		StreamID:    "stream-a",
		Generation:  1,
	}
	const cleanupTimeout = 37 * time.Second
	var (
		observedDeadline time.Time
		observedAt       time.Time
		observedContext  error
		hasDeadline      bool
	)
	manager.disconnectFunc = func(
		ctx context.Context,
		got collectorfleet.Lease,
		_ time.Time,
	) (bool, error) {
		if got != lease {
			return false, errors.New("unexpected lease")
		}
		observedAt = time.Now()
		observedDeadline, hasDeadline = ctx.Deadline()
		observedContext = ctx.Err()
		return true, nil
	}
	config := testServiceConfig()
	config.SessionManager = manager
	config.SessionCleanupTimeout = cleanupTimeout
	service, err := NewService(config, authorizer, acceptingStore())
	if err != nil {
		t.Fatal(err)
	}

	service.disconnectCollectorSession(lease)

	remaining := observedDeadline.Sub(observedAt)
	if !hasDeadline ||
		observedContext != nil ||
		remaining <= cleanupTimeout-time.Second ||
		remaining > cleanupTimeout {
		t.Fatalf(
			"cleanup context = deadline:%v remaining:%v err:%v, want configured %v",
			hasDeadline,
			remaining,
			observedContext,
			cleanupTimeout,
		)
	}
}

func TestCollectAuthorizesExactLeaseAndPersistsHeartbeatBeforeBatch(t *testing.T) {
	t.Parallel()
	initial := boundTestAuthorization(
		"subject-a",
		"tenant-a",
		"collector-a",
	)
	authorizer := AuthorizerFunc(func(context.Context, string) (Authorization, error) {
		return initial, nil
	})
	manager := newTestCollectorSessionManager(authorizer)
	manager.admitFunc = func(
		_ context.Context,
		_ string,
		request CollectorSessionAdmissionRequest,
	) (CollectorSessionAdmission, error) {
		return manager.admissionFor(initial, request), nil
	}
	var authorizationCalls atomic.Uint32
	boundaryMismatch := make(chan string, 1)
	manager.authorizeFunc = func(
		_ context.Context,
		bearer string,
		lease collectorfleet.Lease,
		checkedAt time.Time,
	) (Authorization, error) {
		if bearer != "good-token" ||
			lease.TenantID != "tenant-a" ||
			lease.CollectorID != "collector-a" ||
			!checkedAt.Equal(validationTestNow) {
			boundaryMismatch <- fmt.Sprintf(
				"bearer:%q lease:%+v at:%v",
				bearer, lease, checkedAt,
			)
			return Authorization{}, errors.New(
				"unexpected test lease authorization boundary",
			)
		}
		authorizationCalls.Add(1)
		result := cloneAuthorization(initial)
		result.AuthorizedIndexes = testIndexPolicies("audit")
		return result, nil
	}
	var (
		heartbeatMu sync.Mutex
		heartbeat   collectorfleet.Heartbeat
	)
	manager.heartbeatFunc = func(
		_ context.Context,
		_ collectorfleet.Lease,
		snapshot collectorfleet.Heartbeat,
	) (bool, error) {
		heartbeatMu.Lock()
		heartbeat = snapshot
		heartbeatMu.Unlock()
		return true, nil
	}
	var stored StoreBatch
	store := EventStoreFunc(func(_ context.Context, batch StoreBatch) (StoreResult, error) {
		stored = batch
		return StoreResult{
			Accepted:    uint32(len(batch.Events)),
			CommittedAt: validationTestNow,
		}, nil
	})
	config := testServiceConfig()
	config.SessionManager = manager
	harness := newServiceHarness(t, config, authorizer, store)
	stream := harness.stream(t, "Bearer good-token")
	sendHello(t, stream, 1)
	_ = recvResponse(t, stream)
	if err := stream.Send(&opensplunkv1.CollectRequest{
		StreamSequence: 2,
		SentAt:         timestamppb.New(validationTestNow),
		Payload: &opensplunkv1.CollectRequest_Heartbeat{
			Heartbeat: &opensplunkv1.CollectorHeartbeat{
				CollectorId: "collector-a",
				InstanceId:  "instance-a",
				ObservedAt:  timestamppb.New(validationTestNow),
				Queue: &opensplunkv1.CollectorQueueStats{
					QueuedEvents: 3,
					QueuedBytes:  42,
				},
				ProcessResidentMemoryBytes: 4096,
				ProcessCpuPercent:          7.5,
			},
		},
	}); err != nil {
		t.Fatal(err)
	}
	batch := validTestBatch(
		"collector-a",
		"batch-after-heartbeat",
		1,
		validTestEvent("event-audit", "audit"),
	)
	if err := stream.Send(batchRequest(3, batch)); err != nil {
		t.Fatal(err)
	}
	ack := recvResponse(t, stream).GetBatchAck()
	if ack == nil || ack.GetAcceptedEventCount() != 1 {
		t.Fatalf("batch acknowledgment = %#v", ack)
	}
	if authorizationCalls.Load() != 2 {
		t.Fatalf("lease authorization calls = %d, want 2", authorizationCalls.Load())
	}
	select {
	case mismatch := <-boundaryMismatch:
		t.Fatalf("lease authorization boundary = %s", mismatch)
	default:
	}
	heartbeatMu.Lock()
	gotHeartbeat := heartbeat
	heartbeatMu.Unlock()
	if gotHeartbeat.ObservationSequence != 2 ||
		!gotHeartbeat.ObservedAt.Equal(validationTestNow) ||
		!gotHeartbeat.ReceivedAt.Equal(validationTestNow) ||
		gotHeartbeat.Queue.QueuedEvents != 3 ||
		gotHeartbeat.Queue.QueuedBytes != 42 ||
		gotHeartbeat.ProcessResidentMemoryBytes != 4096 ||
		gotHeartbeat.ProcessCPUPercent != 7.5 {
		t.Fatalf("persisted heartbeat = %+v", gotHeartbeat)
	}
	if len(stored.Events) != 1 ||
		stored.Events[0].Event.GetIndexName() != "audit" {
		t.Fatalf("batch did not use fresh request scope: %+v", stored)
	}
}

func TestCollectStaleDurableLeaseNeverStartsBatchWork(t *testing.T) {
	t.Parallel()
	authorization := boundTestAuthorization(
		"subject-a",
		"tenant-a",
		"collector-a",
	)
	authorizer := AuthorizerFunc(func(context.Context, string) (Authorization, error) {
		return authorization, nil
	})
	manager := newTestCollectorSessionManager(authorizer)
	manager.admitFunc = func(
		_ context.Context,
		_ string,
		request CollectorSessionAdmissionRequest,
	) (CollectorSessionAdmission, error) {
		return manager.admissionFor(authorization, request), nil
	}
	manager.authorizeFunc = func(
		context.Context,
		string,
		collectorfleet.Lease,
		time.Time,
	) (Authorization, error) {
		return Authorization{}, ErrCollectorLeaseNotCurrent
	}
	var storeCalls atomic.Uint32
	store := EventStoreFunc(func(context.Context, StoreBatch) (StoreResult, error) {
		storeCalls.Add(1)
		return StoreResult{}, nil
	})
	config := testServiceConfig()
	config.SessionManager = manager
	harness := newServiceHarness(t, config, authorizer, store)
	stream := harness.stream(t, "Bearer good-token")
	sendHello(t, stream, 1)
	_ = recvResponse(t, stream)
	batch := validTestBatch(
		"collector-a",
		"stale-durable-batch",
		1,
		validTestEvent("event-a", "main"),
	)
	if err := stream.Send(batchRequest(2, batch)); err != nil {
		t.Fatal(err)
	}
	response, err := stream.Recv()
	if response != nil || status.Code(err) != codes.Aborted {
		t.Fatalf("Recv() = (%#v, %v), want nil/Aborted", response, err)
	}
	if storeCalls.Load() != 0 {
		t.Fatalf("durable batch store calls = %d, want 0", storeCalls.Load())
	}
}

func TestCollectHeartbeatNoOpMeansLeaseLost(t *testing.T) {
	t.Parallel()
	authorization := boundTestAuthorization(
		"subject-a",
		"tenant-a",
		"collector-a",
	)
	authorizer := AuthorizerFunc(func(context.Context, string) (Authorization, error) {
		return authorization, nil
	})
	manager := newTestCollectorSessionManager(authorizer)
	manager.admitFunc = func(
		_ context.Context,
		_ string,
		request CollectorSessionAdmissionRequest,
	) (CollectorSessionAdmission, error) {
		return manager.admissionFor(authorization, request), nil
	}
	manager.heartbeatFunc = func(
		context.Context,
		collectorfleet.Lease,
		collectorfleet.Heartbeat,
	) (bool, error) {
		return false, nil
	}
	config := testServiceConfig()
	config.SessionManager = manager
	harness := newServiceHarness(t, config, authorizer, acceptingStore())
	stream := harness.stream(t, "Bearer good-token")
	sendHello(t, stream, 1)
	_ = recvResponse(t, stream)
	if err := stream.Send(&opensplunkv1.CollectRequest{
		StreamSequence: 2,
		SentAt:         timestamppb.New(validationTestNow),
		Payload: &opensplunkv1.CollectRequest_Heartbeat{
			Heartbeat: &opensplunkv1.CollectorHeartbeat{
				CollectorId: "collector-a",
				InstanceId:  "instance-a",
				ObservedAt:  timestamppb.New(validationTestNow),
			},
		},
	}); err != nil {
		t.Fatal(err)
	}
	response, err := stream.Recv()
	if response != nil || status.Code(err) != codes.Aborted {
		t.Fatalf("Recv() = (%#v, %v), want nil/Aborted", response, err)
	}
}

func TestCollectSerializesDurableAdmissionThroughProcessActivationPerCollector(t *testing.T) {
	t.Parallel()
	authorizations := map[string]Authorization{
		"first-token": boundTestAuthorization(
			"subject-a",
			"tenant-a",
			"collector-a",
		),
		"second-token": boundTestAuthorization(
			"subject-b",
			"tenant-a",
			"collector-a",
		),
	}
	secondPreauthorized := make(chan struct{})
	var secondPreauthOnce sync.Once
	authorizer := AuthorizerFunc(func(
		_ context.Context,
		bearer string,
	) (Authorization, error) {
		authorization, ok := authorizations[bearer]
		if !ok {
			return Authorization{}, ErrUnauthorized
		}
		if bearer == "second-token" {
			secondPreauthOnce.Do(func() {
				close(secondPreauthorized)
			})
		}
		return authorization, nil
	})
	secondAdmit := make(chan struct{})
	secondAcceptedAt := make(chan time.Time, 1)
	firstAdmitStarted := make(chan struct{})
	releaseFirstAdmit := make(chan struct{})
	manager := newTestCollectorSessionManager(authorizer)
	manager.admitFunc = func(
		_ context.Context,
		bearer string,
		request CollectorSessionAdmissionRequest,
	) (CollectorSessionAdmission, error) {
		authorization, ok := authorizations[bearer]
		if !ok {
			return CollectorSessionAdmission{}, ErrUnauthorized
		}
		generation := uint64(1)
		if bearer == "first-token" {
			close(firstAdmitStarted)
			<-releaseFirstAdmit
		} else {
			close(secondAdmit)
			secondAcceptedAt <- request.AcceptedAt
			generation = 2
		}
		return CollectorSessionAdmission{
			Authorization: authorization,
			Lease: collectorfleet.Lease{
				Scope: collectorfleet.Scope{
					TenantID: authorization.TenantID,
				},
				CollectorID: request.CollectorID,
				BootEpoch:   request.BootEpoch,
				StreamID:    request.StreamID,
				Generation:  generation,
			},
		}, nil
	}
	config := testServiceConfigWithUniqueStreamIDs()
	var clockMu sync.Mutex
	clockNow := validationTestNow
	config.Clock = func() time.Time {
		clockMu.Lock()
		defer clockMu.Unlock()
		return clockNow
	}
	config.SessionManager = manager
	harness := newServiceHarness(t, config, authorizer, acceptingStore())

	first := harness.stream(t, "Bearer first-token")
	sendHello(t, first, 1)
	select {
	case <-firstAdmitStarted:
	case <-time.After(time.Second):
		t.Fatal("first durable admission did not reach the finalization handoff")
	}

	second := harness.stream(t, "Bearer second-token")
	sendHello(t, second, 1)
	select {
	case <-secondPreauthorized:
	case <-time.After(time.Second):
		t.Fatal("second stream did not reach preliminary authorization")
	}
	waitForCollectorFinalizerReferences(
		t,
		harness.server,
		CollectorStreamKey{TenantID: "tenant-a", CollectorID: "collector-a"},
		2,
	)
	select {
	case <-secondAdmit:
		t.Fatal("newer durable admission crossed the older activation handoff")
	default:
	}

	postWait := validationTestNow.Add(time.Minute)
	clockMu.Lock()
	clockNow = postWait
	clockMu.Unlock()
	close(releaseFirstAdmit)
	if ready := recvResponse(t, first).GetReady(); ready == nil {
		t.Fatal("first response was not Ready")
	}
	if ready := recvResponse(t, second).GetReady(); ready == nil {
		t.Fatal("second response was not Ready")
	}
	select {
	case acceptedAt := <-secondAcceptedAt:
		if !acceptedAt.Equal(postWait) {
			t.Fatalf(
				"second admission time = %v, want post-wait %v",
				acceptedAt,
				postWait,
			)
		}
	default:
		t.Fatal("second durable admission was not observed")
	}
	if _, err := first.Recv(); status.Code(err) != codes.Aborted {
		t.Fatalf("superseded first stream error = %v, want Aborted", err)
	}
	sendGoodbye(t, second)
	if _, err := second.Recv(); !errors.Is(err, io.EOF) {
		t.Fatalf("second stream goodbye error = %v, want EOF", err)
	}
}

func waitForCollectorFinalizerReferences(
	t *testing.T,
	service *Service,
	key CollectorStreamKey,
	want uint64,
) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		service.finalizersMu.Lock()
		finalizer := service.finalizers[key]
		var references uint64
		if finalizer != nil {
			references = finalizer.references
		}
		service.finalizersMu.Unlock()
		if references == want {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("collector finalizer references did not reach %d", want)
}
