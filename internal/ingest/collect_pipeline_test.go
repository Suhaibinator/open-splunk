package ingest

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestCollectPipelinesBoundedBatchesAndReturnsExactOutOfOrderAcks(t *testing.T) {
	t.Parallel()
	started := make(chan uint64, 3)
	release := []chan struct{}{nil, make(chan struct{}), make(chan struct{}), make(chan struct{})}
	store := EventStoreFunc(func(ctx context.Context, batch StoreBatch) (StoreResult, error) {
		started <- batch.BatchSequence
		select {
		case <-release[batch.BatchSequence]:
		case <-ctx.Done():
			return StoreResult{}, ctx.Err()
		}
		through := batch.BatchSequence
		return StoreResult{Accepted: 1, CommittedAt: validationTestNow, AcknowledgedThrough: &through}, nil
	})
	cfg := testServiceConfig()
	cfg.MaxInFlightBatches = 2
	harness := newServiceHarness(t, cfg, staticTestAuthorizer(), store)
	stream := harness.stream(t, "Bearer good-token")
	sendHello(t, stream)
	if ready := recvResponse(t, stream).GetReady(); ready.GetMaxInFlightBatches() != 2 {
		t.Fatal("wrong negotiated bound")
	}
	for i := uint64(1); i <= 3; i++ {
		if err := stream.Send(batchRequest(i+1, validTestBatch("collector-a", fmt.Sprintf("batch-%d", i), i, validTestEvent(fmt.Sprintf("event-%d", i), "main")))); err != nil {
			t.Fatal(err)
		}
	}
	seen := make(map[uint64]bool)
	for range 2 {
		select {
		case sequence := <-started:
			seen[sequence] = true
		case <-time.After(time.Second):
			t.Fatal("second batch could not enter the store while the first awaited commit")
		}
	}
	if !seen[1] || !seen[2] {
		t.Fatalf("admission order/bound violated: %v", seen)
	}
	select {
	case sequence := <-started:
		t.Fatalf("batch %d exceeded in-flight limit", sequence)
	default:
	}
	close(release[2])
	response := recvResponse(t, stream)
	if ack := response.GetBatchAck(); ack.GetBatchSequence() != 2 || ack.AcknowledgedThroughBatchSequence != nil || response.GetStreamSequence() != 2 {
		t.Fatalf("unsafe out-of-order acknowledgment: %v", response)
	}
	select {
	case sequence := <-started:
		if sequence != 3 {
			t.Fatal("wrong next batch")
		}
	case <-time.After(time.Second):
		t.Fatal("completed batch did not release capacity")
	}
	close(release[1])
	if ack := recvResponse(t, stream).GetBatchAck(); ack.GetBatchSequence() != 1 {
		t.Fatal("first acknowledgment lost")
	}
	close(release[3])
	if ack := recvResponse(t, stream).GetBatchAck(); ack.GetBatchSequence() != 3 {
		t.Fatal("last acknowledgment lost")
	}
}

func TestRepackAuthorizationRequiresDurableRejectionAndRecoversCommittedParent(t *testing.T) {
	t.Parallel()
	store := &recoverableTestStore{}
	cfg := testServiceConfig()
	cfg.Limits.MaxBatchEvents = 1
	service, err := NewService(withTestSessionManager(cfg, staticTestAuthorizer()), staticTestAuthorizer(), store)
	if err != nil {
		t.Fatal(err)
	}
	state := testBatchStreamState(service)
	state.supportsRepacking, state.repackRequest = true, true
	batch := validTestBatch("collector-a", "repack-parent", 1, validTestEvent("one", "main"), validTestEvent("two", "main"))
	response, err := service.processBatch(context.Background(), batch, state, validationTestNow)
	if err != nil {
		t.Fatal(err)
	}
	if response.GetBatchReject().GetCode() != opensplunk.BatchRejectionCode_BATCH_REJECTION_CODE_REPACK_REQUIRED || store.rejectCalls != 1 || store.storeCalls != 0 {
		t.Fatalf("repacking was not durably fenced: %v", response)
	}
	// The exact rejection survives a replay even after limits are relaxed.
	service.config.Limits.MaxBatchEvents = 1000
	state = testBatchStreamState(service)
	response, err = service.processBatch(context.Background(), batch, state, validationTestNow)
	if err != nil || response.GetBatchReject().GetCode() != opensplunk.BatchRejectionCode_BATCH_REJECTION_CODE_REPACK_REQUIRED || store.storeCalls != 0 {
		t.Fatalf("parent fence was lost: %v, %v", response, err)
	}
	// A parent committed before the limit reduction must replay its original
	// outcome, never authorize new identities for already-ingested events.
	store.storedState = StoredBatchCommitted
	store.result = StoreResult{Accepted: 2, OriginalEventCount: 2, CommittedAt: validationTestNow}
	service.config.Limits.MaxBatchEvents = 1
	state = testBatchStreamState(service)
	state.supportsRepacking, state.repackRequest = true, true
	response, err = service.processBatch(context.Background(), batch, state, validationTestNow)
	if err != nil || response.GetBatchAck().GetDuplicateEventCount() != 2 || store.rejectCalls != 1 {
		t.Fatalf("committed parent was repacked: %v, %v", response, err)
	}
}

func TestRepackPersistenceFailureNeverAuthorizesNewIdentities(t *testing.T) {
	t.Parallel()
	store := &recoverableTestStore{rejectErr: &TransientStoreError{Err: errors.New("rejection storage offline"), Reason: opensplunk.RetryBatchReason_RETRY_BATCH_REASON_STORAGE_UNAVAILABLE}}
	cfg := testServiceConfig()
	cfg.Limits.MaxBatchEvents = 1
	service, err := NewService(withTestSessionManager(cfg, staticTestAuthorizer()), staticTestAuthorizer(), store)
	if err != nil {
		t.Fatal(err)
	}
	state := testBatchStreamState(service)
	state.supportsRepacking, state.repackRequest = true, true
	batch := validTestBatch("collector-a", "unfenced-parent", 1, validTestEvent("one", "main"), validTestEvent("two", "main"))
	response, err := service.processBatch(context.Background(), batch, state, validationTestNow)
	if err != nil || response.GetRetryBatch().GetReason() != opensplunk.RetryBatchReason_RETRY_BATCH_REASON_STORAGE_UNAVAILABLE || response.GetBatchReject() != nil || store.rejectCalls != 1 {
		t.Fatalf("unpersisted rejection authorized repacking: %v, %v", response, err)
	}
}

func TestCollectCapacityWaitDoesNotAdmitSupersededWork(t *testing.T) {
	t.Parallel()
	started, release := make(chan struct{}), make(chan struct{})
	var calls atomic.Uint32
	store := EventStoreFunc(func(ctx context.Context, _ StoreBatch) (StoreResult, error) {
		if calls.Add(1) == 1 {
			close(started)
		}
		select {
		case <-release:
			return StoreResult{Accepted: 1, CommittedAt: validationTestNow}, nil
		case <-ctx.Done():
			return StoreResult{}, ctx.Err()
		}
	})
	cfg := testServiceConfigWithUniqueStreamIDs()
	cfg.MaxInFlightBatches = 1
	harness := newServiceHarness(t, cfg, staticTestAuthorizer(), store)
	first := harness.stream(t, "Bearer good-token")
	sendHello(t, first)
	_ = recvResponse(t, first)
	for i := uint64(1); i <= 2; i++ {
		if err := first.Send(batchRequest(i+1, validTestBatch("collector-a", fmt.Sprintf("queued-%d", i), i, validTestEvent(fmt.Sprintf("event-%d", i), "main")))); err != nil {
			t.Fatal(err)
		}
	}
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("first batch never entered the store")
	}
	second := harness.stream(t, "Bearer good-token")
	sendHello(t, second)
	_ = recvResponse(t, second)
	close(release)
	if ack := recvResponse(t, first).GetBatchAck(); ack.GetBatchSequence() != 1 {
		t.Fatalf("lost admitted outcome: %v", ack)
	}
	if _, err := first.Recv(); status.Code(err) != codes.Aborted {
		t.Fatalf("superseded stream = %v, want Aborted", err)
	}
	if calls.Load() != 1 {
		t.Fatal("capacity waiter used authority from before takeover")
	}
}
