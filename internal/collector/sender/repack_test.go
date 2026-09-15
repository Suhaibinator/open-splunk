package sender

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/collector/wal"
	"github.com/Suhaibinator/open-splunk/internal/ingest"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func TestSenderRepackingRecoversLostChildAckWithoutDuplicateIdentities(t *testing.T) {
	t.Parallel()
	queue, err := wal.Open(wal.Options{Dir: t.TempDir(), Sync: wal.SyncAlways, CollectorID: "collector-a"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = queue.Close() })
	parent, err := queue.Append([]*opensplunk.LogEvent{makeEvent("one", "main"), makeEvent("two", "main"), makeEvent("three", "main")})
	if err != nil {
		t.Fatal(err)
	}
	server := newFakeServer()
	server.readyFn = func() *opensplunk.CollectorReady {
		ready := defaultReady()
		ready.SupportsBatchRepacking = true
		ready.MaxBatchEvents = 1
		return ready
	}
	var mu sync.Mutex
	identities := make(map[string]*opensplunk.EventBatch)
	failedOnce := false
	server.batchErr = func(server *fakeServer, batch *opensplunk.EventBatch) error {
		mu.Lock()
		defer mu.Unlock()
		if batch.GetBatchId() == parent.GetBatchId() {
			if !proto.Equal(batch, parent) {
				t.Error("repacking changed the original identity before obtaining a fence")
			}
			return server.send(&opensplunk.CollectResponse{Payload: &opensplunk.CollectResponse_BatchReject{BatchReject: &opensplunk.BatchReject{BatchId: batch.GetBatchId(), BatchSequence: batch.GetBatchSequence(), Code: opensplunk.BatchRejectionCode_BATCH_REJECTION_CODE_REPACK_REQUIRED}}})
		}
		if len(batch.GetEvents()) != 1 {
			t.Errorf("child has %d events", len(batch.GetEvents()))
			return status.Error(codes.InvalidArgument, "bad child")
		}
		id := batch.GetEvents()[0].GetEventId()
		if previous, exists := identities[id]; exists && !proto.Equal(previous, batch) {
			t.Errorf("event %s retried with a different identity", id)
		}
		identities[id] = batch
		if !failedOnce {
			failedOnce = true
			return status.Error(codes.Unavailable, "simulate committed child with lost acknowledgment")
		}
		server.ackBatch(batch.GetBatchSequence(), 1)
		return nil
	}
	sink := &memSink{}
	opts := testOptions()
	opts.Hello.Capabilities = []opensplunk.CollectorCapability{opensplunk.CollectorCapability_COLLECTOR_CAPABILITY_LOSSLESS_REPACKING}
	sender := newTestSender(t, opts, queue, sink, nil, startServer(t, server))
	cancel, done := runSender(t, sender)
	defer func() { cancel(); <-done }()
	waitFor(t, "all repacked children acknowledged", func() bool { return queue.Stats().QueuedBatches == 0 })
	mu.Lock()
	defer mu.Unlock()
	if len(identities) != 3 {
		t.Fatalf("delivered %d distinct source events, want 3", len(identities))
	}
	if got := len(sink.snapshot()); got != 0 {
		t.Fatalf("repacking dead-lettered %d valid events", got)
	}
}

// Only the durable storage boundary is in memory: sender, WAL, protocol,
// capability negotiation, policy validation and rejection handling are real.
func TestSenderLosslessRepackingAgainstRealService(t *testing.T) {
	t.Parallel()
	queue, err := wal.Open(wal.Options{Dir: t.TempDir(), Sync: wal.SyncAlways, CollectorID: "collector-a"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = queue.Close() })
	oversized := validLogEvent("oversized", "main")
	oversized.Raw = []byte(strings.Repeat("x", 2000))
	if _, err := queue.Append([]*opensplunk.LogEvent{validLogEvent("one", "main"), oversized, validLogEvent("three", "main")}); err != nil {
		t.Fatal(err)
	}
	store := &repackServiceStore{states: make(map[ingest.StoreBatchIdentity]ingest.StoredBatchState), results: make(map[ingest.StoreBatchIdentity]ingest.StoreResult)}
	authorization := ingest.Authorization{SubjectID: "subject", TenantID: "tenant", CollectorID: "collector-a", AuthorizedIndexes: []ingest.IndexPolicy{{Name: "main", Version: 1}}}
	authorizer := ingest.AuthorizerFunc(func(context.Context, string) (ingest.Authorization, error) { return authorization, nil })
	cfg := realServiceIngestConfig(authorization)
	cfg.Limits.MaxBatchEvents = 1
	cfg.Limits.MaxEventBytes = 1024
	service, err := ingest.NewService(cfg, authorizer, store)
	if err != nil {
		t.Fatal(err)
	}
	opts := testOptions()
	opts.Hello.Capabilities = []opensplunk.CollectorCapability{opensplunk.CollectorCapability_COLLECTOR_CAPABILITY_LOSSLESS_REPACKING}
	sink := &memSink{}
	s := newTestSender(t, opts, queue, sink, nil, startServer(t, service))
	cancel, done := runSender(t, s)
	defer func() { cancel(); <-done }()
	waitFor(t, "real service repacking drained", func() bool { return queue.Stats().QueuedBatches == 0 })
	store.mu.Lock()
	defer store.mu.Unlock()
	if len(store.events) != 2 || store.repackRejections != 1 {
		t.Fatalf("stored events=%v, parent fences=%d", store.events, store.repackRejections)
	}
	seen := make(map[string]bool)
	for _, event := range store.events {
		seen[event] = true
	}
	if !seen["one"] || !seen["three"] {
		t.Fatalf("valid siblings lost: %v", seen)
	}
	records := sink.snapshot()
	if len(records) != 1 || records[0].Event.GetEventId() != "oversized" {
		t.Fatalf("wrong durable dead letters: %+v", records)
	}
}

func TestSenderRepacksAggregateValueBudgetWithoutDroppingEvents(t *testing.T) {
	queue, err := wal.Open(wal.Options{Dir: t.TempDir(), Sync: wal.SyncAlways, CollectorID: "collector-a"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = queue.Close() })
	// Five individually valid large events exceed the aggregate node budget.
	// Four lightweight predecessors keep the ordinary byte/count limits valid
	// while forcing repeated fallback bisection: 9 -> 4/5 -> 2/3.
	events := make([]*opensplunk.LogEvent, 9)
	for i := range events {
		event := validLogEvent(fmt.Sprintf("event-%d", i), "main")
		if i >= 4 {
			values := make([]*opensplunk.TypedValue, ingest.HardMaxBatchValueNodes/5)
			for j := range values {
				values[j] = &opensplunk.TypedValue{Kind: &opensplunk.TypedValue_BoolValue{}}
			}
			event.Fields = &opensplunk.TypedObject{Fields: []*opensplunk.TypedObjectField{{
				Name: "items", Value: &opensplunk.TypedValue{Kind: &opensplunk.TypedValue_ListValue{
					ListValue: &opensplunk.TypedValueList{Values: values},
				}},
			}}}
		}
		events[i] = event
	}
	parent, err := queue.Append(events)
	if err != nil {
		t.Fatal(err)
	}
	if parent.GetUncompressedSizeBytes() >= ingest.HardMaxBatchBytes {
		t.Fatal("aggregate test must remain under the advertised byte ceiling")
	}
	store := &repackServiceStore{states: make(map[ingest.StoreBatchIdentity]ingest.StoredBatchState), results: make(map[ingest.StoreBatchIdentity]ingest.StoreResult)}
	authorization := ingest.Authorization{SubjectID: "subject", TenantID: "tenant", CollectorID: "collector-a", AuthorizedIndexes: []ingest.IndexPolicy{{Name: "main", Version: 1}}}
	authorizer := ingest.AuthorizerFunc(func(context.Context, string) (ingest.Authorization, error) { return authorization, nil })
	service, err := ingest.NewService(realServiceIngestConfig(authorization), authorizer, store)
	if err != nil {
		t.Fatal(err)
	}
	opts := testOptions()
	opts.Hello.Capabilities = []opensplunk.CollectorCapability{opensplunk.CollectorCapability_COLLECTOR_CAPABILITY_LOSSLESS_REPACKING}
	sink := &memSink{}
	sender := newTestSender(t, opts, queue, sink, nil, startServer(t, service))
	cancel, done := runSender(t, sender)
	defer func() { cancel(); <-done }()
	// Race instrumentation makes the aggregate protobuf walk substantially
	// slower, so keep its scheduling headroom local to this stress test.
	waitForWithin(t, time.Minute, "aggregate repacking drained", func() bool {
		return queue.Stats().QueuedBatches == 0
	})
	store.mu.Lock()
	defer store.mu.Unlock()
	if len(store.events) != len(events) || store.repackRejections != 2 {
		t.Fatalf("stored %d events with %d fences; want 9 events/2 fences", len(store.events), store.repackRejections)
	}
	seen := make(map[string]bool)
	for _, id := range store.events {
		if seen[id] {
			t.Fatalf("event %s stored more than once", id)
		}
		seen[id] = true
	}
	for _, event := range events {
		if !seen[event.GetEventId()] {
			t.Fatalf("event %s was lost", event.GetEventId())
		}
	}
	if len(sink.snapshot()) != 0 {
		t.Fatal("aggregate repacking dead-lettered valid events")
	}
}

type repackServiceStore struct {
	mu               sync.Mutex
	states           map[ingest.StoreBatchIdentity]ingest.StoredBatchState
	results          map[ingest.StoreBatchIdentity]ingest.StoreResult
	events           []string
	repackRejections int
}

func (s *repackServiceStore) Store(_ context.Context, batch ingest.StoreBatch) (ingest.StoreResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	id := ingest.StoreBatchIdentity{TenantID: batch.TenantID, CollectorID: batch.CollectorID, BatchID: batch.BatchID, BatchSequence: batch.BatchSequence, SourceBatchSHA256: batch.SourceBatchSHA256}
	if _, exists := s.states[id]; exists {
		return ingest.StoreResult{}, errors.New("unexpected duplicate Store instead of durable lookup")
	}
	for _, event := range batch.Events {
		s.events = append(s.events, event.Event.GetEventId())
	}
	result := ingest.StoreResult{Accepted: uint32(len(batch.Events)), OriginalEventCount: batch.OriginalEventCount, RejectedEvents: batch.RejectedEvents, CommittedAt: time.Now()}
	s.states[id], s.results[id] = ingest.StoredBatchCommitted, result
	return result, nil
}

func (s *repackServiceStore) LookupBatch(_ context.Context, id ingest.StoreBatchIdentity) (ingest.StoredBatchState, ingest.StoreResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	result := s.results[id]
	if s.states[id] == ingest.StoredBatchCommitted {
		result.Duplicate, result.Accepted = result.Accepted, 0
	}
	return s.states[id], result, nil
}

func (*repackServiceStore) ResumeBatch(context.Context, ingest.StoreBatchIdentity) (ingest.StoreResult, error) {
	return ingest.StoreResult{}, errors.New("unexpected pending batch")
}

func (s *repackServiceStore) RejectBatch(_ context.Context, rejection ingest.StoreBatchRejection) (ingest.StoreResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if rejection.Rejection.GetCode() == opensplunk.BatchRejectionCode_BATCH_REJECTION_CODE_REPACK_REQUIRED {
		s.repackRejections++
	}
	result := ingest.StoreResult{BatchRejection: rejection.Rejection}
	s.states[rejection.Identity], s.results[rejection.Identity] = ingest.StoredBatchRejected, result
	return result, nil
}
