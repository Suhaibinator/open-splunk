package ingest

import (
	"context"
	"errors"
	"fmt"
	"testing"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"google.golang.org/protobuf/proto"
)

func TestBatchValueNodeBudgetPreflight(t *testing.T) {
	service := &Service{config: testServiceConfig(), validator: newTestValidator(t, DefaultLimits())}
	for _, nodes := range []int{int(HardMaxBatchValueNodes), int(HardMaxBatchValueNodes) + 1} {
		t.Run(fmt.Sprint(nodes), func(t *testing.T) {
			batch := validTestBatch("collector-a", "batch-budget", 1, batchValueBudgetEvents(nodes)...)
			if batch.GetUncompressedSizeBytes() >= HardMaxBatchBytes {
				t.Fatal("test must remain below the independent byte limit")
			}
			var rejection *opensplunk.BatchReject
			allocs := testing.AllocsPerRun(5, func() {
				rejection = service.validateBatchPolicy(batch, validationTestNow, batch.GetUncompressedSizeBytes())
			})
			if nodes == int(HardMaxBatchValueNodes) {
				if rejection != nil || allocs != 0 {
					t.Fatalf("exact-limit preflight = %v, %.0f allocations; want nil/0", rejection, allocs)
				}
			} else if rejection.GetCode() != opensplunk.BatchRejectionCode_BATCH_REJECTION_CODE_BATCH_TOO_LARGE ||
				len(rejection.GetViolations()) != 1 || rejection.GetViolations()[0].GetCode() != batchValueLimitViolation || allocs > 8 {
				t.Fatalf("aggregate N+1 preflight = %v, %.0f allocations", rejection, allocs)
			}
		})
	}
}

func TestBatchValueNodeBudgetCountsMixedContainers(t *testing.T) {
	fields := object(objectField("nested", object(valueBudgetField("items", valueBudgetListOf(
		objectField("unused", object(stringField("leaf", "value"))).Value,
		valueBudgetList(0),
	)))))
	// Object, list, object, scalar, and empty list each consume one node.
	for _, remaining := range []uint32{4, 5} {
		budget := batchValueBudget{remaining: remaining}
		if fits := budget.consumeObject(fields, 1); fits != (remaining == 5) {
			t.Fatalf("mixed containers with budget %d: fit=%t", remaining, fits)
		}
	}

	value := valueBudgetList(int(HardMaxBatchValueNodes) + 1)
	for range HardMaxNestingDepth {
		value = valueBudgetListOf(value)
	}
	event := validTestEvent("event-deep", "main")
	event.Fields = object(valueBudgetField("nested", value))
	budget := batchValueBudget{remaining: HardMaxBatchValueNodes}
	if !budget.consumeObject(event.Fields, 1) || budget.remaining != HardMaxBatchValueNodes-HardMaxNestingDepth {
		t.Fatal("preflight walked a subtree which cannot pass the hard depth limit")
	}
	_, rejection := newTestValidator(t, DefaultLimits()).ValidateAndNormalizeEvent(event, EventContext{ReceivedAt: validationTestNow})
	assertEventRejectionCode(t, rejection, opensplunk.EventRejectionCode_EVENT_REJECTION_CODE_NESTING_TOO_DEEP)
}

func TestBatchValueNodeBudgetFencesRepackingAndPreservesReplay(t *testing.T) {
	batch := validTestBatch("collector-a", "batch-aggregate", 1, batchValueBudgetEvents(int(HardMaxBatchValueNodes)+1)...)
	for _, supportsRepacking := range []bool{false, true} {
		t.Run(fmt.Sprint(supportsRepacking), func(t *testing.T) {
			store := &recoverableTestStore{}
			service, err := NewService(withTestSessionManager(testServiceConfig(), staticTestAuthorizer()), staticTestAuthorizer(), store)
			if err != nil {
				t.Fatal(err)
			}
			state := testBatchStreamState(service)
			state.supportsRepacking = supportsRepacking
			// Ordinary batches also need a durable repack fence: the node limit
			// is not part of the client's advertised byte/count limits.
			response, err := service.processBatch(context.Background(), batch, state, validationTestNow)
			wantCode := opensplunk.BatchRejectionCode_BATCH_REJECTION_CODE_BATCH_TOO_LARGE
			if supportsRepacking {
				wantCode = opensplunk.BatchRejectionCode_BATCH_REJECTION_CODE_REPACK_REQUIRED
			}
			if err != nil || response.GetBatchReject().GetCode() != wantCode || store.rejectCalls != 1 || store.storeCalls != 0 {
				t.Fatalf("aggregate rejection was not durably fenced: %v, %v", response, err)
			}
			replayed, err := service.processBatch(context.Background(), batch, testBatchStreamState(service), validationTestNow)
			if err != nil || !proto.Equal(replayed.GetBatchReject(), response.GetBatchReject()) || store.rejectCalls != 1 {
				t.Fatalf("aggregate rejection changed during replay: %v, %v", replayed, err)
			}
			// Old committed data is recovered even when its exact original
			// batch exceeds the new aggregate budget.
			store.storedState = StoredBatchCommitted
			store.result = StoreResult{Accepted: uint32(len(batch.Events)), OriginalEventCount: uint32(len(batch.Events)), CommittedAt: validationTestNow}
			replayed, err = service.processBatch(context.Background(), batch, testBatchStreamState(service), validationTestNow)
			if err != nil || replayed.GetBatchAck().GetDuplicateEventCount() != uint32(len(batch.Events)) || store.rejectCalls != 1 || store.storeCalls != 0 {
				t.Fatalf("committed parent did not replay: %v, %v", replayed, err)
			}
		})
	}

	store := &recoverableTestStore{rejectErr: &TransientStoreError{Err: errors.New("rejection storage unavailable"), Reason: opensplunk.RetryBatchReason_RETRY_BATCH_REASON_STORAGE_UNAVAILABLE}}
	service, err := NewService(withTestSessionManager(testServiceConfig(), staticTestAuthorizer()), staticTestAuthorizer(), store)
	if err != nil {
		t.Fatal(err)
	}
	state := testBatchStreamState(service)
	state.supportsRepacking = true
	response, err := service.processBatch(context.Background(), batch, state, validationTestNow)
	if err != nil || response.GetRetryBatch().GetReason() != opensplunk.RetryBatchReason_RETRY_BATCH_REASON_STORAGE_UNAVAILABLE || response.GetBatchReject() != nil || store.storeCalls != 0 {
		t.Fatalf("unpersisted fence authorized splitting: %v, %v", response, err)
	}
}

func TestAdmissionPreparerBatchValueNodeBudget(t *testing.T) {
	store := &admissionTestStagingStore{}
	preparer := admissionTestPreparer(t, AdmissionConfig{}, store)
	for _, nodes := range []int{int(HardMaxBatchValueNodes), int(HardMaxBatchValueNodes) + 1} {
		events := batchValueBudgetEvents(nodes)
		candidates := make([]AdmissionEvent, len(events))
		for i, event := range events {
			candidates[i] = AdmissionEvent{Event: event}
		}
		request := admissionTestHECRequest(candidates...)
		prepared, err := preparer.Prepare(request)
		if nodes == int(HardMaxBatchValueNodes) {
			if err != nil || len(prepared.Events) != len(events) {
				t.Fatalf("exact aggregate request rejected: %v", err)
			}
		} else {
			if !errors.Is(err, ErrAdmissionRequestTooLarge) || len(prepared.Events) != 0 {
				t.Fatalf("aggregate N+1 request = %v, %v", prepared, err)
			}
			allocs := testing.AllocsPerRun(5, func() { _, _ = preparer.Prepare(request) })
			if allocs > 64 {
				t.Fatalf("aggregate rejection allocated %.0f objects before failure", allocs)
			}
		}
	}
	if store.storeCalls != 0 || store.stageCalls != 0 {
		t.Fatal("Prepare mutated persistence")
	}
}

func batchValueBudgetEvents(nodes int) []*opensplunk.LogEvent {
	var events []*opensplunk.LogEvent
	for nodes > 0 {
		count := min(nodes, int(HardMaxEventValueNodes))
		event := validTestEvent(fmt.Sprintf("event-%d", len(events)), "main")
		event.Fields = object(valueBudgetField("items", valueBudgetList(count-1)))
		events = append(events, event)
		nodes -= count
	}
	return events
}
