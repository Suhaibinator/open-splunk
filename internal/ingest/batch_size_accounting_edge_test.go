package ingest

import (
	"bytes"
	"context"
	"fmt"
	"testing"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"google.golang.org/protobuf/proto"
)

// eventOfExactProtoSize returns a valid event whose encoded size is exactly
// target bytes, so hard-limit boundaries can be probed off-by-one.
func eventOfExactProtoSize(t *testing.T, id string, target uint64) *opensplunkv1.LogEvent {
	t.Helper()
	event := validTestEvent(id, "main")
	event.Raw = nil
	padding := int64(target) - int64(proto.Size(event))
	for range 8 {
		if padding < 0 {
			t.Fatalf("target %d is smaller than the minimum encoded event", target)
		}
		event.Raw = bytes.Repeat([]byte("a"), int(padding))
		size := int64(proto.Size(event))
		if size == int64(target) {
			return event
		}
		padding += int64(target) - size
	}
	t.Fatalf("could not build an event of exactly %d bytes", target)
	return nil
}

func hardEnvelopeTestService(t *testing.T) (*Service, *streamState) {
	t.Helper()
	config := testServiceConfig()
	config = withTestSessionManager(config, staticTestAuthorizer())
	service, err := NewService(config, staticTestAuthorizer(), acceptingStore())
	if err != nil {
		t.Fatalf("NewService(): %v", err)
	}
	return service, testBatchStreamState(service)
}

// TestValidateBatchHardEnvelopeSingleWalkMatchesPerEventProtoSize proves the
// single-walk accounting is byte-identical to the per-event proto.Size sum the
// refactor replaced, including at both hard ceilings and with nil holes.
func TestValidateBatchHardEnvelopeSingleWalkMatchesPerEventProtoSize(t *testing.T) {
	t.Parallel()
	service, state := hardEnvelopeTestService(t)

	maxEvents := make([]*opensplunkv1.LogEvent, 0, HardMaxBatchEvents)
	for index := range int(HardMaxBatchEvents) {
		maxEvents = append(maxEvents, validTestEvent(fmt.Sprintf("event-%04d", index), "main"))
	}

	tests := []struct {
		name   string
		events []*opensplunkv1.LogEvent
	}{
		{name: "single event", events: []*opensplunkv1.LogEvent{validTestEvent("event-1", "main")}},
		{name: "nil interleaved", events: []*opensplunkv1.LogEvent{
			nil,
			validTestEvent("event-1", "main"),
			nil,
			validTestEvent("event-2", "main"),
			nil,
		}},
		{name: "all nil", events: []*opensplunkv1.LogEvent{nil, nil}},
		{name: "event exactly at hard limit", events: []*opensplunkv1.LogEvent{
			eventOfExactProtoSize(t, "event-max", HardMaxEventBytes),
		}},
		{name: "batch exactly at hard event count", events: maxEvents},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			batch := validTestBatch("collector-a", "batch-1", 1, test.events...)
			total, sizes, rejection := service.validateBatchHardEnvelope(batch, state)
			if rejection != nil {
				t.Fatalf("validateBatchHardEnvelope() rejection = %+v, want nil", rejection)
			}
			if want := UncompressedEventBytes(test.events); total != want {
				t.Fatalf("total = %d, want %d", total, want)
			}
			if len(sizes) != len(test.events) {
				t.Fatalf("sizes length = %d, want %d", len(sizes), len(test.events))
			}
			var walked uint64
			for index, event := range test.events {
				if want, ok := protobufSizeUint64(event); !ok || sizes[index] != want {
					t.Fatalf("sizes[%d] = %d, want %d (representable %t)", index, sizes[index], want, ok)
				}
				walked += sizes[index]
			}
			if walked != total {
				t.Fatalf("sum of sizes = %d, want reported total %d", walked, total)
			}
		})
	}
}

// TestValidateBatchHardEnvelopeBoundariesAndRejectionAccounting pins the
// inclusive/exclusive edges of both hard ceilings and proves a rejecting walk
// never hands back partially accumulated accounting.
func TestValidateBatchHardEnvelopeBoundariesAndRejectionAccounting(t *testing.T) {
	t.Parallel()
	service, state := hardEnvelopeTestService(t)

	tooManyEvents := make([]*opensplunkv1.LogEvent, 0, HardMaxBatchEvents+1)
	for index := range int(HardMaxBatchEvents) + 1 {
		tooManyEvents = append(tooManyEvents, validTestEvent(fmt.Sprintf("event-%04d", index), "main"))
	}

	tests := []struct {
		name       string
		events     []*opensplunkv1.LogEvent
		declared   uint64
		overriding bool
		wantCode   opensplunkv1.BatchRejectionCode
		wantRule   string
	}{
		{
			name:     "event one byte over the hard limit",
			events:   []*opensplunkv1.LogEvent{eventOfExactProtoSize(t, "event-max", HardMaxEventBytes+1)},
			wantCode: opensplunkv1.BatchRejectionCode_BATCH_REJECTION_CODE_BATCH_TOO_LARGE,
			wantRule: "event_too_large",
		},
		{
			name:     "one event over the hard count",
			events:   tooManyEvents,
			wantCode: opensplunkv1.BatchRejectionCode_BATCH_REJECTION_CODE_TOO_MANY_EVENTS,
			wantRule: "too_many_events",
		},
		{
			name:       "declared size overstates the walked sum",
			events:     []*opensplunkv1.LogEvent{validTestEvent("event-1", "main")},
			declared:   UncompressedEventBytes([]*opensplunkv1.LogEvent{validTestEvent("event-1", "main")}) + 1,
			overriding: true,
			wantCode:   opensplunkv1.BatchRejectionCode_BATCH_REJECTION_CODE_BATCH_TOO_LARGE,
			wantRule:   "batch_size_mismatch_or_limit",
		},
		{
			name:       "oversize event outranks a mismatched declared size",
			events:     []*opensplunkv1.LogEvent{eventOfExactProtoSize(t, "event-big", HardMaxEventBytes+1)},
			declared:   1,
			overriding: true,
			wantCode:   opensplunkv1.BatchRejectionCode_BATCH_REJECTION_CODE_BATCH_TOO_LARGE,
			wantRule:   "event_too_large",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			batch := validTestBatch("collector-a", "batch-1", 1, test.events...)
			if test.overriding {
				batch.UncompressedSizeBytes = test.declared
			}
			total, sizes, rejection := service.validateBatchHardEnvelope(batch, state)
			if rejection == nil {
				t.Fatal("validateBatchHardEnvelope() rejection = nil, want rejection")
			}
			if rejection.GetCode() != test.wantCode {
				t.Fatalf("rejection code = %v, want %v", rejection.GetCode(), test.wantCode)
			}
			if len(rejection.GetViolations()) != 1 || rejection.GetViolations()[0].GetCode() != test.wantRule {
				t.Fatalf("rejection violations = %+v, want rule %q", rejection.GetViolations(), test.wantRule)
			}
			if total != 0 || sizes != nil {
				t.Fatalf("rejected walk accounting = (%d, %v), want (0, nil)", total, sizes)
			}
		})
	}
}

// TestProcessBatchChargesWalkedSizesWithNilEventHoles proves the reused sizes
// stay index-aligned with the event slice when nil events are rejected mid-walk.
func TestProcessBatchChargesWalkedSizesWithNilEventHoles(t *testing.T) {
	t.Parallel()
	var stored StoreBatch
	store := EventStoreFunc(func(_ context.Context, batch StoreBatch) (StoreResult, error) {
		stored = batch
		return StoreResult{Accepted: uint32(len(batch.Events)), CommittedAt: validationTestNow}, nil
	})
	config := testServiceConfig()
	config = withTestSessionManager(config, staticTestAuthorizer())
	service, err := NewService(config, staticTestAuthorizer(), store)
	if err != nil {
		t.Fatalf("NewService(): %v", err)
	}

	first := validTestEvent("event-1", "main")
	second := eventOfExactProtoSize(t, "event-2", 4096)
	events := []*opensplunkv1.LogEvent{nil, first, nil, second, nil}
	batch := validTestBatch("collector-a", "batch-nil-holes", 1, events...)

	response, err := service.processBatch(
		context.Background(), batch, testBatchStreamState(service), validationTestNow,
	)
	if err != nil {
		t.Fatalf("processBatch(): %v", err)
	}
	ack := response.GetBatchAck()
	if ack == nil || ack.GetAcceptedEventCount() != 2 || len(ack.GetRejectedEvents()) != 3 {
		t.Fatalf("batch ack = %+v", ack)
	}
	for index, rejected := range ack.GetRejectedEvents() {
		if rejected.GetCode() != opensplunkv1.EventRejectionCode_EVENT_REJECTION_CODE_VALUE_INVALID ||
			rejected.GetEventIndex() != []uint32{0, 2, 4}[index] {
			t.Fatalf("nil event rejections = %+v, want VALUE_INVALID at ordinals 0, 2, 4", ack.GetRejectedEvents())
		}
	}
	wantBytes := UncompressedEventBytes([]*opensplunkv1.LogEvent{first, second})
	if stored.QuotaAdmission == nil || len(stored.QuotaAdmission.Charges) == 0 {
		t.Fatalf("stored quota admission = %+v", stored.QuotaAdmission)
	}
	tokenCharge := stored.QuotaAdmission.Charges[0]
	if tokenCharge.Events != 2 || tokenCharge.UncompressedBytes != wantBytes {
		t.Fatalf("token charge = %+v, want 2 events and %d bytes", tokenCharge, wantBytes)
	}
}
