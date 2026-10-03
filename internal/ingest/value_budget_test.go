package ingest

import (
	"context"
	"errors"
	"fmt"
	"testing"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"google.golang.org/protobuf/proto"
)

func TestTypedValueNodeBudget(t *testing.T) {
	limit := int(HardMaxEventValueNodes)
	tests := []struct {
		name   string
		fields func(int) *opensplunk.TypedObject
	}{
		{
			name: "flat list",
			fields: func(nodes int) *opensplunk.TypedObject {
				return object(valueBudgetField("items", valueBudgetList(nodes-1)))
			},
		},
		{
			name: "sibling lists",
			fields: func(nodes int) *opensplunk.TypedObject {
				return object(
					valueBudgetField("left", valueBudgetList(nodes/2-1)),
					valueBudgetField("right", valueBudgetList(nodes-nodes/2-1)),
				)
			},
		},
		{
			name: "nested lists",
			fields: func(nodes int) *opensplunk.TypedObject {
				return object(valueBudgetField("items", valueBudgetListOf(
					valueBudgetList(nodes/2-2), valueBudgetList(nodes-nodes/2-1),
				)))
			},
		},
		{
			name: "object containing list",
			fields: func(nodes int) *opensplunk.TypedObject {
				return object(objectField("nested", object(valueBudgetField("items", valueBudgetList(nodes-2)))))
			},
		},
		{
			name: "list containing object and list",
			fields: func(nodes int) *opensplunk.TypedObject {
				nested := objectField("nested", object(valueBudgetField("items", valueBudgetList(nodes-3))))
				return object(valueBudgetField("items", valueBudgetListOf(nested.Value)))
			},
		},
		{
			name: "empty lists",
			fields: func(nodes int) *opensplunk.TypedObject {
				values := make([]*opensplunk.TypedValue, nodes-1)
				for i := range values {
					values[i] = valueBudgetList(0)
				}
				return object(valueBudgetField("items", valueBudgetListOf(values...)))
			},
		},
		{
			name: "empty objects",
			fields: func(nodes int) *opensplunk.TypedObject {
				values := make([]*opensplunk.TypedValue, nodes-1)
				for i := range values {
					values[i] = objectField("unused", object()).Value
				}
				return object(valueBudgetField("items", valueBudgetListOf(values...)))
			},
		},
		{
			name: "scalar after list",
			fields: func(nodes int) *opensplunk.TypedObject {
				return object(valueBudgetField("items", valueBudgetList(nodes-2)), stringField("last", "control"))
			},
		},
	}
	validator := newTestValidator(t, DefaultLimits())
	for _, test := range tests {
		for _, nodes := range []int{limit, limit + 1} {
			t.Run(fmt.Sprintf("%s/%d", test.name, nodes), func(t *testing.T) {
				event := validTestEvent("event-budget", "main")
				event.Fields = test.fields(nodes)
				encoded, err := proto.Marshal(event)
				if err != nil {
					t.Fatal(err)
				}
				if uint64(len(encoded)) >= HardMaxEventBytes {
					t.Fatal("test must stay below the independent encoded-byte limit")
				}
				decoded := new(opensplunk.LogEvent)
				if err := proto.Unmarshal(encoded, decoded); err != nil {
					t.Fatal(err)
				}
				stored, rejection := validator.ValidateAndNormalizeEvent(decoded, EventContext{ReceivedAt: validationTestNow})
				if nodes > limit {
					assertValueBudgetRejection(t, rejection)
					if stored != nil {
						t.Fatal("oversized tree returned a normalized event")
					}
				} else if rejection != nil || stored == nil || stored.Event == decoded || !proto.Equal(stored.Event, decoded) {
					t.Fatalf("boundary event was rejected or changed: %v", rejection)
				}
				if !proto.Equal(decoded, event) {
					t.Fatal("validation mutated input")
				}
			})
		}
	}
}

func TestTypedValueNodeBudgetMergedWireLists(t *testing.T) {
	validator := newTestValidator(t, DefaultLimits())
	for _, nodes := range []int{int(HardMaxEventValueNodes), int(HardMaxEventValueNodes) + 1} {
		t.Run(fmt.Sprint(nodes), func(t *testing.T) {
			// Repeated occurrences of a message-valued oneof merge their repeated
			// children. The budget must apply to the final decoded tree.
			first, err := proto.Marshal(valueBudgetList(nodes / 2))
			if err != nil {
				t.Fatal(err)
			}
			second, err := proto.Marshal(valueBudgetList(nodes - nodes/2 - 1))
			if err != nil {
				t.Fatal(err)
			}
			value := new(opensplunk.TypedValue)
			if err := proto.Unmarshal(append(first, second...), value); err != nil {
				t.Fatal(err)
			}
			if len(value.GetListValue().GetValues()) != nodes-1 {
				t.Fatal("wire control did not merge the list occurrences")
			}
			event := validTestEvent("event-merged", "main")
			event.Fields = object(valueBudgetField("items", value))
			_, rejection := validator.ValidateAndNormalizeEvent(event, EventContext{ReceivedAt: validationTestNow})
			if nodes > int(HardMaxEventValueNodes) {
				assertValueBudgetRejection(t, rejection)
			} else if rejection != nil {
				t.Fatalf("merged boundary event rejected: %v", rejection)
			}
		})
	}
}

func TestTypedValueNodeBudgetPreservesFieldAndDepthLimits(t *testing.T) {
	limits := DefaultLimits()
	limits.MaxFields = 1
	validator := newTestValidator(t, limits)
	event := validTestEvent("event-one-field", "main")
	event.Fields = object(valueBudgetField("items", valueBudgetList(int(HardMaxEventValueNodes)-1)))
	if _, rejection := validator.ValidateAndNormalizeEvent(event, EventContext{ReceivedAt: validationTestNow}); rejection != nil {
		t.Fatalf("list elements incorrectly consumed object-field limit: %v", rejection)
	}
	event.Fields.Fields = append(event.Fields.Fields, stringField("second", "value"))
	_, rejection := validator.ValidateAndNormalizeEvent(event, EventContext{ReceivedAt: validationTestNow})
	assertEventRejectionCode(t, rejection, opensplunk.EventRejectionCode_EVENT_REJECTION_CODE_TOO_MANY_FIELDS)

	for _, containers := range []int{int(limits.MaxNestingDepth) - 1, int(limits.MaxNestingDepth)} {
		value := valueBudgetList(0)
		for i := 1; i < containers; i++ {
			value = valueBudgetListOf(value)
		}
		event.Fields = object(valueBudgetField("nested", value))
		_, rejection := validator.ValidateAndNormalizeEvent(event, EventContext{ReceivedAt: validationTestNow})
		if containers == int(limits.MaxNestingDepth) {
			assertEventRejectionCode(t, rejection, opensplunk.EventRejectionCode_EVENT_REJECTION_CODE_NESTING_TOO_DEEP)
		} else if rejection != nil {
			t.Fatalf("existing depth boundary rejected: %v", rejection)
		}
	}
}

func TestTypedValueNodeBudgetBoundsRejectionAllocations(t *testing.T) {
	validator := newTestValidator(t, DefaultLimits())
	for _, count := range []int{int(HardMaxEventValueNodes), 2 * int(HardMaxEventValueNodes)} {
		event := validTestEvent("event-wide", "main")
		// A sensitive field would be replaced during redaction, but its source
		// tree must still be rejected before the independent clone is allocated.
		event.Fields = object(valueBudgetField("password", valueBudgetList(count)))
		size := uint64(proto.Size(event))
		var rejection *EventError
		allocs := testing.AllocsPerRun(10, func() {
			_, rejection = validator.validateAndNormalizeEventWithSize(event, EventContext{ReceivedAt: validationTestNow}, size)
		})
		assertValueBudgetRejection(t, rejection)
		if allocs > 32 {
			t.Fatalf("%d values allocated %.0f objects during rejection; want at most 32", count, allocs)
		}
	}
}

func TestCollectRejectsValueBudgetBeforeStore(t *testing.T) {
	var stored StoreBatch
	store := EventStoreFunc(func(_ context.Context, batch StoreBatch) (StoreResult, error) {
		stored = batch
		return StoreResult{Accepted: uint32(len(batch.Events)), CommittedAt: validationTestNow}, nil
	})
	harness := newServiceHarness(t, testServiceConfig(), staticTestAuthorizer(), store)
	stream := harness.stream(t, "Bearer good-token")
	sendHello(t, stream)
	_ = recvResponse(t, stream)
	invalid := validTestEvent("event-over-budget", "main")
	invalid.Fields = object(valueBudgetField("items", valueBudgetList(int(HardMaxEventValueNodes))))
	first := validTestEvent("event-first", "main")
	first.Fields = object(valueBudgetField("items", valueBudgetList(int(HardMaxEventValueNodes)-1)))
	second := validTestEvent("event-second", "main")
	second.Fields = first.Fields
	batch := validTestBatch("collector-a", "batch-budget", 1, invalid, first, second)
	if err := stream.Send(batchRequest(2, batch)); err != nil {
		t.Fatal(err)
	}
	ack := recvResponse(t, stream).GetBatchAck()
	if ack == nil || ack.GetAcceptedEventCount() != 2 || len(ack.GetRejectedEvents()) != 1 {
		t.Fatalf("mixed budget batch acknowledgment: %v", ack)
	}
	rejection := ack.GetRejectedEvents()[0]
	if rejection.GetEventIndex() != 0 || rejection.GetCode() != opensplunk.EventRejectionCode_EVENT_REJECTION_CODE_VALUE_INVALID ||
		len(rejection.GetViolations()) != 1 || rejection.GetViolations()[0].GetCode() != "too_many_values" {
		t.Fatalf("unexpected permanent rejection: %v", rejection)
	}
	if len(stored.Events) != 2 || stored.Events[0].Event.GetEventId() != first.GetEventId() || stored.Events[1].Event.GetEventId() != second.GetEventId() {
		t.Fatal("store did not receive exactly the valid events")
	}
}

func TestAdmissionPreparerRejectsValueBudget(t *testing.T) {
	store := &admissionTestStagingStore{}
	preparer := admissionTestPreparer(t, AdmissionConfig{}, store)
	event := validTestEvent("event-budget", "main")
	event.Fields = object(valueBudgetField("items", valueBudgetList(int(HardMaxEventValueNodes))))
	_, err := preparer.Prepare(admissionTestHECRequest(AdmissionEvent{Event: event}))
	var failure *AdmissionFailure
	if !errors.As(err, &failure) {
		t.Fatalf("Prepare() error = %v, want AdmissionFailure", err)
	}
	assertValueBudgetRejection(t, failure.Failure)
	if store.storeCalls != 0 || store.stageCalls != 0 {
		t.Fatal("invalid event reached persistence")
	}
}

func BenchmarkTypedValueNodeBudget(b *testing.B) {
	validator, err := NewValidator(DefaultLimits(), RedactionPolicy{})
	if err != nil {
		b.Fatal(err)
	}
	for _, count := range []int{int(HardMaxEventValueNodes) - 1, int(HardMaxEventValueNodes)} {
		b.Run(fmt.Sprint(count), func(b *testing.B) {
			event := validTestEvent("event-benchmark", "main")
			event.Fields = object(valueBudgetField("items", valueBudgetList(count)))
			size := uint64(proto.Size(event))
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				_, rejection := validator.validateAndNormalizeEventWithSize(event, EventContext{ReceivedAt: validationTestNow}, size)
				if (rejection != nil) != (count == int(HardMaxEventValueNodes)) {
					b.Fatalf("unexpected rejection: %v", rejection)
				}
			}
		})
	}
}

func valueBudgetField(name string, value *opensplunk.TypedValue) *opensplunk.TypedObjectField {
	return &opensplunk.TypedObjectField{Name: name, Value: value}
}

func valueBudgetList(count int) *opensplunk.TypedValue {
	values := make([]*opensplunk.TypedValue, count)
	for i := range values {
		values[i] = &opensplunk.TypedValue{Kind: &opensplunk.TypedValue_BoolValue{BoolValue: false}}
	}
	return valueBudgetListOf(values...)
}

func valueBudgetListOf(values ...*opensplunk.TypedValue) *opensplunk.TypedValue {
	return &opensplunk.TypedValue{Kind: &opensplunk.TypedValue_ListValue{
		ListValue: &opensplunk.TypedValueList{Values: values},
	}}
}

func assertValueBudgetRejection(t *testing.T, rejection *EventError) {
	t.Helper()
	assertEventRejectionCode(t, rejection, opensplunk.EventRejectionCode_EVENT_REJECTION_CODE_VALUE_INVALID)
	if len(rejection.Violations) != 1 || rejection.Violations[0].GetCode() != "too_many_values" {
		t.Fatalf("unexpected node-budget violation: %v", rejection)
	}
}
