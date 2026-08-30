package featureops

import (
	"math"
	"reflect"
	"testing"
)

func TestMetricsRecordsBoundedAggregateEvents(t *testing.T) {
	metrics := NewMetrics()
	event := Event{
		Feature: FeatureDurableArtifacts, Operation: OperationCleanup,
		Outcome: OutcomeSucceeded, Items: 2, Bytes: 3,
	}
	metrics.Observe(event)
	metrics.Observe(event)
	got := metrics.Snapshot().Counter(
		FeatureDurableArtifacts,
		OperationCleanup,
		OutcomeSucceeded,
	)
	if got != (Counter{Events: 2, Items: 4, Bytes: 6}) {
		t.Fatalf("counter = %#v", got)
	}
	if got := metrics.Snapshot().Counter(FeatureInvalid, OperationCleanup, OutcomeSucceeded); got != (Counter{}) {
		t.Fatalf("invalid counter = %#v", got)
	}
}

func TestMetricsSaturatesAndEmitContainsObserverFailure(t *testing.T) {
	metrics := NewMetrics()
	metrics.Observe(Event{
		Feature: FeatureDurableArtifacts, Operation: OperationAdmission,
		Outcome: OutcomeSucceeded, Items: math.MaxUint64, Bytes: math.MaxUint64,
	})
	metrics.Observe(Event{
		Feature: FeatureDurableArtifacts, Operation: OperationAdmission,
		Outcome: OutcomeSucceeded, Items: 1, Bytes: 1,
	})
	got := metrics.Snapshot().Counter(
		FeatureDurableArtifacts,
		OperationAdmission,
		OutcomeSucceeded,
	)
	if got.Events != 2 || got.Items != math.MaxUint64 || got.Bytes != math.MaxUint64 {
		t.Fatalf("saturated counter = %#v", got)
	}
	Emit(panicObserver{}, Event{
		Feature: FeatureDurableArtifacts, Operation: OperationAdmission,
		Outcome: OutcomeFailed,
	})
}

func TestEventShapeCannotCarryUnboundedLabelsOrIdentities(t *testing.T) {
	typeOfEvent := reflect.TypeFor[Event]()
	for field := range typeOfEvent.Fields() {
		switch field.Type.Kind() {
		case reflect.String, reflect.Slice, reflect.Map, reflect.Pointer, reflect.Interface:
			t.Fatalf("event field %s can carry unbounded or identity data: %s", field.Name, field.Type)
		}
	}
}

type panicObserver struct{}

func (panicObserver) Observe(Event) { panic("observer failure") }
