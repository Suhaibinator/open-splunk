package main

import (
	"slices"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/featureops"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
)

func TestRuntimeFeatureOperationsRecordsMetricsAndSafeFixedFields(t *testing.T) {
	t.Parallel()
	core, logs := observer.New(zapcore.InfoLevel)
	operations := newRuntimeFeatureOperations(zap.New(core))
	event := featureops.Event{
		Feature: featureops.FeatureAlerts, Operation: featureops.OperationDelivery,
		Outcome: featureops.OutcomeDelivered, Items: 2, Bytes: 3,
	}
	operations.Observe(event)

	counter := operations.Snapshot().Counter(event.Feature, event.Operation, event.Outcome)
	if counter != (featureops.Counter{Events: 1, Items: 2, Bytes: 3}) {
		t.Fatalf("counter = %#v", counter)
	}
	entries := logs.All()
	if len(entries) != 1 || entries[0].Message != "feature operation" {
		t.Fatalf("logs = %#v", entries)
	}
	fields := entries[0].ContextMap()
	keys := make([]string, 0, len(fields))
	for key := range fields {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	wantKeys := []string{"bytes", "feature", "items", "operation", "outcome"}
	if !slices.Equal(keys, wantKeys) {
		t.Fatalf("log keys = %v, want %v", keys, wantKeys)
	}
	for key, value := range fields {
		switch value.(type) {
		case uint8, uint64:
		default:
			t.Fatalf("log field %q has nonnumeric value type %T", key, value)
		}
	}
}

func TestRuntimeFeatureOperationsIgnoresInvalidEvent(t *testing.T) {
	t.Parallel()
	core, logs := observer.New(zapcore.InfoLevel)
	operations := newRuntimeFeatureOperations(zap.New(core))
	operations.Observe(featureops.Event{Feature: featureops.FeatureAlerts})
	if logs.Len() != 0 {
		t.Fatalf("invalid event logs = %d", logs.Len())
	}
	if counter := operations.Snapshot().Counter(
		featureops.FeatureAlerts,
		featureops.OperationDelivery,
		featureops.OutcomeDelivered,
	); counter != (featureops.Counter{}) {
		t.Fatalf("invalid event counter = %#v", counter)
	}
}

type recordingFeatureObserver struct {
	events []featureops.Event
}

func (observer *recordingFeatureObserver) Observe(event featureops.Event) {
	observer.events = append(observer.events, event)
}

func TestRuntimeFeatureOperationsForwardsSafeEventToDurableAuditor(t *testing.T) {
	t.Parallel()
	operations := newRuntimeFeatureOperations(zap.NewNop())
	auditor := &recordingFeatureObserver{}
	operations.SetAuditor(auditor)
	event := featureops.Event{
		Feature: featureops.FeatureScheduledReports, Operation: featureops.OperationScheduleClaim,
		Outcome: featureops.OutcomeSucceeded, Items: 1,
	}
	operations.Observe(event)
	if !slices.Equal(auditor.events, []featureops.Event{event}) {
		t.Fatalf("audited events = %#v", auditor.events)
	}
}
