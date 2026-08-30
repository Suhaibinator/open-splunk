// Package featureops defines a bounded, identity-free operational event shape
// shared by optional feature services. Events cannot carry queries, URLs,
// secrets, tenant IDs, job IDs, or other unbounded labels.
package featureops

import (
	"math"
	"sync"
)

// Feature is a fixed-cardinality operational subsystem.
type Feature uint8

const (
	FeatureInvalid Feature = iota
	FeatureDurableArtifacts
	FeatureScheduledReports
	FeatureAlerts
	featureLimit
)

// Operation is a fixed-cardinality lifecycle action.
type Operation uint8

const (
	OperationInvalid Operation = iota
	OperationAdmission
	OperationShare
	OperationRetentionChange
	OperationReconciliation
	OperationCleanup
	OperationScheduleClaim
	OperationRunOutcome
	OperationLifecycle
	OperationEvaluation
	OperationDelivery
	OperationRecovery
	OperationCreate
	OperationUpdate
	OperationEnable
	OperationDisable
	OperationDelete
	OperationSecretRotation
	OperationTestDelivery
	operationLimit
)

// Outcome is a fixed-cardinality result suitable for aggregate counters.
type Outcome uint8

const (
	OutcomeInvalid Outcome = iota
	OutcomeSucceeded
	OutcomeFailed
	OutcomeCapacityRejected
	OutcomeConflict
	OutcomeSkipped
	OutcomeTriggered
	OutcomeNotTriggered
	OutcomeIndeterminate
	OutcomeDelivered
	OutcomeSubmitted
	OutcomeCanceled
	OutcomeExpired
	OutcomeInterrupted
	OutcomeUnknown
	outcomeLimit
)

// Event is deliberately numeric and bounded. Items and Bytes are aggregate
// quantities for one operation, never identifiers or caller-controlled labels.
type Event struct {
	Feature   Feature
	Operation Operation
	Outcome   Outcome
	Items     uint64
	Bytes     uint64
}

// Valid reports whether an event belongs to the closed taxonomy.
func (event Event) Valid() bool {
	return event.Feature > FeatureInvalid && event.Feature < featureLimit &&
		event.Operation > OperationInvalid && event.Operation < operationLimit &&
		event.Outcome > OutcomeInvalid && event.Outcome < outcomeLimit
}

// Observer accepts one already-sanitized aggregate operational event.
type Observer interface {
	Observe(Event)
}

// Emit validates an event and isolates feature operations from observer
// failures. Observability must never change a committed feature outcome.
func Emit(observer Observer, event Event) {
	if observer == nil || !event.Valid() {
		return
	}
	defer func() { _ = recover() }()
	observer.Observe(event)
}

// Counter is one detached aggregate cell.
type Counter struct {
	Events uint64
	Items  uint64
	Bytes  uint64
}

// Snapshot is a bounded detached metrics projection.
type Snapshot struct {
	counters [featureLimit][operationLimit][outcomeLimit]Counter
}

// Counter returns one aggregate cell. Invalid taxonomy values return zero.
func (snapshot Snapshot) Counter(feature Feature, operation Operation, outcome Outcome) Counter {
	if !(Event{Feature: feature, Operation: operation, Outcome: outcome}).Valid() {
		return Counter{}
	}
	return snapshot.counters[feature][operation][outcome]
}

// Metrics implements Observer with fixed-size saturating aggregate counters.
type Metrics struct {
	mu       sync.Mutex
	counters [featureLimit][operationLimit][outcomeLimit]Counter
}

// NewMetrics constructs an empty aggregate observer.
func NewMetrics() *Metrics { return &Metrics{} }

// Observe records one valid event. Invalid events are ignored.
func (metrics *Metrics) Observe(event Event) {
	if metrics == nil || !event.Valid() {
		return
	}
	metrics.mu.Lock()
	defer metrics.mu.Unlock()
	counter := &metrics.counters[event.Feature][event.Operation][event.Outcome]
	counter.Events = saturatingAdd(counter.Events, 1)
	counter.Items = saturatingAdd(counter.Items, event.Items)
	counter.Bytes = saturatingAdd(counter.Bytes, event.Bytes)
}

// Snapshot returns a coherent detached copy.
func (metrics *Metrics) Snapshot() Snapshot {
	if metrics == nil {
		return Snapshot{}
	}
	metrics.mu.Lock()
	defer metrics.mu.Unlock()
	return Snapshot{counters: metrics.counters}
}

func saturatingAdd(current, increment uint64) uint64 {
	if math.MaxUint64-current < increment {
		return math.MaxUint64
	}
	return current + increment
}
