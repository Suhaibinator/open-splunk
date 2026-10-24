package control

import (
	"fmt"
	"strings"
	"sync"
)

// AdmissionGate is a process-local fail-fast concurrency bound shared by all
// components constructed over one DB handle. It deliberately exposes no
// blocking acquisition operation.
type AdmissionGate struct {
	permits chan struct{}
}

// admissionGates holds the named gates of one database handle. Both the
// SQLite DB and PostgresDB embed one, so components built over either handle
// share the same fail-fast bounds.
type admissionGates struct {
	mu    sync.Mutex
	gates map[string]*AdmissionGate
}

// SharedAdmissionGate returns the unique named gate owned by db. Repeated
// construction with the same name must use the same capacity.
func (db *DB) SharedAdmissionGate(name string, capacity int) (*AdmissionGate, error) {
	if db == nil || db.sql == nil {
		return nil, fmt.Errorf("%w: invalid shared admission gate", ErrInvalidArgument)
	}
	return db.admission.shared(name, capacity)
}

// SharedAdmissionGate returns the unique named gate owned by db. Repeated
// construction with the same name must use the same capacity. The gate is a
// process-local semaphore and runs no SQL; two handles over one database,
// like two server processes, have independent gates.
func (db *PostgresDB) SharedAdmissionGate(name string, capacity int) (*AdmissionGate, error) {
	if db == nil || db.sql == nil {
		return nil, fmt.Errorf("%w: invalid shared admission gate", ErrInvalidArgument)
	}
	return db.admission.shared(name, capacity)
}

func (registry *admissionGates) shared(name string, capacity int) (*AdmissionGate, error) {
	if strings.TrimSpace(name) == "" || capacity < 1 {
		return nil, fmt.Errorf("%w: invalid shared admission gate", ErrInvalidArgument)
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	if existing := registry.gates[name]; existing != nil {
		if cap(existing.permits) != capacity {
			return nil, fmt.Errorf("%w: shared admission gate capacity disagrees", ErrInvalidArgument)
		}
		return existing, nil
	}
	if registry.gates == nil {
		registry.gates = make(map[string]*AdmissionGate)
	}
	gate := &AdmissionGate{permits: make(chan struct{}, capacity)}
	registry.gates[name] = gate
	return gate, nil
}

// TryAcquire acquires one permit without waiting.
func (gate *AdmissionGate) TryAcquire() bool {
	if gate == nil {
		return false
	}
	select {
	case gate.permits <- struct{}{}:
		return true
	default:
		return false
	}
}

// Release returns one previously acquired permit.
func (gate *AdmissionGate) Release() {
	<-gate.permits
}
