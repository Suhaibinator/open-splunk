package control

import (
	"fmt"
	"strings"
)

// AdmissionGate is a process-local fail-fast concurrency bound shared by all
// components constructed over one DB handle. It deliberately exposes no
// blocking acquisition operation.
type AdmissionGate struct {
	permits chan struct{}
}

// SharedAdmissionGate returns the unique named gate owned by db. Repeated
// construction with the same name must use the same capacity.
func (db *DB) SharedAdmissionGate(name string, capacity int) (*AdmissionGate, error) {
	if db == nil || db.sql == nil || strings.TrimSpace(name) == "" || capacity < 1 {
		return nil, fmt.Errorf("%w: invalid shared admission gate", ErrInvalidArgument)
	}
	db.sharedAdmissionMu.Lock()
	defer db.sharedAdmissionMu.Unlock()
	if existing := db.sharedAdmissionMap[name]; existing != nil {
		if cap(existing.permits) != capacity {
			return nil, fmt.Errorf("%w: shared admission gate capacity disagrees", ErrInvalidArgument)
		}
		return existing, nil
	}
	if db.sharedAdmissionMap == nil {
		db.sharedAdmissionMap = make(map[string]*AdmissionGate)
	}
	gate := &AdmissionGate{permits: make(chan struct{}, capacity)}
	db.sharedAdmissionMap[name] = gate
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
