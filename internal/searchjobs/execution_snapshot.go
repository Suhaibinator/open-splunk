package searchjobs

import (
	"context"
	"errors"
	"strings"
	"time"
)

// ExecutionSnapshot is the immutable execution scope retained for a
// successfully completed search. It is detached from Manager storage and does
// not pin the job's retained result rows or schema.
type ExecutionSnapshot struct {
	ID               string
	OwnerID          string
	TenantID         string
	SPL              string
	EffectiveIndexes []string
	Earliest         time.Time
	Latest           time.Time
	IndexTimeCutoff  time.Time
	VisibilityCutoff uint64
	FinishedAt       time.Time
	ExpiresAt        time.Time
}

// CompletedExecutionSnapshotFor returns the detached execution scope of a
// completed, unexpired job owned by access. Unlike AcquireResultsFor, this
// metadata-only read does not consume result-lease capacity or extend the
// lifetime of retained result storage.
func (manager *Manager) CompletedExecutionSnapshotFor(ctx context.Context, access AccessScope, id string) (ExecutionSnapshot, error) {
	if ctx == nil {
		return ExecutionSnapshot{}, errors.New("read completed search execution snapshot: context is nil")
	}
	if err := ctx.Err(); err != nil {
		return ExecutionSnapshot{}, err
	}

	// Retain manager.mu until entry.mu is acquired so shutdown and tombstone
	// removal are ordered with this read. This is the manager -> entry lock
	// order also used by result-lease admission.
	manager.mu.RLock()
	if manager.closed {
		manager.mu.RUnlock()
		return ExecutionSnapshot{}, ErrClosed
	}
	entry := manager.jobs[id]
	if entry == nil {
		manager.mu.RUnlock()
		return ExecutionSnapshot{}, ErrNotFound
	}
	entry.mu.Lock()
	manager.mu.RUnlock()
	defer entry.mu.Unlock()

	if err := ctx.Err(); err != nil {
		return ExecutionSnapshot{}, err
	}
	if entry.job.TenantID != access.TenantID || entry.job.OwnerID != access.OwnerID {
		return ExecutionSnapshot{}, ErrNotFound
	}
	now := manager.nowUTC()
	if canExpireLocked(entry, now) {
		manager.expireLocked(entry, now)
	}
	switch entry.job.State {
	case StateCompleted:
		// Continue below.
	case StateExpired:
		return ExecutionSnapshot{}, ErrExpired
	case StateFailed, StateCanceled:
		return ExecutionSnapshot{}, ErrResultsUnavailable
	default:
		return ExecutionSnapshot{}, ErrResultsNotReady
	}

	snapshot := ExecutionSnapshot{
		ID:               strings.Clone(entry.job.ID),
		OwnerID:          strings.Clone(entry.job.OwnerID),
		TenantID:         strings.Clone(entry.job.TenantID),
		SPL:              strings.Clone(entry.job.SPL),
		EffectiveIndexes: cloneStrings(entry.job.EffectiveIndexes),
		Earliest:         entry.job.Earliest,
		Latest:           entry.job.Latest,
		IndexTimeCutoff:  entry.job.IndexTimeCutoff,
		VisibilityCutoff: entry.job.VisibilityCutoff,
		FinishedAt:       entry.job.FinishedAt,
		ExpiresAt:        entry.job.ExpiresAt,
	}
	if err := ctx.Err(); err != nil {
		return ExecutionSnapshot{}, err
	}
	return snapshot, nil
}
