package searchjobs

import (
	"context"
	"errors"
	"slices"
	"sync"
	"time"
)

var (
	// ErrResultLeaseClosed means Next was called after the immutable result
	// lease was explicitly closed or its acquisition context was canceled.
	ErrResultLeaseClosed = errors.New("search result lease is closed")
)

// ResultLease is a pinned, immutable view of one completed result generation.
// It is a forward-only iterator. Concurrent calls to Next are serialized, so
// each retained row is returned at most once across all callers of one lease.
//
// The lease shares the pinned snapshot's immutable schema; Schema returns a
// detached copy. Next copies only the row's Value slice: Value payload fields
// are private and immutable, and every accessor which exposes bytes, lists, or
// objects returns detached data.
// Callers must close a lease. Canceling the context passed to
// AcquireResultsFor also closes it as a leak-safe fallback.
//
// The concrete pin token is intentionally private and cannot be copied by
// consumers. Copying this interface is safe; all copies refer to the same
// idempotently closed lease.
type ResultLease interface {
	Schema() Schema
	RowCount() uint64
	// RowCountExact reports whether RowCount is known before iteration. Retained
	// in-memory generations always have an exact count, including empty ones.
	RowCountExact() bool
	// ResultsTruncated reports whether this immutable retained snapshot ended
	// at the manager's row boundary rather than the executor's natural end.
	ResultsTruncated() bool
	Generation() uint64
	Next(context.Context) (ResultRow, bool, error)
	Close() error
}

type resultLease struct {
	manager    *Manager
	entry      *jobEntry
	schema     *Schema
	rowCount   uint64
	truncated  bool
	generation uint64

	mu          sync.Mutex
	closeOnce   sync.Once
	next        uint64
	closed      bool
	stopContext func() bool
}

var _ ResultLease = (*resultLease)(nil)

// AcquireResultsFor pins a completed, unexpired immutable result snapshot
// owned by access. The returned lease is bound to ctx and must also be closed
// explicitly when iteration ends.
func (manager *Manager) AcquireResultsFor(ctx context.Context, access AccessScope, id string) (ResultLease, error) {
	if ctx == nil {
		return nil, errors.New("acquire search results: context is nil")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	// Holding manager.mu until entry.mu is acquired makes admission atomic
	// with manager shutdown and tombstone removal. This follows the existing
	// manager -> entry -> budget lock order.
	manager.mu.RLock()
	if manager.closed {
		manager.mu.RUnlock()
		return nil, ErrClosed
	}
	entry := manager.jobs[id]
	if entry == nil {
		manager.mu.RUnlock()
		return nil, ErrNotFound
	}
	entry.mu.Lock()
	manager.mu.RUnlock()

	if err := ctx.Err(); err != nil {
		entry.mu.Unlock()
		return nil, err
	}
	if entry.job.TenantID != access.TenantID || entry.job.OwnerID != access.OwnerID {
		entry.mu.Unlock()
		return nil, ErrNotFound
	}
	// Resolve expiry only after acquiring the entry lock. A reader that waited
	// behind another entry operation must not use a pre-wait clock sample to
	// admit a lease beyond the retention boundary.
	now := manager.nowUTC()
	if canExpireLocked(entry, now) {
		manager.expireLocked(entry, now)
	}
	switch entry.job.State {
	case StateExpired:
		entry.mu.Unlock()
		return nil, ErrExpired
	case StateFailed, StateCanceled:
		entry.mu.Unlock()
		return nil, ErrResultsUnavailable
	case StateCompleted:
		// Continue below.
	default:
		entry.mu.Unlock()
		return nil, ErrResultsNotReady
	}
	if entry.resultSchema == nil || entry.resultGeneration == 0 || uint64(len(entry.rows)) != entry.job.RowCount {
		entry.mu.Unlock()
		return nil, ErrResultsUnavailable
	}
	if err := manager.reserveResultLeaseLocked(entry); err != nil {
		entry.mu.Unlock()
		return nil, err
	}

	lease := &resultLease{
		manager:    manager,
		entry:      entry,
		schema:     entry.resultSchema,
		rowCount:   uint64(len(entry.rows)),
		truncated:  entry.job.ResultsTruncated,
		generation: entry.resultGeneration,
	}
	entry.mu.Unlock()

	// Background contexts have no cancellation signal and therefore need no
	// watcher. For cancelable contexts, context.AfterFunc has no dedicated
	// goroutine while idle and is stopped on explicit Close.
	if ctx.Done() != nil {
		lease.bindContext(ctx)
	}
	if err := ctx.Err(); err != nil {
		_ = lease.Close()
		return nil, err
	}
	return lease, nil
}

// Schema returns a detached copy of the stable schema captured at acquisition.
func (lease *resultLease) Schema() Schema {
	return cloneSchema(*lease.schema)
}

// RowCount returns the exact number of rows in the pinned generation.
func (lease *resultLease) RowCount() uint64 {
	return lease.rowCount
}

// RowCountExact reports that retained generations capture their final count
// atomically at acquisition.
func (lease *resultLease) RowCountExact() bool {
	return true
}

// ResultsTruncated returns the completeness flag captured atomically with the
// immutable result generation at acquisition.
func (lease *resultLease) ResultsTruncated() bool {
	return lease.truncated
}

// Generation returns the manager-local immutable result generation.
func (lease *resultLease) Generation() uint64 {
	return lease.generation
}

// Next returns the next detached row. ok is false at the end of the snapshot.
// A canceled call context does not consume a row.
func (lease *resultLease) Next(ctx context.Context) (row ResultRow, ok bool, err error) {
	if ctx == nil {
		return ResultRow{}, false, errors.New("read search result lease: context is nil")
	}
	lease.mu.Lock()
	defer lease.mu.Unlock()
	if lease.closed {
		return ResultRow{}, false, ErrResultLeaseClosed
	}
	if err := ctx.Err(); err != nil {
		return ResultRow{}, false, err
	}
	if lease.next >= lease.rowCount {
		return ResultRow{}, false, nil
	}

	// The entry lock makes the storage lifetime relationship explicit. A pin
	// prevents reclamation, and completed result rows never mutate.
	lease.entry.mu.RLock()
	if lease.next >= uint64(len(lease.entry.rows)) {
		lease.entry.mu.RUnlock()
		return ResultRow{}, false, ErrResultsUnavailable
	}
	source := lease.entry.rows[int(lease.next)]
	row = ResultRow{Ordinal: source.Ordinal, Values: slices.Clone(source.Values)}
	lease.entry.mu.RUnlock()
	if err := ctx.Err(); err != nil {
		return ResultRow{}, false, err
	}
	lease.next++
	return row, true, nil
}

// Close releases this lease's result pin. It is safe to call concurrently and
// more than once.
func (lease *resultLease) Close() error {
	lease.closeOnce.Do(func() {
		lease.mu.Lock()
		lease.closed = true
		stopContext := lease.stopContext
		lease.stopContext = nil
		lease.mu.Unlock()

		// Stop outside lease.mu because an already-running context callback may
		// be waiting in this same closeOnce operation. The stop function does
		// not wait for an already-started callback.
		if stopContext != nil {
			stopContext()
		}
		lease.manager.releaseResultPin(lease.entry)
	})
	return nil
}

func (lease *resultLease) bindContext(ctx context.Context) {
	stop := context.AfterFunc(ctx, func() { _ = lease.Close() })
	lease.mu.Lock()
	if lease.closed {
		lease.mu.Unlock()
		stop()
		return
	}
	lease.stopContext = stop
	lease.mu.Unlock()
}

func (manager *Manager) releaseResultPin(entry *jobEntry) {
	entry.mu.Lock()
	// Sample after acquiring the entry lock so a delayed final close cannot
	// miss a tombstone deadline crossed while it waited.
	now := manager.nowUTC()
	if entry.resultPins == 0 {
		entry.mu.Unlock()
		return
	}
	entry.resultPins--
	manager.budgetMu.Lock()
	if manager.activeResultLeases > 0 {
		manager.activeResultLeases--
	}
	manager.budgetMu.Unlock()
	removeDue := false
	if entry.resultPins == 0 && entry.job.State == StateExpired {
		manager.clearResultsLocked(entry)
		removeDue = !entry.expiredAt.Add(manager.expiredRetention).After(now)
	}
	entry.mu.Unlock()
	if removeDue {
		manager.removeExpiredEntry(entry, now)
	}
}

// reserveResultLeaseLocked atomically reserves the per-job and manager-wide
// lease quotas before a lease object or cancellation hook is allocated. The
// caller holds entry.mu; budgetMu is never held while acquiring an entry lock.
func (manager *Manager) reserveResultLeaseLocked(entry *jobEntry) error {
	if entry.resultPins >= manager.maxResultLeasesPerJob {
		return ErrCapacity
	}
	manager.budgetMu.Lock()
	defer manager.budgetMu.Unlock()
	if manager.activeResultLeases >= manager.maxResultLeases {
		return ErrCapacity
	}
	entry.resultPins++
	manager.activeResultLeases++
	return nil
}

func (manager *Manager) removeExpiredEntry(entry *jobEntry, now time.Time) {
	manager.mu.Lock()
	if manager.jobs[entry.job.ID] != entry {
		manager.mu.Unlock()
		return
	}
	entry.mu.Lock()
	if entry.job.State != StateExpired || entry.resultPins != 0 || entry.expiredAt.Add(manager.expiredRetention).After(now) {
		entry.mu.Unlock()
		manager.mu.Unlock()
		return
	}
	delete(manager.jobs, entry.job.ID)
	metadataBytes := entry.metadataBytes
	entry.mu.Unlock()
	manager.mu.Unlock()
	manager.releaseMetadata(metadataBytes)
}
