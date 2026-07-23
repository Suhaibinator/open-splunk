package searchjobs

// PreviewSnapshot is one detached, coherent view of a running or completed
// search. Job, including its schema, and Rows are captured in the same manager
// entry critical section. Revision changes only when the result schema,
// retained row prefix, or completeness changes; unrelated job progress does
// not republish identical preview rows. It is monotonic for the lifetime of a
// retained job.
//
// Truncated reports that the returned rows are a prefix rather than the whole
// logical result: either more rows are currently retained than fit the
// requested row/byte bounds, or the executor crossed the manager's retained
// row limit.
type PreviewSnapshot struct {
	Job       Job
	Rows      []ResultRow
	Revision  uint64
	Truncated bool
}

// MaximumPreviewRows reports the largest row prefix accepted by PreviewFor.
// It lets transports advertise a limit that cannot exceed the backing result
// manager's configured page bound.
func (manager *Manager) MaximumPreviewRows() uint32 {
	if manager == nil || manager.maxPageSize <= 0 {
		return 0
	}
	if uint64(manager.maxPageSize) > uint64(^uint32(0)) {
		return ^uint32(0)
	}
	return uint32(manager.maxPageSize)
}

// PreviewFor returns a bounded live result prefix only when access owns the
// job. Limit must be positive and no greater than the manager's configured
// maximum page size. The schema and rows together are bounded by MaxPageBytes;
// if the next row does not fit, the snapshot still returns its schema and marks
// itself truncated.
//
// Running jobs become previewable after their executor emits a valid schema.
// Completed jobs retain the same preview contract. Queued, parsing, and
// planning jobs return ErrResultsNotReady; failed and canceled jobs return
// ErrResultsUnavailable; expired jobs return ErrExpired.
func (manager *Manager) PreviewFor(access AccessScope, id string, limit int) (PreviewSnapshot, error) {
	return manager.PreviewForBytes(access, id, limit, manager.maxPageBytes)
}

// PreviewForBytes applies an additional caller-owned byte ceiling before any
// result rows are detached. Transports use it to avoid cloning a page that is
// known to exceed their frame or replay budget. A ceiling smaller than the
// schema returns a useful schema-only, truncated preview.
func (manager *Manager) PreviewForBytes(
	access AccessScope,
	id string,
	limit int,
	maximumBytes uint64,
) (PreviewSnapshot, error) {
	if limit <= 0 || limit > manager.maxPageSize {
		return PreviewSnapshot{}, ErrPageSize
	}
	if maximumBytes == 0 {
		return PreviewSnapshot{}, ErrByteLimit
	}
	if !validAccessScope(access) {
		return PreviewSnapshot{}, ErrNotFound
	}

	manager.acquireRead()
	defer manager.releaseRead()

	// Retain manager.mu until entry.mu is acquired so tombstone removal is
	// ordered with this read. This follows the manager -> entry lock order used
	// by result-lease admission while keeping the entry lock hold bounded.
	manager.mu.RLock()
	entry := manager.jobs[id]
	if entry == nil {
		manager.mu.RUnlock()
		return PreviewSnapshot{}, ErrNotFound
	}
	entry.mu.Lock()
	manager.mu.RUnlock()

	if entry.job.TenantID != access.TenantID || entry.job.OwnerID != access.OwnerID {
		entry.mu.Unlock()
		return PreviewSnapshot{}, ErrNotFound
	}
	// Resolve expiry only after acquiring entry.mu. A reader delayed behind a
	// result update must not use a stale pre-wait clock sample.
	now := manager.nowUTC()
	if canExpireLocked(entry, now) {
		manager.expireLocked(entry, now)
	}

	switch entry.job.State {
	case StateExpired:
		entry.mu.Unlock()
		return PreviewSnapshot{}, ErrExpired
	case StateFailed, StateCanceled:
		entry.mu.Unlock()
		return PreviewSnapshot{}, ErrResultsUnavailable
	case StateCompleted:
		if entry.resultSchema == nil || entry.job.Schema == nil || entry.resultGeneration == 0 ||
			uint64(len(entry.rows)) != entry.job.RowCount {
			entry.mu.Unlock()
			return PreviewSnapshot{}, ErrResultsUnavailable
		}
	case StateRunning:
		if entry.resultSchema == nil || entry.job.Schema == nil {
			entry.mu.Unlock()
			return PreviewSnapshot{}, ErrResultsNotReady
		}
		if uint64(len(entry.rows)) != entry.job.RowCount {
			entry.mu.Unlock()
			return PreviewSnapshot{}, ErrResultsUnavailable
		}
	default:
		entry.mu.Unlock()
		return PreviewSnapshot{}, ErrResultsNotReady
	}

	if entry.schemaBytes > manager.maxPageBytes {
		entry.mu.Unlock()
		return PreviewSnapshot{}, ErrByteLimit
	}
	end := boundedResultRowEnd(entry.rows, 0, limit, entry.schemaBytes, min(maximumBytes, manager.maxPageBytes))

	// Copy only immutable references and scalar fields while holding entry.mu.
	// The schema, scope slices, strings, and retained row payloads are never
	// mutated in place, so their deep copies can be made after releasing the
	// writer lock while preserving this exact revision.
	jobSource := entry.job
	revision := entry.resultRevision
	truncated := entry.job.ResultsTruncated || end < len(entry.rows)
	rows := entry.rows[:end:end]
	entry.mu.Unlock()

	return PreviewSnapshot{
		Job:       cloneJob(jobSource),
		Rows:      cloneRows(rows),
		Revision:  revision,
		Truncated: truncated,
	}, nil
}
