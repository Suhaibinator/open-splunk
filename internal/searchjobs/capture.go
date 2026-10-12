package searchjobs

import (
	"context"
	"errors"
	"sync"

	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
)

// CaptureFactory stages the complete output independently of the bounded live
// preview. Staged rows must remain private until Publish succeeds.
type CaptureFactory interface {
	BeginCapture(context.Context, Job, clickhouse.CompiledQuery, uint64) (CaptureTransaction, error)
	AcquireCapture(context.Context, AccessScope, string) (ResultLease, error)
}

type boundedCaptureFactory interface {
	AcquireCaptureBounded(context.Context, AccessScope, string, func(uint64) (func(), bool)) (ResultLease, error)
}

func (manager *Manager) reserveCaptureMemory(bytes uint64) (func(), bool) {
	manager.budgetMu.Lock()
	maximum := manager.currentLimits().MaxTotalResultBytes
	if manager.retainedBytes > maximum || bytes > maximum-manager.retainedBytes {
		manager.budgetMu.Unlock()
		return nil, false
	}
	manager.retainedBytes += bytes
	manager.budgetMu.Unlock()
	var once sync.Once
	return func() {
		once.Do(func() { manager.budgetMu.Lock(); manager.retainedBytes -= bytes; manager.budgetMu.Unlock() })
	}, true
}

// CaptureTransaction is private immutable-result work, not public job state.
// Publish is called only after backend iteration/Close and authority validation.
// Abort is idempotent and must remove or durably queue every staged object.
type CaptureTransaction interface {
	SetSchema(Schema) error
	Append(context.Context, ResultRow) error
	Publish(context.Context, Job) error
	Abort(context.Context) error
	Limits() (rows, bytes uint64)
}

// CapturePublicationPreparer drains private uploads and builds their manifest
// before the manager takes its short job-state publication fence.
type CapturePublicationPreparer interface {
	PreparePublication(context.Context, Job) error
}

// PrivateResultSink marks the manager's private transaction. The unexported
// method prevents outside sinks from claiming the atomic publication contract.
type PrivateResultSink interface {
	ResultSink
	privateResultTransaction()
}

func (*resultSink) privateResultTransaction() {}

// CanStagePrivately is true only for a manager sink with a durable capture.
func CanStagePrivately(sink ResultSink) bool {
	for range 8 {
		if private, ok := sink.(*resultSink); ok {
			return private.capture != nil
		}
		wrapper, ok := sink.(interface{ UnderlyingResultSink() ResultSink })
		if !ok {
			return false
		}
		sink = wrapper.UnderlyingResultSink()
	}
	return false
}

func (sink *resultSink) addCaptureRowLocked(values []Value, bounds *TimeBucketBounds) error {
	schema := sink.entry.resultSchema
	if sink.atomicResult {
		schema = sink.atomicSchema
	}
	if schema == nil || len(values) != len(schema.Columns) {
		return sink.rememberLocked(ErrInvalidResult)
	}
	payload, retained, err := sink.measureRowCellsLocked(schema.Columns, values)
	if err != nil {
		return sink.rememberLocked(err)
	}
	payload, retained, err = measureTimeBucketBounds(schema.Columns, values, bounds, payload, retained)
	if err != nil {
		return sink.rememberLocked(err)
	}
	schemaBytes, err := retainedSchemaSize(*schema)
	if err != nil || schemaBytes > sink.manager.maxPageBytes || retained > sink.manager.maxPageBytes-schemaBytes {
		return sink.rememberLocked(ErrByteLimit)
	}
	rows, bytes := sink.capture.Limits()
	if sink.capturedRows >= rows {
		return sink.rememberLocked(ErrRowLimit)
	}
	if payload > bytes-sink.capturedBytes {
		return sink.rememberLocked(ErrByteLimit)
	}
	row := ResultRow{Ordinal: sink.capturedRows, Values: values, TimeBucket: cloneTimeBucketBounds(bounds)}
	if err := sink.capture.Append(sink.ctx, row); err != nil {
		return sink.rememberLocked(err)
	}
	sink.capturedRows++
	sink.capturedBytes += payload
	sink.entry.job.CaptureRows = sink.capturedRows
	sink.entry.job.CaptureBytes = sink.capturedBytes
	incrementJobVersion(&sink.entry.job)
	if !sink.atomicResult {
		sink.entry.job.RowCount = sink.capturedRows
		sink.entry.job.ResultBytes = sink.capturedBytes
		incrementJobVersion(&sink.entry.job)
	}
	if sink.previewStopped {
		return nil
	}
	preview := sink.entry.rows
	if sink.atomicResult {
		preview = sink.atomicRows
	}
	if uint64(len(preview)) >= sink.effectiveLimits().MaxResultRows {
		sink.previewStopped = true
		sink.entry.job.PreviewTruncated = true
		return nil
	}
	schemaBytes = sink.entry.schemaBytes
	if sink.atomicResult {
		schemaBytes = sink.atomicSchemaBytes
	}
	capacity, charge, pageBytes, err := sink.planRowGrowthLocked(preview, schemaBytes, retained)
	if err != nil {
		if errors.Is(err, ErrByteLimit) {
			sink.previewStopped = true
			sink.entry.job.PreviewTruncated = true
			return nil
		}
		return sink.rememberLocked(err)
	}
	if err := sink.manager.reserveRetainedLocked(sink.entry, charge, sink.effectiveLimits()); err != nil {
		if errors.Is(err, ErrByteLimit) || errors.Is(err, ErrCapacity) {
			sink.previewStopped = true
			sink.entry.job.PreviewTruncated = true
			return nil
		}
		return sink.rememberLocked(err)
	}
	if capacity > cap(preview) {
		grown := make([]ResultRow, len(preview), capacity)
		copy(grown, preview)
		preview = grown
	}
	row.Values = cloneValues(values)
	row.retainedBytes = pageBytes
	preview = append(preview, row)
	if sink.atomicResult {
		sink.atomicRows = preview
	} else {
		sink.entry.rows = preview
		incrementResultRevision(sink.entry)
	}
	return nil
}

func (manager *Manager) finishCaptured(entry *jobEntry, sink *resultSink) error {
	entry.mu.Lock()
	if entry.job.State != StateRunning || entry.ctx.Err() != nil {
		entry.mu.Unlock()
		return context.Canceled
	}
	schema := entry.resultSchema
	if sink.atomicResult {
		schema = sink.atomicSchema
	}
	if schema == nil {
		entry.mu.Unlock()
		return ErrInvalidResult
	}
	job := cloneJob(entry.job)
	job.Schema = schema
	job.RowCount = sink.capturedRows
	job.ResultBytes = sink.capturedBytes
	job.State = StateCompleted
	job.Version++
	job.CaptureComplete = true
	job.CaptureRows = sink.capturedRows
	job.CaptureBytes = sink.capturedBytes
	job.ResultsTruncated = false
	job.PreviewTruncated = sink.previewStopped
	job.FinishedAt = manager.nowUTC()
	job.ExpiresAt = job.FinishedAt.Add(entry.limits.ResultRetention)
	job.NearbyEventProvenance = sink.nearbyProvenance
	entry.mu.Unlock()
	if preparer, ok := sink.capture.(CapturePublicationPreparer); ok {
		if err := preparer.PreparePublication(sink.ctx, job); err != nil {
			return err
		}
	}
	entry.mu.Lock()
	if err := sink.ctx.Err(); err != nil {
		entry.mu.Unlock()
		return err
	}
	if entry.job.State != StateRunning || entry.ctx.Err() != nil {
		entry.mu.Unlock()
		return context.Canceled
	}
	if err := sink.capture.Publish(sink.ctx, job); err != nil {
		entry.mu.Unlock()
		return err
	}
	if sink.atomicResult {
		entry.resultSchema = sink.atomicSchema
		entry.schemaBytes = sink.atomicSchemaBytes
		entry.rows = sink.atomicRows
	}
	entry.job = job
	entry.resultGeneration = entry.generation
	entry.history = append(entry.history, StateCompleted)
	incrementResultRevision(entry)
	sink.atomicRows = nil
	sink.atomicSchema = nil
	terminal, finalize := manager.claimTerminalJournalLocked(entry)
	entry.mu.Unlock()
	entry.cancel()
	if finalize {
		manager.finalizeJournal(terminal)
	}
	return nil
}

func (manager *Manager) capturePage(ctx context.Context, access AccessScope, id string, limit int, token string) (ResultPage, error) {
	if err := manager.acquireReadContext(ctx); err != nil {
		return ResultPage{}, err
	}
	defer manager.releaseRead()
	lease, err := manager.AcquireResultsFor(ctx, access, id)
	if err != nil {
		return ResultPage{}, err
	}
	defer func() { _ = lease.Close() }()
	offset := uint64(0)
	if token != "" {
		cursor, err := decodeCursor(manager.cursorKey, token)
		if err != nil || cursor.Scope != manager.cursorScope || cursor.JobID != id || cursor.Generation != lease.Generation() {
			return ResultPage{}, ErrInvalidCursor
		}
		offset = cursor.Offset
	}
	seekable, ok := lease.(interface {
		Seek(context.Context, uint64) error
	})
	if !ok {
		return ResultPage{}, ErrResultsUnavailable
	}
	if err := seekable.Seek(ctx, offset); err != nil {
		return ResultPage{}, err
	}
	schema := lease.Schema()
	schemaBytes, err := retainedSchemaSize(schema)
	if err != nil || schemaBytes > manager.maxPageBytes {
		return ResultPage{}, ErrByteLimit
	}
	page := ResultPage{Schema: schema, TotalRows: lease.RowCount(), Generation: lease.Generation()}
	bytes := schemaBytes
	for len(page.Rows) < limit {
		row, present, err := lease.Next(ctx)
		if err != nil {
			return ResultPage{}, err
		}
		if !present {
			break
		}
		size := retainedResultRowBase
		for _, value := range row.Values {
			retained, err := value.RetainedSizeBytesContext(ctx)
			if err != nil {
				return ResultPage{}, err
			}
			size, err = checkedAdd(size, retained)
			if err != nil {
				return ResultPage{}, ErrByteLimit
			}
		}
		if size > manager.maxPageBytes-bytes {
			if len(page.Rows) == 0 {
				return ResultPage{}, ErrByteLimit
			}
			break
		}
		bytes += size
		page.Rows = append(page.Rows, row)
	}
	next := offset + uint64(len(page.Rows))
	page.Complete = next == page.TotalRows
	if !page.Complete {
		page.NextCursor, err = encodeCursor(manager.cursorKey, manager.cursorScope, id, page.Generation, next)
		if err != nil {
			return ResultPage{}, err
		}
	}
	return page, nil
}

// ResultsForContext preserves the authenticated actor and cancellation for
// object-backed paging. Transport handlers should prefer this capability.
func (manager *Manager) ResultsForContext(ctx context.Context, access AccessScope, id string, request PageRequest) (ResultPage, error) {
	if ctx == nil {
		return ResultPage{}, ErrInvalidCursor
	}
	if err := ctx.Err(); err != nil {
		return ResultPage{}, err
	}
	limit := request.Limit
	if limit == 0 {
		limit = manager.defaultPageSize
	}
	if limit < 0 || limit > manager.maxPageSize {
		return ResultPage{}, ErrPageSize
	}
	if !validAccessScope(access) {
		return ResultPage{}, ErrNotFound
	}
	entry := manager.lookup(id)
	if entry == nil || !entry.matches(access) {
		return ResultPage{}, ErrNotFound
	}
	if err := manager.authorizeEntry(ctx, access, entry); err != nil {
		return ResultPage{}, err
	}
	entry.mu.RLock()
	captured := entry.job.CaptureComplete
	entry.mu.RUnlock()
	if captured {
		return manager.capturePage(ctx, access, id, limit, request.Cursor)
	}
	return manager.resultsEntry(id, entry, limit, request.Cursor)
}
