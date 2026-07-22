package export

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
	"github.com/Suhaibinator/open-splunk/internal/plan"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
	"github.com/Suhaibinator/open-splunk/internal/spl"
)

const (
	defaultReexecutionRuntime   = 5 * time.Minute
	maximumReexecutionRuntime   = time.Hour
	defaultReexecutionRowBuffer = 8
	maximumReexecutionRowBuffer = 1_024
)

// SearchSnapshotSource supplies both an immutable result pin and its detached
// search definition. searchjobs.Manager satisfies this interface. The result
// pin is used for the stable schema and to keep the job descriptor alive; its
// retained rows are deliberately not used by ReexecutionSource.
type SearchSnapshotSource interface {
	AcquireResultsFor(context.Context, searchjobs.AccessScope, string) (searchjobs.ResultLease, error)
	GetFor(searchjobs.AccessScope, string) (searchjobs.Job, error)
}

// ReexecutionSourceConfig controls bounded query re-execution for exports.
// Executor should have result limits at least as large as the export manager's
// configured maxima. Export's worker count remains the query-concurrency bound
// because execution begins lazily on the first Next call.
type ReexecutionSourceConfig struct {
	Searches   SearchSnapshotSource
	Executor   searchjobs.Executor
	Compiler   clickhouse.Compiler
	MaxRuntime time.Duration
	RowBuffer  int
}

// ReexecutionSource rebuilds a completed search exclusively from the trusted,
// immutable job snapshot. It never accepts generated or caller-provided SQL.
type ReexecutionSource struct {
	searches   SearchSnapshotSource
	executor   searchjobs.Executor
	compiler   clickhouse.Compiler
	maxRuntime time.Duration
	rowBuffer  int
	generation atomic.Uint64
}

var _ ResultSource = (*ReexecutionSource)(nil)

// NewReexecutionSource constructs a streaming export source. Zero duration and
// row-buffer values select conservative defaults.
func NewReexecutionSource(config ReexecutionSourceConfig) (*ReexecutionSource, error) {
	if config.Searches == nil {
		return nil, errors.New("create export re-execution source: search service is required")
	}
	if config.Executor == nil {
		return nil, errors.New("create export re-execution source: query executor is required")
	}
	if config.MaxRuntime < 0 || config.MaxRuntime > maximumReexecutionRuntime {
		return nil, errors.New("create export re-execution source: invalid maximum runtime")
	}
	if config.MaxRuntime == 0 {
		config.MaxRuntime = defaultReexecutionRuntime
	}
	if config.RowBuffer < 0 || config.RowBuffer > maximumReexecutionRowBuffer {
		return nil, errors.New("create export re-execution source: invalid row buffer")
	}
	if config.RowBuffer == 0 {
		config.RowBuffer = defaultReexecutionRowBuffer
	}
	return &ReexecutionSource{
		searches:   config.Searches,
		executor:   config.Executor,
		compiler:   config.Compiler,
		maxRuntime: config.MaxRuntime,
		rowBuffer:  config.RowBuffer,
	}, nil
}

// AcquireResultsFor pins the completed search and rebuilds its fully scoped
// query. Query execution itself is lazy, preserving the export manager's
// worker and queue admission bounds.
func (source *ReexecutionSource) AcquireResultsFor(ctx context.Context, access searchjobs.AccessScope, id string) (searchjobs.ResultLease, error) {
	if ctx == nil {
		return nil, errors.New("acquire export re-execution: context is nil")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	pin, err := source.searches.AcquireResultsFor(ctx, access, id)
	if err != nil {
		return nil, err
	}
	pinReleased := false
	defer func() {
		if !pinReleased {
			_ = pin.Close()
		}
	}()

	job, err := source.searches.GetFor(access, id)
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if job.ID != id || job.TenantID != access.TenantID || job.OwnerID != access.OwnerID {
		return nil, searchjobs.ErrNotFound
	}
	if job.State != searchjobs.StateCompleted && job.State != searchjobs.StateExpired {
		return nil, searchjobs.ErrResultsUnavailable
	}
	schema := pin.Schema()
	if err := pin.Close(); err != nil {
		return nil, fmt.Errorf("%w: release completed search snapshot", searchjobs.ErrResultsUnavailable)
	}
	pinReleased = true
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	compiled, err := source.compile(job)
	if err != nil {
		return nil, fmt.Errorf("%w: rebuild completed search: %v", searchjobs.ErrResultsUnavailable, err)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if !schemaMatchesCompiledQuery(schema, compiled) {
		return nil, fmt.Errorf("%w: completed search schema changed", searchjobs.ErrResultsUnavailable)
	}
	generation, ok := source.nextGeneration()
	if !ok {
		return nil, fmt.Errorf("%w: re-execution generation space exhausted", searchjobs.ErrResultsUnavailable)
	}

	lease := &reexecutionLease{
		parent:     ctx,
		executor:   source.executor,
		compiled:   compiled,
		schema:     cloneResultSchema(schema),
		generation: generation,
		maxRuntime: source.maxRuntime,
		rows:       make(chan searchjobs.ResultRow, source.rowBuffer),
		finished:   make(chan struct{}),
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return lease, nil
}

func (source *ReexecutionSource) nextGeneration() (uint64, bool) {
	for {
		current := source.generation.Load()
		if current == ^uint64(0) {
			return 0, false
		}
		if source.generation.CompareAndSwap(current, current+1) {
			return current + 1, true
		}
	}
}

func (source *ReexecutionSource) compile(job searchjobs.Job) (clickhouse.CompiledQuery, error) {
	parsed, err := spl.Parse(job.SPL)
	if err != nil {
		return clickhouse.CompiledQuery{}, err
	}
	visibilityCutoff := job.VisibilityCutoff
	// EffectiveIndexes is the already-authorized immutable scope selected by
	// the original plan. Reusing it for both scope inputs prevents re-execution
	// from widening access even if the original request named more indexes.
	indexes := slices.Clone(job.EffectiveIndexes)
	logical, err := plan.Build(parsed, plan.Scope{
		TenantID:          job.TenantID,
		AuthorizedIndexes: indexes,
		RequestedIndexes:  slices.Clone(indexes),
		Earliest:          job.Earliest,
		Latest:            job.Latest,
		IndexTimeCutoff:   job.IndexTimeCutoff,
		VisibilityCutoff:  &visibilityCutoff,
	})
	if err != nil {
		return clickhouse.CompiledQuery{}, err
	}
	return source.compiler.Compile(logical)
}

type reexecutionLease struct {
	parent     context.Context
	executor   searchjobs.Executor
	compiled   clickhouse.CompiledQuery
	schema     searchjobs.Schema
	generation uint64
	maxRuntime time.Duration
	rows       chan searchjobs.ResultRow
	finished   chan struct{}

	startOnce sync.Once
	closeOnce sync.Once
	nextMu    sync.Mutex
	stateMu   sync.Mutex
	cancel    context.CancelFunc
	runCtx    context.Context
	runErr    error
	runDone   bool
	closed    bool
	pending   *searchjobs.ResultRow
}

var _ searchjobs.ResultLease = (*reexecutionLease)(nil)

func (lease *reexecutionLease) Schema() searchjobs.Schema {
	return cloneResultSchema(lease.schema)
}

// RowCount returns zero because re-execution intentionally does not run a
// second count query. Export enforces its exact row limit while streaming.
func (*reexecutionLease) RowCount() uint64 { return 0 }

func (*reexecutionLease) RowCountExact() bool { return false }

// Generation identifies this execution, not the retained preview used to
// recover the immutable search definition. Two re-executions may observe
// different physical data as storage retention progresses and must therefore
// never advertise the same immutable result identity.
func (lease *reexecutionLease) Generation() uint64 { return lease.generation }

// ResultsTruncated is false: the iterator is a fresh bounded execution rather
// than the search manager's retained preview snapshot.
func (*reexecutionLease) ResultsTruncated() bool { return false }

func (lease *reexecutionLease) Next(ctx context.Context) (searchjobs.ResultRow, bool, error) {
	if ctx == nil {
		return searchjobs.ResultRow{}, false, errors.New("read export re-execution: context is nil")
	}
	lease.nextMu.Lock()
	defer lease.nextMu.Unlock()
	if lease.isClosed() {
		return searchjobs.ResultRow{}, false, searchjobs.ErrResultLeaseClosed
	}
	if err := ctx.Err(); err != nil {
		return searchjobs.ResultRow{}, false, err
	}
	if err := lease.executionContextFailure(); err != nil {
		return searchjobs.ResultRow{}, false, err
	}
	if lease.pending != nil {
		row := *lease.pending
		lease.pending = nil
		return row, true, nil
	}

	lease.start()
	executionDone := lease.executionDone()
	for {
		select {
		case <-ctx.Done():
			return searchjobs.ResultRow{}, false, ctx.Err()
		case <-executionDone:
			if lease.isClosed() {
				return searchjobs.ResultRow{}, false, searchjobs.ErrResultLeaseClosed
			}
			if err := lease.executionContextFailure(); err != nil {
				return searchjobs.ResultRow{}, false, err
			}
			// A successful producer cancels its timer after publishing its
			// terminal state and closing rows. Disable this already-closed case
			// so the channel can be drained normally.
			executionDone = nil
		case row, ok := <-lease.rows:
			if !ok {
				err, closed := lease.terminalState()
				if closed {
					return searchjobs.ResultRow{}, false, searchjobs.ErrResultLeaseClosed
				}
				return searchjobs.ResultRow{}, false, err
			}
			if err := ctx.Err(); err != nil {
				lease.pending = &row
				return searchjobs.ResultRow{}, false, err
			}
			if lease.isClosed() {
				return searchjobs.ResultRow{}, false, searchjobs.ErrResultLeaseClosed
			}
			// The producer's lifetime governs the whole result stream. A canceled
			// parent or elapsed execution deadline must beat a concurrently-ready
			// buffered row; otherwise a timeout can be serialized as a successful
			// artifact prefix.
			if err := lease.executionContextFailure(); err != nil {
				return searchjobs.ResultRow{}, false, err
			}
			return row, true, nil
		}
	}
}

func (lease *reexecutionLease) start() {
	lease.startOnce.Do(func() {
		lease.stateMu.Lock()
		if lease.closed {
			lease.runErr = searchjobs.ErrResultLeaseClosed
			close(lease.rows)
			close(lease.finished)
			lease.stateMu.Unlock()
			return
		}
		executionContext, cancel := context.WithTimeout(lease.parent, lease.maxRuntime)
		lease.cancel = cancel
		lease.runCtx = executionContext
		lease.stateMu.Unlock()
		go lease.execute(executionContext, cancel)
	})
}

func (lease *reexecutionLease) execute(ctx context.Context, cancel context.CancelFunc) {
	defer cancel()
	sink := &reexecutionSink{ctx: ctx, expected: lease.schema, rows: lease.rows}
	var executionErr error
	func() {
		defer func() {
			if recover() != nil {
				executionErr = fmt.Errorf("%w: query executor panicked", searchjobs.ErrInvalidResult)
			}
		}()
		executionErr = lease.executor.Execute(ctx, lease.compiled, sink)
	}()
	// Cancellation and deadlines are authoritative even when a custom
	// executor swallows the error returned by its sink. Otherwise a buffered
	// prefix could be mistaken for a complete artifact.
	if contextErr := ctx.Err(); contextErr != nil {
		executionErr = contextErr
	} else if sinkErr := sink.failure(); sinkErr != nil {
		executionErr = sinkErr
	} else if executionErr == nil && !sink.schemaReceived() {
		executionErr = fmt.Errorf("%w: re-execution returned no schema", searchjobs.ErrInvalidResult)
	}
	lease.stateMu.Lock()
	lease.runErr = executionErr
	lease.runDone = true
	lease.cancel = nil
	lease.stateMu.Unlock()
	close(lease.rows)
	close(lease.finished)
}

// Close cancels an active query, waits for the bounded executor to return, and
// then releases the search snapshot pin. It is idempotent and concurrent-safe.
func (lease *reexecutionLease) Close() error {
	if lease == nil {
		return nil
	}
	lease.closeOnce.Do(func() {
		lease.stateMu.Lock()
		lease.closed = true
		cancel := lease.cancel
		lease.stateMu.Unlock()
		if cancel != nil {
			cancel()
		}
		lease.start()
		<-lease.finished
	})
	return nil
}

func (lease *reexecutionLease) isClosed() bool {
	lease.stateMu.Lock()
	defer lease.stateMu.Unlock()
	return lease.closed
}

func (lease *reexecutionLease) terminalState() (error, bool) {
	lease.stateMu.Lock()
	defer lease.stateMu.Unlock()
	return lease.runErr, lease.closed
}

func (lease *reexecutionLease) executionContextFailure() error {
	lease.stateMu.Lock()
	defer lease.stateMu.Unlock()
	if lease.runDone {
		if errors.Is(lease.runErr, context.Canceled) || errors.Is(lease.runErr, context.DeadlineExceeded) {
			return lease.runErr
		}
		return nil
	}
	if lease.runCtx != nil {
		return lease.runCtx.Err()
	}
	return lease.parent.Err()
}

func (lease *reexecutionLease) executionDone() <-chan struct{} {
	lease.stateMu.Lock()
	defer lease.stateMu.Unlock()
	if lease.runCtx == nil {
		return nil
	}
	return lease.runCtx.Done()
}

type reexecutionSink struct {
	ctx      context.Context
	expected searchjobs.Schema
	rows     chan<- searchjobs.ResultRow
	schema   bool
	ordinal  uint64
	err      error
}

func (sink *reexecutionSink) SetSchema(schema searchjobs.Schema) error {
	if sink.err != nil {
		return sink.err
	}
	if sink.schema || !equalResultSchemas(schema, sink.expected) {
		sink.err = fmt.Errorf("%w: re-executed schema differs from pinned search schema", searchjobs.ErrInvalidResult)
		return sink.err
	}
	sink.schema = true
	return nil
}

func (sink *reexecutionSink) AddRow(values []searchjobs.Value) error {
	if sink.err != nil {
		return sink.err
	}
	if !sink.schema || len(values) != len(sink.expected.Columns) {
		sink.err = fmt.Errorf("%w: re-executed row does not match schema", searchjobs.ErrInvalidResult)
		return sink.err
	}
	for index, value := range values {
		column := sink.expected.Columns[index]
		kind := value.Kind()
		if kind == searchjobs.ValueKindInvalid || kind == searchjobs.ValueKindMixed ||
			(column.Kind != searchjobs.ValueKindMixed && kind != column.Kind && kind != searchjobs.ValueKindNull) ||
			(kind == searchjobs.ValueKindNull && !column.Nullable && column.Kind != searchjobs.ValueKindNull) {
			sink.err = fmt.Errorf("%w: re-executed cell %d does not match schema", searchjobs.ErrInvalidResult, index)
			return sink.err
		}
	}
	row := searchjobs.ResultRow{Ordinal: sink.ordinal, Values: slices.Clone(values)}
	select {
	case <-sink.ctx.Done():
		sink.err = sink.ctx.Err()
		return sink.err
	case sink.rows <- row:
		sink.ordinal++
		return nil
	}
}

func (sink *reexecutionSink) failure() error { return sink.err }

func (sink *reexecutionSink) schemaReceived() bool { return sink.schema }

func cloneResultSchema(schema searchjobs.Schema) searchjobs.Schema {
	return searchjobs.Schema{Columns: slices.Clone(schema.Columns)}
}

func equalResultSchemas(left, right searchjobs.Schema) bool {
	return slices.Equal(left.Columns, right.Columns)
}

func schemaColumnNames(schema searchjobs.Schema) []string {
	result := make([]string, len(schema.Columns))
	for index, column := range schema.Columns {
		result[index] = column.Name
	}
	return result
}

func schemaMatchesCompiledQuery(schema searchjobs.Schema, compiled clickhouse.CompiledQuery) bool {
	if compiled.Timechart == nil {
		return len(schema.Columns) > 0 && slices.Equal(compiled.OutputFields, schemaColumnNames(schema))
	}
	output := *compiled.Timechart
	if !slices.Equal(compiled.OutputFields, []string{"_time"}) || len(schema.Columns) == 0 ||
		len(schema.Columns)-1 > int(output.MaxSeries) {
		return false
	}
	seen := make(map[string]struct{}, len(schema.Columns))
	for index, column := range schema.Columns {
		if column.Name == "" || !utf8.ValidString(column.Name) || column.Multivalue {
			return false
		}
		if _, exists := seen[column.Name]; exists {
			return false
		}
		seen[column.Name] = struct{}{}
		if index == 0 {
			if column.Name != "_time" || column.Kind != searchjobs.ValueKindTime || column.Nullable {
				return false
			}
			continue
		}
		maximumNameBytes := int(output.MaxLabelBytes)
		if strings.HasPrefix(column.Name, "VALUE_") {
			maximumNameBytes += len("VALUE")
		}
		if len(column.Name) > maximumNameBytes || strings.HasPrefix(column.Name, "_") ||
			column.Kind != searchjobs.ValueKindUnsigned || column.Nullable {
			return false
		}
	}
	return true
}
