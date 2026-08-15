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

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
	"github.com/Suhaibinator/open-splunk/internal/knowledgesnapshot"
	"github.com/Suhaibinator/open-splunk/internal/nilcheck"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
	"github.com/Suhaibinator/open-splunk/internal/searchsnapshot"
)

const (
	defaultReexecutionRuntime   = 5 * time.Minute
	maximumReexecutionRuntime   = time.Hour
	defaultReexecutionRowBuffer = 8
	maximumReexecutionRowBuffer = 1_024
)

// SearchSnapshotSource atomically supplies an immutable result pin and the
// detached execution authority that produced the same retained generation.
// searchjobs.Manager satisfies this interface. ReexecutionSource deliberately
// does not consume the pin's rows, but keeps the pin open for its whole lease
// lifetime so expiry cannot reclaim the corresponding job authority.
type SearchSnapshotSource interface {
	AcquireExecutionFor(
		context.Context,
		searchjobs.AccessScope,
		string,
	) (searchjobs.ResultLease, searchjobs.ExecutionSnapshot, error)
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

// ReexecutionSource executes a completed search exclusively from its trusted,
// immutable execution snapshot. Knowledge-enabled searches use the exact
// retained compiler seal; only legacy snapshots are rebuilt and recompiled.
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
	if nilcheck.IsNil(config.Searches) {
		return nil, errors.New("create export re-execution source: search service is required")
	}
	if nilcheck.IsNil(config.Executor) {
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

// AcquireResultsFor atomically pins the completed search and obtains the exact
// execution authority associated with that pin. Query execution itself is
// lazy, preserving the export manager's worker and queue admission bounds.
func (source *ReexecutionSource) AcquireResultsFor(ctx context.Context, access searchjobs.AccessScope, id string) (searchjobs.ResultLease, error) {
	if ctx == nil {
		return nil, errors.New("acquire export re-execution: context is nil")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	pin, execution, err := source.searches.AcquireExecutionFor(ctx, access, id)
	if err != nil {
		if !nilcheck.IsNil(pin) {
			_ = pin.Close()
		}
		return nil, err
	}
	if nilcheck.IsNil(pin) {
		return nil, searchjobs.ErrResultsUnavailable
	}
	pinReleased := false
	defer func() {
		if !pinReleased {
			_ = pin.Close()
		}
	}()

	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if execution.ID != id || execution.TenantID != access.TenantID || execution.OwnerID != access.OwnerID {
		return nil, searchjobs.ErrNotFound
	}
	resultMetadata, validPin := execution.ValidatedResultLease(pin)
	if !validPin || !resultMetadata.RowCountExact {
		return nil, fmt.Errorf("%w: completed search execution authority is invalid", searchjobs.ErrResultsUnavailable)
	}
	schema := resultMetadata.Schema
	// Reject hostile or corrupted cardinalities before schema projection or
	// cloning can allocate in proportion to source-controlled metadata.
	if !validSourceSchemaCardinality(schema) {
		return nil, fmt.Errorf("%w: completed search schema cardinality is invalid", searchjobs.ErrResultsUnavailable)
	}
	compiled, summary, err := source.executionAuthority(execution)
	if err != nil {
		return nil, fmt.Errorf("%w: recover completed search execution: %w", searchjobs.ErrResultsUnavailable, err)
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
		parent:                ctx,
		executor:              source.executor,
		compiled:              compiled,
		schema:                cloneResultSchema(schema),
		pin:                   pin,
		sourceCompilerVersion: strings.Clone(execution.CompilerVersion),
		knowledgeSnapshot:     summary,
		generation:            generation,
		maxRuntime:            source.maxRuntime,
		rows:                  make(chan searchjobs.ResultRow, source.rowBuffer),
		finished:              make(chan struct{}),
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	pinReleased = true
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

func (source *ReexecutionSource) executionAuthority(
	execution searchjobs.ExecutionSnapshot,
) (clickhouse.CompiledQuery, *opensplunkv1.KnowledgeSnapshotSummary, error) {
	retained, err := execution.OpenRetainedKnowledgeExecution()
	if err != nil {
		return clickhouse.CompiledQuery{}, nil, err
	}
	if retained != nil {
		return retained.CompiledQuery, retained.KnowledgeSummary, nil
	}
	logical, err := searchsnapshot.BuildExecutionPlan(execution)
	if err != nil {
		return clickhouse.CompiledQuery{}, nil, err
	}
	compiled, err := source.compiler.Compile(logical)
	return compiled, nil, err
}

type reexecutionLease struct {
	parent                context.Context
	executor              searchjobs.Executor
	compiled              clickhouse.CompiledQuery
	schema                searchjobs.Schema
	pin                   searchjobs.ResultLease
	sourceCompilerVersion string
	knowledgeSnapshot     *opensplunkv1.KnowledgeSnapshotSummary
	generation            uint64
	maxRuntime            time.Duration
	rows                  chan searchjobs.ResultRow
	finished              chan struct{}

	startOnce sync.Once
	closeOnce sync.Once
	nextMu    sync.Mutex
	stateMu   sync.Mutex
	cancel    context.CancelFunc
	runCtx    context.Context
	runErr    error
	runDone   bool
	closed    bool
	closeErr  error
	pending   *searchjobs.ResultRow
}

var _ searchjobs.ResultLease = (*reexecutionLease)(nil)

func (lease *reexecutionLease) Schema() searchjobs.Schema {
	return cloneResultSchema(lease.schema)
}

// knowledgeSnapshotSummary is intentionally package-private so only the
// trusted re-execution source can supply admission provenance to Manager.
func (lease *reexecutionLease) knowledgeSnapshotSummary() (*opensplunkv1.KnowledgeSnapshotSummary, error) {
	if lease == nil || lease.knowledgeSnapshot == nil {
		return nil, nil
	}
	return knowledgesnapshot.CloneSummary(lease.knowledgeSnapshot)
}

// compilerVersion is intentionally package-private so only the trusted
// re-execution source can attach source compatibility provenance to exports.
func (lease *reexecutionLease) compilerVersion() string {
	if lease == nil {
		return ""
	}
	return strings.Clone(lease.sourceCompilerVersion)
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
				closed, err := lease.terminalState()
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
	sink := newReexecutionSink(ctx, lease.schema, lease.rows)
	var executionErr error
	compiled, validCompiled := lease.compiled.CloneForExecution()
	if !validCompiled {
		executionErr = fmt.Errorf("%w: retained query authority is invalid", searchjobs.ErrInvalidResult)
	} else {
		func() {
			defer func() {
				if recover() != nil {
					executionErr = fmt.Errorf("%w: query executor panicked", searchjobs.ErrInvalidResult)
				}
			}()
			executionErr = lease.executor.Execute(ctx, compiled, sink)
		}()
		// Executor receives a value, but its slices and output pointers would
		// otherwise share backing memory. Reject any in-place mutation before a
		// successful terminal result can be published.
		if !compiled.EqualForExecution(lease.compiled) {
			executionErr = fmt.Errorf("%w: query executor mutated retained authority", searchjobs.ErrInvalidResult)
		}
	}
	// Execute's sink is a borrowed stream capability. Closing it first rejects
	// calls retained by a broken executor and unblocks any callback already
	// backpressured on rows. Once close returns, no sender can reach lease.rows,
	// so closing that channel below is memory-safe.
	sink.close()
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
		if lease.pin != nil {
			lease.closeErr = lease.pin.Close()
		}
	})
	return lease.closeErr
}

func (lease *reexecutionLease) isClosed() bool {
	lease.stateMu.Lock()
	defer lease.stateMu.Unlock()
	return lease.closed
}

func (lease *reexecutionLease) terminalState() (bool, error) {
	lease.stateMu.Lock()
	defer lease.stateMu.Unlock()
	return lease.closed, lease.runErr
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
	done     chan struct{}
	drained  chan struct{}
	sendTurn chan struct{}

	// mu guards only bounded state transitions. Blocking row sends are
	// serialized by sendTurn, while active and drained let close prove that no
	// registered sender can still reach rows without holding mu while waiting.
	mu            sync.Mutex
	closed        bool
	drainedClosed bool
	active        uint64
	schema        bool
	ordinal       uint64
	err           error
}

var errReexecutionSinkClosed = fmt.Errorf("%w: re-execution result sink is closed", searchjobs.ErrInvalidResult)

func newReexecutionSink(
	ctx context.Context,
	expected searchjobs.Schema,
	rows chan<- searchjobs.ResultRow,
) *reexecutionSink {
	sink := &reexecutionSink{
		ctx:      ctx,
		expected: expected,
		rows:     rows,
		done:     make(chan struct{}),
		drained:  make(chan struct{}),
		sendTurn: make(chan struct{}, 1),
	}
	sink.sendTurn <- struct{}{}
	return sink
}

func (sink *reexecutionSink) SetSchema(schema searchjobs.Schema) error {
	sink.mu.Lock()
	defer sink.mu.Unlock()
	if err := sink.readyLocked(); err != nil {
		return err
	}
	if sink.schema || !equalResultSchemas(schema, sink.expected) {
		return sink.rememberLocked(fmt.Errorf("%w: re-executed schema differs from pinned search schema", searchjobs.ErrInvalidResult))
	}
	sink.schema = true
	return nil
}

func (sink *reexecutionSink) AddRow(values []searchjobs.Value) error {
	// Claim the sole send turn before retaining caller-owned row storage. This
	// bounds both cloned rows and registered senders to one even if a buggy
	// executor invokes AddRow concurrently.
	select {
	case <-sink.ctx.Done():
		return sink.rejectUnregisteredRow(sink.ctx.Err())
	case <-sink.done:
		return errReexecutionSinkClosed
	case <-sink.sendTurn:
	}
	defer func() { sink.sendTurn <- struct{}{} }()

	sink.mu.Lock()
	if err := sink.readyLocked(); err != nil {
		sink.mu.Unlock()
		return err
	}
	if !sink.schema || len(values) != len(sink.expected.Columns) {
		err := sink.rememberLocked(fmt.Errorf("%w: re-executed row does not match schema", searchjobs.ErrInvalidResult))
		sink.mu.Unlock()
		return err
	}
	for index, value := range values {
		column := sink.expected.Columns[index]
		kind := value.Kind()
		if kind == searchjobs.ValueKindInvalid || kind == searchjobs.ValueKindMixed ||
			(column.Kind != searchjobs.ValueKindMixed && kind != column.Kind && kind != searchjobs.ValueKindNull) ||
			(kind == searchjobs.ValueKindNull && !column.Nullable && column.Kind != searchjobs.ValueKindNull) {
			err := sink.rememberLocked(fmt.Errorf("%w: re-executed cell %d does not match schema", searchjobs.ErrInvalidResult, index))
			sink.mu.Unlock()
			return err
		}
	}
	cloned := slices.Clone(values)
	sink.active++
	row := searchjobs.ResultRow{Ordinal: sink.ordinal, Values: cloned}
	sink.mu.Unlock()

	var sendErr error
	select {
	case <-sink.ctx.Done():
		sendErr = sink.ctx.Err()
	case <-sink.done:
		sendErr = errReexecutionSinkClosed
	case sink.rows <- row:
	}
	return sink.finishRow(sendErr, sendErr == nil)
}

// rejectUnregisteredRow records cancellation without decrementing the active
// sender that currently owns sendTurn. Once close wins, late callbacks receive
// the fixed closure error and cannot register or reach rows.
func (sink *reexecutionSink) rejectUnregisteredRow(err error) error {
	sink.mu.Lock()
	defer sink.mu.Unlock()
	if sink.closed {
		if sink.err == nil {
			sink.err = errReexecutionSinkClosed
		}
		return errReexecutionSinkClosed
	}
	return sink.rememberLocked(err)
}

func (sink *reexecutionSink) finishRow(sendErr error, sent bool) error {
	sink.mu.Lock()
	defer sink.mu.Unlock()
	var result error
	switch {
	case sink.closed:
		if sink.err == nil {
			sink.err = errReexecutionSinkClosed
		}
		result = errReexecutionSinkClosed
	case sink.err != nil:
		result = sink.err
	case sendErr != nil:
		result = sink.rememberLocked(sendErr)
	case sent:
		sink.ordinal++
	}
	if sink.active > 0 {
		sink.active--
	}
	sink.signalDrainedLocked()
	return result
}

func (sink *reexecutionSink) readyLocked() error {
	if sink.closed {
		return errReexecutionSinkClosed
	}
	return sink.err
}

func (sink *reexecutionSink) rememberLocked(err error) error {
	if sink.err == nil {
		sink.err = err
	}
	return sink.err
}

func (sink *reexecutionSink) close() {
	sink.mu.Lock()
	if !sink.closed {
		sink.closed = true
		close(sink.done)
	}
	sink.signalDrainedLocked()
	drained := sink.drained
	sink.mu.Unlock()
	<-drained
}

func (sink *reexecutionSink) signalDrainedLocked() {
	if sink.closed && sink.active == 0 && !sink.drainedClosed {
		sink.drainedClosed = true
		close(sink.drained)
	}
}

func (sink *reexecutionSink) failure() error {
	sink.mu.Lock()
	defer sink.mu.Unlock()
	return sink.err
}

func (sink *reexecutionSink) schemaReceived() bool {
	sink.mu.Lock()
	defer sink.mu.Unlock()
	return sink.schema
}

func cloneResultSchema(schema searchjobs.Schema) searchjobs.Schema {
	return searchjobs.Schema{Columns: slices.Clone(schema.Columns)}
}

func validSourceSchemaCardinality(schema searchjobs.Schema) bool {
	return len(schema.Columns) > 0 && len(schema.Columns) <= maximumColumns
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
	if !validSourceSchemaCardinality(schema) {
		return false
	}
	if compiled.Timechart != nil && compiled.Chart != nil {
		return false
	}
	if compiled.Timechart == nil && compiled.Chart == nil {
		return slices.Equal(compiled.OutputFields, schemaColumnNames(schema))
	}
	if compiled.Timechart != nil {
		return searchjobs.ValidateTimechartSchema(
			schema,
			compiled.OutputFields,
			*compiled.Timechart,
		) == nil
	}
	// The remaining bounded runtime-wide operator is chart: one fixed,
	// plan-time row column followed by at most MaxSeries runtime-named cells.
	fixedKind, ok := chartRowExportKind(compiled.Chart.RowKind)
	seriesKind, seriesNullable, seriesOK := chartSeriesExportKind(compiled.Chart.ValueKind)
	if !ok || !seriesOK || compiled.Chart.RowField == "" {
		return false
	}
	fixedName := compiled.Chart.RowField
	maxSeries := int(compiled.Chart.MaxSeries)
	maxLabelBytes := int(compiled.Chart.MaxLabelBytes)
	if !slices.Equal(compiled.OutputFields, []string{fixedName}) || len(schema.Columns) == 0 ||
		len(schema.Columns)-1 > maxSeries {
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
			// A Mixed chart row column is nullable, matching the column the
			// ordinary result path publishes for the same field.
			if column.Name != fixedName || column.Kind != fixedKind ||
				column.Nullable != (fixedKind == searchjobs.ValueKindMixed) {
				return false
			}
			continue
		}
		maximumNameBytes := maxLabelBytes
		if strings.HasPrefix(column.Name, "VALUE_") {
			maximumNameBytes += len("VALUE")
		}
		if len(column.Name) > maximumNameBytes || strings.HasPrefix(column.Name, "_") ||
			column.Kind != seriesKind || column.Nullable != seriesNullable {
			return false
		}
	}
	return true
}

func chartSeriesExportKind(kind clickhouse.ChartValueKind) (searchjobs.ValueKind, bool, bool) {
	switch kind {
	case clickhouse.ChartValueKindCount:
		return searchjobs.ValueKindUnsigned, false, true
	case clickhouse.ChartValueKindSum, clickhouse.ChartValueKindAverage, clickhouse.ChartValueKindPercentile:
		return searchjobs.ValueKindDouble, true, true
	default:
		return searchjobs.ValueKindInvalid, false, false
	}
}

func chartRowExportKind(kind clickhouse.ChartRowKind) (searchjobs.ValueKind, bool) {
	switch kind {
	case clickhouse.ChartRowKindString:
		return searchjobs.ValueKindString, true
	case clickhouse.ChartRowKindSigned:
		return searchjobs.ValueKindSigned, true
	case clickhouse.ChartRowKindUnsigned:
		return searchjobs.ValueKindUnsigned, true
	case clickhouse.ChartRowKindDouble:
		return searchjobs.ValueKindDouble, true
	case clickhouse.ChartRowKindBool:
		return searchjobs.ValueKindBool, true
	case clickhouse.ChartRowKindTime:
		return searchjobs.ValueKindTime, true
	case clickhouse.ChartRowKindMixed:
		return searchjobs.ValueKindMixed, true
	default:
		return searchjobs.ValueKindInvalid, false
	}
}
