package searchjobs

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
)

const chartBreakTransportSPL = "index=main | chart count OVER path BY level"

// chartBreakTransportPivot is one buffered chart result exactly as the
// executor publishes it: a runtime-named schema followed by every row at once.
type chartBreakTransportPivot struct {
	series []string
	labels []string
	counts [][]uint64
}

func chartBreakTransportDefaultPivot(rowCount int) chartBreakTransportPivot {
	pivot := chartBreakTransportPivot{series: []string{"ERROR", "INFO", "NULL", "OTHER"}}
	for index := range rowCount {
		pivot.labels = append(pivot.labels, fmt.Sprintf("/p%02d", index))
		pivot.counts = append(pivot.counts, []uint64{
			uint64(index), uint64(index * 2), uint64(index % 3), uint64(index % 5),
		})
	}
	return pivot
}

func (pivot chartBreakTransportPivot) schema(rowField string) Schema {
	columns := make([]Column, 0, len(pivot.series)+1)
	columns = append(columns, Column{Name: rowField, Kind: ValueKindString})
	for _, name := range pivot.series {
		columns = append(columns, Column{Name: name, Kind: ValueKindUnsigned})
	}
	return Schema{Columns: columns}
}

func (pivot chartBreakTransportPivot) row(index int) []Value {
	values := make([]Value, 0, len(pivot.series)+1)
	values = append(values, StringValue(pivot.labels[index]))
	for _, count := range pivot.counts[index] {
		values = append(values, UnsignedValue(count))
	}
	return values
}

// chartBreakTransportExecutor publishes a buffered pivot the way the real
// executor does: nothing at all until the whole result is known, then one
// schema followed by every row. It asserts the manager compiled a real chart
// contract so the sink under test is the chart sink, not the ordinary one.
func chartBreakTransportExecutor(t *testing.T, pivot chartBreakTransportPivot, before func()) executorFunc {
	t.Helper()
	return func(ctx context.Context, query clickhouse.CompiledQuery, sink ResultSink) error {
		if query.Chart == nil || query.Timechart != nil {
			t.Errorf("compiled query is not a chart: chart=%#v timechart=%#v", query.Chart, query.Timechart)
			return errors.New("compiled query is not a chart")
		}
		if before != nil {
			before()
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := sink.SetSchema(pivot.schema(query.Chart.RowField)); err != nil {
			return err
		}
		for index := range pivot.labels {
			if err := ctx.Err(); err != nil {
				return err
			}
			if err := sink.AddRow(pivot.row(index)); err != nil {
				return err
			}
		}
		return ctx.Err()
	}
}

func chartBreakTransportRequest() CreateRequest {
	request := validRequest()
	request.SPL = chartBreakTransportSPL
	return request
}

func chartBreakTransportAccess() AccessScope {
	return AccessScope{TenantID: "tenant", OwnerID: "owner"}
}

func chartBreakTransportRun(t *testing.T, config Config, request CreateRequest) (*Manager, Job) {
	t.Helper()
	if config.NewID == nil {
		config.NewID = sequenceIDs("chart-transport")
	}
	if config.MaxConcurrent == 0 {
		config.MaxConcurrent = 1
	}
	config.CleanupInterval = -1
	manager := newTestManager(t, config)
	created, err := manager.Create(context.Background(), request)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	return manager, created
}

// TestChartBreakTransportManagerPagesTheBufferedPivot walks a retained chart
// snapshot one row at a time. Every page must repeat the whole runtime-named
// schema, and the cursor chain must cover the first, middle, and last page
// without gaps or repeats.
func TestChartBreakTransportManagerPagesTheBufferedPivot(t *testing.T) {
	t.Parallel()

	pivot := chartBreakTransportDefaultPivot(5)
	manager, created := chartBreakTransportRun(t, Config{
		Executor: chartBreakTransportExecutor(t, pivot, nil),
	}, chartBreakTransportRequest())
	terminal := waitForTerminal(t, manager, created.ID)
	if terminal.State != StateCompleted || terminal.RowCount != 5 || terminal.ResultsTruncated {
		t.Fatalf("chart job = %#v failure=%+v", terminal, terminal.Failure)
	}
	assertValidHistory(t, stateHistory(t, manager, created.ID))

	wantSchema := pivot.schema("path")
	cursor := ""
	seen := make([]string, 0, len(pivot.labels))
	for page := 0; page < len(pivot.labels)+1; page++ {
		got, err := manager.ResultsFor(chartBreakTransportAccess(), created.ID, PageRequest{Limit: 1, Cursor: cursor})
		if err != nil {
			t.Fatalf("page %d: %v", page, err)
		}
		if !reflect.DeepEqual(got.Schema, wantSchema) {
			t.Fatalf("page %d schema = %#v, want %#v", page, got.Schema, wantSchema)
		}
		if got.TotalRows != uint64(len(pivot.labels)) {
			t.Fatalf("page %d total = %d, want %d", page, got.TotalRows, len(pivot.labels))
		}
		if len(got.Rows) != 1 {
			t.Fatalf("page %d returned %d rows, want 1", page, len(got.Rows))
		}
		row := got.Rows[0]
		if row.Ordinal != uint64(len(seen)) {
			t.Fatalf("page %d ordinal = %d, want %d", page, row.Ordinal, len(seen))
		}
		label, ok := row.Values[0].String()
		if !ok {
			t.Fatalf("page %d row label = %#v", page, row.Values[0])
		}
		seen = append(seen, label)
		for index, want := range pivot.counts[page] {
			count, ok := row.Values[index+1].Unsigned()
			if !ok || count != want {
				t.Fatalf("page %d cell %d = %d (%v), want %d", page, index, count, ok, want)
			}
		}
		if len(seen) == len(pivot.labels) {
			if !got.Complete || got.NextCursor != "" {
				t.Fatalf("last page complete=%v cursor=%q", got.Complete, got.NextCursor)
			}
			break
		}
		if got.Complete || got.NextCursor == "" {
			t.Fatalf("page %d complete=%v cursor=%q, want a continuation", page, got.Complete, got.NextCursor)
		}
		cursor = got.NextCursor
	}
	if !reflect.DeepEqual(seen, pivot.labels) {
		t.Fatalf("paged labels = %v, want %v", seen, pivot.labels)
	}

	whole, err := manager.ResultsFor(chartBreakTransportAccess(), created.ID, PageRequest{Limit: 100})
	if err != nil {
		t.Fatalf("whole page: %v", err)
	}
	if !whole.Complete || whole.NextCursor != "" || len(whole.Rows) != len(pivot.labels) {
		t.Fatalf("whole page = complete %v cursor %q rows %d", whole.Complete, whole.NextCursor, len(whole.Rows))
	}
}

// TestChartBreakTransportManagerRejectsForeignChartCursors pins the retained
// pivot's cursor domain. A pivot page cursor is only valid for the exact job
// and result generation that produced it.
func TestChartBreakTransportManagerRejectsForeignChartCursors(t *testing.T) {
	t.Parallel()

	pivot := chartBreakTransportDefaultPivot(4)
	manager, first := chartBreakTransportRun(t, Config{
		Executor: chartBreakTransportExecutor(t, pivot, nil),
		MaxJobs:  4,
	}, chartBreakTransportRequest())
	waitForTerminal(t, manager, first.ID)
	second, err := manager.Create(context.Background(), chartBreakTransportRequest())
	if err != nil {
		t.Fatalf("Create() second chart: %v", err)
	}
	waitForTerminal(t, manager, second.ID)

	page, err := manager.Results(first.ID, PageRequest{Limit: 1})
	if err != nil {
		t.Fatalf("first page: %v", err)
	}
	if page.NextCursor == "" {
		t.Fatal("first chart page produced no cursor")
	}
	generation := resultGeneration(t, manager, first.ID)
	if generation == 0 {
		t.Fatal("completed chart has no result generation")
	}

	forge := func(scope, jobID string, gen, offset uint64) string {
		token, err := encodeCursor(manager.cursorKey, scope, jobID, gen, offset)
		if err != nil {
			t.Fatalf("encodeCursor: %v", err)
		}
		return token
	}
	for _, test := range []struct {
		name   string
		cursor string
	}{
		{name: "cursor from another chart job", cursor: forge(manager.cursorScope, second.ID, resultGeneration(t, manager, second.ID), 1)},
		{name: "stale generation", cursor: forge(manager.cursorScope, first.ID, generation+1, 1)},
		{name: "foreign cursor scope", cursor: forge("other-scope", first.ID, generation, 1)},
		{name: "offset beyond the pivot", cursor: forge(manager.cursorScope, first.ID, generation, uint64(len(pivot.labels))+1)},
		{name: "tampered cursor", cursor: tamper(page.NextCursor)},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := manager.Results(first.ID, PageRequest{Limit: 1, Cursor: test.cursor}); !errors.Is(err, ErrInvalidCursor) {
				t.Fatalf("Results() error = %v, want ErrInvalidCursor", err)
			}
		})
	}

	// The exact end offset is a legal cursor and yields an empty final page.
	end, err := manager.Results(first.ID, PageRequest{
		Limit:  1,
		Cursor: forge(manager.cursorScope, first.ID, generation, uint64(len(pivot.labels))),
	})
	if err != nil {
		t.Fatalf("end cursor: %v", err)
	}
	if len(end.Rows) != 0 || !end.Complete || end.NextCursor != "" {
		t.Fatalf("end page = %#v", end)
	}
}

func resultGeneration(t *testing.T, manager *Manager, id string) uint64 {
	t.Helper()
	manager.mu.RLock()
	entry := manager.jobs[id]
	manager.mu.RUnlock()
	if entry == nil {
		t.Fatalf("missing job %q", id)
	}
	entry.mu.RLock()
	defer entry.mu.RUnlock()
	return entry.resultGeneration
}

// TestChartBreakTransportManagerLeaseOverPivotIsImmutable pins the export
// path's view of a chart: the leased schema and rows are detached copies, so a
// consumer cannot mutate the retained pivot every other reader shares.
func TestChartBreakTransportManagerLeaseOverPivotIsImmutable(t *testing.T) {
	t.Parallel()

	pivot := chartBreakTransportDefaultPivot(3)
	manager, created := chartBreakTransportRun(t, Config{
		Executor: chartBreakTransportExecutor(t, pivot, nil),
	}, chartBreakTransportRequest())
	waitForTerminal(t, manager, created.ID)

	ctx := context.Background()
	lease, err := manager.AcquireResultsFor(ctx, chartBreakTransportAccess(), created.ID)
	if err != nil {
		t.Fatalf("AcquireResultsFor: %v", err)
	}
	defer func() { _ = lease.Close() }()

	if !lease.RowCountExact() || lease.RowCount() != uint64(len(pivot.labels)) || lease.ResultsTruncated() {
		t.Fatalf("lease row count = %d (exact %v, truncated %v)", lease.RowCount(), lease.RowCountExact(), lease.ResultsTruncated())
	}
	if lease.Generation() != resultGeneration(t, manager, created.ID) {
		t.Fatalf("lease generation = %d, want %d", lease.Generation(), resultGeneration(t, manager, created.ID))
	}

	schema := lease.Schema()
	if !reflect.DeepEqual(schema, pivot.schema("path")) {
		t.Fatalf("lease schema = %#v", schema)
	}
	// Mutating the detached copy must not reach the retained snapshot.
	schema.Columns[1].Name = "attacker"
	schema.Columns[0].Kind = ValueKindBytes
	if again := lease.Schema(); !reflect.DeepEqual(again, pivot.schema("path")) {
		t.Fatalf("lease schema after mutation = %#v", again)
	}

	for index := range pivot.labels {
		row, ok, err := lease.Next(ctx)
		if err != nil || !ok {
			t.Fatalf("row %d: ok=%v err=%v", index, ok, err)
		}
		if row.Ordinal != uint64(index) {
			t.Fatalf("row %d ordinal = %d", index, row.Ordinal)
		}
		label, labelOK := row.Values[0].String()
		if !labelOK || label != pivot.labels[index] {
			t.Fatalf("row %d label = %q (%v)", index, label, labelOK)
		}
		// Overwrite the leased cells; the retained pivot must not follow.
		row.Values[0] = StringValue("attacker")
		for cell := range row.Values[1:] {
			row.Values[cell+1] = UnsignedValue(^uint64(0))
		}
	}
	if _, ok, err := lease.Next(ctx); ok || err != nil {
		t.Fatalf("lease continued past the pivot: ok=%v err=%v", ok, err)
	}

	page, err := manager.ResultsFor(chartBreakTransportAccess(), created.ID, PageRequest{Limit: 100})
	if err != nil {
		t.Fatalf("Results after lease mutation: %v", err)
	}
	if !reflect.DeepEqual(page.Schema, pivot.schema("path")) {
		t.Fatalf("retained schema drifted: %#v", page.Schema)
	}
	for index, row := range page.Rows {
		label, ok := row.Values[0].String()
		if !ok || label != pivot.labels[index] {
			t.Fatalf("retained row %d label = %q (%v), want %q", index, label, ok, pivot.labels[index])
		}
		for cell, want := range pivot.counts[index] {
			count, ok := row.Values[cell+1].Unsigned()
			if !ok || count != want {
				t.Fatalf("retained row %d cell %d = %d (%v), want %d", index, cell, count, ok, want)
			}
		}
	}
	if err := lease.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if _, ok, err := lease.Next(ctx); ok || err == nil {
		t.Fatalf("closed lease still iterated: ok=%v err=%v", ok, err)
	}
}

// TestChartBreakTransportManagerFailsPivotsOverThePageByteCeiling proves the
// retained-page byte bound still applies to a buffered pivot whose row values
// passed the executor's much larger 48 MiB buffering guard.
func TestChartBreakTransportManagerFailsPivotsOverThePageByteCeiling(t *testing.T) {
	t.Parallel()

	pivot := chartBreakTransportDefaultPivot(2)
	pivot.labels[1] = strings.Repeat("w", 8<<10)
	manager, created := chartBreakTransportRun(t, Config{
		Executor:     chartBreakTransportExecutor(t, pivot, nil),
		MaxPageBytes: 4 << 10,
	}, chartBreakTransportRequest())

	terminal := waitForTerminal(t, manager, created.ID)
	if terminal.State != StateFailed || terminal.Failure == nil || terminal.Failure.Code != FailureResourceLimit {
		t.Fatalf("oversized pivot job = %v failure=%+v", terminal.State, terminal.Failure)
	}
	// The retained snapshot is dropped whole: no schema, no rows, no reader.
	// (Job.RowCount keeps the pre-failure produced-row counter, which is
	// manager-wide progress metadata rather than a retained result.)
	if terminal.Schema != nil {
		t.Fatalf("failed pivot retained schema=%#v", terminal.Schema)
	}
	if strings.Contains(terminal.Failure.Message, "www") || strings.Contains(terminal.Failure.Message, "path") {
		t.Fatalf("failure message leaked result data: %q", terminal.Failure.Message)
	}
	if _, err := manager.Results(created.ID, PageRequest{Limit: 10}); !errors.Is(err, ErrResultsUnavailable) {
		t.Fatalf("Results after byte-limit failure = %v", err)
	}
	if _, err := manager.PreviewFor(chartBreakTransportAccess(), created.ID, 10); !errors.Is(err, ErrResultsUnavailable) {
		t.Fatalf("PreviewFor after byte-limit failure = %v", err)
	}
}

// TestChartBreakTransportManagerTruncatesPivotAtTheRetainedRowCeiling pins the
// manager's own row bound, which sits below the chart operator's 10,000-row
// contract and truncates rather than failing.
func TestChartBreakTransportManagerTruncatesPivotAtTheRetainedRowCeiling(t *testing.T) {
	t.Parallel()

	pivot := chartBreakTransportDefaultPivot(6)
	manager, created := chartBreakTransportRun(t, Config{
		Executor: chartBreakTransportExecutor(t, pivot, nil),
		MaxRows:  3,
	}, chartBreakTransportRequest())

	terminal := waitForTerminal(t, manager, created.ID)
	if terminal.State != StateCompleted || !terminal.ResultsTruncated || terminal.RowCount != 3 {
		t.Fatalf("truncated pivot job = %v truncated=%v rows=%d failure=%+v",
			terminal.State, terminal.ResultsTruncated, terminal.RowCount, terminal.Failure)
	}
	page, err := manager.Results(created.ID, PageRequest{Limit: 10})
	if err != nil {
		t.Fatalf("Results: %v", err)
	}
	if len(page.Rows) != 3 || page.TotalRows != 3 || !page.Complete {
		t.Fatalf("truncated page rows=%d total=%d complete=%v", len(page.Rows), page.TotalRows, page.Complete)
	}
	// Truncation keeps a prefix of the pivot's ordered row axis.
	for index := range page.Rows {
		label, ok := page.Rows[index].Values[0].String()
		if !ok || label != pivot.labels[index] {
			t.Fatalf("truncated row %d = %q (%v), want %q", index, label, ok, pivot.labels[index])
		}
	}
	preview, err := manager.PreviewFor(chartBreakTransportAccess(), created.ID, 10)
	if err != nil {
		t.Fatalf("PreviewFor: %v", err)
	}
	if !preview.Truncated || len(preview.Rows) != 3 {
		t.Fatalf("truncated preview = truncated %v rows %d", preview.Truncated, len(preview.Rows))
	}
}

// TestChartBreakTransportManagerPreviewsAreAllOrNothingWhileBuffering answers
// what a preview subscriber sees for a terminal, fully buffered pivot: nothing
// at all until the executor publishes, then the complete result.
func TestChartBreakTransportManagerPreviewsAreAllOrNothingWhileBuffering(t *testing.T) {
	t.Parallel()

	pivot := chartBreakTransportDefaultPivot(4)
	buffering := make(chan struct{})
	release := make(chan struct{})
	manager, created := chartBreakTransportRun(t, Config{
		Executor: chartBreakTransportExecutor(t, pivot, func() {
			close(buffering)
			<-release
		}),
	}, chartBreakTransportRequest())

	select {
	case <-buffering:
	case <-time.After(3 * time.Second):
		t.Fatal("executor never started buffering the pivot")
	}
	waitForState(t, manager, created.ID, StateRunning)
	if _, err := manager.PreviewFor(chartBreakTransportAccess(), created.ID, 10); !errors.Is(err, ErrResultsNotReady) {
		t.Fatalf("preview while buffering = %v, want ErrResultsNotReady", err)
	}
	close(release)

	terminal := waitForTerminal(t, manager, created.ID)
	if terminal.State != StateCompleted {
		t.Fatalf("pivot job = %v failure=%+v", terminal.State, terminal.Failure)
	}
	preview, err := manager.PreviewFor(chartBreakTransportAccess(), created.ID, 10)
	if err != nil {
		t.Fatalf("PreviewFor: %v", err)
	}
	if preview.Truncated || len(preview.Rows) != len(pivot.labels) || preview.Revision == 0 {
		t.Fatalf("completed preview = truncated %v rows %d revision %d",
			preview.Truncated, len(preview.Rows), preview.Revision)
	}
	if !reflect.DeepEqual(preview.Job.Schema.Columns, pivot.schema("path").Columns) {
		t.Fatalf("preview schema = %#v", preview.Job.Schema)
	}
	// A bounded preview of the same snapshot is a truncated prefix, and the
	// revision never moves for a terminal result.
	bounded, err := manager.PreviewFor(chartBreakTransportAccess(), created.ID, 2)
	if err != nil {
		t.Fatalf("bounded PreviewFor: %v", err)
	}
	if !bounded.Truncated || len(bounded.Rows) != 2 || bounded.Revision != preview.Revision {
		t.Fatalf("bounded preview = truncated %v rows %d revision %d (want revision %d)",
			bounded.Truncated, len(bounded.Rows), bounded.Revision, preview.Revision)
	}
	// A schema-only byte ceiling still returns the runtime-named schema so a
	// subscriber can render an empty pivot rather than an error.
	schemaOnly, err := manager.PreviewForBytes(chartBreakTransportAccess(), created.ID, 10, 1)
	if err != nil {
		t.Fatalf("PreviewForBytes: %v", err)
	}
	if !schemaOnly.Truncated || len(schemaOnly.Rows) != 0 || schemaOnly.Job.Schema == nil {
		t.Fatalf("schema-only preview = truncated %v rows %d schema %#v",
			schemaOnly.Truncated, len(schemaOnly.Rows), schemaOnly.Job.Schema)
	}
}

// TestChartBreakTransportManagerCancellationWhileBufferingKeepsNoResult pins
// the buffered operator's cancellation window at the job boundary.
func TestChartBreakTransportManagerCancellationWhileBufferingKeepsNoResult(t *testing.T) {
	t.Parallel()

	pivot := chartBreakTransportDefaultPivot(4)
	buffering := make(chan struct{})
	release := make(chan struct{})
	manager, created := chartBreakTransportRun(t, Config{
		Executor: chartBreakTransportExecutor(t, pivot, func() {
			close(buffering)
			<-release
		}),
	}, chartBreakTransportRequest())

	select {
	case <-buffering:
	case <-time.After(3 * time.Second):
		t.Fatal("executor never started buffering the pivot")
	}
	if err := manager.CancelFor(chartBreakTransportAccess(), created.ID); err != nil {
		t.Fatalf("CancelFor: %v", err)
	}
	close(release)

	terminal := waitForTerminal(t, manager, created.ID)
	if terminal.State != StateCanceled || terminal.RowCount != 0 || terminal.Schema != nil {
		t.Fatalf("canceled pivot = %v rows=%d schema=%#v", terminal.State, terminal.RowCount, terminal.Schema)
	}
	if _, err := manager.Results(created.ID, PageRequest{Limit: 10}); !errors.Is(err, ErrResultsUnavailable) {
		t.Fatalf("Results after cancellation = %v", err)
	}
	if _, err := manager.AcquireResultsFor(context.Background(), chartBreakTransportAccess(), created.ID); err == nil {
		t.Fatal("canceled pivot handed out a result lease")
	}
}

// TestChartBreakTransportManagerSanitizesPivotExecutionFailures walks each
// classified transport error into its public job failure. The public message
// must never carry the generated SQL or a field value.
func TestChartBreakTransportManagerSanitizesPivotExecutionFailures(t *testing.T) {
	t.Parallel()

	const leak = "SELECT secret_column FROM open_splunk.events WHERE path='/private'"
	for _, test := range []struct {
		name     string
		failure  error
		wantCode FailureCode
	}{
		{
			name:     "execution limit",
			failure:  fmt.Errorf("%w: ClickHouse chart row values exceeded the supported result size: %s", ErrExecutionLimit, leak),
			wantCode: FailureResourceLimit,
		},
		{
			name:     "invalid result",
			failure:  fmt.Errorf("%w: ClickHouse chart ordinal sequence is invalid: %s", ErrInvalidResult, leak),
			wantCode: FailureInternal,
		},
		{
			name:     "unsupported value",
			failure:  fmt.Errorf("%w: %s", ErrUnsupportedValue, leak),
			wantCode: FailureUnsupportedSPL,
		},
		{
			name:     "storage unavailable",
			failure:  fmt.Errorf("%w: %s", ErrStorageUnavailable, leak),
			wantCode: FailureStorageUnavailable,
		},
		{
			name:     "unclassified backend error",
			failure:  errors.New(leak),
			wantCode: FailureExecution,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			manager, created := chartBreakTransportRun(t, Config{
				Executor: executorFunc(func(_ context.Context, query clickhouse.CompiledQuery, _ ResultSink) error {
					if query.Chart == nil {
						t.Errorf("compiled query is not a chart: %#v", query)
					}
					return test.failure
				}),
				NewID: sequenceIDs("chart-failure-" + strings.ReplaceAll(test.name, " ", "-")),
			}, chartBreakTransportRequest())

			terminal := waitForTerminal(t, manager, created.ID)
			if terminal.State != StateFailed || terminal.Failure == nil || terminal.Failure.Code != test.wantCode {
				t.Fatalf("job = %v failure = %+v, want %v", terminal.State, terminal.Failure, test.wantCode)
			}
			if strings.Contains(terminal.Failure.Message, "secret_column") ||
				strings.Contains(terminal.Failure.Message, "/private") ||
				strings.Contains(terminal.Failure.Message, "SELECT") {
				t.Fatalf("failure message leaked backend detail: %q", terminal.Failure.Message)
			}
			if terminal.Schema != nil || terminal.RowCount != 0 {
				t.Fatalf("failed pivot retained schema=%#v rows=%d", terminal.Schema, terminal.RowCount)
			}
			assertValidHistory(t, stateHistory(t, manager, created.ID))
		})
	}
}

// TestChartBreakTransportManagerRejectsMalformedPivotSchemas proves the chart
// sink, not only the executor, refuses a runtime-named schema that violates the
// compiled pivot contract, and that such a job retains nothing.
func TestChartBreakTransportManagerRejectsMalformedPivotSchemas(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name   string
		schema func(rowField string) Schema
		rows   [][]Value
	}{
		{
			name: "series takes the row column name",
			schema: func(rowField string) Schema {
				return Schema{Columns: []Column{
					{Name: rowField, Kind: ValueKindString},
					{Name: rowField, Kind: ValueKindUnsigned},
				}}
			},
		},
		{
			name: "row column renamed to the split field",
			schema: func(string) Schema {
				return Schema{Columns: []Column{
					{Name: "level", Kind: ValueKindString},
					{Name: "INFO", Kind: ValueKindUnsigned},
				}}
			},
		},
		{
			name: "private series name",
			schema: func(rowField string) Schema {
				return Schema{Columns: []Column{
					{Name: rowField, Kind: ValueKindString},
					{Name: "_audit", Kind: ValueKindUnsigned},
				}}
			},
		},
		{
			name: "thirteen runtime series",
			schema: func(rowField string) Schema {
				columns := []Column{{Name: rowField, Kind: ValueKindString}}
				for index := range 13 {
					columns = append(columns, Column{Name: fmt.Sprintf("s%02d", index), Kind: ValueKindUnsigned})
				}
				return Schema{Columns: columns}
			},
		},
		{
			name: "signed count column",
			schema: func(rowField string) Schema {
				return Schema{Columns: []Column{
					{Name: rowField, Kind: ValueKindString},
					{Name: "INFO", Kind: ValueKindSigned},
				}}
			},
		},
		{
			name: "nullable row column for a string row kind",
			schema: func(rowField string) Schema {
				return Schema{Columns: []Column{
					{Name: rowField, Kind: ValueKindString, Nullable: true},
					{Name: "INFO", Kind: ValueKindUnsigned},
				}}
			},
		},
		{
			name: "row cell kind disagrees with the declared schema",
			schema: func(rowField string) Schema {
				return Schema{Columns: []Column{
					{Name: rowField, Kind: ValueKindString},
					{Name: "INFO", Kind: ValueKindUnsigned},
				}}
			},
			rows: [][]Value{{UnsignedValue(7), UnsignedValue(1)}},
		},
		{
			name: "row width disagrees with the published schema",
			schema: func(rowField string) Schema {
				return Schema{Columns: []Column{
					{Name: rowField, Kind: ValueKindString},
					{Name: "INFO", Kind: ValueKindUnsigned},
				}}
			},
			rows: [][]Value{{StringValue("/a")}},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			manager, created := chartBreakTransportRun(t, Config{
				Executor: executorFunc(func(_ context.Context, query clickhouse.CompiledQuery, sink ResultSink) error {
					if query.Chart == nil {
						t.Errorf("compiled query is not a chart: %#v", query)
						return errors.New("not a chart")
					}
					if err := sink.SetSchema(test.schema(query.Chart.RowField)); err != nil {
						return err
					}
					for _, row := range test.rows {
						if err := sink.AddRow(row); err != nil {
							return err
						}
					}
					return nil
				}),
				NewID: sequenceIDs("chart-schema-" + strings.ReplaceAll(test.name, " ", "-")),
			}, chartBreakTransportRequest())

			terminal := waitForTerminal(t, manager, created.ID)
			if terminal.State != StateFailed || terminal.Failure == nil || terminal.Failure.Code != FailureInternal {
				t.Fatalf("job = %v failure = %+v, want an internal failure", terminal.State, terminal.Failure)
			}
			if terminal.Schema != nil || terminal.RowCount != 0 {
				t.Fatalf("rejected pivot retained schema=%#v rows=%d", terminal.Schema, terminal.RowCount)
			}
			if _, err := manager.Results(created.ID, PageRequest{Limit: 10}); !errors.Is(err, ErrResultsUnavailable) {
				t.Fatalf("Results after schema rejection = %v", err)
			}
		})
	}
}
