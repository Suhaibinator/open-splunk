package queryexec

import (
	"context"
	"errors"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"
	"unsafe"

	"github.com/ClickHouse/clickhouse-go/v2/lib/chcol"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
	"github.com/Suhaibinator/open-splunk/internal/plan"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
	"github.com/Suhaibinator/open-splunk/internal/spl"
)

func TestAtomicResultBarrierRejectsLateFailureWithoutPublishing(t *testing.T) {
	t.Parallel()

	backendFailure := errors.New("backend failed after returning rows")
	closeFailure := errors.New("backend failed while closing rows")
	tests := []struct {
		name     string
		data     [][]any
		rowsErr  error
		closeErr error
		want     error
	}{
		{
			name: "later row conversion failure",
			data: [][]any{
				{chcol.NewDynamicWithType(int64(1), "Int64")},
				{chcol.NewDynamicWithType(complex64(2), "Unsupported")},
			},
			want: searchjobs.ErrInvalidResult,
		},
		{
			name: "backend iteration failure after rows",
			data: [][]any{
				{chcol.NewDynamicWithType(int64(1), "Int64")},
				{chcol.NewDynamicWithType(int64(2), "Int64")},
			},
			rowsErr: backendFailure,
			want:    backendFailure,
		},
		{
			name: "close failure after complete iteration",
			data: [][]any{
				{chcol.NewDynamicWithType(int64(1), "Int64")},
				{chcol.NewDynamicWithType(int64(2), "Int64")},
			},
			closeErr: closeFailure,
			want:     closeFailure,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			query := compileAtomicResultFixture(t)
			rows := atomicResultRows(query, test.data)
			rows.err = test.rowsErr
			rows.closeErr = test.closeErr
			sink := &fakeSink{}

			err := mustExecutor(t, &fakeQueryConnection{rows: rows}).Execute(
				context.Background(),
				query,
				sink,
			)
			if !errors.Is(err, test.want) {
				t.Fatalf("Execute() error = %v, want %v", err, test.want)
			}
			if sink.setCalls != 0 || len(sink.rows) != 0 || len(sink.events) != 0 {
				t.Fatalf(
					"atomic failure published schema=%d rows=%d events=%v",
					sink.setCalls,
					len(sink.rows),
					sink.events,
				)
			}
			if !rows.closed {
				t.Fatal("atomic failure did not close the backend rows")
			}
		})
	}
}

func TestAtomicResultBarrierPublishesOnlyAfterCompleteClose(t *testing.T) {
	t.Parallel()

	query := compileAtomicResultFixture(t)
	rows := atomicResultRows(query, [][]any{
		{chcol.NewDynamicWithType(int64(1), "Int64")},
		{chcol.NewDynamicWithType(int64(2), "Int64")},
	})
	sink := &atomicCloseObservingSink{rows: rows}
	if err := mustExecutor(t, &fakeQueryConnection{rows: rows}).Execute(
		context.Background(),
		query,
		sink,
	); err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if !rows.closed {
		t.Fatal("successful atomic execution left backend rows open")
	}
	if sink.callsBeforeClose != 0 {
		t.Fatalf("sink received %d calls before backend close", sink.callsBeforeClose)
	}
	if sink.setCalls != 1 || !slices.Equal(sink.events, []string{"schema", "row", "row"}) {
		t.Fatalf(
			"atomic publication calls schema=%d events=%v",
			sink.setCalls,
			sink.events,
		)
	}
	if len(sink.rowsOut) != 2 {
		t.Fatalf("published rows = %d, want 2", len(sink.rowsOut))
	}
	for index, row := range sink.rowsOut {
		value, ok := row[0].Signed()
		if !ok || value != int64(index+1) {
			t.Fatalf("published row %d = %#v", index, row)
		}
	}
}

func TestMaximumArithmeticShapeCancellationPublishesNothingPromptly(t *testing.T) {
	t.Parallel()

	query := compileMaximumAtomicArithmeticFixture(t)
	if !strings.Contains(query.SQL, "arrayFold(") {
		t.Fatalf("maximum arithmetic fixture did not select bounded fold lowering:\n%s", query.SQL)
	}
	rows := atomicResultRows(query, [][]any{
		{chcol.NewDynamicWithType(float64(1), "Float64")},
		{chcol.NewDynamicWithType(float64(2), "Float64")},
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var canceledAt time.Time
	rows.afterScan = func() {
		if canceledAt.IsZero() {
			canceledAt = time.Now()
			cancel()
		}
	}
	sink := &fakeSink{}

	err := mustExecutor(t, &fakeQueryConnection{rows: rows}).Execute(ctx, query, sink)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Execute() error = %v, want context.Canceled", err)
	}
	if canceledAt.IsZero() {
		t.Fatal("maximum-shape scan did not trigger cancellation")
	}
	prompt := time.Since(canceledAt)
	if prompt > time.Second {
		t.Fatalf("maximum-shape cancellation returned after %v, want at most 1s", prompt)
	}
	if sink.setCalls != 0 || len(sink.rows) != 0 || len(sink.events) != 0 {
		t.Fatalf(
			"canceled maximum shape published schema=%d rows=%d events=%v",
			sink.setCalls,
			len(sink.rows),
			sink.events,
		)
	}
	if !rows.closed {
		t.Fatal("canceled maximum-shape execution left backend rows open")
	}
	t.Logf("maximum-shape Executor cancellation returned in %v", prompt)
}

func TestAtomicResultPrivateBufferHasExact128MiBBoundary(t *testing.T) {
	t.Parallel()

	if maximumAtomicResultBytes != uint64(128<<20) {
		t.Fatalf("atomic result limit = %d, want 128 MiB", maximumAtomicResultBytes)
	}
	row := []searchjobs.Value{searchjobs.StringValue("bounded")}
	structural := uint64(unsafe.Sizeof(atomicBufferedRowBlock{}))
	charge, err := chargeAtomicResultRow(0, structural, row)
	if err != nil || charge == 0 || charge > maximumAtomicResultBytes {
		t.Fatalf("chargeAtomicResultRow() = (%d, %v)", charge, err)
	}
	if got, err := chargeAtomicResultRow(maximumAtomicResultBytes-charge, structural, row); err != nil || got != maximumAtomicResultBytes {
		t.Fatalf("exact-boundary charge = (%d, %v), want (%d, nil)", got, err, maximumAtomicResultBytes)
	}
	if _, err := chargeAtomicResultRow(maximumAtomicResultBytes-charge+1, structural, row); !errors.Is(err, searchjobs.ErrByteLimit) {
		t.Fatalf("over-boundary charge error = %v, want ErrByteLimit", err)
	}
	if _, err := chargeAtomicResultRow(maximumAtomicResultBytes+1, 0, nil); !errors.Is(err, searchjobs.ErrByteLimit) {
		t.Fatalf("forged current byte count error = %v, want ErrByteLimit", err)
	}
}

func TestAtomicResultPrivateBufferChargesNestedRetainedGraphAndRowBlocks(t *testing.T) {
	t.Parallel()

	leaves := make([]searchjobs.Value, 1_024)
	for index := range leaves {
		leaves[index] = searchjobs.NullValue()
	}
	nested, err := searchjobs.ObjectValue(searchjobs.ObjectField{
		Name:  "nested",
		Value: searchjobs.ListValue(leaves...),
	})
	if err != nil {
		t.Fatal(err)
	}
	retained, err := nested.RetainedSizeBytes()
	if err != nil {
		t.Fatal(err)
	}
	row := []searchjobs.Value{nested}
	structural := uint64(unsafe.Sizeof(atomicBufferedRowBlock{}))
	charge, err := chargeAtomicResultRow(0, structural, row)
	want := structural + retained
	if err != nil || charge != want {
		t.Fatalf("nested atomic charge = (%d, %v), want (%d, nil)", charge, err, want)
	}
	lower, exceeded, err := nested.ProtoSizeLowerBound(maximumAtomicResultBytes)
	if err != nil || exceeded {
		t.Fatalf("nested protobuf lower bound = (%d, %t, %v)", lower, exceeded, err)
	}
	oldCharge := uint64(unsafe.Sizeof(row)) + uint64(unsafe.Sizeof(searchjobs.Value{})) + lower
	if charge <= oldCharge*16 {
		t.Fatalf("nested retained charge %d did not expose old wire undercharge %d", charge, oldCharge)
	}
	if got, err := chargeAtomicResultRow(maximumAtomicResultBytes-charge, structural, row); err != nil || got != maximumAtomicResultBytes {
		t.Fatalf("nested exact-boundary charge = (%d, %v)", got, err)
	}
	if _, err := chargeAtomicResultRow(maximumAtomicResultBytes-charge+1, structural, row); !errors.Is(err, searchjobs.ErrByteLimit) {
		t.Fatalf("nested over-boundary charge error = %v, want ErrByteLimit", err)
	}

	var buffer atomicResultBuffer
	if err := buffer.append(row); err != nil {
		t.Fatal(err)
	}
	if err := buffer.append([]searchjobs.Value{searchjobs.NullValue()}); err != nil {
		t.Fatal(err)
	}
	if buffer.first == nil || buffer.last != buffer.first || buffer.first.next != nil ||
		buffer.first.count != 2 || buffer.first.rows[0][0].Kind() != nested.Kind() ||
		buffer.first.rows[1][0].Kind() != searchjobs.ValueKindNull {
		t.Fatalf("atomic first row block is not bounded and ordered: %#v", buffer)
	}
	secondCharge, err := chargeAtomicResultRow(charge, 0, []searchjobs.Value{searchjobs.NullValue()})
	if err != nil || buffer.bytes != secondCharge {
		t.Fatalf("atomic buffer bytes = %d, want %d (%v)", buffer.bytes, secondCharge, err)
	}
	for index := 2; index < atomicRowsPerBlock; index++ {
		if err := buffer.append([]searchjobs.Value{searchjobs.NullValue()}); err != nil {
			t.Fatal(err)
		}
	}
	beforeSecondBlock := buffer.bytes
	if err := buffer.append([]searchjobs.Value{searchjobs.NullValue()}); err != nil {
		t.Fatal(err)
	}
	if buffer.first.next == nil || buffer.last != buffer.first.next ||
		buffer.first.count != atomicRowsPerBlock || buffer.last.count != 1 ||
		buffer.last.next != nil {
		t.Fatalf("atomic row blocks are not a bounded ordered chain: %#v", buffer)
	}
	wantSecondBlock, err := chargeAtomicResultRow(
		beforeSecondBlock,
		structural,
		[]searchjobs.Value{searchjobs.NullValue()},
	)
	if err != nil || buffer.bytes != wantSecondBlock {
		t.Fatalf("second atomic block bytes = %d, want %d (%v)", buffer.bytes, wantSecondBlock, err)
	}
}

func TestAtomicResultPrivateBufferAllocatesByBlockNotByRow(t *testing.T) {
	row := []searchjobs.Value{searchjobs.NullValue()}
	const rows = 65_536
	allocations := testing.AllocsPerRun(3, func() {
		var buffer atomicResultBuffer
		for range rows {
			if err := buffer.append(row); err != nil {
				panic(err)
			}
		}
		if buffer.first == nil || buffer.last == nil ||
			buffer.bytes == 0 || buffer.last.count != atomicRowsPerBlock {
			panic("atomic block buffer is incomplete")
		}
	})
	wantBlocks := float64(rows / atomicRowsPerBlock)
	if allocations < wantBlocks || allocations > wantBlocks+8 {
		t.Fatalf(
			"atomic %d-row buffer allocations = %.0f, want one per block near %.0f",
			rows,
			allocations,
			wantBlocks,
		)
	}
}

func compileAtomicResultFixture(t *testing.T) clickhouse.CompiledQuery {
	t.Helper()
	return compileAtomicResultSource(
		t,
		`index=main | eval atomic_value=status/2 | table atomic_value`,
	)
}

func compileMaximumAtomicArithmeticFixture(t *testing.T) clickhouse.CompiledQuery {
	t.Helper()
	return compileAtomicResultSource(
		t,
		`index=main | eval atomic_value=status`+
			strings.Repeat(`+0`, spl.MaximumArithmeticOperatorsPerQuery)+
			` | table atomic_value`,
	)
}

func compileAtomicResultSource(t *testing.T, source string) clickhouse.CompiledQuery {
	t.Helper()
	parsed, err := spl.Parse(source)
	if err != nil {
		t.Fatalf("parse atomic-result fixture: %v", err)
	}
	searchStart := time.Date(2026, time.August, 11, 12, 0, 0, 0, time.UTC)
	visibility := uint64(7)
	logical, err := plan.Build(parsed, plan.Scope{
		TenantID:          "tenant",
		AuthorizedIndexes: []string{"main"},
		RequestedIndexes:  []string{"main"},
		Earliest:          searchStart.Add(-time.Hour),
		Latest:            searchStart,
		SearchStart:       searchStart,
		SearchTimezone:    "UTC",
		IndexTimeCutoff:   searchStart,
		VisibilityCutoff:  &visibility,
	})
	if err != nil {
		t.Fatalf("plan atomic-result fixture: %v", err)
	}
	compiled, err := (clickhouse.Compiler{}).Compile(logical)
	if err != nil {
		t.Fatalf("compile atomic-result fixture: %v", err)
	}
	if !compiled.RequiresAtomicResult() {
		t.Fatal("arithmetic fixture did not retain the compiler atomic-result requirement")
	}
	if !slices.Equal(compiled.OutputFields, []string{"atomic_value"}) {
		t.Fatalf("atomic fixture output fields = %v", compiled.OutputFields)
	}
	return compiled
}

func atomicResultRows(query clickhouse.CompiledQuery, data [][]any) *fakeRows {
	return &fakeRows{
		columns: slices.Clone(query.OutputFields),
		types: []driver.ColumnType{fakeColumnType{
			name:         query.OutputFields[0],
			databaseType: "Dynamic",
			scanType:     reflect.TypeFor[any](),
			nullable:     true,
		}},
		data: data,
	}
}

type atomicCloseObservingSink struct {
	rows             *fakeRows
	setCalls         int
	callsBeforeClose int
	events           []string
	rowsOut          [][]searchjobs.Value
}

func (sink *atomicCloseObservingSink) SetSchema(searchjobs.Schema) error {
	sink.observe("schema")
	sink.setCalls++
	return nil
}

func (sink *atomicCloseObservingSink) AddRow(values []searchjobs.Value) error {
	sink.observe("row")
	sink.rowsOut = append(sink.rowsOut, slices.Clone(values))
	return nil
}

func (sink *atomicCloseObservingSink) observe(event string) {
	if !sink.rows.closed {
		sink.callsBeforeClose++
	}
	sink.events = append(sink.events, event)
}
