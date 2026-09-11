package clickhouse

import (
	"context"
	"crypto/sha256"
	"errors"
	"hash"
	"math"
	"testing"
	"time"
	"unsafe"

	"github.com/ClickHouse/clickhouse-go/v2/ext"
	"github.com/ClickHouse/clickhouse-go/v2/lib/column"
	driverproto "github.com/ClickHouse/clickhouse-go/v2/lib/proto"
	"github.com/Suhaibinator/open-splunk/internal/searchlimits"
)

func TestRelationInputCloneAndCommitmentPreserveEncodingAndDetach(t *testing.T) {
	stamp := time.Unix(1_789_000_000, 123_456_789).UTC()
	typedNil := []any(nil)
	empty := []any{}
	nested := []any{typedNil, empty, []any{"text", int64(-3), uint64(7), math.Float64frombits(0x7ff8000000000042), false, stamp}}
	columns := []RelationColumn{
		{Name: "_time", Type: "DateTime64(9, 'UTC')"},
		{Name: "value", Type: "Dynamic"},
		{Name: "measure", Type: "Float64"},
		{Name: "optional", Type: "Nullable(Dynamic)"},
	}
	rows := [][]any{
		{stamp, nested, math.Copysign(0, -1), nil},
		{stamp.Add(time.Second), []any{math.Inf(1)}, math.Float64frombits(0x7ff8000000000011), nil},
	}
	ends := []time.Time{stamp.Add(time.Second), stamp.Add(2 * time.Second)}
	wantCommitment := legacyRelationCommitment(t, columns, rows, ends)

	input, err := newRelationInputWithTimeBuckets(context.Background(), columns, rows, ends, false)
	if err != nil {
		t.Fatal(err)
	}
	if input.commitment != wantCommitment {
		t.Fatalf("fused commitment = %x, want legacy %x", input.commitment, wantCommitment)
	}
	originalCommitment := input.commitment
	columns[1].Name = "changed"
	rows[0][1] = nil
	nested[2].([]any)[0] = "changed"
	ends[0] = stamp.Add(24 * time.Hour)
	if input.columns[1].Name != "value" || input.rows[0][1].([]any)[2].([]any)[0] != "text" ||
		input.bucketEnds[0] != stamp.Add(time.Second) || input.commitment != originalCommitment {
		t.Fatalf("source mutation changed detached relation: %#v %#v", input.rows[0], input.bucketEnds)
	}

	nilInput, err := newRelationInput(context.Background(), []RelationColumn{{Name: "value", Type: "Dynamic"}}, [][]any{{[]any(nil)}})
	if err != nil {
		t.Fatal(err)
	}
	emptyInput, err := newRelationInput(context.Background(), []RelationColumn{{Name: "value", Type: "Dynamic"}}, [][]any{{[]any{}}})
	if err != nil {
		t.Fatal(err)
	}
	if nilInput.commitment == emptyInput.commitment {
		t.Fatal("typed-nil and present-empty Dynamic arrays have the same commitment")
	}
}

func TestRelationLifecycleWalksDynamicOncePerAllocationPhase(t *testing.T) {
	items := make([]any, 4_096)
	for index := range items {
		items[index] = []any{uint64(index)}
	}
	wantNodes := uint64(1 + 2*len(items))
	preflight := relationTraversal{ctx: context.Background()}
	if _, _, ok := preflight.preflightTypedValue("Dynamic", items); !ok || preflight.err != nil || preflight.nodes != wantNodes {
		t.Fatalf("preflight = (ok=%t, err=%v, nodes=%d), want (true, nil, %d)", ok, preflight.err, preflight.nodes, wantNodes)
	}
	clone := relationTraversal{ctx: context.Background()}
	if _, ok := clone.cloneAndWriteValue(sha256.New(), items, 0); !ok || clone.err != nil || clone.nodes != wantNodes {
		t.Fatalf("clone+commitment = (ok=%t, err=%v, nodes=%d), want (true, nil, %d)", ok, clone.err, clone.nodes, wantNodes)
	}
}

func TestRelationNativeEstimateIsCachedAndExact(t *testing.T) {
	stamp := time.Unix(10, 0).UTC()
	columns := []RelationColumn{
		{Name: "_time", Type: "DateTime64(9, 'UTC')"},
		{Name: "value", Type: "Dynamic"},
		{Name: "optional", Type: "Nullable(String)"},
		{Name: "nullable_dynamic", Type: "Nullable(Dynamic)"},
		{Name: "flag", Type: "Bool"},
	}
	rows := [][]any{{stamp, []any{nil, []any{}, []any{uint64(1), "text"}}, nil, nil, true}}
	input, err := newRelationInputWithTimeBuckets(context.Background(), columns, rows, []time.Time{stamp.Add(time.Second)}, false)
	if err != nil {
		t.Fatal(err)
	}
	if input.dynamicColumnCount != 1 {
		t.Fatalf("cached Dynamic column count = %d, want 1", input.dynamicColumnCount)
	}
	if want := relationNativeBytesOracle(t, input); input.nativeBytes != want {
		t.Fatalf("cached native bytes = %d, want oracle %d", input.nativeBytes, want)
	}
	ctx := &cancelAfterLookupChecks{Context: context.Background(), cancelAt: math.MaxInt}
	got, ok, err := relationInputNativeMaterializationBytes(ctx, input)
	if err != nil || !ok || got != input.nativeBytes || ctx.calls != 1 {
		t.Fatalf("cached native estimate = (%d, %t, %v), checks=%d", got, ok, err, ctx.calls)
	}
	below := searchlimits.WithRemainingExecutionBytes(context.Background(), input.nativeBytes-1)
	if table, err := materializeRelationInput(below, input); table != nil || !errors.Is(err, ErrTimechartResourceLimit) {
		t.Fatalf("bounds relation below native cache = (%#v, %v)", table, err)
	}
	exact := searchlimits.WithRemainingExecutionBytes(context.Background(), input.nativeBytes)
	if table, err := materializeRelationInput(exact, input); err != nil || table == nil || table.Block().Rows() != 1 {
		t.Fatalf("bounds relation at native cache = (%#v, %v)", table, err)
	}
}

func TestScalarRelationMaterializationSkipsCellConversionTraversal(t *testing.T) {
	const rowCount = 300
	columns := []RelationColumn{
		{Name: "count", Type: "UInt64"},
		{Name: "label", Type: "String"},
		{Name: "flag", Type: "Bool"},
	}
	rows := make([][]any, rowCount)
	for index := range rows {
		rows[index] = []any{uint64(index), "series", index%2 == 0}
	}
	input, err := newRelationInput(context.Background(), columns, rows)
	if err != nil {
		t.Fatal(err)
	}
	if input.dynamicColumnCount != 0 {
		t.Fatalf("scalar relation cached %d Dynamic columns", input.dynamicColumnCount)
	}
	ctx := &cancelAfterLookupChecks{Context: context.Background(), cancelAt: math.MaxInt}
	if _, err := materializeValidatedRelationInput(ctx, input); err != nil {
		t.Fatal(err)
	}
	// One entry check, one definition-traversal poll, two row-boundary checks
	// per row, and one completion check. Scalar cells add no traversal polls.
	if want := 2*rowCount + 3; ctx.calls != want {
		t.Fatalf("scalar materialization context checks = %d, want %d", ctx.calls, want)
	}
}

func TestRelationBoundsRejectBeforePayloadAndPreservePresentEmpty(t *testing.T) {
	large := make([]any, 16_384)
	for index := range large {
		large[index] = []any{uint64(index)}
	}
	columns := []RelationColumn{{Name: "_time", Type: "DateTime64(9, 'UTC')"}, {Name: "value", Type: "Dynamic"}}
	stamp := time.Unix(100, 0).UTC()
	rows := [][]any{{stamp, large}}
	ctx := &cancelAfterLookupChecks{Context: context.Background(), cancelAt: math.MaxInt}
	if input, err := newRelationInputWithTimeBuckets(ctx, columns, rows, []time.Time{}, false); input != nil ||
		err == nil || err.Error() != "continue timechart: invalid bucket bounds" || ctx.calls != 1 {
		t.Fatalf("invalid bounds = (%#v, %v), checks=%d", input, err, ctx.calls)
	}
	if input, err := newRelationInputWithTimeBuckets(context.Background(), columns, rows, []time.Time{stamp}, false); input != nil ||
		err == nil || err.Error() != "continue timechart: invalid bucket interval" {
		t.Fatalf("invalid interval = (%#v, %v)", input, err)
	}
	validEnds := []time.Time{stamp.Add(time.Second)}
	preflight, err := preflightRelationInput(context.Background(), columns, rows, validEnds, 1, math.MaxUint64)
	if err != nil {
		t.Fatal(err)
	}
	below := searchlimits.WithRemainingExecutionBytes(context.Background(), preflight.retainedBytes-1)
	if input, err := newRelationInputWithTimeBuckets(below, columns, rows, validEnds, false); input != nil ||
		!errors.Is(err, ErrTimechartResourceLimit) {
		t.Fatalf("bounds relation below retained cache = (%#v, %v)", input, err)
	}
	exact := searchlimits.WithRemainingExecutionBytes(context.Background(), preflight.retainedBytes)
	if input, err := newRelationInputWithTimeBuckets(exact, columns, rows, validEnds, false); err != nil || input == nil {
		t.Fatalf("bounds relation at retained cache = (%#v, %v)", input, err)
	}

	emptyInput, err := newRelationInputWithTimeBuckets(context.Background(), columns, nil, []time.Time{}, false)
	if err != nil {
		t.Fatal(err)
	}
	if emptyInput.bucketEnds == nil {
		t.Fatal("present-empty bounds became absent")
	}
	table, err := materializeRelationInput(context.Background(), emptyInput)
	if err != nil {
		t.Fatal(err)
	}
	if len(table.Block().Columns) != len(columns)+1 || table.Block().Rows() != 0 {
		t.Fatalf("present-empty bounds schema = %d columns, %d rows", len(table.Block().Columns), table.Block().Rows())
	}
}

func legacyRelationCommitment(t *testing.T, columns []RelationColumn, rows [][]any, ends []time.Time) [sha256.Size]byte {
	t.Helper()
	digest := sha256.New()
	writeTokenPart(digest, "timechart-external-relation-v1")
	for _, descriptor := range columns {
		writeTokenPart(digest, descriptor.Name)
		writeTokenPart(digest, descriptor.Type)
	}
	for _, row := range rows {
		for _, value := range row {
			if !legacyWriteRelationValue(digest, value, 0) {
				t.Fatalf("legacy commitment rejected %#v", value)
			}
		}
	}
	var result [sha256.Size]byte
	copy(result[:], digest.Sum(nil))
	if ends == nil {
		return result
	}
	endDigest := sha256.New()
	_, _ = endDigest.Write(result[:])
	for _, end := range ends {
		if !writeCompiledArgument(endDigest, end, 0) {
			t.Fatalf("legacy bounds commitment rejected %v", end)
		}
	}
	copy(result[:], endDigest.Sum(nil))
	return result
}

func legacyWriteRelationValue(digest hash.Hash, value any, depth int) bool {
	if depth > 17 {
		return false
	}
	if items, ok := value.([]any); ok {
		writeTokenPart(digest, "Array(Dynamic)")
		writeBool(digest, items == nil)
		writeUint64(digest, uint64(len(items)))
		for _, item := range items {
			if !legacyWriteRelationValue(digest, item, depth+1) {
				return false
			}
		}
		return true
	}
	return writeCompiledArgument(digest, value, 0)
}

func relationNativeBytesOracle(t *testing.T, input *compiledRelationInput) uint64 {
	t.Helper()
	physicalColumns := len(input.columns)
	if input.bucketEnds != nil {
		physicalColumns++
	}
	total := uint64(unsafe.Sizeof(ext.Table{})) + uint64(unsafe.Sizeof(driverproto.Block{})) +
		uint64(unsafe.Sizeof(column.ServerContext{}))
	total += uint64(physicalColumns) * (2*uint64(unsafe.Sizeof("")) +
		3*uint64(unsafe.Sizeof(any(nil))) + uint64(unsafe.Sizeof((func(*ext.Table) error)(nil))))
	dynamicColumns := 0
	for _, descriptor := range input.columns {
		total += nativeColumnDescriptorBytes(descriptor)
		total += nativeColumnInitialCapacityBytes(descriptor.Type, len(input.rows))
		if descriptor.Type == "Dynamic" {
			dynamicColumns++
		}
	}
	total += uint64(dynamicColumns) * uint64(unsafe.Sizeof(int(0)))
	if input.bucketEnds != nil {
		bounds := RelationColumn{Name: ResultTimeBucketEndColumn, Type: "DateTime64(9, 'UTC')"}
		total += nativeColumnDescriptorBytes(bounds)
		total += nativeColumnInitialCapacityBytes(bounds.Type, len(input.rows))
	}
	for rowIndex, row := range input.rows {
		for columnIndex, value := range row {
			cell, ok := nativeRelationCellBytesForTest(input.columns[columnIndex].Type, value)
			if !ok {
				t.Fatalf("native oracle rejected row %d column %d", rowIndex, columnIndex)
			}
			total += cell
		}
		if input.bucketEnds != nil {
			total += 2 * uint64(unsafe.Sizeof(time.Time{}))
		}
	}
	return total
}
