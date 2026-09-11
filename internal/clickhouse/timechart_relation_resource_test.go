package clickhouse

import (
	"context"
	"errors"
	"math"
	"reflect"
	"strings"
	"testing"
	"unsafe"

	"github.com/Suhaibinator/open-splunk/internal/searchlimits"
)

func TestRelationInputRetainedBytesHasExactNestedBoundary(t *testing.T) {
	t.Parallel()

	columns := []RelationColumn{{Name: "values", Type: "Dynamic"}}
	value := []any{[]any{[]any{"payload", uint64(1)}}}
	rows := [][]any{{value}}

	listBytes := 3*uint64(unsafe.Sizeof([]any{})) +
		4*uint64(unsafe.Sizeof(any(nil)))
	valueBytes := listBytes + uint64(unsafe.Sizeof("")) + uint64(len("payload")) +
		uint64(unsafe.Sizeof(uint64(0)))
	want := uint64(unsafe.Sizeof(compiledRelationInput{})) +
		uint64(unsafe.Sizeof(RelationColumn{})) +
		uint64(unsafe.Sizeof([]any{})) +
		uint64(unsafe.Sizeof(any(nil))) +
		uint64(len(columns[0].Name)+len(columns[0].Type)) + valueBytes

	got, err := relationInputRetainedBytes(
		context.Background(),
		columns,
		rows,
		1,
		want,
	)
	if err != nil || got != want {
		t.Fatalf("relationInputRetainedBytes() = (%d, %v), want (%d, nil)", got, err, want)
	}
	if _, err := relationInputRetainedBytes(
		context.Background(),
		columns,
		rows,
		1,
		want-1,
	); err == nil {
		t.Fatal("nested relation fit below its exact retained boundary")
	}
}

func TestNewRelationInputUsesTheRemainingStageBudget(t *testing.T) {
	t.Parallel()

	columns := []RelationColumn{{Name: "values", Type: "Dynamic"}}
	rows := [][]any{{[]any{[]any{"payload", uint64(1)}}}}
	retained, err := relationInputRetainedBytes(
		context.Background(),
		columns,
		rows,
		1,
		math.MaxUint64,
	)
	if err != nil {
		t.Fatal(err)
	}
	below := searchlimits.WithRemainingExecutionBytes(context.Background(), retained-1)
	if input, err := newRelationInput(below, columns, rows, false); !errors.Is(err, ErrTimechartResourceLimit) || input != nil {
		t.Fatalf("below-boundary newRelationInput() = (%#v, %v)", input, err)
	}
	atLimit := searchlimits.WithRemainingExecutionBytes(context.Background(), retained)
	input, err := newRelationInput(atLimit, columns, rows, false)
	if err != nil || input == nil || input.retainedBytes != retained {
		t.Fatalf("exact-boundary newRelationInput() = (%#v, %v), retained=%d", input, err, retained)
	}
}

func TestMaterializeRelationInputUsesExactNativeRepresentationBudget(t *testing.T) {
	t.Parallel()

	columns := []RelationColumn{
		{Name: "values", Type: "Dynamic"},
		{Name: "label", Type: "String"},
	}
	rows := [][]any{{[]any{[]any{"payload", uint64(1)}}, "series"}}
	input, err := newRelationInput(context.Background(), columns, rows, false)
	if err != nil {
		t.Fatal(err)
	}
	nativeBytes, ok, err := relationInputNativeMaterializationBytes(context.Background(), input)
	if err != nil || !ok || nativeBytes <= input.retainedBytes {
		t.Fatalf(
			"relationInputNativeMaterializationBytes() = (%d, %t, %v), input=%d",
			nativeBytes,
			ok,
			err,
			input.retainedBytes,
		)
	}
	below := searchlimits.WithRemainingExecutionBytes(context.Background(), nativeBytes-1)
	if table, err := materializeRelationInput(below, input); !errors.Is(err, ErrTimechartResourceLimit) || table != nil {
		t.Fatalf("below-boundary materializeRelationInput() = (%#v, %v)", table, err)
	}
	atLimit := searchlimits.WithRemainingExecutionBytes(context.Background(), nativeBytes)
	table, err := materializeRelationInput(atLimit, input)
	if err != nil || table == nil || table.Block().Rows() != 1 {
		t.Fatalf("exact-boundary materializeRelationInput() = (%#v, %v)", table, err)
	}
}

func TestNativeRelationNullableNilCellsIncludeBaseColumnAndCapacityFloor(t *testing.T) {
	t.Parallel()

	tests := []struct {
		kind      string
		cellBytes uint64
	}{
		{kind: "Nullable(UInt64)", cellBytes: 18},
		{kind: "Nullable(Int64)", cellBytes: 18},
		{kind: "Nullable(Float64)", cellBytes: 18},
		{kind: "Nullable(DateTime64(9, 'UTC'))", cellBytes: 18},
		{kind: "Nullable(String)", cellBytes: 34},
		{kind: "Nullable(Bool)", cellBytes: 4},
		{kind: "Nullable(Dynamic)", cellBytes: 34},
	}
	for _, test := range tests {
		got, ok := nativeRelationCellBytes(test.kind, nil)
		if !ok || got != test.cellBytes {
			t.Errorf("nativeRelationCellBytes(%q, nil) = (%d, %t), want (%d, true)", test.kind, got, ok, test.cellBytes)
		}
		if minimum := nativeColumnInitialCapacityBytes(test.kind, 1); minimum < 16 {
			t.Errorf("nativeColumnInitialCapacityBytes(%q, 1) = %d, want at least 16", test.kind, minimum)
		}
	}
	if minimum := nativeColumnInitialCapacityBytes("Bool", 1); minimum != 8 {
		t.Fatalf("non-nullable bool minimum backing = %d, want 8", minimum)
	}
}

func TestNativeRelationAllNullEstimateCoversActualDriverSliceCapacities(t *testing.T) {
	t.Parallel()

	columns := []RelationColumn{
		{Name: "unsigned", Type: "Nullable(UInt64)"},
		{Name: "signed", Type: "Nullable(Int64)"},
		{Name: "double", Type: "Nullable(Float64)"},
		{Name: "timestamp", Type: "Nullable(DateTime64(9, 'UTC'))"},
		{Name: "text", Type: "Nullable(String)"},
		{Name: "flag", Type: "Nullable(Bool)"},
		{Name: "dynamic", Type: "Nullable(Dynamic)"},
	}
	const rowCount = 1_025
	rows := make([][]any, rowCount)
	for index := range rows {
		rows[index] = make([]any, len(columns))
	}
	input, err := newRelationInput(context.Background(), columns, rows, false)
	if err != nil {
		t.Fatal(err)
	}
	nativeBytes, ok, err := relationInputNativeMaterializationBytes(context.Background(), input)
	if err != nil || !ok {
		t.Fatalf("native estimate = (%d, %t, %v)", nativeBytes, ok, err)
	}
	table, err := materializeRelationInput(context.Background(), input)
	if err != nil {
		t.Fatal(err)
	}
	if table.Block().Rows() != rowCount {
		t.Fatalf("driver rows = %d, want %d", table.Block().Rows(), rowCount)
	}
	actualSliceBytes := uint64(0)
	for index, nativeColumn := range table.Block().Columns {
		actualColumnBytes := driverColumnSliceCapacityBytes(reflect.ValueOf(nativeColumn))
		actualSliceBytes += actualColumnBytes
		cellBytes, ok := nativeRelationCellBytes(columns[index].Type, nil)
		if !ok {
			t.Fatalf("nil cell estimate rejected %s", columns[index].Type)
		}
		estimatedColumnBytes := rowCount*cellBytes +
			nativeColumnInitialCapacityBytes(columns[index].Type, rowCount)
		if estimatedColumnBytes < actualColumnBytes {
			t.Errorf(
				"%s native estimate %d does not cover actual driver slice capacities %d",
				columns[index].Type,
				estimatedColumnBytes,
				actualColumnBytes,
			)
		}
	}
	if actualSliceBytes == 0 || nativeBytes < actualSliceBytes {
		t.Fatalf("native estimate %d does not cover actual driver slice capacities %d", nativeBytes, actualSliceBytes)
	}
}

func driverColumnSliceCapacityBytes(value reflect.Value) uint64 {
	if !value.IsValid() {
		return 0
	}
	switch value.Kind() {
	case reflect.Interface:
		if value.IsNil() {
			return 0
		}
		return driverColumnSliceCapacityBytes(value.Elem())
	case reflect.Pointer:
		if value.IsNil() || !driverResourceType(value.Type().Elem()) {
			return 0
		}
		return driverColumnSliceCapacityBytes(value.Elem())
	case reflect.Struct:
		if !driverResourceType(value.Type()) {
			return 0
		}
		var total uint64
		for _, field := range value.Fields() {
			total += driverColumnSliceCapacityBytes(field)
		}
		return total
	case reflect.Slice:
		return uint64(value.Cap()) * uint64(value.Type().Elem().Size())
	default:
		return 0
	}
}

func driverResourceType(value reflect.Type) bool {
	path := value.PkgPath()
	return strings.Contains(path, "/clickhouse-go/v2/lib/column") ||
		strings.Contains(path, "/ch-go/proto")
}

func TestNewRelationInputRejectsOversizedNestedPayloadBeforeDeepCopy(t *testing.T) {
	policy := searchlimits.SupportedRange().Minimum
	ctx := searchlimits.WithPolicy(context.Background(), policy)
	columns := []RelationColumn{{Name: "values", Type: "Dynamic"}}
	measure := func(value any) (testing.BenchmarkResult, error) {
		rows := [][]any{{value}}
		var got error
		result := testing.Benchmark(func(b *testing.B) {
			b.ReportAllocs()
			for range b.N {
				input, err := newRelationInput(ctx, columns, rows, false)
				got = err
				if input != nil {
					b.Fatal("oversized relation returned an input")
				}
			}
		})
		return result, got
	}

	compact := []any{[]any{strings.Repeat("x", int(policy.MaxResultBytes))}}
	expanded := make([]any, 4_097)
	for index := range expanded[:len(expanded)-1] {
		expanded[index] = []any{uint64(index)}
	}
	expanded[len(expanded)-1] = []any{strings.Repeat("x", 8*int(policy.MaxResultBytes))}

	compactResult, compactErr := measure(compact)
	expandedResult, expandedErr := measure(expanded)
	if !errors.Is(compactErr, ErrTimechartResourceLimit) ||
		!errors.Is(expandedErr, ErrTimechartResourceLimit) {
		t.Fatalf("oversized relation errors = (%v, %v), want resource limits", compactErr, expandedErr)
	}
	// Benchmark accounting can differ by a few bytes or one allocation under the
	// race runtime. The expanded input adds seven times the byte limit and thousands
	// of nested slices, so that constant drift cannot conceal a deep copy.
	if expandedResult.AllocedBytesPerOp() > compactResult.AllocedBytesPerOp()+1<<10 {
		t.Fatalf(
			"oversized preflight bytes/op = compact %d, expanded %d; allocation grew with rejected payload",
			compactResult.AllocedBytesPerOp(),
			expandedResult.AllocedBytesPerOp(),
		)
	}
	if expandedResult.AllocsPerOp() > compactResult.AllocsPerOp()+1 {
		t.Fatalf(
			"oversized preflight allocations/op = compact %d, expanded %d; allocations grew with rejected nesting",
			compactResult.AllocsPerOp(),
			expandedResult.AllocsPerOp(),
		)
	}
}

func TestRelationInputRetainedBytesHonorsCancellationBeforeRows(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := relationInputRetainedBytes(
		ctx,
		[]RelationColumn{{Name: "values", Type: "Dynamic"}},
		[][]any{{[]any{"value"}}},
		1,
		1<<20,
	)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("relationInputRetainedBytes() error = %v, want context.Canceled", err)
	}
}
