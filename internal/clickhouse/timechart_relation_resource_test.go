package clickhouse

import (
	"context"
	"errors"
	"math"
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

func TestNewRelationInputRejectsOversizedNestedPayloadBeforeDeepCopy(t *testing.T) {
	policy := searchlimits.SupportedRange().Minimum
	ctx := searchlimits.WithPolicy(context.Background(), policy)
	columns := []RelationColumn{{Name: "values", Type: "Dynamic"}}
	rows := [][]any{{[]any{[]any{strings.Repeat("x", int(policy.MaxResultBytes))}}}}

	var got error
	allocations := testing.AllocsPerRun(10, func() {
		var input *compiledRelationInput
		input, got = newRelationInput(ctx, columns, rows, false)
		if input != nil {
			t.Fatal("oversized relation returned an input")
		}
	})
	if got == nil {
		t.Fatal("oversized nested relation was accepted")
	}
	// The wrapped returned error may escape. No allocation is proportional to the
	// rejected nested payload, in particular no []any/string deep copy occurs.
	if allocations > 2 {
		t.Fatalf("oversized preflight allocations = %v, want only the bounded error", allocations)
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
