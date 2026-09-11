package clickhouse

import (
	"context"
	"crypto/sha256"
	"errors"
	"math"
	"testing"
	"time"
)

func TestNestedRelationTraversalsPollWithinSingleCell(t *testing.T) {
	items := make([]any, 4096)
	for i := range items {
		items[i] = uint64(i)
	}
	for _, phase := range []struct {
		name string
		run  func(*relationTraversal)
	}{
		{"retained and native size", func(walk *relationTraversal) { _, _, _ = walk.preflightTypedValue("Dynamic", items, nil) }},
		{"clone and commitment", func(walk *relationTraversal) { _, _ = walk.cloneAndWriteValue(sha256.New(), items, 0) }},
		{"native conversion", func(walk *relationTraversal) { _ = walk.nativeDynamic(items) }},
	} {
		t.Run(phase.name, func(t *testing.T) {
			ctx := &cancelAfterLookupChecks{Context: context.Background(), cancelAt: 3}
			walk := relationTraversal{ctx: ctx}
			phase.run(&walk)
			if !errors.Is(walk.err, context.Canceled) {
				t.Fatalf("nested phase ignored cancellation: %v", walk.err)
			}
			if walk.nodes > 513 {
				t.Fatalf("traversal continued after cancellation: nodes=%d", walk.nodes)
			}
		})
	}
}

func TestRelationBucketEndCommitmentPollsWithinBounds(t *testing.T) {
	ends := make([]time.Time, 4_096)
	ctx := &cancelAfterLookupChecks{Context: context.Background(), cancelAt: 3}
	if _, err := relationCommitmentWithBucketEnds(ctx, [sha256.Size]byte{}, ends); !errors.Is(err, context.Canceled) {
		t.Fatalf("bucket-end commitment ignored cancellation: %v", err)
	}
	if ctx.calls > 4 {
		t.Fatalf("bucket-end commitment continued after cancellation: checks=%d", ctx.calls)
	}
}

func TestRelationDynamicGraphChecksCancellationAtCompletion(t *testing.T) {
	ctx := &cancelAfterLookupChecks{Context: context.Background(), cancelAt: 1}
	if _, _, err := nativeDynamicColumnGraphBytes(ctx, [relationDynamicDepths]relationDynamicKindMask{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("Dynamic graph completed after final cancellation: %v", err)
	}
}

func TestRelationPreflightCancelsDuringRowMetadata(t *testing.T) {
	rows := make([][]any, 1_024)
	for index := range rows {
		rows[index] = []any{make(chan struct{})}
	}
	ctx := &cancelAfterLookupChecks{Context: context.Background(), cancelAt: 20}
	if _, err := preflightRelationInput(
		ctx,
		[]RelationColumn{{Name: "value", Type: "UInt64"}},
		rows,
		nil,
		uint64(len(rows)),
		math.MaxUint64,
	); !errors.Is(err, context.Canceled) {
		t.Fatalf("row metadata phase ignored cancellation: %v", err)
	}
}

func TestRelationMaterializationPhasesCheckCompletion(t *testing.T) {
	columns := []RelationColumn{{Name: "value", Type: "Dynamic"}}
	rows := [][]any{{[]any{uint64(1), "text", []any{true}}}}
	input, err := newRelationInput(context.Background(), columns, rows)
	if err != nil {
		t.Fatal(err)
	}
	for _, phase := range []struct {
		name string
		run  func(context.Context) error
	}{
		{"retained size", func(ctx context.Context) error {
			_, err := relationInputRetainedBytesForTest(ctx, columns, rows, 1<<20)
			return err
		}},
		{"clone and commitment", func(ctx context.Context) error { _, err := newRelationInput(ctx, columns, rows); return err }},
		{"native estimate", func(ctx context.Context) error {
			_, _, err := relationInputNativeMaterializationBytes(ctx, input)
			return err
		}},
		{"bucket-end commitment", func(ctx context.Context) error {
			_, err := relationCommitmentWithBucketEnds(ctx, input.commitment, []time.Time{time.Unix(1, 0).UTC()})
			return err
		}},
		{"native append", func(ctx context.Context) error { _, err := materializeValidatedRelationInput(ctx, input); return err }},
	} {
		t.Run(phase.name, func(t *testing.T) {
			count := &cancelAfterLookupChecks{Context: context.Background(), cancelAt: math.MaxInt}
			if err := phase.run(count); err != nil {
				t.Fatal(err)
			}
			// Cancel exactly at the successful phase's final check. This avoids a
			// scheduler race while proving cancellation cannot return success.
			ctx := &cancelAfterLookupChecks{Context: context.Background(), cancelAt: count.calls}
			if err := phase.run(ctx); !errors.Is(err, context.Canceled) {
				t.Fatalf("completed after final cancellation: %v", err)
			}
		})
	}
}

func TestPublicRelationContinuationCancelsNestedCell(t *testing.T) {
	compiled := compileSPL(t, `index=gradethis | timechart span=1h count BY host | table _time values`)
	items := make([]any, 16384)
	for i := range items {
		items[i] = uint64(i)
	}
	columns := []RelationColumn{{Name: "_time", Type: "DateTime64(9, 'UTC')"}, {Name: "values", Type: "Dynamic"}}
	rows := [][]any{{time.Date(2026, 7, 21, 0, 0, 0, 0, time.UTC), items}}
	ctx := &cancelAfterLookupChecks{Context: context.Background(), cancelAt: 20}
	if _, err := compiled.ContinueContext(ctx, columns, rows); !errors.Is(err, context.Canceled) {
		t.Fatalf("public continuation ignored nested cancellation: %v", err)
	}
	next, err := compiled.ContinueContext(context.Background(), columns, rows)
	if err != nil {
		t.Fatal(err)
	}
	ctx = &cancelAfterLookupChecks{Context: context.Background(), cancelAt: 20}
	if tables, err := next.ExternalTablesForExecution(ctx); !errors.Is(err, context.Canceled) || len(tables) != 0 {
		t.Fatalf("public native materialization completed after cancellation: tables=%d err=%v", len(tables), err)
	}
}
