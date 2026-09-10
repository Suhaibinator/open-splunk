package queryexec

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"unsafe"

	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
	"github.com/Suhaibinator/open-splunk/internal/searchlimits"
)

func TestTimechartRemainingBudgetReachesAllStageAllocators(t *testing.T) {
	policy := searchlimits.Default()
	ctx := searchlimits.WithPolicy(context.Background(), policy)
	budget := &stageBudget{maxRetained: 64 << 20, retained: 60 << 20, maxMemory: 64 << 20}
	ctx = budget.allocationContext(ctx)
	remaining, ok := searchlimits.RemainingExecutionBytes(ctx)
	if !ok || remaining != 1<<20 {
		t.Fatalf("remaining=%d present=%v", remaining, ok)
	}
	base, err := validatedQuerySettings(Config{})
	if err != nil {
		t.Fatal(err)
	}
	executor := &Executor{settings: base}
	originalMemory, originalBytes := base.limit("max_memory_usage"), base.limit("max_result_bytes")
	standalone := searchlimits.WithRemainingExecutionBytes(context.Background(), remaining)
	if _, err := executor.settingsForContext(standalone, clickhouse.CompiledQuery{}); err != nil {
		t.Fatal(err)
	}
	if base.limit("max_memory_usage") != originalMemory || base.limit("max_result_bytes") != originalBytes {
		t.Fatal("stage limit mutated frozen executor settings")
	}
	settings, err := executor.settingsForContext(ctx, clickhouse.CompiledQuery{})
	if err != nil {
		t.Fatal(err)
	}
	if settings["max_result_bytes"] != remaining || settings["max_memory_usage"] != remaining {
		t.Fatalf("unbounded stage settings: %v", settings)
	}
	query := clickhouse.CompiledQuery{Timechart: &clickhouse.TimechartOutput{Mode: clickhouse.TimechartModeRuntimeWide}}
	limits, err := timechartResourceLimitsForContext(ctx, settings, query)
	if err != nil {
		t.Fatal(err)
	}
	if limits.retainedBytes != remaining {
		t.Fatalf("decoder and SQL allowance=%d", limits.retainedBytes)
	}
	buffer := atomicResultBuffer{maximumBytes: uint64(unsafe.Sizeof(atomicBufferedRowBlock{}))}
	if err := buffer.append([]searchjobs.Value{searchjobs.StringValue("x")}); !errors.Is(err, searchjobs.ErrByteLimit) || buffer.first != nil {
		t.Fatalf("atomic preallocation cap: %v", err)
	}
	tiny := searchlimits.WithRemainingExecutionBytes(ctx, 1)
	if err := validateFixedTimechartAllocation(tiny, clickhouse.TimechartOutput{BucketCount: 10000}, 1, 8); !errors.Is(err, searchjobs.ErrExecutionLimit) {
		t.Fatalf("fixed grid preallocation cap: %v", err)
	}
	zero := searchlimits.WithRemainingExecutionBytes(ctx, 0)
	if _, err := executor.settingsForContext(zero, clickhouse.CompiledQuery{}); !errors.Is(err, searchjobs.ErrExecutionLimit) {
		t.Fatalf("zero remainder reset to default: %v", err)
	}
}

func TestStageDynamicValueUsesOneBackingPerList(t *testing.T) {
	value := searchjobs.ListValue(searchjobs.StringValue("outer"), searchjobs.ListValue(searchjobs.UnsignedValue(2), searchjobs.ListValue(searchjobs.BoolValue(true))))
	got, err := stageDynamicValue(context.Background(), value)
	if err != nil {
		t.Fatal(err)
	}
	want := []any{"outer", []any{uint64(2), []any{true}}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got=%#v want=%#v", got, want)
	}
}

func TestTimechartBudgetReleasesCompletedIntermediateOwnership(t *testing.T) {
	budget := &stageBudget{maxRetained: 64 << 20, retained: 60 << 20, rows: 100, bytes: 1000}
	if err := budget.replaceResident(1<<20, 2<<20); err != nil {
		t.Fatal(err)
	}
	if budget.retained != 3<<20 || budget.rows != 100 || budget.bytes != 1000 {
		t.Fatalf("resident ownership reset damaged scan totals: %+v", budget)
	}
	if err := budget.replaceResident(1<<20, 64<<20); !errors.Is(err, searchjobs.ErrExecutionLimit) {
		t.Fatalf("resident overflow=%v", err)
	}
}

func TestTimechartReservesClientMemoryWithoutConflatingResultLimit(t *testing.T) {
	budget := &stageBudget{maxRetained: 64 << 20, maxMemory: 1 << 30, retained: 4 << 20}
	ctx := budget.allocationContext(context.Background())
	result, _ := searchlimits.RemainingExecutionBytes(ctx)
	memory, _ := searchlimits.RemainingExecutionMemoryBytes(ctx)
	if result != 15<<20 || memory+budget.retained+3*result != budget.maxMemory {
		t.Fatalf("result=%d memory=%d", result, memory)
	}
}

func TestTimechartDecoderRejectsPrimitiveExpansionBeforeValueAllocation(t *testing.T) {
	// Nullable primitives occupy pointer slots in the native driver, whereas
	// each decoded element occupies an entire tagged Value struct.
	values := make([]*uint64, 1024)
	var native any = values
	if err := preflightDecodedRow(context.Background(), []any{&native}, uint64(len(values))*uint64(unsafe.Sizeof(uintptr(0)))); !errors.Is(err, searchjobs.ErrExecutionLimit) {
		t.Fatalf("decoded expansion accepted: %v", err)
	}
	nested := any([]any{[]any{values}})
	if err := preflightDecodedRow(context.Background(), []any{&nested}, 1024); !errors.Is(err, searchjobs.ErrExecutionLimit) {
		t.Fatalf("nested expansion accepted: %v", err)
	}
	if err := preflightDecodedRow(context.Background(), []any{&native}, uint64(len(values)+1)*uint64(unsafe.Sizeof(searchjobs.Value{}))); err != nil {
		t.Fatalf("exact Value slot budget: %v", err)
	}
}
