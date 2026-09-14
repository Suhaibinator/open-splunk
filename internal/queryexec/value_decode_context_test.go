package queryexec

import (
	"context"
	"errors"
	"math"
	"reflect"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/chcol"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
	"github.com/Suhaibinator/open-splunk/internal/eventfields"
	"github.com/Suhaibinator/open-splunk/internal/plan"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
	"github.com/Suhaibinator/open-splunk/internal/searchlimits"
	"github.com/Suhaibinator/open-splunk/internal/spl"
)

func TestValueDecoderCancelsNestedConversionAndAtomicAccounting(t *testing.T) {
	native := make([]any, 4096)
	for index := range native {
		native[index] = []any{uint64(index), "text"}
	}
	ctx := &stagePollingContext{Context: context.Background(), cancelAt: 8}
	value, err := convertValueContext(ctx, chcol.NewDynamicWithType(native, "Array(Dynamic)"))
	if !errors.Is(err, context.Canceled) || value.Kind() != searchjobs.ValueKindInvalid {
		t.Fatalf("canceled conversion: kind=%v err=%v", value.Kind(), err)
	}
	value, err = convertValue(native)
	if err != nil {
		t.Fatal(err)
	}
	ctx = &stagePollingContext{Context: context.Background(), cancelAt: 4}
	buffer := atomicResultBuffer{}
	err = buffer.appendContext(ctx, []searchjobs.Value{value})
	if !errors.Is(err, context.Canceled) || buffer.first != nil {
		t.Fatalf("canceled accounting retained rows=%v err=%v", buffer.first != nil, err)
	}
}

func TestSealedContinuationCancelsInsideDecoderBeforePublication(t *testing.T) {
	first := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	parsed, err := spl.Parse(`index=target | timechart span=1h count BY host | eval payload="{\"items\":[1,2]}" | spath input=payload output=items path=items | table items`)
	if err != nil {
		t.Fatal(err)
	}
	visibility := uint64(1)
	logical, err := plan.Build(parsed, plan.Scope{TenantID: "tenant", AuthorizedIndexes: []string{"target"}, Earliest: first, Latest: first.Add(time.Hour), SearchStart: first.Add(time.Hour), IndexTimeCutoff: first.Add(time.Hour), VisibilityCutoff: &visibility, SearchTimezone: "UTC"})
	if err != nil {
		t.Fatal(err)
	}
	compiled, err := (clickhouse.Compiler{}).Compile(logical)
	if err != nil {
		t.Fatal(err)
	}
	if !compiled.HasContinuation() || !compiled.HasValidExecutionSeal() {
		t.Fatal("fixture is not a sealed stage")
	}
	compiled, err = compiled.ContinueWithTimeBucketsAndWorkContext(context.Background(), []clickhouse.RelationColumn{{Name: "_time", Type: "DateTime64(9, 'UTC')"}, {Name: "api", Type: "UInt64"}}, [][]any{{first, uint64(1)}}, []time.Time{first.Add(time.Hour)}, 0)
	if err != nil {
		t.Fatal(err)
	}
	if !compiled.HasValidExecutionSeal() {
		t.Fatal("continuation lost authority")
	}
	native := make([]any, 4096)
	for index := range native {
		native[index] = uint64(index)
	}
	ctx := &stagePollingContext{Context: context.Background(), cancelAt: math.MaxInt}
	finalRows := &fakeRows{columns: []string{"items"}, types: []driver.ColumnType{fakeColumnType{name: "items", databaseType: "Dynamic", scanType: reflect.TypeFor[any]()}}, data: [][]any{{chcol.NewDynamicWithType(native, "Array(Dynamic)")}}}
	var scanned []any
	finalRows.observeScan = func(destinations []any) { scanned = destinations }
	finalRows.afterScan = func() {
		// Measure the actual preflight walk rather than hard-coding its node count.
		// Cancel shortly after that walk, while constructing the retained list.
		preflight := &stagePollingContext{Context: context.Background(), cancelAt: math.MaxInt}
		if err := preflightDecodedRow(preflight, scanned, 64<<20); err != nil {
			t.Fatal(err)
		}
		ctx.cancelAt = ctx.checks + preflight.checks + 4
	}
	connection := &terminalTimechartQueueConnection{rows: []driver.Rows{finalRows}}
	sink := &terminalTimechartSink{}
	err = mustExecutor(t, connection).Execute(searchlimits.WithRemainingExecutionBytes(ctx, 64<<20), compiled, sink)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("post-preflight cancellation: checks=%d err=%v", ctx.checks, err)
	}
	if connection.calls != 1 || sink.compiledCalls != 0 || sink.setCalls != 0 || len(sink.rows) != 0 || len(sink.bounds) != 0 {
		t.Fatalf("canceled continuation published: %+v", sink)
	}
}

func TestContextDecoderMatchesNestedAndSortedSemantics(t *testing.T) {
	native := map[string]any{"z": []any{uint64(1), nil}, "a": map[string]any{"nested": "value"}, "m": true, "maximum": uint64(math.MaxUint64), "time": time.Date(2026, 9, 1, 0, 0, 0, 123456789, time.UTC), "bytes": []byte{0, 255}, "decimal": map[string]string{extendedTypeKey: "decimal/v1", extendedValueKey: "12345678901234567890.123456789"}}
	want, err := convertValue(native)
	if err != nil {
		t.Fatal(err)
	}
	got, err := convertValueContext(context.Background(), native)
	if err != nil || !reflect.DeepEqual(got, want) {
		t.Fatalf("context conversion changed semantics: %v", err)
	}
	for size := range 40 {
		values := make([]int, size)
		for index := range values {
			values[index] = (size - index) % 7
		}
		if err := sortDecodedValues(&resultValueDecoder{ctx: context.Background()}, values, func(a, b int) int { return a - b }); err != nil {
			t.Fatal(err)
		}
		for index := 1; index < len(values); index++ {
			if values[index] < values[index-1] {
				t.Fatalf("unsorted size%d: %v", size, values)
			}
		}
	}
}

func TestSpecializedDecodersPollNestedValues(t *testing.T) {
	members := make([]any, 4096)
	for index := range members {
		members[index] = uint64(index)
	}
	document := chcol.NewJSON()
	document.SetValueAtPath("payload", chcol.NewDynamicWithType(members, "Array(Dynamic)"))
	names, types := containerOutputMetadata(containerOutputMetadataField{"payload", eventfields.StoredValueTypeList})
	for _, test := range []struct {
		name string
		run  func(*resultValueDecoder) (searchjobs.Value, error)
	}{
		{"JSON", func(decoder *resultValueDecoder) (searchjobs.Value, error) { return decoder.convertJSON(document) }},
		{"sparse", func(decoder *resultValueDecoder) (searchjobs.Value, error) {
			return decoder.convertSparseEventFieldsWithCache(document, []string{"payload"}, false, nil, nil)
		}},
		{"container", func(decoder *resultValueDecoder) (searchjobs.Value, error) {
			return decoder.convertContainerOutputWithCache(map[string]any{"payload": members}, names, types, eventfields.CurrentFieldMetadataVersion, nil, nil)
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx := &stagePollingContext{Context: context.Background(), cancelAt: 8}
			value, err := test.run(&resultValueDecoder{ctx: ctx})
			if !errors.Is(err, context.Canceled) || value.Kind() != searchjobs.ValueKindInvalid {
				t.Fatalf("specialized decoder canceled kind=%v err=%v", value.Kind(), err)
			}
		})
	}
}
