package clickhouse

import (
	"context"
	"crypto/sha256"
	"errors"
	"reflect"
	"testing"
	"time"
	"unsafe"

	"github.com/Suhaibinator/open-splunk/internal/searchlimits"
)

func TestLookupExternalTableNativeMaterializationUsesRemainingBudget(t *testing.T) {
	t.Parallel()

	table := testCompiledLookupExternalTable(t, [][]string{
		{"", "", ""},
		{"", "", ""},
	})
	nativeBytes, ok, err := lookupExternalTablesNativeMaterializationBytes(
		context.Background(),
		[]compiledLookupExternalTable{table},
	)
	if err != nil || !ok || nativeBytes <= table.backing.retainedBytes {
		t.Fatalf(
			"lookup native estimate = (%d, %t, %v), retained=%d",
			nativeBytes,
			ok,
			err,
			table.backing.retainedBytes,
		)
	}
	below := searchlimits.WithRemainingExecutionBytes(
		context.Background(),
		nativeBytes-1,
	)
	if tables, err := materializeCompiledLookupExternalTables(
		below,
		[]compiledLookupExternalTable{table},
	); !errors.Is(err, ErrTimechartResourceLimit) || tables != nil {
		t.Fatalf("below-boundary lookup materialization = (%#v, %v)", tables, err)
	}
	atLimit := searchlimits.WithRemainingExecutionBytes(context.Background(), nativeBytes)
	tables, err := materializeCompiledLookupExternalTables(
		atLimit,
		[]compiledLookupExternalTable{table},
	)
	if err != nil || len(tables) != 1 || tables[0].Block().Rows() != 3 {
		t.Fatalf("exact-boundary lookup materialization = (%#v, %v)", tables, err)
	}
}

func TestLookupExternalTableWithoutRemainingBudgetPreservesAdmission(t *testing.T) {
	t.Parallel()

	const rowCount = 20_000
	values := [][]string{make([]string, rowCount), make([]string, rowCount)}
	table := testCompiledLookupExternalTable(t, values)
	nativeBytes, ok, err := lookupExternalTablesNativeMaterializationBytes(
		context.Background(),
		[]compiledLookupExternalTable{table},
	)
	minimum := searchlimits.SupportedRange().Minimum
	if err != nil || !ok || nativeBytes <= minimum.MaxResultBytes {
		t.Fatalf(
			"lookup native estimate = (%d, %t, %v), minimum result cap=%d",
			nativeBytes,
			ok,
			err,
			minimum.MaxResultBytes,
		)
	}
	ctx := searchlimits.WithPolicy(context.Background(), minimum)
	tables, err := materializeCompiledLookupExternalTables(
		ctx,
		[]compiledLookupExternalTable{table},
	)
	if err != nil || len(tables) != 1 || tables[0].Block().Rows() != rowCount {
		t.Fatalf("lookup without remaining cap = (%#v, %v)", tables, err)
	}
}

func TestLookupAndRelationNativeMaterializationShareOneBudget(t *testing.T) {
	t.Parallel()

	lookups := []compiledLookupExternalTable{
		testCompiledLookupExternalTable(t, [][]string{{"first"}}),
		testCompiledLookupExternalTableNamed(t, "__os_lookup_table_second", [][]string{{"second"}}),
	}
	input, err := newRelationInput(
		context.Background(),
		[]RelationColumn{
			{Name: "_time", Type: "DateTime64(9, 'UTC')"},
			{Name: "owner", Type: "String"},
		},
		[][]any{{time.Unix(0, 0).UTC(), "first"}},
	)
	if err != nil {
		t.Fatal(err)
	}
	lookupBytes, ok, err := lookupExternalTablesNativeMaterializationBytes(
		context.Background(),
		lookups,
	)
	if err != nil || !ok {
		t.Fatalf("lookup estimate = (%d, %t, %v)", lookupBytes, ok, err)
	}
	relationBytes, ok, err := relationInputNativeMaterializationBytes(
		context.Background(),
		input,
	)
	if err != nil || !ok {
		t.Fatalf("relation estimate = (%d, %t, %v)", relationBytes, ok, err)
	}
	combinedBytes, ok, err := externalTablesNativeMaterializationBytes(
		context.Background(),
		lookups,
		input,
	)
	if err != nil || !ok || combinedBytes <= max(lookupBytes, relationBytes) {
		t.Fatalf(
			"combined estimate = (%d, %t, %v), lookup=%d relation=%d",
			combinedBytes,
			ok,
			err,
			lookupBytes,
			relationBytes,
		)
	}
	// Both independent transports fit this cap, while their aggregate does not.
	individualCap := max(lookupBytes, relationBytes)
	below := searchlimits.WithRemainingExecutionBytes(context.Background(), individualCap)
	if tables, err := materializeCompiledExternalTables(
		below,
		lookups,
		input,
	); !errors.Is(err, ErrTimechartResourceLimit) || tables != nil {
		t.Fatalf("combined materialization below aggregate = (%#v, %v)", tables, err)
	}
	atLimit := searchlimits.WithRemainingExecutionBytes(context.Background(), combinedBytes)
	tables, err := materializeCompiledExternalTables(atLimit, lookups, input)
	if err != nil || len(tables) != 3 {
		t.Fatalf("combined materialization at aggregate = (%#v, %v)", tables, err)
	}
	for index, table := range tables {
		if table == nil || table.Block().Rows() != 1 {
			t.Fatalf("combined table %d = %#v", index, table)
		}
	}
}

func TestLookupEmptyStringNativeEstimateCoversPinnedDriverCapacities(t *testing.T) {
	t.Parallel()

	const rowCount = 100_000
	values := make([][]string, 2)
	for index := range values {
		values[index] = make([]string, rowCount)
	}
	table := testCompiledLookupExternalTable(t, values)
	const retainedStringHeaders = 3_200_000
	if got := uint64(rowCount) * uint64(len(values)) * uint64(unsafe.Sizeof("")); got != retainedStringHeaders {
		t.Fatalf("retained String headers = %d, want %d", got, retainedStringHeaders)
	}

	tables, err := materializeCompiledLookupExternalTables(
		context.Background(),
		[]compiledLookupExternalTable{table},
	)
	if err != nil {
		t.Fatal(err)
	}
	actualDriverBytes := uint64(0)
	for _, nativeColumn := range tables[0].Block().Columns {
		actualDriverBytes += driverColumnSliceCapacityBytes(reflect.ValueOf(nativeColumn))
	}
	const pinnedDriverBytes = 3_899_392
	if actualDriverBytes != pinnedDriverBytes {
		t.Fatalf("pinned driver slice capacities = %d, want %d", actualDriverBytes, pinnedDriverBytes)
	}
	nativeBytes, ok, err := lookupExternalTablesNativeMaterializationBytes(
		context.Background(),
		[]compiledLookupExternalTable{table},
	)
	if err != nil || !ok || nativeBytes < actualDriverBytes {
		t.Fatalf(
			"native estimate = (%d, %t, %v), actual driver slices=%d",
			nativeBytes,
			ok,
			err,
			actualDriverBytes,
		)
	}
}

func BenchmarkExternalTablesNativeMaterializationPreflight(b *testing.B) {
	const rowCount = 10_000
	values := [][]string{make([]string, rowCount), make([]string, rowCount)}
	lookup := testCompiledLookupExternalTable(b, values)
	rows := make([][]any, rowCount)
	for index := range rows {
		rows[index] = []any{uint64(index), ""}
	}
	relation, err := newRelationInput(
		context.Background(),
		[]RelationColumn{{Name: "count", Type: "UInt64"}, {Name: "owner", Type: "String"}},
		rows,
	)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if _, ok, err := externalTablesNativeMaterializationBytes(
			context.Background(),
			[]compiledLookupExternalTable{lookup},
			relation,
		); err != nil || !ok {
			b.Fatalf("native preflight = (%t, %v)", ok, err)
		}
	}
}

func testCompiledLookupExternalTable(
	t testing.TB,
	values [][]string,
) compiledLookupExternalTable {
	t.Helper()
	return testCompiledLookupExternalTableNamed(t, "__os_lookup_table_test", values)
}

func testCompiledLookupExternalTableNamed(
	t testing.TB,
	name string,
	values [][]string,
) compiledLookupExternalTable {
	t.Helper()
	backing, err := authenticateCompiledLookupExternalBackingContext(
		context.Background(),
		values,
	)
	if err != nil {
		t.Fatal(err)
	}
	columns := make([]compiledLookupExternalColumn, len(values))
	for index := range columns {
		columns[index].name = "__os_lookup_value_" + string(rune('a'+index))
	}
	return compiledLookupExternalTable{
		name:           name,
		tenantID:       "tenant-1",
		definitionName: "service_catalog",
		logicalID:      name,
		logicalVersion: 1,
		objectID:       "asset-1",
		version:        1,
		sizeBytes:      1,
		contentSHA256:  sha256.Sum256([]byte(name)),
		matchedColumn:  "__os_lookup_matched_test",
		columns:        columns,
		backing:        backing,
	}
}
