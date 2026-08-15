package queryexec

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/chcol"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
)

// observeScanDestinationReuse asserts that the row loop hands the driver the
// same destination pointers on every row and that each destination is back in
// its zero state before Scan runs. The second half is load bearing: the pinned
// driver's Nullable reader leaves a destination untouched for scan types it
// does not special case (Nullable(Bool), Nullable(UUID), Nullable(Decimal),
// Nullable(Int128), Nullable(IPv6)), so a reused destination that was not
// cleared first would silently repeat the previous row's value on NULL.
func observeScanDestinationReuse(t *testing.T, rows *fakeRows, scans *int) {
	t.Helper()
	var first []uintptr
	rows.observeScan = func(destinations []any) {
		current := make([]uintptr, len(destinations))
		for index, destination := range destinations {
			if *scans > 0 && !reflect.ValueOf(destination).Elem().IsZero() {
				t.Errorf("scan destination %d retained the prior row", index)
			}
			current[index] = reflect.ValueOf(destination).Pointer()
		}
		if *scans == 0 {
			first = current
		} else if !reflect.DeepEqual(current, first) {
			t.Errorf("scan destinations changed: got %v, want %v", current, first)
		}
		*scans++
	}
}

// TestExecuteClearsReusedScanDestinationsBetweenStreamedRows covers the
// streaming path, where every row is converted and forwarded to the sink.
func TestExecuteClearsReusedScanDestinationsBetweenStreamedRows(t *testing.T) {
	t.Parallel()

	timestamp := time.Date(2026, time.August, 15, 1, 2, 3, 0, time.UTC)
	rows := &fakeRows{
		columns: []string{"_time", "message", "tags"},
		types: []driver.ColumnType{
			fakeColumnType{name: "_time", databaseType: "DateTime64(9, 'UTC')", scanType: reflect.TypeFor[time.Time]()},
			fakeColumnType{
				name:         "message",
				databaseType: "Nullable(String)",
				scanType:     reflect.TypeFor[*string](),
				nullable:     true,
			},
			fakeColumnType{name: "tags", databaseType: "Array(String)", scanType: reflect.TypeFor[[]string]()},
		},
		data: [][]any{
			{timestamp, "first", []string{"a", "b"}},
			{timestamp.Add(time.Second), nil, []string{}},
			{timestamp.Add(2 * time.Second), "third", []string{"c"}},
		},
	}
	scans := 0
	observeScanDestinationReuse(t, rows, &scans)

	sink := &fakeSink{}
	query := clickhouse.CompiledQuery{
		SQL:          "SELECT scoped",
		Args:         []any{"tenant"},
		OutputFields: []string{"_time", "message", "tags"},
	}
	if err := mustExecutor(t, &fakeQueryConnection{rows: rows}).Execute(context.Background(), query, sink); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if scans != 3 || len(sink.rows) != 3 {
		t.Fatalf("scans=%d published rows=%d, want three of each", scans, len(sink.rows))
	}
	// The cleared middle row must still publish NULL rather than inheriting the
	// first row's message.
	if !sink.rows[1][1].IsNull() {
		t.Fatalf("middle row message = %#v, want NULL", sink.rows[1][1])
	}
	if value, ok := sink.rows[2][1].String(); !ok || value != "third" {
		t.Fatalf("third row message = %#v", sink.rows[2][1])
	}
}

// TestExecuteClearsReusedScanDestinationsOnTheAtomicPath covers the atomic
// buffer path, whose loop body takes a `continue` before reaching the bottom of
// the iteration. A clear placed at the end of the body would never run here.
func TestExecuteClearsReusedScanDestinationsOnTheAtomicPath(t *testing.T) {
	t.Parallel()

	query := compileAtomicResultFixture(t)
	rows := atomicResultRows(query, [][]any{
		{chcol.NewDynamicWithType(int64(1), "Int64")},
		{chcol.NewDynamicWithType(int64(2), "Int64")},
		{chcol.NewDynamicWithType(int64(3), "Int64")},
	})
	scans := 0
	observeScanDestinationReuse(t, rows, &scans)

	sink := &fakeSink{}
	if err := mustExecutor(t, &fakeQueryConnection{rows: rows}).Execute(context.Background(), query, sink); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if scans != 3 || len(sink.rows) != 3 {
		t.Fatalf("scans=%d published rows=%d, want three of each", scans, len(sink.rows))
	}
}
