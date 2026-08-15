package queryexec

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/chcol"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
)

// hostileScanRows reproduces the two driver behaviors that make a hoisted,
// reused destination dangerous, which the fake used by the rest of the package
// deliberately does not: on NULL the pinned clickhouse-go Nullable reader
// leaves the destination completely untouched for the scan types it does not
// special case, and an array column is decoded by appending into whatever
// buffer the destination already holds. Both are safe only because the row loop
// clears every destination at the top of the iteration.
type hostileScanRows struct {
	*fakeRows
}

func (rows *hostileScanRows) Scan(destinations ...any) error {
	if rows.index == 0 || rows.index > len(rows.data) {
		return errors.New("scan outside active row")
	}
	values := rows.data[rows.index-1]
	if len(values) != len(destinations) {
		return errors.New("destination count mismatch")
	}
	for index, source := range values {
		if source == nil {
			// The pinned driver writes nothing at all for these NULLs.
			continue
		}
		if labels, ok := source.([]string); ok {
			target, targetOK := destinations[index].(*[]string)
			if !targetOK {
				return errors.New("array destination is not *[]string")
			}
			*target = append((*target)[:0], labels...)
			continue
		}
		if err := assignFakeScan(destinations[index], source); err != nil {
			return err
		}
	}
	return nil
}

// TestExecuteSurvivesADriverThatSkipsNullAndRecyclesArrayBuffers drives the
// streaming path with a driver that never writes a NULL and always recycles the
// destination's array buffer. A stale pointer would republish row zero's
// message on row one and alias every published tag list onto one backing array.
func TestExecuteSurvivesADriverThatSkipsNullAndRecyclesArrayBuffers(t *testing.T) {
	t.Parallel()

	timestamp := time.Date(2026, time.August, 15, 4, 5, 6, 0, time.UTC)
	base := &fakeRows{
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
			{timestamp, "first", []string{"alpha", "beta"}},
			{timestamp.Add(time.Second), nil, []string{"gamma", "delta"}},
			{timestamp.Add(2 * time.Second), nil, []string{"epsilon", "zeta"}},
			{timestamp.Add(3 * time.Second), "fourth", []string{"eta", "theta"}},
		},
	}
	query := clickhouse.CompiledQuery{
		SQL:          "SELECT scoped",
		Args:         []any{"tenant"},
		OutputFields: []string{"_time", "message", "tags"},
	}
	sink := &fakeSink{}
	executor := mustExecutor(t, &fakeQueryConnection{rows: &hostileScanRows{fakeRows: base}})
	if err := executor.Execute(context.Background(), query, sink); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if len(sink.rows) != 4 {
		t.Fatalf("published rows = %d, want 4", len(sink.rows))
	}
	if value, ok := sink.rows[0][1].String(); !ok || value != "first" {
		t.Fatalf("row 0 message = %#v", sink.rows[0][1])
	}
	for _, ordinal := range []int{1, 2} {
		if !sink.rows[ordinal][1].IsNull() {
			t.Fatalf("row %d message = %#v, want NULL", ordinal, sink.rows[ordinal][1])
		}
	}
	if value, ok := sink.rows[3][1].String(); !ok || value != "fourth" {
		t.Fatalf("row 3 message = %#v", sink.rows[3][1])
	}
	for ordinal, want := range [][]string{
		{"alpha", "beta"}, {"gamma", "delta"}, {"epsilon", "zeta"}, {"eta", "theta"},
	} {
		list, ok := sink.rows[ordinal][2].List()
		if !ok || len(list) != len(want) {
			t.Fatalf("row %d tags = %#v", ordinal, sink.rows[ordinal][2])
		}
		for index, label := range want {
			if got, gotOK := list[index].String(); !gotOK || got != label {
				t.Fatalf("row %d tag %d = %#v, want %q", ordinal, index, list[index], label)
			}
		}
	}
}

// TestExecuteSurvivesASkippedNullOnTheAtomicPath repeats the NULL-skipping
// driver on the buffered atomic path, whose loop body reaches `continue` before
// the bottom of the iteration.
func TestExecuteSurvivesASkippedNullOnTheAtomicPath(t *testing.T) {
	t.Parallel()

	query := compileAtomicResultFixture(t)
	base := atomicResultRows(query, [][]any{
		{chcol.NewDynamicWithType(int64(11), "Int64")},
		{nil},
		{chcol.NewDynamicWithType(int64(13), "Int64")},
	})
	sink := &fakeSink{}
	executor := mustExecutor(t, &fakeQueryConnection{rows: &hostileScanRows{fakeRows: base}})
	if err := executor.Execute(context.Background(), query, sink); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if len(sink.rows) != 3 {
		t.Fatalf("published rows = %d, want 3", len(sink.rows))
	}
	if value, ok := sink.rows[0][0].Signed(); !ok || value != 11 {
		t.Fatalf("row 0 = %#v", sink.rows[0][0])
	}
	if !sink.rows[1][0].IsNull() {
		t.Fatalf("row 1 = %#v, want NULL", sink.rows[1][0])
	}
	if value, ok := sink.rows[2][0].Signed(); !ok || value != 13 {
		t.Fatalf("row 2 = %#v", sink.rows[2][0])
	}
}

// TestScanDestinationsRejectsAColumnWithoutAScanType pins the one failure the
// hoisted call can report before the row loop begins.
func TestScanDestinationsRejectsAColumnWithoutAScanType(t *testing.T) {
	t.Parallel()

	if _, err := scanDestinations([]driver.ColumnType{
		fakeColumnType{name: "broken", databaseType: "String", scanType: nil},
	}); err == nil {
		t.Fatal("scanDestinations accepted a column without a scan type")
	}
	// Dynamic columns bypass the scan type entirely, so a nil scan type there is
	// not a failure.
	destinations, err := scanDestinations([]driver.ColumnType{
		fakeColumnType{name: "dynamic", databaseType: "Dynamic", scanType: nil},
	})
	if err != nil {
		t.Fatalf("scanDestinations for Dynamic: %v", err)
	}
	if _, ok := destinations[0].(*chcol.Dynamic); !ok {
		t.Fatalf("Dynamic destination = %T, want *chcol.Dynamic", destinations[0])
	}
}

// TestClearScanDestinationsToleratesHostileEntries proves the clear is total
// over the destinations it can write and silently skips the ones it cannot,
// rather than panicking part way through and leaving later entries stale.
func TestClearScanDestinationsToleratesHostileEntries(t *testing.T) {
	t.Parallel()

	text := "stale"
	pointer := &text
	list := []string{"stale"}
	trailing := uint64(7)
	destinations := []any{
		nil,
		(*string)(nil),
		"not a pointer",
		&pointer,
		&list,
		&trailing,
	}
	clearScanDestinations(destinations)
	if pointer != nil || list != nil || trailing != 0 {
		t.Fatalf("clear left pointer=%v list=%v trailing=%d", pointer, list, trailing)
	}
	if text != "stale" {
		t.Fatalf("clear mutated the pointee: %q", text)
	}
}
