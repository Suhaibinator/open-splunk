package queryexec

import (
	"context"
	"errors"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	clickhousedriver "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
	"github.com/Suhaibinator/open-splunk/internal/plan"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
	"github.com/Suhaibinator/open-splunk/internal/spl"
)

// TestStatsByLateUnsupportedValueIsAtomicAndRedacted makes the backend failure
// maximally adversarial: a valid-looking row arrives before the iterator
// reports the runtime stats-BY marker. No prefix or schema may become visible,
// and backend details must be replaced by the stable public sentinel.
func TestStatsByLateUnsupportedValueIsAtomicAndRedacted(t *testing.T) {
	t.Parallel()

	query := compileStatsByAtomicFixture(t)
	if !query.RequiresAtomicResult() {
		t.Fatal("runtime-validated stats BY did not require atomic result execution")
	}
	if !slices.Equal(query.OutputFields, []string{"grouping", "count"}) {
		t.Fatalf("stats BY outputs = %v, want [grouping count]", query.OutputFields)
	}
	backendSecret := "sdet-secret-generated-sql"
	rows := &fakeRows{
		columns: slices.Clone(query.OutputFields),
		types: []driver.ColumnType{
			fakeColumnType{name: "grouping", databaseType: "String", scanType: reflect.TypeFor[string]()},
			fakeColumnType{name: "count", databaseType: "UInt64", scanType: reflect.TypeFor[uint64]()},
		},
		data: [][]any{{"apparently-valid", uint64(1)}},
		err: &clickhousedriver.Exception{
			Code: 395,
			Name: "FUNCTION_THROW_IF_VALUE_IS_NON_ZERO",
			Message: clickhouse.UnsupportedStatsByValueMarker +
				"; while evaluating " + backendSecret,
		},
	}
	sink := &fakeSink{}

	err := mustExecutor(t, &fakeQueryConnection{rows: rows}).Execute(
		context.Background(),
		query,
		sink,
	)
	if !errors.Is(err, searchjobs.ErrUnsupportedValue) {
		t.Fatalf("Execute() error = %v, want ErrUnsupportedValue", err)
	}
	if strings.Contains(err.Error(), clickhouse.UnsupportedStatsByValueMarker) ||
		strings.Contains(err.Error(), backendSecret) {
		t.Fatalf("classified stats BY error leaked backend detail: %v", err)
	}
	if sink.setCalls != 0 || len(sink.rows) != 0 || len(sink.events) != 0 {
		t.Fatalf(
			"late stats BY failure published schema=%d rows=%d events=%v",
			sink.setCalls,
			len(sink.rows),
			sink.events,
		)
	}
	if !rows.closed {
		t.Fatal("late stats BY failure did not close backend rows")
	}
}

// TestStatsByLateExpansionLimitIsAtomicAndRedacted pins the fixed-multivalue
// expansion path independently from Dynamic-value validation. A backend row
// may arrive before ClickHouse reports the per-event Cartesian-product guard;
// the executor must still publish nothing and expose only the public resource
// sentinel.
func TestStatsByLateExpansionLimitIsAtomicAndRedacted(t *testing.T) {
	t.Parallel()

	query := compileStatsByFixedMultivalueAtomicFixture(t)
	if !query.RequiresAtomicResult() {
		t.Fatal("fixed-multivalue stats BY did not require atomic result execution")
	}
	backendSecret := "sdet-secret-expansion-detail"
	types := make([]driver.ColumnType, len(query.OutputFields))
	row := make([]any, len(query.OutputFields))
	for index, field := range query.OutputFields {
		if field == "count" {
			types[index] = fakeColumnType{name: field, databaseType: "UInt64", scanType: reflect.TypeFor[uint64]()}
			row[index] = uint64(1)
			continue
		}
		types[index] = fakeColumnType{name: field, databaseType: "String", scanType: reflect.TypeFor[string]()}
		row[index] = "apparently-valid"
	}
	columns := slices.Clone(query.OutputFields)
	for _, descriptor := range query.StringOrBytesOutputs {
		columns = append(columns, descriptor.SemanticBytesColumn())
		types = append(types, fakeColumnType{
			name: descriptor.SemanticBytesColumn(), databaseType: "UInt8",
			scanType: reflect.TypeFor[uint8](),
		})
		row = append(row, uint8(0))
	}
	rows := &fakeRows{
		columns: columns,
		types:   types,
		data:    [][]any{row},
		err: &clickhousedriver.Exception{
			Code: 395,
			Name: "FUNCTION_THROW_IF_VALUE_IS_NON_ZERO",
			Message: clickhouse.StatsMultivalueByExpansionLimitMarker +
				"; while evaluating " + backendSecret,
		},
	}
	sink := &fakeSink{}

	err := mustExecutor(t, &fakeQueryConnection{rows: rows}).Execute(
		context.Background(),
		query,
		sink,
	)
	if !errors.Is(err, searchjobs.ErrExecutionLimit) {
		t.Fatalf("Execute() error = %v, want ErrExecutionLimit", err)
	}
	if strings.Contains(err.Error(), clickhouse.StatsMultivalueByExpansionLimitMarker) ||
		strings.Contains(err.Error(), backendSecret) {
		t.Fatalf("classified stats BY expansion error leaked backend detail: %v", err)
	}
	if sink.setCalls != 0 || len(sink.rows) != 0 || len(sink.events) != 0 {
		t.Fatalf(
			"late stats BY expansion failure published schema=%d rows=%d events=%v",
			sink.setCalls,
			len(sink.rows),
			sink.events,
		)
	}
	if !rows.closed {
		t.Fatal("late stats BY expansion failure did not close backend rows")
	}
}

func compileStatsByAtomicFixture(t *testing.T) clickhouse.CompiledQuery {
	t.Helper()
	parsed, err := spl.Parse(`index=main | stats count BY grouping | search count>=1`)
	if err != nil {
		t.Fatalf("parse stats BY atomic fixture: %v", err)
	}
	searchStart := time.Date(2026, time.August, 12, 18, 0, 0, 0, time.UTC)
	visibility := uint64(17)
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
		t.Fatalf("plan stats BY atomic fixture: %v", err)
	}
	compiled, err := (clickhouse.Compiler{}).Compile(logical)
	if err != nil {
		t.Fatalf("compile stats BY atomic fixture: %v", err)
	}
	return compiled
}

func compileStatsByFixedMultivalueAtomicFixture(t *testing.T) clickhouse.CompiledQuery {
	t.Helper()
	parsed, err := spl.Parse(
		`index=main | stats values(tag) AS tags values(zone) AS zones | stats count BY tags zones | head 1`,
	)
	if err != nil {
		t.Fatalf("parse fixed-multivalue stats BY atomic fixture: %v", err)
	}
	searchStart := time.Date(2026, time.August, 12, 18, 0, 0, 0, time.UTC)
	visibility := uint64(17)
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
		t.Fatalf("plan fixed-multivalue stats BY atomic fixture: %v", err)
	}
	compiled, err := (clickhouse.Compiler{}).Compile(logical)
	if err != nil {
		t.Fatalf("compile fixed-multivalue stats BY atomic fixture: %v", err)
	}
	return compiled
}
