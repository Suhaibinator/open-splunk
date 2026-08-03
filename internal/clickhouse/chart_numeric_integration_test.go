package clickhouse

import (
	"context"
	"fmt"
	"math"
	"os"
	"os/exec"
	"reflect"
	"testing"
	"time"

	clickhousedriver "github.com/ClickHouse/clickhouse-go/v2"
	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/ingest"
	"github.com/Suhaibinator/open-splunk/internal/testsupport"
)

// TestNumericChartAgainstClickHouse pins the numeric pivot's physical
// transport and aggregation semantics against the repository's digest-pinned
// ClickHouse release. In particular, OTHER must merge aggregate states before
// avg finalization, and the value-presence bitmap must distinguish a real zero
// from a cell whose members were all numerically ineligible.
func TestNumericChartAgainstClickHouse(t *testing.T) {
	if os.Getenv("OPEN_SPLUNK_CLICKHOUSE_INTEGRATION") != "1" {
		t.Skip("set OPEN_SPLUNK_CLICKHOUSE_INTEGRATION=1 to run the Docker integration test")
	}
	if _, err := exec.LookPath("docker"); err != nil {
		t.Skipf("docker CLI is unavailable: %v", err)
	}
	if _, err := testsupport.ResolvePinnedClickHouseImage(
		os.Getenv("OPEN_SPLUNK_CLICKHOUSE_TEST_IMAGE"),
	); err != nil {
		t.Fatalf("resolve pinned ClickHouse integration image: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	connection, store := chartEdgeStartClickHouse(t, ctx)
	indexTime := time.Date(2026, time.July, 21, 4, 0, 0, 0, time.UTC)
	visibilityCutoff := chartNumericStoreFixture(t, ctx, store, indexTime)
	compile := func(aggregate string) CompiledQuery {
		t.Helper()
		return chartEdgeCompile(
			t,
			fmt.Sprintf(
				`index=chartedge source="chart-numeric" | chart %s(metric) OVER path BY series`,
				aggregate,
			),
			indexTime.Add(time.Minute),
			visibilityCutoff,
		)
	}

	wantNames := []string{
		"0:s01", "0:s02", "0:s03", "0:s04", "0:s05", "0:s06",
		"0:s07", "0:s08", "0:s09", "0:s10", "1:", "2:",
	}
	for _, test := range []struct {
		name          string
		aggregate     string
		valueKind     ChartValueKind
		otherWeighted float64
	}{
		{name: "sum", aggregate: "sum", valueKind: ChartValueKindSum, otherWeighted: 50},
		{name: "member weighted average", aggregate: "avg", valueKind: ChartValueKindAverage, otherWeighted: 50.0 / 21.0},
	} {
		t.Run(test.name, func(t *testing.T) {
			compiled := compile(test.aggregate)
			if compiled.Chart == nil || compiled.Chart.ValueKind != test.valueKind {
				t.Fatalf("numeric chart contract = %#v, want value kind %d", compiled.Chart, test.valueKind)
			}

			rows := chartNumericTransport(t, ctx, connection, compiled)
			if len(rows) != 3 {
				t.Fatalf("numeric chart rows = %#v, want three retained row values", rows)
			}
			for index, wantRow := range []string{"/empty", "/weighted", "/zero"} {
				if rows[index].ordinal != uint64(index) || rows[index].row != wantRow {
					t.Fatalf("row %d = %#v, want ordinal %d row %q", index, rows[index], index, wantRow)
				}
				if !reflect.DeepEqual(rows[index].names, wantNames) {
					t.Fatalf("row %q names = %#v, want %#v", wantRow, rows[index].names, wantNames)
				}
			}

			// Every member of /empty is missing, null, text, or boolean. The
			// row survives, but all twelve numeric cells are null.
			for index, present := range rows[0].present {
				if present != 0 || rows[0].values[index] != 0 {
					t.Fatalf("all-ineligible row cell %q = (%v, %d), want null transport", rows[0].names[index], rows[0].values[index], present)
				}
			}

			weighted := rows[1]
			for index := 1; index <= 10; index++ {
				chartNumericRequireCell(
					t,
					weighted,
					fmt.Sprintf("0:s%02d", index),
					true,
					float64(101-index),
				)
			}
			chartNumericRequireCell(t, weighted, "1:", true, 7)
			chartNumericRequireCell(t, weighted, "2:", true, test.otherWeighted)

			// s11 and s12 are outside the top ten. s11 contributes one value
			// of 30 while s12 contributes twenty values of 1, so avg(OTHER)
			// must be 50/21 rather than the average of the two finalized cells.
			for _, excluded := range []string{"0:s11", "0:s12"} {
				for _, name := range weighted.names {
					if name == excluded {
						t.Fatalf("overflow label %q escaped OTHER: %#v", excluded, weighted.names)
					}
				}
			}

			zero := rows[2]
			chartNumericRequireCell(t, zero, "0:s01", true, 0)
			chartNumericRequireCell(t, zero, "0:s02", false, 0)
			chartNumericRequireCell(t, zero, "1:", false, 0)
			chartNumericRequireCell(t, zero, "2:", false, 0)
		})
	}

	// Split validation is independent of row eligibility. The only event in
	// this scope deliberately omits path and carries a reserved series label;
	// both count and numeric chart must still return the private invalid
	// sentinel instead of silently producing an empty successful result.
	for _, aggregate := range []string{"count", "sum(metric)"} {
		aggregate := aggregate
		t.Run("row-independent invalid split "+aggregate, func(t *testing.T) {
			compiled := chartEdgeCompile(
				t,
				fmt.Sprintf(
					`index=chartedge source="chart-numeric-invalid" | chart %s OVER path BY series`,
					aggregate,
				),
				indexTime.Add(time.Minute),
				visibilityCutoff,
			)
			rows, err := connection.Query(ctx, compiled.SQL, compiled.Args...)
			if err != nil {
				t.Fatalf("execute invalid numeric chart: %v\nSQL: %s", err, compiled.SQL)
			}
			defer func() {
				if closeErr := rows.Close(); closeErr != nil && !t.Failed() {
					t.Errorf("close invalid numeric chart rows: %v", closeErr)
				}
			}()
			if !rows.Next() {
				t.Fatalf("invalid chart returned no sentinel: %v", rows.Err())
			}
			var (
				ordinal uint64
				row     string
				names   []string
				invalid uint8
			)
			if compiled.Chart.ValueKind == ChartValueKindCount {
				var counts []uint64
				if scanErr := rows.Scan(&ordinal, &row, &names, &counts, &invalid); scanErr != nil {
					t.Fatalf("scan count validation sentinel: %v", scanErr)
				}
				if len(counts) != 0 {
					t.Fatalf("count validation sentinel values = %#v, want empty", counts)
				}
			} else {
				var values []float64
				var present []uint8
				if scanErr := rows.Scan(&ordinal, &row, &names, &values, &present, &invalid); scanErr != nil {
					t.Fatalf("scan numeric validation sentinel: %v", scanErr)
				}
				if len(values) != 0 || len(present) != 0 {
					t.Fatalf("numeric validation sentinel = values %#v presence %#v, want empty", values, present)
				}
			}
			if ordinal != 0 || row != "" || len(names) != 0 || invalid == 0 {
				t.Fatalf("validation sentinel = ordinal %d row %q names %#v invalid %d", ordinal, row, names, invalid)
			}
			if rows.Next() {
				t.Fatal("invalid chart returned a public row after its sentinel")
			}
			if err := rows.Err(); err != nil {
				t.Fatalf("iterate invalid chart sentinel: %v", err)
			}
		})
	}
}

type chartNumericTransportRow struct {
	ordinal uint64
	row     string
	names   []string
	values  []float64
	present []uint8
}

func chartNumericTransport(
	t *testing.T,
	ctx context.Context,
	connection clickhousedriver.Conn,
	compiled CompiledQuery,
) []chartNumericTransportRow {
	t.Helper()
	rows, err := connection.Query(ctx, compiled.SQL, compiled.Args...)
	if err != nil {
		t.Fatalf("execute numeric chart: %v\nSQL: %s", err, compiled.SQL)
	}
	defer func() {
		if closeErr := rows.Close(); closeErr != nil && !t.Failed() {
			t.Errorf("close numeric chart rows: %v", closeErr)
		}
	}()

	var collected []chartNumericTransportRow
	for rows.Next() {
		var row chartNumericTransportRow
		var invalid uint8
		if scanErr := rows.Scan(
			&row.ordinal,
			&row.row,
			&row.names,
			&row.values,
			&row.present,
			&invalid,
		); scanErr != nil {
			t.Fatalf("scan numeric chart row: %v", scanErr)
		}
		if invalid != 0 {
			t.Fatalf("numeric chart row %q raised the atomic invalid flag", row.row)
		}
		if len(row.names) != len(row.values) || len(row.names) != len(row.present) {
			t.Fatalf(
				"numeric chart row %q has mismatched arrays: names=%d values=%d present=%d",
				row.row,
				len(row.names),
				len(row.values),
				len(row.present),
			)
		}
		collected = append(collected, row)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate numeric chart rows: %v\nSQL: %s", err, compiled.SQL)
	}
	return collected
}

func chartNumericRequireCell(
	t *testing.T,
	row chartNumericTransportRow,
	name string,
	wantPresent bool,
	wantValue float64,
) {
	t.Helper()
	for index, candidate := range row.names {
		if candidate != name {
			continue
		}
		gotPresent := row.present[index] != 0
		if gotPresent != wantPresent {
			t.Fatalf("row %q cell %q presence = %v, want %v", row.row, name, gotPresent, wantPresent)
		}
		if math.Abs(row.values[index]-wantValue) > 1e-12 {
			t.Fatalf("row %q cell %q = %.17g, want %.17g", row.row, name, row.values[index], wantValue)
		}
		return
	}
	t.Fatalf("row %q has no cell %q: %#v", row.row, name, row.names)
}

func chartNumericStoreFixture(
	t *testing.T,
	ctx context.Context,
	store *Store,
	indexTime time.Time,
) uint64 {
	t.Helper()
	info := "INFO"
	var events []*ingest.StoredEvent
	add := func(id, row string, series *string, metric *opensplunkv1.TypedValue) {
		fields := []*opensplunkv1.TypedObjectField{
			typedField("path", typedString(row)),
		}
		if series != nil {
			fields = append(fields, typedField("series", typedString(*series)))
		}
		if metric != nil {
			fields = append(fields, typedField("metric", metric))
		}
		events = append(events, chartEdgeEvent(
			id,
			"chart-numeric",
			chartEdgeBase,
			&info,
			fields...,
		))
	}
	label := func(value string) *string { return &value }

	// The first ten labels dominate both sum and avg score ordering.
	for index := 1; index <= 10; index++ {
		add(
			fmt.Sprintf("numeric-top-%02d", index),
			"/weighted",
			label(fmt.Sprintf("s%02d", index)),
			typedDouble(float64(101-index)),
		)
	}
	add("numeric-overflow-eleven", "/weighted", label("s11"), typedDouble(30))
	for index := range 20 {
		add(
			fmt.Sprintf("numeric-overflow-twelve-%02d", index),
			"/weighted",
			label("s12"),
			typedDouble(1),
		)
	}
	add("numeric-null-series", "/weighted", nil, typedDouble(7))

	// A single eligible zero mixed with poison remains a present zero. A
	// neighboring label with only poison is null, not zero.
	add("numeric-zero", "/zero", label("s01"), typedDouble(0))
	add("numeric-zero-text", "/zero", label("s01"), typedString("not-numeric"))
	add("numeric-zero-bool", "/zero", label("s01"), typedBool(true))
	add("numeric-zero-null", "/zero", label("s01"), typedNull())
	add("numeric-zero-missing", "/zero", label("s01"), nil)
	add("numeric-null-text", "/zero", label("s02"), typedString("still-not-numeric"))
	add("numeric-null-bool", "/zero", label("s02"), typedBool(false))
	add("numeric-null-explicit", "/zero", label("s02"), typedNull())
	add("numeric-null-missing", "/zero", label("s02"), nil)

	// No numeric measure is eligible anywhere in this row. It must still name
	// a result row with an entirely nullable grid.
	add("numeric-empty-text", "/empty", label("s03"), typedString("never-numeric"))
	add("numeric-empty-bool", "/empty", label("s03"), typedBool(true))
	add("numeric-empty-null", "/empty", label("s03"), typedNull())
	add("numeric-empty-missing", "/empty", label("s03"), nil)

	// This separate source has no row-axis value and a reserved column label.
	// It must never affect the valid corpus above, and exists solely to prove
	// that private validation survives a completely empty row domain.
	events = append(events, chartEdgeEvent(
		"numeric-invalid-empty-row",
		"chart-numeric-invalid",
		chartEdgeBase,
		&info,
		typedField("series", typedString("OTHER")),
		typedField("metric", typedDouble(1)),
	))

	chartEdgeStoreBatches(t, ctx, store, indexTime, events)
	cutoff, err := store.VisibilityCutoff(ctx)
	if err != nil {
		t.Fatalf("capture numeric chart visibility cutoff: %v", err)
	}
	return cutoff
}
