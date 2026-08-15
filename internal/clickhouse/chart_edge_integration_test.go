package clickhouse

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"reflect"
	"strings"
	"testing"
	"time"

	clickhousedriver "github.com/ClickHouse/clickhouse-go/v2"
	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/ingest"
	"github.com/Suhaibinator/open-splunk/internal/plan"
	"github.com/Suhaibinator/open-splunk/internal/spl"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	chartEdgeIndex  = "chartedge"
	chartEdgeTenant = "tenant"
	// chartEdgeRowOverflow is one more distinct row value than the compiled
	// row ceiling admits, so the fixture proves both sides of the boundary.
	chartEdgeRowOverflow = 10_001
	// chartEdgeWideRows and chartEdgeWideColumns describe a pivot whose raw
	// (row, column) product is far larger than its published shape, which is
	// what the row-keyed aggregation must stay bounded by.
	chartEdgeWideRows    = 40
	chartEdgeWideColumns = 40
	chartEdgeRowlessKeys = 8
	// The first aggregate retains exactly one state per raw (row, label) pair.
	// Pin both sides of that 1,600-state boundary so labels cannot escape the
	// group limit inside an unbounded aggregate value.
	chartEdgeWideGroupBudget = uint64(chartEdgeWideRows * chartEdgeWideColumns)
)

// chartEdgeBase is the canonical clock for the chart fixture. Every scenario
// that does not care about time shares one instant so the observed row axis is
// decided purely by the charted fields.
var chartEdgeBase = time.Date(2026, time.July, 21, 3, 0, 0, 0, time.UTC)

// TestChartPivotAgainstClickHouse proves the bounded runtime-wide pivot end to
// end against the pinned ClickHouse server. It pins the physical transport the
// executor consumes — the dense ordinal, the typed row column, the encoded
// name array, the per-row count array, and the atomic invalid flag — because
// every public guarantee in the chart contract is derived from those five
// columns. The decoded public schema is pinned by the query executor's own
// integration suite.
func TestChartPivotAgainstClickHouse(t *testing.T) {
	if os.Getenv("OPEN_SPLUNK_CLICKHOUSE_INTEGRATION") != "1" {
		t.Skip("set OPEN_SPLUNK_CLICKHOUSE_INTEGRATION=1 to run the Docker integration test")
	}
	if _, err := exec.LookPath("docker"); err != nil {
		t.Skipf("docker CLI is unavailable: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	connection, store := chartEdgeStartClickHouse(t, ctx)
	indexTime := time.Date(2026, time.July, 21, 4, 0, 0, 0, time.UTC)
	cutoff := chartEdgeStoreFixture(t, ctx, store, indexTime)
	compile := func(source string) CompiledQuery {
		t.Helper()
		return chartEdgeCompile(t, source, indexTime.Add(time.Minute), cutoff)
	}

	t.Run("R5 exact two-axis counts", func(t *testing.T) {
		// The contract's anchor example: /a has two INFO and one ERROR, /b has
		// one ERROR and one event with no level at all.
		want := []chartEdgeTransportRow{
			{ordinal: 0, row: "/a", names: "0:ERROR|0:INFO|1:", counts: "1|2|0"},
			{ordinal: 1, row: "/b", names: "0:ERROR|0:INFO|1:", counts: "1|0|1"},
		}
		for _, source := range []string{
			`index=chartedge source="chart-r5" | chart count OVER path BY level`,
			// S2: the two accepted spellings compile to byte-identical SQL and
			// therefore cannot diverge on real data.
			`index=chartedge source="chart-r5" | chart count BY path, level`,
		} {
			compiled := compile(source)
			if compiled.Chart == nil || compiled.Chart.RowField != "path" || compiled.Chart.RowKind != ChartRowKindString {
				t.Fatalf("chart contract for %q = %#v", source, compiled.Chart)
			}
			got := chartEdgeTransport(t, ctx, connection, compiled)
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("%q pivot = %#v, want %#v", source, got, want)
			}
		}

		// C7: an absent (row, column) pair is exactly zero, never null and
		// never a missing array element.
		if got := chartEdgeTransport(t, ctx, connection,
			compile(`index=chartedge source="chart-r5" | chart count OVER path BY level`)); len(got) != 2 ||
			got[0].counts != "1|2|0" {
			t.Fatalf("zero-filled cell = %#v", got)
		}
	})

	t.Run("I1 pivot over a transformed relation counts input rows", func(t *testing.T) {
		// count counts rows of the input relation, not the upstream count
		// column, and stats BY has already dropped the level-less event.
		compiled := compile(
			`index=chartedge source="chart-r5" | stats count BY path, level | chart count OVER path BY level`)
		if compiled.Chart == nil || compiled.Chart.RowKind != ChartRowKindString {
			t.Fatalf("transformed-relation chart contract = %#v", compiled.Chart)
		}
		want := []chartEdgeTransportRow{
			{ordinal: 0, row: "/a", names: "0:ERROR|0:INFO", counts: "1|1"},
			{ordinal: 1, row: "/b", names: "0:ERROR|0:INFO", counts: "1|0"},
		}
		if got := chartEdgeTransport(t, ctx, connection, compiled); !reflect.DeepEqual(got, want) {
			t.Fatalf("pivot over stats = %#v, want %#v", got, want)
		}
	})

	t.Run("C3 C4 C5 global top ten with null and other", func(t *testing.T) {
		// _audit dominates by count; the twelve single-count labels tie, so
		// UTF-8 ascending order of the raw label selects L01..L09 and leaves
		// L10..L12 for OTHER. Publication order uses the VALUE-normalized name,
		// which is why _audit sorts after L09 rather than before L01.
		compiled := compile(`index=chartedge source="chart-top" | chart count OVER path BY series`)
		want := []chartEdgeTransportRow{{
			ordinal: 0,
			row:     "/top",
			names:   "0:L01|0:L02|0:L03|0:L04|0:L05|0:L06|0:L07|0:L08|0:L09|0:_audit|1:|2:",
			counts:  "1|1|1|1|1|1|1|1|1|50|1|3",
		}}
		if got := chartEdgeTransport(t, ctx, connection, compiled); !reflect.DeepEqual(got, want) {
			t.Fatalf("top-ten pivot = %#v, want %#v", got, want)
		}
	})

	t.Run("C8 totals equal stats count by the row field", func(t *testing.T) {
		compiled := compile(`index=chartedge source="chart-c8" | chart count OVER path BY series`)
		got := chartEdgeTransport(t, ctx, connection, compiled)
		want := []chartEdgeTransportRow{
			{
				ordinal: 0, row: "/x",
				names:  "0:c01|0:c02|0:c03|0:c04|0:c05|0:c06|0:c07|0:c08|0:c09|0:c12|1:|2:",
				counts: "3|2|1|1|1|1|1|1|1|0|2|2",
			},
			{
				ordinal: 1, row: "/y",
				names:  "0:c01|0:c02|0:c03|0:c04|0:c05|0:c06|0:c07|0:c08|0:c09|0:c12|1:|2:",
				counts: "1|0|0|0|0|0|0|0|0|4|1|0",
			},
			{
				ordinal: 2, row: "/z",
				names:  "0:c01|0:c02|0:c03|0:c04|0:c05|0:c06|0:c07|0:c08|0:c09|0:c12|1:|2:",
				counts: "0|0|0|0|1|0|0|0|0|0|0|0",
			},
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("bounded pivot = %#v, want %#v", got, want)
		}

		// The primary differential: every published row's cells sum to exactly
		// the count stats count BY path reports, and the row set and order
		// equal stats count BY path | sort 0 +path.
		reference := compile(`index=chartedge source="chart-c8" | stats count BY path | sort 0 +path`)
		stats := chartEdgeRows(t, ctx, connection,
			"SELECT "+quoteIdentifier("path")+", toString(count) FROM ("+reference.SQL+")", reference.Args, 2)
		if len(stats) != len(got) {
			t.Fatalf("chart published %d rows but stats count BY path published %d", len(got), len(stats))
		}
		for index, statsRow := range stats {
			if statsRow[0] != got[index].row {
				t.Fatalf("row %d = %q, stats reports %q", index, got[index].row, statsRow[0])
			}
			if total := chartEdgeSum(t, got[index].counts); total != statsRow[1] {
				t.Fatalf("row %q cells sum to %s, stats count BY path reports %s", statsRow[0], total, statsRow[1])
			}
		}
	})

	t.Run("R2 R4 row values are data not column names", func(t *testing.T) {
		// An empty string is a present value and names a real row; there is no
		// length ceiling on a row value, only on a column label.
		compiled := compile(`index=chartedge source="chart-rowdata" | chart count OVER path BY level`)
		got := chartEdgeTransport(t, ctx, connection, compiled)
		want := []chartEdgeTransportRow{
			{ordinal: 0, row: "", names: "0:INFO", counts: "1"},
			{ordinal: 1, row: "_underscore", names: "0:INFO", counts: "1"},
			{ordinal: 2, row: strings.Repeat("w", 300), names: "0:INFO", counts: "1"},
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("row-value pivot = %#v, want %#v", got, want)
		}
	})

	t.Run("O1 numeric-aware row ordering", func(t *testing.T) {
		// Automatic numeric-aware ordering: finite numeric labels first in
		// numeric order, then the remaining labels in UTF-8 byte order.
		compiled := compile(`index=chartedge source="chart-order" | chart count OVER path BY level`)
		got := chartEdgeTransport(t, ctx, connection, compiled)
		order := make([]string, 0, len(got))
		for _, row := range got {
			order = append(order, row.row)
		}
		want := []string{"2", "10", "alpha", "beta"}
		if !reflect.DeepEqual(order, want) {
			t.Fatalf("row order = %v, want %v", order, want)
		}
	})

	t.Run("D2 bin discretizes the row axis", func(t *testing.T) {
		compiled := compile(`index=chartedge source="chart-bin" | bin sev span=10 | chart count OVER sev BY level`)
		if compiled.Chart == nil || compiled.Chart.RowKind != ChartRowKindString {
			t.Fatalf("binned Dynamic row contract = %#v", compiled.Chart)
		}
		want := []chartEdgeTransportRow{
			{ordinal: 0, row: "0", names: "0:ERROR|0:INFO", counts: "0|1"},
			{ordinal: 1, row: "10", names: "0:ERROR|0:INFO", counts: "2|0"},
			{ordinal: 2, row: "20", names: "0:ERROR|0:INFO", counts: "0|1"},
		}
		if got := chartEdgeTransport(t, ctx, connection, compiled); !reflect.DeepEqual(got, want) {
			t.Fatalf("binned pivot = %#v, want %#v", got, want)
		}

		// A numerically stored 12 and the numeric text "12" converge on one
		// row exactly as bin plus stats BY already converge them.
		mixed := compile(`index=chartedge source="chart-bin-mixed" | bin sev span=10 | chart count OVER sev BY level`)
		wantMixed := []chartEdgeTransportRow{{ordinal: 0, row: "10", names: "0:ERROR|0:INFO", counts: "1|1"}}
		if got := chartEdgeTransport(t, ctx, connection, mixed); !reflect.DeepEqual(got, wantMixed) {
			t.Fatalf("mixed-representation pivot = %#v, want %#v", got, wantMixed)
		}

		// D5: a fixed numeric row field is charted as the number itself and is
		// never re-binned into runtime text, so its row column keeps the
		// unsigned kind stats BY publishes for the same column.
		for _, test := range []struct {
			span string
			want []chartEdgeTransportRow
		}{
			{
				span: "4",
				want: []chartEdgeTransportRow{
					{ordinal: 0, row: "0", names: "0:ERROR|0:INFO", counts: "1|1"},
					{ordinal: 1, row: "4", names: "0:ERROR|0:INFO", counts: "1|1"},
				},
			},
			{
				// The corpus spelling: every observed severity falls in one
				// bucket, which is a single row rather than an empty axis.
				span: "10",
				want: []chartEdgeTransportRow{
					{ordinal: 0, row: "0", names: "0:ERROR|0:INFO", counts: "2|2"},
				},
			},
		} {
			source := `index=chartedge source="chart-bin" | bin severity span=` + test.span +
				` | chart count OVER severity BY level`
			fixed := compile(source)
			if fixed.Chart == nil || fixed.Chart.RowKind != ChartRowKindUnsigned {
				t.Fatalf("binned fixed-numeric row contract = %#v", fixed.Chart)
			}
			if got := chartEdgeTransport(t, ctx, connection, fixed); !reflect.DeepEqual(got, test.want) {
				t.Fatalf("%q = %#v, want %#v", source, got, test.want)
			}
		}
	})

	t.Run("D3 D4 only observed time buckets", func(t *testing.T) {
		// chart never fills gaps and never extends the axis to the search
		// range, so the empty 03:05 bucket simply does not exist.
		compiled := compile(
			`index=chartedge source="chart-time" | bin _time span=5m AS bucket_time | chart count OVER bucket_time BY level`)
		if compiled.Chart == nil || compiled.Chart.RowKind != ChartRowKindTime {
			t.Fatalf("binned time row contract = %#v", compiled.Chart)
		}
		want := []chartEdgeTransportRow{
			{ordinal: 0, row: "2026-07-21 03:00:00.000000000", names: "0:ERROR|0:INFO", counts: "0|2"},
			{ordinal: 1, row: "2026-07-21 03:10:00.000000000", names: "0:ERROR|0:INFO", counts: "1|0"},
		}
		if got := chartEdgeTransport(t, ctx, connection, compiled); !reflect.DeepEqual(got, want) {
			t.Fatalf("observed time buckets = %#v, want %#v", got, want)
		}

		// D4: chart never requires canonical _time, so an in-place time bin is
		// legal and produces the same observed buckets.
		inPlace := compile(`index=chartedge source="chart-time" | bin _time span=5m | chart count OVER _time BY level`)
		if inPlace.Chart == nil || inPlace.Chart.RowField != "_time" || inPlace.Chart.RowKind != ChartRowKindTime {
			t.Fatalf("in-place time bin contract = %#v", inPlace.Chart)
		}
		gotInPlace := chartEdgeTransport(t, ctx, connection, inPlace)
		for index := range want {
			if index >= len(gotInPlace) || gotInPlace[index].row != want[index].row ||
				gotInPlace[index].counts != want[index].counts {
				t.Fatalf("in-place time bin = %#v, want %#v", gotInPlace, want)
			}
		}
		if len(gotInPlace) != len(want) {
			t.Fatalf("in-place time bin = %#v, want %#v", gotInPlace, want)
		}
	})

	t.Run("D6 numeric values are legal rows and fatal columns", func(t *testing.T) {
		legal := compile(`index=chartedge source="chart-d6" | chart count OVER sev BY level`)
		want := []chartEdgeTransportRow{
			{ordinal: 0, row: "1", names: "0:ERROR|0:INFO", counts: "0|1"},
			{ordinal: 1, row: "2", names: "0:ERROR|0:INFO", counts: "1|0"},
		}
		if got := chartEdgeTransport(t, ctx, connection, legal); !reflect.DeepEqual(got, want) {
			t.Fatalf("numeric row axis = %#v, want %#v", got, want)
		}
		fatal := compile(`index=chartedge source="chart-d6" | chart count OVER level BY sev`)
		if got := chartEdgeInvalid(t, ctx, connection, fatal); got != 1 {
			t.Fatalf("numeric column axis invalid flag = %d, want 1", got)
		}
	})

	t.Run("atomic invalid flag for every unsupported axis value", func(t *testing.T) {
		for _, test := range []struct {
			name   string
			source string
		}{
			{"numeric column value", `index=chartedge source="chart-bad-number" | chart count OVER path BY probe`},
			{"boolean column value", `index=chartedge source="chart-bad-bool" | chart count OVER path BY probe`},
			{"timestamp column value", `index=chartedge source="chart-bad-time" | chart count OVER path BY probe`},
			{"list column value", `index=chartedge source="chart-bad-list" | chart count OVER path BY probe`},
			{"list row value", `index=chartedge source="chart-bad-row-list" | chart count OVER probe BY level`},
			{"object row value", `index=chartedge source="chart-bad-row-object" | chart count OVER probe BY level`},
			{"empty label", `index=chartedge source="chart-bad-empty" | chart count OVER path BY probe`},
			{"oversized label", `index=chartedge source="chart-bad-long" | chart count OVER path BY probe`},
			{"reserved NULL label", `index=chartedge source="chart-bad-null" | chart count OVER path BY probe`},
			{"reserved OTHER label", `index=chartedge source="chart-bad-other" | chart count OVER path BY probe`},
			{"normalization collision", `index=chartedge source="chart-bad-collision" | chart count OVER path BY probe`},
			{"label equal to the row column name", `index=chartedge source="chart-bad-rowname" | chart count OVER path BY probe`},
		} {
			t.Run(test.name, func(t *testing.T) {
				if got := chartEdgeInvalid(t, ctx, connection, compile(test.source)); got != 1 {
					t.Fatalf("%q invalid flag = %d, want 1", test.source, got)
				}
			})
		}

		// The identical shapes are supported when they name a row instead of a
		// column, or a column instead of a row, so the flag is not simply on.
		for _, source := range []string{
			`index=chartedge source="chart-bad-number" | chart count OVER probe BY level`,
			`index=chartedge source="chart-bad-rowname" | chart count OVER probe BY level`,
			`index=chartedge source="chart-bad-row-list" | chart count OVER path BY level`,
		} {
			if got := chartEdgeInvalid(t, ctx, connection, compile(source)); got != 0 {
				t.Fatalf("%q invalid flag = %d, want 0", source, got)
			}
		}
	})

	t.Run("column values are rejected on their own presence", func(t *testing.T) {
		// The offending column value sits on an event that carries no row
		// field at all. The rejection is atomic regardless, and regardless of
		// which field names the row axis.
		for _, fixture := range []string{
			"chart-rowless-list", "chart-rowless-number", "chart-rowless-object", "chart-rowless-other",
		} {
			t.Run(fixture, func(t *testing.T) {
				for _, rowField := range []string{"path", "host"} {
					source := `index=chartedge source="` + fixture + `" | chart count OVER ` + rowField + ` BY probe`
					if got := chartEdgeInvalid(t, ctx, connection, compile(source)); got != 1 {
						t.Fatalf("%q invalid flag = %d, want 1", source, got)
					}
				}
			})
		}

		// The negative control: an ordinary string column value on a row-less
		// event is not a rejection, and contributes no published column
		// because no eligible row ever carried it.
		supported := compile(`index=chartedge source="chart-rowless-ok" | chart count OVER path BY probe`)
		want := []chartEdgeTransportRow{{ordinal: 0, row: "/p", names: "0:INFO", counts: "1"}}
		if got := chartEdgeTransport(t, ctx, connection, supported); !reflect.DeepEqual(got, want) {
			t.Fatalf("row-less ordinary column value = %#v, want %#v", got, want)
		}
	})

	t.Run("a wide column axis stays inside the row-keyed group budget", func(t *testing.T) {
		// The raw (row, label) aggregate is the exact pre-ranking work bound.
		// A budget equal to all 1,600 distinct pairs succeeds before the graph
		// collapses them to the top-ten-plus-OTHER published shape.
		compiled := compile(`index=chartedge source="chart-wide" | chart count OVER path BY series`)
		budgeted := clickhousedriver.Context(ctx, clickhousedriver.WithSettings(map[string]any{
			"max_rows_to_group_by":   chartEdgeWideGroupBudget,
			"group_by_overflow_mode": "throw",
		}))
		names := make([]string, 0, 11)
		counts := make([]string, 0, 11)
		for index := range 10 {
			names = append(names, fmt.Sprintf("0:s%02d", index))
			counts = append(counts, "1")
		}
		names = append(names, "2:")
		counts = append(counts, fmt.Sprintf("%d", chartEdgeWideColumns-10))
		want := make([]chartEdgeTransportRow, 0, chartEdgeWideRows)
		for index := range chartEdgeWideRows {
			want = append(want, chartEdgeTransportRow{
				ordinal: uint64(index),
				row:     fmt.Sprintf("p%02d", index),
				names:   strings.Join(names, "|"),
				counts:  strings.Join(counts, "|"),
			})
		}
		if got := chartEdgeTransport(t, budgeted, connection, compiled); !reflect.DeepEqual(got, want) {
			t.Fatalf("wide column axis pivot = %#v, want %#v", got, want)
		}

		// One fewer raw-pair state fails atomically, proving the positive
		// assertion did not hide label keys inside an aggregate array.
		starved := clickhousedriver.Context(ctx, clickhousedriver.WithSettings(map[string]any{
			"max_rows_to_group_by":   chartEdgeWideGroupBudget - 1,
			"group_by_overflow_mode": "throw",
		}))
		var rows uint64
		err := connection.QueryRow(starved, "SELECT count() FROM ("+compiled.SQL+")", compiled.Args...).Scan(&rows)
		if err == nil || !strings.Contains(err.Error(), "GROUP BY") {
			t.Fatalf("starved group budget error = %v, want a GROUP BY overflow", err)
		}
	})

	t.Run("one row still pays for every raw label group", func(t *testing.T) {
		compiled := compile(`index=chartedge source="chart-one-row-wide" | chart count OVER path BY series`)
		admitted := clickhousedriver.Context(ctx, clickhousedriver.WithSettings(map[string]any{
			"max_rows_to_group_by":   uint64(chartEdgeWideColumns),
			"group_by_overflow_mode": "throw",
		}))
		names := make([]string, 0, 11)
		counts := make([]string, 0, 11)
		for index := range 10 {
			names = append(names, fmt.Sprintf("0:s%02d", index))
			counts = append(counts, "1")
		}
		names = append(names, "2:")
		counts = append(counts, fmt.Sprintf("%d", chartEdgeWideColumns-10))
		want := []chartEdgeTransportRow{{
			ordinal: 0,
			row:     "/only",
			names:   strings.Join(names, "|"),
			counts:  strings.Join(counts, "|"),
		}}
		if got := chartEdgeTransport(t, admitted, connection, compiled); !reflect.DeepEqual(got, want) {
			t.Fatalf("one-row raw-label boundary = %#v, want %#v", got, want)
		}

		starved := clickhousedriver.Context(ctx, clickhousedriver.WithSettings(map[string]any{
			"max_rows_to_group_by":   uint64(chartEdgeWideColumns - 1),
			"group_by_overflow_mode": "throw",
		}))
		var rows uint64
		err := connection.QueryRow(starved, "SELECT count() FROM ("+compiled.SQL+")", compiled.Args...).Scan(&rows)
		if err == nil || !strings.Contains(err.Error(), "GROUP BY") {
			t.Fatalf("one-row starved group budget error = %v, want a GROUP BY overflow", err)
		}
	})

	t.Run("rowless labels remain inside the raw group bound", func(t *testing.T) {
		compiled := compile(`index=chartedge source="chart-rowless-budget" | chart count OVER path BY probe`)
		rawGroups := uint64(chartEdgeRowlessKeys + 1)
		admitted := clickhousedriver.Context(ctx, clickhousedriver.WithSettings(map[string]any{
			"max_rows_to_group_by":   rawGroups,
			"group_by_overflow_mode": "throw",
		}))
		want := []chartEdgeTransportRow{{ordinal: 0, row: "/p", names: "0:public", counts: "1"}}
		if got := chartEdgeTransport(t, admitted, connection, compiled); !reflect.DeepEqual(got, want) {
			t.Fatalf("rowless raw-label boundary = %#v, want %#v", got, want)
		}

		starved := clickhousedriver.Context(ctx, clickhousedriver.WithSettings(map[string]any{
			"max_rows_to_group_by":   rawGroups - 1,
			"group_by_overflow_mode": "throw",
		}))
		var rows uint64
		err := connection.QueryRow(starved, "SELECT count() FROM ("+compiled.SQL+")", compiled.Args...).Scan(&rows)
		if err == nil || !strings.Contains(err.Error(), "GROUP BY") {
			t.Fatalf("rowless starved group budget error = %v, want a GROUP BY overflow", err)
		}
	})

	t.Run("R1 the raw row column carries binary data", func(t *testing.T) {
		// _raw is published as the Mixed group column stats BY produces, so a
		// non-UTF-8 raw value is ordinary data rather than a failed search.
		compiled := compile(`index=chartedge source="chart-raw" | chart count OVER _raw BY level`)
		if compiled.Chart == nil || compiled.Chart.RowField != "_raw" ||
			compiled.Chart.RowKind != ChartRowKindMixed || compiled.Chart.RowDatabaseType != "String" {
			t.Fatalf("raw row contract = %#v", compiled.Chart)
		}
		query := "SELECT hex(" + quoteIdentifier(ChartRowColumn) + "), " +
			"toString(isValidUTF8(" + quoteIdentifier(ChartRowColumn) + ")), " +
			"arrayStringConcat(" + quoteIdentifier(ChartNamesColumn) + ", '|'), " +
			"arrayStringConcat(arrayMap(value -> toString(value), " + quoteIdentifier(ChartCountsColumn) + "), '|'), " +
			"toString(" + quoteIdentifier(ChartInvalidColumn) + ") FROM (" + compiled.SQL + ")"
		want := [][]string{
			{"61FFFE62", "0", "0:ERROR|0:INFO", "1|0", "0"},
			{"706C61696E20617363696920726177", "1", "0:ERROR|0:INFO", "0|1", "0"},
		}
		if got := chartEdgeRows(t, ctx, connection, query, compiled.Args, 5); !reflect.DeepEqual(got, want) {
			t.Fatalf("binary raw pivot = %#v, want %#v", got, want)
		}
	})

	t.Run("I2 I3 I4 projected and empty relations", func(t *testing.T) {
		// I2: the column field is missing for every row, so exactly one NULL
		// column carries each row's whole total.
		projectedColumn := compile(`index=chartedge source="chart-r5" | fields path | chart count OVER path BY level`)
		want := []chartEdgeTransportRow{
			{ordinal: 0, row: "/a", names: "1:", counts: "3"},
			{ordinal: 1, row: "/b", names: "1:", counts: "2"},
		}
		if got := chartEdgeTransport(t, ctx, connection, projectedColumn); !reflect.DeepEqual(got, want) {
			t.Fatalf("projected column field = %#v, want %#v", got, want)
		}

		// I3: the row field is gone, so no row value is present and the pivot
		// emits no rows at all.
		projectedRow := compile(`index=chartedge source="chart-r5" | fields host | chart count OVER path BY level`)
		if projectedRow.Chart == nil || !reflect.DeepEqual(projectedRow.OutputFields, []string{"path"}) {
			t.Fatalf("projected row field contract = %#v / %v", projectedRow.Chart, projectedRow.OutputFields)
		}
		if got := chartEdgeTransport(t, ctx, connection, projectedRow); len(got) != 0 {
			t.Fatalf("projected row field = %#v, want no rows", got)
		}

		// I4: empty input behaves identically, and a grouped chart never emits
		// the global row stats count would emit.
		empty := compile(`index=chartedge source="chart-nothing" | chart count OVER path BY level`)
		if got := chartEdgeTransport(t, ctx, connection, empty); len(got) != 0 {
			t.Fatalf("empty input = %#v, want no rows", got)
		}
	})

	t.Run("L1 row ceiling fails atomically without truncating", func(t *testing.T) {
		// Exactly the ceiling is admitted.
		admitted := compile(`index=chartedge source="chart-overflow" host="in" | chart count OVER path BY level`)
		var rows uint64
		if err := connection.QueryRow(ctx, "SELECT count() FROM ("+admitted.SQL+")", admitted.Args...).Scan(&rows); err != nil {
			t.Fatalf("execute the admitted row ceiling: %v", err)
		}
		if rows != chartEdgeRowOverflow-1 {
			t.Fatalf("admitted row count = %d, want %d", rows, chartEdgeRowOverflow-1)
		}

		// One more distinct row value fails before any row is produced.
		overflow := compile(`index=chartedge source="chart-overflow" | chart count OVER path BY level`)
		err := connection.QueryRow(ctx, "SELECT count() FROM ("+overflow.SQL+")", overflow.Args...).Scan(&rows)
		if err == nil || !strings.Contains(err.Error(), ChartRowLimitMarker) {
			t.Fatalf("row overflow error = %v, want the %q marker", err, ChartRowLimitMarker)
		}
	})
}

// chartEdgeTransportRow is the physical chart contract reduced to comparable
// text: the dense ordinal, the row column rendered through toString, the
// encoded series names, and the per-row counts.
type chartEdgeTransportRow struct {
	ordinal uint64
	row     string
	names   string
	counts  string
}

func chartEdgeTransport(
	t *testing.T,
	ctx context.Context,
	connection clickhousedriver.Conn,
	compiled CompiledQuery,
) []chartEdgeTransportRow {
	t.Helper()
	if compiled.Chart == nil {
		t.Fatalf("compiled query is not a chart: %#v", compiled)
	}
	query := "SELECT toString(" + quoteIdentifier(ChartOrdinalColumn) + "), " +
		"toString(" + quoteIdentifier(ChartRowColumn) + "), " +
		"arrayStringConcat(" + quoteIdentifier(ChartNamesColumn) + ", '|'), " +
		"arrayStringConcat(arrayMap(value -> toString(value), " + quoteIdentifier(ChartCountsColumn) + "), '|'), " +
		"toString(" + quoteIdentifier(ChartInvalidColumn) + ") FROM (" + compiled.SQL + ")"
	scanned := chartEdgeRows(t, ctx, connection, query, compiled.Args, 5)
	collected := make([]chartEdgeTransportRow, 0, len(scanned))
	for index, row := range scanned {
		if row[4] != "0" {
			t.Fatalf("chart row %d raised the atomic invalid flag: %#v", index, row)
		}
		var ordinal uint64
		if _, err := fmt.Sscanf(row[0], "%d", &ordinal); err != nil {
			t.Fatalf("chart ordinal %q is not a number: %v", row[0], err)
		}
		collected = append(collected, chartEdgeTransportRow{
			ordinal: ordinal, row: row[1], names: row[2], counts: row[3],
		})
	}
	return collected
}

// chartEdgeInvalid folds the atomic validity flag over the whole pivot. The
// flag is what makes an unsupported value fail the command before publication.
func chartEdgeInvalid(
	t *testing.T,
	ctx context.Context,
	connection clickhousedriver.Conn,
	compiled CompiledQuery,
) uint8 {
	t.Helper()
	if compiled.Chart == nil {
		t.Fatalf("compiled query is not a chart: %#v", compiled)
	}
	query := "SELECT max(" + quoteIdentifier(ChartInvalidColumn) + ") FROM (" + compiled.SQL + ")"
	var invalid uint8
	if err := connection.QueryRow(ctx, query, compiled.Args...).Scan(&invalid); err != nil {
		t.Fatalf("execute chart validity fold: %v\nSQL: %s", err, query)
	}
	return invalid
}

// chartEdgeSum totals one row's pipe-joined cells so the C8 differential can be
// compared against the stats count BY text without extra scan plumbing.
func chartEdgeSum(t *testing.T, counts string) string {
	t.Helper()
	total := 0
	for _, cell := range strings.Split(counts, "|") {
		var value int
		if _, err := fmt.Sscanf(cell, "%d", &value); err != nil {
			t.Fatalf("chart cell %q is not a number: %v", cell, err)
		}
		total += value
	}
	return fmt.Sprintf("%d", total)
}

func chartEdgeRows(
	t *testing.T,
	ctx context.Context,
	connection clickhousedriver.Conn,
	query string,
	args []any,
	width int,
) [][]string {
	t.Helper()
	rows, err := connection.Query(ctx, query, args...)
	if err != nil {
		t.Fatalf("execute chart query: %v\nSQL: %s", err, query)
	}
	defer func() {
		if closeErr := rows.Close(); closeErr != nil && !t.Failed() {
			t.Errorf("close chart rows: %v", closeErr)
		}
	}()
	var collected [][]string
	for rows.Next() {
		values := make([]string, width)
		targets := make([]any, width)
		for index := range values {
			targets[index] = &values[index]
		}
		if err := rows.Scan(targets...); err != nil {
			t.Fatalf("scan chart row: %v\nSQL: %s", err, query)
		}
		collected = append(collected, values)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate chart rows: %v\nSQL: %s", err, query)
	}
	return collected
}

func chartEdgeCompile(t *testing.T, source string, cutoff time.Time, visibilityCutoff uint64) CompiledQuery {
	t.Helper()
	parsed, err := spl.Parse(source)
	if err != nil {
		t.Fatalf("parse chart SPL %q: %v", source, err)
	}
	logical, err := plan.Build(parsed, plan.Scope{
		TenantID: chartEdgeTenant, AuthorizedIndexes: []string{chartEdgeIndex},
		Earliest:         time.Date(2026, time.July, 21, 0, 0, 0, 0, time.UTC),
		Latest:           time.Date(2026, time.July, 22, 0, 0, 0, 0, time.UTC),
		SearchStart:      cutoff.Add(-time.Second),
		SearchTimezone:   "UTC",
		IndexTimeCutoff:  cutoff,
		VisibilityCutoff: new(visibilityCutoff),
	})
	if err != nil {
		t.Fatalf("build chart SPL %q: %v", source, err)
	}
	compiled, err := (Compiler{}).Compile(logical)
	if err != nil {
		t.Fatalf("compile chart SPL %q: %v", source, err)
	}
	if got, want := strings.Count(compiled.SQL, "?"), len(compiled.Args); got != want {
		t.Fatalf("placeholder count = %d, args = %d for %q", got, want, source)
	}
	return compiled
}

// chartEdgeStoreFixture writes every chart scenario through the store writer.
// Scenarios are separated by source so each subtest scopes to exactly the rows
// it reasons about.
func chartEdgeStoreFixture(t *testing.T, ctx context.Context, store *Store, indexTime time.Time) uint64 {
	t.Helper()
	info, err0 := "INFO", "ERROR"
	var events []*ingest.StoredEvent
	add := func(event *ingest.StoredEvent) { events = append(events, event) }

	// R5: the contract's anchor example.
	add(chartEdgeEvent("r5-a1", "chart-r5", chartEdgeBase, &info, typedField("path", typedString("/a"))))
	add(chartEdgeEvent("r5-a2", "chart-r5", chartEdgeBase, &info, typedField("path", typedString("/a"))))
	add(chartEdgeEvent("r5-a3", "chart-r5", chartEdgeBase, &err0, typedField("path", typedString("/a"))))
	add(chartEdgeEvent("r5-b1", "chart-r5", chartEdgeBase, &err0, typedField("path", typedString("/b"))))
	add(chartEdgeEvent("r5-b2", "chart-r5", chartEdgeBase, nil, typedField("path", typedString("/b"))))

	// C3/C4/C5: one dominant leading-underscore label, twelve tied labels, and
	// one missing value so NULL and OTHER are both required.
	for index := range 50 {
		add(chartEdgeEvent(fmt.Sprintf("top-audit-%d", index), "chart-top", chartEdgeBase, &info,
			typedField("path", typedString("/top")), typedField("series", typedString("_audit"))))
	}
	for index := 1; index <= 12; index++ {
		add(chartEdgeEvent(fmt.Sprintf("top-l-%d", index), "chart-top", chartEdgeBase, &info,
			typedField("path", typedString("/top")), typedField("series", typedString(fmt.Sprintf("L%02d", index)))))
	}
	add(chartEdgeEvent("top-missing", "chart-top", chartEdgeBase, &info, typedField("path", typedString("/top"))))

	// C8: three rows, twelve column values, an excluded tail, and missing
	// column values so the per-row totals must survive the column bound.
	chartEdgeColumnFixture := []struct {
		path   string
		series string
		repeat int
	}{
		{"/x", "c01", 3}, {"/x", "c02", 2}, {"/x", "c03", 1}, {"/x", "c04", 1},
		{"/x", "c05", 1}, {"/x", "c06", 1}, {"/x", "c07", 1}, {"/x", "c08", 1},
		{"/x", "c09", 1}, {"/x", "c10", 1}, {"/x", "c11", 1}, {"/x", "", 2},
		{"/y", "c01", 1}, {"/y", "c12", 4}, {"/y", "", 1},
		{"/z", "c05", 1},
	}
	counter := 0
	for _, entry := range chartEdgeColumnFixture {
		for range entry.repeat {
			counter++
			fields := []*opensplunkv1.TypedObjectField{typedField("path", typedString(entry.path))}
			if entry.series != "" {
				fields = append(fields, typedField("series", typedString(entry.series)))
			}
			add(chartEdgeEvent(fmt.Sprintf("c8-%d", counter), "chart-c8", chartEdgeBase, &info, fields...))
		}
	}

	// R2/R4: row values are data, so the empty string is a real row and a row
	// far longer than the 256-byte column-label ceiling is fine.
	add(chartEdgeEvent("rowdata-empty", "chart-rowdata", chartEdgeBase, &info, typedField("path", typedString(""))))
	add(chartEdgeEvent("rowdata-underscore", "chart-rowdata", chartEdgeBase, &info,
		typedField("path", typedString("_underscore"))))
	add(chartEdgeEvent("rowdata-long", "chart-rowdata", chartEdgeBase, &info,
		typedField("path", typedString(strings.Repeat("w", 300)))))

	// O1: numeric-aware ordering across mixed row labels.
	add(chartEdgeEvent("order-10", "chart-order", chartEdgeBase, &info, typedField("path", typedSint(10))))
	add(chartEdgeEvent("order-2", "chart-order", chartEdgeBase, &info, typedField("path", typedString("2"))))
	add(chartEdgeEvent("order-alpha", "chart-order", chartEdgeBase, &info, typedField("path", typedString("alpha"))))
	add(chartEdgeEvent("order-beta", "chart-order", chartEdgeBase, &info, typedField("path", typedString("beta"))))

	// D2/D5: the contract's discretization example. The runtime-typed sev
	// field carries it; the fixed severity column varies alongside so the same
	// scenario also discretizes a promoted numeric column.
	add(chartEdgeSeverityEvent("bin-5", chartEdgeBase, &info,
		opensplunkv1.LogSeverity_LOG_SEVERITY_TRACE, typedField("sev", typedUint(5))))
	add(chartEdgeSeverityEvent("bin-12", chartEdgeBase, &err0,
		opensplunkv1.LogSeverity_LOG_SEVERITY_ERROR, typedField("sev", typedUint(12))))
	add(chartEdgeSeverityEvent("bin-15", chartEdgeBase, &err0,
		opensplunkv1.LogSeverity_LOG_SEVERITY_INFO, typedField("sev", typedUint(15))))
	add(chartEdgeSeverityEvent("bin-27", chartEdgeBase, &info,
		opensplunkv1.LogSeverity_LOG_SEVERITY_FATAL, typedField("sev", typedUint(27))))
	add(chartEdgeEvent("bin-mixed-number", "chart-bin-mixed", chartEdgeBase, &info, typedField("sev", typedSint(12))))
	add(chartEdgeEvent("bin-mixed-text", "chart-bin-mixed", chartEdgeBase, &err0, typedField("sev", typedString("12"))))

	// D3/D4: two observed five-minute buckets with an empty bucket between
	// them, which chart must not fill.
	add(chartEdgeEvent("time-a", "chart-time", chartEdgeBase, &info))
	add(chartEdgeEvent("time-b", "chart-time", chartEdgeBase.Add(2*time.Minute), &info))
	add(chartEdgeEvent("time-c", "chart-time", chartEdgeBase.Add(12*time.Minute), &err0))

	// D6: the same numeric field on both axes.
	add(chartEdgeEvent("d6-one", "chart-d6", chartEdgeBase, &info, typedField("sev", typedUint(1))))
	add(chartEdgeEvent("d6-two", "chart-d6", chartEdgeBase, &err0, typedField("sev", typedUint(2))))

	// Atomic unsupported-value scenarios. Each isolates exactly one boundary.
	add(chartEdgeEvent("bad-number", "chart-bad-number", chartEdgeBase, &info,
		typedField("path", typedString("/p")), typedField("probe", typedSint(7))))
	add(chartEdgeEvent("bad-bool", "chart-bad-bool", chartEdgeBase, &info,
		typedField("path", typedString("/p")), typedField("probe", typedBool(true))))
	add(chartEdgeEvent("bad-time", "chart-bad-time", chartEdgeBase, &info,
		typedField("path", typedString("/p")), typedField("probe", typedTimestamp(chartEdgeBase))))
	add(chartEdgeEvent("bad-list", "chart-bad-list", chartEdgeBase, &info,
		typedField("path", typedString("/p")), typedField("probe", typedList(typedString("a"), typedString("b")))))
	add(chartEdgeEvent("bad-row-list", "chart-bad-row-list", chartEdgeBase, &info,
		typedField("path", typedString("/p")), typedField("probe", typedList(typedString("a"), typedString("b")))))
	add(chartEdgeEvent("bad-row-object", "chart-bad-row-object", chartEdgeBase, &info,
		typedField("path", typedString("/p")),
		typedField("probe", typedObject(typedField("child", typedString("leaf"))))))
	add(chartEdgeEvent("bad-empty", "chart-bad-empty", chartEdgeBase, &info,
		typedField("path", typedString("/p")), typedField("probe", typedString(""))))
	add(chartEdgeEvent("bad-long", "chart-bad-long", chartEdgeBase, &info,
		typedField("path", typedString("/p")), typedField("probe", typedString(strings.Repeat("y", 257)))))
	add(chartEdgeEvent("bad-null", "chart-bad-null", chartEdgeBase, &info,
		typedField("path", typedString("/p")), typedField("probe", typedString("NULL"))))
	add(chartEdgeEvent("bad-other", "chart-bad-other", chartEdgeBase, &info,
		typedField("path", typedString("/p")), typedField("probe", typedString("OTHER"))))
	add(chartEdgeEvent("bad-collision-prefixed", "chart-bad-collision", chartEdgeBase, &info,
		typedField("path", typedString("/p")), typedField("probe", typedString("_x"))))
	add(chartEdgeEvent("bad-collision-literal", "chart-bad-collision", chartEdgeBase, &info,
		typedField("path", typedString("/p")), typedField("probe", typedString("VALUE_x"))))
	add(chartEdgeEvent("bad-rowname", "chart-bad-rowname", chartEdgeBase, &info,
		typedField("path", typedString("/p")), typedField("probe", typedString("path"))))

	// The column value is classified on its own presence: an unsupported value
	// on an event that carries no row field at all must still fail the whole
	// command, and must not depend on which field names the row axis.
	for _, scenario := range []struct {
		source string
		probe  *opensplunkv1.TypedValue
	}{
		{"chart-rowless-list", typedList(typedString("a"), typedString("b"))},
		{"chart-rowless-number", typedSint(7)},
		{"chart-rowless-object", typedObject(typedField("child", typedString("leaf")))},
		{"chart-rowless-other", typedString("OTHER")},
	} {
		add(chartEdgeEvent(scenario.source+"-row", scenario.source, chartEdgeBase, &info,
			typedField("path", typedString("/p")), typedField("probe", typedString("INFO"))))
		add(chartEdgeEvent(scenario.source+"-rowless", scenario.source, chartEdgeBase, &info,
			typedField("probe", scenario.probe)))
	}
	// The negative control: an ordinary string column value on a row-less
	// event stays valid and contributes no label to the published domain.
	add(chartEdgeEvent("rowless-ok-row", "chart-rowless-ok", chartEdgeBase, &info,
		typedField("path", typedString("/p")), typedField("probe", typedString("INFO"))))
	add(chartEdgeEvent("rowless-ok-rowless", "chart-rowless-ok", chartEdgeBase, &info,
		typedField("probe", typedString("WARN"))))

	// A wide column axis whose published pivot is small: forty row values
	// crossed with forty raw column values is 1,600 raw pairs but publishes
	// forty rows and eleven columns.
	for rowIndex := range chartEdgeWideRows {
		for columnIndex := range chartEdgeWideColumns {
			add(chartEdgeEvent(fmt.Sprintf("wide-%d-%d", rowIndex, columnIndex), "chart-wide", chartEdgeBase, &info,
				typedField("path", typedString(fmt.Sprintf("p%02d", rowIndex))),
				typedField("series", typedString(fmt.Sprintf("s%02d", columnIndex)))))
		}
	}
	for columnIndex := range chartEdgeWideColumns {
		add(chartEdgeEvent(fmt.Sprintf("one-row-wide-%d", columnIndex), "chart-one-row-wide", chartEdgeBase, &info,
			typedField("path", typedString("/only")),
			typedField("series", typedString(fmt.Sprintf("s%02d", columnIndex)))))
	}
	add(chartEdgeEvent("rowless-budget-public", "chart-rowless-budget", chartEdgeBase, &info,
		typedField("path", typedString("/p")), typedField("probe", typedString("public"))))
	for index := range chartEdgeRowlessKeys {
		add(chartEdgeEvent(fmt.Sprintf("rowless-budget-%d", index), "chart-rowless-budget", chartEdgeBase, &info,
			typedField("probe", typedString(fmt.Sprintf("rowless-%02d", index)))))
	}

	// R1: _raw is the one field whose stats BY group column is Mixed rather
	// than String, because ingest accepts non-UTF-8 raw bytes.
	add(chartEdgeRawEvent("raw-utf8", "chart-raw", chartEdgeBase, &info,
		[]byte("plain ascii raw"), opensplunkv1.RawEncoding_RAW_ENCODING_UTF8))
	add(chartEdgeRawEvent("raw-binary", "chart-raw", chartEdgeBase, &err0,
		[]byte{0x61, 0xff, 0xfe, 0x62}, opensplunkv1.RawEncoding_RAW_ENCODING_BINARY))

	// L1: one more distinct row value than the ceiling admits. The host column
	// carves the admitted boundary out of the same scenario.
	for index := range chartEdgeRowOverflow {
		host := "in"
		if index == chartEdgeRowOverflow-1 {
			host = "extra"
		}
		add(chartEdgeEventHost(fmt.Sprintf("overflow-%d", index), "chart-overflow", host, chartEdgeBase, &info,
			opensplunkv1.LogSeverity_LOG_SEVERITY_INFO, typedField("path", typedString(fmt.Sprintf("r%05d", index)))))
	}

	chartEdgeStoreBatches(t, ctx, store, indexTime, events)
	cutoff, err := store.VisibilityCutoff(ctx)
	if err != nil {
		t.Fatalf("capture chart visibility cutoff: %v", err)
	}
	return cutoff
}

// chartEdgeStoreBatches writes the fixture through the store writer within the
// durable batch ceiling so the pivot reads exactly what ingestion produces.
func chartEdgeStoreBatches(
	t *testing.T,
	ctx context.Context,
	store *Store,
	indexTime time.Time,
	events []*ingest.StoredEvent,
) {
	t.Helper()
	const chunk = 500
	for start := 0; start < len(events); start += chunk {
		end := min(start+chunk, len(events))
		batchID := fmt.Sprintf("chart-batch-%d", start/chunk)
		for _, event := range events[start:end] {
			event.BatchID = batchID
		}
		if _, err := store.Store(ctx, ingest.StoreBatch{
			TenantID: chartEdgeTenant, CollectorID: "collector", BatchID: batchID,
			BatchSequence:     uint64(start/chunk) + 1,
			SourceBatchSHA256: testSourceBatchDigest(batchID),
			ReceivedAt:        indexTime,
			Events:            events[start:end],
		}); err != nil {
			t.Fatalf("store chart fixture batch %s: %v", batchID, err)
		}
	}
}

func chartEdgeEvent(
	id, source string,
	at time.Time,
	level *string,
	fields ...*opensplunkv1.TypedObjectField,
) *ingest.StoredEvent {
	return chartEdgeEventHost(id, source, "api", at, level,
		opensplunkv1.LogSeverity_LOG_SEVERITY_INFO, fields...)
}

// chartEdgeRawEvent writes an event whose _raw carries exactly the given bytes
// under the given encoding, so the pivot can be compared against the ordinary
// group column for a field that legitimately holds non-UTF-8 data.
func chartEdgeRawEvent(
	id, source string,
	at time.Time,
	level *string,
	raw []byte,
	encoding opensplunkv1.RawEncoding,
) *ingest.StoredEvent {
	event := chartEdgeEvent(id, source, at, level)
	event.Event.Raw = raw
	event.Event.RawEncoding = encoding
	return event
}

func chartEdgeSeverityEvent(
	id string,
	at time.Time,
	level *string,
	severity opensplunkv1.LogSeverity,
	fields ...*opensplunkv1.TypedObjectField,
) *ingest.StoredEvent {
	return chartEdgeEventHost(id, "chart-bin", "api", at, level, severity, fields...)
}

func chartEdgeEventHost(
	id, source, host string,
	at time.Time,
	level *string,
	severity opensplunkv1.LogSeverity,
	fields ...*opensplunkv1.TypedObjectField,
) *ingest.StoredEvent {
	return &ingest.StoredEvent{
		TenantID:    chartEdgeTenant,
		CollectorID: "collector",
		BatchID:     "chart-batch",
		IndexTime:   time.Date(2026, time.July, 21, 4, 0, 0, 0, time.UTC),
		Event: &opensplunkv1.LogEvent{
			EventId:         id,
			IndexName:       chartEdgeIndex,
			EventTime:       timestamppb.New(at),
			CollectedAt:     timestamppb.New(at),
			EventTimeSource: opensplunkv1.EventTimeSource_EVENT_TIME_SOURCE_PARSED,
			Host:            host,
			Source:          source,
			Sourcetype:      "go:zap:json",
			Severity:        severity,
			Level:           level,
			Raw:             []byte(id),
			RawEncoding:     opensplunkv1.RawEncoding_RAW_ENCODING_UTF8,
			Message:         new("Request metrics"),
			Fields:          typedObjectValue(fields...),
		},
	}
}

// chartEdgeStartClickHouse starts an isolated pinned server, applies the
// repository migrations, and returns a query connection plus a store writer.
func chartEdgeStartClickHouse(t *testing.T, ctx context.Context) (clickhousedriver.Conn, *Store) {
	return startClickHouseStoreFixture(t, ctx)
}
