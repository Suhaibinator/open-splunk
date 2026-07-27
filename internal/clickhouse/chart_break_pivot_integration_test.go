package clickhouse

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	clickhousedriver "github.com/ClickHouse/clickhouse-go/v2"
	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/ingest"
	"github.com/Suhaibinator/open-splunk/internal/plan"
	"github.com/Suhaibinator/open-splunk/internal/spl"
	"github.com/Suhaibinator/open-splunk/internal/visibility"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	chartBreakIndex  = "chartbreak"
	chartBreakTenant = "tenant"
	// chartBreakBigCount is a single cell large enough to prove the published
	// count is an exact UInt64 rather than a saturating or narrowed accumulator.
	chartBreakBigCount = 5_000
)

var chartBreakBase = time.Date(2026, time.July, 21, 3, 0, 0, 0, time.UTC)

// TestChartPivotHostileDataAgainstClickHouse attacks the bounded two-field
// pivot's semantics with data chosen to break its label domain, its column
// bound, its tie-break, and its row-axis parity with stats. Every assertion is
// executed against the pinned ClickHouse server through the real store writer.
func TestChartPivotHostileDataAgainstClickHouse(t *testing.T) {
	if os.Getenv("OPEN_SPLUNK_CLICKHOUSE_INTEGRATION") != "1" {
		t.Skip("set OPEN_SPLUNK_CLICKHOUSE_INTEGRATION=1 to run the Docker integration test")
	}
	if _, err := exec.LookPath("docker"); err != nil {
		t.Skipf("docker CLI is unavailable: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	defer cancel()

	connection, store := chartBreakStartClickHouse(t, ctx)
	indexTime := time.Date(2026, time.July, 21, 4, 0, 0, 0, time.UTC)
	cutoff := chartBreakStoreFixture(t, ctx, store, indexTime)
	compile := func(source string) CompiledQuery {
		t.Helper()
		return chartBreakCompile(t, source, indexTime.Add(time.Minute), cutoff)
	}
	transport := func(source string) []chartBreakRow {
		t.Helper()
		return chartBreakTransport(t, ctx, connection, compile(source))
	}
	invalid := func(source string) uint8 {
		t.Helper()
		return chartBreakInvalid(t, ctx, connection, compile(source))
	}

	// A search comparison on the field that also names the column axis is an
	// ordinary supported query: the predicate belongs to the scoped scan and
	// the same field still names the pivot's column axis.
	//
	// Search string comparisons are case-insensitive (`where` comparisons are
	// not), so `series="alpha"` retains every spelling in bp-case and the pivot
	// publishes all three of them.
	t.Run("a search filter on the column field still executes", func(t *testing.T) {
		want := []chartBreakRow{{
			ordinal: 0, row: "/u",
			names:  "0:ALPHA|0:Alpha|0:alpha",
			counts: "1|2|3",
		}}
		for _, source := range []string{
			`index=chartbreak source="bp-case" series="alpha" | chart count OVER path BY series`,
			`index=chartbreak source="bp-case" | search series="alpha" | chart count OVER path BY series`,
		} {
			compiled := compile(source)
			var rows uint64
			if err := connection.QueryRow(ctx, "SELECT count() FROM ("+compiled.SQL+")", compiled.Args...).Scan(&rows); err != nil {
				t.Errorf("execute %q: %v", source, chartBreakBrief(err))
				continue
			}
			if got := chartBreakTransport(t, ctx, connection, compiled); !reflect.DeepEqual(got, want) {
				t.Errorf("%q = %#v, want %#v", source, got, want)
			}
		}
		// The presence-only comparison retains the same rows.
		presence := compile(`index=chartbreak source="bp-case" series=* | chart count OVER path BY series`)
		var rows uint64
		if err := connection.QueryRow(ctx, "SELECT count() FROM ("+presence.SQL+")", presence.Args...).Scan(&rows); err != nil {
			t.Errorf("execute series=* chart: %v", chartBreakBrief(err))
		} else if got := chartBreakTransport(t, ctx, connection, presence); !reflect.DeepEqual(got, want) {
			t.Errorf("series=* chart = %#v, want %#v", got, want)
		}

		// The controls: every neighboring shape executes, so the fixture and
		// the comparison are both fine.
		for _, source := range []string{
			`index=chartbreak source="bp-case" series="alpha" | stats count BY path, series`,
			`index=chartbreak source="bp-case" path="/u" | chart count OVER path BY series`,
			`index=chartbreak source="bp-case" series="alpha" | chart count OVER series BY level`,
			`index=chartbreak source="bp-case" | where series="alpha" | chart count OVER path BY series`,
		} {
			control := compile(source)
			var controlRows uint64
			if err := connection.QueryRow(ctx, "SELECT count() FROM ("+control.SQL+")", control.Args...).Scan(&controlRows); err != nil {
				t.Fatalf("control %q must execute: %v", source, chartBreakBrief(err))
			}
		}
	})

	t.Run("label normalization collisions", func(t *testing.T) {
		// Both colliding labels lose the top-ten cutoff, so neither would have
		// named a public column. The contract's rule is stated over labels, not
		// over published columns, so the command still fails atomically.
		if got := invalid(`index=chartbreak source="bp-collide-excluded" | chart count OVER path BY series`); got != 1 {
			t.Fatalf("collision below the cutoff invalid flag = %d, want 1", got)
		}
		// One colliding label is retained and the other is folded into OTHER,
		// so the published names are unique and only the raw label domain
		// collides.
		if got := invalid(`index=chartbreak source="bp-collide-split" | chart count OVER path BY series`); got != 1 {
			t.Fatalf("split collision invalid flag = %d, want 1", got)
		}
		// The controls prove both fixtures are otherwise chartable and that the
		// colliding labels really do sit where each fixture claims. With the
		// literal VALUE twin removed, bp-collide-excluded folds _x into OTHER
		// (it lost the cutoff) while bp-collide-split retains it as a column.
		excluded := transport(`index=chartbreak source="bp-collide-excluded" | where series!="VALUE_x" | chart count OVER path BY series`)
		wantExcluded := []chartBreakRow{{
			ordinal: 0, row: "/c",
			names:  "0:f01|0:f02|0:f03|0:f04|0:f05|0:f06|0:f07|0:f08|0:f09|0:f10|2:",
			counts: "10|10|10|10|10|10|10|10|10|10|5",
		}}
		if !reflect.DeepEqual(excluded, wantExcluded) {
			t.Fatalf("collision-free bp-collide-excluded = %#v, want %#v", excluded, wantExcluded)
		}
		split := transport(`index=chartbreak source="bp-collide-split" | where series!="VALUE_x" | chart count OVER path BY series`)
		wantSplit := []chartBreakRow{{
			ordinal: 0, row: "/c",
			names:  "0:_x|0:f01|0:f02|0:f03|0:f04|0:f05|0:f06|0:f07|0:f08|0:f09",
			counts: "8|10|10|10|10|10|10|10|10|10",
		}}
		if !reflect.DeepEqual(split, wantSplit) {
			t.Fatalf("collision-free bp-collide-split = %#v, want %#v", split, wantSplit)
		}
	})

	t.Run("collision on row-ineligible events only", func(t *testing.T) {
		// A reserved label carried only by an event that omits the row field
		// fails the whole command: the column value is classified on its own
		// presence.
		if got := invalid(`index=chartbreak source="bp-rowless-null" | chart count OVER path BY series`); got != 1 {
			t.Fatalf("row-less NULL label invalid flag = %d, want 1", got)
		}
		// The very same sentence of the contract lists convergence after VALUE
		// normalization alongside the reserved labels, so a collision carried
		// only by row-less events must fail identically.
		if got := invalid(`index=chartbreak source="bp-collide-rowless" | chart count OVER path BY series`); got != 1 {
			t.Fatalf("row-less collision invalid flag = %d, want 1", got)
		}
	})

	t.Run("labels differing only by case or unicode form stay distinct", func(t *testing.T) {
		got := transport(`index=chartbreak source="bp-case" | chart count OVER path BY series`)
		want := []chartBreakRow{{
			ordinal: 0, row: "/u",
			names:  "0:ALPHA|0:Alpha|0:alpha",
			counts: "1|2|3",
		}}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("case-distinct labels = %#v, want %#v", got, want)
		}

		// NFC and NFD spellings of the same grapheme, a zero-width space, and a
		// ligature are five distinct byte strings and therefore five columns.
		gotUnicode := transport(`index=chartbreak source="bp-unicode" | chart count OVER path BY series`)
		wantUnicode := []chartBreakRow{{
			ordinal: 0, row: "/u",
			names:  "0:ab|0:a\u200bb|0:é|0:é|0:ﬀ",
			counts: "1|1|1|1|1",
		}}
		if !reflect.DeepEqual(gotUnicode, wantUnicode) {
			t.Fatalf("unicode labels = %#v, want %#v", gotUnicode, wantUnicode)
		}
	})

	t.Run("label length is measured in bytes at the 256 byte boundary", func(t *testing.T) {
		got := transport(`index=chartbreak source="bp-length-ok" | chart count OVER path BY series`)
		want := []chartBreakRow{{
			ordinal: 0, row: "/len",
			names: "0:" + strings.Repeat("a", 255) + "|0:" + strings.Repeat("b", 256) +
				"|0:" + strings.Repeat("é", 128),
			counts: "1|1|1",
		}}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("boundary-length labels = %#v, want %#v", got, want)
		}
		for _, fixture := range []string{"bp-length-257", "bp-length-258", "bp-length-512"} {
			source := `index=chartbreak source="` + fixture + `" | chart count OVER path BY series`
			if got := invalid(source); got != 1 {
				t.Fatalf("%q invalid flag = %d, want 1", source, got)
			}
		}
	})

	t.Run("a label collides with the row column name after normalization", func(t *testing.T) {
		// The row column is literally named VALUE_x, so the ordinary label _x
		// would publish a second column with that exact name.
		if got := invalid(`index=chartbreak source="bp-rowname-value" | chart count OVER VALUE_x BY series`); got != 1 {
			t.Fatalf("normalized row-name collision invalid flag = %d, want 1", got)
		}
		// The same row field with a label that normalizes elsewhere is fine.
		got := transport(`index=chartbreak source="bp-rowname-ok" | chart count OVER VALUE_x BY series`)
		want := []chartBreakRow{{ordinal: 0, row: "/v", names: "0:_y", counts: "1"}}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("normalizing label beside VALUE_x row = %#v, want %#v", got, want)
		}

		// _raw is the mirror case: the label _raw publishes VALUE_raw, which
		// cannot duplicate the row column named _raw, so it is legal.
		gotRaw := transport(`index=chartbreak source="bp-raw-rowname" | chart count OVER _raw BY series`)
		wantRaw := []chartBreakRow{{ordinal: 0, row: "bp-raw-rowname-1", names: "0:_raw", counts: "1"}}
		if !reflect.DeepEqual(gotRaw, wantRaw) {
			t.Fatalf("_raw row with a _raw label = %#v, want %#v", gotRaw, wantRaw)
		}
	})

	t.Run("exactly ten eleven and twelve column values with and without null", func(t *testing.T) {
		ordinary := func(count int) string {
			parts := make([]string, 0, count)
			for index := 1; index <= count; index++ {
				parts = append(parts, fmt.Sprintf("0:k%02d", index))
			}
			return strings.Join(parts, "|")
		}
		ones := func(count int) []string {
			parts := make([]string, 0, count)
			for range count {
				parts = append(parts, "1")
			}
			return parts
		}
		for _, test := range []struct {
			fixture string
			names   string
			counts  string
		}{
			// Ten ordinary values fit exactly, so neither sentinel exists.
			{"bp-c10", ordinary(10), strings.Join(ones(10), "|")},
			// Ten ordinary values plus a missing one: NULL, still no OTHER.
			{"bp-c10n", ordinary(10) + "|1:", strings.Join(ones(11), "|")},
			// Eleven ordinary values: the eleventh becomes OTHER.
			{"bp-c11", ordinary(10) + "|2:", strings.Join(ones(11), "|")},
			// Eleven ordinary values plus a missing one: both sentinels.
			{"bp-c11n", ordinary(10) + "|1:|2:", strings.Join(ones(12), "|")},
			// Twelve ordinary values and no missing one: OTHER carries two.
			{"bp-c12", ordinary(10) + "|2:", strings.Join(ones(10), "|") + "|2"},
			// Twelve ordinary values plus a missing one saturate the twelve
			// runtime series the contract admits.
			{"bp-c12n", ordinary(10) + "|1:|2:", strings.Join(ones(10), "|") + "|1|2"},
		} {
			t.Run(test.fixture, func(t *testing.T) {
				got := transport(`index=chartbreak source="` + test.fixture + `" | chart count OVER path BY series`)
				want := []chartBreakRow{{ordinal: 0, row: "/n", names: test.names, counts: test.counts}}
				if !reflect.DeepEqual(got, want) {
					t.Fatalf("%s pivot = %#v, want %#v", test.fixture, got, want)
				}
				if series := strings.Count(test.names, "|") + 1; series > 12 {
					t.Fatalf("%s published %d series, the contract bounds them at 12", test.fixture, series)
				}
			})
		}
	})

	t.Run("ties at the top ten cutoff are a repeatable total order", func(t *testing.T) {
		// Twelve labels with identical counts. UTF-8 ascending order of the raw
		// label decides, so the underscore labels win their tie against every
		// letter label even though they publish last.
		compiled := compile(`index=chartbreak source="bp-tie" | chart count OVER path BY series`)
		want := []chartBreakRow{{
			ordinal: 0, row: "/t",
			names:  "0:_a|0:_b|0:a01|0:a02|0:a03|0:a04|0:a05|0:a06|0:a07|0:a08|2:",
			counts: "1|1|1|1|1|1|1|1|1|1|2",
		}}
		for attempt := range 5 {
			got := chartBreakTransport(t, ctx, connection, compiled)
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("tie-broken pivot attempt %d = %#v, want %#v", attempt, got, want)
			}
		}
	})

	t.Run("OTHER arithmetic is exact against stats", func(t *testing.T) {
		const scope = `index=chartbreak source="bp-other"`
		got := transport(scope + ` | chart count OVER path BY series`)
		if len(got) == 0 {
			t.Fatalf("OTHER fixture published no rows")
		}
		totals := chartBreakStatsTotals(t, ctx, connection, compile(scope+` | stats count BY path`), "path")
		perLabel := chartBreakStatsPairs(t, ctx, connection, compile(scope+` | stats count BY path, series`))
		for _, row := range got {
			names := strings.Split(row.names, "|")
			cells := strings.Split(row.counts, "|")
			if len(names) != len(cells) {
				t.Fatalf("row %q has %d names and %d cells", row.row, len(names), len(cells))
			}
			var kept, published, other, null uint64
			for index, name := range names {
				cell, err := strconv.ParseUint(cells[index], 10, 64)
				if err != nil {
					t.Fatalf("cell %q is not a count: %v", cells[index], err)
				}
				published += cell
				switch name {
				case "2:":
					other = cell
				case "1:":
					null = cell
				default:
					kept += cell
				}
			}
			total, ok := totals[row.row]
			if !ok {
				t.Fatalf("chart published row %q that stats count BY path never reports", row.row)
			}
			if published != total {
				t.Fatalf("row %q cells sum to %d, stats count BY path reports %d", row.row, published, total)
			}
			// Every labeled input count for this row, from stats, split into
			// the kept domain and the excluded remainder.
			var labeled, wantOther uint64
			for pair, count := range perLabel {
				if pair.row != row.row {
					continue
				}
				labeled += count
				if !chartBreakContains(names, "0:"+pair.label) {
					wantOther += count
				}
			}
			if other != wantOther {
				t.Fatalf("row %q OTHER = %d, stats says the excluded labels total %d", row.row, other, wantOther)
			}
			if kept != labeled-wantOther {
				t.Fatalf("row %q kept cells = %d, stats says the retained labels total %d", row.row, kept, labeled-wantOther)
			}
			if null != total-labeled {
				t.Fatalf("row %q NULL = %d, stats says %d inputs carried no label", row.row, null, total-labeled)
			}
		}
	})

	t.Run("empty explicit null and missing row values", func(t *testing.T) {
		// An empty string is a present value and names a row; an explicit null
		// and a missing field name none. Column values carried only by those
		// ineligible events never reach the published domain.
		got := transport(`index=chartbreak source="bp-rowmodes" | chart count OVER path BY series`)
		want := []chartBreakRow{{ordinal: 0, row: "", names: "0:keep", counts: "1"}}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("row presence modes = %#v, want %#v", got, want)
		}
		// The differential: stats count BY path agrees on exactly one group.
		reference := compile(`index=chartbreak source="bp-rowmodes" | stats count BY path`)
		if rows := chartBreakStatsTotals(t, ctx, connection, reference, "path"); len(rows) != 1 || rows[""] != 1 {
			t.Fatalf("stats count BY path = %#v, want exactly one empty-string group of 1", rows)
		}
	})

	t.Run("every dynamic storage type is a legal row value", func(t *testing.T) {
		const scope = `index=chartbreak source="bp-types-row"`
		got := transport(scope + ` | chart count OVER probe BY series`)
		order := make([]string, 0, len(got))
		for _, row := range got {
			order = append(order, row.row)
		}
		// Numeric-aware ascending order over the lexical scalar text: finite
		// numbers first, then remaining scalars in UTF-8 order. The tagged
		// envelopes contribute their decoded lexical value, not their map.
		want := []string{
			"-5", "2.5", "7", "12.5",
			"2026-07-21T03:00:00Z", "90:0", "AQI", "s", "true",
		}
		if !reflect.DeepEqual(order, want) {
			t.Fatalf("dynamic row values = %v, want %v", order, want)
		}
		// The contract's parity claim: the published row set and order equal
		// stats count BY probe | sort 0 +probe on the same data.
		reference := compile(scope + ` | stats count BY probe | sort 0 +probe`)
		statsRows := chartBreakRows(t, ctx, connection,
			"SELECT toString("+quoteIdentifier("probe")+") FROM ("+reference.SQL+")", reference.Args, 1)
		statsOrder := make([]string, 0, len(statsRows))
		for _, row := range statsRows {
			statsOrder = append(statsOrder, row[0])
		}
		if !reflect.DeepEqual(order, statsOrder) {
			t.Fatalf("chart row axis = %v, stats count BY probe | sort 0 +probe = %v", order, statsOrder)
		}
	})

	t.Run("numeric-looking strings and numbers converge exactly as stats groups them", func(t *testing.T) {
		const scope = `index=chartbreak source="bp-numstr"`
		got := transport(scope + ` | chart count OVER probe BY series`)
		reference := compile(scope + ` | stats count BY probe | sort 0 +probe`)
		statsRows := chartBreakRows(t, ctx, connection,
			"SELECT toString("+quoteIdentifier("probe")+"), toString(count) FROM ("+reference.SQL+")", reference.Args, 2)
		if len(got) != len(statsRows) {
			t.Fatalf("chart published %d rows, stats count BY probe published %d: %#v vs %#v", len(got), len(statsRows), got, statsRows)
		}
		for index, row := range got {
			if row.row != statsRows[index][0] {
				t.Fatalf("row %d = %q, stats reports %q", index, row.row, statsRows[index][0])
			}
			if total := chartBreakSum(t, row.counts); total != statsRows[index][1] {
				t.Fatalf("row %q sums to %s, stats reports %s", row.row, total, statsRows[index][1])
			}
		}
		// The concrete convergence: the stored integer 7 and the string "7" are
		// one row, while "007" and " 7" stay separate row values.
		order := make([]string, 0, len(got))
		for _, row := range got {
			order = append(order, row.row)
		}
		if !reflect.DeepEqual(order, []string{"007", "7", " 7"}) {
			t.Fatalf("numeric-string rows = %#v, want [007 7 \" 7\"]", order)
		}
		if got[1].counts != "2" {
			t.Fatalf("row 7 counts = %q, want the integer and the string folded into 2", got[1].counts)
		}
	})

	t.Run("every non-string dynamic column value fails the command", func(t *testing.T) {
		for _, fixture := range []string{
			"bp-col-double", "bp-col-decimal", "bp-col-duration", "bp-col-bytes", "bp-col-object",
		} {
			t.Run(fixture, func(t *testing.T) {
				source := `index=chartbreak source="` + fixture + `" | chart count OVER path BY series`
				if got := invalid(source); got != 1 {
					t.Fatalf("%q invalid flag = %d, want 1", source, got)
				}
			})
		}
		// An explicit null is not an unsupported value: it is the documented
		// usenull column. A numeric-looking string is an ordinary label.
		got := transport(`index=chartbreak source="bp-col-null" | chart count OVER path BY series`)
		want := []chartBreakRow{{ordinal: 0, row: "/c", names: "1:", counts: "2"}}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("explicit-null column = %#v, want %#v", got, want)
		}
		gotNumeric := transport(`index=chartbreak source="bp-col-numstr" | chart count OVER path BY series`)
		wantNumeric := []chartBreakRow{{ordinal: 0, row: "/c", names: "0:7|0:_7", counts: "1|1"}}
		if !reflect.DeepEqual(gotNumeric, wantNumeric) {
			t.Fatalf("numeric-string column = %#v, want %#v", gotNumeric, wantNumeric)
		}
	})

	t.Run("usenull with no non-null column value at all", func(t *testing.T) {
		for _, fixture := range []string{"bp-usenull-missing", "bp-usenull-explicit"} {
			t.Run(fixture, func(t *testing.T) {
				got := transport(`index=chartbreak source="` + fixture + `" | chart count OVER path BY series`)
				want := []chartBreakRow{{ordinal: 0, row: "/u", names: "1:", counts: "3"}}
				if !reflect.DeepEqual(got, want) {
					t.Fatalf("%s pivot = %#v, want %#v", fixture, got, want)
				}
			})
		}
	})

	t.Run("a previously binned row field", func(t *testing.T) {
		// bin is the only discretizer; its bucket starts are ordinary row values
		// and must group exactly as the identical stats grouping does.
		rowGot := transport(`index=chartbreak source="bp-bin" | bin sev span=10 | chart count OVER sev BY series`)
		reference := compile(`index=chartbreak source="bp-bin" | bin sev span=10 | stats count BY sev | sort 0 +sev`)
		statsRows := chartBreakRows(t, ctx, connection,
			"SELECT toString("+quoteIdentifier("sev")+"), toString(count) FROM ("+reference.SQL+")", reference.Args, 2)
		if len(rowGot) != len(statsRows) || len(statsRows) != 3 {
			t.Fatalf("binned chart rows = %#v, stats rows = %#v", rowGot, statsRows)
		}
		for index, row := range rowGot {
			if row.row != statsRows[index][0] || chartBreakSum(t, row.counts) != statsRows[index][1] {
				t.Fatalf("binned row %d = %#v, stats reports %v", index, row, statsRows[index])
			}
		}
		// A binned fixed numeric or time column keeps its own scalar type, so
		// naming it as the column axis is rejected before execution.
		for _, source := range []string{
			`index=chartbreak source="bp-bin" | bin severity span=10 | chart count OVER path BY severity`,
			`index=chartbreak source="bp-bin" | bin _time span=5m AS bucket_time | chart count OVER path BY bucket_time`,
		} {
			if code := chartBreakCompileDiagnostic(t, source, indexTime.Add(time.Minute), cutoff); code != "SPL_UNSUPPORTED_CHART_FIELD_TYPE" {
				t.Fatalf("%q diagnostic = %q, want SPL_UNSUPPORTED_CHART_FIELD_TYPE", source, code)
			}
		}
	})

	// `bin` is not a column-axis remedy in this version, and the contract no
	// longer claims it is. `bin` discretizes numbers, so binning a column field
	// replaces its string labels with numeric bucket starts — precisely the
	// value class the column axis rejects atomically. A fixed numeric or time
	// field is rejected at compile time; a runtime-typed one raises the atomic
	// invalid flag at execution, which is the same boundary the sibling
	// "every non-string dynamic column value fails the command" pins.
	t.Run("bin is not a column-axis remedy", func(t *testing.T) {
		// The unbinned control: numeric-looking strings are ordinary labels.
		before := transport(`index=chartbreak source="bp-bin-str" | chart count OVER path BY series`)
		wantBefore := []chartBreakRow{{ordinal: 0, row: "/b", names: "0:12|0:15|0:27|0:5", counts: "1|1|1|1"}}
		if !reflect.DeepEqual(before, wantBefore) {
			t.Fatalf("unbinned string column = %#v, want %#v", before, wantBefore)
		}
		// The binned value itself is a perfectly ordinary SPL value: the same
		// bin feeds stats BY and feeds chart's own row axis without complaint.
		statsRef := compile(`index=chartbreak source="bp-bin-str" | bin series span=10 | stats count BY series | sort 0 +series`)
		statsRows := chartBreakRows(t, ctx, connection,
			"SELECT toString("+quoteIdentifier("series")+"), toString(count) FROM ("+statsRef.SQL+")", statsRef.Args, 2)
		wantStats := [][]string{{"0", "1"}, {"10", "2"}, {"20", "1"}}
		if !reflect.DeepEqual(statsRows, wantStats) {
			t.Fatalf("binned stats grouping = %#v, want %#v", statsRows, wantStats)
		}
		rowAxis := transport(`index=chartbreak source="bp-bin-str" | bin series span=10 | chart count OVER series BY level`)
		if len(rowAxis) != 3 {
			t.Fatalf("binned row axis = %#v, want the three observed buckets", rowAxis)
		}

		// Binning the column axis turns every label into a number, so the whole
		// command fails atomically with no partial pivot.
		binned := compile(`index=chartbreak source="bp-bin-str" | bin series span=10 | chart count OVER path BY series`)
		if got := chartBreakInvalid(t, ctx, connection, binned); got != 1 {
			t.Fatalf("binned column axis invalid flag = %d, want 1", got)
		}
	})

	t.Run("_raw rows carry unicode and order exactly as sort does", func(t *testing.T) {
		const scope = `index=chartbreak source="bp-raw-unicode"`
		compiled := compile(scope + ` | chart count OVER _raw BY series`)
		if compiled.Chart == nil || compiled.Chart.RowKind != ChartRowKindMixed {
			t.Fatalf("_raw row contract = %#v", compiled.Chart)
		}
		got := chartBreakTransport(t, ctx, connection, compiled)
		order := make([]string, 0, len(got))
		for _, row := range got {
			order = append(order, row.row)
		}
		reference := compile(scope + ` | stats count BY _raw | sort 0 +_raw`)
		statsRows := chartBreakRows(t, ctx, connection,
			"SELECT toString("+quoteIdentifier("_raw")+"), toString(count) FROM ("+reference.SQL+")", reference.Args, 2)
		statsOrder := make([]string, 0, len(statsRows))
		for _, row := range statsRows {
			statsOrder = append(statsOrder, row[0])
		}
		if !reflect.DeepEqual(order, statsOrder) {
			t.Fatalf("chart _raw order = %#v, stats count BY _raw | sort 0 +_raw = %#v", order, statsOrder)
		}
		if len(order) != 5 {
			t.Fatalf("_raw rows = %#v, want the five distinct raw payloads", order)
		}
		for index, row := range got {
			if total := chartBreakSum(t, row.counts); total != statsRows[index][1] {
				t.Fatalf("_raw row %q sums to %s, stats reports %s", row.row, total, statsRows[index][1])
			}
		}
	})

	t.Run("cells are exact unsigned counts", func(t *testing.T) {
		compiled := compile(`index=chartbreak source="bp-bigcount" | chart count OVER path BY series`)
		rows := chartBreakRows(t, ctx, connection,
			"SELECT toTypeName("+quoteIdentifier(ChartCountsColumn)+"), "+
				"toString(arrayElement("+quoteIdentifier(ChartCountsColumn)+", 1)) FROM ("+compiled.SQL+")",
			compiled.Args, 2)
		want := [][]string{{"Array(UInt64)", strconv.Itoa(chartBreakBigCount)}}
		if !reflect.DeepEqual(rows, want) {
			t.Fatalf("large cell = %#v, want %#v", rows, want)
		}
	})

	t.Run("input where every column value is unusable", func(t *testing.T) {
		// Not one input row carries a usable column value, so the command fails
		// atomically instead of publishing an all-OTHER or all-NULL pivot.
		if got := invalid(`index=chartbreak source="bp-allbad" | chart count OVER path BY series`); got != 1 {
			t.Fatalf("wholly unusable column axis invalid flag = %d, want 1", got)
		}
	})

	t.Run("empty input publishes nothing at all", func(t *testing.T) {
		for _, source := range []string{
			`index=chartbreak source="bp-nothing" | chart count OVER path BY series`,
			`index=chartbreak source="bp-case" path="/does-not-exist" | chart count OVER path BY series`,
		} {
			if got := chartBreakTransport(t, ctx, connection, compile(source)); len(got) != 0 {
				t.Fatalf("%q = %#v, want no rows", source, got)
			}
		}
	})
}

func chartBreakContains(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

type chartBreakRow struct {
	ordinal uint64
	row     string
	names   string
	counts  string
}

func chartBreakTransport(
	t *testing.T,
	ctx context.Context,
	connection clickhousedriver.Conn,
	compiled CompiledQuery,
) []chartBreakRow {
	t.Helper()
	if compiled.Chart == nil {
		t.Fatalf("compiled query is not a chart: %#v", compiled)
	}
	query := "SELECT toString(" + quoteIdentifier(ChartOrdinalColumn) + "), " +
		"toString(" + quoteIdentifier(ChartRowColumn) + "), " +
		"arrayStringConcat(" + quoteIdentifier(ChartNamesColumn) + ", '|'), " +
		"arrayStringConcat(arrayMap(value -> toString(value), " + quoteIdentifier(ChartCountsColumn) + "), '|'), " +
		"toString(" + quoteIdentifier(ChartInvalidColumn) + ") FROM (" + compiled.SQL + ")"
	scanned := chartBreakRows(t, ctx, connection, query, compiled.Args, 5)
	collected := make([]chartBreakRow, 0, len(scanned))
	for index, row := range scanned {
		if row[4] != "0" {
			t.Fatalf("chart row %d raised the atomic invalid flag: %#v", index, row)
		}
		ordinal, err := strconv.ParseUint(row[0], 10, 64)
		if err != nil {
			t.Fatalf("chart ordinal %q is not a number: %v", row[0], err)
		}
		if ordinal != uint64(index) {
			t.Fatalf("chart ordinal %d is not dense at position %d", ordinal, index)
		}
		collected = append(collected, chartBreakRow{ordinal: ordinal, row: row[1], names: row[2], counts: row[3]})
	}
	return collected
}

func chartBreakInvalid(
	t *testing.T,
	ctx context.Context,
	connection clickhousedriver.Conn,
	compiled CompiledQuery,
) uint8 {
	t.Helper()
	if compiled.Chart == nil {
		t.Fatalf("compiled query is not a chart: %#v", compiled)
	}
	query := "SELECT maxOrDefault(" + quoteIdentifier(ChartInvalidColumn) + ") FROM (" + compiled.SQL + ")"
	var invalid uint8
	if err := connection.QueryRow(ctx, query, compiled.Args...).Scan(&invalid); err != nil {
		t.Fatalf("execute chart validity fold: %v\nSQL: %s", err, query)
	}
	return invalid
}

func chartBreakSum(t *testing.T, counts string) string {
	t.Helper()
	var total uint64
	for _, cell := range strings.Split(counts, "|") {
		value, err := strconv.ParseUint(cell, 10, 64)
		if err != nil {
			t.Fatalf("chart cell %q is not a count: %v", cell, err)
		}
		total += value
	}
	return strconv.FormatUint(total, 10)
}

// chartBreakStatsTotals reads stats count BY <field> as the authoritative row
// totals the pivot must reproduce.
func chartBreakStatsTotals(
	t *testing.T,
	ctx context.Context,
	connection clickhousedriver.Conn,
	compiled CompiledQuery,
	field string,
) map[string]uint64 {
	t.Helper()
	query := "SELECT toString(" + quoteIdentifier(field) + "), toString(count) FROM (" + compiled.SQL + ")"
	totals := map[string]uint64{}
	for _, row := range chartBreakRows(t, ctx, connection, query, compiled.Args, 2) {
		value, err := strconv.ParseUint(row[1], 10, 64)
		if err != nil {
			t.Fatalf("stats count %q is not a number: %v", row[1], err)
		}
		totals[row[0]] = value
	}
	return totals
}

type chartBreakPair struct {
	row   string
	label string
}

func chartBreakStatsPairs(
	t *testing.T,
	ctx context.Context,
	connection clickhousedriver.Conn,
	compiled CompiledQuery,
) map[chartBreakPair]uint64 {
	t.Helper()
	query := "SELECT toString(" + quoteIdentifier("path") + "), toString(" + quoteIdentifier("series") +
		"), toString(count) FROM (" + compiled.SQL + ")"
	pairs := map[chartBreakPair]uint64{}
	for _, row := range chartBreakRows(t, ctx, connection, query, compiled.Args, 3) {
		value, err := strconv.ParseUint(row[2], 10, 64)
		if err != nil {
			t.Fatalf("stats count %q is not a number: %v", row[2], err)
		}
		pairs[chartBreakPair{row: row[0], label: row[1]}] = value
	}
	return pairs
}

func chartBreakRows(
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

func chartBreakBuild(source string, cutoff time.Time, visibilityCutoff uint64) (CompiledQuery, error) {
	parsed, err := spl.Parse(source)
	if err != nil {
		return CompiledQuery{}, err
	}
	logical, err := plan.Build(parsed, plan.Scope{
		TenantID: chartBreakTenant, AuthorizedIndexes: []string{chartBreakIndex},
		Earliest:         time.Date(2026, time.July, 21, 0, 0, 0, 0, time.UTC),
		Latest:           time.Date(2026, time.July, 22, 0, 0, 0, 0, time.UTC),
		IndexTimeCutoff:  cutoff,
		VisibilityCutoff: uint64PointerForIntegration(visibilityCutoff),
	})
	if err != nil {
		return CompiledQuery{}, err
	}
	return (Compiler{}).Compile(logical)
}

func chartBreakCompile(t *testing.T, source string, cutoff time.Time, visibilityCutoff uint64) CompiledQuery {
	t.Helper()
	compiled, err := chartBreakBuild(source, cutoff, visibilityCutoff)
	if err != nil {
		t.Fatalf("compile chart SPL %q: %v", source, err)
	}
	if got, want := strings.Count(compiled.SQL, "?"), len(compiled.Args); got != want {
		t.Fatalf("placeholder count = %d, args = %d for %q", got, want, source)
	}
	return compiled
}

// chartBreakCompileDiagnostic returns the diagnostic code a rejected chart
// produces, whichever stage rejects it.
func chartBreakCompileDiagnostic(t *testing.T, source string, cutoff time.Time, visibilityCutoff uint64) string {
	t.Helper()
	_, err := chartBreakBuild(source, cutoff, visibilityCutoff)
	if err == nil {
		t.Fatalf("chart SPL %q compiled, want a diagnostic", source)
	}
	diagnostic := &plan.Diagnostic{}
	ok := errors.As(err, &diagnostic)
	if !ok {
		t.Fatalf("chart SPL %q error = %v (%T), want a diagnostic", source, err, err)
	}
	return diagnostic.Code
}

// chartBreakStoreFixture writes every hostile scenario through the store
// writer. One source per scenario keeps each subtest scoped to its own rows.
func chartBreakStoreFixture(t *testing.T, ctx context.Context, store *Store, indexTime time.Time) uint64 {
	t.Helper()
	info := "INFO"
	var events []*ingest.StoredEvent
	counter := 0
	add := func(source string, fields ...*opensplunkv1.TypedObjectField) {
		counter++
		events = append(events, chartBreakEvent(fmt.Sprintf("%s-%d", source, counter), source, "api", chartBreakBase, &info,
			opensplunkv1.LogSeverity_LOG_SEVERITY_INFO, fields...))
	}
	path := func(value string) *opensplunkv1.TypedObjectField {
		return typedField("path", typedString(value))
	}
	series := func(value string) *opensplunkv1.TypedObjectField {
		return typedField("series", typedString(value))
	}

	// Normalization collisions. In bp-collide-excluded both colliding labels
	// lose the cutoff; in bp-collide-split one is retained and one is not.
	for index := 1; index <= 10; index++ {
		for range 10 {
			add("bp-collide-excluded", path("/c"), series(fmt.Sprintf("f%02d", index)))
		}
	}
	for range 5 {
		add("bp-collide-excluded", path("/c"), series("_x"))
	}
	add("bp-collide-excluded", path("/c"), series("VALUE_x"))
	for index := 1; index <= 9; index++ {
		for range 10 {
			add("bp-collide-split", path("/c"), series(fmt.Sprintf("f%02d", index)))
		}
	}
	for range 8 {
		add("bp-collide-split", path("/c"), series("_x"))
	}
	add("bp-collide-split", path("/c"), series("VALUE_x"))

	// A colliding pair carried only by events that omit the row field, beside
	// a reserved label in the same position as a control.
	add("bp-collide-rowless", path("/c"), series("ok"))
	add("bp-collide-rowless", series("_x"))
	add("bp-collide-rowless", series("VALUE_x"))
	add("bp-rowless-null", path("/c"), series("ok"))
	add("bp-rowless-null", series("NULL"))

	// Labels differing only by case, and by unicode normalization form.
	add("bp-case", path("/u"), series("ALPHA"))
	for range 2 {
		add("bp-case", path("/u"), series("Alpha"))
	}
	for range 3 {
		add("bp-case", path("/u"), series("alpha"))
	}
	for _, label := range []string{"ab", "a\u200bb", "é", "é", "ﬀ"} {
		add("bp-unicode", path("/u"), series(label))
	}

	// Byte-measured label lengths at the 256-byte boundary.
	add("bp-length-ok", path("/len"), series(strings.Repeat("a", 255)))
	add("bp-length-ok", path("/len"), series(strings.Repeat("b", 256)))
	add("bp-length-ok", path("/len"), series(strings.Repeat("é", 128)))
	add("bp-length-257", path("/len"), series(strings.Repeat("a", 257)))
	// 129 two-byte code points: 129 characters but 258 bytes.
	add("bp-length-258", path("/len"), series(strings.Repeat("é", 129)))
	add("bp-length-512", path("/len"), series(strings.Repeat("a", 512)))

	// A row column literally named VALUE_x, which the label _x would duplicate.
	add("bp-rowname-value", typedField("VALUE_x", typedString("/v")), series("_x"))
	add("bp-rowname-ok", typedField("VALUE_x", typedString("/v")), series("_y"))

	// Ten, eleven, and twelve distinct column values with and without NULL.
	for index := 1; index <= 10; index++ {
		label := fmt.Sprintf("k%02d", index)
		for _, fixture := range []string{"bp-c10", "bp-c10n", "bp-c11", "bp-c11n", "bp-c12", "bp-c12n"} {
			add(fixture, path("/n"), series(label))
		}
	}
	add("bp-c10n", path("/n"))
	for _, fixture := range []string{"bp-c11", "bp-c11n", "bp-c12", "bp-c12n"} {
		add(fixture, path("/n"), series("k11"))
	}
	for _, fixture := range []string{"bp-c12", "bp-c12n"} {
		add(fixture, path("/n"), series("k12"))
	}
	add("bp-c11n", path("/n"))
	add("bp-c12n", path("/n"))

	// Twelve labels tied at one count each. UTF-8 ascending order of the raw
	// label decides the cutoff, so _a and _b beat a09 and a10.
	add("bp-tie", path("/t"), series("_a"))
	add("bp-tie", path("/t"), series("_b"))
	for index := 1; index <= 10; index++ {
		add("bp-tie", path("/t"), series(fmt.Sprintf("a%02d", index)))
	}

	// OTHER arithmetic: three rows, fourteen labels with uneven weights, and
	// unlabelled inputs so NULL, OTHER, and the kept domain must all balance.
	for index := 1; index <= 14; index++ {
		for range index {
			add("bp-other", path("/m1"), series(fmt.Sprintf("x%02d", index)))
		}
		if index%2 == 0 {
			add("bp-other", path("/m2"), series(fmt.Sprintf("x%02d", index)))
		}
		if index%5 == 0 {
			for range 3 {
				add("bp-other", path("/m3"), series(fmt.Sprintf("x%02d", index)))
			}
		}
	}
	for range 4 {
		add("bp-other", path("/m1"))
	}
	add("bp-other", path("/m2"), typedField("series", typedNull()))
	add("bp-other", path("/m3"))

	// Empty string, explicit null, and missing row values side by side.
	add("bp-rowmodes", path(""), series("keep"))
	add("bp-rowmodes", typedField("path", typedNull()), series("fromnull"))
	add("bp-rowmodes", series("frommissing"))

	// Every dynamic storage type as a row value, including tagged envelopes.
	for _, value := range []*opensplunkv1.TypedValue{
		typedString("s"),
		typedSint(-5),
		typedUint(7),
		typedDouble(2.5),
		typedBool(true),
		typedTimestamp(chartBreakBase),
		typedDuration(90 * time.Second),
		typedDecimal("12.5"),
		typedBytes([]byte{0x01, 0x02}),
	} {
		add("bp-types-row", typedField("probe", value), series("INFO"))
	}

	// Numeric strings against a stored integer.
	add("bp-numstr", typedField("probe", typedUint(7)), series("INFO"))
	add("bp-numstr", typedField("probe", typedString("7")), series("INFO"))
	add("bp-numstr", typedField("probe", typedString("007")), series("INFO"))
	add("bp-numstr", typedField("probe", typedString(" 7")), series("INFO"))

	// Every non-string column value, plus the two supported shapes.
	add("bp-col-double", path("/c"), typedField("series", typedDouble(1.5)))
	add("bp-col-decimal", path("/c"), typedField("series", typedDecimal("1.5")))
	add("bp-col-duration", path("/c"), typedField("series", typedDuration(time.Second)))
	add("bp-col-bytes", path("/c"), typedField("series", typedBytes([]byte{0x01})))
	add("bp-col-object", path("/c"), typedField("series", typedObject(typedField("child", typedString("leaf")))))
	add("bp-col-null", path("/c"), typedField("series", typedNull()))
	add("bp-col-null", path("/c"))
	add("bp-col-numstr", path("/c"), series("7"))
	add("bp-col-numstr", path("/c"), series("_7"))

	// usenull with no non-null column value anywhere in the input.
	for range 3 {
		add("bp-usenull-missing", path("/u"))
		add("bp-usenull-explicit", path("/u"), typedField("series", typedNull()))
	}

	// A binnable runtime-typed field beside the fixed numeric severity column.
	for _, value := range []uint64{5, 12, 15, 27} {
		add("bp-bin", path("/b"), typedField("sev", typedUint(value)), series("INFO"))
	}
	// The same severities stored as strings, which chart accepts as column
	// labels until the documented bin remedy is applied to them.
	for _, value := range []string{"5", "12", "15", "27"} {
		add("bp-bin-str", path("/b"), series(value))
	}

	// Unicode and numeric-looking _raw payloads.
	for _, raw := range []string{"9", "10", "é", "\U0001f600", "abc"} {
		counter++
		event := chartBreakEvent(fmt.Sprintf("bp-raw-unicode-%d", counter), "bp-raw-unicode", "api",
			chartBreakBase, &info, opensplunkv1.LogSeverity_LOG_SEVERITY_INFO, series("INFO"))
		event.Event.Raw = []byte(raw)
		events = append(events, event)
	}
	counter++
	rawNamed := chartBreakEvent("bp-raw-rowname-1", "bp-raw-rowname", "api", chartBreakBase, &info,
		opensplunkv1.LogSeverity_LOG_SEVERITY_INFO, series("_raw"))
	rawNamed.Event.Raw = []byte("bp-raw-rowname-1")
	events = append(events, rawNamed)

	// One cell large enough to prove the count is an exact UInt64.
	for range chartBreakBigCount {
		add("bp-bigcount", path("/big"), series("one"))
	}

	// Every column value in the input is unusable.
	add("bp-allbad", path("/a"), typedField("series", typedSint(1)))
	add("bp-allbad", path("/a"), typedField("series", typedSint(2)))

	chartBreakStoreBatches(t, ctx, store, indexTime, events)
	cutoff, err := store.VisibilityCutoff(ctx)
	if err != nil {
		t.Fatalf("capture chart visibility cutoff: %v", err)
	}
	return cutoff
}

func chartBreakStoreBatches(
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
		batchID := fmt.Sprintf("chart-break-batch-%d", start/chunk)
		for _, event := range events[start:end] {
			event.BatchID = batchID
		}
		if _, err := store.Store(ctx, ingest.StoreBatch{
			TenantID: chartBreakTenant, CollectorID: "collector", BatchID: batchID,
			BatchSequence:     uint64(start/chunk) + 1,
			SourceBatchSHA256: testSourceBatchDigest(batchID),
			ReceivedAt:        indexTime,
			Events:            events[start:end],
		}); err != nil {
			t.Fatalf("store chart break fixture batch %s: %v", batchID, err)
		}
	}
}

func chartBreakEvent(
	id, source, host string,
	at time.Time,
	level *string,
	severity opensplunkv1.LogSeverity,
	fields ...*opensplunkv1.TypedObjectField,
) *ingest.StoredEvent {
	return &ingest.StoredEvent{
		TenantID:    chartBreakTenant,
		CollectorID: "collector",
		BatchID:     "chart-break-batch",
		IndexTime:   time.Date(2026, time.July, 21, 4, 0, 0, 0, time.UTC),
		Event: &opensplunkv1.LogEvent{
			EventId:         id,
			IndexName:       chartBreakIndex,
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
			Message:         stringPointer("Request metrics"),
			Fields:          typedObjectValue(fields...),
		},
	}
}

func chartBreakStartClickHouse(t *testing.T, ctx context.Context) (clickhousedriver.Conn, *Store) {
	t.Helper()
	container := "open-splunk-chart-break-" + integrationRandomHex(t, 6)
	password := integrationRandomHex(t, 24)
	image := os.Getenv("OPEN_SPLUNK_CLICKHOUSE_TEST_IMAGE")
	if image == "" {
		image = storeIntegrationImage
	}
	integrationDocker(t, ctx, nil,
		"run", "--detach", "--rm", "--name", container,
		"--publish", "127.0.0.1::9000",
		"--env", "CLICKHOUSE_DB=open_splunk",
		"--env", "CLICKHOUSE_USER=open_splunk",
		"--env", "CLICKHOUSE_PASSWORD="+password,
		"--env", "CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT=1",
		image,
	)
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cleanupCancel()
		_ = exec.CommandContext(cleanupCtx, "docker", "rm", "--force", container).Run()
	})
	integrationWaitForClickHouse(t, ctx, container, password)

	migrationPaths, err := filepath.Glob(filepath.Join("..", "..", "migrations", "clickhouse", "[0-9][0-9][0-9][0-9]_*.sql"))
	if err != nil || len(migrationPaths) == 0 {
		t.Fatalf("discover migrations: paths=%v err=%v", migrationPaths, err)
	}
	var migrations bytes.Buffer
	for _, migrationPath := range migrationPaths {
		migration, readErr := os.ReadFile(migrationPath)
		if readErr != nil {
			t.Fatalf("read migration %s: %v", migrationPath, readErr)
		}
		migrations.Write(migration)
		migrations.WriteByte('\n')
	}
	integrationDocker(t, ctx, bytes.NewReader(migrations.Bytes()),
		"exec", "--interactive", container, "clickhouse-client",
		"--user", "open_splunk", "--password", password, "--multiquery",
	)

	config := DefaultConfig()
	config.Addresses = []string{integrationNativeAddress(t, ctx, container)}
	config.Username = "open_splunk"
	config.Password = password
	controlDB, err := control.Open(ctx, filepath.Join(t.TempDir(), "control.sqlite"))
	if err != nil {
		t.Fatalf("open chart break visibility control database: %v", err)
	}
	t.Cleanup(func() { _ = controlDB.Close() })
	sequencer, err := visibility.NewSQLite(ctx, controlDB)
	if err != nil {
		t.Fatalf("create chart break visibility sequencer: %v", err)
	}
	store, err := Open(config, fixedRetention(30*24*time.Hour), sequencer)
	if err != nil {
		t.Fatalf("open chart break store: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	if err := store.Ping(ctx); err != nil {
		t.Fatalf("ping chart break store: %v", err)
	}
	options, _, err := config.clickHouseOptions()
	if err != nil {
		t.Fatalf("resolve chart break ClickHouse options: %v", err)
	}
	connection, err := clickhousedriver.Open(options)
	if err != nil {
		t.Fatalf("open chart break query connection: %v", err)
	}
	t.Cleanup(func() { _ = connection.Close() })
	return connection, store
}

func chartBreakBrief(err error) string {
	if err == nil {
		return "<nil>"
	}
	text := err.Error()
	if index := strings.Index(text, ": in block"); index > 0 {
		text = text[:index]
	}
	if len(text) > 400 {
		text = text[:400]
	}
	return text
}
