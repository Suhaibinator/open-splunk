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
	"github.com/Suhaibinator/open-splunk/internal/testsupport"
)

// TestChartPercentileAgainstClickHouse pins percentile chart's raw GK-state
// merge and nullable transport against the repository's digest-pinned server.
// The fixture deliberately makes eleven ordinary labels tie for the ten slots,
// while the two omitted labels have very different finalized percentiles. A
// correct OTHER merges their twenty-one members before percentile finalization.
func TestChartPercentileAgainstClickHouse(t *testing.T) {
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
	visibilityCutoff := chartPercentileStoreFixture(t, ctx, store, indexTime)
	compile := func(t *testing.T, function string) CompiledQuery {
		t.Helper()
		return chartEdgeCompile(
			t,
			fmt.Sprintf(
				`index=chartedge source="chart-percentile" | chart %s(metric) OVER path BY series`,
				function,
			),
			indexTime.Add(time.Minute),
			visibilityCutoff,
		)
	}

	wantNames := []string{
		"0:a", "0:b", "0:c", "0:d", "0:e", "0:f",
		"0:g", "0:h", "0:i", "0:j", "1:", "2:",
	}
	for _, function := range []string{"p95", "perc50"} {
		t.Run(function+" exact rank lexical ties and pooled other", func(t *testing.T) {
			compiled := compile(t, function)
			if compiled.Chart == nil || compiled.Timechart != nil ||
				compiled.Chart.ValueKind != ChartValueKindPercentile ||
				!compiled.Chart.ValueKind.Valid() {
				t.Fatalf("%s chart contract = %#v", function, compiled)
			}
			if got := strings.Count(compiled.SQL, `FROM "open_splunk"."events"`); got != 1 {
				t.Fatalf("%s chart scoped storage scans = %d, want one:\n%s", function, got, compiled.SQL)
			}
			if strings.Contains(strings.ToUpper(compiled.SQL), "ARRAY JOIN") ||
				strings.Count(compiled.SQL, "quantilesGKOrNullArrayState(") != 1 ||
				strings.Count(compiled.SQL, "quantilesGKOrNullArrayMerge(") != 1 {
				t.Fatalf("%s chart does not use one non-expanding mergeable GK path:\n%s", function, compiled.SQL)
			}
			actions := explainCompiledQuery(t, ctx, connection, explainActionsPrefix, compiled)
			if strings.Contains(actions, "ArrayJoin") {
				t.Fatalf("%s chart physical plan expands event rows:\n%s", function, actions)
			}

			rows := chartNumericTransport(t, ctx, connection, compiled)
			if len(rows) != 3 {
				t.Fatalf("%s chart rows = %#v, want three retained row values", function, rows)
			}
			for index, wantRow := range []string{"/empty", "/rank", "/zero"} {
				if rows[index].ordinal != uint64(index) || rows[index].row != wantRow ||
					!reflect.DeepEqual(rows[index].names, wantNames) {
					t.Fatalf(
						"%s chart row %d = %#v, want ordinal %d row %q names %#v",
						function,
						index,
						rows[index],
						index,
						wantRow,
						wantNames,
					)
				}
			}

			// c is globally retained by the /rank row, but every measure on
			// /empty is missing, null, Boolean, or nonnumeric. Row eligibility
			// is independent of percentile eligibility, so the row remains and
			// its complete grid is null.
			for index, present := range rows[0].present {
				if present != 0 || rows[0].values[index] != 0 {
					t.Fatalf(
						"%s all-ineligible row cell %q = (%v, %d), want null transport",
						function,
						rows[0].names[index],
						rows[0].values[index],
						present,
					)
				}
			}

			rank := rows[1]
			for label := 'a'; label <= 'j'; label++ {
				chartNumericRequireCell(t, rank, "0:"+string(label), true, 100)
			}
			chartNumericRequireCell(t, rank, "1:", true, 7)
			chartNumericRequireCell(t, rank, "2:", true, 1)
			for _, excluded := range []string{"0:k", "0:l"} {
				for _, name := range rank.names {
					if name == excluded {
						t.Fatalf("%s overflow label %q escaped OTHER: %#v", function, excluded, rank.names)
					}
				}
			}

			// k finalizes to 100 and l finalizes to 1. Averaging those cells
			// would produce 50.5; pooling k's one member with l's twenty ones
			// first produces exactly 1 for both p95 and perc50 on pinned GK.
			zero := rows[2]
			chartNumericRequireCell(t, zero, "0:a", true, 0)
			for index, name := range zero.names {
				if name == "0:a" {
					continue
				}
				if zero.present[index] != 0 || zero.values[index] != 0 {
					t.Fatalf(
						"%s zero-row cell %q = (%v, %d), want null transport",
						function,
						name,
						zero.values[index],
						zero.present[index],
					)
				}
			}
		})
	}

	t.Run("raw sketch aggregation fails before top-ten collapse", func(t *testing.T) {
		compiled := compile(t, "p95")
		// The fixture has sixteen raw (row, label) sketch groups. Later
		// aggregates have at most fifteen groups, so this cap specifically
		// proves the raw sketch stage is bounded rather than only its final
		// twelve-column pivot.
		starved := clickhousedriver.Context(ctx, clickhousedriver.WithSettings(map[string]any{
			"max_rows_to_group_by":   uint64(15),
			"group_by_overflow_mode": "throw",
		}))
		var rowCount uint64
		err := connection.QueryRow(
			starved,
			"SELECT count() FROM ("+compiled.SQL+")",
			compiled.Args...,
		).Scan(&rowCount)
		if err == nil || !strings.Contains(err.Error(), "GROUP BY") {
			t.Fatalf("starved percentile chart error = %v, want raw GROUP BY overflow", err)
		}
	})

	t.Run("invalid split survives missing row and ineligible measure", func(t *testing.T) {
		compiled := chartEdgeCompile(
			t,
			`index=chartedge source="chart-percentile-invalid" | chart p95(metric) OVER path BY series`,
			indexTime.Add(time.Minute),
			visibilityCutoff,
		)
		rows, err := connection.Query(ctx, compiled.SQL, compiled.Args...)
		if err != nil {
			t.Fatalf("execute invalid percentile chart: %v\nSQL: %s", err, compiled.SQL)
		}
		defer func() {
			if closeErr := rows.Close(); closeErr != nil && !t.Failed() {
				t.Errorf("close invalid percentile chart rows: %v", closeErr)
			}
		}()
		if !rows.Next() {
			t.Fatalf("invalid percentile chart returned no sentinel: %v", rows.Err())
		}
		var (
			ordinal uint64
			row     string
			names   []string
			values  []float64
			present []uint8
			invalid uint8
		)
		if err := rows.Scan(&ordinal, &row, &names, &values, &present, &invalid); err != nil {
			t.Fatalf("scan percentile validation sentinel: %v", err)
		}
		if ordinal != 0 || row != "" || len(names) != 0 || len(values) != 0 ||
			len(present) != 0 || invalid == 0 {
			t.Fatalf(
				"percentile validation sentinel = ordinal %d row %q names %#v values %#v presence %#v invalid %d",
				ordinal,
				row,
				names,
				values,
				present,
				invalid,
			)
		}
		if rows.Next() {
			t.Fatal("invalid percentile chart returned a public row after its sentinel")
		}
		if err := rows.Err(); err != nil {
			t.Fatalf("iterate percentile validation sentinel: %v", err)
		}
	})
}

func chartPercentileStoreFixture(
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
			"chart-percentile",
			chartEdgeBase,
			&info,
			fields...,
		))
	}
	label := func(value string) *string { return &value }

	// Eleven labels tie at 100. The lexical first ten survive, so k joins
	// l's twenty ones in OTHER. The pooled 21-member p50 and p95 are both 1.
	for value := 'a'; value <= 'k'; value++ {
		add(
			"percentile-rank-"+string(value),
			"/rank",
			label(string(value)),
			typedDouble(100),
		)
	}
	for index := range 20 {
		add(
			fmt.Sprintf("percentile-volume-%02d", index),
			"/rank",
			label("l"),
			typedDouble(1),
		)
	}
	add("percentile-null-series", "/rank", nil, typedDouble(7))

	// One eligible zero mixed with poison remains present. A neighboring label
	// with only poison is null rather than a present zero.
	add("percentile-zero", "/zero", label("a"), typedDouble(0))
	add("percentile-zero-text", "/zero", label("a"), typedString("not-numeric"))
	add("percentile-zero-bool", "/zero", label("a"), typedBool(true))
	add("percentile-zero-null", "/zero", label("a"), typedNull())
	add("percentile-zero-missing", "/zero", label("a"), nil)
	add("percentile-null-text", "/zero", label("b"), typedString("still-not-numeric"))
	add("percentile-null-bool", "/zero", label("b"), typedBool(false))
	add("percentile-null-explicit", "/zero", label("b"), typedNull())
	add("percentile-null-missing", "/zero", label("b"), nil)

	// This row has no eligible percentile member but must retain its row axis.
	add("percentile-empty-text", "/empty", label("c"), typedString("never-numeric"))
	add("percentile-empty-bool", "/empty", label("c"), typedBool(true))
	add("percentile-empty-null", "/empty", label("c"), typedNull())
	add("percentile-empty-missing", "/empty", label("c"), nil)

	// Invalid split validation is independent of both row and measure
	// eligibility: this event has neither a row field nor a numeric measure.
	events = append(events, chartEdgeEvent(
		"percentile-invalid-empty-row",
		"chart-percentile-invalid",
		chartEdgeBase,
		&info,
		typedField("series", typedString("OTHER")),
		typedField("metric", typedString("not-numeric")),
	))

	chartEdgeStoreBatches(t, ctx, store, indexTime, events)
	cutoff, err := store.VisibilityCutoff(ctx)
	if err != nil {
		t.Fatalf("capture percentile chart visibility cutoff: %v", err)
	}
	return cutoff
}
