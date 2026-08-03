package queryexec

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	clickhousedriver "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
	"github.com/Suhaibinator/open-splunk/internal/indexread"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
)

// queryIntegrationTestSplitPercentileTimechart reuses the split-numeric
// fixture to pin the production percentile transport against ClickHouse. Its
// selected series are ranked by their finalized per-bucket percentiles, while
// OTHER must merge the omitted GK states before finalization.
func queryIntegrationTestSplitPercentileTimechart(
	t *testing.T,
	ctx context.Context,
	connection clickhousedriver.Conn,
	explainer *Explainer,
	base time.Time,
	indexTime time.Time,
) {
	t.Helper()

	executor, err := New(connection, Config{
		ReadAdmission: indexread.UnfencedAdmission{},
	})
	if err != nil {
		t.Fatalf("create split percentile executor: %v", err)
	}

	earliest := base
	latest := base.Add(15 * time.Minute)
	wantNames := []string{
		"_time", "a", "b", "c", "d", "e", "f", "g", "h", "i",
		"spike", "NULL", "OTHER",
	}
	type percentileCase struct {
		name     string
		function string
		want     map[string][]*float64
	}
	cases := []percentileCase{
		{
			name: "p95", function: "p95",
			want: map[string][]*float64{
				"a":     splitNumericValues(10, 100, nil),
				"b":     splitNumericValues(90, 7, nil),
				"c":     splitNumericValues(80, nil, nil),
				"d":     splitNumericValues(70, nil, nil),
				"e":     splitNumericValues(60, nil, nil),
				"f":     splitNumericValues(50, nil, nil),
				"g":     splitNumericValues(40, nil, nil),
				"h":     splitNumericValues(35, nil, nil),
				"i":     splitNumericValues(30, nil, nil),
				"spike": splitNumericValues(1_000, nil, nil),
				"NULL":  splitNumericValues(7, nil, nil),
				"OTHER": splitNumericValues(1, nil, nil),
			},
		},
		{
			name: "perc50", function: "perc50",
			want: map[string][]*float64{
				"a":     splitNumericValues(5, 100, nil),
				"b":     splitNumericValues(90, -7, nil),
				"c":     splitNumericValues(80, nil, nil),
				"d":     splitNumericValues(70, nil, nil),
				"e":     splitNumericValues(60, nil, nil),
				"f":     splitNumericValues(50, nil, nil),
				"g":     splitNumericValues(40, nil, nil),
				"h":     splitNumericValues(35, nil, nil),
				"i":     splitNumericValues(30, nil, nil),
				"spike": splitNumericValues(1_000, nil, nil),
				"NULL":  splitNumericValues(3, nil, nil),
				"OTHER": splitNumericValues(1, nil, nil),
			},
		},
	}

	queryLogPrefix := "open-splunk-timechart-split-percentile-" +
		strconv.FormatInt(time.Now().UnixNano(), 10) + "-"
	wantQueryIDs := make(map[string]struct{}, len(cases))
	for _, test := range cases {
		t.Run(test.name+" exact ranking merged other and physical shape", func(t *testing.T) {
			source := `index=main source="timechart-numeric-split" | timechart span=5m ` +
				test.function + `(metric) AS ignored_alias BY path`
			compiled := queryIntegrationCompileSearchRange(
				t,
				source,
				indexTime,
				earliest,
				latest,
			)
			if compiled.Timechart == nil ||
				compiled.Timechart.Mode != clickhouse.TimechartModeRuntimeWideValue ||
				compiled.Timechart.ValueKind != clickhouse.TimechartValueKindPercentile ||
				compiled.Timechart.ValueField != "" ||
				compiled.Timechart.BucketCount != 3 ||
				compiled.Timechart.MaxSeries != 12 ||
				compiled.Timechart.MaxLabelBytes != clickhouse.MaximumTimechartLabelBytes {
				t.Fatalf("compiled split %s percentile contract = %#v", test.name, compiled.Timechart)
			}
			upperSQL := strings.ToUpper(compiled.SQL)
			if strings.Count(compiled.SQL, `FROM "open_splunk"."events"`) != 1 ||
				strings.Contains(upperSQL, "ARRAY JOIN") {
				t.Fatalf("split %s percentile rescans or expands source rows:\n%s", test.name, compiled.SQL)
			}
			if strings.Count(compiled.SQL, "quantilesGKOrNullArrayState(") != 1 ||
				strings.Count(compiled.SQL, "quantilesGKOrNullArrayMerge(") != 1 {
				t.Fatalf("split %s percentile does not share one mergeable state:\n%s", test.name, compiled.SQL)
			}
			settings := executor.settingsFor(compiled)
			if got := settings["max_rows_to_group_by"]; got != maximumRuntimeWidePercentileGroups {
				t.Fatalf(
					"split %s percentile group cap = %v, want %d",
					test.name,
					got,
					maximumRuntimeWidePercentileGroups,
				)
			}
			if got := settings["group_by_overflow_mode"]; got != "throw" {
				t.Fatalf("split %s percentile overflow mode = %v, want throw", test.name, got)
			}

			explained, err := explainer.Explain(ctx, compiled)
			if err != nil {
				t.Fatalf("EXPLAIN split %s percentile: %v\nSQL: %s\nargs: %#v", test.name, err, compiled.SQL, compiled.Args)
			}
			physical := queryIntegrationAssertStructuredExplain(t, explained)
			if len(physical.Reads) != 1 || queryIntegrationPlanContainsArrayJoin(physical) {
				t.Fatalf("split %s percentile physical plan rescans or expands rows: %#v", test.name, physical)
			}
			actions := queryIntegrationExplainActions(t, ctx, connection, compiled)
			if strings.Contains(actions, "ArrayJoin") {
				t.Fatalf("split %s percentile EXPLAIN actions expand rows:\n%s", test.name, actions)
			}

			queryID := queryLogPrefix + test.name
			wantQueryIDs[queryID] = struct{}{}
			issued := 0
			executor.newQueryID = func() (string, error) {
				issued++
				if issued > 1 {
					return "", fmt.Errorf("split %s percentile requested too many query IDs", test.name)
				}
				return queryID, nil
			}
			t.Cleanup(func() { executor.newQueryID = randomQueryID })
			job, page := queryIntegrationRunSearchRange(
				t,
				ctx,
				executor,
				indexTime,
				"queryexec-timechart-split-percentile-"+test.name,
				source,
				earliest,
				latest,
			)
			if issued != 1 {
				t.Fatalf("split %s percentile query IDs = %d, want 1", test.name, issued)
			}
			if job.State != searchjobs.StateCompleted {
				t.Fatalf("split %s percentile state = %v, failure=%#v", test.name, job.State, job.Failure)
			}
			// Exact score ranking retains i over the tied j, and excludes the
			// higher-volume but low-percentile volume series. The runtime schema
			// order is canonical label order after selection.
			queryIntegrationAssertSplitNumericSchema(t, page, wantNames)
			queryIntegrationAssertSplitNumericMatrix(t, page, base, test.want)
			// Omitted j finalizes to 30 and volume finalizes to 1. Their average
			// is 15.5, while merging all twenty-one omitted members first yields
			// 1 for both tested percentiles.
			otherColumn := queryIntegrationColumnIndex(t, page, "OTHER")
			queryIntegrationAssertDouble(
				t,
				page.Rows[0].Values[otherColumn],
				1,
				"split "+test.name+" merged OTHER percentile",
			)
		})
	}

	for _, test := range cases {
		t.Run(test.name+" all-ineligible input retains nullable grid", func(t *testing.T) {
			job, page := queryIntegrationRunSearchRange(
				t,
				ctx,
				executor,
				indexTime,
				"queryexec-timechart-split-percentile-ineligible-"+test.name,
				`index=main source="timechart-numeric-split-ineligible" | timechart span=5m `+
					test.function+`(metric) BY path`,
				earliest,
				latest,
			)
			if job.State != searchjobs.StateCompleted {
				t.Fatalf("split %s percentile all-ineligible state = %v, failure=%#v", test.name, job.State, job.Failure)
			}
			queryIntegrationAssertSplitNumericSchema(t, page, []string{"_time", "bad"})
			queryIntegrationAssertSplitNumericMatrix(
				t,
				page,
				base,
				map[string][]*float64{"bad": splitNumericValues(nil, nil, nil)},
			)
		})

		t.Run(test.name+" empty input publishes runtime schema only", func(t *testing.T) {
			job, page := queryIntegrationRunSearchRange(
				t,
				ctx,
				executor,
				indexTime,
				"queryexec-timechart-split-percentile-empty-"+test.name,
				`index=main source="timechart-numeric-split-empty" | timechart span=5m `+
					test.function+`(metric) BY path`,
				earliest,
				latest,
			)
			if job.State != searchjobs.StateCompleted || len(page.Rows) != 0 ||
				len(page.Schema.Columns) != 1 ||
				page.Schema.Columns[0] != (searchjobs.Column{Name: "_time", Kind: searchjobs.ValueKindTime}) {
				t.Fatalf("split %s percentile empty job=%#v page=%#v", test.name, job, page)
			}
		})
	}

	t.Run("raw sketch budget fails atomically before top-series collapse", func(t *testing.T) {
		limited, err := New(connection, Config{
			MaxRowsToGroupBy: 7,
			ReadAdmission:    indexread.UnfencedAdmission{},
		})
		if err != nil {
			t.Fatalf("create limited split percentile executor: %v", err)
		}
		compiled := queryIntegrationCompileSearchRange(
			t,
			`index=main source="timechart-numeric-split" | timechart span=5m p95(metric) BY path`,
			indexTime,
			earliest,
			earliest.Add(5*time.Minute),
		)
		if got := limited.settingsFor(compiled)["max_rows_to_group_by"]; got != uint64(7) {
			t.Fatalf("explicit low split percentile group cap = %v, want 7", got)
		}
		sink := &fakeSink{}
		err = limited.Execute(ctx, compiled, sink)
		if !errors.Is(err, searchjobs.ErrExecutionLimit) ||
			sink.setCalls != 0 || len(sink.schema.Columns) != 0 || len(sink.rows) != 0 {
			t.Fatalf(
				"over-cap split percentile execution: err=%v schema calls=%d schema=%#v rows=%d",
				err,
				sink.setCalls,
				sink.schema,
				len(sink.rows),
			)
		}
	})

	t.Run("invalid split domain fails atomically", func(t *testing.T) {
		const source = `index=main source="timechart-numeric-split-invalid" | timechart span=5m p95(metric) BY path`
		compiled := queryIntegrationCompileSearchRange(t, source, indexTime, earliest, latest)
		sink := &fakeSink{}
		err := executor.Execute(ctx, compiled, sink)
		if !errors.Is(err, searchjobs.ErrUnsupportedValue) ||
			sink.setCalls != 0 || len(sink.schema.Columns) != 0 || len(sink.rows) != 0 {
			t.Fatalf(
				"invalid split percentile direct execution: err=%v schema calls=%d schema=%#v rows=%d",
				err,
				sink.setCalls,
				sink.schema,
				len(sink.rows),
			)
		}
		job, _ := queryIntegrationRunSearchRange(
			t,
			ctx,
			executor,
			indexTime,
			"queryexec-timechart-split-percentile-invalid",
			source,
			earliest,
			latest,
		)
		if job.State != searchjobs.StateFailed || job.Failure == nil ||
			job.Failure.Code != searchjobs.FailureUnsupportedSPL ||
			job.RowCount != 0 || job.Schema != nil {
			t.Fatalf("invalid split percentile manager job = %#v", job)
		}
	})

	if err := connection.Exec(ctx, "SYSTEM FLUSH LOGS"); err != nil {
		t.Fatalf("flush split percentile query log: %v", err)
	}
	rows, err := connection.Query(
		ctx,
		`SELECT query_id, query, toUInt64OrZero(Settings['max_rows_to_group_by'])
		FROM system.query_log
		WHERE type = 'QueryFinish' AND startsWith(query_id, ?)
		ORDER BY query_id`,
		queryLogPrefix,
	)
	if err != nil {
		t.Fatalf("read split percentile query log: %v", err)
	}
	defer rows.Close()
	seen := make(map[string]struct{}, len(wantQueryIDs))
	for rows.Next() {
		var queryID, query string
		var maximumGroups uint64
		if err := rows.Scan(&queryID, &query, &maximumGroups); err != nil {
			t.Fatalf("scan split percentile query log: %v", err)
		}
		if _, ok := wantQueryIDs[queryID]; !ok {
			t.Fatalf("unexpected split percentile query log row %q", queryID)
		}
		if _, duplicate := seen[queryID]; duplicate {
			t.Fatalf("duplicate split percentile query log row %q", queryID)
		}
		seen[queryID] = struct{}{}
		if strings.Count(query, `FROM "open_splunk"."events"`) != 1 ||
			strings.Contains(strings.ToUpper(query), "ARRAY JOIN") {
			t.Fatalf("logged split percentile query %q rescans or expands rows:\n%s", queryID, query)
		}
		if maximumGroups != maximumRuntimeWidePercentileGroups {
			t.Fatalf(
				"logged split percentile query %q group cap = %d, want %d",
				queryID,
				maximumGroups,
				maximumRuntimeWidePercentileGroups,
			)
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate split percentile query log: %v", err)
	}
	if len(seen) != len(wantQueryIDs) {
		t.Fatalf("split percentile query log rows = %d, want %d", len(seen), len(wantQueryIDs))
	}
}
