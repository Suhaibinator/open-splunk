package clickhouse

import (
	"context"
	"math"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/ingest"
	"github.com/Suhaibinator/open-splunk/internal/testsupport"
)

// TestStatsNumericMomentsAgainstClickHouse is opt-in because it starts the
// repository's digest-pinned ClickHouse image and applies the real schema.
func TestStatsNumericMomentsAgainstClickHouse(t *testing.T) {
	if os.Getenv("OPEN_SPLUNK_CLICKHOUSE_INTEGRATION") != "1" {
		t.Skip("set OPEN_SPLUNK_CLICKHOUSE_INTEGRATION=1 to run the Docker integration test")
	}
	if _, err := exec.LookPath("docker"); err != nil {
		t.Skipf("docker CLI is unavailable: %v", err)
	}
	image, err := testsupport.ResolvePinnedClickHouseImage(
		os.Getenv("OPEN_SPLUNK_CLICKHOUSE_TEST_IMAGE"),
	)
	if err != nil {
		t.Fatalf("resolve pinned ClickHouse image: %v", err)
	}
	t.Logf("ClickHouse image: %s", image)

	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()
	connection, store := chartEdgeStartClickHouse(t, ctx)
	indexTime := time.Date(2026, time.August, 11, 18, 0, 0, 0, time.UTC)
	newEvent := func(
		id string,
		cohort string,
		metric *opensplunkv1.TypedValue,
	) *ingest.StoredEvent {
		fields := []*opensplunkv1.TypedObjectField{
			typedField("cohort", typedString(cohort)),
		}
		if metric != nil {
			fields = append(fields, typedField("metric", metric))
		}
		event := testStoredEvent(id, "stats-numeric", indexTime)
		event.Event.Source = "stats-numeric"
		event.Event.Fields = typedObjectValue(fields...)
		return event
	}
	events := []*ingest.StoredEvent{
		newEvent("mixed-int", "mixed", typedSint(1)),
		newEvent("mixed-string", "mixed", typedString("2")),
		newEvent("mixed-list", "mixed", typedList(
			typedSint(3),
			typedString("4"),
			typedString("bad"),
			typedNull(),
		)),
		newEvent("mixed-boolean", "mixed", typedBool(true)),
		newEvent("mixed-missing", "mixed", nil),
		newEvent("singleton", "singleton", typedSint(7)),
		newEvent("invalid-list", "invalid", typedList(
			typedString("bad"),
			typedNull(),
			typedBool(false),
		)),
		newEvent("invalid-missing", "invalid", nil),
		newEvent("empty-numeric", "empty", typedSint(5)),
		newEvent("empty-string", "empty", typedString("")),
	}
	compile, queryContext := storeScalarFunctionIntegrationFixtures(
		ctx,
		t,
		store,
		indexTime,
		"stats-numeric",
		"stats-numeric-batch",
		351,
		events...,
	)

	const measures = `range(metric) AS span sumsq(metric) AS squared ` +
		`stdev(metric) AS sample_sd stdevp(metric) AS population_sd ` +
		`var(metric) AS sample_variance varp(metric) AS population_variance`
	grouped := compile(
		`index=stats-numeric source="stats-numeric" | stats ` +
			measures + ` BY cohort | sort cohort`,
	)
	if got := strings.Count(grouped.SQL, `FROM "open_splunk"."events"`); got != 1 {
		t.Fatalf("numeric stats storage scans = %d, want 1:\n%s", got, grouped.SQL)
	}
	// Raw Dynamic BY fields now deliberately use ARRAY JOIN so multivalue
	// members produce independent groups. Keep the physical no-row-expansion
	// assertion scoped to the numeric measure path itself by explaining the
	// same measures without a BY expansion.
	physical := compile(
		`index=stats-numeric source="stats-numeric" | stats ` + measures,
	)
	actions := explainCompiledQuery(
		t,
		queryContext,
		connection,
		"EXPLAIN actions=1 ",
		physical,
	)
	assertStatsNumericPhysicalPlanHasNoArrayJoin(t, actions)

	rows, queryErr := connection.Query(queryContext, grouped.SQL, grouped.Args...)
	if queryErr != nil {
		t.Fatalf("execute grouped numeric stats: %v\nSQL: %s\nargs: %#v", queryErr, grouped.SQL, grouped.Args)
	}
	columnTypes := rows.ColumnTypes()
	if len(columnTypes) != 7 {
		_ = rows.Close()
		t.Fatalf("grouped numeric stats column types = %#v", columnTypes)
	}
	for _, columnType := range columnTypes[1:] {
		if columnType.DatabaseTypeName() != "Nullable(Float64)" {
			_ = rows.Close()
			t.Fatalf("grouped numeric stats column types = %#v", columnTypes)
		}
	}

	type numericMoments struct {
		span               *float64
		squared            *float64
		sampleSD           *float64
		populationSD       *float64
		sampleVariance     *float64
		populationVariance *float64
	}
	want := map[string]numericMoments{
		"empty": {
			span:               new(float64(0)),
			squared:            new(float64(25)),
			sampleSD:           new(float64(0)),
			populationSD:       new(float64(0)),
			sampleVariance:     new(float64(0)),
			populationVariance: new(float64(0)),
		},
		"invalid": {},
		"mixed": {
			span:               new(float64(3)),
			squared:            new(float64(30)),
			sampleSD:           new(math.Sqrt(5.0 / 3.0)),
			populationSD:       new(math.Sqrt(1.25)),
			sampleVariance:     new(5.0 / 3.0),
			populationVariance: new(1.25),
		},
		"singleton": {
			span:               new(float64(0)),
			squared:            new(float64(49)),
			sampleSD:           new(float64(0)),
			populationSD:       new(float64(0)),
			sampleVariance:     new(float64(0)),
			populationVariance: new(float64(0)),
		},
	}
	seen := make(map[string]struct{}, len(want))
	for rows.Next() {
		var cohort string
		var got numericMoments
		if scanErr := rows.Scan(
			&cohort,
			&got.span,
			&got.squared,
			&got.sampleSD,
			&got.populationSD,
			&got.sampleVariance,
			&got.populationVariance,
		); scanErr != nil {
			_ = rows.Close()
			t.Fatalf("scan grouped numeric stats: %v", scanErr)
		}
		expected, ok := want[cohort]
		if !ok {
			_ = rows.Close()
			t.Fatalf("unexpected numeric stats cohort %q", cohort)
		}
		seen[cohort] = struct{}{}
		assertNullableFloatApprox(t, cohort+" range", got.span, expected.span)
		assertNullableFloatApprox(t, cohort+" sumsq", got.squared, expected.squared)
		assertNullableFloatApprox(t, cohort+" stdev", got.sampleSD, expected.sampleSD)
		assertNullableFloatApprox(t, cohort+" stdevp", got.populationSD, expected.populationSD)
		assertNullableFloatApprox(t, cohort+" var", got.sampleVariance, expected.sampleVariance)
		assertNullableFloatApprox(t, cohort+" varp", got.populationVariance, expected.populationVariance)
	}
	if rowsErr := rows.Err(); rowsErr != nil {
		_ = rows.Close()
		t.Fatalf("iterate grouped numeric stats: %v", rowsErr)
	}
	if closeErr := rows.Close(); closeErr != nil {
		t.Fatalf("close grouped numeric stats: %v", closeErr)
	}
	if len(seen) != len(want) {
		t.Fatalf("numeric stats cohorts = %v, want %v", seen, want)
	}

	allNumeric := compile(
		`index=stats-numeric source="stats-numeric" | stats allnum=true ` +
			`avg(metric) AS average range(metric) AS span BY cohort | sort cohort`,
	)
	allNumericRows, queryErr := connection.Query(
		queryContext,
		allNumeric.SQL,
		allNumeric.Args...,
	)
	if queryErr != nil {
		t.Fatalf(
			"execute allnum numeric stats: %v\nSQL: %s\nargs: %#v",
			queryErr,
			allNumeric.SQL,
			allNumeric.Args,
		)
	}
	type allNumericResult struct {
		average *float64
		span    *float64
	}
	wantAllNumeric := map[string]allNumericResult{
		"empty":   {},
		"invalid": {},
		"mixed":   {},
		"singleton": {
			average: new(float64(7)),
			span:    new(float64(0)),
		},
	}
	seenAllNumeric := make(map[string]struct{}, len(wantAllNumeric))
	for allNumericRows.Next() {
		var cohort string
		var average, span *float64
		if scanErr := allNumericRows.Scan(&cohort, &average, &span); scanErr != nil {
			_ = allNumericRows.Close()
			t.Fatalf("scan allnum numeric stats: %v", scanErr)
		}
		expected, ok := wantAllNumeric[cohort]
		if !ok {
			_ = allNumericRows.Close()
			t.Fatalf("unexpected allnum cohort %q", cohort)
		}
		seenAllNumeric[cohort] = struct{}{}
		assertNullableFloatApprox(t, cohort+" allnum average", average, expected.average)
		assertNullableFloatApprox(t, cohort+" allnum range", span, expected.span)
	}
	if rowsErr := allNumericRows.Err(); rowsErr != nil {
		_ = allNumericRows.Close()
		t.Fatalf("iterate allnum numeric stats: %v", rowsErr)
	}
	if closeErr := allNumericRows.Close(); closeErr != nil {
		t.Fatalf("close allnum numeric stats: %v", closeErr)
	}
	if len(seenAllNumeric) != len(wantAllNumeric) {
		t.Fatalf("allnum cohorts = %v, want %v", seenAllNumeric, wantAllNumeric)
	}

	empty := compile(`index=stats-numeric source="absent" | stats ` + measures)
	var emptyValues numericMoments
	if queryErr := connection.QueryRow(
		queryContext,
		empty.SQL,
		empty.Args...,
	).Scan(
		&emptyValues.span,
		&emptyValues.squared,
		&emptyValues.sampleSD,
		&emptyValues.populationSD,
		&emptyValues.sampleVariance,
		&emptyValues.populationVariance,
	); queryErr != nil {
		t.Fatalf("execute empty numeric stats: %v\nSQL: %s\nargs: %#v", queryErr, empty.SQL, empty.Args)
	}
	for name, value := range map[string]*float64{
		"range":  emptyValues.span,
		"sumsq":  emptyValues.squared,
		"stdev":  emptyValues.sampleSD,
		"stdevp": emptyValues.populationSD,
		"var":    emptyValues.sampleVariance,
		"varp":   emptyValues.populationVariance,
	} {
		assertNullableFloatApprox(t, "empty "+name, value, nil)
	}

	evaluated := compile(
		`index=stats-numeric source="stats-numeric" cohort="singleton" | stats ` +
			`sumsq(eval(metric+1)) AS shifted_sumsq ` +
			`stdev(eval(metric+1)) AS shifted_stdev`,
	)
	var shiftedSumSquares, shiftedSampleSD *float64
	if queryErr := connection.QueryRow(
		queryContext,
		evaluated.SQL,
		evaluated.Args...,
	).Scan(&shiftedSumSquares, &shiftedSampleSD); queryErr != nil {
		t.Fatalf("execute evaluated numeric stats: %v\nSQL: %s\nargs: %#v", queryErr, evaluated.SQL, evaluated.Args)
	}
	assertNullableFloatApprox(
		t,
		"evaluated singleton sumsq",
		shiftedSumSquares,
		new(float64(64)),
	)
	assertNullableFloatApprox(
		t,
		"evaluated singleton stdev",
		shiftedSampleSD,
		new(float64(0)),
	)
}

func assertNullableFloatApprox(
	t *testing.T,
	name string,
	got *float64,
	want *float64,
) {
	t.Helper()
	if want == nil {
		if got != nil {
			t.Fatalf("%s = %v, want null", name, *got)
		}
		return
	}
	if got == nil || math.IsNaN(*got) || math.IsInf(*got, 0) ||
		math.Abs(*got-*want) > 1e-12 {
		t.Fatalf("%s = %v, want %v", name, got, *want)
	}
}

func assertStatsNumericPhysicalPlanHasNoArrayJoin(
	t *testing.T,
	actions string,
) {
	t.Helper()
	if strings.Contains(actions, "ArrayJoin") {
		t.Fatalf("numeric stats physical plan expands event rows:\n%s", actions)
	}
}
