package clickhouse

import (
	"regexp"
	"slices"
	"strings"
	"testing"
)

func TestCompileAdjacentDynamicChronologicalAggregatesShareBoundedStages(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(
		t,
		`index=gradethis`+
			` | eventstats earliest(payload) AS event_first`+
			` | eventstats latest(other_payload) AS event_last`+
			` | sort 0 +event_id`+
			` | streamstats earliest(third_payload) AS stream_first`+
			` | streamstats latest(payload) AS stream_last`+
			` | table event_id event_first event_last stream_first stream_last`,
	)
	wantOutputs := []string{
		"event_id",
		"event_first",
		"event_last",
		"stream_first",
		"stream_last",
	}
	if !slices.Equal(compiled.OutputFields, wantOutputs) {
		t.Fatalf("fused chronological outputs = %#v, want %#v", compiled.OutputFields, wantOutputs)
	}
	for _, fragment := range []string{
		`__os_eventstats_fused_source_`,
		`__os_streamstats_fused_source_`,
		`__os_eventstats_result_input_`,
		`__os_streamstats_result_input_`,
		UnsupportedStatsMeasureValueMarker,
	} {
		if !strings.Contains(compiled.SQL, fragment) {
			t.Fatalf("fused chronological SQL is missing %q:\n%s", fragment, compiled.SQL)
		}
	}
	for _, pattern := range []struct {
		name string
		re   *regexp.Regexp
		want int
	}{
		{
			name: "event result input",
			re: regexp.MustCompile(
				`"__os_eventstats_result_input_[0-9]+" AS (?:MATERIALIZED )?\(`,
			),
			want: 1,
		},
		{
			name: "stream result input",
			re: regexp.MustCompile(
				`"__os_streamstats_result_input_[0-9]+" AS (?:MATERIALIZED )?\(`,
			),
			want: 1,
		},
		{
			name: "event validation definitions",
			re: regexp.MustCompile(
				` AS "__os_eventstats_validation_[0-9]+"`,
			),
			want: 2,
		},
		{
			name: "stream validation definitions",
			re: regexp.MustCompile(
				` AS "__os_streamstats_validation_[0-9]+"`,
			),
			want: 2,
		},
	} {
		if got := len(pattern.re.FindAllString(compiled.SQL, -1)); got != pattern.want {
			t.Fatalf("fused chronological %s = %d, want %d", pattern.name, got, pattern.want)
		}
	}
	if got := strings.Count(compiled.SQL, "argMinOrNullIf("); got != 2 {
		t.Fatalf("fused chronological earliest states = %d, want 2", got)
	}
	if got := strings.Count(compiled.SQL, "argMaxOrNullIf("); got != 2 {
		t.Fatalf("fused chronological latest states = %d, want 2", got)
	}
	if got := strings.Count(compiled.SQL, `FROM "open_splunk"."events"`); got != 1 {
		t.Fatalf("fused chronological physical scans = %d, want 1", got)
	}
	if got, want := strings.Count(compiled.SQL, "?"), len(compiled.Args); got != want {
		t.Fatalf("fused chronological placeholders = %d, args = %d", got, want)
	}
	finalInput := regexp.MustCompile(
		`"(__os_chronological_final_input_[0-9]+)" AS (?:MATERIALIZED )?\(`,
	).FindStringSubmatch(compiled.SQL)
	if len(finalInput) != 2 {
		t.Fatalf("fused chronological final input is missing")
	}
	if got := strings.Count(compiled.SQL, `FROM "`+finalInput[1]+`" AS `); got != 1 {
		t.Fatalf("fused chronological final input consumers = %d, want one", got)
	}
	for _, typedDummy := range []string{
		`CAST('' AS String) AS "event_id"`,
		`CAST(NULL AS Dynamic) AS "event_first"`,
		`CAST(NULL AS Dynamic) AS "event_last"`,
		`CAST(NULL AS Dynamic) AS "stream_first"`,
		`CAST(NULL AS Dynamic) AS "stream_last"`,
	} {
		if strings.Count(compiled.SQL, typedDummy) != 1 {
			t.Fatalf("fused chronological typed invalid row is missing %q", typedDummy)
		}
	}
}

func TestCompileFusedChronologicalValidationSurvivesEmptyConsumer(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(
		t,
		`index=gradethis`+
			` | eventstats earliest(payload) AS event_first`+
			` | eventstats latest(other_payload) AS event_last`+
			` | streamstats earliest(third_payload) AS stream_first`+
			` | streamstats latest(payload) AS stream_last`+
			` | search definitely_missing=value`+
			` | table event_id`,
	)
	if strings.Count(compiled.SQL, "UNION ALL") != 1 ||
		strings.LastIndex(compiled.SQL, "UNION ALL") < strings.LastIndex(compiled.SQL, "WHERE 0") {
		t.Fatalf("empty consumer can prune fused chronological validation:\n%s", compiled.SQL)
	}
	for _, class := range []string{"eventstats", "streamstats"} {
		validation := regexp.MustCompile(`"__os_` + class + `_validation_[0-9]+" != 0`)
		if got := len(validation.FindAllString(compiled.SQL, -1)); got != 2 {
			t.Fatalf("fused %s validation consumers = %d, want 2", class, got)
		}
	}
}

func TestCompileChronologicalFusionRequiresIndependentIdenticalStages(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name   string
		source string
		marker string
	}{
		{
			name: "event dependency",
			source: `index=gradethis` +
				` | eventstats earliest(payload) AS first_seen` +
				` | eventstats latest(first_seen) AS last_seen` +
				` | table first_seen last_seen`,
			marker: `__os_eventstats_fused_source_`,
		},
		{
			name: "event output replaces sibling input",
			source: `index=gradethis` +
				` | eventstats earliest(payload) AS other_payload` +
				` | eventstats latest(other_payload) AS last_seen` +
				` | table other_payload last_seen`,
			marker: `__os_eventstats_fused_source_`,
		},
		{
			name: "stream frame mismatch",
			source: `index=gradethis` +
				` | streamstats window=2 earliest(payload) AS first_seen` +
				` | streamstats window=3 latest(other_payload) AS last_seen` +
				` | table first_seen last_seen`,
			marker: `__os_streamstats_fused_source_`,
		},
		{
			name: "stream dependency",
			source: `index=gradethis` +
				` | streamstats earliest(payload) AS first_seen` +
				` | streamstats latest(first_seen) AS last_seen` +
				` | table first_seen last_seen`,
			marker: `__os_streamstats_fused_source_`,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			compiled := compileSPL(t, test.source)
			if strings.Contains(compiled.SQL, test.marker) {
				t.Fatalf("unsafe chronological stages used fusion marker %q", test.marker)
			}
		})
	}
}
