package plan

import (
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/eventfields"
	"github.com/Suhaibinator/open-splunk/internal/spl"
)

func TestBuildIntersectsTrustedIndexScope(t *testing.T) {
	t.Parallel()

	query := mustParse(t, `index=gradethis trace_id="abc" | sort -_time | table _time level message | head 20`)
	logical, err := Build(query, testScope([]string{"internal", "gradethis"}, []string{"gradethis"}))
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	if !slices.Equal(logical.EffectiveIndexes, []string{"gradethis"}) {
		t.Fatalf("effective indexes = %v", logical.EffectiveIndexes)
	}
	if len(logical.Operators) != 5 {
		t.Fatalf("operator count = %d, want 5", len(logical.Operators))
	}
	scan := logical.Operators[0].(*Scan)
	if scan.TenantID != "tenant-1" || !slices.Equal(scan.Indexes, []string{"gradethis"}) {
		t.Fatalf("scan = %#v", scan)
	}
	if scan.VisibilityCutoff != 41 {
		t.Fatalf("visibility cutoff = %d, want 41", scan.VisibilityCutoff)
	}
	if !scan.Earliest.Equal(time.Date(2026, 7, 21, 8, 0, 0, 0, time.UTC)) || !scan.Latest.Equal(time.Date(2026, 7, 21, 9, 0, 0, 0, time.UTC)) {
		t.Fatalf("scan range = [%v, %v)", scan.Earliest, scan.Latest)
	}
	sortOp := logical.Operators[2].(*Sort)
	if len(sortOp.Keys) != 1 || sortOp.Keys[0].Field.Name != "_time" || !sortOp.Keys[0].Descending {
		t.Fatalf("sort = %#v", sortOp)
	}
	project := logical.Operators[3].(*Project)
	if project.Mode != ProjectModeTable || !slices.Equal(logical.OutputFields, []string{"_time", "level", "message"}) {
		t.Fatalf("project/output = %#v / %v", project, logical.OutputFields)
	}
}

func TestBuildRejectsExplicitForbiddenIndex(t *testing.T) {
	t.Parallel()

	tests := []string{
		`index=secret`,
		`index=gradethis OR index=secret`,
		`index=gradethis | search index=secret`,
	}
	for _, source := range tests {
		t.Run(source, func(t *testing.T) {
			t.Parallel()
			_, err := Build(mustParse(t, source), testScope([]string{"gradethis"}, nil))
			assertDiagnosticCode(t, err, "SPL_INDEX_FORBIDDEN")
		})
	}
}

func TestBuildDoesNotTreatNegatedIndexAsRequestedScope(t *testing.T) {
	t.Parallel()

	queries := []string{`NOT index=secret`, `index!=secret`}
	for _, source := range queries {
		logical, err := Build(mustParse(t, source), testScope([]string{"gradethis"}, nil))
		if err != nil {
			t.Fatalf("Build(%q): %v", source, err)
		}
		if !slices.Equal(logical.EffectiveIndexes, []string{"gradethis"}) {
			t.Fatalf("effective indexes = %v", logical.EffectiveIndexes)
		}
	}
}

func TestBuildRejectsWildcardIndexSelection(t *testing.T) {
	t.Parallel()

	_, err := Build(mustParse(t, `index=grade*`), testScope([]string{"gradethis"}, nil))
	assertDiagnosticCode(t, err, "SPL_UNSUPPORTED_INDEX_SELECTOR")
}

func TestBuildPreservesLiteralTypes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		source string
		kind   ValueKind
	}{
		{`status=500`, ValueKindInt64},
		{`counter=18446744073709551615`, ValueKindUint64},
		{`ratio=0.5`, ValueKindFloat64},
		{`ok=true`, ValueKindBool},
		{`value=null`, ValueKindNull},
		{`status="500"`, ValueKindString},
	}
	for _, test := range tests {
		t.Run(test.source, func(t *testing.T) {
			t.Parallel()
			logical, err := Build(mustParse(t, test.source), testScope([]string{"gradethis"}, nil))
			if err != nil {
				t.Fatalf("Build: %v", err)
			}
			filter := logical.Operators[1].(*Filter)
			comparison := filter.Expression.(*ComparisonExpression)
			if comparison.Value.Kind != test.kind {
				t.Fatalf("kind = %v, want %v", comparison.Value.Kind, test.kind)
			}
			if comparison.Value.SourceText == "" {
				t.Fatalf("source text was not retained: %#v", comparison.Value)
			}
		})
	}
}

func TestBuildAppliesSplunkDefaultSortLimitButHonorsSortZero(t *testing.T) {
	t.Parallel()

	defaulted, err := Build(mustParse(t, `* | sort -_time`), testScope([]string{"gradethis"}, nil))
	if err != nil {
		t.Fatalf("Build(default sort): %v", err)
	}
	if got := defaulted.Operators[2].(*Sort).Limit; got != 10_000 {
		t.Fatalf("default sort limit = %d, want 10000", got)
	}

	unlimited, err := Build(mustParse(t, `* | sort 0 -_time`), testScope([]string{"gradethis"}, nil))
	if err != nil {
		t.Fatalf("Build(sort 0): %v", err)
	}
	if got := unlimited.Operators[2].(*Sort).Limit; got != 0 {
		t.Fatalf("sort 0 limit = %d, want unlimited", got)
	}
}

func TestBuildDoesNotTreatSortBackslashAsAnEscapeForCanonicalTime(t *testing.T) {
	t.Parallel()

	_, err := Build(mustParse(t, `* | sort + \_time`), testScope([]string{"gradethis"}, nil))
	assertDiagnosticCode(t, err, "SPL_INVALID_FIELD")
}

func TestBuildDedupPreservesSchemaAndExactKeys(t *testing.T) {
	t.Parallel()

	logical, err := Build(
		mustParse(t, `index=gradethis | stats count BY service, status | dedup 2 service status`),
		testScope([]string{"gradethis"}, nil),
	)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	if !slices.Equal(logical.OutputFields, []string{"service", "status", "count"}) {
		t.Fatalf("output fields = %v, want schema preserved", logical.OutputFields)
	}
	dedup, ok := logical.Operators[len(logical.Operators)-1].(*Deduplicate)
	if !ok || dedup.Count != 2 || len(dedup.Keys) != 2 || dedup.Keys[0].Name != "service" || dedup.Keys[1].Name != "status" {
		t.Fatalf("last operator = %#v, want Deduplicate(2, service, status)", logical.Operators[len(logical.Operators)-1])
	}
}

func TestBuildDedupDoesNotHideDownstreamIndexScope(t *testing.T) {
	t.Parallel()

	_, err := Build(
		mustParse(t, `index=gradethis | dedup host | search index=secret`),
		testScope([]string{"gradethis"}, nil),
	)
	assertDiagnosticCode(t, err, "SPL_INDEX_FORBIDDEN")
}

func TestBuildDedupRejectsAmbiguousEventFieldsPayloadButAllowsClosedSchema(t *testing.T) {
	t.Parallel()

	_, err := Build(
		mustParse(t, `index=gradethis | dedup fields`),
		testScope([]string{"gradethis"}, nil),
	)
	assertDiagnosticCode(t, err, "SPL_AMBIGUOUS_DEDUP_FIELD")

	logical, err := Build(
		mustParse(t, `index=gradethis | stats count AS fields | dedup fields`),
		testScope([]string{"gradethis"}, nil),
	)
	if err != nil {
		t.Fatalf("Build(closed schema): %v", err)
	}
	if !slices.Equal(logical.OutputFields, []string{"fields"}) {
		t.Fatalf("closed output fields = %v, want [fields]", logical.OutputFields)
	}
	if _, ok := logical.Operators[len(logical.Operators)-1].(*Deduplicate); !ok {
		t.Fatalf("last operator = %T, want *Deduplicate", logical.Operators[len(logical.Operators)-1])
	}
}

func TestBuildStatsCountReplacesEventSchema(t *testing.T) {
	t.Parallel()

	logical, err := Build(
		mustParse(t, `index=gradethis | stats count AS events by host, status`),
		testScope([]string{"gradethis"}, nil),
	)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	if !slices.Equal(logical.OutputFields, []string{"host", "status", "events"}) {
		t.Fatalf("output fields = %v", logical.OutputFields)
	}
	aggregate, ok := logical.Operators[len(logical.Operators)-1].(*Aggregate)
	if !ok {
		t.Fatalf("last operator = %T, want *Aggregate", logical.Operators[len(logical.Operators)-1])
	}
	if len(aggregate.GroupBy) != 2 || aggregate.GroupBy[0].Name != "host" ||
		aggregate.GroupBy[1].Name != "status" || len(aggregate.Measures) != 1 ||
		aggregate.Measures[0].Function != AggregateFunctionCountRows || aggregate.Measures[0].Output != "events" {
		t.Fatalf("aggregate = %#v", aggregate)
	}
}

func TestBuildRenamePreservesSequentialSchemaEffects(t *testing.T) {
	t.Parallel()

	logical, err := Build(
		mustParse(t, `index=gradethis | stats count by logger | rename logger AS component, component AS subsystem`),
		testScope([]string{"gradethis"}, nil),
	)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	if !slices.Equal(logical.OutputFields, []string{"subsystem", "count"}) {
		t.Fatalf("output fields = %v, want [subsystem count]", logical.OutputFields)
	}
	rename, ok := logical.Operators[len(logical.Operators)-1].(*Rename)
	if !ok || len(rename.Assignments) != 2 {
		t.Fatalf("last operator = %#v, want two-assignment Rename", logical.Operators[len(logical.Operators)-1])
	}
	if rename.Assignments[0].Source.Name != "logger" || rename.Assignments[0].Destination.Name != "component" ||
		rename.Assignments[1].Source.Name != "component" || rename.Assignments[1].Destination.Name != "subsystem" {
		t.Fatalf("rename = %#v", rename)
	}
}

func TestBuildRenameSupportsDynamicFieldsAndOverwritesKnownTarget(t *testing.T) {
	t.Parallel()

	dynamic, err := Build(
		mustParse(t, `index=gradethis | rename logger AS component | where component="api" | table component`),
		testScope([]string{"gradethis"}, nil),
	)
	if err != nil {
		t.Fatalf("Build(dynamic): %v", err)
	}
	rename, ok := dynamic.Operators[2].(*Rename)
	if !ok || rename.Assignments[0].Source.Name != "logger" || rename.Assignments[0].Destination.Name != "component" {
		t.Fatalf("dynamic rename = %#v", dynamic.Operators[2])
	}

	overwrite, err := Build(
		mustParse(t, `index=gradethis | stats count by logger | rename logger AS count`),
		testScope([]string{"gradethis"}, nil),
	)
	if err != nil {
		t.Fatalf("Build(overwrite): %v", err)
	}
	if !slices.Equal(overwrite.OutputFields, []string{"count"}) {
		t.Fatalf("overwrite output fields = %v, want [count]", overwrite.OutputFields)
	}
}

func TestBuildRenameDoesNotTurnCalculatedIndexIntoSecurityScope(t *testing.T) {
	t.Parallel()

	logical, err := Build(
		mustParse(t, `index=gradethis | table path | rename path AS index | search index=secret`),
		testScope([]string{"gradethis"}, nil),
	)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	if !slices.Equal(logical.EffectiveIndexes, []string{"gradethis"}) {
		t.Fatalf("effective indexes = %v", logical.EffectiveIndexes)
	}
}

func TestBuildRenameInvalidatesCanonicalTimeForTimechart(t *testing.T) {
	t.Parallel()

	_, err := Build(
		mustParse(t, `index=gradethis | table _time, level | rename _time AS observed_at | timechart span=5m count by level`),
		testScope([]string{"gradethis"}, nil),
	)
	assertDiagnosticCode(t, err, "SPL_UNSUPPORTED_TIMECHART_TIME_FIELD")
}

func TestBuildRenameRejectsAmbiguousOpenSchemaFieldsAndNestedPaths(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		source string
		code   string
	}{
		{`index=gradethis | rename fields AS payload`, "SPL_AMBIGUOUS_RENAME_FIELD"},
		{`index=gradethis | rename logger AS fields`, "SPL_AMBIGUOUS_RENAME_FIELD"},
		{`index=gradethis | rename request.path AS route`, "SPL_UNSUPPORTED_RENAME_PATH"},
		{`index=gradethis | rename route AS request.path`, "SPL_UNSUPPORTED_RENAME_PATH"},
	} {
		_, err := Build(mustParse(t, test.source), testScope([]string{"gradethis"}, nil))
		assertDiagnosticCode(t, err, test.code)
	}

	// Once a transforming command establishes an exact schema, "fields" and
	// dotted aliases are ordinary declared columns rather than the open event
	// payload or unresolved dynamic paths.
	logical, err := Build(
		mustParse(t, `index=gradethis | stats count AS fields | rename fields AS request.path`),
		testScope([]string{"gradethis"}, nil),
	)
	if err != nil {
		t.Fatalf("Build(closed schema): %v", err)
	}
	if !slices.Equal(logical.OutputFields, []string{"request.path"}) {
		t.Fatalf("closed-schema output = %v", logical.OutputFields)
	}
}

func TestBuildStatsMultipleMeasuresWithP95(t *testing.T) {
	t.Parallel()

	logical, err := Build(
		mustParse(t, `index=gradethis | eval duration_ms=tonumber(replace(duration, "ms$", "")) | stats count p95(duration_ms) AS p95_ms BY path | where p95_ms>500`),
		testScope([]string{"gradethis"}, nil),
	)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	if !slices.Equal(logical.OutputFields, []string{"path", "count", "p95_ms"}) {
		t.Fatalf("output fields = %v", logical.OutputFields)
	}
	aggregate, ok := logical.Operators[len(logical.Operators)-2].(*Aggregate)
	if !ok || len(aggregate.Measures) != 2 {
		t.Fatalf("aggregate operator = %#v", logical.Operators[len(logical.Operators)-2])
	}
	count := aggregate.Measures[0]
	percentile := aggregate.Measures[1]
	if count.Function != AggregateFunctionCountRows || count.Output != "count" {
		t.Fatalf("count measure = %#v", count)
	}
	if percentile.Function != AggregateFunctionPercentile || percentile.Input.Name != "duration_ms" ||
		percentile.Percentile != 95 || percentile.Output != "p95_ms" {
		t.Fatalf("percentile measure = %#v", percentile)
	}
}

func TestBuildStatsCountValuesPreservesMeasureOrderAndAliases(t *testing.T) {
	t.Parallel()

	logical, err := Build(
		mustParse(t, `index=gradethis | stats count count(user) count(device) AS devices BY service`),
		testScope([]string{"gradethis"}, nil),
	)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	if !slices.Equal(logical.OutputFields, []string{"service", "count", "count(user)", "devices"}) {
		t.Fatalf("output fields = %v", logical.OutputFields)
	}
	aggregate, ok := logical.Operators[len(logical.Operators)-1].(*Aggregate)
	if !ok || len(aggregate.Measures) != 3 {
		t.Fatalf("aggregate operator = %#v", logical.Operators[len(logical.Operators)-1])
	}
	if rows := aggregate.Measures[0]; rows.Function != AggregateFunctionCountRows ||
		rows.Input.Name != "" || rows.Output != "count" {
		t.Fatalf("row count measure = %#v", rows)
	}
	for index, want := range []struct {
		input  string
		output string
	}{
		{input: "user", output: "count(user)"},
		{input: "device", output: "devices"},
	} {
		measure := aggregate.Measures[index+1]
		if measure.Function != AggregateFunctionCountValues ||
			measure.Input.Name != want.input || measure.Output != want.output {
			t.Fatalf("measure %d = %#v, want count(%s) AS %s", index+1, measure, want.input, want.output)
		}
	}
}

func TestBuildStatsCountFieldAbbreviationUsesValueCountPlan(t *testing.T) {
	t.Parallel()

	logical, err := Build(
		mustParse(t, `index=gradethis | stats c(user) c(device) AS devices BY service`),
		testScope([]string{"gradethis"}, nil),
	)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	if !slices.Equal(logical.OutputFields, []string{"service", "count(user)", "devices"}) {
		t.Fatalf("output fields = %v", logical.OutputFields)
	}
	aggregate, ok := logical.Operators[len(logical.Operators)-1].(*Aggregate)
	if !ok || len(aggregate.Measures) != 2 {
		t.Fatalf("aggregate operator = %#v", logical.Operators[len(logical.Operators)-1])
	}
	for index, want := range []struct {
		input  string
		output string
	}{
		{input: "user", output: "count(user)"},
		{input: "device", output: "devices"},
	} {
		measure := aggregate.Measures[index]
		if measure.Function != AggregateFunctionCountValues ||
			measure.Input.Name != want.input || measure.Output != want.output {
			t.Fatalf("measure %d = %#v, want count(%s) AS %s", index, measure, want.input, want.output)
		}
	}
}

func TestBuildStatsSumAndAveragePreserveMeasureOrder(t *testing.T) {
	t.Parallel()

	logical, err := Build(
		mustParse(t, `index=gradethis | stats count sum(amount) avg(latency) AS mean BY service, host`),
		testScope([]string{"gradethis"}, nil),
	)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	if !slices.Equal(logical.OutputFields, []string{"service", "host", "count", "sum(amount)", "mean"}) {
		t.Fatalf("output fields = %v", logical.OutputFields)
	}
	aggregate, ok := logical.Operators[len(logical.Operators)-1].(*Aggregate)
	if !ok || len(aggregate.Measures) != 3 {
		t.Fatalf("aggregate operator = %#v", logical.Operators[len(logical.Operators)-1])
	}
	if sum := aggregate.Measures[1]; sum.Function != AggregateFunctionSum || sum.Input.Name != "amount" || sum.Output != "sum(amount)" {
		t.Fatalf("sum measure = %#v", sum)
	}
	if average := aggregate.Measures[2]; average.Function != AggregateFunctionAverage || average.Input.Name != "latency" || average.Output != "mean" {
		t.Fatalf("avg measure = %#v", average)
	}
}

func TestBuildStatsMinimumAndMaximumPreserveMeasureOrder(t *testing.T) {
	t.Parallel()

	logical, err := Build(
		mustParse(t, `index=gradethis | stats count min(amount) max(label) AS largest BY service, host`),
		testScope([]string{"gradethis"}, nil),
	)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	if !slices.Equal(logical.OutputFields, []string{"service", "host", "count", "min(amount)", "largest"}) {
		t.Fatalf("output fields = %v", logical.OutputFields)
	}
	aggregate, ok := logical.Operators[len(logical.Operators)-1].(*Aggregate)
	if !ok || len(aggregate.Measures) != 3 {
		t.Fatalf("aggregate operator = %#v", logical.Operators[len(logical.Operators)-1])
	}
	if minimum := aggregate.Measures[1]; minimum.Function != AggregateFunctionMinimum ||
		minimum.Input.Name != "amount" || minimum.Output != "min(amount)" {
		t.Fatalf("min measure = %#v", minimum)
	}
	if maximum := aggregate.Measures[2]; maximum.Function != AggregateFunctionMaximum ||
		maximum.Input.Name != "label" || maximum.Output != "largest" {
		t.Fatalf("max measure = %#v", maximum)
	}
}

func TestBuildStatsEarliestAndLatestPreserveMeasureOrder(t *testing.T) {
	t.Parallel()

	logical, err := Build(
		mustParse(t, `index=gradethis | stats count earliest(amount) latest(label) AS newest BY service, host`),
		testScope([]string{"gradethis"}, nil),
	)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	if !slices.Equal(logical.OutputFields, []string{"service", "host", "count", "earliest(amount)", "newest"}) {
		t.Fatalf("output fields = %v", logical.OutputFields)
	}
	aggregate, ok := logical.Operators[len(logical.Operators)-1].(*Aggregate)
	if !ok || len(aggregate.Measures) != 3 {
		t.Fatalf("aggregate operator = %#v", logical.Operators[len(logical.Operators)-1])
	}
	if earliest := aggregate.Measures[1]; earliest.Function != AggregateFunctionEarliest ||
		earliest.Input.Name != "amount" || earliest.Output != "earliest(amount)" {
		t.Fatalf("earliest measure = %#v", earliest)
	}
	if latest := aggregate.Measures[2]; latest.Function != AggregateFunctionLatest ||
		latest.Input.Name != "label" || latest.Output != "newest" {
		t.Fatalf("latest measure = %#v", latest)
	}
}

func TestBuildStatsEarliestAndLatestRequireUnmodifiedCanonicalTime(t *testing.T) {
	t.Parallel()

	for _, source := range []string{
		`index=gradethis | fields - _time | stats earliest(value)`,
		`index=gradethis | table value | stats latest(value)`,
		`index=gradethis | eval _time=1 | stats earliest(value)`,
		`index=gradethis | rex "(?<_time>\d+)" | stats latest(value)`,
		`index=gradethis | spath input=_raw output=_time path=observed_at | stats earliest(value)`,
		`index=gradethis | rename _time AS observed_at | stats latest(value)`,
		`index=gradethis | bin _time span=5m | stats earliest(value)`,
		`index=gradethis | stats count BY _time | stats latest(count)`,
	} {
		query := mustParse(t, source)
		_, err := Build(query, testScope([]string{"gradethis"}, nil))
		assertDiagnosticCode(t, err, "SPL_UNSUPPORTED_STATS_TIME_FIELD")
		var diagnostic *Diagnostic
		if !errors.As(err, &diagnostic) {
			t.Fatalf("Build(%q) error = %T, want *Diagnostic", source, err)
		}
		stats := query.Commands[len(query.Commands)-1].(*spl.StatsCommand)
		if diagnostic.Range != stats.Aggregates[0].Range {
			t.Errorf("Build(%q) diagnostic range = %#v, want aggregate %#v", source, diagnostic.Range, stats.Aggregates[0].Range)
		}
		if !slices.Contains(diagnostic.Suggestions, "run stats earliest or latest before removing, replacing, or transforming _time") {
			t.Errorf("Build(%q) suggestions = %v", source, diagnostic.Suggestions)
		}
	}
}

func TestBuildStatsEarliestAndLatestAcceptEventPreservingPipelines(t *testing.T) {
	t.Parallel()

	for _, source := range []string{
		`index=gradethis level=error | stats earliest(value)`,
		`index=gradethis | search level=error | where status>=500 | stats latest(value)`,
		`index=gradethis | sort -_time | head 20 | stats earliest(value)`,
		`index=gradethis | sort _time | tail 20 | stats latest(value)`,
		`index=gradethis | dedup 2 host | stats earliest(value)`,
		`index=gradethis | table _time, value | stats latest(value)`,
		`index=gradethis | bin _time span=5m AS bucket_time | stats earliest(value)`,
	} {
		if _, err := Build(mustParse(t, source), testScope([]string{"gradethis"}, nil)); err != nil {
			t.Errorf("Build(%q): %v", source, err)
		}
	}
}

func TestBuildStatsDistinctCountPreservesMeasureOrderAndAliases(t *testing.T) {
	t.Parallel()

	logical, err := Build(
		mustParse(t, `index=gradethis | stats count dc(user) distinct_count(device) AS devices BY service`),
		testScope([]string{"gradethis"}, nil),
	)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	if !slices.Equal(logical.OutputFields, []string{"service", "count", "dc(user)", "devices"}) {
		t.Fatalf("output fields = %v", logical.OutputFields)
	}
	aggregate, ok := logical.Operators[len(logical.Operators)-1].(*Aggregate)
	if !ok || len(aggregate.Measures) != 3 {
		t.Fatalf("aggregate operator = %#v", logical.Operators[len(logical.Operators)-1])
	}
	for index, want := range []struct {
		input  string
		output string
	}{
		{input: "user", output: "dc(user)"},
		{input: "device", output: "devices"},
	} {
		measure := aggregate.Measures[index+1]
		if measure.Function != AggregateFunctionDistinctCount ||
			measure.Input.Name != want.input || measure.Output != want.output {
			t.Fatalf("measure %d = %#v, want dc(%s) AS %s", index+1, measure, want.input, want.output)
		}
	}
}

func TestBuildStatsValuesPreservesMeasureOrderAndAliases(t *testing.T) {
	t.Parallel()

	logical, err := Build(
		mustParse(t, `index=gradethis | stats count values(user) values(device) AS devices BY service`),
		testScope([]string{"gradethis"}, nil),
	)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	if !slices.Equal(logical.OutputFields, []string{"service", "count", "values(user)", "devices"}) {
		t.Fatalf("output fields = %v", logical.OutputFields)
	}
	aggregate, ok := logical.Operators[len(logical.Operators)-1].(*Aggregate)
	if !ok || len(aggregate.Measures) != 3 {
		t.Fatalf("aggregate operator = %#v", logical.Operators[len(logical.Operators)-1])
	}
	for index, want := range []struct {
		input  string
		output string
	}{
		{input: "user", output: "values(user)"},
		{input: "device", output: "devices"},
	} {
		measure := aggregate.Measures[index+1]
		if measure.Function != AggregateFunctionValues ||
			measure.Input.Name != want.input || measure.Output != want.output {
			t.Fatalf("measure %d = %#v, want values(%s) AS %s", index+1, measure, want.input, want.output)
		}
	}
}

func TestBuildStatsListPreservesMeasureOrderAndAliases(t *testing.T) {
	t.Parallel()

	logical, err := Build(
		mustParse(t, `index=gradethis | stats count list(user) list(device) AS devices BY service`),
		testScope([]string{"gradethis"}, nil),
	)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	if !slices.Equal(logical.OutputFields, []string{"service", "count", "list(user)", "devices"}) {
		t.Fatalf("output fields = %v", logical.OutputFields)
	}
	aggregate, ok := logical.Operators[len(logical.Operators)-1].(*Aggregate)
	if !ok || len(aggregate.Measures) != 3 {
		t.Fatalf("aggregate operator = %#v", logical.Operators[len(logical.Operators)-1])
	}
	for index, want := range []struct {
		input  string
		output string
	}{
		{input: "user", output: "list(user)"},
		{input: "device", output: "devices"},
	} {
		measure := aggregate.Measures[index+1]
		if measure.Function != AggregateFunctionList ||
			measure.Input.Name != want.input || measure.Output != want.output {
			t.Fatalf("measure %d = %#v, want list(%s) AS %s", index+1, measure, want.input, want.output)
		}
	}
}

func TestBuildStatsRejectsReservedOpenSchemaFieldsInputs(t *testing.T) {
	t.Parallel()

	for _, source := range []string{
		`index=gradethis | stats count(fields)`,
		`index=gradethis | stats dc(fields)`,
		`index=gradethis | stats values(fields)`,
		`index=gradethis | stats list(fields)`,
		`index=gradethis | stats min(fields)`,
		`index=gradethis | stats max(fields)`,
		`index=gradethis | stats earliest(fields)`,
		`index=gradethis | stats latest(fields)`,
		`index=gradethis | stats count BY fields`,
	} {
		_, err := Build(mustParse(t, source), testScope([]string{"gradethis"}, nil))
		assertDiagnosticCode(t, err, "SPL_AMBIGUOUS_STATS_FIELD")
	}

	logical, err := Build(
		mustParse(t, `index=gradethis | stats count AS fields | stats dc(fields) AS distinct_counts`),
		testScope([]string{"gradethis"}, nil),
	)
	if err != nil {
		t.Fatalf("Build closed schema: %v", err)
	}
	if !slices.Equal(logical.OutputFields, []string{"distinct_counts"}) {
		t.Fatalf("closed-schema output fields = %v", logical.OutputFields)
	}

	logical, err = Build(
		mustParse(t, `index=gradethis | stats count AS fields | stats min(fields) AS minimum_count`),
		testScope([]string{"gradethis"}, nil),
	)
	if err != nil {
		t.Fatalf("Build closed-schema min(fields): %v", err)
	}
	if !slices.Equal(logical.OutputFields, []string{"minimum_count"}) {
		t.Fatalf("closed-schema min(fields) output fields = %v", logical.OutputFields)
	}

	logical, err = Build(
		mustParse(t, `index=gradethis | stats count AS fields | stats count(fields) AS occurrences`),
		testScope([]string{"gradethis"}, nil),
	)
	if err != nil {
		t.Fatalf("Build closed-schema count(fields): %v", err)
	}
	if !slices.Equal(logical.OutputFields, []string{"occurrences"}) {
		t.Fatalf("closed-schema count(fields) output fields = %v", logical.OutputFields)
	}

	_, err = Build(
		mustParse(t, `index=gradethis | stats dc(user) distinct_count(user)`),
		testScope([]string{"gradethis"}, nil),
	)
	assertDiagnosticCode(t, err, "SPL_DUPLICATE_FIELD")
}

func TestBuildRevalidatesForgedStatsBounds(t *testing.T) {
	t.Parallel()

	base := mustParse(t, `index=gradethis`)
	tooManyMeasures := make([]spl.StatsAggregate, spl.MaximumStatsMeasures+1)
	for index := range tooManyMeasures {
		tooManyMeasures[index] = spl.StatsAggregate{
			Function: spl.AggregateFunctionCount,
			Alias:    fmt.Sprintf("count_%d", index),
		}
	}
	tooManyGroups := make([]spl.StatsGroupField, spl.MaximumStatsGroupFields+1)
	for index := range tooManyGroups {
		tooManyGroups[index] = spl.StatsGroupField{Name: fmt.Sprintf("group_%d", index)}
	}
	for _, command := range []*spl.StatsCommand{
		{Aggregates: tooManyMeasures},
		{Aggregates: []spl.StatsAggregate{{Function: spl.AggregateFunctionCount, Alias: "count"}}, GroupBy: tooManyGroups},
	} {
		query := &spl.Query{Search: base.Search, Commands: []spl.Command{command}, Range: base.Range}
		_, err := Build(query, testScope([]string{"gradethis"}, nil))
		assertDiagnosticCode(t, err, "SPL_QUERY_TOO_COMPLEX")
	}
}

func TestBuildRejectsForgedStatsCountMetadata(t *testing.T) {
	t.Parallel()

	base := mustParse(t, `index=gradethis`)
	fieldRange := spl.Range{
		Start: spl.Position{Line: 1, Column: 1},
		End:   spl.Position{Line: 1, Column: 5},
	}
	for _, aggregate := range []spl.StatsAggregate{
		{
			Function:   spl.AggregateFunctionCount,
			Input:      "host",
			InputRange: fieldRange,
			Alias:      "count",
		},
		{
			Function: spl.AggregateFunctionCountValues,
			Alias:    "count(host)",
		},
		{
			Function:   spl.AggregateFunctionCount,
			InputRange: fieldRange,
			Alias:      "count",
		},
	} {
		command := &spl.StatsCommand{Aggregates: []spl.StatsAggregate{aggregate}}
		query := &spl.Query{Search: base.Search, Commands: []spl.Command{command}, Range: base.Range}
		_, err := Build(query, testScope([]string{"gradethis"}, nil))
		assertDiagnosticCode(t, err, "SPL_UNSUPPORTED_STATS_AGGREGATE")
	}
}

func TestBuildRejectsForgedStatsRequiredInputMetadata(t *testing.T) {
	t.Parallel()

	base := mustParse(t, `index=gradethis`)
	fieldRange := spl.Range{
		Start: spl.Position{Line: 1, Column: 1},
		End:   spl.Position{Line: 1, Column: 5},
	}
	for _, function := range []spl.AggregateFunction{
		spl.AggregateFunctionPercentile,
		spl.AggregateFunctionSum,
		spl.AggregateFunctionAverage,
		spl.AggregateFunctionDistinctCount,
		spl.AggregateFunctionValues,
		spl.AggregateFunctionList,
		spl.AggregateFunctionMinimum,
		spl.AggregateFunctionMaximum,
		spl.AggregateFunctionEarliest,
		spl.AggregateFunctionLatest,
	} {
		for _, aggregate := range []spl.StatsAggregate{
			{Function: function, Input: "host", Alias: "result"},
			{Function: function, InputRange: fieldRange, Alias: "result"},
		} {
			command := &spl.StatsCommand{Aggregates: []spl.StatsAggregate{aggregate}}
			query := &spl.Query{Search: base.Search, Commands: []spl.Command{command}, Range: base.Range}
			_, err := Build(query, testScope([]string{"gradethis"}, nil))
			assertDiagnosticCode(t, err, "SPL_UNSUPPORTED_STATS_AGGREGATE")
		}
	}
}

func TestBuildStatsWildcardInputsRequireClosedSchema(t *testing.T) {
	t.Parallel()

	for _, source := range []string{
		`index=gradethis | stats count(*)`,
		`index=gradethis | stats c(*)`,
		`index=gradethis | stats count(request*)`,
		`index=gradethis | stats c(request*)`,
		`index=gradethis | stats sum(request*)`,
		`index=gradethis | stats avg(request*)`,
		`index=gradethis | stats min(request*)`,
		`index=gradethis | stats max(request*)`,
		`index=gradethis | stats earliest(request*)`,
		`index=gradethis | stats latest(request*)`,
		`index=gradethis | stats dc(request*)`,
		`index=gradethis | stats values(request*)`,
		`index=gradethis | stats list(request*)`,
	} {
		_, err := Build(
			mustParse(t, source),
			testScope([]string{"gradethis"}, nil),
		)
		assertDiagnosticCode(t, err, "SPL_UNSUPPORTED_STATS_WILDCARD")
	}
}

func TestBuildStatsRejectsDuplicateMeasureOutputs(t *testing.T) {
	t.Parallel()

	_, err := Build(
		mustParse(t, `index=gradethis | stats count AS value p95(duration) AS value BY path`),
		testScope([]string{"gradethis"}, nil),
	)
	assertDiagnosticCode(t, err, "SPL_DUPLICATE_FIELD")
}

func TestBuildStatsRejectsAmbiguousOutput(t *testing.T) {
	t.Parallel()

	_, err := Build(
		mustParse(t, `index=gradethis | stats count by count`),
		testScope([]string{"gradethis"}, nil),
	)
	assertDiagnosticCode(t, err, "SPL_DUPLICATE_FIELD")
}

func TestBuildStatsSupportsDownstreamTransformingPipeline(t *testing.T) {
	t.Parallel()

	logical, err := Build(
		mustParse(t, `index=gradethis | stats count AS events by level | search events>1 | sort -events | head 20 | table level, events`),
		testScope([]string{"gradethis"}, nil),
	)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	if !slices.Equal(logical.OutputFields, []string{"level", "events"}) {
		t.Fatalf("output fields = %v", logical.OutputFields)
	}
	wantOperators := []any{&Scan{}, &Filter{}, &Aggregate{}, &Filter{}, &Sort{}, &Limit{}, &Project{}}
	if len(logical.Operators) != len(wantOperators) {
		t.Fatalf("operator count = %d, want %d", len(logical.Operators), len(wantOperators))
	}
	for index, want := range wantOperators {
		if fmt.Sprintf("%T", logical.Operators[index]) != fmt.Sprintf("%T", want) {
			t.Fatalf("operator %d = %T, want %T", index, logical.Operators[index], want)
		}
	}
}

func TestBuildWhereLowersToPostTransformFilter(t *testing.T) {
	t.Parallel()

	logical, err := Build(
		mustParse(t, `index=gradethis | stats count by status | where count>1 AND status!=500`),
		testScope([]string{"gradethis"}, nil),
	)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	if !slices.Equal(logical.OutputFields, []string{"status", "count"}) {
		t.Fatalf("output fields = %v", logical.OutputFields)
	}
	filter, ok := logical.Operators[len(logical.Operators)-1].(*Filter)
	if !ok {
		t.Fatalf("last operator = %T, want *Filter", logical.Operators[len(logical.Operators)-1])
	}
	expression, ok := filter.Expression.(*BooleanExpression)
	if !ok || expression.Op != BooleanOpAnd {
		t.Fatalf("where expression = %#v, want logical AND", filter.Expression)
	}
	left := expression.Left.(*EvalComparisonExpression)
	right := expression.Right.(*EvalComparisonExpression)
	leftField := left.Left.(*ScalarFieldExpression)
	leftValue := left.Right.(*ScalarLiteralExpression)
	rightField := right.Left.(*ScalarFieldExpression)
	rightValue := right.Right.(*ScalarLiteralExpression)
	if leftField.Field.Name != "count" || left.Op != ComparisonOpGreater || leftValue.Value.Int64 != 1 ||
		rightField.Field.Name != "status" || right.Op != ComparisonOpNotEqual || rightValue.Value.Int64 != 500 {
		t.Fatalf("where expression = %#v", filter.Expression)
	}
}

func TestBuildWhereNullPredicatesPreserveTypedScalarCalls(t *testing.T) {
	t.Parallel()

	logical, err := Build(
		mustParse(t, `index=gradethis | where isnull(optional) OR NOT isnotnull(required)`),
		testScope([]string{"gradethis"}, nil),
	)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	filter, ok := logical.Operators[len(logical.Operators)-1].(*Filter)
	if !ok {
		t.Fatalf("last operator = %T, want *Filter", logical.Operators[len(logical.Operators)-1])
	}
	root, ok := filter.Expression.(*BooleanExpression)
	if !ok || root.Op != BooleanOpOr {
		t.Fatalf("filter expression = %#v, want OR", filter.Expression)
	}
	left, ok := root.Left.(*ScalarPredicateExpression)
	if !ok {
		t.Fatalf("filter left = %T, want *ScalarPredicateExpression", root.Left)
	}
	leftCall, ok := left.Value.(*ScalarCallExpression)
	if !ok || leftCall.Function != ScalarFunctionIsNull ||
		leftCall.Arguments[0].(*ScalarFieldExpression).Field.Name != "optional" {
		t.Fatalf("filter left predicate = %#v", left)
	}
	not, ok := root.Right.(*NotExpression)
	if !ok {
		t.Fatalf("filter right = %T, want *NotExpression", root.Right)
	}
	right := not.Operand.(*ScalarPredicateExpression)
	rightCall := right.Value.(*ScalarCallExpression)
	if rightCall.Function != ScalarFunctionIsNotNull ||
		rightCall.Arguments[0].(*ScalarFieldExpression).Field.Name != "required" {
		t.Fatalf("filter right predicate = %#v", right)
	}
}

func TestBuildEvalIfPreservesTypedConditionAndBranches(t *testing.T) {
	t.Parallel()

	logical, err := Build(
		mustParse(
			t,
			`index=gradethis | eval label=if(status>=500 AND isnotnull(host), replace(message, "error", "ERROR"), "ok")`,
		),
		testScope([]string{"gradethis"}, nil),
	)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	extend, ok := logical.Operators[len(logical.Operators)-1].(*Extend)
	if !ok || len(extend.Assignments) != 1 {
		t.Fatalf("last operator = %#v, want one-assignment Extend", logical.Operators[len(logical.Operators)-1])
	}
	conditional, ok := extend.Assignments[0].Expression.(*ScalarIfExpression)
	if !ok {
		t.Fatalf("assignment expression = %T, want *ScalarIfExpression", extend.Assignments[0].Expression)
	}
	condition, ok := conditional.Condition.(*BooleanExpression)
	if !ok || condition.Op != BooleanOpAnd {
		t.Fatalf("if condition = %#v, want AND", conditional.Condition)
	}
	comparison := condition.Left.(*EvalComparisonExpression)
	if comparison.Left.(*ScalarFieldExpression).Field.Name != "status" ||
		comparison.Op != ComparisonOpGreaterEqual ||
		comparison.Right.(*ScalarLiteralExpression).Value.Int64 != 500 {
		t.Fatalf("if comparison = %#v", comparison)
	}
	predicate := condition.Right.(*ScalarPredicateExpression)
	predicateCall := predicate.Value.(*ScalarCallExpression)
	if predicateCall.Function != ScalarFunctionIsNotNull ||
		predicateCall.Arguments[0].(*ScalarFieldExpression).Field.Name != "host" {
		t.Fatalf("if predicate = %#v", predicate)
	}
	trueValue := conditional.True.(*ScalarCallExpression)
	if trueValue.Function != ScalarFunctionReplace ||
		trueValue.Arguments[0].(*ScalarFieldExpression).Field.Name != "message" {
		t.Fatalf("if true branch = %#v", conditional.True)
	}
	if conditional.False.(*ScalarLiteralExpression).Value.String != "ok" {
		t.Fatalf("if false branch = %#v", conditional.False)
	}
}

func TestBuildEvalIfRejectsForgedBooleanConsumers(t *testing.T) {
	t.Parallel()

	query := mustParse(t, `index=gradethis | eval value=tonumber(if(isnull(optional), "1", "0"))`)
	assignment := query.Commands[0].(*spl.EvalCommand).Assignments[0]
	toNumber := assignment.Expression.(*spl.ScalarCallExpr)
	conditional := toNumber.Arguments[0].(*spl.ScalarIfExpr)
	conditional.True = &spl.ScalarCallExpr{
		Function:  spl.ScalarFunctionIsNull,
		Arguments: []spl.ScalarExpr{&spl.ScalarFieldExpr{Field: "left"}},
	}
	_, err := Build(query, testScope([]string{"gradethis"}, nil))
	assertDiagnosticCode(t, err, "SPL_UNSUPPORTED_EVAL_EXPRESSION")
}

func TestBuildEvalIfRejectsForgedMissingNodes(t *testing.T) {
	t.Parallel()

	var nilComparison *spl.WhereComparisonExpr
	var nilField *spl.ScalarFieldExpr
	tests := []struct {
		name       string
		condition  spl.WhereExpr
		trueValue  spl.ScalarExpr
		falseValue spl.ScalarExpr
		code       string
	}{
		{
			name:       "nil condition",
			trueValue:  &spl.ScalarLiteralExpr{Value: spl.Literal{Kind: spl.LiteralKindString, Text: "yes"}},
			falseValue: &spl.ScalarLiteralExpr{Value: spl.Literal{Kind: spl.LiteralKindString, Text: "no"}},
			code:       "SPL_UNSUPPORTED_WHERE_EXPRESSION",
		},
		{
			name:       "typed nil condition",
			condition:  nilComparison,
			trueValue:  &spl.ScalarLiteralExpr{Value: spl.Literal{Kind: spl.LiteralKindString, Text: "yes"}},
			falseValue: &spl.ScalarLiteralExpr{Value: spl.Literal{Kind: spl.LiteralKindString, Text: "no"}},
			code:       "SPL_UNSUPPORTED_WHERE_EXPRESSION",
		},
		{
			name: "typed nil true branch",
			condition: &spl.WhereScalarPredicateExpr{Value: &spl.ScalarCallExpr{
				Function:  spl.ScalarFunctionIsNull,
				Arguments: []spl.ScalarExpr{&spl.ScalarFieldExpr{Field: "optional"}},
			}},
			trueValue:  nilField,
			falseValue: &spl.ScalarLiteralExpr{Value: spl.Literal{Kind: spl.LiteralKindString, Text: "no"}},
			code:       "SPL_UNSUPPORTED_EVAL_EXPRESSION",
		},
		{
			name: "nil false branch",
			condition: &spl.WhereScalarPredicateExpr{Value: &spl.ScalarCallExpr{
				Function:  spl.ScalarFunctionIsNull,
				Arguments: []spl.ScalarExpr{&spl.ScalarFieldExpr{Field: "optional"}},
			}},
			trueValue: &spl.ScalarLiteralExpr{Value: spl.Literal{Kind: spl.LiteralKindString, Text: "yes"}},
			code:      "SPL_UNSUPPORTED_EVAL_EXPRESSION",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			query := mustParse(t, `index=gradethis | eval value=if(isnull(optional), "yes", "no")`)
			query.Commands[0].(*spl.EvalCommand).Assignments[0].Expression = &spl.ScalarIfExpr{
				Condition: test.condition,
				True:      test.trueValue,
				False:     test.falseValue,
			}
			_, err := Build(query, testScope([]string{"gradethis"}, nil))
			assertDiagnosticCode(t, err, test.code)
		})
	}
}

func TestBuildEvalIfRejectsForgedInvalidBooleanOperator(t *testing.T) {
	t.Parallel()

	query := mustParse(t, `index=gradethis | eval value=if(left=1 AND right=2, "yes", "no")`)
	conditional := query.Commands[0].(*spl.EvalCommand).Assignments[0].Expression.(*spl.ScalarIfExpr)
	conditional.Condition.(*spl.WhereBoolExpr).Op = spl.BoolOpInvalid
	_, err := Build(query, testScope([]string{"gradethis"}, nil))
	assertDiagnosticCode(t, err, "SPL_UNSUPPORTED_WHERE_EXPRESSION")
}

func TestBuildRejectsForgedBooleanScalarConsumer(t *testing.T) {
	t.Parallel()

	query := mustParse(t, `index=gradethis | where tonumber(optional)=0`)
	comparison := query.Commands[0].(*spl.WhereCommand).Expression.(*spl.WhereComparisonExpr)
	comparison.Left = &spl.ScalarCallExpr{
		Function: spl.ScalarFunctionToNumber,
		Arguments: []spl.ScalarExpr{&spl.ScalarCallExpr{
			Function: spl.ScalarFunctionIsNull,
			Arguments: []spl.ScalarExpr{&spl.ScalarFieldExpr{
				Field: "optional",
			}},
		}},
	}
	_, err := Build(query, testScope([]string{"gradethis"}, nil))
	assertDiagnosticCode(t, err, "SPL_UNSUPPORTED_EVAL_EXPRESSION")
}

func TestBuildEvalLowersOrderedTypedAssignments(t *testing.T) {
	t.Parallel()

	logical, err := Build(
		mustParse(t, `index=gradethis | eval duration_ms=tonumber(replace(duration, "ms$", ""))`),
		testScope([]string{"gradethis"}, nil),
	)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	extend, ok := logical.Operators[len(logical.Operators)-1].(*Extend)
	if !ok || len(extend.Assignments) != 1 {
		t.Fatalf("last operator = %#v, want one-assignment Extend", logical.Operators[len(logical.Operators)-1])
	}
	assignment := extend.Assignments[0]
	if assignment.Output.Name != "duration_ms" {
		t.Fatalf("assignment output = %#v", assignment.Output)
	}
	toNumber, ok := assignment.Expression.(*ScalarCallExpression)
	if !ok || toNumber.Function != ScalarFunctionToNumber || len(toNumber.Arguments) != 1 {
		t.Fatalf("outer expression = %#v", assignment.Expression)
	}
	replace, ok := toNumber.Arguments[0].(*ScalarCallExpression)
	if !ok || replace.Function != ScalarFunctionReplace || len(replace.Arguments) != 3 {
		t.Fatalf("inner expression = %#v", toNumber.Arguments[0])
	}
	input := replace.Arguments[0].(*ScalarFieldExpression)
	if input.Field.Name != "duration" {
		t.Fatalf("replace input = %#v", input)
	}
}

func TestBuildEvalBoundaryPreventsPipelineIndexFromChangingScanScope(t *testing.T) {
	t.Parallel()

	logical, err := Build(
		mustParse(t, `index=gradethis | eval index=replace(index, "grade", "other") | search index=otherthis`),
		testScope([]string{"gradethis"}, nil),
	)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	if !slices.Equal(logical.EffectiveIndexes, []string{"gradethis"}) {
		t.Fatalf("effective indexes = %v", logical.EffectiveIndexes)
	}
}

func TestBuildEvalWithoutIndexOverwriteRetainsIndexValidation(t *testing.T) {
	t.Parallel()

	_, err := Build(
		mustParse(t, `index=gradethis | eval x=1 | search index=secret`),
		testScope([]string{"gradethis"}, nil),
	)
	assertDiagnosticCode(t, err, "SPL_INDEX_FORBIDDEN")
}

func TestBuildRexProducesBackendNeutralExtractPlan(t *testing.T) {
	t.Parallel()

	logical, err := Build(
		mustParse(t, `index=gradethis | table duration | rex field=duration "^(?<value>\d+(?:\.\d+)?)(?P<unit>µs|ms)$"`),
		testScope([]string{"gradethis"}, nil),
	)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	if len(logical.Operators) != 4 {
		t.Fatalf("operator count = %d, want 4", len(logical.Operators))
	}
	extract, ok := logical.Operators[3].(*Extract)
	if !ok {
		t.Fatalf("operator = %T, want *Extract", logical.Operators[3])
	}
	if extract.Input.Name != "duration" || !strings.HasPrefix(extract.Pattern, "(?-s)") ||
		len(extract.Captures) != 2 {
		t.Fatalf("extract = %#v", extract)
	}
	if extract.Captures[0].Output.Name != "value" || extract.Captures[0].Group != 1 ||
		extract.Captures[1].Output.Name != "unit" || extract.Captures[1].Group != 2 {
		t.Fatalf("captures = %#v", extract.Captures)
	}
	if !slices.Equal(logical.OutputFields, []string{"duration", "value", "unit"}) {
		t.Fatalf("output fields = %v", logical.OutputFields)
	}
}

func TestBuildRexKnownSchemaOverwritesWithoutDuplicateOutput(t *testing.T) {
	t.Parallel()

	logical, err := Build(
		mustParse(t, `index=gradethis | table duration, value | rex field=duration "(?<value>\d+)"`),
		testScope([]string{"gradethis"}, nil),
	)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	if !slices.Equal(logical.OutputFields, []string{"duration", "value"}) {
		t.Fatalf("output fields = %v", logical.OutputFields)
	}
}

func TestBuildRexIndexOutputNeverChangesPhysicalScope(t *testing.T) {
	t.Parallel()

	logical, err := Build(
		mustParse(t, `index=gradethis | rex "(?<index>secret)" | search index=secret`),
		testScope([]string{"gradethis"}, nil),
	)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	scan := logical.Operators[0].(*Scan)
	if !slices.Equal(scan.Indexes, []string{"gradethis"}) ||
		!slices.Equal(logical.EffectiveIndexes, []string{"gradethis"}) {
		t.Fatalf("scope changed by rex output: scan=%v effective=%v", scan.Indexes, logical.EffectiveIndexes)
	}
	extract := logical.Operators[2].(*Extract)
	if extract.Captures[0].Output.Name != "index" {
		t.Fatalf("extract = %#v", extract)
	}
}

func TestBuildRexIndexScopeUsesValidatedPattern(t *testing.T) {
	t.Parallel()

	calculated := mustParse(t, `index=gradethis | rex "(?<index>secret)" | search index=secret`)
	if _, err := Build(calculated, testScope([]string{"gradethis"}, nil)); err != nil {
		t.Fatalf("Build(calculated index): %v", err)
	}

	physical := mustParse(t, `index=gradethis | rex "(?<other>secret)" | search index=secret`)
	_, err := Build(physical, testScope([]string{"gradethis"}, nil))
	assertDiagnosticCode(t, err, "SPL_INDEX_FORBIDDEN")
}

func TestBuildRexRejectsReservedAndAmbiguousOutputs(t *testing.T) {
	t.Parallel()

	tests := []struct {
		source string
		code   string
	}{
		{`index=gradethis | rex "(?<__os_private>x)"`, "SPL_RESERVED_FIELD"},
		{`index=gradethis | rex "(?<fields>x)"`, "SPL_AMBIGUOUS_REX_FIELD"},
	}
	for _, test := range tests {
		_, err := Build(mustParse(t, test.source), testScope([]string{"gradethis"}, nil))
		assertDiagnosticCode(t, err, test.code)
	}

	if _, err := Build(
		mustParse(t, `index=gradethis | table fields | rex field=fields "(?<fields>x)"`),
		testScope([]string{"gradethis"}, nil),
	); err != nil {
		t.Fatalf("closed-schema fields capture: %v", err)
	}
}

func TestBuildRexTimeCaptureInvalidatesCanonicalTime(t *testing.T) {
	t.Parallel()

	_, err := Build(
		mustParse(t, `index=gradethis | rex "(?<_time>\d+)" | timechart span=5m count BY level`),
		testScope([]string{"gradethis"}, nil),
	)
	assertDiagnosticCode(t, err, "SPL_UNSUPPORTED_TIMECHART_TIME_FIELD")
}

func TestBuildRexBoundsTotalOutputsAcrossPipeline(t *testing.T) {
	t.Parallel()

	var source strings.Builder
	source.WriteString("index=gradethis")
	for command := range 5 {
		source.WriteString(` | rex "`)
		for capture := range 16 {
			fmt.Fprintf(&source, "(?<r%d_%d>x)", command, capture)
		}
		source.WriteByte('"')
	}
	_, err := Build(mustParse(t, source.String()), testScope([]string{"gradethis"}, nil))
	assertDiagnosticCode(t, err, "SPL_QUERY_TOO_COMPLEX")
}

func TestBuildTopLowersToAggregateWindowAndDeterministicTopN(t *testing.T) {
	t.Parallel()

	logical, err := Build(
		mustParse(t, `index=gradethis | top limit=20 message`),
		testScope([]string{"gradethis"}, nil),
	)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	if !slices.Equal(logical.OutputFields, []string{"message", "count", "percent"}) {
		t.Fatalf("output fields = %v", logical.OutputFields)
	}
	if len(logical.Operators) != 5 {
		t.Fatalf("operator count = %d, want Scan, Filter, Aggregate, Window, Sort", len(logical.Operators))
	}
	aggregate, ok := logical.Operators[2].(*Aggregate)
	if !ok || len(aggregate.GroupBy) != 1 || aggregate.GroupBy[0].Name != "message" ||
		len(aggregate.Measures) != 1 || aggregate.Measures[0].Function != AggregateFunctionCountRows ||
		aggregate.Measures[0].Output != "count" {
		t.Fatalf("aggregate = %#v", logical.Operators[2])
	}
	window, ok := logical.Operators[3].(*Window)
	if !ok || window.Function != WindowFunctionPercentOfTotal || window.Input.Name != "count" || window.Output != "percent" {
		t.Fatalf("window = %#v", logical.Operators[3])
	}
	sortOp, ok := logical.Operators[4].(*Sort)
	if !ok || sortOp.Limit != 20 || len(sortOp.Keys) != 2 || sortOp.Keys[0].Field.Name != "count" ||
		!sortOp.Keys[0].Descending || sortOp.Keys[1].Field.Name != "message" ||
		!sortOp.Keys[1].Descending || sortOp.Keys[1].Mode != SortValueModeLexical {
		t.Fatalf("top sort = %#v", logical.Operators[4])
	}
}

func TestBuildTopLimitZeroIsBoundedOnlyByExecutorPolicy(t *testing.T) {
	t.Parallel()

	logical, err := Build(mustParse(t, `index=gradethis | top limit=0 host`), testScope([]string{"gradethis"}, nil))
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	if got := logical.Operators[len(logical.Operators)-1].(*Sort).Limit; got != 0 {
		t.Fatalf("top limit = %d, want unlimited logical result", got)
	}
}

func TestBuildTopRejectsGeneratedOutputCollisions(t *testing.T) {
	t.Parallel()

	for _, field := range []string{"count", "percent"} {
		_, err := Build(mustParse(t, `index=gradethis | top `+field), testScope([]string{"gradethis"}, nil))
		assertDiagnosticCode(t, err, "SPL_DUPLICATE_FIELD")
	}
}

func TestBuildTopByPartitionsPercentAndRetainsPerGroupLimit(t *testing.T) {
	t.Parallel()

	logical, err := Build(
		mustParse(t, `index=gradethis | top limit=3 message BY host`),
		testScope([]string{"gradethis"}, nil),
	)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	if !slices.Equal(logical.OutputFields, []string{"host", "message", "count", "percent"}) {
		t.Fatalf("output fields = %v", logical.OutputFields)
	}
	if len(logical.Operators) != 6 {
		t.Fatalf("operator count = %d, want Scan, Filter, Aggregate, Window, Sort, Deduplicate", len(logical.Operators))
	}
	aggregate, ok := logical.Operators[2].(*Aggregate)
	if !ok || len(aggregate.GroupBy) != 2 || aggregate.GroupBy[0].Name != "host" || aggregate.GroupBy[1].Name != "message" {
		t.Fatalf("aggregate = %#v", logical.Operators[2])
	}
	window, ok := logical.Operators[3].(*Window)
	if !ok || window.Function != WindowFunctionPercentOfTotal || window.Input.Name != "count" ||
		window.Output != "percent" || len(window.PartitionBy) != 1 || window.PartitionBy[0].Name != "host" {
		t.Fatalf("window = %#v", logical.Operators[3])
	}
	sortOp, ok := logical.Operators[4].(*Sort)
	if !ok || sortOp.Limit != 0 || len(sortOp.Keys) != 3 ||
		sortOp.Keys[0].Field.Name != "host" || sortOp.Keys[0].Descending || sortOp.Keys[0].Mode != SortValueModeLexical ||
		sortOp.Keys[1].Field.Name != "count" || !sortOp.Keys[1].Descending ||
		sortOp.Keys[2].Field.Name != "message" || !sortOp.Keys[2].Descending || sortOp.Keys[2].Mode != SortValueModeLexical {
		t.Fatalf("top BY sort = %#v", logical.Operators[4])
	}
	dedup, ok := logical.Operators[5].(*Deduplicate)
	if !ok || dedup.Count != 3 || dedup.Consecutive || len(dedup.Keys) != 1 || dedup.Keys[0].Name != "host" {
		t.Fatalf("top BY retention = %#v", logical.Operators[5])
	}

	unlimited, err := Build(mustParse(t, `index=gradethis | rare limit=0 message BY host`), testScope([]string{"gradethis"}, nil))
	if err != nil {
		t.Fatalf("Build rare limit=0: %v", err)
	}
	if len(unlimited.Operators) != 5 {
		t.Fatalf("rare limit=0 BY operator count = %d, want no per-group retention", len(unlimited.Operators))
	}
	if rareSort, ok := unlimited.Operators[4].(*Sort); !ok || rareSort.Keys[1].Field.Name != "count" || rareSort.Keys[1].Descending {
		t.Fatalf("rare BY sort = %#v", unlimited.Operators[4])
	}
}

func TestBuildFrequencyCommandsRenameAndHideGeneratedOutputs(t *testing.T) {
	t.Parallel()

	renamed, err := Build(
		mustParse(t, `index=gradethis | top countfield=total percentfield=share message`),
		testScope([]string{"gradethis"}, nil),
	)
	if err != nil {
		t.Fatalf("Build renamed: %v", err)
	}
	if !slices.Equal(renamed.OutputFields, []string{"message", "total", "share"}) {
		t.Fatalf("renamed output fields = %v", renamed.OutputFields)
	}
	if aggregate := renamed.Operators[2].(*Aggregate); aggregate.Measures[0].Output != "total" {
		t.Fatalf("renamed aggregate = %#v", aggregate)
	}
	if window := renamed.Operators[3].(*Window); window.Input.Name != "total" || window.Output != "share" {
		t.Fatalf("renamed window = %#v", window)
	}
	if sortOp := renamed.Operators[4].(*Sort); sortOp.Keys[0].Field.Name != "total" {
		t.Fatalf("renamed sort = %#v", sortOp)
	}

	hidden, err := Build(
		mustParse(t, `index=gradethis | top showcount=false showperc=false message`),
		testScope([]string{"gradethis"}, nil),
	)
	if err != nil {
		t.Fatalf("Build hidden: %v", err)
	}
	if !slices.Equal(hidden.OutputFields, []string{"message"}) {
		t.Fatalf("hidden output fields = %v", hidden.OutputFields)
	}
	if len(hidden.Operators) != 5 {
		t.Fatalf("hidden operator count = %d, want Scan, Filter, Aggregate, Sort, Project", len(hidden.Operators))
	}
	if sortOp, ok := hidden.Operators[3].(*Sort); !ok || sortOp.Limit != 10 || sortOp.Keys[0].Field.Name != "count" {
		t.Fatalf("hidden sort = %#v", hidden.Operators[3])
	}
	if project, ok := hidden.Operators[4].(*Project); !ok || project.Mode != ProjectModeExclude ||
		len(project.Fields) != 1 || project.Fields[0].Name != "count" {
		t.Fatalf("hidden projection = %#v", hidden.Operators[4])
	}

	percentOnly, err := Build(
		mustParse(t, `index=gradethis | rare showcount=false message BY host`),
		testScope([]string{"gradethis"}, nil),
	)
	if err != nil {
		t.Fatalf("Build percent only: %v", err)
	}
	if !slices.Equal(percentOnly.OutputFields, []string{"host", "message", "percent"}) {
		t.Fatalf("percent-only output fields = %v", percentOnly.OutputFields)
	}
	if _, ok := percentOnly.Operators[len(percentOnly.Operators)-1].(*Project); !ok {
		t.Fatalf("percent-only plan must end by excluding count: %#v", percentOnly.Operators)
	}
}

func TestBuildFrequencyCommandsRejectOutputAndByCollisions(t *testing.T) {
	t.Parallel()

	for _, source := range []string{
		`index=gradethis | top countfield=host host`,
		`index=gradethis | top percentfield=host message BY host`,
		`index=gradethis | top countfield=same percentfield=same host`,
		`index=gradethis | rare countfield=percent host`,
		`index=gradethis | top message BY count`,
	} {
		_, err := Build(mustParse(t, source), testScope([]string{"gradethis"}, nil))
		assertDiagnosticCode(t, err, "SPL_DUPLICATE_FIELD")
	}
	_, err := Build(mustParse(t, `index=gradethis | top countfield=__os_count host`), testScope([]string{"gradethis"}, nil))
	assertDiagnosticCode(t, err, "SPL_RESERVED_FIELD")
}

func TestBuildFrequencyCommandsUseCompleteOrderedFieldTuples(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name          string
		source        string
		leastFrequent bool
	}{
		{name: "top", source: `index=gradethis | top limit=20 service,status`},
		{name: "rare", source: `index=gradethis | rare limit=20 service,status`, leastFrequent: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			logical, err := Build(
				mustParse(t, test.source),
				testScope([]string{"gradethis"}, nil),
			)
			if err != nil {
				t.Fatalf("Build: %v", err)
			}
			if !slices.Equal(
				logical.OutputFields,
				[]string{"service", "status", "count", "percent"},
			) {
				t.Fatalf("output fields = %v", logical.OutputFields)
			}
			if got := len(logical.Operators); got != 5 {
				t.Fatalf("operators = %#v, want scan/filter/aggregate/window/sort", logical.Operators)
			}
			aggregate, ok := logical.Operators[2].(*Aggregate)
			if !ok || len(aggregate.GroupBy) != 2 ||
				aggregate.GroupBy[0].Name != "service" ||
				aggregate.GroupBy[1].Name != "status" ||
				len(aggregate.Measures) != 1 ||
				aggregate.Measures[0].Function != AggregateFunctionCountRows {
				t.Fatalf("aggregate = %#v", logical.Operators[2])
			}
			sortOp, ok := logical.Operators[4].(*Sort)
			if !ok || sortOp.Limit != 20 || len(sortOp.Keys) != 3 ||
				sortOp.Keys[0].Field.Name != "count" ||
				sortOp.Keys[0].Descending == test.leastFrequent ||
				sortOp.Keys[1].Field.Name != "service" ||
				!sortOp.Keys[1].Descending ||
				sortOp.Keys[1].Mode != SortValueModeLexical ||
				sortOp.Keys[2].Field.Name != "status" ||
				!sortOp.Keys[2].Descending ||
				sortOp.Keys[2].Mode != SortValueModeLexical {
				t.Fatalf("sort = %#v", logical.Operators[4])
			}
		})
	}
}

func TestBuildFrequencyCommandAcceptsMaximumOrderedFieldTuple(t *testing.T) {
	t.Parallel()

	fieldNames := make([]string, spl.MaximumFrequencyFields)
	for index := range fieldNames {
		fieldNames[index] = fmt.Sprintf("field_%02d", index)
	}
	logical, err := Build(
		mustParse(t, `index=gradethis | top `+strings.Join(fieldNames, ",")),
		testScope([]string{"gradethis"}, nil),
	)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	if got := len(logical.Operators); got != 5 {
		t.Fatalf("operators = %#v, want scan/filter/aggregate/window/sort", logical.Operators)
	}

	aggregate, ok := logical.Operators[2].(*Aggregate)
	if !ok || len(aggregate.GroupBy) != spl.MaximumFrequencyFields {
		t.Fatalf("aggregate = %#v, want %d grouping fields", logical.Operators[2], spl.MaximumFrequencyFields)
	}
	for index, field := range aggregate.GroupBy {
		if field.Name != fieldNames[index] {
			t.Fatalf("aggregate grouping field %d = %q, want %q", index, field.Name, fieldNames[index])
		}
	}

	sortOp, ok := logical.Operators[4].(*Sort)
	if !ok || len(sortOp.Keys) != spl.MaximumFrequencyFields+1 {
		t.Fatalf("sort = %#v, want %d keys", logical.Operators[4], spl.MaximumFrequencyFields+1)
	}
	if sortOp.Keys[0].Field.Name != "count" || !sortOp.Keys[0].Descending {
		t.Fatalf("frequency sort key 0 = %#v, want descending count", sortOp.Keys[0])
	}
	for index, fieldName := range fieldNames {
		key := sortOp.Keys[index+1]
		if key.Field.Name != fieldName || !key.Descending || key.Mode != SortValueModeLexical {
			t.Fatalf("frequency sort key %d = %#v, want descending lexical %q", index+1, key, fieldName)
		}
	}
}

func TestBuildFrequencyCommandsRevalidateForgedFieldTuples(t *testing.T) {
	t.Parallel()

	base := mustParse(t, `index=gradethis`)
	tooMany := make([]spl.FrequencyField, spl.MaximumFrequencyFields+1)
	for index := range tooMany {
		tooMany[index] = spl.FrequencyField{Name: fmt.Sprintf("field_%d", index)}
	}
	for _, test := range []struct {
		name    string
		command spl.Command
		code    string
	}{
		{name: "empty top", command: &spl.TopCommand{}, code: "SPL_EXPECTED_FIELD"},
		{name: "empty rare", command: &spl.RareCommand{}, code: "SPL_EXPECTED_FIELD"},
		{name: "oversized top", command: &spl.TopCommand{Fields: tooMany}, code: "SPL_QUERY_TOO_COMPLEX"},
		{name: "oversized rare", command: &spl.RareCommand{Fields: tooMany}, code: "SPL_QUERY_TOO_COMPLEX"},
		{
			name: "duplicate top",
			command: &spl.TopCommand{Fields: []spl.FrequencyField{
				{Name: "service"}, {Name: "service"},
			}},
			code: "SPL_DUPLICATE_FIELD",
		},
		{
			name: "generated collision rare",
			command: &spl.RareCommand{Fields: []spl.FrequencyField{
				{Name: "service"}, {Name: "percent"},
			}},
			code: "SPL_DUPLICATE_FIELD",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			query := &spl.Query{
				Search:   base.Search,
				Commands: []spl.Command{test.command},
				Range:    base.Range,
			}
			_, err := Build(
				query,
				testScope([]string{"gradethis"}, nil),
			)
			assertDiagnosticCode(t, err, test.code)
		})
	}
}

func TestBuildRareLowersToAggregateWindowAndDeterministicBottomN(t *testing.T) {
	t.Parallel()

	logical, err := Build(
		mustParse(t, `index=gradethis | rare limit=20 message`),
		testScope([]string{"gradethis"}, nil),
	)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	if !slices.Equal(logical.OutputFields, []string{"message", "count", "percent"}) {
		t.Fatalf("output fields = %v", logical.OutputFields)
	}
	if len(logical.Operators) != 5 {
		t.Fatalf("operator count = %d, want Scan, Filter, Aggregate, Window, Sort", len(logical.Operators))
	}
	aggregate, ok := logical.Operators[2].(*Aggregate)
	if !ok || len(aggregate.GroupBy) != 1 || aggregate.GroupBy[0].Name != "message" ||
		len(aggregate.Measures) != 1 || aggregate.Measures[0].Function != AggregateFunctionCountRows ||
		aggregate.Measures[0].Output != "count" {
		t.Fatalf("aggregate = %#v", logical.Operators[2])
	}
	window, ok := logical.Operators[3].(*Window)
	if !ok || window.Function != WindowFunctionPercentOfTotal || window.Input.Name != "count" || window.Output != "percent" {
		t.Fatalf("window = %#v", logical.Operators[3])
	}
	sortOp, ok := logical.Operators[4].(*Sort)
	if !ok || sortOp.Limit != 20 || len(sortOp.Keys) != 2 || sortOp.Keys[0].Field.Name != "count" ||
		sortOp.Keys[0].Descending || sortOp.Keys[1].Field.Name != "message" ||
		!sortOp.Keys[1].Descending || sortOp.Keys[1].Mode != SortValueModeLexical {
		t.Fatalf("rare sort = %#v", logical.Operators[4])
	}
}

func TestBuildRareLimitZeroAndGeneratedOutputCollisions(t *testing.T) {
	t.Parallel()

	logical, err := Build(mustParse(t, `index=gradethis | rare limit=0 host`), testScope([]string{"gradethis"}, nil))
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	if got := logical.Operators[len(logical.Operators)-1].(*Sort).Limit; got != 0 {
		t.Fatalf("rare limit = %d, want unlimited logical result", got)
	}

	for _, field := range []string{"count", "percent"} {
		_, err := Build(mustParse(t, `index=gradethis | rare `+field), testScope([]string{"gradethis"}, nil))
		assertDiagnosticCode(t, err, "SPL_DUPLICATE_FIELD")
	}
}

func TestBuildTimeBinProducesStreamingTimeBucket(t *testing.T) {
	t.Parallel()

	logical, err := Build(
		mustParse(t, `index=gradethis | bucket span=5m _time | stats count BY _time`),
		testScope([]string{"gradethis"}, nil),
	)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	if len(logical.Operators) != 4 {
		t.Fatalf("operator count = %d, want Scan, Filter, TimeBucket, Aggregate", len(logical.Operators))
	}
	bucket, ok := logical.Operators[2].(*TimeBucket)
	if !ok {
		t.Fatalf("operator 2 = %T, want *TimeBucket", logical.Operators[2])
	}
	if bucket.Field.Name != "_time" || !bucket.Field.Canonical ||
		bucket.Output.Name != "_time" || !bucket.Output.Canonical ||
		bucket.Span != 5*time.Minute {
		t.Fatalf("time bucket = %#v", bucket)
	}
	if !slices.Equal(logical.OutputFields, []string{"_time", "count"}) || logical.DynamicOutput != nil {
		t.Fatalf("output = %v dynamic=%#v, want [_time count] and static schema", logical.OutputFields, logical.DynamicOutput)
	}
}

func TestBuildNumericBinProducesStreamingNumericBucket(t *testing.T) {
	t.Parallel()

	logical, err := Build(
		mustParse(t, `index=gradethis | eval latency=-11.5 | bucket span=10 latency AS band | stats count BY band`),
		testScope([]string{"gradethis"}, nil),
	)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	if len(logical.Operators) != 5 {
		t.Fatalf("operator count = %d, want Scan, Filter, Extend, NumericBucket, Aggregate", len(logical.Operators))
	}
	bucket, ok := logical.Operators[3].(*NumericBucket)
	if !ok {
		t.Fatalf("operator 3 = %T, want *NumericBucket", logical.Operators[3])
	}
	if bucket.Input.Name != "latency" || bucket.Output.Name != "band" || bucket.Span != 10 {
		t.Fatalf("numeric bucket = %#v", bucket)
	}
	if !slices.Equal(logical.OutputFields, []string{"band", "count"}) || logical.DynamicOutput != nil {
		t.Fatalf("output = %v dynamic=%#v, want [band count] and static schema", logical.OutputFields, logical.DynamicOutput)
	}
}

func TestBuildUnitlessTimeBinUsesSeconds(t *testing.T) {
	t.Parallel()

	logical, err := Build(
		mustParse(t, `index=gradethis | bin _time span=5 AS bucket_time`),
		testScope([]string{"gradethis"}, nil),
	)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	bucket, ok := logical.Operators[len(logical.Operators)-1].(*TimeBucket)
	if !ok {
		t.Fatalf("last operator = %T, want *TimeBucket", logical.Operators[len(logical.Operators)-1])
	}
	if bucket.Field.Name != "_time" || bucket.Output.Name != "bucket_time" || bucket.Span != 5*time.Second {
		t.Fatalf("time bucket = %#v", bucket)
	}
}

func TestBuildNumericBinSupportsTransformingRowsAndASOutputSchema(t *testing.T) {
	t.Parallel()

	tests := []struct {
		source string
		input  string
		output string
		fields []string
	}{
		{
			source: `index=gradethis | stats count BY level | bin count span=10`,
			input:  "count", output: "count", fields: []string{"level", "count"},
		},
		{
			source: `index=gradethis | stats count BY level | bin count span=10 AS band`,
			input:  "count", output: "band", fields: []string{"level", "count", "band"},
		},
		{
			source: `index=gradethis | stats count BY level | bin count span=10 AS level`,
			input:  "count", output: "level", fields: []string{"level", "count"},
		},
	}
	for _, test := range tests {
		t.Run(test.source, func(t *testing.T) {
			t.Parallel()
			logical, err := Build(mustParse(t, test.source), testScope([]string{"gradethis"}, nil))
			if err != nil {
				t.Fatalf("Build: %v", err)
			}
			bucket, ok := logical.Operators[len(logical.Operators)-1].(*NumericBucket)
			if !ok || bucket.Input.Name != test.input || bucket.Output.Name != test.output || bucket.Span != 10 {
				t.Fatalf("numeric bucket = %#v", logical.Operators[len(logical.Operators)-1])
			}
			if !slices.Equal(logical.OutputFields, test.fields) {
				t.Fatalf("output fields = %v, want %v", logical.OutputFields, test.fields)
			}
		})
	}
}

func TestBuildNumericBinBoundsExplicitSpan(t *testing.T) {
	t.Parallel()

	logical, err := Build(
		mustParse(t, `index=gradethis | bin severity span=9007199254740991`),
		testScope([]string{"gradethis"}, nil),
	)
	if err != nil {
		t.Fatalf("Build(maximum numeric span): %v", err)
	}
	bucket, ok := logical.Operators[len(logical.Operators)-1].(*NumericBucket)
	if !ok || bucket.Span != MaximumNumericBinSpan {
		t.Fatalf("numeric bucket = %#v, want span %d", logical.Operators[len(logical.Operators)-1], MaximumNumericBinSpan)
	}

	_, err = Build(
		mustParse(t, `index=gradethis | bin severity span=9007199254740992`),
		testScope([]string{"gradethis"}, nil),
	)
	assertDiagnosticCode(t, err, "SPL_NUMBER_OUT_OF_RANGE")
}

func TestBuildBinRejectsUnsupportedFieldAndSpanCombinations(t *testing.T) {
	t.Parallel()

	for _, source := range []string{
		`index=gradethis | bin severity span=5m`,
		`index=gradethis | bin fields span=10`,
		`index=gradethis | bin severity span=10 AS fields`,
	} {
		_, err := Build(mustParse(t, source), testScope([]string{"gradethis"}, nil))
		if source == `index=gradethis | bin severity span=5m` {
			assertDiagnosticCode(t, err, "SPL_UNSUPPORTED_BIN_TIME_FIELD")
		} else {
			assertDiagnosticCode(t, err, "SPL_AMBIGUOUS_BIN_FIELD")
		}
	}
}

func TestBuildBinAllowsFieldsWithClosedSchema(t *testing.T) {
	t.Parallel()

	for _, source := range []string{
		`index=gradethis | stats count AS fields | bin fields span=10`,
		`index=gradethis | stats count | bin count span=10 AS fields`,
	} {
		logical, err := Build(mustParse(t, source), testScope([]string{"gradethis"}, nil))
		if err != nil {
			t.Fatalf("Build(%q): %v", source, err)
		}
		if _, ok := logical.Operators[len(logical.Operators)-1].(*NumericBucket); !ok {
			t.Fatalf("last operator for %q = %T, want *NumericBucket", source, logical.Operators[len(logical.Operators)-1])
		}
		if !slices.Contains(logical.OutputFields, "fields") {
			t.Fatalf("output fields for %q = %v, want fields retained or appended", source, logical.OutputFields)
		}
	}
}

func TestBuildBinASCanonicalTimeProvenance(t *testing.T) {
	t.Parallel()

	if _, err := Build(
		mustParse(t, `index=gradethis | bin _time span=5m AS bucket_time | timechart span=5m count BY level`),
		testScope([]string{"gradethis"}, nil),
	); err != nil {
		t.Fatalf("time-bin AS unexpectedly invalidated source _time: %v", err)
	}

	_, err := Build(
		mustParse(t, `index=gradethis | bin severity span=10 AS _time | timechart span=5m count BY level`),
		testScope([]string{"gradethis"}, nil),
	)
	assertDiagnosticCode(t, err, "SPL_UNSUPPORTED_TIMECHART_TIME_FIELD")
}

func TestBuildBinChecksCanonicalTimeOnlyWhenReadingIt(t *testing.T) {
	t.Parallel()

	logical, err := Build(
		mustParse(t, `index=gradethis | eval _time=1 | bin severity span=10 AS band`),
		testScope([]string{"gradethis"}, nil),
	)
	if err != nil {
		t.Fatalf("Build(non-time bin after replacing _time): %v", err)
	}
	if _, ok := logical.Operators[len(logical.Operators)-1].(*NumericBucket); !ok {
		t.Fatalf("last operator = %T, want *NumericBucket", logical.Operators[len(logical.Operators)-1])
	}
}

func TestBuildTimeBinRequiresUnmodifiedCanonicalTime(t *testing.T) {
	t.Parallel()

	for _, source := range []string{
		`index=gradethis | fields - _time | bin _time span=5m`,
		`index=gradethis | table level | bin _time span=5m`,
		`index=gradethis | eval _time=1 | bin _time span=5m`,
		`index=gradethis | rex "(?<_time>\d+)" | bin _time span=5m`,
		`index=gradethis | rename _time AS observed_at | bin _time span=5m`,
		`index=gradethis | stats count BY _time | bin _time span=5m`,
		`index=gradethis | bin _time span=5m | bin _time span=5m`,
	} {
		_, err := Build(mustParse(t, source), testScope([]string{"gradethis"}, nil))
		assertDiagnosticCode(t, err, "SPL_UNSUPPORTED_BIN_TIME_FIELD")
	}
}

func TestBuildTimeBinInvalidatesCanonicalTimeForTimechart(t *testing.T) {
	t.Parallel()

	_, err := Build(
		mustParse(t, `index=gradethis | bin _time span=5m | timechart span=5m count BY level`),
		testScope([]string{"gradethis"}, nil),
	)
	assertDiagnosticCode(t, err, "SPL_UNSUPPORTED_TIMECHART_TIME_FIELD")
}

func TestBuildTimeBinBoundsFixedSpan(t *testing.T) {
	t.Parallel()

	logical, err := Build(
		mustParse(t, `index=gradethis | bin _time span=86399s`),
		testScope([]string{"gradethis"}, nil),
	)
	if err != nil {
		t.Fatalf("Build(maximum sub-day span): %v", err)
	}
	if got := logical.Operators[len(logical.Operators)-1].(*TimeBucket).Span; got != 86_399*time.Second {
		t.Fatalf("maximum sub-day span = %v, want 86399s", got)
	}

	for _, source := range []string{
		`index=gradethis | bin _time span=86400s`,
		`index=gradethis | bin _time span=1440m`,
		`index=gradethis | bin _time span=24h`,
		`index=gradethis | bin _time span=86401s`,
		`index=gradethis | bucket span=25h _time`,
	} {
		_, err := Build(mustParse(t, source), testScope([]string{"gradethis"}, nil))
		assertDiagnosticCode(t, err, "SPL_UNSUPPORTED_BIN_SYNTAX")
	}
}

func TestBuildRejectsForgedTimeBinAST(t *testing.T) {
	t.Parallel()

	parsed := mustParse(t, `index=gradethis | bin _time span=5m`)
	command := parsed.Commands[0].(*spl.BinCommand)
	command.Field = "status"
	_, err := Build(parsed, testScope([]string{"gradethis"}, nil))
	assertDiagnosticCode(t, err, "SPL_UNSUPPORTED_BIN_TIME_FIELD")
}

func TestBuildRejectsForgedNumericBinAST(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		mutate func(*spl.BinCommand)
		code   string
	}{
		{
			name: "zero span",
			mutate: func(command *spl.BinCommand) {
				command.Span.Magnitude = 0
			},
			code: "SPL_NUMBER_OUT_OF_RANGE",
		},
		{
			name: "span exceeds exact numeric range",
			mutate: func(command *spl.BinCommand) {
				command.Span.Magnitude = MaximumNumericBinSpan + 1
			},
			code: "SPL_NUMBER_OUT_OF_RANGE",
		},
		{
			name: "numeric span carries time unit",
			mutate: func(command *spl.BinCommand) {
				command.Span.Unit = spl.TimeSpanUnitSecond
			},
			code: "SPL_UNSUPPORTED_BIN_SYNTAX",
		},
		{
			name: "invalid span kind",
			mutate: func(command *spl.BinCommand) {
				command.Span.Kind = spl.BinSpanKindInvalid
			},
			code: "SPL_UNSUPPORTED_BIN_SYNTAX",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			parsed := mustParse(t, `index=gradethis | bin severity span=10`)
			command := parsed.Commands[0].(*spl.BinCommand)
			test.mutate(command)
			_, err := Build(parsed, testScope([]string{"gradethis"}, nil))
			assertDiagnosticCode(t, err, test.code)
		})
	}
}

func TestBuildTimechartProducesBoundedRuntimeWideSchema(t *testing.T) {
	t.Parallel()

	scope := testScope([]string{"gradethis"}, nil)
	scope.Earliest = time.Date(2026, 7, 21, 8, 2, 30, 0, time.UTC)
	scope.Latest = time.Date(2026, 7, 21, 8, 12, 0, 1, time.UTC)
	logical, err := Build(
		mustParse(t, `index=gradethis | timechart span=5m count by level`),
		scope,
	)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	if len(logical.Operators) != 3 {
		t.Fatalf("operator count = %d, want Scan, Filter, Timechart", len(logical.Operators))
	}
	operator, ok := logical.Operators[2].(*Timechart)
	if !ok {
		t.Fatalf("last operator = %T, want *Timechart", logical.Operators[2])
	}
	if operator.Time.Name != "_time" || !operator.Time.Canonical || operator.Split == nil ||
		operator.Split.Field.Name != "level" ||
		operator.Measure.Function != AggregateFunctionCountRows ||
		operator.Measure.Output != "count" || operator.Span != 5*time.Minute ||
		!operator.FirstBucket.Equal(time.Date(2026, 7, 21, 8, 0, 0, 0, time.UTC)) || operator.BucketCount != 3 ||
		operator.Split.SeriesLimit != 10 || !operator.Split.IncludeNull || operator.Split.NullLabel != "NULL" ||
		!operator.Split.IncludeOther || operator.Split.OtherLabel != "OTHER" || !operator.FixedRange ||
		!operator.Continuous || !operator.IncludePartial {
		t.Fatalf("timechart = %#v", operator)
	}
	if len(logical.OutputFields) != 0 {
		t.Fatalf("static output fields = %v, want runtime schema", logical.OutputFields)
	}
	if logical.DynamicOutput == nil || !slices.Equal(logical.DynamicOutput.FixedFields, []string{"_time"}) ||
		logical.DynamicOutput.MaxSeries != 12 {
		t.Fatalf("dynamic output = %#v", logical.DynamicOutput)
	}
}

func TestBuildTimechartSeriesOptionsNarrowTheSplitAndRuntimeAllowance(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		source       string
		seriesLimit  uint16
		includeNull  bool
		includeOther bool
		maxSeries    uint16
	}{
		{
			name:         "defaults",
			source:       `index=gradethis | timechart span=5m count by level`,
			seriesLimit:  10,
			includeNull:  true,
			includeOther: true,
			maxSeries:    12,
		},
		{
			name:         "limit only",
			source:       `index=gradethis | timechart span=5m limit=3 count by level`,
			seriesLimit:  3,
			includeNull:  true,
			includeOther: true,
			maxSeries:    5,
		},
		{
			name:         "limit without other",
			source:       `index=gradethis | timechart span=5m count by level limit=5 useother=false`,
			seriesLimit:  5,
			includeNull:  true,
			includeOther: false,
			maxSeries:    6,
		},
		{
			name:         "ordinary series only",
			source:       `index=gradethis | timechart span=5m usenull=false sum(bytes) by level limit=1 useother=false`,
			seriesLimit:  1,
			includeNull:  false,
			includeOther: false,
			maxSeries:    1,
		},
		{
			name:         "explicit defaults",
			source:       `index=gradethis | timechart span=5m limit=10 count by level useother=true usenull=true`,
			seriesLimit:  10,
			includeNull:  true,
			includeOther: true,
			maxSeries:    12,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			logical, err := Build(mustParse(t, test.source), testScope([]string{"gradethis"}, nil))
			if err != nil {
				t.Fatalf("Build: %v", err)
			}
			operator, ok := logical.Operators[len(logical.Operators)-1].(*Timechart)
			if !ok || operator.Split == nil {
				t.Fatalf("last operator = %#v, want split timechart", logical.Operators[len(logical.Operators)-1])
			}
			if operator.Split.SeriesLimit != test.seriesLimit ||
				operator.Split.IncludeNull != test.includeNull ||
				operator.Split.IncludeOther != test.includeOther ||
				operator.Split.NullLabel != "NULL" || operator.Split.OtherLabel != "OTHER" {
				t.Fatalf("split = %#v", operator.Split)
			}
			if logical.DynamicOutput == nil ||
				!slices.Equal(logical.DynamicOutput.FixedFields, []string{"_time"}) ||
				logical.DynamicOutput.MaxSeries != test.maxSeries {
				t.Fatalf("dynamic output = %#v, want max series %d", logical.DynamicOutput, test.maxSeries)
			}
			if _, err := Analyze(logical); err != nil {
				t.Fatalf("Analyze: %v", err)
			}
		})
	}
}

func TestBuildTimechartRejectsForgedSeriesOptions(t *testing.T) {
	t.Parallel()

	base := mustParse(t, `index=gradethis | timechart span=5m count by level`)
	command := base.Commands[0].(*spl.TimechartCommand)
	optionRange := spl.Range{
		Start: spl.Position{Offset: 1, Line: 1, Column: 2},
		End:   spl.Position{Offset: 8, Line: 1, Column: 9},
	}
	tests := []struct {
		name    string
		options spl.TimechartOptions
		split   *spl.StatsGroupField
		code    string
	}{
		{name: "zero limit", options: spl.TimechartOptions{Limit: 0, LimitSpecified: true, LimitRange: optionRange}, split: command.SplitBy, code: "SPL_UNSUPPORTED_TIMECHART_LIMIT"},
		{name: "limit above maximum", options: spl.TimechartOptions{Limit: 11, LimitSpecified: true, LimitRange: optionRange}, split: command.SplitBy, code: "SPL_UNSUPPORTED_TIMECHART_LIMIT"},
		{name: "limit without range", options: spl.TimechartOptions{Limit: 5, LimitSpecified: true}, split: command.SplitBy, code: "SPL_UNSUPPORTED_TIMECHART_SYNTAX"},
		{name: "unspecified limit with value", options: spl.TimechartOptions{Limit: 5}, split: command.SplitBy, code: "SPL_UNSUPPORTED_TIMECHART_SYNTAX"},
		{name: "useother without range", options: spl.TimechartOptions{UseOtherSpecified: true}, split: command.SplitBy, code: "SPL_UNSUPPORTED_TIMECHART_SYNTAX"},
		{name: "unspecified useother with value", options: spl.TimechartOptions{UseOther: true}, split: command.SplitBy, code: "SPL_UNSUPPORTED_TIMECHART_SYNTAX"},
		{name: "usenull without range", options: spl.TimechartOptions{UseNullSpecified: true}, split: command.SplitBy, code: "SPL_UNSUPPORTED_TIMECHART_SYNTAX"},
		{name: "unspecified usenull with range", options: spl.TimechartOptions{UseNullRange: optionRange}, split: command.SplitBy, code: "SPL_UNSUPPORTED_TIMECHART_SYNTAX"},
		{name: "options without split", options: spl.TimechartOptions{Limit: 5, LimitSpecified: true, LimitRange: optionRange}, code: "SPL_UNSUPPORTED_TIMECHART_SYNTAX"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			forged := *command
			forged.Options = test.options
			forged.SplitBy = test.split
			query := &spl.Query{
				Search:   base.Search,
				Commands: []spl.Command{&forged},
				Range:    base.Range,
			}
			_, err := Build(query, testScope([]string{"gradethis"}, nil))
			assertDiagnosticCode(t, err, test.code)
		})
	}
}

func TestBuildTimechartWithoutSplitProducesStaticCountSchema(t *testing.T) {
	t.Parallel()

	scope := testScope([]string{"gradethis"}, nil)
	scope.Earliest = time.Date(2026, 7, 21, 8, 2, 30, 0, time.UTC)
	scope.Latest = time.Date(2026, 7, 21, 8, 12, 0, 1, time.UTC)
	logical, err := Build(
		mustParse(t, `index=gradethis | timechart span=5m count`),
		scope,
	)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	if len(logical.Operators) != 3 {
		t.Fatalf("operator count = %d, want Scan, Filter, Timechart", len(logical.Operators))
	}
	operator, ok := logical.Operators[2].(*Timechart)
	if !ok {
		t.Fatalf("last operator = %T, want *Timechart", logical.Operators[2])
	}
	if operator.Time.Name != "_time" ||
		!operator.Time.Canonical ||
		operator.Split != nil ||
		operator.Measure.Function != AggregateFunctionCountRows ||
		operator.Measure.Output != "count" ||
		operator.Span != 5*time.Minute ||
		!operator.FirstBucket.Equal(time.Date(2026, 7, 21, 8, 0, 0, 0, time.UTC)) ||
		operator.BucketCount != 3 ||
		!operator.FixedRange ||
		!operator.Continuous ||
		!operator.IncludePartial {
		t.Fatalf("timechart = %#v", operator)
	}
	if !slices.Equal(logical.OutputFields, []string{"_time", "count"}) {
		t.Fatalf("static output fields = %v, want _time/count", logical.OutputFields)
	}
	if logical.DynamicOutput != nil {
		t.Fatalf("dynamic output = %#v, want nil", logical.DynamicOutput)
	}
}

func TestBuildTimechartBucketsPreEpochWithHalfOpenLatest(t *testing.T) {
	t.Parallel()

	scope := testScope([]string{"gradethis"}, nil)
	scope.Earliest = time.Date(1969, 12, 31, 23, 57, 0, 500, time.UTC)
	scope.Latest = time.Date(1970, 1, 1, 0, 5, 0, 0, time.UTC)
	logical, err := Build(mustParse(t, `index=gradethis | timechart span=5m count by level`), scope)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	operator := logical.Operators[len(logical.Operators)-1].(*Timechart)
	if !operator.FirstBucket.Equal(time.Date(1969, 12, 31, 23, 55, 0, 0, time.UTC)) || operator.BucketCount != 2 {
		t.Fatalf("bucket start/count = %v/%d, want 1969-12-31T23:55Z/2", operator.FirstBucket, operator.BucketCount)
	}
}

func TestBuildTimechartBucketsAtStorageLowerBoundWithSevenHourSpan(t *testing.T) {
	t.Parallel()

	scope := testScope([]string{"gradethis"}, nil)
	scope.Earliest = time.Date(1900, 1, 1, 0, 0, 0, 0, time.UTC)
	scope.Latest = time.Date(1900, 1, 1, 1, 0, 0, 0, time.UTC)
	logical, err := Build(mustParse(t, `index=gradethis | timechart span=7h count by level`), scope)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	operator := logical.Operators[len(logical.Operators)-1].(*Timechart)
	wantFirstBucket := time.Date(1899, 12, 31, 19, 0, 0, 0, time.UTC)
	if !operator.FirstBucket.Equal(wantFirstBucket) || operator.BucketCount != 1 {
		t.Fatalf("bucket start/count = %v/%d, want %v/1", operator.FirstBucket, operator.BucketCount, wantFirstBucket)
	}
}

func TestBuildTimechartBoundsFixedRangeBucketCount(t *testing.T) {
	t.Parallel()

	scope := testScope([]string{"gradethis"}, nil)
	scope.Earliest = time.Unix(0, 0).UTC()
	scope.Latest = scope.Earliest.Add(maxTimechartBuckets * time.Second)
	logical, err := Build(mustParse(t, `index=gradethis | timechart span=1s count by level`), scope)
	if err != nil {
		t.Fatalf("Build(exact bucket limit): %v", err)
	}
	if got := logical.Operators[len(logical.Operators)-1].(*Timechart).BucketCount; got != maxTimechartBuckets {
		t.Fatalf("bucket count = %d, want %d", got, maxTimechartBuckets)
	}

	scope.Latest = scope.Latest.Add(time.Nanosecond)
	_, err = Build(mustParse(t, `index=gradethis | timechart span=1s count by level`), scope)
	assertDiagnosticCode(t, err, "SPL_QUERY_TOO_COMPLEX")
}

func TestBuildTimechartBoundsFixedSpan(t *testing.T) {
	t.Parallel()

	for _, source := range []string{
		`index=gradethis | timechart span=86401s count by level`,
		`index=gradethis | timechart span=25h count by level`,
	} {
		_, err := Build(mustParse(t, source), testScope([]string{"gradethis"}, nil))
		diagnostic := &Diagnostic{}
		ok := errors.As(err, &diagnostic)
		if !ok || diagnostic.Code != "SPL_UNSUPPORTED_TIMECHART_SYNTAX" {
			t.Errorf("Build(%q) error = %#v, want bounded-span diagnostic", source, err)
		}
	}
}

func TestBuildRequiresTimechartToBeTerminal(t *testing.T) {
	t.Parallel()

	query := mustParse(t, `index=gradethis | timechart span=5m count by level | search index=secret`)
	_, err := Build(query, testScope([]string{"gradethis"}, nil))
	assertDiagnosticCode(t, err, "SPL_UNSUPPORTED_TIMECHART_PIPELINE")
	diagnostic := func() *Diagnostic {
		target := &Diagnostic{}
		_ = errors.As(err, &target)
		return target
	}()
	if got := query.Commands[1].SourceRange(); diagnostic.Range != got {
		t.Fatalf("diagnostic range = %#v, want next command %#v", diagnostic.Range, got)
	}
}

func TestBuildTimechartRequiresUnmodifiedCanonicalTime(t *testing.T) {
	t.Parallel()

	for _, source := range []string{
		`index=gradethis | fields - _time | timechart span=5m count by level`,
		`index=gradethis | table level | timechart span=5m count by level`,
		`index=gradethis | eval _time=1 | timechart span=5m count by level`,
		`index=gradethis | stats count by level | timechart span=5m count by level`,
		`index=gradethis | top level | timechart span=5m count by level`,
	} {
		_, err := Build(mustParse(t, source), testScope([]string{"gradethis"}, nil))
		diagnostic := &Diagnostic{}
		ok := errors.As(err, &diagnostic)
		if !ok || diagnostic.Code != "SPL_UNSUPPORTED_TIMECHART_TIME_FIELD" {
			t.Errorf("Build(%q) error = %#v, want canonical-time diagnostic", source, err)
		}
	}
}

func TestBuildTimechartResolvesNestedSplitField(t *testing.T) {
	t.Parallel()

	logical, err := Build(
		mustParse(t, `index=gradethis | timechart span=1h count by http.route`),
		testScope([]string{"gradethis"}, nil),
	)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	operator := logical.Operators[len(logical.Operators)-1].(*Timechart)
	if operator.Split == nil ||
		!slices.Equal(operator.Split.Field.Path, []string{"http", "route"}) {
		t.Fatalf("split field = %#v", operator.Split)
	}
}

func TestBuildChartProducesBoundedRuntimeWidePivot(t *testing.T) {
	t.Parallel()

	logical, err := Build(
		mustParse(t, `index=gradethis | chart count over path by status_class`),
		testScope([]string{"gradethis"}, nil),
	)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	if len(logical.Operators) != 3 {
		t.Fatalf("operator count = %d, want Scan, Filter, Chart", len(logical.Operators))
	}
	operator, ok := logical.Operators[2].(*Chart)
	if !ok {
		t.Fatalf("last operator = %T, want *Chart", logical.Operators[2])
	}
	if operator.Over.Name != "path" || operator.Over.Canonical || operator.SplitBy.Name != "status_class" ||
		operator.Measure.Function != AggregateFunctionCountRows || operator.Measure.Output != "count" ||
		operator.RowLimit != maxChartRows ||
		operator.SeriesLimit != chartSeriesLimit || !operator.IncludeNull || operator.NullLabel != "NULL" ||
		!operator.IncludeOther || operator.OtherLabel != "OTHER" {
		t.Fatalf("chart = %#v", operator)
	}
	if operator.LogicalName() != "Chart" || operator.SourceRange() != operator.Range {
		t.Fatalf("operator identity = %q/%#v", operator.LogicalName(), operator.SourceRange())
	}
	if len(logical.OutputFields) != 0 {
		t.Fatalf("static output fields = %v, want runtime schema", logical.OutputFields)
	}
	if logical.DynamicOutput == nil || !slices.Equal(logical.DynamicOutput.FixedFields, []string{"path"}) ||
		logical.DynamicOutput.MaxSeries != maxChartSeries {
		t.Fatalf("dynamic output = %#v", logical.DynamicOutput)
	}
}

func TestBuildChartBoundsAreDerivedFromDocumentedDefaults(t *testing.T) {
	t.Parallel()

	if maxChartSeries != chartSeriesLimit+2 {
		t.Fatalf("series bound = %d, want the documented top-10 limit plus NULL and OTHER", maxChartSeries)
	}
	if maxChartRows != 10_000 {
		t.Fatalf("row bound = %d, want the 10,000-row resource policy", maxChartRows)
	}
}

func TestBuildChartSpellingsProduceIdenticalPlans(t *testing.T) {
	t.Parallel()

	overForm, err := Build(
		mustParse(t, `index=gradethis | chart count over path by level`),
		testScope([]string{"gradethis"}, nil),
	)
	if err != nil {
		t.Fatalf("Build(OVER form): %v", err)
	}
	byForm, err := Build(
		mustParse(t, `index=gradethis | chart count by path, level`),
		testScope([]string{"gradethis"}, nil),
	)
	if err != nil {
		t.Fatalf("Build(BY form): %v", err)
	}
	// Both spellings must lower to the same executable pivot; only source
	// provenance is allowed to differ.
	stripRanges := func(operator *Chart) *Chart {
		stripped := *operator
		stripped.Range = spl.Range{}
		stripped.Over.Range = spl.Range{}
		stripped.SplitBy.Range = spl.Range{}
		return &stripped
	}
	over := stripRanges(overForm.Operators[len(overForm.Operators)-1].(*Chart))
	by := stripRanges(byForm.Operators[len(byForm.Operators)-1].(*Chart))
	if !reflect.DeepEqual(over, by) {
		t.Fatalf("chart operators differ:\n%#v\n%#v", over, by)
	}
	if !slices.Equal(overForm.DynamicOutput.FixedFields, byForm.DynamicOutput.FixedFields) ||
		overForm.DynamicOutput.MaxSeries != byForm.DynamicOutput.MaxSeries {
		t.Fatalf("dynamic output differs: %#v vs %#v", overForm.DynamicOutput, byForm.DynamicOutput)
	}
}

func TestBuildRequiresChartToBeTerminal(t *testing.T) {
	t.Parallel()

	query := mustParse(t, `index=gradethis | chart count over path by level | head 5`)
	_, err := Build(query, testScope([]string{"gradethis"}, nil))
	assertDiagnosticCode(t, err, "SPL_UNSUPPORTED_CHART_PIPELINE")
	diagnostic := func() *Diagnostic {
		target := &Diagnostic{}
		_ = errors.As(err, &target)
		return target
	}()
	if got := query.Commands[1].SourceRange(); diagnostic.Range != got {
		t.Fatalf("diagnostic range = %#v, want next command %#v", diagnostic.Range, got)
	}
	if !slices.Contains(diagnostic.Suggestions, "move chart to the final pipeline stage") {
		t.Fatalf("suggestions = %v", diagnostic.Suggestions)
	}
}

func TestBuildRejectsChartCombinedWithOtherWideCommands(t *testing.T) {
	t.Parallel()

	tests := []struct {
		source string
		code   string
	}{
		{`index=gradethis | chart count over path by level | chart count over path by level`, "SPL_UNSUPPORTED_CHART_PIPELINE"},
		{`index=gradethis | chart count over path by level | timechart span=5m count by level`, "SPL_UNSUPPORTED_CHART_PIPELINE"},
		{`index=gradethis | chart count over path by level | stats count`, "SPL_UNSUPPORTED_CHART_PIPELINE"},
		{`index=gradethis | timechart span=5m count by level | chart count over path by level`, "SPL_UNSUPPORTED_TIMECHART_PIPELINE"},
	}
	for _, test := range tests {
		t.Run(test.source, func(t *testing.T) {
			t.Parallel()
			_, err := Build(mustParse(t, test.source), testScope([]string{"gradethis"}, nil))
			assertDiagnosticCode(t, err, test.code)
		})
	}
}

func TestBuildChartRejectsReservedSeriesNamesAndConvenienceColumn(t *testing.T) {
	t.Parallel()

	for _, source := range []string{
		`index=gradethis | chart count over NULL by level`,
		`index=gradethis | chart count over path by NULL`,
		`index=gradethis | chart count over OTHER by level`,
		`index=gradethis | chart count over path by OTHER`,
		`index=gradethis | chart count by NULL, OTHER`,
		`index=gradethis | chart count over fields by level`,
		`index=gradethis | chart count over path by fields`,
	} {
		_, err := Build(mustParse(t, source), testScope([]string{"gradethis"}, nil))
		diagnostic := &Diagnostic{}
		ok := errors.As(err, &diagnostic)
		if !ok || diagnostic.Code != "SPL_UNSUPPORTED_CHART_FIELD_TYPE" {
			t.Errorf("Build(%q) error = %#v, want SPL_UNSUPPORTED_CHART_FIELD_TYPE", source, err)
		}
	}
}

func TestBuildChartAcceptsLowerCaseReservedSpellingsAndClosedFieldsColumn(t *testing.T) {
	t.Parallel()

	for _, source := range []string{
		`index=gradethis | chart count over null by other`,
		`index=gradethis | table fields level | chart count over fields by level`,
		`index=gradethis | table fields level | stats count by fields, level | chart count over fields by level`,
	} {
		if _, err := Build(mustParse(t, source), testScope([]string{"gradethis"}, nil)); err != nil {
			t.Errorf("Build(%q): %v", source, err)
		}
	}
}

func TestBuildChartDoesNotRequireCanonicalTime(t *testing.T) {
	t.Parallel()

	for _, source := range []string{
		`index=gradethis | stats count by path, level | chart count over path by level`,
		`index=gradethis | bin severity span=10 | chart count over severity by level`,
		`index=gradethis | bin _time span=5m | chart count over _time by level`,
		`index=gradethis | bin _time span=5m AS bucket_time | chart count over bucket_time by level`,
		`index=gradethis | fields - _time | chart count over path by level`,
		`index=gradethis | table path level | chart count over path by level`,
		`index=gradethis | top level | chart count over level by count`,
	} {
		logical, err := Build(mustParse(t, source), testScope([]string{"gradethis"}, nil))
		if err != nil {
			t.Errorf("Build(%q): %v", source, err)
			continue
		}
		if _, ok := logical.Operators[len(logical.Operators)-1].(*Chart); !ok {
			t.Errorf("Build(%q) last operator = %T, want *Chart", source, logical.Operators[len(logical.Operators)-1])
		}
	}
}

func TestBuildChartResolvesNestedDottedFields(t *testing.T) {
	t.Parallel()

	logical, err := Build(
		mustParse(t, `index=gradethis | chart count over http.route by http.status_class`),
		testScope([]string{"gradethis"}, nil),
	)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	operator := logical.Operators[len(logical.Operators)-1].(*Chart)
	if !slices.Equal(operator.Over.Path, []string{"http", "route"}) ||
		!slices.Equal(operator.SplitBy.Path, []string{"http", "status_class"}) {
		t.Fatalf("resolved paths = %v/%v", operator.Over.Path, operator.SplitBy.Path)
	}
}

func TestBuildChartRejectsPrivateAndOversizedFieldNames(t *testing.T) {
	t.Parallel()

	tests := []struct {
		source string
		code   string
	}{
		{`index=gradethis | chart count over __os_chart_row by level`, "SPL_RESERVED_FIELD"},
		{`index=gradethis | chart count over path by __os_chart_names`, "SPL_RESERVED_FIELD"},
		{
			source: `index=gradethis | chart count over path by ` + strings.Repeat("a", maxFieldPathSegmentBytes+1),
			code:   "SPL_QUERY_TOO_COMPLEX",
		},
	}
	for _, test := range tests {
		_, err := Build(mustParse(t, test.source), testScope([]string{"gradethis"}, nil))
		assertDiagnosticCode(t, err, test.code)
	}
}

func TestBuildRevalidatesForgedChartCommands(t *testing.T) {
	t.Parallel()

	base := mustParse(t, `index=gradethis`)
	fieldRange := spl.Range{
		Start: spl.Position{Offset: 1, Line: 1, Column: 2},
		End:   spl.Position{Offset: 5, Line: 1, Column: 6},
	}
	tests := []struct {
		name    string
		command *spl.ChartCommand
		code    string
	}{
		{
			name: "repeated axis field",
			command: &spl.ChartCommand{
				Aggregate: spl.StatsAggregate{
					Function:   spl.AggregateFunctionCount,
					Alias:      "count",
					Range:      fieldRange,
					AliasRange: fieldRange,
				},
				Over:    spl.StatsGroupField{Name: "path", Range: fieldRange},
				SplitBy: spl.StatsGroupField{Name: "path", Range: fieldRange},
				Range:   fieldRange,
			},
			code: "SPL_DUPLICATE_FIELD",
		},
		{
			name: "unsupported aggregate",
			command: &spl.ChartCommand{
				Aggregate: spl.StatsAggregate{
					Function:   spl.AggregateFunctionMaximum,
					Input:      "bytes",
					InputRange: fieldRange,
					Alias:      "max(bytes)",
					Range:      fieldRange,
					AliasRange: fieldRange,
				},
				Over:    spl.StatsGroupField{Name: "path", Range: fieldRange},
				SplitBy: spl.StatsGroupField{Name: "level", Range: fieldRange},
				Range:   fieldRange,
			},
			code: "SPL_UNSUPPORTED_CHART_AGGREGATE",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			query := &spl.Query{Search: base.Search, Commands: []spl.Command{test.command}, Range: base.Range}
			_, err := Build(query, testScope([]string{"gradethis"}, nil))
			assertDiagnosticCode(t, err, test.code)
		})
	}
}

func TestBuildPostChartIndexFieldDoesNotChangeInputScope(t *testing.T) {
	t.Parallel()

	logical, err := Build(
		mustParse(t, `index=gradethis | chart count over path by level`),
		testScope([]string{"gradethis"}, nil),
	)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	if !slices.Equal(logical.EffectiveIndexes, []string{"gradethis"}) {
		t.Fatalf("effective indexes = %v", logical.EffectiveIndexes)
	}
	// Chart is transforming, so index-scope inference must stop at it even
	// though the terminality rule already rejects the whole pipeline.
	for _, source := range []string{
		`index=gradethis | chart count over path by level | search index=not-an-index`,
		`index=gradethis | chart count over path by level | search index=not-an-index OR index=other`,
	} {
		if references := positiveIndexReferences(mustParse(t, source), nil); len(references) != 1 {
			t.Fatalf("positiveIndexReferences(%q) = %d references, want only the leading base search", source, len(references))
		}
		_, buildErr := Build(mustParse(t, source), testScope([]string{"gradethis"}, nil))
		assertDiagnosticCode(t, buildErr, "SPL_UNSUPPORTED_CHART_PIPELINE")
	}
}

func TestBuildPostTopIndexFieldDoesNotChangeInputScope(t *testing.T) {
	t.Parallel()

	logical, err := Build(
		mustParse(t, `index=gradethis | top message | search index=not-an-index`),
		testScope([]string{"gradethis"}, nil),
	)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	if !slices.Equal(logical.EffectiveIndexes, []string{"gradethis"}) {
		t.Fatalf("effective indexes = %v", logical.EffectiveIndexes)
	}
}

func TestBuildPostRareIndexFieldDoesNotChangeInputScope(t *testing.T) {
	t.Parallel()

	logical, err := Build(
		mustParse(t, `index=gradethis | rare message | search index=not-an-index`),
		testScope([]string{"gradethis"}, nil),
	)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	if !slices.Equal(logical.EffectiveIndexes, []string{"gradethis"}) {
		t.Fatalf("effective indexes = %v", logical.EffectiveIndexes)
	}
}

func TestBuildPostStatsIndexAliasDoesNotChangeInputScope(t *testing.T) {
	t.Parallel()

	logical, err := Build(
		mustParse(t, `index=gradethis | stats count AS index | search index=1`),
		testScope([]string{"gradethis"}, nil),
	)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	if !slices.Equal(logical.EffectiveIndexes, []string{"gradethis"}) || !slices.Equal(logical.OutputFields, []string{"index"}) {
		t.Fatalf("scope/output = %v / %v", logical.EffectiveIndexes, logical.OutputFields)
	}
	if _, ok := logical.Operators[len(logical.Operators)-1].(*Filter); !ok {
		t.Fatalf("last operator = %T, want post-stats Filter", logical.Operators[len(logical.Operators)-1])
	}
}

func TestBuildFieldsUpdatesKnownTransformingSchema(t *testing.T) {
	t.Parallel()

	logical, err := Build(
		mustParse(t, `index=gradethis | stats count AS events by level | fields events`),
		testScope([]string{"gradethis"}, nil),
	)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	if !slices.Equal(logical.OutputFields, []string{"events"}) {
		t.Fatalf("output fields = %v", logical.OutputFields)
	}

	_, err = Build(
		mustParse(t, `index=gradethis | stats count AS events by level | fields - level, events`),
		testScope([]string{"gradethis"}, nil),
	)
	assertDiagnosticCode(t, err, "SPL_EMPTY_PROJECTION")
}

func TestResolveFieldRejectsCompilerPrivateNamespace(t *testing.T) {
	t.Parallel()

	_, err := ResolveField(`__os_sort_time`, spl.Range{})
	assertDiagnosticCode(t, err, "SPL_RESERVED_FIELD")
}

func TestResolveFieldUsesEveryCanonicalSPLField(t *testing.T) {
	t.Parallel()

	for _, name := range eventfields.CanonicalSPLFieldNames() {
		field, err := ResolveField(name, spl.Range{})
		if err != nil {
			t.Errorf("ResolveField(%q): %v", name, err)
			continue
		}
		if !field.Canonical || field.Name != name || field.Path != nil {
			t.Errorf("ResolveField(%q) = %#v, want exact canonical reference", name, field)
		}
		variant := strings.ToUpper(name)
		if variant == name {
			continue
		}
		caseVariant, err := ResolveField(variant, spl.Range{})
		if err != nil {
			t.Errorf("ResolveField(%q): %v", variant, err)
			continue
		}
		if caseVariant.Canonical {
			t.Errorf("ResolveField(%q) became canonical; SPL field names are case-sensitive", variant)
		}
	}
}

func TestResolveFieldSupportsNestedAndEscapedDot(t *testing.T) {
	t.Parallel()

	nested, err := ResolveField(`http.response.status`, spl.Range{})
	if err != nil {
		t.Fatalf("ResolveField nested: %v", err)
	}
	if !slices.Equal(nested.Path, []string{"http", "response", "status"}) {
		t.Fatalf("nested path = %v", nested.Path)
	}
	literalDot, err := ResolveField(`labels.kubernetes\.io/app`, spl.Range{})
	if err != nil {
		t.Fatalf("ResolveField escaped: %v", err)
	}
	if !slices.Equal(literalDot.Path, []string{"labels", "kubernetes.io/app"}) {
		t.Fatalf("escaped path = %v", literalDot.Path)
	}
}

func TestResolveFieldRejectsAmbiguousPaths(t *testing.T) {
	t.Parallel()

	for _, field := range []string{
		"",
		".foo",
		"foo.",
		"foo..bar",
		`foo\q`,
		"foo\x00bar",
		"foo\u0080bar",
	} {
		_, err := ResolveField(field, spl.Range{})
		assertDiagnosticCode(t, err, "SPL_INVALID_FIELD")
	}
}

func TestResolveFieldBoundsDynamicPathSize(t *testing.T) {
	t.Parallel()

	for _, field := range []string{
		strings.Repeat("x", maxFieldNameBytes+1),
		strings.Repeat("x", maxFieldPathSegmentBytes+1),
		strings.Repeat("x.", maxFieldPathSegments) + "x",
	} {
		_, err := ResolveField(field, spl.Range{})
		assertDiagnosticCode(t, err, "SPL_QUERY_TOO_COMPLEX")
	}
	if _, err := ResolveField(strings.Repeat("x", maxFieldPathSegmentBytes), spl.Range{}); err != nil {
		t.Fatalf("ResolveField(exact segment limit): %v", err)
	}
	escapedSegment := strings.Repeat(`\.`, maxFieldPathSegmentBytes)
	maximal := strings.Repeat(escapedSegment+".", maxFieldPathSegments-1) + escapedSegment
	if len(maximal) != maxFieldNameBytes {
		t.Fatalf("maximal escaped field spelling = %d bytes, want %d", len(maximal), maxFieldNameBytes)
	}
	field, err := ResolveField(maximal, spl.Range{})
	if err != nil {
		t.Fatalf("ResolveField(exact 17-segment escaped limit): %v", err)
	}
	if len(field.Path) != maxFieldPathSegments {
		t.Fatalf("maximal escaped field path has %d segments, want %d", len(field.Path), maxFieldPathSegments)
	}
	for index, segment := range field.Path {
		if len(segment) != maxFieldPathSegmentBytes {
			t.Fatalf("maximal escaped field segment %d = %d bytes, want %d", index, len(segment), maxFieldPathSegmentBytes)
		}
	}
	if _, err := ResolveField(strings.Repeat("x", 129), spl.Range{}); err != nil {
		t.Fatalf("ResolveField(ingest-valid 129-byte root): %v", err)
	}
}

func TestBuildRejectsInvalidSnapshot(t *testing.T) {
	t.Parallel()

	scope := testScope([]string{"gradethis"}, nil)
	scope.Latest = scope.Earliest
	_, err := Build(mustParse(t, `index=gradethis`), scope)
	assertDiagnosticCode(t, err, "SPL_INVALID_TIME_RANGE")
}

func TestBuildRequiresResolvedVisibilitySnapshotButAllowsEmptyTable(t *testing.T) {
	t.Parallel()

	scope := testScope([]string{"gradethis"}, nil)
	scope.VisibilityCutoff = nil
	_, err := Build(mustParse(t, `index=gradethis`), scope)
	assertDiagnosticCode(t, err, "SPL_INVALID_SNAPSHOT")

	emptyCutoff := uint64(0)
	scope.VisibilityCutoff = &emptyCutoff
	logical, err := Build(mustParse(t, `index=gradethis`), scope)
	if err != nil {
		t.Fatalf("Build empty-table snapshot: %v", err)
	}
	if got := logical.Operators[0].(*Scan).VisibilityCutoff; got != 0 {
		t.Fatalf("empty-table cutoff = %d, want 0", got)
	}
}

func testScope(authorized, requested []string) Scope {
	visibilityCutoff := uint64(41)
	return Scope{
		TenantID:          "tenant-1",
		AuthorizedIndexes: authorized,
		RequestedIndexes:  requested,
		Earliest:          time.Date(2026, 7, 21, 1, 0, 0, 0, time.FixedZone("test", -7*60*60)),
		Latest:            time.Date(2026, 7, 21, 2, 0, 0, 0, time.FixedZone("test", -7*60*60)),
		SearchStart:       time.Date(2026, 7, 21, 2, 0, 0, 500_000_000, time.UTC),
		SearchTimezone:    "UTC",
		IndexTimeCutoff:   time.Date(2026, 7, 21, 2, 0, 1, 0, time.UTC),
		VisibilityCutoff:  &visibilityCutoff,
	}
}

func mustParse(t *testing.T, source string) *spl.Query {
	t.Helper()
	query, err := spl.Parse(source)
	if err != nil {
		t.Fatalf("Parse(%q): %v", source, err)
	}
	return query
}

func assertDiagnosticCode(t *testing.T, err error, code string) {
	t.Helper()
	if err == nil {
		t.Fatalf("error is nil, want %s", code)
	}
	diagnostic := &Diagnostic{}
	ok := errors.As(err, &diagnostic)
	if !ok {
		t.Fatalf("error = %T, want *Diagnostic", err)
	}
	if diagnostic.Code != code {
		t.Fatalf("code = %q, want %q", diagnostic.Code, code)
	}
}

func assertForgedEvalBuildDiagnostic(
	t *testing.T,
	base *spl.Query,
	sourceRange spl.Range,
	expression spl.ScalarExpr,
	code string,
) {
	t.Helper()
	query := &spl.Query{
		Search: base.Search,
		Commands: []spl.Command{&spl.EvalCommand{
			Assignments: []spl.EvalAssignment{{
				Field:      "result",
				FieldRange: sourceRange,
				Expression: expression,
				Range:      sourceRange,
			}},
			Range: sourceRange,
		}},
		Range: base.Range,
	}
	_, err := Build(query, testScope([]string{"gradethis"}, nil))
	if err == nil {
		t.Fatalf("Build succeeded for forged expression %#v", expression)
	}
	assertDiagnosticCode(t, err, code)
}
