package clickhouse

import (
	"errors"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/plan"
	"github.com/Suhaibinator/open-splunk/internal/spl"
)

func TestCompileStreamStatsCountUsesDeterministicPipelineOrder(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		prefix         string
		capturePattern string
		orderPattern   string
		minimumOrders  int
	}{
		{
			name:           "stable default event order",
			prefix:         `index=gradethis | table event_id`,
			capturePattern: `"__os_sort_time" AS "__os_streamstats_order_[0-9]+_0", "__os_sort_event_id" AS "__os_streamstats_order_[0-9]+_1", "__os_sort_visibility_seq" AS "__os_streamstats_order_[0-9]+_2", "__os_sort_source_identity" AS "__os_streamstats_order_[0-9]+_3"`,
			orderPattern:   `"__os_streamstats_order_[0-9]+_0" DESC NULLS LAST, "__os_streamstats_order_[0-9]+_1" DESC NULLS LAST, "__os_streamstats_order_[0-9]+_2" DESC NULLS LAST, "__os_streamstats_order_[0-9]+_3" DESC NULLS LAST`,
			minimumOrders:  3,
		},
		{
			name:           "explicit ascending order",
			prefix:         `index=gradethis | table event_id,sequence | sort 0 +sequence`,
			capturePattern: `"__os_order_[0-9]+_0" AS "__os_streamstats_order_[0-9]+_0", "__os_order_[0-9]+_tie_0" AS "__os_streamstats_order_[0-9]+_1", "__os_order_[0-9]+_tie_1" AS "__os_streamstats_order_[0-9]+_2", "__os_order_[0-9]+_tie_2" AS "__os_streamstats_order_[0-9]+_3"`,
			orderPattern:   `"__os_streamstats_order_[0-9]+_0" ASC NULLS LAST, "__os_streamstats_order_[0-9]+_1" DESC NULLS LAST, "__os_streamstats_order_[0-9]+_2" DESC NULLS LAST, "__os_streamstats_order_[0-9]+_3" DESC NULLS LAST`,
			minimumOrders:  3,
		},
		{
			name:           "explicit descending order",
			prefix:         `index=gradethis | table event_id,sequence | sort 0 -sequence`,
			capturePattern: `"__os_order_[0-9]+_0" AS "__os_streamstats_order_[0-9]+_0", "__os_order_[0-9]+_tie_0" AS "__os_streamstats_order_[0-9]+_1", "__os_order_[0-9]+_tie_1" AS "__os_streamstats_order_[0-9]+_2", "__os_order_[0-9]+_tie_2" AS "__os_streamstats_order_[0-9]+_3"`,
			orderPattern:   `"__os_streamstats_order_[0-9]+_0" DESC NULLS LAST, "__os_streamstats_order_[0-9]+_1" DESC NULLS LAST, "__os_streamstats_order_[0-9]+_2" DESC NULLS LAST, "__os_streamstats_order_[0-9]+_3" DESC NULLS LAST`,
			minimumOrders:  3,
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			logical := appendStreamStatsOperator(
				buildPlan(t, test.prefix),
				validStreamStatsOperator(t, "running"),
			)
			compiled, err := (Compiler{}).Compile(logical)
			if err != nil {
				t.Fatalf("Compile(streamstats): %v", err)
			}
			if !slices.Contains(compiled.OutputFields, "running") {
				t.Fatalf("streamstats output fields = %#v, want running", compiled.OutputFields)
			}
			for _, required := range []string{
				`count() OVER (`,
				`ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW`,
				`AS "running"`,
			} {
				if !strings.Contains(compiled.SQL, required) {
					t.Fatalf("streamstats SQL missing %q:\n%s", required, compiled.SQL)
				}
			}
			order := regexp.MustCompile(test.orderPattern)
			if !regexp.MustCompile(test.capturePattern).MatchString(compiled.SQL) {
				t.Fatalf("streamstats did not snapshot its incoming order:\n%s", compiled.SQL)
			}
			if got := len(order.FindAllString(compiled.SQL, -1)); got < test.minimumOrders {
				t.Fatalf(
					"streamstats order %q occurs %d times, want at least %d in window and final publication:\n%s",
					test.orderPattern,
					got,
					test.minimumOrders,
					compiled.SQL,
				)
			}
			assertBoundedStreamStatsSQL(t, compiled)
		})
	}
}

func TestCompileStreamStatsCountPinsCurrentAndWindowFrames(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		current bool
		window  uint64
		frame   string
	}{
		{
			name: "unbounded including current", current: true,
			frame: `ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW`,
		},
		{
			name: "unbounded prior rows", current: false,
			frame: `ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING`,
		},
		{
			name: "one row including current", current: true, window: 1,
			frame: `ROWS BETWEEN CURRENT ROW AND CURRENT ROW`,
		},
		{
			name: "one prior row", current: false, window: 1,
			frame: `ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING`,
		},
		{
			name: "three rows including current", current: true, window: 3,
			frame: `ROWS BETWEEN 2 PRECEDING AND CURRENT ROW`,
		},
		{
			name: "three prior rows", current: false, window: 3,
			frame: `ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING`,
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			operator := validStreamStatsOperator(t, "running")
			operator.IncludeCurrent = test.current
			operator.WindowRows = test.window
			compiled, err := (Compiler{}).Compile(appendStreamStatsOperator(
				buildPlan(t, `index=gradethis | table event_id`),
				operator,
			))
			if err != nil {
				t.Fatalf("Compile(streamstats frame): %v", err)
			}
			if got := strings.Count(compiled.SQL, test.frame); got != 1 {
				t.Fatalf("streamstats frame %q occurs %d times, want one:\n%s", test.frame, got, compiled.SQL)
			}
			assertBoundedStreamStatsSQL(t, compiled)
		})
	}
}

func TestCompileStreamStatsGroupedCountUsesScalarPartitionAndNullablePresence(t *testing.T) {
	t.Parallel()

	operator := validStreamStatsOperator(t, "running")
	operator.GroupBy = []plan.FieldRef{mustResolveStreamStatsField(t, "user")}
	operator.WindowRows = 2
	operator.Global = false
	compiled, err := (Compiler{}).Compile(appendStreamStatsOperator(
		buildPlan(t, `index=gradethis | table event_id,user`),
		operator,
	))
	if err != nil {
		t.Fatalf("Compile(grouped streamstats): %v", err)
	}
	for _, required := range []string{
		`PARTITION BY "__os_streamstats_eligible_`,
		`"__os_streamstats_group_0"`,
		`ROWS BETWEEN 1 PRECEDING AND CURRENT ROW`,
		`CAST(NULL AS Nullable(UInt64))`,
		`"__os_streamstats_exists_`,
		`dynamicType(`,
		UnsupportedStatsByValueMarker,
	} {
		if !strings.Contains(compiled.SQL, required) {
			t.Fatalf("grouped streamstats SQL missing %q:\n%s", required, compiled.SQL)
		}
	}
	if got := strings.Count(compiled.SQL, `AS "running"`); got != 1 {
		t.Fatalf("grouped streamstats publishes running %d times, want once:\n%s", got, compiled.SQL)
	}
	assertBoundedStreamStatsSQL(t, compiled)
}

func TestCompileStreamStatsGroupClassificationKeepsPlaceholderOrder(t *testing.T) {
	t.Parallel()

	operator := validStreamStatsOperator(t, "running")
	operator.GroupBy = []plan.FieldRef{
		mustResolveStreamStatsField(t, "first_group"),
		mustResolveStreamStatsField(t, "second_group"),
	}
	compiled, err := (Compiler{}).Compile(appendStreamStatsOperator(
		buildPlan(t, `index=gradethis`),
		operator,
	))
	if err != nil {
		t.Fatalf("Compile(grouped streamstats): %v", err)
	}
	wantPrefix := []any{
		"first_group", "first_group.",
		"second_group", "second_group.",
	}
	if len(compiled.Args) <= len(wantPrefix) ||
		!slices.Equal(compiled.Args[:len(wantPrefix)], wantPrefix) {
		t.Fatalf("streamstats group argument prefix = %#v, want %#v", compiled.Args, wantPrefix)
	}
	if got := compiled.Args[len(wantPrefix)]; got != "tenant-1" {
		t.Fatalf("first nested scan argument = %#v, want tenant-1", got)
	}
	if got, want := strings.Count(compiled.SQL, "?"), len(compiled.Args); got != want {
		t.Fatalf("placeholder count = %d, args = %d:\nSQL: %s\nargs: %#v", got, want, compiled.SQL, compiled.Args)
	}
}

func TestCompileStreamStatsGuardsInputAndUnsupportedGroupsBeforeDownstreamLimit(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		groupBy  []plan.FieldRef
		marker   string
		guardSQL string
	}{
		{
			name:     "input row overflow",
			marker:   StreamStatsInputLimitMarker,
			guardSQL: `count() OVER () AS "__os_streamstats_input_count_`,
		},
		{
			name:     "unsupported dynamic group",
			groupBy:  []plan.FieldRef{mustResolveStreamStatsField(t, "user")},
			marker:   UnsupportedStatsByValueMarker,
			guardSQL: `max(toUInt8(`,
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			operator := validStreamStatsOperator(t, "discarded")
			operator.GroupBy = test.groupBy
			logical := buildPlan(t, `index=gradethis | head 1 | table event_id`)
			logical.Operators = insertPlanOperator(logical.Operators, 1, operator)
			compiled, err := (Compiler{}).Compile(logical)
			if err != nil {
				t.Fatalf("Compile(guarded streamstats): %v", err)
			}
			sentinel := `LIMIT ` + strconv.FormatUint(MaximumStreamStatsInputRows+1, 10)
			for _, required := range []string{
				` AS MATERIALIZED (`,
				sentinel,
				test.guardSQL,
				test.marker,
			} {
				if !strings.Contains(compiled.SQL, required) {
					t.Fatalf("guarded streamstats SQL missing %q:\n%s", required, compiled.SQL)
				}
			}
			guard := strings.Index(compiled.SQL, test.marker)
			limit := strings.LastIndex(compiled.SQL, `LIMIT ?`)
			if guard < 0 || limit < 0 || guard > limit {
				t.Fatalf("streamstats guard is not evaluated before downstream head:\n%s", compiled.SQL)
			}
			if got := strings.Count(compiled.SQL, ` AS MATERIALIZED (`); got != 1 {
				t.Fatalf("streamstats materialized fences = %d, want one:\n%s", got, compiled.SQL)
			}
			assertBoundedStreamStatsSQL(t, compiled)
		})
	}
}

func TestCompileStreamStatsCapturesOrderBeforeAliasReplacement(t *testing.T) {
	t.Parallel()

	logical := buildPlan(
		t,
		`index=gradethis | table event_id,running | sort 0 +running | where running>1`,
	)
	logical.Operators = insertPlanOperator(
		logical.Operators,
		len(logical.Operators)-1,
		validStreamStatsOperator(t, "running"),
	)
	compiled, err := (Compiler{}).Compile(logical)
	if err != nil {
		t.Fatalf("Compile(streamstats alias replacement): %v", err)
	}
	if got := countString(compiled.OutputFields, "running"); got != 1 {
		t.Fatalf("streamstats replacement output count = %d, fields %#v", got, compiled.OutputFields)
	}
	capture := regexp.MustCompile(`"__os_order_[0-9]+_0" AS "__os_streamstats_order_[0-9]+_0"`)
	windowOrder := regexp.MustCompile(`count\(\) OVER \(ORDER BY "__os_streamstats_order_[0-9]+_0" ASC NULLS LAST, `)
	if !capture.MatchString(compiled.SQL) || !windowOrder.MatchString(compiled.SQL) {
		t.Fatalf("streamstats did not capture the pre-replacement running order:\n%s", compiled.SQL)
	}
	if !strings.Contains(compiled.SQL, `toInt256("running") > accurateCastOrNull(`) {
		t.Fatalf("downstream predicate does not consume the UInt64 streamstats output:\n%s", compiled.SQL)
	}
	assertBoundedStreamStatsSQL(t, compiled)
}

func TestCompileStreamStatsSnapshotsAggregateOrderBeforeReplacingItsAlias(t *testing.T) {
	t.Parallel()

	compiled, err := (Compiler{}).Compile(buildPlan(
		t,
		`index=gradethis | stats count | streamstats count`,
	))
	if err != nil {
		t.Fatalf("Compile(streamstats replacing aggregate order): %v", err)
	}
	if !slices.Equal(compiled.OutputFields, []string{"count"}) {
		t.Fatalf("streamstats replacement fields = %#v, want [count]", compiled.OutputFields)
	}
	for _, required := range []*regexp.Regexp{
		regexp.MustCompile(`"count" AS "__os_streamstats_order_[0-9]+_0"`),
		regexp.MustCompile(`count\(\) OVER \(ORDER BY "__os_streamstats_order_[0-9]+_0" ASC NULLS LAST `),
		regexp.MustCompile(`ORDER BY "__os_streamstats_order_[0-9]+_0" ASC NULLS LAST`),
	} {
		if !required.MatchString(compiled.SQL) {
			t.Fatalf("streamstats aggregate-order snapshot missing %q:\n%s", required, compiled.SQL)
		}
	}
	if strings.Contains(compiled.SQL, `AS "count", "count"`) {
		t.Fatalf("streamstats re-appended the replaced public order alias:\n%s", compiled.SQL)
	}
	assertBoundedStreamStatsSQL(t, compiled)
}

func TestCompileStreamStatsPreservesSnapshottedTieBreakersForDownstreamSort(t *testing.T) {
	t.Parallel()

	compiled, err := (Compiler{}).Compile(buildPlan(
		t,
		`index=gradethis | table _time,event_id,host | streamstats count AS running | sort 0 +host`,
	))
	if err != nil {
		t.Fatalf("Compile(streamstats followed by sort): %v", err)
	}

	tieCapture := regexp.MustCompile(
		`"__os_sort_event_id" AS "__os_streamstats_tie_breaker_[0-9]+_0", ` +
			`"__os_sort_visibility_seq" AS "__os_streamstats_tie_breaker_[0-9]+_1", ` +
			`"__os_sort_source_identity" AS "__os_streamstats_tie_breaker_[0-9]+_2"`,
	)
	downstreamProjection := regexp.MustCompile(
		`"host" AS "__os_order_[0-9]+_0", ` +
			`"__os_streamstats_tie_breaker_[0-9]+_0" AS "__os_order_[0-9]+_tie_0", ` +
			`"__os_streamstats_tie_breaker_[0-9]+_1" AS "__os_order_[0-9]+_tie_1", ` +
			`"__os_streamstats_tie_breaker_[0-9]+_2" AS "__os_order_[0-9]+_tie_2"`,
	)
	downstreamOrder := regexp.MustCompile(
		`"__os_order_[0-9]+_0" ASC NULLS LAST, ` +
			`"__os_order_[0-9]+_tie_0" DESC NULLS LAST, ` +
			`"__os_order_[0-9]+_tie_1" DESC NULLS LAST, ` +
			`"__os_order_[0-9]+_tie_2" DESC NULLS LAST`,
	)
	for description, pattern := range map[string]*regexp.Regexp{
		"streamstats tie-breaker snapshot": tieCapture,
		"downstream tie projection":        downstreamProjection,
		"downstream stable order":          downstreamOrder,
	} {
		if !pattern.MatchString(compiled.SQL) {
			t.Fatalf("%s missing %q:\n%s", description, pattern, compiled.SQL)
		}
	}
	assertBoundedStreamStatsSQL(t, compiled)
}

func TestCompileFieldSummaryConsumesStreamStatsBarrier(t *testing.T) {
	t.Parallel()

	spec := fieldSummaryTestSpec("prior")
	compiled, err := (Compiler{}).CompileFieldSummary(
		buildPlan(t, streamStatsAnalysisSource),
		spec,
	)
	if err != nil {
		t.Fatalf("CompileFieldSummary(streamstats): %v", err)
	}
	if !compiled.FieldKnown {
		t.Fatal("streamstats output prior is not known to field summary")
	}
	if compiled.Spec != spec {
		t.Fatalf("field summary spec = %#v, want %#v", compiled.Spec, spec)
	}
	for _, required := range []string{
		quoteIdentifier(FieldSummaryRowKindColumn),
		quoteIdentifier(FieldSummaryTotalEventCountColumn),
	} {
		if !strings.Contains(compiled.SQL, required) {
			t.Fatalf("field summary SQL missing %q:\n%s", required, compiled.SQL)
		}
	}
	if len(compiled.Args) == 0 {
		t.Fatal("field summary compiled without arguments")
	}
	if got := compiled.Args[len(compiled.Args)-1]; got != spec.FieldName {
		t.Fatalf("last field summary argument = %#v, want %q", got, spec.FieldName)
	}
	assertStreamStatsAnalysisBarrier(t, compiled.SQL, compiled.Args)
}

func TestCompileTimelineConsumesStreamStatsBarrier(t *testing.T) {
	t.Parallel()

	spec := validTimelineSpec()
	compiled, err := (Compiler{}).CompileTimeline(
		buildPlan(t, streamStatsAnalysisSource),
		spec,
	)
	if err != nil {
		t.Fatalf("CompileTimeline(streamstats): %v", err)
	}
	if compiled.Spec != spec {
		t.Fatalf("timeline spec = %#v, want %#v", compiled.Spec, spec)
	}
	for _, required := range []string{
		quoteIdentifier(TimelineOrdinalColumn),
		quoteIdentifier(TimelineCountColumn),
	} {
		if !strings.Contains(compiled.SQL, required) {
			t.Fatalf("timeline SQL missing %q:\n%s", required, compiled.SQL)
		}
	}
	firstBucketNumber, ok := ordinalGridFirstBucketNumber(
		spec.FirstBucket.Unix(),
		spec.SpanSeconds,
		spec.BucketCount,
	)
	if !ok {
		t.Fatal("valid timeline spec did not produce an ordinal grid")
	}
	spanNanoseconds := spec.SpanSeconds * 1_000_000_000
	wantTail := []any{
		spanNanoseconds,
		spanNanoseconds,
		firstBucketNumber,
		spec.BucketCount,
	}
	if len(compiled.Args) < len(wantTail) ||
		!slices.Equal(compiled.Args[len(compiled.Args)-len(wantTail):], wantTail) {
		t.Fatalf("timeline argument tail = %#v, want %#v", compiled.Args, wantTail)
	}
	assertStreamStatsAnalysisBarrier(t, compiled.SQL, compiled.Args)
}

func TestCompileStreamStatsCountsTransformedRowsWithoutRescanningEvents(t *testing.T) {
	t.Parallel()

	logical := appendStreamStatsOperator(
		buildPlan(
			t,
			`index=gradethis | stats count AS events BY service | sort 0 +service`,
		),
		validStreamStatsOperator(t, "running"),
	)
	compiled, err := (Compiler{}).Compile(logical)
	if err != nil {
		t.Fatalf("Compile(streamstats over stats): %v", err)
	}
	if !slices.Equal(compiled.OutputFields, []string{"service", "events", "running"}) {
		t.Fatalf("transformed streamstats fields = %#v", compiled.OutputFields)
	}
	for _, required := range []string{
		`GROUP BY "service"`,
		`count() AS "events"`,
		`count() OVER (ORDER BY "__os_streamstats_order_`,
		`AS "running"`,
	} {
		if !strings.Contains(compiled.SQL, required) {
			t.Fatalf("transformed streamstats SQL missing %q:\n%s", required, compiled.SQL)
		}
	}
	assertBoundedStreamStatsSQL(t, compiled)
}

func TestCompileStreamStatsRejectsFixedMultivalueGroups(t *testing.T) {
	t.Parallel()

	operator := validStreamStatsOperator(t, "running")
	operator.GroupBy = []plan.FieldRef{mustResolveStreamStatsField(t, "hosts")}
	logical := appendStreamStatsOperator(
		buildPlan(t, `index=gradethis | stats values(host) AS hosts`),
		operator,
	)
	_, err := (Compiler{}).Compile(logical)
	var diagnostic *plan.Diagnostic
	if !errors.As(err, &diagnostic) ||
		diagnostic.Code != "SPL_UNSUPPORTED_MULTIVALUE_USAGE" ||
		!strings.Contains(diagnostic.Message, "streamstats BY") {
		t.Fatalf("Compile(multivalue streamstats BY) error = %#v", err)
	}
}

func TestCompileStreamStatsDefensivelyRejectsForgedOperators(t *testing.T) {
	t.Parallel()

	validGroup := mustResolveStreamStatsField(t, "host")
	tests := []struct {
		name   string
		mutate func(*plan.StreamAggregate)
	}{
		{name: "missing output", mutate: func(operator *plan.StreamAggregate) { operator.Measure.Output = "" }},
		{name: "wrong function", mutate: func(operator *plan.StreamAggregate) {
			operator.Measure.Function = plan.AggregateFunctionCountValues
		}},
		{name: "input metadata", mutate: func(operator *plan.StreamAggregate) {
			operator.Measure.Input = validGroup
		}},
		{name: "empty input path metadata", mutate: func(operator *plan.StreamAggregate) {
			operator.Measure.Input.Path = []string{}
		}},
		{name: "predicate metadata", mutate: func(operator *plan.StreamAggregate) {
			operator.Measure.Predicate = &plan.ComparisonExpression{
				Field: validGroup,
				Op:    plan.ComparisonOpEqual,
				Value: plan.Value{Kind: plan.ValueKindString, String: "x"},
			}
		}},
		{name: "percentile metadata", mutate: func(operator *plan.StreamAggregate) {
			operator.Measure.Percentile = 50
		}},
		{name: "window above stage bound", mutate: func(operator *plan.StreamAggregate) {
			operator.WindowRows = spl.MaximumStreamStatsWindow + 1
		}},
		{name: "grouped bounded global window", mutate: func(operator *plan.StreamAggregate) {
			operator.GroupBy = []plan.FieldRef{validGroup}
			operator.WindowRows = 2
			operator.Global = true
		}},
		{name: "duplicate group", mutate: func(operator *plan.StreamAggregate) {
			operator.GroupBy = []plan.FieldRef{validGroup, validGroup}
		}},
		{name: "malformed group", mutate: func(operator *plan.StreamAggregate) {
			operator.GroupBy = []plan.FieldRef{{Name: "host"}}
		}},
		{name: "private output", mutate: func(operator *plan.StreamAggregate) {
			operator.Measure.Output = "__os_streamstats_private"
		}},
		{name: "single quoted output", mutate: func(operator *plan.StreamAggregate) {
			operator.Measure.Output = "'running'"
		}},
		{name: "backtick quoted output", mutate: func(operator *plan.StreamAggregate) {
			operator.Measure.Output = "`running`"
		}},
		{name: "single quoted group", mutate: func(operator *plan.StreamAggregate) {
			operator.GroupBy = []plan.FieldRef{mustResolveStreamStatsField(t, "'host'")}
		}},
		{name: "backtick quoted group", mutate: func(operator *plan.StreamAggregate) {
			operator.GroupBy = []plan.FieldRef{mustResolveStreamStatsField(t, "`host`")}
		}},
		{name: "too many groups", mutate: func(operator *plan.StreamAggregate) {
			operator.GroupBy = make([]plan.FieldRef, spl.MaximumStatsGroupFields+1)
			for index := range operator.GroupBy {
				operator.GroupBy[index] = mustResolveStreamStatsField(
					t,
					"group_"+strconv.Itoa(index),
				)
			}
		}},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			operator := validStreamStatsOperator(t, "running")
			test.mutate(operator)
			logical := appendStreamStatsOperator(buildPlan(t, `index=gradethis`), operator)
			if _, err := (Compiler{}).Compile(logical); err == nil {
				t.Fatal("Compile() accepted a forged streamstats operator")
			}
		})
	}

	var typedNil *plan.StreamAggregate
	logical := buildPlan(t, `index=gradethis`)
	logical.Operators = append(logical.Operators, typedNil)
	if _, err := (Compiler{}).Compile(logical); err == nil {
		t.Fatal("Compile() accepted a typed-nil streamstats operator")
	}
}

func assertBoundedStreamStatsSQL(t *testing.T, compiled CompiledQuery) {
	t.Helper()

	if got := strings.Count(compiled.SQL, `FROM "open_splunk"."events"`); got != 1 {
		t.Fatalf("streamstats physical event scans = %d, want one:\n%s", got, compiled.SQL)
	}
	for _, forbidden := range []string{
		"ARRAY JOIN",
		"arrayJoin(",
		"groupArray(",
		"groupArrayArray(",
		"groupArraySortedArray(",
		"groupUniqArray",
	} {
		if strings.Contains(compiled.SQL, forbidden) {
			t.Fatalf("streamstats SQL contains unbounded or row-expanding %q:\n%s", forbidden, compiled.SQL)
		}
	}
	if got, want := strings.Count(compiled.SQL, "?"), len(compiled.Args); got != want {
		t.Fatalf("streamstats placeholders = %d, args = %d:\nSQL: %s\nargs: %#v", got, want, compiled.SQL, compiled.Args)
	}
}

const streamStatsAnalysisSource = `index=gradethis | table _time,event_id,user,status | sort 0 +event_id | streamstats current=false count(status) AS prior BY user`

func assertStreamStatsAnalysisBarrier(t *testing.T, sql string, args []any) {
	t.Helper()

	for _, required := range []string{
		`"__os_streamstats_input_`,
		`"__os_streamstats_result_`,
		`sum(toUInt128("__os_streamstats_measure_`,
		`OVER (PARTITION BY "__os_streamstats_eligible_`,
		`ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING`,
		`AS "prior"`,
		StreamStatsInputLimitMarker,
	} {
		if !strings.Contains(sql, required) {
			t.Fatalf("analysis SQL lost streamstats barrier fragment %q:\n%s", required, sql)
		}
	}
	materializedInput := regexp.MustCompile(
		`"__os_streamstats_input_[0-9]+" AS MATERIALIZED \(`,
	)
	if !materializedInput.MatchString(sql) {
		t.Fatalf("analysis SQL lost the materialized streamstats input:\n%s", sql)
	}
	orderCapture := regexp.MustCompile(
		`"__os_order_[0-9]+_0" AS "__os_streamstats_order_[0-9]+_0"`,
	)
	windowOrder := regexp.MustCompile(
		`ORDER BY "__os_streamstats_order_[0-9]+_0" ASC NULLS LAST`,
	)
	if !orderCapture.MatchString(sql) || !windowOrder.MatchString(sql) {
		t.Fatalf("analysis SQL lost streamstats input order:\n%s", sql)
	}
	if got := strings.Count(sql, `FROM "open_splunk"."events"`); got != 1 {
		t.Fatalf("analysis physical event scans = %d, want one:\n%s", got, sql)
	}
	wantPrefix := []any{"user", "user.", "status", "status.", "tenant-1"}
	if len(args) < len(wantPrefix) || !slices.Equal(args[:len(wantPrefix)], wantPrefix) {
		t.Fatalf("analysis streamstats argument prefix = %#v, want %#v", args, wantPrefix)
	}
	if got, want := strings.Count(sql, "?"), len(args); got != want {
		t.Fatalf("analysis placeholders = %d, args = %d:\nSQL: %s\nargs: %#v", got, want, sql, args)
	}
}

func validStreamStatsOperator(t *testing.T, output string) *plan.StreamAggregate {
	t.Helper()
	return &plan.StreamAggregate{
		Measure: plan.AggregateMeasure{
			Function: plan.AggregateFunctionCountRows,
			Output:   output,
		},
		IncludeCurrent: true,
		Global:         true,
	}
}

func appendStreamStatsOperator(
	logical *plan.Query,
	operator *plan.StreamAggregate,
) *plan.Query {
	result := *logical
	result.Operators = append(append([]plan.Operator(nil), logical.Operators...), operator)
	return &result
}

func insertPlanOperator(
	operators []plan.Operator,
	index int,
	operator plan.Operator,
) []plan.Operator {
	result := make([]plan.Operator, 0, len(operators)+1)
	result = append(result, operators[:index]...)
	result = append(result, operator)
	result = append(result, operators[index:]...)
	return result
}

func mustResolveStreamStatsField(t *testing.T, name string) plan.FieldRef {
	t.Helper()
	field, err := plan.ResolveField(name, spl.Range{})
	if err != nil {
		t.Fatalf("ResolveField(%q): %v", name, err)
	}
	return field
}

func countString(values []string, target string) int {
	count := 0
	for _, value := range values {
		if value == target {
			count++
		}
	}
	return count
}
