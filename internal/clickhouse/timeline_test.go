package clickhouse

import (
	"errors"
	"math"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/plan"
)

func TestCompileTimelinePreservesEligibleEventPipeline(t *testing.T) {
	t.Parallel()

	logical := buildPlan(t, `index=gradethis trace_id="secret-value" | eval duration_ms=tonumber(duration) | rename duration_ms AS parsed_duration | search parsed_duration>1 | table _time parsed_duration message | sort 0 +_time | dedup 2 parsed_duration | head 7`)
	compiler := Compiler{}
	ordinary, err := compiler.Compile(logical)
	if err != nil {
		t.Fatalf("Compile() error = %v", err)
	}
	spec := validTimelineSpec()
	compiled, err := compiler.CompileTimeline(logical, spec)
	if err != nil {
		t.Fatalf("CompileTimeline() error = %v", err)
	}

	if !strings.Contains(compiled.SQL, ordinary.SQL) {
		t.Fatalf("timeline SQL did not retain the complete ordinary query:\nordinary: %s\ntimeline: %s", ordinary.SQL, compiled.SQL)
	}
	for _, fragment := range []string{
		`"tenant_id" = ?`,
		`"index_name" IN (?)`,
		`"event_time" >= parseDateTime64BestEffort(?, 9, 'UTC')`,
		`"event_time" < parseDateTime64BestEffort(?, 9, 'UTC')`,
		`"index_time" <= parseDateTime64BestEffort(?, 3, 'UTC')`,
		`"visibility_seq" <= ?`,
		`toFloat64OrNull`,
		`LIMIT ? BY`,
		`LIMIT ?`,
	} {
		if !strings.Contains(compiled.SQL, fragment) {
			t.Errorf("timeline SQL missing %q:\n%s", fragment, compiled.SQL)
		}
	}
	for _, secret := range []string{"tenant-1", "gradethis", "secret-value"} {
		if strings.Contains(compiled.SQL, secret) {
			t.Errorf("timeline SQL embedded scoped or user-authored value %q:\n%s", secret, compiled.SQL)
		}
	}

	spanNanoseconds := spec.SpanSeconds * int64(time.Second)
	wantArgs := append(slices.Clone(ordinary.Args),
		spanNanoseconds,
		spanNanoseconds,
		spec.FirstBucket.Unix()/spec.SpanSeconds,
		spec.BucketCount,
	)
	if !slices.Equal(compiled.Args, wantArgs) {
		t.Fatalf("Args = %#v, want lexical order %#v", compiled.Args, wantArgs)
	}
	if got := strings.Count(compiled.SQL, "?"); got != len(compiled.Args) {
		t.Fatalf("placeholder count = %d, args = %d\nSQL: %s\nargs: %#v", got, len(compiled.Args), compiled.SQL, compiled.Args)
	}
	if compiled.Spec != spec {
		t.Fatalf("Spec = %+v, want %+v", compiled.Spec, spec)
	}
}

func TestCompileTimelineBuildsContinuousZeroFilledGrid(t *testing.T) {
	t.Parallel()

	compiled, err := (Compiler{}).CompileTimeline(buildPlan(t, `index=gradethis level=error`), validTimelineSpec())
	if err != nil {
		t.Fatalf("CompileTimeline() error = %v", err)
	}
	for _, fragment := range []string{
		`"__os_timeline_source" AS (SELECT "_time" AS "__os_timeline_event_time" FROM (`,
		`intDiv("__os_timeline_ticks", ?) - if("__os_timeline_ticks" < 0 AND "__os_timeline_ticks" % ? != 0, 1, 0)`,
		`AS "__os_timeline_bucket_number"`,
		`count() AS "__os_timeline_count"`,
		`toUInt64(number) AS "__os_timeline_ordinal", toInt64(?) + toInt64(number) AS "__os_timeline_bucket_number"`,
		`FROM numbers(?)`,
		`LEFT JOIN "__os_timeline_counts" ON "__os_timeline_counts"."__os_timeline_bucket_number" = "__os_timeline_grid"."__os_timeline_bucket_number"`,
		`ifNull("__os_timeline_counts"."__os_timeline_count", toUInt64(0)) AS "` + TimelineCountColumn + `"`,
		`ORDER BY "__os_timeline_grid"."__os_timeline_ordinal" ASC`,
	} {
		if !strings.Contains(compiled.SQL, fragment) {
			t.Errorf("timeline SQL missing %q:\n%s", fragment, compiled.SQL)
		}
	}
	if !strings.Contains(compiled.SQL, `AS "`+TimelineOrdinalColumn+`"`) {
		t.Errorf("timeline SQL missing fixed ordinal output alias:\n%s", compiled.SQL)
	}
	if strings.Contains(compiled.SQL, "fromUnixTimestamp64Nano") || strings.Contains(compiled.SQL, "parseDateTime64BestEffort(?)") {
		t.Fatalf("timeline grid must not convert aligned bucket starts through DateTime64:\n%s", compiled.SQL)
	}
	if strings.Contains(compiled.SQL, "INTERPOLATE") || strings.Contains(compiled.SQL, "WITH FILL") {
		t.Fatalf("timeline grid must have a bounded numbers source, not implicit fill:\n%s", compiled.SQL)
	}
}

func TestCompileTimelineAcceptsEligibleOperatorMatrix(t *testing.T) {
	t.Parallel()

	queries := []string{
		`index=gradethis level=error`,
		`index=gradethis | search level=error | where severity>=3`,
		`index=gradethis | fields _time message`,
		`index=gradethis | fields - host`,
		`index=gradethis | table _time message`,
		`index=gradethis | eval duration_ms=tonumber(duration)`,
		`index=gradethis | rex "(?<request_id>request_id=\w+)"`,
		`index=gradethis | rename logger AS component`,
		`index=gradethis | sort 0 -_time`,
		`index=gradethis | dedup 2 host`,
		`index=gradethis | tail 20`,
	}
	for _, source := range queries {
		source := source
		t.Run(source, func(t *testing.T) {
			t.Parallel()
			if _, err := (Compiler{}).CompileTimeline(buildPlan(t, source), validTimelineSpec()); err != nil {
				t.Fatalf("CompileTimeline() error = %v", err)
			}
		})
	}
}

func TestCompileTimelineRejectsIneligibleOperatorMatrix(t *testing.T) {
	t.Parallel()

	tests := []struct {
		source string
		code   string
	}{
		{`index=gradethis | fields - _time`, "SPL_UNSUPPORTED_TIMELINE_TIME_FIELD"},
		{`index=gradethis | table message`, "SPL_UNSUPPORTED_TIMELINE_TIME_FIELD"},
		{`index=gradethis | eval _time=_indextime`, "SPL_UNSUPPORTED_TIMELINE_TIME_FIELD"},
		{`index=gradethis | rex "(?<_time>\d+)"`, "SPL_UNSUPPORTED_TIMELINE_TIME_FIELD"},
		{`index=gradethis | rename _time AS observed_at`, "SPL_UNSUPPORTED_TIMELINE_TIME_FIELD"},
		{`index=gradethis | rename observed_at AS _time`, "SPL_UNSUPPORTED_TIMELINE_TIME_FIELD"},
		{`index=gradethis | bin _time span=5m`, "SPL_UNSUPPORTED_TIMELINE_TIME_FIELD"},
		{`index=gradethis | fields - _time | table _time`, "SPL_UNSUPPORTED_TIMELINE_TIME_FIELD"},
		{`index=gradethis | stats count`, "SPL_UNSUPPORTED_TIMELINE_PIPELINE"},
		{`index=gradethis | top level`, "SPL_UNSUPPORTED_TIMELINE_PIPELINE"},
		{`index=gradethis | rare level`, "SPL_UNSUPPORTED_TIMELINE_PIPELINE"},
		{`index=gradethis | timechart span=5m count BY level`, "SPL_UNSUPPORTED_TIMELINE_PIPELINE"},
	}
	for _, test := range tests {
		test := test
		t.Run(test.source, func(t *testing.T) {
			t.Parallel()
			_, err := (Compiler{}).CompileTimeline(buildPlan(t, test.source), validTimelineSpec())
			var diagnostic *plan.Diagnostic
			if !errors.As(err, &diagnostic) || diagnostic.Code != test.code {
				t.Fatalf("CompileTimeline() error = %v, want diagnostic %q", err, test.code)
			}
		})
	}
}

func TestCompileTimelineRejectsForgedPlanAndMissingCanonicalOutput(t *testing.T) {
	t.Parallel()

	valid := buildPlan(t, `index=gradethis`)
	tests := []struct {
		name  string
		query *plan.Query
	}{
		{name: "nil", query: nil},
		{name: "empty", query: &plan.Query{}},
		{name: "late scan", query: &plan.Query{Operators: []plan.Operator{valid.Operators[0], valid.Operators[0]}}},
		{name: "nil operator", query: &plan.Query{Operators: []plan.Operator{valid.Operators[0], nil}}},
		{name: "dynamic output", query: &plan.Query{Operators: valid.Operators, DynamicOutput: &plan.DynamicSeriesOutput{FixedFields: []string{"_time"}, MaxSeries: 1}}},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			_, err := (Compiler{}).CompileTimeline(test.query, validTimelineSpec())
			var diagnostic *plan.Diagnostic
			if !errors.As(err, &diagnostic) || diagnostic.Code != "SPL_UNSUPPORTED_TIMELINE_PIPELINE" {
				t.Fatalf("CompileTimeline() error = %v, want timeline pipeline diagnostic", err)
			}
		})
	}
}

func TestCompileTimelineValidatesExactMinimalSpec(t *testing.T) {
	t.Parallel()

	valid := validTimelineSpec()
	nonUTC := time.FixedZone("UTC-alias", 0)
	tests := []struct {
		name   string
		mutate func(*TimelineSpec)
	}{
		{name: "zero first bucket", mutate: func(spec *TimelineSpec) { spec.FirstBucket = time.Time{} }},
		{name: "non UTC first bucket", mutate: func(spec *TimelineSpec) { spec.FirstBucket = spec.FirstBucket.In(nonUTC) }},
		{name: "fractional first bucket", mutate: func(spec *TimelineSpec) { spec.FirstBucket = spec.FirstBucket.Add(time.Nanosecond) }},
		{name: "non UTC earliest", mutate: func(spec *TimelineSpec) { spec.Earliest = spec.Earliest.In(nonUTC) }},
		{name: "non UTC latest", mutate: func(spec *TimelineSpec) { spec.Latest = spec.Latest.In(nonUTC) }},
		{name: "earliest before DateTime64 minimum", mutate: func(spec *TimelineSpec) {
			spec.Earliest = MinimumSearchTime().Add(-time.Nanosecond)
		}},
		{name: "latest after DateTime64 maximum", mutate: func(spec *TimelineSpec) {
			spec.Latest = MaximumSearchTime().Add(time.Nanosecond)
		}},
		{name: "zero span", mutate: func(spec *TimelineSpec) { spec.SpanSeconds = 0 }},
		{name: "negative span", mutate: func(spec *TimelineSpec) { spec.SpanSeconds = -1 }},
		{name: "nanosecond span overflow", mutate: func(spec *TimelineSpec) { spec.SpanSeconds = math.MaxInt64/int64(time.Second) + 1 }},
		{name: "zero buckets", mutate: func(spec *TimelineSpec) { spec.BucketCount = 0 }},
		{name: "too many buckets", mutate: func(spec *TimelineSpec) { spec.BucketCount = 10_001 }},
		{name: "unaligned first bucket", mutate: func(spec *TimelineSpec) { spec.FirstBucket = spec.FirstBucket.Add(time.Second) }},
		{name: "empty range", mutate: func(spec *TimelineSpec) { spec.Latest = spec.Earliest }},
		{name: "reversed range", mutate: func(spec *TimelineSpec) { spec.Latest = spec.Earliest.Add(-time.Second) }},
		{name: "earliest before grid", mutate: func(spec *TimelineSpec) { spec.Earliest = spec.FirstBucket.Add(-time.Second) }},
		{name: "padded leading bucket", mutate: func(spec *TimelineSpec) {
			spec.Earliest = spec.FirstBucket.Add(time.Duration(spec.SpanSeconds) * time.Second)
		}},
		{name: "latest after grid", mutate: func(spec *TimelineSpec) {
			spec.Latest = spec.FirstBucket.Add(time.Duration(spec.SpanSeconds*int64(spec.BucketCount)+1) * time.Second)
		}},
		{name: "padded trailing bucket", mutate: func(spec *TimelineSpec) {
			spec.Latest = spec.FirstBucket.Add(time.Duration(spec.SpanSeconds*int64(spec.BucketCount-1)) * time.Second)
		}},
		{name: "covered seconds overflow", mutate: func(spec *TimelineSpec) {
			spec.SpanSeconds = math.MaxInt64 / 9_999
			spec.BucketCount = 10_000
			spec.FirstBucket = time.Unix(0, 0).UTC()
			spec.Earliest = spec.FirstBucket
			spec.Latest = spec.FirstBucket.Add(time.Second)
		}},
		{name: "grid end overflow", mutate: func(spec *TimelineSpec) {
			spec.SpanSeconds = 1
			spec.BucketCount = 2
			spec.FirstBucket = time.Unix(math.MaxInt64-1, 0).UTC()
			spec.Earliest = spec.FirstBucket
			spec.Latest = time.Unix(math.MaxInt64, 0).UTC()
		}},
	}
	logical := buildPlan(t, `index=gradethis`)
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			spec := valid
			test.mutate(&spec)
			if _, err := (Compiler{}).CompileTimeline(logical, spec); err == nil || !strings.Contains(err.Error(), "compile ClickHouse timeline") {
				t.Fatalf("CompileTimeline() error = %v, want timeline spec error", err)
			}
		})
	}
}

func TestCompileTimelineRequiresExactScanRange(t *testing.T) {
	t.Parallel()

	logical := buildPlan(t, `index=gradethis`)
	tests := []struct {
		name   string
		mutate func(*TimelineSpec)
	}{
		{name: "earliest one nanosecond later", mutate: func(spec *TimelineSpec) { spec.Earliest = spec.Earliest.Add(time.Nanosecond) }},
		{name: "latest one nanosecond earlier", mutate: func(spec *TimelineSpec) { spec.Latest = spec.Latest.Add(-time.Nanosecond) }},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			spec := validTimelineSpec()
			test.mutate(&spec)
			if _, err := (Compiler{}).CompileTimeline(logical, spec); err == nil || !strings.Contains(err.Error(), "does not match the Scan snapshot") {
				t.Fatalf("CompileTimeline() error = %v, want exact Scan range rejection", err)
			}
		})
	}
}

func TestCompileTimelinePreservesNanosecondRangeBoundaries(t *testing.T) {
	t.Parallel()

	logical := buildPlan(t, `index=gradethis`)
	scan := logical.Operators[0].(*plan.Scan)
	scan.Earliest = scan.Earliest.Add(123_456_789 * time.Nanosecond)
	scan.Latest = scan.Latest.Add(-time.Nanosecond)
	spec := validTimelineSpec()
	spec.Earliest = scan.Earliest
	spec.Latest = scan.Latest
	compiled, err := (Compiler{}).CompileTimeline(logical, spec)
	if err != nil {
		t.Fatalf("CompileTimeline() error = %v", err)
	}
	if !compiled.Spec.Earliest.Equal(scan.Earliest) || !compiled.Spec.Latest.Equal(scan.Latest) {
		t.Fatalf("compiled range = [%s, %s), want exact [%s, %s)", compiled.Spec.Earliest, compiled.Spec.Latest, scan.Earliest, scan.Latest)
	}
}

func TestCompileTimelineHandlesPreEpochFlooring(t *testing.T) {
	t.Parallel()

	spec := TimelineSpec{
		FirstBucket: time.Unix(-120, 0).UTC(),
		SpanSeconds: 60,
		BucketCount: 2,
		Earliest:    time.Unix(-90, 0).UTC(),
		Latest:      time.Unix(-1, 0).UTC(),
	}
	logical := buildPlan(t, `index=gradethis`)
	scan := logical.Operators[0].(*plan.Scan)
	scan.Earliest = spec.Earliest
	scan.Latest = spec.Latest
	compiled, err := (Compiler{}).CompileTimeline(logical, spec)
	if err != nil {
		t.Fatalf("CompileTimeline() error = %v", err)
	}
	if !strings.Contains(compiled.SQL, `"__os_timeline_ticks" < 0 AND "__os_timeline_ticks" % ? != 0`) {
		t.Fatalf("timeline SQL does not floor negative epoch timestamps:\n%s", compiled.SQL)
	}
	wantTail := []any{int64(60_000_000_000), int64(60_000_000_000), int64(-2), uint64(2)}
	if !slices.Equal(compiled.Args[len(compiled.Args)-len(wantTail):], wantTail) {
		t.Fatalf("timeline args tail = %#v, want %#v", compiled.Args[len(compiled.Args)-len(wantTail):], wantTail)
	}
}

func TestCompileTimelineEnforcesWrappedQuerySize(t *testing.T) {
	t.Parallel()

	logical := buildPlan(t, `index=gradethis`)
	base, err := (Compiler{}).Compile(logical)
	if err != nil {
		t.Fatalf("Compile() base error = %v", err)
	}
	// A trusted identifier can consume nearly the ordinary compiler's byte
	// budget. The fixed timeline wrapper must still fit inside the same bound.
	databaseLength := maxCompiledQueryBytes - len(base.SQL) + len("open_splunk") - 1
	compiler := Compiler{Database: strings.Repeat("d", databaseLength)}
	ordinary, err := compiler.Compile(logical)
	if err != nil {
		t.Fatalf("Compile() near-limit error = %v", err)
	}
	if len(ordinary.SQL) != maxCompiledQueryBytes-1 {
		t.Fatalf("ordinary SQL bytes = %d, want %d", len(ordinary.SQL), maxCompiledQueryBytes-1)
	}
	_, err = compiler.CompileTimeline(logical, validTimelineSpec())
	var diagnostic *plan.Diagnostic
	if !errors.As(err, &diagnostic) || diagnostic.Code != "SPL_QUERY_TOO_COMPLEX" {
		t.Fatalf("CompileTimeline() error = %v, want SPL_QUERY_TOO_COMPLEX", err)
	}
}

func validTimelineSpec() TimelineSpec {
	return TimelineSpec{
		FirstBucket: time.Date(2026, 7, 21, 0, 0, 0, 0, time.UTC),
		SpanSeconds: 300,
		BucketCount: 288,
		Earliest:    time.Date(2026, 7, 21, 0, 0, 0, 0, time.UTC),
		Latest:      time.Date(2026, 7, 22, 0, 0, 0, 0, time.UTC),
	}
}
