package clickhouse

import (
	"reflect"
	"strings"
	"testing"
	"time"
)

// bucketCountGridBreakIdentifiers are the two live identifier sets that reach
// the shared bucket-count emitter: the timeline's and the fixed-count
// timechart's. Both must produce one shape.
func bucketCountGridBreakIdentifiers() map[string]bucketCountGrid {
	q := quoteIdentifier
	return map[string]bucketCountGrid{
		"timeline": {
			counts:       q("__os_timeline_counts"),
			countsSource: q("__os_timeline_prepared"),
			ticks:        q("__os_timeline_ticks"),
			bucketNumber: q("__os_timeline_bucket_number"),
			grid:         q("__os_timeline_grid"),
			ordinal:      q(TimelineOrdinalColumn),
			count:        q(TimelineCountColumn),
		},
		"timechart": {
			counts:       q("__os_timechart_group_counts"),
			countsSource: q("__os_timechart_source"),
			ticks:        q("__os_tc_ticks"),
			bucketNumber: q("__os_tc_bucket_number"),
			grid:         q("__os_timechart_grid"),
			ordinal:      q(TimechartOrdinalColumn),
			count:        q(TimechartCountColumn),
		},
	}
}

func writeBucketCountGridBreakSQL(grid bucketCountGrid) string {
	var sql strings.Builder
	writeBucketCountGridSQL(&sql, grid)
	return sql.String()
}

// bucketCountGridBreakCanonical rewrites every identifier the caller supplied
// back to its struct field name. Two identifier sets that produce the same
// canonical text are provably the same emitted shape.
func bucketCountGridBreakCanonical(text string, grid bucketCountGrid) string {
	// Longest first so no identifier is a prefix of another's replacement.
	return strings.NewReplacer(
		grid.countsSource, "<countsSource>",
		grid.counts, "<counts>",
		grid.bucketNumber, "<bucketNumber>",
		grid.ticks, "<ticks>",
		grid.grid, "<grid>",
		grid.ordinal, "<ordinal>",
		grid.count, "<count>",
	).Replace(text)
}

// TestBucketCountGridEmitterIsOneShapeForBothCallers pins that the timeline and
// the fixed-count timechart share a single grid emitter. Canonicalizing away
// the identifiers must leave byte-identical text, and each caller's live SQL
// must contain exactly that block. If the emitter ever grew a caller-specific
// branch, the zero-fill join or the ordering would silently differ between the
// two transports that both promise a dense ordinal grid.
func TestBucketCountGridEmitterIsOneShapeForBothCallers(t *testing.T) {
	t.Parallel()

	identifiers := bucketCountGridBreakIdentifiers()
	timeline := writeBucketCountGridBreakSQL(identifiers["timeline"])
	timechart := writeBucketCountGridBreakSQL(identifiers["timechart"])

	canonicalTimeline := bucketCountGridBreakCanonical(timeline, identifiers["timeline"])
	canonicalTimechart := bucketCountGridBreakCanonical(timechart, identifiers["timechart"])
	if canonicalTimeline != canonicalTimechart {
		t.Fatalf("emitted grid shapes diverged:\ntimeline:  %s\ntimechart: %s", canonicalTimeline, canonicalTimechart)
	}
	if strings.Contains(canonicalTimeline, "__os_") {
		t.Fatalf("emitter hard-codes a caller identifier: %s", canonicalTimeline)
	}

	// The zero-fill contract: the dense grid is the preserved side of the
	// join, missing counts become toUInt64(0), and the public order is the
	// grid's ordinal. Swapping any of the three turns absent buckets into
	// dropped rows or into a nondeterministic order.
	for _, fragment := range []string{
		"FROM <grid> LEFT JOIN <counts> ON <counts>.<bucketNumber> = <grid>.<bucketNumber>",
		"ifNull(<counts>.<count>, toUInt64(0)) AS <count>",
		"ORDER BY <grid>.<ordinal> ASC",
		"FROM <countsSource> GROUP BY <bucketNumber>",
	} {
		if !strings.Contains(canonicalTimeline, fragment) {
			t.Fatalf("grid emitter lost %q:\n%s", fragment, canonicalTimeline)
		}
	}
	if strings.Contains(canonicalTimeline, "FROM <counts> LEFT JOIN <grid>") ||
		strings.Contains(canonicalTimeline, "RIGHT JOIN") ||
		strings.Contains(canonicalTimeline, "INNER JOIN") {
		t.Fatalf("grid stopped being the preserved join side:\n%s", canonicalTimeline)
	}

	compiledTimeline, err := (Compiler{}).CompileTimeline(buildPlan(t, `index=gradethis`), validTimelineSpec())
	if err != nil {
		t.Fatalf("CompileTimeline: %v", err)
	}
	if !strings.Contains(compiledTimeline.SQL, timeline) {
		t.Fatalf("timeline SQL does not contain the shared grid block:\n%s", compiledTimeline.SQL)
	}
	compiledTimechart := compileSPL(t, `index=gradethis | timechart span=5m count`)
	if compiledTimechart.Timechart == nil || compiledTimechart.Timechart.Mode != TimechartModeFixedCount {
		t.Fatalf("timechart mode = %#v, want the fixed-count transport", compiledTimechart.Timechart)
	}
	if !strings.Contains(compiledTimechart.SQL, timechart) {
		t.Fatalf("timechart SQL does not contain the shared grid block:\n%s", compiledTimechart.SQL)
	}
}

// TestBucketCountGridPlaceholdersMatchAppendedArguments pins the emitter's only
// implicit contract: it writes exactly four placeholders, and
// appendOrdinalGridArgs must supply exactly four values in that positional
// order (span, span, first bucket, bucket count). A desynchronization here
// shifts every earlier bind value in a query whose grid is not the last stage.
func TestBucketCountGridPlaceholdersMatchAppendedArguments(t *testing.T) {
	t.Parallel()

	for name, grid := range bucketCountGridBreakIdentifiers() {
		text := writeBucketCountGridBreakSQL(grid)
		if got := strings.Count(text, "?"); got != 4 {
			t.Fatalf("%s grid emitted %d placeholders, want 4:\n%s", name, got, text)
		}
		if got := len(appendOrdinalGridArgs(nil, 1, 2, 3)); got != 4 {
			t.Fatalf("appendOrdinalGridArgs supplied %d values, want 4", got)
		}
		// The two span placeholders belong to the bucket-number expression in
		// the counts CTE; the last two belong to the numbers() grid, in that
		// order.
		countsEnd := strings.Index(text, "GROUP BY")
		if countsEnd < 0 {
			t.Fatalf("%s grid has no counts GROUP BY:\n%s", name, text)
		}
		if got := strings.Count(text[:countsEnd], "?"); got != 2 {
			t.Fatalf("%s counts stage has %d placeholders, want the two span binds:\n%s", name, got, text)
		}
		firstBucket := strings.Index(text, "toInt64(?) + toInt64(number)")
		numbers := strings.Index(text, "FROM numbers(?)")
		if firstBucket < 0 || numbers < 0 || firstBucket > numbers {
			t.Fatalf("%s grid bind order is not first-bucket then bucket-count:\n%s", name, text)
		}
	}
}

// TestBucketCountGridArgumentTailsAgreeAcrossTransports compiles the same time
// grid through both callers and requires an identical four-value tail. The
// timeline and the timechart are two views of one grid; if their bind tails
// disagree the two would paint different buckets for the same search.
func TestBucketCountGridArgumentTailsAgreeAcrossTransports(t *testing.T) {
	t.Parallel()

	compiledTimeline, err := (Compiler{}).CompileTimeline(buildPlan(t, `index=gradethis`), validTimelineSpec())
	if err != nil {
		t.Fatalf("CompileTimeline: %v", err)
	}
	compiledTimechart := compileSPL(t, `index=gradethis | timechart span=5m count`)

	spanNanoseconds := int64(5 * time.Minute)
	firstBucketNumber, ok := ordinalGridFirstBucketNumber(
		time.Date(2026, 7, 21, 0, 0, 0, 0, time.UTC).Unix(),
		300,
		288,
	)
	if !ok {
		t.Fatal("the shared fixture grid is not representable")
	}
	want := appendOrdinalGridArgs(nil, spanNanoseconds, firstBucketNumber, 288)
	for name, args := range map[string][]any{
		"timeline":  compiledTimeline.Args,
		"timechart": compiledTimechart.Args,
	} {
		if len(args) < 4 {
			t.Fatalf("%s carries %d arguments, want at least the grid tail", name, len(args))
		}
		if got := args[len(args)-4:]; !reflect.DeepEqual(got, want) {
			t.Fatalf("%s grid argument tail = %#v, want %#v", name, got, want)
		}
	}
}

// TestBucketCountGridTailKeepsItsBindTypesAtBoundaries walks the grid's own
// boundaries — a single bucket, a pre-epoch window whose first bucket number is
// negative, and a sub-second span — and requires the four tail binds to keep
// their exact Go types. The driver binds by concrete type, so an int64 that
// became an int, or a uint64 that became an int64, is a runtime protocol error
// that no compile-time test would otherwise catch.
func TestBucketCountGridTailKeepsItsBindTypesAtBoundaries(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name string
		spec TimelineSpec
	}{
		{
			name: "single bucket",
			spec: TimelineSpec{
				FirstBucket: time.Date(2026, 7, 21, 0, 0, 0, 0, time.UTC),
				SpanSeconds: 300,
				BucketCount: 1,
				Earliest:    time.Date(2026, 7, 21, 0, 0, 0, 0, time.UTC),
				Latest:      time.Date(2026, 7, 21, 0, 5, 0, 0, time.UTC),
			},
		},
		{
			name: "pre-epoch window has a negative first bucket",
			spec: TimelineSpec{
				FirstBucket: time.Date(1969, 12, 31, 23, 0, 0, 0, time.UTC),
				SpanSeconds: 3600,
				BucketCount: 2,
				Earliest:    time.Date(1969, 12, 31, 23, 0, 0, 0, time.UTC),
				Latest:      time.Date(1970, 1, 1, 1, 0, 0, 0, time.UTC),
			},
		},
		{
			name: "one second span",
			spec: TimelineSpec{
				FirstBucket: time.Date(2026, 7, 21, 0, 0, 0, 0, time.UTC),
				SpanSeconds: 1,
				BucketCount: 60,
				Earliest:    time.Date(2026, 7, 21, 0, 0, 0, 0, time.UTC),
				Latest:      time.Date(2026, 7, 21, 0, 1, 0, 0, time.UTC),
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			scope := testChartScope()
			scope.Earliest = test.spec.Earliest
			scope.Latest = test.spec.Latest
			scope.SearchStart = test.spec.Latest.Add(time.Millisecond)
			scope.IndexTimeCutoff = test.spec.Latest.Add(time.Second)
			logical := buildPlanWithScope(t, `index=gradethis`, scope)
			compiled, err := (Compiler{}).CompileTimeline(logical, test.spec)
			if err != nil {
				t.Fatalf("CompileTimeline: %v", err)
			}
			if got, want := strings.Count(compiled.SQL, "?"), len(compiled.Args); got != want {
				t.Fatalf("placeholders = %d, args = %d:\n%s", got, want, compiled.SQL)
			}
			tail := compiled.Args[len(compiled.Args)-4:]
			spanNanoseconds, ok := tail[0].(int64)
			if !ok || spanNanoseconds != test.spec.SpanSeconds*int64(time.Second) {
				t.Fatalf("span bind = %#v, want int64 %d", tail[0], test.spec.SpanSeconds*int64(time.Second))
			}
			if !reflect.DeepEqual(tail[0], tail[1]) {
				t.Fatalf("the two span binds diverged: %#v vs %#v", tail[0], tail[1])
			}
			firstBucketNumber, ok := tail[2].(int64)
			if !ok {
				t.Fatalf("first-bucket bind = %#v, want int64", tail[2])
			}
			wantFirst, representable := ordinalGridFirstBucketNumber(
				test.spec.FirstBucket.Unix(),
				test.spec.SpanSeconds,
				test.spec.BucketCount,
			)
			if !representable || firstBucketNumber != wantFirst {
				t.Fatalf("first bucket number = %d, want %d (%t)", firstBucketNumber, wantFirst, representable)
			}
			if got, ok := tail[3].(uint64); !ok || got != test.spec.BucketCount {
				t.Fatalf("bucket-count bind = %#v, want uint64 %d", tail[3], test.spec.BucketCount)
			}
		})
	}
}
