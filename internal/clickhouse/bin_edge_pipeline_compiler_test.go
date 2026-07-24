package clickhouse

import (
	"errors"
	"regexp"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/plan"
	"github.com/Suhaibinator/open-splunk/internal/spl"
)

// binEdgePipelineShapes enumerates the difficult pipeline shapes this file
// keeps honest. Each entry compiles through the real parser, planner, and
// ClickHouse compiler.
var binEdgePipelineShapes = []struct {
	name   string
	source string
}{
	{
		name: "rex eval bin where stats sort head",
		source: `index=gradethis
| rex field=_raw "latency=(?<latency_text>[0-9]+)"
| eval doubled=tonumber(latency_text)
| bin latency_text span=100 AS latency_band
| where latency_band>=0
| stats count BY latency_band
| sort -count
| head 3`,
	},
	{name: "replace in place", source: `index=gradethis | bin metric span=10`},
	{name: "alias destination", source: `index=gradethis | bin metric span=10 AS band`},
	{name: "bucket alias with leading span", source: `index=gradethis | bucket span=10 metric AS band`},
	{name: "dedup on a binned field", source: `index=gradethis | bin metric span=10 AS band | dedup band`},
	{name: "dedup on a replaced field", source: `index=gradethis | bin metric span=10 | dedup metric`},
	{name: "top over binned output", source: `index=gradethis | bin metric span=10 AS band | top band`},
	{name: "rare over binned output", source: `index=gradethis | bin metric span=10 AS band | rare band`},
	{name: "top with limit over binned output", source: `index=gradethis | bin metric span=10 AS band | top limit=3 band`},
	{
		name:   "timechart alongside a numeric bin",
		source: `index=gradethis | bin metric span=10 AS band | timechart span=5m count BY level`,
	},
	{
		name:   "timechart split by a binned field",
		source: `index=gradethis | bin label span=10 AS band | timechart span=5m count BY band`,
	},
	{
		name:   "bin then rex overwriting the same field",
		source: `index=gradethis | bin metric span=10 AS band | rex field=_raw "status=(?<band>[0-9]+)"`,
	},
	{
		name:   "rex then bin overwriting the same field",
		source: `index=gradethis | rex field=_raw "status=(?<band>[0-9]+)" | bin band span=10`,
	},
	{name: "numeric bin written to canonical time", source: `index=gradethis | bin metric span=10 AS _time`},
	{
		name:   "time bin alias retains the canonical clock",
		source: `index=gradethis | bin _time span=5m AS bucket_time | timechart span=5m count BY level`,
	},
	{
		name:   "time bin alias beside a numeric bin",
		source: `index=gradethis | bin _time span=5m AS bucket_time | bin metric span=10 AS band | table bucket_time band`,
	},
	{
		name:   "consecutive bins on one field",
		source: `index=gradethis | bin metric span=10 | bin metric span=7`,
	},
	{
		name:   "consecutive bins chained through an alias",
		source: `index=gradethis | bin metric span=10 AS first | bin first span=6 AS second`,
	},
	{
		name:   "consecutive bins on distinct fields",
		source: `index=gradethis | bin left span=10 AS a | bin right span=7 AS b | bin left span=3 AS c`,
	},
	{
		name:   "bin then project the binned field away",
		source: `index=gradethis | bin metric span=10 AS band | table event_id`,
	},
	{
		name:   "bin then exclude the binned field",
		source: `index=gradethis | bin metric span=10 AS band | fields - band`,
	},
	{
		name:   "bin over a projected schema",
		source: `index=gradethis | bin metric span=10 AS band | table band | bin band span=5 AS band2`,
	},
	{
		name:   "bin with a calculated prior destination",
		source: `index=gradethis | eval band=1 | bin metric span=10 AS band`,
	},
	{
		name:   "bin with a calculated string prior destination",
		source: `index=gradethis | eval band="stale" | bin metric span=10 AS band`,
	},
	{name: "bin a flattened descendant", source: `index=gradethis | bin nested.value span=10 AS band`},
	{name: "bin into a flattened descendant", source: `index=gradethis | bin metric span=10 AS nested.value`},
	{name: "greater than on binned output", source: `index=gradethis | bin metric span=10 AS band | where band>20 | table band`},
	{name: "at most on binned output", source: `index=gradethis | bin metric span=10 AS band | where band<=20 | table band`},
	{name: "equality on binned output", source: `index=gradethis | bin metric span=10 AS band | where band=20 | table band`},
	{name: "search predicate on binned output", source: `index=gradethis | bin metric span=10 AS band | search band=20`},
	{name: "sort then head on binned output", source: `index=gradethis | bin metric span=10 AS band | sort band | head 3`},
	{name: "tail on binned output", source: `index=gradethis | bin metric span=10 AS band | sort band | tail 3`},
	{name: "rename a binned field", source: `index=gradethis | bin metric span=10 AS band | rename band AS bucket`},
	{name: "bin after a transforming command", source: `index=gradethis | bin metric span=10 AS band | stats count BY band | bin count span=10`},
	{name: "dedup then top over binned output", source: `index=gradethis | bin metric span=10 AS band | dedup band | top band`},
	{
		name:   "stats percentile over a binned group",
		source: `index=gradethis | bin metric span=10 AS band | stats count p95(severity) AS p BY band | sort -count | head 5`,
	},
}

// TestBinEdgePipelineShapesBalancePlaceholdersAndArguments keeps compiler and
// executor placeholder accounting exact for every difficult pipeline shape.
// A single misplaced bound value silently shifts every later argument, so this
// invariant is checked for the complete compiled statement rather than one
// stage.
func TestBinEdgePipelineShapesBalancePlaceholdersAndArguments(t *testing.T) {
	t.Parallel()

	for _, shape := range binEdgePipelineShapes {
		t.Run(shape.name, func(t *testing.T) {
			t.Parallel()
			compiled := compileSPL(t, shape.source)
			if compiled.SQL == "" || len(compiled.OutputFields) == 0 {
				t.Fatalf("compiled query is incomplete: %#v", compiled)
			}
			if got, want := strings.Count(compiled.SQL, "?"), len(compiled.Args); got != want {
				t.Fatalf(
					"placeholder count = %d, args = %d\nSQL: %s\nargs: %#v",
					got, want, compiled.SQL, compiled.Args,
				)
			}
			// The native driver treats a `{name:type}` sequence as a query
			// parameter, so no generated expression may introduce braces.
			if strings.ContainsAny(compiled.SQL, "{}") {
				t.Fatalf("compiled SQL introduced a brace the driver can bind:\n%s", compiled.SQL)
			}
			for _, output := range compiled.OutputFields {
				if strings.HasPrefix(output, "__os_") {
					t.Fatalf("private compiler column leaked into the output schema: %v", compiled.OutputFields)
				}
			}
			// Bound arguments must carry every user value; the scan already
			// proves the tenant and index are parameterized.
			if strings.Contains(compiled.SQL, "gradethis") {
				t.Fatalf("compiled SQL inlined a user value:\n%s", compiled.SQL)
			}
		})
	}
}

// TestBinEdgeStreamingBinPipelinesStayScanShaped pins the streaming contract:
// a bin stage adds no aggregate, window, join, or materialization fence, and
// never introduces a second scoped scan of the storage table.
func TestBinEdgeStreamingBinPipelinesStayScanShaped(t *testing.T) {
	t.Parallel()

	for _, source := range []string{
		`index=gradethis | bin metric span=10`,
		`index=gradethis | bin metric span=10 AS band`,
		`index=gradethis | bin metric span=10 AS band | where band>=20`,
		`index=gradethis | bin metric span=10 AS band | table event_id band`,
		`index=gradethis | bin metric span=10 | bin metric span=7`,
		`index=gradethis | bin left span=10 AS a | bin right span=7 AS b`,
		`index=gradethis | eval band="stale" | bin metric span=10 AS band`,
		`index=gradethis | rex field=_raw "status=(?<cap>[0-9]+)" | bin cap span=10 AS band`,
		`index=gradethis | bin metric span=10 AS band | rex field=_raw "status=(?<band>[0-9]+)"`,
		`index=gradethis | bin _time span=5m AS bucket_time | bin metric span=10 AS band`,
		`index=gradethis | bin metric span=10 AS band | sort band | head 3`,
	} {
		t.Run(source, func(t *testing.T) {
			t.Parallel()
			compiled := compileSPL(t, source)
			for _, forbidden := range []string{" GROUP BY ", " OVER (", " JOIN ", " MATERIALIZED ", "DISTINCT "} {
				if strings.Contains(compiled.SQL, forbidden) {
					t.Fatalf("streaming bin pipeline introduced %q:\n%s", forbidden, compiled.SQL)
				}
			}
			if got := strings.Count(compiled.SQL, `FROM "open_splunk"."events"`); got != 1 {
				t.Fatalf("scoped storage scan occurs %d times, want once:\n%s", got, compiled.SQL)
			}
		})
	}
}

// TestBinEdgeDynamicBinUsesOneMetadataLookupPerStoredSource keeps the bounded
// metadata promise honest across chained stages. A stored Dynamic source needs
// exactly one aligned-metadata position lookup, and a source that a previous
// stage already classified must reuse that stage's type rather than rescanning
// the immutable document.
func TestBinEdgeDynamicBinUsesOneMetadataLookupPerStoredSource(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name              string
		source            string
		wantMetadataScans int
	}{
		{
			name:              "one stored source",
			source:            `index=gradethis | bin metric span=10 AS band`,
			wantMetadataScans: 1,
		},
		{
			name:              "chained through the previous output",
			source:            `index=gradethis | bin metric span=10 AS first | bin first span=6 AS second`,
			wantMetadataScans: 1,
		},
		{
			name:              "replace in place twice",
			source:            `index=gradethis | bin metric span=10 | bin metric span=7`,
			wantMetadataScans: 1,
		},
		{
			name:              "two distinct stored sources",
			source:            `index=gradethis | bin left span=10 AS a | bin right span=7 AS b`,
			wantMetadataScans: 2,
		},
		{
			name:              "three stages over two stored sources",
			source:            `index=gradethis | bin left span=10 AS a | bin right span=7 AS b | bin a span=3 AS c`,
			wantMetadataScans: 2,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			compiled := compileSPL(t, test.source)
			if got := strings.Count(compiled.SQL, "arrayFirstIndex("); got != test.wantMetadataScans {
				t.Fatalf(
					"aligned metadata position lookups = %d, want %d:\n%s",
					got, test.wantMetadataScans, compiled.SQL,
				)
			}
		})
	}

	// The chained stage reads the previous stage's published presence and type
	// instead of the immutable stored document, so an overwritten value can
	// never be reclassified from stale metadata.
	chained := compileSPL(t, `index=gradethis | bin metric span=10 | bin metric span=7`)
	for _, required := range []string{
		`toUInt8(ifNull("__os_numeric_bin_output_exists_2", 0)) AS "__os_numeric_bin_exists_3"`,
		`toUInt8("__os_numeric_bin_output_type_2") AS "__os_numeric_bin_type_3"`,
	} {
		if !strings.Contains(chained.SQL, required) {
			t.Fatalf("chained Dynamic bin is missing %q:\n%s", required, chained.SQL)
		}
	}
	if got := countArgument(chained.Args, uint64(10)); got != 1 {
		t.Fatalf("span 10 argument count = %d, want 1: %#v", got, chained.Args)
	}
	if got := countArgument(chained.Args, uint64(7)); got != 1 {
		t.Fatalf("span 7 argument count = %d, want 1: %#v", got, chained.Args)
	}
}

// TestBinEdgeConsecutiveBinsOnDistinctFieldsKeepStageLocalMetadata proves the
// two stages read different stored paths and never share a WITH alias, which
// ClickHouse would otherwise inherit into the nested stage.
func TestBinEdgeConsecutiveBinsOnDistinctFieldsKeepStageLocalMetadata(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(t, `index=gradethis | bin left span=10 AS a | bin right span=7 AS b | table a b`)
	if got := countArgument(compiled.Args, "left"); got != 1 {
		t.Fatalf("left path argument count = %d, want 1: %#v", got, compiled.Args)
	}
	if got := countArgument(compiled.Args, "right"); got != 1 {
		t.Fatalf("right path argument count = %d, want 1: %#v", got, compiled.Args)
	}
	aliases := regexp.MustCompile(`AS "(__os_numeric_bin_[a-z_]+)_(\d+)"`).FindAllStringSubmatch(compiled.SQL, -1)
	if len(aliases) == 0 {
		t.Fatalf("no Dynamic bin aliases were emitted:\n%s", compiled.SQL)
	}
	seen := make(map[string]struct{}, len(aliases))
	stages := make(map[string]struct{}, 2)
	for _, alias := range aliases {
		full := alias[1] + "_" + alias[2]
		if _, duplicate := seen[full]; duplicate {
			t.Fatalf("Dynamic bin alias %q is defined twice:\n%s", full, compiled.SQL)
		}
		seen[full] = struct{}{}
		stages[alias[2]] = struct{}{}
	}
	if len(stages) != 2 {
		t.Fatalf("Dynamic bin aliases span %d stages, want 2: %v", len(stages), stages)
	}
	if got, want := strings.Count(compiled.SQL, "?"), len(compiled.Args); got != want {
		t.Fatalf("placeholder count = %d, args = %d", got, want)
	}
}

// TestBinEdgeDynamicBinPrunesDeadAliasesWhenProjectedAway keeps a discarded
// bin destination from carrying its private presence/type aliases through the
// rest of the pipeline.
func TestBinEdgeDynamicBinPrunesDeadAliasesWhenProjectedAway(t *testing.T) {
	t.Parallel()

	retained := compileSPL(t, `index=gradethis | bin metric span=10 AS band | table event_id band`)
	retainedExists := strings.Count(retained.SQL, `"__os_numeric_bin_output_exists_2"`)
	retainedType := strings.Count(retained.SQL, `"__os_numeric_bin_output_type_2"`)
	if retainedExists < 3 || retainedType < 3 {
		t.Fatalf(
			"retained destination lost its metadata aliases: exists=%d type=%d\n%s",
			retainedExists, retainedType, retained.SQL,
		)
	}

	for _, source := range []string{
		`index=gradethis | bin metric span=10 AS band | table event_id`,
		`index=gradethis | bin metric span=10 AS band | fields event_id`,
		`index=gradethis | bin metric span=10 AS band | fields - band`,
	} {
		compiled := compileSPL(t, source)
		if got := strings.Count(compiled.SQL, `"__os_numeric_bin_output_exists_2"`); got >= retainedExists {
			t.Fatalf(
				"dead presence alias was not pruned for %q: %d occurrences, retained shape has %d\n%s",
				source, got, retainedExists, compiled.SQL,
			)
		}
		if got := strings.Count(compiled.SQL, `"__os_numeric_bin_output_type_2"`); got >= retainedType {
			t.Fatalf(
				"dead type alias was not pruned for %q: %d occurrences, retained shape has %d\n%s",
				source, got, retainedType, compiled.SQL,
			)
		}
		if got, want := strings.Count(compiled.SQL, "?"), len(compiled.Args); got != want {
			t.Fatalf("placeholder count = %d, args = %d for %q", got, want, source)
		}
	}
}

// TestBinEdgeNumericBinToCanonicalTimeInvalidatesTimeConsumers pins the
// documented provenance rule from both directions: writing a numeric bucket to
// `_time` replaces the canonical clock, while `bin _time ... AS <output>`
// retains it.
func TestBinEdgeNumericBinToCanonicalTimeInvalidatesTimeConsumers(t *testing.T) {
	t.Parallel()

	replaced := compileSPL(t, `index=gradethis | bin metric span=10 AS _time | table event_id _time metric`)
	if !strings.Contains(replaced.SQL, `REPLACE (`) {
		t.Fatalf("numeric bin AS _time did not replace the canonical column:\n%s", replaced.SQL)
	}
	if !slices.Equal(replaced.OutputFields, []string{"event_id", "_time", "metric"}) {
		t.Fatalf("output fields = %v", replaced.OutputFields)
	}
	// The source is retained, so both the bucket and its input remain readable.
	if got, want := strings.Count(replaced.SQL, "?"), len(replaced.Args); got != want {
		t.Fatalf("placeholder count = %d, args = %d", got, want)
	}

	for _, test := range []struct {
		source string
		code   string
	}{
		{
			source: `index=gradethis | bin metric span=10 AS _time | bin _time span=5m`,
			code:   "SPL_UNSUPPORTED_BIN_TIME_FIELD",
		},
		{
			source: `index=gradethis | bin metric span=10 AS _time | timechart span=5m count BY level`,
			code:   "SPL_UNSUPPORTED_TIMECHART_TIME_FIELD",
		},
		{
			source: `index=gradethis | bin _time span=5m | timechart span=5m count BY level`,
			code:   "SPL_UNSUPPORTED_TIMECHART_TIME_FIELD",
		},
	} {
		_, err := compileBinEdgeSPL(test.source)
		var diagnostic *plan.Diagnostic
		if !errors.As(err, &diagnostic) || diagnostic.Code != test.code {
			t.Fatalf("compiling %q = %v, want %s", test.source, err, test.code)
		}
	}

	// The aliased time bin keeps canonical provenance for later analysis, even
	// with a numeric Dynamic bin between it and the consumer.
	for _, source := range []string{
		`index=gradethis | bin _time span=5m AS bucket_time | timechart span=5m count BY level`,
		`index=gradethis | bin _time span=5m AS bucket_time | bin metric span=10 AS band | timechart span=5m count BY level`,
	} {
		compiled := compileSPL(t, source)
		if compiled.Timechart == nil {
			t.Fatalf("aliased time bin lost the timechart contract for %q", source)
		}
		if got, want := strings.Count(compiled.SQL, "?"), len(compiled.Args); got != want {
			t.Fatalf("placeholder count = %d, args = %d for %q", got, want, source)
		}
	}
}

// TestBinEdgeRexOverwritingBinnedDestinationReadsStageMetadata proves a later
// rex stage preserves the bucket a bin stage published when the pattern does
// not match, instead of consulting the immutable stored document.
func TestBinEdgeRexOverwritingBinnedDestinationReadsStageMetadata(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(
		t,
		`index=gradethis | bin metric span=10 AS band | rex field=_raw "status=(?<band>[0-9]+)" | table event_id band`,
	)
	for _, required := range []string{
		// No match keeps the published bucket, its presence, and its type.
		`CAST("band" AS Dynamic))`,
		`ifNull("__os_numeric_bin_output_exists_2", 0))) AS "__os_rex_exists_`,
		`"__os_numeric_bin_output_type_2")) AS "__os_rex_type_`,
	} {
		if !strings.Contains(compiled.SQL, required) {
			t.Fatalf("rex over a binned destination is missing %q:\n%s", required, compiled.SQL)
		}
	}
	if got, want := strings.Count(compiled.SQL, "?"), len(compiled.Args); got != want {
		t.Fatalf("placeholder count = %d, args = %d\nSQL: %s\nargs: %#v", got, want, compiled.SQL, compiled.Args)
	}
	if got := strings.Count(compiled.SQL, "extractGroups("); got != 1 {
		t.Fatalf("rex compiled %d extraction calls, want 1:\n%s", got, compiled.SQL)
	}
}

// TestBinEdgeBinnedOutputStaysVisibleToNumericPredicates keeps the documented
// promise that a bucketed numeric string is the number it spells: the ordered
// and equality comparisons must use the runtime numeric coercion rather than a
// text-only comparison.
func TestBinEdgeBinnedOutputStaysVisibleToNumericPredicates(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name   string
		source string
	}{
		{name: "greater than", source: `index=gradethis | bin metric span=10 AS band | where band>20 | table band`},
		{name: "at most", source: `index=gradethis | bin metric span=10 AS band | where band<=20 | table band`},
		{name: "equality", source: `index=gradethis | bin metric span=10 AS band | where band=20 | table band`},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			compiled := compileSPL(t, test.source)
			for _, required := range []string{
				`dynamicType("band")`,
				`accurateCastOrNull(toString("band"), 'Int256')`,
			} {
				if !strings.Contains(compiled.SQL, required) {
					t.Fatalf("numeric predicate over a bucket is missing %q:\n%s", required, compiled.SQL)
				}
			}
			if got, want := strings.Count(compiled.SQL, "?"), len(compiled.Args); got != want {
				t.Fatalf("placeholder count = %d, args = %d", got, want)
			}
		})
	}
}

// TestBinEdgeBinnedOutputCarriesPresenceIntoGroupingCommands keeps dedup,
// stats, top, and rare reading the bin stage's published presence rather than
// re-deriving it from the immutable stored document.
func TestBinEdgeBinnedOutputCarriesPresenceIntoGroupingCommands(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name   string
		source string
	}{
		{name: "dedup", source: `index=gradethis | bin metric span=10 AS band | dedup band`},
		{name: "stats", source: `index=gradethis | bin metric span=10 AS band | stats count BY band`},
		{name: "top", source: `index=gradethis | bin metric span=10 AS band | top band`},
		{name: "rare", source: `index=gradethis | bin metric span=10 AS band | rare band`},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			compiled := compileSPL(t, test.source)
			if !strings.Contains(compiled.SQL, `"__os_numeric_bin_output_exists_2"`) {
				t.Fatalf("grouping command ignored the bin stage's presence:\n%s", compiled.SQL)
			}
			if !strings.Contains(compiled.SQL, `dynamicType("band")`) {
				t.Fatalf("grouping command ignored the bucket's runtime type:\n%s", compiled.SQL)
			}
			// A published bucket has no stored descendants, so the flattened
			// object probe must not be reintroduced for it.
			if strings.Contains(compiled.SQL, `startsWith(name, ?), "__os_field_names") AS "__os_dedup`) {
				t.Fatalf("dedup resurrected a stored descendant probe for a bucket:\n%s", compiled.SQL)
			}
			if got, want := strings.Count(compiled.SQL, "?"), len(compiled.Args); got != want {
				t.Fatalf("placeholder count = %d, args = %d\nSQL: %s\nargs: %#v", got, want, compiled.SQL, compiled.Args)
			}
		})
	}
}

// binEdgePlanScope mirrors the unit-test scan scope so a compile failure can
// be inspected without failing the test first.
func binEdgePlanScope() plan.Scope {
	return plan.Scope{
		TenantID:          "tenant-1",
		AuthorizedIndexes: []string{"gradethis"},
		Earliest:          time.Date(2026, 7, 21, 0, 0, 0, 0, time.UTC),
		Latest:            time.Date(2026, 7, 22, 0, 0, 0, 0, time.UTC),
		IndexTimeCutoff:   time.Date(2026, 7, 22, 0, 0, 1, 0, time.UTC),
		VisibilityCutoff:  uint64Pointer(73),
	}
}

func compileBinEdgeSPL(source string) (CompiledQuery, error) {
	parsed, parseErr := spl.Parse(source)
	if parseErr != nil {
		return CompiledQuery{}, parseErr
	}
	logical, buildErr := plan.Build(parsed, binEdgePlanScope())
	if buildErr != nil {
		return CompiledQuery{}, buildErr
	}
	return (Compiler{}).Compile(logical)
}
