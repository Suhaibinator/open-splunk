package clickhouse

import (
	"context"
	"errors"
	"fmt"
	"math"
	"reflect"
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"fortio.org/safecast"

	"github.com/Suhaibinator/open-splunk/internal/eventfields"
	"github.com/Suhaibinator/open-splunk/internal/ianatimezone"
	"github.com/Suhaibinator/open-splunk/internal/jsonnumber"
	"github.com/Suhaibinator/open-splunk/internal/plan"
	"github.com/Suhaibinator/open-splunk/internal/searchtimebounds"
	"github.com/Suhaibinator/open-splunk/internal/spl"
	"github.com/Suhaibinator/open-splunk/internal/splpath"
	"github.com/Suhaibinator/open-splunk/internal/splregex"
	"github.com/Suhaibinator/open-splunk/internal/splrelativetime"
	"github.com/Suhaibinator/open-splunk/internal/spltimeformat"
	"github.com/Suhaibinator/open-splunk/internal/splwildcard"
)

const (
	internalFieldsColumn               = "__os_fields"
	internalFieldNamesColumn           = "__os_field_names"
	internalFieldTypesColumn           = "__os_field_types"
	internalFieldMetadataVersionColumn = "__os_field_metadata_version"
	internalRawEncodingColumn          = "__os_raw_encoding"
	internalSortTimeColumn             = "__os_sort_time"
	internalSortIDColumn               = "__os_sort_event_id"
	internalSortVisibilityColumn       = "__os_sort_visibility_seq"
	internalSortSourceIdentityColumn   = "__os_sort_source_identity"
	rawEncodingUTF8                    = 1
	rawEncodingBinary                  = 2
	rawTextIndexExtractionRegex        = "[A-Za-z0-9_]+"
	rawTextIndexASCIIFoldFrom          = "ſK"
	rawTextIndexASCIIFoldTo            = "sk"
	// Timechart physical columns are executor-only transports. The fixed-schema
	// form returns ordinal/count, while runtime series names remain data and are
	// expanded into the public wide schema only after the complete bounded
	// result has been validated. The zero-based ordinal keeps epoch-aligned
	// bucket starts out of DateTime64 conversions: the first bucket may precede
	// ClickHouse's practical lower bound even though every selected event is
	// representable.
	TimechartOrdinalColumn = "__os_timechart_ordinal"
	// TimechartBucketColumn is present only for a calendar timechart. Fixed
	// grids keep their established ordinal-only transport, while calendar grids
	// carry the exact UTC boundary produced by ClickHouse's timezone database.
	TimechartBucketColumn = "__os_timechart_bucket"
	TimechartCountColumn  = "__os_timechart_count"
	// The fixed-value transport is deliberately distinct from the count
	// transport. Its nullable value and repeated upstream-presence proof let the
	// executor distinguish a real all-ineligible input (publish a null grid)
	// from a wholly empty input (publish only the fixed schema). Percentile,
	// sum, and average share this physical transport while ValueKind retains the
	// aggregate-specific validation policy.
	TimechartValueColumn        = "__os_timechart_value"
	TimechartInputPresentColumn = "__os_timechart_input_present"
	TimechartNamesColumn        = "__os_timechart_names"
	TimechartCountsColumn       = "__os_timechart_counts"
	TimechartValuesColumn       = "__os_timechart_values"
	TimechartValuePresentColumn = "__os_timechart_value_present"
	TimechartInvalidColumn      = "__os_timechart_invalid"
	// Chart physical columns are the same executor-only transport with one
	// additional column: the pivot's row axis is runtime data rather than a
	// plan-time constant, so the row value itself crosses the boundary beside
	// the dense ordinal that proves the server-side order was preserved.
	ChartOrdinalColumn          = "__os_chart_ordinal"
	ChartRowColumn              = "__os_chart_row"
	ChartNamesColumn            = "__os_chart_names"
	ChartCountsColumn           = "__os_chart_counts"
	ChartValuesColumn           = "__os_chart_values"
	ChartValuePresentColumn     = "__os_chart_value_present"
	ChartInvalidColumn          = "__os_chart_invalid"
	ChartRowSemanticBytesColumn = "__os_chart_row_semantic_bytes"
	// SparseEventFieldNamesColumn carries per-row presence metadata beside the
	// public raw fields JSON. The executor consumes it and never publishes it.
	SparseEventFieldNamesColumn = "__os_result_field_names"
	// MaximumRexCapturedBytesPerRow bounds the sum of all capture-group bytes
	// produced by every rex stage for one event. It prevents overlapping named
	// groups and repeated stages from amplifying a maximum-sized event without
	// a query-local ceiling.
	MaximumRexCapturedBytesPerRow = 4 << 20
	// MaximumSpathInputBytes bounds one current-pipeline JSON source. Native
	// ingestion already caps complete events at the same size; this independent
	// guard also covers calculated Strings amplified by earlier commands.
	MaximumSpathInputBytes = 1 << 20
	// MaximumSpathJSONTokens bounds the arrays used to preserve numeric JSON
	// lexemes before structural path evaluation. Runs of punctuation and
	// whitespace share one token, so ordinary documents remain far below it.
	MaximumSpathJSONTokens = 16_384
	maxCompiledQueryBytes  = 256 << 10
	// maxCompiledIfScalarSQLBytes stops nested conditional values before
	// Dynamic comparison lowering can repeatedly duplicate their SQL. The
	// final query has a larger independent ceiling, but enforcing this limit
	// at every conditional node bounds compiler allocations on adversarial
	// depth-limited trees.
	maxCompiledIfScalarSQLBytes = 64 << 10
	// maxCompiledCoalesceScalarSQLBytes provides the same incremental bound for
	// a variadic expression. Without it, 32 individually bounded arguments can
	// allocate a multi-megabyte intermediate before the whole-query guard runs.
	maxCompiledCoalesceScalarSQLBytes = 64 << 10
	// maxCompiledNativeMVScalarSQLBytes is enforced after every native-MV call.
	// Without an incremental ceiling, nested list normalization can duplicate a
	// child's value and state SQL exponentially before the final query-size
	// check gets a chance to run.
	maxCompiledNativeMVScalarSQLBytes = 64 << 10
	// maxCompiledCaseScalarSQLBytes independently bounds the alternating
	// predicate/value expansion of a case expression.
	maxCompiledCaseScalarSQLBytes = 64 << 10
	// maxCompiledTextCaseScalarSQLBytes bounds each Unicode case-conversion
	// expression independently. The lowering references its input exactly
	// once, so nested lower/upper calls grow linearly inside this ceiling.
	maxCompiledTextCaseScalarSQLBytes = 64 << 10
	// maxCompiledTextLengthScalarSQLBytes independently bounds UTF-8 code-point
	// counting. Dynamic lowering binds its input once, so nested text
	// expressions grow linearly inside this ceiling.
	maxCompiledTextLengthScalarSQLBytes = 64 << 10
	// maxCompiledSubstringScalarSQLBytes independently bounds each SQLite-
	// compatible UTF-8 interval expression. Inputs and literal indexes are
	// bound once, so nested substr calls grow linearly inside this ceiling.
	maxCompiledSubstringScalarSQLBytes = 64 << 10
	// maxCompiledToStringScalarSQLBytes independently bounds default scalar
	// conversion. Dynamic input is bound once, so nested calls grow linearly.
	maxCompiledToStringScalarSQLBytes = 64 << 10
	// maxCompiledTextTransformScalarSQLBytes independently bounds each
	// trim/urldecode/digest expression. Like case conversion, the lowering
	// references its input exactly once, so nested calls grow linearly.
	maxCompiledTextTransformScalarSQLBytes = 64 << 10
	// maxCompiledTypeOfScalarSQLBytes independently bounds typeof; Dynamic
	// input is bound once, so nested calls grow linearly.
	maxCompiledTypeOfScalarSQLBytes = 64 << 10
	// maxCompiledCIDRMatchScalarSQLBytes independently bounds cidrmatch; the
	// address is bound once and the prefix is a parameter.
	maxCompiledCIDRMatchScalarSQLBytes = 64 << 10
	// maxCompiledToStringFormatScalarSQLBytes independently bounds the
	// "commas" and "duration" tostring formats. The numeric input is bound
	// once, so nested calls grow linearly.
	maxCompiledToStringFormatScalarSQLBytes = 64 << 10
	// maxCompiledConcatenationScalarSQLBytes independently bounds one flattened
	// period-concatenation expression. Every operand is emitted once and the
	// parser caps operand count, so the check is incremental and linear.
	maxCompiledConcatenationScalarSQLBytes = 64 << 10
	// maxCompiledNumericRoundingScalarSQLBytes independently bounds round,
	// ceil, and floor. Dynamic input is bound once, and integral results let
	// redundant outer ceil/floor calls collapse to identities.
	maxCompiledNumericRoundingScalarSQLBytes = 64 << 10
	// maxCompiledMVCountScalarSQLBytes independently bounds value-cardinality
	// expressions. Dynamic input is bound once, so nested calls grow linearly.
	maxCompiledMVCountScalarSQLBytes = 64 << 10
	// maxCompiledMVSortScalarSQLBytes independently bounds lexical multivalue
	// sorting. Dynamic input is bound once, and already-sorted results collapse
	// to identities so nested calls cannot multiply sort work.
	maxCompiledMVSortScalarSQLBytes = 64 << 10
	// maxCompiledMatchScalarSQLBytes independently bounds regular-expression
	// predicate lowering. Each value is referenced once and each normalized
	// pattern remains a bound argument, so nested composition grows linearly.
	maxCompiledMatchScalarSQLBytes = 64 << 10
	// maxCompiledLikeScalarSQLBytes independently bounds wildcard-predicate
	// lowering. Each value is referenced once and each normalized pattern
	// remains a bound argument, so nested composition grows linearly.
	maxCompiledLikeScalarSQLBytes = 64 << 10
	// maxCompiledStrftimeScalarSQLBytes independently bounds one portable
	// date-format lowering. The time operand is bound once before directive
	// expansion, so nested numeric producers grow linearly inside this ceiling.
	maxCompiledStrftimeScalarSQLBytes = 64 << 10
	// maxCompiledStrptimeScalarSQLBytes independently bounds one portable
	// date-parser lowering. The String operand and parser result are each bound
	// once, so nested text producers grow linearly inside this ceiling.
	maxCompiledStrptimeScalarSQLBytes = 64 << 10
	// maxCompiledRelativeTimeScalarSQLBytes independently bounds one
	// offset-and-snap lowering. Every stage binds its predecessor once, so
	// nested relative_time producers grow linearly inside this ceiling.
	maxCompiledRelativeTimeScalarSQLBytes = 64 << 10
	// MaximumMatchInputBytes bounds regex work against calculated strings. A
	// stored event is capped at 1 MiB; the wider allowance admits one worst-
	// case UTF-8 case conversion while rejecting large replace amplification.
	MaximumMatchInputBytes uint64 = 4 << 20
	// MaximumLikeInputBytes bounds wildcard work against calculated strings.
	// LIKE has the same durable-input and UTF-8 case-expansion envelope as
	// match, while retaining an independent compatibility limit.
	MaximumLikeInputBytes uint64 = 4 << 20
	// MaximumLikeQueryInputBytes bounds the total conservative string bytes
	// scanned by all LIKE occurrences for one result row. This admits sixteen
	// worst-case durable fields or four maximum-size calculated inputs.
	MaximumLikeQueryInputBytes uint64 = 16 << 20
	// MaximumConcatenationOutputBytes bounds the conservative String bytes
	// produced by one concatenation occurrence for one result row.
	MaximumConcatenationOutputBytes uint64 = 4 << 20
	// MaximumConcatenationQueryOutputBytes bounds the sum of conservative
	// concatenation outputs across every occurrence for one result row.
	MaximumConcatenationQueryOutputBytes uint64 = 16 << 20
	// MaximumStringConversionDynamicDecimalBytes is the bounded lexical
	// reservation for one open-schema decimal/v1 conversion used by tostring
	// or concatenation.
	MaximumStringConversionDynamicDecimalBytes = MaximumExactNumericBinTextBytes
	// MaximumStringConversionQueryDynamicDecimalBytes bounds aggregate runtime
	// decimal-envelope parsing across tostring and concatenation per result row.
	MaximumStringConversionQueryDynamicDecimalBytes uint64 = 64 << 10
	// MaximumStrftimeQueryWorkUnits bounds the sum of validated format parts
	// across every strftime occurrence in a forged or parser-produced plan.
	MaximumStrftimeQueryWorkUnits = 16 << 10
	// MaximumStrftimeQueryOutputBytes bounds conservative per-row publication
	// across all strftime occurrences, before the whole-query SQL ceiling.
	MaximumStrftimeQueryOutputBytes uint64 = 64 << 10
	// MaximumStrptimeInputBytes bounds the text examined by one parser call.
	// Longer stored values fail closed to null at runtime rather than making a
	// durable 1 MiB field a plan-time rejection.
	MaximumStrptimeInputBytes uint64 = 4 << 10
	// MaximumStrptimeQueryWorkUnits bounds the sum of validated format parts
	// across every strptime occurrence in a forged or parser-produced plan.
	MaximumStrptimeQueryWorkUnits = 16 << 10
	// MaximumStrptimeQueryInputBytes bounds aggregate date-parser work for one
	// result row after every occurrence applies its per-value input ceiling.
	MaximumStrptimeQueryInputBytes uint64 = 64 << 10
	// MaximumRelativeTimeQueryWorkUnits bounds total validated specifier bytes
	// examined across every relative_time occurrence in one logical plan.
	MaximumRelativeTimeQueryWorkUnits = 16 << 10
	// MaximumRelativeTimeQueryOperations independently bounds the total
	// calendar/elapsed operations lowered across one logical plan.
	MaximumRelativeTimeQueryOperations = 256
	// MaximumUnixTimestampDynamicDecimalBytes reuses the bounded exact-decimal
	// lexical envelope for one runtime timestamp conversion shared by
	// relative_time and strftime.
	MaximumUnixTimestampDynamicDecimalBytes = MaximumExactNumericBinTextBytes
	// MaximumUnixTimestampQueryDynamicDecimalBytes bounds aggregate runtime
	// parsing of open-schema decimal timestamp envelopes across every
	// relative_time and strftime occurrence per result row.
	MaximumUnixTimestampQueryDynamicDecimalBytes uint64 = 64 << 10
	minimumRelativeTimeUnixNanoseconds                  = searchtimebounds.MinimumUnixSeconds *
		1_000_000_000
	maximumRelativeTimeUnixNanoseconds = searchtimebounds.MaximumUnixSeconds *
		1_000_000_000
	// strptime accepts authored civil dates from Splunk's documented lower
	// bound through ClickHouse DateTime64's portable upper calendar year.
	// Validate the authored date before timezone conversion so explicit
	// offsets cannot move a supported date across either policy boundary.
	minimumStrptimeCivilDate = 19710101
	maximumStrptimeCivilDate = 22991231
	// MaximumStoredScalarBytes mirrors the hard ingestion ceiling for one
	// complete event and therefore bounds every durable scalar source.
	MaximumStoredScalarBytes = 1 << 20
	// MaximumMVCountTaggedPayloadBytes mirrors the hard ingestion ceiling for
	// one complete event. It keeps adversarial raw storage from driving
	// unbounded regular-expression work while admitting every envelope that
	// could have passed the supported ingestion path.
	MaximumMVCountTaggedPayloadBytes = MaximumStoredScalarBytes
	// ClickHouse accepts UInt64 substring arguments syntactically but treats
	// values above MaxInt64 as signed internally. Wider literals use the
	// Int128 interval fallback instead of a native fast path.
	maximumNativeSubstringInteger uint64 = 1<<63 - 1
	maxCompiledAssignments               = 64
	maxCompiledExtractionOutputs         = 64
	// Parser-produced predicates are already bounded by a 1,024-token source
	// and 32 eval/where leaves. Revalidate a deliberately wider structural
	// ceiling here so forged logical plans cannot drive unbounded recursive
	// validation or materialization-field walks.
	maxCompiledPredicateNodes = 2048
	maxCompiledPredicateDepth = 1024
	maxTimechartLabelBytes    = 256
	// MaximumEventStatsInputRows bounds the complete relation enriched by one
	// eventstats stage. The compiler reads one additional sentinel row and
	// fails the whole search instead of annotating a partial relation.
	MaximumEventStatsInputRows uint64 = 10_000
	// MaximumStreamStatsInputRows bounds the complete ordered relation consumed
	// by one streamstats stage. The compiler reads one sentinel row and fails
	// atomically instead of publishing a partial running sequence.
	MaximumStreamStatsInputRows uint64 = 10_000
	// MaximumEventStatsGraphAmplification bounds passes over the one retained
	// materialized bounded leaf in a deferred stack. Every global stage has two
	// consumers and every grouped stage has three; final result, analysis, and
	// validation consumers are included before the query is emitted.
	MaximumEventStatsGraphAmplification uint64 = 128
	eventStatsOrdinarySourceFanout      uint64 = 1
	eventStatsChartSourceFanout         uint64 = 2
	eventStatsSummarySourceFanout       uint64 = 3
	eventStatsCatalogSourceFanout       uint64 = 5
	// maxChartRowValues bounds the chart pivot's runtime row axis. It is a
	// deliberate resource policy rather than Splunk's configurable
	// maxresultrows truncation: exceeding it fails the whole search.
	maxChartRowValues = 10_000
	// MaximumStatsDistinctValuesPerGroup bounds the exact string set retained
	// by one dc measure in one group. MAX+1 is used as an overflow sentinel so
	// results at or below the boundary stay exact and larger results fail
	// atomically instead of becoming approximate.
	MaximumStatsDistinctValuesPerGroup = 100_000
	// MaximumStatsValuesPerGroup is lower than the dc ceiling because values
	// publishes every retained string and the recursive result transport keeps
	// one typed cell per element.
	MaximumStatsValuesPerGroup = 10_000
	// MaximumStatsValuesBytesPerGroup bounds the raw lexical String bytes
	// published by one values measure in one group. The ClickHouse query memory
	// limit independently bounds the aggregate state before this post-aggregate
	// result check runs.
	MaximumStatsValuesBytesPerGroup = 512 << 10
	// The result-wide ceilings count every published values alias independently,
	// even when aggregate state is shared. They bound both transforming values
	// results and row-preserving eventstats annotation before a later filter,
	// projection, sort, or row limit can hide them.
	MaximumStatsValuesPerResult      = 100_000
	MaximumStatsValuesBytesPerResult = 8 << 20
	// MaximumStatsListValuesPerGroup matches Splunk's fixed list() behavior:
	// only the first 100 eligible values in pipeline order are published.
	MaximumStatsListValuesPerGroup = 100
	// List results share values()'s publication-byte policy while retaining
	// separate whole-result accounting and diagnostics. The aggregate state is
	// already element-bounded by groupArraySortedArray.
	MaximumStatsListBytesPerGroup   = MaximumStatsValuesBytesPerGroup
	MaximumStatsListValuesPerResult = MaximumStatsValuesPerResult
	MaximumStatsListBytesPerResult  = MaximumStatsValuesBytesPerResult
	// MaximumMVSortValues admits every bounded values() result while keeping a
	// forged or corrupted array from driving unbounded per-row sort work.
	MaximumMVSortValues = MaximumStatsValuesPerGroup
	// MaximumMVSortBytes admits every durable event multivalue and every
	// bounded values()/list() result. Larger raw-storage values fail closed.
	MaximumMVSortBytes = MaximumStoredScalarBytes

	// UnsupportedStatsByValueMarker is emitted before an object, nested
	// container, or other unsupported Dynamic value can create a stats BY group.
	// The executor classifies it without exposing SQL or storage details.
	UnsupportedStatsByValueMarker = "open-splunk: stats BY contains an unsupported value"
	// EventStatsInputLimitMarker classifies an eventstats stage whose upstream
	// relation exceeded the bounded row-enrichment contract.
	EventStatsInputLimitMarker = "open-splunk: eventstats input exceeds the supported limit"
	// StreamStatsInputLimitMarker classifies a streamstats stage whose ordered
	// upstream relation exceeded MaximumStreamStatsInputRows.
	StreamStatsInputLimitMarker = "open-splunk: streamstats input exceeds the supported limit"
	// UnsupportedStatsMeasureValueMarker is emitted when a string-oriented
	// stats measure encounters an object or nested container that has no
	// scalar SPL representation.
	UnsupportedStatsMeasureValueMarker = "open-splunk: stats measure requires scalar values"
	// ExactDistinctLimitMarker classifies an exact dc state that exceeded its
	// per-group, per-measure cardinality ceiling.
	ExactDistinctLimitMarker = "open-splunk: exact distinct values exceed the supported limit"
	// StatsValuesBytesLimitMarker classifies an exact values result whose
	// per-group raw lexical payload exceeded the supported byte ceiling.
	StatsValuesBytesLimitMarker = "open-splunk: stats values bytes exceed the supported limit"
	// StatsValuesLimitMarker classifies a stats values cell or complete
	// transforming result that exceeded its published element ceiling.
	StatsValuesLimitMarker = "open-splunk: stats values exceed the supported limit"
	// EventStatsValuesBytesLimitMarker classifies a row-preserving values result
	// whose per-cell or complete annotated raw lexical payload exceeded its byte
	// ceiling.
	EventStatsValuesBytesLimitMarker = "open-splunk: eventstats values bytes exceed the supported limit"
	// EventStatsValuesLimitMarker classifies a row-preserving values result whose
	// per-cell or complete annotated element count exceeded its ceiling.
	EventStatsValuesLimitMarker = "open-splunk: eventstats values exceed the supported limit"
	// EventStatsListBytesLimitMarker classifies a row-preserving list result
	// whose selected first-100 cell or complete repeated annotation exceeds the
	// supported lexical byte ceiling.
	EventStatsListBytesLimitMarker = "open-splunk: eventstats list bytes exceed the supported limit"
	// EventStatsListLimitMarker classifies a complete row-preserving list
	// annotation with too many repeated published elements. Values after the
	// first 100 in one scope are truncated rather than treated as overflow.
	EventStatsListLimitMarker = "open-splunk: eventstats list exceeds the supported limit"
	// StatsListBytesLimitMarker classifies the selected first-100 list values
	// when their per-cell or whole-result lexical payload is too large.
	StatsListBytesLimitMarker = "open-splunk: stats list bytes exceed the supported limit"
	// StatsListLimitMarker classifies a complete transforming result with too
	// many list elements across groups and aliases. Per-cell values beyond the
	// documented first 100 are truncated rather than treated as overflow.
	StatsListLimitMarker = "open-splunk: stats list exceeds the supported result limit"
	// UnsupportedDedupValueMarker is emitted when a complete dedup key contains
	// a runtime list or object. It is intentionally stable for executor-side
	// classification and is never returned verbatim to clients.
	UnsupportedDedupValueMarker = "open-splunk: dedup requires scalar fields"
	// RexCaptureLimitMarker lets the executor map the deliberate throwIf guard
	// to a stable resource-limit result without exposing generated SQL.
	RexCaptureLimitMarker = "open-splunk: rex capture bytes exceed the per-row limit"
	// SpathInputLimitMarker classifies an oversized calculated JSON source as a
	// resource limit without retaining source bytes or generated SQL.
	SpathInputLimitMarker = "open-splunk: spath input bytes exceed the per-row limit"
	// SpathJSONLexemeLimitMarker classifies a structurally adversarial JSON
	// source before its two bounded lexical projections are constructed.
	SpathJSONLexemeLimitMarker = "open-splunk: spath JSON token count exceeds the per-row limit"
	// NativeMVMembersLimitMarker classifies a split, append, zip, wildcard
	// spath, or runtime normalization whose complete per-row list exceeds the
	// shared member ceiling.
	NativeMVMembersLimitMarker = "open-splunk: native multivalue members exceed the per-row limit"
	// NativeMVPayloadLimitMarker classifies a native list whose canonical
	// member-text payload exceeds the shared per-row byte ceiling.
	NativeMVPayloadLimitMarker = "open-splunk: native multivalue payload exceeds the per-row limit"
	// UnsupportedNativeMVValueMarker is deliberately separate from resource
	// markers so the executor can return an unsupported-value result for Bytes,
	// temporal values, objects, nested lists, and non-finite numbers.
	UnsupportedNativeMVValueMarker = "open-splunk: native multivalue contains an unsupported value"
	// UnsupportedSpathValueMarker is emitted when an explicitly selected JSON
	// leaf is a container that the bounded result contract cannot publish.
	UnsupportedSpathValueMarker = "open-splunk: spath selected value is outside the supported scalar domain"
	// UnsupportedNumericBinValueMarker is emitted when a mathematically correct
	// numeric bucket cannot be represented by the input field's fixed type, or
	// when a floating-point input or result is not finite.
	UnsupportedNumericBinValueMarker = "open-splunk: numeric bin value is outside the supported range"
	// ChartRowLimitMarker is emitted when a chart's runtime row axis exceeds
	// its bounded ceiling. The guard runs before the ordered result is
	// produced, so the search fails atomically rather than truncating.
	ChartRowLimitMarker = "open-splunk: chart row values exceed the supported limit"
)

// ChartRowKind is the backend-neutral public value kind of a chart's row
// column. The compiler derives it from the row field's compile-time type so
// every result validator admits the same first column without re-deriving it
// from a ClickHouse type name.
type ChartRowKind uint8

const (
	ChartRowKindInvalid ChartRowKind = iota
	ChartRowKindString
	ChartRowKindSigned
	ChartRowKindUnsigned
	ChartRowKindDouble
	ChartRowKindBool
	ChartRowKindTime
	// ChartRowKindMixed is the String transport whose public column is the
	// Mixed, nullable column stats BY publishes for _raw. _raw legitimately
	// carries non-UTF-8 bytes, so its cells cross the boundary as either a
	// string or a byte string, exactly as the ordinary result path publishes
	// them.
	ChartRowKindMixed
)

// ChartValueKind identifies the public cell policy carried by a chart's
// runtime-named series. The zero value is invalid so a partially initialized
// compiled contract fails closed at every consumer boundary.
type ChartValueKind uint8

const (
	ChartValueKindInvalid ChartValueKind = iota
	ChartValueKindCount
	ChartValueKindSum
	ChartValueKindAverage
	ChartValueKindPercentile
)

// Valid reports whether kind selects one supported chart cell policy.
func (kind ChartValueKind) Valid() bool {
	switch kind {
	case ChartValueKindCount, ChartValueKindSum, ChartValueKindAverage,
		ChartValueKindPercentile:
		return true
	default:
		return false
	}
}

var physicalIdentifier = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

// Compiler lowers backend-neutral logical plans to parameterized ClickHouse
// SQL. Database and table are trusted configuration and still pass a strict
// identifier allowlist; all user-authored values are query parameters.
type Compiler struct {
	Database string
	Table    string

	// lookupResolutions is an ordered, detached control-plane authority. It is
	// populated only through WithLookupResolutions; ordinary struct literals
	// cannot attach asset rows to authored definition names.
	lookupResolutions []LookupResolution
}

// CompiledQuery is executable SQL plus ordered bind arguments and public
// result fields. Internal helper columns never appear in OutputFields.
type CompiledQuery struct {
	SQL          string
	Args         []any
	OutputFields []string
	// OutputPresentations, when nonempty, is aligned exactly by ordinal with
	// OutputFields. Zero entries carry no presentation metadata. The compiler
	// attaches a display-only flat multivalue delimiter to stats list/values and
	// nomv outputs, and a sparkline semantic bit to stats sparkline outputs. The
	// authoritative typed cells and export behavior remain unchanged.
	OutputPresentations []ResultFieldPresentation
	// ContainerOutputs maps selected public Dynamic ordinals to deterministic
	// trailing metadata columns. The executor consumes those columns without
	// exposing them in the public schema.
	ContainerOutputs []ResultContainerOutput
	// OptionalMultivalueOutputs maps selected Array(String) or Array(Dynamic)
	// ordinals to a trailing tri-state sidecar. It is the sealed native-list
	// transport that distinguishes missing, explicit null, and present lists,
	// including present-empty arrays.
	OptionalMultivalueOutputs []ResultOptionalMultivalueOutput
	// StringOrBytesOutputs identifies selected physical String ordinals whose
	// byte-preserving SPL lineage admits either a UTF-8 String cell or a Bytes
	// cell. The sealed descriptor prevents the executor from narrowing a
	// byte-capable stats BY key to a String-only public schema.
	StringOrBytesOutputs []ResultStringOrBytesOutput
	Timechart            *TimechartOutput
	Chart                *ChartOutput
	// SparseFields marks ordinary raw-event output whose public fields object
	// must be reconstructed from the appended private presence column.
	SparseFields bool
	// SparseFieldsSubset permits the sealed sparse presence array to select a
	// subset of paths from the immutable Dynamic payload. Only fields wildcard
	// projections mint this contract; ordinary sparse output still requires an
	// exact payload/metadata match.
	SparseFieldsSubset bool
	// atomicResult is compiler-owned evidence that the query may surface a
	// sanitized runtime-value failure. The executor must consume and close the
	// complete result before invoking the sink. The production manager supplies
	// the transactional staging boundary that makes those subsequent sink calls
	// publicly atomic.
	atomicResult bool

	// relationalDepth is compiler evidence, not part of the execution
	// contract. Keeping it private prevents callers from treating the guard as
	// a tunable query option while allowing terminal analysis compilers to
	// extend an already validated event relation without reparsing SQL.
	relationalDepth           int
	relationalDepthRange      spl.Range
	readScope                 compiledReadScope
	validationDummyProjection []string
	// sourceFanout records how many physical consumers the terminal lowering
	// has for a chronology-validated source. It stays private because it is
	// compiler resource evidence, not an executor transport contract.
	sourceFanout uint64
	// statsPartitionsMaxThreadsHint is a compiler-owned, whole-query execution
	// cap derived from stats partitions. ClickHouse exposes max_threads only at
	// query scope, so this is an Open Splunk approximation rather than Splunk's
	// stage-local reduce-partition behavior. Zero means no stats stage supplied
	// a hint; otherwise the sealed value is in [1, 4].
	statsPartitionsMaxThreadsHint uint8
	// lookupTables are compiler-owned native-block payloads for exact lookup
	// stages. Keeping them outside Args prevents clickhouse-go from expanding an
	// admitted multi-megabyte asset into the bounded SQL text.
	lookupTables []compiledLookupExternalTable
	// automaticLookupReplay retains only selector, logical mapping, and stable
	// placement authority. Exact identities and cells remain in lookupTables.
	automaticLookupReplay []retainedAutomaticLookup

	// executionSeal binds the complete executable contract, including every
	// bind value and result-shape field. knowledgeEvidence exists only for a
	// parser-owned plan.Build query and is covered by the same seal.
	executionSeal     *compiledExecutionSeal
	knowledgeEvidence *knowledgeCompilationEvidence
}

// RequiresAtomicResult reports whether execution must validate the complete
// backend stream before invoking the result sink. Production visibility is
// atomic because the manager stages those sink calls and commits once Execute
// succeeds; another sink must provide equivalent private staging if its calls
// are externally observable. The value is covered by the compiled execution
// seal and cannot be enabled or disabled by callers.
func (compiled CompiledQuery) RequiresAtomicResult() bool {
	return compiled.atomicResult
}

// ChartOutput describes the bounded runtime-wide pivot contract. Both axes are
// runtime data, so the row column's public name and value kind are carried
// beside the series bounds and the physical type the transport must present.
type ChartOutput struct {
	RowField         string
	RowKind          ChartRowKind
	RowDatabaseType  string
	RowLimit         uint64
	MaxSeries        uint16
	MaxLabelBytes    uint16
	ValueKind        ChartValueKind
	RowSemanticBytes bool
}

// TimechartMode identifies the executor transport and public schema contract.
type TimechartMode uint8

const (
	// TimechartModeRuntimeWide is the zero value so existing dynamic compiler
	// fixtures remain explicit through their bounded series metadata.
	TimechartModeRuntimeWide TimechartMode = iota
	TimechartModeFixedCount
	TimechartModeFixedValue
	TimechartModeRuntimeWideValue
	// TimechartModeFixedFieldCount carries a private input-presence flag in
	// addition to the ordinal and unsigned occurrence count. Keeping it
	// distinct from row count prevents a public output name from selecting a
	// physical transport protocol.
	TimechartModeFixedFieldCount
	// MaximumTimechartSeries bounds the runtime-selected ordinary and sentinel
	// columns carried by either wide timechart transport.
	MaximumTimechartSeries uint16 = 12
	// MaximumTimechartLabelBytes bounds one raw runtime series label before its
	// reserved-name normalization is applied.
	MaximumTimechartLabelBytes uint16 = maxTimechartLabelBytes
)

// TimechartValueKind identifies the semantic policy carried by the shared
// nullable-Float64 transport. The zero value is invalid so a partially
// initialized compiled contract fails closed at every consumer boundary.
type TimechartValueKind uint8

const (
	TimechartValueKindInvalid TimechartValueKind = iota
	TimechartValueKindPercentile
	TimechartValueKindSum
	TimechartValueKindAverage
)

// Valid reports whether kind selects a supported nullable-value policy.
func (kind TimechartValueKind) Valid() bool {
	switch kind {
	case TimechartValueKindPercentile, TimechartValueKindSum, TimechartValueKindAverage:
		return true
	default:
		return false
	}
}

// TimechartOutput describes a bounded fixed-count, fixed-value, or runtime-wide
// result contract. ValueField names a fixed field-count public output or a
// fixed nullable-Double result; it stays empty for row count and runtime-wide
// results because their public fields are predetermined or selected by split
// values.
type TimechartOutput struct {
	Mode        TimechartMode
	FirstBucket time.Time
	Span        time.Duration
	// Calendar selects the private exact-boundary transport. It is mutually
	// exclusive with a positive fixed Span and is covered by the execution seal.
	Calendar      bool
	BucketCount   uint64
	MaxSeries     uint16
	MaxLabelBytes uint16
	// ValueKind is populated for both fixed and runtime-wide nullable values.
	// Together ValueField and ValueKind bind each private transport to its
	// aggregate validation policy instead of trusting mutable OutputFields alone.
	ValueField string
	ValueKind  TimechartValueKind
}

func validTimechartOutputSpanContract(output *TimechartOutput) bool {
	return output != nil &&
		((output.Calendar && output.Span == 0) ||
			(!output.Calendar && output.Span > 0))
}

// timechartSplitMaxSeries is the runtime series allowance a timechart split
// publishes: its ordinary series limit plus each enabled NULL and OTHER
// sentinel series. The planner derives DynamicOutput.MaxSeries the same way,
// so a forged plan whose allowance disagrees with its split is rejected.
func timechartSplitMaxSeries(split *plan.TimechartSplit) uint16 {
	series := split.SeriesLimit
	if split.IncludeNull {
		series++
	}
	if split.IncludeOther {
		series++
	}
	return series
}

// RuntimeWideBoundsValid reports whether the dynamic-series metadata is safe
// for both the executor transport and the public search-job schema boundary.
func (output TimechartOutput) RuntimeWideBoundsValid() bool {
	return output.MaxSeries > 0 && output.MaxSeries <= MaximumTimechartSeries &&
		output.MaxLabelBytes > 0 && output.MaxLabelBytes <= MaximumTimechartLabelBytes
}

// Compile compiles one plan without mutating it.
func (c Compiler) Compile(query *plan.Query) (CompiledQuery, error) {
	return c.CompileContext(context.Background(), query)
}

// CompileContext compiles one plan without mutating it and interrupts lookup
// validation, transposition, and sealing when ctx is canceled. Compile remains
// the compatibility entry point for callers without an operation context.
func (c Compiler) CompileContext(
	ctx context.Context,
	query *plan.Query,
) (CompiledQuery, error) {
	if ctx == nil {
		return CompiledQuery{}, errors.New("compile ClickHouse query: context is nil")
	}
	if err := ctx.Err(); err != nil {
		return CompiledQuery{}, err
	}
	compiled, err := c.compileWithFinalizerContext(
		ctx,
		query,
		finalizeOrdinaryQuery,
		true,
	)
	if err != nil {
		return CompiledQuery{}, err
	}
	return compiled, nil
}

type queryFinalizer func(
	relation compiledRelation,
	state compileState,
	args []any,
	scan *plan.Scan,
	aliasSequence int,
) (CompiledQuery, error)

type eventAnalysisResultContract struct {
	columns      []string
	order        string
	sourceFanout uint64
}

type eventAnalysisFinalizationPolicy struct {
	materializeSharedCTEs bool
	includeResultOrder    bool
}

func eventAnalysisFinalizationPolicyFor(
	barriers []compiledChronologicalBarrier,
) eventAnalysisFinalizationPolicy {
	return eventAnalysisFinalizationPolicy{
		materializeSharedCTEs: !hasPrerequisiteChronologicalBarrier(barriers),
		includeResultOrder:    len(barriers) == 0,
	}
}

func writeCTEOpening(sql *strings.Builder, materialized bool) {
	if materialized {
		sql.WriteString(" AS MATERIALIZED (")
		return
	}
	sql.WriteString(" AS (")
}

// compileEventAnalysis proves that the final relation still consists of
// individual events before exposing it to an analysis-specific projection.
func (c Compiler) compileEventAnalysis(query *plan.Query, finalize queryFinalizer) (CompiledQuery, error) {
	return c.compileEventAnalysisContext(context.Background(), query, finalize)
}

func (c Compiler) compileEventAnalysisContext(
	ctx context.Context,
	query *plan.Query,
	finalize queryFinalizer,
) (CompiledQuery, error) {
	if err := plan.ValidateFieldAnalysisEligibility(query); err != nil {
		return CompiledQuery{}, err
	}
	return c.compileWithFinalizerContext(ctx, query, finalize, false)
}

func wrapEventAnalysisValidation(
	compiled CompiledQuery,
	state compileState,
	contract eventAnalysisResultContract,
	aliasSequence int,
) (CompiledQuery, error) {
	if len(state.chronologicalBarriers) == 0 {
		return compiled, nil
	}
	if len(contract.columns) == 0 {
		return CompiledQuery{}, errors.New(
			"compile ClickHouse event analysis: validation result schema is empty",
		)
	}
	projection := make([]string, 0, len(contract.columns))
	for _, name := range contract.columns {
		projection = append(projection, quoteIdentifier(name))
	}
	return wrapChronologicalValidation(
		compiled.SQL,
		compiled.relationalDepth,
		compiled.relationalDepthRange,
		state.chronologicalBarriers,
		projection,
		contract.columns,
		contract.order,
		contract.sourceFanout,
		compiled,
		aliasSequence,
	)
}

// compileWithFinalizer lowers every logical operator once, then delegates the
// final projection. permitTerminalWideOperators is reserved for ordinary search
// compilation; event analyses must consume only the proven event relation and
// therefore may reach neither timechart nor chart.
func (c Compiler) compileWithFinalizer(query *plan.Query, finalize queryFinalizer, permitTerminalWideOperators bool) (CompiledQuery, error) {
	return c.compileWithFinalizerContext(
		context.Background(),
		query,
		finalize,
		permitTerminalWideOperators,
	)
}

func validateCompiledExtractionBudgets(operators []plan.Operator) (authoredKnowledgeCompilation, error) {
	var evidence authoredKnowledgeCompilation
	regexBudget := authoredRegexProgramBudget{
		evidence: &evidence,
	}
	outputs := 0
	spathWorkUnits := 0
	for _, operator := range operators {
		if err := regexBudget.visitOperator(operator); err != nil {
			return authoredKnowledgeCompilation{}, err
		}
		switch operator := operator.(type) {
		case *plan.RegexFilter:
			if operator == nil {
				continue
			}
			patternRange := regexFilterPatternRange(operator)
			validated, err := splregex.CompileMatchPattern(operator.Pattern)
			if err != nil {
				if splregex.IsMatchComplexityError(err) {
					return authoredKnowledgeCompilation{}, &plan.Diagnostic{
						Code:    "SPL_QUERY_TOO_COMPLEX",
						Message: "regex regular expression exceeds the shared match resource limit",
						Range:   patternRange,
					}
				}
				return authoredKnowledgeCompilation{}, &plan.Diagnostic{
					Code:    "SPL_UNSUPPORTED_REGEX",
					Message: "regex regular expression is outside the supported RE2-compatible subset",
					Range:   patternRange,
				}
			}
			if err := regexBudget.chargeMatchStyle(validated.ProgramWorkUnits, patternRange); err != nil {
				return authoredKnowledgeCompilation{}, err
			}
		case *plan.Extract:
			if operator == nil {
				continue
			}
			validated, err := validateExtractOperator(operator)
			if err != nil {
				return authoredKnowledgeCompilation{}, err
			}
			if err := regexBudget.chargeShared(validated.ProgramWorkUnits); err != nil {
				return authoredKnowledgeCompilation{}, err
			}
			outputs += len(operator.Captures)
		case *plan.ExtractJSON:
			if operator == nil {
				continue
			}
			steps, err := validateExtractJSONOperator(operator)
			if err != nil {
				return authoredKnowledgeCompilation{}, err
			}
			outputs++
			spathWorkUnits += splpath.EvaluationWorkUnits(steps)
			if spathWorkUnits > splpath.MaximumEvaluationWorkUnits {
				return authoredKnowledgeCompilation{}, &plan.Diagnostic{
					Code: "SPL_QUERY_TOO_COMPLEX",
					Message: fmt.Sprintf(
						"spath stages require more than %d JSON evaluation work units per row",
						splpath.MaximumEvaluationWorkUnits,
					),
					Range: operator.Range,
				}
			}
		}
		if outputs > maxCompiledExtractionOutputs {
			return authoredKnowledgeCompilation{}, &plan.Diagnostic{
				Code: "SPL_QUERY_TOO_COMPLEX",
				Message: fmt.Sprintf(
					"search creates more than %d extraction output fields",
					maxCompiledExtractionOutputs,
				),
				Range: operator.SourceRange(),
			}
		}
	}
	evidence.extractionOutputs = uint32(outputs)
	evidence.jsonEvaluationWork = uint32(spathWorkUnits)
	return evidence, nil
}

func compileMatchPatternForBackend(
	pattern string,
	sourceRange spl.Range,
) (splregex.MatchPattern, error) {
	compiled, err := splregex.CompileMatchPattern(pattern)
	if err == nil {
		return compiled, nil
	}
	if splregex.IsMatchComplexityError(err) {
		return splregex.MatchPattern{}, &plan.Diagnostic{
			Code: "SPL_QUERY_TOO_COMPLEX",
			Message: fmt.Sprintf(
				"match regular expression exceeds the %d-byte or %d-work-unit limit",
				splregex.MaximumMatchPatternBytes,
				splregex.MaximumMatchProgramWorkUnits,
			),
			Range: sourceRange,
		}
	}
	return splregex.MatchPattern{}, fmt.Errorf(
		"compile ClickHouse match: regular expression is outside the supported RE2 subset: %w",
		err,
	)
}

func compileLikePatternForBackend(
	pattern string,
	sourceRange spl.Range,
) (splwildcard.LikePattern, error) {
	compiled, err := splwildcard.CompileLikePattern(pattern)
	if err == nil {
		return compiled, nil
	}
	if splwildcard.IsLikeComplexityError(err) {
		return splwildcard.LikePattern{}, &plan.Diagnostic{
			Code: "SPL_QUERY_TOO_COMPLEX",
			Message: fmt.Sprintf(
				"like pattern exceeds the %d-byte or %d-work-unit limit",
				splwildcard.MaximumLikePatternBytes,
				splwildcard.MaximumLikePatternWorkUnits,
			),
			Range: sourceRange,
		}
	}
	return splwildcard.LikePattern{}, &plan.Diagnostic{
		Code:    "SPL_UNSUPPORTED_LIKE_PATTERN",
		Message: "like pattern must be valid UTF-8 without NUL bytes or an unpaired terminal backslash",
		Range:   sourceRange,
	}
}

func isNilPlanOperator(operator plan.Operator) bool {
	if operator == nil {
		return true
	}
	value := reflect.ValueOf(operator)
	return value.Kind() == reflect.Pointer && value.IsNil()
}

func finalizeOrdinaryQuery(
	relation compiledRelation,
	state compileState,
	args []any,
	_ *plan.Scan,
	aliasSequence int,
) (CompiledQuery, error) {
	projection, outputFields, err := finalProjection(state)
	if err != nil {
		return CompiledQuery{}, err
	}
	outputPresentations, err := resultFieldPresentations(state, outputFields)
	if err != nil {
		return CompiledQuery{}, err
	}
	containerOutputs, containerProjection, err := compileResultContainerOutputs(
		state,
		outputFields,
	)
	if err != nil {
		return CompiledQuery{}, err
	}
	optionalMultivalueOutputs, optionalMultivalueProjection, err :=
		compileResultOptionalMultivalueOutputs(state, outputFields, containerOutputs)
	if err != nil {
		return CompiledQuery{}, err
	}
	stringOrBytesOutputs, stringOrBytesProjection, err := compileResultStringOrBytesOutputs(
		state,
		outputFields,
		containerOutputs,
		optionalMultivalueOutputs,
	)
	if err != nil {
		return CompiledQuery{}, err
	}
	sparseFields := exposesRawFieldsPayload(state)
	if sparseFields {
		if !slices.Contains(outputFields, "fields") {
			return CompiledQuery{}, errors.New("compile ClickHouse query: sparse fields output is invalid")
		}
		projection = append(
			projection,
			quoteIdentifier(internalFieldNamesColumn)+" AS "+quoteIdentifier(SparseEventFieldNamesColumn),
		)
	}
	projection = append(projection, containerProjection...)
	projection = append(projection, optionalMultivalueProjection...)
	projection = append(projection, stringOrBytesProjection...)
	if len(state.chronologicalBarriers) > 0 {
		return finalizeChronologicallyValidatedQuery(
			relation,
			state,
			args,
			projection,
			outputFields,
			outputPresentations,
			sparseFields,
			containerOutputs,
			optionalMultivalueOutputs,
			stringOrBytesOutputs,
			aliasSequence,
		)
	}
	aliasSequence++
	fragment := "SELECT " + strings.Join(projection, ", ") + " FROM (" + relation.sql + ") AS " + quoteIdentifier(fmt.Sprintf("_stage_%d", aliasSequence))
	finalOrder := defaultCompiledOrder(state)
	if len(finalOrder) > 0 {
		order, orderErr := compileMaterializedOrder(finalOrder, false)
		if orderErr != nil {
			return CompiledQuery{}, orderErr
		}
		fragment += " ORDER BY " + order
	}
	relation = relation.selectFrom(fragment, relation.ownerRange)
	return withCompiledRelationalDepth(
		CompiledQuery{
			SQL:                       relation.sql,
			Args:                      args,
			OutputFields:              outputFields,
			OutputPresentations:       outputPresentations,
			ContainerOutputs:          containerOutputs,
			OptionalMultivalueOutputs: optionalMultivalueOutputs,
			StringOrBytesOutputs:      stringOrBytesOutputs,
			SparseFields:              sparseFields,
			SparseFieldsSubset:        state.sparseFieldsSubset,
		},
		relation.depth,
		relation.ownerRange,
	), nil
}

func ordinaryChronologicalDummyValue(field fieldState) (string, bool) {
	if field.alwaysNull {
		switch field.kind {
		case fieldKindString, fieldKindInvalid:
			return "CAST(NULL AS Nullable(String))", true
		case fieldKindNumber, fieldKindTime:
			if field.numberType == "" {
				return "", false
			}
			return "CAST(NULL AS Nullable(" + field.numberType + "))", true
		case fieldKindBool:
			return "CAST(NULL AS Nullable(Bool))", true
		case fieldKindStringArray:
			return "CAST(NULL AS Nullable(Array(String)))", true
		case fieldKindDynamicArray:
			return "CAST(NULL AS Nullable(Array(Dynamic)))", true
		case fieldKindDynamic:
			return "CAST(NULL AS Dynamic)", true
		default:
			return "", false
		}
	}

	switch field.kind {
	case fieldKindDynamic:
		return "CAST(NULL AS Dynamic)", true
	case fieldKindString:
		return "CAST('' AS String)", true
	case fieldKindNumber, fieldKindTime:
		if field.numberType == "" {
			return "", false
		}
		return "CAST(0 AS " + field.numberType + ")", true
	case fieldKindBool:
		return "CAST(false AS Bool)", true
	case fieldKindStringArray:
		return "CAST([], 'Array(String)')", true
	case fieldKindDynamicArray:
		return emptyNativeMVSQL(), true
	case fieldKindInvalid:
		return "CAST(NULL AS Nullable(String))", true
	default:
		return "", false
	}
}

// ordinaryChronologicalDummyProjection gives the invalid validation branch an
// exact schema row without reading the complete final input a second time.
// This optimization is deliberately limited to ordinary results, whose field
// states and private container sidecars fully describe every physical column.
// Analysis/chart wrappers retain the engine-inferred zero-row fallback.
func ordinaryChronologicalDummyProjection(
	state compileState,
	outputFields []string,
	sparseFields bool,
	containerOutputs []ResultContainerOutput,
	optionalMultivalueOutputs []ResultOptionalMultivalueOutput,
	stringOrBytesOutputs []ResultStringOrBytesOutput,
) ([]string, bool) {
	projection := make([]string, 0, len(outputFields)+1+len(containerOutputs)*3+
		len(optionalMultivalueOutputs)+len(stringOrBytesOutputs))
	for _, name := range outputFields {
		field, visible := state.visible[name]
		if !visible {
			return nil, false
		}
		value, supported := ordinaryChronologicalDummyValue(field)
		if !supported {
			return nil, false
		}
		projection = append(projection, value+" AS "+quoteIdentifier(name))
	}
	if sparseFields {
		projection = append(
			projection,
			"CAST([], 'Array(String)') AS "+
				quoteIdentifier(SparseEventFieldNamesColumn),
		)
	}
	for _, output := range containerOutputs {
		projection = append(
			projection,
			"CAST([], 'Array(String)') AS "+quoteIdentifier(output.NamesColumn()),
			"CAST([], 'Array(UInt8)') AS "+quoteIdentifier(output.TypesColumn()),
			"toUInt8(0) AS "+quoteIdentifier(output.MetadataVersionColumn()),
		)
	}
	for _, output := range optionalMultivalueOutputs {
		projection = append(
			projection,
			"toUInt8(0) AS "+quoteIdentifier(output.PresentColumn()),
		)
	}
	for _, output := range stringOrBytesOutputs {
		projection = append(
			projection,
			"toUInt8(0) AS "+quoteIdentifier(output.SemanticBytesColumn()),
		)
	}
	return projection, len(projection) > 0
}

func finalizeChronologicallyValidatedQuery(
	relation compiledRelation,
	state compileState,
	args []any,
	projection []string,
	outputFields []string,
	outputPresentations []ResultFieldPresentation,
	sparseFields bool,
	containerOutputs []ResultContainerOutput,
	optionalMultivalueOutputs []ResultOptionalMultivalueOutput,
	stringOrBytesOutputs []ResultStringOrBytesOutput,
	aliasSequence int,
) (CompiledQuery, error) {
	order := ""
	finalOrder := defaultCompiledOrder(state)
	if len(finalOrder) > 0 {
		compiledOrder, orderErr := compileMaterializedOrder(finalOrder, false)
		if orderErr != nil {
			return CompiledQuery{}, orderErr
		}
		order = compiledOrder
	}

	resultColumns := append([]string(nil), outputFields...)
	if sparseFields {
		resultColumns = append(resultColumns, SparseEventFieldNamesColumn)
	}
	for _, output := range containerOutputs {
		resultColumns = append(
			resultColumns,
			output.NamesColumn(),
			output.TypesColumn(),
			output.MetadataVersionColumn(),
		)
	}
	for _, output := range optionalMultivalueOutputs {
		resultColumns = append(resultColumns, output.PresentColumn())
	}
	for _, output := range stringOrBytesOutputs {
		resultColumns = append(resultColumns, output.SemanticBytesColumn())
	}
	dummyProjection, _ := ordinaryChronologicalDummyProjection(
		state,
		outputFields,
		sparseFields,
		containerOutputs,
		optionalMultivalueOutputs,
		stringOrBytesOutputs,
	)
	return wrapChronologicalValidation(
		relation.sql,
		relation.depth,
		relation.ownerRange,
		state.chronologicalBarriers,
		projection,
		resultColumns,
		order,
		eventStatsOrdinarySourceFanout,
		CompiledQuery{
			Args:                      args,
			OutputFields:              outputFields,
			OutputPresentations:       outputPresentations,
			ContainerOutputs:          containerOutputs,
			OptionalMultivalueOutputs: optionalMultivalueOutputs,
			StringOrBytesOutputs:      stringOrBytesOutputs,
			SparseFields:              sparseFields,
			SparseFieldsSubset:        state.sparseFieldsSubset,
			validationDummyProjection: dummyProjection,
		},
		aliasSequence,
	)
}

func wrapCompiledChronologicalValidation(
	compiled CompiledQuery,
	state compileState,
	aliasSequence int,
) (CompiledQuery, error) {
	var resultColumns []string
	sourceFanout := eventStatsOrdinarySourceFanout
	switch {
	case compiled.Chart != nil && compiled.Timechart == nil:
		switch compiled.Chart.ValueKind {
		case ChartValueKindCount:
			sourceFanout = compiled.sourceFanout
			if sourceFanout == 0 {
				// Older direct compiler fixtures describe bare count and predate the
				// private evidence field. Preserve their conservative two-consumer
				// contract while letting count(field)'s raw-group path prove one.
				sourceFanout = eventStatsChartSourceFanout
			}
			if sourceFanout != eventStatsOrdinarySourceFanout &&
				sourceFanout != eventStatsChartSourceFanout {
				return CompiledQuery{}, errors.New(
					"compile ClickHouse query: chart source fanout is invalid",
				)
			}
			resultColumns = []string{
				ChartOrdinalColumn,
				ChartRowColumn,
			}
			if compiled.Chart.RowSemanticBytes {
				resultColumns = append(resultColumns, ChartRowSemanticBytesColumn)
			}
			resultColumns = append(
				resultColumns,
				ChartNamesColumn,
				ChartCountsColumn,
				ChartInvalidColumn,
			)
		case ChartValueKindSum, ChartValueKindAverage, ChartValueKindPercentile:
			// Numeric chart derives label selection and validation from its
			// row/label numeric aggregate, so the scoped relation has one
			// physical consumer rather than count chart's two.
			sourceFanout = eventStatsOrdinarySourceFanout
			resultColumns = []string{
				ChartOrdinalColumn,
				ChartRowColumn,
			}
			if compiled.Chart.RowSemanticBytes {
				resultColumns = append(resultColumns, ChartRowSemanticBytesColumn)
			}
			resultColumns = append(
				resultColumns,
				ChartNamesColumn,
				ChartValuesColumn,
				ChartValuePresentColumn,
				ChartInvalidColumn,
			)
		default:
			return CompiledQuery{}, errors.New(
				"compile ClickHouse query: chart output value kind is invalid",
			)
		}
	case compiled.Timechart != nil && compiled.Chart == nil:
		if !validTimechartOutputSpanContract(compiled.Timechart) {
			return CompiledQuery{}, errors.New(
				"compile ClickHouse query: timechart calendar and fixed span contract is invalid",
			)
		}
		switch compiled.Timechart.Mode {
		case TimechartModeFixedCount:
			resultColumns = []string{TimechartOrdinalColumn, TimechartCountColumn}
		case TimechartModeFixedFieldCount:
			resultColumns = []string{
				TimechartOrdinalColumn,
				TimechartCountColumn,
				TimechartInputPresentColumn,
			}
		case TimechartModeFixedValue:
			resultColumns = []string{
				TimechartOrdinalColumn,
				TimechartValueColumn,
				TimechartInputPresentColumn,
			}
		case TimechartModeRuntimeWide:
			resultColumns = []string{
				TimechartOrdinalColumn,
				TimechartNamesColumn,
				TimechartCountsColumn,
				TimechartInvalidColumn,
			}
		case TimechartModeRuntimeWideValue:
			resultColumns = []string{
				TimechartOrdinalColumn,
				TimechartNamesColumn,
				TimechartValuesColumn,
				TimechartValuePresentColumn,
				TimechartInvalidColumn,
			}
		default:
			return CompiledQuery{}, errors.New(
				"compile ClickHouse query: timechart output mode is invalid",
			)
		}
		if compiled.Timechart.Calendar {
			resultColumns = slices.Insert(
				resultColumns,
				1,
				TimechartBucketColumn,
			)
		}
	default:
		return CompiledQuery{}, errors.New(
			"compile ClickHouse query: chronological terminal output contract is invalid",
		)
	}
	projection := make([]string, 0, len(resultColumns))
	for _, name := range resultColumns {
		projection = append(projection, quoteIdentifier(name))
	}
	resultOrder := quoteIdentifier(resultColumns[0]) + " ASC"
	if compiled.Chart != nil {
		// Chart can carry a private invalid-result sentinel whose ordinal is
		// deliberately zero. Keep it ahead of every real ordinal after an
		// eventstats validation wrapper so the executor observes the atomic
		// rejection before applying the dense public-row sequence contract.
		resultOrder = quoteIdentifier(ChartInvalidColumn) + " DESC, " +
			quoteIdentifier(ChartOrdinalColumn) + " ASC"
	}
	return wrapChronologicalValidation(
		compiled.SQL,
		compiled.relationalDepth,
		compiled.relationalDepthRange,
		state.chronologicalBarriers,
		projection,
		resultColumns,
		resultOrder,
		sourceFanout,
		compiled,
		aliasSequence,
	)
}

func wrapChronologicalValidation(
	inputSQL string,
	inputDepth int,
	ownerRange spl.Range,
	barriers []compiledChronologicalBarrier,
	projection []string,
	resultColumns []string,
	order string,
	sourceFanout uint64,
	compiled CompiledQuery,
	aliasSequence int,
) (CompiledQuery, error) {
	if len(barriers) == 0 || inputSQL == "" || inputDepth <= 0 ||
		len(projection) == 0 || len(resultColumns) != len(projection) ||
		sourceFanout == 0 {
		return CompiledQuery{}, errors.New(
			"compile ClickHouse query: chronological validation envelope is invalid",
		)
	}
	if amplificationErr := validateChronologicalGraphAmplification(
		barriers,
		sourceFanout,
		ownerRange,
	); amplificationErr != nil {
		return CompiledQuery{}, amplificationErr
	}
	definitions := make([]string, 0, len(barriers)+2)
	barrierArgs := make([]any, 0)
	// ClickHouse 26.3 can leave a chain of MATERIALIZED CTEs unplanned when
	// every consumer is another CTE. Preserve only the earliest materialization
	// in the complete graph; it is the physical-scan fence, while every later
	// stage already reads that bounded result graph.
	keptGraphMaterialization := false
	for _, barrier := range barriers {
		for _, definition := range barrier.prerequisiteDefinitions {
			if topLevelMaterializedCTE(definition) {
				if keptGraphMaterialization {
					definition = inlineTopLevelMaterializedCTE(definition)
				} else {
					keptGraphMaterialization = true
				}
			}
			definitions = append(definitions, definition)
		}
		barrierClause := " AS MATERIALIZED ("
		if len(barrier.prerequisiteDefinitions) > 0 {
			barrierClause = " AS ("
		} else if keptGraphMaterialization {
			barrierClause = " AS ("
		} else {
			keptGraphMaterialization = true
		}
		definitions = append(
			definitions,
			barrier.name+barrierClause+barrier.sql+")",
		)
		barrierArgs = append(barrierArgs, barrier.args...)
	}

	finalInput := quoteIdentifier(fmt.Sprintf("__os_chronological_final_input_%d", aliasSequence+1))
	finalInputClause := " AS MATERIALIZED ("
	if keptGraphMaterialization {
		finalInputClause = " AS ("
	} else {
		keptGraphMaterialization = true
	}
	definitions = append(definitions, finalInput+finalInputClause+inputSQL+")")

	validationRows := make([]string, 0, len(barriers))
	maximumBarrierDepth := 0
	for _, barrier := range barriers {
		if len(barrier.validationColumns) == 0 {
			continue
		}
		invalid := make([]string, 0, len(barrier.validationColumns))
		for _, column := range barrier.validationColumns {
			invalid = append(invalid, column+" != 0")
		}
		validationRows = append(
			validationRows,
			"SELECT toUInt8(("+strings.Join(invalid, ") OR (")+
				")) AS "+quoteIdentifier("__os_chronological_invalid")+" FROM "+barrier.name,
		)
		if barrier.depth > maximumBarrierDepth {
			maximumBarrierDepth = barrier.depth
		}
	}

	aliasSequence++
	mainAlias := quoteIdentifier(fmt.Sprintf("_stage_%d", aliasSequence))
	if len(validationRows) == 0 {
		main := "SELECT " + strings.Join(projection, ", ") + " FROM " +
			finalInput + " AS " + mainAlias
		if order != "" {
			main += " ORDER BY " + order
		}
		compiled.SQL = "WITH " + strings.Join(definitions, ", ") + " " + main +
			materializedCTESettingsSQL
		compiled.Args = append(barrierArgs, compiled.Args...)
		return withCompiledRelationalDepth(
			compiled,
			relationalNodeDepth(inputDepth),
			ownerRange,
		), nil
	}

	validationName := quoteIdentifier(fmt.Sprintf(
		"__os_chronological_validation_%d",
		aliasSequence,
	))
	aliasSequence++
	mainValidationAlias := quoteIdentifier(fmt.Sprintf("_stage_%d", aliasSequence))
	main := "SELECT " + strings.Join(projection, ", ") + " FROM " + finalInput +
		" AS " + mainAlias + " CROSS JOIN " + validationName + " AS " +
		mainValidationAlias + " WHERE " + mainValidationAlias + "." +
		quoteIdentifier("__os_chronological_valid") + " = 0"
	if order != "" {
		main += " ORDER BY " + order
	}

	dummy := ""
	dummyDepth := 1
	if len(compiled.validationDummyProjection) == len(resultColumns) {
		dummy = "SELECT " + strings.Join(compiled.validationDummyProjection, ", ")
	} else {
		inferredProjection := make([]string, 0, len(resultColumns))
		for _, name := range resultColumns {
			column := quoteIdentifier(name)
			inferredProjection = append(
				inferredProjection,
				"any("+column+") AS "+column,
			)
		}

		aliasSequence++
		schemaSourceAlias := quoteIdentifier(fmt.Sprintf("_stage_%d", aliasSequence))
		schemaSource := "SELECT " + strings.Join(projection, ", ") + " FROM " +
			finalInput + " AS " + schemaSourceAlias + " LIMIT 0"
		aliasSequence++
		schemaAlias := quoteIdentifier(fmt.Sprintf("_stage_%d", aliasSequence))
		dummy = "SELECT " + strings.Join(inferredProjection, ", ") + " FROM (" +
			schemaSource + ") AS " + schemaAlias
		schemaSourceDepth := relationalNodeDepth(inputDepth)
		dummyDepth = relationalNodeDepth(schemaSourceDepth)
	}

	validationUnion := strings.Join(validationRows, " UNION ALL ")
	validationRowsDepth := relationalNodeDepth(maximumBarrierDepth)
	validationDepth := relationalNodeDepth(validationRowsDepth)
	validation := "SELECT if(maxOrDefault(" + quoteIdentifier("__os_chronological_invalid") +
		") != 0, throwIf(toUInt8(1), '" + UnsupportedStatsMeasureValueMarker +
		"'), toUInt8(0)) AS " + quoteIdentifier("__os_chronological_valid") +
		" FROM (" + validationUnion + ")"
	validationClause := " AS MATERIALIZED ("
	if keptGraphMaterialization {
		// Keep this tiny validation aggregate ordinary so both top-level UNION
		// branches directly consume the eventstats barriers. Only the earliest
		// bounded prerequisite remains materialized; later stages stay in the
		// same flat dependency graph. The aggregate dummy branch below supplies
		// one schema row, so validation is still forced when the analysis is empty.
		validationClause = " AS ("
	}
	definitions = append(definitions, validationName+validationClause+validation+")")

	aliasSequence++
	dummyAlias := quoteIdentifier(fmt.Sprintf("_stage_%d", aliasSequence))
	aliasSequence++
	validationAlias := quoteIdentifier(fmt.Sprintf("_stage_%d", aliasSequence))
	validationBranch := "SELECT " + dummyAlias + ".* FROM (" + dummy + ") AS " +
		dummyAlias + " CROSS JOIN " + validationName + " AS " + validationAlias +
		" WHERE " + validationAlias + "." +
		quoteIdentifier("__os_chronological_valid") + " != 0"

	sql := "WITH " + strings.Join(definitions, ", ") + " " + main + " UNION ALL " +
		validationBranch + materializedCTESettingsSQL

	mainDepth := relationalNodeDepth(inputDepth, validationDepth)
	validationBranchDepth := relationalNodeDepth(dummyDepth, validationDepth)
	resultDepth := relationalNodeDepth(mainDepth, validationBranchDepth)
	compiled.SQL = sql
	compiled.Args = append(barrierArgs, compiled.Args...)
	return withCompiledRelationalDepth(compiled, resultDepth, ownerRange), nil
}

func prependArguments(prefix, existing []any) []any {
	if len(prefix) == 0 {
		return existing
	}
	result := make([]any, 0, len(prefix)+len(existing))
	result = append(result, prefix...)
	return append(result, existing...)
}

func hasPrerequisiteChronologicalBarrier(
	barriers []compiledChronologicalBarrier,
) bool {
	for _, barrier := range barriers {
		if len(barrier.prerequisiteDefinitions) > 0 {
			return true
		}
	}
	return false
}

func validateChronologicalGraphAmplification(
	barriers []compiledChronologicalBarrier,
	sourceFanout uint64,
	fallbackRange spl.Range,
) error {
	hasEventStats := false
	hasValidation := false
	diagnosticRange := fallbackRange
	validationRange := fallbackRange
	for _, barrier := range barriers {
		fanout := barrier.fanout
		if fanout == 0 {
			fanout = 1
		}
		if fanout > 3 {
			return errors.New(
				"compile ClickHouse query: chronological barrier fanout is invalid",
			)
		}
		if fanout > 1 {
			hasEventStats = true
			if barrier.ownerRange != (spl.Range{}) {
				diagnosticRange = barrier.ownerRange
			}
		}
		if len(barrier.validationColumns) > 0 {
			hasValidation = true
			if barrier.ownerRange != (spl.Range{}) {
				validationRange = barrier.ownerRange
			}
		}
	}
	if !hasEventStats && !hasValidation {
		return nil
	}
	if !hasEventStats {
		diagnosticRange = validationRange
	}
	graphName := "stacked eventstats"
	if !hasEventStats {
		graphName = "stacked streamstats"
	}

	overflow := func() error {
		return &plan.Diagnostic{
			Code: "SPL_QUERY_TOO_COMPLEX",
			Message: fmt.Sprintf(
				graphName+" exceeds the maximum deferred execution amplification of %d bounded-leaf reads",
				MaximumEventStatsGraphAmplification,
			),
			Range: diagnosticRange,
		}
	}
	consumers := sourceFanout
	if consumers > MaximumEventStatsGraphAmplification {
		return overflow()
	}
	if hasValidation {
		if consumers > MaximumEventStatsGraphAmplification/2 {
			return overflow()
		}
		consumers *= 2
	}
	for _, barrier := range slices.Backward(barriers) {
		if len(barrier.validationColumns) > 0 {
			if consumers > MaximumEventStatsGraphAmplification-2 {
				return overflow()
			}
			consumers += 2
		}
		fanout := barrier.fanout
		if fanout == 0 {
			fanout = 1
		}
		if consumers > MaximumEventStatsGraphAmplification/fanout {
			return overflow()
		}
		consumers *= fanout
	}
	return nil
}

const materializedCTEOpening = " AS MATERIALIZED ("

func topLevelMaterializedCTE(definition string) bool {
	opening := strings.Index(definition, materializedCTEOpening)
	return opening > 0 && !strings.Contains(definition[:opening], "(")
}

func inlineTopLevelMaterializedCTE(definition string) string {
	opening := strings.Index(definition, materializedCTEOpening)
	if opening <= 0 || strings.Contains(definition[:opening], "(") {
		return definition
	}
	return definition[:opening] + " AS (" +
		definition[opening+len(materializedCTEOpening):]
}

func compileTimeBucket(
	relation compiledRelation,
	state compileState,
	scan *plan.Scan,
	operator *plan.TimeBucket,
	alias string,
) (compiledRelation, compileState, []any, error) {
	if operator == nil {
		return compiledRelation{}, compileState{}, nil, errors.New("compile ClickHouse time bucket: operator is nil")
	}
	if err := validateCanonicalFieldRef("time bucket", "input", operator.Field); err != nil {
		return compiledRelation{}, compileState{}, nil, err
	}
	if err := validateCanonicalFieldRef("time bucket", "output", operator.Output); err != nil {
		return compiledRelation{}, compileState{}, nil, err
	}
	if operator.Field.Name != "_time" || !operator.Field.Canonical {
		return compiledRelation{}, compileState{}, nil, errors.New("compile ClickHouse time bucket: canonical _time field is required")
	}
	calendar := operator.Calendar != plan.CalendarNone
	if calendar {
		if operator.Span != 0 ||
			(operator.Calendar != plan.CalendarDay &&
				operator.Calendar != plan.CalendarWeek) {
			return compiledRelation{}, compileState{}, nil, errors.New(
				"compile ClickHouse time bucket: calendar span is invalid",
			)
		}
	} else if operator.Span < time.Second || operator.Span > 24*time.Hour ||
		operator.Span%time.Second != 0 {
		return compiledRelation{}, compileState{}, nil, errors.New("compile ClickHouse time bucket: fixed span must be from one second through 24 hours")
	}
	if !state.eventRows {
		return compiledRelation{}, compileState{}, nil, &plan.Diagnostic{
			Code:    "SPL_UNSUPPORTED_BIN_INPUT",
			Message: "bin requires source event rows",
			Range:   operator.Range,
		}
	}
	field, ok, err := resolveCompiledField(operator.Field, state)
	if err != nil {
		return compiledRelation{}, compileState{}, nil, err
	}
	if !ok || field.kind != fieldKindTime || !field.canonicalTime {
		return compiledRelation{}, compileState{}, nil, &plan.Diagnostic{
			Code:        "SPL_UNSUPPORTED_BIN_TIME_FIELD",
			Message:     "bin requires the unmodified canonical _time field",
			Range:       operator.Range,
			Suggestions: []string{"run bin before removing, replacing, transforming, or previously binning _time"},
		}
	}
	if scan == nil || !SupportsSearchTimeRange(scan.Earliest, scan.Latest) {
		return compiledRelation{}, compileState{}, nil, errors.New("compile ClickHouse time bucket: scan range is invalid")
	}
	var value string
	var valueArgs []any
	if calendar {
		if state.context == nil {
			return compiledRelation{}, compileState{}, nil, errors.New(
				"compile ClickHouse time bucket: search timezone is required",
			)
		}
		if err := validateCompileContextSearchTimezone(state.context); err != nil {
			return compiledRelation{}, compileState{}, nil, err
		}
		location, err := ianatimezone.Load(state.context.searchTimezone)
		if err != nil {
			return compiledRelation{}, compileState{}, nil, err
		}
		firstBucket := calendarBoundary(
			scan.Earliest,
			operator.Calendar,
			location,
		).UTC()
		if firstBucket.Before(MinimumSearchTime()) {
			return compiledRelation{}, compileState{}, nil, &plan.Diagnostic{
				Code:    "SPL_UNSUPPORTED_BIN_TIME_RANGE",
				Message: "the first calendar bin falls before the supported timestamp range",
				Range:   operator.Range,
				Suggestions: []string{
					"move the search earliest time forward",
					"use a fixed span shorter than 24 hours",
				},
			}
		}
		value = calendarBucketKeySQL(field.valueSQL, operator.Calendar)
		valueArgs = appendCalendarBucketKeyArgs(
			nil,
			operator.Calendar,
			state.context.searchTimezone,
		)
	} else {
		spanNanoseconds := int64(operator.Span)
		firstBucketTicks := floorBucketTicks(scan.Earliest.UnixNano(), spanNanoseconds)
		if firstBucketTicks < MinimumSearchTime().UnixNano() {
			return compiledRelation{}, compileState{}, nil, &plan.Diagnostic{
				Code:    "SPL_UNSUPPORTED_BIN_TIME_RANGE",
				Message: "the first epoch-aligned bin falls before the supported timestamp range",
				Range:   operator.Range,
				Suggestions: []string{
					"use a smaller fixed span",
					"move the search earliest time forward",
				},
			}
		}

		ticks := "reinterpretAsInt64(" + field.valueSQL + ")"
		bucketTicks := "(" + epochFloorBucketNumberSQL(ticks) + ") * ?"
		value = "fromUnixTimestamp64Nano(" + bucketTicks + ", 'UTC')"
		valueArgs = []any{spanNanoseconds, spanNanoseconds, spanNanoseconds}
	}
	fragment, next := compileBucketProjection(relation.sql, state, operator.Field.Name, operator.Output, value, field, alias)
	relation = relation.selectFrom(fragment, operator.Range)
	return relation, next, valueArgs, nil
}

func compileNumericBucket(
	relation compiledRelation,
	state compileState,
	operator *plan.NumericBucket,
	alias string,
	stage int,
) (compiledRelation, compileState, []any, error) {
	if operator == nil {
		return compiledRelation{}, compileState{}, nil, errors.New("compile ClickHouse numeric bucket: operator is nil")
	}
	if err := validateCanonicalFieldRef("numeric bucket", "input", operator.Input); err != nil {
		return compiledRelation{}, compileState{}, nil, err
	}
	if err := validateCanonicalFieldRef("numeric bucket", "output", operator.Output); err != nil {
		return compiledRelation{}, compileState{}, nil, err
	}
	if operator.Span == 0 || operator.Span > plan.MaximumNumericBinSpan {
		return compiledRelation{}, compileState{}, nil, errors.New("compile ClickHouse numeric bucket: span must be between 1 and 2^53-1")
	}
	if operator.Input.Name == "_time" && operator.Input.Canonical {
		return compiledRelation{}, compileState{}, nil, errors.New("compile ClickHouse numeric bucket: canonical _time cannot be a numeric input")
	}

	field, ok, err := resolveCompiledField(operator.Input, state)
	if err != nil {
		return compiledRelation{}, compileState{}, nil, err
	}
	if !ok {
		return compiledRelation{}, compileState{}, nil, unsupportedNumericBinFieldType(operator)
	}
	if field.kind == fieldKindDynamic {
		return compileDynamicNumericBucket(relation, state, operator, field, alias, stage)
	}
	if field.kind != fieldKindNumber {
		return compiledRelation{}, compileState{}, nil, unsupportedNumericBinFieldType(operator)
	}

	// WITH expression aliases can be inherited by nested subqueries in
	// ClickHouse. Make them stage-unique so consecutive bin operators cannot
	// capture one another's span or candidate.
	spanAlias := quoteIdentifier(fmt.Sprintf("__os_numeric_bin_span_%d", stage))
	candidateAlias := quoteIdentifier(fmt.Sprintf("__os_numeric_bin_candidate_%d", stage))
	input := field.valueSQL
	var spanSQL, candidateSQL, valueSQL string
	if intermediate, ok := signedBucketIntermediateType(field.numberType); ok {
		wide := "to" + intermediate + "(" + input + ")"
		spanSQL = "to" + intermediate + "(CAST(? AS UInt64))"
		bucket := "(intDiv(" + wide + ", " + spanAlias + ") - if(" +
			wide + " < 0 AND " + wide + " % " + spanAlias + " != 0, 1, 0)) * " + spanAlias
		candidateSQL = "accurateCastOrNull(" + bucket + ", '" + field.numberType + "')"
		valueSQL = guardedNumericBucketSQL(input, candidateAlias, field.numberType, "", "")
	} else if intermediate, ok := unsignedBucketIntermediateType(field.numberType); ok {
		wide := "to" + intermediate + "(" + input + ")"
		spanSQL = "to" + intermediate + "(CAST(? AS UInt64))"
		bucket := "intDiv(" + wide + ", " + spanAlias + ") * " + spanAlias
		// An unsigned bucket is never larger than its input, so this narrowing
		// cast is mathematically safe and needs no per-row range guard.
		candidateSQL = "CAST(" + bucket + " AS " + field.numberType + ")"
		valueSQL = candidateAlias
	} else if field.numberType == "Float64" {
		spanSQL = "toFloat64(CAST(? AS UInt64))"
		bucket := "floor(toFloat64(" + input + ") / " + spanAlias + ") * " + spanAlias
		candidateSQL = bucket
		finite := "isFinite(toFloat64(" + input + ")) AND isFinite(assumeNotNull(" + candidateAlias + "))"
		normalized := "if(assumeNotNull(" + candidateAlias + ") = toFloat64(0), toFloat64(0), assumeNotNull(" + candidateAlias + "))"
		valueSQL = guardedNumericBucketSQL(input, candidateAlias, "Float64", finite, normalized)
	} else {
		return compiledRelation{}, compileState{}, nil, unsupportedNumericBinFieldType(operator)
	}

	fragment := "WITH " + spanSQL + " AS " + spanAlias + ", " +
		candidateSQL + " AS " + candidateAlias + " " +
		upsertFieldProjectionSQL(relation.sql, state, operator.Output.Name, valueSQL, alias)
	relation = relation.selectFrom(fragment, operator.Range)
	next := updateBucketCompileState(state, operator.Input.Name, operator.Output, field)
	return relation, next, []any{operator.Span}, nil
}

type dynamicNumericBinMetadata struct {
	with         []string
	args         []any
	existsAlias  string
	typeAlias    string
	parentAlias  string
	versionAlias string
}

// exactFloat64BucketBound is 2^53, the largest integral bucket magnitude that
// can cross the public boundary as Float64 without losing a bit. Fractional or
// exponent String input whose exact bucket lies outside this interval is
// published as semantic Decimal backed by Int256 instead.
const exactFloat64BucketBound = "9007199254740992"

const (
	// MaximumExactNumericBinTextBytes bounds the lexical exact-decimal path.
	// The signed Int256 result contract needs at most 77 significant decimal
	// digits; 4 KiB leaves ample room for fixed-width padding and exponent
	// spelling without letting one runtime field amplify the generated string
	// operations to the configurable event-size ceiling.
	MaximumExactNumericBinTextBytes = 4 << 10
	exactNumericBinMaxDigits        = 77
	exactNumericBinExponentClamp    = jsonnumber.MaximumExponentMagnitude
	exactNumericBinMaxInt256        = "57896044618658097711785492504343953926634992332820282019728792003956564819967"
	exactNumericBinMinMagnitude     = "57896044618658097711785492504343953926634992332820282019728792003956564819968"
)

// decimalNumericStringPattern is shared by every command that interprets a
// complete String as a decimal number. Optional pieces use empty alternatives
// instead of question marks because generated SQL reserves `?` for arguments.
const decimalNumericStringPattern = `'^([+]|-|)(([0-9]+([.][0-9]*|))|([.][0-9]+))([eE]([+]|-|)[0-9]+|)$'`

const (
	exactNumericBinSourceNone uint8 = iota
	exactNumericBinSourceString
	exactNumericBinSourceDecimal
)

type exactDynamicDecimalBucketSQL struct {
	layers          [][]string
	privateAliases  []string
	sourceModeAlias string
	candidateAlias  string
}

// compileExactDynamicDecimalBucketSQL lowers one already-validated numeric
// spelling to an exact, integral bucket boundary. It parses sign, coefficient,
// decimal point, and exponent lexically, constructs at most 77 integer digits,
// and performs the bucket arithmetic as an unsigned magnitude. That avoids
// ClickHouse's wrapping String-to-Int256 conversions and also represents the
// magnitude of MinInt256 while applying mathematical floor to negative
// fractions.
//
// sourceModeSQL must be zero for nonnumeric or otherwise ineligible input, and
// sourceTextSQL must select either canonical numeric String text or a validated
// decimal/v1 payload. Text above MaximumExactNumericBinTextBytes is replaced by
// zero before any decomposition. The caller keeps the semantic source
// classification separately so an ineligible declared Decimal reaches the
// sanitized unsupported-value arm while ordinary String data passes through.
func compileExactDynamicDecimalBucketSQL(
	stage int,
	spanAlias string,
	sourceModeSQL string,
	sourceTextSQL string,
) exactDynamicDecimalBucketSQL {
	alias := func(name string) string {
		return numericBinStageAlias("exact_"+name, stage)
	}
	sourceModeAlias := alias("source")
	rawAlias := alias("raw")
	candidateAlias := alias("candidate")

	sourceLimit := strconv.Itoa(MaximumExactNumericBinTextBytes)
	exponentClamp := strconv.Itoa(exactNumericBinExponentClamp)
	maxDigits := strconv.Itoa(exactNumericBinMaxDigits)
	spanMagnitude := "toUInt256(" + spanAlias + ")"

	// Lambda parameters are local scalar bindings. Keeping the lexical state
	// inside one expression avoids ten nested SELECT * projections. Those
	// projections caused ClickHouse's analyzer to substitute a prior bin's
	// Dynamic output through every later layer, making a one-row re-bin consume
	// more than a GiB and minutes of CPU.
	lambdaVariable := func(name string) string {
		return fmt.Sprintf("__os_exact_%s_%d", name, stage)
	}
	bind := func(parameters, values []string, body string) string {
		arrays := make([]string, len(values))
		for index, value := range values {
			arrays[index] = "[" + value + "]"
		}
		return "arrayElement(arrayMap((" + strings.Join(parameters, ", ") + ") -> " +
			body + ", " + strings.Join(arrays, ", ") + "), 1)"
	}

	eligible := lambdaVariable("eligible")
	text := lambdaVariable("text")
	negative := lambdaVariable("negative")
	body := lambdaVariable("body")
	exponentPosition := lambdaVariable("exponent_position")
	significand := lambdaVariable("significand")
	exponentText := lambdaVariable("exponent_text")
	exponentNegative := lambdaVariable("exponent_negative")
	exponentTrimmed := lambdaVariable("exponent_trimmed")
	fractionDigits := lambdaVariable("fraction_digits")
	significant := lambdaVariable("significant")
	exponent := lambdaVariable("exponent")
	integerDigits := lambdaVariable("integer_digits")
	integerTextVariable := lambdaVariable("integer_text")
	fractional := lambdaVariable("fractional")
	bucketMagnitudeVariable := lambdaVariable("bucket_magnitude")

	eligibleSQL := "toUInt8(" + sourceModeAlias + " != 0 AND length(" + rawAlias + ") <= " +
		sourceLimit + ")"
	textSQL := "if(" + sourceModeAlias + " != 0 AND length(" + rawAlias + ") <= " +
		sourceLimit + ", " + rawAlias + ", CAST('0' AS String))"
	signOffset := "if(startsWith(" + text + ", '-') OR startsWith(" + text + ", '+'), 2, 1)"
	bodySQL := "substring(" + text + ", " + signOffset + ")"
	exponentPositionSQL := "greatest(position(" + bodySQL + ", 'e'), position(" + bodySQL + ", 'E'))"
	significandSQL := "if(" + exponentPosition + " = 0, " + body + ", substring(" +
		body + ", 1, " + exponentPosition + " - 1))"
	exponentTextSQL := "if(" + exponentPosition + " = 0, CAST('0' AS String), substring(" +
		body + ", " + exponentPosition + " + 1))"
	exponentOffset := "if(startsWith(" + exponentText + ", '-') OR startsWith(" +
		exponentText + ", '+'), 2, 1)"
	exponentDigits := "substring(" + exponentText + ", " + exponentOffset + ")"
	exponentMagnitude := "if(length(" + exponentTrimmed + ") <= 4, toInt64OrZero(" +
		exponentTrimmed + "), toInt64(" + exponentClamp + "))"
	significantLength := "toInt64(length(" + significant + "))"
	safePrefixLength := "toUInt64(greatest(least(" + integerDigits + ", " +
		significantLength + "), 0))"
	safeZeroCount := "toUInt64(greatest(least(" + integerDigits + " - " +
		significantLength + ", " + maxDigits + "), 0))"
	safeFractionStart := "toUInt64(greatest(" + integerDigits + " + 1, 1))"
	integerText := "multiIf(" +
		"empty(" + significant + "), '0', " +
		integerDigits + " <= 0, '0', " +
		integerDigits + " <= " + maxDigits + " AND " +
		integerDigits + " <= " + significantLength + ", substring(" +
		significant + ", 1, " + safePrefixLength + "), " +
		integerDigits + " <= " + maxDigits + ", concat(" + significant +
		", repeat('0', " + safeZeroCount + ")), '0')"
	fractionalSQL := "toUInt8(NOT empty(" + significant + ") AND (" +
		integerDigits + " <= 0 OR (" + integerDigits + " < " +
		significantLength + " AND match(substring(" + significant + ", " +
		safeFractionStart + "), '[1-9]'))))"
	magnitude := "toUInt256(" + integerTextVariable + ")"
	remainder := "modulo(" + magnitude + ", " + spanMagnitude + ")"
	quotient := "intDiv(" + magnitude + ", " + spanMagnitude + ")"
	negativeCorrection := "if(" + negative + " != 0 AND (" + remainder +
		" != toUInt256(0) OR " + fractional + " != 0), toUInt256(1), toUInt256(0))"
	bucketMagnitude := "(" + quotient + " + " + negativeCorrection + ") * " + spanMagnitude
	fits := eligible + " != 0 AND (empty(" + significant + ") OR " +
		integerDigits + " <= " + maxDigits + ") AND if(" + negative + " != 0, " +
		bucketMagnitudeVariable + " <= toUInt256('" + exactNumericBinMinMagnitude + "'), " +
		bucketMagnitudeVariable + " <= toUInt256('" + exactNumericBinMaxInt256 + "'))"
	// Two's-complement conversion is the only safe way to produce MinInt256
	// from its UInt256 magnitude. String and ordinary numeric casts wrap before
	// reporting range failure on the pinned ClickHouse server.
	negativeCandidate := "reinterpretAsInt256(bitNot(" + bucketMagnitudeVariable + ") + toUInt256(1))"
	candidate := "if(" + fits + ", if(" + negative + " != 0, " +
		negativeCandidate + ", accurateCastOrNull(" + bucketMagnitudeVariable +
		", 'Int256')), CAST(NULL AS Nullable(Int256)))"

	candidate = bind(
		[]string{bucketMagnitudeVariable},
		[]string{bucketMagnitude},
		candidate,
	)
	candidate = bind(
		[]string{integerTextVariable, fractional},
		[]string{integerText, fractionalSQL},
		candidate,
	)
	candidate = bind(
		[]string{integerDigits},
		[]string{significantLength + " + " + exponent + " - " + fractionDigits},
		candidate,
	)
	candidate = bind(
		[]string{exponent},
		[]string{"if(" + exponentNegative + " != 0, -(" + exponentMagnitude + "), " +
			exponentMagnitude + ")"},
		candidate,
	)
	candidate = bind(
		[]string{exponentNegative, exponentTrimmed, fractionDigits, significant},
		[]string{
			"toUInt8(startsWith(" + exponentText + ", '-'))",
			"replaceRegexpOne(" + exponentDigits + ", '^0+', '')",
			"toInt64(if(position(" + significand + ", '.') = 0, 0, length(" +
				significand + ") - position(" + significand + ", '.')))",
			"replaceRegexpOne(replaceAll(" + significand + ", '.', ''), '^0+', '')",
		},
		candidate,
	)
	candidate = bind(
		[]string{significand, exponentText},
		[]string{significandSQL, exponentTextSQL},
		candidate,
	)
	candidate = bind(
		[]string{negative, body, exponentPosition},
		[]string{
			"toUInt8(startsWith(" + text + ", '-'))",
			bodySQL,
			exponentPositionSQL,
		},
		candidate,
	)
	candidate = bind(
		[]string{eligible, text},
		[]string{eligibleSQL, textSQL},
		candidate,
	)

	layers := [][]string{
		{
			sourceModeSQL + " AS " + sourceModeAlias,
			sourceTextSQL + " AS " + rawAlias,
		},
		{
			candidate + " AS " + candidateAlias,
		},
	}
	return exactDynamicDecimalBucketSQL{
		layers: layers,
		privateAliases: []string{
			sourceModeAlias,
			rawAlias,
			candidateAlias,
		},
		sourceModeAlias: sourceModeAlias,
		candidateAlias:  candidateAlias,
	}
}

func dynamicNumericBinProjectionLayer(fragment string, stage, layer int, expressions []string) string {
	alias := quoteIdentifier(fmt.Sprintf("__os_numeric_bin_layer_%d_%d", stage, layer))
	return "SELECT *, " + strings.Join(expressions, ", ") + " FROM (" + fragment + ") AS " + alias
}

func compileDynamicNumericBucket(
	relation compiledRelation,
	state compileState,
	operator *plan.NumericBucket,
	field fieldState,
	alias string,
	stage int,
) (compiledRelation, compileState, []any, error) {
	if err := validateKnowledgeFieldSidecars(
		field.relativeFieldNamesSQL,
		field.relativeFieldTypesSQL,
		field.fieldMetadataVersionSQL,
	); err != nil {
		return compiledRelation{}, compileState{}, nil, fmt.Errorf(
			"compile ClickHouse numeric bucket input %q: %w",
			operator.Input.Name,
			err,
		)
	}
	spanAlias := numericBinStageAlias("span", stage)
	physicalTypeAlias := numericBinStageAlias("physical_type", stage)
	signedValueAlias := numericBinStageAlias("signed_value", stage)
	signedCandidateAlias := numericBinStageAlias("signed_candidate", stage)
	unsignedValueAlias := numericBinStageAlias("unsigned_value", stage)
	unsignedCandidateAlias := numericBinStageAlias("unsigned_candidate", stage)
	floatValueAlias := numericBinStageAlias("float_value", stage)
	floatCandidateAlias := numericBinStageAlias("float_candidate", stage)
	stringValueAlias := numericBinStageAlias("string_value", stage)
	stringBoundedAlias := numericBinStageAlias("string_bounded", stage)
	stringNumericAlias := numericBinStageAlias("string_numeric", stage)
	stringCanonicalAlias := numericBinStageAlias("string_canonical", stage)
	stringSignedAlias := numericBinStageAlias("string_signed", stage)
	stringUnsignedAlias := numericBinStageAlias("string_unsigned", stage)
	stringCandidateAlias := numericBinStageAlias("string_candidate", stage)
	stringModeAlias := numericBinStageAlias("string_mode", stage)
	extendedAlias := numericBinStageAlias("extended", stage)
	supportedAlias := numericBinStageAlias("supported", stage)
	outputExistsAlias := numericBinStageAlias("output_exists", stage)
	outputTypeAlias := numericBinStageAlias("output_type", stage)
	outputNamesAlias := ""
	outputTypesAlias := ""
	outputMetadataAlias := ""
	stateSourceField := field

	// ClickHouse's analyzer substitutes ordinary projection aliases through
	// later subqueries. A calculated Dynamic value (especially a previous bin
	// output) is inspected through several dynamicElement alternatives below;
	// without a scoped column, each inspection recursively duplicates the
	// complete producing expression. A singleton arrayJoin is a bounded,
	// one-to-one streaming action that gives value, presence, and semantic type
	// one shared row-local binding and prevents that multiplicative expansion.
	inputBindingAlias := ""
	if field.storedTypeSQL != "" && field.existsSQL != "" && len(field.existsArgs) == 0 &&
		!strings.Contains(field.existsSQL, "?") && !strings.Contains(field.storedTypeSQL, "?") {
		inputBindingAlias = numericBinStageAlias("bound", stage)
		bindingBaseAlias := quoteIdentifier(fmt.Sprintf("__os_numeric_bin_bound_base_%d", stage))
		binding := "arrayJoin([tuple(CAST(" + field.valueSQL + " AS Dynamic), " +
			"toUInt8(ifNull(" + field.existsSQL + ", 0)), toUInt8(" + field.storedTypeSQL + "))])"
		boundSQL := "SELECT *, " + binding + " AS " + inputBindingAlias +
			" FROM (" + relation.sql + ") AS " + bindingBaseAlias
		relation = relation.selectFrom(boundSQL, operator.Range)
		field.valueSQL = "tupleElement(" + inputBindingAlias + ", 1)"
		field.dynamicTypeSQL = "dynamicType(" + field.valueSQL + ")"
		field.existsSQL = "tupleElement(" + inputBindingAlias + ", 2)"
		field.existsArgs = nil
		field.storedTypeSQL = "tupleElement(" + inputBindingAlias + ", 3)"
	}

	metadata, err := compileDynamicNumericBinMetadata(field, stage, state.eventRows)
	if err != nil {
		return compiledRelation{}, compileState{}, nil, fmt.Errorf(
			"compile ClickHouse numeric bucket metadata for %q: %w",
			operator.Input.Name,
			err,
		)
	}

	// bin never writes its destination for an event without the source field,
	// so an existing destination keeps its prior value, semantic type, and
	// sparse presence exactly as rex does on no match.
	previous, previousKnown, err := resolveCompiledField(operator.Output, state)
	if err != nil {
		return compiledRelation{}, compileState{}, nil, err
	}
	preserve := previousKnown && operator.Output.Name != operator.Input.Name
	if err := validateKnowledgeFieldSidecars(
		previous.relativeFieldNamesSQL,
		previous.relativeFieldTypesSQL,
		previous.fieldMetadataVersionSQL,
	); err != nil {
		return compiledRelation{}, compileState{}, nil, fmt.Errorf(
			"compile ClickHouse numeric bucket destination %q: %w",
			operator.Output.Name,
			err,
		)
	}
	if preserve && previous.relativeFieldNamesSQL != "" {
		outputNamesAlias = numericBinStageAlias("output_names", stage)
		outputTypesAlias = numericBinStageAlias("output_types", stage)
		outputMetadataAlias = numericBinStageAlias("output_metadata_version", stage)
	}
	var (
		preserveWith        []string
		preserveBaseAliases []string
		preserveArgs        []any
		previousExistsAlias string
		previousTypeAlias   string
	)
	if preserve {
		previousExistsSQL, previousExistsArgs := knownFieldPresenceSQL(previous)
		previousTypeSQL, previousTypeArgs, typeErr := knownFieldStoredTypeSQL(previous)
		if typeErr != nil {
			return compiledRelation{}, compileState{}, nil, fmt.Errorf(
				"compile ClickHouse numeric bucket destination type for %q: %w",
				operator.Output.Name,
				typeErr,
			)
		}
		previousExistsAlias = numericBinStageAlias("previous_exists", stage)
		previousTypeAlias = numericBinStageAlias("previous_type", stage)
		preserveWith = append(preserveWith,
			"toUInt8(ifNull("+previousExistsSQL+", 0)) AS "+previousExistsAlias,
			"toUInt8("+previousTypeSQL+") AS "+previousTypeAlias,
		)
		preserveBaseAliases = append(preserveBaseAliases, previousExistsAlias, previousTypeAlias)
		preserveArgs = append(preserveArgs, previousExistsArgs...)
		preserveArgs = append(preserveArgs, previousTypeArgs...)
	}

	signedWide := "toInt128(" + signedValueAlias + ")"
	signedSpan := "toInt128(" + spanAlias + ")"
	signedBucket := "(intDiv(" + signedWide + ", " + signedSpan + ") - if(" +
		signedWide + " < 0 AND " + signedWide + " % " + signedSpan + " != 0, 1, 0)) * " + signedSpan
	unsignedBucket := "intDiv(" + unsignedValueAlias + ", " + spanAlias + ") * " + spanAlias
	floatSpan := "toFloat64(" + spanAlias + ")"
	floatBucket := "floor(" + floatValueAlias + " / " + floatSpan + ") * " + floatSpan

	// Splunk fields are textual at the search layer and number-consuming
	// commands convert numeric text internally. Keep the accepted grammar
	// deliberately decimal and whole-string: surrounding whitespace and unit
	// suffixes do not become partially parsed numbers. Text that spells an
	// integer buckets through exact Int256 arithmetic, so the same digits bin
	// identically whether ingestion typed them as a number or as text.
	// Express optional regex components as empty alternatives rather than `?`.
	// Generated SQL uses `?` exclusively for bound placeholders, which keeps
	// compiler/executor placeholder accounting exact.
	integerStringPattern := `'^([+]|-|)[0-9]+$'`
	numericString := "(" + stringBoundedAlias + " = trimBoth(" + stringBoundedAlias + ") AND " +
		"match(" + stringBoundedAlias + ", " + decimalNumericStringPattern + "))"
	exactTextLimit := strconv.Itoa(MaximumExactNumericBinTextBytes)

	with := append([]string(nil), preserveWith...)
	with = append(with, "CAST(? AS UInt64) AS "+spanAlias)
	with = append(with, metadata.with...)
	with = append(with,
		"dynamicType("+field.valueSQL+") AS "+physicalTypeAlias,
		"dynamicElement("+field.valueSQL+", 'Int64') AS "+signedValueAlias,
		"accurateCastOrNull("+signedBucket+", 'Int64') AS "+signedCandidateAlias,
		"dynamicElement("+field.valueSQL+", 'UInt64') AS "+unsignedValueAlias,
		unsignedBucket+" AS "+unsignedCandidateAlias,
		"dynamicElement("+field.valueSQL+", 'Float64') AS "+floatValueAlias,
		floatBucket+" AS "+floatCandidateAlias,
		"dynamicElement("+field.valueSQL+", 'String') AS "+stringValueAlias,
		"if(length("+stringValueAlias+") <= "+exactTextLimit+", "+stringValueAlias+
			", CAST('' AS String)) AS "+stringBoundedAlias,
		"toUInt8(ifNull(isValidUTF8("+stringBoundedAlias+") AND "+numericString+", 0)) AS "+stringNumericAlias,
		"if("+stringNumericAlias+" != 0, "+canonicalNumericTextSQL(stringBoundedAlias)+
			", CAST('' AS String)) AS "+stringCanonicalAlias,
	)

	code := func(value eventfields.StoredValueType) string {
		return "toUInt8(" + strconv.Itoa(int(value)) + ")"
	}
	present := metadata.existsAlias + " != 0"
	noParent := metadata.parentAlias + " = 0"
	storedType := metadata.typeAlias
	physicalType := physicalTypeAlias
	input := field.valueSQL
	dynamicInput := "CAST(" + input + " AS Dynamic)"

	missingCondition := metadata.existsAlias + " = 0 AND " + noParent + " AND " + physicalType + " = 'None'"
	// Stored leaf metadata is only readable at the current version. A row
	// written before that metadata existed carries an intentionally empty type
	// array that must never be interpreted heuristically, so its value passes
	// through unbucketed instead of failing the search as an out-of-range one.
	staleMetadataCondition := metadata.versionAlias + " = 0"
	nullCondition := present + " AND " + noParent + " AND " +
		storedType + " = " + code(eventfields.StoredValueTypeNull) + " AND " + physicalType + " = 'None'"
	signedCondition := present + " AND " + noParent + " AND " +
		storedType + " = " + code(eventfields.StoredValueTypeSint64) + " AND " +
		physicalType + " = 'Int64' AND isNotNull(" + signedCandidateAlias + ")"
	unsignedCondition := present + " AND " + noParent + " AND " +
		storedType + " = " + code(eventfields.StoredValueTypeUint64) + " AND " +
		physicalType + " = 'UInt64'"
	floatCondition := present + " AND " + noParent + " AND " +
		storedType + " = " + code(eventfields.StoredValueTypeDouble) + " AND " +
		physicalType + " = 'Float64' AND isFinite(" + floatValueAlias + ") AND isFinite(" + floatCandidateAlias + ")"
	// The String arm is total: text that cannot be bucketed exactly, including
	// NaN/Inf spellings, overflowing exponents, and invalid UTF-8, keeps its
	// value instead of failing the search on ordinary event text.
	stringBaseCondition := present + " AND " + noParent + " AND " +
		storedType + " = " + code(eventfields.StoredValueTypeString) + " AND " +
		physicalType + " = 'String'"
	boolCondition := present + " AND " + noParent + " AND " +
		storedType + " = " + code(eventfields.StoredValueTypeBool) + " AND " + physicalType + " = 'Bool'"

	tagged := newDynamicEnvelopeSQL(input, physicalType)
	taggedCondition := func(stored eventfields.StoredValueType, tag, payloadValid string) string {
		return "(" + present + " AND " + noParent + " AND " +
			storedType + " = " + code(stored) + " AND " + tagged.envelope + " AND " +
			tagged.mapSQL + "[" + tagged.typeKey + "] = '" + tag + "' AND " + payloadValid + ")"
	}
	boundedDecimalValidSQL := func(payload string) string {
		bounded := "if(length(" + payload + ") <= " + exactTextLimit +
			", " + payload + ", CAST('' AS String))"
		return "length(" + payload + ") <= " + exactTextLimit +
			" AND match(" + bounded + ", " + dynamicDecimalPayloadPattern + ")"
	}
	decimalEnvelopeCondition := taggedCondition(
		eventfields.StoredValueTypeDecimal,
		"decimal/v1",
		boundedDecimalValidSQL(tagged.payload),
	)
	// Exact decimal buckets are published as Dynamic(Int256) with calculated
	// Decimal metadata. Treat that representation as a first-class Decimal
	// input so consecutive bins remain composable instead of accepting only
	// the map envelope written by ingestion.
	decimalInt256Condition := "0"
	if field.storedTypeSQL != "" {
		decimalInt256Condition = "(" + present + " AND " + noParent + " AND " +
			storedType + " = " + code(eventfields.StoredValueTypeDecimal) + " AND " +
			physicalType + " = 'Int256')"
	}
	decimalBaseCondition := "(" + decimalEnvelopeCondition + " OR " + decimalInt256Condition + ")"
	stringNumericCondition := "(" + stringBaseCondition + " AND " + stringNumericAlias + " != 0)"
	exactSourceMode := "toUInt8(multiIf(" +
		decimalBaseCondition + ", " + strconv.Itoa(int(exactNumericBinSourceDecimal)) + ", " +
		stringNumericCondition + ", " + strconv.Itoa(int(exactNumericBinSourceString)) + ", " +
		strconv.Itoa(int(exactNumericBinSourceNone)) + "))"
	exactSourceText := "multiIf(" +
		decimalEnvelopeCondition + ", " + tagged.payload + ", " +
		decimalInt256Condition + ", toString(dynamicElement(" + input + ", 'Int256')), " +
		stringNumericCondition + ", " + stringCanonicalAlias + ", CAST('0' AS String))"
	exact := compileExactDynamicDecimalBucketSQL(
		stage,
		spanAlias,
		exactSourceMode,
		exactSourceText,
	)
	baseAliases := []string{
		spanAlias,
		metadata.existsAlias,
		metadata.typeAlias,
		metadata.parentAlias,
		metadata.versionAlias,
		physicalTypeAlias,
		signedCandidateAlias,
		unsignedCandidateAlias,
		floatValueAlias,
		floatCandidateAlias,
		stringValueAlias,
		stringBoundedAlias,
		stringNumericAlias,
		stringCanonicalAlias,
	}
	baseAliases = append(baseAliases, preserveBaseAliases...)
	baseAlias := quoteIdentifier(fmt.Sprintf("__os_numeric_bin_base_%d", stage))
	baseSQL := "WITH " + strings.Join(with, ", ") + " SELECT *, " +
		strings.Join(baseAliases, ", ") + " FROM (" + relation.sql + ") AS " + baseAlias
	relation = relation.selectFrom(baseSQL, operator.Range)
	privateAliases := append([]string(nil), baseAliases...)
	if inputBindingAlias != "" {
		privateAliases = append(privateAliases, inputBindingAlias)
	}
	layer := 0
	for _, expressions := range exact.layers {
		layerSQL := dynamicNumericBinProjectionLayer(relation.sql, stage, layer, expressions)
		relation = relation.selectFrom(layerSQL, operator.Range)
		layer++
	}
	exactDeadAliases := make([]string, 0, len(exact.privateAliases)-2)
	for _, privateAlias := range exact.privateAliases {
		if privateAlias != exact.sourceModeAlias && privateAlias != exact.candidateAlias {
			exactDeadAliases = append(exactDeadAliases, privateAlias)
		}
	}
	exactCleanupAlias := quoteIdentifier(fmt.Sprintf("__os_numeric_bin_exact_cleanup_%d", stage))
	exactCleanupSQL := "SELECT * EXCEPT (" + strings.Join(exactDeadAliases, ", ") + ") FROM (" +
		relation.sql + ") AS " + exactCleanupAlias
	relation = relation.selectFrom(exactCleanupSQL, operator.Range)
	privateAliases = append(privateAliases, exact.sourceModeAlias, exact.candidateAlias)

	exactStringCondition := exact.sourceModeAlias + " = " +
		strconv.Itoa(int(exactNumericBinSourceString)) + " AND isNotNull(" + exact.candidateAlias + ")"
	integralStringCondition := exactStringCondition + " AND match(" +
		stringBoundedAlias + ", " + integerStringPattern + ")"
	fractionalStringCondition := exactStringCondition + " AND NOT match(" +
		stringBoundedAlias + ", " + integerStringPattern + ")"
	exactFloatLower := "toInt256('-" + exactFloat64BucketBound + "')"
	exactFloatUpper := "toInt256('" + exactFloat64BucketBound + "')"
	exactFloatStringCondition := fractionalStringCondition + " AND " +
		exact.candidateAlias + " BETWEEN " + exactFloatLower + " AND " + exactFloatUpper
	stringMode := "toUInt8(multiIf(" +
		integralStringCondition + " AND isNotNull(" + stringSignedAlias + "), 1, " +
		integralStringCondition + " AND isNotNull(" + stringUnsignedAlias + "), 2, " +
		exactFloatStringCondition + ", 3, " +
		exactStringCondition + ", 4, 0))"
	stringCastSQL := dynamicNumericBinProjectionLayer(relation.sql, stage, layer, []string{
		"accurateCastOrNull(" + exact.candidateAlias + ", 'Int64') AS " + stringSignedAlias,
		"accurateCastOrNull(" + exact.candidateAlias + ", 'UInt64') AS " + stringUnsignedAlias,
		"toFloat64(" + exact.candidateAlias + ") AS " + stringCandidateAlias,
	})
	relation = relation.selectFrom(stringCastSQL, operator.Range)
	layer++
	privateAliases = append(privateAliases, stringSignedAlias, stringUnsignedAlias, stringCandidateAlias)
	stringModeSQL := dynamicNumericBinProjectionLayer(relation.sql, stage, layer, []string{
		stringMode + " AS " + stringModeAlias,
	})
	relation = relation.selectFrom(stringModeSQL, operator.Range)
	layer++
	privateAliases = append(privateAliases, stringModeAlias)
	decimalCondition := exact.sourceModeAlias + " = " +
		strconv.Itoa(int(exactNumericBinSourceDecimal)) + " AND isNotNull(" + exact.candidateAlias + ")"
	stringSignedCondition := stringBaseCondition + " AND " + stringModeAlias + " = 1"
	stringUnsignedCondition := stringBaseCondition + " AND " + stringModeAlias + " = 2"
	stringFloatCondition := stringBaseCondition + " AND " + stringModeAlias + " = 3"
	stringWideCondition := stringBaseCondition + " AND " + stringModeAlias + " = 4"
	stringPassThroughCondition := stringBaseCondition

	// The three admitted envelopes are classified once into a private alias so
	// the pass-through arm and the unsupported guard below share one
	// evaluation instead of restating the envelope grammar twice.
	extendedSQL := dynamicNumericBinProjectionLayer(relation.sql, stage, layer, []string{
		"toUInt8(ifNull(" + strings.Join([]string{
			taggedCondition(eventfields.StoredValueTypeBytes, "bytes/v1", tagged.bytesValid),
			taggedCondition(eventfields.StoredValueTypeTimestamp, "timestamp/v1", tagged.timestampValid),
			taggedCondition(eventfields.StoredValueTypeDuration, "duration/v1", tagged.durationValid),
		}, " OR ") + ", 0)) AS " + extendedAlias,
	})
	relation = relation.selectFrom(extendedSQL, operator.Range)
	layer++
	privateAliases = append(privateAliases, extendedAlias)
	extendedCondition := extendedAlias + " != 0"

	// Whether the row reaches any classifier arm at all. Only the sanitized
	// unsupported guard reads it, and it must stay aligned with the arms below:
	// the three narrower String arms are covered by their shared base
	// condition. An arm added here but not below would let an unclassified
	// value escape as the guard's own result instead of failing the search.
	supportedSQL := "toUInt8(ifNull(" + strings.Join([]string{
		"(" + missingCondition + ")",
		"(" + staleMetadataCondition + ")",
		"(" + nullCondition + ")",
		"(" + signedCondition + ")",
		"(" + unsignedCondition + ")",
		"(" + floatCondition + ")",
		"(" + stringPassThroughCondition + ")",
		"(" + decimalCondition + ")",
		"(" + boolCondition + ")",
		"(" + extendedCondition + ")",
	}, " OR ") + ", 0))"

	// A bucketed String becomes the number it spells, so the destination stays
	// visible to numeric predicates and still converges with its integer twin
	// under the lexical stats BY key. The output's semantic type follows the
	// value it now holds rather than the source's stored type.
	bucketTypeSQL := "multiIf(" +
		decimalCondition + ", " + code(eventfields.StoredValueTypeDecimal) + ", " +
		storedType + " != " + code(eventfields.StoredValueTypeString) + ", " + storedType + ", " +
		stringModeAlias + " = 1, " + code(eventfields.StoredValueTypeSint64) + ", " +
		stringModeAlias + " = 2, " + code(eventfields.StoredValueTypeUint64) + ", " +
		stringModeAlias + " = 3, " + code(eventfields.StoredValueTypeDouble) + ", " +
		stringModeAlias + " = 4, " + code(eventfields.StoredValueTypeDecimal) + ", " + storedType + ")"
	missingValue := "CAST(NULL AS Dynamic)"
	outputExistsSQL := metadata.existsAlias
	outputTypeSQL := bucketTypeSQL
	if preserve {
		missingValue = "CAST(" + previous.valueSQL + " AS Dynamic)"
		if previous.kind == fieldKindString && previous.stringOrBytes {
			if previous.semanticBytesSQL == "" {
				return compiledRelation{}, compileState{}, nil, errors.New(
					"compile ClickHouse numeric bucket: prior String-or-Bytes field lacks semantic Bytes provenance",
				)
			}
			missingValue = "if(ifNull(" + previous.semanticBytesSQL +
				", 0), " + bytesEnvelopePayloadDynamicSQL(
				rawStdBase64EncodeSQL("assumeNotNull("+previous.valueSQL+")"),
			) + ", CAST(" + previous.valueSQL + " AS Dynamic))"
		}
		outputExistsSQL = "if(" + metadata.existsAlias + " != 0, 1, " + previousExistsAlias + ")"
		outputTypeSQL = "if(" + metadata.existsAlias + " != 0, " + bucketTypeSQL + ", " + previousTypeAlias + ")"
	}
	supportedExpressions := []string{
		supportedSQL + " AS " + supportedAlias,
		"toUInt8(" + outputExistsSQL + ") AS " + outputExistsAlias,
		"toUInt8(" + outputTypeSQL + ") AS " + outputTypeAlias,
	}
	if outputNamesAlias != "" {
		supportedExpressions = append(
			supportedExpressions,
			"if("+missingCondition+", "+previous.relativeFieldNamesSQL+", "+
				knowledgeEmptyRelativeFieldNamesSQL()+") AS "+outputNamesAlias,
			"if("+missingCondition+", "+previous.relativeFieldTypesSQL+", "+
				knowledgeEmptyRelativeFieldTypesSQL()+") AS "+outputTypesAlias,
			"toUInt8(if("+missingCondition+", "+
				previous.fieldMetadataVersionSQL+", 0)) AS "+outputMetadataAlias,
		)
	}
	supportedProjectionSQL := dynamicNumericBinProjectionLayer(
		relation.sql,
		stage,
		layer,
		supportedExpressions,
	)
	relation = relation.selectFrom(supportedProjectionSQL, operator.Range)
	privateAliases = append(privateAliases, supportedAlias)

	normalizedFloat := "if(" + floatCandidateAlias + " = toFloat64(0), toFloat64(0), " + floatCandidateAlias + ")"
	normalizedString := "if(" + stringCandidateAlias + " = toFloat64(0), toFloat64(0), " + stringCandidateAlias + ")"
	// The classifier's final branch is the only place an unsupported value is
	// reported, and it is guarded by a row-wise condition rather than a
	// constant. A downstream expression that inspects the destination's runtime
	// type — a `sort` key or a `search` relational predicate — forces the whole
	// Dynamic column to materialize, and ClickHouse then evaluates every branch
	// for every row. A constant `throwIf(1, ...)` fallback would fail the entire
	// search on rows that are perfectly supported, while a guard that repeats
	// the classifier's own coverage stays exactly as row-specific as the branch
	// it belongs to.
	unsupported := "CAST(throwIf(toUInt8(" + supportedAlias + " = 0), '" +
		UnsupportedNumericBinValueMarker + "') AS Dynamic)"
	valueSQL := "multiIf(" +
		missingCondition + ", " + missingValue + ", " +
		staleMetadataCondition + ", " + dynamicInput + ", " +
		nullCondition + ", " + dynamicInput + ", " +
		signedCondition + ", CAST(assumeNotNull(" + signedCandidateAlias + ") AS Dynamic), " +
		unsignedCondition + ", CAST(" + unsignedCandidateAlias + " AS Dynamic), " +
		floatCondition + ", CAST(" + normalizedFloat + " AS Dynamic), " +
		decimalCondition + ", CAST(assumeNotNull(" + exact.candidateAlias + ") AS Dynamic), " +
		stringSignedCondition + ", CAST(assumeNotNull(" + stringSignedAlias + ") AS Dynamic), " +
		stringUnsignedCondition + ", CAST(assumeNotNull(" + stringUnsignedAlias + ") AS Dynamic), " +
		stringFloatCondition + ", CAST(" + normalizedString + " AS Dynamic), " +
		stringWideCondition + ", CAST(assumeNotNull(" + exact.candidateAlias + ") AS Dynamic), " +
		stringPassThroughCondition + ", " + dynamicInput + ", " +
		boolCondition + ", " + dynamicInput + ", " +
		"(" + extendedCondition + "), " + dynamicInput + ", " +
		unsupported + ")"

	output := quoteIdentifier(operator.Output.Name)
	projection := "*, " + valueSQL + " AS " + output
	if _, replacing := state.visible[operator.Output.Name]; replacing {
		projection = "* REPLACE (" + valueSQL + " AS " + output + ")"
	}
	outputSQL := "SELECT " + projection + " FROM (" + relation.sql + ") AS " + alias
	relation = relation.selectFrom(outputSQL, operator.Range)
	cleanupAlias := quoteIdentifier(fmt.Sprintf("__os_numeric_bin_cleanup_%d", stage))
	cleanupSQL := "SELECT * EXCEPT (" + strings.Join(privateAliases, ", ") + ") FROM (" +
		relation.sql + ") AS " + cleanupAlias
	relation = relation.selectFrom(cleanupSQL, operator.Range)

	next := updateDynamicBucketCompileState(
		state,
		operator.Input.Name,
		operator.Output,
		stateSourceField,
		outputExistsAlias,
		outputTypeAlias,
		outputNamesAlias,
		outputTypesAlias,
		outputMetadataAlias,
	)
	args := make([]any, 0, 1+len(metadata.args)+len(preserveArgs))
	args = append(args, preserveArgs...)
	args = append(args, operator.Span)
	args = append(args, metadata.args...)
	return relation, next, args, nil
}

func compileDynamicNumericBinMetadata(
	field fieldState,
	stage int,
	metadataVersionAvailable bool,
) (dynamicNumericBinMetadata, error) {
	existsAlias := numericBinStageAlias("exists", stage)
	typeAlias := numericBinStageAlias("type", stage)
	parentAlias := numericBinStageAlias("parent", stage)
	versionAlias := numericBinStageAlias("metadata_version", stage)
	// Stored event metadata is meaningful only at the current aligned version.
	// A transforming aggregate drops the immutable event document and publishes
	// its own calculated type alias; that alias is authoritative and no longer
	// has an event-level version column in scope.
	versionWith := "toUInt8(1) AS " + versionAlias
	var versionArgs []any
	if metadataVersionAvailable {
		versionWith = "toUInt8(" + quoteIdentifier(internalFieldMetadataVersionColumn) + " = ?) AS " + versionAlias
		versionArgs = append(versionArgs, eventfields.CurrentFieldMetadataVersion)
	}
	if !metadataVersionAvailable && field.storedTypeSQL == "" {
		return dynamicNumericBinMetadata{}, errors.New(
			"transformed Dynamic field has no calculated semantic type",
		)
	}

	// A direct or projected stored path can classify an exact leaf, a
	// flattened object parent, and a missing path with one bounded metadata
	// position lookup. Ingestion sorts the aligned names/types arrays, so an
	// exact name precedes all of its descendants.
	directExistsSQL := "has(" + quoteIdentifier(internalFieldNamesColumn) + ", ?)"
	if field.storedTypeSQL == "" && field.existsSQL == directExistsSQL && len(field.existsArgs) == 1 {
		path, ok := field.existsArgs[0].(string)
		if !ok || path == "" {
			return dynamicNumericBinMetadata{}, errors.New("direct Dynamic path metadata is invalid")
		}
		pathAlias := numericBinStageAlias("path", stage)
		positionAlias := numericBinStageAlias("position", stage)
		matchedAlias := numericBinStageAlias("matched", stage)
		with := []string{
			versionWith,
			"CAST(? AS String) AS " + pathAlias,
			"arrayFirstIndex(name -> name = " + pathAlias + " OR startsWith(name, concat(" +
				pathAlias + ", '.')), " + quoteIdentifier(internalFieldNamesColumn) + ") AS " + positionAlias,
			"arrayElement(" + quoteIdentifier(internalFieldNamesColumn) + ", " + positionAlias + ") AS " + matchedAlias,
			"toUInt8(" + positionAlias + " != 0 AND " + matchedAlias + " = " + pathAlias + ") AS " + existsAlias,
			"toUInt8(" + positionAlias + " != 0 AND " + matchedAlias + " != " + pathAlias + ") AS " + parentAlias,
			"toUInt8(multiIf(" + existsAlias + " != 0, arrayElement(" +
				quoteIdentifier(internalFieldTypesColumn) + ", " + positionAlias + "), " +
				parentAlias + " != 0, toUInt8(" +
				strconv.Itoa(int(eventfields.StoredValueTypeObject)) + "), toUInt8(" +
				strconv.Itoa(int(eventfields.StoredValueTypeNull)) + "))) AS " + typeAlias,
		}
		return dynamicNumericBinMetadata{
			with:         with,
			args:         append(versionArgs, path),
			existsAlias:  existsAlias,
			typeAlias:    typeAlias,
			parentAlias:  parentAlias,
			versionAlias: versionAlias,
		}, nil
	}

	existsSQL := field.existsSQL
	if existsSQL == "" {
		existsSQL = "1"
	}
	storedTypeSQL, storedTypeArgs, err := knownFieldStoredTypeSQL(field)
	if err != nil {
		return dynamicNumericBinMetadata{}, err
	}
	// The stored payload's descendant probe describes the immutable document,
	// not the current value, so a later stage that overwrote this field with a
	// scalar must not be classified as a flattened object parent. The resolved
	// stored type already reports a flattened parent as an object.
	with := []string{
		versionWith,
		"toUInt8(ifNull(" + existsSQL + ", 0)) AS " + existsAlias,
		"toUInt8(" + storedTypeSQL + ") AS " + typeAlias,
		"toUInt8(" + typeAlias + " = " +
			"toUInt8(" + strconv.Itoa(int(eventfields.StoredValueTypeObject)) + ")) AS " + parentAlias,
	}
	args := make([]any, 0, len(versionArgs)+len(field.existsArgs)+len(storedTypeArgs))
	args = append(args, versionArgs...)
	args = append(args, field.existsArgs...)
	args = append(args, storedTypeArgs...)
	return dynamicNumericBinMetadata{
		with:         with,
		args:         args,
		existsAlias:  existsAlias,
		typeAlias:    typeAlias,
		parentAlias:  parentAlias,
		versionAlias: versionAlias,
	}, nil
}

func numericBinStageAlias(name string, stage int) string {
	return quoteIdentifier(fmt.Sprintf("__os_numeric_bin_%s_%d", name, stage))
}

// canonicalNumericTextSQL rewrites decimal numeric text into the shortest
// spelling of the same value by dropping the leading zeros of the significand
// and of the exponent. Zero-padded fixed-width numeric fields are ordinary
// event data, and ClickHouse's text-to-double parser keeps a bounded
// significant-digit window that padding consumes: on the pinned server
// `toFloat64OrNull('000000000000000000021.5')` is `0.5` and
// `toFloat64OrNull('1e0000000000000000000000002')` is `1`. Canonicalizing
// before parsing keeps a padded spelling equal to its unpadded twin, and makes
// the exact integer arm's byte-width bound a bound on significant digits
// rather than on padding. The input has already matched the whole-string
// decimal grammar, so the sign, the significand, and at most one exponent are
// the only components present. Express optional regex components as empty
// alternatives rather than `?`: generated SQL uses `?` exclusively for bound
// placeholders.
//
// Keep the alias holding this expression sparsely referenced. `WITH` aliases
// are inlined at every use, and the field summary embeds a whole bin fragment
// in a repeatedly referenced CTE, so one more chained alias read four times is
// enough to exhaust the server's memory on the pinned image. Both the exact
// lexical parser and the bounded Float64 publication arm read this canonical
// spelling; the original 4 KiB resource bound is applied before
// canonicalization so padding cannot bypass it.
func canonicalNumericTextSQL(value string) string {
	significand := "replaceRegexpOne(" + value + `, '^([+]|-|)0*([0-9])', '\\1\\2')`
	return "replaceRegexpOne(" + significand + `, '([eE])([+]|-|)0*([0-9])', '\\1\\2\\3')`
}

func guardedNumericBucketSQL(input, candidate, numberType, additionalGuard, normalizedValue string) string {
	unsupported := "CAST(throwIf(toUInt8(1), '" + UnsupportedNumericBinValueMarker + "') AS " + numberType + ")"
	value := "assumeNotNull(" + candidate + ")"
	guard := "isNotNull(" + candidate + ")"
	if additionalGuard != "" {
		guard += " AND " + additionalGuard
	}
	if normalizedValue != "" {
		value = normalizedValue
	}
	return "if(isNull(" + input + "), " + input + ", if(" + guard + ", " + value + ", " + unsupported + "))"
}

func signedBucketIntermediateType(numberType string) (string, bool) {
	// Int256 is intentionally absent. ClickHouse has no wider exact signed
	// type in which to calculate the bucket below minInt256 before the guarded
	// cast back to the source type. The compiler never exposes Int256 as a
	// fixed result field.
	switch numberType {
	case "Int8", "Int16", "Int32", "Int64":
		return "Int128", true
	case "Int128":
		return "Int256", true
	default:
		return "", false
	}
}

func unsignedBucketIntermediateType(numberType string) (string, bool) {
	switch numberType {
	case "UInt8", "UInt16", "UInt32", "UInt64":
		return "UInt64", true
	case "UInt128":
		return "UInt128", true
	case "UInt256":
		return "UInt256", true
	default:
		return "", false
	}
}

func unsupportedNumericBinFieldType(operator *plan.NumericBucket) error {
	return &plan.Diagnostic{
		Code:    "SPL_UNSUPPORTED_BIN_FIELD_TYPE",
		Message: "bin with a numeric span requires a fixed numeric field or a runtime-typed event field",
		Range:   operator.Input.Range,
		Suggestions: []string{
			"create a numeric field with eval before bin",
			"aggregate to a fixed numeric field with stats before bin",
		},
	}
}

func validateCanonicalFieldRef(operation, role string, field plan.FieldRef) error {
	resolved, err := plan.ResolveField(field.Name, field.Range)
	if err != nil {
		// A single-quoted downstream reference can name a literal stats output
		// that is deliberately not a dotted storage path (for example ".com").
		// ResolveQuotedField preserves that provenance as one logical segment;
		// resolveCompiledField must still find the exact name in state.visible,
		// so this fallback cannot mint access to a raw Dynamic storage path.
		resolved, err = plan.ResolveQuotedField(field.Name, field.Range)
	}
	if err != nil {
		return fmt.Errorf("compile ClickHouse %s: invalid %s field: %w", operation, role, err)
	}
	if resolved.Name != field.Name || resolved.Canonical != field.Canonical ||
		(resolved.Path == nil) != (field.Path == nil) ||
		!slices.Equal(resolved.Path, field.Path) {
		return fmt.Errorf("compile ClickHouse %s: %s field metadata is not canonical", operation, role)
	}
	return nil
}

func compileBucketProjection(
	fragment string,
	state compileState,
	inputName string,
	output plan.FieldRef,
	value string,
	source fieldState,
	alias string,
) (string, compileState) {
	return upsertFieldProjectionSQL(fragment, state, output.Name, value, alias),
		updateBucketCompileState(state, inputName, output, source)
}

func upsertFieldProjectionSQL(
	fragment string,
	state compileState,
	outputName string,
	value string,
	alias string,
) string {
	projection := upsertWildcardFieldProjection(
		"*",
		state,
		outputName,
		value,
		alias,
		authoredFieldPhysicallyPublic(state, outputName),
	)
	return "SELECT " + projection + " FROM (" + fragment + ") AS " + alias
}

func upsertFieldProjectionWithPrivateSQL(
	fragment string,
	state compileState,
	outputName string,
	value string,
	privateProjection string,
	alias string,
) string {
	projection := upsertWildcardFieldProjection(
		"*",
		state,
		outputName,
		value,
		alias,
		authoredFieldPhysicallyPublic(state, outputName),
	)
	return "SELECT " + projection + ", " + privateProjection +
		" FROM (" + fragment + ") AS " + alias
}

// authoredFieldPhysicallyPublic distinguishes a logical compiler field from a
// same-named column that actually exists in the current relation. Aggregate
// and chronological producers may deliberately retain a compiler-private
// physical identifier until a later publication boundary; SELECT REPLACE is
// invalid for those fields even though they are present in state.visible.
func authoredFieldPhysicallyPublic(state compileState, name string) bool {
	field, exists := state.visible[name]
	return exists && field.valueSQL == quoteIdentifier(name)
}

// logicalFieldPathSegments returns the canonical logical path represented by
// a public SPL field name. Backslash-escaped dots remain inside one segment;
// safe quoted aggregate names that are not valid paths are one opaque segment.
// Comparing these segments instead of string prefixes prevents a literal-dot
// field such as parent\.child from colliding with the path parent.child.
func logicalFieldPathSegments(name string) ([]string, bool) {
	resolved, err := plan.ResolveField(name, spl.Range{})
	if err != nil {
		resolved, err = plan.ResolveQuotedField(name, spl.Range{})
	}
	if err != nil {
		return nil, false
	}
	if resolved.Canonical {
		return []string{resolved.Name}, true
	}
	if len(resolved.Path) == 0 {
		return nil, false
	}
	return resolved.Path, true
}

func strictLogicalFieldPathPrefix(prefix, value []string) bool {
	return len(prefix) < len(value) && slices.Equal(prefix, value[:len(prefix)])
}

func strictAuthoredFieldNamePrefix(prefix, value string) bool {
	return prefix != "" && strings.HasPrefix(value, prefix+".")
}

func logicalFieldNamesOverlap(first, second string) (bool, bool) {
	firstPath, firstOK := logicalFieldPathSegments(first)
	secondPath, secondOK := logicalFieldPathSegments(second)
	if firstOK && secondOK {
		return strictLogicalFieldPathPrefix(firstPath, secondPath) ||
			strictLogicalFieldPathPrefix(secondPath, firstPath), true
	}
	// Safe stats literal outputs can be public physical columns without being
	// valid SPL paths (for example parent..child). ClickHouse still applies its
	// dotted Dynamic namespace rules to that spelling. Fall back only when at
	// least one logical parse failed, and require the literal dot boundary in the
	// authored names. A parsed escaped-dot name such as parent\.child therefore
	// remains one opaque segment and never collides with parent.
	if strictAuthoredFieldNamePrefix(first, second) ||
		strictAuthoredFieldNamePrefix(second, first) {
		return true, true
	}
	return false, false
}

// publicFieldNamespaceOverlapReplacements protects independently authored
// dotted columns from ClickHouse's Dynamic namespace expansion. When SELECT *
// publishes a new ancestor (or descendant), ClickHouse 26.3 can silently drop
// an already-public overlapping column before the next relational boundary.
// Re-emitting each physical overlap through the input alias in the same
// REPLACE clause preserves the independent SPL columns without changing their
// logical state, sidecars, order, or relational depth.
func publicFieldNamespaceOverlapReplacements(
	state compileState,
	outputName string,
	relationAlias string,
) []string {
	replacements := make([]string, 0)
	for _, candidateName := range orderedVisibleNames(state) {
		if candidateName == outputName {
			continue
		}
		if overlaps, ok := logicalFieldNamesOverlap(outputName, candidateName); !ok || !overlaps {
			continue
		}
		candidate := state.visible[candidateName]
		publicName := quoteIdentifier(candidateName)
		if candidate.valueSQL != publicName {
			// A private physical producer has no same-named relation column to
			// preserve. Its logical publication remains the consumer's job.
			continue
		}
		replacements = append(
			replacements,
			relationAlias+"."+publicName+" AS "+publicName,
		)
	}
	return replacements
}

// preserveWildcardFieldNamespace appends only the overlap-preserving REPLACE
// clause to an existing wildcard projection such as "*" or "* EXCEPT (...)".
func preserveWildcardFieldNamespace(
	base string,
	state compileState,
	outputName string,
	relationAlias string,
) string {
	replacements := publicFieldNamespaceOverlapReplacements(
		state,
		outputName,
		relationAlias,
	)
	if len(replacements) == 0 {
		return base
	}
	return base + " REPLACE (" + strings.Join(replacements, ", ") + ")"
}

// upsertWildcardFieldProjection builds one wildcard projection that preserves
// every physical ancestor/descendant and then either replaces or appends the
// authored output. Keeping all replacements in one clause is accepted by the
// pinned ClickHouse analyzer and avoids an additional relational layer.
func upsertWildcardFieldProjection(
	base string,
	state compileState,
	outputName string,
	valueSQL string,
	relationAlias string,
	replaceOutput bool,
) string {
	replacements := publicFieldNamespaceOverlapReplacements(
		state,
		outputName,
		relationAlias,
	)
	publicName := quoteIdentifier(outputName)
	if replaceOutput {
		replacements = append(replacements, valueSQL+" AS "+publicName)
	}
	if len(replacements) > 0 {
		base += " REPLACE (" + strings.Join(replacements, ", ") + ")"
	}
	if !replaceOutput {
		base += ", " + valueSQL + " AS " + publicName
	}
	return base
}

func updateBucketCompileState(state compileState, inputName string, output plan.FieldRef, source fieldState) compileState {
	next := prepareBucketCompileState(state, inputName, output)
	next.visible[output.Name] = fieldState{
		valueSQL:                quoteIdentifier(output.Name),
		maxStringBytes:          source.maxStringBytes,
		existsSQL:               source.existsSQL,
		existsArgs:              append([]any(nil), source.existsArgs...),
		dynamicDomain:           source.dynamicDomain,
		kind:                    source.kind,
		numberType:              source.numberType,
		numericSort:             source.numericSort,
		canonicalTime:           false,
		caseSensitive:           false,
		materializeForPredicate: source.materializeForPredicate,
	}
	next.privateColumns = livePrivateColumns(next.privateColumns, next.visible)
	return next
}

func updateDynamicBucketCompileState(
	state compileState,
	inputName string,
	output plan.FieldRef,
	source fieldState,
	existsAlias, typeAlias string,
	namesAlias, typesAlias, metadataAlias string,
) compileState {
	next := prepareBucketCompileState(state, inputName, output)
	if inputName != output.Name {
		if _, retained := next.visible[inputName]; !retained {
			next.visible[inputName] = source
		}
	}
	outputName := quoteIdentifier(output.Name)
	next.visible[output.Name] = fieldState{
		valueSQL:                outputName,
		dynamicTypeSQL:          "dynamicType(" + outputName + ")",
		storedTypeSQL:           typeAlias,
		existsSQL:               existsAlias,
		relativeFieldNamesSQL:   namesAlias,
		relativeFieldTypesSQL:   typesAlias,
		fieldMetadataVersionSQL: metadataAlias,
		kind:                    fieldKindDynamic,
		caseSensitive:           false,
		materializeForPredicate: true,
	}
	next.privateColumns = livePrivateColumns(next.privateColumns, next.visible)
	next.privateColumns = append(next.privateColumns, existsAlias, typeAlias)
	if namesAlias != "" {
		next.privateColumns = append(
			next.privateColumns,
			namesAlias,
			typesAlias,
			metadataAlias,
		)
	}
	return next
}

func prepareBucketCompileState(state compileState, inputName string, output plan.FieldRef) compileState {
	next := cloneCompileState(state)
	if exposesRawFieldsPayload(state) && !output.Canonical {
		// A calculated top-level field shadows any same-named value still held
		// in the immutable event payload. Publishing that payload would expose
		// two contradictory values for one SPL field.
		dropRawFieldsPayload(&next)
	}
	if inputName != output.Name && !slices.Contains(next.publicOrder, inputName) {
		next.publicOrder = append(next.publicOrder, inputName)
	}
	if !slices.Contains(next.publicOrder, output.Name) {
		next.publicOrder = append(next.publicOrder, output.Name)
	}
	delete(next.blocked, output.Name)
	return next
}

func floorBucketTicks(value, span int64) int64 {
	quotient := value / span
	if value%span < 0 {
		quotient--
	}
	return quotient * span
}

// pivotDescendantSourceColumns keeps descendant probes available across the
// narrow chart/timechart source CTE. Stored event fields evaluate their probe
// from the immutable field-name array. Materialized fields, including
// knowledge-generated fields, instead point at one retained private sidecar;
// that identifier must cross the source CTE so the prepared CTE can evaluate
// it without moving bind markers ahead of the nested scoped relation.
func pivotDescendantSourceColumns(state compileState, fields ...fieldState) []string {
	private := make([]string, 0, len(fields))
	hasDescendant := false
	for _, field := range fields {
		if field.kind != fieldKindDynamic || field.descendantSQL == "" {
			continue
		}
		hasDescendant = true
		for _, column := range state.privateColumns {
			// Some calculated container producers retain an aligned names array
			// and express presence as notEmpty(<names sidecar>) instead of minting
			// a separate Boolean column. Carry every compiler-private identifier
			// referenced by that sealed expression across the narrow pivot source
			// CTE; carrying only an expression that is itself a private column drops
			// fillnull/strcat container authority before chart/timechart prepares it.
			if strings.Contains(field.descendantSQL, column) &&
				!slices.Contains(private, column) {
				private = append(private, column)
			}
		}
	}
	if !hasDescendant {
		return nil
	}
	return append([]string{quoteIdentifier(internalFieldNamesColumn)}, private...)
}

func newCompileContext(searchStart time.Time, searchTimezone string) *compileContext {
	return &compileContext{
		operationContext: context.Background(),
		patternBudgets: compiledPatternBudgets{
			match: compiledMatchBudget{
				patterns: make(map[*plan.ScalarCallExpression]splregex.MatchPattern),
			},
			like: compiledLikeBudget{
				patterns: make(map[*plan.ScalarCallExpression]splwildcard.LikePattern),
			},
		},
		strftimeBudget: compiledStrftimeBudget{
			formats: make(map[*plan.ScalarCallExpression]spltimeformat.StrftimeFormat),
		},
		searchStartUnix: searchStart.Unix(),
		searchTimezone:  strings.Clone(searchTimezone),
	}
}

type compiledStrftimeBudget struct {
	formats     map[*plan.ScalarCallExpression]spltimeformat.StrftimeFormat
	workUnits   int
	outputBytes uint64
}

type compiledStrptimeBudget struct {
	formats    map[*plan.ScalarCallExpression]spltimeformat.StrptimeFormat
	workUnits  int
	inputBytes uint64
}

type compiledRelativeTimeBudget struct {
	specifiers map[*plan.ScalarCallExpression]splrelativetime.Specifier
	workUnits  int
	operations int
}

type compiledUnixTimestampBudget struct {
	dynamicDecimalBytes uint64
}

type compiledConcatenationBudget struct {
	operands    int
	outputBytes uint64
}

type compiledStringConversionBudget struct {
	dynamicDecimalBytes uint64
}

type relativeTimeResultDirection uint8

const (
	relativeTimeResultBefore relativeTimeResultDirection = iota + 1
	relativeTimeResultAfter
	relativeTimeResultNotAfter
)

type compiledMatchBudget struct {
	patterns         map[*plan.ScalarCallExpression]splregex.MatchPattern
	programWorkUnits int
}

type compiledPatternBudgets struct {
	match compiledMatchBudget
	like  compiledLikeBudget
}

type compiledLikeBudget struct {
	patterns   map[*plan.ScalarCallExpression]splwildcard.LikePattern
	workUnits  int
	inputBytes uint64
}

type compiledChronologicalMeasure struct {
	winnerColumn     string
	validationColumn string
	typeColumn       string
	outputColumn     string
}

// compiledChronologicalBarrier owns one deferred relation stage, its bind
// arguments, and any hidden columns checked by the final validation envelope.
// Eventstats stages without validation also use this graph so a later extrema
// stage never nests MATERIALIZED CTEs. Keeping validation outside downstream
// SPL operators prevents ClickHouse from pruning it behind an empty filter.
type compiledChronologicalBarrier struct {
	name                    string
	sql                     string
	prerequisiteDefinitions []string
	args                    []any
	validationColumns       []string
	fanout                  uint64
	depth                   int
	ownerRange              spl.Range
}

// pendingChronologicalBarrier cannot enter compile state until its complete
// placeholder prefix has been captured. This keeps argument ownership atomic
// when a deferred stage takes responsibility for every query stage compiled
// since the preceding barrier.
type pendingChronologicalBarrier struct {
	name                         string
	sql                          string
	prerequisiteDefinitions      []string
	prefixArgumentsAfterExisting bool
	validationColumns            []string
	fanout                       uint64
	depth                        int
	ownerRange                   spl.Range
}

// appendVisibleFieldProjection appends the projection term for a visible field,
// omitting the redundant alias when the value expression is already the public
// column name.
func appendVisibleFieldProjection(projection []string, field fieldState, publicName string) []string {
	if field.valueSQL == publicName {
		return append(projection, publicName)
	}
	return append(projection, field.valueSQL+" AS "+publicName)
}

// dynamicPresenceOperands returns the presence and descendant predicates for a
// field together with their bind values, in placeholder order.
func dynamicPresenceOperands(field fieldState) (existsSQL, descendantSQL string, args []any) {
	existsSQL = field.existsSQL
	if existsSQL == "" {
		existsSQL = "1"
	}
	descendantSQL = "0"
	args = append([]any(nil), field.existsArgs...)
	if field.descendantSQL != "" {
		descendantSQL = field.descendantSQL
		args = append(args, field.descendantArgs...)
	}
	return existsSQL, descendantSQL, args
}

func bindChronologicalBarrier(state compileState, barrier *pendingChronologicalBarrier, args []any) (compileState, []any) {
	if barrier == nil {
		return state, args
	}
	state.chronologicalBarriers = append(state.chronologicalBarriers, barrier.bind(args))
	return state, nil
}

func (barrier pendingChronologicalBarrier) bind(args []any) compiledChronologicalBarrier {
	return compiledChronologicalBarrier{
		name:                    barrier.name,
		sql:                     barrier.sql,
		prerequisiteDefinitions: append([]string(nil), barrier.prerequisiteDefinitions...),
		args:                    append([]any(nil), args...),
		validationColumns:       append([]string(nil), barrier.validationColumns...),
		fanout:                  barrier.fanout,
		depth:                   barrier.depth,
		ownerRange:              barrier.ownerRange,
	}
}

type compiledScalarExtremaMeasure struct {
	winnerColumn string
	typeColumn   string
	outputColumn string
}

type compiledExactStringMeasure struct {
	setColumn    string
	outputColumn string
	function     plan.AggregateFunction
}

type compiledOrderedStringMeasure struct {
	listColumn     string
	overflowColumn string
	outputColumn   string
}

// compiledDistinctCount carries an already-materialized exact cardinality
// through the whole-result overflow barrier without retaining the underlying
// string set beyond the aggregate projection.
type compiledDistinctCount struct {
	cardinalityColumn string
	outputColumn      string
}

type fieldKind uint8

const (
	fieldKindInvalid fieldKind = iota
	fieldKindDynamic
	fieldKindString
	fieldKindNumber
	fieldKindBool
	fieldKindTime
	fieldKindStringArray
	// fieldKindDynamicArray is the native, bounded multivalue representation
	// used when members may have different admitted JSON scalar types.  Unlike a
	// fieldKindDynamic whose runtime value may happen to contain an array, its
	// physical ClickHouse type is always Array(Dynamic).
	fieldKindDynamicArray
)

func isNativeMultivalueKind(kind fieldKind) bool {
	return kind == fieldKindStringArray || kind == fieldKindDynamicArray
}

type dynamicScalarDomain uint8

const (
	dynamicScalarDomainAny dynamicScalarDomain = iota
	dynamicScalarDomainText
	dynamicScalarDomainNumeric
)

func unsupportedMultivalueUsage(operation string, sourceRange spl.Range) error {
	return &plan.Diagnostic{
		Code:    "SPL_UNSUPPORTED_MULTIVALUE_USAGE",
		Message: operation + " does not yet support a multivalue field",
		Range:   sourceRange,
	}
}

type fieldState struct {
	valueSQL                  string
	exactNumericKeySQL        string
	dynamicNumericEligibleSQL string
	maxStringBytes            uint64
	textEligibleSQL           string
	rawTextIndexEligible      bool
	dynamicDomain             dynamicScalarDomain
	numericIntegral           bool
	mvCountOneOrNull          bool
	mvSortedLexicographic     bool
	dynamicTypeSQL            string
	storedTypeSQL             string
	existsSQL                 string
	existsArgs                []any
	descendantSQL             string
	descendantArgs            []any
	storedPath                storedPathAuthority
	// storedMetadataPath retains the normalized source inventory path when a
	// projection freezes presence into a private sidecar and clears the
	// placeholder-bearing exists/descendant arguments. It is compiler-authored
	// semantic type evidence only; it never authorizes a Dynamic value read.
	storedMetadataPath string
	// rawEventDynamic records that this field is an exact projection of the
	// immutable event Dynamic payload and still uses the stored field-name
	// inventory for row-local presence. A later wildcard projection must keep
	// that inventory entry; calculated and renamed shadows deliberately do not.
	rawEventDynamic              bool
	relativeFieldNamesSQL        string
	relativeFieldTypesSQL        string
	fieldMetadataVersionSQL      string
	optionalMultivaluePresentSQL string
	// semanticBytesSQL is a sealed, row-local UInt8/Boolean authority for a
	// fixed String whose public SPL cell may be either String or Bytes. Unlike
	// textEligibleSQL it distinguishes valid UTF-8 bytes declared as binary.
	semanticBytesSQL string
	// textEligibleBySemanticBytes marks outputs whose text eligibility can be
	// rebound exactly from the public value and semanticBytesSQL. Unlike
	// semanticBytesByUTF8Validity, the semantic bit remains authoritative even
	// when a Bytes payload happens to be valid UTF-8.
	textEligibleBySemanticBytes bool
	// semanticBytesByUTF8Validity marks fixed-String producers whose byte
	// classification is exactly invalid UTF-8 (for example values/list members).
	// Their text proof may be safely rebound to a projected value column.
	semanticBytesByUTF8Validity bool
	// stringOrBytes marks a byte-preserving String scalar, or an Array(String)
	// whose members retain that same public String-or-Bytes domain. It is
	// compiler provenance rather than a ClickHouse physical type.
	stringOrBytes bool
	// stringOrBytesNullable records the exact physical String nullability used
	// by the result transport. Byte-capable concatenation deliberately
	// normalizes to Nullable(String), so its descriptor does not have to infer
	// nullability from every ordinary String-producing operand.
	stringOrBytesNullable      bool
	kind                       fieldKind
	caseSensitive              bool
	numberType                 string
	numericSort                bool
	canonicalTime              bool
	alwaysNull                 bool
	ieeeComparison             bool
	materializeForPredicate    bool
	flatMultivalueDelimiter    string
	hasFlatMultivalueDelimiter bool
	statsSparkline             bool
}

type compiledSortKey struct {
	valueSQL           string
	descending         bool
	nullsFirst         bool
	separatePresence   bool
	presenceDescending bool
}

func compileScan(
	database, table string,
	scan *plan.Scan,
	searchStart time.Time,
	searchTimezone string,
) (string, compileState, []any, error) {
	if scan.TenantID == "" || len(scan.Indexes) == 0 || scan.Earliest.IsZero() || scan.Latest.IsZero() || !scan.Earliest.Before(scan.Latest) || scan.IndexTimeCutoff.IsZero() {
		return "", compileState{}, nil, errors.New("compile ClickHouse query: Scan has an invalid security or time scope")
	}
	if searchStart.IsZero() {
		return "", compileState{}, nil, errors.New("compile ClickHouse query: search-start anchor is required")
	}
	selects := []string{
		aliasPhysical("event_id", "event_id"),
		aliasPhysical("index_name", "index"),
		aliasPhysical("event_time", "_time"),
		aliasPhysical("index_time", "_indextime"),
		aliasPhysical("host", "host"),
		aliasPhysical("source", "source"),
		aliasPhysical("sourcetype", "sourcetype"),
		aliasPhysical("service", "service"),
		aliasPhysical("severity", "severity"),
		aliasPhysical("level", "level"),
		aliasPhysical("body", "message"),
		aliasPhysical("raw", "_raw"),
		aliasPhysical("raw_encoding", internalRawEncodingColumn),
		aliasPhysical("trace_id", "trace_id"),
		aliasPhysical("span_id", "span_id"),
		aliasPhysical("collector_id", "collector_id"),
		aliasPhysical("batch_id", "batch_id"),
		aliasPhysical("fields", internalFieldsColumn),
		aliasPhysical("field_names", internalFieldNamesColumn),
		aliasPhysical("field_types", internalFieldTypesColumn),
		aliasPhysical("field_metadata_version", internalFieldMetadataVersionColumn),
		aliasPhysical("event_time", internalSortTimeColumn),
		aliasPhysical("event_id", internalSortIDColumn),
		aliasPhysical("visibility_seq", internalSortVisibilityColumn),
		"tuple(" +
			strings.Join([]string{
				quoteIdentifier("index_name"),
				quoteIdentifier("collector_id"),
				quoteIdentifier("batch_sequence"),
				quoteIdentifier("batch_id"),
			}, ", ") +
			") AS " + quoteIdentifier(internalSortSourceIdentityColumn),
	}
	placeholders := strings.TrimSuffix(strings.Repeat("?, ", len(scan.Indexes)), ", ")
	where := []string{
		quoteIdentifier("tenant_id") + " = ?",
		quoteIdentifier("index_name") + " IN (" + placeholders + ")",
		quoteIdentifier("event_time") + " >= parseDateTime64BestEffort(?, 9, 'UTC')",
		quoteIdentifier("event_time") + " < parseDateTime64BestEffort(?, 9, 'UTC')",
		quoteIdentifier("index_time") + " <= parseDateTime64BestEffort(?, 3, 'UTC')",
		quoteIdentifier("expires_at") + " > parseDateTime64BestEffort(?, 3, 'UTC')",
		quoteIdentifier("visibility_seq") + " <= ?",
	}
	args := make([]any, 0, len(scan.Indexes)+6)
	args = append(args, compiledReadScopeArgument{ordinal: 0, value: scan.TenantID})
	for ordinal, index := range scan.Indexes {
		args = append(args, compiledReadScopeArgument{ordinal: ordinal + 1, value: index})
	}
	// clickhouse-go infers a bare time.Time placeholder as DateTime, which has
	// only second precision. Bind canonical text and parse it explicitly so the
	// index-time and retention predicates share the exact immutable
	// DateTime64(3) snapshot. Text also avoids UnixNano overflow for supported
	// pre-epoch and upper-bound DateTime64 values.
	indexTimeCutoff := formatDateTime64Milliseconds(scan.IndexTimeCutoff)
	args = append(args,
		formatDateTime64Nanoseconds(scan.Earliest),
		formatDateTime64Nanoseconds(scan.Latest),
		indexTimeCutoff,
		indexTimeCutoff,
		scan.VisibilityCutoff,
	)

	visible := make(map[string]fieldState, len(canonicalColumnNames))
	for _, field := range canonicalColumnNames {
		visible[field] = canonicalState(field)
	}
	state := compileState{
		visible:         visible,
		context:         newCompileContext(searchStart, searchTimezone),
		publicOrder:     append([]string(nil), defaultPublicFields...),
		allowDynamic:    true,
		eventRows:       true,
		blocked:         make(map[string]struct{}),
		blockedPrefixes: make(map[string]struct{}),
		tieBreakers: []compiledSortKey{
			{valueSQL: quoteIdentifier(internalSortIDColumn), descending: true},
			{valueSQL: quoteIdentifier(internalSortVisibilityColumn), descending: true},
			{valueSQL: quoteIdentifier(internalSortSourceIdentityColumn), descending: true},
		},
	}
	state.context.searchEarliest = scan.Earliest
	state.context.searchLatest = scan.Latest
	return "SELECT " + strings.Join(selects, ", ") + " FROM " + quoteIdentifier(database) + "." + quoteIdentifier(table) + " WHERE " + strings.Join(where, " AND "), state, args, nil
}

func formatDateTime64Nanoseconds(value time.Time) string {
	return value.UTC().Format("2006-01-02 15:04:05.000000000")
}

func formatDateTime64Milliseconds(value time.Time) string {
	return value.UTC().Truncate(time.Millisecond).Format("2006-01-02 15:04:05.000")
}

var canonicalColumnNames = eventfields.CanonicalSPLFieldNames()

var defaultPublicFields = []string{
	"_time", "_raw", "index", "host", "source", "sourcetype", "service", "level", "message", "trace_id", "span_id", "event_id", "_indextime", "fields",
}

func canonicalState(field string) fieldState {
	value := quoteIdentifier(field)
	kind := fieldKindString
	switch field {
	case "severity":
		kind = fieldKindNumber
	case "_time", "_indextime":
		kind = fieldKindTime
	}
	// Canonical columns exist in the event schema even when their value is
	// nullable. This preserves explicit-null comparisons; field=* separately
	// requires a non-null value.
	state := fieldState{valueSQL: value, existsSQL: "1", kind: kind, caseSensitive: field == "index"}
	if field == "severity" {
		state.numberType = "UInt8"
	}
	if field == "_time" {
		state.numberType = "DateTime64(9, 'UTC')"
	}
	if field == "_indextime" {
		state.numberType = "DateTime64(3, 'UTC')"
	}
	if field == "_time" {
		state.canonicalTime = true
	}
	if field == "_raw" {
		state.textEligibleSQL = quoteIdentifier(internalRawEncodingColumn) + " = " +
			strconv.Itoa(rawEncodingUTF8)
		state.semanticBytesSQL = quoteIdentifier(internalRawEncodingColumn) + " = " +
			strconv.Itoa(rawEncodingBinary)
		state.stringOrBytes = true
		state.rawTextIndexEligible = true
	}
	return state
}

func compileExpression(expression plan.Expression, state compileState) (string, []any, error) {
	return compileExpressionWithRawTextIndex(expression, state, false)
}

// compileFilterExpression permits a native text-index candidate only for a
// positive filter over the canonical physical _raw lineage. Other expression
// consumers (for example eval conditions and aggregate predicates) retain the
// exact scan predicate because an index candidate there cannot prune the
// physical event read. A NOT boundary disables candidates for its complete
// operand: negative token lookup cannot safely or usefully narrow the scan.
func compileFilterExpression(expression plan.Expression, state compileState) (string, []any, error) {
	return compileExpressionWithRawTextIndex(expression, state, true)
}

func compileExpressionWithRawTextIndex(
	expression plan.Expression,
	state compileState,
	allowRawTextIndex bool,
) (string, []any, error) {
	switch expression := expression.(type) {
	case *plan.BooleanExpression:
		left, leftArgs, err := compileExpressionWithRawTextIndex(
			expression.Left,
			state,
			allowRawTextIndex,
		)
		if err != nil {
			return "", nil, err
		}
		right, rightArgs, err := compileExpressionWithRawTextIndex(
			expression.Right,
			state,
			allowRawTextIndex,
		)
		if err != nil {
			return "", nil, err
		}
		operator := "AND"
		if expression.Op == plan.BooleanOpOr {
			operator = "OR"
		}
		return "(" + left + " " + operator + " " + right + ")", append(leftArgs, rightArgs...), nil
	case *plan.NotExpression:
		operand, args, err := compileExpressionWithRawTextIndex(
			expression.Operand,
			state,
			false,
		)
		if err != nil {
			return "", nil, err
		}
		return "NOT (" + operand + ")", args, nil
	case *plan.TextExpression:
		raw, ok := state.visible["_raw"]
		if !ok {
			return "0", nil, nil
		}
		if expression.Value == "*" {
			return "isNotNull(" + raw.valueSQL + ")", nil, nil
		}
		if expression.Wildcard {
			return "match(toString(" + raw.valueSQL + "), ?)", []any{freeTextRegex(expression.Value, expression.Quoted)}, nil
		}
		if !expression.Quoted {
			verifier := freeTextRegex(expression.Value, false)
			if allowRawTextIndex && raw.rawTextIndexEligible &&
				rawTextIndexTokenEligible(expression.Value) {
				candidate := "has(" + rawTextIndexTokensSQL(raw.valueSQL) + ", lower(?))"
				exact := "match(toString(" + raw.valueSQL + "), ?)"
				return "(" + candidate + " AND " + exact + ")",
					[]any{expression.Value, verifier}, nil
			}
			return "match(toString(" + raw.valueSQL + "), ?)", []any{verifier}, nil
		}
		return "positionCaseInsensitiveUTF8(toString(" + raw.valueSQL + "), ?) > 0", []any{expression.Value}, nil
	case *plan.ComparisonExpression:
		field, ok, err := resolveCompiledField(expression.Field, state)
		if err != nil {
			return "", nil, err
		}
		if !ok {
			return "0", nil, nil
		}
		return compileComparison(expression, field)
	case *plan.EvalComparisonExpression:
		return compileEvalComparison(expression, state)
	case *plan.MembershipExpression:
		return compileMembershipExpression(expression, state)
	case *plan.ScalarPredicateExpression:
		if expression == nil || expression.Value == nil {
			return "", nil, errors.New("compile ClickHouse predicate: missing Boolean scalar expression")
		}
		if err := validateCompiledScalarComplexity(expression.Value); err != nil {
			return "", nil, err
		}
		value, err := compileScalarValue(expression.Value, state)
		if err != nil {
			return "", nil, err
		}
		if value.kind != fieldKindBool {
			return "", nil, errors.New("compile ClickHouse predicate: scalar expression must return Boolean")
		}
		return "ifNull(" + value.valueSQL + ", 0)", append([]any(nil), value.valueArgs...), nil
	default:
		return "", nil, fmt.Errorf("compile ClickHouse predicate: unsupported expression %T", expression)
	}
}

// predicateMaterializationFields returns calculated columns that still depend
// on a complex Dynamic projection and must be row-bound before ClickHouse
// analyzes this predicate. The server otherwise pushes the predicate through
// that producer and can expand its expression graph without a practical bound.
// Preserve first-reference order so generated SQL is deterministic.
func predicateMaterializationFields(expression plan.Expression, state compileState) []string {
	var fields []string
	seen := make(map[string]struct{})
	add := func(name string) {
		if !fieldNeedsPredicateMaterialization(name, state) {
			return
		}
		if _, exists := seen[name]; exists {
			return
		}
		seen[name] = struct{}{}
		fields = append(fields, name)
	}

	var visit func(plan.Expression)
	var visitScalar func(plan.ScalarExpression)
	visitScalar = func(expression plan.ScalarExpression) {
		switch expression := expression.(type) {
		case *plan.ScalarUnaryExpression:
			if expression != nil {
				visitScalar(expression.Operand)
			}
		case *plan.ScalarBinaryExpression:
			if expression != nil {
				visitScalar(expression.Left)
				visitScalar(expression.Right)
			}
		case *plan.ScalarFieldExpression:
			if expression != nil {
				add(expression.Field.Name)
			}
		case *plan.ScalarCallExpression:
			if expression == nil {
				return
			}
			for _, argument := range expression.Arguments {
				visitScalar(argument)
			}
		case *plan.ScalarIfExpression:
			if expression == nil {
				return
			}
			visit(expression.Condition)
			visitScalar(expression.True)
			visitScalar(expression.False)
		case *plan.ScalarCaseExpression:
			if expression == nil {
				return
			}
			for _, branch := range expression.Branches {
				visit(branch.Condition)
				visitScalar(branch.Value)
			}
		}
	}

	visit = func(expression plan.Expression) {
		switch expression := expression.(type) {
		case *plan.BooleanExpression:
			if expression != nil {
				visit(expression.Left)
				visit(expression.Right)
			}
		case *plan.NotExpression:
			if expression != nil {
				visit(expression.Operand)
			}
		case *plan.TextExpression:
			if expression != nil {
				add("_raw")
			}
		case *plan.ComparisonExpression:
			if expression != nil {
				add(expression.Field.Name)
			}
		case *plan.EvalComparisonExpression:
			if expression != nil {
				visitScalar(expression.Left)
				visitScalar(expression.Right)
			}
		case *plan.MembershipExpression:
			if expression != nil {
				visitScalar(expression.Value)
				for _, candidate := range expression.Candidates {
					visitScalar(candidate)
				}
			}
		case *plan.ScalarPredicateExpression:
			if expression != nil {
				visitScalar(expression.Value)
			}
		}
	}
	visit(expression)
	return fields
}

// repeatedExactNumericPredicateFields identifies ordinary Dynamic fields whose
// exact decimal key would otherwise be expanded more than once in one filter.
// The caller materializes each key in a private column and removes it
// immediately after filtering. This keeps the parser's legal 32-predicate
// ceiling well below the compiled-SQL byte ceiling without adding a stage to
// the overwhelmingly common one-comparison path.
func repeatedExactNumericPredicateFields(
	expression plan.Expression,
	state compileState,
) []plan.FieldRef {
	counts := make(map[string]int)
	references := make(map[string]plan.FieldRef)
	conflicts := make(map[string]struct{})
	order := make([]plan.FieldRef, 0)
	add := func(reference plan.FieldRef) {
		field, ok, err := resolveCompiledField(reference, state)
		if err != nil || !ok || field.kind != fieldKindDynamic ||
			field.dynamicDomain != dynamicScalarDomainAny ||
			field.materializeForPredicate {
			return
		}
		if previous, exists := references[reference.Name]; exists {
			if previous.Canonical != reference.Canonical ||
				!slices.Equal(previous.Path, reference.Path) {
				// Parser-built plans keep Name/Path canonical, but a forged
				// plan can reuse one public name for distinct dynamic paths.
				// Leave that case unoptimized so the ordinary resolver
				// preserves its established behavior.
				conflicts[reference.Name] = struct{}{}
				return
			}
		} else {
			references[reference.Name] = reference
		}
		if counts[reference.Name] == 0 {
			order = append(order, reference)
		}
		counts[reference.Name]++
	}
	directField := func(expression plan.ScalarExpression) (plan.FieldRef, bool) {
		field, ok := expression.(*plan.ScalarFieldExpression)
		if !ok || field == nil {
			return plan.FieldRef{}, false
		}
		return field.Field, true
	}
	numericLiteral := func(expression plan.ScalarExpression) bool {
		literal, ok := expression.(*plan.ScalarLiteralExpression)
		if !ok || literal == nil {
			return false
		}
		return literal.Value.Kind == plan.ValueKindInt64 ||
			literal.Value.Kind == plan.ValueKindUint64 ||
			literal.Value.Kind == plan.ValueKindFloat64
	}

	var visit func(plan.Expression)
	visit = func(expression plan.Expression) {
		switch expression := expression.(type) {
		case *plan.BooleanExpression:
			if expression != nil {
				visit(expression.Left)
				visit(expression.Right)
			}
		case *plan.NotExpression:
			if expression != nil {
				visit(expression.Operand)
			}
		case *plan.ComparisonExpression:
			if expression != nil &&
				(expression.Value.Kind == plan.ValueKindInt64 ||
					expression.Value.Kind == plan.ValueKindUint64 ||
					expression.Value.Kind == plan.ValueKindFloat64) {
				add(expression.Field)
			}
		case *plan.EvalComparisonExpression:
			if expression == nil {
				return
			}
			leftReference, leftField := directField(expression.Left)
			rightReference, rightField := directField(expression.Right)
			if leftField && (rightField || numericLiteral(expression.Right)) {
				add(leftReference)
			}
			if rightField && (leftField || numericLiteral(expression.Left)) {
				add(rightReference)
			}
		case *plan.MembershipExpression:
			// Membership binds its left value and every candidate once before
			// comparing them, so it never benefits from the separate repeated-key
			// projection used by independent comparison leaves.
			return
		}
	}
	visit(expression)

	fields := make([]plan.FieldRef, 0, len(order))
	for _, reference := range order {
		if _, conflict := conflicts[reference.Name]; !conflict &&
			counts[reference.Name] > 1 {
			fields = append(fields, reference)
		}
	}
	return fields
}

// bindExactNumericPredicateFields replaces repeated Dynamic numeric work with
// private key/eligibility columns. Callers must project the returned columns
// in a relational boundary before compiling the predicate with the returned
// state, and must not retain that predicate-only state as their public output
// state.
func bindExactNumericPredicateFields(
	state compileState,
	fields []plan.FieldRef,
	stage int,
	command string,
) (compileState, []string, []string, error) {
	predicateState := cloneCompileState(state)
	keyColumns := make([]string, 0, len(fields)*2)
	aliases := make([]string, 0, len(fields)*2)
	for index, reference := range fields {
		field, ok, resolveErr := resolveCompiledField(reference, predicateState)
		if resolveErr != nil {
			return compileState{}, nil, nil, resolveErr
		}
		if !ok {
			continue
		}
		scalar := compiledScalarFromField(field)
		keyAlias := quoteIdentifier(fmt.Sprintf(
			"__os_%s_exact_key_%d_%d",
			command,
			stage,
			index+1,
		))
		numericAlias := quoteIdentifier(fmt.Sprintf(
			"__os_%s_exact_numeric_%d_%d",
			command,
			stage,
			index+1,
		))
		keyColumns = append(
			keyColumns,
			exactNumericScalarKeySQL(scalar)+" AS "+keyAlias,
			"toUInt8("+dynamicNumericValuePredicate(scalar)+") AS "+numericAlias,
		)
		field.exactNumericKeySQL = keyAlias
		field.dynamicNumericEligibleSQL = numericAlias
		predicateState.visible[reference.Name] = field
		aliases = append(aliases, keyAlias, numericAlias)
	}
	return predicateState, keyColumns, aliases, nil
}

// aggregatePredicatePreparation owns the transient state and private columns
// needed to compile one conditional aggregate without leaking its aliases into
// the durable pipeline schema. Transforming, event, and stream aggregates keep
// their relation-specific materialization shape, but share this preparation so
// exact-numeric and calculated-field fencing cannot drift between commands.
type aggregatePredicatePreparation struct {
	durableState   compileState
	predicateState compileState
	bindings       []string
	boundColumns   []string
	exactColumns   []string
	exactAliases   []string
}

func prepareAggregatePredicate(
	state compileState,
	predicate plan.Expression,
	stage int,
	command string,
) (aggregatePredicatePreparation, error) {
	prepared := aggregatePredicatePreparation{
		durableState:   state,
		predicateState: state,
	}
	materializedFields := predicateMaterializationFields(predicate, state)
	if len(materializedFields) > 0 {
		prepared.predicateState, prepared.bindings, prepared.boundColumns =
			bindAggregatePredicateFields(state, materializedFields, stage)
		prepared.durableState = predicateMaterializedOutputState(
			state,
			materializedFields,
		)
	}
	exactNumericFields := repeatedExactNumericPredicateFields(
		predicate,
		prepared.predicateState,
	)
	if len(exactNumericFields) == 0 {
		return prepared, nil
	}
	var err error
	prepared.predicateState, prepared.exactColumns, prepared.exactAliases, err =
		bindExactNumericPredicateFields(
			prepared.predicateState,
			exactNumericFields,
			stage,
			command,
		)
	if err != nil {
		return aggregatePredicatePreparation{}, err
	}
	return prepared, nil
}

func aggregatePredicateMaterializationFields(
	operator *plan.Aggregate,
	state compileState,
) []string {
	fields := make([]string, 0)
	seen := make(map[string]struct{})
	for _, measure := range operator.Measures {
		if measure.Function != plan.AggregateFunctionCountPredicate {
			continue
		}
		for _, name := range predicateMaterializationFields(measure.Predicate, state) {
			if _, duplicate := seen[name]; duplicate {
				continue
			}
			seen[name] = struct{}{}
			fields = append(fields, name)
		}
	}
	return fields
}

func predicateFieldSourceRange(
	expression plan.Expression,
	name string,
) (spl.Range, bool) {
	var visitExpression func(plan.Expression) (spl.Range, bool)
	var visitScalar func(plan.ScalarExpression) (spl.Range, bool)
	visitScalar = func(expression plan.ScalarExpression) (spl.Range, bool) {
		switch expression := expression.(type) {
		case *plan.ScalarUnaryExpression:
			if expression != nil {
				return visitScalar(expression.Operand)
			}
		case *plan.ScalarBinaryExpression:
			if expression == nil {
				return spl.Range{}, false
			}
			if sourceRange, found := visitScalar(expression.Left); found {
				return sourceRange, true
			}
			return visitScalar(expression.Right)
		case *plan.ScalarFieldExpression:
			if expression != nil && expression.Field.Name == name {
				return expression.Field.Range, true
			}
		case *plan.ScalarCallExpression:
			if expression == nil {
				return spl.Range{}, false
			}
			for _, argument := range expression.Arguments {
				if sourceRange, found := visitScalar(argument); found {
					return sourceRange, true
				}
			}
		case *plan.ScalarIfExpression:
			if expression == nil {
				return spl.Range{}, false
			}
			if sourceRange, found := visitExpression(expression.Condition); found {
				return sourceRange, true
			}
			if sourceRange, found := visitScalar(expression.True); found {
				return sourceRange, true
			}
			return visitScalar(expression.False)
		case *plan.ScalarCaseExpression:
			if expression == nil {
				return spl.Range{}, false
			}
			for _, branch := range expression.Branches {
				if sourceRange, found := visitExpression(branch.Condition); found {
					return sourceRange, true
				}
				if sourceRange, found := visitScalar(branch.Value); found {
					return sourceRange, true
				}
			}
		}
		return spl.Range{}, false
	}
	visitExpression = func(expression plan.Expression) (spl.Range, bool) {
		switch expression := expression.(type) {
		case *plan.BooleanExpression:
			if expression == nil {
				return spl.Range{}, false
			}
			if sourceRange, found := visitExpression(expression.Left); found {
				return sourceRange, true
			}
			return visitExpression(expression.Right)
		case *plan.NotExpression:
			if expression != nil {
				return visitExpression(expression.Operand)
			}
		case *plan.EvalComparisonExpression:
			if expression == nil {
				return spl.Range{}, false
			}
			if sourceRange, found := visitScalar(expression.Left); found {
				return sourceRange, true
			}
			return visitScalar(expression.Right)
		case *plan.MembershipExpression:
			if expression == nil {
				return spl.Range{}, false
			}
			if sourceRange, found := visitScalar(expression.Value); found {
				return sourceRange, true
			}
			for _, candidate := range expression.Candidates {
				if sourceRange, found := visitScalar(candidate); found {
					return sourceRange, true
				}
			}
		case *plan.ScalarPredicateExpression:
			if expression != nil {
				return visitScalar(expression.Value)
			}
		}
		return spl.Range{}, false
	}
	return visitExpression(expression)
}

// bindAggregatePredicateFields creates singleton aliases that survive only
// until the immediately following transforming aggregate. The MATERIALIZED
// CTE emitted by the caller freezes calculated producers; the alias dependency
// prevents ClickHouse from expanding a predicate back through those producers.
func bindAggregatePredicateFields(
	state compileState,
	fields []string,
	stage int,
) (compileState, []string, []string) {
	boundState := cloneCompileState(state)
	bindings := make([]string, 0, len(fields))
	boundColumns := make([]string, 0, len(fields))
	for index, name := range fields {
		field := state.visible[name]
		bound := quoteIdentifier(fmt.Sprintf(
			"__os_stats_predicate_bound_%d_%d",
			stage,
			index+1,
		))
		boundValue := field.valueSQL
		if field.kind == fieldKindDynamic {
			boundValue = "CAST(" + boundValue + " AS Dynamic)"
		}
		bindings = append(bindings, "["+boundValue+"] AS "+bound)
		boundColumns = append(boundColumns, bound)

		field.valueSQL = bound
		if field.kind == fieldKindDynamic {
			field.dynamicTypeSQL = "dynamicType(" + bound + ")"
		}
		field.materializeForPredicate = false
		boundState.visible[name] = field
	}
	return boundState, bindings, boundColumns
}

// predicateMaterializedOutputState describes the durable public columns after
// a predicate fence. Predicate-only singleton and exact-numeric aliases belong
// to a separate compile state and must never leak into downstream commands.
func predicateMaterializedOutputState(
	state compileState,
	fields []string,
) compileState {
	outputState := cloneCompileState(state)
	for _, name := range fields {
		field := outputState.visible[name]
		field.valueSQL = quoteIdentifier(name)
		field.existsSQL = rewriteExistenceForProjection(field, name)
		if field.kind == fieldKindDynamic {
			field.dynamicTypeSQL = "dynamicType(" + quoteIdentifier(name) + ")"
		}
		field.materializeForPredicate = false
		outputState.visible[name] = field
	}
	outputState.privateColumns = livePrivateColumns(
		outputState.privateColumns,
		outputState.visible,
	)
	return outputState
}

// bindMaterializedPredicateFields builds a predicate state whose selected
// calculated values refer to singleton ARRAY JOIN aliases. The enclosing
// materialized CTE freezes the producer, while the alias dependency prevents
// ClickHouse from pushing the predicate back into that producer. Replacing the
// public columns with those identical bound values makes the fence durable for
// later filters, so only the fields consumed here clear their marker.
func bindMaterializedPredicateFields(
	state compileState,
	fields []string,
	stage int,
) (compileState, compileState, []string, []string, []string) {
	predicateState := cloneCompileState(state)
	outputState := cloneCompileState(state)
	excludedColumns := make([]string, 0, len(fields))
	replacements := make([]string, 0, len(fields))
	bindings := make([]string, 0, len(fields))
	for index, name := range fields {
		field := state.visible[name]
		public := quoteIdentifier(name)
		bound := quoteIdentifier(fmt.Sprintf("__os_filter_bound_%d_%d", stage, index+1))
		boundValue := field.valueSQL
		if field.kind == fieldKindDynamic {
			boundValue = "CAST(" + boundValue + " AS Dynamic)"
		}
		bindings = append(
			bindings,
			"["+boundValue+"] AS "+bound,
		)
		excludedColumns = append(excludedColumns, bound)
		replacements = append(replacements, bound+" AS "+public)

		predicateField := field
		predicateField.valueSQL = bound
		if predicateField.kind == fieldKindDynamic {
			predicateField.dynamicTypeSQL = "dynamicType(" + bound + ")"
		}
		predicateField.materializeForPredicate = false
		predicateState.visible[name] = predicateField

		outputField := field
		outputField.valueSQL = public
		if outputField.kind == fieldKindDynamic {
			outputField.dynamicTypeSQL = "dynamicType(" + public + ")"
		}
		outputField.materializeForPredicate = false
		outputState.visible[name] = outputField
	}
	return predicateState, outputState, excludedColumns, replacements, bindings
}

func fieldNeedsPredicateMaterialization(name string, state compileState) bool {
	field, ok := state.visible[name]
	return ok && field.materializeForPredicate
}

type compiledScalar struct {
	valueSQL                     string
	valueArgs                    []any
	exactNumericKeySQL           string
	dynamicNumericEligibleSQL    string
	maxStringBytes               uint64
	existsSQL                    string
	existsArgs                   []any
	textEligibleSQL              string
	dynamicDomain                dynamicScalarDomain
	numericIntegral              bool
	mvCountOneOrNull             bool
	mvSortedLexicographic        bool
	dynamicTypeSQL               string
	storedTypeSQL                string
	descendantSQL                string
	descendantArgs               []any
	storedPath                   storedPathAuthority
	storedMetadataPath           string
	relativeFieldNamesSQL        string
	relativeFieldTypesSQL        string
	fieldMetadataVersionSQL      string
	optionalMultivaluePresentSQL string
	semanticBytesSQL             string
	semanticBytesArgs            []any
	semanticBytesByUTF8Validity  bool
	textEligibleBySemanticBytes  bool
	stringOrBytes                bool
	stringOrBytesNullable        bool
	kind                         fieldKind
	numberType                   string
	literal                      *plan.Value
	alwaysNull                   bool
	comparisonAtomic             bool
	// ieeeComparison marks values produced by authored arithmetic. Comparisons
	// involving one of these values apply the contract's explicit NaN rules
	// instead of inheriting ClickHouse's ordered-NaN behavior.
	ieeeComparison          bool
	materializeForPredicate bool
	// requiresRuntimeValidation keeps a deliberate unsupported/resource guard
	// alive even when a later projection no longer publishes this assignment.
	// Extend seals the calculated column behind a materialized CTE and an
	// explicit ignore() consumer before continuing the pipeline.
	requiresRuntimeValidation bool
}

// bindCompiledScalarForComparison replaces the authored value and presence
// expressions with compiler-owned bindings. Comparison-derived SQL caches must
// be cleared together because each cache was computed from the authored value;
// semantic metadata such as kind, literal, Dynamic domain, and IEEE behavior is
// deliberately retained for comparison dispatch.
func bindCompiledScalarForComparison(
	value compiledScalar,
	valueSQL string,
	existsSQL string,
) compiledScalar {
	value.valueSQL = valueSQL
	value.valueArgs = nil
	value.existsSQL = existsSQL
	value.existsArgs = nil
	value.dynamicTypeSQL = ""
	value.exactNumericKeySQL = ""
	value.dynamicNumericEligibleSQL = ""
	value.comparisonAtomic = true
	return value
}

func booleanScalarConsumerError(operation string) error {
	return fmt.Errorf(
		"compile ClickHouse %s: cannot consume a Boolean result",
		operation,
	)
}

func compiledScalarStringByteBound(value compiledScalar) uint64 {
	if value.maxStringBytes != 0 {
		return value.maxStringBytes
	}
	switch value.kind {
	case fieldKindInvalid:
		return 1
	case fieldKindBool:
		return 5
	case fieldKindNumber:
		switch value.numberType {
		case "Int8":
			return 4
		case "Int16":
			return 6
		case "Int32":
			return 11
		case "Int64":
			return 20
		case "Int128":
			return 40
		case "Int256":
			return 78
		case "UInt8":
			return 3
		case "UInt16":
			return 5
		case "UInt32":
			return 10
		case "UInt64":
			return 20
		case "UInt128":
			return 39
		case "UInt256":
			return 78
		default:
			// Float and Decimal formatting remains well inside this bound for
			// every fixed numeric type accepted by the compiler.
			return 128
		}
	case fieldKindTime:
		return 64
	default:
		// Durable String and Dynamic event values fit within the ingestion hard
		// limit. Zero is reserved as "source-bounded", not "unbounded".
		return MaximumStoredScalarBytes
	}
}

func fieldStateStringByteBound(field fieldState) uint64 {
	return compiledScalarStringByteBound(compiledScalarFromField(field))
}

func maximumCompiledScalarStringByteBound(values ...compiledScalar) uint64 {
	maximum := uint64(0)
	for _, value := range values {
		maximum = max(maximum, compiledScalarStringByteBound(value))
	}
	return maximum
}

func saturatingStringByteProduct(value, factor uint64) uint64 {
	if factor != 0 && value > math.MaxUint64/factor {
		return math.MaxUint64
	}
	return value * factor
}

func saturatingStringByteSum(left, right uint64) uint64 {
	if left > math.MaxUint64-right {
		return math.MaxUint64
	}
	return left + right
}

func extendCompileState(
	state compileState,
	output plan.FieldRef,
	value compiledScalar,
	retainDirectSidecars bool,
) (compileState, error) {
	if err := validateKnowledgeFieldSidecars(
		value.relativeFieldNamesSQL,
		value.relativeFieldTypesSQL,
		value.fieldMetadataVersionSQL,
	); err != nil {
		return compileState{}, fmt.Errorf(
			"compile ClickHouse extend output %q: %w",
			output.Name,
			err,
		)
	}
	if !retainDirectSidecars {
		value.storedPath = storedPathAuthority{}
		value.storedMetadataPath = ""
		value.relativeFieldNamesSQL = ""
		value.relativeFieldTypesSQL = ""
		value.fieldMetadataVersionSQL = ""
	}
	next := state
	next.visible = make(map[string]fieldState, len(state.visible)+1)
	for name, field := range state.visible {
		field.storedPath = field.storedPath.clone()
		next.visible[name] = field
	}
	next.publicOrder = append([]string(nil), state.publicOrder...)
	next.blocked = cloneSet(state.blocked)
	next.blockedPrefixes = cloneSet(state.blockedPrefixes)
	next.dynamicFieldFilters = cloneCompiledDynamicFieldFilters(state.dynamicFieldFilters)
	if exposesRawFieldsPayload(state) && !output.Canonical {
		// A calculated dynamic-schema output can shadow an immutable member of
		// the public convenience object. Keep private source metadata available
		// to later SPL stages, but do not publish two contradictory values.
		dropRawFieldsPayload(&next)
	}
	delete(next.blocked, output.Name)
	if !slices.Contains(next.publicOrder, output.Name) {
		next.publicOrder = append(next.publicOrder, output.Name)
	}
	existsSQL := value.existsSQL
	if existsSQL == "" {
		if isNativeMultivalueKind(value.kind) {
			// A legacy values() result has no explicit state sidecar. Its empty
			// physical array remains the only available absence proof.
			existsSQL = "notEmpty(" + quoteIdentifier(output.Name) + ")"
		} else {
			existsSQL = "1"
		}
	} else if retainDirectSidecars && value.optionalMultivaluePresentSQL == "" &&
		isNativeMultivalueKind(value.kind) && strings.HasPrefix(existsSQL, "notEmpty(") {
		// Legacy values()/list() arrays have no independent state sidecar. A
		// direct eval copy makes the new public array authoritative, so rebind
		// the only available presence proof to that output instead of retaining
		// a dependency on the source column.
		existsSQL = "notEmpty(" + quoteIdentifier(output.Name) + ")"
		value.existsArgs = nil
	}
	textEligibleSQL := value.textEligibleSQL
	semanticBytesSQL := value.semanticBytesSQL
	if value.semanticBytesByUTF8Validity {
		textEligibleSQL = "isValidUTF8(" + quoteIdentifier(output.Name) + ")"
		semanticBytesSQL = "toUInt8(isNotNull(" + quoteIdentifier(output.Name) +
			") AND NOT isValidUTF8(assumeNotNull(" + quoteIdentifier(output.Name) + ")))"
	} else if value.textEligibleBySemanticBytes {
		textEligibleSQL = semanticBytesTextEligibilitySQL(
			quoteIdentifier(output.Name),
			semanticBytesSQL,
		)
	}
	field := fieldState{
		valueSQL:                     quoteIdentifier(output.Name),
		maxStringBytes:               value.maxStringBytes,
		textEligibleSQL:              textEligibleSQL,
		dynamicDomain:                value.dynamicDomain,
		numericIntegral:              value.numericIntegral,
		mvCountOneOrNull:             value.mvCountOneOrNull,
		mvSortedLexicographic:        value.mvSortedLexicographic,
		existsSQL:                    existsSQL,
		existsArgs:                   append([]any(nil), value.existsArgs...),
		descendantSQL:                value.descendantSQL,
		descendantArgs:               append([]any(nil), value.descendantArgs...),
		storedTypeSQL:                value.storedTypeSQL,
		storedMetadataPath:           value.storedMetadataPath,
		relativeFieldNamesSQL:        value.relativeFieldNamesSQL,
		relativeFieldTypesSQL:        value.relativeFieldTypesSQL,
		fieldMetadataVersionSQL:      value.fieldMetadataVersionSQL,
		optionalMultivaluePresentSQL: value.optionalMultivaluePresentSQL,
		semanticBytesSQL:             semanticBytesSQL,
		semanticBytesByUTF8Validity:  value.semanticBytesByUTF8Validity,
		textEligibleBySemanticBytes:  value.textEligibleBySemanticBytes,
		stringOrBytes:                value.stringOrBytes,
		stringOrBytesNullable:        value.stringOrBytesNullable,
		kind:                         value.kind,
		// An eval output named index is calculated data, not the physical scan
		// selector. It follows its expression type and ordinary comparison rules.
		caseSensitive:           false,
		numberType:              value.numberType,
		alwaysNull:              value.alwaysNull,
		ieeeComparison:          value.ieeeComparison,
		materializeForPredicate: value.materializeForPredicate,
	}
	if value.kind == fieldKindDynamic {
		field.dynamicTypeSQL = "dynamicType(" + field.valueSQL + ")"
	}
	next.visible[output.Name] = field
	next.privateColumns = livePrivateColumns(next.privateColumns, next.visible)
	if field.semanticBytesSQL != "" &&
		!slices.Contains(next.privateColumns, field.semanticBytesSQL) {
		next.privateColumns = append(next.privateColumns, field.semanticBytesSQL)
	}
	return next, nil
}

type compiledExtractCapture struct {
	planCapture             plan.ExtractCapture
	valueSQL                string
	existsColumn            string
	existsProjection        string
	typeColumn              string
	typeProjection          string
	textColumn              string
	textProjection          string
	semanticBytesColumn     string
	semanticBytesProjection string
	descendantColumn        string
	descendantProjection    string
	namesColumn             string
	namesProjection         string
	typesColumn             string
	typesProjection         string
	metadataColumn          string
	metadataProjection      string
}

func extractPrivateColumns(captures []compiledExtractCapture) []string {
	columns := make([]string, 0, len(captures)*8)
	for _, capture := range captures {
		columns = append(columns, capture.existsColumn, capture.typeColumn)
		if capture.textColumn != "" {
			columns = append(columns, capture.textColumn)
		}
		if capture.semanticBytesColumn != "" {
			columns = append(columns, capture.semanticBytesColumn)
		}
		if capture.descendantColumn != "" {
			columns = append(columns, capture.descendantColumn)
		}
		if capture.namesColumn != "" {
			columns = append(
				columns,
				capture.namesColumn,
				capture.typesColumn,
				capture.metadataColumn,
			)
		}
	}
	return columns
}

func compileExtract(
	relation compiledRelation,
	operator *plan.Extract,
	state compileState,
	stage int,
) (compiledRelation, compileState, []any, int, error) {
	validated, err := validateExtractOperator(operator)
	if err != nil {
		return compiledRelation{}, compileState{}, nil, 0, err
	}
	openEventSchema := state.eventRows && state.allowDynamic
	if openEventSchema && operator.Input.Name == "fields" {
		return compiledRelation{}, compileState{}, nil, 0, &plan.Diagnostic{
			Code:    "SPL_AMBIGUOUS_REX_FIELD",
			Message: "rex cannot read the event result's reserved fields payload without an exact upstream schema",
			Range:   operator.Range,
		}
	}

	inputSQL, eligibleSQL, inputArgs, err := compileExtractInput(operator.Input, state)
	if err != nil {
		return compiledRelation{}, compileState{}, nil, 0, err
	}
	eligibleAlias := quoteIdentifier(fmt.Sprintf("__os_rex_eligible_%d", stage))
	inputAlias := quoteIdentifier(fmt.Sprintf("__os_rex_input_%d", stage))
	groupsAlias := quoteIdentifier(fmt.Sprintf("__os_rex_groups_%d", stage))
	matchedAlias := quoteIdentifier(fmt.Sprintf("__os_rex_matched_%d", stage))
	capturedBytesAlias := quoteIdentifier(fmt.Sprintf("__os_rex_captured_bytes_%d", stage))
	innerAlias := quoteIdentifier(fmt.Sprintf("_stage_%d", stage))
	inputExpressions := []string{
		"toUInt8(ifNull(" + eligibleSQL + ", 0)) AS " + eligibleAlias,
		"if(" + eligibleAlias + " != 0, assumeNotNull(" + inputSQL + "), CAST('' AS String)) AS " + inputAlias,
	}
	inputFragment := "SELECT *, " + strings.Join(inputExpressions, ", ") + " FROM (" + relation.sql + ") AS " + innerAlias
	relation = relation.selectFrom(inputFragment, operator.Range)
	groupExpression := "if(" + eligibleAlias + " != 0, extractGroups(" + inputAlias +
		", ?), CAST([], 'Array(String)')) AS " + groupsAlias
	groupFragment := "SELECT *, " + groupExpression + " FROM (" + relation.sql + ") AS " +
		quoteIdentifier(fmt.Sprintf("_rex_input_%d", stage))
	relation = relation.selectFrom(groupFragment, operator.Range)
	capturedBytesExpression := "arraySum(value -> toUInt64(length(value)), " + groupsAlias + ")"
	if state.rexCapturedBytesSQL != "" {
		capturedBytesExpression = "toUInt64(" + state.rexCapturedBytesSQL + ") + " + capturedBytesExpression
	}
	bytesFragment := "SELECT *, " + capturedBytesExpression + " AS " + capturedBytesAlias +
		" FROM (" + relation.sql + ") AS " + quoteIdentifier(fmt.Sprintf("_rex_groups_%d", stage))
	relation = relation.selectFrom(bytesFragment, operator.Range)
	// The limit branch is guarded by its own condition rather than a constant.
	// A downstream expression that forces this column to materialize makes
	// ClickHouse evaluate both branches for every row, and a constant `1` would
	// then fail an otherwise successful search on rows that are inside the
	// limit.
	overLimit := capturedBytesAlias + " > toUInt64(" +
		strconv.FormatUint(MaximumRexCapturedBytesPerRow, 10) + ")"
	matchedExpression := "toUInt8(if(" + overLimit + ", " +
		"throwIf(toUInt8(" + overLimit + "), '" + RexCaptureLimitMarker + "') = 0, " +
		eligibleAlias + " != 0 AND notEmpty(" + groupsAlias + "))) AS " + matchedAlias
	// Keep the extraction and byte guard streaming. The pinned ClickHouse
	// integration test uses EXPLAIN actions=1 to prove that common-expression
	// elimination still executes extractGroups exactly once for all captures.
	matchedFragment := "SELECT *, " + matchedExpression + " FROM (" + relation.sql + ") AS " +
		quoteIdentifier(fmt.Sprintf("_rex_bytes_%d", stage))
	relation = relation.selectFrom(matchedFragment, operator.Range)
	// The groups SELECT appears textually before its nested input SELECT, so
	// the pattern placeholder precedes source-presence placeholders.
	innerArgs := append([]any{validated.Pattern}, inputArgs...)

	next := cloneCompileState(state)
	next.rexCapturedBytesSQL = capturedBytesAlias
	if exposesRawFieldsPayload(state) {
		// The stored convenience object cannot be rewritten cheaply per event.
		// Drop it once a rex output can shadow one of its immutable members.
		dropRawFieldsPayload(&next)
	}
	captures := make([]compiledExtractCapture, 0, len(operator.Captures))
	existenceArgs := make([]any, 0)
	typeArgs := make([]any, 0)
	descendantArgs := make([]any, 0)
	seenOutputs := make(map[string]struct{}, len(operator.Captures))
	for index, capture := range operator.Captures {
		if openEventSchema && capture.Output.Name == "fields" {
			return compiledRelation{}, compileState{}, nil, 0, &plan.Diagnostic{
				Code:    "SPL_AMBIGUOUS_REX_FIELD",
				Message: "rex cannot replace the event result's reserved fields payload without an exact upstream schema",
				Range:   operator.Range,
			}
		}
		if _, duplicate := seenOutputs[capture.Output.Name]; duplicate {
			return compiledRelation{}, compileState{}, nil, 0, errors.New("compile ClickHouse extract: output field is repeated")
		}
		seenOutputs[capture.Output.Name] = struct{}{}
		previous, previousKnown, resolveErr := resolveCompiledField(capture.Output, state)
		if resolveErr != nil {
			return compiledRelation{}, compileState{}, nil, 0, resolveErr
		}
		if err := validateKnowledgeFieldSidecars(
			previous.relativeFieldNamesSQL,
			previous.relativeFieldTypesSQL,
			previous.fieldMetadataVersionSQL,
		); err != nil {
			return compiledRelation{}, compileState{}, nil, 0, fmt.Errorf(
				"compile ClickHouse extract prior output %q: %w",
				capture.Output.Name,
				err,
			)
		}

		capturedValue := "arrayElement(" + groupsAlias + ", " + strconv.Itoa(int(capture.Group)) + ")"
		valueSQL := ""
		kind := fieldKindDynamic
		switch {
		case !previousKnown:
			kind = fieldKindString
			valueSQL = "if(" + matchedAlias + " != 0, " + capturedValue + ", CAST(NULL AS Nullable(String)))"
		case previous.kind == fieldKindString:
			kind = fieldKindString
			valueSQL = "if(" + matchedAlias + " != 0, " + capturedValue + ", " + previous.valueSQL + ")"
		default:
			valueSQL = "if(" + matchedAlias + " != 0, CAST(" + capturedValue + " AS Dynamic), CAST(" + previous.valueSQL + " AS Dynamic))"
		}

		previousExists := "0"
		if previousKnown {
			previousExists = previous.existsSQL
			if previousExists == "" {
				previousExists = "1"
			}
			existenceArgs = append(existenceArgs, previous.existsArgs...)
			if previous.kind == fieldKindDynamic && previous.descendantSQL != "" {
				previousExists = "((" + previousExists + ") OR (" + previous.descendantSQL + "))"
				existenceArgs = append(existenceArgs, previous.descendantArgs...)
			}
		}
		existsName := fmt.Sprintf("__os_rex_exists_%d_%d", stage, index)
		existsAlias := quoteIdentifier(existsName)
		existsSQL := "toUInt8(if(" + matchedAlias + " != 0, 1, ifNull(" + previousExists + ", 0))) AS " + existsAlias

		previousTypeSQL := "toUInt8(0)"
		if previousKnown {
			var previousTypeArgs []any
			previousTypeSQL, previousTypeArgs, resolveErr = knownFieldStoredTypeSQL(previous)
			if resolveErr != nil {
				return compiledRelation{}, compileState{}, nil, 0, fmt.Errorf(
					"compile ClickHouse extract: resolve prior type for %q: %w",
					capture.Output.Name,
					resolveErr,
				)
			}
			typeArgs = append(typeArgs, previousTypeArgs...)
		}
		typeName := fmt.Sprintf("__os_rex_type_%d_%d", stage, index)
		typeAlias := quoteIdentifier(typeName)
		typeSQL := "toUInt8(if(" + matchedAlias + " != 0, toUInt8(" +
			strconv.Itoa(int(eventfields.StoredValueTypeString)) + "), " +
			previousTypeSQL + ")) AS " + typeAlias
		textColumn := ""
		textProjection := ""
		if previousKnown && previous.textEligibleSQL != "" {
			textColumn = quoteIdentifier(fmt.Sprintf("__os_rex_text_eligible_%d_%d", stage, index))
			textProjection = "toUInt8(if(" + matchedAlias + " != 0, 1, ifNull(" +
				previous.textEligibleSQL + ", 0))) AS " + textColumn
		}
		semanticBytesColumn := ""
		semanticBytesProjection := ""
		if previousKnown && previous.kind == fieldKindString && previous.stringOrBytes {
			if previous.semanticBytesSQL == "" {
				return compiledRelation{}, compileState{}, nil, 0, errors.New(
					"compile ClickHouse extract: prior String-or-Bytes field lacks semantic Bytes provenance",
				)
			}
			semanticBytesColumn = quoteIdentifier(fmt.Sprintf(
				"__os_rex_semantic_bytes_%d_%d", stage, index,
			))
			semanticBytesProjection = "toUInt8(if(" + matchedAlias +
				" != 0, 0, ifNull(" + previous.semanticBytesSQL +
				", 0))) AS " + semanticBytesColumn
		}
		descendantColumn := ""
		descendantProjection := ""
		if previousKnown && previous.descendantSQL != "" {
			descendantColumn = quoteIdentifier(fmt.Sprintf("__os_rex_descendant_%d_%d", stage, index))
			descendantProjection = "toUInt8(" + matchedAlias + " = 0 AND (" +
				previous.descendantSQL + ")) AS " + descendantColumn
			descendantArgs = append(descendantArgs, previous.descendantArgs...)
		}
		namesColumn := ""
		namesProjection := ""
		typesColumn := ""
		typesProjection := ""
		metadataColumn := ""
		metadataProjection := ""
		if previousKnown && previous.relativeFieldNamesSQL != "" {
			namesColumn = quoteIdentifier(fmt.Sprintf("__os_rex_names_%d_%d", stage, index))
			typesColumn = quoteIdentifier(fmt.Sprintf("__os_rex_types_%d_%d", stage, index))
			metadataColumn = quoteIdentifier(fmt.Sprintf(
				"__os_rex_metadata_version_%d_%d",
				stage,
				index,
			))
			namesProjection = "if(" + matchedAlias + " != 0, " +
				knowledgeEmptyRelativeFieldNamesSQL() + ", " +
				previous.relativeFieldNamesSQL + ") AS " + namesColumn
			typesProjection = "if(" + matchedAlias + " != 0, " +
				knowledgeEmptyRelativeFieldTypesSQL() + ", " +
				previous.relativeFieldTypesSQL + ") AS " + typesColumn
			metadataProjection = "toUInt8(if(" + matchedAlias +
				" != 0, 0, " + previous.fieldMetadataVersionSQL + ")) AS " +
				metadataColumn
		}
		captures = append(captures, compiledExtractCapture{
			planCapture:             capture,
			valueSQL:                valueSQL,
			existsColumn:            existsAlias,
			existsProjection:        existsSQL,
			typeColumn:              typeAlias,
			typeProjection:          typeSQL,
			textColumn:              textColumn,
			textProjection:          textProjection,
			semanticBytesColumn:     semanticBytesColumn,
			semanticBytesProjection: semanticBytesProjection,
			descendantColumn:        descendantColumn,
			descendantProjection:    descendantProjection,
			namesColumn:             namesColumn,
			namesProjection:         namesProjection,
			typesColumn:             typesColumn,
			typesProjection:         typesProjection,
			metadataColumn:          metadataColumn,
			metadataProjection:      metadataProjection,
		})

		delete(next.blocked, capture.Output.Name)
		if !slices.Contains(next.publicOrder, capture.Output.Name) {
			next.publicOrder = append(next.publicOrder, capture.Output.Name)
		}
		output := quoteIdentifier(capture.Output.Name)
		maxStringBytes := uint64(MaximumRexCapturedBytesPerRow)
		if previousKnown {
			maxStringBytes = max(maxStringBytes, fieldStateStringByteBound(previous))
		}
		field := fieldState{
			valueSQL:                output,
			maxStringBytes:          maxStringBytes,
			textEligibleSQL:         textColumn,
			semanticBytesSQL:        semanticBytesColumn,
			stringOrBytes:           semanticBytesColumn != "",
			stringOrBytesNullable:   previous.stringOrBytesNullable,
			existsSQL:               existsAlias,
			storedTypeSQL:           typeAlias,
			descendantSQL:           descendantColumn,
			relativeFieldNamesSQL:   namesColumn,
			relativeFieldTypesSQL:   typesColumn,
			fieldMetadataVersionSQL: metadataColumn,
			kind:                    kind,
			// A capture named index is calculated data and never regains the
			// physical scan selector's case or authorization semantics.
			caseSensitive: false,
			materializeForPredicate: fieldNeedsPredicateMaterialization(
				operator.Input.Name,
				state,
			) || previous.materializeForPredicate,
		}
		if kind == fieldKindDynamic {
			field.dynamicTypeSQL = "dynamicType(" + output + ")"
		}
		next.visible[capture.Output.Name] = field
	}

	valueByName := make(map[string]string, len(captures))
	existenceExpressions := make([]string, 0, len(captures))
	typeExpressions := make([]string, 0, len(captures))
	textExpressions := make([]string, 0, len(captures))
	semanticBytesExpressions := make([]string, 0, len(captures))
	descendantExpressions := make([]string, 0, len(captures))
	namesExpressions := make([]string, 0, len(captures))
	typesExpressions := make([]string, 0, len(captures))
	metadataExpressions := make([]string, 0, len(captures))
	for _, capture := range captures {
		valueByName[capture.planCapture.Output.Name] = capture.valueSQL
		existenceExpressions = append(existenceExpressions, capture.existsProjection)
		typeExpressions = append(typeExpressions, capture.typeProjection)
		if capture.textProjection != "" {
			textExpressions = append(textExpressions, capture.textProjection)
		}
		if capture.semanticBytesProjection != "" {
			semanticBytesExpressions = append(
				semanticBytesExpressions,
				capture.semanticBytesProjection,
			)
		}
		if capture.descendantProjection != "" {
			descendantExpressions = append(descendantExpressions, capture.descendantProjection)
		}
		if capture.namesProjection != "" {
			namesExpressions = append(namesExpressions, capture.namesProjection)
			typesExpressions = append(typesExpressions, capture.typesProjection)
			metadataExpressions = append(metadataExpressions, capture.metadataProjection)
		}
	}
	liveOldPrivateColumns := livePrivateColumns(state.privateColumns, next.visible)
	next.privateColumns = append(
		append([]string(nil), liveOldPrivateColumns...),
		extractPrivateColumns(captures)...,
	)
	projection := make([]string, 0, len(next.visible)+len(captures)+8)
	for _, name := range orderedVisibleNames(next) {
		publicName := quoteIdentifier(name)
		if valueSQL, captured := valueByName[name]; captured {
			projection = append(projection, valueSQL+" AS "+publicName)
			continue
		}
		field, ok := state.visible[name]
		if !ok {
			return compiledRelation{}, compileState{}, nil, 0, fmt.Errorf("compile ClickHouse extract: field %q has no input value", name)
		}
		projection = appendVisibleFieldProjection(projection, field, publicName)
	}
	projectionState := next
	projectionState.privateColumns = liveOldPrivateColumns
	projection = appendPrivateEventProjection(projection, projectionState)
	projection = append(projection, existenceExpressions...)
	projection = append(projection, typeExpressions...)
	projection = append(projection, textExpressions...)
	projection = append(projection, semanticBytesExpressions...)
	projection = append(projection, descendantExpressions...)
	projection = append(projection, namesExpressions...)
	projection = append(projection, typesExpressions...)
	projection = append(projection, metadataExpressions...)
	outerAlias := quoteIdentifier(fmt.Sprintf("_stage_%d", stage+1))
	outputFragment := "SELECT " + strings.Join(projection, ", ") + " FROM (" + relation.sql + ") AS " + outerAlias
	relation = relation.selectFrom(outputFragment, operator.Range)

	prefixArgs := make([]any, 0, len(existenceArgs)+len(typeArgs)+len(descendantArgs)+len(innerArgs))
	prefixArgs = append(prefixArgs, existenceArgs...)
	prefixArgs = append(prefixArgs, typeArgs...)
	prefixArgs = append(prefixArgs, descendantArgs...)
	prefixArgs = append(prefixArgs, innerArgs...)
	return relation, next, prefixArgs, 1, nil
}

func validateExtractOperator(operator *plan.Extract) (splregex.ExtractionPattern, error) {
	if operator == nil || operator.Input.Name == "" || len(operator.Captures) == 0 ||
		!strings.HasPrefix(operator.Pattern, "(?-s)") {
		return splregex.ExtractionPattern{}, errors.New("compile ClickHouse extract: operator is invalid")
	}
	if err := validateCanonicalFieldRef("extract", "input", operator.Input); err != nil {
		return splregex.ExtractionPattern{}, err
	}
	withoutDefaultFlags := strings.TrimPrefix(operator.Pattern, "(?-s)")
	validated, err := splregex.CompileExtractionPattern(withoutDefaultFlags)
	if err != nil || validated.Pattern != operator.Pattern {
		if err == nil {
			err = errors.New("pattern is not in canonical form")
		}
		return splregex.ExtractionPattern{}, fmt.Errorf("compile ClickHouse extract: invalid pattern: %w", err)
	}
	if len(operator.Captures) != len(validated.Captures) {
		return splregex.ExtractionPattern{}, errors.New("compile ClickHouse extract: capture metadata does not match pattern")
	}
	seen := make(map[string]struct{}, len(operator.Captures))
	for index, capture := range operator.Captures {
		expected := validated.Captures[index]
		if capture.Output.Name == "" || capture.Group == 0 || int(capture.Group) != expected.Group ||
			capture.Output.Name != expected.Name {
			return splregex.ExtractionPattern{}, errors.New("compile ClickHouse extract: capture metadata does not match pattern")
		}
		if err := validateCanonicalFieldRef("extract", "output", capture.Output); err != nil {
			return splregex.ExtractionPattern{}, err
		}
		if _, duplicate := seen[capture.Output.Name]; duplicate {
			return splregex.ExtractionPattern{}, errors.New("compile ClickHouse extract: output field is repeated")
		}
		seen[capture.Output.Name] = struct{}{}
	}
	return validated, nil
}

func compileExtractInput(input plan.FieldRef, state compileState) (valueSQL, eligibleSQL string, args []any, err error) {
	field, ok, err := resolveCompiledField(input, state)
	if err != nil {
		return "", "", nil, err
	}
	if !ok {
		return "CAST(NULL AS Nullable(String))", "0", nil, nil
	}
	existsSQL := field.existsSQL
	if existsSQL == "" {
		existsSQL = "1"
	}
	switch field.kind {
	case fieldKindString:
		textEligible := ""
		if field.textEligibleSQL != "" {
			textEligible = "(" + field.textEligibleSQL + ") AND "
		}
		return field.valueSQL,
			"(" + existsSQL + " AND " + textEligible + "isNotNull(" + field.valueSQL + ") AND isValidUTF8(" + field.valueSQL + "))",
			append([]any(nil), field.existsArgs...),
			nil
	case fieldKindDynamic:
		value := "dynamicElement(" + field.valueSQL + ", 'String')"
		typeSQL := field.dynamicTypeSQL
		if typeSQL == "" {
			typeSQL = "dynamicType(" + field.valueSQL + ")"
		}
		textEligible := ""
		if field.textEligibleSQL != "" {
			textEligible += " AND (" + field.textEligibleSQL + ")"
		}
		if field.storedTypeSQL != "" {
			textEligible += " AND " + field.storedTypeSQL + " = toUInt8(" +
				strconv.Itoa(int(eventfields.StoredValueTypeString)) + ")"
		}
		return value,
			"(" + existsSQL + " AND " + typeSQL + " = 'String'" + textEligible +
				" AND isNotNull(" + field.valueSQL + ") AND isValidUTF8(" + value + "))",
			append([]any(nil), field.existsArgs...),
			nil
	case fieldKindStringArray, fieldKindDynamicArray:
		return "", "", nil, unsupportedMultivalueUsage("field extraction", input.Range)
	default:
		// Field extraction does not stringify numeric, Boolean, time,
		// multivalue, or container sources. They behave exactly like a non-match.
		return "CAST(NULL AS Nullable(String))", "0", nil, nil
	}
}

const (
	// The first alternative consumes one complete JSON string, including
	// escapes, before the number alternative can observe digits inside it. The
	// third alternative groups all bytes that cannot begin either token, and
	// the dot fallback makes extraction lossless even for malformed input. The
	// rebuilt document is still parsed structurally, so tokenization never turns
	// malformed JSON into a successful match.
	spathJSONNumberBodyPattern  = `-?(?:0|[1-9][0-9]*)(?:[.][0-9]+)?(?:[eE][+-]?[0-9]+)?`
	spathJSONTokenPattern       = `(?s)("(?:\\.|[^"\\])*"|` + spathJSONNumberBodyPattern + `|[^"0-9-]+|.)`
	spathJSONNumberPattern      = `^` + spathJSONNumberBodyPattern + `$`
	spathJSONNumberMarkerPrefix = "__open_splunk_number_v1_"
	spathJSONNumberMarkerSuffix = "__"
)

func spathJSONNumberDynamicSQL(valueSQL string) string {
	raw := "__os_spath_number_raw"
	signed := "__os_spath_number_signed"
	unsigned := "__os_spath_number_unsigned"
	floating := "__os_spath_number_float"
	integerSyntax := "position(" + raw + ", '.') = 0 AND positionCaseInsensitive(" +
		raw + ", 'e') = 0"
	decimal := decimalEnvelopePayloadDynamicSQL(spathJSONCanonicalDecimalTextSQL(raw))
	integerBody := "multiIf(" +
		integerSyntax + " AND isNotNull(" + signed + "), CAST(assumeNotNull(" + signed + ") AS Dynamic), " +
		integerSyntax + " AND NOT startsWith(" + raw + ", '-') AND isNotNull(" + unsigned +
		"), CAST(assumeNotNull(" + unsigned + ") AS Dynamic), " + decimal + ")"
	integerBody = bindSQLExpressions(
		[]string{signed, unsigned},
		[]string{
			"accurateCastOrNull(" + raw + ", 'Int64')",
			"accurateCastOrNull(" + raw + ", 'UInt64')",
		},
		integerBody,
	)
	floatBody := "if(" + spathJSONExactFloatSQL(raw, floating) +
		", CAST(assumeNotNull(" + floating + ") AS Dynamic), " + decimal + ")"
	floatBody = bindSQLExpressions(
		[]string{floating},
		[]string{finiteFloatOrNullSQL(spathJSONPlainDecimalSQL(raw))},
		floatBody,
	)
	body := "if(" + integerSyntax + ", " + integerBody + ", " + floatBody + ")"
	return bindSQLExpressions([]string{raw}, []string{valueSQL}, body)
}

// spathJSONPlainDecimalSQL expands one bounded JSON exponent spelling before
// Float64 parsing. The pinned server can round a direct spelling such as
// 9.7e2 one ULP low; its parser is correctly rounded for the minimal plain
// decimal produced here. Ineligible or expansion-heavy inputs return empty and
// remain decimal/v1 values.
func spathJSONPlainDecimalSQL(raw string) string {
	lowered := "__os_spath_plain_lowered"
	negative := "__os_spath_plain_negative"
	bodyVariable := "__os_spath_plain_body"
	exponentPosition := "__os_spath_plain_exponent_position"
	significand := "__os_spath_plain_significand"
	exponentText := "__os_spath_plain_exponent_text"
	exponentEligible := "__os_spath_plain_exponent_eligible"
	exponentValue := "__os_spath_plain_exponent_value"
	dotPosition := "__os_spath_plain_dot_position"
	integerDigits := "__os_spath_plain_integer_digits"
	digits := "__os_spath_plain_digits"
	significant := "__os_spath_plain_significant"
	coefficient := "__os_spath_plain_coefficient"
	leadingZeros := "__os_spath_plain_leading_zeros"
	decimalPosition := "__os_spath_plain_decimal_position"
	eligible := "__os_spath_plain_eligible"

	sign := "if(" + negative + " != 0, CAST('-' AS String), CAST('' AS String))"
	plain := "multiIf(empty(" + significant + "), concat(" + sign + ", '0'), " +
		decimalPosition + " <= 0, concat(" + sign + ", '0.', repeat('0', toUInt64(-(" +
		decimalPosition + "))), " + coefficient + "), " +
		decimalPosition + " >= length(" + coefficient + "), concat(" + sign + ", " +
		coefficient + ", repeat('0', toUInt64(" + decimalPosition + " - length(" +
		coefficient + ")))), concat(" + sign + ", substring(" + coefficient +
		", 1, " + decimalPosition + "), '.', substring(" + coefficient + ", " +
		decimalPosition + " + 1)))"
	result := "if(" + eligible + " != 0, " + plain + ", CAST('' AS String))"
	eligibility := "toUInt8(length(" + raw + ") <= " +
		strconv.Itoa(jsonnumber.MaximumFloat64TextBytes) + " AND " + exponentEligible +
		" != 0 AND (empty(" + significant + ") OR (" + decimalPosition +
		" >= -" + strconv.Itoa(jsonnumber.MaximumFloat64DecimalScale) + " AND " +
		decimalPosition + " <= " + strconv.Itoa(jsonnumber.MaximumFloat64DecimalScale) + ")))"
	result = bindSQLExpressions([]string{eligible}, []string{eligibility}, result)
	positionSQL := integerDigits + " + toInt64(" + exponentValue +
		") - toInt64(" + leadingZeros + ")"
	result = bindSQLExpressions(
		[]string{decimalPosition},
		[]string{positionSQL},
		result,
	)
	result = bindSQLExpressions(
		[]string{coefficient, leadingZeros},
		[]string{
			"replaceRegexpOne(" + significant + ", '0+$', '')",
			"length(" + digits + ") - length(" + significant + ")",
		},
		result,
	)
	result = bindSQLExpressions(
		[]string{significant},
		[]string{"replaceRegexpOne(" + digits + ", '^0+', '')"},
		result,
	)
	result = bindSQLExpressions(
		[]string{integerDigits, digits},
		[]string{
			"if(" + dotPosition + " = 0, toInt64(length(" + significand +
				")), toInt64(" + dotPosition + ") - 1)",
			"replaceAll(" + significand + ", '.', '')",
		},
		result,
	)
	result = bindSQLExpressions(
		[]string{dotPosition},
		[]string{"position(" + significand + ", '.')"},
		result,
	)
	exponentParseInput := "if(" + exponentEligible + " != 0, " + exponentText +
		", CAST('0' AS String))"
	result = bindSQLExpressions(
		[]string{exponentValue},
		[]string{"toInt64OrZero(" + exponentParseInput + ")"},
		result,
	)
	result = bindSQLExpressions(
		[]string{exponentEligible},
		[]string{"toUInt8(" + spathJSONExponentEligibleSQL(raw) + ")"},
		result,
	)
	result = bindSQLExpressions(
		[]string{significand, exponentText},
		[]string{
			"if(" + exponentPosition + " = 0, " + bodyVariable + ", substring(" +
				bodyVariable + ", 1, " + exponentPosition + " - 1))",
			"if(" + exponentPosition + " = 0, CAST('0' AS String), substring(" +
				bodyVariable + ", " + exponentPosition + " + 1))",
		},
		result,
	)
	result = bindSQLExpressions(
		[]string{exponentPosition},
		[]string{"position(" + bodyVariable + ", 'e')"},
		result,
	)
	result = bindSQLExpressions(
		[]string{negative, bodyVariable},
		[]string{
			"toUInt8(startsWith(" + lowered + ", '-'))",
			"if(startsWith(" + lowered + ", '-'), substring(" + lowered + ", 2), " +
				lowered + ")",
		},
		result,
	)
	return bindSQLExpressions([]string{lowered}, []string{"lower(" + raw + ")"}, result)
}

func spathJSONExactFloatSQL(raw, floating string) string {
	bitsVariable := "__os_spath_float_bits"
	magnitudeVariable := "__os_spath_float_magnitude"
	exponentVariable := "__os_spath_float_exponent"
	fractionVariable := "__os_spath_float_fraction"
	significandVariable := "__os_spath_float_significand"
	trailingVariable := "__os_spath_float_trailing"
	binaryExponentVariable := "__os_spath_float_binary_exponent"
	scaleVariable := "__os_spath_float_scale"
	eligibleVariable := "__os_spath_float_eligible"
	formattedVariable := "__os_spath_float_formatted"
	rawKeyVariable := "__os_spath_float_raw_key"
	formattedKeyVariable := "__os_spath_float_formatted_key"

	result := eligibleVariable + " != 0 AND " + rawKeyVariable + " = " + formattedKeyVariable
	result = bindSQLExpressions(
		[]string{rawKeyVariable, formattedKeyVariable},
		[]string{
			exactNumericOrderingKeySQL(raw),
			exactNumericOrderingKeySQL(formattedVariable),
		},
		result,
	)
	result = bindSQLExpressions(
		[]string{formattedVariable},
		[]string{"if(" + eligibleVariable + " != 0, toDecimalString(assumeNotNull(" +
			floating + "), " + strconv.Itoa(jsonnumber.MaximumFloat64DecimalScale) +
			"), CAST('0' AS String))"},
		result,
	)
	// Plain-decimal construction already enforces the raw byte, exponent,
	// expansion, finite-value, and magnitude bounds before floating is non-null.
	eligible := "toUInt8(isNotNull(" + floating + ") AND " + scaleVariable +
		" <= " + strconv.Itoa(jsonnumber.MaximumFloat64DecimalScale) + ")"
	result = bindSQLExpressions(
		[]string{eligibleVariable},
		[]string{eligible},
		result,
	)
	scale := "toInt16(if(" + magnitudeVariable + " = 0, 0, greatest(0, -(" +
		binaryExponentVariable + " + " + trailingVariable + "))))"
	result = bindSQLExpressions([]string{scaleVariable}, []string{scale}, result)
	binaryExponent := "toInt16(if(" + exponentVariable + " = 0, -1074, toInt16(" +
		exponentVariable + ") - 1075))"
	trailing := "toInt16(if(" + significandVariable + " = 0, 0, toInt16(bitCount(bitXor(" +
		significandVariable + ", " + significandVariable + " - toUInt64(1)))) - 1))"
	result = bindSQLExpressions(
		[]string{binaryExponentVariable, trailingVariable},
		[]string{binaryExponent, trailing},
		result,
	)
	significand := "if(" + exponentVariable + " = 0, " + fractionVariable +
		", bitOr(" + fractionVariable + ", toUInt64(4503599627370496)))"
	result = bindSQLExpressions(
		[]string{significandVariable},
		[]string{significand},
		result,
	)
	result = bindSQLExpressions(
		[]string{exponentVariable, fractionVariable},
		[]string{
			"bitAnd(bitShiftRight(" + magnitudeVariable + ", 52), toUInt64(2047))",
			"bitAnd(" + magnitudeVariable + ", toUInt64(4503599627370495))",
		},
		result,
	)
	result = bindSQLExpressions(
		[]string{magnitudeVariable},
		[]string{"bitAnd(" + bitsVariable + ", toUInt64('9223372036854775807'))"},
		result,
	)
	return bindSQLExpressions(
		[]string{bitsVariable},
		[]string{"reinterpretAsUInt64(ifNull(" + floating + ", toFloat64(0)))"},
		result,
	)
}

func spathJSONExponentEligibleSQL(raw string) string {
	lowered := "__os_spath_exponent_lowered"
	positionVariable := "__os_spath_exponent_position"
	textVariable := "__os_spath_exponent_text"
	digitsVariable := "__os_spath_exponent_digits"
	trimmedVariable := "__os_spath_exponent_trimmed"
	maximum := strconv.Itoa(jsonnumber.MaximumExponentMagnitude)
	maximumDigits := strconv.Itoa(len(maximum))
	result := positionVariable + " = 0 OR empty(" + trimmedVariable + ") OR length(" +
		trimmedVariable + ") < " + maximumDigits + " OR (length(" + trimmedVariable +
		") = " + maximumDigits + " AND " +
		trimmedVariable + " <= '" + maximum + "')"
	result = bindSQLExpressions(
		[]string{trimmedVariable},
		[]string{"replaceRegexpOne(" + digitsVariable + ", '^0+', '')"},
		result,
	)
	result = bindSQLExpressions(
		[]string{digitsVariable},
		[]string{"if(startsWith(" + textVariable + ", '+') OR startsWith(" + textVariable +
			", '-'), substring(" + textVariable + ", 2), " + textVariable + ")"},
		result,
	)
	result = bindSQLExpressions(
		[]string{textVariable},
		[]string{"if(" + positionVariable + " = 0, CAST('0' AS String), substring(" +
			lowered + ", " + positionVariable + " + 1))"},
		result,
	)
	result = bindSQLExpressions(
		[]string{positionVariable},
		[]string{"position(" + lowered + ", 'e')"},
		result,
	)
	return bindSQLExpressions([]string{lowered}, []string{"lower(" + raw + ")"}, result)
}

func spathJSONCanonicalDecimalTextSQL(raw string) string {
	lowered := "__os_spath_decimal_lowered"
	positionVariable := "__os_spath_decimal_exponent_position"
	mantissaVariable := "__os_spath_decimal_mantissa"
	exponentVariable := "__os_spath_decimal_exponent"
	digitsVariable := "__os_spath_decimal_exponent_digits"
	trimmedVariable := "__os_spath_decimal_exponent_trimmed"
	canonicalExponent := "if(empty(" + trimmedVariable + "), CAST('0' AS String), concat(" +
		"if(startsWith(" + exponentVariable + ", '-'), '-', ''), " + trimmedVariable + "))"
	result := "if(" + positionVariable + " = 0, " + lowered + ", concat(" +
		mantissaVariable + ", 'e', " + canonicalExponent + "))"
	result = bindSQLExpressions(
		[]string{trimmedVariable},
		[]string{"replaceRegexpOne(" + digitsVariable + ", '^0+', '')"},
		result,
	)
	result = bindSQLExpressions(
		[]string{digitsVariable},
		[]string{"if(startsWith(" + exponentVariable + ", '+') OR startsWith(" +
			exponentVariable + ", '-'), substring(" + exponentVariable + ", 2), " +
			exponentVariable + ")"},
		result,
	)
	result = bindSQLExpressions(
		[]string{mantissaVariable, exponentVariable},
		[]string{
			"if(" + positionVariable + " = 0, " + lowered + ", substring(" + lowered +
				", 1, " + positionVariable + " - 1))",
			"if(" + positionVariable + " = 0, CAST('0' AS String), substring(" + lowered +
				", " + positionVariable + " + 1))",
		},
		result,
	)
	result = bindSQLExpressions(
		[]string{positionVariable},
		[]string{"position(" + lowered + ", 'e')"},
		result,
	)
	return bindSQLExpressions([]string{lowered}, []string{"lower(" + raw + ")"}, result)
}

func compileExtractJSON(
	relation compiledRelation,
	operator *plan.ExtractJSON,
	state compileState,
	stage int,
) (compiledRelation, compileState, []any, int, error) {
	steps, err := validateExtractJSONOperator(operator)
	if err != nil {
		return compiledRelation{}, compileState{}, nil, 0, err
	}
	if splpath.HasWildcard(steps) {
		return compileExtractJSONWildcard(relation, operator, steps, state, stage)
	}
	openEventSchema := state.eventRows && state.allowDynamic
	if openEventSchema && (operator.Input.Name == "fields" || operator.Output.Name == "fields") {
		return compiledRelation{}, compileState{}, nil, 0, &plan.Diagnostic{
			Code:    "SPL_AMBIGUOUS_SPATH_FIELD",
			Message: "spath cannot use the event result's reserved fields payload without an exact upstream schema",
			Range:   operator.Range,
		}
	}

	inputSQL, sourceEligibleSQL, inputArgs, err := compileExtractInput(operator.Input, state)
	if err != nil {
		return compiledRelation{}, compileState{}, nil, 0, err
	}
	inputField, inputKnown, err := resolveCompiledField(operator.Input, state)
	if err != nil {
		return compiledRelation{}, compileState{}, nil, 0, err
	}
	sourceMayExtract := inputKnown &&
		(inputField.kind == fieldKindString || inputField.kind == fieldKindDynamic)
	sourceEligibleAlias := quoteIdentifier(fmt.Sprintf("__os_spath_source_eligible_%d", stage))
	inputAlias := quoteIdentifier(fmt.Sprintf("__os_spath_input_%d", stage))
	eligibleAlias := quoteIdentifier(fmt.Sprintf("__os_spath_eligible_%d", stage))
	tokenCountAlias := quoteIdentifier(fmt.Sprintf("__os_spath_token_count_%d", stage))
	tokensAlias := quoteIdentifier(fmt.Sprintf("__os_spath_tokens_%d", stage))
	tokenGuardAlias := quoteIdentifier(fmt.Sprintf("__os_spath_token_guard_%d", stage))
	numberFlagsAlias := quoteIdentifier(fmt.Sprintf("__os_spath_number_flags_%d", stage))
	nulledJSONAlias := quoteIdentifier(fmt.Sprintf("__os_spath_nulled_json_%d", stage))
	pathEligibleAlias := quoteIdentifier(fmt.Sprintf("__os_spath_path_eligible_%d", stage))
	nullRawAlias := quoteIdentifier(fmt.Sprintf("__os_spath_null_raw_%d", stage))
	numberMarkerAlias := quoteIdentifier(fmt.Sprintf("__os_spath_number_marker_%d", stage))
	numberSelectedAlias := quoteIdentifier(fmt.Sprintf("__os_spath_number_selected_%d", stage))
	rawAlias := quoteIdentifier(fmt.Sprintf("__os_spath_raw_%d", stage))
	matchedAlias := quoteIdentifier(fmt.Sprintf("__os_spath_matched_%d", stage))
	valueAlias := quoteIdentifier(fmt.Sprintf("__os_spath_value_%d", stage))

	sourceExpressions := []string{
		"toUInt8(ifNull(" + sourceEligibleSQL + ", 0)) AS " + sourceEligibleAlias,
		"if(" + sourceEligibleAlias + " != 0, assumeNotNull(" + inputSQL +
			"), CAST('' AS String)) AS " + inputAlias,
	}
	sourceFragment := "SELECT *, " + strings.Join(sourceExpressions, ", ") +
		" FROM (" + relation.sql + ") AS " + quoteIdentifier(fmt.Sprintf("_stage_%d", stage))
	relation = relation.selectFrom(sourceFragment, operator.Range)

	overInputLimit := sourceEligibleAlias + " != 0 AND length(" + inputAlias + ") > " +
		strconv.Itoa(MaximumSpathInputBytes)
	boundedEligible := "toUInt8(if(" + overInputLimit + ", throwIf(toUInt8(" +
		overInputLimit + "), '" + SpathInputLimitMarker + "') = 0, " +
		sourceEligibleAlias + " != 0)) AS " + eligibleAlias
	needsTokenPreflight := eligibleAlias + " != 0 AND length(" + inputAlias + ") > " +
		strconv.Itoa(MaximumSpathJSONTokens)
	preflightInput := "if(" + needsTokenPreflight + ", " + inputAlias +
		", CAST('' AS String))"
	tokenCountExpression := "if(" + needsTokenPreflight + ", countMatches(" +
		preflightInput + ", ?), toUInt64(0)) AS " + tokenCountAlias
	preflightFragment := "SELECT *, " + boundedEligible + ", " + tokenCountExpression +
		" FROM (" + relation.sql + ") AS " + quoteIdentifier(fmt.Sprintf("_spath_source_%d", stage))
	relation = relation.selectFrom(preflightFragment, operator.Range)

	overTokenLimit := eligibleAlias + " != 0 AND " + tokenCountAlias + " > " +
		strconv.Itoa(MaximumSpathJSONTokens)
	tokenGuardExpression := "toUInt8(if(" + overTokenLimit + ", throwIf(toUInt8(" +
		overTokenLimit + "), '" + SpathJSONLexemeLimitMarker + "') = 0, " +
		eligibleAlias + " != 0)) AS " + tokenGuardAlias
	guardedTokenInput := "if(" + tokenGuardAlias + " != 0, " + inputAlias +
		", CAST('' AS String))"
	tokensExpression := "if(" + tokenGuardAlias + " != 0, extractAll(" + guardedTokenInput +
		", ?), CAST([], 'Array(String)')) AS " + tokensAlias
	tokensFragment := "SELECT *, " + tokenGuardExpression + ", " + tokensExpression +
		" FROM (" + relation.sql + ") AS " + quoteIdentifier(fmt.Sprintf("_spath_preflight_%d", stage))
	relation = relation.selectFrom(tokensFragment, operator.Range)

	numberFlagsExpression := "if(" + tokenGuardAlias + " != 0, arrayMap(token -> " +
		"toUInt8(match(token, ?)), " + tokensAlias + "), CAST([], 'Array(UInt8)')) AS " +
		numberFlagsAlias
	nulledJSONExpression := "if(" + tokenGuardAlias + " != 0, if(has(" + numberFlagsAlias +
		", toUInt8(1)), arrayStringConcat(arrayMap((token, flag) -> if(flag != 0, " +
		"CAST('null' AS String), token), " + tokensAlias + ", " + numberFlagsAlias +
		")), " + inputAlias + "), CAST('' AS String)) AS " + nulledJSONAlias
	shapeFragment := "SELECT *, " + strings.Join([]string{
		numberFlagsExpression,
		nulledJSONExpression,
	}, ", ") + " FROM (" + relation.sql + ") AS " +
		quoteIdentifier(fmt.Sprintf("_spath_token_guard_%d", stage))
	relation = relation.selectFrom(shapeFragment, operator.Range)

	arrayGuardSQL, arrayGuardArgs := spathArrayGuardSQL(nulledJSONAlias, steps)
	pathEligible := tokenGuardAlias + " != 0"
	if arrayGuardSQL != "" {
		pathEligible += " AND " + arrayGuardSQL
	}
	pathEligibleExpression := "toUInt8(" + pathEligible + ") AS " + pathEligibleAlias
	pathSQL, pathArgs := spathPathSQL(steps)
	nullRawExpression := "if(" + pathEligibleAlias + " != 0, JSONExtractRaw(" +
		nulledJSONAlias + ", " + pathSQL + "), CAST('' AS String)) AS " + nullRawAlias
	markedJSONSQL := "arrayStringConcat(arrayMap((token, flag, token_index) -> if(flag != 0, " +
		"concat(char(34), '" + spathJSONNumberMarkerPrefix + "', toString(token_index), '" +
		spathJSONNumberMarkerSuffix + "', char(34)), token), " + tokensAlias + ", " +
		numberFlagsAlias + ", arrayEnumerate(" + tokensAlias + ")))"
	numberMarkerExpression := "if(" + pathEligibleAlias + " != 0 AND " + nullRawAlias +
		" = 'null' AND has(" + numberFlagsAlias + ", toUInt8(1)), JSONExtractString(" +
		markedJSONSQL + ", " + pathSQL +
		"), CAST('' AS String)) AS " + numberMarkerAlias
	pathFragment := "SELECT *, " + strings.Join([]string{
		pathEligibleExpression,
		nullRawExpression,
		numberMarkerExpression,
	}, ", ") + " FROM (" + relation.sql + ") AS " +
		quoteIdentifier(fmt.Sprintf("_spath_shape_%d", stage))
	relation = relation.selectFrom(pathFragment, operator.Range)

	numberSelectedExpression := "toUInt8(startsWith(" + numberMarkerAlias + ", '" +
		spathJSONNumberMarkerPrefix + "') AND endsWith(" + numberMarkerAlias + ", '" +
		spathJSONNumberMarkerSuffix + "')) AS " + numberSelectedAlias
	numberFragment := "SELECT *, " + numberSelectedExpression + " FROM (" + relation.sql +
		") AS " + quoteIdentifier(fmt.Sprintf("_spath_path_%d", stage))
	relation = relation.selectFrom(numberFragment, operator.Range)

	markerIndex := "toUInt64OrZero(substring(" + numberMarkerAlias + ", " +
		strconv.Itoa(len(spathJSONNumberMarkerPrefix)+1) + ", length(" + numberMarkerAlias +
		") - " + strconv.Itoa(len(spathJSONNumberMarkerPrefix)+len(spathJSONNumberMarkerSuffix)) + "))"
	rawExpression := "if(" + numberSelectedAlias + " != 0, arrayElement(" +
		tokensAlias + ", " + markerIndex + "), " + nullRawAlias + ") AS " + rawAlias
	rawFragment := "SELECT *, " + rawExpression + " FROM (" + relation.sql +
		") AS " + quoteIdentifier(fmt.Sprintf("_spath_number_%d", stage))
	relation = relation.selectFrom(rawFragment, operator.Range)

	supportedType := numberSelectedAlias + " != 0 OR " + rawAlias +
		" IN ('null', 'true', 'false') OR startsWith(" + rawAlias + ", char(34))"
	unsupportedRaw := "notEmpty(" + rawAlias + ") AND NOT (" + supportedType + ")"
	matchedExpression := "toUInt8(if(" + unsupportedRaw + ", throwIf(toUInt8(" +
		unsupportedRaw + "), '" + UnsupportedSpathValueMarker + "') = 0, notEmpty(" +
		rawAlias + "))) AS " + matchedAlias
	valueExpression := "if(" + matchedAlias + " != 0, if(" + numberSelectedAlias +
		" != 0, " + spathJSONNumberDynamicSQL(rawAlias) + ", JSONExtract(" + rawAlias +
		", 'Dynamic')), CAST(NULL AS Dynamic)) AS " + valueAlias
	previous, previousKnown, err := resolveCompiledField(operator.Output, state)
	if err != nil {
		return compiledRelation{}, compileState{}, nil, 0, err
	}
	if err := validateKnowledgeFieldSidecars(
		previous.relativeFieldNamesSQL,
		previous.relativeFieldTypesSQL,
		previous.fieldMetadataVersionSQL,
	); err != nil {
		return compiledRelation{}, compileState{}, nil, 0, fmt.Errorf(
			"compile ClickHouse spath prior output %q: %w",
			operator.Output.Name,
			err,
		)
	}
	previousValue := "CAST(NULL AS Dynamic)"
	previousExists := "0"
	var existenceArgs, typeArgs []any
	previousTypeSQL := "toUInt8(0)"
	if previousKnown {
		previousValue = "CAST(" + previous.valueSQL + " AS Dynamic)"
		if previous.kind == fieldKindString && previous.stringOrBytes {
			if previous.semanticBytesSQL == "" {
				return compiledRelation{}, compileState{}, nil, 0, errors.New(
					"compile ClickHouse spath: prior String-or-Bytes field lacks semantic Bytes provenance",
				)
			}
			previousValue = "if(ifNull(" + previous.semanticBytesSQL +
				", 0), " + bytesEnvelopePayloadDynamicSQL(
				rawStdBase64EncodeSQL("assumeNotNull("+previous.valueSQL+")"),
			) + ", CAST(" + previous.valueSQL + " AS Dynamic))"
		}
		previousExists, existenceArgs = knownFieldPresenceSQL(previous)
		previousTypeSQL, typeArgs, err = knownFieldStoredTypeSQL(previous)
		if err != nil {
			return compiledRelation{}, compileState{}, nil, 0, fmt.Errorf(
				"compile ClickHouse spath: resolve prior type for %q: %w",
				operator.Output.Name,
				err,
			)
		}
	}

	existsAlias := quoteIdentifier(fmt.Sprintf("__os_spath_exists_%d", stage))
	typeAlias := quoteIdentifier(fmt.Sprintf("__os_spath_type_%d", stage))
	existsProjection := "toUInt8(if(" + matchedAlias + " != 0, 1, ifNull(" +
		previousExists + ", 0))) AS " + existsAlias
	dynamicType := "dynamicType(" + valueAlias + ")"
	selectedType := "multiIf(" +
		dynamicType + " = 'None', toUInt8(" + strconv.Itoa(int(eventfields.StoredValueTypeNull)) + "), " +
		dynamicType + " = 'String', toUInt8(" + strconv.Itoa(int(eventfields.StoredValueTypeString)) + "), " +
		"startsWith(" + dynamicType + ", 'Int'), toUInt8(" + strconv.Itoa(int(eventfields.StoredValueTypeSint64)) + "), " +
		"startsWith(" + dynamicType + ", 'UInt'), toUInt8(" + strconv.Itoa(int(eventfields.StoredValueTypeUint64)) + "), " +
		dynamicType + " = 'Float64', toUInt8(" + strconv.Itoa(int(eventfields.StoredValueTypeDouble)) + "), " +
		dynamicType + " = 'Bool', toUInt8(" + strconv.Itoa(int(eventfields.StoredValueTypeBool)) + "), " +
		numberSelectedAlias + " != 0 AND " + dynamicType + " = 'Map(String, String)', toUInt8(" + strconv.Itoa(int(eventfields.StoredValueTypeDecimal)) + "), " +
		"toUInt8(0))"
	typeProjection := "toUInt8(if(" + matchedAlias + " != 0, " + selectedType +
		", " + previousTypeSQL + ")) AS " + typeAlias
	textEligibleAlias := ""
	textEligibleProjection := ""
	if previousKnown && previous.textEligibleSQL != "" {
		textEligibleAlias = quoteIdentifier(fmt.Sprintf("__os_spath_text_eligible_%d", stage))
		textEligibleProjection = "toUInt8(if(" + matchedAlias + " != 0, 1, ifNull(" +
			previous.textEligibleSQL + ", 0))) AS " + textEligibleAlias
	}
	descendantAlias := ""
	descendantProjection := ""
	var descendantArgs []any
	if previousKnown && previous.descendantSQL != "" {
		descendantAlias = quoteIdentifier(fmt.Sprintf("__os_spath_descendant_%d", stage))
		descendantProjection = "toUInt8(" + matchedAlias + " = 0 AND (" +
			previous.descendantSQL + ")) AS " + descendantAlias
		descendantArgs = append(descendantArgs, previous.descendantArgs...)
	}
	namesAlias := ""
	namesProjection := ""
	typesAlias := ""
	typesProjection := ""
	metadataAlias := ""
	metadataProjection := ""
	if previousKnown && previous.relativeFieldNamesSQL != "" {
		namesAlias = quoteIdentifier(fmt.Sprintf("__os_spath_names_%d", stage))
		typesAlias = quoteIdentifier(fmt.Sprintf("__os_spath_types_%d", stage))
		metadataAlias = quoteIdentifier(fmt.Sprintf(
			"__os_spath_metadata_version_%d",
			stage,
		))
		namesProjection = "if(" + matchedAlias + " != 0, " +
			knowledgeEmptyRelativeFieldNamesSQL() + ", " +
			previous.relativeFieldNamesSQL + ") AS " + namesAlias
		typesProjection = "if(" + matchedAlias + " != 0, " +
			knowledgeEmptyRelativeFieldTypesSQL() + ", " +
			previous.relativeFieldTypesSQL + ") AS " + typesAlias
		metadataProjection = "toUInt8(if(" + matchedAlias + " != 0, 0, " +
			previous.fieldMetadataVersionSQL + ")) AS " + metadataAlias
	}
	// Every projection above reads the prior value of the output field. The
	// output SELECT below redefines that field, and ClickHouse binds a sibling
	// read of the redefined name to the new alias rather than to the source
	// column, so `spath output=_raw` would resolve the prior-value reads to the
	// Dynamic result it is about to write. Emit them alongside the match, where
	// the field still holds its input value, and let the output SELECT carry
	// the aliases through. Sharing this stage keeps the operator's relational
	// depth unchanged.
	priorAlias := quoteIdentifier(fmt.Sprintf("__os_spath_prior_%d", stage))
	valueProjections := []string{
		matchedExpression,
		valueExpression,
		previousValue + " AS " + priorAlias,
		existsProjection,
		typeProjection,
	}
	if textEligibleProjection != "" {
		valueProjections = append(valueProjections, textEligibleProjection)
	}
	if descendantProjection != "" {
		valueProjections = append(valueProjections, descendantProjection)
	}
	if namesProjection != "" {
		valueProjections = append(
			valueProjections,
			namesProjection,
			typesProjection,
			metadataProjection,
		)
	}
	valueFragment := "SELECT *, " + strings.Join(valueProjections, ", ") +
		" FROM (" + relation.sql + ") AS " +
		quoteIdentifier(fmt.Sprintf("_spath_raw_%d", stage))
	relation = relation.selectFrom(valueFragment, operator.Range)

	outputValue := "if(" + matchedAlias + " != 0, " + valueAlias + ", " + priorAlias + ")"

	next := cloneCompileState(state)
	if sourceMayExtract && exposesRawFieldsPayload(state) {
		dropRawFieldsPayload(&next)
	}
	delete(next.blocked, operator.Output.Name)
	if !slices.Contains(next.publicOrder, operator.Output.Name) {
		next.publicOrder = append(next.publicOrder, operator.Output.Name)
	}
	outputName := quoteIdentifier(operator.Output.Name)
	maxStringBytes := uint64(MaximumSpathInputBytes)
	if previousKnown {
		maxStringBytes = max(maxStringBytes, fieldStateStringByteBound(previous))
	}
	next.visible[operator.Output.Name] = fieldState{
		valueSQL:                outputName,
		maxStringBytes:          maxStringBytes,
		textEligibleSQL:         textEligibleAlias,
		dynamicTypeSQL:          "dynamicType(" + outputName + ")",
		storedTypeSQL:           typeAlias,
		existsSQL:               existsAlias,
		descendantSQL:           descendantAlias,
		relativeFieldNamesSQL:   namesAlias,
		relativeFieldTypesSQL:   typesAlias,
		fieldMetadataVersionSQL: metadataAlias,
		kind:                    fieldKindDynamic,
		caseSensitive:           false,
		materializeForPredicate: sourceMayExtract || previous.materializeForPredicate,
	}

	liveOldPrivateColumns := livePrivateColumns(state.privateColumns, next.visible)
	next.privateColumns = append(append([]string(nil), liveOldPrivateColumns...), existsAlias, typeAlias)
	if textEligibleAlias != "" {
		next.privateColumns = append(next.privateColumns, textEligibleAlias)
	}
	if descendantAlias != "" {
		next.privateColumns = append(next.privateColumns, descendantAlias)
	}
	if namesAlias != "" {
		next.privateColumns = append(
			next.privateColumns,
			namesAlias,
			typesAlias,
			metadataAlias,
		)
	}
	projection := make([]string, 0, len(next.visible)+8)
	for _, name := range orderedVisibleNames(next) {
		publicName := quoteIdentifier(name)
		if name == operator.Output.Name {
			projection = append(projection, outputValue+" AS "+publicName)
			continue
		}
		field, ok := state.visible[name]
		if !ok {
			return compiledRelation{}, compileState{}, nil, 0, fmt.Errorf(
				"compile ClickHouse spath: field %q has no input value",
				name,
			)
		}
		projection = appendVisibleFieldProjection(projection, field, publicName)
	}
	projectionState := next
	projectionState.privateColumns = liveOldPrivateColumns
	projection = appendPrivateEventProjection(projection, projectionState)
	projection = append(projection, existsAlias, typeAlias)
	if textEligibleAlias != "" {
		projection = append(projection, textEligibleAlias)
	}
	if descendantAlias != "" {
		projection = append(projection, descendantAlias)
	}
	if namesAlias != "" {
		projection = append(
			projection,
			namesAlias,
			typesAlias,
			metadataAlias,
		)
	}
	outputFragment := "SELECT " + strings.Join(projection, ", ") + " FROM (" +
		relation.sql + ") AS " + quoteIdentifier(fmt.Sprintf("_stage_%d", stage+1))
	relation = relation.selectFrom(outputFragment, operator.Range)

	prefixArgs := make([]any, 0,
		len(existenceArgs)+len(typeArgs)+len(descendantArgs)+len(arrayGuardArgs)+
			2*len(pathArgs)+3+len(inputArgs),
	)
	prefixArgs = append(prefixArgs, existenceArgs...)
	prefixArgs = append(prefixArgs, typeArgs...)
	prefixArgs = append(prefixArgs, descendantArgs...)
	prefixArgs = append(prefixArgs, arrayGuardArgs...)
	prefixArgs = append(prefixArgs, pathArgs...)
	prefixArgs = append(prefixArgs, pathArgs...)
	prefixArgs = append(prefixArgs, spathJSONNumberPattern)
	prefixArgs = append(prefixArgs, spathJSONTokenPattern)
	prefixArgs = append(prefixArgs, spathJSONTokenPattern)
	prefixArgs = append(prefixArgs, inputArgs...)
	return relation, next, prefixArgs, 1, nil
}

func validateExtractJSONOperator(operator *plan.ExtractJSON) ([]splpath.Step, error) {
	if operator == nil || operator.Input.Name == "" || operator.Output.Name == "" || operator.Path == "" {
		return nil, errors.New("compile ClickHouse spath: operator is invalid")
	}
	if err := validateCanonicalFieldRef("spath", "input", operator.Input); err != nil {
		return nil, err
	}
	if err := validateCanonicalFieldRef("spath", "output", operator.Output); err != nil {
		return nil, err
	}
	steps, err := splpath.ParseJSON(operator.Path)
	if err != nil {
		return nil, fmt.Errorf("compile ClickHouse spath: invalid path: %w", err)
	}
	if !slices.Equal(steps, operator.Steps) {
		return nil, errors.New("compile ClickHouse spath: path metadata does not match source")
	}
	return steps, nil
}

func spathPathSQL(steps []splpath.Step) (string, []any) {
	placeholders := make([]string, 0, len(steps)*2)
	args := make([]any, 0, len(steps)*2)
	for _, step := range steps {
		placeholders = append(placeholders, "?")
		args = append(args, step.Key)
		if step.Selector == splpath.ArraySelectorFixed {
			placeholders = append(placeholders, "?")

			args = append(args, safecast.MustConv[int64](step.Index)+1)
		}
	}
	return strings.Join(placeholders, ", "), args
}

func spathArrayGuardSQL(inputSQL string, steps []splpath.Step) (string, []any) {
	pathSQL := make([]string, 0, len(steps)*2)
	pathArgs := make([]any, 0, len(steps)*2)
	guards := make([]string, 0, len(steps))
	args := make([]any, 0, len(steps)*len(steps))
	for _, step := range steps {
		pathSQL = append(pathSQL, "?")
		pathArgs = append(pathArgs, step.Key)
		if step.Selector == splpath.ArraySelectorFixed {
			guards = append(guards, "toString(JSONType("+inputSQL+", "+
				strings.Join(pathSQL, ", ")+")) = 'Array'")
			args = append(args, pathArgs...)
			pathSQL = append(pathSQL, "?")

			pathArgs = append(pathArgs, safecast.MustConv[int64](step.Index)+1)
		}
	}
	return strings.Join(guards, " AND "), args
}

func compileRenameAssignment(assignment plan.RenameAssignment, state compileState) ([]string, compileState, bool, error) {
	if assignment.Source.Name == "" || assignment.Destination.Name == "" || assignment.Source.Name == assignment.Destination.Name {
		return nil, compileState{}, false, errors.New("compile ClickHouse rename: assignment is invalid")
	}
	openEventSchema := state.eventRows && state.allowDynamic
	if openEventSchema && (assignment.Source.Name == "fields" || assignment.Destination.Name == "fields") {
		return nil, compileState{}, false, &plan.Diagnostic{
			Code:    "SPL_AMBIGUOUS_RENAME_FIELD",
			Message: "rename cannot use the event result's reserved fields payload without an exact upstream schema",
			Range:   assignment.Range,
		}
	}
	if openEventSchema && ((!assignment.Source.Canonical && len(assignment.Source.Path) != 1) ||
		(!assignment.Destination.Canonical && len(assignment.Destination.Path) != 1)) {
		return nil, compileState{}, false, &plan.Diagnostic{
			Code:        "SPL_UNSUPPORTED_RENAME_PATH",
			Message:     "rename on an open event schema currently supports top-level exact fields only",
			Range:       assignment.Range,
			Suggestions: []string{"select an exact schema with table before renaming a dotted output field"},
		}
	}
	source, sourceExists, err := resolveCompiledField(assignment.Source, state)
	if err != nil {
		return nil, compileState{}, false, err
	}
	_, destinationExists, err := resolveCompiledField(assignment.Destination, state)
	if err != nil {
		return nil, compileState{}, false, err
	}
	if !sourceExists && !destinationExists {
		// With a closed schema, missing-to-missing is an exact no-op. An open
		// event schema resolves dynamic sources above and preserves per-row
		// missingness through the source existence expression.
		return nil, state, false, nil
	}
	if !sourceExists {
		source = fieldState{
			valueSQL:   "CAST(NULL AS Nullable(String))",
			existsSQL:  "0",
			kind:       fieldKindString,
			alwaysNull: true,
		}
	}
	if err := validateKnowledgeFieldSidecars(
		source.relativeFieldNamesSQL,
		source.relativeFieldTypesSQL,
		source.fieldMetadataVersionSQL,
	); err != nil {
		return nil, compileState{}, false, fmt.Errorf(
			"compile ClickHouse rename source %q: %w",
			assignment.Source.Name,
			err,
		)
	}

	next := cloneCompileState(state)
	delete(next.visible, assignment.Source.Name)
	delete(next.visible, assignment.Destination.Name)
	next.blocked[assignment.Source.Name] = struct{}{}
	next.blockedPrefixes[assignment.Source.Name] = struct{}{}
	next.blockedPrefixes[assignment.Destination.Name] = struct{}{}
	delete(next.blocked, assignment.Destination.Name)
	next.publicOrder = renamePublicOrder(
		state.publicOrder,
		assignment.Source.Name,
		assignment.Destination.Name,
		sourceExists && state.visible[assignment.Source.Name].valueSQL != "",
	)
	if exposesRawFieldsPayload(state) {
		// The public fields object is an Open Splunk convenience representation,
		// not a native SPL field. Publishing its immutable storage copy after a
		// rename would expose the old name and any overwritten destination. Drop
		// only that public convenience column; keep both private columns unchanged
		// so unrelated dynamic fields remain available to downstream SPL.
		dropRawFieldsPayload(&next)
	}

	destination := projectedRenameField(source, assignment.Destination.Name)
	next.visible[assignment.Destination.Name] = destination
	next.privateColumns = livePrivateColumns(next.privateColumns, next.visible)
	projection := renameProjection(state, next, assignment.Destination.Name, source)
	if len(projection) == 0 {
		return nil, compileState{}, false, errors.New("compile ClickHouse rename: projection has no fields")
	}
	return projection, next, true, nil
}

func cloneCompileState(state compileState) compileState {
	next := state
	next.visible = make(map[string]fieldState, len(state.visible)+1)
	for name, field := range state.visible {
		field.storedPath = field.storedPath.clone()
		next.visible[name] = field
	}
	next.publicOrder = append([]string(nil), state.publicOrder...)
	next.privateColumns = append([]string(nil), state.privateColumns...)
	next.blocked = cloneSet(state.blocked)
	next.blockedPrefixes = cloneSet(state.blockedPrefixes)
	next.order = append([]compiledSortKey(nil), state.order...)
	next.tieBreakers = append([]compiledSortKey(nil), state.tieBreakers...)
	next.preAggregateValidationColumns = append([]string(nil), state.preAggregateValidationColumns...)
	next.preAggregateValidationArgs = append([]any(nil), state.preAggregateValidationArgs...)
	next.preAggregateColumns = append([]string(nil), state.preAggregateColumns...)
	next.preAggregateArgs = append([]any(nil), state.preAggregateArgs...)
	next.preAggregateListWindowColumns = append(
		[]string(nil),
		state.preAggregateListWindowColumns...,
	)
	next.preAggregateListCandidateColumns = append(
		[]string(nil),
		state.preAggregateListCandidateColumns...,
	)
	next.postAggregateChronological = append(
		[]compiledChronologicalMeasure(nil),
		state.postAggregateChronological...,
	)
	next.postAggregateScalarExtrema = append(
		[]compiledScalarExtremaMeasure(nil),
		state.postAggregateScalarExtrema...,
	)
	next.postAggregateExactStrings = append(
		[]compiledExactStringMeasure(nil),
		state.postAggregateExactStrings...,
	)
	next.postAggregateDistinctCounts = append(
		[]compiledDistinctCount(nil),
		state.postAggregateDistinctCounts...,
	)
	next.postAggregateOrderedStrings = append(
		[]compiledOrderedStringMeasure(nil),
		state.postAggregateOrderedStrings...,
	)
	next.deferredChronologicalValidation = append(
		[]string(nil),
		state.deferredChronologicalValidation...,
	)
	next.chronologicalBarriers = append(
		[]compiledChronologicalBarrier(nil),
		state.chronologicalBarriers...,
	)
	return next
}

func cloneCompiledDynamicFieldFilters(filters []compiledDynamicFieldFilter) []compiledDynamicFieldFilter {
	result := make([]compiledDynamicFieldFilter, len(filters))
	for index, filter := range filters {
		result[index] = compiledDynamicFieldFilter{
			include:  filter.include,
			fields:   slices.Clone(filter.fields),
			patterns: slices.Clone(filter.patterns),
		}
	}
	return result
}

func renamePublicOrder(current []string, source, destination string, sourceIsPublic bool) []string {
	result := make([]string, 0, len(current)+1)
	if sourceIsPublic && slices.Contains(current, source) {
		for _, name := range current {
			switch name {
			case source:
				result = append(result, destination)
			case destination:
			default:
				result = append(result, name)
			}
		}
		return result
	}
	result = append(result, current...)
	if !slices.Contains(result, destination) {
		result = append(result, destination)
	}
	return result
}

func projectedRenameField(source fieldState, destination string) fieldState {
	value := quoteIdentifier(destination)
	textEligibleSQL := source.textEligibleSQL
	semanticBytesSQL := source.semanticBytesSQL
	if source.semanticBytesByUTF8Validity {
		textEligibleSQL = "isValidUTF8(" + value + ")"
		semanticBytesSQL = "toUInt8(isNotNull(" + value +
			") AND NOT isValidUTF8(assumeNotNull(" + value + ")))"
	} else if source.textEligibleBySemanticBytes {
		textEligibleSQL = semanticBytesTextEligibilitySQL(value, semanticBytesSQL)
	}
	result := fieldState{
		valueSQL:                     value,
		maxStringBytes:               source.maxStringBytes,
		flatMultivalueDelimiter:      source.flatMultivalueDelimiter,
		hasFlatMultivalueDelimiter:   source.hasFlatMultivalueDelimiter,
		statsSparkline:               source.statsSparkline,
		textEligibleSQL:              textEligibleSQL,
		rawTextIndexEligible:         source.rawTextIndexEligible,
		dynamicDomain:                source.dynamicDomain,
		numericIntegral:              source.numericIntegral,
		mvSortedLexicographic:        source.mvSortedLexicographic,
		storedTypeSQL:                source.storedTypeSQL,
		existsSQL:                    rewriteExistenceForProjection(source, destination),
		existsArgs:                   append([]any(nil), source.existsArgs...),
		descendantSQL:                source.descendantSQL,
		descendantArgs:               append([]any(nil), source.descendantArgs...),
		storedMetadataPath:           source.storedMetadataPath,
		relativeFieldNamesSQL:        source.relativeFieldNamesSQL,
		relativeFieldTypesSQL:        source.relativeFieldTypesSQL,
		fieldMetadataVersionSQL:      source.fieldMetadataVersionSQL,
		optionalMultivaluePresentSQL: source.optionalMultivaluePresentSQL,
		semanticBytesSQL:             semanticBytesSQL,
		semanticBytesByUTF8Validity:  source.semanticBytesByUTF8Validity,
		textEligibleBySemanticBytes:  source.textEligibleBySemanticBytes,
		stringOrBytes:                source.stringOrBytes,
		stringOrBytesNullable:        source.stringOrBytesNullable,
		kind:                         source.kind,
		// A field renamed to index is calculated pipeline data, not the
		// authorization-constrained physical index selector.
		caseSensitive:           false,
		numberType:              source.numberType,
		numericSort:             source.numericSort,
		alwaysNull:              source.alwaysNull,
		ieeeComparison:          source.ieeeComparison,
		materializeForPredicate: source.materializeForPredicate,
	}
	if source.kind == fieldKindDynamic {
		result.dynamicTypeSQL = "dynamicType(" + value + ")"
	}
	return result
}

func exposesRawFieldsPayload(state compileState) bool {
	if !state.eventRows || !state.allowDynamic || !slices.Contains(state.publicOrder, "fields") {
		return false
	}
	_, explicitlyVisible := state.visible["fields"]
	return !explicitlyVisible
}

func dropRawFieldsPayload(state *compileState) {
	state.publicOrder = slices.DeleteFunc(
		state.publicOrder,
		func(name string) bool { return name == "fields" },
	)
	state.sparseFieldsSubset = false
}

func renameProjection(state, next compileState, destination string, source fieldState) []string {
	names := orderedVisibleNames(next)
	projection := make([]string, 0, len(names)+6+len(state.order)+len(state.tieBreakers))
	for _, name := range names {
		field := state.visible[name]
		if name == destination {
			field = source
		}
		publicName := quoteIdentifier(name)
		projection = appendVisibleFieldProjection(projection, field, publicName)
	}
	return appendPrivateEventProjection(projection, next)
}

func orderedVisibleNames(state compileState) []string {
	names := make([]string, 0, len(state.visible))
	seen := make(map[string]struct{}, len(state.visible))
	appendVisible := func(name string) {
		if _, duplicate := seen[name]; duplicate {
			return
		}
		if _, visible := state.visible[name]; !visible {
			return
		}
		seen[name] = struct{}{}
		names = append(names, name)
	}
	for _, name := range state.publicOrder {
		appendVisible(name)
	}
	for _, name := range canonicalColumnNames {
		appendVisible(name)
	}
	extra := make([]string, 0, len(state.visible)-len(names))
	for name := range state.visible {
		if _, included := seen[name]; !included {
			extra = append(extra, name)
		}
	}
	sort.Strings(extra)
	for _, name := range extra {
		appendVisible(name)
	}
	return names
}

func livePrivateColumns(columns []string, visible map[string]fieldState) []string {
	if len(columns) == 0 || len(visible) == 0 {
		return nil
	}
	live := make([]string, 0, len(columns))
	for _, column := range columns {
		if strings.HasPrefix(strings.Trim(column, `"`), "__os_mvexpand_query_rows_") {
			// This query-wide expansion charge is not owned by one public field.
			// It remains a sealed relation metric until a later mvexpand consumes
			// it, including across projection, rename, and calculated fields.
			live = append(live, column)
			continue
		}
		for _, field := range visible {
			if fieldStateReferencesPrivateColumn(field, column) {
				live = append(live, column)
				break
			}
		}
	}
	return live
}

// fieldStateReferencesPrivateColumn recognizes both a direct sidecar alias and
// compiler-authored expressions such as tupleElement(sidecar, 1). Private
// identifiers are always quoted, so a complete quoted name cannot collide
// with a longer identifier by prefix.
func fieldStateReferencesPrivateColumn(field fieldState, column string) bool {
	for _, expression := range []string{
		field.existsSQL,
		field.storedTypeSQL,
		field.textEligibleSQL,
		field.semanticBytesSQL,
		field.descendantSQL,
		field.relativeFieldNamesSQL,
		field.relativeFieldTypesSQL,
		field.fieldMetadataVersionSQL,
		field.optionalMultivaluePresentSQL,
	} {
		if expression == column || strings.Contains(expression, column) {
			return true
		}
	}
	return false
}

// appendPrivateEventProjection keeps the immutable source document, its
// aligned leaf metadata, and deterministic ordering state available to later
// event-preserving operators. Explicit projections must never expose these
// columns publicly, but they also must not discard them before an event
// analysis finalizer consumes the relation.
func appendPrivateEventProjection(projection []string, state compileState) []string {
	privateColumns := make([]string, 0, 9+len(state.privateColumns)+len(state.order)+len(state.tieBreakers))
	if state.eventRows {
		privateColumns = append(privateColumns,
			quoteIdentifier(internalFieldsColumn),
			quoteIdentifier(internalFieldNamesColumn),
			quoteIdentifier(internalFieldTypesColumn),
			quoteIdentifier(internalFieldMetadataVersionColumn),
			quoteIdentifier(internalRawEncodingColumn),
			quoteIdentifier(internalSortTimeColumn),
			quoteIdentifier(internalSortIDColumn),
			quoteIdentifier(internalSortVisibilityColumn),
			quoteIdentifier(internalSortSourceIdentityColumn),
		)
	}
	privateColumns = append(privateColumns, state.privateColumns...)
	if state.mvExpandQueryRowsSQL != "" {
		privateColumns = append(privateColumns, state.mvExpandQueryRowsSQL)
	}
	if state.rexCapturedBytesSQL != "" {
		privateColumns = append(privateColumns, state.rexCapturedBytesSQL)
	}
	privateColumns = append(
		privateColumns,
		state.deferredChronologicalValidation...,
	)
	for _, key := range state.order {
		privateColumns = append(privateColumns, key.valueSQL)
	}
	for _, key := range state.tieBreakers {
		privateColumns = append(privateColumns, key.valueSQL)
	}
	for _, column := range privateColumns {
		if !slices.Contains(projection, column) {
			projection = append(projection, column)
		}
	}
	return projection
}

func compileEvalComparison(expression *plan.EvalComparisonExpression, state compileState) (string, []any, error) {
	if err := validateCompiledScalarComplexity(expression.Left); err != nil {
		return "", nil, err
	}
	if err := validateCompiledScalarComplexity(expression.Right); err != nil {
		return "", nil, err
	}
	left, err := compileComparisonScalar(expression.Left, state)
	if err != nil {
		return "", nil, err
	}
	right, err := compileComparisonScalar(expression.Right, state)
	if err != nil {
		return "", nil, err
	}
	if isNativeMultivalueKind(left.kind) || isNativeMultivalueKind(right.kind) {
		return "", nil, unsupportedMultivalueUsage("where comparison", expression.Range)
	}
	operator, err := comparisonSQL(expression.Op)
	if err != nil {
		return "", nil, err
	}
	if expression.Op == plan.ComparisonOpNotEqual {
		operator = "!="
	}

	core, coreArgs := evalComparisonCore(left, right, operator)
	// Eval expressions use three-valued logic. Preserve null for a missing or
	// null operand so NOT(NULL) remains NULL and the final WHERE rejects it;
	// coercing the comparison to false here would make NOT missing=value match.
	predicate := "if((" + left.existsSQL + ") AND (" + right.existsSQL + "), " + core + ", CAST(NULL AS Nullable(Bool)))"
	args := make([]any, 0, len(left.existsArgs)+len(right.existsArgs)+len(coreArgs))
	args = append(args, left.existsArgs...)
	args = append(args, right.existsArgs...)
	args = append(args, coreArgs...)
	return predicate, args, nil
}

func compileComparisonScalar(expression plan.ScalarExpression, state compileState) (compiledScalar, error) {
	return compileScalarValue(expression, state)
}

func evalComparisonCore(left, right compiledScalar, operator string) (string, []any) {
	if !left.ieeeComparison && !right.ieeeComparison {
		return evalComparisonCoreWithoutIEEE(left, right, operator)
	}

	originalLeft := left
	originalRight := right
	left = bindCompiledScalarForComparison(left, "__os_ieee_left", "1")
	right = bindCompiledScalarForComparison(right, "__os_ieee_right", "1")

	core, coreArgs := evalComparisonCoreWithoutIEEE(left, right, operator)
	if len(coreArgs) != 0 {
		panic("IEEE comparison over bound scalar retained arguments")
	}
	nanTerms := make([]string, 0, 2)
	if term := scalarNaNPredicateSQL(left); term != "" {
		nanTerms = append(nanTerms, term)
	}
	if term := scalarNaNPredicateSQL(right); term != "" {
		nanTerms = append(nanTerms, term)
	}
	if len(nanTerms) > 0 {
		nanResult := "CAST(0 AS Nullable(Bool))"
		if operator == "!=" {
			nanResult = "CAST(1 AS Nullable(Bool))"
		}
		core = "if((" + strings.Join(nanTerms, " OR ") + "), " +
			nanResult + ", " + core + ")"
	}
	return bindSQLExpressions(
		[]string{"__os_ieee_left", "__os_ieee_right"},
		[]string{originalLeft.valueSQL, originalRight.valueSQL},
		core,
	), comparisonValueArgs(originalLeft, originalRight)
}

func evalComparisonCoreWithoutIEEE(left, right compiledScalar, operator string) (string, []any) {
	if comparisonOperatorIsOrdered(operator) && (left.kind == fieldKindBool || right.kind == fieldKindBool) {
		return "CAST(NULL AS Nullable(Bool))", nil
	}
	if left.dynamicDomain == dynamicScalarDomainText ||
		right.dynamicDomain == dynamicScalarDomainText {
		return dynamicTextEvalComparisonCore(left, right, operator)
	}
	if left.kind == fieldKindDynamic || right.kind == fieldKindDynamic {
		if left.dynamicDomain == dynamicScalarDomainNumeric ||
			right.dynamicDomain == dynamicScalarDomainNumeric {
			return dynamicEvalComparisonCoreWith(left, right, operator, dynamicNumericComparisonBody)
		}
		return dynamicEvalComparisonCoreWith(left, right, operator, dynamicComparisonBody)
	}
	if !fixedScalarKindsComparable(left.kind, right.kind) {
		return "CAST(NULL AS Nullable(Bool))", nil
	}
	leftSQL := left.valueSQL
	rightSQL := right.valueSQL
	if scalarUsesNumericComparison(left, right) {
		integer := scalarIntegerComparison(left, right)
		leftSQL = numericScalarSQL(left, integer)
		rightSQL = numericScalarSQL(right, integer)
	} else if left.kind == fieldKindString || right.kind == fieldKindString {
		// Eval/where string comparisons are case-sensitive. This intentionally
		// differs from the search command's lowerUTF8 comparison behavior.
		leftSQL = stringScalarSQL(left)
		rightSQL = stringScalarSQL(right)
	}
	comparison := leftSQL + " " + operator + " " + rightSQL
	textEligible := make([]string, 0, 2)
	if left.kind == fieldKindString && left.textEligibleSQL != "" {
		textEligible = append(textEligible, "ifNull("+left.textEligibleSQL+", 0)")
	}
	if right.kind == fieldKindString && right.textEligibleSQL != "" {
		textEligible = append(textEligible, "ifNull("+right.textEligibleSQL+", 0)")
	}
	if len(textEligible) > 0 {
		comparison = "if(" + strings.Join(textEligible, " AND ") + ", " +
			comparison + ", CAST(NULL AS Nullable(Bool)))"
	}
	return comparison, comparisonValueArgs(left, right)
}

func scalarNaNPredicateSQL(value compiledScalar) string {
	switch value.kind {
	case fieldKindNumber:
		return "ifNull(isNaN(toFloat64(" + value.valueSQL + ")), 0)"
	case fieldKindDynamic:
		typeSQL := dynamicScalarTypeSQL(value)
		return "(" + typeSQL + " LIKE 'Float%' AND ifNull(isNaN(" +
			"accurateCastOrNull(" + value.valueSQL + ", 'Float64')), 0))"
	default:
		return ""
	}
}

func dynamicTextEvalComparisonCore(left, right compiledScalar, operator string) (string, []any) {
	const nullBool = "CAST(NULL AS Nullable(Bool))"
	if left.kind != fieldKindDynamic && left.kind != fieldKindString {
		return nullBool, nil
	}
	if right.kind != fieldKindDynamic && right.kind != fieldKindString {
		return nullBool, nil
	}

	if left.comparisonAtomic && right.comparisonAtomic {
		return dynamicTextComparisonBody(left, right, operator),
			comparisonValueArgs(left, right)
	}
	originalLeft := left
	originalRight := right
	left.valueSQL = "left_value"
	right.valueSQL = "right_value"
	return bindComparisonOperands(
		dynamicTextComparisonBody(left, right, operator),
		originalLeft,
		originalRight,
	)
}

func dynamicTextComparisonBody(left, right compiledScalar, operator string) string {
	const nullBool = "CAST(NULL AS Nullable(Bool))"
	leftString := left.valueSQL
	leftEligible := "1"
	if left.kind == fieldKindDynamic {
		leftEligible = "dynamicType(" + left.valueSQL + ") = 'String'"
		leftString = "dynamicElement(" + left.valueSQL + ", 'String')"
	}
	rightString := right.valueSQL
	rightEligible := "1"
	if right.kind == fieldKindDynamic {
		rightEligible = "dynamicType(" + right.valueSQL + ") = 'String'"
		rightString = "dynamicElement(" + right.valueSQL + ", 'String')"
	}

	return "if((" + leftEligible + ") AND (" + rightEligible + "), " +
		leftString + " " + operator + " " + rightString + ", " + nullBool + ")"
}

func fixedScalarKindsComparable(left, right fieldKind) bool {
	if left == fieldKindInvalid || right == fieldKindInvalid {
		return false
	}
	// Bool participates only in Bool-v-Bool equality comparisons. ClickHouse otherwise
	// coerces Bool to 0/1, producing results that disagree with runtime-typed
	// Dynamic comparisons and SPL eval's type semantics.
	if (left == fieldKindBool) != (right == fieldKindBool) {
		return false
	}
	return left == right || left == fieldKindNumber || right == fieldKindNumber ||
		left == fieldKindString || right == fieldKindString
}

// dynamicEvalComparisonCoreWith lowers a dynamic eval comparison using the given
// per-flavor comparison body builder.
func dynamicEvalComparisonCoreWith(
	left, right compiledScalar,
	operator string,
	body func(compiledScalar, compiledScalar, string) (string, int, bool),
) (string, []any) {
	if left.comparisonAtomic && right.comparisonAtomic {
		sql, argumentOccurrences, ok := body(left, right, operator)
		if !ok {
			return "CAST(NULL AS Nullable(Bool))", nil
		}
		return sql, repeatedComparisonValueArgs(
			left,
			right,
			argumentOccurrences,
		)
	}
	originalLeft := left
	originalRight := right
	left.valueSQL = "left_value"
	left.dynamicTypeSQL = ""
	right.valueSQL = "right_value"
	right.dynamicTypeSQL = ""
	sql, _, ok := body(left, right, operator)
	if !ok {
		return "CAST(NULL AS Nullable(Bool))", nil
	}
	return bindComparisonOperands(sql, originalLeft, originalRight)
}

func dynamicComparisonBody(
	left, right compiledScalar,
	operator string,
) (string, int, bool) {
	const nullBool = "CAST(NULL AS Nullable(Bool))"
	leftDynamic := left.kind == fieldKindDynamic
	rightDynamic := right.kind == fieldKindDynamic
	if leftDynamic && rightDynamic {
		leftType := dynamicScalarTypeSQL(left)
		rightType := dynamicScalarTypeSQL(right)
		numericCondition := "(" + dynamicNumericValuePredicate(left) + " AND " + dynamicNumericValuePredicate(right) + ")"
		stringCondition := "(" + leftType + " = 'String' AND " + rightType + " = 'String')"
		boolCondition := "(" + leftType + " = 'Bool' AND " + rightType + " = 'Bool')"
		boolComparison := nullBool
		argumentOccurrences := 1
		if !comparisonOperatorIsOrdered(operator) {
			boolComparison = dynamicBoolScalarSQL(left) + " " + operator + " " + dynamicBoolScalarSQL(right)
		}
		result := "multiIf(" +
			numericCondition + ", " + exactNumericKeyComparisonSQL(
			exactNumericScalarKeySQL(left),
			exactNumericScalarKeySQL(right),
			operator,
		) + ", " +
			stringCondition + ", " + dynamicStringScalarSQL(left) + " " + operator + " " + dynamicStringScalarSQL(right) + ", " +
			boolCondition + ", " + boolComparison + ", " +
			nullBool + ")"
		return result, argumentOccurrences, true
	}

	dynamic := left
	fixed := right
	if rightDynamic {
		dynamic, fixed = right, left
	}
	typeSQL := dynamicScalarTypeSQL(dynamic)
	comparison := func(dynamicSQL, fixedSQL string) string {
		if leftDynamic {
			return dynamicSQL + " " + operator + " " + fixedSQL
		}
		return fixedSQL + " " + operator + " " + dynamicSQL
	}
	switch fixed.kind {
	case fieldKindNumber:
		if fixed.literal != nil {
			exact := exactNumericKeyComparisonSQL(
				exactNumericScalarKeySQL(left),
				exactNumericScalarKeySQL(right),
				operator,
			)
			floating := comparison(
				numericScalarSQL(dynamic, false),
				numericScalarSQL(fixed, false),
			)
			return "if(startsWith(" + typeSQL + ", 'Float'), " +
				floating + ", " + exact + ")", 1, true
		}
		return exactNumericKeyComparisonSQL(
			exactNumericScalarKeySQL(left),
			exactNumericScalarKeySQL(right),
			operator,
		), 1, true
	case fieldKindTime:
		return "if(" + dynamicNumericValuePredicate(dynamic) + ", " +
			comparison(numericScalarSQL(dynamic, false), numericScalarSQL(fixed, false)) +
			", " + nullBool + ")", 1, true
	case fieldKindString:
		return "if(" + typeSQL + " = 'String', " +
			comparison(dynamicStringScalarSQL(dynamic), stringScalarSQL(fixed)) +
			", " + nullBool + ")", 1, true
	case fieldKindBool:
		return "if(" + typeSQL + " = 'Bool', " +
			comparison(dynamicBoolScalarSQL(dynamic), fixed.valueSQL) +
			", " + nullBool + ")", 1, true
	default:
		return "", 0, false
	}
}

func dynamicNumericComparisonBody(
	left, right compiledScalar,
	operator string,
) (string, int, bool) {
	const nullBool = "CAST(NULL AS Nullable(Bool))"
	leftDynamic := left.kind == fieldKindDynamic
	rightDynamic := right.kind == fieldKindDynamic
	if leftDynamic && rightDynamic {
		leftNumeric := left.dynamicDomain == dynamicScalarDomainNumeric
		rightNumeric := right.dynamicDomain == dynamicScalarDomainNumeric
		if leftNumeric && rightNumeric {
			integerCondition := "(" +
				dynamicIntegerTypePredicate(dynamicScalarTypeSQL(left)) +
				" AND " +
				dynamicIntegerTypePredicate(dynamicScalarTypeSQL(right)) +
				")"
			return "if(" + integerCondition + ", " +
				dynamicPhysicalIntegerSQL(left) + " " + operator + " " +
				dynamicPhysicalIntegerSQL(right) + ", " +
				dynamicPhysicalFloatSQL(left) + " " + operator + " " +
				dynamicPhysicalFloatSQL(right) + ")", 0, true
		}

		numeric := left
		other := right
		numericIsLeft := leftNumeric
		if rightNumeric {
			numeric = right
			other = left
			numericIsLeft = false
		}
		exactComparison := exactNumericKeyComparisonSQL(
			exactNumericScalarKeySQL(left),
			exactNumericScalarKeySQL(right),
			operator,
		)
		exact := dynamicPhysicalIntegerSQL(numeric)
		floating := dynamicPhysicalFloatSQL(numeric)
		if numericIsLeft {
			exact += " " + operator + " " + dynamicExactIntegerSQL(other)
			floating += " " + operator + " " + numericScalarSQL(other, false)
		} else {
			exact = dynamicExactIntegerSQL(other) + " " + operator + " " + exact
			floating = numericScalarSQL(other, false) + " " + operator + " " + floating
		}
		integerCondition := "(" +
			dynamicIntegerTypePredicate(dynamicScalarTypeSQL(numeric)) +
			" AND " +
			dynamicExactIntegerPredicate(other, dynamicScalarTypeSQL(other)) +
			")"
		exactDecimalCondition := "(" +
			dynamicIntegerTypePredicate(dynamicScalarTypeSQL(numeric)) +
			" AND " + dynamicNumericValuePredicate(other) +
			" AND NOT startsWith(" + dynamicScalarTypeSQL(other) + ", 'Float'))"
		return "multiIf(" + integerCondition + ", " + exact + ", " +
			exactDecimalCondition + ", " + exactComparison + ", " +
			dynamicNumericValuePredicate(other) + ", " + floating + ", " +
			nullBool + ")", 0, true
	}

	dynamic := left
	fixed := right
	if rightDynamic {
		dynamic = right
		fixed = left
	}
	comparison := func(dynamicSQL, fixedSQL string) string {
		if leftDynamic {
			return dynamicSQL + " " + operator + " " + fixedSQL
		}
		return fixedSQL + " " + operator + " " + dynamicSQL
	}
	switch fixed.kind {
	case fieldKindNumber:
		if fixed.literal != nil &&
			fixed.literal.Kind == plan.ValueKindFloat64 {
			exact := exactNumericKeyComparisonSQL(
				exactNumericScalarKeySQL(left),
				exactNumericScalarKeySQL(right),
				operator,
			)
			floating := comparison(
				dynamicPhysicalFloatSQL(dynamic),
				numericScalarSQL(fixed, false),
			)
			return "multiIf(" +
				dynamicIntegerTypePredicate(dynamicScalarTypeSQL(dynamic)) +
				", " + exact + ", startsWith(" +
				dynamicScalarTypeSQL(dynamic) + ", 'Float'), " +
				floating + ", " + nullBool + ")", 1, true
		}
		if fixedNumberTypeIsInteger(fixed.numberType) {
			return "if(" +
				dynamicIntegerTypePredicate(dynamicScalarTypeSQL(dynamic)) +
				", " +
				comparison(
					dynamicPhysicalIntegerSQL(dynamic),
					numericScalarSQL(fixed, true),
				) + ", " +
				comparison(
					dynamicPhysicalFloatSQL(dynamic),
					numericScalarSQL(fixed, false),
				) + ")", 2, true
		}
		return comparison(
			dynamicPhysicalFloatSQL(dynamic),
			numericScalarSQL(fixed, false),
		), 1, true
	case fieldKindTime:
		return comparison(
			dynamicPhysicalFloatSQL(dynamic),
			numericScalarSQL(fixed, false),
		), 1, true
	default:
		return "", 0, false
	}
}

func dynamicPhysicalIntegerSQL(value compiledScalar) string {
	return "accurateCastOrNull(" + value.valueSQL + ", 'Int256')"
}

func dynamicPhysicalFloatSQL(value compiledScalar) string {
	return finiteDynamicFloatOrNullSQL(value.valueSQL)
}

func bindComparisonOperands(
	body string,
	left, right compiledScalar,
) (string, []any) {
	return bindSQLExpressions(
			[]string{"left_value", "right_value"},
			[]string{left.valueSQL, right.valueSQL},
			body,
		),
		comparisonValueArgs(left, right)
}

func repeatedComparisonValueArgs(
	left, right compiledScalar,
	occurrences int,
) []any {
	arguments := make(
		[]any,
		0,
		occurrences*(len(left.valueArgs)+len(right.valueArgs)),
	)
	for range occurrences {
		arguments = append(arguments, comparisonValueArgs(left, right)...)
	}
	return arguments
}

func comparisonOperatorIsOrdered(operator string) bool {
	return operator != "=" && operator != "!="
}

func comparisonValueArgs(left, right compiledScalar) []any {
	args := make([]any, 0, len(left.valueArgs)+len(right.valueArgs))
	args = append(args, left.valueArgs...)
	return append(args, right.valueArgs...)
}

func dynamicScalarTypeSQL(value compiledScalar) string {
	if value.dynamicTypeSQL != "" {
		return value.dynamicTypeSQL
	}
	return "dynamicType(" + value.valueSQL + ")"
}

func dynamicIntegerTypePredicate(typeSQL string) string {
	return typeSQL + " IN ('Int8', 'Int16', 'Int32', 'Int64', 'Int128', 'Int256', 'UInt8', 'UInt16', 'UInt32', 'UInt64', 'UInt128', 'UInt256')"
}

func dynamicNumericTypePredicate(typeSQL string) string {
	return "(" + dynamicIntegerTypePredicate(typeSQL) + " OR startsWith(" + typeSQL + ", 'Float') OR startsWith(" + typeSQL + ", 'Decimal'))"
}

// dynamicNumericStringTextWithLimit returns the shared runtime classifier and
// bounded text for an open-schema String that SPL treats as numeric. Keep this
// as the single definition used by typeof, arithmetic, and comparisons so a
// value cannot be numeric on one expression surface but textual on another.
func dynamicNumericStringTextWithLimit(
	value compiledScalar,
	maximumBytes int,
) (condition, boundedText string) {
	typeSQL := dynamicScalarTypeSQL(value)
	stringSQL := dynamicStringScalarSQL(value)
	limit := strconv.Itoa(maximumBytes)
	boundedText = "if(length(" + stringSQL + ") <= " + limit + ", " +
		stringSQL + ", CAST('' AS String))"
	// Over-limit text is replaced with empty text, which the complete-number
	// grammar rejects, so a separate raw-length term would only duplicate the
	// attacker-controlled expression in every comparison.
	const textAlias = "__os_numeric_string_text"
	validText := "isValidUTF8(" + textAlias + ") AND match(" + textAlias +
		", " + decimalNumericStringPattern + ")"
	condition = "(" + typeSQL + " = 'String' AND " + bindSQLExpressions(
		[]string{textAlias},
		[]string{boundedText},
		validText,
	) + ")"
	return condition, boundedText
}

func dynamicNumericStringPredicate(value compiledScalar) string {
	condition, _ := dynamicNumericStringTextWithLimit(
		value,
		MaximumArithmeticDynamicStringBytes,
	)
	return condition
}

func dynamicNumericValuePredicate(value compiledScalar) string {
	if value.dynamicNumericEligibleSQL != "" {
		return value.dynamicNumericEligibleSQL
	}
	return "(" + dynamicNumericTypePredicate(dynamicScalarTypeSQL(value)) + " OR " +
		dynamicNumericStringPredicate(value) + " OR " +
		dynamicTaggedDecimalCondition(value) + ")"
}

func dynamicExactIntegerPredicate(value compiledScalar, typeSQL string) string {
	return "(" + dynamicIntegerTypePredicate(typeSQL) + " OR isNotNull(" +
		dynamicTaggedDecimalIntegralSQL(value) + "))"
}

func dynamicExactIntegerSQL(value compiledScalar) string {
	physical := "accurateCastOrNull(toString(" + value.valueSQL + "), 'Int256')"
	return "coalesce(" + dynamicTaggedDecimalIntegralSQL(value) + ", " + physical + ")"
}

func dynamicTaggedDecimalCondition(value compiledScalar) string {
	return dynamicTaggedEnvelopeCondition(value, "decimal/v1")
}

func dynamicTaggedScalarEnvelopeCondition(value compiledScalar) string {
	limit := strconv.Itoa(MaximumMVCountTaggedPayloadBytes)
	boundedPayload := "if(length(payload) <= " + limit +
		", payload, CAST('' AS String))"
	validity := newDynamicEnvelopePayloadValiditySQL(boundedPayload)
	typeKey := "concat(char(0), 'open_splunk_type')"
	valueKey := "concat(char(0), 'open_splunk_value')"
	payloadValid := "length(payload) <= " + limit + " AND multiIf(" +
		"tag = 'bytes/v1', " + validity.bytesValid + ", " +
		"tag = 'timestamp/v1', " + validity.timestampValid + ", " +
		"tag = 'duration/v1', " + validity.durationValid + ", " +
		"tag = 'decimal/v1', " + validity.decimalValid + ", 0)"
	tagged := "arrayElement(arrayMap(map -> (" +
		"length(map) = 2" +
		" AND mapContains(map, " + typeKey + ")" +
		" AND mapContains(map, " + valueKey + ")" +
		" AND arrayElement(arrayMap((tag, payload) -> (" + payloadValid +
		"), [map[" + typeKey + "]], [map[" + valueKey + "]]), 1)" +
		"), [dynamicElement(" + value.valueSQL + ", 'Map(String, String)')]), 1)"
	return "(" + dynamicScalarTypeSQL(value) + " = 'Map(String, String)' AND " + tagged + ")"
}

func dynamicTaggedEnvelopeCondition(
	value compiledScalar,
	tags ...string,
) string {
	if len(tags) == 0 {
		return "0"
	}
	typeSQL := dynamicScalarTypeSQL(value)
	mapSQL := dynamicTaggedMapSQL(value)
	typeKey := "concat(char(0), 'open_splunk_type')"
	valueKey := "concat(char(0), 'open_splunk_value')"
	tagLiterals := make([]string, 0, len(tags))
	for _, tag := range tags {
		// Tags are compiler-owned constants, but quote them here so the shared
		// helper remains safe if a future storage tag contains punctuation.
		escaped := strings.ReplaceAll(strings.ReplaceAll(tag, `\`, `\\`), `'`, `\'`)
		tagLiterals = append(tagLiterals, "'"+escaped+"'")
	}
	tagPredicate := "= " + tagLiterals[0]
	if len(tagLiterals) > 1 {
		tagPredicate = "IN (" + strings.Join(tagLiterals, ", ") + ")"
	}
	return "(" + typeSQL + " = 'Map(String, String)'" +
		" AND length(" + mapSQL + ") = 2" +
		" AND mapContains(" + mapSQL + ", " + typeKey + ")" +
		" AND mapContains(" + mapSQL + ", " + valueKey + ")" +
		" AND " + mapSQL + "[" + typeKey + "] " + tagPredicate + ")"
}

func dynamicTaggedDecimalText(value compiledScalar) (condition, payload string) {
	return dynamicTaggedDecimalTextWithLimit(
		value,
		MaximumExactNumericBinTextBytes,
	)
}

func dynamicTaggedDecimalTextWithLimit(
	value compiledScalar,
	maximumBytes int,
) (condition, payload string) {
	valueKey := "concat(char(0), 'open_splunk_value')"
	payload = dynamicTaggedMapSQL(value) + "[" + valueKey + "]"
	limit := strconv.Itoa(maximumBytes)
	boundedPayload := "if(length(" + payload + ") <= " + limit + ", " +
		payload + ", CAST('' AS String))"
	condition = "(" + dynamicTaggedDecimalCondition(value) +
		" AND length(" + payload + ") <= " + limit +
		" AND match(" + boundedPayload + ", " +
		dynamicDecimalPayloadPattern + "))"
	return condition, payload
}

func dynamicTaggedMapSQL(value compiledScalar) string {
	return "dynamicElement(" + value.valueSQL + ", 'Map(String, String)')"
}

func dynamicTaggedDecimalFloatSQL(value compiledScalar) string {
	valueKey := "concat(char(0), 'open_splunk_value')"
	return finiteFloatOrNullSQL(dynamicTaggedMapSQL(value) + "[" + valueKey + "]")
}

// dynamicTaggedDecimalIntegralSQL extracts the exact signed-Int256 value of
// every bounded decimal/v1 payload whose mathematical value is integral.
// Decimal points and exponents are decomposed lexically: the expression never
// expands an attacker-controlled exponent and never routes a wide integer
// through Float64. Nonintegral or out-of-range values retain the established
// Float64 compatibility path. ClickHouse's signed String conversions wrap on
// overflow, so the final conversion uses a lexically bounded UInt256 magnitude
// and explicit two's-complement construction.
func dynamicTaggedDecimalIntegralSQL(value compiledScalar) string {
	typeVariable := "__os_tagged_exact_type"
	mapVariable := "__os_tagged_exact_map"
	payloadVariable := "__os_tagged_exact_payload"
	eligibleVariable := "__os_tagged_exact_eligible"
	textVariable := "__os_tagged_exact_text"
	negativeVariable := "__os_tagged_exact_negative"
	bodyVariable := "__os_tagged_exact_body"
	exponentPositionVariable := "__os_tagged_exact_exponent_position"
	significandVariable := "__os_tagged_exact_significand"
	exponentTextVariable := "__os_tagged_exact_exponent_text"
	exponentNegativeVariable := "__os_tagged_exact_exponent_negative"
	exponentTrimmedVariable := "__os_tagged_exact_exponent_trimmed"
	fractionDigitsVariable := "__os_tagged_exact_fraction_digits"
	significantVariable := "__os_tagged_exact_significant"
	exponentVariable := "__os_tagged_exact_exponent"
	integerDigitsVariable := "__os_tagged_exact_integer_digits"
	trimmedSignificantVariable := "__os_tagged_exact_trimmed_significant"
	integerTextVariable := "__os_tagged_exact_integer_text"
	magnitudeVariable := "__os_tagged_exact_magnitude"
	bind := func(parameters, values []string, body string) string {
		arrays := make([]string, len(values))
		for index, value := range values {
			arrays[index] = "[" + value + "]"
		}
		return "arrayElement(arrayMap((" + strings.Join(parameters, ", ") + ") -> " +
			body + ", " + strings.Join(arrays, ", ") + "), 1)"
	}

	typeKey := "concat(char(0), 'open_splunk_type')"
	valueKey := "concat(char(0), 'open_splunk_value')"
	limit := strconv.Itoa(MaximumExactNumericBinTextBytes)
	maxDigits := strconv.Itoa(exactNumericBinMaxDigits)
	exponentClamp := strconv.Itoa(exactNumericBinExponentClamp)
	envelopeValid := typeVariable + " = 'Map(String, String)'" +
		" AND length(" + mapVariable + ") = 2" +
		" AND mapContains(" + mapVariable + ", " + typeKey + ")" +
		" AND mapContains(" + mapVariable + ", " + valueKey + ")" +
		" AND " + mapVariable + "[" + typeKey + "] = 'decimal/v1'" +
		" AND length(" + payloadVariable + ") <= " + limit +
		" AND match(if(length(" + payloadVariable + ") <= " + limit + ", " +
		payloadVariable + ", CAST('' AS String)), " + dynamicDecimalPayloadPattern + ")"
	eligibleSQL := "toUInt8(" + envelopeValid + ")"
	textSQL := "if(" + envelopeValid + ", " + payloadVariable + ", CAST('0' AS String))"
	signOffset := "if(startsWith(" + textVariable + ", '-'), 2, 1)"
	bodySQL := "substring(" + textVariable + ", " + signOffset + ")"
	exponentPositionSQL := "greatest(position(" + bodySQL + ", 'e'), position(" + bodySQL + ", 'E'))"
	significandSQL := "if(" + exponentPositionVariable + " = 0, " + bodyVariable +
		", substring(" + bodyVariable + ", 1, " + exponentPositionVariable + " - 1))"
	exponentTextSQL := "if(" + exponentPositionVariable + " = 0, CAST('0' AS String), substring(" +
		bodyVariable + ", " + exponentPositionVariable + " + 1))"
	exponentOffset := "if(startsWith(" + exponentTextVariable + ", '-') OR startsWith(" +
		exponentTextVariable + ", '+'), 2, 1)"
	exponentDigits := "substring(" + exponentTextVariable + ", " + exponentOffset + ")"
	exponentMagnitude := "if(length(" + exponentTrimmedVariable + ") <= 4, toInt64OrZero(" +
		exponentTrimmedVariable + "), toInt64(" + exponentClamp + "))"
	significantLength := "toInt64(length(" + significantVariable + "))"
	safePrefixLength := "toUInt64(greatest(least(" + integerDigitsVariable + ", " +
		significantLength + "), 0))"
	safeZeroCount := "toUInt64(greatest(least(" + integerDigitsVariable + " - " +
		significantLength + ", " + maxDigits + "), 0))"
	integerTextSQL := "multiIf(" +
		"empty(" + significantVariable + "), '0', " +
		integerDigitsVariable + " <= 0, '0', " +
		integerDigitsVariable + " <= " + maxDigits + " AND " +
		integerDigitsVariable + " <= " + significantLength + ", substring(" +
		significantVariable + ", 1, " + safePrefixLength + "), " +
		integerDigitsVariable + " <= " + maxDigits + ", concat(" + significantVariable +
		", repeat('0', " + safeZeroCount + ")), '0')"
	integralNonzero := integerDigitsVariable + " > 0 AND " + integerDigitsVariable +
		" >= toInt64(length(" + trimmedSignificantVariable + "))"
	withinBound := "(length(" + integerTextVariable + ") < " + maxDigits +
		" OR (length(" + integerTextVariable + ") = " + maxDigits +
		" AND " + integerTextVariable + " <= if(" + negativeVariable + ", '" +
		exactNumericBinMinMagnitude + "', '" + exactNumericBinMaxInt256 + "')))"
	fits := eligibleVariable + " != 0 AND (empty(" + significantVariable + ") OR ((" +
		integralNonzero + ") AND " + integerDigitsVariable + " <= " + maxDigits + ")) AND " +
		withinBound
	negativeCandidate := "reinterpretAsInt256(bitNot(" + magnitudeVariable + ") + toUInt256(1))"
	candidate := "if(" + fits + ", if(" + negativeVariable + " AND " +
		magnitudeVariable + " != toUInt256(0), " + negativeCandidate +
		", accurateCastOrNull(" + magnitudeVariable +
		", 'Int256')), CAST(NULL AS Nullable(Int256)))"

	candidate = bind(
		[]string{magnitudeVariable},
		[]string{"toUInt256(" + integerTextVariable + ")"},
		candidate,
	)
	candidate = bind(
		[]string{integerTextVariable},
		[]string{integerTextSQL},
		candidate,
	)
	candidate = bind(
		[]string{integerDigitsVariable, trimmedSignificantVariable},
		[]string{
			significantLength + " + " + exponentVariable + " - " + fractionDigitsVariable,
			"replaceRegexpOne(" + significantVariable + ", '0+$', '')",
		},
		candidate,
	)
	candidate = bind(
		[]string{exponentVariable},
		[]string{"if(" + exponentNegativeVariable + " != 0, -(" + exponentMagnitude +
			"), " + exponentMagnitude + ")"},
		candidate,
	)
	candidate = bind(
		[]string{
			exponentNegativeVariable,
			exponentTrimmedVariable,
			fractionDigitsVariable,
			significantVariable,
		},
		[]string{
			"toUInt8(startsWith(" + exponentTextVariable + ", '-'))",
			"replaceRegexpOne(" + exponentDigits + ", '^0+', '')",
			"toInt64(if(position(" + significandVariable + ", '.') = 0, 0, length(" +
				significandVariable + ") - position(" + significandVariable + ", '.')))",
			"replaceRegexpOne(replaceAll(" + significandVariable + ", '.', ''), '^0+', '')",
		},
		candidate,
	)
	candidate = bind(
		[]string{significandVariable, exponentTextVariable},
		[]string{significandSQL, exponentTextSQL},
		candidate,
	)
	candidate = bind(
		[]string{negativeVariable, bodyVariable, exponentPositionVariable},
		[]string{
			"toUInt8(startsWith(" + textVariable + ", '-'))",
			bodySQL,
			exponentPositionSQL,
		},
		candidate,
	)
	candidate = bind(
		[]string{eligibleVariable, textVariable},
		[]string{eligibleSQL, textSQL},
		candidate,
	)
	candidate = bind(
		[]string{payloadVariable},
		[]string{mapVariable + "[" + valueKey + "]"},
		candidate,
	)
	return bind(
		[]string{typeVariable, mapVariable},
		[]string{dynamicScalarTypeSQL(value), dynamicTaggedMapSQL(value)},
		candidate,
	)
}

func dynamicStringScalarSQL(value compiledScalar) string {
	return "dynamicElement(" + value.valueSQL + ", 'String')"
}

func dynamicBoolScalarSQL(value compiledScalar) string {
	return "dynamicElement(" + value.valueSQL + ", 'Bool')"
}

func scalarUsesNumericComparison(left, right compiledScalar) bool {
	return left.kind == fieldKindNumber || right.kind == fieldKindNumber
}

func scalarIntegerComparison(left, right compiledScalar) bool {
	return fixedNumberTypeIsInteger(left.numberType) && fixedNumberTypeIsInteger(right.numberType)
}

func numericScalarSQL(value compiledScalar, integer bool) string {
	if integer {
		if value.kind == fieldKindDynamic {
			return dynamicExactIntegerSQL(value)
		}
		if fixedNumberTypeIsInteger(value.numberType) {
			if value.literal != nil {
				return "accurateCastOrNull(" + value.valueSQL + ", 'Int256')"
			}
			return "toInt256(" + value.valueSQL + ")"
		}
		return "accurateCastOrNull(toString(" + value.valueSQL + "), 'Int256')"
	}
	if value.kind == fieldKindTime {
		return "(toFloat64(toUnixTimestamp64Nano(" + value.valueSQL + ")) / 1000000000)"
	}
	if value.kind == fieldKindDynamic {
		return "if(" + dynamicTaggedDecimalCondition(value) + ", " + dynamicTaggedDecimalFloatSQL(value) +
			", toFloat64OrNull(toString(" + value.valueSQL + ")))"
	}
	if value.kind == fieldKindNumber {
		return "toFloat64(" + value.valueSQL + ")"
	}
	return "toFloat64OrNull(toString(" + value.valueSQL + "))"
}

func stringScalarSQL(value compiledScalar) string {
	if value.literal != nil && value.literal.Kind == plan.ValueKindString {
		return value.valueSQL
	}
	if value.kind == fieldKindDynamic {
		return dynamicStringScalarSQL(value)
	}
	return "toString(" + value.valueSQL + ")"
}

func compileComparison(expression *plan.ComparisonExpression, field fieldState) (string, []any, error) {
	exists := field.existsSQL
	args := append([]any(nil), field.existsArgs...)
	if isNativeMultivalueKind(field.kind) && expression.Value.Kind == plan.ValueKindNull {
		return "", nil, unsupportedMultivalueUsage("search null comparison", expression.Range)
	}
	if expression.Value.Kind == plan.ValueKindNull {
		equal := "(" + exists + " AND isNull(" + field.valueSQL + "))"
		if expression.Op == plan.ComparisonOpEqual {
			return equal, args, nil
		}
		if expression.Op == plan.ComparisonOpNotEqual {
			return "(" + exists + " AND NOT isNull(" + field.valueSQL + "))", args, nil
		}
		return "", nil, errors.New("compile ClickHouse predicate: null only supports = and !=")
	}

	text := comparisonSourceText(expression.Value)
	if expression.Value.Kind == plan.ValueKindString && text == "*" &&
		(expression.Op == plan.ComparisonOpEqual || expression.Op == plan.ComparisonOpNotEqual) {
		if expression.Op == plan.ComparisonOpNotEqual {
			// SPL field!=* excludes missing fields and every present value,
			// including explicit null, so it cannot match an event.
			return "0", nil, nil
		}
		presence, presenceArgs := logicalFieldPresenceSQL(field)
		return presence, presenceArgs, nil
	}
	if isNativeMultivalueKind(field.kind) &&
		expression.Op != plan.ComparisonOpEqual && expression.Op != plan.ComparisonOpNotEqual {
		return "", nil, unsupportedMultivalueUsage("ordered search comparison", expression.Range)
	}

	operator, err := comparisonSQL(expression.Op)
	if err != nil {
		return "", nil, err
	}
	var (
		predicate           string
		argumentOccurrences int
	)
	if expression.Op == plan.ComparisonOpEqual || expression.Op == plan.ComparisonOpNotEqual {
		predicate, argumentOccurrences = equalityPredicate(expression, field, text)
	} else {
		predicate, argumentOccurrences, err = relationalPredicate(expression, field, operator)
		if err != nil {
			return "", nil, err
		}
	}
	argument := any(text)
	if expression.Value.Kind == plan.ValueKindString && strings.Contains(text, "*") &&
		(expression.Op == plan.ComparisonOpEqual || expression.Op == plan.ComparisonOpNotEqual) {
		argument = wildcardRegex(text, true)
	}
	for range argumentOccurrences {
		args = append(args, argument)
	}
	if expression.Op == plan.ComparisonOpNotEqual {
		// SPL field!=value excludes missing fields while treating a present null
		// as unequal to a non-null value. ifNull collapses SQL's UNKNOWN here.
		if field.kind == fieldKindString && field.textEligibleSQL != "" {
			eligible := "ifNull(" + field.textEligibleSQL + ", 0)"
			return "(" + exists + " AND if(" + eligible +
				", NOT ifNull(" + predicate + ", 0), 0))", args, nil
		}
		return "(" + exists + " AND NOT ifNull(" + predicate + ", 0))", args, nil
	}
	if field.kind == fieldKindString && field.textEligibleSQL != "" {
		eligible := "ifNull(" + field.textEligibleSQL + ", 0)"
		return "(" + exists + " AND if(" + eligible +
			", ifNull(" + predicate + ", 0), 0))", args, nil
	}
	return "(" + exists + " AND ifNull(" + predicate + ", 0))", args, nil
}

func equalityPredicate(expression *plan.ComparisonExpression, field fieldState, text string) (string, int) {
	valueSQL := field.valueSQL
	if field.kind == fieldKindStringArray {
		if expression.Value.Kind == plan.ValueKindString && strings.Contains(text, "*") {
			return "arrayExists(element -> isValidUTF8(element) AND match(element, ?), " + valueSQL + ")", 1
		}
		return "arrayExists(element -> isValidUTF8(element) AND lowerUTF8(element) = lowerUTF8(?), " + valueSQL + ")", 1
	}
	if field.kind == fieldKindDynamicArray {
		// Search equality remains textual for compatibility, but every native
		// member is rendered by the same canonical contract as mvjoin/nomv.
		if expression.Value.Kind == plan.ValueKindString && strings.Contains(text, "*") {
			return "arrayExists(element -> match(" + nativeMVCanonicalTextSQL("element") + ", ?), " + valueSQL + ")", 1
		}
		return "arrayExists(element -> lowerUTF8(" + nativeMVCanonicalTextSQL("element") + ") = lowerUTF8(?), " + valueSQL + ")", 1
	}
	if expression.Value.Kind == plan.ValueKindString && strings.Contains(text, "*") {
		return "match(toString(" + valueSQL + "), ?)", 1
	}
	if field.caseSensitive {
		return valueSQL + " = ?", 1
	}
	if field.kind == fieldKindTime {
		if expression.Value.Kind == plan.ValueKindString {
			return valueSQL + " = parseDateTime64BestEffortOrNull(?, 9, 'UTC')", 1
		}
		return "(toFloat64(toUnixTimestamp64Nano(" + valueSQL + ")) / 1000000000) = toFloat64OrNull(?)", 1
	}
	if left, right, ok := fixedNumberComparisonOperands(field, expression.Value.Kind); ok {
		return left + " = " + right, 1
	}
	base := "lowerUTF8(toString(" + valueSQL + ")) = lowerUTF8(?)"
	if field.kind != fieldKindDynamic {
		return base, 1
	}
	dynamic := compiledScalarFromField(field)
	typeSQL := dynamicScalarTypeSQL(dynamic)
	guard := dynamicLiteralGuard(typeSQL, expression.Value.Kind)
	switch expression.Value.Kind {
	case plan.ValueKindInt64, plan.ValueKindUint64:
		exact := exactNumericLiteralFieldComparisonSQL(
			dynamic,
			&expression.Value,
			"=",
		)
		eligible := "(" + dynamicExactIntegerPredicate(dynamic, typeSQL) + " OR " +
			dynamicNumericStringPredicate(dynamic) + ")"
		return "if(" + eligible + ", " +
			exact + ", 0)", 0
	case plan.ValueKindFloat64:
		exactCondition := "(startsWith(" + typeSQL + ", 'Decimal') OR " +
			dynamicTaggedDecimalCondition(dynamic) + " OR " +
			dynamicNumericStringPredicate(dynamic) + ")"
		exact := exactNumericLiteralFieldComparisonSQL(
			dynamic,
			&expression.Value,
			"=",
		)
		floating := numericScalarSQL(dynamic, false) + " = toFloat64OrNull(?)"
		return "multiIf(startsWith(" + typeSQL + ", 'Float'), " +
			floating + ", " + exactCondition + ", " + exact + ", 0)", 1
	}
	return "(" + guard + " AND " + base + ")", 1
}

func relationalPredicate(expression *plan.ComparisonExpression, field fieldState, operator string) (string, int, error) {
	if expression.Value.Kind == plan.ValueKindBool {
		return "", 0, errors.New("compile ClickHouse predicate: booleans do not support ordered comparison")
	}
	if field.kind == fieldKindTime {
		if expression.Value.Kind == plan.ValueKindString {
			return field.valueSQL + " " + operator + " parseDateTime64BestEffortOrNull(?, 9, 'UTC')", 1, nil
		}
		return "(toFloat64(toUnixTimestamp64Nano(" + field.valueSQL + ")) / 1000000000) " + operator + " toFloat64OrNull(?)", 1, nil
	}
	if expression.Value.Kind == plan.ValueKindString {
		switch field.kind {
		case fieldKindString:
			if field.caseSensitive {
				return field.valueSQL + " " + operator + " ?", 1, nil
			}
			return "lowerUTF8(toString(" + field.valueSQL + ")) " + operator + " lowerUTF8(?)", 1, nil
		case fieldKindDynamic:
			typeSQL := dynamicTypeExpression(field)
			valueSQL := "dynamicElement(" + field.valueSQL + ", 'String')"
			comparison := "lowerUTF8(" + valueSQL + ") " + operator + " lowerUTF8(?)"
			return "(" + typeSQL + " = 'String' AND " + comparison + ")", 1, nil
		}
	}
	if left, right, ok := fixedNumberComparisonOperands(field, expression.Value.Kind); ok {
		return left + " " + operator + " " + right, 1, nil
	}
	if field.kind == fieldKindDynamic &&
		(expression.Value.Kind == plan.ValueKindInt64 ||
			expression.Value.Kind == plan.ValueKindUint64 ||
			expression.Value.Kind == plan.ValueKindFloat64) {
		typeSQL := dynamicTypeExpression(field)
		dynamic := compiledScalarFromField(field)
		exact := exactNumericLiteralFieldComparisonSQL(
			dynamic,
			&expression.Value,
			operator,
		)
		eligible := "(" + dynamicNumericValuePredicate(dynamic) + " OR " +
			typeSQL + " = 'String')"
		floating := numericScalarSQL(dynamic, false) + " " + operator +
			" toFloat64OrNull(?)"
		return "multiIf(startsWith(" + typeSQL + ", 'Float'), " +
			floating + ", " + eligible + ", " + exact +
			", CAST(NULL AS Nullable(Bool)))", 1, nil
	}
	if field.kind == fieldKindDynamic {
		return numericScalarSQL(compiledScalarFromField(field), false) + " " + operator + " toFloat64OrNull(?)", 1, nil
	}
	return "toFloat64OrNull(toString(" + field.valueSQL + ")) " + operator + " toFloat64OrNull(?)", 1, nil
}

func compiledScalarFromField(field fieldState) compiledScalar {
	return compiledScalar{
		valueSQL:                     field.valueSQL,
		exactNumericKeySQL:           field.exactNumericKeySQL,
		dynamicNumericEligibleSQL:    field.dynamicNumericEligibleSQL,
		maxStringBytes:               field.maxStringBytes,
		existsSQL:                    field.existsSQL,
		existsArgs:                   append([]any(nil), field.existsArgs...),
		textEligibleSQL:              field.textEligibleSQL,
		dynamicDomain:                field.dynamicDomain,
		numericIntegral:              field.numericIntegral,
		mvCountOneOrNull:             field.mvCountOneOrNull,
		mvSortedLexicographic:        field.mvSortedLexicographic,
		dynamicTypeSQL:               field.dynamicTypeSQL,
		storedTypeSQL:                field.storedTypeSQL,
		descendantSQL:                field.descendantSQL,
		descendantArgs:               append([]any(nil), field.descendantArgs...),
		storedPath:                   field.storedPath.clone(),
		storedMetadataPath:           field.storedMetadataPath,
		relativeFieldNamesSQL:        field.relativeFieldNamesSQL,
		relativeFieldTypesSQL:        field.relativeFieldTypesSQL,
		fieldMetadataVersionSQL:      field.fieldMetadataVersionSQL,
		optionalMultivaluePresentSQL: field.optionalMultivaluePresentSQL,
		semanticBytesSQL:             field.semanticBytesSQL,
		semanticBytesByUTF8Validity:  field.semanticBytesByUTF8Validity,
		textEligibleBySemanticBytes:  field.textEligibleBySemanticBytes,
		stringOrBytes:                field.stringOrBytes,
		stringOrBytesNullable:        field.stringOrBytesNullable,
		kind:                         field.kind,
		numberType:                   field.numberType,
		alwaysNull:                   field.alwaysNull,
		ieeeComparison:               field.ieeeComparison,
		comparisonAtomic:             true,
		materializeForPredicate:      field.materializeForPredicate,
	}
}

func fixedNumberComparisonOperands(field fieldState, literalKind plan.ValueKind) (left, right string, ok bool) {
	if field.numberType == "" {
		return "", "", false
	}
	if literalKind != plan.ValueKindInt64 && literalKind != plan.ValueKindUint64 && literalKind != plan.ValueKindFloat64 {
		return "", "", false
	}
	if literalKind != plan.ValueKindFloat64 && fixedNumberTypeIsInteger(field.numberType) {
		return "toInt256(" + field.valueSQL + ")", "accurateCastOrNull(?, 'Int256')", true
	}
	return "toFloat64(" + field.valueSQL + ")", "toFloat64OrNull(?)", true
}

func fixedNumberTypeIsInteger(numberType string) bool {
	return strings.HasPrefix(numberType, "Int") || strings.HasPrefix(numberType, "UInt")
}

func dynamicLiteralGuard(typeSQL string, kind plan.ValueKind) string {
	switch kind {
	case plan.ValueKindInt64, plan.ValueKindUint64:
		return typeSQL + " IN ('Int8', 'Int16', 'Int32', 'Int64', 'Int128', 'Int256', 'UInt8', 'UInt16', 'UInt32', 'UInt64', 'UInt128', 'UInt256')"
	case plan.ValueKindFloat64:
		return "(startsWith(" + typeSQL + ", 'Float') OR startsWith(" + typeSQL + ", 'Decimal'))"
	case plan.ValueKindBool:
		return typeSQL + " = 'Bool'"
	case plan.ValueKindString:
		return typeSQL + " = 'String'"
	default:
		return "0"
	}
}

func dynamicTypeExpression(field fieldState) string {
	if field.dynamicTypeSQL != "" {
		return field.dynamicTypeSQL
	}
	return "dynamicType(" + field.valueSQL + ")"
}

func comparisonSourceText(value plan.Value) string {
	if value.SourceText != "" {
		return value.SourceText
	}
	switch value.Kind {
	case plan.ValueKindString:
		return value.String
	case plan.ValueKindInt64:
		return fmt.Sprintf("%d", value.Int64)
	case plan.ValueKindUint64:
		return fmt.Sprintf("%d", value.Uint64)
	case plan.ValueKindFloat64:
		return fmt.Sprintf("%g", value.Float64)
	case plan.ValueKindBool:
		if value.Bool {
			return "true"
		}
		return "false"
	case plan.ValueKindNull:
		return "null"
	default:
		return ""
	}
}

func comparisonSQL(operator plan.ComparisonOp) (string, error) {
	switch operator {
	case plan.ComparisonOpEqual, plan.ComparisonOpNotEqual:
		return "=", nil
	case plan.ComparisonOpLess:
		return "<", nil
	case plan.ComparisonOpLessEqual:
		return "<=", nil
	case plan.ComparisonOpGreater:
		return ">", nil
	case plan.ComparisonOpGreaterEqual:
		return ">=", nil
	default:
		return "", errors.New("compile ClickHouse predicate: invalid comparison operator")
	}
}

func resolveCompiledField(field plan.FieldRef, state compileState) (fieldState, bool, error) {
	if existing, ok := state.visible[field.Name]; ok {
		return existing, true, nil
	}
	if _, blocked := state.blocked[field.Name]; blocked || field.Canonical || !state.allowDynamic {
		return fieldState{}, false, nil
	}
	for prefix := range state.blockedPrefixes {
		if field.Name == prefix || strings.HasPrefix(field.Name, prefix+".") {
			return fieldState{}, false, nil
		}
	}
	if !compiledDynamicFieldNameAllowed(state.dynamicFieldFilters, field.Name) {
		return fieldState{}, false, nil
	}
	if len(field.Path) == 0 {
		return fieldState{}, false, fmt.Errorf("compile ClickHouse field %q: dynamic path is empty", field.Name)
	}
	storedPath, err := mintStoredPathAuthority(field.Path)
	if err != nil {
		return fieldState{}, false, fmt.Errorf("compile ClickHouse field %q: %w", field.Name, err)
	}
	if storedPath.normalizedExactPath != field.Name {
		return fieldState{}, false, fmt.Errorf(
			"compile ClickHouse field %q: dynamic path metadata disagrees with its name",
			field.Name,
		)
	}
	value := storedPath.valueSQL()
	return fieldState{
		valueSQL:       value,
		dynamicTypeSQL: "dynamicType(" + value + ")",
		existsSQL:      "has(" + quoteIdentifier(internalFieldNamesColumn) + ", ?)",
		existsArgs:     []any{storedPath.normalizedExactPath},
		descendantSQL: "arrayExists(name -> startsWith(name, ?), " +
			quoteIdentifier(internalFieldNamesColumn) + ")",
		descendantArgs:     []any{storedPath.normalizedDescendantPrefix},
		storedPath:         storedPath,
		storedMetadataPath: storedPath.normalizedExactPath,
		rawEventDynamic:    true,
		kind:               fieldKindDynamic,
	}, true, nil
}

func compiledDynamicFieldNameAllowed(filters []compiledDynamicFieldFilter, name string) bool {
	for _, filter := range filters {
		matched := slices.Contains(filter.fields, name)
		if !matched && filter.include {
			matched = slices.ContainsFunc(filter.fields, func(field string) bool {
				return strings.HasPrefix(name, field+".")
			})
		}
		if !matched {
			matched = slices.ContainsFunc(filter.patterns, func(pattern string) bool {
				return spl.MatchFieldsFieldGlob(pattern, name)
			})
		}
		if (filter.include && !matched) || (!filter.include && matched) {
			return false
		}
	}
	return true
}

func compileProjection(operator *plan.Project, state compileState, relationAlias string, stage int) ([]string, compileState, []any, error) {
	hasPatterns := len(operator.Patterns) > 0
	next := compileState{
		visible:              make(map[string]fieldState),
		context:              state.context,
		privateColumns:       append([]string(nil), state.privateColumns...),
		rexCapturedBytesSQL:  state.rexCapturedBytesSQL,
		allowDynamic:         state.allowDynamic && (operator.Mode == plan.ProjectModeExclude || hasPatterns),
		sparseFieldsSubset:   false,
		eventRows:            state.eventRows,
		blocked:              cloneSet(state.blocked),
		blockedPrefixes:      cloneSet(state.blockedPrefixes),
		dynamicFieldFilters:  cloneCompiledDynamicFieldFilters(state.dynamicFieldFilters),
		order:                append([]compiledSortKey(nil), state.order...),
		tieBreakers:          append([]compiledSortKey(nil), state.tieBreakers...),
		mvExpandQueryRowsSQL: state.mvExpandQueryRowsSQL,
		chronologicalBarriers: append(
			[]compiledChronologicalBarrier(nil),
			state.chronologicalBarriers...,
		),
	}
	var names []string
	retainSparsePayload := false
	hiddenNames := make(map[string]struct{})
	switch operator.Mode {
	case plan.ProjectModeInclude, plan.ProjectModeTable:
		for _, field := range operator.Fields {
			names = append(names, field.Name)
		}
		if hasPatterns {
			for _, name := range state.publicOrder {
				if name != "fields" && projectPatternsMatchName(operator.Patterns, name) &&
					!slices.Contains(names, name) {
					names = append(names, name)
				}
			}
			remainingVisible := make([]string, 0, len(state.visible))
			for name := range state.visible {
				if name != "fields" && projectPatternsMatchName(operator.Patterns, name) &&
					!slices.Contains(names, name) {
					remainingVisible = append(remainingVisible, name)
				}
			}
			sort.Strings(remainingVisible)
			names = append(names, remainingVisible...)
			retainSparsePayload = state.eventRows && state.allowDynamic
		}
		if operator.Mode == plan.ProjectModeInclude {
			for _, implicit := range []string{"_time", "_raw"} {
				if _, exists := state.visible[implicit]; exists && !slices.Contains(names, implicit) {
					names = append(names, implicit)
				}
			}
		}
	case plan.ProjectModeExclude:
		retainSparsePayload = state.eventRows && state.allowDynamic
		excluded := make(map[string]struct{}, len(operator.Fields))
		for _, field := range operator.Fields {
			excluded[field.Name] = struct{}{}
			next.blocked[field.Name] = struct{}{}
		}
		for _, name := range state.publicOrder {
			if name == "fields" && state.eventRows {
				if _, visible := state.visible[name]; !visible {
					retainSparsePayload = state.allowDynamic
					continue
				}
			}
			_, removeExact := excluded[name]
			removePattern := projectPatternsMatchName(operator.Patterns, name)
			if !removeExact && !removePattern {
				names = append(names, name)
			}
		}
		remainingHidden := make([]string, 0, len(state.visible))
		for name := range state.visible {
			if slices.Contains(state.publicOrder, name) {
				continue
			}
			_, removeExact := excluded[name]
			if removeExact || projectPatternsMatchName(operator.Patterns, name) {
				continue
			}
			remainingHidden = append(remainingHidden, name)
			hiddenNames[name] = struct{}{}
		}
		sort.Strings(remainingHidden)
		names = append(names, remainingHidden...)
	default:
		return nil, compileState{}, nil, errors.New("compile ClickHouse projection: invalid mode")
	}

	projection := make([]string, 0, len(names)+6)
	args := make([]any, 0)
	privateSidecarArgs := make([]any, 0)
	privateSidecarProjections := make([]string, 0)
	privateSidecarColumns := make([]string, 0)
	for fieldIndex, name := range names {
		var ref plan.FieldRef
		for _, candidate := range operator.Fields {
			if candidate.Name == name {
				ref = candidate
				break
			}
		}
		if ref.Name == "" {
			ref = plan.FieldRef{Name: name, Canonical: true}
		}
		compiled, ok, err := resolveCompiledField(ref, state)
		if err != nil {
			return nil, compileState{}, nil, err
		}
		if !ok {
			if operator.Mode != plan.ProjectModeTable {
				continue
			}
			// table declares an exact output schema. Preserve requested fields
			// that a prior transforming stage removed as nullable missing columns
			// instead of silently changing the result shape.
			compiled = fieldState{
				valueSQL:   "CAST(NULL AS Nullable(String))",
				existsSQL:  "0",
				kind:       fieldKindString,
				alwaysNull: true,
			}
		}
		if err := validateKnowledgeFieldSidecars(
			compiled.relativeFieldNamesSQL,
			compiled.relativeFieldTypesSQL,
			compiled.fieldMetadataVersionSQL,
		); err != nil {
			return nil, compileState{}, nil, fmt.Errorf(
				"compile ClickHouse projection field %q: %w",
				name,
				err,
			)
		}
		publicName := quoteIdentifier(name)
		if compiled.valueSQL == publicName {
			projection = append(projection, publicName)
		} else {
			projection = append(projection, compiled.valueSQL+" AS "+publicName)
		}
		textEligibleSQL := compiled.textEligibleSQL
		semanticBytesSQL := compiled.semanticBytesSQL
		if compiled.semanticBytesByUTF8Validity {
			textEligibleSQL = "isValidUTF8(" + publicName + ")"
			semanticBytesSQL = "toUInt8(isNotNull(" + publicName +
				") AND NOT isValidUTF8(assumeNotNull(" + publicName + ")))"
		} else if compiled.textEligibleBySemanticBytes {
			textEligibleSQL = semanticBytesTextEligibilitySQL(
				publicName,
				semanticBytesSQL,
			)
		}
		projectedExistsSQL := rewriteExistenceForProjection(compiled, name)
		projectedExistsArgs := append([]any(nil), compiled.existsArgs...)
		projectedDescendantSQL := compiled.descendantSQL
		projectedDescendantArgs := append([]any(nil), compiled.descendantArgs...)
		storedMetadataPath := compiled.storedMetadataPath
		if storedMetadataPath == "" {
			storedMetadataPath, _ = exactStoredMetadataPath(compiled)
		}
		// Renamed and calculated fields can retain row-local presence authority
		// from a raw Dynamic source. Freeze that authority before wildcard
		// filtering removes tombstoned or shadowed raw names from the inventory.
		// Direct raw projections keep using the filtered inventory itself.
		if retainSparsePayload && !compiled.rawEventDynamic && strings.Contains(
			projectedExistsSQL,
			quoteIdentifier(internalFieldNamesColumn),
		) {
			existsColumn := quoteIdentifier(fmt.Sprintf(
				"__os_fields_exists_%d_%d",
				stage,
				fieldIndex+1,
			))
			privateSidecarProjections = append(
				privateSidecarProjections,
				"toUInt8(ifNull("+qualifyEventFieldMetadataSQL(projectedExistsSQL, relationAlias)+", 0)) AS "+existsColumn,
			)
			privateSidecarColumns = append(privateSidecarColumns, existsColumn)
			privateSidecarArgs = append(privateSidecarArgs, projectedExistsArgs...)
			projectedExistsSQL = existsColumn
			projectedExistsArgs = nil
		}
		if retainSparsePayload && !compiled.rawEventDynamic && strings.Contains(
			projectedDescendantSQL,
			quoteIdentifier(internalFieldNamesColumn),
		) {
			descendantColumn := quoteIdentifier(fmt.Sprintf(
				"__os_fields_descendant_%d_%d",
				stage,
				fieldIndex+1,
			))
			privateSidecarProjections = append(
				privateSidecarProjections,
				"toUInt8(ifNull("+qualifyEventFieldMetadataSQL(projectedDescendantSQL, relationAlias)+", 0)) AS "+descendantColumn,
			)
			privateSidecarColumns = append(privateSidecarColumns, descendantColumn)
			privateSidecarArgs = append(privateSidecarArgs, projectedDescendantArgs...)
			projectedDescendantSQL = descendantColumn
			projectedDescendantArgs = nil
		}
		next.visible[name] = fieldState{
			valueSQL: publicName, maxStringBytes: compiled.maxStringBytes,
			flatMultivalueDelimiter:      compiled.flatMultivalueDelimiter,
			hasFlatMultivalueDelimiter:   compiled.hasFlatMultivalueDelimiter,
			statsSparkline:               compiled.statsSparkline,
			textEligibleSQL:              textEligibleSQL,
			rawTextIndexEligible:         compiled.rawTextIndexEligible,
			dynamicDomain:                compiled.dynamicDomain,
			numericIntegral:              compiled.numericIntegral,
			mvCountOneOrNull:             compiled.mvCountOneOrNull,
			mvSortedLexicographic:        compiled.mvSortedLexicographic,
			dynamicTypeSQL:               compiled.dynamicTypeSQL,
			storedTypeSQL:                compiled.storedTypeSQL,
			existsSQL:                    projectedExistsSQL,
			existsArgs:                   projectedExistsArgs,
			descendantSQL:                projectedDescendantSQL,
			descendantArgs:               projectedDescendantArgs,
			relativeFieldNamesSQL:        compiled.relativeFieldNamesSQL,
			relativeFieldTypesSQL:        compiled.relativeFieldTypesSQL,
			fieldMetadataVersionSQL:      compiled.fieldMetadataVersionSQL,
			optionalMultivaluePresentSQL: compiled.optionalMultivaluePresentSQL,
			rawEventDynamic:              compiled.rawEventDynamic,
			storedMetadataPath:           storedMetadataPath,
			semanticBytesSQL:             semanticBytesSQL,
			semanticBytesByUTF8Validity:  compiled.semanticBytesByUTF8Validity,
			textEligibleBySemanticBytes:  compiled.textEligibleBySemanticBytes,
			stringOrBytes:                compiled.stringOrBytes,
			stringOrBytesNullable:        compiled.stringOrBytesNullable,
			kind:                         compiled.kind,
			caseSensitive:                compiled.caseSensitive,
			numberType:                   compiled.numberType,
			numericSort:                  compiled.numericSort,
			canonicalTime:                compiled.canonicalTime,
			alwaysNull:                   compiled.alwaysNull,
			materializeForPredicate:      compiled.materializeForPredicate,
		}
		if _, hidden := hiddenNames[name]; !hidden {
			next.publicOrder = append(next.publicOrder, name)
		}
	}
	if retainSparsePayload {
		next.publicOrder = append(next.publicOrder, "fields")
		next.sparseFieldsSubset = true
	}
	if hasPatterns {
		filter := compiledDynamicFieldFilter{include: operator.Mode == plan.ProjectModeInclude}
		for _, field := range operator.Fields {
			filter.fields = append(filter.fields, field.Name)
		}
		for _, pattern := range operator.Patterns {
			filter.patterns = append(filter.patterns, pattern.Pattern)
		}
		next.dynamicFieldFilters = append(next.dynamicFieldFilters, filter)
	}
	next.privateColumns = livePrivateColumns(next.privateColumns, next.visible)
	if next.mvExpandQueryRowsSQL != "" &&
		!slices.Contains(next.privateColumns, next.mvExpandQueryRowsSQL) {
		next.privateColumns = append(next.privateColumns, next.mvExpandQueryRowsSQL)
	}
	projection = appendPrivateEventProjection(projection, next)
	projection = append(projection, privateSidecarProjections...)
	next.privateColumns = append(next.privateColumns, privateSidecarColumns...)
	if retainSparsePayload {
		filterFields := operator.Fields
		predicate, predicateArgs, filterErr := compileProjectFieldNamePredicate(
			operator.Mode, filterFields, operator.Patterns,
			projectRawPayloadDeniedNames(state), sortedSetValues(state.blockedPrefixes),
		)
		if filterErr != nil {
			return nil, compileState{}, nil, filterErr
		}
		projection = replacePrivateFieldMetadataProjection(
			projection, predicate, relationAlias,
		)
		args = append(args, predicateArgs...)
		args = append(args, predicateArgs...)
	}
	args = append(args, privateSidecarArgs...)
	return projection, next, args, nil
}

func qualifyEventFieldMetadataSQL(expression, relationAlias string) string {
	for _, column := range []string{internalFieldNamesColumn, internalFieldTypesColumn} {
		identifier := quoteIdentifier(column)
		expression = strings.ReplaceAll(expression, identifier, relationAlias+"."+identifier)
	}
	return expression
}

func projectPatternsMatchName(patterns []plan.ProjectFieldPattern, name string) bool {
	return slices.ContainsFunc(patterns, func(pattern plan.ProjectFieldPattern) bool {
		return spl.MatchFieldsFieldGlob(pattern.Pattern, name)
	})
}

func compileProjectFieldNamePredicate(
	mode plan.ProjectMode,
	fields []plan.FieldRef,
	patterns []plan.ProjectFieldPattern,
	deniedNames []string,
	deniedPrefixes []string,
) (string, []any, error) {
	terms := make([]string, 0, len(fields)+len(patterns))
	args := make([]any, 0, len(fields)+len(patterns))
	for _, field := range fields {
		if field.Name == "" || strings.HasPrefix(strings.ToLower(field.Name), "__os_") {
			return "", nil, errors.New("compile ClickHouse projection: invalid exact field selector")
		}
		if mode == plan.ProjectModeInclude {
			terms = append(terms,
				"(field_name = ? OR startsWith(field_name, concat(?, '.')))",
			)
			args = append(args, field.Name, field.Name)
		} else {
			terms = append(terms, "field_name = ?")
			args = append(args, field.Name)
		}
	}
	for _, pattern := range patterns {
		if !spl.IsFieldsFieldGlob(pattern.Pattern) {
			return "", nil, errors.New("compile ClickHouse projection: invalid field wildcard")
		}
		parts := strings.Split(pattern.Pattern, "*")
		for index := range parts {
			parts[index] = regexp.QuoteMeta(parts[index])
		}
		term := "match(field_name, ?)"
		if !strings.HasPrefix(pattern.Pattern, "_") {
			term = "(NOT startsWith(field_name, '_') AND " + term + ")"
		}
		terms = append(terms, term)
		args = append(args, `(?s:\A(?:`+strings.Join(parts, `.*`)+`)\z)`)
	}
	if len(terms) == 0 {
		return "", nil, errors.New("compile ClickHouse projection: empty wildcard filter")
	}
	predicate := "(" + strings.Join(terms, " OR ") + ")"
	switch mode {
	case plan.ProjectModeInclude:
	case plan.ProjectModeExclude:
		predicate = "NOT " + predicate
	default:
		return "", nil, errors.New("compile ClickHouse projection: wildcard filter has invalid mode")
	}
	deniedTerms := make([]string, 0, len(deniedNames)+len(deniedPrefixes))
	for _, name := range deniedNames {
		deniedTerms = append(deniedTerms, "field_name = ?")
		args = append(args, name)
	}
	for _, prefix := range deniedPrefixes {
		deniedTerms = append(deniedTerms,
			"(field_name = ? OR startsWith(field_name, concat(?, '.')))",
		)
		args = append(args, prefix, prefix)
	}
	if len(deniedTerms) != 0 {
		predicate = "(" + predicate + ") AND NOT (" + strings.Join(deniedTerms, " OR ") + ")"
	}
	return predicate, args, nil
}

func projectRawPayloadDeniedNames(state compileState) []string {
	denied := make(map[string]struct{}, len(state.visible)+len(state.blocked))
	for name, field := range state.visible {
		if !field.rawEventDynamic {
			denied[name] = struct{}{}
		}
	}
	for name := range state.blocked {
		denied[name] = struct{}{}
	}
	return sortedSetValues(denied)
}

func replacePrivateFieldMetadataProjection(projection []string, predicate, relationAlias string) []string {
	names := quoteIdentifier(internalFieldNamesColumn)
	types := quoteIdentifier(internalFieldTypesColumn)
	sourceNames := relationAlias + "." + names
	sourceTypes := relationAlias + "." + types
	for index, term := range projection {
		switch term {
		case names:
			projection[index] = "arrayFilter((field_name, field_type) -> " + predicate +
				", " + sourceNames + ", " + sourceTypes + ") AS " + names
		case types:
			projection[index] = "arrayFilter((field_type, field_name) -> " + predicate +
				", " + sourceTypes + ", " + sourceNames + ") AS " + types
		}
	}
	return projection
}

func rewriteExistenceForProjection(field fieldState, name string) string {
	if field.existsSQL == "1" {
		return "1"
	}
	if isNativeMultivalueKind(field.kind) && strings.HasPrefix(field.existsSQL, "notEmpty(") {
		return "notEmpty(" + quoteIdentifier(name) + ")"
	}
	if strings.HasPrefix(field.existsSQL, "isNotNull(") {
		return "isNotNull(" + quoteIdentifier(name) + ")"
	}
	return field.existsSQL
}

type compiledExactScalarGroup struct {
	field           fieldState
	keySQL          string
	presenceSQL     string
	presenceArgs    []any
	unsupportedSQL  string
	unsupportedArgs []any
}

// compileExactScalarGroup resolves one stats-like BY field into the common SPL
// scalar grouping contract. Both transforming stats and row-preserving
// eventstats intentionally use the same missing/null eligibility, lexical
// Dynamic key, and descendant-aware unsupported-container check.
func compileDeduplicate(
	relation compiledRelation,
	operator *plan.Deduplicate,
	state compileState,
	stage int,
) (deduplicated compiledRelation, prefixArgs []any, currentOrder []compiledSortKey, additionalAliases int, err error) {
	if operator == nil || operator.Count == 0 || len(operator.Keys) == 0 || len(operator.Keys) > 16 {
		return compiledRelation{}, nil, nil, 0, errors.New("compile ClickHouse deduplicate: positive count and 1 through 16 keys are required")
	}

	materialized := make([]string, 0, len(operator.Keys)*3)
	presentColumns := make([]string, 0, len(operator.Keys))
	keyColumns := make([]string, 0, len(operator.Keys))
	invalidValues := make([]string, 0, len(operator.Keys))
	helperColumns := make([]string, 0, len(operator.Keys)*3+1)
	seen := make(map[string]struct{}, len(operator.Keys))
	for index, key := range operator.Keys {
		if state.eventRows && state.allowDynamic && key.Name == "fields" {
			return compiledRelation{}, nil, nil, 0, &plan.Diagnostic{
				Code:    "SPL_AMBIGUOUS_DEDUP_FIELD",
				Message: "dedup cannot use the event result's reserved fields payload without an exact upstream schema",
				Range:   key.Range,
			}
		}
		if _, duplicate := seen[key.Name]; duplicate {
			return compiledRelation{}, nil, nil, 0, fmt.Errorf("compile ClickHouse deduplicate: key %q is duplicated", key.Name)
		}
		seen[key.Name] = struct{}{}

		field, exists, resolveErr := resolveCompiledField(key, state)
		if resolveErr != nil {
			return compiledRelation{}, nil, nil, 0, resolveErr
		}
		if !exists {
			// A prior projection is authoritative. Keep a typed missing key so the
			// eligibility predicate removes every row without consulting the
			// private event document.
			field = fieldState{
				valueSQL:  "CAST(NULL AS Nullable(String))",
				existsSQL: "0",
				kind:      fieldKindString,
			}
		}
		if isNativeMultivalueKind(field.kind) {
			return compiledRelation{}, nil, nil, 0, unsupportedMultivalueUsage(
				"dedup",
				key.Range,
			)
		}

		presentName := quoteIdentifier(fmt.Sprintf("__os_dedup_present_%d_%d", stage, index))
		keyName := quoteIdentifier(fmt.Sprintf("__os_dedup_key_%d_%d", stage, index))
		present := "0"
		if exists {
			fieldExists := field.existsSQL
			if fieldExists == "" {
				fieldExists = "1"
			}
			present = "(" + fieldExists + " AND isNotNull(" + field.valueSQL + "))"
			prefixArgs = append(prefixArgs, field.existsArgs...)
			if field.kind == fieldKindDynamic && field.descendantSQL != "" {
				// Flattened non-empty objects do not have an exact parent leaf. Keep
				// them present until the whole-input unsupported-value guard runs.
				present = "(" + present + " OR " + field.descendantSQL + ")"
				prefixArgs = append(prefixArgs, field.descendantArgs...)
			}
		}
		materialized = append(materialized, "toUInt8("+present+") AS "+presentName)
		presentColumns = append(presentColumns, presentName)
		helperColumns = append(helperColumns, presentName)

		keyValue := field.valueSQL
		if field.kind == fieldKindDynamic {
			supported, lexical := statsByScalarExpressions(field)
			supportedName := quoteIdentifier(fmt.Sprintf("__os_dedup_supported_%d_%d", stage, index))
			materialized = append(materialized,
				"toUInt8("+supported+") AS "+supportedName,
				"if("+supported+", "+lexical+", '') AS "+keyName,
			)
			helperColumns = append(helperColumns, supportedName)
			invalidValues = append(invalidValues, presentName+" != 0 AND "+supportedName+" = 0")
		} else {
			materialized = append(materialized, keyValue+" AS "+keyName)
		}
		keyColumns = append(keyColumns, keyName)
		helperColumns = append(helperColumns, keyName)
	}

	preparedAlias := quoteIdentifier(fmt.Sprintf("_stage_%d", stage))
	preparedSQL := "SELECT *, " + strings.Join(materialized, ", ") + " FROM (" + relation.sql + ") AS " + preparedAlias
	deduplicated = relation.selectFrom(preparedSQL, operator.Range)
	eligible := make([]string, 0, len(presentColumns))
	for _, present := range presentColumns {
		eligible = append(eligible, present+" != 0")
	}
	predicate := "(" + strings.Join(eligible, " AND ") + ")"

	if len(invalidValues) > 0 {
		additionalAliases++
		validationAlias := quoteIdentifier(fmt.Sprintf("_stage_%d", stage+additionalAliases))
		anyUnsupported := quoteIdentifier(fmt.Sprintf("__os_dedup_any_unsupported_%d", stage))
		helperColumns = append(helperColumns, anyUnsupported)
		invalid := "(" + strings.Join(invalidValues, ") OR (") + ")"
		validationSQL := "SELECT *, max(CAST(" + invalid + " AS UInt8)) OVER () AS " + anyUnsupported +
			" FROM (" + deduplicated.sql + ") AS " + validationAlias
		deduplicated = deduplicated.selectFrom(validationSQL, operator.Range)
		// Put validation and eligibility in the two branches of one predicate.
		// The window flag is computed over the complete scoped input first, so an
		// unsupported key cannot be hidden by a missing value in another key.
		predicate = "if(" + anyUnsupported + " != 0, throwIf(toUInt8(1), '" + UnsupportedDedupValueMarker + "') = 0, " + predicate + ")"
	}

	currentOrder = defaultCompiledOrder(state)
	if len(currentOrder) == 0 {
		return compiledRelation{}, nil, nil, 0, errors.New("compile ClickHouse deduplicate: input has no deterministic order")
	}
	order, orderErr := compileMaterializedOrder(currentOrder, false)
	if orderErr != nil {
		return compiledRelation{}, nil, nil, 0, orderErr
	}
	limitBy := keyColumns
	if operator.Consecutive {
		// Consecutive mode retains the first Count rows of every run of equal
		// key tuples. Eligibility is applied before the window so a row missing
		// a key neither starts nor breaks a run, then each row learns whether
		// its tuple repeats the previous eligible row's, and a running count
		// of run starts labels the run that LIMIT BY bounds.
		window := " OVER (ORDER BY " + order + " ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)"
		repeated := make([]string, 0, len(keyColumns)+1)
		repeated = append(repeated, "lagInFrame(toUInt8(1), 1, toUInt8(0))"+window+" != 0")
		for _, key := range keyColumns {
			repeated = append(repeated, "ifNull("+key+" = lagInFrame("+key+", 1, "+key+")"+window+", 0)")
		}
		runStart := quoteIdentifier(fmt.Sprintf("__os_dedup_run_start_%d", stage))
		run := quoteIdentifier(fmt.Sprintf("__os_dedup_run_%d", stage))
		additionalAliases++
		startAlias := quoteIdentifier(fmt.Sprintf("_stage_%d", stage+additionalAliases))
		startSQL := "SELECT *, toUInt8(NOT (" + strings.Join(repeated, " AND ") + ")) AS " + runStart +
			" FROM (" + deduplicated.sql + ") AS " + startAlias + " WHERE " + predicate
		deduplicated = deduplicated.selectFrom(startSQL, operator.Range)
		additionalAliases++
		runAlias := quoteIdentifier(fmt.Sprintf("_stage_%d", stage+additionalAliases))
		runSQL := "SELECT *, sum(" + runStart + ")" + window + " AS " + run +
			" FROM (" + deduplicated.sql + ") AS " + runAlias
		deduplicated = deduplicated.selectFrom(runSQL, operator.Range)
		helperColumns = append(helperColumns, runStart, run)
		predicate = "1"
		limitBy = []string{run}
	}
	additionalAliases++
	dedupAlias := quoteIdentifier(fmt.Sprintf("_stage_%d", stage+additionalAliases))
	dedupSQL := "SELECT * EXCEPT (" + strings.Join(helperColumns, ", ") + ") FROM (" + deduplicated.sql + ") AS " + dedupAlias + " WHERE " + predicate +
		" ORDER BY " + order + " LIMIT ? BY " + strings.Join(limitBy, ", ")
	deduplicated = deduplicated.selectFrom(dedupSQL, operator.Range)
	return deduplicated, prefixArgs, currentOrder, additionalAliases, nil
}

func compileWindow(operator *plan.Window, state compileState) (string, compileState, error) {
	if operator == nil || operator.Output == "" {
		return "", compileState{}, errors.New("compile ClickHouse window: output field is required")
	}
	if _, exists := state.visible[operator.Output]; exists {
		return "", compileState{}, fmt.Errorf("compile ClickHouse window: output field %q is duplicated", operator.Output)
	}
	input, ok, err := resolveCompiledField(operator.Input, state)
	if err != nil {
		return "", compileState{}, err
	}
	if !ok || input.kind != fieldKindNumber {
		return "", compileState{}, fmt.Errorf("compile ClickHouse window: input field %q must be numeric", operator.Input.Name)
	}
	if operator.Function != plan.WindowFunctionPercentOfTotal {
		return "", compileState{}, fmt.Errorf("compile ClickHouse window: unsupported function %d", operator.Function)
	}

	// A partitioned total (top ... BY) scopes the percentage to rows sharing
	// the partition tuple; the tuple columns are aggregate group keys, so they
	// are always present on every row.
	partitions := make([]string, 0, len(operator.PartitionBy))
	for _, ref := range operator.PartitionBy {
		field, ok, err := resolveCompiledField(ref, state)
		if err != nil {
			return "", compileState{}, err
		}
		if !ok {
			return "", compileState{}, fmt.Errorf("compile ClickHouse window: partition field %q is not visible", ref.Name)
		}
		partitions = append(partitions, field.valueSQL)
	}
	over := "OVER ()"
	if len(partitions) > 0 {
		over = "OVER (PARTITION BY " + strings.Join(partitions, ", ") + ")"
	}

	// Aggregate groups always have a strictly positive count, so an empty input
	// produces no row on which division could occur. Cast before multiplication
	// to avoid integer overflow and retain the unrounded SPL percentage.
	total := "sum(" + input.valueSQL + ") " + over
	expression := "toFloat64(" + input.valueSQL + ") * 100.0 / toFloat64(" + total + ")"
	next := state
	next.visible = make(map[string]fieldState, len(state.visible)+1)
	for name, field := range state.visible {
		field.storedPath = field.storedPath.clone()
		next.visible[name] = field
	}
	next.publicOrder = append([]string(nil), state.publicOrder...)
	output := quoteIdentifier(operator.Output)
	next.visible[operator.Output] = fieldState{
		valueSQL: output, existsSQL: "1", kind: fieldKindNumber, numberType: "Float64",
	}
	next.publicOrder = append(next.publicOrder, operator.Output)
	return expression, next, nil
}

func compileSort(keys []plan.SortKey, state compileState, stage int) ([]string, []compiledSortKey, string, []any, error) {
	if len(keys) == 0 {
		return nil, nil, "", nil, errors.New("compile ClickHouse sort: no keys")
	}
	materialized := make([]string, 0, len(keys)+len(state.tieBreakers))
	compiled := make([]compiledSortKey, 0, len(keys)+len(state.tieBreakers))
	prefixArgs := make([]any, 0)
	explicitValues := make(map[string]struct{}, len(keys))
	for i, key := range keys {
		field, ok, err := resolveCompiledField(key.Field, state)
		if err != nil {
			return nil, nil, "", nil, err
		}
		if !ok {
			// SPL permits sorting by a field that is missing from every row. Use
			// one typed NULL key and retain the pipeline's stable row identity;
			// never resurrect event columns after a transforming command.
			field = fieldState{
				valueSQL:  "CAST(NULL AS Nullable(String))",
				existsSQL: "0",
				kind:      fieldKindString,
			}
		}
		explicitValues[field.valueSQL] = struct{}{}
		alias := fmt.Sprintf("__os_order_%d_%d", stage, i)
		presenceSQL, presenceArgs := sortFieldPresenceSQL(field)
		sortValue, compileErr := sortFieldValueSQL(field, key.Mode)
		if compileErr != nil {
			return nil, nil, "", nil, compileErr
		}
		prefixArgs = append(prefixArgs, presenceArgs...)
		materialized = append(materialized,
			"tuple(toUInt8(NOT ifNull("+presenceSQL+", 0)), "+sortValue+") AS "+quoteIdentifier(alias),
		)
		compiled = append(compiled, compiledSortKey{
			valueSQL: quoteIdentifier(alias), descending: key.Descending, separatePresence: true,
		})
	}
	// Preserve a stable row identity without assuming the input still consists
	// of events. Event pipelines use event_id; transforming pipelines use their
	// unique grouping tuple, and a global aggregate needs no tie-breaker.
	for index, tie := range state.tieBreakers {
		if _, explicit := explicitValues[tie.valueSQL]; explicit {
			continue
		}
		tieAlias := fmt.Sprintf("__os_order_%d_tie_%d", stage, index)
		materialized = append(materialized, tie.valueSQL+" AS "+quoteIdentifier(tieAlias))
		tie.valueSQL = quoteIdentifier(tieAlias)
		compiled = append(compiled, tie)
	}
	order, err := compileMaterializedOrder(compiled, false)
	if err != nil {
		return nil, nil, "", nil, err
	}
	return materialized, compiled, order, prefixArgs, nil
}

func equivalentSortOperators(left, right *plan.Sort) bool {
	if left == nil || right == nil || len(left.Keys) != len(right.Keys) {
		return false
	}
	for index := range left.Keys {
		leftKey := left.Keys[index]
		rightKey := right.Keys[index]
		if leftKey.Descending != rightKey.Descending || leftKey.Mode != rightKey.Mode ||
			leftKey.Field.Name != rightKey.Field.Name ||
			leftKey.Field.Canonical != rightKey.Field.Canonical ||
			!slices.Equal(leftKey.Field.Path, rightKey.Field.Path) {
			return false
		}
	}
	return true
}

func compileMaterializedOrder(keys []compiledSortKey, reverse bool) (string, error) {
	if len(keys) == 0 {
		return "", errors.New("compile ClickHouse sort: no keys")
	}
	parts := make([]string, 0, len(keys))
	for _, key := range keys {
		descending := key.descending
		nullsFirst := key.nullsFirst
		presenceDescending := key.presenceDescending
		if reverse {
			descending = !descending
			nullsFirst = !nullsFirst
			presenceDescending = !presenceDescending
		}
		direction := "ASC"
		if descending {
			direction = "DESC"
		}
		nulls := "NULLS LAST"
		if nullsFirst {
			nulls = "NULLS FIRST"
		}
		if key.separatePresence {
			presenceDirection := "ASC"
			if presenceDescending {
				presenceDirection = "DESC"
			}
			parts = append(parts,
				"tupleElement("+key.valueSQL+", 1) "+presenceDirection+" "+nulls,
				"tupleElement("+key.valueSQL+", 2) "+direction+" "+nulls,
			)
			continue
		}
		parts = append(parts, key.valueSQL+" "+direction+" "+nulls)
	}
	return strings.Join(parts, ", "), nil
}

func reverseCompiledSortKeys(keys []compiledSortKey) []compiledSortKey {
	result := make([]compiledSortKey, len(keys))
	for i, key := range keys {
		key.descending = !key.descending
		key.nullsFirst = !key.nullsFirst
		key.presenceDescending = !key.presenceDescending
		result[i] = key
	}
	return result
}

func stableCompiledSortKeys() []compiledSortKey {
	return []compiledSortKey{
		{valueSQL: quoteIdentifier(internalSortTimeColumn), descending: true},
		{valueSQL: quoteIdentifier(internalSortIDColumn), descending: true},
		{valueSQL: quoteIdentifier(internalSortVisibilityColumn), descending: true},
		{valueSQL: quoteIdentifier(internalSortSourceIdentityColumn), descending: true},
	}
}

func defaultCompiledOrder(state compileState) []compiledSortKey {
	if len(state.order) > 0 {
		return state.order
	}
	if state.eventRows {
		return stableCompiledSortKeys()
	}
	return state.tieBreakers
}

func finalProjection(state compileState) ([]string, []string, error) {
	projection := make([]string, 0, len(state.publicOrder))
	output := make([]string, 0, len(state.publicOrder))
	seen := make(map[string]struct{}, len(state.publicOrder))
	for _, name := range state.publicOrder {
		if _, exists := seen[name]; exists {
			continue
		}
		seen[name] = struct{}{}
		field, visible := state.visible[name]
		if name == "fields" && !visible && state.eventRows {
			projection = append(projection, quoteIdentifier(internalFieldsColumn)+" AS "+quoteIdentifier("fields"))
			output = append(output, name)
			continue
		}
		if !visible {
			continue
		}
		publicName := quoteIdentifier(name)
		projection = appendVisibleFieldProjection(projection, field, publicName)
		output = append(output, name)
	}
	if len(projection) == 0 {
		return nil, nil, errors.New("compile ClickHouse query: projection has no visible fields")
	}
	return projection, output, nil
}

func aliasPhysical(physical, alias string) string {
	return quoteIdentifier(physical) + " AS " + quoteIdentifier(alias)
}

func quoteIdentifier(identifier string) string {
	const hexadecimal = "0123456789ABCDEF"
	var quoted strings.Builder
	quoted.Grow(len(identifier) + 2)
	quoted.WriteByte('"')
	for index := 0; index < len(identifier); index++ {
		value := identifier[index]
		switch value {
		case '\\', '"', '?', '$', '{', '}':
			// clickhouse-go's legacy binder recognizes ?, $N, and {name:type}
			// without parsing SQL quoting. ClickHouse decodes hexadecimal escapes
			// inside quoted identifiers, so keep bind markers out of the client-side
			// query while preserving the exact server-visible column name.
			quoted.WriteString(`\x`)
			quoted.WriteByte(hexadecimal[value>>4])
			quoted.WriteByte(hexadecimal[value&0x0f])
		default:
			quoted.WriteByte(value)
		}
	}
	quoted.WriteByte('"')
	return quoted.String()
}

// quoteStringLiteral renders text as a single-quoted ClickHouse string literal
// for the few server functions that require a constant argument (trim
// characters). Values that can be bound stay bound; this is reserved for
// validated, size-bounded, valid UTF-8 text. Quotes, backslashes, driver bind
// metacharacters, and control bytes become \xHH escapes so the literal never
// closes early, never reads as a bind marker, and survives every SQL formatter.
func quoteStringLiteral(text string) string {
	const hexadecimal = "0123456789ABCDEF"
	var quoted strings.Builder
	quoted.Grow(len(text) + 2)
	quoted.WriteByte('\'')
	for index := 0; index < len(text); index++ {
		value := text[index]
		switch {
		case value == '\\', value == '\'', value == '?', value == '$', value == '{', value == '}',
			value < 0x20, value == 0x7f:
			quoted.WriteString(`\x`)
			quoted.WriteByte(hexadecimal[value>>4])
			quoted.WriteByte(hexadecimal[value&0x0f])
		default:
			quoted.WriteByte(value)
		}
	}
	quoted.WriteByte('\'')
	return quoted.String()
}

// isCanonicalQuotedIdentifierSQL reports whether value is exactly one valid
// UTF-8 quoted identifier emitted by quoteIdentifier. Compiler-visible authored
// names are validated UTF-8 and generated physical names are ASCII. It decodes
// hexadecimal escapes before round-tripping because quoteIdentifier deliberately
// represents driver bind metacharacters (including a literal backslash) as
// \xHH. Treating those escape bytes as the identifier itself would encode the
// backslash a second time.
//
// Requiring the canonical round trip also keeps arbitrary SQL expressions out:
// raw quotes, bind markers, noncanonical escapes, and trailing operators cannot
// be mistaken for a physical identifier that is safe to relation-qualify.
func isCanonicalQuotedIdentifierSQL(value string) bool {
	if len(value) < 2 || value[0] != '"' || value[len(value)-1] != '"' {
		return false
	}
	end := len(value) - 1
	identifier := make([]byte, 0, end-1)
	for index := 1; index < end; {
		if value[index] != '\\' {
			identifier = append(identifier, value[index])
			index++
			continue
		}
		if index+3 >= end || value[index+1] != 'x' {
			return false
		}
		high, highOK := hexadecimalDigitValue(value[index+2])
		low, lowOK := hexadecimalDigitValue(value[index+3])
		if !highOK || !lowOK {
			return false
		}
		identifier = append(identifier, high<<4|low)
		index += 4
	}
	return utf8.Valid(identifier) && quoteIdentifier(string(identifier)) == value
}

func hexadecimalDigitValue(value byte) (byte, bool) {
	switch {
	case value >= '0' && value <= '9':
		return value - '0', true
	case value >= 'A' && value <= 'F':
		return value - 'A' + 10, true
	case value >= 'a' && value <= 'f':
		return value - 'a' + 10, true
	default:
		return 0, false
	}
}

func wildcardRegex(value string, caseInsensitive bool) string {
	var result strings.Builder
	if caseInsensitive {
		result.WriteString("(?i)")
	}
	result.WriteByte('^')
	for _, r := range value {
		switch r {
		case '*':
			result.WriteString(".*")
		case '.', '+', '?', '(', ')', '[', ']', '{', '}', '^', '$', '|', '\\':
			result.WriteByte('\\')
			result.WriteRune(r)
		default:
			result.WriteRune(r)
		}
	}
	result.WriteByte('$')
	return result.String()
}

func freeTextRegex(value string, quoted bool) string {
	var result strings.Builder
	result.WriteString("(?i)")
	if !quoted {
		result.WriteString("(?:^|[^[:alnum:]_])")
	}
	for _, r := range value {
		if r == '*' {
			if quoted {
				result.WriteString(".*")
			} else {
				result.WriteString("[[:alnum:]_]*")
			}
			continue
		}
		if strings.ContainsRune(`.+?()[]{}^$|\\`, r) {
			result.WriteByte('\\')
		}
		result.WriteRune(r)
	}
	if !quoted {
		result.WriteString("(?:$|[^[:alnum:]_])")
	}
	return result.String()
}

// rawTextIndexTokenEligible intentionally starts with the narrow query shape
// whose exact-regex matches necessarily contain one indexed ASCII-alphanumeric
// token. The regex remains authoritative; broader Unicode, phrase, wildcard,
// and punctuation forms stay on the exact scan path until their tokenizer
// parity is proven.
func rawTextIndexTokenEligible(value string) bool {
	if value == "" {
		return false
	}
	for index := 0; index < len(value); index++ {
		character := value[index]
		if character >= 'a' && character <= 'z' ||
			character >= 'A' && character <= 'Z' ||
			character >= '0' && character <= '9' {
			continue
		}
		return false
	}
	return true
}

// rawTextIndexTokensSQL must stay expression-identical to idx_raw_text. RE2's
// Unicode simple-fold orbit has two non-ASCII members reachable from ASCII:
// long s folds with s and the Kelvin sign folds with k. Translating those two
// before extracting maximal ASCII alnum/underscore runs preserves both match
// and boundary semantics. Only the resulting ASCII tokens are lowercased, so
// Unicode lowercase expansions cannot join otherwise separate runs.
func rawTextIndexTokensSQL(valueSQL string) string {
	return "arrayMap(token -> lower(token), extractAll(translateUTF8(" + valueSQL +
		", '" + rawTextIndexASCIIFoldFrom + "', '" + rawTextIndexASCIIFoldTo +
		"'), '" + rawTextIndexExtractionRegex + "'))"
}

func cloneSet(source map[string]struct{}) map[string]struct{} {
	result := make(map[string]struct{}, len(source))
	for key := range source {
		result[key] = struct{}{}
	}
	return result
}
