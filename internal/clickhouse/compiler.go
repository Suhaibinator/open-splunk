package clickhouse

import (
	"context"
	"errors"
	"fmt"
	"math"
	"net/netip"
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
	TimechartCountColumn   = "__os_timechart_count"
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
	Mode          TimechartMode
	FirstBucket   time.Time
	Span          time.Duration
	BucketCount   uint64
	MaxSeries     uint16
	MaxLabelBytes uint16
	// ValueKind is populated for both fixed and runtime-wide nullable values.
	// Together ValueField and ValueKind bind each private transport to its
	// aggregate validation policy instead of trusting mutable OutputFields alone.
	ValueField string
	ValueKind  TimechartValueKind
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

func (c Compiler) compileWithFinalizerContext(
	ctx context.Context,
	query *plan.Query,
	finalize queryFinalizer,
	permitTerminalWideOperators bool,
) (CompiledQuery, error) {
	if ctx == nil {
		return CompiledQuery{}, errors.New("compile ClickHouse query: context is nil")
	}
	if err := ctx.Err(); err != nil {
		return CompiledQuery{}, err
	}
	if query == nil || len(query.Operators) == 0 {
		return CompiledQuery{}, errors.New("compile ClickHouse query: logical plan is empty")
	}
	if finalize == nil {
		return CompiledQuery{}, errors.New("compile ClickHouse query: finalizer is required")
	}
	database := c.Database
	if database == "" {
		database = "open_splunk"
	}
	table := c.Table
	if table == "" {
		table = "events"
	}
	if !physicalIdentifier.MatchString(database) || !physicalIdentifier.MatchString(table) {
		return CompiledQuery{}, errors.New("compile ClickHouse query: database and table must be simple identifiers")
	}
	if isNilPlanOperator(query.Operators[0]) {
		return CompiledQuery{}, errors.New("compile ClickHouse query: first operator must be a non-nil Scan")
	}
	scan, ok := query.Operators[0].(*plan.Scan)
	if !ok {
		return CompiledQuery{}, errors.New("compile ClickHouse query: first operator must be Scan")
	}
	preparation, err := prepareKnowledgeCompilation(query)
	if err != nil {
		return CompiledQuery{}, err
	}
	lookupPreparation, err := prepareLookupCompilationContext(
		ctx,
		query,
		scan,
		c.lookupResolutions,
	)
	if err != nil {
		return CompiledQuery{}, err
	}
	fragment, state, args, err := compileScan(
		database,
		table,
		scan,
		query.SearchStart,
		query.SearchTimezone,
	)
	if err != nil {
		return CompiledQuery{}, err
	}
	if state.context == nil {
		return CompiledQuery{}, errors.New("compile ClickHouse query: compile context is unavailable")
	}
	state.context.operationContext = ctx
	relation := newScanRelation(fragment, scan.Range)
	knowledge, err := compileDeferredKnowledgeRelation(
		relation,
		state,
		args,
		preparation,
		lookupPreparation.automatic,
	)
	if err != nil {
		return CompiledQuery{}, err
	}
	relation = knowledge.relation
	state = knowledge.state
	args = knowledge.args

	aliasSequence := 0
	lookupStageIndex := 0
	var statsPartitionsMaxThreadsHint uint8
	finishCompiled := func(
		compiled CompiledQuery,
		complexityRange spl.Range,
	) (CompiledQuery, error) {
		compiled.atomicResult = state.context != nil && state.context.atomicResult
		terminalWide := compiled.Chart != nil || compiled.Timechart != nil
		if terminalWide && len(state.chronologicalBarriers) > 0 {
			var wrapErr error
			compiled, wrapErr = wrapCompiledChronologicalValidation(
				compiled,
				state,
				aliasSequence,
			)
			if wrapErr != nil {
				return CompiledQuery{}, wrapErr
			}
		}
		if terminalWide {
			if depthErr := validateCompiledRelationalDepth(compiled); depthErr != nil {
				return CompiledQuery{}, depthErr
			}
		} else if depthErr := validateFinalizedRelationalDepth(relation, compiled); depthErr != nil {
			return CompiledQuery{}, depthErr
		}
		if state.context != nil && state.context.requiresMaterializedValidationSettings {
			compiled.SQL = applyMaterializedValidationSettings(compiled.SQL)
		}
		compiled.statsPartitionsMaxThreadsHint = statsPartitionsMaxThreadsHint
		if state.context != nil {
			compiled.lookupTables = cloneCompiledLookupExternalTables(
				state.context.lookupTables,
			)
		}
		if len(compiled.SQL) > maxCompiledQueryBytes {
			return CompiledQuery{}, &plan.Diagnostic{
				Code:    "SPL_QUERY_TOO_COMPLEX",
				Message: fmt.Sprintf("compiled query exceeds %d bytes", maxCompiledQueryBytes),
				Range:   complexityRange,
			}
		}
		return sealFinalCompiledQueryContext(
			ctx,
			compiled,
			query,
			scan,
			preparation,
			knowledge.prelude,
			lookupPreparation,
		)
	}
	remainingStart := 1 + preparation.prefixLength
	if lookupPreparation.automatic != nil {
		if remainingStart >= len(query.Operators) ||
			query.Operators[remainingStart] != lookupPreparation.automatic.operator {
			return CompiledQuery{}, errors.New(
				"compile ClickHouse automatic lookups: prepared group order disagrees with the logical plan",
			)
		}
		remainingStart++
	}
	remainingOperators := query.Operators[remainingStart:]
	for operatorIndex := 0; operatorIndex < len(remainingOperators); operatorIndex++ {
		operator := remainingOperators[operatorIndex]
		if isNilPlanOperator(operator) {
			return CompiledQuery{}, fmt.Errorf(
				"compile ClickHouse query: operator %d is nil",
				operatorIndex+1+preparation.prefixLength,
			)
		}
		aliasSequence++
		alias := quoteIdentifier(fmt.Sprintf("_stage_%d", aliasSequence))
		switch operator := operator.(type) {
		case *plan.Filter:
			if complexityErr := validateCompiledPredicateComplexity(operator.Expression); complexityErr != nil {
				return CompiledQuery{}, complexityErr
			}
			materializedFields := predicateMaterializationFields(operator.Expression, state)
			exactNumericFields := repeatedExactNumericPredicateFields(
				operator.Expression,
				state,
			)
			predicateState := state
			nextState := state
			var excludedColumns, replacements, bindings []string
			if len(materializedFields) > 0 {
				predicateState, nextState, excludedColumns, replacements, bindings = bindMaterializedPredicateFields(
					state,
					materializedFields,
					aliasSequence,
				)
			}
			exactNumericAliases := make([]string, 0, len(exactNumericFields)*2)
			if len(exactNumericFields) > 0 {
				var keyColumns []string
				predicateState, keyColumns, exactNumericAliases, err = bindExactNumericPredicateFields(
					predicateState,
					exactNumericFields,
					aliasSequence,
					"filter",
				)
				if err != nil {
					return CompiledQuery{}, err
				}
				relation = relation.selectFrom(
					"SELECT *, "+strings.Join(keyColumns, ", ")+" FROM ("+
						relation.sql+") AS "+alias,
					operator.Range,
				)
				aliasSequence++
				alias = quoteIdentifier(fmt.Sprintf("_stage_%d", aliasSequence))
			}
			predicate, predicateArgs, compileErr := compileFilterExpression(
				operator.Expression,
				predicateState,
			)
			if compileErr != nil {
				return CompiledQuery{}, compileErr
			}
			filterProjection := "*"
			if len(exactNumericAliases) > 0 {
				filterProjection = "* EXCEPT (" +
					strings.Join(exactNumericAliases, ", ") + ")"
			}
			filterSQL := "SELECT " + filterProjection + " FROM (" +
				relation.sql + ") AS " + alias + " WHERE " + predicate
			if len(materializedFields) > 0 {
				materialized := quoteIdentifier(fmt.Sprintf("__os_filter_input_%d", aliasSequence))
				privateColumns := append(
					append([]string(nil), excludedColumns...),
					exactNumericAliases...,
				)
				filterSQL = "WITH " + materialized + " AS MATERIALIZED (" + relation.sql + ") " +
					"SELECT * EXCEPT (" + strings.Join(privateColumns, ", ") + ") REPLACE (" +
					strings.Join(replacements, ", ") + ") FROM " + materialized + " AS " +
					alias + " ARRAY JOIN " +
					strings.Join(bindings, ", ") + " WHERE " + predicate +
					materializedCTESettingsSQL
			}
			relation = relation.selectFrom(filterSQL, operator.Range)
			args = append(args, predicateArgs...)
			state = nextState
		case *plan.RegexFilter:
			predicate, predicateArgs, compileErr := compileRegexFilter(operator, state)
			if compileErr != nil {
				return CompiledQuery{}, compileErr
			}
			relation = relation.selectFrom(
				"SELECT * FROM ("+relation.sql+") AS "+alias+" WHERE "+predicate,
				operator.Range,
			)
			args = append(args, predicateArgs...)
		case *plan.Project:
			projection, nextState, projectionArgs, compileErr := compileProjection(operator, state, alias, aliasSequence)
			if compileErr != nil {
				return CompiledQuery{}, compileErr
			}
			relation = relation.selectFrom(
				"SELECT "+strings.Join(projection, ", ")+" FROM ("+relation.sql+") AS "+alias,
				operator.Range,
			)
			// Projection expressions precede their nested input relation in SQL,
			// so their bind values precede every already-compiled input argument.
			args = append(projectionArgs, args...)
			state = nextState
		case *plan.Extend:
			if len(operator.Assignments) == 0 {
				return CompiledQuery{}, errors.New("compile ClickHouse extend: no assignments")
			}
			if len(operator.Assignments) > maxCompiledAssignments {
				return CompiledQuery{}, &plan.Diagnostic{
					Code:    "SPL_QUERY_TOO_COMPLEX",
					Message: fmt.Sprintf("eval contains more than %d assignments", maxCompiledAssignments),
					Range:   operator.Range,
				}
			}
			for index, assignment := range operator.Assignments {
				if complexityErr := validateCompiledScalarComplexity(assignment.Expression); complexityErr != nil {
					return CompiledQuery{}, complexityErr
				}
				if scalarExpressionMayReturnBooleanFunction(assignment.Expression) {
					return CompiledQuery{}, errors.New(
						"compile ClickHouse extend: eval cannot directly assign a Boolean result",
					)
				}
				value, compileErr := compileScalarValue(assignment.Expression, state)
				if compileErr != nil {
					return CompiledQuery{}, compileErr
				}
				// A native-MV guard can be nested under arithmetic, conditionals, or
				// another scalar wrapper that changes the result kind. Keep the
				// assignment's validation fence based on the authored expression tree,
				// not only on the outer compiled scalar's traits.
				if scalarExpressionRequiresNativeMVValidation(assignment.Expression, state) {
					value.requiresRuntimeValidation = true
				}
				prefixArgs := append([]any(nil), value.valueArgs...)
				semanticAlias := ""
				multivalueStateAlias := ""
				nextSQL := ""
				if value.kind == fieldKindString && value.stringOrBytes {
					if value.semanticBytesSQL == "" {
						return CompiledQuery{}, errors.New(
							"compile ClickHouse extend: String-or-Bytes value lacks semantic Bytes provenance",
						)
					}
					semanticAlias = quoteIdentifier(fmt.Sprintf(
						"__os_string_or_bytes_%d_%d",
						aliasSequence,
						index,
					))
					nextSQL = upsertFieldProjectionWithPrivateSQL(
						relation.sql,
						state,
						assignment.Output.Name,
						value.valueSQL,
						"toUInt8(ifNull("+value.semanticBytesSQL+", 0)) AS "+semanticAlias,
						alias,
					)
					prefixArgs = append(prefixArgs, value.semanticBytesArgs...)
					value.semanticBytesSQL = semanticAlias
					value.semanticBytesArgs = nil
				} else if value.optionalMultivaluePresentSQL != "" {
					// Native eval functions can produce a present-empty list, so
					// presence cannot be reconstructed from notEmpty(output). Seal
					// the authored-input predicate beside the calculated array in the
					// same projection, before an assignment that overwrites its input
					// field could make the predicate self-referential.
					multivalueStateAlias = quoteIdentifier(fmt.Sprintf(
						"__os_eval_mv_state_%d_%d",
						aliasSequence,
						index,
					))
					nextSQL = upsertFieldProjectionWithPrivateSQL(
						relation.sql,
						state,
						assignment.Output.Name,
						value.valueSQL,
						"tuple(toUInt8(ifNull("+value.existsSQL+", 0)), "+
							"toUInt8(ifNull("+value.optionalMultivaluePresentSQL+", 0))) AS "+multivalueStateAlias,
						alias,
					)
					prefixArgs = append(prefixArgs, value.existsArgs...)
					prefixArgs = append(prefixArgs, value.existsArgs...)
					value.existsSQL = "tupleElement(" + multivalueStateAlias + ", 1) != 0"
					value.optionalMultivaluePresentSQL = "tupleElement(" + multivalueStateAlias + ", 2) != 0"
					value.existsArgs = nil
				} else {
					nextSQL = upsertFieldProjectionSQL(
						relation.sql,
						state,
						assignment.Output.Name,
						value.valueSQL,
						alias,
					)
				}
				if value.requiresRuntimeValidation {
					validationInput := quoteIdentifier(fmt.Sprintf(
						"__os_eval_mv_validation_%d_%d",
						aliasSequence,
						index,
					))
					validationAlias := quoteIdentifier(fmt.Sprintf(
						"_stage_%d_eval_mv_validation_%d",
						aliasSequence,
						index,
					))
					nextSQL = "WITH " + validationInput + " AS MATERIALIZED (" +
						nextSQL + ") SELECT * FROM " + validationInput + " AS " +
						validationAlias + " WHERE ignore(" +
						quoteIdentifier(assignment.Output.Name) + ") = 0"
					if state.context != nil {
						state.context.atomicResult = true
						state.context.requiresMaterializedValidationSettings = true
					}
				}
				relation = relation.selectFrom(nextSQL, operator.Range)
				if err := validateRelationalDepth(relation.depth, relation.ownerRange); err != nil {
					return CompiledQuery{}, err
				}
				// Extend is emitted in an outer SELECT, so its placeholders occur
				// before every placeholder already present in the nested fragment.
				// Sequential assignments add another outer SELECT and therefore
				// prepend in reverse nesting order as well.
				args = prependArguments(prefixArgs, args)
				_, directField := assignment.Expression.(*plan.ScalarFieldExpression)
				extendState := state
				if multivalueStateAlias != "" {
					extendState.privateColumns = append(
						append([]string(nil), state.privateColumns...),
						multivalueStateAlias,
					)
				}
				nextState, stateErr := extendCompileState(
					extendState,
					assignment.Output,
					value,
					directField,
				)
				if stateErr != nil {
					return CompiledQuery{}, stateErr
				}
				state = nextState
				if index+1 < len(operator.Assignments) {
					aliasSequence++
					alias = quoteIdentifier(fmt.Sprintf("_stage_%d", aliasSequence))
				}
			}
		case *plan.Strcat:
			enriched, nextState, prefixArgs, compileErr := compileStrcat(
				relation,
				operator,
				state,
				alias,
				aliasSequence,
			)
			if compileErr != nil {
				return CompiledQuery{}, compileErr
			}
			relation = enriched
			args = prependArguments(prefixArgs, args)
			state = nextState
		case *plan.FillNull:
			enriched, nextState, prefixArgs, compileErr := compileFillNull(
				relation,
				operator,
				state,
				aliasSequence,
			)
			if compileErr != nil {
				return CompiledQuery{}, compileErr
			}
			relation = enriched
			args = prependArguments(prefixArgs, args)
			state = nextState
		case *plan.RowTotal:
			enriched, nextState, prefixArgs, barrier, compileErr := compileRowTotal(
				relation,
				operator,
				state,
				aliasSequence,
			)
			if compileErr != nil {
				return CompiledQuery{}, compileErr
			}
			relation = enriched
			args = prependArguments(prefixArgs, args)
			nextState, args = bindChronologicalBarrier(nextState, barrier, args)
			state = nextState
		case *plan.OrderedDelta:
			enriched, nextState, prefixArgs, barrier, compileErr := compileOrderedDelta(
				relation,
				operator,
				state,
				aliasSequence,
			)
			if compileErr != nil {
				return CompiledQuery{}, compileErr
			}
			relation = enriched
			args = prependArguments(prefixArgs, args)
			nextState, args = bindChronologicalBarrier(nextState, barrier, args)
			state = nextState
		case *plan.MakeMultivalue:
			enriched, nextState, prefixArgs, compileErr := compileMakeMultivalue(
				relation,
				operator,
				state,
				aliasSequence,
			)
			if compileErr != nil {
				return CompiledQuery{}, compileErr
			}
			relation = enriched
			args = prependArguments(prefixArgs, args)
			state = nextState
		case *plan.ExpandMultivalue:
			enriched, nextState, prefixArgs, compileErr := compileExpandMultivalue(
				relation,
				operator,
				state,
				aliasSequence,
			)
			if compileErr != nil {
				return CompiledQuery{}, compileErr
			}
			relation = enriched
			args = prependArguments(prefixArgs, args)
			state = nextState
		case *plan.NoMultivalue:
			presented, nextState, prefixArgs, compileErr := compileNoMultivalue(
				relation,
				operator,
				state,
				aliasSequence,
			)
			if compileErr != nil {
				return CompiledQuery{}, compileErr
			}
			relation = presented
			args = prependArguments(prefixArgs, args)
			state = nextState
		case *plan.TimeBucket:
			bucketed, nextState, prefixArgs, compileErr := compileTimeBucket(
				relation,
				state,
				scan,
				operator,
				alias,
			)
			if compileErr != nil {
				return CompiledQuery{}, compileErr
			}
			relation = bucketed
			args = prependArguments(prefixArgs, args)
			state = nextState
		case *plan.NumericBucket:
			bucketed, nextState, prefixArgs, compileErr := compileNumericBucket(
				relation,
				state,
				operator,
				alias,
				aliasSequence,
			)
			if compileErr != nil {
				return CompiledQuery{}, compileErr
			}
			relation = bucketed
			args = prependArguments(prefixArgs, args)
			state = nextState
		case *plan.Extract:
			extracted, nextState, prefixArgs, additionalAliases, compileErr := compileExtract(
				relation,
				operator,
				state,
				aliasSequence,
			)
			if compileErr != nil {
				return CompiledQuery{}, compileErr
			}
			relation = extracted
			args = prependArguments(prefixArgs, args)
			state = nextState
			aliasSequence += additionalAliases
		case *plan.ExtractJSON:
			extracted, nextState, prefixArgs, additionalAliases, compileErr := compileExtractJSON(
				relation,
				operator,
				state,
				aliasSequence,
			)
			if compileErr != nil {
				return CompiledQuery{}, compileErr
			}
			relation = extracted
			args = prependArguments(prefixArgs, args)
			state = nextState
			aliasSequence += additionalAliases
		case *plan.Rename:
			if len(operator.Assignments) == 0 {
				return CompiledQuery{}, errors.New("compile ClickHouse rename: no assignments")
			}
			if len(operator.Assignments) > maxCompiledAssignments {
				return CompiledQuery{}, &plan.Diagnostic{
					Code:    "SPL_QUERY_TOO_COMPLEX",
					Message: fmt.Sprintf("rename contains more than %d assignments", maxCompiledAssignments),
					Range:   operator.Range,
				}
			}
			seenSources := make(map[string]struct{}, len(operator.Assignments))
			seenDestinations := make(map[string]struct{}, len(operator.Assignments))
			for index, assignment := range operator.Assignments {
				if assignment.Source.Name == assignment.Destination.Name {
					return CompiledQuery{}, errors.New("compile ClickHouse rename: source and destination must differ")
				}
				if _, duplicate := seenSources[assignment.Source.Name]; duplicate {
					return CompiledQuery{}, errors.New("compile ClickHouse rename: source field is repeated")
				}
				if _, duplicate := seenDestinations[assignment.Destination.Name]; duplicate {
					return CompiledQuery{}, errors.New("compile ClickHouse rename: destination field is repeated")
				}
				seenSources[assignment.Source.Name] = struct{}{}
				seenDestinations[assignment.Destination.Name] = struct{}{}
				if index > 0 {
					aliasSequence++
					alias = quoteIdentifier(fmt.Sprintf("_stage_%d", aliasSequence))
				}
				projection, nextState, changed, compileErr := compileRenameAssignment(assignment, state)
				if compileErr != nil {
					return CompiledQuery{}, compileErr
				}
				state = nextState
				if changed {
					relation = relation.selectFrom(
						"SELECT "+strings.Join(projection, ", ")+" FROM ("+relation.sql+") AS "+alias,
						operator.Range,
					)
					if err := validateRelationalDepth(relation.depth, relation.ownerRange); err != nil {
						return CompiledQuery{}, err
					}
				}
			}
		case *plan.Lookup:
			if lookupStageIndex >= len(lookupPreparation.stages) ||
				lookupPreparation.stages[lookupStageIndex].sourceOperator != operator ||
				!lookupResolutionContractsEqual(
					*lookupPreparation.stages[lookupStageIndex].operator,
					*operator,
				) {
				return CompiledQuery{}, errors.New(
					"compile ClickHouse lookup: prepared stage order disagrees with the logical plan",
				)
			}
			var additionalAliases int
			relation, state, args, additionalAliases, err = compileLookupStage(
				relation,
				state,
				args,
				lookupPreparation.stages[lookupStageIndex],
				aliasSequence,
			)
			if err != nil {
				return CompiledQuery{}, err
			}
			lookupStageIndex++
			aliasSequence += additionalAliases
		case *plan.Aggregate:
			if cardinalityErr := validateAggregateCardinality(operator); cardinalityErr != nil {
				return CompiledQuery{}, cardinalityErr
			}
			if validateErr := validateAggregatePredicateMeasures(operator, state); validateErr != nil {
				return CompiledQuery{}, validateErr
			}
			// Aggregate is also the internal plan node used by top and rare.
			// Parser-built stats always carries effective StatsOptions, so that
			// pointer is the trust-boundary discriminator for the command-scoped
			// execution hint. Nil legacy/internal aggregates must not serialize
			// unrelated query stages.
			if operator.StatsOptions != nil {
				stageThreadHint, hintErr := effectiveStatsPartitionsMaxThreadsHint(operator)
				if hintErr != nil {
					return CompiledQuery{}, hintErr
				}
				statsPartitionsMaxThreadsHint = mergeStatsPartitionsMaxThreadsHint(
					statsPartitionsMaxThreadsHint,
					stageThreadHint,
				)
			}
			materializedFields := aggregatePredicateMaterializationFields(operator, state)
			if len(materializedFields) > 0 {
				var bindings, boundColumns []string
				state, bindings, boundColumns = bindAggregatePredicateFields(
					state,
					materializedFields,
					aliasSequence,
				)
				materialized := quoteIdentifier(fmt.Sprintf(
					"__os_stats_predicate_input_%d",
					aliasSequence,
				))
				relation = relation.selectFrom(
					"WITH "+materialized+" AS MATERIALIZED ("+relation.sql+") "+
						"SELECT *, "+strings.Join(boundColumns, ", ")+" FROM "+
						materialized+" AS "+alias+" ARRAY JOIN "+
						strings.Join(bindings, ", ")+materializedCTESettingsSQL,
					operator.Range,
				)
				aliasSequence++
				alias = quoteIdentifier(fmt.Sprintf("_stage_%d", aliasSequence))
			}
			projection, predicates, groups, nextState, aggregateArgs, compileErr := compileAggregateValidated(
				operator,
				state,
			)
			if compileErr != nil {
				return CompiledQuery{}, compileErr
			}
			if len(nextState.preAggregateValidationColumns) > 0 {
				// Materialize whole-input validation windows before filtering incomplete
				// group tuples. Otherwise a missing sibling key could hide an
				// unsupported container value.
				relation = relation.selectFrom(
					"SELECT *, "+strings.Join(nextState.preAggregateValidationColumns, ", ")+" FROM ("+relation.sql+") AS "+alias,
					operator.Range,
				)
				args = prependArguments(nextState.preAggregateValidationArgs, args)

				aliasSequence++
				alias = quoteIdentifier(fmt.Sprintf("_stage_%d", aliasSequence))
				nextState.preAggregateValidationColumns = nil
				nextState.preAggregateValidationArgs = nil
			}
			if len(predicates) > 0 {
				// Keep validation and missing/null elimination in a distinct
				// pre-aggregation scope after whole-input flags are materialized.
				relation = relation.selectFrom(
					"SELECT * FROM ("+relation.sql+") AS "+alias+" WHERE "+strings.Join(predicates, " AND "),
					operator.Range,
				)
				aliasSequence++
				alias = quoteIdentifier(fmt.Sprintf("_stage_%d", aliasSequence))
			}
			if len(nextState.preAggregateColumns) > 0 {
				// Materialize grouping keys and numeric measure inputs only after
				// sparse group tuples have been discarded.
				relation = relation.selectFrom(
					"SELECT *, "+strings.Join(nextState.preAggregateColumns, ", ")+" FROM ("+relation.sql+") AS "+alias,
					operator.Range,
				)
				args = prependArguments(nextState.preAggregateArgs, args)
				aliasSequence++
				alias = quoteIdentifier(fmt.Sprintf("_stage_%d", aliasSequence))
				nextState.preAggregateColumns = nil
				nextState.preAggregateArgs = nil
			}
			if len(nextState.preAggregateGroupExpansions) > 0 {
				if nextState.context == nil {
					return CompiledQuery{}, errors.New(
						"compile ClickHouse aggregate: multivalue validation context is missing",
					)
				}
				// The Cartesian guard is runtime data-dependent for both raw Dynamic
				// and fixed Array(String) inputs. A later backend row can therefore
				// select the expansion marker after earlier groups were produced.
				nextState.context.atomicResult = true
				expansionProduct, guardErr := statsMultivalueByExpansionProductSQL(
					nextState.preAggregateGroupExpansions,
				)
				if guardErr != nil {
					return CompiledQuery{}, guardErr
				}
				productAlias := quoteIdentifier("__os_stats_mv_by_combinations")
				anyOverLimitAlias := quoteIdentifier("__os_stats_mv_by_any_over_limit")
				maximum := statsMultivalueByExpansionMaximumSQL()
				// Freeze both the row-local product and a whole-eligible-input
				// violation bit before the first BY ARRAY JOIN. The window flag
				// makes one violating source event poison every retained row, so
				// downstream LIMIT or optimizer consumption cannot hide it.
				relation = relation.selectFrom(
					"SELECT *, "+expansionProduct+" AS "+productAlias+", "+
						"max(toUInt8(("+expansionProduct+") > "+maximum+")) OVER () AS "+
						anyOverLimitAlias+" FROM ("+relation.sql+") AS "+alias,
					operator.Range,
				)
				aliasSequence++
				alias = quoteIdentifier(fmt.Sprintf("_stage_%d", aliasSequence))
				expansionGuard, guardErr := statsMultivalueByExpansionGuardSQL(
					productAlias,
					anyOverLimitAlias,
				)
				if guardErr != nil {
					return CompiledQuery{}, guardErr
				}
				relation = relation.selectFrom(
					"SELECT * EXCEPT ("+productAlias+", "+anyOverLimitAlias+") FROM ("+
						relation.sql+") AS "+alias+" WHERE "+expansionGuard,
					operator.Range,
				)
				aliasSequence++
				alias = quoteIdentifier(fmt.Sprintf("_stage_%d", aliasSequence))
				// Expand one BY field per relational stage. ClickHouse's comma form
				// zips arrays by position; staged ARRAY JOINs instead produce the SPL
				// Cartesian product for multiple multivalue grouping fields.
				for _, expansion := range nextState.preAggregateGroupExpansions {
					relation = relation.selectFrom(
						"SELECT *, "+expansion.valueAlias+" FROM ("+relation.sql+") AS "+alias+" ARRAY JOIN "+
							expansion.valuesAlias+" AS "+expansion.valueAlias,
						operator.Range,
					)
					aliasSequence++
					alias = quoteIdentifier(fmt.Sprintf("_stage_%d", aliasSequence))
				}
				nextState.preAggregateGroupExpansions = nil
			}
			if len(nextState.preAggregateSparklineWindows) > 0 {
				// Sparkline bins partition the already-expanded stats BY domain. Keep
				// their window state in a separate relation before the ordinary outer
				// aggregate so scalar measures and time series can coexist in one row.
				relation = relation.selectFrom(
					"SELECT *, "+strings.Join(nextState.preAggregateSparklineWindows, ", ")+" FROM ("+relation.sql+") AS "+alias,
					operator.Range,
				)
				aliasSequence++
				alias = quoteIdentifier(fmt.Sprintf("_stage_%d", aliasSequence))
				nextState.preAggregateSparklineWindows = nil
			}
			if len(nextState.preAggregateListWindowColumns) > 0 {
				// Bound list() input bytes before aggregation. Per-input prefix
				// windows establish each value's position and cumulative payload
				// within its BY group without expanding event rows.
				relation = relation.selectFrom(
					"SELECT *, "+strings.Join(nextState.preAggregateListWindowColumns, ", ")+" FROM ("+relation.sql+") AS "+alias,
					operator.Range,
				)
				aliasSequence++
				alias = quoteIdentifier(fmt.Sprintf("_stage_%d", aliasSequence))
				nextState.preAggregateListWindowColumns = nil
			}
			if len(nextState.preAggregateListCandidateColumns) > 0 {
				// Freeze the already-bounded candidate arrays and their tiny
				// overflow flags before the aggregate. This prevents repeated
				// evaluation and keeps every partial list state byte-bounded.
				relation = relation.selectFrom(
					"SELECT *, "+strings.Join(nextState.preAggregateListCandidateColumns, ", ")+" FROM ("+relation.sql+") AS "+alias,
					operator.Range,
				)
				aliasSequence++
				alias = quoteIdentifier(fmt.Sprintf("_stage_%d", aliasSequence))
				nextState.preAggregateListCandidateColumns = nil
			}
			aggregateSQL := "SELECT " + strings.Join(projection, ", ") + " FROM (" + relation.sql + ") AS " + alias
			if len(groups) > 0 {
				aggregateSQL += " GROUP BY " + strings.Join(groups, ", ")
			}
			relation = relation.selectFrom(aggregateSQL, operator.Range)
			args = append(args, aggregateArgs...)
			if len(nextState.postAggregateSparklines) > 0 {
				var additionalAliases int
				relation, additionalAliases, compileErr = compileStatsSparklineResults(
					relation,
					nextState.postAggregateSparklines,
					operator.Range,
					aliasSequence,
				)
				if compileErr != nil {
					return CompiledQuery{}, compileErr
				}
				aliasSequence += additionalAliases
				nextState.postAggregateSparklines = nil
			}
			if len(nextState.postAggregateChronological) > 0 {
				var additionalAliases int
				var barrier *pendingChronologicalBarrier
				relation, additionalAliases, barrier = compileChronologicalResults(
					relation,
					nextState.postAggregateChronological,
					operator.Range,
					aliasSequence,
				)
				aliasSequence += additionalAliases
				nextState, args = bindChronologicalBarrier(nextState, barrier, args)
				nextState.postAggregateChronological = nil
			}
			if len(nextState.postAggregateScalarExtrema) > 0 {
				var additionalAliases int
				relation, additionalAliases = compileScalarExtremaResults(
					relation,
					nextState.postAggregateScalarExtrema,
					operator.Range,
					aliasSequence,
				)
				aliasSequence += additionalAliases
				nextState.postAggregateScalarExtrema = nil
			}
			publishedValues := make([]string, 0, len(nextState.postAggregateExactStrings))
			for _, measure := range nextState.postAggregateExactStrings {
				if measure.function == plan.AggregateFunctionValues {
					publishedValues = append(publishedValues, measure.outputColumn)
				}
			}
			if len(nextState.postAggregateExactStrings) > 0 {
				var additionalAliases int
				relation, additionalAliases = compileBoundedExactStringResults(
					relation,
					nextState.postAggregateExactStrings,
					nextState.postAggregateDistinctCounts,
					operator.Range,
					aliasSequence,
				)
				aliasSequence += additionalAliases
				nextState.postAggregateExactStrings = nil
				nextState.postAggregateDistinctCounts = nil
			} else if len(nextState.postAggregateDistinctCounts) > 0 {
				var additionalAliases int
				relation, additionalAliases = compileBoundedDistinctCountResults(
					relation,
					nextState.postAggregateDistinctCounts,
					operator.Range,
					aliasSequence,
				)
				aliasSequence += additionalAliases
				nextState.postAggregateDistinctCounts = nil
			}
			if len(nextState.postAggregateOrderedStrings) > 0 {
				var additionalAliases int
				relation, additionalAliases = compileBoundedOrderedStringResults(
					relation,
					nextState.postAggregateOrderedStrings,
					publishedValues,
					operator.Range,
					aliasSequence,
				)
				aliasSequence += additionalAliases
				nextState.postAggregateOrderedStrings = nil
			}
			state = nextState
		case *plan.EventAggregate:
			if operatorIndex+1 < len(remainingOperators) {
				if adjacent, ok := remainingOperators[operatorIndex+1].(*plan.EventAggregate); ok &&
					canFuseChronologicalEventAggregates(operator, adjacent, state) {
					enriched, nextState, prefixArgs, barrier, compileErr :=
						compileFusedChronologicalEventAggregates(
							relation,
							operator,
							adjacent,
							state,
							aliasSequence,
							aliasSequence+1,
						)
					if compileErr != nil {
						return CompiledQuery{}, compileErr
					}
					relation = enriched
					args = prependArguments(prefixArgs, args)
					nextState, args = bindChronologicalBarrier(nextState, barrier, args)
					state = nextState
					operatorIndex++
					aliasSequence++
					break
				}
			}
			enriched, nextState, prefixArgs, barrier, compileErr := compileEventAggregate(
				relation,
				operator,
				state,
				aliasSequence,
			)
			if compileErr != nil {
				return CompiledQuery{}, compileErr
			}
			relation = enriched
			if barrier != nil && barrier.prefixArgumentsAfterExisting {
				args = append(args, prefixArgs...)
			} else {
				args = prependArguments(prefixArgs, args)
			}
			nextState, args = bindChronologicalBarrier(nextState, barrier, args)
			state = nextState
		case *plan.StreamAggregate:
			if operatorIndex+1 < len(remainingOperators) {
				if adjacent, ok := remainingOperators[operatorIndex+1].(*plan.StreamAggregate); ok &&
					canFuseChronologicalStreamAggregates(operator, adjacent, state) {
					enriched, nextState, prefixArgs, barrier, compileErr :=
						compileFusedChronologicalStreamAggregates(
							relation,
							operator,
							adjacent,
							state,
							aliasSequence,
							aliasSequence+1,
						)
					if compileErr != nil {
						return CompiledQuery{}, compileErr
					}
					relation = enriched
					args = prependArguments(prefixArgs, args)
					nextState, args = bindChronologicalBarrier(nextState, barrier, args)
					state = nextState
					operatorIndex++
					aliasSequence++
					break
				}
			}
			enriched, nextState, prefixArgs, barrier, compileErr := compileStreamAggregate(
				relation,
				operator,
				state,
				aliasSequence,
			)
			if compileErr != nil {
				return CompiledQuery{}, compileErr
			}
			relation = enriched
			args = prependArguments(prefixArgs, args)
			nextState, args = bindChronologicalBarrier(nextState, barrier, args)
			state = nextState
		case *plan.Timechart:
			if !permitTerminalWideOperators {
				return CompiledQuery{}, errors.New("compile ClickHouse query: timechart is unavailable for event analysis")
			}
			if operatorIndex+1 != len(remainingOperators) {
				return CompiledQuery{}, errors.New("compile ClickHouse timechart: operator must be terminal")
			}
			compiled, compileErr := compileTimechart(
				relation,
				state,
				args,
				operator,
				query.OutputFields,
				query.DynamicOutput,
				scan,
				alias,
			)
			if compileErr != nil {
				return CompiledQuery{}, compileErr
			}
			return finishCompiled(compiled, operator.Range)
		case *plan.Chart:
			if !permitTerminalWideOperators {
				return CompiledQuery{}, errors.New("compile ClickHouse query: chart is unavailable for event analysis")
			}
			if operatorIndex+1 != len(remainingOperators) {
				return CompiledQuery{}, errors.New("compile ClickHouse chart: operator must be terminal")
			}
			compiled, compileErr := compileChart(relation, state, args, operator, query.DynamicOutput, alias)
			if compileErr != nil {
				return CompiledQuery{}, compileErr
			}
			return finishCompiled(compiled, operator.Range)
		case *plan.Window:
			expression, nextState, compileErr := compileWindow(operator, state)
			if compileErr != nil {
				return CompiledQuery{}, compileErr
			}
			relation = relation.selectFrom(
				"SELECT *, "+expression+" AS "+quoteIdentifier(operator.Output)+" FROM ("+relation.sql+") AS "+alias,
				operator.Range,
			)
			state = nextState
		case *plan.Sort:
			if operatorIndex > 0 {
				previous, ok := remainingOperators[operatorIndex-1].(*plan.Sort)
				if ok && equivalentSortOperators(previous, operator) {
					// Reuse the preceding command's durable comparator instead of
					// expanding its exact Auto expression again. Retain the authored
					// ORDER BY/LIMIT boundary: besides preserving source-range and
					// relational-depth accounting, that boundary is observable to
					// commands which follow this run of identical sorts.
					order, orderErr := compileMaterializedOrder(state.order, false)
					if orderErr != nil {
						return CompiledQuery{}, orderErr
					}
					sortSQL := "SELECT * FROM (" + relation.sql + ") AS " + alias +
						" ORDER BY " + order
					if operator.Limit > 0 {
						sortSQL += " LIMIT ?"
						args = append(args, operator.Limit)
					}
					relation = relation.selectFrom(sortSQL, operator.Range)
					break
				}
			}
			materialized, sortKeys, order, prefixArgs, compileErr := compileSort(operator.Keys, state, aliasSequence)
			if compileErr != nil {
				return CompiledQuery{}, compileErr
			}
			sortSQL := "SELECT *, " + strings.Join(materialized, ", ") + " FROM (" + relation.sql + ") AS " + alias + " ORDER BY " + order
			args = prependArguments(prefixArgs, args)
			if operator.Limit > 0 {
				sortSQL += " LIMIT ?"
				args = append(args, operator.Limit)
			}
			relation = relation.selectFrom(sortSQL, operator.Range)
			state.order = sortKeys
		case *plan.Deduplicate:
			deduplicated, prefixArgs, currentOrder, additionalAliases, compileErr := compileDeduplicate(relation, operator, state, aliasSequence)
			if compileErr != nil {
				return CompiledQuery{}, compileErr
			}
			relation = deduplicated
			args = prependArguments(prefixArgs, args)
			args = append(args, operator.Count)
			state.order = currentOrder
			aliasSequence += additionalAliases
		case *plan.Limit:
			keys := state.order
			if len(keys) == 0 {
				keys = stableCompiledSortKeys()
			}
			if operator.FromEnd {
				reversed, compileErr := compileMaterializedOrder(keys, true)
				if compileErr != nil {
					return CompiledQuery{}, compileErr
				}
				relation = relation.selectFrom(
					"SELECT * FROM ("+relation.sql+") AS "+alias+" ORDER BY "+reversed+" LIMIT ?",
					operator.Range,
				)
				args = append(args, operator.Count)
				state.order = reverseCompiledSortKeys(keys)
			} else {
				order, compileErr := compileMaterializedOrder(keys, false)
				if compileErr != nil {
					return CompiledQuery{}, compileErr
				}
				relation = relation.selectFrom(
					"SELECT * FROM ("+relation.sql+") AS "+alias+" ORDER BY "+order+" LIMIT ?",
					operator.Range,
				)
				args = append(args, operator.Count)
				state.order = append([]compiledSortKey(nil), keys...)
			}
		case *plan.Reverse:
			reversed, nextState, compileErr := compileReverse(
				relation,
				operator,
				state,
				aliasSequence,
			)
			if compileErr != nil {
				return CompiledQuery{}, compileErr
			}
			relation = reversed
			state = nextState
		default:
			return CompiledQuery{}, fmt.Errorf("compile ClickHouse query: unsupported logical operator %T", operator)
		}
		if err := validateRelationalDepth(relation.depth, relation.ownerRange); err != nil {
			return CompiledQuery{}, err
		}
	}
	if lookupStageIndex != len(lookupPreparation.stages) {
		return CompiledQuery{}, errors.New(
			"compile ClickHouse lookup: not every prepared stage was lowered",
		)
	}

	compiled, err := finalize(relation, state, args, scan, aliasSequence)
	if err != nil {
		return CompiledQuery{}, err
	}
	return finishCompiled(compiled, scan.Range)
}

type authoredKnowledgeCompilation struct {
	regexPrograms      uint32
	regexWorkUnits     uint64
	extractionOutputs  uint32
	jsonEvaluationWork uint32
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
	if operator.Span < time.Second || operator.Span >= 24*time.Hour || operator.Span%time.Second != 0 {
		return compiledRelation{}, compileState{}, nil, errors.New("compile ClickHouse time bucket: fixed span must be at least one second and shorter than 24 hours")
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
	value := "fromUnixTimestamp64Nano(" + bucketTicks + ", 'UTC')"
	fragment, next := compileBucketProjection(relation.sql, state, operator.Field.Name, operator.Output, value, field, alias)
	relation = relation.selectFrom(fragment, operator.Range)
	return relation, next, []any{spanNanoseconds, spanNanoseconds, spanNanoseconds}, nil
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

func compileTimechart(
	relation compiledRelation,
	state compileState,
	args []any,
	operator *plan.Timechart,
	outputFields []string,
	dynamic *plan.DynamicSeriesOutput,
	scan *plan.Scan,
	alias string,
) (CompiledQuery, error) {
	if err := validateTimechartMeasure(operator, state); err != nil {
		return CompiledQuery{}, err
	}
	if operator.Span < time.Second || operator.Span > 24*time.Hour || operator.Span%time.Second != 0 || operator.FirstBucket.Nanosecond() != 0 ||
		operator.FirstBucket.IsZero() || operator.BucketCount == 0 || operator.BucketCount > 10_000 || !operator.FixedRange ||
		!operator.Continuous || !operator.IncludePartial {
		return CompiledQuery{}, errors.New("compile ClickHouse timechart: bounded defaults are invalid")
	}
	if scan == nil {
		return CompiledQuery{}, errors.New("compile ClickHouse timechart: Scan snapshot is required")
	}
	spanSeconds := int64(operator.Span / time.Second)
	spanNanoseconds, err := validateFixedTimeGridSpec(TimelineSpec{
		FirstBucket: operator.FirstBucket,
		SpanSeconds: spanSeconds,
		BucketCount: operator.BucketCount,
		Earliest:    scan.Earliest,
		Latest:      scan.Latest,
	}, "timechart")
	if err != nil {
		return CompiledQuery{}, err
	}
	firstBucketNumber, gridOK := ordinalGridFirstBucketNumber(operator.FirstBucket.Unix(), spanSeconds, operator.BucketCount)
	if !gridOK {
		return CompiledQuery{}, errors.New("compile ClickHouse timechart: bucket grid overflows")
	}
	if !state.eventRows {
		return CompiledQuery{}, &plan.Diagnostic{
			Code:    "SPL_UNSUPPORTED_TIMECHART_INPUT",
			Message: "timechart requires event rows with the canonical _time field",
			Range:   operator.Range,
		}
	}
	if err := validateCanonicalFieldRef("timechart", "time", operator.Time); err != nil {
		return CompiledQuery{}, err
	}
	timeField, ok, err := resolveCompiledField(operator.Time, state)
	if err != nil {
		return CompiledQuery{}, err
	}
	if !ok || operator.Time.Name != "_time" || timeField.kind != fieldKindTime || !timeField.canonicalTime {
		return CompiledQuery{}, &plan.Diagnostic{
			Code:    "SPL_UNSUPPORTED_TIMECHART_TIME_FIELD",
			Message: "timechart requires the unmodified canonical _time field",
			Range:   operator.Range,
		}
	}

	if valueKind, fixedValue := fixedTimechartValueKind(operator.Measure.Function); fixedValue && operator.Split == nil {
		if len(outputFields) != 2 || outputFields[0] != "_time" ||
			outputFields[1] != operator.Measure.Output || outputFields[1] == "_time" ||
			dynamic != nil {
			return CompiledQuery{}, errors.New(
				"compile ClickHouse timechart: fixed value output contract is invalid",
			)
		}
		measureField, measureExists, resolveErr := resolveCompiledField(
			operator.Measure.Input,
			state,
		)
		if resolveErr != nil {
			return CompiledQuery{}, resolveErr
		}
		measureInputSQL := "CAST([], 'Array(Float64)')"
		var measureArgs []any
		if measureExists {
			measureInputSQL, measureArgs = numericArrayInputSQL(measureField)
		}
		return compileFixedValueTimechart(
			relation,
			args,
			operator,
			valueKind,
			timeField,
			measureInputSQL,
			measureArgs,
			outputFields,
			spanNanoseconds,
			firstBucketNumber,
			alias,
		)
	}

	if operator.Split == nil {
		if operator.Measure.Function == plan.AggregateFunctionCountValues {
			if len(outputFields) != 2 || outputFields[0] != "_time" ||
				outputFields[1] != operator.Measure.Output ||
				outputFields[1] == "_time" || dynamic != nil {
				return CompiledQuery{}, errors.New(
					"compile ClickHouse timechart: fixed count(field) output contract is invalid",
				)
			}
			measureInputSQL, measureArgs, resolveErr := resolveCountValueInput(
				operator.Measure.Input,
				state,
			)
			if resolveErr != nil {
				return CompiledQuery{}, resolveErr
			}
			return compileFixedCountValueTimechart(
				relation,
				args,
				operator,
				timeField,
				measureInputSQL,
				measureArgs,
				outputFields,
				spanNanoseconds,
				firstBucketNumber,
				alias,
			)
		}
		if !slices.Equal(outputFields, []string{"_time", "count"}) || dynamic != nil {
			return CompiledQuery{}, errors.New("compile ClickHouse timechart: fixed output contract is invalid")
		}
		return compileFixedCountTimechart(
			relation,
			args,
			operator,
			timeField,
			outputFields,
			spanNanoseconds,
			firstBucketNumber,
			alias,
		)
	}
	if len(outputFields) != 0 || dynamic == nil ||
		!slices.Equal(dynamic.FixedFields, []string{"_time"}) ||
		dynamic.MaxSeries != 12 || operator.Split.SeriesLimit != 10 ||
		uint32(operator.Split.SeriesLimit)+2 != uint32(dynamic.MaxSeries) ||
		!operator.Split.IncludeNull || !operator.Split.IncludeOther ||
		operator.Split.NullLabel != "NULL" || operator.Split.OtherLabel != "OTHER" {
		return CompiledQuery{}, errors.New("compile ClickHouse timechart: dynamic output contract is invalid")
	}
	if err := validateCanonicalFieldRef(
		"timechart",
		"split",
		operator.Split.Field,
	); err != nil {
		return CompiledQuery{}, err
	}

	splitField, splitExists, err := resolveCompiledField(operator.Split.Field, state)
	if err != nil {
		return CompiledQuery{}, err
	}
	if !splitExists {
		// A projected-away split field is missing for every retained event. SPL's
		// default usenull=true therefore produces a NULL series rather than
		// resurrecting the private source document.
		splitField = fieldState{
			valueSQL:  "CAST(NULL AS Nullable(String))",
			existsSQL: "0",
			kind:      fieldKindString,
		}
	}
	if splitField.kind == fieldKindInvalid {
		// A statically null field is in the supported missing/null split domain.
		// Preserve its exact-presence expression while assigning the String type
		// used by the runtime label classifier.
		splitField = fieldState{
			valueSQL:   "CAST(NULL AS Nullable(String))",
			existsSQL:  splitField.existsSQL,
			existsArgs: splitField.existsArgs,
			kind:       fieldKindString,
		}
	}
	if splitField.kind != fieldKindString && splitField.kind != fieldKindDynamic {
		return CompiledQuery{}, &plan.Diagnostic{
			Code:        "SPL_UNSUPPORTED_TIMECHART_FIELD_TYPE",
			Message:     "timechart split fields currently support strings and missing values",
			Range:       operator.Range,
			Suggestions: []string{"convert the split field to a string before timechart"},
		}
	}

	existsSQL := splitField.existsSQL
	if existsSQL == "" {
		existsSQL = "1"
	}
	valueTypeSQL := "if(isNull(" + splitField.valueSQL + "), 'None', 'String')"
	if splitField.kind == fieldKindDynamic {
		valueTypeSQL = dynamicTypeExpression(splitField)
	}
	if valueKind, splitValue := fixedTimechartValueKind(operator.Measure.Function); splitValue {
		measureField, measureExists, resolveErr := resolveCompiledField(
			operator.Measure.Input,
			state,
		)
		if resolveErr != nil {
			return CompiledQuery{}, resolveErr
		}
		measureInputSQL := "CAST([], 'Array(Float64)')"
		var measureArgs []any
		if measureExists {
			measureInputSQL, measureArgs = numericArrayInputSQL(measureField)
		}
		return compileSplitValueTimechart(
			relation,
			state,
			args,
			operator,
			valueKind,
			timeField,
			splitField,
			existsSQL,
			valueTypeSQL,
			measureInputSQL,
			measureArgs,
			dynamic,
			spanNanoseconds,
			firstBucketNumber,
			alias,
		)
	}
	fieldOccurrenceCount := operator.Measure.Function == plan.AggregateFunctionCountValues
	measureInputSQL := ""
	var measureArgs []any
	if fieldOccurrenceCount {
		var resolveErr error
		measureInputSQL, measureArgs, resolveErr = resolveCountValueInput(
			operator.Measure.Input,
			state,
		)
		if resolveErr != nil {
			return CompiledQuery{}, resolveErr
		}
	}
	// Source-select bind markers precede the nested scoped relation. Split
	// descendant detection lives in the next CTE, after every source marker.
	prefixArgs := make([]any, 0, len(splitField.existsArgs)+len(measureArgs))
	prefixArgs = append(prefixArgs, splitField.existsArgs...)
	prefixArgs = append(prefixArgs, measureArgs...)
	args = prependArguments(prefixArgs, args)
	hasDescendant := splitField.kind == fieldKindDynamic && splitField.descendantSQL != ""
	if hasDescendant {
		args = append(args, splitField.descendantArgs...)
	}

	q := quoteIdentifier
	source := q("__os_timechart_source")
	prepared := q("__os_timechart_prepared")
	classified := q("__os_timechart_classified")
	canonicalized := q("__os_timechart_canonicalized")
	counts := q("__os_timechart_group_counts")
	scored := q("__os_timechart_scored")
	ranked := q("__os_timechart_ranked")
	collapsed := q("__os_timechart_collapsed")
	domainRows := q("__os_timechart_domain_rows")
	domain := q("__os_timechart_domain")
	bucketMaps := q("__os_timechart_bucket_maps")
	grid := q("__os_timechart_grid")

	eventTime := q("__os_tc_event_time")
	value := q("__os_tc_value")
	present := q("__os_tc_present")
	descendant := q("__os_tc_descendant")
	valueType := q("__os_tc_value_type")
	ticks := q("__os_tc_ticks")
	label := q("__os_tc_label")
	measureCount := q("__os_tc_measure_count")
	bucketNumber := q("__os_tc_bucket_number")
	kind := q("__os_tc_kind")
	frequency := q("__os_tc_count")
	rowCount := q("__os_tc_row_count")
	occurrenceCount := q("__os_tc_occurrence_count")
	collapsedRowCount := q("__os_tc_collapsed_row_count")
	collapsedCount := q("__os_tc_collapsed_count")
	seriesScore := q("__os_tc_series_score")
	seriesRank := q("__os_tc_series_rank")
	encoded := q("__os_tc_encoded")
	collisionCardinality := q("__os_tc_collision_cardinality")
	collision := q("__os_tc_collision")
	sortLabel := q("__os_tc_sort_label")
	countMap := q("__os_tc_count_map")
	invalid := q("__os_tc_invalid")
	ordinal := q(TimechartOrdinalColumn)

	bucketNumberExpression := epochFloorBucketNumberSQL(ticks)
	validLabel := "isValidUTF8(" + label + ") AND length(" + label + ") BETWEEN 1 AND " +
		strconv.Itoa(maxTimechartLabelBytes) + " AND " + label + " NOT IN ('NULL', 'OTHER')"

	var sql strings.Builder
	sql.Grow(len(relation.sql) + 8_192)
	sql.WriteString("WITH ")
	sql.WriteString(source)
	sql.WriteString(" AS (SELECT ")
	sql.WriteString(timeField.valueSQL)
	sql.WriteString(" AS ")
	sql.WriteString(eventTime)
	sql.WriteString(", ")
	sql.WriteString(splitField.valueSQL)
	sql.WriteString(" AS ")
	sql.WriteString(value)
	sql.WriteString(", ")
	sql.WriteString("toUInt8(")
	sql.WriteString(existsSQL)
	sql.WriteString(") AS ")
	sql.WriteString(present)
	sql.WriteString(", ")
	sql.WriteString(valueTypeSQL)
	sql.WriteString(" AS ")
	sql.WriteString(valueType)
	if fieldOccurrenceCount {
		sql.WriteString(", ")
		sql.WriteString(measureInputSQL)
		sql.WriteString(" AS ")
		sql.WriteString(measureCount)
	}
	for _, column := range pivotDescendantSourceColumns(state, splitField) {
		sql.WriteString(", ")
		sql.WriteString(column)
	}
	sql.WriteString(" FROM (")
	sql.WriteString(relation.sql)
	sql.WriteString(") AS ")
	sql.WriteString(alias)
	sql.WriteString("), ")

	sql.WriteString(prepared)
	sql.WriteString(" AS (SELECT *, ")
	if hasDescendant {
		sql.WriteString("toUInt8(if(")
		sql.WriteString(present)
		sql.WriteString(" != 0, 0, ")
		sql.WriteString(splitField.descendantSQL)
		sql.WriteString(")) AS ")
		sql.WriteString(descendant)
		sql.WriteString(", ")
	} else {
		sql.WriteString("toUInt8(0) AS ")
		sql.WriteString(descendant)
		sql.WriteString(", ")
	}
	sql.WriteString("reinterpretAsInt64(")
	sql.WriteString(eventTime)
	sql.WriteString(") AS ")
	sql.WriteString(ticks)
	sql.WriteString(", ")
	sql.WriteString("if(")
	sql.WriteString(present)
	sql.WriteString(" != 0 AND isNotNull(")
	sql.WriteString(value)
	sql.WriteString(") AND ")
	sql.WriteString(valueType)
	sql.WriteString(" = 'String', ")
	sql.WriteString("assumeNotNull(toString(")
	sql.WriteString(value)
	sql.WriteString(")), CAST('' AS String)) AS ")
	sql.WriteString(label)
	sql.WriteString(" FROM ")
	sql.WriteString(source)
	sql.WriteString("), ")

	sql.WriteString(classified)
	sql.WriteString(" AS (SELECT ")
	sql.WriteString(bucketNumberExpression)
	sql.WriteString(" AS ")
	sql.WriteString(bucketNumber)
	sql.WriteString(", ")
	sql.WriteString("multiIf(")
	sql.WriteString(descendant)
	sql.WriteString(" != 0, toUInt8(3), ")
	sql.WriteString(present)
	sql.WriteString(" = 0 OR isNull(")
	sql.WriteString(value)
	sql.WriteString(") OR ")
	sql.WriteString(valueType)
	sql.WriteString(" = 'None', toUInt8(1), ")
	sql.WriteString(valueType)
	sql.WriteString(" != 'String', toUInt8(3), NOT (")
	sql.WriteString(validLabel)
	sql.WriteString("), toUInt8(3), toUInt8(0)) AS ")
	sql.WriteString(kind)
	sql.WriteString(", ")
	sql.WriteString(label)
	if fieldOccurrenceCount {
		sql.WriteString(", ")
		sql.WriteString(measureCount)
	}
	sql.WriteString(" FROM ")
	sql.WriteString(prepared)
	sql.WriteString("), ")

	sql.WriteString(canonicalized)
	sql.WriteString(" AS (SELECT ")
	sql.WriteString(bucketNumber)
	sql.WriteString(", ")
	sql.WriteString(kind)
	sql.WriteString(", if(")
	sql.WriteString(kind)
	sql.WriteString(" = 0, ")
	sql.WriteString(label)
	sql.WriteString(", CAST('' AS String)) AS ")
	sql.WriteString(label)
	if fieldOccurrenceCount {
		sql.WriteString(", ")
		sql.WriteString(measureCount)
	}
	sql.WriteString(" FROM ")
	sql.WriteString(classified)
	sql.WriteString("), ")

	// Keep the raw bucket/label aggregate as the first bounded operation. The
	// executor's max_rows_to_group_by seal therefore continues to cap exactly
	// the same 130k raw groups before any series selection or publication.
	sql.WriteString(counts)
	sql.WriteString(" AS (SELECT ")
	sql.WriteString(bucketNumber)
	sql.WriteString(", ")
	sql.WriteString(kind)
	sql.WriteString(", ")
	sql.WriteString(label)
	sql.WriteString(", ")
	if fieldOccurrenceCount {
		// Row frequency and occurrence cardinality are intentionally
		// independent. The former keeps zero-contribution labels in the domain
		// and validates bad split rows; only the latter selects series and
		// populates their cells.
		sql.WriteString("count() AS ")
		sql.WriteString(rowCount)
		sql.WriteString(", ")
		sql.WriteString("toUInt64(sum(toUInt128(")
		sql.WriteString(measureCount)
		sql.WriteString("))) AS ")
		sql.WriteString(occurrenceCount)
	} else {
		sql.WriteString("count() AS ")
		sql.WriteString(frequency)
	}
	sql.WriteString(" FROM ")
	sql.WriteString(canonicalized)
	sql.WriteString(" GROUP BY ")
	sql.WriteString(bucketNumber)
	sql.WriteString(", ")
	sql.WriteString(kind)
	sql.WriteString(", ")
	sql.WriteString(label)
	sql.WriteString("), ")

	// Score every raw label once across buckets. The collision cardinality is a
	// window over the public label normalization domain, so it travels with the
	// same single-consumer chain instead of requiring another counts branch.
	scoreInput := frequency
	if fieldOccurrenceCount {
		scoreInput = occurrenceCount
	}
	sql.WriteString(scored)
	sql.WriteString(" AS (SELECT *, sum(toUInt128(")
	sql.WriteString(scoreInput)
	sql.WriteString(")) OVER (PARTITION BY ")
	sql.WriteString(kind)
	sql.WriteString(", ")
	sql.WriteString(label)
	sql.WriteString(") AS ")
	sql.WriteString(seriesScore)
	sql.WriteString(", ")
	sql.WriteString("uniqExact(")
	sql.WriteString(label)
	sql.WriteString(") OVER (PARTITION BY ")
	sql.WriteString(kind)
	sql.WriteString(", ")
	sql.WriteString(splunkSeriesLabelSQL(label))
	sql.WriteString(") AS ")
	sql.WriteString(collisionCardinality)
	sql.WriteString(" FROM ")
	sql.WriteString(counts)
	sql.WriteString("), ")

	// The label tie-breaker makes every ordinary label's dense rank unique,
	// while repeated bucket rows for that label retain one shared rank.
	sql.WriteString(ranked)
	sql.WriteString(" AS (SELECT *, dense_rank() OVER (PARTITION BY ")
	sql.WriteString(kind)
	sql.WriteString(" ORDER BY ")
	sql.WriteString(seriesScore)
	sql.WriteString(" DESC, ")
	sql.WriteString(label)
	sql.WriteString(" ASC) AS ")
	sql.WriteString(seriesRank)
	sql.WriteString(" FROM ")
	sql.WriteString(scored)
	sql.WriteString("), ")

	seriesLimit := strconv.FormatUint(uint64(operator.Split.SeriesLimit), 10)
	sql.WriteString(collapsed)
	sql.WriteString(" AS MATERIALIZED (SELECT ")
	sql.WriteString(bucketNumber)
	sql.WriteString(", multiIf(")
	sql.WriteString(kind)
	sql.WriteString(" = 1, '1:', ")
	sql.WriteString(kind)
	sql.WriteString(" = 0 AND ")
	sql.WriteString(seriesRank)
	sql.WriteString(" <= ")
	sql.WriteString(seriesLimit)
	sql.WriteString(", concat('0:', ")
	sql.WriteString(label)
	sql.WriteString("), ")
	sql.WriteString(kind)
	sql.WriteString(" = 0, '2:', CAST('' AS String)) AS ")
	sql.WriteString(encoded)
	sql.WriteString(", ")
	if fieldOccurrenceCount {
		sql.WriteString("sum(")
		sql.WriteString(rowCount)
		sql.WriteString(") AS ")
		sql.WriteString(collapsedRowCount)
		sql.WriteString(", ")
		sql.WriteString("toUInt64(sum(toUInt128(")
		sql.WriteString(occurrenceCount)
		sql.WriteString("))) AS ")
		sql.WriteString(collapsedCount)
		sql.WriteString(", ")
	} else {
		sql.WriteString("sum(")
		sql.WriteString(frequency)
		sql.WriteString(") AS ")
		sql.WriteString(collapsedCount)
		sql.WriteString(", ")
	}
	validationCount := frequency
	if fieldOccurrenceCount {
		validationCount = rowCount
	}
	sql.WriteString("toUInt8(sumIf(")
	sql.WriteString(validationCount)
	sql.WriteString(", ")
	sql.WriteString(kind)
	sql.WriteString(" = 3) > 0) AS ")
	sql.WriteString(invalid)
	sql.WriteString(", ")
	sql.WriteString("toUInt8(maxIf(")
	sql.WriteString(collisionCardinality)
	sql.WriteString(", ")
	sql.WriteString(kind)
	sql.WriteString(" = 0) > 1) AS ")
	sql.WriteString(collision)
	sql.WriteString(" FROM ")
	sql.WriteString(ranked)
	sql.WriteString(" GROUP BY ")
	sql.WriteString(bucketNumber)
	sql.WriteString(", ")
	sql.WriteString(encoded)
	sql.WriteString("), ")

	// Every domain member now comes from the sealed, already-collapsed relation.
	// Empty encodings are private validation rows and never become map keys or
	// public names.
	domainFrequency := collapsedCount
	if fieldOccurrenceCount {
		domainFrequency = collapsedRowCount
	}
	rawEncodedLabel := "substring(" + encoded + ", 3)"
	sql.WriteString(domainRows)
	sql.WriteString(" AS (SELECT multiIf(")
	sql.WriteString(encoded)
	sql.WriteString(" = '1:', toUInt8(1), ")
	sql.WriteString(encoded)
	sql.WriteString(" = '2:', toUInt8(2), toUInt8(0)) AS sort_kind, ")
	sql.WriteString("if(startsWith(")
	sql.WriteString(encoded)
	sql.WriteString(", '0:'), ")
	sql.WriteString(splunkSeriesLabelSQL(rawEncodedLabel))
	sql.WriteString(", CAST('' AS String)) AS ")
	sql.WriteString(sortLabel)
	sql.WriteString(", ")
	sql.WriteString(encoded)
	sql.WriteString(" FROM ")
	sql.WriteString(collapsed)
	sql.WriteString(" WHERE ")
	sql.WriteString(encoded)
	sql.WriteString(" != '' AND ")
	sql.WriteString(domainFrequency)
	sql.WriteString(" > 0 GROUP BY ")
	sql.WriteString(encoded)
	sql.WriteString("), ")

	sql.WriteString(domain)
	sql.WriteString(" AS (SELECT arrayMap(item -> item.3, arraySort(item -> (item.1, item.2), groupArray((sort_kind, ")
	sql.WriteString(sortLabel)
	sql.WriteString(", ")
	sql.WriteString(encoded)
	sql.WriteString(")))) AS names FROM ")
	sql.WriteString(domainRows)
	sql.WriteString("), ")

	sql.WriteString(bucketMaps)
	mapValue := collapsedCount
	// The empty String is outside the public encoded domain (0:/1:/2:) and is
	// therefore a private per-bucket validation key. Carrying the combined flag
	// in the existing map removes a third collapsed consumer without adding a
	// second public projection or losing invalid-only buckets. The executor
	// buffers the complete fixed grid before publishing, so any nonzero bucket
	// still rejects the result atomically.
	sql.WriteString(" AS (SELECT ")
	sql.WriteString(bucketNumber)
	sql.WriteString(", mapFromArrays(")
	sql.WriteString("arrayPushBack(groupArrayIf(")
	sql.WriteString(encoded)
	sql.WriteString(", ")
	sql.WriteString(encoded)
	sql.WriteString(" != ''), CAST('' AS String)), ")
	sql.WriteString("arrayPushBack(groupArrayIf(")
	sql.WriteString(mapValue)
	sql.WriteString(", ")
	sql.WriteString(encoded)
	sql.WriteString(" != ''), ")
	sql.WriteString("toUInt64(max(")
	sql.WriteString(invalid)
	sql.WriteString(" != 0 OR ")
	sql.WriteString(collision)
	sql.WriteString(" != 0)))) AS ")
	sql.WriteString(countMap)
	sql.WriteString(" FROM ")
	sql.WriteString(collapsed)
	sql.WriteString(" GROUP BY ")
	sql.WriteString(bucketNumber)
	sql.WriteString("), ")

	sql.WriteString(grid)
	sql.WriteString(" AS (")
	sql.WriteString(ordinalGridSQL(ordinal, bucketNumber))
	sql.WriteString(") ")

	sql.WriteString("SELECT ")
	sql.WriteString(grid)
	sql.WriteString(".")
	sql.WriteString(ordinal)
	sql.WriteString(" AS ")
	sql.WriteString(ordinal)
	sql.WriteString(", ")
	sql.WriteString("if(")
	sql.WriteString(grid)
	sql.WriteString(".")
	sql.WriteString(ordinal)
	sql.WriteString(" = 0, ")
	sql.WriteString(domain)
	sql.WriteString(".names, CAST([], 'Array(String)')) AS ")
	sql.WriteString(q(TimechartNamesColumn))
	sql.WriteString(", ")
	sql.WriteString("arrayMap(name -> ifNull(")
	sql.WriteString(bucketMaps)
	sql.WriteString(".")
	sql.WriteString(countMap)
	sql.WriteString("[name], toUInt64(0)), ")
	sql.WriteString(domain)
	sql.WriteString(".names) AS ")
	sql.WriteString(q(TimechartCountsColumn))
	sql.WriteString(", ")
	sql.WriteString("toUInt8(ifNull(")
	sql.WriteString(bucketMaps)
	sql.WriteString(".")
	sql.WriteString(countMap)
	sql.WriteString("[''], toUInt64(0)) != 0) AS ")
	sql.WriteString(q(TimechartInvalidColumn))
	sql.WriteString(" FROM ")
	sql.WriteString(grid)
	sql.WriteString(" CROSS JOIN ")
	sql.WriteString(domain)
	sql.WriteString(" LEFT JOIN ")
	sql.WriteString(bucketMaps)
	sql.WriteString(" ON ")
	sql.WriteString(bucketMaps)
	sql.WriteString(".")
	sql.WriteString(bucketNumber)
	sql.WriteString(" = ")
	sql.WriteString(grid)
	sql.WriteString(".")
	sql.WriteString(bucketNumber)
	sql.WriteString(" ORDER BY ")
	sql.WriteString(grid)
	sql.WriteString(".")
	sql.WriteString(ordinal)
	sql.WriteString(" ASC")
	sql.WriteString(materializedCTESettingsSQL)

	args = appendOrdinalGridArgs(args, spanNanoseconds, firstBucketNumber, operator.BucketCount)
	sourceDepth := relationalNodeDepth(relation.depth)
	preparedDepth := relationalNodeDepth(sourceDepth)
	classifiedDepth := relationalNodeDepth(preparedDepth)
	canonicalizedDepth := relationalNodeDepth(classifiedDepth)
	countsDepth := relationalNodeDepth(canonicalizedDepth)
	scoredDepth := relationalNodeDepth(countsDepth)
	rankedDepth := relationalNodeDepth(scoredDepth)
	collapsedDepth := relationalNodeDepth(rankedDepth)
	domainRowsDepth := relationalNodeDepth(collapsedDepth)
	domainDepth := relationalNodeDepth(domainRowsDepth)
	bucketMapsDepth := relationalNodeDepth(collapsedDepth)
	gridDepth := relationalNodeDepth()
	resultDepth := relationalNodeDepth(
		gridDepth,
		domainDepth,
		bucketMapsDepth,
	)

	compiled := CompiledQuery{
		SQL:          sql.String(),
		Args:         args,
		OutputFields: slices.Clone(dynamic.FixedFields),
		Timechart: &TimechartOutput{
			Mode:          TimechartModeRuntimeWide,
			FirstBucket:   operator.FirstBucket.UTC(),
			Span:          operator.Span,
			BucketCount:   operator.BucketCount,
			MaxSeries:     dynamic.MaxSeries,
			MaxLabelBytes: maxTimechartLabelBytes,
			ValueKind:     TimechartValueKindInvalid,
		},
	}
	return withCompiledRelationalDepth(compiled, resultDepth, operator.Range), nil
}

func compileSplitValueTimechart(
	relation compiledRelation,
	state compileState,
	args []any,
	operator *plan.Timechart,
	valueKind TimechartValueKind,
	timeField fieldState,
	splitField fieldState,
	splitExistsSQL string,
	splitValueTypeSQL string,
	measureInputSQL string,
	measureArgs []any,
	dynamic *plan.DynamicSeriesOutput,
	spanNanoseconds int64,
	firstBucketNumber int64,
	alias string,
) (CompiledQuery, error) {
	if operator == nil || operator.Split == nil || dynamic == nil {
		return CompiledQuery{}, errors.New(
			"compile ClickHouse split value timechart: contract is required",
		)
	}
	if !valueKind.Valid() {
		return CompiledQuery{}, errors.New(
			"compile ClickHouse split value timechart: value kind is invalid",
		)
	}

	q := quoteIdentifier
	source := q("__os_timechart_source")
	prepared := q("__os_timechart_prepared")
	classified := q("__os_timechart_classified")
	canonicalized := q("__os_timechart_canonicalized")
	numericGroups := q("__os_timechart_numeric_groups")
	numericScores := q("__os_timechart_numeric_scores")
	collapsed := q("__os_timechart_collapsed")
	finalized := q("__os_timechart_finalized")
	domainRows := q("__os_timechart_domain_rows")
	domain := q("__os_timechart_domain")
	collisions := q("__os_timechart_normalization_collisions")
	bucketMaps := q("__os_timechart_bucket_maps")
	validation := q("__os_timechart_validation")
	grid := q("__os_timechart_grid")

	eventTime := q("__os_tc_event_time")
	value := q("__os_tc_value")
	present := q("__os_tc_present")
	descendant := q("__os_tc_descendant")
	valueType := q("__os_tc_value_type")
	ticks := q("__os_tc_ticks")
	label := q("__os_tc_label")
	measureValues := q("__os_tc_measure_values")
	bucketNumber := q("__os_tc_bucket_number")
	kind := q("__os_tc_kind")
	numerator := q("__os_tc_numerator")
	denominator := q("__os_tc_denominator")
	frequency := q("__os_tc_count")
	numericState := q("__os_tc_numeric_state")
	percentileState := q("__os_tc_percentile_state")
	percentileValues := q("__os_tc_percentile_values")
	score := q("__os_tc_score")
	encoded := q("__os_tc_encoded")
	measureValue := q("__os_tc_measure_value")
	normalized := q("__os_tc_normalized")
	collision := q("__os_tc_collision")
	sortLabel := q("__os_tc_sort_label")
	valueMap := q("__os_tc_value_map")
	presentMap := q("__os_tc_present_map")
	invalid := q("__os_tc_invalid")
	ordinal := q(TimechartOrdinalColumn)

	bucketNumberExpression := epochFloorBucketNumberSQL(ticks)
	validLabel := "isValidUTF8(" + label + ") AND length(" + label + ") BETWEEN 1 AND " +
		strconv.Itoa(maxTimechartLabelBytes) + " AND " + label + " NOT IN ('NULL', 'OTHER')"

	var scoreSQL string
	var publishSQL string
	switch valueKind {
	case TimechartValueKindPercentile:
		scoreSQL = "sum(ifNull(arrayElementOrNull(finalizeAggregation(" +
			percentileState + "), 1), toFloat64(0)))"
		publishSQL = "arrayElementOrNull(" + percentileValues + ", 1)"
	case TimechartValueKindSum:
		scoreSQL = "sum(if(" + denominator + " = 0, toFloat64(0), " + numerator + "))"
		publishSQL = "if(" + denominator + " = 0, CAST(NULL AS Nullable(Float64)), " + numerator + ")"
	case TimechartValueKindAverage:
		bucketAverage := numerator + " / toFloat64(" + denominator + ")"
		scoreSQL = "sum(if(" + denominator + " = 0, toFloat64(0), " + bucketAverage + "))"
		publishSQL = "if(" + denominator + " = 0, CAST(NULL AS Nullable(Float64)), " + bucketAverage + ")"
	}

	// Source-select bind markers precede the nested relation text. Descendant
	// detection lives in the next CTE and therefore follows every source marker.
	prefixArgs := make([]any, 0, len(splitField.existsArgs)+len(measureArgs))
	prefixArgs = append(prefixArgs, splitField.existsArgs...)
	prefixArgs = append(prefixArgs, measureArgs...)
	args = prependArguments(prefixArgs, args)
	hasDescendant := splitField.kind == fieldKindDynamic && splitField.descendantSQL != ""
	if hasDescendant {
		args = append(args, splitField.descendantArgs...)
	}

	var sql strings.Builder
	sql.Grow(len(relation.sql) + len(measureInputSQL) + 12_288)
	sql.WriteString("WITH ")
	sql.WriteString(source)
	sql.WriteString(" AS (SELECT ")
	sql.WriteString(timeField.valueSQL)
	sql.WriteString(" AS ")
	sql.WriteString(eventTime)
	sql.WriteString(", ")
	sql.WriteString(splitField.valueSQL)
	sql.WriteString(" AS ")
	sql.WriteString(value)
	sql.WriteString(", ")
	sql.WriteString("toUInt8(")
	sql.WriteString(splitExistsSQL)
	sql.WriteString(") AS ")
	sql.WriteString(present)
	sql.WriteString(", ")
	sql.WriteString(splitValueTypeSQL)
	sql.WriteString(" AS ")
	sql.WriteString(valueType)
	sql.WriteString(", ")
	sql.WriteString(measureInputSQL)
	sql.WriteString(" AS ")
	sql.WriteString(measureValues)
	for _, column := range pivotDescendantSourceColumns(state, splitField) {
		sql.WriteString(", ")
		sql.WriteString(column)
	}
	sql.WriteString(" FROM (")
	sql.WriteString(relation.sql)
	sql.WriteString(") AS ")
	sql.WriteString(alias)
	sql.WriteString("), ")

	sql.WriteString(prepared)
	sql.WriteString(" AS (SELECT *, ")
	if hasDescendant {
		sql.WriteString("toUInt8(if(")
		sql.WriteString(present)
		sql.WriteString(" != 0, 0, ")
		sql.WriteString(splitField.descendantSQL)
		sql.WriteString(")) AS ")
		sql.WriteString(descendant)
		sql.WriteString(", ")
	} else {
		sql.WriteString("toUInt8(0) AS ")
		sql.WriteString(descendant)
		sql.WriteString(", ")
	}
	sql.WriteString("reinterpretAsInt64(")
	sql.WriteString(eventTime)
	sql.WriteString(") AS ")
	sql.WriteString(ticks)
	sql.WriteString(", ")
	sql.WriteString("if(")
	sql.WriteString(present)
	sql.WriteString(" != 0 AND isNotNull(")
	sql.WriteString(value)
	sql.WriteString(") AND ")
	sql.WriteString(valueType)
	sql.WriteString(" = 'String', ")
	sql.WriteString("assumeNotNull(toString(")
	sql.WriteString(value)
	sql.WriteString(")), CAST('' AS String)) AS ")
	sql.WriteString(label)
	sql.WriteString(" FROM ")
	sql.WriteString(source)
	sql.WriteString("), ")

	sql.WriteString(classified)
	sql.WriteString(" AS (SELECT ")
	sql.WriteString(bucketNumberExpression)
	sql.WriteString(" AS ")
	sql.WriteString(bucketNumber)
	sql.WriteString(", ")
	sql.WriteString("multiIf(")
	sql.WriteString(descendant)
	sql.WriteString(" != 0, toUInt8(3), ")
	sql.WriteString(present)
	sql.WriteString(" = 0 OR isNull(")
	sql.WriteString(value)
	sql.WriteString(") OR ")
	sql.WriteString(valueType)
	sql.WriteString(" = 'None', toUInt8(1), ")
	sql.WriteString(valueType)
	sql.WriteString(" != 'String', toUInt8(3), NOT (")
	sql.WriteString(validLabel)
	sql.WriteString("), toUInt8(3), toUInt8(0)) AS ")
	sql.WriteString(kind)
	sql.WriteString(", ")
	sql.WriteString(label)
	sql.WriteString(", ")
	sql.WriteString(measureValues)
	sql.WriteString(" FROM ")
	sql.WriteString(prepared)
	sql.WriteString("), ")

	sql.WriteString(canonicalized)
	sql.WriteString(" AS (SELECT ")
	sql.WriteString(bucketNumber)
	sql.WriteString(", ")
	sql.WriteString(kind)
	sql.WriteString(", if(")
	sql.WriteString(kind)
	sql.WriteString(" = 0, ")
	sql.WriteString(label)
	sql.WriteString(", CAST('' AS String)) AS ")
	sql.WriteString(label)
	sql.WriteString(", ")
	sql.WriteString(measureValues)
	sql.WriteString(" FROM ")
	sql.WriteString(classified)
	sql.WriteString("), ")

	sql.WriteString(numericGroups)
	if valueKind == TimechartValueKindPercentile {
		// Retain the GK aggregate state for every raw bucket/split group. Ordinary
		// series scoring finalizes each state independently, while OTHER merges
		// the omitted states before finalization so it remains a true percentile
		// of the combined member population.
		level := statsPercentileLevelSQL(operator.Measure.Percentile)
		sql.WriteString(" AS MATERIALIZED (SELECT ")
		sql.WriteString(bucketNumber)
		sql.WriteString(", ")
		sql.WriteString(kind)
		sql.WriteString(", ")
		sql.WriteString(label)
		sql.WriteString(", ")
		eligibleValues := "if(" + kind + " IN (0, 1), " + measureValues + ", CAST([], 'Array(Float64)'))"
		sql.WriteString("quantilesGKOrNullArrayState(100, ")
		sql.WriteString(level)
		sql.WriteString(")(")
		sql.WriteString(eligibleValues)
		sql.WriteString(") AS ")
		sql.WriteString(percentileState)
		sql.WriteString(", count() AS ")
		sql.WriteString(frequency)
		sql.WriteString(" FROM ")
		sql.WriteString(canonicalized)
		sql.WriteString(" GROUP BY ")
		sql.WriteString(bucketNumber)
		sql.WriteString(", ")
		sql.WriteString(kind)
		sql.WriteString(", ")
		sql.WriteString(label)
		sql.WriteString("), ")
	} else {
		sql.WriteString(" AS MATERIALIZED (SELECT ")
		sql.WriteString(bucketNumber)
		sql.WriteString(", ")
		sql.WriteString(kind)
		sql.WriteString(", ")
		sql.WriteString(label)
		sql.WriteString(", ")
		sql.WriteString("tupleElement(")
		sql.WriteString(numericState)
		sql.WriteString(", 1) AS ")
		sql.WriteString(numerator)
		sql.WriteString(", ")
		sql.WriteString("toUInt64(tupleElement(")
		sql.WriteString(numericState)
		sql.WriteString(", 2)) AS ")
		sql.WriteString(denominator)
		sql.WriteString(", ")
		sql.WriteString(frequency)
		sql.WriteString(" FROM (SELECT ")
		sql.WriteString(bucketNumber)
		sql.WriteString(", ")
		sql.WriteString(kind)
		sql.WriteString(", ")
		sql.WriteString(label)
		sql.WriteString(", ")
		// One mergeable aggregate consumes each normalized immediate-member array
		// exactly once and retains both the Float64 numerator and member count. This
		// avoids ARRAY JOIN and prevents repeated Dynamic-array normalization.
		sql.WriteString("sumCountArray(")
		sql.WriteString(measureValues)
		sql.WriteString(") AS ")
		sql.WriteString(numericState)
		sql.WriteString(", count() AS ")
		sql.WriteString(frequency)
		sql.WriteString(" FROM ")
		sql.WriteString(canonicalized)
		sql.WriteString(" GROUP BY ")
		sql.WriteString(bucketNumber)
		sql.WriteString(", ")
		sql.WriteString(kind)
		sql.WriteString(", ")
		sql.WriteString(label)
		sql.WriteString(") AS ")
		sql.WriteString(q("__os_timechart_numeric_state_source"))
		sql.WriteString("), ")
	}

	sql.WriteString(numericScores)
	sql.WriteString(" AS MATERIALIZED (SELECT ")
	sql.WriteString(label)
	sql.WriteString(", ")
	sql.WriteString(scoreSQL)
	sql.WriteString(" AS ")
	sql.WriteString(score)
	sql.WriteString(" FROM ")
	sql.WriteString(numericGroups)
	sql.WriteString(" WHERE ")
	sql.WriteString(kind)
	sql.WriteString(" = 0 GROUP BY ")
	sql.WriteString(label)
	sql.WriteString(" ORDER BY ")
	// Splunk does not specify computed non-finite score ordering. Pin a stable
	// boundary: +Inf, finite descending, -Inf, NaN, then raw label lexical order.
	sql.WriteString("multiIf(isNaN(")
	sql.WriteString(score)
	sql.WriteString("), toUInt8(0), isInfinite(")
	sql.WriteString(score)
	sql.WriteString(") AND ")
	sql.WriteString(score)
	sql.WriteString(" < 0, toUInt8(1), isInfinite(")
	sql.WriteString(score)
	sql.WriteString("), toUInt8(3), toUInt8(2)) DESC, ")
	sql.WriteString("if(isFinite(")
	sql.WriteString(score)
	sql.WriteString("), ")
	sql.WriteString(score)
	sql.WriteString(", toFloat64(0)) DESC, ")
	sql.WriteString(label)
	sql.WriteString(" ASC LIMIT ")
	sql.WriteString(strconv.FormatUint(uint64(operator.Split.SeriesLimit), 10))
	sql.WriteString("), ")

	sql.WriteString(collapsed)
	sql.WriteString(" AS (SELECT ")
	sql.WriteString(bucketNumber)
	sql.WriteString(", multiIf(")
	sql.WriteString(kind)
	sql.WriteString(" = 1, '1:', ")
	sql.WriteString(label)
	sql.WriteString(" IN (SELECT ")
	sql.WriteString(label)
	sql.WriteString(" FROM ")
	sql.WriteString(numericScores)
	sql.WriteString("), concat('0:', ")
	sql.WriteString(label)
	sql.WriteString("), '2:') AS ")
	sql.WriteString(encoded)
	sql.WriteString(", ")
	if valueKind == TimechartValueKindPercentile {
		level := statsPercentileLevelSQL(operator.Measure.Percentile)
		sql.WriteString("quantilesGKOrNullArrayMerge(100, ")
		sql.WriteString(level)
		sql.WriteString(")(")
		sql.WriteString(percentileState)
		sql.WriteString(") AS ")
		sql.WriteString(percentileValues)
	} else {
		sql.WriteString("sum(")
		sql.WriteString(numerator)
		sql.WriteString(") AS ")
		sql.WriteString(numerator)
		sql.WriteString(", sum(")
		sql.WriteString(denominator)
		sql.WriteString(") AS ")
		sql.WriteString(denominator)
	}
	sql.WriteString(" FROM ")
	sql.WriteString(numericGroups)
	sql.WriteString(" WHERE ")
	sql.WriteString(kind)
	sql.WriteString(" IN (0, 1) GROUP BY ")
	sql.WriteString(bucketNumber)
	sql.WriteString(", ")
	sql.WriteString(encoded)
	sql.WriteString("), ")

	sql.WriteString(finalized)
	sql.WriteString(" AS (SELECT ")
	sql.WriteString(bucketNumber)
	sql.WriteString(", ")
	sql.WriteString(encoded)
	sql.WriteString(", ")
	sql.WriteString(publishSQL)
	sql.WriteString(" AS ")
	sql.WriteString(measureValue)
	sql.WriteString(" FROM ")
	sql.WriteString(collapsed)
	sql.WriteString("), ")

	sql.WriteString(domainRows)
	sql.WriteString(" AS (SELECT toUInt8(0) AS sort_kind, ")
	sql.WriteString(splunkSeriesLabelSQL(label))
	sql.WriteString(" AS ")
	sql.WriteString(sortLabel)
	sql.WriteString(", concat('0:', ")
	sql.WriteString(label)
	sql.WriteString(") AS ")
	sql.WriteString(encoded)
	sql.WriteString(" FROM ")
	sql.WriteString(numericScores)
	sql.WriteString(" UNION ALL SELECT toUInt8(1), CAST('' AS String), CAST('1:' AS String) FROM (SELECT 1 FROM ")
	sql.WriteString(numericGroups)
	sql.WriteString(" WHERE ")
	sql.WriteString(kind)
	sql.WriteString(" = 1 LIMIT 1)")
	sql.WriteString(" UNION ALL SELECT toUInt8(2), CAST('' AS String), CAST('2:' AS String) FROM (SELECT 1 FROM ")
	sql.WriteString(numericGroups)
	sql.WriteString(" WHERE ")
	sql.WriteString(kind)
	sql.WriteString(" = 0 AND ")
	sql.WriteString(label)
	sql.WriteString(" NOT IN (SELECT ")
	sql.WriteString(label)
	sql.WriteString(" FROM ")
	sql.WriteString(numericScores)
	sql.WriteString(") LIMIT 1)), ")

	sql.WriteString(domain)
	sql.WriteString(" AS (SELECT arrayMap(item -> item.3, arraySort(item -> (item.1, item.2), groupArray((sort_kind, ")
	sql.WriteString(sortLabel)
	sql.WriteString(", ")
	sql.WriteString(encoded)
	sql.WriteString(")))) AS names FROM ")
	sql.WriteString(domainRows)
	sql.WriteString("), ")

	sql.WriteString(collisions)
	sql.WriteString(" AS (SELECT toUInt8(count() > 0) AS ")
	sql.WriteString(collision)
	sql.WriteString(" FROM (")
	sql.WriteString("SELECT ")
	sql.WriteString(splunkSeriesLabelSQL(label))
	sql.WriteString(" AS ")
	sql.WriteString(normalized)
	sql.WriteString(" FROM ")
	sql.WriteString(numericGroups)
	sql.WriteString(" WHERE ")
	sql.WriteString(kind)
	sql.WriteString(" = 0 GROUP BY ")
	sql.WriteString(normalized)
	sql.WriteString(" HAVING uniqExact(")
	sql.WriteString(label)
	sql.WriteString(") > 1 LIMIT 1)), ")

	sql.WriteString(bucketMaps)
	sql.WriteString(" AS (SELECT ")
	sql.WriteString(bucketNumber)
	sql.WriteString(", mapFromArrays(groupArray(")
	sql.WriteString(encoded)
	sql.WriteString("), groupArray(ifNull(")
	sql.WriteString(measureValue)
	sql.WriteString(", toFloat64(0)))) AS ")
	sql.WriteString(valueMap)
	sql.WriteString(", ")
	sql.WriteString("mapFromArrays(groupArray(")
	sql.WriteString(encoded)
	sql.WriteString("), groupArray(toUInt8(isNotNull(")
	sql.WriteString(measureValue)
	sql.WriteString(")))) AS ")
	sql.WriteString(presentMap)
	sql.WriteString(" FROM ")
	sql.WriteString(finalized)
	sql.WriteString(" GROUP BY ")
	sql.WriteString(bucketNumber)
	sql.WriteString("), ")

	sql.WriteString(validation)
	sql.WriteString(" AS (SELECT toUInt8(sumIf(")
	sql.WriteString(frequency)
	sql.WriteString(", ")
	sql.WriteString(kind)
	sql.WriteString(" = 3) > 0) AS ")
	sql.WriteString(invalid)
	sql.WriteString(" FROM ")
	sql.WriteString(numericGroups)
	sql.WriteString("), ")

	sql.WriteString(grid)
	sql.WriteString(" AS (")
	sql.WriteString(ordinalGridSQL(ordinal, bucketNumber))
	sql.WriteString(") ")

	sql.WriteString("SELECT ")
	sql.WriteString(grid)
	sql.WriteString(".")
	sql.WriteString(ordinal)
	sql.WriteString(" AS ")
	sql.WriteString(ordinal)
	sql.WriteString(", ")
	sql.WriteString("if(")
	sql.WriteString(grid)
	sql.WriteString(".")
	sql.WriteString(ordinal)
	sql.WriteString(" = 0, ")
	sql.WriteString(domain)
	sql.WriteString(".names, CAST([], 'Array(String)')) AS ")
	sql.WriteString(q(TimechartNamesColumn))
	sql.WriteString(", ")
	sql.WriteString("arrayMap(name -> ifNull(")
	sql.WriteString(bucketMaps)
	sql.WriteString(".")
	sql.WriteString(valueMap)
	sql.WriteString("[name], toFloat64(0)), ")
	sql.WriteString(domain)
	sql.WriteString(".names) AS ")
	sql.WriteString(q(TimechartValuesColumn))
	sql.WriteString(", ")
	sql.WriteString("arrayMap(name -> ifNull(")
	sql.WriteString(bucketMaps)
	sql.WriteString(".")
	sql.WriteString(presentMap)
	sql.WriteString("[name], toUInt8(0)), ")
	sql.WriteString(domain)
	sql.WriteString(".names) AS ")
	sql.WriteString(q(TimechartValuePresentColumn))
	sql.WriteString(", ")
	sql.WriteString("toUInt8(")
	sql.WriteString(validation)
	sql.WriteString(".")
	sql.WriteString(invalid)
	sql.WriteString(" != 0 OR ")
	sql.WriteString(collisions)
	sql.WriteString(".")
	sql.WriteString(collision)
	sql.WriteString(" != 0) AS ")
	sql.WriteString(q(TimechartInvalidColumn))
	sql.WriteString(" FROM ")
	sql.WriteString(grid)
	sql.WriteString(" CROSS JOIN ")
	sql.WriteString(domain)
	sql.WriteString(" CROSS JOIN ")
	sql.WriteString(validation)
	sql.WriteString(" CROSS JOIN ")
	sql.WriteString(collisions)
	sql.WriteString(" LEFT JOIN ")
	sql.WriteString(bucketMaps)
	sql.WriteString(" ON ")
	sql.WriteString(bucketMaps)
	sql.WriteString(".")
	sql.WriteString(bucketNumber)
	sql.WriteString(" = ")
	sql.WriteString(grid)
	sql.WriteString(".")
	sql.WriteString(bucketNumber)
	sql.WriteString(" ORDER BY ")
	sql.WriteString(grid)
	sql.WriteString(".")
	sql.WriteString(ordinal)
	sql.WriteString(" ASC")
	sql.WriteString(materializedCTESettingsSQL)

	args = appendOrdinalGridArgs(args, spanNanoseconds, firstBucketNumber, operator.BucketCount)
	sourceDepth := relationalNodeDepth(relation.depth)
	preparedDepth := relationalNodeDepth(sourceDepth)
	classifiedDepth := relationalNodeDepth(preparedDepth)
	canonicalizedDepth := relationalNodeDepth(classifiedDepth)
	var numericGroupsDepth int
	if valueKind == TimechartValueKindPercentile {
		numericGroupsDepth = relationalNodeDepth(canonicalizedDepth)
	} else {
		numericStateDepth := relationalNodeDepth(canonicalizedDepth)
		numericGroupsDepth = relationalNodeDepth(numericStateDepth)
	}
	numericScoresDepth := relationalNodeDepth(numericGroupsDepth)
	scoreMembershipDepth := relationalNodeDepth(numericScoresDepth)
	collapsedDepth := relationalNodeDepth(numericGroupsDepth, scoreMembershipDepth)
	finalizedDepth := relationalNodeDepth(collapsedDepth)

	domainScoreBranchDepth := relationalNodeDepth(numericScoresDepth)
	domainNullInputDepth := relationalNodeDepth(numericGroupsDepth)
	domainNullBranchDepth := relationalNodeDepth(domainNullInputDepth)
	domainOtherInputDepth := relationalNodeDepth(numericGroupsDepth, scoreMembershipDepth)
	domainOtherBranchDepth := relationalNodeDepth(domainOtherInputDepth)
	domainRowsDepth := relationalNodeDepth(
		domainScoreBranchDepth,
		domainNullBranchDepth,
		domainOtherBranchDepth,
	)
	domainDepth := relationalNodeDepth(domainRowsDepth)
	collisionInputDepth := relationalNodeDepth(numericGroupsDepth)
	collisionsDepth := relationalNodeDepth(collisionInputDepth)
	bucketMapsDepth := relationalNodeDepth(finalizedDepth)
	validationDepth := relationalNodeDepth(numericGroupsDepth)
	gridDepth := relationalNodeDepth()
	resultDepth := relationalNodeDepth(
		gridDepth,
		domainDepth,
		validationDepth,
		collisionsDepth,
		bucketMapsDepth,
	)

	compiled := CompiledQuery{
		SQL:          sql.String(),
		Args:         args,
		OutputFields: slices.Clone(dynamic.FixedFields),
		Timechart: &TimechartOutput{
			Mode:          TimechartModeRuntimeWideValue,
			FirstBucket:   operator.FirstBucket.UTC(),
			Span:          operator.Span,
			BucketCount:   operator.BucketCount,
			MaxSeries:     dynamic.MaxSeries,
			MaxLabelBytes: maxTimechartLabelBytes,
			ValueKind:     valueKind,
		},
	}
	return withCompiledRelationalDepth(compiled, resultDepth, operator.Range), nil
}

func validateTimechartMeasure(operator *plan.Timechart, state compileState) error {
	if operator == nil {
		return errors.New("compile ClickHouse timechart: operator is required")
	}
	measure := operator.Measure
	if err := validateNonStatsAggregateMeasureMetadata("timechart", measure); err != nil {
		return err
	}
	if measure.Predicate != nil {
		return errors.New(
			"compile ClickHouse timechart: aggregate measure contains predicate metadata",
		)
	}
	switch measure.Function {
	case plan.AggregateFunctionCountRows:
		if measure.Input.Name != "" || measure.Input.Canonical ||
			measure.Input.Path != nil || measure.Input.Range != (spl.Range{}) ||
			measure.Percentile != 0 || measure.Output != "count" {
			return errors.New(
				"compile ClickHouse timechart: count measure contract is invalid",
			)
		}
	case plan.AggregateFunctionCountValues:
		if operator.Split != nil &&
			operator.Split.Field.Name == measure.Input.Name {
			return errors.New(
				"compile ClickHouse timechart: aggregate input and split field must differ",
			)
		}
		if measure.Percentile != 0 {
			return errors.New(
				"compile ClickHouse timechart: count(field) contains percentile metadata",
			)
		}
		return validateTimechartFieldMeasure(measure, state, operator.Range)
	case plan.AggregateFunctionPercentile:
		if operator.Split != nil &&
			operator.Split.Field.Name == measure.Input.Name {
			return errors.New(
				"compile ClickHouse timechart: aggregate input and split field must differ",
			)
		}
		if measure.Percentile < 1 || measure.Percentile > 99 {
			return errors.New(
				"compile ClickHouse timechart: percentile must be from 1 through 99",
			)
		}
		return validateTimechartFieldMeasure(measure, state, operator.Range)
	case plan.AggregateFunctionSum,
		plan.AggregateFunctionAverage:
		if operator.Split != nil &&
			operator.Split.Field.Name == measure.Input.Name {
			return errors.New(
				"compile ClickHouse timechart: aggregate input and split field must differ",
			)
		}
		if measure.Percentile != 0 {
			return errors.New(
				"compile ClickHouse timechart: numeric aggregate contains percentile metadata",
			)
		}
		return validateTimechartFieldMeasure(measure, state, operator.Range)
	default:
		return errors.New(
			"compile ClickHouse timechart: aggregate function is unsupported",
		)
	}
	return nil
}

func validateTimechartFieldMeasure(
	measure plan.AggregateMeasure,
	state compileState,
	sourceRange spl.Range,
) error {
	if err := validateCanonicalFieldRef("timechart", "input", measure.Input); err != nil {
		return err
	}
	if _, err := plan.ResolveField(measure.Output, sourceRange); err != nil {
		return fmt.Errorf(
			"compile ClickHouse timechart: invalid output field %q: %w",
			measure.Output,
			err,
		)
	}
	if measure.Output == "_time" {
		return errors.New(
			"compile ClickHouse timechart: field aggregate output contract is invalid",
		)
	}
	if state.eventRows && state.allowDynamic && measure.Input.Name == "fields" {
		return &plan.Diagnostic{
			Code:    "SPL_AMBIGUOUS_TIMECHART_FIELD",
			Message: "timechart cannot read the event result's reserved fields payload without an exact upstream schema",
			Range:   measure.Input.Range,
		}
	}
	return nil
}

func fixedTimechartValueKind(function plan.AggregateFunction) (TimechartValueKind, bool) {
	switch function {
	case plan.AggregateFunctionPercentile:
		return TimechartValueKindPercentile, true
	case plan.AggregateFunctionSum:
		return TimechartValueKindSum, true
	case plan.AggregateFunctionAverage:
		return TimechartValueKindAverage, true
	default:
		return TimechartValueKindInvalid, false
	}
}

func compileFixedCountTimechart(
	relation compiledRelation,
	args []any,
	operator *plan.Timechart,
	timeField fieldState,
	outputFields []string,
	spanNanoseconds int64,
	firstBucketNumber int64,
	alias string,
) (CompiledQuery, error) {
	q := quoteIdentifier
	source := q("__os_timechart_source")
	counts := q("__os_timechart_group_counts")
	grid := q("__os_timechart_grid")
	ticks := q("__os_tc_ticks")
	bucketNumber := q("__os_tc_bucket_number")
	ordinal := q(TimechartOrdinalColumn)
	count := q(TimechartCountColumn)

	var sql strings.Builder
	sql.Grow(len(relation.sql) + 1_536)
	sql.WriteString("WITH ")
	sql.WriteString(source)
	sql.WriteString(" AS (SELECT reinterpretAsInt64(")
	sql.WriteString(timeField.valueSQL)
	sql.WriteString(") AS ")
	sql.WriteString(ticks)
	sql.WriteString(" FROM (")
	sql.WriteString(relation.sql)
	sql.WriteString(") AS ")
	sql.WriteString(alias)
	sql.WriteString("), ")

	writeBucketCountGridSQL(&sql, bucketCountGrid{
		counts:       counts,
		countsSource: source,
		ticks:        ticks,
		bucketNumber: bucketNumber,
		grid:         grid,
		ordinal:      ordinal,
		count:        count,
	})

	args = appendOrdinalGridArgs(args, spanNanoseconds, firstBucketNumber, operator.BucketCount)
	sourceDepth := relationalNodeDepth(relation.depth)
	countsDepth := relationalNodeDepth(sourceDepth)
	gridDepth := relationalNodeDepth()
	resultDepth := relationalNodeDepth(gridDepth, countsDepth)
	compiled := CompiledQuery{
		SQL:          sql.String(),
		Args:         args,
		OutputFields: slices.Clone(outputFields),
		Timechart: &TimechartOutput{
			Mode:          TimechartModeFixedCount,
			FirstBucket:   operator.FirstBucket.UTC(),
			Span:          operator.Span,
			BucketCount:   operator.BucketCount,
			MaxSeries:     1,
			MaxLabelBytes: 0,
		},
	}
	return withCompiledRelationalDepth(compiled, resultDepth, operator.Range), nil
}

func compileFixedCountValueTimechart(
	relation compiledRelation,
	args []any,
	operator *plan.Timechart,
	timeField fieldState,
	measureInputSQL string,
	measureArgs []any,
	outputFields []string,
	spanNanoseconds int64,
	firstBucketNumber int64,
	alias string,
) (CompiledQuery, error) {
	q := quoteIdentifier
	source := q("__os_timechart_source")
	counts := q("__os_timechart_group_counts")
	inputPresence := q("__os_timechart_input_presence")
	grid := q("__os_timechart_grid")
	ticks := q("__os_tc_ticks")
	measureCount := q("__os_tc_measure_count")
	bucketNumber := q("__os_tc_bucket_number")
	upstreamPresent := q(TimechartInputPresentColumn)
	ordinal := q(TimechartOrdinalColumn)
	count := q(TimechartCountColumn)

	var sql strings.Builder
	sql.Grow(len(relation.sql) + len(measureInputSQL) + 2_048)
	sql.WriteString("WITH ")
	sql.WriteString(source)
	sql.WriteString(" AS (SELECT reinterpretAsInt64(")
	sql.WriteString(timeField.valueSQL)
	sql.WriteString(") AS ")
	sql.WriteString(ticks)
	sql.WriteString(", ")
	sql.WriteString(measureInputSQL)
	sql.WriteString(" AS ")
	sql.WriteString(measureCount)
	sql.WriteString(" FROM (")
	sql.WriteString(relation.sql)
	sql.WriteString(") AS ")
	sql.WriteString(alias)
	sql.WriteString("), ")

	// The materialized bucket aggregate is the source's only consumer. Both the
	// fixed grid and the independent upstream-presence proof read this bounded
	// relation, so an all-zero count(field) result never re-runs the scoped scan.
	sql.WriteString(counts)
	sql.WriteString(" AS MATERIALIZED (SELECT ")
	sql.WriteString(epochFloorBucketNumberSQL(ticks))
	sql.WriteString(" AS ")
	sql.WriteString(bucketNumber)
	sql.WriteString(", toUInt64(sum(toUInt128(")
	sql.WriteString(measureCount)
	sql.WriteString("))) AS ")
	sql.WriteString(count)
	sql.WriteString(" FROM ")
	sql.WriteString(source)
	sql.WriteString(" GROUP BY ")
	sql.WriteString(bucketNumber)
	sql.WriteString("), ")

	sql.WriteString(inputPresence)
	sql.WriteString(" AS (SELECT toUInt8(count() > 0) AS ")
	sql.WriteString(upstreamPresent)
	sql.WriteString(" FROM ")
	sql.WriteString(counts)
	sql.WriteString("), ")

	sql.WriteString(grid)
	sql.WriteString(" AS (")
	sql.WriteString(ordinalGridSQL(ordinal, bucketNumber))
	sql.WriteString(") SELECT ")
	sql.WriteString(grid)
	sql.WriteString(".")
	sql.WriteString(ordinal)
	sql.WriteString(" AS ")
	sql.WriteString(ordinal)
	sql.WriteString(", ifNull(")
	sql.WriteString(counts)
	sql.WriteString(".")
	sql.WriteString(count)
	sql.WriteString(", toUInt64(0)) AS ")
	sql.WriteString(count)
	sql.WriteString(", ")
	sql.WriteString(inputPresence)
	sql.WriteString(".")
	sql.WriteString(upstreamPresent)
	sql.WriteString(" AS ")
	sql.WriteString(upstreamPresent)
	sql.WriteString(" FROM ")
	sql.WriteString(grid)
	sql.WriteString(" CROSS JOIN ")
	sql.WriteString(inputPresence)
	sql.WriteString(" LEFT JOIN ")
	sql.WriteString(counts)
	sql.WriteString(" ON ")
	sql.WriteString(counts)
	sql.WriteString(".")
	sql.WriteString(bucketNumber)
	sql.WriteString(" = ")
	sql.WriteString(grid)
	sql.WriteString(".")
	sql.WriteString(bucketNumber)
	sql.WriteString(" ORDER BY ")
	sql.WriteString(grid)
	sql.WriteString(".")
	sql.WriteString(ordinal)
	sql.WriteString(" ASC")
	sql.WriteString(materializedCTESettingsSQL)

	args = prependArguments(measureArgs, args)
	args = appendOrdinalGridArgs(
		args,
		spanNanoseconds,
		firstBucketNumber,
		operator.BucketCount,
	)
	sourceDepth := relationalNodeDepth(relation.depth)
	countsDepth := relationalNodeDepth(sourceDepth)
	inputPresenceDepth := relationalNodeDepth(countsDepth)
	gridDepth := relationalNodeDepth()
	resultDepth := relationalNodeDepth(
		gridDepth,
		countsDepth,
		inputPresenceDepth,
	)
	compiled := CompiledQuery{
		SQL:          sql.String(),
		Args:         args,
		OutputFields: slices.Clone(outputFields),
		Timechart: &TimechartOutput{
			Mode:          TimechartModeFixedFieldCount,
			FirstBucket:   operator.FirstBucket.UTC(),
			Span:          operator.Span,
			BucketCount:   operator.BucketCount,
			MaxSeries:     1,
			MaxLabelBytes: 0,
			ValueField:    operator.Measure.Output,
			ValueKind:     TimechartValueKindInvalid,
		},
	}
	return withCompiledRelationalDepth(compiled, resultDepth, operator.Range), nil
}

func compileFixedValueTimechart(
	relation compiledRelation,
	args []any,
	operator *plan.Timechart,
	valueKind TimechartValueKind,
	timeField fieldState,
	measureInputSQL string,
	measureArgs []any,
	outputFields []string,
	spanNanoseconds int64,
	firstBucketNumber int64,
	alias string,
) (CompiledQuery, error) {
	q := quoteIdentifier
	source := q("__os_timechart_source")
	aggregates := q("__os_timechart_value_groups")
	inputPresence := q("__os_timechart_input_presence")
	grid := q("__os_timechart_grid")
	ticks := q("__os_tc_ticks")
	measureValues := q("__os_tc_measure_values")
	bucketNumber := q("__os_tc_bucket_number")
	measureValue := q("__os_tc_measure_value")
	upstreamPresent := q("__os_tc_input_present")
	ordinal := q(TimechartOrdinalColumn)

	var aggregateValueSQL string
	switch valueKind {
	case TimechartValueKindPercentile:
		aggregateValueSQL = singlePercentileArrayAggregateSQL(
			operator.Measure.Percentile,
			measureValues,
		)
	case TimechartValueKindSum, TimechartValueKindAverage:
		var supported bool
		aggregateValueSQL, supported = numericArrayAggregateSQL(
			operator.Measure.Function,
			measureValues,
		)
		if !supported {
			return CompiledQuery{}, errors.New(
				"compile ClickHouse timechart: fixed value function is invalid",
			)
		}
	default:
		return CompiledQuery{}, errors.New(
			"compile ClickHouse timechart: fixed value kind is invalid",
		)
	}

	var sql strings.Builder
	sql.Grow(len(relation.sql) + len(measureInputSQL) + 2_048)
	sql.WriteString("WITH ")
	sql.WriteString(source)
	sql.WriteString(" AS (SELECT reinterpretAsInt64(")
	sql.WriteString(timeField.valueSQL)
	sql.WriteString(") AS ")
	sql.WriteString(ticks)
	sql.WriteString(", ")
	sql.WriteString(measureInputSQL)
	sql.WriteString(" AS ")
	sql.WriteString(measureValues)
	sql.WriteString(" FROM (")
	sql.WriteString(relation.sql)
	sql.WriteString(") AS ")
	sql.WriteString(alias)
	sql.WriteString("), ")

	sql.WriteString(aggregates)
	sql.WriteString(" AS MATERIALIZED (SELECT ")
	sql.WriteString(epochFloorBucketNumberSQL(ticks))
	sql.WriteString(" AS ")
	sql.WriteString(bucketNumber)
	sql.WriteString(", ")
	sql.WriteString(aggregateValueSQL)
	sql.WriteString(" AS ")
	sql.WriteString(measureValue)
	sql.WriteString(" FROM ")
	sql.WriteString(source)
	sql.WriteString(" GROUP BY ")
	sql.WriteString(bucketNumber)
	sql.WriteString("), ")

	sql.WriteString(inputPresence)
	sql.WriteString(" AS (SELECT toUInt8(count() > 0) AS ")
	sql.WriteString(upstreamPresent)
	sql.WriteString(" FROM ")
	sql.WriteString(aggregates)
	sql.WriteString("), ")

	sql.WriteString(grid)
	sql.WriteString(" AS (")
	sql.WriteString(ordinalGridSQL(ordinal, bucketNumber))
	sql.WriteString(") SELECT ")
	sql.WriteString(grid)
	sql.WriteString(".")
	sql.WriteString(ordinal)
	sql.WriteString(" AS ")
	sql.WriteString(ordinal)
	sql.WriteString(", ")
	sql.WriteString(aggregates)
	sql.WriteString(".")
	sql.WriteString(measureValue)
	sql.WriteString(" AS ")
	sql.WriteString(q(TimechartValueColumn))
	sql.WriteString(", ")
	sql.WriteString(inputPresence)
	sql.WriteString(".")
	sql.WriteString(upstreamPresent)
	sql.WriteString(" AS ")
	sql.WriteString(q(TimechartInputPresentColumn))
	sql.WriteString(" FROM ")
	sql.WriteString(grid)
	sql.WriteString(" CROSS JOIN ")
	sql.WriteString(inputPresence)
	sql.WriteString(" LEFT JOIN ")
	sql.WriteString(aggregates)
	sql.WriteString(" ON ")
	sql.WriteString(aggregates)
	sql.WriteString(".")
	sql.WriteString(bucketNumber)
	sql.WriteString(" = ")
	sql.WriteString(grid)
	sql.WriteString(".")
	sql.WriteString(bucketNumber)
	sql.WriteString(" ORDER BY ")
	sql.WriteString(grid)
	sql.WriteString(".")
	sql.WriteString(ordinal)
	sql.WriteString(" ASC")
	sql.WriteString(materializedCTESettingsSQL)

	args = prependArguments(measureArgs, args)
	args = appendOrdinalGridArgs(
		args,
		spanNanoseconds,
		firstBucketNumber,
		operator.BucketCount,
	)
	sourceDepth := relationalNodeDepth(relation.depth)
	aggregatesDepth := relationalNodeDepth(sourceDepth)
	inputPresenceDepth := relationalNodeDepth(aggregatesDepth)
	gridDepth := relationalNodeDepth()
	resultDepth := relationalNodeDepth(
		gridDepth,
		aggregatesDepth,
		inputPresenceDepth,
	)
	compiled := CompiledQuery{
		SQL:          sql.String(),
		Args:         args,
		OutputFields: slices.Clone(outputFields),
		Timechart: &TimechartOutput{
			Mode:          TimechartModeFixedValue,
			FirstBucket:   operator.FirstBucket.UTC(),
			Span:          operator.Span,
			BucketCount:   operator.BucketCount,
			MaxSeries:     1,
			MaxLabelBytes: 0,
			ValueField:    operator.Measure.Output,
			ValueKind:     valueKind,
		},
	}
	return withCompiledRelationalDepth(compiled, resultDepth, operator.Range), nil
}

// splunkSeriesLabelSQL applies Splunk's leading-underscore VALUE prefix. Series
// labels become public column names, so the prefix decides both their sort
// position and their collision domain. Row values are data and are never
// normalized this way.
func splunkSeriesLabelSQL(label string) string {
	return "if(startsWith(" + label + ", '_'), concat('VALUE', " + label + "), " + label + ")"
}

// chartRowColumnType maps a resolved row field to the exact physical type and
// public value kind of the pivot's first column. It mirrors the group column
// that stats BY publishes for the same field: runtime-typed values converge on
// their lexical scalar text, while fixed columns keep their own scalar type.
// The public name participates because the ordinary result path derives _raw's
// kind from the name as well.
func chartRowColumnType(_ string, field fieldState) (databaseType string, kind ChartRowKind, err error) {
	switch field.kind {
	case fieldKindInvalid, fieldKindString, fieldKindDynamic:
		if field.stringOrBytes {
			// stats count BY _raw publishes a Mixed, nullable column because
			// _raw may hold non-UTF-8 bytes. The pivot's first column is that
			// same group column, so it declares the same kind.
			return "String", ChartRowKindMixed, nil
		}
		// A statically null column (eval x=null) is the String group column
		// stats BY publishes: it never produces a present, non-null value, so
		// it names no rows rather than failing the search.
		return "String", ChartRowKindString, nil
	case fieldKindBool:
		return "Bool", ChartRowKindBool, nil
	case fieldKindTime:
		return "DateTime64(9, 'UTC')", ChartRowKindTime, nil
	case fieldKindNumber:
		switch field.numberType {
		case "Int64":
			return "Int64", ChartRowKindSigned, nil
		case "UInt8", "UInt64":
			return field.numberType, ChartRowKindUnsigned, nil
		case "Float64":
			return "Float64", ChartRowKindDouble, nil
		}
	}
	return "", ChartRowKindInvalid, fmt.Errorf("compile ClickHouse chart: row field has an unsupported fixed type %d/%q", field.kind, field.numberType)
}

// chartValidationRowSQL produces one non-null value of the compiler-validated
// row transport type. It is used only by the private invalid-result sentinel:
// when the row axis is empty, a bad split label still has to cross the storage
// boundary so the executor can reject the whole command before publication.
func chartValidationRowSQL(databaseType string) string {
	if databaseType == "String" {
		return "CAST('' AS String)"
	}
	return "CAST(0 AS " + databaseType + ")"
}

// materializedCTESettingsSQL declares the requirement a lowering takes on when
// it reads an aggregate through a CTE declared `AS MATERIALIZED`. ClickHouse
// honors that declaration only while enable_materialized_cte is on; with it off
// the server silently inlines every reference, which re-runs the whole scoped
// scan once per reference and additionally exposes the analyzer to a
// cross-subquery expression-alias defect that drops a column outright ("Not
// found column multiIf(...)") whenever a search predicate shares expressions
// with the pivot's own projections — for example a comparison on the very field
// that names the column axis. Carrying the setting in the query text keeps the
// compiled SQL correct on any connection rather than only under one caller's
// per-query settings, and it stays correct when the query is wrapped in an
// outer SELECT.
const materializedCTESettingsSQL = " SETTINGS enable_materialized_cte = 1"

// compileChart lowers the bounded runtime-wide pivot. Both axes are runtime
// data, so the scoped scan feeds exactly two aggregations: a one-dimensional
// label aggregate that chooses the published column domain, and a row-keyed
// aggregate whose column axis is already collapsed to that domain. Every later
// stage reads one of those materialized aggregates as an ordinary relation —
// never through a scalar subquery, which ClickHouse evaluates during analysis
// and would therefore re-run the whole scoped scan.
func compileChart(
	relation compiledRelation,
	state compileState,
	args []any,
	operator *plan.Chart,
	dynamic *plan.DynamicSeriesOutput,
	alias string,
) (CompiledQuery, error) {
	if operator == nil {
		return CompiledQuery{}, errors.New("compile ClickHouse chart: operator is required")
	}
	if err := validateNonStatsAggregateMeasureMetadata("chart", operator.Measure); err != nil {
		return CompiledQuery{}, err
	}
	switch operator.Measure.Function {
	case plan.AggregateFunctionCountRows, plan.AggregateFunctionCountValues:
		return compileCountChart(relation, state, args, operator, dynamic, alias)
	case plan.AggregateFunctionPercentile,
		plan.AggregateFunctionSum,
		plan.AggregateFunctionAverage:
		return compileNumericChart(relation, state, args, operator, dynamic, alias)
	default:
		return CompiledQuery{}, errors.New("compile ClickHouse chart: aggregate function is unsupported")
	}
}

// resolveChartAxes revalidates the chart bounding contract and resolves the row
// and column axis fields shared by the count and numeric chart compilers.
func resolveChartAxes(
	operator *plan.Chart,
	dynamic *plan.DynamicSeriesOutput,
	state compileState,
) (fieldState, string, ChartRowKind, fieldState, error) {
	rowName := operator.Over.Name
	if dynamic == nil || !slices.Equal(dynamic.FixedFields, []string{rowName}) || dynamic.MaxSeries == 0 {
		return fieldState{}, "", 0, fieldState{}, errors.New("compile ClickHouse chart: dynamic output contract is invalid")
	}
	// The plan carries the complete bounding contract as data precisely so the
	// backend can revalidate it before emitting SQL.
	if rowName == "" || operator.SplitBy.Name == "" || rowName == operator.SplitBy.Name ||
		operator.RowLimit != maxChartRowValues || operator.SeriesLimit != 10 ||
		dynamic.MaxSeries != 12 || uint32(operator.SeriesLimit)+2 != uint32(dynamic.MaxSeries) ||
		!operator.IncludeNull || !operator.IncludeOther ||
		operator.NullLabel != "NULL" || operator.OtherLabel != "OTHER" {
		return fieldState{}, "", 0, fieldState{}, errors.New("compile ClickHouse chart: bounded defaults are invalid")
	}
	for _, axis := range []plan.FieldRef{operator.Over, operator.SplitBy} {
		if err := validateCanonicalFieldRef("chart", "axis", axis); err != nil {
			return fieldState{}, "", 0, fieldState{}, err
		}
		if axis.Name == operator.NullLabel || axis.Name == operator.OtherLabel {
			return fieldState{}, "", 0, fieldState{}, &plan.Diagnostic{
				Code:    "SPL_UNSUPPORTED_CHART_FIELD_TYPE",
				Message: "NULL and OTHER are reserved chart series names",
				Range:   axis.Range,
			}
		}
		if state.eventRows && state.allowDynamic && axis.Name == "fields" {
			return fieldState{}, "", 0, fieldState{}, &plan.Diagnostic{
				Code:    "SPL_UNSUPPORTED_CHART_FIELD_TYPE",
				Message: "chart cannot use the event result's reserved fields payload without an exact upstream schema",
				Range:   axis.Range,
			}
		}
	}

	rowField, rowResolved, err := resolveCompiledField(operator.Over, state)
	if err != nil {
		return fieldState{}, "", 0, fieldState{}, err
	}
	if !rowResolved {
		// An upstream projection removed the row field, so no row value is
		// present. stats BY emits no groups in that case; keep the declared
		// one-column schema instead of resurrecting the private document.
		rowField = fieldState{
			valueSQL:  "CAST(NULL AS Nullable(String))",
			existsSQL: "0",
			kind:      fieldKindString,
		}
	}
	if isNativeMultivalueKind(rowField.kind) {
		return fieldState{}, "", 0, fieldState{}, unsupportedMultivalueUsage("chart row field", operator.Over.Range)
	}
	rowDatabaseType, rowKind, err := chartRowColumnType(rowName, rowField)
	if err != nil {
		return fieldState{}, "", 0, fieldState{}, err
	}
	if rowKind == ChartRowKindMixed && rowField.semanticBytesSQL == "" {
		return fieldState{}, "", 0, fieldState{}, errors.New(
			"compile ClickHouse chart: Mixed row lacks semantic Bytes provenance",
		)
	}

	splitField, splitResolved, err := resolveCompiledField(operator.SplitBy, state)
	if err != nil {
		return fieldState{}, "", 0, fieldState{}, err
	}
	if !splitResolved {
		// A projected-away column field is missing for every retained row, so
		// the documented usenull=true default produces one NULL column.
		splitField = fieldState{
			valueSQL:  "CAST(NULL AS Nullable(String))",
			existsSQL: "0",
			kind:      fieldKindString,
		}
	}
	if isNativeMultivalueKind(splitField.kind) {
		return fieldState{}, "", 0, fieldState{}, unsupportedMultivalueUsage("chart column field", operator.SplitBy.Range)
	}
	if splitField.kind == fieldKindInvalid {
		// A statically null column field (eval x=null) is inside the documented
		// column domain "string column values plus missing/explicit-null": it
		// carries no present, non-null value on any row, exactly like the
		// projected-away field above. fieldKindInvalid is unconditionally null
		// everywhere else in the compiler too — its stored semantic type is the
		// constant Null — so the pivot reads it as the same typed NULL and
		// publishes one usenull=true NULL series instead of failing the search.
		splitField = fieldState{
			valueSQL:   "CAST(NULL AS Nullable(String))",
			existsSQL:  splitField.existsSQL,
			existsArgs: splitField.existsArgs,
			kind:       fieldKindString,
		}
	}
	if splitField.kind != fieldKindString && splitField.kind != fieldKindDynamic {
		return fieldState{}, "", 0, fieldState{}, &plan.Diagnostic{
			Code:        "SPL_UNSUPPORTED_CHART_FIELD_TYPE",
			Message:     "chart column fields currently support strings plus missing and null values",
			Range:       operator.SplitBy.Range,
			Suggestions: []string{"convert the column field to a string before chart"},
		}
	}
	return rowField, rowDatabaseType, rowKind, splitField, nil
}

func compileCountChart(
	relation compiledRelation,
	state compileState,
	args []any,
	operator *plan.Chart,
	dynamic *plan.DynamicSeriesOutput,
	alias string,
) (CompiledQuery, error) {
	if operator == nil || operator.Measure.Predicate != nil {
		return CompiledQuery{}, errors.New("compile ClickHouse chart: count operator is required")
	}
	fieldOccurrenceCount := operator.Measure.Function == plan.AggregateFunctionCountValues
	switch operator.Measure.Function {
	case plan.AggregateFunctionCountRows:
		if operator.Measure.Input.Name != "" || operator.Measure.Input.Canonical ||
			operator.Measure.Input.Path != nil || operator.Measure.Input.Range != (spl.Range{}) ||
			operator.Measure.Percentile != 0 || operator.Measure.Output != "count" {
			return CompiledQuery{}, errors.New("compile ClickHouse chart: row count contract is invalid")
		}
	case plan.AggregateFunctionCountValues:
		if operator.Measure.Percentile != 0 ||
			operator.Measure.Output != "count("+operator.Measure.Input.Name+")" ||
			operator.Measure.Input.Name == operator.Over.Name ||
			!spl.IsExactUnquotedFieldName(operator.Measure.Input.Name) {
			return CompiledQuery{}, errors.New("compile ClickHouse chart: field count contract is invalid")
		}
		if err := validateCanonicalFieldRef("chart", "input", operator.Measure.Input); err != nil {
			return CompiledQuery{}, err
		}
		if state.eventRows && state.allowDynamic && operator.Measure.Input.Name == "fields" {
			return CompiledQuery{}, &plan.Diagnostic{
				Code:    "SPL_AMBIGUOUS_CHART_FIELD",
				Message: "chart cannot read the event result's reserved fields payload without an exact upstream schema",
				Range:   operator.Measure.Input.Range,
			}
		}
	default:
		return CompiledQuery{}, errors.New("compile ClickHouse chart: count operator is required")
	}
	rowName := operator.Over.Name
	rowField, rowDatabaseType, rowKind, splitField, err := resolveChartAxes(operator, dynamic, state)
	if err != nil {
		return CompiledQuery{}, err
	}
	measureInputSQL := ""
	var measureArgs []any
	if fieldOccurrenceCount {
		var resolveErr error
		measureInputSQL, measureArgs, resolveErr = resolveCountValueInput(
			operator.Measure.Input,
			state,
		)
		if resolveErr != nil {
			return CompiledQuery{}, resolveErr
		}
	}

	rowExistsSQL := rowField.existsSQL
	if rowExistsSQL == "" {
		rowExistsSQL = "1"
	}
	splitExistsSQL := splitField.existsSQL
	if splitExistsSQL == "" {
		splitExistsSQL = "1"
	}
	rowDynamic := rowField.kind == fieldKindDynamic
	splitDynamic := splitField.kind == fieldKindDynamic
	rowHasDescendant := rowDynamic && rowField.descendantSQL != ""
	splitHasDescendant := splitDynamic && splitField.descendantSQL != ""

	q := quoteIdentifier
	source := q("__os_chart_source")
	prepared := q("__os_chart_prepared")
	kinded := q("__os_chart_kinded")
	classified := q("__os_chart_classified")
	canonicalized := q("__os_chart_canonicalized")
	labelTotals := q("__os_chart_label_totals")
	counts := q("__os_chart_group_counts")
	top := q("__os_chart_top")
	collapsed := q("__os_chart_collapsed")
	labelGroups := q("__os_chart_label_groups")
	normalizedGroups := q("__os_chart_normalized_groups")
	authority := q("__os_chart_authority")
	labelExpanded := q("__os_chart_label_expanded")
	expanded := q("__os_chart_expanded")
	domainRows := q("__os_chart_domain_rows")
	domain := q("__os_chart_domain")
	collisions := q("__os_chart_normalization_collisions")
	columnCheck := q("__os_chart_column_check")
	rowMaps := q("__os_chart_row_maps")
	validation := q("__os_chart_validation")
	rowDomain := q("__os_chart_row_domain")

	rowValue := q("__os_ch_row_value")
	rowSemanticBytes := q("__os_ch_row_semantic_bytes")
	rowExact := q("__os_ch_row_exact")
	rowType := q("__os_ch_row_type")
	rowPresent := q("__os_ch_row_present")
	rowEligible := q("__os_ch_row_eligible")
	rowSupported := q("__os_ch_row_supported")
	rowInvalid := q("__os_ch_row_invalid")
	row := q("__os_ch_row")
	value := q("__os_ch_value")
	present := q("__os_ch_present")
	descendant := q("__os_ch_descendant")
	valueType := q("__os_ch_value_type")
	label := q("__os_ch_label")
	kind := q("__os_ch_kind")
	measureCount := q("__os_ch_measure_count")
	rowCount := q("__os_ch_row_count")
	occurrenceCount := q("__os_ch_occurrence_count")
	occurrenceScore := q("__os_ch_occurrence_score")
	frequency := q("__os_ch_count")
	collapsedCount := q("__os_ch_collapsed_count")
	encoded := q("__os_ch_encoded")
	normalized := q("__os_ch_normalized")
	groupRow := q("__os_ch_group_row")
	seriesScore := q("__os_ch_series_score")
	rowEntries := q("__os_ch_row_entries")
	labelRecords := q("__os_ch_label_records")
	authorityValue := q("__os_ch_authority")
	globalCollision := q("__os_ch_global_collision")
	rowInvalidEvidence := q("__os_ch_row_invalid_evidence")
	columnInvalidEvidence := q("__os_ch_column_invalid_evidence")
	collisionEvidence := q("__os_ch_collision_evidence")
	sortLabel := q("__os_ch_sort_label")
	countMap := q("__os_ch_count_map")
	domainNames := q("__os_ch_domain_names")
	invalid := q("__os_ch_invalid")
	collision := q("__os_ch_collision")
	columnInvalid := q("__os_ch_column_invalid")
	transportInvalid := q("__os_ch_transport_invalid")
	ordinal := q(ChartOrdinalColumn)

	// Placeholder order follows CTE nesting, not declaration order. Exact
	// presence probes sit in the outer CTE that wraps the scoped fragment and
	// therefore precede every nested argument; descendant detection and the
	// reserved-column-name probe are emitted afterwards and append in the order
	// they appear.
	prefixArgs := make([]any, 0, len(rowField.existsArgs)+len(splitField.existsArgs)+len(measureArgs))
	prefixArgs = append(prefixArgs, rowField.existsArgs...)
	prefixArgs = append(prefixArgs, splitField.existsArgs...)
	prefixArgs = append(prefixArgs, measureArgs...)
	args = prependArguments(prefixArgs, args)
	if rowHasDescendant {
		args = append(args, rowField.descendantArgs...)
	}
	if splitHasDescendant {
		args = append(args, splitField.descendantArgs...)
	}
	args = append(args, rowName)

	splitTypeSQL := "if(isNull(" + splitField.valueSQL + "), 'None', 'String')"
	if splitDynamic {
		splitTypeSQL = dynamicTypeExpression(splitField)
	}
	// The label is invalid when it cannot name a public column. The row
	// column's own name seeds that collision domain exactly as _time does for
	// timechart, because a runtime value equal to it would duplicate column 0.
	validLabel := "isValidUTF8(" + label + ") AND length(" + label + ") BETWEEN 1 AND " +
		strconv.Itoa(maxTimechartLabelBytes) + " AND " + label + " NOT IN ('NULL', 'OTHER') AND " +
		splunkSeriesLabelSQL(label) + " != ?"

	// Exact presence is materialized once in the source CTE. Re-reading the
	// column keeps each bind marker to exactly one occurrence.
	rowPresenceSQL := "(" + rowExact + " != 0 AND isNotNull(" + rowValue + "))"
	if rowHasDescendant {
		// Non-empty objects are stored as flattened leaf paths, so the parent
		// itself is absent. Retain those rows until the container check rejects
		// them explicitly rather than silently dropping a whole group.
		rowPresenceSQL = "(" + rowPresenceSQL + " OR " + rowField.descendantSQL + ")"
	}
	rowKeySQL := "CAST(assumeNotNull(" + rowValue + ") AS " + rowDatabaseType + ")"
	rowSupportedSQL := "1"
	if rowDynamic {
		// SPL groups by lexical value, so runtime scalar storage types converge
		// on the same row exactly as stats BY converges them. Unsupported
		// containers collapse into one placeholder key and raise the atomic
		// invalid flag instead of naming a row.
		runtime := fieldState{valueSQL: rowValue, dynamicTypeSQL: rowType, kind: fieldKindDynamic}
		supported, lexical := statsByScalarExpressions(runtime)
		rowSupportedSQL = supported
		rowKeySQL = "CAST(if(" + rowSupported + " != 0, " + lexical + ", '') AS String)"
	}
	if rowKind == ChartRowKindMixed {
		rowKeySQL = "tuple(" + rowKeySQL + ", " + rowSemanticBytes + ")"
	}
	rowSortSQL := row
	if rowDynamic || rowField.numericSort {
		// Automatic numeric-aware ordering: the exact order sort 0 +<field>
		// produces on the published column.
		rowSortSQL = dynamicSortValue(row, false)
	}

	var sql strings.Builder
	sql.Grow(len(relation.sql) + 8_192)
	sql.WriteString("WITH ")
	sql.WriteString(source)
	sql.WriteString(" AS (SELECT ")
	sql.WriteString(rowField.valueSQL)
	sql.WriteString(" AS ")
	sql.WriteString(rowValue)
	sql.WriteString(", ")
	sql.WriteString("toUInt8(")
	sql.WriteString(rowExistsSQL)
	sql.WriteString(") AS ")
	sql.WriteString(rowExact)
	sql.WriteString(", ")
	if rowKind == ChartRowKindMixed {
		sql.WriteString("toUInt8(ifNull(")
		sql.WriteString(rowField.semanticBytesSQL)
		sql.WriteString(", 0)) AS ")
		sql.WriteString(rowSemanticBytes)
		sql.WriteString(", ")
	}
	if rowDynamic {
		sql.WriteString(dynamicTypeExpression(rowField))
		sql.WriteString(" AS ")
		sql.WriteString(rowType)
		sql.WriteString(", ")
	}
	sql.WriteString(splitField.valueSQL)
	sql.WriteString(" AS ")
	sql.WriteString(value)
	sql.WriteString(", ")
	sql.WriteString("toUInt8(")
	sql.WriteString(splitExistsSQL)
	sql.WriteString(") AS ")
	sql.WriteString(present)
	sql.WriteString(", ")
	sql.WriteString(splitTypeSQL)
	sql.WriteString(" AS ")
	sql.WriteString(valueType)
	if fieldOccurrenceCount {
		sql.WriteString(", ")
		sql.WriteString(measureInputSQL)
		sql.WriteString(" AS ")
		sql.WriteString(measureCount)
	}
	for _, column := range pivotDescendantSourceColumns(state, rowField, splitField) {
		sql.WriteString(", ")
		sql.WriteString(column)
	}
	sql.WriteString(" FROM (")
	sql.WriteString(relation.sql)
	sql.WriteString(") AS ")
	sql.WriteString(alias)
	sql.WriteString("), ")

	sql.WriteString(prepared)
	sql.WriteString(" AS (SELECT *, ")
	sql.WriteString("toUInt8(")
	sql.WriteString(rowPresenceSQL)
	sql.WriteString(") AS ")
	sql.WriteString(rowPresent)
	sql.WriteString(", ")
	if splitHasDescendant {
		sql.WriteString("toUInt8(if(")
		sql.WriteString(present)
		sql.WriteString(" != 0, 0, ")
		sql.WriteString(splitField.descendantSQL)
		sql.WriteString(")) AS ")
		sql.WriteString(descendant)
		sql.WriteString(", ")
	} else {
		sql.WriteString("toUInt8(0) AS ")
		sql.WriteString(descendant)
		sql.WriteString(", ")
	}
	sql.WriteString("toUInt8(")
	sql.WriteString(rowSupportedSQL)
	sql.WriteString(") AS ")
	sql.WriteString(rowSupported)
	sql.WriteString(", ")
	sql.WriteString("if(")
	sql.WriteString(present)
	sql.WriteString(" != 0 AND isNotNull(")
	sql.WriteString(value)
	sql.WriteString(") AND ")
	sql.WriteString(valueType)
	sql.WriteString(" = 'String', ")
	sql.WriteString("assumeNotNull(toString(")
	sql.WriteString(value)
	sql.WriteString(")), CAST('' AS String)) AS ")
	sql.WriteString(label)
	sql.WriteString(" FROM ")
	sql.WriteString(source)
	sql.WriteString("), ")

	// The column value is classified before row eligibility is considered. A
	// container, a non-string scalar, or an unusable label fails the whole
	// command on its own presence, exactly as compileAggregate validates each
	// BY key independently: an unsupported column value must not become
	// invisible because some other event happened to omit the row field.
	sql.WriteString(kinded)
	sql.WriteString(" AS (SELECT *, ")
	sql.WriteString("multiIf(")
	sql.WriteString(descendant)
	sql.WriteString(" != 0, toUInt8(3), ")
	sql.WriteString(present)
	sql.WriteString(" = 0 OR isNull(")
	sql.WriteString(value)
	sql.WriteString(") OR ")
	sql.WriteString(valueType)
	sql.WriteString(" = 'None', toUInt8(1), ")
	sql.WriteString(valueType)
	sql.WriteString(" != 'String', toUInt8(3), NOT (")
	sql.WriteString(validLabel)
	sql.WriteString("), toUInt8(3), toUInt8(0)) AS ")
	sql.WriteString(kind)
	sql.WriteString(" FROM ")
	sql.WriteString(prepared)
	sql.WriteString("), ")

	// Row eligibility matches stats BY exactly: only present, non-null row
	// values name a row, which is what makes the per-row totals equal
	// stats count BY <row field>. Ineligible rows are retained here so the
	// column-axis rejection above still sees them, and are dropped by the
	// row-keyed aggregation below.
	sql.WriteString(classified)
	sql.WriteString(" AS (SELECT ")
	sql.WriteString(rowKeySQL)
	sql.WriteString(" AS ")
	sql.WriteString(row)
	sql.WriteString(", toUInt8(")
	sql.WriteString(rowSupported)
	sql.WriteString(" = 0) AS ")
	sql.WriteString(rowInvalid)
	sql.WriteString(", ")
	sql.WriteString(rowPresent)
	sql.WriteString(" AS ")
	sql.WriteString(rowEligible)
	sql.WriteString(", ")
	sql.WriteString(kind)
	sql.WriteString(", ")
	sql.WriteString(label)
	if fieldOccurrenceCount {
		sql.WriteString(", ")
		sql.WriteString(measureCount)
	}
	sql.WriteString(" FROM ")
	sql.WriteString(kinded)
	sql.WriteString("), ")

	sql.WriteString(canonicalized)
	sql.WriteString(" AS (SELECT ")
	sql.WriteString(row)
	sql.WriteString(", ")
	sql.WriteString(rowInvalid)
	sql.WriteString(", ")
	sql.WriteString(rowEligible)
	sql.WriteString(", ")
	sql.WriteString(kind)
	sql.WriteString(", if(")
	sql.WriteString(kind)
	sql.WriteString(" = 0, ")
	sql.WriteString(label)
	sql.WriteString(", CAST('' AS String)) AS ")
	sql.WriteString(label)
	if fieldOccurrenceCount {
		sql.WriteString(", ")
		sql.WriteString(measureCount)
	}
	sql.WriteString(" FROM ")
	sql.WriteString(classified)
	sql.WriteString("), ")

	if fieldOccurrenceCount {
		// Field occurrence count cannot select its label domain before it knows
		// the measure totals. Materialize one bounded raw (row, label) aggregate
		// carrying both source-row frequency and a wide occurrence total; every
		// later domain, score, validation, and cell operation reads this relation.
		// This is the same one-scan topology used by numeric chart and avoids
		// materializing the unbounded per-event canonicalized relation.
		sql.WriteString(counts)
		sql.WriteString(" AS MATERIALIZED (SELECT ")
		sql.WriteString(row)
		sql.WriteString(", ")
		sql.WriteString(rowEligible)
		sql.WriteString(", ")
		sql.WriteString(kind)
		sql.WriteString(", ")
		sql.WriteString(label)
		sql.WriteString(", ")
		sql.WriteString("max(")
		sql.WriteString(rowInvalid)
		sql.WriteString(") AS ")
		sql.WriteString(rowInvalid)
		sql.WriteString(", count() AS ")
		sql.WriteString(rowCount)
		sql.WriteString(", ")
		sql.WriteString("sum(toUInt128(")
		sql.WriteString(measureCount)
		sql.WriteString(")) AS ")
		sql.WriteString(occurrenceCount)
		sql.WriteString(" FROM ")
		sql.WriteString(canonicalized)
		sql.WriteString(" GROUP BY ")
		sql.WriteString(row)
		sql.WriteString(", ")
		sql.WriteString(rowEligible)
		sql.WriteString(", ")
		sql.WriteString(kind)
		sql.WriteString(", ")
		sql.WriteString(label)
		sql.WriteString("), ")

		sql.WriteString(labelTotals)
		sql.WriteString(" AS MATERIALIZED (SELECT ")
		sql.WriteString(kind)
		sql.WriteString(", ")
		sql.WriteString(label)
		sql.WriteString(", ")
		sql.WriteString("sumIf(")
		sql.WriteString(rowCount)
		sql.WriteString(", ")
		sql.WriteString(rowEligible)
		sql.WriteString(" != 0) AS ")
		sql.WriteString(rowCount)
		sql.WriteString(", ")
		sql.WriteString("sumIf(")
		sql.WriteString(occurrenceCount)
		sql.WriteString(", ")
		sql.WriteString(rowEligible)
		sql.WriteString(" != 0) AS ")
		sql.WriteString(occurrenceScore)
		sql.WriteString(" FROM ")
		sql.WriteString(counts)
		sql.WriteString(" GROUP BY ")
		sql.WriteString(kind)
		sql.WriteString(", ")
		sql.WriteString(label)
		sql.WriteString("), ")

		sql.WriteString(top)
		sql.WriteString(" AS MATERIALIZED (SELECT ")
		sql.WriteString(label)
		sql.WriteString(", ")
		sql.WriteString(occurrenceScore)
		sql.WriteString(" FROM ")
		sql.WriteString(labelTotals)
		sql.WriteString(" WHERE ")
		sql.WriteString(kind)
		sql.WriteString(" = 0 AND ")
		sql.WriteString(rowCount)
		sql.WriteString(" > 0 ORDER BY ")
		sql.WriteString(occurrenceScore)
		sql.WriteString(" DESC, ")
		sql.WriteString(label)
		sql.WriteString(" ASC LIMIT ")
		sql.WriteString(strconv.FormatUint(uint64(operator.SeriesLimit), 10))
		sql.WriteString("), ")

		sql.WriteString(collapsed)
		sql.WriteString(" AS (SELECT ")
		sql.WriteString(row)
		sql.WriteString(", multiIf(")
		sql.WriteString(kind)
		sql.WriteString(" = 1, '1:', ")
		sql.WriteString(label)
		sql.WriteString(" IN (SELECT ")
		sql.WriteString(label)
		sql.WriteString(" FROM ")
		sql.WriteString(top)
		sql.WriteString("), concat('0:', ")
		sql.WriteString(label)
		sql.WriteString("), '2:') AS ")
		sql.WriteString(encoded)
		sql.WriteString(", ")
		sql.WriteString("toUInt64(sum(toUInt128(")
		sql.WriteString(occurrenceCount)
		sql.WriteString("))) AS ")
		sql.WriteString(frequency)
		sql.WriteString(" FROM ")
		sql.WriteString(counts)
		sql.WriteString(" WHERE ")
		sql.WriteString(rowEligible)
		sql.WriteString(" != 0 AND ")
		sql.WriteString(kind)
		sql.WriteString(" IN (0, 1) GROUP BY ")
		sql.WriteString(row)
		sql.WriteString(", ")
		sql.WriteString(encoded)
		sql.WriteString("), ")
	} else {
		// Bound the exact raw (row, label) work before any array retains label
		// state. max_rows_to_group_by therefore fails the whole query at the
		// executor's 130k chart allowance instead of letting attacker-controlled
		// label cardinality hide inside an unbounded aggregate array.
		sql.WriteString(counts)
		sql.WriteString(" AS MATERIALIZED (SELECT ")
		sql.WriteString(row)
		sql.WriteString(", ")
		sql.WriteString(rowEligible)
		sql.WriteString(", ")
		sql.WriteString(kind)
		sql.WriteString(", ")
		sql.WriteString(label)
		sql.WriteString(", ")
		sql.WriteString("max(")
		sql.WriteString(rowInvalid)
		sql.WriteString(") AS ")
		sql.WriteString(rowInvalid)
		sql.WriteString(", count() AS ")
		sql.WriteString(frequency)
		sql.WriteString(" FROM ")
		sql.WriteString(canonicalized)
		sql.WriteString(" GROUP BY ")
		sql.WriteString(row)
		sql.WriteString(", ")
		sql.WriteString(rowEligible)
		sql.WriteString(", ")
		sql.WriteString(kind)
		sql.WriteString(", ")
		sql.WriteString(label)
		sql.WriteString("), ")

		// Every array below is downstream of the materialized raw-pair bound. Each
		// raw group enters exactly one rowEntries array; later arrays only nest
		// those disjoint arrays and therefore retain at most the raw group count.
		// Zero-score labels remain in the normalization domain, while only positive
		// row-eligible UInt128 scores choose the public top ten.
		sql.WriteString(labelGroups)
		sql.WriteString(" AS (SELECT ")
		sql.WriteString(kind)
		sql.WriteString(", ")
		sql.WriteString(label)
		sql.WriteString(", sumIf(toUInt128(")
		sql.WriteString(frequency)
		sql.WriteString("), ")
		sql.WriteString(rowEligible)
		sql.WriteString(" != 0) AS ")
		sql.WriteString(seriesScore)
		sql.WriteString(", ")
		sql.WriteString("groupArray(tuple(")
		sql.WriteString(row)
		sql.WriteString(", ")
		sql.WriteString(rowEligible)
		sql.WriteString(", ")
		sql.WriteString(rowInvalid)
		sql.WriteString(", ")
		sql.WriteString(frequency)
		sql.WriteString(")) AS ")
		sql.WriteString(rowEntries)
		sql.WriteString(" FROM ")
		sql.WriteString(counts)
		sql.WriteString(" GROUP BY ")
		sql.WriteString(kind)
		sql.WriteString(", ")
		sql.WriteString(label)
		sql.WriteString("), ")

		sql.WriteString(normalizedGroups)
		sql.WriteString(" AS (SELECT ")
		sql.WriteString(kind)
		sql.WriteString(", ")
		sql.WriteString(splunkSeriesLabelSQL(label))
		sql.WriteString(" AS ")
		sql.WriteString(normalized)
		sql.WriteString(", ")
		sql.WriteString("groupArray(tuple(")
		sql.WriteString(kind)
		sql.WriteString(", ")
		sql.WriteString(label)
		sql.WriteString(", ")
		sql.WriteString(seriesScore)
		sql.WriteString(", ")
		sql.WriteString(rowEntries)
		sql.WriteString(")) AS ")
		sql.WriteString(labelRecords)
		sql.WriteString(", ")
		sql.WriteString("toUInt8(")
		sql.WriteString(kind)
		sql.WriteString(" = 0 AND count() > 1) AS ")
		sql.WriteString(collisionEvidence)
		sql.WriteString(" FROM ")
		sql.WriteString(labelGroups)
		sql.WriteString(" GROUP BY ")
		sql.WriteString(kind)
		sql.WriteString(", ")
		sql.WriteString(normalized)
		sql.WriteString("), ")

		recordsName := "__os_ch_records"
		recordName := "__os_ch_record"
		topName := "__os_ch_top_label_values"
		collisionName := "__os_ch_collision_flag"
		recordsAggregate := "arrayFlatten(groupArray(" + labelRecords + "))"
		positiveRecords := "arrayFilter(" + recordName + " -> " + recordName + ".1 = toUInt8(0) AND " + recordName + ".3 > toUInt128(0), " + recordsName + ")"
		topRecords := "arraySlice(arraySort(" + recordName + " -> tuple(-toInt256(" + recordName + ".3), " + recordName + ".2), " + positiveRecords + "), 1, " + strconv.FormatUint(uint64(operator.SeriesLimit), 10) + ")"
		topLabelValues := "arrayMap(" + recordName + " -> " + recordName + ".2, " + topRecords + ")"
		authorizedRecords := "arrayMap(" + recordName + " -> tuple(" + recordName + ".1, " + recordName + ".4, multiIf(" +
			recordName + ".1 = toUInt8(1), CAST('1:' AS String), " +
			recordName + ".1 = toUInt8(0) AND has(" + topName + ", " + recordName + ".2), concat('0:', " + recordName + ".2), " +
			recordName + ".1 = toUInt8(0), CAST('2:' AS String), CAST('' AS String))), " + recordsName + ")"
		authorityExpression := "arrayElement(arrayMap((" + recordsName + ", " + collisionName + ") -> arrayElement(arrayMap(" + topName + " -> tuple(" +
			authorizedRecords + ", " + collisionName + "), [" + topLabelValues + "]), 1), [" + recordsAggregate + "], [toUInt8(maxOrDefault(" + collisionEvidence + ") != 0)]), 1)"
		sql.WriteString(authority)
		sql.WriteString(" AS (SELECT ")
		sql.WriteString(authorityExpression)
		sql.WriteString(" AS ")
		sql.WriteString(authorityValue)
		sql.WriteString(" FROM ")
		sql.WriteString(normalizedGroups)
		sql.WriteString("), ")

		labelRecord := q("__os_ch_label_record")
		sql.WriteString(labelExpanded)
		sql.WriteString(" AS (SELECT tupleElement(")
		sql.WriteString(labelRecord)
		sql.WriteString(", 1) AS ")
		sql.WriteString(kind)
		sql.WriteString(", tupleElement(")
		sql.WriteString(labelRecord)
		sql.WriteString(", 2) AS ")
		sql.WriteString(rowEntries)
		sql.WriteString(", ")
		sql.WriteString("tupleElement(")
		sql.WriteString(labelRecord)
		sql.WriteString(", 3) AS ")
		sql.WriteString(encoded)
		sql.WriteString(", tupleElement(")
		sql.WriteString(authorityValue)
		sql.WriteString(", 2) AS ")
		sql.WriteString(globalCollision)
		sql.WriteString(" FROM ")
		sql.WriteString(authority)
		sql.WriteString(" ARRAY JOIN tupleElement(")
		sql.WriteString(authorityValue)
		sql.WriteString(", 1) AS ")
		sql.WriteString(labelRecord)
		sql.WriteString("), ")

		rowEntry := q("__os_ch_row_entry")
		sql.WriteString(expanded)
		sql.WriteString(" AS (SELECT tupleElement(")
		sql.WriteString(rowEntry)
		sql.WriteString(", 1) AS ")
		sql.WriteString(row)
		sql.WriteString(", tupleElement(")
		sql.WriteString(rowEntry)
		sql.WriteString(", 2) AS ")
		sql.WriteString(rowEligible)
		sql.WriteString(", ")
		sql.WriteString("tupleElement(")
		sql.WriteString(rowEntry)
		sql.WriteString(", 3) AS ")
		sql.WriteString(rowInvalid)
		sql.WriteString(", tupleElement(")
		sql.WriteString(rowEntry)
		sql.WriteString(", 4) AS ")
		sql.WriteString(frequency)
		sql.WriteString(", ")
		sql.WriteString(kind)
		sql.WriteString(", ")
		sql.WriteString(encoded)
		sql.WriteString(", ")
		sql.WriteString(globalCollision)
		sql.WriteString(" FROM ")
		sql.WriteString(labelExpanded)
		sql.WriteString(" ARRAY JOIN ")
		sql.WriteString(rowEntries)
		sql.WriteString(" AS ")
		sql.WriteString(rowEntry)
		sql.WriteString("), ")

		public := rowEligible + " != 0 AND " + kind + " IN (0, 1)"
		typedValidationRow := chartValidationRowSQL(rowDatabaseType)
		if rowKind == ChartRowKindMixed {
			typedValidationRow = "tuple(" + typedValidationRow + ", toUInt8(0))"
		}
		sql.WriteString(collapsed)
		sql.WriteString(" AS (SELECT if(")
		sql.WriteString(public)
		sql.WriteString(", ")
		sql.WriteString(row)
		sql.WriteString(", ")
		sql.WriteString(typedValidationRow)
		sql.WriteString(") AS ")
		sql.WriteString(groupRow)
		sql.WriteString(", ")
		sql.WriteString("if(")
		sql.WriteString(public)
		sql.WriteString(", ")
		sql.WriteString(encoded)
		sql.WriteString(", CAST('' AS String)) AS ")
		sql.WriteString(encoded)
		sql.WriteString(", ")
		sql.WriteString("toUInt64(sum(toUInt128(if(")
		sql.WriteString(public)
		sql.WriteString(", ")
		sql.WriteString(frequency)
		sql.WriteString(", 0)))) AS ")
		sql.WriteString(collapsedCount)
		sql.WriteString(", ")
		sql.WriteString("toUInt8(maxIf(")
		sql.WriteString(rowInvalid)
		sql.WriteString(", ")
		sql.WriteString(rowEligible)
		sql.WriteString(" != 0) > 0) AS ")
		sql.WriteString(rowInvalidEvidence)
		sql.WriteString(", ")
		sql.WriteString("toUInt8(sumIf(toUInt128(")
		sql.WriteString(frequency)
		sql.WriteString("), ")
		sql.WriteString(kind)
		sql.WriteString(" = 3) > 0) AS ")
		sql.WriteString(columnInvalidEvidence)
		sql.WriteString(", ")
		sql.WriteString("toUInt8(max(")
		sql.WriteString(globalCollision)
		sql.WriteString(") > 0) AS ")
		sql.WriteString(collisionEvidence)
		sql.WriteString(" FROM ")
		sql.WriteString(expanded)
		sql.WriteString(" GROUP BY ")
		sql.WriteString(groupRow)
		sql.WriteString(", ")
		sql.WriteString(encoded)
		sql.WriteString("), ")
	}

	if fieldOccurrenceCount {
		// Both sentinels probe the materialized label aggregate as ordinary
		// relations. A scalar subquery would be evaluated during analysis, before
		// the materialized temporary table exists, and would re-run the whole
		// scoped scan once per occurrence.
		domainFrequency := frequency
		if fieldOccurrenceCount {
			domainFrequency = rowCount
		}
		sql.WriteString(domainRows)
		sql.WriteString(" AS (SELECT toUInt8(0) AS sort_kind, ")
		sql.WriteString(splunkSeriesLabelSQL(label))
		sql.WriteString(" AS ")
		sql.WriteString(sortLabel)
		sql.WriteString(", concat('0:', ")
		sql.WriteString(label)
		sql.WriteString(") AS ")
		sql.WriteString(encoded)
		sql.WriteString(" FROM ")
		sql.WriteString(top)
		sql.WriteString(" UNION ALL SELECT toUInt8(1), CAST('' AS String), CAST('1:' AS String) FROM (SELECT 1 FROM ")
		sql.WriteString(labelTotals)
		sql.WriteString(" WHERE ")
		sql.WriteString(kind)
		sql.WriteString(" = 1 AND ")
		sql.WriteString(domainFrequency)
		sql.WriteString(" > 0 LIMIT 1)")
		sql.WriteString(" UNION ALL SELECT toUInt8(2), CAST('' AS String), CAST('2:' AS String) FROM (SELECT 1 FROM ")
		sql.WriteString(labelTotals)
		sql.WriteString(" WHERE ")
		sql.WriteString(kind)
		sql.WriteString(" = 0 AND ")
		sql.WriteString(domainFrequency)
		sql.WriteString(" > 0 AND ")
		sql.WriteString(label)
		sql.WriteString(" NOT IN (SELECT ")
		sql.WriteString(label)
		sql.WriteString(" FROM ")
		sql.WriteString(top)
		sql.WriteString(") LIMIT 1)), ")

		sql.WriteString(domain)
		sql.WriteString(" AS (SELECT arrayMap(item -> item.3, arraySort(item -> (item.1, item.2), groupArray((sort_kind, ")
		sql.WriteString(sortLabel)
		sql.WriteString(", ")
		sql.WriteString(encoded)
		sql.WriteString(")))) AS names FROM ")
		sql.WriteString(domainRows)
		sql.WriteString("), ")

		// Convergence after VALUE normalization is one member of the same label
		// rule as the empty, invalid-UTF-8, over-long, reserved, and row-name
		// labels, and every other member is evaluated on the column value's own
		// presence. The label aggregate carries a kind = 0 group for every ordinary
		// label any classified input row held, so reading it without the
		// row-eligible frequency filter keeps the rule presence-independent: two
		// labels that converge fail the whole command even when only row-ineligible
		// events carried them, exactly as a reserved label on such an event does.
		sql.WriteString(collisions)
		sql.WriteString(" AS (SELECT toUInt8(count() > 0) AS ")
		sql.WriteString(collision)
		sql.WriteString(" FROM (SELECT ")
		sql.WriteString(splunkSeriesLabelSQL(label))
		sql.WriteString(" AS ")
		sql.WriteString(normalized)
		sql.WriteString(" FROM ")
		sql.WriteString(labelTotals)
		sql.WriteString(" WHERE ")
		sql.WriteString(kind)
		sql.WriteString(" = 0 GROUP BY ")
		sql.WriteString(normalized)
		sql.WriteString(" HAVING uniqExact(")
		sql.WriteString(label)
		sql.WriteString(") > 1 LIMIT 1)), ")

		// The atomic column-value rejection is row-independent by construction:
		// the label aggregate carries a kind = 3 group whenever any classified
		// input row held an unsupported column value, whether or not that row also
		// carried an eligible row value.
		sql.WriteString(columnCheck)
		sql.WriteString(" AS (SELECT toUInt8(maxOrDefault(")
		sql.WriteString(kind)
		sql.WriteString(" = 3)) AS ")
		sql.WriteString(columnInvalid)
		sql.WriteString(" FROM ")
		sql.WriteString(labelTotals)
		sql.WriteString("), ")

		sql.WriteString(rowMaps)
		sql.WriteString(" AS (SELECT ")
		sql.WriteString(row)
		sql.WriteString(", mapFromArrays(groupArray(")
		sql.WriteString(encoded)
		sql.WriteString("), groupArray(")
		sql.WriteString(frequency)
		sql.WriteString(")) AS ")
		sql.WriteString(countMap)
		sql.WriteString(" FROM ")
		sql.WriteString(collapsed)
		sql.WriteString(" GROUP BY ")
		sql.WriteString(row)
		sql.WriteString("), ")

		sql.WriteString(validation)
		sql.WriteString(" AS (SELECT toUInt8(maxOrDefault(")
		sql.WriteString(rowInvalid)
		sql.WriteString(") > 0) AS ")
		sql.WriteString(invalid)
		sql.WriteString(" FROM ")
		sql.WriteString(counts)
		if fieldOccurrenceCount {
			// Missing and explicit-null dynamic row values are not chart rows and
			// therefore cannot invalidate the row domain. Unsupported descendants
			// remain eligible by construction and still fail atomically.
			sql.WriteString(" WHERE ")
			sql.WriteString(rowEligible)
			sql.WriteString(" != 0")
		}
		sql.WriteString("), ")

		// The row axis is data, so its ordinal is assigned server-side from the
		// declared order. Only the dense ordinal proves that order to the executor;
		// the row value itself crosses the boundary as an ordinary typed column.
		sql.WriteString(rowDomain)
		sql.WriteString(" AS MATERIALIZED (SELECT ")
		sql.WriteString(row)
		sql.WriteString(", toUInt64(row_number() OVER (ORDER BY ")
		sql.WriteString(rowSortSQL)
		sql.WriteString(" ASC) - 1) AS ")
		sql.WriteString(ordinal)
		sql.WriteString(" FROM (SELECT ")
		sql.WriteString(row)
		sql.WriteString(" FROM ")
		sql.WriteString(counts)
		if fieldOccurrenceCount {
			sql.WriteString(" WHERE ")
			sql.WriteString(rowEligible)
			sql.WriteString(" != 0")
		}
		sql.WriteString(" GROUP BY ")
		sql.WriteString(row)
		sql.WriteString(")) ")

		// A private sentinel carries row-independent validation across an empty row
		// axis. It is ordered first and rejected by the buffering executor, so the
		// synthetic row and empty arrays can never become public output.
		sql.WriteString("SELECT ")
		sql.WriteString(ordinal)
		sql.WriteString(", ")
		sql.WriteString(q(ChartRowColumn))
		sql.WriteString(", ")
		if rowKind == ChartRowKindMixed {
			sql.WriteString(q(ChartRowSemanticBytesColumn))
			sql.WriteString(", ")
		}
		sql.WriteString(q(ChartNamesColumn))
		sql.WriteString(", ")
		sql.WriteString(q(ChartCountsColumn))
		sql.WriteString(", ")
		sql.WriteString(q(ChartInvalidColumn))
		sql.WriteString(" FROM (")
		sql.WriteString("SELECT ")
		sql.WriteString(rowDomain)
		sql.WriteString(".")
		sql.WriteString(ordinal)
		sql.WriteString(" AS ")
		sql.WriteString(ordinal)
		sql.WriteString(", ")
		rowOutputSQL := rowDomain + "." + row
		if rowKind == ChartRowKindMixed {
			rowOutputSQL = "tupleElement(" + rowOutputSQL + ", 1)"
		}
		sql.WriteString(rowOutputSQL)
		sql.WriteString(" AS ")
		sql.WriteString(q(ChartRowColumn))
		sql.WriteString(", ")
		if rowKind == ChartRowKindMixed {
			sql.WriteString("tupleElement(")
			sql.WriteString(rowDomain)
			sql.WriteString(".")
			sql.WriteString(row)
			sql.WriteString(", 2) AS ")
			sql.WriteString(q(ChartRowSemanticBytesColumn))
			sql.WriteString(", ")
		}
		sql.WriteString(domain)
		sql.WriteString(".names AS ")
		sql.WriteString(q(ChartNamesColumn))
		sql.WriteString(", ")
		sql.WriteString("arrayMap(name -> ifNull(")
		sql.WriteString(rowMaps)
		sql.WriteString(".")
		sql.WriteString(countMap)
		sql.WriteString("[name], toUInt64(0)), ")
		sql.WriteString(domain)
		sql.WriteString(".names) AS ")
		sql.WriteString(q(ChartCountsColumn))
		sql.WriteString(", ")
		sql.WriteString("toUInt8(0) AS ")
		sql.WriteString(q(ChartInvalidColumn))
		sql.WriteString(" FROM ")
		sql.WriteString(rowDomain)
		sql.WriteString(" CROSS JOIN ")
		sql.WriteString(domain)
		sql.WriteString(" LEFT JOIN ")
		sql.WriteString(rowMaps)
		sql.WriteString(" ON ")
		sql.WriteString(rowMaps)
		sql.WriteString(".")
		sql.WriteString(row)
		sql.WriteString(" = ")
		sql.WriteString(rowDomain)
		sql.WriteString(".")
		sql.WriteString(row)
		// Deterministic, non-truncating overflow: the guard runs during filtering,
		// before the ordered result is produced, so no partial pivot is published.
		sql.WriteString(" WHERE throwIf(")
		sql.WriteString(rowDomain)
		sql.WriteString(".")
		sql.WriteString(ordinal)
		sql.WriteString(" >= ")
		sql.WriteString(strconv.FormatUint(uint64(operator.RowLimit), 10))
		sql.WriteString(", '")
		sql.WriteString(ChartRowLimitMarker)
		sql.WriteString("') = 0")
		sql.WriteString(" UNION ALL SELECT toUInt64(0) AS ")
		sql.WriteString(ordinal)
		sql.WriteString(", ")
		sql.WriteString(chartValidationRowSQL(rowDatabaseType))
		sql.WriteString(" AS ")
		sql.WriteString(q(ChartRowColumn))
		if rowKind == ChartRowKindMixed {
			sql.WriteString(", toUInt8(0) AS ")
			sql.WriteString(q(ChartRowSemanticBytesColumn))
		}
		sql.WriteString(", CAST([], 'Array(String)') AS ")
		sql.WriteString(q(ChartNamesColumn))
		sql.WriteString(", CAST([], 'Array(UInt64)') AS ")
		sql.WriteString(q(ChartCountsColumn))
		sql.WriteString(", toUInt8(1) AS ")
		sql.WriteString(q(ChartInvalidColumn))
		sql.WriteString(" FROM ")
		sql.WriteString(validation)
		sql.WriteString(" CROSS JOIN ")
		sql.WriteString(collisions)
		sql.WriteString(" CROSS JOIN ")
		sql.WriteString(columnCheck)
		sql.WriteString(" WHERE ")
		sql.WriteString(validation)
		sql.WriteString(".")
		sql.WriteString(invalid)
		sql.WriteString(" != 0 OR ")
		sql.WriteString(collisions)
		sql.WriteString(".")
		sql.WriteString(collision)
		sql.WriteString(" != 0 OR ")
		sql.WriteString(columnCheck)
		sql.WriteString(".")
		sql.WriteString(columnInvalid)
		sql.WriteString(" != 0) AS ")
		sql.WriteString(q("__os_chart_transport"))
		sql.WriteString(" ORDER BY ")
		sql.WriteString(q(ChartInvalidColumn))
		sql.WriteString(" DESC, ")
		sql.WriteString(ordinal)
		sql.WriteString(" ASC")
	} else {
		rawEncodedLabel := "substring(" + encoded + ", 3)"
		published := encoded + " != '' AND " + collapsedCount + " > 0"
		domainItem := "tuple(multiIf(" + encoded + " = '1:', toUInt8(1), " + encoded + " = '2:', toUInt8(2), toUInt8(0)), " +
			"if(startsWith(" + encoded + ", '0:'), " + splunkSeriesLabelSQL(rawEncodedLabel) + ", CAST('' AS String)), " + encoded + ")"

		// Consume the bounded collapsed relation exactly once. Per-row maps are
		// grouped first; only then do fixed-cardinality windows attach the global
		// domain and validation evidence to at most one row per public row key.
		//
		// The column domain must deduplicate inside the window's aggregate state
		// rather than in an expression over its result. Collapsing already maps
		// every label outside the published top to '2:', so the distinct domain
		// holds at most the series limit plus the NULL and OTHER sentinels, which
		// is the transport's whole MaxSeries allowance, no matter how many labels
		// the input carried; but a window aggregate delivers its result to every row,
		// so gathering one entry per collapsed group and deduplicating afterwards
		// would materialize a row-count-sized array once per row. That is
		// quadratic in the row axis and exhausted the executor's memory budget
		// well below the advertised row ceiling. groupUniqArrayArray unions the
		// per-group arrays into that bounded distinct set as it aggregates, so
		// each row receives only the small domain.
		sql.WriteString(rowMaps)
		sql.WriteString(" AS (SELECT ")
		sql.WriteString(groupRow)
		sql.WriteString(", mapFromArrays(groupArrayIf(")
		sql.WriteString(encoded)
		sql.WriteString(", ")
		sql.WriteString(published)
		sql.WriteString("), groupArrayIf(")
		sql.WriteString(collapsedCount)
		sql.WriteString(", ")
		sql.WriteString(published)
		sql.WriteString(")) AS ")
		sql.WriteString(countMap)
		sql.WriteString(", ")
		sql.WriteString("arrayMap(item -> item.3, arraySort(item -> (item.1, item.2), groupUniqArrayArray(groupArrayIf(")
		sql.WriteString(domainItem)
		sql.WriteString(", ")
		sql.WriteString(published)
		sql.WriteString(")) OVER ())) AS ")
		sql.WriteString(domainNames)
		sql.WriteString(", ")
		sql.WriteString("toUInt8(max(max(")
		sql.WriteString(rowInvalidEvidence)
		sql.WriteString(")) OVER () != 0) AS ")
		sql.WriteString(invalid)
		sql.WriteString(", ")
		sql.WriteString("toUInt8(max(max(")
		sql.WriteString(collisionEvidence)
		sql.WriteString(")) OVER () != 0) AS ")
		sql.WriteString(collision)
		sql.WriteString(", ")
		sql.WriteString("toUInt8(max(max(")
		sql.WriteString(columnInvalidEvidence)
		sql.WriteString(")) OVER () != 0) AS ")
		sql.WriteString(columnInvalid)
		sql.WriteString(" FROM ")
		sql.WriteString(collapsed)
		sql.WriteString(" GROUP BY ")
		sql.WriteString(groupRow)
		sql.WriteString("), ")

		bareRowSortSQL := groupRow
		if rowDynamic || rowField.numericSort {
			bareRowSortSQL = dynamicSortValue(groupRow, false)
		}
		sql.WriteString(rowDomain)
		sql.WriteString(" AS (SELECT *, toUInt8(")
		sql.WriteString(invalid)
		sql.WriteString(" != 0 OR ")
		sql.WriteString(collision)
		sql.WriteString(" != 0 OR ")
		sql.WriteString(columnInvalid)
		sql.WriteString(" != 0) AS ")
		sql.WriteString(transportInvalid)
		sql.WriteString(", ")
		sql.WriteString("toUInt64(row_number() OVER (ORDER BY ")
		sql.WriteString(bareRowSortSQL)
		sql.WriteString(" ASC) - 1) AS ")
		sql.WriteString(ordinal)
		sql.WriteString(" FROM ")
		sql.WriteString(rowMaps)
		sql.WriteString(" WHERE length(mapKeys(")
		sql.WriteString(countMap)
		sql.WriteString(")) > 0 OR ")
		sql.WriteString(invalid)
		sql.WriteString(" != 0 OR ")
		sql.WriteString(collision)
		sql.WriteString(" != 0 OR ")
		sql.WriteString(columnInvalid)
		sql.WriteString(" != 0) ")

		groupRowOutputSQL := groupRow
		if rowKind == ChartRowKindMixed {
			groupRowOutputSQL = "tupleElement(" + groupRow + ", 1)"
		}
		sql.WriteString("SELECT ")
		sql.WriteString(ordinal)
		sql.WriteString(", ")
		sql.WriteString(groupRowOutputSQL)
		sql.WriteString(" AS ")
		sql.WriteString(q(ChartRowColumn))
		sql.WriteString(", ")
		if rowKind == ChartRowKindMixed {
			sql.WriteString("tupleElement(")
			sql.WriteString(groupRow)
			sql.WriteString(", 2) AS ")
			sql.WriteString(q(ChartRowSemanticBytesColumn))
			sql.WriteString(", ")
		}
		sql.WriteString("if(")
		sql.WriteString(transportInvalid)
		sql.WriteString(" != 0, CAST([], 'Array(String)'), ")
		sql.WriteString(domainNames)
		sql.WriteString(") AS ")
		sql.WriteString(q(ChartNamesColumn))
		sql.WriteString(", ")
		sql.WriteString("if(")
		sql.WriteString(transportInvalid)
		sql.WriteString(" != 0, CAST([], 'Array(UInt64)'), arrayMap(name -> ifNull(")
		sql.WriteString(countMap)
		sql.WriteString("[name], toUInt64(0)), ")
		sql.WriteString(domainNames)
		sql.WriteString(")) AS ")
		sql.WriteString(q(ChartCountsColumn))
		sql.WriteString(", ")
		sql.WriteString(transportInvalid)
		sql.WriteString(" AS ")
		sql.WriteString(q(ChartInvalidColumn))
		sql.WriteString(" FROM ")
		sql.WriteString(rowDomain)
		sql.WriteString(" WHERE throwIf(if(")
		sql.WriteString(transportInvalid)
		sql.WriteString(" = 0, ")
		sql.WriteString(ordinal)
		sql.WriteString(" >= ")
		sql.WriteString(strconv.FormatUint(uint64(operator.RowLimit), 10))
		sql.WriteString(", 0), '")
		sql.WriteString(ChartRowLimitMarker)
		sql.WriteString("') = 0")
		sql.WriteString(" AND (")
		sql.WriteString(transportInvalid)
		sql.WriteString(" = 0 OR ")
		sql.WriteString(ordinal)
		sql.WriteString(" = 0) ORDER BY ")
		sql.WriteString(q(ChartInvalidColumn))
		sql.WriteString(" DESC, ")
		sql.WriteString(ordinal)
		sql.WriteString(" ASC")
	}
	sql.WriteString(materializedCTESettingsSQL)

	sourceDepth := relationalNodeDepth(relation.depth)
	preparedDepth := relationalNodeDepth(sourceDepth)
	kindedDepth := relationalNodeDepth(preparedDepth)
	classifiedDepth := relationalNodeDepth(kindedDepth)
	canonicalizedDepth := relationalNodeDepth(classifiedDepth)
	var resultDepth int
	if fieldOccurrenceCount {
		countsDepth := relationalNodeDepth(canonicalizedDepth)
		labelTotalsDepth := relationalNodeDepth(countsDepth)
		topDepth := relationalNodeDepth(labelTotalsDepth)
		topMembershipDepth := relationalNodeDepth(topDepth)
		collapsedDepth := relationalNodeDepth(countsDepth, topMembershipDepth)

		domainTopBranchDepth := relationalNodeDepth(topDepth)
		domainNullInputDepth := relationalNodeDepth(labelTotalsDepth)
		domainNullBranchDepth := relationalNodeDepth(domainNullInputDepth)
		domainOtherInputDepth := relationalNodeDepth(labelTotalsDepth, topMembershipDepth)
		domainOtherBranchDepth := relationalNodeDepth(domainOtherInputDepth)
		domainRowsDepth := relationalNodeDepth(
			domainTopBranchDepth,
			domainNullBranchDepth,
			domainOtherBranchDepth,
		)
		domainDepth := relationalNodeDepth(domainRowsDepth)
		collisionInputDepth := relationalNodeDepth(labelTotalsDepth)
		collisionsDepth := relationalNodeDepth(collisionInputDepth)
		columnCheckDepth := relationalNodeDepth(labelTotalsDepth)
		rowMapsDepth := relationalNodeDepth(collapsedDepth)
		validationDepth := relationalNodeDepth(countsDepth)
		rowDomainInputDepth := relationalNodeDepth(countsDepth)
		rowDomainDepth := relationalNodeDepth(rowDomainInputDepth)
		regularResultDepth := relationalNodeDepth(
			rowDomainDepth,
			domainDepth,
			rowMapsDepth,
		)
		validationSentinelDepth := relationalNodeDepth(
			validationDepth,
			collisionsDepth,
			columnCheckDepth,
		)
		unionDepth := relationalNodeDepth(regularResultDepth, validationSentinelDepth)
		resultDepth = relationalNodeDepth(unionDepth)
	} else {
		countsDepth := relationalNodeDepth(canonicalizedDepth)
		labelGroupsDepth := relationalNodeDepth(countsDepth)
		normalizedGroupsDepth := relationalNodeDepth(labelGroupsDepth)
		authorityDepth := relationalNodeDepth(normalizedGroupsDepth)
		labelExpandedDepth := relationalNodeDepth(authorityDepth)
		expandedDepth := relationalNodeDepth(labelExpandedDepth)
		collapsedDepth := relationalNodeDepth(expandedDepth)
		rowMapsDepth := relationalNodeDepth(collapsedDepth)
		rowDomainDepth := relationalNodeDepth(rowMapsDepth)
		resultDepth = relationalNodeDepth(rowDomainDepth)
	}

	compiled := CompiledQuery{
		SQL:          sql.String(),
		Args:         args,
		OutputFields: slices.Clone(dynamic.FixedFields),
		sourceFanout: eventStatsOrdinarySourceFanout,
		Chart: &ChartOutput{
			RowField:         rowName,
			RowKind:          rowKind,
			RowDatabaseType:  rowDatabaseType,
			RowLimit:         uint64(operator.RowLimit),
			MaxSeries:        dynamic.MaxSeries,
			MaxLabelBytes:    maxTimechartLabelBytes,
			ValueKind:        ChartValueKindCount,
			RowSemanticBytes: rowKind == ChartRowKindMixed,
		},
	}
	return withCompiledRelationalDepth(compiled, resultDepth, operator.Range), nil
}

func compileNumericChart(
	relation compiledRelation,
	state compileState,
	args []any,
	operator *plan.Chart,
	dynamic *plan.DynamicSeriesOutput,
	alias string,
) (CompiledQuery, error) {
	if operator == nil || operator.Measure.Predicate != nil {
		return CompiledQuery{}, errors.New("compile ClickHouse chart: numeric measure contract is invalid")
	}
	var valueKind ChartValueKind
	canonicalOutput := ""
	switch operator.Measure.Function {
	case plan.AggregateFunctionPercentile:
		if operator.Measure.Percentile < 1 || operator.Measure.Percentile > 99 {
			return CompiledQuery{}, errors.New(
				"compile ClickHouse chart: percentile must be from 1 through 99",
			)
		}
		valueKind = ChartValueKindPercentile
		canonicalOutput = "perc" +
			strconv.Itoa(int(operator.Measure.Percentile)) + "(" +
			operator.Measure.Input.Name + ")"
	case plan.AggregateFunctionSum:
		if operator.Measure.Percentile != 0 {
			return CompiledQuery{}, errors.New(
				"compile ClickHouse chart: numeric aggregate contains percentile metadata",
			)
		}
		valueKind = ChartValueKindSum
		canonicalOutput = "sum(" + operator.Measure.Input.Name + ")"
	case plan.AggregateFunctionAverage:
		if operator.Measure.Percentile != 0 {
			return CompiledQuery{}, errors.New(
				"compile ClickHouse chart: numeric aggregate contains percentile metadata",
			)
		}
		valueKind = ChartValueKindAverage
		canonicalOutput = "avg(" + operator.Measure.Input.Name + ")"
	default:
		return CompiledQuery{}, errors.New("compile ClickHouse chart: numeric function is unsupported")
	}
	if err := validateCanonicalFieldRef("chart", "input", operator.Measure.Input); err != nil {
		return CompiledQuery{}, err
	}
	if !spl.IsExactUnquotedFieldName(operator.Measure.Input.Name) ||
		operator.Measure.Input.Name == operator.Over.Name ||
		operator.Measure.Output != canonicalOutput {
		return CompiledQuery{}, errors.New("compile ClickHouse chart: numeric measure contract is invalid")
	}
	if state.eventRows && state.allowDynamic && operator.Measure.Input.Name == "fields" {
		return CompiledQuery{}, &plan.Diagnostic{
			Code:    "SPL_AMBIGUOUS_CHART_FIELD",
			Message: "chart cannot read the event result's reserved fields payload without an exact upstream schema",
			Range:   operator.Measure.Input.Range,
		}
	}

	rowName := operator.Over.Name
	rowField, rowDatabaseType, rowKind, splitField, err := resolveChartAxes(operator, dynamic, state)
	if err != nil {
		return CompiledQuery{}, err
	}

	measureField, measureResolved, err := resolveCompiledField(
		operator.Measure.Input,
		state,
	)
	if err != nil {
		return CompiledQuery{}, err
	}
	measureInputSQL := "CAST([], 'Array(Float64)')"
	var measureArgs []any
	if measureResolved {
		measureInputSQL, measureArgs = numericArrayInputSQL(measureField)
	}

	rowExistsSQL := rowField.existsSQL
	if rowExistsSQL == "" {
		rowExistsSQL = "1"
	}
	splitExistsSQL := splitField.existsSQL
	if splitExistsSQL == "" {
		splitExistsSQL = "1"
	}
	rowDynamic := rowField.kind == fieldKindDynamic
	splitDynamic := splitField.kind == fieldKindDynamic
	rowHasDescendant := rowDynamic && rowField.descendantSQL != ""
	splitHasDescendant := splitDynamic && splitField.descendantSQL != ""

	q := quoteIdentifier
	source := q("__os_chart_source")
	prepared := q("__os_chart_prepared")
	kinded := q("__os_chart_kinded")
	classified := q("__os_chart_classified")
	canonicalized := q("__os_chart_canonicalized")
	labelTotals := q("__os_chart_label_totals")
	numericGroups := q("__os_chart_numeric_groups")
	numericScores := q("__os_chart_numeric_scores")
	collapsed := q("__os_chart_collapsed")
	finalized := q("__os_chart_finalized")
	domainRows := q("__os_chart_domain_rows")
	domain := q("__os_chart_domain")
	collisions := q("__os_chart_normalization_collisions")
	columnCheck := q("__os_chart_column_check")
	rowMaps := q("__os_chart_row_maps")
	validation := q("__os_chart_validation")
	rowDomain := q("__os_chart_row_domain")

	rowValue := q("__os_ch_row_value")
	rowSemanticBytes := q("__os_ch_row_semantic_bytes")
	rowExact := q("__os_ch_row_exact")
	rowType := q("__os_ch_row_type")
	rowPresent := q("__os_ch_row_present")
	rowEligible := q("__os_ch_row_eligible")
	rowSupported := q("__os_ch_row_supported")
	rowInvalid := q("__os_ch_row_invalid")
	row := q("__os_ch_row")
	value := q("__os_ch_value")
	present := q("__os_ch_present")
	descendant := q("__os_ch_descendant")
	valueType := q("__os_ch_value_type")
	label := q("__os_ch_label")
	kind := q("__os_ch_kind")
	measureValues := q("__os_ch_measure_values")
	numerator := q("__os_ch_numerator")
	denominator := q("__os_ch_denominator")
	numericState := q("__os_ch_numeric_state")
	percentileState := q("__os_ch_percentile_state")
	percentileValues := q("__os_ch_percentile_values")
	frequency := q("__os_ch_count")
	score := q("__os_ch_score")
	encoded := q("__os_ch_encoded")
	measureValue := q("__os_ch_measure_value")
	normalized := q("__os_ch_normalized")
	sortLabel := q("__os_ch_sort_label")
	valueMap := q("__os_ch_value_map")
	presentMap := q("__os_ch_present_map")
	invalid := q("__os_ch_invalid")
	collision := q("__os_ch_collision")
	columnInvalid := q("__os_ch_column_invalid")
	ordinal := q(ChartOrdinalColumn)

	prefixArgs := make([]any, 0,
		len(rowField.existsArgs)+len(splitField.existsArgs)+len(measureArgs))
	prefixArgs = append(prefixArgs, rowField.existsArgs...)
	prefixArgs = append(prefixArgs, splitField.existsArgs...)
	prefixArgs = append(prefixArgs, measureArgs...)
	args = prependArguments(prefixArgs, args)
	if rowHasDescendant {
		args = append(args, rowField.descendantArgs...)
	}
	if splitHasDescendant {
		args = append(args, splitField.descendantArgs...)
	}
	args = append(args, rowName)

	splitTypeSQL := "if(isNull(" + splitField.valueSQL + "), 'None', 'String')"
	if splitDynamic {
		splitTypeSQL = dynamicTypeExpression(splitField)
	}
	validLabel := "isValidUTF8(" + label + ") AND length(" + label +
		") BETWEEN 1 AND " + strconv.Itoa(maxTimechartLabelBytes) + " AND " +
		label + " NOT IN ('NULL', 'OTHER') AND " +
		splunkSeriesLabelSQL(label) + " != ?"

	rowPresenceSQL := "(" + rowExact + " != 0 AND isNotNull(" + rowValue + "))"
	if rowHasDescendant {
		rowPresenceSQL = "(" + rowPresenceSQL + " OR " +
			rowField.descendantSQL + ")"
	}
	rowKeySQL := "CAST(assumeNotNull(" + rowValue + ") AS " +
		rowDatabaseType + ")"
	rowSupportedSQL := "1"
	if rowDynamic {
		runtime := fieldState{
			valueSQL:       rowValue,
			dynamicTypeSQL: rowType,
			kind:           fieldKindDynamic,
		}
		supported, lexical := statsByScalarExpressions(runtime)
		rowSupportedSQL = supported
		rowKeySQL = "CAST(if(" + rowSupported + " != 0, " + lexical +
			", '') AS String)"
	}
	if rowKind == ChartRowKindMixed {
		rowKeySQL = "tuple(" + rowKeySQL + ", " + rowSemanticBytes + ")"
	}
	rowSortSQL := row
	if rowDynamic || rowField.numericSort {
		rowSortSQL = dynamicSortValue(row, false)
	}

	var scoreSQL string
	var publishSQL string
	switch valueKind {
	case ChartValueKindPercentile:
		scoreSQL = "sum(ifNull(arrayElementOrNull(finalizeAggregation(" +
			percentileState + "), 1), toFloat64(0)))"
		publishSQL = "arrayElementOrNull(" + percentileValues + ", 1)"
	case ChartValueKindSum:
		scoreSQL = "sum(if(" + denominator +
			" = 0, toFloat64(0), " + numerator + "))"
		publishSQL = "if(" + denominator +
			" = 0, CAST(NULL AS Nullable(Float64)), " + numerator + ")"
	case ChartValueKindAverage:
		cellAverage := numerator + " / toFloat64(" + denominator + ")"
		scoreSQL = "sum(if(" + denominator +
			" = 0, toFloat64(0), " + cellAverage + "))"
		publishSQL = "if(" + denominator +
			" = 0, CAST(NULL AS Nullable(Float64)), " + cellAverage + ")"
	default:
		return CompiledQuery{}, errors.New(
			"compile ClickHouse chart: numeric value kind is invalid",
		)
	}

	var sql strings.Builder
	sql.Grow(len(relation.sql) + len(measureInputSQL) + 12_288)
	sql.WriteString("WITH ")
	sql.WriteString(source)
	sql.WriteString(" AS (SELECT ")
	sql.WriteString(rowField.valueSQL)
	sql.WriteString(" AS ")
	sql.WriteString(rowValue)
	sql.WriteString(", ")
	sql.WriteString("toUInt8(")
	sql.WriteString(rowExistsSQL)
	sql.WriteString(") AS ")
	sql.WriteString(rowExact)
	sql.WriteString(", ")
	if rowKind == ChartRowKindMixed {
		sql.WriteString("toUInt8(ifNull(")
		sql.WriteString(rowField.semanticBytesSQL)
		sql.WriteString(", 0)) AS ")
		sql.WriteString(rowSemanticBytes)
		sql.WriteString(", ")
	}
	if rowDynamic {
		sql.WriteString(dynamicTypeExpression(rowField))
		sql.WriteString(" AS ")
		sql.WriteString(rowType)
		sql.WriteString(", ")
	}
	sql.WriteString(splitField.valueSQL)
	sql.WriteString(" AS ")
	sql.WriteString(value)
	sql.WriteString(", ")
	sql.WriteString("toUInt8(")
	sql.WriteString(splitExistsSQL)
	sql.WriteString(") AS ")
	sql.WriteString(present)
	sql.WriteString(", ")
	sql.WriteString(splitTypeSQL)
	sql.WriteString(" AS ")
	sql.WriteString(valueType)
	sql.WriteString(", ")
	sql.WriteString(measureInputSQL)
	sql.WriteString(" AS ")
	sql.WriteString(measureValues)
	for _, column := range pivotDescendantSourceColumns(state, rowField, splitField) {
		sql.WriteString(", ")
		sql.WriteString(column)
	}
	sql.WriteString(" FROM (")
	sql.WriteString(relation.sql)
	sql.WriteString(") AS ")
	sql.WriteString(alias)
	sql.WriteString("), ")

	sql.WriteString(prepared)
	sql.WriteString(" AS (SELECT *, ")
	sql.WriteString("toUInt8(")
	sql.WriteString(rowPresenceSQL)
	sql.WriteString(") AS ")
	sql.WriteString(rowPresent)
	sql.WriteString(", ")
	if splitHasDescendant {
		sql.WriteString("toUInt8(if(")
		sql.WriteString(present)
		sql.WriteString(" != 0, 0, ")
		sql.WriteString(splitField.descendantSQL)
		sql.WriteString(")) AS ")
		sql.WriteString(descendant)
		sql.WriteString(", ")
	} else {
		sql.WriteString("toUInt8(0) AS ")
		sql.WriteString(descendant)
		sql.WriteString(", ")
	}
	sql.WriteString("toUInt8(")
	sql.WriteString(rowSupportedSQL)
	sql.WriteString(") AS ")
	sql.WriteString(rowSupported)
	sql.WriteString(", ")
	sql.WriteString("if(")
	sql.WriteString(present)
	sql.WriteString(" != 0 AND isNotNull(")
	sql.WriteString(value)
	sql.WriteString(") AND ")
	sql.WriteString(valueType)
	sql.WriteString(" = 'String', assumeNotNull(toString(")
	sql.WriteString(value)
	sql.WriteString(")), CAST('' AS String)) AS ")
	sql.WriteString(label)
	sql.WriteString(" FROM ")
	sql.WriteString(source)
	sql.WriteString("), ")

	sql.WriteString(kinded)
	sql.WriteString(" AS (SELECT *, multiIf(")
	sql.WriteString(descendant)
	sql.WriteString(" != 0, toUInt8(3), ")
	sql.WriteString(present)
	sql.WriteString(" = 0 OR isNull(")
	sql.WriteString(value)
	sql.WriteString(") OR ")
	sql.WriteString(valueType)
	sql.WriteString(" = 'None', toUInt8(1), ")
	sql.WriteString(valueType)
	sql.WriteString(" != 'String', toUInt8(3), NOT (")
	sql.WriteString(validLabel)
	sql.WriteString("), toUInt8(3), toUInt8(0)) AS ")
	sql.WriteString(kind)
	sql.WriteString(" FROM ")
	sql.WriteString(prepared)
	sql.WriteString("), ")

	sql.WriteString(classified)
	sql.WriteString(" AS (SELECT ")
	sql.WriteString(rowKeySQL)
	sql.WriteString(" AS ")
	sql.WriteString(row)
	sql.WriteString(", toUInt8(")
	sql.WriteString(rowSupported)
	sql.WriteString(" = 0) AS ")
	sql.WriteString(rowInvalid)
	sql.WriteString(", ")
	sql.WriteString(rowPresent)
	sql.WriteString(" AS ")
	sql.WriteString(rowEligible)
	sql.WriteString(", ")
	sql.WriteString(kind)
	sql.WriteString(", ")
	sql.WriteString(label)
	sql.WriteString(", ")
	sql.WriteString(measureValues)
	sql.WriteString(" FROM ")
	sql.WriteString(kinded)
	sql.WriteString("), ")

	sql.WriteString(canonicalized)
	sql.WriteString(" AS (SELECT ")
	sql.WriteString(row)
	sql.WriteString(", ")
	sql.WriteString(rowInvalid)
	sql.WriteString(", ")
	sql.WriteString(rowEligible)
	sql.WriteString(", ")
	sql.WriteString(kind)
	sql.WriteString(", if(")
	sql.WriteString(kind)
	sql.WriteString(" = 0, ")
	sql.WriteString(label)
	sql.WriteString(", CAST('' AS String)) AS ")
	sql.WriteString(label)
	sql.WriteString(", if(")
	sql.WriteString(kind)
	sql.WriteString(" IN (0, 1) AND ")
	sql.WriteString(rowEligible)
	sql.WriteString(" != 0, ")
	sql.WriteString(measureValues)
	sql.WriteString(", CAST([], 'Array(Float64)')) AS ")
	sql.WriteString(measureValues)
	sql.WriteString(" FROM ")
	sql.WriteString(classified)
	sql.WriteString("), ")

	if valueKind == ChartValueKindPercentile {
		level := statsPercentileLevelSQL(operator.Measure.Percentile)
		sql.WriteString(numericGroups)
		sql.WriteString(" AS MATERIALIZED (SELECT ")
		sql.WriteString(row)
		sql.WriteString(", ")
		sql.WriteString(rowEligible)
		sql.WriteString(", ")
		sql.WriteString(kind)
		sql.WriteString(", ")
		sql.WriteString(label)
		sql.WriteString(", max(")
		sql.WriteString(rowInvalid)
		sql.WriteString(") AS ")
		sql.WriteString(rowInvalid)
		sql.WriteString(", quantilesGKOrNullArrayState(100, ")
		sql.WriteString(level)
		sql.WriteString(")(")
		sql.WriteString(measureValues)
		sql.WriteString(") AS ")
		sql.WriteString(percentileState)
		sql.WriteString(", count() AS ")
		sql.WriteString(frequency)
		sql.WriteString(" FROM ")
		sql.WriteString(canonicalized)
		sql.WriteString(" GROUP BY ")
		sql.WriteString(row)
		sql.WriteString(", ")
		sql.WriteString(rowEligible)
		sql.WriteString(", ")
		sql.WriteString(kind)
		sql.WriteString(", ")
		sql.WriteString(label)
		sql.WriteString("), ")
	} else {
		sql.WriteString(numericGroups)
		sql.WriteString(" AS MATERIALIZED (SELECT ")
		sql.WriteString(row)
		sql.WriteString(", ")
		sql.WriteString(rowEligible)
		sql.WriteString(", ")
		sql.WriteString(kind)
		sql.WriteString(", ")
		sql.WriteString(label)
		sql.WriteString(", ")
		sql.WriteString(rowInvalid)
		sql.WriteString(", tupleElement(")
		sql.WriteString(numericState)
		sql.WriteString(", 1) AS ")
		sql.WriteString(numerator)
		sql.WriteString(", toUInt64(tupleElement(")
		sql.WriteString(numericState)
		sql.WriteString(", 2)) AS ")
		sql.WriteString(denominator)
		sql.WriteString(", ")
		sql.WriteString(frequency)
		sql.WriteString(" FROM (SELECT ")
		sql.WriteString(row)
		sql.WriteString(", ")
		sql.WriteString(rowEligible)
		sql.WriteString(", ")
		sql.WriteString(kind)
		sql.WriteString(", ")
		sql.WriteString(label)
		sql.WriteString(", max(")
		sql.WriteString(rowInvalid)
		sql.WriteString(") AS ")
		sql.WriteString(rowInvalid)
		sql.WriteString(", sumCountArray(")
		sql.WriteString(measureValues)
		sql.WriteString(") AS ")
		sql.WriteString(numericState)
		sql.WriteString(", count() AS ")
		sql.WriteString(frequency)
		sql.WriteString(" FROM ")
		sql.WriteString(canonicalized)
		sql.WriteString(" GROUP BY ")
		sql.WriteString(row)
		sql.WriteString(", ")
		sql.WriteString(rowEligible)
		sql.WriteString(", ")
		sql.WriteString(kind)
		sql.WriteString(", ")
		sql.WriteString(label)
		sql.WriteString(") AS ")
		sql.WriteString(q("__os_chart_numeric_state_source"))
		sql.WriteString("), ")
	}

	// Label selection and row-independent validation derive from the same
	// materialized row/label aggregate. Unlike count chart, numeric chart has
	// exactly one consumer of the scoped event relation; no raw-event CTE is
	// expanded independently for label totals.
	sql.WriteString(labelTotals)
	sql.WriteString(" AS MATERIALIZED (SELECT ")
	sql.WriteString(kind)
	sql.WriteString(", ")
	sql.WriteString(label)
	sql.WriteString(", sumIf(")
	sql.WriteString(frequency)
	sql.WriteString(", ")
	sql.WriteString(rowEligible)
	sql.WriteString(" != 0) AS ")
	sql.WriteString(frequency)
	sql.WriteString(" FROM ")
	sql.WriteString(numericGroups)
	sql.WriteString(" GROUP BY ")
	sql.WriteString(kind)
	sql.WriteString(", ")
	sql.WriteString(label)
	sql.WriteString("), ")

	// The label domain is read from two IN sets below. ClickHouse 26.7 drops
	// the DelayedPortsProcessor gate that holds a materialized CTE's readers
	// behind its writer whenever an IN set reads a materialized CTE that is
	// itself defined over another materialized CTE, and aborts the query with
	// LOGICAL_ERROR "Reading from materialized CTE ... before its
	// materialization completed" (the fail-fast check from ClickHouse PR
	// 108924; the surviving gate losses are tracked in ClickHouse issues
	// 113184, 113489, and 114810).
	//
	// This selection therefore stays an ordinary CTE. That is behavior
	// preserving because every reference selects the identical labels: the
	// GROUP BY makes each label appear once, and the ORDER BY below sorts on
	// the finite/infinite class, then the score, then the label itself, so the
	// final key is unique and the ordering is total. No two distinct rows can
	// compare equal, which leaves the LIMIT no freedom to choose between them.
	// Re-reading also never re-runs the scoped scan, because the group
	// aggregate this reads is itself materialized.
	//
	// Restore MATERIALIZED once the upstream gate is fixed on the pinned
	// release.
	sql.WriteString(numericScores)
	sql.WriteString(" AS (SELECT ")
	sql.WriteString(label)
	sql.WriteString(", ")
	sql.WriteString(scoreSQL)
	sql.WriteString(" AS ")
	sql.WriteString(score)
	sql.WriteString(" FROM ")
	sql.WriteString(numericGroups)
	sql.WriteString(" WHERE ")
	sql.WriteString(rowEligible)
	sql.WriteString(" != 0 AND ")
	sql.WriteString(kind)
	sql.WriteString(" = 0 GROUP BY ")
	sql.WriteString(label)
	sql.WriteString(" ORDER BY ")
	sql.WriteString("multiIf(isNaN(")
	sql.WriteString(score)
	sql.WriteString("), toUInt8(0), isInfinite(")
	sql.WriteString(score)
	sql.WriteString(") AND ")
	sql.WriteString(score)
	sql.WriteString(" < 0, toUInt8(1), isInfinite(")
	sql.WriteString(score)
	sql.WriteString("), toUInt8(3), toUInt8(2)) DESC, if(isFinite(")
	sql.WriteString(score)
	sql.WriteString("), ")
	sql.WriteString(score)
	sql.WriteString(", toFloat64(0)) DESC, ")
	sql.WriteString(label)
	sql.WriteString(" ASC LIMIT ")
	sql.WriteString(strconv.FormatUint(uint64(operator.SeriesLimit), 10))
	sql.WriteString("), ")

	sql.WriteString(collapsed)
	sql.WriteString(" AS (SELECT ")
	sql.WriteString(row)
	sql.WriteString(", multiIf(")
	sql.WriteString(kind)
	sql.WriteString(" = 1, '1:', ")
	sql.WriteString(label)
	sql.WriteString(" IN (SELECT ")
	sql.WriteString(label)
	sql.WriteString(" FROM ")
	sql.WriteString(numericScores)
	sql.WriteString("), concat('0:', ")
	sql.WriteString(label)
	sql.WriteString("), '2:') AS ")
	sql.WriteString(encoded)
	sql.WriteString(", ")
	if valueKind == ChartValueKindPercentile {
		level := statsPercentileLevelSQL(operator.Measure.Percentile)
		sql.WriteString("quantilesGKOrNullArrayMerge(100, ")
		sql.WriteString(level)
		sql.WriteString(")(")
		sql.WriteString(percentileState)
		sql.WriteString(") AS ")
		sql.WriteString(percentileValues)
	} else {
		sql.WriteString("sum(")
		sql.WriteString(numerator)
		sql.WriteString(") AS ")
		sql.WriteString(numerator)
		sql.WriteString(", sum(")
		sql.WriteString(denominator)
		sql.WriteString(") AS ")
		sql.WriteString(denominator)
	}
	sql.WriteString(" FROM ")
	sql.WriteString(numericGroups)
	sql.WriteString(" WHERE ")
	sql.WriteString(rowEligible)
	sql.WriteString(" != 0 AND ")
	sql.WriteString(kind)
	sql.WriteString(" IN (0, 1) GROUP BY ")
	sql.WriteString(row)
	sql.WriteString(", ")
	sql.WriteString(encoded)
	sql.WriteString("), ")

	sql.WriteString(finalized)
	sql.WriteString(" AS (SELECT ")
	sql.WriteString(row)
	sql.WriteString(", ")
	sql.WriteString(encoded)
	sql.WriteString(", ")
	sql.WriteString(publishSQL)
	sql.WriteString(" AS ")
	sql.WriteString(measureValue)
	sql.WriteString(" FROM ")
	sql.WriteString(collapsed)
	sql.WriteString("), ")

	sql.WriteString(domainRows)
	sql.WriteString(" AS (SELECT toUInt8(0) AS sort_kind, ")
	sql.WriteString(splunkSeriesLabelSQL(label))
	sql.WriteString(" AS ")
	sql.WriteString(sortLabel)
	sql.WriteString(", concat('0:', ")
	sql.WriteString(label)
	sql.WriteString(") AS ")
	sql.WriteString(encoded)
	sql.WriteString(" FROM ")
	sql.WriteString(numericScores)
	sql.WriteString(" UNION ALL SELECT toUInt8(1), CAST('' AS String), CAST('1:' AS String) FROM (SELECT 1 FROM ")
	sql.WriteString(numericGroups)
	sql.WriteString(" WHERE ")
	sql.WriteString(rowEligible)
	sql.WriteString(" != 0 AND ")
	sql.WriteString(kind)
	sql.WriteString(" = 1 LIMIT 1)")
	sql.WriteString(" UNION ALL SELECT toUInt8(2), CAST('' AS String), CAST('2:' AS String) FROM (SELECT 1 FROM ")
	sql.WriteString(numericGroups)
	sql.WriteString(" WHERE ")
	sql.WriteString(rowEligible)
	sql.WriteString(" != 0 AND ")
	sql.WriteString(kind)
	sql.WriteString(" = 0 AND ")
	sql.WriteString(label)
	sql.WriteString(" NOT IN (SELECT ")
	sql.WriteString(label)
	sql.WriteString(" FROM ")
	sql.WriteString(numericScores)
	sql.WriteString(") LIMIT 1)), ")

	sql.WriteString(domain)
	sql.WriteString(" AS (SELECT arrayMap(item -> item.3, arraySort(item -> (item.1, item.2), groupArray((sort_kind, ")
	sql.WriteString(sortLabel)
	sql.WriteString(", ")
	sql.WriteString(encoded)
	sql.WriteString(")))) AS names FROM ")
	sql.WriteString(domainRows)
	sql.WriteString("), ")

	sql.WriteString(collisions)
	sql.WriteString(" AS (SELECT toUInt8(count() > 0) AS ")
	sql.WriteString(collision)
	sql.WriteString(" FROM (SELECT ")
	sql.WriteString(splunkSeriesLabelSQL(label))
	sql.WriteString(" AS ")
	sql.WriteString(normalized)
	sql.WriteString(" FROM ")
	sql.WriteString(labelTotals)
	sql.WriteString(" WHERE ")
	sql.WriteString(kind)
	sql.WriteString(" = 0 GROUP BY ")
	sql.WriteString(normalized)
	sql.WriteString(" HAVING uniqExact(")
	sql.WriteString(label)
	sql.WriteString(") > 1 LIMIT 1)), ")

	sql.WriteString(columnCheck)
	sql.WriteString(" AS (SELECT toUInt8(maxOrDefault(")
	sql.WriteString(kind)
	sql.WriteString(" = 3)) AS ")
	sql.WriteString(columnInvalid)
	sql.WriteString(" FROM ")
	sql.WriteString(labelTotals)
	sql.WriteString("), ")

	sql.WriteString(rowMaps)
	sql.WriteString(" AS (SELECT ")
	sql.WriteString(row)
	sql.WriteString(", mapFromArrays(groupArray(")
	sql.WriteString(encoded)
	sql.WriteString("), groupArray(ifNull(")
	sql.WriteString(measureValue)
	sql.WriteString(", toFloat64(0)))) AS ")
	sql.WriteString(valueMap)
	sql.WriteString(", mapFromArrays(groupArray(")
	sql.WriteString(encoded)
	sql.WriteString("), groupArray(toUInt8(isNotNull(")
	sql.WriteString(measureValue)
	sql.WriteString(")))) AS ")
	sql.WriteString(presentMap)
	sql.WriteString(" FROM ")
	sql.WriteString(finalized)
	sql.WriteString(" GROUP BY ")
	sql.WriteString(row)
	sql.WriteString("), ")

	// Missing and explicit-null dynamic row values are outside the row domain.
	// Unsupported descendants are deliberately eligible and remain visible to
	// this atomic validation guard.
	sql.WriteString(validation)
	sql.WriteString(" AS (SELECT toUInt8(maxOrDefault(")
	sql.WriteString(rowInvalid)
	sql.WriteString(") > 0) AS ")
	sql.WriteString(invalid)
	sql.WriteString(" FROM ")
	sql.WriteString(numericGroups)
	sql.WriteString(" WHERE ")
	sql.WriteString(rowEligible)
	sql.WriteString(" != 0), ")

	sql.WriteString(rowDomain)
	sql.WriteString(" AS MATERIALIZED (SELECT ")
	sql.WriteString(row)
	sql.WriteString(", toUInt64(row_number() OVER (ORDER BY ")
	sql.WriteString(rowSortSQL)
	sql.WriteString(" ASC) - 1) AS ")
	sql.WriteString(ordinal)
	sql.WriteString(" FROM (SELECT ")
	sql.WriteString(row)
	sql.WriteString(" FROM ")
	sql.WriteString(numericGroups)
	sql.WriteString(" WHERE ")
	sql.WriteString(rowEligible)
	sql.WriteString(" != 0 GROUP BY ")
	sql.WriteString(row)
	sql.WriteString(")) ")

	// The invalid sentinel is a private transport row, ordered before every
	// real row. It makes split-type/label/collision validation observable even
	// when the row axis has no eligible values. The executor buffers the whole
	// result and rejects any nonzero invalid marker before publishing a schema.
	sql.WriteString("SELECT ")
	sql.WriteString(ordinal)
	sql.WriteString(", ")
	sql.WriteString(q(ChartRowColumn))
	sql.WriteString(", ")
	if rowKind == ChartRowKindMixed {
		sql.WriteString(q(ChartRowSemanticBytesColumn))
		sql.WriteString(", ")
	}
	sql.WriteString(q(ChartNamesColumn))
	sql.WriteString(", ")
	sql.WriteString(q(ChartValuesColumn))
	sql.WriteString(", ")
	sql.WriteString(q(ChartValuePresentColumn))
	sql.WriteString(", ")
	sql.WriteString(q(ChartInvalidColumn))
	sql.WriteString(" FROM (")
	rowOutputSQL := rowDomain + "." + row
	if rowKind == ChartRowKindMixed {
		rowOutputSQL = "tupleElement(" + rowOutputSQL + ", 1)"
	}
	sql.WriteString("SELECT ")
	sql.WriteString(rowDomain)
	sql.WriteString(".")
	sql.WriteString(ordinal)
	sql.WriteString(" AS ")
	sql.WriteString(ordinal)
	sql.WriteString(", ")
	sql.WriteString(rowOutputSQL)
	sql.WriteString(" AS ")
	sql.WriteString(q(ChartRowColumn))
	sql.WriteString(", ")
	if rowKind == ChartRowKindMixed {
		sql.WriteString("tupleElement(")
		sql.WriteString(rowDomain)
		sql.WriteString(".")
		sql.WriteString(row)
		sql.WriteString(", 2) AS ")
		sql.WriteString(q(ChartRowSemanticBytesColumn))
		sql.WriteString(", ")
	}
	sql.WriteString(domain)
	sql.WriteString(".names AS ")
	sql.WriteString(q(ChartNamesColumn))
	sql.WriteString(", ")
	sql.WriteString("arrayMap(name -> ifNull(")
	sql.WriteString(rowMaps)
	sql.WriteString(".")
	sql.WriteString(valueMap)
	sql.WriteString("[name], toFloat64(0)), ")
	sql.WriteString(domain)
	sql.WriteString(".names) AS ")
	sql.WriteString(q(ChartValuesColumn))
	sql.WriteString(", ")
	sql.WriteString("arrayMap(name -> ifNull(")
	sql.WriteString(rowMaps)
	sql.WriteString(".")
	sql.WriteString(presentMap)
	sql.WriteString("[name], toUInt8(0)), ")
	sql.WriteString(domain)
	sql.WriteString(".names) AS ")
	sql.WriteString(q(ChartValuePresentColumn))
	sql.WriteString(", ")
	sql.WriteString("toUInt8(0) AS ")
	sql.WriteString(q(ChartInvalidColumn))
	sql.WriteString(" FROM ")
	sql.WriteString(rowDomain)
	sql.WriteString(" CROSS JOIN ")
	sql.WriteString(domain)
	sql.WriteString(" LEFT JOIN ")
	sql.WriteString(rowMaps)
	sql.WriteString(" ON ")
	sql.WriteString(rowMaps)
	sql.WriteString(".")
	sql.WriteString(row)
	sql.WriteString(" = ")
	sql.WriteString(rowDomain)
	sql.WriteString(".")
	sql.WriteString(row)
	sql.WriteString(" WHERE throwIf(")
	sql.WriteString(rowDomain)
	sql.WriteString(".")
	sql.WriteString(ordinal)
	sql.WriteString(" >= ")
	sql.WriteString(strconv.FormatUint(uint64(operator.RowLimit), 10))
	sql.WriteString(", '")
	sql.WriteString(ChartRowLimitMarker)
	sql.WriteString("') = 0")
	sql.WriteString(" UNION ALL SELECT toUInt64(0) AS ")
	sql.WriteString(ordinal)
	sql.WriteString(", ")
	sql.WriteString(chartValidationRowSQL(rowDatabaseType))
	sql.WriteString(" AS ")
	sql.WriteString(q(ChartRowColumn))
	if rowKind == ChartRowKindMixed {
		sql.WriteString(", toUInt8(0) AS ")
		sql.WriteString(q(ChartRowSemanticBytesColumn))
	}
	sql.WriteString(", CAST([], 'Array(String)') AS ")
	sql.WriteString(q(ChartNamesColumn))
	sql.WriteString(", CAST([], 'Array(Float64)') AS ")
	sql.WriteString(q(ChartValuesColumn))
	sql.WriteString(", CAST([], 'Array(UInt8)') AS ")
	sql.WriteString(q(ChartValuePresentColumn))
	sql.WriteString(", toUInt8(1) AS ")
	sql.WriteString(q(ChartInvalidColumn))
	sql.WriteString(" FROM ")
	sql.WriteString(validation)
	sql.WriteString(" CROSS JOIN ")
	sql.WriteString(collisions)
	sql.WriteString(" CROSS JOIN ")
	sql.WriteString(columnCheck)
	sql.WriteString(" WHERE ")
	sql.WriteString(validation)
	sql.WriteString(".")
	sql.WriteString(invalid)
	sql.WriteString(" != 0 OR ")
	sql.WriteString(collisions)
	sql.WriteString(".")
	sql.WriteString(collision)
	sql.WriteString(" != 0 OR ")
	sql.WriteString(columnCheck)
	sql.WriteString(".")
	sql.WriteString(columnInvalid)
	sql.WriteString(" != 0) AS ")
	sql.WriteString(q("__os_chart_transport"))
	sql.WriteString(" ORDER BY ")
	sql.WriteString(q(ChartInvalidColumn))
	sql.WriteString(" DESC, ")
	sql.WriteString(ordinal)
	sql.WriteString(" ASC")
	sql.WriteString(materializedCTESettingsSQL)

	sourceDepth := relationalNodeDepth(relation.depth)
	preparedDepth := relationalNodeDepth(sourceDepth)
	kindedDepth := relationalNodeDepth(preparedDepth)
	classifiedDepth := relationalNodeDepth(kindedDepth)
	canonicalizedDepth := relationalNodeDepth(classifiedDepth)
	numericStateDepth := relationalNodeDepth(canonicalizedDepth)
	numericGroupsDepth := numericStateDepth
	if valueKind != ChartValueKindPercentile {
		numericGroupsDepth = relationalNodeDepth(numericStateDepth)
	}
	labelTotalsDepth := relationalNodeDepth(numericGroupsDepth)
	numericScoresDepth := relationalNodeDepth(numericGroupsDepth)
	scoreMembershipDepth := relationalNodeDepth(numericScoresDepth)
	collapsedDepth := relationalNodeDepth(numericGroupsDepth, scoreMembershipDepth)
	finalizedDepth := relationalNodeDepth(collapsedDepth)
	domainRowsDepth := relationalNodeDepth(
		relationalNodeDepth(numericScoresDepth),
		relationalNodeDepth(numericGroupsDepth),
		relationalNodeDepth(numericGroupsDepth, scoreMembershipDepth),
	)
	domainDepth := relationalNodeDepth(domainRowsDepth)
	collisionsDepth := relationalNodeDepth(relationalNodeDepth(labelTotalsDepth))
	columnCheckDepth := relationalNodeDepth(labelTotalsDepth)
	rowMapsDepth := relationalNodeDepth(finalizedDepth)
	validationDepth := relationalNodeDepth(numericGroupsDepth)
	rowDomainDepth := relationalNodeDepth(relationalNodeDepth(numericGroupsDepth))
	regularResultDepth := relationalNodeDepth(
		rowDomainDepth,
		domainDepth,
		rowMapsDepth,
	)
	validationSentinelDepth := relationalNodeDepth(
		validationDepth,
		collisionsDepth,
		columnCheckDepth,
	)
	unionDepth := relationalNodeDepth(regularResultDepth, validationSentinelDepth)
	resultDepth := relationalNodeDepth(unionDepth)

	compiled := CompiledQuery{
		SQL:          sql.String(),
		Args:         args,
		OutputFields: slices.Clone(dynamic.FixedFields),
		Chart: &ChartOutput{
			RowField:         rowName,
			RowKind:          rowKind,
			RowDatabaseType:  rowDatabaseType,
			RowLimit:         uint64(operator.RowLimit),
			MaxSeries:        dynamic.MaxSeries,
			MaxLabelBytes:    maxTimechartLabelBytes,
			ValueKind:        valueKind,
			RowSemanticBytes: rowKind == ChartRowKindMixed,
		},
	}
	return withCompiledRelationalDepth(compiled, resultDepth, operator.Range), nil
}

type compileState struct {
	visible                          map[string]fieldState
	context                          *compileContext
	publicOrder                      []string
	privateColumns                   []string
	rexCapturedBytesSQL              string
	allowDynamic                     bool
	sparseFieldsSubset               bool
	eventRows                        bool
	blocked                          map[string]struct{}
	blockedPrefixes                  map[string]struct{}
	dynamicFieldFilters              []compiledDynamicFieldFilter
	order                            []compiledSortKey
	tieBreakers                      []compiledSortKey
	preAggregateValidationColumns    []string
	preAggregateValidationArgs       []any
	preAggregateColumns              []string
	preAggregateArgs                 []any
	preAggregateGroupExpansions      []compiledStatsGroupExpansion
	preAggregateSparklineWindows     []string
	preAggregateListWindowColumns    []string
	preAggregateListCandidateColumns []string
	postAggregateSparklines          []compiledStatsSparklineMeasure
	postAggregateChronological       []compiledChronologicalMeasure
	postAggregateScalarExtrema       []compiledScalarExtremaMeasure
	postAggregateExactStrings        []compiledExactStringMeasure
	postAggregateDistinctCounts      []compiledDistinctCount
	postAggregateOrderedStrings      []compiledOrderedStringMeasure
	deferredChronologicalValidation  []string
	chronologicalBarriers            []compiledChronologicalBarrier
	mvExpandQueryRowsSQL             string
}

type compiledDynamicFieldFilter struct {
	include  bool
	fields   []string
	patterns []string
}

type compiledStatsSparklineMeasure struct {
	recordsColumn string
	outputColumn  string
	spec          statsSparklineBucketSpec
	missing       statsSparklineMissingValue
}

// compileContext contains immutable query-wide values and shared resource
// accounting. Relation-shaping stages carry one pointer instead of manually
// copying each search-scoped constant into newly constructed compileState
// values.
type compileContext struct {
	operationContext                       context.Context
	patternBudgets                         compiledPatternBudgets
	strftimeBudget                         compiledStrftimeBudget
	strptimeBudget                         compiledStrptimeBudget
	relativeTimeBudget                     compiledRelativeTimeBudget
	unixTimestampBudget                    compiledUnixTimestampBudget
	concatenationBudget                    compiledConcatenationBudget
	stringConversionBudget                 compiledStringConversionBudget
	arithmeticOperators                    int
	membershipCandidates                   int
	mvExpandStages                         uint8
	atomicResult                           bool
	requiresMaterializedValidationSettings bool
	searchStartUnix                        int64
	searchEarliest                         time.Time
	searchLatest                           time.Time
	searchTimezone                         string
	searchLocalMinimumUnixNanoseconds      int64
	searchTimezoneChecked                  bool
	searchTimezoneInvalid                  bool
	lookupTables                           []compiledLookupExternalTable
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

func compileScalarValue(expression plan.ScalarExpression, state compileState) (compiledScalar, error) {
	switch expression := expression.(type) {
	case *plan.ScalarUnaryExpression:
		return compileArithmeticUnary(expression, state)
	case *plan.ScalarBinaryExpression:
		return compileArithmeticBinary(expression, state)
	case *plan.ScalarFieldExpression:
		if expression == nil {
			return compiledScalar{}, errors.New("compile ClickHouse scalar expression: missing field expression")
		}
		field, ok, err := resolveCompiledField(expression.Field, state)
		if err != nil {
			return compiledScalar{}, err
		}
		if !ok {
			return compiledScalar{
				valueSQL:       "CAST(NULL AS Nullable(String))",
				maxStringBytes: 1,
				existsSQL:      "0",
				kind:           fieldKindString,
				alwaysNull:     true,
			}, nil
		}
		return compiledScalarFromField(field), nil
	case *plan.ScalarLiteralExpression:
		if expression == nil {
			return compiledScalar{}, errors.New("compile ClickHouse scalar expression: missing literal expression")
		}
		value := expression.Value
		kind := fieldKindString
		numberType := ""
		valueSQL := ""
		maxStringBytes := uint64(64)
		var argument any
		switch value.Kind {
		case plan.ValueKindString:
			valueSQL, argument = "CAST(? AS String)", value.String
			maxStringBytes = max(uint64(1), uint64(len(value.String)))
		case plan.ValueKindInt64:
			kind, numberType = fieldKindNumber, "Int64"
			valueSQL, argument = "CAST(? AS Int64)", value.Int64
		case plan.ValueKindUint64:
			kind, numberType = fieldKindNumber, "UInt64"
			valueSQL, argument = "CAST(? AS UInt64)", value.Uint64
		case plan.ValueKindFloat64:
			kind, numberType = fieldKindNumber, "Float64"
			valueSQL, argument = "CAST(? AS Float64)", value.Float64
		case plan.ValueKindBool:
			kind = fieldKindBool
			valueSQL, argument = "CAST(? AS Bool)", value.Bool
			maxStringBytes = 5
		case plan.ValueKindNull:
			return compiledScalar{
				valueSQL:         "CAST(NULL AS Nullable(String))",
				maxStringBytes:   1,
				existsSQL:        "1",
				kind:             fieldKindInvalid,
				literal:          &value,
				alwaysNull:       true,
				comparisonAtomic: true,
			}, nil
		default:
			return compiledScalar{}, errors.New("compile ClickHouse scalar expression: invalid literal")
		}
		return compiledScalar{
			valueSQL:         valueSQL,
			valueArgs:        []any{argument},
			maxStringBytes:   maxStringBytes,
			existsSQL:        "1",
			kind:             kind,
			numberType:       numberType,
			literal:          &value,
			comparisonAtomic: true,
		}, nil
	case *plan.ScalarCallExpression:
		if expression == nil {
			return compiledScalar{}, errors.New("compile ClickHouse scalar expression: missing call expression")
		}
		switch expression.Function {
		case plan.ScalarFunctionNow:
			return compileNowScalar(expression, state)
		case plan.ScalarFunctionStrftime:
			return compileStrftimeScalar(expression, state)
		case plan.ScalarFunctionStrptime:
			return compileStrptimeScalar(expression, state)
		case plan.ScalarFunctionRelativeTime:
			return compileRelativeTimeScalar(expression, state)
		case plan.ScalarFunctionReplace:
			return compileReplaceScalar(expression, state)
		case plan.ScalarFunctionToNumber:
			return compileToNumberScalar(expression, state)
		case plan.ScalarFunctionToString:
			return compileToStringScalar(expression, state)
		case plan.ScalarFunctionConcat:
			return compileConcatenationScalar(expression, state)
		case plan.ScalarFunctionSplit:
			return compileBoundedNativeMVScalar(expression, state, compileSplitScalar)
		case plan.ScalarFunctionMVAppend:
			return compileBoundedNativeMVScalar(expression, state, compileMVAppendScalar)
		case plan.ScalarFunctionMVDedup:
			return compileBoundedNativeMVScalar(expression, state, compileMVDedupScalar)
		case plan.ScalarFunctionMVIndex:
			return compileBoundedNativeMVScalar(expression, state, compileMVIndexScalar)
		case plan.ScalarFunctionMVJoin:
			return compileBoundedNativeMVScalar(expression, state, compileMVJoinScalar)
		case plan.ScalarFunctionMVZip:
			return compileBoundedNativeMVScalar(expression, state, compileMVZipScalar)
		case plan.ScalarFunctionMVFind:
			return compileBoundedNativeMVScalar(expression, state, compileMVFindScalar)
		case plan.ScalarFunctionRound:
			return compileRoundScalar(expression, state)
		case plan.ScalarFunctionCeil:
			return compileIntegralRoundingScalar(expression, state, "ceil")
		case plan.ScalarFunctionFloor:
			return compileIntegralRoundingScalar(expression, state, "floor")
		case plan.ScalarFunctionMVCount:
			return compileMVCountScalar(expression, state)
		case plan.ScalarFunctionMVSort:
			return compileMVSortScalar(expression, state)
		case plan.ScalarFunctionMatch:
			return compileMatchScalar(expression, state)
		case plan.ScalarFunctionLike:
			return compileLikeScalar(expression, state)
		case plan.ScalarFunctionIsNull, plan.ScalarFunctionIsNotNull:
			return compileNullTestScalar(expression, state)
		case plan.ScalarFunctionCoalesce:
			return compileCoalesceScalar(expression, state)
		case plan.ScalarFunctionLower, plan.ScalarFunctionUpper:
			return compileTextCaseScalar(expression, state)
		case plan.ScalarFunctionTrim,
			plan.ScalarFunctionLTrim,
			plan.ScalarFunctionRTrim,
			plan.ScalarFunctionURLDecode,
			plan.ScalarFunctionMD5,
			plan.ScalarFunctionSHA1,
			plan.ScalarFunctionSHA256,
			plan.ScalarFunctionSHA512:
			return compileTextTransformScalar(expression, state)
		case plan.ScalarFunctionTypeOf:
			return compileTypeOfScalar(expression, state)
		case plan.ScalarFunctionCIDRMatch:
			return compileCIDRMatchScalar(expression, state)
		case plan.ScalarFunctionAbs,
			plan.ScalarFunctionSqrt,
			plan.ScalarFunctionExp,
			plan.ScalarFunctionLn,
			plan.ScalarFunctionLog,
			plan.ScalarFunctionPow,
			plan.ScalarFunctionPi:
			return compileMathScalar(expression, state)
		case plan.ScalarFunctionLength:
			return compileTextLengthScalar(expression, state)
		case plan.ScalarFunctionSubstring:
			return compileSubstringScalar(expression, state)
		default:
			return compiledScalar{}, fmt.Errorf("compile ClickHouse scalar expression: unsupported function %d", expression.Function)
		}
	case *plan.ScalarIfExpression:
		return compileIfScalar(expression, state)
	case *plan.ScalarCaseExpression:
		return compileCaseScalar(expression, state)
	default:
		return compiledScalar{}, fmt.Errorf("compile ClickHouse scalar expression: unsupported expression %T", expression)
	}
}

func compileNowScalar(
	expression *plan.ScalarCallExpression,
	state compileState,
) (compiledScalar, error) {
	if expression == nil {
		return compiledScalar{}, errors.New("compile ClickHouse now: missing expression")
	}
	if len(expression.Arguments) != 0 {
		return compiledScalar{}, errors.New("compile ClickHouse now: now requires no arguments")
	}
	if state.context == nil {
		return compiledScalar{}, errors.New("compile ClickHouse now: search-start anchor is required")
	}
	return compiledScalar{
		valueSQL:         "CAST(? AS Int64)",
		valueArgs:        []any{state.context.searchStartUnix},
		maxStringBytes:   20,
		existsSQL:        "1",
		kind:             fieldKindNumber,
		numberType:       "Int64",
		numericIntegral:  true,
		comparisonAtomic: true,
	}, nil
}

func compileStrftimeScalar(
	expression *plan.ScalarCallExpression,
	state compileState,
) (compiledScalar, error) {
	if expression == nil {
		return compiledScalar{}, errors.New("compile ClickHouse strftime: missing expression")
	}
	if len(expression.Arguments) != 2 {
		return compiledScalar{}, errors.New(
			"compile ClickHouse strftime: expected two arguments",
		)
	}
	format, ok := scalarQuotedStringLiteral(expression.Arguments[1])
	if !ok {
		return compiledScalar{}, errors.New(
			"compile ClickHouse strftime: format must be a quoted string literal",
		)
	}
	if state.context == nil {
		return compiledScalar{}, errors.New(
			"compile ClickHouse strftime: search timezone is required",
		)
	}
	if err := validateCompileContextSearchTimezone(state.context); err != nil {
		return compiledScalar{}, err
	}

	compiledFormat, cached := state.context.strftimeBudget.formats[expression]
	if !cached {
		var err error
		compiledFormat, err = compileStrftimeFormatForBackend(
			format,
			expression.Arguments[1].SourceRange(),
		)
		if err != nil {
			return compiledScalar{}, err
		}
		state.context.strftimeBudget.formats[expression] = compiledFormat
	}
	if compiledFormat.WorkUnits >
		MaximumStrftimeQueryWorkUnits-state.context.strftimeBudget.workUnits {
		return compiledScalar{}, &plan.Diagnostic{
			Code: "SPL_QUERY_TOO_COMPLEX",
			Message: fmt.Sprintf(
				"search strftime formats require more than %d work units",
				MaximumStrftimeQueryWorkUnits,
			),
			Range: expression.Range,
		}
	}
	if compiledFormat.MaximumOutputBytes >
		MaximumStrftimeQueryOutputBytes-state.context.strftimeBudget.outputBytes {
		return compiledScalar{}, &plan.Diagnostic{
			Code: "SPL_QUERY_TOO_COMPLEX",
			Message: fmt.Sprintf(
				"search strftime results may exceed %d bytes per row",
				MaximumStrftimeQueryOutputBytes,
			),
			Range: expression.Range,
		}
	}
	state.context.strftimeBudget.workUnits += compiledFormat.WorkUnits
	state.context.strftimeBudget.outputBytes += compiledFormat.MaximumOutputBytes

	input, err := compileNonBooleanScalarInputArgument(
		expression.Arguments[0],
		state,
		"strftime",
	)
	if err != nil {
		return compiledScalar{}, err
	}
	if isNativeMultivalueKind(input.kind) {
		return compiledScalar{}, unsupportedMultivalueUsage(
			"strftime",
			expression.Range,
		)
	}
	if input.alwaysNull ||
		input.kind == fieldKindInvalid ||
		input.kind == fieldKindDynamic &&
			input.dynamicDomain == dynamicScalarDomainText {
		return compiledScalar{
			valueSQL:                "CAST(NULL AS Nullable(String))",
			maxStringBytes:          1,
			existsSQL:               "1",
			kind:                    fieldKindString,
			alwaysNull:              true,
			materializeForPredicate: input.materializeForPredicate,
		}, nil
	}
	if input.kind == fieldKindString || input.kind == fieldKindBool {
		return compiledScalar{}, &plan.Diagnostic{
			Code:    "SPL_UNSUPPORTED_STRFTIME_VALUE_TYPE",
			Message: "strftime requires a numeric Unix-seconds value or time field",
			Range:   expression.Arguments[0].SourceRange(),
		}
	}
	if err := chargeUnixTimestampDynamicDecimalBudget(
		input,
		state.context,
		"strftime",
		expression.Range,
	); err != nil {
		return compiledScalar{}, err
	}

	timestampSQL, err := unixTimestampScalarSQL(input, "strftime")
	if err != nil {
		return compiledScalar{}, err
	}
	formattedSQL, formatArgs, err := compileStrftimeParts(
		compiledFormat.Parts,
	)
	if err != nil {
		return compiledScalar{}, err
	}
	timestampBinding := "arrayElement(arrayMap(timestamp -> if(isNull(timestamp), " +
		"CAST(NULL AS Nullable(String)), " + formattedSQL + "), [" +
		"toTimeZone(" + timestampSQL + ", ?)]), 1)"
	valueSQL := "arrayElement(arrayMap(value -> " + timestampBinding +
		", [" + input.valueSQL + "]), 1)"
	if len(valueSQL) > maxCompiledStrftimeScalarSQLBytes {
		return compiledScalar{}, &plan.Diagnostic{
			Code: "SPL_QUERY_TOO_COMPLEX",
			Message: fmt.Sprintf(
				"strftime scalar SQL exceeds %d bytes",
				maxCompiledStrftimeScalarSQLBytes,
			),
			Range: expression.Range,
		}
	}
	valueArgs := make(
		[]any,
		0,
		len(formatArgs)+1+len(input.valueArgs),
	)
	valueArgs = append(valueArgs, formatArgs...)
	valueArgs = append(valueArgs, state.context.searchTimezone)
	valueArgs = append(valueArgs, input.valueArgs...)
	return compiledScalar{
		valueSQL:                valueSQL,
		valueArgs:               valueArgs,
		maxStringBytes:          max(uint64(1), compiledFormat.MaximumOutputBytes),
		existsSQL:               "1",
		kind:                    fieldKindString,
		materializeForPredicate: input.materializeForPredicate,
	}, nil
}

// errSearchTimezoneInvalid is a package-level sentinel so compileContext can
// cache the check outcome as a plain bool: storing the error itself would put
// an error value inside compileState's type closure and undermine the
// reflect.DeepEqual state seals that compare compiled preludes.
var errSearchTimezoneInvalid = errors.New(
	"compile ClickHouse date/time function: search timezone is invalid",
)

func validateCompileContextSearchTimezone(context *compileContext) error {
	if context.searchTimezoneChecked {
		if context.searchTimezoneInvalid {
			return errSearchTimezoneInvalid
		}
		return nil
	}
	context.searchTimezoneChecked = true
	location, err := ianatimezone.Load(context.searchTimezone)
	if err != nil {
		context.searchTimezoneInvalid = true
		return errSearchTimezoneInvalid
	}
	localMinimum := time.Date(
		searchtimebounds.MinimumYear,
		time.January,
		1,
		0,
		0,
		0,
		0,
		location,
	)
	// ClickHouse clamps some localized DateTime64 values at its 1900 floor
	// and can report a wall-clock remainder instead of a true UTC offset.
	// Derive the earliest safe local civil instant from the same IANA rules
	// used by search admission. Pinned integration coverage with a historical
	// second-offset zone detects drift against ClickHouse's bundled tzdb.
	context.searchLocalMinimumUnixNanoseconds =
		localMinimum.Unix() * 1_000_000_000
	return nil
}

func compileStrptimeScalar(
	expression *plan.ScalarCallExpression,
	state compileState,
) (compiledScalar, error) {
	if expression == nil {
		return compiledScalar{}, errors.New(
			"compile ClickHouse strptime: missing expression",
		)
	}
	if len(expression.Arguments) != 2 {
		return compiledScalar{}, errors.New(
			"compile ClickHouse strptime: expected two arguments",
		)
	}
	format, ok := scalarQuotedStringLiteral(expression.Arguments[1])
	if !ok {
		return compiledScalar{}, errors.New(
			"compile ClickHouse strptime: format must be a quoted string literal",
		)
	}
	if state.context == nil {
		return compiledScalar{}, errors.New(
			"compile ClickHouse strptime: search timezone is required",
		)
	}
	if err := validateCompileContextSearchTimezone(state.context); err != nil {
		return compiledScalar{}, err
	}

	compiledFormat, cached := state.context.strptimeBudget.formats[expression]
	if !cached {
		var err error
		compiledFormat, err = compileStrptimeFormatForBackend(
			format,
			expression.Arguments[1].SourceRange(),
		)
		if err != nil {
			return compiledScalar{}, err
		}
		if state.context.strptimeBudget.formats == nil {
			state.context.strptimeBudget.formats =
				make(map[*plan.ScalarCallExpression]spltimeformat.StrptimeFormat)
		}
		state.context.strptimeBudget.formats[expression] = compiledFormat
	}
	if compiledFormat.WorkUnits >
		MaximumStrptimeQueryWorkUnits-state.context.strptimeBudget.workUnits {
		return compiledScalar{}, &plan.Diagnostic{
			Code: "SPL_QUERY_TOO_COMPLEX",
			Message: fmt.Sprintf(
				"search strptime formats require more than %d work units",
				MaximumStrptimeQueryWorkUnits,
			),
			Range: expression.Range,
		}
	}
	state.context.strptimeBudget.workUnits += compiledFormat.WorkUnits

	input, err := compileNonBooleanScalarInputArgument(
		expression.Arguments[0],
		state,
		"strptime",
	)
	if err != nil {
		return compiledScalar{}, err
	}
	if isNativeMultivalueKind(input.kind) {
		return compiledScalar{}, unsupportedMultivalueUsage(
			"strptime",
			expression.Range,
		)
	}
	if input.alwaysNull || input.kind == fieldKindInvalid {
		return compiledScalar{
			valueSQL:                "CAST(NULL AS Nullable(Float64))",
			maxStringBytes:          1,
			existsSQL:               "1",
			kind:                    fieldKindNumber,
			numberType:              "Float64",
			alwaysNull:              true,
			materializeForPredicate: input.materializeForPredicate,
		}, nil
	}
	if input.kind != fieldKindString && input.kind != fieldKindDynamic {
		return compiledScalar{}, &plan.Diagnostic{
			Code:    "SPL_UNSUPPORTED_STRPTIME_VALUE_TYPE",
			Message: "strptime requires a String timestamp value",
			Range:   expression.Arguments[0].SourceRange(),
		}
	}

	inputBytes := min(
		compiledScalarStringByteBound(input),
		MaximumStrptimeInputBytes,
	)
	if inputBytes >
		MaximumStrptimeQueryInputBytes-state.context.strptimeBudget.inputBytes {
		return compiledScalar{}, &plan.Diagnostic{
			Code: "SPL_QUERY_TOO_COMPLEX",
			Message: fmt.Sprintf(
				"search strptime inputs require more than %d bytes of date parsing per row",
				MaximumStrptimeQueryInputBytes,
			),
			Range: expression.Range,
		}
	}
	state.context.strptimeBudget.inputBytes += inputBytes

	patterns, err := compileStrptimePatterns(compiledFormat.Parts)
	if err != nil {
		return compiledScalar{}, err
	}
	inputSQL, inputArgs := compiledTextEligibleStringScalar(input)
	parserSQL := "parseDateTime64InJodaSyntaxOrNull(value, ?, ?)"
	parserArgs := []any{
		patterns.primaryJoda,
		state.context.searchTimezone,
	}
	if patterns.fallbackJoda != "" {
		parserSQL = "if(notEmpty(arrayElement(groups, " +
			strconv.Itoa(patterns.optionalFractionGroup) + ")), " +
			parserSQL +
			", parseDateTime64InJodaSyntaxOrNull(value, ?, ?))"
		parserArgs = []any{
			patterns.primaryJoda,
			state.context.searchTimezone,
			patterns.fallbackJoda,
			state.context.searchTimezone,
		}
	}
	maximumDateGroup := max(
		patterns.yearGroup,
		patterns.monthGroup,
		patterns.dayGroup,
	)
	civilDateSQL := "toUInt32OrZero(arrayElement(groups, " +
		strconv.Itoa(patterns.yearGroup) + ")) * 10000 + " +
		"toUInt32OrZero(arrayElement(groups, " +
		strconv.Itoa(patterns.monthGroup) + ")) * 100 + " +
		"toUInt32OrZero(arrayElement(groups, " +
		strconv.Itoa(patterns.dayGroup) + "))"
	parserSQL = "if(ifNull(length(value) <= " +
		strconv.FormatUint(MaximumStrptimeInputBytes, 10) +
		", 0), arrayElement(arrayMap(groups -> if(length(groups) >= " +
		strconv.Itoa(maximumDateGroup) + " AND (" + civilDateSQL + ") >= " +
		strconv.Itoa(minimumStrptimeCivilDate) + " AND (" + civilDateSQL +
		") <= " + strconv.Itoa(maximumStrptimeCivilDate) + ", " + parserSQL +
		", NULL), [extractGroups(ifNull(value, CAST('' AS String)), ?)]), 1), NULL)"
	microsecondsSQL := "toUnixTimestamp64Micro(" + parserSQL + ")"
	epochSQL := "arrayElement(arrayMap(microseconds -> if(" +
		"isNull(microseconds), CAST(NULL AS Nullable(Float64)), " +
		"toFloat64(microseconds) / 1000000), [" + microsecondsSQL + "]), 1)"
	valueSQL := "arrayElement(arrayMap(value -> " + epochSQL +
		", [" + inputSQL + "]), 1)"
	if len(valueSQL) > maxCompiledStrptimeScalarSQLBytes {
		return compiledScalar{}, &plan.Diagnostic{
			Code: "SPL_QUERY_TOO_COMPLEX",
			Message: fmt.Sprintf(
				"strptime scalar SQL exceeds %d bytes",
				maxCompiledStrptimeScalarSQLBytes,
			),
			Range: expression.Range,
		}
	}
	valueArgs := make([]any, 0, 1+len(parserArgs)+len(inputArgs))
	valueArgs = append(valueArgs, parserArgs...)
	valueArgs = append(valueArgs, patterns.civilRegex)
	valueArgs = append(valueArgs, inputArgs...)
	return compiledScalar{
		valueSQL:                valueSQL,
		valueArgs:               valueArgs,
		maxStringBytes:          64,
		existsSQL:               "1",
		kind:                    fieldKindNumber,
		numberType:              "Float64",
		materializeForPredicate: input.materializeForPredicate,
	}, nil
}

func compileRelativeTimeScalar(
	expression *plan.ScalarCallExpression,
	state compileState,
) (compiledScalar, error) {
	if expression == nil {
		return compiledScalar{}, errors.New(
			"compile ClickHouse relative_time: missing expression",
		)
	}
	if len(expression.Arguments) != 2 {
		return compiledScalar{}, errors.New(
			"compile ClickHouse relative_time: expected two arguments",
		)
	}
	specifierText, ok := scalarQuotedStringLiteral(expression.Arguments[1])
	if !ok {
		return compiledScalar{}, errors.New(
			"compile ClickHouse relative_time: specifier must be a quoted string literal",
		)
	}
	if state.context == nil {
		return compiledScalar{}, errors.New(
			"compile ClickHouse relative_time: search timezone is required",
		)
	}
	if err := validateCompileContextSearchTimezone(state.context); err != nil {
		return compiledScalar{}, err
	}

	specifier, cached := state.context.relativeTimeBudget.specifiers[expression]
	if !cached {
		var err error
		specifier, err = compileRelativeTimeSpecifierForBackend(
			specifierText,
			expression.Arguments[1].SourceRange(),
		)
		if err != nil {
			return compiledScalar{}, err
		}
		if state.context.relativeTimeBudget.specifiers == nil {
			state.context.relativeTimeBudget.specifiers =
				make(map[*plan.ScalarCallExpression]splrelativetime.Specifier)
		}
		state.context.relativeTimeBudget.specifiers[expression] = specifier
	}
	if specifier.WorkUnits >
		MaximumRelativeTimeQueryWorkUnits-
			state.context.relativeTimeBudget.workUnits {
		return compiledScalar{}, &plan.Diagnostic{
			Code: "SPL_QUERY_TOO_COMPLEX",
			Message: fmt.Sprintf(
				"search relative_time specifiers require more than %d work units",
				MaximumRelativeTimeQueryWorkUnits,
			),
			Range: expression.Range,
		}
	}
	operationCount := specifier.OperationCount()
	if operationCount >
		MaximumRelativeTimeQueryOperations-
			state.context.relativeTimeBudget.operations {
		return compiledScalar{}, &plan.Diagnostic{
			Code: "SPL_QUERY_TOO_COMPLEX",
			Message: fmt.Sprintf(
				"search relative_time specifiers contain more than %d operations",
				MaximumRelativeTimeQueryOperations,
			),
			Range: expression.Range,
		}
	}
	state.context.relativeTimeBudget.workUnits += specifier.WorkUnits
	state.context.relativeTimeBudget.operations += operationCount

	input, err := compileNonBooleanScalarInputArgument(
		expression.Arguments[0],
		state,
		"relative_time",
	)
	if err != nil {
		return compiledScalar{}, err
	}
	if isNativeMultivalueKind(input.kind) {
		return compiledScalar{}, unsupportedMultivalueUsage(
			"relative_time",
			expression.Range,
		)
	}
	if input.alwaysNull ||
		input.kind == fieldKindInvalid ||
		input.kind == fieldKindDynamic &&
			input.dynamicDomain == dynamicScalarDomainText {
		return compiledScalar{
			valueSQL:                "CAST(NULL AS Nullable(Float64))",
			maxStringBytes:          1,
			existsSQL:               "1",
			kind:                    fieldKindNumber,
			numberType:              "Float64",
			alwaysNull:              true,
			materializeForPredicate: input.materializeForPredicate,
		}, nil
	}
	if input.kind != fieldKindTime &&
		input.kind != fieldKindNumber &&
		input.kind != fieldKindDynamic {
		return compiledScalar{}, &plan.Diagnostic{
			Code:    "SPL_UNSUPPORTED_RELATIVE_TIME_VALUE_TYPE",
			Message: "relative_time requires a numeric Unix-seconds value or time field",
			Range:   expression.Arguments[0].SourceRange(),
		}
	}
	if err := chargeUnixTimestampDynamicDecimalBudget(
		input,
		state.context,
		"relative_time",
		expression.Range,
	); err != nil {
		return compiledScalar{}, err
	}

	timestampSQL, err := unixTimestampScalarSQL(input, "relative_time")
	if err != nil {
		return compiledScalar{}, err
	}
	programSQL := compileRelativeTimeInputTimestampSQL(
		timestampSQL,
		input.valueSQL,
	)
	programArgs := make(
		[]any,
		0,
		1+len(input.valueArgs)+operationCount,
	)
	programArgs = append(programArgs, state.context.searchTimezone)
	programArgs = append(programArgs, input.valueArgs...)
	for index := range operationCount {
		operation, found := specifier.Operation(index)
		if !found {
			return compiledScalar{}, errors.New(
				"compile ClickHouse relative_time: validated operation is missing",
			)
		}
		programSQL, programArgs, err = compileRelativeTimeOperation(
			programSQL,
			programArgs,
			operation,
			state.context.searchLocalMinimumUnixNanoseconds,
		)
		if err != nil {
			return compiledScalar{}, err
		}
	}

	valueSQL := "arrayElement(arrayMap(value -> if(isNull(value), " +
		"CAST(NULL AS Nullable(Float64)), " +
		"toFloat64(toUnixTimestamp64Nano(value)) / 1000000000), [" +
		programSQL + "]), 1)"
	if len(valueSQL) > maxCompiledRelativeTimeScalarSQLBytes {
		return compiledScalar{}, &plan.Diagnostic{
			Code: "SPL_QUERY_TOO_COMPLEX",
			Message: fmt.Sprintf(
				"relative_time scalar SQL exceeds %d bytes",
				maxCompiledRelativeTimeScalarSQLBytes,
			),
			Range: expression.Range,
		}
	}
	return compiledScalar{
		valueSQL:                valueSQL,
		valueArgs:               programArgs,
		maxStringBytes:          64,
		existsSQL:               "1",
		kind:                    fieldKindNumber,
		numberType:              "Float64",
		materializeForPredicate: input.materializeForPredicate,
	}, nil
}

func compileRelativeTimeSpecifierForBackend(
	source string,
	sourceRange spl.Range,
) (splrelativetime.Specifier, error) {
	specifier, err := splrelativetime.CompileSpecifier(source)
	if err == nil {
		return specifier, nil
	}
	if errors.Is(err, splrelativetime.ErrSpecifierTooLarge) {
		return splrelativetime.Specifier{}, &plan.Diagnostic{
			Code:    "SPL_QUERY_TOO_COMPLEX",
			Message: "relative_time specifier exceeds its resource limit",
			Range:   sourceRange,
		}
	}
	if errors.Is(err, splrelativetime.ErrMagnitudeOutOfRange) {
		return splrelativetime.Specifier{}, &plan.Diagnostic{
			Code: "SPL_NUMBER_OUT_OF_RANGE",
			Message: "relative_time magnitude exceeds the supported " +
				searchtimebounds.YearRangeDescription + " timestamp span",
			Range: sourceRange,
		}
	}
	return splrelativetime.Specifier{}, &plan.Diagnostic{
		Code: "SPL_UNSUPPORTED_RELATIVE_TIME_SPECIFIER",
		Message: "relative_time specifier is outside the supported bounded " +
			"offset-and-snap subset",
		Range: sourceRange,
	}
}

func compileRelativeTimeInputTimestampSQL(
	timestampSQL string,
	inputSQL string,
) string {
	return "arrayElement(arrayMap(value -> arrayElement(arrayMap(timestamp -> " +
		"if(" + relativeTimeTimestampRangeCondition("timestamp") +
		", toTimeZone(timestamp, ?), NULL), [" + timestampSQL +
		"]), 1), [" + inputSQL + "]), 1)"
}

func compileRelativeTimeOperation(
	inputSQL string,
	inputArgs []any,
	operation splrelativetime.Operation,
	localMinimumUnixNanoseconds int64,
) (string, []any, error) {
	switch operation.Kind {
	case splrelativetime.OperationOffset:
		if operation.Magnitude == 0 {
			return inputSQL, inputArgs, nil
		}
		var (
			compiledSQL string
			err         error
		)
		switch operation.Unit {
		case splrelativetime.UnitSecond:
			compiledSQL = compileRelativeTimeElapsedOffsetSQL(
				inputSQL,
				operation,
				1_000_000_000,
			)
		case splrelativetime.UnitMinute:
			compiledSQL = compileRelativeTimeElapsedOffsetSQL(
				inputSQL,
				operation,
				60*1_000_000_000,
			)
		case splrelativetime.UnitHour:
			compiledSQL = compileRelativeTimeElapsedOffsetSQL(
				inputSQL,
				operation,
				60*60*1_000_000_000,
			)
		case splrelativetime.UnitDay:
			compiledSQL = compileRelativeTimeCalendarDayOffsetSQL(
				inputSQL,
				operation,
				1,
				localMinimumUnixNanoseconds,
			)
		case splrelativetime.UnitWeek:
			compiledSQL = compileRelativeTimeCalendarDayOffsetSQL(
				inputSQL,
				operation,
				7,
				localMinimumUnixNanoseconds,
			)
		case splrelativetime.UnitMonth:
			compiledSQL = compileRelativeTimeCalendarMonthOffsetSQL(
				inputSQL,
				operation,
				1,
				localMinimumUnixNanoseconds,
			)
		case splrelativetime.UnitQuarter:
			compiledSQL = compileRelativeTimeCalendarMonthOffsetSQL(
				inputSQL,
				operation,
				3,
				localMinimumUnixNanoseconds,
			)
		case splrelativetime.UnitYear:
			compiledSQL = compileRelativeTimeCalendarMonthOffsetSQL(
				inputSQL,
				operation,
				12,
				localMinimumUnixNanoseconds,
			)
		default:
			err = errors.New(
				"compile ClickHouse relative_time: invalid offset unit",
			)
		}
		if err != nil {
			return "", nil, err
		}
		args := make([]any, 0, 1+len(inputArgs))
		args = append(args, operation.Magnitude)
		args = append(args, inputArgs...)
		return compiledSQL, args, nil
	case splrelativetime.OperationSnap:
		compiledSQL, err := compileRelativeTimeSnapSQL(
			inputSQL,
			operation,
			localMinimumUnixNanoseconds,
		)
		if err != nil {
			return "", nil, err
		}
		return compiledSQL, inputArgs, nil
	default:
		return "", nil, errors.New(
			"compile ClickHouse relative_time: invalid operation",
		)
	}
}

func compileRelativeTimeElapsedOffsetSQL(
	inputSQL string,
	operation splrelativetime.Operation,
	nanosecondsPerUnit uint64,
) string {
	operator := "+"
	if operation.Negative {
		operator = "-"
	}
	targetTicks := "toInt256(toUnixTimestamp64Nano(value)) " + operator +
		" toInt256(?) * toInt256(" +
		strconv.FormatUint(nanosecondsPerUnit, 10) + ")"
	candidate := "arrayElement(arrayMap(ticks -> if(isNotNull(value) AND ticks >= " +
		"toInt256(" +
		strconv.FormatInt(minimumRelativeTimeUnixNanoseconds, 10) +
		") AND ticks <= toInt256(" +
		strconv.FormatInt(maximumRelativeTimeUnixNanoseconds, 10) +
		"), toTimeZone(fromUnixTimestamp64Nano(" +
		"accurateCastOrNull(ticks, 'Int64'), 'UTC'), timezoneOf(value)), " +
		"NULL), [" + targetTicks + "]), 1)"
	return boundedRelativeTimeTimestampSQL(
		inputSQL,
		candidate,
		relativeTimeOffsetResultDirection(operation),
	)
}

func compileRelativeTimeCalendarDayOffsetSQL(
	inputSQL string,
	operation splrelativetime.Operation,
	daysPerUnit uint64,
	localMinimumUnixNanoseconds int64,
) string {
	operator := "+"
	if operation.Negative {
		operator = "-"
	}
	currentDay := "toInt64(toDaysSinceYearZero(value))"
	targetDay := currentDay + " " + operator + " toInt64(?) * " +
		strconv.FormatUint(daysPerUnit, 10)
	valid := relativeTimeLocalCivilLowerBoundCondition(
		localMinimumUnixNanoseconds,
	) + " AND " +
		relativeTimeCalendarDayRangeCondition("target_day")
	adjusted := "addDays(value, if(" + valid + ", target_day - " +
		currentDay + ", 0))"
	candidate := "arrayElement(arrayMap(target_day -> " +
		"if(isNotNull(value) AND " + valid + ", " + adjusted +
		", NULL), [" + targetDay + "]), 1)"
	return boundedRelativeTimeTimestampSQL(
		inputSQL,
		candidate,
		relativeTimeOffsetResultDirection(operation),
	)
}

func compileRelativeTimeCalendarMonthOffsetSQL(
	inputSQL string,
	operation splrelativetime.Operation,
	monthsPerUnit uint64,
	localMinimumUnixNanoseconds int64,
) string {
	operator := "+"
	if operation.Negative {
		operator = "-"
	}
	currentMonth := "toInt64(toYear(value)) * 12 + " +
		"toInt64(toMonth(value)) - 1"
	targetMonth := currentMonth + " " + operator + " toInt64(?) * " +
		strconv.FormatUint(monthsPerUnit, 10)
	valid := relativeTimeLocalCivilLowerBoundCondition(
		localMinimumUnixNanoseconds,
	) + " AND " +
		relativeTimeCalendarMonthRangeCondition("target_month")
	adjusted := "addMonths(value, if(" + valid + ", target_month - (" +
		currentMonth + "), 0))"
	candidate := "arrayElement(arrayMap(target_month -> " +
		"if(isNotNull(value) AND " + valid + ", " + adjusted +
		", NULL), [" + targetMonth + "]), 1)"
	return boundedRelativeTimeTimestampSQL(
		inputSQL,
		candidate,
		relativeTimeOffsetResultDirection(operation),
	)
}

func compileRelativeTimeSnapSQL(
	inputSQL string,
	operation splrelativetime.Operation,
	localMinimumUnixNanoseconds int64,
) (string, error) {
	body := ""
	switch operation.Unit {
	case splrelativetime.UnitSecond:
		body = compileRelativeTimeSubdaySnapCandidateSQL(
			1_000_000_000,
			false,
		)
	case splrelativetime.UnitMinute:
		body = compileRelativeTimeSubdaySnapCandidateSQL(
			60*1_000_000_000,
			true,
		)
	case splrelativetime.UnitHour:
		body = compileRelativeTimeSubdaySnapCandidateSQL(
			60*60*1_000_000_000,
			true,
		)
	case splrelativetime.UnitDay:
		body = relativeTimeLocallyRepresentableCandidateSQL(
			"dateTrunc('day', value)",
			localMinimumUnixNanoseconds,
		)
	case splrelativetime.UnitWeek:
		return compileRelativeTimeWeekSnapSQL(
			inputSQL,
			operation.Weekday,
			localMinimumUnixNanoseconds,
		), nil
	case splrelativetime.UnitMonth:
		body = relativeTimeLocallyRepresentableCandidateSQL(
			"toDateTime64(dateTrunc('month', value), 0, timezoneOf(value))",
			localMinimumUnixNanoseconds,
		)
	case splrelativetime.UnitQuarter:
		body = relativeTimeLocallyRepresentableCandidateSQL(
			"toDateTime64(dateTrunc('quarter', value), 0, timezoneOf(value))",
			localMinimumUnixNanoseconds,
		)
	case splrelativetime.UnitYear:
		body = relativeTimeLocallyRepresentableCandidateSQL(
			"toDateTime64(dateTrunc('year', value), 0, timezoneOf(value))",
			localMinimumUnixNanoseconds,
		)
	default:
		return "", errors.New(
			"compile ClickHouse relative_time: invalid snap unit",
		)
	}
	return boundedRelativeTimeTimestampSQL(
		inputSQL,
		body,
		relativeTimeResultNotAfter,
	), nil
}

func compileRelativeTimeSubdaySnapCandidateSQL(
	nanosecondsPerUnit uint64,
	timezoneAligned bool,
) string {
	step := "toInt256(" +
		strconv.FormatUint(nanosecondsPerUnit, 10) + ")"
	alignedTicks := "ticks"
	if timezoneAligned {
		alignedTicks += " + toInt256(timeZoneOffset(value)) * " +
			"toInt256(1000000000)"
	}
	remainder := "modulo(modulo(" + alignedTicks + ", " + step + ") + " + step +
		", " + step + ")"
	target := "ticks - " + remainder
	return "arrayElement(arrayMap(ticks -> " +
		"toTimeZone(fromUnixTimestamp64Nano(" +
		"accurateCastOrNull(" + target + ", 'Int64'), 'UTC'), " +
		"timezoneOf(value)), [" +
		"toInt256(toUnixTimestamp64Nano(value))]), 1)"
}

func compileRelativeTimeWeekSnapSQL(
	inputSQL string,
	weekday uint8,
	localMinimumUnixNanoseconds int64,
) string {
	currentDay := "toInt64(toDaysSinceYearZero(value))"
	daysBack := "modulo(toInt64(modulo(toDayOfWeek(value), 7)) - " +
		strconv.FormatUint(uint64(weekday), 10) + " + 7, 7)"
	targetDay := currentDay + " - " + daysBack
	valid := relativeTimeLocalCivilLowerBoundCondition(
		localMinimumUnixNanoseconds,
	) + " AND " +
		relativeTimeCalendarDayRangeCondition("target_day")
	adjusted := "addDays(dateTrunc('day', value), if(" + valid +
		", target_day - " + currentDay + ", 0))"
	candidate := "arrayElement(arrayMap(target_day -> " +
		"if(isNotNull(value) AND " + valid + ", " + adjusted +
		", NULL), [" + targetDay + "]), 1)"
	return boundedRelativeTimeTimestampSQL(
		inputSQL,
		candidate,
		relativeTimeResultNotAfter,
	)
}

func relativeTimeCalendarDayRangeCondition(valueSQL string) string {
	return valueSQL + " >= toInt64(toDaysSinceYearZero(toDate32('" +
		strconv.Itoa(searchtimebounds.MinimumYear) +
		"-01-01'))) AND " + valueSQL +
		" <= toInt64(toDaysSinceYearZero(toDate32('" +
		strconv.Itoa(searchtimebounds.MaximumYear) + "-01-01')))"
}

func relativeTimeCalendarMonthRangeCondition(valueSQL string) string {
	return valueSQL + " >= " +
		strconv.Itoa(searchtimebounds.MinimumYear*12) + " AND " +
		valueSQL + " <= " +
		strconv.Itoa(searchtimebounds.MaximumYear*12)
}

func relativeTimeTimestampRangeCondition(valueSQL string) string {
	ticks := "toUnixTimestamp64Nano(" + valueSQL + ")"
	return "isNotNull(" + valueSQL + ") AND " + ticks + " >= " +
		strconv.FormatInt(minimumRelativeTimeUnixNanoseconds, 10) +
		" AND " + ticks + " <= " +
		strconv.FormatInt(maximumRelativeTimeUnixNanoseconds, 10)
}

func relativeTimeLocalCivilLowerBoundCondition(
	localMinimumUnixNanoseconds int64,
) string {
	return "toUnixTimestamp64Nano(value) >= " +
		strconv.FormatInt(localMinimumUnixNanoseconds, 10)
}

func relativeTimeLocallyRepresentableCandidateSQL(
	candidateSQL string,
	localMinimumUnixNanoseconds int64,
) string {
	return "if(isNotNull(value) AND " +
		relativeTimeLocalCivilLowerBoundCondition(
			localMinimumUnixNanoseconds,
		) +
		", " + candidateSQL + ", NULL)"
}

func relativeTimeOffsetResultDirection(
	operation splrelativetime.Operation,
) relativeTimeResultDirection {
	if operation.Negative {
		return relativeTimeResultBefore
	}
	return relativeTimeResultAfter
}

func relativeTimeResultDirectionCondition(
	direction relativeTimeResultDirection,
) string {
	resultTicks := "toUnixTimestamp64Nano(result)"
	inputTicks := "toUnixTimestamp64Nano(value)"
	switch direction {
	case relativeTimeResultBefore:
		return resultTicks + " < " + inputTicks
	case relativeTimeResultAfter:
		return resultTicks + " > " + inputTicks
	case relativeTimeResultNotAfter:
		return resultTicks + " <= " + inputTicks
	default:
		return "0"
	}
}

func boundedRelativeTimeTimestampSQL(
	inputSQL string,
	candidateSQL string,
	direction relativeTimeResultDirection,
) string {
	return "arrayElement(arrayMap(value -> " +
		"arrayElement(arrayMap(result -> if(" +
		relativeTimeTimestampRangeCondition("result") + " AND " +
		relativeTimeResultDirectionCondition(direction) +
		", result, NULL), [" + candidateSQL + "]), 1), [" +
		inputSQL + "]), 1)"
}

func compileStrptimeFormatForBackend(
	format string,
	sourceRange spl.Range,
) (spltimeformat.StrptimeFormat, error) {
	compiled, err := spltimeformat.CompileStrptimeFormat(format)
	if err == nil {
		return compiled, nil
	}
	if errors.Is(err, spltimeformat.ErrStrptimeFormatTooLarge) {
		return spltimeformat.StrptimeFormat{}, &plan.Diagnostic{
			Code:    "SPL_QUERY_TOO_COMPLEX",
			Message: "strptime format exceeds its resource limit",
			Range:   sourceRange,
		}
	}
	return spltimeformat.StrptimeFormat{}, &plan.Diagnostic{
		Code: "SPL_UNSUPPORTED_TIME_FORMAT",
		Message: "strptime format is outside the supported deterministic " +
			"full-date parsing subset",
		Range: sourceRange,
	}
}

type compiledStrptimePatterns struct {
	primaryJoda           string
	fallbackJoda          string
	civilRegex            string
	yearGroup             int
	monthGroup            int
	dayGroup              int
	optionalFractionGroup int
}

func compileStrptimePatterns(
	parts []spltimeformat.Part,
) (compiledStrptimePatterns, error) {
	primaryJoda, err := compileStrptimeJodaPattern(parts)
	if err != nil {
		return compiledStrptimePatterns{}, err
	}
	compiled := compiledStrptimePatterns{primaryJoda: primaryJoda}
	optionalFraction := hasOptionalTerminalStrptimeFraction(parts)
	if optionalFraction {
		fallbackParts := slices.Clone(parts[:len(parts)-1])
		lastLiteral := &fallbackParts[len(fallbackParts)-1]
		lastLiteral.Literal = strings.TrimSuffix(lastLiteral.Literal, ".")
		compiled.fallbackJoda, err = compileStrptimeJodaPattern(fallbackParts)
		if err != nil {
			return compiledStrptimePatterns{}, err
		}
	}

	var pattern strings.Builder
	pattern.WriteByte('^')
	groupCount := 0
	appendDateGroup := func(fragment string, target *int) {
		groupCount++
		*target = groupCount
		pattern.WriteByte('(')
		pattern.WriteString(fragment)
		pattern.WriteByte(')')
	}
	for index, part := range parts {
		switch part.Directive {
		case spltimeformat.DirectiveLiteral:
			literal := part.Literal
			if optionalFraction && index == len(parts)-2 {
				literal = strings.TrimSuffix(literal, ".")
			}
			pattern.WriteString(regexp.QuoteMeta(literal))
		case spltimeformat.DirectivePercent:
			pattern.WriteByte('%')
		case spltimeformat.DirectiveYear:
			appendDateGroup(`[0-9]{4}`, &compiled.yearGroup)
		case spltimeformat.DirectiveMonthNumber:
			appendDateGroup(`[0-9]{1,2}`, &compiled.monthGroup)
		case spltimeformat.DirectiveDay:
			appendDateGroup(`[0-9]{1,2}`, &compiled.dayGroup)
		case spltimeformat.DirectiveISODate:
			appendDateGroup(`[0-9]{4}`, &compiled.yearGroup)
			pattern.WriteByte('-')
			appendDateGroup(`[0-9]{1,2}`, &compiled.monthGroup)
			pattern.WriteByte('-')
			appendDateGroup(`[0-9]{1,2}`, &compiled.dayGroup)
		case spltimeformat.DirectiveHour24,
			spltimeformat.DirectiveHour12,
			spltimeformat.DirectiveMinute,
			spltimeformat.DirectiveSecond:
			pattern.WriteString(`[0-9]{1,2}`)
		case spltimeformat.DirectiveAMPM:
			pattern.WriteString(`(?:[Aa][Mm]|[Pp][Mm])`)
		case spltimeformat.DirectiveTime24:
			pattern.WriteString(
				`[0-9]{1,2}:[0-9]{1,2}:[0-9]{1,2}`,
			)
		case spltimeformat.DirectiveTimezoneOffset:
			pattern.WriteString(`[+-][0-9]{4}`)
		case spltimeformat.DirectiveSubseconds:
			isOptional := optionalFraction && index == len(parts)-1
			if isOptional {
				groupCount++
				compiled.optionalFractionGroup = groupCount
			}
			appendStrptimeFractionPattern(
				&pattern,
				part.Width,
				isOptional,
			)
		case spltimeformat.DirectiveMicroseconds:
			appendStrptimeFractionPattern(
				&pattern,
				6,
				optionalFraction && index == len(parts)-1,
			)
		default:
			return compiledStrptimePatterns{}, fmt.Errorf(
				"compile ClickHouse strptime: unsupported directive %d",
				part.Directive,
			)
		}
	}
	pattern.WriteByte('$')
	if compiled.yearGroup == 0 ||
		compiled.monthGroup == 0 ||
		compiled.dayGroup == 0 {
		return compiledStrptimePatterns{}, errors.New(
			"compile ClickHouse strptime: format is missing a complete date",
		)
	}
	compiled.civilRegex = pattern.String()
	return compiled, nil
}

func hasOptionalTerminalStrptimeFraction(parts []spltimeformat.Part) bool {
	if len(parts) < 2 {
		return false
	}
	if parts[len(parts)-1].Directive != spltimeformat.DirectiveSubseconds {
		return false
	}
	literal := parts[len(parts)-2]
	return literal.Directive == spltimeformat.DirectiveLiteral &&
		strings.HasSuffix(literal.Literal, ".")
}

func appendStrptimeFractionPattern(
	pattern *strings.Builder,
	width uint8,
	optional bool,
) {
	if optional {
		pattern.WriteString(`(\.`)
	}
	pattern.WriteString(`[0-9]{1,`)
	pattern.WriteString(strconv.Itoa(int(width)))
	pattern.WriteByte('}')
	if optional {
		pattern.WriteString(`)?`)
	}
}

func compileStrptimeJodaPattern(parts []spltimeformat.Part) (string, error) {
	var pattern strings.Builder
	for _, part := range parts {
		if appendStrftimeJodaPart(&pattern, part) {
			continue
		}
		if part.Directive == spltimeformat.DirectiveTimezoneOffset {
			pattern.WriteByte('Z')
			continue
		}
		return "", fmt.Errorf(
			"compile ClickHouse strptime: unsupported directive %d",
			part.Directive,
		)
	}
	return pattern.String(), nil
}

func compileStrftimeFormatForBackend(
	format string,
	sourceRange spl.Range,
) (spltimeformat.StrftimeFormat, error) {
	compiled, err := spltimeformat.CompileStrftimeFormat(format)
	if err == nil {
		return compiled, nil
	}
	if errors.Is(err, spltimeformat.ErrStrftimeFormatTooLarge) {
		return spltimeformat.StrftimeFormat{}, &plan.Diagnostic{
			Code:    "SPL_QUERY_TOO_COMPLEX",
			Message: "strftime format exceeds its resource limit",
			Range:   sourceRange,
		}
	}
	return spltimeformat.StrftimeFormat{}, &plan.Diagnostic{
		Code: "SPL_UNSUPPORTED_TIME_FORMAT",
		Message: "strftime format is outside the supported locale-stable " +
			"date/time variable subset",
		Range: sourceRange,
	}
}

func chargeUnixTimestampDynamicDecimalBudget(
	input compiledScalar,
	context *compileContext,
	functionName string,
	sourceRange spl.Range,
) error {
	if input.kind != fieldKindDynamic ||
		input.dynamicDomain != dynamicScalarDomainAny {
		return nil
	}
	if context == nil {
		return fmt.Errorf(
			"compile ClickHouse %s: query context is required",
			functionName,
		)
	}
	inputBytes := uint64(MaximumUnixTimestampDynamicDecimalBytes)
	if inputBytes >
		MaximumUnixTimestampQueryDynamicDecimalBytes-
			context.unixTimestampBudget.dynamicDecimalBytes {
		return &plan.Diagnostic{
			Code: "SPL_QUERY_TOO_COMPLEX",
			Message: fmt.Sprintf(
				"search Dynamic decimal timestamp inputs require more than %d bytes of parsing",
				MaximumUnixTimestampQueryDynamicDecimalBytes,
			),
			Range: sourceRange,
		}
	}
	context.unixTimestampBudget.dynamicDecimalBytes += inputBytes
	return nil
}

func unixTimestampScalarSQL(
	input compiledScalar,
	functionName string,
) (string, error) {
	switch input.kind {
	case fieldKindTime:
		return "value", nil
	case fieldKindNumber:
		nanoseconds := ""
		if fixedNumberTypeIsInteger(input.numberType) {
			nanoseconds = "accurateCastOrNull(toInt256(value) * " +
				"toInt256(1000000000), 'Int64')"
		} else {
			nanoseconds = "accurateCastOrNull(floor(toFloat64(value) * " +
				"1000000000), 'Int64')"
		}
		return "fromUnixTimestamp64Nano(" + nanoseconds + ", 'UTC')", nil
	case fieldKindDynamic:
		if input.dynamicDomain == dynamicScalarDomainText {
			return "fromUnixTimestamp64Nano(" +
				"CAST(NULL AS Nullable(Int64)), 'UTC')", nil
		}
		dynamic := compiledScalar{
			valueSQL:       "value",
			dynamicTypeSQL: "dynamicType(value)",
			kind:           fieldKindDynamic,
			dynamicDomain:  input.dynamicDomain,
		}
		typeSQL := dynamicScalarTypeSQL(dynamic)
		integerNanoseconds := "accurateCastOrNull(toInt256(" +
			"accurateCastOrNull(value, 'Int64')) * " +
			"toInt256(1000000000), 'Int64')"
		numeric := finiteDynamicFloatOrNullSQL("value")
		if input.dynamicDomain == dynamicScalarDomainAny {
			taggedDecimal, taggedPayload := dynamicTaggedDecimalText(dynamic)
			payloadLimit := strconv.Itoa(
				MaximumUnixTimestampDynamicDecimalBytes,
			)
			boundedTaggedPayload := "if(length(" + taggedPayload + ") <= " +
				payloadLimit + ", " + taggedPayload +
				", CAST('' AS String))"
			numeric = "multiIf(" +
				dynamicNumericTypePredicate(typeSQL) + ", " +
				numeric + ", " +
				taggedDecimal + ", " +
				finiteFloatOrNullSQL(boundedTaggedPayload) +
				", CAST(NULL AS Nullable(Float64)))"
		}
		floatingNanoseconds := "accurateCastOrNull(floor(ifNotFinite(" + numeric +
			", CAST(NULL AS Nullable(Float64))) * 1000000000), 'Int64')"
		return "fromUnixTimestamp64Nano(if(" +
			dynamicIntegerTypePredicate(typeSQL) + ", " +
			integerNanoseconds + ", " + floatingNanoseconds +
			"), 'UTC')", nil
	default:
		return "", fmt.Errorf(
			"compile ClickHouse %s: unsupported scalar value type",
			functionName,
		)
	}
}

func compileStrftimeParts(
	parts []spltimeformat.Part,
) (string, []any, error) {
	fragments := make([]string, 0, len(parts))
	args := make([]any, 0, len(parts))
	var joda strings.Builder
	flushJoda := func() {
		if joda.Len() == 0 {
			return
		}
		fragments = append(
			fragments,
			"formatDateTimeInJodaSyntax(timestamp, ?)",
		)
		args = append(args, joda.String())
		joda.Reset()
	}
	for _, part := range parts {
		if appendStrftimeJodaPart(&joda, part) {
			continue
		}
		flushJoda()
		switch part.Directive {
		case spltimeformat.DirectiveDaySpace:
			fragments = append(
				fragments,
				"leftPad(toString(toDayOfMonth(timestamp)), 2, ' ')",
			)
		case spltimeformat.DirectiveWeekdayNumber:
			fragments = append(
				fragments,
				"toString(modulo(toDayOfWeek(timestamp), 7))",
			)
		case spltimeformat.DirectiveISOWeekYearShort:
			fragments = append(
				fragments,
				"substring(formatDateTimeInJodaSyntax(timestamp, ?), -2)",
			)
			args = append(args, "xxxx")
		case spltimeformat.DirectiveEpochSeconds:
			fragments = append(
				fragments,
				"arrayElement(arrayMap(nanoseconds -> toString(if(nanoseconds < 0, "+
					"-intDiv(-nanoseconds + 999999999, 1000000000), "+
					"intDiv(nanoseconds, 1000000000))), "+
					"[toUnixTimestamp64Nano(timestamp)]), 1)",
			)
		case spltimeformat.DirectiveTimezoneOffset:
			fragments = append(
				fragments,
				"formatDateTime(timestamp, ?)",
			)
			args = append(args, "%z")
		case spltimeformat.DirectiveTimezoneOffsetColon:
			fragments = append(
				fragments,
				"arrayElement(arrayMap(offset -> concat(substring(offset, 1, 3), "+
					"':', substring(offset, 4, 2)), "+
					"[formatDateTime(timestamp, ?)]), 1)",
			)
			args = append(args, "%z")
		default:
			return "", nil, fmt.Errorf(
				"compile ClickHouse strftime: unsupported directive %d",
				part.Directive,
			)
		}
	}
	flushJoda()
	switch len(fragments) {
	case 0:
		return "CAST('' AS String)", args, nil
	case 1:
		return fragments[0], args, nil
	default:
		return "concat(" + strings.Join(fragments, ", ") + ")", args, nil
	}
}

func appendStrftimeJodaPart(builder *strings.Builder, part spltimeformat.Part) bool {
	switch part.Directive {
	case spltimeformat.DirectiveLiteral:
		appendJodaLiteral(builder, part.Literal)
	case spltimeformat.DirectivePercent:
		builder.WriteByte('%')
	case spltimeformat.DirectiveYear:
		builder.WriteString("yyyy")
	case spltimeformat.DirectiveYearShort:
		builder.WriteString("yy")
	case spltimeformat.DirectiveISOWeekYear:
		builder.WriteString("xxxx")
	case spltimeformat.DirectiveMonthNumber:
		builder.WriteString("MM")
	case spltimeformat.DirectiveMonthShort:
		builder.WriteString("MMM")
	case spltimeformat.DirectiveMonthLong:
		builder.WriteString("MMMM")
	case spltimeformat.DirectiveDay:
		builder.WriteString("dd")
	case spltimeformat.DirectiveDayOfYear:
		builder.WriteString("DDD")
	case spltimeformat.DirectiveISOWeek:
		builder.WriteString("ww")
	case spltimeformat.DirectiveWeekdayShort:
		builder.WriteString("EEE")
	case spltimeformat.DirectiveWeekdayLong:
		builder.WriteString("EEEE")
	case spltimeformat.DirectiveHour24:
		builder.WriteString("HH")
	case spltimeformat.DirectiveHour12:
		builder.WriteString("hh")
	case spltimeformat.DirectiveMinute:
		builder.WriteString("mm")
	case spltimeformat.DirectiveSecond:
		builder.WriteString("ss")
	case spltimeformat.DirectiveAMPM:
		builder.WriteString("a")
	case spltimeformat.DirectiveTime24:
		builder.WriteString("HH:mm:ss")
	case spltimeformat.DirectiveISODate:
		builder.WriteString("yyyy-MM-dd")
	case spltimeformat.DirectiveSubseconds:
		builder.WriteString(strings.Repeat("S", int(part.Width)))
	case spltimeformat.DirectiveMicroseconds:
		builder.WriteString("SSSSSS")
	default:
		return false
	}
	return true
}

func appendJodaLiteral(builder *strings.Builder, literal string) {
	if literal == "" {
		return
	}
	if !strings.ContainsAny(
		literal,
		"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz'",
	) {
		builder.WriteString(literal)
		return
	}
	builder.WriteByte('\'')
	builder.WriteString(strings.ReplaceAll(literal, "'", "''"))
	builder.WriteByte('\'')
}

func compileCoalesceScalar(
	expression *plan.ScalarCallExpression,
	state compileState,
) (compiledScalar, error) {
	if expression == nil {
		return compiledScalar{}, errors.New("compile ClickHouse coalesce: missing expression")
	}
	if len(expression.Arguments) == 0 {
		return compiledScalar{}, errors.New("compile ClickHouse coalesce: requires at least one argument")
	}
	if len(expression.Arguments) > spl.MaximumCoalesceArguments {
		return compiledScalar{}, &plan.Diagnostic{
			Code: "SPL_QUERY_TOO_COMPLEX",
			Message: fmt.Sprintf(
				"coalesce contains more than %d arguments",
				spl.MaximumCoalesceArguments,
			),
			Range: expression.Range,
		}
	}

	values := make([]compiledScalar, 0, len(expression.Arguments))
	alwaysNull := true
	ieeeComparison := false
	materializeForPredicate := false
	sqlBytes := len("coalesce()")
	for _, argument := range expression.Arguments {
		if nilScalarExpression(argument) {
			return compiledScalar{}, errors.New("compile ClickHouse coalesce: missing argument")
		}
		value, err := compileScalarValue(argument, state)
		if err != nil {
			return compiledScalar{}, err
		}
		values = append(values, value)
		alwaysNull = alwaysNull && compiledScalarIsAlwaysNull(value)
		ieeeComparison = ieeeComparison || value.ieeeComparison
		materializeForPredicate = materializeForPredicate || value.materializeForPredicate
		sqlBytes += len(value.valueSQL) + len(", ")
		if err := validateCoalesceScalarSQLBytes(sqlBytes, expression.Range); err != nil {
			return compiledScalar{}, err
		}
	}

	textEligibleSQL, ok := coalesceTextEligibility(values)
	if !ok {
		return compiledScalar{}, &plan.Diagnostic{
			Code: "SPL_UNSUPPORTED_COALESCE_VALUE_TYPE",
			Message: "coalesce values carry incompatible text provenance; use matching text sources " +
				"or normalize each value before coalescing",
			Range: expression.Range,
		}
	}
	semanticBytesSQL, semanticBytesArgs, stringOrBytes, stringOrBytesNullable :=
		coalesceSemanticBytes(values)
	values, kind, numberType, err := normalizeCoalesceValues(values, expression.Range)
	if err != nil {
		return compiledScalar{}, err
	}

	valueSQL := make([]string, 0, len(values))
	args := make([]any, 0)
	for _, value := range values {
		valueSQL = append(valueSQL, value.valueSQL)
		args = append(args, value.valueArgs...)
	}
	sqlBytes = len("coalesce()") + len(strings.Join(valueSQL, ", "))
	if err := validateCoalesceScalarSQLBytes(sqlBytes, expression.Range); err != nil {
		return compiledScalar{}, err
	}
	return compiledScalar{
		valueSQL:                    "coalesce(" + strings.Join(valueSQL, ", ") + ")",
		valueArgs:                   args,
		maxStringBytes:              maximumCompiledScalarStringByteBound(values...),
		existsSQL:                   "1",
		textEligibleSQL:             textEligibleSQL,
		semanticBytesSQL:            semanticBytesSQL,
		semanticBytesArgs:           semanticBytesArgs,
		semanticBytesByUTF8Validity: kind == fieldKindString && semanticBytesValidityOnly(values...),
		textEligibleBySemanticBytes: kind == fieldKindString && stringOrBytes,
		stringOrBytes:               kind == fieldKindString && stringOrBytes,
		stringOrBytesNullable:       kind == fieldKindString && stringOrBytesNullable,
		kind:                        kind,
		numberType:                  numberType,
		alwaysNull:                  alwaysNull,
		ieeeComparison:              ieeeComparison,
		materializeForPredicate:     materializeForPredicate,
	}, nil
}

func validateCoalesceScalarSQLBytes(size int, sourceRange spl.Range) error {
	if size <= maxCompiledCoalesceScalarSQLBytes {
		return nil
	}
	return &plan.Diagnostic{
		Code:    "SPL_QUERY_TOO_COMPLEX",
		Message: fmt.Sprintf("coalesce scalar SQL exceeds %d bytes", maxCompiledCoalesceScalarSQLBytes),
		Range:   sourceRange,
	}
}

func coalesceTextEligibility(values []compiledScalar) (string, bool) {
	textEligibleSQL := ""
	found := false
	for _, value := range values {
		if compiledScalarIsAlwaysNull(value) {
			continue
		}
		if !found {
			textEligibleSQL = value.textEligibleSQL
			found = true
			continue
		}
		if value.textEligibleSQL != textEligibleSQL {
			return "", false
		}
	}
	return textEligibleSQL, true
}

func compiledScalarSemanticBytes(value compiledScalar) (string, []any) {
	if !value.stringOrBytes || compiledScalarIsAlwaysNull(value) ||
		value.semanticBytesSQL == "" {
		return "toUInt8(0)", nil
	}
	return "toUInt8(ifNull(" + value.semanticBytesSQL + ", 0))",
		append([]any(nil), value.semanticBytesArgs...)
}

func semanticBytesTextEligibilitySQL(valueSQL, semanticBytesSQL string) string {
	return "(ifNull(" + semanticBytesSQL + ", 0) = 0 AND isNotNull(" + valueSQL +
		") AND isValidUTF8(assumeNotNull(" + valueSQL + ")))"
}

func coalesceSemanticBytes(values []compiledScalar) (string, []any, bool, bool) {
	parts := make([]string, 0, len(values)*2+1)
	args := make([]any, 0)
	stringOrBytes := false
	nullable := true
	for _, value := range values {
		if compiledScalarIsAlwaysNull(value) {
			continue
		}
		if !value.stringOrBytesNullable {
			nullable = false
		}
		if !value.stringOrBytes {
			continue
		}
		stringOrBytes = true
		flagSQL, flagArgs := compiledScalarSemanticBytes(value)
		parts = append(parts, "isNotNull("+value.valueSQL+")", flagSQL)
		args = append(args, value.valueArgs...)
		args = append(args, flagArgs...)
	}
	if !stringOrBytes {
		return "", nil, false, false
	}
	parts = append(parts, "toUInt8(0)")
	return "multiIf(" + strings.Join(parts, ", ") + ")", args, true, nullable
}

func semanticBytesValidityOnly(values ...compiledScalar) bool {
	found := false
	for _, value := range values {
		if !value.stringOrBytes || compiledScalarIsAlwaysNull(value) {
			continue
		}
		found = true
		if !value.semanticBytesByUTF8Validity {
			return false
		}
	}
	return found
}

func normalizeCoalesceValues(
	values []compiledScalar,
	sourceRange spl.Range,
) ([]compiledScalar, fieldKind, string, error) {
	return normalizeConditionalValues(values, sourceRange, unsupportedCoalesceValueTypes)
}

func normalizeConditionalValues(
	values []compiledScalar,
	sourceRange spl.Range,
	unsupportedValueTypes func(spl.Range, compiledScalar, compiledScalar) error,
) ([]compiledScalar, fieldKind, string, error) {
	target := compiledScalar{}
	found := false
	for _, value := range values {
		if compiledScalarIsAlwaysNull(value) {
			continue
		}
		if !found {
			target = value
			found = true
			continue
		}
		if !coalesceFixedTypesMatch(target, value) {
			return nil, fieldKindInvalid, "",
				unsupportedValueTypes(sourceRange, target, value)
		}
	}
	if !found {
		normalized := append([]compiledScalar(nil), values...)
		target = typedNullIfBranch(fieldKindString, "")
		for index, value := range normalized {
			if coalesceFixedTypesMatch(value, target) {
				continue
			}
			typed := target
			if len(value.valueArgs) > 0 {
				typed.valueSQL = "CAST(" + value.valueSQL +
					" AS Nullable(String))"
				typed.valueArgs = append([]any(nil), value.valueArgs...)
				typed.materializeForPredicate = value.materializeForPredicate
			}
			normalized[index] = typed
		}
		return normalized, fieldKindString, "", nil
	}
	if !supportedCoalesceFixedType(target) {
		return nil, fieldKindInvalid, "",
			unsupportedValueTypes(sourceRange, target, compiledScalar{})
	}

	normalized := append([]compiledScalar(nil), values...)
	for index, value := range normalized {
		if !compiledScalarIsAlwaysNull(value) {
			continue
		}
		if coalesceFixedTypesMatch(value, target) {
			// Keep a typed null-producing expression intact. Preserving it retains
			// source-order bindings and avoids an evaluation-elision contract.
			continue
		}
		typed, ok := typedNullIfBranchFor(target)
		if !ok {
			return nil, fieldKindInvalid, "",
				unsupportedValueTypes(sourceRange, target, value)
		}
		if len(value.valueArgs) > 0 {
			typed.valueSQL = "CAST(" + value.valueSQL + " AS Nullable(" +
				coalesceFixedTypeSQL(target) + "))"
			typed.valueArgs = append([]any(nil), value.valueArgs...)
			typed.materializeForPredicate = value.materializeForPredicate
		}
		normalized[index] = typed
	}
	return normalized, target.kind, target.numberType, nil
}

func coalesceFixedTypeSQL(value compiledScalar) string {
	switch value.kind {
	case fieldKindBool:
		return "Bool"
	case fieldKindNumber:
		return value.numberType
	default:
		return "String"
	}
}

func supportedCoalesceFixedType(value compiledScalar) bool {
	switch value.kind {
	case fieldKindString, fieldKindBool:
		return true
	case fieldKindNumber:
		return value.numberType != "" && supportedIfNumberType(value.numberType)
	default:
		return false
	}
}

func coalesceFixedTypesMatch(left, right compiledScalar) bool {
	if !supportedCoalesceFixedType(left) || !supportedCoalesceFixedType(right) {
		return false
	}
	if left.kind != right.kind {
		return false
	}
	return left.kind != fieldKindNumber || left.numberType == right.numberType
}

func unsupportedCoalesceValueTypes(
	sourceRange spl.Range,
	left, right compiledScalar,
) error {
	leftType := describeIfBranchType(left)
	rightType := describeIfBranchType(right)
	if right.kind == fieldKindInvalid && !compiledScalarIsAlwaysNull(right) {
		rightType = "unsupported"
	}
	return &plan.Diagnostic{
		Code: "SPL_UNSUPPORTED_COALESCE_VALUE_TYPE",
		Message: fmt.Sprintf(
			"coalesce values have unsupported or unstable types %s and %s; use matching fixed String, Bool, or numeric values",
			leftType,
			rightType,
		),
		Range: sourceRange,
	}
}

func compileCaseScalar(
	expression *plan.ScalarCaseExpression,
	state compileState,
) (compiledScalar, error) {
	if expression == nil {
		return compiledScalar{}, errors.New("compile ClickHouse case: missing expression")
	}
	if len(expression.Branches) == 0 {
		return compiledScalar{}, errors.New(
			"compile ClickHouse case: requires at least one condition/value pair",
		)
	}
	if len(expression.Branches) > spl.MaximumCaseBranches {
		return compiledScalar{}, &plan.Diagnostic{
			Code: "SPL_QUERY_TOO_COMPLEX",
			Message: fmt.Sprintf(
				"case contains more than %d condition/value pairs",
				spl.MaximumCaseBranches,
			),
			Range: expression.Range,
		}
	}

	conditionSQL := make([]string, 0, len(expression.Branches))
	conditionArgs := make([][]any, 0, len(expression.Branches))
	values := make([]compiledScalar, 0, len(expression.Branches))
	alwaysNull := true
	ieeeComparison := false
	materializeForPredicate := false
	sqlBytes := len("multiIf()")
	for _, branch := range expression.Branches {
		if nilPlanExpression(branch.Condition) {
			return compiledScalar{}, errors.New("compile ClickHouse case: missing condition")
		}
		if nilScalarExpression(branch.Value) {
			return compiledScalar{}, errors.New("compile ClickHouse case: missing value")
		}
		if err := validateCaseCondition(branch.Condition); err != nil {
			return compiledScalar{}, err
		}
		compiledConditionSQL, compiledConditionArgs, err := compileExpression(
			branch.Condition,
			state,
		)
		if err != nil {
			return compiledScalar{}, err
		}
		compiledValue, err := compileScalarValue(branch.Value, state)
		if err != nil {
			return compiledScalar{}, err
		}
		conditionSQL = append(conditionSQL, compiledConditionSQL)
		conditionArgs = append(conditionArgs, compiledConditionArgs)
		values = append(values, compiledValue)
		alwaysNull = alwaysNull && compiledScalarIsAlwaysNull(compiledValue)
		ieeeComparison = ieeeComparison || compiledValue.ieeeComparison
		materializeForPredicate = materializeForPredicate ||
			len(predicateMaterializationFields(branch.Condition, state)) > 0 ||
			compiledValue.materializeForPredicate
		sqlBytes += len("ifNull(, 0), , ") +
			len(compiledConditionSQL) +
			len(compiledValue.valueSQL)
		if err := validateCaseScalarSQLBytes(sqlBytes, expression.Range); err != nil {
			return compiledScalar{}, err
		}
	}

	textEligibleSQL, ok := coalesceTextEligibility(values)
	if !ok {
		return compiledScalar{}, &plan.Diagnostic{
			Code: "SPL_UNSUPPORTED_CASE_VALUE_TYPE",
			Message: "case values carry incompatible text provenance; use matching text sources " +
				"or normalize each value before the conditional",
			Range: expression.Range,
		}
	}
	semanticParts := make([]string, 0, len(values)*2+1)
	semanticBytesArgs := make([]any, 0)
	stringOrBytes := false
	for index, value := range values {
		flagSQL, flagArgs := compiledScalarSemanticBytes(value)
		semanticParts = append(
			semanticParts,
			"ifNull("+conditionSQL[index]+", 0)",
			flagSQL,
		)
		semanticBytesArgs = append(semanticBytesArgs, conditionArgs[index]...)
		semanticBytesArgs = append(semanticBytesArgs, flagArgs...)
		stringOrBytes = stringOrBytes || value.stringOrBytes
	}
	semanticBytesSQL := ""
	if stringOrBytes {
		semanticParts = append(semanticParts, "toUInt8(0)")
		semanticBytesSQL = "multiIf(" + strings.Join(semanticParts, ", ") + ")"
	}
	values, kind, numberType, err := normalizeCaseValues(values, expression.Range)
	if err != nil {
		return compiledScalar{}, err
	}
	defaultValue := typedNullIfBranch(kind, numberType)

	parts := make([]string, 0, len(values)*2+1)
	args := make([]any, 0)
	for index, value := range values {
		parts = append(
			parts,
			"ifNull("+conditionSQL[index]+", 0)",
			value.valueSQL,
		)
		args = append(args, conditionArgs[index]...)
		args = append(args, value.valueArgs...)
	}
	parts = append(parts, defaultValue.valueSQL)
	valueSQL := "multiIf(" + strings.Join(parts, ", ") + ")"
	if err := validateCaseScalarSQLBytes(len(valueSQL), expression.Range); err != nil {
		return compiledScalar{}, err
	}
	return compiledScalar{
		valueSQL:                    valueSQL,
		valueArgs:                   args,
		maxStringBytes:              maximumCompiledScalarStringByteBound(values...),
		existsSQL:                   "1",
		textEligibleSQL:             textEligibleSQL,
		semanticBytesSQL:            semanticBytesSQL,
		semanticBytesArgs:           semanticBytesArgs,
		semanticBytesByUTF8Validity: kind == fieldKindString && semanticBytesValidityOnly(values...),
		textEligibleBySemanticBytes: kind == fieldKindString && stringOrBytes,
		stringOrBytes:               kind == fieldKindString && stringOrBytes,
		// case has an implicit NULL default even when its selected String
		// branches carry no Bytes provenance. Preserve that physical nullability
		// for a later concatenation that introduces byte capability.
		stringOrBytesNullable:   kind == fieldKindString,
		kind:                    kind,
		numberType:              numberType,
		alwaysNull:              alwaysNull,
		ieeeComparison:          ieeeComparison,
		materializeForPredicate: materializeForPredicate,
	}, nil
}

func validateCaseScalarSQLBytes(size int, sourceRange spl.Range) error {
	if size <= maxCompiledCaseScalarSQLBytes {
		return nil
	}
	return &plan.Diagnostic{
		Code:    "SPL_QUERY_TOO_COMPLEX",
		Message: fmt.Sprintf("case scalar SQL exceeds %d bytes", maxCompiledCaseScalarSQLBytes),
		Range:   sourceRange,
	}
}

func normalizeCaseValues(
	values []compiledScalar,
	sourceRange spl.Range,
) ([]compiledScalar, fieldKind, string, error) {
	return normalizeConditionalValues(values, sourceRange, unsupportedCaseValueTypes)
}

func unsupportedCaseValueTypes(
	sourceRange spl.Range,
	left, right compiledScalar,
) error {
	leftType := describeIfBranchType(left)
	rightType := describeIfBranchType(right)
	if right.kind == fieldKindInvalid && !compiledScalarIsAlwaysNull(right) {
		rightType = "unsupported"
	}
	return &plan.Diagnostic{
		Code: "SPL_UNSUPPORTED_CASE_VALUE_TYPE",
		Message: fmt.Sprintf(
			"case values have unsupported or unstable types %s and %s; use matching fixed String, Bool, or numeric values",
			leftType,
			rightType,
		),
		Range: sourceRange,
	}
}

func compileIfScalar(expression *plan.ScalarIfExpression, state compileState) (compiledScalar, error) {
	if expression == nil {
		return compiledScalar{}, errors.New("compile ClickHouse if: missing if expression")
	}
	if nilPlanExpression(expression.Condition) {
		return compiledScalar{}, errors.New("compile ClickHouse if: missing condition")
	}
	if nilScalarExpression(expression.True) {
		return compiledScalar{}, errors.New("compile ClickHouse if: missing true branch")
	}
	if nilScalarExpression(expression.False) {
		return compiledScalar{}, errors.New("compile ClickHouse if: missing false branch")
	}
	if err := validateIfCondition(expression.Condition); err != nil {
		return compiledScalar{}, err
	}

	conditionSQL, conditionArgs, err := compileExpression(expression.Condition, state)
	if err != nil {
		return compiledScalar{}, err
	}
	if sizeErr := validateIfScalarSQLBytes(len(conditionSQL), expression.Range); sizeErr != nil {
		return compiledScalar{}, sizeErr
	}
	trueValue, err := compileScalarValue(expression.True, state)
	if err != nil {
		return compiledScalar{}, err
	}
	if sizeErr := validateIfScalarSQLBytes(len(trueValue.valueSQL), expression.Range); sizeErr != nil {
		return compiledScalar{}, sizeErr
	}
	falseValue, err := compileScalarValue(expression.False, state)
	if err != nil {
		return compiledScalar{}, err
	}
	if sizeErr := validateIfScalarSQLBytes(len(falseValue.valueSQL), expression.Range); sizeErr != nil {
		return compiledScalar{}, sizeErr
	}
	alwaysNull := compiledScalarIsAlwaysNull(trueValue) &&
		compiledScalarIsAlwaysNull(falseValue)
	textEligibleSQL, ok := ifBranchTextEligibility(trueValue, falseValue)
	if !ok {
		return compiledScalar{}, &plan.Diagnostic{
			Code: "SPL_UNSUPPORTED_IF_BRANCH_TYPE",
			Message: "if branches carry incompatible text provenance; use matching text sources " +
				"or normalize each branch before the conditional",
			Range: expression.Range,
		}
	}
	trueSemanticSQL, trueSemanticArgs := compiledScalarSemanticBytes(trueValue)
	falseSemanticSQL, falseSemanticArgs := compiledScalarSemanticBytes(falseValue)
	stringOrBytes := trueValue.stringOrBytes || falseValue.stringOrBytes
	semanticBytesSQL := ""
	semanticBytesArgs := make([]any, 0)
	if stringOrBytes {
		semanticBytesSQL = "if(ifNull(" + conditionSQL + ", 0), " +
			trueSemanticSQL + ", " + falseSemanticSQL + ")"
		semanticBytesArgs = append(semanticBytesArgs, conditionArgs...)
		semanticBytesArgs = append(semanticBytesArgs, trueSemanticArgs...)
		semanticBytesArgs = append(semanticBytesArgs, falseSemanticArgs...)
	}
	stringOrBytesNullable := compiledScalarIsAlwaysNull(trueValue) ||
		trueValue.stringOrBytesNullable || compiledScalarIsAlwaysNull(falseValue) ||
		falseValue.stringOrBytesNullable
	trueValue, falseValue, kind, numberType, err := normalizeIfBranches(
		trueValue,
		falseValue,
		expression.Range,
	)
	if err != nil {
		return compiledScalar{}, err
	}

	valueSQLBytes := len("if(ifNull(, 0), , )") +
		len(conditionSQL) + len(trueValue.valueSQL) + len(falseValue.valueSQL)
	if err := validateIfScalarSQLBytes(valueSQLBytes, expression.Range); err != nil {
		return compiledScalar{}, err
	}
	args := make([]any, 0, len(conditionArgs)+len(trueValue.valueArgs)+len(falseValue.valueArgs))
	args = append(args, conditionArgs...)
	args = append(args, trueValue.valueArgs...)
	args = append(args, falseValue.valueArgs...)
	return compiledScalar{
		valueSQL: "if(ifNull(" + conditionSQL + ", 0), " +
			trueValue.valueSQL + ", " + falseValue.valueSQL + ")",
		valueArgs: args,
		maxStringBytes: maximumCompiledScalarStringByteBound(
			trueValue,
			falseValue,
		),
		existsSQL:         "1",
		textEligibleSQL:   textEligibleSQL,
		semanticBytesSQL:  semanticBytesSQL,
		semanticBytesArgs: semanticBytesArgs,
		semanticBytesByUTF8Validity: kind == fieldKindString &&
			semanticBytesValidityOnly(trueValue, falseValue),
		textEligibleBySemanticBytes: kind == fieldKindString && stringOrBytes,
		stringOrBytes:               kind == fieldKindString && stringOrBytes,
		stringOrBytesNullable:       kind == fieldKindString && stringOrBytesNullable,
		kind:                        kind,
		numberType:                  numberType,
		alwaysNull:                  alwaysNull,
		ieeeComparison:              trueValue.ieeeComparison || falseValue.ieeeComparison,
		materializeForPredicate: len(predicateMaterializationFields(expression.Condition, state)) > 0 ||
			trueValue.materializeForPredicate ||
			falseValue.materializeForPredicate,
	}, nil
}

func validateIfScalarSQLBytes(size int, sourceRange spl.Range) error {
	if size <= maxCompiledIfScalarSQLBytes {
		return nil
	}
	return &plan.Diagnostic{
		Code:    "SPL_QUERY_TOO_COMPLEX",
		Message: fmt.Sprintf("if scalar SQL exceeds %d bytes", maxCompiledIfScalarSQLBytes),
		Range:   sourceRange,
	}
}

func ifBranchTextEligibility(trueValue, falseValue compiledScalar) (string, bool) {
	switch {
	case compiledScalarIsAlwaysNull(trueValue):
		return falseValue.textEligibleSQL, true
	case compiledScalarIsAlwaysNull(falseValue):
		return trueValue.textEligibleSQL, true
	case trueValue.textEligibleSQL == falseValue.textEligibleSQL:
		return trueValue.textEligibleSQL, true
	default:
		return "", false
	}
}

func validateIfCondition(expression plan.Expression) error {
	validator := predicateComplexityValidator{
		active: make(map[any]struct{}),
	}
	if err := validator.validateExpression(expression, 1); err != nil {
		return err
	}
	return validateIfConditionStructure(expression)
}

func validateIfConditionStructure(expression plan.Expression) error {
	if nilPlanExpression(expression) {
		return errors.New("compile ClickHouse if: missing condition")
	}
	switch expression := expression.(type) {
	case *plan.BooleanExpression:
		if expression.Op != plan.BooleanOpAnd && expression.Op != plan.BooleanOpOr {
			return errors.New("compile ClickHouse if: condition has an invalid Boolean operator")
		}
		if err := validateIfConditionStructure(expression.Left); err != nil {
			return err
		}
		return validateIfConditionStructure(expression.Right)
	case *plan.NotExpression:
		return validateIfConditionStructure(expression.Operand)
	case *plan.EvalComparisonExpression:
		if nilScalarExpression(expression.Left) || nilScalarExpression(expression.Right) {
			return errors.New("compile ClickHouse if: comparison condition has a missing scalar operand")
		}
		if !validComparisonOp(expression.Op) {
			return errors.New("compile ClickHouse if: condition has an invalid comparison operator")
		}
		if err := validatePredicateScalarStructure(expression.Left); err != nil {
			return err
		}
		if err := validatePredicateScalarStructure(expression.Right); err != nil {
			return err
		}
		return nil
	case *plan.MembershipExpression:
		return validateMembershipStructure("if", expression)
	case *plan.ScalarPredicateExpression:
		if nilScalarExpression(expression.Value) {
			return errors.New("compile ClickHouse if: scalar condition is missing")
		}
		if err := validatePredicateScalarStructure(expression.Value); err != nil {
			return err
		}
		if !scalarExpressionReturnsBoolean(expression.Value) {
			return errors.New("compile ClickHouse if: scalar condition must return Boolean")
		}
		return nil
	default:
		return fmt.Errorf(
			"compile ClickHouse if: condition must be an eval/where predicate, got %T",
			expression,
		)
	}
}

func validateCaseCondition(expression plan.Expression) error {
	validator := predicateComplexityValidator{
		active: make(map[any]struct{}),
	}
	if err := validator.validateExpression(expression, 1); err != nil {
		return err
	}
	return validateCaseConditionStructure(expression)
}

func validateCaseConditionStructure(expression plan.Expression) error {
	if nilPlanExpression(expression) {
		return errors.New("compile ClickHouse case: missing condition")
	}
	switch expression := expression.(type) {
	case *plan.BooleanExpression:
		if expression.Op != plan.BooleanOpAnd && expression.Op != plan.BooleanOpOr {
			return errors.New("compile ClickHouse case: condition has an invalid Boolean operator")
		}
		if err := validateCaseConditionStructure(expression.Left); err != nil {
			return err
		}
		return validateCaseConditionStructure(expression.Right)
	case *plan.NotExpression:
		return validateCaseConditionStructure(expression.Operand)
	case *plan.EvalComparisonExpression:
		if nilScalarExpression(expression.Left) || nilScalarExpression(expression.Right) {
			return errors.New(
				"compile ClickHouse case: comparison condition has a missing scalar operand",
			)
		}
		if !validComparisonOp(expression.Op) {
			return errors.New(
				"compile ClickHouse case: condition has an invalid comparison operator",
			)
		}
		if err := validatePredicateScalarStructure(expression.Left); err != nil {
			return err
		}
		return validatePredicateScalarStructure(expression.Right)
	case *plan.MembershipExpression:
		return validateMembershipStructure("case", expression)
	case *plan.ScalarPredicateExpression:
		if nilScalarExpression(expression.Value) {
			return errors.New("compile ClickHouse case: scalar condition is missing")
		}
		if err := validatePredicateScalarStructure(expression.Value); err != nil {
			return err
		}
		if !scalarExpressionReturnsBoolean(expression.Value) {
			return errors.New("compile ClickHouse case: scalar condition must return Boolean")
		}
		return nil
	default:
		return fmt.Errorf(
			"compile ClickHouse case: condition must be an eval/where predicate, got %T",
			expression,
		)
	}
}

type predicateComplexityValidator struct {
	nodes                int
	arithmeticOperators  int
	membershipCandidates int
	active               map[any]struct{}
}

func validateCompiledPredicateComplexity(expression plan.Expression) error {
	validator := predicateComplexityValidator{
		active: make(map[any]struct{}),
	}
	return validator.validateExpression(expression, 1)
}

func validateCompiledScalarComplexity(expression plan.ScalarExpression) error {
	validator := predicateComplexityValidator{
		active: make(map[any]struct{}),
	}
	return validator.validateScalar(expression, 1)
}

func (v *predicateComplexityValidator) validateExpression(
	expression plan.Expression,
	depth int,
) error {
	if nilPlanExpression(expression) {
		return nil
	}
	if err := v.enter(expression, depth, expression.SourceRange()); err != nil {
		return err
	}
	defer v.leave(expression)

	switch expression := expression.(type) {
	case *plan.BooleanExpression:
		if err := v.validateExpression(expression.Left, depth+1); err != nil {
			return err
		}
		return v.validateExpression(expression.Right, depth+1)
	case *plan.NotExpression:
		return v.validateExpression(expression.Operand, depth+1)
	case *plan.EvalComparisonExpression:
		if err := v.validateScalar(expression.Left, depth+1); err != nil {
			return err
		}
		return v.validateScalar(expression.Right, depth+1)
	case *plan.MembershipExpression:
		if len(expression.Candidates) < 1 ||
			len(expression.Candidates) > spl.MaximumMembershipCandidates {
			return predicateComplexityError(
				fmt.Sprintf(
					"membership requires 1 through %d candidates",
					spl.MaximumMembershipCandidates,
				),
				expression.Range,
			)
		}
		if v.membershipCandidates >
			spl.MaximumMembershipCandidatesPerQuery-len(expression.Candidates) {
			return predicateComplexityError(
				fmt.Sprintf(
					"predicate contains more than %d membership candidates",
					spl.MaximumMembershipCandidatesPerQuery,
				),
				expression.Range,
			)
		}
		v.membershipCandidates += len(expression.Candidates)
		if err := v.validateScalar(expression.Value, depth+1); err != nil {
			return err
		}
		for _, candidate := range expression.Candidates {
			if err := v.validateScalar(candidate, depth+1); err != nil {
				return err
			}
		}
		return nil
	case *plan.ScalarPredicateExpression:
		return v.validateScalar(expression.Value, depth+1)
	default:
		return nil
	}
}

func (v *predicateComplexityValidator) validateScalar(
	expression plan.ScalarExpression,
	depth int,
) error {
	if nilScalarExpression(expression) {
		return nil
	}
	if err := v.enter(expression, depth, expression.SourceRange()); err != nil {
		return err
	}
	defer v.leave(expression)

	switch expression := expression.(type) {
	case *plan.ScalarUnaryExpression:
		if !validCompiledScalarUnaryOp(expression.Op) {
			return errors.New("compile ClickHouse predicate: invalid unary arithmetic operator")
		}
		v.arithmeticOperators++
		if v.arithmeticOperators > spl.MaximumArithmeticOperatorsPerQuery {
			return predicateComplexityError(
				fmt.Sprintf(
					"predicate contains more than %d arithmetic operators",
					spl.MaximumArithmeticOperatorsPerQuery,
				),
				expression.Range,
			)
		}
		if compiledUnaryArithmeticChainLength(expression) > spl.MaximumUnaryOperatorChain {
			return predicateComplexityError(
				fmt.Sprintf(
					"unary arithmetic nesting exceeds %d operators",
					spl.MaximumUnaryOperatorChain,
				),
				expression.Range,
			)
		}
		return v.validateScalar(expression.Operand, depth+1)
	case *plan.ScalarBinaryExpression:
		if !validCompiledScalarBinaryOp(expression.Op) {
			return errors.New("compile ClickHouse predicate: invalid binary arithmetic operator")
		}
		v.arithmeticOperators++
		if v.arithmeticOperators > spl.MaximumArithmeticOperatorsPerQuery {
			return predicateComplexityError(
				fmt.Sprintf(
					"predicate contains more than %d arithmetic operators",
					spl.MaximumArithmeticOperatorsPerQuery,
				),
				expression.Range,
			)
		}
		if err := v.validateScalar(expression.Left, depth+1); err != nil {
			return err
		}
		return v.validateScalar(expression.Right, depth+1)
	case *plan.ScalarCallExpression:
		if len(expression.Arguments) > maxCompiledPredicateNodes {
			return predicateComplexityError(
				"predicate scalar call exceeds the structural node budget",
				expression.Range,
			)
		}
		for _, argument := range expression.Arguments {
			if err := v.validateScalar(argument, depth+1); err != nil {
				return err
			}
		}
	case *plan.ScalarIfExpression:
		if err := v.validateExpression(expression.Condition, depth+1); err != nil {
			return err
		}
		if err := v.validateScalar(expression.True, depth+1); err != nil {
			return err
		}
		return v.validateScalar(expression.False, depth+1)
	case *plan.ScalarCaseExpression:
		if len(expression.Branches) > spl.MaximumCaseBranches {
			return predicateComplexityError(
				fmt.Sprintf(
					"case contains more than %d condition/value pairs",
					spl.MaximumCaseBranches,
				),
				expression.Range,
			)
		}
		for _, branch := range expression.Branches {
			if err := v.validateExpression(branch.Condition, depth+1); err != nil {
				return err
			}
			if err := v.validateScalar(branch.Value, depth+1); err != nil {
				return err
			}
		}
	}
	return nil
}

func (v *predicateComplexityValidator) enter(
	node any,
	depth int,
	sourceRange spl.Range,
) error {
	if depth > maxCompiledPredicateDepth {
		return predicateComplexityError(
			fmt.Sprintf("predicate nesting exceeds %d levels", maxCompiledPredicateDepth),
			sourceRange,
		)
	}
	if _, cyclic := v.active[node]; cyclic {
		return predicateComplexityError(
			"predicate expression graph contains a cycle",
			sourceRange,
		)
	}
	// Count occurrences rather than unique pointers. Later compilation walks
	// every occurrence, so memoizing a shared DAG here would let a tiny forged
	// graph expand exponentially after validation.
	v.nodes++
	if v.nodes > maxCompiledPredicateNodes {
		return predicateComplexityError(
			fmt.Sprintf("predicate contains more than %d structural nodes", maxCompiledPredicateNodes),
			sourceRange,
		)
	}
	v.active[node] = struct{}{}
	return nil
}

func (v *predicateComplexityValidator) leave(node any) {
	delete(v.active, node)
}

func predicateComplexityError(message string, sourceRange spl.Range) error {
	return &plan.Diagnostic{
		Code:    "SPL_QUERY_TOO_COMPLEX",
		Message: message,
		Range:   sourceRange,
	}
}

func validComparisonOp(op plan.ComparisonOp) bool {
	switch op {
	case plan.ComparisonOpEqual,
		plan.ComparisonOpNotEqual,
		plan.ComparisonOpLess,
		plan.ComparisonOpLessEqual,
		plan.ComparisonOpGreater,
		plan.ComparisonOpGreaterEqual:
		return true
	default:
		return false
	}
}

func validatePredicateScalarStructure(expression plan.ScalarExpression) error {
	if nilScalarExpression(expression) {
		return errors.New("compile ClickHouse predicate: missing scalar expression")
	}
	switch expression := expression.(type) {
	case *plan.ScalarUnaryExpression:
		if !validCompiledScalarUnaryOp(expression.Op) {
			return errors.New("compile ClickHouse predicate: invalid unary arithmetic operator")
		}
		return validatePredicateScalarStructure(expression.Operand)
	case *plan.ScalarBinaryExpression:
		if !validCompiledScalarBinaryOp(expression.Op) {
			return errors.New("compile ClickHouse predicate: invalid binary arithmetic operator")
		}
		if err := validatePredicateScalarStructure(expression.Left); err != nil {
			return err
		}
		return validatePredicateScalarStructure(expression.Right)
	case *plan.ScalarFieldExpression:
		return validateCanonicalFieldRef("predicate", "scalar", expression.Field)
	case *plan.ScalarLiteralExpression:
		switch expression.Value.Kind {
		case plan.ValueKindNull,
			plan.ValueKindString,
			plan.ValueKindInt64,
			plan.ValueKindUint64,
			plan.ValueKindFloat64,
			plan.ValueKindBool:
			return nil
		default:
			return errors.New("compile ClickHouse predicate: scalar literal has an invalid kind")
		}
	case *plan.ScalarCallExpression:
		expectedArguments := 0
		hasExactArity := false
		switch expression.Function {
		case plan.ScalarFunctionNow:
			hasExactArity = true
		case plan.ScalarFunctionToNumber,
			plan.ScalarFunctionIsNull,
			plan.ScalarFunctionIsNotNull,
			plan.ScalarFunctionLower,
			plan.ScalarFunctionUpper,
			plan.ScalarFunctionLength,
			plan.ScalarFunctionCeil,
			plan.ScalarFunctionFloor,
			plan.ScalarFunctionMVCount,
			plan.ScalarFunctionMVSort:
			expectedArguments = 1
			hasExactArity = true
		case plan.ScalarFunctionToString:
			if len(expression.Arguments) < 1 || len(expression.Arguments) > 2 {
				return errors.New(
					"compile ClickHouse predicate: tostring requires one or two arguments",
				)
			}
			if len(expression.Arguments) == 2 {
				format, ok := scalarQuotedStringLiteral(expression.Arguments[1])
				if !ok || !slices.Contains(spl.SupportedToStringFormats, format) {
					return errors.New(
						"compile ClickHouse predicate: tostring format must be a supported quoted string literal",
					)
				}
			}
		case plan.ScalarFunctionMVDedup:
			expectedArguments = 1
			hasExactArity = true
		case plan.ScalarFunctionSplit, plan.ScalarFunctionMVJoin:
			if len(expression.Arguments) != 2 {
				return fmt.Errorf(
					"compile ClickHouse predicate: scalar function %d requires exactly two arguments",
					expression.Function,
				)
			}
			delimiter, ok := scalarQuotedStringLiteral(expression.Arguments[1])
			if !ok || !utf8.ValidString(delimiter) ||
				len(delimiter) > spl.MaximumMVDelimiterBytes {
				return errors.New(
					"compile ClickHouse predicate: multivalue delimiter must be a bounded quoted UTF-8 string literal",
				)
			}
		case plan.ScalarFunctionMVAppend:
			if len(expression.Arguments) == 0 ||
				len(expression.Arguments) > spl.MaximumMVAppendArguments {
				return fmt.Errorf(
					"compile ClickHouse predicate: mvappend requires one through %d arguments",
					spl.MaximumMVAppendArguments,
				)
			}
		case plan.ScalarFunctionMVIndex:
			if len(expression.Arguments) < 2 || len(expression.Arguments) > 3 {
				return errors.New(
					"compile ClickHouse predicate: mvindex requires two or three arguments",
				)
			}
			for index := 1; index < len(expression.Arguments); index++ {
				if _, ok := signedMVIndexLiteral(expression.Arguments[index]); !ok {
					return errors.New(
						"compile ClickHouse predicate: mvindex indexes must be signed 32-bit integer literals",
					)
				}
			}
		case plan.ScalarFunctionMVZip:
			if len(expression.Arguments) < 2 || len(expression.Arguments) > 3 {
				return errors.New(
					"compile ClickHouse predicate: mvzip requires two or three arguments",
				)
			}
			if len(expression.Arguments) == 3 {
				delimiter, ok := scalarQuotedStringLiteral(expression.Arguments[2])
				if !ok || !utf8.ValidString(delimiter) ||
					len(delimiter) > spl.MaximumMVDelimiterBytes {
					return errors.New(
						"compile ClickHouse predicate: mvzip delimiter must be a bounded quoted UTF-8 string literal",
					)
				}
			}
		case plan.ScalarFunctionMVFind:
			if len(expression.Arguments) != 2 {
				return errors.New(
					"compile ClickHouse predicate: mvfind requires exactly two arguments",
				)
			}
			if _, ok := scalarQuotedStringLiteral(expression.Arguments[1]); !ok {
				return errors.New(
					"compile ClickHouse predicate: mvfind regular expression must be a quoted string literal",
				)
			}
		case plan.ScalarFunctionAbs,
			plan.ScalarFunctionSqrt,
			plan.ScalarFunctionExp,
			plan.ScalarFunctionLn,
			plan.ScalarFunctionURLDecode,
			plan.ScalarFunctionMD5,
			plan.ScalarFunctionSHA1,
			plan.ScalarFunctionSHA256,
			plan.ScalarFunctionSHA512,
			plan.ScalarFunctionTypeOf:
			expectedArguments = 1
			hasExactArity = true
		case plan.ScalarFunctionPow:
			expectedArguments = 2
			hasExactArity = true
		case plan.ScalarFunctionPi:
			hasExactArity = true
		case plan.ScalarFunctionLog:
			if len(expression.Arguments) < 1 || len(expression.Arguments) > 2 {
				return errors.New(
					"compile ClickHouse predicate: log requires one or two arguments",
				)
			}
		case plan.ScalarFunctionTrim,
			plan.ScalarFunctionLTrim,
			plan.ScalarFunctionRTrim:
			if len(expression.Arguments) < 1 || len(expression.Arguments) > 2 {
				return errors.New(
					"compile ClickHouse predicate: trim requires one or two arguments",
				)
			}
			if len(expression.Arguments) == 2 {
				characters, ok := scalarQuotedStringLiteral(expression.Arguments[1])
				if !ok || characters == "" || !utf8.ValidString(characters) ||
					len(characters) > spl.MaximumTrimCharactersBytes {
					return errors.New(
						"compile ClickHouse predicate: trim characters must be a bounded non-empty quoted UTF-8 string literal",
					)
				}
			}
		case plan.ScalarFunctionCIDRMatch:
			if len(expression.Arguments) != 2 {
				return errors.New(
					"compile ClickHouse predicate: cidrmatch requires exactly two arguments",
				)
			}
			prefix, ok := scalarQuotedStringLiteral(expression.Arguments[0])
			if !ok {
				return errors.New(
					"compile ClickHouse predicate: cidrmatch prefix must be a quoted string literal",
				)
			}
			if _, err := netip.ParsePrefix(prefix); err != nil {
				return errors.New(
					"compile ClickHouse predicate: cidrmatch prefix must be a CIDR block",
				)
			}
		case plan.ScalarFunctionMatch:
			if len(expression.Arguments) != 2 {
				return errors.New(
					"compile ClickHouse predicate: match requires exactly two arguments",
				)
			}
			if nilScalarExpression(expression.Arguments[1]) {
				return errors.New(
					"compile ClickHouse predicate: match has a missing regular expression",
				)
			}
			_, ok := scalarQuotedStringLiteral(expression.Arguments[1])
			if !ok {
				return errors.New(
					"compile ClickHouse predicate: match regular expression must be a string literal",
				)
			}
		case plan.ScalarFunctionLike:
			if len(expression.Arguments) != 2 {
				return errors.New(
					"compile ClickHouse predicate: like requires exactly two arguments",
				)
			}
			if nilScalarExpression(expression.Arguments[1]) {
				return errors.New(
					"compile ClickHouse predicate: like has a missing pattern",
				)
			}
			_, ok := scalarQuotedStringLiteral(expression.Arguments[1])
			if !ok {
				return errors.New(
					"compile ClickHouse predicate: like pattern must be a string literal",
				)
			}
		case plan.ScalarFunctionStrftime:
			if len(expression.Arguments) != 2 {
				return errors.New(
					"compile ClickHouse predicate: strftime requires exactly two arguments",
				)
			}
			if nilScalarExpression(expression.Arguments[1]) {
				return errors.New(
					"compile ClickHouse predicate: strftime has a missing format",
				)
			}
			_, ok := scalarQuotedStringLiteral(expression.Arguments[1])
			if !ok {
				return errors.New(
					"compile ClickHouse predicate: strftime format must be a quoted string literal",
				)
			}
		case plan.ScalarFunctionStrptime:
			if len(expression.Arguments) != 2 {
				return errors.New(
					"compile ClickHouse predicate: strptime requires exactly two arguments",
				)
			}
			if nilScalarExpression(expression.Arguments[1]) {
				return errors.New(
					"compile ClickHouse predicate: strptime has a missing format",
				)
			}
			_, ok := scalarQuotedStringLiteral(expression.Arguments[1])
			if !ok {
				return errors.New(
					"compile ClickHouse predicate: strptime format must be a quoted string literal",
				)
			}
		case plan.ScalarFunctionRelativeTime:
			if len(expression.Arguments) != 2 {
				return errors.New(
					"compile ClickHouse predicate: relative_time requires exactly two arguments",
				)
			}
			if nilScalarExpression(expression.Arguments[1]) {
				return errors.New(
					"compile ClickHouse predicate: relative_time has a missing specifier",
				)
			}
			_, ok := scalarQuotedStringLiteral(expression.Arguments[1])
			if !ok {
				return errors.New(
					"compile ClickHouse predicate: relative_time specifier must be a quoted string literal",
				)
			}
		case plan.ScalarFunctionRound:
			if len(expression.Arguments) < 1 || len(expression.Arguments) > 2 {
				return errors.New(
					"compile ClickHouse predicate: round requires one or two arguments",
				)
			}
			if len(expression.Arguments) == 2 {
				if nilScalarExpression(expression.Arguments[1]) {
					return errors.New(
						"compile ClickHouse predicate: round has a missing precision",
					)
				}
				if _, err := roundPrecisionLiteral(
					expression.Arguments[1],
				); err != nil {
					return fmt.Errorf(
						"compile ClickHouse predicate: %w",
						err,
					)
				}
			}
		case plan.ScalarFunctionReplace:
			expectedArguments = 3
			hasExactArity = true
		case plan.ScalarFunctionSubstring:
			if len(expression.Arguments) < 2 || len(expression.Arguments) > 3 {
				return errors.New(
					"compile ClickHouse predicate: substr requires two or three arguments",
				)
			}
			for index := 1; index < len(expression.Arguments); index++ {
				if nilScalarExpression(expression.Arguments[index]) {
					return errors.New(
						"compile ClickHouse predicate: substr has a missing index",
					)
				}
				if _, ok := scalarIntegerLiteral(expression.Arguments[index]); !ok {
					return errors.New(
						"compile ClickHouse predicate: substr indexes must be literal integers",
					)
				}
			}
		case plan.ScalarFunctionCoalesce:
			if len(expression.Arguments) == 0 {
				return errors.New(
					"compile ClickHouse predicate: coalesce requires at least one argument",
				)
			}
			if len(expression.Arguments) > spl.MaximumCoalesceArguments {
				return fmt.Errorf(
					"compile ClickHouse predicate: coalesce contains more than %d arguments",
					spl.MaximumCoalesceArguments,
				)
			}
		case plan.ScalarFunctionConcat:
			if len(expression.Arguments) < 2 {
				return errors.New(
					"compile ClickHouse predicate: concatenation requires at least two operands",
				)
			}
			if len(expression.Arguments) > spl.MaximumConcatenationOperands {
				return fmt.Errorf(
					"compile ClickHouse predicate: concatenation contains more than %d operands",
					spl.MaximumConcatenationOperands,
				)
			}
		default:
			return fmt.Errorf(
				"compile ClickHouse predicate: unsupported scalar function %d",
				expression.Function,
			)
		}
		if hasExactArity && len(expression.Arguments) != expectedArguments {
			if expectedArguments == 0 {
				return errors.New(
					"compile ClickHouse predicate: now requires no arguments",
				)
			}
			return fmt.Errorf(
				"compile ClickHouse predicate: scalar function %d requires %d arguments",
				expression.Function,
				expectedArguments,
			)
		}
		for _, argument := range expression.Arguments {
			if err := validatePredicateScalarStructure(argument); err != nil {
				return err
			}
		}
		return nil
	case *plan.ScalarIfExpression:
		if err := validateIfConditionStructure(expression.Condition); err != nil {
			return err
		}
		if err := validatePredicateScalarStructure(expression.True); err != nil {
			return err
		}
		return validatePredicateScalarStructure(expression.False)
	case *plan.ScalarCaseExpression:
		if len(expression.Branches) == 0 {
			return errors.New(
				"compile ClickHouse predicate: case requires at least one condition/value pair",
			)
		}
		if len(expression.Branches) > spl.MaximumCaseBranches {
			return fmt.Errorf(
				"compile ClickHouse predicate: case contains more than %d condition/value pairs",
				spl.MaximumCaseBranches,
			)
		}
		for _, branch := range expression.Branches {
			if err := validateCaseConditionStructure(branch.Condition); err != nil {
				return err
			}
			if err := validatePredicateScalarStructure(branch.Value); err != nil {
				return err
			}
		}
		return nil
	default:
		return fmt.Errorf(
			"compile ClickHouse predicate: unsupported scalar expression %T",
			expression,
		)
	}
}

func scalarExpressionReturnsBoolean(expression plan.ScalarExpression) bool {
	switch expression := expression.(type) {
	case *plan.ScalarUnaryExpression, *plan.ScalarBinaryExpression:
		return false
	case *plan.ScalarCallExpression:
		if expression == nil {
			return false
		}
		if expression.Function.ReturnsBoolean() {
			return true
		}
		if expression.Function == plan.ScalarFunctionCoalesce {
			return coalescePlanScalarReturnsBoolean(expression.Arguments)
		}
		return false
	case *plan.ScalarLiteralExpression:
		return expression != nil && expression.Value.Kind == plan.ValueKindBool
	case *plan.ScalarIfExpression:
		return expression != nil &&
			scalarExpressionReturnsBoolean(expression.True) &&
			scalarExpressionReturnsBoolean(expression.False)
	case *plan.ScalarCaseExpression:
		return expression != nil &&
			casePlanScalarReturnsBoolean(expression.Branches)
	default:
		return false
	}
}

func coalescePlanScalarReturnsBoolean(arguments []plan.ScalarExpression) bool {
	foundBoolean := false
	for _, argument := range arguments {
		if literal, ok := argument.(*plan.ScalarLiteralExpression); ok &&
			literal != nil &&
			literal.Value.Kind == plan.ValueKindNull {
			continue
		}
		if !scalarExpressionReturnsBoolean(argument) {
			return false
		}
		foundBoolean = true
	}
	return foundBoolean
}

func casePlanScalarReturnsBoolean(branches []plan.ScalarCaseBranch) bool {
	foundBoolean := false
	for _, branch := range branches {
		if literal, ok := branch.Value.(*plan.ScalarLiteralExpression); ok &&
			literal != nil &&
			literal.Value.Kind == plan.ValueKindNull {
			continue
		}
		if !scalarExpressionReturnsBoolean(branch.Value) {
			return false
		}
		foundBoolean = true
	}
	return foundBoolean
}

func nilPlanExpression(expression plan.Expression) bool {
	if expression == nil {
		return true
	}
	switch expression := expression.(type) {
	case *plan.BooleanExpression:
		return expression == nil
	case *plan.NotExpression:
		return expression == nil
	case *plan.TextExpression:
		return expression == nil
	case *plan.ComparisonExpression:
		return expression == nil
	case *plan.EvalComparisonExpression:
		return expression == nil
	case *plan.MembershipExpression:
		return expression == nil
	case *plan.ScalarPredicateExpression:
		return expression == nil
	default:
		return false
	}
}

func nilScalarExpression(expression plan.ScalarExpression) bool {
	if expression == nil {
		return true
	}
	switch expression := expression.(type) {
	case *plan.ScalarUnaryExpression:
		return expression == nil
	case *plan.ScalarBinaryExpression:
		return expression == nil
	case *plan.ScalarFieldExpression:
		return expression == nil
	case *plan.ScalarLiteralExpression:
		return expression == nil
	case *plan.ScalarCallExpression:
		return expression == nil
	case *plan.ScalarIfExpression:
		return expression == nil
	case *plan.ScalarCaseExpression:
		return expression == nil
	default:
		return false
	}
}

func normalizeIfBranches(
	trueValue, falseValue compiledScalar,
	sourceRange spl.Range,
) (compiledScalar, compiledScalar, fieldKind, string, error) {
	trueNull := compiledScalarIsAlwaysNull(trueValue)
	falseNull := compiledScalarIsAlwaysNull(falseValue)
	if trueNull && falseNull {
		trueValue = typedNullIfBranch(fieldKindString, "")
		falseValue = typedNullIfBranch(fieldKindString, "")
		return trueValue, falseValue, fieldKindString, "", nil
	}
	if trueNull {
		normalized, ok := typedNullIfBranchFor(falseValue)
		if !ok {
			return compiledScalar{}, compiledScalar{}, fieldKindInvalid, "",
				unsupportedIfBranchTypes(sourceRange, trueValue, falseValue)
		}
		trueValue = normalized
	}
	if falseNull {
		normalized, ok := typedNullIfBranchFor(trueValue)
		if !ok {
			return compiledScalar{}, compiledScalar{}, fieldKindInvalid, "",
				unsupportedIfBranchTypes(sourceRange, trueValue, falseValue)
		}
		falseValue = normalized
	}

	switch {
	case trueValue.kind == fieldKindString && falseValue.kind == fieldKindString:
		return trueValue, falseValue, fieldKindString, "", nil
	case trueValue.kind == fieldKindBool && falseValue.kind == fieldKindBool:
		return trueValue, falseValue, fieldKindBool, "", nil
	case trueValue.kind == fieldKindNumber &&
		falseValue.kind == fieldKindNumber &&
		trueValue.numberType != "" &&
		trueValue.numberType == falseValue.numberType &&
		supportedIfNumberType(trueValue.numberType):
		return trueValue, falseValue, fieldKindNumber, trueValue.numberType, nil
	default:
		return compiledScalar{}, compiledScalar{}, fieldKindInvalid, "",
			unsupportedIfBranchTypes(sourceRange, trueValue, falseValue)
	}
}

func compiledScalarIsAlwaysNull(value compiledScalar) bool {
	return value.alwaysNull ||
		(value.literal != nil && value.literal.Kind == plan.ValueKindNull)
}

func typedNullIfBranchFor(value compiledScalar) (compiledScalar, bool) {
	switch value.kind {
	case fieldKindString, fieldKindBool:
		return typedNullIfBranch(value.kind, ""), true
	case fieldKindNumber:
		if value.numberType == "" || !supportedIfNumberType(value.numberType) {
			return compiledScalar{}, false
		}
		return typedNullIfBranch(value.kind, value.numberType), true
	default:
		return compiledScalar{}, false
	}
}

func typedNullIfBranch(kind fieldKind, numberType string) compiledScalar {
	typeSQL := "String"
	switch kind {
	case fieldKindBool:
		typeSQL = "Bool"
	case fieldKindNumber:
		typeSQL = numberType
	}
	return compiledScalar{
		valueSQL:       "CAST(NULL AS Nullable(" + typeSQL + "))",
		maxStringBytes: 1,
		existsSQL:      "1",
		kind:           kind,
		numberType:     numberType,
		alwaysNull:     true,
	}
}

func supportedIfNumberType(numberType string) bool {
	switch numberType {
	case "UInt8", "Int64", "UInt64", "Float64":
		return true
	default:
		return false
	}
}

func unsupportedIfBranchTypes(
	sourceRange spl.Range,
	trueValue, falseValue compiledScalar,
) error {
	return &plan.Diagnostic{
		Code: "SPL_UNSUPPORTED_IF_BRANCH_TYPE",
		Message: fmt.Sprintf(
			"if branches have unsupported or unstable types %s and %s; use matching fixed String, Bool, or numeric branches",
			describeIfBranchType(trueValue),
			describeIfBranchType(falseValue),
		),
		Range: sourceRange,
	}
}

func describeIfBranchType(value compiledScalar) string {
	if compiledScalarIsAlwaysNull(value) {
		return "Null"
	}
	switch value.kind {
	case fieldKindDynamic:
		return "Dynamic"
	case fieldKindString:
		return "String"
	case fieldKindNumber:
		if value.numberType != "" {
			return value.numberType
		}
		return "Number"
	case fieldKindBool:
		return "Bool"
	case fieldKindTime:
		return "Time"
	case fieldKindStringArray:
		return "StringArray"
	case fieldKindDynamicArray:
		return "DynamicArray"
	default:
		return "Invalid"
	}
}

func compileNullTestScalar(expression *plan.ScalarCallExpression, state compileState) (compiledScalar, error) {
	if len(expression.Arguments) != 1 {
		return compiledScalar{}, errors.New("compile ClickHouse null predicate: expected one argument")
	}
	input, err := compileScalarValue(expression.Arguments[0], state)
	if err != nil {
		return compiledScalar{}, err
	}

	presenceSQL, presenceArgs := compiledScalarPresenceSQL(input)
	valueSQL := "CAST(ifNull(" + presenceSQL + ", 0) AS Bool)"
	if expression.Function == plan.ScalarFunctionIsNull {
		valueSQL = "CAST(NOT ifNull(" + presenceSQL + ", 0) AS Bool)"
	}
	return compiledScalar{
		valueSQL:                valueSQL,
		valueArgs:               presenceArgs,
		maxStringBytes:          5,
		existsSQL:               "1",
		kind:                    fieldKindBool,
		materializeForPredicate: input.materializeForPredicate,
	}, nil
}

// compiledScalarPresenceSQL implements SPL's distinction between a missing or
// null value and every present, non-null scalar. Dynamic object parents can be
// represented only by their flattened descendants, so their bounded metadata
// probe also establishes presence. Keep every argument with valueSQL because
// null predicates are complete scalar values whose existsSQL is the constant 1.
func compiledScalarPresenceSQL(value compiledScalar) (string, []any) {
	existsSQL := value.existsSQL
	if existsSQL == "" {
		existsSQL = "1"
	}
	if isNativeMultivalueKind(value.kind) {
		// Native eval/spath/nomv values carry an authoritative list-presence
		// sidecar independent of physical cardinality. It is false for both a
		// missing field and an explicit null, and true for every list, including
		// a present-empty list. The sidecar reuses existsArgs by contract.
		if value.optionalMultivaluePresentSQL != "" {
			presentSQL := value.optionalMultivaluePresentSQL
			presentArgs := append([]any(nil), value.existsArgs...)
			if value.requiresRuntimeValidation {
				// A wrapper such as isnull() consumes only logical presence. Force
				// the guarded list expression through ignore() as well, otherwise
				// ClickHouse could prune unsupported-member or resource checks whose
				// output cardinality does not affect the sidecar.
				presentSQL = bindSQLExpressions(
					[]string{"validated_mv", "mv_present"},
					[]string{value.valueSQL, presentSQL},
					"toUInt8(ifNull(mv_present, 0)) + ignore(validated_mv) != 0",
				)
				presentArgs = append(
					append([]any(nil), value.valueArgs...),
					presentArgs...,
				)
			}
			return presentSQL, presentArgs
		}
		// Fixed multivalue results are physically non-null Array(String), but
		// their canonical empty representation is logically absent in SPL.
		// Calculated arrays without a separate existence predicate must test
		// their members instead of treating isNotNull([]) as presence. Projected
		// arrays already carry a notEmpty(alias) existence predicate and retain
		// the ordinary physical-null check below.
		if existsSQL == "0" {
			return "0", nil
		}
		if existsSQL == "1" {
			return "notEmpty(" + value.valueSQL + ")",
				append([]any(nil), value.valueArgs...)
		}
	}
	presenceSQL := "((" + existsSQL + ") AND isNotNull(" + value.valueSQL + "))"
	args := make([]any, 0, len(value.existsArgs)+len(value.valueArgs)+len(value.descendantArgs))
	args = append(args, value.existsArgs...)
	args = append(args, value.valueArgs...)
	if value.descendantSQL != "" {
		presenceSQL = "(" + presenceSQL + " OR (" + value.descendantSQL + "))"
		args = append(args, value.descendantArgs...)
	}
	return presenceSQL, args
}

// logicalFieldPresenceSQL is the shared non-null SPL presence contract for a
// resolved field. In particular, native multivalue fields must consult their
// sealed list-presence sidecar rather than physical Array nullability or
// cardinality so missing, explicit null, and present-empty remain distinct.
func logicalFieldPresenceSQL(field fieldState) (string, []any) {
	return compiledScalarPresenceSQL(compiledScalarFromField(field))
}

func compileReplaceScalar(expression *plan.ScalarCallExpression, state compileState) (compiledScalar, error) {
	if len(expression.Arguments) != 3 {
		return compiledScalar{}, errors.New("compile ClickHouse replace: expected three arguments")
	}
	if scalarExpressionMayReturnBooleanFunction(expression.Arguments[0]) {
		return compiledScalar{}, booleanScalarConsumerError("replace")
	}
	input, err := compileScalarValue(expression.Arguments[0], state)
	if err != nil {
		return compiledScalar{}, err
	}
	if isNativeMultivalueKind(input.kind) {
		return compiledScalar{}, unsupportedMultivalueUsage("replace", expression.Range)
	}
	pattern, ok := scalarStringLiteral(expression.Arguments[1])
	if !ok {
		return compiledScalar{}, errors.New("compile ClickHouse replace: regular expression must be a string literal")
	}
	if pattern == "" {
		return compiledScalar{}, errors.New("compile ClickHouse replace: empty regular expressions are not supported")
	}
	if err := splregex.ValidateReplacePattern(pattern); err != nil {
		return compiledScalar{}, fmt.Errorf("compile ClickHouse replace: regular expression is outside the supported RE2 subset: %w", err)
	}
	replacement, ok := scalarStringLiteral(expression.Arguments[2])
	if !ok {
		return compiledScalar{}, errors.New("compile ClickHouse replace: replacement must be a string literal")
	}
	inputSQL, inputArgs := compiledStringScalar(input)
	replacementFactor := uint64(len(replacement)) + 1
	return compiledScalar{
		valueSQL:                    "replaceRegexpAll(" + inputSQL + ", ?, ?)",
		valueArgs:                   append(inputArgs, pattern, replacement),
		maxStringBytes:              saturatingStringByteProduct(compiledScalarStringByteBound(input), replacementFactor),
		existsSQL:                   "1",
		textEligibleSQL:             input.textEligibleSQL,
		semanticBytesSQL:            input.semanticBytesSQL,
		semanticBytesArgs:           append([]any(nil), input.semanticBytesArgs...),
		semanticBytesByUTF8Validity: input.semanticBytesByUTF8Validity,
		textEligibleBySemanticBytes: input.textEligibleBySemanticBytes,
		stringOrBytes:               input.stringOrBytes,
		stringOrBytesNullable:       input.stringOrBytesNullable,
		kind:                        fieldKindString,
		materializeForPredicate:     input.materializeForPredicate,
	}, nil
}

func compileBinaryTextPredicateOperands(
	expression *plan.ScalarCallExpression,
	state compileState,
	functionName string,
	patternDescription string,
) (compiledScalar, string, error) {
	if len(expression.Arguments) != 2 {
		return compiledScalar{}, "", fmt.Errorf(
			"compile ClickHouse %s: expected two arguments",
			functionName,
		)
	}
	if scalarExpressionMayReturnBooleanFunction(expression.Arguments[0]) {
		return compiledScalar{}, "", booleanScalarConsumerError(functionName)
	}
	input, err := compileScalarValue(expression.Arguments[0], state)
	if err != nil {
		return compiledScalar{}, "", err
	}
	if isNativeMultivalueKind(input.kind) {
		return compiledScalar{}, "", unsupportedMultivalueUsage(
			functionName,
			expression.Range,
		)
	}
	pattern, ok := scalarQuotedStringLiteral(expression.Arguments[1])
	if !ok {
		return compiledScalar{}, "", fmt.Errorf(
			"compile ClickHouse %s: %s must be a string literal",
			functionName,
			patternDescription,
		)
	}
	return input, pattern, nil
}

func compileBoundedTextPredicateResult(
	input compiledScalar,
	pattern string,
	functionName string,
	maximumInputBytes uint64,
	maximumSQLBytes int,
	sourceRange spl.Range,
) (compiledScalar, error) {
	if input.alwaysNull || input.kind == fieldKindInvalid {
		return compiledScalar{
			valueSQL:       "CAST(NULL AS Nullable(Bool))",
			maxStringBytes: 1,
			existsSQL:      "1",
			kind:           fieldKindBool,
			alwaysNull:     true,
		}, nil
	}
	if compiledScalarStringByteBound(input) > maximumInputBytes {
		return compiledScalar{}, &plan.Diagnostic{
			Code: "SPL_QUERY_TOO_COMPLEX",
			Message: fmt.Sprintf(
				"%s input may exceed %d bytes after scalar evaluation",
				functionName,
				maximumInputBytes,
			),
			Range: sourceRange,
		}
	}
	inputSQL, inputArgs := compiledTextEligibleStringScalar(input)
	valueSQL := "CAST(" + functionName + "(" + inputSQL + ", ?) AS Nullable(Bool))"
	if len(valueSQL) > maximumSQLBytes {
		return compiledScalar{}, &plan.Diagnostic{
			Code: "SPL_QUERY_TOO_COMPLEX",
			Message: fmt.Sprintf(
				"%s scalar SQL exceeds %d bytes",
				functionName,
				maximumSQLBytes,
			),
			Range: sourceRange,
		}
	}
	return compiledScalar{
		valueSQL:                valueSQL,
		valueArgs:               append(inputArgs, pattern),
		maxStringBytes:          5,
		existsSQL:               "1",
		kind:                    fieldKindBool,
		materializeForPredicate: input.materializeForPredicate,
	}, nil
}

func compileMatchScalar(
	expression *plan.ScalarCallExpression,
	state compileState,
) (compiledScalar, error) {
	input, pattern, err := compileBinaryTextPredicateOperands(
		expression,
		state,
		"match",
		"regular expression",
	)
	if err != nil {
		return compiledScalar{}, err
	}
	compiledPattern := splregex.MatchPattern{}
	if state.context != nil {
		compiledPattern = state.context.patternBudgets.match.patterns[expression]
	}
	if compiledPattern.ProgramWorkUnits == 0 {
		compiledPattern, err = compileMatchPatternForBackend(pattern, expression.Range)
		if err != nil {
			return compiledScalar{}, err
		}
		if state.context != nil {
			state.context.patternBudgets.match.patterns[expression] = compiledPattern
		}
	}
	if state.context != nil {
		if compiledPattern.ProgramWorkUnits >
			splregex.MaximumMatchQueryProgramWorkUnits-state.context.patternBudgets.match.programWorkUnits {
			return compiledScalar{}, &plan.Diagnostic{
				Code: "SPL_QUERY_TOO_COMPLEX",
				Message: fmt.Sprintf(
					"search match programs require more than %d work units",
					splregex.MaximumMatchQueryProgramWorkUnits,
				),
				Range: expression.Range,
			}
		}
		state.context.patternBudgets.match.programWorkUnits += compiledPattern.ProgramWorkUnits
	}
	return compileBoundedTextPredicateResult(
		input,
		compiledPattern.Pattern,
		"match",
		MaximumMatchInputBytes,
		maxCompiledMatchScalarSQLBytes,
		expression.Range,
	)
}

func compileLikeScalar(
	expression *plan.ScalarCallExpression,
	state compileState,
) (compiledScalar, error) {
	input, pattern, err := compileBinaryTextPredicateOperands(
		expression,
		state,
		"like",
		"pattern",
	)
	if err != nil {
		return compiledScalar{}, err
	}
	compiledPattern := splwildcard.LikePattern{}
	if state.context != nil {
		compiledPattern = state.context.patternBudgets.like.patterns[expression]
	}
	if compiledPattern.WorkUnits == 0 {
		compiledPattern, err = compileLikePatternForBackend(pattern, expression.Range)
		if err != nil {
			return compiledScalar{}, err
		}
		if state.context != nil {
			state.context.patternBudgets.like.patterns[expression] = compiledPattern
		}
	}
	if state.context != nil {
		if compiledPattern.WorkUnits >
			splwildcard.MaximumLikeQueryPatternWorkUnits-state.context.patternBudgets.like.workUnits {
			return compiledScalar{}, &plan.Diagnostic{
				Code: "SPL_QUERY_TOO_COMPLEX",
				Message: fmt.Sprintf(
					"search like patterns require more than %d work units",
					splwildcard.MaximumLikeQueryPatternWorkUnits,
				),
				Range: expression.Range,
			}
		}
		state.context.patternBudgets.like.workUnits += compiledPattern.WorkUnits
		if !input.alwaysNull && input.kind != fieldKindInvalid {
			inputBytes := compiledScalarStringByteBound(input)
			if inputBytes >
				MaximumLikeQueryInputBytes-state.context.patternBudgets.like.inputBytes {
				return compiledScalar{}, &plan.Diagnostic{
					Code: "SPL_QUERY_TOO_COMPLEX",
					Message: fmt.Sprintf(
						"search like inputs require more than %d bytes of wildcard scanning per row",
						MaximumLikeQueryInputBytes,
					),
					Range: expression.Range,
				}
			}
			state.context.patternBudgets.like.inputBytes += inputBytes
		}
	}
	return compileBoundedTextPredicateResult(
		input,
		compiledPattern.Pattern,
		"like",
		MaximumLikeInputBytes,
		maxCompiledLikeScalarSQLBytes,
		expression.Range,
	)
}

func compileTextLengthScalar(
	expression *plan.ScalarCallExpression,
	state compileState,
) (compiledScalar, error) {
	input, err := compileUnaryNonBooleanScalarInput(expression, state, "len")
	if err != nil {
		return compiledScalar{}, err
	}

	valueSQL := ""
	valueArgs := append([]any(nil), input.valueArgs...)
	switch input.kind {
	case fieldKindDynamic:
		// dynamicElement returns Nullable(String), with null for every other
		// runtime type. It therefore preserves len's scalar-only boundary while
		// referencing the open event field exactly once.
		valueSQL = "lengthUTF8(dynamicElement(" + input.valueSQL + ", 'String'))"
	case fieldKindStringArray, fieldKindDynamicArray:
		return compiledScalar{}, unsupportedMultivalueUsage("len", expression.Range)
	case fieldKindString, fieldKindInvalid:
		inputSQL, inputArgs := compiledTextEligibleStringScalar(input)
		valueArgs = inputArgs
		valueSQL = "lengthUTF8(" + inputSQL + ")"
	default:
		return compiledScalar{}, &plan.Diagnostic{
			Code:    "SPL_UNSUPPORTED_TEXT_LENGTH_VALUE_TYPE",
			Message: "len requires a String input",
			Range:   expression.Range,
		}
	}
	if len(valueSQL) > maxCompiledTextLengthScalarSQLBytes {
		return compiledScalar{}, &plan.Diagnostic{
			Code: "SPL_QUERY_TOO_COMPLEX",
			Message: fmt.Sprintf(
				"len scalar SQL exceeds %d bytes",
				maxCompiledTextLengthScalarSQLBytes,
			),
			Range: expression.Range,
		}
	}
	return compiledScalar{
		valueSQL:                valueSQL,
		valueArgs:               valueArgs,
		existsSQL:               "1",
		kind:                    fieldKindNumber,
		numberType:              "UInt64",
		alwaysNull:              input.alwaysNull,
		materializeForPredicate: input.materializeForPredicate,
	}, nil
}

func compileSubstringScalar(
	expression *plan.ScalarCallExpression,
	state compileState,
) (compiledScalar, error) {
	if expression == nil {
		return compiledScalar{}, errors.New(
			"compile ClickHouse substr: missing expression",
		)
	}
	if len(expression.Arguments) < 2 || len(expression.Arguments) > 3 {
		return compiledScalar{}, errors.New(
			"compile ClickHouse substr: expected two or three arguments",
		)
	}
	input, err := compileNonBooleanScalarInputArgument(
		expression.Arguments[0],
		state,
		"substr",
	)
	if err != nil {
		return compiledScalar{}, err
	}
	inputSQL := ""
	inputArgs := append([]any(nil), input.valueArgs...)
	switch input.kind {
	case fieldKindDynamic:
		// dynamicElement returns Nullable(String), so unsupported runtime
		// numbers, Booleans, arrays, and objects fail closed without generic
		// Dynamic conversion branches.
		inputSQL = "dynamicElement(" + input.valueSQL + ", 'String')"
	case fieldKindStringArray, fieldKindDynamicArray:
		return compiledScalar{}, unsupportedMultivalueUsage(
			"substr",
			expression.Range,
		)
	case fieldKindString, fieldKindInvalid:
		inputSQL, inputArgs = compiledTextEligibleStringScalar(input)
	default:
		return compiledScalar{}, &plan.Diagnostic{
			Code:    "SPL_UNSUPPORTED_SUBSTRING_VALUE_TYPE",
			Message: "substr requires a String input",
			Range:   expression.Range,
		}
	}

	start, err := compileSubstringIntegerLiteral(
		expression.Arguments[1],
	)
	if err != nil {
		return compiledScalar{}, err
	}
	var length *plan.Value
	if len(expression.Arguments) == 3 {
		compiledLength, lengthErr := compileSubstringIntegerLiteral(
			expression.Arguments[2],
		)
		if lengthErr != nil {
			return compiledScalar{}, lengthErr
		}
		length = &compiledLength
	}

	valueSQL, indexArgs := compileSQLiteSubstringUTF8SQL(
		inputSQL,
		start,
		length,
	)
	valueArgs := append(inputArgs, indexArgs...)
	if len(valueSQL) > maxCompiledSubstringScalarSQLBytes {
		return compiledScalar{}, &plan.Diagnostic{
			Code: "SPL_QUERY_TOO_COMPLEX",
			Message: fmt.Sprintf(
				"substr scalar SQL exceeds %d bytes",
				maxCompiledSubstringScalarSQLBytes,
			),
			Range: expression.Range,
		}
	}
	return compiledScalar{
		valueSQL:                valueSQL,
		valueArgs:               valueArgs,
		maxStringBytes:          compiledScalarStringByteBound(input),
		existsSQL:               "1",
		kind:                    fieldKindString,
		alwaysNull:              input.alwaysNull,
		materializeForPredicate: input.materializeForPredicate,
	}, nil
}

func compileSubstringIntegerLiteral(
	expression plan.ScalarExpression,
) (plan.Value, error) {
	if nilScalarExpression(expression) {
		return plan.Value{}, errors.New(
			"compile ClickHouse substr: missing index",
		)
	}
	value, ok := scalarIntegerLiteral(expression)
	if !ok {
		return plan.Value{}, errors.New(
			"compile ClickHouse substr: index must be a literal integer",
		)
	}
	return value, nil
}

func scalarIntegerLiteral(
	expression plan.ScalarExpression,
) (plan.Value, bool) {
	literal, ok := expression.(*plan.ScalarLiteralExpression)
	if !ok || literal == nil {
		return plan.Value{}, false
	}
	switch literal.Value.Kind {
	case plan.ValueKindInt64, plan.ValueKindUint64:
		return literal.Value, true
	default:
		return plan.Value{}, false
	}
}

func compileSQLiteSubstringUTF8SQL(
	inputSQL string,
	start plan.Value,
	length *plan.Value,
) (string, []any) {
	startSign := substringIntegerSign(start)
	if length == nil {
		if startSign == 0 {
			// SQLite treats position zero as immediately before the first
			// character; with no explicit length that is the whole String.
			return inputSQL, nil
		}
		if !nativeSubstringIntegerSafe(start) {
			return compileGenericSQLiteSubstringUTF8SQL(
				inputSQL,
				start,
				nil,
			)
		}
		startSQL, startArgs := compileNativeSubstringInteger(start)
		return "substringUTF8(" + inputSQL + ", " + startSQL + ")", startArgs
	}

	lengthSign := substringIntegerSign(*length)
	if lengthSign == 0 {
		return compileNativeSubstringUTF8(inputSQL, 1, 0)
	}
	if startSign >= 0 {
		startValue := nonnegativeSubstringInteger(start)
		if lengthSign > 0 {
			lengthValue := nonnegativeSubstringInteger(*length)
			if startValue == 0 {
				// SQLite counts the virtual zero position against a positive
				// length, so only length-1 real characters are returned.
				if lengthValue-1 <= maximumNativeSubstringInteger {
					return compileNativeSubstringUTF8(
						inputSQL,
						1,
						lengthValue-1,
					)
				}
				return compileGenericSQLiteSubstringUTF8SQL(
					inputSQL,
					start,
					length,
				)
			}
			if !nativeSubstringIntegerSafe(start) ||
				!nativeSubstringIntegerSafe(*length) {
				return compileGenericSQLiteSubstringUTF8SQL(
					inputSQL,
					start,
					length,
				)
			}
			startSQL, startArgs := compileNativeSubstringInteger(start)
			lengthSQL, lengthArgs := compileNativeSubstringInteger(*length)
			return "substringUTF8(" + inputSQL + ", " + startSQL +
					", " + lengthSQL + ")",
				append(startArgs, lengthArgs...)
		}

		// For a non-negative start and negative length, both SQLite interval
		// endpoints are compile-time constants. Clip the lower endpoint here;
		// native substringUTF8 clips the upper endpoint at the row's end.
		end := max(startValue, uint64(1))
		magnitude := negativeSubstringIntegerMagnitude(*length)
		begin := uint64(1)
		if startValue > 1 && magnitude < startValue-1 {
			begin = startValue - magnitude
		}
		if begin > maximumNativeSubstringInteger ||
			end-begin > maximumNativeSubstringInteger {
			return compileGenericSQLiteSubstringUTF8SQL(
				inputSQL,
				start,
				length,
			)
		}
		return compileNativeSubstringUTF8(inputSQL, begin, end-begin)
	}

	// A negative start with an explicit non-zero length has a clipped interval
	// which depends on the row's UTF-8 code-point count.
	return compileGenericSQLiteSubstringUTF8SQL(inputSQL, start, length)
}

func compileNativeSubstringUTF8(
	inputSQL string,
	start, length uint64,
) (string, []any) {
	return "substringUTF8(" + inputSQL +
			", CAST(? AS UInt64), CAST(? AS UInt64))",
		[]any{start, length}
}

func compileNativeSubstringInteger(value plan.Value) (string, []any) {
	if value.Kind == plan.ValueKindInt64 {
		return "CAST(? AS Int64)", []any{value.Int64}
	}
	return "CAST(? AS UInt64)", []any{value.Uint64}
}

func nativeSubstringIntegerSafe(value plan.Value) bool {
	return value.Kind == plan.ValueKindInt64 ||
		value.Uint64 <= maximumNativeSubstringInteger
}

func compileInt128SubstringInteger(value plan.Value) []any {
	if value.Kind == plan.ValueKindInt64 {
		return []any{value.Int64}
	}
	return []any{value.Uint64}
}

func substringIntegerSign(value plan.Value) int {
	if value.Kind == plan.ValueKindUint64 {
		if value.Uint64 == 0 {
			return 0
		}
		return 1
	}
	switch {
	case value.Int64 < 0:
		return -1
	case value.Int64 > 0:
		return 1
	default:
		return 0
	}
}

func nonnegativeSubstringInteger(value plan.Value) uint64 {
	if value.Kind == plan.ValueKindUint64 {
		return value.Uint64
	}
	if value.Int64 < 0 {
		return 0
	}

	return safecast.MustConv[uint64](value.Int64)
}

func negativeSubstringIntegerMagnitude(value plan.Value) uint64 {
	if value.Int64 >= 0 {
		return 0
	}
	// Subtract before negating so MinInt64 stays representable in its signed
	// domain. The result is non-negative and therefore fits UInt64.
	magnitudeMinusOne := -(value.Int64 + 1)

	return safecast.MustConv[uint64](magnitudeMinusOne) + 1
}

func compileGenericSQLiteSubstringUTF8SQL(
	inputSQL string,
	start plan.Value,
	length *plan.Value,
) (string, []any) {
	startArgs := compileInt128SubstringInteger(start)
	outerParameters := "value, start"
	outerArguments := "[" + inputSQL + "], [CAST(? AS Int128)]"

	positionSQL := "if(start < 0, n + start + 1, start)"
	beginSQL := positionSQL
	endSQL := "n + 1"
	indexArgs := startArgs
	if length != nil {
		lengthArgs := compileInt128SubstringInteger(*length)
		outerParameters += ", span"
		outerArguments += ", [CAST(? AS Int128)]"
		indexArgs = append(indexArgs, lengthArgs...)
		beginSQL = "if(span < 0, (" + positionSQL + ") + span, " +
			positionSQL + ")"
		endSQL = "if(span < 0, " + positionSQL + ", (" +
			positionSQL + ") + span)"
	}
	clippedBeginSQL := "clamp(" + beginSQL +
		", CAST(1 AS Int128), n + 1)"
	clippedEndSQL := "clamp(" + endSQL +
		", CAST(1 AS Int128), n + 1)"
	substringSQL := "substringUTF8(value, toInt64(" +
		clippedBeginSQL + "), toUInt64(" +
		clippedEndSQL + " - " + clippedBeginSQL + "))"
	lengthBindingSQL := "arrayElement(arrayMap(n -> " +
		substringSQL +
		", [CAST(lengthUTF8(value) AS Int128)]), 1)"
	return "arrayElement(arrayMap((" + outerParameters + ") -> " +
		lengthBindingSQL + ", " + outerArguments + "), 1)", indexArgs
}

func compileUnaryNonBooleanScalarInput(
	expression *plan.ScalarCallExpression,
	state compileState,
	functionName string,
) (compiledScalar, error) {
	input, err := compileUnaryScalarInput(expression, state, functionName)
	if err != nil {
		return compiledScalar{}, err
	}
	if scalarExpressionMayReturnBooleanFunction(expression.Arguments[0]) {
		return compiledScalar{}, booleanScalarConsumerError(functionName)
	}
	return input, nil
}

func compileUnaryScalarInput(
	expression *plan.ScalarCallExpression,
	state compileState,
	functionName string,
) (compiledScalar, error) {
	if expression == nil {
		return compiledScalar{}, fmt.Errorf(
			"compile ClickHouse %s: missing expression",
			functionName,
		)
	}
	if len(expression.Arguments) != 1 {
		return compiledScalar{}, fmt.Errorf(
			"compile ClickHouse %s: expected one argument",
			functionName,
		)
	}
	return compileScalarInputArgument(
		expression.Arguments[0],
		state,
		functionName,
	)
}

func compileScalarInputArgument(
	argument plan.ScalarExpression,
	state compileState,
	functionName string,
) (compiledScalar, error) {
	if nilScalarExpression(argument) {
		return compiledScalar{}, fmt.Errorf(
			"compile ClickHouse %s: missing scalar expression",
			functionName,
		)
	}
	return compileScalarValue(argument, state)
}

func compileNonBooleanScalarInputArgument(
	argument plan.ScalarExpression,
	state compileState,
	functionName string,
) (compiledScalar, error) {
	input, err := compileScalarInputArgument(argument, state, functionName)
	if err != nil {
		return compiledScalar{}, err
	}
	if scalarExpressionMayReturnBooleanFunction(argument) {
		return compiledScalar{}, booleanScalarConsumerError(functionName)
	}
	return input, nil
}

func compiledTextEligibleStringScalar(input compiledScalar) (string, []any) {
	inputSQL, inputArgs := compiledStringScalar(input)
	if input.textEligibleSQL == "" {
		return inputSQL, inputArgs
	}
	// _raw and conditionals derived from it carry a provenance guard.
	// Ingestion verifies the UTF-8 declaration, so this avoids both undefined
	// UTF-8 function behavior and a redundant byte scan.
	return "if(ifNull(" + input.textEligibleSQL + ", 0), " +
		inputSQL + ", CAST(NULL AS Nullable(String)))", inputArgs
}

func compileToStringScalar(
	expression *plan.ScalarCallExpression,
	state compileState,
) (compiledScalar, error) {
	if expression != nil && len(expression.Arguments) == 2 {
		return compileToStringFormatScalar(expression, state)
	}
	input, err := compileUnaryScalarInput(expression, state, "tostring")
	if err != nil {
		return compiledScalar{}, err
	}
	return compileLexicalStringScalar(
		input,
		state,
		scalarStringConversion{
			operation:           "tostring",
			unsupportedTypeCode: "SPL_UNSUPPORTED_TOSTRING_VALUE_TYPE",
			allowBoolean:        true,
			maximumSQLBytes:     maxCompiledToStringScalarSQLBytes,
		},
		expression.Range,
	)
}

type scalarStringConversion struct {
	operation           string
	unsupportedTypeCode string
	allowBoolean        bool
	maximumSQLBytes     int
}

// compileLexicalStringScalar implements the exact scalar-to-String spelling
// shared by explicit tostring and period concatenation. Dynamic inputs are
// bound once and domain-specialized; only the unrestricted domain reserves
// bounded decimal/v1 parsing work.
func compileLexicalStringScalar(
	input compiledScalar,
	state compileState,
	conversion scalarStringConversion,
	sourceRange spl.Range,
) (compiledScalar, error) {
	valueSQL := ""
	valueArgs := append([]any(nil), input.valueArgs...)
	textEligibleSQL := ""
	switch input.kind {
	case fieldKindDynamic:
		switch input.dynamicDomain {
		case dynamicScalarDomainText:
			// Text-case producers can only contain String, String arrays, or
			// null. A direct extraction avoids a redundant singleton-array
			// binding and runtime dispatch while rejecting multivalue variants.
			valueSQL = "dynamicElement(" + input.valueSQL + ", 'String')"
		case dynamicScalarDomainNumeric:
			typeSQL := "dynamicType(value)"
			valueSQL = "arrayElement(arrayMap(value -> if(" +
				dynamicNumericTypePredicate(typeSQL) + ", toString(value), " +
				"CAST(NULL AS Nullable(String))), [" +
				input.valueSQL + "]), 1)"
		default:
			if err := reserveStringConversionDynamicDecimal(
				state.context,
				conversion.operation,
				sourceRange,
			); err != nil {
				return compiledScalar{}, err
			}
			typeSQL := "dynamicType(value)"
			dynamicValue := compiledScalar{
				valueSQL:       "value",
				dynamicTypeSQL: typeSQL,
				kind:           fieldKindDynamic,
			}
			decimalCondition, decimalPayload := dynamicTaggedDecimalText(
				dynamicValue,
			)
			branches := typeSQL +
				" = 'String', dynamicElement(value, 'String'), "
			if conversion.allowBoolean {
				branches += typeSQL +
					" = 'Bool', if(dynamicElement(value, 'Bool'), " +
					"CAST('True' AS String), CAST('False' AS String)), "
			}
			valueSQL = "arrayElement(arrayMap(value -> multiIf(" +
				branches + decimalCondition + ", " + decimalPayload + ", " +
				dynamicNumericTypePredicate(typeSQL) + ", toString(value), " +
				"CAST(NULL AS Nullable(String))), [" +
				input.valueSQL + "]), 1)"
		}
	case fieldKindString:
		valueSQL = input.valueSQL
		textEligibleSQL = input.textEligibleSQL
	case fieldKindNumber:
		valueSQL = "toString(" + input.valueSQL + ")"
	case fieldKindBool:
		if !conversion.allowBoolean {
			return compiledScalar{}, booleanScalarConsumerError(
				conversion.operation,
			)
		}
		// transform preserves nullable Boolean null while evaluating its input
		// once, without allocating a singleton array per row.
		valueSQL = "transform(" + input.valueSQL + ", [true, false], " +
			"['True', 'False'], CAST(NULL AS Nullable(String)))"
	case fieldKindInvalid:
		valueSQL = "CAST(NULL AS Nullable(String))"
	case fieldKindStringArray, fieldKindDynamicArray:
		return compiledScalar{}, unsupportedMultivalueUsage(
			conversion.operation,
			sourceRange,
		)
	default:
		supportedTypes := "scalar String and number"
		if conversion.allowBoolean {
			supportedTypes = "scalar String, number, and Boolean"
		}
		return compiledScalar{}, &plan.Diagnostic{
			Code: conversion.unsupportedTypeCode,
			Message: conversion.operation +
				" supports " + supportedTypes + " input",
			Range: sourceRange,
		}
	}
	if len(valueSQL) > conversion.maximumSQLBytes {
		return compiledScalar{}, &plan.Diagnostic{
			Code: "SPL_QUERY_TOO_COMPLEX",
			Message: fmt.Sprintf(
				"%s scalar SQL exceeds %d bytes",
				conversion.operation,
				conversion.maximumSQLBytes,
			),
			Range: sourceRange,
		}
	}
	return compiledScalar{
		valueSQL:                    valueSQL,
		valueArgs:                   valueArgs,
		maxStringBytes:              compiledScalarStringByteBound(input),
		existsSQL:                   "1",
		textEligibleSQL:             textEligibleSQL,
		semanticBytesSQL:            input.semanticBytesSQL,
		semanticBytesArgs:           append([]any(nil), input.semanticBytesArgs...),
		semanticBytesByUTF8Validity: input.semanticBytesByUTF8Validity,
		textEligibleBySemanticBytes: input.textEligibleBySemanticBytes,
		stringOrBytes:               input.kind == fieldKindString && input.stringOrBytes,
		stringOrBytesNullable:       input.stringOrBytesNullable,
		kind:                        fieldKindString,
		alwaysNull:                  input.alwaysNull,
		materializeForPredicate:     input.materializeForPredicate,
	}, nil
}

func reserveStringConversionDynamicDecimal(
	context *compileContext,
	operation string,
	sourceRange spl.Range,
) error {
	if context == nil {
		return fmt.Errorf(
			"compile ClickHouse %s: query context is required",
			operation,
		)
	}
	reservation := uint64(MaximumStringConversionDynamicDecimalBytes)
	used := context.stringConversionBudget.dynamicDecimalBytes
	if used > MaximumStringConversionQueryDynamicDecimalBytes ||
		reservation > MaximumStringConversionQueryDynamicDecimalBytes-used {
		return &plan.Diagnostic{
			Code: "SPL_QUERY_TOO_COMPLEX",
			Message: fmt.Sprintf(
				"search Dynamic decimal String conversions require more than %d bytes of parsing",
				MaximumStringConversionQueryDynamicDecimalBytes,
			),
			Range: sourceRange,
		}
	}
	context.stringConversionBudget.dynamicDecimalBytes += reservation
	return nil
}

func compileConcatenationScalar(
	expression *plan.ScalarCallExpression,
	state compileState,
) (compiledScalar, error) {
	return compileConcatenationScalarWithNullPolicy(expression, state, false)
}

func compileConcatenationScalarWithNullPolicy(
	expression *plan.ScalarCallExpression,
	state compileState,
	nullAsEmpty bool,
) (compiledScalar, error) {
	if expression == nil {
		return compiledScalar{}, errors.New(
			"compile ClickHouse concatenation: missing expression",
		)
	}
	if len(expression.Arguments) < 2 {
		return compiledScalar{}, errors.New(
			"compile ClickHouse concatenation: requires at least two operands",
		)
	}
	if len(expression.Arguments) > spl.MaximumConcatenationOperands {
		return compiledScalar{}, &plan.Diagnostic{
			Code: "SPL_QUERY_TOO_COMPLEX",
			Message: fmt.Sprintf(
				"concatenation contains more than %d operands",
				spl.MaximumConcatenationOperands,
			),
			Range: expression.Range,
		}
	}
	if state.context == nil {
		return compiledScalar{}, errors.New(
			"compile ClickHouse concatenation: query context is required",
		)
	}
	if slices.ContainsFunc(expression.Arguments, nilScalarExpression) {
		return compiledScalar{}, errors.New(
			"compile ClickHouse concatenation: missing operand",
		)
	}
	if err := reserveConcatenationOperands(
		state.context,
		len(expression.Arguments),
		expression.Range,
	); err != nil {
		return compiledScalar{}, err
	}

	operands := make([]compiledScalar, 0, len(expression.Arguments))
	outputBytes := uint64(0)
	alwaysNull := false
	materializeForPredicate := false
	for _, argument := range expression.Arguments {
		input, err := compileScalarValue(argument, state)
		if err != nil {
			return compiledScalar{}, err
		}
		operand, err := compileLexicalStringScalar(
			input,
			state,
			scalarStringConversion{
				operation:           "concatenation",
				unsupportedTypeCode: "SPL_UNSUPPORTED_CONCATENATION_VALUE_TYPE",
				maximumSQLBytes:     maxCompiledConcatenationScalarSQLBytes,
			},
			argument.SourceRange(),
		)
		if err != nil {
			return compiledScalar{}, err
		}
		if nullAsEmpty {
			// A missing/null provenance-bearing String contributes an ordinary
			// empty String. Its dormant source provenance must not taint the
			// concatenation result as Bytes when every remaining contribution is
			// text. Command operands are exact fields or literals, so a guarded
			// field value is already a compiler-owned column without value args.
			if operand.textEligibleSQL != "" {
				operand.textEligibleSQL = "(isNull(" + operand.valueSQL + ") OR ifNull(" +
					operand.textEligibleSQL + ", 0))"
			}
			if operand.stringOrBytes && operand.semanticBytesSQL != "" {
				operand.semanticBytesSQL = "toUInt8(if(isNotNull(" + operand.valueSQL +
					"), ifNull(" + operand.semanticBytesSQL + ", 0), 0))"
				operand.semanticBytesArgs = append(
					append([]any(nil), operand.valueArgs...),
					operand.semanticBytesArgs...,
				)
				operand.stringOrBytesNullable = false
			}
			operand.valueSQL = "ifNull(" + operand.valueSQL + ", CAST('' AS String))"
			operand.alwaysNull = false
		}
		outputBytes = saturatingStringByteSum(
			outputBytes,
			compiledScalarStringByteBound(operand),
		)
		if outputBytes > MaximumConcatenationOutputBytes {
			return compiledScalar{}, &plan.Diagnostic{
				Code: "SPL_QUERY_TOO_COMPLEX",
				Message: fmt.Sprintf(
					"concatenation output may exceed %d bytes",
					MaximumConcatenationOutputBytes,
				),
				Range: expression.Range,
			}
		}
		alwaysNull = alwaysNull || operand.alwaysNull
		materializeForPredicate = materializeForPredicate ||
			operand.materializeForPredicate
		operands = append(operands, operand)
	}
	if err := reserveConcatenationOutput(
		state.context,
		outputBytes,
		expression.Range,
	); err != nil {
		return compiledScalar{}, err
	}

	var sql strings.Builder
	sql.WriteString("concat(")
	args := make([]any, 0)
	for index, operand := range operands {
		separatorBytes := 0
		if index > 0 {
			separatorBytes = 2
		}
		if sql.Len() >
			maxCompiledConcatenationScalarSQLBytes-
				separatorBytes-len(operand.valueSQL)-1 {
			return compiledScalar{}, &plan.Diagnostic{
				Code: "SPL_QUERY_TOO_COMPLEX",
				Message: fmt.Sprintf(
					"concatenation scalar SQL exceeds %d bytes",
					maxCompiledConcatenationScalarSQLBytes,
				),
				Range: expression.Range,
			}
		}
		if index > 0 {
			sql.WriteString(", ")
		}
		sql.WriteString(operand.valueSQL)
		args = append(args, operand.valueArgs...)
	}
	sql.WriteByte(')')
	valueSQL := sql.String()
	semanticGuards := make([]string, 0, len(operands))
	semanticGuardArgs := make([]any, 0)
	stringOrBytes := false
	stringOrBytesNullable := false
	for _, operand := range operands {
		stringOrBytesNullable = stringOrBytesNullable ||
			operand.stringOrBytesNullable || operand.alwaysNull
		if !operand.stringOrBytes {
			continue
		}
		stringOrBytes = true
		flagSQL, flagArgs := compiledScalarSemanticBytes(operand)
		semanticGuards = append(semanticGuards, flagSQL+" != 0")
		semanticGuardArgs = append(semanticGuardArgs, flagArgs...)
	}
	semanticBytesSQL := ""
	semanticBytesArgs := make([]any, 0)
	if stringOrBytes {
		// ClickHouse propagates Nullable from any concat operand, including an
		// ordinary String producer that carries no semantic-Bytes provenance.
		// Normalize every byte-capable concatenation to Nullable(String) so the
		// sealed result descriptor has one exact, conservative physical type.
		valueSQL = "CAST(" + valueSQL + " AS Nullable(String))"
		stringOrBytesNullable = true
		semanticBytesSQL = "toUInt8(if(isNotNull(" + valueSQL + "), " +
			"(" + strings.Join(semanticGuards, " OR ") + "), 0))"
		semanticBytesArgs = append(semanticBytesArgs, args...)
		semanticBytesArgs = append(semanticBytesArgs, semanticGuardArgs...)
	}
	return compiledScalar{
		valueSQL:                    valueSQL,
		valueArgs:                   args,
		maxStringBytes:              outputBytes,
		existsSQL:                   "1",
		textEligibleSQL:             concatenationTextEligibility(operands),
		semanticBytesSQL:            semanticBytesSQL,
		semanticBytesArgs:           semanticBytesArgs,
		semanticBytesByUTF8Validity: semanticBytesValidityOnly(operands...),
		textEligibleBySemanticBytes: stringOrBytes,
		stringOrBytes:               stringOrBytes,
		stringOrBytesNullable:       stringOrBytesNullable,
		kind:                        fieldKindString,
		alwaysNull:                  alwaysNull,
		materializeForPredicate:     materializeForPredicate,
	}, nil
}

func reserveConcatenationOperands(
	context *compileContext,
	operands int,
	sourceRange spl.Range,
) error {
	used := context.concatenationBudget.operands
	if used > spl.MaximumConcatenationOperandsPerQuery ||
		operands > spl.MaximumConcatenationOperandsPerQuery-used {
		return &plan.Diagnostic{
			Code: "SPL_QUERY_TOO_COMPLEX",
			Message: fmt.Sprintf(
				"concatenation contains more than %d operand occurrences per query",
				spl.MaximumConcatenationOperandsPerQuery,
			),
			Range: sourceRange,
		}
	}
	context.concatenationBudget.operands += operands
	return nil
}

func reserveConcatenationOutput(
	context *compileContext,
	outputBytes uint64,
	sourceRange spl.Range,
) error {
	used := context.concatenationBudget.outputBytes
	if used > MaximumConcatenationQueryOutputBytes ||
		outputBytes > MaximumConcatenationQueryOutputBytes-used {
		return &plan.Diagnostic{
			Code: "SPL_QUERY_TOO_COMPLEX",
			Message: fmt.Sprintf(
				"concatenation outputs may exceed %d bytes per query row",
				MaximumConcatenationQueryOutputBytes,
			),
			Range: sourceRange,
		}
	}
	context.concatenationBudget.outputBytes += outputBytes
	return nil
}

func concatenationTextEligibility(operands []compiledScalar) string {
	seen := make(map[string]struct{}, len(operands))
	guards := make([]string, 0, len(operands))
	for _, operand := range operands {
		if operand.textEligibleSQL == "" {
			continue
		}
		if _, duplicate := seen[operand.textEligibleSQL]; duplicate {
			continue
		}
		seen[operand.textEligibleSQL] = struct{}{}
		guards = append(guards, operand.textEligibleSQL)
	}
	switch len(guards) {
	case 0:
		return ""
	case 1:
		return guards[0]
	default:
		return "(" + strings.Join(guards, ") AND (") + ")"
	}
}

func compileRoundScalar(
	expression *plan.ScalarCallExpression,
	state compileState,
) (compiledScalar, error) {
	if expression == nil {
		return compiledScalar{}, errors.New(
			"compile ClickHouse round: missing expression",
		)
	}
	if len(expression.Arguments) < 1 || len(expression.Arguments) > 2 {
		return compiledScalar{}, errors.New(
			"compile ClickHouse round: requires one or two arguments",
		)
	}
	input, err := compileNonBooleanScalarInputArgument(
		expression.Arguments[0],
		state,
		"round",
	)
	if err != nil {
		return compiledScalar{}, err
	}

	operation := numericRoundingOperation{
		functionName:        "round",
		unsupportedTypeCode: "SPL_UNSUPPORTED_ROUND_VALUE_TYPE",
	}
	if len(expression.Arguments) == 2 {
		precision, precisionErr := roundPrecisionLiteral(
			expression.Arguments[1],
		)
		if precisionErr != nil {
			return compiledScalar{}, precisionErr
		}
		operation.precision = &precision
	}

	return compileNumericRoundingInput(
		input,
		operation,
		expression.Range,
	)
}

func compileIntegralRoundingScalar(
	expression *plan.ScalarCallExpression,
	state compileState,
	functionName string,
) (compiledScalar, error) {
	input, err := compileUnaryNonBooleanScalarInput(
		expression,
		state,
		functionName,
	)
	if err != nil {
		return compiledScalar{}, err
	}
	unsupportedTypeCode := "SPL_UNSUPPORTED_CEIL_VALUE_TYPE"
	if functionName == "floor" {
		unsupportedTypeCode = "SPL_UNSUPPORTED_FLOOR_VALUE_TYPE"
	}
	return compileNumericRoundingInput(
		input,
		numericRoundingOperation{
			functionName:        functionName,
			unsupportedTypeCode: unsupportedTypeCode,
		},
		expression.Range,
	)
}

type numericRoundingOperation struct {
	functionName        string
	unsupportedTypeCode string
	precision           *uint8
}

// compileNumericRoundingInput implements the common numeric contract for
// round, ceil, and floor. Dynamic input is bound once; integer variants and
// integral semantic Decimals stay exact, while other numeric variants are
// converted through finite Float64 before applying the requested function.
func compileNumericRoundingInput(
	input compiledScalar,
	operation numericRoundingOperation,
	sourceRange spl.Range,
) (compiledScalar, error) {
	numericIntegral := operation.functionName != "round" ||
		operation.precision == nil ||
		*operation.precision == 0
	if input.alwaysNull {
		return compiledScalar{
			valueSQL:        "CAST(NULL AS Nullable(Float64))",
			existsSQL:       "1",
			kind:            fieldKindNumber,
			numberType:      "Float64",
			numericIntegral: numericIntegral,
			alwaysNull:      true,
			ieeeComparison:  input.ieeeComparison,
		}, nil
	}
	if input.numericIntegral &&
		(operation.functionName == "ceil" || operation.functionName == "floor") {
		return input, nil
	}
	fixedArgumentsSQL := ""
	dynamicArgumentsSQL := ""
	var operationArgs []any
	if operation.precision != nil {
		fixedArgumentsSQL = ", CAST(? AS UInt8)"
		dynamicArgumentsSQL = ", precision"
		operationArgs = []any{*operation.precision}
	}
	valueSQL := ""
	valueArgs := append([]any(nil), input.valueArgs...)
	resultKind := fieldKindNumber
	numberType := input.numberType
	dynamicDomain := dynamicScalarDomainAny
	alwaysNull := input.alwaysNull
	switch input.kind {
	case fieldKindDynamic:
		if input.dynamicDomain == dynamicScalarDomainText {
			valueSQL = "CAST(NULL AS Nullable(Float64))"
			valueArgs = nil
			numberType = "Float64"
			alwaysNull = true
			break
		}
		typeSQL := "dynamicType(value)"
		integerCondition := dynamicIntegerTypePredicate(typeSQL)
		body := ""
		if input.dynamicDomain == dynamicScalarDomainNumeric {
			rounded := operation.functionName + "(" +
				finiteDynamicFloatOrNullSQL("value") +
				dynamicArgumentsSQL + ")"
			body = "multiIf(" +
				integerCondition + ", value, " +
				"CAST(" + rounded + " AS Dynamic))"
		} else {
			dynamicValue := compiledScalar{
				valueSQL:       "value",
				dynamicTypeSQL: typeSQL,
				kind:           fieldKindDynamic,
			}
			decimalCondition, decimalPayload := dynamicTaggedDecimalText(
				dynamicValue,
			)
			exactTaggedInteger := dynamicTaggedDecimalIntegralSQL(
				dynamicValue,
			)
			numericValue := "multiIf(" +
				decimalCondition + ", " +
				finiteFloatOrNullSQL(decimalPayload) + ", " +
				dynamicNumericTypePredicate(typeSQL) + ", " +
				finiteDynamicFloatOrNullSQL("value") + ", " +
				"CAST(NULL AS Nullable(Float64)))"
			rounded := operation.functionName + "(" + numericValue + dynamicArgumentsSQL + ")"
			body = "arrayElement(arrayMap(exact_value -> multiIf(" +
				integerCondition + ", value, (" +
				"isNotNull(exact_value)), CAST(assumeNotNull(exact_value) AS Dynamic), " +
				"CAST(" + rounded + " AS Dynamic)), [" +
				exactTaggedInteger + "]), 1)"
		}
		if len(operationArgs) == 0 {
			valueSQL = "arrayElement(arrayMap(value -> " + body + ", [" +
				input.valueSQL + "]), 1)"
		} else {
			valueSQL = "arrayElement(arrayMap((value, precision) -> " + body +
				", [" + input.valueSQL + "], [CAST(? AS UInt8)]), 1)"
		}
		valueArgs = append(valueArgs, operationArgs...)
		resultKind = fieldKindDynamic
		numberType = ""
		dynamicDomain = dynamicScalarDomainNumeric
	case fieldKindNumber:
		if fixedNumberTypeIsInteger(input.numberType) {
			valueSQL = input.valueSQL
			break
		}
		valueSQL = operation.functionName + "(" + input.valueSQL + fixedArgumentsSQL + ")"
		valueArgs = append(valueArgs, operationArgs...)
	case fieldKindInvalid:
		valueSQL = "CAST(NULL AS Nullable(Float64))"
		numberType = "Float64"
		alwaysNull = true
	case fieldKindStringArray, fieldKindDynamicArray:
		return compiledScalar{}, unsupportedMultivalueUsage(
			operation.functionName,
			sourceRange,
		)
	default:
		return compiledScalar{}, &plan.Diagnostic{
			Code:    operation.unsupportedTypeCode,
			Message: operation.functionName + " requires a numeric input",
			Range:   sourceRange,
		}
	}
	if len(valueSQL) > maxCompiledNumericRoundingScalarSQLBytes {
		return compiledScalar{}, &plan.Diagnostic{
			Code: "SPL_QUERY_TOO_COMPLEX",
			Message: fmt.Sprintf(
				"%s scalar SQL exceeds %d bytes",
				operation.functionName,
				maxCompiledNumericRoundingScalarSQLBytes,
			),
			Range: sourceRange,
		}
	}
	return compiledScalar{
		valueSQL:                valueSQL,
		valueArgs:               valueArgs,
		existsSQL:               "1",
		dynamicDomain:           dynamicDomain,
		numericIntegral:         numericIntegral,
		kind:                    resultKind,
		numberType:              numberType,
		alwaysNull:              alwaysNull,
		ieeeComparison:          input.ieeeComparison,
		materializeForPredicate: input.materializeForPredicate,
	}, nil
}

func compileMVSortScalar(
	expression *plan.ScalarCallExpression,
	state compileState,
) (compiledScalar, error) {
	input, err := compileUnaryNonBooleanScalarInput(expression, state, "mvsort")
	if err != nil {
		return compiledScalar{}, err
	}
	if input.mvSortedLexicographic {
		return input, nil
	}

	emptyArray := "CAST([], 'Array(String)')"
	if input.alwaysNull {
		return compiledScalar{
			valueSQL:              emptyArray,
			existsSQL:             "0",
			kind:                  fieldKindStringArray,
			alwaysNull:            true,
			mvSortedLexicographic: true,
		}, nil
	}

	valueSQL := ""
	valueArgs := append([]any(nil), input.valueArgs...)
	existsSQL := "1"
	var existsArgs []any
	presentSQL := ""
	resultKind := fieldKindStringArray
	dynamicDomain := dynamicScalarDomainAny
	requiresRuntimeValidation := input.requiresRuntimeValidation
	switch input.kind {
	case fieldKindStringArray:
		valueSQL = "arrayElement(arrayMap(values -> " +
			boundedMVSortStringArraySQL(
				"values",
				emptyArray,
				"Array(String)",
				false,
			) +
			", [" + input.valueSQL + "]), 1)"
		if input.optionalMultivaluePresentSQL != "" {
			existsSQL, existsArgs = scalarExistsSQL(input)
			presentSQL = input.optionalMultivaluePresentSQL
		}
	case fieldKindDynamicArray:
		normalized, normalizeErr := compileNativeMVState(input, false)
		if normalizeErr != nil {
			return compiledScalar{}, normalizeErr
		}
		stateAlias := "__os_mvsort_native_state"
		valuesAlias := "__os_mvsort_native_values"
		sortedAlias := "__os_mvsort_native_sorted"
		values := "tupleElement(" + stateAlias + ", 1)"
		stringsSQL := "arrayMap(element -> assumeNotNull(dynamicElement(element, 'String')), " +
			valuesAlias + ")"
		sorted := "arrayMap(element -> CAST(element AS Dynamic), arraySort(" + stringsSQL + "))"
		invalid := "tupleElement(" + stateAlias + ", 4) != 0 OR " +
			"arrayExists(element -> dynamicType(element) != 'String', " + valuesAlias + ")"
		body := bindSQLExpressions(
			[]string{sortedAlias},
			[]string{sorted},
			nativeMVPreflightSQL(
				sortedAlias,
				invalid,
				"length("+sortedAlias+")",
				nativeMVArrayPayloadBytesSQL(sortedAlias),
				emptyNativeMVSQL(),
			),
		)
		body = bindSQLExpressions([]string{valuesAlias}, []string{values}, body)
		valueSQL = bindSQLExpressions(
			[]string{stateAlias},
			[]string{normalized.sql},
			body,
		)
		valueArgs = append([]any(nil), normalized.args...)
		existsSQL, presentSQL, existsArgs = nativeMVPreservedStateSQL(input, normalized)
		resultKind = fieldKindDynamicArray
		dynamicDomain = dynamicScalarDomainText
		requiresRuntimeValidation = true
		markNativeMVRuntimeValidation(state)
	case fieldKindDynamic:
		nullDynamic := "CAST(NULL AS Dynamic)"
		stringArray := "arrayElement(arrayMap(values -> " +
			boundedMVSortStringArraySQL(
				"values",
				nullDynamic,
				"Dynamic",
				true,
			) +
			", [dynamicElement(value, 'Array(String)')]), 1)"
		dynamicArray := "arrayElement(arrayMap(values -> " +
			boundedMVSortDynamicArraySQL("values") +
			", [dynamicElement(value, 'Array(Dynamic)')]), 1)"
		body := "multiIf(" +
			"dynamicType(value) = 'Array(String)', " + stringArray + ", " +
			"dynamicType(value) = 'Array(Dynamic)', " + dynamicArray + ", " +
			nullDynamic + ")"
		bound := "arrayElement(arrayMap(value -> " + body +
			", [" + input.valueSQL + "]), 1)"
		dynamicExistsSQL := input.existsSQL
		if dynamicExistsSQL == "" {
			dynamicExistsSQL = "1"
		}
		if dynamicExistsSQL == "1" {
			valueSQL = bound
		} else {
			valueSQL = "if(" + dynamicExistsSQL + ", " + bound + ", " + nullDynamic + ")"
			valueArgs = append(
				append([]any(nil), input.existsArgs...),
				input.valueArgs...,
			)
		}
		resultKind = fieldKindDynamic
		dynamicDomain = dynamicScalarDomainText
	case fieldKindInvalid:
		valueSQL = emptyArray
		valueArgs = nil
	default:
		return compiledScalar{}, &plan.Diagnostic{
			Code:    "SPL_UNSUPPORTED_MVSORT_VALUE_TYPE",
			Message: "mvsort requires a multivalue String input",
			Range:   expression.Range,
		}
	}
	if len(valueSQL) > maxCompiledMVSortScalarSQLBytes {
		return compiledScalar{}, &plan.Diagnostic{
			Code: "SPL_QUERY_TOO_COMPLEX",
			Message: fmt.Sprintf(
				"mvsort scalar SQL exceeds %d bytes",
				maxCompiledMVSortScalarSQLBytes,
			),
			Range: expression.Range,
		}
	}
	return compiledScalar{
		valueSQL:                     valueSQL,
		valueArgs:                    valueArgs,
		maxStringBytes:               input.maxStringBytes,
		existsSQL:                    existsSQL,
		existsArgs:                   existsArgs,
		optionalMultivaluePresentSQL: presentSQL,
		dynamicDomain:                dynamicDomain,
		kind:                         resultKind,
		mvSortedLexicographic:        true,
		materializeForPredicate:      input.materializeForPredicate,
		requiresRuntimeValidation:    requiresRuntimeValidation,
	}, nil
}

func boundedMVSortStringArraySQL(
	valuesSQL string,
	invalidSQL string,
	resultType string,
	requireNonEmpty bool,
) string {
	conditions := []string{
		"length(" + valuesSQL + ") <= toUInt64(" +
			strconv.FormatUint(uint64(MaximumMVSortValues), 10) + ")",
		stringArrayPayloadBytesSQL(valuesSQL) + " <= toUInt128(" +
			strconv.FormatUint(uint64(MaximumMVSortBytes), 10) + ")",
		"arrayAll(element -> isValidUTF8(element), " + valuesSQL + ")",
	}
	if requireNonEmpty {
		conditions = append([]string{"notEmpty(" + valuesSQL + ")"}, conditions...)
	}
	return "if(" + strings.Join(conditions, " AND ") +
		", CAST(arraySort(" + valuesSQL + ") AS " + resultType +
		"), " + invalidSQL + ")"
}

func boundedMVSortDynamicArraySQL(valuesSQL string) string {
	nullableStringValue := "dynamicElement(element, 'String')"
	stringValue := "assumeNotNull(" + nullableStringValue + ")"
	overLimitBytes := strconv.FormatUint(uint64(MaximumMVSortBytes)+1, 10)
	payloadBytes := "arrayFold((bytes, element) -> bytes + toUInt128(ifNull(" +
		"length(" + nullableStringValue + "), toUInt64(" + overLimitBytes + "))), " +
		valuesSQL + ", toUInt128(0))"
	conditions := []string{
		"notEmpty(" + valuesSQL + ")",
		"length(" + valuesSQL + ") <= toUInt64(" +
			strconv.FormatUint(uint64(MaximumMVSortValues), 10) + ")",
		"arrayAll(element -> dynamicType(element) = 'String', " + valuesSQL + ")",
		payloadBytes + " <= toUInt128(" +
			strconv.FormatUint(uint64(MaximumMVSortBytes), 10) + ")",
		"arrayAll(element -> isValidUTF8(ifNull(" + nullableStringValue +
			", '')), " + valuesSQL + ")",
	}
	stringsSQL := "arrayMap(element -> " + stringValue + ", " + valuesSQL + ")"
	return "if(" + strings.Join(conditions, " AND ") +
		", CAST(arraySort(" + stringsSQL + ") AS Dynamic), CAST(NULL AS Dynamic))"
}

func compileMVCountScalar(
	expression *plan.ScalarCallExpression,
	state compileState,
) (compiledScalar, error) {
	input, err := compileUnaryScalarInput(expression, state, "mvcount")
	if err != nil {
		return compiledScalar{}, err
	}
	if input.mvCountOneOrNull {
		return input, nil
	}
	nullUInt64 := "CAST(NULL AS Nullable(UInt64))"
	if input.alwaysNull {
		return compiledScalar{
			valueSQL:         nullUInt64,
			existsSQL:        "1",
			kind:             fieldKindNumber,
			numberType:       "UInt64",
			numericIntegral:  true,
			mvCountOneOrNull: true,
			alwaysNull:       true,
		}, nil
	}

	valueSQL := ""
	valueArgs := append([]any(nil), input.valueArgs...)
	switch input.kind {
	case fieldKindStringArray:
		valueSQL = "nullIf(toUInt64(length(" + input.valueSQL +
			")), toUInt64(0))"
	case fieldKindDynamicArray:
		// Explicit native null members are retained in the typed list but do
		// not contribute to mvcount, matching the open Dynamic Array(Dynamic)
		// path below and stats count(field).
		valueSQL = "nullIf(toUInt64(arrayCount(element -> dynamicType(element) != 'None', " +
			input.valueSQL + ")), toUInt64(0))"
	case fieldKindDynamic:
		existsSQL := input.existsSQL
		if existsSQL == "" {
			existsSQL = "1"
		}
		body := ""
		switch input.dynamicDomain {
		case dynamicScalarDomainText:
			typeSQL := "dynamicType(value)"
			body = "multiIf(" +
				typeSQL + " = 'String', toUInt64(1), " +
				typeSQL + " = 'Array(String)', nullIf(toUInt64(length(" +
				"dynamicElement(value, 'Array(String)'))), toUInt64(0)), " +
				nullUInt64 + ")"
		case dynamicScalarDomainNumeric:
			body = "if(dynamicType(" + input.valueSQL + ") = 'None', " +
				nullUInt64 + ", toUInt64(1))"
			if existsSQL == "1" {
				valueSQL = body
			} else {
				valueSQL = "if(" + existsSQL + ", " + body + ", " + nullUInt64 + ")"
				valueArgs = append(append([]any(nil), input.existsArgs...), input.valueArgs...)
			}
		default:
			typeSQL := "dynamicType(value)"
			dynamicValue := compiledScalar{
				valueSQL:       "value",
				dynamicTypeSQL: typeSQL,
				kind:           fieldKindDynamic,
			}
			dynamicCount := "nullIf(" +
				dynamicNonNullArrayCardinalitySQL("value") +
				", toUInt64(0))"
			otherArrayCount := "nullIf(toUInt64(length(value)), toUInt64(0))"
			scalar := "(" +
				typeSQL + " = 'String' OR " +
				typeSQL + " = 'Bool' OR " +
				dynamicNumericTypePredicate(typeSQL) + " OR " +
				dynamicTaggedScalarEnvelopeCondition(dynamicValue) + ")"
			body = "multiIf(" +
				typeSQL + " = 'None', " + nullUInt64 + ", " +
				typeSQL + " = 'Array(Dynamic)', " + dynamicCount + ", " +
				"startsWith(" + typeSQL + ", 'Array('), " +
				otherArrayCount + ", " +
				scalar + ", toUInt64(1), " +
				nullUInt64 + ")"
		}
		if input.dynamicDomain != dynamicScalarDomainNumeric {
			bound := "arrayElement(arrayMap(value -> " + body +
				", [" + input.valueSQL + "]), 1)"
			if existsSQL == "1" {
				valueSQL = bound
			} else {
				valueSQL = "if(" + existsSQL + ", " + bound + ", " + nullUInt64 + ")"
				valueArgs = append(append([]any(nil), input.existsArgs...), input.valueArgs...)
			}
		}
	case fieldKindInvalid:
		valueSQL = nullUInt64
		valueArgs = nil
	default:
		valueSQL = "if(isNotNull(" + input.valueSQL +
			"), toUInt64(1), " + nullUInt64 + ")"
	}
	if len(valueSQL) > maxCompiledMVCountScalarSQLBytes {
		return compiledScalar{}, &plan.Diagnostic{
			Code: "SPL_QUERY_TOO_COMPLEX",
			Message: fmt.Sprintf(
				"mvcount scalar SQL exceeds %d bytes",
				maxCompiledMVCountScalarSQLBytes,
			),
			Range: expression.Range,
		}
	}
	return compiledScalar{
		valueSQL:                valueSQL,
		valueArgs:               valueArgs,
		existsSQL:               "1",
		kind:                    fieldKindNumber,
		numberType:              "UInt64",
		numericIntegral:         true,
		mvCountOneOrNull:        true,
		materializeForPredicate: input.materializeForPredicate,
	}, nil
}

func roundPrecisionLiteral(
	expression plan.ScalarExpression,
) (uint8, error) {
	if nilScalarExpression(expression) {
		return 0, errors.New(
			"compile ClickHouse round: missing precision",
		)
	}
	value, ok := scalarIntegerLiteral(expression)
	if !ok {
		return 0, errors.New(
			"compile ClickHouse round: precision must be a literal integer",
		)
	}
	switch value.Kind {
	case plan.ValueKindInt64:
		if value.Int64 < 0 ||
			value.Int64 > spl.MaximumRoundPrecision {
			return 0, fmt.Errorf(
				"compile ClickHouse round: precision must be from 0 through %d",
				spl.MaximumRoundPrecision,
			)
		}

		return safecast.MustConv[uint8](value.Int64), nil
	case plan.ValueKindUint64:
		if value.Uint64 > spl.MaximumRoundPrecision {
			return 0, fmt.Errorf(
				"compile ClickHouse round: precision must be from 0 through %d",
				spl.MaximumRoundPrecision,
			)
		}

		return safecast.MustConv[uint8](value.Uint64), nil
	default:
		return 0, errors.New(
			"compile ClickHouse round: precision must be a literal integer",
		)
	}
}

func compileToNumberScalar(expression *plan.ScalarCallExpression, state compileState) (compiledScalar, error) {
	if len(expression.Arguments) != 1 {
		return compiledScalar{}, errors.New("compile ClickHouse tonumber: expected one argument")
	}
	if scalarExpressionMayReturnBooleanFunction(expression.Arguments[0]) {
		return compiledScalar{}, booleanScalarConsumerError("tonumber")
	}
	input, err := compileScalarValue(expression.Arguments[0], state)
	if err != nil {
		return compiledScalar{}, err
	}
	if isNativeMultivalueKind(input.kind) {
		return compiledScalar{}, unsupportedMultivalueUsage("tonumber", expression.Range)
	}
	inputSQL, inputArgs := compiledStringScalar(input)
	return compiledScalar{
		valueSQL:                "ifNotFinite(toFloat64OrNull(" + inputSQL + "), CAST(NULL AS Nullable(Float64)))",
		valueArgs:               inputArgs,
		existsSQL:               "1",
		kind:                    fieldKindNumber,
		numberType:              "Float64",
		materializeForPredicate: input.materializeForPredicate,
	}, nil
}

func compiledStringScalar(value compiledScalar) (string, []any) {
	if value.kind == fieldKindDynamic {
		return "if(" + value.existsSQL + ", dynamicElement(" + value.valueSQL + ", 'String'), CAST(NULL AS Nullable(String)))",
			append(append([]any(nil), value.existsArgs...), value.valueArgs...)
	}
	if value.existsSQL != "" && value.existsSQL != "1" {
		return "if(" + value.existsSQL + ", toString(" + value.valueSQL + "), CAST(NULL AS Nullable(String)))",
			append(append([]any(nil), value.existsArgs...), value.valueArgs...)
	}
	if value.kind == fieldKindString {
		return value.valueSQL, append([]any(nil), value.valueArgs...)
	}
	if value.kind == fieldKindTime {
		return "toString(" + numericScalarSQL(value, false) + ")", append([]any(nil), value.valueArgs...)
	}
	return "toString(" + value.valueSQL + ")", append([]any(nil), value.valueArgs...)
}

func scalarStringLiteral(expression plan.ScalarExpression) (string, bool) {
	literal, ok := expression.(*plan.ScalarLiteralExpression)
	if !ok || literal == nil || literal.Value.Kind != plan.ValueKindString {
		return "", false
	}
	return literal.Value.String, true
}

func scalarQuotedStringLiteral(expression plan.ScalarExpression) (string, bool) {
	literal, ok := expression.(*plan.ScalarLiteralExpression)
	if !ok || literal == nil ||
		literal.Value.Kind != plan.ValueKindString ||
		!literal.Value.Quoted {
		return "", false
	}
	return literal.Value.String, true
}

func scalarExpressionMayReturnBooleanFunction(expression plan.ScalarExpression) bool {
	switch expression := expression.(type) {
	case *plan.ScalarUnaryExpression, *plan.ScalarBinaryExpression:
		// Arithmetic has a fixed numeric result. Its operands are validated by
		// arithmetic lowering so a nested Boolean receives the source-located
		// unsupported-arithmetic diagnostic instead of being mistaken for a
		// directly assigned Boolean result here.
		return false
	case *plan.ScalarCallExpression:
		if expression == nil {
			return false
		}
		if expression.Function.ReturnsBoolean() {
			return true
		}
		if expression.Function == plan.ScalarFunctionCoalesce {
			if slices.ContainsFunc(expression.Arguments, scalarExpressionMayReturnBooleanFunction) {
				return true
			}
		}
		return false
	case *plan.ScalarIfExpression:
		return expression != nil &&
			(scalarExpressionMayReturnBooleanFunction(expression.True) ||
				scalarExpressionMayReturnBooleanFunction(expression.False))
	case *plan.ScalarCaseExpression:
		if expression == nil {
			return false
		}
		for _, branch := range expression.Branches {
			if scalarExpressionMayReturnBooleanFunction(branch.Value) {
				return true
			}
		}
		return false
	default:
		return false
	}
}

func scalarExpressionRequiresNativeMVValidation(
	expression plan.ScalarExpression,
	state compileState,
) bool {
	if nilScalarExpression(expression) {
		return false
	}
	switch expression := expression.(type) {
	case *plan.ScalarUnaryExpression:
		return scalarExpressionRequiresNativeMVValidation(expression.Operand, state)
	case *plan.ScalarBinaryExpression:
		return scalarExpressionRequiresNativeMVValidation(expression.Left, state) ||
			scalarExpressionRequiresNativeMVValidation(expression.Right, state)
	case *plan.ScalarCallExpression:
		switch expression.Function {
		case plan.ScalarFunctionSplit,
			plan.ScalarFunctionMVAppend,
			plan.ScalarFunctionMVDedup,
			plan.ScalarFunctionMVIndex,
			plan.ScalarFunctionMVJoin,
			plan.ScalarFunctionMVZip,
			plan.ScalarFunctionMVFind:
			return true
		case plan.ScalarFunctionLower,
			plan.ScalarFunctionUpper,
			plan.ScalarFunctionMVSort:
			// These established transforms gain a runtime guard only for a
			// compiler-sealed native-list input. Detect that direct field form
			// precisely instead of forcing every ordinary scalar lower/upper or
			// open-Dynamic mvsort through a materialized validation fence. Nested
			// native producers are found by the recursive argument walk below.
			if len(expression.Arguments) == 1 &&
				sealedNativeMVFieldExpression(expression.Arguments[0], state) {
				return true
			}
		case plan.ScalarFunctionTrim,
			plan.ScalarFunctionLTrim,
			plan.ScalarFunctionRTrim,
			plan.ScalarFunctionURLDecode,
			plan.ScalarFunctionMD5,
			plan.ScalarFunctionSHA1,
			plan.ScalarFunctionSHA256,
			plan.ScalarFunctionSHA512:
			// Per-member text transforms follow the lower/upper rule; the
			// optional trim character set is a literal and never a list.
			if len(expression.Arguments) >= 1 &&
				sealedNativeMVFieldExpression(expression.Arguments[0], state) {
				return true
			}
		}
		for _, argument := range expression.Arguments {
			if scalarExpressionRequiresNativeMVValidation(argument, state) {
				return true
			}
		}
		return false
	case *plan.ScalarIfExpression:
		return expressionRequiresNativeMVValidation(expression.Condition, state) ||
			scalarExpressionRequiresNativeMVValidation(expression.True, state) ||
			scalarExpressionRequiresNativeMVValidation(expression.False, state)
	case *plan.ScalarCaseExpression:
		for _, branch := range expression.Branches {
			if expressionRequiresNativeMVValidation(branch.Condition, state) ||
				scalarExpressionRequiresNativeMVValidation(branch.Value, state) {
				return true
			}
		}
	}
	return false
}

func sealedNativeMVFieldExpression(
	expression plan.ScalarExpression,
	state compileState,
) bool {
	fieldExpression, ok := expression.(*plan.ScalarFieldExpression)
	if !ok || fieldExpression == nil {
		return false
	}
	field, resolved, err := resolveCompiledField(fieldExpression.Field, state)
	return err == nil && resolved && isNativeMultivalueKind(field.kind) &&
		(field.kind == fieldKindDynamicArray ||
			field.optionalMultivaluePresentSQL != "")
}

func expressionRequiresNativeMVValidation(
	expression plan.Expression,
	state compileState,
) bool {
	if nilPlanExpression(expression) {
		return false
	}
	switch expression := expression.(type) {
	case *plan.BooleanExpression:
		return expressionRequiresNativeMVValidation(expression.Left, state) ||
			expressionRequiresNativeMVValidation(expression.Right, state)
	case *plan.NotExpression:
		return expressionRequiresNativeMVValidation(expression.Operand, state)
	case *plan.EvalComparisonExpression:
		return scalarExpressionRequiresNativeMVValidation(expression.Left, state) ||
			scalarExpressionRequiresNativeMVValidation(expression.Right, state)
	case *plan.MembershipExpression:
		if scalarExpressionRequiresNativeMVValidation(expression.Value, state) {
			return true
		}
		for _, candidate := range expression.Candidates {
			if scalarExpressionRequiresNativeMVValidation(candidate, state) {
				return true
			}
		}
	case *plan.ScalarPredicateExpression:
		return scalarExpressionRequiresNativeMVValidation(expression.Value, state)
	}
	return false
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
			return "multiIf(startsWith(" + typeSQL + ", 'Float'), " +
				floating + ", " + dynamicNumericValuePredicate(dynamic) +
				", " + exact + ", " + nullBool + ")", 1, true
		}
		return "if(" + dynamicNumericValuePredicate(dynamic) + ", " +
			exactNumericKeyComparisonSQL(
				exactNumericScalarKeySQL(left),
				exactNumericScalarKeySQL(right),
				operator,
			) +
			", " + nullBool + ")", 1, true
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

func dynamicNumericValuePredicate(value compiledScalar) string {
	if value.dynamicNumericEligibleSQL != "" {
		return value.dynamicNumericEligibleSQL
	}
	return "(" + dynamicNumericTypePredicate(dynamicScalarTypeSQL(value)) + " OR " + dynamicTaggedDecimalCondition(value) + ")"
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
		return "if(" + dynamicExactIntegerPredicate(dynamic, typeSQL) + ", " +
			exact + ", 0)", 0
	case plan.ValueKindFloat64:
		exactCondition := "(startsWith(" + typeSQL + ", 'Decimal') OR " +
			dynamicTaggedDecimalCondition(dynamic) + ")"
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
func compileExactScalarGroup(
	group plan.FieldRef,
	state compileState,
	multivalueOperation string,
) (compiledExactScalarGroup, error) {
	field, exists, resolveErr := resolveCompiledField(group, state)
	if resolveErr != nil {
		return compiledExactScalarGroup{}, resolveErr
	}
	if !exists {
		// A prior projection is authoritative. The typed missing key preserves
		// the declared downstream schema without consulting private event data.
		field = fieldState{
			valueSQL:   "CAST(NULL AS Nullable(String))",
			existsSQL:  "0",
			kind:       fieldKindString,
			alwaysNull: true,
		}
	}
	if isNativeMultivalueKind(field.kind) {
		return compiledExactScalarGroup{}, unsupportedMultivalueUsage(
			multivalueOperation,
			group.Range,
		)
	}

	existsSQL := field.existsSQL
	if existsSQL == "" {
		existsSQL = "1"
	}
	presenceSQL := "(" + existsSQL + " AND isNotNull(" + field.valueSQL + "))"
	presenceArgs := append([]any(nil), field.existsArgs...)
	if field.kind == fieldKindDynamic && field.descendantSQL != "" {
		// Flattened non-empty objects have no exact parent leaf. Keep them
		// present until the scoped scalar validation rejects the container.
		presenceSQL = "(" + presenceSQL + " OR " + field.descendantSQL + ")"
		presenceArgs = append(presenceArgs, field.descendantArgs...)
	}
	compiled := compiledExactScalarGroup{
		field:        field,
		keySQL:       field.valueSQL,
		presenceSQL:  presenceSQL,
		presenceArgs: presenceArgs,
	}
	if field.kind == fieldKindDynamic {
		supported, lexical := statsByScalarExpressions(field)
		compiled.keySQL = "if(" + supported + ", " + lexical + ", '')"
		compiled.unsupportedSQL = "(" + presenceSQL + ") AND NOT (" + supported + ")"
		compiled.unsupportedArgs = presenceArgs
	}
	return compiled, nil
}

// exactScalarGroupClassificationSQL binds the Dynamic support predicate once
// and publishes the complete row-local BY contract as:
//
//	(key, present, unsupported)
//
// Windowed Dynamic eventstats extrema consume this tuple from a separate
// preparation layer. That keeps key construction and independent container
// validation from expanding the tagged-envelope classifier twice.
func exactScalarGroupClassificationSQL(
	group compiledExactScalarGroup,
) (string, []any) {
	if group.field.kind != fieldKindDynamic {
		return "tuple(" + group.keySQL + ", toUInt8(" +
				group.presenceSQL + "), toUInt8(0))",
			append([]any(nil), group.presenceArgs...)
	}

	presenceVariable := "__os_eventstats_group_present"
	typeVariable := "__os_eventstats_group_type"
	supportedVariable := "__os_eventstats_group_supported"
	supportedSQL, lexicalSQL := statsByScalarExpressionsFor(
		group.field.valueSQL,
		typeVariable,
	)
	classification := "tuple(if(" + supportedVariable + ", " + lexicalSQL +
		", CAST('' AS String)), toUInt8(" + presenceVariable + "), toUInt8(" +
		presenceVariable + " AND NOT (" + supportedVariable + ")))"
	classification = bindSQLExpressions(
		[]string{presenceVariable, supportedVariable},
		[]string{group.presenceSQL, supportedSQL},
		classification,
	)
	classification = bindSQLExpressions(
		[]string{typeVariable},
		[]string{dynamicTypeExpression(group.field)},
		classification,
	)
	return classification, append([]any(nil), group.presenceArgs...)
}

type compiledEventStatsGroup struct {
	scalar   compiledExactScalarGroup
	keyAlias string
}

type eventAggregateMeasureSpec struct {
	function        plan.AggregateFunction
	percentile      uint8
	materialized    bool
	numberType      string
	numericIntegral bool
	valuePrefix     string
}

func streamAggregateFieldFunctionForm(
	function plan.AggregateFunction,
) (string, bool) {
	switch function {
	case plan.AggregateFunctionCountValues:
		return "count", true
	case plan.AggregateFunctionSum:
		return "sum", true
	case plan.AggregateFunctionAverage:
		return "avg", true
	case plan.AggregateFunctionMinimum:
		return "min", true
	case plan.AggregateFunctionMaximum:
		return "max", true
	case plan.AggregateFunctionEarliest:
		return "earliest", true
	case plan.AggregateFunctionLatest:
		return "latest", true
	default:
		return "", false
	}
}

func validateStreamAggregate(
	operator *plan.StreamAggregate,
	state compileState,
) (plan.FieldRef, error) {
	if operator == nil {
		return plan.FieldRef{}, errors.New(
			"compile ClickHouse streamstats: operator is missing",
		)
	}
	if err := validateNonStatsAggregateMeasureMetadata("streamstats", operator.Measure); err != nil {
		return plan.FieldRef{}, err
	}
	if len(operator.GroupBy) > spl.MaximumStatsGroupFields {
		return plan.FieldRef{}, fmt.Errorf(
			"compile ClickHouse streamstats: more than %d grouping fields",
			spl.MaximumStatsGroupFields,
		)
	}
	if operator.WindowRows > spl.MaximumStreamStatsWindow {
		return plan.FieldRef{}, fmt.Errorf(
			"compile ClickHouse streamstats: window exceeds %d rows",
			spl.MaximumStreamStatsWindow,
		)
	}
	if len(operator.GroupBy) > 0 && operator.WindowRows > 0 && operator.Global {
		return plan.FieldRef{}, errors.New(
			"compile ClickHouse streamstats: grouped positive windows require global=false",
		)
	}
	measure := operator.Measure
	if measure.Output == "" ||
		(measure.Function != plan.AggregateFunctionCountPredicate &&
			(measure.Predicate != nil || measure.Percentile != 0)) {
		return plan.FieldRef{}, errors.New(
			"compile ClickHouse streamstats: aggregate contains unsupported metadata",
		)
	}
	if (measure.Function == plan.AggregateFunctionEarliest ||
		measure.Function == plan.AggregateFunctionLatest) &&
		!hasCanonicalEventTime(state) {
		return plan.FieldRef{}, &plan.Diagnostic{
			Code: "SPL_UNSUPPORTED_STREAMSTATS_TIME_FIELD",
			Message: "streamstats earliest and latest require event rows " +
				"with the unmodified canonical _time field",
			Range: measure.Input.Range,
			Suggestions: []string{
				"run streamstats earliest or latest before removing, replacing, or transforming _time",
			},
		}
	}
	switch measure.Function {
	case plan.AggregateFunctionCountRows:
		if measure.Input.Name != "" ||
			measure.Input.Canonical ||
			measure.Input.Path != nil ||
			measure.Input.Range != (spl.Range{}) {
			return plan.FieldRef{}, errors.New(
				"compile ClickHouse streamstats: argument-free count contains input metadata",
			)
		}
	case plan.AggregateFunctionCountPredicate:
		if err := validateConditionalCountMeasure(
			measure,
			state,
			"streamstats",
			"SPL_AMBIGUOUS_STREAMSTATS_FIELD",
			"streamstats cannot read the event result's reserved fields payload without an exact upstream schema",
		); err != nil {
			return plan.FieldRef{}, err
		}
	case plan.AggregateFunctionCountValues, plan.AggregateFunctionSum,
		plan.AggregateFunctionAverage, plan.AggregateFunctionMinimum,
		plan.AggregateFunctionMaximum, plan.AggregateFunctionEarliest,
		plan.AggregateFunctionLatest:
		form, supported := streamAggregateFieldFunctionForm(measure.Function)
		if !supported {
			return plan.FieldRef{}, errors.New(
				"compile ClickHouse streamstats: field aggregate form is unsupported",
			)
		}
		if !spl.IsExactUnquotedFieldName(measure.Input.Name) {
			return plan.FieldRef{}, errors.New(
				"compile ClickHouse streamstats: " + form +
					" input must be one exact unquoted field",
			)
		}
		if err := validateCanonicalFieldRef(
			"streamstats",
			form+" input",
			measure.Input,
		); err != nil {
			return plan.FieldRef{}, err
		}
		if state.eventRows && state.allowDynamic && measure.Input.Name == "fields" {
			return plan.FieldRef{}, &plan.Diagnostic{
				Code: "SPL_AMBIGUOUS_STREAMSTATS_FIELD",
				Message: "streamstats cannot read the event result's reserved " +
					"fields payload without an exact upstream schema",
				Range: measure.Input.Range,
			}
		}
	default:
		return plan.FieldRef{}, errors.New(
			"compile ClickHouse streamstats: only count, count(field), count(eval(...)), sum(field), avg(field), min(field), max(field), earliest(field), and latest(field) are supported",
		)
	}
	defaultOutput := ""
	if form, supported := streamAggregateFieldFunctionForm(measure.Function); supported {
		defaultOutput = form + "(" + measure.Input.Name + ")"
	}
	validOutput := spl.IsExactUnquotedFieldName(measure.Output) ||
		(defaultOutput != "" && measure.Output == defaultOutput)
	if !validOutput {
		return plan.FieldRef{}, errors.New(
			"compile ClickHouse streamstats: output must be one exact unquoted field",
		)
	}
	output, err := plan.ResolveField(measure.Output, operator.Range)
	if err != nil {
		return plan.FieldRef{}, fmt.Errorf(
			"compile ClickHouse streamstats: invalid output field %q: %w",
			measure.Output,
			err,
		)
	}
	if state.eventRows && state.allowDynamic && output.Name == "fields" {
		return plan.FieldRef{}, &plan.Diagnostic{
			Code: "SPL_AMBIGUOUS_STREAMSTATS_FIELD",
			Message: "streamstats cannot replace the event result's reserved " +
				"fields payload without an exact upstream schema",
			Range: output.Range,
		}
	}
	seen := make(map[string]struct{}, len(operator.GroupBy))
	for _, group := range operator.GroupBy {
		if !spl.IsExactUnquotedFieldName(group.Name) {
			return plan.FieldRef{}, errors.New(
				"compile ClickHouse streamstats: grouping fields must be exact and unquoted",
			)
		}
		if err := validateCanonicalFieldRef(
			"streamstats",
			"grouping",
			group,
		); err != nil {
			return plan.FieldRef{}, err
		}
		if _, duplicate := seen[group.Name]; duplicate {
			return plan.FieldRef{}, fmt.Errorf(
				"compile ClickHouse streamstats: grouping field %q is repeated",
				group.Name,
			)
		}
		seen[group.Name] = struct{}{}
		if state.eventRows && state.allowDynamic && group.Name == "fields" {
			return plan.FieldRef{}, &plan.Diagnostic{
				Code: "SPL_AMBIGUOUS_STREAMSTATS_FIELD",
				Message: "streamstats cannot group by the event result's reserved " +
					"fields payload without an exact upstream schema",
				Range: group.Range,
			}
		}
	}
	return output, nil
}

func streamAggregateCompileState(
	state compileState,
	output plan.FieldRef,
	outputState fieldState,
	grouped bool,
	stage int,
	order []compiledSortKey,
	tieBreakers []compiledSortKey,
) compileState {
	next := cloneCompileState(state)
	if exposesRawFieldsPayload(state) && !output.Canonical {
		dropRawFieldsPayload(&next)
	}
	delete(next.blocked, output.Name)
	if !slices.Contains(next.publicOrder, output.Name) {
		next.publicOrder = append(next.publicOrder, output.Name)
	}
	existsSQL := "1"
	if grouped {
		existsSQL = quoteIdentifier(fmt.Sprintf(
			"__os_streamstats_exists_%d",
			stage,
		))
	}
	outputState.valueSQL = quoteIdentifier(output.Name)
	outputState.existsSQL = existsSQL
	// Every streamstats result is derived. Even min(_time) is not the immutable
	// event timestamp required by canonical-time consumers such as timechart.
	outputState.canonicalTime = false
	next.visible[output.Name] = outputState
	// The running value may replace a public field that supplied the incoming
	// order. Every key was snapshotted before replacement, so make those private
	// sequences the durable pipeline order and stable event identity. A later
	// explicit sort consumes tieBreakers independently from order.
	next.order = append([]compiledSortKey(nil), order...)
	next.tieBreakers = append([]compiledSortKey(nil), tieBreakers...)
	next.privateColumns = livePrivateColumns(next.privateColumns, next.visible)
	if outputState.storedTypeSQL != "" {
		next.privateColumns = append(next.privateColumns, outputState.storedTypeSQL)
	}
	if grouped {
		next.privateColumns = append(next.privateColumns, existsSQL)
	}
	return next
}

// streamStatsStringArrayExtremaMeasureSQL folds a fixed Array(String) row to
// the same constant-size winner tuple used by Dynamic extrema. The stream
// window is still measured in source rows: multivalue members never expand the
// relation or consume independent frame positions.
func streamStatsStringArrayExtremaMeasureSQL(
	function plan.AggregateFunction,
	valuesSQL string,
	rowEligibleSQL string,
) string {
	if rowEligibleSQL == "" {
		rowEligibleSQL = "1"
	}
	state := "__os_streamstats_extrema_state"
	value := "value"
	candidateSQL := statsExtremaScalarCandidateSQL(
		value,
		statsExtremaScalarNumberSQL(value),
		"0",
	)
	step := extremaFoldWinnerStateSQL(
		function,
		state,
		candidateSQL,
		"",
	)
	empty := eventStatsExtremaEmptyRowStateSQL("0")
	fold := "arrayFold((" + state + ", " + value + ") -> " + step + ", " +
		valuesSQL + ", " + empty + ")"
	return "if(" + rowEligibleSQL + ", " + fold + ", " + empty + ")"
}

func streamStatsFrameSQL(includeCurrent bool, window uint64) string {
	if window == 0 {
		if includeCurrent {
			return "ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW"
		}
		return "ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING"
	}
	if includeCurrent {
		if window == 1 {
			return "ROWS BETWEEN CURRENT ROW AND CURRENT ROW"
		}
		return "ROWS BETWEEN " + strconv.FormatUint(window-1, 10) +
			" PRECEDING AND CURRENT ROW"
	}
	return "ROWS BETWEEN " + strconv.FormatUint(window, 10) +
		" PRECEDING AND 1 PRECEDING"
}

func chronologicalAggregateFunction(function plan.AggregateFunction) bool {
	return function == plan.AggregateFunctionEarliest ||
		function == plan.AggregateFunctionLatest
}

func newChronologicalAggregateOutput(state compileState, name string) bool {
	if name == "" || slices.Contains(state.publicOrder, name) {
		return false
	}
	_, visible := state.visible[name]
	return !visible
}

func independentChronologicalAggregateOutputs(
	state compileState,
	firstInput, secondInput, firstOutput, secondOutput string,
) bool {
	if !newChronologicalAggregateOutput(state, firstOutput) ||
		!newChronologicalAggregateOutput(state, secondOutput) ||
		firstOutput == secondOutput {
		return false
	}
	// Restrict fusion to pure sibling publications. In particular, neither
	// measure may observe a value authored by the other logical stage, and an
	// output may not replace a source field whose pre-replacement value the
	// sibling consumes.
	for _, output := range []string{firstOutput, secondOutput} {
		if output == firstInput || output == secondInput {
			return false
		}
	}
	return true
}

func dynamicChronologicalInputs(
	state compileState,
	first, second plan.FieldRef,
) bool {
	firstField, firstExists, firstErr := resolveCompiledField(first, state)
	secondField, secondExists, secondErr := resolveCompiledField(second, state)
	return firstErr == nil && secondErr == nil && firstExists && secondExists &&
		firstField.kind == fieldKindDynamic && secondField.kind == fieldKindDynamic
}

func canFuseChronologicalEventAggregates(
	first, second *plan.EventAggregate,
	state compileState,
) bool {
	if first == nil || second == nil || len(first.GroupBy) != 0 ||
		len(second.GroupBy) != 0 ||
		!chronologicalAggregateFunction(first.Measure.Function) ||
		!chronologicalAggregateFunction(second.Measure.Function) ||
		!independentChronologicalAggregateOutputs(
			state,
			first.Measure.Input.Name,
			second.Measure.Input.Name,
			first.Measure.Output,
			second.Measure.Output,
		) {
		return false
	}
	return dynamicChronologicalInputs(
		state,
		first.Measure.Input,
		second.Measure.Input,
	)
}

func canFuseChronologicalStreamAggregates(
	first, second *plan.StreamAggregate,
	state compileState,
) bool {
	if first == nil || second == nil || len(first.GroupBy) != 0 ||
		len(second.GroupBy) != 0 ||
		first.IncludeCurrent != second.IncludeCurrent ||
		first.WindowRows != second.WindowRows || first.Global != second.Global ||
		!chronologicalAggregateFunction(first.Measure.Function) ||
		!chronologicalAggregateFunction(second.Measure.Function) ||
		!independentChronologicalAggregateOutputs(
			state,
			first.Measure.Input.Name,
			second.Measure.Input.Name,
			first.Measure.Output,
			second.Measure.Output,
		) {
		return false
	}
	return dynamicChronologicalInputs(
		state,
		first.Measure.Input,
		second.Measure.Input,
	)
}

type fusedChronologicalPublication struct {
	name             string
	valueSQL         string
	storedTypeSQL    string
	validationColumn string
	validationSQL    string
}

func fusedChronologicalProjection(
	state, next compileState,
	publications []fusedChronologicalPublication,
) ([]string, error) {
	byName := make(map[string]fusedChronologicalPublication, len(publications))
	for _, publication := range publications {
		if publication.name == "" || publication.valueSQL == "" ||
			publication.storedTypeSQL == "" || publication.validationColumn == "" ||
			publication.validationSQL == "" {
			return nil, errors.New(
				"compile ClickHouse chronological fusion: publication is incomplete",
			)
		}
		if _, duplicate := byName[publication.name]; duplicate {
			return nil, errors.New(
				"compile ClickHouse chronological fusion: output is repeated",
			)
		}
		byName[publication.name] = publication
	}

	names := orderedVisibleNames(next)
	projection := make([]string, 0, len(names)+16+len(next.privateColumns))
	for _, name := range names {
		publicName := quoteIdentifier(name)
		if publication, authored := byName[name]; authored {
			projection = append(
				projection,
				publication.valueSQL+" AS "+publicName,
			)
			continue
		}
		field, present := state.visible[name]
		if !present {
			return nil, fmt.Errorf(
				"compile ClickHouse chronological fusion: input field %q is unavailable",
				name,
			)
		}
		projection = appendVisibleFieldProjection(projection, field, publicName)
	}

	projectionState := next
	projectionState.privateColumns = livePrivateColumns(
		state.privateColumns,
		next.visible,
	)
	projection = appendPrivateEventProjection(projection, projectionState)
	for _, publication := range publications {
		output := next.visible[publication.name]
		if output.storedTypeSQL == "" {
			return nil, errors.New(
				"compile ClickHouse chronological fusion: output type sidecar is missing",
			)
		}
		projection = append(
			projection,
			publication.storedTypeSQL+" AS "+output.storedTypeSQL,
			"toUInt8("+publication.validationSQL+") AS "+
				publication.validationColumn,
		)
	}
	return projection, nil
}

func fusedChronologicalOutputState(
	output plan.FieldRef,
	input fieldState,
	typeColumn string,
) fieldState {
	return fieldState{
		kind:           fieldKindDynamic,
		dynamicTypeSQL: "dynamicType(" + quoteIdentifier(output.Name) + ")",
		storedTypeSQL:  typeColumn,
		maxStringBytes: fieldStateStringByteBound(input),
	}
}

// transferDeferredChronologicalValidation moves validation ownership to a
// later complete barrier only when every moved column is still a private
// column of that barrier's input. The earlier barrier remains the semantic
// source relation, but no longer needs a second top-level consumer solely to
// repeat validation work.
func transferDeferredChronologicalValidation(
	barriers []compiledChronologicalBarrier,
	available []string,
) ([]compiledChronologicalBarrier, []string) {
	if len(barriers) == 0 || len(available) == 0 {
		return barriers, nil
	}
	transferred := make([]string, 0, len(available))
	for index := range barriers {
		barrier := &barriers[index]
		retained := make([]string, 0, len(barrier.validationColumns))
		for _, column := range barrier.validationColumns {
			if slices.Contains(available, column) {
				transferred = append(transferred, column)
				continue
			}
			retained = append(retained, column)
		}
		barrier.validationColumns = retained
		if len(retained) == 0 && barrier.fanout == 2 {
			// The ungrouped fused eventstats source is row-preserving and has one
			// remaining consumer after its validation columns move forward.
			barrier.fanout = 1
		}
	}
	return barriers, transferred
}

// compileFusedChronologicalEventAggregates lowers two independent sibling
// eventstats chronological measures over one bounded input and one global
// window. Both row-local poison bits remain independently named on the shared
// barrier, so the final validation consumer still sees the complete relation
// even if a later command removes every public row.
func compileFusedChronologicalEventAggregates(
	relation compiledRelation,
	first, second *plan.EventAggregate,
	state compileState,
	firstStage, secondStage int,
) (compiledRelation, compileState, []any, *pendingChronologicalBarrier, error) {
	if !canFuseChronologicalEventAggregates(first, second, state) ||
		secondStage != firstStage+1 {
		return compiledRelation{}, compileState{}, nil, nil, errors.New(
			"compile ClickHouse fused eventstats chronology: contract is invalid",
		)
	}

	firstOutput, firstErr := validateEventAggregate(first, state)
	if firstErr != nil {
		return compiledRelation{}, compileState{}, nil, nil, firstErr
	}
	firstInput, firstExists, resolveErr := resolveCompiledField(
		first.Measure.Input,
		state,
	)
	if resolveErr != nil {
		return compiledRelation{}, compileState{}, nil, nil, resolveErr
	}
	firstCandidate, firstArgs, firstValidated, candidateErr :=
		singleChronologicalCandidateSQL(
			first.Measure.Function,
			firstInput,
			firstExists,
		)
	if candidateErr != nil {
		return compiledRelation{}, compileState{}, nil, nil, candidateErr
	}
	if !firstValidated {
		return compiledRelation{}, compileState{}, nil, nil, errors.New(
			"compile ClickHouse fused eventstats chronology: first input is not runtime validated",
		)
	}
	firstType := quoteIdentifier(fmt.Sprintf(
		"__os_eventstats_extrema_type_%d",
		firstStage,
	))
	firstState := eventAggregateCompileState(
		state,
		firstOutput,
		fusedChronologicalOutputState(firstOutput, firstInput, firstType),
		false,
		firstStage,
	)

	secondOutput, secondErr := validateEventAggregate(second, firstState)
	if secondErr != nil {
		return compiledRelation{}, compileState{}, nil, nil, secondErr
	}
	secondInput, secondExists, resolveErr := resolveCompiledField(
		second.Measure.Input,
		firstState,
	)
	if resolveErr != nil {
		return compiledRelation{}, compileState{}, nil, nil, resolveErr
	}
	secondCandidate, secondArgs, secondValidated, candidateErr :=
		singleChronologicalCandidateSQL(
			second.Measure.Function,
			secondInput,
			secondExists,
		)
	if candidateErr != nil {
		return compiledRelation{}, compileState{}, nil, nil, candidateErr
	}
	if !secondValidated {
		return compiledRelation{}, compileState{}, nil, nil, errors.New(
			"compile ClickHouse fused eventstats chronology: second input is not runtime validated",
		)
	}
	secondType := quoteIdentifier(fmt.Sprintf(
		"__os_eventstats_extrema_type_%d",
		secondStage,
	))
	next := eventAggregateCompileState(
		firstState,
		secondOutput,
		fusedChronologicalOutputState(secondOutput, secondInput, secondType),
		false,
		secondStage,
	)

	firstMeasure := quoteIdentifier(fmt.Sprintf(
		"__os_eventstats_measure_%d",
		firstStage,
	))
	secondMeasure := quoteIdentifier(fmt.Sprintf(
		"__os_eventstats_measure_%d",
		secondStage,
	))
	sourceAlias := quoteIdentifier(fmt.Sprintf(
		"__os_eventstats_fused_source_%d",
		secondStage,
	))
	rowKey := immutableChronologicalRowKeySQL()
	preparedSQL := "SELECT *, tuple(" + firstCandidate + ", " + rowKey +
		") AS " + firstMeasure + ", tuple(" + secondCandidate + ", " +
		rowKey + ") AS " + secondMeasure + " FROM (" + relation.sql +
		") AS " + sourceAlias + " LIMIT " +
		strconv.FormatUint(MaximumEventStatsInputRows+1, 10)

	firstAggregate, aggregateErr := singleChronologicalAggregateSQL(
		first.Measure.Function,
		firstMeasure,
	)
	if aggregateErr != nil {
		return compiledRelation{}, compileState{}, nil, nil, aggregateErr
	}
	secondAggregate, aggregateErr := singleChronologicalAggregateSQL(
		second.Measure.Function,
		secondMeasure,
	)
	if aggregateErr != nil {
		return compiledRelation{}, compileState{}, nil, nil, aggregateErr
	}
	rawCount := quoteIdentifier(fmt.Sprintf(
		"__os_eventstats_fused_raw_count_%d",
		secondStage,
	))
	firstWinner := quoteIdentifier(fmt.Sprintf(
		"__os_eventstats_fused_winner_%d",
		firstStage,
	))
	secondWinner := quoteIdentifier(fmt.Sprintf(
		"__os_eventstats_fused_winner_%d",
		secondStage,
	))
	preparedAlias := quoteIdentifier(fmt.Sprintf(
		"__os_eventstats_fused_prepared_%d",
		secondStage,
	))
	windowSQL := "SELECT *, count() OVER () AS " + rawCount + ", " +
		firstAggregate + " OVER () AS " + firstWinner + ", " +
		secondAggregate + " OVER () AS " + secondWinner + " FROM (" +
		preparedSQL + ") AS " + preparedAlias

	inputCount := quoteIdentifier(fmt.Sprintf(
		"__os_eventstats_input_count_%d",
		secondStage,
	))
	windowAlias := quoteIdentifier(fmt.Sprintf(
		"__os_eventstats_fused_window_%d",
		secondStage,
	))
	boundedSQL := "SELECT *, " + boundedEventStatsCountSQL(rawCount) +
		" AS " + inputCount + " FROM (" + windowSQL + ") AS " + windowAlias
	resultAlias := quoteIdentifier(fmt.Sprintf(
		"__os_eventstats_fused_result_%d",
		secondStage,
	))
	firstValidation := quoteIdentifier(fmt.Sprintf(
		"__os_eventstats_validation_%d",
		firstStage,
	))
	secondValidation := quoteIdentifier(fmt.Sprintf(
		"__os_eventstats_validation_%d",
		secondStage,
	))
	publications := []fusedChronologicalPublication{
		{
			name:             firstOutput.Name,
			valueSQL:         chronologicalPublishedValueSQL(resultAlias + "." + firstWinner),
			storedTypeSQL:    chronologicalPublishedTypeSQL(resultAlias + "." + firstWinner),
			validationColumn: firstValidation,
			validationSQL: "tupleElement(tupleElement(" + resultAlias + "." +
				firstMeasure + ", 1), 4)",
		},
		{
			name:             secondOutput.Name,
			valueSQL:         chronologicalPublishedValueSQL(resultAlias + "." + secondWinner),
			storedTypeSQL:    chronologicalPublishedTypeSQL(resultAlias + "." + secondWinner),
			validationColumn: secondValidation,
			validationSQL: "tupleElement(tupleElement(" + resultAlias + "." +
				secondMeasure + ", 1), 4)",
		},
	}
	projection, projectionErr := fusedChronologicalProjection(
		state,
		next,
		publications,
	)
	if projectionErr != nil {
		return compiledRelation{}, compileState{}, nil, nil, projectionErr
	}
	next.deferredChronologicalValidation = append(
		next.deferredChronologicalValidation,
		firstValidation,
		secondValidation,
	)
	maximumRows := strconv.FormatUint(MaximumEventStatsInputRows, 10)
	resultSQL := "SELECT " + strings.Join(projection, ", ") + " FROM (" +
		boundedSQL + ") AS " + resultAlias + " WHERE " + resultAlias + "." +
		inputCount + " <= " + maximumRows
	resultDepth := relation.depth + 4
	enriched := compiledRelation{
		sql:        resultSQL,
		depth:      resultDepth,
		ownerRange: second.Range,
	}

	resultInputName := quoteIdentifier(fmt.Sprintf(
		"__os_eventstats_result_input_%d",
		secondStage,
	))
	barrierName := quoteIdentifier(fmt.Sprintf(
		"__os_eventstats_result_%d",
		secondStage,
	))
	barrierDepth := relationalNodeDepth(resultDepth)
	barrier := &pendingChronologicalBarrier{
		name: barrierName,
		sql:  "SELECT * FROM " + resultInputName,
		prerequisiteDefinitions: []string{
			resultInputName + " AS MATERIALIZED (" + resultSQL + ")",
		},
		validationColumns: []string{firstValidation, secondValidation},
		fanout:            2,
		depth:             barrierDepth,
		ownerRange:        second.Range,
	}
	publishedAlias := quoteIdentifier(fmt.Sprintf(
		"__os_eventstats_rows_result_%d",
		secondStage,
	))
	publishedSQL := "SELECT * FROM " + barrierName + " AS " + publishedAlias
	enriched.depth = barrierDepth
	prefixArgs := append(append([]any(nil), firstArgs...), secondArgs...)
	return enriched.selectFrom(publishedSQL, second.Range), next, prefixArgs, barrier, nil
}

// compileFusedChronologicalStreamAggregates captures the established order
// once and evaluates two independent windows with the same frame. Sequential
// streamstats semantics are unchanged because neither measure consumes or
// replaces the sibling's input or output.
func compileFusedChronologicalStreamAggregates(
	relation compiledRelation,
	first, second *plan.StreamAggregate,
	state compileState,
	firstStage, secondStage int,
) (compiledRelation, compileState, []any, *pendingChronologicalBarrier, error) {
	if !canFuseChronologicalStreamAggregates(first, second, state) ||
		secondStage != firstStage+1 {
		return compiledRelation{}, compileState{}, nil, nil, errors.New(
			"compile ClickHouse fused streamstats chronology: contract is invalid",
		)
	}

	firstOutput, firstErr := validateStreamAggregate(first, state)
	if firstErr != nil {
		return compiledRelation{}, compileState{}, nil, nil, firstErr
	}
	firstInput, firstExists, resolveErr := resolveCompiledField(
		first.Measure.Input,
		state,
	)
	if resolveErr != nil {
		return compiledRelation{}, compileState{}, nil, nil, resolveErr
	}
	firstCandidate, firstArgs, firstValidated, candidateErr :=
		singleChronologicalCandidateSQL(
			first.Measure.Function,
			firstInput,
			firstExists,
		)
	if candidateErr != nil {
		return compiledRelation{}, compileState{}, nil, nil, candidateErr
	}
	if !firstValidated {
		return compiledRelation{}, compileState{}, nil, nil, errors.New(
			"compile ClickHouse fused streamstats chronology: first input is not runtime validated",
		)
	}

	orderKeys := append([]compiledSortKey(nil), defaultCompiledOrder(state)...)
	tieBreakers := append([]compiledSortKey(nil), state.tieBreakers...)
	orderProjection := make([]string, 0, len(orderKeys)+len(tieBreakers))
	for index := range orderKeys {
		captured := quoteIdentifier(fmt.Sprintf(
			"__os_streamstats_order_%d_%d",
			firstStage,
			index,
		))
		orderProjection = append(
			orderProjection,
			orderKeys[index].valueSQL+" AS "+captured,
		)
		orderKeys[index].valueSQL = captured
	}
	for index := range tieBreakers {
		captured := quoteIdentifier(fmt.Sprintf(
			"__os_streamstats_tie_breaker_%d_%d",
			firstStage,
			index,
		))
		orderProjection = append(
			orderProjection,
			tieBreakers[index].valueSQL+" AS "+captured,
		)
		tieBreakers[index].valueSQL = captured
	}
	orderSQL := ""
	if len(orderKeys) > 0 {
		var orderErr error
		orderSQL, orderErr = compileMaterializedOrder(orderKeys, false)
		if orderErr != nil {
			return compiledRelation{}, compileState{}, nil, nil, fmt.Errorf(
				"compile ClickHouse fused streamstats order: %w",
				orderErr,
			)
		}
	}

	firstType := quoteIdentifier(fmt.Sprintf(
		"__os_streamstats_chronological_type_%d",
		firstStage,
	))
	firstState := streamAggregateCompileState(
		state,
		firstOutput,
		fusedChronologicalOutputState(firstOutput, firstInput, firstType),
		false,
		firstStage,
		orderKeys,
		tieBreakers,
	)
	secondOutput, secondErr := validateStreamAggregate(second, firstState)
	if secondErr != nil {
		return compiledRelation{}, compileState{}, nil, nil, secondErr
	}
	secondInput, secondExists, resolveErr := resolveCompiledField(
		second.Measure.Input,
		firstState,
	)
	if resolveErr != nil {
		return compiledRelation{}, compileState{}, nil, nil, resolveErr
	}
	secondCandidate, secondArgs, secondValidated, candidateErr :=
		singleChronologicalCandidateSQL(
			second.Measure.Function,
			secondInput,
			secondExists,
		)
	if candidateErr != nil {
		return compiledRelation{}, compileState{}, nil, nil, candidateErr
	}
	if !secondValidated {
		return compiledRelation{}, compileState{}, nil, nil, errors.New(
			"compile ClickHouse fused streamstats chronology: second input is not runtime validated",
		)
	}
	secondType := quoteIdentifier(fmt.Sprintf(
		"__os_streamstats_chronological_type_%d",
		secondStage,
	))
	next := streamAggregateCompileState(
		firstState,
		secondOutput,
		fusedChronologicalOutputState(secondOutput, secondInput, secondType),
		false,
		secondStage,
		orderKeys,
		tieBreakers,
	)

	firstMeasure := quoteIdentifier(fmt.Sprintf(
		"__os_streamstats_measure_%d",
		firstStage,
	))
	secondMeasure := quoteIdentifier(fmt.Sprintf(
		"__os_streamstats_measure_%d",
		secondStage,
	))
	rowKey := immutableChronologicalRowKeySQL()
	sourceAlias := quoteIdentifier(fmt.Sprintf(
		"__os_streamstats_fused_source_%d",
		secondStage,
	))
	orderedProjection := []string{
		"*",
		"tuple(" + firstCandidate + ", " + rowKey + ") AS " + firstMeasure,
		"tuple(" + secondCandidate + ", " + rowKey + ") AS " + secondMeasure,
	}
	orderedProjection = append(orderedProjection, orderProjection...)
	orderedInput := "SELECT " + strings.Join(orderedProjection, ", ") +
		" FROM (" + relation.sql + ") AS " + sourceAlias
	if orderSQL != "" {
		orderedInput += " ORDER BY " + orderSQL
	}
	orderedInput += " LIMIT " + strconv.FormatUint(MaximumStreamStatsInputRows+1, 10)

	windowParts := make([]string, 0, 2)
	if orderSQL != "" {
		windowParts = append(windowParts, "ORDER BY "+orderSQL)
	} else {
		windowParts = append(windowParts, "ORDER BY tuple()")
	}
	windowParts = append(
		windowParts,
		streamStatsFrameSQL(first.IncludeCurrent, first.WindowRows),
	)
	windowClause := strings.Join(windowParts, " ")
	firstAggregate, aggregateErr := singleChronologicalAggregateSQL(
		first.Measure.Function,
		firstMeasure,
	)
	if aggregateErr != nil {
		return compiledRelation{}, compileState{}, nil, nil, aggregateErr
	}
	secondAggregate, aggregateErr := singleChronologicalAggregateSQL(
		second.Measure.Function,
		secondMeasure,
	)
	if aggregateErr != nil {
		return compiledRelation{}, compileState{}, nil, nil, aggregateErr
	}
	inputCount := quoteIdentifier(fmt.Sprintf(
		"__os_streamstats_input_count_%d",
		secondStage,
	))
	firstWinner := quoteIdentifier(fmt.Sprintf(
		"__os_streamstats_value_%d",
		firstStage,
	))
	secondWinner := quoteIdentifier(fmt.Sprintf(
		"__os_streamstats_value_%d",
		secondStage,
	))
	preparedAlias := quoteIdentifier(fmt.Sprintf(
		"__os_streamstats_fused_prepared_%d",
		secondStage,
	))
	windowSQL := "SELECT *, count() OVER () AS " + inputCount + ", " +
		firstAggregate + " OVER (" + windowClause + ") AS " + firstWinner +
		", " + secondAggregate + " OVER (" + windowClause + ") AS " +
		secondWinner + " FROM (" + orderedInput + ") AS " + preparedAlias

	windowAlias := quoteIdentifier(fmt.Sprintf(
		"__os_streamstats_fused_window_%d",
		secondStage,
	))
	firstValidation := quoteIdentifier(fmt.Sprintf(
		"__os_streamstats_validation_%d",
		firstStage,
	))
	secondValidation := quoteIdentifier(fmt.Sprintf(
		"__os_streamstats_validation_%d",
		secondStage,
	))
	var transferredValidation []string
	next.chronologicalBarriers, transferredValidation =
		transferDeferredChronologicalValidation(
			next.chronologicalBarriers,
			state.deferredChronologicalValidation,
		)
	allValidation := append(
		append([]string(nil), transferredValidation...),
		firstValidation,
		secondValidation,
	)
	publications := []fusedChronologicalPublication{
		{
			name:             firstOutput.Name,
			valueSQL:         chronologicalPublishedValueSQL(windowAlias + "." + firstWinner),
			storedTypeSQL:    chronologicalPublishedTypeSQL(windowAlias + "." + firstWinner),
			validationColumn: firstValidation,
			validationSQL: "tupleElement(tupleElement(" + windowAlias + "." +
				firstMeasure + ", 1), 4)",
		},
		{
			name:             secondOutput.Name,
			valueSQL:         chronologicalPublishedValueSQL(windowAlias + "." + secondWinner),
			storedTypeSQL:    chronologicalPublishedTypeSQL(windowAlias + "." + secondWinner),
			validationColumn: secondValidation,
			validationSQL: "tupleElement(tupleElement(" + windowAlias + "." +
				secondMeasure + ", 1), 4)",
		},
	}
	projection, projectionErr := fusedChronologicalProjection(
		state,
		next,
		publications,
	)
	if projectionErr != nil {
		return compiledRelation{}, compileState{}, nil, nil, projectionErr
	}
	next.deferredChronologicalValidation = append(
		[]string(nil),
		allValidation...,
	)
	maximumRows := strconv.FormatUint(MaximumStreamStatsInputRows, 10)
	guard := "if(" + windowAlias + "." + inputCount + " > toUInt64(" +
		maximumRows + "), throwIf(toUInt8(1), '" +
		StreamStatsInputLimitMarker + "'), toUInt8(0))"
	resultSQL := "SELECT " + strings.Join(projection, ", ") + " FROM (" +
		windowSQL + ") AS " + windowAlias + " WHERE " + guard + " = 0"
	resultDepth := relation.depth + 3
	enriched := compiledRelation{
		sql:        resultSQL,
		depth:      resultDepth,
		ownerRange: second.Range,
	}

	resultInputName := quoteIdentifier(fmt.Sprintf(
		"__os_streamstats_result_input_%d",
		secondStage,
	))
	barrierName := quoteIdentifier(fmt.Sprintf(
		"__os_streamstats_result_%d",
		secondStage,
	))
	barrierDepth := relationalNodeDepth(resultDepth)
	barrier := &pendingChronologicalBarrier{
		name: barrierName,
		sql:  "SELECT * FROM " + resultInputName,
		prerequisiteDefinitions: []string{
			resultInputName + " AS MATERIALIZED (" + resultSQL + ")",
		},
		validationColumns: allValidation,
		fanout:            1,
		depth:             barrierDepth,
		ownerRange:        second.Range,
	}
	publishedAlias := quoteIdentifier(fmt.Sprintf(
		"__os_streamstats_rows_result_%d",
		secondStage,
	))
	publishedSQL := "SELECT * FROM " + barrierName + " AS " + publishedAlias
	enriched.depth = barrierDepth
	prefixArgs := append(append([]any(nil), firstArgs...), secondArgs...)
	return enriched.selectFrom(publishedSQL, second.Range), next, prefixArgs, barrier, nil
}

// compileStreamAggregate lowers one running count, true-only predicate count,
// numeric sum/average, mixed extremum, or chronological selection over frames in the order already
// established by the pipeline. Its retained relation is capped at one sentinel
// beyond the public limit; row overflow, Dynamic BY poison, and aggregate
// measure poison are forced through the deferred barrier before downstream
// operators can hide them.
func compileStreamAggregate(
	relation compiledRelation,
	operator *plan.StreamAggregate,
	state compileState,
	stage int,
) (compiledRelation, compileState, []any, *pendingChronologicalBarrier, error) {
	output, validateErr := validateStreamAggregate(operator, state)
	if validateErr != nil {
		return compiledRelation{}, compileState{}, nil, nil, validateErr
	}
	function := operator.Measure.Function
	isConditionalCount := function == plan.AggregateFunctionCountPredicate
	isExtrema := function == plan.AggregateFunctionMinimum ||
		function == plan.AggregateFunctionMaximum
	isChronological := function == plan.AggregateFunctionEarliest ||
		function == plan.AggregateFunctionLatest

	orderKeys := append([]compiledSortKey(nil), defaultCompiledOrder(state)...)
	tieBreakers := append([]compiledSortKey(nil), state.tieBreakers...)
	orderProjection := make([]string, 0, len(orderKeys)+len(tieBreakers))
	for index := range orderKeys {
		captured := quoteIdentifier(fmt.Sprintf(
			"__os_streamstats_order_%d_%d",
			stage,
			index,
		))
		orderProjection = append(
			orderProjection,
			orderKeys[index].valueSQL+" AS "+captured,
		)
		orderKeys[index].valueSQL = captured
	}
	for index := range tieBreakers {
		captured := quoteIdentifier(fmt.Sprintf(
			"__os_streamstats_tie_breaker_%d_%d",
			stage,
			index,
		))
		orderProjection = append(
			orderProjection,
			tieBreakers[index].valueSQL+" AS "+captured,
		)
		tieBreakers[index].valueSQL = captured
	}
	orderSQL := ""
	if len(orderKeys) > 0 {
		var orderErr error
		orderSQL, orderErr = compileMaterializedOrder(orderKeys, false)
		if orderErr != nil {
			return compiledRelation{}, compileState{}, nil, nil, fmt.Errorf(
				"compile ClickHouse streamstats order: %w",
				orderErr,
			)
		}
	}

	groupClassifications := make([]string, 0, len(operator.GroupBy))
	groupClassificationAliases := make([]string, 0, len(operator.GroupBy))
	groupAliases := make([]string, 0, len(operator.GroupBy))
	groupPresence := make([]string, 0, len(operator.GroupBy))
	groupUnsupported := make([]string, 0, len(operator.GroupBy))
	groupArgs := make([]any, 0, len(operator.GroupBy)*2)
	for index, group := range operator.GroupBy {
		scalar, compileErr := compileExactScalarGroup(
			group,
			state,
			"streamstats BY",
		)
		if compileErr != nil {
			return compiledRelation{}, compileState{}, nil, nil, compileErr
		}
		classification, classificationArgs := exactScalarGroupClassificationSQL(
			scalar,
		)
		classificationAlias := quoteIdentifier(fmt.Sprintf(
			"__os_streamstats_group_classification_%d",
			index,
		))
		groupAlias := quoteIdentifier(fmt.Sprintf(
			"__os_streamstats_group_%d",
			index,
		))
		groupClassifications = append(
			groupClassifications,
			classification+" AS "+classificationAlias,
		)
		groupClassificationAliases = append(
			groupClassificationAliases,
			classificationAlias,
		)
		groupAliases = append(groupAliases, groupAlias)
		groupPresence = append(
			groupPresence,
			"tupleElement("+classificationAlias+", 2) != 0",
		)
		groupUnsupported = append(
			groupUnsupported,
			"tupleElement("+classificationAlias+", 3) != 0",
		)
		groupArgs = append(groupArgs, classificationArgs...)
	}

	eligibleAlias := quoteIdentifier(fmt.Sprintf(
		"__os_streamstats_eligible_%d",
		stage,
	))
	unsupportedAlias := quoteIdentifier(fmt.Sprintf(
		"__os_streamstats_unsupported_%d",
		stage,
	))
	durableState := state
	predicateState := state
	var predicatePreparation aggregatePredicatePreparation
	if isConditionalCount {
		var preparationErr error
		predicatePreparation, preparationErr = prepareAggregatePredicate(
			state,
			operator.Measure.Predicate,
			stage,
			"streamstats",
		)
		if preparationErr != nil {
			return compiledRelation{}, compileState{}, nil, nil, preparationErr
		}
		durableState = predicatePreparation.durableState
		predicateState = predicatePreparation.predicateState
	}
	measureAlias := ""
	measureProjection := ""
	var stageMeasureArgs []any
	if operator.Measure.Function == plan.AggregateFunctionCountValues ||
		operator.Measure.Function == plan.AggregateFunctionSum ||
		operator.Measure.Function == plan.AggregateFunctionAverage {
		input, exists, resolveErr := resolveCompiledField(operator.Measure.Input, state)
		if resolveErr != nil {
			return compiledRelation{}, compileState{}, nil, nil, resolveErr
		}
		measureSQL := "toUInt64(0)"
		var contributionArgs []any
		switch operator.Measure.Function {
		case plan.AggregateFunctionCountValues:
			if exists {
				measureSQL, contributionArgs = countValueInputSQL(input)
			}
		case plan.AggregateFunctionSum, plan.AggregateFunctionAverage:
			measureSQL = "CAST([], 'Array(Float64)')"
			if exists {
				measureSQL, contributionArgs = numericArrayInputSQL(input)
			}
		}
		measureAlias = quoteIdentifier(fmt.Sprintf(
			"__os_streamstats_measure_%d",
			stage,
		))
		measureProjection = measureSQL + " AS " + measureAlias
		stageMeasureArgs = contributionArgs
	}
	if isConditionalCount {
		predicateSQL, predicateArgs, compileErr := compileExpression(
			operator.Measure.Predicate,
			predicateState,
		)
		if compileErr != nil {
			return compiledRelation{}, compileState{}, nil, nil, compileErr
		}
		measureAlias = quoteIdentifier(fmt.Sprintf(
			"__os_streamstats_measure_%d",
			stage,
		))
		measureProjection = "toUInt64(ifNull(" + predicateSQL + ", 0)) AS " +
			measureAlias
		stageMeasureArgs = predicateArgs
	}

	maximumRows := strconv.FormatUint(MaximumStreamStatsInputRows, 10)
	sentinelRows := strconv.FormatUint(MaximumStreamStatsInputRows+1, 10)
	sourceAlias := quoteIdentifier(fmt.Sprintf(
		"__os_streamstats_source_%d",
		stage,
	))
	orderedInput := "SELECT *"
	if len(groupClassifications) > 0 {
		orderedInput += ", " + strings.Join(groupClassifications, ", ")
	}
	if measureProjection != "" && !isConditionalCount {
		orderedInput += ", " + measureProjection
	}
	if len(orderProjection) > 0 {
		orderedInput += ", " + strings.Join(orderProjection, ", ")
	}
	orderedInput += " FROM (" + relation.sql + ") AS " + sourceAlias
	if orderSQL != "" {
		orderedInput += " ORDER BY " + orderSQL
	}
	orderedInput += " LIMIT " + sentinelRows

	preparedSQL := orderedInput
	preparedLayers := 0
	if len(predicatePreparation.bindings) > 0 {
		// Limit the deterministic input to the public bound plus one sentinel
		// before evaluating calculated predicate producers. A singleton ARRAY
		// JOIN then gives each producer a named dependency that ClickHouse cannot
		// inline back through the predicate without changing row cardinality.
		predicateBindingAlias := quoteIdentifier(fmt.Sprintf(
			"__os_streamstats_predicate_binding_source_%d",
			stage,
		))
		preparedSQL = "SELECT *, " +
			strings.Join(predicatePreparation.boundColumns, ", ") +
			" FROM (" + preparedSQL + ") AS " + predicateBindingAlias +
			" ARRAY JOIN " + strings.Join(predicatePreparation.bindings, ", ")
		preparedLayers++
	}
	if len(predicatePreparation.exactColumns) > 0 {
		// Exact-numeric keys can depend on the singleton aliases above, so keep
		// them in their own post-limit layer before compiling the predicate.
		exactPredicateAlias := quoteIdentifier(fmt.Sprintf(
			"__os_streamstats_predicate_exact_source_%d",
			stage,
		))
		preparedSQL = "SELECT *, " +
			strings.Join(predicatePreparation.exactColumns, ", ") +
			" FROM (" + preparedSQL + ") AS " + exactPredicateAlias
		preparedLayers++
	}
	if len(groupAliases) > 0 {
		preparedProjection := []string{
			"* EXCEPT (" + strings.Join(groupClassificationAliases, ", ") + ")",
		}
		for index, groupAlias := range groupAliases {
			preparedProjection = append(
				preparedProjection,
				"tupleElement("+quoteIdentifier(fmt.Sprintf(
					"__os_streamstats_group_classification_%d",
					index,
				))+", 1) AS "+groupAlias,
			)
		}
		preparedProjection = append(
			preparedProjection,
			"toUInt8("+strings.Join(groupPresence, " AND ")+") AS "+eligibleAlias,
			"toUInt8(("+strings.Join(groupUnsupported, ") OR (")+")) AS "+unsupportedAlias,
		)
		preparedAlias := quoteIdentifier(fmt.Sprintf(
			"__os_streamstats_classified_%d",
			stage,
		))
		preparedSQL = "SELECT " + strings.Join(preparedProjection, ", ") +
			" FROM (" + preparedSQL + ") AS " + preparedAlias
		preparedLayers++
	}
	if isConditionalCount {
		privatePredicateColumns := append(
			append([]string(nil), predicatePreparation.boundColumns...),
			predicatePreparation.exactAliases...,
		)
		predicateProjection := "*"
		if len(privatePredicateColumns) > 0 {
			predicateProjection = "* EXCEPT (" +
				strings.Join(privatePredicateColumns, ", ") + ")"
		}
		predicateAlias := quoteIdentifier(fmt.Sprintf(
			"__os_streamstats_predicate_source_%d",
			stage,
		))
		preparedSQL = "SELECT " + predicateProjection + ", " +
			measureProjection + " FROM (" + preparedSQL + ") AS " + predicateAlias
		preparedLayers++
	}

	outputState := fieldState{
		kind:            fieldKindNumber,
		numberType:      "UInt64",
		numericIntegral: true,
	}
	extremaNullType := ""
	measureValidationSQL := ""
	var extremaScratchColumns []string
	if operator.Measure.Function == plan.AggregateFunctionSum ||
		operator.Measure.Function == plan.AggregateFunctionAverage {
		outputState.numberType = "Float64"
		outputState.numericIntegral = false
	}
	if isExtrema {
		measureAlias = quoteIdentifier(fmt.Sprintf(
			"__os_streamstats_measure_%d",
			stage,
		))
		rowEligibleSQL := "1"
		if len(groupAliases) > 0 {
			rowEligibleSQL = eligibleAlias + " != 0"
		}
		input, exists, resolveErr := resolveCompiledField(operator.Measure.Input, state)
		if resolveErr != nil {
			return compiledRelation{}, compileState{}, nil, nil, resolveErr
		}
		measureSQL := eventStatsExtremaEmptyRowStateSQL("0")
		var measureArgs []any
		outputState = fieldState{
			kind:           fieldKindDynamic,
			dynamicTypeSQL: "dynamicType(" + quoteIdentifier(output.Name) + ")",
			storedTypeSQL: quoteIdentifier(fmt.Sprintf(
				"__os_streamstats_extrema_type_%d",
				stage,
			)),
		}
		if exists {
			outputState.maxStringBytes = fieldStateStringByteBound(input)
			switch input.kind {
			case fieldKindNumber, fieldKindBool, fieldKindTime:
				eligibleSQL, eligibleArgs, fixed := fixedExtremaEligibilitySQL(input)
				if !fixed {
					return compiledRelation{}, compileState{}, nil, nil, errors.New(
						"compile ClickHouse streamstats extrema: fixed input is invalid",
					)
				}
				eligibleSQL = "(" + rowEligibleSQL + ") AND (" + eligibleSQL + ")"
				measureSQL = "tuple(" + input.valueSQL + ", toUInt8(" +
					eligibleSQL + "))"
				measureArgs = eligibleArgs
				outputState = fieldState{
					maxStringBytes:  fieldStateStringByteBound(input),
					kind:            input.kind,
					caseSensitive:   input.caseSensitive,
					numberType:      input.numberType,
					numericSort:     input.numericSort,
					numericIntegral: input.numericIntegral,
				}
				var nullTypeErr error
				extremaNullType, nullTypeErr = nullableEventStatsExtremaType(input)
				if nullTypeErr != nil {
					return compiledRelation{}, compileState{}, nil, nil, nullTypeErr
				}
			case fieldKindString:
				valueAlias := quoteIdentifier(fmt.Sprintf(
					"__os_streamstats_extrema_string_%d",
					stage,
				))
				numberAlias := quoteIdentifier(fmt.Sprintf(
					"__os_streamstats_extrema_number_%d",
					stage,
				))
				valueSQL, valueArgs := statsScalarStringInputSQL(input)
				scalarAlias := quoteIdentifier(fmt.Sprintf(
					"__os_streamstats_extrema_scalar_%d",
					stage,
				))
				preparedSQL = "SELECT *, " + valueSQL + " AS " + valueAlias +
					", " + statsExtremaScalarNumberSQL(valueAlias) + " AS " +
					numberAlias + " FROM (" + preparedSQL + ") AS " + scalarAlias
				preparedLayers++
				extremaScratchColumns = append(
					extremaScratchColumns,
					valueAlias,
					numberAlias,
				)
				measureSQL = "if(" + rowEligibleSQL + ", " +
					statsExtremaScalarCandidateSQL(
						valueAlias,
						numberAlias,
						fixedStringExtremaRawBytesSQL(input),
					) + ", " +
					"tuple(" + eventStatsExtremaEmptyOrderingKeySQL() +
					", toUInt8(" + strconv.Itoa(int(statsExtremaPublicationLexical)) +
					"), toFloat64(0), CAST('' AS String), toUInt8(0)))"
				measureArgs = valueArgs
			case fieldKindDynamic:
				measureSQL, measureArgs = eventStatsExtremaDynamicMeasureSQL(
					operator.Measure.Function,
					input,
					rowEligibleSQL,
				)
				measureValidationSQL = "toUInt8(tupleElement(" + measureAlias + ", 6))"
			case fieldKindStringArray, fieldKindDynamicArray:
				valuesSQL, valuesArgs := stringArrayInputSQL(input)
				measureSQL = streamStatsStringArrayExtremaMeasureSQL(
					operator.Measure.Function,
					valuesSQL,
					rowEligibleSQL,
				)
				measureArgs = valuesArgs
			default:
				return compiledRelation{}, compileState{}, nil, nil, errors.New(
					"compile ClickHouse streamstats extrema: input state is invalid",
				)
			}
		}
		measureStageAlias := quoteIdentifier(fmt.Sprintf(
			"__os_streamstats_measure_source_%d",
			stage,
		))
		measureSourceProjection := "*"
		if len(extremaScratchColumns) > 0 {
			measureSourceProjection = "* EXCEPT (" +
				strings.Join(extremaScratchColumns, ", ") + ")"
		}
		preparedSQL = "SELECT " + measureSourceProjection + ", " + measureSQL + " AS " + measureAlias +
			" FROM (" + preparedSQL + ") AS " + measureStageAlias
		preparedLayers++
		stageMeasureArgs = measureArgs
	}
	if isChronological {
		measureAlias = quoteIdentifier(fmt.Sprintf(
			"__os_streamstats_measure_%d",
			stage,
		))
		rowEligibleSQL := "1"
		if len(groupAliases) > 0 {
			rowEligibleSQL = eligibleAlias + " != 0"
		}
		input, exists, resolveErr := resolveCompiledField(operator.Measure.Input, state)
		if resolveErr != nil {
			return compiledRelation{}, compileState{}, nil, nil, resolveErr
		}
		candidateSQL, candidateArgs, runtimeValidated, candidateErr :=
			singleChronologicalCandidateSQL(
				operator.Measure.Function,
				input,
				exists,
			)
		if candidateErr != nil {
			return compiledRelation{}, compileState{}, nil, nil, candidateErr
		}
		if len(groupAliases) > 0 {
			candidateSQL = "if(" + rowEligibleSQL + ", " + candidateSQL +
				", " + emptySingleChronologicalCandidateSQL() + ")"
		}
		measureSQL := "tuple(" + candidateSQL + ", " +
			immutableChronologicalRowKeySQL() + ")"
		measureStageAlias := quoteIdentifier(fmt.Sprintf(
			"__os_streamstats_measure_source_%d",
			stage,
		))
		preparedSQL = "SELECT *, " + measureSQL + " AS " + measureAlias +
			" FROM (" + preparedSQL + ") AS " + measureStageAlias
		preparedLayers++
		stageMeasureArgs = candidateArgs
		outputState = fieldState{
			kind:           fieldKindDynamic,
			dynamicTypeSQL: "dynamicType(" + quoteIdentifier(output.Name) + ")",
			storedTypeSQL: quoteIdentifier(fmt.Sprintf(
				"__os_streamstats_chronological_type_%d",
				stage,
			)),
		}
		if exists {
			outputState.maxStringBytes = fieldStateStringByteBound(input)
		}
		if runtimeValidated {
			measureValidationSQL = "toUInt8(tupleElement(tupleElement(" +
				measureAlias + ", 1), 4))"
		}
	}

	// Extrema and chronological projections are textually outside the bounded BY
	// classification subquery, so its placeholders precede group arguments.
	prefixArgs := make([]any, 0, len(groupArgs)+len(stageMeasureArgs))
	if isConditionalCount || isExtrema || isChronological {
		prefixArgs = append(prefixArgs, stageMeasureArgs...)
		prefixArgs = append(prefixArgs, groupArgs...)
	} else {
		prefixArgs = append(prefixArgs, groupArgs...)
		prefixArgs = append(prefixArgs, stageMeasureArgs...)
	}

	inputName := quoteIdentifier(fmt.Sprintf(
		"__os_streamstats_input_%d",
		stage,
	))
	inputCount := quoteIdentifier(fmt.Sprintf(
		"__os_streamstats_input_count_%d",
		stage,
	))
	windowValue := quoteIdentifier(fmt.Sprintf(
		"__os_streamstats_value_%d",
		stage,
	))
	windowParts := make([]string, 0, 3)
	if len(groupAliases) > 0 {
		partition := append([]string{eligibleAlias}, groupAliases...)
		windowParts = append(
			windowParts,
			"PARTITION BY "+strings.Join(partition, ", "),
		)
	}
	if orderSQL != "" {
		windowParts = append(windowParts, "ORDER BY "+orderSQL)
	} else {
		// A supported relation without order keys is a global aggregate and has
		// at most one row. Pin a syntactically complete window order without
		// pretending that a wider unordered relation is deterministic.
		windowParts = append(windowParts, "ORDER BY tuple()")
	}
	windowParts = append(
		windowParts,
		streamStatsFrameSQL(operator.IncludeCurrent, operator.WindowRows),
	)
	windowClause := strings.Join(windowParts, " ")
	windowExpression := "count() OVER (" + windowClause + ")"
	switch operator.Measure.Function {
	case plan.AggregateFunctionCountValues, plan.AggregateFunctionCountPredicate:
		windowExpression = "toUInt64(ifNull(sum(toUInt128(" + measureAlias +
			")) OVER (" + windowClause + "), toUInt128(0)))"
	case plan.AggregateFunctionSum, plan.AggregateFunctionAverage:
		numericAggregate, supported := numericArrayAggregateSQL(
			operator.Measure.Function,
			measureAlias,
		)
		if !supported {
			return compiledRelation{}, compileState{}, nil, nil, errors.New(
				"compile ClickHouse streamstats: numeric aggregate is unsupported",
			)
		}
		windowExpression = "CAST(" + numericAggregate + " OVER (" +
			windowClause + ") AS Nullable(Float64))"
	case plan.AggregateFunctionMinimum, plan.AggregateFunctionMaximum:
		if extremaNullType != "" {
			aggregateName := "minIfOrNull"
			if operator.Measure.Function == plan.AggregateFunctionMaximum {
				aggregateName = "maxIfOrNull"
			}
			windowExpression = aggregateName + "(tupleElement(" + measureAlias +
				", 1), tupleElement(" + measureAlias + ", 2) != 0) OVER (" +
				windowClause + ")"
		} else {
			windowExpression = statsExtremaScalarAggregateWinnerSQL(
				operator.Measure.Function,
				measureAlias,
			) + " OVER (" + windowClause + ")"
		}
	case plan.AggregateFunctionEarliest, plan.AggregateFunctionLatest:
		chronologicalAggregate, aggregateErr := singleChronologicalAggregateSQL(
			operator.Measure.Function,
			measureAlias,
		)
		if aggregateErr != nil {
			return compiledRelation{}, compileState{}, nil, nil, aggregateErr
		}
		windowExpression = chronologicalAggregate + " OVER (" + windowClause + ")"
	default:
		if !operator.IncludeCurrent {
			windowExpression = "ifNull(" + windowExpression + ", toUInt64(0))"
		}
	}

	windowProjection := []string{
		"*",
		"count() OVER () AS " + inputCount,
		windowExpression + " AS " + windowValue,
	}
	unsupportedAny := ""
	if len(groupAliases) > 0 {
		unsupportedAny = quoteIdentifier(fmt.Sprintf(
			"__os_streamstats_any_unsupported_%d",
			stage,
		))
		windowProjection = append(
			windowProjection,
			"max(toUInt8("+unsupportedAlias+" != 0)) OVER () AS "+unsupportedAny,
		)
	}
	validationColumn := ""
	if measureValidationSQL != "" {
		validationColumn = quoteIdentifier(fmt.Sprintf(
			"__os_streamstats_validation_%d",
			stage,
		))
		windowProjection = append(
			windowProjection,
			measureValidationSQL+" AS "+validationColumn,
		)
	}
	windowAlias := quoteIdentifier(fmt.Sprintf(
		"__os_streamstats_window_%d",
		stage,
	))
	windowSource := inputName
	if measureValidationSQL != "" {
		preparedAlias := quoteIdentifier(fmt.Sprintf(
			"__os_streamstats_prepared_%d",
			stage,
		))
		windowSource = "(" + preparedSQL + ") AS " + preparedAlias
	}
	windowSQL := "SELECT " + strings.Join(windowProjection, ", ") +
		" FROM " + windowSource

	next := streamAggregateCompileState(
		durableState,
		output,
		outputState,
		len(groupAliases) > 0,
		stage,
		orderKeys,
		tieBreakers,
	)
	rawOutputValue := windowAlias + "." + windowValue
	outputValue := rawOutputValue
	outputStoredType := ""
	usesDynamicExtrema := isExtrema && extremaNullType == ""
	if usesDynamicExtrema {
		outputValue = statsExtremaScalarValueSQL(outputValue)
		outputStoredType = statsExtremaScalarStoredTypeSQL(
			rawOutputValue,
		)
	}
	if isChronological {
		outputValue = chronologicalPublishedValueSQL(rawOutputValue)
		outputStoredType = chronologicalPublishedTypeSQL(rawOutputValue)
	}
	outputExists := "1"
	if len(groupAliases) > 0 {
		outputExists = windowAlias + "." + eligibleAlias + " != 0"
		nullType := "UInt64"
		if operator.Measure.Function == plan.AggregateFunctionSum ||
			operator.Measure.Function == plan.AggregateFunctionAverage {
			nullType = "Float64"
		}
		if isExtrema {
			if extremaNullType != "" {
				nullType = extremaNullType
				outputValue = "if(" + outputExists + ", " + outputValue +
					", CAST(NULL AS Nullable(" + nullType + ")))"
			}
		}
		if usesDynamicExtrema || isChronological {
			outputValue = "if(" + outputExists + ", " + outputValue +
				", CAST(NULL AS Dynamic))"
			outputStoredType = "if(" + outputExists + ", " +
				outputStoredType + ", toUInt8(" +
				strconv.Itoa(int(eventfields.StoredValueTypeNull)) + "))"
		} else if !isExtrema {
			outputValue = "if(" + outputExists + ", " + outputValue +
				", CAST(NULL AS Nullable(" + nullType + ")))"
		}
	}
	outputValidation := ""
	if validationColumn != "" {
		outputValidation = windowAlias + "." + validationColumn
	}
	projection := eventAggregateProjection(
		durableState,
		next,
		output.Name,
		outputValue,
		outputStoredType,
		outputExists,
		validationColumn,
		outputValidation,
		windowAlias,
	)
	guard := "if(" + windowAlias + "." + inputCount + " > toUInt64(" +
		maximumRows + "), throwIf(toUInt8(1), '" +
		StreamStatsInputLimitMarker + "'), toUInt8(0))"
	if unsupportedAny != "" {
		guard = "if(" + windowAlias + "." + inputCount + " > toUInt64(" +
			maximumRows + "), throwIf(toUInt8(1), '" +
			StreamStatsInputLimitMarker + "'), if(" + windowAlias + "." +
			unsupportedAny + " != 0, throwIf(toUInt8(1), '" +
			UnsupportedStatsByValueMarker + "'), toUInt8(0)))"
	}
	resultSQL := "SELECT " + strings.Join(projection, ", ") + " FROM (" +
		windowSQL + ") AS " + windowAlias + " WHERE " + guard + " = 0"

	depth := relation.depth + 3 + preparedLayers
	enriched := compiledRelation{
		sql:        resultSQL,
		depth:      depth,
		ownerRange: operator.Range,
	}
	barrierName := quoteIdentifier(fmt.Sprintf(
		"__os_streamstats_result_%d",
		stage,
	))
	barrierSQL := resultSQL
	barrierDepth := depth
	prerequisiteDefinitions := []string{
		inputName + " AS MATERIALIZED (" + preparedSQL + ")",
	}
	if validationColumn != "" {
		resultInputName := quoteIdentifier(fmt.Sprintf(
			"__os_streamstats_result_input_%d",
			stage,
		))
		barrierSQL = "SELECT * FROM " + resultInputName
		barrierDepth = relationalNodeDepth(depth)
		prerequisiteDefinitions = []string{
			resultInputName + " AS MATERIALIZED (" + resultSQL + ")",
		}
		enriched.depth = barrierDepth
	}
	barrier := &pendingChronologicalBarrier{
		name:                    barrierName,
		sql:                     barrierSQL,
		prerequisiteDefinitions: prerequisiteDefinitions,
		fanout:                  1,
		depth:                   barrierDepth,
		ownerRange:              operator.Range,
	}
	if validationColumn != "" {
		barrier.validationColumns = []string{validationColumn}
	}
	publishedAlias := quoteIdentifier(fmt.Sprintf(
		"__os_streamstats_rows_result_%d",
		stage,
	))
	publishedSQL := "SELECT * FROM " + barrierName + " AS " + publishedAlias
	if validationColumn != "" {
		publishedSQL = "SELECT * EXCEPT (" + validationColumn + ") FROM " +
			barrierName + " AS " + publishedAlias
	}
	return enriched.selectFrom(publishedSQL, operator.Range), next, prefixArgs, barrier, nil
}

func eventAggregateMeasureSpecFor(
	measure plan.AggregateMeasure,
) (eventAggregateMeasureSpec, error) {
	spec := eventAggregateMeasureSpec{
		function:        measure.Function,
		percentile:      measure.Percentile,
		numberType:      "UInt64",
		numericIntegral: true,
	}
	switch measure.Function {
	case plan.AggregateFunctionCountRows:
		return spec, nil
	case plan.AggregateFunctionCountValues,
		plan.AggregateFunctionCountPredicate:
		spec.materialized = true
		spec.valuePrefix = "__os_eventstats_value_count_"
		return spec, nil
	case plan.AggregateFunctionDistinctCount:
		spec.materialized = true
		spec.valuePrefix = "__os_eventstats_value_dc_"
		return spec, nil
	case plan.AggregateFunctionValues:
		spec.materialized = true
		spec.numberType = ""
		spec.numericIntegral = false
		spec.valuePrefix = "__os_eventstats_value_values_"
		return spec, nil
	case plan.AggregateFunctionList:
		spec.materialized = true
		spec.numberType = ""
		spec.numericIntegral = false
		spec.valuePrefix = "__os_eventstats_value_list_"
		return spec, nil
	case plan.AggregateFunctionPercentile:
		if measure.Percentile < 1 || measure.Percentile > 99 {
			return eventAggregateMeasureSpec{}, fmt.Errorf(
				"compile ClickHouse eventstats: invalid percentile level %d",
				measure.Percentile,
			)
		}
		spec.materialized = true
		spec.numberType = "Float64"
		spec.numericIntegral = false
		spec.valuePrefix = "__os_eventstats_value_percentile_"
		return spec, nil
	case plan.AggregateFunctionSum, plan.AggregateFunctionAverage:
		spec.materialized = true
		spec.numberType = "Float64"
		spec.numericIntegral = false
		if measure.Function == plan.AggregateFunctionSum {
			spec.valuePrefix = "__os_eventstats_value_sum_"
		} else {
			spec.valuePrefix = "__os_eventstats_value_avg_"
		}
		return spec, nil
	case plan.AggregateFunctionMinimum, plan.AggregateFunctionMaximum:
		spec.materialized = true
		spec.numberType = ""
		spec.numericIntegral = false
		if measure.Function == plan.AggregateFunctionMinimum {
			spec.valuePrefix = "__os_eventstats_value_min_"
		} else {
			spec.valuePrefix = "__os_eventstats_value_max_"
		}
		return spec, nil
	case plan.AggregateFunctionEarliest, plan.AggregateFunctionLatest:
		spec.materialized = true
		spec.numberType = ""
		spec.numericIntegral = false
		if measure.Function == plan.AggregateFunctionEarliest {
			spec.valuePrefix = "__os_eventstats_value_earliest_"
		} else {
			spec.valuePrefix = "__os_eventstats_value_latest_"
		}
		return spec, nil
	default:
		return eventAggregateMeasureSpec{}, fmt.Errorf(
			"compile ClickHouse eventstats: unsupported function %d",
			measure.Function,
		)
	}
}

func (spec eventAggregateMeasureSpec) aggregateSQL(
	inputSQL string,
) (string, error) {
	switch spec.function {
	case plan.AggregateFunctionCountValues,
		plan.AggregateFunctionCountPredicate:
		return "toUInt64(sum(toUInt128(" + inputSQL + ")))", nil
	case plan.AggregateFunctionDistinctCount:
		return distinctCountCardinalitySQL(
			"tupleElement(" + inputSQL + ", 1)",
		), nil
	case plan.AggregateFunctionValues:
		return exactDistinctStringSetSQL(
			"tupleElement("+inputSQL+", 1)",
			uint64(MaximumStatsValuesPerGroup),
		), nil
	case plan.AggregateFunctionList:
		return boundedOrderedStringListSQL(
			"tupleElement(" + inputSQL + ", 1)",
		), nil
	case plan.AggregateFunctionPercentile:
		return singlePercentileArrayAggregateSQL(spec.percentile, inputSQL), nil
	case plan.AggregateFunctionSum, plan.AggregateFunctionAverage:
		if sql, supported := numericArrayAggregateSQL(spec.function, inputSQL); supported {
			return sql, nil
		}
	}
	return "", fmt.Errorf(
		"compile ClickHouse eventstats: function %d has no materialized measure",
		spec.function,
	)
}

func nullableEventStatsExtremaType(field fieldState) (string, error) {
	switch field.kind {
	case fieldKindNumber:
		if field.numberType != "" {
			return field.numberType, nil
		}
	case fieldKindBool:
		return "Bool", nil
	case fieldKindTime:
		if field.numberType != "" {
			return field.numberType, nil
		}
	}
	return "", fmt.Errorf(
		"compile ClickHouse eventstats extrema: fixed input has unsupported type %d/%q",
		field.kind,
		field.numberType,
	)
}

// fixedExtremaEligibilitySQL is the common row contract for native extrema.
// Keeping fixed Number, Bool, and Time values in their physical type avoids a
// lossy String/Float64 round trip; only present, non-null values participate,
// and non-finite floating-point numbers are omitted.
func fixedExtremaEligibilitySQL(field fieldState) (string, []any, bool) {
	switch field.kind {
	case fieldKindNumber, fieldKindBool, fieldKindTime:
	default:
		return "", nil, false
	}
	existsSQL := field.existsSQL
	if existsSQL == "" {
		existsSQL = "1"
	}
	eligibleSQL := "(" + existsSQL + ") AND isNotNull(" + field.valueSQL + ")"
	if field.kind == fieldKindNumber && strings.HasPrefix(field.numberType, "Float") {
		eligibleSQL += " AND isFinite(" + field.valueSQL + ")"
	}
	return eligibleSQL, append([]any(nil), field.existsArgs...), true
}

func compileEventAggregate(
	relation compiledRelation,
	operator *plan.EventAggregate,
	state compileState,
	stage int,
) (compiledRelation, compileState, []any, *pendingChronologicalBarrier, error) {
	output, validateErr := validateEventAggregate(operator, state)
	if validateErr != nil {
		return compiledRelation{}, compileState{}, nil, nil, validateErr
	}
	if state.eventRows && state.allowDynamic && output.Name == "fields" {
		return compiledRelation{}, compileState{}, nil, nil, &plan.Diagnostic{
			Code: "SPL_AMBIGUOUS_EVENTSTATS_FIELD",
			Message: "eventstats cannot replace the event result's " +
				"reserved fields payload without an exact upstream schema",
			Range: output.Range,
		}
	}

	measure := operator.Measure
	measureSpec, specErr := eventAggregateMeasureSpecFor(measure)
	if specErr != nil {
		return compiledRelation{}, compileState{}, nil, nil, specErr
	}
	measureAggregateSQL := measureSpec.aggregateSQL
	measurePublishValueSQL := func(valueSQL string) string { return valueSQL }
	measurePublishTypeSQL := func(string) string { return "" }
	measureNullSQL := ""
	if measureSpec.numberType != "" {
		measureNullSQL = "CAST(NULL AS Nullable(" + measureSpec.numberType + "))"
	}
	outputState := fieldState{
		kind:            fieldKindNumber,
		numberType:      measureSpec.numberType,
		numericIntegral: measureSpec.numericIntegral,
	}
	var measureInputColumns []string
	measureInputSQL := ""
	var measureInputArgs []any
	var measureValidationSQL func(string, string) string
	measureUsesValuesValidation := false
	listInputExists := false
	measureUsesGroupEligibility := false
	var eventStatsPrerequisiteDefinitions []string
	prefixArgumentsAfterExisting := false
	durableState := state
	switch measure.Function {
	case plan.AggregateFunctionCountPredicate:
		sentinelRows := strconv.FormatUint(MaximumEventStatsInputRows+1, 10)
		predicatePreparation, preparationErr := prepareAggregatePredicate(
			state,
			measure.Predicate,
			stage,
			"eventstats",
		)
		if preparationErr != nil {
			return compiledRelation{}, compileState{}, nil, nil, preparationErr
		}
		durableState = predicatePreparation.durableState
		predicateState := predicatePreparation.predicateState
		predicateColumns := append(
			append([]string(nil), predicatePreparation.boundColumns...),
			predicatePreparation.exactColumns...,
		)
		if len(predicateColumns) > 0 {
			materialized := quoteIdentifier(fmt.Sprintf(
				"__os_eventstats_predicate_input_%d",
				stage,
			))
			alias := quoteIdentifier(fmt.Sprintf("__os_eventstats_predicate_rows_%d", stage))
			predicateSQL := "SELECT *, " + strings.Join(predicateColumns, ", ") +
				" FROM (" + relation.sql + ") AS " + alias
			if len(predicatePreparation.bindings) > 0 {
				predicateSQL += " ARRAY JOIN " + strings.Join(predicatePreparation.bindings, ", ")
			}
			predicateSQL += " LIMIT " + sentinelRows
			eventStatsPrerequisiteDefinitions = append(
				eventStatsPrerequisiteDefinitions,
				materialized+" AS MATERIALIZED ("+predicateSQL+")",
			)
			relation = relation.selectFrom(
				"SELECT * FROM "+materialized,
				operator.Range,
			)
			// Hoisting moves the predicate fence ahead of the eventstats input
			// definition. Its already-compiled relation arguments must therefore
			// precede the predicate/group arguments introduced by this stage.
			prefixArgumentsAfterExisting = true
			if err := validateRelationalDepth(relation.depth, relation.ownerRange); err != nil {
				return compiledRelation{}, compileState{}, nil, nil, err
			}
		}
		predicateSQL, predicateArgs, compileErr := compileExpression(
			measure.Predicate,
			predicateState,
		)
		if compileErr != nil {
			return compiledRelation{}, compileState{}, nil, nil, compileErr
		}
		measureInputSQL = "toUInt64(ifNull(" + predicateSQL + ", 0))"
		measureInputArgs = predicateArgs
	case plan.AggregateFunctionCountValues, plan.AggregateFunctionPercentile,
		plan.AggregateFunctionSum,
		plan.AggregateFunctionAverage, plan.AggregateFunctionMinimum,
		plan.AggregateFunctionMaximum, plan.AggregateFunctionEarliest,
		plan.AggregateFunctionLatest, plan.AggregateFunctionDistinctCount,
		plan.AggregateFunctionValues, plan.AggregateFunctionList:
		if durableState.eventRows && durableState.allowDynamic && measure.Input.Name == "fields" {
			return compiledRelation{}, compileState{}, nil, nil, &plan.Diagnostic{
				Code: "SPL_AMBIGUOUS_EVENTSTATS_FIELD",
				Message: "eventstats cannot read the event result's " +
					"reserved fields payload without an exact upstream schema",
				Range: measure.Input.Range,
			}
		}
		input, exists, resolveErr := resolveCompiledField(measure.Input, durableState)
		if resolveErr != nil {
			return compiledRelation{}, compileState{}, nil, nil, resolveErr
		}
		switch measure.Function {
		case plan.AggregateFunctionCountValues:
			measureInputSQL = "toUInt64(0)"
			if exists {
				measureInputSQL, measureInputArgs = countValueInputSQL(input)
			}
		case plan.AggregateFunctionPercentile, plan.AggregateFunctionSum,
			plan.AggregateFunctionAverage:
			measureInputSQL = "CAST([], 'Array(Float64)')"
			if exists {
				measureInputSQL, measureInputArgs = numericArrayInputSQL(input)
			}
		case plan.AggregateFunctionDistinctCount, plan.AggregateFunctionValues,
			plan.AggregateFunctionList:
			emptyValues := "CAST([], 'Array(String)')"
			measureInputSQL = "tuple(" + emptyValues + ", toUInt8(0))"
			if exists {
				if input.kind == fieldKindDynamic {
					rowEligibleSQL := "1"
					if len(operator.GroupBy) > 0 {
						rowEligibleSQL = quoteIdentifier(fmt.Sprintf(
							"__os_eventstats_eligible_%d",
							stage,
						)) + " != 0"
						measureUsesGroupEligibility = true
					}
					measureInputSQL, measureInputArgs =
						eventStatsExactStringDynamicMeasureSQL(
							input,
							rowEligibleSQL,
						)
				} else {
					valuesSQL, valuesArgs := stringArrayInputSQL(input)
					measureInputSQL = "tuple(" + valuesSQL + ", toUInt8(0))"
					measureInputArgs = valuesArgs
				}
			}
			switch measure.Function {
			case plan.AggregateFunctionDistinctCount:
				measureValidationSQL = eventStatsDistinctCountValidationSQL
			case plan.AggregateFunctionValues:
				measureUsesValuesValidation = true
				measureNullSQL = emptyValues
				outputState = fieldState{
					kind:                  fieldKindStringArray,
					mvSortedLexicographic: true,
					stringOrBytes:         true,
				}
			case plan.AggregateFunctionList:
				listInputExists = exists
				measureNullSQL = emptyValues
				outputState = fieldState{
					kind:          fieldKindStringArray,
					stringOrBytes: true,
				}
			default:
				return compiledRelation{}, compileState{}, nil, nil, fmt.Errorf(
					"compile ClickHouse eventstats: exact-string function %d has no publication contract",
					measure.Function,
				)
			}
		case plan.AggregateFunctionMinimum, plan.AggregateFunctionMaximum:
			extremaFunction := measure.Function
			outputState = fieldState{
				kind:           fieldKindDynamic,
				dynamicTypeSQL: "dynamicType(" + quoteIdentifier(output.Name) + ")",
			}
			measureNullSQL = "CAST(NULL AS Dynamic)"
			measurePublishTypeSQL = statsExtremaStoredTypeSQL
			if exists {
				outputState.maxStringBytes = fieldStateStringByteBound(input)
			}
			switch {
			case exists && (input.kind == fieldKindNumber ||
				input.kind == fieldKindBool || input.kind == fieldKindTime):
				eligibleSQL, eligibleArgs, fixed := fixedExtremaEligibilitySQL(input)
				if !fixed {
					return compiledRelation{}, compileState{}, nil, nil, errors.New(
						"compile ClickHouse eventstats extrema: fixed input is invalid",
					)
				}
				measureInputSQL = "tuple(" + input.valueSQL + ", toUInt8(" +
					eligibleSQL + "))"
				measureInputArgs = eligibleArgs
				measureAggregateSQL = func(inputSQL string) (string, error) {
					aggregateName := "minIfOrNull"
					if extremaFunction == plan.AggregateFunctionMaximum {
						aggregateName = "maxIfOrNull"
					}
					return aggregateName + "(tupleElement(" + inputSQL +
						", 1), tupleElement(" + inputSQL + ", 2) != 0)", nil
				}
				outputState = fieldState{
					maxStringBytes:  fieldStateStringByteBound(input),
					kind:            input.kind,
					caseSensitive:   input.caseSensitive,
					numberType:      input.numberType,
					numericSort:     input.numericSort,
					numericIntegral: input.numericIntegral,
				}
				nullType, nullTypeErr := nullableEventStatsExtremaType(input)
				if nullTypeErr != nil {
					return compiledRelation{}, compileState{}, nil, nil, nullTypeErr
				}
				measureNullSQL = "CAST(NULL AS Nullable(" + nullType + "))"
				measurePublishTypeSQL = func(string) string { return "" }
			case exists && input.kind == fieldKindString:
				valueAlias := quoteIdentifier(fmt.Sprintf(
					"__os_eventstats_extrema_string_%d",
					stage,
				))
				numberAlias := quoteIdentifier(fmt.Sprintf(
					"__os_eventstats_extrema_number_%d",
					stage,
				))
				valueSQL, valueArgs := statsScalarStringInputSQL(input)
				measureInputColumns = append(
					measureInputColumns,
					valueSQL+" AS "+valueAlias,
					statsExtremaScalarNumberSQL(valueAlias)+" AS "+numberAlias,
				)
				measureInputArgs = valueArgs
				measureInputSQL = statsExtremaScalarCandidateSQL(
					valueAlias,
					numberAlias,
					fixedStringExtremaRawBytesSQL(input),
				)
				measureAggregateSQL = func(inputSQL string) (string, error) {
					return statsExtremaScalarAggregateWinnerSQL(
						extremaFunction,
						inputSQL,
					), nil
				}
				measurePublishValueSQL = statsExtremaScalarValueSQL
				measurePublishTypeSQL = statsExtremaScalarStoredTypeSQL
			case exists && input.kind == fieldKindDynamic:
				rowEligibleSQL := "1"
				if len(operator.GroupBy) > 0 {
					rowEligibleSQL = quoteIdentifier(fmt.Sprintf(
						"__os_eventstats_eligible_%d",
						stage,
					)) + " != 0"
					measureUsesGroupEligibility = true
				}
				measureInputSQL, measureInputArgs =
					eventStatsExtremaDynamicMeasureSQL(
						extremaFunction,
						input,
						rowEligibleSQL,
					)
				measureAggregateSQL = func(inputSQL string) (string, error) {
					return statsExtremaScalarAggregateWinnerSQL(
						extremaFunction,
						inputSQL,
					), nil
				}
				measurePublishValueSQL = statsExtremaScalarValueSQL
				measurePublishTypeSQL = statsExtremaScalarStoredTypeSQL
				measureValidationSQL = func(inputSQL, _ string) string {
					return "maxOrDefault(toUInt8(tupleElement(" + inputSQL +
						", 6)))"
				}
			default:
				valuesSQL := "CAST([], 'Array(String)')"
				if exists {
					valuesSQL, measureInputArgs = stringArrayInputSQL(input)
				}
				measureInputSQL = statsExtremaCandidatesSQL(valuesSQL)
				measureAggregateSQL = func(inputSQL string) (string, error) {
					return statsExtremaAggregateSQL(
						extremaFunction,
						inputSQL,
					), nil
				}
			}
		case plan.AggregateFunctionEarliest, plan.AggregateFunctionLatest:
			chronologicalFunction := measure.Function
			candidateSQL, candidateArgs, runtimeValidated, candidateErr :=
				singleChronologicalCandidateSQL(
					chronologicalFunction,
					input,
					exists,
				)
			if candidateErr != nil {
				return compiledRelation{}, compileState{}, nil, nil, candidateErr
			}
			if len(operator.GroupBy) > 0 {
				rowEligibleSQL := quoteIdentifier(fmt.Sprintf(
					"__os_eventstats_eligible_%d",
					stage,
				)) + " != 0"
				candidateSQL = "if(" + rowEligibleSQL + ", " + candidateSQL +
					", " + emptySingleChronologicalCandidateSQL() + ")"
				measureUsesGroupEligibility = true
			}
			measureInputSQL = "tuple(" + candidateSQL + ", " +
				immutableChronologicalRowKeySQL() + ")"
			measureInputArgs = candidateArgs
			measureAggregateSQL = func(inputSQL string) (string, error) {
				return singleChronologicalAggregateSQL(
					chronologicalFunction,
					inputSQL,
				)
			}
			outputState = fieldState{
				kind:           fieldKindDynamic,
				dynamicTypeSQL: "dynamicType(" + quoteIdentifier(output.Name) + ")",
			}
			if exists {
				outputState.maxStringBytes = fieldStateStringByteBound(input)
			}
			measureNullSQL = "CAST(NULL AS Dynamic)"
			measurePublishValueSQL = chronologicalPublishedValueSQL
			measurePublishTypeSQL = chronologicalPublishedTypeSQL
			if runtimeValidated {
				measureValidationSQL = eventStatsChronologicalValidationSQL
			}
		}
	}

	groups := make([]compiledEventStatsGroup, 0, len(operator.GroupBy))
	seenGroups := make(map[string]struct{}, len(operator.GroupBy))
	for index, group := range operator.GroupBy {
		if validateErr := validateCanonicalFieldRef("eventstats", "group", group); validateErr != nil {
			return compiledRelation{}, compileState{}, nil, nil, validateErr
		}
		if _, duplicate := seenGroups[group.Name]; duplicate {
			return compiledRelation{}, compileState{}, nil, nil, fmt.Errorf(
				"compile ClickHouse eventstats: grouping field %q is repeated",
				group.Name,
			)
		}
		seenGroups[group.Name] = struct{}{}
		if durableState.eventRows && durableState.allowDynamic && group.Name == "fields" {
			return compiledRelation{}, compileState{}, nil, nil, &plan.Diagnostic{
				Code:    "SPL_AMBIGUOUS_EVENTSTATS_FIELD",
				Message: "eventstats cannot group by the event result's reserved fields payload without an exact upstream schema",
				Range:   group.Range,
			}
		}

		scalar, compileErr := compileExactScalarGroup(group, durableState, "eventstats BY")
		if compileErr != nil {
			return compiledRelation{}, compileState{}, nil, nil, compileErr
		}
		groups = append(groups, compiledEventStatsGroup{
			scalar:   scalar,
			keyAlias: quoteIdentifier(fmt.Sprintf("__os_eventstats_group_%d", index)),
		})
	}
	if outputState.kind == fieldKindDynamic {
		outputState.storedTypeSQL = quoteIdentifier(fmt.Sprintf(
			"__os_eventstats_extrema_type_%d",
			stage,
		))
	}
	next := eventAggregateCompileState(
		durableState,
		output,
		outputState,
		len(groups) > 0,
		stage,
	)
	inputName := quoteIdentifier(fmt.Sprintf("__os_eventstats_input_%d", stage))
	totalName := quoteIdentifier(fmt.Sprintf("__os_eventstats_total_%d", stage))
	inputAlias := quoteIdentifier(fmt.Sprintf("__os_eventstats_rows_%d", stage))
	totalAlias := quoteIdentifier(fmt.Sprintf("__os_eventstats_total_row_%d", stage))
	totalColumn := quoteIdentifier(fmt.Sprintf("__os_eventstats_input_count_%d", stage))
	validationColumn := ""
	if measureValidationSQL != nil || measureUsesValuesValidation ||
		listInputExists {
		validationColumn = quoteIdentifier(fmt.Sprintf(
			"__os_eventstats_validation_%d",
			stage,
		))
	}
	maximumRows := strconv.FormatUint(MaximumEventStatsInputRows, 10)
	sentinelRows := strconv.FormatUint(MaximumEventStatsInputRows+1, 10)
	windowedDynamicExtrema := (measure.Function == plan.AggregateFunctionMinimum ||
		measure.Function == plan.AggregateFunctionMaximum) &&
		outputState.kind == fieldKindDynamic && measureValidationSQL != nil

	inputProjection := []string{"*"}
	classificationProjection := []string{"*"}
	var classificationArgs []any
	measureAlias := ""
	if measureSpec.materialized {
		measureAlias = quoteIdentifier(fmt.Sprintf("__os_eventstats_measure_%d", stage))
		inputProjection = append(inputProjection, measureInputColumns...)
		if !measureUsesGroupEligibility {
			inputProjection = append(
				inputProjection,
				measureInputSQL+" AS "+measureAlias,
			)
		}
	}
	var eligibilityArgs, unsupportedArgs []any
	eligibility := make([]string, 0, len(groups))
	unsupported := make([]string, 0, len(groups))
	for _, group := range groups {
		if windowedDynamicExtrema {
			classification, args := exactScalarGroupClassificationSQL(group.scalar)
			classificationProjection = append(
				classificationProjection,
				classification+" AS "+group.keyAlias,
			)
			classificationArgs = append(classificationArgs, args...)
			eligibility = append(
				eligibility,
				"tupleElement("+group.keyAlias+", 2) != 0",
			)
			unsupported = append(
				unsupported,
				"tupleElement("+group.keyAlias+", 3) != 0",
			)
			continue
		}
		inputProjection = append(
			inputProjection,
			group.scalar.keySQL+" AS "+group.keyAlias,
		)
		eligibility = append(eligibility, group.scalar.presenceSQL)
		eligibilityArgs = append(eligibilityArgs, group.scalar.presenceArgs...)
		if group.scalar.unsupportedSQL != "" {
			unsupported = append(unsupported, group.scalar.unsupportedSQL)
			unsupportedArgs = append(unsupportedArgs, group.scalar.unsupportedArgs...)
		}
	}
	eligibleAlias := quoteIdentifier(fmt.Sprintf("__os_eventstats_eligible_%d", stage))
	unsupportedAlias := quoteIdentifier(fmt.Sprintf("__os_eventstats_unsupported_%d", stage))
	if len(groups) > 0 {
		inputProjection = append(
			inputProjection,
			"toUInt8("+strings.Join(eligibility, " AND ")+") AS "+eligibleAlias,
		)
		unsupportedSQL := "0"
		if len(unsupported) > 0 {
			unsupportedSQL = "(" + strings.Join(unsupported, ") OR (") + ")"
		}
		inputProjection = append(
			inputProjection,
			"toUInt8("+unsupportedSQL+") AS "+unsupportedAlias,
		)
	}
	if measureSpec.materialized && measureUsesGroupEligibility {
		// Keep the BY eligibility alias textually before the Dynamic fold that it
		// guards. ClickHouse aliases are visible throughout a SELECT projection;
		// the pinned integration suite proves that this reference also preserves
		// short-circuit traversal for incomplete group rows.
		inputProjection = append(
			inputProjection,
			measureInputSQL+" AS "+measureAlias,
		)
	}

	inputSourceAlias := quoteIdentifier(fmt.Sprintf("__os_eventstats_source_%d", stage))
	inputSourceSQL := relation.sql
	inputLimitSQL := " LIMIT " + sentinelRows
	if windowedDynamicExtrema && len(groups) > 0 {
		classificationAlias := quoteIdentifier(fmt.Sprintf(
			"__os_eventstats_group_source_%d",
			stage,
		))
		inputSourceSQL = "SELECT " + strings.Join(classificationProjection, ", ") +
			" FROM (" + relation.sql + ") AS " + inputSourceAlias +
			" LIMIT " + sentinelRows
		inputSourceAlias = classificationAlias
		inputLimitSQL = ""
	}
	inputSQL := "SELECT " + strings.Join(inputProjection, ", ") + " FROM (" +
		inputSourceSQL + ") AS " + inputSourceAlias + inputLimitSQL
	prefixArgs := make(
		[]any,
		0,
		len(measureInputArgs)+len(eligibilityArgs)+len(unsupportedArgs)+
			len(classificationArgs),
	)
	if windowedDynamicExtrema && len(groups) > 0 {
		// The outer prepared projection (including the measure fold) appears
		// textually before the classified source subquery and its BY arguments.
		prefixArgs = append(prefixArgs, measureInputArgs...)
		prefixArgs = append(prefixArgs, classificationArgs...)
	} else if measureUsesGroupEligibility {
		prefixArgs = append(prefixArgs, eligibilityArgs...)
		prefixArgs = append(prefixArgs, unsupportedArgs...)
		prefixArgs = append(prefixArgs, measureInputArgs...)
	} else {
		prefixArgs = append(prefixArgs, measureInputArgs...)
		prefixArgs = append(prefixArgs, eligibilityArgs...)
		prefixArgs = append(prefixArgs, unsupportedArgs...)
	}
	if measure.Function == plan.AggregateFunctionCountRows && len(groups) == 0 {
		return compileWindowedGlobalEventStatsCount(
			relation,
			operator,
			durableState,
			next,
			output,
			stage,
			inputName,
			inputSQL,
			prefixArgs,
		)
	}
	if windowedDynamicExtrema {
		return compileWindowedDynamicEventStatsExtrema(
			relation,
			operator,
			durableState,
			next,
			output,
			outputState,
			stage,
			groups,
			inputSQL,
			prefixArgs,
		)
	}
	aggregateInputName := inputName
	aggregateMeasureAlias := measureAlias
	listRowStateAlias := ""
	var listDefinitions []string
	if measure.Function == plan.AggregateFunctionList && listInputExists {
		orderKeys := defaultCompiledOrder(durableState)
		if len(orderKeys) == 0 {
			return compiledRelation{}, compileState{}, nil, nil, errors.New(
				"compile ClickHouse eventstats list order: input has no deterministic row identity",
			)
		}
		orderSQL, orderErr := compileMaterializedOrder(orderKeys, false)
		if orderErr != nil {
			return compiledRelation{}, compileState{}, nil, nil, fmt.Errorf(
				"compile ClickHouse eventstats list order: %w",
				orderErr,
			)
		}
		windowParts := make([]string, 0, 2)
		if len(groups) > 0 {
			// Incomplete BY rows normalize missing keys to the same physical
			// value as a present empty String. Partition eligibility separately
			// so a fixed scalar or Array(String) measure from an incomplete row
			// cannot consume the complete empty-key group's first-100 prefix.
			groupKeys := make([]string, 0, len(groups)+1)
			groupKeys = append(groupKeys, eligibleAlias)
			for _, group := range groups {
				groupKeys = append(groupKeys, group.keyAlias)
			}
			windowParts = append(
				windowParts,
				"PARTITION BY "+strings.Join(groupKeys, ", "),
			)
		}
		windowParts = append(windowParts, "ORDER BY "+orderSQL)
		windowOrder := strings.Join(windowParts, " ")
		rowOrdinal := quoteIdentifier(fmt.Sprintf(
			"__os_eventstats_list_row_ordinal_%d",
			stage,
		))
		priorElements := quoteIdentifier(fmt.Sprintf(
			"__os_eventstats_list_prior_elements_%d",
			stage,
		))
		priorBytes := quoteIdentifier(fmt.Sprintf(
			"__os_eventstats_list_prior_bytes_%d",
			stage,
		))
		valuesSQL := "tupleElement(" + measureAlias + ", 1)"
		frame := " OVER (" + windowOrder +
			" ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)"
		windowName := quoteIdentifier(fmt.Sprintf(
			"__os_eventstats_list_window_%d",
			stage,
		))
		maximumListValues := strconv.FormatUint(
			MaximumStatsListValuesPerGroup,
			10,
		)
		// priorBytes only influences rows reached before the first-100 element
		// ceiling. If fewer than 100 elements precede the current row, slicing
		// every preceding row to 100 members is identity-preserving; after that
		// point priorBytes is ignored. Keep this secondary payload walk bounded
		// even when a retained source row holds a very large multivalue.
		boundedByteValuesSQL := "arraySlice(" + valuesSQL + ", 1, " +
			maximumListValues + ")"
		windowSQL := "SELECT *, row_number() OVER (" + windowOrder + ") AS " +
			rowOrdinal + ", ifNull(sum(toUInt128(length(" + valuesSQL + ")))" +
			frame + ", toUInt128(0)) AS " + priorElements + ", ifNull(sum(" +
			stringArrayPayloadBytesSQL(boundedByteValuesSQL) + ")" + frame +
			", toUInt128(0)) AS " + priorBytes + " FROM " + inputName

		listRowStateAlias = quoteIdentifier(fmt.Sprintf(
			"__os_eventstats_list_row_state_%d",
			stage,
		))
		candidateName := quoteIdentifier(fmt.Sprintf(
			"__os_eventstats_list_candidates_%d",
			stage,
		))
		candidateSQL := "SELECT *, " + boundedOrderedStringRowStateSQL(
			rowOrdinal,
			valuesSQL,
			priorElements,
			priorBytes,
		) + " AS " + listRowStateAlias + " FROM " + windowName
		listDefinitions = append(
			listDefinitions,
			windowName+" AS ("+windowSQL+")",
			candidateName+" AS MATERIALIZED ("+candidateSQL+")",
		)
		aggregateInputName = candidateName
		aggregateMeasureAlias = listRowStateAlias
	}
	totalProjection := []string{
		boundedEventStatsCountSQL("count()") + " AS " + totalColumn,
	}
	valueColumn := totalColumn
	typeColumn := ""
	publishAggregateResult := outputState.kind == fieldKindDynamic
	publishesValues := measure.Function == plan.AggregateFunctionValues
	publishesList := measure.Function == plan.AggregateFunctionList
	valueElementsColumn := ""
	valueBytesColumn := ""
	if publishesValues || listInputExists {
		valueElementsColumn = quoteIdentifier(fmt.Sprintf(
			"__os_eventstats_value_elements_%d",
			stage,
		))
		valueBytesColumn = quoteIdentifier(fmt.Sprintf(
			"__os_eventstats_value_bytes_%d",
			stage,
		))
	}
	if measureSpec.materialized && len(groups) == 0 {
		rawValueColumn := quoteIdentifier(measureSpec.valuePrefix + strconv.Itoa(stage))
		aggregateSQL, aggregateErr := measureAggregateSQL(aggregateMeasureAlias)
		if aggregateErr != nil {
			return compiledRelation{}, compileState{}, nil, nil, aggregateErr
		}
		if publishesList && !listInputExists {
			aggregateSQL = emptyOrderedStringListSQL()
		}
		totalProjection = append(
			totalProjection,
			aggregateSQL+" AS "+rawValueColumn,
		)
		valueColumn = rawValueColumn
		if publishAggregateResult {
			valueColumn = quoteIdentifier(fmt.Sprintf(
				"__os_eventstats_published_value_%d",
				stage,
			))
			typeColumn = outputState.storedTypeSQL
			totalProjection = append(
				totalProjection,
				measurePublishValueSQL(rawValueColumn)+" AS "+valueColumn,
				measurePublishTypeSQL(rawValueColumn)+" AS "+typeColumn,
			)
		}
		if publishesValues {
			totalProjection = append(
				totalProjection,
				"toUInt64(length("+rawValueColumn+")) AS "+valueElementsColumn,
				stringArrayPayloadBytesSQL(rawValueColumn)+" AS "+valueBytesColumn,
				eventStatsValuesValidationSQL(
					measureAlias,
					valueElementsColumn,
					valueBytesColumn,
				)+" AS "+validationColumn,
			)
			valueColumn = quoteIdentifier(fmt.Sprintf(
				"__os_eventstats_published_value_%d",
				stage,
			))
			totalProjection = append(
				totalProjection,
				"if(toUInt8("+validationColumn+") = 0, arraySort("+
					rawValueColumn+"), "+measureNullSQL+") AS "+valueColumn,
			)
		} else if publishesList {
			valueColumn = quoteIdentifier(fmt.Sprintf(
				"__os_eventstats_published_value_%d",
				stage,
			))
			if listInputExists {
				totalProjection = append(
					totalProjection,
					"toUInt64(length("+rawValueColumn+")) AS "+valueElementsColumn,
					orderedStringListPayloadBytesSQL(rawValueColumn)+" AS "+valueBytesColumn,
					eventStatsListValidationSQL(
						measureAlias,
						listRowStateAlias,
						valueElementsColumn,
						valueBytesColumn,
					)+" AS "+validationColumn,
				)
				totalProjection = append(
					totalProjection,
					"if(toUInt8("+validationColumn+") = 0, "+
						orderedStringListValuesSQL(rawValueColumn)+", "+
						measureNullSQL+") AS "+valueColumn,
				)
			} else {
				totalProjection = append(
					totalProjection,
					orderedStringListValuesSQL(rawValueColumn)+" AS "+valueColumn,
				)
			}
		} else if measureValidationSQL != nil {
			totalProjection = append(
				totalProjection,
				measureValidationSQL(measureAlias, rawValueColumn)+
					" AS "+validationColumn,
			)
		}
	}
	// A list stage materializes its fully prepared candidate relation instead
	// of the raw bounded input. The total, grouped aggregate, output, and atomic
	// validation branches can then share one ordered-window execution. This is
	// still exactly one fence: finalization keeps the earliest materialization
	// and inlines this candidate if an earlier deferred stage already owns it.
	inputClause := " AS MATERIALIZED ("
	if publishesList && listInputExists {
		inputClause = " AS ("
	}
	definitions := []string{inputName + inputClause + inputSQL + ")"}
	definitions = append(definitions, listDefinitions...)
	definitions = append(
		definitions,
		totalName+" AS (SELECT "+strings.Join(totalProjection, ", ")+
			" FROM "+aggregateInputName+")",
	)

	outputValue := totalAlias + "." + valueColumn
	outputValueElements := "toUInt64(0)"
	outputValueBytes := "toUInt128(0)"
	if (publishesValues || listInputExists) && len(groups) == 0 {
		outputValueElements = totalAlias + "." + valueElementsColumn
		outputValueBytes = totalAlias + "." + valueBytesColumn
	}
	outputStoredType := ""
	if typeColumn != "" {
		outputStoredType = totalAlias + "." + typeColumn
	}
	outputValidation := ""
	if measureValidationSQL != nil || measureUsesValuesValidation ||
		listInputExists {
		outputValidation = totalAlias + "." + validationColumn
	}
	outputExistsSQL := "1"
	fromSQL := aggregateInputName + " AS " + inputAlias + " CROSS JOIN " +
		totalName + " AS " + totalAlias
	if len(groups) > 0 {
		groupCountsName := quoteIdentifier(fmt.Sprintf("__os_eventstats_counts_%d", stage))
		groupCountsAlias := quoteIdentifier(fmt.Sprintf("__os_eventstats_group_row_%d", stage))
		groupCountColumn := quoteIdentifier(fmt.Sprintf("__os_eventstats_group_count_%d", stage))
		groupKeys := make([]string, 0, len(groups))
		joinPredicates := make([]string, 0, len(groups))
		for _, group := range groups {
			groupKeys = append(groupKeys, group.keyAlias)
			joinPredicates = append(
				joinPredicates,
				inputAlias+"."+group.keyAlias+" = "+groupCountsAlias+"."+group.keyAlias,
			)
		}
		validGroup := eligibleAlias + " != 0"
		if len(unsupported) > 0 {
			validGroup = "if(" + unsupportedAlias + " != 0, throwIf(toUInt8(1), '" +
				UnsupportedStatsByValueMarker + "') = 0, " + validGroup + ")"
		}
		groupValueSQL := "toUInt64(count())"
		if measureSpec.materialized {
			var groupValueErr error
			groupValueSQL, groupValueErr = measureAggregateSQL(aggregateMeasureAlias)
			if groupValueErr != nil {
				return compiledRelation{}, compileState{}, nil, nil, groupValueErr
			}
			if publishesList && !listInputExists {
				groupValueSQL = emptyOrderedStringListSQL()
			}
		}
		groupProjection := strings.Join(groupKeys, ", ") + ", " +
			groupValueSQL + " AS " + groupCountColumn
		groupValueColumn := groupCountColumn
		if publishesValues {
			groupProjection += ", toUInt64(length(" + groupCountColumn +
				")) AS " + valueElementsColumn + ", " +
				stringArrayPayloadBytesSQL(groupCountColumn) + " AS " + valueBytesColumn
			groupProjection += ", " + eventStatsValuesValidationSQL(
				measureAlias,
				valueElementsColumn,
				valueBytesColumn,
			) + " AS " + validationColumn
			groupValueColumn = quoteIdentifier(fmt.Sprintf(
				"__os_eventstats_published_value_%d",
				stage,
			))
			groupProjection += ", if(toUInt8(" + validationColumn +
				") = 0, arraySort(" + groupCountColumn + "), " +
				measureNullSQL + ") AS " + groupValueColumn
		} else if publishesList {
			groupValueColumn = quoteIdentifier(fmt.Sprintf(
				"__os_eventstats_published_value_%d",
				stage,
			))
			if listInputExists {
				groupProjection += ", toUInt64(length(" + groupCountColumn +
					")) AS " + valueElementsColumn + ", " +
					orderedStringListPayloadBytesSQL(groupCountColumn) +
					" AS " + valueBytesColumn
				groupProjection += ", " + eventStatsListValidationSQL(
					measureAlias,
					listRowStateAlias,
					valueElementsColumn,
					valueBytesColumn,
				) + " AS " + validationColumn
				groupProjection += ", if(toUInt8(" + validationColumn +
					") = 0, " + orderedStringListValuesSQL(groupCountColumn) +
					", " + measureNullSQL + ") AS " + groupValueColumn
			} else {
				groupProjection += ", " +
					orderedStringListValuesSQL(groupCountColumn) +
					" AS " + groupValueColumn
			}
		} else if measureValidationSQL != nil {
			groupProjection += ", " + measureValidationSQL(
				measureAlias,
				groupCountColumn,
			) +
				" AS " + validationColumn
		}
		groupTypeColumn := ""
		if publishAggregateResult {
			groupValueColumn = quoteIdentifier(fmt.Sprintf(
				"__os_eventstats_published_value_%d",
				stage,
			))
			groupTypeColumn = outputState.storedTypeSQL
			groupProjection += ", " + measurePublishValueSQL(groupCountColumn) +
				" AS " + groupValueColumn + ", " +
				measurePublishTypeSQL(groupCountColumn) + " AS " + groupTypeColumn
		}
		definitions = append(
			definitions,
			groupCountsName+" AS (SELECT "+groupProjection+
				" FROM "+aggregateInputName+" WHERE "+validGroup+
				" GROUP BY "+strings.Join(groupKeys, ", ")+")",
		)
		fromSQL += " LEFT JOIN " + groupCountsName + " AS " + groupCountsAlias +
			" ON " + strings.Join(joinPredicates, " AND ")
		outputExistsSQL = inputAlias + "." + eligibleAlias + " != 0"
		outputValue = "if(" + outputExistsSQL + ", " + groupCountsAlias + "." +
			groupValueColumn + ", " + measureNullSQL + ")"
		if publishesValues || listInputExists {
			outputValueElements = "if(" + outputExistsSQL + ", " + groupCountsAlias +
				"." + valueElementsColumn + ", toUInt64(0))"
			outputValueBytes = "if(" + outputExistsSQL + ", " + groupCountsAlias +
				"." + valueBytesColumn + ", toUInt128(0))"
		}
		if groupTypeColumn != "" {
			outputStoredType = "if(" + outputExistsSQL + ", " + groupCountsAlias +
				"." + groupTypeColumn +
				", toUInt8(" + strconv.Itoa(int(eventfields.StoredValueTypeNull)) + "))"
		}
		if measureValidationSQL != nil || measureUsesValuesValidation ||
			listInputExists {
			outputValidation = "if(" + outputExistsSQL + ", " + groupCountsAlias +
				"." + validationColumn + ", toUInt8(0))"
		}
	}
	if publishesValues || publishesList {
		// Empty values/list results are physically [] but logically absent to SPL.
		// Keep the presence expression bound to the public output alias so later
		// projections and direct copies preserve the fixed multivalue contract.
		outputExistsSQL = "notEmpty(" + quoteIdentifier(output.Name) + ")"
	}
	if publishesValues {
		outputValidation = eventStatsValuesAnnotatedResultValidationSQL(
			outputValidation,
			outputValueElements,
			outputValueBytes,
		)
	} else if listInputExists {
		outputValidation = eventStatsListAnnotatedResultValidationSQL(
			outputValidation,
			outputValueElements,
			outputValueBytes,
		)
	}

	projection := eventAggregateProjection(
		durableState,
		next,
		output.Name,
		outputValue,
		outputStoredType,
		outputExistsSQL,
		validationColumn,
		outputValidation,
		inputAlias,
	)
	resultSQL := "SELECT " +
		strings.Join(projection, ", ") + " FROM " + fromSQL +
		" WHERE " + totalAlias + "." + totalColumn + " <= " + maximumRows
	enriched := compiledRelation{
		sql:        resultSQL,
		depth:      relation.depth + 3 + len(listDefinitions),
		ownerRange: operator.Range,
	}

	barrierName := quoteIdentifier(fmt.Sprintf(
		"__os_eventstats_result_%d",
		stage,
	))
	barrier := &pendingChronologicalBarrier{
		name: barrierName,
		// Defer every eventstats stage into the final flat CTE graph. A later
		// validating extrema can then compose with count/sum/average stages in
		// either order without nesting one MATERIALIZED input inside another.
		sql: resultSQL,
		prerequisiteDefinitions: append(
			append(
				[]string(nil),
				eventStatsPrerequisiteDefinitions...,
			),
			definitions...,
		),
		prefixArgumentsAfterExisting: prefixArgumentsAfterExisting,
		fanout:                       2,
		depth:                        enriched.depth,
		ownerRange:                   operator.Range,
	}
	if len(groups) > 0 {
		barrier.fanout = 3
	}
	if validationColumn != "" {
		barrier.validationColumns = []string{validationColumn}
	}
	publishedAlias := quoteIdentifier(fmt.Sprintf(
		"__os_eventstats_rows_result_%d",
		stage,
	))
	publishedSQL := "SELECT * FROM " + barrierName + " AS " + publishedAlias
	if validationColumn != "" {
		publishedSQL = "SELECT * EXCEPT (" + validationColumn + ") FROM " +
			barrierName + " AS " + publishedAlias
	}
	return enriched.selectFrom(publishedSQL, operator.Range), next, prefixArgs, barrier, nil
}

// compileWindowedGlobalEventStatsCount publishes an argument-free global
// count from the same bounded input row stream. The general eventstats graph
// has two consumers for that input (the aggregate and the row publication),
// but count() OVER () can attach the identical total while preserving every
// input row. Keeping the sentinel input as the one prerequisite fence removes
// the cross-join fanout without weakening the input-row guard.
func compileWindowedGlobalEventStatsCount(
	relation compiledRelation,
	operator *plan.EventAggregate,
	state compileState,
	next compileState,
	output plan.FieldRef,
	stage int,
	inputName string,
	inputSQL string,
	prefixArgs []any,
) (compiledRelation, compileState, []any, *pendingChronologicalBarrier, error) {
	if operator == nil ||
		operator.Measure.Function != plan.AggregateFunctionCountRows ||
		len(operator.GroupBy) != 0 || output.Name == "" || stage < 0 ||
		inputName == "" || inputSQL == "" || len(prefixArgs) != 0 {
		return compiledRelation{}, compileState{}, nil, nil, errors.New(
			"compile ClickHouse global eventstats count: contract is invalid",
		)
	}

	rawTotal := quoteIdentifier(fmt.Sprintf(
		"__os_eventstats_raw_count_%d",
		stage,
	))
	validationColumn := quoteIdentifier(fmt.Sprintf(
		"__os_eventstats_validation_%d",
		stage,
	))
	windowAlias := quoteIdentifier(fmt.Sprintf(
		"__os_eventstats_window_%d",
		stage,
	))
	windowSQL := "SELECT *, count() OVER () AS " + rawTotal + " FROM " + inputName
	maximumRows := strconv.FormatUint(MaximumEventStatsInputRows, 10)
	projection := eventAggregateProjection(
		state,
		next,
		output.Name,
		boundedEventStatsCountSQL(windowAlias+"."+rawTotal),
		"",
		"1",
		validationColumn,
		"toUInt8("+boundedEventStatsCountSQL(windowAlias+"."+rawTotal)+
			" > "+maximumRows+")",
		windowAlias,
	)
	resultSQL := "SELECT " + strings.Join(projection, ", ") + " FROM (" +
		windowSQL + ") AS " + windowAlias
	enriched := compiledRelation{
		sql:        resultSQL,
		depth:      relation.depth + 3,
		ownerRange: operator.Range,
	}

	barrierName := quoteIdentifier(fmt.Sprintf(
		"__os_eventstats_result_%d",
		stage,
	))
	barrier := &pendingChronologicalBarrier{
		name: barrierName,
		sql:  resultSQL,
		prerequisiteDefinitions: []string{
			inputName + " AS MATERIALIZED (" + inputSQL + ")",
		},
		validationColumns: []string{validationColumn},
		fanout:            1,
		depth:             enriched.depth,
		ownerRange:        operator.Range,
	}
	publishedAlias := quoteIdentifier(fmt.Sprintf(
		"__os_eventstats_rows_result_%d",
		stage,
	))
	publishedSQL := "SELECT * EXCEPT (" + validationColumn + ") FROM " +
		barrierName + " AS " + publishedAlias
	return enriched.selectFrom(publishedSQL, operator.Range), next, nil, barrier, nil
}

// compileWindowedDynamicEventStatsExtrema keeps the bounded Dynamic row fold,
// winner aggregate, row-count guard, publication, and validation inside one
// materialized result input. The public barrier is a cheap pass-through, which
// lets the established prerequisite graph keep its result, analysis source,
// final input, and validation CTEs ordinary. ClickHouse 26.3 can then reuse the
// complete evaluated event relation without planning a materialized CTE chain.
func compileWindowedDynamicEventStatsExtrema(
	relation compiledRelation,
	operator *plan.EventAggregate,
	state compileState,
	next compileState,
	output plan.FieldRef,
	outputState fieldState,
	stage int,
	groups []compiledEventStatsGroup,
	preparedSQL string,
	prefixArgs []any,
) (compiledRelation, compileState, []any, *pendingChronologicalBarrier, error) {
	if operator == nil ||
		(operator.Measure.Function != plan.AggregateFunctionMinimum &&
			operator.Measure.Function != plan.AggregateFunctionMaximum) ||
		outputState.kind != fieldKindDynamic || outputState.storedTypeSQL == "" ||
		preparedSQL == "" {
		return compiledRelation{}, compileState{}, nil, nil, errors.New(
			"compile ClickHouse eventstats extrema window: contract is invalid",
		)
	}

	inputAlias := quoteIdentifier(fmt.Sprintf("__os_eventstats_rows_%d", stage))
	measureAlias := quoteIdentifier(fmt.Sprintf("__os_eventstats_measure_%d", stage))
	eligibleAlias := quoteIdentifier(fmt.Sprintf("__os_eventstats_eligible_%d", stage))
	unsupportedAlias := quoteIdentifier(fmt.Sprintf("__os_eventstats_unsupported_%d", stage))
	totalColumn := quoteIdentifier(fmt.Sprintf("__os_eventstats_input_count_%d", stage))
	rawTotalColumn := quoteIdentifier(fmt.Sprintf("__os_eventstats_raw_count_%d", stage))
	rawValueColumn := quoteIdentifier(fmt.Sprintf(
		"__os_eventstats_raw_extrema_%d",
		stage,
	))
	publishedValueColumn := quoteIdentifier(fmt.Sprintf(
		"__os_eventstats_published_value_%d",
		stage,
	))
	typeColumn := outputState.storedTypeSQL
	validationColumn := quoteIdentifier(fmt.Sprintf(
		"__os_eventstats_validation_%d",
		stage,
	))

	partition := ""
	if len(groups) > 0 {
		partitionKeys := make([]string, 0, len(groups)+1)
		partitionKeys = append(partitionKeys, eligibleAlias)
		for _, group := range groups {
			partitionKeys = append(partitionKeys, group.keyAlias)
		}
		partition = "PARTITION BY " + strings.Join(partitionKeys, ", ")
	}
	window := " OVER (" + partition + ")"
	winner := statsExtremaScalarAggregateWinnerSQL(
		operator.Measure.Function,
		measureAlias,
	) + window

	preparedAlias := quoteIdentifier(fmt.Sprintf(
		"__os_eventstats_prepared_%d",
		stage,
	))
	windowSQL := "SELECT *, count() OVER () AS " + rawTotalColumn + ", " +
		winner + " AS " + rawValueColumn + " FROM (" + preparedSQL + ") AS " +
		preparedAlias

	// The final chronological validation envelope already reduces this hidden
	// bit across every row of the complete materialized result. Keeping the
	// row-local poison flag avoids a second window aggregate while preserving
	// whole-result atomicity behind downstream projection, filtering, or LIMIT.
	validation := "toUInt8(tupleElement(" + measureAlias + ", 6))"
	hasUnsupportedGroup := false
	for _, group := range groups {
		hasUnsupportedGroup = hasUnsupportedGroup || group.scalar.unsupportedSQL != ""
	}
	if hasUnsupportedGroup {
		// Validate each BY key independently of combined group eligibility. An
		// object/list key must poison the complete scoped result even when a
		// different key is missing on the same row.
		validation = "if(" + unsupportedAlias + " != 0, throwIf(toUInt8(1), '" +
			UnsupportedStatsByValueMarker + "'), " + validation + ")"
	}

	windowAlias := quoteIdentifier(fmt.Sprintf(
		"__os_eventstats_window_%d",
		stage,
	))
	discarded := []string{
		measureAlias,
		rawTotalColumn,
		rawValueColumn,
	}
	materializedSQL := "SELECT * EXCEPT (" + strings.Join(discarded, ", ") + "), " +
		boundedEventStatsCountSQL(rawTotalColumn) + " AS " + totalColumn + ", " +
		statsExtremaScalarValueSQL(rawValueColumn) + " AS " + publishedValueColumn +
		", " + statsExtremaScalarStoredTypeSQL(rawValueColumn) + " AS " + typeColumn +
		", toUInt8(" + validation + ") AS " + validationColumn + " FROM (" +
		windowSQL + ") AS " + windowAlias

	outputExistsSQL := "1"
	outputValue := inputAlias + "." + publishedValueColumn
	outputStoredType := inputAlias + "." + typeColumn
	if len(groups) > 0 {
		outputExistsSQL = inputAlias + "." + eligibleAlias + " != 0"
		outputValue = "if(" + outputExistsSQL + ", " + outputValue +
			", CAST(NULL AS Dynamic))"
		outputStoredType = "if(" + outputExistsSQL + ", " + outputStoredType +
			", toUInt8(" + strconv.Itoa(int(eventfields.StoredValueTypeNull)) + "))"
	}
	projection := eventAggregateProjection(
		state,
		next,
		output.Name,
		outputValue,
		outputStoredType,
		outputExistsSQL,
		validationColumn,
		inputAlias+"."+validationColumn,
		inputAlias,
	)
	maximumRows := strconv.FormatUint(MaximumEventStatsInputRows, 10)
	resultSQL := "SELECT " + strings.Join(projection, ", ") + " FROM (" +
		materializedSQL + ") AS " + inputAlias + " WHERE " + inputAlias + "." +
		totalColumn + " <= " + maximumRows
	resultDepth := relation.depth + 4
	if len(groups) > 0 {
		// Grouped window extrema classify every BY field in one additional
		// projection below the prepared measure relation. Account for that
		// dependency even though the classifier is embedded in preparedSQL.
		resultDepth++
	}
	enriched := compiledRelation{
		sql:        resultSQL,
		depth:      resultDepth,
		ownerRange: operator.Range,
	}

	resultInputName := quoteIdentifier(fmt.Sprintf(
		"__os_eventstats_result_input_%d",
		stage,
	))
	barrierName := quoteIdentifier(fmt.Sprintf(
		"__os_eventstats_result_%d",
		stage,
	))
	barrierSQL := "SELECT * FROM " + resultInputName
	barrierDepth := relationalNodeDepth(enriched.depth)
	barrier := &pendingChronologicalBarrier{
		name: barrierName,
		sql:  barrierSQL,
		prerequisiteDefinitions: []string{
			resultInputName + " AS MATERIALIZED (" + resultSQL + ")",
		},
		validationColumns: []string{validationColumn},
		fanout:            2,
		depth:             barrierDepth,
		ownerRange:        operator.Range,
	}
	if len(groups) > 0 {
		barrier.fanout = 3
	}
	publishedAlias := quoteIdentifier(fmt.Sprintf(
		"__os_eventstats_rows_result_%d",
		stage,
	))
	publishedSQL := "SELECT * EXCEPT (" + validationColumn + ") FROM " +
		barrierName + " AS " + publishedAlias
	enriched.depth = barrierDepth
	return enriched.selectFrom(publishedSQL, operator.Range), next, prefixArgs, barrier, nil
}

func validateEventAggregate(
	operator *plan.EventAggregate,
	state compileState,
) (plan.FieldRef, error) {
	if operator == nil {
		return plan.FieldRef{}, errors.New("compile ClickHouse eventstats: operator is missing")
	}
	if err := validateNonStatsAggregateMeasureMetadata("eventstats", operator.Measure); err != nil {
		return plan.FieldRef{}, err
	}
	if len(operator.GroupBy) > spl.MaximumStatsGroupFields {
		return plan.FieldRef{}, fmt.Errorf(
			"compile ClickHouse eventstats: more than %d grouping fields",
			spl.MaximumStatsGroupFields,
		)
	}
	measure := operator.Measure
	if measure.Function == plan.AggregateFunctionEarliest ||
		measure.Function == plan.AggregateFunctionLatest {
		if !hasCanonicalEventTime(state) {
			return plan.FieldRef{}, &plan.Diagnostic{
				Code: "SPL_UNSUPPORTED_EVENTSTATS_TIME_FIELD",
				Message: "eventstats earliest and latest require event rows " +
					"with the unmodified canonical _time field",
				Range: measure.Input.Range,
				Suggestions: []string{
					"run eventstats earliest or latest before removing, replacing, or transforming _time",
				},
			}
		}
	}
	switch measure.Function {
	case plan.AggregateFunctionCountRows:
		if measure.Input.Name != "" ||
			measure.Input.Canonical ||
			measure.Input.Path != nil ||
			measure.Input.Range != (spl.Range{}) ||
			measure.Predicate != nil ||
			measure.Percentile != 0 {
			return plan.FieldRef{}, errors.New(
				"compile ClickHouse eventstats: argument-free count contains unsupported metadata",
			)
		}
	case plan.AggregateFunctionCountValues, plan.AggregateFunctionSum,
		plan.AggregateFunctionAverage, plan.AggregateFunctionMinimum,
		plan.AggregateFunctionMaximum, plan.AggregateFunctionEarliest,
		plan.AggregateFunctionLatest, plan.AggregateFunctionDistinctCount,
		plan.AggregateFunctionValues, plan.AggregateFunctionList:
		form := "count(field)"
		switch measure.Function {
		case plan.AggregateFunctionSum:
			form = "sum(field)"
		case plan.AggregateFunctionAverage:
			form = "avg(field)"
		case plan.AggregateFunctionMinimum:
			form = "min(field)"
		case plan.AggregateFunctionMaximum:
			form = "max(field)"
		case plan.AggregateFunctionEarliest:
			form = "earliest(field)"
		case plan.AggregateFunctionLatest:
			form = "latest(field)"
		case plan.AggregateFunctionDistinctCount:
			form = "dc(field)"
		case plan.AggregateFunctionValues:
			form = "values(field)"
		case plan.AggregateFunctionList:
			form = "list(field)"
		}
		if measure.Predicate != nil || measure.Percentile != 0 {
			return plan.FieldRef{}, fmt.Errorf(
				"compile ClickHouse eventstats: %s contains unsupported predicate or percentile metadata",
				form,
			)
		}
		if err := validateCanonicalFieldRef(
			"eventstats",
			"input",
			measure.Input,
		); err != nil {
			return plan.FieldRef{}, err
		}
	case plan.AggregateFunctionPercentile:
		if measure.Predicate != nil ||
			measure.Percentile < 1 || measure.Percentile > 99 {
			return plan.FieldRef{}, errors.New(
				"compile ClickHouse eventstats: pN(field) contains unsupported predicate or percentile metadata",
			)
		}
		if err := validateCanonicalFieldRef(
			"eventstats",
			"input",
			measure.Input,
		); err != nil {
			return plan.FieldRef{}, err
		}
	case plan.AggregateFunctionCountPredicate:
		if err := validateConditionalCountMeasure(
			measure,
			state,
			"eventstats",
			"SPL_AMBIGUOUS_EVENTSTATS_FIELD",
			"eventstats cannot read the event result's reserved fields payload without an exact upstream schema",
		); err != nil {
			return plan.FieldRef{}, err
		}
	default:
		return plan.FieldRef{}, errors.New(
			"compile ClickHouse eventstats: only count, count(field), count(eval(...)), dc(field), values(field), list(field), min(field), max(field), earliest(field), latest(field), sum(field), avg(field), or pN/percN(field) is supported",
		)
	}
	output, err := plan.ResolveField(measure.Output, operator.Range)
	if err != nil {
		return plan.FieldRef{}, fmt.Errorf(
			"compile ClickHouse eventstats: invalid output field %q: %w",
			measure.Output,
			err,
		)
	}
	return output, nil
}

// validateNonStatsAggregateMeasureMetadata closes the compiler trust boundary
// for plan nodes whose AggregateMeasure predates stats scalar inputs, literal
// outputs, and sparklines. Those arms are stats-only. Related commands have
// their own bounded predicate support, so Predicate remains command-validated
// by their existing paths rather than being rejected here.
func validateNonStatsAggregateMeasureMetadata(
	command string,
	measure plan.AggregateMeasure,
) error {
	if measure.Sparkline != nil || measure.InputExpression != nil || measure.OutputLiteral {
		return fmt.Errorf(
			"compile ClickHouse %s: aggregate contains stats-only sparkline, scalar-input, or literal-output metadata",
			command,
		)
	}
	return nil
}

func eventAggregateCompileState(
	state compileState,
	output plan.FieldRef,
	outputState fieldState,
	grouped bool,
	stage int,
) compileState {
	next := cloneCompileState(state)
	if exposesRawFieldsPayload(state) && !output.Canonical {
		dropRawFieldsPayload(&next)
	}
	delete(next.blocked, output.Name)
	if !slices.Contains(next.publicOrder, output.Name) {
		next.publicOrder = append(next.publicOrder, output.Name)
	}
	existsSQL := "1"
	hasLogicalPresence := grouped || isNativeMultivalueKind(outputState.kind)
	if hasLogicalPresence {
		existsSQL = quoteIdentifier(fmt.Sprintf("__os_eventstats_exists_%d", stage))
	}
	outputState.valueSQL = quoteIdentifier(output.Name)
	outputState.existsSQL = existsSQL
	next.visible[output.Name] = outputState
	next.privateColumns = livePrivateColumns(next.privateColumns, next.visible)
	if outputState.storedTypeSQL != "" {
		next.privateColumns = append(next.privateColumns, outputState.storedTypeSQL)
	}
	if hasLogicalPresence {
		next.privateColumns = append(next.privateColumns, existsSQL)
	}
	return next
}

func eventAggregateProjection(
	state, next compileState,
	outputName, outputValue, outputStoredTypeSQL, outputExistsSQL string,
	validationColumn, outputValidationSQL, relationAlias string,
) []string {
	names := orderedVisibleNames(next)
	projection := make([]string, 0, len(names)+12+len(next.privateColumns))
	for _, name := range names {
		publicName := quoteIdentifier(name)
		if name == outputName {
			projection = append(projection, outputValue+" AS "+publicName)
			continue
		}
		field := state.visible[name]
		if field.valueSQL == publicName {
			if pathsOverlap, ok := logicalFieldNamesOverlap(outputName, name); ok && pathsOverlap && relationAlias != "" {
				projection = append(
					projection,
					relationAlias+"."+publicName+" AS "+publicName,
				)
			} else {
				projection = append(projection, publicName)
			}
		} else {
			projection = append(projection, field.valueSQL+" AS "+publicName)
		}
	}
	projectionState := next
	projectionState.privateColumns = livePrivateColumns(state.privateColumns, next.visible)
	projection = appendPrivateEventProjection(projection, projectionState)
	if outputStoredTypeSQL != "" {
		projection = append(
			projection,
			outputStoredTypeSQL+" AS "+next.visible[outputName].storedTypeSQL,
		)
	}
	if outputExistsSQL != "1" {
		projection = append(
			projection,
			"toUInt8("+outputExistsSQL+") AS "+next.visible[outputName].existsSQL,
		)
	}
	if validationColumn != "" && outputValidationSQL != "" {
		projection = append(
			projection,
			"toUInt8("+outputValidationSQL+") AS "+validationColumn,
		)
	}
	return projection
}

func boundedEventStatsCountSQL(countSQL string) string {
	maximum := strconv.FormatUint(MaximumEventStatsInputRows, 10)
	return "arrayElement(arrayMap(total -> total + toUInt64(throwIf(toUInt8(total > " +
		maximum + "), '" + EventStatsInputLimitMarker + "')), [toUInt64(" +
		countSQL + ")]), 1)"
}

// eventStatsDistinctCountValidationSQL keeps both failure classes inside the
// deferred whole-result validation graph. The value set itself contains at
// most one sentinel beyond the supported cardinality, while unsupported
// containers contribute no strings and retain a separate constant-size bit.
func eventStatsDistinctCountValidationSQL(inputSQL, cardinalitySQL string) string {
	maximum := strconv.FormatUint(MaximumStatsDistinctValuesPerGroup, 10)
	unsupported := "maxOrDefault(toUInt8(tupleElement(" + inputSQL + ", 2)))"
	return "if(" + unsupported + " != 0, " +
		"throwIf(toUInt8(1), '" + UnsupportedStatsMeasureValueMarker + "'), " +
		"if(" + cardinalitySQL + " > toUInt64(" + maximum + "), " +
		"throwIf(toUInt8(1), '" + ExactDistinctLimitMarker + "'), " +
		"toUInt8(0)))"
}

// eventStatsValuesValidationSQL validates one complete global or grouped
// exact-string cell before it can be copied onto source rows. The set itself
// retains only one count sentinel; the raw byte ceiling remains independently
// enforced after aggregation under ClickHouse's query-memory limit.
func eventStatsValuesValidationSQL(
	inputSQL, valueElementsSQL, valueBytesSQL string,
) string {
	unsupported := "maxOrDefault(toUInt8(tupleElement(" + inputSQL + ", 2)))"
	maximumValues := strconv.FormatUint(MaximumStatsValuesPerGroup, 10)
	maximumBytes := strconv.FormatUint(MaximumStatsValuesBytesPerGroup, 10)
	return "if(" + unsupported + " != 0, " +
		"throwIf(toUInt8(1), '" + UnsupportedStatsMeasureValueMarker + "'), " +
		"if(" + valueElementsSQL + " > toUInt64(" + maximumValues + "), " +
		"throwIf(toUInt8(1), '" + EventStatsValuesLimitMarker + "'), " +
		"if(" + valueBytesSQL + " > toUInt128(" +
		maximumBytes + "), throwIf(toUInt8(1), '" +
		EventStatsValuesBytesLimitMarker + "'), toUInt8(0))))"
}

// eventStatsValuesAnnotatedResultValidationSQL bounds the serialized
// amplification created when a values cell is repeated on every eligible
// source row. Its inputs are scalar counts materialized once per aggregate
// scope, so the window sums never rescan the arrays they account for.
func eventStatsValuesAnnotatedResultValidationSQL(
	cellValidationSQL, valueElementsSQL, valueBytesSQL string,
) string {
	return eventStatsAnnotatedArrayResultValidationSQL(
		cellValidationSQL,
		valueElementsSQL,
		valueBytesSQL,
		MaximumStatsValuesPerResult,
		MaximumStatsValuesBytesPerResult,
		EventStatsValuesLimitMarker,
		EventStatsValuesBytesLimitMarker,
	)
}

func eventStatsAnnotatedArrayResultValidationSQL(
	cellValidationSQL string,
	valueElementsSQL string,
	valueBytesSQL string,
	maximumElements uint64,
	maximumBytes uint64,
	elementLimitMarker string,
	bytesLimitMarker string,
) string {
	totalElements := "sum(toUInt128(" + valueElementsSQL + ")) OVER ()"
	totalBytes := "sum(toUInt128(" + valueBytesSQL + ")) OVER ()"
	resultValidation := "if(" + totalElements + " > toUInt128(" +
		strconv.FormatUint(maximumElements, 10) + "), " +
		"throwIf(toUInt8(1), '" + elementLimitMarker + "'), " +
		"if(" + totalBytes + " > toUInt128(" +
		strconv.FormatUint(maximumBytes, 10) + "), " +
		"throwIf(toUInt8(1), '" + bytesLimitMarker + "'), toUInt8(0)))"
	if cellValidationSQL == "" {
		return resultValidation
	}
	return "if(toUInt8(" + cellValidationSQL + ") != 0, toUInt8(1), " +
		resultValidation + ")"
}

// eventStatsListValidationSQL validates the complete retained scope before
// publishing its first-100 ordered prefix. Unsupported members are tracked
// independently from the bounded prefix, so a poisoned value after the first
// 100 still fails the whole command. The row state carries a constant-size bit
// when that selected prefix crossed the byte ceiling.
func eventStatsListValidationSQL(
	inputSQL, rowStateSQL, valueElementsSQL, valueBytesSQL string,
) string {
	unsupported := "maxOrDefault(toUInt8(tupleElement(" + inputSQL + ", 2)))"
	bytesOverflow := "maxOrDefault(toUInt8(tupleElement(" + rowStateSQL + ", 2)))"
	maximumValues := strconv.FormatUint(MaximumStatsListValuesPerGroup, 10)
	maximumBytes := strconv.FormatUint(MaximumStatsListBytesPerGroup, 10)
	return "if(" + unsupported + " != 0, " +
		"throwIf(toUInt8(1), '" + UnsupportedStatsMeasureValueMarker + "'), " +
		"if(" + valueElementsSQL + " > toUInt64(" + maximumValues + "), " +
		"throwIf(toUInt8(1), '" + EventStatsListLimitMarker + "'), " +
		"if(" + bytesOverflow + " != 0 OR " + valueBytesSQL +
		" > toUInt128(" + maximumBytes + "), throwIf(toUInt8(1), '" +
		EventStatsListBytesLimitMarker + "'), toUInt8(0))))"
}

// eventStatsListAnnotatedResultValidationSQL bounds the serialized
// amplification caused by copying one selected ordered list onto every source
// row. Element and byte counts are scalar aggregate metadata, so these windows
// never walk the public arrays again.
func eventStatsListAnnotatedResultValidationSQL(
	cellValidationSQL, valueElementsSQL, valueBytesSQL string,
) string {
	return eventStatsAnnotatedArrayResultValidationSQL(
		cellValidationSQL,
		valueElementsSQL,
		valueBytesSQL,
		MaximumStatsListValuesPerResult,
		MaximumStatsListBytesPerResult,
		EventStatsListLimitMarker,
		EventStatsListBytesLimitMarker,
	)
}

func validateAggregatePredicateMeasures(
	operator *plan.Aggregate,
	state compileState,
) error {
	if operator == nil {
		return errors.New("compile ClickHouse aggregate: aggregate is missing")
	}
	for _, measure := range operator.Measures {
		if measure.Function != plan.AggregateFunctionCountPredicate {
			if measure.Predicate != nil {
				return fmt.Errorf(
					"compile ClickHouse aggregate: function %d contains predicate metadata",
					measure.Function,
				)
			}
			continue
		}
		if err := validateConditionalCountMeasure(
			measure,
			state,
			"aggregate",
			"SPL_AMBIGUOUS_STATS_FIELD",
			"stats cannot read the event result's reserved fields payload without an exact upstream schema",
		); err != nil {
			return err
		}
	}
	return nil
}

func validateConditionalCountMeasure(
	measure plan.AggregateMeasure,
	state compileState,
	command, reservedCode, reservedMessage string,
) error {
	prefix := "compile ClickHouse " + command + ": "
	if measure.Input.Name != "" ||
		measure.Input.Canonical ||
		measure.Input.Path != nil ||
		measure.Input.Range != (spl.Range{}) ||
		measure.InputExpression != nil ||
		measure.Percentile != 0 {
		return errors.New(
			prefix + "count(eval(...)) contains unsupported field, scalar-input, or percentile metadata",
		)
	}
	if nilPlanExpression(measure.Predicate) {
		return errors.New(prefix + "count(eval(...)) predicate is missing")
	}
	if err := validateIfCondition(measure.Predicate); err != nil {
		return fmt.Errorf(prefix+"invalid count(eval(...)) predicate: %w", err)
	}
	if state.eventRows && state.allowDynamic {
		if sourceRange, reserved := predicateFieldSourceRange(
			measure.Predicate,
			"fields",
		); reserved {
			return &plan.Diagnostic{
				Code:    reservedCode,
				Message: reservedMessage,
				Range:   sourceRange,
			}
		}
	}
	return nil
}

func validateAggregateCardinality(operator *plan.Aggregate) error {
	if operator == nil || len(operator.Measures) == 0 {
		return errors.New("compile ClickHouse aggregate: no measures")
	}
	if _, err := effectiveStatsOptions(operator); err != nil {
		return err
	}
	if operator.StatsOptions != nil {
		if err := plan.ValidateStatsAggregateSourceUniqueness(operator.Measures); err != nil {
			return fmt.Errorf("compile ClickHouse aggregate: %w", err)
		}
	}
	if len(operator.Measures) > spl.MaximumStatsMeasures {
		return fmt.Errorf(
			"compile ClickHouse aggregate: more than %d measures",
			spl.MaximumStatsMeasures,
		)
	}
	if len(operator.GroupBy) > spl.MaximumStatsGroupFields {
		return fmt.Errorf(
			"compile ClickHouse aggregate: more than %d group fields",
			spl.MaximumStatsGroupFields,
		)
	}
	return nil
}

func hasCanonicalEventTime(state compileState) bool {
	timeField, ok := state.visible["_time"]
	return state.eventRows && ok && timeField.kind == fieldKindTime &&
		timeField.canonicalTime
}

func compileAggregate(operator *plan.Aggregate, state compileState) (
	projection []string,
	predicates []string,
	groups []string,
	next compileState,
	args []any,
	err error,
) {
	if cardinalityErr := validateAggregateCardinality(operator); cardinalityErr != nil {
		return nil, nil, nil, compileState{}, nil, cardinalityErr
	}
	if validateErr := validateAggregatePredicateMeasures(operator, state); validateErr != nil {
		return nil, nil, nil, compileState{}, nil, validateErr
	}
	return compileAggregateValidated(operator, state)
}

// compileAggregateValidated lowers an aggregate after the caller has checked
// its cardinality and conditional-predicate structure. The pipeline compiler
// performs that preflight before predicate materialization; compileAggregate
// remains the defensive entry point for direct package callers and tests.
func compileAggregateValidated(operator *plan.Aggregate, state compileState) (
	projection []string,
	predicates []string,
	groups []string,
	next compileState,
	args []any,
	err error,
) {
	statsOptions, optionsErr := effectiveStatsOptions(operator)
	if optionsErr != nil {
		return nil, nil, nil, compileState{}, nil, optionsErr
	}
	for _, measure := range operator.Measures {
		if measure.Function != plan.AggregateFunctionEarliest &&
			measure.Function != plan.AggregateFunctionLatest &&
			measure.Function != plan.AggregateFunctionEarliestTime &&
			measure.Function != plan.AggregateFunctionLatestTime &&
			measure.Function != plan.AggregateFunctionRate {
			continue
		}
		if !hasCanonicalEventTime(state) {
			sourceRange := measure.Input.Range
			if measure.InputExpression != nil &&
				!nilScalarExpression(measure.InputExpression) {
				sourceRange = measure.InputExpression.SourceRange()
			}
			return nil, nil, nil, compileState{}, nil, &plan.Diagnostic{
				Code:        "SPL_UNSUPPORTED_STATS_TIME_FIELD",
				Message:     "stats time functions require event rows with the unmodified canonical _time field",
				Range:       sourceRange,
				Suggestions: []string{"run the stats time function before removing, replacing, or transforming _time"},
			}
		}
	}
	next = compileState{
		visible:               make(map[string]fieldState, len(operator.GroupBy)+len(operator.Measures)),
		context:               state.context,
		allowDynamic:          false,
		eventRows:             false,
		blocked:               make(map[string]struct{}),
		chronologicalBarriers: append([]compiledChronologicalBarrier(nil), state.chronologicalBarriers...),
	}
	if state.mvExpandQueryRowsSQL != "" {
		// The first expansion's whole-stage charge is constant on every output
		// row. Collapse that private authority through transforming aggregates so
		// a later expansion can add its complete output count rather than resetting
		// the query-wide ceiling. maxOrDefault also yields zero for a global
		// aggregate over an empty input.
		projection = append(
			projection,
			"maxOrDefault("+state.mvExpandQueryRowsSQL+") AS "+state.mvExpandQueryRowsSQL,
		)
		next.mvExpandQueryRowsSQL = state.mvExpandQueryRowsSQL
		next.privateColumns = append(next.privateColumns, state.mvExpandQueryRowsSQL)
	}
	// Even a group-less aggregate produces a deterministic zero-or-one-row
	// relation. Give it a durable constant lineage immediately; grouped
	// aggregates replace this key with their exact group tuple below.
	if len(operator.GroupBy) == 0 {
		ordinal := quoteIdentifier("__os_aggregate_ordinal")
		projection = append(projection, "toUInt8(0) AS "+ordinal)
		next.order = []compiledSortKey{{valueSQL: ordinal}}
		next.tieBreakers = []compiledSortKey{{valueSQL: ordinal}}
	}
	seen := make(map[string]struct{}, len(operator.GroupBy)+len(operator.Measures))
	dynamicGroupInvalid := make([]string, 0, len(operator.GroupBy))
	var dynamicGroupInvalidArgs []any
	for _, group := range operator.GroupBy {
		if err := validateCanonicalFieldRef("aggregate", "group", group); err != nil {
			return nil, nil, nil, compileState{}, nil, err
		}
		if state.eventRows && state.allowDynamic && group.Name == "fields" {
			return nil, nil, nil, compileState{}, nil, &plan.Diagnostic{
				Code:    "SPL_AMBIGUOUS_STATS_FIELD",
				Message: "stats cannot group by the event result's reserved fields payload without an exact upstream schema",
				Range:   group.Range,
			}
		}
		if _, duplicate := seen[group.Name]; duplicate {
			return nil, nil, nil, compileState{}, nil, fmt.Errorf("compile ClickHouse aggregate: output field %q is duplicated", group.Name)
		}
		seen[group.Name] = struct{}{}
		var multivalueGroup compiledStatsMultivalueGroup
		expanded := false
		var compileErr error
		if operator.StatsOptions != nil {
			multivalueGroup, expanded, compileErr = compileStatsMultivalueGroup(
				group,
				state,
				statsOptions.DeduplicateSplitValues,
			)
			if compileErr != nil {
				return nil, nil, nil, compileState{}, nil, compileErr
			}
		}
		if expanded {
			ordinal := len(next.publicOrder)
			valuesAlias := quoteIdentifier(fmt.Sprintf("__os_group_values_%d", ordinal))
			valueAlias := quoteIdentifier(fmt.Sprintf("__os_group_value_%d", ordinal))
			next.preAggregateColumns = append(
				next.preAggregateColumns,
				multivalueGroup.valuesSQL+" AS "+valuesAlias,
			)
			next.preAggregateArgs = append(
				next.preAggregateArgs,
				multivalueGroup.valuesArgs...,
			)
			next.preAggregateGroupExpansions = append(
				next.preAggregateGroupExpansions,
				compiledStatsGroupExpansion{
					valuesAlias: valuesAlias,
					valueAlias:  valueAlias,
				},
			)
			groupOutput := fmt.Sprintf("__os_group_%d", ordinal)
			projection = append(
				projection,
				valueAlias+" AS "+quoteIdentifier(groupOutput),
			)
			groups = append(groups, valueAlias)
			if multivalueGroup.unsupportedSQL != "" {
				dynamicGroupInvalid = append(
					dynamicGroupInvalid,
					multivalueGroup.unsupportedSQL,
				)
				dynamicGroupInvalidArgs = append(
					dynamicGroupInvalidArgs,
					multivalueGroup.unsupportedArgs...,
				)
			}
			if multivalueGroup.field.kind == fieldKindDynamic {
				// Keep the established scalar-presence predicate as a redundant
				// eligibility fence. Empty arrays already disappear in ARRAY JOIN,
				// while this preserves missing/null and flattened-parent validation
				// contracts (including their source-located arguments).
				scalarPresence, presenceErr := compileExactScalarGroup(
					group,
					state,
					"stats BY",
				)
				if presenceErr != nil {
					return nil, nil, nil, compileState{}, nil, presenceErr
				}
				predicates = append(predicates, scalarPresence.presenceSQL)
				args = append(args, scalarPresence.presenceArgs...)
			}
			privateGroup := quoteIdentifier(groupOutput)
			semanticBytesSQL := ""
			if multivalueGroup.field.kind == fieldKindStringArray &&
				multivalueGroup.field.stringOrBytes {
				semanticBytesSQL = quoteIdentifier(fmt.Sprintf(
					"__os_group_semantic_bytes_%d",
					ordinal,
				))
				semanticValueSQL := "toUInt8(NOT isValidUTF8(" + valueAlias + "))"
				projection = append(
					projection,
					semanticValueSQL+" AS "+semanticBytesSQL,
				)
				groups = append(groups, semanticValueSQL)
				next.privateColumns = append(next.privateColumns, semanticBytesSQL)
			}
			numericSort := multivalueGroup.field.numericSort
			if multivalueGroup.field.kind == fieldKindDynamic {
				numericSort = true
			}
			next.visible[group.Name] = fieldState{
				valueSQL:       privateGroup,
				maxStringBytes: fieldStateStringByteBound(multivalueGroup.field),
				// Re-derive text eligibility from the durable aggregate output.
				// The ARRAY JOIN element alias is out of scope after aggregation,
				// while projections and renames can safely rebind this expression.
				textEligibleSQL: func() string {
					if multivalueGroup.field.kind == fieldKindStringArray &&
						multivalueGroup.field.stringOrBytes {
						return "isValidUTF8(" + privateGroup + ")"
					}
					return ""
				}(),
				semanticBytesSQL:            semanticBytesSQL,
				semanticBytesByUTF8Validity: semanticBytesSQL != "",
				existsSQL:                   "1",
				// Fixed values()/list() arrays preserve arbitrary String bytes.
				// ARRAY JOIN changes only cardinality; it must not silently narrow
				// an invalid-UTF-8 member from Bytes provenance to String.
				stringOrBytes: multivalueGroup.field.kind == fieldKindStringArray &&
					multivalueGroup.field.stringOrBytes,
				kind:          fieldKindString,
				caseSensitive: multivalueGroup.field.caseSensitive,
				numericSort:   numericSort,
			}
			next.publicOrder = append(next.publicOrder, group.Name)
			next.order = append(next.order, compiledSortKey{valueSQL: privateGroup})
			next.tieBreakers = append(next.tieBreakers, compiledSortKey{valueSQL: privateGroup})
			continue
		}
		scalarGroup, compileErr := compileExactScalarGroup(group, state, "stats BY")
		if compileErr != nil {
			return nil, nil, nil, compileState{}, nil, compileErr
		}
		field := scalarGroup.field
		valueSQL := scalarGroup.keySQL
		kind := field.kind
		numericSort := field.numericSort
		maxStringBytes := fieldStateStringByteBound(field)
		if kind == fieldKindDynamic {
			// Unsupported containers use one private placeholder group. A scoped
			// whole-input window below fails the search before any key is exposed.
			valueAlias := quoteIdentifier(fmt.Sprintf("__os_group_value_%d", len(groups)))
			next.preAggregateColumns = append(next.preAggregateColumns,
				scalarGroup.keySQL+" AS "+valueAlias,
			)
			valueSQL = valueAlias
			kind = fieldKindString
			numericSort = true
		}
		ordinal := len(next.publicOrder)
		groupOutput := fmt.Sprintf("__os_group_%d", ordinal)
		projection = append(projection, valueSQL+" AS "+quoteIdentifier(groupOutput))
		if scalarGroup.unsupportedSQL != "" {
			// Validate each key against its own presence rather than the combined
			// group eligibility predicate. A container must fail the whole scoped
			// search even when another BY key is missing on the same row.
			dynamicGroupInvalid = append(dynamicGroupInvalid, scalarGroup.unsupportedSQL)
			dynamicGroupInvalidArgs = append(
				dynamicGroupInvalidArgs,
				scalarGroup.unsupportedArgs...,
			)
		}
		predicates = append(predicates, scalarGroup.presenceSQL)
		args = append(args, scalarGroup.presenceArgs...)
		groups = append(groups, valueSQL)
		privateGroup := quoteIdentifier(groupOutput)
		textEligibleSQL := field.textEligibleSQL
		semanticBytesSQL := ""
		if field.kind == fieldKindString && field.stringOrBytes {
			if field.semanticBytesSQL == "" {
				return nil, nil, nil, compileState{}, nil, errors.New(
					"compile ClickHouse aggregate: String-or-Bytes group lacks semantic Bytes provenance",
				)
			}
			semanticBytesSQL = quoteIdentifier(fmt.Sprintf(
				"__os_group_semantic_bytes_%d",
				ordinal,
			))
			semanticValueSQL := "toUInt8(ifNull(" + field.semanticBytesSQL + ", 0))"
			projection = append(
				projection,
				semanticValueSQL+" AS "+semanticBytesSQL,
			)
			groups = append(groups, semanticValueSQL)
			next.privateColumns = append(next.privateColumns, semanticBytesSQL)
			textEligibleSQL = "(ifNull(" + semanticBytesSQL +
				", 0) = 0 AND isValidUTF8(" + privateGroup + "))"
		}
		next.visible[group.Name] = fieldState{
			valueSQL: privateGroup, maxStringBytes: maxStringBytes,
			textEligibleSQL:             textEligibleSQL,
			semanticBytesSQL:            semanticBytesSQL,
			textEligibleBySemanticBytes: semanticBytesSQL != "",
			existsSQL:                   "1",
			stringOrBytes:               field.stringOrBytes,
			stringOrBytesNullable:       field.stringOrBytesNullable,
			kind:                        kind,
			caseSensitive:               field.caseSensitive, numberType: field.numberType,
			numericSort: numericSort, alwaysNull: field.alwaysNull,
		}
		next.publicOrder = append(next.publicOrder, group.Name)
		next.order = append(next.order, compiledSortKey{valueSQL: privateGroup})
		next.tieBreakers = append(next.tieBreakers, compiledSortKey{valueSQL: privateGroup})
	}
	numericInputs := make(map[string]string)
	numericInputForResolved := func(ref plan.FieldRef, input fieldState, ok bool) string {
		if inputAlias, cached := numericInputs[ref.Name]; cached {
			return inputAlias
		}
		inputSQL := "CAST([], 'Array(Float64)')"
		var inputArgs []any
		if ok {
			inputSQL, inputArgs = numericArrayInputSQL(input)
		}
		inputAlias := quoteIdentifier(fmt.Sprintf("__os_measure_values_%d", len(numericInputs)))
		numericInputs[ref.Name] = inputAlias
		next.preAggregateColumns = append(next.preAggregateColumns, inputSQL+" AS "+inputAlias)
		next.preAggregateArgs = append(next.preAggregateArgs, inputArgs...)
		return inputAlias
	}
	numericInputFor := func(ref plan.FieldRef) (string, error) {
		if inputAlias, cached := numericInputs[ref.Name]; cached {
			return inputAlias, nil
		}
		input, ok, resolveErr := resolveCompiledField(ref, state)
		if resolveErr != nil {
			return "", resolveErr
		}
		inputAlias := numericInputForResolved(ref, input, ok)
		return inputAlias, nil
	}
	type aggregateExpressionInput struct {
		valueSQL              string
		valueArgs             []any
		kind                  fieldKind
		numberType            string
		dynamicDomain         dynamicScalarDomain
		maxStringBytes        uint64
		numericIntegral       bool
		mvCountOneOrNull      bool
		mvSortedLexicographic bool
		alwaysNull            bool
		ieeeComparison        bool
		ordinal               int
		field                 fieldState
		numericAlias          string
		stringAlias           string
	}
	aggregateExpressionInputs := make([]*aggregateExpressionInput, 0)
	aggregateExpressionInputFor := func(
		expression plan.ScalarExpression,
	) (*aggregateExpressionInput, error) {
		if nilScalarExpression(expression) {
			return nil, errors.New("compile ClickHouse aggregate: scalar eval input is missing")
		}
		compiled, compileErr := compileScalarValue(expression, state)
		if compileErr != nil {
			return nil, fmt.Errorf("compile ClickHouse aggregate scalar eval input: %w", compileErr)
		}
		for _, cached := range aggregateExpressionInputs {
			if cached.valueSQL == compiled.valueSQL &&
				reflect.DeepEqual(cached.valueArgs, compiled.valueArgs) &&
				cached.field.textEligibleSQL == compiled.textEligibleSQL &&
				cached.field.semanticBytesSQL == compiled.semanticBytesSQL &&
				cached.field.textEligibleBySemanticBytes == compiled.textEligibleBySemanticBytes &&
				cached.field.stringOrBytes == compiled.stringOrBytes &&
				cached.field.stringOrBytesNullable == compiled.stringOrBytesNullable &&
				cached.kind == compiled.kind &&
				cached.numberType == compiled.numberType &&
				cached.dynamicDomain == compiled.dynamicDomain &&
				cached.maxStringBytes == compiled.maxStringBytes &&
				cached.numericIntegral == compiled.numericIntegral &&
				cached.mvCountOneOrNull == compiled.mvCountOneOrNull &&
				cached.mvSortedLexicographic == compiled.mvSortedLexicographic &&
				cached.alwaysNull == compiled.alwaysNull &&
				cached.ieeeComparison == compiled.ieeeComparison {
				return cached, nil
			}
		}

		ordinal := len(aggregateExpressionInputs)
		valueAlias := quoteIdentifier(fmt.Sprintf(
			"__os_measure_numeric_expression_value_%d",
			ordinal,
		))
		next.preAggregateColumns = append(
			next.preAggregateColumns,
			compiled.valueSQL+" AS "+valueAlias,
		)
		next.preAggregateArgs = append(next.preAggregateArgs, compiled.valueArgs...)
		semanticBytesSQL := compiled.semanticBytesSQL
		if compiled.kind == fieldKindString && compiled.stringOrBytes {
			if semanticBytesSQL == "" {
				return nil, errors.New(
					"compile ClickHouse aggregate scalar eval input: String-or-Bytes value lacks semantic Bytes provenance",
				)
			}
			semanticAlias := quoteIdentifier(fmt.Sprintf(
				"__os_measure_expression_semantic_bytes_%d",
				ordinal,
			))
			next.preAggregateColumns = append(
				next.preAggregateColumns,
				"toUInt8(ifNull("+semanticBytesSQL+", 0)) AS "+semanticAlias,
			)
			next.preAggregateArgs = append(
				next.preAggregateArgs,
				compiled.semanticBytesArgs...,
			)
			semanticBytesSQL = semanticAlias
		}
		materialized := fieldState{
			valueSQL:                    valueAlias,
			textEligibleSQL:             compiled.textEligibleSQL,
			semanticBytesSQL:            semanticBytesSQL,
			semanticBytesByUTF8Validity: compiled.semanticBytesByUTF8Validity,
			textEligibleBySemanticBytes: compiled.textEligibleBySemanticBytes,
			stringOrBytes:               compiled.stringOrBytes,
			stringOrBytesNullable:       compiled.stringOrBytesNullable,
			existsSQL:                   "1",
			kind:                        compiled.kind,
			numberType:                  compiled.numberType,
			maxStringBytes:              compiled.maxStringBytes,
			numericIntegral:             compiled.numericIntegral,
			mvCountOneOrNull:            compiled.mvCountOneOrNull,
			mvSortedLexicographic:       compiled.mvSortedLexicographic,
			alwaysNull:                  compiled.alwaysNull,
			dynamicDomain:               compiled.dynamicDomain,
			ieeeComparison:              compiled.ieeeComparison,
		}
		if compiled.kind == fieldKindDynamic {
			materialized.dynamicTypeSQL = "dynamicType(" + valueAlias + ")"
		}
		cached := &aggregateExpressionInput{
			valueSQL:              compiled.valueSQL,
			valueArgs:             append([]any(nil), compiled.valueArgs...),
			kind:                  compiled.kind,
			numberType:            compiled.numberType,
			dynamicDomain:         compiled.dynamicDomain,
			maxStringBytes:        compiled.maxStringBytes,
			numericIntegral:       compiled.numericIntegral,
			mvCountOneOrNull:      compiled.mvCountOneOrNull,
			mvSortedLexicographic: compiled.mvSortedLexicographic,
			alwaysNull:            compiled.alwaysNull,
			ieeeComparison:        compiled.ieeeComparison,
			ordinal:               ordinal,
			field:                 materialized,
		}
		aggregateExpressionInputs = append(aggregateExpressionInputs, cached)
		return cached, nil
	}
	numericInputForExpression := func(expression plan.ScalarExpression) (string, error) {
		cached, inputErr := aggregateExpressionInputFor(expression)
		if inputErr != nil {
			return "", inputErr
		}
		if cached.numericAlias != "" {
			return cached.numericAlias, nil
		}
		inputSQL, inputArgs := numericArrayInputSQL(cached.field)
		inputAlias := quoteIdentifier(fmt.Sprintf(
			"__os_measure_numeric_expression_%d",
			cached.ordinal,
		))
		next.preAggregateColumns = append(
			next.preAggregateColumns,
			inputSQL+" AS "+inputAlias,
		)
		next.preAggregateArgs = append(next.preAggregateArgs, inputArgs...)
		cached.numericAlias = inputAlias
		return inputAlias, nil
	}
	type aggregateInputCacheKey struct {
		fieldName         string
		expressionOrdinal int
		expression        bool
	}
	fieldInputCacheKey := func(name string) aggregateInputCacheKey {
		return aggregateInputCacheKey{fieldName: name}
	}
	expressionInputCacheKey := func(ordinal int) aggregateInputCacheKey {
		return aggregateInputCacheKey{
			expressionOrdinal: ordinal,
			expression:        true,
		}
	}
	stringInputs := make(map[string]string)
	allNumericInvalidInputs := make(map[aggregateInputCacheKey]string)
	allNumericInvalidInputFor := func(
		key aggregateInputCacheKey,
		input fieldState,
		exists bool,
	) string {
		if !statsOptions.AllNumeric {
			return ""
		}
		if cached, ok := allNumericInvalidInputs[key]; ok {
			return cached
		}
		if !exists {
			allNumericInvalidInputs[key] = ""
			return ""
		}
		invalidSQL, invalidArgs := statsAllNumericInvalidSQL(input)
		if invalidSQL == "toUInt8(0)" {
			allNumericInvalidInputs[key] = ""
			return ""
		}
		alias := quoteIdentifier(fmt.Sprintf(
			"__os_measure_all_numeric_invalid_%d",
			len(allNumericInvalidInputs),
		))
		allNumericInvalidInputs[key] = alias
		next.preAggregateColumns = append(
			next.preAggregateColumns,
			invalidSQL+" AS "+alias,
		)
		next.preAggregateArgs = append(next.preAggregateArgs, invalidArgs...)
		return alias
	}
	allNumericInvalidFor := func(
		measure plan.AggregateMeasure,
		key aggregateInputCacheKey,
	) (string, error) {
		if !statsOptions.AllNumeric || !statsUsesAllNumericPolicy(measure.Function) {
			return "", nil
		}
		if measure.InputExpression != nil {
			cached, inputErr := aggregateExpressionInputFor(measure.InputExpression)
			if inputErr != nil {
				return "", inputErr
			}
			return allNumericInvalidInputFor(key, cached.field, !cached.field.alwaysNull), nil
		}
		input, exists, resolveErr := resolveCompiledField(measure.Input, state)
		if resolveErr != nil {
			return "", resolveErr
		}
		return allNumericInvalidInputFor(key, input, exists), nil
	}
	type scalarStringInput struct {
		ordinal        int
		valueAlias     string
		numberAlias    string
		candidateAlias string
		rawBytesSQL    string
		extremaReady   bool
	}
	scalarStringInputs := make(map[aggregateInputCacheKey]*scalarStringInput)
	countInputs := make(map[string]string)
	countInputFor := func(ref plan.FieldRef) (string, error) {
		if inputAlias, cached := countInputs[ref.Name]; cached {
			return inputAlias, nil
		}
		input, ok, resolveErr := resolveCompiledField(ref, state)
		if resolveErr != nil {
			return "", resolveErr
		}
		inputSQL := "toUInt64(0)"
		var inputArgs []any
		if ok {
			inputSQL, inputArgs = countValueInputSQL(input)
		}
		inputAlias := quoteIdentifier(fmt.Sprintf(
			"__os_measure_count_%d",
			len(countInputs),
		))
		countInputs[ref.Name] = inputAlias
		next.preAggregateColumns = append(
			next.preAggregateColumns,
			inputSQL+" AS "+inputAlias,
		)
		next.preAggregateArgs = append(next.preAggregateArgs, inputArgs...)
		return inputAlias, nil
	}
	type conditionalCountInput struct {
		predicateSQL  string
		predicateArgs []any
		alias         string
	}
	conditionalCountInputs := make([]conditionalCountInput, 0)
	conditionalCountInputFor := func(expression plan.Expression) (string, error) {
		predicateSQL, predicateArgs, compileErr := compileExpression(expression, state)
		if compileErr != nil {
			return "", compileErr
		}
		for _, cached := range conditionalCountInputs {
			if cached.predicateSQL == predicateSQL &&
				reflect.DeepEqual(cached.predicateArgs, predicateArgs) {
				return cached.alias, nil
			}
		}
		alias := quoteIdentifier(fmt.Sprintf(
			"__os_measure_conditional_count_%d",
			len(conditionalCountInputs),
		))
		conditionalCountInputs = append(
			conditionalCountInputs,
			conditionalCountInput{
				predicateSQL:  predicateSQL,
				predicateArgs: append([]any(nil), predicateArgs...),
				alias:         alias,
			},
		)
		next.preAggregateColumns = append(
			next.preAggregateColumns,
			"toUInt64(ifNull("+predicateSQL+", 0)) AS "+alias,
		)
		next.preAggregateArgs = append(next.preAggregateArgs, predicateArgs...)
		return alias, nil
	}
	extremaInputs := make(map[aggregateInputCacheKey]string)
	type scalarExtremaResultKey struct {
		input    aggregateInputCacheKey
		function plan.AggregateFunction
	}
	type scalarExtremaResult struct {
		winnerAlias string
		typeAlias   string
	}
	scalarExtremaResults := make(map[scalarExtremaResultKey]scalarExtremaResult)
	dynamicExtremaResults := make(map[scalarExtremaResultKey]scalarExtremaResult)
	type chronologicalInput struct {
		candidatesAlias string
		validationAlias string
		multiple        bool
	}
	type chronologicalResultKey struct {
		input    aggregateInputCacheKey
		function plan.AggregateFunction
	}
	type chronologicalResult struct {
		winnerAlias string
		typeAlias   string
	}
	chronologicalInputs := make(map[aggregateInputCacheKey]chronologicalInput)
	chronologicalResults := make(map[chronologicalResultKey]chronologicalResult)
	chronologicalInputDirections := make(map[string]chronologicalDirections)
	chronologicalRowKey := ""
	exactStringSets := make(map[aggregateInputCacheKey]string)
	distinctCounts := make(map[aggregateInputCacheKey]string)
	type orderedStringList struct {
		listColumn     string
		overflowColumn string
	}
	orderedStringLists := make(map[aggregateInputCacheKey]orderedStringList)
	valuesInputs := make(map[aggregateInputCacheKey]struct{})
	extremaMeasureInputs := make(map[string]struct{})
	numericArrayConsumers := make(map[string]struct{})
	percentileLevels := make(map[string][]uint8)
	for _, measure := range operator.Measures {
		if measure.Function == plan.AggregateFunctionEarliest ||
			measure.Function == plan.AggregateFunctionLatest ||
			measure.Function == plan.AggregateFunctionFirst ||
			measure.Function == plan.AggregateFunctionLast ||
			measure.Function == plan.AggregateFunctionEarliestTime ||
			measure.Function == plan.AggregateFunctionLatestTime {
			directions := chronologicalInputDirections[measure.Input.Name]
			if measure.Function == plan.AggregateFunctionEarliest ||
				measure.Function == plan.AggregateFunctionFirst ||
				measure.Function == plan.AggregateFunctionEarliestTime {
				directions.earliest = true
			} else {
				directions.latest = true
			}
			chronologicalInputDirections[measure.Input.Name] = directions
		}
		if measure.Function == plan.AggregateFunctionValues && measure.InputExpression == nil {
			valuesInputs[fieldInputCacheKey(measure.Input.Name)] = struct{}{}
		}
		if measure.Function == plan.AggregateFunctionMinimum ||
			measure.Function == plan.AggregateFunctionMaximum {
			extremaMeasureInputs[measure.Input.Name] = struct{}{}
		}
		if measure.InputExpression == nil && (measure.Function == plan.AggregateFunctionSum ||
			measure.Function == plan.AggregateFunctionAverage ||
			measure.Function == plan.AggregateFunctionExactPercentile ||
			measure.Function == plan.AggregateFunctionUpperPercentile ||
			measure.Function == plan.AggregateFunctionMedian ||
			measure.Function == plan.AggregateFunctionRange ||
			measure.Function == plan.AggregateFunctionSumSquares ||
			measure.Function == plan.AggregateFunctionStandardDeviationSample ||
			measure.Function == plan.AggregateFunctionStandardDeviationPopulation ||
			measure.Function == plan.AggregateFunctionVarianceSample ||
			measure.Function == plan.AggregateFunctionVariancePopulation ||
			measure.Function == plan.AggregateFunctionRate) {
			numericArrayConsumers[measure.Input.Name] = struct{}{}
		}
		if measure.Function == plan.AggregateFunctionPercentile &&
			measure.InputExpression == nil &&
			measure.Percentile >= 1 && measure.Percentile <= 99 &&
			!slices.Contains(percentileLevels[measure.Input.Name], measure.Percentile) {
			percentileLevels[measure.Input.Name] = append(
				percentileLevels[measure.Input.Name],
				measure.Percentile,
			)
		}
	}
	listRowOrdinal := ""
	listWindowOrder := ""
	listRowOrdinalFor := func() (string, string, error) {
		if listRowOrdinal != "" {
			return listRowOrdinal, listWindowOrder, nil
		}
		orderKeys := defaultCompiledOrder(state)
		if len(orderKeys) == 0 {
			return "", "", errors.New("compile ClickHouse list order: input has no deterministic row identity")
		}
		orderSQL, orderErr := compileMaterializedOrder(orderKeys, false)
		if orderErr != nil {
			return "", "", fmt.Errorf("compile ClickHouse list order: %w", orderErr)
		}
		windowParts := make([]string, 0, 2)
		if len(groups) > 0 {
			windowParts = append(windowParts, "PARTITION BY "+strings.Join(groups, ", "))
		}
		windowParts = append(windowParts, "ORDER BY "+orderSQL)
		listWindowOrder = strings.Join(windowParts, " ")
		listRowOrdinal = quoteIdentifier("__os_list_row_ordinal")
		next.preAggregateListWindowColumns = append(
			next.preAggregateListWindowColumns,
			"row_number() OVER ("+listWindowOrder+") AS "+listRowOrdinal,
		)
		return listRowOrdinal, listWindowOrder, nil
	}
	scalarStringInputFor := func(key aggregateInputCacheKey, input fieldState) *scalarStringInput {
		if cached, ok := scalarStringInputs[key]; ok {
			return cached
		}
		ordinal := len(scalarStringInputs)
		inputSQL, inputArgs := statsScalarStringInputSQL(input)
		inputAlias := quoteIdentifier(fmt.Sprintf(
			"__os_measure_scalar_string_%d",
			ordinal,
		))
		cached := &scalarStringInput{
			ordinal:     ordinal,
			valueAlias:  inputAlias,
			rawBytesSQL: fixedStringExtremaRawBytesSQL(input),
		}
		scalarStringInputs[key] = cached
		next.preAggregateColumns = append(next.preAggregateColumns, inputSQL+" AS "+inputAlias)
		next.preAggregateArgs = append(next.preAggregateArgs, inputArgs...)
		return cached
	}
	stringInputFor := func(ref plan.FieldRef) (string, error) {
		if inputSQL, cached := stringInputs[ref.Name]; cached {
			return inputSQL, nil
		}
		input, ok, resolveErr := resolveCompiledField(ref, state)
		if resolveErr != nil {
			return "", resolveErr
		}
		inputSQL := "CAST([], 'Array(String)')"
		if ok {
			var inputArgs []any
			if _, sharesScalar := extremaMeasureInputs[ref.Name]; sharesScalar &&
				input.kind == fieldKindString && input.textEligibleSQL == "" {
				scalarInput := scalarStringInputFor(fieldInputCacheKey(ref.Name), input)
				inputSQL = compactNullableArraySQL("[" + scalarInput.valueAlias + "]")
			} else {
				inputSQL, inputArgs = stringArrayInputSQL(input)
			}
			inputAlias := quoteIdentifier(fmt.Sprintf("__os_measure_strings_%d", len(stringInputs)))
			stringInputs[ref.Name] = inputAlias
			next.preAggregateColumns = append(next.preAggregateColumns, inputSQL+" AS "+inputAlias)
			next.preAggregateArgs = append(next.preAggregateArgs, inputArgs...)
			inputSQL = inputAlias
		}
		stringInputs[ref.Name] = inputSQL
		return inputSQL, nil
	}
	stringInputForExpression := func(expression plan.ScalarExpression) (string, error) {
		cached, inputErr := aggregateExpressionInputFor(expression)
		if inputErr != nil {
			return "", inputErr
		}
		if cached.stringAlias != "" {
			return cached.stringAlias, nil
		}
		inputSQL, inputArgs := stringArrayInputSQL(cached.field)
		inputAlias := quoteIdentifier(fmt.Sprintf(
			"__os_measure_string_expression_%d",
			cached.ordinal,
		))
		next.preAggregateColumns = append(
			next.preAggregateColumns,
			inputSQL+" AS "+inputAlias,
		)
		next.preAggregateArgs = append(next.preAggregateArgs, inputArgs...)
		cached.stringAlias = inputAlias
		return inputAlias, nil
	}
	chronologicalRowKeyFor := func() string {
		if chronologicalRowKey != "" {
			return chronologicalRowKey
		}
		chronologicalRowKey = quoteIdentifier("__os_chronological_row_key")
		next.preAggregateColumns = append(
			next.preAggregateColumns,
			immutableChronologicalRowKeySQL()+" AS "+chronologicalRowKey,
		)
		return chronologicalRowKey
	}
	chronologicalInputForResolved := func(
		key aggregateInputCacheKey,
		input fieldState,
		exists bool,
		directions chronologicalDirections,
	) chronologicalInput {
		if cached, ok := chronologicalInputs[key]; ok {
			return cached
		}
		ordinal := len(chronologicalInputs)
		compiled := chronologicalInput{}
		candidatesSQL, candidateArgs, runtimeValidated := chronologicalCandidatesSQL(
			input,
			exists,
			directions,
		)
		compiled.candidatesAlias = quoteIdentifier(fmt.Sprintf(
			"__os_chronological_candidates_%d",
			ordinal,
		))
		compiled.multiple = exists &&
			(input.kind == fieldKindDynamic || isNativeMultivalueKind(input.kind))
		next.preAggregateColumns = append(
			next.preAggregateColumns,
			candidatesSQL+" AS "+compiled.candidatesAlias,
		)
		next.preAggregateArgs = append(next.preAggregateArgs, candidateArgs...)
		if runtimeValidated {
			compiled.validationAlias = quoteIdentifier(fmt.Sprintf(
				"__os_stats_chronological_any_unsupported_%d",
				ordinal,
			))
			projection = append(
				projection,
				"max(toUInt8(tupleElement("+compiled.candidatesAlias+", 4))) AS "+
					compiled.validationAlias,
			)
		}
		chronologicalInputs[key] = compiled
		return compiled
	}
	chronologicalInputFor := func(ref plan.FieldRef) (chronologicalInput, error) {
		input, exists, resolveErr := resolveCompiledField(ref, state)
		if resolveErr != nil {
			return chronologicalInput{}, resolveErr
		}
		return chronologicalInputForResolved(
			fieldInputCacheKey(ref.Name),
			input,
			exists,
			chronologicalInputDirections[ref.Name],
		), nil
	}
	chronologicalInputForExpression := func(
		expression plan.ScalarExpression,
	) (chronologicalInput, error) {
		cached, inputErr := aggregateExpressionInputFor(expression)
		if inputErr != nil {
			return chronologicalInput{}, inputErr
		}
		return chronologicalInputForResolved(
			expressionInputCacheKey(cached.ordinal),
			cached.field,
			!cached.field.alwaysNull,
			chronologicalDirections{earliest: true, latest: true},
		), nil
	}
	type percentileState struct {
		column    string
		positions map[uint8]int
	}
	percentileStates := make(map[string]percentileState)
	percentileInputFor := func(ref plan.FieldRef) (string, bool, error) {
		if _, sharedWithArrayConsumer := numericArrayConsumers[ref.Name]; sharedWithArrayConsumer {
			inputAlias, err := numericInputFor(ref)
			return inputAlias, true, err
		}
		input, ok, resolveErr := resolveCompiledField(ref, state)
		if resolveErr != nil {
			return "", false, resolveErr
		}
		if ok && (input.kind == fieldKindDynamic || isNativeMultivalueKind(input.kind)) {
			return numericInputForResolved(ref, input, true), true, nil
		}
		inputSQL := "CAST(NULL AS Nullable(Float64))"
		if ok {
			inputSQL = percentileInputSQL(input)
			if input.existsSQL != "" && input.existsSQL != "1" {
				inputSQL = "if(" + input.existsSQL + ", " + inputSQL +
					", CAST(NULL AS Nullable(Float64)))"
				next.preAggregateArgs = append(next.preAggregateArgs, input.existsArgs...)
			}
		}
		inputAlias := quoteIdentifier(fmt.Sprintf(
			"__os_measure_percentile_value_%d",
			len(percentileStates),
		))
		next.preAggregateColumns = append(next.preAggregateColumns, inputSQL+" AS "+inputAlias)
		return inputAlias, false, nil
	}
	type sparklineBucketInput struct {
		spec  statsSparklineBucketSpec
		alias string
	}
	sparklineBuckets := make(map[plan.SparklineSpan]sparklineBucketInput)
	for measureIndex, measure := range operator.Measures {
		if measure.OutputLiteral {
			if operator.StatsOptions == nil || !spl.IsStatsLiteralOutputName(measure.Output) {
				return nil, nil, nil, compileState{}, nil, fmt.Errorf(
					"compile ClickHouse aggregate: invalid literal output field %q",
					measure.Output,
				)
			}
		} else if _, fieldErr := plan.ResolveField(measure.Output, spl.Range{}); fieldErr != nil {
			return nil, nil, nil, compileState{}, nil, fmt.Errorf(
				"compile ClickHouse aggregate: invalid output field %q: %w",
				measure.Output,
				fieldErr,
			)
		}
		if measure.Sparkline != nil {
			if operator.StatsOptions == nil ||
				measure.Function != plan.AggregateFunctionInvalid ||
				measure.Input.Name != "" || measure.Input.Canonical ||
				measure.Input.Path != nil || measure.Input.Range != (spl.Range{}) ||
				measure.InputExpression != nil || measure.Predicate != nil ||
				measure.Percentile != 0 {
				return nil, nil, nil, compileState{}, nil, errors.New(
					"compile ClickHouse aggregate: sparkline and scalar aggregate metadata overlap",
				)
			}
			if state.context == nil || state.context.searchEarliest.IsZero() ||
				state.context.searchLatest.IsZero() {
				return nil, nil, nil, compileState{}, nil, errors.New(
					"compile ClickHouse stats sparkline: search time range is unavailable",
				)
			}

			sparkline := measure.Sparkline
			if err := validateCanonicalFieldRef("stats sparkline", "time", sparkline.Time); err != nil {
				return nil, nil, nil, compileState{}, nil, err
			}
			if sparkline.Time.Name != "_time" || !sparkline.Time.Canonical ||
				sparkline.Time.Path != nil {
				return nil, nil, nil, compileState{}, nil, errors.New(
					"compile ClickHouse stats sparkline: time field is not canonical _time",
				)
			}
			timeField, timeExists, resolveErr := resolveCompiledField(sparkline.Time, state)
			if resolveErr != nil {
				return nil, nil, nil, compileState{}, nil, resolveErr
			}
			if !timeExists || timeField.kind != fieldKindTime || !timeField.canonicalTime {
				return nil, nil, nil, compileState{}, nil, &plan.Diagnostic{
					Code:    "SPL_UNSUPPORTED_STATS_TIME_FIELD",
					Message: "stats sparkline requires event rows with the unmodified canonical _time field",
					Range:   sparkline.Time.Range,
				}
			}

			hasSparklineInput := sparkline.Input.Name != "" ||
				sparkline.Input.Canonical || sparkline.Input.Path != nil ||
				sparkline.Input.Range != (spl.Range{})
			if hasSparklineInput {
				if err := validateCanonicalFieldRef("stats sparkline", "input", sparkline.Input); err != nil {
					return nil, nil, nil, compileState{}, nil, err
				}
				if state.eventRows && state.allowDynamic && sparkline.Input.Name == "fields" {
					return nil, nil, nil, compileState{}, nil, &plan.Diagnostic{
						Code:    "SPL_AMBIGUOUS_STATS_FIELD",
						Message: "stats sparkline cannot read the event result's reserved fields payload without an exact upstream schema",
						Range:   sparkline.Input.Range,
					}
				}
			}

			bucket, cached := sparklineBuckets[sparkline.Span]
			if !cached {
				spec, specErr := statsSparklineBucketSpecFor(
					sparkline.Span,
					state.context.searchEarliest,
					state.context.searchLatest,
					sparkline.MaximumPoints,
					timeField.valueSQL,
					state.context.searchTimezone,
				)
				if specErr != nil {
					return nil, nil, nil, compileState{}, nil, specErr
				}
				bucket = sparklineBucketInput{
					spec: spec,
					alias: quoteIdentifier(fmt.Sprintf(
						"__os_sparkline_bucket_%d",
						len(sparklineBuckets),
					)),
				}
				sparklineBuckets[sparkline.Span] = bucket
				next.preAggregateColumns = append(
					next.preAggregateColumns,
					spec.BucketSQL+" AS "+bucket.alias,
				)
				next.preAggregateArgs = append(next.preAggregateArgs, spec.BucketArgs...)
			}

			partition := append(append([]string(nil), groups...), bucket.alias)
			partitionSQL := strings.Join(partition, ", ")
			inputSQL := ""
			expectedInput := statsSparklineInputNone
			missing := statsSparklineMissingEmpty
			switch sparkline.Function {
			case plan.AggregateFunctionCountRows:
				if hasSparklineInput {
					return nil, nil, nil, compileState{}, nil, errors.New(
						"compile ClickHouse stats sparkline: row count contains an input field",
					)
				}
				missing = statsSparklineMissingZero
			case plan.AggregateFunctionCountValues:
				if !hasSparklineInput {
					return nil, nil, nil, compileState{}, nil, errors.New(
						"compile ClickHouse stats sparkline: count(field) input is missing",
					)
				}
				inputSQL, resolveErr = countInputFor(sparkline.Input)
				expectedInput = statsSparklineInputOccurrenceCount
				missing = statsSparklineMissingZero
			case plan.AggregateFunctionDistinctCount,
				plan.AggregateFunctionMinimum,
				plan.AggregateFunctionMaximum:
				if !hasSparklineInput {
					return nil, nil, nil, compileState{}, nil, errors.New(
						"compile ClickHouse stats sparkline: string aggregate input is missing",
					)
				}
				inputSQL, resolveErr = stringInputFor(sparkline.Input)
				expectedInput = statsSparklineInputStringArray
				if sparkline.Function == plan.AggregateFunctionDistinctCount {
					missing = statsSparklineMissingZero
				}
			case plan.AggregateFunctionAverage,
				plan.AggregateFunctionStandardDeviationSample,
				plan.AggregateFunctionStandardDeviationPopulation,
				plan.AggregateFunctionVarianceSample,
				plan.AggregateFunctionVariancePopulation,
				plan.AggregateFunctionSum,
				plan.AggregateFunctionSumSquares,
				plan.AggregateFunctionRange:
				if !hasSparklineInput {
					return nil, nil, nil, compileState{}, nil, errors.New(
						"compile ClickHouse stats sparkline: numeric aggregate input is missing",
					)
				}
				inputSQL, resolveErr = numericInputFor(sparkline.Input)
				expectedInput = statsSparklineInputFloat64Array
			default:
				return nil, nil, nil, compileState{}, nil, fmt.Errorf(
					"compile ClickHouse stats sparkline: unsupported function %d",
					sparkline.Function,
				)
			}
			if resolveErr != nil {
				return nil, nil, nil, compileState{}, nil, resolveErr
			}
			lowering, supported := statsSparklineWindowAggregateSQL(
				sparkline.Function,
				inputSQL,
				partitionSQL,
			)
			if !supported || lowering.Input != expectedInput {
				return nil, nil, nil, compileState{}, nil, errors.New(
					"compile ClickHouse stats sparkline: aggregate lowering is invalid",
				)
			}
			windowSQL := lowering.SQL
			if statsOptions.AllNumeric && statsUsesAllNumericPolicy(sparkline.Function) {
				input, exists, inputErr := resolveCompiledField(sparkline.Input, state)
				if inputErr != nil {
					return nil, nil, nil, compileState{}, nil, inputErr
				}
				invalidAlias := allNumericInvalidInputFor(
					fieldInputCacheKey(sparkline.Input.Name),
					input,
					exists,
				)
				if invalidAlias != "" {
					windowSQL = "if(max(" + invalidAlias + ") OVER (PARTITION BY " +
						partitionSQL + ") != 0, CAST(NULL AS Nullable(Float64)), " +
						windowSQL + ")"
				}
			}

			if _, duplicate := seen[measure.Output]; duplicate {
				return nil, nil, nil, compileState{}, nil, fmt.Errorf(
					"compile ClickHouse aggregate: output field %q is duplicated",
					measure.Output,
				)
			}
			seen[measure.Output] = struct{}{}
			windowAlias := quoteIdentifier(fmt.Sprintf(
				"__os_sparkline_window_%d",
				measureIndex,
			))
			next.preAggregateSparklineWindows = append(
				next.preAggregateSparklineWindows,
				windowSQL+" AS "+windowAlias,
			)
			recordsSQL, ok := statsSparklineBucketRecordsSQL(
				bucket.alias,
				windowAlias,
				sparkline.MaximumPoints,
			)
			if !ok {
				return nil, nil, nil, compileState{}, nil, errors.New(
					"compile ClickHouse stats sparkline: bucket record lowering is invalid",
				)
			}
			recordsAlias := quoteIdentifier(fmt.Sprintf(
				"__os_sparkline_records_%d",
				measureIndex,
			))
			projection = append(projection, recordsSQL+" AS "+recordsAlias)
			output := quoteIdentifier(measure.Output)
			next.postAggregateSparklines = append(
				next.postAggregateSparklines,
				compiledStatsSparklineMeasure{
					recordsColumn: recordsAlias,
					outputColumn:  output,
					spec:          bucket.spec,
					missing:       missing,
				},
			)
			next.visible[measure.Output] = fieldState{
				valueSQL:       output,
				existsSQL:      "1",
				kind:           fieldKindStringArray,
				maxStringBytes: MaximumStatsSparklineBytesPerCell,
				statsSparkline: true,
				stringOrBytes: sparkline.Function == plan.AggregateFunctionMinimum ||
					sparkline.Function == plan.AggregateFunctionMaximum,
			}
			next.publicOrder = append(next.publicOrder, measure.Output)
			if len(next.order) == 0 {
				next.order = append(next.order, compiledSortKey{valueSQL: output})
			}
			continue
		}
		hasFieldInput := measure.Input.Name != "" || measure.Input.Canonical ||
			len(measure.Input.Path) != 0 || measure.Input.Range != (spl.Range{})
		hasExpressionInput := measure.InputExpression != nil
		if hasExpressionInput && nilScalarExpression(measure.InputExpression) {
			return nil, nil, nil, compileState{}, nil, errors.New(
				"compile ClickHouse aggregate: scalar eval input is a typed nil",
			)
		}
		supportsExpressionInput := false
		switch measure.Function {
		case plan.AggregateFunctionCountRows:
			if hasFieldInput || hasExpressionInput || measure.Percentile != 0 {
				return nil, nil, nil, compileState{}, nil, errors.New(
					"compile ClickHouse aggregate: count contains unsupported input metadata",
				)
			}
		case plan.AggregateFunctionCountPredicate:
			// Predicate structure and mutually exclusive metadata were
			// validated before any materialization-field traversal.
		case plan.AggregateFunctionCountValues:
			if hasExpressionInput || measure.Percentile != 0 {
				return nil, nil, nil, compileState{}, nil, errors.New(
					"compile ClickHouse aggregate: count(field) contains scalar-input or percentile metadata",
				)
			}
		case plan.AggregateFunctionPercentile,
			plan.AggregateFunctionExactPercentile,
			plan.AggregateFunctionUpperPercentile:
			supportsExpressionInput = true
			if measure.Percentile < 1 || measure.Percentile > 99 {
				return nil, nil, nil, compileState{}, nil, fmt.Errorf(
					"compile ClickHouse aggregate: unsupported percentile %d",
					measure.Percentile,
				)
			}
		case plan.AggregateFunctionSum, plan.AggregateFunctionAverage,
			plan.AggregateFunctionMedian,
			plan.AggregateFunctionRange, plan.AggregateFunctionSumSquares,
			plan.AggregateFunctionStandardDeviationSample,
			plan.AggregateFunctionStandardDeviationPopulation,
			plan.AggregateFunctionVarianceSample,
			plan.AggregateFunctionVariancePopulation,
			plan.AggregateFunctionRate:
			supportsExpressionInput = true
			if measure.Percentile != 0 {
				return nil, nil, nil, compileState{}, nil, fmt.Errorf(
					"compile ClickHouse aggregate: function %d contains percentile metadata",
					measure.Function,
				)
			}
		case plan.AggregateFunctionDistinctCount,
			plan.AggregateFunctionEstimatedDistinctCount,
			plan.AggregateFunctionEstimatedDistinctCountError,
			plan.AggregateFunctionValues,
			plan.AggregateFunctionList,
			plan.AggregateFunctionMinimum, plan.AggregateFunctionMaximum,
			plan.AggregateFunctionMode,
			plan.AggregateFunctionFirst, plan.AggregateFunctionLast,
			plan.AggregateFunctionEarliest, plan.AggregateFunctionLatest,
			plan.AggregateFunctionEarliestTime,
			plan.AggregateFunctionLatestTime:
			supportsExpressionInput = true
			if measure.Percentile != 0 {
				return nil, nil, nil, compileState{}, nil, fmt.Errorf(
					"compile ClickHouse aggregate: function %d contains percentile metadata",
					measure.Function,
				)
			}
		default:
			return nil, nil, nil, compileState{}, nil, fmt.Errorf(
				"compile ClickHouse aggregate: unsupported function %d",
				measure.Function,
			)
		}
		if measure.Function != plan.AggregateFunctionCountRows &&
			measure.Function != plan.AggregateFunctionCountPredicate {
			if hasFieldInput == hasExpressionInput {
				return nil, nil, nil, compileState{}, nil, errors.New(
					"compile ClickHouse aggregate: measure requires exactly one field or scalar eval input",
				)
			}
			if hasExpressionInput && !supportsExpressionInput {
				return nil, nil, nil, compileState{}, nil, fmt.Errorf(
					"compile ClickHouse aggregate: function %d does not support a scalar eval input",
					measure.Function,
				)
			}
			if hasFieldInput {
				if err := validateCanonicalFieldRef("aggregate", "input", measure.Input); err != nil {
					return nil, nil, nil, compileState{}, nil, err
				}
				if state.eventRows && state.allowDynamic && measure.Input.Name == "fields" {
					return nil, nil, nil, compileState{}, nil, &plan.Diagnostic{
						Code:    "SPL_AMBIGUOUS_STATS_FIELD",
						Message: "stats cannot read the event result's reserved fields payload without an exact upstream schema",
						Range:   measure.Input.Range,
					}
				}
			} else if state.eventRows && state.allowDynamic {
				if sourceRange, reserved := predicateFieldSourceRange(
					&plan.ScalarPredicateExpression{Value: measure.InputExpression},
					"fields",
				); reserved {
					return nil, nil, nil, compileState{}, nil, &plan.Diagnostic{
						Code:    "SPL_AMBIGUOUS_STATS_FIELD",
						Message: "stats cannot read the event result's reserved fields payload without an exact upstream schema",
						Range:   sourceRange,
					}
				}
			}
		}
		measureInputKey := fieldInputCacheKey(measure.Input.Name)
		if hasExpressionInput {
			cached, inputErr := aggregateExpressionInputFor(measure.InputExpression)
			if inputErr != nil {
				return nil, nil, nil, compileState{}, nil, inputErr
			}
			measureInputKey = expressionInputCacheKey(cached.ordinal)
		}
		if _, duplicate := seen[measure.Output]; duplicate {
			return nil, nil, nil, compileState{}, nil, fmt.Errorf("compile ClickHouse aggregate: output field %q is duplicated", measure.Output)
		}
		seen[measure.Output] = struct{}{}
		output := quoteIdentifier(measure.Output)
		measureState := fieldState{valueSQL: output, existsSQL: "1", kind: fieldKindNumber}
		allNumericInvalidAlias, invalidErr := allNumericInvalidFor(measure, measureInputKey)
		if invalidErr != nil {
			return nil, nil, nil, compileState{}, nil, invalidErr
		}
		switch measure.Function {
		case plan.AggregateFunctionCountRows:
			projection = append(projection, "count() AS "+output)
			measureState.numberType = "UInt64"
		case plan.AggregateFunctionCountPredicate:
			inputAlias, inputErr := conditionalCountInputFor(measure.Predicate)
			if inputErr != nil {
				return nil, nil, nil, compileState{}, nil, inputErr
			}
			// The predicate is a measure, not a prefilter: TRUE contributes one
			// while FALSE/NULL contributes zero. UInt128 protects the partial
			// aggregate state and the production row ceiling bounds the final
			// UInt64 conversion.
			projection = append(
				projection,
				"toUInt64(sum(toUInt128("+inputAlias+"))) AS "+output,
			)
			measureState.numberType = "UInt64"
		case plan.AggregateFunctionCountValues:
			inputAlias, inputErr := countInputFor(measure.Input)
			if inputErr != nil {
				return nil, nil, nil, compileState{}, nil, inputErr
			}
			// Aggregate in UInt128 so the intermediate state cannot wrap. The
			// production 250M-row read ceiling and 1 MiB hard event ceiling make
			// the final occurrence total strictly smaller than UInt64.
			projection = append(projection, "toUInt64(sum(toUInt128("+inputAlias+"))) AS "+output)
			measureState.numberType = "UInt64"
		case plan.AggregateFunctionPercentile:
			if measure.InputExpression != nil {
				inputAlias, inputErr := numericInputForExpression(measure.InputExpression)
				if inputErr != nil {
					return nil, nil, nil, compileState{}, nil, inputErr
				}
				projection = append(
					projection,
					statsAllNumericResultSQL(
						singlePercentileArrayAggregateSQL(measure.Percentile, inputAlias),
						allNumericInvalidAlias,
					)+
						" AS "+output,
				)
				measureState.numberType = "Float64"
				break
			}
			percentiles, cached := percentileStates[measure.Input.Name]
			if !cached {
				inputAlias, inputIsArray, inputErr := percentileInputFor(measure.Input)
				if inputErr != nil {
					return nil, nil, nil, compileState{}, nil, inputErr
				}
				levels := percentileLevels[measure.Input.Name]
				if len(levels) == 0 {
					return nil, nil, nil, compileState{}, nil, errors.New(
						"compile ClickHouse aggregate: percentile input has no valid levels",
					)
				}
				levelSQL := make([]string, 0, len(levels))
				positions := make(map[uint8]int, len(levels))
				for index, level := range levels {
					levelSQL = append(levelSQL, statsPercentileLevelSQL(level))
					positions[level] = index + 1
				}
				percentiles = percentileState{
					column: quoteIdentifier(fmt.Sprintf(
						"__os_stats_percentiles_%d",
						len(percentileStates),
					)),
					positions: positions,
				}
				percentileStates[measure.Input.Name] = percentiles
				aggregateFunction := "quantilesGKOrNull"
				if inputIsArray {
					aggregateFunction += "Array"
				}
				projection = append(
					projection,
					aggregateFunction+"(100, "+strings.Join(levelSQL, ", ")+")("+
						inputAlias+") AS "+percentiles.column,
				)
			}
			position, ok := percentiles.positions[measure.Percentile]
			if !ok {
				return nil, nil, nil, compileState{}, nil, errors.New(
					"compile ClickHouse aggregate: percentile level was not collected",
				)
			}
			projection = append(
				projection,
				statsAllNumericResultSQL(
					"arrayElementOrNull("+percentiles.column+", "+strconv.Itoa(position)+")",
					allNumericInvalidAlias,
				)+" AS "+output,
			)
			measureState.numberType = "Float64"
		case plan.AggregateFunctionExactPercentile,
			plan.AggregateFunctionUpperPercentile,
			plan.AggregateFunctionMedian:
			var inputAlias string
			var inputErr error
			if measure.InputExpression != nil {
				inputAlias, inputErr = numericInputForExpression(measure.InputExpression)
			} else {
				inputAlias, inputErr = numericInputFor(measure.Input)
			}
			if inputErr != nil {
				return nil, nil, nil, compileState{}, nil, inputErr
			}
			lowering, supported := statsDistributionArrayAggregateSQL(
				measure.Function,
				measure.Percentile,
				inputAlias,
			)
			if !supported || lowering.Result != statsDistributionResultNullableFloat64 {
				return nil, nil, nil, compileState{}, nil, errors.New(
					"compile ClickHouse aggregate: percentile distribution lowering is invalid",
				)
			}
			projection = append(
				projection,
				statsAllNumericResultSQL(lowering.SQL, allNumericInvalidAlias)+" AS "+output,
			)
			measureState.numberType = "Float64"
		case plan.AggregateFunctionRate:
			var inputAlias string
			var inputErr error
			if measure.InputExpression != nil {
				inputAlias, inputErr = numericInputForExpression(measure.InputExpression)
			} else {
				inputAlias, inputErr = numericInputFor(measure.Input)
			}
			if inputErr != nil {
				return nil, nil, nil, compileState{}, nil, inputErr
			}
			timeField, timeExists := state.visible["_time"]
			if !timeExists || timeField.kind != fieldKindTime || !timeField.canonicalTime {
				return nil, nil, nil, compileState{}, nil, errors.New(
					"compile ClickHouse aggregate: rate has no canonical _time input",
				)
			}
			projection = append(
				projection,
				statsAllNumericResultSQL(
					statsRateAggregateSQL(
						inputAlias,
						chronologicalRowKeyFor(),
						percentileInputSQL(timeField),
					),
					allNumericInvalidAlias,
				)+" AS "+output,
			)
			measureState.numberType = "Float64"
		case plan.AggregateFunctionSum, plan.AggregateFunctionAverage,
			plan.AggregateFunctionRange, plan.AggregateFunctionSumSquares,
			plan.AggregateFunctionStandardDeviationSample,
			plan.AggregateFunctionStandardDeviationPopulation,
			plan.AggregateFunctionVarianceSample,
			plan.AggregateFunctionVariancePopulation:
			var inputAlias string
			var inputErr error
			if measure.InputExpression != nil {
				inputAlias, inputErr = numericInputForExpression(measure.InputExpression)
			} else {
				inputAlias, inputErr = numericInputFor(measure.Input)
			}
			if inputErr != nil {
				return nil, nil, nil, compileState{}, nil, inputErr
			}
			valueSQL, supported := statsNumericArrayAggregateSQL(measure.Function, inputAlias)
			if !supported {
				return nil, nil, nil, compileState{}, nil, errors.New(
					"compile ClickHouse aggregate: numeric array function is invalid",
				)
			}
			projection = append(
				projection,
				statsAllNumericResultSQL(valueSQL, allNumericInvalidAlias)+" AS "+output,
			)
			measureState.numberType = "Float64"
		case plan.AggregateFunctionMinimum, plan.AggregateFunctionMaximum:
			var input fieldState
			var ok bool
			if measure.InputExpression != nil {
				cached, inputErr := aggregateExpressionInputFor(measure.InputExpression)
				if inputErr != nil {
					return nil, nil, nil, compileState{}, nil, inputErr
				}
				input = cached.field
				ok = !input.alwaysNull
			} else {
				var resolveErr error
				input, ok, resolveErr = resolveCompiledField(measure.Input, state)
				if resolveErr != nil {
					return nil, nil, nil, compileState{}, nil, resolveErr
				}
			}
			if eligible, eligibleArgs, fixed := fixedExtremaEligibilitySQL(input); ok && fixed {
				function := "minIfOrNull"
				if measure.Function == plan.AggregateFunctionMaximum {
					function = "maxIfOrNull"
				}
				projection = append(projection, function+"("+input.valueSQL+", "+eligible+") AS "+output)
				args = append(args, eligibleArgs...)
				measureState.kind = input.kind
				measureState.numberType = input.numberType
				measureState.caseSensitive = input.caseSensitive
				break
			}

			if ok && input.kind == fieldKindString {
				scalarInput := scalarStringInputFor(measureInputKey, input)
				if !scalarInput.extremaReady {
					scalarInput.numberAlias = quoteIdentifier(fmt.Sprintf(
						"__os_measure_extrema_number_%d",
						scalarInput.ordinal,
					))
					scalarInput.candidateAlias = quoteIdentifier(fmt.Sprintf(
						"__os_measure_extrema_scalar_%d",
						scalarInput.ordinal,
					))
					next.preAggregateColumns = append(
						next.preAggregateColumns,
						statsExtremaScalarNumberSQL(scalarInput.valueAlias)+" AS "+scalarInput.numberAlias,
						statsExtremaScalarCandidateSQL(
							scalarInput.valueAlias,
							scalarInput.numberAlias,
							scalarInput.rawBytesSQL,
						)+" AS "+scalarInput.candidateAlias,
					)
					scalarInput.extremaReady = true
				}

				resultKey := scalarExtremaResultKey{
					input:    measureInputKey,
					function: measure.Function,
				}
				result, cached := scalarExtremaResults[resultKey]
				if !cached {
					result = scalarExtremaResult{
						winnerAlias: quoteIdentifier(fmt.Sprintf(
							"__os_stats_extrema_winner_%d",
							measureIndex,
						)),
						typeAlias: quoteIdentifier(fmt.Sprintf(
							"__os_stats_extrema_type_%d",
							measureIndex,
						)),
					}
					scalarExtremaResults[resultKey] = result
					projection = append(
						projection,
						statsExtremaScalarAggregateWinnerSQL(
							measure.Function,
							scalarInput.candidateAlias,
						)+" AS "+result.winnerAlias,
					)
					next.privateColumns = append(
						next.privateColumns,
						result.typeAlias,
					)
				}
				next.postAggregateScalarExtrema = append(
					next.postAggregateScalarExtrema,
					compiledScalarExtremaMeasure{
						winnerColumn: result.winnerAlias,
						typeColumn:   result.typeAlias,
						outputColumn: output,
					},
				)
				measureState = fieldState{
					valueSQL:       output,
					dynamicTypeSQL: "dynamicType(" + output + ")",
					storedTypeSQL:  result.typeAlias,
					existsSQL:      "1",
					kind:           fieldKindDynamic,
				}
				break
			}

			candidates, cached := extremaInputs[measureInputKey]
			if !cached {
				candidates = quoteIdentifier(fmt.Sprintf("__os_measure_extrema_%d", len(extremaInputs)))
				extremaInputs[measureInputKey] = candidates
				var candidateSQL string
				var candidateArgs []any
				if ok && input.kind == fieldKindDynamic {
					candidateSQL, candidateArgs = statsExtremaDynamicCandidatesSQL(input)
				} else {
					var stringInputSQL string
					var inputErr error
					if measure.InputExpression != nil {
						stringInputSQL, inputErr = stringInputForExpression(measure.InputExpression)
					} else {
						stringInputSQL, inputErr = stringInputFor(measure.Input)
					}
					if inputErr != nil {
						return nil, nil, nil, compileState{}, nil, inputErr
					}
					candidateSQL = statsExtremaCandidatesSQL(stringInputSQL)
				}
				next.preAggregateColumns = append(
					next.preAggregateColumns,
					candidateSQL+" AS "+candidates,
				)
				next.preAggregateArgs = append(next.preAggregateArgs, candidateArgs...)
			}
			resultKey := scalarExtremaResultKey{
				input:    measureInputKey,
				function: measure.Function,
			}
			result, cached := dynamicExtremaResults[resultKey]
			if !cached {
				result = scalarExtremaResult{
					winnerAlias: quoteIdentifier(fmt.Sprintf(
						"__os_stats_extrema_winner_%d",
						measureIndex,
					)),
					typeAlias: quoteIdentifier(fmt.Sprintf(
						"__os_stats_extrema_type_%d",
						measureIndex,
					)),
				}
				dynamicExtremaResults[resultKey] = result
				projection = append(
					projection,
					statsExtremaAggregateSQL(measure.Function, candidates)+
						" AS "+result.winnerAlias,
					statsExtremaStoredTypeSQL(result.winnerAlias)+
						" AS "+result.typeAlias,
				)
				next.privateColumns = append(next.privateColumns, result.typeAlias)
			}
			projection = append(projection, result.winnerAlias+" AS "+output)
			measureState = fieldState{
				valueSQL:       output,
				dynamicTypeSQL: "dynamicType(" + output + ")",
				storedTypeSQL:  result.typeAlias,
				existsSQL:      "1",
				kind:           fieldKindDynamic,
			}
		case plan.AggregateFunctionFirst, plan.AggregateFunctionLast,
			plan.AggregateFunctionEarliest, plan.AggregateFunctionLatest:
			var input chronologicalInput
			var inputErr error
			if measure.InputExpression != nil {
				input, inputErr = chronologicalInputForExpression(measure.InputExpression)
			} else {
				input, inputErr = chronologicalInputFor(measure.Input)
			}
			if inputErr != nil {
				return nil, nil, nil, compileState{}, nil, inputErr
			}
			rowKey := chronologicalRowKeyFor()
			if measure.Function == plan.AggregateFunctionFirst ||
				measure.Function == plan.AggregateFunctionLast {
				var orderErr error
				rowKey, _, orderErr = listRowOrdinalFor()
				if orderErr != nil {
					return nil, nil, nil, compileState{}, nil, orderErr
				}
			}
			resultKey := chronologicalResultKey{
				input:    measureInputKey,
				function: measure.Function,
			}
			result, cached := chronologicalResults[resultKey]
			if !cached {
				ordinal := len(chronologicalResults)
				result = chronologicalResult{
					winnerAlias: quoteIdentifier(fmt.Sprintf(
						"__os_chronological_winner_%d",
						ordinal,
					)),
					typeAlias: quoteIdentifier(fmt.Sprintf(
						"__os_chronological_type_%d",
						ordinal,
					)),
				}
				chronologicalResults[resultKey] = result
				aggregateSQL, aggregateErr := chronologicalAggregateSQL(
					measure.Function,
					input.candidatesAlias,
					rowKey,
					input.multiple,
				)
				if aggregateErr != nil {
					return nil, nil, nil, compileState{}, nil, aggregateErr
				}
				projection = append(
					projection,
					aggregateSQL+" AS "+result.winnerAlias,
				)
				next.privateColumns = append(next.privateColumns, result.typeAlias)
			}
			next.postAggregateChronological = append(
				next.postAggregateChronological,
				compiledChronologicalMeasure{
					winnerColumn:     result.winnerAlias,
					validationColumn: input.validationAlias,
					typeColumn:       result.typeAlias,
					outputColumn:     output,
				},
			)
			measureState = fieldState{
				valueSQL:       output,
				dynamicTypeSQL: "dynamicType(" + output + ")",
				storedTypeSQL:  result.typeAlias,
				existsSQL:      "1",
				kind:           fieldKindDynamic,
			}
		case plan.AggregateFunctionEarliestTime,
			plan.AggregateFunctionLatestTime:
			var input chronologicalInput
			var inputErr error
			if measure.InputExpression != nil {
				input, inputErr = chronologicalInputForExpression(measure.InputExpression)
			} else {
				input, inputErr = chronologicalInputFor(measure.Input)
			}
			if inputErr != nil {
				return nil, nil, nil, compileState{}, nil, inputErr
			}
			timeField, timeExists := state.visible["_time"]
			if !timeExists || timeField.kind != fieldKindTime || !timeField.canonicalTime {
				return nil, nil, nil, compileState{}, nil, errors.New(
					"compile ClickHouse aggregate: occurrence time has no canonical _time input",
				)
			}
			valueSQL, valueErr := statsOccurrenceTimeAggregateSQL(
				measure.Function,
				input.candidatesAlias,
				chronologicalRowKeyFor(),
				percentileInputSQL(timeField),
			)
			if valueErr != nil {
				return nil, nil, nil, compileState{}, nil, valueErr
			}
			projection = append(projection, valueSQL+" AS "+output)
			measureState.numberType = "Float64"
		case plan.AggregateFunctionEstimatedDistinctCount,
			plan.AggregateFunctionEstimatedDistinctCountError,
			plan.AggregateFunctionMode:
			var inputSQL string
			var modeInput fieldState
			modeInputKnown := false
			var inputErr error
			if measure.InputExpression != nil {
				inputSQL, inputErr = stringInputForExpression(measure.InputExpression)
				if inputErr == nil {
					cached, cachedErr := aggregateExpressionInputFor(measure.InputExpression)
					if cachedErr != nil {
						return nil, nil, nil, compileState{}, nil, cachedErr
					}
					modeInput = cached.field
					modeInputKnown = !modeInput.alwaysNull
				}
			} else {
				inputSQL, inputErr = stringInputFor(measure.Input)
				if inputErr == nil {
					modeInput, modeInputKnown, inputErr = resolveCompiledField(
						measure.Input,
						state,
					)
				}
			}
			if inputErr != nil {
				return nil, nil, nil, compileState{}, nil, inputErr
			}
			lowering, supported := statsDistributionArrayAggregateSQL(
				measure.Function,
				measure.Percentile,
				inputSQL,
			)
			if !supported {
				return nil, nil, nil, compileState{}, nil, errors.New(
					"compile ClickHouse aggregate: string distribution lowering is invalid",
				)
			}
			modeSemanticBytesSQL := ""
			if measure.Function == plan.AggregateFunctionMode && modeInputKnown &&
				modeInput.stringOrBytes &&
				(modeInput.kind == fieldKindString || modeInput.kind == fieldKindStringArray) {
				if modeInput.semanticBytesSQL == "" {
					if modeInput.kind == fieldKindString {
						return nil, nil, nil, compileState{}, nil, errors.New(
							"compile ClickHouse aggregate: mode String-or-Bytes input lacks semantic Bytes provenance",
						)
					}
				}
				modeValuesInput := quoteIdentifier(fmt.Sprintf(
					"__os_measure_mode_values_%d",
					measureIndex,
				))
				modeSemanticInput := quoteIdentifier(fmt.Sprintf(
					"__os_measure_semantic_bytes_%d",
					measureIndex,
				))
				modeExistsSQL := modeInput.existsSQL
				if modeExistsSQL == "" {
					modeExistsSQL = "1"
				}
				modeValuesSQL := "if(" + modeExistsSQL + " AND isNotNull(" +
					modeInput.valueSQL + "), [assumeNotNull(" + modeInput.valueSQL +
					")], CAST([], 'Array(String)'))"
				modeSemanticSQL := "if(" + modeExistsSQL + " AND isNotNull(" +
					modeInput.valueSQL + "), [toUInt8(ifNull(" +
					modeInput.semanticBytesSQL + ", 0))], CAST([], 'Array(UInt8)'))"
				if modeInput.kind == fieldKindStringArray {
					modeValuesSQL = "if(" + modeExistsSQL + ", " +
						modeInput.valueSQL + ", CAST([], 'Array(String)'))"
					modeSemanticSQL = "arrayMap(value -> toUInt8(NOT isValidUTF8(value)), " +
						modeValuesSQL + ")"
				}
				next.preAggregateColumns = append(
					next.preAggregateColumns,
					modeValuesSQL+" AS "+modeValuesInput,
					modeSemanticSQL+" AS "+modeSemanticInput,
				)
				next.preAggregateArgs = append(
					next.preAggregateArgs,
					modeInput.existsArgs...,
				)
				next.preAggregateArgs = append(
					next.preAggregateArgs,
					modeInput.existsArgs...,
				)
				modeLowering := statsExactModeWithSemanticBytesSQL(
					modeValuesInput,
					modeSemanticInput,
				)
				lowering.SQL = modeLowering.ValueSQL
				modeSemanticOutput := quoteIdentifier(fmt.Sprintf(
					"__os_mode_semantic_bytes_%d",
					measureIndex,
				))
				projection = append(
					projection,
					modeLowering.SemanticBytesSQL+" AS "+modeSemanticOutput,
				)
				next.privateColumns = append(next.privateColumns, modeSemanticOutput)
				modeSemanticBytesSQL = modeSemanticOutput
			}
			projection = append(projection, lowering.SQL+" AS "+output)
			switch lowering.Result {
			case statsDistributionResultUInt64:
				measureState.numberType = "UInt64"
			case statsDistributionResultFloat64:
				measureState.numberType = "Float64"
			case statsDistributionResultNullableString:
				measureState.kind = fieldKindString
				measureState.numberType = ""
				measureState.existsSQL = "isNotNull(" + output + ")"
				if measure.Function == plan.AggregateFunctionMode {
					measureState.stringOrBytes = true
					measureState.stringOrBytesNullable = true
					measureState.semanticBytesByUTF8Validity = modeSemanticBytesSQL == ""
					measureState.semanticBytesSQL = modeSemanticBytesSQL
					if measureState.semanticBytesSQL == "" {
						measureState.semanticBytesSQL = "toUInt8(isNotNull(" + output +
							") AND NOT isValidUTF8(assumeNotNull(" + output + ")))"
					}
					measureState.textEligibleSQL = "(ifNull(" +
						measureState.semanticBytesSQL + ", 0) = 0 AND isNotNull(" +
						output + ") AND isValidUTF8(assumeNotNull(" + output + ")))"
					measureState.textEligibleBySemanticBytes = true
				}
				if measure.InputExpression != nil {
					cached, inputErr := aggregateExpressionInputFor(measure.InputExpression)
					if inputErr != nil {
						return nil, nil, nil, compileState{}, nil, inputErr
					}
					measureState.maxStringBytes = fieldStateStringByteBound(cached.field)
				} else if input, ok, resolveErr := resolveCompiledField(measure.Input, state); resolveErr != nil {
					return nil, nil, nil, compileState{}, nil, resolveErr
				} else if ok {
					measureState.maxStringBytes = fieldStateStringByteBound(input)
				}
			default:
				return nil, nil, nil, compileState{}, nil, errors.New(
					"compile ClickHouse aggregate: distribution result kind is invalid",
				)
			}
		case plan.AggregateFunctionDistinctCount, plan.AggregateFunctionValues:
			var inputSQL string
			var inputErr error
			if measure.InputExpression != nil {
				inputSQL, inputErr = stringInputForExpression(measure.InputExpression)
			} else {
				inputSQL, inputErr = stringInputFor(measure.Input)
			}
			if inputErr != nil {
				return nil, nil, nil, compileState{}, nil, inputErr
			}
			_, publishesValues := valuesInputs[measureInputKey]
			if measure.Function == plan.AggregateFunctionDistinctCount && !publishesValues {
				cardinalityColumn, cached := distinctCounts[measureInputKey]
				if !cached {
					cardinalityColumn = quoteIdentifier(fmt.Sprintf("__os_dc_cardinality_%d", len(distinctCounts)))
					distinctCounts[measureInputKey] = cardinalityColumn
					projection = append(projection, distinctCountCardinalitySQL(inputSQL)+" AS "+cardinalityColumn)
				}
				next.postAggregateDistinctCounts = append(next.postAggregateDistinctCounts, compiledDistinctCount{
					cardinalityColumn: cardinalityColumn,
					outputColumn:      output,
				})
				measureState.numberType = "UInt64"
			} else {
				setColumn, cached := exactStringSets[measureInputKey]
				if !cached {
					setColumn = quoteIdentifier(fmt.Sprintf("__os_exact_strings_%d", len(exactStringSets)))
					exactStringSets[measureInputKey] = setColumn
					projection = append(
						projection,
						exactDistinctStringSetSQL(inputSQL, uint64(MaximumStatsValuesPerGroup))+" AS "+setColumn,
					)
				}
				next.postAggregateExactStrings = append(next.postAggregateExactStrings, compiledExactStringMeasure{
					setColumn:    setColumn,
					outputColumn: output,
					function:     measure.Function,
				})
				if measure.Function == plan.AggregateFunctionDistinctCount {
					measureState.numberType = "UInt64"
				} else {
					measureState.kind = fieldKindStringArray
					measureState.mvSortedLexicographic = true
					measureState.stringOrBytes = true
					// The physical result is always a non-null Array(String), but an
					// empty multivalue has no logical SPL field value.
					measureState.existsSQL = "notEmpty(" + output + ")"
				}
			}
		case plan.AggregateFunctionList:
			inputExists := false
			if measure.InputExpression != nil {
				cached, inputErr := aggregateExpressionInputFor(measure.InputExpression)
				if inputErr != nil {
					return nil, nil, nil, compileState{}, nil, inputErr
				}
				inputExists = !cached.field.alwaysNull
			} else {
				_, resolved, resolveErr := resolveCompiledField(measure.Input, state)
				if resolveErr != nil {
					return nil, nil, nil, compileState{}, nil, resolveErr
				}
				inputExists = resolved
			}
			if !inputExists {
				// Preserve global aggregate and retained-group row semantics with
				// one constant-size aggregate state. There is no row order to
				// recover for a field that is statically absent, so ordered
				// prefix windows would only sort the entire input to publish [].
				projection = append(
					projection,
					"groupArrayArray(1)(CAST([], 'Array(String)')) AS "+output,
				)
				measureState.kind = fieldKindStringArray
				measureState.stringOrBytes = true
				measureState.existsSQL = "notEmpty(" + output + ")"
				break
			}
			var inputSQL string
			var inputErr error
			if measure.InputExpression != nil {
				inputSQL, inputErr = stringInputForExpression(measure.InputExpression)
			} else {
				inputSQL, inputErr = stringInputFor(measure.Input)
			}
			if inputErr != nil {
				return nil, nil, nil, compileState{}, nil, inputErr
			}
			rowOrdinal, windowOrder, orderErr := listRowOrdinalFor()
			if orderErr != nil {
				return nil, nil, nil, compileState{}, nil, orderErr
			}
			list, cached := orderedStringLists[measureInputKey]
			if !cached {
				ordinal := len(orderedStringLists)
				priorElements := quoteIdentifier(fmt.Sprintf(
					"__os_list_prior_elements_%d",
					ordinal,
				))
				priorBytes := quoteIdentifier(fmt.Sprintf(
					"__os_list_prior_bytes_%d",
					ordinal,
				))
				frame := " OVER (" + windowOrder +
					" ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)"
				next.preAggregateListWindowColumns = append(
					next.preAggregateListWindowColumns,
					"ifNull(sum(toUInt128(length("+inputSQL+")))"+frame+
						", toUInt128(0)) AS "+priorElements,
					"ifNull(sum("+stringArrayPayloadBytesSQL(inputSQL)+")"+frame+
						", toUInt128(0)) AS "+priorBytes,
				)
				rowState := quoteIdentifier(fmt.Sprintf(
					"__os_list_row_state_%d",
					ordinal,
				))
				next.preAggregateListCandidateColumns = append(
					next.preAggregateListCandidateColumns,
					boundedOrderedStringRowStateSQL(
						rowOrdinal,
						inputSQL,
						priorElements,
						priorBytes,
					)+" AS "+rowState,
				)
				list.listColumn = quoteIdentifier(fmt.Sprintf(
					"__os_ordered_strings_%d",
					ordinal,
				))
				list.overflowColumn = quoteIdentifier(fmt.Sprintf(
					"__os_ordered_strings_bytes_overflow_%d",
					ordinal,
				))
				orderedStringLists[measureInputKey] = list
				projection = append(
					projection,
					boundedOrderedStringListSQL("tupleElement("+rowState+", 1)")+
						" AS "+list.listColumn,
					"max(tupleElement("+rowState+", 2)) AS "+list.overflowColumn,
				)
			}
			next.postAggregateOrderedStrings = append(
				next.postAggregateOrderedStrings,
				compiledOrderedStringMeasure{
					listColumn:     list.listColumn,
					overflowColumn: list.overflowColumn,
					outputColumn:   output,
				},
			)
			measureState.kind = fieldKindStringArray
			measureState.stringOrBytes = true
			// As with values(), an empty physical array has no logical SPL value.
			measureState.existsSQL = "notEmpty(" + output + ")"
		default:
			return nil, nil, nil, compileState{}, nil, fmt.Errorf("compile ClickHouse aggregate: unsupported function %d", measure.Function)
		}
		switch measure.Function {
		case plan.AggregateFunctionMinimum, plan.AggregateFunctionMaximum,
			plan.AggregateFunctionFirst, plan.AggregateFunctionLast,
			plan.AggregateFunctionEarliest, plan.AggregateFunctionLatest:
			var input fieldState
			var ok bool
			if measure.InputExpression != nil {
				cached, inputErr := aggregateExpressionInputFor(measure.InputExpression)
				if inputErr != nil {
					return nil, nil, nil, compileState{}, nil, inputErr
				}
				input = cached.field
				ok = !input.alwaysNull
			} else {
				var resolveErr error
				input, ok, resolveErr = resolveCompiledField(measure.Input, state)
				if resolveErr != nil {
					return nil, nil, nil, compileState{}, nil, resolveErr
				}
			}
			if ok {
				measureState.maxStringBytes = fieldStateStringByteBound(input)
			}
		}
		if measure.Function == plan.AggregateFunctionValues ||
			measure.Function == plan.AggregateFunctionList {
			// delim is presentation metadata only. Keep the aggregate as a typed
			// Array(String) and bind the effective default/authored delimiter to
			// this exact output field for downstream exact projections.
			measureState.flatMultivalueDelimiter = strings.Clone(statsOptions.Delimiter)
			measureState.hasFlatMultivalueDelimiter = true
		}
		next.visible[measure.Output] = measureState
		next.publicOrder = append(next.publicOrder, measure.Output)
		if len(next.order) == 0 {
			orderSQL := quoteIdentifier(measure.Output)
			if measureState.kind == fieldKindDynamic {
				orderSQL = quoteIdentifier("__os_aggregate_order")
				projection = append(projection, "toUInt8(0) AS "+orderSQL)
			}
			next.order = append(next.order, compiledSortKey{valueSQL: orderSQL})
		}
	}
	if len(dynamicGroupInvalid) > 0 {
		if next.context == nil {
			return nil, nil, nil, compileState{}, nil, errors.New(
				"compile ClickHouse aggregate: runtime validation context is missing",
			)
		}
		// Raw Dynamic BY inputs are validated at execution because their runtime
		// shape can be a scalar, an admitted scalar-member multivalue, or an
		// unsupported nested container. A backend iterator may discover a late
		// unsupported value only after yielding otherwise valid groups, so the
		// complete result must remain staged until validation and Close succeed.
		next.context.atomicResult = true
		anyUnsupportedColumn := quoteIdentifier("__os_stats_by_any_unsupported")
		invalid := "(" + strings.Join(dynamicGroupInvalid, ") OR (") + ")"
		next.preAggregateValidationColumns = append(next.preAggregateValidationColumns,
			"max(CAST("+invalid+" AS UInt8)) OVER () AS "+anyUnsupportedColumn,
		)
		next.preAggregateValidationArgs = append(next.preAggregateValidationArgs, dynamicGroupInvalidArgs...)
		eligible := "1"
		if len(predicates) > 0 {
			eligible = "(" + strings.Join(predicates, " AND ") + ")"
		}
		predicates = []string{
			"if(" + anyUnsupportedColumn + " != 0, throwIf(toUInt8(1), '" + UnsupportedStatsByValueMarker + "') = 0, " + eligible + ")",
		}
	}
	return projection, predicates, groups, next, args, nil
}

func resolveCountValueInput(
	input plan.FieldRef,
	state compileState,
) (string, []any, error) {
	field, resolved, err := resolveCompiledField(input, state)
	if err != nil {
		return "", nil, err
	}
	if !resolved {
		return "toUInt64(0)", nil, nil
	}
	inputSQL, args := countValueInputSQL(field)
	return inputSQL, args, nil
}

func countValueInputSQL(field fieldState) (string, []any) {
	if field.kind == fieldKindStringArray {
		// A fixed multivalue is physically non-null and its empty representation
		// has cardinality zero, so its logical presence predicate is unnecessary.
		return "toUInt64(length(" + field.valueSQL + "))", nil
	}
	if field.kind == fieldKindDynamicArray {
		cardinality := "toUInt64(arrayCount(element -> dynamicType(element) != 'None', " +
			field.valueSQL + "))"
		if field.existsSQL != "" && field.existsSQL != "1" {
			return "if(" + field.existsSQL + ", " + cardinality +
				", toUInt64(0))", append([]any(nil), field.existsArgs...)
		}
		return cardinality, nil
	}

	existsSQL := field.existsSQL
	if existsSQL == "" {
		existsSQL = "1"
	}
	args := append([]any(nil), field.existsArgs...)
	if field.kind != fieldKindDynamic {
		return "toUInt64((" + existsSQL + ") AND isNotNull(" + field.valueSQL + "))", args
	}

	typeSQL := dynamicTypeExpression(field)
	arrayCount := dynamicNonNullArrayCardinalitySQL(field.valueSQL)
	descendantCount := "toUInt64(0)"
	if field.descendantSQL != "" {
		// Non-empty typed objects are stored as flattened leaves. The object
		// parent is still one present field occurrence. Calculated field copies
		// can retain those descendants while binding an exact Dynamic None, so
		// descendant presence must also be the None fallback.
		descendantCount = "if(" + field.descendantSQL + ", toUInt64(1), toUInt64(0))"
		args = append(args, field.descendantArgs...)
	}
	return "if((" + existsSQL + ") AND " + typeSQL + " != 'None', " +
		"multiIf(" +
		typeSQL + " = 'Array(Dynamic)', " + arrayCount + ", " +
		"startsWith(" + typeSQL + ", 'Array('), " +
		"ifNull(toUInt64(length(" + field.valueSQL + ")), toUInt64(0)), " +
		"toUInt64(1)), " +
		descendantCount + ")", args
}

func dynamicNonNullArrayCardinalitySQL(valueSQL string) string {
	return "toUInt64(arrayCount(element -> dynamicType(element) != 'None', " +
		"dynamicElement(" + valueSQL + ", 'Array(Dynamic)')))"
}

func percentileInputSQL(field fieldState) string {
	nullFloat := "CAST(NULL AS Nullable(Float64))"
	switch field.kind {
	case fieldKindNumber:
		return "ifNotFinite(toFloat64(" + field.valueSQL + "), " + nullFloat + ")"
	case fieldKindTime:
		return "ifNotFinite(toFloat64(toUnixTimestamp64Nano(" + field.valueSQL + ")) / 1000000000, " + nullFloat + ")"
	case fieldKindDynamic:
		return dynamicFiniteFloatOrNullSQL(field.valueSQL, dynamicTypeExpression(field))
	case fieldKindString:
		return finiteFloatOrNullSQL(field.valueSQL)
	default:
		return nullFloat
	}
}

func statsPercentileLevelSQL(percentile uint8) string {
	if percentile%10 == 0 {
		return "0." + strconv.Itoa(int(percentile/10))
	}
	return fmt.Sprintf("0.%02d", percentile)
}

func singlePercentileArrayAggregateSQL(
	percentile uint8,
	inputSQL string,
) string {
	return "arrayElementOrNull(quantilesGKOrNullArray(100, " +
		statsPercentileLevelSQL(percentile) + ")(" + inputSQL + "), 1)"
}

func numericArrayAggregateSQL(function plan.AggregateFunction, inputSQL string) (string, bool) {
	switch function {
	case plan.AggregateFunctionSum:
		return "sumOrNullArray(" + inputSQL + ")", true
	case plan.AggregateFunctionAverage:
		return "avgOrNullArray(" + inputSQL + ")", true
	default:
		return "", false
	}
}

func numericArrayInputSQL(field fieldState) (string, []any) {
	empty := "CAST([], 'Array(Float64)')"
	if field.kind == fieldKindStringArray {
		value := compactNullableArraySQL(
			"arrayMap(element -> " + finiteFloatOrNullSQL("element") + ", " + field.valueSQL + ")",
		)
		if field.existsSQL == "" || field.existsSQL == "1" {
			return value, nil
		}
		return "if(" + field.existsSQL + ", " + value + ", " + empty + ")",
			append([]any(nil), field.existsArgs...)
	}
	if field.kind == fieldKindDynamicArray {
		value := compactNullableArraySQL(
			"arrayMap(element -> " + dynamicFiniteFloatOrNullSQL(
				"element", "dynamicType(element)",
			) + ", " + field.valueSQL + ")",
		)
		if field.existsSQL == "" || field.existsSQL == "1" {
			return value, nil
		}
		return "if(" + field.existsSQL + ", " + value + ", " + empty + ")",
			append([]any(nil), field.existsArgs...)
	}
	scalar := percentileInputSQL(field)
	scalarArray := compactNullableArraySQL("[" + scalar + "]")
	value := scalarArray
	if field.kind == fieldKindDynamic {
		element := dynamicFiniteFloatOrNullSQL("element", "dynamicType(element)")
		array := compactNullableArraySQL(
			"arrayMap(element -> " + element + ", dynamicElement(" + field.valueSQL + ", 'Array(Dynamic)'))",
		)
		value = "if(" + dynamicTypeExpression(field) + " = 'Array(Dynamic)', " + array + ", " + scalarArray + ")"
	}
	if field.existsSQL == "" || field.existsSQL == "1" {
		return value, nil
	}
	return "if(" + field.existsSQL + ", " + value + ", " + empty + ")", append([]any(nil), field.existsArgs...)
}

func stringArrayInputSQL(field fieldState) (string, []any) {
	empty := "CAST([], 'Array(String)')"
	if field.kind == fieldKindStringArray {
		if field.existsSQL == "" || field.existsSQL == "1" {
			return field.valueSQL, nil
		}
		return "if(" + field.existsSQL + ", " + field.valueSQL + ", " + empty + ")",
			append([]any(nil), field.existsArgs...)
	}
	if field.kind == fieldKindDynamicArray {
		value := "arrayMap(element -> " + nativeMVCanonicalTextSQL("element") +
			", arrayFilter(element -> dynamicType(element) != 'None', " +
			field.valueSQL + "))"
		if field.existsSQL == "" || field.existsSQL == "1" {
			return value, nil
		}
		return "if(" + field.existsSQL + ", " + value + ", " + empty + ")",
			append([]any(nil), field.existsArgs...)
	}
	if field.kind == fieldKindDynamic {
		state, args := dynamicStringArrayStateSQL(field, "1")
		stateAlias := "__os_dynamic_string_array_state"
		body := "if(throwIf(toUInt8(tupleElement(" + stateAlias + ", 2)), '" +
			UnsupportedStatsMeasureValueMarker + "') = 0, tupleElement(" +
			stateAlias + ", 1), " + empty + ")"
		return bindSQLExpressions(
			[]string{stateAlias},
			[]string{state},
			body,
		), args
	}
	scalar := statsTextEligibleScalarStringOrNullSQL(field)
	value := compactNullableArraySQL("[" + scalar + "]")
	if field.existsSQL == "" || field.existsSQL == "1" {
		return value, nil
	}
	return "if(" + field.existsSQL + ", " + value + ", " + empty + ")", append([]any(nil), field.existsArgs...)
}

// dynamicStringArrayStateSQL normalizes one Dynamic field into an exact String
// array plus an unsupported-container bit. Callers choose whether to throw
// immediately or retain the bit for deferred whole-result validation. A false
// row-eligibility expression short-circuits member inspection.
func dynamicStringArrayStateSQL(
	field fieldState,
	rowEligibleSQL string,
) (string, []any) {
	emptyValues := "CAST([], 'Array(String)')"
	empty := "tuple(" + emptyValues + ", toUInt8(0))"
	scalar := compileDynamicMeasureScalar(field)
	invalid := "tuple(" + emptyValues + ", toUInt8(1))"
	scalarState := "tuple(if(" + scalar.eligibleSQL + ", [" +
		scalar.lexicalSQL + "], " + emptyValues + "), toUInt8(" +
		scalar.invalidSQL + "))"

	element := fieldState{
		valueSQL:       "element",
		dynamicTypeSQL: "dynamicType(element)",
		kind:           fieldKindDynamic,
	}
	member := compileDynamicMeasureScalar(element)
	nullString := "CAST(NULL AS Nullable(String))"
	dynamicValues := "dynamicElement(" + field.valueSQL + ", 'Array(Dynamic)')"
	memberStates := "arrayMap(element -> tuple(" +
		"if(" + member.eligibleSQL + ", CAST(" + member.lexicalSQL +
		" AS Nullable(String)), " + nullString + "), toUInt8(" +
		member.invalidSQL + ")), " + dynamicValues + ")"
	memberStatesAlias := "__os_dynamic_string_member_states"
	arrayState := bindSQLExpressions(
		[]string{memberStatesAlias},
		[]string{memberStates},
		"tuple("+compactNullableArraySQL(
			"arrayMap(member -> tupleElement(member, 1), "+memberStatesAlias+")",
		)+", toUInt8(arrayExists(member -> tupleElement(member, 2) != 0, "+
			memberStatesAlias+")))",
	)

	existsSQL, descendantSQL, args := dynamicPresenceOperands(field)
	if rowEligibleSQL == "" {
		rowEligibleSQL = "1"
	}
	value := "multiIf(" +
		"row_eligible = 0, " + empty + ", " +
		"descendant_present != 0, " + invalid + ", " +
		"field_present = 0 OR " + scalar.typeSQL + " = 'None', " + empty + ", " +
		scalar.typeSQL + " = 'Array(Dynamic)', " + arrayState + ", " +
		scalarState + ")"
	return "arrayElement(arrayMap((row_eligible, field_present, descendant_present) -> " +
		value + ", [toUInt8(" + rowEligibleSQL + ")], [toUInt8(" + existsSQL +
		")], [toUInt8(" + descendantSQL + ")]), 1)", args
}

// eventStatsExactStringDynamicMeasureSQL normalizes the shared eventstats dc,
// values, and list input while retaining unsupported data as a constant-size
// bit, so downstream projection or limits cannot hide failure.
func eventStatsExactStringDynamicMeasureSQL(
	field fieldState,
	rowEligibleSQL string,
) (string, []any) {
	return dynamicStringArrayStateSQL(field, rowEligibleSQL)
}

type chronologicalDirections struct {
	earliest bool
	latest   bool
}

func emptyChronologicalCandidatesSQL() string {
	return "tuple(CAST('' AS String), CAST('' AS String), " +
		"toUInt8(0), toUInt8(0), toUInt64(0), toUInt64(0))"
}

func immutableChronologicalRowKeySQL() string {
	return "tuple(" + strings.Join([]string{
		quoteIdentifier(internalSortTimeColumn),
		quoteIdentifier(internalSortIDColumn),
		quoteIdentifier(internalSortVisibilityColumn),
		quoteIdentifier(internalSortSourceIdentityColumn),
	}, ", ") + ")"
}

func emptySingleChronologicalCandidateSQL() string {
	return "tuple(CAST('' AS String), toUInt64(0), toUInt8(0), toUInt8(0))"
}

// singleChronologicalCandidateSQL reduces one event field to the only
// direction a single-measure chronological aggregate consumes: selected lexical
// value, original one-based member ordinal, eligible bit, and invalid bit.
// Unlike multi-measure stats, this avoids selecting and retaining the opposite
// end of every multivalue.
func singleChronologicalCandidateSQL(
	function plan.AggregateFunction,
	field fieldState,
	exists bool,
) (string, []any, bool, error) {
	arrayIndexSelector := "arrayFirstIndex"
	fixedArrayIndex := "1"
	switch function {
	case plan.AggregateFunctionEarliest:
	case plan.AggregateFunctionLatest:
		arrayIndexSelector = "arrayLastIndex"
		fixedArrayIndex = "-1"
	default:
		return "", nil, false, fmt.Errorf(
			"compile ClickHouse chronological candidate: unsupported function %d",
			function,
		)
	}

	empty := emptySingleChronologicalCandidateSQL()
	if !exists {
		return empty, nil, false, nil
	}
	if field.kind == fieldKindDynamicArray {
		field.valueSQL = "arrayMap(element -> " + nativeMVCanonicalTextSQL("element") +
			", arrayFilter(element -> dynamicType(element) != 'None', " +
			field.valueSQL + "))"
		field.kind = fieldKindStringArray
		return singleChronologicalCandidateSQL(function, field, true)
	}

	if field.kind == fieldKindStringArray {
		values := field.valueSQL
		var args []any
		if field.existsSQL != "" && field.existsSQL != "1" {
			values = "if(" + field.existsSQL + ", " + values +
				", CAST([], 'Array(String)'))"
			args = append(args, field.existsArgs...)
		}
		count := "toUInt64(length(" + values + "))"
		ordinal := "toUInt64(1)"
		if function == plan.AggregateFunctionLatest {
			ordinal = count
		}
		return "tuple(" +
				"if(" + count + " != 0, arrayElement(" + values + ", " +
				fixedArrayIndex + "), CAST('' AS String)), " +
				"if(" + count + " != 0, " + ordinal + ", toUInt64(0)), " +
				"toUInt8(" + count + " != 0), toUInt8(0))",
			args,
			false,
			nil
	}

	if field.kind != fieldKindDynamic {
		value := statsScalarStringOrNullSQL(field)
		existsSQL := field.existsSQL
		if existsSQL == "" {
			existsSQL = "1"
		}
		value = "if(" + existsSQL + ", " + value +
			", CAST(NULL AS Nullable(String)))"
		present := "isNotNull(" + value + ")"
		return "tuple(" +
				"ifNull(" + value + ", CAST('' AS String)), " +
				"if(" + present + ", toUInt64(1), toUInt64(0)), " +
				"toUInt8(" + present + "), toUInt8(0))",
			append([]any(nil), field.existsArgs...),
			false,
			nil
	}

	typeSQL := dynamicTypeExpression(field)
	scalarSupported, scalarLexical := statsByScalarExpressions(field)
	element := fieldState{
		valueSQL:       "element",
		dynamicTypeSQL: "dynamicType(element)",
		kind:           fieldKindDynamic,
	}
	elementSupported, _ := statsByScalarExpressions(element)
	elementType := dynamicTypeExpression(element)
	elementEligible := "(" + elementType + " != 'None' AND " + elementSupported + ")"
	elementInvalid := "(" + elementType + " != 'None' AND NOT (" + elementSupported + "))"
	values := "dynamicElement(" + field.valueSQL + ", 'Array(Dynamic)')"
	ordinal := "toUInt64(" + arrayIndexSelector + "(element -> " +
		elementEligible + ", " + values + "))"
	memberInvalid := "toUInt8(arrayExists(element -> " + elementInvalid +
		", " + values + "))"
	selectedElement := fieldState{
		valueSQL:       "arrayElement(" + values + ", selected_ordinal)",
		dynamicTypeSQL: "dynamicType(arrayElement(" + values + ", selected_ordinal))",
		kind:           fieldKindDynamic,
	}
	_, selectedLexical := statsByScalarExpressions(selectedElement)
	selectedState := "arrayElement(arrayMap(" +
		"(selected_ordinal, member_invalid) -> tuple(" +
		"if(selected_ordinal != 0, " + selectedLexical + ", CAST('' AS String)), " +
		"selected_ordinal, toUInt8(selected_ordinal != 0), member_invalid), " +
		"[" + ordinal + "], [" + memberInvalid + "]), 1)"
	scalar := "tuple(" + scalarLexical +
		", toUInt64(1), toUInt8(1), toUInt8(0))"
	invalid := "tuple(CAST('' AS String), toUInt64(0), toUInt8(0), toUInt8(1))"

	existsSQL, descendantSQL, args := dynamicPresenceOperands(field)
	value := "multiIf(" +
		"descendant_present != 0, " + invalid + ", " +
		"field_present = 0 OR " + typeSQL + " = 'None', " + empty + ", " +
		typeSQL + " = 'Array(Dynamic)', " + selectedState + ", " +
		scalarSupported + ", " + scalar + ", " +
		invalid + ")"
	return "arrayElement(arrayMap((field_present, descendant_present) -> " + value +
			", [toUInt8(" + existsSQL + ")], [toUInt8(" + descendantSQL + ")]), 1)",
		args,
		true,
		nil
}

func singleChronologicalAggregateSQL(
	function plan.AggregateFunction,
	inputSQL string,
) (string, error) {
	candidate := "tupleElement(" + inputSQL + ", 1)"
	rowKey := "tupleElement(" + inputSQL + ", 2)"
	aggregate := "argMinOrNullIf"
	switch function {
	case plan.AggregateFunctionEarliest:
	case plan.AggregateFunctionLatest:
		aggregate = "argMaxOrNullIf"
	default:
		return "", fmt.Errorf(
			"compile ClickHouse single chronological aggregate: unsupported function %d",
			function,
		)
	}
	value := "tupleElement(" + candidate + ", 1)"
	ordinal := "tupleElement(" + candidate + ", 2)"
	present := "tupleElement(" + candidate + ", 3)"
	key := "tuple(" + rowKey + ", " + ordinal + ")"
	return aggregate + "(" + value + ", " + key + ", " + present + " != 0)", nil
}

func chronologicalAggregateSQL(
	function plan.AggregateFunction,
	candidatesSQL string,
	rowKeySQL string,
	multiple bool,
) (string, error) {
	eligible := "tupleElement(" + candidatesSQL + ", 3)"
	value := "tupleElement(" + candidatesSQL + ", 1)"
	ordinal := "toUInt64(1)"
	aggregate := "argMinOrNullIf"
	switch function {
	case plan.AggregateFunctionEarliest, plan.AggregateFunctionFirst:
		if multiple {
			ordinal = "tupleElement(" + candidatesSQL + ", 5)"
		}
	case plan.AggregateFunctionLatest, plan.AggregateFunctionLast:
		value = "tupleElement(" + candidatesSQL + ", 2)"
		if multiple {
			ordinal = "tupleElement(" + candidatesSQL + ", 6)"
		}
		aggregate = "argMaxOrNullIf"
	default:
		return "", fmt.Errorf(
			"compile ClickHouse chronological aggregate: unsupported function %d",
			function,
		)
	}
	key := "tuple(" + rowKeySQL + ", " + ordinal + ")"
	return aggregate + "(" + value + ", " + key + ", " + eligible + " != 0)", nil
}

func statsOccurrenceTimeAggregateSQL(
	function plan.AggregateFunction,
	candidatesSQL string,
	rowKeySQL string,
	timeSQL string,
) (string, error) {
	aggregate := "argMinOrNullIf"
	switch function {
	case plan.AggregateFunctionEarliestTime:
	case plan.AggregateFunctionLatestTime:
		aggregate = "argMaxOrNullIf"
	default:
		return "", fmt.Errorf(
			"compile ClickHouse occurrence-time aggregate: unsupported function %d",
			function,
		)
	}
	eligible := "tupleElement(" + candidatesSQL + ", 3) != 0"
	invalid := "tupleElement(" + candidatesSQL + ", 4) != 0"
	winner := aggregate + "(" + timeSQL + ", " + rowKeySQL + ", " + eligible + ")"
	return "if(max(toUInt8(" + invalid + ")) != 0, toFloat64(throwIf(" +
		"toUInt8(1), '" + UnsupportedStatsMeasureValueMarker + "')), " + winner + ")", nil
}

// statsRateAggregateSQL implements the documented no-reset endpoint formula.
// Splunk's separate "largest value reset" behavior is not specified well
// enough by the pinned reference to reproduce without a differential oracle;
// that case remains explicitly tracked in the stats parity inventory.
func statsRateAggregateSQL(inputSQL, rowKeySQL, timeSQL string) string {
	firstValue := "arrayElementOrNull(" + inputSQL + ", 1)"
	lastValue := "arrayElementOrNull(" + inputSQL + ", -1)"
	firstEligible := "isNotNull(" + firstValue + ")"
	lastEligible := "isNotNull(" + lastValue + ")"
	earliestValue := "argMinOrNullIf(" + firstValue + ", " + rowKeySQL + ", " + firstEligible + ")"
	latestValue := "argMaxOrNullIf(" + lastValue + ", " + rowKeySQL + ", " + lastEligible + ")"
	earliestTime := "argMinOrNullIf(" + timeSQL + ", " + rowKeySQL + ", " + firstEligible + ")"
	latestTime := "argMaxOrNullIf(" + timeSQL + ", " + rowKeySQL + ", " + lastEligible + ")"
	pointCount := "countIf(" + firstEligible + " OR " + lastEligible + ")"
	duration := "(" + latestTime + " - " + earliestTime + ")"
	nullFloat := "CAST(NULL AS Nullable(Float64))"
	return "if(" + pointCount + " < 2 OR isNull(" + duration + ") OR " + duration +
		" = 0, " + nullFloat + ", ifNotFinite((" + latestValue + " - " +
		earliestValue + ") / " + duration + ", " + nullFloat + "))"
}

func eventStatsChronologicalValidationSQL(inputSQL, _ string) string {
	return "maxOrDefault(toUInt8(tupleElement(tupleElement(" + inputSQL +
		", 1), 4)))"
}

func chronologicalPublishedValueSQL(winnerSQL string) string {
	nonNull := "assumeNotNull(" + winnerSQL + ")"
	return "if(isNull(" + winnerSQL + "), CAST(NULL AS Dynamic), if(" +
		"isValidUTF8(" + nonNull + "), CAST(" + nonNull + " AS Dynamic), " +
		bytesEnvelopePayloadDynamicSQL(rawStdBase64EncodeSQL(nonNull)) + "))"
}

func chronologicalPublishedTypeSQL(winnerSQL string) string {
	return statsExtremaStoredTypeFromConditionsSQL(
		"isNull("+winnerSQL+")",
		"0",
		"1",
		"assumeNotNull("+winnerSQL+")",
	)
}

// chronologicalCandidatesSQL normalizes one event field to a constant-size
// tuple: requested first and/or last eligible lexical value, an eligible bit,
// unsupported-container bit, and the original one-based requested ordinals.
// Each requested direction uses one bounded index pass over a Dynamic
// multivalue; guarded indexed lookup avoids repeating the eligibility pass or
// retaining either an Array ordering key or normalized member array.
func chronologicalCandidatesSQL(
	field fieldState,
	exists bool,
	directions chronologicalDirections,
) (string, []any, bool) {
	empty := emptyChronologicalCandidatesSQL()
	if !exists {
		return empty, nil, false
	}
	if field.kind == fieldKindDynamicArray {
		field.valueSQL = "arrayMap(element -> " + nativeMVCanonicalTextSQL("element") +
			", arrayFilter(element -> dynamicType(element) != 'None', " +
			field.valueSQL + "))"
		field.kind = fieldKindStringArray
		return chronologicalCandidatesSQL(field, true, directions)
	}

	if field.kind == fieldKindStringArray {
		values := field.valueSQL
		var args []any
		if field.existsSQL != "" && field.existsSQL != "1" {
			values = "if(" + field.existsSQL + ", " + values +
				", CAST([], 'Array(String)'))"
			args = append(args, field.existsArgs...)
		}
		count := "toUInt64(length(" + values + "))"
		firstValue := "CAST('' AS String)"
		firstOrdinal := "toUInt64(0)"
		if directions.earliest {
			firstValue = "if(" + count + " != 0, arrayElement(" + values +
				", 1), CAST('' AS String))"
			firstOrdinal = "if(" + count + " != 0, toUInt64(1), toUInt64(0))"
		}
		lastValue := "CAST('' AS String)"
		lastOrdinal := "toUInt64(0)"
		if directions.latest {
			lastValue = "if(" + count + " != 0, arrayElement(" + values +
				", -1), CAST('' AS String))"
			lastOrdinal = count
		}
		eligibleOrdinal := firstOrdinal
		if !directions.earliest {
			eligibleOrdinal = lastOrdinal
		}
		return "tuple(" + firstValue + ", " + lastValue + ", toUInt8(" +
				eligibleOrdinal + " != 0), toUInt8(0), " + firstOrdinal + ", " +
				lastOrdinal + ")",
			args,
			false
	}

	if field.kind != fieldKindDynamic {
		value := statsScalarStringOrNullSQL(field)
		existsSQL := field.existsSQL
		if existsSQL == "" {
			existsSQL = "1"
		}
		value = "if(" + existsSQL + ", " + value +
			", CAST(NULL AS Nullable(String)))"
		present := "isNotNull(" + value + ")"
		firstValue := "CAST('' AS String)"
		firstOrdinal := "toUInt64(0)"
		if directions.earliest {
			firstValue = "ifNull(" + value + ", CAST('' AS String))"
			firstOrdinal = "if(" + present + ", toUInt64(1), toUInt64(0))"
		}
		lastValue := "CAST('' AS String)"
		lastOrdinal := "toUInt64(0)"
		if directions.latest {
			lastValue = "ifNull(" + value + ", CAST('' AS String))"
			lastOrdinal = "if(" + present + ", toUInt64(1), toUInt64(0))"
		}
		return "tuple(" + firstValue + ", " + lastValue + ", toUInt8(" +
				present + "), toUInt8(0), " + firstOrdinal + ", " + lastOrdinal + ")",
			append([]any(nil), field.existsArgs...),
			false
	}

	typeSQL := dynamicTypeExpression(field)
	scalarSupported, scalarLexical := statsByScalarExpressions(field)
	element := fieldState{
		valueSQL:       "element",
		dynamicTypeSQL: "dynamicType(element)",
		kind:           fieldKindDynamic,
	}
	elementSupported, _ := statsByScalarExpressions(element)
	elementType := dynamicTypeExpression(element)
	elementEligible := "(" + elementType + " != 'None' AND " + elementSupported + ")"
	elementInvalid := "(" + elementType + " != 'None' AND NOT (" + elementSupported + "))"
	values := "dynamicElement(" + field.valueSQL + ", 'Array(Dynamic)')"
	firstOrdinal := "toUInt64(0)"
	if directions.earliest {
		firstOrdinal = "toUInt64(arrayFirstIndex(element -> " + elementEligible +
			", " + values + "))"
	}
	lastOrdinal := "toUInt64(0)"
	if directions.latest {
		lastOrdinal = "toUInt64(arrayLastIndex(element -> " + elementEligible +
			", " + values + "))"
	}
	memberInvalid := "toUInt8(arrayExists(element -> " + elementInvalid +
		", " + values + "))"
	firstLexical := "CAST('' AS String)"
	if directions.earliest {
		firstElement := fieldState{
			valueSQL:       "arrayElement(" + values + ", first_ordinal)",
			dynamicTypeSQL: "dynamicType(arrayElement(" + values + ", first_ordinal))",
			kind:           fieldKindDynamic,
		}
		_, lexical := statsByScalarExpressions(firstElement)
		firstLexical = "if(first_ordinal != 0, " + lexical +
			", CAST('' AS String))"
	}
	lastLexical := "CAST('' AS String)"
	if directions.latest {
		lastElement := fieldState{
			valueSQL:       "arrayElement(" + values + ", last_ordinal)",
			dynamicTypeSQL: "dynamicType(arrayElement(" + values + ", last_ordinal))",
			kind:           fieldKindDynamic,
		}
		_, lexical := statsByScalarExpressions(lastElement)
		lastLexical = "if(last_ordinal != 0, " + lexical +
			", CAST('' AS String))"
	}
	eligibleOrdinal := "first_ordinal"
	if !directions.earliest {
		eligibleOrdinal = "last_ordinal"
	}
	selected := "arrayElement(arrayMap(" +
		"(first_ordinal, last_ordinal, member_invalid) -> tuple(" +
		firstLexical + ", " + lastLexical + ", toUInt8(" + eligibleOrdinal +
		" != 0), member_invalid, first_ordinal, last_ordinal), " +
		"[" + firstOrdinal + "], [" + lastOrdinal + "], [" + memberInvalid + "]), 1)"
	scalarFirst := "CAST('' AS String)"
	scalarFirstOrdinal := "toUInt64(0)"
	if directions.earliest {
		scalarFirst = scalarLexical
		scalarFirstOrdinal = "toUInt64(1)"
	}
	scalarLast := "CAST('' AS String)"
	scalarLastOrdinal := "toUInt64(0)"
	if directions.latest {
		scalarLast = scalarLexical
		scalarLastOrdinal = "toUInt64(1)"
	}
	scalar := "tuple(" + scalarFirst + ", " + scalarLast +
		", toUInt8(1), toUInt8(0), " + scalarFirstOrdinal + ", " +
		scalarLastOrdinal + ")"
	invalid := "tuple(CAST('' AS String), CAST('' AS String), toUInt8(0), " +
		"toUInt8(1), toUInt64(0), toUInt64(0))"

	existsSQL, descendantSQL, args := dynamicPresenceOperands(field)
	value := "multiIf(" +
		"descendant_present != 0, " + invalid + ", " +
		"field_present = 0 OR " + typeSQL + " = 'None', " + empty + ", " +
		typeSQL + " = 'Array(Dynamic)', " + selected + ", " +
		scalarSupported + ", " + scalar + ", " +
		invalid + ")"
	return "arrayElement(arrayMap((field_present, descendant_present) -> " + value +
			", [toUInt8(" + existsSQL + ")], [toUInt8(" + descendantSQL + ")]), 1)",
		args,
		true
}

func statsScalarStringInputSQL(field fieldState) (string, []any) {
	value := statsScalarStringOrNullSQL(field)
	existsSQL := field.existsSQL
	if existsSQL == "" || existsSQL == "1" {
		return value, nil
	}
	return "if(" + existsSQL + ", " + value +
		", CAST(NULL AS Nullable(String)))", append([]any(nil), field.existsArgs...)
}

func fixedStringExtremaRawBytesSQL(field fieldState) string {
	if field.kind != fieldKindString || field.textEligibleSQL == "" {
		return "0"
	}
	return "NOT ifNull(" + field.textEligibleSQL + ", 0)"
}

func statsExtremaScalarNumberSQL(valueSQL string) string {
	value := "ifNull(" + valueSQL + ", CAST('' AS String))"
	return "if(isNotNull(" + valueSQL + "), " + statsExtremaNumericOrNullSQL(value) +
		", CAST(NULL AS Nullable(Float64)))"
}

func statsExtremaNormalizedNumberSQL(numberSQL string) string {
	return "if(assumeNotNull(" + numberSQL +
		") = 0, toFloat64(0), assumeNotNull(" + numberSQL + "))"
}

const (
	statsExtremaPublicationFloat uint8 = iota
	statsExtremaPublicationDecimal
	statsExtremaPublicationLexical
	statsExtremaPublicationEncodedBytes
)

func statsExtremaOrderingKeySQL(
	valueSQL string,
	exactKeySQL string,
	typeTieBreakSQL string,
) string {
	exact := exactNumericKeyValueSQL(exactKeySQL)
	numeric := exactNumericKeyEligibleSQL(exactKeySQL)
	return "tuple(toUInt8(NOT (" + numeric + ")), " +
		"if(" + numeric + ", tupleElement(" + exact + ", 1), toUInt8(1)), " +
		"if(" + numeric + ", tupleElement(" + exact + ", 2), toInt64(0)), " +
		"if(" + numeric + ", tupleElement(" + exact + ", 3), CAST('' AS String)), " +
		"if(" + numeric + ", CAST('' AS String), " + valueSQL + "), toUInt8(" +
		typeTieBreakSQL + "))"
}

func statsExtremaExactFloatPublicationSQL(numberSQL, exactKeySQL string) string {
	roundTrip := exactNumericOrderingKeySQL(
		"toString(" + statsExtremaNormalizedNumberSQL(numberSQL) + ")",
	)
	return statsExtremaExactFloatKeyMatchSQL(
		numberSQL,
		exactKeySQL,
		roundTrip,
	)
}

func statsExtremaExactFloatKeyMatchSQL(
	numberSQL, exactKeySQL, floatKeySQL string,
) string {
	return "isNotNull(" + numberSQL + ") AND " +
		exactNumericKeyEligibleSQL(exactKeySQL) + " AND " +
		exactKeySQL + " = " + floatKeySQL
}

func statsExtremaScalarCandidateSQL(
	valueSQL string,
	numberSQL string,
	rawBytesSQL string,
) string {
	value := "ifNull(" + valueSQL + ", CAST('' AS String))"
	if rawBytesSQL == "" {
		rawBytesSQL = "0"
	}
	rawBytes := "__os_stats_extrema_scalar_raw_bytes"
	candidate := statsExtremaPublicationCandidateSQL(
		statsExtremaPublicationCandidateInput{
			publicationValueSQL: "if(" + rawBytes + ", " +
				rawStdBase64EncodeSQL(value) + ", " + value + ")",
			orderingValueSQL: value,
			numberSQL: "if(" + rawBytes + ", CAST(NULL AS Nullable(Float64)), " +
				numberSQL + ")",
			exactTextSQL: "if(" + rawBytes + ", CAST('' AS String), " +
				boundedExactNumericOrderingInputSQL(value) + ")",
			lexicalPublicationKindSQL: "if(" + rawBytes + ", toUInt8(" +
				strconv.Itoa(int(statsExtremaPublicationEncodedBytes)) + "), toUInt8(" +
				strconv.Itoa(int(statsExtremaPublicationLexical)) + "))",
			eligibleSQL: "isNotNull(" + valueSQL + ")",
		},
	)
	return bindSQLExpressions(
		[]string{rawBytes},
		[]string{"toUInt8(ifNull(" + rawBytesSQL + ", 0)) != 0"},
		candidate,
	)
}

// statsExtremaPublicationCandidateSQL lowers one already-classified scalar to
// the fixed candidate tuple shared by scalar String extrema and row-local
// Dynamic eventstats folds:
//
//	(exact ordering key, publication kind, Float64 publication,
//	 publication text, eligible bit)
//
// The publication tuple deliberately contains no Dynamic value. This lets
// argMinOrNullIf represent an empty aggregate explicitly without attempting to
// construct Nullable(Dynamic), which ClickHouse does not support.
type statsExtremaPublicationCandidateInput struct {
	publicationValueSQL       string
	orderingValueSQL          string
	numberSQL                 string
	exactTextSQL              string
	lexicalPublicationKindSQL string
	eligibleSQL               string
}

func statsExtremaPublicationCandidateSQL(
	input statsExtremaPublicationCandidateInput,
) string {
	valueVariable := "__os_stats_extrema_value"
	orderingValueVariable := "__os_stats_extrema_ordering_value"
	numberVariable := "__os_stats_extrema_number"
	exactTextVariable := "__os_stats_extrema_exact_text"
	exactVariable := "__os_stats_extrema_exact_key"
	floatVariable := "__os_stats_extrema_float_key"
	exactFloatVariable := "__os_stats_extrema_exact_float"
	exactFloat := statsExtremaExactFloatKeyMatchSQL(
		numberVariable,
		exactVariable,
		floatVariable,
	)
	numeric := exactNumericKeyEligibleSQL(exactVariable)
	decimalInput := "if(" + numeric + ", " + valueVariable + ", CAST('0' AS String))"
	publicationKind := "toUInt8(multiIf(NOT (" + numeric + "), " +
		input.lexicalPublicationKindSQL + ", " +
		exactFloatVariable + ", " +
		strconv.Itoa(int(statsExtremaPublicationFloat)) + ", " +
		strconv.Itoa(int(statsExtremaPublicationDecimal)) + "))"
	ordering := statsExtremaOrderingKeySQL(
		orderingValueVariable,
		exactVariable,
		"if("+numeric+", toUInt8(0), "+input.lexicalPublicationKindSQL+")",
	)
	publicationNumber := "if(isNotNull(" + numberVariable + "), " +
		statsExtremaNormalizedNumberSQL(numberVariable) + ", toFloat64(0))"
	publicationText := "multiIf(NOT (" + numeric + "), " + valueVariable + ", " +
		exactFloatVariable + ", CAST('' AS String), " +
		decimalInput + ")"
	candidate := "tuple(" + ordering + ", " + publicationKind + ", " +
		publicationNumber + ", " + publicationText + ", toUInt8(" +
		input.eligibleSQL + "))"
	candidate = bindSQLExpressions(
		[]string{exactFloatVariable},
		[]string{exactFloat},
		candidate,
	)
	candidate = bindSQLExpressions(
		[]string{exactVariable, floatVariable},
		[]string{
			exactNumericOrderingKeySQL(exactTextVariable),
			trustedFiniteFloatOrderingKeySQL(
				"ifNull(" + numberVariable + ", toFloat64(0))",
			),
		},
		candidate,
	)
	return bindSQLExpressions(
		[]string{valueVariable, orderingValueVariable, numberVariable, exactTextVariable},
		[]string{
			input.publicationValueSQL,
			input.orderingValueSQL,
			input.numberSQL,
			input.exactTextSQL,
		},
		candidate,
	)
}

func statsExtremaScalarAggregateWinnerSQL(
	function plan.AggregateFunction,
	candidateSQL string,
) string {
	name := "argMinOrNullIf"
	if function == plan.AggregateFunctionMaximum {
		name = "argMaxOrNullIf"
	}
	key := "tupleElement(" + candidateSQL + ", 1)"
	eligible := "tupleElement(" + candidateSQL + ", 5) != 0"
	publication := "tuple(tupleElement(" + candidateSQL + ", 2), tupleElement(" +
		candidateSQL + ", 3), tupleElement(" + candidateSQL + ", 4))"
	return name + "(" + publication + ", " + key + ", " + eligible + ")"
}

func statsExtremaScalarValueSQL(extremeWinnerSQL string) string {
	nonNull := "assumeNotNull(" + extremeWinnerSQL + ")"
	kind := "tupleElement(" + nonNull + ", 1)"
	number := "tupleElement(" + nonNull + ", 2)"
	text := "tupleElement(" + nonNull + ", 3)"
	return "if(isNull(" + extremeWinnerSQL + "), CAST(NULL AS Dynamic), multiIf(" +
		kind + " = " + strconv.Itoa(int(statsExtremaPublicationFloat)) +
		", CAST(" + number + " AS Dynamic), " +
		kind + " = " + strconv.Itoa(int(statsExtremaPublicationDecimal)) +
		", " + decimalEnvelopeDynamicSQL(text) + ", " +
		kind + " = " + strconv.Itoa(int(statsExtremaPublicationEncodedBytes)) +
		", " + bytesEnvelopePayloadDynamicSQL(text) + ", " +
		"CAST(" + text + " AS Dynamic)))"
}

func statsExtremaScalarStoredTypeSQL(extremeWinnerSQL string) string {
	nonNull := "assumeNotNull(" + extremeWinnerSQL + ")"
	kind := "tupleElement(" + nonNull + ", 1)"
	lexical := "tupleElement(" + nonNull + ", 3)"
	ordinary := statsExtremaStoredTypeWithDecimalSQL(
		"isNull("+extremeWinnerSQL+")",
		kind+" = "+strconv.Itoa(int(statsExtremaPublicationFloat)),
		kind+" = "+strconv.Itoa(int(statsExtremaPublicationDecimal)),
		kind+" = "+strconv.Itoa(int(statsExtremaPublicationLexical)),
		lexical,
	)
	return "if(" + kind + " = " +
		strconv.Itoa(int(statsExtremaPublicationEncodedBytes)) +
		", toUInt8(" + strconv.Itoa(int(eventfields.StoredValueTypeBytes)) +
		"), " + ordinary + ")"
}

func statsExtremaCandidatesSQL(valuesSQL string) string {
	candidate := statsExtremaCandidateSQL(
		"value",
		"value",
		boundedExactNumericOrderingInputSQL("value"),
		"toUInt8("+strconv.Itoa(int(statsExtremaPublicationLexical))+")",
	)
	return "arrayMap(value -> " + candidate + ", " + valuesSQL + ")"
}

func statsExtremaCandidateSQL(
	publicationValueSQL string,
	orderingValueSQL string,
	exactTextSQL string,
	lexicalPublicationKindSQL string,
) string {
	publicationValue := "__os_stats_extrema_publication_value"
	orderingValue := "__os_stats_extrema_ordering_value"
	exactText := "__os_stats_extrema_exact_text"
	lexicalKind := "__os_stats_extrema_lexical_kind"
	number := "if(" + lexicalKind + " = toUInt8(" +
		strconv.Itoa(int(statsExtremaPublicationEncodedBytes)) + "), " +
		"CAST(NULL AS Nullable(Float64)), " +
		statsExtremaNumericOrNullSQL(publicationValue) + ")"
	exact := exactNumericOrderingKeySQL(exactText)
	exactFloat := statsExtremaExactFloatPublicationSQL("number", "exact_key")
	numeric := exactNumericKeyEligibleSQL("exact_key")
	decimalInput := "if(" + numeric + ", " + publicationValue +
		", CAST('0' AS String))"
	lexicalCandidate := "if(" + lexicalKind + " = toUInt8(" +
		strconv.Itoa(int(statsExtremaPublicationEncodedBytes)) + "), " +
		bytesEnvelopePayloadDynamicSQL(publicationValue) + ", CAST(" +
		publicationValue + " AS Dynamic))"
	candidate := "multiIf(NOT (" + numeric + "), " + lexicalCandidate + ", " +
		exactFloat + ", CAST(" + statsExtremaNormalizedNumberSQL("number") +
		" AS Dynamic), " + decimalEnvelopeDynamicSQL(decimalInput) + ")"
	key := statsExtremaOrderingKeySQL(
		orderingValue,
		"exact_key",
		"if("+numeric+", toUInt8(0), "+lexicalKind+")",
	)
	bound := bindSQLExpressions(
		[]string{"number", "exact_key"},
		[]string{number, exact},
		"tuple("+candidate+", "+key+")",
	)
	return bindSQLExpressions(
		[]string{publicationValue, orderingValue, exactText, lexicalKind},
		[]string{
			publicationValueSQL,
			orderingValueSQL,
			exactTextSQL,
			lexicalPublicationKindSQL,
		},
		bound,
	)
}

type compiledDynamicMeasureScalar struct {
	valueSQL     string
	typeSQL      string
	supportedSQL string
	lexicalSQL   string
	exactTextSQL string
	eligibleSQL  string
	invalidSQL   string
}

// compileDynamicMeasureScalar centralizes the scalar/member classification
// used by transforming stats and row-preserving eventstats. None is missing;
// supported scalar values are eligible unless an upstream text guard excludes
// them; unsupported non-None values poison the enclosing measure.
func compileDynamicMeasureScalar(field fieldState) compiledDynamicMeasureScalar {
	typeSQL := dynamicTypeExpression(field)
	supportedSQL, lexicalSQL := statsByScalarExpressions(field)
	eligibleSQL := "(" + typeSQL + " != 'None' AND " + supportedSQL + ")"
	if field.textEligibleSQL != "" {
		eligibleSQL = "(" + eligibleSQL + " AND ifNull(" +
			field.textEligibleSQL + ", 0))"
	}
	return compiledDynamicMeasureScalar{
		valueSQL:     field.valueSQL,
		typeSQL:      typeSQL,
		supportedSQL: supportedSQL,
		lexicalSQL:   lexicalSQL,
		exactTextSQL: exactNumericScalarTextSQL(compiledScalarFromField(field)),
		eligibleSQL:  eligibleSQL,
		invalidSQL: "(" + typeSQL + " != 'None' AND NOT (" +
			supportedSQL + "))",
	}
}

// dynamicExtremaNormalizedTupleSQL preserves the distinction between a
// bytes/v1 envelope's RawStd payload and a Dynamic String that already carries
// Bytes provenance. Both order on raw bytes and publish one canonical bytes/v1
// payload; ordinary lexical and numeric candidates retain their existing text.
//
// The tuple is:
//
//	(publication text, ordering text, exact-numeric text, lexical kind)
func dynamicExtremaNormalizedTupleSQL(
	field fieldState,
	scalar compiledDynamicMeasureScalar,
) string {
	lexical := "__os_stats_extrema_dynamic_lexical"
	exactText := "__os_stats_extrema_dynamic_exact_text"
	encodedBytes := "__os_stats_extrema_dynamic_encoded_bytes"
	rawBytes := "__os_stats_extrema_dynamic_raw_bytes"
	bytesValue := "__os_stats_extrema_dynamic_bytes"
	ordering := "__os_stats_extrema_dynamic_ordering"
	publication := "__os_stats_extrema_dynamic_publication"

	dynamic := compiledScalar{
		valueSQL:       field.valueSQL,
		dynamicTypeSQL: scalar.typeSQL,
		kind:           fieldKindDynamic,
	}
	encodedBytesSQL := dynamicTaggedEnvelopeCondition(dynamic, "bytes/v1")
	rawBytesSQL := "0"
	if field.storedTypeSQL != "" {
		rawBytesSQL = "(" + scalar.typeSQL + " = 'String' AND toUInt8(" +
			field.storedTypeSQL + ") = toUInt8(" +
			strconv.Itoa(int(eventfields.StoredValueTypeBytes)) + "))"
	}
	orderingSQL := "if(" + encodedBytes + ", " +
		rawStdBase64DecodeSQL(lexical) + ", " + lexical + ")"
	publicationSQL := "multiIf(" + encodedBytes + ", " + lexical + ", " +
		rawBytes + ", " + rawStdBase64EncodeSQL(ordering) + ", " + lexical + ")"
	lexicalKind := "if(" + bytesValue + ", toUInt8(" +
		strconv.Itoa(int(statsExtremaPublicationEncodedBytes)) + "), toUInt8(" +
		strconv.Itoa(int(statsExtremaPublicationLexical)) + "))"
	body := "tuple(" + publication + ", " + ordering + ", if(" + bytesValue +
		", CAST('' AS String), " + exactText + "), " + lexicalKind + ")"
	body = bindSQLExpressions(
		[]string{publication},
		[]string{publicationSQL},
		body,
	)
	body = bindSQLExpressions(
		[]string{bytesValue},
		[]string{"(" + encodedBytes + " OR " + rawBytes + ")"},
		body,
	)
	body = bindSQLExpressions(
		[]string{ordering},
		[]string{orderingSQL},
		body,
	)
	body = bindSQLExpressions(
		[]string{encodedBytes, rawBytes},
		[]string{encodedBytesSQL, rawBytesSQL},
		body,
	)
	return bindSQLExpressions(
		[]string{lexical, exactText},
		[]string{scalar.lexicalSQL, scalar.exactTextSQL},
		body,
	)
}

func statsExtremaDynamicCandidatesSQL(field fieldState) (string, []any) {
	empty := "CAST([], 'Array(Tuple(String, String, String, UInt8))')"
	scalar := compileDynamicMeasureScalar(field)
	scalarInput := "if(" + scalar.eligibleSQL + ", [" +
		dynamicExtremaNormalizedTupleSQL(field, scalar) + "], " + empty + ")"

	elementField := fieldState{
		valueSQL:       "element",
		dynamicTypeSQL: "dynamicType(element)",
		kind:           fieldKindDynamic,
	}
	element := compileDynamicMeasureScalar(elementField)
	elementInput := "if(throwIf(toUInt8(" + element.invalidSQL + "), '" +
		UnsupportedStatsMeasureValueMarker + "') = 0, " +
		dynamicExtremaNormalizedTupleSQL(elementField, element) +
		", tuple(CAST('' AS String), CAST('' AS String), CAST('' AS String), " +
		"toUInt8(" + strconv.Itoa(int(statsExtremaPublicationLexical)) + ")))"
	arrayValues := "arrayFilter(element -> " + element.typeSQL +
		" != 'None', dynamicElement(" + field.valueSQL + ", 'Array(Dynamic)'))"
	arrayInput := "arrayMap(element -> " + elementInput + ", " + arrayValues + ")"

	existsSQL, descendantSQL, args := dynamicPresenceOperands(field)
	topLevelUnsupported := "(field_present != 0 AND " + scalar.typeSQL +
		" != 'None' AND " + scalar.typeSQL + " != 'Array(Dynamic)' AND NOT (" +
		scalar.supportedSQL + "))"
	invalid := "(" + topLevelUnsupported + " OR descendant_present != 0)"
	value := "multiIf(" + scalar.typeSQL + " = 'None', " + empty + ", " +
		scalar.typeSQL + " = 'Array(Dynamic)', " + arrayInput + ", " +
		scalarInput + ")"
	body := "if(throwIf(toUInt8(" + invalid + "), '" +
		UnsupportedStatsMeasureValueMarker + "') = 0, if(field_present != 0, " +
		value + ", " + empty + "), " + empty + ")"
	inputs := bindSQLExpressions(
		[]string{"field_present", "descendant_present"},
		[]string{"toUInt8(" + existsSQL + ")", "toUInt8(" + descendantSQL + ")"},
		body,
	)
	input := "__os_stats_extrema_input"
	candidate := statsExtremaCandidateSQL(
		"tupleElement("+input+", 1)",
		"tupleElement("+input+", 2)",
		"tupleElement("+input+", 3)",
		"tupleElement("+input+", 4)",
	)
	return "arrayMap(" + input + " -> " + candidate + ", " + inputs + ")", args
}

func eventStatsExtremaEmptyOrderingKeySQL() string {
	return "tuple(toUInt8(1), toUInt8(1), toInt64(0), " +
		"CAST('' AS String), CAST('' AS String), toUInt8(" +
		strconv.Itoa(int(statsExtremaPublicationLexical)) + "))"
}

func eventStatsExtremaEmptyRowStateSQL(invalidSQL string) string {
	return "tuple(" + eventStatsExtremaEmptyOrderingKeySQL() + ", toUInt8(" +
		strconv.Itoa(int(statsExtremaPublicationLexical)) + "), toFloat64(0), " +
		"CAST('' AS String), toUInt8(0), toUInt8(" + invalidSQL + "))"
}

// extremaFoldWinnerStateSQL merges one normalized five-element candidate into
// the shared six-element row state. Dynamic callers supply the candidate's
// unsupported-value bit; fixed String arrays leave it empty.
func extremaFoldWinnerStateSQL(
	function plan.AggregateFunction,
	stateSQL string,
	candidateSQL string,
	invalidSQL string,
) string {
	// Keep the established private alias stable because compiler-shape tests and
	// query diagnostics use it to identify the normalized extrema candidate.
	candidate := "__os_eventstats_extrema_candidate"
	comparison := "<"
	if function == plan.AggregateFunctionMaximum {
		comparison = ">"
	}
	replace := "(tupleElement(" + candidate + ", 5) != 0 AND (tupleElement(" +
		stateSQL + ", 5) = 0 OR tupleElement(" + candidate + ", 1) " + comparison +
		" tupleElement(" + stateSQL + ", 1)))"
	fields := make([]string, 0, 6)
	for index := 1; index <= 4; index++ {
		position := strconv.Itoa(index)
		fields = append(fields, "if("+replace+", tupleElement("+candidate+", "+
			position+"), tupleElement("+stateSQL+", "+position+"))")
	}
	fields = append(
		fields,
		"toUInt8(tupleElement("+stateSQL+", 5) != 0 OR tupleElement("+
			candidate+", 5) != 0)",
	)
	invalidState := "tupleElement(" + stateSQL + ", 6)"
	if invalidSQL != "" {
		invalidState = "toUInt8(" + invalidState + " != 0 OR (" + invalidSQL + "))"
	}
	fields = append(fields, invalidState)
	return bindSQLExpressions(
		[]string{candidate},
		[]string{candidateSQL},
		"tuple("+strings.Join(fields, ", ")+")",
	)
}

func eventStatsExtremaFoldStepSQL(
	function plan.AggregateFunction,
	stateSQL string,
	value fieldState,
	eligibilityGuardSQL string,
) string {
	typeVariable := "__os_eventstats_extrema_type"
	supportedVariable := "__os_eventstats_extrema_supported"
	supportedSQL, lexicalSQL := statsByScalarExpressionsFor(
		value.valueSQL,
		typeVariable,
	)
	exactTextSQL := exactNumericScalarTextSQL(compiledScalar{
		valueSQL:       value.valueSQL,
		dynamicTypeSQL: typeVariable,
		kind:           fieldKindDynamic,
	})
	eligibleSQL := "(" + typeVariable + " != 'None' AND " +
		supportedVariable + ")"
	if eligibilityGuardSQL != "" {
		eligibleSQL = "(" + eligibleSQL + " AND ifNull(" +
			eligibilityGuardSQL + ", 0))"
	}
	invalidSQL := "(" + typeVariable + " != 'None' AND NOT (" +
		supportedVariable + "))"
	scalar := compiledDynamicMeasureScalar{
		valueSQL:     value.valueSQL,
		typeSQL:      typeVariable,
		supportedSQL: supportedVariable,
		lexicalSQL:   lexicalSQL,
		exactTextSQL: exactTextSQL,
		eligibleSQL:  eligibleSQL,
		invalidSQL:   invalidSQL,
	}
	normalizedVariable := "__os_eventstats_extrema_normalized"
	publicationValue := "tupleElement(" + normalizedVariable + ", 1)"
	orderingValue := "tupleElement(" + normalizedVariable + ", 2)"
	exactText := "tupleElement(" + normalizedVariable + ", 3)"
	lexicalKind := "tupleElement(" + normalizedVariable + ", 4)"
	numberSQL := "if(" + lexicalKind + " = toUInt8(" +
		strconv.Itoa(int(statsExtremaPublicationEncodedBytes)) + "), " +
		"CAST(NULL AS Nullable(Float64)), " +
		statsExtremaNumericOrNullSQL(publicationValue) + ")"
	candidateSQL := statsExtremaPublicationCandidateSQL(
		statsExtremaPublicationCandidateInput{
			publicationValueSQL:       publicationValue,
			orderingValueSQL:          orderingValue,
			numberSQL:                 numberSQL,
			exactTextSQL:              exactText,
			lexicalPublicationKindSQL: lexicalKind,
			eligibleSQL:               "1",
		},
	)
	candidateSQL = bindSQLExpressions(
		[]string{normalizedVariable},
		[]string{dynamicExtremaNormalizedTupleSQL(value, scalar)},
		candidateSQL,
	)
	emptyCandidate := "tuple(" + eventStatsExtremaEmptyOrderingKeySQL() +
		", toUInt8(" + strconv.Itoa(int(statsExtremaPublicationLexical)) +
		"), toFloat64(0), CAST('' AS String), toUInt8(0))"
	result := extremaFoldWinnerStateSQL(
		function,
		stateSQL,
		"if("+eligibleSQL+", "+candidateSQL+", "+emptyCandidate+")",
		invalidSQL,
	)
	result = bindSQLExpressions(
		[]string{supportedVariable},
		[]string{supportedSQL},
		result,
	)
	return bindSQLExpressions(
		[]string{typeVariable},
		[]string{dynamicTypeExpression(value)},
		result,
	)
}

// eventStatsExtremaDynamicMeasureSQL folds one Dynamic row to a constant-size
// winner tuple plus an invalid-container bit. Dynamic multivalue members are
// visited once; no candidate array or second validation walk is retained. A
// grouped query gates the entire fold with its already-bound BY eligibility,
// so an incomplete group row cannot spend work or contribute poison.
func eventStatsExtremaDynamicMeasureSQL(
	function plan.AggregateFunction,
	field fieldState,
	rowEligibleSQL string,
) (string, []any) {
	if rowEligibleSQL == "" {
		rowEligibleSQL = "1"
	}
	topLevelType := "__os_eventstats_extrema_top_level_type"
	elementStoredTypeSQL := ""
	if field.storedTypeSQL != "" {
		elementStoredTypeSQL = "if(" + topLevelType +
			" = 'Array(Dynamic)', toUInt8(0), toUInt8(" +
			field.storedTypeSQL + "))"
	}
	element := fieldState{
		valueSQL:       "element",
		dynamicTypeSQL: "dynamicType(element)",
		storedTypeSQL:  elementStoredTypeSQL,
		kind:           fieldKindDynamic,
	}
	fieldValue := "__os_eventstats_extrema_field_value"
	eligibilityGuardSQL := ""
	if field.textEligibleSQL != "" {
		// A scalar text guard applies to the singleton top-level value. Array
		// members retain their existing independent eligibility contract.
		eligibilityGuardSQL = "(" + topLevelType +
			" = 'Array(Dynamic)' OR ifNull(" + field.textEligibleSQL + ", 0))"
	}

	existsSQL, descendantSQL, args := dynamicPresenceOperands(field)
	empty := eventStatsExtremaEmptyRowStateSQL("0")
	initial := eventStatsExtremaEmptyRowStateSQL("descendant_present != 0")
	memberState := "__os_eventstats_extrema_state"
	values := "multiIf(field_present = 0 OR " + topLevelType +
		" = 'None', arraySlice([" + fieldValue + "], 1, 0), " + topLevelType +
		" = 'Array(Dynamic)', dynamicElement(" + fieldValue +
		", 'Array(Dynamic)'), [" + fieldValue + "])"
	rowState := "arrayFold((" + memberState + ", element) -> " +
		eventStatsExtremaFoldStepSQL(
			function,
			memberState,
			element,
			eligibilityGuardSQL,
		) + ", " + values +
		", " + initial + ")"
	gated := "if(" + rowEligibleSQL + ", " + rowState + ", " + empty + ")"
	return bindSQLExpressions(
		[]string{
			"field_present",
			"descendant_present",
			topLevelType,
			fieldValue,
		},
		[]string{
			"toUInt8(" + existsSQL + ")",
			"toUInt8(" + descendantSQL + ")",
			dynamicTypeExpression(field),
			field.valueSQL,
		},
		gated,
	), args
}

func statsExtremaNumericOrNullSQL(valueSQL string) string {
	limit := strconv.Itoa(MaximumExactNumericBinTextBytes)
	bounded := "if(length(" + valueSQL + ") <= " + limit + ", " + valueSQL +
		", CAST('' AS String))"
	numeric := "isValidUTF8(" + valueSQL + ") AND length(" + valueSQL + ") <= " +
		limit + " AND " + valueSQL + " = trimBoth(" + valueSQL + ") AND match(" +
		bounded + ", " + decimalNumericStringPattern + ")"
	converted := finiteFloatOrNullSQL(canonicalNumericTextSQL(bounded))
	return "if(" + numeric + ", " + converted + ", CAST(NULL AS Nullable(Float64)))"
}

func statsExtremaAggregateSQL(function plan.AggregateFunction, candidatesSQL string) string {
	name := "argMinArray"
	if function == plan.AggregateFunctionMaximum {
		name = "argMaxArray"
	}
	return name + "(arrayMap(candidate -> tupleElement(candidate, 1), " + candidatesSQL +
		"), arrayMap(candidate -> tupleElement(candidate, 2), " + candidatesSQL + "))"
}

func statsExtremaStoredTypeSQL(valueSQL string) string {
	valueVariable := "__os_stats_extrema_stored_value"
	typeSQL := "dynamicType(" + valueVariable + ")"
	stringSQL := "dynamicElement(" + valueVariable + ", 'String')"
	value := compiledScalar{
		valueSQL:       valueVariable,
		dynamicTypeSQL: typeSQL,
		kind:           fieldKindDynamic,
	}
	decimal := dynamicTaggedEnvelopeCondition(value, "decimal/v1")
	bytesValue := dynamicTaggedEnvelopeCondition(value, "bytes/v1")
	body := "multiIf(" +
		typeSQL + " = 'None', toUInt8(" +
		strconv.Itoa(int(eventfields.StoredValueTypeNull)) + "), " +
		typeSQL + " = 'Float64', toUInt8(" +
		strconv.Itoa(int(eventfields.StoredValueTypeDouble)) + "), " +
		decimal + ", toUInt8(" +
		strconv.Itoa(int(eventfields.StoredValueTypeDecimal)) + "), " +
		bytesValue + ", toUInt8(" +
		strconv.Itoa(int(eventfields.StoredValueTypeBytes)) + "), " +
		typeSQL + " = 'String' AND isValidUTF8(" + stringSQL + "), toUInt8(" +
		strconv.Itoa(int(eventfields.StoredValueTypeString)) + "), " +
		typeSQL + " = 'String', toUInt8(" +
		strconv.Itoa(int(eventfields.StoredValueTypeBytes)) + "), toUInt8(0))"
	return bindSQLExpressions(
		[]string{valueVariable},
		[]string{valueSQL},
		body,
	)
}

func statsExtremaStoredTypeFromConditionsSQL(
	nullConditionSQL string,
	numberConditionSQL string,
	stringConditionSQL string,
	stringSQL string,
) string {
	return statsExtremaStoredTypeWithDecimalSQL(
		nullConditionSQL,
		numberConditionSQL,
		"0",
		stringConditionSQL,
		stringSQL,
	)
}

func statsExtremaStoredTypeWithDecimalSQL(
	nullConditionSQL string,
	numberConditionSQL string,
	decimalConditionSQL string,
	stringConditionSQL string,
	stringSQL string,
) string {
	return "multiIf(" +
		nullConditionSQL + ", toUInt8(" + strconv.Itoa(int(eventfields.StoredValueTypeNull)) + "), " +
		numberConditionSQL + ", toUInt8(" + strconv.Itoa(int(eventfields.StoredValueTypeDouble)) + "), " +
		decimalConditionSQL + ", toUInt8(" + strconv.Itoa(int(eventfields.StoredValueTypeDecimal)) + "), " +
		stringConditionSQL + " AND isValidUTF8(" + stringSQL + "), toUInt8(" +
		strconv.Itoa(int(eventfields.StoredValueTypeString)) + "), " +
		stringConditionSQL + ", toUInt8(" + strconv.Itoa(int(eventfields.StoredValueTypeBytes)) + "), " +
		"toUInt8(0))"
}

func compileStatsSparklineResults(
	relation compiledRelation,
	measures []compiledStatsSparklineMeasure,
	ownerRange spl.Range,
	stage int,
) (compiledRelation, int, error) {
	if len(measures) == 0 {
		return relation, 0, nil
	}

	excluded := make([]string, 0, len(measures))
	projection := make([]string, 1, 1+len(measures))
	for _, measure := range measures {
		if measure.recordsColumn == "" || measure.outputColumn == "" {
			return compiledRelation{}, 0, errors.New(
				"compile ClickHouse stats sparkline: publication metadata is invalid",
			)
		}
		published, ok := statsSparklinePublishSQL(
			measure.recordsColumn,
			measure.spec,
			measure.missing,
		)
		if !ok {
			return compiledRelation{}, 0, errors.New(
				"compile ClickHouse stats sparkline: publication lowering is invalid",
			)
		}
		excluded = append(excluded, measure.recordsColumn)
		projection = append(projection, published+" AS "+measure.outputColumn)
	}
	projection[0] = "* EXCEPT (" + strings.Join(excluded, ", ") + ")"
	alias := quoteIdentifier(fmt.Sprintf("_stage_%d", stage+1))
	sql := "SELECT " + strings.Join(projection, ", ") + " FROM (" +
		relation.sql + ") AS " + alias
	return relation.selectFrom(sql, ownerRange), 1, nil
}

func compileChronologicalResults(
	relation compiledRelation,
	measures []compiledChronologicalMeasure,
	ownerRange spl.Range,
	stage int,
) (compiledRelation, int, *pendingChronologicalBarrier) {
	if len(measures) == 0 {
		return relation, 0, nil
	}

	excluded := make([]string, 0, len(measures)*2)
	seenExcluded := make(map[string]struct{}, len(measures)*2)
	validations := make([]string, 0, len(measures))
	seenValidations := make(map[string]struct{}, len(measures))
	for _, measure := range measures {
		if _, seen := seenExcluded[measure.winnerColumn]; !seen {
			seenExcluded[measure.winnerColumn] = struct{}{}
			excluded = append(excluded, measure.winnerColumn)
		}
		if measure.validationColumn == "" {
			continue
		}
		if _, seen := seenExcluded[measure.validationColumn]; !seen {
			seenExcluded[measure.validationColumn] = struct{}{}
			excluded = append(excluded, measure.validationColumn)
		}
		if _, seen := seenValidations[measure.validationColumn]; !seen {
			seenValidations[measure.validationColumn] = struct{}{}
			validations = append(validations, measure.validationColumn)
		}
	}

	projection := []string{"* EXCEPT (" + strings.Join(excluded, ", ") + ")"}
	publishedTypes := make(map[string]struct{}, len(measures))
	for _, measure := range measures {
		projection = append(
			projection,
			chronologicalPublishedValueSQL(measure.winnerColumn)+
				" AS "+measure.outputColumn,
		)
		if _, published := publishedTypes[measure.winnerColumn]; published {
			continue
		}
		publishedTypes[measure.winnerColumn] = struct{}{}
		projection = append(
			projection,
			chronologicalPublishedTypeSQL(measure.winnerColumn)+
				" AS "+measure.typeColumn,
		)
	}

	alias := quoteIdentifier(fmt.Sprintf("_stage_%d", stage+1))
	if len(validations) == 0 {
		sql := "SELECT " + strings.Join(projection, ", ") +
			" FROM (" + relation.sql + ") AS " + alias
		return relation.selectFrom(sql, ownerRange), 1, nil
	}

	materialized := quoteIdentifier(fmt.Sprintf("__os_chronological_input_%d", stage+1))
	sql := "SELECT " + strings.Join(projection, ", ") +
		" FROM " + materialized + " AS " + alias
	published := relation.selectFrom(sql, ownerRange)
	return published, 1, &pendingChronologicalBarrier{
		name:              materialized,
		sql:               relation.sql,
		validationColumns: validations,
		fanout:            1,
		depth:             relation.depth,
		ownerRange:        ownerRange,
	}
}

func compileScalarExtremaResults(
	relation compiledRelation,
	measures []compiledScalarExtremaMeasure,
	ownerRange spl.Range,
	stage int,
) (compiledRelation, int) {
	if len(measures) == 0 {
		return relation, 0
	}

	winners := make([]string, 0, len(measures))
	seenWinners := make(map[string]struct{}, len(measures))
	projection := make([]string, 1, 1+len(measures)*2)
	for _, measure := range measures {
		if _, seen := seenWinners[measure.winnerColumn]; !seen {
			seenWinners[measure.winnerColumn] = struct{}{}
			winners = append(winners, measure.winnerColumn)
		}
	}
	projection[0] = "* EXCEPT (" + strings.Join(winners, ", ") + ")"

	publishedTypes := make(map[string]struct{}, len(winners))
	for _, measure := range measures {
		projection = append(
			projection,
			statsExtremaScalarValueSQL(measure.winnerColumn)+" AS "+measure.outputColumn,
		)
		if _, published := publishedTypes[measure.winnerColumn]; !published {
			publishedTypes[measure.winnerColumn] = struct{}{}
			projection = append(
				projection,
				statsExtremaScalarStoredTypeSQL(measure.winnerColumn)+" AS "+measure.typeColumn,
			)
		}
	}

	alias := quoteIdentifier(fmt.Sprintf("_stage_%d", stage+1))
	sql := "SELECT " + strings.Join(projection, ", ") +
		" FROM (" + relation.sql + ") AS " + alias
	return relation.selectFrom(sql, ownerRange), 1
}

func compactNullableArraySQL(valuesSQL string) string {
	return "arrayMap(value -> assumeNotNull(value), arrayFilter(value -> isNotNull(value), " + valuesSQL + "))"
}

func statsScalarStringOrNullSQL(field fieldState) string {
	nullString := "CAST(NULL AS Nullable(String))"
	switch field.kind {
	case fieldKindDynamic:
		supported, lexical := statsByScalarExpressions(field)
		return "if(" + supported + ", " + lexical + ", " + nullString + ")"
	case fieldKindString, fieldKindNumber, fieldKindBool, fieldKindTime:
		return "CAST(toString(" + field.valueSQL + ") AS Nullable(String))"
	default:
		return nullString
	}
}

func statsTextEligibleScalarStringOrNullSQL(field fieldState) string {
	value := statsScalarStringOrNullSQL(field)
	if field.textEligibleSQL == "" {
		return value
	}
	nullString := "CAST(NULL AS Nullable(String))"
	value = "if(ifNull(" + field.textEligibleSQL + ", 0), " +
		value + ", " + nullString + ")"
	return value
}

func boundedDistinctCountSQL(inputSQL string) string {
	maximum := strconv.FormatUint(MaximumStatsDistinctValuesPerGroup, 10)
	cardinality := distinctCountCardinalitySQL(inputSQL)
	return "arrayElement(arrayMap(cardinality -> cardinality + toUInt64(throwIf(toUInt8(cardinality > " +
		maximum + "), '" + ExactDistinctLimitMarker + "')), [" + cardinality + "]), 1)"
}

func distinctCountCardinalitySQL(inputSQL string) string {
	return "toUInt64(length(" + exactDistinctStringSetSQL(
		inputSQL,
		uint64(MaximumStatsDistinctValuesPerGroup),
	) + "))"
}

func exactDistinctStringSetSQL(inputSQL string, maximum uint64) string {
	sentinel := strconv.FormatUint(maximum+1, 10)
	return "groupUniqArrayArray(" + sentinel + ")(" + inputSQL + ")"
}

func stringArrayPayloadBytesSQL(valuesSQL string) string {
	return "arrayFold((bytes, value) -> bytes + toUInt128(length(value)), " +
		valuesSQL + ", toUInt128(0))"
}

func orderedStringMembersSQL(valuesSQL string) string {
	return "arrayMap((value, element_index, cumulative_bytes) -> " +
		"tuple(value, toUInt128(element_index), cumulative_bytes), " +
		valuesSQL + ", arrayEnumerate(" + valuesSQL + "), " +
		"arrayCumSum(arrayMap(value -> toUInt128(length(value)), " + valuesSQL + ")))"
}

func boundedOrderedStringRowStateSQL(
	rowOrdinalSQL string,
	valuesSQL string,
	priorElementsSQL string,
	priorBytesSQL string,
) string {
	maximumValues := strconv.FormatUint(MaximumStatsListValuesPerGroup, 10)
	maximumBytes := strconv.FormatUint(MaximumStatsListBytesPerGroup, 10)
	remainingValues := "if(" + priorElementsSQL + " < toUInt128(" + maximumValues +
		"), arraySlice(" + valuesSQL + ", 1, toUInt64(toUInt128(" + maximumValues +
		") - " + priorElementsSQL + ")), CAST([], 'Array(String)'))"
	remaining := "__os_list_remaining_values"
	members := orderedStringMembersSQL(remaining)
	member := "member"
	bytes := priorBytesSQL + " + tupleElement(" + member + ", 3)"
	candidates := "arrayMap(" + member + " -> tuple(" + rowOrdinalSQL +
		", toUInt64(tupleElement(" + member + ", 2)), tupleElement(" + member +
		", 1)), arrayFilter(" + member + " -> " + bytes + " <= toUInt128(" +
		maximumBytes + "), " + members + "))"
	overflow := "toUInt8(" + priorElementsSQL + " < toUInt128(" + maximumValues +
		") AND " + priorBytesSQL + " + " + stringArrayPayloadBytesSQL(remaining) +
		" > toUInt128(" + maximumBytes + "))"
	return "arrayElement(arrayMap(" + remaining + " -> tuple(" + candidates +
		", " + overflow + "), [" + remainingValues + "]), 1)"
}

func boundedOrderedStringListSQL(candidatesSQL string) string {
	return "groupArraySortedArray(" +
		strconv.FormatUint(MaximumStatsListValuesPerGroup, 10) +
		")(" + candidatesSQL + ")"
}

func emptyOrderedStringListSQL() string {
	return "CAST([], 'Array(Tuple(UInt64, UInt64, String))')"
}

func orderedStringListValuesSQL(listSQL string) string {
	return "arrayMap(item -> tupleElement(item, 3), " + listSQL + ")"
}

func orderedStringListPayloadBytesSQL(listSQL string) string {
	return "arrayFold((bytes, item) -> bytes + toUInt128(length(tupleElement(item, 3))), " +
		listSQL + ", toUInt128(0))"
}

func compileBoundedOrderedStringResults(
	relation compiledRelation,
	measures []compiledOrderedStringMeasure,
	existingValues []string,
	ownerRange spl.Range,
	stage int,
) (compiledRelation, int) {
	if len(measures) == 0 {
		return relation, 0
	}

	listColumns := make([]string, 0, len(measures))
	overflowColumns := make([]string, 0, len(measures))
	materialized := make(map[string]string, len(measures))
	seenLists := make(map[string]struct{}, len(measures))
	for _, measure := range measures {
		if _, seen := seenLists[measure.listColumn]; seen {
			continue
		}
		seenLists[measure.listColumn] = struct{}{}
		listColumns = append(listColumns, measure.listColumn)
		overflowColumns = append(overflowColumns, measure.overflowColumn)
		materialized[measure.listColumn] = quoteIdentifier(fmt.Sprintf(
			"__os_list_strings_%d",
			len(materialized),
		))
	}

	windowColumns := make([]string, 0, len(listColumns)+4)
	byteConditions := make([]string, 0, len(listColumns))
	for index, listColumn := range listColumns {
		windowColumns = append(
			windowColumns,
			orderedStringListValuesSQL(listColumn)+" AS "+
				materialized[listColumn],
		)
		byteConditions = append(
			byteConditions,
			overflowColumns[index]+" != 0",
			orderedStringListPayloadBytesSQL(listColumn)+" > toUInt128("+
				strconv.FormatUint(MaximumStatsListBytesPerGroup, 10)+")",
		)
	}

	var rowElementTotal strings.Builder
	rowElementTotal.WriteString("toUInt128(0)")
	var rowByteTotal strings.Builder
	rowByteTotal.WriteString("toUInt128(0)")
	for _, measure := range measures {
		// Public aliases count independently even when their physical ordered
		// aggregate state is shared.
		rowElementTotal.WriteString(" + toUInt128(length(")
		rowElementTotal.WriteString(measure.listColumn)
		rowElementTotal.WriteString("))")
		rowByteTotal.WriteString(" + ")
		rowByteTotal.WriteString(orderedStringListPayloadBytesSQL(measure.listColumn))
	}
	for _, valuesColumn := range existingValues {
		// values() has already passed its own exact-state barrier. Include each
		// public values alias again so list() cannot bypass the combined
		// transforming-row and transport budgets.
		rowElementTotal.WriteString(" + toUInt128(length(")
		rowElementTotal.WriteString(valuesColumn)
		rowElementTotal.WriteString("))")
		rowByteTotal.WriteString(" + ")
		rowByteTotal.WriteString(stringArrayPayloadBytesSQL(valuesColumn))
	}

	elementOverflow := quoteIdentifier("__os_stats_list_any_overflow")
	totalElements := quoteIdentifier("__os_stats_list_total_elements")
	bytesOverflow := quoteIdentifier("__os_stats_list_bytes_any_overflow")
	totalBytes := quoteIdentifier("__os_stats_list_total_bytes")
	windowColumns = append(
		windowColumns,
		"max(toUInt8("+rowElementTotal.String()+" > toUInt128("+
			strconv.FormatUint(MaximumStatsValuesPerGroup, 10)+
			"))) OVER () AS "+elementOverflow,
		"sum("+rowElementTotal.String()+") OVER () AS "+totalElements,
		"max(toUInt8(("+strings.Join(byteConditions, " OR ")+") OR "+
			rowByteTotal.String()+" > toUInt128("+
			strconv.FormatUint(MaximumStatsValuesBytesPerGroup, 10)+
			"))) OVER () AS "+bytesOverflow,
		"sum("+rowByteTotal.String()+") OVER () AS "+totalBytes,
	)

	windowAlias := quoteIdentifier(fmt.Sprintf("_stage_%d", stage+1))
	windowSQL := "SELECT *, " + strings.Join(windowColumns, ", ") +
		" FROM (" + relation.sql + ") AS " + windowAlias
	relation = relation.selectFrom(windowSQL, ownerRange)

	excluded := append([]string(nil), listColumns...)
	excluded = append(excluded, overflowColumns...)
	for _, listColumn := range listColumns {
		excluded = append(excluded, materialized[listColumn])
	}
	excluded = append(
		excluded,
		elementOverflow,
		totalElements,
		bytesOverflow,
		totalBytes,
	)
	projection := []string{"* EXCEPT (" + strings.Join(excluded, ", ") + ")"}
	for _, measure := range measures {
		projection = append(
			projection,
			materialized[measure.listColumn]+" AS "+measure.outputColumn,
		)
	}

	publishAlias := quoteIdentifier(fmt.Sprintf("_stage_%d", stage+2))
	publishSQL := "SELECT " + strings.Join(projection, ", ") + " FROM (" + relation.sql +
		") AS " + publishAlias +
		" WHERE throwIf(toUInt8(" + elementOverflow +
		" != 0 OR " + totalElements + " > toUInt128(" +
		strconv.FormatUint(MaximumStatsListValuesPerResult, 10) +
		")), '" + StatsListLimitMarker + "') = 0" +
		" AND throwIf(toUInt8(" + bytesOverflow +
		" != 0 OR " + totalBytes + " > toUInt128(" +
		strconv.FormatUint(MaximumStatsListBytesPerResult, 10) +
		")), '" + StatsListBytesLimitMarker + "') = 0"
	return relation.selectFrom(publishSQL, ownerRange), 2
}

func compileBoundedExactStringResults(
	relation compiledRelation,
	measures []compiledExactStringMeasure,
	counts []compiledDistinctCount,
	ownerRange spl.Range,
	stage int,
) (compiledRelation, int) {
	if len(measures) == 0 {
		return relation, 0
	}

	setColumns := make([]string, 0, len(measures))
	valuesMeasures := make([]compiledExactStringMeasure, 0, len(measures))
	valuesSetColumns := make([]string, 0, len(measures))
	seenSets := make(map[string]struct{}, len(measures))
	seenValuesSets := make(map[string]struct{}, len(measures))
	for _, measure := range measures {
		if _, seen := seenSets[measure.setColumn]; !seen {
			seenSets[measure.setColumn] = struct{}{}
			setColumns = append(setColumns, measure.setColumn)
		}
		if measure.function == plan.AggregateFunctionValues {
			valuesMeasures = append(valuesMeasures, measure)
			if _, seen := seenValuesSets[measure.setColumn]; !seen {
				seenValuesSets[measure.setColumn] = struct{}{}
				valuesSetColumns = append(valuesSetColumns, measure.setColumn)
			}
		}
	}

	windowColumns := make([]string, 0, 7)
	sortedValuesSets := make(map[string]string, len(valuesSetColumns))
	sortedSetColumns := make([]string, 0, len(valuesSetColumns))
	for index, setColumn := range valuesSetColumns {
		sorted := quoteIdentifier(fmt.Sprintf("__os_sorted_exact_strings_%d", index))
		sortedValuesSets[setColumn] = sorted
		sortedSetColumns = append(sortedSetColumns, sorted)
		windowColumns = append(windowColumns, "arraySort("+setColumn+") AS "+sorted)
	}

	cardinalityOverflow := ""
	cardinalityColumns := make([]string, 0, len(counts))
	seenCardinalities := make(map[string]struct{}, len(counts))
	for _, count := range counts {
		if _, seen := seenCardinalities[count.cardinalityColumn]; seen {
			continue
		}
		seenCardinalities[count.cardinalityColumn] = struct{}{}
		cardinalityColumns = append(cardinalityColumns, count.cardinalityColumn)
	}
	if len(cardinalityColumns) > 0 {
		maximumDistinctValues := strconv.FormatUint(MaximumStatsDistinctValuesPerGroup, 10)
		cardinalityConditions := make([]string, 0, len(cardinalityColumns))
		for _, cardinalityColumn := range cardinalityColumns {
			cardinalityConditions = append(
				cardinalityConditions,
				cardinalityColumn+" > toUInt64("+maximumDistinctValues+")",
			)
		}
		cardinalityOverflow = quoteIdentifier("__os_stats_distinct_any_overflow")
		windowColumns = append(
			windowColumns,
			"max(toUInt8("+strings.Join(cardinalityConditions, " OR ")+
				")) OVER () AS "+cardinalityOverflow,
		)
	}

	valuesOverflow := ""
	valuesTotalElements := ""
	valuesBytesOverflow := ""
	valuesTotalBytes := ""
	if len(valuesSetColumns) > 0 {
		maximumValues := strconv.FormatUint(MaximumStatsValuesPerGroup, 10)
		valueConditions := make([]string, 0, len(valuesSetColumns))
		byteConditions := make([]string, 0, len(valuesSetColumns))
		for _, setColumn := range valuesSetColumns {
			valueConditions = append(
				valueConditions,
				"length("+setColumn+") > toUInt64("+maximumValues+")",
			)
			byteConditions = append(
				byteConditions,
				stringArrayPayloadBytesSQL(setColumn)+" > toUInt128("+
					strconv.FormatUint(MaximumStatsValuesBytesPerGroup, 10)+")",
			)
		}
		valuesOverflow = quoteIdentifier("__os_stats_values_any_overflow")
		valuesTotalElements = quoteIdentifier("__os_stats_values_total_elements")
		valuesBytesOverflow = quoteIdentifier("__os_stats_values_bytes_any_overflow")
		valuesTotalBytes = quoteIdentifier("__os_stats_values_total_bytes")

		var rowElementTotal strings.Builder
		rowElementTotal.WriteString("toUInt128(0)")
		var rowByteTotal strings.Builder
		rowByteTotal.WriteString("toUInt128(0)")
		for _, measure := range valuesMeasures {
			// Deliberately retain duplicates: two public aliases create two
			// recursive list cells even when their aggregate state is shared.
			rowElementTotal.WriteString(" + toUInt128(length(")
			rowElementTotal.WriteString(measure.setColumn)
			rowElementTotal.WriteString("))")
			rowByteTotal.WriteString(" + ")
			rowByteTotal.WriteString(stringArrayPayloadBytesSQL(measure.setColumn))
		}
		windowColumns = append(
			windowColumns,
			"max(toUInt8(("+strings.Join(valueConditions, " OR ")+") OR "+rowElementTotal.String()+
				" > toUInt128("+strconv.FormatUint(MaximumStatsValuesPerGroup, 10)+
				"))) OVER () AS "+valuesOverflow,
			"sum("+rowElementTotal.String()+") OVER () AS "+valuesTotalElements,
			"max(toUInt8(("+strings.Join(byteConditions, " OR ")+") OR "+rowByteTotal.String()+
				" > toUInt128("+strconv.FormatUint(MaximumStatsValuesBytesPerGroup, 10)+
				"))) OVER () AS "+valuesBytesOverflow,
			"sum("+rowByteTotal.String()+") OVER () AS "+valuesTotalBytes,
		)
	}

	windowAlias := quoteIdentifier(fmt.Sprintf("_stage_%d", stage+1))
	windowSQL := "SELECT *, " + strings.Join(windowColumns, ", ") +
		" FROM (" + relation.sql + ") AS " + windowAlias
	relation = relation.selectFrom(windowSQL, ownerRange)

	excluded := append([]string(nil), setColumns...)
	excluded = append(excluded, sortedSetColumns...)
	excluded = append(excluded, cardinalityColumns...)
	if cardinalityOverflow != "" {
		excluded = append(excluded, cardinalityOverflow)
	}
	if valuesOverflow != "" {
		excluded = append(excluded, valuesOverflow, valuesTotalElements)
	}
	if valuesBytesOverflow != "" {
		excluded = append(excluded, valuesBytesOverflow, valuesTotalBytes)
	}
	projection := []string{"* EXCEPT (" + strings.Join(excluded, ", ") + ")"}
	for _, measure := range measures {
		switch measure.function {
		case plan.AggregateFunctionDistinctCount:
			projection = append(
				projection,
				"toUInt64(length("+measure.setColumn+")) AS "+measure.outputColumn,
			)
		case plan.AggregateFunctionValues:
			projection = append(
				projection,
				sortedValuesSets[measure.setColumn]+" AS "+measure.outputColumn,
			)
		}
	}
	for _, count := range counts {
		projection = append(
			projection,
			count.cardinalityColumn+" AS "+count.outputColumn,
		)
	}

	validations := make([]string, 0, 3)
	if cardinalityOverflow != "" {
		validations = append(
			validations,
			"throwIf(toUInt8("+cardinalityOverflow+" != 0), '"+
				ExactDistinctLimitMarker+"') = 0",
		)
	}
	if valuesBytesOverflow != "" {
		validations = append(
			validations,
			"throwIf(toUInt8("+valuesOverflow+" != 0 OR "+valuesTotalElements+
				" > toUInt128("+strconv.FormatUint(MaximumStatsValuesPerResult, 10)+")), '"+
				StatsValuesLimitMarker+"') = 0",
		)
		validations = append(
			validations,
			"throwIf(toUInt8("+valuesBytesOverflow+" != 0 OR "+valuesTotalBytes+
				" > toUInt128("+strconv.FormatUint(MaximumStatsValuesBytesPerResult, 10)+")), '"+
				StatsValuesBytesLimitMarker+"') = 0",
		)
	}
	publishAlias := quoteIdentifier(fmt.Sprintf("_stage_%d", stage+2))
	publishSQL := "SELECT " + strings.Join(projection, ", ") + " FROM (" + relation.sql +
		") AS " + publishAlias + " WHERE " + strings.Join(validations, " AND ")
	return relation.selectFrom(publishSQL, ownerRange), 2
}

func compileBoundedDistinctCountResults(
	relation compiledRelation,
	counts []compiledDistinctCount,
	ownerRange spl.Range,
	stage int,
) (compiledRelation, int) {
	if len(counts) == 0 {
		return relation, 0
	}
	maximum := strconv.FormatUint(MaximumStatsDistinctValuesPerGroup, 10)
	cardinalityColumns := make([]string, 0, len(counts))
	overflowConditions := make([]string, 0, len(counts))
	seenCardinalities := make(map[string]struct{}, len(counts))
	for _, count := range counts {
		if _, seen := seenCardinalities[count.cardinalityColumn]; seen {
			continue
		}
		seenCardinalities[count.cardinalityColumn] = struct{}{}
		cardinalityColumns = append(cardinalityColumns, count.cardinalityColumn)
		overflowConditions = append(
			overflowConditions,
			count.cardinalityColumn+" > toUInt64("+maximum+")",
		)
	}
	overflowColumn := quoteIdentifier("__os_stats_dc_any_overflow")
	windowAlias := quoteIdentifier(fmt.Sprintf("_stage_%d", stage+1))
	windowSQL := "SELECT *, max(toUInt8(" + strings.Join(overflowConditions, " OR ") +
		")) OVER () AS " + overflowColumn + " FROM (" + relation.sql + ") AS " + windowAlias
	relation = relation.selectFrom(windowSQL, ownerRange)

	excluded := append(cardinalityColumns, overflowColumn)
	projection := []string{"* EXCEPT (" + strings.Join(excluded, ", ") + ")"}
	for _, count := range counts {
		projection = append(projection, count.cardinalityColumn+" AS "+count.outputColumn)
	}
	publishAlias := quoteIdentifier(fmt.Sprintf("_stage_%d", stage+2))
	publishSQL := "SELECT " + strings.Join(projection, ", ") + " FROM (" + relation.sql +
		") AS " + publishAlias + " WHERE throwIf(toUInt8(" + overflowColumn +
		" != 0), '" + ExactDistinctLimitMarker + "') = 0"
	return relation.selectFrom(publishSQL, ownerRange), 2
}

func dynamicFiniteFloatOrNullSQL(valueSQL, typeSQL string) string {
	value := compiledScalar{valueSQL: valueSQL, dynamicTypeSQL: typeSQL, kind: fieldKindDynamic}
	numericOrString := "(" + typeSQL + " = 'String' OR " + dynamicNumericTypePredicate(typeSQL) + ")"
	converted := finiteFloatOrNullSQL("toString(" + valueSQL + ")")
	decimalTag := dynamicTaggedDecimalCondition(value)
	decimal := dynamicTaggedDecimalFloatSQL(value)
	return "multiIf(" + numericOrString + ", " + converted + ", " + decimalTag + ", " + decimal +
		", CAST(NULL AS Nullable(Float64)))"
}

func finiteFloatOrNullSQL(valueSQL string) string {
	return "ifNotFinite(toFloat64OrNull(" + valueSQL + "), CAST(NULL AS Nullable(Float64)))"
}

func finiteDynamicFloatOrNullSQL(valueSQL string) string {
	return "ifNotFinite(accurateCastOrNull(" + valueSQL +
		", 'Float64'), CAST(NULL AS Nullable(Float64)))"
}

func statsByScalarExpressions(field fieldState) (supported, lexical string) {
	return statsByScalarExpressionsFor(field.valueSQL, dynamicTypeExpression(field))
}

func statsByScalarExpressionsFor(
	valueSQL, typeSQL string,
) (supported, lexical string) {
	mapSQL := "dynamicElement(" + valueSQL + ", 'Map(String, String)')"
	valueKey := "concat(char(0), 'open_splunk_value')"
	value := compiledScalar{
		valueSQL:       valueSQL,
		dynamicTypeSQL: typeSQL,
		kind:           fieldKindDynamic,
	}
	extended := dynamicTaggedScalarEnvelopeCondition(value)
	// None is excluded deliberately. Missing and explicit-null leaves are
	// removed before aggregation, while a flattened object parent reads as None
	// at its literal path and must set the unsupported-container flag.
	supported = "(" + typeSQL + " IN ('String', 'Float64', 'Bool') OR " +
		dynamicIntegerTypePredicate(typeSQL) + " OR " + extended + ")"
	lexical = "if(" + typeSQL + " = 'Map(String, String)', " + mapSQL + "[" +
		valueKey + "], toString(" + valueSQL + "))"
	return supported, lexical
}

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
	additionalAliases++
	dedupAlias := quoteIdentifier(fmt.Sprintf("_stage_%d", stage+additionalAliases))
	dedupSQL := "SELECT * EXCEPT (" + strings.Join(helperColumns, ", ") + ") FROM (" + deduplicated.sql + ") AS " + dedupAlias + " WHERE " + predicate +
		" ORDER BY " + order + " LIMIT ? BY " + strings.Join(keyColumns, ", ")
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

	// Aggregate groups always have a strictly positive count, so an empty input
	// produces no row on which division could occur. Cast before multiplication
	// to avoid integer overflow and retain the unrounded SPL percentage.
	total := "sum(" + input.valueSQL + ") OVER ()"
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
