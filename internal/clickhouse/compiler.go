package clickhouse

import (
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

	"github.com/Suhaibinator/open-splunk/internal/eventfields"
	"github.com/Suhaibinator/open-splunk/internal/plan"
	"github.com/Suhaibinator/open-splunk/internal/spl"
	"github.com/Suhaibinator/open-splunk/internal/splpath"
	"github.com/Suhaibinator/open-splunk/internal/splregex"
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
	// Timechart physical columns are an executor-only transport. Runtime series
	// names are data, never SQL identifiers, and are expanded into the public
	// wide schema only after the complete bounded result has been validated.
	// The zero-based ordinal keeps epoch-aligned bucket starts out of
	// DateTime64 conversions: the first bucket may precede ClickHouse's
	// practical lower bound even though every selected event is representable.
	TimechartOrdinalColumn = "__os_timechart_ordinal"
	TimechartNamesColumn   = "__os_timechart_names"
	TimechartCountsColumn  = "__os_timechart_counts"
	TimechartInvalidColumn = "__os_timechart_invalid"
	// Chart physical columns are the same executor-only transport with one
	// additional column: the pivot's row axis is runtime data rather than a
	// plan-time constant, so the row value itself crosses the boundary beside
	// the dense ordinal that proves the server-side order was preserved.
	ChartOrdinalColumn = "__os_chart_ordinal"
	ChartRowColumn     = "__os_chart_row"
	ChartNamesColumn   = "__os_chart_names"
	ChartCountsColumn  = "__os_chart_counts"
	ChartInvalidColumn = "__os_chart_invalid"
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
	// maxCompiledNumericRoundingScalarSQLBytes independently bounds round,
	// ceil, and floor. Dynamic input is bound once, and integral results let
	// redundant outer ceil/floor calls collapse to identities.
	maxCompiledNumericRoundingScalarSQLBytes = 64 << 10
	// maxCompiledMVCountScalarSQLBytes independently bounds value-cardinality
	// expressions. Dynamic input is bound once, so nested calls grow linearly.
	maxCompiledMVCountScalarSQLBytes = 64 << 10
	// maxCompiledMatchScalarSQLBytes independently bounds regular-expression
	// predicate lowering. Each value is referenced once and each normalized
	// pattern remains a bound argument, so nested composition grows linearly.
	maxCompiledMatchScalarSQLBytes = 64 << 10
	// maxCompiledLikeScalarSQLBytes independently bounds wildcard-predicate
	// lowering. Each value is referenced once and each normalized pattern
	// remains a bound argument, so nested composition grows linearly.
	maxCompiledLikeScalarSQLBytes = 64 << 10
	// MaximumMatchInputBytes bounds regex work against calculated strings. A
	// stored event is capped at 1 MiB; the wider allowance admits one worst-
	// case UTF-8 case conversion while rejecting large replace amplification.
	MaximumMatchInputBytes uint64 = 4 << 20
	// MaximumLikeInputBytes bounds wildcard work against calculated strings.
	// LIKE has the same durable-input and UTF-8 case-expansion envelope as
	// match, while retaining an independent compatibility limit.
	MaximumLikeInputBytes uint64 = 4 << 20
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
	// even when aggregate state is shared. They bound intermediate transforming
	// results before a later head/sort limit can hide them.
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

	// UnsupportedStatsByValueMarker is emitted by the scalar-only stats BY
	// guard so the executor can classify the ClickHouse exception without
	// exposing generated SQL or storage details.
	UnsupportedStatsByValueMarker = "open-splunk: stats BY requires a scalar field"
	// UnsupportedStatsMeasureValueMarker is emitted when a string-oriented
	// stats measure encounters an object or nested container that has no
	// scalar SPL representation.
	UnsupportedStatsMeasureValueMarker = "open-splunk: stats measure requires scalar values"
	// UnsupportedStatsDistinctLimitMarker classifies an exact dc state that
	// exceeded its per-group, per-measure cardinality ceiling.
	UnsupportedStatsDistinctLimitMarker = "open-splunk: stats distinct values exceed the supported limit"
	// StatsValuesBytesLimitMarker classifies an exact values result whose
	// per-group raw lexical payload exceeded the supported byte ceiling.
	StatsValuesBytesLimitMarker = "open-splunk: stats values bytes exceed the supported limit"
	// StatsValuesLimitMarker classifies a values cell or complete transforming
	// result that exceeded its published element ceiling.
	StatsValuesLimitMarker = "open-splunk: stats values exceed the supported limit"
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
	// UnsupportedSpathValueMarker is emitted when an explicitly selected JSON
	// leaf is a container or a number that this compatibility slice cannot
	// publish without losing information.
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

var physicalIdentifier = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

// Compiler lowers backend-neutral logical plans to parameterized ClickHouse
// SQL. Database and table are trusted configuration and still pass a strict
// identifier allowlist; all user-authored values are query parameters.
type Compiler struct {
	Database string
	Table    string
}

// CompiledQuery is executable SQL plus ordered bind arguments and public
// result fields. Internal helper columns never appear in OutputFields.
type CompiledQuery struct {
	SQL          string
	Args         []any
	OutputFields []string
	Timechart    *TimechartOutput
	Chart        *ChartOutput
	// SparseFields marks ordinary raw-event output whose public fields object
	// must be reconstructed from the appended private presence column.
	SparseFields bool

	// relationalDepth is compiler evidence, not part of the execution
	// contract. Keeping it private prevents callers from treating the guard as
	// a tunable query option while allowing terminal analysis compilers to
	// extend an already validated event relation without reparsing SQL.
	relationalDepth      int
	relationalDepthRange spl.Range
}

// ChartOutput describes the bounded runtime-wide pivot contract. Both axes are
// runtime data, so the row column's public name and value kind are carried
// beside the series bounds and the physical type the transport must present.
type ChartOutput struct {
	RowField        string
	RowKind         ChartRowKind
	RowDatabaseType string
	RowLimit        uint64
	MaxSeries       uint16
	MaxLabelBytes   uint16
}

// TimechartOutput describes the bounded runtime-wide result contract. The SQL
// result itself has fixed private columns; OutputFields contains only the
// fixed public prefix, currently _time.
type TimechartOutput struct {
	FirstBucket   time.Time
	Span          time.Duration
	BucketCount   uint64
	MaxSeries     uint16
	MaxLabelBytes uint16
}

// Compile compiles one plan without mutating it.
func (c Compiler) Compile(query *plan.Query) (CompiledQuery, error) {
	return c.compileWithFinalizer(query, finalizeOrdinaryQuery, true)
}

type queryFinalizer func(
	relation compiledRelation,
	state compileState,
	args []any,
	scan *plan.Scan,
	aliasSequence int,
) (CompiledQuery, error)

// compileEventAnalysis proves that the final relation still consists of
// individual events before exposing it to an analysis-specific projection.
func (c Compiler) compileEventAnalysis(query *plan.Query, finalize queryFinalizer) (CompiledQuery, error) {
	if err := plan.ValidateFieldAnalysisEligibility(query); err != nil {
		return CompiledQuery{}, err
	}
	return c.compileWithFinalizer(query, finalize, false)
}

// compileWithFinalizer lowers every logical operator once, then delegates the
// final projection. permitTerminalWideOperators is reserved for ordinary search
// compilation; event analyses must consume only the proven event relation and
// therefore may reach neither timechart nor chart.
func (c Compiler) compileWithFinalizer(query *plan.Query, finalize queryFinalizer, permitTerminalWideOperators bool) (CompiledQuery, error) {
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
	if err := validateCompiledExtractionBudgets(query.Operators[1:]); err != nil {
		return CompiledQuery{}, err
	}
	fragment, state, args, err := compileScan(database, table, scan)
	if err != nil {
		return CompiledQuery{}, err
	}
	state.matchBudget = &compiledMatchBudget{
		patterns: make(map[*plan.ScalarCallExpression]splregex.MatchPattern),
	}
	state.likeBudget = &compiledLikeBudget{
		patterns: make(map[*plan.ScalarCallExpression]splwildcard.LikePattern),
	}
	relation := newScanRelation(fragment, scan.Range)

	aliasSequence := 0
	remainingOperators := query.Operators[1:]
	for operatorIndex, operator := range remainingOperators {
		if isNilPlanOperator(operator) {
			return CompiledQuery{}, fmt.Errorf("compile ClickHouse query: operator %d is nil", operatorIndex+1)
		}
		aliasSequence++
		alias := quoteIdentifier(fmt.Sprintf("_stage_%d", aliasSequence))
		switch operator := operator.(type) {
		case *plan.Filter:
			if complexityErr := validateCompiledPredicateComplexity(operator.Expression); complexityErr != nil {
				return CompiledQuery{}, complexityErr
			}
			materializedFields := predicateMaterializationFields(operator.Expression, state)
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
			predicate, predicateArgs, compileErr := compileExpression(operator.Expression, predicateState)
			if compileErr != nil {
				return CompiledQuery{}, compileErr
			}
			filterSQL := "SELECT * FROM (" + relation.sql + ") AS " + alias + " WHERE " + predicate
			if len(materializedFields) > 0 {
				materialized := quoteIdentifier(fmt.Sprintf("__os_filter_input_%d", aliasSequence))
				filterSQL = "WITH " + materialized + " AS MATERIALIZED (" + relation.sql + ") " +
					"SELECT * EXCEPT (" + strings.Join(excludedColumns, ", ") + ") REPLACE (" +
					strings.Join(replacements, ", ") + ") FROM " + materialized + " AS " +
					alias + " ARRAY JOIN " +
					strings.Join(bindings, ", ") + " WHERE " + predicate +
					materializedCTESettingsSQL
			}
			relation = relation.selectFrom(filterSQL, operator.Range)
			args = append(args, predicateArgs...)
			state = nextState
		case *plan.Project:
			projection, nextState, projectionArgs, compileErr := compileProjection(operator, state)
			if compileErr != nil {
				return CompiledQuery{}, compileErr
			}
			relation = relation.selectFrom(
				"SELECT "+strings.Join(projection, ", ")+" FROM ("+relation.sql+") AS "+alias,
				operator.Range,
			)
			args = append(args, projectionArgs...)
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
				nextSQL := upsertFieldProjectionSQL(
					relation.sql,
					state,
					assignment.Output.Name,
					value.valueSQL,
					alias,
				)
				relation = relation.selectFrom(nextSQL, operator.Range)
				if err := validateRelationalDepth(relation.depth, relation.ownerRange); err != nil {
					return CompiledQuery{}, err
				}
				// Extend is emitted in an outer SELECT, so its placeholders occur
				// before every placeholder already present in the nested fragment.
				// Sequential assignments add another outer SELECT and therefore
				// prepend in reverse nesting order as well.
				args = prependArguments(value.valueArgs, args)
				state = extendCompileState(state, assignment.Output, value)
				if index+1 < len(operator.Assignments) {
					aliasSequence++
					alias = quoteIdentifier(fmt.Sprintf("_stage_%d", aliasSequence))
				}
			}
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
		case *plan.Aggregate:
			if cardinalityErr := validateAggregateCardinality(operator); cardinalityErr != nil {
				return CompiledQuery{}, cardinalityErr
			}
			if validateErr := validateAggregatePredicateMeasures(operator, state); validateErr != nil {
				return CompiledQuery{}, validateErr
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
			if len(nextState.postAggregateChronological) > 0 {
				var additionalAliases int
				var barrier *compiledChronologicalBarrier
				relation, additionalAliases, barrier = compileChronologicalResults(
					relation,
					nextState.postAggregateChronological,
					operator.Range,
					aliasSequence,
				)
				aliasSequence += additionalAliases
				if barrier != nil {
					barrier.args = append([]any(nil), args...)
					args = nil
					nextState.chronologicalBarriers = append(
						nextState.chronologicalBarriers,
						*barrier,
					)
				}
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
		case *plan.Timechart:
			if !permitTerminalWideOperators {
				return CompiledQuery{}, errors.New("compile ClickHouse query: timechart is unavailable for event analysis")
			}
			if operatorIndex+1 != len(remainingOperators) {
				return CompiledQuery{}, errors.New("compile ClickHouse timechart: operator must be terminal")
			}
			compiled, compileErr := compileTimechart(relation, state, args, operator, query.DynamicOutput, alias)
			if compileErr != nil {
				return CompiledQuery{}, compileErr
			}
			if len(state.chronologicalBarriers) > 0 {
				compiled, compileErr = wrapCompiledChronologicalValidation(
					compiled,
					state,
					aliasSequence,
				)
				if compileErr != nil {
					return CompiledQuery{}, compileErr
				}
			}
			if compileErr = validateCompiledRelationalDepth(compiled); compileErr != nil {
				return CompiledQuery{}, compileErr
			}
			if len(compiled.SQL) > maxCompiledQueryBytes {
				return CompiledQuery{}, &plan.Diagnostic{
					Code:    "SPL_QUERY_TOO_COMPLEX",
					Message: fmt.Sprintf("compiled query exceeds %d bytes", maxCompiledQueryBytes),
					Range:   operator.Range,
				}
			}
			return compiled, nil
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
			if len(state.chronologicalBarriers) > 0 {
				compiled, compileErr = wrapCompiledChronologicalValidation(
					compiled,
					state,
					aliasSequence,
				)
				if compileErr != nil {
					return CompiledQuery{}, compileErr
				}
			}
			if compileErr = validateCompiledRelationalDepth(compiled); compileErr != nil {
				return CompiledQuery{}, compileErr
			}
			if len(compiled.SQL) > maxCompiledQueryBytes {
				return CompiledQuery{}, &plan.Diagnostic{
					Code:    "SPL_QUERY_TOO_COMPLEX",
					Message: fmt.Sprintf("compiled query exceeds %d bytes", maxCompiledQueryBytes),
					Range:   operator.Range,
				}
			}
			return compiled, nil
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
			materialized, sortKeys, order, compileErr := compileSort(operator.Keys, state, aliasSequence)
			if compileErr != nil {
				return CompiledQuery{}, compileErr
			}
			sortSQL := "SELECT *, " + strings.Join(materialized, ", ") + " FROM (" + relation.sql + ") AS " + alias + " ORDER BY " + order
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
		default:
			return CompiledQuery{}, fmt.Errorf("compile ClickHouse query: unsupported logical operator %T", operator)
		}
		if err := validateRelationalDepth(relation.depth, relation.ownerRange); err != nil {
			return CompiledQuery{}, err
		}
	}

	compiled, err := finalize(relation, state, args, scan, aliasSequence)
	if err != nil {
		return CompiledQuery{}, err
	}
	if err := validateFinalizedRelationalDepth(relation, compiled); err != nil {
		return CompiledQuery{}, err
	}
	if len(compiled.SQL) > maxCompiledQueryBytes {
		return CompiledQuery{}, &plan.Diagnostic{
			Code:    "SPL_QUERY_TOO_COMPLEX",
			Message: fmt.Sprintf("compiled query exceeds %d bytes", maxCompiledQueryBytes),
			Range:   scan.Range,
		}
	}
	return compiled, nil
}

func validateCompiledExtractionBudgets(operators []plan.Operator) error {
	outputs := 0
	spathWorkUnits := 0
	for _, operator := range operators {
		switch operator := operator.(type) {
		case *plan.Extract:
			if operator == nil {
				continue
			}
			outputs += len(operator.Captures)
		case *plan.ExtractJSON:
			if operator == nil {
				continue
			}
			steps, err := validateExtractJSONOperator(operator)
			if err != nil {
				return err
			}
			outputs++
			spathWorkUnits += splpath.EvaluationWorkUnits(steps)
			if spathWorkUnits > splpath.MaximumEvaluationWorkUnits {
				return &plan.Diagnostic{
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
			return &plan.Diagnostic{
				Code: "SPL_QUERY_TOO_COMPLEX",
				Message: fmt.Sprintf(
					"search creates more than %d extraction output fields",
					maxCompiledExtractionOutputs,
				),
				Range: operator.SourceRange(),
			}
		}
	}
	return nil
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
		Message: "like pattern must be valid UTF-8 without NUL bytes",
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
	if len(state.chronologicalBarriers) > 0 {
		return finalizeChronologicallyValidatedQuery(
			relation,
			state,
			args,
			projection,
			outputFields,
			sparseFields,
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
			SQL: relation.sql, Args: args, OutputFields: outputFields,
			SparseFields: sparseFields,
		},
		relation.depth,
		relation.ownerRange,
	), nil
}

func finalizeChronologicallyValidatedQuery(
	relation compiledRelation,
	state compileState,
	args []any,
	projection []string,
	outputFields []string,
	sparseFields bool,
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
	return wrapChronologicalValidation(
		relation.sql,
		relation.depth,
		relation.ownerRange,
		state.chronologicalBarriers,
		projection,
		resultColumns,
		order,
		CompiledQuery{
			Args:         args,
			OutputFields: outputFields,
			SparseFields: sparseFields,
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
	switch {
	case compiled.Chart != nil && compiled.Timechart == nil:
		resultColumns = []string{
			ChartOrdinalColumn,
			ChartRowColumn,
			ChartNamesColumn,
			ChartCountsColumn,
			ChartInvalidColumn,
		}
	case compiled.Timechart != nil && compiled.Chart == nil:
		resultColumns = []string{
			TimechartOrdinalColumn,
			TimechartNamesColumn,
			TimechartCountsColumn,
			TimechartInvalidColumn,
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
	return wrapChronologicalValidation(
		compiled.SQL,
		compiled.relationalDepth,
		compiled.relationalDepthRange,
		state.chronologicalBarriers,
		projection,
		resultColumns,
		quoteIdentifier(resultColumns[0])+" ASC",
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
	compiled CompiledQuery,
	aliasSequence int,
) (CompiledQuery, error) {
	if len(barriers) == 0 || inputSQL == "" || inputDepth <= 0 ||
		len(projection) == 0 || len(resultColumns) != len(projection) {
		return CompiledQuery{}, errors.New(
			"compile ClickHouse query: chronological validation envelope is invalid",
		)
	}
	definitions := make([]string, 0, len(barriers)+2)
	barrierArgs := make([]any, 0)
	for _, barrier := range barriers {
		definitions = append(
			definitions,
			barrier.name+" AS MATERIALIZED ("+barrier.sql+")",
		)
		barrierArgs = append(barrierArgs, barrier.args...)
	}

	finalInput := quoteIdentifier(fmt.Sprintf("__os_chronological_final_input_%d", aliasSequence+1))
	definitions = append(definitions, finalInput+" AS MATERIALIZED ("+inputSQL+")")
	validationName := quoteIdentifier(fmt.Sprintf(
		"__os_chronological_validation_%d",
		aliasSequence+1,
	))

	aliasSequence++
	mainAlias := quoteIdentifier(fmt.Sprintf("_stage_%d", aliasSequence))
	aliasSequence++
	mainValidationAlias := quoteIdentifier(fmt.Sprintf("_stage_%d", aliasSequence))
	main := "SELECT " + strings.Join(projection, ", ") + " FROM " + finalInput +
		" AS " + mainAlias + " CROSS JOIN " + validationName + " AS " +
		mainValidationAlias + " WHERE " + mainValidationAlias + "." +
		quoteIdentifier("__os_chronological_valid") + " = 0"
	if order != "" {
		main += " ORDER BY " + order
	}

	dummyProjection := make([]string, 0, len(resultColumns))
	for _, name := range resultColumns {
		column := quoteIdentifier(name)
		dummyProjection = append(dummyProjection, "any("+column+") AS "+column)
	}

	aliasSequence++
	schemaSourceAlias := quoteIdentifier(fmt.Sprintf("_stage_%d", aliasSequence))
	schemaSource := "SELECT " + strings.Join(projection, ", ") + " FROM " +
		finalInput + " AS " + schemaSourceAlias + " LIMIT 0"
	aliasSequence++
	schemaAlias := quoteIdentifier(fmt.Sprintf("_stage_%d", aliasSequence))
	dummy := "SELECT " + strings.Join(dummyProjection, ", ") + " FROM (" +
		schemaSource + ") AS " + schemaAlias

	validationRows := make([]string, 0, len(barriers))
	maximumBarrierDepth := 0
	for _, barrier := range barriers {
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
	validationUnion := strings.Join(validationRows, " UNION ALL ")
	validationRowsDepth := relationalNodeDepth(maximumBarrierDepth)
	validationDepth := relationalNodeDepth(validationRowsDepth)
	validation := "SELECT if(maxOrDefault(" + quoteIdentifier("__os_chronological_invalid") +
		") != 0, throwIf(toUInt8(1), '" + UnsupportedStatsMeasureValueMarker +
		"'), toUInt8(0)) AS " + quoteIdentifier("__os_chronological_valid") +
		" FROM (" + validationUnion + ")"
	definitions = append(definitions, validationName+" AS MATERIALIZED ("+validation+")")

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
	schemaSourceDepth := relationalNodeDepth(inputDepth)
	dummyDepth := relationalNodeDepth(schemaSourceDepth)
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
	exactNumericBinExponentClamp    = 10_000
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
		outputExistsSQL = "if(" + metadata.existsAlias + " != 0, 1, " + previousExistsAlias + ")"
		outputTypeSQL = "if(" + metadata.existsAlias + " != 0, " + bucketTypeSQL + ", " + previousTypeAlias + ")"
	}
	supportedProjectionSQL := dynamicNumericBinProjectionLayer(relation.sql, stage, layer, []string{
		supportedSQL + " AS " + supportedAlias,
		"toUInt8(" + outputExistsSQL + ") AS " + outputExistsAlias,
		"toUInt8(" + outputTypeSQL + ") AS " + outputTypeAlias,
	})
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
		return fmt.Errorf("compile ClickHouse %s: invalid %s field: %w", operation, role, err)
	}
	if resolved.Name != field.Name || resolved.Canonical != field.Canonical ||
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
	name := quoteIdentifier(outputName)
	if _, replacing := state.visible[outputName]; replacing {
		return "SELECT * REPLACE (" + value + " AS " + name + ") FROM (" + fragment + ") AS " + alias
	}
	return "SELECT *, " + value + " AS " + name + " FROM (" + fragment + ") AS " + alias
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
		kind:                    fieldKindDynamic,
		caseSensitive:           false,
		materializeForPredicate: true,
	}
	next.privateColumns = livePrivateColumns(next.privateColumns, next.visible)
	next.privateColumns = append(next.privateColumns, existsAlias, typeAlias)
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

func compileTimechart(
	relation compiledRelation,
	state compileState,
	args []any,
	operator *plan.Timechart,
	dynamic *plan.DynamicSeriesOutput,
	alias string,
) (CompiledQuery, error) {
	if operator == nil || operator.Function != plan.AggregateFunctionCountRows {
		return CompiledQuery{}, errors.New("compile ClickHouse timechart: count operator is required")
	}
	if dynamic == nil || !slices.Equal(dynamic.FixedFields, []string{"_time"}) || dynamic.MaxSeries == 0 {
		return CompiledQuery{}, errors.New("compile ClickHouse timechart: dynamic output contract is invalid")
	}
	if operator.Span < time.Second || operator.Span > 24*time.Hour || operator.Span%time.Second != 0 || operator.FirstBucket.Nanosecond() != 0 ||
		operator.FirstBucket.IsZero() || operator.BucketCount == 0 || operator.BucketCount > 10_000 || operator.SeriesLimit != 10 ||
		dynamic.MaxSeries != 12 || uint32(operator.SeriesLimit)+2 != uint32(dynamic.MaxSeries) || !operator.IncludeNull || !operator.IncludeOther ||
		operator.NullLabel != "NULL" || operator.OtherLabel != "OTHER" || !operator.FixedRange ||
		!operator.Continuous || !operator.IncludePartial {
		return CompiledQuery{}, errors.New("compile ClickHouse timechart: bounded defaults are invalid")
	}
	spanSeconds := int64(operator.Span / time.Second)
	if spanSeconds <= 0 || operator.FirstBucket.Unix()%spanSeconds != 0 {
		return CompiledQuery{}, errors.New("compile ClickHouse timechart: first bucket is not epoch aligned")
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

	splitField, splitExists, err := resolveCompiledField(operator.SplitBy, state)
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
	// The exact-presence placeholder occurs before the nested scoped fragment.
	// Descendant detection is emitted in the following CTE so exact leaves do
	// not pay for a second field_names scan.
	args = prependArguments(splitField.existsArgs, args)
	if splitField.kind == fieldKindDynamic && splitField.descendantSQL != "" {
		args = append(args, splitField.descendantArgs...)
	}

	q := quoteIdentifier
	source := q("__os_timechart_source")
	prepared := q("__os_timechart_prepared")
	classified := q("__os_timechart_classified")
	canonicalized := q("__os_timechart_canonicalized")
	counts := q("__os_timechart_group_counts")
	top := q("__os_timechart_top")
	collapsed := q("__os_timechart_collapsed")
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
	bucketNumber := q("__os_tc_bucket_number")
	kind := q("__os_tc_kind")
	frequency := q("__os_tc_count")
	encoded := q("__os_tc_encoded")
	normalized := q("__os_tc_normalized")
	collision := q("__os_tc_collision")
	sortLabel := q("__os_tc_sort_label")
	countMap := q("__os_tc_count_map")
	invalid := q("__os_tc_invalid")
	ordinal := q(TimechartOrdinalColumn)

	spanNanoseconds := int64(operator.Span)
	bucketNumberExpression := epochFloorBucketNumberSQL(ticks)
	validLabel := "isValidUTF8(" + label + ") AND length(" + label + ") BETWEEN 1 AND " +
		strconv.Itoa(maxTimechartLabelBytes) + " AND " + label + " NOT IN ('NULL', 'OTHER')"

	var sql strings.Builder
	sql.Grow(len(relation.sql) + 8_192)
	sql.WriteString("WITH ")
	sql.WriteString(source)
	sql.WriteString(" AS (SELECT ")
	sql.WriteString(timeField.valueSQL + " AS " + eventTime + ", ")
	sql.WriteString(splitField.valueSQL + " AS " + value + ", ")
	sql.WriteString("toUInt8(" + existsSQL + ") AS " + present + ", ")
	sql.WriteString(valueTypeSQL + " AS " + valueType)
	if splitField.kind == fieldKindDynamic && splitField.descendantSQL != "" {
		sql.WriteString(", " + q(internalFieldNamesColumn))
	}
	sql.WriteString(" FROM (")
	sql.WriteString(relation.sql)
	sql.WriteString(") AS " + alias + "), ")

	sql.WriteString(prepared)
	sql.WriteString(" AS (SELECT *, ")
	if splitField.kind == fieldKindDynamic && splitField.descendantSQL != "" {
		sql.WriteString("toUInt8(if(" + present + " != 0, 0, " + splitField.descendantSQL + ")) AS " + descendant + ", ")
	} else {
		sql.WriteString("toUInt8(0) AS " + descendant + ", ")
	}
	sql.WriteString("reinterpretAsInt64(" + eventTime + ") AS " + ticks + ", ")
	sql.WriteString("if(" + present + " != 0 AND isNotNull(" + value + ") AND " + valueType + " = 'String', ")
	sql.WriteString("assumeNotNull(toString(" + value + ")), CAST('' AS String)) AS " + label)
	sql.WriteString(" FROM " + source + "), ")

	sql.WriteString(classified)
	sql.WriteString(" AS (SELECT " + bucketNumberExpression + " AS " + bucketNumber + ", ")
	sql.WriteString("multiIf(" + descendant + " != 0, toUInt8(3), " + present + " = 0 OR isNull(" + value + ") OR " + valueType + " = 'None', toUInt8(1), ")
	sql.WriteString(valueType + " != 'String', toUInt8(3), NOT (" + validLabel + "), toUInt8(3), toUInt8(0)) AS " + kind + ", " + label)
	sql.WriteString(" FROM " + prepared + "), ")

	sql.WriteString(canonicalized)
	sql.WriteString(" AS (SELECT " + bucketNumber + ", " + kind + ", if(" + kind + " = 0, " + label + ", CAST('' AS String)) AS " + label)
	sql.WriteString(" FROM " + classified + "), ")

	sql.WriteString(counts)
	sql.WriteString(" AS MATERIALIZED (SELECT " + bucketNumber + ", " + kind + ", " + label + ", count() AS " + frequency)
	sql.WriteString(" FROM " + canonicalized + " GROUP BY " + bucketNumber + ", " + kind + ", " + label + "), ")

	sql.WriteString(top)
	sql.WriteString(" AS MATERIALIZED (SELECT " + label + ", sum(" + frequency + ") AS " + frequency + " FROM " + counts)
	sql.WriteString(" WHERE " + kind + " = 0 GROUP BY " + label + " ORDER BY " + frequency + " DESC, " + label + " ASC LIMIT ")
	sql.WriteString(strconv.FormatUint(uint64(operator.SeriesLimit), 10))
	sql.WriteString("), ")

	sql.WriteString(collapsed)
	sql.WriteString(" AS (SELECT " + bucketNumber + ", multiIf(" + kind + " = 1, '1:', ")
	sql.WriteString(label + " IN (SELECT " + label + " FROM " + top + "), concat('0:', " + label + "), '2:') AS " + encoded + ", ")
	sql.WriteString("sum(" + frequency + ") AS " + frequency + " FROM " + counts + " WHERE " + kind + " IN (0, 1) GROUP BY " + bucketNumber + ", " + encoded + "), ")

	sql.WriteString(domainRows)
	sql.WriteString(" AS (SELECT toUInt8(0) AS sort_kind, if(startsWith(")
	sql.WriteString(label)
	sql.WriteString(", '_'), concat('VALUE', ")
	sql.WriteString(label)
	sql.WriteString("), ")
	sql.WriteString(label)
	sql.WriteString(") AS ")
	sql.WriteString(sortLabel)
	sql.WriteString(", concat('0:', ")
	sql.WriteString(label)
	sql.WriteString(") AS ")
	sql.WriteString(encoded)
	sql.WriteString(" FROM ")
	sql.WriteString(top)
	// Both sentinels probe the materialized aggregate as an ordinary relation.
	// A scalar subquery would be evaluated during analysis, before the
	// materialized temporary table exists, and would re-run the scoped scan
	// once per occurrence.
	sql.WriteString(" UNION ALL SELECT toUInt8(1), CAST('' AS String), CAST('1:' AS String) FROM (SELECT 1 FROM " + counts + " WHERE " + kind + " = 1 LIMIT 1)")
	sql.WriteString(" UNION ALL SELECT toUInt8(2), CAST('' AS String), CAST('2:' AS String) FROM (SELECT 1 FROM " + counts + " WHERE " + kind + " = 0 AND " + label + " NOT IN (SELECT " + label + " FROM " + top + ") LIMIT 1)), ")

	sql.WriteString(domain)
	sql.WriteString(" AS (SELECT arrayMap(item -> item.3, arraySort(item -> (item.1, item.2), groupArray((sort_kind, " + sortLabel + ", " + encoded + ")))) AS names FROM " + domainRows + "), ")

	sql.WriteString(collisions)
	sql.WriteString(" AS (SELECT toUInt8(count() > 0) AS " + collision + " FROM (")
	sql.WriteString("SELECT if(startsWith(" + label + ", '_'), concat('VALUE', " + label + "), " + label + ") AS " + normalized)
	sql.WriteString(" FROM " + counts + " WHERE " + kind + " = 0 GROUP BY " + normalized + " HAVING uniqExact(" + label + ") > 1 LIMIT 1)), ")

	sql.WriteString(bucketMaps)
	sql.WriteString(" AS (SELECT " + bucketNumber + ", mapFromArrays(groupArray(" + encoded + "), groupArray(" + frequency + ")) AS " + countMap)
	sql.WriteString(" FROM " + collapsed + " GROUP BY " + bucketNumber + "), ")

	sql.WriteString(validation)
	sql.WriteString(" AS (SELECT toUInt8(sumIf(" + frequency + ", " + kind + " = 3) > 0) AS " + invalid + " FROM " + counts + "), ")

	sql.WriteString(grid)
	sql.WriteString(" AS (" + ordinalGridSQL(ordinal, bucketNumber) + ") ")

	sql.WriteString("SELECT " + grid + "." + ordinal + " AS " + ordinal + ", " + domain + ".names AS " + q(TimechartNamesColumn) + ", ")
	sql.WriteString("arrayMap(name -> ifNull(" + bucketMaps + "." + countMap + "[name], toUInt64(0)), " + domain + ".names) AS " + q(TimechartCountsColumn) + ", ")
	sql.WriteString("toUInt8(" + validation + "." + invalid + " != 0 OR " + collisions + "." + collision + " != 0) AS " + q(TimechartInvalidColumn))
	sql.WriteString(" FROM " + grid + " CROSS JOIN " + domain + " CROSS JOIN " + validation + " CROSS JOIN " + collisions)
	sql.WriteString(" LEFT JOIN " + bucketMaps + " ON " + bucketMaps + "." + bucketNumber + " = " + grid + "." + bucketNumber + " ORDER BY " + grid + "." + ordinal + " ASC")
	sql.WriteString(materializedCTESettingsSQL)

	args = appendOrdinalGridArgs(args, spanNanoseconds, firstBucketNumber, operator.BucketCount)
	sourceDepth := relationalNodeDepth(relation.depth)
	preparedDepth := relationalNodeDepth(sourceDepth)
	classifiedDepth := relationalNodeDepth(preparedDepth)
	canonicalizedDepth := relationalNodeDepth(classifiedDepth)
	countsDepth := relationalNodeDepth(canonicalizedDepth)
	topDepth := relationalNodeDepth(countsDepth)
	topMembershipDepth := relationalNodeDepth(topDepth)
	collapsedDepth := relationalNodeDepth(countsDepth, topMembershipDepth)

	domainTopBranchDepth := relationalNodeDepth(topDepth)
	domainNullInputDepth := relationalNodeDepth(countsDepth)
	domainNullBranchDepth := relationalNodeDepth(domainNullInputDepth)
	domainOtherInputDepth := relationalNodeDepth(countsDepth, topMembershipDepth)
	domainOtherBranchDepth := relationalNodeDepth(domainOtherInputDepth)
	domainRowsDepth := relationalNodeDepth(
		domainTopBranchDepth,
		domainNullBranchDepth,
		domainOtherBranchDepth,
	)
	domainDepth := relationalNodeDepth(domainRowsDepth)
	collisionInputDepth := relationalNodeDepth(countsDepth)
	collisionsDepth := relationalNodeDepth(collisionInputDepth)
	bucketMapsDepth := relationalNodeDepth(collapsedDepth)
	validationDepth := relationalNodeDepth(countsDepth)
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
			FirstBucket:   operator.FirstBucket.UTC(),
			Span:          operator.Span,
			BucketCount:   operator.BucketCount,
			MaxSeries:     dynamic.MaxSeries,
			MaxLabelBytes: maxTimechartLabelBytes,
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
func chartRowColumnType(name string, field fieldState) (databaseType string, kind ChartRowKind, err error) {
	switch field.kind {
	case fieldKindInvalid, fieldKindString, fieldKindDynamic:
		if name == "_raw" {
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
	if operator == nil || operator.Function != plan.AggregateFunctionCountRows {
		return CompiledQuery{}, errors.New("compile ClickHouse chart: count operator is required")
	}
	rowName := operator.Over.Name
	if dynamic == nil || !slices.Equal(dynamic.FixedFields, []string{rowName}) || dynamic.MaxSeries == 0 {
		return CompiledQuery{}, errors.New("compile ClickHouse chart: dynamic output contract is invalid")
	}
	// The plan carries the complete bounding contract as data precisely so the
	// backend can revalidate it before emitting SQL.
	if rowName == "" || operator.SplitBy.Name == "" || rowName == operator.SplitBy.Name ||
		operator.RowLimit == 0 || uint64(operator.RowLimit) > uint64(maxChartRowValues) || operator.SeriesLimit != 10 ||
		dynamic.MaxSeries != 12 || uint32(operator.SeriesLimit)+2 != uint32(dynamic.MaxSeries) ||
		!operator.IncludeNull || !operator.IncludeOther ||
		operator.NullLabel != "NULL" || operator.OtherLabel != "OTHER" {
		return CompiledQuery{}, errors.New("compile ClickHouse chart: bounded defaults are invalid")
	}
	for _, axis := range []plan.FieldRef{operator.Over, operator.SplitBy} {
		if axis.Name == operator.NullLabel || axis.Name == operator.OtherLabel {
			return CompiledQuery{}, &plan.Diagnostic{
				Code:    "SPL_UNSUPPORTED_CHART_FIELD_TYPE",
				Message: "NULL and OTHER are reserved chart series names",
				Range:   axis.Range,
			}
		}
		if state.eventRows && state.allowDynamic && axis.Name == "fields" {
			return CompiledQuery{}, &plan.Diagnostic{
				Code:    "SPL_UNSUPPORTED_CHART_FIELD_TYPE",
				Message: "chart cannot use the event result's reserved fields payload without an exact upstream schema",
				Range:   axis.Range,
			}
		}
	}

	rowField, rowResolved, err := resolveCompiledField(operator.Over, state)
	if err != nil {
		return CompiledQuery{}, err
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
	if rowField.kind == fieldKindStringArray {
		return CompiledQuery{}, unsupportedMultivalueUsage("chart row field", operator.Over.Range)
	}
	rowDatabaseType, rowKind, err := chartRowColumnType(rowName, rowField)
	if err != nil {
		return CompiledQuery{}, err
	}

	splitField, splitResolved, err := resolveCompiledField(operator.SplitBy, state)
	if err != nil {
		return CompiledQuery{}, err
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
	if splitField.kind == fieldKindStringArray {
		return CompiledQuery{}, unsupportedMultivalueUsage("chart column field", operator.SplitBy.Range)
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
		return CompiledQuery{}, &plan.Diagnostic{
			Code:        "SPL_UNSUPPORTED_CHART_FIELD_TYPE",
			Message:     "chart column fields currently support strings plus missing and null values",
			Range:       operator.SplitBy.Range,
			Suggestions: []string{"convert the column field to a string before chart"},
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
	domainRows := q("__os_chart_domain_rows")
	domain := q("__os_chart_domain")
	collisions := q("__os_chart_normalization_collisions")
	columnCheck := q("__os_chart_column_check")
	rowMaps := q("__os_chart_row_maps")
	validation := q("__os_chart_validation")
	rowDomain := q("__os_chart_row_domain")

	rowValue := q("__os_ch_row_value")
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
	frequency := q("__os_ch_count")
	encoded := q("__os_ch_encoded")
	normalized := q("__os_ch_normalized")
	sortLabel := q("__os_ch_sort_label")
	countMap := q("__os_ch_count_map")
	invalid := q("__os_ch_invalid")
	collision := q("__os_ch_collision")
	columnInvalid := q("__os_ch_column_invalid")
	ordinal := q(ChartOrdinalColumn)

	// Placeholder order follows CTE nesting, not declaration order. Exact
	// presence probes sit in the outer CTE that wraps the scoped fragment and
	// therefore precede every nested argument; descendant detection and the
	// reserved-column-name probe are emitted afterwards and append in the order
	// they appear.
	var prefixArgs []any
	prefixArgs = append(prefixArgs, rowField.existsArgs...)
	prefixArgs = append(prefixArgs, splitField.existsArgs...)
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
	sql.WriteString(rowField.valueSQL + " AS " + rowValue + ", ")
	sql.WriteString("toUInt8(" + rowExistsSQL + ") AS " + rowExact + ", ")
	if rowDynamic {
		sql.WriteString(dynamicTypeExpression(rowField) + " AS " + rowType + ", ")
	}
	sql.WriteString(splitField.valueSQL + " AS " + value + ", ")
	sql.WriteString("toUInt8(" + splitExistsSQL + ") AS " + present + ", ")
	sql.WriteString(splitTypeSQL + " AS " + valueType)
	if rowHasDescendant || splitHasDescendant {
		sql.WriteString(", " + q(internalFieldNamesColumn))
	}
	sql.WriteString(" FROM (")
	sql.WriteString(relation.sql)
	sql.WriteString(") AS " + alias + "), ")

	sql.WriteString(prepared)
	sql.WriteString(" AS (SELECT *, ")
	sql.WriteString("toUInt8(" + rowPresenceSQL + ") AS " + rowPresent + ", ")
	if splitHasDescendant {
		sql.WriteString("toUInt8(if(" + present + " != 0, 0, " + splitField.descendantSQL + ")) AS " + descendant + ", ")
	} else {
		sql.WriteString("toUInt8(0) AS " + descendant + ", ")
	}
	sql.WriteString("toUInt8(" + rowSupportedSQL + ") AS " + rowSupported + ", ")
	sql.WriteString("if(" + present + " != 0 AND isNotNull(" + value + ") AND " + valueType + " = 'String', ")
	sql.WriteString("assumeNotNull(toString(" + value + ")), CAST('' AS String)) AS " + label)
	sql.WriteString(" FROM " + source + "), ")

	// The column value is classified before row eligibility is considered. A
	// container, a non-string scalar, or an unusable label fails the whole
	// command on its own presence, exactly as compileAggregate validates each
	// BY key independently: an unsupported column value must not become
	// invisible because some other event happened to omit the row field.
	sql.WriteString(kinded)
	sql.WriteString(" AS (SELECT *, ")
	sql.WriteString("multiIf(" + descendant + " != 0, toUInt8(3), " + present + " = 0 OR isNull(" + value + ") OR " + valueType + " = 'None', toUInt8(1), ")
	sql.WriteString(valueType + " != 'String', toUInt8(3), NOT (" + validLabel + "), toUInt8(3), toUInt8(0)) AS " + kind)
	sql.WriteString(" FROM " + prepared + "), ")

	// Row eligibility matches stats BY exactly: only present, non-null row
	// values name a row, which is what makes the per-row totals equal
	// stats count BY <row field>. Ineligible rows are retained here so the
	// column-axis rejection above still sees them, and are dropped by the
	// row-keyed aggregation below.
	sql.WriteString(classified)
	sql.WriteString(" AS (SELECT " + rowKeySQL + " AS " + row + ", toUInt8(" + rowSupported + " = 0) AS " + rowInvalid + ", ")
	sql.WriteString(rowPresent + " AS " + rowEligible + ", " + kind + ", " + label)
	sql.WriteString(" FROM " + kinded + "), ")

	sql.WriteString(canonicalized)
	sql.WriteString(" AS (SELECT " + row + ", " + rowInvalid + ", " + rowEligible + ", " + kind)
	sql.WriteString(", if(" + kind + " = 0, " + label + ", CAST('' AS String)) AS " + label)
	sql.WriteString(" FROM " + classified + "), ")

	// The column axis is collapsed before the row-keyed aggregation, so the
	// only wide intermediate is this one-dimensional label aggregate whose
	// state count is the number of distinct raw column values. Its frequency
	// counts row-eligible input only, matching the counts the pivot publishes,
	// while the group itself exists for every classified input row so the
	// atomic column rejection is visible without a second scoped scan.
	sql.WriteString(labelTotals)
	sql.WriteString(" AS MATERIALIZED (SELECT " + kind + ", " + label + ", countIf(" + rowEligible + " != 0) AS " + frequency)
	sql.WriteString(" FROM " + canonicalized + " GROUP BY " + kind + ", " + label + "), ")

	sql.WriteString(top)
	sql.WriteString(" AS MATERIALIZED (SELECT " + label + ", " + frequency + " FROM " + labelTotals)
	sql.WriteString(" WHERE " + kind + " = 0 AND " + frequency + " > 0 ORDER BY " + frequency + " DESC, " + label + " ASC LIMIT ")
	sql.WriteString(strconv.FormatUint(uint64(operator.SeriesLimit), 10))
	sql.WriteString("), ")

	// The row-keyed aggregation. Every column value is already encoded into the
	// published domain, so this holds exactly one state per (row value, public
	// series) pair plus one canonical unsupported-value state per row — the
	// bound the executor's expanded group budget describes.
	sql.WriteString(counts)
	sql.WriteString(" AS MATERIALIZED (SELECT " + row + ", " + kind + ", multiIf(" + kind + " = 1, '1:', " + kind + " = 3, CAST('' AS String), ")
	sql.WriteString(label + " IN (SELECT " + label + " FROM " + top + "), concat('0:', " + label + "), '2:') AS " + encoded + ", ")
	sql.WriteString("max(" + rowInvalid + ") AS " + rowInvalid + ", count() AS " + frequency)
	sql.WriteString(" FROM " + canonicalized + " WHERE " + rowEligible + " != 0 GROUP BY " + row + ", " + kind + ", " + encoded + "), ")

	sql.WriteString(collapsed)
	sql.WriteString(" AS (SELECT " + row + ", " + encoded + ", sum(" + frequency + ") AS " + frequency)
	sql.WriteString(" FROM " + counts + " WHERE " + kind + " IN (0, 1) GROUP BY " + row + ", " + encoded + "), ")

	// Both sentinels probe the materialized label aggregate as ordinary
	// relations. A scalar subquery would be evaluated during analysis, before
	// the materialized temporary table exists, and would re-run the whole
	// scoped scan once per occurrence.
	sql.WriteString(domainRows)
	sql.WriteString(" AS (SELECT toUInt8(0) AS sort_kind, " + splunkSeriesLabelSQL(label) + " AS " + sortLabel)
	sql.WriteString(", concat('0:', " + label + ") AS " + encoded + " FROM " + top)
	sql.WriteString(" UNION ALL SELECT toUInt8(1), CAST('' AS String), CAST('1:' AS String) FROM (SELECT 1 FROM " + labelTotals)
	sql.WriteString(" WHERE " + kind + " = 1 AND " + frequency + " > 0 LIMIT 1)")
	sql.WriteString(" UNION ALL SELECT toUInt8(2), CAST('' AS String), CAST('2:' AS String) FROM (SELECT 1 FROM " + labelTotals)
	sql.WriteString(" WHERE " + kind + " = 0 AND " + frequency + " > 0 AND " + label + " NOT IN (SELECT " + label + " FROM " + top + ") LIMIT 1)), ")

	sql.WriteString(domain)
	sql.WriteString(" AS (SELECT arrayMap(item -> item.3, arraySort(item -> (item.1, item.2), groupArray((sort_kind, " + sortLabel + ", " + encoded + ")))) AS names FROM " + domainRows + "), ")

	// Convergence after VALUE normalization is one member of the same label
	// rule as the empty, invalid-UTF-8, over-long, reserved, and row-name
	// labels, and every other member is evaluated on the column value's own
	// presence. The label aggregate carries a kind = 0 group for every ordinary
	// label any classified input row held, so reading it without the
	// row-eligible frequency filter keeps the rule presence-independent: two
	// labels that converge fail the whole command even when only row-ineligible
	// events carried them, exactly as a reserved label on such an event does.
	sql.WriteString(collisions)
	sql.WriteString(" AS (SELECT toUInt8(count() > 0) AS " + collision + " FROM (SELECT " + splunkSeriesLabelSQL(label) + " AS " + normalized)
	sql.WriteString(" FROM " + labelTotals + " WHERE " + kind + " = 0 GROUP BY " + normalized)
	sql.WriteString(" HAVING uniqExact(" + label + ") > 1 LIMIT 1)), ")

	// The atomic column-value rejection is row-independent by construction:
	// the label aggregate carries a kind = 3 group whenever any classified
	// input row held an unsupported column value, whether or not that row also
	// carried an eligible row value.
	sql.WriteString(columnCheck)
	sql.WriteString(" AS (SELECT toUInt8(maxOrDefault(" + kind + " = 3)) AS " + columnInvalid + " FROM " + labelTotals + "), ")

	sql.WriteString(rowMaps)
	sql.WriteString(" AS (SELECT " + row + ", mapFromArrays(groupArray(" + encoded + "), groupArray(" + frequency + ")) AS " + countMap)
	sql.WriteString(" FROM " + collapsed + " GROUP BY " + row + "), ")

	sql.WriteString(validation)
	sql.WriteString(" AS (SELECT toUInt8(max(" + rowInvalid + ") > 0) AS " + invalid)
	sql.WriteString(" FROM " + counts + "), ")

	// The row axis is data, so its ordinal is assigned server-side from the
	// declared order. Only the dense ordinal proves that order to the executor;
	// the row value itself crosses the boundary as an ordinary typed column.
	sql.WriteString(rowDomain)
	sql.WriteString(" AS MATERIALIZED (SELECT " + row + ", toUInt64(row_number() OVER (ORDER BY " + rowSortSQL + " ASC) - 1) AS " + ordinal)
	sql.WriteString(" FROM (SELECT " + row + " FROM " + counts + " GROUP BY " + row + ")) ")

	sql.WriteString("SELECT " + rowDomain + "." + ordinal + " AS " + ordinal + ", ")
	sql.WriteString(rowDomain + "." + row + " AS " + q(ChartRowColumn) + ", ")
	sql.WriteString(domain + ".names AS " + q(ChartNamesColumn) + ", ")
	sql.WriteString("arrayMap(name -> ifNull(" + rowMaps + "." + countMap + "[name], toUInt64(0)), " + domain + ".names) AS " + q(ChartCountsColumn) + ", ")
	sql.WriteString("toUInt8(" + validation + "." + invalid + " != 0 OR " + collisions + "." + collision + " != 0 OR ")
	sql.WriteString(columnCheck + "." + columnInvalid + " != 0) AS " + q(ChartInvalidColumn))
	sql.WriteString(" FROM " + rowDomain + " CROSS JOIN " + domain + " CROSS JOIN " + validation)
	sql.WriteString(" CROSS JOIN " + collisions + " CROSS JOIN " + columnCheck)
	sql.WriteString(" LEFT JOIN " + rowMaps + " ON " + rowMaps + "." + row + " = " + rowDomain + "." + row)
	// Deterministic, non-truncating overflow: the guard runs during filtering,
	// before the ordered result is produced, so no partial pivot is published.
	sql.WriteString(" WHERE throwIf(" + rowDomain + "." + ordinal + " >= " + strconv.FormatUint(uint64(operator.RowLimit), 10) +
		", '" + ChartRowLimitMarker + "') = 0")
	sql.WriteString(" ORDER BY " + rowDomain + "." + ordinal + " ASC")
	sql.WriteString(materializedCTESettingsSQL)

	sourceDepth := relationalNodeDepth(relation.depth)
	preparedDepth := relationalNodeDepth(sourceDepth)
	kindedDepth := relationalNodeDepth(preparedDepth)
	classifiedDepth := relationalNodeDepth(kindedDepth)
	canonicalizedDepth := relationalNodeDepth(classifiedDepth)
	labelTotalsDepth := relationalNodeDepth(canonicalizedDepth)
	topDepth := relationalNodeDepth(labelTotalsDepth)
	topMembershipDepth := relationalNodeDepth(topDepth)
	countsDepth := relationalNodeDepth(canonicalizedDepth, topMembershipDepth)
	collapsedDepth := relationalNodeDepth(countsDepth)

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
	resultDepth := relationalNodeDepth(
		rowDomainDepth,
		domainDepth,
		validationDepth,
		collisionsDepth,
		columnCheckDepth,
		rowMapsDepth,
	)

	compiled := CompiledQuery{
		SQL:          sql.String(),
		Args:         args,
		OutputFields: slices.Clone(dynamic.FixedFields),
		Chart: &ChartOutput{
			RowField:        rowName,
			RowKind:         rowKind,
			RowDatabaseType: rowDatabaseType,
			RowLimit:        uint64(operator.RowLimit),
			MaxSeries:       dynamic.MaxSeries,
			MaxLabelBytes:   maxTimechartLabelBytes,
		},
	}
	return withCompiledRelationalDepth(compiled, resultDepth, operator.Range), nil
}

type compileState struct {
	visible                          map[string]fieldState
	matchBudget                      *compiledMatchBudget
	likeBudget                       *compiledLikeBudget
	publicOrder                      []string
	privateColumns                   []string
	rexCapturedBytesSQL              string
	allowDynamic                     bool
	eventRows                        bool
	blocked                          map[string]struct{}
	blockedPrefixes                  map[string]struct{}
	order                            []compiledSortKey
	tieBreakers                      []compiledSortKey
	preAggregateValidationColumns    []string
	preAggregateValidationArgs       []any
	preAggregateColumns              []string
	preAggregateArgs                 []any
	preAggregateListWindowColumns    []string
	preAggregateListCandidateColumns []string
	postAggregateChronological       []compiledChronologicalMeasure
	postAggregateScalarExtrema       []compiledScalarExtremaMeasure
	postAggregateExactStrings        []compiledExactStringMeasure
	postAggregateDistinctCounts      []compiledDistinctCount
	postAggregateOrderedStrings      []compiledOrderedStringMeasure
	chronologicalBarriers            []compiledChronologicalBarrier
}

type compiledMatchBudget struct {
	patterns         map[*plan.ScalarCallExpression]splregex.MatchPattern
	programWorkUnits int
}

type compiledLikeBudget struct {
	patterns  map[*plan.ScalarCallExpression]splwildcard.LikePattern
	workUnits int
}

type compiledChronologicalMeasure struct {
	winnerColumn     string
	validationColumn string
	typeColumn       string
	outputColumn     string
}

// compiledChronologicalBarrier owns one materialized aggregate result, its
// bind arguments, and the hidden columns checked by the final validation
// envelope. Keeping the check outside every downstream SPL operator prevents
// ClickHouse from proving an intervening filter empty and pruning the
// validation subtree.
type compiledChronologicalBarrier struct {
	name              string
	sql               string
	args              []any
	validationColumns []string
	depth             int
}

type compiledScalarExtremaMeasure struct {
	keyColumn    string
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
)

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
	valueSQL                string
	maxStringBytes          uint64
	textEligibleSQL         string
	dynamicDomain           dynamicScalarDomain
	numericIntegral         bool
	mvCountOneOrNull        bool
	dynamicTypeSQL          string
	storedTypeSQL           string
	existsSQL               string
	existsArgs              []any
	descendantSQL           string
	descendantArgs          []any
	kind                    fieldKind
	caseSensitive           bool
	numberType              string
	numericSort             bool
	canonicalTime           bool
	alwaysNull              bool
	materializeForPredicate bool
}

type compiledSortKey struct {
	valueSQL   string
	descending bool
	nullsFirst bool
}

func compileScan(database, table string, scan *plan.Scan) (string, compileState, []any, error) {
	if scan.TenantID == "" || len(scan.Indexes) == 0 || scan.Earliest.IsZero() || scan.Latest.IsZero() || !scan.Earliest.Before(scan.Latest) || scan.IndexTimeCutoff.IsZero() {
		return "", compileState{}, nil, errors.New("compile ClickHouse query: Scan has an invalid security or time scope")
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
	args = append(args, scan.TenantID)
	for _, index := range scan.Indexes {
		args = append(args, index)
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
		state.canonicalTime = true
	}
	if field == "_raw" {
		state.textEligibleSQL = quoteIdentifier(internalRawEncodingColumn) + " = " +
			strconv.Itoa(rawEncodingUTF8)
	}
	return state
}

func compileExpression(expression plan.Expression, state compileState) (string, []any, error) {
	switch expression := expression.(type) {
	case *plan.BooleanExpression:
		left, leftArgs, err := compileExpression(expression.Left, state)
		if err != nil {
			return "", nil, err
		}
		right, rightArgs, err := compileExpression(expression.Right, state)
		if err != nil {
			return "", nil, err
		}
		operator := "AND"
		if expression.Op == plan.BooleanOpOr {
			operator = "OR"
		}
		return "(" + left + " " + operator + " " + right + ")", append(leftArgs, rightArgs...), nil
	case *plan.NotExpression:
		operand, args, err := compileExpression(expression.Operand, state)
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
			return "match(toString(" + raw.valueSQL + "), ?)", []any{freeTextRegex(expression.Value, false)}, nil
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
		case *plan.ScalarPredicateExpression:
			if expression != nil {
				visitScalar(expression.Value)
			}
		}
	}
	visit(expression)
	return fields
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
	valueSQL                string
	valueArgs               []any
	maxStringBytes          uint64
	existsSQL               string
	existsArgs              []any
	textEligibleSQL         string
	dynamicDomain           dynamicScalarDomain
	numericIntegral         bool
	mvCountOneOrNull        bool
	dynamicTypeSQL          string
	storedTypeSQL           string
	descendantSQL           string
	descendantArgs          []any
	kind                    fieldKind
	numberType              string
	literal                 *plan.Value
	alwaysNull              bool
	comparisonAtomic        bool
	materializeForPredicate bool
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
	// Durable event values and every fixed source column fit within the
	// ingestion hard limit. Zero is reserved as "source-bounded", not
	// "unbounded", so existing non-string scalar metadata remains compact.
	return MaximumStoredScalarBytes
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

func compileScalarValue(expression plan.ScalarExpression, state compileState) (compiledScalar, error) {
	switch expression := expression.(type) {
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
		case plan.ScalarFunctionReplace:
			return compileReplaceScalar(expression, state)
		case plan.ScalarFunctionToNumber:
			return compileToNumberScalar(expression, state)
		case plan.ScalarFunctionToString:
			return compileToStringScalar(expression, state)
		case plan.ScalarFunctionRound:
			return compileRoundScalar(expression, state)
		case plan.ScalarFunctionCeil:
			return compileIntegralRoundingScalar(expression, state, "ceil")
		case plan.ScalarFunctionFloor:
			return compileIntegralRoundingScalar(expression, state, "floor")
		case plan.ScalarFunctionMVCount:
			return compileMVCountScalar(expression, state)
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
		valueSQL:                "coalesce(" + strings.Join(valueSQL, ", ") + ")",
		valueArgs:               args,
		maxStringBytes:          maximumCompiledScalarStringByteBound(values...),
		existsSQL:               "1",
		textEligibleSQL:         textEligibleSQL,
		kind:                    kind,
		numberType:              numberType,
		alwaysNull:              alwaysNull,
		materializeForPredicate: materializeForPredicate,
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

func normalizeCoalesceValues(
	values []compiledScalar,
	sourceRange spl.Range,
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
				unsupportedCoalesceValueTypes(sourceRange, target, value)
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
			unsupportedCoalesceValueTypes(sourceRange, target, compiledScalar{})
	}

	normalized := append([]compiledScalar(nil), values...)
	for index, value := range normalized {
		if !compiledScalarIsAlwaysNull(value) {
			continue
		}
		if coalesceFixedTypesMatch(value, target) {
			// Keep a typed null-producing expression intact. Its result cannot
			// win selection, but preserving it retains source-order bindings
			// and avoids inventing an evaluation-elision contract.
			continue
		}
		typed, ok := typedNullIfBranchFor(target)
		if !ok {
			return nil, fieldKindInvalid, "",
				unsupportedCoalesceValueTypes(sourceRange, target, value)
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
		valueSQL:                valueSQL,
		valueArgs:               args,
		maxStringBytes:          maximumCompiledScalarStringByteBound(values...),
		existsSQL:               "1",
		textEligibleSQL:         textEligibleSQL,
		kind:                    kind,
		numberType:              numberType,
		alwaysNull:              alwaysNull,
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
				unsupportedCaseValueTypes(sourceRange, target, value)
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
			unsupportedCaseValueTypes(sourceRange, target, compiledScalar{})
	}

	normalized := append([]compiledScalar(nil), values...)
	for index, value := range normalized {
		if !compiledScalarIsAlwaysNull(value) {
			continue
		}
		if coalesceFixedTypesMatch(value, target) {
			continue
		}
		typed, ok := typedNullIfBranchFor(target)
		if !ok {
			return nil, fieldKindInvalid, "",
				unsupportedCaseValueTypes(sourceRange, target, value)
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
		existsSQL:       "1",
		textEligibleSQL: textEligibleSQL,
		kind:            kind,
		numberType:      numberType,
		alwaysNull:      alwaysNull,
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
	nodes  int
	active map[any]struct{}
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
		switch expression.Function {
		case plan.ScalarFunctionToNumber,
			plan.ScalarFunctionToString,
			plan.ScalarFunctionIsNull,
			plan.ScalarFunctionIsNotNull,
			plan.ScalarFunctionLower,
			plan.ScalarFunctionUpper,
			plan.ScalarFunctionLength,
			plan.ScalarFunctionCeil,
			plan.ScalarFunctionFloor,
			plan.ScalarFunctionMVCount:
			expectedArguments = 1
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
			_, ok := scalarStringLiteral(expression.Arguments[1])
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
			_, ok := scalarStringLiteral(expression.Arguments[1])
			if !ok {
				return errors.New(
					"compile ClickHouse predicate: like pattern must be a string literal",
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
		default:
			return fmt.Errorf(
				"compile ClickHouse predicate: unsupported scalar function %d",
				expression.Function,
			)
		}
		if expectedArguments != 0 && len(expression.Arguments) != expectedArguments {
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
	if input.kind == fieldKindStringArray {
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
		valueSQL:                "replaceRegexpAll(" + inputSQL + ", ?, ?)",
		valueArgs:               append(inputArgs, pattern, replacement),
		maxStringBytes:          saturatingStringByteProduct(compiledScalarStringByteBound(input), replacementFactor),
		existsSQL:               "1",
		textEligibleSQL:         input.textEligibleSQL,
		kind:                    fieldKindString,
		materializeForPredicate: input.materializeForPredicate,
	}, nil
}

func compileMatchScalar(
	expression *plan.ScalarCallExpression,
	state compileState,
) (compiledScalar, error) {
	if len(expression.Arguments) != 2 {
		return compiledScalar{}, errors.New("compile ClickHouse match: expected two arguments")
	}
	if scalarExpressionMayReturnBooleanFunction(expression.Arguments[0]) {
		return compiledScalar{}, booleanScalarConsumerError("match")
	}
	input, err := compileScalarValue(expression.Arguments[0], state)
	if err != nil {
		return compiledScalar{}, err
	}
	if input.kind == fieldKindStringArray {
		return compiledScalar{}, unsupportedMultivalueUsage("match", expression.Range)
	}
	pattern, ok := scalarStringLiteral(expression.Arguments[1])
	if !ok {
		return compiledScalar{}, errors.New(
			"compile ClickHouse match: regular expression must be a string literal",
		)
	}
	compiledPattern := splregex.MatchPattern{}
	if state.matchBudget != nil {
		compiledPattern = state.matchBudget.patterns[expression]
	}
	if compiledPattern.ProgramWorkUnits == 0 {
		compiledPattern, err = compileMatchPatternForBackend(pattern, expression.Range)
		if err != nil {
			return compiledScalar{}, err
		}
		if state.matchBudget != nil {
			state.matchBudget.patterns[expression] = compiledPattern
		}
	}
	if state.matchBudget != nil {
		if compiledPattern.ProgramWorkUnits >
			splregex.MaximumMatchQueryProgramWorkUnits-state.matchBudget.programWorkUnits {
			return compiledScalar{}, &plan.Diagnostic{
				Code: "SPL_QUERY_TOO_COMPLEX",
				Message: fmt.Sprintf(
					"search match programs require more than %d work units",
					splregex.MaximumMatchQueryProgramWorkUnits,
				),
				Range: expression.Range,
			}
		}
		state.matchBudget.programWorkUnits += compiledPattern.ProgramWorkUnits
	}
	if input.alwaysNull || input.kind == fieldKindInvalid {
		return compiledScalar{
			valueSQL:       "CAST(NULL AS Nullable(Bool))",
			maxStringBytes: 1,
			existsSQL:      "1",
			kind:           fieldKindBool,
			alwaysNull:     true,
		}, nil
	}
	if maximumInputBytes := compiledScalarStringByteBound(input); maximumInputBytes > MaximumMatchInputBytes {
		return compiledScalar{}, &plan.Diagnostic{
			Code: "SPL_QUERY_TOO_COMPLEX",
			Message: fmt.Sprintf(
				"match input may exceed %d bytes after scalar evaluation",
				MaximumMatchInputBytes,
			),
			Range: expression.Range,
		}
	}
	inputSQL, inputArgs := compiledTextEligibleStringScalar(input)
	valueSQL := "CAST(match(" + inputSQL + ", ?) AS Nullable(Bool))"
	if len(valueSQL) > maxCompiledMatchScalarSQLBytes {
		return compiledScalar{}, &plan.Diagnostic{
			Code: "SPL_QUERY_TOO_COMPLEX",
			Message: fmt.Sprintf(
				"match scalar SQL exceeds %d bytes",
				maxCompiledMatchScalarSQLBytes,
			),
			Range: expression.Range,
		}
	}
	return compiledScalar{
		valueSQL:                valueSQL,
		valueArgs:               append(inputArgs, compiledPattern.Pattern),
		maxStringBytes:          5,
		existsSQL:               "1",
		kind:                    fieldKindBool,
		materializeForPredicate: input.materializeForPredicate,
	}, nil
}

func compileLikeScalar(
	expression *plan.ScalarCallExpression,
	state compileState,
) (compiledScalar, error) {
	if len(expression.Arguments) != 2 {
		return compiledScalar{}, errors.New("compile ClickHouse like: expected two arguments")
	}
	if scalarExpressionMayReturnBooleanFunction(expression.Arguments[0]) {
		return compiledScalar{}, booleanScalarConsumerError("like")
	}
	input, err := compileScalarValue(expression.Arguments[0], state)
	if err != nil {
		return compiledScalar{}, err
	}
	if input.kind == fieldKindStringArray {
		return compiledScalar{}, unsupportedMultivalueUsage("like", expression.Range)
	}
	pattern, ok := scalarStringLiteral(expression.Arguments[1])
	if !ok {
		return compiledScalar{}, errors.New(
			"compile ClickHouse like: pattern must be a string literal",
		)
	}
	compiledPattern := splwildcard.LikePattern{}
	if state.likeBudget != nil {
		compiledPattern = state.likeBudget.patterns[expression]
	}
	if compiledPattern.WorkUnits == 0 {
		compiledPattern, err = compileLikePatternForBackend(pattern, expression.Range)
		if err != nil {
			return compiledScalar{}, err
		}
		if state.likeBudget != nil {
			state.likeBudget.patterns[expression] = compiledPattern
		}
	}
	if state.likeBudget != nil {
		if compiledPattern.WorkUnits >
			splwildcard.MaximumLikeQueryPatternWorkUnits-state.likeBudget.workUnits {
			return compiledScalar{}, &plan.Diagnostic{
				Code: "SPL_QUERY_TOO_COMPLEX",
				Message: fmt.Sprintf(
					"search like patterns require more than %d work units",
					splwildcard.MaximumLikeQueryPatternWorkUnits,
				),
				Range: expression.Range,
			}
		}
		state.likeBudget.workUnits += compiledPattern.WorkUnits
	}
	if input.alwaysNull || input.kind == fieldKindInvalid {
		return compiledScalar{
			valueSQL:       "CAST(NULL AS Nullable(Bool))",
			maxStringBytes: 1,
			existsSQL:      "1",
			kind:           fieldKindBool,
			alwaysNull:     true,
		}, nil
	}
	if maximumInputBytes := compiledScalarStringByteBound(input); maximumInputBytes > MaximumLikeInputBytes {
		return compiledScalar{}, &plan.Diagnostic{
			Code: "SPL_QUERY_TOO_COMPLEX",
			Message: fmt.Sprintf(
				"like input may exceed %d bytes after scalar evaluation",
				MaximumLikeInputBytes,
			),
			Range: expression.Range,
		}
	}
	inputSQL, inputArgs := compiledTextEligibleStringScalar(input)
	valueSQL := "CAST(like(" + inputSQL + ", ?) AS Nullable(Bool))"
	if len(valueSQL) > maxCompiledLikeScalarSQLBytes {
		return compiledScalar{}, &plan.Diagnostic{
			Code: "SPL_QUERY_TOO_COMPLEX",
			Message: fmt.Sprintf(
				"like scalar SQL exceeds %d bytes",
				maxCompiledLikeScalarSQLBytes,
			),
			Range: expression.Range,
		}
	}
	return compiledScalar{
		valueSQL:                valueSQL,
		valueArgs:               append(inputArgs, compiledPattern.Pattern),
		maxStringBytes:          5,
		existsSQL:               "1",
		kind:                    fieldKindBool,
		materializeForPredicate: input.materializeForPredicate,
	}, nil
}

func compileTextCaseScalar(
	expression *plan.ScalarCallExpression,
	state compileState,
) (compiledScalar, error) {
	functionName := "lower"
	clickHouseFunction := "lowerUTF8"
	if expression.Function == plan.ScalarFunctionUpper {
		functionName = "upper"
		clickHouseFunction = "upperUTF8"
	}
	input, err := compileUnaryNonBooleanScalarInput(expression, state, functionName)
	if err != nil {
		return compiledScalar{}, err
	}

	valueSQL := ""
	valueArgs := append([]any(nil), input.valueArgs...)
	resultKind := fieldKindString
	dynamicDomain := dynamicScalarDomainAny
	switch input.kind {
	case fieldKindDynamic:
		// Dynamic event fields can be either scalar String or Splunk
		// multivalue Array(String). Bind the input once through a single-element
		// higher-order expression so nested calls grow linearly instead of
		// duplicating the complete child SQL in each runtime-type branch.
		valueSQL = "arrayElement(arrayMap(value -> multiIf(" +
			"dynamicType(value) = 'String', " +
			"CAST(" + clickHouseFunction +
			"(dynamicElement(value, 'String')) AS Dynamic), " +
			"dynamicType(value) = 'Array(String)', " +
			"CAST(arrayMap(element -> " + clickHouseFunction +
			"(element), dynamicElement(value, 'Array(String)')) AS Dynamic), " +
			"dynamicType(value) = 'Array(Dynamic)' AND " +
			"arrayAll(element -> dynamicType(element) = 'String', " +
			"dynamicElement(value, 'Array(Dynamic)')), " +
			"CAST(arrayMap(element -> " + clickHouseFunction +
			"(assumeNotNull(dynamicElement(element, 'String'))), " +
			"dynamicElement(value, 'Array(Dynamic)')) AS Dynamic), " +
			"CAST(NULL AS Dynamic)), [" + input.valueSQL + "]), 1)"
		resultKind = fieldKindDynamic
		dynamicDomain = dynamicScalarDomainText
	case fieldKindStringArray:
		// A fixed Array(String) can originate from an aggregate over _raw.
		// Bind it once and validate every member before calling the UTF-8
		// function; invalid arrays become the canonical empty/absent MV value.
		valueSQL = "arrayElement(arrayMap(values -> if(" +
			"arrayAll(element -> isValidUTF8(element), values), " +
			"arrayMap(element -> " + clickHouseFunction +
			"(element), values), CAST([], 'Array(String)')), [" +
			input.valueSQL + "]), 1)"
		resultKind = fieldKindStringArray
	case fieldKindString, fieldKindInvalid:
		inputSQL, inputArgs := compiledTextEligibleStringScalar(input)
		valueArgs = inputArgs
		valueSQL = clickHouseFunction + "(" + inputSQL + ")"
	default:
		return compiledScalar{}, &plan.Diagnostic{
			Code: "SPL_UNSUPPORTED_TEXT_CASE_VALUE_TYPE",
			Message: fmt.Sprintf(
				"%s requires a String or multivalue String input",
				functionName,
			),
			Range: expression.Range,
		}
	}
	if len(valueSQL) > maxCompiledTextCaseScalarSQLBytes {
		return compiledScalar{}, &plan.Diagnostic{
			Code: "SPL_QUERY_TOO_COMPLEX",
			Message: fmt.Sprintf(
				"%s scalar SQL exceeds %d bytes",
				functionName,
				maxCompiledTextCaseScalarSQLBytes,
			),
			Range: expression.Range,
		}
	}
	return compiledScalar{
		valueSQL:                valueSQL,
		valueArgs:               valueArgs,
		maxStringBytes:          saturatingStringByteProduct(compiledScalarStringByteBound(input), 4),
		existsSQL:               "1",
		dynamicDomain:           dynamicDomain,
		kind:                    resultKind,
		alwaysNull:              input.alwaysNull,
		materializeForPredicate: input.materializeForPredicate,
	}, nil
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
	case fieldKindStringArray:
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
	case fieldKindStringArray:
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

func compileInt128SubstringInteger(value plan.Value) (string, []any) {
	if value.Kind == plan.ValueKindInt64 {
		return "CAST(? AS Int128)", []any{value.Int64}
	}
	return "CAST(? AS Int128)", []any{value.Uint64}
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
	// #nosec G115 -- the explicit sign guard proves the Int64 is representable.
	return uint64(value.Int64)
}

func negativeSubstringIntegerMagnitude(value plan.Value) uint64 {
	if value.Int64 >= 0 {
		return 0
	}
	// Subtract before negating so MinInt64 stays representable in its signed
	// domain. The result is non-negative and therefore fits UInt64.
	magnitudeMinusOne := -(value.Int64 + 1)
	// #nosec G115 -- magnitudeMinusOne is non-negative by the guard above.
	return uint64(magnitudeMinusOne) + 1
}

func compileGenericSQLiteSubstringUTF8SQL(
	inputSQL string,
	start plan.Value,
	length *plan.Value,
) (string, []any) {
	startSQL, startArgs := compileInt128SubstringInteger(start)
	outerParameters := "value, start"
	outerArguments := "[" + inputSQL + "], [" + startSQL + "]"

	positionSQL := "if(start < 0, n + start + 1, start)"
	beginSQL := positionSQL
	endSQL := "n + 1"
	indexArgs := startArgs
	if length != nil {
		lengthSQL, lengthArgs := compileInt128SubstringInteger(*length)
		outerParameters += ", span"
		outerArguments += ", [" + lengthSQL + "]"
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
	input, err := compileUnaryScalarInput(expression, state, "tostring")
	if err != nil {
		return compiledScalar{}, err
	}

	valueSQL := ""
	valueArgs := append([]any(nil), input.valueArgs...)
	textEligibleSQL := ""
	switch input.kind {
	case fieldKindDynamic:
		if input.dynamicDomain == dynamicScalarDomainText {
			// Text-case producers can only contain String, String arrays, or
			// null. A direct extraction avoids a redundant singleton-array
			// binding and runtime dispatch while rejecting multivalue variants.
			valueSQL = "dynamicElement(" + input.valueSQL + ", 'String')"
		} else {
			typeSQL := "dynamicType(value)"
			dynamicValue := compiledScalar{
				valueSQL:       "value",
				dynamicTypeSQL: typeSQL,
				kind:           fieldKindDynamic,
			}
			decimalCondition, decimalPayload := dynamicTaggedDecimalText(
				dynamicValue,
			)
			valueSQL = "arrayElement(arrayMap(value -> multiIf(" +
				typeSQL + " = 'String', dynamicElement(value, 'String'), " +
				typeSQL + " = 'Bool', if(dynamicElement(value, 'Bool'), " +
				"CAST('True' AS String), CAST('False' AS String)), " +
				decimalCondition + ", " + decimalPayload + ", " +
				dynamicNumericTypePredicate(typeSQL) + ", toString(value), " +
				"CAST(NULL AS Nullable(String))), [" + input.valueSQL + "]), 1)"
		}
	case fieldKindString:
		valueSQL = input.valueSQL
		textEligibleSQL = input.textEligibleSQL
	case fieldKindNumber:
		valueSQL = "toString(" + input.valueSQL + ")"
	case fieldKindBool:
		// transform preserves nullable Boolean null while evaluating its input
		// once, without allocating a singleton array per row.
		valueSQL = "transform(" + input.valueSQL + ", [true, false], " +
			"['True', 'False'], CAST(NULL AS Nullable(String)))"
	case fieldKindInvalid:
		valueSQL = "CAST(NULL AS Nullable(String))"
	case fieldKindStringArray:
		return compiledScalar{}, unsupportedMultivalueUsage(
			"tostring",
			expression.Range,
		)
	default:
		return compiledScalar{}, &plan.Diagnostic{
			Code: "SPL_UNSUPPORTED_TOSTRING_VALUE_TYPE",
			Message: "tostring supports scalar String, number, and Boolean " +
				"input in compatibility version 0.1",
			Range: expression.Range,
		}
	}
	if len(valueSQL) > maxCompiledToStringScalarSQLBytes {
		return compiledScalar{}, &plan.Diagnostic{
			Code: "SPL_QUERY_TOO_COMPLEX",
			Message: fmt.Sprintf(
				"tostring scalar SQL exceeds %d bytes",
				maxCompiledToStringScalarSQLBytes,
			),
			Range: expression.Range,
		}
	}
	return compiledScalar{
		valueSQL:                valueSQL,
		valueArgs:               valueArgs,
		maxStringBytes:          compiledScalarStringByteBound(input),
		existsSQL:               "1",
		textEligibleSQL:         textEligibleSQL,
		kind:                    fieldKindString,
		alwaysNull:              input.alwaysNull,
		materializeForPredicate: input.materializeForPredicate,
	}, nil
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
	case fieldKindStringArray:
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
		materializeForPredicate: input.materializeForPredicate,
	}, nil
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
		// #nosec G115 -- the preceding range check caps the value at 18.
		return uint8(value.Int64), nil
	case plan.ValueKindUint64:
		if value.Uint64 > spl.MaximumRoundPrecision {
			return 0, fmt.Errorf(
				"compile ClickHouse round: precision must be from 0 through %d",
				spl.MaximumRoundPrecision,
			)
		}
		// #nosec G115 -- the preceding range check caps the value at 18.
		return uint8(value.Uint64), nil
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
	if input.kind == fieldKindStringArray {
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

func scalarExpressionMayReturnBooleanFunction(expression plan.ScalarExpression) bool {
	switch expression := expression.(type) {
	case *plan.ScalarCallExpression:
		if expression == nil {
			return false
		}
		if expression.Function.ReturnsBoolean() {
			return true
		}
		if expression.Function == plan.ScalarFunctionCoalesce {
			for _, argument := range expression.Arguments {
				if scalarExpressionMayReturnBooleanFunction(argument) {
					return true
				}
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

func extendCompileState(state compileState, output plan.FieldRef, value compiledScalar) compileState {
	next := state
	next.visible = make(map[string]fieldState, len(state.visible)+1)
	for name, field := range state.visible {
		next.visible[name] = field
	}
	next.publicOrder = append([]string(nil), state.publicOrder...)
	next.blocked = cloneSet(state.blocked)
	next.blockedPrefixes = cloneSet(state.blockedPrefixes)
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
	existsSQL := "1"
	if value.kind == fieldKindStringArray {
		// A values() result is physically a non-null array, but SPL treats an
		// empty multivalue result as absent. Rebind that logical presence check
		// to the eval output because the source expression lives in the nested
		// SELECT and is no longer visible at this stage.
		existsSQL = "notEmpty(" + quoteIdentifier(output.Name) + ")"
	}
	field := fieldState{
		valueSQL:         quoteIdentifier(output.Name),
		maxStringBytes:   value.maxStringBytes,
		textEligibleSQL:  value.textEligibleSQL,
		dynamicDomain:    value.dynamicDomain,
		numericIntegral:  value.numericIntegral,
		mvCountOneOrNull: value.mvCountOneOrNull,
		existsSQL:        existsSQL,
		descendantSQL:    value.descendantSQL,
		descendantArgs:   append([]any(nil), value.descendantArgs...),
		storedTypeSQL:    value.storedTypeSQL,
		kind:             value.kind,
		// An eval output named index is calculated data, not the physical scan
		// selector. It follows its expression type and ordinary comparison rules.
		caseSensitive:           false,
		numberType:              value.numberType,
		alwaysNull:              value.alwaysNull,
		materializeForPredicate: value.materializeForPredicate,
	}
	if value.kind == fieldKindDynamic {
		field.dynamicTypeSQL = "dynamicType(" + field.valueSQL + ")"
	}
	next.visible[output.Name] = field
	next.privateColumns = livePrivateColumns(next.privateColumns, next.visible)
	return next
}

type compiledExtractCapture struct {
	planCapture          plan.ExtractCapture
	valueSQL             string
	existsColumn         string
	existsProjection     string
	typeColumn           string
	typeProjection       string
	textColumn           string
	textProjection       string
	descendantColumn     string
	descendantProjection string
}

func extractPrivateColumns(captures []compiledExtractCapture) []string {
	columns := make([]string, 0, len(captures)*4)
	for _, capture := range captures {
		columns = append(columns, capture.existsColumn, capture.typeColumn)
		if capture.textColumn != "" {
			columns = append(columns, capture.textColumn)
		}
		if capture.descendantColumn != "" {
			columns = append(columns, capture.descendantColumn)
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
		descendantColumn := ""
		descendantProjection := ""
		if previousKnown && previous.descendantSQL != "" {
			descendantColumn = quoteIdentifier(fmt.Sprintf("__os_rex_descendant_%d_%d", stage, index))
			descendantProjection = "toUInt8(" + matchedAlias + " = 0 AND (" +
				previous.descendantSQL + ")) AS " + descendantColumn
			descendantArgs = append(descendantArgs, previous.descendantArgs...)
		}
		captures = append(captures, compiledExtractCapture{
			planCapture:          capture,
			valueSQL:             valueSQL,
			existsColumn:         existsAlias,
			existsProjection:     existsSQL,
			typeColumn:           typeAlias,
			typeProjection:       typeSQL,
			textColumn:           textColumn,
			textProjection:       textProjection,
			descendantColumn:     descendantColumn,
			descendantProjection: descendantProjection,
		})

		delete(next.blocked, capture.Output.Name)
		if !slices.Contains(next.publicOrder, capture.Output.Name) {
			next.publicOrder = append(next.publicOrder, capture.Output.Name)
		}
		output := quoteIdentifier(capture.Output.Name)
		field := fieldState{
			valueSQL:        output,
			textEligibleSQL: textColumn,
			existsSQL:       existsAlias,
			storedTypeSQL:   typeAlias,
			descendantSQL:   descendantColumn,
			kind:            kind,
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
	descendantExpressions := make([]string, 0, len(captures))
	for _, capture := range captures {
		valueByName[capture.planCapture.Output.Name] = capture.valueSQL
		existenceExpressions = append(existenceExpressions, capture.existsProjection)
		typeExpressions = append(typeExpressions, capture.typeProjection)
		if capture.textProjection != "" {
			textExpressions = append(textExpressions, capture.textProjection)
		}
		if capture.descendantProjection != "" {
			descendantExpressions = append(descendantExpressions, capture.descendantProjection)
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
		if field.valueSQL == publicName {
			projection = append(projection, publicName)
		} else {
			projection = append(projection, field.valueSQL+" AS "+publicName)
		}
	}
	projectionState := next
	projectionState.privateColumns = liveOldPrivateColumns
	projection = appendPrivateEventProjection(projection, projectionState)
	projection = append(projection, existenceExpressions...)
	projection = append(projection, typeExpressions...)
	projection = append(projection, textExpressions...)
	projection = append(projection, descendantExpressions...)
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
	case fieldKindStringArray:
		return "", "", nil, unsupportedMultivalueUsage("field extraction", input.Range)
	default:
		// v0.1 does not stringify numeric, Boolean, time, multivalue, or
		// container sources. They behave exactly like a non-match.
		return "CAST(NULL AS Nullable(String))", "0", nil, nil
	}
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
	pathEligibleAlias := quoteIdentifier(fmt.Sprintf("__os_spath_path_eligible_%d", stage))
	jsonTypeAlias := quoteIdentifier(fmt.Sprintf("__os_spath_json_type_%d", stage))
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
	arrayGuardSQL, arrayGuardArgs := spathArrayGuardSQL(inputAlias, steps)
	pathEligible := eligibleAlias + " != 0"
	if arrayGuardSQL != "" {
		pathEligible += " AND " + arrayGuardSQL
	}
	pathEligibleExpression := "toUInt8(" + pathEligible + ") AS " + pathEligibleAlias
	typePathSQL, typePathArgs := spathPathSQL(steps)
	jsonTypeExpression := "if(" + pathEligibleAlias + " != 0, toString(JSONType(" +
		inputAlias + ", " + typePathSQL + ")), CAST('' AS String)) AS " + jsonTypeAlias
	rawPathSQL, rawPathArgs := spathPathSQL(steps)
	rawExpression := "if(" + pathEligibleAlias + " != 0, JSONExtractRaw(" + inputAlias +
		", " + rawPathSQL + "), CAST('' AS String)) AS " + rawAlias
	rawFragment := "SELECT *, " + boundedEligible + ", " + pathEligibleExpression +
		", " + jsonTypeExpression + ", " + rawExpression +
		" FROM (" + relation.sql + ") AS " + quoteIdentifier(fmt.Sprintf("_spath_source_%d", stage))
	relation = relation.selectFrom(rawFragment, operator.Range)

	supportedType := jsonTypeAlias + " IN ('Null', 'String', 'Bool', 'Int64', 'UInt64')"
	unsupportedRaw := "notEmpty(" + rawAlias + ") AND NOT (" + supportedType + ")"
	matchedExpression := "toUInt8(if(" + unsupportedRaw + ", throwIf(toUInt8(" +
		unsupportedRaw + "), '" + UnsupportedSpathValueMarker + "') = 0, notEmpty(" +
		rawAlias + "))) AS " + matchedAlias
	valueExpression := "if(" + matchedAlias + " != 0, JSONExtract(" + rawAlias +
		", 'Dynamic'), CAST(NULL AS Dynamic)) AS " + valueAlias
	valueFragment := "SELECT *, " + matchedExpression + ", " + valueExpression +
		" FROM (" + relation.sql + ") AS " + quoteIdentifier(fmt.Sprintf("_spath_raw_%d", stage))
	relation = relation.selectFrom(valueFragment, operator.Range)

	previous, previousKnown, err := resolveCompiledField(operator.Output, state)
	if err != nil {
		return compiledRelation{}, compileState{}, nil, 0, err
	}
	previousValue := "CAST(NULL AS Dynamic)"
	previousExists := "0"
	var existenceArgs, typeArgs []any
	previousTypeSQL := "toUInt8(0)"
	if previousKnown {
		previousValue = "CAST(" + previous.valueSQL + " AS Dynamic)"
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
		dynamicType + " = 'Bool', toUInt8(" + strconv.Itoa(int(eventfields.StoredValueTypeBool)) + "), " +
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
	outputValue := "if(" + matchedAlias + " != 0, " + valueAlias + ", " + previousValue + ")"

	next := cloneCompileState(state)
	if sourceMayExtract && exposesRawFieldsPayload(state) {
		dropRawFieldsPayload(&next)
	}
	delete(next.blocked, operator.Output.Name)
	if !slices.Contains(next.publicOrder, operator.Output.Name) {
		next.publicOrder = append(next.publicOrder, operator.Output.Name)
	}
	outputName := quoteIdentifier(operator.Output.Name)
	next.visible[operator.Output.Name] = fieldState{
		valueSQL:                outputName,
		textEligibleSQL:         textEligibleAlias,
		dynamicTypeSQL:          "dynamicType(" + outputName + ")",
		storedTypeSQL:           typeAlias,
		existsSQL:               existsAlias,
		descendantSQL:           descendantAlias,
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
		if field.valueSQL == publicName {
			projection = append(projection, publicName)
		} else {
			projection = append(projection, field.valueSQL+" AS "+publicName)
		}
	}
	projectionState := next
	projectionState.privateColumns = liveOldPrivateColumns
	projection = appendPrivateEventProjection(projection, projectionState)
	projection = append(projection, existsProjection, typeProjection)
	if textEligibleProjection != "" {
		projection = append(projection, textEligibleProjection)
	}
	if descendantProjection != "" {
		projection = append(projection, descendantProjection)
	}
	outputFragment := "SELECT " + strings.Join(projection, ", ") + " FROM (" +
		relation.sql + ") AS " + quoteIdentifier(fmt.Sprintf("_stage_%d", stage+1))
	relation = relation.selectFrom(outputFragment, operator.Range)

	prefixArgs := make([]any, 0,
		len(existenceArgs)+len(typeArgs)+len(descendantArgs)+len(arrayGuardArgs)+
			len(typePathArgs)+len(rawPathArgs)+len(inputArgs),
	)
	prefixArgs = append(prefixArgs, existenceArgs...)
	prefixArgs = append(prefixArgs, typeArgs...)
	prefixArgs = append(prefixArgs, descendantArgs...)
	prefixArgs = append(prefixArgs, arrayGuardArgs...)
	prefixArgs = append(prefixArgs, typePathArgs...)
	prefixArgs = append(prefixArgs, rawPathArgs...)
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
		if step.HasIndex {
			placeholders = append(placeholders, "?")
			// #nosec G115 -- splpath parsing caps array indices at 2^31-2.
			args = append(args, int64(step.Index)+1)
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
		if step.HasIndex {
			guards = append(guards, "toString(JSONType("+inputSQL+", "+
				strings.Join(pathSQL, ", ")+")) = 'Array'")
			args = append(args, pathArgs...)
			pathSQL = append(pathSQL, "?")
			// #nosec G115 -- splpath parsing caps array indices at 2^31-2.
			pathArgs = append(pathArgs, int64(step.Index)+1)
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
	next.chronologicalBarriers = append(
		[]compiledChronologicalBarrier(nil),
		state.chronologicalBarriers...,
	)
	return next
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
	result := fieldState{
		valueSQL:        value,
		maxStringBytes:  source.maxStringBytes,
		textEligibleSQL: source.textEligibleSQL,
		dynamicDomain:   source.dynamicDomain,
		numericIntegral: source.numericIntegral,
		storedTypeSQL:   source.storedTypeSQL,
		existsSQL:       rewriteExistenceForProjection(source, destination),
		existsArgs:      append([]any(nil), source.existsArgs...),
		descendantSQL:   source.descendantSQL,
		descendantArgs:  append([]any(nil), source.descendantArgs...),
		kind:            source.kind,
		// A field renamed to index is calculated pipeline data, not the
		// authorization-constrained physical index selector.
		caseSensitive:           false,
		numberType:              source.numberType,
		numericSort:             source.numericSort,
		alwaysNull:              source.alwaysNull,
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
		if field.valueSQL == publicName {
			projection = append(projection, publicName)
		} else {
			projection = append(projection, field.valueSQL+" AS "+publicName)
		}
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
		for _, field := range visible {
			if field.existsSQL == column || field.storedTypeSQL == column ||
				field.textEligibleSQL == column || field.descendantSQL == column {
				live = append(live, column)
				break
			}
		}
	}
	return live
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
	if state.rexCapturedBytesSQL != "" {
		privateColumns = append(privateColumns, state.rexCapturedBytesSQL)
	}
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
	if left.kind == fieldKindStringArray || right.kind == fieldKindStringArray {
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
			return dynamicNumericEvalComparisonCore(left, right, operator)
		}
		return dynamicEvalComparisonCore(left, right, operator)
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
	return leftSQL + " " + operator + " " + rightSQL, comparisonValueArgs(left, right)
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

func dynamicEvalComparisonCore(left, right compiledScalar, operator string) (string, []any) {
	if left.comparisonAtomic && right.comparisonAtomic {
		body, argumentOccurrences, ok := dynamicComparisonBody(
			left,
			right,
			operator,
		)
		if !ok {
			return "CAST(NULL AS Nullable(Bool))", nil
		}
		return body, repeatedComparisonValueArgs(
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
	body, _, ok := dynamicComparisonBody(left, right, operator)
	if !ok {
		return "CAST(NULL AS Nullable(Bool))", nil
	}
	return bindComparisonOperands(body, originalLeft, originalRight)
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
		integerCondition := "(" + dynamicExactIntegerPredicate(left, leftType) + " AND " +
			dynamicExactIntegerPredicate(right, rightType) + ")"
		numericCondition := "(" + dynamicNumericValuePredicate(left) + " AND " + dynamicNumericValuePredicate(right) + ")"
		stringCondition := "(" + leftType + " = 'String' AND " + rightType + " = 'String')"
		boolCondition := "(" + leftType + " = 'Bool' AND " + rightType + " = 'Bool')"
		boolComparison := nullBool
		argumentOccurrences := 3
		if !comparisonOperatorIsOrdered(operator) {
			boolComparison = dynamicBoolScalarSQL(left) + " " + operator + " " + dynamicBoolScalarSQL(right)
			argumentOccurrences = 4
		}
		result := "multiIf(" +
			integerCondition + ", " + scalarComparisonSQL(left, right, operator, true) + ", " +
			numericCondition + ", " + scalarComparisonSQL(left, right, operator, false) + ", " +
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
		if fixedNumberTypeIsInteger(fixed.numberType) {
			integer := comparison(numericScalarSQL(dynamic, true), numericScalarSQL(fixed, true))
			floating := comparison(numericScalarSQL(dynamic, false), numericScalarSQL(fixed, false))
			result := "multiIf(" + dynamicExactIntegerPredicate(dynamic, typeSQL) + ", " + integer + ", " +
				dynamicNumericValuePredicate(dynamic) + ", " + floating + ", " + nullBool + ")"
			return result, 2, true
		}
		return "if(" + dynamicNumericValuePredicate(dynamic) + ", " +
			comparison(numericScalarSQL(dynamic, false), numericScalarSQL(fixed, false)) +
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

func dynamicNumericEvalComparisonCore(
	left, right compiledScalar,
	operator string,
) (string, []any) {
	if left.comparisonAtomic && right.comparisonAtomic {
		body, argumentOccurrences, ok := dynamicNumericComparisonBody(
			left,
			right,
			operator,
		)
		if !ok {
			return "CAST(NULL AS Nullable(Bool))", nil
		}
		return body, repeatedComparisonValueArgs(
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
	body, _, ok := dynamicNumericComparisonBody(left, right, operator)
	if !ok {
		return "CAST(NULL AS Nullable(Bool))", nil
	}
	return bindComparisonOperands(body, originalLeft, originalRight)
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
		return "multiIf(" + integerCondition + ", " + exact + ", " +
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
	return "arrayElement(arrayMap((left_value, right_value) -> " +
			body + ", [" + left.valueSQL + "], [" +
			right.valueSQL + "]), 1)",
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

func scalarComparisonSQL(left, right compiledScalar, operator string, integer bool) string {
	return numericScalarSQL(left, integer) + " " + operator + " " + numericScalarSQL(right, integer)
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
	valueKey := "concat(char(0), 'open_splunk_value')"
	payload = dynamicTaggedMapSQL(value) + "[" + valueKey + "]"
	limit := strconv.Itoa(MaximumExactNumericBinTextBytes)
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
	if field.kind == fieldKindStringArray && expression.Value.Kind == plan.ValueKindNull {
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
		if field.kind == fieldKindDynamic {
			presence := "((" + exists + ") AND isNotNull(" + field.valueSQL + "))"
			if field.descendantSQL != "" {
				presence = "(" + presence + " OR (" + field.descendantSQL + "))"
				args = append(args, field.descendantArgs...)
			}
			return presence, args, nil
		}
		return "(" + exists + " AND isNotNull(" + field.valueSQL + "))", args, nil
	}
	if field.kind == fieldKindStringArray &&
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
		return "(" + exists + " AND NOT ifNull(" + predicate + ", 0))", args, nil
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
		exact := dynamicExactIntegerSQL(dynamic) + " = accurateCastOrNull(?, 'Int256')"
		decimal := dynamicTaggedDecimalFloatSQL(dynamic) + " = toFloat64OrNull(?)"
		return "multiIf(" + dynamicExactIntegerPredicate(dynamic, typeSQL) + ", " + exact + ", " +
			dynamicTaggedDecimalCondition(dynamic) + ", " + decimal + ", 0)", 2
	case plan.ValueKindFloat64:
		guard = "(" + guard + " OR " + dynamicTaggedDecimalCondition(dynamic) + ")"
		base = numericScalarSQL(dynamic, false) + " = toFloat64OrNull(?)"
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
		(expression.Value.Kind == plan.ValueKindInt64 || expression.Value.Kind == plan.ValueKindUint64) {
		typeSQL := dynamicTypeExpression(field)
		dynamic := compiledScalarFromField(field)
		exact := dynamicExactIntegerSQL(dynamic) + " " + operator + " accurateCastOrNull(?, 'Int256')"
		fallback := numericScalarSQL(dynamic, false) + " " + operator + " toFloat64OrNull(?)"
		return "multiIf(" + dynamicExactIntegerPredicate(dynamic, typeSQL) + ", " + exact + ", " + fallback + ")", 2, nil
	}
	if field.kind == fieldKindDynamic {
		return numericScalarSQL(compiledScalarFromField(field), false) + " " + operator + " toFloat64OrNull(?)", 1, nil
	}
	return "toFloat64OrNull(toString(" + field.valueSQL + ")) " + operator + " toFloat64OrNull(?)", 1, nil
}

func compiledScalarFromField(field fieldState) compiledScalar {
	return compiledScalar{
		valueSQL:                field.valueSQL,
		maxStringBytes:          field.maxStringBytes,
		existsSQL:               field.existsSQL,
		existsArgs:              append([]any(nil), field.existsArgs...),
		textEligibleSQL:         field.textEligibleSQL,
		dynamicDomain:           field.dynamicDomain,
		numericIntegral:         field.numericIntegral,
		mvCountOneOrNull:        field.mvCountOneOrNull,
		dynamicTypeSQL:          field.dynamicTypeSQL,
		storedTypeSQL:           field.storedTypeSQL,
		descendantSQL:           field.descendantSQL,
		descendantArgs:          append([]any(nil), field.descendantArgs...),
		kind:                    field.kind,
		numberType:              field.numberType,
		alwaysNull:              field.alwaysNull,
		comparisonAtomic:        true,
		materializeForPredicate: field.materializeForPredicate,
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
	if len(field.Path) == 0 {
		return fieldState{}, false, fmt.Errorf("compile ClickHouse field %q: dynamic path is empty", field.Name)
	}
	value := quoteIdentifier(internalFieldsColumn)
	for _, segment := range field.Path {
		if segment == "" {
			return fieldState{}, false, fmt.Errorf("compile ClickHouse field %q: dynamic path has empty segment", field.Name)
		}
		value += "." + quoteIdentifier(eventfields.EncodePhysicalPathSegment(segment))
	}
	return fieldState{
		valueSQL:       value,
		dynamicTypeSQL: "dynamicType(" + value + ")",
		existsSQL:      "has(" + quoteIdentifier(internalFieldNamesColumn) + ", ?)",
		existsArgs:     []any{eventfields.NormalizeDynamicPath(field.Path)},
		descendantSQL: "arrayExists(name -> startsWith(name, ?), " +
			quoteIdentifier(internalFieldNamesColumn) + ")",
		descendantArgs: []any{eventfields.NormalizeDynamicPath(field.Path) + "."},
		kind:           fieldKindDynamic,
	}, true, nil
}

func compileProjection(operator *plan.Project, state compileState) ([]string, compileState, []any, error) {
	next := compileState{
		visible:             make(map[string]fieldState),
		matchBudget:         state.matchBudget,
		likeBudget:          state.likeBudget,
		privateColumns:      append([]string(nil), state.privateColumns...),
		rexCapturedBytesSQL: state.rexCapturedBytesSQL,
		allowDynamic:        operator.Mode == plan.ProjectModeExclude && state.allowDynamic,
		eventRows:           state.eventRows,
		blocked:             cloneSet(state.blocked),
		blockedPrefixes:     cloneSet(state.blockedPrefixes),
		order:               append([]compiledSortKey(nil), state.order...),
		tieBreakers:         append([]compiledSortKey(nil), state.tieBreakers...),
		chronologicalBarriers: append(
			[]compiledChronologicalBarrier(nil),
			state.chronologicalBarriers...,
		),
	}
	var names []string
	switch operator.Mode {
	case plan.ProjectModeInclude, plan.ProjectModeTable:
		for _, field := range operator.Fields {
			names = append(names, field.Name)
		}
		if operator.Mode == plan.ProjectModeInclude {
			for _, implicit := range []string{"_time", "_raw"} {
				if _, exists := state.visible[implicit]; exists && !slices.Contains(names, implicit) {
					names = append(names, implicit)
				}
			}
		}
	case plan.ProjectModeExclude:
		excluded := make(map[string]struct{}, len(operator.Fields))
		for _, field := range operator.Fields {
			excluded[field.Name] = struct{}{}
			next.blocked[field.Name] = struct{}{}
		}
		for _, name := range state.publicOrder {
			if name == "fields" && state.eventRows {
				if _, visible := state.visible[name]; !visible {
					continue // avoid leaking excluded dynamic members in the public object
				}
			}
			if _, remove := excluded[name]; !remove {
				names = append(names, name)
			}
		}
	default:
		return nil, compileState{}, nil, errors.New("compile ClickHouse projection: invalid mode")
	}

	projection := make([]string, 0, len(names)+6)
	args := make([]any, 0)
	for _, name := range names {
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
		publicName := quoteIdentifier(name)
		if compiled.valueSQL == publicName {
			projection = append(projection, publicName)
		} else {
			projection = append(projection, compiled.valueSQL+" AS "+publicName)
		}
		next.visible[name] = fieldState{
			valueSQL: publicName, maxStringBytes: compiled.maxStringBytes,
			textEligibleSQL:         compiled.textEligibleSQL,
			dynamicDomain:           compiled.dynamicDomain,
			numericIntegral:         compiled.numericIntegral,
			mvCountOneOrNull:        compiled.mvCountOneOrNull,
			dynamicTypeSQL:          compiled.dynamicTypeSQL,
			storedTypeSQL:           compiled.storedTypeSQL,
			existsSQL:               rewriteExistenceForProjection(compiled, name),
			existsArgs:              append([]any(nil), compiled.existsArgs...),
			descendantSQL:           compiled.descendantSQL,
			descendantArgs:          append([]any(nil), compiled.descendantArgs...),
			kind:                    compiled.kind,
			caseSensitive:           compiled.caseSensitive,
			numberType:              compiled.numberType,
			numericSort:             compiled.numericSort,
			canonicalTime:           compiled.canonicalTime,
			alwaysNull:              compiled.alwaysNull,
			materializeForPredicate: compiled.materializeForPredicate,
		}
		next.publicOrder = append(next.publicOrder, name)
	}
	next.privateColumns = livePrivateColumns(next.privateColumns, next.visible)
	return appendPrivateEventProjection(projection, next), next, args, nil
}

func rewriteExistenceForProjection(field fieldState, name string) string {
	if field.existsSQL == "1" {
		return "1"
	}
	if field.kind == fieldKindStringArray && strings.HasPrefix(field.existsSQL, "notEmpty(") {
		return "notEmpty(" + quoteIdentifier(name) + ")"
	}
	if strings.HasPrefix(field.existsSQL, "isNotNull(") {
		return "isNotNull(" + quoteIdentifier(name) + ")"
	}
	return field.existsSQL
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
		if measure.Input.Name != "" ||
			measure.Input.Canonical ||
			len(measure.Input.Path) != 0 ||
			measure.Input.Range != (spl.Range{}) ||
			measure.Percentile != 0 {
			return errors.New(
				"compile ClickHouse aggregate: count(eval(...)) contains unsupported field or percentile metadata",
			)
		}
		if nilPlanExpression(measure.Predicate) {
			return errors.New(
				"compile ClickHouse aggregate: count(eval(...)) predicate is missing",
			)
		}
		if err := validateIfCondition(measure.Predicate); err != nil {
			return fmt.Errorf(
				"compile ClickHouse aggregate: invalid count(eval(...)) predicate: %w",
				err,
			)
		}
		if state.eventRows && state.allowDynamic {
			if sourceRange, reserved := predicateFieldSourceRange(
				measure.Predicate,
				"fields",
			); reserved {
				return &plan.Diagnostic{
					Code:    "SPL_AMBIGUOUS_STATS_FIELD",
					Message: "stats cannot read the event result's reserved fields payload without an exact upstream schema",
					Range:   sourceRange,
				}
			}
		}
	}
	return nil
}

func validateAggregateCardinality(operator *plan.Aggregate) error {
	if operator == nil || len(operator.Measures) == 0 {
		return errors.New("compile ClickHouse aggregate: no measures")
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
	for _, measure := range operator.Measures {
		if measure.Function != plan.AggregateFunctionEarliest &&
			measure.Function != plan.AggregateFunctionLatest {
			continue
		}
		timeField, ok := state.visible["_time"]
		if !state.eventRows || !ok || timeField.kind != fieldKindTime || !timeField.canonicalTime {
			return nil, nil, nil, compileState{}, nil, &plan.Diagnostic{
				Code:        "SPL_UNSUPPORTED_STATS_TIME_FIELD",
				Message:     "earliest and latest require event rows with the unmodified canonical _time field",
				Range:       measure.Input.Range,
				Suggestions: []string{"run stats earliest or latest before removing, replacing, or transforming _time"},
			}
		}
	}
	next = compileState{
		visible:               make(map[string]fieldState, len(operator.GroupBy)+len(operator.Measures)),
		matchBudget:           state.matchBudget,
		likeBudget:            state.likeBudget,
		allowDynamic:          false,
		eventRows:             false,
		blocked:               make(map[string]struct{}),
		chronologicalBarriers: append([]compiledChronologicalBarrier(nil), state.chronologicalBarriers...),
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
		field, ok, resolveErr := resolveCompiledField(group, state)
		if resolveErr != nil {
			return nil, nil, nil, compileState{}, nil, resolveErr
		}
		if !ok {
			// A transforming command retains its declared output schema even when
			// an upstream projection removed the grouping field. SPL emits no
			// groups in that case; use a typed NULL plus an always-false predicate
			// rather than resurrecting the private source document or surfacing an
			// internal compiler error.
			field = fieldState{
				valueSQL:   "CAST(NULL AS Nullable(String))",
				existsSQL:  "0",
				kind:       fieldKindString,
				alwaysNull: true,
			}
		}
		if field.kind == fieldKindStringArray {
			return nil, nil, nil, compileState{}, nil, unsupportedMultivalueUsage(
				"stats BY",
				group.Range,
			)
		}
		valueSQL := field.valueSQL
		kind := field.kind
		numericSort := field.numericSort
		supportedSQL := ""
		if kind == fieldKindDynamic {
			// SPL fields are compared and grouped by their lexical value. Dynamic
			// scalar storage types therefore intentionally converge on the same
			// UTF-8 group key (for example integer 500 and string "500").
			// Missing and explicit-null values are removed below.
			supported, lexical := statsByScalarExpressions(field)
			// Unsupported containers use one private placeholder group. A scoped
			// whole-input window below fails the search before any key is exposed.
			valueAlias := quoteIdentifier(fmt.Sprintf("__os_group_value_%d", len(groups)))
			next.preAggregateColumns = append(next.preAggregateColumns,
				"if("+supported+", "+lexical+", '') AS "+valueAlias,
			)
			valueSQL = valueAlias
			kind = fieldKindString
			numericSort = true
			supportedSQL = supported
		}
		groupOutput := fmt.Sprintf("__os_group_%d", len(groups))
		projection = append(projection, valueSQL+" AS "+quoteIdentifier(groupOutput))
		presence := "(" + field.existsSQL + " AND isNotNull(" + field.valueSQL + "))"
		presenceArgs := append([]any(nil), field.existsArgs...)
		if field.kind == fieldKindDynamic && field.descendantSQL != "" {
			// Non-empty objects are stored as flattened leaf paths, so the parent
			// itself is absent from field_names. Retain those rows until the scoped
			// aggregate support check can reject the container explicitly.
			presence = "(" + presence + " OR " + field.descendantSQL + ")"
			presenceArgs = append(presenceArgs, field.descendantArgs...)
		}
		if supportedSQL != "" {
			// Validate each key against its own presence rather than the combined
			// group eligibility predicate. A container must fail the whole scoped
			// search even when another BY key is missing on the same row.
			dynamicGroupInvalid = append(dynamicGroupInvalid, "("+presence+") AND NOT ("+supportedSQL+")")
			dynamicGroupInvalidArgs = append(dynamicGroupInvalidArgs, presenceArgs...)
		}
		predicates = append(predicates, presence)
		args = append(args, presenceArgs...)
		groups = append(groups, valueSQL)
		privateGroup := quoteIdentifier(groupOutput)
		next.visible[group.Name] = fieldState{
			valueSQL: privateGroup, existsSQL: "1", kind: kind,
			caseSensitive: field.caseSensitive, numberType: field.numberType,
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
	stringInputs := make(map[string]string)
	type scalarStringInput struct {
		ordinal        int
		valueAlias     string
		numberAlias    string
		candidateAlias string
		extremaReady   bool
	}
	scalarStringInputs := make(map[string]*scalarStringInput)
	countInputs := make(map[string]string)
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
	extremaInputs := make(map[string]string)
	type scalarExtremaResultKey struct {
		input    string
		function plan.AggregateFunction
	}
	type scalarExtremaResult struct {
		keyAlias  string
		typeAlias string
	}
	scalarExtremaResults := make(map[scalarExtremaResultKey]scalarExtremaResult)
	type chronologicalInput struct {
		candidatesAlias string
		validationAlias string
		multiple        bool
	}
	type chronologicalResultKey struct {
		input    string
		function plan.AggregateFunction
	}
	type chronologicalResult struct {
		winnerAlias string
		typeAlias   string
	}
	chronologicalInputs := make(map[string]chronologicalInput)
	chronologicalResults := make(map[chronologicalResultKey]chronologicalResult)
	chronologicalRowKey := ""
	exactStringSets := make(map[string]string)
	distinctCounts := make(map[string]string)
	type orderedStringList struct {
		listColumn     string
		overflowColumn string
	}
	orderedStringLists := make(map[string]orderedStringList)
	valuesInputs := make(map[string]struct{})
	extremaMeasureInputs := make(map[string]struct{})
	numericArrayConsumers := make(map[string]struct{})
	percentileLevels := make(map[string][]uint8)
	for _, measure := range operator.Measures {
		if measure.Function == plan.AggregateFunctionValues {
			valuesInputs[measure.Input.Name] = struct{}{}
		}
		if measure.Function == plan.AggregateFunctionMinimum ||
			measure.Function == plan.AggregateFunctionMaximum {
			extremaMeasureInputs[measure.Input.Name] = struct{}{}
		}
		if measure.Function == plan.AggregateFunctionSum ||
			measure.Function == plan.AggregateFunctionAverage {
			numericArrayConsumers[measure.Input.Name] = struct{}{}
		}
		if measure.Function == plan.AggregateFunctionPercentile &&
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
	scalarStringInputFor := func(ref plan.FieldRef, input fieldState) *scalarStringInput {
		if cached, ok := scalarStringInputs[ref.Name]; ok {
			return cached
		}
		ordinal := len(scalarStringInputs)
		inputSQL, inputArgs := statsScalarStringInputSQL(input)
		inputAlias := quoteIdentifier(fmt.Sprintf(
			"__os_measure_scalar_string_%d",
			ordinal,
		))
		cached := &scalarStringInput{
			ordinal:    ordinal,
			valueAlias: inputAlias,
		}
		scalarStringInputs[ref.Name] = cached
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
				input.kind == fieldKindString {
				scalarInput := scalarStringInputFor(ref, input)
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
	chronologicalRowKeyFor := func() string {
		if chronologicalRowKey != "" {
			return chronologicalRowKey
		}
		chronologicalRowKey = quoteIdentifier("__os_chronological_row_key")
		next.preAggregateColumns = append(
			next.preAggregateColumns,
			"tuple("+
				strings.Join([]string{
					quoteIdentifier(internalSortTimeColumn),
					quoteIdentifier(internalSortIDColumn),
					quoteIdentifier(internalSortVisibilityColumn),
					quoteIdentifier(internalSortSourceIdentityColumn),
				}, ", ")+
				") AS "+chronologicalRowKey,
		)
		return chronologicalRowKey
	}
	chronologicalInputFor := func(ref plan.FieldRef) (chronologicalInput, error) {
		if cached, ok := chronologicalInputs[ref.Name]; ok {
			return cached, nil
		}
		input, exists, resolveErr := resolveCompiledField(ref, state)
		if resolveErr != nil {
			return chronologicalInput{}, resolveErr
		}
		ordinal := len(chronologicalInputs)
		compiled := chronologicalInput{}
		candidatesSQL, candidateArgs, runtimeValidated := chronologicalCandidatesSQL(input, exists)
		compiled.candidatesAlias = quoteIdentifier(fmt.Sprintf(
			"__os_chronological_candidates_%d",
			ordinal,
		))
		compiled.multiple = exists &&
			(input.kind == fieldKindDynamic || input.kind == fieldKindStringArray)
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
		chronologicalInputs[ref.Name] = compiled
		return compiled, nil
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
		if ok && (input.kind == fieldKindDynamic || input.kind == fieldKindStringArray) {
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
	for measureIndex, measure := range operator.Measures {
		if _, fieldErr := plan.ResolveField(measure.Output, spl.Range{}); fieldErr != nil {
			return nil, nil, nil, compileState{}, nil, fmt.Errorf(
				"compile ClickHouse aggregate: invalid output field %q: %w",
				measure.Output,
				fieldErr,
			)
		}
		switch measure.Function {
		case plan.AggregateFunctionCountRows:
			if measure.Input.Name != "" || measure.Input.Canonical || len(measure.Input.Path) != 0 ||
				measure.Input.Range != (spl.Range{}) || measure.Percentile != 0 {
				return nil, nil, nil, compileState{}, nil, errors.New(
					"compile ClickHouse aggregate: count contains unsupported input metadata",
				)
			}
		case plan.AggregateFunctionCountPredicate:
			// Predicate structure and mutually exclusive metadata were
			// validated before any materialization-field traversal.
		case plan.AggregateFunctionCountValues:
			if measure.Percentile != 0 {
				return nil, nil, nil, compileState{}, nil, errors.New(
					"compile ClickHouse aggregate: count(field) contains percentile metadata",
				)
			}
		case plan.AggregateFunctionPercentile:
			if measure.Percentile < 1 || measure.Percentile > 99 {
				return nil, nil, nil, compileState{}, nil, fmt.Errorf(
					"compile ClickHouse aggregate: unsupported percentile %d",
					measure.Percentile,
				)
			}
		case plan.AggregateFunctionSum, plan.AggregateFunctionAverage,
			plan.AggregateFunctionDistinctCount, plan.AggregateFunctionValues,
			plan.AggregateFunctionList,
			plan.AggregateFunctionMinimum, plan.AggregateFunctionMaximum,
			plan.AggregateFunctionEarliest, plan.AggregateFunctionLatest:
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
		}
		if _, duplicate := seen[measure.Output]; duplicate {
			return nil, nil, nil, compileState{}, nil, fmt.Errorf("compile ClickHouse aggregate: output field %q is duplicated", measure.Output)
		}
		seen[measure.Output] = struct{}{}
		output := quoteIdentifier(measure.Output)
		measureState := fieldState{valueSQL: output, existsSQL: "1", kind: fieldKindNumber}
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
			inputAlias, cached := countInputs[measure.Input.Name]
			if !cached {
				input, ok, resolveErr := resolveCompiledField(measure.Input, state)
				if resolveErr != nil {
					return nil, nil, nil, compileState{}, nil, resolveErr
				}
				inputSQL := "toUInt64(0)"
				var inputArgs []any
				if ok {
					inputSQL, inputArgs = countValueInputSQL(input)
				}
				inputAlias = quoteIdentifier(fmt.Sprintf("__os_measure_count_%d", len(countInputs)))
				countInputs[measure.Input.Name] = inputAlias
				next.preAggregateColumns = append(next.preAggregateColumns, inputSQL+" AS "+inputAlias)
				next.preAggregateArgs = append(next.preAggregateArgs, inputArgs...)
			}
			// Aggregate in UInt128 so the intermediate state cannot wrap. The
			// production 250M-row read ceiling and 1 MiB hard event ceiling make
			// the final occurrence total strictly smaller than UInt64.
			projection = append(projection, "toUInt64(sum(toUInt128("+inputAlias+"))) AS "+output)
			measureState.numberType = "UInt64"
		case plan.AggregateFunctionPercentile:
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
				"arrayElementOrNull("+percentiles.column+", "+
					strconv.Itoa(position)+") AS "+output,
			)
			measureState.numberType = "Float64"
		case plan.AggregateFunctionSum, plan.AggregateFunctionAverage:
			inputAlias, inputErr := numericInputFor(measure.Input)
			if inputErr != nil {
				return nil, nil, nil, compileState{}, nil, inputErr
			}
			countSQL := "sum(length(" + inputAlias + "))"
			sumSQL := "sum(arraySum(" + inputAlias + "))"
			nullFloat := "CAST(NULL AS Nullable(Float64))"
			valueSQL := "if(" + countSQL + " = 0, " + nullFloat + ", toFloat64(" + sumSQL + "))"
			if measure.Function == plan.AggregateFunctionAverage {
				valueSQL = "if(" + countSQL + " = 0, " + nullFloat + ", toFloat64(" + sumSQL + ") / toFloat64(" + countSQL + "))"
			}
			projection = append(projection, valueSQL+" AS "+output)
			measureState.numberType = "Float64"
		case plan.AggregateFunctionMinimum, plan.AggregateFunctionMaximum:
			input, ok, resolveErr := resolveCompiledField(measure.Input, state)
			if resolveErr != nil {
				return nil, nil, nil, compileState{}, nil, resolveErr
			}
			if ok && (input.kind == fieldKindNumber || input.kind == fieldKindTime ||
				input.kind == fieldKindBool) {
				existsSQL := input.existsSQL
				if existsSQL == "" {
					existsSQL = "1"
				}
				eligible := "(" + existsSQL + ") AND isNotNull(" + input.valueSQL + ")"
				if input.kind == fieldKindNumber && strings.HasPrefix(input.numberType, "Float") {
					eligible += " AND isFinite(" + input.valueSQL + ")"
				}
				function := "minIfOrNull"
				if measure.Function == plan.AggregateFunctionMaximum {
					function = "maxIfOrNull"
				}
				projection = append(projection, function+"("+input.valueSQL+", "+eligible+") AS "+output)
				args = append(args, input.existsArgs...)
				measureState.kind = input.kind
				measureState.numberType = input.numberType
				measureState.caseSensitive = input.caseSensitive
				break
			}

			if ok && input.kind == fieldKindString {
				scalarInput := scalarStringInputFor(measure.Input, input)
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
						)+" AS "+scalarInput.candidateAlias,
					)
					scalarInput.extremaReady = true
				}

				resultKey := scalarExtremaResultKey{
					input:    measure.Input.Name,
					function: measure.Function,
				}
				result, cached := scalarExtremaResults[resultKey]
				if !cached {
					result = scalarExtremaResult{
						keyAlias: quoteIdentifier(fmt.Sprintf(
							"__os_stats_extrema_key_%d",
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
						statsExtremaScalarAggregateKeySQL(
							measure.Function,
							scalarInput.candidateAlias,
						)+" AS "+result.keyAlias,
					)
					next.privateColumns = append(
						next.privateColumns,
						result.typeAlias,
					)
				}
				next.postAggregateScalarExtrema = append(
					next.postAggregateScalarExtrema,
					compiledScalarExtremaMeasure{
						keyColumn:    result.keyAlias,
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

			stringInputSQL, inputErr := stringInputFor(measure.Input)
			if inputErr != nil {
				return nil, nil, nil, compileState{}, nil, inputErr
			}
			candidates, cached := extremaInputs[measure.Input.Name]
			if !cached {
				candidates = quoteIdentifier(fmt.Sprintf("__os_measure_extrema_%d", len(extremaInputs)))
				extremaInputs[measure.Input.Name] = candidates
				next.preAggregateColumns = append(
					next.preAggregateColumns,
					statsExtremaCandidatesSQL(stringInputSQL)+" AS "+candidates,
				)
			}
			extreme := statsExtremaAggregateSQL(measure.Function, candidates)
			typeAlias := quoteIdentifier(fmt.Sprintf("__os_stats_extrema_type_%d", measureIndex))
			projection = append(
				projection,
				extreme+" AS "+output,
				statsExtremaStoredTypeSQL(extreme)+" AS "+typeAlias,
			)
			measureState = fieldState{
				valueSQL:       output,
				dynamicTypeSQL: "dynamicType(" + output + ")",
				storedTypeSQL:  typeAlias,
				existsSQL:      "1",
				kind:           fieldKindDynamic,
			}
			next.privateColumns = append(next.privateColumns, typeAlias)
		case plan.AggregateFunctionEarliest, plan.AggregateFunctionLatest:
			input, inputErr := chronologicalInputFor(measure.Input)
			if inputErr != nil {
				return nil, nil, nil, compileState{}, nil, inputErr
			}
			rowKey := chronologicalRowKeyFor()
			resultKey := chronologicalResultKey{
				input:    measure.Input.Name,
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
				function := "argMinOrNullIf"
				argument := "tupleElement(" + input.candidatesAlias + ", 1)"
				key := "tuple(" + rowKey + ", toUInt64(1))"
				count := "tupleElement(" + input.candidatesAlias + ", 3)"
				condition := count + " != 0"
				if measure.Function == plan.AggregateFunctionLatest {
					function = "argMaxOrNullIf"
					argument = "tupleElement(" + input.candidatesAlias + ", 2)"
					if input.multiple {
						key = "tuple(" + rowKey + ", toUInt64(" + count + "))"
					}
				}
				aggregateSQL := function + "(" + argument + ", " + key + ", " +
					condition + ")"
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
		case plan.AggregateFunctionDistinctCount, plan.AggregateFunctionValues:
			inputSQL, inputErr := stringInputFor(measure.Input)
			if inputErr != nil {
				return nil, nil, nil, compileState{}, nil, inputErr
			}
			_, publishesValues := valuesInputs[measure.Input.Name]
			if measure.Function == plan.AggregateFunctionDistinctCount && !publishesValues {
				cardinalityColumn, cached := distinctCounts[measure.Input.Name]
				if !cached {
					cardinalityColumn = quoteIdentifier(fmt.Sprintf("__os_dc_cardinality_%d", len(distinctCounts)))
					distinctCounts[measure.Input.Name] = cardinalityColumn
					projection = append(projection, distinctCountCardinalitySQL(inputSQL)+" AS "+cardinalityColumn)
				}
				next.postAggregateDistinctCounts = append(next.postAggregateDistinctCounts, compiledDistinctCount{
					cardinalityColumn: cardinalityColumn,
					outputColumn:      output,
				})
				measureState.numberType = "UInt64"
			} else {
				setColumn, cached := exactStringSets[measure.Input.Name]
				if !cached {
					setColumn = quoteIdentifier(fmt.Sprintf("__os_exact_strings_%d", len(exactStringSets)))
					exactStringSets[measure.Input.Name] = setColumn
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
					// The physical result is always a non-null Array(String), but an
					// empty multivalue has no logical SPL field value.
					measureState.existsSQL = "notEmpty(" + output + ")"
				}
			}
		case plan.AggregateFunctionList:
			_, inputExists, resolveErr := resolveCompiledField(measure.Input, state)
			if resolveErr != nil {
				return nil, nil, nil, compileState{}, nil, resolveErr
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
				measureState.existsSQL = "notEmpty(" + output + ")"
				break
			}
			inputSQL, inputErr := stringInputFor(measure.Input)
			if inputErr != nil {
				return nil, nil, nil, compileState{}, nil, inputErr
			}
			rowOrdinal, windowOrder, orderErr := listRowOrdinalFor()
			if orderErr != nil {
				return nil, nil, nil, compileState{}, nil, orderErr
			}
			list, cached := orderedStringLists[measure.Input.Name]
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
				orderedStringLists[measure.Input.Name] = list
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
			// As with values(), an empty physical array has no logical SPL value.
			measureState.existsSQL = "notEmpty(" + output + ")"
		default:
			return nil, nil, nil, compileState{}, nil, fmt.Errorf("compile ClickHouse aggregate: unsupported function %d", measure.Function)
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
		anyUnsupportedColumn := quoteIdentifier("__os_stats_by_any_unsupported")
		invalid := "(" + strings.Join(dynamicGroupInvalid, ") OR (") + ")"
		next.preAggregateValidationColumns = append(next.preAggregateValidationColumns,
			"max(CAST("+invalid+" AS UInt8)) OVER () AS "+anyUnsupportedColumn,
		)
		next.preAggregateValidationArgs = append(next.preAggregateValidationArgs, dynamicGroupInvalidArgs...)
		eligible := "(" + strings.Join(predicates, " AND ") + ")"
		predicates = []string{
			"if(" + anyUnsupportedColumn + " != 0, throwIf(toUInt8(1), '" + UnsupportedStatsByValueMarker + "') = 0, " + eligible + ")",
		}
	}
	return projection, predicates, groups, next, args, nil
}

func countValueInputSQL(field fieldState) (string, []any) {
	if field.kind == fieldKindStringArray {
		// A fixed multivalue is physically non-null and its empty representation
		// has cardinality zero, so its logical presence predicate is unnecessary.
		return "toUInt64(length(" + field.valueSQL + "))", nil
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
		"if(" + typeSQL + " = 'Array(Dynamic)', " + arrayCount + ", toUInt64(1)), " +
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
	scalar := statsScalarStringOrNullSQL(field)
	scalarArray := compactNullableArraySQL("[" + scalar + "]")
	value := scalarArray
	if field.kind == fieldKindDynamic {
		typeSQL := dynamicTypeExpression(field)
		element := fieldState{
			valueSQL:       "element",
			dynamicTypeSQL: "dynamicType(element)",
			kind:           fieldKindDynamic,
		}
		elementSupported, elementLexical := statsByScalarExpressions(element)
		elementType := dynamicTypeExpression(element)
		nullString := "CAST(NULL AS Nullable(String))"
		elementValue := "if(" + elementType + " = 'None', " + nullString +
			", if(throwIf(toUInt8(NOT (" + elementSupported + ")), '" +
			UnsupportedStatsMeasureValueMarker + "') = 0, " + elementLexical + ", " + nullString + "))"
		array := compactNullableArraySQL(
			"arrayMap(element -> " + elementValue +
				", dynamicElement(" + field.valueSQL + ", 'Array(Dynamic)'))",
		)
		value = "if(" + typeSQL + " = 'Array(Dynamic)', " + array + ", " + scalarArray + ")"

		scalarSupported, _ := statsByScalarExpressions(field)
		existsSQL := field.existsSQL
		if existsSQL == "" {
			existsSQL = "1"
		}
		lambdaParameters := []string{"field_present"}
		lambdaArguments := []string{"[toUInt8(" + existsSQL + ")]"}
		args := append([]any(nil), field.existsArgs...)
		descendantPresent := "0"
		if field.descendantSQL != "" {
			lambdaParameters = append(lambdaParameters, "descendant_present")
			lambdaArguments = append(lambdaArguments, "[toUInt8("+field.descendantSQL+")]")
			args = append(args, field.descendantArgs...)
			descendantPresent = "descendant_present"
		}
		topLevelUnsupported := "(field_present != 0 AND " + typeSQL +
			" != 'None' AND " + typeSQL + " != 'Array(Dynamic)' AND NOT (" + scalarSupported + "))"
		invalid := "(" + topLevelUnsupported + " OR " + descendantPresent + " != 0)"
		body := "if(throwIf(toUInt8(" + invalid + "), '" + UnsupportedStatsMeasureValueMarker +
			"') = 0, if(field_present != 0, " + value + ", " + empty + "), " + empty + ")"
		return "arrayElement(arrayMap((" + strings.Join(lambdaParameters, ", ") + ") -> " + body +
			", " + strings.Join(lambdaArguments, ", ") + "), 1)", args
	}
	if field.existsSQL == "" || field.existsSQL == "1" {
		return value, nil
	}
	return "if(" + field.existsSQL + ", " + value + ", " + empty + ")", append([]any(nil), field.existsArgs...)
}

// chronologicalCandidatesSQL normalizes one event field to a constant-size
// tuple: first eligible lexical value, last eligible lexical value, eligible
// member count, and an unsupported-container bit. Bounded selector passes over
// Dynamic multivalues avoid retaining either an Array ordering key or a
// normalized copy of every member.
func chronologicalCandidatesSQL(field fieldState, exists bool) (string, []any, bool) {
	empty := "tuple(CAST('' AS String), CAST('' AS String), toUInt64(0), toUInt8(0))"
	if !exists {
		return empty, nil, false
	}

	if field.kind == fieldKindStringArray {
		values := field.valueSQL
		var args []any
		if field.existsSQL != "" && field.existsSQL != "1" {
			values = "if(" + field.existsSQL + ", " + values + ", CAST([], 'Array(String)'))"
			args = append(args, field.existsArgs...)
		}
		count := "toUInt64(length(" + values + "))"
		return "tuple(" +
				"if(" + count + " != 0, arrayElement(" + values + ", 1), CAST('' AS String)), " +
				"if(" + count + " != 0, arrayElement(" + values + ", -1), CAST('' AS String)), " +
				count + ", toUInt8(0))",
			args,
			false
	}

	if field.kind != fieldKindDynamic {
		value := statsScalarStringOrNullSQL(field)
		existsSQL := field.existsSQL
		if existsSQL == "" {
			existsSQL = "1"
		}
		value = "if(" + existsSQL + ", " + value + ", CAST(NULL AS Nullable(String)))"
		return "tuple(" +
				"ifNull(" + value + ", CAST('' AS String)), " +
				"ifNull(" + value + ", CAST('' AS String)), " +
				"toUInt64(isNotNull(" + value + ")), toUInt8(0))",
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
	first := "arrayFirst(element -> " + elementEligible + ", " + values + ")"
	last := "arrayLast(element -> " + elementEligible + ", " + values + ")"
	count := "toUInt64(arrayCount(element -> " + elementEligible + ", " + values + "))"
	memberInvalid := "toUInt8(arrayExists(element -> " + elementInvalid + ", " + values + "))"
	firstElement := fieldState{
		valueSQL:       "first_element",
		dynamicTypeSQL: "dynamicType(first_element)",
		kind:           fieldKindDynamic,
	}
	lastElement := fieldState{
		valueSQL:       "last_element",
		dynamicTypeSQL: "dynamicType(last_element)",
		kind:           fieldKindDynamic,
	}
	_, firstLexical := statsByScalarExpressions(firstElement)
	_, lastLexical := statsByScalarExpressions(lastElement)
	selected := "arrayElement(arrayMap(" +
		"(first_element, last_element, eligible_count, member_invalid) -> tuple(" +
		"if(eligible_count != 0, " + firstLexical + ", CAST('' AS String)), " +
		"if(eligible_count != 0, " + lastLexical + ", CAST('' AS String)), " +
		"eligible_count, member_invalid), " +
		"[" + first + "], [" + last + "], [" + count + "], [" + memberInvalid + "]), 1)"
	scalar := "tuple(" + scalarLexical + ", " + scalarLexical +
		", toUInt64(1), toUInt8(0))"
	invalid := "tuple(CAST('' AS String), CAST('' AS String), toUInt64(0), toUInt8(1))"

	existsSQL := field.existsSQL
	if existsSQL == "" {
		existsSQL = "1"
	}
	descendantSQL := "0"
	args := append([]any(nil), field.existsArgs...)
	if field.descendantSQL != "" {
		descendantSQL = field.descendantSQL
		args = append(args, field.descendantArgs...)
	}
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

func statsExtremaScalarNumberSQL(valueSQL string) string {
	value := "ifNull(" + valueSQL + ", CAST('' AS String))"
	return "if(isNotNull(" + valueSQL + "), " + statsExtremaNumericOrNullSQL(value) +
		", CAST(NULL AS Nullable(Float64)))"
}

func statsExtremaNormalizedNumberSQL(numberSQL string) string {
	return "if(assumeNotNull(" + numberSQL +
		") = 0, toFloat64(0), assumeNotNull(" + numberSQL + "))"
}

func statsExtremaOrderingKeySQL(valueSQL, numberSQL string) string {
	return "tuple(toUInt8(isNull(" + numberSQL + ")), if(isNotNull(" + numberSQL +
		"), " + statsExtremaNormalizedNumberSQL(numberSQL) +
		", toFloat64(0)), if(isNull(" + numberSQL + "), " + valueSQL +
		", CAST('' AS String)))"
}

func statsExtremaScalarCandidateSQL(valueSQL, numberSQL string) string {
	value := "ifNull(" + valueSQL + ", CAST('' AS String))"
	key := statsExtremaOrderingKeySQL(value, numberSQL)
	return "tuple(" + key + ", toUInt8(isNotNull(" + valueSQL + ")))"
}

func statsExtremaScalarAggregateKeySQL(
	function plan.AggregateFunction,
	candidateSQL string,
) string {
	name := "minIfOrNull"
	if function == plan.AggregateFunctionMaximum {
		name = "maxIfOrNull"
	}
	key := "tupleElement(" + candidateSQL + ", 1)"
	eligible := "tupleElement(" + candidateSQL + ", 2) != 0"
	return name + "(" + key + ", " + eligible + ")"
}

func statsExtremaScalarValueSQL(extremeKeySQL string) string {
	nonNull := "assumeNotNull(" + extremeKeySQL + ")"
	return "if(isNull(" + extremeKeySQL + "), CAST(NULL AS Dynamic), if(tupleElement(" +
		nonNull + ", 1) = 0, CAST(tupleElement(" + nonNull +
		", 2) AS Dynamic), CAST(tupleElement(" + nonNull + ", 3) AS Dynamic)))"
}

func statsExtremaScalarStoredTypeSQL(extremeKeySQL string) string {
	nonNull := "assumeNotNull(" + extremeKeySQL + ")"
	class := "tupleElement(" + nonNull + ", 1)"
	lexical := "tupleElement(" + nonNull + ", 3)"
	return statsExtremaStoredTypeFromConditionsSQL(
		"isNull("+extremeKeySQL+")",
		class+" = 0",
		class+" != 0",
		lexical,
	)
}

func statsExtremaCandidatesSQL(valuesSQL string) string {
	number := statsExtremaNumericOrNullSQL("value")
	candidate := "if(isNotNull(number), CAST(" + statsExtremaNormalizedNumberSQL("number") +
		" AS Dynamic), CAST(value AS Dynamic))"
	key := statsExtremaOrderingKeySQL("value", "number")
	bound := "arrayElement(arrayMap(number -> tuple(" + candidate + ", " + key +
		"), [" + number + "]), 1)"
	return "arrayMap(value -> " + bound + ", " + valuesSQL + ")"
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
	typeSQL := "dynamicType(" + valueSQL + ")"
	stringSQL := "dynamicElement(" + valueSQL + ", 'String')"
	return statsExtremaStoredTypeFromConditionsSQL(
		typeSQL+" = 'None'",
		typeSQL+" = 'Float64'",
		typeSQL+" = 'String'",
		stringSQL,
	)
}

func statsExtremaStoredTypeFromConditionsSQL(
	nullConditionSQL string,
	numberConditionSQL string,
	stringConditionSQL string,
	stringSQL string,
) string {
	return "multiIf(" +
		nullConditionSQL + ", toUInt8(" + strconv.Itoa(int(eventfields.StoredValueTypeNull)) + "), " +
		numberConditionSQL + ", toUInt8(" + strconv.Itoa(int(eventfields.StoredValueTypeDouble)) + "), " +
		stringConditionSQL + " AND isValidUTF8(" + stringSQL + "), toUInt8(" +
		strconv.Itoa(int(eventfields.StoredValueTypeString)) + "), " +
		stringConditionSQL + ", toUInt8(" + strconv.Itoa(int(eventfields.StoredValueTypeBytes)) + "), " +
		"toUInt8(0))"
}

func compileChronologicalResults(
	relation compiledRelation,
	measures []compiledChronologicalMeasure,
	ownerRange spl.Range,
	stage int,
) (compiledRelation, int, *compiledChronologicalBarrier) {
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
		nonNullWinner := "assumeNotNull(" + measure.winnerColumn + ")"
		value := nonNullWinner
		projection = append(
			projection,
			"if(isNull("+measure.winnerColumn+"), CAST(NULL AS Dynamic), CAST("+
				value+" AS Dynamic)) AS "+measure.outputColumn,
		)
		if _, published := publishedTypes[measure.winnerColumn]; published {
			continue
		}
		publishedTypes[measure.winnerColumn] = struct{}{}
		projection = append(
			projection,
			statsExtremaStoredTypeFromConditionsSQL(
				"isNull("+measure.winnerColumn+")",
				"0",
				"1",
				value,
			)+" AS "+measure.typeColumn,
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
	return published, 1, &compiledChronologicalBarrier{
		name:              materialized,
		sql:               relation.sql,
		validationColumns: validations,
		depth:             relation.depth,
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

	keys := make([]string, 0, len(measures))
	seenKeys := make(map[string]struct{}, len(measures))
	projection := make([]string, 1, 1+len(measures)*2)
	for _, measure := range measures {
		if _, seen := seenKeys[measure.keyColumn]; !seen {
			seenKeys[measure.keyColumn] = struct{}{}
			keys = append(keys, measure.keyColumn)
		}
	}
	projection[0] = "* EXCEPT (" + strings.Join(keys, ", ") + ")"

	publishedTypes := make(map[string]struct{}, len(keys))
	for _, measure := range measures {
		projection = append(
			projection,
			statsExtremaScalarValueSQL(measure.keyColumn)+" AS "+measure.outputColumn,
		)
		if _, published := publishedTypes[measure.keyColumn]; !published {
			publishedTypes[measure.keyColumn] = struct{}{}
			projection = append(
				projection,
				statsExtremaScalarStoredTypeSQL(measure.keyColumn)+" AS "+measure.typeColumn,
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
	value := nullString
	switch field.kind {
	case fieldKindDynamic:
		supported, lexical := statsByScalarExpressions(field)
		value = "if(" + supported + ", " + lexical + ", " + nullString + ")"
	case fieldKindString, fieldKindNumber, fieldKindBool, fieldKindTime:
		value = "CAST(toString(" + field.valueSQL + ") AS Nullable(String))"
	}
	if field.textEligibleSQL != "" {
		value = "if(ifNull(" + field.textEligibleSQL + ", 0), " +
			value + ", " + nullString + ")"
	}
	return value
}

func boundedDistinctCountSQL(inputSQL string) string {
	maximum := strconv.FormatUint(MaximumStatsDistinctValuesPerGroup, 10)
	cardinality := distinctCountCardinalitySQL(inputSQL)
	return "arrayElement(arrayMap(cardinality -> cardinality + toUInt64(throwIf(toUInt8(cardinality > " +
		maximum + "), '" + UnsupportedStatsDistinctLimitMarker + "')), [" + cardinality + "]), 1)"
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
			"arrayMap(item -> tupleElement(item, 3), "+listColumn+") AS "+
				materialized[listColumn],
		)
		byteConditions = append(
			byteConditions,
			overflowColumns[index]+" != 0",
			orderedStringListPayloadBytesSQL(listColumn)+" > toUInt128("+
				strconv.FormatUint(MaximumStatsListBytesPerGroup, 10)+")",
		)
	}

	rowElementTotal := "toUInt128(0)"
	rowByteTotal := "toUInt128(0)"
	for _, measure := range measures {
		// Public aliases count independently even when their physical ordered
		// aggregate state is shared.
		rowElementTotal += " + toUInt128(length(" + measure.listColumn + "))"
		rowByteTotal += " + " + orderedStringListPayloadBytesSQL(measure.listColumn)
	}
	for _, valuesColumn := range existingValues {
		// values() has already passed its own exact-state barrier. Include each
		// public values alias again so list() cannot bypass the combined
		// transforming-row and transport budgets.
		rowElementTotal += " + toUInt128(length(" + valuesColumn + "))"
		rowByteTotal += " + " + stringArrayPayloadBytesSQL(valuesColumn)
	}

	elementOverflow := quoteIdentifier("__os_stats_list_any_overflow")
	totalElements := quoteIdentifier("__os_stats_list_total_elements")
	bytesOverflow := quoteIdentifier("__os_stats_list_bytes_any_overflow")
	totalBytes := quoteIdentifier("__os_stats_list_total_bytes")
	windowColumns = append(
		windowColumns,
		"max(toUInt8("+rowElementTotal+" > toUInt128("+
			strconv.FormatUint(MaximumStatsValuesPerGroup, 10)+
			"))) OVER () AS "+elementOverflow,
		"sum("+rowElementTotal+") OVER () AS "+totalElements,
		"max(toUInt8(("+strings.Join(byteConditions, " OR ")+") OR "+
			rowByteTotal+" > toUInt128("+
			strconv.FormatUint(MaximumStatsValuesBytesPerGroup, 10)+
			"))) OVER () AS "+bytesOverflow,
		"sum("+rowByteTotal+") OVER () AS "+totalBytes,
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

		rowElementTotal := "toUInt128(0)"
		rowByteTotal := "toUInt128(0)"
		for _, measure := range valuesMeasures {
			// Deliberately retain duplicates: two public aliases create two
			// recursive list cells even when their aggregate state is shared.
			rowElementTotal += " + toUInt128(length(" + measure.setColumn + "))"
			rowByteTotal += " + " + stringArrayPayloadBytesSQL(measure.setColumn)
		}
		windowColumns = append(
			windowColumns,
			"max(toUInt8(("+strings.Join(valueConditions, " OR ")+") OR "+rowElementTotal+
				" > toUInt128("+strconv.FormatUint(MaximumStatsValuesPerGroup, 10)+
				"))) OVER () AS "+valuesOverflow,
			"sum("+rowElementTotal+") OVER () AS "+valuesTotalElements,
			"max(toUInt8(("+strings.Join(byteConditions, " OR ")+") OR "+rowByteTotal+
				" > toUInt128("+strconv.FormatUint(MaximumStatsValuesBytesPerGroup, 10)+
				"))) OVER () AS "+valuesBytesOverflow,
			"sum("+rowByteTotal+") OVER () AS "+valuesTotalBytes,
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
				UnsupportedStatsDistinctLimitMarker+"') = 0",
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
		" != 0), '" + UnsupportedStatsDistinctLimitMarker + "') = 0"
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
	typeSQL := dynamicTypeExpression(field)
	mapSQL := "dynamicElement(" + field.valueSQL + ", 'Map(String, String)')"
	valueKey := "concat(char(0), 'open_splunk_value')"
	value := compiledScalar{
		valueSQL:       field.valueSQL,
		dynamicTypeSQL: typeSQL,
		kind:           fieldKindDynamic,
	}
	extended := dynamicTaggedScalarEnvelopeCondition(value)
	// None is excluded deliberately. Missing and explicit-null leaves are
	// removed before aggregation, while a flattened object parent reads as None
	// at its literal path and must set the unsupported-container flag.
	supported = "(" + typeSQL + " IN ('String', 'Float64', 'Bool') OR " +
		dynamicIntegerTypePredicate(typeSQL) + " OR " + extended + ")"
	lexical = "if(" + typeSQL + " = 'Map(String, String)', " + mapSQL + "[" + valueKey + "], toString(" + field.valueSQL + "))"
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
		if field.kind == fieldKindStringArray {
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

func compileSort(keys []plan.SortKey, state compileState, stage int) ([]string, []compiledSortKey, string, error) {
	if len(keys) == 0 {
		return nil, nil, "", errors.New("compile ClickHouse sort: no keys")
	}
	materialized := make([]string, 0, len(keys)+len(state.tieBreakers))
	compiled := make([]compiledSortKey, 0, len(keys)+len(state.tieBreakers))
	explicitValues := make(map[string]struct{}, len(keys))
	for i, key := range keys {
		field, ok, err := resolveCompiledField(key.Field, state)
		if err != nil {
			return nil, nil, "", err
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
		if field.kind == fieldKindStringArray {
			return nil, nil, "", unsupportedMultivalueUsage("sort", key.Field.Range)
		}
		explicitValues[field.valueSQL] = struct{}{}
		alias := fmt.Sprintf("__os_order_%d_%d", stage, i)
		sortValue := field.valueSQL
		switch key.Mode {
		case plan.SortValueModeAuto:
			if field.kind == fieldKindDynamic || field.numericSort {
				sortValue = dynamicSortValue(field.valueSQL, field.kind == fieldKindDynamic)
			}
		case plan.SortValueModeLexical:
			sortValue = "toString(" + field.valueSQL + ")"
		default:
			return nil, nil, "", fmt.Errorf("compile ClickHouse sort: invalid value mode %d", key.Mode)
		}
		materialized = append(materialized, sortValue+" AS "+quoteIdentifier(alias))
		compiled = append(compiled, compiledSortKey{valueSQL: quoteIdentifier(alias), descending: key.Descending})
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
		return nil, nil, "", err
	}
	return materialized, compiled, order, nil
}

func dynamicSortValue(valueSQL string, dynamicValue bool) string {
	text := "toString(" + valueSQL + ")"
	number := finiteFloatOrNullSQL(text)
	if dynamicValue {
		dynamic := compiledScalar{
			valueSQL:       valueSQL,
			dynamicTypeSQL: "dynamicType(" + valueSQL + ")",
			kind:           fieldKindDynamic,
		}
		number = "ifNotFinite(" + numericScalarSQL(dynamic, false) + ", CAST(NULL AS Nullable(Float64)))"
	}
	integer := "accurateCastOrNull(" + text + ", 'Int256')"
	if dynamicValue {
		dynamic := compiledScalar{
			valueSQL:       valueSQL,
			dynamicTypeSQL: "dynamicType(" + valueSQL + ")",
			kind:           fieldKindDynamic,
		}
		integer = dynamicExactIntegerSQL(dynamic)
	}
	// Dynamic itself is intentionally forbidden in ClickHouse ORDER BY. A
	// fixed tuple also gives SPL-like numeric ordering for numeric values and
	// strings. The Int256 tie-break preserves adjacent integral values that
	// collapse to the same Float64 beyond 2^53. Nonnumeric scalars sort before
	// missing/explicit null.
	return "tuple(" +
		"if(isNull(" + valueSQL + "), toUInt8(2), if(isNotNull(" + number + "), toUInt8(0), toUInt8(1))), " +
		"ifNull(" + number + ", 0.), " +
		"if(isNotNull(" + integer + "), toUInt8(0), toUInt8(1)), " +
		"ifNull(" + integer + ", toInt256(0)), " +
		"ifNull(" + text + ", '')" +
		")"
}

func compileMaterializedOrder(keys []compiledSortKey, reverse bool) (string, error) {
	if len(keys) == 0 {
		return "", errors.New("compile ClickHouse sort: no keys")
	}
	parts := make([]string, 0, len(keys))
	for _, key := range keys {
		descending := key.descending
		nullsFirst := key.nullsFirst
		if reverse {
			descending = !descending
			nullsFirst = !nullsFirst
		}
		direction := "ASC"
		if descending {
			direction = "DESC"
		}
		nulls := "NULLS LAST"
		if nullsFirst {
			nulls = "NULLS FIRST"
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
		if field.valueSQL == publicName {
			projection = append(projection, publicName)
		} else {
			projection = append(projection, field.valueSQL+" AS "+publicName)
		}
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

func cloneSet(source map[string]struct{}) map[string]struct{} {
	result := make(map[string]struct{}, len(source))
	for key := range source {
		result[key] = struct{}{}
	}
	return result
}
