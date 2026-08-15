package clickhouse

import (
	"context"
	"errors"
	"fmt"
	"math"
	"slices"
	"strings"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/plan"
	"github.com/Suhaibinator/open-splunk/internal/spl"
)

const (
	// TimelineOrdinalColumn and TimelineCountColumn are the fixed transport
	// columns returned by CompileTimeline. They are private to the executor and
	// must not be interpreted as user-authored result fields. The ordinal keeps
	// epoch-aligned bucket timestamps out of DateTime64 conversions: an aligned
	// first bucket may precede ClickHouse's practical 1900 lower bound even
	// though every selected event is representable.
	TimelineOrdinalColumn = "__os_timeline_ordinal"
	TimelineCountColumn   = "__os_timeline_count"

	maxTimelineBuckets = uint64(10_000)
)

// TimelineSpec is the exact, minimal bucket cover for one search range.
// Earliest and Latest retain the half-open search boundaries so callers can
// mark clipped first and last buckets without inferring them from SQL rows.
type TimelineSpec struct {
	FirstBucket time.Time
	SpanSeconds int64
	BucketCount uint64
	Earliest    time.Time
	Latest      time.Time
}

// CompiledTimeline is executable SQL plus ordered bind arguments and its
// validated bucket contract.
type CompiledTimeline struct {
	SQL  string
	Args []any
	Spec TimelineSpec

	readScope          compiledReadScope
	executionAuthority *derivedExecutionAuthority
}

// CompileTimeline preserves the complete ordinary event pipeline, then counts
// its final rows over a bounded continuous grid. The plan-level eligibility
// proof is repeated here because callers may construct logical plans directly.
func (c Compiler) CompileTimeline(query *plan.Query, spec TimelineSpec) (CompiledTimeline, error) {
	return c.CompileTimelineContext(context.Background(), query, spec)
}

func (c Compiler) CompileTimelineContext(
	ctx context.Context,
	query *plan.Query,
	spec TimelineSpec,
) (CompiledTimeline, error) {
	if ctx == nil {
		return CompiledTimeline{}, errors.New("compile ClickHouse timeline: context is nil")
	}
	if err := ctx.Err(); err != nil {
		return CompiledTimeline{}, err
	}
	if err := plan.ValidateTimelineEligibility(query); err != nil {
		return CompiledTimeline{}, err
	}
	spanNanoseconds, err := validateTimelineSpec(spec)
	if err != nil {
		return CompiledTimeline{}, err
	}
	scan := query.Operators[0].(*plan.Scan)
	if !spec.Earliest.Equal(scan.Earliest) || !spec.Latest.Equal(scan.Latest) {
		return CompiledTimeline{}, errors.New("compile ClickHouse timeline: covered range does not match the Scan snapshot")
	}

	firstBucketNumber, gridOK := ordinalGridFirstBucketNumber(spec.FirstBucket.Unix(), spec.SpanSeconds, spec.BucketCount)
	if !gridOK {
		return CompiledTimeline{}, errors.New("compile ClickHouse timeline: grid bucket number overflows")
	}
	source, err := c.compileWithFinalizerContext(
		ctx,
		query,
		func(
			relation compiledRelation,
			state compileState,
			args []any,
			_ *plan.Scan,
			aliasSequence int,
		) (CompiledQuery, error) {
			_, outputFields, projectionErr := finalProjection(state)
			if projectionErr != nil {
				return CompiledQuery{}, projectionErr
			}
			if !timelineOutputContainsCanonicalTime(outputFields) {
				return CompiledQuery{}, &plan.Diagnostic{
					Code:        "SPL_UNSUPPORTED_TIMELINE_TIME_FIELD",
					Message:     "timeline requires the unmodified canonical _time field",
					Range:       query.Operators[0].SourceRange(),
					Suggestions: []string{"request the timeline before removing, replacing, or renaming _time"},
				}
			}
			return finalizeTimeline(
				relation,
				state,
				args,
				spanNanoseconds,
				firstBucketNumber,
				spec.BucketCount,
				scan.Range,
				aliasSequence,
			)
		},
		false,
	)
	if err != nil {
		return CompiledTimeline{}, err
	}
	compiled := CompiledTimeline{
		SQL:       source.SQL,
		Args:      source.Args,
		Spec:      spec,
		readScope: source.readScope,
	}
	compiled.executionAuthority, err = sealCompiledTimelineExecutionContext(
		ctx,
		source,
		compiled,
	)
	if err != nil {
		return CompiledTimeline{}, err
	}
	return compiled, nil
}

// finalizeTimeline consumes the proven event relation directly. In
// particular, it does not embed a separately finalized ordinary query: doing
// so would duplicate that query's chronological validation graph before the
// bucket aggregate. The shared finalizer keeps authored operators, runtime
// guards, and the timeline transport in one validation envelope.
func finalizeTimeline(
	relation compiledRelation,
	state compileState,
	args []any,
	spanNanoseconds int64,
	firstBucketNumber int64,
	bucketCount uint64,
	ownerRange spl.Range,
	aliasSequence int,
) (CompiledQuery, error) {
	q := quoteIdentifier
	source := q("__os_timeline_source")
	input := q("__os_timeline_input")
	prepared := q("__os_timeline_prepared")
	counts := q("__os_timeline_counts")
	grid := q("__os_timeline_grid")
	eventTime := q("__os_timeline_event_time")
	ticks := q("__os_timeline_ticks")
	bucketNumber := q("__os_timeline_bucket_number")
	ordinal := q(TimelineOrdinalColumn)
	count := q(TimelineCountColumn)

	var sql strings.Builder
	sql.Grow(len(relation.sql) + 1_536)
	sql.WriteString("WITH ")
	sql.WriteString(source)
	sql.WriteString(" AS (SELECT ")
	sql.WriteString(q("_time"))
	sql.WriteString(" AS ")
	sql.WriteString(eventTime)
	sql.WriteString(" FROM (")
	sql.WriteString(relation.sql)
	sql.WriteString(") AS ")
	sql.WriteString(input)
	sql.WriteString("), ")

	sql.WriteString(prepared)
	sql.WriteString(" AS (SELECT reinterpretAsInt64(")
	sql.WriteString(eventTime)
	sql.WriteString(") AS ")
	sql.WriteString(ticks)
	sql.WriteString(" FROM ")
	sql.WriteString(source)
	sql.WriteString("), ")

	sql.WriteString(counts)
	sql.WriteString(" AS (SELECT ")
	sql.WriteString(epochFloorBucketNumberSQL(ticks))
	sql.WriteString(" AS ")
	sql.WriteString(bucketNumber)
	sql.WriteString(", count() AS ")
	sql.WriteString(count)
	sql.WriteString(" FROM ")
	sql.WriteString(prepared)
	sql.WriteString(" GROUP BY ")
	sql.WriteString(bucketNumber)
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
	sql.WriteString(" FROM ")
	sql.WriteString(grid)
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

	sourceDepth := relationalNodeDepth(relation.depth)
	preparedDepth := relationalNodeDepth(sourceDepth)
	countsDepth := relationalNodeDepth(preparedDepth)
	gridDepth := relationalNodeDepth()
	resultDepth := relationalNodeDepth(gridDepth, countsDepth)
	compiled := withCompiledRelationalDepth(
		CompiledQuery{
			SQL: sql.String(),
			Args: appendOrdinalGridArgs(
				append([]any(nil), args...),
				spanNanoseconds,
				firstBucketNumber,
				bucketCount,
			),
		},
		resultDepth,
		ownerRange,
	)
	if len(state.chronologicalBarriers) == 0 {
		return compiled, nil
	}
	return wrapChronologicalValidation(
		compiled.SQL,
		compiled.relationalDepth,
		ownerRange,
		state.chronologicalBarriers,
		[]string{ordinal, count},
		[]string{TimelineOrdinalColumn, TimelineCountColumn},
		ordinal+" ASC",
		eventStatsOrdinarySourceFanout,
		compiled,
		aliasSequence,
	)
}

func validateTimelineSpec(spec TimelineSpec) (int64, error) {
	return validateFixedTimeGridSpec(spec, "timeline")
}

func validateFixedTimeGridSpec(spec TimelineSpec, operation string) (int64, error) {
	prefix := "compile ClickHouse " + operation + ": "
	if !validTimelineFirstBucket(spec.FirstBucket) || !validTimelineRangeBoundary(spec.Earliest) || !validTimelineRangeBoundary(spec.Latest) {
		return 0, errors.New(prefix + "boundaries must be nonzero UTC timestamps and first bucket must use whole seconds")
	}
	if !SupportsSearchTimeRange(spec.Earliest, spec.Latest) {
		return 0, errors.New(prefix + "covered range is outside the supported DateTime64(9) bounds")
	}
	if spec.SpanSeconds <= 0 || spec.SpanSeconds > math.MaxInt64/int64(time.Second) {
		return 0, errors.New(prefix + "span seconds are out of range")
	}
	if spec.BucketCount == 0 || spec.BucketCount > maxTimelineBuckets {
		return 0, fmt.Errorf("%sbucket count must be between 1 and %d", prefix, maxTimelineBuckets)
	}
	if spec.FirstBucket.Unix()%spec.SpanSeconds != 0 {
		return 0, errors.New(prefix + "first bucket is not epoch aligned")
	}
	if !spec.Earliest.Before(spec.Latest) {
		return 0, errors.New(prefix + "covered range must be nonempty")
	}

	bucketCount := int64(spec.BucketCount)
	if spec.SpanSeconds > math.MaxInt64/bucketCount {
		return 0, errors.New(prefix + "covered seconds overflow")
	}
	coveredSeconds := spec.SpanSeconds * bucketCount
	firstSecond := spec.FirstBucket.Unix()
	if firstSecond > math.MaxInt64-coveredSeconds {
		return 0, errors.New(prefix + "grid end overflows Unix seconds")
	}
	gridEndSecond := firstSecond + coveredSeconds
	firstBucketEnd := time.Unix(firstSecond+spec.SpanSeconds, 0).UTC()
	lastBucketStart := time.Unix(gridEndSecond-spec.SpanSeconds, 0).UTC()
	gridEnd := time.Unix(gridEndSecond, 0).UTC()
	if spec.Earliest.Before(spec.FirstBucket) || !spec.Earliest.Before(firstBucketEnd) {
		return 0, errors.New(prefix + "earliest must be inside the first bucket")
	}
	if !spec.Latest.After(lastBucketStart) || spec.Latest.After(gridEnd) {
		return 0, errors.New(prefix + "latest must be inside the final bucket cover")
	}
	if _, ok := ordinalGridFirstBucketNumber(firstSecond, spec.SpanSeconds, spec.BucketCount); !ok {
		return 0, errors.New(prefix + "grid bucket number overflows")
	}

	return spec.SpanSeconds * int64(time.Second), nil
}

func validTimelineFirstBucket(value time.Time) bool {
	return !value.IsZero() && value.Location() == time.UTC && value.Nanosecond() == 0
}

func validTimelineRangeBoundary(value time.Time) bool {
	return !value.IsZero() && value.Location() == time.UTC
}

func timelineOutputContainsCanonicalTime(fields []string) bool {
	return slices.Contains(fields, "_time")
}
