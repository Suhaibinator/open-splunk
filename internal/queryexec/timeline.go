package queryexec

import (
	"context"
	"errors"
	"fmt"
	"math"
	"slices"
	"strings"
	"time"

	"fortio.org/safecast"
	clickhousedriver "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
)

const (
	maximumTimelineResultBuckets = uint64(10_000)
	maximumTimelineResultBytes   = uint64(1 << 20)
	maximumTimelineSpanSeconds   = math.MaxInt64 / int64(time.Second)
	timelineOrdinalDatabaseType  = "UInt64"
	timelineCountDatabaseType    = "UInt64"
)

// TimelineBucket is one epoch-aligned interval in a completed search's event
// timeline. AlignedStart is inclusive and the next bucket start is exclusive.
type TimelineBucket struct {
	AlignedStart time.Time
	Count        uint64
}

// ExecuteTimeline reads a compiler-produced, fixed-schema timeline grid. It
// validates and buffers the complete grid before returning it so malformed or
// interrupted ClickHouse results can never be observed partially.
func (executor *Executor) ExecuteTimeline(ctx context.Context, query clickhouse.CompiledTimeline) (buckets []TimelineBucket, resultErr error) {
	if ctx == nil {
		return nil, errors.New("execute ClickHouse timeline: context is nil")
	}
	if executor == nil || isNilDriverValue(executor.connection) {
		return nil, errors.New("execute ClickHouse timeline: executor connection is required")
	}
	if executor.newQueryID == nil {
		return nil, errors.New("execute ClickHouse timeline: query ID generator is required")
	}
	if executor.readAdmission != nil {
		detached, ok, cloneErr := query.CloneForExecutionContext(ctx)
		if cloneErr != nil {
			return nil, cloneErr
		}
		if !ok {
			return nil, fmt.Errorf(
				"%w: compiled timeline execution authority is invalid",
				searchjobs.ErrInvalidResult,
			)
		}
		query = detached
	}
	query.Args = slices.Clone(query.Args)
	if err := validateCompiledTimeline(query); err != nil {
		return nil, err
	}
	admittedContext, releaseRead, err := executor.acquireRead(ctx, query, "execute ClickHouse timeline")
	if err != nil {
		return nil, err
	}
	defer releaseRead()
	defer func() {
		resultErr = preserveReadCancellationCause(admittedContext, resultErr)
		if resultErr != nil {
			buckets = nil
		}
	}()
	ctx = admittedContext
	settings, err := settingsForTimeline(executor.baseSettings(), query.Spec.BucketCount)
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	externalTables, err := query.ExternalTablesForExecution(ctx)
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, ctxErr
		}
		return nil, fmt.Errorf(
			"%w: compiled timeline lookup transport is invalid",
			searchjobs.ErrInvalidResult,
		)
	}

	rows, err := executor.issueRead(ctx, "timeline", query.SQL, query.Args, settings, externalTables)
	if err != nil {
		return nil, err
	}

	rowsClosed := false
	defer closeReadStream(ctx, rows, "timeline", &rowsClosed, &buckets, &resultErr)

	if err := ctx.Err(); err != nil {
		return nil, err
	}
	columns := rows.Columns()
	columnTypes := rows.ColumnTypes()
	if err := validateTimelineColumns(columns, columnTypes); err != nil {
		return nil, err
	}

	bucketCapacity := safecast.MustConv[int](query.Spec.BucketCount)
	buckets = make([]TimelineBucket, 0, bucketCapacity)
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if !rows.Next() {
			break
		}
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if uint64(len(buckets)) >= query.Spec.BucketCount {
			return nil, fmt.Errorf("%w: ClickHouse timeline returned too many buckets", searchjobs.ErrInvalidResult)
		}

		var ordinal uint64
		var count uint64
		if err := rows.Scan(&ordinal, &count); err != nil {
			return nil, classifyQueryError(ctx, fmt.Errorf("scan ClickHouse timeline result row: %w", err))
		}
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		expectedOrdinal := uint64(len(buckets))
		if ordinal != expectedOrdinal {
			return nil, fmt.Errorf("%w: ClickHouse timeline bucket sequence is invalid", searchjobs.ErrInvalidResult)
		}
		expectedUnix, ok := checkedBucketBoundary(query.Spec.FirstBucket.Unix(), query.Spec.SpanSeconds, expectedOrdinal)
		if !ok {
			return nil, fmt.Errorf("%w: compiled timeline bucket arithmetic overflowed", searchjobs.ErrInvalidResult)
		}
		expected := time.Unix(expectedUnix, 0).UTC()
		buckets = append(buckets, TimelineBucket{AlignedStart: expected, Count: count})
	}
	if err := rows.Err(); err != nil {
		return nil, classifyQueryError(ctx, fmt.Errorf("iterate ClickHouse timeline results: %w", err))
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if uint64(len(buckets)) != query.Spec.BucketCount {
		return nil, fmt.Errorf("%w: ClickHouse timeline returned an incomplete bucket sequence", searchjobs.ErrInvalidResult)
	}

	rowsClosed = true
	if err := rows.Close(); err != nil {
		return nil, classifyQueryError(ctx, fmt.Errorf("close ClickHouse timeline result stream: %w", err))
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return buckets, nil
}

func validateCompiledTimeline(query clickhouse.CompiledTimeline) error {
	if strings.TrimSpace(query.SQL) == "" {
		return fmt.Errorf("%w: compiled timeline SQL is empty", searchjobs.ErrInvalidResult)
	}
	spec := query.Spec
	if spec.FirstBucket.IsZero() || spec.FirstBucket.Location() != time.UTC || spec.FirstBucket.Nanosecond() != 0 ||
		spec.SpanSeconds <= 0 || spec.SpanSeconds > maximumTimelineSpanSeconds ||
		spec.BucketCount == 0 || spec.BucketCount > maximumTimelineResultBuckets ||
		spec.Earliest.IsZero() || spec.Latest.IsZero() || spec.Earliest.Location() != time.UTC || spec.Latest.Location() != time.UTC ||
		!spec.Earliest.Before(spec.Latest) || !clickhouse.SupportsSearchTimeRange(spec.Earliest, spec.Latest) {
		return fmt.Errorf("%w: compiled timeline specification is invalid", searchjobs.ErrInvalidResult)
	}

	firstUnix := spec.FirstBucket.Unix()
	if firstUnix%spec.SpanSeconds != 0 {
		return fmt.Errorf("%w: compiled timeline bucket origin is not epoch-aligned", searchjobs.ErrInvalidResult)
	}
	secondUnix, secondOK := checkedBucketBoundary(firstUnix, spec.SpanSeconds, 1)
	lastUnix, lastOK := checkedBucketBoundary(firstUnix, spec.SpanSeconds, spec.BucketCount-1)
	endUnix, endOK := checkedBucketBoundary(firstUnix, spec.SpanSeconds, spec.BucketCount)
	if !secondOK || !lastOK || !endOK {
		return fmt.Errorf("%w: compiled timeline bucket arithmetic overflowed", searchjobs.ErrInvalidResult)
	}
	second := time.Unix(secondUnix, 0).UTC()
	last := time.Unix(lastUnix, 0).UTC()
	end := time.Unix(endUnix, 0).UTC()
	if spec.Earliest.Before(spec.FirstBucket) || !spec.Earliest.Before(second) ||
		!last.Before(spec.Latest) || end.Before(spec.Latest) {
		return fmt.Errorf("%w: compiled timeline buckets are not the minimal range cover", searchjobs.ErrInvalidResult)
	}
	return nil
}

func checkedBucketBoundary(first, span int64, multiplier uint64) (int64, bool) {
	if span <= 0 || multiplier > (^uint64(0)>>1)/uint64(span) {
		return 0, false
	}

	offset := safecast.MustConv[int64](multiplier * safecast.MustConv[uint64](span))
	if offset > 0 && first > int64(^uint64(0)>>1)-offset {
		return 0, false
	}
	return first + offset, true
}

func settingsForTimeline(base *validatedExecutorSettings, bucketCount uint64) (clickhousedriver.Settings, error) {
	if bucketCount == 0 || bucketCount > maximumTimelineResultBuckets {
		return nil, errors.New("execute ClickHouse timeline: bucket limit is invalid")
	}
	if base == nil {
		return nil, errors.New("execute ClickHouse timeline: executor does not have read-only settings")
	}
	baseRows := base.limit("max_result_rows")
	baseBytes := base.limit("max_result_bytes")
	settings := base.clone()
	settings["max_result_rows"] = min(baseRows, bucketCount)
	settings["max_result_bytes"] = min(baseBytes, maximumTimelineResultBytes)
	return settings, nil
}

func validateTimelineColumns(columns []string, columnTypes []driver.ColumnType) error {
	contracts := []resultColumnContract{
		{name: clickhouse.TimelineOrdinalColumn, databaseType: timelineOrdinalDatabaseType},
		{name: clickhouse.TimelineCountColumn, databaseType: timelineCountDatabaseType},
	}
	return validateResultColumns(columns, columnTypes, contracts, 0, "ClickHouse timeline")
}
