// Package searchanalysis provides bounded, on-demand analyses derived from an
// immutable completed search execution scope.
package searchanalysis

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
	"github.com/Suhaibinator/open-splunk/internal/plan"
	"github.com/Suhaibinator/open-splunk/internal/queryexec"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
	"github.com/Suhaibinator/open-splunk/internal/searchsnapshot"
)

const (
	defaultTimelineMaxBuckets = uint32(100)
	defaultMaximumBuckets     = uint32(1_000)
	defaultTimelineConcurrent = 4
	maximumTimelineConcurrent = 64
	defaultTimelineRuntime    = 15 * time.Second
	maximumTimelineRuntime    = 2 * time.Minute
	maximumTimelineJobIDBytes = 1 << 10
)

var (
	// ErrTimelineUnsupported means the completed SPL result no longer consists
	// of identifiable source events with their original canonical _time.
	ErrTimelineUnsupported = errors.New("search timeline is unsupported for this SPL")
	// ErrTimelineCapacity means every bounded analysis execution slot is busy.
	ErrTimelineCapacity = errors.New("search timeline capacity is exhausted")
)

// Request selects one completed search and a bounded fixed-width timeline.
// Pointer options distinguish omission from explicitly invalid zero values.
type Request struct {
	SearchJobID                 string
	MaxBuckets                  *uint32
	PreferredBucketWidthSeconds *int64
}

// Bucket is one clipped interval from an epoch-aligned fixed-width grid.
// Intervals are half-open and EventCount may be zero.
type Bucket struct {
	Earliest   time.Time
	Latest     time.Time
	EventCount uint64
	Partial    bool
}

// Result is returned atomically only after the complete fixed grid validates.
type Result struct {
	Buckets            []Bucket
	BucketWidthSeconds int64
	Complete           bool
}

type completedSearches interface {
	CompletedExecutionSnapshotFor(context.Context, searchjobs.AccessScope, string) (searchjobs.ExecutionSnapshot, error)
}

type timelineCompiler interface {
	CompileTimeline(*plan.Query, clickhouse.TimelineSpec) (clickhouse.CompiledTimeline, error)
}

type timelineExecutor interface {
	ExecuteTimeline(context.Context, clickhouse.CompiledTimeline) ([]queryexec.TimelineBucket, error)
}

// Config controls synchronous timeline admission and execution. Zero limits
// select conservative defaults.
type Config struct {
	Searches          completedSearches
	Compiler          timelineCompiler
	Executor          timelineExecutor
	DefaultMaxBuckets uint32
	MaximumBuckets    uint32
	MaxConcurrent     int
	MaxRuntime        time.Duration
}

// Service executes bounded timelines against completed immutable searches.
type Service struct {
	searches          completedSearches
	compiler          timelineCompiler
	executor          timelineExecutor
	defaultMaxBuckets uint32
	maximumBuckets    uint32
	maxRuntime        time.Duration
	gate              chan struct{}
}

// New validates dependencies and constructs a timeline service.
func New(config Config) (*Service, error) {
	if nilInterface(config.Searches) {
		return nil, errors.New("create search timeline service: completed search snapshots are required")
	}
	if nilInterface(config.Compiler) {
		return nil, errors.New("create search timeline service: timeline compiler is required")
	}
	if nilInterface(config.Executor) {
		return nil, errors.New("create search timeline service: timeline executor is required")
	}
	if config.MaximumBuckets == 0 {
		config.MaximumBuckets = defaultMaximumBuckets
	}
	if config.MaximumBuckets > absoluteMaximumTimelineBuckets {
		return nil, fmt.Errorf("create search timeline service: maximum buckets cannot exceed %d", absoluteMaximumTimelineBuckets)
	}
	if config.DefaultMaxBuckets == 0 {
		config.DefaultMaxBuckets = min(defaultTimelineMaxBuckets, config.MaximumBuckets)
	}
	if config.DefaultMaxBuckets > config.MaximumBuckets {
		return nil, errors.New("create search timeline service: default buckets exceed the maximum")
	}
	if config.MaxConcurrent < 0 || config.MaxConcurrent > maximumTimelineConcurrent {
		return nil, fmt.Errorf("create search timeline service: concurrent limit must not exceed %d", maximumTimelineConcurrent)
	}
	if config.MaxConcurrent == 0 {
		config.MaxConcurrent = defaultTimelineConcurrent
	}
	if config.MaxRuntime < 0 || config.MaxRuntime > maximumTimelineRuntime {
		return nil, fmt.Errorf("create search timeline service: runtime must not exceed %s", maximumTimelineRuntime)
	}
	if config.MaxRuntime == 0 {
		config.MaxRuntime = defaultTimelineRuntime
	}
	return &Service{
		searches:          config.Searches,
		compiler:          config.Compiler,
		executor:          config.Executor,
		defaultMaxBuckets: config.DefaultMaxBuckets,
		maximumBuckets:    config.MaximumBuckets,
		maxRuntime:        config.MaxRuntime,
		gate:              make(chan struct{}, config.MaxConcurrent),
	}, nil
}

// MaximumBuckets returns the exact per-request bound enforced by Get.
func (service *Service) MaximumBuckets() uint32 {
	if service == nil {
		return 0
	}
	return service.maximumBuckets
}

// Get re-executes an eligible event pipeline at its original security and
// storage snapshot, then returns a continuous timeline. Later inserts cannot
// enter the result; physical TTL deletion may remove older retained events.
func (service *Service) Get(ctx context.Context, access searchjobs.AccessScope, request Request) (Result, error) {
	if service == nil {
		return Result{}, errors.New("get search timeline: service is nil")
	}
	if ctx == nil {
		return Result{}, errors.New("get search timeline: context is nil")
	}
	if err := ctx.Err(); err != nil {
		return Result{}, err
	}
	id, maxBuckets, preferredSeconds, err := service.normalizeRequest(request)
	if err != nil {
		return Result{}, err
	}
	select {
	case service.gate <- struct{}{}:
		defer func() { <-service.gate }()
	default:
		return Result{}, ErrTimelineCapacity
	}

	executionContext, cancel := context.WithTimeout(ctx, service.maxRuntime)
	defer cancel()
	snapshot, err := service.searches.CompletedExecutionSnapshotFor(executionContext, access, id)
	if contextErr := executionContext.Err(); contextErr != nil {
		return Result{}, contextErr
	}
	if err != nil {
		return Result{}, err
	}
	if snapshot.ID != id || snapshot.TenantID != access.TenantID || snapshot.OwnerID != access.OwnerID {
		return Result{}, fmt.Errorf("%w: completed execution snapshot identity changed", searchjobs.ErrInvalidResult)
	}

	geometry, err := selectTimelineGeometry(snapshot.Earliest, snapshot.Latest, maxBuckets, preferredSeconds)
	if err != nil {
		return Result{}, err
	}
	logical, err := searchsnapshot.BuildExecutionPlan(snapshot)
	if err != nil {
		return Result{}, fmt.Errorf("rebuild completed search for timeline: %w", err)
	}
	if err := plan.ValidateTimelineEligibility(logical); err != nil {
		return Result{}, fmt.Errorf("%w: %v", ErrTimelineUnsupported, err)
	}
	spec := clickhouse.TimelineSpec{
		FirstBucket: geometry.FirstBucket,
		SpanSeconds: geometry.SpanSeconds,
		BucketCount: geometry.BucketCount,
		Earliest:    snapshot.Earliest,
		Latest:      snapshot.Latest,
	}
	compiled, err := service.compiler.CompileTimeline(logical, spec)
	if err != nil {
		var diagnostic *plan.Diagnostic
		if errors.As(err, &diagnostic) {
			switch {
			case diagnostic.Code == "SPL_QUERY_TOO_COMPLEX":
				return Result{}, fmt.Errorf("%w: compile completed search timeline: %v", searchjobs.ErrExecutionLimit, err)
			case strings.HasPrefix(diagnostic.Code, "SPL_UNSUPPORTED_TIMELINE_"):
				return Result{}, fmt.Errorf("%w: %v", ErrTimelineUnsupported, err)
			}
		}
		return Result{}, fmt.Errorf("compile completed search timeline: %w", err)
	}
	if compiled.Spec != spec {
		return Result{}, fmt.Errorf("%w: timeline compiler changed the bucket contract", searchjobs.ErrInvalidResult)
	}
	rows, err := service.executor.ExecuteTimeline(executionContext, compiled)
	if contextErr := executionContext.Err(); contextErr != nil {
		return Result{}, contextErr
	}
	if err != nil {
		return Result{}, err
	}
	return buildTimelineResult(executionContext, spec, rows)
}

func (service *Service) normalizeRequest(request Request) (string, uint32, int64, error) {
	id := strings.TrimSpace(request.SearchJobID)
	if id == "" || len(id) > maximumTimelineJobIDBytes {
		return "", 0, 0, ErrInvalidTimelineRequest
	}
	maxBuckets := service.defaultMaxBuckets
	if request.MaxBuckets != nil {
		maxBuckets = *request.MaxBuckets
		if maxBuckets == 0 || maxBuckets > service.maximumBuckets {
			return "", 0, 0, ErrInvalidTimelineRequest
		}
	}
	preferredSeconds := int64(0)
	if request.PreferredBucketWidthSeconds != nil {
		preferredSeconds = *request.PreferredBucketWidthSeconds
		if preferredSeconds <= 0 || preferredSeconds > maximumTimelineSpanSeconds {
			return "", 0, 0, ErrInvalidTimelineRequest
		}
	}
	return id, maxBuckets, preferredSeconds, nil
}

func buildTimelineResult(ctx context.Context, spec clickhouse.TimelineSpec, rows []queryexec.TimelineBucket) (Result, error) {
	if uint64(len(rows)) != spec.BucketCount {
		return Result{}, fmt.Errorf("%w: timeline executor returned an incomplete grid", searchjobs.ErrInvalidResult)
	}
	buckets := make([]Bucket, len(rows))
	for index, row := range rows {
		if err := ctx.Err(); err != nil {
			return Result{}, err
		}
		alignedStart := time.Unix(spec.FirstBucket.Unix()+int64(index)*spec.SpanSeconds, 0).UTC()
		if !row.AlignedStart.Equal(alignedStart) || row.AlignedStart.Location() != time.UTC {
			return Result{}, fmt.Errorf("%w: timeline executor returned an unexpected bucket", searchjobs.ErrInvalidResult)
		}
		alignedEnd := time.Unix(alignedStart.Unix()+spec.SpanSeconds, 0).UTC()
		earliest := alignedStart
		if earliest.Before(spec.Earliest) {
			earliest = spec.Earliest
		}
		latest := alignedEnd
		if latest.After(spec.Latest) {
			latest = spec.Latest
		}
		if !earliest.Before(latest) {
			return Result{}, fmt.Errorf("%w: timeline bucket does not intersect the search range", searchjobs.ErrInvalidResult)
		}
		buckets[index] = Bucket{
			Earliest:   earliest,
			Latest:     latest,
			EventCount: row.Count,
			Partial:    !earliest.Equal(alignedStart) || !latest.Equal(alignedEnd),
		}
	}
	if err := ctx.Err(); err != nil {
		return Result{}, err
	}
	return Result{Buckets: buckets, BucketWidthSeconds: spec.SpanSeconds, Complete: true}, nil
}

func nilInterface(value any) bool {
	if value == nil {
		return true
	}
	reflected := reflect.ValueOf(value)
	switch reflected.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return reflected.IsNil()
	default:
		return false
	}
}
