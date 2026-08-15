//go:build !windows

package integration_test

import (
	"context"
	"fmt"
	"math"
	"net/http"
	"slices"
	"strings"
	"testing"
	"time"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type backendLoadSearchObservation struct {
	JobID         string
	CreatedAt     time.Time
	StartedAt     time.Time
	FinishedAt    time.Time
	LifecycleTime time.Duration
	Elapsed       time.Duration
	QueueWait     time.Duration
	ScannedRows   uint64
	ScannedBytes  uint64
	Events        uint64
	EventIDs      uint64
	RequestIDs    uint64
	UserIDs       uint64
	EventTimes    uint64
	FirstEventAt  time.Time
	LastEventAt   time.Time
}

type backendLoadSearchSpec struct {
	SPL       string
	IndexName string
	Timezone  string
	Earliest  time.Time
	Latest    time.Time
}

func newBackendLoadSearchSpec(plan backendLoadPlan, fixtureStart time.Time) backendLoadSearchSpec {
	return backendLoadSearchSpec{
		SPL: fmt.Sprintf(
			`index=%q | stats count AS events dc(event_id) AS event_ids dc(request_id) AS request_ids dc(user_id) AS user_ids dc(_time) AS event_times min(_time) AS first_event_at max(_time) AS last_event_at`,
			plan.IndexName,
		),
		IndexName: plan.IndexName,
		Timezone:  "UTC",
		Earliest:  fixtureStart,
		Latest:    fixtureStart.Add(time.Duration(plan.eventCount()) * plan.interval()),
	}
}

func (spec backendLoadSearchSpec) request() *opensplunkv1.CreateSearchJobRequest {
	earliest := spec.Earliest.Format(time.RFC3339Nano)
	latest := spec.Latest.Format(time.RFC3339Nano)
	timezone := spec.Timezone
	return &opensplunkv1.CreateSearchJobRequest{
		Definition: &opensplunkv1.SearchDefinition{
			Spl: spec.SPL,
			TimeRange: &opensplunkv1.TimeRangeSpec{
				Earliest: &earliest,
				Latest:   &latest,
				Timezone: &timezone,
			},
			IndexScope: []string{spec.IndexName},
		},
	}
}

type backendLoadSearchExpectation struct {
	MinimumEvents  uint64
	MaximumEvents  uint64
	MaximumUserIDs uint64
	FixtureStart   time.Time
	EventInterval  time.Duration
}

type backendLoadSearchAdmission struct {
	JobID string
}

type backendLoadSearchCohort struct {
	ReleasedAt         time.Time
	CollectedAt        time.Time
	LifecycleStartedAt time.Time
	LifecycleEndedAt   time.Time
	Searches           []backendLoadSearchObservation
}

type backendLoadConcurrentSearchWindow struct {
	Cohorts      []backendLoadSearchCohort
	SourceBefore backendLoadSourceProgress
	SourceAfter  backendLoadSourceProgress
	StoredBefore uint64
	StoredAfter  uint64
}

const (
	backendLoadEventsColumn = iota
	backendLoadEventIDsColumn
	backendLoadRequestIDsColumn
	backendLoadUserIDsColumn
	backendLoadEventTimesColumn
	backendLoadFirstEventAtColumn
	backendLoadLastEventAtColumn
)

var backendLoadSearchAggregateColumns = [...]struct {
	name      string
	valueType opensplunkv1.ValueType
	nullable  bool
}{
	{name: "events", valueType: opensplunkv1.ValueType_VALUE_TYPE_UINT64},
	{name: "event_ids", valueType: opensplunkv1.ValueType_VALUE_TYPE_UINT64},
	{name: "request_ids", valueType: opensplunkv1.ValueType_VALUE_TYPE_UINT64},
	{name: "user_ids", valueType: opensplunkv1.ValueType_VALUE_TYPE_UINT64},
	{name: "event_times", valueType: opensplunkv1.ValueType_VALUE_TYPE_UINT64},
	{name: "first_event_at", valueType: opensplunkv1.ValueType_VALUE_TYPE_TIMESTAMP, nullable: true},
	{name: "last_event_at", valueType: opensplunkv1.ValueType_VALUE_TYPE_TIMESTAMP, nullable: true},
}

func runBackendLoadSearch(
	t *testing.T,
	ctx context.Context,
	client *http.Client,
	baseURL string,
	plan backendLoadPlan,
	fixtureStart time.Time,
	source backendLoadSourceCorpus,
) backendLoadSearchObservation {
	t.Helper()
	spec := newBackendLoadSearchSpec(plan, fixtureStart)
	admission, err := createBackendLoadSearch(ctx, client, baseURL, spec)
	if err != nil {
		t.Fatal(err)
	}
	observation := collectBackendLoadSearch(t, ctx, client, baseURL, spec, admission)
	if err := validateBackendLoadSearchPrefix(observation, backendLoadSearchExpectation{
		MinimumEvents:  plan.eventCount(),
		MaximumEvents:  plan.eventCount() + 1,
		MaximumUserIDs: plan.Cardinality,
		FixtureStart:   fixtureStart,
		EventInterval:  plan.interval(),
	}); err != nil {
		t.Fatal(err)
	}
	if observation.RequestIDs != uint64(len(source.RequestIDs)) ||
		observation.UserIDs != uint64(len(source.UserIDs)) {
		t.Fatalf(
			"backend load aggregate request/user IDs = %d/%d, want %d/%d",
			observation.RequestIDs,
			observation.UserIDs,
			len(source.RequestIDs),
			len(source.UserIDs),
		)
	}
	return observation
}

func createBackendLoadSearch(
	ctx context.Context,
	client *http.Client,
	baseURL string,
	spec backendLoadSearchSpec,
) (backendLoadSearchAdmission, error) {
	var created opensplunkv1.CreateSearchJobResponse
	if _, err := postProtoRequest(
		ctx,
		client,
		baseURL+"/api/v1/search/jobs/create",
		spec.request(),
		&created,
	); err != nil {
		return backendLoadSearchAdmission{}, err
	}
	jobID := created.GetSearchJob().GetSearchJobId()
	if jobID == "" {
		return backendLoadSearchAdmission{}, fmt.Errorf(
			"created backend load search has no job ID: %+v",
			created.GetSearchJob(),
		)
	}
	return backendLoadSearchAdmission{JobID: jobID}, nil
}

func collectBackendLoadSearch(
	t *testing.T,
	ctx context.Context,
	client *http.Client,
	baseURL string,
	spec backendLoadSearchSpec,
	admission backendLoadSearchAdmission,
) backendLoadSearchObservation {
	t.Helper()
	completed := waitForCompletedSearch(t, ctx, client, baseURL, admission.JobID, 60*time.Second)
	progress := completed.GetProgress()
	resolved := completed.GetResolvedTimeRange()
	if completed.GetSearchJobId() != admission.JobID ||
		completed.GetDefinition().GetSpl() != spec.SPL ||
		completed.GetResultsTruncated() ||
		progress.GetProducedRows() != 1 ||
		progress.GetScannedRows() == 0 ||
		progress.GetScannedBytes() == 0 ||
		progress.GetCountersAreEstimates() ||
		!slices.Equal(completed.GetEffectiveIndexScope(), []string{spec.IndexName}) ||
		resolved == nil ||
		resolved.GetTimezone() != spec.Timezone {
		t.Fatalf("completed backend load search = %+v", completed)
	}
	resolvedEarliest := validBackendLoadTimestamp(t, "resolved earliest", resolved.GetEarliest())
	resolvedLatest := validBackendLoadTimestamp(t, "resolved latest", resolved.GetLatest())
	if !resolvedEarliest.Equal(spec.Earliest) || !resolvedLatest.Equal(spec.Latest) {
		t.Fatalf(
			"backend load resolved range = [%s,%s), want [%s,%s)",
			resolvedEarliest,
			resolvedLatest,
			spec.Earliest,
			spec.Latest,
		)
	}

	createdAt := validBackendLoadTimestamp(t, "created at", completed.GetCreatedAt())
	startedAt := validBackendLoadTimestamp(t, "started at", completed.GetStartedAt())
	finishedAt := validBackendLoadTimestamp(t, "finished at", completed.GetFinishedAt())
	_ = validBackendLoadTimestamp(t, "index-time cutoff", completed.GetIndexTimeCutoff())
	if createdAt.After(startedAt) || !startedAt.Before(finishedAt) {
		t.Fatalf(
			"backend load search lifecycle timestamps: created=%s started=%s finished=%s",
			createdAt,
			startedAt,
			finishedAt,
		)
	}

	results := fetchAllCompletedSearchResults(t, ctx, client, baseURL, admission.JobID, 1, 1)
	columns := results.schema.GetColumns()
	if completed.GetResultKind() != opensplunkv1.ResultSetKind_RESULT_SET_KIND_STATISTICS ||
		results.schema.GetResultKind() != opensplunkv1.ResultSetKind_RESULT_SET_KIND_STATISTICS ||
		len(columns) != len(backendLoadSearchAggregateColumns) ||
		len(results.rows) != 1 ||
		len(results.rows[0].GetCells()) != len(backendLoadSearchAggregateColumns) {
		t.Fatalf("backend load aggregate result = %+v rows=%+v", results.schema, results.rows)
	}
	for index, want := range backendLoadSearchAggregateColumns {
		column := columns[index]
		if column.GetFieldName() != want.name ||
			column.GetValueType() != want.valueType ||
			column.GetNullable() != want.nullable ||
			column.GetMultivalue() {
			t.Fatalf(
				"backend load aggregate column %d = %+v, want type=%s nullable=%t name=%q",
				index,
				column,
				want.valueType,
				want.nullable,
				want.name,
			)
		}
	}
	cells := results.rows[0].GetCells()
	unsigned := func(index int) uint64 {
		t.Helper()
		value := cells[index]
		if _, ok := value.GetKind().(*opensplunkv1.TypedValue_Uint64Value); !ok {
			t.Fatalf(
				"backend load aggregate cell %q = %+v, want UInt64",
				backendLoadSearchAggregateColumns[index].name,
				value,
			)
		}
		return value.GetUint64Value()
	}
	timestamp := func(index int) time.Time {
		t.Helper()
		value := cells[index]
		if _, ok := value.GetKind().(*opensplunkv1.TypedValue_TimestampValue); !ok {
			t.Fatalf(
				"backend load aggregate cell %q = %+v, want Timestamp",
				backendLoadSearchAggregateColumns[index].name,
				value,
			)
		}
		return validBackendLoadTimestamp(
			t,
			backendLoadSearchAggregateColumns[index].name,
			value.GetTimestampValue(),
		)
	}

	return backendLoadSearchObservation{
		JobID:         admission.JobID,
		CreatedAt:     createdAt,
		StartedAt:     startedAt,
		FinishedAt:    finishedAt,
		LifecycleTime: finishedAt.Sub(createdAt),
		Elapsed:       validBackendLoadDuration(t, "elapsed", progress.GetElapsed()),
		QueueWait:     validBackendLoadDuration(t, "queue wait", progress.GetQueueWait()),
		ScannedRows:   progress.GetScannedRows(),
		ScannedBytes:  progress.GetScannedBytes(),
		Events:        unsigned(backendLoadEventsColumn),
		EventIDs:      unsigned(backendLoadEventIDsColumn),
		RequestIDs:    unsigned(backendLoadRequestIDsColumn),
		UserIDs:       unsigned(backendLoadUserIDsColumn),
		EventTimes:    unsigned(backendLoadEventTimesColumn),
		FirstEventAt:  timestamp(backendLoadFirstEventAtColumn),
		LastEventAt:   timestamp(backendLoadLastEventAtColumn),
	}
}

func runBackendLoadConcurrentSearches(
	t *testing.T,
	ctx context.Context,
	client *http.Client,
	baseURL string,
	plan backendLoadPlan,
	fixtureStart time.Time,
) backendLoadSearchCohort {
	t.Helper()
	searchContext, searchCancel := context.WithTimeout(ctx, 60*time.Second)
	defer searchCancel()
	spec := newBackendLoadSearchSpec(plan, fixtureStart)

	type admissionResult struct {
		slot      int
		admission backendLoadSearchAdmission
		err       error
	}
	ready := make(chan struct{}, backendLoadConcurrentJobs)
	start := make(chan struct{})
	results := make(chan admissionResult, backendLoadConcurrentJobs)
	for slot := range backendLoadConcurrentJobs {
		go func() {
			ready <- struct{}{}
			<-start
			admission, err := createBackendLoadSearch(searchContext, client, baseURL, spec)
			results <- admissionResult{slot: slot, admission: admission, err: err}
		}()
	}
	for range backendLoadConcurrentJobs {
		<-ready
	}

	cohort := backendLoadSearchCohort{
		ReleasedAt: time.Now(),
		Searches:   make([]backendLoadSearchObservation, backendLoadConcurrentJobs),
	}
	close(start)
	admissions := make([]backendLoadSearchAdmission, backendLoadConcurrentJobs)
	var firstError error
	for range backendLoadConcurrentJobs {
		result := <-results
		if result.err != nil {
			if firstError == nil {
				firstError = fmt.Errorf(
					"create concurrent backend load search %d: %w",
					result.slot,
					result.err,
				)
				searchCancel()
			}
			continue
		}
		admissions[result.slot] = result.admission
	}
	if firstError != nil {
		t.Fatal(firstError)
	}

	for slot, admission := range admissions {
		cohort.Searches[slot] = collectBackendLoadSearch(
			t,
			searchContext,
			client,
			baseURL,
			spec,
			admission,
		)
	}
	cohort.CollectedAt = time.Now()
	if err := validateBackendLoadConcurrentSearches(
		cohort.Searches,

		backendLoadSearchExpectation{
			MinimumEvents:  plan.WarmEvents,
			MaximumEvents:  plan.eventCount(),
			MaximumUserIDs: plan.Cardinality,
			FixtureStart:   fixtureStart,
			EventInterval:  plan.interval(),
		}); err != nil {
		t.Fatal(err)
	}
	cohort.LifecycleStartedAt = cohort.Searches[0].CreatedAt
	cohort.LifecycleEndedAt = cohort.Searches[0].FinishedAt
	for _, observation := range cohort.Searches[1:] {
		if observation.CreatedAt.Before(cohort.LifecycleStartedAt) {
			cohort.LifecycleStartedAt = observation.CreatedAt
		}
		if observation.FinishedAt.After(cohort.LifecycleEndedAt) {
			cohort.LifecycleEndedAt = observation.FinishedAt
		}
	}
	return cohort
}

func validBackendLoadDuration(t *testing.T, name string, value *durationpb.Duration) time.Duration {
	t.Helper()
	if value == nil {
		t.Fatalf("backend load search %s is absent", name)
	}
	if err := value.CheckValid(); err != nil {
		t.Fatalf("backend load search %s is invalid: %v", name, err)
	}
	duration := value.AsDuration()
	if duration < 0 {
		t.Fatalf("backend load search %s = %s", name, duration)
	}
	return duration
}

func validBackendLoadTimestamp(t *testing.T, name string, value *timestamppb.Timestamp) time.Time {
	t.Helper()
	if value == nil {
		t.Fatalf("backend load search %s is absent", name)
	}
	if err := value.CheckValid(); err != nil {
		t.Fatalf("backend load search %s is invalid: %v", name, err)
	}
	return value.AsTime()
}

func validateBackendLoadConcurrentSearches(
	observations []backendLoadSearchObservation,
	expectation backendLoadSearchExpectation,
) error {
	if len(observations) != backendLoadConcurrentJobs {
		return fmt.Errorf(
			"concurrent backend load searches = %d, want %d",
			len(observations),
			backendLoadConcurrentJobs,
		)
	}
	jobIDs := make(map[string]struct{}, len(observations))
	for index, observation := range observations {
		if observation.JobID == "" {
			return fmt.Errorf("concurrent backend load search %d has an empty job ID", index)
		}
		if _, duplicate := jobIDs[observation.JobID]; duplicate {
			return fmt.Errorf(
				"concurrent backend load search %d repeats job ID %q",
				index,
				observation.JobID,
			)
		}
		jobIDs[observation.JobID] = struct{}{}
		if observation.CreatedAt.IsZero() ||
			observation.StartedAt.IsZero() ||
			observation.FinishedAt.IsZero() ||
			observation.CreatedAt.After(observation.StartedAt) ||
			!observation.StartedAt.Before(observation.FinishedAt) {
			return fmt.Errorf(
				"concurrent backend load search %d has invalid lifecycle timestamps: created=%s started=%s finished=%s",
				index,
				observation.CreatedAt,
				observation.StartedAt,
				observation.FinishedAt,
			)
		}
		if observation.LifecycleTime <= 0 ||
			observation.Elapsed < 0 ||
			observation.QueueWait < 0 ||
			observation.ScannedRows == 0 ||
			observation.ScannedBytes == 0 {
			return fmt.Errorf(
				"concurrent backend load search %d has invalid metrics: %+v",
				index,
				observation,
			)
		}
		if err := validateBackendLoadSearchPrefix(observation, expectation); err != nil {
			return fmt.Errorf("concurrent backend load search %d: %w", index, err)
		}
	}
	return nil
}

func validateBackendLoadSearchPrefix(
	observation backendLoadSearchObservation,
	expectation backendLoadSearchExpectation,
) error {
	if expectation.MinimumEvents >= expectation.MaximumEvents ||
		expectation.MaximumUserIDs == 0 ||
		expectation.FixtureStart.IsZero() ||
		expectation.EventInterval <= 0 ||
		expectation.MaximumEvents-1 > uint64(math.MaxInt64)/uint64(expectation.EventInterval) {
		return fmt.Errorf("invalid backend load search expectation: %+v", expectation)
	}
	if observation.Events < expectation.MinimumEvents ||
		observation.Events >= expectation.MaximumEvents {
		return fmt.Errorf(
			"events = %d, want [%d,%d)",
			observation.Events,
			expectation.MinimumEvents,
			expectation.MaximumEvents,
		)
	}
	if observation.EventIDs != observation.Events ||
		observation.RequestIDs != observation.Events {
		return fmt.Errorf(
			"events/unique event/request IDs = %d/%d/%d",
			observation.Events,
			observation.EventIDs,
			observation.RequestIDs,
		)
	}
	if observation.UserIDs == 0 ||
		observation.UserIDs > min(observation.Events, expectation.MaximumUserIDs) {
		return fmt.Errorf(
			"user IDs = %d, want (0,min(%d,%d)]",
			observation.UserIDs,
			observation.Events,
			expectation.MaximumUserIDs,
		)
	}
	wantLastEventAt := expectation.FixtureStart.Add(
		time.Duration(observation.Events-1) * expectation.EventInterval,
	)
	if observation.EventTimes != observation.Events ||
		!observation.FirstEventAt.Equal(expectation.FixtureStart) ||
		!observation.LastEventAt.Equal(wantLastEventAt) {
		return fmt.Errorf(
			"exact source prefix = events/times %d/%d range=[%s,%s], want %d unique times range=[%s,%s]",
			observation.Events,
			observation.EventTimes,
			observation.FirstEventAt,
			observation.LastEventAt,
			observation.Events,
			expectation.FixtureStart,
			wantLastEventAt,
		)
	}
	return nil
}

func maximumBackendLoadSearchActiveOverlap(observations []backendLoadSearchObservation) int {
	maximum := 0
	for _, candidate := range observations {
		overlap := 0
		for _, observation := range observations {
			if !candidate.StartedAt.Before(observation.StartedAt) &&
				candidate.StartedAt.Before(observation.FinishedAt) {
				overlap++
			}
		}
		maximum = max(maximum, overlap)
	}
	return maximum
}

func logBackendLoadConcurrentSearchWindow(
	t *testing.T,
	window backendLoadConcurrentSearchWindow,
) {
	t.Helper()
	if len(window.Cohorts) == 0 {
		t.Fatal("backend load concurrent-search window has no cohorts")
	}
	searchCount := 0
	for _, cohort := range window.Cohorts {
		searchCount += len(cohort.Searches)
	}
	lifecycleTimes := make([]time.Duration, 0, searchCount)
	elapsedTimes := make([]time.Duration, 0, searchCount)
	observations := make([]backendLoadSearchObservation, 0, searchCount)
	var (
		maximumQueueWait  time.Duration
		minimumEvents     uint64
		maximumEvents     uint64
		totalScannedRows  uint64
		totalScannedBytes uint64
	)
	for wave, cohort := range window.Cohorts {
		for slot, observation := range cohort.Searches {
			logBackendLoadSearchObservation(
				t,
				fmt.Sprintf("concurrent[%d][%d]", wave, slot),
				observation,
			)
			lifecycleTimes = append(lifecycleTimes, observation.LifecycleTime)
			elapsedTimes = append(elapsedTimes, observation.Elapsed)
			observations = append(observations, observation)
			maximumQueueWait = max(maximumQueueWait, observation.QueueWait)
			if len(observations) == 1 || observation.Events < minimumEvents {
				minimumEvents = observation.Events
			}
			maximumEvents = max(maximumEvents, observation.Events)
			totalScannedRows += observation.ScannedRows
			totalScannedBytes += observation.ScannedBytes
		}
	}
	slices.Sort(lifecycleTimes)
	slices.Sort(elapsedTimes)
	medianIndex := len(lifecycleTimes) / 2
	p95Index := (len(lifecycleTimes)*95+99)/100 - 1
	firstCohort := window.Cohorts[0]
	lastCohort := window.Cohorts[len(window.Cohorts)-1]
	lifecycleStartedAt := firstCohort.LifecycleStartedAt
	lifecycleEndedAt := firstCohort.LifecycleEndedAt
	for _, cohort := range window.Cohorts[1:] {
		if cohort.LifecycleStartedAt.Before(lifecycleStartedAt) {
			lifecycleStartedAt = cohort.LifecycleStartedAt
		}
		if cohort.LifecycleEndedAt.After(lifecycleEndedAt) {
			lifecycleEndedAt = cohort.LifecycleEndedAt
		}
	}
	t.Logf(
		"backend load concurrent-search window: waves=%d jobs=%d client_wall=%s lifecycle_span=%s max_active_overlap=%d source_records=%d->%d source_delta=%d stored_rows=%d->%d stored_delta=%d snapshot_events=%d..%d total_scanned_rows=%d total_scanned_bytes=%d",
		len(window.Cohorts),
		searchCount,
		lastCohort.CollectedAt.Sub(firstCohort.ReleasedAt),
		max(time.Duration(0), lifecycleEndedAt.Sub(lifecycleStartedAt)),
		maximumBackendLoadSearchActiveOverlap(observations),
		window.SourceBefore.Records,
		window.SourceAfter.Records,
		window.SourceAfter.Records-window.SourceBefore.Records,
		window.StoredBefore,
		window.StoredAfter,
		window.StoredAfter-window.StoredBefore,
		minimumEvents,
		maximumEvents,
		totalScannedRows,
		totalScannedBytes,
	)
	t.Logf(
		"backend load concurrent-search latency (observational): lifecycle_min=%s lifecycle_median=%s lifecycle_p95=%s lifecycle_max=%s elapsed_min=%s elapsed_median=%s elapsed_p95=%s elapsed_max=%s max_queue_wait=%s",
		lifecycleTimes[0],
		lifecycleTimes[medianIndex],
		lifecycleTimes[p95Index],
		lifecycleTimes[len(lifecycleTimes)-1],
		elapsedTimes[0],
		elapsedTimes[medianIndex],
		elapsedTimes[p95Index],
		elapsedTimes[len(elapsedTimes)-1],
		maximumQueueWait,
	)
}

func TestBackendLoadConcurrentSearchValidationRejectsInconsistentSnapshots(t *testing.T) {
	t.Parallel()
	fixtureStart := time.Date(2026, time.July, 26, 12, 0, 0, 0, time.UTC)
	expectation := backendLoadSearchExpectation{
		MinimumEvents:  5,
		MaximumEvents:  30,
		MaximumUserIDs: 10,
		FixtureStart:   fixtureStart,
		EventInterval:  time.Millisecond,
	}
	observations := make([]backendLoadSearchObservation, backendLoadConcurrentJobs)
	for index := range observations {
		startedAt := fixtureStart.Add(time.Duration(index) * time.Second)
		observations[index] = backendLoadSearchObservation{
			JobID:         fmt.Sprintf("job-%d", index),
			CreatedAt:     startedAt.Add(-time.Millisecond),
			StartedAt:     startedAt,
			FinishedAt:    startedAt.Add(time.Second),
			LifecycleTime: time.Second + time.Millisecond,
			ScannedRows:   12,
			ScannedBytes:  1,
			Events:        12,
			EventIDs:      12,
			RequestIDs:    12,
			UserIDs:       7,
			EventTimes:    12,
			FirstEventAt:  fixtureStart,
			LastEventAt:   fixtureStart.Add(11 * time.Millisecond),
		}
	}
	if err := validateBackendLoadConcurrentSearches(
		observations,

		expectation); err != nil {
		t.Fatalf("valid serial lifecycle observations: %v", err)
	}

	observations[3].EventIDs--
	if err := validateBackendLoadConcurrentSearches(
		observations,

		expectation); err == nil || !strings.Contains(err.Error(), "unique event/request IDs") {
		t.Fatalf("inconsistent concurrent-search validation error = %v, want ID rejection", err)
	}
	observations[3].EventIDs++

	observations[2].LastEventAt = fixtureStart.Add(12 * time.Millisecond)
	if err := validateBackendLoadConcurrentSearches(
		observations,

		expectation); err == nil || !strings.Contains(err.Error(), "exact source prefix") {
		t.Fatalf("gapped concurrent-search validation error = %v, want exact-prefix rejection", err)
	}
}

func TestMaximumBackendLoadSearchActiveOverlapUsesHalfOpenIntervals(t *testing.T) {
	t.Parallel()
	start := time.Date(2026, time.July, 26, 12, 0, 0, 0, time.UTC)
	observations := make([]backendLoadSearchObservation, backendLoadConcurrentJobs)
	for index := range observations {
		observations[index] = backendLoadSearchObservation{
			StartedAt:  start.Add(time.Duration(index) * time.Second),
			FinishedAt: start.Add(time.Duration(index+1) * time.Second),
		}
	}
	if got := maximumBackendLoadSearchActiveOverlap(observations); got != 1 {
		t.Fatalf("adjacent active overlap = %d, want 1", got)
	}
	for index := range observations {
		observations[index].StartedAt = start
		observations[index].FinishedAt = start.Add(time.Second)
	}
	if got := maximumBackendLoadSearchActiveOverlap(observations); got != backendLoadConcurrentJobs {
		t.Fatalf("coincident active overlap = %d, want %d", got, backendLoadConcurrentJobs)
	}
}

func TestValidateBackendLoadCohortVisibilityRejectsAnyRegression(t *testing.T) {
	t.Parallel()
	cohort := backendLoadSearchCohort{
		Searches: make([]backendLoadSearchObservation, backendLoadConcurrentJobs),
	}
	for index := range cohort.Searches {
		cohort.Searches[index].Events = 101
	}
	cohort.Searches[3].Events = 99
	if _, err := validateBackendLoadCohortVisibility(100, cohort); err == nil ||
		!strings.Contains(err.Error(), "regressed") {
		t.Fatalf("regressed cohort visibility error = %v, want regression rejection", err)
	}

	cohort.Searches[3].Events = 100
	if got, err := validateBackendLoadCohortVisibility(100, cohort); err != nil || got != 101 {
		t.Fatalf("monotonic cohort visibility = %d, %v; want 101, nil", got, err)
	}
}
