package alerts

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/Suhaibinator/open-splunk/internal/errorreport"
)

const (
	DefaultCoordinatorClaimLimit  = 16
	DefaultCoordinatorConcurrency = 4
	DefaultCoordinatorQueueSize   = MaximumAlertsPerOwner
	MaximumCoordinatorConcurrency = 32
	DefaultSearchPollInterval     = 250 * time.Millisecond
	DefaultCompletionTimeout      = 5 * time.Second
	MaximumCompletionTimeout      = 10 * time.Second
	DefaultCompletionRetryDelay   = 25 * time.Millisecond
	MaximumCompletionRetryDelay   = time.Second
)

// CompletionRetryWaitFunc waits between durable completion attempts. Tests
// inject it to drive retry behavior without wall-clock sleeps.
type CompletionRetryWaitFunc func(context.Context, time.Duration) error

type CoordinatorOptions struct {
	RunRepository       RunRepository
	Admission           SearchAdmission
	Jobs                SearchJobReader
	Results             SearchResultReader
	Retention           DurableRetentionUpdater
	Authorizer          DeliveryAuthorizer
	Deliverer           WebhookDeliverer
	Poller              SearchJobPoller
	Clock               func() time.Time
	DeliveryID          func() (string, error)
	PublicBaseURL       string
	ClaimLimit          int
	ConcurrencyLimit    int
	QueueCapacity       int
	CompletionTimeout   time.Duration
	CompletionRetryWait CompletionRetryWaitFunc
	OnError             func(error)
}

// Coordinator owns the complete lifecycle of a claimed alert occurrence. It
// deliberately depends on narrow adapters so scheduled searches use the same
// trusted admission, retained-result, and delivery implementations as the
// interactive server without importing transport code.
type Coordinator struct {
	runs                RunRepository
	admission           SearchAdmission
	jobs                SearchJobReader
	results             SearchResultReader
	retention           DurableRetentionUpdater
	authorizer          DeliveryAuthorizer
	deliverer           WebhookDeliverer
	poller              SearchJobPoller
	clock               func() time.Time
	deliveryID          func() (string, error)
	publicBaseURL       string
	claimLimit          int
	completionTimeout   time.Duration
	completionRetryWait CompletionRetryWaitFunc
	errorReports        errorreport.SingleFlight
	executorContext     context.Context
	cancelExecutor      context.CancelFunc
	completionContext   context.Context
	cancelCompletion    context.CancelFunc
	queue               chan RunSnapshot
	slots               chan struct{}
	workers             sync.WaitGroup
	submissions         sync.WaitGroup
	lifecycleMu         sync.Mutex
	closed              bool
	shutdownOnce        sync.Once
	shutdownDone        chan struct{}
}

func NewCoordinator(options CoordinatorOptions) (*Coordinator, error) {
	if options.RunRepository == nil || options.Admission == nil || options.Jobs == nil ||
		options.Results == nil || options.Retention == nil || options.Authorizer == nil || options.Deliverer == nil {
		return nil, fmt.Errorf("%w: all coordinator dependencies are required", ErrInvalidArgument)
	}
	if strings.TrimSpace(options.PublicBaseURL) != "" {
		if err := ValidatePublicBaseURL(options.PublicBaseURL); err != nil {
			return nil, err
		}
	}
	clock := options.Clock
	if clock == nil {
		clock = time.Now
	}
	deliveryID := options.DeliveryID
	if deliveryID == nil {
		deliveryID = func() (string, error) { return uuid.NewString(), nil }
	}
	poller := options.Poller
	if poller == nil {
		poller = IntervalSearchJobPoller{Interval: DefaultSearchPollInterval}
	}
	claimLimit := options.ClaimLimit
	if claimLimit == 0 {
		claimLimit = DefaultCoordinatorClaimLimit
	}
	if claimLimit < 1 || claimLimit > MaximumAlertsPerOwner {
		return nil, fmt.Errorf("%w: coordinator claim limit is out of range", ErrInvalidArgument)
	}
	concurrency := options.ConcurrencyLimit
	if concurrency == 0 {
		concurrency = DefaultCoordinatorConcurrency
	}
	if concurrency < 1 || concurrency > MaximumCoordinatorConcurrency {
		return nil, fmt.Errorf("%w: coordinator concurrency limit is out of range", ErrInvalidArgument)
	}
	queueCapacity := options.QueueCapacity
	if queueCapacity == 0 {
		queueCapacity = DefaultCoordinatorQueueSize
	}
	if queueCapacity < 1 || queueCapacity > MaximumAlertsPerOwner || concurrency > queueCapacity {
		return nil, fmt.Errorf("%w: coordinator queue capacity is out of range", ErrInvalidArgument)
	}
	completionTimeout := options.CompletionTimeout
	if completionTimeout == 0 {
		completionTimeout = DefaultCompletionTimeout
	}
	if completionTimeout < 0 || completionTimeout > MaximumCompletionTimeout {
		return nil, fmt.Errorf("%w: coordinator completion timeout must be positive and at most %s", ErrInvalidArgument, MaximumCompletionTimeout)
	}
	completionRetryWait := options.CompletionRetryWait
	if completionRetryWait == nil {
		completionRetryWait = waitForCompletionRetry
	}
	executorContext, cancelExecutor := context.WithCancel(context.Background())
	completionContext, cancelCompletion := context.WithCancel(context.Background())
	coordinator := &Coordinator{
		runs: options.RunRepository, admission: options.Admission, jobs: options.Jobs,
		results: options.Results, retention: options.Retention, authorizer: options.Authorizer,
		deliverer: options.Deliverer, poller: poller, clock: clock, deliveryID: deliveryID,
		publicBaseURL: options.PublicBaseURL, claimLimit: claimLimit,
		completionTimeout: completionTimeout, completionRetryWait: completionRetryWait,
		errorReports:    errorreport.SingleFlight{Callback: options.OnError},
		executorContext: executorContext, cancelExecutor: cancelExecutor,
		completionContext: completionContext, cancelCompletion: cancelCompletion,
		queue: make(chan RunSnapshot, queueCapacity), slots: make(chan struct{}, queueCapacity),
		shutdownDone: make(chan struct{}),
	}
	coordinator.workers.Add(concurrency)
	for index := 0; index < concurrency; index++ {
		go coordinator.worker()
	}
	return coordinator, nil
}

// IntervalSearchJobPoller is the production polling policy. Tests can inject
// a synchronous poller and therefore never wait on wall-clock sleeps.
type IntervalSearchJobPoller struct{ Interval time.Duration }

func (poller IntervalSearchJobPoller) WaitForTerminal(ctx context.Context, ownerID, jobID string, reader SearchJobReader) (SearchJobSnapshot, error) {
	interval := poller.Interval
	if interval <= 0 {
		return SearchJobSnapshot{}, fmt.Errorf("%w: poll interval must be positive", ErrInvalidArgument)
	}
	for {
		job, err := reader.ReadAlertSearchJob(ctx, ownerID, jobID)
		if err != nil {
			return SearchJobSnapshot{}, err
		}
		if job.State != SearchJobQueued && job.State != SearchJobRunning {
			return job, nil
		}
		timer := time.NewTimer(interval)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return SearchJobSnapshot{}, ctx.Err()
		case <-timer.C:
		}
	}
}

// Step reserves bounded executor capacity, persists due claims, and enqueues
// them. It returns after enqueueing; durable completion belongs to the
// coordinator workers rather than the scheduler tick's request context.
func (coordinator *Coordinator) Step(ctx context.Context, now time.Time) error {
	if ctx == nil || now.IsZero() {
		return ErrInvalidArgument
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := coordinator.beginSubmission(); err != nil {
		return err
	}
	defer coordinator.submissions.Done()
	reserved := coordinator.reserve(coordinator.claimLimit)
	if reserved == 0 {
		return nil
	}
	claimed, err := coordinator.runs.ClaimDue(ctx, now.UTC(), reserved)
	if err != nil {
		coordinator.release(reserved)
		return err
	}
	if len(claimed) > reserved {
		coordinator.release(reserved)
		return errors.New("alerts: run repository exceeded the claim limit")
	}
	coordinator.release(reserved - len(claimed))
	for _, snapshot := range claimed {
		coordinator.queue <- snapshot
	}
	return nil
}

// RunNow claims and enqueues one immediate occurrence without advancing the
// alert's cron cursor. The returned SEARCHING summary identifies the durable
// run; callers inspect run history for its eventual terminal outcome.
func (coordinator *Coordinator) RunNow(ctx context.Context, ownerID, alertID string) (RunSummary, error) {
	if ctx == nil || strings.TrimSpace(ownerID) == "" || strings.TrimSpace(alertID) == "" {
		return RunSummary{}, ErrInvalidArgument
	}
	if err := ctx.Err(); err != nil {
		return RunSummary{}, err
	}
	if err := coordinator.beginSubmission(); err != nil {
		return RunSummary{}, err
	}
	defer coordinator.submissions.Done()
	if coordinator.reserve(1) == 0 {
		return RunSummary{}, ErrCapacity
	}
	now := coordinator.clock().UTC()
	snapshot, active, err := coordinator.runs.ClaimRunNow(ctx, ownerID, alertID, now)
	if err != nil {
		coordinator.release(1)
		return RunSummary{}, err
	}
	if !active {
		coordinator.release(1)
		return RunSummary{
			AlertID: snapshot.AlertID, AlertRunID: snapshot.AlertRunID, AlertVersion: snapshot.AlertVersion,
			Outcome: RunOverlapSkipped, ScheduledAt: snapshot.ScheduledAt, StartedAt: snapshot.ClaimedAt,
			FinishedAt: snapshot.ClaimedAt,
		}, nil
	}
	coordinator.queue <- snapshot
	return RunSummary{
		AlertID: snapshot.AlertID, AlertRunID: snapshot.AlertRunID, AlertVersion: snapshot.AlertVersion,
		Outcome: RunSearching, ScheduledAt: snapshot.ScheduledAt, StartedAt: snapshot.ClaimedAt,
		MissedOccurrenceCount: snapshot.MissedOccurrenceCount,
	}, nil
}

// Recover closes every pre-restart active run before the scheduler resumes.
func (coordinator *Coordinator) Recover(ctx context.Context) (int64, error) {
	if ctx == nil {
		return 0, ErrInvalidArgument
	}
	if err := coordinator.beginSubmission(); err != nil {
		return 0, err
	}
	defer coordinator.submissions.Done()
	return coordinator.runs.InterruptUnfinished(ctx, coordinator.clock().UTC())
}

// Close rejects new submissions, cancels active execution, and joins every
// worker before returning. Durable completion retries continue during the
// caller's shutdown grace period. At its deadline Close cancels those retries,
// still performs the definitive join, and returns the context error. This
// guarantee lets callers tear down repository dependencies after Close returns.
func (coordinator *Coordinator) Close(ctx context.Context) error {
	if ctx == nil {
		return ErrInvalidArgument
	}
	coordinator.shutdownOnce.Do(func() {
		coordinator.lifecycleMu.Lock()
		coordinator.closed = true
		coordinator.cancelExecutor()
		coordinator.lifecycleMu.Unlock()
		go func() {
			coordinator.submissions.Wait()
			close(coordinator.queue)
			coordinator.workers.Wait()
			coordinator.cancelCompletion()
			close(coordinator.shutdownDone)
		}()
	})
	select {
	case <-coordinator.shutdownDone:
		return nil
	case <-ctx.Done():
		coordinator.cancelCompletion()
		<-coordinator.shutdownDone
		return ctx.Err()
	}
}

func (coordinator *Coordinator) beginSubmission() error {
	coordinator.lifecycleMu.Lock()
	defer coordinator.lifecycleMu.Unlock()
	if coordinator.closed {
		return ErrClosed
	}
	coordinator.submissions.Add(1)
	return nil
}

func (coordinator *Coordinator) reserve(limit int) int {
	reserved := 0
	for reserved < limit {
		select {
		case coordinator.slots <- struct{}{}:
			reserved++
		default:
			return reserved
		}
	}
	return reserved
}

func (coordinator *Coordinator) release(count int) {
	for range count {
		<-coordinator.slots
	}
}

func (coordinator *Coordinator) worker() {
	defer coordinator.workers.Done()
	for snapshot := range coordinator.queue {
		_, err := coordinator.execute(coordinator.executorContext, snapshot)
		coordinator.release(1)
		coordinator.errorReports.Report(err)
	}
}

func (coordinator *Coordinator) execute(ctx context.Context, snapshot RunSnapshot) (RunSummary, error) {
	summary := RunSummary{
		AlertID: snapshot.AlertID, AlertRunID: snapshot.AlertRunID, AlertVersion: snapshot.AlertVersion,
		Outcome: RunSearching, ScheduledAt: snapshot.ScheduledAt, StartedAt: snapshot.ClaimedAt,
		MissedOccurrenceCount: snapshot.MissedOccurrenceCount,
	}
	if err := ctx.Err(); err != nil {
		summary.Outcome = RunInterrupted
		summary.FailureCategory = string(FailureCanceled)
		return coordinator.complete(ctx, summary, err)
	}
	searchTimezone := snapshot.Definition.SearchTimezone
	if searchTimezone == "" {
		searchTimezone = snapshot.Definition.Timezone
	}
	jobID, err := coordinator.admission.AdmitAlertSearch(ctx, SearchRequest{
		OwnerID: snapshot.OwnerID, TenantID: snapshot.TenantID, Application: snapshot.Definition.Application,
		SPL: snapshot.Definition.SPL, Earliest: snapshot.Definition.Earliest, Latest: snapshot.Definition.Latest,
		Timezone: searchTimezone, IndexScope: append([]string(nil), snapshot.Definition.IndexScope...),
		AlertID: snapshot.AlertID, AlertRunID: snapshot.AlertRunID, ScheduledAt: snapshot.ScheduledAt,
		Retention: snapshot.DispatchRetention,
	})
	if err != nil || strings.TrimSpace(jobID) == "" {
		summary.Outcome = RunSearchFailed
		summary.FailureCategory = string(FailureAdmission)
		if err == nil {
			err = errors.New("alerts: search admission returned an empty job ID")
		}
		return coordinator.complete(ctx, summary, err)
	}
	summary.SearchJobID = jobID
	summary.SearchJobExpiresAt = coordinator.clock().UTC().Add(snapshot.DispatchRetention)
	if err := coordinator.runs.AttachSearchJob(ctx, snapshot.AlertID, snapshot.AlertRunID, jobID, summary.SearchJobExpiresAt); err != nil {
		summary.Outcome = RunSearchFailed
		summary.FailureCategory = string(FailureAttach)
		return coordinator.complete(ctx, summary, err)
	}
	job, err := coordinator.poller.WaitForTerminal(ctx, snapshot.OwnerID, jobID, coordinator.jobs)
	if err != nil {
		if ctx.Err() != nil {
			summary.Outcome = RunInterrupted
			summary.FailureCategory = string(FailureCanceled)
		} else {
			summary.Outcome = RunSearchFailed
			summary.FailureCategory = string(FailureJobRead)
		}
		return coordinator.complete(ctx, summary, err)
	}
	if job.ID != jobID {
		summary.Outcome = RunSearchFailed
		summary.FailureCategory = string(FailureJobIDMismatch)
		return coordinator.complete(ctx, summary, errors.New("alerts: search reader returned the wrong job"))
	}
	if !job.StartedAt.IsZero() {
		summary.StartedAt = job.StartedAt
	}
	if !job.ExpiresAt.IsZero() {
		summary.SearchJobExpiresAt = job.ExpiresAt
	}
	summary.ResultCount = job.ResultCount
	summary.ResultCountExact = !job.ResultsTruncated
	switch job.State {
	case SearchJobFailed:
		summary.Outcome = RunSearchFailed
		summary.FailureCategory = safeFailureCategory(job.FailureCategory, FailureSearch)
		return coordinator.complete(ctx, summary, nil)
	case SearchJobCanceled:
		summary.Outcome = RunSearchCanceled
		summary.FailureCategory = string(FailureSearchCanceled)
		return coordinator.complete(ctx, summary, nil)
	case SearchJobExpired:
		summary.Outcome = RunSearchExpired
		summary.FailureCategory = string(FailureSearchExpired)
		return coordinator.complete(ctx, summary, nil)
	case SearchJobInterrupted:
		summary.Outcome = RunInterrupted
		summary.FailureCategory = string(FailureSearchInterrupted)
		return coordinator.complete(ctx, summary, nil)
	case SearchJobCompleted:
	case SearchJobQueued, SearchJobRunning:
		summary.Outcome = RunSearchFailed
		summary.FailureCategory = string(FailureNonterminalJob)
		return coordinator.complete(ctx, summary, errors.New("alerts: search poller returned a nonterminal job"))
	default:
		summary.Outcome = RunSearchFailed
		summary.FailureCategory = string(FailureInvalidJobState)
		return coordinator.complete(ctx, summary, errors.New("alerts: search reader returned an invalid state"))
	}
	evaluation, err := Evaluate(snapshot.Definition.Condition, CountObservation{Count: job.ResultCount, Exact: !job.ResultsTruncated})
	if err != nil {
		summary.Outcome = RunSearchFailed
		summary.FailureCategory = string(FailureEvaluation)
		return coordinator.complete(ctx, summary, err)
	}
	summary.Evaluation = evaluation.Certainty
	if evaluation.Certainty == EvaluationFalse {
		summary.Outcome = RunNotTriggered
		return coordinator.complete(ctx, summary, nil)
	}
	if evaluation.Certainty == EvaluationIndeterminate {
		summary.Outcome = RunIndeterminate
		return coordinator.complete(ctx, summary, nil)
	}
	if err := ctx.Err(); err != nil {
		summary.Outcome = RunInterrupted
		summary.FailureCategory = string(FailureCanceled)
		return coordinator.complete(ctx, summary, err)
	}
	extendedExpiry, err := coordinator.retention.ExtendAlertSearchJob(ctx, snapshot.OwnerID, jobID, snapshot.TriggeredRetention)
	if err != nil || extendedExpiry.IsZero() {
		summary.Outcome = RunDeliveryFailed
		summary.FailureCategory = string(FailureRetentionExtension)
		if err == nil {
			err = errors.New("alerts: retention updater returned an empty expiry")
		}
		return coordinator.complete(ctx, summary, err)
	}
	summary.SearchJobExpiresAt = extendedExpiry
	opened, err := coordinator.authorizer.AuthorizeAndOpenDelivery(ctx, snapshot, coordinator.deliveryID)
	if err != nil {
		switch {
		case errors.Is(err, ErrSecretRotated):
			summary.Outcome = RunDeliverySkipped
			summary.FailureCategory = string(FailureSecretRotated)
			return coordinator.complete(ctx, summary, nil)
		case errors.Is(err, ErrDeliveryAttempted):
			summary.Outcome = RunDeliveryUnknown
			summary.FailureCategory = string(FailureDeliveryAlreadyAuthorized)
		case errors.Is(err, ErrDeliveryIDGeneration):
			summary.Outcome = RunDeliveryFailed
			summary.FailureCategory = string(FailureDeliveryID)
		default:
			summary.Outcome = RunDeliveryFailed
			summary.FailureCategory = string(FailureDeliveryAuthorization)
		}
		return coordinator.complete(ctx, summary, err)
	}
	defer clear(opened.Secret)
	summary.DeliveryID = opened.DeliveryID
	result, err := coordinator.results.ReadAlertSearchResults(ctx, snapshot.OwnerID, jobID, snapshot.Definition.SampleRows)
	if err != nil {
		summary.Outcome = RunDeliveryUnknown
		summary.FailureCategory = string(FailureResultSample)
		return coordinator.complete(ctx, summary, err)
	}
	rows := result.Rows
	sampleTruncated := result.More
	if len(rows) > snapshot.Definition.SampleRows {
		rows = rows[:snapshot.Definition.SampleRows]
		sampleTruncated = true
	}
	resultsURL, err := coordinator.resultsURL(jobID)
	if err != nil {
		summary.Outcome = RunDeliveryUnknown
		summary.FailureCategory = string(FailurePublicBaseURL)
		return coordinator.complete(ctx, summary, err)
	}
	deliveryAt := coordinator.clock().UTC()
	signed, err := BuildSignedPayload(WebhookPayload{
		EventType: WebhookEventTriggered, AlertID: snapshot.AlertID, AlertRunID: snapshot.AlertRunID,
		SearchJobID: jobID, AlertName: snapshot.Definition.Name, Application: snapshot.Definition.Application,
		ScheduledAt: snapshot.ScheduledAt, StartedAt: summary.StartedAt, FinishedAt: job.FinishedAt,
		DeliveryAt: deliveryAt, MissedOccurrenceCount: uint64(snapshot.MissedOccurrenceCount),
		Operator: snapshot.Definition.Condition.Operator, Threshold: snapshot.Definition.Condition.Threshold,
		ResultCount: job.ResultCount, ResultCountExact: !job.ResultsTruncated, ResultSchema: result.Schema,
		SampleRows: rows, SearchTruncated: job.ResultsTruncated, SampleTruncated: sampleTruncated,
		ResultsURL: resultsURL,
	}, opened.DeliveryID, opened.Secret)
	clear(opened.Secret)
	if err != nil {
		summary.Outcome = RunDeliveryUnknown
		summary.FailureCategory = string(FailurePayloadBuild)
		return coordinator.complete(ctx, summary, err)
	}
	delivery, deliverErr := coordinator.deliverer.Deliver(ctx, opened.Endpoint, signed)
	summary.Delivery = delivery
	if deliverErr != nil || !delivery.Delivered {
		summary.Outcome = RunDeliveryFailed
		summary.FailureCategory = safeFailureCategory(string(delivery.Category), FailureDelivery)
		return coordinator.complete(ctx, summary, nil)
	}
	summary.Outcome = RunDelivered
	return coordinator.complete(ctx, summary, nil)
}

func (coordinator *Coordinator) complete(_ context.Context, summary RunSummary, cause error) (RunSummary, error) {
	summary.FinishedAt = coordinator.clock().UTC()
	for retry := uint(0); ; retry++ {
		completionContext, cancel := context.WithTimeout(coordinator.completionContext, coordinator.completionTimeout)
		completeErr := coordinator.runs.CompleteRun(completionContext, summary)
		cancel()
		if completeErr == nil {
			return summary, cause
		}
		if errors.Is(completeErr, ErrInvalidArgument) ||
			errors.Is(completeErr, ErrNotFound) ||
			errors.Is(completeErr, ErrVersionConflict) {
			return summary, errors.Join(cause, completeErr)
		}
		if coordinator.completionContext.Err() != nil {
			return summary, errors.Join(cause, completeErr, coordinator.completionContext.Err())
		}
		if waitErr := coordinator.completionRetryWait(coordinator.completionContext, completionRetryDelay(retry)); waitErr != nil {
			return summary, errors.Join(cause, completeErr, waitErr)
		}
	}
}

func completionRetryDelay(retry uint) time.Duration {
	delay := DefaultCompletionRetryDelay
	for retry > 0 && delay < MaximumCompletionRetryDelay {
		if delay > MaximumCompletionRetryDelay/2 {
			return MaximumCompletionRetryDelay
		}
		delay *= 2
		retry--
	}
	return delay
}

func waitForCompletionRetry(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func (coordinator *Coordinator) resultsURL(jobID string) (string, error) {
	return SearchJobResultsURL(coordinator.publicBaseURL, jobID)
}

func safeFailureCategory(value string, fallback FailureCategory) string {
	value = strings.TrimSpace(value)
	if value == "" || len(value) > 128 {
		return string(fallback)
	}
	for _, character := range value {
		if (character < 'A' || character > 'Z') && (character < '0' || character > '9') && character != '_' && character != '-' {
			return string(fallback)
		}
	}
	return value
}
