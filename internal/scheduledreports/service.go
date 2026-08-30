package scheduledreports

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"google.golang.org/protobuf/proto"

	"github.com/Suhaibinator/open-splunk/internal/featureops"
	"github.com/Suhaibinator/open-splunk/internal/scheduler"
	"github.com/Suhaibinator/open-splunk/internal/schedulevalidation"
	"github.com/Suhaibinator/open-splunk/internal/searchretention"
)

// Store is the durable scheduling boundary used by Service.
type Store interface {
	Configure(context.Context, string, string, string, uint64, Configuration, *time.Time) (Schedule, error)
	Get(context.Context, string, string) (Schedule, error)
	ListDue(context.Context, time.Time, int) ([]Schedule, error)
	ClaimDue(context.Context, Schedule, time.Time, time.Time, time.Time, uint64, time.Duration, time.Duration) (Run, bool, error)
	ClaimRunNow(context.Context, Schedule, time.Time, time.Duration, time.Duration) (Run, bool, error)
	ClaimOneOff(context.Context, string, string, string, time.Time, time.Duration, time.Duration) (Run, bool, error)
	MarkSubmitted(context.Context, string, string) error
	Finish(context.Context, string, RunOutcome, string, time.Time) error
	InterruptActive(context.Context, time.Time) (int64, error)
	ListRuns(context.Context, string, string, int) ([]Run, error)
	ListRunPage(context.Context, string, string, RunPageRequest) (RunPage, error)
	ListCurrentProjections(context.Context, string, []string) (map[string]CurrentProjection, error)
}

// ServiceOptions contains orchestration dependencies and bounds.
type ServiceOptions struct {
	Admitter   SearchAdmitter
	ClaimLimit int
	Clock      func() time.Time
	Store      Store
	Observer   featureops.Observer
}

// Service validates schedules, claims occurrences, and submits immutable
// snapshots through trusted search admission.
type Service struct {
	admitter   SearchAdmitter
	claimLimit int
	clock      func() time.Time
	store      Store
	observer   featureops.Observer
}

// NewService constructs a scheduled-report service.
func NewService(options ServiceOptions) (*Service, error) {
	if options.Store == nil || options.Admitter == nil {
		return nil, fmt.Errorf("%w: store and search admitter are required", ErrInvalidArgument)
	}
	claimLimit := options.ClaimLimit
	if claimLimit == 0 {
		claimLimit = DefaultClaimLimit
	}
	if claimLimit < 1 || claimLimit > MaximumClaimLimit {
		return nil, fmt.Errorf("%w: claim limit is out of range", ErrInvalidArgument)
	}
	clock := options.Clock
	if clock == nil {
		clock = time.Now
	}
	return &Service{
		admitter: options.Admitter, claimLimit: claimLimit, clock: clock,
		store: options.Store, observer: options.Observer,
	}, nil
}

// Configure validates cron, timezone, and dispatch.ttl before persistence.
func (service *Service) Configure(
	ctx context.Context,
	ownerID, tenantID, savedSearchID string,
	expectedVersion uint64,
	configuration Configuration,
) (Schedule, error) {
	if ctx == nil {
		return Schedule{}, ErrInvalidArgument
	}
	now, err := normalizedTime(service.clock())
	if err != nil {
		return Schedule{}, err
	}
	validation, err := schedulevalidation.ValidateAt(schedulevalidation.Input{
		Mode: schedulevalidation.ModeScheduledReport, Cron: configuration.Cron,
		Timezone: configuration.Timezone, DispatchTTL: configuration.DispatchTTL,
	}, now)
	if err != nil {
		return Schedule{}, fmt.Errorf("%w: validate schedule: %w", ErrInvalidArgument, err)
	}
	if !validation.Valid() {
		first := validation.Violations[0]
		return Schedule{}, fmt.Errorf("%w: %s: %s", ErrInvalidArgument, first.Field, first.Code)
	}
	normalized := Configuration{
		Cron: validation.Cron, Timezone: validation.Timezone,
		DispatchTTL: validation.DispatchTTL, Enabled: configuration.Enabled,
	}
	var next *time.Time
	if normalized.Enabled {
		next = &validation.Next
	}
	return service.store.Configure(ctx, ownerID, tenantID, savedSearchID, expectedVersion, normalized, next)
}

// SetEnabled changes only enabled state while preserving validated schedule
// configuration under the caller's expected config version.
func (service *Service) SetEnabled(
	ctx context.Context,
	ownerID, tenantID, savedSearchID string,
	expectedVersion uint64,
	enabled bool,
) (Schedule, error) {
	current, err := service.store.Get(ctx, ownerID, savedSearchID)
	if err != nil {
		return Schedule{}, err
	}
	if current.ConfigVersion != expectedVersion {
		return Schedule{}, ErrConflict
	}
	return service.Configure(ctx, ownerID, tenantID, savedSearchID, expectedVersion, Configuration{
		Cron: current.Cron, Timezone: current.Timezone, DispatchTTL: current.DispatchTTL, Enabled: enabled,
	})
}

// Step implements scheduler.Stepper. Each due row is independently claimed;
// operator/configuration races are benign compare-and-swap misses.
func (service *Service) Step(ctx context.Context, now time.Time) error {
	if ctx == nil {
		return ErrInvalidArgument
	}
	now, err := normalizedTime(now)
	if err != nil {
		return err
	}
	due, err := service.store.ListDue(ctx, now, service.claimLimit)
	if err != nil {
		return err
	}
	var failures []error
	for _, candidate := range due {
		if err := ctx.Err(); err != nil {
			return errors.Join(append(failures, err)...)
		}
		if err := service.claimAndAdmit(ctx, candidate, now); err != nil {
			if _, ok := errors.AsType[expectedRunFailure](err); ok {
				continue
			}
			failures = append(failures, err)
		}
	}
	return errors.Join(failures...)
}

func (service *Service) claimAndAdmit(ctx context.Context, candidate Schedule, now time.Time) error {
	parsed, err := scheduler.ParseCron(candidate.Cron, candidate.Timezone)
	if err != nil {
		return fmt.Errorf("scheduled report %q has invalid persisted cron: %w", candidate.SavedSearchID, err)
	}
	if candidate.NextRunAt == nil {
		return fmt.Errorf("scheduled report %q has no next occurrence", candidate.SavedSearchID)
	}
	scheduledAt, nextRunAt, skipped, err := parsed.AdvancePastContext(ctx, *candidate.NextRunAt, now)
	if err != nil {
		return fmt.Errorf("advance scheduled report %q: %w", candidate.SavedSearchID, err)
	}
	period, err := parsed.Period(scheduledAt)
	if err != nil {
		return fmt.Errorf("resolve scheduled report %q period: %w", candidate.SavedSearchID, err)
	}
	retention, err := searchretention.ScheduledReport(candidate.DispatchTTL, period)
	if err != nil {
		return fmt.Errorf("resolve scheduled report %q retention: %w", candidate.SavedSearchID, err)
	}
	run, claimed, err := service.store.ClaimDue(ctx, candidate, scheduledAt, nextRunAt, now, skipped, period, retention.Lifetime)
	if err != nil {
		service.observe(featureops.OperationScheduleClaim, featureops.OutcomeFailed, 1)
		return err
	}
	if !claimed {
		service.observe(featureops.OperationScheduleClaim, featureops.OutcomeSkipped, 1)
		return nil
	}
	service.observe(featureops.OperationScheduleClaim, featureops.OutcomeSucceeded, 1)
	if run.Outcome == RunOutcomeSkippedOverlap {
		service.observeRunOutcome(run.Outcome)
		return nil
	}
	_, err = service.admit(ctx, run)
	return err
}

// RunNow submits one immediate occurrence without changing the cron cursor.
func (service *Service) RunNow(ctx context.Context, ownerID, savedSearchID string) (Run, error) {
	if ctx == nil {
		return Run{}, ErrInvalidArgument
	}
	candidate, err := service.store.Get(ctx, ownerID, savedSearchID)
	if err != nil {
		return Run{}, err
	}
	return service.runConfiguredNow(ctx, candidate)
}

func (service *Service) runConfiguredNow(ctx context.Context, candidate Schedule) (Run, error) {
	now, err := normalizedTime(service.clock())
	if err != nil {
		return Run{}, err
	}
	parsed, err := scheduler.ParseCron(candidate.Cron, candidate.Timezone)
	if err != nil {
		return Run{}, err
	}
	reference := parsed.Next(now)
	period, err := parsed.Period(reference)
	if err != nil {
		return Run{}, err
	}
	retention, err := searchretention.ScheduledReport(candidate.DispatchTTL, period)
	if err != nil {
		return Run{}, err
	}
	run, claimed, err := service.store.ClaimRunNow(ctx, candidate, now, period, retention.Lifetime)
	if err != nil {
		service.observe(featureops.OperationScheduleClaim, featureops.OutcomeFailed, 1)
		return Run{}, err
	}
	if !claimed {
		service.observe(featureops.OperationScheduleClaim, featureops.OutcomeSkipped, 1)
		return Run{}, ErrConflict
	}
	service.observe(featureops.OperationScheduleClaim, featureops.OutcomeSucceeded, 1)
	if run.Outcome == RunOutcomeSkippedOverlap {
		service.observeRunOutcome(run.Outcome)
		return Run{}, ErrConflict
	}
	jobID, err := service.admit(ctx, run)
	if err != nil {
		return Run{}, err
	}
	run.Outcome = RunOutcomeSubmitted
	run.SearchJobID = jobID
	return run, nil
}

// RunNowOrOneOff runs a configured schedule when one exists. An unscheduled
// saved search instead receives a one-off scheduled-report snapshot using the
// caller-supplied period and Splunk's default 2p dispatch retention.
func (service *Service) RunNowOrOneOff(
	ctx context.Context,
	ownerID, tenantID, savedSearchID string,
	oneOffPeriod time.Duration,
) (Run, error) {
	if ctx == nil || oneOffPeriod <= 0 {
		return Run{}, ErrInvalidArgument
	}
	if candidate, err := service.store.Get(ctx, ownerID, savedSearchID); err == nil {
		return service.runConfiguredNow(ctx, candidate)
	} else if !errors.Is(err, ErrNotFound) {
		return Run{}, err
	}
	now, err := normalizedTime(service.clock())
	if err != nil {
		return Run{}, err
	}
	retention, err := searchretention.ScheduledReport(DefaultOneOffDispatchTTL, oneOffPeriod)
	if err != nil {
		return Run{}, fmt.Errorf("resolve one-off report retention: %w", err)
	}
	run, claimed, err := service.store.ClaimOneOff(
		ctx, ownerID, tenantID, savedSearchID, now, oneOffPeriod, retention.Lifetime,
	)
	if err != nil {
		service.observe(featureops.OperationScheduleClaim, featureops.OutcomeFailed, 1)
		return Run{}, err
	}
	if !claimed {
		service.observe(featureops.OperationScheduleClaim, featureops.OutcomeSkipped, 1)
		return Run{}, ErrConflict
	}
	service.observe(featureops.OperationScheduleClaim, featureops.OutcomeSucceeded, 1)
	if run.Outcome == RunOutcomeSkippedOverlap {
		service.observeRunOutcome(run.Outcome)
		return Run{}, ErrConflict
	}
	jobID, err := service.admit(ctx, run)
	if err != nil {
		return Run{}, err
	}
	run.Outcome = RunOutcomeSubmitted
	run.SearchJobID = jobID
	return run, nil
}

func (service *Service) admit(ctx context.Context, run Run) (string, error) {
	request := AdmissionRequest{
		RunID: run.RunID, SavedSearchID: run.SavedSearchID, DefinitionVersion: run.DefinitionVersion,
		Definition: proto.Clone(run.Definition).(*opensplunk.SavedSearchDefinition),
		OwnerID:    run.OwnerID, TenantID: run.TenantID, ScheduledAt: run.ScheduledAt,
		SchedulePeriod: run.SchedulePeriod, RetentionLifetime: run.RetentionLifetime,
	}
	jobID, err := service.admitter.AdmitScheduledReport(ctx, request)
	if err != nil {
		finishErr := service.store.Finish(context.WithoutCancel(ctx), run.RunID, RunOutcomeFailed, "admission_failed", service.clock())
		if finishErr != nil && !errors.Is(finishErr, ErrConflict) {
			service.observeRunOutcome(RunOutcomeFailed)
			return "", errors.Join(fmt.Errorf("admit scheduled report %q: %w", run.SavedSearchID, err), finishErr)
		}
		// A synchronous journal attachment can commit immediately before a later
		// admission projection fails. The manager compensates that admitted job
		// to a terminal state; its terminal callback wins this exact conflict.
		service.observeRunOutcome(RunOutcomeFailed)
		return "", expectedRunFailure{cause: fmt.Errorf("admit scheduled report %q: %w", run.SavedSearchID, err)}
	}
	if strings.TrimSpace(jobID) == "" {
		finishErr := service.store.Finish(context.WithoutCancel(ctx), run.RunID, RunOutcomeFailed, "invalid_job_id", service.clock())
		if finishErr != nil {
			service.observeRunOutcome(RunOutcomeFailed)
			return "", errors.Join(errors.New("scheduled report admission returned an empty job ID"), finishErr)
		}
		service.observeRunOutcome(RunOutcomeFailed)
		return "", expectedRunFailure{cause: errors.New("scheduled report admission returned an empty job ID")}
	}
	if err := service.MarkSubmitted(context.WithoutCancel(ctx), run.RunID, jobID); err != nil {
		service.observeRunOutcome(RunOutcomeFailed)
		return "", fmt.Errorf("attach scheduled report job %q: %w", jobID, err)
	}
	service.observeRunOutcome(RunOutcomeSubmitted)
	return jobID, nil
}

// MarkSubmitted is the synchronous search-journal admission barrier. The
// search manager must not publish or execute a scheduled job until this
// durable run-to-job attachment succeeds.
func (service *Service) MarkSubmitted(ctx context.Context, runID, jobID string) error {
	if ctx == nil {
		return ErrInvalidArgument
	}
	return service.store.MarkSubmitted(ctx, runID, jobID)
}

type expectedRunFailure struct{ cause error }

func (failure expectedRunFailure) Error() string { return failure.cause.Error() }

func (failure expectedRunFailure) Unwrap() error { return failure.cause }

// Finish records the terminal state observed for an admitted search job.
func (service *Service) Finish(ctx context.Context, runID string, outcome RunOutcome, failureCategory string) error {
	if ctx == nil {
		return ErrInvalidArgument
	}
	err := service.store.Finish(ctx, runID, outcome, failureCategory, service.clock())
	if err != nil {
		service.observe(featureops.OperationRunOutcome, featureops.OutcomeFailed, 1)
		return err
	}
	service.observeRunOutcome(outcome)
	return nil
}

// Recover marks pre-restart active runs interrupted before scheduling resumes.
func (service *Service) Recover(ctx context.Context) (int64, error) {
	if ctx == nil {
		return 0, ErrInvalidArgument
	}
	count, err := service.store.InterruptActive(ctx, service.clock())
	if err != nil {
		service.observe(featureops.OperationRecovery, featureops.OutcomeFailed, 0)
		return 0, err
	}
	service.observe(featureops.OperationRecovery, featureops.OutcomeSucceeded, nonnegativeCount(count))
	return count, nil
}

// ListRuns returns a bounded owner-scoped run history.
func (service *Service) ListRuns(ctx context.Context, ownerID, savedSearchID string, limit int) ([]Run, error) {
	return service.store.ListRuns(ctx, ownerID, savedSearchID, limit)
}

// ListRunPage returns a bounded keyset page for the HTTP history route.
func (service *Service) ListRunPage(
	ctx context.Context,
	ownerID, savedSearchID string,
	request RunPageRequest,
) (RunPage, error) {
	if ctx == nil {
		return RunPage{}, ErrInvalidArgument
	}
	return service.store.ListRunPage(ctx, ownerID, savedSearchID, request)
}

// GetSchedule returns the current detached configuration without advancing
// the scheduler cursor.
func (service *Service) GetSchedule(ctx context.Context, ownerID, savedSearchID string) (Schedule, error) {
	if ctx == nil {
		return Schedule{}, ErrInvalidArgument
	}
	return service.store.Get(ctx, ownerID, savedSearchID)
}

// CurrentProjections returns list-view schedule state without issuing queries
// per saved search when the production repository supports the batch boundary.
func (service *Service) CurrentProjections(
	ctx context.Context,
	ownerID string,
	savedSearchIDs []string,
) (map[string]CurrentProjection, error) {
	if ctx == nil || len(savedSearchIDs) > MaximumProjectionBatch {
		return nil, ErrInvalidArgument
	}
	return service.store.ListCurrentProjections(ctx, ownerID, savedSearchIDs)
}
