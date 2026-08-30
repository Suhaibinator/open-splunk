package alerts

import (
	"context"
	"errors"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/featureops"
)

// ObservabilityOptions configures identity-free operational events for alert
// persistence boundaries. Wrapping is optional and never changes behavior.
type ObservabilityOptions struct {
	Observer featureops.Observer
}

// ObserveRepository decorates alert lifecycle persistence with bounded
// operational events. Read paths deliberately remain silent.
func ObserveRepository(repository Repository, options ObservabilityOptions) Repository {
	if repository == nil || options.Observer == nil {
		return repository
	}
	return &observedRepository{Repository: repository, observer: options.Observer}
}

// ObserveRunRepository decorates scheduler claim, completion, and recovery
// persistence. Events describe aggregate outcomes only and carry no run or
// alert identity.
func ObserveRunRepository(repository RunRepository, options ObservabilityOptions) RunRepository {
	if repository == nil || options.Observer == nil {
		return repository
	}
	return &observedRunRepository{RunRepository: repository, observer: options.Observer}
}

// ObserveTestWebhookDeliverer decorates only operator-initiated test webhook
// delivery. Scheduled delivery outcomes are emitted when their durable run
// summary is committed, which avoids double-counting attempts.
func ObserveTestWebhookDeliverer(deliverer WebhookDeliverer, options ObservabilityOptions) WebhookDeliverer {
	if deliverer == nil || options.Observer == nil {
		return deliverer
	}
	return &observedTestWebhookDeliverer{WebhookDeliverer: deliverer, observer: options.Observer}
}

type observedRepository struct {
	Repository
	observer featureops.Observer
}

func (repository *observedRepository) Create(ctx context.Context, record CreateRecord) (CreateResult, error) {
	result, err := repository.Repository.Create(ctx, record)
	outcome := alertMutationOutcome(err)
	if err == nil && result.Disposition == CreateReplayed {
		outcome = featureops.OutcomeSkipped
	}
	repository.observe(featureops.OperationCreate, outcome)
	return result, err
}

func (repository *observedRepository) Update(ctx context.Context, record UpdateRecord) (Alert, error) {
	alert, err := repository.Repository.Update(ctx, record)
	repository.observe(featureops.OperationUpdate, alertMutationOutcome(err))
	return alert, err
}

func (repository *observedRepository) SetState(ctx context.Context, record SetStateRecord) (Alert, error) {
	alert, err := repository.Repository.SetState(ctx, record)
	operation := featureops.OperationDisable
	if record.State == AlertEnabled {
		operation = featureops.OperationEnable
	}
	repository.observe(operation, alertMutationOutcome(err))
	return alert, err
}

func (repository *observedRepository) RotateSecret(ctx context.Context, record RotateSecretRecord) (Alert, error) {
	alert, err := repository.Repository.RotateSecret(ctx, record)
	repository.observe(featureops.OperationSecretRotation, alertMutationOutcome(err))
	return alert, err
}

func (repository *observedRepository) DeleteIfIdle(ctx context.Context, record DeleteRecord) error {
	err := repository.Repository.DeleteIfIdle(ctx, record)
	repository.observe(featureops.OperationDelete, alertMutationOutcome(err))
	return err
}

func (repository *observedRepository) observe(operation featureops.Operation, outcome featureops.Outcome) {
	emitAlert(repository.observer, operation, outcome, 1)
}

type observedRunRepository struct {
	RunRepository
	observer featureops.Observer
}

func (repository *observedRunRepository) ClaimDue(ctx context.Context, now time.Time, limit int) ([]RunSnapshot, error) {
	claimed, err := repository.RunRepository.ClaimDue(ctx, now, limit)
	if err != nil {
		emitAlert(repository.observer, featureops.OperationScheduleClaim, alertMutationOutcome(err), 0)
	} else if len(claimed) != 0 {
		emitAlert(repository.observer, featureops.OperationScheduleClaim, featureops.OutcomeSucceeded, uint64(len(claimed)))
	}
	return claimed, err
}

func (repository *observedRunRepository) ClaimRunNow(ctx context.Context, ownerID, alertID string, now time.Time) (RunSnapshot, bool, error) {
	snapshot, active, err := repository.RunRepository.ClaimRunNow(ctx, ownerID, alertID, now)
	outcome := alertMutationOutcome(err)
	if err == nil && !active {
		outcome = featureops.OutcomeSkipped
	}
	emitAlert(repository.observer, featureops.OperationScheduleClaim, outcome, 1)
	if err == nil && !active {
		emitAlert(repository.observer, featureops.OperationRunOutcome, featureops.OutcomeSkipped, 1)
	}
	return snapshot, active, err
}

func (repository *observedRunRepository) RecordOverlap(ctx context.Context, summary RunSummary) error {
	err := repository.RunRepository.RecordOverlap(ctx, summary)
	outcome := alertMutationOutcome(err)
	if err == nil {
		outcome = featureops.OutcomeSkipped
	}
	emitAlert(repository.observer, featureops.OperationRunOutcome, outcome, 1)
	return err
}

func (repository *observedRunRepository) CompleteRun(ctx context.Context, summary RunSummary) error {
	err := repository.RunRepository.CompleteRun(ctx, summary)
	if err != nil {
		emitAlert(repository.observer, featureops.OperationRunOutcome, alertMutationOutcome(err), 1)
		return err
	}
	emitAlert(repository.observer, featureops.OperationRunOutcome, alertRunOutcome(summary.Outcome), 1)
	if summary.Evaluation != "" {
		emitAlert(repository.observer, featureops.OperationEvaluation, alertEvaluationOutcome(summary.Evaluation), 1)
	}
	if outcome, present := alertDeliveryOutcome(summary.Outcome); present {
		emitAlert(repository.observer, featureops.OperationDelivery, outcome, 1)
	}
	return nil
}

func (repository *observedRunRepository) InterruptUnfinished(ctx context.Context, now time.Time) (int64, error) {
	count, err := repository.RunRepository.InterruptUnfinished(ctx, now)
	if err != nil {
		emitAlert(repository.observer, featureops.OperationRecovery, alertMutationOutcome(err), 0)
		return count, err
	}
	emitAlert(repository.observer, featureops.OperationRecovery, featureops.OutcomeSucceeded, nonnegativeAlertCount(count))
	return count, nil
}

type observedTestWebhookDeliverer struct {
	WebhookDeliverer
	observer featureops.Observer
}

func (deliverer *observedTestWebhookDeliverer) Deliver(ctx context.Context, endpoint string, payload SignedPayload) (DeliveryResult, error) {
	result, err := deliverer.WebhookDeliverer.Deliver(ctx, endpoint, payload)
	outcome := featureops.OutcomeFailed
	if err == nil && result.Delivered {
		outcome = featureops.OutcomeDelivered
	}
	emitAlert(deliverer.observer, featureops.OperationTestDelivery, outcome, 1)
	return result, err
}

func emitAlert(observer featureops.Observer, operation featureops.Operation, outcome featureops.Outcome, items uint64) {
	featureops.Emit(observer, featureops.Event{
		Feature: featureops.FeatureAlerts, Operation: operation,
		Outcome: outcome, Items: items,
	})
}

func alertMutationOutcome(err error) featureops.Outcome {
	switch {
	case err == nil:
		return featureops.OutcomeSucceeded
	case errors.Is(err, ErrCapacity):
		return featureops.OutcomeCapacityRejected
	case errors.Is(err, ErrVersionConflict), errors.Is(err, ErrIdempotencyConflict), errors.Is(err, ErrActiveRun):
		return featureops.OutcomeConflict
	default:
		return featureops.OutcomeFailed
	}
}

func alertRunOutcome(outcome RunOutcome) featureops.Outcome {
	switch outcome {
	case RunDelivered:
		return featureops.OutcomeDelivered
	case RunNotTriggered:
		return featureops.OutcomeNotTriggered
	case RunIndeterminate:
		return featureops.OutcomeIndeterminate
	case RunSearchCanceled:
		return featureops.OutcomeCanceled
	case RunSearchExpired:
		return featureops.OutcomeExpired
	case RunInterrupted:
		return featureops.OutcomeInterrupted
	case RunOverlapSkipped, RunDeliverySkipped:
		return featureops.OutcomeSkipped
	case RunDeliveryUnknown:
		return featureops.OutcomeUnknown
	default:
		return featureops.OutcomeFailed
	}
}

func alertEvaluationOutcome(certainty EvaluationCertainty) featureops.Outcome {
	switch certainty {
	case EvaluationTrue:
		return featureops.OutcomeTriggered
	case EvaluationFalse:
		return featureops.OutcomeNotTriggered
	case EvaluationIndeterminate:
		return featureops.OutcomeIndeterminate
	default:
		return featureops.OutcomeFailed
	}
}

func alertDeliveryOutcome(outcome RunOutcome) (featureops.Outcome, bool) {
	switch outcome {
	case RunDelivered:
		return featureops.OutcomeDelivered, true
	case RunDeliveryFailed:
		return featureops.OutcomeFailed, true
	case RunDeliverySkipped:
		return featureops.OutcomeSkipped, true
	case RunDeliveryUnknown:
		return featureops.OutcomeUnknown, true
	default:
		return featureops.OutcomeInvalid, false
	}
}

func nonnegativeAlertCount(count int64) uint64 {
	if count <= 0 {
		return 0
	}
	return uint64(count)
}
