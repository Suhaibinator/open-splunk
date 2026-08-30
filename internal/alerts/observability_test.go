package alerts

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/featureops"
)

func TestObservedRepositoryRecordsLifecycleOutcomes(t *testing.T) {
	t.Parallel()
	metrics := featureops.NewMetrics()
	base := &observabilityRepositoryStub{
		createResult: CreateResult{Disposition: CreateReplayed},
		updateErr:    ErrVersionConflict,
		rotateErr:    ErrCapacity,
		deleteErr:    ErrActiveRun,
	}
	repository := ObserveRepository(base, ObservabilityOptions{Observer: metrics})
	ctx := context.Background()
	if _, err := repository.Create(ctx, CreateRecord{}); err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	if _, err := repository.Update(ctx, UpdateRecord{}); !errors.Is(err, ErrVersionConflict) {
		t.Fatalf("Update() error = %v", err)
	}
	if _, err := repository.SetState(ctx, SetStateRecord{State: AlertEnabled}); err != nil {
		t.Fatalf("SetState(enabled) error = %v", err)
	}
	if _, err := repository.SetState(ctx, SetStateRecord{State: AlertDisabled}); err != nil {
		t.Fatalf("SetState(disabled) error = %v", err)
	}
	if _, err := repository.RotateSecret(ctx, RotateSecretRecord{}); !errors.Is(err, ErrCapacity) {
		t.Fatalf("RotateSecret() error = %v", err)
	}
	if err := repository.DeleteIfIdle(ctx, DeleteRecord{}); !errors.Is(err, ErrActiveRun) {
		t.Fatalf("DeleteIfIdle() error = %v", err)
	}

	snapshot := metrics.Snapshot()
	assertAlertCounter(t, snapshot, featureops.OperationCreate, featureops.OutcomeSkipped, 1, 1)
	assertAlertCounter(t, snapshot, featureops.OperationUpdate, featureops.OutcomeConflict, 1, 1)
	assertAlertCounter(t, snapshot, featureops.OperationEnable, featureops.OutcomeSucceeded, 1, 1)
	assertAlertCounter(t, snapshot, featureops.OperationDisable, featureops.OutcomeSucceeded, 1, 1)
	assertAlertCounter(t, snapshot, featureops.OperationSecretRotation, featureops.OutcomeCapacityRejected, 1, 1)
	assertAlertCounter(t, snapshot, featureops.OperationDelete, featureops.OutcomeConflict, 1, 1)
}

func TestObservedRunRepositoryRecordsClaimsEvaluationDeliveryAndRecovery(t *testing.T) {
	t.Parallel()
	metrics := featureops.NewMetrics()
	base := &observabilityRunRepositoryStub{
		due:              []RunSnapshot{{}, {}},
		runNowActive:     false,
		interruptedCount: 3,
	}
	repository := ObserveRunRepository(base, ObservabilityOptions{Observer: metrics})
	ctx := context.Background()
	if _, err := repository.ClaimDue(ctx, time.Unix(1, 0), 2); err != nil {
		t.Fatalf("ClaimDue() error = %v", err)
	}
	if _, active, err := repository.ClaimRunNow(ctx, "owner", "alert", time.Unix(2, 0)); err != nil || active {
		t.Fatalf("ClaimRunNow() active = %t, error = %v", active, err)
	}
	if err := repository.RecordOverlap(ctx, RunSummary{}); err != nil {
		t.Fatalf("RecordOverlap() error = %v", err)
	}
	for _, summary := range []RunSummary{
		{Outcome: RunNotTriggered, Evaluation: EvaluationFalse},
		{Outcome: RunIndeterminate, Evaluation: EvaluationIndeterminate},
		{Outcome: RunDelivered, Evaluation: EvaluationTrue},
		{Outcome: RunDeliveryUnknown, Evaluation: EvaluationTrue},
	} {
		if err := repository.CompleteRun(ctx, summary); err != nil {
			t.Fatalf("CompleteRun(%s) error = %v", summary.Outcome, err)
		}
	}
	if count, err := repository.InterruptUnfinished(ctx, time.Unix(3, 0)); err != nil || count != 3 {
		t.Fatalf("InterruptUnfinished() = %d, %v", count, err)
	}

	snapshot := metrics.Snapshot()
	assertAlertCounter(t, snapshot, featureops.OperationScheduleClaim, featureops.OutcomeSucceeded, 1, 2)
	assertAlertCounter(t, snapshot, featureops.OperationScheduleClaim, featureops.OutcomeSkipped, 1, 1)
	assertAlertCounter(t, snapshot, featureops.OperationRunOutcome, featureops.OutcomeSkipped, 2, 2)
	assertAlertCounter(t, snapshot, featureops.OperationRunOutcome, featureops.OutcomeNotTriggered, 1, 1)
	assertAlertCounter(t, snapshot, featureops.OperationRunOutcome, featureops.OutcomeIndeterminate, 1, 1)
	assertAlertCounter(t, snapshot, featureops.OperationRunOutcome, featureops.OutcomeDelivered, 1, 1)
	assertAlertCounter(t, snapshot, featureops.OperationRunOutcome, featureops.OutcomeUnknown, 1, 1)
	assertAlertCounter(t, snapshot, featureops.OperationEvaluation, featureops.OutcomeNotTriggered, 1, 1)
	assertAlertCounter(t, snapshot, featureops.OperationEvaluation, featureops.OutcomeIndeterminate, 1, 1)
	assertAlertCounter(t, snapshot, featureops.OperationEvaluation, featureops.OutcomeTriggered, 2, 2)
	assertAlertCounter(t, snapshot, featureops.OperationDelivery, featureops.OutcomeDelivered, 1, 1)
	assertAlertCounter(t, snapshot, featureops.OperationDelivery, featureops.OutcomeUnknown, 1, 1)
	assertAlertCounter(t, snapshot, featureops.OperationRecovery, featureops.OutcomeSucceeded, 1, 3)
}

func TestObservedTestWebhookDelivererRecordsOnlyBoundedOutcome(t *testing.T) {
	t.Parallel()
	metrics := featureops.NewMetrics()
	deliverer := ObserveTestWebhookDeliverer(
		observabilityDelivererStub{result: DeliveryResult{Delivered: true}},
		ObservabilityOptions{Observer: metrics},
	)
	if _, err := deliverer.Deliver(context.Background(), "https://secret.example.test/path", SignedPayload{}); err != nil {
		t.Fatalf("Deliver() error = %v", err)
	}
	assertAlertCounter(
		t,
		metrics.Snapshot(),
		featureops.OperationTestDelivery,
		featureops.OutcomeDelivered,
		1,
		1,
	)
}

func assertAlertCounter(
	t *testing.T,
	snapshot featureops.Snapshot,
	operation featureops.Operation,
	outcome featureops.Outcome,
	wantEvents uint64,
	wantItems uint64,
) {
	t.Helper()
	got := snapshot.Counter(featureops.FeatureAlerts, operation, outcome)
	if got.Events != wantEvents || got.Items != wantItems || got.Bytes != 0 {
		t.Fatalf("counter(%d, %d) = %#v, want events=%d items=%d bytes=0", operation, outcome, got, wantEvents, wantItems)
	}
}

type observabilityRepositoryStub struct {
	Repository
	createResult CreateResult
	createErr    error
	updateErr    error
	rotateErr    error
	deleteErr    error
}

func (repository *observabilityRepositoryStub) Create(context.Context, CreateRecord) (CreateResult, error) {
	return repository.createResult, repository.createErr
}

func (repository *observabilityRepositoryStub) Update(context.Context, UpdateRecord) (Alert, error) {
	return Alert{}, repository.updateErr
}

func (*observabilityRepositoryStub) SetState(context.Context, SetStateRecord) (Alert, error) {
	return Alert{}, nil
}

func (repository *observabilityRepositoryStub) RotateSecret(context.Context, RotateSecretRecord) (Alert, error) {
	return Alert{}, repository.rotateErr
}

func (repository *observabilityRepositoryStub) DeleteIfIdle(context.Context, DeleteRecord) error {
	return repository.deleteErr
}

type observabilityRunRepositoryStub struct {
	RunRepository
	due              []RunSnapshot
	runNowActive     bool
	interruptedCount int64
}

func (repository *observabilityRunRepositoryStub) ClaimDue(context.Context, time.Time, int) ([]RunSnapshot, error) {
	return repository.due, nil
}

func (repository *observabilityRunRepositoryStub) ClaimRunNow(context.Context, string, string, time.Time) (RunSnapshot, bool, error) {
	return RunSnapshot{}, repository.runNowActive, nil
}

func (*observabilityRunRepositoryStub) RecordOverlap(context.Context, RunSummary) error { return nil }

func (*observabilityRunRepositoryStub) CompleteRun(context.Context, RunSummary) error { return nil }

func (repository *observabilityRunRepositoryStub) InterruptUnfinished(context.Context, time.Time) (int64, error) {
	return repository.interruptedCount, nil
}

type observabilityDelivererStub struct {
	result DeliveryResult
	err    error
}

func (deliverer observabilityDelivererStub) Deliver(context.Context, string, SignedPayload) (DeliveryResult, error) {
	return deliverer.result, deliverer.err
}
