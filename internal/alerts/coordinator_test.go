package alerts

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestCoordinatorExecutesTriggeredDeliveryEndToEnd(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, time.August, 30, 9, 0, 0, 0, time.UTC)
	snapshot := coordinatorSnapshot(now)
	snapshot.Definition.SearchTimezone = "Pacific/Chatham"
	runs := &coordinatorRunStore{due: []RunSnapshot{snapshot}}
	order := &orderedCalls{}
	admission := &coordinatorAdmission{jobID: "job-1"}
	jobs := coordinatorJobReader{job: SearchJobSnapshot{
		ID: "job-1", State: SearchJobCompleted, StartedAt: now.Add(time.Second),
		FinishedAt: now.Add(2 * time.Second), ExpiresAt: now.Add(10 * time.Minute),
		ResultCount: 12,
	}}
	poller := coordinatorPoller{wait: func(ctx context.Context, ownerID, jobID string, reader SearchJobReader) (SearchJobSnapshot, error) {
		return reader.ReadAlertSearchJob(ctx, ownerID, jobID)
	}}
	results := &coordinatorResults{result: SearchResults{
		Schema: []ResultField{{Name: "host", Type: "string"}},
		Rows:   []map[string]any{{"host": "one"}, {"host": "two"}, {"host": "three"}}, More: true,
	}}
	retention := &coordinatorRetention{expiresAt: now.Add(50 * time.Minute), order: order}
	secret := bytes.Repeat([]byte{0x5a}, SecretBytes)
	expectedSecret := append([]byte(nil), secret...)
	authorizer := &coordinatorAuthorizer{opened: OpenedDeliverySecrets{Endpoint: "https://hooks.example.test/alert", Secret: secret}, order: order}
	deliverer := &coordinatorDeliverer{order: order, deliver: func(_ context.Context, endpoint string, signed SignedPayload) (DeliveryResult, error) {
		if endpoint != "https://hooks.example.test/alert" || !VerifySignature(signed.Timestamp, signed.Body, expectedSecret, signed.Headers[HeaderSignature][3:]) {
			t.Fatal("delivery did not receive the authorized endpoint and exact signed body")
		}
		var payload WebhookPayload
		if err := json.Unmarshal(signed.Body, &payload); err != nil {
			t.Fatalf("decode payload: %v", err)
		}
		if payload.ResultsURL != "https://splunk.example.test/search/?searchJobId=job-1" || len(payload.SampleRows) != 2 || !payload.SampleTruncated {
			t.Fatalf("payload = %#v", payload)
		}
		return DeliveryResult{Category: DeliverySucceeded, Delivered: true, StatusCode: 204, AttemptedAt: now.Add(3 * time.Second)}, nil
	}}
	coordinator := newTestCoordinator(t, now.Add(3*time.Second), runs, admission, jobs, results, retention, authorizer, deliverer, poller, 2)
	if err := coordinator.Step(context.Background(), now); err != nil {
		t.Fatalf("Step() error = %v", err)
	}
	completed := waitForCompleted(t, runs, 1)[0]
	if completed.Outcome != RunDelivered || completed.Evaluation != EvaluationTrue || completed.ResultCount != 12 || !completed.ResultCountExact {
		t.Fatalf("completed = %#v", completed)
	}
	if !completed.SearchJobExpiresAt.Equal(retention.expiresAt) {
		t.Fatalf("retained expiry = %v", completed.SearchJobExpiresAt)
	}
	if got := order.snapshot(); len(got) != 3 || got[0] != "retention" || got[1] != "authorization" || got[2] != "delivery" {
		t.Fatalf("trigger order = %#v", got)
	}
	if admission.request.Retention != snapshot.DispatchRetention || admission.request.AlertRunID != snapshot.AlertRunID || admission.request.Timezone != snapshot.Definition.SearchTimezone || len(admission.request.IndexScope) != 1 || admission.request.IndexScope[0] != "main" {
		t.Fatalf("admission request = %#v", admission.request)
	}
	for index, value := range authorizer.opened.Secret {
		if value != 0 {
			t.Fatalf("authorized secret byte %d was not cleared", index)
		}
	}
}

func TestCoordinatorMissingPublicBaseURLFailsBeforeDeliveryPreparation(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, time.August, 30, 9, 0, 0, 0, time.UTC)
	runs := &coordinatorRunStore{
		due:             []RunSnapshot{coordinatorSnapshot(now)},
		completedSignal: make(chan struct{}, MaximumAlertsPerOwner),
		attemptSignal:   make(chan struct{}, MaximumAlertsPerOwner),
	}
	jobs := coordinatorJobReader{job: SearchJobSnapshot{
		ID: "job-1", State: SearchJobCompleted, StartedAt: now.Add(time.Second),
		FinishedAt: now.Add(2 * time.Second), ExpiresAt: now.Add(10 * time.Minute), ResultCount: 12,
	}}
	poller := coordinatorPoller{wait: func(ctx context.Context, ownerID, jobID string, reader SearchJobReader) (SearchJobSnapshot, error) {
		return reader.ReadAlertSearchJob(ctx, ownerID, jobID)
	}}
	retention := &coordinatorRetention{expiresAt: now.Add(50 * time.Minute)}
	authorizer := &coordinatorAuthorizer{opened: OpenedDeliverySecrets{Endpoint: "https://hooks.example.test/alert", Secret: bytes.Repeat([]byte{0x5a}, SecretBytes)}}
	deliverer := &coordinatorDeliverer{deliver: func(context.Context, string, SignedPayload) (DeliveryResult, error) {
		t.Error("delivery must not be attempted without a public base URL")
		return DeliveryResult{}, errors.New("unexpected delivery")
	}}
	coordinator, err := NewCoordinator(CoordinatorOptions{
		RunRepository: runs, Admission: &coordinatorAdmission{jobID: "job-1"}, Jobs: jobs,
		Results: &coordinatorResults{}, Retention: retention, Authorizer: authorizer, Deliverer: deliverer,
		Poller: poller, Clock: func() time.Time { return now }, ConcurrencyLimit: 1,
	})
	if err != nil {
		t.Fatalf("NewCoordinator() error = %v", err)
	}
	t.Cleanup(func() {
		shutdownContext, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := coordinator.Close(shutdownContext); err != nil {
			t.Errorf("Close() error = %v", err)
		}
	})
	if err := coordinator.Step(context.Background(), now); err != nil {
		t.Fatalf("Step() error = %v", err)
	}
	completed := waitForCompleted(t, runs, 1)[0]
	if completed.Outcome != RunDeliveryFailed || completed.FailureCategory != string(FailurePublicBaseURL) || completed.Evaluation != EvaluationTrue {
		t.Fatalf("completed = %#v", completed)
	}
	retention.mu.Lock()
	retentionCalls := retention.calls
	retention.mu.Unlock()
	authorizer.mu.Lock()
	authorizerCalls := authorizer.calls
	authorizer.mu.Unlock()
	if retentionCalls != 0 || authorizerCalls != 0 {
		t.Fatalf("retention calls = %d, authorization calls = %d, want none before the results link resolves", retentionCalls, authorizerCalls)
	}
}

func TestCoordinatorEvaluationOutcomesDoNotDeliver(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, time.August, 30, 10, 0, 0, 0, time.UTC)
	tests := []struct {
		name      string
		condition Condition
		count     uint64
		truncated bool
		want      RunOutcome
		certainty EvaluationCertainty
	}{
		{name: "exact false", condition: Condition{Operator: ConditionGreaterThan, Threshold: 4}, count: 4, want: RunNotTriggered, certainty: EvaluationFalse},
		{name: "truncated indeterminate", condition: Condition{Operator: ConditionEqual, Threshold: 8}, count: 8, truncated: true, want: RunIndeterminate, certainty: EvaluationIndeterminate},
		{name: "truncated less below threshold stays indeterminate", condition: Condition{Operator: ConditionLessThan, Threshold: 8}, count: 7, truncated: true, want: RunIndeterminate, certainty: EvaluationIndeterminate},
		{name: "truncated less at threshold is disproved", condition: Condition{Operator: ConditionLessThan, Threshold: 8}, count: 9, truncated: true, want: RunNotTriggered, certainty: EvaluationFalse},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			snapshot := coordinatorSnapshot(now)
			snapshot.Definition.Condition = test.condition
			runs := &coordinatorRunStore{due: []RunSnapshot{snapshot}}
			retention := &coordinatorRetention{expiresAt: now.Add(time.Hour)}
			authorizer := &coordinatorAuthorizer{opened: OpenedDeliverySecrets{Secret: make([]byte, SecretBytes)}}
			deliverer := &coordinatorDeliverer{}
			coordinator := newTestCoordinator(t, now, runs, &coordinatorAdmission{jobID: "job-1"}, coordinatorJobReader{job: SearchJobSnapshot{
				ID: "job-1", State: SearchJobCompleted, FinishedAt: now, ResultCount: test.count, ResultsTruncated: test.truncated,
			}}, &coordinatorResults{}, retention, authorizer, deliverer, coordinatorPoller{}, 1)
			if err := coordinator.Step(context.Background(), now); err != nil {
				t.Fatalf("Step() error = %v", err)
			}
			completed := waitForCompleted(t, runs, 1)
			if completed[0].Outcome != test.want || completed[0].Evaluation != test.certainty {
				t.Fatalf("completed = %#v", completed)
			}
			if retention.calls != 0 || authorizer.calls != 0 || deliverer.calls != 0 {
				t.Fatalf("unexpected trigger side effects: retention=%d authorization=%d delivery=%d", retention.calls, authorizer.calls, deliverer.calls)
			}
		})
	}
}

func TestCoordinatorSecretRotationWinsBeforeDeliveryPreparation(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, time.August, 30, 10, 30, 0, 0, time.UTC)
	runs := &coordinatorRunStore{due: []RunSnapshot{coordinatorSnapshot(now)}}
	results := &coordinatorResults{err: errors.New("sample read must not run")}
	retention := &coordinatorRetention{expiresAt: now.Add(time.Hour)}
	authorizer := &coordinatorAuthorizer{err: ErrSecretRotated}
	deliverer := &coordinatorDeliverer{}
	coordinator := newTestCoordinator(t, now, runs, &coordinatorAdmission{jobID: "job-1"}, coordinatorJobReader{job: SearchJobSnapshot{
		ID: "job-1", State: SearchJobCompleted, FinishedAt: now, ResultCount: 5,
	}}, results, retention, authorizer, deliverer, coordinatorPoller{}, 1)
	var deliveryIDCalls atomic.Int64
	coordinator.deliveryID = func() (string, error) {
		deliveryIDCalls.Add(1)
		return "", errors.New("delivery ID must not be generated")
	}

	if err := coordinator.Step(context.Background(), now); err != nil {
		t.Fatalf("Step() error = %v", err)
	}
	completed := waitForCompleted(t, runs, 1)
	if completed[0].Outcome != RunDeliverySkipped || completed[0].FailureCategory != string(FailureSecretRotated) {
		t.Fatalf("completed = %#v", completed[0])
	}
	if retention.calls != 1 || authorizer.calls != 1 || results.calls != 0 || deliveryIDCalls.Load() != 0 || deliverer.calls != 0 {
		t.Fatalf("side effects: retention=%d authorization=%d results=%d delivery IDs=%d delivery=%d", retention.calls, authorizer.calls, results.calls, deliveryIDCalls.Load(), deliverer.calls)
	}
}

func TestCoordinatorSearchFailureAndCancellationNeverDeliver(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, time.August, 30, 11, 0, 0, 0, time.UTC)
	for _, test := range []struct {
		name    string
		ctx     func() context.Context
		job     SearchJobSnapshot
		outcome RunOutcome
	}{
		{name: "failed", ctx: context.Background, job: SearchJobSnapshot{ID: "job-1", State: SearchJobFailed, FailureCategory: "EXECUTION_FAILED"}, outcome: RunSearchFailed},
		{name: "canceled context", ctx: func() context.Context { ctx, cancel := context.WithCancel(context.Background()); cancel(); return ctx }, outcome: RunInterrupted},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			runs := &coordinatorRunStore{due: []RunSnapshot{coordinatorSnapshot(now)}}
			retention := &coordinatorRetention{}
			deliverer := &coordinatorDeliverer{}
			coordinator := newTestCoordinator(t, now, runs, &coordinatorAdmission{jobID: "job-1"}, coordinatorJobReader{job: test.job}, &coordinatorResults{}, retention, &coordinatorAuthorizer{}, deliverer, coordinatorPoller{}, 1)
			err := coordinator.Step(test.ctx(), now)
			if test.outcome == RunInterrupted && !errors.Is(err, context.Canceled) {
				t.Fatalf("Step() error = %v", err)
			}
			if test.outcome == RunInterrupted {
				if completed := runs.completedSnapshot(); len(completed) != 0 {
					t.Fatalf("canceled submission completed unexpected runs: %#v", completed)
				}
				return
			}
			completed := waitForCompleted(t, runs, 1)
			if completed[0].Outcome != test.outcome || retention.calls != 0 || deliverer.calls != 0 {
				t.Fatalf("completed = %#v, retention=%d, delivery=%d", completed, retention.calls, deliverer.calls)
			}
			if test.outcome == RunInterrupted && (!runs.completeHadDeadline || runs.completeWasCanceled) {
				t.Fatalf("completion context deadline/canceled = %t/%t", runs.completeHadDeadline, runs.completeWasCanceled)
			}
		})
	}
}

func TestCoordinatorStepBoundsConcurrentClaimsWithoutSleeps(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, time.August, 30, 12, 0, 0, 0, time.UTC)
	claims := make([]RunSnapshot, 5)
	for index := range claims {
		claims[index] = coordinatorSnapshot(now.Add(time.Duration(index) * time.Minute))
		claims[index].AlertID = "alert-" + string(rune('a'+index))
		claims[index].AlertRunID = "run-" + string(rune('a'+index))
	}
	runs := &coordinatorRunStore{due: claims}
	started := make(chan struct{}, len(claims))
	release := make(chan struct{}, len(claims))
	var active atomic.Int64
	var maximum atomic.Int64
	poller := coordinatorPoller{wait: func(_ context.Context, _, jobID string, _ SearchJobReader) (SearchJobSnapshot, error) {
		current := active.Add(1)
		for {
			observed := maximum.Load()
			if current <= observed || maximum.CompareAndSwap(observed, current) {
				break
			}
		}
		started <- struct{}{}
		<-release
		active.Add(-1)
		return SearchJobSnapshot{ID: jobID, State: SearchJobFailed}, nil
	}}
	coordinator := newTestCoordinator(t, now, runs, &coordinatorAdmission{jobID: "job-shared"}, coordinatorJobReader{}, &coordinatorResults{}, &coordinatorRetention{}, &coordinatorAuthorizer{}, &coordinatorDeliverer{}, poller, 2)
	done := make(chan error, 1)
	go func() { done <- coordinator.Step(context.Background(), now) }()
	<-started
	<-started
	if maximum.Load() != 2 {
		t.Fatalf("maximum active = %d, want 2", maximum.Load())
	}
	if err := <-done; err != nil {
		t.Fatalf("Step() error = %v", err)
	}
	release <- struct{}{}
	<-started
	for index := 0; index < len(claims)-1; index++ {
		release <- struct{}{}
	}
	completed := waitForCompleted(t, runs, len(claims))
	if maximum.Load() != 2 || len(completed) != len(claims) {
		t.Fatalf("maximum=%d completed=%d", maximum.Load(), len(completed))
	}
}

func TestCoordinatorRunNowOverlapAndRecover(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, time.August, 30, 13, 0, 0, 0, time.UTC)
	snapshot := coordinatorSnapshot(now)
	runs := &coordinatorRunStore{runNow: snapshot, runNowActive: false, interrupted: 3}
	coordinator := newTestCoordinator(t, now, runs, &coordinatorAdmission{}, coordinatorJobReader{}, &coordinatorResults{}, &coordinatorRetention{}, &coordinatorAuthorizer{}, &coordinatorDeliverer{}, coordinatorPoller{}, 1)
	summary, err := coordinator.RunNow(context.Background(), snapshot.OwnerID, snapshot.AlertID)
	if err != nil || summary.Outcome != RunOverlapSkipped {
		t.Fatalf("RunNow() = %#v, %v", summary, err)
	}
	count, err := coordinator.Recover(context.Background())
	if err != nil || count != 3 || !runs.recoveredAt.Equal(now) {
		t.Fatalf("Recover() = %d, %v at %v", count, err, runs.recoveredAt)
	}
}

func TestCoordinatorRunNowReturnsBeforeExecutionCompletes(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, time.August, 30, 13, 30, 0, 0, time.UTC)
	snapshot := coordinatorSnapshot(now)
	runs := &coordinatorRunStore{runNow: snapshot, runNowActive: true}
	started := make(chan struct{})
	release := make(chan struct{})
	poller := coordinatorPoller{wait: func(ctx context.Context, _, jobID string, _ SearchJobReader) (SearchJobSnapshot, error) {
		close(started)
		select {
		case <-release:
			return SearchJobSnapshot{ID: jobID, State: SearchJobFailed}, nil
		case <-ctx.Done():
			return SearchJobSnapshot{}, ctx.Err()
		}
	}}
	coordinator := newTestCoordinator(t, now, runs, &coordinatorAdmission{jobID: "job-1"}, coordinatorJobReader{}, &coordinatorResults{}, &coordinatorRetention{}, &coordinatorAuthorizer{}, &coordinatorDeliverer{}, poller, 1)
	summary, err := coordinator.RunNow(context.Background(), snapshot.OwnerID, snapshot.AlertID)
	if err != nil || summary.Outcome != RunSearching || summary.AlertRunID != snapshot.AlertRunID {
		t.Fatalf("RunNow() = %#v, %v", summary, err)
	}
	<-started
	if completed := runs.completedSnapshot(); len(completed) != 0 {
		t.Fatalf("RunNow() waited for terminal execution: %#v", completed)
	}
	close(release)
	if completed := waitForCompleted(t, runs, 1); completed[0].Outcome != RunSearchFailed {
		t.Fatalf("completed = %#v", completed)
	}
}

func TestCoordinatorCloseCancelsAndCompletesClaimedRuns(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, time.August, 30, 14, 0, 0, 0, time.UTC)
	runs := &coordinatorRunStore{due: []RunSnapshot{coordinatorSnapshot(now)}}
	started := make(chan struct{})
	poller := coordinatorPoller{wait: func(ctx context.Context, _, _ string, _ SearchJobReader) (SearchJobSnapshot, error) {
		close(started)
		<-ctx.Done()
		return SearchJobSnapshot{}, ctx.Err()
	}}
	coordinator := newTestCoordinator(t, now, runs, &coordinatorAdmission{jobID: "job-1"}, coordinatorJobReader{}, &coordinatorResults{}, &coordinatorRetention{}, &coordinatorAuthorizer{}, &coordinatorDeliverer{}, poller, 1)
	if err := coordinator.Step(context.Background(), now); err != nil {
		t.Fatalf("Step() error = %v", err)
	}
	<-started
	shutdownContext, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := coordinator.Close(shutdownContext); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	completed := waitForCompleted(t, runs, 1)
	if completed[0].Outcome != RunInterrupted || completed[0].FailureCategory != "CANCELED" {
		t.Fatalf("completed = %#v", completed)
	}
	if err := coordinator.Step(context.Background(), now); !errors.Is(err, ErrClosed) {
		t.Fatalf("Step() after Close error = %v", err)
	}
}

func TestCoordinatorRetriesTerminalWriteWithoutRepeatingDelivery(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, time.August, 30, 14, 30, 0, 0, time.UTC)
	runs := &coordinatorRunStore{due: []RunSnapshot{coordinatorSnapshot(now)}, completeFailures: 2}
	retention := &coordinatorRetention{expiresAt: now.Add(time.Hour)}
	authorizer := &coordinatorAuthorizer{opened: OpenedDeliverySecrets{
		Endpoint: "https://hooks.example.test/alert",
		Secret:   bytes.Repeat([]byte{0x4c}, SecretBytes),
	}}
	deliverer := &coordinatorDeliverer{deliver: func(context.Context, string, SignedPayload) (DeliveryResult, error) {
		return DeliveryResult{Category: DeliverySucceeded, Delivered: true, StatusCode: 204, AttemptedAt: now}, nil
	}}
	retryWaiter := &coordinatorRetryWaiter{}
	coordinator := newTestCoordinator(t, now, runs, &coordinatorAdmission{jobID: "job-1"}, coordinatorJobReader{job: SearchJobSnapshot{
		ID: "job-1", State: SearchJobCompleted, FinishedAt: now, ResultCount: 5,
	}}, &coordinatorResults{}, retention, authorizer, deliverer, coordinatorPoller{}, 1)
	coordinator.completionRetryWait = retryWaiter.wait

	if err := coordinator.Step(context.Background(), now); err != nil {
		t.Fatalf("Step() error = %v", err)
	}
	completed := waitForCompleted(t, runs, 1)
	if completed[0].Outcome != RunDelivered || !completed[0].FinishedAt.Equal(now) {
		t.Fatalf("completed = %#v", completed[0])
	}
	attempts, attempted := runs.completionAttemptsSnapshot()
	if attempts != 3 || len(attempted) != 3 {
		t.Fatalf("completion attempts = %d, summaries = %d, want 3", attempts, len(attempted))
	}
	for index, summary := range attempted {
		if summary.DeliveryID != "delivery-1" || summary.Outcome != RunDelivered || !summary.FinishedAt.Equal(now) {
			t.Fatalf("completion summary %d = %#v", index, summary)
		}
	}
	if got := retryWaiter.delaysSnapshot(); len(got) != 2 || got[0] != DefaultCompletionRetryDelay || got[1] != 2*DefaultCompletionRetryDelay {
		t.Fatalf("retry delays = %v", got)
	}
	if deliverer.calls != 1 || authorizer.calls != 1 || retention.calls != 1 {
		t.Fatalf("side-effect calls: delivery=%d authorization=%d retention=%d", deliverer.calls, authorizer.calls, retention.calls)
	}
}

func TestCoordinatorCloseCancelsCompletionRetryAndJoinsWorker(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, time.August, 30, 15, 0, 0, 0, time.UTC)
	runs := &coordinatorRunStore{due: []RunSnapshot{coordinatorSnapshot(now)}, completeFailures: 100}
	retryStarted := make(chan struct{})
	retryExited := make(chan struct{})
	var started sync.Once
	var exited sync.Once
	coordinator := newTestCoordinator(t, now, runs, &coordinatorAdmission{jobID: "job-1"}, coordinatorJobReader{job: SearchJobSnapshot{
		ID: "job-1", State: SearchJobFailed,
	}}, &coordinatorResults{}, &coordinatorRetention{}, &coordinatorAuthorizer{}, &coordinatorDeliverer{}, coordinatorPoller{}, 1)
	coordinator.completionRetryWait = func(ctx context.Context, _ time.Duration) error {
		started.Do(func() { close(retryStarted) })
		<-ctx.Done()
		exited.Do(func() { close(retryExited) })
		return ctx.Err()
	}

	if err := coordinator.Step(context.Background(), now); err != nil {
		t.Fatalf("Step() error = %v", err)
	}
	<-retryStarted
	shutdownContext, cancel := context.WithCancel(context.Background())
	cancel()
	if err := coordinator.Close(shutdownContext); !errors.Is(err, context.Canceled) {
		t.Fatalf("Close() error = %v, want context.Canceled", err)
	}
	select {
	case <-retryExited:
	default:
		t.Fatal("Close returned before the completion retry worker exited")
	}
	if attempts, _ := runs.completionAttemptsSnapshot(); attempts != 1 {
		t.Fatalf("completion attempts = %d, want 1 before shutdown cancellation", attempts)
	}
	if err := coordinator.Step(context.Background(), now); !errors.Is(err, ErrClosed) {
		t.Fatalf("Step() after Close error = %v", err)
	}
}

func TestCompletionRetryDelayIsBounded(t *testing.T) {
	t.Parallel()
	if got := completionRetryDelay(0); got != DefaultCompletionRetryDelay {
		t.Fatalf("initial retry delay = %v", got)
	}
	if got := completionRetryDelay(1_000); got != MaximumCompletionRetryDelay {
		t.Fatalf("maximum retry delay = %v", got)
	}
}

func TestCoordinatorDoesNotRetryPermanentCompletionConflict(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, time.August, 30, 9, 0, 0, 0, time.UTC)
	runs := &coordinatorRunStore{due: []RunSnapshot{coordinatorSnapshot(now)}, completeErr: ErrVersionConflict}
	coordinator := newTestCoordinator(
		t, now, runs, &coordinatorAdmission{jobID: "job-1"},
		coordinatorJobReader{job: SearchJobSnapshot{ID: "job-1", State: SearchJobFailed, FinishedAt: now}},
		&coordinatorResults{}, &coordinatorRetention{}, &coordinatorAuthorizer{},
		&coordinatorDeliverer{}, coordinatorPoller{}, 1,
	)
	if err := coordinator.Step(context.Background(), now); err != nil {
		t.Fatalf("Step() error = %v", err)
	}
	select {
	case <-runs.attemptSignal:
	case <-time.After(time.Second):
		t.Fatal("coordinator did not attempt completion")
	}
	if attempts, _ := runs.completionAttemptsSnapshot(); attempts != 1 {
		t.Fatalf("completion attempts = %d, want 1", attempts)
	}
}

func TestCoordinatorErrorCallbackCannotDelayShutdown(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, time.August, 30, 16, 0, 0, 0, time.UTC)
	callbackStarted := make(chan struct{})
	releaseCallback := make(chan struct{})
	defer close(releaseCallback)
	coordinator := newErrorReportingCoordinator(t, now, func(error) {
		close(callbackStarted)
		<-releaseCallback
	})
	if err := coordinator.Step(context.Background(), now); err != nil {
		t.Fatalf("Step() error = %v", err)
	}
	select {
	case <-callbackStarted:
	case <-time.After(time.Second):
		t.Fatal("error callback did not start")
	}
	closed := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		closed <- coordinator.Close(ctx)
	}()
	select {
	case err := <-closed:
		if err != nil {
			t.Fatalf("Close() error = %v", err)
		}
	case <-time.After(250 * time.Millisecond):
		t.Fatal("Close() waited for error callback")
	}
}

func TestCoordinatorContainsErrorCallbackPanic(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, time.August, 30, 16, 30, 0, 0, time.UTC)
	callbackStarted := make(chan struct{})
	coordinator := newErrorReportingCoordinator(t, now, func(error) {
		close(callbackStarted)
		panic("callback failure")
	})
	if err := coordinator.Step(context.Background(), now); err != nil {
		t.Fatalf("Step() error = %v", err)
	}
	select {
	case <-callbackStarted:
	case <-time.After(time.Second):
		t.Fatal("error callback did not start")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := coordinator.Close(ctx); err != nil {
		t.Fatalf("Close() after callback panic = %v", err)
	}
}

func newErrorReportingCoordinator(t *testing.T, now time.Time, onError func(error)) *Coordinator {
	t.Helper()
	runs := &coordinatorRunStore{
		due:             []RunSnapshot{coordinatorSnapshot(now)},
		completedSignal: make(chan struct{}, 1),
		attemptSignal:   make(chan struct{}, 1),
	}
	coordinator, err := NewCoordinator(CoordinatorOptions{
		RunRepository: runs,
		Admission:     &coordinatorAdmission{jobID: "job-1"},
		Jobs:          coordinatorJobReader{},
		Results:       &coordinatorResults{},
		Retention:     &coordinatorRetention{},
		Authorizer:    &coordinatorAuthorizer{},
		Deliverer:     &coordinatorDeliverer{},
		Poller: coordinatorPoller{wait: func(context.Context, string, string, SearchJobReader) (SearchJobSnapshot, error) {
			return SearchJobSnapshot{}, errors.New("search failed")
		}},
		Clock:            func() time.Time { return now },
		ConcurrencyLimit: 1,
		QueueCapacity:    1,
		OnError:          onError,
	})
	if err != nil {
		t.Fatalf("NewCoordinator() error = %v", err)
	}
	return coordinator
}

func coordinatorSnapshot(now time.Time) RunSnapshot {
	return RunSnapshot{
		AlertID: "alert-1", AlertRunID: "run-1", AlertVersion: 2, OwnerID: "owner-1", TenantID: "tenant-1",
		Definition: Definition{
			Name: "Errors", Application: "search", SPL: "index=main", IndexScope: []string{"main"}, Earliest: "-5m", Latest: "now",
			Timezone: "UTC", Condition: Condition{Operator: ConditionGreaterThan, Threshold: 4}, SampleRows: 2,
		},
		ScheduledAt: now, ClaimedAt: now, DispatchRetention: 10 * time.Minute, TriggeredRetention: 50 * time.Minute,
	}
}

func newTestCoordinator(t *testing.T, now time.Time, runs RunRepository, admission SearchAdmission, jobs SearchJobReader, results SearchResultReader, retention DurableRetentionUpdater, authorizer DeliveryAuthorizer, deliverer WebhookDeliverer, poller SearchJobPoller, concurrency int) *Coordinator {
	t.Helper()
	coordinator, err := NewCoordinator(CoordinatorOptions{
		RunRepository: runs, Admission: admission, Jobs: jobs, Results: results, Retention: retention,
		Authorizer: authorizer, Deliverer: deliverer, Poller: poller, Clock: func() time.Time { return now },
		DeliveryID: func() (string, error) { return "delivery-1", nil }, PublicBaseURL: "https://splunk.example.test",
		ConcurrencyLimit: concurrency,
	})
	if err != nil {
		t.Fatalf("NewCoordinator() error = %v", err)
	}
	if store, ok := runs.(*coordinatorRunStore); ok {
		store.mu.Lock()
		if store.completedSignal == nil {
			store.completedSignal = make(chan struct{}, MaximumAlertsPerOwner)
		}
		if store.attemptSignal == nil {
			store.attemptSignal = make(chan struct{}, MaximumAlertsPerOwner)
		}
		store.mu.Unlock()
	}
	t.Cleanup(func() {
		shutdownContext, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := coordinator.Close(shutdownContext); err != nil {
			t.Errorf("Close() error = %v", err)
		}
	})
	return coordinator
}

type coordinatorRunStore struct {
	mu                  sync.Mutex
	due                 []RunSnapshot
	runNow              RunSnapshot
	runNowActive        bool
	completed           []RunSummary
	interrupted         int64
	recoveredAt         time.Time
	completeHadDeadline bool
	completeWasCanceled bool
	completeFailures    int
	completeErr         error
	completeAttempts    int
	completionSummaries []RunSummary
	completedSignal     chan struct{}
	attemptSignal       chan struct{}
}

func (store *coordinatorRunStore) ClaimDue(_ context.Context, _ time.Time, limit int) ([]RunSnapshot, error) {
	if len(store.due) > limit {
		return append([]RunSnapshot(nil), store.due[:limit]...), nil
	}
	return append([]RunSnapshot(nil), store.due...), nil
}
func (store *coordinatorRunStore) ClaimRunNow(context.Context, string, string, time.Time) (RunSnapshot, bool, error) {
	return store.runNow, store.runNowActive, nil
}
func (*coordinatorRunStore) RecordOverlap(context.Context, RunSummary) error { return nil }
func (*coordinatorRunStore) AttachSearchJob(context.Context, string, string, string, time.Time) error {
	return nil
}
func (store *coordinatorRunStore) CompleteRun(ctx context.Context, summary RunSummary) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	_, store.completeHadDeadline = ctx.Deadline()
	store.completeWasCanceled = ctx.Err() != nil
	store.completeAttempts++
	store.completionSummaries = append(store.completionSummaries, summary)
	if store.attemptSignal != nil {
		store.attemptSignal <- struct{}{}
	}
	if store.completeFailures > 0 {
		store.completeFailures--
		return errors.New("transient completion failure")
	}
	if store.completeErr != nil {
		return store.completeErr
	}
	store.completed = append(store.completed, summary)
	if store.completedSignal != nil {
		store.completedSignal <- struct{}{}
	}
	return nil
}

func (store *coordinatorRunStore) completionAttemptsSnapshot() (int, []RunSummary) {
	store.mu.Lock()
	defer store.mu.Unlock()
	return store.completeAttempts, append([]RunSummary(nil), store.completionSummaries...)
}
func (store *coordinatorRunStore) InterruptUnfinished(_ context.Context, now time.Time) (int64, error) {
	store.recoveredAt = now
	return store.interrupted, nil
}

func (store *coordinatorRunStore) completedSnapshot() []RunSummary {
	store.mu.Lock()
	defer store.mu.Unlock()
	return append([]RunSummary(nil), store.completed...)
}

func waitForCompleted(t *testing.T, store *coordinatorRunStore, count int) []RunSummary {
	t.Helper()
	deadline := time.NewTimer(time.Second)
	defer deadline.Stop()
	for {
		completed := store.completedSnapshot()
		if len(completed) >= count {
			return completed
		}
		select {
		case <-store.completedSignal:
		case <-deadline.C:
			t.Fatalf("completed runs = %d, want at least %d", len(completed), count)
		}
	}
}

type coordinatorAdmission struct {
	mu      sync.Mutex
	jobID   string
	request SearchRequest
}

func (admission *coordinatorAdmission) AdmitAlertSearch(_ context.Context, request SearchRequest) (string, error) {
	admission.mu.Lock()
	defer admission.mu.Unlock()
	admission.request = request
	return admission.jobID, nil
}

type coordinatorJobReader struct{ job SearchJobSnapshot }

func (reader coordinatorJobReader) ReadAlertSearchJob(context.Context, string, string) (SearchJobSnapshot, error) {
	return reader.job, nil
}

type coordinatorPoller struct {
	wait func(context.Context, string, string, SearchJobReader) (SearchJobSnapshot, error)
}

func (poller coordinatorPoller) WaitForTerminal(ctx context.Context, ownerID, jobID string, reader SearchJobReader) (SearchJobSnapshot, error) {
	if poller.wait != nil {
		return poller.wait(ctx, ownerID, jobID, reader)
	}
	return reader.ReadAlertSearchJob(ctx, ownerID, jobID)
}

type coordinatorResults struct {
	result SearchResults
	err    error
	calls  int
}

func (reader *coordinatorResults) ReadAlertSearchResults(context.Context, string, string, int) (SearchResults, error) {
	reader.calls++
	return reader.result, reader.err
}

type coordinatorRetention struct {
	mu        sync.Mutex
	expiresAt time.Time
	order     *orderedCalls
	calls     int
}

func (retention *coordinatorRetention) ExtendAlertSearchJob(context.Context, string, string, time.Duration) (time.Time, error) {
	retention.mu.Lock()
	defer retention.mu.Unlock()
	retention.calls++
	if retention.order != nil {
		retention.order.add("retention")
	}
	return retention.expiresAt, nil
}

type coordinatorAuthorizer struct {
	mu     sync.Mutex
	opened OpenedDeliverySecrets
	err    error
	order  *orderedCalls
	calls  int
}

func (authorizer *coordinatorAuthorizer) AuthorizeAndOpenDelivery(_ context.Context, _ RunSnapshot, generateDeliveryID func() (string, error)) (OpenedDeliverySecrets, error) {
	authorizer.mu.Lock()
	defer authorizer.mu.Unlock()
	authorizer.calls++
	if authorizer.order != nil {
		authorizer.order.add("authorization")
	}
	if authorizer.err != nil {
		return OpenedDeliverySecrets{}, authorizer.err
	}
	deliveryID, err := generateDeliveryID()
	if err != nil {
		return OpenedDeliverySecrets{}, errors.Join(ErrDeliveryIDGeneration, err)
	}
	opened := authorizer.opened
	opened.DeliveryID = deliveryID
	return opened, nil
}

type coordinatorDeliverer struct {
	mu      sync.Mutex
	order   *orderedCalls
	deliver func(context.Context, string, SignedPayload) (DeliveryResult, error)
	calls   int
}

func (deliverer *coordinatorDeliverer) Deliver(ctx context.Context, endpoint string, payload SignedPayload) (DeliveryResult, error) {
	deliverer.mu.Lock()
	deliverer.calls++
	if deliverer.order != nil {
		deliverer.order.add("delivery")
	}
	deliver := deliverer.deliver
	deliverer.mu.Unlock()
	if deliver != nil {
		return deliver(ctx, endpoint, payload)
	}
	return DeliveryResult{}, nil
}

type orderedCalls struct {
	mu    sync.Mutex
	calls []string
}

type coordinatorRetryWaiter struct {
	mu     sync.Mutex
	delays []time.Duration
}

func (waiter *coordinatorRetryWaiter) wait(ctx context.Context, delay time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	waiter.mu.Lock()
	waiter.delays = append(waiter.delays, delay)
	waiter.mu.Unlock()
	return nil
}

func (waiter *coordinatorRetryWaiter) delaysSnapshot() []time.Duration {
	waiter.mu.Lock()
	defer waiter.mu.Unlock()
	return append([]time.Duration(nil), waiter.delays...)
}

func (order *orderedCalls) add(value string) {
	order.mu.Lock()
	defer order.mu.Unlock()
	order.calls = append(order.calls, value)
}
func (order *orderedCalls) snapshot() []string {
	order.mu.Lock()
	defer order.mu.Unlock()
	return append([]string(nil), order.calls...)
}
