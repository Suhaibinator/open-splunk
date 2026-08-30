package scheduledreports

import (
	"context"
	"testing"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/featureops"
)

func TestServiceObservesClaimsOutcomesAndRecoveryWithoutIdentityLabels(t *testing.T) {
	now := time.Date(2026, time.August, 29, 12, 0, 1, 0, time.UTC)
	repository, database := newTestRepository(t, now)
	seedSavedSearch(t, database, "saved-observed", "owner-private", 1, "search index=secret")
	metrics := featureops.NewMetrics()
	service, err := NewService(ServiceOptions{
		Store: repository, Admitter: new(recordingAdmitter), Clock: func() time.Time { return now },
		Observer: metrics,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.Configure(
		context.Background(),
		"owner-private",
		"tenant-private",
		"saved-observed",
		0,
		Configuration{Cron: "*/5 * * * *", Timezone: "UTC", Enabled: true},
	); err != nil {
		t.Fatal(err)
	}
	if err := service.Step(
		context.Background(),
		time.Date(2026, time.August, 29, 12, 5, 0, 0, time.UTC),
	); err != nil {
		t.Fatal(err)
	}
	if err := service.Step(
		context.Background(),
		time.Date(2026, time.August, 29, 12, 10, 0, 0, time.UTC),
	); err != nil {
		t.Fatal(err)
	}
	count, err := service.Recover(context.Background())
	if err != nil || count != 1 {
		t.Fatalf("Recover = %d, %v", count, err)
	}

	snapshot := metrics.Snapshot()
	assertScheduledCounter(t, snapshot, featureops.OperationScheduleClaim, featureops.OutcomeSucceeded, 2, 2)
	assertScheduledCounter(t, snapshot, featureops.OperationRunOutcome, featureops.OutcomeSubmitted, 1, 1)
	assertScheduledCounter(t, snapshot, featureops.OperationRunOutcome, featureops.OutcomeSkipped, 1, 1)
	assertScheduledCounter(t, snapshot, featureops.OperationRecovery, featureops.OutcomeSucceeded, 1, 1)
}

func assertScheduledCounter(
	t *testing.T,
	snapshot featureops.Snapshot,
	operation featureops.Operation,
	outcome featureops.Outcome,
	events uint64,
	items uint64,
) {
	t.Helper()
	got := snapshot.Counter(featureops.FeatureScheduledReports, operation, outcome)
	if got.Events != events || got.Items != items || got.Bytes != 0 {
		t.Fatalf("scheduled counter %v/%v = %#v", operation, outcome, got)
	}
}
