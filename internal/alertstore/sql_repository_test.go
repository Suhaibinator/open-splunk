package alertstore

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/alerts"
	"github.com/Suhaibinator/open-splunk/internal/control"
)

func TestAdvanceScheduleHonorsCancellation(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	scheduled := time.Date(2026, time.August, 29, 12, 0, 0, 0, time.UTC)
	if _, _, _, _, err := advanceSchedule(ctx, validDefinition(), scheduled, scheduled.AddDate(10, 0, 0)); !errors.Is(err, context.Canceled) {
		t.Fatalf("advanceSchedule() error = %v, want context.Canceled", err)
	}
}

func TestAdvanceScheduleCoalescesAtLatestOccurrenceAcrossDST(t *testing.T) {
	t.Parallel()
	definition := validDefinition()
	definition.Cron = "0 9 * * *"
	definition.Timezone = "America/Los_Angeles"
	firstDue := time.Date(2026, time.March, 7, 17, 0, 0, 0, time.UTC)
	now := time.Date(2026, time.March, 8, 16, 30, 0, 0, time.UTC)

	claimed, next, period, missed, err := advanceSchedule(
		context.Background(), definition, firstDue, now,
	)
	if err != nil {
		t.Fatalf("advanceSchedule() error = %v", err)
	}
	wantClaimed := time.Date(2026, time.March, 8, 16, 0, 0, 0, time.UTC)
	wantNext := time.Date(2026, time.March, 9, 16, 0, 0, 0, time.UTC)
	if !claimed.Equal(wantClaimed) || !next.Equal(wantNext) || period != 24*time.Hour || missed != 1 {
		t.Fatalf(
			"coalesced schedule = claimed %v next %v period %v missed %d",
			claimed, next, period, missed,
		)
	}
}

func TestSQLRepositoryLifecycleClaimAndDeliveryAuthorization(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, time.August, 29, 12, 0, 0, 0, time.UTC)
	repository, database := openSQLRepository(t, now)
	defer database.Close()
	service := newSQLService(t, repository, now)

	issued, err := service.Create(context.Background(), alerts.CreateInput{
		OwnerID: "owner-1", Definition: validDefinition(), WebhookURL: "https://hooks.example.com/notify",
	})
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	if issued.Alert.State != alerts.AlertDisabled || issued.PlaintextSecret == "" {
		t.Fatalf("issued alert = %#v", issued)
	}
	summaries, err := repository.List(context.Background(), "owner-1", 10)
	if err != nil {
		t.Fatalf("List() error = %v", err)
	}
	if len(summaries) != 1 || summaries[0].WebhookHostname != "hooks.example.com" || summaries[0].NextRunAt != nil {
		t.Fatalf("redacted summaries = %#v", summaries)
	}

	enabled, err := service.SetEnabled(context.Background(), "owner-1", issued.Alert.ID, issued.Alert.Version, true)
	if err != nil {
		t.Fatalf("SetEnabled() error = %v", err)
	}
	summaries, err = repository.List(context.Background(), "owner-1", 10)
	if err != nil || summaries[0].NextRunAt == nil {
		t.Fatalf("enabled List() = %#v, %v", summaries, err)
	}
	firstDue := *summaries[0].NextRunAt
	claimed, err := repository.ClaimDue(context.Background(), firstDue, 10)
	if err != nil {
		t.Fatalf("ClaimDue() error = %v", err)
	}
	if len(claimed) != 1 || claimed[0].AlertID != issued.Alert.ID || claimed[0].TriggeredRetention != 50*time.Minute {
		t.Fatalf("claimed = %#v", claimed)
	}
	if claimed[0].DispatchRetention != 10*time.Minute {
		t.Fatalf("dispatch retention = %s", claimed[0].DispatchRetention)
	}

	// Leaving the first run active makes the following due occurrence a
	// persisted overlap rather than a second active run.
	summaries, _ = repository.List(context.Background(), "owner-1", 10)
	secondDue := *summaries[0].NextRunAt
	secondClaim, err := repository.ClaimDue(context.Background(), secondDue, 10)
	if err != nil {
		t.Fatalf("second ClaimDue() error = %v", err)
	}
	if len(secondClaim) != 0 {
		t.Fatalf("second ClaimDue() returned active work: %#v", secondClaim)
	}
	runs, err := repository.ListRuns(context.Background(), "owner-1", issued.Alert.ID, 10)
	if err != nil || len(runs) != 2 || runs[0].Outcome != alerts.RunOverlapSkipped {
		t.Fatalf("ListRuns() = %#v, %v", runs, err)
	}

	old := claimed[0]
	old.SecretGeneration.Generation++
	authorization, err := repository.AuthorizeDelivery(context.Background(), alerts.AuthorizeDeliveryRecord{
		AlertID: old.AlertID, AlertRunID: old.AlertRunID, OwnerID: old.OwnerID,
		DeliveryID: "delivery-old", SecretGeneration: old.SecretGeneration.Generation, AuthorizedAt: now,
	})
	if err != nil || authorization != alerts.DeliverySecretRotated {
		t.Fatalf("old authorization = %q, %v", authorization, err)
	}
	authorization, err = repository.AuthorizeDelivery(context.Background(), alerts.AuthorizeDeliveryRecord{
		AlertID: claimed[0].AlertID, AlertRunID: claimed[0].AlertRunID, OwnerID: claimed[0].OwnerID,
		DeliveryID: "delivery-1", SecretGeneration: claimed[0].SecretGeneration.Generation, AuthorizedAt: now,
	})
	if err != nil || authorization != alerts.DeliveryAuthorized {
		t.Fatalf("authorization = %q, %v", authorization, err)
	}
	authorization, err = repository.AuthorizeDelivery(context.Background(), alerts.AuthorizeDeliveryRecord{
		AlertID: claimed[0].AlertID, AlertRunID: claimed[0].AlertRunID, OwnerID: claimed[0].OwnerID,
		DeliveryID: "delivery-2", SecretGeneration: claimed[0].SecretGeneration.Generation, AuthorizedAt: now,
	})
	if err != nil || authorization != alerts.DeliveryAlreadyAttempted {
		t.Fatalf("repeated authorization = %q, %v", authorization, err)
	}

	if err := service.Delete(context.Background(), "owner-1", issued.Alert.ID, enabled.Version); !errors.Is(err, alerts.ErrActiveRun) {
		t.Fatalf("Delete(active) error = %v", err)
	}
	if err := repository.AttachSearchJob(context.Background(), claimed[0].AlertID, claimed[0].AlertRunID, "job-1", firstDue.Add(50*time.Minute)); err != nil {
		t.Fatalf("AttachSearchJob() error = %v", err)
	}
	if err := repository.AttachSearchJob(context.Background(), claimed[0].AlertID, claimed[0].AlertRunID, "job-1", firstDue.Add(50*time.Minute)); err != nil {
		t.Fatalf("AttachSearchJob(idempotent retry) error = %v", err)
	}
	completed := alerts.RunSummary{
		AlertID: claimed[0].AlertID, AlertRunID: claimed[0].AlertRunID,
		Outcome: alerts.RunDelivered, FinishedAt: firstDue.Add(time.Minute), Delivery: alerts.DeliveryResult{Category: alerts.DeliverySucceeded},
	}
	if err := repository.CompleteRun(context.Background(), completed); err != nil {
		t.Fatalf("CompleteRun() error = %v", err)
	}
	if err := repository.CompleteRun(context.Background(), completed); err != nil {
		t.Fatalf("CompleteRun(idempotent retry) error = %v", err)
	}
	conflicting := completed
	conflicting.Outcome = alerts.RunDeliveryFailed
	if err := repository.CompleteRun(context.Background(), conflicting); !errors.Is(err, alerts.ErrVersionConflict) {
		t.Fatalf("CompleteRun(conflicting retry) error = %v, want alerts.ErrVersionConflict", err)
	}
	if err := service.Delete(context.Background(), "owner-1", issued.Alert.ID, enabled.Version); err != nil {
		t.Fatalf("Delete(idle) error = %v", err)
	}
}

func TestSQLRepositoryCreateRetrySurvivesServiceRestartWithoutReissuingSecret(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, time.August, 29, 12, 0, 0, 0, time.UTC)
	repository, database := openSQLRepository(t, now)
	defer database.Close()
	input := alerts.CreateInput{
		OwnerID: "owner-1", ClientRequestID: "alert-create-request-0001",
		Definition: validDefinition(), WebhookURL: "https://hooks.example.com/notify",
	}
	committed, err := newSQLService(t, repository, now).Create(context.Background(), input)
	if err != nil {
		t.Fatalf("first Create() error = %v", err)
	}
	if committed.Replayed || committed.PlaintextSecret == "" {
		t.Fatalf("first Create() = %#v", committed)
	}

	restartedRepository, err := NewSQLRepository(database, SQLRepositoryOptions{TenantID: "tenant-1"})
	if err != nil {
		t.Fatalf("NewSQLRepository(restarted) error = %v", err)
	}
	cipher, err := alerts.NewAESGCMCipher(make([]byte, 32), rand.Reader)
	if err != nil {
		t.Fatalf("alerts.NewAESGCMCipher() error = %v", err)
	}
	restartedService, err := alerts.NewService(restartedRepository, cipher, alerts.ServiceOptions{
		Clock: func() time.Time { return now },
		IDGenerator: func() (string, error) {
			return "", errors.New("ID generator must not run for a replay")
		},
		SecretGenerator: failingSecretGenerator{},
	})
	if err != nil {
		t.Fatalf("alerts.NewService(restarted) error = %v", err)
	}
	replayed, err := restartedService.Create(context.Background(), input)
	if err != nil {
		t.Fatalf("replayed Create() error = %v", err)
	}
	if !replayed.Replayed || replayed.Alert.ID != committed.Alert.ID || replayed.PlaintextSecret != "" {
		t.Fatalf("replayed Create() = %#v", replayed)
	}
	changed := input
	changed.Definition = validDefinition()
	changed.Definition.Description = "different retry"
	if _, err := restartedService.Create(context.Background(), changed); !errors.Is(err, alerts.ErrIdempotencyConflict) {
		t.Fatalf("changed Create() error = %v, want alerts.ErrIdempotencyConflict", err)
	}
	var count int
	var fingerprintBytes int
	if err := database.SQLDB().QueryRowContext(context.Background(), `
		SELECT count(*), length(create_request_sha256)
		FROM alerts
		WHERE tenant_id = ? AND owner_id = ? AND client_request_id = ?`,
		"tenant-1", "owner-1", input.ClientRequestID,
	).Scan(&count, &fingerprintBytes); err != nil {
		t.Fatalf("inspect durable alert create receipt: %v", err)
	}
	if count != 1 || fingerprintBytes != 32 {
		t.Fatalf("durable create receipt = count %d fingerprint bytes %d", count, fingerprintBytes)
	}
	summary, err := restartedRepository.GetSummary(context.Background(), "owner-1", committed.Alert.ID)
	if err != nil || summary.ID != committed.Alert.ID || summary.WebhookHostname != "hooks.example.com" {
		t.Fatalf("redacted replay summary = %#v, %v", summary, err)
	}
}

func TestSQLRepositoryClaimsEmptyRetentionDefaultsAcrossDST(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, time.October, 31, 8, 0, 0, 0, time.UTC)
	repository, database := openSQLRepository(t, now)
	defer database.Close()
	definition := validDefinition()
	definition.Cron = "0 0 ? * *"
	definition.Timezone = "America/Los_Angeles"
	definition.DispatchTTL = ""
	definition.WebhookTTL = ""
	issued, err := newSQLService(t, repository, now).Create(context.Background(), alerts.CreateInput{
		OwnerID: "owner-1", Definition: definition, WebhookURL: "https://hooks.example.com/notify",
	})
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	if _, err := newSQLService(t, repository, now).SetEnabled(context.Background(), "owner-1", issued.Alert.ID, issued.Alert.Version, true); err != nil {
		t.Fatalf("SetEnabled() error = %v", err)
	}
	summary, err := repository.GetSummary(context.Background(), "owner-1", issued.Alert.ID)
	if err != nil || summary.NextRunAt == nil {
		t.Fatalf("GetSummary() = %#v, %v", summary, err)
	}
	claimed, err := repository.ClaimDue(context.Background(), *summary.NextRunAt, 1)
	if err != nil {
		t.Fatalf("ClaimDue() error = %v", err)
	}
	if len(claimed) != 1 || claimed[0].DispatchRetention != 50*time.Hour || claimed[0].TriggeredRetention != 250*time.Hour {
		t.Fatalf("DST retention snapshot = %#v", claimed)
	}

	definition.DispatchTTL = "7200"
	definition.WebhookTTL = "2p"
	dispatch, triggered, err := resolveAlertRetention(definition, 30*time.Minute)
	if err != nil || dispatch != 2*time.Hour || triggered != 2*time.Hour {
		t.Fatalf("explicit retention precedence = dispatch %s triggered %s, %v", dispatch, triggered, err)
	}
	definition.WebhookTTL = "5p"
	_, triggered, err = resolveAlertRetention(definition, 30*time.Minute)
	if err != nil || triggered != 150*time.Minute {
		t.Fatalf("webhook retention precedence = %s, %v", triggered, err)
	}
}

func TestSQLRepositoryCoalescedClaimSnapshotsLatestDSTOccurrenceAndRetention(t *testing.T) {
	t.Parallel()
	configuredAt := time.Date(2026, time.March, 7, 16, 0, 0, 0, time.UTC)
	repository, database := openSQLRepository(t, configuredAt)
	defer database.Close()
	definition := validDefinition()
	definition.Cron = "0 9 * * *"
	definition.Timezone = "America/Los_Angeles"
	definition.DispatchTTL = ""
	definition.WebhookTTL = ""
	service := newSQLService(t, repository, configuredAt)
	issued, err := service.Create(context.Background(), alerts.CreateInput{
		OwnerID: "owner-1", Definition: definition, WebhookURL: "https://hooks.example.com/notify",
	})
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	if _, err := service.SetEnabled(
		context.Background(), "owner-1", issued.Alert.ID, issued.Alert.Version, true,
	); err != nil {
		t.Fatalf("SetEnabled() error = %v", err)
	}

	claimAt := time.Date(2026, time.March, 8, 16, 30, 0, 0, time.UTC)
	claimed, err := repository.ClaimDue(context.Background(), claimAt, 1)
	if err != nil || len(claimed) != 1 {
		t.Fatalf("ClaimDue() = %#v, %v", claimed, err)
	}
	wantScheduled := time.Date(2026, time.March, 8, 16, 0, 0, 0, time.UTC)
	wantNext := time.Date(2026, time.March, 9, 16, 0, 0, 0, time.UTC)
	snapshot := claimed[0]
	if !snapshot.ScheduledAt.Equal(wantScheduled) ||
		!snapshot.NextScheduledAt.Equal(wantNext) ||
		snapshot.MissedOccurrenceCount != 1 ||
		snapshot.DispatchRetention != 48*time.Hour ||
		snapshot.TriggeredRetention != 240*time.Hour {
		t.Fatalf("coalesced claim = %#v", snapshot)
	}
	runs, err := repository.ListRuns(context.Background(), "owner-1", issued.Alert.ID, 1)
	if err != nil || len(runs) != 1 || !runs[0].ScheduledAt.Equal(wantScheduled) {
		t.Fatalf("coalesced run history = %#v, %v", runs, err)
	}
}

func TestSQLRepositoryClaimDueRollsBackStaleDefinitionSnapshot(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, time.August, 29, 12, 0, 0, 0, time.UTC)
	repository, database := openSQLRepository(t, now)
	defer database.Close()
	issued, err := newSQLService(t, repository, now).Create(context.Background(), alerts.CreateInput{
		OwnerID: "owner-1", Definition: validDefinition(), WebhookURL: "https://hooks.example.com/notify",
	})
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	if _, err := newSQLService(t, repository, now).SetEnabled(context.Background(), "owner-1", issued.Alert.ID, issued.Alert.Version, true); err != nil {
		t.Fatalf("SetEnabled() error = %v", err)
	}
	summary, err := repository.GetSummary(context.Background(), "owner-1", issued.Alert.ID)
	if err != nil || summary.NextRunAt == nil {
		t.Fatalf("GetSummary() = %#v, %v", summary, err)
	}
	if _, err := database.SQLDB().ExecContext(context.Background(), `
		CREATE TRIGGER alert_test_stale_claim
		BEFORE INSERT ON alert_runs
		BEGIN
			UPDATE alerts SET version = version + 1 WHERE alert_id = NEW.alert_id;
		END`); err != nil {
		t.Fatalf("create stale-claim trigger: %v", err)
	}
	if _, err := repository.ClaimDue(context.Background(), *summary.NextRunAt, 1); !errors.Is(err, alerts.ErrVersionConflict) {
		t.Fatalf("ClaimDue() error = %v, want version conflict", err)
	}
	var runs int
	if err := database.SQLDB().QueryRowContext(context.Background(), "SELECT count(*) FROM alert_runs WHERE alert_id = ?", issued.Alert.ID).Scan(&runs); err != nil {
		t.Fatalf("count rolled-back runs: %v", err)
	}
	if runs != 0 {
		t.Fatalf("stale claim persisted %d runs", runs)
	}
}

type failingSecretGenerator struct{}

func (failingSecretGenerator) Generate() ([]byte, error) {
	return nil, errors.New("secret generator must not run for a replay")
}

func TestSQLRepositoryInterruptsUnfinishedRuns(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, time.August, 29, 12, 0, 0, 0, time.UTC)
	repository, database := openSQLRepository(t, now)
	defer database.Close()
	service := newSQLService(t, repository, now)
	issued, err := service.Create(context.Background(), alerts.CreateInput{
		OwnerID: "owner-1", Definition: validDefinition(), WebhookURL: "https://hooks.example.com/notify",
	})
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	if _, err := service.SetEnabled(context.Background(), "owner-1", issued.Alert.ID, issued.Alert.Version, true); err != nil {
		t.Fatalf("SetEnabled() error = %v", err)
	}
	summaries, _ := repository.List(context.Background(), "owner-1", 1)
	if _, err := repository.ClaimDue(context.Background(), *summaries[0].NextRunAt, 1); err != nil {
		t.Fatalf("ClaimDue() error = %v", err)
	}
	interrupted, err := repository.InterruptUnfinished(context.Background(), summaries[0].NextRunAt.Add(time.Minute))
	if err != nil || interrupted != 1 {
		t.Fatalf("InterruptUnfinished() = %d, %v", interrupted, err)
	}
	runs, err := repository.ListRuns(context.Background(), "owner-1", issued.Alert.ID, 10)
	if err != nil || len(runs) != 1 || runs[0].Outcome != alerts.RunInterrupted {
		t.Fatalf("interrupted runs = %#v, %v", runs, err)
	}
}

func TestSQLRepositorySummaryTracksOutcomeEvaluationAndDeliveryIndependently(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, time.August, 29, 12, 0, 0, 0, time.UTC)
	repository, database := openSQLRepository(t, now)
	defer database.Close()
	service := newSQLService(t, repository, now)
	issued, err := service.Create(context.Background(), alerts.CreateInput{
		OwnerID: "owner-1", Definition: validDefinition(), WebhookURL: "https://hooks.example.com/notify",
	})
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	if _, err := service.SetEnabled(context.Background(), "owner-1", issued.Alert.ID, issued.Alert.Version, true); err != nil {
		t.Fatalf("SetEnabled() error = %v", err)
	}
	summary, _ := repository.GetSummary(context.Background(), "owner-1", issued.Alert.ID)
	firstDue := *summary.NextRunAt
	first, err := repository.ClaimDue(context.Background(), firstDue, 1)
	if err != nil || len(first) != 1 {
		t.Fatalf("first ClaimDue() = %#v, %v", first, err)
	}
	summary, _ = repository.GetSummary(context.Background(), "owner-1", issued.Alert.ID)
	secondDue := *summary.NextRunAt
	if _, err := repository.ClaimDue(context.Background(), secondDue, 1); err != nil {
		t.Fatalf("overlap ClaimDue() error = %v", err)
	}
	deliveredAt := secondDue.Add(time.Minute)
	if err := repository.CompleteRun(context.Background(), alerts.RunSummary{
		AlertID: first[0].AlertID, AlertRunID: first[0].AlertRunID, Outcome: alerts.RunDelivered,
		Evaluation: alerts.EvaluationTrue, FinishedAt: deliveredAt,
		Delivery: alerts.DeliveryResult{Category: alerts.DeliverySucceeded, Delivered: true, AttemptedAt: deliveredAt},
	}); err != nil {
		t.Fatalf("CompleteRun(delivered) error = %v", err)
	}
	summary, err = repository.GetSummary(context.Background(), "owner-1", issued.Alert.ID)
	if err != nil || summary.LastOutcome != alerts.RunOverlapSkipped || summary.LastEvaluatedAt == nil || !summary.LastEvaluatedAt.Equal(deliveredAt) || summary.LastDeliveredAt == nil || !summary.LastDeliveredAt.Equal(deliveredAt) {
		t.Fatalf("summary after older delivery = %#v, %v", summary, err)
	}

	thirdDue := *summary.NextRunAt
	third, err := repository.ClaimDue(context.Background(), thirdDue, 1)
	if err != nil || len(third) != 1 {
		t.Fatalf("third ClaimDue() = %#v, %v", third, err)
	}
	summary, _ = repository.GetSummary(context.Background(), "owner-1", issued.Alert.ID)
	if summary.LastOutcome != alerts.RunSearching || summary.LastEvaluatedAt == nil || !summary.LastEvaluatedAt.Equal(deliveredAt) || summary.LastDeliveredAt == nil || !summary.LastDeliveredAt.Equal(deliveredAt) {
		t.Fatalf("running summary erased historical timestamps: %#v", summary)
	}
	evaluatedAt := thirdDue.Add(time.Minute)
	if err := repository.CompleteRun(context.Background(), alerts.RunSummary{
		AlertID: third[0].AlertID, AlertRunID: third[0].AlertRunID, Outcome: alerts.RunNotTriggered,
		Evaluation: alerts.EvaluationFalse, FinishedAt: evaluatedAt,
	}); err != nil {
		t.Fatalf("CompleteRun(not triggered) error = %v", err)
	}
	summary, err = repository.GetSummary(context.Background(), "owner-1", issued.Alert.ID)
	if err != nil || summary.LastOutcome != alerts.RunNotTriggered || summary.LastEvaluatedAt == nil || !summary.LastEvaluatedAt.Equal(evaluatedAt) || summary.LastDeliveredAt == nil || !summary.LastDeliveredAt.Equal(deliveredAt) {
		t.Fatalf("independent terminal summary = %#v, %v", summary, err)
	}
}

func TestSQLRepositoryClaimRunNowDoesNotAdvanceScheduleAndRecordsOverlap(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, time.August, 29, 12, 0, 0, 0, time.UTC)
	repository, database := openSQLRepository(t, now)
	defer database.Close()
	service := newSQLService(t, repository, now)
	issued, err := service.Create(context.Background(), alerts.CreateInput{
		OwnerID: "owner-1", Definition: validDefinition(), WebhookURL: "https://hooks.example.com/notify",
	})
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	if _, err := service.SetEnabled(context.Background(), "owner-1", issued.Alert.ID, issued.Alert.Version, true); err != nil {
		t.Fatalf("SetEnabled() error = %v", err)
	}
	before, err := repository.GetSummary(context.Background(), "owner-1", issued.Alert.ID)
	if err != nil || before.NextRunAt == nil {
		t.Fatalf("GetSummary() = %#v, %v", before, err)
	}
	first, active, err := repository.ClaimRunNow(context.Background(), "owner-1", issued.Alert.ID, now)
	if err != nil || !active || first.TenantID != "tenant-1" || first.DispatchRetention != 10*time.Minute || first.TriggeredRetention != 50*time.Minute {
		t.Fatalf("first ClaimRunNow() = %#v, %t, %v", first, active, err)
	}
	after, err := repository.GetSummary(context.Background(), "owner-1", issued.Alert.ID)
	if err != nil || after.NextRunAt == nil || !after.NextRunAt.Equal(*before.NextRunAt) {
		t.Fatalf("run now moved schedule: before=%v after=%v error=%v", before.NextRunAt, after.NextRunAt, err)
	}
	second, active, err := repository.ClaimRunNow(context.Background(), "owner-1", issued.Alert.ID, now.Add(time.Microsecond))
	if err != nil || active || second.AlertRunID == first.AlertRunID {
		t.Fatalf("overlap ClaimRunNow() = %#v, %t, %v", second, active, err)
	}
	runs, err := repository.ListRuns(context.Background(), "owner-1", issued.Alert.ID, 10)
	if err != nil || len(runs) != 2 || runs[0].Outcome != alerts.RunOverlapSkipped {
		t.Fatalf("ListRuns() = %#v, %v", runs, err)
	}
}

func TestSQLRepositoryScheduledOccurrenceCanFollowRunNowAtSameTimestamp(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, time.August, 29, 12, 0, 0, 0, time.UTC)
	repository, database := openSQLRepository(t, now)
	defer database.Close()
	service := newSQLService(t, repository, now)
	issued, err := service.Create(context.Background(), alerts.CreateInput{
		OwnerID: "owner-1", Definition: validDefinition(), WebhookURL: "https://hooks.example.com/notify",
	})
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	if _, err := service.SetEnabled(context.Background(), "owner-1", issued.Alert.ID, issued.Alert.Version, true); err != nil {
		t.Fatalf("SetEnabled() error = %v", err)
	}
	summary, err := repository.GetSummary(context.Background(), "owner-1", issued.Alert.ID)
	if err != nil || summary.NextRunAt == nil {
		t.Fatalf("GetSummary() = %#v, %v", summary, err)
	}
	manual, active, err := repository.ClaimRunNow(context.Background(), "owner-1", issued.Alert.ID, *summary.NextRunAt)
	if err != nil || !active {
		t.Fatalf("ClaimRunNow() = %#v, %t, %v", manual, active, err)
	}
	if err := repository.CompleteRun(context.Background(), alerts.RunSummary{
		AlertID: manual.AlertID, AlertRunID: manual.AlertRunID, Outcome: alerts.RunNotTriggered,
		Evaluation: alerts.EvaluationFalse, FinishedAt: *summary.NextRunAt,
	}); err != nil {
		t.Fatalf("CompleteRun(run now) error = %v", err)
	}
	claimed, err := repository.ClaimDue(context.Background(), *summary.NextRunAt, 1)
	if err != nil || len(claimed) != 1 {
		t.Fatalf("ClaimDue(exact timestamp) = %#v, %v", claimed, err)
	}
	updated, err := repository.GetSummary(context.Background(), "owner-1", issued.Alert.ID)
	if err != nil || updated.NextRunAt == nil || !updated.NextRunAt.After(*summary.NextRunAt) {
		t.Fatalf("advanced alert = %#v, %v", updated, err)
	}
}

func TestSQLRepositoryAuthorizesOneConcurrentDelivery(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, time.August, 29, 12, 0, 0, 0, time.UTC)
	repository, database := openSQLRepository(t, now)
	defer database.Close()
	service := newSQLService(t, repository, now)
	issued, err := service.Create(context.Background(), alerts.CreateInput{
		OwnerID: "owner-1", Definition: validDefinition(), WebhookURL: "https://hooks.example.com/notify",
	})
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	if _, err := service.SetEnabled(context.Background(), "owner-1", issued.Alert.ID, issued.Alert.Version, true); err != nil {
		t.Fatalf("SetEnabled() error = %v", err)
	}
	summaries, _ := repository.List(context.Background(), "owner-1", 1)
	claimed, err := repository.ClaimDue(context.Background(), *summaries[0].NextRunAt, 1)
	if err != nil {
		t.Fatalf("ClaimDue() error = %v", err)
	}

	const attempts = 8
	results := make(chan alerts.DeliveryAuthorization, attempts)
	errorsSeen := make(chan error, attempts)
	var waitGroup sync.WaitGroup
	for index := range attempts {
		waitGroup.Add(1)
		go func(index int) {
			defer waitGroup.Done()
			authorization, authorizeErr := repository.AuthorizeDelivery(context.Background(), alerts.AuthorizeDeliveryRecord{
				AlertID: claimed[0].AlertID, AlertRunID: claimed[0].AlertRunID, OwnerID: claimed[0].OwnerID,
				DeliveryID: fmt.Sprintf("delivery-%d", index), SecretGeneration: claimed[0].SecretGeneration.Generation,
				AuthorizedAt: now,
			})
			results <- authorization
			errorsSeen <- authorizeErr
		}(index)
	}
	waitGroup.Wait()
	close(results)
	close(errorsSeen)
	for authorizeErr := range errorsSeen {
		if authorizeErr != nil {
			t.Fatalf("AuthorizeDelivery() error = %v", authorizeErr)
		}
	}
	authorized := 0
	alreadyAttempted := 0
	for result := range results {
		switch result {
		case alerts.DeliveryAuthorized:
			authorized++
		case alerts.DeliveryAlreadyAttempted:
			alreadyAttempted++
		default:
			t.Fatalf("unexpected authorization %q", result)
		}
	}
	if authorized != 1 || alreadyAttempted != attempts-1 {
		t.Fatalf("authorization counts = %d authorized, %d repeated", authorized, alreadyAttempted)
	}
}

func TestSQLRepositoryEnforcesCapacityAndRunHistoryBounds(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, time.August, 29, 12, 0, 0, 0, time.UTC)
	repository, database := openSQLRepository(t, now)
	defer database.Close()
	ctx := context.Background()
	for index := range alerts.MaximumAlertsPerOwner {
		record := validCreateRecord(fmt.Sprintf("alert-%d", index), fmt.Sprintf("alert %d", index), now)
		if _, err := repository.Create(ctx, record); err != nil {
			t.Fatalf("Create(%d) error = %v", index, err)
		}
	}
	if _, err := repository.Create(ctx, validCreateRecord("overflow", "overflow", now)); !errors.Is(err, alerts.ErrCapacity) {
		t.Fatalf("overflow Create() error = %v", err)
	}
	for index := range alerts.MaximumRunHistory + 5 {
		scheduled := now.Add(time.Duration(index) * time.Minute)
		if err := repository.RecordOverlap(ctx, alerts.RunSummary{
			AlertID: "alert-0", AlertRunID: fmt.Sprintf("overlap-%03d", index),
			Outcome: alerts.RunOverlapSkipped, ScheduledAt: scheduled, StartedAt: scheduled, FinishedAt: scheduled,
		}); err != nil {
			t.Fatalf("RecordOverlap(%d) error = %v", index, err)
		}
	}
	runs, err := repository.ListRuns(ctx, "owner-1", "alert-0", alerts.MaximumRunHistory)
	if err != nil || len(runs) != alerts.MaximumRunHistory {
		t.Fatalf("bounded ListRuns() length = %d, error = %v", len(runs), err)
	}
	if runs[0].AlertRunID != "overlap-104" || runs[len(runs)-1].AlertRunID != "overlap-005" {
		t.Fatalf("retained run range = %q .. %q", runs[0].AlertRunID, runs[len(runs)-1].AlertRunID)
	}
}

func TestSQLRepositoryAlertNamesAreUniqueWithinTenant(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, time.August, 29, 12, 0, 0, 0, time.UTC)
	first, database := openSQLRepository(t, now)
	defer database.Close()
	second, err := NewSQLRepository(database, SQLRepositoryOptions{
		TenantID: "tenant-2", Clock: func() time.Time { return now },
		IDGenerator: func() (string, error) { return "run-tenant-2", nil },
	})
	if err != nil {
		t.Fatalf("NewSQLRepository(tenant-2) error = %v", err)
	}
	if _, err := first.Create(context.Background(), validCreateRecord("alert-tenant-1", "same name", now)); err != nil {
		t.Fatalf("Create(tenant-1) error = %v", err)
	}
	if _, err := second.Create(context.Background(), validCreateRecord("alert-tenant-2", "same name", now)); err != nil {
		t.Fatalf("Create(tenant-2) error = %v", err)
	}
	if _, err := second.Create(context.Background(), validCreateRecord("alert-tenant-2-duplicate", "same name", now)); !errors.Is(err, alerts.ErrAlreadyExists) {
		t.Fatalf("Create(tenant-2 duplicate) error = %v", err)
	}
}

func openSQLRepository(t *testing.T, now time.Time) (*SQLRepository, *control.DB) {
	t.Helper()
	database, err := control.Open(context.Background(), filepath.Join(t.TempDir(), "control.sqlite"))
	if err != nil {
		t.Fatalf("control.Open() error = %v", err)
	}
	var sequence atomic.Uint64
	repository, err := NewSQLRepository(database, SQLRepositoryOptions{
		TenantID: "tenant-1", Clock: func() time.Time { return now },
		IDGenerator: func() (string, error) { return fmt.Sprintf("run-%d", sequence.Add(1)), nil },
	})
	if err != nil {
		if closeErr := database.Close(); closeErr != nil {
			t.Errorf("close control database after repository setup failure: %v", closeErr)
		}
		t.Fatalf("NewSQLRepository() error = %v", err)
	}
	return repository, database
}

func newSQLService(t *testing.T, repository alerts.Repository, now time.Time) *alerts.Service {
	t.Helper()
	cipher, err := alerts.NewAESGCMCipher(make([]byte, 32), rand.Reader)
	if err != nil {
		t.Fatalf("alerts.NewAESGCMCipher() error = %v", err)
	}
	service, err := alerts.NewService(repository, cipher, alerts.ServiceOptions{
		Clock: func() time.Time { return now }, PublicBaseURL: "https://splunk.example.com",
		IDGenerator: func() (string, error) { return "lifecycle-alert", nil },
	})
	if err != nil {
		t.Fatalf("alerts.NewService() error = %v", err)
	}
	return service
}

func validCreateRecord(id, name string, now time.Time) alerts.CreateRecord {
	definition := validDefinition()
	definition.Name = name
	return alerts.CreateRecord{
		ID: id, OwnerID: "owner-1", State: alerts.AlertDisabled, Definition: definition,
		Endpoint: alerts.EncryptedValue{Nonce: make([]byte, 12), Ciphertext: make([]byte, 17)}, EndpointGeneration: 1,
		WebhookHostname:  "hooks.example.com",
		SecretGeneration: alerts.SecretGeneration{Generation: 1, Encrypted: alerts.EncryptedValue{Nonce: make([]byte, 12), Ciphertext: make([]byte, 48)}, CreatedAt: now},
		CreatedAt:        now,
	}
}

func validDefinition() alerts.Definition {
	return alerts.Definition{
		Name: "errors", Application: "search", SPL: "search index=main error", IndexScope: []string{"main"},
		Earliest: "-5m", Latest: "now", Cron: "*/5 * * * *", Timezone: "UTC",
		Condition:  alerts.Condition{Operator: alerts.ConditionGreaterThan, Threshold: 0},
		SampleRows: 5, DispatchTTL: "2p", WebhookTTL: "10p",
	}
}
