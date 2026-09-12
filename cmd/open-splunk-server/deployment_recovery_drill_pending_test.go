package main

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/searchartifacts"
	"github.com/Suhaibinator/open-splunk/internal/searchaudit"
	"github.com/Suhaibinator/open-splunk/internal/searchhistory"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
)

func TestDeploymentRecoveryDrillPendingAttemptUsesProductionDurableProjections(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	directory := t.TempDir()
	databasePath := filepath.Join(directory, "control.db")
	artifactDirectory := filepath.Join(directory, "search-artifacts")
	admittedAt := time.Date(2026, time.September, 12, 10, 15, 30, 123_456_000, time.UTC)

	database, err := control.Open(ctx, databasePath)
	if err != nil {
		t.Fatal(err)
	}
	if err := admitDeploymentRecoveryDrillPendingAttempt(
		ctx,
		database,
		artifactDirectory,
		admittedAt,
	); err != nil {
		_ = database.Close()
		t.Fatal(err)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}

	recoveredAt := admittedAt.Add(45 * time.Second)
	reopened, err := control.Open(ctx, databasePath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := reopened.Close(); err != nil {
			t.Error(err)
		}
	})
	artifacts, err := searchartifacts.New(ctx, searchartifacts.Config{
		DB:              reopened.SQLDB(),
		Directory:       artifactDirectory,
		Clock:           func() time.Time { return recoveredAt },
		CleanupInterval: -1,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := artifacts.Close(); err != nil {
			t.Error(err)
		}
	})
	history, err := searchhistory.New(reopened, searchhistory.Options{
		Clock:     func() time.Time { return recoveredAt },
		CursorKey: []byte("deployment-recovery-drill-cursor-key-v1"),
	})
	if err != nil {
		t.Fatal(err)
	}
	historyScope := searchhistory.AccessScope{TenantID: deploymentRecoveryDrillTenantID, OwnerID: defaultOwnerID}
	if recovered, recoverErr := history.RecoverInterrupted(ctx, historyScope); recoverErr != nil || recovered != 1 {
		t.Fatalf("RecoverInterrupted() = (%d, %v), want (1, nil)", recovered, recoverErr)
	}

	access := searchjobs.AccessScope{TenantID: deploymentRecoveryDrillTenantID, OwnerID: defaultOwnerID}
	artifact, err := artifacts.Get(ctx, access, deploymentRecoveryDrillPendingJobID, searchartifacts.AccessInspect)
	if err != nil {
		t.Fatal(err)
	}
	if artifact.State != searchartifacts.StateInterrupted ||
		artifact.Job.State != searchjobs.StateFailed ||
		artifact.Job.Failure == nil ||
		artifact.Job.Failure.Code != searchjobs.FailureInternal ||
		artifact.Job.Failure.Message != "search was interrupted by server restart" ||
		!artifact.Job.Failure.Retryable {
		t.Fatalf("recovered artifact = %#v", artifact)
	}
	if artifact.Job.ID != deploymentRecoveryDrillPendingJobID || artifact.Job.Version != 1 ||
		artifact.Job.TenantID != deploymentRecoveryDrillTenantID || artifact.Job.OwnerID != defaultOwnerID ||
		artifact.Job.AppID != "search" || artifact.Job.SPL != "index=main | head 1" ||
		artifact.Job.Source.Origin != searchjobs.JobOriginAdHoc ||
		len(artifact.Job.RequestedIndexes) != 1 || artifact.Job.RequestedIndexes[0] != "main" ||
		len(artifact.Job.EffectiveIndexes) != 1 || artifact.Job.EffectiveIndexes[0] != "main" ||
		artifact.Job.TimeRange.Earliest != "-15m" || artifact.Job.TimeRange.Latest != "now" ||
		artifact.Job.TimeRange.Timezone != "UTC" ||
		!artifact.Job.Earliest.Equal(admittedAt.Add(-15*time.Minute)) ||
		!artifact.Job.Latest.Equal(admittedAt) ||
		!artifact.Job.FinishedAt.Equal(recoveredAt) {
		t.Fatalf("recovered artifact identity or provenance = %#v", artifact.Job)
	}
	if artifact.ArtifactPresent || artifact.ArtifactBytes != 0 || artifact.Job.Schema != nil ||
		!artifact.Job.StartedAt.IsZero() || artifact.Job.ScannedRows != 0 ||
		artifact.Job.ScannedBytes != 0 || artifact.Job.RowCount != 0 || artifact.Job.ResultBytes != 0 {
		t.Fatalf("recovered artifact invented results = %#v", artifact)
	}
	if lease, acquireErr := artifacts.Acquire(ctx, access, artifact.Job.ID); !errors.Is(acquireErr, searchartifacts.ErrNotReady) {
		if lease != nil {
			_ = lease.Close()
		}
		t.Fatalf("Acquire(interrupted) error = %v, want ErrNotReady", acquireErr)
	}

	entry, err := history.Get(ctx, historyScope, artifact.Job.ID)
	if err != nil {
		t.Fatal(err)
	}
	if entry.GetFinalState() != opensplunk.SearchJobState_SEARCH_JOB_STATE_FAILED ||
		entry.GetFailure().GetCode() != opensplunk.SearchFailureCode_SEARCH_FAILURE_CODE_INTERNAL ||
		entry.GetFailure().GetMessage() != "search interrupted by server restart" ||
		!entry.GetFailure().GetRetryable() {
		t.Fatalf("recovered history entry = %+v", entry)
	}
	if entry.GetSearchJobId() != artifact.Job.ID || entry.GetDefinition().GetSpl() != artifact.Job.SPL ||
		entry.GetDefinition().GetAppId() != artifact.Job.AppID ||
		len(entry.GetDefinition().GetIndexScope()) != 1 || entry.GetDefinition().GetIndexScope()[0] != "main" ||
		entry.GetSource().GetOrigin() != opensplunk.SearchJobOrigin_SEARCH_JOB_ORIGIN_AD_HOC ||
		!entry.GetCreatedAt().AsTime().Equal(admittedAt) ||
		!entry.GetFinishedAt().AsTime().Equal(recoveredAt) {
		t.Fatalf("recovered history identity or provenance = %+v", entry)
	}
	if entry.GetStartedAt() != nil || len(entry.GetEffectiveIndexScope()) != 0 ||
		entry.GetDuration().AsDuration() != 0 || entry.GetMatchedEvents() != 0 || entry.GetScannedRows() != 0 ||
		entry.GetScannedBytes() != 0 || entry.GetProducedRows() != 0 {
		t.Fatalf("recovered history invented execution metadata = %+v", entry)
	}
	if second, secondErr := history.RecoverInterrupted(ctx, historyScope); secondErr != nil || second != 0 {
		t.Fatalf("second RecoverInterrupted() = (%d, %v), want (0, nil)", second, secondErr)
	}
	stable, err := artifacts.Get(ctx, access, artifact.Job.ID, searchartifacts.AccessInspect)
	if err != nil || stable.State != searchartifacts.StateInterrupted || stable.Job.Version != artifact.Job.Version {
		t.Fatalf("second artifact inspection = %#v, %v", stable, err)
	}
	auditEvents, err := searchaudit.New(reopened, searchaudit.Options{
		CursorKey: []byte("deployment-recovery-drill-audit-cursor-key-v1"),
	})
	if err != nil {
		t.Fatal(err)
	}
	auditPage, err := auditEvents.List(ctx, deploymentRecoveryDrillTenantID, searchaudit.ListRequest{
		OwnerID:      new(defaultOwnerID),
		IncludeTotal: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(auditPage.Events) != 1 || auditPage.TotalSize == nil || *auditPage.TotalSize != 1 ||
		auditPage.Events[0].SearchJobID != artifact.Job.ID ||
		auditPage.Events[0].OwnerID != defaultOwnerID {
		t.Fatalf("persisted search-attempt audit = %+v", auditPage)
	}
}
