package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/controlbackup"
	"github.com/Suhaibinator/open-splunk/internal/searchartifacts"
	"github.com/Suhaibinator/open-splunk/internal/searchaudit"
	"github.com/Suhaibinator/open-splunk/internal/searchhistory"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
	"github.com/Suhaibinator/open-splunk/internal/searchtime"
)

const (
	deploymentRecoveryDrillArchiveRoot    = "/var/lib/open-splunk-clickhouse-backups"
	deploymentRecoveryDrillPrivateRoot    = "/var/lib/open-splunk/state/private"
	deploymentRecoveryDrillRecoverySource = "/var/lib/open-splunk/recovery/private/rehearsal-001"
	deploymentRecoveryDrillPendingJobID   = "recovery-pending"
	deploymentRecoveryDrillTenantID       = "default"
)

// TestDeploymentRecoveryDrillChild is an opt-in subprocess entry point for the
// disposable deployment drill. It is test-only so production recovery exposes
// no crash switches or fixture mutations.
func TestDeploymentRecoveryDrillChild(t *testing.T) {
	switch mode := os.Getenv("OPEN_SPLUNK_RECOVERY_DRILL_CHILD"); mode {
	case "":
		return
	case "seed-pending":
		seedDeploymentRecoveryDrillPendingAttempt(t)
	case "crash-restore":
		crashDeploymentRecoveryDrillAfterReceipt(t)
	case "pause-after-receipt":
		t.Fatal(pauseDeploymentRecoveryDrillAfterReceipt(context.Background(), controlbackup.RestoreOptions{}))
	case "assert-target-absent":
		assertDeploymentRecoveryDrillTargetAbsent(t)
	default:
		t.Fatalf("unknown recovery drill child mode %q", mode)
	}
}

func assertDeploymentRecoveryDrillTargetAbsent(t *testing.T) {
	t.Helper()
	for _, name := range []string{
		"open-splunk.db",
		"master.key",
		"administrator.token",
		"search-artifacts",
	} {
		path := filepath.Join(deploymentRecoveryDrillPrivateRoot, name)
		if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("recovery target %q exists before control-plane publication: %v", path, err)
		}
	}
}

func seedDeploymentRecoveryDrillPendingAttempt(t *testing.T) {
	t.Helper()
	ctx := t.Context()
	database, err := control.Open(ctx, deploymentRecoveryDrillPrivateRoot+"/open-splunk.db")
	if err != nil {
		t.Fatalf("open drill control plane: %v", err)
	}
	defer func() {
		if err := database.Close(); err != nil {
			t.Errorf("close drill control plane: %v", err)
		}
	}()
	now := time.Now().UTC().Truncate(time.Microsecond)
	if err := admitDeploymentRecoveryDrillPendingAttempt(
		ctx,
		database,
		deploymentRecoveryDrillPrivateRoot+"/search-artifacts",
		now,
	); err != nil {
		t.Fatalf("seed drill pending search attempt: %v", err)
	}
	if _, err := fmt.Fprintln(os.Stdout, "RECOVERY_PENDING_SEEDED"); err != nil {
		t.Fatalf("report drill pending search attempt: %v", err)
	}
}

func admitDeploymentRecoveryDrillPendingAttempt(
	ctx context.Context,
	database *control.DB,
	artifactDirectory string,
	now time.Time,
) error {
	auditEvents, err := searchaudit.New(database, searchaudit.Options{
		CursorKey: []byte("deployment-recovery-drill-audit-cursor-key-v1"),
	})
	if err != nil {
		return fmt.Errorf("open drill search-attempt audit: %w", err)
	}
	history, err := searchhistory.New(database, searchhistory.Options{
		AuditAppender:             auditEvents,
		RequireSearchAttemptAudit: true,
		CursorKey:                 []byte("deployment-recovery-drill-cursor-key-v1"),
	})
	if err != nil {
		return fmt.Errorf("open drill search history: %w", err)
	}
	historyJournal, err := searchhistory.NewJobJournal(history)
	if err != nil {
		return fmt.Errorf("open drill search-history journal: %w", err)
	}
	artifacts, err := searchartifacts.New(ctx, searchartifacts.Config{
		DB:              database.SQLDB(),
		Directory:       artifactDirectory,
		Clock:           func() time.Time { return now },
		CleanupInterval: -1,
	})
	if err != nil {
		return fmt.Errorf("open drill search artifacts: %w", err)
	}
	timeRange := searchtime.Intent{
		Earliest: "-15m",
		Latest:   "now",
		Timezone: "UTC",
	}
	job := searchjobs.Job{
		ID:               deploymentRecoveryDrillPendingJobID,
		Version:          1,
		OwnerID:          defaultOwnerID,
		SPL:              "index=main | head 1",
		TenantID:         deploymentRecoveryDrillTenantID,
		RequestedIndexes: []string{"main"},
		EffectiveIndexes: []string{"main"},
		TimeRange:        timeRange,
		AppID:            "search",
		Source:           searchjobs.JobSource{Origin: searchjobs.JobOriginAdHoc},
		Earliest:         now.Add(-15 * time.Minute),
		Latest:           now,
		IndexTimeCutoff:  now,
		State:            searchjobs.StateQueued,
		CreatedAt:        now,
	}
	admitErr := searchjobs.NewCompositeJournal(artifacts, historyJournal).Admit(ctx, job)
	return errors.Join(admitErr, artifacts.Close())
}

func crashDeploymentRecoveryDrillAfterReceipt(t *testing.T) {
	t.Helper()
	release, err := loadControlPlaneRecoveryRelease()
	if err != nil {
		t.Fatalf("load drill recovery release: %v", err)
	}
	dependencies := defaultDeploymentRecoveryDependencies()
	dependencies.restoreControlPlane = pauseDeploymentRecoveryDrillAfterReceipt
	err = runRestoreDeploymentRecoverySetWithDependencies(
		context.Background(),
		deploymentRecoveryRestoreOptions{
			Source:                  deploymentRecoveryDrillRecoverySource,
			ArchiveRoot:             deploymentRecoveryDrillArchiveRoot,
			DatabasePath:            deploymentRecoveryDrillPrivateRoot + "/open-splunk.db",
			MasterKeyPath:           deploymentRecoveryDrillPrivateRoot + "/master.key",
			AdministratorTokenPath:  deploymentRecoveryDrillPrivateRoot + "/administrator.token",
			SearchArtifactDirectory: deploymentRecoveryDrillPrivateRoot + "/search-artifacts",
			Address:                 "clickhouse:9440",
			PasswordFile:            "/run/recovery/restore.password",
			CACertFile:              "/run/recovery/ca.crt",
			ServerName:              "clickhouse",
		},
		release,
		dependencies,
	)
	if err == nil {
		t.Fatal("crash drill restore returned before the parent terminated it")
	}
	t.Fatalf("crash drill restore ended before parent termination: %v", err)
}

func pauseDeploymentRecoveryDrillAfterReceipt(ctx context.Context, _ controlbackup.RestoreOptions) error {
	// The native session has closed at this boundary. Keep a timer live while
	// the parent inspects the receipt and sends SIGKILL: a static helper with
	// only a nil Done channel can otherwise exit through Go's deadlock detector.
	// Bound this test-only pause by the same budget as the parent drill.
	ctx, cancel := context.WithTimeout(ctx, 12*time.Minute)
	defer cancel()
	if _, err := fmt.Fprintln(os.Stdout, "RECOVERY_RECEIPT_PUBLISHED"); err != nil {
		return fmt.Errorf("report drill recovery receipt: %w", err)
	}
	<-ctx.Done()
	return ctx.Err()
}
