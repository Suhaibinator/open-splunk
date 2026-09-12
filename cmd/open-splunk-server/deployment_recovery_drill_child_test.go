package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/controlbackup"
	"github.com/Suhaibinator/open-splunk/internal/searchhistory"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	deploymentRecoveryDrillArchiveRoot    = "/var/lib/open-splunk-clickhouse-backups"
	deploymentRecoveryDrillPrivateRoot    = "/var/lib/open-splunk/state/private"
	deploymentRecoveryDrillRecoverySource = "/var/lib/open-splunk/recovery/private/rehearsal-001"
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
	store, err := searchhistory.New(database, searchhistory.Options{
		CursorKey: []byte("deployment-recovery-drill-cursor-key-v1"),
	})
	if err != nil {
		t.Fatalf("open drill search history: %v", err)
	}
	now := time.Now().UTC().Round(0)
	appID := "search"
	earliest := "-15m"
	latest := "now"
	if _, err := store.BeginAttempt(
		ctx,
		searchhistory.AccessScope{TenantID: "default", OwnerID: defaultOwnerID},
		&opensplunk.SearchHistoryEntry{
			SearchJobId: "recovery-pending",
			Definition: &opensplunk.SearchDefinition{
				Spl:        "index=main | head 1",
				AppId:      &appID,
				IndexScope: []string{"main"},
				TimeRange: &opensplunk.TimeRangeSpec{
					Earliest: &earliest,
					Latest:   &latest,
				},
			},
			Source: &opensplunk.SearchJobSource{
				Origin: opensplunk.SearchJobOrigin_SEARCH_JOB_ORIGIN_AD_HOC,
			},
			EffectiveIndexScope: []string{"main"},
			ResolvedTimeRange: &opensplunk.ResolvedTimeRange{
				Earliest: timestamppb.New(now.Add(-15 * time.Minute)),
				Latest:   timestamppb.New(now),
				Timezone: "UTC",
			},
			FinalState: opensplunk.SearchJobState_SEARCH_JOB_STATE_QUEUED,
			CreatedAt:  timestamppb.New(now),
		},
	); err != nil {
		t.Fatalf("seed drill pending search attempt: %v", err)
	}
	if _, err := fmt.Fprintln(os.Stdout, "RECOVERY_PENDING_SEEDED"); err != nil {
		t.Fatalf("report drill pending search attempt: %v", err)
	}
}

func crashDeploymentRecoveryDrillAfterReceipt(t *testing.T) {
	t.Helper()
	release, err := loadControlPlaneRecoveryRelease()
	if err != nil {
		t.Fatalf("load drill recovery release: %v", err)
	}
	dependencies := defaultDeploymentRecoveryDependencies()
	dependencies.restoreControlPlane = func(
		ctx context.Context,
		_ controlbackup.RestoreOptions,
	) error {
		if _, err := fmt.Fprintln(os.Stdout, "RECOVERY_RECEIPT_PUBLISHED"); err != nil {
			return fmt.Errorf("report drill recovery receipt: %w", err)
		}
		<-ctx.Done()
		return ctx.Err()
	}
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
