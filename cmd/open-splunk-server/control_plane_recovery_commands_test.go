package main

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/controlbackup"
	"github.com/Suhaibinator/open-splunk/internal/pgtest"
)

func TestRunDeploymentSubcommandRecognizesRecoveryCommandsBeforeRuntime(t *testing.T) {
	for _, command := range []string{"backup-control-plane", "verify-control-plane-backup", "restore-control-plane"} {
		handled, err := runDeploymentSubcommand([]string{command, "-unknown"})
		if !handled || err == nil {
			t.Errorf("%s dispatch=(%t,%v)", command, handled, err)
		}
	}
}
func recoveryObjectArgumentsForTest() []string {
	return []string{"-minio-endpoint", "127.0.0.1:59000", "-minio-access-key", "test-access", "-minio-secret-key", "test-secret", "-minio-bucket", "test-results", "-minio-use-tls=false"}
}
func TestRecoveryCommandFlagParsersAcceptOnlyCompleteExactAbsolutePaths(t *testing.T) {
	root := filepath.Join(string(filepath.Separator), "private", "recovery")
	dsn := "postgres://postgres@localhost/control?sslmode=disable"
	backupArguments := append([]string{"-postgres-dsn", dsn, "-master-key", filepath.Join(root, "master.key"), "-destination", filepath.Join(root, "bundle")}, recoveryObjectArgumentsForTest()...)
	backup, err := parseBackupControlPlaneOptions(backupArguments)
	if err != nil {
		t.Fatal(err)
	}
	if backup.PostgresDSN != dsn || backup.MasterKeyPath != filepath.Join(root, "master.key") || backup.Destination != filepath.Join(root, "bundle") {
		t.Fatal("backup scope changed")
	}
	restoreArguments := append([]string{"-source", filepath.Join(root, "bundle"), "-postgres-dsn", dsn, "-master-key", filepath.Join(root, "restored.key")}, recoveryObjectArgumentsForTest()...)
	restore, err := parseRestoreControlPlaneOptions(restoreArguments)
	if err != nil {
		t.Fatal(err)
	}
	if restore.PostgresDSN != dsn || restore.Source != filepath.Join(root, "bundle") || restore.MasterKeyPath != filepath.Join(root, "restored.key") {
		t.Fatal("restore scope changed")
	}
	source, err := parseVerifyControlPlaneBackupOptions([]string{"-source", filepath.Join(root, "bundle")})
	if err != nil || source != filepath.Join(root, "bundle") {
		t.Fatalf("source=%q err=%v", source, err)
	}
	for _, path := range []string{"", "relative", root + "/../recovery/file", root + "/file/", " " + root + "/file", root + "/file ", root + "/bad\x00file", root + "/bad name", root + "/bäckup", "/"} {
		if err := validateRecoveryCommandPath("-test", path); err == nil {
			t.Errorf("unsafe path accepted %q", path)
		}
	}
	for name, parse := range map[string]func() error{
		"backup missing":     func() error { _, err := parseBackupControlPlaneOptions(nil); return err },
		"backup positional":  func() error { _, err := parseBackupControlPlaneOptions(append(backupArguments, "extra")); return err },
		"restore missing":    func() error { _, err := parseRestoreControlPlaneOptions(nil); return err },
		"restore positional": func() error { _, err := parseRestoreControlPlaneOptions(append(restoreArguments, "extra")); return err },
		"verify missing":     func() error { _, err := parseVerifyControlPlaneBackupOptions(nil); return err },
		"verify positional": func() error {
			_, err := parseVerifyControlPlaneBackupOptions([]string{"-source", filepath.Join(root, "bundle"), "extra"})
			return err
		},
		"retired control file": func() error {
			_, err := parseBackupControlPlaneOptions([]string{"-control-db", root + "/control.db"})
			return err
		},
		"retired admin token": func() error {
			_, err := parseBackupControlPlaneOptions([]string{"-administrator-token-file", root + "/token"})
			return err
		},
	} {
		if err := parse(); err == nil {
			t.Errorf("%s accepted", name)
		}
	}
	for _, dsn := range []string{"", "control.db", "secret-must-not-leak"} {
		if err := validateRecoveryDSN(dsn); err == nil || strings.Contains(err.Error(), "secret-must-not-leak") {
			t.Errorf("invalid DSN result %v", err)
		}
	}
}
func TestRecoveryCommandsRequireStoppedServerBeforeAccessingState(t *testing.T) {
	dsn := pgtest.DSN(t)
	database, err := control.Open(t.Context(), dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	destination := filepath.Join(t.TempDir(), "not-created")
	release := testRecoveryReleaseIdentity()
	err = runBackupControlPlane(t.Context(), controlbackup.CreateOptions{PostgresDSN: dsn, Destination: destination}, release, control.AcquireMaintenanceLease)
	if !errors.Is(err, control.ErrServerAlreadyRunning) {
		t.Fatalf("active server backup=%v", err)
	}
	err = runRestoreControlPlane(t.Context(), controlbackup.RestoreOptions{PostgresDSN: dsn, Source: destination}, release, control.AcquireMaintenanceLease)
	if !errors.Is(err, control.ErrServerAlreadyRunning) {
		t.Fatalf("active server restore=%v", err)
	}
	if _, err := os.Lstat(destination); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("durable state accessed before ownership acquired: %v", err)
	}
}
func TestRecoveryCommandRunnersRejectMissingLockDependencies(t *testing.T) {
	release := testRecoveryReleaseIdentity()
	nilLease := func(context.Context, string) (*control.MaintenanceLease, error) { return nil, nil }
	for _, ctx := range []context.Context{nil, t.Context()} {
		for _, acquire := range []maintenanceLeaseAcquirer{nil, nilLease} {
			if err := runBackupControlPlane(ctx, controlbackup.CreateOptions{}, release, acquire); err == nil {
				t.Fatal("backup accepted missing context or lease")
			}
			if err := runRestoreControlPlane(ctx, controlbackup.RestoreOptions{}, release, acquire); err == nil {
				t.Fatal("restore accepted missing context or lease")
			}
		}
	}
}
func TestRecoveryCommandContextStopIsIdempotentAndCancels(t *testing.T) {
	ctx, stop := recoveryCommandContext()
	stop()
	stop()
	if !errors.Is(ctx.Err(), context.Canceled) {
		t.Fatalf("signal context=%v", ctx.Err())
	}
}
func testRecoveryReleaseIdentity() controlbackup.ReleaseIdentity {
	return controlbackup.ReleaseIdentity{SourceRevision: "development", PostgresMigrations: controlbackup.MigrationIdentity{SHA256: strings.Repeat("1", 64), LatestVersion: 1}, ClickHouseMigrations: controlbackup.MigrationIdentity{SHA256: strings.Repeat("2", 64), LatestVersion: 1}}
}
