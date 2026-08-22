package main

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/controlbackup"
)

func TestRunDeploymentSubcommandRecognizesRecoveryCommandsBeforeRuntime(t *testing.T) {
	t.Parallel()

	for _, command := range []string{
		"backup-control-plane",
		"verify-control-plane-backup",
		"restore-control-plane",
	} {
		handled, err := runDeploymentSubcommand([]string{command, "-unknown"})
		if !handled || err == nil {
			t.Errorf("%s dispatch = (%t, %v), want (true, error)", command, handled, err)
		}
	}
}

func TestRecoveryCommandFlagParsersAcceptOnlyCompleteExactAbsolutePaths(t *testing.T) {
	t.Parallel()

	root := filepath.Join(string(filepath.Separator), "private", "recovery")
	backupArguments := []string{
		"-control-db", filepath.Join(root, "open-splunk.db"),
		"-master-key", filepath.Join(root, "master.key"),
		"-administrator-token-file", filepath.Join(root, "administrator.token"),
		"-destination", filepath.Join(root, "bundle"),
	}
	backup, err := parseBackupControlPlaneOptions(backupArguments)
	if err != nil {
		t.Fatal(err)
	}
	if backup.DatabasePath != filepath.Join(root, "open-splunk.db") ||
		backup.MasterKeyPath != filepath.Join(root, "master.key") ||
		backup.AdministratorTokenPath != filepath.Join(root, "administrator.token") ||
		backup.Destination != filepath.Join(root, "bundle") {
		t.Fatalf("parsed backup options = %#v", backup)
	}

	source, err := parseVerifyControlPlaneBackupOptions([]string{
		"-source", filepath.Join(root, "bundle"),
	})
	if err != nil || source != filepath.Join(root, "bundle") {
		t.Fatalf("parsed verify source = (%q, %v)", source, err)
	}

	restore, err := parseRestoreControlPlaneOptions([]string{
		"-source", filepath.Join(root, "bundle"),
		"-control-db", filepath.Join(root, "restored.db"),
		"-master-key", filepath.Join(root, "restored.key"),
		"-administrator-token-file", filepath.Join(root, "restored.token"),
	})
	if err != nil {
		t.Fatal(err)
	}
	if restore.Source != filepath.Join(root, "bundle") ||
		restore.DatabasePath != filepath.Join(root, "restored.db") ||
		restore.MasterKeyPath != filepath.Join(root, "restored.key") ||
		restore.AdministratorTokenPath != filepath.Join(root, "restored.token") {
		t.Fatalf("parsed restore options = %#v", restore)
	}

	invalidPaths := []string{
		"",
		"relative",
		root + string(filepath.Separator) + ".." + string(filepath.Separator) + "recovery" + string(filepath.Separator) + "file",
		filepath.Join(root, "file") + string(filepath.Separator),
		" " + filepath.Join(root, "file"),
		filepath.Join(root, "file") + " ",
		filepath.Join(root, "bad\x00file"),
		filepath.Join(root, "bad name"),
		filepath.Join(root, "bäckup"),
		string(filepath.Separator),
	}
	for _, path := range invalidPaths {
		if err := validateRecoveryCommandPath("-test", path); err == nil {
			t.Errorf("validateRecoveryCommandPath(%q) succeeded", path)
		}
	}
	for name, parse := range map[string]func() error{
		"backup missing": func() error {
			_, err := parseBackupControlPlaneOptions(nil)
			return err
		},
		"backup positional": func() error {
			_, err := parseBackupControlPlaneOptions(append(backupArguments, "extra"))
			return err
		},
		"verify missing": func() error {
			_, err := parseVerifyControlPlaneBackupOptions(nil)
			return err
		},
		"verify positional": func() error {
			_, err := parseVerifyControlPlaneBackupOptions([]string{"-source", filepath.Join(root, "bundle"), "extra"})
			return err
		},
		"restore missing": func() error {
			_, err := parseRestoreControlPlaneOptions(nil)
			return err
		},
		"restore positional": func() error {
			_, err := parseRestoreControlPlaneOptions([]string{
				"-source", filepath.Join(root, "bundle"),
				"-control-db", filepath.Join(root, "restored.db"),
				"-master-key", filepath.Join(root, "restored.key"),
				"-administrator-token-file", filepath.Join(root, "restored.token"),
				"extra",
			})
			return err
		},
	} {
		if err := parse(); err == nil {
			t.Errorf("%s parser succeeded", name)
		}
	}
}

func TestRecoveryCommandsRequireStoppedServerBeforeAccessingState(t *testing.T) {
	t.Parallel()

	directory := t.TempDir()
	databasePath := filepath.Join(directory, "control.db")
	singletonPath := filepath.Join(directory, "host.lock")
	held, err := acquireServerLockAt(databasePath, singletonPath)
	if err != nil {
		t.Fatal(err)
	}
	defer held.Close()
	release := testRecoveryReleaseIdentity()
	backupDestination := filepath.Join(directory, "bundle")
	err = runBackupControlPlane(
		context.Background(),
		controlbackup.CreateOptions{
			DatabasePath: databasePath,
			Destination:  backupDestination,
		},
		release,
		func(path string) (*serverLock, error) {
			return acquireServerLockAt(path, singletonPath)
		},
	)
	if !errors.Is(err, errServerAlreadyRunning) ||
		!strings.Contains(err.Error(), "stopped") {
		t.Fatalf("backup held-lock error = %v", err)
	}
	matches, err := filepath.Glob(backupDestination + "*")
	if err != nil {
		t.Fatal(err)
	} else if len(matches) != 0 {
		t.Fatalf("held-lock backup created artifacts: %v", matches)
	}

	restoreDirectory := t.TempDir()
	err = runRestoreControlPlane(
		context.Background(),
		controlbackup.RestoreOptions{
			Source:       backupDestination,
			DatabasePath: filepath.Join(restoreDirectory, "restored.db"),
		},
		release,
		func() (*serverLock, error) {
			return acquireHostServerLockAt(singletonPath)
		},
	)
	if !errors.Is(err, errServerAlreadyRunning) ||
		!strings.Contains(err.Error(), "stopped") {
		t.Fatalf("restore held-lock error = %v", err)
	}
}

func TestRecoveryCommandRunnersRejectMissingLockDependencies(t *testing.T) {
	t.Parallel()

	release := testRecoveryReleaseIdentity()
	var nilContext context.Context
	if err := runBackupControlPlane(
		context.Background(),
		controlbackup.CreateOptions{},
		release,
		nil,
	); err == nil {
		t.Fatal("backup with nil lock dependency succeeded")
	}
	if err := runRestoreControlPlane(
		context.Background(),
		controlbackup.RestoreOptions{},
		release,
		nil,
	); err == nil {
		t.Fatal("restore with nil lock dependency succeeded")
	}
	if err := runBackupControlPlane(
		t.Context(),
		controlbackup.CreateOptions{},
		release,
		func(string) (*serverLock, error) { return nil, nil },
	); err == nil {
		t.Fatal("backup with nil acquired lock succeeded")
	}
	if err := runRestoreControlPlane(
		t.Context(),
		controlbackup.RestoreOptions{},
		release,
		func() (*serverLock, error) { return nil, nil },
	); err == nil {
		t.Fatal("restore with nil acquired lock succeeded")
	}
	if err := runBackupControlPlane(
		nilContext,
		controlbackup.CreateOptions{},
		release,
		func(string) (*serverLock, error) { return &serverLock{}, nil },
	); err == nil {
		t.Fatal("backup with nil context succeeded")
	}
	if err := runRestoreControlPlane(
		nilContext,
		controlbackup.RestoreOptions{},
		release,
		func() (*serverLock, error) { return &serverLock{}, nil },
	); err == nil {
		t.Fatal("restore with nil context succeeded")
	}
}

func TestRecoveryCommandContextStopIsIdempotentAndCancels(t *testing.T) {
	t.Parallel()

	ctx, stop := recoveryCommandContext()
	stop()
	stop()
	select {
	case <-ctx.Done():
		if !errors.Is(ctx.Err(), context.Canceled) {
			t.Fatalf("recovery command context error = %v, want context.Canceled", ctx.Err())
		}
	case <-t.Context().Done():
		t.Fatal("recovery command context did not stop")
	}
}

func testRecoveryReleaseIdentity() controlbackup.ReleaseIdentity {
	return controlbackup.ReleaseIdentity{
		SourceRevision: "development",
		SQLiteMigrations: controlbackup.MigrationIdentity{
			SHA256: strings.Repeat("1", 64), LatestVersion: 1,
		},
		ClickHouseMigrations: controlbackup.MigrationIdentity{
			SHA256: strings.Repeat("2", 64), LatestVersion: 1,
		},
	}
}
