package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"sync"
	"syscall"

	opensplunk "github.com/Suhaibinator/open-splunk"
	"github.com/Suhaibinator/open-splunk/internal/controlbackup"
)

func runBackupControlPlaneSubcommand(arguments []string) error {
	options, err := parseBackupControlPlaneOptions(arguments)
	if err != nil {
		return err
	}
	release, err := loadControlPlaneRecoveryRelease()
	if err != nil {
		return err
	}
	ctx, stop := recoveryCommandContext()
	defer stop()
	return runBackupControlPlane(
		ctx,
		options,
		release,
		acquireServerLock,
	)
}

func runVerifyControlPlaneBackupSubcommand(arguments []string) error {
	source, err := parseVerifyControlPlaneBackupOptions(arguments)
	if err != nil {
		return err
	}
	release, err := loadControlPlaneRecoveryRelease()
	if err != nil {
		return err
	}
	ctx, stop := recoveryCommandContext()
	defer stop()
	if _, err := controlbackup.Verify(ctx, source, release); err != nil {
		return fmt.Errorf("verify control-plane backup: %w", err)
	}
	return nil
}

func runRestoreControlPlaneSubcommand(arguments []string) error {
	options, err := parseRestoreControlPlaneOptions(arguments)
	if err != nil {
		return err
	}
	release, err := loadControlPlaneRecoveryRelease()
	if err != nil {
		return err
	}
	ctx, stop := recoveryCommandContext()
	defer stop()
	return runRestoreControlPlane(
		ctx,
		options,
		release,
		acquireHostServerLock,
	)
}

func recoveryCommandContext() (context.Context, context.CancelFunc) {
	ctx, rawStop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	var stopOnce sync.Once
	stop := func() { stopOnce.Do(rawStop) }
	go func() {
		<-ctx.Done()
		// Restore default signal handling while canceled recovery work runs its
		// bounded cleanup. A second signal may then force termination.
		stop()
	}()
	return ctx, stop
}

func parseBackupControlPlaneOptions(arguments []string) (controlbackup.CreateOptions, error) {
	flags := flag.NewFlagSet("backup-control-plane", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	var options controlbackup.CreateOptions
	flags.StringVar(&options.DatabasePath, "control-db", "", "absolute stopped-server SQLite database path")
	flags.StringVar(&options.MasterKeyPath, "master-key", "", "absolute matching server master-key path")
	flags.StringVar(
		&options.AdministratorTokenPath,
		"administrator-token-file",
		"",
		"absolute matching administrator-token path",
	)
	flags.StringVar(&options.Destination, "destination", "", "absolute new private bundle directory")
	if err := flags.Parse(arguments); err != nil {
		return controlbackup.CreateOptions{}, fmt.Errorf("backup control plane: parse flags: %w", err)
	}
	if flags.NArg() != 0 {
		return controlbackup.CreateOptions{}, errors.New("backup control plane: positional arguments are not allowed")
	}
	for _, item := range []struct {
		name string
		path string
	}{
		{name: "-control-db", path: options.DatabasePath},
		{name: "-master-key", path: options.MasterKeyPath},
		{name: "-administrator-token-file", path: options.AdministratorTokenPath},
		{name: "-destination", path: options.Destination},
	} {
		if err := validateRecoveryCommandPath(item.name, item.path); err != nil {
			return controlbackup.CreateOptions{}, fmt.Errorf("backup control plane: %w", err)
		}
	}
	return options, nil
}

func parseVerifyControlPlaneBackupOptions(arguments []string) (string, error) {
	flags := flag.NewFlagSet("verify-control-plane-backup", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	var source string
	flags.StringVar(&source, "source", "", "absolute private bundle directory")
	if err := flags.Parse(arguments); err != nil {
		return "", fmt.Errorf("verify control-plane backup: parse flags: %w", err)
	}
	if flags.NArg() != 0 {
		return "", errors.New("verify control-plane backup: positional arguments are not allowed")
	}
	if err := validateRecoveryCommandPath("-source", source); err != nil {
		return "", fmt.Errorf("verify control-plane backup: %w", err)
	}
	return source, nil
}

func parseRestoreControlPlaneOptions(arguments []string) (controlbackup.RestoreOptions, error) {
	flags := flag.NewFlagSet("restore-control-plane", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	var options controlbackup.RestoreOptions
	flags.StringVar(&options.Source, "source", "", "absolute verified bundle directory")
	flags.StringVar(&options.DatabasePath, "control-db", "", "absolute absent SQLite database path")
	flags.StringVar(&options.MasterKeyPath, "master-key", "", "absolute absent server master-key path")
	flags.StringVar(
		&options.AdministratorTokenPath,
		"administrator-token-file",
		"",
		"absolute absent administrator-token path",
	)
	if err := flags.Parse(arguments); err != nil {
		return controlbackup.RestoreOptions{}, fmt.Errorf("restore control plane: parse flags: %w", err)
	}
	if flags.NArg() != 0 {
		return controlbackup.RestoreOptions{}, errors.New("restore control plane: positional arguments are not allowed")
	}
	for _, item := range []struct {
		name string
		path string
	}{
		{name: "-source", path: options.Source},
		{name: "-control-db", path: options.DatabasePath},
		{name: "-master-key", path: options.MasterKeyPath},
		{name: "-administrator-token-file", path: options.AdministratorTokenPath},
	} {
		if err := validateRecoveryCommandPath(item.name, item.path); err != nil {
			return controlbackup.RestoreOptions{}, fmt.Errorf("restore control plane: %w", err)
		}
	}
	return options, nil
}

func validateRecoveryCommandPath(name, path string) error {
	return controlbackup.ValidateExactAbsolutePath(name, path)
}

func loadControlPlaneRecoveryRelease() (controlbackup.ReleaseIdentity, error) {
	release, err := opensplunk.EmbeddedRelease()
	if err != nil {
		return controlbackup.ReleaseIdentity{}, fmt.Errorf(
			"load embedded release for control-plane recovery: %w",
			err,
		)
	}
	return controlbackup.ReleaseIdentity{
		ApplicationVersion: release.Metadata.ApplicationVersion,
		SourceRevision:     release.Metadata.SourceRevision,
		SQLiteMigrations: controlbackup.MigrationIdentity{
			SHA256:        release.Metadata.SQLiteMigrations.SHA256,
			LatestVersion: release.Metadata.SQLiteMigrations.LatestVersion,
		},
		ClickHouseMigrations: controlbackup.MigrationIdentity{
			SHA256:        release.Metadata.ClickHouseMigrations.SHA256,
			LatestVersion: release.Metadata.ClickHouseMigrations.LatestVersion,
		},
	}, nil
}

type databaseLockAcquirer func(string) (*serverLock, error)

func runBackupControlPlane(
	ctx context.Context,
	options controlbackup.CreateOptions,
	release controlbackup.ReleaseIdentity,
	acquire databaseLockAcquirer,
) (returnedErr error) {
	if ctx == nil {
		return errors.New("backup control plane: context is required")
	}
	if acquire == nil {
		return errors.New("backup control plane: server lock dependency is required")
	}
	lock, err := acquire(options.DatabasePath)
	if err != nil {
		return fmt.Errorf("backup control plane: require the server to be stopped: %w", err)
	}
	if lock == nil {
		return errors.New("backup control plane: server lock dependency returned no lock")
	}
	defer func() { returnedErr = errors.Join(returnedErr, lock.Close()) }()
	options.Release = release
	if _, err := controlbackup.Create(ctx, options); err != nil {
		return fmt.Errorf("backup control plane: %w", err)
	}
	return nil
}

type hostLockAcquirer func() (*serverLock, error)

func runRestoreControlPlane(
	ctx context.Context,
	options controlbackup.RestoreOptions,
	release controlbackup.ReleaseIdentity,
	acquire hostLockAcquirer,
) (returnedErr error) {
	if ctx == nil {
		return errors.New("restore control plane: context is required")
	}
	if acquire == nil {
		return errors.New("restore control plane: server lock dependency is required")
	}
	lock, err := acquire()
	if err != nil {
		return fmt.Errorf("restore control plane: require the server to be stopped: %w", err)
	}
	if lock == nil {
		return errors.New("restore control plane: server lock dependency returned no lock")
	}
	defer func() { returnedErr = errors.Join(returnedErr, lock.Close()) }()
	options.Release = release
	if err := controlbackup.Restore(ctx, options); err != nil {
		return fmt.Errorf("restore control plane: %w", err)
	}
	return nil
}
