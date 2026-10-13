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
	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/controlbackup"
)

type controlPlaneBackupCommandOptions struct {
	controlbackup.CreateOptions
	recoveryObjectOptions
}
type controlPlaneRestoreCommandOptions struct {
	controlbackup.RestoreOptions
	recoveryObjectOptions
}
type maintenanceLeaseAcquirer func(context.Context, string) (*control.MaintenanceLease, error)
type hostLockAcquirer func() (*serverLock, error)

func runBackupControlPlaneSubcommand(arguments []string) error {
	options, err := parseBackupControlPlaneOptions(arguments)
	if err != nil {
		return err
	}
	hostGuard, err := acquireHostServerLock()
	if err != nil {
		return err
	}
	defer hostGuard.Close()

	release, err := loadControlPlaneRecoveryRelease()
	if err != nil {
		return err
	}
	ctx, stop := recoveryCommandContext()
	defer stop()
	blobs, err := options.open(ctx)
	if err != nil {
		return err
	}
	defer blobs.Close()
	options.Blobs = blobs
	return runBackupControlPlane(ctx, options.CreateOptions, release, control.AcquireMaintenanceLease)
}
func runRestoreControlPlaneSubcommand(arguments []string) error {
	options, err := parseRestoreControlPlaneOptions(arguments)
	if err != nil {
		return err
	}
	hostGuard, err := acquireHostServerLock()
	if err != nil {
		return err
	}
	defer hostGuard.Close()

	release, err := loadControlPlaneRecoveryRelease()
	if err != nil {
		return err
	}
	ctx, stop := recoveryCommandContext()
	defer stop()
	blobs, err := options.open(ctx)
	if err != nil {
		return err
	}
	defer blobs.Close()
	options.Blobs = blobs
	return runRestoreControlPlane(ctx, options.RestoreOptions, release, control.AcquireMaintenanceLease)
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
	_, err = controlbackup.Verify(ctx, source, release)
	return err
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

func parseBackupControlPlaneOptions(arguments []string) (controlPlaneBackupCommandOptions, error) {
	var options controlPlaneBackupCommandOptions
	flags := flag.NewFlagSet("backup-control-plane", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	flags.StringVar(&options.PostgresDSN, "postgres-dsn", consumeRecoverySecret("OPEN_SPLUNK_SERVER_POSTGRES_DSN"), "PostgreSQL control schema; stopped server required")
	flags.StringVar(&options.MasterKeyPath, "master-key", "", "absolute matching server master key")
	flags.StringVar(&options.Destination, "destination", "", "absolute new private bundle directory")
	registerRecoveryObjectFlags(flags, &options.recoveryObjectOptions)
	if err := flags.Parse(arguments); err != nil {
		return options, err
	}
	if flags.NArg() != 0 {
		return options, errors.New("positional arguments are not allowed")
	}
	if err := validateRecoveryDSN(options.PostgresDSN); err != nil {
		return options, err
	}
	if err := validateRecoveryCommandPath("-master-key", options.MasterKeyPath); err != nil {
		return options, err
	}
	if err := validateRecoveryCommandPath("-destination", options.Destination); err != nil {
		return options, err
	}
	return options, options.validate()
}
func parseRestoreControlPlaneOptions(arguments []string) (controlPlaneRestoreCommandOptions, error) {
	var options controlPlaneRestoreCommandOptions
	flags := flag.NewFlagSet("restore-control-plane", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	flags.StringVar(&options.PostgresDSN, "postgres-dsn", consumeRecoverySecret("OPEN_SPLUNK_SERVER_POSTGRES_DSN"), "empty PostgreSQL control schema")
	flags.StringVar(&options.MasterKeyPath, "master-key", "", "absolute absent server master key")
	flags.StringVar(&options.Source, "source", "", "absolute verified bundle directory")
	registerRecoveryObjectFlags(flags, &options.recoveryObjectOptions)
	if err := flags.Parse(arguments); err != nil {
		return options, err
	}
	if flags.NArg() != 0 {
		return options, errors.New("positional arguments are not allowed")
	}
	if err := validateRecoveryDSN(options.PostgresDSN); err != nil {
		return options, err
	}
	if err := validateRecoveryCommandPath("-master-key", options.MasterKeyPath); err != nil {
		return options, err
	}
	if err := validateRecoveryCommandPath("-source", options.Source); err != nil {
		return options, err
	}
	return options, options.validate()
}
func parseVerifyControlPlaneBackupOptions(arguments []string) (string, error) {
	flags := flag.NewFlagSet("verify-control-plane-backup", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	var source string
	flags.StringVar(&source, "source", "", "absolute private bundle directory")
	if err := flags.Parse(arguments); err != nil {
		return "", err
	}
	if flags.NArg() != 0 {
		return "", errors.New("positional arguments are not allowed")
	}
	return source, validateRecoveryCommandPath("-source", source)
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
		SourceRevision: release.Metadata.SourceRevision,
		PostgresMigrations: controlbackup.MigrationIdentity{
			SHA256:        release.Metadata.PostgresMigrations.SHA256,
			LatestVersion: release.Metadata.PostgresMigrations.LatestVersion,
		},
		ClickHouseMigrations: controlbackup.MigrationIdentity{
			SHA256:        release.Metadata.ClickHouseMigrations.SHA256,
			LatestVersion: release.Metadata.ClickHouseMigrations.LatestVersion,
		},
	}, nil
}

func runBackupControlPlane(ctx context.Context, options controlbackup.CreateOptions, release controlbackup.ReleaseIdentity, acquire maintenanceLeaseAcquirer) (returnedErr error) {
	if ctx == nil || acquire == nil {
		return errors.New("backup control plane: context and PostgreSQL lease dependency required")
	}
	lease, err := acquire(ctx, options.PostgresDSN)
	if err != nil {
		return err
	}
	if lease == nil {
		return errors.New("PostgreSQL maintenance lease missing")
	}
	defer func() { returnedErr = errors.Join(returnedErr, lease.Close()) }()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() {
		select {
		case <-lease.OwnershipLost():
			cancel()
		case <-ctx.Done():
		}
	}()
	options.Lease = lease
	options.Release = release
	_, err = controlbackup.Create(ctx, options)
	return err
}
func runRestoreControlPlane(ctx context.Context, options controlbackup.RestoreOptions, release controlbackup.ReleaseIdentity, acquire maintenanceLeaseAcquirer) (returnedErr error) {
	if ctx == nil || acquire == nil {
		return errors.New("restore control plane: context and PostgreSQL lease dependency required")
	}
	lease, err := acquire(ctx, options.PostgresDSN)
	if err != nil {
		return err
	}
	if lease == nil {
		return errors.New("PostgreSQL maintenance lease missing")
	}
	defer func() { returnedErr = errors.Join(returnedErr, lease.Close()) }()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() {
		select {
		case <-lease.OwnershipLost():
			cancel()
		case <-ctx.Done():
		}
	}()
	options.Lease = lease
	options.Release = release
	return controlbackup.Restore(ctx, options)
}
