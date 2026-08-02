package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/Suhaibinator/open-splunk/internal/controlbackup"
	"github.com/Suhaibinator/open-splunk/internal/recoverycontract"
	"github.com/Suhaibinator/open-splunk/internal/recoveryset"
)

type deploymentRecoveryArchiveDeleteOptions struct {
	ArchiveRoot          string
	ArchiveName          string
	ConfirmedArchiveName string
}

type deploymentRecoveryArchiveDeleteDependencies struct {
	effectiveUID func() int
	effectiveGID func() int
	delete       func(context.Context, recoveryset.DeleteAttestedArchiveOptions) error
}

func runDeleteDeploymentRecoveryArchiveSubcommand(arguments []string) error {
	ctx, stop := recoveryCommandContext()
	defer stop()
	return runDeleteDeploymentRecoveryArchiveSubcommandWithDependencies(
		ctx,
		arguments,
		deploymentRecoveryArchiveDeleteDependencies{
			effectiveUID: os.Geteuid,
			effectiveGID: os.Getegid,
			delete:       recoveryset.DeleteAttestedArchive,
		},
	)
}

func runDeleteDeploymentRecoveryArchiveSubcommandWithDependencies(
	ctx context.Context,
	arguments []string,
	dependencies deploymentRecoveryArchiveDeleteDependencies,
) error {
	options, err := parseDeploymentRecoveryArchiveDeleteOptions(arguments)
	if err != nil {
		return err
	}
	if ctx == nil {
		return errors.New("delete deployment recovery archive: context is required")
	}
	if dependencies.effectiveUID == nil || dependencies.effectiveGID == nil ||
		dependencies.delete == nil {
		return errors.New("delete deployment recovery archive: dependencies are incomplete")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if effectiveUID := dependencies.effectiveUID(); effectiveUID != deploymentRecoveryArchiveUID {
		return fmt.Errorf(
			"delete deployment recovery archive: effective UID is %d, want exactly %d",
			effectiveUID,
			deploymentRecoveryArchiveUID,
		)
	}
	if effectiveGID := dependencies.effectiveGID(); effectiveGID != deploymentRecoveryArchiveGID {
		return fmt.Errorf(
			"delete deployment recovery archive: effective GID is %d, want exactly %d",
			effectiveGID,
			deploymentRecoveryArchiveGID,
		)
	}
	if err := dependencies.delete(ctx, recoveryset.DeleteAttestedArchiveOptions{
		ArchiveRoot:          options.ArchiveRoot,
		ArchiveName:          options.ArchiveName,
		ConfirmedArchiveName: options.ConfirmedArchiveName,
		ArchiveOwnership:     deploymentRecoveryArchiveOwnership(),
	}); err != nil {
		return fmt.Errorf("delete deployment recovery archive: %w", err)
	}
	return nil
}

func parseDeploymentRecoveryArchiveDeleteOptions(
	arguments []string,
) (deploymentRecoveryArchiveDeleteOptions, error) {
	flags := flag.NewFlagSet("delete-deployment-recovery-archive", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	var archiveRoot exactOnceStringFlag
	var archiveName exactOnceStringFlag
	var confirmedArchiveName exactOnceStringFlag
	flags.Var(&archiveRoot, "archive-root", "exact ClickHouse recovery archive volume root")
	flags.Var(&archiveName, "archive-name", "canonical ClickHouse archive name to destroy")
	flags.Var(&confirmedArchiveName, "confirm-archive-name", "repeat the exact archive name to attest destructive deletion")
	if err := flags.Parse(arguments); err != nil {
		return deploymentRecoveryArchiveDeleteOptions{}, fmt.Errorf(
			"delete deployment recovery archive: parse flags: %w",
			err,
		)
	}
	if flags.NArg() != 0 {
		return deploymentRecoveryArchiveDeleteOptions{}, errors.New(
			"delete deployment recovery archive: positional arguments are not allowed",
		)
	}
	if !archiveRoot.set || !archiveName.set || !confirmedArchiveName.set {
		return deploymentRecoveryArchiveDeleteOptions{}, errors.New(
			"delete deployment recovery archive: -archive-root, -archive-name, and -confirm-archive-name are required",
		)
	}
	if err := controlbackup.ValidateExactAbsolutePath("-archive-root", archiveRoot.value); err != nil {
		return deploymentRecoveryArchiveDeleteOptions{}, fmt.Errorf(
			"delete deployment recovery archive: %w",
			err,
		)
	}
	if !recoverycontract.ValidArchiveName(archiveName.value) {
		return deploymentRecoveryArchiveDeleteOptions{}, errors.New(
			"delete deployment recovery archive: -archive-name must be exactly 32 lowercase hexadecimal characters followed by .tar.zst",
		)
	}
	if confirmedArchiveName.value != archiveName.value {
		return deploymentRecoveryArchiveDeleteOptions{}, errors.New(
			"delete deployment recovery archive: -confirm-archive-name must exactly match -archive-name",
		)
	}
	return deploymentRecoveryArchiveDeleteOptions{
		ArchiveRoot:          archiveRoot.value,
		ArchiveName:          archiveName.value,
		ConfirmedArchiveName: confirmedArchiveName.value,
	}, nil
}
