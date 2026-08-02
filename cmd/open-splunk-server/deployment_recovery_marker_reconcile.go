package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/fs"

	clickhousedriver "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/Suhaibinator/open-splunk/internal/recoverycontract"
	"github.com/Suhaibinator/open-splunk/internal/server"
	"github.com/Suhaibinator/open-splunk/migrations"
	"github.com/google/uuid"
)

// deploymentRecoveryMarkerReconcileOptions requires operators to repeat both
// pieces of the stale marker identity. Reconciliation deliberately has no
// archive path or archive name input: it repairs only the canonical source
// database and never opens, queries, or deletes a native archive.
type deploymentRecoveryMarkerReconcileOptions struct {
	RecoverySetID                string
	ConfirmedRecoverySetID       string
	BackupOperationUUID          string
	ConfirmedBackupOperationUUID string
	Address                      string
	PasswordFile                 string
	CACertFile                   string
	ServerName                   string
}

type deploymentRecoveryMarkerReconcileDependencies struct {
	migrationFiles           fs.FS
	acquireHostLock          hostLockAcquirer
	open                     func(*clickhousedriver.Options) (deploymentRecoverySession, error)
	validateBackupPrivileges func(context.Context, server.ClickHousePrivilegeConnection) error
	validateBackupSource     func(
		context.Context,
		server.ClickHouseRecoveryValidationConnection,
		fs.FS,
	) (server.ClickHouseRecoveryDatabaseInspection, error)
	requireMarker func(
		context.Context,
		server.ClickHouseRecoveryArchiveMarkerConnection,
		string,
		string,
		string,
	) error
	clearMarker func(
		context.Context,
		server.ClickHouseRecoveryArchiveMarkerConnection,
		string,
		string,
		string,
	) error
	requireMarkerAbsent func(
		context.Context,
		server.ClickHouseRecoveryArchiveMarkerConnection,
		string,
	) error
}

func defaultDeploymentRecoveryMarkerReconcileDependencies() deploymentRecoveryMarkerReconcileDependencies {
	return deploymentRecoveryMarkerReconcileDependencies{
		migrationFiles:  migrations.ClickHouse(),
		acquireHostLock: acquireHostServerLock,
		open: func(options *clickhousedriver.Options) (deploymentRecoverySession, error) {
			return openClickHouse(options)
		},
		validateBackupPrivileges: server.ValidateClickHouseBackupPrivileges,
		validateBackupSource:     server.ValidateClickHouseBackupSource,
		requireMarker:            server.RequireClickHouseRecoveryArchiveMarker,
		clearMarker:              server.ClearClickHouseRecoveryArchiveMarker,
		requireMarkerAbsent:      server.RequireClickHouseRecoveryArchiveMarkerAbsent,
	}
}

func runDeploymentRecoveryMarkerReconcileSubcommand(arguments []string) error {
	options, err := parseDeploymentRecoveryMarkerReconcileOptions(arguments)
	if err != nil {
		return err
	}
	ctx, stop := recoveryCommandContext()
	defer stop()
	ctx, cancel := context.WithTimeout(ctx, deploymentRecoveryTimeout)
	defer cancel()
	return runDeploymentRecoveryMarkerReconcile(
		ctx,
		options,
		defaultDeploymentRecoveryMarkerReconcileDependencies(),
	)
}

func parseDeploymentRecoveryMarkerReconcileOptions(
	arguments []string,
) (deploymentRecoveryMarkerReconcileOptions, error) {
	const operation = "reconcile deployment recovery source marker"
	flags := flag.NewFlagSet("reconcile-deployment-recovery-marker", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	var recoverySetID exactOnceStringFlag
	var confirmedRecoverySetID exactOnceStringFlag
	var backupOperationUUID exactOnceStringFlag
	var confirmedBackupOperationUUID exactOnceStringFlag
	var address exactOnceStringFlag
	var passwordFile exactOnceStringFlag
	var caCertFile exactOnceStringFlag
	var serverName exactOnceStringFlag
	flags.Var(&recoverySetID, "recovery-set-id", "exact retained recovery-set marker identity")
	flags.Var(&confirmedRecoverySetID, "confirm-recovery-set-id", "repeat the exact recovery-set marker identity")
	flags.Var(&backupOperationUUID, "backup-operation-uuid", "canonical retained backup operation UUID")
	flags.Var(&confirmedBackupOperationUUID, "confirm-backup-operation-uuid", "repeat the exact backup operation UUID")
	flags.Var(&address, "address", "ClickHouse native TLS address")
	flags.Var(&passwordFile, "password-file", "read-only backup-principal password file")
	flags.Var(&caCertFile, "ca-cert", "explicit certificate-only CA bundle")
	flags.Var(&serverName, "server-name", "explicit ClickHouse TLS certificate name")
	if err := flags.Parse(arguments); err != nil {
		return deploymentRecoveryMarkerReconcileOptions{}, fmt.Errorf(
			"%s: parse flags: %w",
			operation,
			err,
		)
	}
	if flags.NArg() != 0 {
		return deploymentRecoveryMarkerReconcileOptions{}, fmt.Errorf(
			"%s: positional arguments are not allowed",
			operation,
		)
	}
	for _, required := range []struct {
		name  string
		value *exactOnceStringFlag
	}{
		{name: "-recovery-set-id", value: &recoverySetID},
		{name: "-confirm-recovery-set-id", value: &confirmedRecoverySetID},
		{name: "-backup-operation-uuid", value: &backupOperationUUID},
		{name: "-confirm-backup-operation-uuid", value: &confirmedBackupOperationUUID},
		{name: "-address", value: &address},
		{name: "-password-file", value: &passwordFile},
		{name: "-ca-cert", value: &caCertFile},
		{name: "-server-name", value: &serverName},
	} {
		if !required.value.set {
			return deploymentRecoveryMarkerReconcileOptions{}, fmt.Errorf(
				"%s: %s is required",
				operation,
				required.name,
			)
		}
	}
	options := deploymentRecoveryMarkerReconcileOptions{
		RecoverySetID:                recoverySetID.value,
		ConfirmedRecoverySetID:       confirmedRecoverySetID.value,
		BackupOperationUUID:          backupOperationUUID.value,
		ConfirmedBackupOperationUUID: confirmedBackupOperationUUID.value,
		Address:                      address.value,
		PasswordFile:                 passwordFile.value,
		CACertFile:                   caCertFile.value,
		ServerName:                   serverName.value,
	}
	if err := validateDeploymentRecoveryMarkerReconcileConfirmations(options); err != nil {
		return deploymentRecoveryMarkerReconcileOptions{}, fmt.Errorf("%s: %w", operation, err)
	}
	if err := validateDeploymentRecoveryPaths(operation, []deploymentRecoveryPath{
		{name: "-password-file", value: options.PasswordFile},
		{name: "-ca-cert", value: options.CACertFile},
	}); err != nil {
		return deploymentRecoveryMarkerReconcileOptions{}, err
	}
	if err := validateDeploymentRecoveryConnectionFields(
		operation,
		options.Address,
		options.ServerName,
	); err != nil {
		return deploymentRecoveryMarkerReconcileOptions{}, err
	}
	return options, nil
}

func runDeploymentRecoveryMarkerReconcile(
	ctx context.Context,
	options deploymentRecoveryMarkerReconcileOptions,
	dependencies deploymentRecoveryMarkerReconcileDependencies,
) (returnedErr error) {
	const operation = "reconcile deployment recovery source marker"
	if ctx == nil {
		return fmt.Errorf("%s: context is required", operation)
	}
	if err := validateDeploymentRecoveryMarkerReconcileDependencies(dependencies); err != nil {
		return err
	}
	if err := validateDeploymentRecoveryMarkerReconcileConfirmations(options); err != nil {
		return fmt.Errorf("%s: %w", operation, err)
	}
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("%s: %w", operation, err)
	}

	// The host lock must remain held across all credential reads and network
	// activity so a server or another recovery command cannot race the repair.
	lock, err := dependencies.acquireHostLock()
	if err != nil {
		return fmt.Errorf("%s: require the server to be stopped: %w", operation, err)
	}
	if lock == nil {
		return fmt.Errorf("%s: server lock dependency returned no lock", operation)
	}
	defer func() { returnedErr = errors.Join(returnedErr, lock.Close()) }()

	if err := discardDeploymentRecoveryCredentialEnvironment(); err != nil {
		return fmt.Errorf("%s: %w", operation, err)
	}
	session, err := openDeploymentRecoverySession(
		ctx,
		options.Address,
		options.PasswordFile,
		options.CACertFile,
		options.ServerName,
		deploymentRecoveryBackupUsername,
		dependencies.open,
	)
	if err != nil {
		return fmt.Errorf("%s: %w", operation, err)
	}
	defer func() {
		if closeErr := session.Close(); closeErr != nil {
			returnedErr = errors.Join(
				returnedErr,
				fmt.Errorf("%s: close ClickHouse backup session: %w", operation, closeErr),
			)
		}
	}()

	if err := dependencies.validateBackupPrivileges(ctx, session); err != nil {
		return fmt.Errorf("%s: validate ClickHouse backup principal: %w", operation, err)
	}
	if _, err := dependencies.validateBackupSource(
		ctx,
		session,
		dependencies.migrationFiles,
	); err != nil {
		return fmt.Errorf("%s: validate canonical ClickHouse backup source: %w", operation, err)
	}

	markerErr := dependencies.requireMarker(
		ctx,
		session,
		deploymentRecoveryDatabase,
		options.RecoverySetID,
		options.BackupOperationUUID,
	)
	if markerErr != nil {
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("%s: %w", operation, err)
		}
		if !errors.Is(markerErr, server.ErrClickHouseRecoveryArchiveMarkerMismatch) {
			return fmt.Errorf("%s: read exact confirmed marker: %w", operation, markerErr)
		}
		// A previous invocation may have completed the synchronous clear before
		// its caller observed success. Absence is therefore the sole idempotent
		// alternative to the exact confirmed marker. Wrong or duplicate rows
		// fail both checks and are never mutated.
		absentErr := dependencies.requireMarkerAbsent(
			ctx,
			session,
			deploymentRecoveryDatabase,
		)
		if absentErr == nil {
			return nil
		}
		return errors.Join(
			fmt.Errorf("%s: require exact confirmed marker: %w", operation, markerErr),
			fmt.Errorf("%s: marker is not already absent: %w", operation, absentErr),
		)
	}
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("%s: %w", operation, err)
	}
	if err := dependencies.clearMarker(
		ctx,
		session,
		deploymentRecoveryDatabase,
		options.RecoverySetID,
		options.BackupOperationUUID,
	); err != nil {
		return fmt.Errorf("%s: synchronously clear exact confirmed marker: %w", operation, err)
	}
	return nil
}

func validateDeploymentRecoveryMarkerReconcileDependencies(
	dependencies deploymentRecoveryMarkerReconcileDependencies,
) error {
	if dependencies.migrationFiles == nil || dependencies.acquireHostLock == nil ||
		dependencies.open == nil || dependencies.validateBackupPrivileges == nil ||
		dependencies.validateBackupSource == nil || dependencies.requireMarker == nil ||
		dependencies.clearMarker == nil || dependencies.requireMarkerAbsent == nil {
		return errors.New("reconcile deployment recovery source marker: incomplete dependencies")
	}
	return nil
}

func validateDeploymentRecoveryMarkerReconcileConfirmations(
	options deploymentRecoveryMarkerReconcileOptions,
) error {
	if !recoverycontract.ValidRecoverySetID(options.RecoverySetID) {
		return errors.New("recovery set ID must be exactly 32 lowercase hexadecimal characters")
	}
	if options.ConfirmedRecoverySetID != options.RecoverySetID {
		return errors.New("confirmed recovery set ID must exactly repeat the recovery set ID")
	}
	operationUUID, err := uuid.Parse(options.BackupOperationUUID)
	if err != nil || operationUUID == uuid.Nil || operationUUID.String() != options.BackupOperationUUID {
		return errors.New("backup operation UUID must be a canonical non-zero UUID")
	}
	if options.ConfirmedBackupOperationUUID != options.BackupOperationUUID {
		return errors.New("confirmed backup operation UUID must exactly repeat the backup operation UUID")
	}
	return nil
}
