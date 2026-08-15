package main

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/recoverycontract"
	"github.com/Suhaibinator/open-splunk/internal/recoveryset"
	"github.com/Suhaibinator/open-splunk/internal/server"
	"github.com/google/uuid"
)

const (
	deploymentRecoveryDisk                  = recoverycontract.Disk
	deploymentRecoveryDatabase              = recoverycontract.CanonicalDatabase
	deploymentRecoveryArchiveDatabasePrefix = recoverycontract.ArchiveDatabasePrefix
	deploymentRecoveryArchiveSuffix         = recoverycontract.ArchiveSuffix
	deploymentRecoveryBackupCompleteStatus  = "BACKUP_CREATED"
	deploymentRecoveryRestoreCompleteStatus = "RESTORED"
)

type deploymentRecoveryPostRestoreVerifier func(
	context.Context,
) (recoveryset.Verification, error)

func runDeploymentNativeBackup(
	ctx context.Context,
	session deploymentRecoverySession,
	migrationFiles fs.FS,
	request recoveryset.NativeBackupRequest,
	dependencies deploymentRecoveryDependencies,
) (identity recoveryset.NativeBackupIdentity, returnedErr error) {
	if ctx == nil || session == nil || migrationFiles == nil ||
		dependencies.validateBackupSource == nil ||
		dependencies.newOperationUUID == nil {
		return recoveryset.NativeBackupIdentity{}, errors.New(
			"native ClickHouse backup: context, session, migrations, validator, and UUID source are required",
		)
	}
	if err := ctx.Err(); err != nil {
		return recoveryset.NativeBackupIdentity{}, err
	}
	if err := validateDeploymentNativeBackupRequest(request); err != nil {
		return recoveryset.NativeBackupIdentity{}, err
	}

	before, err := dependencies.validateBackupSource(
		ctx,
		session,
		migrationFiles,
	)
	if err != nil {
		return recoveryset.NativeBackupIdentity{}, fmt.Errorf(
			"native ClickHouse backup: validate source before snapshot: %w",
			err,
		)
	}
	operationID, err := nextDeploymentRecoveryOperationUUID(
		dependencies.newOperationUUID,
	)
	if err != nil {
		return recoveryset.NativeBackupIdentity{}, fmt.Errorf(
			"native ClickHouse backup: allocate operation ID: %w",
			err,
		)
	}
	if err := requireDeploymentRecoveryOperationUUIDDistinct(
		operationID,
		before.DatabaseUUID,
		before.SchemaMigrationsTableUUID,
		before.EventsTableUUID,
		before.RecoverySetsTableUUID,
		before.RecoveryArchiveMarkersTableUUID,
	); err != nil {
		return recoveryset.NativeBackupIdentity{}, fmt.Errorf(
			"native ClickHouse backup: %w",
			err,
		)
	}
	markerReconciliation := fmt.Sprintf(
		"recovery-set ID %q, backup operation UUID %q; follow interrupted-marker reconciliation if the source archive marker remains",
		request.RecoverySetID,
		operationID.String(),
	)
	markerNeedsCleanup := true
	defer func() {
		if !markerNeedsCleanup {
			return
		}
		cleanupContext, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
		defer cancel()
		cleanupErr := server.ClearClickHouseRecoveryArchiveMarker(
			cleanupContext,
			session,
			deploymentRecoveryDatabase,
			request.RecoverySetID,
			operationID.String(),
		)
		if cleanupErr != nil {
			returnedErr = errors.Join(
				returnedErr,
				fmt.Errorf(
					"native ClickHouse backup: %s: clear source archive marker: %w",
					markerReconciliation,
					cleanupErr,
				),
			)
		}
	}()
	if err := server.WriteClickHouseRecoveryArchiveMarker(
		ctx,
		session,
		deploymentRecoveryDatabase,
		request.RecoverySetID,
		operationID.String(),
	); err != nil {
		return recoveryset.NativeBackupIdentity{}, fmt.Errorf(
			"native ClickHouse backup: %s: publish source archive marker: %w",
			markerReconciliation,
			err,
		)
	}
	// Once ClickHouse has received the BACKUP request, a client error or an
	// unexpected result cannot prove that the server-side operation stopped. Keep
	// the marker as the durable retry fence until exact terminal completion is
	// observed; the operator reconciliation path restarts ClickHouse before
	// clearing an ambiguous operation.
	markerNeedsCleanup = false
	if err := runDeploymentRecoveryOperation(
		ctx,
		session,
		deploymentRecoveryBackupQuery(request, operationID),
		operationID,
		deploymentRecoveryBackupCompleteStatus,
	); err != nil {
		return recoveryset.NativeBackupIdentity{}, fmt.Errorf(
			"native ClickHouse backup: recovery-set ID %q, backup operation UUID %q did not prove exact terminal completion; source archive marker retained, follow interrupted-marker reconciliation: %w",
			request.RecoverySetID,
			operationID.String(),
			err,
		)
	}
	markerNeedsCleanup = true
	if err := server.RequireClickHouseRecoveryArchiveMarker(
		ctx,
		session,
		deploymentRecoveryDatabase,
		request.RecoverySetID,
		operationID.String(),
	); err != nil {
		return recoveryset.NativeBackupIdentity{}, fmt.Errorf(
			"native ClickHouse backup: %s: revalidate source archive marker after snapshot: %w",
			markerReconciliation,
			err,
		)
	}
	after, err := dependencies.validateBackupSource(
		ctx,
		session,
		migrationFiles,
	)
	if err != nil {
		return recoveryset.NativeBackupIdentity{}, fmt.Errorf(
			"native ClickHouse backup: %s: validate source after snapshot: %w",
			markerReconciliation,
			err,
		)
	}
	if before != after {
		return recoveryset.NativeBackupIdentity{}, fmt.Errorf(
			"native ClickHouse backup: %s: source identity or visibility changed during snapshot",
			markerReconciliation,
		)
	}
	if err := server.ClearClickHouseRecoveryArchiveMarker(
		ctx,
		session,
		deploymentRecoveryDatabase,
		request.RecoverySetID,
		operationID.String(),
	); err != nil {
		return recoveryset.NativeBackupIdentity{}, fmt.Errorf(
			"native ClickHouse backup: %s: clear source archive marker after snapshot: %w",
			markerReconciliation,
			err,
		)
	}
	markerNeedsCleanup = false
	return recoveryset.NativeBackupIdentity{
		ServerVersion:                   before.ServerVersion,
		MigrationLedgerSHA256:           hex.EncodeToString(before.MigrationLedger.SHA256[:]),
		DatabaseUUID:                    before.DatabaseUUID,
		SchemaMigrationsTableUUID:       before.SchemaMigrationsTableUUID,
		EventsTableUUID:                 before.EventsTableUUID,
		RecoverySetsTableUUID:           before.RecoverySetsTableUUID,
		RecoveryArchiveMarkersTableUUID: before.RecoveryArchiveMarkersTableUUID,
		MaxVisibilitySeq:                before.MaximumVisibilitySequence,
		BackupOperationUUID:             operationID.String(),
	}, nil
}

func runDeploymentNativeRestore(
	ctx context.Context,
	session deploymentRecoverySession,
	verification recoveryset.Verification,
	postRestoreVerify deploymentRecoveryPostRestoreVerifier,
	dependencies deploymentRecoveryDependencies,
) error {
	return runDeploymentRestoreStateMachine(
		ctx,
		session,
		verification,
		postRestoreVerify,
		dependencies,
	)
}

func runDeploymentRestoreStateMachine(
	ctx context.Context,
	session deploymentRecoverySession,
	verification recoveryset.Verification,
	postRestoreVerify deploymentRecoveryPostRestoreVerifier,
	dependencies deploymentRecoveryDependencies,
) error {
	if ctx == nil || session == nil || dependencies.migrationFiles == nil ||
		dependencies.inspectDatabase == nil || dependencies.readReceipt == nil ||
		dependencies.writeReceipt == nil || dependencies.listRecoveryDatabases == nil ||
		dependencies.validateRestoreDisk == nil || dependencies.newOperationUUID == nil ||
		dependencies.now == nil || postRestoreVerify == nil {
		return errors.New("native ClickHouse restore: incomplete dependencies")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	manifest := verification.Manifest
	if err := validateDeploymentVerifiedRecoveryIdentity(verification); err != nil {
		return err
	}
	recoverySetID := manifest.RecoverySetID
	databaseNames, err := dependencies.listRecoveryDatabases(ctx, session)
	if err != nil {
		return fmt.Errorf("native ClickHouse restore: inspect database namespace: %w", err)
	}
	finalExists, err := classifyDeploymentRecoveryDatabaseNamespace(databaseNames)
	if err != nil {
		return fmt.Errorf("native ClickHouse restore: reject database namespace: %w", err)
	}
	if finalExists {
		if err := validateDeploymentRestoredDatabaseAndReceipt(
			ctx,
			session,
			deploymentRecoveryDatabase,
			verification,
			dependencies,
			true,
		); err != nil {
			return fmt.Errorf(
				"native ClickHouse restore: existing canonical database is not an exact resumable restore; use a fresh ClickHouse data volume: %w",
				err,
			)
		}
		return requireDeploymentRecoveryNamespaceState(
			ctx,
			session,
			dependencies,
			true,
		)
	}
	{
		if err := dependencies.validateRestoreDisk(ctx, session); err != nil {
			return fmt.Errorf("native ClickHouse restore: validate read-only recovery disk: %w", err)
		}
		operationID, err := nextDeploymentRecoveryOperationUUID(
			dependencies.newOperationUUID,
		)
		if err != nil {
			return fmt.Errorf("native ClickHouse restore: allocate operation ID: %w", err)
		}
		if err := requireDeploymentRecoveryOperationUUIDDistinct(
			operationID,
			manifest.ClickHouse.BackupOperationUUID,
			manifest.ClickHouse.DatabaseUUID,
			manifest.ClickHouse.SchemaMigrationsTableUUID,
			manifest.ClickHouse.EventsTableUUID,
			manifest.ClickHouse.RecoverySetsTableUUID,
			manifest.ClickHouse.RecoveryArchiveMarkersTableUUID,
		); err != nil {
			return fmt.Errorf("native ClickHouse restore: %w", err)
		}
		if err := runDeploymentRecoveryOperation(
			ctx,
			session,
			deploymentRecoveryRestoreQuery(
				verification,
				deploymentRecoveryDatabase,
				operationID,
			),
			operationID,
			deploymentRecoveryRestoreCompleteStatus,
		); err != nil {
			return fmt.Errorf(
				"native ClickHouse restore operation %s: %w",
				operationID,
				err,
			)
		}
		if err := requireDeploymentRecoveryNamespaceState(
			ctx,
			session,
			dependencies,
			true,
		); err != nil {
			return fmt.Errorf(
				"native ClickHouse restore: revalidate database namespace after native restore: %w",
				err,
			)
		}
		reverified, err := postRestoreVerify(ctx)
		if err != nil {
			return fmt.Errorf(
				"native ClickHouse restore: reverify recovery set after native restore: %w",
				err,
			)
		}
		if err := validateDeploymentPostRestoreVerification(
			verification,
			reverified,
		); err != nil {
			return err
		}
		restoredInspection, err := inspectDeploymentRestoredDatabase(
			ctx,
			session,
			deploymentRecoveryDatabase,
			verification,
			dependencies,
		)
		if err != nil {
			return fmt.Errorf("native ClickHouse restore: validate new canonical database: %w", err)
		}
		if err := server.RequireClickHouseRecoveryArchiveMarker(
			ctx,
			session,
			deploymentRecoveryDatabase,
			recoverySetID,
			manifest.ClickHouse.BackupOperationUUID,
		); err != nil {
			return fmt.Errorf("native ClickHouse restore: validate restored archive marker: %w", err)
		}
		if _, err := dependencies.writeReceipt(
			ctx,
			session,
			deploymentRecoveryDatabase,
			server.ClickHouseRecoveryReceipt{
				RecoverySetID:                   recoverySetID,
				DeploymentManifestSHA256:        verification.ManifestSHA256,
				DatabaseUUID:                    restoredInspection.DatabaseUUID,
				SchemaMigrationsTableUUID:       restoredInspection.SchemaMigrationsTableUUID,
				EventsTableUUID:                 restoredInspection.EventsTableUUID,
				RecoverySetsTableUUID:           restoredInspection.RecoverySetsTableUUID,
				RecoveryArchiveMarkersTableUUID: restoredInspection.RecoveryArchiveMarkersTableUUID,
				RestoredAt:                      dependencies.now(),
			},
		); err != nil {
			return fmt.Errorf("native ClickHouse restore: publish canonical receipt: %w", err)
		}
		if err := validateDeploymentRestoredDatabaseAndReceipt(
			ctx,
			session,
			deploymentRecoveryDatabase,
			verification,
			dependencies,
			true,
		); err != nil {
			return fmt.Errorf("native ClickHouse restore: revalidate receipted canonical database: %w", err)
		}
	}

	if err := validateDeploymentRestoredDatabaseAndReceipt(
		ctx,
		session,
		deploymentRecoveryDatabase,
		verification,
		dependencies,
		false,
	); err != nil {
		return fmt.Errorf("native ClickHouse restore: validate completed canonical database: %w", err)
	}
	return requireDeploymentRecoveryNamespaceState(
		ctx,
		session,
		dependencies,
		true,
	)
}

func classifyDeploymentRecoveryDatabaseNamespace(
	names []string,
) (bool, error) {
	var finalExists bool
	if len(names) > deploymentRecoveryDatabaseNamespaceScanLimit {
		return false, fmt.Errorf(
			"bounded database namespace contains %d entries, want at most %d",
			len(names),
			deploymentRecoveryDatabaseNamespaceScanLimit,
		)
	}
	for index, name := range names {
		if !strings.HasPrefix(name, deploymentRecoveryDatabase) {
			return false, fmt.Errorf(
				"database namespace entry %q is outside the reserved prefix",
				name,
			)
		}
		if index > 0 && names[index-1] >= name {
			return false, errors.New(
				"database namespace is not strictly sorted and unique",
			)
		}
		switch name {
		case deploymentRecoveryDatabase:
			finalExists = true
		default:
			return false, fmt.Errorf(
				"unexpected reserved database %q; use a fresh ClickHouse data volume",
				name,
			)
		}
	}
	return finalExists, nil
}

func requireDeploymentRecoveryNamespaceState(
	ctx context.Context,
	session deploymentRecoverySession,
	dependencies deploymentRecoveryDependencies,
	wantFinal bool,
) error {
	names, err := dependencies.listRecoveryDatabases(ctx, session)
	if err != nil {
		return fmt.Errorf("inspect database namespace: %w", err)
	}
	finalExists, err := classifyDeploymentRecoveryDatabaseNamespace(names)
	if err != nil {
		return err
	}
	if finalExists != wantFinal {
		return fmt.Errorf(
			"canonical database presence is %t, want %t",
			finalExists,
			wantFinal,
		)
	}
	return nil
}

func validateDeploymentPostRestoreVerification(
	expected recoveryset.Verification,
	actual recoveryset.Verification,
) error {
	if actual != expected {
		return errors.New(
			"native ClickHouse restore: recovery manifest or archive digest changed during native restore",
		)
	}
	return nil
}

func validateDeploymentRestoredDatabaseAndReceipt(
	ctx context.Context,
	session deploymentRecoverySession,
	databaseName string,
	verification recoveryset.Verification,
	dependencies deploymentRecoveryDependencies,
	consumeArchiveMarker bool,
) error {
	inspection, err := inspectDeploymentRestoredDatabase(
		ctx,
		session,
		databaseName,
		verification,
		dependencies,
	)
	if err != nil {
		return err
	}
	receipt, err := dependencies.readReceipt(
		ctx,
		session,
		databaseName,
		verification.Manifest.RecoverySetID,
		verification.ManifestSHA256,
	)
	if err != nil {
		return err
	}
	if receipt.DatabaseUUID != inspection.DatabaseUUID ||
		receipt.SchemaMigrationsTableUUID != inspection.SchemaMigrationsTableUUID ||
		receipt.EventsTableUUID != inspection.EventsTableUUID ||
		receipt.RecoverySetsTableUUID != inspection.RecoverySetsTableUUID ||
		receipt.RecoveryArchiveMarkersTableUUID != inspection.RecoveryArchiveMarkersTableUUID {
		return errors.New("restored ClickHouse database UUID identity differs from its recovery receipt")
	}
	if consumeArchiveMarker {
		if err := server.ClearClickHouseRecoveryArchiveMarker(
			ctx,
			session,
			databaseName,
			verification.Manifest.RecoverySetID,
			verification.Manifest.ClickHouse.BackupOperationUUID,
		); err != nil {
			return err
		}
	} else if err := server.RequireClickHouseRecoveryArchiveMarkerAbsent(
		ctx,
		session,
		databaseName,
	); err != nil {
		return err
	}
	return nil
}

func inspectDeploymentRestoredDatabase(
	ctx context.Context,
	session deploymentRecoverySession,
	databaseName string,
	verification recoveryset.Verification,
	dependencies deploymentRecoveryDependencies,
) (server.ClickHouseRecoveryDatabaseInspection, error) {
	inspection, err := dependencies.inspectDatabase(
		ctx,
		session,
		dependencies.migrationFiles,
		databaseName,
	)
	if err != nil {
		return server.ClickHouseRecoveryDatabaseInspection{}, err
	}
	manifest := verification.Manifest
	identity := manifest.ClickHouse
	ledgerSHA256 := hex.EncodeToString(inspection.MigrationLedger.SHA256[:])
	if inspection.DatabaseName != databaseName ||
		inspection.ServerVersion != identity.ServerVersion ||
		inspection.DatabaseEngine != "Atomic" ||
		inspection.MaximumVisibilitySequence != identity.MaxVisibilitySeq ||
		inspection.ActiveMutationCount != 0 ||
		inspection.MigrationLedger.LatestVersion != manifest.ClickHouseMigrations.LatestVersion ||
		ledgerSHA256 != identity.MigrationLedgerSHA256 {
		return server.ClickHouseRecoveryDatabaseInspection{}, errors.New(
			"restored ClickHouse database identity, schema, ledger, visibility, or mutation state differs from the recovery manifest",
		)
	}
	return inspection, nil
}

func validateDeploymentNativeBackupRequest(
	request recoveryset.NativeBackupRequest,
) error {
	if !recoverycontract.ValidRecoverySetID(request.RecoverySetID) ||
		request.Disk != deploymentRecoveryDisk ||
		request.Database != deploymentRecoveryDatabase ||
		request.ArchiveDatabase != deploymentRecoveryArchiveDatabasePrefix+request.RecoverySetID ||
		request.ArchiveName != request.RecoverySetID+deploymentRecoveryArchiveSuffix ||
		request.ArchivePath == "" || !filepath.IsAbs(request.ArchivePath) ||
		filepath.Clean(request.ArchivePath) != request.ArchivePath ||
		filepath.Base(request.ArchivePath) != request.ArchiveName {
		return errors.New("native ClickHouse backup: recovery request identity is invalid")
	}
	return nil
}

func validateDeploymentVerifiedRecoveryIdentity(
	verification recoveryset.Verification,
) error {
	manifest := verification.Manifest
	if !recoverycontract.ValidRecoverySetID(manifest.RecoverySetID) ||
		len(verification.ManifestSHA256) != 64 ||
		strings.Trim(verification.ManifestSHA256, "0123456789abcdef") != "" ||
		manifest.ClickHouse.Disk != deploymentRecoveryDisk ||
		manifest.ClickHouse.Database != deploymentRecoveryDatabase ||
		manifest.ClickHouse.ArchiveDatabase != deploymentRecoveryArchiveDatabasePrefix+manifest.RecoverySetID ||
		manifest.ClickHouse.Archive.Name != manifest.RecoverySetID+deploymentRecoveryArchiveSuffix {
		return errors.New("native ClickHouse restore: verified recovery identity is invalid")
	}
	return nil
}

func deploymentRecoveryBackupQuery(
	request recoveryset.NativeBackupRequest,
	operationID uuid.UUID,
) string {
	return fmt.Sprintf(
		"BACKUP DATABASE open_splunk AS %s "+
			"TO Disk('open_splunk_recovery', '%s') "+
			"SETTINGS id = '%s'",
		request.ArchiveDatabase,
		request.ArchiveName,
		operationID.String(),
	)
}

func deploymentRecoveryRestoreQuery(
	verification recoveryset.Verification,
	targetDatabase string,
	operationID uuid.UUID,
) string {
	identity := verification.Manifest.ClickHouse
	return fmt.Sprintf(
		"RESTORE DATABASE %s AS %s "+
			"FROM Disk('open_splunk_recovery', '%s') "+
			"SETTINGS id = '%s', create_database = 'create', "+
			"create_table = 'create', allow_non_empty_tables = false",
		identity.ArchiveDatabase,
		targetDatabase,
		identity.Archive.Name,
		operationID.String(),
	)
}

func runDeploymentRecoveryOperation(
	ctx context.Context,
	session deploymentRecoverySession,
	query string,
	operationID uuid.UUID,
	wantStatus string,
) error {
	var returnedID string
	var status string
	if err := session.QueryRow(
		ctx,
		query,
	).Scan(&returnedID, &status); err != nil {
		return fmt.Errorf("execute native operation %s: %w", operationID, err)
	}
	if returnedID != operationID.String() || status != wantStatus {
		return fmt.Errorf(
			"native operation %s returned id=%q status=%q, want exact id and status=%q",
			operationID,
			returnedID,
			status,
			wantStatus,
		)
	}
	return nil
}

func nextDeploymentRecoveryOperationUUID(
	generate func() (uuid.UUID, error),
) (uuid.UUID, error) {
	if generate == nil {
		return uuid.Nil, errors.New("operation UUID source is required")
	}
	value, err := generate()
	if err != nil {
		return uuid.Nil, err
	}
	if value == uuid.Nil || value.String() == "00000000-0000-0000-0000-000000000000" {
		return uuid.Nil, errors.New("operation UUID source returned the nil UUID")
	}
	return value, nil
}

func requireDeploymentRecoveryOperationUUIDDistinct(
	operationID uuid.UUID,
	reserved ...string,
) error {
	if slices.Contains(reserved, operationID.String()) {
		return fmt.Errorf(
			"operation UUID %s reuses a durable database, table, or backup operation identity",
			operationID,
		)
	}
	return nil
}
