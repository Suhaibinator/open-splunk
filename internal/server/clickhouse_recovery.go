package server

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io/fs"
	"strconv"
	"time"

	clickhouserow "github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/Suhaibinator/open-splunk/internal/buildinfo"
	"github.com/Suhaibinator/open-splunk/internal/recoverycontract"
	"github.com/google/uuid"
)

var (
	// ErrClickHouseRecoveryStateInvalid means a recovery database is missing or
	// does not have the exact Atomic and UUID identity needed for a release-owned
	// native backup or restore.
	ErrClickHouseRecoveryStateInvalid = errors.New(
		"server: ClickHouse recovery database state is invalid",
	)
	// ErrClickHouseRecoveryActiveMutations prevents a source backup from racing
	// an unfinished physical deletion or other table mutation.
	ErrClickHouseRecoveryActiveMutations = errors.New(
		"server: ClickHouse recovery source has active mutations",
	)
	// ErrClickHouseRecoveryReceiptMismatch means a restored database has no one
	// exact singleton receipt for the expected recovery set and outer manifest.
	ErrClickHouseRecoveryReceiptMismatch = errors.New(
		"server: ClickHouse recovery receipt is missing or differs",
	)
	// ErrClickHouseRecoveryArchiveMarkerMismatch means the restored database did
	// not carry the exact singleton identity written immediately before BACKUP.
	ErrClickHouseRecoveryArchiveMarkerMismatch = errors.New(
		"server: ClickHouse recovery archive marker is missing or differs",
	)
	// ErrClickHouseRecoveryDiskWritable means ClickHouse can mutate the archive
	// pathname after Open Splunk verifies it. Restore is allowed only while
	// ClickHouse itself sees the exact recovery disk as read-only.
	ErrClickHouseRecoveryDiskWritable = errors.New(
		"server: ClickHouse recovery disk is not the exact read-only restore disk",
	)
)

const (
	clickHouseRecoveryDatabaseMetadataQuery = `
		SELECT count(), any(engine), toString(any(uuid))
		FROM system.databases
		WHERE name = ?`
	clickHouseRecoveryActiveMutationsQuery = `
		SELECT count()
		FROM system.mutations
		WHERE database = ? AND is_done = 0`
	clickHouseRecoveryDiskMetadataQuery = `
		SELECT count(), any(path), any(is_read_only)
		FROM system.disks
		WHERE name = ?`
	clickHouseRecoveryDiskPath             = "/var/lib/open-splunk-clickhouse-backups/"
	clickHouseMigrationLedgerDigestDomain  = "open-splunk/clickhouse-migration-ledger/v1\x00"
	clickHouseRecoverySingletonRowSentinel = 2
	clickHouseRecoverySingletonByteLimit   = 1 << 20
)

// ClickHouseMigrationLedgerConnection is the read-only connection surface used
// to validate a migration ledger. Requiring no Exec method makes the helper
// usable by backup tooling without accidentally admitting schema mutation.
type ClickHouseMigrationLedgerConnection interface {
	Select(ctx context.Context, dest any, query string, args ...any) error
}

// ClickHouseRecoveryValidationConnection is the minimal read-only surface used
// for version, schema, identity, visibility, and mutation inspection.
type ClickHouseRecoveryValidationConnection interface {
	ClickHouseVersionConnection
	ClickHouseMigrationLedgerConnection
}

// ClickHouseRecoveryReceiptConnection is the minimal surface needed for the
// intentional post-restore singleton write and its exact readback.
type ClickHouseRecoveryReceiptConnection interface {
	QueryRow(ctx context.Context, query string, args ...any) clickhouserow.Row
	Exec(ctx context.Context, query string, args ...any) error
}

// ClickHouseRecoveryArchiveMarkerConnection is the bounded surface used to
// write, consume, and prove absence of the singleton identity carried inside a
// native deployment archive.
type ClickHouseRecoveryArchiveMarkerConnection interface {
	QueryRow(ctx context.Context, query string, args ...any) clickhouserow.Row
	Exec(ctx context.Context, query string, args ...any) error
}

// ClickHouseRecoveryDiskConnection is the single read-only query needed to
// attest the native recovery disk before ClickHouse opens an archive by name.
type ClickHouseRecoveryDiskConnection interface {
	QueryRow(ctx context.Context, query string, args ...any) clickhouserow.Row
}

// ValidateClickHouseRecoveryDiskReadOnly proves the pinned ClickHouse server
// sees the exact configured recovery disk and that its mount is read-only.
// The archive marker separately binds the database ClickHouse consumes to the
// externally verified deployment set, including split-volume misconfiguration.
func ValidateClickHouseRecoveryDiskReadOnly(
	ctx context.Context,
	connection ClickHouseRecoveryDiskConnection,
) error {
	if ctx == nil || connection == nil {
		return errors.New(
			"validate ClickHouse recovery disk: context and connection are required",
		)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	var count uint64
	var path string
	var readOnly uint8
	if err := connection.QueryRow(
		ctx,
		clickHouseRecoveryDiskMetadataQuery,
		recoverycontract.Disk,
	).Scan(&count, &path, &readOnly); err != nil {
		return fmt.Errorf("validate ClickHouse recovery disk metadata: %w", err)
	}
	if count != 1 || path != clickHouseRecoveryDiskPath || readOnly != 1 {
		return fmt.Errorf(
			"%w: count=%d path=%q read_only=%d",
			ErrClickHouseRecoveryDiskWritable,
			count,
			path,
			readOnly,
		)
	}
	return nil
}

// ClickHouseMigrationLedgerIdentity canonically identifies a completely
// validated ClickHouse migration ledger without including application times or
// the database alias under which it was inspected.
type ClickHouseMigrationLedgerIdentity struct {
	LatestVersion uint32
	SHA256        [sha256.Size]byte
}

// ClickHouseRecoveryDatabaseInspection is the immutable identity and high-water
// mark captured before backup and re-inspected after restore.
type ClickHouseRecoveryDatabaseInspection struct {
	DatabaseName                    string
	ServerVersion                   string
	DatabaseEngine                  string
	DatabaseUUID                    string
	SchemaMigrationsTableUUID       string
	EventsTableUUID                 string
	RecoverySetsTableUUID           string
	RecoveryArchiveMarkersTableUUID string
	MaximumVisibilitySequence       uint64
	ActiveMutationCount             uint64
	MigrationLedger                 ClickHouseMigrationLedgerIdentity
}

// ClickHouseRecoveryReceipt is the exact durable proof written only after an
// alias-bound restore succeeds.
type ClickHouseRecoveryReceipt struct {
	RecoverySetID                   string
	DeploymentManifestSHA256        string
	DatabaseUUID                    string
	SchemaMigrationsTableUUID       string
	EventsTableUUID                 string
	RecoverySetsTableUUID           string
	RecoveryArchiveMarkersTableUUID string
	RestoredAt                      time.Time
}

// ValidateClickHouseMigrationLedger performs a complete, non-mutating ledger
// validation against the supplied embedded migrations and returns its stable
// database-alias-independent identity.
func ValidateClickHouseMigrationLedger(
	ctx context.Context,
	connection ClickHouseMigrationLedgerConnection,
	migrationFiles fs.FS,
	databaseName string,
) (ClickHouseMigrationLedgerIdentity, error) {
	return validateClickHouseMigrationLedger(
		ctx,
		connection,
		migrationFiles,
		databaseName,
		false,
	)
}

func validateClickHouseMigrationLedger(
	ctx context.Context,
	connection ClickHouseMigrationLedgerConnection,
	migrationFiles fs.FS,
	databaseName string,
	tableSetValidated bool,
) (ClickHouseMigrationLedgerIdentity, error) {
	if ctx == nil || connection == nil || migrationFiles == nil {
		return ClickHouseMigrationLedgerIdentity{}, errors.New(
			"validate ClickHouse migration ledger: context, connection, and filesystem are required",
		)
	}
	if err := ctx.Err(); err != nil {
		return ClickHouseMigrationLedgerIdentity{}, fmt.Errorf(
			"validate ClickHouse migration ledger: %w",
			err,
		)
	}
	if err := validateClickHouseRecoveryDatabaseName(databaseName); err != nil {
		return ClickHouseMigrationLedgerIdentity{}, fmt.Errorf(
			"validate ClickHouse migration ledger: %w",
			err,
		)
	}

	migrations, err := loadClickHouseMigrations(migrationFiles)
	if err != nil {
		return ClickHouseMigrationLedgerIdentity{}, err
	}
	history, err := readClickHouseMigrationHistoryForDatabase(
		ctx,
		connection,
		databaseName,
		tableSetValidated,
	)
	if err != nil {
		return ClickHouseMigrationLedgerIdentity{}, err
	}
	if err := verifyClickHouseMigrationHistory(history, migrations, true); err != nil {
		return ClickHouseMigrationLedgerIdentity{}, err
	}
	return clickHouseMigrationLedgerIdentity(history), nil
}

// InspectClickHouseRecoveryDatabase verifies all release-owned state without
// writing: pinned server version, Atomic database identity, complete migration
// ledger, exact physical schema, table UUIDs, visibility high-water mark, and
// active mutation count.
func InspectClickHouseRecoveryDatabase(
	ctx context.Context,
	connection ClickHouseRecoveryValidationConnection,
	migrationFiles fs.FS,
	databaseName string,
) (ClickHouseRecoveryDatabaseInspection, error) {
	if ctx == nil || connection == nil || migrationFiles == nil {
		return ClickHouseRecoveryDatabaseInspection{}, errors.New(
			"inspect ClickHouse recovery database: context, connection, and filesystem are required",
		)
	}
	if err := ctx.Err(); err != nil {
		return ClickHouseRecoveryDatabaseInspection{}, fmt.Errorf(
			"inspect ClickHouse recovery database: %w",
			err,
		)
	}
	if err := validateClickHouseRecoveryDatabaseName(databaseName); err != nil {
		return ClickHouseRecoveryDatabaseInspection{}, fmt.Errorf(
			"inspect ClickHouse recovery database: %w",
			err,
		)
	}
	if err := ValidateClickHouseServerVersion(ctx, connection); err != nil {
		return ClickHouseRecoveryDatabaseInspection{}, err
	}

	var databaseCount uint64
	var databaseEngine string
	var databaseUUID string
	if err := connection.QueryRow(
		ctx,
		clickHouseRecoveryDatabaseMetadataQuery,
		databaseName,
	).Scan(&databaseCount, &databaseEngine, &databaseUUID); err != nil {
		return ClickHouseRecoveryDatabaseInspection{}, fmt.Errorf(
			"inspect ClickHouse recovery database metadata: %w",
			err,
		)
	}
	if databaseCount != 1 || databaseEngine != "Atomic" {
		return ClickHouseRecoveryDatabaseInspection{}, fmt.Errorf(
			"%w: %s has count=%d engine=%q, want count=1 engine=Atomic",
			ErrClickHouseRecoveryStateInvalid,
			databaseName,
			databaseCount,
			databaseEngine,
		)
	}
	if err := validateClickHouseRecoveryUUID("database", databaseUUID); err != nil {
		return ClickHouseRecoveryDatabaseInspection{}, err
	}

	physicalSchema, err := inspectClickHousePhysicalSchemaForDatabase(
		ctx,
		connection,
		databaseName,
	)
	if err != nil {
		return ClickHouseRecoveryDatabaseInspection{}, err
	}

	migrationLedger, err := validateClickHouseMigrationLedger(
		ctx,
		connection,
		migrationFiles,
		databaseName,
		true,
	)
	if err != nil {
		return ClickHouseRecoveryDatabaseInspection{}, fmt.Errorf(
			"inspect ClickHouse recovery migration ledger: %w",
			err,
		)
	}
	if err := validateClickHouseRecoveryUUIDIdentity(
		databaseUUID,
		physicalSchema.SchemaMigrationsTableUUID,
		physicalSchema.EventsTableUUID,
		physicalSchema.RecoverySetsTableUUID,
		physicalSchema.RecoveryArchiveMarkersTableUUID,
	); err != nil {
		return ClickHouseRecoveryDatabaseInspection{}, err
	}

	var maximumVisibilitySequence uint64
	if err := connection.QueryRow(
		ctx,
		clickHouseRecoveryMaximumVisibilitySequenceQuery(databaseName),
	).Scan(&maximumVisibilitySequence); err != nil {
		return ClickHouseRecoveryDatabaseInspection{}, fmt.Errorf(
			"inspect ClickHouse recovery visibility sequence: %w",
			err,
		)
	}
	var activeMutationCount uint64
	if err := connection.QueryRow(
		ctx,
		clickHouseRecoveryActiveMutationsQuery,
		databaseName,
	).Scan(&activeMutationCount); err != nil {
		return ClickHouseRecoveryDatabaseInspection{}, fmt.Errorf(
			"inspect ClickHouse recovery mutations: %w",
			err,
		)
	}

	return ClickHouseRecoveryDatabaseInspection{
		DatabaseName:                    databaseName,
		ServerVersion:                   clickHousePrivilegeContractVersion,
		DatabaseEngine:                  databaseEngine,
		DatabaseUUID:                    databaseUUID,
		SchemaMigrationsTableUUID:       physicalSchema.SchemaMigrationsTableUUID,
		EventsTableUUID:                 physicalSchema.EventsTableUUID,
		RecoverySetsTableUUID:           physicalSchema.RecoverySetsTableUUID,
		RecoveryArchiveMarkersTableUUID: physicalSchema.RecoveryArchiveMarkersTableUUID,
		MaximumVisibilitySequence:       maximumVisibilitySequence,
		ActiveMutationCount:             activeMutationCount,
		MigrationLedger:                 migrationLedger,
	}, nil
}

// ValidateClickHouseBackupSource applies the additional backup-only invariant:
// no active mutation may remain in the canonical open_splunk source database.
func ValidateClickHouseBackupSource(
	ctx context.Context,
	connection ClickHouseRecoveryValidationConnection,
	migrationFiles fs.FS,
) (ClickHouseRecoveryDatabaseInspection, error) {
	inspection, err := InspectClickHouseRecoveryDatabase(
		ctx,
		connection,
		migrationFiles,
		recoverycontract.CanonicalDatabase,
	)
	if err != nil {
		return ClickHouseRecoveryDatabaseInspection{}, err
	}
	if inspection.ActiveMutationCount != 0 {
		return ClickHouseRecoveryDatabaseInspection{}, fmt.Errorf(
			"%w: %s has %d active mutations",
			ErrClickHouseRecoveryActiveMutations,
			recoverycontract.CanonicalDatabase,
			inspection.ActiveMutationCount,
		)
	}
	return inspection, nil
}

// WriteClickHouseRecoveryArchiveMarker requires an empty singleton table,
// writes the exact identity for the next native BACKUP, and reads it back
// before the archive can be created. Unknown stale state is never overwritten.
func WriteClickHouseRecoveryArchiveMarker(
	ctx context.Context,
	connection ClickHouseRecoveryArchiveMarkerConnection,
	databaseName string,
	recoverySetID string,
	backupOperationUUID string,
) error {
	if err := validateClickHouseRecoveryArchiveMarkerInputs(
		ctx,
		connection,
		databaseName,
		recoverySetID,
		backupOperationUUID,
	); err != nil {
		return fmt.Errorf("write ClickHouse recovery archive marker: %w", err)
	}
	if err := RequireClickHouseRecoveryArchiveMarkerAbsent(
		ctx,
		connection,
		databaseName,
	); err != nil {
		return fmt.Errorf("require empty ClickHouse recovery archive marker: %w", err)
	}
	if err := connection.Exec(
		ctx,
		clickHouseRecoveryArchiveMarkerInsertQuery(databaseName),
		uint8(1),
		recoverySetID,
		backupOperationUUID,
	); err != nil {
		return fmt.Errorf("insert ClickHouse recovery archive marker: %w", err)
	}
	if err := RequireClickHouseRecoveryArchiveMarker(
		ctx,
		connection,
		databaseName,
		recoverySetID,
		backupOperationUUID,
	); err != nil {
		return fmt.Errorf("verify written ClickHouse recovery archive marker: %w", err)
	}
	return nil
}

// RequireClickHouseRecoveryArchiveMarker proves a restored database contains
// exactly the singleton identity paired with the outer recovery manifest.
func RequireClickHouseRecoveryArchiveMarker(
	ctx context.Context,
	connection ClickHouseRecoveryArchiveMarkerConnection,
	databaseName string,
	recoverySetID string,
	backupOperationUUID string,
) error {
	if err := validateClickHouseRecoveryArchiveMarkerInputs(
		ctx,
		connection,
		databaseName,
		recoverySetID,
		backupOperationUUID,
	); err != nil {
		return fmt.Errorf("require ClickHouse recovery archive marker: %w", err)
	}
	rowCount, slot, actualRecoverySetID, actualOperationUUID, err :=
		readClickHouseRecoveryArchiveMarker(ctx, connection, databaseName)
	if err != nil {
		return err
	}
	if rowCount != 1 || slot != 1 || actualRecoverySetID != recoverySetID ||
		actualOperationUUID != backupOperationUUID {
		return fmt.Errorf(
			"%w: %s has row_count=%d and no exact expected singleton",
			ErrClickHouseRecoveryArchiveMarkerMismatch,
			databaseName,
			rowCount,
		)
	}
	return nil
}

// ClearClickHouseRecoveryArchiveMarker synchronously removes only the exact
// expected operational marker and proves absence. An already-absent marker is
// an idempotent success; an unknown marker fails without mutation.
func ClearClickHouseRecoveryArchiveMarker(
	ctx context.Context,
	connection ClickHouseRecoveryArchiveMarkerConnection,
	databaseName string,
	recoverySetID string,
	backupOperationUUID string,
) error {
	if err := validateClickHouseRecoveryArchiveMarkerInputs(
		ctx,
		connection,
		databaseName,
		recoverySetID,
		backupOperationUUID,
	); err != nil {
		return fmt.Errorf("clear ClickHouse recovery archive marker: %w", err)
	}
	rowCount, slot, actualRecoverySetID, actualOperationUUID, err :=
		readClickHouseRecoveryArchiveMarker(ctx, connection, databaseName)
	if err != nil {
		return err
	}
	if rowCount == 0 {
		return nil
	}
	if rowCount != 1 || slot != 1 || actualRecoverySetID != recoverySetID ||
		actualOperationUUID != backupOperationUUID {
		return fmt.Errorf(
			"%w: %s marker differs from the expected cleanup identity",
			ErrClickHouseRecoveryArchiveMarkerMismatch,
			databaseName,
		)
	}
	if err := connection.Exec(
		ctx,
		clickHouseRecoveryArchiveMarkerTruncateQuery(databaseName),
	); err != nil {
		return fmt.Errorf("truncate ClickHouse recovery archive marker: %w", err)
	}
	if err := RequireClickHouseRecoveryArchiveMarkerAbsent(
		ctx,
		connection,
		databaseName,
	); err != nil {
		return fmt.Errorf("verify cleared ClickHouse recovery archive marker: %w", err)
	}
	return nil
}

// RequireClickHouseRecoveryArchiveMarkerAbsent rejects a receipted staging or
// promoted database if the archive-only marker was not consumed first.
func RequireClickHouseRecoveryArchiveMarkerAbsent(
	ctx context.Context,
	connection ClickHouseRecoveryArchiveMarkerConnection,
	databaseName string,
) error {
	if err := validateClickHouseRecoveryArchiveMarkerConnection(
		ctx,
		connection,
		databaseName,
	); err != nil {
		return fmt.Errorf("require absent ClickHouse recovery archive marker: %w", err)
	}
	rowCount, _, _, _, err := readClickHouseRecoveryArchiveMarker(
		ctx,
		connection,
		databaseName,
	)
	if err != nil {
		return err
	}
	if rowCount != 0 {
		return fmt.Errorf(
			"%w: %s retains %d marker rows after restore consumption",
			ErrClickHouseRecoveryArchiveMarkerMismatch,
			databaseName,
			rowCount,
		)
	}
	return nil
}

// ReadClickHouseRecoveryReceipt admits a retry only when recovery_sets has one
// exact singleton row for the expected recovery set and outer manifest.
func ReadClickHouseRecoveryReceipt(
	ctx context.Context,
	connection ClickHouseRecoveryReceiptConnection,
	databaseName string,
	recoverySetID string,
	deploymentManifestSHA256 string,
) (ClickHouseRecoveryReceipt, error) {
	if err := validateClickHouseRecoveryReceiptInputs(
		ctx,
		connection,
		databaseName,
		recoverySetID,
		deploymentManifestSHA256,
	); err != nil {
		return ClickHouseRecoveryReceipt{}, fmt.Errorf(
			"read ClickHouse recovery receipt: %w",
			err,
		)
	}

	var rowCount uint64
	var slot uint8
	var actualRecoverySetID string
	var actualManifestSHA256 string
	var databaseUUID string
	var schemaMigrationsTableUUID string
	var eventsTableUUID string
	var recoverySetsTableUUID string
	var recoveryArchiveMarkersTableUUID string
	var restoredAt time.Time
	if err := connection.QueryRow(
		ctx,
		clickHouseRecoveryReceiptReadQuery(databaseName),
	).Scan(
		&rowCount,
		&slot,
		&actualRecoverySetID,
		&actualManifestSHA256,
		&databaseUUID,
		&schemaMigrationsTableUUID,
		&eventsTableUUID,
		&recoverySetsTableUUID,
		&recoveryArchiveMarkersTableUUID,
		&restoredAt,
	); err != nil {
		return ClickHouseRecoveryReceipt{}, fmt.Errorf(
			"read ClickHouse recovery receipt row: %w",
			err,
		)
	}
	canonicalRestoredAt := restoredAt.UTC().Truncate(time.Millisecond)
	if rowCount != 1 || slot != 1 ||
		actualRecoverySetID != recoverySetID ||
		actualManifestSHA256 != deploymentManifestSHA256 ||
		restoredAt.IsZero() || !restoredAt.Equal(canonicalRestoredAt) {
		return ClickHouseRecoveryReceipt{}, fmt.Errorf(
			"%w: %s has row_count=%d and no exact expected singleton",
			ErrClickHouseRecoveryReceiptMismatch,
			databaseName,
			rowCount,
		)
	}
	if err := validateClickHouseRecoveryUUIDIdentity(
		databaseUUID,
		schemaMigrationsTableUUID,
		eventsTableUUID,
		recoverySetsTableUUID,
		recoveryArchiveMarkersTableUUID,
	); err != nil {
		return ClickHouseRecoveryReceipt{}, fmt.Errorf(
			"%w: %s receipt UUID identity is invalid: %w",
			ErrClickHouseRecoveryReceiptMismatch,
			databaseName,
			err,
		)
	}
	return ClickHouseRecoveryReceipt{
		RecoverySetID:                   recoverySetID,
		DeploymentManifestSHA256:        deploymentManifestSHA256,
		DatabaseUUID:                    databaseUUID,
		SchemaMigrationsTableUUID:       schemaMigrationsTableUUID,
		EventsTableUUID:                 eventsTableUUID,
		RecoverySetsTableUUID:           recoverySetsTableUUID,
		RecoveryArchiveMarkersTableUUID: recoveryArchiveMarkersTableUUID,
		RestoredAt:                      canonicalRestoredAt,
	}, nil
}

// WriteClickHouseRecoveryReceipt synchronously replaces any archived prior
// receipt only after a successful restore, then reads it back and requires one
// exact row. Cancellation or any partial failure leaves no admissible receipt.
func WriteClickHouseRecoveryReceipt(
	ctx context.Context,
	connection ClickHouseRecoveryReceiptConnection,
	databaseName string,
	receipt ClickHouseRecoveryReceipt,
) (ClickHouseRecoveryReceipt, error) {
	if err := validateClickHouseRecoveryReceiptInputs(
		ctx,
		connection,
		databaseName,
		receipt.RecoverySetID,
		receipt.DeploymentManifestSHA256,
	); err != nil {
		return ClickHouseRecoveryReceipt{}, fmt.Errorf(
			"write ClickHouse recovery receipt: %w",
			err,
		)
	}
	if receipt.RestoredAt.IsZero() {
		return ClickHouseRecoveryReceipt{}, errors.New(
			"write ClickHouse recovery receipt: restored time is required",
		)
	}
	if err := validateClickHouseRecoveryUUIDIdentity(
		receipt.DatabaseUUID,
		receipt.SchemaMigrationsTableUUID,
		receipt.EventsTableUUID,
		receipt.RecoverySetsTableUUID,
		receipt.RecoveryArchiveMarkersTableUUID,
	); err != nil {
		return ClickHouseRecoveryReceipt{}, fmt.Errorf(
			"write ClickHouse recovery receipt: %w",
			err,
		)
	}
	canonicalRestoredAt := receipt.RestoredAt.UTC().Truncate(time.Millisecond)
	if err := connection.Exec(
		ctx,
		clickHouseRecoveryReceiptTruncateQuery(databaseName),
	); err != nil {
		return ClickHouseRecoveryReceipt{}, fmt.Errorf(
			"truncate ClickHouse recovery receipt: %w",
			err,
		)
	}
	if err := ctx.Err(); err != nil {
		return ClickHouseRecoveryReceipt{}, fmt.Errorf(
			"write ClickHouse recovery receipt after truncate: %w",
			err,
		)
	}
	if err := connection.Exec(
		ctx,
		clickHouseRecoveryReceiptInsertQuery(databaseName),
		uint8(1),
		receipt.RecoverySetID,
		receipt.DeploymentManifestSHA256,
		receipt.DatabaseUUID,
		receipt.SchemaMigrationsTableUUID,
		receipt.EventsTableUUID,
		receipt.RecoverySetsTableUUID,
		receipt.RecoveryArchiveMarkersTableUUID,
		canonicalRestoredAt.UnixMilli(),
	); err != nil {
		return ClickHouseRecoveryReceipt{}, fmt.Errorf(
			"insert ClickHouse recovery receipt: %w",
			err,
		)
	}
	expected := receipt
	expected.RestoredAt = canonicalRestoredAt
	actual, err := ReadClickHouseRecoveryReceipt(
		ctx,
		connection,
		databaseName,
		expected.RecoverySetID,
		expected.DeploymentManifestSHA256,
	)
	if err != nil {
		return ClickHouseRecoveryReceipt{}, fmt.Errorf(
			"verify written ClickHouse recovery receipt: %w",
			err,
		)
	}
	if actual.RecoverySetID != expected.RecoverySetID ||
		actual.DeploymentManifestSHA256 != expected.DeploymentManifestSHA256 ||
		actual.DatabaseUUID != expected.DatabaseUUID ||
		actual.SchemaMigrationsTableUUID != expected.SchemaMigrationsTableUUID ||
		actual.EventsTableUUID != expected.EventsTableUUID ||
		actual.RecoverySetsTableUUID != expected.RecoverySetsTableUUID ||
		actual.RecoveryArchiveMarkersTableUUID != expected.RecoveryArchiveMarkersTableUUID ||
		!actual.RestoredAt.Equal(expected.RestoredAt) {
		return ClickHouseRecoveryReceipt{}, fmt.Errorf(
			"%w: receipt identity differs after write",
			ErrClickHouseRecoveryReceiptMismatch,
		)
	}
	return actual, nil
}

func clickHouseMigrationLedgerIdentity(
	history []clickHouseMigrationLedgerRow,
) ClickHouseMigrationLedgerIdentity {
	hasher := sha256.New()
	_, _ = hasher.Write([]byte(clickHouseMigrationLedgerDigestDomain))
	for _, row := range history {
		_, _ = hasher.Write([]byte(strconv.FormatUint(uint64(row.Version), 10)))
		_, _ = hasher.Write([]byte{0})
		_, _ = hasher.Write([]byte(strconv.Itoa(len(row.Name))))
		_, _ = hasher.Write([]byte{0})
		_, _ = hasher.Write([]byte(row.Name))
	}
	var digest [sha256.Size]byte
	copy(digest[:], hasher.Sum(nil))
	return ClickHouseMigrationLedgerIdentity{
		LatestVersion: history[len(history)-1].Version,
		SHA256:        digest,
	}
}

func validateClickHouseRecoveryDatabaseName(databaseName string) error {
	if !recoverycontract.ValidDatabaseName(databaseName) {
		return fmt.Errorf("ClickHouse recovery database name %q is invalid", databaseName)
	}
	return nil
}

func validateClickHouseRecoveryUUID(name, value string) error {
	parsed, err := uuid.Parse(value)
	if err != nil || parsed == uuid.Nil || parsed.String() != value {
		return fmt.Errorf(
			"%w: %s UUID %q is not canonical and nonzero",
			ErrClickHouseRecoveryStateInvalid,
			name,
			value,
		)
	}
	return nil
}

func validateClickHouseRecoveryUUIDIdentity(
	databaseUUID string,
	schemaMigrationsTableUUID string,
	eventsTableUUID string,
	recoverySetsTableUUID string,
	recoveryArchiveMarkersTableUUID string,
) error {
	return validateClickHouseRecoveryUUIDCandidates([]clickHouseRecoveryUUIDCandidate{
		{name: "database", value: databaseUUID},
		{name: "schema_migrations", value: schemaMigrationsTableUUID},
		{name: "events", value: eventsTableUUID},
		{name: "recovery_sets", value: recoverySetsTableUUID},
		{name: "recovery_archive_markers", value: recoveryArchiveMarkersTableUUID},
	})
}

func validateClickHouseRecoveryTableUUIDIdentity(
	schemaMigrationsTableUUID string,
	eventsTableUUID string,
	recoverySetsTableUUID string,
	recoveryArchiveMarkersTableUUID string,
) error {
	return validateClickHouseRecoveryUUIDCandidates([]clickHouseRecoveryUUIDCandidate{
		{name: "schema_migrations", value: schemaMigrationsTableUUID},
		{name: "events", value: eventsTableUUID},
		{name: "recovery_sets", value: recoverySetsTableUUID},
		{name: "recovery_archive_markers", value: recoveryArchiveMarkersTableUUID},
	})
}

type clickHouseRecoveryUUIDCandidate struct {
	name  string
	value string
}

func validateClickHouseRecoveryUUIDCandidates(
	values []clickHouseRecoveryUUIDCandidate,
) error {
	seen := make(map[string]struct{}, len(values))
	for _, candidate := range values {
		if err := validateClickHouseRecoveryUUID(candidate.name, candidate.value); err != nil {
			return err
		}
		if _, exists := seen[candidate.value]; exists {
			return fmt.Errorf(
				"%w: %s UUID %s is not unique",
				ErrClickHouseRecoveryStateInvalid,
				candidate.name,
				candidate.value,
			)
		}
		seen[candidate.value] = struct{}{}
	}
	return nil
}

func validateClickHouseRecoveryReceiptInputs(
	ctx context.Context,
	connection ClickHouseRecoveryReceiptConnection,
	databaseName string,
	recoverySetID string,
	deploymentManifestSHA256 string,
) error {
	if ctx == nil || connection == nil {
		return errors.New("context and connection are required")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := validateClickHouseRecoveryDatabaseName(databaseName); err != nil {
		return err
	}
	if !recoverycontract.ValidRecoverySetID(recoverySetID) {
		return errors.New("recovery set ID must be exactly 32 lowercase hexadecimal characters")
	}
	if !buildinfo.ValidSHA256(deploymentManifestSHA256) {
		return errors.New("deployment manifest SHA-256 must be exactly 64 lowercase hexadecimal characters")
	}
	return nil
}

func validateClickHouseRecoveryArchiveMarkerConnection(
	ctx context.Context,
	connection ClickHouseRecoveryArchiveMarkerConnection,
	databaseName string,
) error {
	if ctx == nil || connection == nil {
		return errors.New("context and connection are required")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return validateClickHouseRecoveryDatabaseName(databaseName)
}

func validateClickHouseRecoveryArchiveMarkerInputs(
	ctx context.Context,
	connection ClickHouseRecoveryArchiveMarkerConnection,
	databaseName string,
	recoverySetID string,
	backupOperationUUID string,
) error {
	if err := validateClickHouseRecoveryArchiveMarkerConnection(
		ctx,
		connection,
		databaseName,
	); err != nil {
		return err
	}
	if !recoverycontract.ValidRecoverySetID(recoverySetID) {
		return errors.New("recovery set ID must be exactly 32 lowercase hexadecimal characters")
	}
	if err := validateClickHouseRecoveryUUID("backup operation", backupOperationUUID); err != nil {
		return err
	}
	return nil
}

func readClickHouseRecoveryArchiveMarker(
	ctx context.Context,
	connection ClickHouseRecoveryArchiveMarkerConnection,
	databaseName string,
) (rowCount uint64, slot uint8, recoverySetID string, operationUUID string, err error) {
	if scanErr := connection.QueryRow(
		ctx,
		clickHouseRecoveryArchiveMarkerReadQuery(databaseName),
	).Scan(
		&rowCount,
		&slot,
		&recoverySetID,
		&operationUUID,
	); scanErr != nil {
		err = fmt.Errorf("read ClickHouse recovery archive marker row: %w", scanErr)
	}
	return
}

func clickHouseRecoveryMaximumVisibilitySequenceQuery(databaseName string) string {
	return fmt.Sprintf("SELECT max(visibility_seq) FROM %s.events", databaseName)
}

func clickHouseRecoveryArchiveMarkerTruncateQuery(databaseName string) string {
	return fmt.Sprintf("TRUNCATE TABLE %s.recovery_archive_markers SYNC", databaseName)
}

func clickHouseRecoveryArchiveMarkerInsertQuery(databaseName string) string {
	return fmt.Sprintf(
		"INSERT INTO %s.recovery_archive_markers "+
			"(slot, recovery_set_id, backup_operation_uuid) "+
			"SETTINGS async_insert = 0 VALUES (?, ?, ?)",
		databaseName,
	)
}

func clickHouseRecoveryArchiveMarkerReadQuery(databaseName string) string {
	return fmt.Sprintf(`
		SELECT
			count(),
			any(slot),
			toString(any(recovery_set_id)),
			toString(any(backup_operation_uuid))
		FROM
		(
			SELECT slot, recovery_set_id, backup_operation_uuid
			FROM %s.recovery_archive_markers
			LIMIT %d
		)
		SETTINGS
			max_rows_to_read = %d,
			max_bytes_to_read = %d,
			read_overflow_mode = 'throw',
			max_result_rows = 1,
			max_result_bytes = %d,
			result_overflow_mode = 'throw'`,
		databaseName,
		clickHouseRecoverySingletonRowSentinel,
		clickHouseRecoverySingletonRowSentinel,
		clickHouseRecoverySingletonByteLimit,
		clickHouseRecoverySingletonByteLimit,
	)
}

func clickHouseRecoveryReceiptTruncateQuery(databaseName string) string {
	return fmt.Sprintf("TRUNCATE TABLE %s.recovery_sets SYNC", databaseName)
}

func clickHouseRecoveryReceiptInsertQuery(databaseName string) string {
	return fmt.Sprintf(
		"INSERT INTO %s.recovery_sets "+
			"(slot, recovery_set_id, deployment_manifest_sha256, database_uuid, "+
			"schema_migrations_table_uuid, events_table_uuid, recovery_sets_table_uuid, "+
			"recovery_archive_markers_table_uuid, restored_at) "+
			"SETTINGS async_insert = 0 VALUES (?, ?, ?, ?, ?, ?, ?, ?, fromUnixTimestamp64Milli(?))",
		databaseName,
	)
}

func clickHouseRecoveryReceiptReadQuery(databaseName string) string {
	return fmt.Sprintf(`
		SELECT
			count(),
			any(slot),
			toString(any(recovery_set_id)),
			toString(any(deployment_manifest_sha256)),
			toString(any(database_uuid)),
			toString(any(schema_migrations_table_uuid)),
			toString(any(events_table_uuid)),
			toString(any(recovery_sets_table_uuid)),
			toString(any(recovery_archive_markers_table_uuid)),
			any(restored_at)
		FROM
		(
			SELECT
				slot,
				recovery_set_id,
				deployment_manifest_sha256,
				database_uuid,
				schema_migrations_table_uuid,
				events_table_uuid,
				recovery_sets_table_uuid,
				recovery_archive_markers_table_uuid,
				restored_at
			FROM %s.recovery_sets
			LIMIT %d
		)
		SETTINGS
			max_rows_to_read = %d,
			max_bytes_to_read = %d,
			read_overflow_mode = 'throw',
			max_result_rows = 1,
			max_result_bytes = %d,
			result_overflow_mode = 'throw'`,
		databaseName,
		clickHouseRecoverySingletonRowSentinel,
		clickHouseRecoverySingletonRowSentinel,
		clickHouseRecoverySingletonByteLimit,
		clickHouseRecoverySingletonByteLimit,
	)
}
