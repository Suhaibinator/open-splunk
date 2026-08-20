// Package recoveryset owns the versioned outer deployment recovery-set
// manifest. The existing controlbackup v1 bundle remains an independently
// verifiable child; ClickHouse's native archive remains on its separately
// mounted recovery disk and is bound here by exact identity.
package recoveryset

import (
	"errors"
	"fmt"

	"github.com/Suhaibinator/open-splunk/internal/buildinfo"
	"github.com/Suhaibinator/open-splunk/internal/controlbackup"
	"github.com/Suhaibinator/open-splunk/internal/recoverycontract"
	"github.com/google/uuid"
)

const (
	manifestFormatVersion           = uint32(1)
	maximumManifestBytes            = 32 << 10
	manifestSubject                 = "deployment recovery-set manifest"
	maximumControlManifestBytes     = uint64(16 << 10)
	maximumClickHouseArchiveBytes   = uint64(1 << 50)
	maximumTimestampUnixMicro       = int64(253_402_300_799_999_999)
	deploymentRecoverySetScope      = "deployment-recovery-set"
	manifestFilename                = "manifest.json"
	controlPlaneDirectory           = "control-plane"
	controlPlaneManifestName        = "control-plane/manifest.json"
	clickHouseServerVersion         = "26.7.3.19"
	clickHouseRecoveryDisk          = recoverycontract.Disk
	clickHouseDatabase              = recoverycontract.CanonicalDatabase
	clickHouseArchiveDatabasePrefix = recoverycontract.ArchiveDatabasePrefix
	clickHouseArchiveFormat         = "clickhouse-native-tar-zstd"
	clickHouseArchiveSuffix         = recoverycontract.ArchiveSuffix
)

// FileIdentity binds one fixed member name to its exact closed-file bytes.
type FileIdentity = controlbackup.FileIdentity

// ControlPlaneIdentity binds the unchanged controlbackup v1 child through its
// canonical manifest. Verifying the child manifest recursively verifies its
// SQLite snapshot, master key, and administrator token.
type ControlPlaneIdentity struct {
	Directory string       `json:"directory"`
	Manifest  FileIdentity `json:"manifest"`
}

// ClickHouseIdentity records the exact release-owned native backup state.
// Credentials, paths, and raw data-derived secrets are deliberately absent.
type ClickHouseIdentity struct {
	ServerVersion                   string       `json:"server_version"`
	Disk                            string       `json:"disk"`
	Database                        string       `json:"database"`
	ArchiveDatabase                 string       `json:"archive_database"`
	ArchiveFormat                   string       `json:"archive_format"`
	Archive                         FileIdentity `json:"archive"`
	MigrationLedgerSHA256           string       `json:"migration_ledger_sha256"`
	DatabaseUUID                    string       `json:"database_uuid"`
	SchemaMigrationsTableUUID       string       `json:"schema_migrations_table_uuid"`
	EventsTableUUID                 string       `json:"events_table_uuid"`
	RecoverySetsTableUUID           string       `json:"recovery_sets_table_uuid"`
	RecoveryArchiveMarkersTableUUID string       `json:"recovery_archive_markers_table_uuid"`
	MaxVisibilitySeq                uint64       `json:"max_visibility_seq"`
	BackupOperationUUID             string       `json:"backup_operation_uuid"`
}

// Manifest is the canonical outer deployment recovery-set manifest.
type Manifest struct {
	FormatVersion        uint32                          `json:"format_version"`
	CreatedAtUnixMicro   int64                           `json:"created_at_unix_micro"`
	RecoverySetID        string                          `json:"recovery_set_id"`
	Scope                string                          `json:"scope"`
	ClickHouseIncluded   bool                            `json:"clickhouse_included"`
	SourceRevision       string                          `json:"source_revision"`
	SQLiteMigrations     controlbackup.MigrationIdentity `json:"sqlite_migrations"`
	ClickHouseMigrations controlbackup.MigrationIdentity `json:"clickhouse_migrations"`
	ControlPlane         ControlPlaneIdentity            `json:"control_plane"`
	ClickHouse           ClickHouseIdentity              `json:"clickhouse"`
}

func marshalManifest(manifest Manifest) ([]byte, error) {
	return controlbackup.MarshalCanonicalJSON(manifest, manifestSubject, maximumManifestBytes, validateManifest)
}

func unmarshalManifest(encoded []byte) (Manifest, error) {
	return controlbackup.UnmarshalCanonicalJSON(encoded, manifestSubject, maximumManifestBytes, validateManifest)
}

func validateManifest(manifest Manifest) error {
	if manifest.FormatVersion != manifestFormatVersion {
		return fmt.Errorf(
			"deployment recovery-set manifest format version %d is unsupported; create a fresh recovery set",
			manifest.FormatVersion,
		)
	}
	if manifest.CreatedAtUnixMicro <= 0 ||
		manifest.CreatedAtUnixMicro > maximumTimestampUnixMicro {
		return errors.New("deployment recovery-set manifest creation time is invalid")
	}
	if !recoverycontract.ValidRecoverySetID(manifest.RecoverySetID) {
		return errors.New("deployment recovery-set manifest recovery-set ID is invalid")
	}
	if manifest.Scope != deploymentRecoverySetScope || !manifest.ClickHouseIncluded {
		return errors.New("deployment recovery-set manifest scope is invalid")
	}
	if err := validateReleaseIdentity(manifest.releaseIdentity()); err != nil {
		return err
	}
	if manifest.ControlPlane.Directory != controlPlaneDirectory {
		return fmt.Errorf(
			"deployment recovery-set control-plane directory must be exactly %q",
			controlPlaneDirectory,
		)
	}
	if err := controlbackup.ValidateFileIdentity(
		manifest.ControlPlane.Manifest,
		controlPlaneManifestName,
		1,
		maximumControlManifestBytes,
	); err != nil {
		return fmt.Errorf("deployment recovery-set control-plane manifest: %w", err)
	}
	if err := validateClickHouseIdentity(
		manifest.RecoverySetID,
		manifest.ClickHouse,
	); err != nil {
		return err
	}
	return nil
}

func validateClickHouseIdentity(
	recoverySetID string,
	identity ClickHouseIdentity,
) error {
	if identity.ServerVersion != clickHouseServerVersion {
		return fmt.Errorf(
			"deployment recovery-set ClickHouse server version must be exactly %q",
			clickHouseServerVersion,
		)
	}
	if identity.Disk != clickHouseRecoveryDisk || identity.Database != clickHouseDatabase {
		return errors.New("deployment recovery-set ClickHouse storage identity is invalid")
	}
	if identity.ArchiveDatabase != clickHouseArchiveDatabase(recoverySetID) {
		return errors.New("deployment recovery-set ClickHouse archive database is invalid")
	}
	if identity.ArchiveFormat != clickHouseArchiveFormat {
		return errors.New("deployment recovery-set ClickHouse archive format is invalid")
	}
	if err := controlbackup.ValidateFileIdentity(
		identity.Archive,
		clickHouseArchiveName(recoverySetID),
		1,
		maximumClickHouseArchiveBytes,
	); err != nil {
		return fmt.Errorf("deployment recovery-set ClickHouse archive: %w", err)
	}
	if !buildinfo.ValidSHA256(identity.MigrationLedgerSHA256) {
		return errors.New("deployment recovery-set ClickHouse migration-ledger identity is invalid")
	}
	uuidFields := []struct {
		name  string
		value string
	}{
		{name: "database", value: identity.DatabaseUUID},
		{name: "schema_migrations table", value: identity.SchemaMigrationsTableUUID},
		{name: "events table", value: identity.EventsTableUUID},
		{name: "recovery_sets table", value: identity.RecoverySetsTableUUID},
		{name: "recovery_archive_markers table", value: identity.RecoveryArchiveMarkersTableUUID},
		{name: "backup operation", value: identity.BackupOperationUUID},
	}
	seen := make(map[uuid.UUID]string, len(uuidFields))
	for _, field := range uuidFields {
		parsed, err := uuid.Parse(field.value)
		if err != nil || parsed == uuid.Nil || parsed.String() != field.value {
			return fmt.Errorf(
				"deployment recovery-set ClickHouse %s UUID is invalid",
				field.name,
			)
		}
		if previous, exists := seen[parsed]; exists {
			return fmt.Errorf(
				"deployment recovery-set ClickHouse %s UUID duplicates %s UUID",
				field.name,
				previous,
			)
		}
		seen[parsed] = field.name
	}
	return nil
}

func validateReleaseIdentity(identity controlbackup.ReleaseIdentity) error {
	if err := controlbackup.ValidateReleaseIdentity(identity); err != nil {
		return fmt.Errorf("deployment recovery-set %w", err)
	}
	return nil
}

func validateManifestRelease(
	manifest Manifest,
	expected controlbackup.ReleaseIdentity,
) error {
	if err := validateReleaseIdentity(expected); err != nil {
		return err
	}
	actual := manifest.releaseIdentity()
	if actual != expected {
		return errors.New(
			"deployment recovery set was created by a different source or migration identity",
		)
	}
	return nil
}

func (manifest Manifest) releaseIdentity() controlbackup.ReleaseIdentity {
	return controlbackup.ReleaseIdentity{
		SourceRevision:       manifest.SourceRevision,
		SQLiteMigrations:     manifest.SQLiteMigrations,
		ClickHouseMigrations: manifest.ClickHouseMigrations,
	}
}

func clickHouseArchiveDatabase(recoverySetID string) string {
	return clickHouseArchiveDatabasePrefix + recoverySetID
}

func clickHouseArchiveName(recoverySetID string) string {
	return recoverySetID + clickHouseArchiveSuffix
}
