// Package controlbackup owns the versioned, verifiable control-plane recovery
// bundle. It deliberately excludes ClickHouse event data and export artifacts.
package controlbackup

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"github.com/Suhaibinator/open-splunk/internal/auth"
	"github.com/Suhaibinator/open-splunk/internal/buildinfo"
)

const (
	manifestFormatVersion          = uint32(1)
	maximumManifestBytes           = 16 << 10
	maximumDatabaseBytes           = uint64(1 << 40)
	maximumTimestampUnixMicro      = int64(253_402_300_799_999_999)
	recoverySetIDBytes             = 16
	recoverySetIDHexBytes          = recoverySetIDBytes * 2
	serverMasterKeyBytes           = uint64(auth.ServerMasterKeyBytes)
	minimumAdministratorTokenBytes = uint64(auth.MinimumBrowserBearerTokenBytes)
	maximumAdministratorTokenBytes = uint64(auth.MaximumBrowserBearerTokenBytes)

	controlPlaneOnlyScope      = "control-plane-only"
	databaseFilename           = "control.sqlite"
	masterKeyFilename          = "master.key"
	administratorTokenFilename = "administrator.token"
	manifestFilename           = "manifest.json"
)

// MigrationIdentity binds a contiguous migration ledger to the exact source
// corpus shipped by one release.
type MigrationIdentity struct {
	SHA256        string `json:"sha256"`
	LatestVersion uint32 `json:"latest_version"`
}

// FileIdentity describes one fixed bundle member. Paths are never accepted in
// the manifest; names are exact format constants.
type FileIdentity struct {
	Name      string `json:"name"`
	SizeBytes uint64 `json:"size_bytes"`
	SHA256    string `json:"sha256"`
}

// ReleaseIdentity is the compatibility boundary required to verify or restore
// a bundle. V1 intentionally requires the same release, then normal startup
// may perform a separately controlled upgrade.
type ReleaseIdentity struct {
	ApplicationVersion   string
	SourceRevision       string
	SQLiteMigrations     MigrationIdentity
	ClickHouseMigrations MigrationIdentity
}

// Manifest is the canonical recovery-bundle manifest. ClickHouseIncluded is
// required to be false: a control-plane bundle is not a deployment backup.
type Manifest struct {
	FormatVersion               uint32            `json:"format_version"`
	CreatedAtUnixMicro          int64             `json:"created_at_unix_micro"`
	RecoverySetID               string            `json:"recovery_set_id"`
	Scope                       string            `json:"scope"`
	ClickHouseIncluded          bool              `json:"clickhouse_included"`
	ApplicationVersion          string            `json:"application_version"`
	SourceRevision              string            `json:"source_revision"`
	SQLiteMigrations            MigrationIdentity `json:"sqlite_migrations"`
	SQLiteMigrationLedgerSHA256 string            `json:"sqlite_migration_ledger_sha256"`
	ClickHouseMigrations        MigrationIdentity `json:"clickhouse_migrations"`
	Database                    FileIdentity      `json:"database"`
	MasterKey                   FileIdentity      `json:"master_key"`
	AdministratorToken          FileIdentity      `json:"administrator_token"`
	MasterKeyFingerprintSHA256  string            `json:"master_key_fingerprint_sha256"`
}

// ReleaseIdentity returns the exact release compatibility fields recorded by
// the manifest.
func (manifest Manifest) ReleaseIdentity() ReleaseIdentity {
	return ReleaseIdentity{
		ApplicationVersion:   manifest.ApplicationVersion,
		SourceRevision:       manifest.SourceRevision,
		SQLiteMigrations:     manifest.SQLiteMigrations,
		ClickHouseMigrations: manifest.ClickHouseMigrations,
	}
}

func marshalManifest(manifest Manifest) ([]byte, error) {
	if err := validateManifest(manifest); err != nil {
		return nil, err
	}
	var output bytes.Buffer
	encoder := json.NewEncoder(&output)
	encoder.SetIndent("", "  ")
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(manifest); err != nil {
		return nil, fmt.Errorf("encode control-plane backup manifest: %w", err)
	}
	if output.Len() > maximumManifestBytes {
		return nil, errors.New("control-plane backup manifest exceeds its size limit")
	}
	return output.Bytes(), nil
}

func unmarshalManifest(encoded []byte) (Manifest, error) {
	if len(encoded) == 0 || len(encoded) > maximumManifestBytes {
		return Manifest{}, errors.New("control-plane backup manifest has an invalid size")
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.DisallowUnknownFields()
	var manifest Manifest
	if err := decoder.Decode(&manifest); err != nil {
		return Manifest{}, fmt.Errorf("decode control-plane backup manifest: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return Manifest{}, errors.New("control-plane backup manifest contains multiple JSON values")
		}
		return Manifest{}, fmt.Errorf("decode control-plane backup manifest terminator: %w", err)
	}
	if err := validateManifest(manifest); err != nil {
		return Manifest{}, err
	}
	canonical, err := marshalManifest(manifest)
	if err != nil {
		return Manifest{}, err
	}
	if !bytes.Equal(encoded, canonical) {
		return Manifest{}, errors.New("control-plane backup manifest is not canonical")
	}
	return manifest, nil
}

func validateManifest(manifest Manifest) error {
	if manifest.FormatVersion != manifestFormatVersion {
		return fmt.Errorf("control-plane backup manifest format version %d is unsupported", manifest.FormatVersion)
	}
	if manifest.CreatedAtUnixMicro <= 0 || manifest.CreatedAtUnixMicro > maximumTimestampUnixMicro {
		return errors.New("control-plane backup manifest creation time is invalid")
	}
	if !validLowerHex(manifest.RecoverySetID, recoverySetIDBytes) {
		return errors.New("control-plane backup manifest recovery-set ID is invalid")
	}
	if manifest.Scope != controlPlaneOnlyScope || manifest.ClickHouseIncluded {
		return errors.New("control-plane backup manifest scope is invalid")
	}
	if _, err := buildinfo.Parse(manifest.ApplicationVersion, manifest.SourceRevision); err != nil {
		return fmt.Errorf("control-plane backup manifest release identity is invalid: %w", err)
	}
	if err := validateMigrationIdentity("SQLite", manifest.SQLiteMigrations); err != nil {
		return err
	}
	if !buildinfo.ValidSHA256(manifest.SQLiteMigrationLedgerSHA256) {
		return errors.New("control-plane backup SQLite migration-ledger identity is invalid")
	}
	if err := validateMigrationIdentity("ClickHouse", manifest.ClickHouseMigrations); err != nil {
		return err
	}
	if err := validateFileIdentity(manifest.Database, databaseFilename, 1, maximumDatabaseBytes); err != nil {
		return fmt.Errorf("control-plane backup database: %w", err)
	}
	if err := validateFileIdentity(manifest.MasterKey, masterKeyFilename, serverMasterKeyBytes, serverMasterKeyBytes); err != nil {
		return fmt.Errorf("control-plane backup master key: %w", err)
	}
	if err := validateFileIdentity(
		manifest.AdministratorToken,
		administratorTokenFilename,
		minimumAdministratorTokenBytes,
		maximumAdministratorTokenBytes,
	); err != nil {
		return fmt.Errorf("control-plane backup administrator token: %w", err)
	}
	if !buildinfo.ValidSHA256(manifest.MasterKeyFingerprintSHA256) {
		return errors.New("control-plane backup manifest master-key identity is invalid")
	}
	return nil
}

func validateManifestRelease(manifest Manifest, expected ReleaseIdentity) error {
	if err := validateReleaseIdentity(expected); err != nil {
		return err
	}
	if manifest.ReleaseIdentity() != expected {
		return errors.New("control-plane backup was created by a different release; restore it with the original release first")
	}
	return nil
}

// ValidateReleaseIdentity validates the release compatibility boundary without
// attaching an operation-specific error prefix, so deployment recovery sets
// can reuse the exact same rules.
func ValidateReleaseIdentity(identity ReleaseIdentity) error {
	if _, err := buildinfo.Parse(identity.ApplicationVersion, identity.SourceRevision); err != nil {
		return fmt.Errorf("release identity is invalid: %w", err)
	}
	if err := validateReleaseMigrationIdentity("SQLite", identity.SQLiteMigrations); err != nil {
		return err
	}
	return validateReleaseMigrationIdentity("ClickHouse", identity.ClickHouseMigrations)
}

func validateReleaseIdentity(identity ReleaseIdentity) error {
	if err := ValidateReleaseIdentity(identity); err != nil {
		return fmt.Errorf("control-plane backup %w", err)
	}
	return nil
}

func validateMigrationIdentity(name string, identity MigrationIdentity) error {
	if err := validateReleaseMigrationIdentity(name, identity); err != nil {
		return fmt.Errorf("control-plane backup %w", err)
	}
	return nil
}

func validateReleaseMigrationIdentity(name string, identity MigrationIdentity) error {
	if !buildinfo.ValidSHA256(identity.SHA256) || identity.LatestVersion == 0 {
		return fmt.Errorf("%s migration identity is invalid", name)
	}
	return nil
}

func validateFileIdentity(identity FileIdentity, name string, minimum, maximum uint64) error {
	if identity.Name != name {
		return fmt.Errorf("member name must be exactly %q", name)
	}
	if identity.SizeBytes < minimum || identity.SizeBytes > maximum {
		return errors.New("member size is outside the supported bounds")
	}
	if !buildinfo.ValidSHA256(identity.SHA256) {
		return errors.New("member SHA-256 is invalid")
	}
	return nil
}

func validLowerHex(value string, bytesLength int) bool {
	if len(value) != bytesLength*2 {
		return false
	}
	decoded, err := hex.DecodeString(value)
	return err == nil && len(decoded) == bytesLength && hex.EncodeToString(decoded) == value
}
