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

	manifestSubject            = "control-plane backup manifest"
	controlPlaneOnlyScope      = "control-plane-only"
	databaseFilename           = "control.sqlite"
	masterKeyFilename          = "master.key"
	administratorTokenFilename = "administrator.token"
	manifestFilename           = "manifest.json"
)

// MigrationIdentity binds a contiguous migration ledger to the exact source
// corpus shipped by one build.
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
// a bundle. The name is retained for callers, but identity is source- and
// schema-based and does not include a product version.
type ReleaseIdentity struct {
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
		SourceRevision:       manifest.SourceRevision,
		SQLiteMigrations:     manifest.SQLiteMigrations,
		ClickHouseMigrations: manifest.ClickHouseMigrations,
	}
}

// MarshalCanonicalJSON validates a value and encodes it in the single
// canonical JSON form shared by every recovery manifest: two-space indent, no
// HTML escaping, and a hard size ceiling. The subject names the document in
// error messages.
func MarshalCanonicalJSON[T any](value T, subject string, maximumBytes int, validate func(T) error) ([]byte, error) {
	if err := validate(value); err != nil {
		return nil, err
	}
	var output bytes.Buffer
	encoder := json.NewEncoder(&output)
	encoder.SetIndent("", "  ")
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(value); err != nil {
		return nil, fmt.Errorf("encode %s: %w", subject, err)
	}
	if output.Len() > maximumBytes {
		return nil, errors.New(subject + " exceeds its size limit")
	}
	return output.Bytes(), nil
}

// UnmarshalCanonicalJSON decodes exactly one JSON value, validates it, and
// rejects any encoding that is not byte-identical to its canonical form.
func UnmarshalCanonicalJSON[T any](encoded []byte, subject string, maximumBytes int, validate func(T) error) (T, error) {
	var zero T
	if len(encoded) == 0 || len(encoded) > maximumBytes {
		return zero, errors.New(subject + " has an invalid size")
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.DisallowUnknownFields()
	var value T
	if err := decoder.Decode(&value); err != nil {
		return zero, fmt.Errorf("decode %s: %w", subject, err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return zero, errors.New(subject + " contains multiple JSON values")
		}
		return zero, fmt.Errorf("decode %s terminator: %w", subject, err)
	}
	if err := validate(value); err != nil {
		return zero, err
	}
	canonical, err := MarshalCanonicalJSON(value, subject, maximumBytes, validate)
	if err != nil {
		return zero, err
	}
	if !bytes.Equal(encoded, canonical) {
		return zero, errors.New(subject + " is not canonical")
	}
	return value, nil
}

func marshalManifest(manifest Manifest) ([]byte, error) {
	return MarshalCanonicalJSON(manifest, manifestSubject, maximumManifestBytes, validateManifest)
}

func unmarshalManifest(encoded []byte) (Manifest, error) {
	return UnmarshalCanonicalJSON(encoded, manifestSubject, maximumManifestBytes, validateManifest)
}

func validateManifest(manifest Manifest) error {
	if manifest.FormatVersion != manifestFormatVersion {
		return fmt.Errorf("control-plane backup manifest format version %d is unsupported; create a fresh backup", manifest.FormatVersion)
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
	if _, err := buildinfo.Parse(manifest.SourceRevision); err != nil {
		return fmt.Errorf("control-plane backup manifest source identity is invalid: %w", err)
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
	if err := ValidateFileIdentity(manifest.Database, databaseFilename, 1, maximumDatabaseBytes); err != nil {
		return fmt.Errorf("control-plane backup database: %w", err)
	}
	if err := ValidateFileIdentity(manifest.MasterKey, masterKeyFilename, serverMasterKeyBytes, serverMasterKeyBytes); err != nil {
		return fmt.Errorf("control-plane backup master key: %w", err)
	}
	if err := ValidateFileIdentity(
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
		return errors.New("control-plane backup was created by a different source or migration identity")
	}
	return nil
}

// ValidateReleaseIdentity validates the release compatibility boundary without
// attaching an operation-specific error prefix, so deployment recovery sets
// can reuse the exact same rules.
func ValidateReleaseIdentity(identity ReleaseIdentity) error {
	if _, err := buildinfo.Parse(identity.SourceRevision); err != nil {
		return fmt.Errorf("source identity is invalid: %w", err)
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

// ValidateFileIdentity checks that a manifest member names the exact expected
// file, has a size inside the supported bounds, and carries a well-formed
// SHA-256 digest.
func ValidateFileIdentity(identity FileIdentity, name string, minimum, maximum uint64) error {
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
