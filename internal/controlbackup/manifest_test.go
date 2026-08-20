package controlbackup

import (
	"bytes"
	"strings"
	"testing"
	"time"
)

func TestManifestRoundTripIsCanonicalAndStrict(t *testing.T) {
	t.Parallel()

	manifest := validTestManifest()
	encoded, err := marshalManifest(manifest)
	if err != nil {
		t.Fatal(err)
	}
	if len(encoded) == 0 || encoded[len(encoded)-1] != '\n' {
		t.Fatalf("canonical manifest terminator = %q, want newline", encoded)
	}
	decoded, err := unmarshalManifest(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if decoded != manifest {
		t.Fatalf("manifest round trip = %#v, want %#v", decoded, manifest)
	}
	reencoded, err := marshalManifest(decoded)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(reencoded, encoded) {
		t.Fatalf("canonical re-encoding changed:\n%s\n%s", encoded, reencoded)
	}
}

func TestManifestRejectsNoncanonicalUnknownDuplicateAndOversizedInput(t *testing.T) {
	t.Parallel()

	encoded, err := marshalManifest(validTestManifest())
	if err != nil {
		t.Fatal(err)
	}
	for name, mutate := range map[string]func([]byte) []byte{
		"leading whitespace": func(input []byte) []byte {
			return append([]byte(" "), input...)
		},
		"trailing whitespace": func(input []byte) []byte {
			return append(input, ' ')
		},
		"unknown field": func(input []byte) []byte {
			return bytes.Replace(input, []byte("{\n"), []byte("{\n  \"unknown\": true,\n"), 1)
		},
		"duplicate field": func(input []byte) []byte {
			return bytes.Replace(input, []byte("{\n"), []byte("{\n  \"format_version\": 1,\n"), 1)
		},
		"multiple documents": func(input []byte) []byte {
			return append(input, []byte("{}\n")...)
		},
		"oversized": func(_ []byte) []byte {
			return bytes.Repeat([]byte{'x'}, maximumManifestBytes+1)
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			if _, err := unmarshalManifest(mutate(append([]byte(nil), encoded...))); err == nil {
				t.Fatal("invalid manifest succeeded")
			}
		})
	}
}

func TestManifestValidationRejectsEveryBoundedContractViolation(t *testing.T) {
	t.Parallel()

	tests := map[string]func(*Manifest){
		"format":                   func(value *Manifest) { value.FormatVersion++ },
		"creation zero":            func(value *Manifest) { value.CreatedAtUnixMicro = 0 },
		"creation too large":       func(value *Manifest) { value.CreatedAtUnixMicro = maximumTimestampUnixMicro + 1 },
		"recovery id":              func(value *Manifest) { value.RecoverySetID = strings.Repeat("A", recoverySetIDHexBytes) },
		"scope":                    func(value *Manifest) { value.Scope = "deployment" },
		"clickhouse included":      func(value *Manifest) { value.ClickHouseIncluded = true },
		"source revision":          func(value *Manifest) { value.SourceRevision = "main" },
		"sqlite digest":            func(value *Manifest) { value.SQLiteMigrations.SHA256 = "bad" },
		"sqlite version":           func(value *Manifest) { value.SQLiteMigrations.LatestVersion = 0 },
		"sqlite ledger digest":     func(value *Manifest) { value.SQLiteMigrationLedgerSHA256 = "bad" },
		"clickhouse digest":        func(value *Manifest) { value.ClickHouseMigrations.SHA256 = "bad" },
		"clickhouse version":       func(value *Manifest) { value.ClickHouseMigrations.LatestVersion = 0 },
		"database name":            func(value *Manifest) { value.Database.Name = "../control.sqlite" },
		"database empty":           func(value *Manifest) { value.Database.SizeBytes = 0 },
		"database oversized":       func(value *Manifest) { value.Database.SizeBytes = maximumDatabaseBytes + 1 },
		"database digest":          func(value *Manifest) { value.Database.SHA256 = "bad" },
		"master key name":          func(value *Manifest) { value.MasterKey.Name = "MASTER.KEY" },
		"master key size":          func(value *Manifest) { value.MasterKey.SizeBytes-- },
		"master key digest":        func(value *Manifest) { value.MasterKey.SHA256 = "bad" },
		"administrator token name": func(value *Manifest) { value.AdministratorToken.Name = "token" },
		"administrator token small": func(value *Manifest) {
			value.AdministratorToken.SizeBytes = minimumAdministratorTokenBytes - 1
		},
		"administrator token large": func(value *Manifest) {
			value.AdministratorToken.SizeBytes = maximumAdministratorTokenBytes + 1
		},
		"administrator token digest": func(value *Manifest) { value.AdministratorToken.SHA256 = "bad" },
		"master key identity":        func(value *Manifest) { value.MasterKeyFingerprintSHA256 = "bad" },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			manifest := validTestManifest()
			mutate(&manifest)
			if err := validateManifest(manifest); err == nil {
				t.Fatalf("invalid manifest = %#v", manifest)
			}
		})
	}
}

func TestManifestRequiresExactExpectedSourceAndMigrations(t *testing.T) {
	t.Parallel()

	manifest := validTestManifest()
	expected := manifest.ReleaseIdentity()
	if err := validateManifestRelease(manifest, expected); err != nil {
		t.Fatal(err)
	}
	for name, mutate := range map[string]func(*ReleaseIdentity){
		"source revision": func(value *ReleaseIdentity) { value.SourceRevision = strings.Repeat("b", 40) },
		"sqlite digest": func(value *ReleaseIdentity) {
			value.SQLiteMigrations.SHA256 = strings.Repeat("b", 64)
		},
		"sqlite version": func(value *ReleaseIdentity) { value.SQLiteMigrations.LatestVersion++ },
		"clickhouse digest": func(value *ReleaseIdentity) {
			value.ClickHouseMigrations.SHA256 = strings.Repeat("c", 64)
		},
		"clickhouse version": func(value *ReleaseIdentity) { value.ClickHouseMigrations.LatestVersion++ },
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			changed := expected
			mutate(&changed)
			if err := validateManifestRelease(manifest, changed); err == nil {
				t.Fatal("mismatched source or migrations succeeded")
			}
		})
	}
}

func validTestManifest() Manifest {
	return Manifest{
		FormatVersion:               manifestFormatVersion,
		CreatedAtUnixMicro:          time.Date(2026, 8, 2, 1, 2, 3, 4_000, time.UTC).UnixMicro(),
		RecoverySetID:               strings.Repeat("a", recoverySetIDHexBytes),
		Scope:                       controlPlaneOnlyScope,
		ClickHouseIncluded:          false,
		SourceRevision:              "development",
		SQLiteMigrations:            MigrationIdentity{SHA256: strings.Repeat("1", 64), LatestVersion: 1},
		SQLiteMigrationLedgerSHA256: strings.Repeat("7", 64),
		ClickHouseMigrations:        MigrationIdentity{SHA256: strings.Repeat("2", 64), LatestVersion: 1},
		Database: FileIdentity{
			Name: databaseFilename, SizeBytes: 4096, SHA256: strings.Repeat("3", 64),
		},
		MasterKey: FileIdentity{
			Name: masterKeyFilename, SizeBytes: serverMasterKeyBytes, SHA256: strings.Repeat("4", 64),
		},
		AdministratorToken: FileIdentity{
			Name:      administratorTokenFilename,
			SizeBytes: minimumAdministratorTokenBytes,
			SHA256:    strings.Repeat("5", 64),
		},
		MasterKeyFingerprintSHA256: strings.Repeat("6", 64),
	}
}
