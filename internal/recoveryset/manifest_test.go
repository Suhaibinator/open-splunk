//go:build darwin || linux

package recoveryset

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/controlbackup"
)

func TestManifestCanonicalRoundTrip(t *testing.T) {
	t.Parallel()

	want := validTestManifest()
	encoded, err := marshalManifest(want)
	if err != nil {
		t.Fatal(err)
	}
	if len(encoded) == 0 || encoded[len(encoded)-1] != '\n' {
		t.Fatalf("canonical manifest terminator = %q, want newline", encoded)
	}
	got, err := unmarshalManifest(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("manifest = %#v, want %#v", got, want)
	}
	reencoded, err := marshalManifest(got)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(reencoded, encoded) {
		t.Fatal("canonical manifest bytes changed after round trip")
	}
}

func TestManifestRejectsInvalidFields(t *testing.T) {
	t.Parallel()

	tests := map[string]func(*Manifest){
		"format version":     func(value *Manifest) { value.FormatVersion++ },
		"creation time zero": func(value *Manifest) { value.CreatedAtUnixMicro = 0 },
		"creation time too large": func(value *Manifest) {
			value.CreatedAtUnixMicro = maximumTimestampUnixMicro + 1
		},
		"recovery id": func(value *Manifest) { value.RecoverySetID = strings.Repeat("A", 32) },
		"scope":       func(value *Manifest) { value.Scope = "deployment" },
		"ClickHouse excluded": func(value *Manifest) {
			value.ClickHouseIncluded = false
		},
		"application version": func(value *Manifest) { value.ApplicationVersion = "latest" },
		"source revision":     func(value *Manifest) { value.SourceRevision = "main" },
		"SQLite migration digest": func(value *Manifest) {
			value.SQLiteMigrations.SHA256 = strings.Repeat("A", 64)
		},
		"SQLite migration version": func(value *Manifest) {
			value.SQLiteMigrations.LatestVersion = 0
		},
		"ClickHouse migration digest": func(value *Manifest) {
			value.ClickHouseMigrations.SHA256 = ""
		},
		"control directory": func(value *Manifest) { value.ControlPlane.Directory = "control" },
		"control manifest name": func(value *Manifest) {
			value.ControlPlane.Manifest.Name = "manifest.json"
		},
		"control manifest empty": func(value *Manifest) {
			value.ControlPlane.Manifest.SizeBytes = 0
		},
		"control manifest too large": func(value *Manifest) {
			value.ControlPlane.Manifest.SizeBytes = maximumControlManifestBytes + 1
		},
		"control manifest digest": func(value *Manifest) {
			value.ControlPlane.Manifest.SHA256 = "bad"
		},
		"ClickHouse server version": func(value *Manifest) {
			value.ClickHouse.ServerVersion = "latest"
		},
		"ClickHouse disk": func(value *Manifest) { value.ClickHouse.Disk = "default" },
		"ClickHouse database": func(value *Manifest) {
			value.ClickHouse.Database = "other"
		},
		"ClickHouse archive database": func(value *Manifest) {
			value.ClickHouse.ArchiveDatabase = "open_splunk_recovery_other"
		},
		"ClickHouse archive format": func(value *Manifest) {
			value.ClickHouse.ArchiveFormat = "zip"
		},
		"ClickHouse archive name": func(value *Manifest) {
			value.ClickHouse.Archive.Name = "other.tar.zst"
		},
		"ClickHouse archive empty": func(value *Manifest) {
			value.ClickHouse.Archive.SizeBytes = 0
		},
		"ClickHouse archive too large": func(value *Manifest) {
			value.ClickHouse.Archive.SizeBytes = maximumClickHouseArchiveBytes + 1
		},
		"ClickHouse archive digest": func(value *Manifest) {
			value.ClickHouse.Archive.SHA256 = strings.Repeat("g", 64)
		},
		"ClickHouse ledger digest": func(value *Manifest) {
			value.ClickHouse.MigrationLedgerSHA256 = ""
		},
		"database UUID": func(value *Manifest) {
			value.ClickHouse.DatabaseUUID = "00000000-0000-0000-0000-000000000000"
		},
		"schema migrations UUID": func(value *Manifest) {
			value.ClickHouse.SchemaMigrationsTableUUID = "not-a-uuid"
		},
		"events UUID": func(value *Manifest) {
			value.ClickHouse.EventsTableUUID = "AAAAAAAA-AAAA-4AAA-8AAA-AAAAAAAAAAAA"
		},
		"recovery sets UUID": func(value *Manifest) {
			value.ClickHouse.RecoverySetsTableUUID = value.ClickHouse.EventsTableUUID
		},
		"recovery archive markers UUID": func(value *Manifest) {
			value.ClickHouse.RecoveryArchiveMarkersTableUUID = value.ClickHouse.RecoverySetsTableUUID
		},
		"backup operation UUID": func(value *Manifest) {
			value.ClickHouse.BackupOperationUUID = value.ClickHouse.DatabaseUUID
		},
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			manifest := validTestManifest()
			mutate(&manifest)
			if _, err := marshalManifest(manifest); err == nil {
				t.Fatal("invalid manifest marshaled successfully")
			}
		})
	}
}

func TestUnmarshalManifestRejectsUnknownTrailingAndNoncanonicalJSON(t *testing.T) {
	t.Parallel()

	canonical, err := marshalManifest(validTestManifest())
	if err != nil {
		t.Fatal(err)
	}
	tests := map[string][]byte{
		"empty":         nil,
		"unknown":       bytes.Replace(canonical, []byte("{"), []byte(`{"unknown":true,`), 1),
		"trailing":      append(append([]byte(nil), canonical...), []byte("{}")...),
		"leading space": append([]byte(" "), canonical...),
		"compact":       []byte(`{"format_version":1}`),
		"oversized":     bytes.Repeat([]byte{' '}, maximumManifestBytes+1),
	}
	for name, encoded := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			if _, err := unmarshalManifest(encoded); err == nil {
				t.Fatal("invalid encoded manifest was accepted")
			}
		})
	}
}

func validTestManifest() Manifest {
	recoverySetID := strings.Repeat("a", 32)
	return Manifest{
		FormatVersion:      manifestFormatVersion,
		CreatedAtUnixMicro: time.Date(2026, 8, 2, 1, 2, 3, 4_000, time.UTC).UnixMicro(),
		RecoverySetID:      recoverySetID,
		Scope:              deploymentRecoverySetScope,
		ClickHouseIncluded: true,
		ApplicationVersion: "0.1.0",
		SourceRevision:     "development",
		SQLiteMigrations: controlbackup.MigrationIdentity{
			SHA256: strings.Repeat("1", 64), LatestVersion: 21,
		},
		ClickHouseMigrations: controlbackup.MigrationIdentity{
			SHA256: strings.Repeat("2", 64), LatestVersion: 4,
		},
		ControlPlane: ControlPlaneIdentity{
			Directory: controlPlaneDirectory,
			Manifest: FileIdentity{
				Name: controlPlaneManifestName, SizeBytes: 4096, SHA256: strings.Repeat("3", 64),
			},
		},
		ClickHouse: ClickHouseIdentity{
			ServerVersion:   clickHouseServerVersion,
			Disk:            clickHouseRecoveryDisk,
			Database:        clickHouseDatabase,
			ArchiveDatabase: clickHouseArchiveDatabase(recoverySetID),
			ArchiveFormat:   clickHouseArchiveFormat,
			Archive: FileIdentity{
				Name: clickHouseArchiveName(recoverySetID), SizeBytes: 8192, SHA256: strings.Repeat("4", 64),
			},
			MigrationLedgerSHA256:           strings.Repeat("5", 64),
			DatabaseUUID:                    "11111111-1111-4111-8111-111111111111",
			SchemaMigrationsTableUUID:       "22222222-2222-4222-8222-222222222222",
			EventsTableUUID:                 "33333333-3333-4333-8333-333333333333",
			RecoverySetsTableUUID:           "44444444-4444-4444-8444-444444444444",
			RecoveryArchiveMarkersTableUUID: "55555555-5555-4555-8555-555555555555",
			MaxVisibilitySeq:                42,
			BackupOperationUUID:             "66666666-6666-4666-8666-666666666666",
		},
	}
}
