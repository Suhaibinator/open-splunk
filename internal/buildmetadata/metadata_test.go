package buildmetadata

import (
	"errors"
	"strings"
	"testing"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/buildinfo"
)

func validMetadata(t *testing.T) *opensplunkv1.BuildMetadata {
	t.Helper()
	identity, err := buildinfo.Parse("1.2.3", strings.Repeat("a", 40))
	if err != nil {
		t.Fatal(err)
	}
	uiBuildID, err := identity.UIBuildID()
	if err != nil {
		t.Fatal(err)
	}
	return &opensplunkv1.BuildMetadata{
		ApplicationVersion:         identity.ApplicationVersion,
		SourceRevision:             identity.SourceRevision,
		UiBuildId:                  uiBuildID,
		UiSha256:                   strings.Repeat("1", 64),
		ProtobufSchemaSha256:       strings.Repeat("2", 64),
		SqliteMigrationsSha256:     strings.Repeat("3", 64),
		SqliteMigrationVersion:     2,
		ClickhouseMigrationsSha256: strings.Repeat("4", 64),
		ClickhouseMigrationVersion: 1,
		AssetManifestFormatVersion: 1,
	}
}

func TestValidateAcceptsCompleteConsistentRelease(t *testing.T) {
	t.Parallel()

	metadata := validMetadata(t)
	identity, err := Validate(metadata)
	if err != nil {
		t.Fatalf("Validate: %v", err)
	}
	if identity.DisplayVersion() != "1.2.3 ("+strings.Repeat("a", 40)+")" {
		t.Fatalf("identity = %+v", identity)
	}
}

func TestNormalizeClonesAndResolvesLegacyVersion(t *testing.T) {
	t.Parallel()

	metadata := validMetadata(t)
	wantVersion := "1.2.3 (" + strings.Repeat("a", 40) + ")"
	normalized, version, err := Normalize(metadata, "")
	if err != nil {
		t.Fatalf("Normalize: %v", err)
	}
	if normalized == metadata {
		t.Fatal("Normalize returned the caller-owned metadata pointer")
	}
	if version != wantVersion {
		t.Fatalf("Normalize version = %q, want %q", version, wantVersion)
	}
	normalized.UiSha256 = strings.Repeat("9", 64)
	if metadata.UiSha256 != strings.Repeat("1", 64) {
		t.Fatal("Normalize result aliases caller-owned metadata")
	}
	if _, _, err := Normalize(metadata, "contradictory"); !errors.Is(err, ErrVersionMismatch) {
		t.Fatalf("Normalize contradiction error = %v", err)
	}
	if Clone(nil) != nil {
		t.Fatal("Clone(nil) is not nil")
	}
}

func TestValidateRejectsContradictoryOrMalformedRelease(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		mutate     func(*opensplunkv1.BuildMetadata)
		wantDetail string
	}{
		{
			name: "wrong UI build ID",
			mutate: func(metadata *opensplunkv1.BuildMetadata) {
				metadata.UiBuildId = "r" + strings.Repeat("0", 64)
			},
			wantDetail: "UI build ID",
		},
		{
			name: "uppercase digest",
			mutate: func(metadata *opensplunkv1.BuildMetadata) {
				metadata.UiSha256 = strings.Repeat("A", 64)
			},
			wantDetail: "UI SHA-256",
		},
		{
			name: "missing digest",
			mutate: func(metadata *opensplunkv1.BuildMetadata) {
				metadata.ProtobufSchemaSha256 = ""
			},
			wantDetail: "protobuf schema",
		},
		{
			name: "zero migration version",
			mutate: func(metadata *opensplunkv1.BuildMetadata) {
				metadata.ClickhouseMigrationVersion = 0
			},
			wantDetail: "ClickHouse migration version",
		},
		{
			name: "unsupported manifest format version",
			mutate: func(metadata *opensplunkv1.BuildMetadata) {
				metadata.AssetManifestFormatVersion = 2
			},
			wantDetail: "asset manifest format version",
		},
		{
			name: "unknown protobuf fields",
			mutate: func(metadata *opensplunkv1.BuildMetadata) {
				metadata.ProtoReflect().SetUnknown([]byte{0xf8, 0x07, 0x01})
			},
			wantDetail: "unknown fields",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			metadata := validMetadata(t)
			test.mutate(metadata)
			if _, err := Validate(metadata); err == nil ||
				!strings.Contains(err.Error(), test.wantDetail) {
				t.Fatalf("Validate error = %v, want detail %q", err, test.wantDetail)
			}
		})
	}
}
