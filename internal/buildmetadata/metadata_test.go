package buildmetadata

import (
	"strings"
	"testing"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/buildinfo"
)

func validMetadata(t *testing.T) *opensplunk.BuildMetadata {
	t.Helper()
	identity, err := buildinfo.Parse(strings.Repeat("a", 40))
	if err != nil {
		t.Fatal(err)
	}
	uiBuildID, err := identity.UIBuildID()
	if err != nil {
		t.Fatal(err)
	}
	return &opensplunk.BuildMetadata{
		SourceRevision:             identity.SourceRevision,
		UiBuildId:                  uiBuildID,
		UiSha256:                   strings.Repeat("1", 64),
		ProtobufSchemaSha256:       strings.Repeat("2", 64),
		SqliteMigrationsSha256:     strings.Repeat("3", 64),
		ClickhouseMigrationsSha256: strings.Repeat("4", 64),
	}
}

func TestValidateAcceptsCompleteConsistentBuild(t *testing.T) {
	t.Parallel()

	metadata := validMetadata(t)
	identity, err := Validate(metadata)
	if err != nil {
		t.Fatalf("Validate: %v", err)
	}
	if identity.SourceRevision != strings.Repeat("a", 40) {
		t.Fatalf("identity = %+v", identity)
	}
}

func TestNormalizeClonesMetadata(t *testing.T) {
	t.Parallel()

	metadata := validMetadata(t)
	normalized, err := Normalize(metadata)
	if err != nil {
		t.Fatalf("Normalize: %v", err)
	}
	if normalized == metadata {
		t.Fatal("Normalize returned the caller-owned metadata pointer")
	}
	normalized.UiSha256 = strings.Repeat("9", 64)
	if metadata.UiSha256 != strings.Repeat("1", 64) {
		t.Fatal("Normalize result aliases caller-owned metadata")
	}
	if Clone(nil) != nil {
		t.Fatal("Clone(nil) is not nil")
	}
}

func TestValidateRejectsContradictoryOrMalformedRelease(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		mutate     func(*opensplunk.BuildMetadata)
		wantDetail string
	}{
		{
			name: "wrong UI build ID",
			mutate: func(metadata *opensplunk.BuildMetadata) {
				metadata.UiBuildId = "r" + strings.Repeat("0", 64)
			},
			wantDetail: "UI build ID",
		},
		{
			name: "uppercase digest",
			mutate: func(metadata *opensplunk.BuildMetadata) {
				metadata.UiSha256 = strings.Repeat("A", 64)
			},
			wantDetail: "UI SHA-256",
		},
		{
			name: "missing digest",
			mutate: func(metadata *opensplunk.BuildMetadata) {
				metadata.ProtobufSchemaSha256 = ""
			},
			wantDetail: "protobuf schema",
		},
		{
			name: "unknown protobuf fields",
			mutate: func(metadata *opensplunk.BuildMetadata) {
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
