package main

import (
	"bytes"
	"strings"
	"testing"

	opensplunk "github.com/Suhaibinator/open-splunk"
)

func TestEmbeddedReleaseVerificationIncludesSPLCompatibilityIdentity(t *testing.T) {
	t.Parallel()

	release := opensplunk.Release{Metadata: opensplunk.ReleaseMetadata{
		ApplicationVersion: "1.2.3",
		SourceRevision:     strings.Repeat("a", 40),
		UIBuildID:          "ui-build",
		UI: opensplunk.ComponentMetadata{
			SHA256: strings.Repeat("b", 64),
		},
	}}
	var output bytes.Buffer
	if err := writeEmbeddedReleaseVerification(&output, release, "0.3"); err != nil {
		t.Fatalf("writeEmbeddedReleaseVerification() error = %v", err)
	}
	want := "application_version=1.2.3\n" +
		"source_revision=" + strings.Repeat("a", 40) + "\n" +
		"spl_compatibility_version=0.3\n" +
		"ui_build_id=ui-build\n" +
		"ui_sha256=" + strings.Repeat("b", 64) + "\n"
	if output.String() != want {
		t.Fatalf("verification output = %q, want %q", output.String(), want)
	}
}

func TestEmbeddedReleaseVerificationRejectsInvalidIdentityBeforeWriting(t *testing.T) {
	t.Parallel()

	for _, version := range []string{"", " 0.3", "0.3\nforged", string([]byte{0xff})} {
		t.Run(version, func(t *testing.T) {
			t.Parallel()
			var output bytes.Buffer
			if err := writeEmbeddedReleaseVerification(&output, opensplunk.Release{}, version); err == nil {
				t.Fatalf("writeEmbeddedReleaseVerification(%q) unexpectedly succeeded", version)
			}
			if output.Len() != 0 {
				t.Fatalf("writeEmbeddedReleaseVerification(%q) wrote %q before rejection", version, output.String())
			}
		})
	}
}
