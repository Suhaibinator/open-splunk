package main

import (
	"bytes"
	"strings"
	"testing"

	opensplunk "github.com/Suhaibinator/open-splunk"
	"github.com/Suhaibinator/open-splunk/internal/spl"
)

func TestEmbeddedReleaseVerificationIncludesCurrentSPLIdentity(t *testing.T) {
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
	if err := writeEmbeddedReleaseVerification(&output, release); err != nil {
		t.Fatalf("writeEmbeddedReleaseVerification() error = %v", err)
	}
	want := "application_version=1.2.3\n" +
		"source_revision=" + strings.Repeat("a", 40) + "\n" +
		"spl_compatibility_version=" + spl.CompatibilityVersion + "\n" +
		"ui_build_id=ui-build\n" +
		"ui_sha256=" + strings.Repeat("b", 64) + "\n"
	if output.String() != want {
		t.Fatalf("verification output = %q, want %q", output.String(), want)
	}
}

func TestEmbeddedReleaseVerificationRejectsNilOutput(t *testing.T) {
	t.Parallel()
	if err := writeEmbeddedReleaseVerification(nil, opensplunk.Release{}); err == nil {
		t.Fatal("writeEmbeddedReleaseVerification(nil) unexpectedly succeeded")
	}
}
