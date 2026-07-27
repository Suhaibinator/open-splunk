package buildinfo

import (
	"bytes"
	"strings"
	"testing"
)

func TestParseAcceptsCanonicalDevelopmentAndReleaseIdentities(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		version  string
		revision string
		release  bool
	}{
		{version: "0.1.0", revision: DevelopmentRevision},
		{version: "1.2.3-rc.1+build.5", revision: strings.Repeat("a", 40), release: true},
		{version: "2.0.0", revision: strings.Repeat("f", 64), release: true},
	} {
		identity, err := Parse(test.version, test.revision)
		if err != nil {
			t.Fatalf("Parse(%q, %q): %v", test.version, test.revision, err)
		}
		if identity.Release() != test.release {
			t.Fatalf("Release(%q) = %t", test.revision, identity.Release())
		}
	}
}

func TestParseRejectsNoncanonicalIdentity(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		version  string
		revision string
	}{
		{version: "1.0.0-01", revision: DevelopmentRevision},
		{version: "1.0.0-alpha..1", revision: DevelopmentRevision},
		{version: "1.0.0+build.", revision: DevelopmentRevision},
		{version: " 1.0.0", revision: DevelopmentRevision},
		{version: "1.0.0", revision: "01234567"},
		{version: "1.0.0", revision: strings.Repeat("A", 40)},
	} {
		if _, err := Parse(test.version, test.revision); err == nil {
			t.Fatalf("Parse(%q, %q) error = nil", test.version, test.revision)
		}
	}
}

func TestUIBuildIDIsDomainSeparatedVersionSensitiveAndAdBlockerSafe(t *testing.T) {
	t.Parallel()

	left, err := Parse("1.0.0", "ad"+strings.Repeat("0", 38))
	if err != nil {
		t.Fatal(err)
	}
	right, err := Parse("1.0.0", "da"+strings.Repeat("0", 38))
	if err != nil {
		t.Fatal(err)
	}
	leftID, err := left.UIBuildID()
	if err != nil {
		t.Fatal(err)
	}
	rightID, err := right.UIBuildID()
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(strings.ToLower(leftID), "ad") {
		t.Fatalf("UI build ID %q contains ad-blocker token", leftID)
	}
	if leftID != "r794427mgh1k73212247721j70hgn9h4kknngk19nj356810km7k5397635k45m0g" {
		t.Fatalf("UI build ID = %q", leftID)
	}
	if leftID == rightID {
		t.Fatalf("distinct revisions share UI build ID %q", leftID)
	}
	otherVersion := left
	otherVersion.ApplicationVersion = "1.0.1"
	otherVersionID, err := otherVersion.UIBuildID()
	if err != nil {
		t.Fatal(err)
	}
	if otherVersionID == leftID {
		t.Fatalf("distinct application versions share UI build ID %q", leftID)
	}
}

func TestDisplayVersionPreservesFullIdentity(t *testing.T) {
	t.Parallel()

	revision := strings.Repeat("1", 40)
	identity, err := Parse("1.2.3", revision)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := identity.DisplayVersion(), "1.2.3 ("+revision+")"; got != want {
		t.Fatalf("DisplayVersion = %q, want %q", got, want)
	}
	if err := identity.ValidateRelease(); err != nil {
		t.Fatalf("ValidateRelease: %v", err)
	}
	development, err := Parse("1.2.3", DevelopmentRevision)
	if err != nil {
		t.Fatal(err)
	}
	if err := development.ValidateRelease(); err == nil {
		t.Fatal("development ValidateRelease error = nil")
	}
}

func TestWriteIdentityAndDigestValidation(t *testing.T) {
	t.Parallel()

	identity, err := Parse("1.2.3", strings.Repeat("a", 40))
	if err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	if err := WriteIdentity(&output, identity); err != nil {
		t.Fatalf("WriteIdentity: %v", err)
	}
	want := "application_version=1.2.3\nsource_revision=" +
		strings.Repeat("a", 40) + "\n"
	if output.String() != want {
		t.Fatalf("WriteIdentity output = %q, want %q", output.String(), want)
	}
	if err := WriteIdentity(nil, identity); err == nil {
		t.Fatal("WriteIdentity(nil) error = nil")
	}
	if !ValidSHA256(strings.Repeat("a", 64)) {
		t.Fatal("ValidSHA256 rejected a canonical digest")
	}
	for _, invalid := range []string{
		strings.Repeat("a", 63),
		strings.Repeat("A", 64),
		strings.Repeat("z", 64),
	} {
		if ValidSHA256(invalid) {
			t.Fatalf("ValidSHA256(%q) = true", invalid)
		}
	}
}
