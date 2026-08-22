package buildinfo

import (
	"bytes"
	"strings"
	"testing"
)

func TestParseAcceptsCanonicalDevelopmentAndImmutableIdentities(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		revision string
		release  bool
	}{
		{revision: DevelopmentRevision},
		{revision: strings.Repeat("a", 40), release: true},
		{revision: strings.Repeat("f", 64), release: true},
	} {
		identity, err := Parse(test.revision)
		if err != nil {
			t.Fatalf("Parse(%q): %v", test.revision, err)
		}
		if identity.Publishable() != test.release {
			t.Fatalf("Publishable(%q) = %t", test.revision, identity.Publishable())
		}
	}
}

func TestParseRejectsNoncanonicalIdentity(t *testing.T) {
	t.Parallel()

	for _, revision := range []string{
		"", "01234567", strings.Repeat("A", 40), strings.Repeat("a", 39),
	} {
		if _, err := Parse(revision); err == nil {
			t.Fatalf("Parse(%q) error = nil", revision)
		}
	}
}

func TestParseReleaseValidatesOptionalV0ProductVersion(t *testing.T) {
	t.Parallel()

	revision := strings.Repeat("a", 40)
	identity, err := ParseRelease(revision, "0.12.3")
	if err != nil {
		t.Fatal(err)
	}
	if identity.ProductVersion != "0.12.3" {
		t.Fatalf("product version = %q", identity.ProductVersion)
	}
	for _, version := range []string{"v0.1.0", "1.0.0", "0.01.0", "0.1", "0.1.0-rc.1"} {
		if _, err := ParseRelease(revision, version); err == nil {
			t.Fatalf("ParseRelease accepted %q", version)
		}
	}
	if _, err := ParseRelease(DevelopmentRevision, "0.1.0"); err == nil {
		t.Fatal("development identity accepted a product version")
	}
}

func TestUIBuildIDIsDomainSeparatedRevisionSensitiveAndAdBlockerSafe(t *testing.T) {
	t.Parallel()

	left, err := Parse("ad" + strings.Repeat("0", 38))
	if err != nil {
		t.Fatal(err)
	}
	right, err := Parse("da" + strings.Repeat("0", 38))
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
	if leftID == rightID {
		t.Fatalf("distinct revisions share UI build ID %q", leftID)
	}
}

func TestPublishableIdentityRequiresAnImmutableRevision(t *testing.T) {
	t.Parallel()

	revision := strings.Repeat("1", 40)
	identity, err := Parse(revision)
	if err != nil {
		t.Fatal(err)
	}
	if err := identity.ValidatePublishable(); err != nil {
		t.Fatalf("ValidatePublishable: %v", err)
	}
	development, err := Parse(DevelopmentRevision)
	if err != nil {
		t.Fatal(err)
	}
	if err := development.ValidatePublishable(); err == nil {
		t.Fatal("development ValidatePublishable error = nil")
	}
}

func TestWriteIdentityAndDigestValidation(t *testing.T) {
	t.Parallel()

	identity, err := Parse(strings.Repeat("a", 40))
	if err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	if err := WriteIdentity(&output, identity); err != nil {
		t.Fatalf("WriteIdentity: %v", err)
	}
	want := "source_revision=" + strings.Repeat("a", 40) + "\n"
	if output.String() != want {
		t.Fatalf("WriteIdentity output = %q, want %q", output.String(), want)
	}
	releaseIdentity, err := ParseRelease(strings.Repeat("b", 40), "0.2.0")
	if err != nil {
		t.Fatal(err)
	}
	output.Reset()
	if err := WriteIdentity(&output, releaseIdentity); err != nil {
		t.Fatal(err)
	}
	if want := "source_revision=" + strings.Repeat("b", 40) + "\nproduct_version=0.2.0\n"; output.String() != want {
		t.Fatalf("release identity = %q, want %q", output.String(), want)
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
