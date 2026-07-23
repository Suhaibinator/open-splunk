package input

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func writeFileT(t *testing.T, path, content string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}

func TestFileIdentityString(t *testing.T) {
	t.Parallel()
	id := FileIdentity{Device: 1, Inode: 2, Generation: 3, Fingerprint: "ab12", FingerprintLength: 4}
	if got, want := id.String(), "dev=1;ino=2;gen=3;fp=ab12"; got != want {
		t.Fatalf("String() = %q, want %q", got, want)
	}
	if got, want := id.TrackingKey(), "dev=1;ino=2"; got != want {
		t.Fatalf("TrackingKey() = %q, want %q", got, want)
	}
	if id.IsZero() {
		t.Fatalf("non-zero identity reported zero")
	}
	if !(FileIdentity{}).IsZero() {
		t.Fatalf("zero identity reported non-zero")
	}
}

func TestParseFileIdentityStrictRoundTrip(t *testing.T) {
	t.Parallel()
	id := FileIdentity{
		Device:      17,
		Inode:       29,
		Generation:  3,
		Fingerprint: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
	}
	got, err := ParseFileIdentity(id.String())
	if err != nil {
		t.Fatalf("ParseFileIdentity: %v", err)
	}
	if got != id {
		t.Fatalf("parsed identity = %+v, want %+v", got, id)
	}
}

func TestParseFileIdentityRejectsNonCanonicalOrIncompleteValues(t *testing.T) {
	t.Parallel()
	validFP := "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	for _, raw := range []string{
		"",
		"dev=1;ino=2;fp=" + validFP,
		"dev=1;ino=2;gen=0;fp=" + validFP,
		"dev=01;ino=2;gen=1;fp=" + validFP,
		"dev=1;ino=2;gen=1;fp=xyz",
		"dev=1;ino=2;gen=1;fp=" + validFP + ";extra=1",
		"ino=2;dev=1;gen=1;fp=" + validFP,
	} {
		if _, err := ParseFileIdentity(raw); err == nil {
			t.Errorf("ParseFileIdentity(%q) succeeded, want error", raw)
		}
	}
}

func TestIdentityStableAcrossRename(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	a := filepath.Join(dir, "a.log")
	b := filepath.Join(dir, "b.log")
	writeFileT(t, a, "hello world this is stable content\n")

	id1, err := NewFileIdentity(a, 0)
	if err != nil {
		t.Fatalf("identity a: %v", err)
	}
	if err := os.Rename(a, b); err != nil {
		t.Fatalf("rename: %v", err)
	}
	id2, err := NewFileIdentity(b, 0)
	if err != nil {
		t.Fatalf("identity b: %v", err)
	}
	if id1.String() != id2.String() {
		t.Fatalf("identity changed across rename: %s -> %s", id1, id2)
	}
	if id1.Device == 0 && id1.Inode == 0 {
		t.Fatalf("expected non-zero dev/ino on this platform, got %s", id1)
	}
}

func TestFingerprintDistinguishesContent(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	a := filepath.Join(dir, "a.log")
	b := filepath.Join(dir, "b.log")
	writeFileT(t, a, strings.Repeat("A", 64))
	writeFileT(t, b, strings.Repeat("B", 64))

	idA, err := NewFileIdentity(a, 0)
	if err != nil {
		t.Fatalf("identity a: %v", err)
	}
	idB, err := NewFileIdentity(b, 0)
	if err != nil {
		t.Fatalf("identity b: %v", err)
	}
	if idA.Fingerprint == idB.Fingerprint {
		t.Fatalf("distinct content produced identical fingerprints: %s", idA.Fingerprint)
	}
}

func TestFingerprintOfShortFileChangesAsItGrows(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	p := filepath.Join(dir, "grow.log")
	writeFileT(t, p, "abc")
	id1, err := NewFileIdentity(p, 1024)
	if err != nil {
		t.Fatalf("identity 1: %v", err)
	}
	writeFileT(t, p, "abcdef")
	id2, err := NewFileIdentity(p, 1024)
	if err != nil {
		t.Fatalf("identity 2: %v", err)
	}
	// Documented behavior: a file shorter than FingerprintBytes has a
	// fingerprint that changes as it grows.
	if id1.Fingerprint == id2.Fingerprint {
		t.Fatalf("short-file fingerprint did not change as file grew")
	}
	// dev+inode remain stable across the growth (same physical file).
	if id1.Device != id2.Device || id1.Inode != id2.Inode {
		t.Fatalf("dev/ino changed across in-place growth")
	}
}
