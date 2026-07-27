package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestWriteAtomicReplacesDestinationWithoutLeavingTemporaryFiles(t *testing.T) {
	t.Parallel()

	directory := t.TempDir()
	destination := filepath.Join(directory, "asset-manifest.json")
	if err := os.WriteFile(destination, []byte("previous"), 0o600); err != nil {
		t.Fatal(err)
	}

	if err := writeAtomic(destination, []byte("replacement\n")); err != nil {
		t.Fatalf("writeAtomic: %v", err)
	}
	contents, err := os.ReadFile(destination)
	if err != nil {
		t.Fatal(err)
	}
	if string(contents) != "replacement\n" {
		t.Fatalf("destination = %q", contents)
	}
	info, err := os.Stat(destination)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o644 {
		t.Fatalf("destination permissions = %04o", info.Mode().Perm())
	}
	assertNoManifestTemporaryFiles(t, directory)
}

func TestWriteAtomicPreservesCleanupOnPublishFailure(t *testing.T) {
	t.Parallel()

	directory := t.TempDir()
	destination := filepath.Join(directory, "occupied")
	if err := os.Mkdir(destination, 0o750); err != nil {
		t.Fatal(err)
	}

	if err := writeAtomic(destination, []byte("replacement")); err == nil {
		t.Fatal("writeAtomic unexpectedly replaced a directory")
	}
	info, err := os.Stat(destination)
	if err != nil {
		t.Fatal(err)
	}
	if !info.IsDir() {
		t.Fatal("failed publication changed the destination")
	}
	assertNoManifestTemporaryFiles(t, directory)
}

func assertNoManifestTemporaryFiles(t *testing.T, directory string) {
	t.Helper()
	entries, err := os.ReadDir(directory)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), ".asset-manifest-") {
			t.Fatalf("temporary manifest remained after publication: %s", entry.Name())
		}
	}
}
