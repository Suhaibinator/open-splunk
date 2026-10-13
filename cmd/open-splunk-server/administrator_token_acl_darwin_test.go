//go:build darwin

package main

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/privatefs"
)

// File-based operational secrets still require strict ACL handling after the
// independent browser administrator credential has been removed.
func TestOperationalCredentialRejectsDarwinExtendedACLs(t *testing.T) {
	for _, inherited := range []bool{false, true} {
		t.Run(map[bool]string{false: "direct", true: "inherited"}[inherited], func(t *testing.T) {
			directory := t.TempDir()
			if inherited {
				addDarwinACL(t, directory, "everyone allow read,file_inherit")
			}
			path := filepath.Join(directory, "credential")
			if err := os.WriteFile(path, []byte("private-operational-secret"), 0o600); err != nil {
				t.Fatal(err)
			}
			if !inherited {
				addDarwinACL(t, path, "everyone allow read")
			}
			if _, err := readClickHouseCredentialFile(path); err == nil || !strings.Contains(err.Error(), "access-control") {
				t.Fatalf("extended ACL credential result: %v", err)
			}
		})
	}
}
func TestRecoveryDirectoryRejectsDarwinParentACL(t *testing.T) {
	directory := t.TempDir()
	if err := os.Chmod(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	addDarwinACL(t, directory, "everyone allow read")
	if opened, err := privatefs.OpenDirectory(directory); err == nil {
		opened.Close()
		t.Fatal("ACL-bearing private recovery directory accepted")
	}
}
func addDarwinACL(t *testing.T, path, entry string) {
	t.Helper()
	output, err := exec.CommandContext(t.Context(), "/bin/chmod", "+a", entry, path).CombinedOutput()
	if err != nil {
		t.Fatalf("add Darwin ACL: %v: %s", err, output)
	}
}
