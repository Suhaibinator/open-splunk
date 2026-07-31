//go:build darwin

package main

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/auth"
)

func TestReadAdministratorTokenRejectsDarwinExtendedACLs(t *testing.T) {
	t.Parallel()

	token := []byte(strings.Repeat(
		"A",
		auth.MinimumBrowserBearerTokenBytes,
	))
	t.Run("direct", func(t *testing.T) {
		path := writeAdministratorTokenFile(t, token, 0o600)
		addDarwinACL(t, path, "everyone allow read")
		assertAdministratorTokenACLRejected(t, path)
	})
	t.Run("inherited", func(t *testing.T) {
		directory := t.TempDir()
		addDarwinACL(
			t,
			directory,
			"everyone allow read,file_inherit",
		)
		path := filepath.Join(directory, "administrator-token")
		if err := os.WriteFile(path, token, 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.Chmod(path, 0o600); err != nil {
			t.Fatal(err)
		}
		assertAdministratorTokenACLRejected(t, path)
	})
}

func TestProvisionAdministratorTokenRejectsDarwinParentACL(t *testing.T) {
	t.Parallel()

	token := []byte(strings.Repeat(
		"A",
		auth.MinimumBrowserBearerTokenBytes,
	))
	source := writeProvisioningTokenSource(t, token, 0o444)
	directory := secureProvisioningDirectory(t)
	addDarwinACL(t, directory, "everyone allow read")
	if err := provisionAdministratorToken(
		source,
		filepath.Join(directory, "administrator-token"),
	); err == nil || !strings.Contains(err.Error(), "ACL") {
		t.Fatalf("ACL-bearing destination parent error = %v", err)
	}
}

func addDarwinACL(t *testing.T, path string, entry string) {
	t.Helper()

	output, err := exec.CommandContext(
		t.Context(),
		"/bin/chmod",
		"+a",
		entry,
		path,
	).
		CombinedOutput()
	if err != nil {
		t.Fatalf("add Darwin ACL: %v: %s", err, output)
	}
}

func assertAdministratorTokenACLRejected(t *testing.T, path string) {
	t.Helper()

	info, err := os.Lstat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("ACL fixture mode = %#o, want 0600", info.Mode().Perm())
	}
	if _, err := readAdministratorToken(path); err == nil ||
		!strings.Contains(err.Error(), "extended ACL") {
		t.Fatalf("ACL-bearing token error = %v", err)
	}
}
