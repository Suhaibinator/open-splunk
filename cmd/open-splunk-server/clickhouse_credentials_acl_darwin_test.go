//go:build darwin

package main

import (
	"os"
	"strings"
	"testing"
)

func TestReadClickHouseCredentialFileRejectsDarwinExtendedACL(t *testing.T) {
	path := writeClickHouseCredentialFixture(t, "secret", 0o600)
	addDarwinACL(t, path, "everyone allow read")
	info, err := os.Lstat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("ACL fixture mode = %#o, want 0600", info.Mode().Perm())
	}
	credential, err := readClickHouseCredentialFile(path)
	if err == nil || credential != nil || !strings.Contains(err.Error(), "access-control") {
		t.Fatalf("ACL-bearing credential returned (%q, %v), want rejection", credential, err)
	}
}
