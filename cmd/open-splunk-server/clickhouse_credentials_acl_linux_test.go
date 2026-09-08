//go:build linux

package main

import (
	"errors"
	"os"
	"strings"
	"testing"

	"golang.org/x/sys/unix"
)

func TestReadClickHouseCredentialFileRejectsLinuxExtendedACL(t *testing.T) {
	path := writeClickHouseCredentialFixture(t, "secret", 0o600)
	// Keep mode 0600 while retaining an extended ACL, so mode validation alone
	// cannot satisfy the credential's access-control contract.
	posixACL := []byte{
		2, 0, 0, 0, // POSIX_ACL_XATTR_VERSION
		1, 0, 6, 0, 0xff, 0xff, 0xff, 0xff, // ACL_USER_OBJ: rw-
		4, 0, 0, 0, 0xff, 0xff, 0xff, 0xff, // ACL_GROUP_OBJ: ---
		16, 0, 0, 0, 0xff, 0xff, 0xff, 0xff, // ACL_MASK: ---
		32, 0, 0, 0, 0xff, 0xff, 0xff, 0xff, // ACL_OTHER: ---
	}
	err := unix.Setxattr(path, "system.posix_acl_access", posixACL, 0)
	if errors.Is(err, unix.ENOTSUP) {
		t.Skipf("filesystem does not permit a POSIX ACL fixture: %v", err)
	}
	if err != nil {
		t.Fatalf("install POSIX ACL fixture: %v", err)
	}
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
