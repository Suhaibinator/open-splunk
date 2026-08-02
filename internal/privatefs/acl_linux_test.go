//go:build linux

package privatefs

import (
	"os"
	"testing"

	"golang.org/x/sys/unix"
)

func TestValidateNoExtendedACLRejectsLinuxPOSIXACL(t *testing.T) {
	t.Parallel()

	file, err := os.CreateTemp(t.TempDir(), "acl-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = file.Close() })

	// A POSIX ACL xattr begins with its little-endian version. The kernel
	// validates the payload before storing it, so an empty ACL header is enough
	// to exercise descriptor-based discovery without requiring setfacl.
	aclHeader := []byte{2, 0, 0, 0}
	// #nosec G115 -- os.File descriptors are native int descriptors on Linux.
	err = unix.Fsetxattr(
		int(file.Fd()),
		"system.posix_acl_access",
		aclHeader,
		0,
	)
	if err != nil {
		t.Skipf("filesystem does not permit a POSIX ACL fixture: %v", err)
	}
	if err := validateNoExtendedACL(file); err == nil {
		t.Fatal("validateNoExtendedACL accepted a POSIX ACL xattr")
	}
}

func TestContainsLinuxExtendedACL(t *testing.T) {
	t.Parallel()

	for _, attributes := range []string{
		"system.posix_acl_access\x00",
		"user.comment\x00system.posix_acl_default\x00",
		"system.nfs4_acl\x00user.comment\x00",
		"security.NTACL\x00",
	} {
		if !containsLinuxExtendedACL([]byte(attributes)) {
			t.Errorf("containsLinuxExtendedACL(%q) = false", attributes)
		}
	}
	if containsLinuxExtendedACL([]byte("user.comment\x00security.selinux\x00")) {
		t.Fatal("containsLinuxExtendedACL rejected unrelated xattrs")
	}
}
