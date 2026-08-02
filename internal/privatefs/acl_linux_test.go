//go:build linux

package privatefs

import (
	"errors"
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

	// Linux canonicalizes a header-only ACL as "no ACL", so install a complete
	// non-mode-equivalent access ACL. The mask entry makes the ACL extended and
	// forces the kernel to retain the xattr instead of folding it into mode bits.
	posixACL := []byte{
		2, 0, 0, 0, // POSIX_ACL_XATTR_VERSION
		1, 0, 6, 0, 0xff, 0xff, 0xff, 0xff, // ACL_USER_OBJ: rw-
		4, 0, 0, 0, 0xff, 0xff, 0xff, 0xff, // ACL_GROUP_OBJ: ---
		16, 0, 0, 0, 0xff, 0xff, 0xff, 0xff, // ACL_MASK: ---
		32, 0, 0, 0, 0xff, 0xff, 0xff, 0xff, // ACL_OTHER: ---
	}
	// #nosec G115 -- os.File descriptors are native int descriptors on Linux.
	err = unix.Fsetxattr(
		int(file.Fd()),
		"system.posix_acl_access",
		posixACL,
		0,
	)
	if errors.Is(err, unix.ENOTSUP) {
		t.Skipf("filesystem does not permit a POSIX ACL fixture: %v", err)
	}
	if err != nil {
		t.Fatalf("install POSIX ACL fixture: %v", err)
	}
	if size, err := unix.Fgetxattr(
		int(file.Fd()),
		"system.posix_acl_access",
		nil,
	); err != nil || size == 0 {
		t.Fatalf("POSIX ACL fixture was not retained: size=%d err=%v", size, err)
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
