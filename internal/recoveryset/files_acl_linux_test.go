//go:build linux

package recoveryset

import (
	"errors"
	"os"
	"testing"

	"golang.org/x/sys/unix"
)

func addArchiveTestACL(t *testing.T, path string, ownerPermissions byte) {
	t.Helper()
	file, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := file.Close(); err != nil {
			t.Error(err)
		}
	}()
	// Linux canonicalizes a header-only ACL as no ACL. A mask entry makes
	// this ACL extended while the object/group/other entries preserve the
	// fixture's ordinary POSIX mode bits.
	posixACL := []byte{
		2, 0, 0, 0, // POSIX_ACL_XATTR_VERSION
		1, 0, ownerPermissions, 0, 0xff, 0xff, 0xff, 0xff, // ACL_USER_OBJ
		4, 0, 0, 0, 0xff, 0xff, 0xff, 0xff, // ACL_GROUP_OBJ
		16, 0, 0, 0, 0xff, 0xff, 0xff, 0xff, // ACL_MASK
		32, 0, 0, 0, 0xff, 0xff, 0xff, 0xff, // ACL_OTHER
	}
	// #nosec G115 -- os.File descriptors are native int descriptors on Linux.
	err = unix.Fsetxattr(int(file.Fd()), "system.posix_acl_access", posixACL, 0)
	if errors.Is(err, unix.ENOTSUP) {
		t.Skipf("filesystem does not permit a POSIX ACL fixture: %v", err)
	}
	if err != nil {
		t.Fatalf("install POSIX ACL fixture: %v", err)
	}
	// #nosec G115 -- os.File descriptors are native int descriptors on Linux.
	if size, err := unix.Fgetxattr(
		int(file.Fd()),
		"system.posix_acl_access",
		nil,
	); err != nil || size == 0 {
		t.Fatalf("POSIX ACL fixture was not retained: size=%d err=%v", size, err)
	}
}
