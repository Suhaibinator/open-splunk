//go:build linux

package privatefs

import (
	"errors"
	"fmt"
	"os"
	"slices"
	"strings"

	"golang.org/x/sys/unix"
)

var linuxExtendedACLAttributes = []string{
	"security.NTACL",
	"system.nfs4_acl",
	"system.posix_acl_access",
	"system.posix_acl_default",
}

// validateNoExtendedACL checks the descriptor's ACL xattrs. Linux stores
// POSIX, NFSv4, and Samba ACL representations under these reserved names;
// unrelated xattrs do not grant filesystem access and are intentionally
// allowed.
func validateNoExtendedACL(file *os.File) error {
	if file == nil {
		return errors.New("private filesystem ACL is unavailable")
	}
	// #nosec G115 -- os.File descriptors are native int descriptors on Linux.
	fd := int(file.Fd())
	bytes, err := unix.Flistxattr(fd, nil)
	if errors.Is(err, unix.ENOTSUP) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("inspect private filesystem extended ACL: %w", err)
	}
	if bytes == 0 {
		return nil
	}
	buffer := make([]byte, bytes)
	bytes, err = unix.Flistxattr(fd, buffer)
	if err != nil {
		return fmt.Errorf("inspect private filesystem extended ACL: %w", err)
	}
	if containsLinuxExtendedACL(buffer[:bytes]) {
		return errors.New("private filesystem object must not have an extended ACL")
	}
	return nil
}

func containsLinuxExtendedACL(attributes []byte) bool {
	for _, name := range strings.Split(string(attributes), "\x00") {
		if slices.Contains(linuxExtendedACLAttributes, name) {
			return true
		}
	}
	return false
}
