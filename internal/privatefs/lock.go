//go:build darwin || linux

package privatefs

import (
	"errors"
	"os"
	"syscall"
)

// ValidateExactLockFileInfo validates the metadata contract shared by every
// supported server-lock consumer. Descriptor ACL and pathname-identity checks
// remain the caller's responsibility because they require the live file and
// its pinned parent directory.
func ValidateExactLockFileInfo(info os.FileInfo) error {
	if info == nil || !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 {
		return errors.New("lock must be a regular file")
	}
	if info.Mode().Perm() != 0o600 ||
		info.Mode()&(os.ModeSetuid|os.ModeSetgid|os.ModeSticky) != 0 {
		return errors.New("lock permissions must be exactly 0600 without special bits")
	}
	if info.Size() != 0 {
		return errors.New("lock must be empty")
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || stat == nil {
		return errors.New("lock ownership and link metadata are unavailable")
	}
	// Group ownership is deliberately irrelevant for an exact 0600 file. On
	// Darwin, newly created files inherit the parent directory's group, so a
	// secure lock in /tmp commonly has group wheel even when the process's
	// effective group is staff.
	if int64(stat.Uid) != int64(os.Geteuid()) {
		return errors.New("lock must be owned by the effective user")
	}
	if stat.Nlink != 1 {
		return errors.New("lock must have exactly one link")
	}
	return nil
}
