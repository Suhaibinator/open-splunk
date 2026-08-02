//go:build darwin || linux

package privatefs

import (
	"os"
	"path/filepath"
	"syscall"
	"testing"
)

type lockFileInfoWithStat struct {
	os.FileInfo
	stat syscall.Stat_t
}

func (info lockFileInfoWithStat) Sys() any {
	return &info.stat
}

func TestValidateExactLockFileInfoUsesOwnerNotDirectoryInheritedGroup(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "server.lock")
	if err := os.WriteFile(path, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	info, err := os.Lstat(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := ValidateExactLockFileInfo(info); err != nil {
		t.Fatalf("valid lock metadata rejected: %v", err)
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || stat == nil {
		t.Fatal("lock fixture has no Unix metadata")
	}
	differentGroup := *stat
	if differentGroup.Gid == ^uint32(0) {
		differentGroup.Gid--
	} else {
		differentGroup.Gid++
	}
	if err := ValidateExactLockFileInfo(lockFileInfoWithStat{
		FileInfo: info,
		stat:     differentGroup,
	}); err != nil {
		t.Fatalf("secure owner-only lock with a directory-inherited group was rejected: %v", err)
	}

	differentOwner := *stat
	if differentOwner.Uid == ^uint32(0) {
		differentOwner.Uid--
	} else {
		differentOwner.Uid++
	}
	if err := ValidateExactLockFileInfo(lockFileInfoWithStat{
		FileInfo: info,
		stat:     differentOwner,
	}); err == nil {
		t.Fatal("lock metadata owned by another user was accepted")
	}
}
