//go:build darwin || linux

package collector

import (
	"errors"
	"fmt"
	"os"
	"syscall"
)

func validateCollectorFileOwner(info os.FileInfo) error {
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return errors.New("filesystem ownership metadata is unavailable")
	}
	if int64(stat.Uid) != int64(os.Geteuid()) {
		return fmt.Errorf("owned by uid %d, collector runs as uid %d", stat.Uid, os.Geteuid())
	}
	return nil
}

func validateCollectorIdentityLinkCount(info os.FileInfo) error {
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return errors.New("filesystem link metadata is unavailable")
	}
	if stat.Nlink != 1 {
		return fmt.Errorf("has %d hard links, want exactly one", stat.Nlink)
	}
	return nil
}
