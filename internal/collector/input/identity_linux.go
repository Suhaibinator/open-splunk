//go:build linux

package input

import (
	"os"
	"syscall"
)

// statDevIno extracts Linux device and inode numbers from fi. Linux exposes
// both values directly as uint64.
func statDevIno(fi os.FileInfo) (dev, ino uint64, ok bool) {
	st, ok := fi.Sys().(*syscall.Stat_t)
	if !ok {
		return 0, 0, false
	}
	return st.Dev, st.Ino, true
}
