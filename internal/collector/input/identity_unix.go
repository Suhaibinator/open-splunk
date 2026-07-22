//go:build darwin || linux

package input

import (
	"os"
	"syscall"
)

// statDevIno extracts the platform device and inode numbers from fi. It reports
// ok=false when the underlying os.FileInfo does not carry a *syscall.Stat_t (it
// always does on darwin/linux for files stat'd from the local filesystem).
func statDevIno(fi os.FileInfo) (dev, ino uint64, ok bool) {
	st, ok := fi.Sys().(*syscall.Stat_t)
	if !ok {
		return 0, 0, false
	}
	return uint64(st.Dev), uint64(st.Ino), true
}
