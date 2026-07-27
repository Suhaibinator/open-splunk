//go:build darwin

package input

import (
	"os"
	"syscall"
)

// statDevIno extracts Darwin device and inode numbers from fi. Darwin exposes
// dev_t through a signed int32. The direct widening below deliberately retains
// the historical FileIdentity encoding so existing checkpoints remain usable.
func statDevIno(fi os.FileInfo) (dev, ino uint64, ok bool) {
	st, ok := fi.Sys().(*syscall.Stat_t)
	if !ok {
		return 0, 0, false
	}
	// #nosec G115 -- this signed-to-unsigned widening is the established on-disk
	// identity encoding; changing it would orphan persisted checkpoints.
	return uint64(st.Dev), st.Ino, true
}
