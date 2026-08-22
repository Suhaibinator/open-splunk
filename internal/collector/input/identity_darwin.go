//go:build darwin

package input

import (
	"os"
	"syscall"

	"fortio.org/safecast"
)

// statDevIno extracts Darwin device and inode numbers from fi. Darwin exposes
// dev_t through a signed int32. The direct widening below deliberately retains
// the historical FileIdentity encoding so existing checkpoints remain usable.
func statDevIno(fi os.FileInfo) (dev, ino uint64, ok bool) {
	st, ok := fi.Sys().(*syscall.Stat_t)
	if !ok {
		return 0, 0, false
	}

	// identity encoding; changing it would orphan persisted checkpoints.
	return signedDeviceBits(st.Dev), st.Ino, true
}

func signedDeviceBits(device int32) uint64 {
	if device >= 0 {
		return safecast.MustConv[uint64](device)
	}
	magnitude := safecast.MustConv[uint64](-(int64(device) + 1)) + 1
	return ^(magnitude - 1)
}
