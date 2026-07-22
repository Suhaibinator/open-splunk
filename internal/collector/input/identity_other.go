//go:build !darwin && !linux

package input

import "os"

// statDevIno has no portable implementation off darwin/linux; identity there
// degrades to a fingerprint-only value (Device and Inode stay zero).
func statDevIno(fi os.FileInfo) (dev, ino uint64, ok bool) {
	return 0, 0, false
}
