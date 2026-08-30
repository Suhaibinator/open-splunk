//go:build darwin

package searchartifacts

import (
	"os"

	"golang.org/x/sys/unix"
)

func statArtifactFile(file *os.File) (artifactFileIdentity, error) {
	var stat unix.Stat_t
	if err := unix.Fstat(int(file.Fd()), &stat); err != nil {
		return artifactFileIdentity{}, err
	}
	return artifactFileIdentity{
		Device: uint64(stat.Dev), Inode: stat.Ino, Size: stat.Size,
		ModifiedSeconds: stat.Mtim.Sec, ModifiedNanos: stat.Mtim.Nsec,
		ChangedSeconds: stat.Ctim.Sec, ChangedNanos: stat.Ctim.Nsec,
	}, nil
}
