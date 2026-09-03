//go:build darwin

package privatefs

import (
	"strings"

	"golang.org/x/sys/unix"
)

func filesystemFromStatfs(stat *unix.Statfs_t) Filesystem {
	return filesystemFromDarwinType(unix.ByteSliceToString(stat.Fstypename[:]), stat.Flags)
}

// filesystemFromDarwinType classifies a mount by the kernel's type name and
// mount flags. MNT_LOCAL is authoritative for network mounts; FUSE bridges
// (macFUSE, FUSE-T) advertise themselves as local, so they are matched by
// name as well.
func filesystemFromDarwinType(name string, flags uint32) Filesystem {
	name = strings.ToLower(strings.TrimSpace(name))
	if name == "" {
		name = "unknown"
	}
	remote := flags&unix.MNT_LOCAL == 0 || strings.Contains(name, "fuse")
	switch name {
	case "nfs", "smbfs", "afpfs", "webdav":
		remote = true
	}
	return Filesystem{Name: name, Remote: remote}
}
