//go:build linux

package privatefs

import (
	"fmt"
	"math"

	"fortio.org/safecast"
	"golang.org/x/sys/unix"
)

// Super-block magics that golang.org/x/sys/unix does not define.
const (
	ntfsSuperMagic uint32 = 0x5346544e
	zfsSuperMagic  uint32 = 0x2fc12fc1
)

func filesystemFromStatfs(stat *unix.Statfs_t) Filesystem {
	return filesystemFromMagic(statfsMagic(stat.Type))
}

// statfsMagic normalizes the kernel's word-sized f_type to the 32-bit magic
// the constants are defined in. The field is signed and its width follows the
// architecture, so magics above 0x7fffffff arrive negative on 32-bit targets.
func statfsMagic[T ~int32 | ~int64](raw T) uint32 {
	return safecast.MustConv[uint32](int64(raw) & math.MaxUint32)
}

func filesystemFromMagic(magic uint32) Filesystem {
	switch magic {
	case unix.NFS_SUPER_MAGIC:
		return Filesystem{Name: "nfs", Remote: true}
	case unix.CIFS_SUPER_MAGIC:
		return Filesystem{Name: "cifs", Remote: true}
	case unix.SMB_SUPER_MAGIC:
		return Filesystem{Name: "smb", Remote: true}
	case unix.SMB2_SUPER_MAGIC:
		return Filesystem{Name: "smb2", Remote: true}
	case unix.FUSE_SUPER_MAGIC:
		return Filesystem{Name: "fuse", Remote: true}
	case unix.V9FS_MAGIC:
		return Filesystem{Name: "9p", Remote: true}
	case unix.CEPH_SUPER_MAGIC:
		return Filesystem{Name: "ceph", Remote: true}
	case unix.AFS_SUPER_MAGIC, unix.AFS_FS_MAGIC:
		return Filesystem{Name: "afs", Remote: true}
	case unix.CODA_SUPER_MAGIC:
		return Filesystem{Name: "coda", Remote: true}
	case unix.EXT4_SUPER_MAGIC:
		// ext2, ext3, and ext4 share one magic; ext4 is the deployed default.
		return Filesystem{Name: "ext4"}
	case unix.XFS_SUPER_MAGIC:
		return Filesystem{Name: "xfs"}
	case unix.BTRFS_SUPER_MAGIC, unix.BTRFS_TEST_MAGIC:
		return Filesystem{Name: "btrfs"}
	case unix.TMPFS_MAGIC:
		return Filesystem{Name: "tmpfs"}
	case unix.OVERLAYFS_SUPER_MAGIC:
		return Filesystem{Name: "overlay"}
	case zfsSuperMagic:
		return Filesystem{Name: "zfs"}
	case unix.EXFAT_SUPER_MAGIC:
		return Filesystem{Name: "exfat"}
	case unix.MSDOS_SUPER_MAGIC:
		return Filesystem{Name: "vfat"}
	case ntfsSuperMagic:
		return Filesystem{Name: "ntfs"}
	case unix.F2FS_SUPER_MAGIC:
		return Filesystem{Name: "f2fs"}
	case unix.BCACHEFS_SUPER_MAGIC:
		return Filesystem{Name: "bcachefs"}
	case unix.SQUASHFS_MAGIC:
		return Filesystem{Name: "squashfs"}
	case unix.RAMFS_MAGIC:
		return Filesystem{Name: "ramfs"}
	default:
		return Filesystem{Name: fmt.Sprintf("0x%x", magic)}
	}
}
