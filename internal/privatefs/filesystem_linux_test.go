//go:build linux

package privatefs

import (
	"testing"

	"golang.org/x/sys/unix"
)

func TestFilesystemFromMagicNamesCommonFilesystems(t *testing.T) {
	t.Parallel()

	for _, testCase := range []struct {
		magic uint32
		want  Filesystem
	}{
		{magic: unix.NFS_SUPER_MAGIC, want: Filesystem{Name: "nfs", Remote: true}},
		{magic: unix.CIFS_SUPER_MAGIC, want: Filesystem{Name: "cifs", Remote: true}},
		{magic: unix.SMB_SUPER_MAGIC, want: Filesystem{Name: "smb", Remote: true}},
		{magic: unix.SMB2_SUPER_MAGIC, want: Filesystem{Name: "smb2", Remote: true}},
		{magic: unix.FUSE_SUPER_MAGIC, want: Filesystem{Name: "fuse", Remote: true}},
		{magic: unix.V9FS_MAGIC, want: Filesystem{Name: "9p", Remote: true}},
		{magic: unix.CEPH_SUPER_MAGIC, want: Filesystem{Name: "ceph", Remote: true}},
		{magic: unix.AFS_SUPER_MAGIC, want: Filesystem{Name: "afs", Remote: true}},
		{magic: unix.EXT4_SUPER_MAGIC, want: Filesystem{Name: "ext4"}},
		{magic: unix.XFS_SUPER_MAGIC, want: Filesystem{Name: "xfs"}},
		{magic: unix.BTRFS_SUPER_MAGIC, want: Filesystem{Name: "btrfs"}},
		{magic: unix.TMPFS_MAGIC, want: Filesystem{Name: "tmpfs"}},
		{magic: unix.OVERLAYFS_SUPER_MAGIC, want: Filesystem{Name: "overlay"}},
		{magic: zfsSuperMagic, want: Filesystem{Name: "zfs"}},
		{magic: unix.EXFAT_SUPER_MAGIC, want: Filesystem{Name: "exfat"}},
		{magic: unix.MSDOS_SUPER_MAGIC, want: Filesystem{Name: "vfat"}},
		{magic: ntfsSuperMagic, want: Filesystem{Name: "ntfs"}},
		{magic: 0x12345678, want: Filesystem{Name: "0x12345678"}},
	} {
		if got := filesystemFromMagic(testCase.magic); got != testCase.want {
			t.Errorf("filesystemFromMagic(%#x) = %#v, want %#v", testCase.magic, got, testCase.want)
		}
	}
}

func TestStatfsMagicNormalizesSignedWordSizes(t *testing.T) {
	t.Parallel()

	if got := statfsMagic(int64(unix.CIFS_SUPER_MAGIC)); got != unix.CIFS_SUPER_MAGIC {
		t.Fatalf("statfsMagic(int64 CIFS) = %#x", got)
	}
	// A 32-bit kernel word carries magics above 0x7fffffff as negative values.
	if got := statfsMagic(int32(-11318974)); got != unix.CIFS_SUPER_MAGIC {
		t.Fatalf("statfsMagic(int32 CIFS) = %#x, want %#x", got, uint32(unix.CIFS_SUPER_MAGIC))
	}
	if got := statfsMagic(int64(unix.NFS_SUPER_MAGIC)); got != unix.NFS_SUPER_MAGIC {
		t.Fatalf("statfsMagic(int64 NFS) = %#x", got)
	}
}
