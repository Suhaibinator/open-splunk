//go:build darwin

package privatefs

import (
	"testing"

	"golang.org/x/sys/unix"
)

func TestFilesystemFromDarwinTypeClassifiesMounts(t *testing.T) {
	t.Parallel()

	for _, testCase := range []struct {
		name  string
		flags uint32
		want  Filesystem
	}{
		{name: "apfs", flags: unix.MNT_LOCAL, want: Filesystem{Name: "apfs"}},
		{name: "hfs", flags: unix.MNT_LOCAL | unix.MNT_JOURNALED, want: Filesystem{Name: "hfs"}},
		{name: "nfs", flags: 0, want: Filesystem{Name: "nfs", Remote: true}},
		{name: "smbfs", flags: 0, want: Filesystem{Name: "smbfs", Remote: true}},
		{name: "nfs", flags: unix.MNT_LOCAL, want: Filesystem{Name: "nfs", Remote: true}},
		{name: "macfuse", flags: unix.MNT_LOCAL, want: Filesystem{Name: "macfuse", Remote: true}},
		{name: "fuse-t", flags: unix.MNT_LOCAL, want: Filesystem{Name: "fuse-t", Remote: true}},
		{name: "", flags: unix.MNT_LOCAL, want: Filesystem{Name: "unknown"}},
	} {
		if got := filesystemFromDarwinType(testCase.name, testCase.flags); got != testCase.want {
			t.Errorf("filesystemFromDarwinType(%q, %#x) = %#v, want %#v", testCase.name, testCase.flags, got, testCase.want)
		}
	}
}
