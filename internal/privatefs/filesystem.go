//go:build darwin || linux

package privatefs

import (
	"fmt"

	"golang.org/x/sys/unix"
)

// Filesystem describes the mounted filesystem behind a path or descriptor.
type Filesystem struct {
	// Name is the lowercase filesystem type, for example "ext4" or "nfs".
	// Linux reports an unrecognized super-block magic as its hex value.
	Name string
	// Remote reports a network or FUSE-backed filesystem. Neither class
	// guarantees the atomic rename flags used by private publication or the
	// coherent shared memory and byte-range locks SQLite WAL mode relies on.
	Remote bool
}

// DescribeFilesystem resolves the filesystem behind an existing path. It is
// diagnostic only: code that publishes files must probe its pinned directory
// descriptor with ProbeRenameNoReplace instead of trusting a type name.
func DescribeFilesystem(path string) (Filesystem, error) {
	var stat unix.Statfs_t
	if err := unix.Statfs(path, &stat); err != nil {
		return Filesystem{}, fmt.Errorf("inspect filesystem of %q: %w", path, err)
	}
	return filesystemFromStatfs(&stat), nil
}

// Filesystem reports the filesystem behind the pinned directory descriptor.
func (directory *Directory) Filesystem() (Filesystem, error) {
	descriptor, err := directory.descriptor()
	if err != nil {
		return Filesystem{}, err
	}
	var stat unix.Statfs_t
	if err := unix.Fstatfs(descriptor, &stat); err != nil {
		return Filesystem{}, fmt.Errorf("inspect private directory filesystem: %w", err)
	}
	return filesystemFromStatfs(&stat), nil
}
