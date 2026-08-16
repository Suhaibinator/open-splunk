//go:build darwin || linux

package input

import (
	"os"

	"golang.org/x/sys/unix"
)

// openFileForTailing prevents a regular-file-to-FIFO swap between discovery's
// Stat and Open from blocking the manager goroutine indefinitely. O_NONBLOCK is
// inert for regular files; manager revalidates the opened descriptor before use.
func openFileForTailing(path string) (*os.File, error) {
	fd, err := unix.Open(path, unix.O_RDONLY|unix.O_NONBLOCK|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, err
	}
	file := os.NewFile(uintptr(fd), path)
	if file == nil {
		_ = unix.Close(fd)
		return nil, os.ErrInvalid
	}
	return file, nil
}
