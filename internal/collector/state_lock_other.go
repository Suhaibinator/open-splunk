//go:build !darwin && !linux

package collector

import (
	"fmt"
	"os"
	"path/filepath"
)

const stateLockFile = ".collector.lock"

// The fallback uses exclusive creation. Production targets use the
// kernel-released flock implementation in state_lock_unix.go.
type stateDirectoryLock struct {
	file *os.File
	path string
}

func acquireStateDirectoryLock(dir string) (*stateDirectoryLock, error) {
	path := filepath.Join(dir, stateLockFile)
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o600)
	if err != nil {
		return nil, fmt.Errorf("collector: state directory %q is already in use or cannot be locked: %w", dir, err)
	}
	return &stateDirectoryLock{file: f, path: path}, nil
}

func (l *stateDirectoryLock) Close() error {
	if l == nil || l.file == nil {
		return nil
	}
	f := l.file
	l.file = nil
	closeErr := f.Close()
	removeErr := os.Remove(l.path)
	if closeErr != nil {
		return closeErr
	}
	return removeErr
}
