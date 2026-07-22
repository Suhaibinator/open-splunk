//go:build darwin || linux

package collector

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"syscall"
)

const stateLockFile = ".collector.lock"

// stateDirectoryLock prevents two collector processes from mutating the same
// WAL/checkpoint directory. flock is released by the kernel on process exit.
type stateDirectoryLock struct {
	file *os.File
}

func acquireStateDirectoryLock(dir string) (*stateDirectoryLock, error) {
	path := filepath.Join(dir, stateLockFile)
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, fmt.Errorf("collector: open state lock %q: %w", path, err)
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		_ = f.Close()
		if errors.Is(err, syscall.EWOULDBLOCK) {
			return nil, fmt.Errorf("collector: state directory %q is already in use", dir)
		}
		return nil, fmt.Errorf("collector: lock state directory %q: %w", dir, err)
	}
	return &stateDirectoryLock{file: f}, nil
}

func (l *stateDirectoryLock) Close() error {
	if l == nil || l.file == nil {
		return nil
	}
	f := l.file
	l.file = nil
	unlockErr := syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
	closeErr := f.Close()
	if unlockErr != nil {
		return fmt.Errorf("collector: unlock state directory: %w", unlockErr)
	}
	if closeErr != nil {
		return fmt.Errorf("collector: close state lock: %w", closeErr)
	}
	return nil
}
