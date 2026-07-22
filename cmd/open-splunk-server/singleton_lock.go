package main

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"golang.org/x/sys/unix"
)

const hostSingletonLockPath = "/tmp/open-splunk-server-open_splunk.server.lock"

var errServerAlreadyRunning = errors.New("another open-splunk server is already running")

// serverLock fences the complete supported single-node deployment lifetime,
// not merely SQLite migrations. The host-wide lock prevents two processes
// with different control paths from independently sequencing writes into the
// same canonical ClickHouse schema. The database sidecar additionally ties
// ownership to the resolved control path and makes configuration mistakes
// fail closed.
type serverLock struct {
	files []*os.File
}

func acquireServerLock(databasePath string) (*serverLock, error) {
	return acquireServerLockAt(databasePath, hostSingletonLockPath)
}

func acquireServerLockAt(databasePath, singletonPath string) (*serverLock, error) {
	if strings.TrimSpace(databasePath) == "" || databasePath == ":memory:" {
		return nil, errors.New("acquire server lock: control database must name a persistent file")
	}
	if !filepath.IsAbs(singletonPath) {
		return nil, errors.New("acquire server lock: host singleton path must be absolute")
	}
	absoluteDatabasePath, err := filepath.Abs(databasePath)
	if err != nil {
		return nil, fmt.Errorf("acquire server lock: resolve control database path: %w", err)
	}
	globalPath := filepath.Clean(singletonPath)
	controlPath := absoluteDatabasePath + ".server.lock"
	paths := []string{globalPath}
	if controlPath != globalPath {
		paths = append(paths, controlPath)
	}
	lock := &serverLock{files: make([]*os.File, 0, len(paths))}
	for _, path := range paths {
		file, err := acquireFileLock(path)
		if err != nil {
			return nil, errors.Join(err, lock.Close())
		}
		lock.files = append(lock.files, file)
	}
	return lock, nil
}

func acquireFileLock(path string) (*os.File, error) {
	// O_NOFOLLOW makes a predictable lock path fail closed if replaced by a
	// symlink. The file remains after shutdown: unlinking a flock file permits
	// a new inode to be locked while an older process still owns the original.
	fd, err := unix.Open(path, unix.O_CREAT|unix.O_RDWR|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open server lock %s: %w", path, err)
	}
	file := os.NewFile(uintptr(fd), path)
	if file == nil {
		_ = unix.Close(fd)
		return nil, fmt.Errorf("open server lock %s: invalid file descriptor", path)
	}
	if err := unix.Flock(fd, unix.LOCK_EX|unix.LOCK_NB); err != nil {
		closeErr := file.Close()
		if errors.Is(err, unix.EWOULDBLOCK) || errors.Is(err, unix.EAGAIN) {
			return nil, errors.Join(fmt.Errorf("%w: lock %s is held", errServerAlreadyRunning, path), closeErr)
		}
		return nil, errors.Join(fmt.Errorf("acquire server lock %s: %w", path, err), closeErr)
	}
	return file, nil
}

func (lock *serverLock) Close() error {
	if lock == nil {
		return nil
	}
	var result error
	for index := len(lock.files) - 1; index >= 0; index-- {
		file := lock.files[index]
		unlockErr := unix.Flock(int(file.Fd()), unix.LOCK_UN)
		closeErr := file.Close()
		if unlockErr != nil {
			result = errors.Join(result, fmt.Errorf("release server lock %s: %w", file.Name(), unlockErr))
		}
		if closeErr != nil {
			result = errors.Join(result, fmt.Errorf("close server lock %s: %w", file.Name(), closeErr))
		}
	}
	lock.files = nil
	return result
}
