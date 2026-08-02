package main

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/Suhaibinator/open-splunk/internal/privatefs"
	"golang.org/x/sys/unix"
)

const (
	hostSingletonLockPath      = "/tmp/open-splunk-server-open_splunk.server.lock"
	serverSingletonLockPathEnv = "OPEN_SPLUNK_SERVER_SINGLETON_LOCK_PATH"
)

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
	singletonPath, err := configuredServerSingletonLockPath()
	if err != nil {
		return nil, err
	}
	if err := validateConfiguredServerSingletonLockDirectory(singletonPath); err != nil {
		return nil, err
	}
	return acquireServerLockAt(databasePath, singletonPath)
}

func acquireHostServerLock() (*serverLock, error) {
	singletonPath, err := configuredServerSingletonLockPath()
	if err != nil {
		return nil, err
	}
	if err := validateConfiguredServerSingletonLockDirectory(singletonPath); err != nil {
		return nil, err
	}
	return acquireHostServerLockAt(singletonPath)
}

func configuredServerSingletonLockPath() (string, error) {
	value, configured := os.LookupEnv(serverSingletonLockPathEnv)
	if !configured {
		return hostSingletonLockPath, nil
	}
	if value == "" || strings.TrimSpace(value) != value ||
		!filepath.IsAbs(value) || filepath.Clean(value) != value {
		return "", fmt.Errorf(
			"acquire server lock: %s must be an exact absolute path",
			serverSingletonLockPathEnv,
		)
	}
	return value, nil
}

func validateConfiguredServerSingletonLockDirectory(singletonPath string) (returnedErr error) {
	if _, configured := os.LookupEnv(serverSingletonLockPathEnv); !configured {
		return nil
	}
	directory, err := privatefs.OpenDirectory(filepath.Dir(singletonPath))
	if err != nil {
		return fmt.Errorf("acquire server lock: validate configured private directory: %w", err)
	}
	defer func() { returnedErr = errors.Join(returnedErr, directory.Close()) }()
	if err := directory.Revalidate(); err != nil {
		return fmt.Errorf("acquire server lock: revalidate configured private directory: %w", err)
	}
	return nil
}

func acquireHostServerLockAt(singletonPath string) (*serverLock, error) {
	if !filepath.IsAbs(singletonPath) || filepath.Clean(singletonPath) != singletonPath {
		return nil, errors.New("acquire server lock: host singleton path must be exact and absolute")
	}
	file, err := acquireFileLock(singletonPath)
	if err != nil {
		return nil, err
	}
	return &serverLock{files: []*os.File{file}}, nil
}

func acquireServerLockAt(databasePath, singletonPath string) (*serverLock, error) {
	if strings.TrimSpace(databasePath) == "" || databasePath == ":memory:" {
		return nil, errors.New("acquire server lock: control database must name a persistent file")
	}
	if !filepath.IsAbs(singletonPath) || filepath.Clean(singletonPath) != singletonPath {
		return nil, errors.New("acquire server lock: host singleton path must be exact and absolute")
	}
	absoluteDatabasePath, err := filepath.Abs(databasePath)
	if err != nil {
		return nil, fmt.Errorf("acquire server lock: resolve control database path: %w", err)
	}
	globalPath := singletonPath
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
	// #nosec G115 -- unix.Open succeeded, so fd is a non-negative native file descriptor.
	file := os.NewFile(uintptr(fd), path)
	if file == nil {
		_ = unix.Close(fd)
		return nil, fmt.Errorf("open server lock %s: invalid file descriptor", path)
	}
	if err := validateOpenServerLockFile(file, path); err != nil {
		return nil, errors.Join(err, file.Close())
	}
	if err := unix.Flock(fd, unix.LOCK_EX|unix.LOCK_NB); err != nil {
		closeErr := file.Close()
		if errors.Is(err, unix.EWOULDBLOCK) || errors.Is(err, unix.EAGAIN) {
			return nil, errors.Join(fmt.Errorf("%w: lock %s is held", errServerAlreadyRunning, path), closeErr)
		}
		return nil, errors.Join(fmt.Errorf("acquire server lock %s: %w", path, err), closeErr)
	}
	if err := validateOpenServerLockFile(file, path); err != nil {
		unlockErr := unix.Flock(fd, unix.LOCK_UN)
		return nil, errors.Join(err, unlockErr, file.Close())
	}
	return file, nil
}

func validateOpenServerLockFile(file *os.File, path string) error {
	opened, err := file.Stat()
	if err != nil {
		return fmt.Errorf("validate server lock %s: inspect descriptor: %w", path, err)
	}
	if err := privatefs.ValidateExactLockFileInfo(opened); err != nil {
		return fmt.Errorf("validate server lock %s: %w", path, err)
	}
	if err := privatefs.ValidateNoExtendedACL(file); err != nil {
		return fmt.Errorf("validate server lock %s: %w", path, err)
	}
	current, err := os.Lstat(path)
	if err != nil {
		return fmt.Errorf("validate server lock %s: inspect path: %w", path, err)
	}
	if !os.SameFile(opened, current) {
		return fmt.Errorf("validate server lock %s: path changed while locking", path)
	}
	if err := privatefs.ValidateExactLockFileInfo(current); err != nil {
		return fmt.Errorf("validate server lock %s path: %w", path, err)
	}
	return nil
}

func (lock *serverLock) Close() error {
	if lock == nil {
		return nil
	}
	var result error
	for index := len(lock.files) - 1; index >= 0; index-- {
		file := lock.files[index]
		// #nosec G115 -- os.File descriptors originate as native int descriptors on Unix.
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

func (lock *serverLock) fileForExactPath(path string) (*os.File, error) {
	if lock == nil {
		return nil, errors.New("server lock is missing")
	}
	if !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return nil, errors.New("server lock path must be exact and absolute")
	}
	for _, file := range lock.files {
		if file == nil || file.Name() != path {
			continue
		}
		if _, err := file.Stat(); err != nil {
			return nil, fmt.Errorf("inspect server lock %s: %w", path, err)
		}
		return file, nil
	}
	return nil, fmt.Errorf("server lock does not hold exact path %s", path)
}
