package main

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"golang.org/x/sys/unix"

	"github.com/Suhaibinator/open-splunk/internal/privatefs"
)

const (
	hostSingletonLockPath            = "/tmp/open-splunk-server-open_splunk.server.lock"
	serverSingletonLockPathEnv       = "OPEN_SPLUNK_SERVER_LOCK_FILE"
	legacyServerSingletonLockPathEnv = "OPEN_SPLUNK_SERVER_SINGLETON_LOCK_PATH"
)

var errServerAlreadyRunning = errors.New("another open-splunk server is already running")

// serverLock fences the supported single-host deployment lifetime. It prevents
// separate PostgreSQL control schemas from independently sequencing writes
// into the same canonical ClickHouse database on this host. PostgreSQL also
// holds its own database/schema ownership lease.
type serverLock struct {
	files []*os.File
}

func acquireHostServerLock() (*serverLock, error) {
	singletonPath, err := configuredServerSingletonLockPath()
	if err != nil {
		return nil, err
	}
	if err := validateServerSingletonLockDirectory(singletonPath); err != nil {
		return nil, err
	}
	return acquireHostServerLockAt(singletonPath)
}

func configuredServerSingletonLockPath() (string, error) {
	// Recovery dispatch precedes runtime option parsing. Enforce the same
	// rename here so a stale setting can never select a recovery-only lock.
	if _, configured := os.LookupEnv(legacyServerSingletonLockPathEnv); configured {
		return "", fmt.Errorf(
			"%s was renamed to %s",
			legacyServerSingletonLockPathEnv,
			serverSingletonLockPathEnv,
		)
	}
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

func validateServerSingletonLockDirectory(singletonPath string) (returnedErr error) {
	if singletonPath == "" {
		return errors.New("acquire server lock: server lock file is required")
	}
	if singletonPath == hostSingletonLockPath {
		return nil
	}
	return validatePrivateServerSingletonLockDirectory(singletonPath)
}

func validatePrivateServerSingletonLockDirectory(singletonPath string) (returnedErr error) {
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
	for _, file := range slices.Backward(lock.files) {

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
