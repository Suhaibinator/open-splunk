package main

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// Legacy filesystem fixtures retain security checks for retired control-file
// publication. Production ownership uses PostgreSQL maintenance/server leases.
func acquireConfiguredServerLock(databasePath, singletonPath string) (*serverLock, error) {
	if singletonPath == "" {
		singletonPath = hostSingletonLockPath
	}
	if err := validateServerSingletonLockDirectory(singletonPath); err != nil {
		return nil, err
	}
	return acquireServerLockAt(databasePath, singletonPath)
}

func acquireServerLock(databasePath string) (*serverLock, error) {
	singletonPath, err := configuredServerSingletonLockPath()
	if err != nil {
		return nil, err
	}
	return acquireConfiguredServerLock(databasePath, singletonPath)
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
