package collector

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/Suhaibinator/open-splunk/internal/protocolid"
	"github.com/google/uuid"
)

const maximumCollectorIdentityFileBytes = int(protocolid.MaximumBytes) + 1

const (
	collectorIdentityTempPrefix      = ".collector-id-"
	maximumCollectorStateRootEntries = 1024
)

// InitializeCollectorID returns the canonical identity for stateDir, creating
// and durably publishing one only when the state directory is new. It takes the
// same state-directory lock as Daemon so standalone initialization cannot race
// a running collector.
func InitializeCollectorID(stateDir string) (collectorID string, returnedErr error) {
	if stateDir == "" {
		return "", errors.New("collector: state directory is empty")
	}
	stateDir = filepath.Clean(stateDir)
	if err := ensureCollectorStateDirectory(stateDir); err != nil {
		return "", err
	}
	if err := secureCollectorStateDirectory(stateDir); err != nil {
		return "", err
	}

	stateLock, err := acquireStateDirectoryLock(stateDir)
	if err != nil {
		return "", err
	}
	defer func() {
		if err := stateLock.Close(); err != nil {
			returnedErr = errors.Join(returnedErr, err)
		}
	}()

	return loadOrCreateCollectorIDLocked(stateDir)
}

// ensureCollectorStateDirectory creates each missing path component in order
// and syncs its parent before proceeding. Syncing the nearest existing
// ancestor's parent also makes a retry repair a prior interrupted directory
// publication instead of silently treating it as durable.
func ensureCollectorStateDirectory(stateDir string) error {
	cleanStateDir := filepath.Clean(stateDir)
	if err := validateCollectorStateDirectoryPath(cleanStateDir); err != nil {
		return err
	}
	current := cleanStateDir
	missing := make([]string, 0, 2)
	for {
		info, err := os.Stat(current)
		if err == nil {
			if !info.IsDir() {
				return fmt.Errorf("collector: state path %q is not a directory", current)
			}
			if err := validateCollectorFileOwner(info); err != nil {
				return fmt.Errorf("collector: unsafe state directory ancestor %q: %w", current, err)
			}
			if current != cleanStateDir && info.Mode().Perm()&0o022 != 0 {
				return fmt.Errorf("collector: state directory ancestor %q is writable by another user", current)
			}
			break
		}
		if !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("collector: inspect state directory %q: %w", current, err)
		}
		missing = append(missing, current)
		parent := filepath.Dir(current)
		if parent == current {
			return fmt.Errorf("collector: no existing ancestor for state directory %q", stateDir)
		}
		current = parent
	}

	if err := syncCollectorDirectory(filepath.Dir(current)); err != nil {
		return fmt.Errorf("collector: make state directory ancestor %q durable: %w", current, err)
	}
	for index := len(missing) - 1; index >= 0; index-- {
		directory := missing[index]
		if err := os.Mkdir(directory, 0o700); err != nil && !errors.Is(err, os.ErrExist) {
			return fmt.Errorf("collector: create state directory %q: %w", directory, err)
		}
		info, err := os.Stat(directory)
		if err != nil {
			return fmt.Errorf("collector: inspect created state directory %q: %w", directory, err)
		}
		if !info.IsDir() {
			return fmt.Errorf("collector: state path %q is not a directory", directory)
		}
		if err := syncCollectorDirectory(filepath.Dir(directory)); err != nil {
			return fmt.Errorf("collector: publish state directory %q: %w", directory, err)
		}
	}
	return nil
}

func validateCollectorStateDirectoryPath(cleanStateDir string) error {
	if cleanStateDir == "." || filepath.Dir(cleanStateDir) == cleanStateDir {
		return fmt.Errorf(
			"collector: state directory %q must be a dedicated child directory, not a filesystem root or the current directory",
			cleanStateDir,
		)
	}
	return nil
}

func secureCollectorStateDirectory(stateDir string) error {
	before, err := os.Lstat(stateDir)
	if err != nil {
		return fmt.Errorf("collector: inspect state directory %q: %w", stateDir, err)
	}
	if !before.IsDir() {
		return fmt.Errorf("collector: state path %q must be a real directory, not a symlink or file", stateDir)
	}
	if err := validateCollectorFileOwner(before); err != nil {
		return fmt.Errorf("collector: unsafe state directory %q: %w", stateDir, err)
	}
	// #nosec G302 -- directories require execute permission; 0700 is owner-only.
	if err := os.Chmod(stateDir, 0o700); err != nil {
		return fmt.Errorf("collector: secure state directory %q: %w", stateDir, err)
	}
	after, err := os.Lstat(stateDir)
	if err != nil {
		return fmt.Errorf("collector: re-inspect state directory %q: %w", stateDir, err)
	}
	if !after.IsDir() || !os.SameFile(before, after) || after.Mode().Perm() != 0o700 {
		return fmt.Errorf("collector: state directory %q changed while securing it", stateDir)
	}
	if err := validateCollectorFileOwner(after); err != nil {
		return fmt.Errorf("collector: unsafe state directory %q after securing it: %w", stateDir, err)
	}
	if err := syncCollectorDirectory(stateDir); err != nil {
		return fmt.Errorf("collector: make state directory %q permissions durable: %w", stateDir, err)
	}
	return nil
}

// loadOrCreateCollectorIDLocked shares the durable identity implementation
// with Daemon.New, whose caller already owns the state-directory lock.
func loadOrCreateCollectorIDLocked(stateDir string) (string, error) {
	if err := cleanupCollectorIdentityTemps(stateDir); err != nil {
		return "", err
	}
	identityPath := filepath.Join(stateDir, collectorIDFile)
	collectorID, exists, err := readCollectorID(identityPath)
	if err != nil {
		return "", err
	}
	if exists {
		return collectorID, nil
	}

	if err := rejectPriorStateWithoutCollectorID(stateDir); err != nil {
		return "", err
	}
	return createCollectorID(stateDir, identityPath)
}

func readCollectorID(identityPath string) (collectorID string, exists bool, returnedErr error) {
	pathInfo, err := os.Lstat(identityPath)
	if errors.Is(err, os.ErrNotExist) {
		return "", false, nil
	}
	if err != nil {
		return "", false, fmt.Errorf("collector: inspect collector id %q: %w", identityPath, err)
	}
	if !pathInfo.Mode().IsRegular() {
		return "", false, fmt.Errorf("collector: collector id %q is not a regular file", identityPath)
	}
	if pathInfo.Mode().Perm()&0o077 != 0 {
		return "", false, fmt.Errorf("collector: collector id %q permissions must be owner-only", identityPath)
	}
	if err := validateCollectorIdentityMetadata(pathInfo); err != nil {
		return "", false, fmt.Errorf("collector: unsafe collector id %q: %w", identityPath, err)
	}

	identityFile, err := os.Open(identityPath)
	if err != nil {
		return "", false, fmt.Errorf("collector: open collector id %q: %w", identityPath, err)
	}
	defer func() {
		if err := identityFile.Close(); err != nil {
			returnedErr = errors.Join(returnedErr,
				fmt.Errorf("collector: close collector id %q: %w", identityPath, err))
		}
	}()

	openedInfo, err := identityFile.Stat()
	if err != nil {
		return "", false, fmt.Errorf("collector: stat opened collector id %q: %w", identityPath, err)
	}
	if !openedInfo.Mode().IsRegular() || openedInfo.Mode().Perm()&0o077 != 0 ||
		!os.SameFile(pathInfo, openedInfo) {
		return "", false, fmt.Errorf("collector: collector id %q changed while opening", identityPath)
	}
	if err := validateCollectorIdentityMetadata(openedInfo); err != nil {
		return "", false, fmt.Errorf("collector: unsafe opened collector id %q: %w", identityPath, err)
	}

	contents, err := io.ReadAll(io.LimitReader(identityFile, int64(maximumCollectorIdentityFileBytes+1)))
	if err != nil {
		return "", false, fmt.Errorf("collector: read collector id %q: %w", identityPath, err)
	}
	if len(contents) > maximumCollectorIdentityFileBytes {
		return "", false, fmt.Errorf("collector: collector id %q exceeds %d bytes", identityPath, protocolid.MaximumBytes)
	}
	if len(contents) < 2 || contents[len(contents)-1] != '\n' || bytes.IndexByte(contents[:len(contents)-1], '\n') >= 0 {
		return "", false, fmt.Errorf("collector: collector id %q must contain exactly one ID followed by a newline", identityPath)
	}

	collectorID = string(contents[:len(contents)-1])
	if !protocolid.Valid(collectorID) {
		return "", false, fmt.Errorf("collector: collector id %q is not a canonical protocol identifier", identityPath)
	}
	if err := identityFile.Sync(); err != nil {
		return "", false, fmt.Errorf("collector: sync collector id %q: %w", identityPath, err)
	}

	currentInfo, err := os.Lstat(identityPath)
	if err != nil {
		return "", false, fmt.Errorf("collector: re-inspect collector id %q: %w", identityPath, err)
	}
	if !currentInfo.Mode().IsRegular() || currentInfo.Mode().Perm()&0o077 != 0 ||
		!os.SameFile(openedInfo, currentInfo) {
		return "", false, fmt.Errorf("collector: collector id %q changed while reading", identityPath)
	}
	if err := validateCollectorIdentityMetadata(currentInfo); err != nil {
		return "", false, fmt.Errorf("collector: unsafe collector id %q after reading: %w", identityPath, err)
	}
	if err := syncCollectorDirectory(filepath.Dir(identityPath)); err != nil {
		return "", false, fmt.Errorf("collector: make collector id %q durable: %w", identityPath, err)
	}
	return collectorID, true, nil
}

func validateCollectorIdentityMetadata(info os.FileInfo) error {
	if err := validateCollectorFileOwner(info); err != nil {
		return err
	}
	return validateCollectorIdentityLinkCount(info)
}

func cleanupCollectorIdentityTemps(stateDir string) error {
	directory, err := os.Open(stateDir)
	if err != nil {
		return fmt.Errorf("collector: open state directory %q for identity cleanup: %w", stateDir, err)
	}
	entries, readErr := directory.ReadDir(maximumCollectorStateRootEntries + 1)
	closeErr := directory.Close()
	if err := errors.Join(readErr, closeErr); err != nil {
		return fmt.Errorf("collector: inspect state directory %q for identity cleanup: %w", stateDir, err)
	}
	if len(entries) > maximumCollectorStateRootEntries {
		return fmt.Errorf(
			"collector: state directory %q exceeds %d top-level entries",
			stateDir,
			maximumCollectorStateRootEntries,
		)
	}

	removed := false
	for _, entry := range entries {
		if !strings.HasPrefix(entry.Name(), collectorIdentityTempPrefix) {
			continue
		}
		path := filepath.Join(stateDir, entry.Name())
		info, err := os.Lstat(path)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return fmt.Errorf("collector: inspect stale identity temporary file %q: %w", path, err)
		}
		if !info.Mode().IsRegular() || info.Mode().Perm()&0o077 != 0 {
			return fmt.Errorf("collector: unsafe stale identity temporary file %q", path)
		}
		if err := validateCollectorFileOwner(info); err != nil {
			return fmt.Errorf("collector: unsafe stale identity temporary file %q: %w", path, err)
		}
		if err := os.Remove(path); err != nil {
			return fmt.Errorf("collector: remove stale identity temporary file %q: %w", path, err)
		}
		removed = true
	}
	if removed {
		if err := syncCollectorDirectory(stateDir); err != nil {
			return fmt.Errorf("collector: make identity temporary cleanup durable: %w", err)
		}
	}
	return nil
}

func rejectPriorStateWithoutCollectorID(stateDir string) error {
	for _, artifact := range [...]string{walSubdir, checkpointsSubdir, deadLetterFile} {
		artifactPath := filepath.Join(stateDir, artifact)
		_, err := os.Lstat(artifactPath)
		if err == nil {
			return fmt.Errorf("collector: refuse to create a new collector id beside prior state %q", artifactPath)
		}
		if !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("collector: inspect prior state %q: %w", artifactPath, err)
		}
	}
	return nil
}

func createCollectorID(stateDir, identityPath string) (collectorID string, returnedErr error) {
	collectorID = uuid.NewString()
	if !protocolid.Valid(collectorID) {
		return "", fmt.Errorf("collector: generated invalid collector id %q", collectorID)
	}

	temporaryFile, err := os.CreateTemp(stateDir, collectorIdentityTempPrefix)
	if err != nil {
		return "", fmt.Errorf("collector: create temporary collector id: %w", err)
	}
	temporaryPath := temporaryFile.Name()
	defer func() {
		if temporaryFile != nil {
			if err := temporaryFile.Close(); err != nil {
				returnedErr = errors.Join(returnedErr,
					fmt.Errorf("collector: close temporary collector id: %w", err))
			}
		}
		if err := os.Remove(temporaryPath); err != nil && !errors.Is(err, os.ErrNotExist) {
			returnedErr = errors.Join(returnedErr,
				fmt.Errorf("collector: clean up temporary collector id %q: %w", temporaryPath, err))
		}
	}()

	if err := temporaryFile.Chmod(0o600); err != nil {
		return "", fmt.Errorf("collector: secure temporary collector id: %w", err)
	}
	encodedIdentity := collectorID + "\n"
	written, err := io.WriteString(temporaryFile, encodedIdentity)
	if err != nil {
		return "", fmt.Errorf("collector: write temporary collector id: %w", err)
	}
	if written != len(encodedIdentity) {
		return "", fmt.Errorf("collector: write temporary collector id: %w", io.ErrShortWrite)
	}
	if err := temporaryFile.Sync(); err != nil {
		return "", fmt.Errorf("collector: sync temporary collector id: %w", err)
	}
	if err := temporaryFile.Close(); err != nil {
		return "", fmt.Errorf("collector: close temporary collector id: %w", err)
	}
	temporaryFile = nil

	// Linking publishes the fully synced inode atomically and, unlike Rename,
	// cannot replace an identity that appeared unexpectedly.
	if err := os.Link(temporaryPath, identityPath); err != nil {
		return "", fmt.Errorf("collector: publish collector id %q: %w", identityPath, err)
	}
	if err := os.Remove(temporaryPath); err != nil {
		return "", fmt.Errorf("collector: remove published collector id temporary file %q: %w", temporaryPath, err)
	}
	if err := syncCollectorDirectory(stateDir); err != nil {
		return "", err
	}
	return collectorID, nil
}

func syncCollectorDirectory(path string) (returnedErr error) {
	directory, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("collector: open directory %q for sync: %w", path, err)
	}
	defer func() {
		if err := directory.Close(); err != nil {
			returnedErr = errors.Join(returnedErr,
				fmt.Errorf("collector: close directory %q after sync: %w", path, err))
		}
	}()
	if err := directory.Sync(); err != nil {
		return fmt.Errorf("collector: sync directory %q: %w", path, err)
	}
	return nil
}
