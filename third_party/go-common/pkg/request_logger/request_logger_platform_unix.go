//go:build darwin || linux || freebsd || netbsd || openbsd

package request_logger

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"golang.org/x/sys/unix"
)

type requestLogFileIdentity struct{ device, inode uint64 }

const requestLogTrustedRootUID uint32 = 0

type requestLogCreatedDirectory struct {
	parentFD int
	name     string
	identity requestLogFileIdentity
}

type requestLogArtifacts struct {
	path, base          string
	parentFD            int
	openDirectories     []*os.File
	mainFile            *os.File
	mainIdentity        requestLogFileIdentity
	createdFile         bool
	createdDirectories  []requestLogCreatedDirectory
	preExistingSidecars map[string]requestLogFileIdentity
	createdSidecars     map[string]requestLogFileIdentity
	released            bool
}

func requestLoggerPlatformError() error {
	return nil
}

func identityFromStat(stat *unix.Stat_t) requestLogFileIdentity {
	return requestLogFileIdentity{device: uint64(stat.Dev), inode: uint64(stat.Ino)}
}

func statRequestLogEntryAt(dirFD int, name string) (unix.Stat_t, error) {
	var stat unix.Stat_t
	if err := unix.Fstatat(dirFD, name, &stat, unix.AT_SYMLINK_NOFOLLOW); err != nil {
		return unix.Stat_t{}, err
	}
	return stat, nil
}

func requestLogIdentityAt(dirFD int, name string) (requestLogFileIdentity, error) {
	stat, err := statRequestLogEntryAt(dirFD, name)
	if err != nil {
		return requestLogFileIdentity{}, err
	}
	return identityFromStat(&stat), nil
}

func requestLogIdentityForPath(path string) (requestLogFileIdentity, uint32, error) {
	var stat unix.Stat_t
	if err := unix.Lstat(path, &stat); err != nil {
		return requestLogFileIdentity{}, 0, err
	}
	return identityFromStat(&stat), uint32(stat.Mode), nil
}

func sameRequestLogIdentity(left, right requestLogFileIdentity) bool {
	return left.device == right.device && left.inode == right.inode
}

func isRegularRequestLogMode(mode uint32) bool   { return mode&unix.S_IFMT == unix.S_IFREG }
func isDirectoryRequestLogMode(mode uint32) bool { return mode&unix.S_IFMT == unix.S_IFDIR }

func requestLogPathComponents(dir string) (start string, components []string) {
	start = "."
	if filepath.IsAbs(dir) {
		start = string(os.PathSeparator)
		dir = strings.TrimPrefix(dir, string(os.PathSeparator))
	}
	if dir == "" || dir == "." {
		return start, nil
	}
	for component := range strings.SplitSeq(dir, string(os.PathSeparator)) {
		if component != "" && component != "." {
			components = append(components, component)
		}
	}
	return start, components
}

func openRequestLogDirectoryAt(parentFD int, name string) (*os.File, error) {
	fd, err := unix.Openat(parentFD, name, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, err
	}
	return os.NewFile(uintptr(fd), name), nil
}

func openRequestLogFileAt(parentFD int, name string, create bool) (*os.File, bool, error) {
	flags := unix.O_RDWR | unix.O_NOFOLLOW | unix.O_CLOEXEC
	fd, err := unix.Openat(parentFD, name, flags, 0)
	if err == nil {
		return os.NewFile(uintptr(fd), name), false, nil
	}
	if !errors.Is(err, unix.ENOENT) || !create {
		return nil, false, err
	}
	fd, err = unix.Openat(parentFD, name, flags|unix.O_CREAT|unix.O_EXCL, requestLogFileMode)
	if err == nil {
		return os.NewFile(uintptr(fd), name), true, nil
	}
	if !errors.Is(err, unix.EEXIST) {
		return nil, false, err
	}
	fd, err = unix.Openat(parentFD, name, flags, 0)
	if err != nil {
		return nil, false, err
	}
	return os.NewFile(uintptr(fd), name), false, nil
}

func inspectRequestLogFile(file *os.File) (requestLogFileIdentity, uint32, uint32, error) {
	var stat unix.Stat_t
	if err := unix.Fstat(int(file.Fd()), &stat); err != nil {
		return requestLogFileIdentity{}, 0, 0, err
	}
	return identityFromStat(&stat), uint32(stat.Mode), stat.Uid, nil
}

func validateRequestLogAncestorOwner(ownerUID, effectiveUID uint32) error {
	if ownerUID != effectiveUID && ownerUID != requestLogTrustedRootUID {
		return fmt.Errorf("directory is controlled by untrusted owner %d", ownerUID)
	}
	return nil
}

func validateRequestLogChildEntryParentMetadata(parentMode, parentUID, childUID, effectiveUID uint32) error {
	if err := validateRequestLogAncestorOwner(parentUID, effectiveUID); err != nil {
		return err
	}
	if parentMode&0o022 == 0 {
		return nil
	}
	if parentMode&unix.S_ISVTX != 0 && childUID == effectiveUID {
		return nil
	}
	return fmt.Errorf("directory entry can be replaced by another user")
}

func validateRequestLogChildEntryParent(parent *os.File, childUID uint32) error {
	_, parentMode, parentUID, err := inspectRequestLogFile(parent)
	if err != nil {
		return err
	}
	return validateRequestLogChildEntryParentMetadata(parentMode, parentUID, childUID, uint32(os.Geteuid()))
}

func secureRequestLogFilePermissions(file *os.File) error {
	return unix.Fchmod(int(file.Fd()), requestLogFileMode)
}

type requestLogFilePermissionSetter func(*os.File) error

func inspectAndSecureRegularRequestLogFile(
	file *os.File,
	setPermissions requestLogFilePermissionSetter,
) (requestLogFileIdentity, error) {
	identity, mode, _, err := inspectRequestLogFile(file)
	if err != nil {
		return requestLogFileIdentity{}, err
	}
	if !isRegularRequestLogMode(mode) {
		return requestLogFileIdentity{}, fmt.Errorf("not a regular file")
	}
	if err := setPermissions(file); err != nil {
		return identity, err
	}
	return identity, nil
}

func prepareRequestLogArtifacts(path string) (artifacts *requestLogArtifacts, err error) {
	return prepareRequestLogArtifactsWithFilePermissionSetter(path, secureRequestLogFilePermissions)
}

func prepareRequestLogArtifactsWithFilePermissionSetter(
	path string,
	setFilePermissions requestLogFilePermissionSetter,
) (artifacts *requestLogArtifacts, err error) {
	artifacts = &requestLogArtifacts{
		path: path, base: filepath.Base(path), parentFD: -1,
		preExistingSidecars: make(map[string]requestLogFileIdentity, len(requestLogSidecarSuffixes)),
		createdSidecars:     make(map[string]requestLogFileIdentity, len(requestLogSidecarSuffixes)),
	}
	defer func() {
		if err != nil {
			err = errors.Join(err, artifacts.cleanup())
		}
	}()

	start, components := requestLogPathComponents(filepath.Dir(path))
	startFD, openErr := unix.Open(start, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if openErr != nil {
		return artifacts, fmt.Errorf("failed to open request-log path root %q: %w", start, openErr)
	}
	current := os.NewFile(uintptr(startFD), start)
	artifacts.openDirectories = append(artifacts.openDirectories, current)

	for _, component := range components {
		// An untrusted owner of an owner-writable ancestor, or any user with
		// write access to a non-sticky ancestor, can replace the next component.
		// Reject either case before creating anything. Sticky directories are
		// accepted only when their owner is trusted and the child belongs to the
		// effective user (the usual /tmp/private-dir case), checked after open.
		_, currentMode, currentUID, currentErr := inspectRequestLogFile(current)
		if currentErr != nil {
			return artifacts, fmt.Errorf("failed to inspect parent of request-log component %q: %w", component, currentErr)
		}
		if ownerErr := validateRequestLogAncestorOwner(currentUID, uint32(os.Geteuid())); ownerErr != nil {
			return artifacts, fmt.Errorf("request-log ancestor containing %q has an untrusted owner: %w", component, ownerErr)
		}
		if currentMode&0o022 != 0 && currentMode&unix.S_ISVTX == 0 {
			return artifacts, fmt.Errorf("request-log ancestor containing %q is writable by group or others without sticky-bit protection", component)
		}
		child, childErr := openRequestLogDirectoryAt(int(current.Fd()), component)
		created := false
		if errors.Is(childErr, unix.ENOENT) {
			if childErr = unix.Mkdirat(int(current.Fd()), component, requestLogDirectoryMode); childErr != nil {
				return artifacts, fmt.Errorf("failed to create request-log directory component %q: %w", component, childErr)
			}
			created = true
			child, childErr = openRequestLogDirectoryAt(int(current.Fd()), component)
		}
		if childErr != nil {
			return artifacts, fmt.Errorf("request-log parent component %q must be a non-symlink directory: %w", component, childErr)
		}
		identity, mode, childUID, identityErr := inspectRequestLogFile(child)
		if identityErr != nil || !isDirectoryRequestLogMode(mode) {
			_ = child.Close()
			if identityErr != nil {
				return artifacts, fmt.Errorf("failed to inspect request-log directory component %q: %w", component, identityErr)
			}
			return artifacts, fmt.Errorf("request-log parent component %q is not a directory", component)
		}
		if entryErr := validateRequestLogChildEntryParent(current, childUID); entryErr != nil {
			_ = child.Close()
			return artifacts, fmt.Errorf("request-log parent component %q is not protected from replacement: %w", component, entryErr)
		}
		if created {
			if chmodErr := unix.Fchmod(int(child.Fd()), requestLogDirectoryMode); chmodErr != nil {
				_ = child.Close()
				return artifacts, fmt.Errorf("failed to secure request-log directory component %q: %w", component, chmodErr)
			}
			artifacts.createdDirectories = append(artifacts.createdDirectories, requestLogCreatedDirectory{
				parentFD: int(current.Fd()), name: component, identity: identity,
			})
		}
		artifacts.openDirectories = append(artifacts.openDirectories, child)
		current = child
	}

	artifacts.parentFD = int(current.Fd())
	_, parentMode, parentUID, parentErr := inspectRequestLogFile(current)
	if parentErr != nil {
		return artifacts, fmt.Errorf("failed to inspect request-log parent directory: %w", parentErr)
	}
	if parentUID != uint32(os.Geteuid()) || parentMode&0o022 != 0 {
		return artifacts, fmt.Errorf("request-log parent directory must be owned by the effective user and not writable by group or others")
	}

	for _, suffix := range requestLogSidecarSuffixes {
		name := artifacts.base + suffix
		stat, statErr := statRequestLogEntryAt(artifacts.parentFD, name)
		if errors.Is(statErr, unix.ENOENT) {
			continue
		}
		if statErr != nil {
			return artifacts, fmt.Errorf("failed to inspect SQLite sidecar %q: %w", path+suffix, statErr)
		}
		if !isRegularRequestLogMode(uint32(stat.Mode)) {
			return artifacts, fmt.Errorf("SQLite sidecar %q must be a regular non-symlink file", path+suffix)
		}
		sidecar, _, openSidecarErr := openRequestLogFileAt(artifacts.parentFD, name, false)
		if openSidecarErr != nil {
			return artifacts, fmt.Errorf("failed to securely open SQLite sidecar %q: %w", path+suffix, openSidecarErr)
		}
		identity, identityErr := inspectAndSecureRegularRequestLogFile(sidecar, setFilePermissions)
		closeErr := sidecar.Close()
		if identityErr != nil || closeErr != nil {
			return artifacts, fmt.Errorf("failed to secure SQLite sidecar %q: %w", path+suffix, errors.Join(identityErr, closeErr))
		}
		artifacts.preExistingSidecars[suffix] = identity
	}

	mainFile, created, openMainErr := openRequestLogFileAt(artifacts.parentFD, artifacts.base, true)
	if openMainErr != nil {
		if errors.Is(openMainErr, unix.ELOOP) || errors.Is(openMainErr, unix.EISDIR) {
			return artifacts, fmt.Errorf("log database path must name a regular file (not a symlink): %w", openMainErr)
		}
		return artifacts, fmt.Errorf("failed to securely open log database file: %w", openMainErr)
	}
	artifacts.mainFile = mainFile
	artifacts.createdFile = created
	identity, identityErr := inspectAndSecureRegularRequestLogFile(mainFile, setFilePermissions)
	artifacts.mainIdentity = identity
	if identityErr != nil {
		return artifacts, fmt.Errorf("failed to inspect and secure log database file: %w", identityErr)
	}
	return artifacts, nil
}

func (artifacts *requestLogArtifacts) databasePath() string { return artifacts.path }

func (artifacts *requestLogArtifacts) verifyDatabasePath() error {
	anchoredIdentity, err := requestLogIdentityAt(artifacts.parentFD, artifacts.base)
	if err != nil {
		return fmt.Errorf("inspect database entry in held parent directory: %w", err)
	}
	if !sameRequestLogIdentity(anchoredIdentity, artifacts.mainIdentity) {
		return errors.New("database entry identity changed in held parent directory")
	}
	publicParentIdentity, publicParentMode, err := requestLogIdentityForPath(filepath.Dir(artifacts.path))
	if err != nil {
		return fmt.Errorf("inspect public parent path: %w", err)
	}
	if !isDirectoryRequestLogMode(publicParentMode) {
		return errors.New("public parent path is not a directory")
	}
	heldParentIdentity, _, _, err := inspectRequestLogFile(artifacts.openDirectories[len(artifacts.openDirectories)-1])
	if err != nil {
		return fmt.Errorf("inspect held parent directory: %w", err)
	}
	if !sameRequestLogIdentity(publicParentIdentity, heldParentIdentity) {
		return errors.New("public parent directory identity changed")
	}
	publicIdentity, publicMode, err := requestLogIdentityForPath(artifacts.path)
	if err != nil {
		return fmt.Errorf("inspect public database file: %w", err)
	}
	if !isRegularRequestLogMode(publicMode) {
		return errors.New("public database path is not a regular file")
	}
	if !sameRequestLogIdentity(publicIdentity, artifacts.mainIdentity) {
		return errors.New("public database file identity changed")
	}
	return nil
}

func (artifacts *requestLogArtifacts) captureCreatedSidecars() error {
	var captureErrors []error
	for _, suffix := range requestLogSidecarSuffixes {
		if _, existed := artifacts.preExistingSidecars[suffix]; existed {
			continue
		}
		identity, err := requestLogIdentityAt(artifacts.parentFD, artifacts.base+suffix)
		if errors.Is(err, unix.ENOENT) {
			continue
		}
		if err != nil {
			captureErrors = append(captureErrors, fmt.Errorf("inspect created SQLite sidecar %q: %w", artifacts.path+suffix, err))
			continue
		}
		artifacts.createdSidecars[suffix] = identity
	}
	return errors.Join(captureErrors...)
}

func unlinkRequestLogEntryIfIdentityMatches(parentFD int, name, displayPath string, expected requestLogFileIdentity, flags int) error {
	actual, err := requestLogIdentityAt(parentFD, name)
	if errors.Is(err, unix.ENOENT) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("inspect %q before cleanup: %w", displayPath, err)
	}
	if !sameRequestLogIdentity(actual, expected) {
		return fmt.Errorf("refusing to remove %q because its identity changed", displayPath)
	}
	if err := unix.Unlinkat(parentFD, name, flags); err != nil && !errors.Is(err, unix.ENOENT) {
		return fmt.Errorf("remove %q: %w", displayPath, err)
	}
	return nil
}

func createRequestLogCleanupPlaceholder(parentFD int) (string, requestLogFileIdentity, error) {
	var randomBytes [12]byte
	for range 8 {
		if _, err := rand.Read(randomBytes[:]); err != nil {
			return "", requestLogFileIdentity{}, fmt.Errorf("generate cleanup quarantine name: %w", err)
		}
		name := requestLogCleanupQuarantinePrefix + hex.EncodeToString(randomBytes[:])
		fd, err := unix.Openat(
			parentFD,
			name,
			unix.O_RDWR|unix.O_CREAT|unix.O_EXCL|unix.O_NOFOLLOW|unix.O_CLOEXEC,
			requestLogFileMode,
		)
		if errors.Is(err, unix.EEXIST) {
			continue
		}
		if err != nil {
			return "", requestLogFileIdentity{}, fmt.Errorf("create cleanup quarantine placeholder: %w", err)
		}
		placeholder := os.NewFile(uintptr(fd), name)
		identity, inspectErr := inspectAndSecureRegularRequestLogFile(
			placeholder,
			secureRequestLogFilePermissions,
		)
		closeErr := placeholder.Close()
		if inspectErr != nil || closeErr != nil {
			// The final parent is effective-user-owned and protected from writes
			// by other users, so a direct unlink is sufficient while unwinding
			// placeholder creation. Malicious same-effective-UID mutation is trusted.
			unlinkErr := unix.Unlinkat(parentFD, name, 0)
			if errors.Is(unlinkErr, unix.ENOENT) {
				unlinkErr = nil
			}
			return "", requestLogFileIdentity{}, errors.Join(inspectErr, closeErr, unlinkErr)
		}
		return name, identity, nil
	}
	return "", requestLogFileIdentity{}, fmt.Errorf("allocate a unique cleanup quarantine name")
}

func quarantineAndRemoveRequestLogFile(
	parentFD int,
	name, displayPath string,
	expected requestLogFileIdentity,
) (err error) {
	quarantineName, placeholderIdentity, err := createRequestLogCleanupPlaceholder(parentFD)
	if err != nil {
		return err
	}
	placeholderExists := true
	defer func() {
		if placeholderExists {
			err = errors.Join(err, unlinkRequestLogEntryIfIdentityMatches(
				parentFD, quarantineName, quarantineName, placeholderIdentity, 0,
			))
		}
	}()

	// Moving the entry over a unique 0600 placeholder under the held final-parent
	// descriptor closes the identity-check-to-unlink gap for regular files: the
	// identity is checked at the name that will actually be unlinked. A process
	// with the same effective UID can still mutate the directory and is trusted.
	if renameErr := unix.Renameat(parentFD, name, parentFD, quarantineName); renameErr != nil {
		if errors.Is(renameErr, unix.ENOENT) {
			return nil
		}
		return fmt.Errorf("quarantine %q before cleanup: %w", displayPath, renameErr)
	}
	placeholderExists = false
	return removeQuarantinedRequestLogFile(parentFD, name, quarantineName, displayPath, expected)
}

func removeQuarantinedRequestLogFile(
	parentFD int,
	name, quarantineName, displayPath string,
	expected requestLogFileIdentity,
) error {
	actual, inspectErr := requestLogIdentityAt(parentFD, quarantineName)
	if inspectErr != nil {
		return fmt.Errorf(
			"inspect quarantined request-log artifact %q: %w",
			filepath.Join(filepath.Dir(displayPath), quarantineName), inspectErr,
		)
	}
	if sameRequestLogIdentity(actual, expected) {
		if unlinkErr := unix.Unlinkat(parentFD, quarantineName, 0); unlinkErr != nil && !errors.Is(unlinkErr, unix.ENOENT) {
			return fmt.Errorf("remove quarantined request-log artifact %q: %w", displayPath, unlinkErr)
		}
		return nil
	}

	// Linkat restores regular files without overwriting a newer entry at the
	// public name. If restoration cannot be done, retain the quarantine entry and
	// report its name instead of deleting an identity we did not create.
	restoreErr := unix.Linkat(parentFD, quarantineName, parentFD, name, 0)
	if restoreErr == nil {
		if unlinkErr := unix.Unlinkat(parentFD, quarantineName, 0); unlinkErr != nil && !errors.Is(unlinkErr, unix.ENOENT) {
			return fmt.Errorf(
				"refusing to remove %q because its identity changed; restored the public name but could not remove quarantine link %q: %w",
				displayPath, quarantineName, unlinkErr,
			)
		}
		return fmt.Errorf("refusing to remove %q because its identity changed", displayPath)
	}
	return fmt.Errorf(
		"refusing to remove %q because its identity changed; replacement preserved as %q because its public name could not be restored: %w",
		displayPath, filepath.Join(filepath.Dir(displayPath), quarantineName), restoreErr,
	)
}

func removeRequestLogEntryIfIdentityMatches(parentFD int, name, displayPath string, expected requestLogFileIdentity, flags int) error {
	if flags == 0 {
		return quarantineAndRemoveRequestLogFile(parentFD, name, displayPath, expected)
	}
	// Created directories are removed only when their last observed identity
	// matches. The validated private/sticky-ancestor rules protect their parent
	// entries from other users. Malicious same-effective-UID mutation is trusted
	// and is outside this component's threat boundary.
	return unlinkRequestLogEntryIfIdentityMatches(parentFD, name, displayPath, expected, flags)
}

func (artifacts *requestLogArtifacts) cleanup() error {
	if artifacts == nil || artifacts.released {
		return nil
	}
	var cleanupErrors []error
	if artifacts.createdFile && artifacts.parentFD >= 0 {
		for suffix, identity := range artifacts.createdSidecars {
			cleanupErrors = append(cleanupErrors, removeRequestLogEntryIfIdentityMatches(
				artifacts.parentFD, artifacts.base+suffix, artifacts.path+suffix, identity, 0,
			))
		}
		cleanupErrors = append(cleanupErrors, removeRequestLogEntryIfIdentityMatches(
			artifacts.parentFD, artifacts.base, artifacts.path, artifacts.mainIdentity, 0,
		))
	}
	for _, directory := range slices.Backward(artifacts.createdDirectories) {

		cleanupErrors = append(cleanupErrors, removeRequestLogEntryIfIdentityMatches(
			directory.parentFD, directory.name, directory.name, directory.identity, unix.AT_REMOVEDIR,
		))
	}
	cleanupErrors = append(cleanupErrors, artifacts.release())
	return errors.Join(cleanupErrors...)
}

func (artifacts *requestLogArtifacts) commit() error {
	if artifacts == nil || artifacts.mainFile == nil {
		return nil
	}
	err := artifacts.mainFile.Close()
	artifacts.mainFile = nil
	return err
}

func (artifacts *requestLogArtifacts) release() error {
	if artifacts == nil || artifacts.released {
		return nil
	}
	artifacts.released = true
	var closeErrors []error
	if artifacts.mainFile != nil {
		closeErrors = append(closeErrors, artifacts.mainFile.Close())
		artifacts.mainFile = nil
	}
	for _, v := range slices.Backward(artifacts.openDirectories) {
		closeErrors = append(closeErrors, v.Close())
	}
	artifacts.openDirectories = nil
	return errors.Join(closeErrors...)
}
