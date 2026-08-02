//go:build darwin || linux

package recoveryset

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"math"
	"os"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/Suhaibinator/open-splunk/internal/privatefs"
	"golang.org/x/sys/unix"
)

var privateFileModes = []fs.FileMode{0o600}

const archiveSpecialMode = os.ModeSetuid | os.ModeSetgid | os.ModeSticky

// ArchiveObjectOwnershipPolicy is the exact UID, GID, and permission metadata
// for either the recovery root or one native archive. Mode excludes the object
// type but includes any required special bits using os.ModeSetuid,
// os.ModeSetgid, and os.ModeSticky.
type ArchiveObjectOwnershipPolicy struct {
	UID  int
	GID  int
	Mode fs.FileMode
}

// ArchiveOwnershipPolicy binds the separately mounted recovery root and every
// ClickHouse-owned native archive to one exact filesystem contract. Production
// uses a UID 101, GID 65532, mode 02750 root and UID 101, GID 65532, mode 0640
// archives; tests may explicitly use their own IDs and modes.
type ArchiveOwnershipPolicy struct {
	Root ArchiveObjectOwnershipPolicy
	File ArchiveObjectOwnershipPolicy
}

type archiveDirectory struct {
	path   string
	file   *os.File
	info   os.FileInfo
	policy ArchiveOwnershipPolicy
}

type inspectedFile struct {
	identity FileIdentity
	contents []byte
}

type pinnedArchive struct {
	name string
	file *os.File
	info os.FileInfo
}

func (archive *pinnedArchive) close() error {
	if archive == nil || archive.file == nil {
		return nil
	}
	file := archive.file
	archive.file = nil
	return file.Close()
}

func validateArchiveOwnershipPolicy(policy ArchiveOwnershipPolicy) error {
	if err := validateArchiveObjectIDs("root", policy.Root); err != nil {
		return err
	}
	if policy.Root.Mode != archiveMetadataMode(policy.Root.Mode) {
		return errors.New("deployment recovery-set archive root mode is invalid")
	}
	if err := validateArchiveObjectIDs("file", policy.File); err != nil {
		return err
	}
	if policy.File.Mode != 0o400 && policy.File.Mode != 0o440 &&
		policy.File.Mode != 0o600 && policy.File.Mode != 0o640 {
		return errors.New("deployment recovery-set archive mode is invalid")
	}
	return nil
}

func validateArchiveObjectIDs(
	object string,
	policy ArchiveObjectOwnershipPolicy,
) error {
	if policy.UID < 0 || policy.GID < 0 ||
		uint64(policy.UID) > math.MaxUint32 || uint64(policy.GID) > math.MaxUint32 {
		return fmt.Errorf("deployment recovery-set archive %s ownership is invalid", object)
	}
	return nil
}

func archiveMetadataMode(mode fs.FileMode) fs.FileMode {
	return mode.Perm() | mode&archiveSpecialMode
}

func openArchiveDirectory(
	path string,
	policy ArchiveOwnershipPolicy,
) (*archiveDirectory, error) {
	if err := validateArchiveOwnershipPolicy(policy); err != nil {
		return nil, err
	}
	if path == "" || path != strings.TrimSpace(path) || strings.IndexByte(path, 0) >= 0 ||
		!filepath.IsAbs(path) || filepath.Clean(path) != path {
		return nil, errors.New(
			"deployment recovery-set archive root must be a nonempty clean absolute path",
		)
	}
	before, err := os.Lstat(path)
	if err != nil {
		return nil, fmt.Errorf("open deployment recovery-set archive root: inspect path: %w", err)
	}
	if err := validateArchiveRoot(before, policy.Root); err != nil {
		return nil, fmt.Errorf("open deployment recovery-set archive root: %w", err)
	}
	// #nosec G304,G703 -- the exact absolute operator path is opened as a
	// directory, and O_NOFOLLOW rejects a redirected final component.
	fd, err := unix.Open(
		path,
		unix.O_RDONLY|unix.O_CLOEXEC|unix.O_DIRECTORY|unix.O_NOFOLLOW,
		0,
	)
	if err != nil {
		return nil, fmt.Errorf("open deployment recovery-set archive root: %w", err)
	}
	// #nosec G115 -- unix.Open returned a non-negative native descriptor.
	file := os.NewFile(uintptr(fd), path)
	if file == nil {
		_ = unix.Close(fd)
		return nil, errors.New("open deployment recovery-set archive root: invalid descriptor")
	}
	opened, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("open deployment recovery-set archive root: inspect descriptor: %w", err)
	}
	if err := validateArchiveRoot(opened, policy.Root); err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("open deployment recovery-set archive root: %w", err)
	}
	if !os.SameFile(before, opened) {
		_ = file.Close()
		return nil, errors.New("deployment recovery-set archive root changed while opening")
	}
	if err := privatefs.ValidateNoExtendedACL(file); err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("open deployment recovery-set archive root: %w", err)
	}
	directory := &archiveDirectory{
		path:   path,
		file:   file,
		info:   opened,
		policy: policy,
	}
	if err := directory.revalidate(); err != nil {
		_ = file.Close()
		return nil, err
	}
	return directory, nil
}

func validateArchiveRoot(
	info os.FileInfo,
	policy ArchiveObjectOwnershipPolicy,
) error {
	if info == nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return errors.New("deployment recovery-set archive root must be a real directory")
	}
	if archiveMetadataMode(info.Mode()) != policy.Mode {
		return errors.New("deployment recovery-set archive root mode differs from the ownership policy")
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || stat == nil {
		return errors.New("deployment recovery-set archive root ownership is unavailable")
	}
	if int64(stat.Uid) != int64(policy.UID) || int64(stat.Gid) != int64(policy.GID) {
		return errors.New("deployment recovery-set archive root ownership differs from the ownership policy")
	}
	return nil
}

func (directory *archiveDirectory) descriptor() (int, error) {
	if directory == nil || directory.file == nil {
		return -1, errors.New("deployment recovery-set archive root is closed")
	}
	// #nosec G115 -- os.File descriptors are native int descriptors here.
	return int(directory.file.Fd()), nil
}

func (directory *archiveDirectory) close() error {
	if directory == nil || directory.file == nil {
		return nil
	}
	file := directory.file
	directory.file = nil
	return file.Close()
}

func (directory *archiveDirectory) revalidate() error {
	if directory == nil || directory.file == nil {
		return errors.New("deployment recovery-set archive root is closed")
	}
	opened, err := directory.file.Stat()
	if err != nil {
		return fmt.Errorf("revalidate deployment recovery-set archive root: %w", err)
	}
	if err := validateArchiveRoot(opened, directory.policy.Root); err != nil {
		return fmt.Errorf("revalidate deployment recovery-set archive root: %w", err)
	}
	if err := privatefs.ValidateNoExtendedACL(directory.file); err != nil {
		return fmt.Errorf("revalidate deployment recovery-set archive root: %w", err)
	}
	current, err := os.Lstat(directory.path)
	if err != nil {
		return fmt.Errorf("revalidate deployment recovery-set archive root path: %w", err)
	}
	if err := validateArchiveRoot(current, directory.policy.Root); err != nil {
		return fmt.Errorf("revalidate deployment recovery-set archive root path: %w", err)
	}
	if !os.SameFile(directory.info, opened) || !os.SameFile(opened, current) {
		return errors.New("deployment recovery-set archive root identity changed")
	}
	return nil
}

func (directory *archiveDirectory) sync() error {
	if err := directory.revalidate(); err != nil {
		return err
	}
	if err := directory.file.Sync(); err != nil {
		return fmt.Errorf("sync deployment recovery-set archive root: %w", err)
	}
	return directory.revalidate()
}

func (directory *archiveDirectory) pathExists(name string) (bool, error) {
	if err := privatefs.ValidateComponent(name); err != nil {
		return false, err
	}
	if err := directory.revalidate(); err != nil {
		return false, err
	}
	fd, err := directory.descriptor()
	if err != nil {
		return false, err
	}
	var info unix.Stat_t
	err = unix.Fstatat(fd, name, &info, unix.AT_SYMLINK_NOFOLLOW)
	if errors.Is(err, unix.ENOENT) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("inspect deployment recovery-set archive name %q: %w", name, err)
	}
	return true, nil
}

func (directory *archiveDirectory) inspectArchive(
	ctx context.Context,
	name string,
	minimum uint64,
	maximum uint64,
) (FileIdentity, error) {
	return directory.inspectArchiveWithRetention(ctx, name, minimum, maximum, nil)
}

func (directory *archiveDirectory) inspectArchivePinned(
	ctx context.Context,
	name string,
	minimum uint64,
	maximum uint64,
) (identity FileIdentity, pinned *pinnedArchive, returnedErr error) {
	identity, returnedErr = directory.inspectArchiveWithRetention(
		ctx,
		name,
		minimum,
		maximum,
		func(archive *pinnedArchive) { pinned = archive },
	)
	if returnedErr != nil && pinned != nil {
		returnedErr = errors.Join(returnedErr, pinned.close())
		pinned = nil
	}
	return identity, pinned, returnedErr
}

func (directory *archiveDirectory) inspectArchiveWithRetention(
	ctx context.Context,
	name string,
	minimum uint64,
	maximum uint64,
	retain func(*pinnedArchive),
) (FileIdentity, error) {
	if ctx == nil {
		return FileIdentity{}, errors.New("inspect ClickHouse recovery archive: context is required")
	}
	if err := ctx.Err(); err != nil {
		return FileIdentity{}, err
	}
	if err := privatefs.ValidateComponent(name); err != nil {
		return FileIdentity{}, err
	}
	if minimum > maximum || maximum > math.MaxInt64 {
		return FileIdentity{}, errors.New("inspect ClickHouse recovery archive: invalid size policy")
	}
	// #nosec G115 -- both unsigned bounds were checked against MaxInt64 above.
	minimumSize := int64(minimum)
	// #nosec G115 -- both unsigned bounds were checked against MaxInt64 above.
	maximumSize := int64(maximum)
	if err := directory.revalidate(); err != nil {
		return FileIdentity{}, err
	}
	directoryFD, err := directory.descriptor()
	if err != nil {
		return FileIdentity{}, err
	}
	fd, err := unix.Openat(
		directoryFD,
		name,
		unix.O_RDONLY|unix.O_CLOEXEC|unix.O_NOFOLLOW|unix.O_NONBLOCK,
		0,
	)
	if err != nil {
		return FileIdentity{}, fmt.Errorf("open ClickHouse recovery archive %q: %w", name, err)
	}
	// #nosec G115 -- unix.Openat returned a non-negative native descriptor.
	file := os.NewFile(uintptr(fd), name)
	if file == nil {
		_ = unix.Close(fd)
		return FileIdentity{}, errors.New("open ClickHouse recovery archive: invalid descriptor")
	}
	closed := false
	defer func() {
		if !closed {
			_ = file.Close()
		}
	}()
	before, err := file.Stat()
	if err != nil {
		return FileIdentity{}, fmt.Errorf("inspect ClickHouse recovery archive %q: %w", name, err)
	}
	if err := validateArchiveFile(before, directory.policy.File, minimumSize, maximumSize); err != nil {
		return FileIdentity{}, fmt.Errorf("inspect ClickHouse recovery archive %q: %w", name, err)
	}
	if err := privatefs.ValidateNoExtendedACL(file); err != nil {
		return FileIdentity{}, fmt.Errorf("inspect ClickHouse recovery archive %q: %w", name, err)
	}
	if err := sameArchiveOpenPath(directoryFD, name, fd); err != nil {
		return FileIdentity{}, fmt.Errorf("inspect ClickHouse recovery archive %q: %w", name, err)
	}
	hash := sha256.New()
	read, err := privatefs.CopyBufferWithContext(
		ctx,
		hash,
		io.LimitReader(file, maximumSize+1),
		make([]byte, 64<<10),
	)
	if err != nil {
		return FileIdentity{}, fmt.Errorf("read ClickHouse recovery archive %q: %w", name, err)
	}
	if read < minimumSize || read > maximumSize || read != before.Size() {
		return FileIdentity{}, fmt.Errorf("ClickHouse recovery archive %q changed while read", name)
	}
	after, err := file.Stat()
	if err != nil {
		return FileIdentity{}, fmt.Errorf("reinspect ClickHouse recovery archive %q: %w", name, err)
	}
	if err := validateArchiveFile(
		after,
		directory.policy.File,
		minimumSize,
		maximumSize,
	); err != nil {
		return FileIdentity{}, fmt.Errorf("reinspect ClickHouse recovery archive %q: %w", name, err)
	}
	if err := privatefs.ValidateNoExtendedACL(file); err != nil {
		return FileIdentity{}, fmt.Errorf("reinspect ClickHouse recovery archive %q: %w", name, err)
	}
	if !sameFileState(before, after) {
		return FileIdentity{}, fmt.Errorf("ClickHouse recovery archive %q changed while read", name)
	}
	if err := ctx.Err(); err != nil {
		return FileIdentity{}, err
	}
	if err := file.Close(); err != nil {
		return FileIdentity{}, fmt.Errorf("close ClickHouse recovery archive %q: %w", name, err)
	}
	closed = true

	reopenedFD, err := unix.Openat(
		directoryFD,
		name,
		unix.O_RDONLY|unix.O_CLOEXEC|unix.O_NOFOLLOW|unix.O_NONBLOCK,
		0,
	)
	if err != nil {
		return FileIdentity{}, fmt.Errorf("reopen ClickHouse recovery archive %q: %w", name, err)
	}
	// #nosec G115 -- unix.Openat returned a non-negative native descriptor.
	reopened := os.NewFile(uintptr(reopenedFD), name)
	if reopened == nil {
		_ = unix.Close(reopenedFD)
		return FileIdentity{}, errors.New("reopen ClickHouse recovery archive: invalid descriptor")
	}
	reopenedClosed := false
	defer func() {
		if !reopenedClosed {
			_ = reopened.Close()
		}
	}()
	reopenedInfo, statErr := reopened.Stat()
	var metadataErr error
	var aclErr error
	if statErr == nil {
		metadataErr = validateArchiveFile(
			reopenedInfo,
			directory.policy.File,
			minimumSize,
			maximumSize,
		)
		aclErr = privatefs.ValidateNoExtendedACL(reopened)
	}
	pathErr := sameArchiveOpenPath(directoryFD, name, reopenedFD)
	var closeErr error
	if retain == nil {
		closeErr = reopened.Close()
		reopenedClosed = true
	}
	if err := errors.Join(statErr, metadataErr, aclErr, pathErr, closeErr); err != nil {
		return FileIdentity{}, fmt.Errorf("reinspect ClickHouse recovery archive %q: %w", name, err)
	}
	if !sameFileState(before, reopenedInfo) {
		return FileIdentity{}, fmt.Errorf("ClickHouse recovery archive %q changed after read", name)
	}
	if err := directory.revalidate(); err != nil {
		return FileIdentity{}, err
	}
	identity := FileIdentity{
		Name:      name,
		SizeBytes: uint64(read), // #nosec G115 -- bounded above and nonnegative.
		SHA256:    hex.EncodeToString(hash.Sum(nil)),
	}
	if retain != nil {
		retain(&pinnedArchive{name: name, file: reopened, info: reopenedInfo})
		reopenedClosed = true
	}
	return identity, nil
}

func validateArchiveFile(
	info os.FileInfo,
	policy ArchiveObjectOwnershipPolicy,
	minimum int64,
	maximum int64,
) error {
	if info == nil || !info.Mode().IsRegular() {
		return errors.New("archive must be a regular file")
	}
	if info.Mode().Perm() != policy.Mode ||
		info.Mode()&(os.ModeSetuid|os.ModeSetgid|os.ModeSticky) != 0 {
		return errors.New("archive permissions differ from the ownership policy")
	}
	if info.Size() < minimum || info.Size() > maximum {
		return errors.New("archive size is outside the supported bounds")
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || stat == nil {
		return errors.New("archive ownership and link metadata are unavailable")
	}
	if int64(stat.Uid) != int64(policy.UID) || int64(stat.Gid) != int64(policy.GID) {
		return errors.New("archive ownership differs from the ownership policy")
	}
	if stat.Nlink != 1 {
		return errors.New("archive must have exactly one hard link")
	}
	return nil
}

func sameArchiveOpenPath(directoryFD int, name string, openedFD int) error {
	var opened, current unix.Stat_t
	if err := unix.Fstat(openedFD, &opened); err != nil {
		return err
	}
	if err := unix.Fstatat(
		directoryFD,
		name,
		&current,
		unix.AT_SYMLINK_NOFOLLOW,
	); err != nil {
		return err
	}
	if opened.Dev != current.Dev || opened.Ino != current.Ino {
		return errors.New("archive name changed while opening")
	}
	return nil
}

func (directory *archiveDirectory) pinArchiveForRemoval(
	name string,
) (archive *pinnedArchive, returnedErr error) {
	if err := privatefs.ValidateComponent(name); err != nil {
		return nil, err
	}
	if err := directory.revalidate(); err != nil {
		return nil, err
	}
	directoryFD, err := directory.descriptor()
	if err != nil {
		return nil, err
	}
	fd, err := unix.Openat(
		directoryFD,
		name,
		unix.O_RDONLY|unix.O_CLOEXEC|unix.O_NOFOLLOW|unix.O_NONBLOCK,
		0,
	)
	if errors.Is(err, unix.ENOENT) {
		return nil, os.ErrNotExist
	}
	if err != nil {
		return nil, fmt.Errorf(
			"pin ClickHouse recovery archive %q for removal: %w",
			name,
			err,
		)
	}
	// #nosec G115 -- unix.Openat returned a non-negative native descriptor.
	file := os.NewFile(uintptr(fd), name)
	if file == nil {
		_ = unix.Close(fd)
		return nil, errors.New(
			"pin ClickHouse recovery archive for removal: invalid descriptor",
		)
	}
	defer func() {
		if archive == nil {
			returnedErr = errors.Join(returnedErr, file.Close())
		}
	}()

	before, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf(
			"pin ClickHouse recovery archive %q for removal: inspect descriptor: %w",
			name,
			err,
		)
	}
	if err := validateArchiveFile(
		before,
		directory.policy.File,
		0,
		int64(maximumClickHouseArchiveBytes),
	); err != nil {
		return nil, fmt.Errorf(
			"pin ClickHouse recovery archive %q for removal: %w",
			name,
			err,
		)
	}
	if err := privatefs.ValidateNoExtendedACL(file); err != nil {
		return nil, fmt.Errorf(
			"pin ClickHouse recovery archive %q for removal: %w",
			name,
			err,
		)
	}
	if err := sameArchiveOpenPath(directoryFD, name, fd); err != nil {
		return nil, fmt.Errorf(
			"pin ClickHouse recovery archive %q for removal: %w",
			name,
			err,
		)
	}
	stable, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf(
			"pin ClickHouse recovery archive %q for removal: reinspect descriptor: %w",
			name,
			err,
		)
	}
	if err := validateArchiveFile(
		stable,
		directory.policy.File,
		0,
		int64(maximumClickHouseArchiveBytes),
	); err != nil {
		return nil, fmt.Errorf(
			"pin ClickHouse recovery archive %q for removal: %w",
			name,
			err,
		)
	}
	if err := privatefs.ValidateNoExtendedACL(file); err != nil {
		return nil, fmt.Errorf(
			"pin ClickHouse recovery archive %q for removal: %w",
			name,
			err,
		)
	}
	if !sameFileState(before, stable) {
		return nil, fmt.Errorf(
			"pin ClickHouse recovery archive %q for removal: metadata changed",
			name,
		)
	}
	if err := directory.revalidate(); err != nil {
		return nil, err
	}
	if err := sameArchiveOpenPath(directoryFD, name, fd); err != nil {
		return nil, fmt.Errorf(
			"pin ClickHouse recovery archive %q for removal: revalidate name: %w",
			name,
			err,
		)
	}
	archive = &pinnedArchive{name: name, file: file, info: stable}
	return archive, nil
}

func (directory *archiveDirectory) requirePinnedArchive(
	name string,
	archive *pinnedArchive,
) (int, error) {
	if err := privatefs.ValidateComponent(name); err != nil {
		return -1, err
	}
	if archive == nil || archive.file == nil {
		return -1, errors.New("pinned ClickHouse recovery archive is required")
	}
	if archive.name != name {
		return -1, errors.New("pinned ClickHouse recovery archive name differs")
	}
	if err := directory.revalidate(); err != nil {
		return -1, err
	}
	directoryFD, err := directory.descriptor()
	if err != nil {
		return -1, err
	}
	current, err := archive.file.Stat()
	if err != nil {
		return -1, fmt.Errorf("inspect pinned ClickHouse recovery archive %q: %w", name, err)
	}
	if err := validateArchiveFile(
		current,
		directory.policy.File,
		0,
		int64(maximumClickHouseArchiveBytes),
	); err != nil {
		return -1, fmt.Errorf("inspect pinned ClickHouse recovery archive %q: %w", name, err)
	}
	if err := privatefs.ValidateNoExtendedACL(archive.file); err != nil {
		return -1, fmt.Errorf("inspect pinned ClickHouse recovery archive %q: %w", name, err)
	}
	if !sameFileState(archive.info, current) {
		return -1, fmt.Errorf("pinned ClickHouse recovery archive %q changed", name)
	}
	// #nosec G115 -- os.File descriptors are native int descriptors on the
	// supported Unix targets.
	archiveFD := int(archive.file.Fd())
	if err := sameArchiveOpenPath(directoryFD, name, archiveFD); err != nil {
		return -1, fmt.Errorf(
			"pinned ClickHouse recovery archive %q no longer owns its name: %w",
			name,
			err,
		)
	}
	if err := directory.revalidate(); err != nil {
		return -1, err
	}
	if err := sameArchiveOpenPath(directoryFD, name, archiveFD); err != nil {
		return -1, fmt.Errorf(
			"pinned ClickHouse recovery archive %q no longer owns its name: %w",
			name,
			err,
		)
	}
	return directoryFD, nil
}

func (directory *archiveDirectory) removePinnedArchive(
	name string,
	archive *pinnedArchive,
) (bool, error) {
	directoryFD, err := directory.requirePinnedArchive(name, archive)
	if err != nil {
		return false, err
	}
	// The path-identity proof is deliberately the last potentially blocking
	// operation before unlink. Production additionally requires ClickHouse to
	// be stopped because POSIX has no unlink-by-file-descriptor primitive.
	// #nosec G115 -- os.File descriptors are native int descriptors on the
	// supported Unix targets.
	if err := sameArchiveOpenPath(directoryFD, name, int(archive.file.Fd())); err != nil {
		return false, fmt.Errorf(
			"clean staged ClickHouse recovery archive %q: revalidate pinned name: %w",
			name,
			err,
		)
	}
	if err := unix.Unlinkat(directoryFD, name, 0); err != nil {
		return false, fmt.Errorf(
			"clean staged ClickHouse recovery archive %q: %w",
			name,
			err,
		)
	}
	syncErr := directory.sync()
	exists, absenceErr := directory.pathExists(name)
	if absenceErr == nil && exists {
		absenceErr = fmt.Errorf(
			"clean staged ClickHouse recovery archive %q: name was recreated after unlink",
			name,
		)
	}
	return true, errors.Join(syncErr, absenceErr)
}

// removeKnownArchive is reserved for the explicit confirmation-bound cleanup
// path. Normal recovery-set creation instead retains the descriptor returned by
// inspectArchivePinned and calls removePinnedArchive with that attempt-owned
// handle.
func (directory *archiveDirectory) removeKnownArchive(
	name string,
) (removed bool, returnedErr error) {
	archive, err := directory.pinArchiveForRemoval(name)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	defer func() { returnedErr = errors.Join(returnedErr, archive.close()) }()
	return directory.removePinnedArchive(name, archive)
}

func inspectPrivateFile(
	ctx context.Context,
	directory *privatefs.Directory,
	name string,
	minimum int64,
	maximum int64,
	retain bool,
) (inspectedFile, error) {
	if ctx == nil {
		return inspectedFile{}, errors.New("inspect recovery-set member: context is required")
	}
	if err := ctx.Err(); err != nil {
		return inspectedFile{}, err
	}
	policy := privatefs.FilePolicy{
		AllowedModes: privateFileModes,
		MinimumSize:  minimum,
		MaximumSize:  maximum,
	}
	inspected, err := privatefs.InspectStableRegularFile(
		ctx,
		directory,
		name,
		policy,
		retain,
	)
	if err != nil {
		return inspectedFile{}, err
	}
	return inspectedFile{
		identity: FileIdentity{
			Name:      name,
			SizeBytes: inspected.SizeBytes,
			SHA256:    hex.EncodeToString(inspected.SHA256[:]),
		},
		contents: inspected.Contents,
	}, nil
}

func writePrivateFile(
	ctx context.Context,
	directory *privatefs.Directory,
	name string,
	contents []byte,
) error {
	if ctx == nil {
		return errors.New("write recovery-set member: context is required")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	_, err := privatefs.WriteStableRegularFile(ctx, directory, name, contents)
	return err
}

func sameFileState(left, right os.FileInfo) bool {
	return left != nil && right != nil &&
		os.SameFile(left, right) &&
		left.Mode() == right.Mode() &&
		left.Size() == right.Size() &&
		left.ModTime().Equal(right.ModTime())
}
