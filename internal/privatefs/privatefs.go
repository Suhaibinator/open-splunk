//go:build darwin || linux

package privatefs

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"math"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"syscall"
	"unicode/utf8"

	"fortio.org/safecast"
	"golang.org/x/sys/unix"
)

const (
	// MaximumComponentBytes is the conservative component ceiling used by
	// private publication formats on both production filesystems.
	MaximumComponentBytes = 255
	maximumNameAttempts   = 16
)

var (
	ErrClosed              = errors.New("private filesystem directory is closed")
	ErrDestinationExists   = errors.New("private filesystem destination already exists")
	ErrUnsupportedPlatform = errors.New("private filesystem operation is unsupported on this platform")
)

// NameGenerator returns one candidate exact path component. RandomName is the
// production generator; accepting an injected generator keeps collision and
// interruption behavior deterministic in tests and higher-level recovery
// protocols.
type NameGenerator func() (string, error)

// FilePolicy is the complete metadata and size contract for an opened file.
// AllowedModes must contain at least one exact permission mode. MaximumSize is
// inclusive, so callers can explicitly admit an empty file with a zero bound.
type FilePolicy struct {
	AllowedModes []fs.FileMode
	MinimumSize  int64
	MaximumSize  int64
}

// Directory is an owner-private 0700 directory pinned by an open descriptor.
// Its configured path is retained only to prove that the name still resolves
// to the opened inode; child operations themselves are descriptor-relative.
type Directory struct {
	path string
	file *os.File
	info os.FileInfo
}

// ValidateComponent accepts a single canonical, portable ASCII component.
// Fixed backup member names and randomized staging names therefore cannot gain
// alternate spellings on case-insensitive or Unicode-normalizing filesystems.
func ValidateComponent(name string) error {
	if name == "" || name == "." || name == ".." {
		return errors.New("private filesystem name must be a non-dot component")
	}
	if len(name) > MaximumComponentBytes {
		return fmt.Errorf(
			"private filesystem name exceeds %d bytes",
			MaximumComponentBytes,
		)
	}
	if !utf8.ValidString(name) {
		return errors.New("private filesystem name must be valid UTF-8")
	}
	for index := range len(name) {
		character := name[index]
		if (character >= 'a' && character <= 'z') ||
			(character >= 'A' && character <= 'Z') ||
			(character >= '0' && character <= '9') ||
			character == '.' || character == '_' || character == '-' {
			continue
		}
		return errors.New(
			"private filesystem name must contain only ASCII letters, digits, dot, underscore, or hyphen",
		)
	}
	return nil
}

// RandomName returns a concurrency-safe CSPRNG-backed component generator.
// The prefix is revalidated together with every complete generated name.
func RandomName(prefix string) NameGenerator {
	return func() (string, error) {
		var randomBytes [16]byte
		if _, err := rand.Read(randomBytes[:]); err != nil {
			return "", errors.New("generate private filesystem temporary name: secure randomness unavailable")
		}
		name := prefix + hex.EncodeToString(randomBytes[:])
		clear(randomBytes[:])
		if err := ValidateComponent(name); err != nil {
			return "", err
		}
		return name, nil
	}
}

// SameFileState reports that two stat results describe the same inode with
// unchanged mode, size, and modification time.
func SameFileState(left, right os.FileInfo) bool {
	return left != nil && right != nil &&
		os.SameFile(left, right) &&
		left.Mode() == right.Mode() &&
		left.Size() == right.Size() &&
		left.ModTime().Equal(right.ModTime())
}

// OpenValidatedDirectory opens an already-cleaned absolute path as a pinned
// directory descriptor, validating the pre-image and the opened inode with the
// caller's predicate and rejecting any change across the open. The subject
// names the directory in error messages. The caller owns the returned
// descriptor and must close it.
func OpenValidatedDirectory(
	path string,
	subject string,
	validate func(os.FileInfo) error,
) (*os.File, os.FileInfo, error) {
	before, err := os.Lstat(path)
	if err != nil {
		return nil, nil, fmt.Errorf("open %s: inspect path: %w", subject, err)
	}
	if err := validate(before); err != nil {
		return nil, nil, fmt.Errorf("open %s: %w", subject, err)
	}

	// read-only, and O_NOFOLLOW rejects a redirected final component.
	fd, err := unix.Open(
		path,
		unix.O_RDONLY|unix.O_CLOEXEC|unix.O_DIRECTORY|unix.O_NOFOLLOW,
		0,
	)
	if err != nil {
		return nil, nil, fmt.Errorf("open %s: open path: %w", subject, err)
	}

	file := os.NewFile(uintptr(fd), path)
	if file == nil {
		_ = unix.Close(fd)
		return nil, nil, errors.New("open " + subject + ": invalid descriptor")
	}
	opened, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return nil, nil, fmt.Errorf("open %s: inspect descriptor: %w", subject, err)
	}
	if !os.SameFile(before, opened) {
		_ = file.Close()
		return nil, nil, errors.New("open " + subject + ": path changed while opening")
	}
	if err := validate(opened); err != nil {
		_ = file.Close()
		return nil, nil, fmt.Errorf("open %s: %w", subject, err)
	}
	if err := validateNoExtendedACL(file); err != nil {
		_ = file.Close()
		return nil, nil, fmt.Errorf("open %s: %w", subject, err)
	}
	return file, opened, nil
}

// OpenDirectory pins an existing absolute owner-private 0700 directory. The
// final path component may not be a symlink, and the path is rechecked after
// descriptor and ACL validation.
func OpenDirectory(path string) (*Directory, error) {
	if strings.IndexByte(path, 0) >= 0 {
		return nil, errors.New("open private directory: path contains a NUL byte")
	}
	if !filepath.IsAbs(path) {
		return nil, errors.New("open private directory: path must be absolute")
	}
	path = filepath.Clean(path)
	file, opened, err := OpenValidatedDirectory(path, "private directory", func(info os.FileInfo) error {
		return validateOwnedDirectory(info, os.Geteuid())
	})
	if err != nil {
		return nil, err
	}
	directory := &Directory{path: path, file: file, info: opened}
	if err := directory.Revalidate(); err != nil {
		_ = file.Close()
		return nil, err
	}
	return directory, nil
}

// SecureDirectory atomically tightens an existing, effective-user-owned
// directory to mode 0700 through a pinned descriptor. The final path component
// may not be a symlink, and the name must still resolve to the same inode after
// the mode change.
func SecureDirectory(path string) error {
	if strings.IndexByte(path, 0) >= 0 {
		return errors.New("secure private directory: path contains a NUL byte")
	}
	if !filepath.IsAbs(path) {
		return errors.New("secure private directory: path must be absolute")
	}
	path = filepath.Clean(path)
	validateOwner := func(info os.FileInfo) error {
		if info == nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
			return errors.New("path must be a real directory")
		}
		if hasSpecialMode(info.Mode()) {
			return errors.New("directory must not have special permission bits")
		}
		stat, ok := info.Sys().(*syscall.Stat_t)
		if !ok || stat == nil || int64(stat.Uid) != int64(os.Geteuid()) {
			return errors.New("directory must be owned by the effective user")
		}
		return nil
	}
	file, _, err := OpenValidatedDirectory(path, "private directory", validateOwner)
	if err != nil {
		return err
	}
	defer func() { _ = file.Close() }()
	if err := unix.Fchmod(safecast.MustConv[int](file.Fd()), 0o700); err != nil {
		return fmt.Errorf("secure private directory: change mode: %w", err)
	}
	opened, err := file.Stat()
	if err != nil {
		return fmt.Errorf("secure private directory: inspect descriptor: %w", err)
	}
	if err := validateOwnedDirectory(opened, os.Geteuid()); err != nil {
		return fmt.Errorf("secure private directory: %w", err)
	}
	current, err := os.Lstat(path)
	if err != nil {
		return fmt.Errorf("secure private directory: re-inspect path: %w", err)
	}
	if !os.SameFile(opened, current) {
		return errors.New("secure private directory: path changed while securing")
	}
	return nil
}

func validateOwnedDirectory(info os.FileInfo, effectiveUID int) error {
	if info == nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return errors.New("path must be a real directory")
	}
	if info.Mode().Perm() != 0o700 {
		return errors.New("directory permissions must be exactly 0700")
	}
	if hasSpecialMode(info.Mode()) {
		return errors.New("directory must not have special permission bits")
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || stat == nil {
		return errors.New("directory ownership is unavailable")
	}
	if effectiveUID < 0 || int64(stat.Uid) != int64(effectiveUID) {
		return errors.New("directory must be owned by the effective user")
	}
	return nil
}

func hasSpecialMode(mode fs.FileMode) bool {
	return mode&(os.ModeSetuid|os.ModeSetgid|os.ModeSticky) != 0
}

func (directory *Directory) descriptor() (int, error) {
	if directory == nil || directory.file == nil {
		return -1, ErrClosed
	}

	// supported Unix targets.
	return int(directory.file.Fd()), nil
}

// Path returns the cleaned absolute path whose identity Revalidate protects.
func (directory *Directory) Path() string {
	if directory == nil {
		return ""
	}
	return directory.path
}

// Close releases the pinned descriptor. It is idempotent.
func (directory *Directory) Close() error {
	if directory == nil || directory.file == nil {
		return nil
	}
	file := directory.file
	directory.file = nil
	return file.Close()
}

// PinnedInfo returns metadata for the exact descriptor-backed directory
// without requiring its original pathname to remain present. This is useful
// across an intentional directory rename while still rejecting descriptor,
// ownership, mode, or ACL changes.
func (directory *Directory) PinnedInfo() (os.FileInfo, error) {
	if directory == nil || directory.file == nil {
		return nil, ErrClosed
	}
	opened, err := directory.file.Stat()
	if err != nil {
		return nil, fmt.Errorf("inspect pinned private directory descriptor: %w", err)
	}
	if !os.SameFile(directory.info, opened) {
		return nil, errors.New("pinned private directory descriptor identity changed")
	}
	if err := validateOwnedDirectory(opened, os.Geteuid()); err != nil {
		return nil, fmt.Errorf("inspect pinned private directory: %w", err)
	}
	if err := validateNoExtendedACL(directory.file); err != nil {
		return nil, fmt.Errorf("inspect pinned private directory: %w", err)
	}
	return opened, nil
}

// Revalidate proves that the configured pathname still resolves to the
// pinned, owner-private directory. Directory modification times and link
// counts intentionally are not frozen because child publication changes them.
func (directory *Directory) Revalidate() error {
	opened, err := directory.PinnedInfo()
	if err != nil {
		return fmt.Errorf("revalidate private directory: %w", err)
	}
	current, err := os.Lstat(directory.path)
	if err != nil {
		return fmt.Errorf("revalidate private directory: inspect path: %w", err)
	}
	if !os.SameFile(opened, current) {
		return errors.New("revalidate private directory: path no longer names the pinned directory")
	}
	if err := validateOwnedDirectory(current, os.Geteuid()); err != nil {
		return fmt.Errorf("revalidate private directory: %w", err)
	}
	return nil
}

// Sync flushes directory metadata and verifies the pinned name on both sides
// of the durability boundary.
func (directory *Directory) Sync() error {
	if err := directory.Revalidate(); err != nil {
		return err
	}
	if err := directory.file.Sync(); err != nil {
		return fmt.Errorf("sync private directory: %w", err)
	}
	return directory.Revalidate()
}

// SyncFile flushes a temporary or opened regular file. Callers remain
// responsible for closing it before publication.
func SyncFile(file *os.File) error {
	if file == nil {
		return errors.New("sync private file: file is missing")
	}
	if err := file.Sync(); err != nil {
		return fmt.Errorf("sync private file: %w", err)
	}
	return nil
}

func validateFilePolicy(policy FilePolicy) error {
	if len(policy.AllowedModes) == 0 {
		return errors.New("private file policy requires at least one allowed mode")
	}
	if policy.MinimumSize < 0 || policy.MaximumSize < policy.MinimumSize {
		return errors.New("private file policy has an invalid size range")
	}
	for _, mode := range policy.AllowedModes {
		if mode != mode.Perm() || hasSpecialMode(mode) {
			return errors.New("private file policy contains an invalid mode")
		}
	}
	return nil
}

func validateOwnedRegularFile(
	info os.FileInfo,
	policy FilePolicy,
	effectiveUID int,
) error {
	if err := validateFilePolicy(policy); err != nil {
		return err
	}
	if info == nil || !info.Mode().IsRegular() {
		return errors.New("private file must be a regular file")
	}
	if hasSpecialMode(info.Mode()) {
		return errors.New("private file must not have special permission bits")
	}
	if !slices.Contains(policy.AllowedModes, info.Mode().Perm()) {
		return errors.New("private file permissions are not allowed")
	}
	if info.Size() < policy.MinimumSize || info.Size() > policy.MaximumSize {
		return errors.New("private file size is outside the allowed range")
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || stat == nil {
		return errors.New("private file ownership and link metadata are unavailable")
	}
	if effectiveUID < 0 || int64(stat.Uid) != int64(effectiveUID) {
		return errors.New("private file must be owned by the effective user")
	}
	if stat.Nlink != 1 {
		return errors.New("private file must have exactly one hard link")
	}
	return nil
}

// OpenRegular opens one exact child without following it and validates its
// current owner, mode, size, link count, and extended ACL.
func (directory *Directory) OpenRegular(
	name string,
	policy FilePolicy,
) (*os.File, error) {
	if err := ValidateComponent(name); err != nil {
		return nil, err
	}
	if err := validateFilePolicy(policy); err != nil {
		return nil, err
	}
	if err := directory.Revalidate(); err != nil {
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
	if err != nil {
		return nil, fmt.Errorf("open private file %q: %w", name, err)
	}

	file := os.NewFile(uintptr(fd), name)
	if file == nil {
		_ = unix.Close(fd)
		return nil, fmt.Errorf("open private file %q: invalid descriptor", name)
	}
	info, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("open private file %q: inspect descriptor: %w", name, err)
	}
	if err := validateOwnedRegularFile(info, policy, os.Geteuid()); err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("open private file %q: %w", name, err)
	}
	if err := validateNoExtendedACL(file); err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("open private file %q: %w", name, err)
	}
	if err := sameOpenPath(directoryFD, name, fd); err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("open private file %q: %w", name, err)
	}
	stableInfo, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("open private file %q: reinspect descriptor: %w", name, err)
	}
	if err := validateOwnedRegularFile(stableInfo, policy, os.Geteuid()); err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("open private file %q: %w", name, err)
	}
	if err := validateNoExtendedACL(file); err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("open private file %q: %w", name, err)
	}
	if err := directory.Revalidate(); err != nil {
		_ = file.Close()
		return nil, err
	}
	return file, nil
}

func sameOpenPath(directoryFD int, name string, openedFD int) error {
	var opened, current unix.Stat_t
	if err := unix.Fstat(openedFD, &opened); err != nil {
		return fmt.Errorf("inspect open descriptor: %w", err)
	}
	if err := unix.Fstatat(
		directoryFD,
		name,
		&current,
		unix.AT_SYMLINK_NOFOLLOW,
	); err != nil {
		return fmt.Errorf("reinspect child name: %w", err)
	}
	if opened.Dev != current.Dev || opened.Ino != current.Ino {
		return errors.New("child name changed while opening")
	}
	return nil
}

func nextTemporaryName(generator NameGenerator) (string, error) {
	if generator == nil {
		return "", errors.New("private filesystem temporary name generator is required")
	}
	name, err := generator()
	if err != nil {
		return "", err
	}
	if err := ValidateComponent(name); err != nil {
		return "", err
	}
	return name, nil
}

// CreateTemporaryFile exclusively creates one owner-private 0600 regular file
// relative to the pinned directory. Name collisions are retried without ever
// inspecting or replacing the colliding object.
func (directory *Directory) CreateTemporaryFile(
	generator NameGenerator,
) (string, *os.File, error) {
	if err := directory.Revalidate(); err != nil {
		return "", nil, err
	}
	directoryFD, err := directory.descriptor()
	if err != nil {
		return "", nil, err
	}
	for range maximumNameAttempts {
		name, nameErr := nextTemporaryName(generator)
		if nameErr != nil {
			return "", nil, nameErr
		}
		fd, openErr := unix.Openat(
			directoryFD,
			name,
			unix.O_RDWR|unix.O_CREAT|unix.O_EXCL|unix.O_CLOEXEC|unix.O_NOFOLLOW,
			0o600,
		)
		if errors.Is(openErr, unix.EEXIST) {
			continue
		}
		if openErr != nil {
			return "", nil, fmt.Errorf("create private temporary file %q: %w", name, openErr)
		}

		file := os.NewFile(uintptr(fd), name)
		if file == nil {
			_ = unix.Close(fd)
			_ = unix.Unlinkat(directoryFD, name, 0)
			return "", nil, errors.New("create private temporary file: invalid descriptor")
		}
		cleanup := func() {
			_ = file.Close()
			_ = unix.Unlinkat(directoryFD, name, 0)
		}
		if chmodErr := unix.Fchmod(fd, 0o600); chmodErr != nil {
			cleanup()
			return "", nil, fmt.Errorf("secure private temporary file %q: %w", name, chmodErr)
		}
		info, statErr := file.Stat()
		if statErr != nil {
			cleanup()
			return "", nil, fmt.Errorf("inspect private temporary file %q: %w", name, statErr)
		}
		policy := FilePolicy{AllowedModes: []fs.FileMode{0o600}, MaximumSize: 0}
		if validateErr := validateOwnedRegularFile(info, policy, os.Geteuid()); validateErr != nil {
			cleanup()
			return "", nil, fmt.Errorf("inspect private temporary file %q: %w", name, validateErr)
		}
		if aclErr := validateNoExtendedACL(file); aclErr != nil {
			cleanup()
			return "", nil, fmt.Errorf("inspect private temporary file %q: %w", name, aclErr)
		}
		if pathErr := sameOpenPath(directoryFD, name, fd); pathErr != nil {
			cleanup()
			return "", nil, fmt.Errorf("inspect private temporary file %q: %w", name, pathErr)
		}
		stableInfo, statErr := file.Stat()
		if statErr != nil {
			cleanup()
			return "", nil, fmt.Errorf("reinspect private temporary file %q: %w", name, statErr)
		}
		if validateErr := validateOwnedRegularFile(
			stableInfo,
			policy,
			os.Geteuid(),
		); validateErr != nil {
			cleanup()
			return "", nil, fmt.Errorf("reinspect private temporary file %q: %w", name, validateErr)
		}
		if stableErr := directory.Revalidate(); stableErr != nil {
			cleanup()
			return "", nil, stableErr
		}
		return name, file, nil
	}
	return "", nil, fmt.Errorf(
		"create private temporary file: all %d candidate names exist",
		maximumNameAttempts,
	)
}

// CreateTemporaryDirectory exclusively creates and pins one owner-private
// 0700 child directory.
func (directory *Directory) CreateTemporaryDirectory(
	generator NameGenerator,
) (string, *Directory, error) {
	if err := directory.Revalidate(); err != nil {
		return "", nil, err
	}
	directoryFD, err := directory.descriptor()
	if err != nil {
		return "", nil, err
	}
	for range maximumNameAttempts {
		name, nameErr := nextTemporaryName(generator)
		if nameErr != nil {
			return "", nil, nameErr
		}
		mkdirErr := unix.Mkdirat(directoryFD, name, 0o700)
		if errors.Is(mkdirErr, unix.EEXIST) {
			continue
		}
		if mkdirErr != nil {
			return "", nil, fmt.Errorf("create private temporary directory %q: %w", name, mkdirErr)
		}
		child, openErr := directory.openChildDirectory(name, true)
		if openErr != nil {
			_ = unix.Unlinkat(directoryFD, name, unix.AT_REMOVEDIR)
			return "", nil, openErr
		}
		if stableErr := directory.Revalidate(); stableErr != nil {
			_ = child.Close()
			_ = unix.Unlinkat(directoryFD, name, unix.AT_REMOVEDIR)
			return "", nil, stableErr
		}
		return name, child, nil
	}
	return "", nil, fmt.Errorf(
		"create private temporary directory: all %d candidate names exist",
		maximumNameAttempts,
	)
}

// RequirePinnedChildDirectory proves that name still identifies the exact
// owner-private child descriptor supplied by the caller. The child descriptor
// must have been opened beneath this parent and must remain open for the whole
// operation that relies on the proof.
func (directory *Directory) RequirePinnedChildDirectory(
	name string,
	child *Directory,
) error {
	if err := ValidateComponent(name); err != nil {
		return err
	}
	if child == nil {
		return errors.New("require pinned private child directory: child is required")
	}
	if err := directory.Revalidate(); err != nil {
		return err
	}
	if child.Path() != filepath.Join(directory.Path(), name) {
		return errors.New(
			"require pinned private child directory: child was not opened at the expected name",
		)
	}
	if err := child.Revalidate(); err != nil {
		return err
	}
	directoryFD, err := directory.descriptor()
	if err != nil {
		return err
	}
	childFD, err := child.descriptor()
	if err != nil {
		return err
	}
	if err := sameOpenPath(directoryFD, name, childFD); err != nil {
		return fmt.Errorf("require pinned private child directory %q: %w", name, err)
	}
	if err := directory.Revalidate(); err != nil {
		return err
	}
	return sameOpenPath(directoryFD, name, childFD)
}

func (directory *Directory) openChildDirectory(
	name string,
	secureNewDirectory bool,
) (*Directory, error) {
	directoryFD, err := directory.descriptor()
	if err != nil {
		return nil, err
	}
	fd, err := unix.Openat(
		directoryFD,
		name,
		unix.O_RDONLY|unix.O_CLOEXEC|unix.O_DIRECTORY|unix.O_NOFOLLOW,
		0,
	)
	if err != nil {
		return nil, fmt.Errorf("open private child directory %q: %w", name, err)
	}

	file := os.NewFile(uintptr(fd), name)
	if file == nil {
		_ = unix.Close(fd)
		return nil, fmt.Errorf("open private child directory %q: invalid descriptor", name)
	}
	cleanup := func() { _ = file.Close() }
	if secureNewDirectory {
		if err := unix.Fchmod(fd, 0o700); err != nil {
			cleanup()
			return nil, fmt.Errorf("secure private child directory %q: %w", name, err)
		}
	}
	info, err := file.Stat()
	if err != nil {
		cleanup()
		return nil, fmt.Errorf("inspect private child directory %q: %w", name, err)
	}
	if err := validateOwnedDirectory(info, os.Geteuid()); err != nil {
		cleanup()
		return nil, fmt.Errorf("inspect private child directory %q: %w", name, err)
	}
	if err := validateNoExtendedACL(file); err != nil {
		cleanup()
		return nil, fmt.Errorf("inspect private child directory %q: %w", name, err)
	}
	if err := sameOpenPath(directoryFD, name, fd); err != nil {
		cleanup()
		return nil, fmt.Errorf("inspect private child directory %q: %w", name, err)
	}
	child := &Directory{
		path: filepath.Join(directory.path, name),
		file: file,
		info: info,
	}
	if err := child.Revalidate(); err != nil {
		cleanup()
		return nil, err
	}
	return child, nil
}

// List returns sorted exact component names and refuses to examine more than
// maximum entries. A fresh descriptor prevents prior calls from sharing a
// directory stream offset.
func (directory *Directory) List(maximum int) ([]string, error) {
	if maximum < 0 || maximum == math.MaxInt {
		return nil, errors.New("list private directory: maximum is outside the valid range")
	}
	if err := directory.Revalidate(); err != nil {
		return nil, err
	}
	directoryFD, err := directory.descriptor()
	if err != nil {
		return nil, err
	}
	fd, err := unix.Openat(
		directoryFD,
		".",
		unix.O_RDONLY|unix.O_CLOEXEC|unix.O_DIRECTORY|unix.O_NOFOLLOW,
		0,
	)
	if err != nil {
		return nil, fmt.Errorf("list private directory: open stream: %w", err)
	}

	stream := os.NewFile(uintptr(fd), directory.path)
	if stream == nil {
		_ = unix.Close(fd)
		return nil, errors.New("list private directory: invalid stream descriptor")
	}
	entries, readErr := stream.ReadDir(maximum + 1)
	closeErr := stream.Close()
	if readErr != nil && !errors.Is(readErr, io.EOF) {
		return nil, fmt.Errorf("list private directory: read stream: %w", readErr)
	}
	if closeErr != nil {
		return nil, fmt.Errorf("list private directory: close stream: %w", closeErr)
	}
	if len(entries) > maximum {
		return nil, fmt.Errorf("list private directory: contains more than %d entries", maximum)
	}
	names := make([]string, 0, len(entries))
	seen := make(map[string]struct{}, len(entries))
	for _, entry := range entries {
		name := entry.Name()
		if err := ValidateComponent(name); err != nil {
			return nil, fmt.Errorf("list private directory: invalid entry name: %w", err)
		}
		if _, duplicate := seen[name]; duplicate {
			return nil, errors.New("list private directory: duplicate entry name")
		}
		seen[name] = struct{}{}
		names = append(names, name)
	}
	slices.Sort(names)
	if err := directory.Revalidate(); err != nil {
		return nil, err
	}
	return names, nil
}

// RequireEntries verifies the complete exact entry set under one bounded
// directory scan.
func (directory *Directory) RequireEntries(
	expected []string,
	maximum int,
) error {
	if len(expected) > maximum {
		return errors.New("require private directory entries: expected set exceeds maximum")
	}
	want := slices.Clone(expected)
	for _, name := range want {
		if err := ValidateComponent(name); err != nil {
			return err
		}
	}
	slices.Sort(want)
	for index := 1; index < len(want); index++ {
		if want[index-1] == want[index] {
			return errors.New("require private directory entries: duplicate expected name")
		}
	}
	got, err := directory.List(maximum)
	if err != nil {
		return err
	}
	if !slices.Equal(got, want) {
		return fmt.Errorf("require private directory entries: got %v, want %v", got, want)
	}
	return nil
}

// RequirePinnedRegular proves that name still identifies the exact open
// regular-file descriptor supplied by the caller. Callers retain ownership of
// the descriptor and must keep it open until their descriptor-relative
// operation has finished.
func (directory *Directory) RequirePinnedRegular(name string, file *os.File) error {
	if err := ValidateComponent(name); err != nil {
		return err
	}
	if file == nil {
		return errors.New("require pinned private regular file: file is required")
	}
	if err := directory.Revalidate(); err != nil {
		return err
	}
	info, err := file.Stat()
	if err != nil {
		return fmt.Errorf("require pinned private regular file %q: %w", name, err)
	}
	if !info.Mode().IsRegular() {
		return fmt.Errorf("require pinned private regular file %q: descriptor is not regular", name)
	}
	directoryFD, err := directory.descriptor()
	if err != nil {
		return err
	}

	// supported Unix targets.
	fileFD := safecast.MustConv[int](file.Fd())
	if err := sameOpenPath(directoryFD, name, fileFD); err != nil {
		return fmt.Errorf("require pinned private regular file %q: %w", name, err)
	}
	if err := directory.Revalidate(); err != nil {
		return err
	}
	if err := sameOpenPath(directoryFD, name, fileFD); err != nil {
		return fmt.Errorf("require pinned private regular file %q: %w", name, err)
	}
	return nil
}

// UnlinkPinnedRegular removes name only while it still identifies the exact
// live regular-file descriptor supplied by the caller. The descriptor remains
// open and owned by the caller after this method returns.
func (directory *Directory) UnlinkPinnedRegular(name string, file *os.File) error {
	if err := directory.RequirePinnedRegular(name, file); err != nil {
		return err
	}
	directoryFD, err := directory.descriptor()
	if err != nil {
		return err
	}
	// Keep the pathname comparison immediately adjacent to unlink. POSIX has no
	// unlink-by-file-descriptor primitive; owner-private parents and the
	// deployment singleton lock exclude supported concurrent writers.

	// supported Unix targets.
	if err := sameOpenPath(directoryFD, name, safecast.MustConv[int](file.Fd())); err != nil {
		return fmt.Errorf("unlink pinned private regular file %q: %w", name, err)
	}
	if err := unix.Unlinkat(directoryFD, name, 0); err != nil {
		return fmt.Errorf("unlink pinned private regular file %q: %w", name, err)
	}
	return directory.Revalidate()
}

// RemovePinnedEmptyDirectory removes name only while it still identifies the
// exact live child descriptor supplied by the caller. The child descriptor
// remains open and owned by the caller after this method returns.
func (directory *Directory) RemovePinnedEmptyDirectory(
	name string,
	child *Directory,
) error {
	if err := directory.RequirePinnedChildDirectory(name, child); err != nil {
		return err
	}
	if err := child.RequireEntries(nil, 0); err != nil {
		return fmt.Errorf("remove pinned private empty directory %q: %w", name, err)
	}
	if err := directory.RequirePinnedChildDirectory(name, child); err != nil {
		return fmt.Errorf("remove pinned private empty directory %q: %w", name, err)
	}
	directoryFD, err := directory.descriptor()
	if err != nil {
		return err
	}
	childFD, err := child.descriptor()
	if err != nil {
		return err
	}
	if err := sameOpenPath(directoryFD, name, childFD); err != nil {
		return fmt.Errorf("remove pinned private empty directory %q: %w", name, err)
	}
	if err := unix.Unlinkat(directoryFD, name, unix.AT_REMOVEDIR); err != nil {
		return fmt.Errorf("remove pinned private empty directory %q: %w", name, err)
	}
	return directory.Revalidate()
}

// RenameNoReplace atomically moves one exact child to an exact child of the
// destination directory and fails if any destination object already exists.
// Unsupported syscall/filesystem behavior is returned rather than emulated.
func (directory *Directory) RenameNoReplace(
	from string,
	destination *Directory,
	to string,
) error {
	_, err := directory.renameNoReplaceWithStatus(
		from,
		nil,
		destination,
		to,
		renameNoReplaceAt,
	)
	return err
}

// RenameNoReplaceOutcome records what is known about an attempted atomic
// no-replace rename. Completed and Ambiguous outcomes must be treated as
// mutations by callers whose error cleanup could destroy the destination.
type RenameNoReplaceOutcome uint8

const (
	RenameNoReplaceNotAttempted RenameNoReplaceOutcome = iota
	RenameNoReplaceUnchanged
	RenameNoReplaceCompleted
	RenameNoReplaceAmbiguous
)

// RenameDirectoryNoReplaceWithStatus atomically publishes the exact source
// directory identified by its open descriptor. It reports an authoritative or
// conservative mutation outcome. A source-name replacement before the syscall
// and an error-after-commit are therefore never mistaken for a safely
// unpublished stage.
func (directory *Directory) RenameDirectoryNoReplaceWithStatus(
	from string,
	source *Directory,
	destination *Directory,
	to string,
) (RenameNoReplaceOutcome, error) {
	if source == nil {
		return RenameNoReplaceNotAttempted, errors.New(
			"rename private directory: pinned source descriptor is required",
		)
	}
	return directory.renameNoReplaceWithStatus(
		from,
		source,
		destination,
		to,
		renameNoReplaceAt,
	)
}

type renameNoReplaceOperation func(int, string, int, string) error

func (directory *Directory) renameNoReplaceWithStatus(
	from string,
	expectedSource *Directory,
	destination *Directory,
	to string,
	rename renameNoReplaceOperation,
) (RenameNoReplaceOutcome, error) {
	if err := ValidateComponent(from); err != nil {
		return RenameNoReplaceNotAttempted, err
	}
	if err := ValidateComponent(to); err != nil {
		return RenameNoReplaceNotAttempted, err
	}
	if rename == nil {
		return RenameNoReplaceNotAttempted, errors.New("rename private child: operation is required")
	}
	if err := directory.Revalidate(); err != nil {
		return renamePreparationFailureOutcome(expectedSource), err
	}
	if err := destination.Revalidate(); err != nil {
		return renamePreparationFailureOutcome(expectedSource), err
	}
	fromFD, err := directory.descriptor()
	if err != nil {
		return renamePreparationFailureOutcome(expectedSource), err
	}
	toFD, err := destination.descriptor()
	if err != nil {
		return renamePreparationFailureOutcome(expectedSource), err
	}
	var sourceIdentity unix.Stat_t
	if expectedSource == nil {
		if err := unix.Fstatat(fromFD, from, &sourceIdentity, unix.AT_SYMLINK_NOFOLLOW); err != nil {
			return RenameNoReplaceNotAttempted, fmt.Errorf(
				"rename private child: pin source identity: %w",
				err,
			)
		}
	} else {
		if _, err := expectedSource.PinnedInfo(); err != nil {
			return RenameNoReplaceAmbiguous, fmt.Errorf(
				"rename private directory: inspect expected source: %w",
				err,
			)
		}
		expectedFD, err := expectedSource.descriptor()
		if err != nil {
			return RenameNoReplaceAmbiguous, fmt.Errorf(
				"rename private directory: inspect expected source descriptor: %w",
				err,
			)
		}
		if err := unix.Fstat(expectedFD, &sourceIdentity); err != nil {
			return RenameNoReplaceAmbiguous, fmt.Errorf(
				"rename private directory: pin expected source identity: %w",
				err,
			)
		}
		outcome, bindingErr := requireExpectedRenameSource(
			fromFD,
			from,
			toFD,
			to,
			sourceIdentity,
		)
		if bindingErr != nil {
			return outcome, bindingErr
		}
	}
	renameErr := rename(fromFD, from, toFD, to)
	outcome, observationErr := classifyRenameNoReplaceOutcome(
		fromFD,
		from,
		toFD,
		to,
		sourceIdentity,
		renameErr,
	)
	fromStableErr := directory.Revalidate()
	var toStableErr error
	if destination != directory {
		toStableErr = destination.Revalidate()
	}
	stabilityErr := errors.Join(fromStableErr, toStableErr)
	if stabilityErr != nil {
		stabilityErr = fmt.Errorf("rename private child: directory stability: %w", stabilityErr)
	}
	if renameErr == nil {
		return outcome, errors.Join(observationErr, stabilityErr)
	}
	return outcome, errors.Join(
		renameNoReplaceError(to, renameErr),
		observationErr,
		stabilityErr,
	)
}

func renamePreparationFailureOutcome(
	expectedSource *Directory,
) RenameNoReplaceOutcome {
	if expectedSource != nil {
		return RenameNoReplaceAmbiguous
	}
	return RenameNoReplaceNotAttempted
}

func requireExpectedRenameSource(
	fromFD int,
	from string,
	toFD int,
	to string,
	expected unix.Stat_t,
) (RenameNoReplaceOutcome, error) {
	sourcePresent, sourceMatches, sourceErr := childMatchesIdentity(fromFD, from, expected)
	if sourceErr == nil && sourcePresent && sourceMatches {
		return RenameNoReplaceUnchanged, nil
	}
	destinationPresent, destinationMatches, destinationErr := childMatchesIdentity(
		toFD,
		to,
		expected,
	)
	outcome := RenameNoReplaceAmbiguous
	if sourceErr == nil && (!sourcePresent || !sourceMatches) &&
		destinationErr == nil && destinationPresent && destinationMatches {
		outcome = RenameNoReplaceCompleted
	}
	return outcome, errors.Join(
		errors.New("rename private directory: source name no longer identifies the pinned stage"),
		sourceErr,
		destinationErr,
	)
}

func classifyRenameNoReplaceOutcome(
	fromFD int,
	from string,
	toFD int,
	to string,
	sourceIdentity unix.Stat_t,
	renameErr error,
) (RenameNoReplaceOutcome, error) {
	if renameErr == nil {
		return RenameNoReplaceCompleted, nil
	}
	sourcePresent, sourceMatches, sourceErr := childMatchesIdentity(
		fromFD,
		from,
		sourceIdentity,
	)
	destinationPresent, destinationMatches, destinationErr := childMatchesIdentity(
		toFD,
		to,
		sourceIdentity,
	)
	observationErr := errors.Join(sourceErr, destinationErr)
	if observationErr != nil {
		return RenameNoReplaceAmbiguous, fmt.Errorf(
			"rename private child: inspect names after syscall error: %w",
			observationErr,
		)
	}
	if destinationPresent && destinationMatches && (!sourcePresent || !sourceMatches) {
		return RenameNoReplaceCompleted, nil
	}
	if definitivelyUnchangedRenameError(renameErr) && sourcePresent && sourceMatches {
		return RenameNoReplaceUnchanged, nil
	}
	return RenameNoReplaceAmbiguous, nil
}

func childMatchesIdentity(
	directoryFD int,
	name string,
	want unix.Stat_t,
) (present bool, matches bool, returnedErr error) {
	var current unix.Stat_t
	if err := unix.Fstatat(directoryFD, name, &current, unix.AT_SYMLINK_NOFOLLOW); err != nil {
		if errors.Is(err, unix.ENOENT) {
			return false, false, nil
		}
		return false, false, err
	}
	return true, current.Dev == want.Dev && current.Ino == want.Ino, nil
}

type renameNoReplaceFailure uint8

const (
	renameNoReplaceFailureOther renameNoReplaceFailure = iota
	renameNoReplaceFailureDestinationExists
	renameNoReplaceFailureUnsupported
)

func classifyRenameNoReplaceFailure(err error) renameNoReplaceFailure {
	if errors.Is(err, unix.EEXIST) || errors.Is(err, unix.ENOTEMPTY) {
		return renameNoReplaceFailureDestinationExists
	}
	if errors.Is(err, unix.ENOSYS) || errors.Is(err, unix.ENOTSUP) ||
		errors.Is(err, unix.EOPNOTSUPP) || errors.Is(err, unix.EINVAL) {
		return renameNoReplaceFailureUnsupported
	}
	return renameNoReplaceFailureOther
}

func definitivelyUnchangedRenameError(err error) bool {
	return classifyRenameNoReplaceFailure(err) != renameNoReplaceFailureOther
}

func renameNoReplaceError(to string, err error) error {
	switch classifyRenameNoReplaceFailure(err) {
	case renameNoReplaceFailureDestinationExists:
		return fmt.Errorf("%w: %s", ErrDestinationExists, to)
	case renameNoReplaceFailureUnsupported:
		return fmt.Errorf(
			"%w: no-replace rename: %w",
			ErrUnsupportedPlatform,
			err,
		)
	default:
		return fmt.Errorf("rename private child without replacement: %w", err)
	}
}
