package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"syscall"

	"github.com/Suhaibinator/open-splunk/internal/controlbackup"
	"github.com/Suhaibinator/open-splunk/internal/privatefs"
	"golang.org/x/sys/unix"
)

const (
	// The prepared volume is the same directory the backup subcommand passes as
	// -archive-root, so these must equal the archive ownership policy.
	clickHouseRecoveryVolumeUID                     = deploymentRecoveryArchiveUID
	clickHouseRecoveryVolumeGID                     = deploymentRecoveryArchiveGID
	clickHouseRecoveryVolumeReadyMode   os.FileMode = deploymentRecoveryArchiveRootMode
	clickHouseRecoveryVolumeSpecialMode             = os.ModeSetuid | os.ModeSetgid | os.ModeSticky
)

type clickHouseRecoveryVolumeDirectory interface {
	Stat() (os.FileInfo, error)
	Readdirnames(int) ([]string, error)
	Chown(int, int) error
	Chmod(os.FileMode) error
	Close() error
}

type prepareClickHouseRecoveryVolumeDependencies struct {
	effectiveUID func() int
	lstat        func(string) (os.FileInfo, error)
	open         func(string) (clickHouseRecoveryVolumeDirectory, error)
	sameFile     func(os.FileInfo, os.FileInfo) bool
	validateACL  func(clickHouseRecoveryVolumeDirectory) error
}

type prepareClickHouseRecoveryVolumeHooks struct {
	afterOpen           func()
	afterEmptinessCheck func()
	afterChown          func()
	afterChmod          func()
}

func runPrepareClickHouseRecoveryVolumeSubcommand(arguments []string) error {
	return runPrepareClickHouseRecoveryVolumeSubcommandWithDependencies(
		arguments,
		defaultPrepareClickHouseRecoveryVolumeDependencies(),
		prepareClickHouseRecoveryVolumeHooks{},
	)
}

func runPrepareClickHouseRecoveryVolumeSubcommandWithDependencies(
	arguments []string,
	dependencies prepareClickHouseRecoveryVolumeDependencies,
	hooks prepareClickHouseRecoveryVolumeHooks,
) error {
	flags := flag.NewFlagSet(
		"prepare-clickhouse-recovery-volume",
		flag.ContinueOnError,
	)
	flags.SetOutput(io.Discard)
	pathFlag := exactOnceStringFlag{
		duplicateError: "-path may be specified exactly once",
	}
	flags.Var(
		&pathFlag,
		"path",
		"absolute ClickHouse recovery archive volume root",
	)
	if err := flags.Parse(arguments); err != nil {
		return fmt.Errorf(
			"prepare ClickHouse recovery volume: parse flags: %w",
			err,
		)
	}
	if flags.NArg() != 0 {
		return errors.New(
			"prepare ClickHouse recovery volume: positional arguments are not allowed",
		)
	}
	if !pathFlag.set {
		return errors.New("prepare ClickHouse recovery volume: -path is required")
	}
	if err := controlbackup.ValidateExactAbsolutePath("-path", pathFlag.value); err != nil {
		return fmt.Errorf("prepare ClickHouse recovery volume: %w", err)
	}
	return prepareClickHouseRecoveryVolumeWithDependencies(
		pathFlag.value,
		dependencies,
		hooks,
	)
}

func defaultPrepareClickHouseRecoveryVolumeDependencies() prepareClickHouseRecoveryVolumeDependencies {
	return prepareClickHouseRecoveryVolumeDependencies{
		effectiveUID: os.Geteuid,
		lstat:        os.Lstat,
		open:         openClickHouseRecoveryVolumeDirectory,
		sameFile:     os.SameFile,
		validateACL:  validateClickHouseRecoveryVolumeACL,
	}
}

func openClickHouseRecoveryVolumeDirectory(
	path string,
) (clickHouseRecoveryVolumeDirectory, error) {
	// #nosec G304,G703 -- -path is an exact absolute operator path. O_NOFOLLOW
	// rejects a symlink in the mount-root path component, and all mutations use
	// the returned descriptor rather than resolving the pathname again.
	fd, err := unix.Open(
		path,
		unix.O_RDONLY|unix.O_CLOEXEC|unix.O_DIRECTORY|unix.O_NOFOLLOW,
		0,
	)
	if err != nil {
		return nil, fmt.Errorf("open recovery volume root: %w", err)
	}
	// #nosec G115 -- unix.Open returned a non-negative native descriptor.
	file := os.NewFile(uintptr(fd), path)
	if file == nil {
		_ = unix.Close(fd)
		return nil, errors.New("open recovery volume root: invalid descriptor")
	}
	return file, nil
}

func prepareClickHouseRecoveryVolumeWithDependencies(
	path string,
	dependencies prepareClickHouseRecoveryVolumeDependencies,
	hooks prepareClickHouseRecoveryVolumeHooks,
) (resultErr error) {
	if err := validatePrepareClickHouseRecoveryVolumeDependencies(dependencies); err != nil {
		return err
	}
	if dependencies.effectiveUID() != 0 {
		return errors.New(
			"prepare ClickHouse recovery volume: effective user must be root",
		)
	}
	if err := controlbackup.ValidateExactAbsolutePath("-path", path); err != nil {
		return fmt.Errorf("prepare ClickHouse recovery volume: %w", err)
	}

	before, err := dependencies.lstat(path)
	if err != nil {
		return fmt.Errorf(
			"prepare ClickHouse recovery volume: inspect volume root: %w",
			err,
		)
	}
	initialState, err := classifyClickHouseRecoveryVolumeInitialState(before)
	if err != nil {
		return err
	}
	directory, err := dependencies.open(path)
	if err != nil {
		return fmt.Errorf("prepare ClickHouse recovery volume: %w", err)
	}
	if directory == nil {
		return errors.New(
			"prepare ClickHouse recovery volume: open volume root returned an invalid descriptor",
		)
	}
	defer func() {
		if err := directory.Close(); resultErr == nil && err != nil {
			resultErr = fmt.Errorf(
				"prepare ClickHouse recovery volume: close volume root: %w",
				err,
			)
		}
	}()

	if hooks.afterOpen != nil {
		hooks.afterOpen()
	}
	if err := validatePinnedClickHouseRecoveryVolume(
		path,
		before,
		directory,
		initialState,
		dependencies,
	); err != nil {
		return err
	}
	if initialState == clickHouseRecoveryVolumeReadyState {
		return nil
	}

	if err := requireEmptyClickHouseRecoveryVolume(directory); err != nil {
		return err
	}
	if hooks.afterEmptinessCheck != nil {
		hooks.afterEmptinessCheck()
	}
	if err := validatePinnedClickHouseRecoveryVolume(
		path,
		before,
		directory,
		initialState,
		dependencies,
	); err != nil {
		return err
	}
	if initialState == clickHouseRecoveryVolumeFreshState {
		if err := directory.Chown(
			clickHouseRecoveryVolumeUID,
			clickHouseRecoveryVolumeGID,
		); err != nil {
			return fmt.Errorf(
				"prepare ClickHouse recovery volume: set volume ownership: %w",
				err,
			)
		}
		if hooks.afterChown != nil {
			hooks.afterChown()
		}
		if err := validatePinnedClickHouseRecoveryVolume(
			path,
			before,
			directory,
			clickHouseRecoveryVolumeChownedState,
			dependencies,
		); err != nil {
			return err
		}
	}
	if err := directory.Chmod(clickHouseRecoveryVolumeReadyMode); err != nil {
		return fmt.Errorf(
			"prepare ClickHouse recovery volume: set volume permissions: %w",
			err,
		)
	}
	if hooks.afterChmod != nil {
		hooks.afterChmod()
	}
	return validatePinnedClickHouseRecoveryVolume(
		path,
		before,
		directory,
		clickHouseRecoveryVolumeReadyState,
		dependencies,
	)
}

func validatePrepareClickHouseRecoveryVolumeDependencies(
	dependencies prepareClickHouseRecoveryVolumeDependencies,
) error {
	if dependencies.effectiveUID == nil || dependencies.lstat == nil ||
		dependencies.open == nil || dependencies.sameFile == nil ||
		dependencies.validateACL == nil {
		return errors.New(
			"prepare ClickHouse recovery volume: filesystem dependencies are incomplete",
		)
	}
	return nil
}

type clickHouseRecoveryVolumeState uint8

const (
	clickHouseRecoveryVolumeFreshState clickHouseRecoveryVolumeState = iota + 1
	clickHouseRecoveryVolumeChownedState
	clickHouseRecoveryVolumeReadyState
)

type clickHouseRecoveryVolumeMetadata struct {
	uid         uint32
	gid         uint32
	permissions os.FileMode
	special     os.FileMode
}

func classifyClickHouseRecoveryVolumeInitialState(
	info os.FileInfo,
) (clickHouseRecoveryVolumeState, error) {
	metadata, err := readClickHouseRecoveryVolumeMetadata(info)
	if err != nil {
		return 0, err
	}
	if metadata.matches(clickHouseRecoveryVolumeFreshState) {
		return clickHouseRecoveryVolumeFreshState, nil
	}
	if metadata.matches(clickHouseRecoveryVolumeChownedState) {
		return clickHouseRecoveryVolumeChownedState, nil
	}
	if metadata.matches(clickHouseRecoveryVolumeReadyState) {
		return clickHouseRecoveryVolumeReadyState, nil
	}
	return 0, errors.New(
		"prepare ClickHouse recovery volume: volume root must be empty root:root mode 0755, empty uid 101 gid 65532 mode 0755, or uid 101 gid 65532 mode 02750",
	)
}

func readClickHouseRecoveryVolumeMetadata(
	info os.FileInfo,
) (clickHouseRecoveryVolumeMetadata, error) {
	if info == nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return clickHouseRecoveryVolumeMetadata{}, errors.New(
			"prepare ClickHouse recovery volume: volume root must be a real directory",
		)
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || stat == nil {
		return clickHouseRecoveryVolumeMetadata{}, errors.New(
			"prepare ClickHouse recovery volume: volume root ownership is unavailable",
		)
	}
	return clickHouseRecoveryVolumeMetadata{
		uid:         stat.Uid,
		gid:         stat.Gid,
		permissions: info.Mode().Perm(),
		special:     info.Mode() & clickHouseRecoveryVolumeSpecialMode,
	}, nil
}

func (metadata clickHouseRecoveryVolumeMetadata) matches(
	state clickHouseRecoveryVolumeState,
) bool {
	switch state {
	case clickHouseRecoveryVolumeFreshState:
		return metadata.uid == 0 && metadata.gid == 0 &&
			metadata.permissions == 0o755 && metadata.special == 0
	case clickHouseRecoveryVolumeChownedState:
		return metadata.uid == clickHouseRecoveryVolumeUID &&
			metadata.gid == clickHouseRecoveryVolumeGID &&
			metadata.permissions == 0o755 && metadata.special == 0
	case clickHouseRecoveryVolumeReadyState:
		return metadata.uid == clickHouseRecoveryVolumeUID &&
			metadata.gid == clickHouseRecoveryVolumeGID &&
			metadata.permissions == 0o750 && metadata.special == os.ModeSetgid
	default:
		return false
	}
}

func validatePinnedClickHouseRecoveryVolume(
	path string,
	identity os.FileInfo,
	directory clickHouseRecoveryVolumeDirectory,
	wantState clickHouseRecoveryVolumeState,
	dependencies prepareClickHouseRecoveryVolumeDependencies,
) error {
	opened, err := directory.Stat()
	if err != nil {
		return fmt.Errorf(
			"prepare ClickHouse recovery volume: inspect open volume root: %w",
			err,
		)
	}
	if !dependencies.sameFile(identity, opened) {
		return errors.New(
			"prepare ClickHouse recovery volume: open volume root identity changed",
		)
	}
	openedMetadata, err := readClickHouseRecoveryVolumeMetadata(opened)
	if err != nil {
		return err
	}
	if !openedMetadata.matches(wantState) {
		return errors.New(
			"prepare ClickHouse recovery volume: open volume root changed to an invalid state",
		)
	}
	if err := dependencies.validateACL(directory); err != nil {
		return fmt.Errorf(
			"prepare ClickHouse recovery volume: volume root ACL is unsafe or unavailable: %w",
			err,
		)
	}
	current, err := dependencies.lstat(path)
	if err != nil {
		return fmt.Errorf(
			"prepare ClickHouse recovery volume: reinspect volume root path: %w",
			err,
		)
	}
	if !dependencies.sameFile(opened, current) {
		return errors.New(
			"prepare ClickHouse recovery volume: path no longer names the pinned volume root",
		)
	}
	currentMetadata, err := readClickHouseRecoveryVolumeMetadata(current)
	if err != nil {
		return err
	}
	if !currentMetadata.matches(wantState) {
		return errors.New(
			"prepare ClickHouse recovery volume: volume root path changed to an invalid state",
		)
	}
	return nil
}

func requireEmptyClickHouseRecoveryVolume(
	directory clickHouseRecoveryVolumeDirectory,
) error {
	entries, err := directory.Readdirnames(1)
	if len(entries) != 0 {
		return errors.New(
			"prepare ClickHouse recovery volume: uninitialized volume root must be empty",
		)
	}
	if errors.Is(err, io.EOF) {
		return nil
	}
	if err != nil {
		return fmt.Errorf(
			"prepare ClickHouse recovery volume: inspect volume root contents: %w",
			err,
		)
	}
	return errors.New(
		"prepare ClickHouse recovery volume: could not prove uninitialized volume root is empty",
	)
}

func validateClickHouseRecoveryVolumeACL(
	directory clickHouseRecoveryVolumeDirectory,
) error {
	file, ok := directory.(*os.File)
	if !ok || file == nil {
		return errors.New("recovery volume ACL descriptor is unavailable")
	}
	return privatefs.ValidateNoExtendedACL(file)
}
