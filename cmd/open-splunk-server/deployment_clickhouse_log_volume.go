package main

import (
	"errors"
	"fmt"
	"io"
	"os"
	"syscall"

	"github.com/Suhaibinator/open-splunk/internal/controlbackup"
)

const (
	clickHouseLogVolumeUID                   = 101
	clickHouseLogVolumeGID                   = 101
	clickHouseLogVolumeReadyMode os.FileMode = 0o700
)

type clickHouseLogVolumeState uint8

const (
	clickHouseLogVolumeNeedsChown clickHouseLogVolumeState = iota + 1
	clickHouseLogVolumeNeedsChmod
	clickHouseLogVolumeReady
)

func prepareClickHouseLogVolumeWithDependencies(
	path string,
	dependencies prepareClickHouseRecoveryVolumeDependencies,
) (resultErr error) {
	if err := validatePrepareClickHouseRecoveryVolumeDependencies(dependencies); err != nil {
		return err
	}
	if dependencies.effectiveUID() != 0 {
		return errors.New("prepare ClickHouse log volume: effective user must be root")
	}
	if err := controlbackup.ValidateExactAbsolutePath("-log-path", path); err != nil {
		return fmt.Errorf("prepare ClickHouse log volume: %w", err)
	}

	before, err := dependencies.lstat(path)
	if err != nil {
		return fmt.Errorf("prepare ClickHouse log volume: inspect volume root: %w", err)
	}
	initialState, err := classifyClickHouseLogVolumeState(before)
	if err != nil {
		return err
	}
	directory, err := dependencies.open(path)
	if err != nil {
		return fmt.Errorf("prepare ClickHouse log volume: %w", err)
	}
	if directory == nil {
		return errors.New("prepare ClickHouse log volume: open volume root returned an invalid descriptor")
	}
	defer func() {
		if err := directory.Close(); resultErr == nil && err != nil {
			resultErr = fmt.Errorf("prepare ClickHouse log volume: close volume root: %w", err)
		}
	}()

	if err := validatePinnedClickHouseLogVolume(path, before, directory, initialState, dependencies); err != nil {
		return err
	}
	if initialState == clickHouseLogVolumeReady {
		return nil
	}
	if err := requireEmptyClickHouseLogVolume(directory); err != nil {
		return err
	}
	if err := validatePinnedClickHouseLogVolume(path, before, directory, initialState, dependencies); err != nil {
		return err
	}
	if initialState == clickHouseLogVolumeNeedsChown {
		if err := directory.Chown(clickHouseLogVolumeUID, clickHouseLogVolumeGID); err != nil {
			return fmt.Errorf("prepare ClickHouse log volume: set volume ownership: %w", err)
		}
		if err := validatePinnedClickHouseLogVolume(
			path,
			before,
			directory,
			clickHouseLogVolumeNeedsChmod,
			dependencies,
		); err != nil {
			return err
		}
	}
	if err := directory.Chmod(clickHouseLogVolumeReadyMode); err != nil {
		return fmt.Errorf("prepare ClickHouse log volume: set volume permissions: %w", err)
	}
	return validatePinnedClickHouseLogVolume(
		path,
		before,
		directory,
		clickHouseLogVolumeReady,
		dependencies,
	)
}

func classifyClickHouseLogVolumeState(info os.FileInfo) (clickHouseLogVolumeState, error) {
	metadata, err := readClickHouseLogVolumeMetadata(info)
	if err != nil {
		return 0, err
	}
	if metadata.uid == clickHouseLogVolumeUID && metadata.gid == clickHouseLogVolumeGID &&
		metadata.permissions == clickHouseLogVolumeReadyMode && metadata.special == 0 {
		return clickHouseLogVolumeReady, nil
	}
	if metadata.uid == clickHouseLogVolumeUID && metadata.gid == clickHouseLogVolumeGID &&
		(metadata.permissions == 0o777 || metadata.permissions == 0o755) && metadata.special == 0 {
		return clickHouseLogVolumeNeedsChmod, nil
	}
	if ((metadata.uid == 0 && metadata.gid == clickHouseLogVolumeGID && metadata.permissions == 0o777) ||
		(metadata.uid == 0 && metadata.gid == 0 && metadata.permissions == 0o755)) &&
		metadata.special == 0 {
		return clickHouseLogVolumeNeedsChown, nil
	}
	return 0, errors.New(
		"prepare ClickHouse log volume: volume root must be empty root:101 mode 0777, empty root:root mode 0755, an interrupted uid 101 gid 101 mode 0755/0777, or uid 101 gid 101 mode 0700",
	)
}

func readClickHouseLogVolumeMetadata(info os.FileInfo) (clickHouseRecoveryVolumeMetadata, error) {
	if info == nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return clickHouseRecoveryVolumeMetadata{}, errors.New(
			"prepare ClickHouse log volume: volume root must be a real directory",
		)
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || stat == nil {
		return clickHouseRecoveryVolumeMetadata{}, errors.New(
			"prepare ClickHouse log volume: volume root ownership is unavailable",
		)
	}
	return clickHouseRecoveryVolumeMetadata{
		uid:         stat.Uid,
		gid:         stat.Gid,
		permissions: info.Mode().Perm(),
		special:     info.Mode() & clickHouseRecoveryVolumeSpecialMode,
	}, nil
}

func validatePinnedClickHouseLogVolume(
	path string,
	identity os.FileInfo,
	directory clickHouseRecoveryVolumeDirectory,
	wantState clickHouseLogVolumeState,
	dependencies prepareClickHouseRecoveryVolumeDependencies,
) error {
	opened, err := directory.Stat()
	if err != nil {
		return fmt.Errorf("prepare ClickHouse log volume: inspect open volume root: %w", err)
	}
	if !dependencies.sameFile(identity, opened) {
		return errors.New("prepare ClickHouse log volume: open volume root identity changed")
	}
	openedState, err := classifyClickHouseLogVolumeState(opened)
	if err != nil || openedState != wantState {
		return errors.New("prepare ClickHouse log volume: open volume root changed to an invalid state")
	}
	if err := dependencies.validateACL(directory); err != nil {
		return fmt.Errorf("prepare ClickHouse log volume: volume root ACL is unsafe or unavailable: %w", err)
	}
	current, err := dependencies.lstat(path)
	if err != nil {
		return fmt.Errorf("prepare ClickHouse log volume: reinspect volume root path: %w", err)
	}
	if !dependencies.sameFile(opened, current) {
		return errors.New("prepare ClickHouse log volume: path no longer names the pinned volume root")
	}
	currentState, err := classifyClickHouseLogVolumeState(current)
	if err != nil || currentState != wantState {
		return errors.New("prepare ClickHouse log volume: volume root path changed to an invalid state")
	}
	return nil
}

func requireEmptyClickHouseLogVolume(directory clickHouseRecoveryVolumeDirectory) error {
	entries, err := directory.Readdirnames(1)
	if len(entries) != 0 {
		return errors.New("prepare ClickHouse log volume: uninitialized volume root must be empty")
	}
	if errors.Is(err, io.EOF) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("prepare ClickHouse log volume: inspect volume root contents: %w", err)
	}
	return errors.New("prepare ClickHouse log volume: could not prove uninitialized volume root is empty")
}
