package main

import (
	"errors"
	"fmt"
	"io"
	"os"
	"syscall"

	"github.com/Suhaibinator/open-splunk/internal/privatefs"
	"golang.org/x/sys/unix"
)

type stablePathFileReadHooks struct {
	afterOpen func()
	afterRead func()
}

type stablePathFileReadMessages struct {
	inspectPath         string
	openPath            string
	invalidDescriptor   string
	inspectOpen         string
	changedWhileOpening string
	read                string
	overflow            string
	changedWhileReading string
	reinspectOpen       string
	reinspectPath       string
	close               string
}

type stablePathFileReadConfig struct {
	path              string
	maximumReadBytes  int64
	hooks             stablePathFileReadHooks
	validateBefore    func(os.FileInfo) error
	validateOpen      func(*os.File, os.FileInfo) error
	validateAfterPath func(os.FileInfo) error
	sameState         func(os.FileInfo, os.FileInfo) bool
	messages          stablePathFileReadMessages
}

// readBoundedCABundleFile reads an operator-supplied CA bundle with the shared
// stable-path mechanics. Callers supply the message prefix, the noun used in
// every message and the size bound. Both consumers enforce the same CA custody
// policy, independently of the stricter confidentiality policy for secrets.
func readBoundedCABundleFile(
	path string,
	prefix string,
	noun string,
	maximumBytes int64,
) ([]byte, error) {
	return readBoundedCABundleFileWithHooks(path, prefix, noun, maximumBytes, stablePathFileReadHooks{})
}

func readBoundedCABundleFileWithHooks(
	path string,
	prefix string,
	noun string,
	maximumBytes int64,
	hooks stablePathFileReadHooks,
) ([]byte, error) {
	validate := func(info os.FileInfo) error {
		if err := validateCABundleFile(info, os.Geteuid()); err != nil {
			return fmt.Errorf("%s: %s %w", prefix, noun, err)
		}
		if info.Size() > maximumBytes {
			return fmt.Errorf("%s: %s exceeds %d bytes", prefix, noun, maximumBytes)
		}
		return nil
	}
	return readStablePathFile(stablePathFileReadConfig{
		path:             path,
		maximumReadBytes: maximumBytes,
		hooks:            hooks,
		validateBefore:   validate,
		validateOpen: func(file *os.File, info os.FileInfo) error {
			if err := validate(info); err != nil {
				return err
			}
			if err := privatefs.ValidateNoExtendedACL(file); err != nil {
				return fmt.Errorf("%s: %s has unsupported access-control metadata", prefix, noun)
			}
			return nil
		},
		validateAfterPath: validate,
		sameState:         sameCABundleFileState,
		messages: stablePathFileReadMessages{
			inspectPath:         fmt.Sprintf("%s: inspect %s", prefix, noun),
			openPath:            fmt.Sprintf("%s: open %s", prefix, noun),
			invalidDescriptor:   fmt.Sprintf("%s: invalid %s descriptor", prefix, noun),
			inspectOpen:         fmt.Sprintf("%s: inspect open %s", prefix, noun),
			changedWhileOpening: fmt.Sprintf("%s: %s changed while opening", prefix, noun),
			read:                fmt.Sprintf("%s: read %s", prefix, noun),
			overflow:            fmt.Sprintf("%s: %s exceeds %d bytes", prefix, noun, maximumBytes),
			changedWhileReading: fmt.Sprintf("%s: %s changed while reading", prefix, noun),
			reinspectOpen:       fmt.Sprintf("%s: reinspect open %s", prefix, noun),
			reinspectPath:       fmt.Sprintf("%s: reinspect %s", prefix, noun),
			close:               fmt.Sprintf("%s: close %s", prefix, noun),
		},
	})
}

func validateCABundleFile(info os.FileInfo, effectiveUID int) error {
	if info == nil || !info.Mode().IsRegular() {
		return errors.New("must be a regular file")
	}
	mode := info.Mode()
	if mode.Perm()&0o400 == 0 || mode.Perm()&0o133 != 0 {
		return errors.New("must be owner-readable without execute or group/other write permissions")
	}
	if mode&(os.ModeSetuid|os.ModeSetgid|os.ModeSticky) != 0 {
		return errors.New("must not have special permission bits")
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || stat == nil {
		return errors.New("ownership and link metadata are unavailable")
	}
	if effectiveUID < 0 || (stat.Uid != 0 && int64(stat.Uid) != int64(effectiveUID)) {
		return errors.New("must be owned by root or the effective user")
	}
	if stat.Nlink != 1 {
		return errors.New("must have exactly one hard link")
	}
	return nil
}

func sameCABundleFileState(left, right os.FileInfo) bool {
	if left == nil || right == nil || !os.SameFile(left, right) ||
		left.Mode() != right.Mode() || left.Size() != right.Size() ||
		!left.ModTime().Equal(right.ModTime()) {
		return false
	}
	leftStat, leftOK := left.Sys().(*syscall.Stat_t)
	rightStat, rightOK := right.Sys().(*syscall.Stat_t)
	return leftOK && rightOK && leftStat != nil && rightStat != nil &&
		leftStat.Uid == rightStat.Uid && leftStat.Gid == rightStat.Gid &&
		leftStat.Nlink == rightStat.Nlink
}

// readStablePathFile centralizes the race-resistant mechanics shared by
// operator-supplied credential and trust files. Domain wrappers retain their
// own path, metadata, ACL, size, terminator, and error policies.
func readStablePathFile(config stablePathFileReadConfig) ([]byte, error) {
	before, err := os.Lstat(config.path)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", config.messages.inspectPath, err)
	}
	if err := config.validateBefore(before); err != nil {
		return nil, err
	}

	// domain-specific validation; O_NOFOLLOW and O_NONBLOCK prevent final-link
	// redirection and blocking filesystem objects.
	fd, err := unix.Open(
		config.path,
		unix.O_RDONLY|unix.O_CLOEXEC|unix.O_NOFOLLOW|unix.O_NONBLOCK,
		0,
	)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", config.messages.openPath, err)
	}

	// descriptor.
	file := os.NewFile(uintptr(fd), config.path)
	if file == nil {
		_ = unix.Close(fd)
		return nil, errors.New(config.messages.invalidDescriptor)
	}
	closed := false
	defer func() {
		if !closed {
			_ = file.Close()
		}
	}()
	if config.hooks.afterOpen != nil {
		config.hooks.afterOpen()
	}

	opened, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("%s: %w", config.messages.inspectOpen, err)
	}
	if !os.SameFile(before, opened) {
		return nil, errors.New(config.messages.changedWhileOpening)
	}
	if err := config.validateOpen(file, opened); err != nil {
		return nil, err
	}

	contents, err := io.ReadAll(io.LimitReader(file, config.maximumReadBytes+1))
	if err != nil {
		return nil, fmt.Errorf("%s: %w", config.messages.read, err)
	}
	returnContents := false
	defer func() {
		if !returnContents {
			clear(contents)
		}
	}()
	if int64(len(contents)) > config.maximumReadBytes {
		return nil, errors.New(config.messages.overflow)
	}
	if int64(len(contents)) != opened.Size() {
		return nil, errors.New(config.messages.changedWhileReading)
	}
	if config.hooks.afterRead != nil {
		config.hooks.afterRead()
	}

	afterOpen, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("%s: %w", config.messages.reinspectOpen, err)
	}
	if !config.sameState(opened, afterOpen) {
		return nil, errors.New(config.messages.changedWhileReading)
	}
	if err := config.validateOpen(file, afterOpen); err != nil {
		return nil, err
	}
	afterPath, err := os.Lstat(config.path)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", config.messages.reinspectPath, err)
	}
	if !config.sameState(afterOpen, afterPath) {
		return nil, errors.New(config.messages.changedWhileReading)
	}
	if err := config.validateAfterPath(afterPath); err != nil {
		return nil, err
	}
	if err := file.Close(); err != nil {
		return nil, fmt.Errorf("%s: %w", config.messages.close, err)
	}
	closed = true
	returnContents = true
	return contents, nil
}
