package main

import (
	"errors"
	"fmt"
	"io"
	"os"

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
// every message, the size bound, and their own file-state comparison policy.
func readBoundedCABundleFile(
	path string,
	prefix string,
	noun string,
	maximumBytes int64,
	sameState func(os.FileInfo, os.FileInfo) bool,
) ([]byte, error) {
	validate := func(info os.FileInfo) error {
		if info == nil || !info.Mode().IsRegular() {
			return fmt.Errorf("%s: %s must be a regular file", prefix, noun)
		}
		if info.Size() > maximumBytes {
			return fmt.Errorf("%s: %s exceeds %d bytes", prefix, noun, maximumBytes)
		}
		return nil
	}
	return readStablePathFile(stablePathFileReadConfig{
		path:             path,
		maximumReadBytes: maximumBytes,
		validateBefore:   validate,
		validateOpen: func(_ *os.File, info os.FileInfo) error {
			return validate(info)
		},
		validateAfterPath: validate,
		sameState:         sameState,
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
