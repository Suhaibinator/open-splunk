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

	// #nosec G304,G703 -- callers supply read-only operator paths and retain
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
	// #nosec G115 -- unix.Open succeeded, so fd is a non-negative native file
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
