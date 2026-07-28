package main

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/Suhaibinator/open-splunk/internal/auth"
	"golang.org/x/sys/unix"
)

const maximumAdministratorTokenFileBytes = auth.MaximumBrowserBearerTokenBytes + 2

type administratorTokenReadHooks struct {
	afterOpen func()
	afterRead func()
}

func newAdministratorBrowserAuthenticator(
	path string,
	tenantID string,
	ownerID string,
) (auth.BrowserAuthenticator, error) {
	token, err := readAdministratorToken(path)
	if err != nil {
		return nil, fmt.Errorf("load administrator token: %w", err)
	}
	defer clear(token)

	authenticator, err := auth.NewBearerTokenAuthenticator(
		token,
		tenantID,
		ownerID,
		auth.BrowserRoleAdministrator,
	)
	if err != nil {
		return nil, errors.New("configure administrator authentication: invalid token or identity")
	}
	return authenticator, nil
}

func readAdministratorToken(path string) ([]byte, error) {
	return readAdministratorTokenWithHooks(path, administratorTokenReadHooks{})
}

func readAdministratorTokenWithHooks(
	path string,
	hooks administratorTokenReadHooks,
) ([]byte, error) {
	absolutePath, err := resolveAdministratorTokenPath(path)
	if err != nil {
		return nil, err
	}
	before, err := os.Lstat(absolutePath)
	if err != nil {
		return nil, fmt.Errorf("inspect administrator token file: %w", err)
	}
	if err := validateAdministratorTokenFile(before, os.Geteuid()); err != nil {
		return nil, err
	}

	fd, err := unix.Open(
		absolutePath,
		unix.O_RDONLY|unix.O_CLOEXEC|unix.O_NOFOLLOW|unix.O_NONBLOCK,
		0,
	)
	if err != nil {
		return nil, fmt.Errorf("open administrator token file: %w", err)
	}
	// #nosec G115 -- unix.Open succeeded, so fd is a non-negative native file descriptor.
	file := os.NewFile(uintptr(fd), absolutePath)
	if file == nil {
		_ = unix.Close(fd)
		return nil, errors.New("open administrator token file: invalid file descriptor")
	}
	closed := false
	defer func() {
		if !closed {
			_ = file.Close()
		}
	}()
	if hooks.afterOpen != nil {
		hooks.afterOpen()
	}

	opened, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("inspect open administrator token file: %w", err)
	}
	if !os.SameFile(before, opened) {
		return nil, errors.New("administrator token file changed while it was opened")
	}
	if err := validateAdministratorTokenFile(opened, os.Geteuid()); err != nil {
		return nil, err
	}
	if err := validateAdministratorTokenACL(file); err != nil {
		return nil, err
	}
	if opened.Size() < auth.MinimumBrowserBearerTokenBytes ||
		opened.Size() > maximumAdministratorTokenFileBytes {
		return nil, errors.New("administrator token file has an invalid size")
	}

	contents, err := io.ReadAll(io.LimitReader(file, maximumAdministratorTokenFileBytes+1))
	if err != nil {
		return nil, fmt.Errorf("read administrator token file: %w", err)
	}
	defer clear(contents)
	if len(contents) > maximumAdministratorTokenFileBytes ||
		int64(len(contents)) != opened.Size() {
		return nil, errors.New("administrator token file changed while it was read")
	}
	if hooks.afterRead != nil {
		hooks.afterRead()
	}

	afterOpen, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("reinspect open administrator token file: %w", err)
	}
	if !sameAdministratorTokenFileState(opened, afterOpen) {
		return nil, errors.New("administrator token file changed while it was read")
	}
	if err := validateAdministratorTokenFile(afterOpen, os.Geteuid()); err != nil {
		return nil, err
	}
	if err := validateAdministratorTokenACL(file); err != nil {
		return nil, err
	}
	afterPath, err := os.Lstat(absolutePath)
	if err != nil {
		return nil, fmt.Errorf("reinspect administrator token file: %w", err)
	}
	if !sameAdministratorTokenFileState(afterOpen, afterPath) {
		return nil, errors.New("administrator token file changed while it was read")
	}
	if err := validateAdministratorTokenFile(afterPath, os.Geteuid()); err != nil {
		return nil, err
	}
	if err := file.Close(); err != nil {
		return nil, fmt.Errorf("close administrator token file: %w", err)
	}
	closed = true

	token := stripAdministratorTokenTerminator(contents)
	if err := auth.ValidateBrowserBearerToken(token); err != nil {
		return nil, errors.New("administrator token file contains an invalid bearer token")
	}
	return append([]byte(nil), token...), nil
}

func stripAdministratorTokenTerminator(contents []byte) []byte {
	if len(contents) == 0 || contents[len(contents)-1] != '\n' {
		return contents
	}
	contents = contents[:len(contents)-1]
	if len(contents) != 0 && contents[len(contents)-1] == '\r' {
		contents = contents[:len(contents)-1]
	}
	return contents
}

func resolveAdministratorTokenPath(path string) (string, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return "", errors.New("-administrator-token-file is required")
	}
	if strings.IndexByte(path, 0) >= 0 {
		return "", errors.New("administrator token file path contains a NUL byte")
	}
	absolutePath, err := filepath.Abs(path)
	if err != nil {
		return "", fmt.Errorf("resolve administrator token file path: %w", err)
	}
	return absolutePath, nil
}

func validateAdministratorTokenFile(info os.FileInfo, effectiveUID int) error {
	if info == nil || !info.Mode().IsRegular() {
		return errors.New("administrator token file must be a regular file")
	}
	mode := info.Mode()
	if mode.Perm() != 0o400 && mode.Perm() != 0o600 {
		return errors.New("administrator token file permissions must be exactly 0400 or 0600")
	}
	if mode&(os.ModeSetuid|os.ModeSetgid|os.ModeSticky) != 0 {
		return errors.New("administrator token file must not have special permission bits")
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || stat == nil {
		return errors.New("administrator token file ownership is unavailable")
	}
	if effectiveUID < 0 || int64(stat.Uid) != int64(effectiveUID) {
		return errors.New("administrator token file must be owned by the server user")
	}
	if uint64(stat.Nlink) != 1 {
		return errors.New("administrator token file must have exactly one hard link")
	}
	return nil
}

func sameAdministratorTokenFileState(left, right os.FileInfo) bool {
	if left == nil || right == nil || !os.SameFile(left, right) {
		return false
	}
	return left.Mode() == right.Mode() &&
		left.Size() == right.Size() &&
		left.ModTime().Equal(right.ModTime())
}
