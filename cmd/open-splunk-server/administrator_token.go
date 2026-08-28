package main

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/Suhaibinator/open-splunk/internal/auth"
	"github.com/Suhaibinator/open-splunk/internal/privatefs"
)

const maximumAdministratorTokenFileBytes = auth.MaximumBrowserBearerTokenBytes + 2

type administratorTokenReadHooks = stablePathFileReadHooks

func newAdministratorBrowserAuthenticator(
	inlineToken string,
	path string,
	tenantID string,
	ownerID string,
) (auth.BrowserAuthenticator, error) {
	token, loadErr := loadAdministratorToken(inlineToken, path)
	if loadErr != nil {
		clear(token)
		return nil, fmt.Errorf("load administrator token: %w", loadErr)
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

func loadAdministratorToken(inlineToken, path string) ([]byte, error) {
	path = strings.TrimSpace(path)
	if path != "" && inlineToken != "" {
		return nil, fmt.Errorf(
			"administrator token and administrator token file are mutually exclusive",
		)
	}
	if path != "" {
		return readAdministratorToken(path)
	}
	if inlineToken == "" {
		return nil, fmt.Errorf(
			"administrator token or administrator token file is required",
		)
	}
	token := []byte(inlineToken)
	if err := auth.ValidateBrowserBearerToken(token); err != nil {
		clear(token)
		return nil, errors.New(
			"administrator token contains an invalid bearer token",
		)
	}
	return token, nil
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
	contents, err := readStablePathFile(stablePathFileReadConfig{
		path:             absolutePath,
		maximumReadBytes: maximumAdministratorTokenFileBytes,
		hooks:            hooks,
		validateBefore: func(info os.FileInfo) error {
			return validateAdministratorTokenFile(info, os.Geteuid())
		},
		validateOpen: func(file *os.File, info os.FileInfo) error {
			if err := validateAdministratorTokenFile(info, os.Geteuid()); err != nil {
				return err
			}
			if err := privatefs.ValidateNoExtendedACL(file); err != nil {
				return err
			}
			if info.Size() < auth.MinimumBrowserBearerTokenBytes ||
				info.Size() > maximumAdministratorTokenFileBytes {
				return errors.New("administrator token file has an invalid size")
			}
			return nil
		},
		validateAfterPath: func(info os.FileInfo) error {
			return validateAdministratorTokenFile(info, os.Geteuid())
		},
		sameState: sameAdministratorTokenFileState,
		messages: stablePathFileReadMessages{
			inspectPath:         "inspect administrator token file",
			openPath:            "open administrator token file",
			invalidDescriptor:   "open administrator token file: invalid file descriptor",
			inspectOpen:         "inspect open administrator token file",
			changedWhileOpening: "administrator token file changed while it was opened",
			read:                "read administrator token file",
			overflow:            "administrator token file changed while it was read",
			changedWhileReading: "administrator token file changed while it was read",
			reinspectOpen:       "reinspect open administrator token file",
			reinspectPath:       "reinspect administrator token file",
			close:               "close administrator token file",
		},
	})
	if err != nil {
		return nil, err
	}
	defer clear(contents)

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
		return "", errors.New("administrator token file path is required")
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
	if stat.Nlink != 1 {
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
