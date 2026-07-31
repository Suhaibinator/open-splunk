package main

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"
)

const maximumClickHouseCredentialBytes = 4096

type clickHouseCredentialReadHooks = stablePathFileReadHooks

func loadClickHouseCredential(path, environmentName, role string) (string, error) {
	path = strings.TrimSpace(path)
	environmentValue := os.Getenv(environmentName)
	if path == "" {
		if environmentValue == "" {
			return "", fmt.Errorf(
				"configure ClickHouse: %s password requires -clickhouse-%s-password-file or %s",
				role,
				role,
				environmentName,
			)
		}
		return environmentValue, nil
	}
	if environmentValue != "" {
		return "", fmt.Errorf(
			"configure ClickHouse: -clickhouse-%s-password-file and %s are mutually exclusive",
			role,
			environmentName,
		)
	}
	contents, err := readClickHouseCredentialFile(path)
	if err != nil {
		return "", fmt.Errorf(
			"configure ClickHouse: load %s password file: %w",
			role,
			err,
		)
	}
	defer clear(contents)
	return string(contents), nil
}

func readClickHouseCredentialFile(path string) ([]byte, error) {
	return readClickHouseCredentialFileWithHooks(
		path,
		clickHouseCredentialReadHooks{},
	)
}

func readClickHouseCredentialFileWithHooks(
	path string,
	hooks clickHouseCredentialReadHooks,
) ([]byte, error) {
	absolutePath, err := resolveClickHouseCredentialPath(path)
	if err != nil {
		return nil, err
	}
	contents, err := readStablePathFile(stablePathFileReadConfig{
		path:             absolutePath,
		maximumReadBytes: maximumClickHouseCredentialBytes + 1,
		hooks:            hooks,
		validateBefore:   validateClickHouseCredentialFile,
		validateOpen: func(file *os.File, info os.FileInfo) error {
			if err := validateClickHouseCredentialFile(info); err != nil {
				return err
			}
			if err := validateAdministratorTokenACL(file); err != nil {
				return errors.New(
					"credential file has unsupported access-control metadata",
				)
			}
			if info.Size() < 1 ||
				info.Size() > maximumClickHouseCredentialBytes+1 {
				return errors.New("credential file has an invalid size")
			}
			return nil
		},
		validateAfterPath: validateClickHouseCredentialFile,
		sameState:         sameClickHouseCredentialFileState,
		messages: stablePathFileReadMessages{
			inspectPath:         "inspect credential file",
			openPath:            "open credential file",
			invalidDescriptor:   "open credential file: invalid file descriptor",
			inspectOpen:         "inspect open credential file",
			changedWhileOpening: "credential file changed while it was opened",
			read:                "read credential file",
			overflow:            "credential file changed while it was read",
			changedWhileReading: "credential file changed while it was read",
			reinspectOpen:       "reinspect open credential file",
			reinspectPath:       "reinspect credential file",
			close:               "close credential file",
		},
	})
	if err != nil {
		return nil, err
	}
	defer clear(contents)

	credential := contents
	if credential[len(credential)-1] == '\n' {
		credential = credential[:len(credential)-1]
	}
	if len(credential) == 0 || len(credential) > maximumClickHouseCredentialBytes {
		return nil, errors.New("credential file has an invalid size")
	}
	return append([]byte(nil), credential...), nil
}

func resolveClickHouseCredentialPath(path string) (string, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return "", errors.New("credential file path is required")
	}
	if strings.IndexByte(path, 0) >= 0 {
		return "", errors.New("credential file path contains a NUL byte")
	}
	absolutePath, err := filepath.Abs(path)
	if err != nil {
		return "", fmt.Errorf("resolve credential file path: %w", err)
	}
	return absolutePath, nil
}

func validateClickHouseCredentialFile(info os.FileInfo) error {
	if info == nil || !info.Mode().IsRegular() {
		return errors.New("credential file must be a regular file")
	}
	mode := info.Mode()
	permissions := mode.Perm()
	if permissions&0o400 == 0 || permissions&0o133 != 0 {
		return errors.New(
			"credential file must be owner-readable without execute or group/other write permissions",
		)
	}
	if mode&(os.ModeSetuid|os.ModeSetgid|os.ModeSticky) != 0 {
		return errors.New("credential file must not have special permission bits")
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || stat == nil {
		return errors.New("credential file metadata is unavailable")
	}
	if stat.Nlink != 1 {
		return errors.New("credential file must have exactly one hard link")
	}
	return nil
}

func sameClickHouseCredentialFileState(left, right os.FileInfo) bool {
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
