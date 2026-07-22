package main

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

const masterKeyBytes = 32

func loadOrCreateMasterKey(path string, random io.Reader) ([]byte, error) {
	absPath, err := resolveMasterKeyPath(path)
	if err != nil {
		return nil, err
	}
	key, err := readMasterKey(absPath)
	if err == nil {
		return key, nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	if random == nil {
		random = rand.Reader
	}

	key = make([]byte, masterKeyBytes)
	if _, err := io.ReadFull(random, key); err != nil {
		return nil, errors.New("generate server master key: secure randomness unavailable")
	}
	directoryPath := filepath.Dir(absPath)
	file, err := os.CreateTemp(directoryPath, ".open-splunk-master-key-*")
	if err != nil {
		return nil, fmt.Errorf("create temporary server master key: %w", err)
	}
	temporaryPath := file.Name()
	defer func() {
		_ = file.Close()
		_ = os.Remove(temporaryPath)
	}()
	if err := file.Chmod(0o600); err != nil {
		return nil, fmt.Errorf("secure temporary server master key: %w", err)
	}
	if written, err := file.Write(key); err != nil || written != len(key) {
		if err == nil {
			err = io.ErrShortWrite
		}
		return nil, fmt.Errorf("write server master key: %w", err)
	}
	if err := file.Sync(); err != nil {
		return nil, fmt.Errorf("sync server master key: %w", err)
	}
	if err := file.Close(); err != nil {
		return nil, fmt.Errorf("close server master key: %w", err)
	}
	// A same-directory hard link publishes the fully synced inode atomically
	// without replacing an existing key. The final pathname therefore never
	// refers to partially written key bytes.
	if err := os.Link(temporaryPath, absPath); errors.Is(err, os.ErrExist) {
		return loadOrCreateMasterKey(absPath, random)
	} else if err != nil {
		return nil, fmt.Errorf("publish server master key: %w", err)
	}
	if err := syncDirectory(directoryPath); err != nil {
		return nil, fmt.Errorf("sync server master key directory: %w", err)
	}
	if err := os.Remove(temporaryPath); err != nil {
		return nil, fmt.Errorf("remove temporary server master key: %w", err)
	}
	if err := syncDirectory(directoryPath); err != nil {
		return nil, fmt.Errorf("sync server master key directory cleanup: %w", err)
	}
	return key, nil
}

func readMasterKey(path string) ([]byte, error) {
	before, err := os.Lstat(path)
	if err != nil {
		return nil, fmt.Errorf("inspect server master key: %w", err)
	}
	if !before.Mode().IsRegular() {
		return nil, errors.New("server master key must be a regular file")
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open server master key: %w", err)
	}
	closed := false
	defer func() {
		if !closed {
			_ = file.Close()
		}
	}()
	opened, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("inspect open server master key: %w", err)
	}
	if !opened.Mode().IsRegular() || !os.SameFile(before, opened) {
		return nil, errors.New("server master key changed while it was opened")
	}
	if opened.Size() != masterKeyBytes {
		return nil, fmt.Errorf("server master key must contain exactly %d bytes", masterKeyBytes)
	}
	if err := file.Chmod(0o600); err != nil {
		return nil, fmt.Errorf("secure server master key: %w", err)
	}
	if err := file.Sync(); err != nil {
		return nil, fmt.Errorf("sync server master key permissions: %w", err)
	}
	key := make([]byte, masterKeyBytes)
	if _, err := io.ReadFull(file, key); err != nil {
		return nil, fmt.Errorf("read server master key: %w", err)
	}
	var extra [1]byte
	if count, err := file.Read(extra[:]); count != 0 || !errors.Is(err, io.EOF) {
		if err == nil {
			err = errors.New("master key grew while it was read")
		}
		return nil, fmt.Errorf("read server master key terminator: %w", err)
	}
	after, err := os.Lstat(path)
	if err != nil {
		return nil, fmt.Errorf("reinspect server master key: %w", err)
	}
	if !after.Mode().IsRegular() || !os.SameFile(opened, after) {
		return nil, errors.New("server master key changed while it was read")
	}
	if err := file.Close(); err != nil {
		return nil, fmt.Errorf("close server master key: %w", err)
	}
	closed = true
	return key, nil
}

func resolveMasterKeyPath(path string) (string, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return "", errors.New("server master key path is required")
	}
	absPath, err := filepath.Abs(path)
	if err != nil {
		return "", fmt.Errorf("resolve server master key path: %w", err)
	}
	return absPath, nil
}

func syncDirectory(path string) error {
	directory, err := os.Open(path)
	if err != nil {
		return err
	}
	if err := directory.Sync(); err != nil {
		_ = directory.Close()
		return err
	}
	return directory.Close()
}

func deriveServerKey(master []byte, purpose string) ([]byte, error) {
	if len(master) != masterKeyBytes {
		return nil, fmt.Errorf("derive server key: master key must contain exactly %d bytes", masterKeyBytes)
	}
	purpose = strings.TrimSpace(purpose)
	if purpose == "" {
		return nil, errors.New("derive server key: purpose is required")
	}
	mac := hmac.New(sha256.New, master)
	_, _ = mac.Write([]byte("open-splunk/server-key/v1\x00"))
	_, _ = mac.Write([]byte(purpose))
	return mac.Sum(nil), nil
}
