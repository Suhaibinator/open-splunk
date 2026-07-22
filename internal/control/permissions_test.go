package control

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

func TestOpenSecuresDatabaseAndSQLiteSidecars(t *testing.T) {
	t.Parallel()
	databasePath := filepath.Join(t.TempDir(), "control.sqlite")
	file, err := os.OpenFile(databasePath, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o666)
	if err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(databasePath, 0o666); err != nil {
		t.Fatal(err)
	}

	db, err := Open(context.Background(), databasePath)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	for _, candidate := range []string{databasePath, databasePath + "-wal", databasePath + "-shm", databasePath + "-journal"} {
		info, statErr := os.Stat(candidate)
		if os.IsNotExist(statErr) && candidate != databasePath {
			continue
		}
		if statErr != nil {
			t.Fatalf("stat %s: %v", filepath.Base(candidate), statErr)
		}
		if permissions := info.Mode().Perm(); permissions != 0o600 {
			t.Errorf("%s permissions = %#o, want 0600", filepath.Base(candidate), permissions)
		}
	}
}

func TestOpenRejectsSymlinkDatabase(t *testing.T) {
	t.Parallel()
	directory := t.TempDir()
	target := filepath.Join(directory, "target.sqlite")
	if err := os.WriteFile(target, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(directory, "control.sqlite")
	if err := os.Symlink(target, link); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(context.Background(), link); err == nil {
		t.Fatal("Open accepted a symlink database path")
	}
}
