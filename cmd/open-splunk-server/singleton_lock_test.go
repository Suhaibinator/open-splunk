package main

import (
	"errors"
	"path/filepath"
	"testing"
)

func TestServerLockExcludesSameHostInstancesAndReleases(t *testing.T) {
	temporaryDirectory := t.TempDir()
	databasePath := filepath.Join(temporaryDirectory, "control.db")
	otherDatabasePath := filepath.Join(temporaryDirectory, "other-control.db")
	singletonPath := filepath.Join(temporaryDirectory, "host.lock")

	first, err := acquireServerLockAt(databasePath, singletonPath)
	if err != nil {
		t.Fatalf("acquire first lock: %v", err)
	}
	second, err := acquireServerLockAt(databasePath, singletonPath)
	if second != nil {
		_ = second.Close()
		t.Fatal("second lock acquisition succeeded")
	}
	if !errors.Is(err, errServerAlreadyRunning) {
		t.Fatalf("second lock error = %v, want errServerAlreadyRunning", err)
	}
	other, err := acquireServerLockAt(otherDatabasePath, singletonPath)
	if other != nil {
		_ = other.Close()
		t.Fatal("different control database bypassed the host singleton")
	}
	if !errors.Is(err, errServerAlreadyRunning) {
		t.Fatalf("different-database lock error = %v, want errServerAlreadyRunning", err)
	}

	if err := first.Close(); err != nil {
		t.Fatalf("release first lock: %v", err)
	}
	third, err := acquireServerLockAt(otherDatabasePath, singletonPath)
	if err != nil {
		t.Fatalf("reacquire lock: %v", err)
	}
	if err := third.Close(); err != nil {
		t.Fatalf("release third lock: %v", err)
	}

	collisionDatabase := filepath.Join(temporaryDirectory, "collision.db")
	collision, err := acquireServerLockAt(collisionDatabase, collisionDatabase+".server.lock")
	if err != nil {
		t.Fatalf("acquire coincident global and sidecar lock: %v", err)
	}
	if err := collision.Close(); err != nil {
		t.Fatalf("release coincident lock: %v", err)
	}
}

func TestServerLockRejectsNonPersistentPaths(t *testing.T) {
	t.Parallel()
	singletonPath := filepath.Join(t.TempDir(), "host.lock")
	for _, path := range []string{"", "   ", ":memory:"} {
		if lock, err := acquireServerLockAt(path, singletonPath); err == nil {
			_ = lock.Close()
			t.Fatalf("acquireServerLockAt(%q) succeeded", path)
		}
	}
}
