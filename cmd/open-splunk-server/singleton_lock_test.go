package main

import (
	"errors"
	"os"
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

func TestConfiguredServerSingletonLockPath(t *testing.T) {
	t.Run("default", func(t *testing.T) {
		t.Setenv(serverSingletonLockPathEnv, "")
		if err := os.Unsetenv(serverSingletonLockPathEnv); err != nil {
			t.Fatalf("unset lock path: %v", err)
		}
		got, err := configuredServerSingletonLockPath()
		if err != nil {
			t.Fatalf("resolve default lock path: %v", err)
		}
		if got != hostSingletonLockPath {
			t.Fatalf("default lock path = %q, want %q", got, hostSingletonLockPath)
		}
	})

	t.Run("exact override", func(t *testing.T) {
		want := filepath.Join(t.TempDir(), "deployment.lock")
		t.Setenv(serverSingletonLockPathEnv, want)
		got, err := configuredServerSingletonLockPath()
		if err != nil {
			t.Fatalf("resolve configured lock path: %v", err)
		}
		if got != want {
			t.Fatalf("configured lock path = %q, want %q", got, want)
		}
	})

	for _, value := range []string{"", "relative.lock", "/tmp/../tmp/lock", " /tmp/lock", "/tmp/lock "} {
		t.Run("reject "+value, func(t *testing.T) {
			t.Setenv(serverSingletonLockPathEnv, value)
			if _, err := configuredServerSingletonLockPath(); err == nil {
				t.Fatalf("configuredServerSingletonLockPath accepted %q", value)
			}
		})
	}
}

func TestServerLockUsesConfiguredDeploymentPath(t *testing.T) {
	lockDirectory := t.TempDir()
	if err := os.Chmod(lockDirectory, 0o700); err != nil {
		t.Fatal(err)
	}
	lockPath := filepath.Join(lockDirectory, "deployment.lock")
	t.Setenv(serverSingletonLockPathEnv, lockPath)
	databasePath := filepath.Join(t.TempDir(), "control.db")

	lock, err := acquireServerLock(databasePath)
	if err != nil {
		t.Fatalf("acquire configured server lock: %v", err)
	}
	defer func() {
		if err := lock.Close(); err != nil {
			t.Errorf("close configured server lock: %v", err)
		}
	}()
	if _, err := lock.fileForExactPath(lockPath); err != nil {
		t.Fatalf("configured deployment lock is not held: %v", err)
	}
}

func TestConfiguredServerLockRejectsUnsafePrivateDirectory(t *testing.T) {
	for _, testCase := range []struct {
		name string
		path func(*testing.T) string
	}{
		{
			name: "permissive",
			path: func(t *testing.T) string {
				parent := filepath.Join(t.TempDir(), "permissive")
				if err := os.Mkdir(parent, 0o755); err != nil {
					t.Fatal(err)
				}
				if err := os.Chmod(parent, 0o755); err != nil {
					t.Fatal(err)
				}
				return filepath.Join(parent, "deployment.lock")
			},
		},
		{
			name: "symlink",
			path: func(t *testing.T) string {
				target := filepath.Join(t.TempDir(), "target")
				if err := os.Mkdir(target, 0o700); err != nil {
					t.Fatal(err)
				}
				parent := filepath.Join(t.TempDir(), "redirected")
				if err := os.Symlink(target, parent); err != nil {
					t.Fatal(err)
				}
				return filepath.Join(parent, "deployment.lock")
			},
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			path := testCase.path(t)
			t.Setenv(serverSingletonLockPathEnv, path)
			lock, err := acquireHostServerLock()
			if lock != nil {
				_ = lock.Close()
				t.Fatal("configured lock accepted an unsafe private directory")
			}
			if err == nil {
				t.Fatal("configured lock returned no error for an unsafe private directory")
			}
			if _, statErr := os.Lstat(path); !errors.Is(statErr, os.ErrNotExist) {
				t.Fatalf("unsafe directory validation created lock: %v", statErr)
			}
		})
	}
}

func TestServerLockRejectsUnsafeExistingInodeMetadata(t *testing.T) {
	t.Parallel()

	for _, testCase := range []struct {
		name string
		seed func(*testing.T, string)
	}{
		{
			name: "nonempty",
			seed: func(t *testing.T, path string) {
				if err := os.WriteFile(path, []byte("not an empty lock"), 0o600); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "mode",
			seed: func(t *testing.T, path string) {
				if err := os.WriteFile(path, nil, 0o600); err != nil {
					t.Fatal(err)
				}
				if err := os.Chmod(path, 0o640); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "hardlink",
			seed: func(t *testing.T, path string) {
				target := filepath.Join(filepath.Dir(path), "other-link")
				if err := os.WriteFile(target, nil, 0o600); err != nil {
					t.Fatal(err)
				}
				if err := os.Link(target, path); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "directory",
			seed: func(t *testing.T, path string) {
				if err := os.Mkdir(path, 0o700); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "symlink",
			seed: func(t *testing.T, path string) {
				target := filepath.Join(filepath.Dir(path), "target")
				if err := os.WriteFile(target, nil, 0o600); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(target, path); err != nil {
					t.Fatal(err)
				}
			},
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			path := filepath.Join(t.TempDir(), "deployment.lock")
			testCase.seed(t, path)
			lock, err := acquireHostServerLockAt(path)
			if lock != nil {
				_ = lock.Close()
				t.Fatal("server lock accepted unsafe existing inode metadata")
			}
			if err == nil {
				t.Fatal("server lock returned no error for unsafe existing inode metadata")
			}
		})
	}
}
