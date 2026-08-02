//go:build darwin || linux

package recoveryset

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestOpenArchiveDirectoryRejectsUnsafeRootMetadata(t *testing.T) {
	t.Parallel()

	tests := map[string]func(*testing.T, string, *ArchiveOwnershipPolicy) string{
		"wrong mode": func(t *testing.T, root string, _ *ArchiveOwnershipPolicy) string {
			if err := os.Chmod(root, 0o750); err != nil {
				t.Fatal(err)
			}
			return root
		},
		"wrong owner": func(_ *testing.T, root string, policy *ArchiveOwnershipPolicy) string {
			policy.Root.UID = mismatchedArchiveTestID(policy.Root.UID)
			return root
		},
		"wrong group": func(_ *testing.T, root string, policy *ArchiveOwnershipPolicy) string {
			policy.Root.GID = mismatchedArchiveTestID(policy.Root.GID)
			return root
		},
		"unexpected special bit": func(t *testing.T, root string, _ *ArchiveOwnershipPolicy) string {
			if err := os.Chmod(root, 0o700|os.ModeSticky); err != nil {
				t.Fatal(err)
			}
			info, err := os.Lstat(root)
			if err != nil {
				t.Fatal(err)
			}
			if info.Mode()&os.ModeSticky == 0 {
				t.Skip("filesystem did not retain the sticky-bit fixture")
			}
			return root
		},
		"symlink": func(t *testing.T, root string, _ *ArchiveOwnershipPolicy) string {
			link := filepath.Join(filepath.Dir(root), "archive-link")
			if err := os.Symlink(root, link); err != nil {
				t.Fatal(err)
			}
			return link
		},
	}
	for name, arrange := range tests {
		name, arrange := name, arrange
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			root, policy := newTestArchiveRoot(t)
			path := arrange(t, root, &policy)
			if directory, err := openArchiveDirectory(path, policy); err == nil {
				_ = directory.close()
				t.Fatal("unsafe archive root was accepted")
			}
		})
	}
}

func TestOpenArchiveDirectoryAcceptsConfiguredSpecialBits(t *testing.T) {
	t.Parallel()

	root, policy := newTestArchiveRoot(t)
	configuredMode := os.FileMode(0o750) | os.ModeSetgid
	if err := os.Chmod(root, configuredMode); err != nil {
		t.Fatal(err)
	}
	info, err := os.Lstat(root)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode()&os.ModeSetgid == 0 {
		t.Skip("filesystem did not retain the setgid fixture")
	}
	policy.Root.Mode = configuredMode
	directory, err := openArchiveDirectory(root, policy)
	if err != nil {
		t.Fatal(err)
	}
	if err := directory.close(); err != nil {
		t.Fatal(err)
	}
}

func TestArchiveDirectoryRevalidateRejectsRootMetadataChange(t *testing.T) {
	t.Parallel()

	root, policy := newTestArchiveRoot(t)
	directory, err := openArchiveDirectory(root, policy)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := directory.close(); err != nil {
			t.Error(err)
		}
	})
	if err := os.Chmod(root, 0o750); err != nil {
		t.Fatal(err)
	}
	if err := directory.revalidate(); err == nil {
		t.Fatal("archive root mode change was accepted")
	}
}

func TestArchiveDirectoryRevalidateRejectsRootIdentityChange(t *testing.T) {
	t.Parallel()

	root, policy := newTestArchiveRoot(t)
	directory, err := openArchiveDirectory(root, policy)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := directory.close(); err != nil {
			t.Error(err)
		}
	})
	moved := root + "-moved"
	if err := os.Rename(root, moved); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(root, policy.Root.Mode.Perm()); err != nil {
		t.Fatal(err)
	}
	if err := directory.revalidate(); err == nil {
		t.Fatal("archive root identity replacement was accepted")
	}
}

func TestInspectArchiveRejectsUnsafeMetadata(t *testing.T) {
	t.Parallel()

	for name, create := range unsafeArchiveFixtures() {
		name, create := name, create
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			root, policy := newTestArchiveRoot(t)
			archiveName := "unsafe.tar.zst"
			archivePath := filepath.Join(root, archiveName)
			create(t, root, archivePath)
			directory, err := openArchiveDirectory(root, policy)
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if err := directory.close(); err != nil {
					t.Error(err)
				}
			}()

			if _, err := directory.inspectArchive(
				t.Context(),
				archiveName,
				1,
				maximumClickHouseArchiveBytes,
			); err == nil {
				t.Fatal("unsafe archive inspection succeeded")
			}
		})
	}
}

func TestRemoveKnownArchiveUsesBoundedMetadataOnlyCleanup(t *testing.T) {
	t.Parallel()

	root, policy := newTestArchiveRoot(t)
	directory, err := openArchiveDirectory(root, policy)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := directory.close(); err != nil {
			t.Error(err)
		}
	})
	archiveName := "large-sparse.tar.zst"
	archivePath := filepath.Join(root, archiveName)
	file, err := os.OpenFile(archivePath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	// A large sparse fixture makes accidental content hashing prohibitively
	// expensive while requiring no corresponding disk allocation. Cleanup is
	// intentionally bounded to descriptor metadata.
	const sparseArchiveBytes = int64(16 << 30)
	truncateErr := file.Truncate(sparseArchiveBytes)
	closeErr := file.Close()
	if err := errors.Join(truncateErr, closeErr); err != nil {
		t.Fatal(err)
	}
	removed, err := directory.removeKnownArchive(archiveName)
	if err != nil {
		t.Fatal(err)
	}
	if !removed {
		t.Fatal("existing archive cleanup reported no removal")
	}
	if _, err := os.Lstat(archivePath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("cleaned sparse archive still exists: %v", err)
	}
	removed, err = directory.removeKnownArchive(archiveName)
	if err != nil {
		t.Fatalf("idempotent missing-archive cleanup: %v", err)
	}
	if removed {
		t.Fatal("missing archive cleanup reported a removal")
	}
}

func TestRemoveKnownArchiveRejectsUnsafeMetadataWithoutDeletion(t *testing.T) {
	t.Parallel()

	for name, create := range unsafeArchiveFixtures() {
		name, create := name, create
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			root, policy := newTestArchiveRoot(t)
			archiveName := "unsafe.tar.zst"
			archivePath := filepath.Join(root, archiveName)
			create(t, root, archivePath)
			directory, err := openArchiveDirectory(root, policy)
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if err := directory.close(); err != nil {
					t.Error(err)
				}
			}()

			if _, err := directory.removeKnownArchive(archiveName); err == nil {
				t.Fatal("unsafe archive cleanup succeeded")
			}
			if _, err := os.Lstat(archivePath); err != nil {
				t.Fatalf("unsafe archive was deleted or replaced: %v", err)
			}
		})
	}
}

func newTestArchiveRoot(t *testing.T) (string, ArchiveOwnershipPolicy) {
	t.Helper()
	root := t.TempDir()
	if err := os.Chmod(root, 0o700); err != nil {
		t.Fatal(err)
	}
	return root, ArchiveOwnershipPolicy{
		Root: ArchiveObjectOwnershipPolicy{
			UID:  os.Geteuid(),
			GID:  os.Getegid(),
			Mode: 0o700,
		},
		File: ArchiveObjectOwnershipPolicy{
			UID:  os.Geteuid(),
			GID:  os.Getegid(),
			Mode: 0o600,
		},
	}
}

func mismatchedArchiveTestID(id int) int {
	if id == 0 {
		return 1
	}
	return 0
}

func unsafeArchiveFixtures() map[string]func(*testing.T, string, string) {
	return map[string]func(*testing.T, string, string){
		"symlink": func(t *testing.T, root, path string) {
			target := filepath.Join(root, "target")
			if err := os.WriteFile(target, []byte("target"), 0o600); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink(target, path); err != nil {
				t.Fatal(err)
			}
		},
		"hard link": func(t *testing.T, root, path string) {
			target := filepath.Join(root, "target")
			if err := os.WriteFile(target, []byte("target"), 0o600); err != nil {
				t.Fatal(err)
			}
			if err := os.Link(target, path); err != nil {
				t.Fatal(err)
			}
		},
		"wrong mode": func(t *testing.T, _, path string) {
			if err := os.WriteFile(path, []byte("wide"), 0o644); err != nil {
				t.Fatal(err)
			}
		},
		"directory": func(t *testing.T, _, path string) {
			if err := os.Mkdir(path, 0o700); err != nil {
				t.Fatal(err)
			}
		},
	}
}
