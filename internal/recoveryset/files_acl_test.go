//go:build darwin || linux

package recoveryset

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestOpenArchiveDirectoryRejectsExtendedACL(t *testing.T) {
	t.Parallel()

	root, policy := newTestArchiveRoot(t)
	addArchiveTestACL(t, root, 0o7)
	assertArchiveTestMode(t, root, policy.Root.Mode)
	if directory, err := openArchiveDirectory(root, policy); err == nil {
		_ = directory.close()
		t.Fatal("archive root with an extended ACL was accepted")
	} else if !strings.Contains(err.Error(), "extended ACL") {
		t.Fatalf("archive root ACL error = %v", err)
	}
}

func TestArchiveDirectoryRevalidateRejectsExtendedACL(t *testing.T) {
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
	addArchiveTestACL(t, root, 0o7)
	assertArchiveTestMode(t, root, policy.Root.Mode)
	if err := directory.revalidate(); err == nil {
		t.Fatal("archive root ACL added after open was accepted")
	} else if !strings.Contains(err.Error(), "extended ACL") {
		t.Fatalf("archive root revalidation ACL error = %v", err)
	}
}

func TestArchiveOperationsRejectExtendedACLWithoutDeletion(t *testing.T) {
	t.Parallel()

	for _, operation := range []string{"inspect", "cleanup"} {
		operation := operation
		t.Run(operation, func(t *testing.T) {
			t.Parallel()

			root, policy := newTestArchiveRoot(t)
			directory, err := openArchiveDirectory(root, policy)
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if err := directory.close(); err != nil {
					t.Error(err)
				}
			}()
			archiveName := operation + ".tar.zst"
			archivePath := filepath.Join(root, archiveName)
			if err := os.WriteFile(archivePath, []byte("archive"), policy.File.Mode); err != nil {
				t.Fatal(err)
			}
			addArchiveTestACL(t, archivePath, 0o6)
			assertArchiveTestMode(t, archivePath, policy.File.Mode)

			switch operation {
			case "inspect":
				_, err = directory.inspectArchive(
					t.Context(),
					archiveName,
					1,
					maximumClickHouseArchiveBytes,
				)
			case "cleanup":
				_, err = directory.removeKnownArchive(archiveName)
			default:
				t.Fatalf("unknown archive operation %q", operation)
			}
			if err == nil {
				t.Fatalf("%s accepted an archive with an extended ACL", operation)
			}
			if !strings.Contains(err.Error(), "extended ACL") {
				t.Fatalf("%s ACL error = %v", operation, err)
			}
			if _, statErr := os.Lstat(archivePath); statErr != nil {
				t.Fatalf("ACL-bearing archive was deleted or replaced: %v", statErr)
			}
		})
	}
}

func assertArchiveTestMode(t *testing.T, path string, want os.FileMode) {
	t.Helper()
	info, err := os.Lstat(path)
	if err != nil {
		t.Fatal(err)
	}
	if got := archiveMetadataMode(info.Mode()); got != want {
		t.Fatalf("%s mode = %v, want %v", path, got, want)
	}
}
