//go:build darwin || linux

package recoveryset

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const testAttestedArchiveName = "0123456789abcdef0123456789abcdef.tar.zst"

func TestDeleteAttestedArchiveRemovesExactArchiveAndIsAbsentIdempotent(
	t *testing.T,
) {
	t.Parallel()

	root, policy := newTestArchiveRoot(t)
	archivePath := filepath.Join(root, testAttestedArchiveName)
	if err := os.WriteFile(archivePath, []byte("operator-attested archive"), policy.File.Mode); err != nil {
		t.Fatal(err)
	}
	options := DeleteAttestedArchiveOptions{
		ArchiveRoot:          root,
		ArchiveName:          testAttestedArchiveName,
		ConfirmedArchiveName: testAttestedArchiveName,
		ArchiveOwnership:     policy,
	}
	if err := DeleteAttestedArchive(t.Context(), options); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(archivePath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("removed operator-attested archive still exists: %v", err)
	}
	if err := DeleteAttestedArchive(t.Context(), options); err != nil {
		t.Fatalf("remove already absent operator-attested archive: %v", err)
	}
}

func TestDeleteAttestedArchiveRequiresLiveContext(t *testing.T) {
	t.Parallel()

	root, policy := newTestArchiveRoot(t)
	archivePath := filepath.Join(root, testAttestedArchiveName)
	if err := os.WriteFile(archivePath, []byte("retain"), policy.File.Mode); err != nil {
		t.Fatal(err)
	}
	options := DeleteAttestedArchiveOptions{
		ArchiveRoot:          root,
		ArchiveName:          testAttestedArchiveName,
		ConfirmedArchiveName: testAttestedArchiveName,
		ArchiveOwnership:     policy,
	}
	//nolint:staticcheck // The exported boundary must reject a nil context without panicking.
	if err := DeleteAttestedArchive(nil, options); err == nil ||
		!strings.Contains(err.Error(), "context is required") {
		t.Fatalf("nil-context error = %v", err)
	}
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	if err := DeleteAttestedArchive(canceled, options); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled-context error = %v, want context.Canceled", err)
	}
	if _, err := os.Lstat(archivePath); err != nil {
		t.Fatalf("context rejection changed archive: %v", err)
	}
}

func TestDeleteAttestedArchiveRejectsInvalidRootAndName(t *testing.T) {
	t.Parallel()

	root, policy := newTestArchiveRoot(t)
	for name, options := range map[string]DeleteAttestedArchiveOptions{
		"empty root": {
			ArchiveName: testAttestedArchiveName, ArchiveOwnership: policy,
		},
		"relative root": {
			ArchiveRoot: "recovery", ArchiveName: testAttestedArchiveName, ArchiveOwnership: policy,
		},
		"filesystem root": {
			ArchiveRoot: string(filepath.Separator), ArchiveName: testAttestedArchiveName, ArchiveOwnership: policy,
		},
		"unclean root": {
			ArchiveRoot: root + string(filepath.Separator) + "child" + string(filepath.Separator) + "..",
			ArchiveName: testAttestedArchiveName, ArchiveOwnership: policy,
		},
		"empty name": {
			ArchiveRoot: root, ArchiveOwnership: policy,
		},
		"short ID": {
			ArchiveRoot: root, ArchiveName: "0123456789abcdef.tar.zst", ArchiveOwnership: policy,
		},
		"uppercase ID": {
			ArchiveRoot: root,
			ArchiveName: "0123456789ABCDEF0123456789abcdef.tar.zst", ArchiveOwnership: policy,
		},
		"nonhex ID": {
			ArchiveRoot: root,
			ArchiveName: "g123456789abcdef0123456789abcdef.tar.zst", ArchiveOwnership: policy,
		},
		"wrong suffix": {
			ArchiveRoot: root,
			ArchiveName: "0123456789abcdef0123456789abcdef.zip", ArchiveOwnership: policy,
		},
		"path name": {
			ArchiveRoot: root,
			ArchiveName: "../0123456789abcdef0123456789abc.tar.zst", ArchiveOwnership: policy,
		},
		"mismatched confirmation": {
			ArchiveRoot:          root,
			ArchiveName:          testAttestedArchiveName,
			ConfirmedArchiveName: "fedcba9876543210fedcba9876543210.tar.zst",
			ArchiveOwnership:     policy,
		},
	} {
		name, options := name, options
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			if err := DeleteAttestedArchive(t.Context(), options); err == nil {
				t.Fatal("invalid cleanup options succeeded")
			}
		})
	}
}

func TestDeleteAttestedArchiveRejectsUnsafeExistingObject(t *testing.T) {
	t.Parallel()

	for name, create := range unsafeArchiveFixtures() {
		name, create := name, create
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			root, policy := newTestArchiveRoot(t)
			archivePath := filepath.Join(root, testAttestedArchiveName)
			create(t, root, archivePath)
			err := DeleteAttestedArchive(t.Context(), DeleteAttestedArchiveOptions{
				ArchiveRoot:          root,
				ArchiveName:          testAttestedArchiveName,
				ConfirmedArchiveName: testAttestedArchiveName,
				ArchiveOwnership:     policy,
			})
			if err == nil {
				t.Fatal("unsafe existing archive was removed")
			}
			if _, statErr := os.Lstat(archivePath); statErr != nil {
				t.Fatalf("unsafe existing archive changed: %v", statErr)
			}
		})
	}
}

func TestDeleteAttestedArchiveRejectsOwnershipMismatch(t *testing.T) {
	t.Parallel()

	for name, mutate := range map[string]func(*ArchiveOwnershipPolicy){
		"root owner": func(policy *ArchiveOwnershipPolicy) {
			policy.Root.UID = mismatchedArchiveTestID(policy.Root.UID)
		},
		"archive owner": func(policy *ArchiveOwnershipPolicy) {
			policy.File.UID = mismatchedArchiveTestID(policy.File.UID)
		},
		"archive group": func(policy *ArchiveOwnershipPolicy) {
			policy.File.GID = mismatchedArchiveTestID(policy.File.GID)
		},
	} {
		name, mutate := name, mutate
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			root, policy := newTestArchiveRoot(t)
			archivePath := filepath.Join(root, testAttestedArchiveName)
			if err := os.WriteFile(archivePath, []byte("retain"), policy.File.Mode); err != nil {
				t.Fatal(err)
			}
			mutate(&policy)
			if err := DeleteAttestedArchive(t.Context(), DeleteAttestedArchiveOptions{
				ArchiveRoot:          root,
				ArchiveName:          testAttestedArchiveName,
				ConfirmedArchiveName: testAttestedArchiveName,
				ArchiveOwnership:     policy,
			}); err == nil {
				t.Fatal("ownership mismatch was accepted")
			}
			if _, err := os.Lstat(archivePath); err != nil {
				t.Fatalf("ownership rejection changed archive: %v", err)
			}
		})
	}
}

func TestDeleteAttestedArchiveRejectsExtendedACL(t *testing.T) {
	t.Parallel()

	for _, target := range []string{"root", "archive"} {
		target := target
		t.Run(target, func(t *testing.T) {
			t.Parallel()

			root, policy := newTestArchiveRoot(t)
			archivePath := filepath.Join(root, testAttestedArchiveName)
			if err := os.WriteFile(archivePath, []byte("retain"), policy.File.Mode); err != nil {
				t.Fatal(err)
			}
			aclPath := archivePath
			permissions := byte(0o6)
			if target == "root" {
				aclPath = root
				permissions = 0o7
			}
			addArchiveTestACL(t, aclPath, permissions)
			if err := DeleteAttestedArchive(t.Context(), DeleteAttestedArchiveOptions{
				ArchiveRoot:          root,
				ArchiveName:          testAttestedArchiveName,
				ConfirmedArchiveName: testAttestedArchiveName,
				ArchiveOwnership:     policy,
			}); err == nil || !strings.Contains(err.Error(), "extended ACL") {
				t.Fatalf("extended-ACL error = %v", err)
			}
			if _, err := os.Lstat(archivePath); err != nil {
				t.Fatalf("ACL rejection changed archive: %v", err)
			}
		})
	}
}
