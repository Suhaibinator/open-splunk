//go:build darwin || linux

package recoveryset

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/auth"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/controlbackup"
	"github.com/Suhaibinator/open-splunk/internal/privatefs"
	"github.com/Suhaibinator/open-splunk/migrations"
)

func TestCreateAndVerifyRecoverySetRoundTrip(t *testing.T) {
	t.Parallel()

	fixture := newRecoverySetFixture(t)
	var request NativeBackupRequest
	options := fixture.createOptions(func(
		ctx context.Context,
		got NativeBackupRequest,
	) (NativeBackupIdentity, error) {
		request = got
		if err := ctx.Err(); err != nil {
			return NativeBackupIdentity{}, err
		}
		if err := os.WriteFile(got.ArchivePath, []byte("native-clickhouse-archive"), 0o600); err != nil {
			return NativeBackupIdentity{}, err
		}
		return validNativeBackupIdentity(), nil
	})
	created, err := Create(t.Context(), options)
	if err != nil {
		t.Fatal(err)
	}
	if request.RecoverySetID != created.Manifest.RecoverySetID ||
		request.Database != clickHouseDatabase ||
		request.ArchiveDatabase != clickHouseArchiveDatabase(request.RecoverySetID) ||
		request.Disk != clickHouseRecoveryDisk ||
		request.ArchiveName != clickHouseArchiveName(request.RecoverySetID) ||
		request.ArchivePath != filepath.Join(fixture.archiveRoot, request.ArchiveName) {
		t.Fatalf("native backup request = %#v", request)
	}
	if created.ManifestSHA256 == "" {
		t.Fatal("created manifest SHA-256 is empty")
	}
	assertExactDirectoryEntries(t, fixture.destination, []string{
		controlPlaneDirectory,
		manifestFilename,
	})

	verified, err := Verify(t.Context(), fixture.verifyOptions())
	if err != nil {
		t.Fatal(err)
	}
	if verified != created {
		t.Fatalf("verified recovery set = %#v, want %#v", verified, created)
	}
	child, err := controlbackup.Verify(
		t.Context(),
		filepath.Join(fixture.destination, controlPlaneDirectory),
		fixture.release,
	)
	if err != nil {
		t.Fatal(err)
	}
	if child.RecoverySetID != verified.Manifest.RecoverySetID ||
		child.CreatedAtUnixMicro != verified.Manifest.CreatedAtUnixMicro {
		t.Fatalf("child identity = (%q, %d), outer = (%q, %d)",
			child.RecoverySetID,
			child.CreatedAtUnixMicro,
			verified.Manifest.RecoverySetID,
			verified.Manifest.CreatedAtUnixMicro,
		)
	}
}

func TestCreateClassifiesPostPublicationFailureWithoutDeletingMembers(t *testing.T) {
	t.Parallel()

	fixture := newRecoverySetFixture(t)
	injected := errors.New("injected post-publication failure")
	_, err := createWithHooks(
		t.Context(),
		fixture.createOptions(successfulNativeBackup),
		createHooks{
			afterPublication: func() error { return injected },
		},
	)
	if err == nil {
		t.Fatal("Create succeeded after a post-publication failure")
	}
	var publicationErr *PublicationStatusError
	if !errors.As(err, &publicationErr) || !errors.Is(err, injected) {
		t.Fatalf("post-publication error = %v, want typed status wrapping injected cause", err)
	}
	if publicationErr.Destination != fixture.destination ||
		publicationErr.Outcome != privatefs.RenameNoReplaceCompleted ||
		!strings.Contains(err.Error(), "do not delete its ClickHouse archive") {
		t.Fatalf("publication status = %#v, error = %v", publicationErr, err)
	}
	verified, verifyErr := Verify(t.Context(), fixture.verifyOptions())
	if verifyErr != nil {
		t.Fatalf("independently verify published recovery set: %v", verifyErr)
	}
	archivePath := filepath.Join(
		fixture.archiveRoot,
		verified.Manifest.ClickHouse.Archive.Name,
	)
	if _, statErr := os.Lstat(archivePath); statErr != nil {
		t.Fatalf("post-publication failure removed archive: %v", statErr)
	}
}

func TestCreateClassifiesRenameRevalidationFailureAsPublished(t *testing.T) {
	t.Parallel()

	fixture := newRecoverySetFixture(t)
	injected := errors.New("injected post-rename revalidation failure")
	_, err := createWithHooks(
		t.Context(),
		fixture.createOptions(successfulNativeBackup),
		createHooks{
			publish: func(
				sourceParent *privatefs.Directory,
				from string,
				source *privatefs.Directory,
				destination *privatefs.Directory,
				to string,
			) (privatefs.RenameNoReplaceOutcome, error) {
				outcome, renameErr := sourceParent.RenameDirectoryNoReplaceWithStatus(
					from,
					source,
					destination,
					to,
				)
				if renameErr != nil {
					return outcome, renameErr
				}
				return outcome, injected
			},
		},
	)
	if err == nil {
		t.Fatal("Create succeeded after a post-rename revalidation failure")
	}
	var publicationErr *PublicationStatusError
	if !errors.As(err, &publicationErr) || !errors.Is(err, injected) {
		t.Fatalf("rename publication error = %v, want typed status and injected cause", err)
	}
	if publicationErr.Destination != fixture.destination {
		t.Fatalf(
			"rename publication destination = %q, want %q",
			publicationErr.Destination,
			fixture.destination,
		)
	}
	if publicationErr.Outcome != privatefs.RenameNoReplaceCompleted {
		t.Fatalf("rename publication outcome = %v, want completed", publicationErr.Outcome)
	}
	verified, verifyErr := Verify(t.Context(), fixture.verifyOptions())
	if verifyErr != nil {
		t.Fatalf("independently verify rename-published recovery set: %v", verifyErr)
	}
	archivePath := filepath.Join(
		fixture.archiveRoot,
		verified.Manifest.ClickHouse.Archive.Name,
	)
	if _, statErr := os.Lstat(archivePath); statErr != nil {
		t.Fatalf("rename publication failure removed archive: %v", statErr)
	}
}

func TestCreatePreservesArchiveWhenPinnedStageNameIsReplaced(t *testing.T) {
	t.Parallel()

	fixture := newRecoverySetFixture(t)
	_, err := createWithHooks(
		t.Context(),
		fixture.createOptions(successfulNativeBackup),
		createHooks{
			publish: func(
				sourceParent *privatefs.Directory,
				from string,
				source *privatefs.Directory,
				destination *privatefs.Directory,
				to string,
			) (privatefs.RenameNoReplaceOutcome, error) {
				if renameErr := sourceParent.RenameNoReplace(
					from,
					destination,
					to,
				); renameErr != nil {
					return privatefs.RenameNoReplaceNotAttempted, renameErr
				}
				if mkdirErr := os.Mkdir(filepath.Join(sourceParent.Path(), from), 0o700); mkdirErr != nil {
					return privatefs.RenameNoReplaceCompleted, mkdirErr
				}
				return sourceParent.RenameDirectoryNoReplaceWithStatus(
					from,
					source,
					destination,
					to,
				)
			},
		},
	)
	if err == nil {
		t.Fatal("Create succeeded after the pinned stage name was replaced")
	}
	var publicationErr *PublicationStatusError
	if !errors.As(err, &publicationErr) {
		t.Fatalf("stage-replacement error = %v, want typed publication status", err)
	}
	if publicationErr.Outcome != privatefs.RenameNoReplaceCompleted {
		t.Fatalf("stage-replacement outcome = %v, want completed", publicationErr.Outcome)
	}
	verified, verifyErr := Verify(t.Context(), fixture.verifyOptions())
	if verifyErr != nil {
		t.Fatalf("verify stage-replacement publication: %v", verifyErr)
	}
	archivePath := filepath.Join(
		fixture.archiveRoot,
		verified.Manifest.ClickHouse.Archive.Name,
	)
	if _, statErr := os.Lstat(archivePath); statErr != nil {
		t.Fatalf("stage-replacement failure removed archive: %v", statErr)
	}
}

func TestCreatePreservesStageAndArchiveAfterAmbiguousPublication(t *testing.T) {
	t.Parallel()

	fixture := newRecoverySetFixture(t)
	injected := errors.New("injected ambiguous publication failure")
	_, err := createWithHooks(
		t.Context(),
		fixture.createOptions(successfulNativeBackup),
		createHooks{
			publish: func(
				_ *privatefs.Directory,
				_ string,
				_ *privatefs.Directory,
				_ *privatefs.Directory,
				_ string,
			) (privatefs.RenameNoReplaceOutcome, error) {
				return privatefs.RenameNoReplaceAmbiguous, injected
			},
		},
	)
	if err == nil {
		t.Fatal("Create succeeded after an ambiguous publication failure")
	}
	var publicationErr *PublicationStatusError
	if !errors.As(err, &publicationErr) || !errors.Is(err, injected) {
		t.Fatalf("ambiguous publication error = %v, want typed injected cause", err)
	}
	if publicationErr.Outcome != privatefs.RenameNoReplaceAmbiguous {
		t.Fatalf("ambiguous publication outcome = %v", publicationErr.Outcome)
	}
	entries, readErr := os.ReadDir(filepath.Dir(fixture.destination))
	if readErr != nil {
		t.Fatal(readErr)
	}
	if len(entries) != 1 ||
		!strings.HasPrefix(entries[0].Name(), ".deployment-recovery-set-tmp-") {
		t.Fatalf("preserved ambiguous stages = %v", entries)
	}
	preservedStage := filepath.Join(filepath.Dir(fixture.destination), entries[0].Name())
	verified, verifyErr := Verify(t.Context(), VerifyOptions{
		Source:           preservedStage,
		ArchiveRoot:      fixture.archiveRoot,
		ArchiveOwnership: fixture.archivePolicy,
		Release:          fixture.release,
	})
	if verifyErr != nil {
		t.Fatalf("verify preserved ambiguous stage: %v", verifyErr)
	}
	archivePath := filepath.Join(
		fixture.archiveRoot,
		verified.Manifest.ClickHouse.Archive.Name,
	)
	if _, statErr := os.Lstat(archivePath); statErr != nil {
		t.Fatalf("ambiguous publication removed archive: %v", statErr)
	}
}

func TestCreatePreservesWholeOuterStageAfterNestedChildPublicationFailure(t *testing.T) {
	t.Parallel()

	fixture := newRecoverySetFixture(t)
	injected := errors.New("injected completed child publication failure")
	nativeCalled := false
	_, err := createWithHooks(
		t.Context(),
		fixture.createOptions(func(
			context.Context,
			NativeBackupRequest,
		) (NativeBackupIdentity, error) {
			nativeCalled = true
			return NativeBackupIdentity{}, nil
		}),
		createHooks{
			createControlPlane: func(
				_ context.Context,
				options controlbackup.CreateOptions,
			) (controlbackup.Manifest, *privatefs.Directory, error) {
				if mkdirErr := os.Mkdir(options.Destination, 0o700); mkdirErr != nil {
					return controlbackup.Manifest{}, nil, mkdirErr
				}
				if writeErr := os.WriteFile(
					filepath.Join(options.Destination, "candidate-marker"),
					[]byte("preserve-me"),
					0o600,
				); writeErr != nil {
					return controlbackup.Manifest{}, nil, writeErr
				}
				return controlbackup.Manifest{}, nil, &controlbackup.PublicationStatusError{
					Destination: options.Destination,
					Outcome:     privatefs.RenameNoReplaceCompleted,
					Err:         injected,
				}
			},
		},
	)
	if err == nil || !errors.Is(err, injected) {
		t.Fatalf("nested publication failure = %v, want injected cause", err)
	}
	if _, ok := errors.AsType[*controlbackup.PublicationStatusError](err); !ok {
		t.Fatalf("nested publication failure = %v, want controlbackup status", err)
	}
	if !strings.Contains(err.Error(), "preserve the entire outer stage") {
		t.Fatalf("nested publication diagnostic = %v", err)
	}
	if nativeCalled {
		t.Fatal("native backup ran after ambiguous child publication")
	}
	entries, readErr := os.ReadDir(filepath.Dir(fixture.destination))
	if readErr != nil {
		t.Fatal(readErr)
	}
	if len(entries) != 1 ||
		!strings.HasPrefix(entries[0].Name(), ".deployment-recovery-set-tmp-") {
		t.Fatalf("preserved outer stages = %v", entries)
	}
	markerPath := filepath.Join(
		filepath.Dir(fixture.destination),
		entries[0].Name(),
		controlPlaneDirectory,
		"candidate-marker",
	)
	contents, readErr := os.ReadFile(markerPath)
	if readErr != nil || string(contents) != "preserve-me" {
		t.Fatalf("preserved child marker = %q, error=%v", contents, readErr)
	}
}

func TestCreatePreservesNameDiscoveredArchiveAfterNativeBackupFailure(t *testing.T) {
	t.Parallel()

	fixture := newRecoverySetFixture(t)
	var archivePath string
	_, err := Create(t.Context(), fixture.createOptions(func(
		_ context.Context,
		request NativeBackupRequest,
	) (NativeBackupIdentity, error) {
		archivePath = request.ArchivePath
		if err := os.WriteFile(archivePath, []byte("partial"), 0o600); err != nil {
			return NativeBackupIdentity{}, err
		}
		return NativeBackupIdentity{}, errors.New("injected backup failure")
	}))
	if err == nil {
		t.Fatal("Create succeeded after native backup failure")
	}
	if !strings.Contains(err.Error(), "ambiguous ownership") ||
		!strings.Contains(err.Error(), "explicit confirmation-bound archive cleanup") {
		t.Fatalf("native backup failure diagnostic = %v", err)
	}
	if _, statErr := os.Lstat(fixture.destination); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("destination exists after failure: %v", statErr)
	}
	contents, readErr := os.ReadFile(archivePath)
	if readErr != nil || string(contents) != "partial" {
		t.Fatalf("preserved ambiguous archive = %q, error=%v", contents, readErr)
	}
	assertExactDirectoryEntries(t, filepath.Dir(fixture.destination), nil)
}

func TestCreatePreservesNameDiscoveredArchiveAfterCallbackCancelsContext(t *testing.T) {
	t.Parallel()

	fixture := newRecoverySetFixture(t)
	ctx, cancel := context.WithCancel(t.Context())
	var archivePath string
	_, err := Create(ctx, fixture.createOptions(func(
		_ context.Context,
		request NativeBackupRequest,
	) (NativeBackupIdentity, error) {
		archivePath = request.ArchivePath
		if err := os.WriteFile(archivePath, []byte("partial"), 0o600); err != nil {
			return NativeBackupIdentity{}, err
		}
		cancel()
		return NativeBackupIdentity{}, errors.New("injected canceled backup failure")
	}))
	if err == nil {
		t.Fatal("Create succeeded after canceled native backup failure")
	}
	contents, readErr := os.ReadFile(archivePath)
	if readErr != nil || string(contents) != "partial" {
		t.Fatalf("preserved canceled archive = %q, error=%v", contents, readErr)
	}
}

func TestCreatePinnedArchiveCleanupRejectsConformingPathReplacement(t *testing.T) {
	t.Parallel()

	fixture := newRecoverySetFixture(t)
	injected := errors.New("injected pre-publication failure")
	var archivePath string
	var originalPath string
	_, err := createWithHooks(
		t.Context(),
		fixture.createOptions(func(
			_ context.Context,
			request NativeBackupRequest,
		) (NativeBackupIdentity, error) {
			archivePath = request.ArchivePath
			if writeErr := os.WriteFile(
				archivePath,
				[]byte("native-clickhouse-archive"),
				0o600,
			); writeErr != nil {
				return NativeBackupIdentity{}, writeErr
			}
			return validNativeBackupIdentity(), nil
		}),
		createHooks{
			publish: func(
				_ *privatefs.Directory,
				_ string,
				_ *privatefs.Directory,
				_ *privatefs.Directory,
				_ string,
			) (privatefs.RenameNoReplaceOutcome, error) {
				originalPath = archivePath + ".original"
				if renameErr := os.Rename(archivePath, originalPath); renameErr != nil {
					return privatefs.RenameNoReplaceUnchanged, renameErr
				}
				if writeErr := os.WriteFile(
					archivePath,
					[]byte("native-clickhouse-archive"),
					0o600,
				); writeErr != nil {
					return privatefs.RenameNoReplaceUnchanged, writeErr
				}
				return privatefs.RenameNoReplaceUnchanged, injected
			},
		},
	)
	if err == nil || !errors.Is(err, injected) {
		t.Fatalf("archive replacement failure = %v, want injected cause", err)
	}
	if !strings.Contains(err.Error(), "no longer owns its name") {
		t.Fatalf("archive replacement diagnostic = %v", err)
	}
	for _, path := range []string{archivePath, originalPath} {
		contents, readErr := os.ReadFile(path)
		if readErr != nil || string(contents) != "native-clickhouse-archive" {
			t.Fatalf("preserved archive %q = %q, error=%v", path, contents, readErr)
		}
	}
	if _, statErr := os.Lstat(fixture.destination); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("failed replacement attempt published destination: %v", statErr)
	}
}

func TestCreateOuterCleanupPreflightsUnexpectedEntryBeforeMutation(t *testing.T) {
	t.Parallel()

	fixture := newRecoverySetFixture(t)
	injected := errors.New("injected native backup failure")
	var stagePath string
	_, err := Create(t.Context(), fixture.createOptions(func(
		_ context.Context,
		_ NativeBackupRequest,
	) (NativeBackupIdentity, error) {
		entries, readErr := os.ReadDir(filepath.Dir(fixture.destination))
		if readErr != nil {
			return NativeBackupIdentity{}, readErr
		}
		if len(entries) != 1 {
			return NativeBackupIdentity{}, fmt.Errorf("outer staging entries = %v", entries)
		}
		stagePath = filepath.Join(filepath.Dir(fixture.destination), entries[0].Name())
		if writeErr := os.WriteFile(
			filepath.Join(stagePath, "unexpected"),
			[]byte("do-not-mutate"),
			0o600,
		); writeErr != nil {
			return NativeBackupIdentity{}, writeErr
		}
		return NativeBackupIdentity{}, injected
	}))
	if err == nil || !errors.Is(err, injected) {
		t.Fatalf("unexpected-entry failure = %v, want injected cause", err)
	}
	if !strings.Contains(err.Error(), "unexpected entry") {
		t.Fatalf("unexpected-entry cleanup diagnostic = %v", err)
	}
	for _, path := range []string{
		filepath.Join(stagePath, "unexpected"),
		filepath.Join(stagePath, controlPlaneDirectory, "manifest.json"),
	} {
		if _, statErr := os.Lstat(path); statErr != nil {
			t.Fatalf("preflight failure mutated %q: %v", path, statErr)
		}
	}
}

func TestCreateRejectsInvalidNativeBackupIdentityAndCleansArchive(t *testing.T) {
	t.Parallel()

	fixture := newRecoverySetFixture(t)
	identity := validNativeBackupIdentity()
	identity.ServerVersion = "latest"
	var archivePath string
	_, err := Create(t.Context(), fixture.createOptions(func(
		_ context.Context,
		request NativeBackupRequest,
	) (NativeBackupIdentity, error) {
		archivePath = request.ArchivePath
		if err := os.WriteFile(archivePath, []byte("archive"), 0o600); err != nil {
			return NativeBackupIdentity{}, err
		}
		return identity, nil
	}))
	if err == nil {
		t.Fatal("Create accepted an invalid native backup identity")
	}
	if _, statErr := os.Lstat(archivePath); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("archive exists after identity rejection: %v", statErr)
	}
}

func TestVerifyRejectsExternalArchiveTamperingAndUnsafeFiles(t *testing.T) {
	t.Parallel()

	tests := map[string]func(*testing.T, string){
		"contents": func(t *testing.T, archivePath string) {
			if err := os.WriteFile(archivePath, []byte("changed"), 0o600); err != nil {
				t.Fatal(err)
			}
		},
		"mode": func(t *testing.T, archivePath string) {
			if err := os.Chmod(archivePath, 0o644); err != nil {
				t.Fatal(err)
			}
		},
		"symlink": func(t *testing.T, archivePath string) {
			target := archivePath + ".target"
			if err := os.WriteFile(target, []byte("native-clickhouse-archive"), 0o600); err != nil {
				t.Fatal(err)
			}
			if err := os.Remove(archivePath); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink(target, archivePath); err != nil {
				t.Fatal(err)
			}
		},
		"hard link": func(t *testing.T, archivePath string) {
			target := archivePath + ".target"
			if err := os.Rename(archivePath, target); err != nil {
				t.Fatal(err)
			}
			if err := os.Link(target, archivePath); err != nil {
				t.Fatal(err)
			}
		},
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			fixture := newRecoverySetFixture(t)
			created, err := Create(t.Context(), fixture.createOptions(successfulNativeBackup))
			if err != nil {
				t.Fatal(err)
			}
			archivePath := filepath.Join(
				fixture.archiveRoot,
				created.Manifest.ClickHouse.Archive.Name,
			)
			mutate(t, archivePath)
			if _, err := Verify(t.Context(), fixture.verifyOptions()); err == nil {
				t.Fatal("Verify accepted a tampered or unsafe archive")
			}
		})
	}
}

func TestVerifyRejectsExtraOuterEntryAndMismatchedSource(t *testing.T) {
	t.Parallel()

	fixture := newRecoverySetFixture(t)
	if _, err := Create(t.Context(), fixture.createOptions(successfulNativeBackup)); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(fixture.destination, "extra"), []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Verify(t.Context(), fixture.verifyOptions()); err == nil {
		t.Fatal("Verify accepted an extra outer entry")
	}
	if err := os.Remove(filepath.Join(fixture.destination, "extra")); err != nil {
		t.Fatal(err)
	}
	options := fixture.verifyOptions()
	options.Release.SourceRevision = strings.Repeat("f", 40)
	if _, err := Verify(t.Context(), options); err == nil {
		t.Fatal("Verify accepted a different source")
	}
}

func successfulNativeBackup(
	_ context.Context,
	request NativeBackupRequest,
) (NativeBackupIdentity, error) {
	if err := os.WriteFile(request.ArchivePath, []byte("native-clickhouse-archive"), 0o600); err != nil {
		return NativeBackupIdentity{}, err
	}
	return validNativeBackupIdentity(), nil
}

func validNativeBackupIdentity() NativeBackupIdentity {
	return NativeBackupIdentity{
		ServerVersion:                   clickHouseServerVersion,
		MigrationLedgerSHA256:           strings.Repeat("5", 64),
		DatabaseUUID:                    "11111111-1111-4111-8111-111111111111",
		SchemaMigrationsTableUUID:       "22222222-2222-4222-8222-222222222222",
		EventsTableUUID:                 "33333333-3333-4333-8333-333333333333",
		RecoverySetsTableUUID:           "44444444-4444-4444-8444-444444444444",
		RecoveryArchiveMarkersTableUUID: "55555555-5555-4555-8555-555555555555",
		MaxVisibilitySeq:                42,
		BackupOperationUUID:             "66666666-6666-4666-8666-666666666666",
	}
}

type recoverySetFixture struct {
	databasePath      string
	masterKeyPath     string
	administratorPath string
	destination       string
	archiveRoot       string
	release           controlbackup.ReleaseIdentity
	archivePolicy     ArchiveOwnershipPolicy
}

func newRecoverySetFixture(t *testing.T) *recoverySetFixture {
	t.Helper()
	ctx := t.Context()
	sourceDirectory := privateTestDirectory(t)
	databasePath := filepath.Join(sourceDirectory, "open-splunk.db")
	database, err := control.Open(ctx, databasePath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := database.Close(); err != nil {
			t.Error(err)
		}
	})
	if _, err := database.CreateIndex(ctx, control.IndexDefinition{
		Name:             "recovery-set-index",
		RetentionPeriod:  24 * time.Hour,
		IngestionEnabled: true,
		SearchEnabled:    true,
	}); err != nil {
		t.Fatal(err)
	}
	masterKey := bytes.Repeat([]byte{0x5a}, auth.ServerMasterKeyBytes)
	fingerprint, err := auth.FingerprintServerMasterKey(masterKey)
	if err != nil {
		t.Fatal(err)
	}
	if err := auth.RegisterServerMasterKeyIdentity(ctx, database, fingerprint[:]); err != nil {
		t.Fatal(err)
	}
	masterKeyPath := filepath.Join(sourceDirectory, "master.key")
	if err := os.WriteFile(masterKeyPath, masterKey, 0o600); err != nil {
		t.Fatal(err)
	}
	administratorPath := filepath.Join(sourceDirectory, "administrator.token")
	if err := os.WriteFile(
		administratorPath,
		bytes.Repeat([]byte{'A'}, auth.MinimumBrowserBearerTokenBytes),
		0o600,
	); err != nil {
		t.Fatal(err)
	}
	migrationIdentity, err := database.VerifyCurrentMigrations(ctx, migrations.SQLite())
	if err != nil {
		t.Fatal(err)
	}
	destinationParent := privateTestDirectory(t)
	archiveRoot := privateTestDirectory(t)
	return &recoverySetFixture{
		databasePath:      databasePath,
		masterKeyPath:     masterKeyPath,
		administratorPath: administratorPath,
		destination:       filepath.Join(destinationParent, "deployment-recovery-set"),
		archiveRoot:       archiveRoot,
		release: controlbackup.ReleaseIdentity{
			SourceRevision: "development",
			SQLiteMigrations: controlbackup.MigrationIdentity{
				SHA256: strings.Repeat("1", sha256.Size*2), LatestVersion: migrationIdentity.LatestVersion,
			},
			ClickHouseMigrations: controlbackup.MigrationIdentity{
				SHA256: strings.Repeat("2", sha256.Size*2), LatestVersion: 1,
			},
		},
		archivePolicy: ArchiveOwnershipPolicy{
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
		},
	}
}

func (fixture *recoverySetFixture) createOptions(backup NativeBackup) CreateOptions {
	return CreateOptions{
		DatabasePath:           fixture.databasePath,
		MasterKeyPath:          fixture.masterKeyPath,
		AdministratorTokenPath: fixture.administratorPath,
		Destination:            fixture.destination,
		ArchiveRoot:            fixture.archiveRoot,
		ArchiveOwnership:       fixture.archivePolicy,
		Release:                fixture.release,
		NativeBackup:           backup,
	}
}

func (fixture *recoverySetFixture) verifyOptions() VerifyOptions {
	return VerifyOptions{
		Source:           fixture.destination,
		ArchiveRoot:      fixture.archiveRoot,
		ArchiveOwnership: fixture.archivePolicy,
		Release:          fixture.release,
	}
}

func privateTestDirectory(t *testing.T) string {
	t.Helper()
	path := t.TempDir()
	if err := os.Chmod(path, 0o700); err != nil {
		t.Fatal(err)
	}
	return path
}

func assertExactDirectoryEntries(t *testing.T, path string, want []string) {
	t.Helper()
	entries, err := os.ReadDir(path)
	if err != nil {
		t.Fatal(err)
	}
	got := make([]string, 0, len(entries))
	for _, entry := range entries {
		got = append(got, entry.Name())
	}
	slices.Sort(got)
	slices.Sort(want)
	if !slices.Equal(got, want) {
		t.Fatalf("directory entries = %v, want %v", got, want)
	}
}
