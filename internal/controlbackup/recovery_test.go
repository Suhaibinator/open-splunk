//go:build darwin || linux

package controlbackup

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"

	"github.com/Suhaibinator/open-splunk/internal/auth"
	"github.com/Suhaibinator/open-splunk/internal/buildassets"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/pgtest"
	"github.com/Suhaibinator/open-splunk/internal/privatefs"
	"github.com/Suhaibinator/open-splunk/internal/searchartifacts"

	"github.com/Suhaibinator/open-splunk/migrations"
	"github.com/google/uuid"
	pgx "github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"

	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"

	"testing"
	"time"
)

func TestRemoveCreatedBundleRejectsPinnedChildReplacementWithoutMutation(t *testing.T) {
	t.Parallel()

	fixture := newRecoveryFixture(t)
	if _, err := Create(t.Context(), fixture.createOptions()); err != nil {
		t.Fatal(err)
	}
	parent, err := privatefs.OpenDirectory(filepath.Dir(fixture.bundle))
	if err != nil {
		t.Fatal(err)
	}
	defer parent.Close()
	child, err := privatefs.OpenDirectory(fixture.bundle)
	if err != nil {
		t.Fatal(err)
	}
	defer child.Close()

	originalPath := fixture.bundle + ".original"
	if err := os.Rename(fixture.bundle, originalPath); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(fixture.bundle, 0o700); err != nil {
		t.Fatal(err)
	}
	replacementPath := filepath.Join(fixture.bundle, databaseFilename)
	if err := os.WriteFile(replacementPath, []byte("replacement"), 0o600); err != nil {
		t.Fatal(err)
	}

	if err := RemoveCreatedBundle(parent, filepath.Base(fixture.bundle), child); err == nil {
		t.Fatal("bundle cleanup accepted a replacement for its pinned child")
	}
	contents, err := os.ReadFile(replacementPath)
	if err != nil || string(contents) != "replacement" {
		t.Fatalf("replacement member = %q, error=%v", contents, err)
	}
	if _, err := os.Lstat(filepath.Join(originalPath, manifestFilename)); err != nil {
		t.Fatalf("attempt-owned child was mutated: %v", err)
	}
}

func TestRemoveCreatedBundleRejectsUnexpectedEntryBeforeAnyMutation(t *testing.T) {
	t.Parallel()

	fixture := newRecoveryFixture(t)
	if _, err := Create(t.Context(), fixture.createOptions()); err != nil {
		t.Fatal(err)
	}
	unexpectedPath := filepath.Join(fixture.bundle, "unexpected")
	if err := os.WriteFile(unexpectedPath, []byte("preserve"), 0o600); err != nil {
		t.Fatal(err)
	}
	parent, err := privatefs.OpenDirectory(filepath.Dir(fixture.bundle))
	if err != nil {
		t.Fatal(err)
	}
	defer parent.Close()
	child, err := privatefs.OpenDirectory(fixture.bundle)
	if err != nil {
		t.Fatal(err)
	}
	defer child.Close()

	if err := RemoveCreatedBundle(parent, filepath.Base(fixture.bundle), child); err == nil ||
		!strings.Contains(err.Error(), "unexpected child entry") {
		t.Fatalf("unexpected-entry cleanup error = %v", err)
	}
	for _, name := range append(append([]string(nil), bundleMemberNames...), "unexpected") {
		if _, err := os.Lstat(filepath.Join(fixture.bundle, name)); err != nil {
			t.Fatalf("preflight failure removed %q: %v", name, err)
		}
	}
}

func TestCreateCleanupPreservesReplacedStageName(t *testing.T) {
	t.Parallel()

	fixture := newRecoveryFixture(t)
	const stageName = ".test-backup-stage"
	stagePath := filepath.Join(filepath.Dir(fixture.bundle), stageName)
	originalPath := stagePath + ".original"
	_, err := createWithHooks(t.Context(), fixture.createOptions(), createHooks{
		now:       time.Now,
		random:    bytes.NewReader(bytes.Repeat([]byte{8}, recoverySetIDBytes)),
		stageName: fixedNameGenerator(stageName),
		afterStageSync: func() {
			if renameErr := os.Rename(stagePath, originalPath); renameErr != nil {
				t.Error(renameErr)
				return
			}
			if mkdirErr := os.Mkdir(stagePath, 0o700); mkdirErr != nil {
				t.Error(mkdirErr)
			}
		},
	})
	if err == nil {
		t.Fatal("Create succeeded after its pinned stage name was replaced")
	}
	if _, statErr := os.Lstat(stagePath); statErr != nil {
		t.Fatalf("replacement stage was deleted: %v", statErr)
	}
	for _, name := range bundleMemberNames {
		if _, statErr := os.Lstat(filepath.Join(originalPath, name)); statErr != nil {
			t.Fatalf("attempt-owned stage member %q was deleted: %v", name, statErr)
		}
	}
}

func TestCopyMemberCleanupPreservesConformingPathReplacement(t *testing.T) {
	t.Parallel()

	fixture := newRecoveryFixture(t)
	manifest, err := Create(t.Context(), fixture.createOptions())
	if err != nil {
		t.Fatal(err)
	}
	source, err := privatefs.OpenDirectory(fixture.bundle)
	if err != nil {
		t.Fatal(err)
	}
	defer source.Close()
	destinationPath := privateTestDirectory(t)
	destination, err := privatefs.OpenDirectory(destinationPath)
	if err != nil {
		t.Fatal(err)
	}
	defer destination.Close()

	const destinationName = ".restore-test-master-key"
	memberPath := filepath.Join(destinationPath, destinationName)
	originalPath := memberPath + ".original"
	var hookErr error
	err = copyMemberWithHooks(
		t.Context(),
		source,
		manifest.MasterKey.Name,
		destination,
		destinationName,
		manifest.MasterKey,
		copyMemberHooks{
			afterWriterClose: func() {
				hookErr = os.Rename(memberPath, originalPath)
				if hookErr != nil {
					return
				}
				hookErr = os.WriteFile(memberPath, fixture.masterKey, 0o600)
			},
		},
	)
	if hookErr != nil {
		t.Fatal(hookErr)
	}
	if err == nil {
		t.Fatal("copy accepted a content-conforming replacement for its staged path")
	}
	for _, candidate := range []string{memberPath, originalPath} {
		got, readErr := os.ReadFile(candidate)
		if readErr != nil || !bytes.Equal(got, fixture.masterKey) {
			t.Fatalf("preserved candidate %q = %x, error=%v", candidate, got, readErr)
		}
	}
}

func TestCopyMemberCleanupOpenRejectsConformingPathReplacement(t *testing.T) {
	t.Parallel()

	fixture := newRecoveryFixture(t)
	manifest, err := Create(t.Context(), fixture.createOptions())
	if err != nil {
		t.Fatal(err)
	}
	source, err := privatefs.OpenDirectory(fixture.bundle)
	if err != nil {
		t.Fatal(err)
	}
	defer source.Close()
	destinationPath := privateTestDirectory(t)
	destination, err := privatefs.OpenDirectory(destinationPath)
	if err != nil {
		t.Fatal(err)
	}
	defer destination.Close()

	const destinationName = ".restore-test-master-key"
	memberPath := filepath.Join(destinationPath, destinationName)
	originalPath := memberPath + ".original"
	var hookErr error
	err = copyMemberWithHooks(
		t.Context(),
		source,
		manifest.MasterKey.Name,
		destination,
		destinationName,
		manifest.MasterKey,
		copyMemberHooks{
			beforeCleanupOpen: func() {
				hookErr = os.Rename(memberPath, originalPath)
				if hookErr != nil {
					return
				}
				hookErr = os.WriteFile(memberPath, fixture.masterKey, 0o600)
			},
		},
	)
	if hookErr != nil {
		t.Fatal(hookErr)
	}
	if err == nil {
		t.Fatal("copy trusted a conforming replacement as its cleanup descriptor")
	}
	for _, candidate := range []string{memberPath, originalPath} {
		got, readErr := os.ReadFile(candidate)
		if readErr != nil || !bytes.Equal(got, fixture.masterKey) {
			t.Fatalf("preserved candidate %q = %x, error=%v", candidate, got, readErr)
		}
	}
}

func TestValidateReleaseIdentityProvidesReusableAndScopedErrors(t *testing.T) {
	t.Parallel()

	valid := ReleaseIdentity{
		SourceRevision: "development",
		PostgresMigrations: MigrationIdentity{
			SHA256: strings.Repeat("1", sha256.Size*2), LatestVersion: 1,
		},
		ClickHouseMigrations: MigrationIdentity{
			SHA256: strings.Repeat("2", sha256.Size*2), LatestVersion: 1,
		},
	}
	if err := ValidateReleaseIdentity(valid); err != nil {
		t.Fatalf("valid release identity: %v", err)
	}

	for _, test := range []struct {
		name       string
		mutate     func(*ReleaseIdentity)
		genericErr string
		scopedErr  string
	}{
		{
			name: "Postgres migrations",
			mutate: func(identity *ReleaseIdentity) {
				identity.PostgresMigrations.LatestVersion = 0
			},
			genericErr: "Postgres migration identity is invalid",
			scopedErr:  "control-plane backup Postgres migration identity is invalid",
		},
		{
			name: "ClickHouse migrations",
			mutate: func(identity *ReleaseIdentity) {
				identity.ClickHouseMigrations.SHA256 = ""
			},
			genericErr: "ClickHouse migration identity is invalid",
			scopedErr:  "control-plane backup ClickHouse migration identity is invalid",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			identity := valid
			test.mutate(&identity)
			if err := ValidateReleaseIdentity(identity); err == nil || err.Error() != test.genericErr {
				t.Fatalf("generic error = %v, want %q", err, test.genericErr)
			}
			if err := validateReleaseIdentity(identity); err == nil || err.Error() != test.scopedErr {
				t.Fatalf("scoped error = %v, want %q", err, test.scopedErr)
			}
		})
	}
}

func TestCreateRequiresAbsentBundleAndLeavesExistingDestinationUntouched(t *testing.T) {
	t.Parallel()

	fixture := newRecoveryFixture(t)
	if err := os.Mkdir(fixture.bundle, 0o700); err != nil {
		t.Fatal(err)
	}
	marker := filepath.Join(fixture.bundle, "marker")
	if err := os.WriteFile(marker, []byte("unchanged"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Create(t.Context(), fixture.createOptions()); err == nil {
		t.Fatal("existing bundle destination succeeded")
	}
	contents, err := os.ReadFile(marker)
	if err != nil {
		t.Fatal(err)
	}
	if string(contents) != "unchanged" {
		t.Fatalf("existing bundle marker = %q", contents)
	}
}

func TestCreateCancellationAndPublicationRaceLeaveNoOwnedPartialBundle(t *testing.T) {
	t.Parallel()

	t.Run("canceled after staged fsync", func(t *testing.T) {
		t.Parallel()
		fixture := newRecoveryFixture(t)
		ctx, cancel := context.WithCancel(t.Context())
		_, err := createWithHooks(ctx, fixture.createOptions(), createHooks{
			now:       time.Now,
			random:    bytes.NewReader(bytes.Repeat([]byte{1}, recoverySetIDBytes)),
			stageName: fixedNameGenerator(".test-backup-stage"),
			afterStageSync: func() {
				cancel()
			},
		})
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("canceled create error = %v, want context.Canceled", err)
		}
		if _, err := os.Lstat(fixture.bundle); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("canceled create published destination: %v", err)
		}
		assertExactRecoveryDirectory(t, filepath.Dir(fixture.bundle), nil)
	})

	t.Run("competitor wins publication", func(t *testing.T) {
		t.Parallel()
		fixture := newRecoveryFixture(t)
		marker := []byte("competitor")
		_, err := createWithHooks(t.Context(), fixture.createOptions(), createHooks{
			now:       time.Now,
			random:    bytes.NewReader(bytes.Repeat([]byte{2}, recoverySetIDBytes)),
			stageName: fixedNameGenerator(".test-backup-stage"),
			beforePublish: func() {
				if mkdirErr := os.Mkdir(fixture.bundle, 0o700); mkdirErr != nil {
					t.Error(mkdirErr)
					return
				}
				if writeErr := os.WriteFile(filepath.Join(fixture.bundle, "marker"), marker, 0o600); writeErr != nil {
					t.Error(writeErr)
				}
			},
		})
		if err == nil {
			t.Fatal("create won over a competing destination")
		}
		contents, readErr := os.ReadFile(filepath.Join(fixture.bundle, "marker"))
		if readErr != nil {
			t.Fatal(readErr)
		}
		if !bytes.Equal(contents, marker) {
			t.Fatalf("competing destination marker = %q", contents)
		}
		entries, readErr := os.ReadDir(filepath.Dir(fixture.bundle))
		if readErr != nil {
			t.Fatal(readErr)
		}
		if len(entries) != 1 || entries[0].Name() != filepath.Base(fixture.bundle) {
			t.Fatalf("publication-race parent entries = %v", entries)
		}
	})
}

func TestCreateKeepsRenamedStageDescriptorLiveUntilDestinationIsPinned(t *testing.T) {
	t.Parallel()

	fixture := newRecoveryFixture(t)
	descriptorLive := false
	_, err := createWithHooks(t.Context(), fixture.createOptions(), createHooks{
		now:       time.Now,
		random:    bytes.NewReader(bytes.Repeat([]byte{9}, recoverySetIDBytes)),
		stageName: fixedNameGenerator(".test-backup-stage"),
		afterRename: func(stage *privatefs.Directory) error {
			if _, statErr := stage.PinnedInfo(); statErr != nil {
				return statErr
			}
			descriptorLive = true
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !descriptorLive {
		t.Fatal("renamed staging descriptor was not retained through destination pinning")
	}
	if _, err := Verify(t.Context(), fixture.bundle, fixture.release); err != nil {
		t.Fatalf("verify live-descriptor publication: %v", err)
	}
}

func TestCreatePreservesCompletedOrAmbiguousPublicationFailures(t *testing.T) {
	t.Parallel()

	t.Run("completed rename then error", func(t *testing.T) {
		t.Parallel()
		fixture := newRecoveryFixture(t)
		injected := errors.New("injected post-rename error")
		_, err := createWithHooks(t.Context(), fixture.createOptions(), createHooks{
			now:       time.Now,
			random:    bytes.NewReader(bytes.Repeat([]byte{3}, recoverySetIDBytes)),
			stageName: fixedNameGenerator(".test-backup-stage"),
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
		})
		if err == nil {
			t.Fatal("Create succeeded after completed rename error")
		}
		var publicationErr *PublicationStatusError
		if !errors.As(err, &publicationErr) || !errors.Is(err, injected) {
			t.Fatalf("completed publication error = %v, want typed injected cause", err)
		}
		if publicationErr.Outcome != privatefs.RenameNoReplaceCompleted {
			t.Fatalf("completed publication outcome = %v", publicationErr.Outcome)
		}
		if _, verifyErr := Verify(t.Context(), fixture.bundle, fixture.release); verifyErr != nil {
			t.Fatalf("verify preserved published bundle: %v", verifyErr)
		}
		entries, readErr := os.ReadDir(filepath.Dir(fixture.bundle))
		if readErr != nil {
			t.Fatal(readErr)
		}
		if len(entries) != 1 || entries[0].Name() != filepath.Base(fixture.bundle) {
			t.Fatalf("preserved publication entries = %v", entries)
		}
	})

	t.Run("ambiguous rename", func(t *testing.T) {
		t.Parallel()
		fixture := newRecoveryFixture(t)
		injected := errors.New("injected ambiguous rename error")
		const stageName = ".test-backup-stage"
		_, err := createWithHooks(t.Context(), fixture.createOptions(), createHooks{
			now:       time.Now,
			random:    bytes.NewReader(bytes.Repeat([]byte{4}, recoverySetIDBytes)),
			stageName: fixedNameGenerator(stageName),
			publish: func(
				_ *privatefs.Directory,
				_ string,
				_ *privatefs.Directory,
				_ *privatefs.Directory,
				_ string,
			) (privatefs.RenameNoReplaceOutcome, error) {
				return privatefs.RenameNoReplaceAmbiguous, injected
			},
		})
		if err == nil {
			t.Fatal("Create succeeded after ambiguous rename")
		}
		var publicationErr *PublicationStatusError
		if !errors.As(err, &publicationErr) || !errors.Is(err, injected) {
			t.Fatalf("ambiguous publication error = %v, want typed injected cause", err)
		}
		if publicationErr.Outcome != privatefs.RenameNoReplaceAmbiguous {
			t.Fatalf("ambiguous publication outcome = %v", publicationErr.Outcome)
		}
		preservedStage := filepath.Join(filepath.Dir(fixture.bundle), stageName)
		if _, verifyErr := Verify(t.Context(), preservedStage, fixture.release); verifyErr != nil {
			t.Fatalf("verify preserved ambiguous stage: %v", verifyErr)
		}
		entries, readErr := os.ReadDir(filepath.Dir(fixture.bundle))
		if readErr != nil {
			t.Fatal(readErr)
		}
		if len(entries) != 1 || entries[0].Name() != stageName {
			t.Fatalf("preserved ambiguous entries = %v", entries)
		}
	})
}

func TestVerifyRejectsUnsafeMemberObjectsAndManifestConsistentWrongKey(t *testing.T) {
	t.Parallel()

	for name, mutate := range map[string]func(*testing.T, *recoveryFixture){
		"unsafe mode": func(t *testing.T, fixture *recoveryFixture) {
			if err := os.Chmod(filepath.Join(fixture.bundle, masterKeyFilename), 0o644); err != nil {
				t.Fatal(err)
			}
		},
		"symlink": func(t *testing.T, fixture *recoveryFixture) {
			path := filepath.Join(fixture.bundle, masterKeyFilename)
			if err := os.Remove(path); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink(fixture.masterKeyPath, path); err != nil {
				t.Fatal(err)
			}
		},
		"hard link": func(t *testing.T, fixture *recoveryFixture) {
			path := filepath.Join(fixture.bundle, masterKeyFilename)
			if err := os.Link(path, filepath.Join(privateTestDirectory(t), "other-link")); err != nil {
				t.Fatal(err)
			}
		},
		"consistent wrong key": func(t *testing.T, fixture *recoveryFixture) {
			key := bytes.Repeat([]byte{0x7f}, auth.ServerMasterKeyBytes)
			keyPath := filepath.Join(fixture.bundle, masterKeyFilename)
			if err := os.WriteFile(keyPath, key, 0o600); err != nil {
				t.Fatal(err)
			}
			manifestPath := filepath.Join(fixture.bundle, manifestFilename)
			encoded, err := os.ReadFile(manifestPath)
			if err != nil {
				t.Fatal(err)
			}
			manifest, err := unmarshalManifest(encoded)
			if err != nil {
				t.Fatal(err)
			}
			digest := sha256.Sum256(key)
			manifest.MasterKey.SHA256 = hex.EncodeToString(digest[:])
			fingerprint, err := auth.FingerprintServerMasterKey(key)
			if err != nil {
				t.Fatal(err)
			}
			manifest.MasterKeyFingerprintSHA256 = hex.EncodeToString(fingerprint[:])
			encoded, err = marshalManifest(manifest)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(manifestPath, encoded, 0o600); err != nil {
				t.Fatal(err)
			}
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			fixture := newRecoveryFixture(t)
			if _, err := Create(t.Context(), fixture.createOptions()); err != nil {
				t.Fatal(err)
			}
			mutate(t, fixture)
			if _, err := Verify(t.Context(), fixture.bundle, fixture.release); err == nil {
				t.Fatal("unsafe or mismatched bundle verified")
			}
		})
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

func assertExactRecoveryDirectory(t *testing.T, path string, want []string) {
	t.Helper()
	info, err := os.Lstat(path)
	if err != nil {
		t.Fatal(err)
	}
	if !info.IsDir() || info.Mode().Perm() != 0o700 {
		t.Fatalf("directory state = (%v, %#o), want directory 0700", info.Mode(), info.Mode().Perm())
	}
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
	for _, name := range got {
		if info, err := os.Stat(filepath.Join(path, name)); err != nil {
			t.Fatal(err)
		} else if !info.IsDir() {
			assertPrivateRegular(t, filepath.Join(path, name))
		}
	}
}

func assertPrivateRegular(t *testing.T, path string) {
	t.Helper()
	info, err := os.Lstat(path)
	if err != nil {
		t.Fatal(err)
	}
	if !info.Mode().IsRegular() || info.Mode().Perm() != 0o600 {
		t.Fatalf("%s state = (%v, %#o), want regular 0600", filepath.Base(path), info.Mode(), info.Mode().Perm())
	}
}

func testDirectorySnapshot(t *testing.T, path string) []string {
	t.Helper()
	entries, err := os.ReadDir(path)
	if err != nil {
		t.Fatal(err)
	}
	result := make([]string, 0, len(entries))
	for _, entry := range entries {
		info, err := entry.Info()
		if err != nil {
			t.Fatal(err)
		}
		contents := []byte(nil)
		if info.Mode().IsRegular() {
			contents, err = os.ReadFile(filepath.Join(path, entry.Name()))
			if err != nil {
				t.Fatal(err)
			}
		}
		digest := sha256.Sum256(contents)
		result = append(result, entry.Name()+":"+info.Mode().String()+":"+string(digest[:]))
	}
	slices.Sort(result)
	return result
}

type recoveryFixture struct {
	blobs                                                              searchartifacts.BlobStore
	sourceDatabase, masterKeyPath, bundle, restoreDirectory, targetDSN string
	release                                                            ReleaseIdentity
	masterKey                                                          []byte
	database                                                           *control.DB
	test                                                               *testing.T
}

func newRecoveryFixture(t *testing.T) *recoveryFixture {
	t.Helper()
	dsn := pgtest.DSN(t)
	database, err := control.Open(t.Context(), dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	if _, err := database.CreateIndex(t.Context(), control.IndexDefinition{Name: "recovery-index", RetentionPeriod: 24 * time.Hour, IngestionEnabled: true, SearchEnabled: true}); err != nil {
		t.Fatal(err)
	}
	key := bytes.Repeat([]byte{0x5a}, auth.ServerMasterKeyBytes)
	fingerprint, err := auth.FingerprintServerMasterKey(key)
	if err != nil {
		t.Fatal(err)
	}
	if err := auth.RegisterServerMasterKeyIdentity(t.Context(), database, fingerprint[:]); err != nil {
		t.Fatal(err)
	}
	identity, err := buildassets.DigestMigrations(migrations.PostgreSQL(), ".")
	if err != nil {
		t.Fatal(err)
	}
	keyPath := filepath.Join(privateTestDirectory(t), "master.key")
	if err := os.WriteFile(keyPath, key, 0600); err != nil {
		t.Fatal(err)
	}
	return &recoveryFixture{sourceDatabase: dsn, masterKeyPath: keyPath, bundle: filepath.Join(privateTestDirectory(t), "control-plane-bundle"), restoreDirectory: privateTestDirectory(t), masterKey: key, database: database, test: t, release: ReleaseIdentity{SourceRevision: "development", PostgresMigrations: MigrationIdentity{SHA256: identity.SHA256, LatestVersion: identity.LatestVersion}, ClickHouseMigrations: MigrationIdentity{SHA256: strings.Repeat("2", 64), LatestVersion: 1}}}
}
func (f *recoveryFixture) createOptions() CreateOptions {
	if err := f.database.Close(); err != nil {
		f.test.Fatal(err)
	}
	return CreateOptions{PostgresDSN: f.sourceDatabase, MasterKeyPath: f.masterKeyPath, Destination: f.bundle, Release: f.release, Blobs: f.blobs}
}
func (f *recoveryFixture) restoreOptions() RestoreOptions {
	if f.targetDSN == "" {
		f.targetDSN = emptyRestoreDatabase(f.test, f.sourceDatabase)
	}
	return RestoreOptions{Source: f.bundle, PostgresDSN: f.targetDSN, MasterKeyPath: filepath.Join(f.restoreDirectory, "restored.key"), Release: f.release, Blobs: f.blobs}
}
func emptyRestoreDatabase(t *testing.T, source string) string {
	t.Helper()
	cfg, err := pgx.ParseConfig(source)
	if err != nil {
		t.Fatal(err)
	}
	schema := strings.Trim(cfg.RuntimeParams["search_path"], `"`)
	delete(cfg.RuntimeParams, "search_path")
	admin := stdlib.OpenDB(*cfg)
	databaseName := "os_test_restore_" + strings.ReplaceAll(uuid.NewString(), "-", "_")
	if _, err := admin.ExecContext(t.Context(), "CREATE DATABASE "+pgx.Identifier{databaseName}.Sanitize()); err != nil {
		t.Fatal(errors.Join(err, admin.Close()))
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if _, err := admin.ExecContext(ctx, "DROP DATABASE "+pgx.Identifier{databaseName}.Sanitize()+" WITH (FORCE)"); err != nil {
			t.Error(err)
		}
		_ = admin.Close()
	})
	cfg.Database = databaseName
	pool := stdlib.OpenDB(*cfg)
	if _, err := pool.ExecContext(t.Context(), "CREATE SCHEMA "+pgx.Identifier{schema}.Sanitize()); err != nil {
		t.Fatal(errors.Join(err, pool.Close()))
	}
	_ = pool.Close()
	cfg.RuntimeParams["search_path"] = pgx.Identifier{schema}.Sanitize()
	parsed, err := url.Parse(source)
	if err != nil {
		t.Fatal(err)
	}
	parsed.Path = "/" + databaseName
	query := parsed.Query()
	query.Set("search_path", schema)
	parsed.RawQuery = query.Encode()
	return parsed.String()
}
func TestCreateVerifyRestoreControlPlaneRoundTrip(t *testing.T) {
	fixture := newRecoveryFixture(t)
	manifest, err := Create(t.Context(), fixture.createOptions())
	if err != nil {
		t.Fatal(err)
	}
	if manifest.PostgresMigrationLedgerSHA256 == manifest.PostgresMigrations.SHA256 {
		t.Fatal("source corpus identity must remain separate from ledger identity")
	}
	if manifest.FormatVersion != 3 || manifest.ClickHouseIncluded || manifest.Scope != controlPlaneOnlyScope || manifest.Objects == nil {
		t.Fatalf("unexpected native manifest: %#v", manifest)
	}
	assertExactRecoveryDirectory(t, fixture.bundle, append([]string(nil), bundleMemberNames...))
	verified, err := Verify(t.Context(), fixture.bundle, fixture.release)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(verified, manifest) {
		t.Fatal("verified manifest changed")
	}
	options := fixture.restoreOptions()
	if err := PreflightRestore(t.Context(), options); err != nil {
		t.Fatal(err)
	}
	if err := Restore(t.Context(), options); err != nil {
		t.Fatal(err)
	}
	assertExactRecoveryDirectory(t, fixture.restoreDirectory, []string{"restored.key"})
	db, err := control.Open(t.Context(), options.PostgresDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	index, err := db.GetIndexByName(t.Context(), "recovery-index")
	if err != nil || index.Definition.Name != "recovery-index" {
		t.Fatalf("restored index: %#v %v", index, err)
	}
	stored, registered, err := auth.ReadServerMasterKeyIdentity(t.Context(), db)
	want, _ := auth.FingerprintServerMasterKey(fixture.masterKey)
	if err != nil || !registered || !bytes.Equal(stored, want[:]) {
		t.Fatalf("restored key identity: %x %v", stored, err)
	}
	var phase string
	if err := db.SQLDB().QueryRowContext(t.Context(), `SELECT phase FROM control_recovery_publication WHERE singleton=1`).Scan(&phase); err != nil || phase != "complete" {
		t.Fatalf("receipt: %q %v", phase, err)
	}
}
func TestRestoreInterruptionAfterEveryPublicationBoundaryIsResumable(t *testing.T) {
	for _, boundary := range []string{"database", "objects", "master key"} {
		t.Run(boundary, func(t *testing.T) {
			fixture := newRecoveryFixture(t)
			if _, err := Create(t.Context(), fixture.createOptions()); err != nil {
				t.Fatal(err)
			}
			options := fixture.restoreOptions()
			injected := errors.New("stop at durable boundary")
			hooks := restoreHooks{}
			switch boundary {
			case "database":
				hooks.afterDatabasePublish = func() error { return injected }
			case "objects":
				hooks.afterObjectsPublish = func() error { return injected }
			case "master key":
				hooks.afterMasterKeyPublish = func() error { return injected }
			}
			if err := restoreWithHooks(t.Context(), options, hooks); !errors.Is(err, injected) {
				t.Fatalf("injected restore: %v", err)
			}
			before := testDirectorySnapshot(t, fixture.restoreDirectory)
			if err := PreflightRestore(t.Context(), options); err != nil {
				t.Fatal(err)
			}
			if !slices.Equal(before, testDirectorySnapshot(t, fixture.restoreDirectory)) {
				t.Fatal("preflight mutated key directory")
			}
			if err := Restore(t.Context(), options); err != nil {
				t.Fatal(err)
			}
			if err := Restore(t.Context(), options); err != nil {
				t.Fatalf("exact repeated recovery: %v", err)
			}
		})
	}
}
func TestMigrationSourceCorpusDriftIsRejectedBeforePublication(t *testing.T) {
	fixture := newRecoveryFixture(t)
	bad := fixture.createOptions()
	bad.Release.PostgresMigrations.SHA256 = strings.Repeat("0", 64)
	if _, err := Create(t.Context(), bad); err == nil || !strings.Contains(err.Error(), "source corpus") {
		t.Fatalf("drifted migration corpus create: %v", err)
	}
	if _, err := os.Lstat(fixture.bundle); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("drifted source corpus published backup: %v", err)
	}
	if _, err := Create(t.Context(), fixture.createOptions()); err != nil {
		t.Fatal(err)
	}
	if _, err := Verify(t.Context(), fixture.bundle, bad.Release); err == nil {
		t.Fatal("offline verify accepted different migration source corpus")
	}
}

func TestRestoreRejectsPopulatedTargetAndMismatchedGeneration(t *testing.T) {
	fixture := newRecoveryFixture(t)
	if _, err := Create(t.Context(), fixture.createOptions()); err != nil {
		t.Fatal(err)
	}
	options := fixture.restoreOptions()
	pool, err := control.OpenReadOnly(t.Context(), options.PostgresDSN)
	if err != nil {
		t.Fatal(err)
	}
	if err := pool.Close(); err != nil {
		t.Fatal(err)
	}
	raw, err := pgx.Connect(t.Context(), options.PostgresDSN)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := raw.Exec(t.Context(), `CREATE TABLE unrelated(payload text);INSERT INTO unrelated VALUES('preserve')`); err != nil {
		t.Fatal(err)
	}
	if err := Restore(t.Context(), options); err == nil {
		t.Fatal("restore adopted unrelated populated schema")
	}
	var payload string
	if err := raw.QueryRow(t.Context(), `SELECT payload FROM unrelated`).Scan(&payload); err != nil || payload != "preserve" {
		t.Fatalf("unrelated target changed: %q %v", payload, err)
	}
	if err := raw.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
}
func TestRestoreEnforcesOptionalDeploymentBinding(t *testing.T) {
	fixture := newRecoveryFixture(t)
	manifest, err := Create(t.Context(), fixture.createOptions())
	if err != nil {
		t.Fatal(err)
	}
	options := fixture.restoreOptions()
	digest, err := manifestDigest(manifest)
	if err != nil {
		t.Fatal(err)
	}
	options.ExpectedRecoverySetID = manifest.RecoverySetID
	options.ExpectedManifestSHA256 = strings.Repeat("f", 64)
	if err := PreflightRestore(t.Context(), options); !errors.Is(err, ErrDeploymentBindingMismatch) {
		t.Fatalf("mismatched binding: %v", err)
	}
	options.ExpectedManifestSHA256 = digest
	if err := PreflightRestore(t.Context(), options); err != nil {
		t.Fatal(err)
	}
	options.ExpectedManifestSHA256 = ""
	if err := PreflightRestore(t.Context(), options); err == nil {
		t.Fatal("partial binding accepted")
	}
}
func TestRestoreRequiresExactLiveMaintenanceLease(t *testing.T) {
	fixture := newRecoveryFixture(t)
	if _, err := Create(t.Context(), fixture.createOptions()); err != nil {
		t.Fatal(err)
	}
	options := fixture.restoreOptions()
	lease, err := control.AcquireMaintenanceLease(t.Context(), options.PostgresDSN)
	if err != nil {
		t.Fatal(err)
	}
	options.Lease = lease
	if err := PreflightRestore(t.Context(), options); err != nil {
		t.Fatal(err)
	}
	sourceLease, err := control.AcquireMaintenanceLease(t.Context(), fixture.sourceDatabase)
	if err != nil {
		t.Fatal(err)
	}
	options.Lease = sourceLease
	if err := Restore(t.Context(), options); err == nil {
		t.Fatal("foreign database lease accepted")
	}
	if err := sourceLease.Close(); err != nil {
		t.Fatal(err)
	}
	if err := lease.Close(); err != nil {
		t.Fatal(err)
	}
	options.Lease = lease
	if err := Restore(t.Context(), options); err == nil {
		t.Fatal("closed lease accepted")
	}
	assertExactRecoveryDirectory(t, fixture.restoreDirectory, nil)
}
func TestRestoreRejectsInvalidTargetsBeforeOpeningBundle(t *testing.T) {
	for _, path := range []string{"", "relative.key", "/tmp/../key", "/"} {
		if err := Restore(t.Context(), RestoreOptions{Source: "/does/not/exist", MasterKeyPath: path}); err == nil || strings.Contains(err.Error(), "no such file") {
			t.Fatalf("invalid key path %q: %v", path, err)
		}
	}
}
func TestRestoreCancellationBeforePublicationLeavesNoPartialTarget(t *testing.T) {
	fixture := newRecoveryFixture(t)
	if _, err := Create(t.Context(), fixture.createOptions()); err != nil {
		t.Fatal(err)
	}
	options := fixture.restoreOptions()
	ctx, cancel := context.WithCancel(t.Context())
	err := restoreWithHooks(ctx, options, restoreHooks{beforePublication: func(index int) {
		if index == 0 {
			cancel()
		}
	}})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled restore: %v", err)
	}
	assertExactRecoveryDirectory(t, fixture.restoreDirectory, nil)
	db, err := control.OpenReadOnly(t.Context(), options.PostgresDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var count int64
	if err := db.SQLDB().QueryRowContext(t.Context(), `SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname=current_schema() AND c.relkind='r'`).Scan(&count); err != nil || count != 0 {
		t.Fatalf("canceled target has %d tables: %v", count, err)
	}
}
