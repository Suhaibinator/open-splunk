//go:build darwin || linux

package controlbackup

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/auth"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/migrations"
)

func TestCreateVerifyRestoreControlPlaneRoundTrip(t *testing.T) {
	t.Parallel()

	fixture := newRecoveryFixture(t)
	manifest, err := Create(t.Context(), fixture.createOptions())
	if err != nil {
		t.Fatal(err)
	}
	if manifest.Scope != controlPlaneOnlyScope || manifest.ClickHouseIncluded {
		t.Fatalf("manifest scope = %#v", manifest)
	}
	if _, err := os.Lstat(fixture.sourceDatabase + "-wal"); err != nil {
		t.Fatalf("source WAL fixture is missing: %v", err)
	}
	for _, sidecar := range []string{
		filepath.Join(fixture.bundle, databaseFilename+"-wal"),
		filepath.Join(fixture.bundle, databaseFilename+"-shm"),
		filepath.Join(fixture.bundle, databaseFilename+"-journal"),
	} {
		if _, err := os.Lstat(sidecar); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("bundle sidecar %q exists: %v", filepath.Base(sidecar), err)
		}
	}
	assertExactRecoveryDirectory(t, fixture.bundle, []string{
		administratorTokenFilename,
		databaseFilename,
		manifestFilename,
		masterKeyFilename,
	})

	verified, err := Verify(t.Context(), fixture.bundle, fixture.release)
	if err != nil {
		t.Fatal(err)
	}
	if verified != manifest {
		t.Fatalf("verified manifest = %#v, want %#v", verified, manifest)
	}

	restore := fixture.restoreOptions()
	if err := Restore(t.Context(), restore); err != nil {
		t.Fatal(err)
	}
	assertExactRecoveryDirectory(t, fixture.restoreDirectory, []string{
		filepath.Base(restore.AdministratorTokenPath),
		filepath.Base(restore.DatabasePath),
		filepath.Base(restore.MasterKeyPath),
	})
	assertPrivateRegular(t, restore.DatabasePath)
	assertPrivateRegular(t, restore.MasterKeyPath)
	assertPrivateRegular(t, restore.AdministratorTokenPath)

	restored, err := control.Open(t.Context(), restore.DatabasePath)
	if err != nil {
		t.Fatal(err)
	}
	defer restored.Close()
	index, err := restored.GetIndexByName(t.Context(), "recovery-index")
	if err != nil {
		t.Fatal(err)
	}
	if index.Definition.Name != "recovery-index" || !index.Definition.SearchEnabled {
		t.Fatalf("restored index = %#v", index)
	}
	key, err := os.ReadFile(restore.MasterKeyPath)
	if err != nil {
		t.Fatal(err)
	}
	fingerprint, err := auth.FingerprintServerMasterKey(key)
	clear(key)
	if err != nil {
		t.Fatal(err)
	}
	storedFingerprint, registered, err := auth.ReadServerMasterKeyIdentity(t.Context(), restored)
	if err != nil {
		t.Fatal(err)
	}
	if !registered || !bytes.Equal(storedFingerprint, fingerprint[:]) {
		t.Fatalf("restored key identity = (%x, %t)", storedFingerprint, registered)
	}
	token, err := os.ReadFile(restore.AdministratorTokenPath)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(token, fixture.administratorToken) {
		t.Fatal("restored administrator token differs")
	}
}

func TestVerifyRejectsTamperingExtraMembersAndDifferentReleaseWithoutSecrets(t *testing.T) {
	t.Parallel()

	for name, mutate := range map[string]func(*testing.T, *recoveryFixture){
		"database": func(t *testing.T, fixture *recoveryFixture) {
			path := filepath.Join(fixture.bundle, databaseFilename)
			file, err := os.OpenFile(path, os.O_WRONLY, 0)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := file.WriteAt([]byte("corrupt"), 0); err != nil {
				t.Fatal(err)
			}
			if err := file.Close(); err != nil {
				t.Fatal(err)
			}
		},
		"master key": func(t *testing.T, fixture *recoveryFixture) {
			path := filepath.Join(fixture.bundle, masterKeyFilename)
			if err := os.WriteFile(path, bytes.Repeat([]byte{0x7f}, auth.ServerMasterKeyBytes), 0o600); err != nil {
				t.Fatal(err)
			}
		},
		"administrator token": func(t *testing.T, fixture *recoveryFixture) {
			path := filepath.Join(fixture.bundle, administratorTokenFilename)
			if err := os.WriteFile(path, bytes.Repeat([]byte{'Z'}, len(fixture.administratorToken)), 0o600); err != nil {
				t.Fatal(err)
			}
		},
		"manifest": func(t *testing.T, fixture *recoveryFixture) {
			path := filepath.Join(fixture.bundle, manifestFilename)
			contents, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(path, append([]byte(" "), contents...), 0o600); err != nil {
				t.Fatal(err)
			}
		},
		"migration ledger identity": func(t *testing.T, fixture *recoveryFixture) {
			manifestPath := filepath.Join(fixture.bundle, manifestFilename)
			encoded, err := os.ReadFile(manifestPath)
			if err != nil {
				t.Fatal(err)
			}
			manifest, err := unmarshalManifest(encoded)
			if err != nil {
				t.Fatal(err)
			}
			manifest.SQLiteMigrationLedgerSHA256 = strings.Repeat("f", sha256.Size*2)
			encoded, err = marshalManifest(manifest)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(manifestPath, encoded, 0o600); err != nil {
				t.Fatal(err)
			}
		},
		"extra member": func(t *testing.T, fixture *recoveryFixture) {
			if err := os.WriteFile(filepath.Join(fixture.bundle, "extra"), []byte("x"), 0o600); err != nil {
				t.Fatal(err)
			}
		},
		"different release": func(_ *testing.T, fixture *recoveryFixture) {
			fixture.release.SourceRevision = strings.Repeat("f", 40)
		},
	} {
		name, mutate := name, mutate
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			fixture := newRecoveryFixture(t)
			if _, err := Create(t.Context(), fixture.createOptions()); err != nil {
				t.Fatal(err)
			}
			mutate(t, fixture)
			_, err := Verify(t.Context(), fixture.bundle, fixture.release)
			if err == nil {
				t.Fatal("tampered bundle verified")
			}
			if strings.Contains(err.Error(), string(fixture.masterKey)) ||
				strings.Contains(err.Error(), string(fixture.administratorToken)) {
				t.Fatalf("verification error exposed credential material: %v", err)
			}
		})
	}
}

func TestRestoreResumesOnlyExactFailClosedPublicationPrefixes(t *testing.T) {
	t.Parallel()

	for name, prefix := range map[string][]string{
		"empty":               nil,
		"database":            {databaseFilename},
		"database and key":    {databaseFilename, masterKeyFilename},
		"complete idempotent": {databaseFilename, masterKeyFilename, administratorTokenFilename},
	} {
		name, prefix := name, prefix
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			fixture := newRecoveryFixture(t)
			if _, err := Create(t.Context(), fixture.createOptions()); err != nil {
				t.Fatal(err)
			}
			restore := fixture.restoreOptions()
			for _, member := range prefix {
				source := filepath.Join(fixture.bundle, member)
				var destination string
				switch member {
				case databaseFilename:
					destination = restore.DatabasePath
				case masterKeyFilename:
					destination = restore.MasterKeyPath
				case administratorTokenFilename:
					destination = restore.AdministratorTokenPath
				}
				copyTestFile(t, source, destination)
			}
			if err := Restore(t.Context(), restore); err != nil {
				t.Fatal(err)
			}
			if err := Restore(t.Context(), restore); err != nil {
				t.Fatalf("idempotent restore: %v", err)
			}
		})
	}

	for name, seed := range map[string]func(*testing.T, *recoveryFixture, RestoreOptions){
		"key without database": func(t *testing.T, fixture *recoveryFixture, restore RestoreOptions) {
			copyTestFile(t, filepath.Join(fixture.bundle, masterKeyFilename), restore.MasterKeyPath)
		},
		"token without predecessors": func(t *testing.T, fixture *recoveryFixture, restore RestoreOptions) {
			copyTestFile(t, filepath.Join(fixture.bundle, administratorTokenFilename), restore.AdministratorTokenPath)
		},
		"mismatched database": func(t *testing.T, _ *recoveryFixture, restore RestoreOptions) {
			if err := os.WriteFile(restore.DatabasePath, []byte("different"), 0o600); err != nil {
				t.Fatal(err)
			}
		},
		"unrelated entry": func(t *testing.T, fixture *recoveryFixture, _ RestoreOptions) {
			if err := os.WriteFile(filepath.Join(fixture.restoreDirectory, "unrelated"), []byte("x"), 0o600); err != nil {
				t.Fatal(err)
			}
		},
		"sqlite sidecar": func(t *testing.T, _ *recoveryFixture, restore RestoreOptions) {
			if err := os.WriteFile(restore.DatabasePath+"-wal", []byte("x"), 0o600); err != nil {
				t.Fatal(err)
			}
		},
	} {
		name, seed := name, seed
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			fixture := newRecoveryFixture(t)
			if _, err := Create(t.Context(), fixture.createOptions()); err != nil {
				t.Fatal(err)
			}
			restore := fixture.restoreOptions()
			seed(t, fixture, restore)
			before := testDirectorySnapshot(t, fixture.restoreDirectory)
			if err := Restore(t.Context(), restore); err == nil {
				t.Fatal("unsafe restore state succeeded")
			}
			after := testDirectorySnapshot(t, fixture.restoreDirectory)
			if !slices.Equal(before, after) {
				t.Fatalf("rejected restore mutated destination: before=%q after=%q", before, after)
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

func TestRestoreInterruptionAfterEveryPublicationBoundaryIsResumable(t *testing.T) {
	t.Parallel()

	for name, testCase := range map[string]struct {
		hooks       restoreHooks
		wantEntries []string
	}{
		"database": {
			hooks: restoreHooks{afterDatabasePublish: func() error {
				return errors.New("injected database publication interruption")
			}},
			wantEntries: []string{"restored.db"},
		},
		"master key": {
			hooks: restoreHooks{afterMasterKeyPublish: func() error {
				return errors.New("injected master-key publication interruption")
			}},
			wantEntries: []string{"restored.db", "restored.key"},
		},
		"administrator token": {
			hooks: restoreHooks{afterAdministratorTokenPublish: func() error {
				return errors.New("injected administrator-token publication interruption")
			}},
			wantEntries: []string{"restored.db", "restored.key", "restored.token"},
		},
	} {
		name, testCase := name, testCase
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			fixture := newRecoveryFixture(t)
			if _, err := Create(t.Context(), fixture.createOptions()); err != nil {
				t.Fatal(err)
			}
			restore := fixture.restoreOptions()
			if err := restoreWithHooks(t.Context(), restore, testCase.hooks); err == nil ||
				!strings.Contains(err.Error(), "injected") {
				t.Fatalf("interrupted restore error = %v", err)
			}
			assertExactRecoveryDirectory(t, fixture.restoreDirectory, testCase.wantEntries)
			if err := Restore(t.Context(), restore); err != nil {
				t.Fatalf("resume interrupted restore: %v", err)
			}
			assertExactRecoveryDirectory(t, fixture.restoreDirectory, []string{
				"restored.db", "restored.key", "restored.token",
			})
		})
	}
}

func TestRestoreCancellationBeforePublicationLeavesNoPartialTarget(t *testing.T) {
	t.Parallel()

	fixture := newRecoveryFixture(t)
	if _, err := Create(t.Context(), fixture.createOptions()); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	err := restoreWithHooks(ctx, fixture.restoreOptions(), restoreHooks{
		beforePublication: func(index int) {
			if index != 0 {
				t.Errorf("unexpected publication index %d", index)
			}
			cancel()
		},
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled restore error = %v, want context.Canceled", err)
	}
	assertExactRecoveryDirectory(t, fixture.restoreDirectory, nil)
}

func TestRestoreRejectsInvalidTargetsBeforeOpeningBundle(t *testing.T) {
	t.Parallel()

	fixture := newRecoveryFixture(t)
	options := fixture.restoreOptions()
	options.Source = filepath.Join(privateTestDirectory(t), "missing-bundle")
	options.DatabasePath = "relative.db"
	err := Restore(t.Context(), options)
	if err == nil || !strings.Contains(err.Error(), "restored control database") {
		t.Fatalf("early target validation error = %v", err)
	}
}

func TestRestoreRejectsFinalNameInItsStagingNamespaceWithoutMutation(t *testing.T) {
	t.Parallel()

	fixture := newRecoveryFixture(t)
	manifest, err := Create(t.Context(), fixture.createOptions())
	if err != nil {
		t.Fatal(err)
	}
	restore := fixture.restoreOptions()
	restore.MasterKeyPath = filepath.Join(
		fixture.restoreDirectory,
		restoreStageNames(manifest.RecoverySetID)[0],
	)
	marker := []byte("must remain untouched")
	if err := os.WriteFile(restore.MasterKeyPath, marker, 0o600); err != nil {
		t.Fatal(err)
	}
	before := testDirectorySnapshot(t, fixture.restoreDirectory)
	if err := Restore(t.Context(), restore); err == nil {
		t.Fatal("restore with a target/staging collision succeeded")
	}
	after := testDirectorySnapshot(t, fixture.restoreDirectory)
	if !slices.Equal(before, after) {
		t.Fatalf("rejected restore mutated destination: before=%q after=%q", before, after)
	}
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
		name, mutate := name, mutate
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

type recoveryFixture struct {
	sourceDatabase     string
	masterKeyPath      string
	administratorPath  string
	bundle             string
	restoreDirectory   string
	release            ReleaseIdentity
	masterKey          []byte
	administratorToken []byte
}

func newRecoveryFixture(t *testing.T) *recoveryFixture {
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
		Name:             "recovery-index",
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
	administratorToken := bytes.Repeat([]byte{'A'}, auth.MinimumBrowserBearerTokenBytes)
	administratorPath := filepath.Join(sourceDirectory, "administrator.token")
	if err := os.WriteFile(administratorPath, append(append([]byte(nil), administratorToken...), '\n'), 0o600); err != nil {
		t.Fatal(err)
	}
	migrationIdentity, err := database.VerifyCurrentMigrations(ctx, migrations.SQLite())
	if err != nil {
		t.Fatal(err)
	}
	bundleParent := privateTestDirectory(t)
	restoreDirectory := privateTestDirectory(t)
	return &recoveryFixture{
		sourceDatabase:     databasePath,
		masterKeyPath:      masterKeyPath,
		administratorPath:  administratorPath,
		bundle:             filepath.Join(bundleParent, "control-plane-bundle"),
		restoreDirectory:   restoreDirectory,
		masterKey:          append([]byte(nil), masterKey...),
		administratorToken: append([]byte(nil), administratorToken...),
		release: ReleaseIdentity{
			ApplicationVersion: "0.1.0",
			SourceRevision:     "development",
			SQLiteMigrations: MigrationIdentity{
				SHA256: strings.Repeat("1", sha256.Size*2), LatestVersion: migrationIdentity.LatestVersion,
			},
			ClickHouseMigrations: MigrationIdentity{
				SHA256: strings.Repeat("2", sha256.Size*2), LatestVersion: 1,
			},
		},
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

func (fixture *recoveryFixture) createOptions() CreateOptions {
	return CreateOptions{
		DatabasePath:           fixture.sourceDatabase,
		MasterKeyPath:          fixture.masterKeyPath,
		AdministratorTokenPath: fixture.administratorPath,
		Destination:            fixture.bundle,
		Release:                fixture.release,
	}
}

func (fixture *recoveryFixture) restoreOptions() RestoreOptions {
	return RestoreOptions{
		Source:                 fixture.bundle,
		DatabasePath:           filepath.Join(fixture.restoreDirectory, "restored.db"),
		MasterKeyPath:          filepath.Join(fixture.restoreDirectory, "restored.key"),
		AdministratorTokenPath: filepath.Join(fixture.restoreDirectory, "restored.token"),
		Release:                fixture.release,
	}
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
		assertPrivateRegular(t, filepath.Join(path, name))
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

func copyTestFile(t *testing.T, source, destination string) {
	t.Helper()
	contents, err := os.ReadFile(source)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(destination, contents, 0o600); err != nil {
		t.Fatal(err)
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
