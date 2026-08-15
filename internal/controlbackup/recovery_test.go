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
	"github.com/Suhaibinator/open-splunk/internal/privatefs"
	"github.com/Suhaibinator/open-splunk/migrations"
	"golang.org/x/sys/unix"
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

func TestRemoveCreatedBundleOwnsCompleteMemberNamespace(t *testing.T) {
	t.Parallel()

	fixture := newRecoveryFixture(t)
	if _, err := Create(t.Context(), fixture.createOptions()); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(
		filepath.Join(fixture.bundle, databaseFilename+"-wal"),
		[]byte("interrupted-sidecar"),
		0o600,
	); err != nil {
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

	if err := RemoveCreatedBundle(parent, filepath.Base(fixture.bundle), child); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(fixture.bundle); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("removed bundle remains: %v", err)
	}
	if err := RemoveCreatedBundle(parent, filepath.Base(fixture.bundle), nil); err == nil {
		t.Fatal("bundle cleanup accepted a missing attempt-owned descriptor")
	}
	assertExactRecoveryDirectory(t, filepath.Dir(fixture.bundle), nil)
}

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

func TestRestoreEnforcesOptionalDeploymentBinding(t *testing.T) {
	t.Parallel()

	for _, testCase := range []struct {
		name        string
		configure   func(Manifest, string, *RestoreOptions)
		wantSuccess bool
	}{
		{
			name: "exact binding",
			configure: func(manifest Manifest, digest string, options *RestoreOptions) {
				options.ExpectedRecoverySetID = manifest.RecoverySetID
				options.ExpectedManifestSHA256 = digest
			},
			wantSuccess: true,
		},
		{
			name: "partial ID",
			configure: func(manifest Manifest, _ string, options *RestoreOptions) {
				options.ExpectedRecoverySetID = manifest.RecoverySetID
			},
		},
		{
			name: "partial digest",
			configure: func(_ Manifest, digest string, options *RestoreOptions) {
				options.ExpectedManifestSHA256 = digest
			},
		},
		{
			name: "malformed ID",
			configure: func(_ Manifest, digest string, options *RestoreOptions) {
				options.ExpectedRecoverySetID = "not-an-id"
				options.ExpectedManifestSHA256 = digest
			},
		},
		{
			name: "malformed digest",
			configure: func(manifest Manifest, _ string, options *RestoreOptions) {
				options.ExpectedRecoverySetID = manifest.RecoverySetID
				options.ExpectedManifestSHA256 = "not-a-digest"
			},
		},
		{
			name: "wrong ID",
			configure: func(manifest Manifest, digest string, options *RestoreOptions) {
				options.ExpectedRecoverySetID = strings.Repeat("0", recoverySetIDHexBytes)
				if options.ExpectedRecoverySetID == manifest.RecoverySetID {
					options.ExpectedRecoverySetID = strings.Repeat("1", recoverySetIDHexBytes)
				}
				options.ExpectedManifestSHA256 = digest
			},
		},
		{
			name: "wrong digest",
			configure: func(manifest Manifest, digest string, options *RestoreOptions) {
				options.ExpectedRecoverySetID = manifest.RecoverySetID
				options.ExpectedManifestSHA256 = strings.Repeat("0", sha256.Size*2)
				if options.ExpectedManifestSHA256 == digest {
					options.ExpectedManifestSHA256 = strings.Repeat("1", sha256.Size*2)
				}
			},
		},
	} {
		testCase := testCase
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			fixture := newRecoveryFixture(t)
			manifest, err := Create(t.Context(), fixture.createOptions())
			if err != nil {
				t.Fatal(err)
			}
			encoded, err := marshalManifest(manifest)
			if err != nil {
				t.Fatal(err)
			}
			digest := sha256.Sum256(encoded)
			restore := fixture.restoreOptions()
			testCase.configure(manifest, hex.EncodeToString(digest[:]), &restore)
			before := testDirectorySnapshot(t, fixture.restoreDirectory)
			preflightErr := PreflightRestore(t.Context(), restore)
			if testCase.wantSuccess {
				if preflightErr != nil {
					t.Fatalf("preflight exact binding: %v", preflightErr)
				}
				if err := Restore(t.Context(), restore); err != nil {
					t.Fatalf("restore exact binding: %v", err)
				}
				return
			}
			if preflightErr == nil {
				t.Fatal("preflight accepted mismatched deployment binding")
			}
			if err := Restore(t.Context(), restore); err == nil {
				t.Fatal("restore accepted mismatched deployment binding")
			}
			after := testDirectorySnapshot(t, fixture.restoreDirectory)
			if !slices.Equal(before, after) {
				t.Fatalf("rejected binding mutated destination: before=%q after=%q", before, after)
			}
		})
	}
}

func TestValidateReleaseIdentityProvidesReusableAndScopedErrors(t *testing.T) {
	t.Parallel()

	valid := ReleaseIdentity{
		ApplicationVersion: "0.1.0",
		SourceRevision:     "development",
		SQLiteMigrations: MigrationIdentity{
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
			name: "SQLite migrations",
			mutate: func(identity *ReleaseIdentity) {
				identity.SQLiteMigrations.LatestVersion = 0
			},
			genericErr: "SQLite migration identity is invalid",
			scopedErr:  "control-plane backup SQLite migration identity is invalid",
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

func TestRestorePreservesExplicitExactDatabaseLock(t *testing.T) {
	t.Parallel()

	fixture := newRecoveryFixture(t)
	if _, err := Create(t.Context(), fixture.createOptions()); err != nil {
		t.Fatal(err)
	}
	restore := fixture.restoreOptions()
	lockPath := restore.DatabasePath + ".server.lock"
	restore.DatabaseLock = openTestDatabaseLock(t, lockPath)
	lockBefore, err := os.Lstat(lockPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := PreflightRestore(t.Context(), restore); err != nil {
		t.Fatalf("preflight exact held database lock: %v", err)
	}
	if err := Restore(t.Context(), restore); err != nil {
		t.Fatal(err)
	}
	if err := Restore(t.Context(), restore); err != nil {
		t.Fatalf("idempotent restore with held database lock: %v", err)
	}
	lockAfter, err := os.Lstat(lockPath)
	if err != nil {
		t.Fatal(err)
	}
	if !os.SameFile(lockBefore, lockAfter) || lockAfter.Size() != 0 ||
		!lockAfter.Mode().IsRegular() || lockAfter.Mode().Perm() != 0o600 {
		t.Fatalf("restored database lock changed: before=%v after=%v", lockBefore, lockAfter)
	}
	assertExactRecoveryDirectory(t, fixture.restoreDirectory, []string{
		filepath.Base(restore.AdministratorTokenPath),
		filepath.Base(restore.DatabasePath),
		filepath.Base(lockPath),
		filepath.Base(restore.MasterKeyPath),
	})
}

func TestPreflightRestoreAdmitsSafeInterruptedStageAndValidPrefixWithoutMutation(t *testing.T) {
	t.Parallel()

	fixture := newRecoveryFixture(t)
	manifest, err := Create(t.Context(), fixture.createOptions())
	if err != nil {
		t.Fatal(err)
	}
	restore := fixture.restoreOptions()
	copyTestFile(
		t,
		filepath.Join(fixture.bundle, databaseFilename),
		restore.DatabasePath,
	)
	staleStagePath := filepath.Join(
		fixture.restoreDirectory,
		restoreStageNames(manifest.RecoverySetID)[1],
	)
	if err := os.WriteFile(staleStagePath, []byte("safe stale stage"), 0o600); err != nil {
		t.Fatal(err)
	}
	before := testDirectorySnapshot(t, fixture.restoreDirectory)
	if err := PreflightRestore(t.Context(), restore); err != nil {
		t.Fatal(err)
	}
	after := testDirectorySnapshot(t, fixture.restoreDirectory)
	if !slices.Equal(before, after) {
		t.Fatalf("preflight mutated resumable state: before=%q after=%q", before, after)
	}
	if err := Restore(t.Context(), restore); err != nil {
		t.Fatalf("restore after resumable preflight: %v", err)
	}
}

func TestCleanupRestoreStagesPreservesReplacementBeforeAnyMutation(t *testing.T) {
	t.Parallel()

	destinationPath := privateTestDirectory(t)
	destination, err := privatefs.OpenDirectory(destinationPath)
	if err != nil {
		t.Fatal(err)
	}
	defer destination.Close()
	stageNames := []string{
		".restore-test-database",
		".restore-test-master-key",
	}
	contents := [][]byte{
		[]byte("stale database"),
		[]byte("stale master key"),
	}
	for index, stageName := range stageNames {
		if err := os.WriteFile(
			filepath.Join(destinationPath, stageName),
			contents[index],
			0o600,
		); err != nil {
			t.Fatal(err)
		}
	}
	replacedPath := filepath.Join(destinationPath, stageNames[1])
	originalPath := replacedPath + ".original"
	var hookErr error
	err = cleanupRestoreStagesWithHooks(
		destination,
		stageNames,
		nil,
		"",
		nil,
		cleanupRestoreStagesHooks{
			afterPreflight: func() {
				hookErr = os.Rename(replacedPath, originalPath)
				if hookErr != nil {
					return
				}
				hookErr = os.WriteFile(replacedPath, contents[1], 0o600)
			},
		},
	)
	if hookErr != nil {
		t.Fatal(hookErr)
	}
	if err == nil {
		t.Fatal("stale-stage cleanup accepted a content-conforming path replacement")
	}
	for _, candidate := range []struct {
		path     string
		contents []byte
	}{
		{path: filepath.Join(destinationPath, stageNames[0]), contents: contents[0]},
		{path: replacedPath, contents: contents[1]},
		{path: originalPath, contents: contents[1]},
	} {
		got, readErr := os.ReadFile(candidate.path)
		if readErr != nil || !bytes.Equal(got, candidate.contents) {
			t.Fatalf("preserved candidate %q = %q, error=%v", candidate.path, got, readErr)
		}
	}
}

func TestRestoreRebuildsPlanAfterSuccessfulPreflight(t *testing.T) {
	t.Parallel()

	fixture := newRecoveryFixture(t)
	if _, err := Create(t.Context(), fixture.createOptions()); err != nil {
		t.Fatal(err)
	}
	restore := fixture.restoreOptions()
	if err := PreflightRestore(t.Context(), restore); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(
		filepath.Join(fixture.restoreDirectory, "appeared-after-preflight"),
		[]byte("unrelated"),
		0o600,
	); err != nil {
		t.Fatal(err)
	}
	before := testDirectorySnapshot(t, fixture.restoreDirectory)
	if err := Restore(t.Context(), restore); err == nil {
		t.Fatal("restore accepted destination state that changed after preflight")
	}
	after := testDirectorySnapshot(t, fixture.restoreDirectory)
	if !slices.Equal(before, after) {
		t.Fatalf("rejected restore mutated destination: before=%q after=%q", before, after)
	}
}

func TestRestoreRejectsUnsafeOrImplicitDatabaseLockWithoutMutation(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name string
		seed func(*testing.T, *recoveryFixture, *RestoreOptions, string)
	}{
		{
			name: "implicit",
			seed: func(t *testing.T, _ *recoveryFixture, _ *RestoreOptions, path string) {
				t.Helper()
				if err := os.WriteFile(path, nil, 0o600); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "removed pathname",
			seed: func(t *testing.T, _ *recoveryFixture, restore *RestoreOptions, path string) {
				restore.DatabaseLock = openTestDatabaseLock(t, path)
				if err := os.Remove(path); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "closed descriptor",
			seed: func(t *testing.T, _ *recoveryFixture, restore *RestoreOptions, path string) {
				restore.DatabaseLock = openTestDatabaseLock(t, path)
				if err := restore.DatabaseLock.Close(); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "wrong exact path",
			seed: func(t *testing.T, _ *recoveryFixture, restore *RestoreOptions, _ string) {
				restore.DatabaseLock = openTestDatabaseLock(
					t,
					filepath.Join(privateTestDirectory(t), "other.server.lock"),
				)
			},
		},
		{
			name: "mode",
			seed: func(t *testing.T, _ *recoveryFixture, restore *RestoreOptions, path string) {
				t.Helper()
				if err := os.WriteFile(path, nil, 0o600); err != nil {
					t.Fatal(err)
				}
				if err := os.Chmod(path, 0o640); err != nil {
					t.Fatal(err)
				}
				restore.DatabaseLock = openTestDatabaseLock(t, path)
			},
		},
		{
			name: "nonempty",
			seed: func(t *testing.T, _ *recoveryFixture, restore *RestoreOptions, path string) {
				t.Helper()
				if err := os.WriteFile(path, []byte("not a lock artifact"), 0o600); err != nil {
					t.Fatal(err)
				}
				restore.DatabaseLock = openTestDatabaseLock(t, path)
			},
		},
		{
			name: "symlink",
			seed: func(t *testing.T, _ *recoveryFixture, restore *RestoreOptions, path string) {
				t.Helper()
				target := filepath.Join(t.TempDir(), "target")
				if err := os.WriteFile(target, nil, 0o600); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(target, path); err != nil {
					t.Fatal(err)
				}
				restore.DatabaseLock = openTestDatabaseLock(t, path)
			},
		},
		{
			name: "hardlink",
			seed: func(t *testing.T, _ *recoveryFixture, restore *RestoreOptions, path string) {
				t.Helper()
				target := filepath.Join(t.TempDir(), "target")
				if err := os.WriteFile(target, nil, 0o600); err != nil {
					t.Fatal(err)
				}
				if err := os.Link(target, path); err != nil {
					t.Fatal(err)
				}
				restore.DatabaseLock = openTestDatabaseLock(t, path)
			},
		},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			fixture := newRecoveryFixture(t)
			if _, err := Create(t.Context(), fixture.createOptions()); err != nil {
				t.Fatal(err)
			}
			restore := fixture.restoreOptions()
			lockPath := restore.DatabasePath + ".server.lock"
			test.seed(t, fixture, &restore, lockPath)
			before := testDirectorySnapshot(t, fixture.restoreDirectory)
			if err := PreflightRestore(t.Context(), restore); err == nil {
				t.Fatal("unsafe database lock state passed preflight")
			}
			if err := Restore(t.Context(), restore); err == nil {
				t.Fatal("unsafe database lock state restored")
			}
			after := testDirectorySnapshot(t, fixture.restoreDirectory)
			if !slices.Equal(before, after) {
				t.Fatalf("rejected lock state mutated destination: before=%q after=%q", before, after)
			}
		})
	}
}

func TestRestoreRejectsDatabaseLockPathReplacementDuringPublication(t *testing.T) {
	t.Parallel()

	fixture := newRecoveryFixture(t)
	if _, err := Create(t.Context(), fixture.createOptions()); err != nil {
		t.Fatal(err)
	}
	restore := fixture.restoreOptions()
	lockPath := restore.DatabasePath + ".server.lock"
	restore.DatabaseLock = openTestDatabaseLock(t, lockPath)
	movedLockPath := filepath.Join(filepath.Dir(fixture.restoreDirectory), "moved-held-lock")
	replaced := false
	err := restoreWithHooks(t.Context(), restore, restoreHooks{
		beforePublication: func(index int) {
			if index != 0 || replaced {
				return
			}
			replaced = true
			if renameErr := os.Rename(lockPath, movedLockPath); renameErr != nil {
				t.Fatal(renameErr)
			}
			if writeErr := os.WriteFile(lockPath, nil, 0o600); writeErr != nil {
				t.Fatal(writeErr)
			}
		},
	})
	if err == nil || !strings.Contains(err.Error(), "no longer names the held lock") {
		t.Fatalf("lock replacement restore error = %v", err)
	}
	if !replaced {
		t.Fatal("lock replacement hook did not run")
	}
	for _, path := range []string{
		restore.DatabasePath,
		restore.MasterKeyPath,
		restore.AdministratorTokenPath,
	} {
		if _, statErr := os.Lstat(path); !errors.Is(statErr, os.ErrNotExist) {
			t.Fatalf("lock replacement published %q: %v", path, statErr)
		}
	}
}

func openTestDatabaseLock(t *testing.T, path string) *os.File {
	t.Helper()
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	if err := unix.Flock(int(file.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = file.Close() })
	return file
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
