package collector

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/protocolid"
	"github.com/google/uuid"
)

func TestInitializeCollectorIDCreatesCanonicalStableOwnerOnlyIdentity(t *testing.T) {
	t.Parallel()

	stateDirectory := filepath.Join(t.TempDir(), "nested", "collector-state")
	first, err := InitializeCollectorID(stateDirectory)
	if err != nil {
		t.Fatalf("InitializeCollectorID(first): %v", err)
	}
	if !protocolid.Valid(first) {
		t.Fatalf("generated collector ID %q is not a canonical protocol identifier", first)
	}
	if _, err := uuid.Parse(first); err != nil {
		t.Fatalf("generated collector ID %q is not a UUID: %v", first, err)
	}

	identityPath := filepath.Join(stateDirectory, collectorIDFile)
	firstInfo, err := os.Stat(identityPath)
	if err != nil {
		t.Fatalf("stat generated collector identity: %v", err)
	}
	if !firstInfo.Mode().IsRegular() {
		t.Fatalf("collector identity mode = %v, want a regular file", firstInfo.Mode())
	}
	if permissions := firstInfo.Mode().Perm(); permissions != 0o600 {
		t.Fatalf("collector identity permissions = %#o, want 0600", permissions)
	}
	contents, err := os.ReadFile(identityPath)
	if err != nil {
		t.Fatalf("read generated collector identity: %v", err)
	}
	if got, want := string(contents), first+"\n"; got != want {
		t.Fatalf("collector identity encoding = %q, want %q", got, want)
	}
	stateInfo, err := os.Stat(stateDirectory)
	if err != nil {
		t.Fatalf("stat collector state directory: %v", err)
	}
	if permissions := stateInfo.Mode().Perm(); permissions != 0o700 {
		t.Fatalf("collector state directory permissions = %#o, want 0700", permissions)
	}
	assertNoCollectorIdentityTemporaryFiles(t, stateDirectory)

	second, err := InitializeCollectorID(stateDirectory)
	if err != nil {
		t.Fatalf("InitializeCollectorID(second): %v", err)
	}
	if second != first {
		t.Fatalf("collector ID changed across initialization: first %q, second %q", first, second)
	}
	secondInfo, err := os.Stat(identityPath)
	if err != nil {
		t.Fatalf("stat reused collector identity: %v", err)
	}
	if !os.SameFile(firstInfo, secondInfo) {
		t.Fatal("stable collector identity was replaced instead of reused")
	}
	contents, err = os.ReadFile(identityPath)
	if err != nil {
		t.Fatalf("read reused collector identity: %v", err)
	}
	if got, want := string(contents), first+"\n"; got != want {
		t.Fatalf("reused collector identity encoding = %q, want %q", got, want)
	}
	assertNoCollectorIdentityTemporaryFiles(t, stateDirectory)
}

func TestValidateCollectorStateDirectoryPathRejectsFilesystemRootAndCurrentDirectory(t *testing.T) {
	t.Parallel()

	for _, path := range []string{string(os.PathSeparator), "."} {
		if err := validateCollectorStateDirectoryPath(filepath.Clean(path)); err == nil {
			t.Fatalf("validateCollectorStateDirectoryPath(%q) succeeded", path)
		}
	}
	if err := validateCollectorStateDirectoryPath(filepath.Join("state", "collector")); err != nil {
		t.Fatalf("validateCollectorStateDirectoryPath(dedicated child): %v", err)
	}
}

func TestInitializeCollectorIDReusesCanonicalPersistedIdentity(t *testing.T) {
	t.Parallel()

	stateDirectory := t.TempDir()
	const persisted = "collector-custom.01"
	identityPath := filepath.Join(stateDirectory, collectorIDFile)
	if err := os.WriteFile(identityPath, []byte(persisted+"\n"), 0o600); err != nil {
		t.Fatalf("write existing collector identity: %v", err)
	}
	before, err := os.Stat(identityPath)
	if err != nil {
		t.Fatal(err)
	}

	got, err := InitializeCollectorID(stateDirectory)
	if err != nil {
		t.Fatalf("InitializeCollectorID: %v", err)
	}
	if got != persisted {
		t.Fatalf("InitializeCollectorID = %q, want persisted identity %q", got, persisted)
	}
	after, err := os.Stat(identityPath)
	if err != nil {
		t.Fatal(err)
	}
	if !os.SameFile(before, after) {
		t.Fatal("canonical persisted collector identity was replaced")
	}
	contents, err := os.ReadFile(identityPath)
	if err != nil {
		t.Fatal(err)
	}
	if got := string(contents); got != persisted+"\n" {
		t.Fatalf("persisted collector identity changed to %q", got)
	}
}

func TestInitializeCollectorIDRejectsInvalidPersistedIdentityWithoutMutation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		contents string
	}{
		{name: "empty", contents: ""},
		{name: "whitespace", contents: " \n"},
		{name: "malformed", contents: "-leading-hyphen\n"},
		{name: "multiple records", contents: "collector-one\ncollector-two\n"},
		{
			name:     "oversized",
			contents: strings.Repeat("a", int(protocolid.MaximumBytes)+1) + "\n",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			stateDirectory := t.TempDir()
			identityPath := filepath.Join(stateDirectory, collectorIDFile)
			if err := os.WriteFile(identityPath, []byte(test.contents), 0o600); err != nil {
				t.Fatalf("write invalid collector identity: %v", err)
			}
			before, err := os.Stat(identityPath)
			if err != nil {
				t.Fatal(err)
			}

			if got, err := InitializeCollectorID(stateDirectory); err == nil {
				t.Fatalf("InitializeCollectorID returned %q for invalid persisted identity", got)
			}
			after, err := os.Stat(identityPath)
			if err != nil {
				t.Fatalf("stat rejected collector identity: %v", err)
			}
			if !os.SameFile(before, after) {
				t.Fatal("invalid persisted collector identity was replaced")
			}
			contents, err := os.ReadFile(identityPath)
			if err != nil {
				t.Fatalf("read rejected collector identity: %v", err)
			}
			if got := string(contents); got != test.contents {
				t.Fatalf("invalid persisted collector identity mutated to %q", got)
			}
			assertNoCollectorIdentityTemporaryFiles(t, stateDirectory)
		})
	}
}

func TestInitializeCollectorIDRejectsUnsafeIdentityPermissionsWithoutMutation(t *testing.T) {
	t.Parallel()

	stateDirectory := t.TempDir()
	identityPath := filepath.Join(stateDirectory, collectorIDFile)
	const persisted = "collector-readable-by-others\n"
	if err := os.WriteFile(identityPath, []byte(persisted), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(identityPath, 0o644); err != nil {
		t.Fatal(err)
	}
	before, err := os.Stat(identityPath)
	if err != nil {
		t.Fatal(err)
	}

	if got, err := InitializeCollectorID(stateDirectory); err == nil {
		t.Fatalf("InitializeCollectorID returned %q for an identity with unsafe permissions", got)
	}
	after, err := os.Stat(identityPath)
	if err != nil {
		t.Fatal(err)
	}
	if !os.SameFile(before, after) || after.Mode().Perm() != 0o644 {
		t.Fatalf("unsafe identity file changed: before=%v after=%v", before.Mode(), after.Mode())
	}
	contents, err := os.ReadFile(identityPath)
	if err != nil {
		t.Fatal(err)
	}
	if got := string(contents); got != persisted {
		t.Fatalf("unsafe identity contents changed to %q", got)
	}
	assertNoCollectorIdentityTemporaryFiles(t, stateDirectory)
}

func TestInitializeCollectorIDRejectsSymlinkWithoutMutation(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	stateDirectory := filepath.Join(root, "state")
	if err := os.Mkdir(stateDirectory, 0o700); err != nil {
		t.Fatal(err)
	}
	targetPath := filepath.Join(root, "external-identity")
	const targetContents = "collector-external\n"
	if err := os.WriteFile(targetPath, []byte(targetContents), 0o600); err != nil {
		t.Fatal(err)
	}
	identityPath := filepath.Join(stateDirectory, collectorIDFile)
	if err := os.Symlink(targetPath, identityPath); err != nil {
		t.Fatalf("create collector identity symlink: %v", err)
	}

	if got, err := InitializeCollectorID(stateDirectory); err == nil {
		t.Fatalf("InitializeCollectorID followed identity symlink and returned %q", got)
	}
	info, err := os.Lstat(identityPath)
	if err != nil {
		t.Fatalf("lstat rejected identity symlink: %v", err)
	}
	if info.Mode()&os.ModeSymlink == 0 {
		t.Fatalf("identity path mode = %v, want the original symlink", info.Mode())
	}
	linkTarget, err := os.Readlink(identityPath)
	if err != nil {
		t.Fatal(err)
	}
	if linkTarget != targetPath {
		t.Fatalf("identity symlink target = %q, want %q", linkTarget, targetPath)
	}
	contents, err := os.ReadFile(targetPath)
	if err != nil {
		t.Fatal(err)
	}
	if got := string(contents); got != targetContents {
		t.Fatalf("identity symlink target mutated to %q", got)
	}
	assertNoCollectorIdentityTemporaryFiles(t, stateDirectory)
}

func TestInitializeCollectorIDRejectsStateDirectorySymlink(t *testing.T) {
	t.Parallel()

	for name, suffix := range map[string]string{
		"plain":              "",
		"trailing separator": string(os.PathSeparator),
		"trailing dot":       string(os.PathSeparator) + ".",
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			root := t.TempDir()
			target := filepath.Join(root, "target")
			if err := os.Mkdir(target, 0o700); err != nil {
				t.Fatal(err)
			}
			stateLink := filepath.Join(root, "state-link")
			if err := os.Symlink(target, stateLink); err != nil {
				t.Fatal(err)
			}
			if got, err := InitializeCollectorID(stateLink + suffix); err == nil {
				t.Fatalf("InitializeCollectorID followed state-directory symlink and returned %q", got)
			}
			if _, err := os.Lstat(filepath.Join(target, collectorIDFile)); !os.IsNotExist(err) {
				t.Fatalf("state-directory symlink target received an identity: %v", err)
			}
		})
	}
}

func TestInitializeCollectorIDCleansCrashLeftoverTemporaryFiles(t *testing.T) {
	t.Parallel()

	stateDirectory := t.TempDir()
	temporaryPath := filepath.Join(stateDirectory, collectorIdentityTempPrefix+"crash")
	if err := os.WriteFile(temporaryPath, []byte("unpublished-id\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	id, err := InitializeCollectorID(stateDirectory)
	if err != nil {
		t.Fatalf("InitializeCollectorID: %v", err)
	}
	if !protocolid.Valid(id) {
		t.Fatalf("collector ID = %q, want canonical identifier", id)
	}
	if _, err := os.Lstat(temporaryPath); !os.IsNotExist(err) {
		t.Fatalf("stale identity temporary file remains: %v", err)
	}
	assertNoCollectorIdentityTemporaryFiles(t, stateDirectory)
}

func TestInitializeCollectorIDCleansPublishedTemporaryHardLink(t *testing.T) {
	t.Parallel()

	stateDirectory := t.TempDir()
	identityPath := filepath.Join(stateDirectory, collectorIDFile)
	const persisted = "collector-after-publish-crash"
	if err := os.WriteFile(identityPath, []byte(persisted+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	temporaryPath := filepath.Join(stateDirectory, collectorIdentityTempPrefix+"published")
	if err := os.Link(identityPath, temporaryPath); err != nil {
		t.Fatal(err)
	}
	got, err := InitializeCollectorID(stateDirectory)
	if err != nil {
		t.Fatalf("InitializeCollectorID: %v", err)
	}
	if got != persisted {
		t.Fatalf("InitializeCollectorID = %q, want %q", got, persisted)
	}
	if _, err := os.Lstat(temporaryPath); !os.IsNotExist(err) {
		t.Fatalf("published identity temporary link remains: %v", err)
	}
	assertNoCollectorIdentityTemporaryFiles(t, stateDirectory)
}

func TestInitializeCollectorIDRejectsExternalHardLink(t *testing.T) {
	t.Parallel()
	if runtime.GOOS != "darwin" && runtime.GOOS != "linux" {
		t.Skip("hard-link count validation is implemented on production Unix targets")
	}

	root := t.TempDir()
	stateDirectory := filepath.Join(root, "state")
	if err := os.Mkdir(stateDirectory, 0o700); err != nil {
		t.Fatal(err)
	}
	externalPath := filepath.Join(root, "external-identity")
	const persisted = "collector-with-external-alias\n"
	if err := os.WriteFile(externalPath, []byte(persisted), 0o600); err != nil {
		t.Fatal(err)
	}
	identityPath := filepath.Join(stateDirectory, collectorIDFile)
	if err := os.Link(externalPath, identityPath); err != nil {
		t.Fatal(err)
	}
	if got, err := InitializeCollectorID(stateDirectory); err == nil {
		t.Fatalf("InitializeCollectorID accepted externally linked identity %q", got)
	}
	contents, err := os.ReadFile(externalPath)
	if err != nil {
		t.Fatal(err)
	}
	if got := string(contents); got != persisted {
		t.Fatalf("external identity alias changed to %q", got)
	}
	if info, err := os.Lstat(identityPath); err != nil || !info.Mode().IsRegular() {
		t.Fatalf("identity hard link changed: info=%v err=%v", info, err)
	}
}

func TestInitializeCollectorIDRefusesToReplaceMissingIdentityForPriorState(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		create func(*testing.T, string) string
	}{
		{
			name: "wal",
			create: func(t *testing.T, stateDirectory string) string {
				t.Helper()
				path := filepath.Join(stateDirectory, walSubdir)
				if err := os.Mkdir(path, 0o700); err != nil {
					t.Fatal(err)
				}
				return path
			},
		},
		{
			name: "checkpoints",
			create: func(t *testing.T, stateDirectory string) string {
				t.Helper()
				path := filepath.Join(stateDirectory, checkpointsSubdir)
				if err := os.Mkdir(path, 0o700); err != nil {
					t.Fatal(err)
				}
				return path
			},
		},
		{
			name: "dead letter",
			create: func(t *testing.T, stateDirectory string) string {
				t.Helper()
				path := filepath.Join(stateDirectory, deadLetterFile)
				if err := os.WriteFile(path, []byte("prior terminal event\n"), 0o600); err != nil {
					t.Fatal(err)
				}
				return path
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			stateDirectory := t.TempDir()
			artifactPath := test.create(t, stateDirectory)
			if got, err := InitializeCollectorID(stateDirectory); err == nil {
				t.Fatalf("InitializeCollectorID returned replacement identity %q beside prior %s state", got, test.name)
			}
			if _, err := os.Lstat(filepath.Join(stateDirectory, collectorIDFile)); !os.IsNotExist(err) {
				t.Fatalf("collector identity exists after refused replacement: %v", err)
			}
			if _, err := os.Lstat(artifactPath); err != nil {
				t.Fatalf("prior %s artifact was mutated or removed: %v", test.name, err)
			}
			assertNoCollectorIdentityTemporaryFiles(t, stateDirectory)
		})
	}
}

func TestInitializeCollectorIDHonorsStateDirectoryLock(t *testing.T) {
	t.Parallel()

	stateDirectory := t.TempDir()
	lock, err := acquireStateDirectoryLock(stateDirectory)
	if err != nil {
		t.Fatalf("acquire state directory lock: %v", err)
	}
	if got, err := InitializeCollectorID(stateDirectory); err == nil {
		_ = lock.Close()
		t.Fatalf("InitializeCollectorID returned %q while state directory was in use", got)
	}
	if _, err := os.Lstat(filepath.Join(stateDirectory, collectorIDFile)); !os.IsNotExist(err) {
		_ = lock.Close()
		t.Fatalf("collector identity exists after lock exclusion: %v", err)
	}
	assertNoCollectorIdentityTemporaryFiles(t, stateDirectory)
	if err := lock.Close(); err != nil {
		t.Fatalf("release state directory lock: %v", err)
	}

	id, err := InitializeCollectorID(stateDirectory)
	if err != nil {
		t.Fatalf("InitializeCollectorID after lock release: %v", err)
	}
	if !protocolid.Valid(id) {
		t.Fatalf("collector ID after lock release = %q, want canonical identifier", id)
	}
	assertNoCollectorIdentityTemporaryFiles(t, stateDirectory)
}

func assertNoCollectorIdentityTemporaryFiles(t *testing.T, stateDirectory string) {
	t.Helper()
	entries, err := os.ReadDir(stateDirectory)
	if err != nil {
		t.Fatalf("read collector state directory: %v", err)
	}
	for _, entry := range entries {
		name := entry.Name()
		if name != collectorIDFile &&
			(strings.Contains(name, collectorIDFile) || strings.Contains(name, "collector-id")) {
			t.Errorf("collector identity temporary file was not cleaned up: %q", name)
		}
	}
}
