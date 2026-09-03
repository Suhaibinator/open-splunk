//go:build darwin || linux

package privatefs

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"golang.org/x/sys/unix"
)

func TestValidateComponent(t *testing.T) {
	t.Parallel()

	for _, name := range []string{
		"backup",
		"backup-2026.08.01",
		".stage_01",
		strings.Repeat("a", MaximumComponentBytes),
	} {
		if err := ValidateComponent(name); err != nil {
			t.Errorf("ValidateComponent(%q) = %v", name, err)
		}
	}

	for _, name := range []string{
		"",
		".",
		"..",
		"../backup",
		"backup/member",
		`backup\member`,
		"backup member",
		"backup\x00member",
		"backup\nmember",
		"bäckup",
		string([]byte{0xff}),
		strings.Repeat("a", MaximumComponentBytes+1),
	} {
		if err := ValidateComponent(name); err == nil {
			t.Errorf("ValidateComponent(%q) unexpectedly succeeded", name)
		}
	}
}

func TestRandomNameProducesValidComponents(t *testing.T) {
	t.Parallel()

	generator := RandomName(".stage-")
	first, err := generator()
	if err != nil {
		t.Fatal(err)
	}
	second, err := generator()
	if err != nil {
		t.Fatal(err)
	}
	if first == second {
		t.Fatal("two random temporary names are identical")
	}
	for _, name := range []string{first, second} {
		if err := ValidateComponent(name); err != nil {
			t.Fatalf("generated name %q is invalid: %v", name, err)
		}
		if len(name) != len(".stage-")+32 {
			t.Fatalf("generated name length = %d, want %d", len(name), len(".stage-")+32)
		}
	}

	invalidGenerator := RandomName("bad/name-")
	if _, err := invalidGenerator(); err == nil {
		t.Fatal("RandomName accepted an invalid prefix")
	}
}

func TestOpenDirectoryRequiresStablePrivateDirectory(t *testing.T) {
	t.Parallel()

	if _, err := OpenDirectory("relative"); err == nil {
		t.Fatal("OpenDirectory accepted a relative path")
	}
	if _, err := OpenDirectory("/tmp/private\x00directory"); err == nil {
		t.Fatal("OpenDirectory accepted a NUL byte")
	}

	base := t.TempDir()
	privatePath := filepath.Join(base, "private")
	mustMkdir(t, privatePath, 0o700)
	directory, err := OpenDirectory(privatePath + string(os.PathSeparator) + ".")
	if err != nil {
		t.Fatal(err)
	}
	if directory.Path() != privatePath {
		t.Fatalf("Path() = %q, want %q", directory.Path(), privatePath)
	}
	if err := directory.Close(); err != nil {
		t.Fatal(err)
	}
	if err := directory.Close(); err != nil {
		t.Fatalf("second Close() = %v", err)
	}

	unsafePath := filepath.Join(base, "unsafe")
	mustMkdir(t, unsafePath, 0o755)
	if _, err := OpenDirectory(unsafePath); err == nil {
		t.Fatal("OpenDirectory accepted mode 0755")
	}
	specialPath := filepath.Join(base, "special")
	mustMkdir(t, specialPath, 0o700|os.ModeSticky)
	if _, err := OpenDirectory(specialPath); err == nil {
		t.Fatal("OpenDirectory accepted a special permission bit")
	}

	regularPath := filepath.Join(base, "regular")
	mustWriteFile(t, regularPath, []byte("data"), 0o700)
	if _, err := OpenDirectory(regularPath); err == nil {
		t.Fatal("OpenDirectory accepted a regular file")
	}

	symlinkPath := filepath.Join(base, "symlink")
	if err := os.Symlink(privatePath, symlinkPath); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenDirectory(symlinkPath); err == nil {
		t.Fatal("OpenDirectory followed a final symlink")
	}

	info, err := os.Lstat(privatePath)
	if err != nil {
		t.Fatal(err)
	}
	if err := validateOwnedDirectory(info, os.Geteuid()+1); err == nil {
		t.Fatal("directory ownership validation accepted another user")
	}
}

func TestOpenRegularEnforcesCompletePolicy(t *testing.T) {
	t.Parallel()

	path, directory := openTestDirectory(t)
	policy := FilePolicy{
		AllowedModes: []fs.FileMode{0o400, 0o600},
		MinimumSize:  1,
		MaximumSize:  3,
	}
	mustWriteFile(t, filepath.Join(path, "valid"), []byte("abc"), 0o400)
	file, err := directory.OpenRegular("valid", policy)
	if err != nil {
		t.Fatal(err)
	}
	contents, err := io.ReadAll(file)
	if err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	if string(contents) != "abc" {
		t.Fatalf("valid contents = %q, want abc", contents)
	}

	fixtures := []struct {
		name  string
		setup func(string)
	}{
		{
			name: "symlink",
			setup: func(name string) {
				if err := os.Symlink("valid", filepath.Join(path, name)); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "fifo",
			setup: func(name string) {
				if err := unix.Mkfifo(filepath.Join(path, name), 0o600); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "directory",
			setup: func(name string) {
				mustMkdir(t, filepath.Join(path, name), 0o700)
			},
		},
		{
			name: "hardlink",
			setup: func(name string) {
				source := filepath.Join(path, "hardlink-source")
				mustWriteFile(t, source, []byte("abc"), 0o600)
				if err := os.Link(source, filepath.Join(path, name)); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "bad-mode",
			setup: func(name string) {
				mustWriteFile(t, filepath.Join(path, name), []byte("abc"), 0o640)
			},
		},
		{
			name: "special-mode",
			setup: func(name string) {
				filePath := filepath.Join(path, name)
				mustWriteFile(t, filePath, []byte("abc"), 0o600)
				if err := os.Chmod(filePath, 0o600|os.ModeSetuid); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "too-small",
			setup: func(name string) {
				mustWriteFile(t, filepath.Join(path, name), nil, 0o600)
			},
		},
		{
			name: "too-large",
			setup: func(name string) {
				mustWriteFile(t, filepath.Join(path, name), []byte("abcd"), 0o600)
			},
		},
	}
	for _, fixture := range fixtures {
		fixture.setup(fixture.name)
		if opened, openErr := directory.OpenRegular(fixture.name, policy); openErr == nil {
			_ = opened.Close()
			t.Errorf("OpenRegular(%q) unexpectedly succeeded", fixture.name)
		}
	}

	if _, err := directory.OpenRegular("missing", policy); err == nil {
		t.Fatal("OpenRegular accepted a missing file")
	}
	if _, err := directory.OpenRegular("../valid", policy); err == nil {
		t.Fatal("OpenRegular accepted a path instead of a component")
	}

	info, err := os.Lstat(filepath.Join(path, "valid"))
	if err != nil {
		t.Fatal(err)
	}
	if err := validateOwnedRegularFile(info, policy, os.Geteuid()+1); err == nil {
		t.Fatal("file ownership validation accepted another user")
	}

	invalidPolicies := []FilePolicy{
		{},
		{AllowedModes: []fs.FileMode{0o600}, MinimumSize: -1},
		{AllowedModes: []fs.FileMode{0o600}, MinimumSize: 2, MaximumSize: 1},
		{AllowedModes: []fs.FileMode{os.ModeDir | 0o600}},
		{AllowedModes: []fs.FileMode{os.ModeSticky | 0o600}},
	}
	for _, invalid := range invalidPolicies {
		if _, err := directory.OpenRegular("valid", invalid); err == nil {
			t.Errorf("OpenRegular accepted invalid policy %+v", invalid)
		}
	}
}

func TestCreateTemporaryObjectsUsesExactModes(t *testing.T) {
	t.Parallel()

	path, directory := openTestDirectory(t)
	mustWriteFile(t, filepath.Join(path, "collision"), []byte("reserved"), 0o600)
	name, file, err := directory.CreateTemporaryFile(
		sequenceGenerator("collision", ".file-stage"),
	)
	if err != nil {
		t.Fatal(err)
	}
	if name != ".file-stage" {
		t.Fatalf("temporary file name = %q, want .file-stage", name)
	}
	assertExactMode(t, filepath.Join(path, name), 0o600)
	if _, err := file.WriteString("payload"); err != nil {
		t.Fatal(err)
	}
	if err := SyncFile(file); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	opened, err := directory.OpenRegular(name, FilePolicy{
		AllowedModes: []fs.FileMode{0o600},
		MinimumSize:  7,
		MaximumSize:  7,
	})
	if err != nil {
		t.Fatal(err)
	}

	directoryName, child, err := directory.CreateTemporaryDirectory(
		sequenceGenerator("collision", ".directory-stage"),
	)
	if err != nil {
		t.Fatal(err)
	}
	if directoryName != ".directory-stage" {
		t.Fatalf("temporary directory name = %q, want .directory-stage", directoryName)
	}
	if child.Path() != filepath.Join(path, directoryName) {
		t.Fatalf("child Path() = %q", child.Path())
	}
	assertExactMode(t, child.Path(), 0o700)
	if err := child.RequireEntries(nil, 0); err != nil {
		t.Fatal(err)
	}
	if err := child.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := child.Close(); err != nil {
		t.Fatal(err)
	}
	stage, err := directory.openChildDirectory(directoryName, false)
	if err != nil {
		t.Fatal(err)
	}
	if err := directory.RemovePinnedEmptyDirectory(directoryName, stage); err != nil {
		t.Fatal(err)
	}
	if err := stage.Close(); err != nil {
		t.Fatal(err)
	}

	if err := directory.UnlinkPinnedRegular(name, opened); err != nil {
		t.Fatal(err)
	}
	if err := opened.Close(); err != nil {
		t.Fatal(err)
	}
	if err := directory.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := SyncFile(nil); err == nil {
		t.Fatal("SyncFile accepted a nil file")
	}
}

func TestTemporaryCreationRejectsBadGeneratorsAndExhaustion(t *testing.T) {
	t.Parallel()

	path, directory := openTestDirectory(t)
	if _, _, err := directory.CreateTemporaryFile(nil); err == nil {
		t.Fatal("CreateTemporaryFile accepted a nil generator")
	}
	if _, _, err := directory.CreateTemporaryDirectory(
		sequenceGenerator("../stage"),
	); err == nil {
		t.Fatal("CreateTemporaryDirectory accepted an invalid name")
	}
	wantErr := errors.New("generator failed")
	if _, _, err := directory.CreateTemporaryFile(func() (string, error) {
		return "", wantErr
	}); !errors.Is(err, wantErr) {
		t.Fatalf("generator error = %v, want %v", err, wantErr)
	}

	mustWriteFile(t, filepath.Join(path, "collision"), nil, 0o600)
	if _, _, err := directory.CreateTemporaryFile(func() (string, error) {
		return "collision", nil
	}); err == nil || !strings.Contains(err.Error(), "all 16 candidate names exist") {
		t.Fatalf("collision exhaustion error = %v", err)
	}
	if _, _, err := directory.CreateTemporaryDirectory(func() (string, error) {
		return "collision", nil
	}); err == nil || !strings.Contains(err.Error(), "all 16 candidate names exist") {
		t.Fatalf("directory collision exhaustion error = %v", err)
	}
	if err := directory.RequireEntries([]string{"collision"}, 1); err != nil {
		t.Fatal(err)
	}
}

func TestListAndRequireEntriesAreExactAndBounded(t *testing.T) {
	t.Parallel()

	path, directory := openTestDirectory(t)
	entries, err := directory.List(0)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("empty entries = %v", entries)
	}

	mustWriteFile(t, filepath.Join(path, "b"), nil, 0o600)
	mustWriteFile(t, filepath.Join(path, "a"), nil, 0o600)
	entries, err = directory.List(2)
	if err != nil {
		t.Fatal(err)
	}
	if fmt.Sprint(entries) != "[a b]" {
		t.Fatalf("entries = %v, want [a b]", entries)
	}
	if _, err := directory.List(1); err == nil {
		t.Fatal("List accepted more than the configured maximum")
	}
	if _, err := directory.List(-1); err == nil {
		t.Fatal("List accepted a negative maximum")
	}
	if _, err := directory.List(math.MaxInt); err == nil {
		t.Fatal("List accepted an overflowing maximum")
	}
	if err := directory.RequireEntries([]string{"b", "a"}, 2); err != nil {
		t.Fatal(err)
	}
	if err := directory.RequireEntries([]string{"a"}, 2); err == nil {
		t.Fatal("RequireEntries accepted a missing expected entry")
	}
	if err := directory.RequireEntries([]string{"a", "b", "c"}, 3); err == nil {
		t.Fatal("RequireEntries accepted an extra expected entry")
	}
	if err := directory.RequireEntries([]string{"a", "a"}, 2); err == nil {
		t.Fatal("RequireEntries accepted duplicate expected names")
	}
	if err := directory.RequireEntries([]string{"a", "b"}, 1); err == nil {
		t.Fatal("RequireEntries accepted an expected set beyond the bound")
	}
	if err := directory.RequireEntries([]string{"../a"}, 1); err == nil {
		t.Fatal("RequireEntries accepted an invalid expected name")
	}

	mustWriteFile(t, filepath.Join(path, "bad name"), nil, 0o600)
	if _, err := directory.List(3); err == nil {
		t.Fatal("List accepted a noncanonical entry name")
	}
}

func TestRemoveEmptyDirectoryDoesNotTraverse(t *testing.T) {
	t.Parallel()

	path, directory := openTestDirectory(t)
	removeEmptyDirectory := func(name string) error {
		child, err := directory.openChildDirectory(name, false)
		if err != nil {
			return err
		}
		removeErr := directory.RemovePinnedEmptyDirectory(name, child)
		return errors.Join(removeErr, child.Close())
	}

	mustMkdir(t, filepath.Join(path, "empty-stage"), 0o700)
	if err := removeEmptyDirectory("empty-stage"); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(filepath.Join(path, "empty-stage")); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("removed stage stat error = %v", err)
	}

	mustMkdir(t, filepath.Join(path, "nonempty-stage"), 0o700)
	mustWriteFile(t, filepath.Join(path, "nonempty-stage", "member"), nil, 0o600)
	if err := removeEmptyDirectory("nonempty-stage"); err == nil {
		t.Fatal("RemovePinnedEmptyDirectory removed a nonempty stage")
	}

	mustMkdir(t, filepath.Join(path, "unsafe-stage"), 0o755)
	if err := removeEmptyDirectory("unsafe-stage"); err == nil {
		t.Fatal("openChildDirectory accepted mode 0755")
	}
	assertExactMode(t, filepath.Join(path, "unsafe-stage"), 0o755)

	outsideDirectory := t.TempDir()
	if err := os.Symlink(outsideDirectory, filepath.Join(path, "stage-link")); err != nil {
		t.Fatal(err)
	}
	if err := removeEmptyDirectory("stage-link"); err == nil {
		t.Fatal("openChildDirectory followed a symlink")
	}
	if _, err := os.Stat(outsideDirectory); err != nil {
		t.Fatalf("outside directory changed: %v", err)
	}

	mustWriteFile(t, filepath.Join(path, "regular"), nil, 0o600)
	if err := removeEmptyDirectory("regular"); err == nil {
		t.Fatal("openChildDirectory accepted a regular file")
	}
}

func TestPinnedDirectoryDetectsPathReplacement(t *testing.T) {
	t.Parallel()

	path, directory := openTestDirectory(t)
	movedPath := path + ".moved"
	if err := os.Rename(path, movedPath); err != nil {
		t.Fatal(err)
	}
	mustMkdir(t, path, 0o700)

	if err := directory.Revalidate(); err == nil {
		t.Fatal("Revalidate accepted a replacement directory")
	}
	if _, err := directory.List(0); err == nil {
		t.Fatal("List used a pinned directory after its path was replaced")
	}
	if _, _, err := directory.CreateTemporaryFile(
		sequenceGenerator("must-not-exist"),
	); err == nil {
		t.Fatal("CreateTemporaryFile used a replaced parent")
	}
	if err := directory.Sync(); err == nil {
		t.Fatal("Sync accepted a replacement directory")
	}
	assertDirectoryEntryCount(t, path, 0)
	assertDirectoryEntryCount(t, movedPath, 0)
}

func TestRenameNoReplaceIsAtomicUnderConcurrency(t *testing.T) {
	t.Parallel()

	path, directory := openTestDirectory(t)
	const contenders = 24
	sources := make([]string, contenders)
	for index := range contenders {
		name := fmt.Sprintf("source-%02d", index)
		sources[index] = name
		createdName, file, err := directory.CreateTemporaryFile(sequenceGenerator(name))
		if err != nil {
			t.Fatal(err)
		}
		if createdName != name {
			t.Fatalf("created name = %q, want %q", createdName, name)
		}
		if _, err := file.WriteString(name); err != nil {
			t.Fatal(err)
		}
		if err := SyncFile(file); err != nil {
			t.Fatal(err)
		}
		if err := file.Close(); err != nil {
			t.Fatal(err)
		}
	}

	type result struct {
		source string
		err    error
	}
	start := make(chan struct{})
	results := make(chan result, contenders)
	var group sync.WaitGroup
	for _, source := range sources {
		group.Go(func() {
			<-start
			results <- result{
				source: source,
				err:    directory.RenameNoReplace(source, directory, "winner"),
			}
		})
	}
	close(start)
	group.Wait()
	close(results)

	winner := ""
	for renameResult := range results {
		if renameResult.err == nil {
			if winner != "" {
				t.Fatalf("multiple renames succeeded: %q and %q", winner, renameResult.source)
			}
			winner = renameResult.source
			continue
		}
		if !errors.Is(renameResult.err, ErrDestinationExists) {
			t.Fatalf("rename %q error = %v", renameResult.source, renameResult.err)
		}
	}
	if winner == "" {
		t.Fatal("no concurrent rename succeeded")
	}

	destination, err := directory.OpenRegular("winner", FilePolicy{
		AllowedModes: []fs.FileMode{0o600},
		MinimumSize:  1,
		MaximumSize:  32,
	})
	if err != nil {
		t.Fatal(err)
	}
	contents, err := io.ReadAll(destination)
	if err != nil {
		t.Fatal(err)
	}
	if err := destination.Close(); err != nil {
		t.Fatal(err)
	}
	if string(contents) != winner {
		t.Fatalf("winner contents = %q, want %q", contents, winner)
	}
	if _, err := os.Lstat(filepath.Join(path, winner)); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("winning source still exists: %v", err)
	}
	for _, source := range sources {
		if source == winner {
			continue
		}
		if _, err := os.Lstat(filepath.Join(path, source)); err != nil {
			t.Fatalf("losing source %q is missing: %v", source, err)
		}
	}
}

func TestRenameNoReplaceFailureClassificationDrivesOutcomeAndPublicError(t *testing.T) {
	t.Parallel()

	for _, testCase := range []struct {
		name          string
		err           error
		wantClass     renameNoReplaceFailure
		wantPublicErr error
		wantUnchanged bool
	}{
		{
			name:          "destination exists",
			err:           unix.EEXIST,
			wantClass:     renameNoReplaceFailureDestinationExists,
			wantPublicErr: ErrDestinationExists,
			wantUnchanged: true,
		},
		{
			name:          "unsupported",
			err:           unix.ENOSYS,
			wantClass:     renameNoReplaceFailureUnsupported,
			wantPublicErr: ErrUnsupportedFilesystem,
			wantUnchanged: true,
		},
		{
			name:          "ambiguous syscall failure",
			err:           unix.EIO,
			wantClass:     renameNoReplaceFailureOther,
			wantPublicErr: unix.EIO,
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			_, directory := openTestDirectory(t)
			if got := classifyRenameNoReplaceFailure(testCase.err); got != testCase.wantClass {
				t.Fatalf("failure class = %d, want %d", got, testCase.wantClass)
			}
			if got := definitivelyUnchangedRenameError(testCase.err); got != testCase.wantUnchanged {
				t.Fatalf("definitively unchanged = %t, want %t", got, testCase.wantUnchanged)
			}
			if err := renameNoReplaceError(directory, "destination", testCase.err); !errors.Is(err, testCase.wantPublicErr) {
				t.Fatalf("public error = %v, want errors.Is(_, %v)", err, testCase.wantPublicErr)
			}
		})
	}
}

func TestRenameNoReplaceUnsupportedErrorNamesDirectoryAndFilesystem(t *testing.T) {
	t.Parallel()

	path, directory := openTestDirectory(t)
	err := renameNoReplaceError(directory, "destination", unix.EINVAL)
	unsupported, ok := errors.AsType[*UnsupportedFilesystemError](err)
	if !ok {
		t.Fatalf("unsupported rename error = %T (%v), want *UnsupportedFilesystemError", err, err)
	}
	if unsupported.Directory != path || unsupported.Operation != "no-replace rename" {
		t.Fatalf("unsupported rename error = %#v", unsupported)
	}
	if unsupported.Filesystem == "" {
		t.Fatal("unsupported rename error omitted the filesystem type")
	}
	if !errors.Is(err, ErrUnsupportedFilesystem) || !errors.Is(err, unix.EINVAL) {
		t.Fatalf("errors.Is chain broken for %v", err)
	}
	if strings.Contains(err.Error(), "platform") {
		t.Fatalf("error text blames the platform: %q", err.Error())
	}
	for _, want := range []string{
		"private filesystem operation is unsupported by the filesystem",
		"no-replace rename in " + path,
		unsupported.Filesystem,
		"invalid argument",
	} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("error %q lacks %q", err.Error(), want)
		}
	}
	guidance := unsupported.Guidance("retained-search directory")
	want := "retained-search directory " + path + " is on " + unsupported.Filesystem +
		", which does not support atomic no-replace rename; place it on a local filesystem (ext4, xfs, btrfs)"
	if guidance != want {
		t.Fatalf("guidance = %q, want %q", guidance, want)
	}
	nameless := &UnsupportedFilesystemError{Operation: "no-replace rename", Directory: path, Err: unix.EINVAL}
	if got := nameless.Guidance("backup destination"); !strings.Contains(got, "is on a filesystem that does not support atomic no-replace rename") {
		t.Fatalf("nameless guidance = %q", got)
	}
}

func TestProbeRenameNoReplaceSucceedsAndLeavesNoProbeFile(t *testing.T) {
	t.Parallel()

	_, directory := openTestDirectory(t)
	if err := directory.ProbeRenameNoReplace(RandomName(".probe-")); err != nil {
		t.Fatalf("ProbeRenameNoReplace() = %v", err)
	}
	if err := directory.RequireEntries(nil, 0); err != nil {
		t.Fatalf("probe left entries behind: %v", err)
	}
	if err := RequireRenameNoReplace(directory, "retained-search directory", RandomName(".probe-")); err != nil {
		t.Fatalf("RequireRenameNoReplace() = %v", err)
	}
	if err := directory.RequireEntries(nil, 0); err != nil {
		t.Fatalf("second probe left entries behind: %v", err)
	}
}

func TestProbeRenameNoReplaceUsesCallerNamesAndRequiresGenerator(t *testing.T) {
	t.Parallel()

	_, directory := openTestDirectory(t)
	if err := directory.ProbeRenameNoReplace(nil); err == nil {
		t.Fatal("ProbeRenameNoReplace accepted a nil generator")
	}
	if err := directory.RequireEntries(nil, 0); err != nil {
		t.Fatalf("nil-generator probe left entries behind: %v", err)
	}

	// A caller-supplied fixed sequence is exercised in order, so a probe file
	// orphaned by a crash carries a name the caller's own cleanup reclaims.
	var observed []string
	err := directory.probeRenameNoReplace(
		FixedNames(".stage-one", ".stage-two"),
		func(fromDirectory int, from string, toDirectory int, to string) error {
			observed = append(observed, from, to)
			return renameNoReplaceAt(fromDirectory, from, toDirectory, to)
		},
	)
	if err != nil {
		t.Fatalf("fixed-name probe = %v", err)
	}
	if len(observed) != 2 || observed[0] != ".stage-one" || observed[1] != ".stage-two" {
		t.Fatalf("probe names = %v, want [.stage-one .stage-two]", observed)
	}
	if err := directory.RequireEntries(nil, 0); err != nil {
		t.Fatalf("fixed-name probe left entries behind: %v", err)
	}

	// Exhausting the fixed names fails the probe without leaving the source.
	err = directory.probeRenameNoReplace(FixedNames(".only-one"), func(int, string, int, string) error {
		return unix.EEXIST
	})
	if err == nil || !strings.Contains(err.Error(), "fixed names are exhausted") {
		t.Fatalf("exhausted fixed-name probe = %v", err)
	}
	if err := directory.RequireEntries(nil, 0); err != nil {
		t.Fatalf("exhausted probe left entries behind: %v", err)
	}
}

func TestFixedNamesValidatesEveryComponent(t *testing.T) {
	t.Parallel()

	generator := FixedNames("valid-name", "bad/name")
	if name, err := generator(); err != nil || name != "valid-name" {
		t.Fatalf("first fixed name = %q, %v", name, err)
	}
	if _, err := generator(); err == nil {
		t.Fatal("invalid fixed component was accepted")
	}
	if _, err := generator(); err == nil {
		t.Fatal("exhausted generator returned a name")
	}
}

func TestProbeRenameNoReplaceReportsUnsupportedFilesystem(t *testing.T) {
	t.Parallel()

	for _, errno := range []unix.Errno{unix.EINVAL, unix.ENOSYS, unix.ENOTSUP} {
		t.Run(errno.Error(), func(t *testing.T) {
			t.Parallel()
			path, directory := openTestDirectory(t)
			err := directory.probeRenameNoReplace(RandomName(".probe-"), func(int, string, int, string) error {
				return errno
			})
			if !errors.Is(err, ErrUnsupportedFilesystem) || !errors.Is(err, errno) {
				t.Fatalf("probe error = %v, want unsupported filesystem wrapping %v", err, errno)
			}
			unsupported, ok := errors.AsType[*UnsupportedFilesystemError](err)
			if !ok || unsupported.Directory != path || unsupported.Filesystem == "" {
				t.Fatalf("probe error = %T %#v, want directory %q with a filesystem name", err, unsupported, path)
			}
			if entriesErr := directory.RequireEntries(nil, 0); entriesErr != nil {
				t.Fatalf("unsupported probe left entries behind: %v", entriesErr)
			}
		})
	}
}

func TestProbeRenameNoReplaceDistinguishesOtherFailures(t *testing.T) {
	t.Parallel()

	_, directory := openTestDirectory(t)
	err := directory.probeRenameNoReplace(RandomName(".probe-"), func(int, string, int, string) error {
		return unix.EIO
	})
	if err == nil || errors.Is(err, ErrUnsupportedFilesystem) || !errors.Is(err, unix.EIO) {
		t.Fatalf("probe error = %v, want EIO that is not an unsupported filesystem", err)
	}
	if entriesErr := directory.RequireEntries(nil, 0); entriesErr != nil {
		t.Fatalf("failed probe left entries behind: %v", entriesErr)
	}
}

func TestProbeRenameNoReplaceCleansUpAmbiguousOutcome(t *testing.T) {
	t.Parallel()

	_, directory := openTestDirectory(t)
	err := directory.probeRenameNoReplace(RandomName(".probe-"), func(fromDirectory int, from string, toDirectory int, to string) error {
		if err := renameNoReplaceAt(fromDirectory, from, toDirectory, to); err != nil {
			return err
		}
		return unix.EIO
	})
	if err == nil || !errors.Is(err, unix.EIO) {
		t.Fatalf("probe error = %v, want EIO", err)
	}
	if entriesErr := directory.RequireEntries(nil, 0); entriesErr != nil {
		t.Fatalf("committed-then-failed probe left entries behind: %v", entriesErr)
	}
}

func TestRequireRenameNoReplaceExplainsUnsupportedFilesystem(t *testing.T) {
	t.Parallel()

	path, directory := openTestDirectory(t)
	unsupported := &UnsupportedFilesystemError{
		Operation:  "no-replace rename",
		Directory:  path,
		Filesystem: "nfs",
		Err:        unix.EINVAL,
	}
	guidance := unsupported.Guidance("retained-search directory")
	want := "retained-search directory " + path +
		" is on nfs, which does not support atomic no-replace rename; place it on a local filesystem (ext4, xfs, btrfs)"
	if guidance != want {
		t.Fatalf("guidance = %q, want %q", guidance, want)
	}
	wrapped := fmt.Errorf("%s: %w", guidance, unsupported)
	if !errors.Is(wrapped, ErrUnsupportedFilesystem) {
		t.Fatalf("wrapped guidance lost the sentinel: %v", wrapped)
	}
	if _, ok := errors.AsType[*UnsupportedFilesystemError](wrapped); !ok {
		t.Fatalf("wrapped guidance lost the typed error: %v", wrapped)
	}
	if err := RequireRenameNoReplace(directory, "retained-search directory", RandomName(".probe-")); err != nil {
		t.Fatalf("RequireRenameNoReplace on a local directory = %v", err)
	}
}

func TestDescribeFilesystemReportsLocalTemporaryDirectory(t *testing.T) {
	t.Parallel()

	path, directory := openTestDirectory(t)
	byPath, err := DescribeFilesystem(path)
	if err != nil {
		t.Fatalf("DescribeFilesystem(%q) = %v", path, err)
	}
	byDescriptor, err := directory.Filesystem()
	if err != nil {
		t.Fatalf("Directory.Filesystem() = %v", err)
	}
	if byPath != byDescriptor {
		t.Fatalf("path filesystem %#v != descriptor filesystem %#v", byPath, byDescriptor)
	}
	if byPath.Name == "" || byPath.Remote {
		t.Fatalf("temporary directory filesystem = %#v, want a named local filesystem", byPath)
	}
	if _, err := DescribeFilesystem(filepath.Join(path, "missing")); err == nil {
		t.Fatal("DescribeFilesystem accepted a missing path")
	}
	if err := directory.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := directory.Filesystem(); !errors.Is(err, ErrClosed) {
		t.Fatalf("closed Directory.Filesystem() = %v, want ErrClosed", err)
	}
}

func TestRenameNoReplacePreservesEveryExistingDestinationKind(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		setup func(*testing.T, string)
	}{
		{
			name: "regular",
			setup: func(t *testing.T, path string) {
				t.Helper()
				mustWriteFile(t, path, []byte("existing"), 0o600)
			},
		},
		{
			name: "directory",
			setup: func(t *testing.T, path string) {
				t.Helper()
				mustMkdir(t, path, 0o700)
			},
		},
		{
			name: "symlink",
			setup: func(t *testing.T, path string) {
				t.Helper()
				if err := os.Symlink("source", path); err != nil {
					t.Fatal(err)
				}
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			path, directory := openTestDirectory(t)
			mustWriteFile(t, filepath.Join(path, "source"), []byte("source"), 0o600)
			destinationPath := filepath.Join(path, "destination")
			test.setup(t, destinationPath)
			before, err := os.Lstat(destinationPath)
			if err != nil {
				t.Fatal(err)
			}
			err = directory.RenameNoReplace("source", directory, "destination")
			if !errors.Is(err, ErrDestinationExists) {
				t.Fatalf("RenameNoReplace error = %v", err)
			}
			after, err := os.Lstat(destinationPath)
			if err != nil {
				t.Fatal(err)
			}
			if !os.SameFile(before, after) {
				t.Fatal("existing destination identity changed")
			}
			contents, err := os.ReadFile(filepath.Join(path, "source"))
			if err != nil {
				t.Fatal(err)
			}
			if string(contents) != "source" {
				t.Fatalf("source contents = %q", contents)
			}
		})
	}
}

func TestRenameNoReplaceAcrossPinnedDirectories(t *testing.T) {
	t.Parallel()

	sourcePath, source := openTestDirectory(t)
	destinationPath, destination := openTestDirectory(t)
	mustWriteFile(t, filepath.Join(sourcePath, "source"), []byte("payload"), 0o600)
	if err := source.RenameNoReplace("source", destination, "destination"); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(filepath.Join(sourcePath, "source")); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("source stat error = %v", err)
	}
	contents, err := os.ReadFile(filepath.Join(destinationPath, "destination"))
	if err != nil {
		t.Fatal(err)
	}
	if string(contents) != "payload" {
		t.Fatalf("destination contents = %q", contents)
	}
}

func TestRenameNoReplaceWithStatusReportsCompletedRenameBeforeStabilityFailure(
	t *testing.T,
) {
	t.Parallel()

	path, directory := openTestDirectory(t)
	source := createPinnedSourceDirectory(t, path)
	movedPath := path + ".moved"
	outcome, err := directory.renameNoReplaceWithStatus(
		"source",
		source,
		directory,
		"destination",
		func(fromDirectory int, from string, toDirectory int, to string) error {
			if err := renameNoReplaceAt(fromDirectory, from, toDirectory, to); err != nil {
				return err
			}
			if err := os.Rename(path, movedPath); err != nil {
				return err
			}
			return os.Mkdir(path, 0o700)
		},
	)
	if outcome != RenameNoReplaceCompleted {
		t.Fatalf("completed no-replace rename outcome = %v", outcome)
	}
	if err == nil || !strings.Contains(err.Error(), "directory stability") {
		t.Fatalf("RenameNoReplaceWithStatus error = %v, want stability failure", err)
	}
	contents, readErr := os.ReadFile(filepath.Join(movedPath, "destination", "payload"))
	if readErr != nil {
		t.Fatalf("read renamed destination: %v", readErr)
	}
	if string(contents) != "payload" {
		t.Fatalf("renamed destination contents = %q", contents)
	}
}

func TestRenameNoReplaceWithStatusRecognizesErrorAfterCommit(t *testing.T) {
	t.Parallel()

	path, directory := openTestDirectory(t)
	source := createPinnedSourceDirectory(t, path)
	outcome, err := directory.renameNoReplaceWithStatus(
		"source",
		source,
		directory,
		"destination",
		func(fromDirectory int, from string, toDirectory int, to string) error {
			if err := renameNoReplaceAt(fromDirectory, from, toDirectory, to); err != nil {
				return err
			}
			return unix.EIO
		},
	)
	if outcome != RenameNoReplaceCompleted {
		t.Fatalf("error-after-commit outcome = %v, want completed", outcome)
	}
	if !errors.Is(err, unix.EIO) {
		t.Fatalf("error-after-commit error = %v, want EIO", err)
	}
	contents, readErr := os.ReadFile(filepath.Join(path, "destination", "payload"))
	if readErr != nil {
		t.Fatalf("read committed destination: %v", readErr)
	}
	if string(contents) != "payload" {
		t.Fatalf("committed destination contents = %q", contents)
	}
}

func TestRenameNoReplaceWithStatusPreservesAmbiguousMutation(t *testing.T) {
	t.Parallel()

	path, directory := openTestDirectory(t)
	source := createPinnedSourceDirectory(t, path)
	outcome, err := directory.renameNoReplaceWithStatus(
		"source",
		source,
		directory,
		"destination",
		func(fromDirectory int, from string, _ int, _ string) error {
			if err := renameNoReplaceAt(fromDirectory, from, fromDirectory, "third"); err != nil {
				return err
			}
			return unix.EIO
		},
	)
	if outcome != RenameNoReplaceAmbiguous {
		t.Fatalf("unresolved mutation outcome = %v, want ambiguous", outcome)
	}
	if !errors.Is(err, unix.EIO) {
		t.Fatalf("ambiguous mutation error = %v, want EIO", err)
	}
	contents, readErr := os.ReadFile(filepath.Join(path, "third", "payload"))
	if readErr != nil {
		t.Fatalf("read ambiguously moved source: %v", readErr)
	}
	if string(contents) != "payload" {
		t.Fatalf("ambiguously moved contents = %q", contents)
	}
}

func TestRenameNoReplaceWithStatusBindsPinnedStageBeforeSyscall(t *testing.T) {
	t.Parallel()

	path, directory := openTestDirectory(t)
	source := createPinnedSourceDirectory(t, path)
	if err := directory.RenameNoReplace("source", directory, "destination"); err != nil {
		t.Fatal(err)
	}
	mustMkdir(t, filepath.Join(path, "source"), 0o700)
	operationCalled := false
	outcome, err := directory.renameNoReplaceWithStatus(
		"source",
		source,
		directory,
		"destination",
		func(_ int, _ string, _ int, _ string) error {
			operationCalled = true
			return nil
		},
	)
	if operationCalled {
		t.Fatal("rename syscall ran after the stage source name was replaced")
	}
	if outcome != RenameNoReplaceCompleted {
		t.Fatalf("displaced stage outcome = %v, want completed", outcome)
	}
	if err == nil || !strings.Contains(err.Error(), "pinned stage") {
		t.Fatalf("displaced stage error = %v", err)
	}
	contents, readErr := os.ReadFile(filepath.Join(path, "destination", "payload"))
	if readErr != nil {
		t.Fatalf("read displaced published stage: %v", readErr)
	}
	if string(contents) != "payload" {
		t.Fatalf("displaced published stage contents = %q", contents)
	}
}

func TestRenameNoReplaceWithStatusPreservesStageAfterParentStabilityFailure(
	t *testing.T,
) {
	t.Parallel()

	path, directory := openTestDirectory(t)
	source := createPinnedSourceDirectory(t, path)
	if err := directory.RenameNoReplace("source", directory, "destination"); err != nil {
		t.Fatal(err)
	}
	movedPath := path + ".moved"
	if err := os.Rename(path, movedPath); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(path, 0o700); err != nil {
		t.Fatal(err)
	}
	operationCalled := false
	outcome, err := directory.renameNoReplaceWithStatus(
		"source",
		source,
		directory,
		"destination",
		func(_ int, _ string, _ int, _ string) error {
			operationCalled = true
			return nil
		},
	)
	if operationCalled {
		t.Fatal("rename syscall ran after parent stability failed")
	}
	if outcome != RenameNoReplaceAmbiguous {
		t.Fatalf("unstable-parent outcome = %v, want ambiguous", outcome)
	}
	if err == nil || !strings.Contains(err.Error(), "revalidate private directory") {
		t.Fatalf("unstable-parent error = %v", err)
	}
	contents, readErr := os.ReadFile(filepath.Join(movedPath, "destination", "payload"))
	if readErr != nil {
		t.Fatalf("read publication after parent stability failure: %v", readErr)
	}
	if string(contents) != "payload" {
		t.Fatalf("publication after parent stability failure = %q", contents)
	}
}

func TestClosedDirectoryFailsClosed(t *testing.T) {
	t.Parallel()

	var nilDirectory *Directory
	if nilDirectory.Path() != "" {
		t.Fatalf("nil Path() = %q", nilDirectory.Path())
	}
	if err := nilDirectory.Close(); err != nil {
		t.Fatal(err)
	}
	if err := nilDirectory.Revalidate(); !errors.Is(err, ErrClosed) {
		t.Fatalf("nil Revalidate() = %v", err)
	}

	_, directory := openTestDirectory(t)
	if err := directory.Close(); err != nil {
		t.Fatal(err)
	}
	if err := directory.Revalidate(); !errors.Is(err, ErrClosed) {
		t.Fatalf("closed Revalidate() = %v", err)
	}
	if _, err := directory.List(0); !errors.Is(err, ErrClosed) {
		t.Fatalf("closed List() = %v", err)
	}
	if _, _, err := directory.CreateTemporaryFile(
		sequenceGenerator("stage"),
	); !errors.Is(err, ErrClosed) {
		t.Fatalf("closed CreateTemporaryFile() = %v", err)
	}
}

func openTestDirectory(t *testing.T) (string, *Directory) {
	t.Helper()
	path := t.TempDir()
	if err := os.Chmod(path, 0o700); err != nil {
		t.Fatal(err)
	}
	directory, err := OpenDirectory(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := directory.Close(); err != nil {
			t.Errorf("close private directory: %v", err)
		}
	})
	return path, directory
}

func createPinnedSourceDirectory(t *testing.T, parent string) *Directory {
	t.Helper()
	path := filepath.Join(parent, "source")
	mustMkdir(t, path, 0o700)
	mustWriteFile(t, filepath.Join(path, "payload"), []byte("payload"), 0o600)
	directory, err := OpenDirectory(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := directory.Close(); err != nil {
			t.Errorf("close pinned source directory: %v", err)
		}
	})
	return directory
}

func sequenceGenerator(names ...string) NameGenerator {
	index := 0
	return func() (string, error) {
		if index >= len(names) {
			return "", errors.New("temporary name sequence exhausted")
		}
		name := names[index]
		index++
		return name, nil
	}
}

func mustMkdir(t *testing.T, path string, mode fs.FileMode) {
	t.Helper()
	if err := os.Mkdir(path, mode); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, mode); err != nil {
		t.Fatal(err)
	}
}

func mustWriteFile(t *testing.T, path string, contents []byte, mode fs.FileMode) {
	t.Helper()
	if err := os.WriteFile(path, contents, mode); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, mode); err != nil {
		t.Fatal(err)
	}
}

func assertExactMode(t *testing.T, path string, want fs.FileMode) {
	t.Helper()
	info, err := os.Lstat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != want || hasSpecialMode(info.Mode()) {
		t.Fatalf("mode for %q = %v, want %#o with no special bits", path, info.Mode(), want)
	}
}

func assertDirectoryEntryCount(t *testing.T, path string, want int) {
	t.Helper()
	entries, err := os.ReadDir(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != want {
		t.Fatalf("entry count for %q = %d, want %d", path, len(entries), want)
	}
}
