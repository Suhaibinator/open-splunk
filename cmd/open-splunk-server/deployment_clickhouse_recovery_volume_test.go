package main

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestRunDeploymentSubcommandRecognizesPrepareClickHouseRecoveryVolume(t *testing.T) {
	t.Parallel()

	handled, err := runDeploymentSubcommand([]string{
		"prepare-clickhouse-recovery-volume",
		"-unknown",
	})
	if !handled || err == nil {
		t.Fatalf("prepare recovery volume dispatch = (%t, %v), want (true, error)", handled, err)
	}
}

func TestPrepareClickHouseRecoveryVolumeFlagContract(t *testing.T) {
	t.Parallel()

	harness := newFakeClickHouseRecoveryVolumeHarness(
		fakeClickHouseRecoveryVolumeReadyMetadata(),
	)
	if err := runPrepareClickHouseRecoveryVolumeSubcommandWithDependencies(
		[]string{"-path", "/recovery-volume"},
		harness.dependencies(),
		prepareClickHouseRecoveryVolumeHooks{},
	); err != nil {
		t.Fatal(err)
	}

	for name, arguments := range map[string][]string{
		"missing":         nil,
		"relative":        {"-path", "recovery-volume"},
		"filesystem root": {"-path", string(filepath.Separator)},
		"unclean":         {"-path", "/recovery/../recovery-volume"},
		"duplicate":       {"-path", "/recovery-volume", "-path", "/other"},
		"unknown":         {"-unknown"},
		"positional":      {"-path", "/recovery-volume", "unexpected"},
	} {
		name := name
		arguments := arguments
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			local := newFakeClickHouseRecoveryVolumeHarness(
				fakeClickHouseRecoveryVolumeReadyMetadata(),
			)
			if err := runPrepareClickHouseRecoveryVolumeSubcommandWithDependencies(
				arguments,
				local.dependencies(),
				prepareClickHouseRecoveryVolumeHooks{},
			); err == nil {
				t.Fatalf("arguments %#v succeeded", arguments)
			}
			if len(local.calls) != 0 {
				t.Fatalf("invalid arguments reached filesystem dependencies: %#v", local.calls)
			}
		})
	}
}

func TestPrepareClickHouseRecoveryVolumeRequiresRootBeforeInspection(t *testing.T) {
	t.Parallel()

	harness := newFakeClickHouseRecoveryVolumeHarness(
		fakeClickHouseRecoveryVolumeFreshMetadata(),
	)
	harness.effectiveUID = 65532
	err := prepareClickHouseRecoveryVolumeWithDependencies(
		"/recovery-volume",
		harness.dependencies(),
		prepareClickHouseRecoveryVolumeHooks{},
	)
	if err == nil || !strings.Contains(err.Error(), "must be root") {
		t.Fatalf("non-root error = %v", err)
	}
	if len(harness.calls) != 0 {
		t.Fatalf("non-root invocation inspected filesystem: %#v", harness.calls)
	}
}

func TestPrepareClickHouseRecoveryVolumeInitializesFreshEmptyRoot(t *testing.T) {
	t.Parallel()

	harness := newFakeClickHouseRecoveryVolumeHarness(
		fakeClickHouseRecoveryVolumeFreshMetadata(),
	)
	err := prepareClickHouseRecoveryVolumeWithDependencies(
		"/recovery-volume",
		harness.dependencies(),
		prepareClickHouseRecoveryVolumeHooks{},
	)
	if err != nil {
		t.Fatal(err)
	}
	if !harness.descriptor.matches(fakeClickHouseRecoveryVolumeReadyMetadata()) {
		t.Fatalf("prepared metadata = %+v", *harness.descriptor)
	}
	if got, want := harness.directory.chmodMode, clickHouseRecoveryVolumeReadyMode; got != want {
		t.Fatalf("descriptor chmod mode = %#o, want %#o", got, want)
	}
	assertFakeRecoveryVolumeCallOrder(
		t,
		harness.calls,
		"readdirnames",
		fmt.Sprintf("chown:%d:%d", clickHouseRecoveryVolumeUID, clickHouseRecoveryVolumeGID),
		"chmod",
		"close",
	)
}

func TestPrepareClickHouseRecoveryVolumeResumesInterruptedChownAcrossInvocations(
	t *testing.T,
) {
	t.Parallel()

	interrupted := errors.New("interrupted before chmod")
	first := newFakeClickHouseRecoveryVolumeHarness(
		fakeClickHouseRecoveryVolumeFreshMetadata(),
	)
	afterChownCalls := 0
	err := prepareClickHouseRecoveryVolumeWithDependencies(
		"/recovery-volume",
		first.dependencies(),
		prepareClickHouseRecoveryVolumeHooks{
			afterChown: func() {
				afterChownCalls++
				first.directory.chmodErr = interrupted
			},
		},
	)
	if !errors.Is(err, interrupted) {
		t.Fatalf("interrupted preparation error = %v, want injected interruption", err)
	}
	if !first.descriptor.matches(fakeClickHouseRecoveryVolumeChownedMetadata()) {
		t.Fatalf("interrupted metadata = %+v", *first.descriptor)
	}
	if first.directory.chownCalls != 1 || first.directory.chmodCalls != 1 {
		t.Fatalf("first invocation mutations = chown:%d chmod:%d, want 1 each",
			first.directory.chownCalls,
			first.directory.chmodCalls,
		)
	}

	second := newFakeClickHouseRecoveryVolumeHarness(*first.descriptor)
	second.directory.chownErr = errors.New("resumed preparation repeated chown")
	if err := prepareClickHouseRecoveryVolumeWithDependencies(
		"/recovery-volume",
		second.dependencies(),
		prepareClickHouseRecoveryVolumeHooks{
			afterChown: func() { afterChownCalls++ },
		},
	); err != nil {
		t.Fatalf("resume interrupted preparation: %v", err)
	}
	if !second.descriptor.matches(fakeClickHouseRecoveryVolumeReadyMetadata()) {
		t.Fatalf("resumed metadata = %+v", *second.descriptor)
	}
	if second.directory.chownCalls != 0 || second.directory.chmodCalls != 1 {
		t.Fatalf("resumed invocation mutations = chown:%d chmod:%d, want 0 and 1",
			second.directory.chownCalls,
			second.directory.chmodCalls,
		)
	}
	if afterChownCalls != 1 {
		t.Fatalf("after-chown hook calls = %d, want only the first invocation", afterChownCalls)
	}
	assertFakeRecoveryVolumeCallOrder(
		t,
		second.calls,
		"readdirnames",
		"chmod",
		"close",
	)
}

func TestPrepareClickHouseRecoveryVolumeAcceptsPreparedNonemptyRootIdempotently(
	t *testing.T,
) {
	t.Parallel()

	harness := newFakeClickHouseRecoveryVolumeHarness(
		fakeClickHouseRecoveryVolumeReadyMetadata(),
	)
	harness.directory.entries = []string{"retained-clickhouse-backup.zip"}
	if err := prepareClickHouseRecoveryVolumeWithDependencies(
		"/recovery-volume",
		harness.dependencies(),
		prepareClickHouseRecoveryVolumeHooks{},
	); err != nil {
		t.Fatal(err)
	}
	for _, call := range harness.calls {
		if call == "readdirnames" || call == "chmod" || strings.HasPrefix(call, "chown:") {
			t.Fatalf("idempotent prepared volume performed mutation/emptiness call %q", call)
		}
	}
	if harness.calls[len(harness.calls)-1] != "close" {
		t.Fatalf("prepared volume calls = %#v, want descriptor close", harness.calls)
	}
}

func TestPrepareClickHouseRecoveryVolumeRejectsEveryOtherInitialState(t *testing.T) {
	t.Parallel()

	tests := map[string]fakeClickHouseRecoveryVolumeMetadata{
		"regular file": {
			mode:  0o755,
			uid:   0,
			gid:   0,
			inode: 1,
		},
		"symlink": {
			mode:  os.ModeSymlink | 0o755,
			uid:   0,
			gid:   0,
			inode: 1,
		},
		"ownership unavailable": {
			mode:           os.ModeDir | 0o755,
			uid:            0,
			gid:            0,
			inode:          1,
			sysUnavailable: true,
		},
		"fresh wrong owner": {
			mode: os.ModeDir | 0o755, uid: 1, gid: 0, inode: 1,
		},
		"fresh wrong group": {
			mode: os.ModeDir | 0o755, uid: 0, gid: 1, inode: 1,
		},
		"fresh wrong mode": {
			mode: os.ModeDir | 0o700, uid: 0, gid: 0, inode: 1,
		},
		"fresh special bit": {
			mode: os.ModeDir | os.ModeSticky | 0o755, uid: 0, gid: 0, inode: 1,
		},
		"interrupted permissive mode": {
			mode: os.ModeDir | 0o775,
			uid:  clickHouseRecoveryVolumeUID, gid: clickHouseRecoveryVolumeGID, inode: 1,
		},
		"interrupted wrong owner": {
			mode: os.ModeDir | 0o755,
			uid:  102, gid: clickHouseRecoveryVolumeGID, inode: 1,
		},
		"interrupted wrong group": {
			mode: os.ModeDir | 0o755,
			uid:  clickHouseRecoveryVolumeUID, gid: 65531, inode: 1,
		},
		"interrupted special bit": {
			mode: os.ModeDir | os.ModeSticky | 0o755,
			uid:  clickHouseRecoveryVolumeUID, gid: clickHouseRecoveryVolumeGID, inode: 1,
		},
		"prepared missing setgid": {
			mode: os.ModeDir | 0o750,
			uid:  clickHouseRecoveryVolumeUID, gid: clickHouseRecoveryVolumeGID, inode: 1,
		},
		"prepared wrong owner": {
			mode: os.ModeDir | clickHouseRecoveryVolumeReadyMode,
			uid:  102, gid: clickHouseRecoveryVolumeGID, inode: 1,
		},
		"prepared wrong group": {
			mode: os.ModeDir | clickHouseRecoveryVolumeReadyMode,
			uid:  clickHouseRecoveryVolumeUID, gid: 65531, inode: 1,
		},
		"prepared extra sticky": {
			mode: os.ModeDir | clickHouseRecoveryVolumeReadyMode | os.ModeSticky,
			uid:  clickHouseRecoveryVolumeUID, gid: clickHouseRecoveryVolumeGID, inode: 1,
		},
	}
	for name, metadata := range tests {
		name := name
		metadata := metadata
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			harness := newFakeClickHouseRecoveryVolumeHarness(metadata)
			if err := prepareClickHouseRecoveryVolumeWithDependencies(
				"/recovery-volume",
				harness.dependencies(),
				prepareClickHouseRecoveryVolumeHooks{},
			); err == nil {
				t.Fatal("invalid initial state succeeded")
			}
			if len(harness.calls) != 1 || harness.calls[0] != "lstat" {
				t.Fatalf("invalid state calls = %#v, want lstat only", harness.calls)
			}
		})
	}
}

func TestPrepareClickHouseRecoveryVolumeRejectsNonemptyFreshRoot(t *testing.T) {
	t.Parallel()

	harness := newFakeClickHouseRecoveryVolumeHarness(
		fakeClickHouseRecoveryVolumeFreshMetadata(),
	)
	harness.directory.entries = []string{"unexpected"}
	err := prepareClickHouseRecoveryVolumeWithDependencies(
		"/recovery-volume",
		harness.dependencies(),
		prepareClickHouseRecoveryVolumeHooks{},
	)
	if err == nil || !strings.Contains(err.Error(), "must be empty") {
		t.Fatalf("nonempty fresh-root error = %v", err)
	}
	if harness.directory.chownCalls != 0 || harness.directory.chmodCalls != 0 {
		t.Fatalf("nonempty root was mutated: %#v", harness.calls)
	}
}

func TestPrepareClickHouseRecoveryVolumeRejectsNonemptyInterruptedRoot(t *testing.T) {
	t.Parallel()

	harness := newFakeClickHouseRecoveryVolumeHarness(
		fakeClickHouseRecoveryVolumeChownedMetadata(),
	)
	harness.directory.entries = []string{"unexpected"}
	err := prepareClickHouseRecoveryVolumeWithDependencies(
		"/recovery-volume",
		harness.dependencies(),
		prepareClickHouseRecoveryVolumeHooks{},
	)
	if err == nil || !strings.Contains(err.Error(), "must be empty") {
		t.Fatalf("nonempty interrupted-root error = %v", err)
	}
	if harness.directory.chownCalls != 0 || harness.directory.chmodCalls != 0 {
		t.Fatalf("nonempty interrupted root was mutated: %#v", harness.calls)
	}
}

func TestPrepareClickHouseRecoveryVolumeRejectsUnsafeOrUnavailableACL(t *testing.T) {
	t.Parallel()

	for name, metadata := range map[string]fakeClickHouseRecoveryVolumeMetadata{
		"fresh":       fakeClickHouseRecoveryVolumeFreshMetadata(),
		"interrupted": fakeClickHouseRecoveryVolumeChownedMetadata(),
	} {
		name := name
		metadata := metadata
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			harness := newFakeClickHouseRecoveryVolumeHarness(metadata)
			harness.aclErrors[1] = errors.New("extended ACL")
			err := prepareClickHouseRecoveryVolumeWithDependencies(
				"/recovery-volume",
				harness.dependencies(),
				prepareClickHouseRecoveryVolumeHooks{},
			)
			if err == nil || !strings.Contains(err.Error(), "ACL") {
				t.Fatalf("ACL error = %v", err)
			}
			if harness.directory.chownCalls != 0 || harness.directory.chmodCalls != 0 {
				t.Fatalf("ACL-bearing root was mutated: %#v", harness.calls)
			}
		})
	}
}

func TestPrepareClickHouseRecoveryVolumeRevalidatesInterruptedRootAfterEmptiness(
	t *testing.T,
) {
	t.Parallel()

	harness := newFakeClickHouseRecoveryVolumeHarness(
		fakeClickHouseRecoveryVolumeChownedMetadata(),
	)
	err := prepareClickHouseRecoveryVolumeWithDependencies(
		"/recovery-volume",
		harness.dependencies(),
		prepareClickHouseRecoveryVolumeHooks{
			afterEmptinessCheck: harness.swapPath,
		},
	)
	if err == nil || !strings.Contains(err.Error(), "pinned volume root") {
		t.Fatalf("interrupted-root path-swap error = %v", err)
	}
	if harness.directory.chownCalls != 0 || harness.directory.chmodCalls != 0 {
		t.Fatalf("swapped interrupted root was mutated: %#v", harness.calls)
	}
}

func TestPrepareClickHouseRecoveryVolumeRejectsPathSwapsAtEveryBoundary(
	t *testing.T,
) {
	t.Parallel()

	tests := map[string]func(*fakeClickHouseRecoveryVolumeHarness) prepareClickHouseRecoveryVolumeHooks{
		"after open": func(harness *fakeClickHouseRecoveryVolumeHarness) prepareClickHouseRecoveryVolumeHooks {
			return prepareClickHouseRecoveryVolumeHooks{afterOpen: harness.swapPath}
		},
		"after emptiness check": func(harness *fakeClickHouseRecoveryVolumeHarness) prepareClickHouseRecoveryVolumeHooks {
			return prepareClickHouseRecoveryVolumeHooks{afterEmptinessCheck: harness.swapPath}
		},
		"after chown": func(harness *fakeClickHouseRecoveryVolumeHarness) prepareClickHouseRecoveryVolumeHooks {
			return prepareClickHouseRecoveryVolumeHooks{afterChown: harness.swapPath}
		},
		"after chmod": func(harness *fakeClickHouseRecoveryVolumeHarness) prepareClickHouseRecoveryVolumeHooks {
			return prepareClickHouseRecoveryVolumeHooks{afterChmod: harness.swapPath}
		},
	}
	for name, makeHooks := range tests {
		name := name
		makeHooks := makeHooks
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			harness := newFakeClickHouseRecoveryVolumeHarness(
				fakeClickHouseRecoveryVolumeFreshMetadata(),
			)
			err := prepareClickHouseRecoveryVolumeWithDependencies(
				"/recovery-volume",
				harness.dependencies(),
				makeHooks(harness),
			)
			if err == nil || !strings.Contains(err.Error(), "pinned volume root") {
				t.Fatalf("path-swap error = %v", err)
			}
		})
	}
}

func TestPrepareClickHouseRecoveryVolumeRejectsDifferentOpenedIdentity(t *testing.T) {
	t.Parallel()

	harness := newFakeClickHouseRecoveryVolumeHarness(
		fakeClickHouseRecoveryVolumeFreshMetadata(),
	)
	replacement := *harness.descriptor
	replacement.inode++
	harness.descriptor = &replacement
	harness.directory.metadata = harness.descriptor
	err := prepareClickHouseRecoveryVolumeWithDependencies(
		"/recovery-volume",
		harness.dependencies(),
		prepareClickHouseRecoveryVolumeHooks{},
	)
	if err == nil || !strings.Contains(err.Error(), "identity changed") {
		t.Fatalf("different opened identity error = %v", err)
	}
	if harness.directory.chownCalls != 0 || harness.directory.chmodCalls != 0 {
		t.Fatalf("replacement descriptor was mutated: %#v", harness.calls)
	}
}

func TestPrepareClickHouseRecoveryVolumePropagatesDescriptorFailures(t *testing.T) {
	t.Parallel()

	injected := errors.New("injected failure")
	tests := map[string]func(*fakeClickHouseRecoveryVolumeHarness){
		"lstat": func(harness *fakeClickHouseRecoveryVolumeHarness) {
			harness.lstatErrors[1] = injected
		},
		"open": func(harness *fakeClickHouseRecoveryVolumeHarness) {
			harness.openErr = injected
		},
		"stat": func(harness *fakeClickHouseRecoveryVolumeHarness) {
			harness.directory.statErrors[1] = injected
		},
		"readdir": func(harness *fakeClickHouseRecoveryVolumeHarness) {
			harness.directory.readdirErr = injected
		},
		"chown": func(harness *fakeClickHouseRecoveryVolumeHarness) {
			harness.directory.chownErr = injected
		},
		"chmod": func(harness *fakeClickHouseRecoveryVolumeHarness) {
			harness.directory.chmodErr = injected
		},
		"close": func(harness *fakeClickHouseRecoveryVolumeHarness) {
			harness.directory.closeErr = injected
		},
	}
	for name, inject := range tests {
		name := name
		inject := inject
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			harness := newFakeClickHouseRecoveryVolumeHarness(
				fakeClickHouseRecoveryVolumeFreshMetadata(),
			)
			inject(harness)
			err := prepareClickHouseRecoveryVolumeWithDependencies(
				"/recovery-volume",
				harness.dependencies(),
				prepareClickHouseRecoveryVolumeHooks{},
			)
			if !errors.Is(err, injected) {
				t.Fatalf("failure error = %v, want injected error", err)
			}
		})
	}
}

func TestOpenClickHouseRecoveryVolumeDirectoryDoesNotFollowFinalSymlink(
	t *testing.T,
) {
	t.Parallel()

	root := t.TempDir()
	target := filepath.Join(root, "target")
	if err := os.Mkdir(target, 0o755); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(root, "link")
	if err := os.Symlink(target, link); err != nil {
		t.Fatal(err)
	}
	if directory, err := openClickHouseRecoveryVolumeDirectory(link); err == nil {
		_ = directory.Close()
		t.Fatal("opened symlinked recovery volume root")
	}
	regular := filepath.Join(root, "file")
	if err := os.WriteFile(regular, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if directory, err := openClickHouseRecoveryVolumeDirectory(regular); err == nil {
		_ = directory.Close()
		t.Fatal("opened regular file as recovery volume root")
	}
}

type fakeClickHouseRecoveryVolumeMetadata struct {
	mode           os.FileMode
	uid            uint32
	gid            uint32
	inode          uint64
	sysUnavailable bool
}

func fakeClickHouseRecoveryVolumeFreshMetadata() fakeClickHouseRecoveryVolumeMetadata {
	return fakeClickHouseRecoveryVolumeMetadata{
		mode:  os.ModeDir | 0o755,
		uid:   0,
		gid:   0,
		inode: 1,
	}
}

func fakeClickHouseRecoveryVolumeChownedMetadata() fakeClickHouseRecoveryVolumeMetadata {
	return fakeClickHouseRecoveryVolumeMetadata{
		mode:  os.ModeDir | 0o755,
		uid:   clickHouseRecoveryVolumeUID,
		gid:   clickHouseRecoveryVolumeGID,
		inode: 1,
	}
}

func fakeClickHouseRecoveryVolumeReadyMetadata() fakeClickHouseRecoveryVolumeMetadata {
	return fakeClickHouseRecoveryVolumeMetadata{
		mode:  os.ModeDir | clickHouseRecoveryVolumeReadyMode,
		uid:   clickHouseRecoveryVolumeUID,
		gid:   clickHouseRecoveryVolumeGID,
		inode: 1,
	}
}

func (metadata fakeClickHouseRecoveryVolumeMetadata) matches(
	want fakeClickHouseRecoveryVolumeMetadata,
) bool {
	return metadata.mode == want.mode && metadata.uid == want.uid &&
		metadata.gid == want.gid && metadata.inode == want.inode &&
		metadata.sysUnavailable == want.sysUnavailable
}

type fakeClickHouseRecoveryVolumeInfo struct {
	metadata fakeClickHouseRecoveryVolumeMetadata
}

func (info fakeClickHouseRecoveryVolumeInfo) Name() string       { return "recovery-volume" }
func (info fakeClickHouseRecoveryVolumeInfo) Size() int64        { return 0 }
func (info fakeClickHouseRecoveryVolumeInfo) Mode() os.FileMode  { return info.metadata.mode }
func (info fakeClickHouseRecoveryVolumeInfo) ModTime() time.Time { return time.Unix(0, 0) }
func (info fakeClickHouseRecoveryVolumeInfo) IsDir() bool        { return info.metadata.mode.IsDir() }
func (info fakeClickHouseRecoveryVolumeInfo) Sys() any {
	if info.metadata.sysUnavailable {
		return nil
	}
	return &syscall.Stat_t{Uid: info.metadata.uid, Gid: info.metadata.gid}
}

type fakeClickHouseRecoveryVolumeDirectory struct {
	metadata   *fakeClickHouseRecoveryVolumeMetadata
	calls      *[]string
	entries    []string
	statErrors map[int]error
	statCalls  int
	readdirErr error
	chownErr   error
	chmodErr   error
	closeErr   error
	chownCalls int
	chmodCalls int
	chmodMode  os.FileMode
	closed     bool
}

func (directory *fakeClickHouseRecoveryVolumeDirectory) Stat() (os.FileInfo, error) {
	directory.statCalls++
	*directory.calls = append(*directory.calls, "stat")
	if err := directory.statErrors[directory.statCalls]; err != nil {
		return nil, err
	}
	return fakeClickHouseRecoveryVolumeInfo{metadata: *directory.metadata}, nil
}

func (directory *fakeClickHouseRecoveryVolumeDirectory) Readdirnames(
	_ int,
) ([]string, error) {
	*directory.calls = append(*directory.calls, "readdirnames")
	if directory.readdirErr != nil {
		return nil, directory.readdirErr
	}
	if len(directory.entries) == 0 {
		return nil, io.EOF
	}
	return []string{directory.entries[0]}, nil
}

func (directory *fakeClickHouseRecoveryVolumeDirectory) Chown(uid, gid int) error {
	directory.chownCalls++
	*directory.calls = append(*directory.calls, fmt.Sprintf("chown:%d:%d", uid, gid))
	if directory.chownErr != nil {
		return directory.chownErr
	}
	directory.metadata.uid = uint32(uid) //nolint:gosec // Fixed production IDs are bounded.
	directory.metadata.gid = uint32(gid) //nolint:gosec // Fixed production IDs are bounded.
	return nil
}

func (directory *fakeClickHouseRecoveryVolumeDirectory) Chmod(mode os.FileMode) error {
	directory.chmodCalls++
	directory.chmodMode = mode
	*directory.calls = append(*directory.calls, "chmod")
	if directory.chmodErr != nil {
		return directory.chmodErr
	}
	directory.metadata.mode = os.ModeDir | mode
	return nil
}

func (directory *fakeClickHouseRecoveryVolumeDirectory) Close() error {
	if directory.closed {
		return errors.New("fake recovery volume descriptor closed twice")
	}
	directory.closed = true
	*directory.calls = append(*directory.calls, "close")
	return directory.closeErr
}

type fakeClickHouseRecoveryVolumeHarness struct {
	effectiveUID int
	descriptor   *fakeClickHouseRecoveryVolumeMetadata
	path         *fakeClickHouseRecoveryVolumeMetadata
	directory    *fakeClickHouseRecoveryVolumeDirectory
	calls        []string
	lstatErrors  map[int]error
	lstatCalls   int
	openErr      error
	aclErrors    map[int]error
	aclCalls     int
}

func newFakeClickHouseRecoveryVolumeHarness(
	metadata fakeClickHouseRecoveryVolumeMetadata,
) *fakeClickHouseRecoveryVolumeHarness {
	harness := &fakeClickHouseRecoveryVolumeHarness{
		descriptor:  &metadata,
		path:        &metadata,
		lstatErrors: make(map[int]error),
		aclErrors:   make(map[int]error),
	}
	harness.directory = &fakeClickHouseRecoveryVolumeDirectory{
		metadata:   harness.descriptor,
		calls:      &harness.calls,
		statErrors: make(map[int]error),
	}
	return harness
}

func (harness *fakeClickHouseRecoveryVolumeHarness) dependencies() prepareClickHouseRecoveryVolumeDependencies {
	return prepareClickHouseRecoveryVolumeDependencies{
		effectiveUID: func() int { return harness.effectiveUID },
		lstat: func(string) (os.FileInfo, error) {
			harness.lstatCalls++
			harness.calls = append(harness.calls, "lstat")
			if err := harness.lstatErrors[harness.lstatCalls]; err != nil {
				return nil, err
			}
			return fakeClickHouseRecoveryVolumeInfo{metadata: *harness.path}, nil
		},
		open: func(string) (clickHouseRecoveryVolumeDirectory, error) {
			harness.calls = append(harness.calls, "open")
			if harness.openErr != nil {
				return nil, harness.openErr
			}
			return harness.directory, nil
		},
		sameFile: func(left, right os.FileInfo) bool {
			leftInfo, leftOK := left.(fakeClickHouseRecoveryVolumeInfo)
			rightInfo, rightOK := right.(fakeClickHouseRecoveryVolumeInfo)
			return leftOK && rightOK && leftInfo.metadata.inode == rightInfo.metadata.inode
		},
		validateACL: func(clickHouseRecoveryVolumeDirectory) error {
			harness.aclCalls++
			harness.calls = append(harness.calls, "acl")
			return harness.aclErrors[harness.aclCalls]
		},
	}
}

func (harness *fakeClickHouseRecoveryVolumeHarness) swapPath() {
	swapped := *harness.path
	swapped.inode++
	harness.path = &swapped
}

func assertFakeRecoveryVolumeCallOrder(
	t *testing.T,
	calls []string,
	wanted ...string,
) {
	t.Helper()
	next := 0
	for _, call := range calls {
		if next < len(wanted) && call == wanted[next] {
			next++
		}
	}
	if next != len(wanted) {
		t.Fatalf("calls = %#v, want ordered subsequence %#v", calls, wanted)
	}
}
