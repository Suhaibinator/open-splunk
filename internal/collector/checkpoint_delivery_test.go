package collector

import (
	"strings"
	"testing"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/collector/input"
	"github.com/Suhaibinator/open-splunk/internal/collector/wal"
)

const testCheckpointInputID = "input"

func TestCommitTerminalCheckpointsAdvancesFromDurableBatchOrigin(t *testing.T) {
	t.Parallel()
	store, err := input.NewCheckpointStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewCheckpointStore: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	identity := input.FileIdentity{
		Device:            7,
		Inode:             11,
		Generation:        2,
		Fingerprint:       strings.Repeat("ab", 32),
		FingerprintLength: 913,
	}
	if err := store.Set(input.Checkpoint{
		InputID: testCheckpointInputID, Identity: identity, Path: "/logs/app.log", Offset: 0,
	}); err != nil {
		t.Fatalf("seed discovery checkpoint: %v", err)
	}
	marks := []wal.SourceCheckpointMark{
		checkpointSourceMark(1, identity, 100, 1),
		checkpointSourceMark(2, identity, 240, 2),
	}

	if _, err := commitTerminalCheckpoints(store, marks); err != nil {
		t.Fatalf("commitTerminalCheckpoints: %v", err)
	}
	got, ok, err := store.Get(testCheckpointInputID, identity)
	if err != nil || !ok {
		t.Fatalf("Get = (%+v, %t, %v)", got, ok, err)
	}
	if got.Offset != 240 || got.LineNumber != 2 || got.NextLineNumber != 3 ||
		got.Path != "/logs/app.log" {
		t.Fatalf("checkpoint = %+v, want offset 240 lines [2,3) with source path", got)
	}
	if got.Identity != identity {
		t.Fatalf("identity = %+v, want full identity %+v", got.Identity, identity)
	}
}

func TestCommitTerminalCheckpointsFencesDelayedPreCopytruncateGeneration(t *testing.T) {
	t.Parallel()
	store, err := input.NewCheckpointStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewCheckpointStore: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	oldIdentity := input.FileIdentity{
		Device: 5, Inode: 9, Generation: 1,
		Fingerprint: strings.Repeat("11", 32), FingerprintLength: 1024,
	}
	newIdentity := input.FileIdentity{
		Device: 5, Inode: 9, Generation: 2,
		Fingerprint: strings.Repeat("22", 32), FingerprintLength: 64,
	}
	if err := store.Set(input.Checkpoint{
		InputID: testCheckpointInputID, Identity: newIdentity,
		Path: "/logs/app.log", Offset: 20, LineNumber: 1,
	}); err != nil {
		t.Fatalf("seed new generation checkpoint: %v", err)
	}

	if _, err := commitTerminalCheckpoints(store, []wal.SourceCheckpointMark{
		checkpointSourceMark(1, oldIdentity, 900, 90),
	}); err != nil {
		t.Fatalf("commit old generation: %v", err)
	}
	got, ok, err := store.Get(testCheckpointInputID, newIdentity)
	if err != nil || !ok {
		t.Fatalf("Get = (%+v, %t, %v)", got, ok, err)
	}
	if got.Identity != newIdentity || got.Offset != 20 {
		t.Fatalf("delayed old generation crossed copytruncate fence: %+v", got)
	}
}

func TestCommitTerminalCheckpointsRejectsInvalidNextLine(t *testing.T) {
	t.Parallel()
	store, err := input.NewCheckpointStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewCheckpointStore: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	identity := input.FileIdentity{
		Device: 1, Inode: 2, Generation: 1,
		Fingerprint: strings.Repeat("ab", 32), FingerprintLength: 64,
	}
	if err := store.Set(input.Checkpoint{
		InputID:  testCheckpointInputID,
		Identity: identity, Path: "/logs/app.log", NextLineNumber: 1,
	}); err != nil {
		t.Fatalf("seed discovery checkpoint: %v", err)
	}

	for _, nextLine := range []uint64{0, 5, ^uint64(0)} {
		mark := checkpointSourceMark(1, identity, 100, 5)
		mark.NextLineNumber = nextLine
		if _, err := commitTerminalCheckpoints(store, []wal.SourceCheckpointMark{mark}); err == nil ||
			!strings.Contains(err.Error(), "invalid next_line_number") {
			t.Fatalf("commit next line %d error = %v, want invalid next_line_number", nextLine, err)
		}
	}
}

func TestSourceCheckpointsFromWALRejectsCursorConflictWithDurableCheckpoint(t *testing.T) {
	t.Parallel()
	store, err := input.NewCheckpointStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewCheckpointStore: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	identity := input.FileIdentity{
		Device: 1, Inode: 2, Generation: 1,
		Fingerprint: strings.Repeat("ab", 32), FingerprintLength: 64,
	}
	if err := store.Set(input.Checkpoint{
		InputID:  testCheckpointInputID,
		Identity: identity, Path: "/logs/app.log", Offset: 100,
		LineNumber: 9, NextLineNumber: 10,
	}); err != nil {
		t.Fatalf("seed durable checkpoint: %v", err)
	}

	for _, mark := range []wal.SourceCheckpointMark{
		checkpointSourceMark(1, identity, 200, 8),
		checkpointSourceMark(1, identity, 100, 10),
	} {
		if _, err := sourceCheckpointsFromWAL(store, []wal.SourceCheckpointMark{mark}); err == nil ||
			!strings.Contains(err.Error(), "line cursor conflicts with durable checkpoint") {
			t.Fatalf("sourceCheckpointsFromWAL(%+v) error = %v, want durable cursor conflict", mark, err)
		}
	}
}

func TestSourceCheckpointsFromWALRejectsConflictingPendingIdentities(t *testing.T) {
	t.Parallel()
	store, err := input.NewCheckpointStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewCheckpointStore: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	first := input.FileIdentity{
		Device: 1, Inode: 2, Generation: 1,
		Fingerprint: strings.Repeat("ab", 32), FingerprintLength: 64,
	}
	second := first
	second.Fingerprint = strings.Repeat("cd", 32)

	_, err = sourceCheckpointsFromWAL(store, []wal.SourceCheckpointMark{
		checkpointSourceMark(1, first, 100, 1),
		checkpointSourceMark(2, second, 200, 2),
	})
	if err == nil || !strings.Contains(err.Error(), "conflicts with another source identity") {
		t.Fatalf("sourceCheckpointsFromWAL conflicting identities error = %v", err)
	}
}

func TestCommitTerminalCheckpointsKeepsIdenticalFilesIndependentByInput(t *testing.T) {
	t.Parallel()
	store, err := input.NewCheckpointStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewCheckpointStore: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	identity := input.FileIdentity{
		Device: 31, Inode: 41, Generation: 1,
		Fingerprint: strings.Repeat("ef", 32), FingerprintLength: 64,
	}

	markA := checkpointSourceMarkForInput("input-a", 1, identity, "/logs/shared.log", 100, 1)
	markB := checkpointSourceMarkForInput("input-b", 2, identity, "/logs/shared.log", 240, 2)
	committed, err := commitTerminalCheckpoints(store, []wal.SourceCheckpointMark{markA, markB})
	if err != nil {
		t.Fatalf("commitTerminalCheckpoints: %v", err)
	}
	if len(committed) != 2 {
		t.Fatalf("committed checkpoints = %+v, want one per input", committed)
	}

	for inputID, wantOffset := range map[string]uint64{"input-a": 100, "input-b": 240} {
		got, ok, getErr := store.Get(inputID, identity)
		if getErr != nil || !ok || got.InputID != inputID || got.Offset != wantOffset {
			t.Fatalf(
				"Get(%q) = (%+v, %t, %v), want input-scoped offset %d",
				inputID, got, ok, getErr, wantOffset,
			)
		}
	}
}

func TestSourceCheckpointsFromWALReturnsDeterministicOrder(t *testing.T) {
	t.Parallel()
	store, err := input.NewCheckpointStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewCheckpointStore: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	first := input.FileIdentity{
		Device: 1, Inode: 3, Generation: 1,
		Fingerprint: strings.Repeat("ab", 32), FingerprintLength: 64,
	}
	second := input.FileIdentity{
		Device: 1, Inode: 2, Generation: 1,
		Fingerprint: strings.Repeat("cd", 32), FingerprintLength: 64,
	}
	marks := []wal.SourceCheckpointMark{
		checkpointSourceMarkForInput("z-input", 1, first, "/logs/z.log", 10, 1),
		checkpointSourceMarkForInput("a-input", 2, first, "/logs/a-3.log", 20, 2),
		checkpointSourceMarkForInput("a-input", 3, second, "/logs/a-2.log", 30, 3),
	}

	got, err := sourceCheckpointsFromWAL(store, marks)
	if err != nil {
		t.Fatalf("sourceCheckpointsFromWAL: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("checkpoint count = %d, want 3", len(got))
	}
	if got[0].InputID != "a-input" || got[0].Identity.String() != second.String() ||
		got[1].InputID != "a-input" || got[1].Identity.String() != first.String() ||
		got[2].InputID != "z-input" {
		t.Fatalf("checkpoint order = [%s/%s, %s/%s, %s/%s]",
			got[0].InputID, got[0].Identity.String(),
			got[1].InputID, got[1].Identity.String(),
			got[2].InputID, got[2].Identity.String())
	}
}

func TestCommitTerminalCheckpointsPersistsRewriteGuard(t *testing.T) {
	t.Parallel()
	store, err := input.NewCheckpointStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewCheckpointStore: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	identity := input.FileIdentity{
		Device: 7, Inode: 9, Generation: 1,
		Fingerprint: strings.Repeat("ab", 32), FingerprintLength: 64,
	}
	if err := store.Set(input.Checkpoint{
		InputID: testCheckpointInputID, Identity: identity, Path: "/logs/app.log",
	}); err != nil {
		t.Fatalf("seed checkpoint: %v", err)
	}
	mark := checkpointSourceMark(1, identity, 100, 1)
	mark.GuardFingerprint = strings.Repeat("cd", 32)
	mark.GuardLength = 32
	mark.HasGuardFingerprint = true
	mark.HasGuardLength = true

	if _, err := commitTerminalCheckpoints(store, []wal.SourceCheckpointMark{mark}); err != nil {
		t.Fatalf("commitTerminalCheckpoints: %v", err)
	}
	got, ok, err := store.Get(testCheckpointInputID, identity)
	if err != nil || !ok {
		t.Fatalf("Get = (%+v, %t, %v)", got, ok, err)
	}
	if got.GuardFingerprint != mark.GuardFingerprint || got.GuardLength != mark.GuardLength {
		t.Fatalf("checkpoint guard = %q/%d, want %q/%d", got.GuardFingerprint, got.GuardLength, mark.GuardFingerprint, mark.GuardLength)
	}
}

func TestSourceCheckpointsFromWALValidatesRewriteGuard(t *testing.T) {
	t.Parallel()
	store, err := input.NewCheckpointStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewCheckpointStore: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	identity := input.FileIdentity{
		Device: 7, Inode: 9, Generation: 1,
		Fingerprint: strings.Repeat("ab", 32), FingerprintLength: 64,
	}
	if err := store.Set(input.Checkpoint{
		InputID: testCheckpointInputID, Identity: identity, Path: "/logs/app.log",
	}); err != nil {
		t.Fatalf("seed checkpoint: %v", err)
	}
	for _, mutate := range []func(*wal.SourceCheckpointMark){
		func(mark *wal.SourceCheckpointMark) { mark.HasGuardFingerprint = true },
		func(mark *wal.SourceCheckpointMark) {
			mark.HasGuardFingerprint, mark.HasGuardLength = true, true
			mark.GuardFingerprint, mark.GuardLength = "bad", 1
		},
		func(mark *wal.SourceCheckpointMark) {
			mark.HasGuardFingerprint, mark.HasGuardLength = true, true
			mark.GuardFingerprint, mark.GuardLength = strings.Repeat("cd", 32), 101
		},
	} {
		mark := checkpointSourceMark(1, identity, 100, 1)
		mutate(&mark)
		if _, err := sourceCheckpointsFromWAL(store, []wal.SourceCheckpointMark{mark}); err == nil {
			t.Fatalf("sourceCheckpointsFromWAL(%+v) error = nil", mark)
		}
	}
}

func TestSourceCheckpointsFromWALLegacyMarkKeepsDurableGuardAtSameOffset(t *testing.T) {
	t.Parallel()
	store, err := input.NewCheckpointStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewCheckpointStore: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	identity := input.FileIdentity{
		Device: 7, Inode: 9, Generation: 1,
		Fingerprint: strings.Repeat("ab", 32), FingerprintLength: 64,
	}
	wantGuard := strings.Repeat("ef", 32)
	if err := store.Set(input.Checkpoint{
		InputID: testCheckpointInputID, Identity: identity, Path: "/logs/app.log",
		Offset: 100, LineNumber: 1, NextLineNumber: 2,
		GuardFingerprint: wantGuard, GuardLength: 32,
	}); err != nil {
		t.Fatalf("seed checkpoint: %v", err)
	}
	checkpoints, err := sourceCheckpointsFromWAL(store, []wal.SourceCheckpointMark{
		checkpointSourceMark(1, identity, 100, 1),
	})
	if err != nil {
		t.Fatalf("sourceCheckpointsFromWAL: %v", err)
	}
	if len(checkpoints) != 1 || checkpoints[0].GuardFingerprint != wantGuard || checkpoints[0].GuardLength != 32 {
		t.Fatalf("reconstructed checkpoints = %+v", checkpoints)
	}
}

func checkpointSourceMark(sequence uint64, identity input.FileIdentity, end, line uint64) wal.SourceCheckpointMark {
	return checkpointSourceMarkForInput(
		testCheckpointInputID,
		sequence,
		identity,
		"/logs/app.log",
		end,
		line,
	)
}

func checkpointSourceMarkForInput(
	inputID string,
	sequence uint64,
	identity input.FileIdentity,
	path string,
	end, line uint64,
) wal.SourceCheckpointMark {
	return wal.SourceCheckpointMark{
		InputID:       inputID,
		BatchSequence: sequence, FileIdentity: identity.String(),
		SourcePath: path, HasSourcePath: true,
		EndOffset: end, HasEndOffset: true, LineNumber: line,
		NextLineNumber: line + 1, HasNextLineNumber: true,
		FingerprintLength: identity.FingerprintLength, HasFingerprintLength: true,
	}
}

func checkpointBatch(sequence uint64, identity input.FileIdentity, path string, start, end, line uint64) *opensplunkv1.EventBatch {
	return &opensplunkv1.EventBatch{
		BatchSequence: sequence,
		Events: []*opensplunkv1.LogEvent{{
			EventId: "event",
			Origin: &opensplunkv1.EventOrigin{
				InputId:               "input",
				FileIdentity:          new(identity.String()),
				StartOffset:           new(start),
				EndOffset:             new(end),
				LineNumber:            new(line),
				NextLineNumber:        new(line + 1),
				SourcePath:            new(path),
				FileFingerprintLength: new(identity.FingerprintLength),
			},
		}},
	}
}
