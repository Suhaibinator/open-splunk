package input

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

const checkpointTestInputID = "input-a"

func TestCheckpointStoreSetGetReopen(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	store, err := NewCheckpointStore(dir)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	id := canonicalIdentityForTest(7, 42, 1, "ab", 64)
	// Use a recent, truncated timestamp so JSON round-tripping is exact.
	cp := Checkpoint{
		InputID:  checkpointTestInputID,
		Identity: id, Path: "/var/log/app.log", Offset: 4096,
		LineNumber: 12, NextLineNumber: 15,
		UpdatedAt: time.Now().UTC().Add(-time.Minute).Truncate(time.Second),
	}
	if err := store.Set(cp); err != nil {
		t.Fatalf("set: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// Reopen: the checkpoint must survive.
	store2, err := NewCheckpointStore(dir)
	if err != nil {
		t.Fatalf("reopen store: %v", err)
	}
	t.Cleanup(func() { _ = store2.Close() })

	got, ok, err := store2.Get(checkpointTestInputID, id)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if !ok {
		t.Fatalf("checkpoint not found after reopen")
	}
	if got.Offset != 4096 || got.LineNumber != 12 || got.NextLineNumber != 15 ||
		got.Path != "/var/log/app.log" {
		t.Fatalf("checkpoint round-trip mismatch: %+v", got)
	}
	if got.Identity.String() != id.String() {
		t.Fatalf("identity round-trip mismatch: %s", got.Identity)
	}
}

func TestCheckpointStoreUsesPhysicalKeyAndFencesOldGeneration(t *testing.T) {
	t.Parallel()
	store, err := NewCheckpointStore(t.TempDir())
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	old := canonicalIdentityForTest(7, 9, 1, "ab", 64)
	newer := canonicalIdentityForTest(7, 9, 2, "cd", 64)
	if err := store.Set(Checkpoint{
		InputID: checkpointTestInputID, Identity: old,
		Path: "/logs/app.log", Offset: 900,
	}); err != nil {
		t.Fatalf("set old: %v", err)
	}
	if err := store.Set(Checkpoint{
		InputID: checkpointTestInputID, Identity: newer,
		Path: "/logs/app.log", Offset: 20,
	}); err != nil {
		t.Fatalf("set new: %v", err)
	}
	// A delayed checkpoint from an old-generation batch cannot restore 900.
	if err := store.Set(Checkpoint{
		InputID: checkpointTestInputID, Identity: old,
		Path: "/logs/app.log", Offset: 950,
	}); err != nil {
		t.Fatalf("set stale: %v", err)
	}
	lookup := newer
	lookup.Fingerprint = "fingerprint-computed-after-growth"
	cp, ok, err := store.Get(checkpointTestInputID, lookup)
	if err != nil || !ok {
		t.Fatalf("get by stable key: ok=%v err=%v", ok, err)
	}
	if cp.Identity.String() != newer.String() || cp.Offset != 20 {
		t.Fatalf("checkpoint regressed: %+v", cp)
	}
}

func TestCheckpointStoreSetManyPersistsOneDeterministicSnapshot(t *testing.T) {
	t.Parallel()
	storeAPI, err := NewCheckpointStore(t.TempDir())
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = storeAPI.Close() })
	store := storeAPI.(*fileCheckpointStore)

	originalPersist := store.persistSnapshot
	var persisted [][]Checkpoint
	store.persistSnapshot = func(checkpoints []Checkpoint) error {
		persisted = append(persisted, append([]Checkpoint(nil), checkpoints...))
		return originalPersist(checkpoints)
	}

	first := canonicalIdentityForTest(1, 1, 1, "ab", 64)
	secondV1 := canonicalIdentityForTest(1, 2, 1, "cd", 64)
	secondV2 := canonicalIdentityForTest(1, 2, 2, "ef", 64)
	err = storeAPI.SetMany([]Checkpoint{
		{InputID: checkpointTestInputID, Identity: secondV1, Path: "/logs/second.log", Offset: 500, LineNumber: 5},
		{InputID: checkpointTestInputID, Identity: first, Path: "/logs/first.log", Offset: 100, LineNumber: 1},
		{InputID: checkpointTestInputID, Identity: secondV2, Path: "/logs/second.log", Offset: 20, LineNumber: 6},
		{InputID: checkpointTestInputID, Identity: secondV1, Path: "/logs/second.log", Offset: 900, LineNumber: 9}, // stale after generation 2
		{InputID: checkpointTestInputID, Identity: secondV2, Path: "/logs/second.log", Offset: 25, LineNumber: 7},
		{InputID: checkpointTestInputID, Identity: secondV2, Path: "/logs/second.log", Offset: 15, LineNumber: 6}, // regressing in generation 2
	})
	if err != nil {
		t.Fatalf("SetMany: %v", err)
	}
	if len(persisted) != 1 {
		t.Fatalf("persist calls = %d, want 1", len(persisted))
	}
	if len(persisted[0]) != 2 {
		t.Fatalf("persisted checkpoints = %d, want 2", len(persisted[0]))
	}
	if persisted[0][0].Identity.TrackingKey() != first.TrackingKey() ||
		persisted[0][1].Identity.TrackingKey() != secondV2.TrackingKey() {
		t.Fatalf("snapshot is not identity-sorted: %+v", persisted[0])
	}
	if got := persisted[0][1]; got.Identity.String() != secondV2.String() || got.Offset != 25 || got.LineNumber != 7 {
		t.Fatalf("second checkpoint = %+v, want generation 2 offset 25", got)
	}
	if persisted[0][0].UpdatedAt.IsZero() || persisted[0][1].UpdatedAt.IsZero() {
		t.Fatalf("SetMany did not stamp zero UpdatedAt values: %+v", persisted[0])
	}
}

func TestCheckpointStoreSetManyNoEffectiveAdvanceDoesNotPersist(t *testing.T) {
	t.Parallel()
	storeAPI, err := NewCheckpointStore(t.TempDir())
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = storeAPI.Close() })
	store := storeAPI.(*fileCheckpointStore)

	currentID := canonicalIdentityForTest(7, 9, 3, "ab", 64)
	current := Checkpoint{
		InputID:    checkpointTestInputID,
		Identity:   currentID,
		Path:       "/logs/current.log",
		Offset:     100,
		LineNumber: 10, NextLineNumber: 11,
		UpdatedAt: time.Now().UTC().Add(-time.Minute),
	}
	if err := storeAPI.Set(current); err != nil {
		t.Fatalf("seed: %v", err)
	}

	persistCalls := 0
	store.persistSnapshot = func([]Checkpoint) error {
		persistCalls++
		return nil
	}
	err = storeAPI.SetMany([]Checkpoint{
		{
			InputID:    checkpointTestInputID,
			Identity:   currentID,
			Path:       current.Path,
			Offset:     100,
			LineNumber: current.LineNumber,
		},
		{
			InputID:  checkpointTestInputID,
			Identity: canonicalIdentityForTest(7, 9, 2, "cd", 64),
			Path:     current.Path,
			Offset:   1_000,
		},
		{InputID: checkpointTestInputID, Identity: currentID, Path: current.Path, Offset: 99},
	})
	if err != nil {
		t.Fatalf("SetMany no-op: %v", err)
	}
	if err := storeAPI.SetMany(nil); err != nil {
		t.Fatalf("SetMany empty: %v", err)
	}
	if persistCalls != 0 {
		t.Fatalf("persist calls = %d, want 0", persistCalls)
	}
	got, ok, err := storeAPI.Get(checkpointTestInputID, currentID)
	if err != nil || !ok {
		t.Fatalf("Get current: ok=%v err=%v", ok, err)
	}
	if got.Path != current.Path || got.Offset != current.Offset || got.LineNumber != current.LineNumber ||
		got.NextLineNumber != current.NextLineNumber ||
		!got.UpdatedAt.Equal(current.UpdatedAt) {
		t.Fatalf("no-op batch changed checkpoint: got %+v want %+v", got, current)
	}
}

func TestCheckpointStoreRejectsConflictingNextLineAtSameOffset(t *testing.T) {
	t.Parallel()
	store, err := NewCheckpointStore(t.TempDir())
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	identity := canonicalIdentityForTest(7, 9, 3, "ab", 64)
	if err := store.Set(Checkpoint{
		InputID:  checkpointTestInputID,
		Identity: identity, Path: "/logs/current.log", Offset: 100,
		LineNumber: 10, NextLineNumber: 11,
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}
	if err := store.Set(Checkpoint{
		InputID:  checkpointTestInputID,
		Identity: identity, Path: "/logs/current.log", Offset: 100,
		LineNumber: 10, NextLineNumber: 12,
	}); err == nil || !strings.Contains(err.Error(), "conflicting next line number") {
		t.Fatalf("conflicting same-offset Set error = %v", err)
	}
	got, ok, err := store.Get(checkpointTestInputID, identity)
	if err != nil || !ok || got.NextLineNumber != 11 {
		t.Fatalf("checkpoint after rejected conflict = %+v (ok=%t err=%v)", got, ok, err)
	}
}

func TestCheckpointStoreRejectsConflictingLineAtSameOffsetAndReopens(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	store, err := NewCheckpointStore(dir)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	identity := canonicalIdentityForTest(7, 9, 3, "ab", 64)
	current := Checkpoint{
		InputID:  checkpointTestInputID,
		Identity: identity, Path: "/logs/current.log", Offset: 100,
		LineNumber: 10, NextLineNumber: 11,
	}
	if err := store.Set(current); err != nil {
		t.Fatalf("seed: %v", err)
	}
	conflict := current
	conflict.LineNumber = 100
	conflict.NextLineNumber = 0
	if err := store.Set(conflict); err == nil ||
		!strings.Contains(err.Error(), "conflicting line number") {
		t.Fatalf("conflicting same-offset Set error = %v", err)
	}
	got, ok, err := store.Get(checkpointTestInputID, identity)
	if err != nil || !ok || !checkpointPositionEqual(got, current) {
		t.Fatalf("checkpoint after rejected conflict = %+v (ok=%t err=%v)", got, ok, err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	reopened, err := NewCheckpointStore(dir)
	if err != nil {
		t.Fatalf("reopen after rejected conflict: %v", err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	got, ok, err = reopened.Get(checkpointTestInputID, identity)
	if err != nil || !ok || !checkpointPositionEqual(got, current) {
		t.Fatalf("reopened checkpoint = %+v (ok=%t err=%v), want %+v", got, ok, err, current)
	}
}

func TestCheckpointStoreRejectsRegressingNextLineAtGreaterOffset(t *testing.T) {
	t.Parallel()
	store, err := NewCheckpointStore(t.TempDir())
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	identity := canonicalIdentityForTest(7, 9, 3, "ab", 64)
	current := Checkpoint{
		InputID:  checkpointTestInputID,
		Identity: identity, Path: "/logs/current.log", Offset: 100,
		LineNumber: 100, NextLineNumber: 101,
	}
	if err := store.Set(current); err != nil {
		t.Fatalf("seed: %v", err)
	}
	if err := store.Set(Checkpoint{
		InputID:  checkpointTestInputID,
		Identity: identity, Path: current.Path, Offset: 200,
		LineNumber: 1, NextLineNumber: 2,
	}); err == nil || !strings.Contains(err.Error(), "does not advance") {
		t.Fatalf("regressing higher-offset Set error = %v", err)
	}
	got, ok, err := store.Get(checkpointTestInputID, identity)
	if err != nil || !ok || !checkpointPositionEqual(got, current) {
		t.Fatalf("checkpoint after rejected regression = %+v (ok=%t err=%v)", got, ok, err)
	}
}

func TestCheckpointStoreSetManyRefreshesIdentityMetadataAtEqualOffset(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	store, err := NewCheckpointStore(dir)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	empty := FileIdentity{
		Device: 4, Inode: 8, Generation: 1,
		Fingerprint: emptyFingerprintSHA256,
	}
	grown := canonicalIdentityForTest(4, 8, 1, "ab", 64)
	if err := store.Set(Checkpoint{
		InputID: checkpointTestInputID, Identity: empty,
		Path: "/logs/app.log", Offset: 0,
	}); err != nil {
		t.Fatalf("seed empty identity: %v", err)
	}
	if err := store.SetMany([]Checkpoint{{
		InputID: checkpointTestInputID, Identity: grown,
		Path: "/logs/app.log", Offset: 0,
	}}); err != nil {
		t.Fatalf("refresh identity: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	reopened, err := NewCheckpointStore(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	cp, ok, err := reopened.Get(checkpointTestInputID, grown)
	if err != nil || !ok {
		t.Fatalf("Get grown: ok=%v err=%v", ok, err)
	}
	if cp.Identity != grown || cp.Offset != 0 {
		t.Fatalf("checkpoint metadata = %+v, want grown identity at offset zero", cp)
	}
}

func TestCheckpointStoreSetManyPersistenceFailureRollsBackMemory(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	storeAPI, err := NewCheckpointStore(dir)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = storeAPI.Close() })
	store := storeAPI.(*fileCheckpointStore)

	existingID := canonicalIdentityForTest(1, 1, 1, "ab", 64)
	existing := Checkpoint{
		InputID:    checkpointTestInputID,
		Identity:   existingID,
		Path:       "/logs/existing.log",
		Offset:     10,
		LineNumber: 1,
		UpdatedAt:  time.Now().UTC().Add(-time.Minute),
	}
	if err := storeAPI.Set(existing); err != nil {
		t.Fatalf("seed: %v", err)
	}

	persistErr := errors.New("injected persistence failure")
	persistCalls := 0
	store.persistSnapshot = func([]Checkpoint) error {
		persistCalls++
		return persistErr
	}
	newID := canonicalIdentityForTest(1, 2, 1, "cd", 64)
	err = storeAPI.SetMany([]Checkpoint{
		{InputID: checkpointTestInputID, Identity: existingID, Path: existing.Path, Offset: 20, LineNumber: 2},
		{InputID: checkpointTestInputID, Identity: newID, Path: "/logs/new.log", Offset: 30, LineNumber: 3},
	})
	if !errors.Is(err, persistErr) {
		t.Fatalf("SetMany error = %v, want %v", err, persistErr)
	}
	if persistCalls != 1 {
		t.Fatalf("persist calls = %d, want 1", persistCalls)
	}
	got, ok, err := storeAPI.Get(checkpointTestInputID, existingID)
	if err != nil || !ok {
		t.Fatalf("Get existing: ok=%v err=%v", ok, err)
	}
	if got.Offset != existing.Offset || got.LineNumber != existing.LineNumber || !got.UpdatedAt.Equal(existing.UpdatedAt) {
		t.Fatalf("existing checkpoint advanced after failure: got %+v want %+v", got, existing)
	}
	if _, newExists, err := storeAPI.Get(checkpointTestInputID, newID); err != nil || newExists {
		t.Fatalf("new checkpoint published after failure: ok=%v err=%v", newExists, err)
	}

	reopened, err := NewCheckpointStore(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	disk, ok, err := reopened.Get(checkpointTestInputID, existingID)
	if err != nil || !ok {
		t.Fatalf("Get reopened existing: ok=%v err=%v", ok, err)
	}
	if disk.Offset != existing.Offset || disk.LineNumber != existing.LineNumber {
		t.Fatalf("disk checkpoint changed after failure: got %+v want %+v", disk, existing)
	}
	if _, ok, err := reopened.Get(checkpointTestInputID, newID); err != nil || ok {
		t.Fatalf("new disk checkpoint present after failure: ok=%v err=%v", ok, err)
	}
}

func TestCheckpointStoreSetDelegatesToSetMany(t *testing.T) {
	t.Parallel()
	storeAPI, err := NewCheckpointStore(t.TempDir())
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = storeAPI.Close() })
	store := storeAPI.(*fileCheckpointStore)

	persistCalls := 0
	store.persistSnapshot = func(checkpoints []Checkpoint) error {
		persistCalls++
		if len(checkpoints) != 1 || checkpoints[0].Offset != 42 {
			t.Fatalf("persisted snapshot = %+v", checkpoints)
		}
		return nil
	}
	if err := storeAPI.Set(Checkpoint{
		InputID:  checkpointTestInputID,
		Identity: canonicalIdentityForTest(1, 1, 1, "ab", 64),
		Path:     "/logs/app.log",
		Offset:   42,
	}); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if persistCalls != 1 {
		t.Fatalf("persist calls = %d, want 1", persistCalls)
	}
}

func TestCheckpointStoreAtomicRewriteNoTempLeak(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	store, err := NewCheckpointStore(dir)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	for i := 0; i < 5; i++ {
		id := canonicalIdentityForTest(1, uint64(i), 1, "ab", 64)
		if err := store.Set(Checkpoint{
			InputID: checkpointTestInputID, Identity: id,
			Path: "/logs/app.log", Offset: uint64(i * 10),
		}); err != nil {
			t.Fatalf("set %d: %v", i, err)
		}
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("readdir: %v", err)
	}
	for _, e := range entries {
		if strings.Contains(e.Name(), ".tmp-") {
			t.Fatalf("temp file leaked after atomic rewrite: %s", e.Name())
		}
	}
	if _, err := os.Stat(filepath.Join(dir, checkpointFileName)); err != nil {
		t.Fatalf("checkpoint file missing: %v", err)
	}

	all, err := store.List()
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(all) != 5 {
		t.Fatalf("expected 5 checkpoints, got %d", len(all))
	}
}

func TestCheckpointStoreDelete(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	store, err := NewCheckpointStore(dir)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	id := canonicalIdentityForTest(1, 1, 1, "ab", 64)
	if err := store.Set(Checkpoint{
		InputID: checkpointTestInputID, Identity: id,
		Path: "/logs/app.log", Offset: 5,
	}); err != nil {
		t.Fatalf("set: %v", err)
	}
	if err := store.Delete(checkpointTestInputID, id); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if _, ok, _ := store.Get(checkpointTestInputID, id); ok {
		t.Fatalf("checkpoint present after delete")
	}
	// Deleting a missing checkpoint is a no-op.
	if err := store.Delete(checkpointTestInputID, id); err != nil {
		t.Fatalf("delete missing: %v", err)
	}
}

func TestCheckpointStoreMissingFileTolerated(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	// No file exists yet; opening must succeed with an empty store.
	store, err := NewCheckpointStore(dir)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	all, err := store.List()
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(all) != 0 {
		t.Fatalf("expected empty store, got %d", len(all))
	}
}

func TestCheckpointStoreCorruptFileErrorsWithPath(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, checkpointFileName)
	if err := os.WriteFile(path, []byte("{not valid json"), 0o644); err != nil {
		t.Fatalf("seed corrupt file: %v", err)
	}
	_, err := NewCheckpointStore(dir)
	if err == nil {
		t.Fatalf("expected error opening corrupt store")
	}
	if !strings.Contains(err.Error(), path) {
		t.Fatalf("corrupt-store error should name the path %q, got %v", path, err)
	}
}
