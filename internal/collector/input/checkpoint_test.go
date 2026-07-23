package input

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestCheckpointStoreSetGetReopen(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	store, err := NewCheckpointStore(dir)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	id := FileIdentity{Device: 7, Inode: 42, Fingerprint: "deadbeef"}
	// Use a recent, truncated timestamp so JSON round-tripping is exact.
	cp := Checkpoint{Identity: id, Path: "/var/log/app.log", Offset: 4096, LineNumber: 12, UpdatedAt: time.Now().UTC().Add(-time.Minute).Truncate(time.Second)}
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

	got, ok, err := store2.Get(id)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if !ok {
		t.Fatalf("checkpoint not found after reopen")
	}
	if got.Offset != 4096 || got.LineNumber != 12 || got.Path != "/var/log/app.log" {
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

	old := FileIdentity{Device: 7, Inode: 9, Generation: 1, Fingerprint: "old"}
	newer := FileIdentity{Device: 7, Inode: 9, Generation: 2, Fingerprint: "new"}
	if err := store.Set(Checkpoint{Identity: old, Offset: 900}); err != nil {
		t.Fatalf("set old: %v", err)
	}
	if err := store.Set(Checkpoint{Identity: newer, Offset: 20}); err != nil {
		t.Fatalf("set new: %v", err)
	}
	// A delayed checkpoint from an old-generation batch cannot restore 900.
	if err := store.Set(Checkpoint{Identity: old, Offset: 950}); err != nil {
		t.Fatalf("set stale: %v", err)
	}
	lookup := newer
	lookup.Fingerprint = "fingerprint-computed-after-growth"
	cp, ok, err := store.Get(lookup)
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

	first := FileIdentity{Device: 1, Inode: 1, Generation: 1, Fingerprint: "first"}
	secondV1 := FileIdentity{Device: 1, Inode: 2, Generation: 1, Fingerprint: "second-v1"}
	secondV2 := FileIdentity{Device: 1, Inode: 2, Generation: 2, Fingerprint: "second-v2"}
	err = storeAPI.SetMany([]Checkpoint{
		{Identity: secondV1, Offset: 500, LineNumber: 5},
		{Identity: first, Offset: 100, LineNumber: 1},
		{Identity: secondV2, Offset: 20, LineNumber: 6},
		{Identity: secondV1, Offset: 900, LineNumber: 9}, // stale after generation 2
		{Identity: secondV2, Offset: 25, LineNumber: 7},
		{Identity: secondV2, Offset: 15, LineNumber: 6}, // regressing in generation 2
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

	currentID := FileIdentity{Device: 7, Inode: 9, Generation: 3, Fingerprint: "current"}
	current := Checkpoint{
		Identity:   currentID,
		Path:       "/logs/current.log",
		Offset:     100,
		LineNumber: 10,
		UpdatedAt:  time.Now().UTC().Add(-time.Minute),
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
			Identity:   currentID,
			Path:       current.Path,
			Offset:     100,
			LineNumber: current.LineNumber,
		},
		{
			Identity: FileIdentity{Device: 7, Inode: 9, Generation: 2, Fingerprint: "old"},
			Offset:   1_000,
		},
		{Identity: currentID, Offset: 99},
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
	got, ok, err := storeAPI.Get(currentID)
	if err != nil || !ok {
		t.Fatalf("Get current: ok=%v err=%v", ok, err)
	}
	if got.Path != current.Path || got.Offset != current.Offset || got.LineNumber != current.LineNumber ||
		!got.UpdatedAt.Equal(current.UpdatedAt) {
		t.Fatalf("no-op batch changed checkpoint: got %+v want %+v", got, current)
	}
}

func TestCheckpointStoreSetManyRefreshesIdentityMetadataAtEqualOffset(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	store, err := NewCheckpointStore(dir)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	empty := FileIdentity{Device: 4, Inode: 8, Generation: 1, Fingerprint: "empty", FingerprintLength: 0}
	grown := FileIdentity{Device: 4, Inode: 8, Generation: 1, Fingerprint: "grown", FingerprintLength: 64}
	if err := store.Set(Checkpoint{Identity: empty, Path: "/logs/app.log", Offset: 0}); err != nil {
		t.Fatalf("seed empty identity: %v", err)
	}
	if err := store.SetMany([]Checkpoint{{Identity: grown, Path: "/logs/app.log", Offset: 0}}); err != nil {
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
	cp, ok, err := reopened.Get(grown)
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

	existingID := FileIdentity{Device: 1, Inode: 1, Generation: 1, Fingerprint: "existing"}
	existing := Checkpoint{
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
	newID := FileIdentity{Device: 1, Inode: 2, Generation: 1, Fingerprint: "new"}
	err = storeAPI.SetMany([]Checkpoint{
		{Identity: existingID, Path: existing.Path, Offset: 20, LineNumber: 2},
		{Identity: newID, Path: "/logs/new.log", Offset: 30, LineNumber: 3},
	})
	if !errors.Is(err, persistErr) {
		t.Fatalf("SetMany error = %v, want %v", err, persistErr)
	}
	if persistCalls != 1 {
		t.Fatalf("persist calls = %d, want 1", persistCalls)
	}
	got, ok, err := storeAPI.Get(existingID)
	if err != nil || !ok {
		t.Fatalf("Get existing: ok=%v err=%v", ok, err)
	}
	if got.Offset != existing.Offset || got.LineNumber != existing.LineNumber || !got.UpdatedAt.Equal(existing.UpdatedAt) {
		t.Fatalf("existing checkpoint advanced after failure: got %+v want %+v", got, existing)
	}
	if _, ok, err := storeAPI.Get(newID); err != nil || ok {
		t.Fatalf("new checkpoint published after failure: ok=%v err=%v", ok, err)
	}

	reopened, err := NewCheckpointStore(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	disk, ok, err := reopened.Get(existingID)
	if err != nil || !ok {
		t.Fatalf("Get reopened existing: ok=%v err=%v", ok, err)
	}
	if disk.Offset != existing.Offset || disk.LineNumber != existing.LineNumber {
		t.Fatalf("disk checkpoint changed after failure: got %+v want %+v", disk, existing)
	}
	if _, ok, err := reopened.Get(newID); err != nil || ok {
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
		Identity: FileIdentity{Device: 1, Inode: 1, Fingerprint: "one"},
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
		id := FileIdentity{Device: 1, Inode: uint64(i), Fingerprint: "fp"}
		if err := store.Set(Checkpoint{Identity: id, Offset: uint64(i * 10)}); err != nil {
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

	id := FileIdentity{Device: 1, Inode: 1, Fingerprint: "x"}
	if err := store.Set(Checkpoint{Identity: id, Offset: 5}); err != nil {
		t.Fatalf("set: %v", err)
	}
	if err := store.Delete(id); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if _, ok, _ := store.Get(id); ok {
		t.Fatalf("checkpoint present after delete")
	}
	// Deleting a missing checkpoint is a no-op.
	if err := store.Delete(id); err != nil {
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
