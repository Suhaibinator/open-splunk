package input

import (
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
	cp := Checkpoint{Identity: id, Path: "/var/log/app.log", Offset: 4096, LineNumber: 12, UpdatedAt: time.Unix(1000, 0).UTC()}
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
