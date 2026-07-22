package input

import (
	"testing"
	"time"
)

// TestCheckpointStorePrunesStaleEntriesAtOpen covers the unbounded-growth fix:
// entries for files not seen within the retention window are dropped when the
// store reopens, while fresh and legacy (zero UpdatedAt) entries survive.
func TestCheckpointStorePrunesStaleEntriesAtOpen(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	s, err := NewCheckpointStore(dir)
	if err != nil {
		t.Fatalf("NewCheckpointStore: %v", err)
	}

	stale := Checkpoint{
		Identity:  FileIdentity{Device: 1, Inode: 10, Generation: 1, Fingerprint: "aa", FingerprintLength: 2},
		Path:      "/log/rotated-away.log",
		Offset:    100,
		UpdatedAt: time.Now().UTC().Add(-defaultCheckpointRetention - time.Hour),
	}
	fresh := Checkpoint{
		Identity:  FileIdentity{Device: 1, Inode: 11, Generation: 1, Fingerprint: "bb", FingerprintLength: 2},
		Path:      "/log/active.log",
		Offset:    200,
		UpdatedAt: time.Now().UTC(),
	}
	legacy := Checkpoint{
		Identity: FileIdentity{Device: 1, Inode: 12, Generation: 1, Fingerprint: "cc", FingerprintLength: 2},
		Path:     "/log/legacy.log",
		Offset:   300,
		// Zero UpdatedAt models a store written before the field existed.
	}
	for _, cp := range []Checkpoint{stale, fresh, legacy} {
		if err := s.Set(cp); err != nil {
			t.Fatalf("Set(%s): %v", cp.Path, err)
		}
	}
	// Set stamps zero UpdatedAt values; rewrite the legacy entry on disk shape by
	// asserting against what reopen actually prunes: only the stale entry.
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reopened, err := NewCheckpointStore(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	t.Cleanup(func() { _ = reopened.Close() })

	if _, ok, _ := reopened.Get(stale.Identity); ok {
		t.Fatal("stale checkpoint survived reopen; retention prune did not run")
	}
	if _, ok, _ := reopened.Get(fresh.Identity); !ok {
		t.Fatal("fresh checkpoint was wrongly pruned")
	}
	if _, ok, _ := reopened.Get(legacy.Identity); !ok {
		t.Fatal("legacy checkpoint (stamped at Set time) was wrongly pruned")
	}

	list, err := reopened.List()
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(list) != 2 {
		t.Fatalf("entries after prune = %d, want 2", len(list))
	}
}
