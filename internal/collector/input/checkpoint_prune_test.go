package input

import (
	"testing"
	"time"
)

const staleCheckpointAge = 8 * 24 * time.Hour

// TestCheckpointStorePreservesStaleEntriesAtOpen prevents age alone from
// discarding a resume position. UpdatedAt records acknowledgment activity, not
// whether a source or unacknowledged source bytes still exist.
func TestCheckpointStorePreservesStaleEntriesAtOpen(t *testing.T) {
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
		UpdatedAt: time.Now().UTC().Add(-staleCheckpointAge),
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
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reopened, err := NewCheckpointStore(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	t.Cleanup(func() { _ = reopened.Close() })

	if got, ok, getErr := reopened.Get(stale.Identity); getErr != nil || !ok || got.Offset != stale.Offset {
		t.Fatalf("stale checkpoint after reopen = (%+v, %t, %v), want preserved offset %d", got, ok, getErr, stale.Offset)
	}
	if _, ok, _ := reopened.Get(fresh.Identity); !ok {
		t.Fatal("fresh checkpoint was lost")
	}
	if _, ok, _ := reopened.Get(legacy.Identity); !ok {
		t.Fatal("legacy checkpoint was lost")
	}

	list, err := reopened.List()
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(list) != 3 {
		t.Fatalf("entries after reopen = %d, want 3", len(list))
	}
}
