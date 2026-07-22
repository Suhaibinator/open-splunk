package wal

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

// TestAppendBatchTooLargeReturnsSentinel confirms that a batch whose on-disk
// record can never fit MaxQueueBytes (even in an empty queue) returns the
// terminal ErrBatchTooLarge, not the transient ErrQueueFull. (FIX 2)
func TestAppendBatchTooLargeReturnsSentinel(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	opts := defaultOpts(dir)
	opts.MaxQueueBytes = 8 // smaller than any real batch record
	q, err := Open(opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = q.Close() })

	_, err = q.Append(makeEvents("x"))
	if !errors.Is(err, ErrBatchTooLarge) {
		t.Fatalf("Append of oversized batch = %v, want ErrBatchTooLarge", err)
	}
	if errors.Is(err, ErrQueueFull) {
		t.Fatalf("oversized batch must not be reported as ErrQueueFull")
	}

	// The sequence must not have advanced (a clean no-op, like ErrQueueFull).
	if st := q.Stats(); st.NextBatchSequence != 1 {
		t.Fatalf("NextBatchSequence after ErrBatchTooLarge = %d, want 1 (no sequence burned)", st.NextBatchSequence)
	}
}

// TestWALFilesAreOwnerOnly verifies the WAL directory, segments, and meta file
// carry no group/world access, since they hold raw event payloads. (FIX 6)
func TestWALFilesAreOwnerOnly(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	q, err := Open(defaultOpts(dir))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if _, err := q.Append(makeEvents("a", "b")); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if err := q.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	assertNoGroupOrWorld(t, dir)
	assertNoGroupOrWorld(t, filepath.Join(dir, metaFileName))
	for _, name := range listWALFiles(t, dir) {
		assertNoGroupOrWorld(t, filepath.Join(dir, name))
	}
}

// assertNoGroupOrWorld fails if path grants any permission to group or other.
func assertNoGroupOrWorld(t *testing.T, path string) {
	t.Helper()
	fi, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat %s: %v", path, err)
	}
	if perm := fi.Mode().Perm(); perm&0o077 != 0 {
		t.Fatalf("%s mode = %o, want no group/world access (0o077 bits clear)", path, perm)
	}
}
