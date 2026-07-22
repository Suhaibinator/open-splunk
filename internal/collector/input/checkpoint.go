package input

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

// checkpointFileName is the single JSON document holding every checkpoint for a
// store directory.
const checkpointFileName = "checkpoints.json"

// checkpointFormatVersion is written into the store file so a future format
// change can be detected on load.
const checkpointFormatVersion = 1

// defaultCheckpointRetention bounds how long a checkpoint for a vanished file
// is kept. Hosts that rotate to fresh inodes accumulate one entry per rotated
// file; without pruning the store grows without bound and every Set rewrites an
// ever-larger document. Entries older than this are dropped when the store is
// opened. Seven days comfortably exceeds any plausible collector downtime while
// keeping the store bounded by the number of files active within the window.
const defaultCheckpointRetention = 7 * 24 * time.Hour

// checkpointDoc is the on-disk shape of the checkpoint store.
type checkpointDoc struct {
	Version     int          `json:"version"`
	Checkpoints []Checkpoint `json:"checkpoints"`
}

// fileCheckpointStore is a CheckpointStore backed by one atomically-rewritten
// JSON file. The whole document is rewritten on every mutation; checkpoint
// counts are bounded by the number of tracked files, so this stays cheap.
type fileCheckpointStore struct {
	dir  string
	path string

	mu      sync.Mutex
	entries map[string]Checkpoint // keyed by FileIdentity.TrackingKey()
}

// NewCheckpointStore opens or creates the checkpoint store rooted at dir. A
// missing store file is tolerated (an empty store). A store file that exists
// but cannot be parsed is a hard error naming the path, so a corrupt file is
// never silently discarded.
func NewCheckpointStore(dir string) (CheckpointStore, error) {
	// 0o700 and tighten a pre-existing directory: checkpoints reveal tracked
	// file paths and must not be world-readable, matching the WAL and
	// dead-letter treatment of the state directory.
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, fmt.Errorf("collector/input: create checkpoint dir %s: %w", dir, err)
	}
	if err := os.Chmod(dir, 0o700); err != nil {
		return nil, fmt.Errorf("collector/input: secure checkpoint dir %s: %w", dir, err)
	}
	s := &fileCheckpointStore{
		dir:     dir,
		path:    filepath.Join(dir, checkpointFileName),
		entries: make(map[string]Checkpoint),
	}
	if err := s.load(); err != nil {
		return nil, err
	}
	if _, err := s.pruneOlderThan(defaultCheckpointRetention); err != nil {
		return nil, err
	}
	return s, nil
}

// pruneOlderThan drops entries whose UpdatedAt is older than retention and
// persists the result when anything was removed, returning the removed count.
// Entries with a zero UpdatedAt (a format predating the field) are kept.
func (s *fileCheckpointStore) pruneOlderThan(retention time.Duration) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	cutoff := time.Now().UTC().Add(-retention)
	removed := 0
	for key, cp := range s.entries {
		if !cp.UpdatedAt.IsZero() && cp.UpdatedAt.Before(cutoff) {
			delete(s.entries, key)
			removed++
		}
	}
	if removed == 0 {
		return 0, nil
	}
	return removed, s.persistLocked()
}

// load reads the store file into memory. A missing file yields an empty store.
func (s *fileCheckpointStore) load() error {
	data, err := os.ReadFile(s.path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return fmt.Errorf("collector/input: read checkpoint file %s: %w", s.path, err)
	}
	var doc checkpointDoc
	if err := json.Unmarshal(data, &doc); err != nil {
		return fmt.Errorf("collector/input: corrupt checkpoint file %s: %w", s.path, err)
	}
	for _, cp := range doc.Checkpoints {
		s.entries[cp.Identity.TrackingKey()] = cp
	}
	return nil
}

// Get returns the checkpoint for id and whether one exists.
func (s *fileCheckpointStore) Get(id FileIdentity) (Checkpoint, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	cp, ok := s.entries[id.TrackingKey()]
	return cp, ok, nil
}

// Set atomically persists cp (temp file + fsync + rename over the target). If
// cp.UpdatedAt is unset it is stamped with the current time.
func (s *fileCheckpointStore) Set(cp Checkpoint) error {
	if cp.UpdatedAt.IsZero() {
		cp.UpdatedAt = time.Now().UTC()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	key := cp.Identity.TrackingKey()
	if current, ok := s.entries[key]; ok {
		switch {
		case cp.Identity.Generation < current.Identity.Generation:
			return nil // a delayed old-generation batch must not undo truncation
		case cp.Identity.Generation == current.Identity.Generation && cp.Offset < current.Offset:
			return nil // offsets are monotonic within one generation
		}
	}
	s.entries[key] = cp
	return s.persistLocked()
}

// Delete removes the checkpoint for id, if any, and persists the result.
func (s *fileCheckpointStore) Delete(id FileIdentity) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := id.TrackingKey()
	if _, ok := s.entries[key]; !ok {
		return nil
	}
	delete(s.entries, key)
	return s.persistLocked()
}

// List returns all persisted checkpoints, ordered by identity for determinism.
func (s *fileCheckpointStore) List() ([]Checkpoint, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.snapshotLocked(), nil
}

// Close releases the store. Every mutation already persisted synchronously, so
// there is nothing to flush.
func (s *fileCheckpointStore) Close() error { return nil }

// snapshotLocked returns the entries as an identity-sorted slice.
func (s *fileCheckpointStore) snapshotLocked() []Checkpoint {
	out := make([]Checkpoint, 0, len(s.entries))
	for _, cp := range s.entries {
		out = append(out, cp)
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].Identity.String() < out[j].Identity.String()
	})
	return out
}

// persistLocked writes the whole store atomically: marshal, write a temp file
// in the same directory, fsync it, rename over the target, then fsync the
// directory so the rename itself is durable. A crash leaves either the old or
// the new complete file, never a torn one.
func (s *fileCheckpointStore) persistLocked() error {
	doc := checkpointDoc{Version: checkpointFormatVersion, Checkpoints: s.snapshotLocked()}
	data, err := json.MarshalIndent(doc, "", "  ")
	if err != nil {
		return fmt.Errorf("collector/input: marshal checkpoints: %w", err)
	}

	tmp, err := os.CreateTemp(s.dir, checkpointFileName+".tmp-*")
	if err != nil {
		return fmt.Errorf("collector/input: create temp checkpoint file: %w", err)
	}
	tmpName := tmp.Name()
	// Best-effort cleanup if we fail before the rename.
	defer func() { _ = os.Remove(tmpName) }()

	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("collector/input: write temp checkpoint file: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("collector/input: fsync temp checkpoint file: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("collector/input: close temp checkpoint file: %w", err)
	}
	if err := os.Rename(tmpName, s.path); err != nil {
		return fmt.Errorf("collector/input: rename checkpoint file: %w", err)
	}
	s.fsyncDir()
	return nil
}

// fsyncDir flushes the store directory so a just-completed rename survives a
// crash. Directory fsync is best-effort: not every filesystem supports it, and
// failure here does not undo the durable temp write + rename.
func (s *fileCheckpointStore) fsyncDir() {
	d, err := os.Open(s.dir)
	if err != nil {
		return
	}
	defer d.Close()
	_ = d.Sync()
}
