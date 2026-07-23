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

	mu              sync.Mutex
	entries         map[string]Checkpoint // keyed by FileIdentity.TrackingKey()
	persistSnapshot func([]Checkpoint) error
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
	s.persistSnapshot = s.writeSnapshot
	if err := s.load(); err != nil {
		return nil, err
	}
	return s, nil
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

// Set atomically persists cp by delegating to SetMany.
func (s *fileCheckpointStore) Set(cp Checkpoint) error {
	return s.SetMany([]Checkpoint{cp})
}

// SetMany atomically persists all effective checkpoint advances with one temp
// file + fsync + rename. The in-memory snapshot is not published until that
// persistence succeeds.
func (s *fileCheckpointStore) SetMany(checkpoints []Checkpoint) error {
	if len(checkpoints) == 0 {
		return nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	next := cloneCheckpoints(s.entries)
	now := time.Now().UTC()
	changed := false
	for _, cp := range checkpoints {
		key := cp.Identity.TrackingKey()
		if current, ok := next[key]; ok {
			switch {
			case cp.Identity.Generation < current.Identity.Generation:
				continue // a delayed old-generation batch must not undo truncation
			case cp.Identity.Generation == current.Identity.Generation && cp.Offset < current.Offset:
				continue // offsets are monotonic within one generation
			case cp.Identity.Generation == current.Identity.Generation && cp.Offset == current.Offset &&
				checkpointPositionEqual(cp, current):
				continue
			}
		}
		if cp.UpdatedAt.IsZero() {
			cp.UpdatedAt = now
		}
		next[key] = cp
		changed = true
	}
	if !changed {
		return nil
	}
	if err := s.persistEntriesLocked(next); err != nil {
		return err
	}
	s.entries = next
	return nil
}

func checkpointPositionEqual(left, right Checkpoint) bool {
	return left.Identity == right.Identity && left.Path == right.Path &&
		left.Offset == right.Offset && left.LineNumber == right.LineNumber
}

// Delete removes the checkpoint for id, if any, and persists the result.
func (s *fileCheckpointStore) Delete(id FileIdentity) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := id.TrackingKey()
	if _, ok := s.entries[key]; !ok {
		return nil
	}
	next := cloneCheckpoints(s.entries)
	delete(next, key)
	if err := s.persistEntriesLocked(next); err != nil {
		return err
	}
	s.entries = next
	return nil
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
	return checkpointSnapshot(s.entries)
}

func checkpointSnapshot(entries map[string]Checkpoint) []Checkpoint {
	out := make([]Checkpoint, 0, len(entries))
	for _, cp := range entries {
		out = append(out, cp)
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].Identity.String() < out[j].Identity.String()
	})
	return out
}

func cloneCheckpoints(entries map[string]Checkpoint) map[string]Checkpoint {
	cloned := make(map[string]Checkpoint, len(entries))
	for key, cp := range entries {
		cloned[key] = cp
	}
	return cloned
}

// persistEntriesLocked persists entries in deterministic identity order.
func (s *fileCheckpointStore) persistEntriesLocked(entries map[string]Checkpoint) error {
	return s.persistSnapshot(checkpointSnapshot(entries))
}

// writeSnapshot writes the whole store atomically: marshal, write a temp file
// in the same directory, fsync it, rename over the target, then fsync the
// directory so the rename itself is durable. A crash leaves either the old or
// the new complete file, never a torn one.
func (s *fileCheckpointStore) writeSnapshot(checkpoints []Checkpoint) error {
	doc := checkpointDoc{Version: checkpointFormatVersion, Checkpoints: checkpoints}
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
