package input

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/protocolid"
)

// checkpointFileName is the single JSON document holding every checkpoint for a
// store directory.
const checkpointFileName = "checkpoints.json"

// checkpointFormatVersion is written into the store file so a future format
// change can be detected on load.
const checkpointFormatVersion = 2

const maximumCheckpointInputIDBytes = int(protocolid.MaximumBytes)

// checkpointDoc is the on-disk shape of the checkpoint store.
type checkpointDoc struct {
	Version     int          `json:"version"`
	Checkpoints []Checkpoint `json:"checkpoints"`
}

// checkpointKey is deliberately a struct rather than a concatenated string:
// input IDs and tracking keys are independently validated values, and keeping
// them as separate comparable fields makes key boundaries unambiguous.
type checkpointKey struct {
	inputID     string
	trackingKey string
}

// fileCheckpointStore is a CheckpointStore backed by one atomically-rewritten
// JSON file. The whole document is rewritten on every mutation; checkpoint
// counts are bounded by the number of tracked files, so this stays cheap.
type fileCheckpointStore struct {
	dir  string
	path string

	mu              sync.Mutex
	entries         map[checkpointKey]Checkpoint
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
	// #nosec G302 -- dir is a directory and is deliberately owner-only.
	if err := os.Chmod(dir, 0o700); err != nil {
		return nil, fmt.Errorf("collector/input: secure checkpoint dir %s: %w", dir, err)
	}
	s := &fileCheckpointStore{
		dir:     dir,
		path:    filepath.Join(dir, checkpointFileName),
		entries: make(map[checkpointKey]Checkpoint),
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
	switch doc.Version {
	case checkpointFormatVersion:
	case 1:
		if len(doc.Checkpoints) != 0 {
			return fmt.Errorf(
				"collector/input: checkpoint file %s uses unsupported version 1 with %d checkpoints; remove or reset it before starting this greenfield version",
				s.path,
				len(doc.Checkpoints),
			)
		}
		return nil
	default:
		return fmt.Errorf(
			"collector/input: checkpoint file %s uses unsupported version %d",
			s.path,
			doc.Version,
		)
	}
	for _, cp := range doc.Checkpoints {
		if checkpointErr := validateCheckpoint(cp); checkpointErr != nil {
			return fmt.Errorf(
				"collector/input: invalid checkpoint in %s: %w",
				s.path,
				checkpointErr,
			)
		}
		key, keyErr := checkpointKeyFor(cp.InputID, cp.Identity)
		if keyErr != nil {
			return fmt.Errorf("collector/input: invalid checkpoint in %s: %w", s.path, keyErr)
		}
		if _, duplicate := s.entries[key]; duplicate {
			return fmt.Errorf(
				"collector/input: checkpoint file %s contains duplicate input/file checkpoint for input ID %q",
				s.path,
				cp.InputID,
			)
		}
		s.entries[key] = cp
	}
	return nil
}

// Get returns the checkpoint for inputID and id and whether one exists.
func (s *fileCheckpointStore) Get(
	inputID string,
	id FileIdentity,
) (Checkpoint, bool, error) {
	key, err := checkpointKeyFor(inputID, id)
	if err != nil {
		return Checkpoint{}, false, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	cp, ok := s.entries[key]
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
	keys := make([]checkpointKey, len(checkpoints))
	for index, cp := range checkpoints {
		if err := validateCheckpoint(cp); err != nil {
			return fmt.Errorf("collector/input: checkpoint %d: %w", index, err)
		}
		key, err := checkpointKeyFor(cp.InputID, cp.Identity)
		if err != nil {
			return fmt.Errorf("collector/input: checkpoint %d: %w", index, err)
		}
		keys[index] = key
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	next := cloneCheckpoints(s.entries)
	now := time.Now().UTC()
	changed := false
	for index, cp := range checkpoints {
		key := keys[index]
		normalized := false
		if current, ok := next[key]; ok {
			switch {
			case cp.Identity.Generation < current.Identity.Generation:
				continue // a delayed old-generation batch must not undo truncation
			case cp.Identity.Generation == current.Identity.Generation && cp.Offset < current.Offset:
				continue // offsets are monotonic within one generation
			case cp.Identity.Generation == current.Identity.Generation &&
				cp.Offset > current.Offset &&
				cp.NextLineNumber != 0 &&
				current.NextLineNumber != 0 &&
				cp.NextLineNumber <= current.NextLineNumber:
				return errors.New("collector/input: next line number does not advance with checkpoint offset")
			case cp.Identity.Generation == current.Identity.Generation && cp.Offset == current.Offset:
				if cp.LineNumber != current.LineNumber {
					return errors.New("collector/input: conflicting line number at the same checkpoint offset")
				}
				switch {
				case cp.NextLineNumber == 0:
					cp.NextLineNumber = current.NextLineNumber
					normalized = cp.NextLineNumber != 0
				case current.NextLineNumber != 0 && cp.NextLineNumber != current.NextLineNumber:
					return errors.New("collector/input: conflicting next line number at the same checkpoint offset")
				}
				if checkpointPositionEqual(cp, current) {
					continue
				}
			}
		}
		if normalized {
			if err := validateCheckpoint(cp); err != nil {
				return fmt.Errorf(
					"collector/input: checkpoint %d is invalid after monotonic merge: %w",
					index,
					err,
				)
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
	return left.InputID == right.InputID &&
		left.Identity == right.Identity && left.Path == right.Path &&
		left.Offset == right.Offset && left.LineNumber == right.LineNumber &&
		left.NextLineNumber == right.NextLineNumber
}

// Delete removes the checkpoint for inputID and id, if any, and persists the
// result.
func (s *fileCheckpointStore) Delete(inputID string, id FileIdentity) error {
	key, err := checkpointKeyFor(inputID, id)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
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

// List returns all persisted checkpoints, ordered by input and identity for
// determinism.
func (s *fileCheckpointStore) List() ([]Checkpoint, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.snapshotLocked(), nil
}

// Close releases the store. Every mutation already persisted synchronously, so
// there is nothing to flush.
func (s *fileCheckpointStore) Close() error { return nil }

// snapshotLocked returns the entries as an input/identity-sorted slice.
func (s *fileCheckpointStore) snapshotLocked() []Checkpoint {
	return checkpointSnapshot(s.entries)
}

func checkpointSnapshot(entries map[checkpointKey]Checkpoint) []Checkpoint {
	out := make([]Checkpoint, 0, len(entries))
	for _, cp := range entries {
		out = append(out, cp)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].InputID != out[j].InputID {
			return out[i].InputID < out[j].InputID
		}
		return out[i].Identity.String() < out[j].Identity.String()
	})
	return out
}

func cloneCheckpoints(
	entries map[checkpointKey]Checkpoint,
) map[checkpointKey]Checkpoint {
	cloned := make(map[checkpointKey]Checkpoint, len(entries))
	for key, cp := range entries {
		cloned[key] = cp
	}
	return cloned
}

// persistEntriesLocked persists entries in deterministic input/identity order.
func (s *fileCheckpointStore) persistEntriesLocked(
	entries map[checkpointKey]Checkpoint,
) error {
	return s.persistSnapshot(checkpointSnapshot(entries))
}

func checkpointKeyFor(
	inputID string,
	identity FileIdentity,
) (checkpointKey, error) {
	if !ValidInputID(inputID) {
		return checkpointKey{}, fmt.Errorf(
			"input ID %q is not a canonical identifier",
			inputID,
		)
	}
	return checkpointKey{
		inputID:     inputID,
		trackingKey: identity.TrackingKey(),
	}, nil
}

// ValidInputID reports whether inputID uses the collector protocol's canonical
// identifier grammar: 1..128 ASCII bytes, beginning with an alphanumeric byte,
// followed by alphanumeric bytes or '.', '_', ':', and '-'.
func ValidInputID(inputID string) bool {
	return protocolid.Valid(inputID)
}

func validateCheckpoint(checkpoint Checkpoint) error {
	if !ValidInputID(checkpoint.InputID) {
		return fmt.Errorf(
			"input ID %q is not a canonical identifier",
			checkpoint.InputID,
		)
	}
	if _, err := ParseFileIdentity(checkpoint.Identity.String()); err != nil {
		return fmt.Errorf("identity is not canonical: %w", err)
	}
	if checkpoint.Path == "" {
		return errors.New("source path is empty")
	}
	if checkpoint.Offset > math.MaxInt64 {
		return fmt.Errorf(
			"offset %d exceeds maximum supported file offset %d",
			checkpoint.Offset,
			uint64(math.MaxInt64),
		)
	}
	if checkpoint.Identity.FingerprintLength > maximumFingerprintBytes {
		return fmt.Errorf(
			"fingerprint length %d exceeds absolute maximum %d",
			checkpoint.Identity.FingerprintLength,
			maximumFingerprintBytes,
		)
	}
	if checkpoint.Identity.FingerprintLength == 0 &&
		(checkpoint.Identity.Fingerprint != emptyFingerprintSHA256 ||
			checkpoint.Offset != 0) {
		return errors.New(
			"zero-length fingerprint requires the canonical empty digest at offset zero",
		)
	}
	if _, _, err := checkpointNextLine(checkpoint, false); err != nil {
		return fmt.Errorf("line cursor is invalid: %w", err)
	}
	return nil
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
