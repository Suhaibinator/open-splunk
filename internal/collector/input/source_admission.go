package input

import (
	"encoding/json"
	"errors"
	"math"
	"strings"
	"sync"
	"time"
)

const (
	// These are collector-wide limits, shared by every input and WAL resume view.
	maximumLiveSources       = 1024
	maximumCheckpointEntries = 131072
)

var errSourceCapacity = errors.New("collector/input: live source capacity reached; new sources will be retried")
var errCheckpointCapacity = errors.New("collector/input: retained source identity capacity reached; historical checkpoints must be preserved")

// TryAcquireSource reserves one complete source lifecycle, including draining
// and blocked publication. The caller releases it only after closing the file.
func (s *fileCheckpointStore) TryAcquireSource() (func(), bool) {
	s.mu.Lock()
	if s.sourceSlots == nil {
		s.sourceSlots = make(chan struct{}, maximumLiveSources)
	}
	slots := s.sourceSlots
	s.mu.Unlock()
	select {
	case slots <- struct{}{}:
		return sync.OnceFunc(func() { <-slots }), true
	default:
		return nil, false
	}
}

// ReservePending keeps WAL-only identities available for eventual terminal
// writes without persisting their nonterminal positions. Restart reconstructs
// these reservations from the durable WAL before any manager can discover files.
func (s *fileCheckpointStore) ReservePending(checkpoints []Checkpoint) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, _, err := s.checkSnapshotCapacity(nil); err != nil {
		return err
	}
	next := make(map[checkpointKey]int)
	bytes := s.entryBytes
	for _, size := range s.reserved {
		bytes += size
	}
	for _, cp := range checkpoints {
		if err := ValidateCheckpoint(cp); err != nil {
			return err
		}
		key, err := checkpointKeyFor(cp.InputID, cp.Identity)
		if err != nil {
			return err
		}
		if _, exists := s.entries[key]; exists {
			continue
		}
		if _, reserved := s.reserved[key]; reserved {
			continue
		}
		size, err := checkpointSnapshotEntryBytes(cp)
		if err != nil {
			return err
		}
		bytes += size - next[key]
		next[key] = size
		if len(s.entries)+len(s.reserved)+len(next) > s.entryLimit() {
			return errCheckpointCapacity
		}
		if bytes > s.snapshotLimit()-1024 {
			return errors.New("collector/input: pending checkpoints exceed snapshot byte capacity")
		}
	}
	if s.reserved == nil {
		s.reserved = make(map[checkpointKey]int)
	}
	for key, size := range next {
		s.reserved[key] = size
	}
	return nil
}

func (s *fileCheckpointStore) entryLimit() int {
	if s.maximumEntries > 0 {
		return s.maximumEntries
	}
	return maximumCheckpointEntries
}

func (s *fileCheckpointStore) snapshotLimit() int {
	if s.maximumSnapshotBytes > 0 {
		return s.maximumSnapshotBytes
	}
	return maximumCheckpointSnapshotBytes
}

func (s *fileCheckpointStore) checkNewEntries(next map[checkpointKey]Checkpoint) error {
	count := len(s.entries) + len(s.reserved)
	for key := range next {
		if _, exists := s.entries[key]; exists {
			continue
		}
		if _, reserved := s.reserved[key]; reserved {
			continue
		}
		count++
		if count > s.entryLimit() {
			return errCheckpointCapacity
		}
	}
	return nil
}

// checkSnapshotCapacity fences mutations before their journal append so every
// accepted state can still be compacted and reopened under the snapshot bound.
// Cache entry footprints: ordinary terminal advances remain proportional to
// the changed sources, not the whole retained history.
func (s *fileCheckpointStore) checkSnapshotCapacity(next map[checkpointKey]Checkpoint) (map[checkpointKey]int, int, error) {
	if s.entrySizes == nil {
		s.entrySizes = make(map[checkpointKey]int, len(s.entries))
		s.entryBytes = 0
		for key, cp := range s.entries {
			size, err := checkpointSnapshotEntryBytes(cp)
			if err != nil {
				return nil, 0, err
			}
			s.entrySizes[key] = size
			s.entryBytes += size
		}
	}
	sizes := make(map[checkpointKey]int, len(next))
	total := s.entryBytes
	for key, cp := range next {
		size, err := checkpointSnapshotEntryBytes(cp)
		if err != nil {
			return nil, 0, err
		}
		total += size - s.entrySizes[key]
		sizes[key] = size
	}
	// Reserve the document wrapper and maximum-width sequence/version fields.
	reservedBytes := 0
	for key, size := range s.reserved {
		if _, committing := next[key]; !committing {
			reservedBytes += size
		}
	}
	if total+reservedBytes > s.snapshotLimit()-1024 {
		return nil, 0, errors.New("collector/input: checkpoints exceed snapshot byte capacity")
	}
	return sizes, total, nil
}

func checkpointSnapshotEntryBytes(cp Checkpoint) (int, error) {
	// Reserve maximum-width position metadata up front so terminal advances,
	// guard enrichment and generation changes cannot exhaust byte capacity.
	cp.Identity = FileIdentity{
		Device: math.MaxUint64, Inode: math.MaxUint64, Generation: math.MaxUint64,
		Fingerprint: strings.Repeat("f", 64), FingerprintLength: math.MaxUint32,
	}
	cp.Offset, cp.LineNumber, cp.NextLineNumber = math.MaxUint64, math.MaxUint64, math.MaxUint64
	cp.GuardFingerprint, cp.GuardLength = strings.Repeat("f", 64), math.MaxUint32
	cp.UpdatedAt = time.Date(9999, 12, 31, 23, 59, 59, 999999999, time.FixedZone("", 23*3600+59*60))
	data, err := json.MarshalIndent(cp, "    ", "  ")
	// Include the first line's array indentation, comma, and newline.
	return len(data) + 6, err
}
