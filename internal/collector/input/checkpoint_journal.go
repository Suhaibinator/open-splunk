package input

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"os"
	"path/filepath"

	"fortio.org/safecast"
)

const checkpointJournalName = "checkpoints.journal"
const maximumCheckpointTransactionBytes = 256 << 20
const checkpointJournalHeaderBytes = 12

var checkpointCRC = crc32.MakeTable(crc32.Castagnoli)

type checkpointTransaction struct {
	Sequence    uint64       `json:"sequence"`
	Checkpoints []Checkpoint `json:"checkpoints"`
}

// ReadCheckpoints inspects a running collector without opening a writer,
// repairing a tail, or compacting its journal. Concurrent compaction can return
// an error; monitoring callers may retry. A trailing in-progress append is not
// included. Both checkpoint files must be preserved when backing up state.
func ReadCheckpoints(dir string) ([]Checkpoint, error) {
	s := &fileCheckpointStore{dir: dir, path: filepath.Join(dir, checkpointFileName),
		entries: make(map[checkpointKey]Checkpoint), readOnly: true}
	before, statErr := os.Stat(s.path)
	if statErr != nil && !errors.Is(statErr, os.ErrNotExist) {
		return nil, statErr
	}
	if err := s.load(); err != nil {
		return nil, err
	}
	if err := s.loadCheckpointJournal(); err != nil {
		return nil, err
	}
	if s.journal != nil {
		if err := s.journal.Close(); err != nil {
			return nil, err
		}
	}
	after, err := os.Stat(s.path)
	if errors.Is(statErr, os.ErrNotExist) && errors.Is(err, os.ErrNotExist) {
		return s.snapshotLocked(), nil
	}
	if statErr != nil || err != nil || !os.SameFile(before, after) {
		return nil, errors.New("collector/input: checkpoint snapshot changed during inspection; retry")
	}
	return s.snapshotLocked(), nil
}

// Each record has a checksummed length/payload-CRC header and JSON payload.
// Checking the header prevents a damaged length from looking like a torn tail.
// Only an incomplete final record can
// be discarded: SetMany never acknowledges it until the complete record syncs.
func (s *fileCheckpointStore) loadCheckpointJournal() error {
	path := filepath.Join(s.dir, checkpointJournalName)
	before, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) && !s.journalRequired {
		return nil
	}
	if err != nil {
		return fmt.Errorf("collector/input: inspect checkpoint journal: %w", err)
	}
	if !before.Mode().IsRegular() || before.Mode().Perm() != 0o600 {
		return errors.New("collector/input: checkpoint journal must be a regular mode-0600 file")
	}
	flags := os.O_RDWR
	if s.readOnly {
		flags = os.O_RDONLY
	}
	f, err := os.OpenFile(path, flags, 0o600)
	if errors.Is(err, os.ErrNotExist) && !s.journalRequired {
		return nil
	}
	if err != nil {
		return fmt.Errorf("collector/input: open checkpoint journal: %w", err)
	}
	opened, err := f.Stat()
	if err != nil || !os.SameFile(before, opened) {
		_ = f.Close()
		return errors.Join(errors.New("collector/input: checkpoint journal changed while opening"), err)
	}
	s.journal = f
	if err := s.replayCheckpointJournal(); err != nil {
		_ = f.Close()
		s.journal = nil
		return fmt.Errorf("collector/input: recover checkpoint journal %s: %w", path, err)
	}
	return nil
}

func (s *fileCheckpointStore) replayCheckpointJournal() error {
	var header [checkpointJournalHeaderBytes]byte
	var previousSequence uint64
	baselineSequence := s.journalSequence
	for {
		_, err := io.ReadFull(s.journal, header[:])
		if errors.Is(err, io.EOF) {
			return nil
		}
		if errors.Is(err, io.ErrUnexpectedEOF) {
			return s.truncateCheckpointJournal(s.journalBytes)
		}
		if err != nil {
			return err
		}
		if crc32.Checksum(header[:8], checkpointCRC) != binary.LittleEndian.Uint32(header[8:]) {
			return errors.New("checkpoint transaction header checksum mismatch")
		}
		size := binary.LittleEndian.Uint32(header[:4])
		if size == 0 || size > maximumCheckpointTransactionBytes {
			return errors.New("invalid checkpoint transaction length")
		}
		data := make([]byte, size)
		if _, err := io.ReadFull(s.journal, data); err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				return s.truncateCheckpointJournal(s.journalBytes)
			}
			return err
		}
		if crc32.Checksum(data, checkpointCRC) != binary.LittleEndian.Uint32(header[4:]) {
			return errors.New("checkpoint transaction checksum mismatch")
		}
		var tx checkpointTransaction
		if err := json.Unmarshal(data, &tx); err != nil {
			return err
		}
		if !s.journalRequired {
			return errors.New("checkpoint journal has no durable baseline")
		}
		if tx.Sequence == 0 {
			return errors.New("zero checkpoint transaction sequence")
		}
		if previousSequence != 0 && (previousSequence == math.MaxUint64 || tx.Sequence != previousSequence+1) {
			return errors.New("checkpoint transactions are not consecutive")
		}
		previousSequence = tx.Sequence
		if tx.Sequence > baselineSequence {
			if tx.Sequence != s.journalSequence+1 {
				return errors.New("checkpoint transaction sequence gap")
			}
			for _, cp := range tx.Checkpoints {
				if err := ValidateCheckpoint(cp); err != nil {
					return err
				}
				key, err := checkpointKeyFor(cp.InputID, cp.Identity)
				if err != nil {
					return err
				}
				s.entries[key] = cp
			}
			s.journalSequence = tx.Sequence
		}
		s.journalBytes += int64(len(header)) + int64(size)
	}
}

func (s *fileCheckpointStore) ensureCheckpointJournal() error {
	if s.journalErr != nil {
		return s.journalErr
	}
	if s.journal == nil {
		f, err := os.OpenFile(filepath.Join(s.dir, checkpointJournalName), os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o600)
		if err != nil {
			return err
		}
		s.journal = f
	}
	if !s.journalRequired {
		if err := s.journal.Sync(); err != nil {
			return err
		}
		if err := s.syncDirectory(); err != nil {
			return err
		}
		if err := s.writeSnapshot(s.snapshotLocked()); err != nil {
			return err
		}
		s.journalRequired = true
	}
	return nil
}

func (s *fileCheckpointStore) appendCheckpointUpdates(checkpoints []Checkpoint) error {
	if err := s.ensureCheckpointJournal(); err != nil {
		return err
	}
	// Amortize compaction over at least one snapshot's worth of delta bytes.
	if s.journalBytes >= max(1<<20, s.snapshotBytes) {
		if err := s.compactCheckpointEntries(s.entries); err != nil {
			return err
		}
	}
	if s.journalSequence == math.MaxUint64 {
		return errors.New("checkpoint journal sequence exhausted")
	}
	data, err := json.Marshal(checkpointTransaction{Sequence: s.journalSequence + 1, Checkpoints: checkpoints})
	if err != nil {
		return err
	}
	if len(data) > maximumCheckpointTransactionBytes {
		return errors.New("checkpoint transaction too large")
	}
	record := checkpointJournalRecord(data)
	if err := writeCheckpointData(s.journal, record); err != nil {
		rollbackErr := s.truncateCheckpointJournal(s.journalBytes)
		if rollbackErr != nil {
			s.journalErr = rollbackErr
		}
		return errors.Join(err, rollbackErr)
	}
	if err := s.journal.Sync(); err != nil {
		// Durability is ambiguous: do not append behind an unacknowledged record.
		s.journalErr = err
		return err
	}
	s.journalSequence++
	s.journalBytes += int64(len(record))
	return nil
}

func checkpointJournalRecord(data []byte) []byte {
	record := make([]byte, checkpointJournalHeaderBytes+len(data))
	binary.LittleEndian.PutUint32(record[:4], safecast.MustConv[uint32](len(data)))
	binary.LittleEndian.PutUint32(record[4:8], crc32.Checksum(data, checkpointCRC))
	binary.LittleEndian.PutUint32(record[8:12], crc32.Checksum(record[:8], checkpointCRC))
	copy(record[checkpointJournalHeaderBytes:], data)
	return record
}

func (s *fileCheckpointStore) compactCheckpointEntries(entries map[checkpointKey]Checkpoint) error {
	if err := s.ensureCheckpointJournal(); err != nil {
		return err
	}
	if err := s.writeSnapshot(checkpointSnapshot(entries)); err != nil {
		// A failed directory sync can leave the new snapshot visible. Stop
		// writes so a failed deletion cannot later be silently resurrected.
		s.journalErr = err
		return err
	}
	// The durable snapshot records the sequence before journal reclamation.
	// A crash here leaves redundant records, which replay skips by sequence.
	if err := s.truncateCheckpointJournal(0); err != nil {
		s.journalErr = err
		return err
	}
	s.journalBytes = 0
	return nil
}

func (s *fileCheckpointStore) truncateCheckpointJournal(size int64) error {
	if s.readOnly {
		return nil
	}
	if err := s.journal.Truncate(size); err != nil {
		return err
	}
	if _, err := s.journal.Seek(size, io.SeekStart); err != nil {
		return err
	}
	return s.journal.Sync()
}
