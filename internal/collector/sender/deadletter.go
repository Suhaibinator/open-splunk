package sender

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"time"

	"google.golang.org/protobuf/encoding/protojson"
)

// ErrFileDeadLetterSinkClosed is returned by WriteRecords after the file sink
// has been closed.
var ErrFileDeadLetterSinkClosed = errors.New("collector/sender: file dead-letter sink is closed")

// FileDeadLetterSinkOptions controls optional size-based rotation. The zero
// value preserves the unbounded, append-only behavior of NewFileDeadLetterSink.
type FileDeadLetterSinkOptions struct {
	// MaxBytes rotates the active file before writing a record that would make
	// it exceed this size. Zero disables rotation. A single JSONL record larger
	// than MaxBytes is kept intact and may make a file exceed the limit.
	MaxBytes int64
	// MaxBackups is the number of rotated files to retain. Rotated files are
	// named path.1 (newest) through path.MaxBackups (oldest). When MaxBytes is
	// set and MaxBackups is zero, rotation discards the previous active file.
	MaxBackups int
}

// fileDeadLetterSink appends dead-letter records to a JSONL file. Each record is
// one line; the LogEvent is encoded with protojson so it round-trips through the
// canonical proto3 JSON form. Every WriteRecords call fsyncs the file so a
// crash cannot lose a record the sink reported as written.
type fileDeadLetterSink struct {
	mu       sync.Mutex
	file     *os.File
	path     string
	dir      string
	size     int64
	options  FileDeadLetterSinkOptions
	closed   bool
	failed   error
	syncFile func(*os.File) error
}

// deadLetterLine is the on-disk JSON shape. Event holds the protojson-encoded
// LogEvent as a raw message so it is not re-encoded by encoding/json.
type deadLetterLine struct {
	BatchID       string          `json:"batch_id"`
	BatchSequence uint64          `json:"batch_sequence"`
	Code          string          `json:"code"`
	Reason        string          `json:"reason,omitempty"`
	RejectedAt    time.Time       `json:"rejected_at"`
	Event         json.RawMessage `json:"event,omitempty"`
}

// NewFileDeadLetterSink opens or creates an unbounded JSONL dead-letter file at
// path, creating parent directories as needed. Writes are append-only.
func NewFileDeadLetterSink(path string) (DeadLetterSink, error) {
	return NewFileDeadLetterSinkWithOptions(path, FileDeadLetterSinkOptions{})
}

// NewFileDeadLetterSinkWithOptions opens or creates a JSONL dead-letter file
// with optional size-based rotation. On open, an unterminated final line is
// truncated so a write interrupted by a process or machine crash cannot corrupt
// the next record.
func NewFileDeadLetterSinkWithOptions(path string, options FileDeadLetterSinkOptions) (DeadLetterSink, error) {
	if path == "" {
		return nil, fmt.Errorf("collector/sender: dead-letter path is required")
	}
	if options.MaxBytes < 0 {
		return nil, fmt.Errorf("collector/sender: dead-letter max bytes must not be negative")
	}
	if options.MaxBackups < 0 {
		return nil, fmt.Errorf("collector/sender: dead-letter max backups must not be negative")
	}
	if options.MaxBytes == 0 && options.MaxBackups != 0 {
		return nil, fmt.Errorf("collector/sender: dead-letter max backups requires max bytes")
	}

	path = filepath.Clean(path)
	dir := filepath.Dir(path)
	if err := makeDirectoriesDurable(dir); err != nil {
		return nil, fmt.Errorf("collector/sender: create dead-letter dir: %w", err)
	}

	file, size, err := openDeadLetterFile(path, dir)
	if err != nil {
		return nil, err
	}
	return &fileDeadLetterSink{
		file:     file,
		path:     path,
		dir:      dir,
		size:     size,
		options:  options,
		syncFile: func(file *os.File) error { return file.Sync() },
	}, nil
}

// makeDirectoriesDurable creates each missing directory privately and fsyncs
// its parent before moving on to the next component. Existing directories are
// deliberately not chmodded because they may be shared by unrelated data.
func makeDirectoriesDurable(path string) error {
	path = filepath.Clean(path)
	var missing []string
	for current := path; ; current = filepath.Dir(current) {
		info, err := os.Stat(current)
		if err == nil {
			if !info.IsDir() {
				return fmt.Errorf("%s is not a directory", current)
			}
			break
		}
		if !errors.Is(err, os.ErrNotExist) {
			return err
		}
		missing = append(missing, current)
		parent := filepath.Dir(current)
		if parent == current {
			return err
		}
	}

	for _, component := range slices.Backward(missing) {
		created := false
		if err := os.Mkdir(component, 0o700); err != nil {
			if !errors.Is(err, os.ErrExist) {
				return err
			}
			info, statErr := os.Stat(component)
			if statErr != nil {
				return statErr
			}
			if !info.IsDir() {
				return fmt.Errorf("%s is not a directory", component)
			}
		} else {
			created = true
		}
		if created {
			if err := syncDirectory(filepath.Dir(component)); err != nil {
				return err
			}
		}
	}
	return nil
}

func openDeadLetterFile(path, dir string) (_ *os.File, _ int64, returnErr error) {
	// O_RDWR is needed to inspect and repair a torn tail. O_APPEND keeps writes
	// append-only even if another descriptor changes the shared file offset.
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o600)
	if err != nil {
		return nil, 0, fmt.Errorf("collector/sender: open dead-letter file: %w", err)
	}
	defer func() {
		if returnErr != nil {
			_ = file.Close()
		}
	}()

	info, err := file.Stat()
	if err != nil {
		return nil, 0, fmt.Errorf("collector/sender: stat dead-letter file: %w", err)
	}
	if !info.Mode().IsRegular() {
		return nil, 0, fmt.Errorf("collector/sender: dead-letter path is not a regular file")
	}
	// Rejected events may contain credentials or other sensitive payloads. Tighten
	// permissions on existing files as well as applying 0600 at creation time.
	if info.Mode().Perm() != 0o600 {
		if err := file.Chmod(0o600); err != nil {
			return nil, 0, fmt.Errorf("collector/sender: secure dead-letter file: %w", err)
		}
	}

	size, err := repairDeadLetterTail(file, info.Size())
	if err != nil {
		return nil, 0, err
	}
	// Syncing the file persists a repair or permission change (and is harmless
	// for an unchanged file). Syncing the directory publishes a newly-created
	// entry before the constructor reports success. These are unconditional so
	// a racing creator cannot make us incorrectly skip either durability step.
	if err := file.Sync(); err != nil {
		return nil, 0, fmt.Errorf("collector/sender: sync dead-letter file: %w", err)
	}
	if err := syncDirectory(dir); err != nil {
		return nil, 0, fmt.Errorf("collector/sender: sync dead-letter dir: %w", err)
	}
	return file, size, nil
}

// repairDeadLetterTail returns the byte offset immediately after the last
// newline. A nonempty suffix after that offset is an incomplete JSONL record and
// is removed. Complete prior records are retained even when the suffix spans
// multiple reads.
func repairDeadLetterTail(file *os.File, size int64) (int64, error) {
	if size == 0 {
		return 0, nil
	}

	var last [1]byte
	if _, err := file.ReadAt(last[:], size-1); err != nil {
		return 0, fmt.Errorf("collector/sender: inspect dead-letter tail: %w", err)
	}
	if last[0] == '\n' {
		return size, nil
	}

	const scanBlockBytes = 32 * 1024
	end := size
	truncateAt := int64(0)
	for end > 0 {
		start := max(int64(0), end-scanBlockBytes)
		block := make([]byte, end-start)
		if _, err := file.ReadAt(block, start); err != nil && !errors.Is(err, io.EOF) {
			return 0, fmt.Errorf("collector/sender: inspect dead-letter tail: %w", err)
		}
		if index := bytes.LastIndexByte(block, '\n'); index >= 0 {
			truncateAt = start + int64(index) + 1
			break
		}
		end = start
	}

	if err := file.Truncate(truncateAt); err != nil {
		return 0, fmt.Errorf("collector/sender: repair dead-letter tail: %w", err)
	}
	// openDeadLetterFile performs one unconditional Sync after repair and mode
	// hardening, so syncing the same descriptor here would add no durability.
	return truncateAt, nil
}

func encodeDeadLetterRecord(record DeadLetterRecord) ([]byte, error) {
	line := deadLetterLine{
		BatchID:       record.BatchID,
		BatchSequence: record.BatchSequence,
		Code:          record.Code,
		Reason:        record.Reason,
		RejectedAt:    record.RejectedAt.UTC(),
	}
	if record.Event != nil {
		encoded, err := (protojson.MarshalOptions{}).Marshal(record.Event)
		if err != nil {
			return nil, fmt.Errorf("collector/sender: encode dead-letter event: %w", err)
		}
		line.Event = encoded
	}
	encoded, err := json.Marshal(line)
	if err != nil {
		return nil, fmt.Errorf("collector/sender: encode dead-letter record: %w", err)
	}
	return append(encoded, '\n'), nil
}

func (s *fileDeadLetterSink) WriteRecords(records []DeadLetterRecord) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return ErrFileDeadLetterSinkClosed
	}
	if s.failed != nil {
		return fmt.Errorf("collector/sender: file dead-letter sink is unavailable: %w", s.failed)
	}
	if len(records) == 0 {
		return nil
	}

	encodedRecords := make([][]byte, len(records))
	for index, record := range records {
		encoded, err := encodeDeadLetterRecord(record)
		if err != nil {
			return err
		}
		encodedRecords[index] = encoded
	}

	dirty := false
	for _, encoded := range encodedRecords {
		encodedSize := int64(len(encoded))
		wouldExceedLimit := s.options.MaxBytes > 0 && s.size > 0 &&
			(encodedSize > s.options.MaxBytes || s.size > s.options.MaxBytes-encodedSize)
		if wouldExceedLimit {
			// A retained backup needs its newly written records synced before the
			// rename publishes it. With no backups, rotation deliberately truncates
			// those records, so syncing them immediately before truncation is wasted.
			if dirty && s.options.MaxBackups > 0 {
				if err := s.syncLocked(); err != nil {
					s.failed = fmt.Errorf("collector/sender: sync dead-letter file before rotation: %w", err)
					return s.failed
				}
			}
			if err := s.rotateLocked(); err != nil {
				s.failed = err
				return err
			}
		}

		startSize := s.size
		n, writeErr := s.file.Write(encoded)
		s.size += int64(n)
		if writeErr != nil || n != len(encoded) {
			if writeErr == nil {
				writeErr = io.ErrShortWrite
			}
			// Do not leave a partial line that would poison a later write in the
			// same process. If rollback itself fails, make the sink terminal.
			if truncateErr := s.file.Truncate(startSize); truncateErr != nil {
				s.failed = errors.Join(writeErr, truncateErr)
				return fmt.Errorf("collector/sender: write dead-letter record and roll back partial line: %w", s.failed)
			}
			s.size = startSize
			if syncErr := s.syncLocked(); syncErr != nil {
				s.failed = errors.Join(writeErr, syncErr)
				return fmt.Errorf("collector/sender: write dead-letter record and sync rollback: %w", s.failed)
			}
			return fmt.Errorf("collector/sender: write dead-letter record: %w", writeErr)
		}
		dirty = true
	}

	if dirty {
		if err := s.syncLocked(); err != nil {
			s.failed = fmt.Errorf("collector/sender: sync dead-letter file: %w", err)
			return s.failed
		}
	}
	return nil
}

func (s *fileDeadLetterSink) rotateLocked() error {
	// When backups are retained, the caller syncs newly written records before
	// entering rotation; otherwise the previous successful WriteRecords call
	// already synced the active file. With no backups, the truncation itself is
	// the only state that must become durable.
	if s.options.MaxBackups == 0 {
		if err := s.file.Truncate(0); err != nil {
			return fmt.Errorf("collector/sender: truncate dead-letter file for rotation: %w", err)
		}
		if err := s.syncLocked(); err != nil {
			return fmt.Errorf("collector/sender: sync rotated dead-letter file: %w", err)
		}
		s.size = 0
		return nil
	}

	closeErr := s.file.Close()
	s.file = nil
	if closeErr != nil {
		return fmt.Errorf("collector/sender: close dead-letter file for rotation: %w", closeErr)
	}

	oldest := rotatedDeadLetterPath(s.path, s.options.MaxBackups)
	if err := os.Remove(oldest); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("collector/sender: remove oldest dead-letter backup: %w", err)
	}
	for index := s.options.MaxBackups - 1; index >= 1; index-- {
		from := rotatedDeadLetterPath(s.path, index)
		to := rotatedDeadLetterPath(s.path, index+1)
		if err := os.Rename(from, to); err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("collector/sender: rotate dead-letter backup %d: %w", index, err)
		}
	}
	if err := os.Rename(s.path, rotatedDeadLetterPath(s.path, 1)); err != nil {
		return fmt.Errorf("collector/sender: rotate active dead-letter file: %w", err)
	}
	if err := syncDirectory(s.dir); err != nil {
		return fmt.Errorf("collector/sender: sync dead-letter rotation: %w", err)
	}

	file, size, err := openDeadLetterFile(s.path, s.dir)
	if err != nil {
		return err
	}
	s.file = file
	s.size = size
	return nil
}

func (s *fileDeadLetterSink) syncLocked() error {
	if s.syncFile != nil {
		return s.syncFile(s.file)
	}
	return s.file.Sync()
}

func rotatedDeadLetterPath(path string, index int) string {
	return fmt.Sprintf("%s.%d", path, index)
}

func syncDirectory(path string) error {
	directory, err := os.Open(path)
	if err != nil {
		return err
	}
	syncErr := directory.Sync()
	closeErr := directory.Close()
	return errors.Join(syncErr, closeErr)
}

func (s *fileDeadLetterSink) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	if s.file == nil {
		return nil
	}
	syncErr := s.syncLocked()
	closeErr := s.file.Close()
	s.file = nil
	if err := errors.Join(syncErr, closeErr); err != nil {
		return fmt.Errorf("collector/sender: close dead-letter file: %w", err)
	}
	return nil
}
