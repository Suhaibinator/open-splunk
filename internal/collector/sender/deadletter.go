package sender

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"google.golang.org/protobuf/encoding/protojson"
)

// fileDeadLetterSink appends dead-letter records to a JSONL file. Each record is
// one line; the LogEvent is encoded with protojson so it round-trips through the
// canonical proto3 JSON form. Every WriteRecords call fsyncs the file so a
// crash cannot lose a record the sink reported as written.
type fileDeadLetterSink struct {
	mu   sync.Mutex
	file *os.File
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

// NewFileDeadLetterSink opens or creates a JSONL dead-letter file at path,
// creating parent directories as needed. Writes are append-only (O_APPEND).
func NewFileDeadLetterSink(path string) (DeadLetterSink, error) {
	if path == "" {
		return nil, fmt.Errorf("collector/sender: dead-letter path is required")
	}
	if dir := filepath.Dir(path); dir != "" {
		if err := os.MkdirAll(dir, 0o750); err != nil {
			return nil, fmt.Errorf("collector/sender: create dead-letter dir: %w", err)
		}
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o640)
	if err != nil {
		return nil, fmt.Errorf("collector/sender: open dead-letter file: %w", err)
	}
	return &fileDeadLetterSink{file: file}, nil
}

func (s *fileDeadLetterSink) WriteRecords(records []DeadLetterRecord) error {
	if len(records) == 0 {
		return nil
	}
	var buf bytes.Buffer
	marshaler := protojson.MarshalOptions{}
	for _, record := range records {
		line := deadLetterLine{
			BatchID:       record.BatchID,
			BatchSequence: record.BatchSequence,
			Code:          record.Code,
			Reason:        record.Reason,
			RejectedAt:    record.RejectedAt.UTC(),
		}
		if record.Event != nil {
			encoded, err := marshaler.Marshal(record.Event)
			if err != nil {
				return fmt.Errorf("collector/sender: encode dead-letter event: %w", err)
			}
			line.Event = encoded
		}
		encoded, err := json.Marshal(line)
		if err != nil {
			return fmt.Errorf("collector/sender: encode dead-letter record: %w", err)
		}
		buf.Write(encoded)
		buf.WriteByte('\n')
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if _, err := s.file.Write(buf.Bytes()); err != nil {
		return fmt.Errorf("collector/sender: write dead-letter records: %w", err)
	}
	if err := s.file.Sync(); err != nil {
		return fmt.Errorf("collector/sender: sync dead-letter file: %w", err)
	}
	return nil
}

func (s *fileDeadLetterSink) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.file.Close()
}
