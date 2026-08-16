package sender

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
)

func TestFileDeadLetterSinkAppendsJSONL(t *testing.T) {
	t.Parallel()
	// Path with a not-yet-existing parent directory to exercise MkdirAll.
	path := filepath.Join(t.TempDir(), "nested", "dead-letter.jsonl")

	sink, err := NewFileDeadLetterSink(path)
	if err != nil {
		t.Fatal(err)
	}

	rejectedAt := time.Date(2026, 7, 21, 12, 0, 0, 0, time.UTC)
	if err := sink.WriteRecords([]DeadLetterRecord{{
		Event:         &opensplunkv1.LogEvent{EventId: "e1", IndexName: "main"},
		BatchID:       "batch-1",
		BatchSequence: 7,
		Code:          "EVENT_REJECTION_CODE_UNAUTHORIZED_INDEX",
		Reason:        "not allowed",
		RejectedAt:    rejectedAt,
	}}); err != nil {
		t.Fatal(err)
	}
	// A second call must append, not truncate.
	if err := sink.WriteRecords([]DeadLetterRecord{{
		Event:         &opensplunkv1.LogEvent{EventId: "e2"},
		BatchID:       "batch-1",
		BatchSequence: 8,
		Code:          "BATCH_REJECTION_CODE_BATCH_TOO_LARGE",
		RejectedAt:    rejectedAt,
	}}); err != nil {
		t.Fatal(err)
	}
	if err := sink.Close(); err != nil {
		t.Fatal(err)
	}

	file, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()

	var lines []map[string]any
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		var m map[string]any
		if err := json.Unmarshal(scanner.Bytes(), &m); err != nil {
			t.Fatalf("line is not valid JSON: %v (%s)", err, scanner.Text())
		}
		lines = append(lines, m)
	}
	if err := scanner.Err(); err != nil {
		t.Fatal(err)
	}
	if len(lines) != 2 {
		t.Fatalf("dead-letter file has %d lines, want 2", len(lines))
	}
	if lines[0]["batch_id"] != "batch-1" || lines[0]["code"] != "EVENT_REJECTION_CODE_UNAUTHORIZED_INDEX" {
		t.Fatalf("first line = %#v", lines[0])
	}
	// Event was encoded with protojson (camelCase field names, string event_id).
	event, ok := lines[0]["event"].(map[string]any)
	if !ok || event["eventId"] != "e1" || event["indexName"] != "main" {
		t.Fatalf("first line event = %#v", lines[0]["event"])
	}
	if lines[1]["batch_sequence"].(float64) != 8 {
		t.Fatalf("second line sequence = %v", lines[1]["batch_sequence"])
	}

	fileInfo, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if got := fileInfo.Mode().Perm(); got != 0o600 {
		t.Fatalf("dead-letter file mode = %o, want 600", got)
	}
	dirInfo, err := os.Stat(filepath.Dir(path))
	if err != nil {
		t.Fatal(err)
	}
	if got := dirInfo.Mode().Perm() & 0o077; got != 0 {
		t.Fatalf("new dead-letter directory grants group/world permissions %o", got)
	}
}

func TestFileDeadLetterSinkRepairsTornTail(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		prefix    []byte
		wantFirst string
	}{
		{
			name:      "preserves_complete_lines",
			prefix:    []byte("{\"batch_id\":\"preserved\"}\n"),
			wantFirst: "preserved",
		},
		{
			name: "removes_wholly_incomplete_file",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "dead-letter.jsonl")
			// Make the torn suffix larger than the repair scan block so recovery
			// deterministically exercises its backwards, multi-block search.
			torn := append(bytes.Clone(test.prefix), []byte("{\"batch_id\":\"torn\",\"padding\":\"")...)
			torn = append(torn, bytes.Repeat([]byte("x"), 40*1024)...)
			if err := os.WriteFile(path, torn, 0o644); err != nil {
				t.Fatal(err)
			}
			if err := os.Chmod(path, 0o644); err != nil {
				t.Fatal(err)
			}

			sink, err := NewFileDeadLetterSink(path)
			if err != nil {
				t.Fatal(err)
			}
			if err := sink.WriteRecords([]DeadLetterRecord{{
				BatchID:    "after-recovery",
				Code:       "TEST_REJECTION",
				RejectedAt: time.Date(2026, 7, 21, 12, 0, 0, 0, time.UTC),
			}}); err != nil {
				t.Fatal(err)
			}
			if err := sink.Close(); err != nil {
				t.Fatal(err)
			}

			contents, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			if bytes.Contains(contents, []byte("torn")) || bytes.Contains(contents, bytes.Repeat([]byte("x"), 1024)) {
				t.Fatalf("repaired file retained torn suffix: %q", contents)
			}
			lines := decodeDeadLetterTestLines(t, contents)
			wantLines := 1
			if test.wantFirst != "" {
				wantLines++
			}
			if len(lines) != wantLines {
				t.Fatalf("repaired file has %d lines, want %d: %q", len(lines), wantLines, contents)
			}
			if test.wantFirst != "" {
				if got := lines[0]["batch_id"]; got != test.wantFirst {
					t.Fatalf("preserved batch ID = %v, want %q", got, test.wantFirst)
				}
			}
			if got := lines[len(lines)-1]["batch_id"]; got != "after-recovery" {
				t.Fatalf("new batch ID = %v, want after-recovery", got)
			}
			info, err := os.Stat(path)
			if err != nil {
				t.Fatal(err)
			}
			if got := info.Mode().Perm(); got != 0o600 {
				t.Fatalf("repaired file mode = %o, want 600", got)
			}
		})
	}
}

func TestFileDeadLetterSinkRotatesAndLimitsBackups(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "dead-letter.jsonl")
	rejectedAt := time.Date(2026, 7, 21, 12, 0, 0, 0, time.UTC)
	records := make([]DeadLetterRecord, 4)
	for index := range records {
		records[index] = DeadLetterRecord{
			Event:         &opensplunkv1.LogEvent{EventId: fmt.Sprintf("e%d", index+1)},
			BatchID:       "batch",
			BatchSequence: uint64(index + 1),
			Code:          "TEST_REJECTION",
			RejectedAt:    rejectedAt,
		}
	}
	firstLine, err := encodeDeadLetterRecord(records[0])
	if err != nil {
		t.Fatal(err)
	}
	for index := 1; index < len(records); index++ {
		line, err := encodeDeadLetterRecord(records[index])
		if err != nil {
			t.Fatal(err)
		}
		if len(line) != len(firstLine) {
			t.Fatalf("test records have unequal encoded sizes %d and %d", len(firstLine), len(line))
		}
	}

	sink, err := NewFileDeadLetterSinkWithOptions(path, FileDeadLetterSinkOptions{
		MaxBytes:   int64(len(firstLine)),
		MaxBackups: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	// One call exercises rotation between records, not just between calls.
	if err := sink.WriteRecords(records); err != nil {
		t.Fatal(err)
	}
	if err := sink.Close(); err != nil {
		t.Fatal(err)
	}

	for _, test := range []struct {
		path        string
		wantEventID string
	}{
		{path: path, wantEventID: "e4"},
		{path: path + ".1", wantEventID: "e3"},
		{path: path + ".2", wantEventID: "e2"},
	} {
		contents, err := os.ReadFile(test.path)
		if err != nil {
			t.Fatal(err)
		}
		lines := decodeDeadLetterTestLines(t, contents)
		if len(lines) != 1 {
			t.Fatalf("%s has %d lines, want 1", test.path, len(lines))
		}
		event, ok := lines[0]["event"].(map[string]any)
		if !ok || event["eventId"] != test.wantEventID {
			t.Fatalf("%s event = %#v, want %q", test.path, lines[0]["event"], test.wantEventID)
		}
		info, err := os.Stat(test.path)
		if err != nil {
			t.Fatal(err)
		}
		if got := info.Mode().Perm(); got != 0o600 {
			t.Fatalf("%s mode = %o, want 600", test.path, got)
		}
	}
	if _, err := os.Stat(path + ".3"); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("third backup exists or stat failed unexpectedly: %v", err)
	}
}

func TestFileDeadLetterSinkRotationAvoidsRedundantFileSync(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "dead-letter.jsonl")
	rejectedAt := time.Date(2026, 7, 21, 12, 0, 0, 0, time.UTC)
	records := []DeadLetterRecord{
		{BatchID: "a", Code: "TEST", RejectedAt: rejectedAt},
		{BatchID: "b", Code: "TEST", RejectedAt: rejectedAt},
	}
	line, err := encodeDeadLetterRecord(records[0])
	if err != nil {
		t.Fatal(err)
	}
	secondLine, err := encodeDeadLetterRecord(records[1])
	if err != nil {
		t.Fatal(err)
	}
	if len(secondLine) != len(line) {
		t.Fatalf("test records have unequal encoded sizes %d and %d", len(line), len(secondLine))
	}

	sink, err := NewFileDeadLetterSinkWithOptions(path, FileDeadLetterSinkOptions{
		MaxBytes: int64(len(line)),
	})
	if err != nil {
		t.Fatal(err)
	}
	fileSink := sink.(*fileDeadLetterSink)
	fileSyncs := 0
	fileSink.mu.Lock()
	fileSink.syncFile = func(file *os.File) error {
		fileSyncs++
		return file.Sync()
	}
	fileSink.mu.Unlock()

	// One call writes, rotates by truncation, and writes again. The first record
	// is deliberately discarded, so durability needs exactly the truncate sync
	// and the final record sync; syncing record one immediately before discarding
	// it would be redundant.
	if err := sink.WriteRecords(records); err != nil {
		t.Fatal(err)
	}
	if fileSyncs != 2 {
		t.Fatalf("file syncs during write/rotation = %d, want 2", fileSyncs)
	}
	if err := sink.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestFileDeadLetterSinkSyncsDirtyFileBeforeRetainedRotation(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "dead-letter.jsonl")
	rejectedAt := time.Date(2026, 7, 21, 12, 0, 0, 0, time.UTC)
	records := []DeadLetterRecord{
		{BatchID: "a", Code: "TEST", RejectedAt: rejectedAt},
		{BatchID: "b", Code: "TEST", RejectedAt: rejectedAt},
	}
	line, err := encodeDeadLetterRecord(records[0])
	if err != nil {
		t.Fatal(err)
	}

	sink, err := NewFileDeadLetterSinkWithOptions(path, FileDeadLetterSinkOptions{
		MaxBytes: int64(len(line)), MaxBackups: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	fileSink := sink.(*fileDeadLetterSink)
	fileSyncs := 0
	fileSink.mu.Lock()
	fileSink.syncFile = func(file *os.File) error {
		fileSyncs++
		return file.Sync()
	}
	fileSink.mu.Unlock()

	if err := sink.WriteRecords(records); err != nil {
		t.Fatal(err)
	}
	if fileSyncs != 2 {
		t.Fatalf("file syncs during retained rotation = %d, want pre-rotation and final sync", fileSyncs)
	}
	backup, err := os.ReadFile(path + ".1")
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(backup, []byte(`"batch_id":"a"`)) {
		t.Fatalf("retained backup = %q, want durable first record", backup)
	}
	if err := sink.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestFileDeadLetterSinkSerializesCloseAndWrites(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "dead-letter.jsonl")
	sink, err := NewFileDeadLetterSink(path)
	if err != nil {
		t.Fatal(err)
	}

	const writers = 32
	start := make(chan struct{})
	errorsByWriter := make(chan error, writers)
	var group sync.WaitGroup
	for index := range writers {
		group.Go(func() {
			<-start
			errorsByWriter <- sink.WriteRecords([]DeadLetterRecord{{
				BatchID:    fmt.Sprintf("batch-%d", index),
				Code:       "TEST_REJECTION",
				RejectedAt: time.Date(2026, 7, 21, 12, 0, 0, 0, time.UTC),
			}})
		})
	}
	close(start)
	if err := sink.Close(); err != nil {
		t.Fatal(err)
	}
	group.Wait()
	close(errorsByWriter)
	for err := range errorsByWriter {
		if err != nil && !errors.Is(err, ErrFileDeadLetterSinkClosed) {
			t.Fatalf("concurrent write returned non-lifecycle error: %v", err)
		}
	}

	if err := sink.WriteRecords(nil); !errors.Is(err, ErrFileDeadLetterSinkClosed) {
		t.Fatalf("empty write after close error = %v, want ErrFileDeadLetterSinkClosed", err)
	}
	if err := sink.WriteRecords([]DeadLetterRecord{{}}); !errors.Is(err, ErrFileDeadLetterSinkClosed) {
		t.Fatalf("write after close error = %v, want ErrFileDeadLetterSinkClosed", err)
	}
	if err := sink.Close(); err != nil {
		t.Fatalf("second Close = %v, want nil", err)
	}
}

func TestFileDeadLetterSinkOptionsValidation(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "dead-letter.jsonl")
	for _, options := range []FileDeadLetterSinkOptions{
		{MaxBytes: -1},
		{MaxBackups: -1},
		{MaxBackups: 1},
	} {
		if _, err := NewFileDeadLetterSinkWithOptions(path, options); err == nil {
			t.Fatalf("NewFileDeadLetterSinkWithOptions(%+v) succeeded, want error", options)
		}
	}
}

func TestFileDeadLetterSinkSyncFailureIsSticky(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "dead-letter.jsonl")
	sink, err := NewFileDeadLetterSink(path)
	if err != nil {
		t.Fatal(err)
	}
	fileSink := sink.(*fileDeadLetterSink)
	syncFailure := errors.New("injected dead-letter fsync failure")
	fileSink.mu.Lock()
	fileSink.syncFile = func(*os.File) error { return syncFailure }
	fileSink.mu.Unlock()
	record := DeadLetterRecord{BatchID: "batch", Code: "TEST", RejectedAt: time.Now()}
	if err := sink.WriteRecords([]DeadLetterRecord{record}); !errors.Is(err, syncFailure) {
		t.Fatalf("WriteRecords sync failure = %v, want injected failure", err)
	}
	before, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := sink.WriteRecords([]DeadLetterRecord{record}); !errors.Is(err, syncFailure) {
		t.Fatalf("second WriteRecords = %v, want sticky injected failure", err)
	}
	after, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if after.Size() != before.Size() {
		t.Fatalf("sticky failure appended duplicate bytes: size %d -> %d", before.Size(), after.Size())
	}
	fileSink.mu.Lock()
	fileSink.syncFile = func(file *os.File) error { return file.Sync() }
	fileSink.mu.Unlock()
	if err := sink.Close(); err != nil {
		t.Fatalf("Close after restoring sync: %v", err)
	}
}

func decodeDeadLetterTestLines(t *testing.T, contents []byte) []map[string]any {
	t.Helper()
	var lines []map[string]any
	scanner := bufio.NewScanner(bytes.NewReader(contents))
	for scanner.Scan() {
		var line map[string]any
		if err := json.Unmarshal(scanner.Bytes(), &line); err != nil {
			t.Fatalf("line is not valid JSON: %v (%s)", err, scanner.Text())
		}
		lines = append(lines, line)
	}
	if err := scanner.Err(); err != nil {
		t.Fatal(err)
	}
	return lines
}
