package sender

import (
	"bufio"
	"encoding/json"
	"os"
	"path/filepath"
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
}
