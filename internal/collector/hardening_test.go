package collector

import (
	"bytes"
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/collector/input"
	"github.com/Suhaibinator/open-splunk/internal/collector/sender"
	"github.com/Suhaibinator/open-splunk/internal/collector/wal"
)

// captureSink records dead-letter records in memory for assertions.
type captureSink struct {
	mu      sync.Mutex
	records []sender.DeadLetterRecord
}

func (s *captureSink) WriteRecords(records []sender.DeadLetterRecord) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.records = append(s.records, records...)
	return nil
}

func (s *captureSink) Close() error { return nil }

func (s *captureSink) snapshot() []sender.DeadLetterRecord {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]sender.DeadLetterRecord, len(s.records))
	copy(out, s.records)
	return out
}

// TestDaemonOversizedEventDeadLettered drives the batcher against a real WAL
// queue sized so any single event's record exceeds max_queue_bytes: the event is
// dead-lettered (never wedging the pipeline in an infinite retry), counted, and
// its checkpoint still advances. (FIX 2, daemon side)
func TestDaemonOversizedEventDeadLettered(t *testing.T) {
	t.Parallel()
	q, err := wal.Open(wal.Options{
		Dir:           t.TempDir(),
		MaxQueueBytes: 8, // smaller than any real single-event batch record
		Sync:          wal.SyncAlways,
		CollectorID:   "cid",
		ProtocolMajor: protocolMajor,
	})
	if err != nil {
		t.Fatalf("open queue: %v", err)
	}
	defer q.Close()
	cps, err := input.NewCheckpointStore(t.TempDir())
	if err != nil {
		t.Fatalf("checkpoint store: %v", err)
	}
	defer cps.Close()

	sink := &captureSink{}
	d := &Daemon{
		log: discardLogger(), now: time.Now, queue: q, checkpoints: cps, deadLetter: sink,
		batchMaxEvents: 1, batchMaxBytes: 1 << 20, batchLinger: time.Hour,
		queueFullRetry: 5 * time.Millisecond, shutdownFlushGrace: time.Second,
		lastOffsets: make(map[string]uint64),
	}
	identity := input.FileIdentity{Device: 1, Inode: 2, Generation: 1, Fingerprint: "fp"}

	processed := make(chan processedEvent, 1)
	processed <- processedEvent{
		event:    &opensplunkv1.LogEvent{EventId: "e1", IndexName: "main"},
		identity: identity, path: "/x.log", endOffset: 42, lineNumber: 1, size: 10,
	}
	close(processed)

	if err := d.runBatcher(context.Background(), processed); err != nil {
		t.Fatalf("runBatcher returned fatal error for an oversized (policy-drop) event: %v", err)
	}

	recs := sink.snapshot()
	if len(recs) != 1 {
		t.Fatalf("dead-letter records = %d, want 1", len(recs))
	}
	if recs[0].Code != "BATCH_TOO_LARGE_FOR_QUEUE" {
		t.Fatalf("dead-letter code = %q, want BATCH_TOO_LARGE_FOR_QUEUE", recs[0].Code)
	}
	if got := d.OversizedDrops(); got != 1 {
		t.Fatalf("OversizedDrops = %d, want 1", got)
	}
	// The checkpoint must advance past the dropped event so it does not strand.
	if cp, ok, _ := cps.Get(identity); !ok || cp.Offset != 42 {
		t.Fatalf("checkpoint after oversized drop = %+v (ok=%v), want offset 42", cp, ok)
	}
	if st := q.Stats(); st.QueuedEvents != 0 {
		t.Fatalf("queue should be empty after oversized drop, got %+v", st)
	}
}

// alwaysFullQueue is a wal.Queue whose Append always reports the queue full,
// counting calls so a shutdown busy-spin is observable.
type alwaysFullQueue struct {
	wal.Queue
	calls atomic.Int64
}

func (q *alwaysFullQueue) Append([]*opensplunkv1.LogEvent) (*opensplunkv1.EventBatch, error) {
	q.calls.Add(1)
	return nil, wal.ErrQueueFull
}

// TestFlushShutdownDoesNotBusySpin verifies that once ctx is cancelled and the
// queue stays full, flush sleeps between attempts rather than spinning at 100%
// CPU: the number of Append attempts over the grace window is bounded to roughly
// grace/queueFullRetry, not thousands. (FIX 9)
func TestFlushShutdownDoesNotBusySpin(t *testing.T) {
	t.Parallel()
	q := &alwaysFullQueue{}
	d := &Daemon{
		log: discardLogger(), now: time.Now, queue: q,
		queueFullRetry: 5 * time.Millisecond, shutdownFlushGrace: 50 * time.Millisecond,
		lastOffsets: make(map[string]uint64),
	}
	b := &pendingBatch{}
	b.add(processedEvent{
		event:     &opensplunkv1.LogEvent{EventId: "e1"},
		identity:  input.FileIdentity{Device: 1, Inode: 2, Generation: 1, Fingerprint: "fp"},
		endOffset: 1, size: 10,
	})

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // already cancelled: exercise the shutdown grace path

	done := make(chan struct{})
	go func() { defer close(done); _ = d.flush(ctx, b) }()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("flush did not terminate within the grace window")
	}

	calls := q.calls.Load()
	// grace/retry = 50/5 ~= 10 attempts. A busy-spin would be thousands.
	if calls < 2 {
		t.Fatalf("Append attempts = %d, want at least a couple of retries", calls)
	}
	if calls > 60 {
		t.Fatalf("Append attempts = %d over a 50ms/5ms grace window: busy-spin (want ~10)", calls)
	}
}

// logCapture is a concurrency-safe io.Writer for inspecting log output.
type logCapture struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (w *logCapture) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.buf.Write(p)
}

func (w *logCapture) String() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.buf.String()
}

// TestDecodeFailureLogNeverLeaksTimestampSecret decodes a line whose invalid
// timestamp carries a secret and asserts the daemon's decode-failure log does
// not contain it (the RFC3339 parse error must be value-free). (FIX 8)
func TestDecodeFailureLogNeverLeaksTimestampSecret(t *testing.T) {
	t.Parallel()
	const secret = "topsecret-value-42"
	dec := testDecoder(t)
	line := `{"timestamp":"not-a-date-` + secret + `","message":"m"}`
	pos := SourcePosition{FileIdentity: "dev=1;ino=2;gen=1;fp=abc", StartOffset: 0, EndOffset: uint64(len(line)), LineNumber: 1}
	_, err := dec.Decode([]byte(line), pos, time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	if err == nil {
		t.Fatal("expected a decode error for the invalid timestamp")
	}
	if strings.Contains(err.Error(), secret) {
		t.Fatalf("decode error itself leaked the secret: %v", err)
	}

	cap := &logCapture{}
	d := &Daemon{log: slog.New(slog.NewTextHandler(cap, &slog.HandlerOptions{Level: slog.LevelDebug}))}
	d.recordDecodeFailure("app", input.SourceRef{
		Identity:    input.FileIdentity{Device: 1, Inode: 2, Generation: 1, Fingerprint: "abc"},
		StartOffset: 0, EndOffset: uint64(len(line)), LineNumber: 1,
	}, len(line), err)

	if logs := cap.String(); strings.Contains(logs, secret) {
		t.Fatalf("decode-failure log leaked the secret: %s", logs)
	}
}

// TestDaemonTightensStateDirAndWarnsPlaintext verifies New tightens a loosely
// permissioned state directory to owner-only (FIX 6) and warns when plaintext
// transport is active even for a loopback address (FIX 7 Warn path).
func TestDaemonTightensStateDirAndWarnsPlaintext(t *testing.T) {
	t.Parallel()
	stateDir := t.TempDir()
	if err := os.Chmod(stateDir, 0o777); err != nil {
		t.Fatalf("chmod loose: %v", err)
	}
	logDir := t.TempDir()
	capture := &logCapture{}
	logger := slog.New(slog.NewTextHandler(capture, &slog.HandlerOptions{Level: slog.LevelDebug}))

	cfg := newTestConfig(t, stateDir, filepath.Join(logDir, "*.log"), filepath.Join(stateDir, "token"))
	if _, err := New(cfg, WithLogger(logger), WithCollectorID("cid"), WithInstanceID("iid")); err != nil {
		t.Fatalf("New: %v", err)
	}

	fi, err := os.Stat(stateDir)
	if err != nil {
		t.Fatalf("stat state dir: %v", err)
	}
	if perm := fi.Mode().Perm(); perm&0o077 != 0 {
		t.Fatalf("state dir mode = %o, want owner-only (0o077 bits clear)", perm)
	}
	if logs := capture.String(); !strings.Contains(logs, "cleartext") {
		t.Fatalf("expected a plaintext-transport warning, got: %s", logs)
	}
}
