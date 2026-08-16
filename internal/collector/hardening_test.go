package collector

import (
	"bytes"
	"context"
	"errors"
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
// its checkpoint advances when no earlier WAL ownership exists. (FIX 2, daemon
// side)
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
		lastOffsets: make(map[inputFileKey]uint64),
	}
	identity := input.FileIdentity{
		Device: 1, Inode: 2, Generation: 1,
		Fingerprint: strings.Repeat("ab", 32), FingerprintLength: 64,
	}
	const inputID = "oversized-input"

	processed := make(chan processedEvent, 1)
	processed <- processedEvent{
		event:   &opensplunkv1.LogEvent{EventId: "e1", IndexName: "main"},
		inputID: inputID, identity: identity, path: "/x.log", endOffset: 42,
		lineNumber: 1, nextLineNumber: 3, size: 10,
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
	if cp, ok, _ := cps.Get(inputID, identity); !ok || cp.Offset != 42 ||
		cp.LineNumber != 1 || cp.NextLineNumber != 3 {
		t.Fatalf("checkpoint after oversized drop = %+v (ok=%v), want offset 42 lines [1,3)", cp, ok)
	}
	if st := q.Stats(); st.QueuedEvents != 0 {
		t.Fatalf("queue should be empty after oversized drop, got %+v", st)
	}
}

type secondAppendTooLargeQueue struct {
	wal.ResumeQueue
	appendCalls atomic.Int64
}

type splitAcceptQueue struct {
	wal.ResumeQueue
	appendCalls atomic.Int64
}

func (q *splitAcceptQueue) Append(
	events []*opensplunkv1.LogEvent,
) (*opensplunkv1.EventBatch, error) {
	q.appendCalls.Add(1)
	if len(events) > 1 {
		return nil, wal.ErrBatchTooLarge
	}
	return q.ResumeQueue.Append(events)
}

func TestSuccessfulOversizedSplitResetsOriginalBatch(t *testing.T) {
	t.Parallel()
	underlying, err := wal.Open(wal.Options{
		Dir: t.TempDir(), Sync: wal.SyncAlways, CollectorID: "cid", ProtocolMajor: protocolMajor,
	})
	if err != nil {
		t.Fatalf("open queue: %v", err)
	}
	t.Cleanup(func() { _ = underlying.Close() })
	queue := &splitAcceptQueue{ResumeQueue: underlying}
	d := &Daemon{log: discardLogger(), queue: queue}
	b := &pendingBatch{}
	for _, id := range []string{"first", "second"} {
		b.add(processedEvent{
			event: &opensplunkv1.LogEvent{EventId: id}, size: len(id),
		})
	}

	if err := d.flush(context.Background(), b); err != nil {
		t.Fatalf("split flush: %v", err)
	}
	if !b.empty() || len(b.items) != 0 || b.bytes != 0 {
		t.Fatalf("original batch retained after successful split: %+v", b)
	}
	if stats := underlying.Stats(); stats.QueuedBatches != 2 || stats.QueuedEvents != 2 {
		t.Fatalf("split queue stats = %+v, want two one-event batches", stats)
	}
	if calls := queue.appendCalls.Load(); calls != 3 {
		t.Fatalf("split append calls = %d, want parent plus two halves", calls)
	}

	// A later barrier or shutdown flush sees the original pendingBatch again.
	// It must be a no-op rather than duplicating both successfully split events.
	if err := d.flush(context.Background(), b); err != nil {
		t.Fatalf("second flush: %v", err)
	}
	if calls := queue.appendCalls.Load(); calls != 3 {
		t.Fatalf("second flush appended retained split events: calls=%d", calls)
	}
	if stats := underlying.Stats(); stats.QueuedBatches != 2 || stats.QueuedEvents != 2 {
		t.Fatalf("second flush duplicated split events: %+v", stats)
	}
}

func (q *secondAppendTooLargeQueue) Append(
	events []*opensplunkv1.LogEvent,
) (*opensplunkv1.EventBatch, error) {
	if q.appendCalls.Add(1) == 2 {
		return nil, wal.ErrBatchTooLarge
	}
	return q.ResumeQueue.Append(events)
}

func TestOversizedDeadLetterDoesNotCheckpointPastPendingWAL(t *testing.T) {
	t.Parallel()
	underlying, err := wal.Open(wal.Options{
		Dir: t.TempDir(), Sync: wal.SyncAlways, CollectorID: "cid", ProtocolMajor: protocolMajor,
	})
	if err != nil {
		t.Fatalf("open queue: %v", err)
	}
	t.Cleanup(func() { _ = underlying.Close() })
	queue := &secondAppendTooLargeQueue{ResumeQueue: underlying}
	checkpoints, err := input.NewCheckpointStore(t.TempDir())
	if err != nil {
		t.Fatalf("checkpoint store: %v", err)
	}
	t.Cleanup(func() { _ = checkpoints.Close() })
	identity := input.FileIdentity{
		Device: 1, Inode: 2, Generation: 1,
		Fingerprint: strings.Repeat("ab", 32), FingerprintLength: 64,
	}
	const inputID = "ordered-oversized"
	if err := checkpoints.Set(input.Checkpoint{
		InputID: inputID, Identity: identity, Path: "/x.log", Offset: 0,
	}); err != nil {
		t.Fatalf("seed checkpoint: %v", err)
	}
	sink := &captureSink{}
	d := &Daemon{
		log: discardLogger(), now: time.Now, queue: queue, checkpoints: checkpoints,
		deadLetter: sink, lastOffsets: make(map[inputFileKey]uint64),
	}

	earlier := terminalPendingBatch(identity, 0, 100, 1)
	earlier.items[0].inputID = inputID
	if err := d.flush(context.Background(), earlier); err != nil {
		t.Fatalf("append earlier WAL batch: %v", err)
	}
	later := terminalPendingBatch(identity, 100, 200, 2)
	later.items[0].inputID = inputID
	if err := d.flush(context.Background(), later); err != nil {
		t.Fatalf("dead-letter later oversized event: %v", err)
	}

	if got := len(sink.snapshot()); got != 1 {
		t.Fatalf("dead-letter records = %d, want 1", got)
	}
	cp, ok, err := checkpoints.Get(inputID, identity)
	if err != nil {
		t.Fatalf("get checkpoint: %v", err)
	}
	if !ok || cp.Offset != 0 {
		t.Fatalf("checkpoint after later local terminal = %+v (ok=%v), want discovery offset 0", cp, ok)
	}
	if got := underlying.Stats().QueuedBatches; got != 1 {
		t.Fatalf("pending WAL batches = %d, want 1", got)
	}
}

// alwaysFullQueue is a wal.Queue whose Append always reports the queue full,
// counting calls so a shutdown busy-spin is observable.
type alwaysFullQueue struct {
	wal.Queue
	calls atomic.Int64
}

type splitShutdownQueue struct {
	wal.Queue
	secondCalls atomic.Int64
}

func (q *splitShutdownQueue) Append(events []*opensplunkv1.LogEvent) (*opensplunkv1.EventBatch, error) {
	if len(events) > 1 {
		return nil, wal.ErrBatchTooLarge
	}
	if len(events) == 1 && events[0].GetEventId() == "later-oversized" {
		q.secondCalls.Add(1)
		return nil, wal.ErrBatchTooLarge
	}
	return nil, wal.ErrQueueFull
}

func TestShutdownSplitCannotCheckpointPastAbandonedEarlierEvent(t *testing.T) {
	t.Parallel()
	queue := &splitShutdownQueue{}
	checkpoints, err := input.NewCheckpointStore(t.TempDir())
	if err != nil {
		t.Fatalf("checkpoint store: %v", err)
	}
	t.Cleanup(func() { _ = checkpoints.Close() })
	sink := &captureSink{}
	identity := input.FileIdentity{
		Device: 1, Inode: 2, Generation: 1,
		Fingerprint: strings.Repeat("ab", 32), FingerprintLength: 64,
	}
	if err := checkpoints.Set(input.Checkpoint{
		InputID: "input", Identity: identity, Path: "/x.log", Offset: 0,
	}); err != nil {
		t.Fatalf("seed checkpoint: %v", err)
	}
	d := &Daemon{
		log: discardLogger(), now: time.Now, queue: queue,
		checkpoints: checkpoints, deadLetter: sink,
		queueFullRetry: time.Millisecond, shutdownFlushGrace: 10 * time.Millisecond,
		lastOffsets: make(map[inputFileKey]uint64),
	}
	b := &pendingBatch{}
	b.add(processedEvent{
		event: &opensplunkv1.LogEvent{EventId: "earlier"}, inputID: "input",
		identity: identity, path: "/x.log", endOffset: 100, lineNumber: 1,
		nextLineNumber: 2, size: 10,
	})
	b.add(processedEvent{
		event: &opensplunkv1.LogEvent{EventId: "later-oversized"}, inputID: "input",
		identity: identity, path: "/x.log", endOffset: 200, lineNumber: 2,
		nextLineNumber: 3, size: 10,
	})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := d.flush(ctx, b); !errors.Is(err, context.Canceled) {
		t.Fatalf("flush error = %v, want context cancellation after abandonment", err)
	}
	if got := queue.secondCalls.Load(); got != 0 {
		t.Fatalf("later split append attempts = %d, want 0 after earlier abandonment", got)
	}
	if got := len(sink.snapshot()); got != 0 {
		t.Fatalf("dead letters = %d, want 0 after earlier abandonment", got)
	}
	checkpoint, ok, err := checkpoints.Get("input", identity)
	if err != nil || !ok || checkpoint.Offset != 0 {
		t.Fatalf("checkpoint = %+v (ok=%t, err=%v), want offset 0", checkpoint, ok, err)
	}
}

func (q *alwaysFullQueue) Append([]*opensplunkv1.LogEvent) (*opensplunkv1.EventBatch, error) {
	q.calls.Add(1)
	return nil, wal.ErrQueueFull
}

// TestFlushShutdownDoesNotBusySpin verifies that once ctx is canceled and the
// queue stays full, flush sleeps between attempts rather than spinning at 100%
// CPU: the number of Append attempts over the grace window is bounded to roughly
// grace/queueFullRetry, not thousands. (FIX 9)
func TestFlushShutdownDoesNotBusySpin(t *testing.T) {
	t.Parallel()
	q := &alwaysFullQueue{}
	d := &Daemon{
		log: discardLogger(), now: time.Now, queue: q,
		queueFullRetry: 5 * time.Millisecond, shutdownFlushGrace: 50 * time.Millisecond,
		lastOffsets: make(map[inputFileKey]uint64),
	}
	b := &pendingBatch{}
	b.add(processedEvent{
		event:     &opensplunkv1.LogEvent{EventId: "e1"},
		identity:  input.FileIdentity{Device: 1, Inode: 2, Generation: 1, Fingerprint: "fp"},
		endOffset: 1, size: 10,
	})

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // already canceled: exercise the shutdown grace path

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

	capture := &logCapture{}
	d := &Daemon{log: slog.New(slog.NewTextHandler(capture, &slog.HandlerOptions{Level: slog.LevelDebug}))}
	d.recordDecodeFailure("app", input.SourceRef{
		Identity:    input.FileIdentity{Device: 1, Inode: 2, Generation: 1, Fingerprint: "abc"},
		StartOffset: 0, EndOffset: uint64(len(line)), LineNumber: 1,
	}, len(line))

	if logs := capture.String(); strings.Contains(logs, secret) {
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
