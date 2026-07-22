package collector

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/collector/config"
	"github.com/Suhaibinator/open-splunk/internal/collector/input"
	"github.com/Suhaibinator/open-splunk/internal/collector/wal"
	"google.golang.org/protobuf/proto"
)

// deadServerAddr is an address nothing listens on; the sender fails to connect
// and backs off, so batches accumulate in the queue for inspection without
// being delivered or acked.
const deadServerAddr = "127.0.0.1:1"

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// waitFor polls cond until it is true or the deadline elapses, failing the test
// otherwise. It avoids bare sleeps in favor of deadline polling.
func waitFor(t *testing.T, timeout time.Duration, msg string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", msg)
}

// writeFile writes data to path, failing the test on error.
func writeFile(t *testing.T, path, data string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(data), 0o644); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}

// newTestConfig returns a minimal valid config with one beginning-at-start file
// input over logGlob, storing state under stateDir.
func newTestConfig(t *testing.T, stateDir, logGlob, tokenFile string) *config.Config {
	t.Helper()
	writeFile(t, tokenFile, "test-token\n")
	return &config.Config{
		Server: config.ServerConfig{
			Address:   deadServerAddr,
			Transport: "grpc",
			TokenFile: tokenFile,
			TLS:       config.TLSConfig{Enabled: false},
		},
		State: config.StateConfig{
			Directory:     stateDir,
			MaxQueueBytes: 8 << 20,
		},
		Inputs: []config.InputConfig{{
			ID:           "app",
			Type:         "file",
			Include:      []string{logGlob},
			Format:       "ndjson",
			StartAt:      "beginning",
			Index:        "main",
			Source:       "app-log",
			Sourcetype:   "json",
			Host:         "test-host",
			PollInterval: config.Duration(10 * time.Millisecond),
		}},
	}
}

func TestDaemonStateDirectoryHasSingleOwner(t *testing.T) {
	t.Parallel()
	stateDir := t.TempDir()
	logDir := t.TempDir()
	cfg := newTestConfig(t, stateDir, filepath.Join(logDir, "*.log"), filepath.Join(stateDir, "token"))
	first, err := New(cfg, WithLogger(discardLogger()), WithCollectorID("cid-1"), WithInstanceID("iid-1"))
	if err != nil {
		t.Fatalf("first New: %v", err)
	}
	if _, err := New(cfg, WithLogger(discardLogger()), WithCollectorID("cid-2"), WithInstanceID("iid-2")); err == nil {
		t.Fatal("second collector unexpectedly acquired the same state directory")
	}
	if err := first.closeAll(); err != nil {
		t.Fatalf("close first: %v", err)
	}
	second, err := New(cfg, WithLogger(discardLogger()), WithCollectorID("cid-2"), WithInstanceID("iid-2"))
	if err != nil {
		t.Fatalf("New after release: %v", err)
	}
	if err := second.closeAll(); err != nil {
		t.Fatalf("close second: %v", err)
	}
}

// TestDaemonFileToWALAndCheckpoint exercises the full path: a file is discovered,
// tailed, decoded, batched, appended durably, and its checkpoint advances only
// after the append returns.
func TestDaemonFileToWALAndCheckpoint(t *testing.T) {
	t.Parallel()
	stateDir := t.TempDir()
	logDir := t.TempDir()
	logPath := filepath.Join(logDir, "app.log")

	const lines = `{"message":"one","n":1}
{"message":"two","n":2}
{"message":"three","n":3}
`
	writeFile(t, logPath, lines)

	cfg := newTestConfig(t, stateDir, filepath.Join(logDir, "*.log"), filepath.Join(stateDir, "token"))
	d, err := New(cfg, WithLogger(discardLogger()), WithCollectorID("cid"), WithInstanceID("iid"))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	d.batchLinger = 15 * time.Millisecond
	d.drainWindow = 50 * time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	runErr := make(chan error, 1)
	go func() { runErr <- d.Run(ctx) }()

	waitFor(t, 3*time.Second, "3 events queued", func() bool {
		return d.queue.Stats().QueuedEvents == 3
	})

	// The checkpoint advances to the end of the file (all bytes consumed) only
	// because the batch was appended durably first.
	waitFor(t, 2*time.Second, "checkpoint at end of file", func() bool {
		cps, err := d.checkpoints.List()
		if err != nil || len(cps) != 1 {
			return false
		}
		return cps[0].Offset == uint64(len(lines))
	})

	cancel()
	if err := <-runErr; err != nil {
		t.Fatalf("Run returned error: %v", err)
	}

	// Reopen the queue after shutdown and confirm the batch survived and carries
	// the decoded, index-routed events.
	q, err := wal.Open(wal.Options{Dir: filepath.Join(stateDir, walSubdir), Sync: wal.SyncAlways, CollectorID: "cid"})
	if err != nil {
		t.Fatalf("reopen queue: %v", err)
	}
	defer q.Close()
	if got := q.Stats().QueuedEvents; got != 3 {
		t.Fatalf("after restart QueuedEvents = %d, want 3", got)
	}
	batch, err := q.NextBatch(context.Background())
	if err != nil {
		t.Fatalf("NextBatch: %v", err)
	}
	if len(batch.GetEvents()) != 3 {
		t.Fatalf("batch has %d events, want 3", len(batch.GetEvents()))
	}
	for _, ev := range batch.GetEvents() {
		if ev.GetIndexName() != "main" {
			t.Errorf("event index = %q, want main", ev.GetIndexName())
		}
		if ev.GetHost() != "test-host" {
			t.Errorf("event host = %q, want test-host", ev.GetHost())
		}
	}
}

// TestDaemonDecodeFailurePolicy confirms a malformed line is skipped and counted
// while the valid lines around it are delivered, and the checkpoint still
// advances past the bad line (covered by a later durable event).
func TestDaemonDecodeFailurePolicy(t *testing.T) {
	t.Parallel()
	stateDir := t.TempDir()
	logDir := t.TempDir()
	logPath := filepath.Join(logDir, "app.log")

	const lines = `{"message":"good-1"}
{ this is not valid json
{"message":"good-2"}
`
	writeFile(t, logPath, lines)

	cfg := newTestConfig(t, stateDir, filepath.Join(logDir, "*.log"), filepath.Join(stateDir, "token"))
	d, err := New(cfg, WithLogger(discardLogger()), WithCollectorID("cid"), WithInstanceID("iid"))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	d.batchLinger = 15 * time.Millisecond
	d.drainWindow = 50 * time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	runErr := make(chan error, 1)
	go func() { runErr <- d.Run(ctx) }()

	waitFor(t, 3*time.Second, "2 valid events queued", func() bool {
		return d.queue.Stats().QueuedEvents == 2
	})
	waitFor(t, 2*time.Second, "1 decode failure counted", func() bool {
		return d.DecodeFailures() == 1
	})
	// A later valid event covers the malformed line's position.
	waitFor(t, 2*time.Second, "checkpoint past malformed line", func() bool {
		cps, err := d.checkpoints.List()
		return err == nil && len(cps) == 1 && cps[0].Offset == uint64(len(lines))
	})

	cancel()
	if err := <-runErr; err != nil {
		t.Fatalf("Run returned error: %v", err)
	}
	if got := d.DecodeFailures(); got != 1 {
		t.Fatalf("DecodeFailures = %d, want 1", got)
	}
}

// testEvent decodes raw at the given position into a LogEvent using a decoder
// with fixed metadata, at a fixed collection time so encoded sizes are stable.
func testEvent(t *testing.T, dec *Decoder, start, end, line uint64, raw string) *opensplunkv1.LogEvent {
	t.Helper()
	pos := SourcePosition{FileIdentity: "dev=1;ino=2;fp=abc", StartOffset: start, EndOffset: end, LineNumber: line}
	ev, err := dec.Decode([]byte(raw), pos, time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatalf("decode test event: %v", err)
	}
	return ev
}

func testDecoder(t *testing.T) *Decoder {
	t.Helper()
	dec, err := NewDecoder(DecodeConfig{
		Format: InputFormatNDJSON, InputID: "app", IndexName: "main",
		Source: "s", Sourcetype: "st", Host: "h",
	})
	if err != nil {
		t.Fatalf("NewDecoder: %v", err)
	}
	return dec
}

// TestDaemonBackpressureNoDrop drives the batcher directly against a real WAL
// queue sized to hold one batch: a second append hits ErrQueueFull, the batcher
// pauses (no drop, checkpoint not advanced), and once space is freed by an Ack
// the batch is appended and its checkpoint advances.
func TestDaemonBackpressureNoDrop(t *testing.T) {
	t.Parallel()
	dec := testDecoder(t)
	identity := input.FileIdentity{Device: 1, Inode: 2, Fingerprint: "abc"}
	raw := `{"k":"v"}`

	// Size one batch, then bound the real queue just above it so a second batch
	// overflows.
	oneBatch := measureBatchBytes(t, testEvent(t, dec, 0, 100, 1, raw))

	queueDir := t.TempDir()
	q, err := wal.Open(wal.Options{
		Dir:           queueDir,
		MaxQueueBytes: oneBatch + 64,
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

	d := &Daemon{
		log:                discardLogger(),
		now:                time.Now,
		queue:              q,
		checkpoints:        cps,
		batchMaxEvents:     1, // one event per batch for deterministic sizing
		batchMaxBytes:      1 << 20,
		batchLinger:        time.Hour, // disabled; flush by count
		queueFullRetry:     5 * time.Millisecond,
		shutdownFlushGrace: time.Second,
		lastOffsets:        make(map[string]uint64),
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	processed := make(chan processedEvent)
	batcherDone := make(chan struct{})
	go func() { defer close(batcherDone); d.runBatcher(ctx, processed) }()

	mk := func(start, end, line uint64) processedEvent {
		return processedEvent{
			event: testEvent(t, dec, start, end, line, raw), identity: identity,
			path: "/x.log", endOffset: end, lineNumber: line,
			size: proto.Size(testEvent(t, dec, start, end, line, raw)),
		}
	}

	// First event fits and is appended durably; its checkpoint advances.
	processed <- mk(0, 100, 1)
	waitFor(t, time.Second, "first batch appended", func() bool {
		return q.Stats().QueuedBatches == 1
	})
	waitFor(t, time.Second, "checkpoint at 100", func() bool {
		cp, ok, _ := cps.Get(identity)
		return ok && cp.Offset == 100
	})

	// Second event is handed to the batcher, which now blocks on ErrQueueFull.
	processed <- mk(100, 200, 2)

	// Because the batcher is stuck flushing, it stops draining: a third send
	// blocks, proving input consumption is paused rather than dropping data.
	select {
	case processed <- mk(200, 300, 3):
		t.Fatal("expected backpressure: send should block while queue is full")
	case <-time.After(150 * time.Millisecond):
	}
	// The queue did not grow and the second event's checkpoint did not advance.
	if got := q.Stats().QueuedBatches; got != 1 {
		t.Fatalf("QueuedBatches while full = %d, want 1", got)
	}
	if cp, _, _ := cps.Get(identity); cp.Offset != 100 {
		t.Fatalf("checkpoint advanced to %d during backpressure, want 100", cp.Offset)
	}

	// Free space: the stuck batch is now appended and its checkpoint advances.
	if err := q.Ack(1); err != nil {
		t.Fatalf("Ack: %v", err)
	}
	waitFor(t, time.Second, "second batch appended after ack", func() bool {
		cp, ok, _ := cps.Get(identity)
		return ok && cp.Offset == 200
	})

	// Closing the stream (not ctx) is how the batcher terminates, mirroring the
	// daemon where the input readers close the processed channel on shutdown.
	close(processed)
	<-batcherDone
}

type appendFailQueue struct{ wal.Queue }

func (appendFailQueue) Append([]*opensplunkv1.LogEvent) (*opensplunkv1.EventBatch, error) {
	return nil, errors.New("simulated WAL IO failure")
}

func TestBatcherReturnsFatalAppendFailure(t *testing.T) {
	t.Parallel()
	d := &Daemon{
		log: discardLogger(), queue: appendFailQueue{}, batchMaxEvents: 1,
		batchMaxBytes: 1024, batchLinger: time.Hour,
	}
	processed := make(chan processedEvent, 1)
	processed <- processedEvent{event: &opensplunkv1.LogEvent{EventId: "e1"}, size: 10}
	close(processed)
	if err := d.runBatcher(context.Background(), processed); err == nil || !strings.Contains(err.Error(), "durable append failed") {
		t.Fatalf("runBatcher error = %v, want fatal durable append failure", err)
	}
}

func TestBatcherFlushesBeforeCrossingByteCap(t *testing.T) {
	t.Parallel()
	q, err := wal.Open(wal.Options{Dir: t.TempDir(), Sync: wal.SyncAlways, CollectorID: "cid", ProtocolMajor: protocolMajor})
	if err != nil {
		t.Fatalf("open queue: %v", err)
	}
	defer q.Close()
	cps, err := input.NewCheckpointStore(t.TempDir())
	if err != nil {
		t.Fatalf("checkpoint store: %v", err)
	}
	defer cps.Close()
	d := &Daemon{
		log: discardLogger(), queue: q, checkpoints: cps,
		batchMaxEvents: 100, batchMaxBytes: 10, batchLinger: time.Hour,
		lastOffsets: make(map[string]uint64),
	}
	identity := input.FileIdentity{Device: 1, Inode: 2, Generation: 1, Fingerprint: "fp"}
	processed := make(chan processedEvent, 2)
	for i := 0; i < 2; i++ {
		processed <- processedEvent{
			event:    &opensplunkv1.LogEvent{EventId: fmt.Sprintf("e%d", i)},
			identity: identity, endOffset: uint64(i + 1), size: 6,
		}
	}
	close(processed)
	if err := d.runBatcher(context.Background(), processed); err != nil {
		t.Fatalf("runBatcher: %v", err)
	}
	if got := q.Stats(); got.QueuedBatches != 2 || got.QueuedEvents != 2 {
		t.Fatalf("batch cap was crossed instead of pre-flushed: %+v", got)
	}
}

// TestDaemonGracefulShutdownFlushesPartialBatch verifies that closing the input
// stream flushes the pending sub-threshold batch to the queue.
func TestDaemonGracefulShutdownFlushesPartialBatch(t *testing.T) {
	t.Parallel()
	dec := testDecoder(t)
	identity := input.FileIdentity{Device: 3, Inode: 4, Fingerprint: "xyz"}

	q, err := wal.Open(wal.Options{Dir: t.TempDir(), Sync: wal.SyncAlways, CollectorID: "cid", ProtocolMajor: protocolMajor})
	if err != nil {
		t.Fatalf("open queue: %v", err)
	}
	defer q.Close()
	cps, err := input.NewCheckpointStore(t.TempDir())
	if err != nil {
		t.Fatalf("checkpoint store: %v", err)
	}
	defer cps.Close()

	d := &Daemon{
		log:                discardLogger(),
		now:                time.Now,
		queue:              q,
		checkpoints:        cps,
		batchMaxEvents:     100,       // never reached
		batchMaxBytes:      1 << 20,   // never reached
		batchLinger:        time.Hour, // never fires
		queueFullRetry:     5 * time.Millisecond,
		shutdownFlushGrace: time.Second,
		lastOffsets:        make(map[string]uint64),
	}

	ctx := context.Background()
	processed := make(chan processedEvent)
	batcherDone := make(chan struct{})
	go func() { defer close(batcherDone); d.runBatcher(ctx, processed) }()

	processed <- processedEvent{
		event: testEvent(t, dec, 0, 42, 1, `{"k":"v"}`), identity: identity,
		path: "/x.log", endOffset: 42, lineNumber: 1,
		size: proto.Size(testEvent(t, dec, 0, 42, 1, `{"k":"v"}`)),
	}

	// Nothing has flushed yet (below all thresholds).
	if got := q.Stats().QueuedEvents; got != 0 {
		t.Fatalf("QueuedEvents before shutdown = %d, want 0", got)
	}

	// Closing the stream triggers the final partial-batch flush.
	close(processed)
	<-batcherDone

	if got := q.Stats().QueuedEvents; got != 1 {
		t.Fatalf("QueuedEvents after shutdown flush = %d, want 1", got)
	}
	if cp, ok, _ := cps.Get(identity); !ok || cp.Offset != 42 {
		t.Fatalf("checkpoint after flush = %+v, want offset 42", cp)
	}
}

// measureBatchBytes returns the on-disk size of a single-event batch, used to
// size a bounded queue precisely for the backpressure test.
func measureBatchBytes(t *testing.T, ev *opensplunkv1.LogEvent) uint64 {
	t.Helper()
	q, err := wal.Open(wal.Options{Dir: t.TempDir(), Sync: wal.SyncAlways, CollectorID: "cid", ProtocolMajor: protocolMajor})
	if err != nil {
		t.Fatalf("measure open: %v", err)
	}
	defer q.Close()
	if _, err := q.Append([]*opensplunkv1.LogEvent{ev}); err != nil {
		t.Fatalf("measure append: %v", err)
	}
	return q.Stats().QueuedBytes
}

// TestNewRejectsInvalidConfig confirms construction validates the config.
func TestNewRejectsInvalidConfig(t *testing.T) {
	t.Parallel()
	if _, err := New(nil); err == nil {
		t.Fatal("New(nil) should error")
	}
	cfg := &config.Config{State: config.StateConfig{Directory: t.TempDir()}}
	if _, err := New(cfg); err == nil {
		t.Fatal("New with invalid config should error")
	}
}
