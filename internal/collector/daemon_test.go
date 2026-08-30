package collector

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
	"unicode"
	"unicode/utf8"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/collector/config"
	"github.com/Suhaibinator/open-splunk/internal/collector/input"
	"github.com/Suhaibinator/open-splunk/internal/collector/sender"
	"github.com/Suhaibinator/open-splunk/internal/collector/wal"
	"github.com/Suhaibinator/open-splunk/internal/collectorlimits"
	"github.com/Suhaibinator/open-splunk/internal/ingest"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

// deadServerAddr is an address nothing listens on; the sender fails to connect
// and backs off, so batches accumulate in the queue for inspection without
// being delivered or acked.
const deadServerAddr = "127.0.0.1:1"

func discardLogger() *zap.Logger {
	return zap.NewNop()
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

type blockedFirstAppendQueue struct {
	wal.Queue
	first         sync.Once
	appendStarted chan struct{}
	appendRelease chan struct{}
}

func (q *blockedFirstAppendQueue) Append(
	events []*opensplunk.LogEvent,
) (*opensplunk.EventBatch, error) {
	block := false
	q.first.Do(func() {
		block = true
		close(q.appendStarted)
	})
	if block {
		<-q.appendRelease
	}
	return q.Queue.Append(events)
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

func TestDaemonWiresBoundedDeadLetterRotation(t *testing.T) {
	t.Parallel()
	stateDir := t.TempDir()
	logDir := t.TempDir()
	cfg := newTestConfig(t, stateDir, filepath.Join(logDir, "*.log"), filepath.Join(stateDir, "token"))
	cfg.State.DeadLetterMaxBytes = 1
	cfg.State.DeadLetterMaxBackups = 1
	d, err := New(cfg, WithLogger(discardLogger()), WithCollectorID("cid"), WithInstanceID("iid"))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = d.closeAll() })

	record := sender.DeadLetterRecord{Code: "test", RejectedAt: time.Now().UTC()}
	if err := d.deadLetter.WriteRecords([]sender.DeadLetterRecord{record}); err != nil {
		t.Fatalf("first WriteRecords: %v", err)
	}
	if err := d.deadLetter.WriteRecords([]sender.DeadLetterRecord{record}); err != nil {
		t.Fatalf("second WriteRecords: %v", err)
	}
	if info, err := os.Stat(filepath.Join(stateDir, deadLetterFile+".1")); err != nil || info.Size() == 0 {
		t.Fatalf("rotated dead letter = (%v, %v), want nonempty backup", info, err)
	}
}

func TestDaemonRejectsStateDirectorySymlinkWithTrailingSeparator(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	target := filepath.Join(root, "target")
	if err := os.Mkdir(target, 0o700); err != nil {
		t.Fatal(err)
	}
	stateLink := filepath.Join(root, "state-link")
	if err := os.Symlink(target, stateLink); err != nil {
		t.Fatal(err)
	}
	logDirectory := t.TempDir()
	tokenPath := filepath.Join(target, "token")
	cfg := newTestConfig(
		t,
		stateLink+string(os.PathSeparator),
		filepath.Join(logDirectory, "*.log"),
		tokenPath,
	)

	if daemon, err := New(
		cfg,
		WithLogger(discardLogger()),
		WithCollectorID("collector-symlink-test"),
		WithInstanceID("instance-symlink-test"),
	); err == nil {
		_ = daemon.closeAll()
		t.Fatal("New followed a state-directory symlink with a trailing separator")
	}
	for _, artifact := range []string{collectorIDFile, checkpointsSubdir, walSubdir, deadLetterFile} {
		if _, err := os.Lstat(filepath.Join(target, artifact)); !os.IsNotExist(err) {
			t.Fatalf("state-directory symlink target received %q: %v", artifact, err)
		}
	}
}

// TestDaemonFileToWALWithheldAckKeepsDiscoveryCheckpoint exercises the full
// local path while the server is unreachable. WAL append is durable, but no
// terminal server disposition exists, so the checkpoint must remain at its
// discovery offset.
func TestDaemonFileToWALWithheldAckKeepsDiscoveryCheckpoint(t *testing.T) {
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

	// The input manager wrote the discovery checkpoint before reading, but WAL
	// append alone must not move it forward.
	waitFor(t, 2*time.Second, "discovery checkpoint remains at zero", func() bool {
		cps, err := d.checkpoints.List()
		if err != nil || len(cps) != 1 {
			return false
		}
		return cps[0].Offset == 0
	})

	cancel()
	if err := <-runErr; err != nil {
		t.Fatalf("Run returned error: %v", err)
	}

	// Reopen the queue after shutdown and confirm the batch survived and carries
	// the decoded, index-routed events.
	q, err := wal.Open(wal.Options{
		Dir: filepath.Join(stateDir, walSubdir), Sync: wal.SyncAlways,
		CollectorID: "cid",
	})
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

func TestDaemonGenerationResetWaitsForPriorSyncAlwaysAppend(t *testing.T) {
	stateDir := t.TempDir()
	logDir := t.TempDir()
	logPath := filepath.Join(logDir, "app.log")
	const original = "{\"message\":\"old\"}\n"
	const replacement = "{\"message\":\"new\"}\n"
	if len(original) != len(replacement) {
		t.Fatal("generation barrier fixture must preserve file size")
	}
	writeFile(t, logPath, original)

	cfg := newTestConfig(t, stateDir, logPath, filepath.Join(stateDir, "token"))
	d, err := New(
		cfg,
		WithLogger(discardLogger()),
		WithCollectorID("cid-generation-barrier"),
		WithInstanceID("iid-generation-barrier"),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	underlying, ok := d.queue.(wal.ResumeQueue)
	if !ok {
		_ = d.closeAll()
		t.Fatalf("daemon queue = %T, want wal.ResumeQueue", d.queue)
	}
	blocked := &blockedFirstAppendQueue{
		Queue: underlying, appendStarted: make(chan struct{}), appendRelease: make(chan struct{}),
	}
	d.queue = blocked
	// The first event must remain in the batcher indefinitely absent the
	// generation barrier; this makes the regression independent of the normal
	// 250ms linger and scheduler timing.
	d.batchLinger = time.Hour
	d.batchMaxEvents = 100
	d.batchMaxBytes = 1 << 20
	d.drainWindow = 25 * time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	runErr := make(chan error, 1)
	go func() { runErr <- d.Run(ctx) }()
	releaseAppend := sync.OnceFunc(func() { close(blocked.appendRelease) })
	runCollected := false
	t.Cleanup(func() {
		releaseAppend()
		cancel()
		if runCollected {
			return
		}
		select {
		case runError := <-runErr:
			if runError != nil {
				t.Errorf("Run: %v", runError)
			}
		case <-time.After(3 * time.Second):
			t.Error("Daemon.Run did not stop")
		}
	})

	waitFor(t, 3*time.Second, "original raw event publication", func() bool {
		return d.inputs[0].manager.Health().EventsReadTotal == 1
	})
	checkpoints, err := d.checkpoints.List()
	if err != nil || len(checkpoints) != 1 {
		t.Fatalf("initial checkpoints = %+v, err=%v", checkpoints, err)
	}
	originalIdentity := checkpoints[0].Identity
	if checkpoints[0].Offset != 0 {
		t.Fatalf("initial checkpoint offset = %d, want discovery offset 0", checkpoints[0].Offset)
	}

	// Copy-truncate and rewrite the same inode to the same size. readInput must
	// finish decoding/processing the old event, and the batcher must interrupt
	// its one-hour linger and attempt the SyncAlways append, before input may
	// persist the replacement generation.
	writeFile(t, logPath, replacement)
	select {
	case <-blocked.appendStarted:
	case <-time.After(3 * time.Second):
		t.Fatalf("timed out waiting for barrier-triggered append; health=%+v", d.inputs[0].manager.Health())
	}
	if stats := underlying.Stats(); stats.QueuedBatches != 0 || stats.QueuedEvents != 0 {
		t.Fatalf("underlying WAL changed before blocked append was released: %+v", stats)
	}
	checkpoints, err = d.checkpoints.List()
	if err != nil || len(checkpoints) != 1 {
		t.Fatalf("blocked checkpoints = %+v, err=%v", checkpoints, err)
	}
	if checkpoints[0].Identity.Generation != originalIdentity.Generation {
		t.Fatalf(
			"generation checkpoint overtook SyncAlways append: got %d want %d",
			checkpoints[0].Identity.Generation,
			originalIdentity.Generation,
		)
	}

	releaseAppend()
	waitFor(t, 3*time.Second, "old generation durable WAL append", func() bool {
		stats := underlying.Stats()
		return stats.QueuedBatches == 1 && stats.QueuedEvents == 1
	})
	waitFor(t, 3*time.Second, "replacement generation checkpoint", func() bool {
		cps, listErr := d.checkpoints.List()
		return listErr == nil && len(cps) == 1 &&
			cps[0].Identity.Generation == originalIdentity.Generation+1
	})
	marks, err := underlying.PendingSourceMarks()
	if err != nil {
		t.Fatalf("PendingSourceMarks: %v", err)
	}
	if len(marks) != 1 || marks[0].FileIdentity != originalIdentity.String() {
		t.Fatalf("durable source marks = %+v, want original generation %s", marks, originalIdentity.String())
	}

	cancel()
	select {
	case runError := <-runErr:
		runCollected = true
		if runError != nil {
			t.Fatalf("Run: %v", runError)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Daemon.Run did not stop after cancellation")
	}
}

func TestDaemonQueueFullBarrierDoesNotAdvanceGenerationOnShutdown(t *testing.T) {
	stateDir := t.TempDir()
	logDir := t.TempDir()
	logPath := filepath.Join(logDir, "app.log")
	const original = "{\"message\":\"old\"}\n"
	const replacement = "{\"message\":\"new\"}\n"
	writeFile(t, logPath, original)

	cfg := newTestConfig(t, stateDir, logPath, filepath.Join(stateDir, "token"))
	d, err := New(
		cfg,
		WithLogger(discardLogger()),
		WithCollectorID("cid-full-generation-barrier"),
		WithInstanceID("iid-full-generation-barrier"),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	underlying := d.queue
	full := &alwaysFullQueue{Queue: underlying}
	d.queue = full
	d.batchLinger = time.Hour
	d.batchMaxEvents = 100
	d.batchMaxBytes = 1 << 20
	d.queueFullRetry = 2 * time.Millisecond
	d.shutdownFlushGrace = 20 * time.Millisecond
	d.drainWindow = 20 * time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	runErr := make(chan error, 1)
	go func() { runErr <- d.Run(ctx) }()
	runCollected := false
	t.Cleanup(func() {
		cancel()
		if runCollected {
			return
		}
		select {
		case <-runErr:
		case <-time.After(3 * time.Second):
			t.Error("Daemon.Run did not stop")
		}
	})

	waitFor(t, 3*time.Second, "original raw event publication", func() bool {
		return d.inputs[0].manager.Health().EventsReadTotal == 1
	})
	checkpoints, err := d.checkpoints.List()
	if err != nil || len(checkpoints) != 1 {
		t.Fatalf("initial checkpoints = %+v, err=%v", checkpoints, err)
	}
	originalIdentity := checkpoints[0].Identity
	writeFile(t, logPath, replacement)
	waitFor(t, 3*time.Second, "barrier flush blocked by full queue", func() bool {
		return full.calls.Load() > 0
	})
	checkpoints, err = d.checkpoints.List()
	if err != nil || len(checkpoints) != 1 {
		t.Fatalf("queue-full checkpoints = %+v, err=%v", checkpoints, err)
	}
	if checkpoints[0].Identity.Generation != originalIdentity.Generation {
		t.Fatalf("queue-full barrier advanced generation: %+v", checkpoints[0])
	}

	cancel()
	select {
	case runError := <-runErr:
		runCollected = true
		if runError != nil {
			t.Fatalf("Run: %v", runError)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("queue-full durability barrier blocked shutdown")
	}

	reopened, err := input.NewCheckpointStore(filepath.Join(stateDir, checkpointsSubdir))
	if err != nil {
		t.Fatalf("reopen checkpoints: %v", err)
	}
	defer reopened.Close()
	checkpoints, err = reopened.List()
	if err != nil || len(checkpoints) != 1 {
		t.Fatalf("reopened checkpoints = %+v, err=%v", checkpoints, err)
	}
	if checkpoints[0].Identity.Generation != originalIdentity.Generation {
		t.Fatalf("shutdown persisted generation through failed barrier: %+v", checkpoints[0])
	}
}

func TestDaemonRestartDoesNotRequeuePendingWALSourcePrefix(t *testing.T) {
	t.Parallel()
	stateDir := t.TempDir()
	logDir := t.TempDir()
	logPath := filepath.Join(logDir, "app.log")
	const firstLines = `{"message":"one","n":1}
{"message":"two","n":2}
{"message":"three","n":3}
`
	writeFile(t, logPath, firstLines)
	cfg := newTestConfig(t, stateDir, filepath.Join(logDir, "*.log"), filepath.Join(stateDir, "token"))

	runUntilQueued := func(instanceID string, want uint64) (*Daemon, context.CancelFunc, <-chan error) {
		t.Helper()
		daemon, err := New(
			cfg,
			WithLogger(discardLogger()),
			WithCollectorID("cid"),
			WithInstanceID(instanceID),
		)
		if err != nil {
			t.Fatalf("New(%s): %v", instanceID, err)
		}
		daemon.batchLinger = 15 * time.Millisecond
		daemon.drainWindow = 50 * time.Millisecond
		ctx, cancel := context.WithCancel(context.Background())
		runErr := make(chan error, 1)
		go func() { runErr <- daemon.Run(ctx) }()
		waitFor(t, 3*time.Second, fmt.Sprintf("%d events queued by %s", want, instanceID), func() bool {
			return daemon.queue.Stats().QueuedEvents >= want
		})
		return daemon, cancel, runErr
	}

	first, cancelFirst, firstErr := runUntilQueued("iid-1", 3)
	if got := first.queue.Stats().QueuedEvents; got != 3 {
		t.Fatalf("first queued events = %d, want 3", got)
	}
	cancelFirst()
	if err := <-firstErr; err != nil {
		t.Fatalf("first Run: %v", err)
	}

	file, err := os.OpenFile(logPath, os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	_, writeErr := io.WriteString(file, "{\"message\":\"four\",\"n\":4}\n")
	syncErr := file.Sync()
	closeErr := file.Close()
	if err := errors.Join(writeErr, syncErr, closeErr); err != nil {
		t.Fatalf("append fourth source line: %v", err)
	}

	second, cancelSecond, secondErr := runUntilQueued("iid-2", 4)
	if got := second.queue.Stats().QueuedEvents; got != 4 {
		t.Fatalf(
			"queued events after collector restart = %d, want the 3 pending WAL events plus only the new source line",
			got,
		)
	}
	checkpoints, err := second.checkpoints.List()
	if err != nil || len(checkpoints) != 1 || checkpoints[0].Offset != 0 {
		t.Fatalf(
			"pending WAL resume advanced terminal checkpoint before acknowledgment: %+v, %v",
			checkpoints,
			err,
		)
	}
	cancelSecond()
	if err := <-secondErr; err != nil {
		t.Fatalf("second Run: %v", err)
	}
}

// TestDaemonRedactsSecretsBeforeOfflineWALAppend proves the edge collector
// never persists known secret families while the ingestion server is
// unreachable. Server-side redaction is too late for this trust boundary: the
// local WAL must already contain only the sanitized event.
func TestDaemonRedactsSecretsBeforeOfflineWALAppend(t *testing.T) {
	t.Parallel()
	stateDir := t.TempDir()
	logDir := t.TempDir()
	logPath := filepath.Join(logDir, "app.log")

	const (
		structuredSecret       = "offline-structured-secret"
		messageSecret          = "offline-message-secret"
		noteSecret             = "offline-note-secret"
		customStructuredSecret = "offline-custom-structured-secret"
		customMessageSecret    = "offline-custom-message-secret"
		secondStructuredSecret = "offline-second-structured-secret"
		secondMessageSecret    = "offline-second-message-secret"
		renamedSecret          = "offline-rename-to-sensitive-secret"
	)
	writeFile(t, logPath,
		`{"message":"token=`+messageSecret+` customer_credential=`+customMessageSecret+
			` customer_pin=`+secondMessageSecret+` safe=message-kept","token":"`+structuredSecret+
			`","customer_credential":"`+customStructuredSecret+
			`","customer_pin":"`+secondStructuredSecret+
			`","credential":"`+renamedSecret+
			`","note":"authorization=Bearer `+noteSecret+` safe=note-kept"}`+"\n")

	cfg := newTestConfig(t, stateDir, filepath.Join(logDir, "*.log"), filepath.Join(stateDir, "token"))
	cfg.Processors = []config.ProcessorConfig{
		{
			Type: "redact", Fields: []string{"customer_credential"}, Replacement: "***",
		},
		{
			Type: "redact", Fields: []string{"customer_pin"}, Replacement: "###",
		},
		{
			Type: "rename", From: "credential", To: "token",
		},
	}
	d, err := New(cfg, WithLogger(discardLogger()), WithCollectorID("cid"), WithInstanceID("iid"))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	d.batchLinger = 15 * time.Millisecond
	d.drainWindow = 50 * time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	runErr := make(chan error, 1)
	go func() { runErr <- d.Run(ctx) }()
	waitFor(t, 3*time.Second, "redacted event queued", func() bool {
		return d.queue.Stats().QueuedEvents == 1
	})
	cancel()
	if err := <-runErr; err != nil {
		t.Fatalf("Run returned error: %v", err)
	}

	queue, err := wal.Open(wal.Options{
		Dir: filepath.Join(stateDir, walSubdir), Sync: wal.SyncAlways,
		CollectorID: "cid",
	})
	if err != nil {
		t.Fatalf("reopen queue: %v", err)
	}
	defer queue.Close()
	batch, err := queue.NextBatch(context.Background())
	if err != nil {
		t.Fatalf("read pending batch: %v", err)
	}
	wire, err := proto.MarshalOptions{Deterministic: true}.Marshal(batch)
	if err != nil {
		t.Fatalf("marshal pending batch: %v", err)
	}
	for _, secret := range []string{
		structuredSecret,
		messageSecret,
		noteSecret,
		customStructuredSecret,
		customMessageSecret,
		secondStructuredSecret,
		secondMessageSecret,
		renamedSecret,
	} {
		if bytes.Contains(wire, []byte(secret)) {
			t.Fatal("offline collector WAL retained a planted secret")
		}
	}
	event := batch.GetEvents()[0]
	if !bytes.Contains(event.GetRaw(), []byte("***")) ||
		!bytes.Contains(event.GetRaw(), []byte("###")) ||
		!bytes.Contains(event.GetRaw(), []byte("message-kept")) ||
		!bytes.Contains(event.GetRaw(), []byte("note-kept")) ||
		!strings.Contains(event.GetMessage(), "***") ||
		!strings.Contains(event.GetMessage(), "###") ||
		!strings.Contains(event.GetMessage(), "message-kept") {
		t.Fatalf("offline WAL redaction lost safe data or its replacement: raw=%q message=%q",
			event.GetRaw(), event.GetMessage())
	}
	fields := make(map[string]*opensplunk.TypedValue, len(event.GetFields().GetFields()))
	for _, field := range event.GetFields().GetFields() {
		fields[field.GetName()] = field.GetValue()
	}
	if fields["customer_credential"].GetStringValue() != "***" ||
		fields["customer_pin"].GetStringValue() != "###" ||
		fields["token"].GetStringValue() == renamedSecret {
		t.Fatalf("offline WAL configured/post-pipeline redaction = %+v", fields)
	}
}

func TestDurableRedactorsTraceRenameChainsBackToRawKeys(t *testing.T) {
	t.Parallel()

	processors := []config.ProcessorConfig{
		{Type: "rename", From: "original_name", To: "intermediate_name"},
		{Type: "rename", From: "intermediate_name", To: "customer_credential"},
		{Type: "redact", Fields: []string{"customer_credential"}, Replacement: "MASKED"},
	}
	pipeline, err := buildPipeline(processors)
	if err != nil {
		t.Fatal(err)
	}
	redactor, err := buildDurableRedactor(pipeline)
	if err != nil {
		t.Fatal(err)
	}
	event := testEvent(t, testDecoder(t), 0, 64, 1, `{"original_name":"rename-chain-secret","safe":"kept"}`)
	event = redactor.beforePipeline(event, nil)
	out, err := pipeline.Process(event)
	if err != nil {
		t.Fatal(err)
	}
	wire, err := proto.MarshalOptions{Deterministic: true}.Marshal(out)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(wire, []byte("rename-chain-secret")) ||
		!bytes.Contains(out.GetRaw(), []byte("MASKED")) ||
		!bytes.Contains(out.GetRaw(), []byte(`"safe":"kept"`)) {
		t.Fatalf("durable rename-chain redaction = raw:%q event:%+v", out.GetRaw(), out)
	}
	fields := out.GetFields().GetFields()
	if len(fields) != 2 || fields[0].GetName() != "customer_credential" ||
		fields[0].GetValue().GetStringValue() != "MASKED" {
		t.Fatalf("durable rename-chain fields = %+v", fields)
	}
}

func TestBuildProcessorRuntimeSharesResolvedProcessorSemantics(t *testing.T) {
	t.Parallel()

	pipeline, redactor, err := buildProcessorRuntime([]config.ProcessorConfig{{
		Type: "redact", Fields: []string{"customer_credential"},
	}})
	if err != nil {
		t.Fatal(err)
	}
	if len(pipeline.processors) != 1 {
		t.Fatalf("compiled processors = %d, want 1", len(pipeline.processors))
	}
	configured, ok := pipeline.processors[0].(*redactProcessor)
	if !ok {
		t.Fatalf("compiled processor type = %T, want *redactProcessor", pipeline.processors[0])
	}
	if configured.replacement != ingest.DefaultRedactionReplacement ||
		redactor.configuredReplacement["customer_credential"] != configured.replacement {
		t.Fatalf(
			"resolved replacement drifted: pipeline=%q durable=%q",
			configured.replacement,
			redactor.configuredReplacement["customer_credential"],
		)
	}
}

func TestBuildDurableRedactorRejectsUnsupportedProcessor(t *testing.T) {
	t.Parallel()

	unsupported := pipeFunc{
		fn: func(event *opensplunk.LogEvent) (*opensplunk.LogEvent, error) {
			return event, nil
		},
	}
	if _, err := buildDurableRedactor(NewPipeline(unsupported)); err == nil {
		t.Fatal("unsupported processor did not fail closed at the durable boundary")
	}
}

func TestBuildProcessorRuntimeEmptyPipelineRetainsMandatoryBoundary(t *testing.T) {
	t.Parallel()

	pipeline, redactor, err := buildProcessorRuntime(nil)
	if err != nil {
		t.Fatal(err)
	}
	raw := `{"token":"mandatory-secret","safe":"kept"}`
	event := testEvent(t, testDecoder(t), 0, uint64(len(raw)), 1, raw)
	sanitized := redactor.beforePipeline(event, nil)
	out, err := pipeline.Process(sanitized)
	if err != nil {
		t.Fatal(err)
	}
	if out != event {
		t.Fatal("empty processor runtime cloned an exclusively owned event")
	}
	if bytes.Contains(out.GetRaw(), []byte("mandatory-secret")) ||
		!bytes.Contains(out.GetRaw(), []byte(ingest.DefaultRedactionReplacement)) {
		t.Fatalf("empty processor runtime did not retain mandatory redaction: %q", out.GetRaw())
	}
}

func TestDurableRedactorPreventsRenameDeclassification(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		processors  []config.ProcessorConfig
		raw         string
		finalField  string
		replacement string
	}{
		{
			name: "mandatory sensitive source to ordinary destination",
			processors: []config.ProcessorConfig{
				{Type: "rename", From: "token", To: "public_value"},
			},
			raw:         `{"token":"mandatory-declassification-secret","safe":"kept"}`,
			finalField:  "public_value",
			replacement: ingest.DefaultRedactionReplacement,
		},
		{
			name: "transient sensitive name in rename chain",
			processors: []config.ProcessorConfig{
				{Type: "rename", From: "credential", To: "token"},
				{Type: "rename", From: "token", To: "public_value"},
			},
			raw:         `{"credential":"transient-declassification-secret","safe":"kept"}`,
			finalField:  "public_value",
			replacement: ingest.DefaultRedactionReplacement,
		},
		{
			name: "configured sensitive source renamed before ordered processor",
			processors: []config.ProcessorConfig{
				{Type: "rename", From: "customer_credential", To: "public_value"},
				{Type: "redact", Fields: []string{"customer_credential"}, Replacement: "MASKED"},
			},
			raw:         `{"customer_credential":"configured-declassification-secret","safe":"kept"}`,
			finalField:  "public_value",
			replacement: "MASKED",
		},
		{
			name: "configured destination reached after ordered processor",
			processors: []config.ProcessorConfig{
				{Type: "redact", Fields: []string{"customer_credential"}, Replacement: "MASKED"},
				{Type: "rename", From: "credential", To: "customer_credential"},
			},
			raw:         `{"credential":"configured-destination-declassification-secret","safe":"kept"}`,
			finalField:  "customer_credential",
			replacement: "MASKED",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			pipeline, err := buildPipeline(test.processors)
			if err != nil {
				t.Fatal(err)
			}
			redactor, err := buildDurableRedactor(pipeline)
			if err != nil {
				t.Fatal(err)
			}
			event := testEvent(t, testDecoder(t), 0, uint64(len(test.raw)), 1, test.raw)
			event = redactor.beforePipeline(event, nil)
			out, err := pipeline.Process(event)
			if err != nil {
				t.Fatal(err)
			}
			wire, err := proto.MarshalOptions{Deterministic: true}.Marshal(out)
			if err != nil {
				t.Fatal(err)
			}
			if bytes.Contains(wire, []byte("declassification-secret")) ||
				!bytes.Contains(out.GetRaw(), []byte(test.replacement)) ||
				!bytes.Contains(out.GetRaw(), []byte(`"safe":"kept"`)) {
				t.Fatalf("durable rename declassification = raw:%q event:%+v", out.GetRaw(), out)
			}
			var final *opensplunk.TypedValue
			for _, field := range out.GetFields().GetFields() {
				if field.GetName() == test.finalField {
					final = field.GetValue()
					break
				}
			}
			if final.GetStringValue() != test.replacement {
				t.Fatalf("durable final field %q = %+v, want %q", test.finalField, final, test.replacement)
			}
		})
	}
}

func TestDurableRedactorDoesNotRedactNoOpRenameDestination(t *testing.T) {
	t.Parallel()

	processors := []config.ProcessorConfig{
		{Type: "rename", From: "token", To: "public_value"},
	}
	pipeline, err := buildPipeline(processors)
	if err != nil {
		t.Fatal(err)
	}
	redactor, err := buildDurableRedactor(pipeline)
	if err != nil {
		t.Fatal(err)
	}
	raw := `{"public_value":"safe-business-data","safe":"kept"}`
	event := testEvent(t, testDecoder(t), 0, uint64(len(raw)), 1, raw)
	event = redactor.beforePipeline(event, nil)
	out, err := pipeline.Process(event)
	if err != nil {
		t.Fatal(err)
	}

	if !bytes.Contains(out.GetRaw(), []byte(`"public_value":"safe-business-data"`)) {
		t.Fatalf("no-op rename corrupted raw destination data: %q", out.GetRaw())
	}
	fields := make(map[string]string)
	for _, field := range out.GetFields().GetFields() {
		fields[field.GetName()] = field.GetValue().GetStringValue()
	}
	if fields["public_value"] != "safe-business-data" || fields["safe"] != "kept" {
		t.Fatalf("no-op rename corrupted structured destination data: %+v", fields)
	}
}

func TestDurableRedactorSkipsLineageWithoutOrdinaryRenameSource(t *testing.T) {
	t.Parallel()

	pipeline, err := buildPipeline([]config.ProcessorConfig{
		{Type: "rename", From: "ordinary_source", To: "token"},
	})
	if err != nil {
		t.Fatal(err)
	}
	redactor, err := buildDurableRedactor(pipeline)
	if err != nil {
		t.Fatal(err)
	}
	raw := `{"unrelated":"safe-business-data"}`
	event := testEvent(t, testDecoder(t), 0, uint64(len(raw)), 1, raw)
	if aliases := redactor.activeAliases(event, nil); aliases != nil {
		t.Fatalf("unrelated event activated rename aliases: %+v", aliases)
	}
	if out := redactor.beforePipeline(event, nil); out != event {
		t.Fatal("in-place durability sanitizer replaced an exclusively owned event")
	}
	if !bytes.Equal(event.GetRaw(), []byte(raw)) {
		t.Fatalf("unrelated event raw changed: %q", event.GetRaw())
	}
}

func TestDurableRedactorPreservesOrderedReplacementSemantics(t *testing.T) {
	t.Parallel()

	processors := []config.ProcessorConfig{
		{Type: "redact", Fields: []string{"A"}, Replacement: "RA"},
		{Type: "redact", Fields: []string{"B"}, Replacement: "RB"},
		{Type: "rename", From: "A", To: "B"},
	}
	pipeline, err := buildPipeline(processors)
	if err != nil {
		t.Fatal(err)
	}
	redactor, err := buildDurableRedactor(pipeline)
	if err != nil {
		t.Fatal(err)
	}
	raw := `{"A":"secret-a","B":"secret-b","safe":"kept"}`
	event := redactor.beforePipeline(testEvent(t, testDecoder(t), 0, uint64(len(raw)), 1, raw), nil)
	out, err := pipeline.Process(event)
	if err != nil {
		t.Fatal(err)
	}
	fields := out.GetFields().GetFields()
	if len(fields) != 2 || fields[0].GetName() != "B" ||
		fields[0].GetValue().GetStringValue() != "RA" ||
		fields[1].GetName() != "safe" || fields[1].GetValue().GetStringValue() != "kept" {
		t.Fatalf("ordered conflicting replacements = %+v", fields)
	}
	if !bytes.Contains(out.GetRaw(), []byte(`"A":"RA"`)) ||
		!bytes.Contains(out.GetRaw(), []byte(`"B":"RB"`)) {
		t.Fatalf("direct raw replacement policies = %q", out.GetRaw())
	}
}

func TestDurableRedactorTracksExactOrderedRenameLineage(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		processors []config.ProcessorConfig
		raw        string
		wantName   string
		wantValue  string
	}{
		{
			name: "later source cannot traverse an earlier rename",
			processors: []config.ProcessorConfig{
				{Type: "rename", From: "A", To: "B"},
				{Type: "rename", From: "C", To: "A"},
				{Type: "redact", Fields: []string{"B"}, Replacement: "MASKED"},
			},
			raw:       `{"C":"ordered-safe-data"}`,
			wantName:  "A",
			wantValue: "ordered-safe-data",
		},
		{
			name: "denied source cannot reach sensitive destination",
			processors: []config.ProcessorConfig{
				{Type: "deny", Fields: []string{"C"}},
				{Type: "rename", From: "C", To: "token"},
			},
			raw:       `{"C":"denied-safe-data"}`,
			wantValue: "denied-safe-data",
		},
		{
			name: "normalized lookalike is not exact source",
			processors: []config.ProcessorConfig{
				{Type: "rename", From: "userID", To: "token"},
			},
			raw:       `{"user_id":"lookalike-safe-data","nested":{"userID":"nested-safe-data"}}`,
			wantName:  "user_id",
			wantValue: "lookalike-safe-data",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			pipeline, err := buildPipeline(test.processors)
			if err != nil {
				t.Fatal(err)
			}
			redactor, err := buildDurableRedactor(pipeline)
			if err != nil {
				t.Fatal(err)
			}
			event := redactor.beforePipeline(
				testEvent(t, testDecoder(t), 0, uint64(len(test.raw)), 1, test.raw),
				nil,
			)
			out, err := pipeline.Process(event)
			if err != nil {
				t.Fatal(err)
			}
			if test.wantValue != "" && !bytes.Contains(out.GetRaw(), []byte(test.wantValue)) {
				t.Fatalf("ordered lineage corrupted raw: %q", out.GetRaw())
			}
			if test.wantName == "" {
				if len(out.GetFields().GetFields()) != 0 {
					t.Fatalf("denied lineage fields = %+v", out.GetFields().GetFields())
				}
				return
			}
			var got string
			for _, field := range out.GetFields().GetFields() {
				if field.GetName() == test.wantName {
					got = field.GetValue().GetStringValue()
				}
			}
			if got != test.wantValue {
				t.Fatalf("ordered lineage field %q = %q, want %q", test.wantName, got, test.wantValue)
			}
		})
	}
}

func TestDurableRedactorHandlesPunctuationAliasAndPreservesNestedLookalike(t *testing.T) {
	t.Parallel()

	processors := []config.ProcessorConfig{{Type: "rename", From: "---", To: "token"}}
	pipeline, err := buildPipeline(processors)
	if err != nil {
		t.Fatal(err)
	}
	redactor, err := buildDurableRedactor(pipeline)
	if err != nil {
		t.Fatal(err)
	}
	raw := `{"---":"punctuation-secret","nested":{"---":"nested-safe"},"safe":"kept"}`
	event := redactor.beforePipeline(testEvent(t, testDecoder(t), 0, uint64(len(raw)), 1, raw), nil)
	out, err := pipeline.Process(event)
	if err != nil {
		t.Fatal(err)
	}
	wire, err := proto.MarshalOptions{Deterministic: true}.Marshal(out)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(wire, []byte("punctuation-secret")) ||
		!bytes.Contains(out.GetRaw(), []byte(`"---":"nested-safe"`)) {
		t.Fatalf("punctuation alias redaction = raw:%q event:%+v", out.GetRaw(), out)
	}
	fields := out.GetFields().GetFields()
	if fields[0].GetName() != "token" ||
		fields[0].GetValue().GetStringValue() != ingest.DefaultRedactionReplacement ||
		fields[1].GetValue().GetObjectValue().GetFields()[0].GetValue().GetStringValue() != "nested-safe" {
		t.Fatalf("punctuation alias fields = %+v", fields)
	}
}

func TestDurableRedactorSeparatesConstantAliasFromRawAndMessageProvenance(t *testing.T) {
	t.Parallel()

	processors := []config.ProcessorConfig{{Type: "rename", From: "userID", To: "token"}}
	pipeline, err := buildPipeline(processors)
	if err != nil {
		t.Fatal(err)
	}
	redactor, err := buildDurableRedactor(pipeline)
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name   string
		format InputFormat
		raw    string
	}{
		{
			name:   "raw input",
			format: InputFormatRaw,
			raw:    "userID=ordinary-raw-business-data",
		},
		{
			name:   "NDJSON constant override",
			format: InputFormatNDJSON,
			raw:    `{"userID":"ordinary-json-business-data","message":"userID=ordinary-message-business-data"}`,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			decoder, decodeErr := NewDecoder(DecodeConfig{
				Format: test.format, InputID: "app", IndexName: "main",
				Source: "s", Sourcetype: "st", Host: "h",
				ConstantFields: &opensplunk.TypedObject{Fields: []*opensplunk.TypedObjectField{{
					Name: "userID",
					Value: &opensplunk.TypedValue{
						Kind: &opensplunk.TypedValue_StringValue{StringValue: "constant-secret"},
					},
				}}},
			})
			if decodeErr != nil {
				t.Fatal(decodeErr)
			}
			event, decodeErr := decoder.Decode(
				[]byte(test.raw),
				SourcePosition{
					FileIdentity: "dev=1;ino=2;fp=abc",
					EndOffset:    uint64(len(test.raw)),
					LineNumber:   1,
				},
				time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
			)
			if decodeErr != nil {
				t.Fatal(decodeErr)
			}
			event = redactor.beforePipeline(event, decoder.constantNames)
			out, processErr := pipeline.Process(event)
			if processErr != nil {
				t.Fatal(processErr)
			}
			wire, marshalErr := proto.MarshalOptions{Deterministic: true}.Marshal(out)
			if marshalErr != nil {
				t.Fatal(marshalErr)
			}
			if bytes.Contains(wire, []byte("constant-secret")) ||
				!bytes.Contains(out.GetRaw(), []byte("ordinary")) ||
				!strings.Contains(out.GetMessage(), "ordinary") {
				t.Fatalf("constant alias provenance = raw:%q message:%q event:%+v",
					out.GetRaw(), out.GetMessage(), out)
			}
			fields := out.GetFields().GetFields()
			if len(fields) != 1 || fields[0].GetName() != "token" ||
				fields[0].GetValue().GetStringValue() != ingest.DefaultRedactionReplacement {
				t.Fatalf("constant alias structured fields = %+v", fields)
			}
		})
	}
}

func TestDurableRedactorPreservesExactConfiguredPoliciesAndDeduplicatesMandatoryDefaults(t *testing.T) {
	t.Parallel()

	redundantPipeline, err := buildPipeline([]config.ProcessorConfig{{
		Type: "redact", Fields: []string{"token", "authorization", "private_key"},
		Replacement: ingest.DefaultRedactionReplacement,
	}})
	if err != nil {
		t.Fatal(err)
	}
	redundant, err := buildDurableRedactor(redundantPipeline)
	if err != nil {
		t.Fatal(err)
	}
	if len(redundant.configured) != 0 {
		t.Fatalf("redundant mandatory policy was not eliminated: %+v", redundant)
	}
	groupedPipeline, err := buildPipeline([]config.ProcessorConfig{
		{Type: "redact", Fields: []string{"customer_a"}, Replacement: "MASKED"},
		{Type: "redact", Fields: []string{"customer_b"}, Replacement: "MASKED"},
		{Type: "redact", Fields: []string{"customer_c"}, Replacement: "MASKED"},
	})
	if err != nil {
		t.Fatal(err)
	}
	grouped, err := buildDurableRedactor(groupedPipeline)
	if err != nil {
		t.Fatal(err)
	}
	if len(grouped.configured) != 1 {
		t.Fatalf("same final replacement compiled to %d scans, want 1", len(grouped.configured))
	}

	processors := []config.ProcessorConfig{
		{Type: "redact", Fields: []string{"customerCredential"}, Replacement: "CAMEL"},
		{Type: "redact", Fields: []string{"customer_credential"}, Replacement: "SNAKE"},
	}
	pipeline, err := buildPipeline(processors)
	if err != nil {
		t.Fatal(err)
	}
	redactor, err := buildDurableRedactor(pipeline)
	if err != nil {
		t.Fatal(err)
	}
	if len(redactor.configured) != 1 {
		t.Fatalf("distinct configured markers compiled to %d direct passes, want one composite pass",
			len(redactor.configured))
	}
	event := testEvent(
		t,
		testDecoder(t),
		0,
		128,
		1,
		`{"customerCredential":"camel-secret","customer_credential":"snake-secret","safe":"kept"}`,
	)
	event = redactor.beforePipeline(event, nil)
	out, err := pipeline.Process(event)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(out.GetRaw(), []byte("camel-secret")) ||
		bytes.Contains(out.GetRaw(), []byte("snake-secret")) ||
		!bytes.Contains(out.GetRaw(), []byte(`"customerCredential":"CAMEL"`)) ||
		!bytes.Contains(out.GetRaw(), []byte(`"customer_credential":"SNAKE"`)) {
		t.Fatalf("configured raw replacement semantics = %q", out.GetRaw())
	}
	fields := make(map[string]string)
	for _, field := range out.GetFields().GetFields() {
		fields[field.GetName()] = field.GetValue().GetStringValue()
	}
	if fields["customerCredential"] != "CAMEL" ||
		fields["customer_credential"] != "SNAKE" ||
		fields["safe"] != "kept" {
		t.Fatalf("configured structured replacement semantics = %+v", fields)
	}
}

func TestDurableRedactorKeepsLastConfiguredReplacementAfterMandatoryPolicy(t *testing.T) {
	t.Parallel()

	processors := []config.ProcessorConfig{
		{Type: "redact", Fields: []string{"token"}, Replacement: "CUSTOM"},
		{Type: "redact", Fields: []string{"token"}, Replacement: ingest.DefaultRedactionReplacement},
	}
	pipeline, err := buildPipeline(processors)
	if err != nil {
		t.Fatal(err)
	}
	redactor, err := buildDurableRedactor(pipeline)
	if err != nil {
		t.Fatal(err)
	}
	if len(redactor.configured) != 0 {
		t.Fatalf("final mandatory replacement should need no supplemental scan: %d", len(redactor.configured))
	}
	raw := `{"message":"token=message-secret","token":"structured-secret","safe":"kept"}`
	event := redactor.beforePipeline(
		testEvent(t, testDecoder(t), 0, uint64(len(raw)), 1, raw),
		nil,
	)
	out, err := pipeline.Process(event)
	if err != nil {
		t.Fatal(err)
	}
	wire, err := proto.MarshalOptions{Deterministic: true}.Marshal(out)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(wire, []byte("CUSTOM")) ||
		bytes.Contains(wire, []byte("message-secret")) ||
		bytes.Contains(wire, []byte("structured-secret")) ||
		!bytes.Contains(out.GetRaw(), []byte(ingest.DefaultRedactionReplacement)) ||
		!strings.Contains(out.GetMessage(), ingest.DefaultRedactionReplacement) {
		t.Fatalf("last configured replacement did not win: raw:%q message:%q event:%+v",
			out.GetRaw(), out.GetMessage(), out)
	}
}

func TestDurableRedactorConfiguredReplacementOverridesMandatoryMarkerForResolvedFields(t *testing.T) {
	t.Parallel()

	pipeline, err := buildPipeline([]config.ProcessorConfig{
		{Type: "redact", Fields: []string{"token"}, Replacement: "CUSTOM"},
		{Type: "redact", Fields: []string{"customer_pin"}, Replacement: "PIN"},
	})
	if err != nil {
		t.Fatal(err)
	}
	redactor, err := buildDurableRedactor(pipeline)
	if err != nil {
		t.Fatal(err)
	}
	if len(redactor.configured) != 1 {
		t.Fatalf("mandatory override plus neutral marker compiled to %d direct passes, want one",
			len(redactor.configured))
	}

	raw := `{"message":"token=message-secret customer_pin=pin-secret safe=value",` +
		`"token":"structured-secret","customer_pin":"structured-pin","safe":"kept"}`
	event := redactor.beforePipeline(
		testEvent(t, testDecoder(t), 0, uint64(len(raw)), 1, raw),
		nil,
	)
	out, err := pipeline.Process(event)
	if err != nil {
		t.Fatal(err)
	}
	wire, err := proto.MarshalOptions{Deterministic: true}.Marshal(out)
	if err != nil {
		t.Fatal(err)
	}
	for _, secret := range []string{"message-secret", "pin-secret", "structured-secret", "structured-pin"} {
		if bytes.Contains(wire, []byte(secret)) {
			t.Fatalf("configured override leaked %q", secret)
		}
	}
	if !bytes.Contains(out.GetRaw(), []byte(`"token":"CUSTOM"`)) ||
		!bytes.Contains(out.GetRaw(), []byte(`"customer_pin":"PIN"`)) ||
		!strings.Contains(out.GetMessage(), "CUSTOM") ||
		!strings.Contains(out.GetMessage(), "PIN") {
		t.Fatalf("configured override markers = raw:%q message:%q", out.GetRaw(), out.GetMessage())
	}
	fields := make(map[string]string)
	for _, field := range out.GetFields().GetFields() {
		fields[field.GetName()] = field.GetValue().GetStringValue()
	}
	if fields["token"] != "CUSTOM" || fields["customer_pin"] != "PIN" || fields["safe"] != "kept" {
		t.Fatalf("configured override fields = %+v", fields)
	}
}

func TestDurableRedactorMandatoryFailClosedBoundaryPrecedesConfiguredReplacement(t *testing.T) {
	t.Parallel()

	pipeline, err := buildPipeline([]config.ProcessorConfig{{
		Type: "redact", Fields: []string{"token"}, Replacement: "CUSTOM",
	}})
	if err != nil {
		t.Fatal(err)
	}
	redactor, err := buildDurableRedactor(pipeline)
	if err != nil {
		t.Fatal(err)
	}

	event := testEvent(t, testDecoder(t), 0, 16, 1, `{"safe":"kept"}`)
	ambiguous := `token=\"boundary-secret\" safe=value`
	event.Raw = []byte(ambiguous)
	event.Message = &ambiguous
	event.Fields = nil
	event = redactor.beforePipeline(event, nil)
	out, err := pipeline.Process(event)
	if err != nil {
		t.Fatal(err)
	}
	if string(out.GetRaw()) != ingest.DefaultRedactionReplacement ||
		out.GetMessage() != ingest.DefaultRedactionReplacement {
		t.Fatalf("mandatory fail-closed marker = raw:%q message:%q, want %q",
			out.GetRaw(), out.GetMessage(), ingest.DefaultRedactionReplacement)
	}
	if bytes.Contains(out.GetRaw(), []byte("boundary-secret")) ||
		strings.Contains(out.GetMessage(), "boundary-secret") ||
		bytes.Contains(out.GetRaw(), []byte("CUSTOM")) ||
		strings.Contains(out.GetMessage(), "CUSTOM") {
		t.Fatalf("fail-closed boundary leaked or was reclassified: raw:%q message:%q",
			out.GetRaw(), out.GetMessage())
	}
}

// TestDaemonDecodeFailurePolicy confirms a malformed line is skipped and
// counted while valid lines around it are durably queued. With acknowledgments
// withheld, even a later valid event must not advance the discovery checkpoint.
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
	waitFor(t, 2*time.Second, "checkpoint stays at discovery offset", func() bool {
		cps, err := d.checkpoints.List()
		return err == nil && len(cps) == 1 && cps[0].Offset == 0
	})

	cancel()
	if err := <-runErr; err != nil {
		t.Fatalf("Run returned error: %v", err)
	}
	if got := d.DecodeFailures(); got != 1 {
		t.Fatalf("DecodeFailures = %d, want 1", got)
	}
	if got := d.localDroppedEvents(); got != 1 {
		t.Fatalf("localDroppedEvents = %d, want 1", got)
	}
}

type fixedHealthManager struct {
	input.Manager
	health input.Health
}

func (manager fixedHealthManager) Health() input.Health { return manager.health }

func TestInputHealthSnapshotSanitizesFleetBoundary(t *testing.T) {
	t.Parallel()
	invalidStatus := string([]byte{0xff}) + "line\nwith\x00controls" +
		strings.Repeat("x", maximumReportedInputStatusBytes+128)
	inputs := []*inputRuntime{{
		id: "input",
		manager: fixedHealthManager{health: input.Health{
			InputID: "input", State: opensplunk.CollectorInputState_COLLECTOR_INPUT_STATE_ERROR,
			StatusMessage: invalidStatus, DiscoveredSources: 1, ActiveSources: 2,
			EventsReadTotal: ^uint64(0), BytesReadTotal: ^uint64(0),
		}},
	}}

	got := inputHealthSnapshot(inputs)
	if len(got) != 1 {
		t.Fatalf("input health count = %d, want 1", len(got))
	}
	health := got[0]
	if !utf8.ValidString(health.GetStatusMessage()) ||
		len(health.GetStatusMessage()) > maximumReportedInputStatusBytes {
		t.Fatalf("status is not bounded valid UTF-8: %q", health.GetStatusMessage())
	}
	for _, character := range health.GetStatusMessage() {
		if unicode.IsControl(character) {
			t.Fatalf("status retained control character %U", character)
		}
	}
	if health.GetDiscoveredSources() != 2 || health.GetActiveSources() != 2 {
		t.Fatalf("source counts = discovered:%d active:%d, want 2/2",
			health.GetDiscoveredSources(), health.GetActiveSources())
	}
	const maximumFleetCounter = uint64(1<<63 - 1)
	if health.GetEventsReadTotal() != maximumFleetCounter ||
		health.GetBytesReadTotal() != maximumFleetCounter {
		t.Fatalf("counters = events:%d bytes:%d, want saturated %d",
			health.GetEventsReadTotal(), health.GetBytesReadTotal(), maximumFleetCounter)
	}
}

func TestLocalDroppedEventsSaturatesWithoutIntermediateWrap(t *testing.T) {
	t.Parallel()
	var daemon Daemon
	daemon.decodeFailures.Store(collectorlimits.MaximumFleetCounter - 1)
	daemon.pipelineFailures.Store(10)
	daemon.policyDrops.Store(^uint64(0))
	daemon.oversizedDrops.Store(20)

	if got := daemon.localDroppedEvents(); got != collectorlimits.MaximumFleetCounter {
		t.Fatalf("localDroppedEvents = %d, want saturated %d", got, collectorlimits.MaximumFleetCounter)
	}
}

func TestCollectorBoundaryTextValidation(t *testing.T) {
	t.Parallel()
	if !validCollectorBoundaryText("host.example", 255, false) {
		t.Fatal("valid hostname rejected")
	}
	for _, invalid := range []string{"", "bad\nhost", string([]byte{0xff}), strings.Repeat("x", 256)} {
		if validCollectorBoundaryText(invalid, 255, false) {
			t.Fatalf("invalid boundary text accepted: %q", invalid)
		}
	}
}

// testEvent decodes raw at the given position into a LogEvent using a decoder
// with fixed metadata, at a fixed collection time so encoded sizes are stable.
func testEvent(t *testing.T, dec *Decoder, start, end, line uint64, raw string) *opensplunk.LogEvent {
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
	identity := input.FileIdentity{
		Device: 1, Inode: 2, Generation: 1,
		Fingerprint: strings.Repeat("ab", 32), FingerprintLength: 64,
	}
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
		lastOffsets:        make(map[inputFileKey]uint64),
	}

	ctx := t.Context()
	processed := make(chan processedEvent)
	batcherDone := make(chan error, 1)
	go func() { batcherDone <- d.runBatcher(ctx, processed) }()

	mk := func(start, end, line uint64) processedEvent {
		return processedEvent{
			event: testEvent(t, dec, start, end, line, raw), inputID: "app", identity: identity,
			path: "/x.log", endOffset: end, lineNumber: line, nextLineNumber: line + 1,
			size: proto.Size(testEvent(t, dec, start, end, line, raw)),
		}
	}

	if err := cps.Set(input.Checkpoint{
		InputID: "app", Identity: identity, Path: "/x.log", Offset: 0,
	}); err != nil {
		t.Fatalf("seed discovery checkpoint: %v", err)
	}

	// First event fits and is appended durably; its checkpoint does not advance.
	processed <- mk(0, 100, 1)
	waitFor(t, time.Second, "first batch appended", func() bool {
		return q.Stats().QueuedBatches == 1
	})
	if cp, ok, _ := cps.Get("app", identity); !ok || cp.Offset != 0 {
		t.Fatalf("checkpoint after first append = %+v (ok=%t), want discovery offset 0", cp, ok)
	}

	// Second event is handed to the batcher, which now blocks on ErrQueueFull.
	processed <- mk(100, 200, 2)

	// Because the batcher is stuck flushing, it stops draining: a third send
	// blocks, proving input consumption is paused rather than dropping data.
	select {
	case processed <- mk(200, 300, 3):
		t.Fatal("expected backpressure: send should block while queue is full")
	case <-time.After(150 * time.Millisecond):
	}
	// The queue did not grow and no unacknowledged source coordinate advanced.
	if got := q.Stats().QueuedBatches; got != 1 {
		t.Fatalf("QueuedBatches while full = %d, want 1", got)
	}
	if cp, _, _ := cps.Get("app", identity); cp.Offset != 0 {
		t.Fatalf("checkpoint advanced to %d without terminal ack, want 0", cp.Offset)
	}

	// Free space: the stuck batch is now appended, but direct WAL Ack is not the
	// sender's terminal transaction and therefore still cannot move checkpoints.
	if err := q.Ack(1); err != nil {
		t.Fatalf("Ack: %v", err)
	}
	waitFor(t, time.Second, "second batch appended after ack", func() bool {
		return q.Stats().QueuedBatches == 1 && q.Stats().NextBatchSequence == 3
	})
	if cp, ok, _ := cps.Get("app", identity); !ok || cp.Offset != 0 {
		t.Fatalf("checkpoint after second append = %+v (ok=%t), want discovery offset 0", cp, ok)
	}

	// Closing the stream (not ctx) is how the batcher terminates, mirroring the
	// daemon where the input readers close the processed channel on shutdown.
	close(processed)
	if err := <-batcherDone; err != nil {
		t.Fatalf("runBatcher: %v", err)
	}
}

func TestDaemonTerminalAckAdvancesCheckpointOnlyAfterDisposition(t *testing.T) {
	t.Parallel()
	d, identity := newTerminalCheckpointTestDaemon(t)
	if err := d.flush(context.Background(), terminalPendingBatch(identity, 0, 100, 1)); err != nil {
		t.Fatalf("flush: %v", err)
	}
	if cp, ok, _ := d.checkpoints.Get(testCheckpointInputID, identity); !ok || cp.Offset != 0 {
		t.Fatalf("checkpoint after withheld ack = %+v (ok=%t), want 0", cp, ok)
	}

	prepared, err := d.queue.PrepareAck(1)
	if err != nil {
		t.Fatalf("PrepareAck: %v", err)
	}
	if _, err := commitTerminalCheckpoints(d.checkpoints, prepared.Marks); err != nil {
		t.Fatalf("commitTerminalCheckpoints: %v", err)
	}
	if err := d.queue.Ack(1); err != nil {
		t.Fatalf("Ack: %v", err)
	}
	if cp, ok, _ := d.checkpoints.Get(testCheckpointInputID, identity); !ok || cp.Offset != 100 {
		t.Fatalf("checkpoint after terminal ack = %+v (ok=%t), want 100", cp, ok)
	}
}

func TestDaemonOutOfOrderTerminalAckDoesNotAdvancePastGap(t *testing.T) {
	t.Parallel()
	d, identity := newTerminalCheckpointTestDaemon(t)
	for sequence, bounds := range [][2]uint64{{0, 100}, {100, 200}} {
		if err := d.flush(context.Background(), terminalPendingBatch(identity, bounds[0], bounds[1], uint64(sequence+1))); err != nil {
			t.Fatalf("flush batch %d: %v", sequence+1, err)
		}
	}

	prepared, err := d.queue.PrepareAck(2)
	if err != nil {
		t.Fatalf("PrepareAck(2): %v", err)
	}
	if len(prepared.Marks) != 0 {
		t.Fatalf("out-of-order PrepareAck(2) returned %d marks, want none", len(prepared.Marks))
	}
	if err := d.queue.Ack(2); err != nil {
		t.Fatalf("Ack(2): %v", err)
	}
	if cp, _, _ := d.checkpoints.Get(testCheckpointInputID, identity); cp.Offset != 0 {
		t.Fatalf("out-of-order ack advanced checkpoint to %d, want 0", cp.Offset)
	}

	prepared, err = d.queue.PrepareAck(1)
	if err != nil {
		t.Fatalf("PrepareAck(1): %v", err)
	}
	if len(prepared.Marks) != 1 {
		t.Fatalf("PrepareAck(1) returned %d marks, want one coalesced source", len(prepared.Marks))
	}
	if _, err := commitTerminalCheckpoints(d.checkpoints, prepared.Marks); err != nil {
		t.Fatalf("commitTerminalCheckpoints: %v", err)
	}
	if err := d.queue.Ack(1); err != nil {
		t.Fatalf("Ack(1): %v", err)
	}
	if cp, ok, _ := d.checkpoints.Get(testCheckpointInputID, identity); !ok || cp.Offset != 200 {
		t.Fatalf("checkpoint after gap closure = %+v (ok=%t), want 200", cp, ok)
	}
}

func newTerminalCheckpointTestDaemon(t *testing.T) (*Daemon, input.FileIdentity) {
	t.Helper()
	q, err := wal.Open(wal.Options{
		Dir: t.TempDir(), Sync: wal.SyncAlways, CollectorID: "cid",
	})
	if err != nil {
		t.Fatalf("wal.Open: %v", err)
	}
	t.Cleanup(func() { _ = q.Close() })
	cps, err := input.NewCheckpointStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewCheckpointStore: %v", err)
	}
	t.Cleanup(func() { _ = cps.Close() })
	identity := input.FileIdentity{
		Device: 1, Inode: 2, Generation: 1,
		Fingerprint: strings.Repeat("ab", 32), FingerprintLength: 128,
	}
	if err := cps.Set(input.Checkpoint{
		InputID: testCheckpointInputID, Identity: identity, Path: "/x.log", Offset: 0,
	}); err != nil {
		t.Fatalf("seed discovery checkpoint: %v", err)
	}
	return &Daemon{
		log: discardLogger(), now: time.Now, queue: q, checkpoints: cps,
		lastOffsets: make(map[inputFileKey]uint64),
	}, identity
}

func terminalPendingBatch(identity input.FileIdentity, start, end, line uint64) *pendingBatch {
	event := checkpointBatch(0, identity, "/x.log", start, end, line).GetEvents()[0]
	batch := &pendingBatch{}
	batch.add(processedEvent{
		event: event, inputID: testCheckpointInputID, identity: identity, path: "/x.log",
		endOffset: end, lineNumber: line, nextLineNumber: line + 1,
		size: proto.Size(event),
	})
	return batch
}

func TestPendingBatchKeepsIdenticalFilesIndependentByInput(t *testing.T) {
	t.Parallel()
	identity := input.FileIdentity{
		Device: 3, Inode: 5, Generation: 1,
		Fingerprint: strings.Repeat("ab", 32), FingerprintLength: 64,
	}
	batch := &pendingBatch{}
	for inputID, offset := range map[string]uint64{"input-a": 100, "input-b": 200} {
		batch.add(processedEvent{
			event:   &opensplunk.LogEvent{EventId: inputID},
			inputID: inputID, identity: identity, path: "/logs/shared.log",
			endOffset: offset, lineNumber: 1, nextLineNumber: 2, size: 1,
		})
	}
	marks := batch.checkpointMarks()
	if len(marks) != 2 {
		t.Fatalf("pending marks = %+v, want one for each input", marks)
	}
	for inputID, wantOffset := range map[string]uint64{"input-a": 100, "input-b": 200} {
		mark, ok := marks[inputFileTrackingKey(inputID, identity)]
		if !ok || mark.inputID != inputID || mark.offset != wantOffset {
			t.Fatalf(
				"pending mark for %q = (%+v, %t), want offset %d",
				inputID, mark, ok, wantOffset,
			)
		}
	}
}

type appendFailQueue struct{ wal.Queue }

func (appendFailQueue) Append([]*opensplunk.LogEvent) (*opensplunk.EventBatch, error) {
	return nil, errors.New("simulated WAL IO failure")
}

func TestBatcherReturnsFatalAppendFailure(t *testing.T) {
	t.Parallel()
	d := &Daemon{
		log: discardLogger(), queue: appendFailQueue{}, batchMaxEvents: 1,
		batchMaxBytes: 1024, batchLinger: time.Hour,
	}
	processed := make(chan processedEvent, 1)
	processed <- processedEvent{event: &opensplunk.LogEvent{EventId: "e1"}, size: 10}
	close(processed)
	if err := d.runBatcher(context.Background(), processed); err == nil || !strings.Contains(err.Error(), "durable append failed") {
		t.Fatalf("runBatcher error = %v, want fatal durable append failure", err)
	}
}

func TestBatcherFlushesBeforeCrossingByteCap(t *testing.T) {
	t.Parallel()
	q, err := wal.Open(wal.Options{Dir: t.TempDir(), Sync: wal.SyncAlways, CollectorID: "cid"})
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
		lastOffsets: make(map[inputFileKey]uint64),
	}
	identity := input.FileIdentity{Device: 1, Inode: 2, Generation: 1, Fingerprint: "fp"}
	processed := make(chan processedEvent, 2)
	for i := range 2 {
		processed <- processedEvent{
			event:   &opensplunk.LogEvent{EventId: fmt.Sprintf("e%d", i)},
			inputID: "input", identity: identity, endOffset: uint64(i + 1), size: 6,
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

	q, err := wal.Open(wal.Options{Dir: t.TempDir(), Sync: wal.SyncAlways, CollectorID: "cid"})
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
		lastOffsets:        make(map[inputFileKey]uint64),
	}

	ctx := context.Background()
	processed := make(chan processedEvent)
	batcherDone := make(chan error, 1)
	go func() { batcherDone <- d.runBatcher(ctx, processed) }()

	processed <- processedEvent{
		event: testEvent(t, dec, 0, 42, 1, `{"k":"v"}`), inputID: "app", identity: identity,
		path: "/x.log", endOffset: 42, lineNumber: 1, nextLineNumber: 2,
		size: proto.Size(testEvent(t, dec, 0, 42, 1, `{"k":"v"}`)),
	}

	// Nothing has flushed yet (below all thresholds).
	if got := q.Stats().QueuedEvents; got != 0 {
		t.Fatalf("QueuedEvents before shutdown = %d, want 0", got)
	}

	// Closing the stream triggers the final partial-batch flush.
	close(processed)
	if err := <-batcherDone; err != nil {
		t.Fatalf("runBatcher: %v", err)
	}

	if got := q.Stats().QueuedEvents; got != 1 {
		t.Fatalf("QueuedEvents after shutdown flush = %d, want 1", got)
	}
	if _, ok, _ := cps.Get("app", identity); ok {
		t.Fatal("shutdown WAL flush advanced a checkpoint without terminal acknowledgment")
	}
}

// measureBatchBytes returns the on-disk size of a single-event batch, used to
// size a bounded queue precisely for the backpressure test.
func measureBatchBytes(t *testing.T, ev *opensplunk.LogEvent) uint64 {
	t.Helper()
	q, err := wal.Open(wal.Options{Dir: t.TempDir(), Sync: wal.SyncAlways, CollectorID: "cid"})
	if err != nil {
		t.Fatalf("measure open: %v", err)
	}
	defer q.Close()
	if _, err := q.Append([]*opensplunk.LogEvent{ev}); err != nil {
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
