package collector

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/collector/config"
	"github.com/Suhaibinator/open-splunk/internal/collector/input"
	"github.com/Suhaibinator/open-splunk/internal/collector/wal"
	"github.com/Suhaibinator/open-splunk/internal/ingest"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

// This file is the collector's product-level end-to-end test: a real file
// fixture is tailed by a Daemon constructed from a real YAML config, decoded,
// processed (redaction), durably queued, and delivered over a real gRPC stream
// to the real internal/ingest.Service backed by an in-memory EventStore. It is
// the "collector fixture -> ingestion -> ClickHouse -> SPL" path of the
// architecture plan, stopping at the storage boundary (the EventStore stands in
// for ClickHouse). Nothing here is mocked between the file on disk and the
// authenticated server: only the terminal store and the authorizer are
// in-memory, exactly as internal/ingest/service_test.go wires them.

const (
	// e2eToken is the bearer token on disk that the authorizer accepts.
	e2eToken = "e2e-super-token"
	// e2eIndex is the single index the token is authorized for.
	e2eIndex = "gradethis"
	// e2eSecret is a token *value* embedded in a payload field; it must never
	// reach the store in any form.
	e2eSecret = "SUPERSECRETTOKENVALUE-do-not-leak"
)

// recordingStore is an idempotent in-memory EventStore. It dedupes by stable
// event_id (mirroring the server's at-least-once contract) so the crash/restart
// and rotation legs can assert that no event is ever committed twice.
type recordingStore struct {
	mu        sync.Mutex
	seen      map[string]struct{}
	committed []*opensplunkv1.LogEvent
	batches   int
	duplicate int
}

func newRecordingStore() *recordingStore {
	return &recordingStore{seen: make(map[string]struct{})}
}

func (s *recordingStore) Store(_ context.Context, batch ingest.StoreBatch) (ingest.StoreResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.batches++
	var accepted, duplicate uint32
	for _, se := range batch.Events {
		id := se.Event.GetEventId()
		if _, ok := s.seen[id]; ok {
			duplicate++
			s.duplicate++
			continue
		}
		s.seen[id] = struct{}{}
		s.committed = append(s.committed, proto.Clone(se.Event).(*opensplunkv1.LogEvent))
		accepted++
	}
	// Accepted + Duplicate must exactly equal the supplied event count, or the
	// server refuses to acknowledge (see ingest.processBatch accounting check).
	return ingest.StoreResult{Accepted: accepted, Duplicate: duplicate, CommittedAt: time.Now()}, nil
}

// snapshot returns a copy of the committed events (independent clones).
func (s *recordingStore) snapshot() []*opensplunkv1.LogEvent {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]*opensplunkv1.LogEvent, len(s.committed))
	for i, ev := range s.committed {
		out[i] = proto.Clone(ev).(*opensplunkv1.LogEvent)
	}
	return out
}

func (s *recordingStore) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.committed)
}

func (s *recordingStore) duplicates() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.duplicate
}

// startIngestServer boots the real internal/ingest.Service on a loopback TCP
// port with an in-memory authorizer (token authorized for e2eIndex) and the
// supplied store. It returns the dial address; the server is stopped on cleanup.
func startIngestServer(t *testing.T, store ingest.EventStore) string {
	t.Helper()
	authorizer := ingest.AuthorizerFunc(func(_ context.Context, token string) (ingest.Authorization, error) {
		if token != e2eToken {
			return ingest.Authorization{}, fmt.Errorf("unexpected token")
		}
		return ingest.Authorization{
			SubjectID:         "e2e-subject",
			TenantID:          "e2e-tenant",
			AuthorizedIndexes: []string{e2eIndex},
		}, nil
	})
	svc, err := ingest.NewService(ingest.DefaultConfig(), authorizer, store)
	if err != nil {
		t.Fatalf("ingest.NewService: %v", err)
	}
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	server := grpc.NewServer()
	opensplunkv1.RegisterCollectorIngestServiceServer(server, svc)
	go func() { _ = server.Serve(lis) }()
	t.Cleanup(server.Stop)
	return lis.Addr().String()
}

// writeE2EConfig writes a real collector YAML file wiring one ndjson file input
// (index gradethis, constant fields including service) and a redact processor
// for the "token" field, over plaintext gRPC to addr with the on-disk token.
func writeE2EConfig(t *testing.T, addr, stateDir, logGlob, tokenFile string) string {
	t.Helper()
	writeFile(t, tokenFile, e2eToken+"\n")
	yaml := fmt.Sprintf(`server:
  address: "%s"
  transport: grpc
  token_file: "%s"
  tls:
    enabled: false
state:
  directory: "%s"
  max_queue_bytes: 8MiB
inputs:
  - id: app
    type: file
    include:
      - "%s"
    format: ndjson
    start_at: beginning
    index: %s
    source: app-log
    sourcetype: json
    host: test-host
    poll_interval: 15ms
    fields:
      environment: prod
      service: checkout
processors:
  - type: redact
    fields:
      - token
    replacement: "[REDACTED]"
`, addr, tokenFile, stateDir, logGlob, e2eIndex)
	path := filepath.Join(t.TempDir(), "collector.yaml")
	writeFile(t, path, yaml)
	return path
}

// newE2EDaemon loads configPath and constructs a Daemon with fast batching for
// tests. It does not start the daemon.
func newE2EDaemon(t *testing.T, configPath string) *Daemon {
	t.Helper()
	cfg, err := config.Load(configPath)
	if err != nil {
		t.Fatalf("config.Load: %v", err)
	}
	d, err := New(cfg, WithLogger(discardLogger()))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	d.batchLinger = 20 * time.Millisecond
	d.drainWindow = 3 * time.Second
	return d
}

// appendFile appends data to path, creating it if necessary.
func appendFile(t *testing.T, path, data string) {
	t.Helper()
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatalf("open %s for append: %v", path, err)
	}
	if _, err := f.WriteString(data); err != nil {
		_ = f.Close()
		t.Fatalf("append %s: %v", path, err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("close %s: %v", path, err)
	}
}

// eventByMessage returns the first committed event whose canonical message
// equals msg, or nil.
func eventByMessage(events []*opensplunkv1.LogEvent, msg string) *opensplunkv1.LogEvent {
	for _, ev := range events {
		if ev.GetMessage() == msg {
			return ev
		}
	}
	return nil
}

// fieldValue returns the dynamic field named name, or nil.
func fieldValue(ev *opensplunkv1.LogEvent, name string) *opensplunkv1.TypedValue {
	for _, f := range ev.GetFields().GetFields() {
		if f.GetName() == name {
			return f.GetValue()
		}
	}
	return nil
}

// assertNoSecret fails the test if secret appears anywhere in any committed
// event: its wire encoding, its text form, its raw body, or any field.
func assertNoSecret(t *testing.T, events []*opensplunkv1.LogEvent, secret string) {
	t.Helper()
	for _, ev := range events {
		wire, err := proto.Marshal(ev)
		if err != nil {
			t.Fatalf("marshal event %s: %v", ev.GetEventId(), err)
		}
		if bytes.Contains(wire, []byte(secret)) {
			t.Fatalf("secret leaked into wire-encoded event %s", ev.GetEventId())
		}
		if strings.Contains(ev.String(), secret) {
			t.Fatalf("secret leaked into event text form %s", ev.GetEventId())
		}
		if bytes.Contains(ev.GetRaw(), []byte(secret)) {
			t.Fatalf("secret leaked into event raw body %s", ev.GetEventId())
		}
	}
}

// assertNoDuplicateEventIDs fails if any event_id appears more than once among
// the committed events.
func assertNoDuplicateEventIDs(t *testing.T, events []*opensplunkv1.LogEvent) {
	t.Helper()
	seen := make(map[string]struct{}, len(events))
	for _, ev := range events {
		id := ev.GetEventId()
		if _, ok := seen[id]; ok {
			t.Fatalf("duplicate event_id committed: %s", id)
		}
		seen[id] = struct{}{}
	}
}

// diskCheckpointOffset opens the on-disk checkpoint store under stateDir and
// returns the single checkpoint's offset, asserting exactly one exists.
func diskCheckpointOffset(t *testing.T, stateDir string) uint64 {
	t.Helper()
	cps, err := input.NewCheckpointStore(filepath.Join(stateDir, checkpointsSubdir))
	if err != nil {
		t.Fatalf("open checkpoint store: %v", err)
	}
	defer cps.Close()
	list, err := cps.List()
	if err != nil {
		t.Fatalf("checkpoint List: %v", err)
	}
	if len(list) != 1 {
		t.Fatalf("checkpoints on disk = %d, want 1", len(list))
	}
	return list[0].Offset
}

// TestE2ECollectorFixtureToIngest is the core product path: a file with a
// normal metrics line, a secret-bearing line, a malformed line, and a
// multibyte UTF-8 line is tailed and delivered to the real server. It asserts
// correct routing/metadata/typed fields, that the secret never reaches the
// store, that the malformed line is skipped-but-counted (pipeline survives),
// that checkpoints advanced on disk, and that the WAL drained to zero.
func TestE2ECollectorFixtureToIngest(t *testing.T) {
	t.Parallel()

	store := newRecordingStore()
	addr := startIngestServer(t, store)

	stateDir := t.TempDir()
	logDir := t.TempDir()
	logPath := filepath.Join(logDir, "app.log")
	configPath := writeE2EConfig(t, addr, stateDir, filepath.Join(logDir, "*.log"), filepath.Join(t.TempDir(), "token"))

	// Order matters: the LAST line must be valid so the checkpoint can advance to
	// end-of-file (a trailing malformed line would never be covered by a later
	// durable event). Each line is newline-terminated so it frames completely.
	normal := `{"message":"request completed","method":"GET","path":"/api/orders","status":200,"duration_ms":12.5}`
	secretLine := `{"message":"login","user":"alice","token":"` + e2eSecret + `"}`
	malformed := `{ this is not valid json`
	utf8Line := `{"message":"héllo wörld 日本語 café ☕","emoji":"🎉"}`
	content := normal + "\n" + secretLine + "\n" + malformed + "\n" + utf8Line + "\n"
	writeFile(t, logPath, content)

	d := newE2EDaemon(t, configPath)
	ctx, cancel := context.WithCancel(context.Background())
	runErr := make(chan error, 1)
	go func() { runErr <- d.Run(ctx) }()

	// Three of four lines are valid; the malformed one is skipped and counted.
	waitFor(t, 10*time.Second, "3 events committed to the store", func() bool {
		return store.count() == 3
	})
	waitFor(t, 5*time.Second, "1 decode failure counted", func() bool {
		return d.DecodeFailures() == 1
	})
	// Delivery reached at-least-once durability: the WAL drains as acks arrive.
	waitFor(t, 5*time.Second, "WAL drains to zero queued batches", func() bool {
		return d.queue.Stats().QueuedBatches == 0
	})
	waitFor(t, 5*time.Second, "checkpoint advanced to end of file", func() bool {
		list, err := d.checkpoints.List()
		return err == nil && len(list) == 1 && list[0].Offset == uint64(len(content))
	})

	cancel()
	if err := <-runErr; err != nil {
		t.Fatalf("Run returned error: %v", err)
	}

	events := store.snapshot()
	if len(events) != 3 {
		t.Fatalf("committed events = %d, want 3", len(events))
	}

	// The secret never appears anywhere in the store — not in fields, not in raw.
	assertNoSecret(t, events, e2eSecret)
	assertNoDuplicateEventIDs(t, events)

	// The malformed line produced no event.
	for _, ev := range events {
		if strings.Contains(string(ev.GetRaw()), "this is not valid json") {
			t.Fatalf("malformed line was ingested: %s", ev.GetRaw())
		}
	}

	// Every event carries the trusted, config-driven metadata, constant fields,
	// and service extracted from the input configuration.
	for _, ev := range events {
		if ev.GetIndexName() != e2eIndex {
			t.Errorf("event %s index = %q, want %q", ev.GetEventId(), ev.GetIndexName(), e2eIndex)
		}
		if ev.GetHost() != "test-host" {
			t.Errorf("event %s host = %q, want test-host", ev.GetEventId(), ev.GetHost())
		}
		if ev.GetSource() != "app-log" {
			t.Errorf("event %s source = %q, want app-log", ev.GetEventId(), ev.GetSource())
		}
		if ev.GetSourcetype() != "json" {
			t.Errorf("event %s sourcetype = %q, want json", ev.GetEventId(), ev.GetSourcetype())
		}
		if ev.GetService() != "checkout" {
			t.Errorf("event %s service = %q, want checkout", ev.GetEventId(), ev.GetService())
		}
		if v := fieldValue(ev, "environment"); v.GetStringValue() != "prod" {
			t.Errorf("event %s field environment = %q, want prod", ev.GetEventId(), v.GetStringValue())
		}
	}

	// The normal line preserves JSON type fidelity: integer stays integer,
	// decimal stays a double.
	metrics := eventByMessage(events, "request completed")
	if metrics == nil {
		t.Fatal("normal metrics event was not committed")
	}
	if got := fieldValue(metrics, "status").GetSint64Value(); got != 200 {
		t.Errorf("status field = %d, want 200 (typed integer)", got)
	}
	if got := fieldValue(metrics, "duration_ms").GetDoubleValue(); got != 12.5 {
		t.Errorf("duration_ms field = %v, want 12.5 (typed double)", got)
	}
	if got := fieldValue(metrics, "method").GetStringValue(); got != "GET" {
		t.Errorf("method field = %q, want GET", got)
	}

	// The secret-bearing event arrived with the token field redacted.
	secretEv := eventByMessage(events, "login")
	if secretEv == nil {
		t.Fatal("secret-bearing event was not committed")
	}
	if got := fieldValue(secretEv, "token").GetStringValue(); got != "[REDACTED]" {
		t.Errorf("token field = %q, want [REDACTED]", got)
	}
	if got := fieldValue(secretEv, "user").GetStringValue(); got != "alice" {
		t.Errorf("user field = %q, want alice", got)
	}

	// The multibyte UTF-8 line survived byte-for-byte in its canonical message.
	utf8Ev := eventByMessage(events, "héllo wörld 日本語 café ☕")
	if utf8Ev == nil {
		t.Fatal("UTF-8 event was not committed (message mismatch)")
	}
	if got := fieldValue(utf8Ev, "emoji").GetStringValue(); got != "🎉" {
		t.Errorf("emoji field = %q, want 🎉", got)
	}

	// Durability survives process exit: checkpoints and the WAL are correct on
	// disk after a clean shutdown.
	if off := diskCheckpointOffset(t, stateDir); off != uint64(len(content)) {
		t.Fatalf("on-disk checkpoint offset = %d, want %d", off, len(content))
	}
	q, err := wal.Open(wal.Options{Dir: filepath.Join(stateDir, walSubdir), Sync: wal.SyncAlways, CollectorID: d.collectorID})
	if err != nil {
		t.Fatalf("reopen WAL: %v", err)
	}
	defer q.Close()
	if got := q.Stats().QueuedBatches; got != 0 {
		t.Fatalf("WAL queued batches after clean shutdown = %d, want 0", got)
	}
}

// TestE2ECollectorCrashRestartResumesWithoutDuplicates stops the daemon, writes
// more lines while it is DOWN, then restarts a fresh daemon over the same state
// directory. The checkpoint governs resume, so the new lines arrive exactly and
// the previously acknowledged events are never re-ingested. The first phase
// intentionally writes more than the fingerprint window (1024 bytes) so the
// file identity is stable across the append, exercising true checkpoint resume.
func TestE2ECollectorCrashRestartResumesWithoutDuplicates(t *testing.T) {
	t.Parallel()

	store := newRecordingStore()
	addr := startIngestServer(t, store)

	stateDir := t.TempDir()
	logDir := t.TempDir()
	logPath := filepath.Join(logDir, "app.log")
	configPath := writeE2EConfig(t, addr, stateDir, filepath.Join(logDir, "*.log"), filepath.Join(t.TempDir(), "token"))

	// A leading padding line pushes the file past the 1024-byte fingerprint
	// window so appends never change the file identity — otherwise the restarted
	// daemon would compute a different fingerprint, miss the checkpoint, and
	// re-read from the beginning (the exact bug this leg must prove absent).
	pad := strings.Repeat("A", 1200)
	phase1 := `{"message":"boot","pad":"` + pad + `"}` + "\n" +
		`{"message":"p1-a","seq":1}` + "\n" +
		`{"message":"p1-b","seq":2}` + "\n"
	writeFile(t, logPath, phase1)

	d1 := newE2EDaemon(t, configPath)
	ctx1, cancel1 := context.WithCancel(context.Background())
	run1 := make(chan error, 1)
	go func() { run1 <- d1.Run(ctx1) }()

	waitFor(t, 10*time.Second, "phase-1 events committed", func() bool {
		return store.count() == 3
	})
	waitFor(t, 5*time.Second, "phase-1 WAL drained", func() bool {
		return d1.queue.Stats().QueuedBatches == 0
	})
	waitFor(t, 5*time.Second, "phase-1 checkpoint at end of file", func() bool {
		list, err := d1.checkpoints.List()
		return err == nil && len(list) == 1 && list[0].Offset == uint64(len(phase1))
	})

	cancel1()
	if err := <-run1; err != nil {
		t.Fatalf("daemon1 Run error: %v", err)
	}

	// Write more lines while the collector is DOWN.
	phase2 := `{"message":"p2-a","seq":3}` + "\n" + `{"message":"p2-b","seq":4}` + "\n"
	appendFile(t, logPath, phase2)

	// Restart a fresh daemon over the same durable state.
	d2 := newE2EDaemon(t, configPath)
	ctx2, cancel2 := context.WithCancel(context.Background())
	run2 := make(chan error, 1)
	go func() { run2 <- d2.Run(ctx2) }()

	waitFor(t, 10*time.Second, "phase-2 events committed", func() bool {
		return store.count() == 5
	})
	waitFor(t, 5*time.Second, "phase-2 WAL drained", func() bool {
		return d2.queue.Stats().QueuedBatches == 0
	})
	waitFor(t, 5*time.Second, "phase-2 checkpoint at end of file", func() bool {
		list, err := d2.checkpoints.List()
		return err == nil && len(list) == 1 && list[0].Offset == uint64(len(phase1)+len(phase2))
	})

	cancel2()
	if err := <-run2; err != nil {
		t.Fatalf("daemon2 Run error: %v", err)
	}

	events := store.snapshot()
	assertNoDuplicateEventIDs(t, events)
	if len(events) != 5 {
		t.Fatalf("committed events = %d, want 5", len(events))
	}
	// The checkpoint governed resume: no acknowledged event was ever re-delivered.
	if dup := store.duplicates(); dup != 0 {
		t.Fatalf("store observed %d duplicate deliveries; the checkpoint failed to prevent re-ingest", dup)
	}
	for _, msg := range []string{"boot", "p1-a", "p1-b", "p2-a", "p2-b"} {
		if eventByMessage(events, msg) == nil {
			t.Errorf("expected committed event with message %q, missing", msg)
		}
	}
	if off := diskCheckpointOffset(t, stateDir); off != uint64(len(phase1)+len(phase2)) {
		t.Fatalf("final on-disk checkpoint = %d, want %d", off, len(phase1)+len(phase2))
	}
}

// TestE2ECollectorRotationContinuity renames the live log out of the glob,
// creates a fresh file at the same path, and appends to both. The renamed
// remainder is drained (no loss) and the fresh file is tailed from the start,
// with no event committed twice through the store.
func TestE2ECollectorRotationContinuity(t *testing.T) {
	t.Parallel()

	store := newRecordingStore()
	addr := startIngestServer(t, store)

	stateDir := t.TempDir()
	logDir := t.TempDir()
	logPath := filepath.Join(logDir, "app.log")
	rotatedPath := filepath.Join(logDir, "app.log.1") // no longer matches *.log
	configPath := writeE2EConfig(t, addr, stateDir, filepath.Join(logDir, "*.log"), filepath.Join(t.TempDir(), "token"))

	writeFile(t, logPath, `{"message":"r-init-1"}`+"\n"+`{"message":"r-init-2"}`+"\n")

	d := newE2EDaemon(t, configPath)
	ctx, cancel := context.WithCancel(context.Background())
	runErr := make(chan error, 1)
	go func() { runErr <- d.Run(ctx) }()

	waitFor(t, 10*time.Second, "initial rotation events committed", func() bool {
		return store.count() == 2
	})

	// Append a final line to the live file, then rotate it out of the glob and
	// drop a fresh file in its place.
	appendFile(t, logPath, `{"message":"r-remainder"}`+"\n")
	if err := os.Rename(logPath, rotatedPath); err != nil {
		t.Fatalf("rename for rotation: %v", err)
	}
	writeFile(t, logPath, `{"message":"r-new-1"}`+"\n"+`{"message":"r-new-2"}`+"\n")

	// All five distinct messages must arrive: the two originals, the drained
	// remainder from the rotated-out file, and the two from the fresh file.
	waitFor(t, 10*time.Second, "post-rotation events committed", func() bool {
		return store.count() == 5
	})
	waitFor(t, 5*time.Second, "WAL drained after rotation", func() bool {
		return d.queue.Stats().QueuedBatches == 0
	})

	cancel()
	if err := <-runErr; err != nil {
		t.Fatalf("Run returned error: %v", err)
	}

	events := store.snapshot()
	assertNoDuplicateEventIDs(t, events)
	if len(events) != 5 {
		t.Fatalf("committed events = %d, want 5", len(events))
	}
	for _, msg := range []string{"r-init-1", "r-init-2", "r-remainder", "r-new-1", "r-new-2"} {
		if eventByMessage(events, msg) == nil {
			t.Errorf("expected committed event with message %q after rotation, missing", msg)
		}
	}
}
