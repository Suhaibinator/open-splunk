package input

import (
	"context"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"sync"
	"testing"
	"time"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/collector/framing"
)

// --- test harness ---------------------------------------------------------

const testPoll = 10 * time.Millisecond

// collected is a concurrency-safe sink for emitted RawEvents.
type collected struct {
	mu  sync.Mutex
	evs []RawEvent
}

func (c *collected) add(ev RawEvent) {
	c.mu.Lock()
	c.evs = append(c.evs, ev)
	c.mu.Unlock()
}

func (c *collected) texts() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]string, len(c.evs))
	for i, ev := range c.evs {
		out[i] = string(ev.Bytes)
	}
	return out
}

func (c *collected) snapshot() []RawEvent {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]RawEvent, len(c.evs))
	copy(out, c.evs)
	return out
}

type harness struct {
	t     *testing.T
	mgr   Manager
	store CheckpointStore
	col   *collected
}

func startManager(t *testing.T, cfg Config, store CheckpointStore) *harness {
	t.Helper()
	if cfg.PollInterval == 0 {
		cfg.PollInterval = testPoll
	}
	mgr, err := NewManager(cfg, store)
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	col := &collected{}
	drained := make(chan struct{})
	go func() {
		defer close(drained)
		for ev := range mgr.Events() {
			col.add(ev)
		}
	}()
	runErr := make(chan error, 1)
	go func() { runErr <- mgr.Run(ctx) }()

	t.Cleanup(func() {
		cancel()
		select {
		case err := <-runErr:
			if err != nil {
				t.Errorf("Run returned error: %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Errorf("Run did not return after cancel")
		}
		select {
		case <-drained:
		case <-time.After(5 * time.Second):
			t.Errorf("events channel not closed after Run returned")
		}
		_ = store.Close()
	})

	return &harness{t: t, mgr: mgr, store: store, col: col}
}

func newStore(t *testing.T) CheckpointStore {
	t.Helper()
	store, err := NewCheckpointStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewCheckpointStore: %v", err)
	}
	return store
}

// waitFor polls cond until it is true or the deadline elapses.
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for: %s", what)
}

// waitForTexts waits until the collected event texts, sorted, equal want.
func (h *harness) waitForTexts(want []string) {
	h.t.Helper()
	sortedWant := append([]string(nil), want...)
	sort.Strings(sortedWant)
	waitFor(h.t, "events "+joinq(sortedWant), func() bool {
		got := h.col.texts()
		if len(got) != len(sortedWant) {
			return false
		}
		sort.Strings(got)
		for i := range got {
			if got[i] != sortedWant[i] {
				return false
			}
		}
		return true
	})
}

func joinq(s []string) string {
	out := ""
	for i, v := range s {
		if i > 0 {
			out += ","
		}
		out += v
	}
	return out
}

func appendFileT(t *testing.T, path, content string) {
	t.Helper()
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0o644)
	if err != nil {
		t.Fatalf("open append %s: %v", path, err)
	}
	defer f.Close()
	if _, err := f.WriteString(content); err != nil {
		t.Fatalf("append %s: %v", path, err)
	}
}

// --- tests ----------------------------------------------------------------

func TestManagerAppendWhileTailing(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	p := filepath.Join(dir, "app.log")
	writeFileT(t, p, "a1\na2\n")

	h := startManager(t, Config{
		InputID: "in", Include: []string{filepath.Join(dir, "*.log")},
		StartAt: StartAtBeginning,
	}, newStore(t))

	h.waitForTexts([]string{"a1", "a2"})
	appendFileT(t, p, "a3\na4\n")
	h.waitForTexts([]string{"a1", "a2", "a3", "a4"})

	waitFor(t, "healthy state", func() bool {
		return h.mgr.Health().State == opensplunkv1.CollectorInputState_COLLECTOR_INPUT_STATE_HEALTHY
	})
}

func TestManagerStartAtEnd(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	p := filepath.Join(dir, "app.log")
	writeFileT(t, p, "old1\nold2\n")

	h := startManager(t, Config{
		InputID: "in", Include: []string{filepath.Join(dir, "*.log")},
		StartAt: StartAtEnd,
	}, newStore(t))

	// Give the manager time to discover and seek to end; pre-existing content
	// must not be emitted.
	waitFor(t, "file discovered", func() bool { return h.mgr.Health().DiscoveredSources == 1 })
	appendFileT(t, p, "new1\n")
	h.waitForTexts([]string{"new1"})
}

func TestManagerStartAtEndReadsFileCreatedAfterStartup(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	p := filepath.Join(dir, "later.log")
	h := startManager(t, Config{
		InputID: "in", Include: []string{p}, StartAt: StartAtEnd,
	}, newStore(t))

	waitFor(t, "initial scan completed", func() bool {
		return h.mgr.Health().State == opensplunkv1.CollectorInputState_COLLECTOR_INPUT_STATE_MISSING
	})
	writeFileT(t, p, "created-before-discovery-1\ncreated-before-discovery-2\n")
	h.waitForTexts([]string{"created-before-discovery-1", "created-before-discovery-2"})
}

func TestManagerResumeFromCheckpoint(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	p := filepath.Join(dir, "app.log")
	writeFileT(t, p, "first\nsecond\n")

	store := newStore(t)
	id, err := NewFileIdentity(p, 0)
	if err != nil {
		t.Fatalf("identity: %v", err)
	}
	// Checkpoint after "first\n" (6 bytes): resume should skip it.
	if err := store.Set(Checkpoint{Identity: id, Path: p, Offset: 6, LineNumber: 1}); err != nil {
		t.Fatalf("seed checkpoint: %v", err)
	}

	h := startManager(t, Config{
		InputID: "in", Include: []string{filepath.Join(dir, "*.log")},
		StartAt: StartAtBeginning,
	}, store)

	h.waitForTexts([]string{"second"})
}

func TestManagerStartAtEndResumesFromStaleCheckpointAfterReopen(t *testing.T) {
	t.Parallel()
	sourceDir := t.TempDir()
	p := filepath.Join(sourceDir, "app.log")
	writeFileT(t, p, "first\nsecond\nthird\n")

	checkpointDir := t.TempDir()
	store, err := NewCheckpointStore(checkpointDir)
	if err != nil {
		t.Fatalf("NewCheckpointStore: %v", err)
	}
	id, err := NewFileIdentity(p, 0)
	if err != nil {
		t.Fatalf("identity: %v", err)
	}
	if err := store.Set(Checkpoint{
		Identity: id, Path: p, Offset: uint64(len("first\n")), LineNumber: 1,
		UpdatedAt: time.Now().UTC().Add(-staleCheckpointAge),
	}); err != nil {
		t.Fatalf("seed stale checkpoint: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close checkpoint store: %v", err)
	}

	reopened, err := NewCheckpointStore(checkpointDir)
	if err != nil {
		t.Fatalf("reopen checkpoint store: %v", err)
	}
	h := startManager(t, Config{
		InputID: "in", Include: []string{p}, StartAt: StartAtEnd,
	}, reopened)

	// start_at=end applies only to truly new sources. A previously tracked source
	// must resume at its durable position even when its last ack is old.
	h.waitForTexts([]string{"second", "third"})
}

func TestManagerRenameRotationContinuity(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	p := filepath.Join(dir, "app.log")
	writeFileT(t, p, "a1\na2\n")

	h := startManager(t, Config{
		InputID: "in", Include: []string{filepath.Join(dir, "app.log*")},
		StartAt: StartAtBeginning,
	}, newStore(t))

	h.waitForTexts([]string{"a1", "a2"})

	// Rotate by rename: the old inode moves to app.log.1 (still matched by the
	// glob), a fresh app.log is created. No duplicates, no loss.
	if err := os.Rename(p, p+".1"); err != nil {
		t.Fatalf("rename: %v", err)
	}
	writeFileT(t, p, "b1\nb2\n")

	h.waitForTexts([]string{"a1", "a2", "b1", "b2"})
}

func TestManagerRecreateSamePath(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	p := filepath.Join(dir, "app.log")
	writeFileT(t, p, "a1\na2\n")

	h := startManager(t, Config{
		InputID: "in", Include: []string{p},
		StartAt: StartAtBeginning,
	}, newStore(t))

	h.waitForTexts([]string{"a1", "a2"})

	// Delete then recreate the same path with a new inode: read from 0.
	if err := os.Remove(p); err != nil {
		t.Fatalf("remove: %v", err)
	}
	waitFor(t, "file gone", func() bool { return h.mgr.Health().DiscoveredSources == 0 })
	writeFileT(t, p, "b1\nb2\n")

	h.waitForTexts([]string{"a1", "a2", "b1", "b2"})
}

func TestManagerCopyTruncate(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	p := filepath.Join(dir, "app.log")
	writeFileT(t, p, "line-one\nline-two\n") // 18 bytes

	h := startManager(t, Config{
		InputID: "in", Include: []string{p},
		StartAt: StartAtBeginning,
	}, newStore(t))

	h.waitForTexts([]string{"line-one", "line-two"})

	// Copy-truncate: same inode, file shrinks below our offset. New (shorter)
	// content is read from 0.
	if err := os.Truncate(p, 0); err != nil {
		t.Fatalf("truncate: %v", err)
	}
	waitFor(t, "truncate observed", func() bool {
		st, err := os.Stat(p)
		return err == nil && st.Size() == 0
	})
	appendFileT(t, p, "x\n")

	h.waitForTexts([]string{"line-one", "line-two", "x"})
}

func TestManagerCopyTruncateRewriteLargerBetweenPolls(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	p := filepath.Join(dir, "app.log")
	writeFileT(t, p, "old-one\nold-two\n")

	h := startManager(t, Config{
		InputID: "in", Include: []string{p}, StartAt: StartAtBeginning,
		PollInterval: 30 * time.Millisecond,
	}, newStore(t))
	h.waitForTexts([]string{"old-one", "old-two"})
	before := h.col.snapshot()[0].Source.Identity

	// os.WriteFile truncates and rewrites the existing inode. By the next poll
	// the replacement is larger than the old offset, so size-only detection
	// would silently skip its prefix.
	writeFileT(t, p, "replacement-one\nreplacement-two\n")
	h.waitForTexts([]string{"old-one", "old-two", "replacement-one", "replacement-two"})
	afterEvents := h.col.snapshot()
	after := afterEvents[len(afterEvents)-1].Source.Identity
	if after.Generation <= before.Generation {
		t.Fatalf("generation did not advance across copy-truncate: %d -> %d", before.Generation, after.Generation)
	}
}

func TestManagerDeletionDrainsAndStops(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	p := filepath.Join(dir, "app.log")
	writeFileT(t, p, "a1\na2\n")

	h := startManager(t, Config{
		InputID: "in", Include: []string{p},
		StartAt: StartAtBeginning,
	}, newStore(t))

	h.waitForTexts([]string{"a1", "a2"})

	if err := os.Remove(p); err != nil {
		t.Fatalf("remove: %v", err)
	}
	waitFor(t, "input missing after deletion", func() bool {
		hl := h.mgr.Health()
		return hl.State == opensplunkv1.CollectorInputState_COLLECTOR_INPUT_STATE_MISSING &&
			hl.ActiveSources == 0
	})
}

func TestManagerDeletionFlushesTrailingPartialLine(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	p := filepath.Join(dir, "app.log")
	writeFileT(t, p, "done\npartial-no-newline")

	h := startManager(t, Config{
		InputID: "in", Include: []string{p},
		StartAt: StartAtBeginning,
	}, newStore(t))

	// The complete line emits immediately; the trailing partial waits.
	h.waitForTexts([]string{"done"})

	// On deletion the trailing partial must be flushed so nothing is lost.
	if err := os.Remove(p); err != nil {
		t.Fatalf("remove: %v", err)
	}
	h.waitForTexts([]string{"done", "partial-no-newline"})
}

func TestManagerDelayedCreation(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	p := filepath.Join(dir, "later.log")

	h := startManager(t, Config{
		InputID: "in", Include: []string{p},
		StartAt: StartAtBeginning,
	}, newStore(t))

	waitFor(t, "missing while absent", func() bool {
		hl := h.mgr.Health()
		return hl.State == opensplunkv1.CollectorInputState_COLLECTOR_INPUT_STATE_MISSING &&
			hl.StatusMessage != ""
	})

	writeFileT(t, p, "hello\n")
	h.waitForTexts([]string{"hello"})
	waitFor(t, "healthy after creation", func() bool {
		return h.mgr.Health().State == opensplunkv1.CollectorInputState_COLLECTOR_INPUT_STATE_HEALTHY
	})
}

func TestManagerExcludeGlobs(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	keep := filepath.Join(dir, "keep.log")
	skip := filepath.Join(dir, "skip.log")
	writeFileT(t, keep, "keep1\n")
	writeFileT(t, skip, "skip1\n")

	h := startManager(t, Config{
		InputID: "in",
		Include: []string{filepath.Join(dir, "*.log")},
		Exclude: []string{"skip.log"},
		StartAt: StartAtBeginning,
	}, newStore(t))

	h.waitForTexts([]string{"keep1"})
	// Ensure the excluded file is never tailed even after another poll cycle.
	waitFor(t, "one discovered source", func() bool { return h.mgr.Health().DiscoveredSources == 1 })
	if got := h.col.texts(); len(got) != 1 {
		t.Fatalf("excluded file leaked events: %v", got)
	}
}

func TestManagerOversizedEventSkipped(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	p := filepath.Join(dir, "app.log")
	writeFileT(t, p, "ok1\n"+repeatByte('A', 64)+"\nok2\n")

	h := startManager(t, Config{
		InputID: "in", Include: []string{p},
		StartAt: StartAtBeginning,
		Framing: framing.Options{MaxEventBytes: 8},
	}, newStore(t))

	// The oversized middle record is skipped; the two small records survive.
	h.waitForTexts([]string{"ok1", "ok2"})
}

func TestManagerMultilineFlushAfterInactivity(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	p := filepath.Join(dir, "app.log")
	// A started multiline event with a continuation line and no following start
	// line: it only emits via inactivity flush.
	writeFileT(t, p, "START a\ncontinuation line\n")

	h := startManager(t, Config{
		InputID:    "in",
		Include:    []string{p},
		StartAt:    StartAtBeginning,
		Multiline:  true,
		FlushAfter: 40 * time.Millisecond,
		Framing:    framing.Options{LineStartPattern: regexp.MustCompile(`^START`)},
	}, newStore(t))

	h.waitForTexts([]string{"START a\ncontinuation line"})
}

func repeatByte(b byte, n int) string {
	buf := make([]byte, n)
	for i := range buf {
		buf[i] = b
	}
	return string(buf)
}
