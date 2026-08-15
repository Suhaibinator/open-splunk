package input

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
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

func (c *collected) descriptions() []string {
	events := c.snapshot()
	out := make([]string, len(events))
	for i, event := range events {
		identity := event.Source.Identity
		out[i] = fmt.Sprintf(
			"%q path=%q dev=%d ino=%d gen=%d offsets=%d:%d",
			event.Bytes,
			event.Source.Path,
			identity.Device,
			identity.Inode,
			identity.Generation,
			event.Source.StartOffset,
			event.Source.EndOffset,
		)
	}
	return out
}

type harness struct {
	t     *testing.T
	mgr   Manager
	store CheckpointStore
	col   *collected
}

func startManager(t *testing.T, cfg Config, store CheckpointStore) *harness {
	return startManagerWithAfterDrainObserver(t, cfg, store, nil)
}

func startManagerWithAfterDrainObserver(
	t *testing.T,
	cfg Config,
	store CheckpointStore,
	observer func(tailerPollObservation),
) *harness {
	return startManagerWithHooks(t, cfg, store, managerTestHooks{
		afterDrain: observer,
	})
}

type managerTestHooks struct {
	afterDrain         func(tailerPollObservation)
	beforeStartGuard   func(tailerPollObservation)
	beforeSnapshotRead func(tailerPollObservation)
	afterSnapshotChunk func(tailerPollObservation)
	beforeRetireCommit func(tailerPollObservation)
	afterRetireCancel  func(tailerPollObservation)
}

func startManagerWithHooks(
	t *testing.T,
	cfg Config,
	store CheckpointStore,
	hooks managerTestHooks,
) *harness {
	t.Helper()
	if cfg.PollInterval == 0 {
		cfg.PollInterval = testPoll
	}
	mgr, err := NewManager(cfg, store)
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	concrete, ok := mgr.(*manager)
	if !ok {
		t.Fatalf("NewManager returned %T, want *manager", mgr)
	}
	concrete.afterDrainObserver = hooks.afterDrain
	concrete.beforeStartGuardObserver = hooks.beforeStartGuard
	concrete.beforeSnapshotReadObserver = hooks.beforeSnapshotRead
	concrete.afterSnapshotChunkObserver = hooks.afterSnapshotChunk
	concrete.beforeRetireCommitObserver = hooks.beforeRetireCommit
	concrete.afterRetireCancelObserver = hooks.afterRetireCancel
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

func newStore(t testing.TB) CheckpointStore {
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
	deadline := time.Now().Add(3 * time.Second)
	var got []string
	for time.Now().Before(deadline) {
		got = h.col.texts()
		if len(got) != len(sortedWant) {
			time.Sleep(2 * time.Millisecond)
			continue
		}
		sort.Strings(got)
		matched := true
		for i := range got {
			if got[i] != sortedWant[i] {
				matched = false
				break
			}
		}
		if matched {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	got = h.col.texts()
	sort.Strings(got)
	h.t.Fatalf(
		"timed out waiting for events %s; got %s; sources=%v; health=%+v",
		joinq(sortedWant),
		joinq(got),
		h.col.descriptions(),
		h.mgr.Health(),
	)
}

func joinq(s []string) string {
	var out strings.Builder
	for i, v := range s {
		if i > 0 {
			out.WriteString(",")
		}
		out.WriteString(v)
	}
	return out.String()
}

func TestTailerPollTimerRearmsWithoutAllocating(t *testing.T) {
	timer := tailerPollTimer{}
	defer timer.stop()
	ctx := context.Background()
	if !timer.wait(ctx) {
		t.Fatal("initial poll wait unexpectedly canceled")
	}

	const waitsPerRun = 32
	allocations := testing.AllocsPerRun(20, func() {
		for range waitsPerRun {
			if !timer.wait(ctx) {
				panic("poll wait unexpectedly canceled")
			}
		}
	})
	if allocations != 0 {
		t.Fatalf(
			"steady poll waits allocated %.0f objects per %d waits, want 0",
			allocations,
			waitsPerRun,
		)
	}
}

func TestTailerPollTimerDropsConsumedTickAndHonorsCancellation(t *testing.T) {
	timer := tailerPollTimer{}
	defer timer.stop()
	if !timer.wait(context.Background()) {
		t.Fatal("initial immediate poll wait unexpectedly canceled")
	}

	timer.interval = time.Hour
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if timer.wait(ctx) {
		t.Fatal("rearmed poll timer consumed a stale tick")
	}
	if ctx.Err() != context.DeadlineExceeded {
		t.Fatalf("poll wait returned before cancellation: %v", ctx.Err())
	}

	var preCanceled tailerPollTimer
	preCanceled.interval = time.Hour
	preCanceledCtx, preCancel := context.WithCancel(context.Background())
	preCancel()
	if preCanceled.wait(preCanceledCtx) {
		t.Fatal("pre-canceled poll wait succeeded")
	}
	if preCanceled.timer != nil {
		t.Fatal("pre-canceled poll wait allocated and armed a timer")
	}
}

func TestManagerReconcileTailersRequiresConfirmedMiss(t *testing.T) {
	t.Parallel()
	const key = "dev=1;ino=2"
	tracked := &tailer{}
	mgr := &manager{tailers: map[string]*tailer{key: tracked}}

	mgr.reconcileTailers(map[string]struct{}{})
	if tracked.retireRequested.Load() || tracked.missingDiscoveries != 1 {
		t.Fatalf(
			"first miss = drain:%t misses:%d, want false/1",
			tracked.retireRequested.Load(),
			tracked.missingDiscoveries,
		)
	}

	// Seeing the inode at a renamed path on the next complete scan cancels the
	// stale miss without ever making the drain request visible to its tailer.
	mgr.reconcileTailers(map[string]struct{}{key: {}})
	if tracked.retireRequested.Load() || tracked.missingDiscoveries != 0 {
		t.Fatalf(
			"seen after miss = drain:%t misses:%d, want false/0",
			tracked.retireRequested.Load(),
			tracked.missingDiscoveries,
		)
	}

	mgr.reconcileTailers(map[string]struct{}{})
	mgr.reconcileTailers(map[string]struct{}{})
	if !tracked.retireRequested.Load() {
		t.Fatal("two consecutive misses did not request a drain")
	}
}

func TestManagerClaimTailerReapsFinishedEntryImmediately(t *testing.T) {
	t.Parallel()
	const key = "dev=1;ino=2"
	tracked := &tailer{}
	tracked.finished.Store(true)
	mgr := &manager{tailers: map[string]*tailer{key: tracked}}

	if mgr.claimTailer(key, tracked, "renamed.log") {
		t.Fatal("finished tailer claimed a live discovery")
	}
	if _, exists := mgr.tailers[key]; exists {
		t.Fatal("finished tailer remained in map and would suppress same-pass open")
	}
}

func TestManagerClaimTailerSteadyFastPathAvoidsLockAndPathStore(t *testing.T) {
	t.Parallel()
	const key = "dev=1;ino=2"
	const path = "app.log"
	tracked := &tailer{}
	tracked.setPath(path)
	originalPath := tracked.path.Load()
	mgr := &manager{tailers: map[string]*tailer{key: tracked}}

	tracked.retireMu.Lock()
	claimed := make(chan bool, 1)
	go func() { claimed <- mgr.claimTailer(key, tracked, path) }()
	select {
	case ok := <-claimed:
		tracked.retireMu.Unlock()
		if !ok {
			t.Fatal("steady live tailer was not claimed")
		}
	case <-time.After(time.Second):
		tracked.retireMu.Unlock()
		<-claimed
		t.Fatal("steady claim waited for retirement lock")
	}
	if got := tracked.path.Load(); got != originalPath {
		t.Fatal("unchanged path replaced its atomic pointer")
	}
}

func TestManagerStagedTransactionPermitBoundsTailersAndCancels(t *testing.T) {
	t.Parallel()
	mgr := &manager{stagedTransaction: make(chan struct{}, 1)}
	first := &tailer{m: mgr}
	second := &tailer{m: mgr}
	if !first.m.acquireStagedTransaction(context.Background()) {
		t.Fatal("first tailer did not acquire staged transaction permit")
	}
	if got := len(mgr.stagedTransaction); got != 1 {
		t.Fatalf("held staged transaction permits = %d, want 1", got)
	}

	waitCtx, cancel := context.WithCancel(context.Background())
	started := make(chan struct{})
	result := make(chan bool, 1)
	go func() {
		close(started)
		result <- second.m.acquireStagedTransaction(waitCtx)
	}()
	<-started
	cancel()
	select {
	case acquired := <-result:
		if acquired {
			second.m.releaseStagedTransaction()
			t.Fatal("second tailer acquired a full staged transaction permit")
		}
	case <-time.After(time.Second):
		t.Fatal("canceled second tailer remained blocked on staged transaction permit")
	}
	first.m.releaseStagedTransaction()
	preCanceled, cancelPreCanceled := context.WithCancel(context.Background())
	cancelPreCanceled()
	if second.m.acquireStagedTransaction(preCanceled) {
		second.m.releaseStagedTransaction()
		t.Fatal("pre-canceled tailer acquired a free staged transaction permit")
	}

	retryCtx, retryCancel := context.WithTimeout(context.Background(), time.Second)
	defer retryCancel()
	if !second.m.acquireStagedTransaction(retryCtx) {
		t.Fatal("permit was not reusable after the first tailer released it")
	}
	second.m.releaseStagedTransaction()
}

func TestManagerStagedTransactionPermitIsHeldThroughPublication(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	firstPath := filepath.Join(dir, "a.log")
	secondPath := filepath.Join(dir, "b.log")
	writeFileT(t, firstPath, "first\n")
	writeFileT(t, secondPath, "second\n")

	store := newStore(t)
	mgrInterface, err := NewManager(Config{
		InputID:      "in",
		Include:      []string{filepath.Join(dir, "*.log")},
		StartAt:      StartAtBeginning,
		PollInterval: testPoll,
	}, store)
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	mgr := mgrInterface.(*manager)
	staged := make(chan string, 2)
	mgr.afterDrainObserver = func(observation tailerPollObservation) {
		staged <- observation.path
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- mgr.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			t.Error("manager did not stop after staged-permit test cancellation")
		}
	})

	select {
	case <-staged:
	case <-time.After(3 * time.Second):
		t.Fatal("no tailer staged its first transaction")
	}
	if got := len(mgr.stagedTransaction); got != 1 {
		t.Fatalf("publication-held staged permits = %d, want 1", got)
	}
	select {
	case path := <-staged:
		t.Fatalf("second tailer staged before first publication completed: %s", path)
	case <-time.After(50 * time.Millisecond):
	}

	select {
	case <-mgr.Events():
	case <-time.After(3 * time.Second):
		t.Fatal("first staged event was not published")
	}
	select {
	case <-staged:
	case <-time.After(3 * time.Second):
		t.Fatal("second tailer did not stage after first publication released the permit")
	}
	select {
	case <-mgr.Events():
	case <-time.After(3 * time.Second):
		t.Fatal("second staged event was not published")
	}
}

func appendFileT(t testing.TB, path, content string) {
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

func newEmptyFingerprintTransitionTailer(
	t *testing.T,
) (*tailer, *fileCheckpointStore, FileIdentity) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "app.log")
	writeFileT(t, path, "")
	file, err := os.Open(path)
	if err != nil {
		t.Fatalf("open empty input: %v", err)
	}
	t.Cleanup(func() { _ = file.Close() })
	info, err := file.Stat()
	if err != nil {
		t.Fatalf("stat empty input: %v", err)
	}
	emptyIdentity, err := identityFor(file, info, defaultFingerprintBytes)
	if err != nil {
		t.Fatalf("identify empty input: %v", err)
	}
	if emptyIdentity.FingerprintLength != 0 ||
		emptyIdentity.Fingerprint != emptyFingerprintSHA256 {
		t.Fatalf("empty identity = %+v, want canonical zero-length fingerprint", emptyIdentity)
	}

	store := newStore(t).(*fileCheckpointStore)
	t.Cleanup(func() { _ = store.Close() })
	if err := store.Set(Checkpoint{
		InputID: "in", Identity: emptyIdentity, Path: path,
		Offset: 0, NextLineNumber: 1,
	}); err != nil {
		t.Fatalf("seed empty discovery checkpoint: %v", err)
	}
	manager := &manager{
		cfg:         Config{InputID: "in"},
		checkpoints: store,
		fpBytes:     defaultFingerprintBytes,
	}
	tracked := &tailer{
		m:               manager,
		f:               file,
		id:              emptyIdentity,
		nextLineNumber:  1,
		lineCursorKnown: true,
	}
	tracked.path.Store(&path)
	appendFileT(t, path, "first\n")
	return tracked, store, emptyIdentity
}

func assertEmptyFingerprintTransitionEstablished(
	t *testing.T,
	tracked *tailer,
	store *fileCheckpointStore,
	emptyIdentity FileIdentity,
) {
	t.Helper()
	if tracked.offset != 0 {
		t.Fatalf("tailer offset = %d, want zero before first emission", tracked.offset)
	}
	if tracked.id == emptyIdentity || tracked.id.FingerprintLength == 0 {
		t.Fatalf("tailer identity = %+v, want established nonempty fingerprint", tracked.id)
	}
	checkpoint, found, err := store.Get("in", tracked.id)
	if err != nil || !found {
		t.Fatalf("get established checkpoint = (%+v, %t, %v)", checkpoint, found, err)
	}
	if checkpoint.Identity != tracked.id || checkpoint.Offset != 0 ||
		checkpoint.NextLineNumber != 1 {
		t.Fatalf("established checkpoint = %+v, want tailer identity at initial cursor", checkpoint)
	}
}

func TestTailerEmptyFingerprintTransitionRetriesFingerprintFailure(t *testing.T) {
	t.Parallel()
	tracked, store, emptyIdentity := newEmptyFingerprintTransitionTailer(t)
	injected := errors.New("injected fingerprint failure")
	attempts := 0
	tracked.m.identityFn = func(
		file *os.File,
		info os.FileInfo,
		fingerprintBytes int,
	) (FileIdentity, error) {
		attempts++
		if attempts == 1 {
			return FileIdentity{}, injected
		}
		return identityFor(file, info, fingerprintBytes)
	}

	if size, trackable := tracked.trackGrowthAndTruncate(); trackable || size != 0 {
		t.Fatalf("failed transition = size:%d trackable:%t, want 0/false", size, trackable)
	}
	if tracked.id != emptyIdentity || tracked.offset != 0 {
		t.Fatalf("failed transition advanced tailer: identity=%+v offset=%d", tracked.id, tracked.offset)
	}
	if attempts != 1 {
		t.Fatalf("identity attempts after failure = %d, want 1", attempts)
	}

	size, trackable := tracked.trackGrowthAndTruncate()
	if !trackable || size != uint64(len("first\n")) {
		t.Fatalf("retried transition = size:%d trackable:%t", size, trackable)
	}
	if attempts != 2 {
		t.Fatalf("identity attempts after retry = %d, want 2", attempts)
	}
	assertEmptyFingerprintTransitionEstablished(t, tracked, store, emptyIdentity)
}

func TestTailerEmptyFingerprintTransitionRetriesCheckpointFailure(t *testing.T) {
	t.Parallel()
	tracked, store, emptyIdentity := newEmptyFingerprintTransitionTailer(t)
	injected := errors.New("injected checkpoint failure")
	originalPersist := store.persistSnapshot
	persistAttempts := 0
	store.persistSnapshot = func(checkpoints []Checkpoint) error {
		persistAttempts++
		if persistAttempts == 1 {
			return injected
		}
		return originalPersist(checkpoints)
	}

	if size, trackable := tracked.trackGrowthAndTruncate(); trackable || size != 0 {
		t.Fatalf("failed transition = size:%d trackable:%t, want 0/false", size, trackable)
	}
	if tracked.id != emptyIdentity || tracked.offset != 0 {
		t.Fatalf("failed transition advanced tailer: identity=%+v offset=%d", tracked.id, tracked.offset)
	}
	checkpoint, found, err := store.Get("in", emptyIdentity)
	if err != nil || !found || checkpoint.Identity != emptyIdentity || checkpoint.Offset != 0 {
		t.Fatalf("checkpoint after failure = (%+v, %t, %v), want empty discovery", checkpoint, found, err)
	}

	size, trackable := tracked.trackGrowthAndTruncate()
	if !trackable || size != uint64(len("first\n")) {
		t.Fatalf("retried transition = size:%d trackable:%t", size, trackable)
	}
	if persistAttempts != 2 {
		t.Fatalf("persistence attempts = %d, want 2", persistAttempts)
	}
	assertEmptyFingerprintTransitionEstablished(t, tracked, store, emptyIdentity)
}

// --- tests ----------------------------------------------------------------

func TestNewManagerRejectsMultilineWithoutLineStartPattern(t *testing.T) {
	t.Parallel()
	store := newStore(t)
	t.Cleanup(func() { _ = store.Close() })
	_, err := NewManager(Config{
		InputID:   "in",
		Include:   []string{filepath.Join(t.TempDir(), "*.log")},
		Multiline: true,
	}, store)
	const want = "collector/input: multiline line-start pattern is required"
	if err == nil || err.Error() != want {
		t.Fatalf("NewManager error = %v, want %q", err, want)
	}
}

func TestNewManagerRejectsFingerprintLimitAboveAbsoluteMaximum(t *testing.T) {
	t.Parallel()
	store := newStore(t)
	t.Cleanup(func() { _ = store.Close() })
	_, err := NewManager(Config{
		InputID:          "in",
		Include:          []string{filepath.Join(t.TempDir(), "*.log")},
		FingerprintBytes: maximumFingerprintBytes + 1,
	}, store)
	if err == nil {
		t.Fatal("NewManager accepted fingerprint limit above absolute maximum")
	}
}

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
	appendFileT(t, p, "a5\n")
	h.waitForTexts([]string{"a1", "a2", "a3", "a4", "a5"})
	events := h.col.snapshot()
	wantTexts := []string{"a1", "a2", "a3", "a4", "a5"}
	for i, event := range events {
		wantLine := uint64(i + 1)
		if string(event.Bytes) != wantTexts[i] ||
			event.Source.LineNumber != wantLine ||
			event.Source.NextLineNumber != wantLine+1 {
			t.Fatalf(
				"event %d = %q source lines (%d, %d), want %q at (%d, %d)",
				i,
				event.Bytes,
				event.Source.LineNumber,
				event.Source.NextLineNumber,
				wantTexts[i],
				wantLine,
				wantLine+1,
			)
		}
	}

	waitFor(t, "healthy state", func() bool {
		return h.mgr.Health().State == opensplunkv1.CollectorInputState_COLLECTOR_INPUT_STATE_HEALTHY
	})
}

func TestManagerStartAtEnd(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	p := filepath.Join(dir, "app.log")
	const initial = "old1\nold2\n"
	writeFileT(t, p, initial)
	identity, err := NewFileIdentity(p, 0)
	if err != nil {
		t.Fatalf("identity: %v", err)
	}

	h := startManager(t, Config{
		InputID: "in", Include: []string{filepath.Join(dir, "*.log")},
		StartAt: StartAtEnd,
	}, newStore(t))

	// DiscoveredSources is published before resolveStart snapshots the initial
	// EOF. Wait for the exact discovery checkpoint so the append is
	// unambiguously part of the monitored stream.
	waitFor(t, "start-at-end discovery checkpoint", func() bool {
		checkpoint, ok, getErr := h.store.Get("in", identity)
		return getErr == nil && ok && checkpoint.Offset == uint64(len(initial))
	})
	appendFileT(t, p, "new1\n")
	h.waitForTexts([]string{"new1"})
	event := h.col.snapshot()[0]
	if event.Source.LineNumber != 1 || event.Source.NextLineNumber != 2 {
		t.Fatalf(
			"start-at-end source lines = (%d, %d), want monitored stream (1, 2)",
			event.Source.LineNumber,
			event.Source.NextLineNumber,
		)
	}
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
	if err := store.Set(Checkpoint{
		InputID: "in", Identity: id, Path: p,
		Offset: 6, LineNumber: 1, NextLineNumber: 2,
	}); err != nil {
		t.Fatalf("seed checkpoint: %v", err)
	}

	h := startManager(t, Config{
		InputID: "in", Include: []string{filepath.Join(dir, "*.log")},
		StartAt: StartAtBeginning,
	}, store)

	h.waitForTexts([]string{"second"})
	event := h.col.snapshot()[0]
	if event.Source.LineNumber != 2 || event.Source.NextLineNumber != 3 {
		t.Fatalf(
			"resumed source lines = (%d, %d), want (2, 3)",
			event.Source.LineNumber,
			event.Source.NextLineNumber,
		)
	}
}

func TestManagerKeepsLegacyMultilineCheckpointLineCursorUnknown(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	p := filepath.Join(dir, "app.log")
	first := "START first\ncontinuation\n"
	writeFileT(t, p, first+"START second\nSTART third\n")

	checkpointDir := t.TempDir()
	store, err := NewCheckpointStore(checkpointDir)
	if err != nil {
		t.Fatalf("NewCheckpointStore: %v", err)
	}
	id, err := NewFileIdentity(p, 0)
	if err != nil {
		t.Fatalf("identity: %v", err)
	}
	// Checkpoints written before NextLineNumber existed retain only the first
	// physical line of the last acknowledged logical event.
	if err := store.Set(Checkpoint{
		InputID: "in", Identity: id, Path: p,
		Offset: uint64(len(first)), LineNumber: 1,
	}); err != nil {
		t.Fatalf("seed legacy checkpoint: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close seeded checkpoint store: %v", err)
	}

	for _, restart := range []string{"upgrade", "reopen"} {
		t.Run(restart, func(t *testing.T) {
			reopened, err := NewCheckpointStore(checkpointDir)
			if err != nil {
				t.Fatalf("reopen checkpoint store: %v", err)
			}
			h := startManager(t, Config{
				InputID: "in", Include: []string{p}, StartAt: StartAtBeginning,
				Multiline: true,
				Framing:   framing.Options{LineStartPattern: regexp.MustCompile(`^START`)},
			}, reopened)
			h.waitForTexts([]string{"START second"})
			event := h.col.snapshot()[0]
			if event.Source.LineNumber != 0 || event.Source.NextLineNumber != 0 {
				t.Fatalf(
					"legacy resume source lines = (%d, %d), want unknown (0, 0)",
					event.Source.LineNumber,
					event.Source.NextLineNumber,
				)
			}
			checkpoint, ok, err := reopened.Get("in", id)
			if err != nil || !ok || checkpoint.NextLineNumber != 0 {
				t.Fatalf(
					"legacy checkpoint = %+v (ok=%t err=%v), want unknown next line",
					checkpoint,
					ok,
					err,
				)
			}
		})
	}
}

func TestManagerBatchesLegacyCheckpointCursorUpgrades(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	paths := []string{
		filepath.Join(dir, "a.log"),
		filepath.Join(dir, "b.log"),
	}
	store := newStore(t).(*fileCheckpointStore)
	legacy := make([]Checkpoint, 0, len(paths))
	for _, path := range paths {
		writeFileT(t, path, "first\n")
		id, err := NewFileIdentity(path, 0)
		if err != nil {
			t.Fatalf("identity %s: %v", path, err)
		}
		legacy = append(legacy, Checkpoint{
			InputID:    "in",
			Identity:   id,
			Path:       path,
			Offset:     uint64(len("first\n")),
			LineNumber: 1,
		})
	}
	if err := store.SetMany(legacy); err != nil {
		t.Fatalf("seed legacy checkpoints: %v", err)
	}

	originalPersist := store.persistSnapshot
	var writes atomic.Uint64
	store.persistSnapshot = func(checkpoints []Checkpoint) error {
		writes.Add(1)
		return originalPersist(checkpoints)
	}

	h := startManager(t, Config{
		InputID: "in", Include: []string{filepath.Join(dir, "*.log")},
		StartAt: StartAtBeginning,
	}, store)
	waitFor(t, "legacy cursors persisted", func() bool {
		for _, checkpoint := range legacy {
			got, ok, err := store.Get("in", checkpoint.Identity)
			if err != nil || !ok || got.NextLineNumber != 2 {
				return false
			}
		}
		return true
	})
	if got := writes.Load(); got != 1 {
		t.Fatalf("legacy checkpoint persistence writes = %d, want one batched write", got)
	}
	if got := h.mgr.Health().DiscoveredSources; got != uint64(len(paths)) {
		t.Fatalf("discovered sources = %d, want %d", got, len(paths))
	}
}

func TestCheckpointNextLineRejectsRegressingOrExhaustedCursor(t *testing.T) {
	t.Parallel()
	for _, checkpoint := range []Checkpoint{
		{LineNumber: 4, NextLineNumber: 4},
		{LineNumber: 4, NextLineNumber: 3},
		{LineNumber: 0, NextLineNumber: 2},
		{LineNumber: ^uint64(0) - 1, NextLineNumber: ^uint64(0)},
		{LineNumber: ^uint64(0) - 1},
		{LineNumber: ^uint64(0)},
	} {
		if _, _, err := checkpointNextLine(checkpoint, false); err == nil {
			t.Fatalf("checkpointNextLine(%+v) error = nil", checkpoint)
		}
	}
}

func TestCheckpointNextLineMigratesFlushedLegacyPartial(t *testing.T) {
	t.Parallel()
	next, known, err := checkpointNextLine(Checkpoint{
		Offset: uint64(len("first\npartial")), LineNumber: 2,
	}, false)
	if err != nil || next != 3 || !known {
		t.Fatalf(
			"flushed legacy partial next line = %d known=%t err=%v, want exact 3",
			next,
			known,
			err,
		)
	}
}

func TestCheckpointNextLineKeepsUnanchoredLegacyCursorUnknown(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		checkpoint Checkpoint
		multiline  bool
	}{
		// The old multiline format retained only the event's starting line.
		{checkpoint: Checkpoint{Offset: 1 << 40, LineNumber: 42}, multiline: true},
		// A nonzero legacy discovery offset has no physical-line anchor even for
		// ordinary line framing.
		{checkpoint: Checkpoint{Offset: 100}},
	} {
		next, known, err := checkpointNextLine(test.checkpoint, test.multiline)
		if err != nil || next != 1 || known {
			t.Fatalf(
				"unanchored legacy cursor %+v = next:%d known:%t err:%v, want internal 1/unknown",
				test.checkpoint,
				next,
				known,
				err,
			)
		}
	}
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
		InputID: "in", Identity: id, Path: p,
		Offset: uint64(len("first\n")), LineNumber: 1,
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
	events := h.col.snapshot()
	last := events[len(events)-1]
	if last.Source.LineNumber != 1 || last.Source.NextLineNumber != 2 {
		t.Fatalf(
			"copy-truncated source lines = (%d, %d), want new generation (1, 2)",
			last.Source.LineNumber,
			last.Source.NextLineNumber,
		)
	}
}

func TestManagerCopyTruncateRewriteLargerBetweenPolls(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	p := filepath.Join(dir, "app.log")
	const oldContent = "old-one\nold-two\n"
	writeFileT(t, p, oldContent)

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

func TestManagerRewriteWhileBatchStagedValidatesExactSpanBeforePublish(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	p := filepath.Join(dir, "app.log")
	const original = "a-old\n"
	const replacement = "a-new\n"
	writeFileT(t, p, original)

	batchStaged := make(chan tailerPollObservation, 1)
	rewriteDone := make(chan struct{})
	var observeOnce sync.Once
	observer := func(observation tailerPollObservation) {
		if observation.path != p || observation.offset != uint64(len(original)) {
			return
		}
		observeOnce.Do(func() {
			batchStaged <- observation
			<-rewriteDone
		})
	}
	h := startManagerWithAfterDrainObserver(t, Config{
		InputID: "in", Include: []string{p}, StartAt: StartAtBeginning,
		FingerprintBytes: 1,
	}, newStore(t), observer)
	release := sync.OnceFunc(func() { close(rewriteDone) })
	t.Cleanup(release)

	var staged tailerPollObservation
	select {
	case staged = <-batchStaged:
	case <-time.After(3 * time.Second):
		t.Fatalf("timed out waiting for staged batch; health=%+v", h.mgr.Health())
	}
	if events := h.col.snapshot(); len(events) != 0 {
		t.Fatalf("unvalidated events were published: %v", h.col.descriptions())
	}
	// Prefix and trailing one-byte guards both remain identical ('a' and '\n').
	// Only validation of the complete raw dependency span detects this rewrite.
	writeFileT(t, p, replacement)
	release()
	h.waitForTexts([]string{"a-new"})
	event := h.col.snapshot()[0]
	if event.Source.Identity.Generation <= staged.generation {
		t.Fatalf(
			"replacement generation = %d, want greater than staged generation %d",
			event.Source.Identity.Generation,
			staged.generation,
		)
	}
}

func TestManagerRewritePriorGuardDuringValidationResetsBeforePublish(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	p := filepath.Join(dir, "app.log")
	const first = "first\n"
	const second = "second\n"
	writeFileT(t, p, first)

	batchStaged := make(chan tailerPollObservation, 1)
	rewriteDone := make(chan struct{})
	var observeOnce sync.Once
	observer := func(observation tailerPollObservation) {
		if observation.path != p || observation.offset != uint64(len(first+second)) {
			return
		}
		observeOnce.Do(func() {
			batchStaged <- observation
			<-rewriteDone
		})
	}
	h := startManagerWithAfterDrainObserver(t, Config{
		InputID: "in", Include: []string{p}, StartAt: StartAtBeginning,
		FingerprintBytes: len(first),
	}, newStore(t), observer)
	release := sync.OnceFunc(func() { close(rewriteDone) })
	t.Cleanup(release)

	h.waitForTexts([]string{"first"})
	oldGeneration := h.col.snapshot()[0].Source.Identity.Generation
	appendFileT(t, p, second)
	select {
	case <-batchStaged:
	case <-time.After(3 * time.Second):
		t.Fatalf("timed out waiting for appended batch; health=%+v", h.mgr.Health())
	}
	// Mutate only the previously consumed guard while leaving the newly staged
	// second record unchanged. The combined guard+raw validation must fail.
	writeFileT(t, p, "FIRST\n"+second)
	release()
	h.waitForTexts([]string{"first", "FIRST", "second"})
	events := h.col.snapshot()
	if events[1].Source.Identity.Generation <= oldGeneration ||
		events[2].Source.Identity.Generation != events[1].Source.Identity.Generation {
		t.Fatalf("replacement generations = %v", h.col.descriptions())
	}
}

func TestManagerMultilineLookaheadRewriteIsValidatedBeforePublish(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	p := filepath.Join(dir, "app.log")
	const firstEvent = "START a\nline\n"
	const original = firstEvent + "START b\n"
	const replacement = firstEvent + "other b\n"
	writeFileT(t, p, original)

	batchStaged := make(chan tailerPollObservation, 1)
	rewriteDone := make(chan struct{})
	var observeOnce sync.Once
	observer := func(observation tailerPollObservation) {
		if observation.path != p || observation.offset != uint64(len(firstEvent)) {
			return
		}
		observeOnce.Do(func() {
			batchStaged <- observation
			<-rewriteDone
		})
	}
	h := startManagerWithAfterDrainObserver(t, Config{
		InputID: "in", Include: []string{p}, StartAt: StartAtBeginning,
		FingerprintBytes: 1,
		Multiline:        true,
		FlushAfter:       20 * time.Millisecond,
		Framing: framing.Options{
			LineStartPattern: regexp.MustCompile(`^START`),
		},
	}, newStore(t), observer)
	release := sync.OnceFunc(func() { close(rewriteDone) })
	t.Cleanup(release)

	var staged tailerPollObservation
	select {
	case staged = <-batchStaged:
	case <-time.After(3 * time.Second):
		t.Fatalf("timed out waiting for multiline batch; health=%+v", h.mgr.Health())
	}
	if events := h.col.snapshot(); len(events) != 0 {
		t.Fatalf("lookahead-dependent event published early: %v", h.col.descriptions())
	}
	writeFileT(t, p, replacement)
	release()
	h.waitForTexts([]string{"START a\nline\nother b"})
	if generation := h.col.snapshot()[0].Source.Identity.Generation; generation <= staged.generation {
		t.Fatalf("lookahead replacement generation = %d, want > %d", generation, staged.generation)
	}
}

func TestManagerRewriteDuringSnapshotReadCannotPublishMixedFrame(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	p := filepath.Join(dir, "app.log")
	const payloadBytes = validationChunkBytes + 1024
	original := repeatByte('A', payloadBytes) + "\n"
	replacement := repeatByte('B', payloadBytes) + "\n"
	writeFileT(t, p, original)

	chunkRead := make(chan tailerPollObservation, 1)
	rewriteDone := make(chan struct{})
	var observeOnce sync.Once
	observer := func(observation tailerPollObservation) {
		if observation.path != p || observation.offset < validationChunkBytes {
			return
		}
		observeOnce.Do(func() {
			chunkRead <- observation
			<-rewriteDone
		})
	}
	h := startManagerWithHooks(t, Config{
		InputID: "in", Include: []string{p}, StartAt: StartAtBeginning,
		FingerprintBytes: 1,
		Framing:          framing.Options{MaxEventBytes: payloadBytes},
	}, newStore(t), managerTestHooks{afterSnapshotChunk: observer})
	release := sync.OnceFunc(func() { close(rewriteDone) })
	t.Cleanup(release)

	var staged tailerPollObservation
	select {
	case staged = <-chunkRead:
	case <-time.After(3 * time.Second):
		t.Fatalf("timed out waiting for snapshot chunk; health=%+v", h.mgr.Health())
	}
	writeFileT(t, p, replacement)
	release()
	waitFor(t, "replacement frame", func() bool { return len(h.col.snapshot()) == 1 })
	event := h.col.snapshot()[0]
	if string(event.Bytes) != replacement[:len(replacement)-1] {
		t.Fatalf("published mixed snapshot: length=%d prefix=%q", len(event.Bytes), event.Bytes[:8])
	}
	if event.Source.Identity.Generation <= staged.generation {
		t.Fatalf("snapshot replacement generation = %d, want > %d", event.Source.Identity.Generation, staged.generation)
	}
}

func TestManagerTruncateBeforeSnapshotReadArmsGenerationReset(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	p := filepath.Join(dir, "app.log")
	const original = "old\n"
	writeFileT(t, p, original)

	readReady := make(chan tailerPollObservation, 1)
	truncateDone := make(chan struct{})
	var observeOnce sync.Once
	observer := func(observation tailerPollObservation) {
		if observation.path != p || observation.offset != uint64(len(original)) {
			return
		}
		observeOnce.Do(func() {
			readReady <- observation
			<-truncateDone
		})
	}
	h := startManagerWithHooks(t, Config{
		InputID: "in", Include: []string{p}, StartAt: StartAtBeginning,
	}, newStore(t), managerTestHooks{beforeSnapshotRead: observer})
	release := sync.OnceFunc(func() { close(truncateDone) })
	t.Cleanup(release)

	var staged tailerPollObservation
	select {
	case staged = <-readReady:
	case <-time.After(3 * time.Second):
		t.Fatalf("timed out waiting before snapshot read; health=%+v", h.mgr.Health())
	}
	if err := os.Truncate(p, 0); err != nil {
		t.Fatalf("truncate staged source: %v", err)
	}
	release()
	waitFor(t, "short snapshot reset", func() bool { return !h.mgr.Health().LastErrorAt.IsZero() })
	appendFileT(t, p, "new\n")
	h.waitForTexts([]string{"new"})
	if generation := h.col.snapshot()[0].Source.Identity.Generation; generation <= staged.generation {
		t.Fatalf("post-short-read generation = %d, want > %d", generation, staged.generation)
	}
}

func TestManagerRewriteBetweenResumeAndInitialGuardBurnsGeneration(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	p := filepath.Join(dir, "app.log")
	const original = "old\n"
	const replacement = "new\n"
	writeFileT(t, p, original)
	store := newStore(t)
	identity, err := NewFileIdentity(p, 0)
	if err != nil {
		t.Fatalf("identity: %v", err)
	}
	if err := store.Set(Checkpoint{
		InputID: "in", Identity: identity, Path: p,
		Offset: uint64(len(original)), LineNumber: 1, NextLineNumber: 2,
	}); err != nil {
		t.Fatalf("seed resume checkpoint: %v", err)
	}

	rewriteResult := make(chan error, 1)
	var rewriteOnce sync.Once
	h := startManagerWithHooks(t, Config{
		InputID: "in", Include: []string{p}, StartAt: StartAtBeginning,
	}, store, managerTestHooks{
		beforeStartGuard: func(tailerPollObservation) {
			rewriteOnce.Do(func() {
				err := os.WriteFile(p, []byte(replacement), 0o644)
				rewriteResult <- err
			})
		},
	})
	h.waitForTexts([]string{"new"})
	if err := <-rewriteResult; err != nil {
		t.Fatalf("rewrite during start: %v", err)
	}
	event := h.col.snapshot()[0]
	if event.Source.Identity.Generation <= identity.Generation {
		t.Fatalf(
			"resume-race generation = %d, want > %d",
			event.Source.Identity.Generation,
			identity.Generation,
		)
	}
}

func newTrackedTailerForTest(
	t testing.TB,
	path string,
	offset uint64,
	nextLineNumber uint64,
) (*tailer, FileIdentity) {
	t.Helper()
	file, err := os.Open(path)
	if err != nil {
		t.Fatalf("open input: %v", err)
	}
	t.Cleanup(func() { _ = file.Close() })
	info, err := file.Stat()
	if err != nil {
		t.Fatalf("stat input: %v", err)
	}
	if offset > uint64(info.Size()) {
		t.Fatalf("tailer offset %d exceeds file size %d", offset, info.Size())
	}
	identity, err := identityFor(file, info, defaultFingerprintBytes)
	if err != nil {
		t.Fatalf("identify input: %v", err)
	}
	store := newStore(t)
	t.Cleanup(func() { _ = store.Close() })
	manager := &manager{
		cfg:         Config{InputID: "in"},
		checkpoints: store,
		fpBytes:     defaultFingerprintBytes,
		readWindow:  framing.DefaultMaxEventBytes + framingReadSlop,
		events:      make(chan RawEvent, 8),
	}
	tracked := &tailer{
		m:               manager,
		f:               file,
		id:              identity,
		offset:          offset,
		nextLineNumber:  nextLineNumber,
		lineCursorKnown: true,
		lastSize:        uint64(info.Size()),
		lastSizeChange:  time.Now(),
	}
	tracked.path.Store(&path)
	if err := tracked.refreshGuard(); err != nil {
		t.Fatalf("refresh guard: %v", err)
	}
	return tracked, identity
}

func TestTailerGuardFingerprintReusesScratchWithoutAllocating(t *testing.T) {
	path := filepath.Join(t.TempDir(), "app.log")
	content := repeatByte('G', defaultFingerprintBytes)
	writeFileT(t, path, content)
	tracked, _ := newTrackedTailerForTest(t, path, uint64(len(content)), 2)
	want := tracked.guardFingerprint

	allocations := testing.AllocsPerRun(50, func() {
		got, err := tracked.readGuardFingerprint()
		if err != nil {
			panic(err)
		}
		if got != want {
			panic("guard fingerprint changed")
		}
	})
	if allocations != 0 {
		t.Fatalf("steady guard fingerprint allocated %.0f objects, want 0", allocations)
	}
}

func TestTailerSmallAppendPreservesFullRewriteGuard(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "app.log")
	initial := repeatByte('a', 2*defaultFingerprintBytes-1) + "\n"
	writeFileT(t, path, initial)
	tracked, identity := newTrackedTailerForTest(t, path, uint64(len(initial)), 2)
	if tracked.guardLength != defaultFingerprintBytes {
		t.Fatalf(
			"initial guard length = %d, want %d",
			tracked.guardLength,
			defaultFingerprintBytes,
		)
	}

	const appended = "x\n"
	appendFileT(t, path, appended)
	observedEnd := uint64(len(initial + appended))
	batch, err := tracked.stageRead(context.Background(), observedEnd, false)
	if err != nil {
		t.Fatalf("stage small append: %v", err)
	}
	matches, err := tracked.stagedBatchMatches(batch)
	if err != nil || !matches {
		t.Fatalf("validate small append: matches=%t err=%v", matches, err)
	}
	if !tracked.commitBatch(context.Background(), batch) {
		t.Fatal("commit small append canceled")
	}
	if tracked.guardLength != defaultFingerprintBytes {
		t.Fatalf(
			"post-append guard length = %d, want retained %d",
			tracked.guardLength,
			defaultFingerprintBytes,
		)
	}

	// Preserve the appended suffix while replacing every byte covered only by
	// the prior guard. A guard incorrectly shrunk to len(appended) would miss it.
	writeFileT(t, path, repeatByte('b', len(initial))+appended)
	size, trackable := tracked.trackGrowthAndTruncate()
	if !trackable || size != observedEnd {
		t.Fatalf(
			"same-size replacement tracking = size %d trackable %t, want %d/true",
			size,
			trackable,
			observedEnd,
		)
	}
	if tracked.offset != 0 || tracked.id.Generation != identity.Generation+1 {
		t.Fatalf(
			"same-size replacement state = offset %d generation %d, want 0/%d",
			tracked.offset,
			tracked.id.Generation,
			identity.Generation+1,
		)
	}
}

func TestTailerArtificialWindowEOFDoesNotFlushMultilinePartial(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "app.log")
	const firstLine = "START a\n"
	content := firstLine + "continuation beyond the staged window\n"
	writeFileT(t, path, content)
	tracked, _ := newTrackedTailerForTest(t, path, 0, 1)
	tracked.id.Fingerprint = ""
	tracked.id.FingerprintLength = 0
	tracked.m.cfg.Multiline = true
	tracked.m.cfg.FlushAfter = time.Nanosecond
	tracked.m.cfg.Framing.LineStartPattern = regexp.MustCompile(`^START`)
	tracked.m.readWindow = uint64(len(firstLine))
	tracked.lastSizeChange = time.Now().Add(-time.Hour)

	batch, err := tracked.stageRead(context.Background(), uint64(len(content)), false)
	if err != nil {
		t.Fatalf("stage bounded multiline read: %v", err)
	}
	if batch.reachedObservedEnd() || batch.flushed || len(batch.events) != 0 ||
		batch.cursor.offset != 0 {
		t.Fatalf(
			"artificial EOF batch = reached:%t flushed:%t events:%d offset:%d",
			batch.reachedObservedEnd(),
			batch.flushed,
			len(batch.events),
			batch.cursor.offset,
		)
	}
}

func TestTailerDenseInputUsesSmallInitialSnapshot(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "dense.log")
	content := repeatByte('\n', 64<<10)
	writeFileT(t, path, content)
	tracked, _ := newTrackedTailerForTest(t, path, 0, 1)
	tracked.id.Fingerprint = ""
	tracked.id.FingerprintLength = 0

	batch, err := tracked.stageRead(context.Background(), uint64(len(content)), false)
	if err != nil {
		t.Fatalf("stage dense input: %v", err)
	}
	if got := len(batch.raw); got != initialStagedReadBytes {
		t.Fatalf("dense initial snapshot bytes = %d, want %d", got, initialStagedReadBytes)
	}
	if got := len(batch.events); got != maxStagedEvents {
		t.Fatalf("dense staged events = %d, want cap %d", got, maxStagedEvents)
	}
	if got := batch.cursor.offset; got != maxStagedEvents {
		t.Fatalf("dense staged cursor = %d, want %d", got, maxStagedEvents)
	}
}

func TestTailerStagedReadWindowEvolution(t *testing.T) {
	t.Parallel()
	tracked := &tailer{m: &manager{readWindow: 64 << 10}}
	if got := tracked.currentStagedReadWindow(); got != initialStagedReadBytes {
		t.Fatalf("initial staged window = %d, want %d", got, initialStagedReadBytes)
	}
	probe := &stagedBatch{
		start:       0,
		snapshotEnd: initialStagedReadBytes,
		observedEnd: 2 * initialStagedReadBytes,
		cursor:      tailerCursor{offset: 0},
	}
	if !probe.stoppedAtArtificialBoundary() {
		t.Fatal("zero-progress bounded probe was not classified as artificial")
	}
	for _, want := range []uint64{8 << 10, 16 << 10, 32 << 10} {
		if !tracked.growStagedReadWindow() || tracked.stagedWindow != want {
			t.Fatalf("grown staged window = %d, want %d", tracked.stagedWindow, want)
		}
	}

	large := &stagedBatch{start: 0, cursor: tailerCursor{offset: 24 << 10}}
	tracked.tuneProductiveStagedReadWindow(large)
	if got := tracked.stagedWindow; got != 32<<10 {
		t.Fatalf("productive large-record window = %d, want retained %d", got, 32<<10)
	}

	lowUtilization := &stagedBatch{
		start:  24 << 10,
		cursor: tailerCursor{offset: 28 << 10},
	}
	tracked.tuneProductiveStagedReadWindow(lowUtilization)
	if got := tracked.currentStagedReadWindow(); got != initialStagedReadBytes {
		t.Fatalf("low-utilization window = %d, want %d", got, initialStagedReadBytes)
	}

	tracked.stagedWindow = 32 << 10
	eventLimited := &stagedBatch{
		start:      0,
		cursor:     tailerCursor{offset: 24 << 10},
		eventLimit: true,
	}
	tracked.tuneProductiveStagedReadWindow(eventLimited)
	if got := tracked.currentStagedReadWindow(); got != initialStagedReadBytes {
		t.Fatalf("event-limited window = %d, want %d", got, initialStagedReadBytes)
	}
}

func TestManagerFramingErrorDoesNotBurnGenerationOrReplay(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "app.log")
	const consumed = "old\n"
	writeFileT(t, path, consumed+"new\n")
	store := newStore(t)
	identity, err := NewFileIdentity(path, 0)
	if err != nil {
		t.Fatalf("identity: %v", err)
	}
	checkpoint := Checkpoint{
		InputID: "in", Identity: identity, Path: path,
		Offset:         uint64(len(consumed)),
		LineNumber:     ^uint64(0) - 2,
		NextLineNumber: ^uint64(0) - 1,
	}
	if err := store.Set(checkpoint); err != nil {
		t.Fatalf("seed exhausted-line checkpoint: %v", err)
	}

	h := startManager(t, Config{
		InputID: "in", Include: []string{path}, StartAt: StartAtBeginning,
	}, store)
	waitFor(t, "framing error", func() bool {
		return !h.mgr.Health().LastErrorAt.IsZero()
	})
	time.Sleep(3 * testPoll)
	if events := h.col.descriptions(); len(events) != 0 {
		t.Fatalf("framing error replayed events: %v", events)
	}
	got, ok, err := store.Get("in", identity)
	if err != nil || !ok {
		t.Fatalf("get checkpoint after framing error: ok=%t err=%v", ok, err)
	}
	if got.Identity.Generation != identity.Generation || got.Offset != checkpoint.Offset {
		t.Fatalf(
			"checkpoint after framing error = gen %d offset %d, want gen %d offset %d",
			got.Identity.Generation,
			got.Offset,
			identity.Generation,
			checkpoint.Offset,
		)
	}
}

func TestTailerSameSizeCopyTruncateResetsInactivityClock(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "app.log")
	const original = "old\n"
	writeFileT(t, path, original)

	before := time.Now().Add(-time.Hour)
	tracked, identity := newTrackedTailerForTest(t, path, uint64(len(original)), 2)
	tracked.lastSizeChange = before

	// Rewrite the same inode to the same byte length. Size-only activity
	// tracking cannot distinguish this from an idle file, but the guard can.
	rewriteStarted := time.Now()
	writeFileT(t, path, "new\n")
	size, trackable := tracked.trackGrowthAndTruncate()
	if !trackable {
		t.Fatal("same-size replacement was not trackable")
	}
	if size != uint64(len(original)) {
		t.Fatalf("same-size replacement size = %d, want %d", size, len(original))
	}
	if tracked.offset != 0 || tracked.id.Generation != identity.Generation+1 {
		t.Fatalf(
			"same-size replacement state = offset %d generation %d, want 0/%d",
			tracked.offset,
			tracked.id.Generation,
			identity.Generation+1,
		)
	}
	if tracked.lastSizeChange.Before(rewriteStarted) {
		t.Fatalf(
			"same-size replacement inactivity clock = %v, before rewrite %v",
			tracked.lastSizeChange,
			rewriteStarted,
		)
	}
}

func TestTailerDrainReframesAfterAppendRacesWithEOFStat(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "app.log")
	const original = "old\n"
	const appended = "late\n"
	writeFileT(t, path, original)
	tracked, _ := newTrackedTailerForTest(t, path, uint64(len(original)), 2)

	observedSize, trackable := tracked.trackGrowthAndTruncate()
	if !trackable || observedSize != tracked.offset {
		t.Fatalf(
			"initial tracking = size %d offset %d trackable %t, want clean EOF",
			observedSize,
			tracked.offset,
			trackable,
		)
	}
	if !tracked.canWaitAtCleanBoundary(observedSize) {
		t.Fatal("clean non-draining EOF did not select idle wait")
	}

	// Model an append to a renamed/unlinked but still-open file after Stat,
	// followed by the manager's drain request for the departed path.
	appendFileT(t, path, appended)
	tracked.requestDrain()
	if tracked.canWaitAtCleanBoundary(observedSize) {
		t.Fatal("draining tailer trusted a stale EOF observation")
	}
	latestSize, trackable := tracked.trackGrowthAndTruncate()
	if !trackable {
		t.Fatal("late append was not trackable")
	}
	batch, err := tracked.stageRead(context.Background(), latestSize, false)
	if err != nil {
		t.Fatalf("stage after stale EOF: %v", err)
	}
	matches, err := tracked.stagedBatchMatches(batch)
	if err != nil || !matches {
		t.Fatalf("validate staged append: matches=%t err=%v", matches, err)
	}
	if !tracked.commitBatch(context.Background(), batch) {
		t.Fatal("commit staged append canceled")
	}
	select {
	case event := <-tracked.m.events:
		if string(event.Bytes) != "late" ||
			event.Source.StartOffset != uint64(len(original)) ||
			event.Source.EndOffset != uint64(len(original)+len(appended)) ||
			event.Source.LineNumber != 2 ||
			event.Source.NextLineNumber != 3 {
			t.Fatalf("late drain event = %+v", event)
		}
	default:
		t.Fatal("late append was not drained")
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

func TestManagerRetirementDrainsAppendBeforeFinalBoundaryWithoutReset(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	p := filepath.Join(dir, "app.log")
	writeFileT(t, p, "old\n")
	writer, err := os.OpenFile(p, os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatalf("open retained writer: %v", err)
	}
	t.Cleanup(func() { _ = writer.Close() })

	appendResult := make(chan error, 1)
	var appendOnce sync.Once
	observer := func(tailerPollObservation) {
		appendOnce.Do(func() {
			_, err := writer.WriteString("late\n")
			appendResult <- err
		})
	}
	h := startManagerWithHooks(t, Config{
		InputID: "in", Include: []string{p}, StartAt: StartAtBeginning,
	}, newStore(t), managerTestHooks{beforeRetireCommit: observer})
	h.waitForTexts([]string{"old"})
	originalGeneration := h.col.snapshot()[0].Source.Identity.Generation

	if err := os.Remove(p); err != nil {
		t.Fatalf("remove source with retained writer: %v", err)
	}
	h.waitForTexts([]string{"old", "late"})
	select {
	case err := <-appendResult:
		if err != nil {
			t.Fatalf("append at retirement boundary: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("retirement boundary hook did not run")
	}
	waitFor(t, "retired after appended suffix", func() bool {
		return h.mgr.Health().ActiveSources == 0
	})
	events := h.col.snapshot()
	if len(events) != 2 || events[1].Source.Identity.Generation != originalGeneration {
		t.Fatalf("retirement append replayed/reset generation: %v", h.col.descriptions())
	}
}

func TestManagerRediscoveryCancelsFinalizingRetirementWithoutPartialSplit(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	p := filepath.Join(dir, "app.log")
	outside := filepath.Join(dir, "outside")
	writeFileT(t, p, "done\npartial")

	finalizing := make(chan struct{}, 1)
	releaseFinalizer := make(chan struct{})
	canceled := make(chan struct{}, 1)
	var finalizerOnce sync.Once
	h := startManagerWithHooks(t, Config{
		InputID: "in", Include: []string{p}, StartAt: StartAtBeginning,
	}, newStore(t), managerTestHooks{
		beforeRetireCommit: func(tailerPollObservation) {
			finalizerOnce.Do(func() {
				finalizing <- struct{}{}
				<-releaseFinalizer
			})
		},
		afterRetireCancel: func(tailerPollObservation) {
			select {
			case canceled <- struct{}{}:
			default:
			}
		},
	})
	release := sync.OnceFunc(func() { close(releaseFinalizer) })
	t.Cleanup(release)
	h.waitForTexts([]string{"done"})

	if err := os.Rename(p, outside); err != nil {
		t.Fatalf("move source outside include: %v", err)
	}
	select {
	case <-finalizing:
	case <-time.After(3 * time.Second):
		t.Fatalf("timed out waiting for provisional finalization; health=%+v", h.mgr.Health())
	}
	if err := os.Rename(outside, p); err != nil {
		t.Fatalf("rediscover source: %v", err)
	}
	select {
	case <-canceled:
	case <-time.After(3 * time.Second):
		t.Fatalf("rediscovery did not cancel retirement; health=%+v", h.mgr.Health())
	}
	release()
	appendFileT(t, p, "-continued\n")
	h.waitForTexts([]string{"done", "partial-continued"})
	if events := h.col.snapshot(); len(events) != 2 {
		t.Fatalf("rediscovery split or replayed partial: %v", h.col.descriptions())
	}
}

func TestManagerPartialAcrossPollsAdvancesLineOnlyWhenCompleted(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	p := filepath.Join(dir, "app.log")
	writeFileT(t, p, "part")

	h := startManager(t, Config{
		InputID: "in", Include: []string{p}, StartAt: StartAtBeginning,
	}, newStore(t))
	waitFor(t, "partial file discovered", func() bool {
		return h.mgr.Health().DiscoveredSources == 1
	})
	if got := h.col.snapshot(); len(got) != 0 {
		t.Fatalf("partial input emitted before delimiter: %+v", got)
	}
	appendFileT(t, p, "ial\nnext\n")
	h.waitForTexts([]string{"partial", "next"})
	events := h.col.snapshot()
	if events[0].Source.LineNumber != 1 || events[0].Source.NextLineNumber != 2 ||
		events[1].Source.LineNumber != 2 || events[1].Source.NextLineNumber != 3 {
		t.Fatalf("completed partial source coordinates = %+v, want consecutive lines 1 and 2", events)
	}
}

func TestManagerExactCapLineWaitsForDelimiterAcrossPolls(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	p := filepath.Join(dir, "app.log")
	writeFileT(t, p, "abcd")

	h := startManager(t, Config{
		InputID: "in", Include: []string{p}, StartAt: StartAtBeginning,
		Framing: framing.Options{MaxEventBytes: 4},
	}, newStore(t))
	waitFor(t, "exact-cap partial discovered", func() bool {
		return h.mgr.Health().DiscoveredSources == 1
	})
	if got := h.col.snapshot(); len(got) != 0 {
		t.Fatalf("exact-cap partial emitted before delimiter: %+v", got)
	}

	appendFileT(t, p, "\nok\n")
	h.waitForTexts([]string{"abcd", "ok"})
	events := h.col.snapshot()
	if events[0].Source.LineNumber != 1 || events[0].Source.NextLineNumber != 2 ||
		events[1].Source.LineNumber != 2 || events[1].Source.NextLineNumber != 3 {
		t.Fatalf("exact-cap cross-poll source coordinates = %+v", events)
	}
}

func TestManagerOversizedPartialDiscardsAppendedSuffix(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	p := filepath.Join(dir, "app.log")
	writeFileT(t, p, "abcdefgh")

	h := startManager(t, Config{
		InputID: "in", Include: []string{p}, StartAt: StartAtBeginning,
		Framing: framing.Options{MaxEventBytes: 4},
	}, newStore(t))
	waitFor(t, "oversized partial detected", func() bool {
		return !h.mgr.Health().LastErrorAt.IsZero()
	})
	if got := h.col.snapshot(); len(got) != 0 {
		t.Fatalf("oversized partial emitted before delimiter: %+v", got)
	}

	appendFileT(t, p, "-suffix\nok\n")
	h.waitForTexts([]string{"ok"})
	event := h.col.snapshot()[0]
	if event.Source.LineNumber != 2 || event.Source.NextLineNumber != 3 {
		t.Fatalf(
			"post-oversize source lines = (%d, %d), want (2, 3)",
			event.Source.LineNumber,
			event.Source.NextLineNumber,
		)
	}
}

func TestManagerOversizedPartialRestartReplaysFromDurableBoundary(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	p := filepath.Join(dir, "app.log")
	writeFileT(t, p, "abcdefgh")
	checkpointDir := t.TempDir()

	t.Run("stop while delimiter pending", func(t *testing.T) {
		store, err := NewCheckpointStore(checkpointDir)
		if err != nil {
			t.Fatalf("NewCheckpointStore: %v", err)
		}
		h := startManager(t, Config{
			InputID: "in", Include: []string{p}, StartAt: StartAtBeginning,
			Framing: framing.Options{MaxEventBytes: 4},
		}, store)
		waitFor(t, "oversized partial detected", func() bool {
			return !h.mgr.Health().LastErrorAt.IsZero()
		})
	})

	appendFileT(t, p, "-suffix\nok\n")
	t.Run("restart", func(t *testing.T) {
		store, err := NewCheckpointStore(checkpointDir)
		if err != nil {
			t.Fatalf("NewCheckpointStore: %v", err)
		}
		h := startManager(t, Config{
			InputID: "in", Include: []string{p}, StartAt: StartAtBeginning,
			Framing: framing.Options{MaxEventBytes: 4},
		}, store)
		h.waitForTexts([]string{"ok"})
		event := h.col.snapshot()[0]
		if event.Source.LineNumber != 2 || event.Source.NextLineNumber != 3 {
			t.Fatalf(
				"restart post-oversize source lines = (%d, %d), want (2, 3)",
				event.Source.LineNumber,
				event.Source.NextLineNumber,
			)
		}
	})
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

func TestManagerMatchPathsSortsAndDeduplicatesIncludeGlobs(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	first := filepath.Join(dir, "a.log")
	second := filepath.Join(dir, "b.log")
	excluded := filepath.Join(dir, "skip.log")
	writeFileT(t, second, "")
	writeFileT(t, excluded, "")
	writeFileT(t, first, "")

	manager := &manager{cfg: Config{
		Include: []string{
			filepath.Join(dir, "*.log"),
			filepath.Join(dir, "a.*"),
		},
		Exclude: []string{"skip.*"},
	}}
	got := manager.matchPaths()
	want := []string{first, second}
	if len(got) != len(want) {
		t.Fatalf("matched paths = %v, want %v", got, want)
	}
	for index := range want {
		if got[index] != want[index] {
			t.Fatalf("matched paths = %v, want %v", got, want)
		}
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
	events := h.col.snapshot()
	if events[0].Source.LineNumber != 1 || events[0].Source.NextLineNumber != 2 ||
		events[1].Source.LineNumber != 3 || events[1].Source.NextLineNumber != 4 {
		t.Fatalf("oversized-skip source coordinates = %+v, want lines 1 then 3", events)
	}
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
	appendFileT(t, p, "START b\nsecond continuation\n")
	h.waitForTexts([]string{"START a\ncontinuation line", "START b\nsecond continuation"})
	events := h.col.snapshot()
	if events[0].Source.LineNumber != 1 || events[0].Source.NextLineNumber != 3 ||
		events[1].Source.LineNumber != 3 || events[1].Source.NextLineNumber != 5 {
		t.Fatalf("multiline source coordinates = %+v, want lines [1,3) then [3,5)", events)
	}
}

func repeatByte(b byte, n int) string {
	buf := make([]byte, n)
	for i := range buf {
		buf[i] = b
	}
	return string(buf)
}
