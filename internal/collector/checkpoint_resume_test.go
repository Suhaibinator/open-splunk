package collector

import (
	"sync"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/collector/input"
)

func TestCheckpointResumeViewIsEphemeralAndYieldsToTerminalProgress(t *testing.T) {
	t.Parallel()
	durable, err := input.NewCheckpointStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = durable.Close() })
	identity := input.FileIdentity{
		Device: 1, Inode: 2, Generation: 1,
		Fingerprint: "fingerprint", FingerprintLength: 11,
	}
	checkpoint := func(identity input.FileIdentity, offset, line, nextLine uint64) input.Checkpoint {
		return input.Checkpoint{
			Identity: identity, Path: "/logs/app.log", Offset: offset,
			LineNumber: line, NextLineNumber: nextLine,
		}
	}
	if err := durable.Set(checkpoint(identity, 10, 1, 2)); err != nil {
		t.Fatal(err)
	}
	// A legacy pending WAL mark lacks next_line_number. File discovery derives
	// that cursor, but the resulting enrichment must remain ephemeral until the
	// pending byte position receives a terminal disposition.
	pending := checkpoint(identity, 20, 2, 0)
	view, resumeView := newCheckpointResumeView(durable, []input.Checkpoint{pending})
	if resumeView == nil {
		t.Fatal("newCheckpointResumeView returned no overlay for a pending checkpoint")
	}

	resumed, found, err := view.Get(identity)
	if err != nil || !found || resumed.Offset != 20 || resumed.LineNumber != 2 {
		t.Fatalf("resume Get = %+v, %t, %v", resumed, found, err)
	}
	list, err := durable.List()
	if err != nil || len(list) != 1 || list[0].Offset != 10 {
		t.Fatalf("resume List exposed nonterminal state: %+v, %v", list, err)
	}
	if err := view.SetMany([]input.Checkpoint{
		checkpoint(identity, 20, 2, 3),
	}); err != nil {
		t.Fatal(err)
	}
	list, err = durable.List()
	if err != nil || len(list) != 1 || list[0].Offset != 10 {
		t.Fatalf("resume Set persisted nonterminal state: %+v, %v", list, err)
	}

	// Once terminal delivery persists the pending position, equal-position
	// reads prefer that validated durable state and cursor enrichment is safe.
	if err := durable.Set(pending); err != nil {
		t.Fatal(err)
	}
	terminal, found, err := view.Get(identity)
	if err != nil || !found || terminal.Offset != 20 || terminal.NextLineNumber != 0 {
		t.Fatalf("equal-position terminal Get = %+v, %t, %v", terminal, found, err)
	}
	resumeView.pruneCovered([]input.Checkpoint{pending})
	if len(resumeView.pending) != 0 || resumeView.active.Load() {
		t.Fatalf("terminal commit retained pending overlay: %+v", resumeView.pending)
	}
	if err := view.Set(checkpoint(identity, 20, 2, 3)); err != nil {
		t.Fatal(err)
	}
	terminal, found, err = durable.Get(identity)
	if err != nil || !found || terminal.Offset != 20 || terminal.NextLineNumber != 3 {
		t.Fatalf("terminal cursor enrichment = %+v, %t, %v", terminal, found, err)
	}

	// A truncation creates a new generation at offset zero. It must pass
	// through the old-generation overlay immediately so future events cannot
	// collide with the pending source generation.
	nextIdentity := identity
	nextIdentity.Generation++
	if err := view.Set(checkpoint(nextIdentity, 0, 0, 1)); err != nil {
		t.Fatal(err)
	}
	terminal, found, err = view.Get(identity)
	if err != nil || !found || terminal.Identity.Generation != nextIdentity.Generation ||
		terminal.Offset != 0 {
		t.Fatalf("new-generation terminal Get = %+v, %t, %v", terminal, found, err)
	}

	// A stale old-generation mutation remains harmless after durable progress.
	if err := view.Set(checkpoint(identity, 30, 3, 4)); err != nil {
		t.Fatal(err)
	}
	terminal, found, err = view.Get(identity)
	if err != nil || !found || terminal.Identity.Generation != nextIdentity.Generation ||
		terminal.Offset != 0 {
		t.Fatalf("Get after stale old-generation Set = %+v, %t, %v", terminal, found, err)
	}
}

func TestCheckpointResumeViewLookupIsAtomicWithTerminalPrune(t *testing.T) {
	t.Parallel()
	durable, err := input.NewCheckpointStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = durable.Close() })
	identity := input.FileIdentity{
		Device: 1, Inode: 2, Generation: 1,
		Fingerprint: "fingerprint", FingerprintLength: 11,
	}
	old := input.Checkpoint{Identity: identity, Path: "/logs/app.log", Offset: 10}
	pending := input.Checkpoint{Identity: identity, Path: "/logs/app.log", Offset: 20}
	if err := durable.Set(old); err != nil {
		t.Fatal(err)
	}
	blocking := &blockingGetCheckpointStore{
		CheckpointStore: durable,
		getStarted:      make(chan struct{}),
		releaseGet:      make(chan struct{}),
	}
	manager, resumeView := newCheckpointResumeView(blocking, []input.Checkpoint{pending})

	type getResult struct {
		checkpoint input.Checkpoint
		found      bool
		err        error
	}
	got := make(chan getResult, 1)
	go func() {
		checkpoint, found, err := manager.Get(identity)
		got <- getResult{checkpoint: checkpoint, found: found, err: err}
	}()
	<-blocking.getStarted
	if resumeView.mu.TryLock() {
		resumeView.mu.Unlock()
		t.Fatal("resume view did not linearize the durable lookup with terminal pruning")
	}
	if err := durable.Set(pending); err != nil {
		t.Fatal(err)
	}
	pruned := make(chan struct{})
	go func() {
		resumeView.pruneCovered([]input.Checkpoint{pending})
		close(pruned)
	}()
	close(blocking.releaseGet)
	result := <-got
	if result.err != nil || !result.found || result.checkpoint.Offset != pending.Offset {
		t.Fatalf("lookup across terminal prune = %+v, want pending offset %d", result, pending.Offset)
	}
	<-pruned
}

type blockingGetCheckpointStore struct {
	input.CheckpointStore

	once       sync.Once
	getStarted chan struct{}
	releaseGet chan struct{}
}

func (store *blockingGetCheckpointStore) Get(
	identity input.FileIdentity,
) (input.Checkpoint, bool, error) {
	checkpoint, found, err := store.CheckpointStore.Get(identity)
	store.once.Do(func() {
		close(store.getStarted)
		<-store.releaseGet
	})
	return checkpoint, found, err
}
