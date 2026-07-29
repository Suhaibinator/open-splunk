package collector

import (
	"strings"
	"sync"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/collector/input"
)

const resumeTestInputID = "input"

func TestCheckpointResumeViewIsEphemeralAndYieldsToTerminalProgress(t *testing.T) {
	t.Parallel()
	durable, err := input.NewCheckpointStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = durable.Close() })
	identity := input.FileIdentity{
		Device: 1, Inode: 2, Generation: 1,
		Fingerprint: strings.Repeat("ab", 32), FingerprintLength: 11,
	}
	checkpoint := func(identity input.FileIdentity, offset, line, nextLine uint64) input.Checkpoint {
		return input.Checkpoint{
			InputID:  resumeTestInputID,
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

	resumed, found, err := view.Get(resumeTestInputID, identity)
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
	terminal, found, err := view.Get(resumeTestInputID, identity)
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
	terminal, found, err = durable.Get(resumeTestInputID, identity)
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
	terminal, found, err = view.Get(resumeTestInputID, identity)
	if err != nil || !found || terminal.Identity.Generation != nextIdentity.Generation ||
		terminal.Offset != 0 {
		t.Fatalf("new-generation terminal Get = %+v, %t, %v", terminal, found, err)
	}

	// A stale old-generation mutation remains harmless after durable progress.
	if err := view.Set(checkpoint(identity, 30, 3, 4)); err != nil {
		t.Fatal(err)
	}
	terminal, found, err = view.Get(resumeTestInputID, identity)
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
		Fingerprint: strings.Repeat("ab", 32), FingerprintLength: 11,
	}
	old := input.Checkpoint{
		InputID: resumeTestInputID, Identity: identity, Path: "/logs/app.log", Offset: 10,
	}
	pending := input.Checkpoint{
		InputID: resumeTestInputID, Identity: identity, Path: "/logs/app.log", Offset: 20,
	}
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
		checkpoint, found, err := manager.Get(resumeTestInputID, identity)
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
	inputID string,
	identity input.FileIdentity,
) (input.Checkpoint, bool, error) {
	checkpoint, found, err := store.CheckpointStore.Get(inputID, identity)
	store.once.Do(func() {
		close(store.getStarted)
		<-store.releaseGet
	})
	return checkpoint, found, err
}

func TestCheckpointResumeViewScopesRecoveryAndSuppressionByInput(t *testing.T) {
	t.Parallel()
	durable, err := input.NewCheckpointStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = durable.Close() })
	identity := input.FileIdentity{
		Device: 71, Inode: 81, Generation: 1,
		Fingerprint: strings.Repeat("cd", 32), FingerprintLength: 9,
	}
	checkpoint := func(inputID string, offset uint64) input.Checkpoint {
		return input.Checkpoint{
			InputID: inputID, Identity: identity, Path: "/logs/shared.log", Offset: offset,
		}
	}
	if err := durable.SetMany([]input.Checkpoint{
		checkpoint("input-a", 10),
		checkpoint("input-b", 10),
	}); err != nil {
		t.Fatal(err)
	}
	manager, resumeView := newCheckpointResumeView(durable, []input.Checkpoint{
		checkpoint("input-a", 20),
		checkpoint("input-b", 30),
	})
	if resumeView == nil {
		t.Fatal("newCheckpointResumeView returned no overlay")
	}

	for inputID, wantOffset := range map[string]uint64{"input-a": 20, "input-b": 30} {
		got, found, getErr := manager.Get(inputID, identity)
		if getErr != nil || !found || got.InputID != inputID || got.Offset != wantOffset {
			t.Fatalf(
				"resume Get(%q) = (%+v, %t, %v), want input-scoped offset %d",
				inputID, got, found, getErr, wantOffset,
			)
		}
	}

	// This advances beyond input-a's pending coordinate but remains behind
	// input-b's. It must persist; an identity-only overlay would suppress it
	// against input-b's unrelated high-water mark.
	if err := manager.Set(checkpoint("input-a", 25)); err != nil {
		t.Fatal(err)
	}
	gotA, foundA, err := durable.Get("input-a", identity)
	if err != nil || !foundA || gotA.Offset != 25 {
		t.Fatalf("durable input-a = (%+v, %t, %v), want offset 25", gotA, foundA, err)
	}
	gotB, foundB, err := durable.Get("input-b", identity)
	if err != nil || !foundB || gotB.Offset != 10 {
		t.Fatalf("durable input-b = (%+v, %t, %v), want unchanged offset 10", gotB, foundB, err)
	}
	if len(resumeView.pending) != 1 {
		t.Fatalf("pending overlays = %+v, want only input-b", resumeView.pending)
	}
}
