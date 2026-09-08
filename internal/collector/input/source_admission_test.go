package input

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
)

func capacityCheckpoint(inode uint64) Checkpoint {
	cp := journalTestCheckpoint()
	cp.Identity.Inode = inode
	return cp
}

func TestCheckpointCapacityIsAtomicAcrossInputsAndRestarts(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	s := openJournalTestStore(t, dir)
	s.maximumEntries = 2
	first, second, third := capacityCheckpoint(1), capacityCheckpoint(2), capacityCheckpoint(3)
	second.InputID = "input-b"
	if err := s.SetMany([]Checkpoint{first, second, first}); err != nil {
		t.Fatal(err)
	}
	initialBytes := s.journalBytes
	first.Offset++
	if err := s.SetMany([]Checkpoint{first, third}); !errors.Is(err, errCheckpointCapacity) {
		t.Fatalf("mixed update/new batch = %v, want capacity rejection", err)
	}
	if got, _, _ := s.Get(first.InputID, first.Identity); got.Offset != first.Offset-1 || s.journalBytes != initialBytes {
		t.Fatalf("failed batch mutated state: %+v, journal bytes %d", got, s.journalBytes)
	}
	if err := s.Set(first); err != nil {
		t.Fatal(err)
	}
	crashJournalTestStore(t, s)
	s = openJournalTestStore(t, dir)
	s.maximumEntries = 2
	if got, found, err := s.Get(first.InputID, first.Identity); err != nil || !found || got.Offset != first.Offset {
		t.Fatalf("journal resume = %+v, %t, %v", got, found, err)
	}
	for inode := uint64(3); inode < 20; inode++ {
		if err := s.Set(capacityCheckpoint(inode)); !errors.Is(err, errCheckpointCapacity) {
			t.Fatalf("churn identity %d = %v", inode, err)
		}
	}
	first.Identity.Generation++
	first.Offset = 0
	if err := s.Set(first); err != nil {
		t.Fatalf("same physical identity generation = %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s = openJournalTestStore(t, dir)
	s.maximumEntries = 2
	if got, found, err := s.Get(first.InputID, first.Identity); err != nil || !found || got.Identity.Generation != first.Identity.Generation {
		t.Fatalf("snapshot resume = %+v, %t, %v", got, found, err)
	}
	if list, err := s.List(); err != nil || len(list) != 2 {
		t.Fatalf("retained identities = %d, %v", len(list), err)
	}
}

func TestCheckpointPendingReservationsPreserveTerminalCapacity(t *testing.T) {
	t.Parallel()
	s := openJournalTestStore(t, t.TempDir())
	s.maximumEntries = 2
	first, pending, excess := capacityCheckpoint(1), capacityCheckpoint(2), capacityCheckpoint(3)
	if err := s.Set(first); err != nil {
		t.Fatal(err)
	}
	if err := s.ReservePending([]Checkpoint{first, pending, pending}); err != nil {
		t.Fatal(err)
	}
	if _, found, _ := s.Get(pending.InputID, pending.Identity); found {
		t.Fatal("reservation persisted a nonterminal checkpoint")
	}
	if err := s.ReservePending([]Checkpoint{excess}); !errors.Is(err, errCheckpointCapacity) {
		t.Fatalf("excess reservation = %v", err)
	}
	if err := s.Set(excess); !errors.Is(err, errCheckpointCapacity) {
		t.Fatalf("new discovery consumed pending slot: %v", err)
	}
	persist := s.persistUpdates
	s.persistUpdates = func([]Checkpoint) error { return errors.New("write failed") }
	if err := s.Set(pending); err == nil || len(s.reserved) != 1 {
		t.Fatalf("failed terminal write lost reservation: %v, %d", err, len(s.reserved))
	}
	s.persistUpdates = persist
	if err := s.Set(pending); err != nil || len(s.reserved) != 0 {
		t.Fatalf("terminal write = %v, pending reservations = %d", err, len(s.reserved))
	}
	if err := s.Set(excess); !errors.Is(err, errCheckpointCapacity) {
		t.Fatalf("terminal reservation counted twice or lost: %v", err)
	}
}

func TestCheckpointByteCapacityReservesTerminalMetadataAndPendingIdentity(t *testing.T) {
	t.Parallel()
	s := openJournalTestStore(t, t.TempDir())
	first, pending, excess := capacityCheckpoint(1), capacityCheckpoint(2), capacityCheckpoint(3)
	firstSize, err := checkpointSnapshotEntryBytes(first)
	if err != nil {
		t.Fatal(err)
	}
	pendingSize, err := checkpointSnapshotEntryBytes(pending)
	if err != nil {
		t.Fatal(err)
	}
	s.maximumSnapshotBytes = 1024 + firstSize + pendingSize
	if err := s.Set(first); err != nil {
		t.Fatal(err)
	}
	if err := s.ReservePending([]Checkpoint{pending}); err != nil {
		t.Fatal(err)
	}
	bytes := s.journalBytes
	if err := s.Set(excess); err == nil || s.journalBytes != bytes {
		t.Fatalf("new source used reserved snapshot bytes: %v", err)
	}
	first.Offset++
	first.GuardFingerprint, first.GuardLength = first.Identity.Fingerprint, 2
	if err := s.Set(first); err != nil {
		t.Fatalf("terminal position/guard enrichment exhausted capacity: %v", err)
	}
	if err := s.Set(pending); err != nil {
		t.Fatalf("reserved terminal identity exhausted capacity: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	reopened := openJournalTestStore(t, s.dir)
	if list, err := reopened.List(); err != nil || len(list) != 2 {
		t.Fatalf("bounded snapshot cannot reopen: %d, %v", len(list), err)
	}
}

func TestCheckpointRecoveryRejectsExcessIdentitiesAndSnapshotBytes(t *testing.T) {
	t.Parallel()
	t.Run("snapshot", func(t *testing.T) {
		dir := t.TempDir()
		writeCheckpointDocument(t, dir, checkpointDoc{Version: 1, Checkpoints: []Checkpoint{capacityCheckpoint(1), capacityCheckpoint(2)}})
		s := &fileCheckpointStore{path: filepath.Join(dir, checkpointFileName), entries: make(map[checkpointKey]Checkpoint), maximumEntries: 1}
		if err := s.load(); !errors.Is(err, errCheckpointCapacity) || len(s.entries) != 0 {
			t.Fatalf("oversized snapshot = %v, loaded entries %d", err, len(s.entries))
		}
	})
	t.Run("journal", func(t *testing.T) {
		dir := t.TempDir()
		s := openJournalTestStore(t, dir)
		if err := s.SetMany([]Checkpoint{capacityCheckpoint(1), capacityCheckpoint(2)}); err != nil {
			t.Fatal(err)
		}
		crashJournalTestStore(t, s)
		before, err := os.ReadFile(filepath.Join(dir, checkpointJournalName))
		if err != nil {
			t.Fatal(err)
		}
		recovered := &fileCheckpointStore{dir: dir, path: filepath.Join(dir, checkpointFileName), entries: make(map[checkpointKey]Checkpoint), maximumEntries: 1}
		if err := recovered.load(); err != nil {
			t.Fatal(err)
		}
		if err := recovered.loadCheckpointJournal(); !errors.Is(err, errCheckpointCapacity) {
			t.Fatalf("oversized journal = %v", err)
		}
		after, err := os.ReadFile(filepath.Join(dir, checkpointJournalName))
		if err != nil || string(before) != string(after) {
			t.Fatalf("capacity rejection changed journal: %v", err)
		}
	})
	t.Run("byte limit before decoding", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, checkpointFileName)
		f, err := os.Create(path)
		if err != nil {
			t.Fatal(err)
		}
		if err := f.Truncate(maximumCheckpointSnapshotBytes + 1); err != nil {
			t.Fatal(err)
		}
		if err := f.Close(); err != nil {
			t.Fatal(err)
		}
		if _, err := NewCheckpointStore(dir); err == nil || !strings.Contains(err.Error(), "snapshot byte capacity") {
			t.Fatalf("oversized snapshot = %v", err)
		}
	})
	t.Run("journal byte limit before replay", func(t *testing.T) {
		dir := t.TempDir()
		s := openJournalTestStore(t, dir)
		if err := s.Set(capacityCheckpoint(1)); err != nil {
			t.Fatal(err)
		}
		if err := s.journal.Truncate(maximumCheckpointJournalBytes + 1); err != nil {
			t.Fatal(err)
		}
		crashJournalTestStore(t, s)
		if _, err := NewCheckpointStore(dir); err == nil || !strings.Contains(err.Error(), "recovery byte capacity") {
			t.Fatalf("oversized recovery journal = %v", err)
		}
		if _, err := ReadCheckpoints(dir); err == nil || !strings.Contains(err.Error(), "recovery byte capacity") {
			t.Fatalf("oversized inspection journal = %v", err)
		}
	})
}

func TestManagerSharesSourceCapacityAndRetriesWithoutSkippingBytes(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	firstPath, secondPath := filepath.Join(dir, "first.log"), filepath.Join(dir, "second.log")
	writeFileT(t, firstPath, "first\n")
	writeFileT(t, secondPath, "second\n")
	s := openJournalTestStore(t, t.TempDir())
	s.sourceSlots = make(chan struct{}, 1)
	first := startManager(t, Config{InputID: "first", Include: []string{firstPath}}, s)
	first.waitForTexts([]string{"first"})
	second := startManager(t, Config{InputID: "second", Include: []string{secondPath}, StartAt: StartAtEnd}, s)
	waitFor(t, "shared capacity rejection", func() bool {
		return strings.Contains(second.mgr.Health().StatusMessage, "live source capacity")
	})
	if list, err := s.List(); err != nil || len(list) != 1 {
		t.Fatalf("denied source persisted discovery: %d, %v", len(list), err)
	}
	appendFileT(t, firstPath, "continued\n")
	first.waitForTexts([]string{"first", "continued"})
	if err := os.Rename(firstPath, filepath.Join(dir, "first.retired")); err != nil {
		t.Fatal(err)
	}
	second.waitForTexts([]string{"second"})
	waitFor(t, "capacity recovered", func() bool {
		return second.mgr.Health().State == opensplunk.CollectorInputState_COLLECTOR_INPUT_STATE_HEALTHY
	})
}

func TestManagerFailedCheckpointAdmissionReleasesLiveSlot(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	s := openJournalTestStore(t, t.TempDir())
	s.maximumEntries = 1
	s.sourceSlots = make(chan struct{}, 1)
	if err := s.Set(capacityCheckpoint(1)); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "new.log")
	writeFileT(t, path, "new\n")
	api, err := NewManager(Config{InputID: "input-a", Include: []string{path}}, s)
	if err != nil {
		t.Fatal(err)
	}
	m := api.(*manager)
	for range 3 {
		m.pollOnce(context.Background(), false)
		if len(s.sourceSlots) != 0 || len(m.tailers) != 0 || !strings.Contains(m.Health().StatusMessage, "retained source identity capacity") {
			t.Fatalf("failed discovery retained live resources: slots %d tailers %d health %+v", len(s.sourceSlots), len(m.tailers), m.Health())
		}
	}
}

func TestManagerSourceChurnRemainsBoundedAcrossRestart(t *testing.T) {
	t.Parallel()
	logs, state := t.TempDir(), t.TempDir()
	s := openJournalTestStore(t, state)
	s.maximumEntries = 2
	for index := range 6 {
		if index == 2 {
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			s = openJournalTestStore(t, state)
			s.maximumEntries = 2
		}
		path := filepath.Join(logs, fmt.Sprintf("source-%d.log", index))
		writeFileT(t, path, fmt.Sprintf("source %d\n", index))
		api, err := NewManager(Config{InputID: "in", Include: []string{filepath.Join(logs, "*.log")}}, s)
		if err != nil {
			t.Fatal(err)
		}
		m := api.(*manager)
		ctx, cancel := context.WithCancel(context.Background())
		m.pollOnce(ctx, true)
		cancel()
		m.wg.Wait()
		if index >= 2 && (len(m.tailers) != 0 || !strings.Contains(m.Health().StatusMessage, "retained source identity capacity")) {
			t.Fatalf("churn bypassed persisted admission: tailers %d, health %+v", len(m.tailers), m.Health())
		}
		if entries, err := s.List(); err != nil || len(entries) != min(index+1, 2) {
			t.Fatalf("churn retained %d identities: %v", len(entries), err)
		}
		// Keep old inodes allocated while moving them outside discovery.
		if err := os.Rename(path, path+".retired"); err != nil {
			t.Fatal(err)
		}
	}
}

func TestManagerFailedTailerStartClosesFileAndReleasesSlot(t *testing.T) {
	t.Parallel()
	s := openJournalTestStore(t, t.TempDir())
	s.sourceSlots = make(chan struct{}, 1)
	path := filepath.Join(t.TempDir(), "source.log")
	writeFileT(t, path, "original\n")
	api, err := NewManager(Config{InputID: "in", Include: []string{path}}, s)
	if err != nil {
		t.Fatal(err)
	}
	m := api.(*manager)
	var opened *os.File
	m.identityFn = func(f *os.File, info os.FileInfo, bytes int) (FileIdentity, error) {
		opened = f
		return identityFor(f, info, bytes)
	}
	m.beforeStartGuardObserver = func(tailerPollObservation) { writeFileT(t, path, "changed!\n") }
	m.pollOnce(context.Background(), true)
	if len(m.tailers) != 0 || len(s.sourceSlots) != 0 {
		t.Fatalf("failed start retained source resources: %d tailers, %d slots", len(m.tailers), len(s.sourceSlots))
	}
	if opened == nil {
		t.Fatal("test did not reach file admission")
	}
	if _, err := opened.Stat(); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("failed start retained its descriptor: %v", err)
	}
}

func TestManagerDiscoveryOverflowPreservesExistingLifecycle(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "existing.log")
	writeFileT(t, path, "first\n")
	s := openJournalTestStore(t, t.TempDir())
	h := startManager(t, Config{InputID: "in", Include: []string{filepath.Join(dir, "*.log")}}, s)
	h.waitForTexts([]string{"first"})
	// Duplicate literal includes exercise overflow with a bounded fixture;
	// the scan-work limit counts lookups even when they refer to the same file.
	// A separate manager keeps one synthetic lifecycle for deterministic polls.
	probeAPI, err := NewManager(Config{InputID: "probe", Include: make([]string, maximumDiscoveryEntries+1)}, s)
	if err != nil {
		t.Fatal(err)
	}
	probe := probeAPI.(*manager)
	for index := range probe.cfg.Include {
		probe.cfg.Include[index] = path
	}
	tracked := &tailer{}
	probe.tailers["existing"] = tracked
	for range 3 {
		probe.pollOnce(context.Background(), false)
	}
	if tracked.missingDiscoveries != 0 || tracked.retireRequested.Load() {
		t.Fatal("incomplete snapshot requested retirement")
	}
	if !strings.Contains(probe.Health().StatusMessage, "discovery capacity") {
		t.Fatalf("overflow health = %+v", probe.Health())
	}
	appendFileT(t, path, "continued\n")
	h.waitForTexts([]string{"first", "continued"})
}

func TestSourceSlotReleaseIsIdempotent(t *testing.T) {
	t.Parallel()
	s := openJournalTestStore(t, t.TempDir())
	s.sourceSlots = make(chan struct{}, 1)
	release, ok := s.TryAcquireSource()
	if !ok {
		t.Fatal("initial acquire failed")
	}
	if _, ok := s.TryAcquireSource(); ok {
		t.Fatal("admitted beyond capacity")
	}
	release()
	release()
	if release, ok := s.TryAcquireSource(); !ok {
		t.Fatal("released capacity unavailable")
	} else {
		release()
	}
}
