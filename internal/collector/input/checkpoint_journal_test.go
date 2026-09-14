package input

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func journalTestCheckpoint() Checkpoint {
	return Checkpoint{InputID: "input-a", Identity: canonicalIdentityForTest(1, 2, 1, "ab", 64), Path: "/logs/app.log", Offset: 10}
}

func openJournalTestStore(t *testing.T, dir string) *fileCheckpointStore {
	t.Helper()
	store, err := NewCheckpointStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	s := store.(*fileCheckpointStore)
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Error(err)
		}
	})
	return s
}

func crashJournalTestStore(t *testing.T, s *fileCheckpointStore) {
	t.Helper()
	if err := s.journal.Close(); err != nil {
		t.Fatal(err)
	}
	s.journal = nil
}

func TestCheckpointJournalReplaysAtomicAdvancesAndMigratesLegacy(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	cp := journalTestCheckpoint()
	writeCheckpointDocument(t, dir, checkpointDoc{Version: 1, Checkpoints: []Checkpoint{cp}})
	s := openJournalTestStore(t, dir)
	cp.Offset = 20
	second := cp
	second.InputID = "input-b"
	if err := s.SetMany([]Checkpoint{cp, second}); err != nil {
		t.Fatal(err)
	}
	baseline, err := os.ReadFile(s.path)
	if err != nil {
		t.Fatal(err)
	}
	var doc checkpointDoc
	if err := json.Unmarshal(baseline, &doc); err != nil {
		t.Fatal(err)
	}
	if doc.Version != 2 || len(doc.Checkpoints) != 1 || doc.Checkpoints[0].Offset != 10 {
		t.Fatalf("baseline was not a migrated snapshot: %+v", doc)
	}
	crashJournalTestStore(t, s)
	s = openJournalTestStore(t, dir)
	for _, id := range []string{"input-a", "input-b"} {
		got, ok, err := s.Get(id, cp.Identity)
		if err != nil || !ok || got.Offset != 20 {
			t.Fatalf("replay %s = %+v, %v, %v", id, got, ok, err)
		}
	}
}

func TestCheckpointJournalRepairsOnlyIncompleteTail(t *testing.T) {
	t.Parallel()
	for _, tail := range [][]byte{{1, 2, 3}, checkpointJournalRecord(make([]byte, 20))[:checkpointJournalHeaderBytes+1]} {
		t.Run(fmt.Sprintf("%d-bytes", len(tail)), func(t *testing.T) {
			dir := t.TempDir()
			s := openJournalTestStore(t, dir)
			cp := journalTestCheckpoint()
			if err := s.Set(cp); err != nil {
				t.Fatal(err)
			}
			validBytes := s.journalBytes
			if _, err := s.journal.Write(tail); err != nil {
				t.Fatal(err)
			}
			crashJournalTestStore(t, s)
			// Live inspection must not truncate another process's pending append.
			list, err := ReadCheckpoints(dir)
			if err != nil || len(list) != 1 {
				t.Fatalf("inspection = %v, %v", list, err)
			}
			info, err := os.Stat(filepath.Join(dir, checkpointJournalName))
			if err != nil || info.Size() != validBytes+int64(len(tail)) {
				t.Fatalf("inspection mutated journal: %v, %v", info, err)
			}
			s = openJournalTestStore(t, dir)
			info, err = s.journal.Stat()
			if err != nil || info.Size() != validBytes {
				t.Fatalf("tail repair = %v, %v", info, err)
			}
			cp.Offset++
			if err := s.Set(cp); err != nil {
				t.Fatal(err)
			}
			crashJournalTestStore(t, s)
			s = openJournalTestStore(t, dir)
			got, ok, err := s.Get(cp.InputID, cp.Identity)
			if err != nil || !ok || got.Offset != cp.Offset {
				t.Fatalf("append after repair = %+v, %v, %v", got, ok, err)
			}
		})
	}
}

func TestCheckpointJournalFailsClosedOnCorruptionOrMissingJournal(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"checksum", "missing", "length", "sequence-gap"} {
		t.Run(mode, func(t *testing.T) {
			dir := t.TempDir()
			s := openJournalTestStore(t, dir)
			if err := s.Set(journalTestCheckpoint()); err != nil {
				t.Fatal(err)
			}
			path := s.journal.Name()
			crashJournalTestStore(t, s)
			data, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			switch mode {
			case "missing":
				if err := os.Remove(path); err != nil {
					t.Fatal(err)
				}
			case "checksum":
				data[len(data)-1] ^= 1
			case "length":
				binary.LittleEndian.PutUint32(data[:4], maximumCheckpointTransactionBytes+1)
			case "sequence-gap":
				var tx checkpointTransaction
				if err := json.Unmarshal(data[checkpointJournalHeaderBytes:], &tx); err != nil {
					t.Fatal(err)
				}
				tx.Sequence = 2
				payload, err := json.Marshal(tx)
				if err != nil {
					t.Fatal(err)
				}
				data = checkpointJournalRecord(payload)
			}
			if mode != "missing" {
				if err := os.WriteFile(path, data, 0o600); err != nil {
					t.Fatal(err)
				}
			}
			if store, err := NewCheckpointStore(dir); err == nil {
				_ = store.Close()
				t.Fatal("corrupt journal accepted")
			}
		})
	}
}

func TestCheckpointJournalCompactionDoesNotResurrectDeletedIdentity(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	s := openJournalTestStore(t, dir)
	cp := journalTestCheckpoint()
	if err := s.Set(cp); err != nil {
		t.Fatal(err)
	}
	old, err := os.ReadFile(s.journal.Name())
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Delete(cp.InputID, cp.Identity); err != nil {
		t.Fatal(err)
	}
	// Simulate a crash after publishing the deletion snapshot but before
	// reclaiming the old journal. Its covered records must not resurrect cp.
	if _, err := s.journal.Write(old); err != nil {
		t.Fatal(err)
	}
	crashJournalTestStore(t, s)
	s = openJournalTestStore(t, dir)
	if _, ok, err := s.Get(cp.InputID, cp.Identity); err != nil || ok {
		t.Fatalf("deleted checkpoint returned: %v, %v", ok, err)
	}
	cp.Offset++
	if err := s.Set(cp); err != nil {
		t.Fatal(err)
	}
	crashJournalTestStore(t, s)
	s = openJournalTestStore(t, dir)
	if got, ok, err := s.Get(cp.InputID, cp.Identity); err != nil || !ok || got.Offset != cp.Offset {
		t.Fatalf("post-compaction advance = %+v, %v, %v", got, ok, err)
	}
}

func TestCheckpointJournalWritesOnlyChangedPositions(t *testing.T) {
	t.Parallel()
	s := openJournalTestStore(t, t.TempDir())
	cp := journalTestCheckpoint()
	for i := range 1000 {
		entry := cp
		entry.InputID = fmt.Sprintf("input-%d", i)
		s.entries[checkpointKey{inputID: entry.InputID, trackingKey: entry.Identity.TrackingKey()}] = entry
	}
	if err := s.ensureCheckpointJournal(); err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(s.path)
	if err != nil {
		t.Fatal(err)
	}
	cp.InputID = "input-0"
	cp.Offset++
	if err := s.Set(cp); err != nil {
		t.Fatal(err)
	}
	after, err := os.ReadFile(s.path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(before, after) {
		t.Fatal("single-source advance rewrote full snapshot")
	}
	data, err := os.ReadFile(s.journal.Name())
	if err != nil {
		t.Fatal(err)
	}
	var tx checkpointTransaction
	if err := json.Unmarshal(data[checkpointJournalHeaderBytes:], &tx); err != nil {
		t.Fatal(err)
	}
	if len(tx.Checkpoints) != 1 || tx.Checkpoints[0].InputID != cp.InputID {
		t.Fatalf("journal transaction contains unchanged entries: %+v", tx)
	}
}

func TestCheckpointJournalCompactsAtSnapshotSizedThreshold(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	s := openJournalTestStore(t, dir)
	cp := journalTestCheckpoint()
	cp.Path = "/" + strings.Repeat("a", 1<<20)
	if err := s.Set(cp); err != nil {
		t.Fatal(err)
	}
	cp.Offset++
	if err := s.Set(cp); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(s.path)
	if err != nil {
		t.Fatal(err)
	}
	var doc checkpointDoc
	if err := json.Unmarshal(data, &doc); err != nil {
		t.Fatal(err)
	}
	if doc.JournalSequence != 1 || len(doc.Checkpoints) != 1 || doc.Checkpoints[0].Offset != cp.Offset-1 {
		t.Fatalf("compaction baseline sequence=%d entries=%d", doc.JournalSequence, len(doc.Checkpoints))
	}
	crashJournalTestStore(t, s)
	s = openJournalTestStore(t, dir)
	if got, ok, err := s.Get(cp.InputID, cp.Identity); err != nil || !ok || got.Offset != cp.Offset {
		t.Fatalf("compaction lost latest position: offset=%d present=%t error=%v", got.Offset, ok, err)
	}
}

func TestCheckpointJournalRejectsSymlinkWithoutTouchingTarget(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	target := filepath.Join(t.TempDir(), "unrelated")
	content := []byte("unrelated contents")
	if err := os.WriteFile(target, content, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(target, filepath.Join(dir, checkpointJournalName)); err != nil {
		t.Fatal(err)
	}
	if store, err := NewCheckpointStore(dir); err == nil {
		_ = store.Close()
		t.Fatal("journal symlink accepted")
	}
	got, err := os.ReadFile(target)
	if err != nil || !bytes.Equal(got, content) {
		t.Fatalf("external target changed: %q, %v", got, err)
	}
}

func BenchmarkCheckpointAdvance(b *testing.B) {
	for _, count := range []int{1, 1000, 100000} {
		b.Run(fmt.Sprintf("sources-%d", count), func(b *testing.B) {
			store, err := NewCheckpointStore(b.TempDir())
			if err != nil {
				b.Fatal(err)
			}
			defer store.Close()
			s := store.(*fileCheckpointStore)
			cp := journalTestCheckpoint()
			for i := range count {
				entry := cp
				entry.InputID = fmt.Sprintf("input-%d", i)
				s.entries[checkpointKey{inputID: entry.InputID, trackingKey: entry.Identity.TrackingKey()}] = entry
			}
			if err := s.ensureCheckpointJournal(); err != nil {
				b.Fatal(err)
			}
			cp.InputID = "input-0"
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				cp.Offset++
				if err := s.Set(cp); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
