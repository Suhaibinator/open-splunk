package wal

import (
	"context"
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"google.golang.org/protobuf/proto"
)

// TestAppendBatchTooLargeReturnsSentinel confirms that a batch whose on-disk
// record can never fit MaxQueueBytes (even in an empty queue) returns the
// terminal ErrBatchTooLarge, not the transient ErrQueueFull. (FIX 2)
func TestAppendBatchTooLargeReturnsSentinel(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	opts := defaultOpts(dir)
	opts.MaxQueueBytes = 8 // smaller than any real batch record
	q, err := Open(opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = q.Close() })

	_, err = q.Append(makeEvents("x"))
	if !errors.Is(err, ErrBatchTooLarge) {
		t.Fatalf("Append of oversized batch = %v, want ErrBatchTooLarge", err)
	}
	if errors.Is(err, ErrQueueFull) {
		t.Fatalf("oversized batch must not be reported as ErrQueueFull")
	}

	// The sequence must not have advanced (a clean no-op, like ErrQueueFull).
	if st := q.Stats(); st.NextBatchSequence != 1 {
		t.Fatalf("NextBatchSequence after ErrBatchTooLarge = %d, want 1 (no sequence burned)", st.NextBatchSequence)
	}
}

// TestWALFilesAreOwnerOnly verifies the WAL directory, segments, and meta file
// carry no group/world access, since they hold raw event payloads. (FIX 6)
func TestWALFilesAreOwnerOnly(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	q, err := Open(defaultOpts(dir))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if _, err := q.Append(makeEvents("a", "b")); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if err := q.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	assertNoGroupOrWorld(t, dir)
	assertNoGroupOrWorld(t, filepath.Join(dir, metaFileName))
	for _, name := range listWALFiles(t, dir) {
		assertNoGroupOrWorld(t, filepath.Join(dir, name))
	}
}

// assertNoGroupOrWorld fails if path grants any permission to group or other.
func assertNoGroupOrWorld(t *testing.T, path string) {
	t.Helper()
	fi, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat %s: %v", path, err)
	}
	if perm := fi.Mode().Perm(); perm&0o077 != 0 {
		t.Fatalf("%s mode = %o, want no group/world access (0o077 bits clear)", path, perm)
	}
}

func TestOpenRejectsInvalidOrUnknownMetaState(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		meta string
		want string
	}{
		{
			name: "unsupported version",
			meta: `{"format_version":2,"next_batch_sequence":1,"last_acked_batch_sequence":0}`,
			want: "unsupported format_version",
		},
		{
			name: "ack reaches next sequence",
			meta: `{"format_version":1,"next_batch_sequence":4,"last_acked_batch_sequence":4}`,
			want: "at or beyond next_batch_sequence",
		},
		{
			name: "unknown field",
			meta: `{"format_version":1,"next_batch_sequence":1,"last_acked_batch_sequence":0,"next_batch_sequnce":2}`,
			want: "unknown field",
		},
		{
			name: "multiple values",
			meta: `{"format_version":1,"next_batch_sequence":1,"last_acked_batch_sequence":0} {}`,
			want: "trailing data",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			dir := t.TempDir()
			if err := os.WriteFile(filepath.Join(dir, metaFileName), []byte(test.meta), 0o600); err != nil {
				t.Fatalf("write meta: %v", err)
			}
			opened, err := Open(defaultOpts(dir))
			if err == nil {
				_ = opened.Close()
				t.Fatal("Open accepted invalid metadata")
			}
			if !strings.Contains(err.Error(), test.want) {
				t.Fatalf("Open error = %q, want %q", err, test.want)
			}
		})
	}
}

func TestOpenRejectsZeroProtocolMajor(t *testing.T) {
	t.Parallel()
	opts := defaultOpts(t.TempDir())
	opts.ProtocolMajor = 0
	if opened, err := Open(opts); err == nil {
		_ = opened.Close()
		t.Fatal("Open accepted ProtocolMajor zero")
	}
}

func TestRecoveryQuarantinesSparseRecordAboveStableAllocationBound(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	opts := defaultOpts(dir)
	opts.MaxQueueBytes = 1 << 20
	q, err := Open(opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := q.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	path := filepath.Join(dir, segmentName(1))
	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	const forgedPayloadLength = 32 << 20
	var header [recordHeaderSize]byte
	binary.BigEndian.PutUint32(header[0:4], forgedPayloadLength)
	if _, err := file.Write(header[:]); err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	// Sparse allocation models the dangerous case: the apparent record is large
	// enough for the old scanner to allocate it, without requiring test memory.
	if err := file.Truncate(recordHeaderSize + forgedPayloadLength); err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := Open(opts)
	if err != nil {
		t.Fatalf("Open forged sparse segment: %v", err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	stats := reopened.Stats()
	if stats.QueuedBatches != 0 || stats.QuarantinedSegments != 1 ||
		stats.QuarantinedBytes < recordHeaderSize+forgedPayloadLength {
		t.Fatalf("sparse oversized recovery stats = %+v", stats)
	}
	if live := listWALFiles(t, dir); len(live) != 0 {
		t.Fatalf("oversized sparse record remained live: %v", live)
	}
}

func TestAppendRejectsPayloadAboveStableRecoveryBound(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	opts := defaultOpts(dir)
	opts.MaxQueueBytes = 0
	q, err := Open(opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = q.Close() })

	_, err = q.Append([]*opensplunkv1.LogEvent{{
		EventId: "oversized",
		Raw:     make([]byte, int(maximumRecordPayloadBytes)+1),
	}})
	if !errors.Is(err, ErrBatchTooLarge) {
		t.Fatalf("Append above stable record ceiling = %v, want ErrBatchTooLarge", err)
	}
	if stats := q.Stats(); stats.NextBatchSequence != 1 || stats.QueuedBatches != 0 || stats.PhysicalBytes != 0 {
		t.Fatalf("oversized Append mutated queue: %+v", stats)
	}
	if live := listWALFiles(t, dir); len(live) != 0 {
		t.Fatalf("oversized Append created segments: %v", live)
	}
}

func TestRecoveryIgnoresLoweredMaxQueueForPreviouslyValidRecord(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	original := defaultOpts(dir)
	original.MaxQueueBytes = 0
	q, err := Open(original)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if _, err := q.Append([]*opensplunkv1.LogEvent{{
		EventId: "retained",
		Raw:     make([]byte, 256<<10),
	}}); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if err := q.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	lowered := original
	lowered.MaxQueueBytes = 128
	reopened, err := Open(lowered)
	if err != nil {
		t.Fatalf("Open after lowering MaxQueueBytes: %v", err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	stats := reopened.Stats()
	if stats.QueuedBatches != 1 || stats.QuarantinedSegments != 0 {
		t.Fatalf("lower-limit recovery stats = %+v, want one live batch and no quarantine", stats)
	}
	batch, err := reopened.NextBatch(context.Background())
	if err != nil {
		t.Fatalf("NextBatch: %v", err)
	}
	if batch.GetBatchSequence() != 1 || len(batch.GetEvents()) != 1 || batch.GetEvents()[0].GetEventId() != "retained" {
		t.Fatalf("recovered batch = %+v", batch)
	}
}

func TestSemanticBarrierWithoutMetaNeverReusesAllocatedSequences(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	opts := defaultOpts(dir)
	q, err := Open(opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	for _, id := range []string{"one", "two", "three"} {
		if _, err := q.Append(makeEvents(id)); err != nil {
			t.Fatalf("Append %q: %v", id, err)
		}
	}
	if err := q.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	path := filepath.Join(dir, listWALFiles(t, dir)[0])
	scan, err := scanSegment(path)
	if err != nil {
		t.Fatalf("scan: %v", err)
	}
	scan.records[1].batch.EventIdsSha256[0] ^= 0xff
	payload, err := proto.Marshal(scan.records[1].batch)
	if err != nil {
		t.Fatalf("marshal corrupt record: %v", err)
	}
	framed, err := encodeRecord(payload)
	if err != nil {
		t.Fatalf("frame corrupt record: %v", err)
	}
	if len(framed) != recordHeaderSize+int(scan.records[1].payloadLen) {
		t.Fatal("digest mutation unexpectedly changed record length")
	}
	file, err := os.OpenFile(path, os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.WriteAt(framed, scan.records[1].payloadOff-recordHeaderSize); err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(dir, metaFileName)); err != nil {
		t.Fatal(err)
	}

	reopened, err := Open(opts)
	if err != nil {
		t.Fatalf("Open semantic barrier without meta: %v", err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	stats := reopened.Stats()
	if stats.QueuedBatches != 1 || stats.QuarantinedSegments != 1 || stats.NextBatchSequence != 4 {
		t.Fatalf("recovered allocation floor = %+v, want prefix one and next sequence 4", stats)
	}
	appended, err := reopened.Append(makeEvents("after"))
	if err != nil {
		t.Fatalf("Append after recovery: %v", err)
	}
	if appended.GetBatchSequence() != 4 {
		t.Fatalf("reused allocated sequence: got %d, want 4", appended.GetBatchSequence())
	}
}

func TestMetaPublicationFailureSealsSegmentBeforeLaterAppend(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	opts := defaultOpts(dir)
	opened, err := Open(opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	q := opened.(*queue)
	if _, err := q.Append(makeEvents("one")); err != nil {
		t.Fatalf("Append sequence 1: %v", err)
	}

	injected := errors.New("injected error after meta publication")
	q.mu.Lock()
	q.persistMetaFile = func(dir string, meta walMeta) error {
		if err := writeMeta(dir, meta); err != nil {
			return err
		}
		return injected
	}
	q.mu.Unlock()
	if _, err := q.Append(makeEvents("burned-two")); !errors.Is(err, injected) {
		t.Fatalf("Append sequence 2 = %v, want injected publication error", err)
	}
	q.mu.Lock()
	if q.active != nil || len(q.segments) != 1 || !q.segments[0].sealed || q.nextSeq != 3 {
		active := q.active != nil
		segmentCount := len(q.segments)
		sealed := segmentCount > 0 && q.segments[0].sealed
		nextSequence := q.nextSeq
		q.mu.Unlock()
		t.Fatalf(
			"meta failure state: active=%t segments=%d sealed=%t next=%d; want false/1/true/3",
			active, segmentCount, sealed, nextSequence,
		)
	}
	q.persistMetaFile = writeMeta
	q.mu.Unlock()
	batch, err := q.Append(makeEvents("three"))
	if err != nil {
		t.Fatalf("Append sequence 3: %v", err)
	}
	if batch.GetBatchSequence() != 3 {
		t.Fatalf("Append after burned sequence = %d, want 3", batch.GetBatchSequence())
	}
	if err := q.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	segments := listWALFiles(t, dir)
	if len(segments) != 2 || segments[0] != segmentName(1) || segments[1] != segmentName(3) {
		t.Fatalf("segments after meta failure = %v, want first sequences 1 and 3", segments)
	}
	path := filepath.Join(dir, segments[1])
	scan, err := scanSegment(path)
	if err != nil {
		t.Fatalf("scan sequence 3: %v", err)
	}
	scan.records[0].batch.EventIdsSha256[0] ^= 0xff
	payload, err := proto.Marshal(scan.records[0].batch)
	if err != nil {
		t.Fatal(err)
	}
	framed, err := encodeRecord(payload)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, framed, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(dir, metaFileName)); err != nil {
		t.Fatal(err)
	}

	reopened, err := Open(opts)
	if err != nil {
		t.Fatalf("Open without meta after semantic corruption: %v", err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	stats := reopened.Stats()
	if stats.QueuedBatches != 1 || stats.QuarantinedSegments != 1 || stats.NextBatchSequence != 4 {
		t.Fatalf("recovered burned-sequence floor = %+v, want next sequence 4", stats)
	}
	next, err := reopened.Append(makeEvents("four"))
	if err != nil {
		t.Fatalf("Append after recovery: %v", err)
	}
	if next.GetBatchSequence() != 4 {
		t.Fatalf("sequence after recovery = %d, want 4", next.GetBatchSequence())
	}
}

func TestPhysicalCorruptionWithoutMetaBurnsAttemptedRecordSequence(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name         string
		eventIDs     []string
		corruptIndex int
		minimumNext  uint64
		mutate       func(t *testing.T, path string, record scannedRecord)
	}{
		{
			name:         "CRC mismatch at tail",
			eventIDs:     []string{"one", "two"},
			corruptIndex: 1,
			minimumNext:  2,
			mutate: func(t *testing.T, path string, record scannedRecord) {
				t.Helper()
				file, err := os.OpenFile(path, os.O_RDWR, 0o600)
				if err != nil {
					t.Fatal(err)
				}
				var value [1]byte
				if _, err := file.ReadAt(value[:], record.payloadOff); err != nil {
					_ = file.Close()
					t.Fatal(err)
				}
				value[0] ^= 0xff
				if _, err := file.WriteAt(value[:], record.payloadOff); err != nil {
					_ = file.Close()
					t.Fatal(err)
				}
				if err := file.Sync(); err != nil {
					_ = file.Close()
					t.Fatal(err)
				}
				if err := file.Close(); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name:         "torn payload at tail",
			eventIDs:     []string{"one", "two"},
			corruptIndex: 1,
			minimumNext:  2,
			mutate: func(t *testing.T, path string, record scannedRecord) {
				t.Helper()
				if err := os.Truncate(path, record.payloadOff+int64(record.payloadLen)/2); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name:         "CRC mismatch before later allocation",
			eventIDs:     []string{"one", "two", "three"},
			corruptIndex: 1,
			minimumNext:  3,
			mutate: func(t *testing.T, path string, record scannedRecord) {
				t.Helper()
				file, err := os.OpenFile(path, os.O_RDWR, 0o600)
				if err != nil {
					t.Fatal(err)
				}
				var value [1]byte
				if _, err := file.ReadAt(value[:], record.payloadOff); err != nil {
					_ = file.Close()
					t.Fatal(err)
				}
				value[0] ^= 0xff
				if _, err := file.WriteAt(value[:], record.payloadOff); err != nil {
					_ = file.Close()
					t.Fatal(err)
				}
				if err := file.Sync(); err != nil {
					_ = file.Close()
					t.Fatal(err)
				}
				if err := file.Close(); err != nil {
					t.Fatal(err)
				}
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			dir := t.TempDir()
			opts := defaultOpts(dir)
			q, err := Open(opts)
			if err != nil {
				t.Fatalf("Open: %v", err)
			}
			for _, id := range test.eventIDs {
				if _, err := q.Append(makeEvents(id)); err != nil {
					t.Fatalf("Append %q: %v", id, err)
				}
			}
			if err := q.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}
			path := filepath.Join(dir, listWALFiles(t, dir)[0])
			scan, err := scanSegment(path)
			if err != nil {
				t.Fatalf("scan: %v", err)
			}
			test.mutate(t, path, scan.records[test.corruptIndex])
			if err := os.Remove(filepath.Join(dir, metaFileName)); err != nil {
				t.Fatal(err)
			}

			reopened, err := Open(opts)
			if err != nil {
				t.Fatalf("Open physical corruption without meta: %v", err)
			}
			t.Cleanup(func() { _ = reopened.Close() })
			stats := reopened.Stats()
			if stats.QueuedBatches != 1 || stats.QuarantinedSegments != 1 ||
				stats.NextBatchSequence <= test.minimumNext {
				t.Fatalf("physical corruption allocation floor = %+v, want next sequence > %d", stats, test.minimumNext)
			}
			nextSequence := stats.NextBatchSequence
			next, err := reopened.Append(makeEvents("after-corruption"))
			if err != nil {
				t.Fatalf("Append after physical recovery: %v", err)
			}
			if next.GetBatchSequence() != nextSequence {
				t.Fatalf("sequence after physical recovery = %d, want %d", next.GetBatchSequence(), nextSequence)
			}
		})
	}
}

func TestSemanticCorruptMaxSequenceDoesNotPoisonRecoveryFloor(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	opts := defaultOpts(dir)
	q, err := Open(opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if _, err := q.Append(makeEvents("one")); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if err := q.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	path := filepath.Join(dir, listWALFiles(t, dir)[0])
	scan, err := scanSegment(path)
	if err != nil {
		t.Fatalf("scan: %v", err)
	}
	scan.records[0].batch.BatchSequence = ^uint64(0)
	payload, err := proto.Marshal(scan.records[0].batch)
	if err != nil {
		t.Fatal(err)
	}
	framed, err := encodeRecord(payload)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, framed, 0o600); err != nil {
		t.Fatal(err)
	}

	reopened, err := Open(opts)
	if err != nil {
		t.Fatalf("Open forged MaxUint64 semantic record: %v", err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	stats := reopened.Stats()
	if stats.QueuedBatches != 0 || stats.QuarantinedSegments != 1 || stats.NextBatchSequence != 2 {
		t.Fatalf("MaxUint semantic recovery = %+v", stats)
	}
}

func TestReclaimPartialFailureCommitsAccountingProgressOnce(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	opts := defaultOpts(dir)
	opts.SegmentMaxBytes = 1
	opened, err := Open(opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	for _, id := range []string{"one", "two", "three"} {
		if _, err := opened.Append(makeEvents(id)); err != nil {
			t.Fatalf("Append %q: %v", id, err)
		}
	}
	if err := opened.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reopened, err := Open(opts)
	if err != nil {
		t.Fatalf("Reopen: %v", err)
	}
	q := reopened.(*queue)
	t.Cleanup(func() { _ = q.Close() })
	q.mu.Lock()
	initialPhysical := q.physicalBytes
	firstSize := q.segments[0].size
	removeCalls := 0
	directorySyncs := 0
	injected := errors.New("injected second remove failure")
	q.removeFile = func(path string) error {
		removeCalls++
		if removeCalls == 2 {
			return injected
		}
		return os.Remove(path)
	}
	q.syncDirectory = func(path string) error {
		directorySyncs++
		return fsyncDir(path)
	}
	q.mu.Unlock()

	if err := q.AckThrough(3); !errors.Is(err, injected) {
		t.Fatalf("AckThrough partial reclaim = %v, want injected error", err)
	}
	q.mu.Lock()
	if len(q.segments) != 2 || q.physicalBytes != initialPhysical-firstSize || directorySyncs != 1 {
		q.mu.Unlock()
		t.Fatalf(
			"partial reclaim state: segments=%d physical=%d directory syncs=%d, want 2/%d/1",
			len(q.segments), q.physicalBytes, directorySyncs, initialPhysical-firstSize,
		)
	}
	q.removeFile = os.Remove
	if err := q.reclaimLocked(); err != nil {
		q.mu.Unlock()
		t.Fatalf("retry reclaim: %v", err)
	}
	remainingSegments := len(q.segments)
	remainingPhysical := q.physicalBytes
	q.mu.Unlock()
	if remainingSegments != 0 || remainingPhysical != 0 {
		t.Fatalf("reclaim retry under/over-counted: segments=%d physical=%d", remainingSegments, remainingPhysical)
	}
}

func TestReclaimAccountingInvariantFailsClosedAfterPartialProgress(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	opts := defaultOpts(dir)
	opts.SegmentMaxBytes = 1
	opened, err := Open(opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	for _, id := range []string{"one", "two", "three"} {
		if _, err := opened.Append(makeEvents(id)); err != nil {
			t.Fatalf("Append %q: %v", id, err)
		}
	}
	if err := opened.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reopened, err := Open(opts)
	if err != nil {
		t.Fatalf("Reopen: %v", err)
	}
	q := reopened.(*queue)
	t.Cleanup(func() { _ = q.Close() })

	q.mu.Lock()
	firstPath := filepath.Join(q.dir, q.segments[0].name)
	secondPath := filepath.Join(q.dir, q.segments[1].name)
	firstSize := q.segments[0].size
	secondSize := q.segments[1].size
	thirdSize := q.segments[2].size
	// Allow the first subtraction, then make the second segment larger than
	// the remaining counter. The old saturating subtraction silently reset the
	// counter to zero and let capacity checks undercount both remaining files.
	q.physicalBytes = firstSize + secondSize - 1
	directorySyncs := 0
	q.syncDirectory = func(path string) error {
		directorySyncs++
		return fsyncDir(path)
	}
	q.mu.Unlock()

	err = q.AckThrough(3)
	if err == nil || !strings.Contains(err.Error(), "physical accounting invariant violated") {
		t.Fatalf("AckThrough accounting mismatch = %v, want invariant error", err)
	}
	if _, err := os.Stat(firstPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("first reclaimed segment still exists or stat failed: %v", err)
	}
	if _, err := os.Stat(secondPath); err != nil {
		t.Fatalf("second segment was removed past accounting barrier: %v", err)
	}

	q.mu.Lock()
	if len(q.segments) != 2 || q.physicalBytes != secondSize+thirdSize || directorySyncs != 1 {
		q.mu.Unlock()
		t.Fatalf(
			"fail-closed reclaim state: segments=%d physical=%d directory syncs=%d, want 2/%d/1",
			len(q.segments), q.physicalBytes, directorySyncs, secondSize+thirdSize,
		)
	}
	if err := q.reclaimLocked(); err != nil {
		q.mu.Unlock()
		t.Fatalf("retry reclaim after accounting repair: %v", err)
	}
	remainingSegments := len(q.segments)
	remainingPhysical := q.physicalBytes
	q.mu.Unlock()
	if remainingSegments != 0 || remainingPhysical != 0 {
		t.Fatalf("reclaim retry state: segments=%d physical=%d, want 0/0", remainingSegments, remainingPhysical)
	}
}

func TestQuarantineCapacityRefreshIsRateLimitedAndObservesCleanup(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	artifact := filepath.Join(dir, segmentName(1)+".corrupt")
	if err := os.WriteFile(artifact, make([]byte, 64), 0o600); err != nil {
		t.Fatalf("write quarantine artifact: %v", err)
	}
	opts := defaultOpts(dir)
	opts.MaxQueueBytes = 80
	opened, err := Open(opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	q := opened.(*queue)
	t.Cleanup(func() { _ = q.Close() })

	q.mu.Lock()
	if err := q.ensureRecordCapacityLocked(20); !errors.Is(err, ErrQueueFull) {
		q.mu.Unlock()
		t.Fatalf("initial capacity check = %v, want ErrQueueFull", err)
	}
	q.mu.Unlock()
	if err := os.Remove(artifact); err != nil {
		t.Fatalf("remove quarantine artifact: %v", err)
	}

	q.mu.Lock()
	// Force an unexpired refresh window without relying on a wall-clock sleep.
	q.nextQuarantineStatsRefresh = time.Now().Add(time.Hour)
	if err := q.ensureRecordCapacityLocked(20); !errors.Is(err, ErrQueueFull) {
		q.mu.Unlock()
		t.Fatalf("throttled capacity check = %v, want stale conservative ErrQueueFull", err)
	}
	if q.quarantinedBytes != 64 {
		q.mu.Unlock()
		t.Fatalf("throttled scan changed quarantine bytes to %d, want 64", q.quarantinedBytes)
	}
	q.nextQuarantineStatsRefresh = time.Time{}
	if err := q.ensureRecordCapacityLocked(20); err != nil {
		q.mu.Unlock()
		t.Fatalf("capacity check after refresh window: %v", err)
	}
	quarantinedBytes := q.quarantinedBytes
	quarantinedSegments := q.quarantined
	q.mu.Unlock()
	if quarantinedBytes != 0 || quarantinedSegments != 0 {
		t.Fatalf("operator cleanup was not observed: bytes=%d segments=%d", quarantinedBytes, quarantinedSegments)
	}
}

func TestIntervalSyncFailurePoisonsAppendUntilDurabilityRecovers(t *testing.T) {
	t.Parallel()
	opts := defaultOpts(t.TempDir())
	opts.Sync = SyncInterval
	opts.SyncInterval = 2 * time.Millisecond
	opened, err := Open(opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	q := opened.(*queue)
	syncFailure := errors.New("injected fsync failure")
	q.mu.Lock()
	q.syncFile = func(*os.File) error { return syncFailure }
	q.mu.Unlock()
	if _, err := q.Append(makeEvents("first")); err != nil {
		t.Fatalf("initial interval append: %v", err)
	}

	deadline := time.Now().Add(time.Second)
	for q.Stats().LastSyncError == "" && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if got := q.Stats().LastSyncError; !strings.Contains(got, syncFailure.Error()) {
		t.Fatalf("LastSyncError = %q, want injected failure", got)
	}
	if _, err := q.Append(makeEvents("must-not-claim-durable")); !errors.Is(err, syncFailure) {
		t.Fatalf("Append during fsync failure = %v, want injected failure", err)
	}

	q.mu.Lock()
	q.syncFile = func(file *os.File) error { return file.Sync() }
	q.mu.Unlock()
	deadline = time.Now().Add(time.Second)
	for q.Stats().LastSyncError != "" && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if got := q.Stats().LastSyncError; got != "" {
		t.Fatalf("LastSyncError did not clear after a successful sync: %q", got)
	}
	if _, err := q.Append(makeEvents("durability-restored")); err != nil {
		t.Fatalf("Append after sync recovery: %v", err)
	}
	if err := q.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestInterruptedQuarantineRecoversPrefixAndCountsAgainstCapacity(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	opts := defaultOpts(dir)
	opened, err := Open(opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	for _, id := range []string{"one", "two"} {
		if _, err := opened.Append(makeEvents(id)); err != nil {
			t.Fatalf("Append %q: %v", id, err)
		}
	}
	if err := opened.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	segments := listWALFiles(t, dir)
	if len(segments) != 1 {
		t.Fatalf("segments = %v, want one", segments)
	}
	live := filepath.Join(dir, segments[0])
	info, err := os.Stat(live)
	if err != nil {
		t.Fatal(err)
	}
	originalSize := uint64(info.Size())
	originalContents, err := os.ReadFile(live)
	if err != nil {
		t.Fatal(err)
	}
	file, err := os.OpenFile(live, os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.Write([]byte{0xde, 0xad, 0xbe}); err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	rewrite := live + ".rewrite"
	if err := os.WriteFile(rewrite, originalContents, 0o600); err != nil {
		t.Fatal(err)
	}
	rewriteFile, err := os.OpenFile(rewrite, os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	if err := rewriteFile.Sync(); err != nil {
		_ = rewriteFile.Close()
		t.Fatal(err)
	}
	if err := rewriteFile.Close(); err != nil {
		t.Fatal(err)
	}
	artifact := live + ".corrupt"
	// This is the historical rename-first crash point after the replacement was
	// prepared and synced but before it was renamed into place.
	if err := os.Rename(live, artifact); err != nil {
		t.Fatal(err)
	}

	limited := opts
	limited.MaxQueueBytes = originalSize
	recovered, err := Open(limited)
	if err != nil {
		t.Fatalf("Open interrupted quarantine: %v", err)
	}
	q := recovered.(*queue)
	stats := q.Stats()
	if stats.QueuedBatches != 2 || stats.QuarantinedSegments != 1 {
		t.Fatalf("recovered stats = %+v, want two live batches and one quarantine", stats)
	}
	if stats.OnDiskBytes != stats.PhysicalBytes+stats.QuarantinedBytes ||
		stats.OnDiskBytes <= limited.MaxQueueBytes || stats.RecoveryWarning == "" {
		t.Fatalf("quarantine storage visibility = %+v", stats)
	}
	if err := q.AckThrough(2); err != nil {
		t.Fatalf("AckThrough recovered prefix: %v", err)
	}
	if _, err := q.Append(makeEvents("blocked-by-forensics")); !errors.Is(err, ErrQueueFull) {
		t.Fatalf("Append with oversized quarantine = %v, want ErrQueueFull", err)
	}
	if err := os.Remove(artifact); err != nil {
		t.Fatalf("operator remove quarantine: %v", err)
	}
	q.mu.Lock()
	q.nextQuarantineStatsRefresh = time.Time{}
	q.mu.Unlock()
	if _, err := q.Append(makeEvents("operator-unblocked")); err != nil {
		t.Fatalf("Append after quarantine removal: %v", err)
	}
	if got := q.Stats(); got.QuarantinedSegments != 0 || got.QuarantinedBytes != 0 {
		t.Fatalf("quarantine stats after operator removal = %+v", got)
	}
	if err := q.Close(); err != nil {
		t.Fatalf("Close recovered queue: %v", err)
	}
}

func TestInterruptedLinkQuarantineCompletesWithoutDuplicateArtifact(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	opts := defaultOpts(dir)
	q, err := Open(opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	for _, id := range []string{"one", "two"} {
		if _, err := q.Append(makeEvents(id)); err != nil {
			t.Fatalf("Append %q: %v", id, err)
		}
	}
	if err := q.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	live := filepath.Join(dir, listWALFiles(t, dir)[0])
	scan, err := scanSegment(live)
	if err != nil {
		t.Fatalf("scan: %v", err)
	}
	prefixLength := scan.records[1].payloadOff - recordHeaderSize
	rewrite := live + ".rewrite"
	if err := writeSyncedPrefix(live, rewrite, prefixLength); err != nil {
		t.Fatalf("prepare rewrite: %v", err)
	}
	artifact := live + ".corrupt"
	if err := os.Link(live, artifact); err != nil {
		t.Fatalf("publish forensic hard link: %v", err)
	}
	if err := fsyncDir(dir); err != nil {
		t.Fatalf("sync transaction state: %v", err)
	}

	reopened, err := Open(opts)
	if err != nil {
		t.Fatalf("Open interrupted link transaction: %v", err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	stats := reopened.Stats()
	if stats.QueuedBatches != 1 || stats.QuarantinedSegments != 1 {
		t.Fatalf("completed link transaction stats = %+v", stats)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	artifacts := 0
	for _, entry := range entries {
		if strings.Contains(entry.Name(), corruptSuffix) {
			artifacts++
		}
		if strings.HasSuffix(entry.Name(), ".rewrite") {
			t.Fatalf("prepared rewrite remained after recovery: %s", entry.Name())
		}
	}
	if artifacts != 1 {
		t.Fatalf("quarantine artifacts = %d, want exactly one original", artifacts)
	}
}
