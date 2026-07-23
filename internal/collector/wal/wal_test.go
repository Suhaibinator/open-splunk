package wal

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"google.golang.org/protobuf/proto"
)

// makeEvents builds a slice of minimal LogEvents from the given event IDs.
func makeEvents(ids ...string) []*opensplunkv1.LogEvent {
	evs := make([]*opensplunkv1.LogEvent, len(ids))
	for i, id := range ids {
		evs[i] = &opensplunkv1.LogEvent{EventId: id, IndexName: "main"}
	}
	return evs
}

// defaultOpts returns Options for a durable, per-record-fsynced queue in dir.
func defaultOpts(dir string) Options {
	return Options{
		Dir:             dir,
		SegmentMaxBytes: 1 << 20,
		Sync:            SyncAlways,
		CollectorID:     "collector-test",
		ProtocolMajor:   1,
		ProtocolMinor:   0,
	}
}

// listWALFiles returns the live segment-*.wal file names in dir.
func listWALFiles(t *testing.T, dir string) []string {
	t.Helper()
	names, err := listSegments(dir)
	if err != nil {
		t.Fatalf("listSegments: %v", err)
	}
	return names
}

func TestAppendReadAckRoundTrip(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	q, err := Open(defaultOpts(dir))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = q.Close() })

	events := makeEvents("a", "bb", "ccc")
	appended, err := q.Append(events)
	if err != nil {
		t.Fatalf("Append: %v", err)
	}
	if appended.GetBatchSequence() != 1 {
		t.Fatalf("first batch sequence = %d, want 1", appended.GetBatchSequence())
	}
	if appended.GetBatchId() == "" {
		t.Fatalf("batch_id is empty")
	}
	if appended.GetCollectorId() != "collector-test" {
		t.Fatalf("collector_id = %q, want collector-test", appended.GetCollectorId())
	}
	wantDigest := ComputeEventIDsDigest(events)
	if got := appended.GetEventIdsSha256(); hex.EncodeToString(got) != hex.EncodeToString(wantDigest) {
		t.Fatalf("digest mismatch: got %x want %x", got, wantDigest)
	}
	if appended.GetUncompressedSizeBytes() != uncompressedEventBytes(events) {
		t.Fatalf("uncompressed size = %d, want %d", appended.GetUncompressedSizeBytes(), uncompressedEventBytes(events))
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	got, err := q.NextBatch(ctx)
	if err != nil {
		t.Fatalf("NextBatch: %v", err)
	}
	if got.GetBatchId() != appended.GetBatchId() || got.GetBatchSequence() != 1 {
		t.Fatalf("NextBatch returned batch %s/%d, want %s/1", got.GetBatchId(), got.GetBatchSequence(), appended.GetBatchId())
	}
	if len(got.GetEvents()) != 3 || got.GetEvents()[2].GetEventId() != "ccc" {
		t.Fatalf("NextBatch events mismatch: %+v", got.GetEvents())
	}

	if st := q.Stats(); st.QueuedBatches != 1 || st.QueuedEvents != 3 {
		t.Fatalf("stats before ack = %+v, want 1 batch / 3 events", st)
	}

	if err := q.Ack(1); err != nil {
		t.Fatalf("Ack: %v", err)
	}
	st := q.Stats()
	if st.QueuedBatches != 0 || st.QueuedEvents != 0 || st.QueuedBytes != 0 {
		t.Fatalf("stats after ack = %+v, want empty", st)
	}
	if st.LastAckedBatchSequence != 1 || st.NextBatchSequence != 2 {
		t.Fatalf("counters after ack = %+v", st)
	}
}

func TestReopenResumesUnacked(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	q, err := Open(defaultOpts(dir))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	for i := 0; i < 3; i++ {
		if _, err := q.Append(makeEvents("e")); err != nil {
			t.Fatalf("Append %d: %v", i, err)
		}
	}
	// Ack only the first batch, then simulate a restart.
	if err := q.Ack(1); err != nil {
		t.Fatalf("Ack: %v", err)
	}
	if err := q.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	q2, err := Open(defaultOpts(dir))
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	t.Cleanup(func() { _ = q2.Close() })

	if st := q2.Stats(); st.QueuedBatches != 2 || st.LastAckedBatchSequence != 1 || st.NextBatchSequence != 4 {
		t.Fatalf("stats after reopen = %+v, want 2 unacked / acked 1 / next 4", st)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	first, err := q2.NextBatch(ctx)
	if err != nil {
		t.Fatalf("NextBatch: %v", err)
	}
	if first.GetBatchSequence() != 2 {
		t.Fatalf("resumed at sequence %d, want lowest unacked 2", first.GetBatchSequence())
	}
	second, err := q2.NextBatch(ctx)
	if err != nil {
		t.Fatalf("NextBatch: %v", err)
	}
	if second.GetBatchSequence() != 3 {
		t.Fatalf("second resumed sequence %d, want 3", second.GetBatchSequence())
	}
}

func TestSequenceNeverReusedAfterCrash(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	q, err := Open(defaultOpts(dir))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if _, err := q.Append(makeEvents("first")); err != nil {
		t.Fatalf("Append 1: %v", err)
	}

	// Simulate a crash after the sequence bump is durable but before the record
	// is written: sequence 2 must be burned, never reused.
	q.(*queue).crashAfterMetaWrite = true
	if _, err := q.Append(makeEvents("burned")); err != errSimulatedCrash {
		t.Fatalf("Append with crash hook = %v, want errSimulatedCrash", err)
	}
	if err := q.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	q2, err := Open(defaultOpts(dir))
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	t.Cleanup(func() { _ = q2.Close() })

	next, err := q2.Append(makeEvents("after"))
	if err != nil {
		t.Fatalf("Append after reopen: %v", err)
	}
	if next.GetBatchSequence() != 3 {
		t.Fatalf("sequence after crash = %d, want 3 (2 burned, never reused)", next.GetBatchSequence())
	}

	// Confirm only sequences 1 and 3 exist on disk; 2 is a gap.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	seqs := map[uint64]bool{}
	for i := 0; i < 2; i++ {
		b, err := q2.NextBatch(ctx)
		if err != nil {
			t.Fatalf("NextBatch: %v", err)
		}
		seqs[b.GetBatchSequence()] = true
	}
	if !seqs[1] || !seqs[3] || seqs[2] {
		t.Fatalf("delivered sequences = %v, want {1,3} without 2", seqs)
	}
}

func TestCorruptCRCQuarantine(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	q, err := Open(defaultOpts(dir))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	for i := 0; i < 5; i++ {
		if _, err := q.Append(makeEvents("evt")); err != nil {
			t.Fatalf("Append %d: %v", i, err)
		}
	}
	if err := q.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	segFiles := listWALFiles(t, dir)
	if len(segFiles) != 1 {
		t.Fatalf("expected 1 segment, got %v", segFiles)
	}
	segPath := filepath.Join(dir, segFiles[0])

	// Locate record index 2's payload and flip a byte inside it to break its CRC.
	res, err := scanSegment(segPath)
	if err != nil {
		t.Fatalf("scanSegment: %v", err)
	}
	if len(res.records) != 5 {
		t.Fatalf("expected 5 records, got %d", len(res.records))
	}
	corruptAt := res.records[2].payloadOff
	data, err := os.ReadFile(segPath)
	if err != nil {
		t.Fatalf("read segment: %v", err)
	}
	data[corruptAt] ^= 0xff
	if err := os.WriteFile(segPath, data, 0o644); err != nil {
		t.Fatalf("write corrupted segment: %v", err)
	}

	q2, err := Open(defaultOpts(dir))
	if err != nil {
		t.Fatalf("reopen after corruption: %v", err)
	}
	t.Cleanup(func() { _ = q2.Close() })

	st := q2.Stats()
	if st.QuarantinedSegments != 1 {
		t.Fatalf("QuarantinedSegments = %d, want 1", st.QuarantinedSegments)
	}
	// Records 0 and 1 (sequences 1 and 2) precede the corruption and must survive.
	if st.QueuedBatches != 2 {
		t.Fatalf("QueuedBatches after quarantine = %d, want 2 (good prefix)", st.QueuedBatches)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for want := uint64(1); want <= 2; want++ {
		b, err := q2.NextBatch(ctx)
		if err != nil {
			t.Fatalf("NextBatch: %v", err)
		}
		if b.GetBatchSequence() != want {
			t.Fatalf("surviving sequence = %d, want %d", b.GetBatchSequence(), want)
		}
	}

	// A quarantine artifact must exist and the good prefix must remain live.
	entries, _ := os.ReadDir(dir)
	var hasCorrupt bool
	for _, e := range entries {
		if strings.Contains(e.Name(), corruptSuffix) {
			hasCorrupt = true
		}
	}
	if !hasCorrupt {
		t.Fatalf("expected a .wal.corrupt quarantine file in %v", entries)
	}
}

func TestRecoveryQuarantinesEverySegmentAfterCorruptGap(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	opts := defaultOpts(dir)
	// Force one record per segment so corruption in sequence 2 leaves later,
	// individually valid segment files for sequences 3 and 4.
	opts.SegmentMaxBytes = 1
	q, err := Open(opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	for sequence := 1; sequence <= 4; sequence++ {
		if _, err := q.Append(makeEvents("event-" + itoaForTest(uint64(sequence)))); err != nil {
			t.Fatalf("Append %d: %v", sequence, err)
		}
	}
	if err := q.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	segments := listWALFiles(t, dir)
	if len(segments) != 4 {
		t.Fatalf("live segments = %v, want one per sequence", segments)
	}
	secondPath := filepath.Join(dir, segments[1])
	second, err := scanSegment(secondPath)
	if err != nil {
		t.Fatalf("scan second segment: %v", err)
	}
	if len(second.records) != 1 {
		t.Fatalf("second segment records = %d, want 1", len(second.records))
	}
	data, err := os.ReadFile(secondPath)
	if err != nil {
		t.Fatalf("read second segment: %v", err)
	}
	data[second.records[0].payloadOff] ^= 0xff
	if err := os.WriteFile(secondPath, data, 0o600); err != nil {
		t.Fatalf("corrupt second segment: %v", err)
	}

	reopened, err := Open(opts)
	if err != nil {
		t.Fatalf("reopen after corruption: %v", err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	stats := reopened.Stats()
	if stats.QuarantinedSegments != 3 {
		t.Fatalf("QuarantinedSegments = %d, want corrupt segment and both successors", stats.QuarantinedSegments)
	}
	if stats.QueuedBatches != 1 {
		t.Fatalf("QueuedBatches = %d, want only sequence 1 before the corrupt gap", stats.QueuedBatches)
	}
	if live := listWALFiles(t, dir); len(live) != 1 || live[0] != segments[0] {
		t.Fatalf("live segments = %v, want only %s", live, segments[0])
	}
	if _, err := reopened.PrepareAckThrough(4); !errors.Is(err, ErrInvalidAck) {
		t.Fatalf("PrepareAckThrough(4) = %v, want ErrInvalidAck after quarantine barrier", err)
	}
	appended, err := reopened.Append(makeEvents("reread-after-gap"))
	if err != nil {
		t.Fatalf("Append after quarantine: %v", err)
	}
	if appended.GetBatchSequence() != 5 {
		t.Fatalf("sequence after quarantine = %d, want burned high-water sequence 5", appended.GetBatchSequence())
	}
}

func TestRecoveryCrashCannotForgetCorruptGap(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	opts := defaultOpts(dir)
	opts.SegmentMaxBytes = 1
	q, err := Open(opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	for sequence := 1; sequence <= 4; sequence++ {
		if _, err := q.Append(makeEvents("event-" + itoaForTest(uint64(sequence)))); err != nil {
			t.Fatalf("Append %d: %v", sequence, err)
		}
	}
	if err := q.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	segments := listWALFiles(t, dir)
	secondPath := filepath.Join(dir, segments[1])
	second, err := scanSegment(secondPath)
	if err != nil {
		t.Fatalf("scan second segment: %v", err)
	}
	data, err := os.ReadFile(secondPath)
	if err != nil {
		t.Fatalf("read second segment: %v", err)
	}
	data[second.records[0].payloadOff] ^= 0xff
	if err := os.WriteFile(secondPath, data, 0o600); err != nil {
		t.Fatalf("corrupt second segment: %v", err)
	}

	meta, ok, err := readMeta(dir)
	if err != nil || !ok {
		t.Fatalf("readMeta: ok=%v err=%v", ok, err)
	}
	crashing := &queue{
		opts: opts, dir: dir,
		nextSeq: meta.NextBatchSequence, lastAcked: meta.LastAckedBatchSequence,
		terminal: make(map[uint64]struct{}), crashAfterSuccessorQuarantine: true,
	}
	if err := crashing.recover(); !errors.Is(err, errSimulatedRecoveryCrash) {
		t.Fatalf("recover with crash hook = %v, want errSimulatedRecoveryCrash", err)
	}
	// Successors are already durable quarantine artifacts, while the triggering
	// corrupt segment intentionally remains live so the next Open rediscovers it.
	if live := listWALFiles(t, dir); len(live) != 2 || live[0] != segments[0] || live[1] != segments[1] {
		t.Fatalf("live segments at crash point = %v, want intact first plus corrupt trigger", live)
	}

	reopened, err := Open(opts)
	if err != nil {
		t.Fatalf("Open after simulated recovery crash: %v", err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	stats := reopened.Stats()
	if stats.QueuedBatches != 1 {
		t.Fatalf("QueuedBatches after recovery retry = %d, want only sequence 1", stats.QueuedBatches)
	}
	if live := listWALFiles(t, dir); len(live) != 1 || live[0] != segments[0] {
		t.Fatalf("live segments after recovery retry = %v, want only %s", live, segments[0])
	}
	if _, err := reopened.PrepareAckThrough(4); !errors.Is(err, ErrInvalidAck) {
		t.Fatalf("PrepareAckThrough(4) = %v, want ErrInvalidAck after recovery retry", err)
	}
}

func itoaForTest(value uint64) string {
	if value == 0 {
		return "0"
	}
	var digits [20]byte
	index := len(digits)
	for value > 0 {
		index--
		digits[index] = byte('0' + value%10)
		value /= 10
	}
	return string(digits[index:])
}

func TestTruncatedTail(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	q, err := Open(defaultOpts(dir))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	for i := 0; i < 4; i++ {
		if _, err := q.Append(makeEvents("evt")); err != nil {
			t.Fatalf("Append %d: %v", i, err)
		}
	}
	if err := q.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	segPath := filepath.Join(dir, listWALFiles(t, dir)[0])
	res, err := scanSegment(segPath)
	if err != nil {
		t.Fatalf("scanSegment: %v", err)
	}
	// Truncate into the middle of the last record's payload.
	last := res.records[len(res.records)-1]
	truncateTo := last.payloadOff + int64(last.payloadLen)/2
	if err := os.Truncate(segPath, truncateTo); err != nil {
		t.Fatalf("truncate: %v", err)
	}

	q2, err := Open(defaultOpts(dir))
	if err != nil {
		t.Fatalf("reopen after truncation: %v", err)
	}
	t.Cleanup(func() { _ = q2.Close() })

	st := q2.Stats()
	if st.QuarantinedSegments != 1 {
		t.Fatalf("QuarantinedSegments = %d, want 1", st.QuarantinedSegments)
	}
	if st.QueuedBatches != 3 {
		t.Fatalf("QueuedBatches after truncation = %d, want 3 intact records", st.QueuedBatches)
	}
}

func TestErrQueueFullThenAckFreesSpace(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	opts := defaultOpts(dir)
	opts.MaxQueueBytes = 4096
	q, err := Open(opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = q.Close() })

	// Fill the queue until it reports full.
	var lastSeq uint64
	appended := 0
	for {
		b, err := q.Append(makeEvents("payload-event"))
		if err == ErrQueueFull {
			break
		}
		if err != nil {
			t.Fatalf("Append: %v", err)
		}
		lastSeq = b.GetBatchSequence()
		appended++
		if appended > 100000 {
			t.Fatalf("queue never reported full")
		}
	}
	if appended == 0 {
		t.Fatalf("queue reported full before accepting any batch")
	}

	// Acking everything must free the space so the next append succeeds.
	if err := q.AckThrough(lastSeq); err != nil {
		t.Fatalf("Ack: %v", err)
	}
	if st := q.Stats(); st.QueuedBytes != 0 {
		t.Fatalf("QueuedBytes after full ack = %d, want 0", st.QueuedBytes)
	}
	if _, err := q.Append(makeEvents("payload-event")); err != nil {
		t.Fatalf("Append after freeing space = %v, want success", err)
	}
}

func TestAckOutOfOrderDoesNotDeleteRetryablePrefix(t *testing.T) {
	t.Parallel()
	q, err := Open(defaultOpts(t.TempDir()))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = q.Close() })
	for i := 0; i < 3; i++ {
		if _, err := q.Append(makeEvents("event")); err != nil {
			t.Fatalf("Append %d: %v", i, err)
		}
	}

	if err := q.Ack(2); err != nil {
		t.Fatalf("Ack(2): %v", err)
	}
	if got := q.Stats(); got.LastAckedBatchSequence != 0 || got.QueuedBatches != 3 {
		t.Fatalf("out-of-order ack advanced/deleted prefix: %+v", got)
	}
	if err := q.Ack(1); err != nil {
		t.Fatalf("Ack(1): %v", err)
	}
	if got := q.Stats(); got.LastAckedBatchSequence != 2 || got.QueuedBatches != 1 {
		t.Fatalf("contiguous terminal prefix not advanced: %+v", got)
	}
}

func TestPrepareAckReportsOnlyNewlyContiguousBatchesWithoutMutation(t *testing.T) {
	t.Parallel()
	q, err := Open(defaultOpts(t.TempDir()))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = q.Close() })
	for i := 1; i <= 3; i++ {
		if _, err := q.Append(makeEvents("event-" + string(rune('0'+i)))); err != nil {
			t.Fatalf("Append %d: %v", i, err)
		}
	}

	prepared, err := q.PrepareAck(2)
	if err != nil {
		t.Fatalf("PrepareAck(2): %v", err)
	}
	if prepared.BatchCount != 0 || prepared.ThroughBatchSequence != 0 {
		t.Fatalf("PrepareAck(2) = %+v, want empty preview before gap closes", prepared)
	}
	if got := q.Stats(); got.LastAckedBatchSequence != 0 || got.QueuedBatches != 3 {
		t.Fatalf("PrepareAck mutated queue: %+v", got)
	}
	if err := q.Ack(2); err != nil {
		t.Fatalf("Ack(2): %v", err)
	}

	prepared, err = q.PrepareAck(1)
	if err != nil {
		t.Fatalf("PrepareAck(1): %v", err)
	}
	if prepared.BatchCount != 2 || prepared.ThroughBatchSequence != 2 {
		t.Fatalf("PrepareAck(1) = %+v, want two batches through sequence 2", prepared)
	}
	if got := q.Stats(); got.LastAckedBatchSequence != 0 || got.QueuedBatches != 3 {
		t.Fatalf("PrepareAck mutated queue after gap closed hypothetically: %+v", got)
	}
	if err := q.Ack(1); err != nil {
		t.Fatalf("Ack(1): %v", err)
	}
	if got := q.Stats(); got.LastAckedBatchSequence != 2 || got.QueuedBatches != 1 {
		t.Fatalf("Ack(1) did not commit prepared prefix: %+v", got)
	}
}

func TestPrepareAckThroughReconstructsBatchesAfterRestart(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	q, err := Open(defaultOpts(dir))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	for i := 1; i <= 3; i++ {
		if _, err := q.Append(makeEvents("event-" + string(rune('0'+i)))); err != nil {
			t.Fatalf("Append %d: %v", i, err)
		}
	}
	if err := q.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reopened, err := Open(defaultOpts(dir))
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	prepared, err := reopened.PrepareAckThrough(2)
	if err != nil {
		t.Fatalf("PrepareAckThrough(2): %v", err)
	}
	if prepared.BatchCount != 2 || prepared.ThroughBatchSequence != 2 {
		t.Fatalf("prepared preview after restart = %+v, want two batches through sequence 2", prepared)
	}
	if got := reopened.Stats(); got.LastAckedBatchSequence != 0 || got.QueuedBatches != 3 {
		t.Fatalf("PrepareAckThrough mutated reopened queue: %+v", got)
	}
}

func TestPrepareAckCachesAndCoalescesSourceMarksAcrossRecovery(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	q, err := Open(defaultOpts(dir))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	identity := "dev=7;ino=9;gen=3;fp=" + strings.Repeat("ab", 32)
	for sequence, end := range []uint64{100, 200, 350} {
		event := makeEvents("event")[0]
		event.Raw = []byte(strings.Repeat("x", 64<<10))
		event.Origin = &opensplunkv1.EventOrigin{
			FileIdentity: proto.String(identity), SourcePath: proto.String("/logs/app.log"),
			EndOffset: proto.Uint64(end), LineNumber: proto.Uint64(uint64(sequence + 1)),
			FileFingerprintLength: proto.Uint32(1024),
		}
		if _, err := q.Append([]*opensplunkv1.LogEvent{event}); err != nil {
			t.Fatalf("Append %d: %v", sequence+1, err)
		}
	}
	before, err := q.PrepareAckThrough(3)
	if err != nil {
		t.Fatalf("PrepareAckThrough before restart: %v", err)
	}
	if before.BatchCount != 3 || before.ThroughBatchSequence != 3 || len(before.Marks) != 1 {
		t.Fatalf("preview before restart = %+v, want 3 batches and one source mark", before)
	}
	mark := before.Marks[0]
	if mark.FileIdentity != identity || mark.SourcePath != "/logs/app.log" || mark.EndOffset != 350 ||
		mark.LineNumber != 3 || mark.FingerprintLength != 1024 ||
		!mark.HasSourcePath || !mark.HasEndOffset || !mark.HasFingerprintLength {
		t.Fatalf("coalesced mark before restart = %+v", mark)
	}
	if err := q.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reopened, err := Open(defaultOpts(dir))
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	after, err := reopened.PrepareAckThrough(3)
	if err != nil {
		t.Fatalf("PrepareAckThrough after restart: %v", err)
	}
	if before.Marks[0] != after.Marks[0] || before.BatchCount != after.BatchCount ||
		before.ThroughBatchSequence != after.ThroughBatchSequence {
		t.Fatalf("recovered preview = %+v, want %+v", after, before)
	}
}

func TestCheckpointMarksKeepFingerprintConflictSticky(t *testing.T) {
	t.Parallel()
	identity := "dev=1;ino=2;gen=1;fp=" + strings.Repeat("cd", 32)
	events := make([]*opensplunkv1.LogEvent, 0, 3)
	for index, length := range []uint32{10, 20, 20} {
		event := makeEvents("event")[0]
		event.Origin = &opensplunkv1.EventOrigin{
			FileIdentity: proto.String(identity), SourcePath: proto.String("/x.log"),
			EndOffset: proto.Uint64(uint64(index + 1)), FileFingerprintLength: proto.Uint32(length),
		}
		events = append(events, event)
	}
	marks := checkpointMarksForBatch(1, events)
	if len(marks) != 1 || !marks[0].ConflictingMetadata {
		t.Fatalf("marks = %+v, want one sticky metadata conflict", marks)
	}
}

func TestAckClearsRemovedDescriptorReferences(t *testing.T) {
	t.Parallel()
	opened, err := Open(defaultOpts(t.TempDir()))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = opened.Close() })
	q := opened.(*queue)
	identity := "dev=1;ino=2;gen=1;fp=" + strings.Repeat("ef", 32)
	for sequence := 1; sequence <= 3; sequence++ {
		event := makeEvents("event")[0]
		event.Origin = &opensplunkv1.EventOrigin{
			FileIdentity: proto.String(identity), SourcePath: proto.String("/x.log"),
			EndOffset: proto.Uint64(uint64(sequence)), FileFingerprintLength: proto.Uint32(64),
		}
		if _, err := q.Append([]*opensplunkv1.LogEvent{event}); err != nil {
			t.Fatalf("Append %d: %v", sequence, err)
		}
	}
	if err := q.AckThrough(2); err != nil {
		t.Fatalf("AckThrough: %v", err)
	}

	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.unacked) != 1 || q.unacked[0].seq != 3 {
		t.Fatalf("unacked = %+v, want only sequence 3", q.unacked)
	}
	backing := q.unacked[:cap(q.unacked)]
	for index := len(q.unacked); index < len(backing); index++ {
		if backing[index].segName != "" || backing[index].sourceMarks != nil {
			t.Fatalf("removed descriptor %d retained references: %+v", index, backing[index])
		}
	}
}

func TestAckRejectsUnknownFutureSequence(t *testing.T) {
	t.Parallel()
	q, err := Open(defaultOpts(t.TempDir()))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = q.Close() })
	if _, err := q.Append(makeEvents("event")); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if err := q.Ack(999); !errors.Is(err, ErrInvalidAck) {
		t.Fatalf("Ack(future) = %v, want ErrInvalidAck", err)
	}
	if err := q.AckThrough(999); !errors.Is(err, ErrInvalidAck) {
		t.Fatalf("AckThrough(future) = %v, want ErrInvalidAck", err)
	}
	if got := q.Stats(); got.LastAckedBatchSequence != 0 || got.QueuedBatches != 1 {
		t.Fatalf("future ack mutated queue: %+v", got)
	}
}

func TestSegmentReclamationRemovesFiles(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	opts := defaultOpts(dir)
	opts.SegmentMaxBytes = 1 // force a new segment per record
	q, err := Open(opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	var lastSeq uint64
	for i := 0; i < 4; i++ {
		b, err := q.Append(makeEvents("evt"))
		if err != nil {
			t.Fatalf("Append %d: %v", i, err)
		}
		lastSeq = b.GetBatchSequence()
	}
	before := listWALFiles(t, dir)
	if len(before) < 3 {
		t.Fatalf("expected multiple segments, got %v", before)
	}

	if err := q.AckThrough(lastSeq); err != nil {
		t.Fatalf("Ack: %v", err)
	}
	// All sealed segments are reclaimed; only the active (unsealed) one remains.
	afterAck := listWALFiles(t, dir)
	if len(afterAck) != 1 {
		t.Fatalf("segments after ack = %v, want 1 (active only)", afterAck)
	}
	if err := q.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// On reopen the now-sealed final segment is fully acked and reclaimed too.
	q2, err := Open(opts)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	t.Cleanup(func() { _ = q2.Close() })
	if after := listWALFiles(t, dir); len(after) != 0 {
		t.Fatalf("segments after reopen = %v, want 0", after)
	}
	if st := q2.Stats(); st.QueuedBatches != 0 || st.LastAckedBatchSequence != lastSeq {
		t.Fatalf("stats after reclamation = %+v", st)
	}
}

func TestComputeEventIDsDigestGolden(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		ids  []string
		want string
	}{
		{
			name: "three ascending ids",
			ids:  []string{"a", "bb", "ccc"},
			// SHA-256 over 00000001 'a' 00000002 'bb' 00000003 'ccc'.
			want: "ebe59d71a877399c9c420666e7037b1380cd69dbccacb3cf9b97b468a7813ed6",
		},
		{
			name: "empty",
			ids:  nil,
			want: "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
		},
	}
	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := hex.EncodeToString(ComputeEventIDsDigest(makeEvents(tc.ids...)))
			if got != tc.want {
				t.Fatalf("digest = %s, want %s", got, tc.want)
			}
		})
	}

	// Independent verification of the length-prefix framing for the first vector.
	var manual []byte
	for _, id := range []string{"a", "bb", "ccc"} {
		var l [4]byte
		binary.BigEndian.PutUint32(l[:], uint32(len(id)))
		manual = append(manual, l[:]...)
		manual = append(manual, id...)
	}
	if len(manual) != 4+1+4+2+4+3 {
		t.Fatalf("framed length = %d, want 18", len(manual))
	}
}

func TestConcurrentAppendConsumeStats(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	opts := defaultOpts(dir)
	opts.SegmentMaxBytes = 512 // exercise rotation and reclamation under load
	q, err := Open(opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = q.Close() })

	const total = 500
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(3)

	// Appender.
	go func() {
		defer wg.Done()
		for i := 0; i < total; i++ {
			if _, err := q.Append(makeEvents("evt")); err != nil {
				t.Errorf("Append: %v", err)
				return
			}
		}
	}()

	// Consumer: reads and acks every batch in order.
	go func() {
		defer wg.Done()
		for got := 0; got < total; {
			b, err := q.NextBatch(ctx)
			if err != nil {
				t.Errorf("NextBatch: %v", err)
				return
			}
			got++
			if err := q.Ack(b.GetBatchSequence()); err != nil {
				t.Errorf("Ack: %v", err)
				return
			}
		}
	}()

	// Stats caller: concurrently snapshots the queue.
	go func() {
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			default:
			}
			st := q.Stats()
			if st.LastAckedBatchSequence >= total {
				return
			}
		}
	}()

	wg.Wait()

	st := q.Stats()
	if st.QueuedBatches != 0 || st.QueuedEvents != 0 {
		t.Fatalf("stats after draining = %+v, want empty", st)
	}
	if st.LastAckedBatchSequence != total {
		t.Fatalf("last acked = %d, want %d", st.LastAckedBatchSequence, total)
	}
}

// BenchmarkAppend1KiBEvents measures durable (SyncAlways, fsync-per-append)
// throughput for realistically sized ~1KiB events. Each Append seals a batch of
// eventsPerBatch events so the fsync cost is amortized the way the daemon
// amortizes it, and the benchmark reports an events/s metric for a 1k events/s
// sanity check.
func BenchmarkAppend1KiBEvents(b *testing.B) {
	const eventsPerBatch = 128

	dir := b.TempDir()
	opts := defaultOpts(dir)
	opts.SegmentMaxBytes = 32 << 20
	q, err := Open(opts)
	if err != nil {
		b.Fatalf("Open: %v", err)
	}
	defer func() { _ = q.Close() }()

	payload := make([]byte, 1024)
	for i := range payload {
		payload[i] = byte('A' + i%26)
	}
	events := make([]*opensplunkv1.LogEvent, eventsPerBatch)
	for i := range events {
		events[i] = &opensplunkv1.LogEvent{EventId: "bench-event", IndexName: "main", Raw: payload}
	}
	if got := proto.Size(events[0]); got < 1024 {
		b.Fatalf("event size %d < 1KiB", got)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		batch, err := q.Append(events)
		if err != nil {
			b.Fatalf("Append: %v", err)
		}
		// Keep the queue bounded so the benchmark measures steady-state append.
		if i%16 == 0 {
			if err := q.Ack(batch.GetBatchSequence()); err != nil {
				b.Fatalf("Ack: %v", err)
			}
		}
	}
	b.StopTimer()
	b.ReportMetric(float64(b.N*eventsPerBatch)/b.Elapsed().Seconds(), "events/s")
}

func TestRewindRedeliversUnackedBatches(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	q, err := Open(defaultOpts(dir))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = q.Close() })

	for _, id := range []string{"a", "b", "c"} {
		if _, err := q.Append(makeEvents(id)); err != nil {
			t.Fatalf("Append(%s): %v", id, err)
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for want := uint64(1); want <= 3; want++ {
		got, err := q.NextBatch(ctx)
		if err != nil {
			t.Fatalf("NextBatch: %v", err)
		}
		if got.GetBatchSequence() != want {
			t.Fatalf("NextBatch sequence = %d, want %d", got.GetBatchSequence(), want)
		}
	}

	// All three batches were handed out but never acked (a dead connection).
	// Rewind must re-yield them all, starting from the lowest unacked sequence.
	q.Rewind()
	got, err := q.NextBatch(ctx)
	if err != nil {
		t.Fatalf("NextBatch after Rewind: %v", err)
	}
	if got.GetBatchSequence() != 1 {
		t.Fatalf("first redelivered sequence = %d, want 1", got.GetBatchSequence())
	}

	// Acking the redelivered batch and rewinding again starts at the next
	// unacked sequence, not at the acked one.
	if err := q.Ack(1); err != nil {
		t.Fatalf("Ack: %v", err)
	}
	q.Rewind()
	got, err = q.NextBatch(ctx)
	if err != nil {
		t.Fatalf("NextBatch after Ack+Rewind: %v", err)
	}
	if got.GetBatchSequence() != 2 {
		t.Fatalf("redelivered sequence after ack = %d, want 2", got.GetBatchSequence())
	}

	// A blocked NextBatch caller is woken by Rewind rather than waiting for a
	// fresh Append.
	if _, err := q.NextBatch(ctx); err != nil {
		t.Fatalf("NextBatch(3): %v", err)
	}
	blockedCtx, blockedCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer blockedCancel()
	done := make(chan error, 1)
	go func() {
		b, err := q.NextBatch(blockedCtx)
		if err == nil && b.GetBatchSequence() != 2 {
			err = errAssertSequence
		}
		done <- err
	}()
	time.Sleep(20 * time.Millisecond) // let the goroutine block in NextBatch
	q.Rewind()
	if err := <-done; err != nil {
		t.Fatalf("blocked NextBatch after Rewind: %v", err)
	}
}

// errAssertSequence reports an unexpected redelivered sequence in the blocked
// NextBatch assertion above.
var errAssertSequence = errors.New("blocked NextBatch returned unexpected sequence")
