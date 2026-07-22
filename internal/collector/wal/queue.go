package wal

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// errSimulatedCrash is returned from Append when the crashAfterMetaWrite test
// hook fires, after the sequence has been durably burned in meta but before the
// batch record is written. Production code never sets the hook.
var errSimulatedCrash = errors.New("collector/wal: simulated crash after meta write")

// batchDesc locates one unacked batch record on disk and caches the cheap
// bookkeeping the queue needs without holding the marshaled batch in memory.
type batchDesc struct {
	seq        uint64
	segName    string
	payloadOff int64
	payloadLen uint32
	crc        uint32
	eventCount uint64
	sizeOnDisk uint64 // recordHeaderSize + payloadLen
	createdAt  time.Time
}

// segInfo tracks a segment file's sequence span and seal state for reclamation.
type segInfo struct {
	name     string
	firstSeq uint64
	lastSeq  uint64
	sealed   bool
}

// queue is the concrete Queue. A single mutex guards all mutable state; it is
// safe for one appender, one consumer, and concurrent Stats callers.
type queue struct {
	opts Options
	dir  string

	mu        sync.Mutex
	closed    bool
	nextSeq   uint64
	lastAcked uint64

	unacked      []batchDesc
	deliverIdx   int
	liveBytes    uint64
	queuedEvents uint64

	segments    []*segInfo
	activeSeg   *segInfo
	active      *os.File
	activeSize  int64
	activeDirty bool

	quarantined uint64

	notify   chan struct{}
	closedCh chan struct{}

	syncStop chan struct{}
	syncDone chan struct{}

	// crashAfterMetaWrite, when set by a test, makes Append return
	// errSimulatedCrash immediately after the sequence bump is durable and before
	// the record is written, modeling a crash at that instant. Never set in
	// production.
	crashAfterMetaWrite bool
}

// openQueue implements Open: it creates or recovers the on-disk state under
// opts.Dir, quarantining corrupt tails, and returns a ready queue.
func openQueue(opts Options) (Queue, error) {
	if opts.Dir == "" {
		return nil, errors.New("collector/wal: Options.Dir is required")
	}
	if err := os.MkdirAll(opts.Dir, 0o755); err != nil {
		return nil, fmt.Errorf("collector/wal: create dir: %w", err)
	}

	q := &queue{
		opts:     opts,
		dir:      opts.Dir,
		notify:   make(chan struct{}, 1),
		closedCh: make(chan struct{}),
	}

	m, ok, err := readMeta(opts.Dir)
	if err != nil {
		return nil, err
	}
	if !ok {
		m = walMeta{FormatVersion: currentFormatVersion, NextBatchSequence: 1, LastAckedBatchSequence: 0}
		if err := writeMeta(opts.Dir, m); err != nil {
			return nil, err
		}
	}
	q.nextSeq = m.NextBatchSequence
	q.lastAcked = m.LastAckedBatchSequence

	if err := q.recover(); err != nil {
		return nil, err
	}

	// Reclaim any segment that was already fully acked before the last crash.
	if err := q.reclaimLocked(); err != nil {
		return nil, err
	}

	if opts.Sync == SyncInterval && opts.SyncInterval > 0 {
		q.syncStop = make(chan struct{})
		q.syncDone = make(chan struct{})
		go q.syncLoop(opts.SyncInterval)
	}

	return q, nil
}

// recover scans existing segments in ascending order, quarantines corrupt
// tails, and rebuilds the unacked index and segment list.
func (q *queue) recover() error {
	names, err := listSegments(q.dir)
	if err != nil {
		return err
	}
	var maxSeq uint64
	for _, name := range names {
		firstSeq, _ := parseSegmentName(name)
		res, err := scanSegment(filepath.Join(q.dir, name))
		if err != nil {
			return err
		}
		if res.corrupt {
			if err := quarantineTail(q.dir, name, res.badOffset); err != nil {
				return fmt.Errorf("collector/wal: quarantine %s: %w", name, err)
			}
			q.quarantined++
		}
		if len(res.records) == 0 {
			// Whole segment was garbage (badOffset==0): the live file no longer
			// exists after quarantine, so there is nothing to track.
			continue
		}
		seg := &segInfo{name: name, firstSeq: firstSeq, sealed: true}
		for _, rec := range res.records {
			seq := rec.batch.GetBatchSequence()
			seg.lastSeq = seq
			if seq > maxSeq {
				maxSeq = seq
			}
			if seq <= q.lastAcked {
				// Already acknowledged before the crash; not part of the queue.
				continue
			}
			var createdAt time.Time
			if ts := rec.batch.GetCreatedAt(); ts != nil {
				createdAt = ts.AsTime()
			}
			d := batchDesc{
				seq:        seq,
				segName:    name,
				payloadOff: rec.payloadOff,
				payloadLen: rec.payloadLen,
				crc:        rec.crc,
				eventCount: uint64(len(rec.batch.GetEvents())),
				sizeOnDisk: uint64(recordHeaderSize) + uint64(rec.payloadLen),
				createdAt:  createdAt,
			}
			q.unacked = append(q.unacked, d)
			q.liveBytes += d.sizeOnDisk
			q.queuedEvents += d.eventCount
		}
		q.segments = append(q.segments, seg)
	}
	// Defensive: never hand out a sequence that a recovered record already used.
	if maxSeq+1 > q.nextSeq {
		q.nextSeq = maxSeq + 1
		if err := q.persistMetaLocked(); err != nil {
			return err
		}
	}
	return nil
}

// Append implements Queue.Append.
func (q *queue) Append(events []*opensplunkv1.LogEvent) (*opensplunkv1.EventBatch, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return nil, ErrClosed
	}

	seq := q.nextSeq
	batch := &opensplunkv1.EventBatch{
		CollectorId:           q.opts.CollectorID,
		BatchId:               uuid.NewString(),
		BatchSequence:         seq,
		CreatedAt:             timestamppb.New(time.Now().UTC()),
		Events:                events,
		UncompressedSizeBytes: uncompressedEventBytes(events),
		EventIdsSha256:        ComputeEventIDsDigest(events),
		ProtocolMajor:         q.opts.ProtocolMajor,
		ProtocolMinor:         q.opts.ProtocolMinor,
	}
	payload, err := proto.Marshal(batch)
	if err != nil {
		return nil, fmt.Errorf("collector/wal: marshal batch: %w", err)
	}
	record := encodeRecord(payload)
	recordSize := uint64(len(record))

	// Backpressure BEFORE burning a sequence: a full queue must be a clean no-op.
	if q.opts.MaxQueueBytes > 0 && q.liveBytes+recordSize > q.opts.MaxQueueBytes {
		return nil, ErrQueueFull
	}

	// Durably advance the sequence counter BEFORE writing the record so a crash
	// here burns the sequence (a gap) rather than ever reusing it.
	q.nextSeq = seq + 1
	if err := q.persistMetaLocked(); err != nil {
		q.nextSeq = seq // roll back the in-memory counter; nothing was made durable
		return nil, err
	}

	// Test-only crash injection: meta is durable, record is not yet written.
	if q.crashAfterMetaWrite {
		return nil, errSimulatedCrash
	}

	segName, payloadOff, err := q.writeRecordLocked(seq, record)
	if err != nil {
		return nil, err
	}

	if q.opts.Sync == SyncAlways {
		if err := q.active.Sync(); err != nil {
			return nil, fmt.Errorf("collector/wal: fsync record: %w", err)
		}
		q.activeDirty = false
	}

	d := batchDesc{
		seq:        seq,
		segName:    segName,
		payloadOff: payloadOff,
		payloadLen: uint32(len(payload)),
		crc:        crc32c(payload),
		eventCount: uint64(len(events)),
		sizeOnDisk: recordSize,
		createdAt:  batch.GetCreatedAt().AsTime(),
	}
	q.unacked = append(q.unacked, d)
	q.liveBytes += recordSize
	q.queuedEvents += d.eventCount
	q.signalLocked()
	return batch, nil
}

// writeRecordLocked rotates if needed, ensures an active segment, and appends
// record, returning the live segment name and the payload offset of the record.
func (q *queue) writeRecordLocked(seq uint64, record []byte) (string, int64, error) {
	recordSize := int64(len(record))
	if q.active != nil && q.activeSize > 0 && q.opts.SegmentMaxBytes > 0 &&
		q.activeSize+recordSize > int64(q.opts.SegmentMaxBytes) {
		if err := q.sealActiveLocked(); err != nil {
			return "", 0, err
		}
	}
	if q.active == nil {
		if err := q.openActiveLocked(seq); err != nil {
			return "", 0, err
		}
	}
	payloadOff := q.activeSize + recordHeaderSize
	if _, err := q.active.Write(record); err != nil {
		// A partial write leaves a corrupt tail; abandon this segment so no
		// further records are appended after the damage. The tail is quarantined
		// on the next Open.
		_ = q.sealActiveLocked()
		return "", 0, fmt.Errorf("collector/wal: write record: %w", err)
	}
	q.activeSize += recordSize
	q.activeDirty = true
	q.activeSeg.lastSeq = seq
	return q.activeSeg.name, payloadOff, nil
}

// openActiveLocked creates a fresh active segment whose first batch is firstSeq.
func (q *queue) openActiveLocked(firstSeq uint64) error {
	name := segmentName(firstSeq)
	f, err := os.OpenFile(filepath.Join(q.dir, name), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return fmt.Errorf("collector/wal: create segment: %w", err)
	}
	if err := fsyncDir(q.dir); err != nil {
		_ = f.Close()
		return fmt.Errorf("collector/wal: fsync dir after segment create: %w", err)
	}
	seg := &segInfo{name: name, firstSeq: firstSeq, lastSeq: firstSeq, sealed: false}
	q.segments = append(q.segments, seg)
	q.activeSeg = seg
	q.active = f
	q.activeSize = 0
	q.activeDirty = false
	return nil
}

// sealActiveLocked fsyncs and closes the active segment, marking it sealed. A
// sealed segment is only read (by the consumer) and reclaimed once fully acked.
func (q *queue) sealActiveLocked() error {
	if q.active == nil {
		return nil
	}
	syncErr := q.active.Sync()
	closeErr := q.active.Close()
	if q.activeSeg != nil {
		q.activeSeg.sealed = true
	}
	q.active = nil
	q.activeSeg = nil
	q.activeSize = 0
	q.activeDirty = false
	if syncErr != nil {
		return fmt.Errorf("collector/wal: fsync on seal: %w", syncErr)
	}
	if closeErr != nil {
		return fmt.Errorf("collector/wal: close on seal: %w", closeErr)
	}
	return nil
}

// NextBatch implements Queue.NextBatch.
func (q *queue) NextBatch(ctx context.Context) (*opensplunkv1.EventBatch, error) {
	for {
		q.mu.Lock()
		if q.closed {
			q.mu.Unlock()
			return nil, ErrClosed
		}
		if q.deliverIdx < len(q.unacked) {
			d := q.unacked[q.deliverIdx]
			q.deliverIdx++
			q.mu.Unlock()
			return q.readBatch(d)
		}
		q.mu.Unlock()

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-q.closedCh:
			return nil, ErrClosed
		case <-q.notify:
		}
	}
}

// readBatch loads and CRC-verifies an unacked batch from disk. The referenced
// segment cannot be reclaimed while the batch is unacked, so the read is safe
// without holding the queue lock.
func (q *queue) readBatch(d batchDesc) (*opensplunkv1.EventBatch, error) {
	return readRecordPayload(filepath.Join(q.dir, d.segName), d.payloadOff, d.payloadLen, d.crc)
}

// Ack implements Queue.Ack. Acks are cumulative per the collector protocol's
// acknowledged_through semantics: Ack(n) marks every batch with sequence <= n as
// delivered. The high-water mark is persisted durably and advances monotonically;
// acking an older or unknown sequence is a no-op.
func (q *queue) Ack(batchSequence uint64) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return ErrClosed
	}
	if batchSequence <= q.lastAcked {
		return nil
	}

	newLast := batchSequence
	prev := q.lastAcked
	q.lastAcked = newLast
	if err := q.persistMetaLocked(); err != nil {
		q.lastAcked = prev
		return err
	}

	removed := 0
	for removed < len(q.unacked) && q.unacked[removed].seq <= newLast {
		d := q.unacked[removed]
		q.liveBytes -= d.sizeOnDisk
		q.queuedEvents -= d.eventCount
		removed++
	}
	if removed > 0 {
		q.unacked = append(q.unacked[:0], q.unacked[removed:]...)
		q.deliverIdx -= removed
		if q.deliverIdx < 0 {
			q.deliverIdx = 0
		}
	}
	return q.reclaimLocked()
}

// Rewind implements Queue.Rewind. It moves the delivery cursor back to the
// lowest unacked batch and wakes any blocked NextBatch caller.
func (q *queue) Rewind() {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return
	}
	q.deliverIdx = 0
	q.signalLocked()
}

// reclaimLocked deletes sealed segments whose every batch is acknowledged,
// scanning from the front since segments are ordered by ascending sequence.
func (q *queue) reclaimLocked() error {
	reclaimed := 0
	for _, seg := range q.segments {
		if !seg.sealed || seg.lastSeq > q.lastAcked {
			break
		}
		if err := os.Remove(filepath.Join(q.dir, seg.name)); err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("collector/wal: reclaim segment %s: %w", seg.name, err)
		}
		reclaimed++
	}
	if reclaimed > 0 {
		q.segments = append(q.segments[:0], q.segments[reclaimed:]...)
		if err := fsyncDir(q.dir); err != nil {
			return fmt.Errorf("collector/wal: fsync dir after reclaim: %w", err)
		}
	}
	return nil
}

// Stats implements Queue.Stats.
func (q *queue) Stats() Stats {
	q.mu.Lock()
	defer q.mu.Unlock()
	var oldest time.Duration
	if len(q.unacked) > 0 {
		if t := q.unacked[0].createdAt; !t.IsZero() {
			if age := time.Since(t); age > 0 {
				oldest = age
			}
		}
	}
	return Stats{
		QueuedBatches:          uint64(len(q.unacked)),
		QueuedEvents:           q.queuedEvents,
		QueuedBytes:            q.liveBytes,
		OldestEventAge:         oldest,
		NextBatchSequence:      q.nextSeq,
		LastAckedBatchSequence: q.lastAcked,
		QuarantinedSegments:    q.quarantined,
	}
}

// Close implements Queue.Close.
func (q *queue) Close() error {
	q.mu.Lock()
	if q.closed {
		q.mu.Unlock()
		return nil
	}
	q.closed = true
	close(q.closedCh)
	sealErr := q.sealActiveLocked()
	q.mu.Unlock()

	if q.syncStop != nil {
		close(q.syncStop)
		<-q.syncDone
	}
	return sealErr
}

// persistMetaLocked writes the current sequence counters durably. Callers hold mu.
func (q *queue) persistMetaLocked() error {
	return writeMeta(q.dir, walMeta{
		FormatVersion:          currentFormatVersion,
		NextBatchSequence:      q.nextSeq,
		LastAckedBatchSequence: q.lastAcked,
	})
}

// signalLocked wakes at most one NextBatch waiter. Callers hold mu.
func (q *queue) signalLocked() {
	select {
	case q.notify <- struct{}{}:
	default:
	}
}

// syncLoop periodically fsyncs the active segment when Sync == SyncInterval.
func (q *queue) syncLoop(interval time.Duration) {
	defer close(q.syncDone)
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-q.syncStop:
			return
		case <-t.C:
			q.mu.Lock()
			if q.active != nil && q.activeDirty {
				if err := q.active.Sync(); err == nil {
					q.activeDirty = false
				}
			}
			q.mu.Unlock()
		}
	}
}

// uncompressedEventBytes is the deterministic sum of protobuf-encoded event
// sizes stamped into EventBatch.uncompressed_size_bytes, matching the server's
// UncompressedEventBytes.
func uncompressedEventBytes(events []*opensplunkv1.LogEvent) uint64 {
	var total uint64
	for _, event := range events {
		size := uint64(proto.Size(event))
		if ^uint64(0)-total < size {
			return ^uint64(0)
		}
		total += size
	}
	return total
}
