package wal

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
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

// errSimulatedRecoveryCrash is returned by the recovery-ordering test hook
// after successor segments are durably quarantined but before the triggering
// corrupt segment is repaired. Production code never enables the hook.
var errSimulatedRecoveryCrash = errors.New("collector/wal: simulated crash during corruption recovery")

// batchDesc locates one unacked batch record on disk and caches the cheap
// bookkeeping the queue needs without holding the marshaled batch in memory.
type batchDesc struct {
	seq         uint64
	segName     string
	payloadOff  int64
	payloadLen  uint32
	crc         uint32
	eventCount  uint64
	sizeOnDisk  uint64 // recordHeaderSize + payloadLen
	createdAt   time.Time
	sourceMarks []SourceCheckpointMark
}

// ackMarkGroup is an immutable view of one batch's cached source marks.
// PrepareAck copies only these small slice headers while holding q.mu; hash
// aggregation and sorting happen after the queue is unlocked.
type ackMarkGroup struct {
	marks []SourceCheckpointMark
}

type ackPlan struct {
	throughBatchSequence uint64
	batchCount           uint64
	markGroups           []ackMarkGroup
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

	unacked []batchDesc
	// unackedHeadWaste counts descriptors cleared from the front since the
	// current backing array was allocated. It triggers geometric compaction so
	// repeated single-batch acknowledgments are amortized O(n), while old mark
	// references and oversized backing arrays are still released promptly.
	unackedHeadWaste int
	deliverIdx       int
	liveBytes        uint64
	queuedEvents     uint64
	// terminal contains exact out-of-order acknowledgments not yet representable
	// by the persisted cumulative high-water mark. It is intentionally volatile:
	// losing it on crash only causes safe at-least-once replay.
	terminal map[uint64]struct{}

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
	// crashAfterSuccessorQuarantine is a test-only recovery ordering hook.
	crashAfterSuccessorQuarantine bool
}

// openQueue implements Open: it creates or recovers the on-disk state under
// opts.Dir, quarantining corrupt tails, and returns a ready queue.
func openQueue(opts Options) (Queue, error) {
	if opts.Dir == "" {
		return nil, errors.New("collector/wal: Options.Dir is required")
	}
	if err := os.MkdirAll(opts.Dir, 0o700); err != nil {
		return nil, fmt.Errorf("collector/wal: create dir: %w", err)
	}
	// Tighten an existing directory too: MkdirAll leaves a pre-existing dir's mode
	// untouched, and the queue holds raw event payloads that must not be
	// world/group readable.
	if err := os.Chmod(opts.Dir, 0o700); err != nil {
		return nil, fmt.Errorf("collector/wal: secure dir: %w", err)
	}

	q := &queue{
		opts:     opts,
		dir:      opts.Dir,
		notify:   make(chan struct{}, 1),
		closedCh: make(chan struct{}),
		terminal: make(map[uint64]struct{}),
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
// tails, and rebuilds the unacked index and segment list. Once corruption
// removes an unknown record, every later segment is quarantined too: retaining
// a later source offset would let checkpoint advancement jump across bytes that
// now exist in neither the WAL nor the server.
func (q *queue) recover() error {
	names, err := listSegments(q.dir)
	if err != nil {
		return err
	}
	var maxSeq uint64
	for nameIndex, name := range names {
		firstSeq, _ := parseSegmentName(name)
		if firstSeq > maxSeq {
			maxSeq = firstSeq
		}
		res, err := scanSegment(filepath.Join(q.dir, name))
		if err != nil {
			return err
		}
		// Even records behind the barrier contribute only to the defensive next
		// sequence calculation. They are never made live again.
		for _, rec := range res.records {
			if seq := rec.batch.GetBatchSequence(); seq > maxSeq {
				maxSeq = seq
			}
		}
		stopAfterSegment := res.corrupt
		if res.corrupt {
			// Discover the full allocated sequence floor before mutating any
			// successor. This preserves non-reuse even if meta.json was missing and
			// recovery crashes partway through quarantine.
			for _, successor := range names[nameIndex+1:] {
				successorFirst, _ := parseSegmentName(successor)
				if successorFirst > maxSeq {
					maxSeq = successorFirst
				}
				successorScan, scanErr := scanSegment(filepath.Join(q.dir, successor))
				if scanErr != nil {
					return scanErr
				}
				for _, record := range successorScan.records {
					if seq := record.batch.GetBatchSequence(); seq > maxSeq {
						maxSeq = seq
					}
				}
			}
			if err := q.persistRecoveredSequenceFloorLocked(maxSeq); err != nil {
				return err
			}

			// Quarantine and fsync every successor while the triggering corrupt
			// record is still live. Any crash or error in this loop leaves that
			// record discoverable on the next Open, so the barrier cannot vanish.
			for _, successor := range names[nameIndex+1:] {
				if err := quarantineTail(q.dir, successor, 0); err != nil {
					return fmt.Errorf("collector/wal: quarantine segment after corrupt gap %s: %w", successor, err)
				}
				q.quarantined++
			}
			if q.crashAfterSuccessorQuarantine {
				return errSimulatedRecoveryCrash
			}
			// Repair the triggering segment last. Once its corrupt marker is no
			// longer visible, no live successor remains that could cross the gap.
			if err := quarantineTail(q.dir, name, res.badOffset); err != nil {
				return fmt.Errorf("collector/wal: quarantine %s: %w", name, err)
			}
			q.quarantined++
		}
		if len(res.records) == 0 {
			// Whole segment was garbage (badOffset==0): the live file no longer
			// exists after quarantine, so there is nothing to track.
			if stopAfterSegment {
				break
			}
			continue
		}
		seg := &segInfo{name: name, firstSeq: firstSeq, sealed: true}
		for _, rec := range res.records {
			seq := rec.batch.GetBatchSequence()
			seg.lastSeq = seq
			if seq <= q.lastAcked {
				// Already acknowledged before the crash; not part of the queue.
				continue
			}
			var createdAt time.Time
			if ts := rec.batch.GetCreatedAt(); ts != nil {
				createdAt = ts.AsTime()
			}
			d := batchDesc{
				seq:         seq,
				segName:     name,
				payloadOff:  rec.payloadOff,
				payloadLen:  rec.payloadLen,
				crc:         rec.crc,
				eventCount:  uint64(len(rec.batch.GetEvents())),
				sizeOnDisk:  uint64(recordHeaderSize) + uint64(rec.payloadLen),
				createdAt:   createdAt,
				sourceMarks: checkpointMarksForBatch(seq, rec.batch.GetEvents()),
			}
			q.appendUnackedLocked(d)
			q.liveBytes += d.sizeOnDisk
			q.queuedEvents += d.eventCount
		}
		q.segments = append(q.segments, seg)
		if stopAfterSegment {
			break
		}
	}
	// Defensive: never hand out a sequence that a recovered record already used.
	if err := q.persistRecoveredSequenceFloorLocked(maxSeq); err != nil {
		return err
	}
	return nil
}

func (q *queue) persistRecoveredSequenceFloorLocked(maxSeq uint64) error {
	if maxSeq == ^uint64(0) {
		return errors.New("collector/wal: recovered batch sequence exhausted uint64")
	}
	if next := maxSeq + 1; next > q.nextSeq {
		q.nextSeq = next
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

	// A record that cannot fit even an empty queue is terminal, not backpressure:
	// reporting ErrQueueFull would make the daemon retry it forever. Check this
	// BEFORE the live-bytes backpressure check.
	if q.opts.MaxQueueBytes > 0 && recordSize > q.opts.MaxQueueBytes {
		return nil, ErrBatchTooLarge
	}

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
		seq:         seq,
		segName:     segName,
		payloadOff:  payloadOff,
		payloadLen:  uint32(len(payload)),
		crc:         crc32c(payload),
		eventCount:  uint64(len(events)),
		sizeOnDisk:  recordSize,
		createdAt:   batch.GetCreatedAt().AsTime(),
		sourceMarks: checkpointMarksForBatch(seq, events),
	}
	q.appendUnackedLocked(d)
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
	f, err := os.OpenFile(filepath.Join(q.dir, name), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
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

// Ack implements Queue.Ack. It records one exact terminal disposition. A later
// batch cannot delete an earlier retryable batch: the durable high-water mark
// advances only across the terminal prefix of q.unacked.
func (q *queue) Ack(batchSequence uint64) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return ErrClosed
	}
	if batchSequence <= q.lastAcked {
		return nil
	}
	if !q.hasSequenceLocked(batchSequence) {
		return fmt.Errorf("%w: sequence %d is not queued", ErrInvalidAck, batchSequence)
	}
	q.terminal[batchSequence] = struct{}{}
	return q.advanceTerminalLocked()
}

// PrepareAck implements Queue.PrepareAck.
func (q *queue) PrepareAck(batchSequence uint64) (AckPreview, error) {
	q.mu.Lock()
	plan, err := q.prepareAckPlanLocked(batchSequence, false)
	q.mu.Unlock()
	if err != nil {
		return AckPreview{}, err
	}
	return aggregateAckPlan(plan), nil
}

// AckThrough implements Queue.AckThrough for a server's explicit cumulative
// durable claim. Validation prevents a corrupt/malicious future resume value
// from burning batches that have not even been appended yet.
func (q *queue) AckThrough(batchSequence uint64) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return ErrClosed
	}
	if batchSequence <= q.lastAcked {
		return nil
	}
	if !q.hasSequenceLocked(batchSequence) {
		return fmt.Errorf("%w: cumulative sequence %d is not queued", ErrInvalidAck, batchSequence)
	}
	for _, d := range q.unacked {
		if d.seq > batchSequence {
			break
		}
		q.terminal[d.seq] = struct{}{}
	}
	return q.advanceTerminalLocked()
}

// PrepareAckThrough implements Queue.PrepareAckThrough.
func (q *queue) PrepareAckThrough(batchSequence uint64) (AckPreview, error) {
	q.mu.Lock()
	plan, err := q.prepareAckPlanLocked(batchSequence, true)
	q.mu.Unlock()
	if err != nil {
		return AckPreview{}, err
	}
	return aggregateAckPlan(plan), nil
}

// prepareAckPlanLocked validates a hypothetical exact or cumulative ack and
// snapshots immutable source-mark slice headers for its newly contiguous
// prefix. It deliberately does not mutate q.terminal or persisted state.
func (q *queue) prepareAckPlanLocked(batchSequence uint64, cumulative bool) (ackPlan, error) {
	if q.closed {
		return ackPlan{}, ErrClosed
	}
	if batchSequence <= q.lastAcked {
		return ackPlan{}, nil
	}
	if !q.hasSequenceLocked(batchSequence) {
		kind := "sequence"
		if cumulative {
			kind = "cumulative sequence"
		}
		return ackPlan{}, fmt.Errorf("%w: %s %d is not queued", ErrInvalidAck, kind, batchSequence)
	}

	var plan ackPlan
	prefixLength := 0
	markGroupCount := 0
	for descriptorIndex, d := range q.unacked {
		_, alreadyTerminal := q.terminal[d.seq]
		hypotheticallyTerminal := alreadyTerminal || d.seq == batchSequence
		if cumulative && d.seq <= batchSequence {
			hypotheticallyTerminal = true
		}
		if !hypotheticallyTerminal {
			break
		}
		plan.throughBatchSequence = d.seq
		plan.batchCount++
		prefixLength = descriptorIndex + 1
		if len(d.sourceMarks) > 0 {
			markGroupCount++
		}
	}
	if markGroupCount == 0 {
		return plan, nil
	}
	// Size the immutable header snapshot exactly. Geometric append growth for a
	// 100K-batch prefix otherwise allocates and copies tens of megabytes while
	// holding q.mu even when the final checkpoint set coalesces to one identity.
	plan.markGroups = make([]ackMarkGroup, 0, markGroupCount)
	for _, d := range q.unacked[:prefixLength] {
		if len(d.sourceMarks) > 0 {
			plan.markGroups = append(plan.markGroups, ackMarkGroup{
				marks: d.sourceMarks,
			})
		}
	}
	return plan, nil
}

// aggregateAckPlan performs the potentially expensive identity hash
// aggregation and deterministic sort without holding the queue mutex. The mark
// arrays are immutable after Append/recovery, so a concurrent ack may discard
// its descriptor without invalidating this snapshot.
func aggregateAckPlan(plan ackPlan) AckPreview {
	var byIdentity map[string]SourceCheckpointMark
	for _, group := range plan.markGroups {
		if byIdentity == nil {
			byIdentity = make(map[string]SourceCheckpointMark)
		}
		for _, mark := range group.marks {
			// Preserve the first malformed mark verbatim so the daemon fails
			// closed without retaining every malformed event in a hostile WAL.
			if mark.FileIdentity == "" || !mark.HasEndOffset || mark.ConflictingMetadata {
				return AckPreview{
					ThroughBatchSequence: plan.throughBatchSequence,
					BatchCount:           plan.batchCount,
					Marks:                []SourceCheckpointMark{mark},
				}
			}
			current, ok := byIdentity[mark.FileIdentity]
			if ok && current.HasFingerprintLength && mark.HasFingerprintLength &&
				current.FingerprintLength != mark.FingerprintLength {
				mark.ConflictingMetadata = true
				return AckPreview{
					ThroughBatchSequence: plan.throughBatchSequence,
					BatchCount:           plan.batchCount,
					Marks:                []SourceCheckpointMark{mark},
				}
			}
			if !ok || mark.EndOffset > current.EndOffset ||
				(mark.EndOffset == current.EndOffset && mark.BatchSequence >= current.BatchSequence) {
				byIdentity[mark.FileIdentity] = mark
			}
		}
	}
	marks := make([]SourceCheckpointMark, 0, len(byIdentity))
	for _, mark := range byIdentity {
		marks = append(marks, mark)
	}
	sort.Slice(marks, func(i, j int) bool {
		return marks[i].FileIdentity < marks[j].FileIdentity
	})
	return AckPreview{
		ThroughBatchSequence: plan.throughBatchSequence,
		BatchCount:           plan.batchCount,
		Marks:                marks,
	}
}

// checkpointMarksForBatch extracts a compact high-water mark per exact file
// identity while the EventBatch is already resident (Append or recovery).
// Invalid file origins retain one representative mark so acknowledgment later
// fails closed instead of silently dropping source coordinates.
func checkpointMarksForBatch(batchSequence uint64, events []*opensplunkv1.LogEvent) []SourceCheckpointMark {
	byIdentity := make(map[string]SourceCheckpointMark)
	var invalid *SourceCheckpointMark
	for eventIndex, event := range events {
		if event == nil || event.GetOrigin() == nil || event.GetOrigin().FileIdentity == nil {
			continue
		}
		origin := event.GetOrigin()
		mark := SourceCheckpointMark{
			BatchSequence:        batchSequence,
			EventIndex:           uint32(eventIndex),
			FileIdentity:         origin.GetFileIdentity(),
			SourcePath:           origin.GetSourcePath(),
			EndOffset:            origin.GetEndOffset(),
			LineNumber:           origin.GetLineNumber(),
			FingerprintLength:    origin.GetFileFingerprintLength(),
			HasSourcePath:        origin.SourcePath != nil,
			HasEndOffset:         origin.EndOffset != nil,
			HasFingerprintLength: origin.FileFingerprintLength != nil,
		}
		if mark.FileIdentity == "" || !mark.HasEndOffset {
			if invalid == nil {
				copy := mark
				invalid = &copy
			}
			continue
		}
		current, ok := byIdentity[mark.FileIdentity]
		if ok && current.ConflictingMetadata {
			continue
		}
		if ok && current.HasFingerprintLength && mark.HasFingerprintLength &&
			current.FingerprintLength != mark.FingerprintLength {
			mark.ConflictingMetadata = true
			byIdentity[mark.FileIdentity] = mark
			continue
		}
		if !ok || mark.EndOffset >= current.EndOffset {
			byIdentity[mark.FileIdentity] = mark
		}
	}
	marks := make([]SourceCheckpointMark, 0, len(byIdentity)+1)
	if invalid != nil {
		marks = append(marks, *invalid)
	}
	for _, mark := range byIdentity {
		marks = append(marks, mark)
	}
	sort.Slice(marks, func(i, j int) bool {
		if marks[i].FileIdentity == marks[j].FileIdentity {
			return marks[i].EventIndex < marks[j].EventIndex
		}
		return marks[i].FileIdentity < marks[j].FileIdentity
	})
	return marks
}

func (q *queue) hasSequenceLocked(sequence uint64) bool {
	for _, d := range q.unacked {
		if d.seq == sequence {
			return true
		}
		if d.seq > sequence {
			return false
		}
	}
	return false
}

func (q *queue) appendUnackedLocked(descriptor batchDesc) {
	// append reallocates exactly when len == cap. Any inaccessible consumed
	// prefix belongs to the old allocation and can no longer cause retention.
	if len(q.unacked) == cap(q.unacked) {
		q.unackedHeadWaste = 0
	}
	q.unacked = append(q.unacked, descriptor)
}

func (q *queue) advanceTerminalLocked() error {
	removed := 0
	var newLast uint64
	for removed < len(q.unacked) {
		d := q.unacked[removed]
		if _, ok := q.terminal[d.seq]; !ok {
			break
		}
		newLast = d.seq
		removed++
	}
	if removed == 0 {
		return nil
	}

	prev := q.lastAcked
	q.lastAcked = newLast
	if err := q.persistMetaLocked(); err != nil {
		q.lastAcked = prev
		return err
	}

	for i := 0; i < removed; i++ {
		d := q.unacked[i]
		q.liveBytes -= d.sizeOnDisk
		q.queuedEvents -= d.eventCount
		delete(q.terminal, d.seq)
		q.unacked[i] = batchDesc{}
	}
	remaining := len(q.unacked) - removed
	q.unackedHeadWaste += removed
	switch {
	case remaining == 0:
		// Retain only a small empty buffer for the common short drain/refill
		// cycle. Let a large backing array go once the queue becomes empty.
		if cap(q.unacked) <= 256 {
			q.unacked = q.unacked[:0]
		} else {
			q.unacked = nil
		}
		q.unackedHeadWaste = 0
	case q.unackedHeadWaste >= remaining:
		// At least half of the backing storage is consumed. One geometric copy
		// here prevents both O(n²) per-ack shifts and unbounded head retention.
		compacted := make([]batchDesc, remaining)
		copy(compacted, q.unacked[removed:])
		q.unacked = compacted
		q.unackedHeadWaste = 0
	default:
		q.unacked = q.unacked[removed:]
	}
	q.deliverIdx -= removed
	if q.deliverIdx < 0 {
		q.deliverIdx = 0
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
