package wal

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"fortio.org/safecast"
	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/privatefs"
)

// errSimulatedCrash is returned from Append when the crashAfterMetaWrite test
// hook fires, after the sequence has been durably burned in meta but before the
// batch record is written. Production code never sets the hook.
var errSimulatedCrash = errors.New("collector/wal: simulated crash after meta write")

// errSimulatedRecoveryCrash is returned by the recovery-ordering test hook
// after successor segments are durably quarantined but before the triggering
// corrupt segment is repaired. Production code never enables the hook.
var errSimulatedRecoveryCrash = errors.New("collector/wal: simulated crash during corruption recovery")

// quarantineStorageRefreshInterval bounds directory scans when an operator-
// managed quarantine artifact keeps the queue at capacity. Healthy appends do
// not scan, and removal is observed within this interval while pressure lasts.
const quarantineStorageRefreshInterval = time.Second

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

type ackPlan struct {
	throughBatchSequence uint64
	batchCount           uint64
	// markGroups holds immutable views of each batch's cached source marks.
	// PrepareAck copies only these small slice headers while holding q.mu; hash
	// aggregation and sorting happen after the queue is unlocked.
	markGroups [][]SourceCheckpointMark
}

// segInfo tracks a segment file's sequence span and seal state for reclamation.
type segInfo struct {
	name     string
	firstSeq uint64
	lastSeq  uint64
	sealed   bool
	size     uint64
}

// recoveredSegmentScan is the compact result of streaming one segment during
// startup. descriptors contains only the valid unacknowledged prefix; decoded
// EventBatch payloads are released record by record.
type recoveredSegmentScan struct {
	result         scanResult
	validRecords   uint64
	lastSequence   uint64
	descriptors    []batchDesc
	semanticReason string
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
	physicalBytes    uint64
	quarantinedBytes uint64
	queuedEvents     uint64
	// terminal contains exact out-of-order acknowledgments not yet representable
	// by the persisted cumulative high-water mark. It is intentionally volatile:
	// losing it on crash only causes safe at-least-once replay.
	terminal map[uint64]struct{}

	segments        []*segInfo
	activeSeg       *segInfo
	active          *os.File
	activeSize      int64
	activeDirty     bool
	syncErr         error
	syncFile        func(*os.File) error
	syncDirectory   func(string) error
	removeFile      func(string) error
	persistMetaFile func(string, walMeta) error

	quarantined                uint64
	recoveryWarning            string
	nextQuarantineStatsRefresh time.Time
	quarantineStatsRefreshErr  error

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
func openQueue(opts Options) (*queue, error) {
	if opts.Dir == "" {
		return nil, errors.New("collector/wal: Options.Dir is required")
	}
	if opts.Sync < SyncOnSeal || opts.Sync > SyncAlways {
		return nil, fmt.Errorf("collector/wal: invalid sync policy %d", opts.Sync)
	}
	if opts.Sync == SyncInterval && opts.SyncInterval <= 0 {
		return nil, errors.New("collector/wal: SyncInterval requires a positive interval")
	}
	if _, err := mkdirAllDurable(opts.Dir, 0o700); err != nil {
		return nil, fmt.Errorf("collector/wal: create dir: %w", err)
	}
	// Tighten an existing directory too: MkdirAll leaves a pre-existing dir's mode
	// untouched, and the queue holds raw event payloads that must not be
	// world/group readable.

	if err := privatefs.SecureDirectory(opts.Dir); err != nil {
		return nil, fmt.Errorf("collector/wal: secure dir: %w", err)
	}
	if err := recoverInterruptedQuarantines(opts.Dir); err != nil {
		return nil, fmt.Errorf("collector/wal: recover interrupted quarantine: %w", err)
	}

	q := &queue{
		opts:            opts,
		dir:             opts.Dir,
		notify:          make(chan struct{}, 1),
		closedCh:        make(chan struct{}),
		terminal:        make(map[uint64]struct{}),
		syncFile:        func(file *os.File) error { return file.Sync() },
		syncDirectory:   fsyncDir,
		removeFile:      os.Remove,
		persistMetaFile: writeMeta,
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
	if err := q.refreshStorageStatsLocked(); err != nil {
		return nil, err
	}

	// Reclaim any segment that was already fully acked before the last crash.
	if err := q.reclaimLocked(); err != nil {
		return nil, err
	}

	if opts.Sync == SyncInterval {
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
	var lastSequence uint64
	batchIDs := make(map[string]uint64)
	for nameIndex, name := range names {
		firstSeq, _ := parseSegmentName(name)
		if firstSeq > maxSeq {
			maxSeq = firstSeq
		}
		scan, err := q.scanRecoverableSegment(name, &lastSequence, batchIDs, &maxSeq)
		if err != nil {
			return err
		}
		res := scan.result
		if scan.semanticReason != "" && q.recoveryWarning == "" {
			q.recoveryWarning = fmt.Sprintf("%s: %s", name, scan.semanticReason)
		}
		stopAfterSegment := res.corrupt
		if res.corrupt {
			if q.recoveryWarning == "" {
				q.recoveryWarning = fmt.Sprintf("%s: corrupt record at byte offset %d", name, res.badOffset)
			}
			// Discover the full allocated sequence floor before mutating any
			// successor. This preserves non-reuse even if meta.json was missing and
			// recovery crashes partway through quarantine.
			for _, successor := range names[nameIndex+1:] {
				successorFirst, _ := parseSegmentName(successor)
				if successorFirst > maxSeq {
					maxSeq = successorFirst
				}
				if err := q.observeSegmentRecords(
					successor, &lastSequence, batchIDs, &maxSeq,
				); err != nil {
					return err
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
		if scan.validRecords == 0 {
			// Whole segment was garbage (badOffset==0): the live file no longer
			// exists after quarantine, so there is nothing to track.
			if stopAfterSegment {
				break
			}
			continue
		}
		info, err := os.Stat(filepath.Join(q.dir, name))
		if err != nil {
			return err
		}
		if info.Size() < 0 {
			return fmt.Errorf("collector/wal: segment %s has a negative size", name)
		}

		size := safecast.MustConv[uint64](info.Size())
		seg := &segInfo{
			name: name, firstSeq: firstSeq, lastSeq: scan.lastSequence,
			sealed: true, size: size,
		}
		for _, d := range scan.descriptors {
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

// scanRecoverableSegment streams one segment, validating ownership for every
// physically intact record even after a semantic barrier. Only the valid prefix
// contributes descriptors, so a large CRC-valid file cannot retain every
// decoded EventBatch in memory during startup.
func (q *queue) scanRecoverableSegment(
	name string,
	lastSequence *uint64,
	batchIDs map[string]uint64,
	maximumSequence *uint64,
) (recoveredSegmentScan, error) {
	firstSequence, _ := parseSegmentName(name)
	var scan recoveredSegmentScan
	var semanticOffset int64
	semanticBarrier := false
	recordIndex := 0
	result, err := walkSegment(filepath.Join(q.dir, name), func(record scannedRecord) error {
		index := recordIndex
		recordIndex++
		// Even records behind the semantic barrier contribute to the defensive
		// sequence floor and must have the configured owner/protocol. They never
		// become live, but foreign intact data must not be hidden by an earlier
		// malformed record.
		if err := q.validateRecoveredOwnership(name, record); err != nil {
			return err
		}
		if semanticBarrier {
			return nil
		}
		if reason := validateRecoveredRecord(
			firstSequence, index, record, lastSequence, batchIDs,
		); reason != "" {
			semanticBarrier = true
			semanticOffset = record.payloadOff - recordHeaderSize
			scan.semanticReason = reason
			return nil
		}

		batch := record.batch
		sequence := batch.GetBatchSequence()
		if sequence > *maximumSequence {
			*maximumSequence = sequence
		}
		scan.validRecords++
		scan.lastSequence = sequence
		if sequence <= q.lastAcked {
			return nil
		}
		createdAt := batch.GetCreatedAt().AsTime()
		scan.descriptors = append(scan.descriptors, batchDesc{
			seq:         sequence,
			segName:     name,
			payloadOff:  record.payloadOff,
			payloadLen:  record.payloadLen,
			crc:         record.crc,
			eventCount:  uint64(len(batch.GetEvents())),
			sizeOnDisk:  uint64(recordHeaderSize) + uint64(record.payloadLen),
			createdAt:   createdAt,
			sourceMarks: checkpointMarksForBatch(sequence, batch.GetEvents()),
		})
		return nil
	})
	if err != nil {
		return recoveredSegmentScan{}, err
	}
	if err := observeAllocatedSequenceFloor(
		name, firstSequence, result, maximumSequence,
	); err != nil {
		return recoveredSegmentScan{}, err
	}
	if semanticBarrier {
		result.badOffset = semanticOffset
		result.corrupt = true
	}
	scan.result = result
	return scan, nil
}

// observeSegmentRecords streams a segment behind an already-established gap.
// It retains no descriptors but still validates immutable ownership/protocol
// and discovers the allocated sequence floor before quarantine mutates files.
func (q *queue) observeSegmentRecords(
	name string,
	lastSequence *uint64,
	batchIDs map[string]uint64,
	maximumSequence *uint64,
) error {
	firstSequence, _ := parseSegmentName(name)
	semanticBarrier := false
	recordIndex := 0
	result, err := walkSegment(filepath.Join(q.dir, name), func(record scannedRecord) error {
		index := recordIndex
		recordIndex++
		if err := q.validateRecoveredOwnership(name, record); err != nil {
			return err
		}
		if semanticBarrier {
			return nil
		}
		if reason := validateRecoveredRecord(
			firstSequence, index, record, lastSequence, batchIDs,
		); reason != "" {
			// The successor is already being quarantined whole. Ignore untrusted
			// sequence values at and beyond its first semantic corruption rather
			// than allowing a forged MaxUint64 to make recovery permanently fail.
			semanticBarrier = true
			return nil
		}
		if sequence := record.batch.GetBatchSequence(); sequence > *maximumSequence {
			*maximumSequence = sequence
		}
		return nil
	})
	if err != nil {
		return err
	}
	return observeAllocatedSequenceFloor(name, firstSequence, result, maximumSequence)
}

// observeAllocatedSequenceFloor derives a trustworthy allocation floor without
// trusting semantic fields at or behind a corrupt record. Append writes records
// contiguously within a segment: any counter burn without a record seals the
// active segment before another Append. Thus the filename's first sequence plus
// the count of physically intact records establishes the clean floor. Once a
// physical record is corrupt, its framing and every later boundary are
// untrusted. The remaining byte count therefore supplies a conservative upper
// bound using the minimum possible nonempty frame size. Semantic validation may
// raise the floor further for a legitimate historical gap.
func observeAllocatedSequenceFloor(
	name string,
	firstSequence uint64,
	result scanResult,
	maximumSequence *uint64,
) error {
	floor := firstSequence
	var additional uint64
	switch {
	case result.corrupt:
		if result.badOffset < 0 || result.fileSize <= result.badOffset {
			return fmt.Errorf("collector/wal: segment %s has invalid corrupt-tail bounds", name)
		}
		// A physical tail may contain later allocated records even though scanning
		// stopped at the first bad CRC/header. Because the corrupt header can also
		// falsify its payload length, no apparent later boundary is trustworthy.
		// Every complete record occupies at least the fixed header plus one nonzero
		// payload byte; ceil(tail/minimumFrame) also accounts for one final partial
		// append. Over-burning is safe, while undercounting could reuse a sequence
		// previously observed by the server.
		const minimumRecordBytes = uint64(recordHeaderSize + 1)

		tailBytes := safecast.MustConv[uint64](result.fileSize - result.badOffset)
		tailAllocations := tailBytes / minimumRecordBytes
		if tailBytes%minimumRecordBytes != 0 {
			tailAllocations++
		}
		if result.recordCount > math.MaxUint64-tailAllocations {
			return fmt.Errorf("collector/wal: segment %s allocation count overflows", name)
		}
		allocationCount := result.recordCount + tailAllocations
		additional = allocationCount - 1
	case result.recordCount > 0:
		additional = result.recordCount - 1
	}
	if additional > math.MaxUint64-firstSequence {
		return fmt.Errorf("collector/wal: segment %s record count overflows batch sequence", name)
	}
	floor += additional
	if floor > *maximumSequence {
		*maximumSequence = floor
	}
	return nil
}

func (q *queue) refreshStorageStatsLocked() error {
	entries, err := os.ReadDir(q.dir)
	if err != nil {
		return fmt.Errorf("collector/wal: inspect queue storage: %w", err)
	}
	var physicalBytes uint64
	var quarantinedBytes uint64
	var quarantined uint64
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		info, err := entry.Info()
		if err != nil {
			return fmt.Errorf("collector/wal: stat %s: %w", entry.Name(), err)
		}
		if info.Size() < 0 {
			return fmt.Errorf("collector/wal: %s has a negative size", entry.Name())
		}

		size := safecast.MustConv[uint64](info.Size())
		if _, live := parseSegmentName(entry.Name()); live {
			physicalBytes += size
			continue
		}
		if strings.Contains(entry.Name(), corruptSuffix) {
			quarantined++
			quarantinedBytes += size
		}
	}
	// Publish only after the entire directory scan succeeds. A transient Info
	// failure must preserve the prior conservative accounting rather than leave
	// a partial undercount that a later Append could mistake for free capacity.
	q.physicalBytes = physicalBytes
	q.quarantinedBytes = quarantinedBytes
	q.quarantined = quarantined
	if quarantined > 0 && q.recoveryWarning == "" {
		q.recoveryWarning = "retained quarantine artifacts require operator review"
	}
	return nil
}

// refreshQuarantineStatsForCapacityLocked refreshes operator-managed
// quarantine state at most once per interval while the queue is under pressure.
// A failed scan is cached for the same interval: accounting remains at its last
// fully observed (conservative) value, and callers continue to receive the
// diagnostic error rather than mistaking stale state for available capacity.
func (q *queue) refreshQuarantineStatsForCapacityLocked() error {
	now := time.Now()
	if !q.nextQuarantineStatsRefresh.IsZero() && now.Before(q.nextQuarantineStatsRefresh) {
		return q.quarantineStatsRefreshErr
	}
	err := q.refreshStorageStatsLocked()
	q.nextQuarantineStatsRefresh = time.Now().Add(quarantineStorageRefreshInterval)
	q.quarantineStatsRefreshErr = err
	return err
}

func (q *queue) validateRecoveredOwnership(segmentName string, record scannedRecord) error {
	recoveredCollectorID := record.batch.GetCollectorId()
	if recoveredCollectorID != q.opts.CollectorID {
		return fmt.Errorf(
			"collector/wal: recovered batch %d in %s has collector identity mismatch: batch %q, configured %q",
			record.batch.GetBatchSequence(), segmentName, recoveredCollectorID, q.opts.CollectorID,
		)
	}
	return nil
}

// validateRecoveredRecord validates invariants which the record CRC cannot
// express. It returns the reason this record must form a global quarantine
// barrier, or empty after advancing the semantic validation state. Ownership
// and protocol are checked separately for every physically intact record.
func validateRecoveredRecord(
	firstSequence uint64,
	recordIndex int,
	record scannedRecord,
	lastSequence *uint64,
	batchIDs map[string]uint64,
) string {
	batch := record.batch
	sequence := batch.GetBatchSequence()
	switch {
	case sequence == 0:
		return "zero batch sequence"
	case recordIndex == 0 && sequence != firstSequence:
		return "segment filename does not match first record"
	case sequence <= *lastSequence:
		return "batch sequences are not globally increasing"
	}
	batchID := batch.GetBatchId()
	parsedID, err := uuid.Parse(batchID)
	if err != nil || parsedID.Version() != 4 {
		return "batch id is not a UUIDv4"
	}
	if previous, duplicate := batchIDs[batchID]; duplicate {
		return fmt.Sprintf("batch id duplicates sequence %d", previous)
	}
	if !bytes.Equal(batch.GetEventIdsSha256(), ComputeEventIDsDigest(batch.GetEvents())) {
		return "event id digest mismatch"
	}
	if batch.GetUncompressedSizeBytes() != uncompressedEventBytes(batch.GetEvents()) {
		return "uncompressed event size mismatch"
	}
	if batch.GetCreatedAt() == nil || batch.GetCreatedAt().CheckValid() != nil {
		return "invalid created_at"
	}
	*lastSequence = sequence
	batchIDs[batchID] = sequence
	return ""
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
func (q *queue) Append(events []*opensplunk.LogEvent) (*opensplunk.EventBatch, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return nil, ErrClosed
	}
	if q.syncErr != nil {
		return nil, fmt.Errorf("collector/wal: storage sync is unhealthy: %w", q.syncErr)
	}

	seq := q.nextSeq
	if seq == math.MaxUint64 {
		return nil, errors.New("collector/wal: batch sequence exhausted uint64")
	}
	eventIDsDigest := ComputeEventIDsDigest(events)
	if eventIDsDigest == nil {
		return nil, ErrBatchTooLarge
	}
	batch := &opensplunk.EventBatch{
		CollectorId:           q.opts.CollectorID,
		BatchId:               uuid.NewString(),
		BatchSequence:         seq,
		CreatedAt:             timestamppb.New(time.Now().UTC()),
		Events:                events,
		UncompressedSizeBytes: uncompressedEventBytes(events),
		EventIdsSha256:        eventIDsDigest,
	}
	payloadSize := proto.Size(batch)
	if payloadSize < 0 {
		return nil, ErrBatchTooLarge
	}
	// Enforce the same stable payload ceiling used by recovery. Otherwise Append
	// could successfully publish a record which this process would quarantine on
	// its next restart. Size the message before marshaling so repeated QueueFull
	// retries do not allocate and copy the entire event batch on every attempt.

	payloadSizeBytes := uint64(payloadSize)
	if payloadSizeBytes > maximumRecordPayloadBytes {
		return nil, ErrBatchTooLarge
	}
	recordSize := uint64(recordHeaderSize) + payloadSizeBytes

	if err := q.ensureRecordCapacityLocked(recordSize); err != nil {
		return nil, err
	}

	payload, err := proto.Marshal(batch)
	if err != nil {
		return nil, fmt.Errorf("collector/wal: marshal batch: %w", err)
	}
	if len(payload) != payloadSize {
		// Defensively re-check the exact representation if a custom protobuf value
		// or racy caller made proto.Size stale. Normal generated messages never take
		// this path.

		payloadSizeBytes = uint64(len(payload))
		if payloadSizeBytes > maximumRecordPayloadBytes {
			return nil, ErrBatchTooLarge
		}
		recordSize = uint64(recordHeaderSize) + payloadSizeBytes
		if err := q.ensureRecordCapacityLocked(recordSize); err != nil {
			return nil, err
		}
	}
	record, err := encodeRecord(payload)
	if err != nil {
		return nil, err
	}

	// Durably advance the sequence counter BEFORE writing the record so a crash
	// here burns the sequence (a gap) rather than ever reusing it.
	q.nextSeq = seq + 1
	if err := q.persistMetaLocked(); err != nil {
		// Do not roll the counter back. A failure after rename but during the
		// directory fsync is ambiguous: the new meta may already be visible or even
		// durable. Burning this sequence in memory is always safe; reusing it on a
		// later Append in the same process is not.
		return nil, q.sealActiveAfterSequenceBurnLocked(err)
	}

	// Test-only crash injection: meta is durable, record is not yet written.
	if q.crashAfterMetaWrite {
		return nil, q.sealActiveAfterSequenceBurnLocked(errSimulatedCrash)
	}

	segName, payloadOff, err := q.writeRecordLocked(seq, record)
	if err != nil {
		return nil, err
	}

	if q.opts.Sync == SyncAlways {
		if err := q.syncActiveLocked(); err != nil {
			q.syncErr = err
			return nil, fmt.Errorf("collector/wal: fsync record: %w", err)
		}
		q.activeDirty = false
		q.syncErr = nil
	}

	d := batchDesc{
		seq:        seq,
		segName:    segName,
		payloadOff: payloadOff,

		payloadLen:  safecast.MustConv[uint32](len(payload)),
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

func exceedsByteLimit(current, additional, limit uint64) bool {
	return current > limit || additional > limit-current
}

// ensureRecordCapacityLocked classifies an intrinsically oversized record
// before transient queue pressure, refreshes operator-managed quarantine state
// only on pressure, and opportunistically reclaims a fully acknowledged active
// segment. It is called before sequence allocation, so every failure is a clean
// no-op. Callers hold q.mu.
func (q *queue) ensureRecordCapacityLocked(recordSize uint64) error {
	if q.opts.MaxQueueBytes == 0 {
		return nil
	}
	// A record that cannot fit even an empty queue is terminal, not backpressure:
	// reporting ErrQueueFull would make the daemon retry it forever.
	if recordSize > q.opts.MaxQueueBytes {
		return ErrBatchTooLarge
	}
	if !exceedsByteLimit(q.storageBytesLocked(), recordSize, q.opts.MaxQueueBytes) {
		return nil
	}
	// Quarantine cleanup is an explicit operator action. Refresh only on pressure
	// so removal can unblock a running queue without adding a directory scan to
	// the healthy append path.
	if q.quarantinedBytes > 0 {
		if err := q.refreshQuarantineStatsForCapacityLocked(); err != nil {
			return err
		}
	}
	if err := q.reclaimAckedActiveForCapacityLocked(); err != nil {
		return err
	}
	if exceedsByteLimit(q.storageBytesLocked(), recordSize, q.opts.MaxQueueBytes) {
		return ErrQueueFull
	}
	return nil
}

func (q *queue) storageBytesLocked() uint64 {
	if q.quarantinedBytes > math.MaxUint64-q.physicalBytes {
		return math.MaxUint64
	}
	return q.physicalBytes + q.quarantinedBytes
}

// reclaimAckedActiveForCapacityLocked seals and removes an active segment only
// when every sequence it ever contained is already covered by the durable ack
// high-water. It is invoked on physical capacity pressure, avoiding one segment
// per batch during a healthy append/ack stream while still making MaxQueueBytes
// a true upper bound on live .wal files.
func (q *queue) reclaimAckedActiveForCapacityLocked() error {
	if q.active == nil || q.activeSeg == nil || q.activeSize == 0 ||
		q.activeSeg.lastSeq > q.lastAcked {
		return nil
	}
	if err := q.sealActiveLocked(); err != nil {
		return err
	}
	return q.reclaimLocked()
}

// writeRecordLocked rotates if needed, ensures an active segment, and appends
// record, returning the live segment name and the payload offset of the record.
func (q *queue) writeRecordLocked(seq uint64, record []byte) (string, int64, error) {
	recordSize := int64(len(record))

	rotateAtLimit := q.opts.SegmentMaxBytes > 0 &&
		q.opts.SegmentMaxBytes <= math.MaxInt64 &&
		(recordSize > int64(q.opts.SegmentMaxBytes) ||
			q.activeSize > int64(q.opts.SegmentMaxBytes)-recordSize)
	if q.active != nil && q.activeSize > 0 && rotateAtLimit {
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
	if written, err := q.active.Write(record); err != nil || written != len(record) {
		// A partial write leaves a corrupt tail; abandon this segment so no
		// further records are appended after the damage. The tail is quarantined
		// on the next Open.
		_ = q.sealActiveLocked()
		if err == nil {
			err = io.ErrShortWrite
		}
		q.syncErr = err
		return "", 0, fmt.Errorf("collector/wal: write record: %w", err)
	}
	q.activeSize += recordSize
	q.physicalBytes += uint64(recordSize)
	q.activeDirty = true
	q.activeSeg.lastSeq = seq
	q.activeSeg.size += uint64(recordSize)
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
	syncErr := q.syncActiveLocked()
	closeErr := q.active.Close()
	if q.activeSeg != nil {
		q.activeSeg.sealed = true
	}
	q.active = nil
	q.activeSeg = nil
	q.activeSize = 0
	q.activeDirty = false
	if syncErr != nil {
		q.syncErr = syncErr
		return fmt.Errorf("collector/wal: fsync on seal: %w", syncErr)
	}
	if closeErr != nil {
		return fmt.Errorf("collector/wal: close on seal: %w", closeErr)
	}
	q.syncErr = nil
	return nil
}

func (q *queue) syncActiveLocked() error {
	if q.syncFile != nil {
		return q.syncFile(q.active)
	}
	return q.active.Sync()
}

// NextBatch implements Queue.NextBatch.
func (q *queue) NextBatch(ctx context.Context) (*opensplunk.EventBatch, error) {
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
func (q *queue) readBatch(d batchDesc) (*opensplunk.EventBatch, error) {
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
	// holding q.mu even when the final checkpoint set coalesces to one
	// input/file key.
	plan.markGroups = make([][]SourceCheckpointMark, 0, markGroupCount)
	for _, d := range q.unacked[:prefixLength] {
		if len(d.sourceMarks) > 0 {
			plan.markGroups = append(plan.markGroups, d.sourceMarks)
		}
	}
	return plan, nil
}

// aggregateAckPlan performs the potentially expensive input/file-key hash
// aggregation and deterministic sort without holding the queue mutex. The mark
// arrays are immutable after Append/recovery, so a concurrent ack may discard
// its descriptor without invalidating this snapshot.
func aggregateAckPlan(plan ackPlan) AckPreview {
	var aggregator sourceMarkAggregator
aggregate:
	for _, marks := range plan.markGroups {
		for _, mark := range marks {
			if !aggregator.add(mark) {
				break aggregate
			}
		}
	}
	return AckPreview{
		ThroughBatchSequence: plan.throughBatchSequence,
		BatchCount:           plan.batchCount,
		Marks:                aggregator.marks(),
	}
}

type sourceMarkAggregator struct {
	bySource map[sourceMarkKey]SourceCheckpointMark
	failure  SourceCheckpointMark
	failed   bool
}

// sourceMarkKey keeps inputs independent even when they intentionally or
// accidentally observe the same physical file generation. A struct avoids the
// delimiter collisions possible with concatenated string keys.
type sourceMarkKey struct {
	InputID      string
	FileIdentity string
}

func (aggregator *sourceMarkAggregator) add(mark SourceCheckpointMark) bool {
	// Preserve the first malformed mark verbatim so the daemon fails closed
	// without retaining every malformed event in a hostile WAL.
	if mark.InputID == "" || mark.FileIdentity == "" ||
		!mark.HasEndOffset || mark.ConflictingMetadata ||
		mark.HasGuardFingerprint != mark.HasGuardLength {
		aggregator.failure = mark
		aggregator.failed = true
		return false
	}
	if aggregator.bySource == nil {
		aggregator.bySource = make(map[sourceMarkKey]SourceCheckpointMark)
	}
	key := sourceMarkKey{InputID: mark.InputID, FileIdentity: mark.FileIdentity}
	current, ok := aggregator.bySource[key]
	if ok && current.HasFingerprintLength && mark.HasFingerprintLength &&
		current.FingerprintLength != mark.FingerprintLength {
		mark.ConflictingMetadata = true
		aggregator.failure = mark
		aggregator.failed = true
		return false
	}
	if ok && sourceLineCursorsConflict(current, mark) {
		mark.ConflictingMetadata = true
		aggregator.failure = mark
		aggregator.failed = true
		return false
	}
	if ok && sourceGuardsConflict(current, mark) {
		mark.ConflictingMetadata = true
		aggregator.failure = mark
		aggregator.failed = true
		return false
	}
	if !ok || sourceMarkShouldReplace(current, mark) {
		aggregator.bySource[key] = mark
	}
	return true
}

func (aggregator *sourceMarkAggregator) marks() []SourceCheckpointMark {
	if aggregator.failed {
		return []SourceCheckpointMark{aggregator.failure}
	}
	marks := make([]SourceCheckpointMark, 0, len(aggregator.bySource))
	for _, mark := range aggregator.bySource {
		marks = append(marks, mark)
	}
	sort.Slice(marks, func(i, j int) bool {
		return sourceCheckpointMarkLess(marks[i], marks[j])
	})
	return marks
}

// checkpointMarksForBatch extracts a compact high-water mark per input and
// exact file identity while the EventBatch is already resident (Append or
// recovery). Invalid file origins retain one representative mark so
// acknowledgment later fails closed instead of silently dropping source
// coordinates.
func checkpointMarksForBatch(batchSequence uint64, events []*opensplunk.LogEvent) []SourceCheckpointMark {
	bySource := make(map[sourceMarkKey]SourceCheckpointMark)
	var invalid *SourceCheckpointMark
	for eventIndex, event := range events {
		if event == nil || event.GetOrigin() == nil {
			continue
		}
		origin := event.GetOrigin()
		mark := SourceCheckpointMark{
			BatchSequence:        batchSequence,
			EventIndex:           uint32(eventIndex),
			InputID:              origin.GetInputId(),
			FileIdentity:         origin.GetFileIdentity(),
			SourcePath:           origin.GetSourcePath(),
			EndOffset:            origin.GetEndOffset(),
			LineNumber:           origin.GetLineNumber(),
			NextLineNumber:       origin.GetNextLineNumber(),
			FingerprintLength:    origin.GetFileFingerprintLength(),
			GuardFingerprint:     origin.GetCheckpointGuardFingerprint(),
			GuardLength:          origin.GetCheckpointGuardLength(),
			HasSourcePath:        origin.SourcePath != nil,
			HasEndOffset:         origin.EndOffset != nil,
			HasNextLineNumber:    origin.NextLineNumber != nil,
			HasFingerprintLength: origin.FileFingerprintLength != nil,
			HasGuardFingerprint:  origin.CheckpointGuardFingerprint != nil,
			HasGuardLength:       origin.CheckpointGuardLength != nil,
		}
		if mark.InputID == "" || mark.FileIdentity == "" || !mark.HasEndOffset ||
			mark.HasGuardFingerprint != mark.HasGuardLength {
			mark.ConflictingMetadata = mark.HasGuardFingerprint != mark.HasGuardLength
			if invalid == nil {
				invalidMark := mark
				invalid = &invalidMark
			}
			continue
		}
		key := sourceMarkKey{InputID: mark.InputID, FileIdentity: mark.FileIdentity}
		current, ok := bySource[key]
		if ok && current.ConflictingMetadata {
			continue
		}
		if ok && current.HasFingerprintLength && mark.HasFingerprintLength &&
			current.FingerprintLength != mark.FingerprintLength {
			mark.ConflictingMetadata = true
			bySource[key] = mark
			continue
		}
		if ok && sourceLineCursorsConflict(current, mark) {
			mark.ConflictingMetadata = true
			bySource[key] = mark
			continue
		}
		if ok && sourceGuardsConflict(current, mark) {
			mark.ConflictingMetadata = true
			bySource[key] = mark
			continue
		}
		if !ok || sourceMarkShouldReplace(current, mark) {
			bySource[key] = mark
		}
	}
	marks := make([]SourceCheckpointMark, 0, len(bySource)+1)
	if invalid != nil {
		marks = append(marks, *invalid)
	}
	for _, mark := range bySource {
		marks = append(marks, mark)
	}
	sort.Slice(marks, func(i, j int) bool {
		return sourceCheckpointMarkLess(marks[i], marks[j])
	})
	return marks
}

func sourceCheckpointMarkLess(left, right SourceCheckpointMark) bool {
	if left.InputID != right.InputID {
		return left.InputID < right.InputID
	}
	if left.FileIdentity != right.FileIdentity {
		return left.FileIdentity < right.FileIdentity
	}
	return left.EventIndex < right.EventIndex
}

// sourceMarkShouldReplace chooses the furthest coordinate, preferring richer
// optional metadata at an equal coordinate so a later legacy record cannot
// erase a restart guard (or another field) recovered from an earlier record.
func sourceMarkShouldReplace(current, candidate SourceCheckpointMark) bool {
	if candidate.EndOffset != current.EndOffset {
		return candidate.EndOffset > current.EndOffset
	}
	for _, presence := range [][2]bool{
		{candidate.HasNextLineNumber, current.HasNextLineNumber},
		{candidate.HasGuardFingerprint, current.HasGuardFingerprint},
		{candidate.HasFingerprintLength, current.HasFingerprintLength},
		{candidate.HasSourcePath, current.HasSourcePath},
	} {
		if presence[0] != presence[1] {
			return presence[0]
		}
	}
	if candidate.BatchSequence != current.BatchSequence {
		return candidate.BatchSequence > current.BatchSequence
	}
	return candidate.EventIndex >= current.EventIndex
}

// sourceLineCursorsConflict reports metadata that cannot describe one
// forward-only source generation. Equal byte positions require equal cursors;
// a greater byte position requires a strictly greater next-line cursor.
func sourceLineCursorsConflict(left, right SourceCheckpointMark) bool {
	if !left.HasNextLineNumber || !right.HasNextLineNumber {
		return false
	}
	switch {
	case left.EndOffset == right.EndOffset:
		return left.NextLineNumber != right.NextLineNumber
	case left.EndOffset < right.EndOffset:
		return left.NextLineNumber >= right.NextLineNumber
	default:
		return left.NextLineNumber <= right.NextLineNumber
	}
}

func sourceGuardsConflict(left, right SourceCheckpointMark) bool {
	if left.HasGuardFingerprint != left.HasGuardLength ||
		right.HasGuardFingerprint != right.HasGuardLength {
		return true
	}
	if left.EndOffset != right.EndOffset ||
		!left.HasGuardFingerprint || !right.HasGuardFingerprint {
		return false
	}
	return left.GuardFingerprint != right.GuardFingerprint ||
		left.GuardLength != right.GuardLength
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
	var removeErr error
	for index, seg := range q.segments {
		if !seg.sealed || seg.lastSeq > q.lastAcked {
			break
		}
		if seg.size > q.physicalBytes {
			accounted := q.physicalBytes
			// q.segments is the authoritative inventory of live WAL files. Restore
			// at least their known size before returning so a broken subtraction
			// invariant cannot turn into apparent capacity on a later Append.
			q.physicalBytes = segmentPhysicalBytes(q.segments[index:])
			removeErr = fmt.Errorf(
				"collector/wal: reclaim segment %s: physical accounting invariant violated: segment size %d exceeds accounted bytes %d",
				seg.name, seg.size, accounted,
			)
			break
		}
		remove := q.removeFile
		if remove == nil {
			remove = os.Remove
		}
		if err := remove(filepath.Join(q.dir, seg.name)); err != nil && !errors.Is(err, os.ErrNotExist) {
			removeErr = fmt.Errorf("collector/wal: reclaim segment %s: %w", seg.name, err)
			break
		}
		q.physicalBytes -= seg.size
		reclaimed++
	}
	if reclaimed > 0 {
		q.segments = append(q.segments[:0], q.segments[reclaimed:]...)
		syncDirectory := q.syncDirectory
		if syncDirectory == nil {
			syncDirectory = fsyncDir
		}
		if err := syncDirectory(q.dir); err != nil {
			return errors.Join(removeErr, fmt.Errorf("collector/wal: fsync dir after reclaim: %w", err))
		}
	}
	return removeErr
}

func segmentPhysicalBytes(segments []*segInfo) uint64 {
	var total uint64
	for _, seg := range segments {
		if seg.size > math.MaxUint64-total {
			return math.MaxUint64
		}
		total += seg.size
	}
	return total
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
	lastSyncError := ""
	if q.syncErr != nil {
		lastSyncError = q.syncErr.Error()
	}
	return Stats{
		QueuedBatches:          uint64(len(q.unacked)),
		QueuedEvents:           q.queuedEvents,
		QueuedBytes:            q.liveBytes,
		OldestEventAge:         oldest,
		NextBatchSequence:      q.nextSeq,
		LastAckedBatchSequence: q.lastAcked,
		QuarantinedSegments:    q.quarantined,
		PhysicalBytes:          q.physicalBytes,
		QuarantinedBytes:       q.quarantinedBytes,
		OnDiskBytes:            q.storageBytesLocked(),
		LastSyncError:          lastSyncError,
		RecoveryWarning:        q.recoveryWarning,
	}
}

// PendingSourceMarks implements ResumeQueue.PendingSourceMarks. Startup is its only
// production caller, so aggregating under the lock avoids retaining one
// transient slice header per queued batch without adding runtime contention.
func (q *queue) PendingSourceMarks() ([]SourceCheckpointMark, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return nil, ErrClosed
	}
	var aggregator sourceMarkAggregator
	for _, descriptor := range q.unacked {
		for _, mark := range descriptor.sourceMarks {
			if !aggregator.add(mark) {
				return aggregator.marks(), nil
			}
		}
	}
	return aggregator.marks(), nil
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
	persist := q.persistMetaFile
	if persist == nil {
		persist = writeMeta
	}
	return persist(q.dir, walMeta{
		FormatVersion:          currentFormatVersion,
		NextBatchSequence:      q.nextSeq,
		LastAckedBatchSequence: q.lastAcked,
	})
}

// sealActiveAfterSequenceBurnLocked preserves the invariant that every live
// segment contains a contiguous run of allocated sequences. Once meta may have
// advanced but no record was written, later appends must start a segment whose
// filename publishes their new first sequence; otherwise record-count recovery
// cannot reconstruct a missing meta file without trusting corrupt payloads.
func (q *queue) sealActiveAfterSequenceBurnLocked(cause error) error {
	if err := q.sealActiveLocked(); err != nil {
		return errors.Join(cause, err)
	}
	return cause
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
				if err := q.syncActiveLocked(); err == nil {
					q.activeDirty = false
					q.syncErr = nil
				} else {
					q.syncErr = err
				}
			}
			q.mu.Unlock()
		}
	}
}

// uncompressedEventBytes is the deterministic sum of protobuf-encoded event
// sizes stamped into EventBatch.uncompressed_size_bytes, matching the server's
// UncompressedEventBytes.
func uncompressedEventBytes(events []*opensplunk.LogEvent) uint64 {
	var total uint64
	for _, event := range events {
		protoBytes := proto.Size(event)
		if protoBytes < 0 {
			return math.MaxUint64
		}

		size := uint64(protoBytes)
		if ^uint64(0)-total < size {
			return ^uint64(0)
		}
		total += size
	}
	return total
}
