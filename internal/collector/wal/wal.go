package wal

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"math"
	"time"

	"fortio.org/safecast"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
)

// Sentinel errors. Callers classify them with errors.Is.
var (
	// ErrQueueFull is returned by Append when accepting the batch would exceed
	// Options.MaxQueueBytes. The daemon must apply input backpressure, not drop.
	ErrQueueFull = errors.New("collector/wal: durable queue is full")

	// ErrBatchTooLarge is returned by Append when a single batch's on-disk record
	// exceeds Options.MaxQueueBytes or the WAL format's stable per-record ceiling:
	// it can never fit even in an empty queue, so retrying as backpressure would
	// wedge the pipeline forever. It is a terminal condition the daemon resolves
	// by splitting or dead-lettering the batch, and is distinct from ErrQueueFull
	// (which is transient backpressure).
	ErrBatchTooLarge = errors.New("collector/wal: batch record exceeds WAL capacity limit")

	// ErrCorruptSegment reports that a segment failed validation during recovery.
	// Its unreadable tail and all successor segments are quarantined; recovery
	// continues only from the globally intact prefix.
	ErrCorruptSegment = errors.New("collector/wal: corrupt segment quarantined")

	// ErrClosed is returned once the queue has been closed.
	ErrClosed = errors.New("collector/wal: queue is closed")

	// ErrInvalidAck reports an acknowledgment for a sequence the queue never
	// handed out (most importantly, a future sequence). Such an ack must never
	// advance the durable high-water mark.
	ErrInvalidAck = errors.New("collector/wal: invalid acknowledgment")
)

// SyncPolicy selects the fsync cadence for durability versus throughput.
type SyncPolicy int

const (
	// SyncOnSeal fsyncs when a segment is sealed (default). Lowest overhead;
	// a crash may lose batches appended since the last seal.
	SyncOnSeal SyncPolicy = iota
	// SyncInterval fsyncs at most once per Options.SyncInterval.
	SyncInterval
	// SyncAlways fsyncs after every Append. Strongest durability, slowest.
	SyncAlways
)

// Options configures a Queue.
type Options struct {
	// Dir is the directory holding meta.json and segment files.
	Dir string
	// MaxQueueBytes bounds live WAL segments plus retained corrupt/quarantine
	// segment artifacts (0 = unbounded). A quarantine can therefore fail closed
	// until an operator archives/removes it; metadata files are not included.
	MaxQueueBytes uint64
	// SegmentMaxBytes is the target size at which a new segment is started.
	SegmentMaxBytes uint64
	// Sync selects the fsync policy.
	Sync SyncPolicy
	// SyncInterval is used when Sync == SyncInterval.
	SyncInterval time.Duration

	// CollectorID is stamped onto every sealed EventBatch so the sender
	// transmits it without further mutation.
	CollectorID string
}

// Stats is a point-in-time snapshot of queue depth. It maps onto the queue-
// depth fields of opensplunk.CollectorQueueStats (queued_events, queued_bytes,
// oldest_event_age); the sender contributes the delivery counters (sent,
// acknowledged, retried, rejected, dropped).
type Stats struct {
	QueuedBatches          uint64
	QueuedEvents           uint64
	QueuedBytes            uint64
	OldestEventAge         time.Duration
	NextBatchSequence      uint64
	LastAckedBatchSequence uint64

	// QuarantinedSegments counts corrupt segments and successors quarantined
	// behind the first recovery gap and renamed to .wal.corrupt siblings. It is the
	// documented mechanism by which a non-fatal [ErrCorruptSegment] event is
	// surfaced: Open never fails hard on corruption, so callers observe it here.
	//
	// This field is an additive extension to the frozen contract Stats struct;
	// existing callers that only read the queue-depth fields are unaffected.
	QuarantinedSegments uint64
	// PhysicalBytes is the current size of live .wal segment files. It includes
	// acknowledged bytes that remain in an active or partially acknowledged
	// segment. Options.MaxQueueBytes is enforced against PhysicalBytes plus
	// QuarantinedBytes.
	PhysicalBytes uint64
	// QuarantinedBytes is the size of retained .wal.corrupt forensic artifacts.
	// Those files are reported separately but count toward MaxQueueBytes so
	// forensic retention cannot grow the state directory outside its bound.
	QuarantinedBytes uint64
	// OnDiskBytes is the saturating sum of PhysicalBytes and QuarantinedBytes and
	// is the value compared with Options.MaxQueueBytes.
	OnDiskBytes uint64
	// LastSyncError is non-empty while an interval or explicit segment sync has
	// failed and no later successful sync has cleared the failure.
	LastSyncError string
	// RecoveryWarning describes the first corruption barrier repaired during this
	// Open. The quarantined counters remain visible across later restarts through
	// the retained forensic artifacts.
	RecoveryWarning string
}

// SourceCheckpointMark is the compact input-scoped source coordinate retained
// alongside a durable batch descriptor. It contains no event payload, so
// planning a large cumulative acknowledgment is bounded by the number of
// distinct (input, file-generation) pairs rather than the byte size of the WAL.
//
// Presence bits are deliberate. Older WAL records predate source_path,
// file_fingerprint_length, and next_line_number and can be reconciled against
// their discovery checkpoint, while a malformed new record must not be
// mistaken for a valid zero value.
type SourceCheckpointMark struct {
	InputID              string
	FileIdentity         string
	SourcePath           string
	BatchSequence        uint64
	EndOffset            uint64
	LineNumber           uint64
	NextLineNumber       uint64
	EventIndex           uint32
	FingerprintLength    uint32
	GuardFingerprint     string
	GuardLength          uint32
	HasSourcePath        bool
	HasEndOffset         bool
	HasNextLineNumber    bool
	HasFingerprintLength bool
	HasGuardFingerprint  bool
	HasGuardLength       bool
	ConflictingMetadata  bool
}

// AckPreview is a read-only plan for the newly contiguous terminal WAL prefix.
// Marks are coalesced to the highest source coordinate per input and exact file
// identity.
type AckPreview struct {
	ThroughBatchSequence uint64
	BatchCount           uint64
	Marks                []SourceCheckpointMark
}

// Queue is the durable, at-least-once batch queue.
//
// Append is called by the daemon after events are decoded and processed;
// NextBatch and Ack are called by the sender. Implementations must be safe for
// concurrent use by one appender and one consumer.
type Queue interface {
	// Append seals events into a durable EventBatch, assigning a stable batch_id
	// and monotonic batch_sequence, computing event_ids_sha256 and size, and
	// persisting the record per the sync policy. It returns the sealed batch
	// ready for transmission, or ErrQueueFull when MaxQueueBytes is reached.
	Append(events []*opensplunk.LogEvent) (*opensplunk.EventBatch, error)

	// NextBatch blocks until an unacked batch is available or ctx is done,
	// returning batches in ascending batch_sequence. After a resume the first
	// batch returned is the lowest unacked sequence.
	NextBatch(ctx context.Context) (*opensplunk.EventBatch, error)

	// Ack marks exactly one batch terminal. Out-of-order terminal acks are held
	// in memory until every earlier queued batch is terminal; only then does the
	// durable cumulative high-water mark advance. Unknown/future sequences fail
	// with ErrInvalidAck. Replaying an already-durable ack is a no-op.
	Ack(batchSequence uint64) error

	// PrepareAck returns compact source marks for the batches that would newly
	// become part of the durable cumulative terminal prefix if batchSequence
	// were acknowledged now. It is read-only: queue contents, terminal state,
	// and the persisted high-water mark are unchanged.
	PrepareAck(batchSequence uint64) (AckPreview, error)

	// AckThrough applies an explicitly cumulative terminal response after every
	// covered batch's local outcome has been handled. Handshake resume hints are
	// advisory and must not drive deletion because they carry no rejection
	// details. The endpoint sequence must identify a real queued batch;
	// unknown/future values fail closed.
	AckThrough(batchSequence uint64) error

	// PrepareAckThrough is the read-only cumulative counterpart to AckThrough.
	PrepareAckThrough(batchSequence uint64) (AckPreview, error)

	// Rewind restarts delivery so NextBatch re-yields every unacked batch,
	// beginning again from the lowest unacked sequence. The sender must call it
	// when a delivery stream is (re)established: batches handed out on a
	// previous connection but never acknowledged would otherwise be stranded
	// behind the delivery cursor until process restart. Redelivering
	// sent-but-unacknowledged batches is exactly the at-least-once contract;
	// the server deduplicates by batch ID.
	Rewind()

	// Stats returns the current queue-depth snapshot.
	Stats() Stats

	// Close flushes and releases the queue.
	Close() error
}

// ResumeQueue is the concrete durable queue capability returned by Open. The
// narrower Queue interface is sufficient for senders and test doubles; daemon
// startup additionally needs the recovered source high-water marks.
type ResumeQueue interface {
	Queue
	// PendingSourceMarks returns the highest source coordinate retained by all
	// currently unacknowledged batches, coalesced per input and exact file
	// identity. It is read-only and is intended for startup resume before queue
	// consumers begin running.
	PendingSourceMarks() ([]SourceCheckpointMark, error)
}

// Open opens or creates the durable queue described by opts, replaying and
// validating existing segments.
//
// Corruption discovered during replay is non-fatal: the unreadable tail and
// every successor segment are quarantined, recovery continues with the global
// intact prefix, and the count is reported via Stats.QuarantinedSegments.
func Open(opts Options) (ResumeQueue, error) {
	return openQueue(opts)
}

// ComputeEventIDsDigest returns the event_ids_sha256 value defined by
// collector.proto: SHA-256 over each event's UTF-8 event_id, each prefixed by
// its unsigned 32-bit big-endian byte length. Exposed so the server-side and
// tests can compute the same digest.
func ComputeEventIDsDigest(events []*opensplunk.LogEvent) []byte {
	h := sha256.New()
	var length [4]byte
	for _, event := range events {
		id := ""
		if event != nil {
			id = event.GetEventId()
		}

		// is exactly representable as uint64.
		if uint64(len(id)) > math.MaxUint32 {
			return nil
		}

		binary.BigEndian.PutUint32(length[:], safecast.MustConv[uint32](len(id)))
		_, _ = h.Write(length[:])
		_, _ = h.Write([]byte(id))
	}
	return h.Sum(nil)
}
