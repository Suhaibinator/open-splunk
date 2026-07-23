package wal

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"time"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
)

// Sentinel errors. Callers classify them with errors.Is.
var (
	// ErrQueueFull is returned by Append when accepting the batch would exceed
	// Options.MaxQueueBytes. The daemon must apply input backpressure, not drop.
	ErrQueueFull = errors.New("collector/wal: durable queue is full")

	// ErrBatchTooLarge is returned by Append when a single batch's on-disk record
	// exceeds Options.MaxQueueBytes: it can never fit even in an empty queue, so
	// retrying as backpressure would wedge the pipeline forever. It is a terminal
	// condition the daemon resolves by splitting or dead-lettering the batch, and
	// is distinct from ErrQueueFull (which is transient backpressure).
	ErrBatchTooLarge = errors.New("collector/wal: batch record exceeds max_queue_bytes")

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
	// MaxQueueBytes bounds the total on-disk queue size (0 = unbounded).
	MaxQueueBytes uint64
	// SegmentMaxBytes is the target size at which a new segment is started.
	SegmentMaxBytes uint64
	// Sync selects the fsync policy.
	Sync SyncPolicy
	// SyncInterval is used when Sync == SyncInterval.
	SyncInterval time.Duration

	// CollectorID, ProtocolMajor, and ProtocolMinor are stamped onto every
	// sealed EventBatch so the sender transmits it without further mutation.
	CollectorID   string
	ProtocolMajor uint32
	ProtocolMinor uint32
}

// Stats is a point-in-time snapshot of queue depth. It maps onto the queue-
// depth fields of opensplunkv1.CollectorQueueStats (queued_events, queued_bytes,
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
}

// SourceCheckpointMark is the compact source coordinate retained alongside a
// durable batch descriptor. It contains no event payload, so planning a large
// cumulative acknowledgment is bounded by the number of distinct file
// generations rather than the byte size of the WAL.
//
// Presence bits are deliberate. Older WAL records predate source_path and
// file_fingerprint_length and can be reconciled against their discovery
// checkpoint, while a malformed new record must not be mistaken for a valid
// zero value.
type SourceCheckpointMark struct {
	BatchSequence        uint64
	EventIndex           uint32
	FileIdentity         string
	SourcePath           string
	EndOffset            uint64
	LineNumber           uint64
	FingerprintLength    uint32
	HasSourcePath        bool
	HasEndOffset         bool
	HasFingerprintLength bool
	ConflictingMetadata  bool
}

// AckPreview is a read-only plan for the newly contiguous terminal WAL prefix.
// Marks are coalesced to the highest source coordinate per exact file identity.
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
	Append(events []*opensplunkv1.LogEvent) (*opensplunkv1.EventBatch, error)

	// NextBatch blocks until an unacked batch is available or ctx is done,
	// returning batches in ascending batch_sequence. After a resume the first
	// batch returned is the lowest unacked sequence.
	NextBatch(ctx context.Context) (*opensplunkv1.EventBatch, error)

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

	// AckThrough applies an explicitly cumulative acknowledgment from the
	// protocol handshake or acknowledged_through field. The endpoint sequence
	// must identify a real queued batch; unknown/future values fail closed.
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

// Open opens or creates the durable queue described by opts, replaying and
// validating existing segments.
//
// Corruption discovered during replay is non-fatal: the unreadable tail and
// every successor segment are quarantined, recovery continues with the global
// intact prefix, and the count is reported via Stats.QuarantinedSegments.
func Open(opts Options) (Queue, error) {
	return openQueue(opts)
}

// ComputeEventIDsDigest returns the event_ids_sha256 value defined by
// collector.proto: SHA-256 over each event's UTF-8 event_id, each prefixed by
// its unsigned 32-bit big-endian byte length. Exposed so the server-side and
// tests can compute the same digest.
func ComputeEventIDsDigest(events []*opensplunkv1.LogEvent) []byte {
	h := sha256.New()
	var length [4]byte
	for _, event := range events {
		id := ""
		if event != nil {
			id = event.GetEventId()
		}
		binary.BigEndian.PutUint32(length[:], uint32(len(id)))
		_, _ = h.Write(length[:])
		_, _ = h.Write([]byte(id))
	}
	return h.Sum(nil)
}
