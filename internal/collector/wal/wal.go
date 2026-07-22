package wal

import (
	"context"
	"errors"
	"time"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
)

// Sentinel errors. Callers classify them with errors.Is.
var (
	// ErrQueueFull is returned by Append when accepting the batch would exceed
	// Options.MaxQueueBytes. The daemon must apply input backpressure, not drop.
	ErrQueueFull = errors.New("collector/wal: durable queue is full")

	// ErrCorruptSegment reports that a segment failed CRC validation during
	// recovery and its unreadable tail was quarantined. Recovery continues.
	ErrCorruptSegment = errors.New("collector/wal: corrupt segment quarantined")

	// ErrClosed is returned once the queue has been closed.
	ErrClosed = errors.New("collector/wal: queue is closed")

	// errNotImplemented is returned by contract stubs during the skeleton phase.
	errNotImplemented = errors.New("collector/wal: not implemented")
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

	// Ack marks the batch durably delivered. Segments whose every batch is acked
	// are reclaimed. Acking an already-acked or unknown sequence is a no-op.
	Ack(batchSequence uint64) error

	// Stats returns the current queue-depth snapshot.
	Stats() Stats

	// Close flushes and releases the queue.
	Close() error
}

// Open opens or creates the durable queue described by opts, replaying and
// validating existing segments (quarantining any corrupt tail).
func Open(opts Options) (Queue, error) {
	return nil, errNotImplemented
}

// ComputeEventIDsDigest returns the event_ids_sha256 value defined by
// collector.proto: SHA-256 over each event's UTF-8 event_id, each prefixed by
// its unsigned 32-bit big-endian byte length. Exposed so the server-side and
// tests can compute the same digest.
func ComputeEventIDsDigest(events []*opensplunkv1.LogEvent) []byte {
	return nil
}
