// Package wal is the collector's segmented, disk-backed durable queue of event
// batches. It is the durability boundary of the collector: once Append returns,
// the batch survives process crashes and restarts and will be delivered
// at-least-once regardless of the state of the source file.
//
// # Batch identity
//
// The queue is the point at which batch identity is minted and frozen. Append
// takes a slice of already-decoded and processed events, sizes and seals them
// into an EventBatch, and assigns:
//
//   - batch_id: a fresh UUIDv4 string, stable forever for this batch;
//   - batch_sequence: a monotonic uint64 allocated from the persisted WAL meta
//     counter (never reused, never reordered);
//   - created_at: the seal time;
//   - uncompressed_size_bytes and event_ids_sha256, the latter computed per the
//     collector.proto contract (SHA-256 over each UTF-8 event_id prefixed by its
//     unsigned 32-bit big-endian byte length).
//
// The queue deals in *opensplunkv1.EventBatch directly rather than a redundant
// record struct: the sealed proto already carries exactly batch_id,
// batch_sequence, created_at, and events, the on-disk record IS the marshaled
// proto, and the sender transmits it without remapping. collector_id and the
// protocol version are stamped from [Options] at seal time. A retry resends the
// byte-identical batch, so the server can deduplicate.
//
// # On-disk format
//
// State lives under Options.Dir:
//
//	<dir>/meta.json                 next_batch_sequence, last_acked_batch_sequence, format_version
//	<dir>/segment-<seq20>.wal       append-only records; <seq20> is the zero-padded
//	                                20-digit batch_sequence of the segment's first batch
//	<dir>/segment-<seq20>.wal.corrupt  quarantined segment tail after a CRC failure
//
// Each record is length-prefixed and checksummed:
//
//	[uint32 big-endian payload length]
//	[uint32 big-endian CRC32C (Castagnoli) of the payload]
//	[payload: marshaled opensplunkv1.EventBatch]
//
// On open the queue replays segments in order, validating each record's CRC. A
// record that fails validation, or a truncated trailing record from a crash
// during append, terminates replay of that segment; the unreadable tail is
// renamed to a .corrupt sibling ([ErrCorruptSegment]) and recovery continues
// with the batches that were intact. A new segment is started when the current
// one reaches Options.SegmentMaxBytes.
//
// # Durability and reclamation
//
// [SyncPolicy] selects the fsync cadence (every record, on segment seal, or on
// an interval). Ack records that a batch has reached its server durability
// point; once every batch in a segment is acked the segment file is deleted and
// last_acked_batch_sequence advances in meta. NextBatch yields unacked batches
// in ascending batch_sequence for in-order delivery and resume.
//
// # Backpressure
//
// Options.MaxQueueBytes bounds the total on-disk size. When an Append would
// exceed it, Append returns [ErrQueueFull]; the daemon reacts by pausing input
// reads (backpressure to the files, which persist) rather than dropping data.
//
// # Dependency direction
//
// wal imports only the standard library and the generated protobuf types. It is
// imported by the sender (sender -> wal) and by the root daemon; it imports no
// other internal/collector package.
package wal
