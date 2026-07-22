// Package sender delivers durable batches to the ingestion server over the
// bidirectional CollectorIngestService.Collect gRPC stream.
//
// A [Sender] consumes sealed batches from a [wal.Queue], transmits them, and
// applies the server's acknowledgments back to the queue and to a dead-letter
// sink. It owns the whole client side of the protocol: dial (TLS or explicit
// plaintext), bearer token in gRPC metadata, Hello/Ready negotiation, resume
// from CollectorReady.resume_after_batch_sequence, batch transmission bounded by
// the negotiated max_in_flight_batches / max_batch_events / max_batch_bytes,
// heartbeats at Ready.heartbeat_interval, Goodbye on shutdown, and reconnect
// with bounded exponential backoff plus jitter.
//
// # Acknowledgment handling
//
//   - BatchAck: the batch is terminal. Accepted and duplicate events are acked
//     to the queue (wal.Queue.Ack). Any per-event rejections are written to the
//     dead-letter sink and do not block the queue.
//   - BatchReject: the whole batch is permanently rejected; every event is
//     dead-lettered and the batch is acked off the queue.
//   - RetryBatch: non-terminal; the exact same durable batch is retained and
//     resent after retry_after.
//   - Throttle: adjusts send pacing and in-flight limits until effective_until.
//   - ServerNotice: informational / maintenance / shutdown signalling.
//
// The token is read from its file at dial time via the Options.Token callback
// and is never logged or retained beyond the gRPC call credentials.
//
// # Dead-letter file
//
// Rejected events are appended as JSON Lines, one [DeadLetterRecord] per line,
// so an operator can inspect or replay them without blocking live delivery.
//
// # Dependency direction
//
// sender imports wal (sender -> wal), the generated protobuf types, and gRPC.
// It is self-contained via its Options and does NOT import config, input,
// framing, or the root collector package; the daemon translates configuration
// into Options.
package sender
