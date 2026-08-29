package collector

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.uber.org/zap"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/collector/input"
	"github.com/Suhaibinator/open-splunk/internal/collector/sender"
	"github.com/Suhaibinator/open-splunk/internal/collector/wal"
	"google.golang.org/protobuf/proto"
)

// processedEvent is one decoded, processed event ready to be batched. Its
// source coordinates are already encoded in EventOrigin for acknowledgment-
// coupled checkpointing; the explicit fields remain for the local terminal
// path when one oversized event cannot enter the WAL.
type processedEvent struct {
	event *opensplunk.LogEvent
	// durabilityBarrier is a zero-payload input control record. The batcher
	// acknowledges it only after every earlier processed event has crossed the
	// durable queue boundary (or completed a deliberate terminal disposition).
	durabilityBarrier input.RawEvent
	inputID           string
	identity          input.FileIdentity
	path              string
	endOffset         uint64
	lineNumber        uint64
	nextLineNumber    uint64
	guardFingerprint  string
	guardLength       uint32
	size              int
}

// checkpointMark is the highest source position seen for one input-scoped file
// identity within a pending batch that may need local oversized-event
// disposition.
type checkpointMark struct {
	inputID          string
	identity         input.FileIdentity
	path             string
	offset           uint64
	lineNumber       uint64
	nextLineNumber   uint64
	guardFingerprint string
	guardLength      uint32
}

// pendingBatch accumulates processed events. Ordinary queued events advance
// from WAL-cached EventOrigin metadata after terminal acknowledgment. Compact
// source marks are derived lazily only for the rare local dead-letter path, so
// ordinary ingestion does not pay for an otherwise-unused map insertion per
// event.
type pendingBatch struct {
	items  []processedEvent
	events []*opensplunk.LogEvent
	bytes  int
}

func (b *pendingBatch) empty() bool { return len(b.events) == 0 }

func (b *pendingBatch) add(pe processedEvent) {
	b.items = append(b.items, pe)
	b.events = append(b.events, pe.event)
	b.bytes += pe.size
}

func (b *pendingBatch) reset() {
	b.items = nil
	b.events = nil
	b.bytes = 0
}

func (b *pendingBatch) checkpointMarks() map[inputFileKey]checkpointMark {
	marks := make(map[inputFileKey]checkpointMark)
	for _, pe := range b.items {
		key := inputFileTrackingKey(pe.inputID, pe.identity)
		if current, ok := marks[key]; !ok ||
			current.identity.String() != pe.identity.String() ||
			pe.endOffset > current.offset {
			marks[key] = checkpointMark{
				inputID: pe.inputID, identity: pe.identity, path: pe.path,
				offset: pe.endOffset, lineNumber: pe.lineNumber,
				nextLineNumber:   pe.nextLineNumber,
				guardFingerprint: pe.guardFingerprint, guardLength: pe.guardLength,
			}
		}
	}
	return marks
}

// split divides the batch into two halves that together cover exactly the same
// events and checkpoint marks. It is used to make progress on a batch the
// durable queue rejects as ErrBatchTooLarge.
func (b *pendingBatch) split() (*pendingBatch, *pendingBatch) {
	mid := len(b.items) / 2
	first := &pendingBatch{}
	for _, pe := range b.items[:mid] {
		first.add(pe)
	}
	second := &pendingBatch{}
	for _, pe := range b.items[mid:] {
		second.add(pe)
	}
	return first, second
}

// readInput consumes one input's RawEvents, decoding and processing each and
// forwarding survivors to the batcher. It returns when the input's Events
// channel closes (its Manager stopped) or ctx is canceled. Decode failures and
// policy drops are handled here, never propagated as fatal.
func (d *Daemon) readInput(ctx context.Context, ir *inputRuntime, processed chan<- processedEvent) {
	for raw := range ir.manager.Events() {
		if raw.IsDurabilityBarrier() {
			select {
			case processed <- processedEvent{durabilityBarrier: raw}:
			case <-ctx.Done():
				return
			}
			continue
		}
		pos := SourcePosition{
			FileIdentity:               raw.Source.Identity.String(),
			SourcePath:                 raw.Source.Path,
			FileFingerprintLength:      raw.Source.Identity.FingerprintLength,
			CheckpointGuardFingerprint: raw.Source.GuardFingerprint,
			CheckpointGuardLength:      raw.Source.GuardLength,
			StartOffset:                raw.Source.StartOffset,
			EndOffset:                  raw.Source.EndOffset,
			LineNumber:                 raw.Source.LineNumber,
			NextLineNumber:             raw.Source.NextLineNumber,
		}
		event, err := ir.decoder.Decode(raw.Bytes, pos, d.now())
		if err != nil {
			d.recordDecodeFailure(ir.id, raw.Source, len(raw.Bytes))
			continue
		}
		// Sanitize direct policies and the exact origins of rename values that
		// actually reach sensitive names before any local durability boundary.
		event = d.redactor.beforePipeline(event, ir.decoder.constantNames)
		out, err := ir.pipeline.Process(event)
		if err != nil {
			// A pipeline error is a configuration/logic fault, not a per-event
			// rejection. Log and skip; do not stall the whole input.
			d.log.Error("collector: processor pipeline failed", zap.String("input", ir.id), zap.Error(err))
			d.pipelineFailures.Add(1)
			continue
		}
		if out == nil {
			d.policyDrops.Add(1)
			continue // dropped by an allow/deny processor
		}
		pe := processedEvent{
			event:            out,
			inputID:          ir.id,
			identity:         raw.Source.Identity,
			path:             raw.Source.Path,
			endOffset:        raw.Source.EndOffset,
			lineNumber:       raw.Source.LineNumber,
			nextLineNumber:   raw.Source.NextLineNumber,
			guardFingerprint: raw.Source.GuardFingerprint,
			guardLength:      raw.Source.GuardLength,
			size:             proto.Size(out),
		}
		select {
		case processed <- pe:
		case <-ctx.Done():
			return
		}
	}
}

// recordDecodeFailure counts and logs a skipped record per the decode-failure
// policy. Neither the raw payload nor the decoder error is logged: structural
// errors can contain attacker-controlled JSON field names.
func (d *Daemon) recordDecodeFailure(inputID string, src input.SourceRef, n int) {
	d.decodeFailures.Add(1)
	d.log.Warn("collector: skipping undecodable record",
		zap.String("input", inputID),
		zap.String("file_identity", src.Identity.String()),
		zap.Uint64("start_offset", src.StartOffset),
		zap.Uint64("end_offset", src.EndOffset),
		zap.Uint64("line", src.LineNumber),
		zap.Int("bytes", n),
		zap.String("reason", "decode_error"),
	)
}

// runBatcher accumulates processed events into batches and flushes them to the
// durable queue by max event count, max byte size, or linger delay. It runs
// until processed is closed, then flushes the final partial batch and returns.
// Because flush blocks while the queue is full, the single batcher goroutine is
// the point at which backpressure propagates upstream.
func (d *Daemon) runBatcher(ctx context.Context, processed <-chan processedEvent) error {
	b := &pendingBatch{}
	var linger *time.Timer
	var lingerC <-chan time.Time
	stopLinger := func() {
		if linger != nil {
			linger.Stop()
			linger = nil
			lingerC = nil
		}
	}

	for {
		select {
		case pe, ok := <-processed:
			if !ok {
				stopLinger()
				return d.flush(ctx, b)
			}
			if pe.durabilityBarrier.IsDurabilityBarrier() {
				stopLinger()
				if err := d.flush(ctx, b); err != nil {
					return err
				}
				pe.durabilityBarrier.AcknowledgeDurabilityBarrier()
				continue
			}
			// Flush the existing batch before adding an event that would cross a
			// configured cap. A single over-cap event is still admitted alone so it
			// can receive a deterministic server rejection/dead-letter disposition.
			if !b.empty() && (len(b.events)+1 > d.batchMaxEvents || b.bytes+pe.size > d.batchMaxBytes) {
				stopLinger()
				if err := d.flush(ctx, b); err != nil {
					return err
				}
			}
			b.add(pe)
			if len(b.events) == 1 {
				linger = time.NewTimer(d.batchLinger)
				lingerC = linger.C
			}
			if len(b.events) >= d.batchMaxEvents || b.bytes >= d.batchMaxBytes {
				stopLinger()
				if err := d.flush(ctx, b); err != nil {
					return err
				}
			}
		case <-lingerC:
			linger = nil
			lingerC = nil
			if err := d.flush(ctx, b); err != nil {
				return err
			}
		}
	}
}

// flush appends the pending batch to the durable queue. A successful append
// deliberately does not advance source checkpoints: the sender owns that step
// after a terminal server disposition. ErrQueueFull is transient backpressure
// and is retried; ErrBatchTooLarge is split (or durably dead-lettered when it
// contains one event). Any other append error is fatal and stops the daemon.
func (d *Daemon) flush(ctx context.Context, b *pendingBatch) error {
	if b.empty() {
		return nil
	}
	var graceDeadline time.Time
	for {
		batch, err := d.queue.Append(b.events)
		if err == nil {
			d.log.Debug("collector: batch appended",
				zap.Uint64("batch_sequence", batch.GetBatchSequence()),
				zap.Int("events", len(b.events)),
				zap.Int("bytes", b.bytes))
			b.reset()
			return nil
		}

		// A batch whose single record can never fit the queue is terminal, not
		// backpressure: retrying it forever would wedge the pipeline.
		if errors.Is(err, wal.ErrBatchTooLarge) {
			return d.flushTooLarge(ctx, b)
		}
		if !errors.Is(err, wal.ErrQueueFull) {
			return fmt.Errorf("collector: durable append failed for %d events: %w", len(b.events), err)
		}

		// Bound queue-full backpressure once shutdown begins.
		if ctx.Err() != nil {
			if graceDeadline.IsZero() {
				graceDeadline = d.sharedShutdownFlushDeadline()
			}
			remaining := time.Until(graceDeadline)
			if remaining <= 0 {
				d.log.Warn("collector: queue full at shutdown; events left for re-read",
					zap.Int("events", len(b.events)))
				b.reset()
				// Cancellation is an explicit non-success result. In particular,
				// flushTooLarge must not continue with a later split after this
				// earlier source range was abandoned: doing so could dead-letter the
				// later range and checkpoint past bytes that never reached the WAL.
				return ctx.Err()
			}
			// ctx is already canceled, so a ctx-aware select would fall through
			// instantly and busy-spin re-marshaling; sleep a bounded plain interval.
			time.Sleep(minDuration(d.queueFullRetry, remaining))
			continue
		}

		timer := time.NewTimer(d.queueFullRetry)
		select {
		case <-timer.C:
		case <-ctx.Done():
			// React promptly to shutdown; the grace deadline is enforced above.
			timer.Stop()
		}
	}
}

// sharedShutdownFlushDeadline returns one deadline for every recursive and
// sequential flush attempted during this daemon's shutdown. A per-call grace
// period lets an oversized batch multiply the shutdown budget as it splits.
func (d *Daemon) sharedShutdownFlushDeadline() time.Time {
	if deadline := d.shutdownFlushDeadline.Load(); deadline != 0 {
		return time.Unix(0, deadline)
	}
	deadline := time.Now().Add(d.shutdownFlushGrace).UnixNano()
	if d.shutdownFlushDeadline.CompareAndSwap(0, deadline) {
		return time.Unix(0, deadline)
	}
	return time.Unix(0, d.shutdownFlushDeadline.Load())
}

// flushTooLarge resolves a batch the durable queue rejected as ErrBatchTooLarge.
// A multi-event batch is split in half and each half re-flushed recursively; a
// single un-queueable event is a deliberate policy drop: it is written to the
// dead-letter sink, counted, and its checkpoint marks are advanced so the drop
// does not strand the file's checkpoint behind it.
func (d *Daemon) flushTooLarge(ctx context.Context, b *pendingBatch) error {
	if len(b.items) > 1 {
		first, second := b.split()
		if err := d.flush(ctx, first); err != nil {
			return err
		}
		if err := d.flush(ctx, second); err != nil {
			return err
		}
		b.reset()
		return nil
	}
	if err := d.deadLetterOversized(b); err != nil {
		return err
	}
	if err := d.advanceCheckpoints(b); err != nil {
		return err
	}
	b.reset()
	return nil
}

// deadLetterOversized records the single un-queueable event to the dead-letter
// sink under BATCH_TOO_LARGE_FOR_QUEUE and counts it.
func (d *Daemon) deadLetterOversized(b *pendingBatch) error {
	d.log.Error("collector: event batch record exceeds max_queue_bytes; dead-lettering and dropping",
		zap.Int("events", len(b.events)))
	if d.deadLetter == nil {
		return errors.New("collector: no dead-letter sink for event exceeding max_queue_bytes")
	}
	records := make([]sender.DeadLetterRecord, 0, len(b.events))
	now := d.now()
	for _, ev := range b.events {
		records = append(records, sender.DeadLetterRecord{
			Event:      ev,
			Code:       "BATCH_TOO_LARGE_FOR_QUEUE",
			Reason:     "event batch record exceeds state.max_queue_bytes",
			RejectedAt: now,
		})
	}
	if err := d.deadLetter.WriteRecords(records); err != nil {
		return fmt.Errorf("collector: persist oversized event dead letter: %w", err)
	}
	d.oversizedDrops.Add(uint64(len(b.events)))
	return nil
}

// minDuration returns the smaller of two durations.
func minDuration(a, b time.Duration) time.Duration {
	if a < b {
		return a
	}
	return b
}

// advanceCheckpoints is reserved for oversized-at-append records that cannot
// enter the WAL and become terminal through their durable local dead-letter
// write. Ordinary queued batches advance only via commitTerminalCheckpoints.
func (d *Daemon) advanceCheckpoints(b *pendingBatch) error {
	// A WAL record with missing/legacy origin metadata cannot be attributed to a
	// specific source. Require the entire queue to be empty before bypassing WAL
	// ordering with a locally terminal dead-letter checkpoint. This path is rare,
	// and a deferred position is safely covered by a later queued event (or may be
	// dead-lettered again after restart).
	queueEmpty := d.queue.Stats().QueuedBatches == 0
	for _, m := range b.checkpointMarks() {
		if !queueEmpty {
			// The dead-letter record is durable, but advancing this later source
			// position would cross an earlier WAL-owned event. If that WAL record
			// were later quarantined, restart could no longer fall back to the
			// source. Leave the checkpoint behind; a later normally queued event
			// will cover this drop, or it may be dead-lettered again after restart.
			d.log.Warn("collector: deferring oversized-event checkpoint behind pending WAL",
				zap.String("input", m.inputID),
				zap.String("file_identity", m.identity.String()),
				zap.Uint64("offset", m.offset))
			continue
		}
		generationKey := inputFileGenerationKey(m.inputID, m.identity)
		if last, ok := d.lastOffsets[generationKey]; ok && m.offset <= last {
			continue
		}
		cp := input.Checkpoint{
			InputID:          m.inputID,
			Identity:         m.identity,
			Path:             m.path,
			Offset:           m.offset,
			LineNumber:       m.lineNumber,
			NextLineNumber:   m.nextLineNumber,
			GuardFingerprint: m.guardFingerprint,
			GuardLength:      m.guardLength,
		}
		if err := d.checkpoints.Set(cp); err != nil {
			return fmt.Errorf(
				"collector: persist terminal oversized-event checkpoint for input %q file %s at offset %d: %w",
				m.inputID, m.identity.String(), m.offset, err,
			)
		}
		d.lastOffsets[generationKey] = m.offset
	}
	return nil
}
