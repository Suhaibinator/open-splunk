package collector

import (
	"context"
	"errors"
	"fmt"
	"time"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/collector/input"
	"github.com/Suhaibinator/open-splunk/internal/collector/sender"
	"github.com/Suhaibinator/open-splunk/internal/collector/wal"
	"google.golang.org/protobuf/proto"
)

// processedEvent is one decoded, processed event ready to be batched, carrying
// the durable source coordinates the batcher needs to advance checkpoints once
// the covering batch is durable.
type processedEvent struct {
	event      *opensplunkv1.LogEvent
	identity   input.FileIdentity
	path       string
	endOffset  uint64
	lineNumber uint64
	size       int
}

// checkpointMark is the highest durable position seen for one file identity
// within a single pending batch.
type checkpointMark struct {
	identity   input.FileIdentity
	path       string
	offset     uint64
	lineNumber uint64
}

// pendingBatch accumulates processed events and, per file identity, the highest
// EndOffset the batch covers so checkpoints can advance atomically after the
// batch is durable.
type pendingBatch struct {
	items  []processedEvent
	events []*opensplunkv1.LogEvent
	bytes  int
	marks  map[string]checkpointMark
}

func (b *pendingBatch) empty() bool { return len(b.events) == 0 }

func (b *pendingBatch) add(pe processedEvent) {
	b.items = append(b.items, pe)
	b.events = append(b.events, pe.event)
	b.bytes += pe.size
	if b.marks == nil {
		b.marks = make(map[string]checkpointMark)
	}
	key := pe.identity.TrackingKey()
	if m, ok := b.marks[key]; !ok || m.identity.String() != pe.identity.String() || pe.endOffset > m.offset {
		b.marks[key] = checkpointMark{identity: pe.identity, path: pe.path, offset: pe.endOffset, lineNumber: pe.lineNumber}
	}
}

func (b *pendingBatch) reset() {
	b.items = nil
	b.events = nil
	b.bytes = 0
	b.marks = nil
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
// channel closes (its Manager stopped) or ctx is cancelled. Decode failures and
// policy drops are handled here, never propagated as fatal.
func (d *Daemon) readInput(ctx context.Context, ir *inputRuntime, processed chan<- processedEvent) {
	for raw := range ir.manager.Events() {
		pos := SourcePosition{
			FileIdentity: raw.Source.Identity.String(),
			StartOffset:  raw.Source.StartOffset,
			EndOffset:    raw.Source.EndOffset,
			LineNumber:   raw.Source.LineNumber,
		}
		event, err := ir.decoder.Decode(raw.Bytes, pos, d.now())
		if err != nil {
			d.recordDecodeFailure(ir.id, raw.Source, len(raw.Bytes), err)
			continue
		}
		out, err := ir.pipeline.Process(event)
		if err != nil {
			// A pipeline error is a configuration/logic fault, not a per-event
			// rejection. Log and skip; do not stall the whole input.
			d.log.Error("collector: processor pipeline failed", "input", ir.id, "error", err.Error())
			continue
		}
		if out == nil {
			continue // dropped by an allow/deny processor
		}
		pe := processedEvent{
			event:      out,
			identity:   raw.Source.Identity,
			path:       raw.Source.Path,
			endOffset:  raw.Source.EndOffset,
			lineNumber: raw.Source.LineNumber,
			size:       proto.Size(out),
		}
		select {
		case processed <- pe:
		case <-ctx.Done():
			return
		}
	}
}

// recordDecodeFailure counts and logs a skipped record per the decode-failure
// policy. The raw payload is never logged (only its length), because a source
// line may carry secret material.
func (d *Daemon) recordDecodeFailure(inputID string, src input.SourceRef, n int, err error) {
	d.decodeFailures.Add(1)
	d.log.Warn("collector: skipping undecodable record",
		"input", inputID,
		"file_identity", src.Identity.String(),
		"start_offset", src.StartOffset,
		"end_offset", src.EndOffset,
		"line", src.LineNumber,
		"bytes", n,
		"error", err.Error(),
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

// flush appends the pending batch to the durable queue and, on success, advances
// the covered checkpoints. ErrQueueFull is transient backpressure and is
// retried; ErrBatchTooLarge is split (or durably dead-lettered when it contains
// one event). Any other append error is fatal and stops the daemon. Continuing
// after an IO/marshal failure would hide that data is not becoming durable.
func (d *Daemon) flush(ctx context.Context, b *pendingBatch) error {
	if b.empty() {
		return nil
	}
	var graceDeadline time.Time
	for {
		batch, err := d.queue.Append(b.events)
		if err == nil {
			d.advanceCheckpoints(b)
			d.log.Debug("collector: batch appended",
				"batch_sequence", batch.GetBatchSequence(), "events", len(b.events), "bytes", b.bytes)
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
				graceDeadline = time.Now().Add(d.shutdownFlushGrace)
			}
			remaining := time.Until(graceDeadline)
			if remaining <= 0 {
				d.log.Warn("collector: queue full at shutdown; events left for re-read",
					"events", len(b.events))
				b.reset()
				return nil
			}
			// ctx is already cancelled, so a ctx-aware select would fall through
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
		return d.flush(ctx, second)
	}
	if err := d.deadLetterOversized(b); err != nil {
		return err
	}
	d.advanceCheckpoints(b)
	b.reset()
	return nil
}

// deadLetterOversized records the single un-queueable event to the dead-letter
// sink under BATCH_TOO_LARGE_FOR_QUEUE and counts it.
func (d *Daemon) deadLetterOversized(b *pendingBatch) error {
	d.log.Error("collector: event batch record exceeds max_queue_bytes; dead-lettering and dropping",
		"events", len(b.events))
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

// advanceCheckpoints persists, for every file identity the just-durable batch
// covered, the highest EndOffset in that batch. Offsets are monotonic per
// identity: a mark that does not advance the last persisted offset is skipped.
func (d *Daemon) advanceCheckpoints(b *pendingBatch) {
	for _, m := range b.marks {
		generationKey := m.identity.String()
		if last, ok := d.lastOffsets[generationKey]; ok && m.offset <= last {
			continue
		}
		cp := input.Checkpoint{
			Identity:   m.identity,
			Path:       m.path,
			Offset:     m.offset,
			LineNumber: m.lineNumber,
		}
		if err := d.checkpoints.Set(cp); err != nil {
			d.log.Error("collector: checkpoint persist failed",
				"file_identity", generationKey, "offset", m.offset, "error", err.Error())
			continue
		}
		d.lastOffsets[generationKey] = m.offset
	}
}
