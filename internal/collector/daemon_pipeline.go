package collector

import (
	"context"
	"errors"
	"time"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/collector/input"
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
	events []*opensplunkv1.LogEvent
	bytes  int
	marks  map[string]checkpointMark
}

func (b *pendingBatch) empty() bool { return len(b.events) == 0 }

func (b *pendingBatch) add(pe processedEvent) {
	b.events = append(b.events, pe.event)
	b.bytes += pe.size
	if b.marks == nil {
		b.marks = make(map[string]checkpointMark)
	}
	key := pe.identity.String()
	if m, ok := b.marks[key]; !ok || pe.endOffset > m.offset {
		b.marks[key] = checkpointMark{identity: pe.identity, path: pe.path, offset: pe.endOffset, lineNumber: pe.lineNumber}
	}
}

func (b *pendingBatch) reset() {
	b.events = nil
	b.bytes = 0
	b.marks = nil
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
func (d *Daemon) runBatcher(ctx context.Context, processed <-chan processedEvent) {
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
				d.flush(ctx, b)
				return
			}
			b.add(pe)
			if len(b.events) == 1 {
				linger = time.NewTimer(d.batchLinger)
				lingerC = linger.C
			}
			if len(b.events) >= d.batchMaxEvents || b.bytes >= d.batchMaxBytes {
				stopLinger()
				d.flush(ctx, b)
			}
		case <-lingerC:
			linger = nil
			lingerC = nil
			d.flush(ctx, b)
		}
	}
}

// flush appends the pending batch to the durable queue and, on success, advances
// the covered checkpoints. On wal.ErrQueueFull it applies backpressure: it holds
// the batch and retries after a short delay, which stalls the batcher and every
// producer upstream (files persist — nothing is dropped). During shutdown the
// retry is bounded by shutdownFlushGrace; if the queue is still full the events
// are left uncheckpointed for re-read on the next start. A non-ErrQueueFull
// append error is logged and the batch dropped uncheckpointed (safe: it will be
// re-read), since retrying a marshal/IO fault would not help.
func (d *Daemon) flush(ctx context.Context, b *pendingBatch) {
	if b.empty() {
		return
	}
	var graceDeadline time.Time
	for {
		batch, err := d.queue.Append(b.events)
		if err == nil {
			d.advanceCheckpoints(b)
			d.log.Debug("collector: batch appended",
				"batch_sequence", batch.GetBatchSequence(), "events", len(b.events), "bytes", b.bytes)
			b.reset()
			return
		}
		if !errors.Is(err, wal.ErrQueueFull) {
			d.log.Error("collector: durable append failed; events left for re-read",
				"error", err.Error(), "events", len(b.events))
			b.reset()
			return
		}

		// Queue full: backpressure. Bound the wait only once we are shutting down.
		if ctx.Err() != nil {
			if graceDeadline.IsZero() {
				graceDeadline = time.Now().Add(d.shutdownFlushGrace)
			}
			if !time.Now().Before(graceDeadline) {
				d.log.Warn("collector: queue full at shutdown; events left for re-read", "events", len(b.events))
				b.reset()
				return
			}
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

// advanceCheckpoints persists, for every file identity the just-durable batch
// covered, the highest EndOffset in that batch. Offsets are monotonic per
// identity: a mark that does not advance the last persisted offset is skipped.
func (d *Daemon) advanceCheckpoints(b *pendingBatch) {
	for key, m := range b.marks {
		if last, ok := d.lastOffsets[key]; ok && m.offset <= last {
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
				"file_identity", key, "offset", m.offset, "error", err.Error())
			continue
		}
		d.lastOffsets[key] = m.offset
	}
}
