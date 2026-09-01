package input

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math"
	"slices"
	"time"

	"fortio.org/safecast"

	"github.com/Suhaibinator/open-splunk/internal/collector/framing"
)

// framing's documented default is 1 MiB. A staged window is proportional to
// the configured per-event memory bound: line framing needs one event plus
// delimiter/read slop, while multiline may need one complete following line to
// prove the preceding event boundary. The raw snapshot, staged event payloads,
// and validation scratch therefore remain bounded by a constant multiple of
// MaxEventBytes instead of growing with the source file.
const (
	framingReadSlop        = 4 << 10
	initialStagedReadBytes = 4 << 10
	maxStagedEvents        = 1024
	validationChunkBytes   = 32 << 10
)

func (t *tailer) currentStagedReadWindow() uint64 {
	maximum := t.m.readWindow
	if t.stagedWindow == 0 || t.stagedWindow > maximum {
		t.stagedWindow = min(initialStagedReadBytes, maximum)
	}
	return t.stagedWindow
}

func (t *tailer) growStagedReadWindow() bool {
	current := t.currentStagedReadWindow()
	if current >= t.m.readWindow {
		return false
	}
	next := current * 2
	if next < current || next > t.m.readWindow {
		next = t.m.readWindow
	}
	t.stagedWindow = next
	return true
}

func (t *tailer) resetStagedReadWindow() {
	t.stagedWindow = 0
}

func (t *tailer) tuneProductiveStagedReadWindow(batch *stagedBatch) {
	consumed := batch.cursor.offset - batch.start
	current := t.currentStagedReadWindow()
	if batch.eventLimit || consumed <= current/4 {
		t.resetStagedReadWindow()
	}
}

func stagedReadWindow(cfg Config) (uint64, error) {
	maxEventBytes := cfg.Framing.MaxEventBytes
	if maxEventBytes <= 0 {
		maxEventBytes = framing.DefaultMaxEventBytes
	}
	window := uint64(maxEventBytes) + framingReadSlop
	if cfg.Multiline {
		if window > math.MaxUint64/2 {
			return 0, errors.New("collector/input: multiline staged-read window overflows uint64")
		}
		window *= 2
	}
	if window > uint64(math.MaxInt) {
		return 0, fmt.Errorf(
			"collector/input: staged-read window %d exceeds platform int",
			window,
		)
	}
	return window, nil
}

type tailerCursor struct {
	offset                         uint64
	nextLineNumber                 uint64
	discardingOversize             bool
	discardingMultilineOversize    bool
	discardingMultilinePartialLine bool
}

type stagedBatch struct {
	start                uint64
	snapshotEnd          uint64
	observedEnd          uint64
	dependencyStart      uint64
	dependency           []byte
	raw                  []byte
	capturedGuardMatches bool
	events               []RawEvent
	rejections           []RawEvent
	cursor               tailerCursor
	pendingLen           int
	oversize             bool
	flushed              bool
	eventLimit           bool
}

func (batch *stagedBatch) consumedLength() (int, error) {
	if batch.cursor.offset < batch.start {
		return 0, errors.New("collector/input: staged cursor moved backwards")
	}
	length := batch.cursor.offset - batch.start
	if length > uint64(len(batch.raw)) {
		return 0, fmt.Errorf(
			"collector/input: staged cursor consumed %d bytes from %d-byte snapshot",
			length,
			len(batch.raw),
		)
	}

	return safecast.MustConv[int](length), nil
}

func (batch *stagedBatch) reachedObservedEnd() bool {
	return batch.snapshotEnd == batch.observedEnd && !batch.eventLimit
}

func (batch *stagedBatch) stoppedAtArtificialBoundary() bool {
	return !batch.reachedObservedEnd() && batch.cursor.offset == batch.start
}

// stageRead snapshots a bounded source range before framing. Framing therefore
// cannot combine bytes from different file generations, and no RawEvent is
// externally visible until stagedBatchMatches proves the exact dependency and
// commitBatch installs its cursor and publishes the private events.
func (t *tailer) stageRead(
	ctx context.Context,
	observedEnd uint64,
	forceFlush bool,
) (*stagedBatch, error) {
	if observedEnd < t.offset {
		return nil, fmt.Errorf(
			"collector/input: observed end %d precedes tailer offset %d",
			observedEnd,
			t.offset,
		)
	}
	remaining := observedEnd - t.offset
	readLimit := t.currentStagedReadWindow()
	if t.guardLength == 0 && t.offset == 0 &&
		uint64(t.id.FingerprintLength) > readLimit {
		readLimit = uint64(t.id.FingerprintLength)
	}
	if remaining > readLimit {
		remaining = readLimit
	}
	if remaining > uint64(math.MaxInt) {
		return nil, fmt.Errorf("collector/input: staged read %d exceeds platform int", remaining)
	}
	dependencyStart := t.offset
	guardLength := uint64(0)
	if t.guardLength > 0 {
		if t.guardOffset > t.offset || t.guardLength > t.offset-t.guardOffset {
			return nil, errors.New("collector/input: installed guard is not behind cursor")
		}
		if t.guardOffset+t.guardLength != t.offset {
			return nil, errors.New("collector/input: installed guard is not contiguous with cursor")
		}
		dependencyStart = t.guardOffset
		guardLength = t.guardLength
	}
	dependencyLength := guardLength + remaining
	if dependencyLength < remaining || dependencyLength > uint64(math.MaxInt) {
		return nil, fmt.Errorf(
			"collector/input: staged dependency length %d exceeds platform int",
			dependencyLength,
		)
	}
	dependencyOffset, err := checkedFileOffset(dependencyStart)
	if err != nil {
		return nil, err
	}
	if observer := t.m.beforeSnapshotReadObserver; observer != nil {
		observer(tailerPollObservation{
			path:       t.pathStr(),
			offset:     t.offset + remaining,
			generation: t.id.Generation,
		})
	}
	dependency := make([]byte, int(dependencyLength))
	if len(dependency) > 0 {
		reader := io.NewSectionReader(t.f, dependencyOffset, int64(len(dependency)))
		observer := t.m.afterSnapshotChunkObserver
		if observer == nil {
			if _, err := io.ReadFull(reader, dependency); err != nil {
				return nil, classifyExactReadError(err)
			}
		} else {
			for position := 0; position < len(dependency); {
				chunkLength := min(len(dependency)-position, validationChunkBytes)
				if _, err := io.ReadFull(reader, dependency[position:position+chunkLength]); err != nil {
					return nil, classifyExactReadError(err)
				}
				position += chunkLength

				positionOffset := safecast.MustConv[uint64](position)
				observer(tailerPollObservation{
					path:       t.pathStr(),
					offset:     dependencyStart + positionOffset,
					generation: t.id.Generation,
				})
			}
		}
	}
	raw := dependency[int(guardLength):]
	capturedGuardMatches := true
	if guardLength > 0 {
		capturedGuardMatches = sha256.Sum256(dependency[:guardLength]) == t.guardFingerprint
	} else if t.id.FingerprintLength > 0 {
		identityLength := int(t.id.FingerprintLength)
		capturedGuardMatches = identityLength <= len(raw)
		if capturedGuardMatches {
			digest := sha256.Sum256(raw[:identityLength])
			capturedGuardMatches = hex.EncodeToString(digest[:]) == t.id.Fingerprint
		}
	}
	batch := &stagedBatch{
		start:                t.offset,
		snapshotEnd:          t.offset + remaining,
		observedEnd:          observedEnd,
		dependencyStart:      dependencyStart,
		dependency:           dependency,
		raw:                  raw,
		capturedGuardMatches: capturedGuardMatches,
		cursor: tailerCursor{
			offset:                         t.offset,
			nextLineNumber:                 t.nextLineNumber,
			discardingOversize:             t.discardingOversize,
			discardingMultilineOversize:    t.discardingMultilineOversize,
			discardingMultilinePartialLine: t.discardingMultilinePartialLine,
		},
	}
	if err := t.stageSnapshot(ctx, batch, forceFlush); err != nil {
		return nil, err
	}
	return batch, nil
}

func (t *tailer) stageSnapshot(
	ctx context.Context,
	batch *stagedBatch,
	forceFlush bool,
) error {
	position := 0
	if batch.cursor.discardingMultilineOversize {
		var err error
		position, err = t.skipMultilineOversize(batch, forceFlush)
		if err != nil {
			return err
		}
		if batch.cursor.discardingMultilineOversize {
			return nil
		}
	}
	if batch.cursor.discardingOversize {
		delimiter := bytes.IndexByte(batch.raw, '\n')
		if delimiter < 0 {
			batch.cursor.offset += uint64(len(batch.raw))
			return nil
		}
		if batch.cursor.nextLineNumber >= ^uint64(0)-1 {
			return framing.ErrLineNumberOverflow
		}
		position = delimiter + 1
		batch.cursor.offset += uint64(position)
		batch.cursor.nextLineNumber++
		batch.cursor.discardingOversize = false
	}

	fr, err := t.m.newFramer(
		bytes.NewReader(batch.raw[position:]),
		batch.cursor.offset,
		batch.cursor.nextLineNumber,
	)
	if err != nil {
		return err
	}
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		frame, frameErr := fr.Next()
		switch {
		case frameErr == nil:
			if err := t.stageFrame(batch, frame); err != nil {
				return err
			}
			if len(batch.events) >= maxStagedEvents {
				batch.eventLimit = true
				return nil
			}
		case errors.Is(frameErr, framing.ErrEventTooLargeIncomplete):
			if err := t.stageRejectedFrame(batch, frame); err != nil {
				return err
			}
			batch.cursor.offset = frame.EndOffset
			batch.cursor.nextLineNumber = frame.NextLineNumber
			if t.m.cfg.Multiline {
				batch.cursor.discardingMultilineOversize = true
				batch.cursor.discardingMultilinePartialLine = true
			} else {
				batch.cursor.discardingOversize = true
			}
			batch.oversize = true
			return nil
		case errors.Is(frameErr, framing.ErrEventTooLarge):
			if err := t.stageRejectedFrame(batch, frame); err != nil {
				return err
			}
			batch.cursor.offset = frame.EndOffset
			batch.cursor.nextLineNumber = frame.NextLineNumber
			batch.oversize = true
			if t.m.cfg.Multiline {
				batch.cursor.discardingMultilineOversize = true
				batch.cursor.discardingMultilinePartialLine = false
				return nil
			}
		case errors.Is(frameErr, framing.ErrPartialFrame):
			_, batch.pendingLen = fr.Pending()
			if batch.snapshotEnd == batch.observedEnd &&
				(forceFlush || t.shouldFlushInactive(batch.pendingLen)) {
				frame, ok, flushErr := fr.Flush()
				if !ok {
					if flushErr != nil {
						return flushErr
					}
					return nil
				}
				batch.pendingLen = 0
				switch {
				case flushErr == nil:
					if err := t.stageFrame(batch, frame); err != nil {
						return err
					}
					batch.flushed = true
					if len(batch.events) >= maxStagedEvents {
						batch.eventLimit = true
						return nil
					}
					// A multiline flush may emit an assembled event while retaining a
					// matching delimiter-free start line. Keep draining under the same
					// inactivity/retirement decision so a final snapshot cannot retire
					// with that candidate still unconsumed.
					continue
				case errors.Is(flushErr, framing.ErrEventTooLargeIncomplete):
					if err := t.stageRejectedFrame(batch, frame); err != nil {
						return err
					}
					batch.cursor.offset = frame.EndOffset
					batch.cursor.nextLineNumber = frame.NextLineNumber
					if t.m.cfg.Multiline {
						batch.cursor.discardingMultilineOversize = true
						batch.cursor.discardingMultilinePartialLine = true
					} else {
						batch.cursor.discardingOversize = true
					}
					batch.oversize = true
				case errors.Is(flushErr, framing.ErrEventTooLarge):
					if err := t.stageRejectedFrame(batch, frame); err != nil {
						return err
					}
					batch.cursor.offset = frame.EndOffset
					batch.cursor.nextLineNumber = frame.NextLineNumber
					batch.oversize = true
					if t.m.cfg.Multiline {
						batch.cursor.discardingMultilineOversize = true
						batch.cursor.discardingMultilinePartialLine = false
					}
				default:
					return flushErr
				}
			}
			return nil
		case errors.Is(frameErr, io.EOF):
			return nil
		default:
			return frameErr
		}
	}
}

func (t *tailer) skipMultilineOversize(
	batch *stagedBatch,
	forceFlush bool,
) (int, error) {
	position := 0
	maxEventBytes := t.m.cfg.Framing.MaxEventBytes
	if maxEventBytes <= 0 {
		maxEventBytes = framing.DefaultMaxEventBytes
	}
	for {
		if position == len(batch.raw) {
			// A prior bounded snapshot may already have consumed the known-rejected
			// prefix of an incomplete physical line. Retirement force-resolves that
			// line even when no newly appended bytes remain to read.
			resolve := batch.snapshotEnd == batch.observedEnd &&
				(forceFlush || t.inactivityElapsed())
			if resolve && batch.cursor.discardingMultilinePartialLine {
				if batch.cursor.nextLineNumber >= ^uint64(0)-1 {
					return 0, framing.ErrLineNumberOverflow
				}
				batch.cursor.nextLineNumber++
				batch.cursor.discardingMultilinePartialLine = false
			}
			return position, nil
		}
		relativeDelimiter := bytes.IndexByte(batch.raw[position:], '\n')
		if batch.cursor.discardingMultilinePartialLine {
			if relativeDelimiter < 0 {
				remaining := len(batch.raw) - position
				batch.cursor.offset += uint64(remaining)
				resolve := batch.snapshotEnd == batch.observedEnd &&
					(forceFlush || t.shouldFlushInactive(remaining))
				if resolve {
					if batch.cursor.nextLineNumber >= ^uint64(0)-1 {
						return 0, framing.ErrLineNumberOverflow
					}
					batch.cursor.nextLineNumber++
					batch.cursor.discardingMultilinePartialLine = false
				}
				return len(batch.raw), nil
			}
			if batch.cursor.nextLineNumber >= ^uint64(0)-1 {
				return 0, framing.ErrLineNumberOverflow
			}
			consumed := relativeDelimiter + 1
			position += consumed
			batch.cursor.offset += uint64(consumed)
			batch.cursor.nextLineNumber++
			batch.cursor.discardingMultilinePartialLine = false
			continue
		}
		if relativeDelimiter < 0 {
			partial := batch.raw[position:]
			resolve := batch.snapshotEnd == batch.observedEnd &&
				(forceFlush || t.shouldFlushInactive(len(partial)))
			// A delimiter-free line that is already over budget cannot become a
			// usable start line. Consume it incrementally while retaining the
			// discard mode and physical-line cursor until its delimiter arrives.
			couldBeExactCapCRLF := len(partial) == maxEventBytes+1 &&
				partial[len(partial)-1] == '\r'
			if len(partial) > maxEventBytes && !couldBeExactCapCRLF {
				batch.cursor.offset += uint64(len(partial))
				if resolve {
					if batch.cursor.nextLineNumber >= ^uint64(0)-1 {
						return 0, framing.ErrLineNumberOverflow
					}
					batch.cursor.nextLineNumber++
					batch.cursor.discardingMultilinePartialLine = false
				} else {
					batch.cursor.discardingMultilinePartialLine = true
				}
				return len(batch.raw), nil
			}
			if !resolve {
				return position, nil
			}
			matchContent := partial
			if len(matchContent) > 0 && matchContent[len(matchContent)-1] == '\r' {
				matchContent = matchContent[:len(matchContent)-1]
			}
			if len(partial) > 0 && t.m.cfg.Framing.LineStartPattern.Match(matchContent) {
				batch.cursor.discardingMultilineOversize = false
				batch.cursor.discardingMultilinePartialLine = false
				return position, nil
			}
			if len(partial) > 0 {
				if batch.cursor.nextLineNumber >= ^uint64(0)-1 {
					return 0, framing.ErrLineNumberOverflow
				}
				batch.cursor.offset += uint64(len(partial))
				batch.cursor.nextLineNumber++
			}
			return len(batch.raw), nil
		}
		delimiter := position + relativeDelimiter
		line := batch.raw[position:delimiter]
		if len(line) > 0 && line[len(line)-1] == '\r' {
			line = line[:len(line)-1]
		}
		if t.m.cfg.Framing.LineStartPattern.Match(line) {
			batch.cursor.discardingMultilineOversize = false
			batch.cursor.discardingMultilinePartialLine = false
			return position, nil
		}
		if batch.cursor.nextLineNumber >= ^uint64(0)-1 {
			return 0, framing.ErrLineNumberOverflow
		}
		consumed := relativeDelimiter + 1
		position += consumed
		batch.cursor.offset += uint64(consumed)
		batch.cursor.nextLineNumber++
	}
}

func (t *tailer) stageFrame(batch *stagedBatch, frame framing.Frame) error {
	return t.stageFrameWithDisposition(batch, frame, "", false)
}

func (t *tailer) stageRejectedFrame(
	batch *stagedBatch,
	frame framing.Frame,
) error {
	return t.stageFrameWithDisposition(
		batch, frame, "FRAMING_EVENT_TOO_LARGE", true,
	)
}

func (t *tailer) stageFrameWithDisposition(
	batch *stagedBatch,
	frame framing.Frame,
	rejectionCode string,
	truncated bool,
) error {
	guardFingerprint, guardLength, err := t.guardForEvent(batch, frame.EndOffset)
	if err != nil {
		return err
	}
	payload := frame.Bytes
	lineNumber, nextLineNumber := frame.LineNumber, frame.NextLineNumber
	if !t.lineCursorKnown {
		lineNumber, nextLineNumber = 0, 0
	}
	target := &batch.events
	if rejectionCode != "" {
		target = &batch.rejections
	}
	*target = append(*target, RawEvent{
		Bytes: payload, RejectionCode: rejectionCode, Truncated: truncated,
		Source: SourceRef{
			Path:             t.pathStr(),
			Identity:         t.id,
			StartOffset:      frame.StartOffset,
			EndOffset:        frame.EndOffset,
			LineNumber:       lineNumber,
			NextLineNumber:   nextLineNumber,
			GuardFingerprint: guardFingerprint,
			GuardLength:      guardLength,
		},
	})
	batch.cursor.offset = frame.EndOffset
	batch.cursor.nextLineNumber = frame.NextLineNumber
	return nil
}

// guardForEvent derives the exact bounded trailing evidence for one event from
// the immutable staged dependency. Framer implementations return independently
// owned Frame.Bytes, so stageFrame can transfer that payload directly while the
// dependency remains available for this coordinate hash.
func (t *tailer) guardForEvent(
	batch *stagedBatch,
	end uint64,
) (fingerprint string, length uint32, err error) {
	if end < batch.dependencyStart {
		return "", 0, errors.New("collector/input: event ends before staged dependency")
	}
	available := end - batch.dependencyStart
	if available > uint64(len(batch.dependency)) {
		return "", 0, errors.New("collector/input: event guard exceeds staged dependency")
	}
	guardLength := end

	if maximum := safecast.MustConv[uint64](t.m.fpBytes); guardLength > maximum {
		guardLength = maximum
	}
	if guardLength == 0 {
		return "", 0, nil
	}
	guardStartOffset := end - guardLength
	if guardStartOffset < batch.dependencyStart {
		return "", 0, errors.New("collector/input: staged dependency lacks event guard prefix")
	}
	start := guardStartOffset - batch.dependencyStart
	if start+guardLength > uint64(len(batch.dependency)) {
		return "", 0, errors.New("collector/input: event guard range exceeds staged dependency")
	}

	guardBytes := batch.dependency[safecast.MustConv[int](start):safecast.MustConv[int](start+guardLength)]
	digest := sha256.Sum256(guardBytes)

	return hex.EncodeToString(digest[:]), safecast.MustConv[uint32](guardLength), nil
}

func (t *tailer) snapshotDependenciesMatch(batch *stagedBatch) (bool, error) {
	length := len(batch.dependency)
	if length == 0 {
		return true, nil
	}
	scratchLength := min(length, validationChunkBytes)
	t.validationScratch = slices.Grow(
		t.validationScratch[:0],
		scratchLength,
	)[:scratchLength]
	for position := 0; position < length; {
		chunkLength := min(length-position, len(t.validationScratch))
		offset, err := checkedFileOffset(batch.dependencyStart + uint64(position))
		if err != nil {
			return false, err
		}
		chunk := t.validationScratch[:chunkLength]
		n, err := t.f.ReadAt(chunk, offset)
		if err != nil && !errors.Is(err, io.EOF) {
			return false, err
		}
		if n != chunkLength {
			return false, fmt.Errorf(
				"%w: validation range at offset %d: %w",
				errSourceSnapshotChanged,
				batch.dependencyStart+uint64(position),
				io.ErrUnexpectedEOF,
			)
		}
		if !bytes.Equal(chunk, batch.dependency[position:position+chunkLength]) {
			return false, nil
		}
		position += chunkLength
	}
	return true, nil
}

func (t *tailer) stagedBatchMatches(batch *stagedBatch) (bool, error) {
	if !batch.capturedGuardMatches {
		return false, nil
	}
	return t.snapshotDependenciesMatch(batch)
}

func (t *tailer) guardFromBatch(batch *stagedBatch) (tailerRewriteGuard, error) {
	consumed, err := batch.consumedLength()
	if err != nil {
		return tailerRewriteGuard{}, err
	}
	if consumed == 0 {
		return tailerRewriteGuard{}, nil
	}
	if batch.dependencyStart > batch.cursor.offset {
		return tailerRewriteGuard{}, errors.New(
			"collector/input: staged dependency starts after cursor",
		)
	}
	available := batch.cursor.offset - batch.dependencyStart
	if available > uint64(len(batch.dependency)) {
		return tailerRewriteGuard{}, fmt.Errorf(
			"collector/input: staged guard needs %d bytes from %d-byte dependency",
			available,
			len(batch.dependency),
		)
	}
	guardLength := available
	// validatedFingerprintBytes guarantees a positive bounded int at construction.

	if maximum := safecast.MustConv[uint64](t.m.fpBytes); guardLength > maximum {
		guardLength = maximum
	}

	guardEnd := safecast.MustConv[int](available)

	guardStart := guardEnd - safecast.MustConv[int](guardLength)
	return tailerRewriteGuard{
		offset:      batch.cursor.offset - guardLength,
		length:      guardLength,
		fingerprint: sha256.Sum256(batch.dependency[guardStart:guardEnd]),
	}, nil
}

// commitBatch installs the validated cursor and bounded trailing guard before
// publishing. A rewrite after this point cannot alter an already-staged event:
// the next poll compares the installed old snapshot and burns a new generation.
func (t *tailer) commitBatch(ctx context.Context, batch *stagedBatch) bool {
	guard, err := t.guardFromBatch(batch)
	if err != nil {
		t.setReadError(err)
		return false
	}
	for _, rejection := range batch.rejections {
		if t.m.rejectionHandler == nil {
			continue
		}
		if err := t.m.rejectionHandler(ctx, rejection); err != nil {
			t.setReadError(err)
			return false
		}
	}
	t.offset = batch.cursor.offset
	t.nextLineNumber = batch.cursor.nextLineNumber
	t.discardingOversize = batch.cursor.discardingOversize
	t.discardingMultilineOversize = batch.cursor.discardingMultilineOversize
	t.discardingMultilinePartialLine = batch.cursor.discardingMultilinePartialLine
	if guard.length > 0 {
		t.installGuard(guard)
	}
	if batch.oversize {
		t.m.lastErrorNs.Store(time.Now().UnixNano())
	}
	for _, event := range batch.events {
		select {
		case t.m.events <- event:
			t.emittedSinceFence = true
			t.m.eventsRead.Add(1)
			t.m.bytesRead.Add(uint64(len(event.Bytes)))
			t.m.lastEventNs.Store(time.Now().UnixNano())
		case <-ctx.Done():
			return false
		}
	}
	return true
}

// finalizeRetirement establishes the finite cross-platform handoff boundary.
// The manager may cancel a provisional request until retireCommitted is set.
// Snapshot preparation remains cancellable; the lifecycle lock protects the
// final size check, exact guard+snapshot validation, and EOF probe. Bytes
// written after that finite boundary are outside the retirement contract:
// portable file APIs cannot prove that an unlinked descriptor has no writer
// capable of appending in the future.
func (t *tailer) finalizeRetirement(
	ctx context.Context,
	expectedSize uint64,
	expectedVersion uint64,
) (done bool, retryNow bool) {
	t.retireMu.Lock()
	if !t.retireRequested.Load() || t.retireCommitted ||
		t.retireVersion.Load() != expectedVersion {
		t.retireStable = 0
		t.retireMu.Unlock()
		return false, false
	}
	t.retireMu.Unlock()
	if !t.m.acquireStagedTransaction(ctx) {
		return false, false
	}
	permitHeld := true
	defer func() {
		if permitHeld {
			t.m.releaseStagedTransaction()
		}
	}()
	t.retireMu.Lock()
	if !t.retireRequested.Load() || t.retireCommitted ||
		t.retireVersion.Load() != expectedVersion {
		t.retireStable = 0
		t.retireMu.Unlock()
		return false, false
	}
	t.retireMu.Unlock()

	// Snapshot preparation remains cancellable. Only the short final exact
	// validation/EOF-probe section below holds retireMu and decides whether
	// cancellation or retirement wins the lifecycle race.
	if observer := t.m.beforeRetireCommitObserver; observer != nil {
		observer(tailerPollObservation{
			path:       t.pathStr(),
			offset:     t.offset,
			generation: t.id.Generation,
		})
	}
	info, err := t.f.Stat()
	if err != nil || info.Size() < 0 {
		t.retireStable = 0
		if err == nil {
			err = errors.New("file has a negative size")
		}
		t.setReadError(err)
		return false, false
	}

	finalSize := safecast.MustConv[uint64](info.Size())
	if finalSize != expectedSize {
		t.retireStable = 0
		return false, true
	}
	batch, err := t.stageRead(ctx, finalSize, true)
	if err != nil {
		t.retireStable = 0
		if !errors.Is(err, context.Canceled) &&
			t.handleSourceValidationFailure(false, err) {
			return false, true
		}
		return false, false
	}
	if !batch.reachedObservedEnd() || batch.cursor.offset != finalSize {
		// A forced final snapshot must classify every byte before retirement can
		// release the descriptor. snapshotEnd proves what was read; cursor proves
		// what framing actually consumed. Keeping both checks makes a future
		// partial-frame state-machine regression fail safe instead of losing the
		// retained suffix.
		t.retireStable = 0
		if batch.cursor.offset == batch.start {
			t.growStagedReadWindow()
		}
		return false, true
	}
	stable, validationErr := t.stagedBatchMatches(batch)
	if !stable || validationErr != nil {
		t.retireStable = 0
		if t.handleSourceValidationFailure(stable, validationErr) {
			return false, true
		}
		return false, false
	}

	t.retireMu.Lock()
	if !t.retireRequested.Load() || t.retireCommitted ||
		t.retireVersion.Load() != expectedVersion {
		t.retireStable = 0
		t.retireMu.Unlock()
		return false, false
	}
	info, err = t.f.Stat()
	if err != nil || info.Size() < 0 {
		t.retireStable = 0
		t.retireMu.Unlock()
		if err == nil {
			err = errors.New("file has a negative size")
		}
		t.setReadError(err)
		return false, false
	}

	finalObservedSize := safecast.MustConv[uint64](info.Size())
	if finalObservedSize != finalSize {
		// Growth is an ordinary append, not a generation change. The normal
		// transaction loop will stage the suffix and begin quiescence again.
		t.retireStable = 0
		t.retireMu.Unlock()
		return false, true
	}
	// This second exact comparison (followed by an EOF probe), not the preceding
	// size-only Stat, establishes the documented retirement boundary.
	stable, validationErr = t.stagedBatchMatches(batch)
	if !stable || validationErr != nil {
		t.retireStable = 0
		t.retireMu.Unlock()
		if t.handleSourceValidationFailure(stable, validationErr) {
			return false, true
		}
		return false, false
	}
	finalOffset, offsetErr := checkedFileOffset(finalSize)
	if offsetErr != nil {
		t.retireStable = 0
		t.retireMu.Unlock()
		t.setReadError(offsetErr)
		return false, false
	}
	var probe [1]byte
	n, probeErr := t.f.ReadAt(probe[:], finalOffset)
	if n > 0 || (probeErr == nil && n == 0) {
		t.retireStable = 0
		t.retireMu.Unlock()
		return false, true
	}
	if !errors.Is(probeErr, io.EOF) {
		t.retireStable = 0
		t.retireMu.Unlock()
		t.setReadError(probeErr)
		return false, false
	}
	t.retireCommitted = true
	t.retireMu.Unlock()
	// Once retireCommitted is visible, discovery retains this tailer as the
	// owner of its physical tracking key. Publish the final snapshot in FIFO
	// order, then release the bounded staging-memory permit before waiting on
	// daemon/WAL durability. Holding a permit across that I/O boundary would let
	// several retiring files stall otherwise independent active tailers.
	t.commitBatch(ctx, batch)
	batch = nil
	t.m.releaseStagedTransaction()
	permitHeld = false
	// Fence prior publications even if this final commit was interrupted or hit
	// an internal staging error. Under a live context those earlier events still
	// must be durable before discovery can reuse the tracking key; under shutdown
	// awaitDurabilityBarrier returns immediately through runCtx cancellation.
	if barrierErr := t.awaitDurabilityBarrier(); barrierErr != nil &&
		!errors.Is(barrierErr, context.Canceled) &&
		!errors.Is(barrierErr, context.DeadlineExceeded) {
		t.setReadError(barrierErr)
	}
	// Retirement crossed its exact source boundary and cannot be canceled. If
	// shutdown interrupted publication or its durability fence, no replacement
	// generation can start in this Manager; restart resumes from the older
	// durable checkpoint and safely rereads the source range.
	return true, false
}
