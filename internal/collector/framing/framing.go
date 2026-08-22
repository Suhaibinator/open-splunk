package framing

import (
	"bytes"
	"errors"
	"io"
	"math"
	"regexp"

	"fortio.org/safecast"
)

// Sentinel errors returned by a Framer. Callers use errors.Is to classify them.
var (
	// ErrPartialFrame is returned when the reader reached EOF with an
	// unterminated trailing record. The partial bytes are retained (see
	// Framer.Pending) and not consumed; the returned Frame is the zero value.
	ErrPartialFrame = errors.New("collector/framing: incomplete trailing frame")

	// ErrEventTooLarge is returned when a delimited record exceeds Options.
	// MaxEventBytes. The returned Frame carries the truncated bytes and the
	// offsets consumed through the boundary where the oversize was detected.
	// After a multiline error, the framer discards continuation lines until the
	// next line matching Options.LineStartPattern, which remains unconsumed and
	// begins the next frame.
	ErrEventTooLarge = errors.New("collector/framing: event exceeds max size")

	// ErrEventTooLargeIncomplete reports an oversized record whose delimiter
	// was not observed in the bounded working buffer. The returned Frame spans
	// bytes safely discarded so far but does not advance NextLineNumber. A
	// streaming caller must retain resynchronization state before framing
	// subsequent bytes: line framing discards through the physical delimiter;
	// multiline framing additionally discards continuation lines through (but
	// not including) the next matching start line.
	ErrEventTooLargeIncomplete = errors.New("collector/framing: oversized event is still incomplete")

	// ErrLineNumberOverflow prevents a corrupt or exhausted cursor from wrapping
	// a physical source line back to zero.
	ErrLineNumberOverflow = errors.New("collector/framing: source line number exhausted")
)

// DefaultMaxEventBytes caps a single frame when Options.MaxEventBytes is zero.
// Callers that size framing-adjacent buffers should use this same value.
const DefaultMaxEventBytes = 1 << 20

// readChunkSize is the working buffer size used per underlying Read.
const readChunkSize = 4096

// Frame is one framed event and its position in the underlying stream.
//
// Bytes excludes the record delimiter and is owned by the returned Frame; it
// remains valid across later Framer calls and may be transferred without a
// defensive copy. StartOffset is the byte offset of the first byte of the
// record; EndOffset is the offset one past the last byte consumed for this
// record, including its delimiter, and is the value an input checkpoints once
// the covering durable batch receives a terminal server disposition.
// LineNumber is the 1-based line of the record's first physical line.
// NextLineNumber is the 1-based physical line at EndOffset and can seed a
// replacement framer without rescanning the source prefix. A frame returned
// with ErrEventTooLargeIncomplete is not checkpointable: its NextLineNumber is
// the still-pending physical line rather than a completed boundary.
type Frame struct {
	Bytes          []byte
	StartOffset    uint64
	EndOffset      uint64
	LineNumber     uint64
	NextLineNumber uint64
}

// Options configures a Framer.
type Options struct {
	// StartLineNumber seeds the 1-based line number at the supplied start
	// offset. Zero selects line 1.
	StartLineNumber uint64

	// MaxEventBytes caps Frame.Bytes. A frame's final LF or CRLF delimiter does
	// not count toward the cap; delimiters between multiline physical lines do.
	// Zero selects the package default.
	MaxEventBytes int

	// LineStartPattern is used only by the multiline framer: a physical line
	// matching it begins a new logical event. Ignored by the line framer.
	LineStartPattern *regexp.Regexp

	// MaxLines bounds physical lines per multiline event (0 = unbounded until a
	// new start line or MaxEventBytes).
	MaxLines int
}

// Framer splits an underlying reader into Frames.
//
// Next returns the next complete frame. It returns io.EOF at a clean record
// boundary when the reader is exhausted, ErrPartialFrame when an in-budget
// record is incomplete, ErrEventTooLarge for a delimited oversized record, and
// ErrEventTooLargeIncomplete when an oversized record is still waiting for its
// delimiter. Implementations are not required to be safe for concurrent use.
type Framer interface {
	Next() (Frame, error)

	// Pending reports the start offset and length of buffered bytes that have
	// not yet formed a complete frame. After ErrPartialFrame it describes the
	// retained partial record so the caller can resume from startOffset.
	Pending() (startOffset uint64, length int)

	// Flush force-resolves currently buffered partial data, returning ok=false
	// when no frame is produced. Multiline discard mode may still consume a
	// continuation of an already-rejected event. It is used by the tailer after
	// Next reports ErrPartialFrame (for example when a multiline event has been
	// idle past the inactivity window). err classifies an oversized or otherwise
	// invalid forced record; a successful Frame never exceeds MaxEventBytes.
	// After a consumed flush, a subsequent Next continues from the resolved
	// offset.
	Flush() (frame Frame, ok bool, err error)
}

// source is the shared incremental reader used by both framers. It pulls from r
// on demand and tolerates readers that return one byte at a time.
type source struct {
	r   io.Reader
	buf []byte // unconsumed bytes; buf[0] sits at stream offset off
	off uint64 // stream offset of buf[0]
	eof bool   // r has reported io.EOF
}

// fill reads at least one more byte from r into buf, or records EOF. It retries
// past zero-length, nil-error reads so it never returns without progress unless
// the reader is exhausted or errors.
func (s *source) fill() error {
	if s.eof {
		return nil
	}
	var tmp [readChunkSize]byte
	for {
		n, err := s.r.Read(tmp[:])
		if n > 0 {
			s.buf = append(s.buf, tmp[:n]...)
		}
		if err == io.EOF {
			s.eof = true
			return nil
		}
		if err != nil {
			return err
		}
		if n > 0 {
			return nil
		}
	}
}

// NewLineFramer returns a Framer that emits one Frame per newline-delimited
// record read from r. startOffset is the byte offset of r's first byte within
// the underlying stream and seeds the frame offsets.
//
// A trailing "\r" before the "\n" is stripped from Frame.Bytes but is counted
// in EndOffset. A final line without a terminating newline is not emitted:
// Next returns ErrPartialFrame and the bytes are retained (see Pending) so the
// caller can re-read once the record is completed.
func NewLineFramer(r io.Reader, startOffset uint64, opts Options) (Framer, error) {
	if r == nil {
		return nil, errors.New("collector/framing: nil reader")
	}
	maxBytes := opts.MaxEventBytes
	if maxBytes <= 0 {
		maxBytes = DefaultMaxEventBytes
	}
	return &lineFramer{
		r: r, off: startOffset,
		line:     normalizedStartLine(opts.StartLineNumber),
		maxBytes: maxBytes,
	}, nil
}

// NewMultilineFramer returns a Framer that assembles physical lines into one
// logical event. A line matching opts.LineStartPattern begins a new event; the
// preceding assembled event is emitted. opts.LineStartPattern is required.
//
// Non-matching (continuation) lines are appended to the current event with
// their internal delimiter bytes preserved inside Frame.Bytes; only the trailing
// delimiter of the whole logical event is excluded from Bytes (but counted in
// EndOffset). Non-matching lines that precede the first start line form their
// own frame. opts.MaxLines bounds physical lines per event; opts.MaxEventBytes
// caps its byte length. After an oversized logical event is reported, its
// continuation lines are discarded until the next matching start line; that
// start line is retained as the beginning of the next event.
func NewMultilineFramer(r io.Reader, startOffset uint64, opts Options) (Framer, error) {
	if r == nil {
		return nil, errors.New("collector/framing: nil reader")
	}
	if opts.LineStartPattern == nil {
		return nil, errors.New("collector/framing: multiline framer requires LineStartPattern")
	}
	maxBytes := opts.MaxEventBytes
	if maxBytes <= 0 {
		maxBytes = DefaultMaxEventBytes
	}
	return &multilineFramer{
		r: r, off: startOffset,
		pattern:    opts.LineStartPattern,
		maxLines:   opts.MaxLines,
		maxBytes:   maxBytes,
		nextLineNo: normalizedStartLine(opts.StartLineNumber),
	}, nil
}

func normalizedStartLine(line uint64) uint64 {
	if line == 0 {
		return 1
	}
	return line
}

// canAdvanceLineNumber reserves MaxUint64 as an invalid sentinel. Emitting it
// as a next-line cursor would create an event that ingestion and checkpoint
// validation can never acknowledge.
func canAdvanceLineNumber(line uint64) bool {
	return line < math.MaxUint64-1
}

// lineFramer implements newline-delimited framing.
type lineFramer struct {
	source
	line     uint64 // 1-based line number of the next frame's first line
	maxBytes int    // always > 0
}

// Next implements Framer.
func (f *lineFramer) Next() (Frame, error) {
	for {
		i := bytes.IndexByte(f.buf, '\n')
		// MaxEventBytes applies to Frame.Bytes, so neither byte of a final
		// CRLF delimiter counts. Before LF arrives, keep one trailing CR in
		// reserve as a possible first half of that delimiter. This remains true
		// when a bounded source snapshot reports EOF; Flush makes the final
		// record-boundary decision.
		contentBytes := len(f.buf)
		if i >= 0 {
			contentBytes = i
			if i > 0 && f.buf[i-1] == '\r' {
				contentBytes--
			}
		} else if contentBytes > 0 && f.buf[contentBytes-1] == '\r' {
			contentBytes--
		}
		if contentBytes > f.maxBytes {
			return f.emitTooLarge()
		}
		if i >= 0 {
			if !canAdvanceLineNumber(f.line) {
				return Frame{}, ErrLineNumberOverflow
			}
			return f.emitLine(i), nil
		}
		if f.eof {
			if len(f.buf) == 0 {
				return Frame{}, io.EOF
			}
			if !canAdvanceLineNumber(f.line) {
				return Frame{}, ErrLineNumberOverflow
			}
			return Frame{}, ErrPartialFrame
		}
		if err := f.fill(); err != nil {
			return Frame{}, err
		}
	}
}

// emitLine consumes buf[:i+1] (the record and its '\n') and returns its Frame.
func (f *lineFramer) emitLine(i int) Frame {
	start := f.off
	ln := f.line
	record := f.buf[:i+1]
	content := record[:len(record)-1] // excludes '\n'
	if len(content) > 0 && content[len(content)-1] == '\r' {
		content = content[:len(content)-1]
	}
	out := make([]byte, len(content))
	copy(out, content)
	end := f.off + uint64(len(record))
	f.buf = f.buf[len(record):]
	f.off = end
	f.line++
	return Frame{
		Bytes: out, StartOffset: start, EndOffset: end,
		LineNumber: ln, NextLineNumber: f.line,
	}
}

// emitTooLarge returns a truncated oversized record. It consumes through a
// delimiter already in the working buffer, or returns
// ErrEventTooLargeIncomplete after the current bounded buffer so a streaming
// caller can discard subsequent chunks with cancellation.
func (f *lineFramer) emitTooLarge() (Frame, error) {
	if !canAdvanceLineNumber(f.line) {
		return Frame{}, ErrLineNumberOverflow
	}
	start := f.off
	ln := f.line
	out := make([]byte, f.maxBytes)
	copy(out, f.buf[:f.maxBytes])
	if i := bytes.IndexByte(f.buf, '\n'); i >= 0 {
		end := f.off + uint64(i+1)
		f.buf = f.buf[i+1:]
		f.off = end
		f.line++
		return Frame{
			Bytes: out, StartOffset: start, EndOffset: end,
			LineNumber: ln, NextLineNumber: f.line,
		}, ErrEventTooLarge
	}
	// No delimiter is currently buffered. Stop after this bounded chunk rather
	// than blocking inside an arbitrary Reader until a delimiter or EOF.
	f.off += uint64(len(f.buf))
	f.buf = f.buf[:0]
	return Frame{
		Bytes: out, StartOffset: start, EndOffset: f.off,
		LineNumber: ln, NextLineNumber: f.line,
	}, ErrEventTooLargeIncomplete
}

// Pending implements Framer.
func (f *lineFramer) Pending() (uint64, int) {
	return f.off, len(f.buf)
}

// Flush implements Framer.
func (f *lineFramer) Flush() (Frame, bool, error) {
	if len(f.buf) == 0 {
		return Frame{}, false, nil
	}
	if !canAdvanceLineNumber(f.line) {
		return Frame{}, false, ErrLineNumberOverflow
	}
	start := f.off
	ln := f.line
	content := f.buf
	if len(content) > 0 && content[len(content)-1] == '\n' {
		content = content[:len(content)-1]
	}
	if len(content) > 0 && content[len(content)-1] == '\r' {
		content = content[:len(content)-1]
	}
	contentLength := len(content)
	flushErr := error(nil)
	if contentLength > f.maxBytes {
		contentLength = f.maxBytes
		flushErr = ErrEventTooLarge
	}
	out := make([]byte, contentLength)
	copy(out, content)
	end := f.off + uint64(len(f.buf))
	f.off = end
	f.buf = f.buf[:0]
	f.line++
	return Frame{
		Bytes: out, StartOffset: start, EndOffset: end,
		LineNumber: ln, NextLineNumber: f.line,
	}, true, flushErr
}

// pendingFrame is a frame queued for emission by the multiline framer.
type pendingFrame struct {
	frame Frame
	err   error
}

// multilineFramer implements pattern-anchored multiline framing.
type multilineFramer struct {
	source
	pattern  *regexp.Regexp
	maxLines int
	maxBytes int // always > 0

	event   []byte // raw consumed bytes of the current logical event
	evStart uint64 // stream offset of the event's first byte
	evLine  uint64 // 1-based line number of the event's first physical line
	evLines int    // physical lines consumed into the current event
	evDelim int    // delimiter length (1 or 2) of the last consumed line
	started bool   // an event is currently being assembled

	// An oversized event is reported as soon as its bound is crossed. The
	// following continuation lines still belong to that rejected event and
	// must not be emitted as standalone frames. If the bound was crossed in an
	// unterminated physical line, discardPartialLine remains set until that
	// line's delimiter is consumed.
	discardingOversize bool
	discardPartialLine bool

	nextLineNo uint64         // 1-based number of the next physical line to read
	pending    []pendingFrame // frames produced but not yet returned
}

// Next implements Framer.
func (m *multilineFramer) Next() (Frame, error) {
	for {
		if len(m.pending) > 0 {
			pf := m.pending[0]
			m.pending = m.pending[1:]
			return pf.frame, pf.err
		}
		if m.discardingOversize {
			if err := m.discardUntilStart(); err != nil {
				return Frame{}, err
			}
			continue
		}
		i := bytes.IndexByte(m.buf, '\n')
		if i < 0 {
			if !m.eof {
				// A single physical line without a delimiter that, together with
				// the assembled event, exceeds the cap is oversized. A trailing
				// CR may be the first half of the final delimiter and the last
				// delimiter already in event remains final while buf is empty. If
				// an event is already assembled, allow one valid-sized lookahead
				// line to complete: it may match the start pattern and delimit the
				// current event rather than continue it.
				tooLarge := m.pendingContentBytes() > m.maxBytes
				if m.started && len(m.buf) > 0 {
					tooLarge = m.pendingLineContentBytes() > m.maxBytes
				}
				if tooLarge {
					if err := m.emitOversized(); err != nil {
						return Frame{}, err
					}
					continue
				}
				if err := m.fill(); err != nil {
					return Frame{}, err
				}
				continue
			}
			// Once an event is assembled, an unterminated following physical
			// line is ambiguous: it may become a matching start line and delimit
			// the valid event when its LF arrives. Never classify their combined
			// bytes as oversized at a bounded-reader EOF. Retain both until the
			// physical boundary arrives or Flush explicitly resolves inactivity.
			tooLarge := m.pendingContentBytes() > m.maxBytes
			if m.started && len(m.buf) > 0 {
				tooLarge = false
			}
			if tooLarge {
				if err := m.emitOversized(); err != nil {
					return Frame{}, err
				}
				continue
			}
			// EOF with no complete physical line buffered.
			if m.started || len(m.buf) > 0 {
				if len(m.buf) > 0 && !canAdvanceLineNumber(m.nextLineNo) {
					return Frame{}, ErrLineNumberOverflow
				}
				return Frame{}, ErrPartialFrame
			}
			return Frame{}, io.EOF
		}
		if !canAdvanceLineNumber(m.nextLineNo) {
			return Frame{}, ErrLineNumberOverflow
		}

		content := m.buf[:i] // excludes '\n'
		matchContent := content
		delim := 1
		if len(content) > 0 && content[len(content)-1] == '\r' {
			matchContent = content[:len(content)-1]
			delim = 2
		}
		isStart := m.pattern.Match(matchContent)
		lineLen := i + 1

		if isStart && m.started {
			m.pending = append(m.pending, pendingFrame{frame: m.finishEvent()})
			m.beginEvent()
			if err := m.consumeLine(m.buf[:lineLen], delim); err != nil {
				return Frame{}, err
			}
			m.checkBounds()
			continue
		}
		if !m.started {
			m.beginEvent()
		}
		if err := m.consumeLine(m.buf[:lineLen], delim); err != nil {
			return Frame{}, err
		}
		m.checkBounds()
	}
}

// pendingContentBytes reports the bytes that would appear in Frame.Bytes if
// the buffered physical line ended with a delimiter next. It deliberately
// treats a trailing CR as a possible half-delimiter even when a bounded source
// snapshot reports EOF; Flush makes the final record-boundary decision.
func (m *multilineFramer) pendingContentBytes() int {
	n := len(m.event) + len(m.buf)
	if len(m.buf) == 0 {
		if m.started {
			n -= m.evDelim
		}
		return n
	}
	if m.buf[len(m.buf)-1] == '\r' {
		n--
	}
	return n
}

func (m *multilineFramer) pendingLineContentBytes() int {
	n := len(m.buf)
	if n > 0 && m.buf[n-1] == '\r' {
		n--
	}
	return n
}

// discardUntilStart consumes continuation lines following an oversized
// multiline event. It leaves the first complete matching start line untouched
// so the ordinary framing path can use it to begin the next event.
func (m *multilineFramer) discardUntilStart() error {
	for {
		i := bytes.IndexByte(m.buf, '\n')
		if m.discardPartialLine {
			if i >= 0 {
				if !canAdvanceLineNumber(m.nextLineNo) {
					return ErrLineNumberOverflow
				}
				m.discardLine(i + 1)
				m.discardPartialLine = false
				continue
			}
			if m.eof {
				return ErrPartialFrame
			}
			// This physical line is already known to belong to the rejected
			// event, so its buffered prefix is safe to drop before reading on.
			m.off += uint64(len(m.buf))
			m.buf = m.buf[:0]
			if err := m.fill(); err != nil {
				return err
			}
			continue
		}

		if i >= 0 {
			content := m.buf[:i]
			if len(content) > 0 && content[len(content)-1] == '\r' {
				content = content[:len(content)-1]
			}
			if m.pattern.Match(content) {
				m.discardingOversize = false
				return nil
			}
			if !canAdvanceLineNumber(m.nextLineNo) {
				return ErrLineNumberOverflow
			}
			m.discardLine(i + 1)
			continue
		}
		if m.eof {
			if len(m.buf) > 0 {
				return ErrPartialFrame
			}
			return io.EOF
		}

		// A delimiter-free candidate that is already too large cannot seed
		// a valid next frame. Discard it incrementally through its delimiter
		// rather than growing the working buffer without bound.
		candidateBytes := len(m.buf)
		if candidateBytes > 0 && m.buf[candidateBytes-1] == '\r' {
			candidateBytes--
		}
		if candidateBytes > m.maxBytes {
			m.discardPartialLine = true
			continue
		}
		if err := m.fill(); err != nil {
			return err
		}
	}
}

func (m *multilineFramer) discardLine(n int) {
	m.buf = m.buf[n:]

	m.off += safecast.MustConv[uint64](n)
	m.nextLineNo++
}

// beginEvent starts a fresh logical event anchored at the current position.
func (m *multilineFramer) beginEvent() {
	m.event = m.event[:0]
	m.evStart = m.off
	m.evLine = m.nextLineNo
	m.evLines = 0
	m.evDelim = 0
	m.started = true
}

// consumeLine appends a physical line and its delimiter to the current event
// and advances the stream position.
func (m *multilineFramer) consumeLine(line []byte, delim int) error {
	if !canAdvanceLineNumber(m.nextLineNo) {
		return ErrLineNumberOverflow
	}
	m.event = append(m.event, line...)
	m.buf = m.buf[len(line):]
	m.off += uint64(len(line))
	m.evLines++
	m.evDelim = delim
	m.nextLineNo++
	return nil
}

// checkBounds emits the current event when it hits the byte or line cap.
func (m *multilineFramer) checkBounds() {
	if !m.started {
		return
	}
	// Frame.Bytes excludes the final physical-line delimiter, so an event whose
	// content is exactly at the cap remains valid.
	contentBytes := len(m.event) - m.evDelim
	if contentBytes > m.maxBytes {
		fr := m.finishEventTruncated()
		m.discardingOversize = true
		m.discardPartialLine = false
		m.pending = append(m.pending, pendingFrame{frame: fr, err: ErrEventTooLarge})
		return
	}
	if m.maxLines > 0 && m.evLines >= m.maxLines {
		m.pending = append(m.pending, pendingFrame{frame: m.finishEvent()})
	}
}

// finishEventLen builds the frame for the current event using the first n
// assembled bytes (offsets always span the whole event) and resets assembly
// state.
func (m *multilineFramer) finishEventLen(n int) Frame {
	out := make([]byte, n)
	copy(out, m.event[:n])
	fr := Frame{
		Bytes:          out,
		StartOffset:    m.evStart,
		EndOffset:      m.evStart + uint64(len(m.event)),
		LineNumber:     m.evLine,
		NextLineNumber: m.nextLineNo,
	}
	m.resetEvent()
	return fr
}

// finishEvent builds the frame for a completed event (trailing delimiter
// excluded from Bytes) and resets assembly state.
func (m *multilineFramer) finishEvent() Frame {
	return m.finishEventLen(max(len(m.event)-m.evDelim, 0))
}

// finishEventTruncated builds an oversized event frame carrying the first
// maxBytes bytes but offsets spanning the whole assembled event.
func (m *multilineFramer) finishEventTruncated() Frame {
	return m.finishEventLen(min(m.maxBytes, len(m.event)))
}

// resetEvent clears the current-event assembly state.
func (m *multilineFramer) resetEvent() {
	m.event = m.event[:0]
	m.started = false
	m.evLines = 0
	m.evDelim = 0
}

// emitOversized handles an unterminated physical line that, with the assembled
// event, exceeds the cap. It consumes only the current bounded buffer and
// queues ErrEventTooLargeIncomplete so a streaming caller can discard
// subsequent chunks with cancellation.
func (m *multilineFramer) emitOversized() error {
	if !canAdvanceLineNumber(m.nextLineNo) {
		return ErrLineNumberOverflow
	}
	start := m.evStart
	lineNo := m.evLine
	if !m.started {
		start = m.off
		lineNo = m.nextLineNo
	}
	combined := len(m.event) + len(m.buf)
	tn := min(m.maxBytes, combined)
	out := make([]byte, tn)
	k := copy(out, m.event)
	if k < tn {
		copy(out[k:], m.buf[:tn-k])
	}
	// Discard everything currently buffered; it is part of the oversized record.
	m.off += uint64(len(m.buf))
	m.buf = m.buf[:0]
	m.resetEvent()
	m.discardingOversize = true
	m.discardPartialLine = true
	m.pending = append(m.pending, pendingFrame{
		frame: Frame{
			Bytes: out, StartOffset: start, EndOffset: m.off,
			LineNumber: lineNo, NextLineNumber: m.nextLineNo,
		},
		err: ErrEventTooLargeIncomplete,
	})
	return nil
}

// Pending implements Framer.
func (m *multilineFramer) Pending() (uint64, int) {
	return m.off - uint64(len(m.event)), len(m.event) + len(m.buf)
}

// Flush implements Framer.
func (m *multilineFramer) Flush() (Frame, bool, error) {
	if len(m.pending) > 0 {
		pf := m.pending[0]
		m.pending = m.pending[1:]
		return pf.frame, true, pf.err
	}
	if m.discardingOversize {
		if len(m.buf) == 0 {
			if m.discardPartialLine {
				if !canAdvanceLineNumber(m.nextLineNo) {
					return Frame{}, false, ErrLineNumberOverflow
				}
				m.nextLineNo++
				m.discardPartialLine = false
			}
			return Frame{}, false, nil
		}
		if !canAdvanceLineNumber(m.nextLineNo) {
			return Frame{}, false, ErrLineNumberOverflow
		}
		content := m.buf
		if content[len(content)-1] == '\r' {
			content = content[:len(content)-1]
		}
		if !m.discardPartialLine && len(content) <= m.maxBytes &&
			m.pattern.Match(content) {
			// Inactivity makes this delimiter-free matching candidate a complete
			// physical line. Let the ordinary flush path emit it as the first event
			// after the rejected multiline record.
			m.discardingOversize = false
		} else {
			// A non-matching candidate, or the remainder of the physical line that
			// crossed the cap, still belongs to the rejected event. Forced flush
			// resolves that physical line without manufacturing another oversize
			// result for the same logical event.
			m.off += uint64(len(m.buf))
			m.buf = m.buf[:0]
			m.nextLineNo++
			m.discardPartialLine = false
			return Frame{}, false, nil
		}
	}
	if !m.started && len(m.buf) == 0 {
		return Frame{}, false, nil
	}
	hasPartialLine := len(m.buf) > 0
	if hasPartialLine && !canAdvanceLineNumber(m.nextLineNo) {
		return Frame{}, false, ErrLineNumberOverflow
	}
	if m.started && hasPartialLine {
		matchContent := m.buf
		if matchContent[len(matchContent)-1] == '\r' {
			matchContent = matchContent[:len(matchContent)-1]
		}
		if m.pattern.Match(matchContent) {
			// Forced inactivity makes the buffered candidate a complete logical
			// boundary for this decision. Preserve the already-assembled valid
			// event and leave the matching candidate untouched for the next frame.
			return m.finishEvent(), true, nil
		}
	}
	start := m.off - uint64(len(m.event))
	lineNo := m.nextLineNo
	if m.started {
		lineNo = m.evLine
	}
	combined := make([]byte, 0, len(m.event)+len(m.buf))
	combined = append(combined, m.event...)
	combined = append(combined, m.buf...)
	end := start + uint64(len(combined))
	content := combined
	if len(content) > 0 && content[len(content)-1] == '\n' {
		content = content[:len(content)-1]
		if len(content) > 0 && content[len(content)-1] == '\r' {
			content = content[:len(content)-1]
		}
	} else if len(content) > 0 && content[len(content)-1] == '\r' {
		// Keep a final CR pending as the first half of a possible CRLF across
		// source snapshots. A forced flush makes the same record-boundary choice
		// as the line framer and excludes that delimiter byte.
		content = content[:len(content)-1]
	}
	flushErr := error(nil)
	if len(content) > m.maxBytes {
		content = content[:m.maxBytes]
		flushErr = ErrEventTooLarge
	}
	out := make([]byte, len(content))
	copy(out, content)
	m.buf = m.buf[:0]
	m.off = end
	m.resetEvent()
	if hasPartialLine {
		m.nextLineNo++
	}
	return Frame{
		Bytes: out, StartOffset: start, EndOffset: end,
		LineNumber: lineNo, NextLineNumber: m.nextLineNo,
	}, true, flushErr
}
