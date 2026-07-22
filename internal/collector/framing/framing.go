package framing

import (
	"bytes"
	"errors"
	"io"
	"regexp"
)

// Sentinel errors returned by a Framer. Callers use errors.Is to classify them.
var (
	// ErrPartialFrame is returned when the reader reached EOF with an
	// unterminated trailing record. The partial bytes are retained (see
	// Framer.Pending) and not consumed; the returned Frame is the zero value.
	ErrPartialFrame = errors.New("collector/framing: incomplete trailing frame")

	// ErrEventTooLarge is returned when a single record reaches Options.
	// MaxEventBytes without a delimiter. The returned Frame carries the
	// truncated bytes and the offsets spanning the oversized record.
	ErrEventTooLarge = errors.New("collector/framing: event exceeds max size")
)

// defaultMaxEventBytes caps a single frame when Options.MaxEventBytes is zero.
const defaultMaxEventBytes = 1 << 20

// readChunkSize is the working buffer size used per underlying Read.
const readChunkSize = 4096

// Frame is one framed event and its position in the underlying stream.
//
// Bytes excludes the record delimiter and is only valid until the next call to
// Next; a caller that retains it must copy. StartOffset is the byte offset of
// the first byte of the record; EndOffset is the offset one past the last byte
// consumed for this record, including its delimiter, and is the value an input
// checkpoints once the frame is durably persisted. LineNumber is the 1-based
// line of the record's first line.
type Frame struct {
	Bytes       []byte
	StartOffset uint64
	EndOffset   uint64
	LineNumber  uint64
}

// Options configures a Framer.
type Options struct {
	// MaxEventBytes caps a single frame. Zero selects the package default.
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
// boundary when the reader is exhausted, ErrPartialFrame when the reader is
// exhausted mid-record, and ErrEventTooLarge when a record exceeds the size
// cap. Implementations are not required to be safe for concurrent use.
type Framer interface {
	Next() (Frame, error)

	// Pending reports the start offset and length of buffered bytes that have
	// not yet formed a complete frame. After ErrPartialFrame it describes the
	// retained partial record so the caller can resume from startOffset.
	Pending() (startOffset uint64, length int)

	// Flush force-emits the currently buffered partial record as a complete
	// frame and consumes it, returning ok=false when nothing is buffered. It is
	// used by the tailer to release a trailing record that Next has reported as
	// ErrPartialFrame (for example a multiline event that has been idle past the
	// inactivity window). After a successful Flush the returned bytes are
	// consumed; a subsequent Next continues from the returned EndOffset.
	Flush() (frame Frame, ok bool)
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
	max := opts.MaxEventBytes
	if max <= 0 {
		max = defaultMaxEventBytes
	}
	return &lineFramer{
		source:   source{r: r, off: startOffset},
		line:     1,
		maxBytes: max,
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
// caps its byte length.
func NewMultilineFramer(r io.Reader, startOffset uint64, opts Options) (Framer, error) {
	if r == nil {
		return nil, errors.New("collector/framing: nil reader")
	}
	if opts.LineStartPattern == nil {
		return nil, errors.New("collector/framing: multiline framer requires LineStartPattern")
	}
	max := opts.MaxEventBytes
	if max <= 0 {
		max = defaultMaxEventBytes
	}
	return &multilineFramer{
		source:     source{r: r, off: startOffset},
		pattern:    opts.LineStartPattern,
		maxLines:   opts.MaxLines,
		maxBytes:   max,
		nextLineNo: 1,
	}, nil
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
		// Oversized: the record reaches the cap without a usable delimiter.
		if (i < 0 && len(f.buf) >= f.maxBytes) || (i > f.maxBytes) {
			return f.emitTooLarge()
		}
		if i >= 0 {
			return f.emitLine(i), nil
		}
		if f.eof {
			if len(f.buf) == 0 {
				return Frame{}, io.EOF
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
	content := f.buf[:i] // excludes '\n'
	if len(content) > 0 && content[len(content)-1] == '\r' {
		content = content[:len(content)-1]
	}
	out := make([]byte, len(content))
	copy(out, content)
	end := f.off + uint64(i+1)
	f.buf = f.buf[i+1:]
	f.off = end
	f.line++
	return Frame{Bytes: out, StartOffset: start, EndOffset: end, LineNumber: ln}
}

// emitTooLarge returns the truncated oversized record, consuming input up to and
// including the next '\n' (or EOF) so framing can resume at the next record.
func (f *lineFramer) emitTooLarge() (Frame, error) {
	start := f.off
	ln := f.line
	out := make([]byte, f.maxBytes)
	copy(out, f.buf[:f.maxBytes])
	for {
		if i := bytes.IndexByte(f.buf, '\n'); i >= 0 {
			end := f.off + uint64(i+1)
			f.buf = f.buf[i+1:]
			f.off = end
			f.line++
			return Frame{Bytes: out, StartOffset: start, EndOffset: end, LineNumber: ln}, ErrEventTooLarge
		}
		// No delimiter in buf: discard it (bounded memory) and read more.
		f.off += uint64(len(f.buf))
		f.buf = f.buf[:0]
		if f.eof {
			f.line++
			return Frame{Bytes: out, StartOffset: start, EndOffset: f.off, LineNumber: ln}, ErrEventTooLarge
		}
		if err := f.fill(); err != nil {
			return Frame{}, err
		}
	}
}

// Pending implements Framer.
func (f *lineFramer) Pending() (uint64, int) {
	return f.off, len(f.buf)
}

// Flush implements Framer.
func (f *lineFramer) Flush() (Frame, bool) {
	if len(f.buf) == 0 {
		return Frame{}, false
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
	out := make([]byte, len(content))
	copy(out, content)
	end := f.off + uint64(len(f.buf))
	f.off = end
	f.buf = f.buf[:0]
	f.line++
	return Frame{Bytes: out, StartOffset: start, EndOffset: end, LineNumber: ln}, true
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
		i := bytes.IndexByte(m.buf, '\n')
		if i < 0 {
			if !m.eof {
				// A single physical line without a delimiter that, together with
				// the assembled event, reaches the cap is oversized.
				if len(m.event)+len(m.buf) >= m.maxBytes {
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
			// EOF with no complete physical line buffered.
			if m.started || len(m.buf) > 0 {
				return Frame{}, ErrPartialFrame
			}
			return Frame{}, io.EOF
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
			m.consumeLine(lineLen, delim)
			m.checkBounds()
			continue
		}
		if !m.started {
			m.beginEvent()
		}
		m.consumeLine(lineLen, delim)
		m.checkBounds()
	}
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

// consumeLine appends buf[:lineLen] (a physical line and its delimiter) to the
// current event and advances the stream position.
func (m *multilineFramer) consumeLine(lineLen, delim int) {
	m.event = append(m.event, m.buf[:lineLen]...)
	m.buf = m.buf[lineLen:]
	m.off += uint64(lineLen)
	m.evLines++
	m.evDelim = delim
	m.nextLineNo++
}

// checkBounds emits the current event when it hits the byte or line cap.
func (m *multilineFramer) checkBounds() {
	if !m.started {
		return
	}
	if len(m.event) >= m.maxBytes {
		m.pending = append(m.pending, pendingFrame{frame: m.finishEventTruncated(), err: ErrEventTooLarge})
		return
	}
	if m.maxLines > 0 && m.evLines >= m.maxLines {
		m.pending = append(m.pending, pendingFrame{frame: m.finishEvent()})
	}
}

// finishEvent builds the frame for a completed event (trailing delimiter
// excluded from Bytes) and resets assembly state.
func (m *multilineFramer) finishEvent() Frame {
	n := len(m.event) - m.evDelim
	if n < 0 {
		n = 0
	}
	out := make([]byte, n)
	copy(out, m.event[:n])
	fr := Frame{
		Bytes:       out,
		StartOffset: m.evStart,
		EndOffset:   m.evStart + uint64(len(m.event)),
		LineNumber:  m.evLine,
	}
	m.resetEvent()
	return fr
}

// finishEventTruncated builds an oversized event frame carrying the first
// maxBytes bytes but offsets spanning the whole assembled event.
func (m *multilineFramer) finishEventTruncated() Frame {
	n := m.maxBytes
	if n > len(m.event) {
		n = len(m.event)
	}
	out := make([]byte, n)
	copy(out, m.event[:n])
	fr := Frame{
		Bytes:       out,
		StartOffset: m.evStart,
		EndOffset:   m.evStart + uint64(len(m.event)),
		LineNumber:  m.evLine,
	}
	m.resetEvent()
	return fr
}

// resetEvent clears the current-event assembly state.
func (m *multilineFramer) resetEvent() {
	m.event = m.event[:0]
	m.started = false
	m.evLines = 0
	m.evDelim = 0
}

// emitOversized handles an unterminated physical line that, with the assembled
// event, exceeds the cap. It queues one ErrEventTooLarge frame and skips input
// up to and including the next '\n' (or EOF).
func (m *multilineFramer) emitOversized() error {
	start := m.evStart
	lineNo := m.evLine
	consumed := m.evLines
	if !m.started {
		start = m.off
		lineNo = m.nextLineNo
		consumed = 0
	}
	combined := len(m.event) + len(m.buf)
	tn := m.maxBytes
	if tn > combined {
		tn = combined
	}
	out := make([]byte, tn)
	k := copy(out, m.event)
	if k < tn {
		copy(out[k:], m.buf[:tn-k])
	}
	// Discard everything currently buffered; it is part of the oversized record.
	m.off += uint64(len(m.buf))
	m.buf = m.buf[:0]
	m.resetEvent()
	for {
		if err := m.fill(); err != nil {
			return err
		}
		if i := bytes.IndexByte(m.buf, '\n'); i >= 0 {
			m.off += uint64(i + 1)
			m.buf = m.buf[i+1:]
			m.nextLineNo = lineNo + uint64(consumed) + 1
			m.pending = append(m.pending, pendingFrame{
				frame: Frame{Bytes: out, StartOffset: start, EndOffset: m.off, LineNumber: lineNo},
				err:   ErrEventTooLarge,
			})
			return nil
		}
		m.off += uint64(len(m.buf))
		m.buf = m.buf[:0]
		if m.eof {
			m.nextLineNo = lineNo + uint64(consumed) + 1
			m.pending = append(m.pending, pendingFrame{
				frame: Frame{Bytes: out, StartOffset: start, EndOffset: m.off, LineNumber: lineNo},
				err:   ErrEventTooLarge,
			})
			return nil
		}
	}
}

// Pending implements Framer.
func (m *multilineFramer) Pending() (uint64, int) {
	return m.off - uint64(len(m.event)), len(m.event) + len(m.buf)
}

// Flush implements Framer.
func (m *multilineFramer) Flush() (Frame, bool) {
	if len(m.pending) > 0 {
		pf := m.pending[0]
		m.pending = m.pending[1:]
		return pf.frame, true
	}
	if !m.started && len(m.buf) == 0 {
		return Frame{}, false
	}
	start := m.off - uint64(len(m.event))
	lineNo := m.nextLineNo
	if m.started {
		lineNo = m.evLine
	}
	flushedLines := m.evLines
	if len(m.buf) > 0 {
		flushedLines++
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
	}
	m.buf = m.buf[:0]
	m.off = end
	m.resetEvent()
	m.nextLineNo = lineNo + uint64(flushedLines)
	return Frame{Bytes: content, StartOffset: start, EndOffset: end, LineNumber: lineNo}, true
}
