package framing

import (
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

	// errNotImplemented is returned by contract stubs during the skeleton phase.
	errNotImplemented = errors.New("collector/framing: not implemented")
)

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
}

// NewLineFramer returns a Framer that emits one Frame per newline-delimited
// record read from r. startOffset is the byte offset of r's first byte within
// the underlying stream and seeds the frame offsets.
func NewLineFramer(r io.Reader, startOffset uint64, opts Options) (Framer, error) {
	return nil, errNotImplemented
}

// NewMultilineFramer returns a Framer that assembles physical lines into one
// logical event. A line matching opts.LineStartPattern begins a new event; the
// preceding assembled event is emitted. opts.LineStartPattern is required.
func NewMultilineFramer(r io.Reader, startOffset uint64, opts Options) (Framer, error) {
	return nil, errNotImplemented
}
