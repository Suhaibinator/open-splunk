// Package framing splits a byte stream into discrete event frames tagged with
// their byte extent in the underlying source.
//
// A [Framer] reads from an io.Reader and yields [Frame] values. Two framers are
// provided: a newline framer ([NewLineFramer]) that emits one frame per
// newline-delimited record, and a multiline framer ([NewMultilineFramer]) that
// assembles several physical lines into one logical event using a line-start
// pattern.
//
// # Offsets and at-least-once framing
//
// Every Frame carries StartOffset and EndOffset measured in bytes from the
// beginning of the underlying stream (seeded by the startOffset passed to the
// constructor), plus the 1-based LineNumber of its first line. These offsets
// are the durable coordinate the input package checkpoints against and the
// decoder binds into stable event IDs, so they must be exact.
//
// A Framer never returns a Frame for an unterminated trailing segment. When the
// reader reaches EOF in the middle of a record, Next returns [ErrPartialFrame]
// and the bytes of the partial record are reported by [Framer.Pending]; they
// are not consumed. Because checkpoints only ever advance to the EndOffset of a
// returned (complete) Frame, a file that is still being appended to is re-read
// from the last complete boundary on the next poll, never splitting a record.
//
// # Size enforcement
//
// [Options.MaxEventBytes] caps a single frame. A record that reaches the cap
// without a delimiter yields [ErrEventTooLarge]; the frame's offsets still
// advance past the oversized record so framing can continue, and the truncated
// bytes are available for dead-lettering. Implementations must not buffer
// unboundedly while searching for a delimiter.
//
// # Dependency direction
//
// framing depends only on the standard library. It is imported by the input
// package (input -> framing) and must not import any other internal/collector
// package.
package framing
