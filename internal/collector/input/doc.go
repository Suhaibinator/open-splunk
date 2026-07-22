// Package input discovers log files, tails them, and emits framed raw events
// tagged with their durable source position.
//
// # File identity
//
// A [FileIdentity] pairs the platform file identifier (device + inode from
// os.FileInfo.Sys on darwin/linux) with a content fingerprint (a hash over the
// first FingerprintBytes of the file). The platform id detects the same file
// across renames; the fingerprint detects copy-truncate and inode reuse. Its
// String form is stable and is what the decoder receives as
// SourcePosition.FileIdentity, so it must not change for a given physical file.
// Windows is out of scope; syscall-specific identity code is build-tagged for
// darwin/linux and degrades to a fingerprint-only identity elsewhere.
//
// # Checkpoints and at-least-once
//
// A [CheckpointStore] persists the last durably handled byte offset per file
// identity. Persistence is atomic (write a temp file, fsync, rename over the
// target) so a crash never leaves a torn checkpoint. The [Manager] reads
// checkpoints at discovery to resume; it does NOT advance them. Checkpoint
// advancement is owned by the root daemon, which calls CheckpointStore.Set only
// after the covering events are durable in the WAL. This ordering (frame ->
// decode -> WAL append -> checkpoint) is what makes file re-reads after a crash
// safe: unadvanced checkpoints cause at-most a bounded duplicate re-read, which
// the server deduplicates by stable event ID.
//
// # Tailing behavior
//
// The Manager polls discovered files (include globs minus exclude globs),
// handling rotation by rename/recreate, copy-truncate (detected when the file
// shrinks or its fingerprint changes), deletion, and delayed creation. On first
// discovery StartAt selects whether an unknown file is read from its beginning
// or only from its current end. Absent or unreadable inputs are reported as
// [Health] states, never as fatal errors.
//
// # Dependency direction
//
// input imports framing (input -> framing) and the generated protobuf types. It
// must not import wal, sender, config, or the root collector package.
package input
