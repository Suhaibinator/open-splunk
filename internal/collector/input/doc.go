// Package input discovers log files, tails them, and emits framed raw events
// tagged with their durable source position.
//
// # File identity
//
// A [FileIdentity] pairs the platform file identifier (device + inode from
// os.FileInfo.Sys on darwin/linux) with a content fingerprint (a hash over the
// first FingerprintBytes of the file). The platform id detects the same file
// across renames; the leading fingerprint plus a checkpoint's bounded trailing
// rewrite guard detect copy-truncate, in-place rewrites, and inode reuse. Its
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
// advancement is owned by the root daemon, which calls CheckpointStore.SetMany
// only after the covering batches have a durable terminal disposition and the
// entire earlier WAL prefix is terminal. This ordering (frame -> decode -> WAL
// append -> durable server acknowledgment -> checkpoint) makes file re-reads
// after a crash safe. The root collector overlays intact pending-WAL source
// coordinates as an ephemeral startup cursor to avoid ordinary rebatching.
// Unavoidable crash-boundary rereads remain bounded and retain stable event IDs
// for explicit logical deduplication; the server only suppresses retries of the
// same durable batch identity, so storage remains at-least-once.
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
// Rename/recreate is the required rotation mode when source-level at-least-once
// recovery matters: rotated files must remain readable and covered by the input
// globs until their terminal checkpoints advance. Copy-truncate is detected and
// its rewritten generations are never confused with an acknowledged prefix,
// including when the replacement regrows past the old offset between polls.
// However, no tailer can recover bytes an external process has already
// truncated before they cross the collector's WAL durability boundary. For that
// reason copy-truncate remains best-effort and must not be used as a strict
// at-least-once source contract.
//
// # Dependency direction
//
// input imports framing (input -> framing) and the generated protobuf types. It
// must not import wal, sender, config, or the root collector package.
package input
