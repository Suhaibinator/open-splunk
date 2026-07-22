// Package export creates bounded CSV and JSON Lines artifacts from immutable
// search-job snapshots.
//
// Artifacts live in an owned per-manager session directory and are never
// exposed by filesystem path. This foundation intentionally does not provide
// an Open method or download token: a later server adapter must add a scoped,
// short-lived read lease so TTL cleanup cannot race an active download.
//
// Lifecycle metadata is currently process-local. Close removes the owned
// session, but a process crash may leave that randomized directory behind;
// durable restart reconciliation belongs with the server's artifact journal.
package export
