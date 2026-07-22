// Package export creates bounded CSV and JSON Lines artifacts from immutable
// search-job snapshots.
//
// Artifacts live in an owned per-manager session directory and are never
// exposed by filesystem path. Scoped callers can mint bounded, short-lived,
// one-time bearer grants. Redeeming a grant returns a pinned read lease so TTL
// cleanup can hide an expired artifact immediately without racing a download.
//
// Lifecycle metadata is currently process-local. Close removes the owned
// session, but a process crash may leave that randomized directory behind;
// durable restart reconciliation belongs with the server's artifact journal.
package export
