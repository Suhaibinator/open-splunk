# Ingestion token host/source constraints v0.1

This document fixes the first-release contract for the optional `host` and
`source` restrictions carried by native-ingestion tokens. It complements the
collector wire contract in `proto/open_splunk/v1/collector.proto`, the token
administration contract in `proto/open_splunk/v1/collector_admin.proto`, and
the durable replay contract in `docs/protobuf-v1-contracts.md`.

## Configuration

An ingestion token continues to require at least one explicit index scope. It
may additionally carry either or both of these independently optional lists:

- `allowed_host_regexes`;
- `allowed_source_regexes`.

An empty list means that dimension is unrestricted. A nonempty list contains
at most 16 unique patterns. Each pattern is nonempty valid UTF-8, contains no
NUL byte, occupies at most 512 UTF-8 bytes, and must compile under Go's RE2
regular-expression syntax. The aggregate pattern payload for one dimension is
at most 4,096 bytes. After RE2 simplification and compilation, one pattern may
contain at most 4,096 program instructions and one dimension may contain at
most 16,384 program instructions. These program bounds prevent compact source
expressions from expanding into unexpectedly expensive authorization state.

Patterns are exact configuration data: whitespace and case are not normalized.
Duplicate patterns are removed and the remaining patterns are stored and
published in lexical byte order. Consequently, semantically equivalent but
textually different regular expressions remain distinct configuration values.

The constraints are durable token metadata in the GORM-backed SQLite control
plane. They are not stored in ClickHouse, and the ClickHouse event path does
not use GORM. Creating or updating a token commits its definition, index
memberships, and host/source constraints atomically. Reclaiming a revoked token
tombstone removes all of those child records in the same transaction.

## Matching

Every pattern must match the complete canonical value. Conceptually, a stored
pattern `P` is evaluated as:

```text
\A(?:P)\z
```

This is full-value matching, not substring search. Matching is case-sensitive
unless the pattern explicitly selects another RE2 mode. Patterns within one
dimension use OR semantics; the host and source dimensions use AND semantics.
For example, an event is admitted by the following definition only when its
host matches either host pattern and its source matches the source pattern:

```text
allowed_host_regexes   = ["api-[0-9]+", "worker-[0-9]+"]
allowed_source_regexes = ["/var/log/gradethis\\.log"]
```

The server matches the validated canonical `LogEvent.host` and
`LogEvent.source` strings. It never matches dynamic fields, raw text, a
collector-supplied alternative index, or a value recovered after redaction.
Invalid event strings fail ordinary event validation before constraint
matching.

The bounded pattern set is compiled once for each freshly resolved
authorization snapshot, never once per event. A malformed, oversized, or
otherwise corrupt stored projection invalidates the complete refreshed token
authority and fails closed. It is not permission to continue with a subset of
patterns or with the previous policy for a fresh protected operation.

## Admission and rejection

Token authorization is refreshed at collector admission and at every protected
heartbeat and batch boundary. A token update may therefore allow only the
operation which already crossed the previous boundary; the next boundary
observes the complete new pattern set.

For a fresh batch, index authorization and ordinary event validation precede
host/source matching. A host mismatch produces
`EVENT_REJECTION_CODE_UNAUTHORIZED_HOST`; a source mismatch produces
`EVENT_REJECTION_CODE_UNAUTHORIZED_SOURCE`. When both dimensions fail, host is
reported first. Public violations identify only the canonical field and the
stable `unauthorized_host` or `unauthorized_source` reason. They never echo the
rejected value, a configured pattern, or token metadata.

Constraint failures are permanent per-event rejections. Other valid and
authorized events in the same batch may still be stored. Rejected events do
not consume token or index rate limits and do not reach ClickHouse. If no event
survives, the server durably records the existing
`BATCH_REJECTION_CODE_NO_AUTHORIZED_EVENTS` terminal outcome.

## Durable retry precedence

Exact durable batch lookup precedes mutable host/source policy, just as it
precedes mutable index and quota policy:

- a committed acknowledgement or terminal rejection replays exactly;
- a pending ClickHouse outbox resumes its already-admitted event set;
- only a fresh batch is evaluated against the current constraint snapshot;
- retrying the same batch after a token update cannot rewrite its durable
  disposition or charge a different event set.

This ordering makes an administrator update effective for future admission
without turning an acknowledged batch into a rejection or allowing a rejected
batch to become newly writable.

## Required coverage

The implementation must cover empty/unrestricted dimensions, exact full-value
matching, alternation, inline RE2 modes, OR-within/AND-across behavior,
host-first dual failure, pattern count, byte, and compiled-instruction
boundaries, duplicate canonicalization, invalid syntax and encoding,
create/update/list/get/reopen,
optimistic-lock rollback, revoked-token reclamation, corrupt child fanout and
widths, refreshed authorization without reconnect, partial batch acceptance,
all-rejected durable replay, quota exclusion, and real ClickHouse proof that a
rejected event creates no event row.
