# Ingestion rate limits v0.1

This document fixes the first-release contract for per-token and per-index
native-ingestion quotas. It complements the collector wire contract in
`proto/open_splunk/v1/collector.proto` and the durable replay contract in
`docs/protobuf-v1-contracts.md`.

## Configuration

Every ingestion token and logical index has two independently optional limits:

- `max_events_per_second`;
- `max_uncompressed_bytes_per_second`.

Zero means unlimited for that dimension. A configured value must be positive
and no greater than 1,000,000 events/second or 1 TiB/second, respectively.
There is no inherited deployment default in v0.1. These limits are versioned
with the existing token or index definition and are read from the same fresh
authorization snapshot used at each protected batch boundary.

The configured limits are durable control-plane data. Runtime accounting is
also durable in the single-node SQLite database; it is not stored in
ClickHouse and does not use GORM on the ClickHouse path.

A token bucket carries a cascading owner reference to its physical ingestion
token row, so normal revoked-token tombstone reclamation also removes the
accounting state without a deletion race. Index bucket identities remain
bounded by the index catalog's permanently reserved physical identities.

## Charge

Only fresh events which pass index authorization, event validation, and
mandatory redaction admission are charged. Permanently rejected events do not
consume a token or index limit.

The event charge is one unit per admitted event. The byte charge is the
server-computed protobuf-encoded size of that event as received from the
collector, before normalization or redaction. Client-reported byte totals are
validated by the hard envelope but are never trusted as quota accounting.

The token receives the sum of every admitted event in the batch. Each index
receives only the events admitted to that index. A mixed-index batch is one
atomic admission decision: all token and index charges commit, or none do.
Quota pressure never creates a partial quota admission.

Scope keys are the trusted `(tenant_id, token_id)` and
`(tenant_id, index_name)` projections. Collector-supplied labels cannot choose
or alias an accounting scope. Tenant-level quotas, retained-storage quotas,
and calendar-period quotas are outside v0.1.

## Schedule and burst behavior

Each enabled scope dimension owns a durable next-admission timestamp. A fresh
batch may be admitted only when every applicable next-admission timestamp is
less than or equal to the current server time. On admission, each timestamp is
advanced by:

```text
ceil(charge / configured_rate seconds)
```

This is a virtual-schedule limiter with an implicit burst of one complete
fresh batch. It intentionally permits an otherwise idle scope to admit a
legal batch whose charge is greater than one second of configured capacity;
the resulting debt delays later batches proportionally. A finite legal batch
therefore cannot become permanently un-admittable merely because an operator
configured a low rate.

If multiple dimensions block a batch, the retry delay is the greatest
remaining delay. The blocking scope reported to the collector is the scope
which determines that delay; token wins an exact tie, followed by lexical
index name order. An externally advertised delay is capped at one hour, so a
larger durable debt is rechecked periodically without weakening the stored
schedule.

Changing a configured rate resets only that changed dimension at the first
fresh batch boundary which observes the new policy. An unchanged dimension
keeps its accumulated schedule. Backward wall-clock movement cannot refill a
scope early: a future stored timestamp continues to block until server time
catches up.

## Durable identity and retry precedence

Durable batch lookup precedes mutable quota policy:

- committed and rejected outcomes replay exactly and do not charge again;
- a pending ClickHouse outbox is resumed and does not charge again;
- a fresh batch is charged in the same serializable SQLite transaction which
  creates its immutable batch identity, visibility sequence, and outbox;
- the durable quota-admission marker survives a provably unsent abandonment,
  so an exact retry can create replacement outbox work without being charged
  twice;
- ambiguous sends, reconciler retries, stream takeover, process restart, and
  concurrent exact duplicates all reuse the first durable admission.

Quota-admission markers are removed only when their immutable batch identities
leave the documented terminal replay horizon. A replay older than that horizon
may be admitted and charged as new, matching the existing bounded duplicate
contract.

## Collector backpressure

When quota admission fails, the server sends two independently sequenced
responses:

1. `RetryBatch` for the current batch with reason `RATE_LIMITED` and the
   computed retry delay. The collector retains and resends the exact batch.
2. `Throttle` for subsequent sends with reason `TOKEN_QUOTA` or
   `INDEX_QUOTA`, the same minimum delay, and an `effective_until` derived from
   server time.

The throttle leaves `max_in_flight_batches`, `max_batch_events`, and
`max_batch_bytes` at zero. Those zeroes preserve negotiated limits and avoid
making an already-durable batch impossible to resend. `Throttle` acknowledges
nothing; failure to deliver it does not change the preceding `RetryBatch`
contract.

The collector retains each batch's latest retry deadline across stream
disconnects, so reconnecting and rewinding the WAL cannot bypass a delivered
`RetryBatch` when the following `Throttle` was lost. It derives throttle
duration from the server's `effective_until - CollectResponse.sent_at`
interval and anchors that interval at local receipt time; it never compares an
absolute server timestamp directly with an unsynchronized collector wall
clock. The minimum delay is a conservative lower bound, and outbound batch
sends recheck the latest pacing state after a blocking WAL dequeue.

## Required coverage

The implementation must cover zero/unlimited and hard-bound policy validation,
exact schedule boundaries, oversized batches, mixed-index atomicity, token and
index contention, policy changes, backward clocks, arithmetic overflow,
durable restart, concurrent exact duplicates, abandoned and ambiguous storage
outcomes, response sequencing, and real ClickHouse proof that a denied batch
creates no event rows before its retry becomes eligible.
