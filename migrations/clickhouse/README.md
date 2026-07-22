# ClickHouse migrations

This directory is the ordered, forward-only schema for the shared
`open_splunk.events` table. Files use contiguous four-digit versions and are
safe to retry. ClickHouse DDL is not transactional, so each migration performs
idempotent DDL first and writes its `schema_migrations` ledger row last.

## Event storage choices

- One table stores every logical index. `tenant_id` and `index_name` lead the
  primary/sorting key so authorized index and time filters prune data early.
- Monthly `event_time` partitions keep partition cardinality bounded and make
  old data manageable. The sorting key adds an hour bucket, exact event time,
  and stable event ID. The in-memory primary key intentionally omits
  `event_id`.
- `event_time` preserves event nanoseconds and `index_time` records server
  acceptance time. The ingestion service resolves per-index retention into
  `expires_at`; table TTL removes expired rows during background merges. We
  intentionally do not enable `ttl_only_drop_parts`, because a shared partition
  may contain indexes with different expiry times.
- Promoted columns cover the Splunk event model and common filters. Optional
  promoted strings remain nullable so absent and empty are distinguishable.
- `fields` uses ClickHouse's production native `JSON` type with at most 256
  dedicated paths and 16 physical types per dynamic path. Overflow remains
  queryable through ClickHouse shared-data serialization instead of creating an
  unbounded number of files. `field_names` is a sorted, unique list of
  normalized dotted paths for field discovery.
- Bloom filters accelerate exact event/trace/span lookups. The GA native text
  index covers case-folded raw text and the normalized field-name array. The
  query corpus and load generator must benchmark these before changing their
  tokenizer or granularity.

The SPL compiler exposes canonical aliases rather than leaking these physical
names: `index` maps to `index_name`, `_time` to `event_time`, `_indextime` to
`index_time`, `_raw` to `raw`, and `message` to `body`.

The schema targets `clickhouse/clickhouse-server:26.3.17.4`, the concrete patch
of the current 26.3 LTS line used by local deployment. Native JSON has been
production-ready since 25.3, and native full-text indexes are GA from 26.2.
The version decisions are checkable against the [Docker Official Image tag
list](https://hub.docker.com/_/clickhouse), [native JSON
reference](https://clickhouse.com/docs/sql-reference/data-types/newjson), and
[full-text GA announcement](https://clickhouse.com/blog/full-text-search-ga-release).

## Typed JSON insertion

The writer must send a JSON object, not a JSON-encoded string, and keep
ClickHouse input inference from coercing numbers or booleans to strings. When
using `JSONEachRow`, use these settings (native-driver typed insertion should
produce the equivalent values):

```sql
SETTINGS
    input_format_json_read_numbers_as_strings = 0,
    input_format_json_read_bools_as_numbers = 0,
    input_format_json_read_bools_as_strings = 0,
    input_format_json_infer_array_of_dynamic_from_array_of_different_types = 1,
    input_format_try_infer_dates = 0,
    input_format_try_infer_datetimes = 0
```

Disabling date inference is deliberate: an application string that resembles a
date must remain a string unless the collector's typed-value envelope says
otherwise. Enabling dynamic-array inference preserves heterogeneous protobuf
lists instead of coercing all members to one common scalar type.

`JSONEachRow` is suitable for schema smoke tests and UTF-8 fixtures, but it is
not the production representation for every column. `raw` is a ClickHouse
`String` specifically because that type can hold arbitrary bytes; the Go writer
must bind the collector's `[]byte` through a byte-safe/native insertion path.
It must not UTF-8-repair or base64-replace the original bytes.

The protobuf value domain is wider than JSON's scalar domain. Bytes,
timestamps, durations, and decimals need an explicit lossless tagged or native
mapping before the server may acknowledge events containing them; silently
turning those values into ordinary JSON strings would violate the wire
contract. `field_names` also carries semantic information: ingestion must
derive sorted, unique normalized paths recursively, including explicitly-null
paths, so SPL can distinguish a present null from a missing path. Literal dots
in source field names require one canonical escaping/collision policy.

## Retry and deduplication contract

The collector protocol is at-least-once. For each accepted batch, the server
must issue one deterministic ClickHouse insert whose rows are in original event
order and set `insert_deduplication_token` to an unambiguous value derived from
protocol version, tenant, collector ID, and batch ID. A retry must reuse the
same token, accepted row subset, row order, insert settings, and payload bytes.
If a batch must be split into multiple inserts, each stable slice needs a stable
token suffix and must never be re-sliced on retry.

Server-derived values are part of that stable inserted block. In particular,
`index_time` must be fixed for the first batch attempt and reused after
reconnect, while `expires_at` must be resolved by the store from the authorized
tenant/index retention policy. Neither value may come from collector payload.
If the server cannot reconstruct the exact accepted block after a process
restart, it needs a durable batch ledger rather than assuming the insert token
alone makes a changed block idempotent.

The non-replicated `MergeTree` remembers 10,000 recent insert blocks locally.
That is a count-bounded retry window, not a time guarantee. Once a token leaves
the window—or after loss of ClickHouse's local deduplication metadata—the same
insert can be accepted again. `event_id` is therefore retained for diagnostics
and future reconciliation, and the product must not promise global exactly-once
delivery. The server acknowledges a batch only after ClickHouse commits it; a
retry response or ambiguous error leaves the collector's durable batch intact.

`ReplacingMergeTree` is intentionally not used as a fallback: its merge-time
deduplication is eventual and would require `FINAL` on ordinary SPL searches.

## Tests

Fast schema-contract tests are part of the default Go suite:

```sh
go test ./migrations/clickhouse
```

The opt-in smoke test starts the pinned image without publishing any ports,
applies every migration twice, inserts typed nested JSON larger than JavaScript's
safe integer range, and verifies retry-token deduplication:

```sh
OPEN_SPLUNK_CLICKHOUSE_INTEGRATION=1 \
  go test ./migrations/clickhouse -run AgainstClickHouse -v
```

Set `OPEN_SPLUNK_CLICKHOUSE_TEST_IMAGE` only when deliberately validating an
upgrade candidate. Passing the smoke test is necessary but not sufficient for
changing the deployment pin; run the query and ingestion performance corpus too.
