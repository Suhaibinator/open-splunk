# Backend checkpoint handoff

This is the canonical restart point for backend work. The checkpoint is the
`main` commit containing this document; after a fresh clone or pull, confirm it
with:

```sh
git status --short --branch
git log -1 --oneline
```

## Pause checkpoint: exact Dynamic numeric binning

Date: 2026-07-24

Branch: `main`

Starting commit for this slice:
`0449a405b210eab77acb17fe45e08507dfbcbbf1`
(`record exact bin restart checkpoint`)

Work is intentionally paused after a green, committed, and pushed checkpoint.
The overall backend goal is still active; this is a safe stopping point, not a
claim that the product architecture plan is complete.

## What this slice completed

The previously planned exact-Decimal `bin`/`bucket` slice is implemented.

- Runtime numeric String and stored `decimal/v1` inputs use exact lexical
  decimal arithmetic for `floor(value / span) * span`.
- Exact input is bounded at 4,096 bytes and the public exact result is bounded
  to signed `Int256`. The parser compares bounds lexically before any
  potentially wrapping conversion.
- Negative fractions use mathematical floor, signed `Int256` minimum is
  handled through unsigned magnitude and two's-complement conversion, and zero
  is canonicalized.
- Exact integral String results retain `Int64` or `UInt64` when possible.
  Wider integral results become physical `Int256` with semantic type Decimal.
- Fractional/exponent String results inside `[-2^53, 2^53]` are published as an
  exactly representable `Float64`; wider exact boundaries use the
  Decimal/`Int256` representation.
- Valid stored Decimal envelopes always publish semantic Decimal backed by
  `Int256`. A calculated Decimal can be binned again.
- An ordinary String that is nonnumeric, oversized, invalid UTF-8,
  non-finite-looking, exponent-overflowing, or outside the exact result bound
  passes through unchanged. A value declared Decimal that is malformed,
  oversized, or out of range fails with the sanitized unsupported-bin marker.
- Missing, explicit-null, stale-metadata, `AS`, in-place overwrite, and
  prior-destination preservation behavior remains unchanged.
- Mathematically integral preserved Decimal envelopes—including exponent and
  fractional spellings—and newly produced `Int256` Decimals compare exactly
  with one another in `search`/`where`, remain distinct in `sort`, and converge
  correctly in `stats count BY`.
- Field catalogs and exact field summaries recognize calculated
  semantic-Decimal/physical-`Int256` values and encode them canonically.

The ClickHouse lowering was changed to keep the exact path usable under hostile
query shapes:

- Lexical decomposition now uses scalar-local singleton `arrayMap` bindings
  and only two relational projection layers. The earlier nested projection
  design caused ClickHouse's analyzer to duplicate a previous bin expression,
  consuming more than 1.2 GiB and several minutes for one row.
- A calculated Dynamic value consumed by a later bin is bound once as a
  singleton `arrayJoin([tuple(value, presence, semantic_type)])`. This is a
  one-to-one streaming analyzer fence; it prevents recursive alias expansion
  without an aggregate, join, window, or source rescan.
- The exact re-bin and recoverable-String retry tests run with a 256 MiB
  ClickHouse query cap, a 15-second server execution cap, one thread, and a
  20-second Go context. Both pass.
- Field-summary lowering materializes its narrow typed projection and appends
  `SETTINGS enable_materialized_cte = 1`, preventing analyzer duplication of a
  complex calculated field pipeline.

The result transport now handles real wide Dynamic values:

- `clickhouse-go` can advertise `interface{}` as the scan type for a Dynamic
  column but cannot scan a concrete `Int256` alternative into that destination.
- The executor now supplies `chcol.Dynamic` for Dynamic/Variant database types,
  preserving the concrete alternative for the existing typed conversion path.
- Unit and pinned ClickHouse integration coverage transports
  `Dynamic(Int256)` through `Executor.Execute` and verifies the exact Decimal
  result.

## Current backend state

The backend already includes the following larger slices from earlier
checkpoints:

- durable collector queue/checkpoint and ingestion acknowledgment coupling,
  scoped ingestion tokens, canonical typed events, and ClickHouse storage;
- bounded search jobs with cancellation, history, progress, result leases,
  typed paging, timelines, field catalogs, exact field summaries, and bounded
  CSV/JSONL exports;
- binary protobuf WebSocket progress and previews with replay,
  resynchronization, bounded queues, lifecycle handling, and graceful
  shutdown;
- the documented SPL v0.1 subset: base search, comparisons and Boolean
  expressions, `fields`, `table`, `rename`, `sort`, `head`, `tail`, `dedup`,
  the documented `eval`/`where` subset, `stats` (`count`, `sum`, `avg`, `p95`),
  `top`, `rare`, `timechart`, bounded two-field `chart`, extraction-mode
  `rex`, and explicit-span `bin`/`bucket`;
- source-located diagnostics and compiler/executor row, byte, time, memory,
  command-count, and generated-SQL limits;
- materialized-CTE single-scan lowering for runtime-wide chart/timechart and
  the exact field-summary path; and
- defensive browser preview/result integration from the earlier checkpoints.

Read these documents before changing semantics:

- `docs/product-architecture-plan.md`
- `docs/spl-compatibility-v0.1.md`
- this file

## Validation evidence

The final non-Docker checkpoint validation is:

```sh
go test ./... -count=1 -timeout=5m
go vet ./...
go build ./...
```

The exact slice also passed focused unit suites repeatedly:

```sh
go test ./internal/clickhouse ./internal/queryexec -count=1
```

Pinned integration tests use
`clickhouse/clickhouse-server:26.3.17.4`. The following focused runs passed
during this slice:

```sh
OPEN_SPLUNK_CLICKHOUSE_INTEGRATION=1 \
  go test ./internal/queryexec \
  -run '^TestExecutorAndManagerAgainstClickHouse/native_typed_scan_types$' \
  -count=1 -timeout=3m

OPEN_SPLUNK_CLICKHOUSE_INTEGRATION=1 \
  go test ./internal/clickhouse \
  -run '^TestBinEdgeNumericDynamicStringsAgainstClickHouse/(published_exact_Decimals_can_be_binned_again|consecutive_bins_retry_recoverable_numeric_text)$' \
  -count=1 -timeout=3m
```

Additional pinned groups passed for:

- tagged Decimal exact values, signed `Int256` limits, the 4 KiB lexical
  ceiling, and declared-Decimal failure behavior;
- exact downstream `search`, `where`, `sort`, and `stats count BY`;
- mixed preserved Decimal envelopes and new `Int256` buckets, including
  exponent/fraction integral forms at signed-`Int256` boundaries;
- field catalog and field-summary Decimal metadata;
- missing-source and explicit-null preservation;
- calculated `rex`/`eval` inputs;
- consecutive-bin row counts and semantic retry behavior; and
- the streaming physical-plan assertion.

Do not count a skipped opt-in test as database validation. If Docker is
unavailable, record the omission explicitly.

## Adversarial review record

Three independent reviewers examined the slice from different angles:

1. SPL and numeric semantics, including negative floor, signed-`Int256`
   boundaries, chained bins, String retry behavior, and exact-vs-approximate
   representation.
2. ClickHouse SQL shape, analyzer alias expansion, streaming/cardinality
   behavior, memory and execution bounds, and materialized-CTE use.
3. Test design and transport, including bounded analyzer regressions,
   `Dynamic(Int256)` driver behavior, and missing complexity guards.

Their concrete findings produced the scalar lambda parser, calculated-Dynamic
row-local binding, bounded re-bin/retry tests, and real executor transport
coverage. The final gate also caught an exponent-form Decimal comparison that
would have collapsed adjacent values above `2^53`; the checkpoint includes its
bounded exact parser and pinned comparison/sort regression. Re-run adversarial
review after changing the numeric representation, the analyzer fence, or
downstream Dynamic comparison logic.

## Remaining work, in priority order

### 1. Close the exact-bin hardening gaps

- Add raw malformed Decimal-envelope integration fixtures covering missing
  keys, extra keys, the wrong tag/type pairing, noncanonical payloads, and
  invalid payload grammar.
- Prove malformed or otherwise poisonous rows outside the authorized
  tenant/index/time/visibility scope cannot affect a valid query. Existing
  generic Dynamic poison visibility tests are not a substitute for this
  Decimal-envelope matrix.
- Add a compiler-tracked generated relational-depth budget. The existing
  generated-SQL byte ceilings remain active, but they do not directly bound
  ClickHouse analyzer depth. Pin the accepted boundary and the
  `SPL_QUERY_TOO_COMPLEX` rejection immediately above it.
- Consider a direct unsigned-magnitude arithmetic fast path for calculated
  semantic-Decimal/physical-`Int256` input. The current bounded lexical re-bin
  is correct and passes its resource test, but converting an already typed
  `Int256` to text and reparsing it is avoidable work.
- Run the full opt-in ClickHouse and query-executor integration suites after
  those changes, not only focused subtests.

### 2. Complete the first-release analytical SPL surface

- Implement `spath` in contract-first, test-driven slices with pinned
  ClickHouse fixtures.
- Extend `stats` beyond `count`, `sum`, `avg`, and `p95`: `dc`, `values`,
  `list`, `min`, `max`, `earliest`, `latest`, and the remaining percentile
  forms still need parser, plan, compiler, and integration coverage.
- Decimal `sum`/`avg`/`p95` currently use the documented finite-`Float64`
  compatibility path. Exact Decimal aggregation is a separate feature and
  must not be implied by exact `bin`.
- Later `chart` slices remain: additional aggregates, explicit
  `limit`/`useother`/`usenull`, the single-split form, and supported axis
  discretization. Reuse the existing bounded runtime-wide transport.
- Extend `rex` only behind compatibility tests. Unsupported areas include
  multi-match output, `offset_field`, sed mode, wildcard/quoted fields, and
  PCRE-only constructs.

### 3. Finish first-release product proof

- Add deterministic frontend component/browser tests for reconnect rejection,
  stale-frame fencing, resynchronization, expiration, and preview-to-final
  result replacement.
- Add the browser-visible end-to-end path:
  generated log -> collector durable acknowledgment -> ingestion ->
  ClickHouse -> SPL job -> WebSocket preview/progress -> paged authoritative
  result rendered in the UI.
- Exercise the product from an empty working directory using the compiled
  embedded-UI server binary, as required by the architecture acceptance
  criteria.
- Run and record a load/performance harness against the sustained 1,000
  events/second target, including collector offline recovery, slow WebSocket
  consumers, concurrent searches/previews, and ClickHouse scan budgets.

### 4. Continue Phase 3 and Phase 4 hardening

- Per-index retention and permissions, index/app administration, token and
  collector-fleet operations, reports/dashboards, HEC compatibility, RBAC, and
  audit search.
- Migration upgrade tests, backup/restore and disaster recovery, load
  shedding, fair scheduling, per-user concurrency, alerts/scheduled searches,
  packaging, installers, upgrades, and signed releases.
- Keep the single-node design excellent; distributed ClickHouse/search/control
  plane work remains outside the current plan.

## Known compatibility boundaries

- `bin` still requires one exact field and an explicit bounded span.
  Automatic `bins`/`minspan`, `start`/`end`, `aligntime`,
  calendar/subsecond/logarithmic spans, wildcards, and multiple fields remain
  unsupported.
- Exact `bin` does not make all Decimal operations exact. Nonintegral stored
  Decimal comparisons and numeric aggregates retain their documented
  finite-`Float64` compatibility behavior unless they consume an exact
  integral bucket representation.
- A live licensed Splunk differential oracle is still unavailable. The public
  Splunk documentation does not settle all numeric-text, Decimal, result-type,
  or failed-conversion behavior. Keep Open Splunk's chosen behavior explicit
  in `docs/spl-compatibility-v0.1.md` and do not present it as verified Splunk
  parity.
- Do not accept a changing legal agreement or start a licensed Splunk image on
  the user's behalf merely to obtain the oracle.

## Safe resume procedure

1. Confirm `main` is clean and matches `origin/main`.
2. Read the three documents named above.
3. Run the non-Docker validation commands.
4. Check that no stale `open-splunk-*` Docker test containers are running.
5. Start with Remaining work item 1 and write the failing test before changing
   the compiler.
6. Keep working on `main`; commit and push each cohesive green slice.
7. Preserve unexpected local changes and never reset them away.
