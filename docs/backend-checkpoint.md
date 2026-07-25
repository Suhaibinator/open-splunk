# Backend checkpoint handoff

This is the canonical restart point for backend work. The checkpoint is the
`main` commit containing this document; after a fresh clone or pull, confirm it
with:

```sh
git status --short --branch
git log -1 --oneline
```

## Pause checkpoint: bounded relational depth and analyzer-safe predicates

Date: 2026-07-25

Branch: `main`

Starting commit for this slice:
`05e86f8c37c2fb187987970ad61b9b0a75da5564`
(`harden exact Decimal poison isolation`)

Work is intentionally paused after a green, committed, and pushed checkpoint.
The overall backend goal is still active; this is a safe stopping point, not a
claim that the product architecture plan is complete.

## What this slice completed

The compiler now admits generated relational SQL by its structure as well as
its byte size:

- The longest generated `SELECT`/`UNION` dependency path is tracked while SQL
  is lowered and capped at 96 levels. The model uses maximum dependency height,
  not a textual `SELECT` count or the sum of independent CTE/`UNION` siblings.
- Every ordinary and terminal compiler path reports its structural depth,
  including dynamic numeric `bin`, `rex`, fixed and dynamic `dedup`, aggregate
  validation/preaggregation, `timechart`, `chart`, field catalog, field
  summary, and timeline lowering.
- The exact 96-level boundary compiles. The next level fails before execution
  with source-located `SPL_QUERY_TOO_COMPLEX`; terminal and private-analysis
  paths have their own 96/97 boundary coverage.
- Analysis finalizers fail closed when they omit depth evidence. Directly
  forged `eval` and `rename` plans are also revalidated against the existing
  64-assignment contract inside the compiler.
- The executor pins ClickHouse `max_subquery_depth` independently to 100. The
  server and compiler metrics intentionally are not treated as interchangeable.
  ClickHouse exception 162 (`TOO_DEEP_SUBQUERIES`) is redacted and classified
  as a stable execution-limit error.
- A pinned `clickhouse/clickhouse-server:26.3.17.4` integration test executes
  the accepted 96-level query under a 30-second execution cap, 256 MiB memory
  cap, one thread, and a 1 MiB query-size cap.

The full opt-in database gate exposed and fixed a separate ClickHouse analyzer
failure in a supported pipeline:

- `bin <Dynamic> ... | where <bucket> ...`, when wrapped by timeline or another
  analysis query, let ClickHouse push the predicate back through the large
  exact-bin projection. The pinned server expanded that graph to roughly
  6.7 GiB even though the query read only a tiny fixture.
- The filter now materializes the already calculated row relation and binds
  only predicate-referenced calculated values through singleton `ARRAY JOIN`
  aliases. The predicate depends on those aliases, which prevents pushdown
  into the producer. `SELECT * EXCEPT (...) REPLACE (...)` republishes the
  identical bound values in their original positions, so the fence is durable
  across later filters.
- The fence remains one row per input row, performs one scoped storage scan,
  preserves value/presence/semantic-type metadata, adds no relational level
  beyond the filter's existing `SELECT`, and carries its required
  `enable_materialized_cte` setting in the query text for direct compiler
  clients.
- Dependency tracking follows calculated values through projection, rename,
  `tonumber`, `replace`, fixed numeric re-binning, and `rex` fallback paths.
  Unrelated predicates do not materialize the relation, multiple affected
  fields share one fence, and a second predicate over a durably rebound field
  does not create another temporary table.
- The formerly failing field-summary/timeline integration case now completes
  on the pinned server in about three seconds and returns the expected 48
  buckets with two matching events.
- Two stale store-integration expectations were corrected to match the already
  documented exact-bin behavior: a wide negative numeric String produces
  `Int256`, and a valid stored Decimal buckets exactly instead of being
  classified as unsupported.

The preceding exact-Decimal `bin`/`bucket` slice remains implemented and its
raw-storage trust boundary is covered adversarially.

This hardening checkpoint adds direct ClickHouse fixtures that deliberately
bypass `Store` normalization:

- Current-metadata Decimal envelopes with a missing type key, missing value
  key, extra key, wrong tag, wrong semantic type, noncanonical numeric text,
  invalid grammar, an oversized payload, or invalid UTF-8 all fail through the
  single sanitized unsupported-value marker.
- The malformed payload is not exposed in the returned database exception.
- Poison Decimal rows that differ from a valid row only by tenant, authorized
  index, earliest/latest event time, index-time cutoff, or immutable
  visibility sequence cannot affect the authorized query.
- The poison rows deliberately reuse the valid event ID. A forced
  `OPTIMIZE TABLE ... FINAL` puts valid and poison fixtures in the same sorted
  part/granule before the query, preventing part pruning from making the test
  pass accidentally.
- The authorized result still contains exactly the valid row and publishes
  the expected semantic Decimal as physical `Int256`.

The adversarial matrix required no production-code correction. The existing
compiler already classifies the strict two-key `decimal/v1` envelope only
after the storage scan's tenant/index/time/index-time/visibility fences, and
the executor already removes ClickHouse SQL and payload details when mapping
the fixed failure marker.

The exact-Decimal implementation from the preceding checkpoint provides the
following behavior:

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
  command-count, generated-SQL byte, and generated relational-depth limits;
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

The relational-depth slice passed focused and package unit suites:

```sh
go test ./internal/clickhouse ./internal/queryexec -count=1

go test ./internal/clickhouse \
  -run '^(TestCompileBoundsGeneratedRelationalDepth|TestRelationalNodeDepthUsesMaximumDependencyHeight|TestCompiledRelationalDepthPinsRepresentativeOperatorCosts|TestCompiledRelationalDepthPinsTerminalWideOperatorCosts|TestFieldAnalysisRelationalDepthBoundariesIncludePrivateFinalizers|TestCompilerBoundsForgedAssignmentCounts|TestAnalysisFinalizerCannotUnderreportItsRelationalWrapper|TestAnalysisFinalizerMustReportCompiledRelationalDepth|TestBinEdgeCalculatedPredicateUsesOneDurableAnalyzerFence|TestBinEdgeDistinctCalculatedPredicatesUseNestedScopedFences)$' \
  -count=1
```

Pinned integration tests use
`clickhouse/clickhouse-server:26.3.17.4`. The relational-depth boundary passed
during this slice:

```sh
OPEN_SPLUNK_CLICKHOUSE_INTEGRATION=1 \
  go test ./internal/clickhouse \
  -run '^TestRelationalDepthBoundaryAgainstClickHouse$' \
  -count=1 -timeout=4m
```

The complete compiler and query-executor ClickHouse integration packages
passed serially at the final gate:

```sh
OPEN_SPLUNK_CLICKHOUSE_INTEGRATION=1 \
  go test ./internal/clickhouse \
  -count=1 -timeout=20m -parallel=1

OPEN_SPLUNK_CLICKHOUSE_INTEGRATION=1 \
  go test ./internal/queryexec \
  -count=1 -timeout=10m -parallel=1
```

The two defining analyzer regressions were then rerun from the final source
state. Their test bodies pin one thread, short server/client timeouts, a 1 MiB
query ceiling, and 256 MiB for the predicate paths (the independent
field-summary assertion uses 512 MiB, below the executor's 1 GiB default):

```sh
OPEN_SPLUNK_CLICKHOUSE_INTEGRATION=1 \
  go test ./internal/clickhouse \
  -run '^TestBinEdgeMetadataAgainstClickHouse$/bin_output_feeds_field_summaries_and_timelines$' \
  -count=1 -timeout=4m -parallel=1

OPEN_SPLUNK_CLICKHOUSE_INTEGRATION=1 \
  go test ./internal/clickhouse \
  -run '^TestBinEdgePipelineAgainstClickHouse$/distinct_calculated_predicates_use_nested_bounded_fences$' \
  -count=1 -timeout=4m -parallel=1
```

The immediately preceding exact-bin checkpoints also recorded these focused
runs:

```sh
OPEN_SPLUNK_CLICKHOUSE_INTEGRATION=1 \
  go test ./internal/clickhouse \
  -run '^TestBinEdgeNumericDynamicStringsAgainstClickHouse/malformed_Decimal_envelopes_fail_only_inside_the_authorized_snapshot$' \
  -count=1 -timeout=4m

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

For this hardening checkpoint, reviewers separately audited strict Decimal
classification, storage normalization, predicate placement, executor error
sanitization, raw-fixture construction, boundary isolation, and ClickHouse
part-pruning false positives. Their recommendations produced the full
malformed-envelope matrix, individual checks for all six scope fences, the
forced part merge, and the physical-result assertion. No production
correctness defect was found.

For the relational-depth checkpoint, three reviewers independently mapped the
generated SQL graph, recalculated exact per-operator and terminal dependency
heights, and attacked the boundary tests and executor coupling. Their findings
produced maximum-path rather than sibling-sum accounting, forged-plan
assignment validation, fail-closed finalizers, an independently pinned
ClickHouse limit, redaction of server depth failures, and a resource-bounded
database proof. The final audit found no remaining accounting defect across
CTEs, scalar subqueries, materialized dependencies, or `UNION` branches.

The full-suite analyzer failure was then reviewed through independent SQL,
semantic, performance, and test-design attacks. Materialization alone, a
singleton binding alone, and eager binding inside `bin` all failed. Reviewers
proved the final consumer-local materialized-CTE plus predicate-dependent
singleton-binding shape on the pinned server, including two consecutive
filters, multiple bound fields, null/missing values, field catalog, timeline,
and exact value/type/presence preservation. Their propagation review also
caught the initially missing `tonumber`, `replace`, fixed-bin, and `rex`
dependencies before this checkpoint.

## Remaining work, in priority order

### 1. Finish exact-bin efficiency follow-up

- Consider a direct unsigned-magnitude arithmetic fast path for calculated
  semantic-Decimal/physical-`Int256` input. The current bounded lexical re-bin
  is correct and passes its resource test, but converting an already typed
  `Int256` to text and reparsing it is avoidable work.
- Preserve and re-run the full opt-in ClickHouse and query-executor integration
  suites after changing that representation or the analyzer fence, not only
  focused subtests.

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
5. Resume with the optional exact-`Int256` fast path in Remaining work item 1,
   or move directly to the contract-first `spath` slice in item 2. Preserve the
   relational-depth and bounded analyzer tests while changing SQL shape.
6. Keep working on `main`; commit and push each cohesive green slice.
7. Preserve unexpected local changes and never reset them away.
