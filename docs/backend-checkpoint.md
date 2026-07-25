# Backend checkpoint handoff

This is the canonical restart document for backend work. Read it together with:

- `docs/product-architecture-plan.md`
- `docs/spl-compatibility-v0.1.md`
- the latest `main` commit

## Pause checkpoint: bounded exact `stats dc`

Date: 2026-07-25

Branch: `main`

Starting commit for this slice:
`f211eaac5bcdb2ddf227927ba2f9e889da7c0887`
(`record spath backend checkpoint`)

Feature commit for this slice:
`1e14eb692151bf8f055257d3d1442f6bd578feba`
(`implement bounded exact stats distinct count`)

The overall backend objective remains active. Work is intentionally paused at a
green, committed, and pushed checkpoint because the user asked for a safe
break, not because the product architecture plan is complete.

## What this slice completed

Open Splunk now implements exact, bounded `stats dc(field)` and
`stats distinct_count(field)` from SPL source through ClickHouse execution and
typed result transport.

### SPL, AST, and plan

- Function names are case-insensitive.
- Both accepted spellings canonicalize the default output to `dc(field)`;
  explicit `AS` aliases are supported and recommended for downstream use.
- Exactly one unquoted exact input field is required. `count(field)`, wildcard
  fields, quoted fields, and arbitrary expressions remain unsupported.
- The existing ceilings of 16 measures and 16 `BY` fields are now defined once
  and revalidated by parser, planner, and compiler.
- Hand-built ASTs and plans cannot bypass:
  - measure or group-field limits;
  - function-specific input and percentile metadata;
  - canonical field references;
  - output-field validity and uniqueness; or
  - the open event schema's reserved `fields` convenience payload.
- A prior transforming command or exact `table` may close the schema and
  declare an ordinary field named `fields`.
- The editor catalog and compile compatibility corpus advertise the new
  aggregate.

### Value and result semantics

- Equality is case-sensitive and uses the existing stats lexical scalar
  normalization:
  - integer `1` and String `"1"` converge;
  - String `"1.0"` and `"01"` remain distinct;
  - Bool `true` and String `"true"` converge; and
  - Bytes, timestamp, duration, and decimal tags use their deterministic
    lexical payloads.
- Missing and explicit-null inputs contribute nothing. Empty String is one
  distinct value.
- A top-level multivalue input contributes each scalar member. Null members
  are ignored and duplicates collapse within and across events.
- Generic objects, empty objects, nested arrays, and object/nested-container
  multivalue members fail the live search with a redacted unsupported-value
  error. They are never silently stringified or ignored.
- A successful `rex` or `spath` scalar overwrite replaces an older object for
  aggregation. A failed extraction preserves the older object and therefore
  still fails.
- Global aggregation over zero rows or zero eligible values emits one
  non-null `UInt64(0)`. Grouped zero input emits no groups; a retained group
  with no eligible measure value emits zero.
- A projected-away input remains absent and contributes zero. The compiler
  does not resurrect hidden event data.
- Rows outside tenant/index/time/visibility scope, removed by an upstream
  filter, or ineligible because a `BY` tuple is incomplete do not contribute
  or expose measure poison.
- Downstream numeric predicates and repeated stats consume the `UInt64`
  result correctly. The manager integration corpus transports
  `distinct_count(logger) AS unique_loggers` as unsigned `5`.

### Exact ClickHouse lowering and bounds

- Each measure uses `groupUniqArrayArray(100001)` over `Array(String)`:
  - results through 100,000 distinct values per group per measure are exact;
  - the 100,001st distinct value fails atomically;
  - results are never truncated or approximated; and
  - no `ARRAY JOIN` can multiply rows or corrupt sibling measures.
- ClickHouse's query memory limit remains an independent byte and total-state
  bound across strings, groups, and measures.
- Dynamic multivalue validation and lexical conversion occur in one array
  pass. Repeated measures on the same input reuse one normalized string-array
  column.
- The raw cardinalities are followed by a whole-result window overflow flag
  and an immediate validation predicate. This forces every group to validate
  before later filters, projections, sorting, or `head`; a valid first group
  cannot hide an overflowing later group.
- Pinned `EXPLAIN actions=1` coverage proves that repeated equivalent aliases
  share one physical ClickHouse aggregate state.
- Measure-container and cardinality-limit markers map to stable public
  unsupported-value and execution-limit errors without leaking generated SQL,
  values, or storage details.

### Extraction overlay correction

The adversarial `dc` review exposed a pre-existing calculated-field provenance
bug. `rex` and `spath` previously retained an older object's descendant proof
even after a successful scalar overwrite. Both stages now materialize a
private, match-conditioned descendant alias:

```text
no match AND prior descendant exists
```

That alias participates in private-column liveness, field catalog/summary
analysis, projections, binning, and later aggregation. Successful overwrites
clear the old container proof; misses preserve it. Existing binary, type,
presence, and calculated-overlay behavior remains intact.

## Adversarial review record

Independent reviewers attacked semantics/security, ClickHouse
correctness/performance, code reuse, maintainability, efficiency, and tests.
Concrete findings fixed before this checkpoint included:

1. `distinct_count(field)` initially used the wrong default output instead of
   canonical `dc(field)`.
2. Dynamic containers could be silently omitted, and flattened object parents
   needed explicit descendant detection.
3. Open-schema `stats ... fields` and forged measure/group limits needed
   planner and compiler defenses.
4. Forged `count(field)` metadata could be erased by the planner before the
   compiler saw it.
5. Stats limits and aggregate-name metadata were duplicated across layers.
6. Dynamic multivalue `dc` validation walked arrays twice.
7. Boundary tests hardcoded 100,000/100,001 instead of deriving them from the
   production constant.
8. A per-projected-group throw allowed `head 1` to hide an overflowing later
   group.
9. A successful `rex` or `spath` scalar overwrite could inherit an older
   object's descendant poison flag.
10. The new descendant alias was initially pruned by field analysis until it
    was added to private-column liveness.
11. The first physical-plan assertion matched the wrong ClickHouse
    `EXPLAIN actions=1` label.

After the fixes and pinned reruns, the semantic and ClickHouse reviewers
reported no remaining P0-P2 finding.

## Validation evidence

Focused packages passed:

```sh
go test ./internal/spl ./internal/plan ./internal/clickhouse \
  ./internal/queryexec -count=1 -timeout=5m
```

The pinned `clickhouse/clickhouse-server:26.3.17.4` store suite passed:

```sh
OPEN_SPLUNK_CLICKHOUSE_INTEGRATION=1 \
  go test ./internal/clickhouse \
  -run '^TestStoreAgainstClickHouse$' \
  -count=1 -timeout=8m -v
```

Its `dc` coverage includes:

- canonical scalar identity, case distinctions, empty String, null, missing,
  scalar/multivalue convergence, duplicates, and nested-container failures;
- global/grouped empty behavior and projected-away inputs;
- foreign-tenant container poison and incomplete-`BY` isolation;
- exact 100,000 success and sanitized 100,001 failure;
- overflow in a later group remaining fatal behind `ORDER BY ... LIMIT 1`;
- mixed `count`, `sum`, `avg`, and repeated `dc` without row expansion;
- shared physical aggregate state;
- downstream and repeated aggregation; and
- successful `rex`/`spath` overwrite of an older object.

The full query executor and manager suite passed:

```sh
OPEN_SPLUNK_CLICKHOUSE_INTEGRATION=1 \
  go test ./internal/queryexec \
  -run '^TestExecutorAndManagerAgainstClickHouse$' \
  -count=1 -timeout=10m -v
```

The final ordinary gates passed:

```sh
go test ./... -count=1 -timeout=5m
go vet ./...
go build ./...
npm run build
npm run test:frontend
git diff --check
```

Do not count a skipped opt-in test as database validation.

## Current backend state

The backend now includes:

- durable collector queue/checkpoint and ingestion acknowledgement coupling;
- scoped ingestion tokens, canonical typed events, and ClickHouse storage;
- bounded search jobs with cancellation, history, progress, result leases,
  typed paging, timelines, field catalog, field summary, and CSV/JSONL export;
- binary protobuf WebSocket previews/progress with replay and resynchronization;
- the documented SPL v0.1 base search and Boolean/comparison expressions;
- `fields`, `table`, `rename`, `sort`, `head`, `tail`, and `dedup`;
- the documented `eval`/`where` subset;
- `stats` with `count`, `dc`/`distinct_count`, `sum`, `avg`, and `p95`;
- `top`, `rare`, `timechart`, and bounded two-field `chart`;
- extraction-mode `rex`, explicit-span exact `bin`/`bucket`, and bounded
  explicit-path `spath`;
- compiler/executor limits for rows, bytes, time, memory, commands, generated
  SQL bytes, relational depth, extraction outputs, JSON work, and exact
  distinct state; and
- materialized-CTE single-scan lowering for runtime-wide and
  analyzer-sensitive paths.

## Remaining work, in priority order

### 1. Complete the analytical SPL surface

- Implement bounded `stats values(field)` next. Before coding, pin:
  - lexical ordering and case behavior;
  - top-level multivalue flattening;
  - result multivalue transport through ClickHouse and the job API;
  - exact cardinality/byte ceilings and whole-result validation; and
  - downstream use of multivalue results.
- Then add `list`, `min`, `max`, `earliest`, `latest`, `count(field)`, and the
  remaining percentile forms behind equivalent compatibility tests.
- Decimal `sum`/`avg`/`p95` still use the documented finite-Float64
  compatibility path; exact Decimal aggregation is separate work.
- Add later `chart` forms and aggregate functions while preserving bounded
  runtime-wide transport.
- Extend `rex` only behind compatibility tests: multi-match output,
  `offset_field`, sed mode, wildcard/quoted fields, and supported PCRE gaps.
- Later `spath` slices are auto-extraction and its 5,000-character discovery
  cutoff, `{}`/multivalue output, XML, containers, dynamic paths, escaped
  literal-dot keys, and the `spath()` eval function.

### 2. Improve proven performance without weakening bounds

- Preserve the whole-result `dc` validation barrier. Any replacement must
  retain the later-group-behind-`LIMIT` regression.
- Preserve one-pass Dynamic multivalue normalization and shared physical
  aggregate-state coverage.
- Consider a shared, tested conditional-overlay helper for `rex` and `spath`;
  their match-conditioned descendant aliases now provide the missing
  provenance contract.
- Add a frontend regression that keeps the supported and unsupported editor
  command/function catalogs aligned with the backend.
- Centralize the repeated pinned ClickHouse migration/store bootstrap used by
  the growing integration suites.
- Run the complete opt-in ClickHouse and query-executor packages serially after
  changing aggregate lowering, Dynamic metadata, or analyzer fences.

### 3. Finish first-release product proof

- Add deterministic frontend tests for reconnect rejection, stale-frame
  fencing, resynchronization, expiration, and preview-to-final replacement.
- Add the browser-visible path from generated logs through durable collection,
  ClickHouse, SPL job execution, WebSocket preview/progress, and authoritative
  paged results.
- Exercise the compiled embedded-UI server from an empty working directory.
- Record a load/performance run against the sustained 1,000 events/second
  target, including offline recovery, slow WebSocket consumers, concurrent
  searches, high-cardinality `dc`, and scan budgets.

### 4. Continue Phase 3 and Phase 4 hardening

- Per-index retention and permissions, index/app administration, token and
  collector-fleet operations, reports/dashboards, HEC compatibility, RBAC, and
  audit search.
- Migration upgrade tests, backup/restore and disaster recovery, load
  shedding, fair scheduling, per-user concurrency, alerts/scheduled searches,
  packaging, installers, upgrades, and signed releases.
- Keep the single-node design excellent; distributed control/search work
  remains outside the current plan.

## Known compatibility boundaries

- A live licensed Splunk differential oracle is unavailable. Public Splunk
  documentation leaves several failure and edge-type behaviors unspecified.
  Keep Open Splunk's chosen behavior explicit and do not present it as verified
  parity.
- Collector decoding does not preserve every original numeric token spelling.
  For example, JSON `1.0` may be stored as Float64 with canonical search
  spelling `"1"`. `dc` is exact over stored typed values, not lost source
  bytes.
- Default aggregate names containing parentheses cannot yet be referenced by
  the downstream field grammar; use `AS`.
- The 100,000 ceiling is per group per measure. The query memory limit remains
  authoritative across all groups and measures.
- Duplicate JSON member selection follows the pinned ClickHouse parser's first
  member behavior.
- Do not accept a changing legal agreement or start a licensed Splunk image on
  the user's behalf merely to obtain an oracle.

## Safe resume procedure

1. Confirm `main` is clean and exactly matches `origin/main`.
2. Read the three documents listed at the top.
3. Run the ordinary validation commands above.
4. Check that no stale `open-splunk-*` Docker test containers are running.
5. Begin with bounded `stats values(field)` contract research and tests unless
   the user changes priority.
6. Preserve the 100,000/100,001, later-group-behind-`LIMIT`, one-pass
   normalization, physical-state-sharing, scope-poison, calculated-overwrite,
   field-analysis, and unsigned-manager-transport regressions.
7. Keep working on `main`; commit and push each cohesive green slice.
8. Preserve unexpected local changes and never reset them away.
