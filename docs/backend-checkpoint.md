# Backend checkpoint handoff

This is the canonical restart document for backend work. Read it together with:

- `docs/product-architecture-plan.md`
- `docs/spl-compatibility-v0.1.md`
- the latest `main` commit

## Pause checkpoint: exact `stats count(field)`

Date: 2026-07-25

Branch: `main`

Starting commit for this slice:
`de29117d68e330b3b7d4d02482fcc59cf944c955`
(`record stats values backend checkpoint`)

Feature commit for this slice:
`59173ff4ac97a3ade9f1724fc0750cda0193d04c`
(`implement stats count field`)

The overall backend objective remains active. Work is intentionally paused at
a green, committed, and pushed checkpoint because the user requested a safe
break. The product architecture plan is not complete.

## What this slice completed

Open Splunk now implements exact `stats count(field)` from SPL source through
ClickHouse execution, search-job paging, and typed UInt64 result transport.
Argument-free `count` remains the distinct row-count operation.

### SPL, AST, plan, and editor contract

- `count(field)` is case-insensitive and requires exactly one unquoted exact
  field. Its default output is canonical `count(field)`; explicit `AS` aliases
  are supported and needed for downstream use with the current field grammar.
- `count()`, `c(field)`, wildcard and quoted fields, multiple inputs, and
  `count(eval(...))` fail explicitly rather than being approximated.
- Separate AST and plan enum values keep argument-free row count distinct from
  field-occurrence count.
- Parser, planner, and compiler revalidate exact fields, reserved open-schema
  `fields`, output uniqueness, forged input/range/percentile metadata, and
  relational-depth limits.
- The search editor advertises both row and field count in the supported
  `stats` surface.
- The public behavior and typed-container boundary are recorded in
  `docs/spl-compatibility-v0.1.md`.

### Exact occurrence semantics

- Missing, explicit null, an empty multivalue, and null multivalue members
  contribute zero.
- Every other scalar contributes one, including empty String, zero, `false`,
  Bytes, timestamp, duration, and decimal values.
- A top-level multivalue contributes its number of immediate non-null members.
  Duplicates count; nested lists and objects count atomically and are not
  traversed.
- A top-level object contributes one. A non-empty object stored as flattened
  leaves contributes once for its parent, not once per descendant. Direct,
  renamed, projected, `eval`-copied, and extraction-preserved object
  provenance is retained.
- The ordinary scalar/multivalue contract follows Splunk's documented
  occurrence behavior. Null multivalue members and typed containers are
  explicit Open Splunk v0.1 choices because public Splunk documentation does
  not pin those cases.
- Global aggregation over no rows and retained groups with no occurrences
  publish non-null UInt64 zero. Grouped aggregation over no rows publishes no
  groups.
- Projected-away fields remain absent and contribute zero rather than being
  recovered from hidden event columns.

### Efficient ClickHouse lowering and bounds

- Each distinct input has one cached per-row UInt64 occurrence expression.
  Repeated aliases reuse it.
- No `ARRAY JOIN` is used, so sibling counts and numeric aggregates cannot be
  multiplied by multivalue expansion.
- Dynamic arrays use one `arrayCount` over immediate members; fixed
  `Array(String)` results use `length`; fixed nullable scalars use their
  presence and null predicates; and absent compiled fields lower to literal
  zero.
- Dynamic `None` falls back to descendant presence so a copied flattened
  object counts once while an explicit null remains zero.
- Contributions sum in UInt128 and publish as UInt64. The shipped 250-million
  source-row read ceiling, 1 MiB hard event ceiling, absence of row expansion,
  and separate fixed-list bounds make the final cast safe.
- The generated relation adds no wrapper, and the existing relational-depth
  limit remains exact.

## Adversarial review record

Independent agents reviewed SPL semantics, ClickHouse correctness, optimizer
behavior, overflow safety, transport, test coverage, code reuse,
maintainability, and efficiency. Concrete findings and design changes before
the checkpoint included:

1. Row count and occurrence count needed distinct AST/plan enum values rather
   than inferring semantics from optional input metadata.
2. Forged argument-free count plans could carry a source range; the compiler
   now rejects that metadata alongside names, paths, canonical flags, and
   percentiles.
3. A UInt64 aggregate state can wrap in ClickHouse. Occurrence contributions
   now sum in UInt128, with the production row/event bounds documenting why
   UInt64 publication is exact.
4. Container behavior is not fully specified by public Splunk documentation.
   The atomic typed-container behavior is labeled an Open Splunk v0.1 boundary
   instead of an unverified parity claim.
5. Repeated measures needed proof that lowering shares one input computation
   and one physical sum without `ARRAY JOIN`; the pinned `EXPLAIN actions=1`
   regression now enforces this.
6. The first flattened-object compiler test only checked that descendant SQL
   existed. An adversarial reviewer showed `eval copied=object_parent` still
   returned zero because exact Dynamic `None` masked descendant presence. The
   lowering now uses descendants as the None/missing fallback, and an executed
   ClickHouse regression pins the result at one.

The final semantic, ClickHouse, and code-quality re-reviews reported no
remaining blocker and no concrete simplification worth making.

## Validation evidence

The full ordinary Go suite passed after the final adversarial fix:

```sh
go test ./... -count=1 -timeout=5m
```

The pinned `clickhouse/clickhouse-server:26.3.17.4` store suite passed:

```sh
OPEN_SPLUNK_CLICKHOUSE_INTEGRATION=1 \
OPEN_SPLUNK_CLICKHOUSE_TEST_IMAGE=clickhouse/clickhouse-server:26.3.17.4 \
go test ./internal/clickhouse \
  -run '^TestStoreAgainstClickHouse$' -count=1
```

Its new count coverage includes:

- the documented Splunk multivalue differential where two rows contain four
  field occurrences;
- missing/null, empty String, zero, false, Bytes, timestamp, duration,
  decimal, empty/null-only lists, nested members, objects, and duplicates;
- direct and `eval`-copied flattened objects;
- global/grouped empties, projected-away inputs, and fixed multivalue output;
- tenant scope, exact UInt64 results, and one shared physical lowering under
  `EXPLAIN actions=1` with no `ARRAY JOIN`.

The full query executor and manager suite passed:

```sh
OPEN_SPLUNK_CLICKHOUSE_INTEGRATION=1 \
OPEN_SPLUNK_CLICKHOUSE_TEST_IMAGE=clickhouse/clickhouse-server:26.3.17.4 \
go test ./internal/queryexec \
  -run '^TestExecutorAndManagerAgainstClickHouse$' -count=1
```

This pins parser-to-manager transport of non-null UInt64 occurrence counts for
present and absent fields.

The final ordinary gates passed:

```sh
go vet ./...
go build ./...
npm run lint
npm run typecheck
npm run build
npm run test:frontend
git diff --check
```

Do not count a skipped opt-in test as database validation.

## Current backend state

The backend now includes:

- durable collector queue/checkpoint and ingestion acknowledgement coupling;
- scoped ingestion tokens, canonical typed events, and ClickHouse storage;
- bounded search jobs with cancellation, history, progress, typed paging,
  result leases, timelines, field catalog/summary, and CSV/JSONL export;
- binary protobuf WebSocket preview/progress with replay and resynchronization;
- the documented SPL v0.1 base search and Boolean/comparison expressions;
- `fields`, `table`, `rename`, `sort`, `head`, `tail`, and `dedup`;
- the documented `eval`/`where` subset;
- `stats` with row `count`, exact `count(field)`, `dc`/`distinct_count`,
  `values`, `sum`, `avg`, and `p95`;
- `top`, `rare`, `timechart`, and bounded two-field `chart`;
- extraction-mode `rex`, explicit-span exact `bin`/`bucket`, and bounded
  explicit-path `spath`;
- resource limits for rows, bytes, time, memory, commands, generated SQL,
  relational depth, extraction work, exact distinct state, and multivalue
  publication; and
- materialized-CTE single-scan lowering for runtime-wide and
  analyzer-sensitive paths.

## Remaining work, in priority order

### 1. Continue the analytical SPL surface with tests first

- Choose and pin the next aggregate contract before implementation. The
  lowest-risk useful candidates are `min` and `max`.
- Broaden count only behind separate contracts and differential tests for
  `c(field)`, wildcards, predicates, and `count(eval(...))`; do not interpret
  those unsupported forms as exact-field count.
- Treat `list(field)` as a separate order-sensitive, duplicate-preserving,
  bounded multivalue design; do not implement it by adapting unordered
  `values`.
- Add `earliest`, `latest`, and remaining percentile forms only with explicit
  type, null, ordering, approximation, and resource contracts.
- Implement `eventstats` after the aggregate library is stable. Keep
  `streamstats` outside the first release unless scope changes.
- Exact Decimal aggregation remains separate work; current numeric aggregates
  use the documented finite-Float64 compatibility path.
- Extend chart/rex/spath only behind compatibility and ClickHouse integration
  tests. Deferred spath work includes auto-extraction, `{}` multivalue output,
  XML, containers, escaped literal-dot keys, and the `spath()` eval function.

### 2. Improve maintainability and proven performance

- Move pending aggregate publication/validation descriptors out of persistent
  `compileState` into a typed aggregate-compilation result.
- Replace string-prefix presence rebinding with an explicit field presence
  policy shared by projection, rename, and eval.
- Consolidate the growing exact-string descriptors without weakening the
  dc-only scalar path, values/dc state sharing, or two-stage validation depth.
- Split pinned ClickHouse fixture/bootstrap and aggregate assertions into
  smaller helpers.
- Keep regressions for physical aggregate sharing, later hidden overflow,
  binary multivalue transport, scope poison, and relational depth.

### 3. Finish first-release product proof

- Add the browser-visible path from generated logs through durable collection,
  ClickHouse, SPL job execution, WebSocket preview/progress, and authoritative
  paged results.
- Add deterministic frontend tests for reconnect rejection, stale-frame
  fencing, resynchronization, expiration, and preview-to-final replacement.
- Exercise the compiled embedded-UI server from an empty working directory.
- Record a load/performance run against sustained 1,000 events/second,
  including offline recovery, slow WebSocket consumers, concurrent searches,
  high-cardinality exact aggregates, and scan budgets.

### 4. Continue Phase 3/4 product hardening

- Per-index retention and permissions, index/app administration, collector
  fleet operations, reports/dashboards, HEC compatibility, RBAC, and audit
  search.
- Migration upgrades, backup/restore and disaster recovery, load shedding,
  fair scheduling, per-user concurrency, alerts/scheduled searches, packaging,
  installers, upgrades, and signed releases.
- Keep the first release single-node; distributed control/search remains
  outside the current plan.

## Known compatibility boundaries

- A live licensed Splunk differential oracle is unavailable. Public
  documentation leaves several multivalue, null, binary, and error behaviors
  unspecified; keep Open Splunk choices explicit rather than claiming verified
  parity.
- Public Splunk documentation establishes the row-versus-multivalue occurrence
  distinction for `count(field)`, but does not pin null multivalue members or
  typed containers. Open Splunk counts immediate non-null containers
  atomically as an explicit v0.1 extension.
- Collector decoding does not preserve every original numeric token spelling.
  Exact string-oriented stats operate on stored canonical values.
- Default aggregate names containing parentheses cannot be referenced by the
  downstream field grammar; use `AS`.
- The 512 KiB values bound is a publication bound, not an aggregate-state
  memory guarantee. ClickHouse query memory is authoritative before the
  post-aggregate check.
- Dynamic event lists and fixed `values` output lists intentionally have
  different compile-time knowledge. For example, dynamic-list `p95` is
  ineligible while fixed-list `p95` is rejected explicitly.
- Duplicate JSON member selection follows the pinned ClickHouse parser's
  first-member behavior.
- Do not accept a changing legal agreement or start a licensed Splunk image on
  the user's behalf merely to obtain an oracle.

## Safe resume procedure

1. Confirm `main` is clean and exactly matches `origin/main`.
2. Read the three documents listed at the top.
3. Run the ordinary gates above.
4. Confirm no stale `open-splunk-*` Docker test containers are running.
5. Pick the next bounded aggregate contract and write parser/planner/compiler
   tests before implementation; prefer `min` or `max` unless the user changes
   priority.
6. Preserve the 10,000/10,001 values boundary, 100,000/100,001 dc-only
   boundary, raw-byte sorting, binary transport, later-group and total-only
   `LIMIT` barriers, count input sharing and UInt128 accumulation, flattened
   object fallback, scope poison, calculated overwrite, and relational-depth
   regressions.
7. Keep working on `main`; commit and push each cohesive green slice.
8. Preserve unexpected local changes and never reset them away.
