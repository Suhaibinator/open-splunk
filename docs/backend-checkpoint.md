# Backend checkpoint handoff

This is the canonical restart document for backend work. Read it together with:

- `docs/product-architecture-plan.md`
- `docs/spl-compatibility-v0.1.md`
- the latest `main` commit

## Pause checkpoint: bounded exact `stats values`

Date: 2026-07-25

Branch: `main`

Starting commit for this slice:
`d736396e780dc065769bdb9ae85392b2df19d0b5`
(`record distinct count backend checkpoint`)

Feature commit for this slice:
`6d6d5d145a7a7b58a03793b4e21edf1d41fe0c69`
(`implement bounded stats values`)

The overall backend objective remains active. Work is intentionally paused at
a green, committed, and pushed checkpoint because the user requested a safe
break. The product architecture plan is not complete.

## What this slice completed

Open Splunk now implements exact, bounded `stats values(field)` from SPL source
through ClickHouse execution, search-job paging, and typed result transport.

### SPL, AST, plan, and editor contract

- `values(field)` is case-insensitive and requires exactly one unquoted exact
  field.
- Its default output is the canonical lowercase spelling `values(field)`;
  explicit `AS` aliases are supported and recommended for downstream use.
- Parser, planner, and compiler revalidate measure count, exact fields,
  reserved open-schema `fields`, output uniqueness, forged function metadata,
  and relational-depth limits.
- The search editor advertises `values` in the supported `stats` surface.
- The public behavior and conservative multivalue boundaries are recorded in
  `docs/spl-compatibility-v0.1.md`.

### Exact value semantics

- `values` reuses the exact canonical scalar spelling used by `dc`:
  - missing and explicit null contribute nothing;
  - empty String is retained;
  - a top-level multivalue input is flattened one level;
  - null members are ignored and duplicates collapse;
  - integer `1` and String `"1"` converge;
  - Strings `"01"` and `"1.0"` remain distinct;
  - Bool and equivalent String spellings converge; and
  - Bytes, timestamps, durations, and decimals use deterministic stored
    lexical payloads.
- Generic objects, empty objects, nested arrays, and nested container members
  fail the scoped search atomically with a sanitized unsupported-value error.
- Results are sorted by raw String bytes. There is no numeric, locale,
  case-insensitive, or Unicode-normalized ordering.
- Global aggregation over no rows and retained groups with no eligible values
  publish a non-null typed `[]`. Grouped aggregation over no rows publishes no
  rows.
- A physical empty list is logically absent for downstream `field=*`.
- Invalid-UTF-8 fixed String values cross the typed boundary as Bytes children.
  Text equality and wildcard matching explicitly exclude those members.

### Shared lowering and resource bounds

- `values(x)` and `dc(x)` in the same aggregate share one normalized input and
  one exact ClickHouse set.
- Repeated `values(x)` aliases share one aggregate state and one lexical sort.
- A values-bearing set uses `groupUniqArrayArray(10001)`. A dc-only input keeps
  the larger `100001` sentinel and publishes only a scalar cardinality through
  the validation window, avoiding a post-aggregate array regression.
- No `ARRAY JOIN` is used, so sibling counts and numeric aggregates cannot be
  multiplied by multivalue expansion.
- Publication limits are exact and fail rather than truncate:
  - 10,000 elements and 512 KiB raw lexical payload per `values` cell;
  - the same combined element/byte ceilings per public row across all values
    aliases, counting duplicate aliases independently; and
  - 100,000 elements and 8 MiB across the complete pre-downstream result.
- UInt128 streaming folds and scalar additions account bytes/elements without
  temporary per-element accounting arrays.
- A whole-result window barrier validates every group before later
  projection, filter, sort, `head`, or SQL `LIMIT`. A valid first row cannot
  hide a later per-cell or total-only overflow.
- ClickHouse query memory remains an independent bound on exact aggregate
  state before the post-aggregate publication checks run.
- Stable element and byte markers map to redacted search-job resource-limit
  failures.

### Typed downstream behavior

- ClickHouse `Array(String)` becomes `ValueKindList` with
  `Column.Multivalue=true`; valid text members are String children and invalid
  UTF-8 members are Bytes children.
- `fields`, `table`, `rename`, `head`, `tail`, and direct `eval` field copies
  preserve the fixed multivalue type and empty-list presence rule.
- A later `dc` or `values` flattens the list. `sum` and `avg` parse and flatten
  finite numeric members.
- Base-search equality and wildcard use any-valid-text-member matching.
  Inequality requires a nonempty list with no equal member; `field=*` requires
  a nonempty list.
- Known fixed multivalue use is rejected explicitly for ordered search,
  `where`, scalar `eval` functions, `sort`, `dedup`, `stats ... BY`, `p95`,
  `rex`, `spath`, `bin`, `top`, `rare`, and chart axes. The compiler never
  silently applies ClickHouse array stringification.

## Adversarial review record

Independent agents reviewed SPL semantics, ClickHouse correctness, optimizer
behavior, bounds, transport, test coverage, code reuse, maintainability, and
efficiency. Concrete findings fixed before the checkpoint included:

1. Direct `eval copied=values_result` initially treated physical `[]` as
   logically present.
2. Invalid-UTF-8 members reached Unicode and regex functions without an
   explicit validity guard.
3. The first transport probe bypassed parser, compiler, storage, and the job
   manager instead of proving real `stats values(_raw)` paging.
4. Whole-result total-only overflows needed regressions behind an outer
   `ORDER BY ... LIMIT 1`.
5. Downstream rejection diagnostics only asserted a nonzero range; they now
   pin the exact source text, including bin and both chart axes.
6. Forged `values` plans and reserved/wildcard inputs needed parity with the
   existing `dc` defenses.
7. Generalizing the exact-set barrier caused dc-only queries to carry up to
   100,001 strings past aggregation. The scalar-cardinality path was restored,
   while mixed values/dc queries retain one shared two-stage barrier.
8. Byte/row accounting allocated temporary UInt128 arrays; it now uses a
   streaming `arrayFold` and scalar totals.
9. Repeated code-395 resource-marker classification was replaced by one
   ordered marker table.
10. A frontend aggregate-name heuristic did not need to include `values`
    because the real result is already typed as a list; that unnecessary
    widening was removed.

The final reviewers reported no remaining correctness blocker. Deferred
maintainability ideas, not required for correctness, include moving transient
post-aggregate work out of `compileState`, introducing an explicit presence
policy instead of recognizing presence SQL text, and splitting the large
ClickHouse aggregate integration helper into smaller reusable fixtures.

## Validation evidence

Focused packages passed:

```sh
go test ./internal/spl ./internal/plan ./internal/clickhouse \
  ./internal/queryexec -count=1
```

The pinned `clickhouse/clickhouse-server:26.3.17.4` store suite passed:

```sh
OPEN_SPLUNK_CLICKHOUSE_INTEGRATION=1 \
OPEN_SPLUNK_CLICKHOUSE_TEST_IMAGE=clickhouse/clickhouse-server:26.3.17.4 \
go test ./internal/clickhouse \
  -run '^TestStoreAgainstClickHouse$' -count=1
```

Its values coverage includes:

- canonical scalar convergence, empty String, missing/null, top-level list
  flattening, duplicate collapse, raw-byte ordering, case, and Unicode;
- global/grouped empties and projected-away inputs;
- scoped object/nested-container poison and calculated overwrite behavior;
- exact element and byte boundaries;
- duplicate-alias row totals and complete-result element/byte totals;
- per-cell and total-only overflow hidden behind downstream `LIMIT`;
- repeated `dc`/`values`, numeric-member aggregation, and any-member search;
- shared physical aggregate state under `EXPLAIN actions=1`; and
- preservation of the larger, scalar dc-only path.

The full query executor and manager suite passed:

```sh
OPEN_SPLUNK_CLICKHOUSE_INTEGRATION=1 \
OPEN_SPLUNK_CLICKHOUSE_TEST_IMAGE=clickhouse/clickhouse-server:26.3.17.4 \
go test ./internal/queryexec \
  -run '^TestExecutorAndManagerAgainstClickHouse$' -count=1
```

This includes a real invalid-UTF-8 event stored in a separate index and queried
through `stats values(_raw)`, the search-job manager, and paged results. It
pins Bytes-child transport plus equality, wildcard, inequality, and presence
behavior.

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
- bounded search jobs with cancellation, history, progress, typed paging,
  result leases, timelines, field catalog/summary, and CSV/JSONL export;
- binary protobuf WebSocket preview/progress with replay and resynchronization;
- the documented SPL v0.1 base search and Boolean/comparison expressions;
- `fields`, `table`, `rename`, `sort`, `head`, `tail`, and `dedup`;
- the documented `eval`/`where` subset;
- `stats` with `count`, `dc`/`distinct_count`, `values`, `sum`, `avg`, and
  `p95`;
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
  lowest-risk useful candidates are `count(field)`, `min`, and `max`.
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
   tests before implementation; prefer `count(field)`, `min`, or `max` unless
   the user changes priority.
6. Preserve the 10,000/10,001 values boundary, 100,000/100,001 dc-only
   boundary, raw-byte sorting, binary transport, later-group and total-only
   `LIMIT` barriers, physical state sharing, scope poison, calculated
   overwrite, and relational-depth regressions.
7. Keep working on `main`; commit and push each cohesive green slice.
8. Preserve unexpected local changes and never reset them away.
