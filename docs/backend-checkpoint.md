# Backend checkpoint handoff

This is the canonical restart document for backend work. Read it together
with:

- `docs/product-architecture-plan.md`
- `docs/spl-compatibility-v0.1.md`
- the latest `main` commit

## Pause checkpoint: typed `stats min` and `stats max`

Date: 2026-07-25

Branch: `main`

Starting checkpoint:
`a3dcb61` (`record stats count field checkpoint`)

Feature commit:
`b4c795b` (`implement typed stats min and max`)

Adversarial-hardening commit:
`0a995d7` (`harden typed extrema pipelines`)

This document is committed immediately after the hardening commit. Use the
current `main` HEAD as the document commit rather than copying an older hash
from this file.

The earlier branch repair is complete: the work is on `main`, the feature and
hardening commits were pushed directly to `origin/main`, and subsequent work
must continue on `main`.

The overall backend objective remains active. Work is intentionally paused at
a green, committed, and pushed checkpoint because the user requested a safe
break. The product architecture plan is not complete.

## What this slice completed

Open Splunk now implements typed `stats min(field)` and `stats max(field)` from
SPL parsing through ClickHouse execution, downstream calculated-field use,
search-job transport, documentation, and editor support.

### SPL, AST, and plan contract

- `min` and `max` are case-insensitive and accept exactly one unquoted, exact
  field.
- The default names are canonical `min(field)` and `max(field)`; use `AS` when
  the current downstream field grammar must reference the result.
- Empty calls, expressions, quoted fields, wildcards, multiple arguments, and
  forged aggregate metadata fail explicitly rather than being approximated.
- Separate AST and plan enum values, canonical-name mapping, required-input
  validation, reserved open-schema `fields` handling, output uniqueness, and
  relational-depth checks are covered by tests.
- A closed schema may legitimately contain a calculated field named `fields`;
  the planner accepts extrema over that field.
- The search editor and compatibility contract advertise the supported forms.

### Runtime ordering and typed results

- Missing, explicit null, an empty multivalue, and null multivalue members do
  not participate.
- Every immediate scalar member of a top-level multivalue participates without
  expanding event rows. Nested arrays and objects, generic objects, and
  flattened object parents fail atomically with the existing sanitized stats
  unsupported-value marker.
- Runtime String/Dynamic candidates use a deterministic two-class order:
  finite numeric candidates sort before lexical candidates; numeric candidates
  compare numerically and lexical candidates compare by raw bytes. Therefore
  `min` selects a numeric candidate whenever one exists, while `max` selects a
  lexical candidate whenever one exists.
- Numeric text must be valid UTF-8, at most 4 KiB, match the complete decimal
  grammar, and convert to finite `Float64`. Whitespace, `NaN`, infinity,
  overflowing exponents, invalid UTF-8, and longer values remain lexical.
- Numeric zero is normalized to positive zero. Equivalent spellings such as
  `01`, `1.0`, and `1e0` publish the same `Double(1)` result.
- Runtime winners are nullable `Mixed`: numeric winners are `Double`, lexical
  winners retain their String/Bytes representation, and no winner is null.
- Statically typed numeric, Boolean, and timestamp scalars use native
  `minIfOrNull` / `maxIfOrNull`, preserving physical type and timestamp
  nanosecond precision rather than converting through `Float64`.
- Global aggregation over no rows emits one null result row. Retained groups
  with no eligible value contain null; grouped aggregation over no rows emits
  no groups. Projected-away inputs remain absent.

Punctuation placement is deliberately documented as an Open Splunk v0.1
boundary. Public Splunk documentation warns that symbol ordering is not
standard. The pinned fixture now includes numeric text with `!` and `~` so the
chosen numeric-before-lexical/raw-byte behavior cannot drift silently.

### Efficient ClickHouse lowering

- Repeated extrema over the same runtime input share one canonical
  `Array(String)` input alias and one candidate alias.
- `min` and `max` use separate constant-size aggregate states; repeated
  identical measures are physically shared by ClickHouse.
- `min`/`max` and `values` over the same input share the canonical String
  materialization.
- The lowering performs no `ARRAY JOIN`, row expansion, sorting, or unbounded
  list aggregation.
- The numeric text grammar is shared with `bin`, preventing command semantics
  from drifting.
- Runtime result type metadata is private and survives `rename`, `eval`,
  downstream `bin`, and re-aggregation.
- A transforming aggregate no longer causes downstream `bin` to reference the
  event-only `__os_field_metadata_version` column. Calculated semantic type
  metadata is authoritative after event rows have been transformed.
- Generated placeholder count/order and the existing generated-SQL and
  relational-depth limits remain exact.

## Adversarial review record

Three independent post-implementation agents reviewed correctness and SPL
semantics, ClickHouse execution/performance, and reuse/maintainability. Their
concrete findings were handled as follows:

1. **Stop-ship downstream failure fixed.** `stats min(metric) AS low | bin low`
   originally referenced the event-only metadata-version column after `stats`
   had dropped it, causing ClickHouse Code 47. The compiler now distinguishes
   event metadata from authoritative calculated metadata. A unit assertion and
   executed pinned-ClickHouse regression cover this path.
2. **Shared logic consolidated.** Numeric-bin and extrema decimal grammar now
   use one constant. Extrema, `dc`, and `values` share one String-input
   resolution/cache helper.
3. **Documentation corrected.** “No row materialization” was too broad; the
   true invariant is no row expansion. Symbol ordering is now labeled as an
   explicit compatibility boundary.
4. **Coverage strengthened.** Native Boolean extrema execute for both values.
   Native timestamp extrema now compare distinct timestamps one nanosecond
   apart, proving ordering and precision. Punctuation, closed-schema `fields`,
   and min-plus-values input sharing gained regressions.
5. **Test-only parser mistake corrected.** An attempted Bool integration probe
   used an unsupported comparison inside `eval`; it was replaced with the
   supported Bool-literal path.

The final reviewer verdict reported no remaining stop-ship correctness,
security, argument-ordering, relational-depth, optimizer-sharing, or
unbounded-state issue.

One nonblocking performance opportunity remains: statically scalar String
fields such as `host`, `source`, and `service` currently use the same bounded
array/candidate lowering as multivalue-capable inputs. A dedicated scalar
candidate alias with conditional scalar aggregates could remove several
row-local singleton arrays and Dynamic boxes. Preserve numeric-before-lexical
semantics and prove the physical plan before changing this path.

## Validation evidence

The full ordinary backend suite passed after the hardening changes:

```sh
go test ./... -count=1 -timeout=5m
go vet ./...
go build ./...
```

The frontend/static-application gates passed:

```sh
npm run lint
npm run typecheck
npm run test:frontend
npm run build
```

The full Store integration suite passed against the production-pinned image:

```sh
OPEN_SPLUNK_CLICKHOUSE_INTEGRATION=1 \
OPEN_SPLUNK_CLICKHOUSE_TEST_IMAGE=clickhouse/clickhouse-server:26.3.17.4 \
go test ./internal/clickhouse \
  -run '^TestStoreAgainstClickHouse$' -count=1
```

The extrema coverage in that suite includes:

- numeric-only, lexical-only, mixed, punctuation, zero, null, missing, and
  multivalue groups;
- whitespace, `NaN`, overflowing exponents, empty String, and UTF-8 lexical
  fallback behavior;
- nested/container and flattened-object guards;
- incomplete `BY` eligibility, projected-away input, global/grouped empties,
  and tenant-scope poison isolation;
- downstream `bin`, re-aggregation, native Bool, and distinct nanosecond
  timestamps; and
- `EXPLAIN actions=1` proof of shared aggregate states with no `ArrayJoin`.

The full query executor and manager suite also passed against the pinned
ClickHouse image:

```sh
OPEN_SPLUNK_CLICKHOUSE_INTEGRATION=1 \
OPEN_SPLUNK_CLICKHOUSE_TEST_IMAGE=clickhouse/clickhouse-server:26.3.17.4 \
go test ./internal/queryexec \
  -run '^TestExecutorAndManagerAgainstClickHouse$' -count=1
```

This pins parser-to-manager nullable `Mixed` transport, including numeric,
String, null, and binary `_raw` extrema results.

The focused post-review compiler tests and `git diff --check` also passed. Do
not count a skipped opt-in test as database validation.

## Current backend state

The backend now includes:

- durable collector queue/checkpoint and ingestion acknowledgement coupling;
- scoped ingestion tokens, canonical typed events, and ClickHouse storage;
- bounded search jobs with cancellation, history, progress, typed paging,
  result leases, timelines, field catalog/summary, and CSV/JSONL export;
- binary protobuf WebSocket preview/progress with replay and
  resynchronization;
- the documented SPL v0.1 base search and Boolean/comparison expressions;
- `fields`, `table`, `rename`, `sort`, `head`, `tail`, and `dedup`;
- the documented `eval`/`where` subset;
- `stats` with row `count`, exact `count(field)`, `dc`/`distinct_count`,
  `values`, typed `min`/`max`, `sum`, `avg`, and `p95`;
- `top`, `rare`, `timechart`, and bounded two-field `chart`;
- extraction-mode `rex`, explicit-span exact `bin`/`bucket`, and bounded
  explicit-path JSON `spath`;
- resource limits for rows, bytes, time, memory, commands, generated SQL,
  relational depth, extraction work, exact distinct state, and multivalue
  publication; and
- materialized-CTE single-scan lowering for runtime-wide and
  analyzer-sensitive paths.

## Remaining work, in priority order

### 1. Continue TDD on aggregate correctness and efficiency

Start with the deferred scalar-String extrema optimization because it is a
concrete adversarial performance finding and the user emphasized efficiency:

- write compiler and pinned `EXPLAIN actions=1` tests first;
- introduce one scalar candidate/classification alias shared by repeated
  `min`/`max`;
- avoid singleton `Array(String)` and candidate-array construction only for
  fields proven statically scalar;
- retain the existing Array path for Dynamic and `StringArray` inputs;
- preserve numeric-before-lexical selection, null behavior, private result
  type, placeholder order, downstream use, and sanitized guards; and
- benchmark both forms over a generated corpus before claiming improvement.

After that, choose one bounded aggregate contract at a time:

- `list(field)` must be order-sensitive, duplicate-preserving, and explicitly
  bounded; do not adapt unordered `values`;
- `earliest(field)` / `latest(field)` need explicit event-order, tie-break,
  null, multivalue, type, and precision contracts;
- broader `count` forms (`c`, wildcards, predicates, and `count(eval(...))`)
  need separate syntax and differential tests;
- remaining percentile forms need explicit approximation/error and resource
  contracts;
- `eventstats` should follow only after the aggregate library is stable;
  `streamstats` remains outside the first release unless scope changes; and
- exact Decimal comparison/aggregation remains separate work from the current
  finite-`Float64` runtime compatibility path.

Extend `chart`, `rex`, or `spath` only behind compatibility and pinned
ClickHouse tests. Deferred `spath` surface includes auto-extraction, `{}`
multivalue output, XML, terminal containers, escaped literal-dot keys, and the
`spath()` eval function.

### 2. Finish the first-release product proof

- Add one browser-visible end-to-end test that starts the stack, writes a
  generated log, waits for durable collector acknowledgment, searches it,
  observes protobuf WebSocket progress/preview, and verifies authoritative
  paged results.
- Exercise the compiled embedded-UI server from an empty working directory,
  without Node.js or external frontend assets.
- Add deterministic frontend/server tests for reconnect rejection,
  stale-frame fencing, resynchronization, expiration, and preview-to-final
  replacement.
- Record a load/performance run at sustained 1,000 events/second, including
  collector offline recovery, slow WebSocket consumers, concurrent searches,
  high-cardinality exact aggregates, and scan budgets.
- Use the existing log generator and sanitized fixtures; `app.log` is suitable
  local test input when its contents pass the fixture secret scan.

### 3. Improve compiler maintainability

- Move pending aggregate publication/validation descriptors out of persistent
  `compileState` into a typed aggregate-compilation result.
- Replace string-prefix presence rebinding with an explicit field-presence
  policy shared by projection, rename, eval, extraction, and aggregate output.
- Consolidate exact-string/candidate descriptors without weakening dc-only
  scalar lowering, values/dc sharing, extrema type propagation, or two-stage
  validation depth.
- Split the large pinned ClickHouse fixture/bootstrap and aggregate assertions
  into smaller helpers while retaining one reusable ephemeral container.
- Keep regressions for physical aggregate sharing, later hidden overflow,
  binary multivalue transport, tenant/index/time scope poison, calculated
  overwrite, and relational depth.

### 4. Continue Phase 3/4 product hardening

- Per-index retention and permissions, index/app administration, collector
  fleet operations, reports/dashboards, HEC compatibility, RBAC, and audit
  search.
- Migration upgrades, backup/restore and disaster recovery, load shedding,
  fair scheduling, per-user concurrency, alerts/scheduled searches, packaging,
  installers, upgrades, and signed releases.
- Keep the first release single-node; distributed control/search remains
  outside the current plan.

The architecture plan still requires product decisions for capacity-planning
retention/event size, target hardware, concurrent search load, immediate
Windows collector support, and whether dashboards are first-release scope.
Do not guess those decisions if they materially affect the implementation.

## Known compatibility boundaries

- A live licensed Splunk differential oracle is unavailable. Public
  documentation leaves several multivalue, null, binary, symbol-ordering, and
  error behaviors unspecified; keep Open Splunk choices explicit.
- Runtime String/Dynamic extrema compare numeric candidates through `Float64`.
  Distinct very-wide integers or exact decimals can collapse to the same key.
  Do not silently claim exactness until an exact decimal comparison key and
  live oracle exist.
- Numeric candidates sort before all lexical candidates in Open Splunk v0.1;
  punctuation within the lexical class uses raw-byte order. Symbol placement
  is not claimed as verified Splunk parity.
- Public Splunk documentation establishes ordinary occurrence semantics for
  `count(field)` but does not pin null multivalue members or typed containers.
  Open Splunk counts immediate non-null containers atomically.
- Collector decoding does not preserve every original numeric token spelling.
  String-oriented aggregates operate on stored canonical values.
- Default aggregate names containing parentheses cannot be referenced by the
  downstream field grammar; use `AS`.
- The 512 KiB `values` bound is a publication bound, not an aggregate-state
  memory guarantee. ClickHouse query memory remains authoritative before the
  post-aggregate check.
- Duplicate JSON member selection follows the pinned ClickHouse parser's
  first-member behavior.
- Do not accept a changing legal agreement or start a licensed Splunk image on
  the user's behalf merely to obtain an oracle.

## Safe resume procedure

1. Confirm `main` is clean and exactly matches `origin/main`.
2. Read the three documents listed at the top and inspect commits `b4c795b`
   and `0a995d7`.
3. Confirm no stale `open-splunk-*` Docker test containers are running.
4. Run the ordinary Go/frontend gates above. Run both opt-in pinned ClickHouse
   suites before changing extrema/bin metadata behavior.
5. Begin with tests for the statically scalar String extrema optimization
   unless the user changes priority. Preserve the Dynamic/StringArray path.
6. Preserve numeric grammar sharing, punctuation/UTF-8/zero boundaries,
   native timestamp precision, private calculated types, downstream `bin`,
   re-aggregation, scope poison, binary transport, physical state sharing, and
   relational-depth regressions.
7. Keep working on `main`; commit and push every cohesive green slice.
8. Preserve unexpected local changes and never reset them away.
