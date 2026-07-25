# Backend checkpoint handoff

This is the canonical restart document for backend work. Read it together with:

- `docs/product-architecture-plan.md`
- `docs/spl-compatibility-v0.1.md`
- the latest `main` commit

## Pause checkpoint: bounded explicit-path `spath`

Date: 2026-07-25

Branch: `main`

Starting commit for this slice:
`527b4c012662586b98c8ed8cb550ffca323877db`
(`bound generated relational depth`)

Feature commit for this slice:
`d1096f51bf3bd6e5a3845b14657cd5774cb5f841`
(`implement bounded explicit-path spath`)

The overall backend objective remains active. Work is intentionally paused at a
green, committed, and pushed checkpoint because the user asked for a safe
break, not because the architecture plan is complete.

## What this slice completed

Open Splunk now implements a bounded first `spath` slice from SPL source
through result transport and completed-job analysis.

### SPL, AST, and plan

- `spath` accepts exactly one explicit, constant JSON path:

  ```spl
  | spath path=request.context.trace_id
  | spath input=payload output=status path=response.status
  | spath output=first_sku items{0}.sku
  ```

- `input` defaults to `_raw`; `output` defaults to the decoded path spelling.
  Options are case-insensitive, may appear in any order, and may be quoted.
- Auto-extraction, XML, wildcards, root arrays, escaped path separators,
  repeated selectors, dynamic paths, container output, multivalue output, and
  `spath()` in `eval` remain explicitly unsupported.
- Parser failures are source-located. Path diagnostics also report the byte
  offset in the decoded path, and unknown `name=value` options are rejected at
  the option rather than misread as an unlabelled path.
- The planner emits a row-preserving `ExtractJSON` operator, revalidates path
  metadata, protects the open event schema's reserved `fields` payload, and
  preserves the authorization boundary when the output is named `index`.
- An output named `_time` invalidates canonical-time provenance, so `timechart`
  and completed-job timelines fail closed.
- `rex` captures and `spath` destinations share the existing ceiling of 64
  calculated extraction outputs. The compiler independently rechecks that
  ceiling against hand-built plans.

### Path and runtime bounds

- A path has one through 17 object steps, a 4 KiB decoded-size ceiling,
  256-byte keys, at most four fixed zero-based array selectors, and a maximum
  index of `2^31-2`.
- The index ceiling is deliberately below signed `Int32` maximum because the
  pinned ClickHouse server wraps larger integer path arguments.
- One input String is limited to 1 MiB per row.
- Each stage costs three JSON evaluation work units—terminal type inspection,
  raw extraction, and typed-leaf decoding—plus one unit per selector prefix.
  A stage therefore costs at most seven units.
- All `spath` stages in a query share a cumulative ceiling of 32 JSON
  evaluation work units per row. Both planner and compiler enforce it.
- The default output is independently subject to ordinary Open Splunk field
  name limits. For example, a 256-byte key plus `{0}` needs a shorter explicit
  output because the default spelling exceeds the 256-byte field-segment
  ceiling.

### ClickHouse semantics and safety

- Every path key and translated one-based array index is a bound query
  argument; user path text is never interpolated into SQL.
- Fixed array selectors first require `JSONType(prefix) = 'Array'`. This is
  necessary because ClickHouse integer path arguments also index object
  members positionally.
- Terminal `JSONType` is checked on the original document before the selected
  raw value is decoded. This prevents ClickHouse from normalizing
  `9007199254740993.0`, `1e0`, or `1.5e1` into an apparently integral value.
- Supported leaves retain typed String, Bool, explicit null, Int64, or UInt64
  values. Fraction/exponent numbers, objects, and arrays fail through one
  stable unsupported-value marker.
- Missing members, malformed JSON, wrong containers, out-of-range indexes, and
  missing/null/non-string/ineligible inputs are row-local misses.
- A miss preserves an existing destination's exact value, sparse presence,
  and stored semantic type. Input equal to output reads the pre-command value.
- `_raw` eligibility uses the stored `raw_encoding`, not only byte validity.
  Valid UTF-8 bytes marked BINARY remain ineligible through direct `eval`
  copies, rename/projection, and miss-preserving `rex`/`spath` overlays.
- Binary-vs-String semantic type also survives those overlays into field
  catalog and summary analysis. An inconsistent stored row marked UTF8 but
  containing invalid bytes fails closed as Bytes.
- The immutable `fields` convenience object is removed after a potentially
  shadowing extraction because it cannot represent sparse calculated
  overwrites.
- The executor maps the input-limit and unsupported-value markers to stable,
  redacted public errors without payload or generated-SQL leakage.
- Calculated outputs consumed by predicates reuse the existing
  consumer-local materialized-CTE fence. Statically absent or fixed non-string
  sources do not pay for that fence.

### Value-realization boundary

ClickHouse may prune a `spath` computation whose destination is provably
unobservable after an exact projection or unconditional overwrite. Therefore
the input-size and unsupported-value errors are required for a live
destination: one that is returned, consumed downstream, analyzed, or needed as
the prior-value fallback of a later extraction. A dead destination may be
skipped without an error. This is documented and pinned; it is not compiler
level dead-code elimination.

### Analysis and UI integration

- `spath` outputs participate in `search`, `where`, `sort`, `stats`, `dedup`,
  `bin`, projections, field catalog, exact field summary, and timeline under
  their existing typed-value contracts.
- The pinned integration suite executes field catalog checks for Bool,
  explicit-null presence, and preserved binary `_raw`; it also executes a
  field summary and 24-bucket timeline.
- The query executor and search manager transport a concrete typed value from
  a Dynamic ClickHouse column as a mixed/nullable result column.
- The editor command catalog now advertises the implemented `spath`, `rex`,
  `bin`/`bucket`, and `chart` syntax instead of listing those commands as
  unsupported.

## Adversarial review record

Three independent reviewers attacked the complete diff from semantic,
ClickHouse/performance, and maintainability/test angles. Their concrete
findings were fixed before this checkpoint:

1. Large ClickHouse path indexes wrap instead of reliably missing.
2. Integer path arguments index object members positionally.
3. `JSONExtractRaw` normalizes some Double spellings before lexical
   classification, allowing silent rounding.
4. Valid UTF-8 bytes marked BINARY were initially accepted because
   `raw_encoding` was not carried into the compiler.
5. Binary provenance and semantic Bytes type could be lost through direct
   copies and miss-preserving calculated overlays.
6. Input-limit and unsupported-value guards are optimizer-prunable when the
   destination is dead; the compatibility contract now states that boundary.
7. Per-stage selector bounds were insufficient without a cumulative query
   work budget and compiler-side forged-plan defense.
8. Unknown options could be misparsed as an unlabelled path.
9. The default output can exceed ordinary field limits even when the path is
   valid.
10. Existing integration coverage did not prove exact numeric boundaries,
    sequential stages, explicit-null presence, binary provenance, or the real
    byte ceiling.

The final reviews found no remaining P0–P2 defect in this slice. One future
maintainability item remains: `rex` and `spath` duplicate conditional
extraction-overlay projection mechanics and should share a helper only after
their different type and resource semantics are captured by tests.

## Validation evidence

Ordinary focused packages passed after the final fixes:

```sh
go test ./internal/splpath ./internal/spl ./internal/plan \
  ./internal/clickhouse ./internal/queryexec -count=1 -timeout=5m
```

The pinned `clickhouse/clickhouse-server:26.3.17.4` `spath` suite passed:

```sh
OPEN_SPLUNK_CLICKHOUSE_INTEGRATION=1 \
  go test ./internal/clickhouse \
  -run '^TestSpathAgainstClickHouse$' \
  -count=1 -timeout=10m -v
```

It covers:

- String/empty String, true/false, null, zero/negative zero, signed min/max,
  the Int64-to-UInt64 boundary, UInt64 max, and zero-based array indexes;
- duplicate members, missing/malformed/wrong-container/out-of-range behavior,
  same-field reads, and exact prior-destination preservation;
- the normalization attacks `1e0`, `1.5e1`, and
  `9007199254740993.0`;
- object/array/fraction/exponent unsupported errors;
- valid-UTF-8 BINARY input through direct access, copy, rename,
  `spath`-preserve, and `rex`-preserve pipelines;
- exact 1 MiB acceptance, 1 MiB+1 rejection, and dead-destination pruning;
- unauthorized-index poison isolation;
- sequential and independent multi-stage extraction;
- downstream predicate/stats use;
- field catalog, field summary, timeline, and bounded physical query shape.

The real manager transport case passed:

```sh
OPEN_SPLUNK_CLICKHOUSE_INTEGRATION=1 \
  go test ./internal/queryexec \
  -run '^TestExecutorAndManagerAgainstClickHouse/spath_typed_output_through_manager$' \
  -count=1 -timeout=5m -v
```

The final ordinary gate passed:

```sh
go test ./... -count=1 -timeout=5m
go vet ./...
go build ./...
npm run build
git diff --check
```

The complete opt-in database packages also passed serially:

```sh
OPEN_SPLUNK_CLICKHOUSE_INTEGRATION=1 \
  go test ./internal/clickhouse -count=1 -timeout=20m -parallel=1

OPEN_SPLUNK_CLICKHOUSE_INTEGRATION=1 \
  go test ./internal/queryexec -count=1 -timeout=10m -parallel=1
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
- `fields`, `table`, `rename`, `sort`, `head`, `tail`, `dedup`;
- the documented `eval`/`where` subset;
- `stats` with `count`, `sum`, `avg`, and `p95`;
- `top`, `rare`, `timechart`, bounded two-field `chart`;
- extraction-mode `rex`, explicit-span exact `bin`/`bucket`, and the bounded
  explicit-path `spath` slice described above;
- compiler/executor limits for rows, bytes, time, memory, commands, generated
  SQL bytes, generated relational depth, extraction outputs, and JSON work;
- materialized-CTE single-scan lowering for runtime-wide and analyzer-sensitive
  paths.

## Remaining work, in priority order

### 1. Complete the analytical SPL surface

- Extend `stats` with `dc`, `values`, `list`, `min`, `max`, `earliest`,
  `latest`, and remaining percentile forms.
- Decimal `sum`/`avg`/`p95` still use the documented finite-Float64
  compatibility path; exact Decimal aggregation is separate work.
- Add later `chart` forms and aggregates while preserving bounded runtime-wide
  transport.
- Extend `rex` only behind compatibility tests: multi-match output,
  `offset_field`, sed mode, wildcard/quoted fields, and supported PCRE gaps.
- Later `spath` slices are auto-extraction and its 5,000-character discovery
  cutoff, `{}`/multivalue output, XML, containers, dynamic paths, escaped
  literal-dot keys, and the `spath()` eval function.

### 2. Improve proven performance without weakening bounds

- Consider a shared, tested conditional-overlay helper for `rex` and `spath`.
- Add a small frontend regression test that asserts the supported and
  unsupported SPL editor command catalogs stay aligned with the backend.
- Centralize the repeated pinned ClickHouse migration/store bootstrap used by
  the growing command integration suites.
- Consider multi-path JSON parsing only if the pinned ClickHouse version can
  preserve current exact type, duplicate-key, array-container, and error
  behavior.
- The exact Decimal `bin` path can avoid text round-tripping an already
  calculated `Int256`; its current implementation is correct and bounded.
- Run the complete opt-in ClickHouse and query-executor integration packages
  serially after changing JSON lowering, Dynamic metadata, or analyzer fences.

### 3. Finish first-release product proof

- Add deterministic frontend tests for reconnect rejection, stale-frame
  fencing, resynchronization, expiration, and preview-to-final replacement.
- Add the browser-visible path from generated logs through durable collection,
  ClickHouse, SPL job execution, WebSocket preview/progress, and authoritative
  paged results.
- Exercise the compiled embedded-UI server from an empty working directory.
- Record a load/performance run against the sustained 1,000 events/second
  target, including offline recovery, slow WebSocket consumers, concurrent
  searches, and scan budgets.

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
- Duplicate JSON member selection follows the pinned ClickHouse parser's first
  member behavior.
- A document containing a number outside ClickHouse's Int64/UInt64 parser
  domain may be treated as malformed even when another member is selected.
- Do not accept a changing legal agreement or start a licensed Splunk image on
  the user's behalf merely to obtain an oracle.

## Safe resume procedure

1. Confirm `main` is clean and matches `origin/main`.
2. Read the three documents listed at the top.
3. Run the ordinary validation commands above.
4. Check that no stale `open-splunk-*` Docker test containers are running.
5. Start with Remaining work item 1 unless the user changes priority.
6. Preserve the `spath` numeric-normalization, wrong-container, binary
   provenance, work-budget, dead-destination, and manager-transport tests when
   changing SQL shape.
7. Keep working on `main`; commit and push each cohesive green slice.
8. Preserve unexpected local changes and never reset them away.
