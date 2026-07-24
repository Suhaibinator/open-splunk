# Backend checkpoint handoff

This file is the restart point for backend work. The checkpoint commit is the
`main` commit that contains this document; confirm it with `git log -1` after
pulling `origin/main`.

## Safe resume procedure

1. Run `git status --short --branch` before editing anything.
2. Preserve any unexpected local changes; this checkpoint is intended to be
   clean on `main`, so investigate a dirty worktree before editing it.
3. Run `git pull --ff-only origin main` only when doing so will not disturb
   local work.
4. Read `docs/product-architecture-plan.md` and
   `docs/spl-compatibility-v0.1.md` before expanding SPL behavior.
5. Keep changes on `main`, commit cohesive green slices, and push each
   checkpoint.

## What is complete at this checkpoint

- The collector-to-ingestion path uses durable acknowledgment/checkpoint
  coupling and has restart/ambiguous-delivery coverage.
- Search jobs expose bounded typed result pages, immutable result leases,
  cancellation/lifecycle state, progress, history, timelines, field catalogs,
  exact field summaries, and bounded CSV/JSONL export jobs.
- The executable SPL v0.1 contract and GradeThis compatibility corpus cover
  base search, field comparisons, Boolean expressions, `fields`, `table`,
  `rename`, `sort`, `head`, `tail`, `dedup`, the documented `eval`/`where`
  subset, `stats` (`count`, `sum`, `avg`, and `p95`), `top`, `rare`, and
  `timechart`.
- This checkpoint adds the first bounded extraction-mode `rex` slice:
  - `_raw` is the default source and `field=<exact field>` selects another;
  - the supported surface is the first match, equivalently `max_match=1`;
  - uniquely named `(?<name>...)` and `(?P<name>...)` captures are compiled
    through the RE2-compatible validator and assigned simultaneously;
  - no match preserves each destination's prior value, semantic type, and
    sparse presence, including explicit null and flattened object parents;
  - separately flattened dotted descendants remain resolvable after an exact
    destination update;
  - missing, null, binary, invalid-UTF-8, list, and object sources do not run
    the regex and behave as no match;
  - each stage remains streaming while a pinned ClickHouse `EXPLAIN actions=1`
    regression proves that one physical `extractGroups` action supplies all
    captures;
  - capture bytes accumulate across all `rex` stages and fail with a classified
    execution-limit error above 4 MiB per row;
  - dead private presence/type aliases are pruned at overwrite and projection
    boundaries; and
  - result classification, field catalogs, field summaries, timelines, index
    scope, and downstream SPL consume the extracted schema consistently.
- This checkpoint adds the first bounded streaming `bin`/`bucket` slice:
  - both aliases accept an explicit fixed `s`, `m`, or `h` span before or after
    the exact canonical `_time` field;
  - spans are bounded from one second up to, but not including, 24 hours and
    lower through one row-preserving ClickHouse projection with no
    aggregation/materialization fence; day-or-longer spans remain rejected
    until timezone-aware midnight alignment is represented;
  - bucket starts use nanosecond-safe mathematical floor division on the Unix
    epoch, including timestamps before 1970 and values immediately below a
    boundary;
  - `bucket` compiles identically to `bin`, and `bin ... | stats count BY
    _time` has pinned ClickHouse execution coverage;
  - a first aligned bucket below ClickHouse's `DateTime64(9)` storage minimum
    fails during planning instead of clamping or wrapping;
  - binned `_time` remains a timestamp in an Events result and in completed-job
    field analysis, while a second `bin`, `timechart`, and timeline correctly
    reject its modified canonical-time provenance; and
  - numeric fields, automatic/data-dependent bins, `AS`, custom alignment,
    calendar/subsecond/log spans, wildcards, and multiple fields remain
    explicit compatibility errors.
- The binary protobuf Search WebSocket supports bounded target journals,
  replay/resynchronization, connection and global queue limits, terminal
  delivery, application/transport ping-pong, and graceful shutdown.
- This checkpoint adds coherent bounded live result previews:
  - manager snapshots capture job/schema/rows/revision atomically;
  - transport and manager row/byte ceilings are aligned before row cloning;
  - per-subscription preview limits share one canonical target sequence;
  - opted-out subscribers receive zero-row continuity markers;
  - preview invalidation is replayable before failed/canceled/expired terminal
    state;
  - bootstrap, replay, queue, and transformation allocations are bounded;
  - projection work is concurrency-limited through publication;
  - preview demand uses a bounded histogram rather than repeated full scans;
  - the TypeScript client fences stale subscription frames, isolates reconnect
    failures, and does not retry server-rejected subscriptions forever; and
  - deprecated create-time preview options are marked deprecated in protobuf
    metadata while preview policy remains subscription-scoped.
- The browser workspace consumes those previews defensively: it validates
  schemas and typed rows, applies revisioned reset/append snapshots, clears
  discontinuous data, distinguishes provisional from authoritative results,
  disables paging/export and authoritative-only field interactions during a
  preview, and replaces previews with the completed REST snapshot.

## Validation for the checkpoint

The following backend commands passed for the `bin`/`bucket` checkpoint:

```sh
go test ./... -count=1 -timeout=3m
go test -race ./internal/splregex ./internal/spl ./internal/plan ./internal/clickhouse ./internal/queryexec -count=1 -timeout=5m
go vet ./...
go build ./...
OPEN_SPLUNK_CLICKHOUSE_INTEGRATION=1 go test ./internal/clickhouse -run '^TestStoreAgainstClickHouse$' -count=1 -timeout=6m
```

The pinned Docker test uses `clickhouse/clickhouse-server:26.3.17.4`. Its `rex`
fixture covers ordinary and Unicode extraction, simultaneous captures,
optional/empty groups, missing and explicit-null destinations, invalid input
types, zero-width matches, strict versus multiline end anchors, consecutive
stages, mixed destination types, flattened object preservation, field
catalogs/summaries, the cumulative byte guard, and an `EXPLAIN actions=1`
assertion that sees exactly one physical `extractGroups` action for a stage.
The `bin` fixture covers alias lowering, fixed timestamp result typing,
composition with `stats`, completed-job field-catalog typing, UTC epoch
alignment, and the compiled projection over a stored timestamp one nanosecond
before the Unix epoch.

If a later environment cannot run Docker, keep the unit suite green and record
the integration omission explicitly; do not silently treat a skipped opt-in
test as database validation.

## Remaining work, in priority order

1. Extend `bin`/`bucket` in a separate precision-focused slice before building
   `chart`. Numeric explicit spans need executable decisions for numeric
   strings, mixed signed/unsigned/float/decimal rows, non-finite values,
   negative mathematical floor, overflow, sparse presence, containers,
   multivalue data, and `AS` collisions. Automatic `bins`/`minspan`,
   `start`/`end`, `aligntime`, calendar/subsecond spans, and logarithmic spans
   remain separate whole-input/alignment features and must not be
   approximated.
2. Implement `chart` after numeric binning, then `spath`, in conformance-first,
   test-driven slices with pinned ClickHouse fixtures. A meaningful two-field
   `chart` is a bounded runtime-wide pivot, not a `stats` alias; reuse the
   hardened timechart series transport and the new discretization contract.
3. Extend `rex` only behind new compatibility tests. Still unsupported are
   `max_match=0`, `max_match>1`, `offset_field`, sed mode, quoted/wildcard field
   names, and PCRE-only constructs such as lookaround and backreferences.
   Optional nonparticipating captures currently become `""`, the behavior
   exposed by ClickHouse `extractGroups`; retain this documented v0.1 choice
   until it can be checked against a live Splunk differential oracle. One
   bounded performance follow-up remains: derive prior dynamic-field presence
   and type from one metadata-position alias per destination, then extend the
   `EXPLAIN` regression to pin metadata action counts. ClickHouse currently
   common-subexpression-eliminates identical probes, and the 64-output/1,024-
   stored-field limits keep this from blocking the checkpoint.
4. Expand aggregate compatibility beyond `count`, `sum`, `avg`, and `p95`:
   `dc`, `values`, `list`, `min`, `max`, `earliest`, `latest`, and the remaining
   documented percentile forms all need parser, plan, SQL, semantic, and
   ClickHouse integration tests.
5. Add a frontend component/browser test harness so reconnect rejection,
   stale-frame fencing, resynchronization, expiration, and
   preview-to-authoritative-result replacement have deterministic UI tests.
6. Add the full browser-visible end-to-end test: generated log -> collector
   durable acknowledgment -> ingestion -> ClickHouse -> SPL job -> WebSocket
   preview/progress -> authoritative paged result rendered by the UI.
7. Run and record the performance harness against the plan's sustained 1,000
   events/second target, including slow-consumer WebSockets, concurrent preview
   subscriptions, ClickHouse scan limits, and collector offline recovery.
   Profile browser preview adaptation as part of this: preview snapshots are
   strictly bounded (100 rows by default, 1,000 maximum), but each update is
   currently adapted as a complete bounded snapshot rather than incrementally.
8. Continue Phase 3/4 product hardening: per-index permissions/retention UI,
   token and collector fleet operations, RBAC/audit search, backup/restore,
   migration upgrade tests, fair query scheduling, packaging, and upgrades.

## Review notes

Independent adversarial passes covered SPL semantics/correctness,
performance/resource accounting, code reuse, and maintainability. Findings
fixed before this checkpoint include canonicalizing capture metadata before
index-scope analysis, revalidating forged field references at the compiler
boundary, accepting and diagnosing option order consistently, short-circuiting
ineligible rows, adding the query-wide byte budget, proving physical
single-extraction with ClickHouse `EXPLAIN`, pruning stale helper columns, and
preserving flattened object-parent presence and type. The `bin` review also
caught canonical-time provenance being dropped across projections, rejected
day-or-longer UTC approximation in favor of future timezone-aware alignment,
preserved the prior `timechart` logarithmic-span diagnostic, and upgraded the
pinned database test from an isolated arithmetic expression to a stored
pre-epoch event executed through the compiled projection.

No live Splunk instance was available for a differential oracle. RE2/PCRE
differences and optional-group behavior therefore remain explicit,
test-enforced compatibility decisions rather than claims of full Splunk
equivalence.
