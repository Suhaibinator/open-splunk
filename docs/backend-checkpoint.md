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
  subset, `stats` (`count`, `sum`, `avg`, and `p95`), `top`, `rare`,
  `timechart`, and the two-field `chart` pivot.
- This checkpoint adds the first `chart` slice, a bounded runtime-wide
  two-field pivot:
  - the accepted surface is `chart count OVER <row> BY <col>` and
    `chart count BY <row>, <col>`, compiling to identical plans and SQL; every
    other aggregate, option, `WHERE` clause, wildcard, single-split form, and
    duplicated axis is a source-located diagnostic;
  - undocumented Splunk behavior became explicit chosen boundaries in the
    contract (tie-breaks, empty cells, row ordering, reserved `NULL`/`OTHER`
    series names) rather than guesses;
  - behavior matches Splunk's documented defaults `limit=top 10`,
    `useother=true`, and `usenull=true`; the top-ten column domain is selected
    one-dimensionally before the row-keyed aggregation so the intermediate
    GROUP BY stays bounded at rows times thirteen;
  - `chart` is terminal, publishes through the hardened timechart runtime
    series transport (`MaxSeries` 12, atomic invalid flag, schema before
    rows), and is rejected by field analysis, field summaries, and timelines;
  - the row axis reproduces `stats count BY` group semantics for fixed,
    Dynamic, binned, `_raw` (a Mixed kind tolerating binary), and
    statically-null fields; column labels are validated for length, UTF-8,
    reserved names, and VALUE-normalization collisions over all input events;
  - the pivot is bounded end to end: a 10,000-row ceiling and a 48 MiB
    executor byte guard both fail atomically with a classified execution-limit
    error before any publication, and the buffered path has cancellation,
    mid-stream error, and ceiling tests in every window;
  - compiled chart and timechart SQL now carry
    `SETTINGS enable_materialized_cte = 1` so the materialized-CTE single-scan
    bound holds on any connection, and scalar-subquery references that
    re-scanned storage were rewritten into relation references for both
    commands;
  - runtime-named count columns are published as metric columns through a new
    protobuf result shape rather than as event metadata, and CSV export
    header/cell encoding is injective for hostile data-derived column names;
  - `search` comparisons on charted fields, keyword-shaped field names, both
    spellings across 18+ upstream command families, and the 64-command/16 KiB
    budgets are pinned by hostile parser/plan/compiler/integration suites.
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
- This checkpoint extends the bounded streaming `bin`/`bucket` implementation:
  - both aliases accept one exact field, an explicit span before or after it,
    and an optional final `AS` destination;
  - a unitless positive integer span is an absolute numeric width capped at
    `2^53-1`; fixed known numeric fields include promoted numeric columns,
    numeric `eval` outputs, and numeric `stats` outputs;
  - signed and unsigned values bucket through exact widened intermediate
    arithmetic, retain their physical type, and use mathematical floor for
    negative values; unrepresentable boundaries fail the search through a
    sanitized unsupported-value marker;
  - finite `Float64` values retain double semantics, normalize negative zero,
    and reject non-finite input or output;
  - fixed strings, Booleans, and other non-numeric fixed types stay compile-time
    type errors, while a runtime-typed event field is classified per row from
    its stored semantic type and its runtime scalar type with one bounded
    metadata lookup;
  - numeric text becomes the number it spells, so the bucket stays visible to
    later numeric filters and converges with its numeric twin: integral text
    buckets through exact widened arithmetic into a signed or unsigned number,
    fractional or exponent text buckets through `Float64` inside the exactly
    representable range, and the output carries a matching semantic type;
  - a numeric spelling is canonicalized before it is classified, so leading
    zeros and an explicit `+` in the significand or the exponent never change
    the bucket and never push an ordinary integer off the exact path; this also
    keeps padded text away from ClickHouse's bounded text-to-double
    significant-digit window, which otherwise reads `000000000000000000021.5`
    as `0.5`;
  - the sanitized unsupported-value marker is guarded by the classifier's own
    row-wise coverage rather than by a constant, so a downstream `sort` key or
    `search` relational predicate that forces the runtime-typed destination to
    materialize cannot fail a search whose rows are all supported; the `rex`
    capture-limit guard is written the same way;
  - every other String keeps its exact text, including non-numeric text,
    `NaN`/`inf` spellings, overflowing exponents, invalid UTF-8, and spellings
    whose exact bucket start is unrepresentable, so one anomalous text value can
    never fail an otherwise successful search; Booleans, timestamps, durations,
    and bytes likewise keep their value, while tagged decimals, containers, and
    multivalue values remain sanitized runtime errors;
  - rows written before the current aligned field metadata existed are never
    interpreted heuristically and pass through unbinned instead of failing the
    search with a misclassified value error;
  - numeric binning works after transforming commands, including `stats count`,
    and stays row-preserving with one scoped scan and no aggregate, window, or
    materialization fence;
  - `AS` retains the source, overwrites an existing destination, updates exact
    result/analysis typing, and suppresses the immutable public `fields`
    convenience payload when it could expose a stale shadowed value; an event
    without the source keeps the destination's prior value, semantic type, and
    sparse presence;
  - unitless `_time` spans mean seconds; explicit fixed `s`, `m`, or `h` spans
    remain bounded from one second up to, but not including, 24 hours;
  - `_time AS bucket_time` retains the original canonical clock for later
    timeline/timechart work, while writing a bucket to `_time` invalidates that
    provenance;
  - timestamp buckets retain nanosecond-safe pre-epoch floor behavior and
    reject an aligned boundary below ClickHouse's `DateTime64(9)` minimum; and
  - automatic/data-dependent bins, custom alignment, calendar/subsecond/log
    spans, wildcards, and multiple fields remain explicit compatibility
    boundaries.
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

The following backend commands passed for the `chart` checkpoint (905 and 577
integration subtests respectively, zero skips):

```sh
go test ./... -count=1 -timeout=5m
go test -race ./internal/splregex ./internal/spl ./internal/plan ./internal/clickhouse ./internal/queryexec -count=1 -timeout=5m
go vet ./...
go build ./...
OPEN_SPLUNK_CLICKHOUSE_INTEGRATION=1 go test ./internal/clickhouse -count=1 -timeout=15m
OPEN_SPLUNK_CLICKHOUSE_INTEGRATION=1 go test ./internal/queryexec -count=1 -timeout=10m
```

The unrestricted `internal/clickhouse` integration run now includes three
dedicated Dynamic-bin edge suites that each start their own pinned container
and write fixtures through the store writer:
`bin_edge_numeric_integration_test.go` (numeric-text spellings compared against
their numerically stored twins, width/padding canonicalization, the exact
Int256 arm at and beyond `2^53`, pass-through of `NaN`/`inf`/overflow text),
`bin_edge_metadata_integration_test.go` (missing versus explicit-null versus
present, rex/eval-made destinations, tagged envelopes, containers, catalogs and
summaries over binned output), and `bin_edge_pipeline_integration_test.go`
(long chained pipelines, `sort`/`search`/`where` over a binned destination, the
default in-place form, fixed-versus-Dynamic convergence, and streaming-shape
assertions).

The `chart` slice adds its own pinned suites: `chart_edge_integration_test.go`
and `chart_break_pivot_integration_test.go` in `internal/clickhouse` (hostile
labels, unicode/normalization collisions, tie-breaks, `OTHER`/`NULL`
arithmetic reconciled against `stats`, Dynamic and binned axes, ceilings under
real group budgets), `chart_break_pipeline_*` across `internal/spl`,
`internal/plan`, and `internal/clickhouse` (keyword-shaped names, spelling
equivalence over 18 upstream families, budgets, diagnostics), and
`chart_break_transport_*` across `internal/queryexec`, `internal/searchjobs`,
`internal/export`, and `internal/searchws` (byte-exact ceilings, cancellation
in every buffered window, paging, export encoding, protobuf result shape).

The pinned Docker test uses `clickhouse/clickhouse-server:26.3.17.4`. Its `rex`
fixture covers ordinary and Unicode extraction, simultaneous captures,
optional/empty groups, missing and explicit-null destinations, invalid input
types, zero-width matches, strict versus multiline end anchors, consecutive
stages, mixed destination types, flattened object preservation, field
catalogs/summaries, the cumulative byte guard, and an `EXPLAIN actions=1`
assertion that sees exactly one physical `extractGroups` action for a stage.
The `bin` fixture covers alias lowering, signed negative floor, `UInt64`
maximum-value precision, `Float64`, explicit-null preservation, consecutive
bins with distinct spans, post-`stats` input, `AS`, classified `Int64`
underflow, fixed timestamp result typing, completed-job field-catalog typing,
UTC epoch alignment, and the compiled projection over a stored timestamp one
nanosecond before the Unix epoch. The executor integration verifies that the
numeric-bin runtime marker is mapped to a sanitized unsupported-value error.

If a later environment cannot run Docker, keep the unit suite green and record
the integration omission explicitly; do not silently treat a skipped opt-in
test as database validation.

## Remaining work, in priority order

1. Runtime Dynamic `bin`/`bucket` now covers exact presence/type metadata,
   numeric strings, mixed signed/unsigned/float rows, tagged decimals,
   containers, and multivalue data. What remains is an exact bucket for stored
   tagged decimals and for integer text whose bucket start falls outside
   `Int64`/`UInt64`, which currently keep their value rather than being
   approximated. Automatic `bins`/`minspan`, `start`/`end`, `aligntime`,
   calendar/subsecond spans, and logarithmic spans remain separate
   whole-input/alignment features and must not be approximated.
2. Implement `spath` in conformance-first, test-driven slices with pinned
   ClickHouse fixtures. The two-field `chart` pivot is complete; later chart
   slices (further aggregates, `limit`/`useother`/`usenull` options, the
   single-split form, `span` discretization of numeric column axes) should
   follow the same contract-first pattern and reuse the chart transport. The
   frontend does not yet render chart results (only `timechart` maps to a
   visualization mode); wire that when frontend work resumes.
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
ClickHouse arithmetic and query shape, performance/resource accounting, code
reuse, and maintainability. The numeric-bin review required a discriminated
numeric/time span AST, compiler-boundary revalidation, a `2^53-1` width bound,
exact wide-integer arithmetic, finite floating guards, sanitized runtime
classification, canonical-time output provenance, stale-payload suppression,
and tests over transforming rows and the unsigned 64-bit boundary.
Final review also restored the precise missing-`=` parser diagnostic, made
per-stage ClickHouse aliases collision-proof, narrowed integer intermediates
without losing exactness, removed redundant runtime casts, and corrected the
documentation not to promise atomic publication on a late streaming error.
Later adversarial passes for Dynamic binning and chart are reflected in the
completion and validation sections above rather than duplicated here.

The official Splunk 10.4 container image was downloaded for a possible
differential oracle, but startup additionally required accepting Splunk's
current General Terms. No changing legal agreement was accepted on the user's
behalf; the failed ephemeral container and image were removed. Public
documentation describes numerical binning but does not settle coercion of
numeric-looking text, result typing, failed conversion, decimals, or
multivalue fields. Open Splunk's implemented choices and executable coverage
are explicit in the [`bin` / `bucket`
contract](spl-compatibility-v0.1.md#bin--bucket), while the unfinished exact
decimal and wide-integer cases remain in Remaining work item 1. These choices
still need live Splunk differential validation rather than being presented as
hidden compatibility claims.
