# Timechart contract

`timechart` builds a typed relation whose first column is `_time`. Without a
split field, the remaining column is the aggregate output. With `BY field`, the
remaining columns are the selected split labels. The result can continue
through the supported SPL pipeline, including filtering, projection, sorting,
aggregation, and another `timechart` when a valid `_time` column remains.

This document defines Open Splunk's implemented contract. It does not claim
complete behavior parity with another SPL implementation. Syntax and default
comparison fixtures cite the official
[Splunk Enterprise 10.2 `timechart` reference](https://help.splunk.com/en/splunk-enterprise/spl-search-reference/10.2/search-commands/timechart),
but the fixtures run only against Open Splunk and are not live parity evidence.

## Defaults and option placement

The defaults are `bins=100`, `limit=10`, `cont=true`, `partial=true`,
`fixedrange=true`, `useother=true`, `usenull=true`, and epoch alignment. Time
axis controls (`span`, `bins`, `minspan`, `aligntime`, `cont`, `partial`, and
`fixedrange`) appear before the aggregate. Series controls (`limit`, `useother`,
and `usenull`) may appear before the aggregate or after the `BY` field.

For example:

```spl
index=main
| timechart span=250ms cont=false partial=false count BY service limit=0 useother=false
| where api > 0
| fields _time api*
```

`count`, `count(field)`, `sum(field)`, `avg(field)`, and the supported
percentile spellings retain their existing value and null rules. A split
timechart selects series from the complete validated input, then publishes
columns in deterministic label order.

## Time grid

Fixed spans use signed nanosecond arithmetic. Microseconds, milliseconds,
centiseconds, deciseconds, seconds, minutes, and hours are supported. The
subsecond aliases are `us`/`usec`/`microsecond`,
`ms`/`msec`/`millisecond`, `cs`/`csec`/`centisecond`, and
`ds`/`dsec`/`decisecond`; plural forms are accepted. A subsecond span must be
less than one second and divide one second exactly. Elapsed spans may exceed 24
hours.

Calendar spans accept positive magnitudes of days, weeks, months, quarters, and
years. Quarter aliases include `q`, `qtr`, and `quarter`; year aliases include
`y`, `yr`, and `year`; plural forms are accepted. Quarters normalize to three
months and years normalize to twelve months. Civil day, month, quarter, and
year grids start from 1970-01-01 in the effective search timezone. Week grids
start from the preceding Sunday, 1969-12-28. Civil boundaries remain aligned in
the search timezone, so adjacent UTC bucket widths can differ across daylight
saving transitions. A boundary in a civil-time gap resolves to the first valid
instant after the gap, and a boundary in a fold uses the earlier occurrence; an
explicit weekly alignment retains its resolved instant. Vanished dates that
resolve to a duplicate instant do not create zero-width buckets. Every later
boundary derives from the immutable civil origin, so one transition cannot
shift the remainder of the grid.

When `span` is omitted, `bins=100` chooses the first supported aligned span
that does not exceed the requested bucket count. `minspan` excludes smaller
candidates. The selected range and grid are bounded independently of series
selection and retained-result limits.

`aligntime` accepts the supported relative-time syntax and resolves once from
the immutable search-start time and effective timezone. It shifts elapsed and
week grids. Civil day, month, quarter, and year grids ignore it. The default
origin for fixed elapsed spans is the Unix epoch.

With `fixedrange=true`, grid construction uses the original half-open search
range. With `fixedrange=false`, it uses the actual post-filter input extent and
chooses an automatic span from that extent when no span is authored. The latest
observed event is included in the final bucket. Empty input produces no
buckets.

`cont=true` publishes the complete selected grid, including empty buckets.
`cont=false` publishes only buckets containing post-filter input. Aggregate
eligibility is separate: a present bucket whose measure has no eligible values
can still contain a null aggregate. `partial=false` removes only edge buckets
that are not fully covered by the original search bounds.

## Split series and limits

The default `limit=10` ranks ordinary series by the existing aggregate-specific
score and deterministic tie break. `useother=true` combines excluded ordinary
series into `OTHER`; it does not create `OTHER` when no exclusion occurred.
`usenull=true` retains the `NULL` series for missing or null split values.

An authored positive limit is a `uint64` selection request, not an executor
resource allowance. `limit=0` selects every ordinary series and therefore does
not create `OTHER`. Independent limits bound raw groups, series-domain bytes,
label bytes, dense cells, retained intermediate capacity, query memory, result
rows, and result bytes. Crossing one fails the whole search; it never silently
reduces an authored limit or publishes a partial pivot.

The browser accepts every column in that bounded server result. It does not add
a separate 64-column ceiling: line, area, and categorical charts page visible
series, and the Statistics table pages columns, so every series remains
inspectable without creating an unbounded DOM tree. Chart domains use the
complete finite series domain while geometry is limited to the visible page.
Statistics column overrides are retained in a bounded recent-query cache.
Export authenticates the complete retained runtime schema before applying the
selected-column bound, so a result wider than the export limit can still export
a permitted subset.

## Pipeline composition

A fixed-schema timechart continues as an ordinary typed relation. A dynamic
split timechart first materializes and validates its pivot once, seals the
exact runtime schema, and continues the remaining pipeline over that immutable
relation. This continuation does not discover or scan source events again.
Every stage shares the admitted search scope, snapshot, deadline, cancellation,
and cumulative resource policy. The executor detaches and validates the sealed
authority before acquiring one index-read lease, holds that lease across every
physical stage, and releases it once after success, failure, or cancellation.

Runtime series labels are literal closed-schema column names. Quoted field
references address labels containing spaces, punctuation, or dots; field
wildcards expand against the sealed series schema. Compiler-private names
remain unavailable. Removing or replacing `_time`, or reaggregating rows,
clears bucket metadata. Row filtering, ordering, and compatible projection
retain it. A later timechart replaces it with its own bucket metadata. When the
final `_time` field is absent or is not a timestamp, the result is published as
statistics instead of an empty timeline.

Validation covers the complete upstream relation before a downstream `head`,
filter, or projection can hide an invalid value. Any invalid split domain,
timestamp, aggregate transport, schema, grid descriptor, or resource charge
fails atomically with no public schema, preview, or result prefix.

## Result bounds and transport

Each surviving timechart row may carry an exact half-open bucket interval as
`time_bucket.earliest` and `time_bucket.latest`. Both values are canonical UTC
RFC 3339 strings with nanosecond precision, and earliest is strictly before
latest. The metadata is separate from SPL columns and survives retained
snapshots, paging, WebSocket and HTTP delivery, and artifact replay. Export
authenticates that retained snapshot and its full runtime schema before
applying the selected-column bound. Downloaded CSV and JSON Lines contain the
selected SPL column values; they do not add time-bucket sidecar fields.

Clients must treat these strings as exact instants. Browser chart coordinates
derive differences with integer nanosecond arithmetic instead of passing the
precision-bearing metadata through JavaScript `Date`. Line and area geometry,
domains, and point inspection use a chronological copy keyed by those exact
nanoseconds. The Statistics table applies its own selected table sort while
retaining stable membership of the server result page.
Rows with explicit null or missing metric values remain present: line and area
charts show a gap, while Statistics displays the value as unavailable instead
of inventing zero. Finite values at representable numeric extremes remain in
the chart domain, and coordinate projection bounds intermediate arithmetic
that would overflow.
Clicking the chart inspector, or pressing Enter while it has focus, pins the
current values so their text can be selected. Copy controls in pinned values
and the legend copy the exact displayed series label. Escape or the Close control
returns focus to the inspector; clicking outside dismisses the pinned values.
Legacy result rows that do not contain `time_bucket` continue to render without
an invented bucket end or drilldown range.

## Verification

The parser compatibility corpus covers aliases, calendar magnitudes, relative
alignment, sparse controls, post-`BY` series options, and `limit=0`. Compiler
and pinned ClickHouse integration tests verify one event-source read, exact
grid boundaries, complete upstream validation, staged suffix execution,
cancellation, and atomic resource failures. Manager, protocol, adapter,
artifact, and browser tests verify exact bounds through snapshots, pagination,
replay, export, and rendering.

The review ledger records implementation findings separately from verification
results:

| Review wave | Findings | Resolution and checks | Scope and assumptions |
| --- | ---: | --- | --- |
| Coordinator implementation | 17 | Resolved before independent review; focused compiler, executor, protocol, browser, and ClickHouse fixtures retain the regressions | Findings came from integration of the enhanced feature set |
| Independent review 1 and fix audits | 12 (4 P1, 6 P2, 2 P3) | All fixes are implemented in `4915ba82`; focused owner checks cover wide rendering, exact grids, staged budgets and cancellation, final result shape, observed field presence, and the editor focus race | Complete final gates run after the documentation commit |
| Independent review 2 and fix audits | 6 (1 P1, 5 P2) | All fixes are implemented in `88e2a643`; focused checks cover precise sorting and typed-axis selection, cumulative compile and runtime quotas, complete-result validation, and combined native external-table admission | Complete final gates run after the documentation commit |
| Independent review 3 and fix audits | 7 (6 P2, 1 P3) | All fixes are implemented in `1c418dbe`; focused checks cover chronological chart geometry, bounded selected-column export, transactional staged publication, incremental paging and domain reuse, recursive cancellation, and native relation scratch reuse | Complete final gates run after this documentation commit |
| Independent review 4 and fix audits | 6 (5 P2, 1 P3) | All fixes are implemented in `a1c434a7`; focused checks cover gap/fold/vanished-date grids and one physical read, null gaps, nested decoder cancellation, bounded Statistics layouts, categorical series windows, and the export metadata contract | Complete final gates run after this documentation commit |

Subsequent independent review and final gate outcomes are recorded on
[pull request #113](https://github.com/Suhaibinator/open-splunk/pull/113).

Run the deterministic compiler and publication benchmark with:

```sh
go test ./internal/clickhouse ./internal/queryexec -run '^$' \
  -bench '^(BenchmarkTimechartCompile|BenchmarkTimechartPublication)$' \
  -benchtime=100x -count=5 -benchmem
```

The compiler cases report SQL bytes and assert one `open_splunk.events` source
reference. The publication cases report retained pivot cells and include
buffering, validation, schema construction, and sink publication in the timed
region.

For the paired result, the benchmark-only files from `61fa5829` were present in
both checkouts. Each package was compiled once per revision with `go test -c`;
the resulting baseline and final binaries were then alternated, one process per
sample, with `-test.benchtime=500x -test.count=1 -test.benchmem`. This avoids
including compilation or a concurrently populated build cache in one side of
the comparison.

The following result uses Darwin arm64 on an Apple M4 Max. Each value is the
median of seven paired samples from baseline `ebcf1554` and measured candidate
`9c3b3f64043f1535f32316ef2b869fb35f6aa139`:

| Case | Baseline ns/op | Final ns/op | Baseline B/op | Final B/op | Baseline allocs/op | Final allocs/op | SQL bytes, baseline → final |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Compile, fixed count | 46,116 | 53,973 | 85,262 | 103,190 | 521 | 545 | 2,147 → 2,147 |
| Compile, automatic count | 46,114 | 48,913 | 85,247 | 92,154 | 520 | 544 | 2,147 → 2,147 |
| Compile, calendar count | 47,206 | 52,939 | 86,441 | 109,464 | 527 | 566 | 2,411 → 3,776 |
| Compile, split count | 51,893 | 65,316 | 100,569 | 138,070 | 555 | 602 | 5,989 → 8,927 |
| Compile, split average | 62,514 | 75,337 | 133,844 | 175,398 | 638 | 684 | 10,029 → 12,921 |
| Publish 100 buckets × 10 series | 50,965 | 51,618 | 229,039 | 229,268 | 629 | 630 | n/a |
| Publish 1,000 buckets × 10 series | 474,925 | 484,307 | 2,246,128 | 2,246,378 | 6,029 | 6,030 | n/a |

Every compiler sample reported one textual event-source reference at both
commits. The opt-in ClickHouse integration test separately requires exactly one
physical `ReadFromMergeTree` node. Split SQL grows because it carries the
domain, dense-cell, and retained-byte guards and validates invalid labels and
normalization collisions across the complete source, independent of the
visible grid. Across these paired samples, publication medians rise by 1.3%
and 2.0%; compiler medians rise by 6.1% to 25.9% depending on the path,
alongside the additional grid and series validation.

The later CI fix reduces repeated query-tree branches while preserving scalar
resource checks before label and bucket arrays. Knowledge-bearing count,
count(field), sum, average, and percentile fixtures now execute under the
unchanged ClickHouse 100,000-node limit; native plans retain one physical event
read. The earlier table records its stated historical candidate. A separate
compiler-only comparison below measures failed-CI head `4dc1a544` against the
completed compiler fix `f21038a0`, using Go 1.27.1 on the same Darwin arm64
Apple M4 Max, seven alternating pairs of precompiled binaries, and 500 iterations
per process. Other test workloads were paused during these samples.

| Compile case | Before ns/op | After ns/op | Before B/op | After B/op | Before allocs/op | After allocs/op | SQL bytes, before → after |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Fixed count | 51,385 | 50,103 | 103,193 | 103,180 | 545 | 545 | 2,147 → 2,147 |
| Automatic count | 45,274 | 45,319 | 92,144 | 92,154 | 544 | 544 | 2,147 → 2,147 |
| Calendar count | 48,418 | 48,211 | 109,463 | 109,454 | 566 | 566 | 3,776 → 3,776 |
| Split count | 59,236 | 60,603 | 138,068 | 139,941 | 602 | 615 | 8,927 → 8,941 |
| Split average | 70,086 | 70,866 | 175,405 | 179,295 | 684 | 714 | 12,921 → 12,832 |

All 70 samples retain one textual event-source reference. Split count and
average median compilation times rise 2.31% and 1.11%; their temporary SQL
construction adds 13 and 30 allocations, respectively. The extra allocation
is localized to window/domain SQL assembly: unsplit allocation counts stay
unchanged, and split-average SQL shrinks despite its 3,890-byte heap increase.
These small compiler costs accompany the bounded query-tree structure; they
are not native execution latency measurements. The final native plans also
remove an unused numeric count aggregate. Subsequent relation and export fixes
combine Dynamic type validation with retained-size accounting and reuse trusted
export-schema measurements. The historical publication measurements above were
not rerun for those distinct paths.

The legacy publication benchmark uses a manually constructed descriptor, so it
does not exercise the sealed timechart provenance or admitted logical row cap.
`BenchmarkTimechartSingleStageRowLimit` measures that path separately on the
measured candidate. It compiles an actual sealed split timechart before the
timer, passes it through unfenced read admission and the full executor with an
admitted policy, validates the dense native grid, applies the exact public row
cap, and publishes to an in-memory sink. The 100-bucket cases used 500
iterations per process; the 1,000-bucket cases used 100. Each value below is
the median of seven fresh-process samples:

| Sealed execution case | ns/op | B/op | allocs/op | Native rows | Published rows |
| --- | ---: | ---: | ---: | ---: | ---: |
| 100 buckets, dense | 170,743 | 192,809 | 1,248 | 100 | 100 |
| 100 buckets, `cont=false` sparse | 195,896 | 225,316 | 1,439 | 100 | 10 |
| 1,000 buckets, dense | 463,188 | 700,027 | 6,649 | 1,000 | 1,000 |
| 1,000 buckets, `cont=false` sparse | 616,110 | 783,949 | 8,640 | 1,000 | 100 |

Compile the candidate-only harness once, then run each benchmark-binary command
seven times as a fresh process:

```sh
go test -c -o /tmp/timechart-queryexec.test ./internal/queryexec
/tmp/timechart-queryexec.test -test.run='^$' \
  -test.bench='^BenchmarkTimechartSingleStageRowLimit/buckets-00100/' \
  -test.benchtime=500x -test.count=1 -test.benchmem
/tmp/timechart-queryexec.test -test.run='^$' \
  -test.bench='^BenchmarkTimechartSingleStageRowLimit/buckets-01000/' \
  -test.benchtime=100x -test.count=1 -test.benchmem
```

There is no baseline value for this harness because the baseline does not have
the sealed provenance and admitted-row path it measures. The benchmark uses a
deterministic driver fixture and excludes ClickHouse and network execution.

An independent two-stage executor fixture counted three admission acquisitions
and releases before staged lease reuse and one acquisition and release after
it. The one-lease result is required on success, suffix failure, and caller
cancellation. The same checks mutate the caller's original argument and output
slices only after outer admission begins, require execution to complete, and
verify that the first physical query receives the original sealed arguments.
The real retirement registry confirms that retirement cancels a blocked suffix
and waits for the outer lease to release. These are deterministic lifecycle
counts, not a staged-execution latency measurement.

The exact-grid harness compares the pre-allocation-fix transport at `d5fe39c6`
with the final candidate. At 10,000 buckets, transport allocation fell from
20,001 allocations and 660,244 B/op to 2 allocations and 10,400 B/op. Complete
publication fell from 120,035 allocations and 24,339,074 B/op to 100,035
allocations and 23,379,188 B/op. Run that five-sample harness with:

```sh
go test ./internal/queryexec -run '^$' \
  -bench '^BenchmarkExactTimechart(Transport|Publication)$' \
  -benchtime=100x -count=5 -benchmem
```

The final native external-table preflight benchmark scans the capacity of a
10,000-row, two-string-column lookup and a 10,000-row continuation relation in
84,051 ns/op with 24 B/op and zero allocations/op. It measures the bounded
capacity calculation, not native table construction. Run it with:

```sh
go test ./internal/clickhouse -run '^$' \
  -bench '^BenchmarkExternalTablesNativeMaterializationPreflight$' \
  -benchtime=100x -count=5 -benchmem
```

The native continuation-relation benchmark measures construction of 10,000
rows. From pre-fix candidate `53269afc` to `1c418dbe`, its median allocation
fell from 1,879,217 B/op and 10,077 allocations/op to 1,559,228 B/op and 77
allocations/op. The retained-value sizing primitive remained allocation-free:
the final scalar and 1,024-element nested cases reported zero B/op and zero
allocations/op. Run those harnesses with:

```sh
go test ./internal/clickhouse -run '^$' \
  -bench '^BenchmarkTimechartRelationNativeRows10000$' \
  -benchtime=10x -count=5 -benchmem
go test ./internal/searchjobs -run '^$' \
  -bench '^BenchmarkValueRetainedSizeBytes$' \
  -benchtime=100ms -count=5 -benchmem
```

The transition-safe calendar implementation builds 10,000 civil boundaries in
one allocation. The final calendar compile harness reports 568 allocations and
1,167,144 B/op for 10,000 sealed boundaries; the initial fix used 100,586
allocations and 2,691,772 B/op. Its SQL is 3,775 bytes and retains the compiler
benchmark's one event-source assertion. Run the focused harness with:

```sh
go test ./internal/plan ./internal/clickhouse -run '^$' \
  -bench '^(BenchmarkTimechartCivilGrid|BenchmarkTimechartCalendarCompileGrid)$' \
  -benchtime=100x -count=3 -benchmem
```

The cancellable production decoding-loop harness includes row allocation, a
shared decoder, and per-column context phases; it excludes driver scanning,
native preflight, and later atomic accounting. Across five alternating paired
samples, one scalar column rose from 46.01 to 47.62 ns/op and sixteen scalar
columns rose from 526.2 to 547.7 ns/op; bytes and allocations remained
unchanged.

The 1,024-element nested case fell from 687,999 to 526,782 ns/op, from
1,368,244 to 811,184 B/op, and from 10,242 to 7,170 allocations/op. Run it
with:

```sh
go test ./internal/queryexec -run '^$' \
  -bench '^BenchmarkNativeValueDecodingLoop$' \
  -benchtime=500ms -count=5 -benchmem
```

The retained browser seed was measured separately with Node 26.7 and a valid
3,880,588-byte response containing 5,000 rows and 64 series. Across five
alternating fresh-process pairs, a second independent adaptation had a median
47.273667 ms direct runtime and retained another 9,175,664 bytes. A shallow
copy from the shared immutable seed had a median 0.002606045 ms direct runtime
and retained 39,820.32 bytes per copy, measured over 200 retained copies. The
direct timings exclude garbage collection and browser rendering; this local
allocation contrast verifies removal of repeated full adaptation and is not a
production speedup claim.

A separate Node 26.7 server-rendering fixture alternated null and finite values
across 10,000 buckets and 64 series, with 24 series visible. Before gapped paths
were batched, the line chart emitted 120,000 polylines and 18,328,290 bytes of
markup; the area chart emitted 120,000 polygons plus 120,000 polylines and
38,081,890 bytes. With one gap-preserving path per visible line or area series,
the same fixture emits 24 paths and 1,671,334 bytes for the line chart, and 48
paths and 5,127,978 bytes for the area chart. These deterministic node and byte
counts compare `f90d788b` before with geometry commit `02e867db`, integrated as
`16e4f347`, after. The measurement predates later copy-control markup. It is
historical evidence of the rendering bound; the local server-rendering timings
are not used as browser or production latency evidence.

The publication harnesses use production buffering and publication paths with
in-memory driver fixtures. They exclude ClickHouse and network execution.
These fixed samples are repeatable regression references; they are not
production latency measurements or latency-parity claims about Splunk.
