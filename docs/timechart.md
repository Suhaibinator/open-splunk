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
saving transitions.

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
a separate 64-column ceiling: line and area charts page visible series, and the
Statistics table pages columns, so every series remains inspectable without
creating an unbounded DOM tree.

## Pipeline composition

A fixed-schema timechart continues as an ordinary typed relation. A dynamic
split timechart first materializes and validates its pivot once, seals the
exact runtime schema, and continues the remaining pipeline over that immutable
relation. This continuation does not discover or scan source events again.
Every stage shares the admitted search scope, snapshot, deadline, cancellation,
and cumulative resource policy.

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
snapshots, paging, WebSocket and HTTP delivery, artifact replay, and export.

Clients must treat these strings as exact instants. Browser chart coordinates
derive differences with integer nanosecond arithmetic instead of passing the
precision-bearing metadata through JavaScript `Date`. Legacy result rows that
do not contain `time_bucket` continue to render without an invented bucket end
or drilldown range.

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
| Independent review 2 | Pending | Correctness, efficiency, and code-quality reviewers are inspecting `4915ba82` | No clean-review claim is made before their reports arrive |

Run the deterministic paired compiler and publication benchmark with:

```sh
go test ./internal/clickhouse ./internal/queryexec -run '^$' \
  -bench '^(BenchmarkTimechartCompile|BenchmarkTimechartPublication)$' \
  -benchtime=100x -count=5 -benchmem
```

The compiler cases report SQL bytes and assert one `open_splunk.events` source
reference. The publication cases report retained pivot cells and include
buffering, validation, schema construction, and sink publication in the timed
region.

The following paired result uses Darwin arm64 on an Apple M4 Max. Each value is
the median of five fixed 100-iteration samples from baseline `ebcf1554` and the
final implementation candidate `4915ba82`:

| Case | Baseline ns/op | Final ns/op | Baseline B/op | Final B/op | Baseline allocs/op | Final allocs/op | SQL bytes, baseline → final |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Compile, fixed count | 48,415 | 62,432 | 85,273 | 102,602 | 521 | 541 | 2,147 → 2,147 |
| Compile, automatic count | 47,463 | 45,680 | 85,265 | 91,588 | 520 | 540 | 2,147 → 2,147 |
| Compile, calendar count | 48,894 | 42,151 | 86,460 | 90,804 | 527 | 547 | 2,411 → 2,411 |
| Compile, split count | 52,110 | 57,058 | 100,569 | 137,474 | 555 | 597 | 5,989 → 8,879 |
| Compile, split average | 64,546 | 67,943 | 133,850 | 174,838 | 638 | 680 | 10,029 → 12,921 |
| Publish 100 buckets × 10 series | 57,997 | 56,052 | 229,018 | 229,195 | 629 | 630 | n/a |
| Publish 1,000 buckets × 10 series | 470,752 | 445,752 | 2,246,120 | 2,246,293 | 6,029 | 6,030 | n/a |

Every compiler sample reported one textual event-source reference at both
commits. The opt-in ClickHouse integration test separately requires exactly one
physical `ReadFromMergeTree` node. Split SQL grows because it carries the
domain, dense-cell, and retained-byte guards.

The exact-grid harness compares the pre-allocation-fix transport at `d5fe39c6`
with the final candidate. At 10,000 buckets, transport allocation fell from
20,001 allocations and 660,244 B/op to 2 allocations and 10,400 B/op. Complete
publication fell from 120,035 allocations and 24,339,074 B/op to 100,035
allocations and 23,379,125 B/op. Run that five-sample harness with:

```sh
go test ./internal/queryexec -run '^$' \
  -bench '^BenchmarkExactTimechart(Transport|Publication)$' \
  -benchtime=100x -count=5 -benchmem
```

Both publication harnesses use the production buffering and publication paths
with in-memory driver fixtures. They exclude ClickHouse and network execution.
These fixed samples are repeatable regression references; they are not
production latency measurements or latency-parity claims about Splunk.
