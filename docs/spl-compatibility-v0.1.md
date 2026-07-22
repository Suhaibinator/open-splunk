# Open Splunk SPL compatibility contract v0.1

**Status:** executable implementation contract
**Compatibility version:** `0.1`
**Last updated:** July 21, 2026

Open Splunk accepts only the syntax and behavior described here. Unsupported
commands or forms fail with a source-located diagnostic; the compiler never
executes a supported prefix as a partial search.

All searches are additionally constrained by the server-resolved tenant,
authorized index set, half-open event-time range, index-time cutoff, and
storage visibility snapshot. SPL cannot widen those boundaries.

To bound parser, compiler, and ClickHouse AST work, one search may contain at
most 16 KiB of UTF-8 source, 1,024 syntax tokens, and 64 pipeline commands.
Scalar expressions may nest 32 levels, with at most 32 `where` comparisons.
`eval` accepts at most 64 assignments; `stats` accepts at most 16 measures and
16 `BY` fields. Exceeding a limit returns the source-located
`SPL_QUERY_TOO_COMPLEX` diagnostic before planning or execution. Dynamic field
paths align with ingestion's ceiling: 17 dotted segments and 256 unescaped
UTF-8 bytes per segment. Generated ClickHouse SQL is additionally capped at
256 KiB; exceeding that internal expansion budget returns the same diagnostic.
The executor applies a 1 MiB ClickHouse `max_query_size` ceiling after bound
arguments are expanded, in addition to its time, memory, scan, group, and result
budgets.

## Search expressions

Base search and pipeline `search` support:

- bare terms and quoted phrases over `_raw`;
- `AND`, `OR`, `NOT`, parentheses, and implicit `AND`;
- Splunk search-command precedence: parentheses, `NOT`, `OR`, then `AND`;
- `=`, `!=`, `<`, `<=`, `>`, and `>=` field comparisons;
- string, signed/unsigned integer, floating-point, boolean, and null literals;
- `*` wildcards in term and comparison values; and
- canonical fields plus deterministic dotted dynamic-field paths.

Equality comparisons preserve literal type intent. Ordered comparisons against
a numeric literal also accept numeric-looking dynamic strings; failed numeric
conversion does not match. `field!=value` excludes a missing field, while `NOT
field=value` includes it. `field=*` requires a present, non-null value. Canonical
`index` comparisons are case-sensitive; ordinary string comparisons are
case-insensitive. Extended decimal values compare through finite `Float64`, so
distinct values beyond `Float64` precision can compare as equal in compatibility
version 0.1.

### `where`

```spl
| where p95_ms > 500
| where status=500 OR duration_ms>500 AND level="ERROR"
```

`where` uses a separate eval-expression grammar: quoted strings are literals,
bare names are fields, comparisons are case-sensitive, implicit `AND` is not
accepted, and precedence is parentheses, `NOT`, `AND`, then `OR`. The current
slice supports Boolean combinations of scalar comparisons. Scalar operands may
be fields, typed literals, or the supported `tonumber` and `replace` calls;
arithmetic, field quoting, `XOR`, and other eval functions are not yet accepted.
Missing, null, container, or failed numeric operands do not pass the filter.
Dynamic values compare through their runtime scalar types: integer pairs retain
full 64-bit precision, numeric pairs compare numerically, and string pairs
compare lexically. Canonical `_time` and `_indextime` use Unix epoch seconds in
numeric comparisons. Extended decimals have the same `Float64` precision caveat
as base-search comparisons.

## Pipeline commands

### `eval`

```spl
| eval duration_ms=tonumber(replace(duration, "ms$", ""))
```

Assignments are evaluated from left to right, and later assignments may use an
earlier output. Existing fields can be replaced without producing duplicate
columns. Literal assignments retain their `Int64`, `UInt64`, `Float64`, `Bool`,
or `String` output type. The current scalar-expression surface is deliberately narrow:

- `replace(value, "regex", "replacement")` substitutes every match and
  requires literal regex/replacement arguments;
- `tonumber(value)` returns a nullable `Float64`; invalid, missing, null,
  non-string dynamic, multivalue, object, `NaN`, and infinite inputs become
  null.

Splunk uses PCRE for `replace`; Open Splunk validates and executes the bounded
RE2-compatible subset supported by ClickHouse. Any pattern capable of a
zero-width match is rejected because ClickHouse does not implement PCRE's
global zero-width replacement semantics. These differences are explicit.
The optional `tonumber` base and integer-versus-double result distinction are
not yet implemented.

### `fields`

```spl
| fields field1, field2
| fields - field1, field2
```

Only exact field names are supported. Include mode retains `_time` and `_raw`
when they are still available. Exclusion blocks removed dynamic paths from
being resolved later. Wildcard field names are not yet supported.

### `table`

```spl
| table field1, field2
```

Declares the exact ordered output schema and removes other public fields. If a
prior transforming command removed a requested field, the declared column is
retained as a nullable missing value rather than resurrecting event data.

### `sort`

```spl
| sort [-|+]field, [-|+]field
| sort 20 -field
| sort 0 field
```

The default result bound is 10,000. An explicit zero removes the logical SQL
limit but does not bypass server execution, memory, group, result-row, or
result-byte budgets. Dynamic values use numeric-aware automatic ordering;
stable private row/group identities make ties deterministic. Missing values
sort last in the forward direction and first when `tail` reverses the order.

### `head` and `tail`

```spl
| head 20
| tail 20
```

The default count is 10. This version supports only a positive integer count;
predicate, `limit=`, `null`, and `keeplast` forms are rejected. `tail` returns
the selected rows in reversed order, matching its pipeline semantics.

### `stats`

```spl
| stats count
| stats count AS events BY field1, field2
| stats count p95(duration_ms) AS p95_ms BY path
```

Argument-free `count` and `p95(field)` are supported, including multiple
space- or comma-separated measures and `AS` aliases. The command is
transforming: output contains only the `BY` fields followed by measures in
source order. `count` includes every row in a retained group; `p95` ignores
missing, null, nonnumeric, `NaN`, and infinite inputs and returns nullable
`Float64`. Canonical timestamps are converted to Unix epoch seconds, and
tagged decimal values are converted to finite `Float64` inputs.

`p95` uses ClickHouse `quantileGKOrNull(100, 0.95)`, a bounded approximately
1%-rank-error aggregate. Splunk's percentile implementation uses different
exact/interpolated behavior for small cardinalities and a proprietary bounded
approximation for larger inputs, so exact percentile values can differ while
the rank/error intent is preserved. An all-null percentile remains null.

Missing and explicit-null group values are omitted. Dynamic scalar values
group by their lexical representation, so numeric `500` and string `"500"`
share a group. Bytes, timestamps, durations, and decimals use deterministic
tagged scalar representations. Lists and objects fail the whole search with an
unsupported-value error before any group is exposed.

Downstream `search`, `where`, `eval`, `fields`, `table`, `sort`, `head`, `tail`,
and another supported transforming command operate on the statistical schema,
never on hidden event columns.

### `top`

```spl
| top field
| top 20 field
| top limit=20 field
| top limit=0 field
```

This version supports one exact scalar field. Default output is exactly:

```text
field, count, percent
```

`count` is an unsigned 64-bit frequency. `percent` is an unrounded `Float64`
computed as `100 * count / eligible_count` across every eligible group before
the result limit is applied. Missing and explicit-null values are ineligible;
an empty string is a value. Lists and objects fail the whole search before any
partial result is exposed.

Rows order by count descending, then field value descending in UTF-8 lexical
order. The default limit is 10. `limit=0` means no logical top-N limit, but the
query still fails explicitly at Open Splunk's configured group/result budget;
the default distinct-group budget is 10,001. This resource policy intentionally
differs from Splunk's installation-configurable `maxresultrows` ceiling.

Multiple fields, `BY`, wildcards, `countfield`, `percentfield`, `showcount`,
`showperc`, `useother`, and `otherstr` are not yet supported. Selecting a field
named `count` or `percent` is rejected until output-renaming options exist.

### `timechart`

```spl
| timechart span=5m count BY level
```

The initial slice accepts exactly one argument-free `count`, one exact split
field, and a positive fixed `s`, `m`, or `h` span from one second through 24
hours. `timechart` must be the final command because its wide output columns
are derived from runtime field values. Options, aliases, calendar/subsecond
spans, multiple split fields, and other aggregate functions fail explicitly.

The search time range remains half-open `[earliest, latest)`. Buckets are
aligned to Unix epoch boundaries using mathematical floor division, including
before 1970. Partial first and last buckets are retained, missing buckets are
filled, and rows are ordered by `_time` ascending. `timechart` requires the
unmodified canonical `_time`; removing, replacing, or transforming it is a
source-located error. A completely empty input returns an `_time`-only schema
and zero rows.

The public result is wide: `_time` is a non-null timestamp followed by
non-null unsigned count columns. The ten ordinary string series with the
highest total count across the complete range are retained; equal scores use
UTF-8 lexical order. Ordinary output columns are then ordered lexically,
followed by `NULL` for missing/explicit-null values and `OTHER` for omitted
ordinary series. `NULL` does not consume a top-ten slot. Split values beginning
with `_` receive Splunk's `VALUE` prefix (`_audit` becomes `VALUE_audit`). An
upstream projection that removes the split field treats it as missing for all
retained events.

This version supports string split values plus missing/null. Numeric, Boolean,
extended, list, and object values fail the whole command before schema or rows
are published; Splunk's default numeric discretization is not approximated.
Empty, invalid UTF-8, reserved `NULL`/`OTHER`, or labels over 256 bytes also
fail atomically, as do collisions after `VALUE` normalization. Results are
bounded to 10,000 buckets and 12 runtime series (ten ordinary, `NULL`, and
`OTHER`), for at most 13 public columns. The 10,000-bucket resource policy is
intentionally lower than Splunk's installation-configurable `maxbins` default.
With default executor settings, the intermediate group budget grows with the
requested bucket count to at most 130,000 states. Domains with enough distinct
raw values to exceed that budget fail atomically with an execution-limit error;
an explicitly configured lower group cap remains authoritative.

## Current GradeThis corpus

All ten of the product plan's initial searches compile in v0.1, and their
storage/execution primitives have pinned ClickHouse integration coverage:

- trace-ID event investigation;
- errors/warnings with descending time;
- quoted raw error fragments;
- severity counts;
- frequent errors by logger and message;
- event volume by severity through `timechart`;
- server errors by route through `timechart`;
- HTTP response counts by route and status;
- slow routes through `eval`, `p95`, and `where`; and
- top messages.

## Explicitly unsupported surface

The following planned commands are not implemented in this version:

```text
rename, rex, spath, bin, bucket, chart,
dedup, rare, eventstats, streamstats
```

All `stats` functions other than argument-free `count` and `p95(field)` are
unsupported, including `count(field)`, `dc`, `values`, `list`, `sum`, `avg`,
`min`, `max`, `earliest`, `latest`, other fixed percentiles, `perc<N>`,
`upperperc`, and `exactperc`.

This contract will be versioned as support expands. A live Splunk differential
oracle is not currently available, so ambiguous null, multivalue, formatting,
and type edges remain conservative and must gain executable tests before they
are declared compatible.

Reference behavior is compared against Splunk's official [`search`](https://help.splunk.com/en/splunk-enterprise/search/spl-search-reference/10.2/search-commands/search),
[`sort`](https://help.splunk.com/en/splunk-enterprise/search/spl-search-reference/10.2/search-commands/sort),
[`stats`](https://help.splunk.com/en/splunk-enterprise/spl-search-reference/10.0/search-commands/stats),
[`where`](https://help.splunk.com/en/splunk-enterprise/spl-search-reference/10.2/search-commands/where),
[`replace`](https://help.splunk.com/en/splunk-cloud-platform/spl-search-reference/10.4.2604/evaluation-functions/text-functions),
[`tonumber`](https://help.splunk.com/en/splunk-enterprise/search/spl-search-reference/9.0/evaluation-functions/conversion-functions),
[`percentile functions`](https://help.splunk.com/en/splunk-enterprise/search/spl-search-reference/9.4/statistical-and-charting-functions/aggregate-functions),
[`top`](https://help.splunk.com/en/splunk-enterprise/spl-search-reference/9.0/search-commands/top),
and [`timechart`](https://help.splunk.com/en/splunk-enterprise/spl-search-reference/10.4/search-commands/timechart)
documentation.
