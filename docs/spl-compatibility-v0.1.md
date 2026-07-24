# Open Splunk SPL compatibility contract v0.1

**Status:** executable implementation contract
**Compatibility version:** `0.1`
**Last updated:** July 23, 2026

Open Splunk accepts only the syntax and behavior described here. Unsupported
commands or forms fail with a source-located diagnostic; the compiler never
executes a supported prefix as a partial search.

All searches are additionally constrained by the server-resolved tenant,
authorized index set, half-open event-time range, index-time cutoff, and
storage visibility snapshot. SPL cannot widen those boundaries.

To bound parser, compiler, and ClickHouse AST work, one search may contain at
most 16 KiB of UTF-8 source, 1,024 syntax tokens, and 64 pipeline commands.
Scalar expressions may nest 32 levels, with at most 32 `where` comparisons.
`eval` and `rename` accept at most 64 assignments; `stats` accepts at most 16
measures and 16 `BY` fields; `dedup` accepts at most 16 key fields. A `rex`
pattern and its normalized form are each limited to 4 KiB and 16 total capture
groups, including unnamed groups; one query may produce at most 64 named `rex`
outputs. Across all `rex` stages, the total bytes in all capture groups for
one row may not exceed 4 MiB; exceeding that runtime ceiling fails the query
with an execution-limit error.
Exceeding a general structural limit returns the source-located
`SPL_QUERY_TOO_COMPLEX` diagnostic before planning or execution. Unsupported
`dedup` arity is reported as `SPL_UNSUPPORTED_DEDUP_SYNTAX`. Dynamic field
paths align with ingestion's ceiling: 17 dotted segments and 256 unescaped
UTF-8 bytes per segment. Generated ClickHouse SQL is additionally capped at
256 KiB; exceeding that internal expansion budget returns the same diagnostic.
The executor applies a 1 MiB ClickHouse `max_query_size` ceiling after bound
arguments are expanded, in addition to its time, memory, scan, group, and
result budgets.

## Search time range

Job creation requires both earliest and latest expressions. Version 0.1 accepts
only these executable forms:

- strict RFC 3339 timestamps with an explicit `Z` or numeric offset and up to
  nine fractional digits;
- the exact keyword `now`; and
- negative integer offsets `-N[s|m|h|d]`, where `N` is greater than zero.

Both endpoints resolve from one captured `now` anchor and form a half-open
`[earliest, latest)` interval. Seconds, minutes, and hours are elapsed durations.
Days are calendar days in the effective IANA timezone, so `-1d` and `-24h` can
differ across daylight-saving transitions, as they do in Splunk. An omitted
timezone means UTC. The original normalized expressions and optional timezone
presence are retained separately from the resolved UTC nanosecond timestamps,
including in search history.

Expressions outside ClickHouse's supported `DateTime64(9)` range, empty or
inverted intervals, invalid timezone names, and arithmetic overflow fail before
job admission. Splunk forms not listed above—including implied-one offsets such
as `-h`, positive offsets, snap expressions such as `@d` or `-1d@d`, aliases,
chained modifiers, and week/month/quarter/year units—are explicitly rejected in
version 0.1 rather than approximated.

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

### `rex`

```spl
| rex "method=(?<method>[A-Z]+)\s+status=(?<status>\d+)"
| rex field=duration max_match=1 "^(?<value>\d+(?:\.\d+)?)(?<unit>ms|µs)$"
```

Version 0.1 supports extraction mode for the first regular-expression match.
The source defaults to `_raw`; `field=<exact unquoted field>` selects another
source. `field` and `max_match=1` may appear in either order before the pattern;
the supported `max_match=1` spelling is also accepted immediately after it.
It is otherwise the default. The expression is unanchored unless the pattern
contains anchors. Sed mode, `offset_field`, `max_match=0`, `max_match` greater
than one, and other option forms fail with `SPL_UNSUPPORTED_REX_SYNTAX`.
Quoted and wildcard field names are not accepted.

Every pattern must contain at least one uniquely named capture. Both
`(?<name>...)` and `(?P<name>...)` are accepted. Unnamed groups are permitted
and count toward both later capture indexes and the 16-group limit. All named
captures from the same match are assigned simultaneously, so a capture may
replace its source field without changing the input seen by another capture.
On a match, every named output is a present string. Because ClickHouse does not
distinguish a nonparticipating optional group from a participating empty group,
both are represented by the empty string in this compatibility version.

A missing, explicit-null, non-string, multivalue, object, binary, or invalid
UTF-8 source behaves as no match. On no match, an existing destination retains
its exact prior value and presence, including explicit null; a destination
that did not exist remains missing. A successful match replaces a prior
destination even when its old value had another type. Downstream commands see
the new fields and their exact sparse presence. A capture updates the exact
destination spelling; separately flattened dotted descendants remain
available to downstream SPL.

Splunk executes PCRE, while Open Splunk validates and executes RE2 through
ClickHouse. Lookaround, backreferences, conditionals, duplicate capture names,
and other constructs outside RE2 are rejected with `SPL_UNSUPPORTED_REGEX`.
Dot does not match newline by default; an inline `(?s)` can opt in. RE2's `$`
is a strict end-of-text anchor unless multiline mode is enabled, unlike PCRE's
default allowance before a final newline. A zero-width first match is
supported, including an empty named capture; global zero-width iteration is
outside the supported `max_match=1` surface.

The normalized pattern is passed as a bound argument. Each `rex` stage
short-circuits ineligible rows and streams one optimizer-shared capture array
per eligible row before projecting every output; a pinned ClickHouse
`EXPLAIN actions=1` regression verifies one physical extraction action.
Capture-group bytes are accumulated across all stages and capped at 4 MiB per
row. On an open event schema, `fields` is the reserved whole-payload convenience
column and cannot be the source or a capture name; a prior exact `table` or
transforming schema may declare an ordinary column with that spelling. The
convenience payload is omitted after `rex` because it cannot safely reflect
sparse per-event replacements, while unrelated dynamic fields remain
available to downstream SPL. Capturing `index` changes only calculated
pipeline data and cannot widen the already-resolved authorization scope.
Capturing `_time` makes the pipeline ineligible for `timechart` and timeline
analysis because the canonical event clock has been replaced.

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

### `rename`

```spl
| rename logger AS component
| rename path AS route, status AS response_status
```

This slice accepts one or more comma-separated, exact `source AS destination`
pairs. Pairs run from left to right, so `a AS b, b AS c` leaves `c` with the
original value of `a`. Repeating a source, merging multiple sources into one
destination, using the same source and destination, omitting `AS`, or omitting
the comma between pairs produces a source-located diagnostic. Wildcard rename
patterns and quoted field-name syntax are not supported. On an open event
schema, sources and destinations must be top-level exact fields; dotted paths
require an upstream `table` or transforming command that declares the dotted
name as one exact output column. Descendant paths under an open-schema source
or destination are unavailable after the rename rather than resolving stale
members from the immutable payload.

A present source replaces an existing destination. On a closed transforming
schema, a missing source nulls an existing destination and a missing-to-missing
pair is a no-op. Event schemas are sparse, so an exact dynamic source such as
`logger` or `path` is represented by one nullable destination column; its
per-row presence predicate follows the original source. The old name is
blocked from later pipeline resolution, while later `search`, `where`, `stats`,
`table`, and rename pairs resolve the new name. The immutable tenant, physical
index, time, and visibility scan predicates remain below every rename stage.
Renaming from or to `_time` makes the value ineligible as timechart's canonical
event clock. Renaming from or to `index` never changes the already-resolved
authorization scope or turns calculated data into a physical index selector.

Open Splunk normally includes a convenience `fields` JSON object in unprojected
event results. Its contents are the immutable stored dynamic payload, so after
the first rename on an open event schema that convenience column is omitted
instead of leaking the old source or an overwritten destination. The private
payload remains available to downstream SPL: unrelated dynamic fields can
still be searched or selected with `table`. Using `fields` itself as a rename
source or destination is rejected as ambiguous unless an upstream exact schema
has declared an ordinary column with that name. This result-shape rule avoids a
per-event JSON serialize/reparse pass for every rename pair.

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

### `dedup`

```spl
| dedup session_id
| dedup 2 host, source
| sort 0 +_time | dedup request_id
```

The optional leading count is a positive unsigned 64-bit integer and defaults
to one. One through 16 unique exact fields may follow, separated by whitespace,
commas, or both. Quoted fields, wildcards, a trailing comma, duplicate fields,
zero/negative/overflowing counts, and the `keepempty`, `consecutive`,
`keepevents`, and `sortby` forms are rejected with the source-located
`SPL_UNSUPPORTED_DEDUP_SYNTAX` diagnostic.

`dedup` is global rather than consecutive: it retains the first count rows for
each complete key tuple in the deterministic order established by the current
pipeline. Event searches default to `_time DESC, event_id DESC`; an upstream
sort controls the winners. Use `sort 0` when the sort itself must not impose its
normal 10,000-row bound. The command preserves both the public schema and the
current row order, including through downstream projections and transforming
pipelines.

With the supported default `keepempty=false` behavior, a row missing any key or
having an explicit null in any key is removed. An empty string is a present
value. Keys compare case-sensitively. Dynamic scalar keys use the same lexical
normalization as `stats BY`, so numeric `500` and string `"500"` share a key;
bytes, timestamps, durations, and decimals use their deterministic tagged
scalar representations. A field removed by an upstream projection is missing
for every row and is never recovered from private event data.

On an open event schema, the name `fields` is reserved for the convenience
whole-payload result and is therefore rejected as an ambiguous dedup key. A
closed upstream schema may declare an ordinary output with that name, for
example `stats count AS fields | dedup fields`.

Multivalue lists and objects, including flattened non-empty object parents,
are an explicit Open Splunk v0.1 unsupported-value boundary: encountering one
in any present key fails the whole search before any row is exposed. Splunk
supports broader field-value behavior; this restriction is conservative and
intentional rather than a claim of exact compatibility for containers.

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
| stats sum(bytes) AS total_bytes avg(duration_ms) AS mean_ms BY path
```

Argument-free `count` plus `p95(field)`, `sum(field)`, and `avg(field)` are
supported, including multiple space- or comma-separated measures and `AS`
aliases. Function names are case-insensitive; default output names use canonical
lowercase spelling such as `sum(bytes)`. The command is transforming: output
contains only the `BY` fields followed by measures in source order. `count`
includes every input row in a retained group.

The current downstream field grammar cannot reference a default aggregate name
that contains parentheses. Use `AS` when a `sum`, `avg`, or `p95` result will be
consumed by a later `search`, `where`, `sort`, `fields`, or `table` command.

The numeric aggregates accept finite integers, floats, numeric strings, tagged
decimals, and canonical timestamps converted to Unix epoch seconds. Missing,
null, empty-string, Boolean, bytes, object, nonnumeric, `NaN`, and infinite
inputs are ignored. For `sum` and `avg`, each finite numeric scalar in a
top-level multivalue array contributes independently; nonnumeric elements and
nested containers are ignored without expanding event rows. `p95` retains its
scalar-only behavior and treats a multivalue input as ineligible.

`sum`, `avg`, and `p95` return nullable `Float64`. A global aggregation over no
rows still emits one row; an aggregate with no eligible numeric contribution is
null, including `sum` rather than zero. Splunk's primary documentation specifies
that nonnumeric values are ignored but does not define the empty `sum` result,
so null is the explicit v0.1 compatibility choice. A grouped aggregation over no
rows emits no groups. Finite inputs are filtered before aggregation, but a
computed IEEE `NaN` or positive/negative infinity caused by Float64 arithmetic
is preserved rather than changed to null. Projected-away inputs stay absent and
cannot be recovered from hidden event columns.

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

### `rare`

```spl
| rare field
| rare 20 field
| rare limit=20 field
| rare limit=0 field
```

The supported schema, scalar eligibility, percentage calculation, limit
behavior, and failure semantics are identical to `top`. Rows order by count
ascending to select the least frequent values, then by field value descending
in UTF-8 lexical order for a deterministic tie cutoff. The default limit is 10;
`limit=0` removes the logical bottom-N limit while retaining the same configured
group and result budgets.

Only one exact field and a limit are accepted. Multiple fields, `BY`, wildcards,
`countfield`, `percentfield`, `showcount`, `showperc`, and any other option are
rejected. A field named `count` or `percent` is rejected because it collides with
the fixed output schema.

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

Aligned bucket starts are not constrained to ClickHouse's timestamp storage
range. For example, a supported search beginning at `1900-01-01T00:00:00Z`
with `span=7h` retains the partial bucket beginning at
`1899-12-31T19:00:00Z`; the executor reconstructs public bucket timestamps from
bounded integer ordinals rather than round-tripping them through `DateTime64`.

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

## Completed-job field analysis

Field discovery re-executes an immutable completed-job snapshot with the same
tenant, authorized indexes, half-open event-time range, index-time cutoff, and
visibility cutoff as the original search. It analyzes the final event relation,
so `search`, `where`, `eval`, `rex`, `rename`, `fields`, `table`, `sort`,
`head`, `tail`, and `dedup` affect the catalog and summaries exactly as they
affect event results. Transforming final relations are rejected explicitly.

`POST /api/v1/search/jobs/fields/list` returns a bounded, case-sensitive field
catalog. Presence, explicit null, and missing counts are separate, and observed
types retain stored scalar/container identity. Catalog pages do not claim a
distinct count; page tokens are scoped to the caller and immutable snapshot.

`POST /api/v1/search/jobs/field-summary` returns an exact frequency summary for
one catalog spelling. Missing and explicit-null values are excluded from the
distinct count and top values. Scalar type is part of value identity, so the
integer `500` and string `"500"` remain distinct. Equivalent decimal spellings
are canonicalized before counting. Ties order by canonical display value and
then type, making prefixes deterministic.

Version 0.1 returns at most 100 values and reads at most 10,000 raw typed scalar
encodings before canonicalizing equivalent values. The exact semantic distinct
count is therefore also limited to 10,000. A search with more than 10,000 raw
encodings can fail its execution limit even when equivalent decimal spellings
would collapse below that limit; it is never sampled or reported as exact.
An encoded or canonical value above 256 KiB also fails the summary. Lists and
objects fail the whole summary as unsupported; the server does not silently
stringify, sample, or approximate them. Both catalog and summary execution,
coalescing, caches, response bytes, and detached worker lifecycle are bounded.
The `enable_field_discovery` create option remains reserved for future eager or
in-progress analysis; completed-job analysis is available on demand whenever
the bootstrap advertises field discovery.

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
spath, bin, bucket, chart, eventstats, streamstats
```

All `stats` functions other than argument-free `count`, `p95(field)`,
`sum(field)`, and `avg(field)` are unsupported, including `count(field)`, `dc`,
`values`, `list`, `min`, `max`, `earliest`, `latest`, other fixed percentiles,
`perc<N>`, `upperperc`, and `exactperc`.

This contract will be versioned as support expands. A live Splunk differential
oracle is not currently available, so ambiguous null, multivalue, formatting,
and type edges remain conservative and must gain executable tests before they
are declared compatible.

Reference behavior is compared against Splunk's official [`search`](https://help.splunk.com/en/splunk-enterprise/search/spl-search-reference/10.2/search-commands/search),
[`sort`](https://help.splunk.com/en/splunk-enterprise/search/spl-search-reference/10.2/search-commands/sort),
[`dedup`](https://help.splunk.com/en/splunk-enterprise/search/spl-search-reference/10.2/search-commands/dedup),
[`stats`](https://help.splunk.com/en/splunk-enterprise/spl-search-reference/10.0/search-commands/stats),
[`where`](https://help.splunk.com/en/splunk-enterprise/spl-search-reference/10.2/search-commands/where),
[`rex`](https://help.splunk.com/en/splunk-cloud-platform/spl-search-reference/10.2.2510/search-commands/rex),
[`replace`](https://help.splunk.com/en/splunk-cloud-platform/spl-search-reference/10.4.2604/evaluation-functions/text-functions),
[`tonumber`](https://help.splunk.com/en/splunk-enterprise/search/spl-search-reference/9.0/evaluation-functions/conversion-functions),
[`rename`](https://help.splunk.com/en/splunk-enterprise/search/spl-search-reference/10.2/search-commands/rename),
[`percentile functions`](https://help.splunk.com/en/splunk-enterprise/search/spl-search-reference/9.4/statistical-and-charting-functions/aggregate-functions),
[`top`](https://help.splunk.com/en/splunk-enterprise/spl-search-reference/9.0/search-commands/top),
[`rare`](https://help.splunk.com/en/splunk-enterprise/spl-search-reference/9.4/search-commands/rare),
[`timechart`](https://help.splunk.com/en/splunk-enterprise/spl-search-reference/10.4/search-commands/timechart),
[`time modifiers`](https://help.splunk.com/en/splunk-enterprise/search/search-manual/10.4/specify-time-ranges/specify-time-modifiers-in-your-search),
ClickHouse's [`extractGroups`](https://clickhouse.com/docs/sql-reference/functions/string-search-functions),
and the [RE2 syntax reference](https://github.com/google/re2/wiki/Syntax)
documentation.
