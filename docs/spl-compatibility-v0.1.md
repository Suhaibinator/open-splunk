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

## Search expressions

Base search and pipeline `search` support:

- bare terms and quoted phrases over `_raw`;
- `AND`, `OR`, `NOT`, parentheses, and implicit `AND`;
- Splunk search-command precedence: parentheses, `NOT`, `OR`, then `AND`;
- `=`, `!=`, `<`, `<=`, `>`, and `>=` field comparisons;
- string, signed/unsigned integer, floating-point, boolean, and null literals;
- `*` wildcards in term and comparison values; and
- canonical fields plus deterministic dotted dynamic-field paths.

Comparisons preserve literal type intent. Dynamic numeric comparisons do not
coerce arbitrary strings into stored numeric values. `field!=value` excludes a
missing field, while `NOT field=value` includes it. `field=*` requires a
present, non-null value. Canonical `index` comparisons are case-sensitive;
ordinary string comparisons are case-insensitive.

## Pipeline commands

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
```

Only argument-free `count` is supported. The command is transforming: output
contains only the `BY` fields followed by the count output. Missing and
explicit-null group values are omitted. Dynamic scalar values group by their
lexical representation, so numeric `500` and string `"500"` share a group.
Bytes, timestamps, durations, and decimals use deterministic tagged scalar
representations. Lists and objects fail the whole search with an unsupported-
value error before any group is exposed.

Downstream `search`, `fields`, `table`, `sort`, `head`, `tail`, and another
supported transforming command operate on the statistical schema, never on
hidden event columns.

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

## Current GradeThis corpus

Seven of the product plan's ten initial searches compile and execute in v0.1:

- trace-ID event investigation;
- errors/warnings with descending time;
- quoted raw error fragments;
- severity counts;
- frequent errors by logger and message;
- HTTP response counts by route and status; and
- top messages.

The three remaining searches fail explicitly at `timechart` or `eval`.
`timechart`, `eval`, `where`, and percentile aggregation are the blockers for
those corpus entries.

## Explicitly unsupported surface

The following planned commands are not implemented in this version:

```text
where, eval, rename, rex, spath, bin, bucket, chart, timechart,
dedup, rare, eventstats, streamstats
```

All `stats` functions other than argument-free `count` are unsupported,
including `count(field)`, `dc`, `values`, `list`, `sum`, `avg`, `min`, `max`,
`earliest`, `latest`, and percentile functions.

This contract will be versioned as support expands. A live Splunk differential
oracle is not currently available, so ambiguous null, multivalue, formatting,
and type edges remain conservative and must gain executable tests before they
are declared compatible.

Reference behavior is compared against Splunk's official [`search`](https://help.splunk.com/en/splunk-enterprise/search/spl-search-reference/10.2/search-commands/search),
[`sort`](https://help.splunk.com/en/splunk-enterprise/search/spl-search-reference/10.2/search-commands/sort),
[`stats`](https://help.splunk.com/en/splunk-enterprise/spl-search-reference/10.0/search-commands/stats),
and [`top`](https://help.splunk.com/en/splunk-enterprise/spl-search-reference/9.0/search-commands/top)
documentation.
