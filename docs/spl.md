# SPL contract

Open Splunk implements the authored-search behavior described here. The
language is pre-release and may change with the source tree; this is not a
claim of full Splunk parity or a backward-compatibility promise. Callers cannot
select alternate grammars or individual feature sets. The strict executable
corpus is `internal/spl/testdata/compatibility.json`; this document and that
corpus must change together.

Unsupported syntax fails with a source-located diagnostic before execution.
The compiler never executes a valid prefix of an invalid search. Every search
is additionally restricted by the server-resolved tenant, indexes, half-open
event-time interval, index-time cutoff, visibility snapshot, and immutable
knowledge authority. SPL cannot widen those boundaries.

## Authored surface and common limits

`SPL-PROFILE-001` defines the single current authored surface and its use by
validation, jobs, saved-search launch, history rerun, knowledge composition,
paging, export, inspection, field analysis, timeline, suggestions, and browser
syntax. Rebuilding retained authority with another compiler identity fails
closed. Calculated knowledge fields deliberately use the smaller
`KnowledgeExpressionProfile`, not the complete authored grammar.

One search is limited to 16 KiB of UTF-8 source, 1,024 tokens, 64 pipeline
commands, 32 expression levels, and 96 relational dependency levels. `eval`
and `rename` accept at most 64 assignments; statistical commands accept at
most 16 measures and 16 grouping fields; `dedup` accepts at most 16 keys.
Generated SQL is capped at 256 KiB and the expanded ClickHouse request at 1
MiB. Regex, wildcard, concatenation, multivalue, grouping, external lookup,
query-memory, scan, duration, row, and result-byte budgets apply independently.
Crossing a source-derived structural limit reports `SPL_QUERY_TOO_COMPLEX`;
data-derived hard-limit failures are atomic and redact backend details.

The semantic rule inventory is:

| Rule | Contract |
| --- | --- |
| `SPL-PROFILE-001` | one authored surface and one immutable product vertical |
| `SPL-GRAMMAR-001` | bounded authored eval grammar |
| `SPL-PRECEDENCE-001` | fixed operator precedence and associativity |
| `SPL-GROUPING-001` | scalar/Boolean parenthesis disambiguation |
| `SPL-LEXER-001` | exact punctuation and operator transitions |
| `SPL-QUOTED-FIELD-001` | exact quoted field references where explicitly supported |
| `SPL-STATS-BY-MULTIVALUE-001` | bounded multivalue grouping |
| `SPL-ARITHMETIC-TYPE-001` | numeric operator eligibility and result types |
| `SPL-ARITHMETIC-NULL-001` | missing, null, and conversion behavior |
| `SPL-ARITHMETIC-EXCEPTION-001` | divide-by-zero, overflow, and non-finite behavior |
| `SPL-ARITHMETIC-EVALUATION-001` | bounded, deterministic evaluation |
| `SPL-MEMBERSHIP-001` | `IN`/`NOT IN` syntax and equality |
| `SPL-MEMBERSHIP-NULL-001` | three-valued membership results |
| `SPL-EXPRESSION-LIMITS-001` | expression structural and work ceilings |
| `SPL-SECURITY-001` | authorization and parameterized-SQL invariants |
| `SPL-EXPRESSION-DIAGNOSTICS-001` | stable expression failures |
| `SPL-REGEX-001` | bounded row-filtering regex command |
| `SPL-REVERSE-001` | complete established-order reversal |
| `SPL-ACCUM-001` | running numeric sum |
| `SPL-STRCAT-001` | bounded concatenation command |
| `SPL-ADDINFO-001` | immutable admitted-search metadata |
| `SPL-FILLNULL-001` | explicit String fill projection |
| `SPL-ADDTOTALS-001` | row-only explicit numeric total |
| `SPL-DELTA-001` | bounded ordered subtraction |
| `SPL-MULTIVALUE-EVAL-001` | bounded native multivalue eval functions |
| `SPL-SPATH-MULTIVALUE-001` | ordered wildcard-array JSON extraction |
| `SPL-MAKEMV-001` | typed String multivalue construction |
| `SPL-MVEXPAND-001` | controlled ordered row expansion |
| `SPL-NOMV-001` | presentation-only flat multivalue display |
| `SPL-FIELDS-001` | exact public command fields and private namespace |
| `SPL-MULTIVALUE-TYPE-001` | nullable native typed-list transport |
| `SPL-ORDER-001` | durable private relation lineage |
| `SPL-PIPELINE-LIMITS-001` | pipeline source and runtime accounting |
| `SPL-ATOMIC-001` | no observable result prefix on hard failure |
| `SPL-PIPELINE-DIAGNOSTICS-001` | stable pipeline failures |
| `SPL-LOOKUP-SYNTAX-001` | exact lookup syntax and row-preserving semantics |
| `SPL-LOOKUP-BOUNDS-001` | bounded immutable lookup resolution and execution |

## Time and base search

Job creation requires earliest and latest time intent. Accepted endpoints are
strict RFC 3339 with an explicit offset, `now`, negative integer
`s|m|h|d` offsets, `@d`, `-Nd@d`, and earliest-only `0` for the backend
minimum. Both endpoints resolve from one clock capture and form `[earliest,
latest)`. Calendar-day operations use the effective IANA timezone; elapsed
hours and calendar days can differ across daylight-saving transitions.

Base search and pipeline `search` support terms and phrases over `_raw`,
parentheses, implicit and explicit `AND`, `OR`, `NOT`, typed field comparisons,
`*` wildcards, canonical fields, and bounded dotted dynamic paths. Search
precedence is parentheses, `NOT`, `OR`, then `AND`. Ordinary string matching is
case-insensitive; canonical index identity is case-sensitive. `field!=value`
does not match a missing field, while `NOT field=value` does. `field=*`
requires present non-null data.

Eligible positive ASCII bare terms may add a ClickHouse text-index candidate,
but the exact parameterized regex remains authoritative. Negative terms,
phrases, wildcard/Unicode/punctuation terms, and values without physical `_raw`
lineage do not use that optimization.

## Expressions

`where` and conditional predicates use explicit Boolean operators with
precedence `NOT`, `AND`, then `OR`. Quoted text is a literal and bare names are
fields. Supported scalar functions include `isnull`, `isnotnull`, `replace`,
`tonumber`, `if`, `coalesce`, `case`, period concatenation, `lower`, `upper`,
`len`/`length`, `substr`, default `tostring`, `round`, `ceil`/`ceiling`,
`floor`, `mvcount`, `mvsort`, `split`, `mvappend`, `mvdedup`, `mvindex`,
`mvjoin`, `mvzip`, `mvfind`, `match`, `like`, `now`, `relative_time`,
`strftime`, and `strptime`.

Authored `eval` additionally supports bounded arithmetic `+`, `-`, `*`, `/`,
and `%`, plus `IN` and `NOT IN` membership. Arithmetic accepts finite numeric
values and bounded numeric strings, preserves null for missing/ineligible
operands, and follows the typed result and exceptional-value rules pinned by
the corpus. Membership compares each right-hand candidate with the left and
returns Boolean or null under three-valued semantics. Grouping never changes
a scalar expression into truthiness or a Boolean predicate into a scalar.

`eval` assignments run left to right, so a later assignment may read an
earlier output. A fixed output replaces the existing occurrence instead of
creating a duplicate. Missing and explicit null remain distinct from empty
String, zero, false, and present empty multivalue values.

### Native multivalue expressions

Native multivalue fields preserve member order and the scalar JSON types
String, finite Number/Decimal, Boolean, and explicit null. Missing fields,
explicit null fields, and present empty lists are distinct. Bytes, time,
duration, non-finite numbers, objects, and nested lists are not valid native
multivalue members. Operations that consume member text use raw Strings,
canonical numeric text, lowercase `true` and `false`, and literal `null`.
Native equality is type-sensitive, so integer `1` and floating `1.0` are
different members.

Each constructed list is limited to 1,000 members and 1 MiB of member payload.
Construction is preflighted and an overflow fails the query atomically. A
delimiter is a quoted UTF-8 literal of at most 1 KiB. The eval functions have
these exact contracts:

- `split(string, delimiter)` splits on the exact literal delimiter and returns
  a String multivalue. An empty delimiter splits into Unicode characters.
- `mvappend(value, ...)` accepts one through 32 scalar or multivalue arguments,
  flattens one list level in argument order, skips missing arguments, and keeps
  explicit null members.
- `mvdedup(mv)` retains the first occurrence of each type-sensitive native
  value.
- `mvindex(mv, start)` returns one scalar. `mvindex(mv, start, end)` returns an
  inclusive multivalue range. Indexes are signed 32-bit integer literals;
  negative indexes count from the end, and invalid or out-of-range selections
  return null.
- `mvjoin(mv, delimiter)` returns one String by joining canonical member text.
- `mvzip(left, right)` uses `","`; the three-argument form uses its supplied
  literal delimiter. Both truncate to the shorter list and return String
  pairs in a multivalue.
- `mvfind(mv, regex)` accepts a quoted bounded RE2 pattern and returns the first
  zero-based matching member index, or null.

## Commands

The cumulative command surface is:

| Command | Implemented behavior |
| --- | --- |
| `search` | base-search predicate grammar over the current relation |
| `where` | bounded eval-predicate filtering |
| `eval` | left-to-right fixed scalar assignments |
| `rex` | first-match RE2 extraction with bounded named captures |
| `spath` | row-preserving typed JSON scalar and wildcard-array extraction (`SPL-SPATH-MULTIVALUE-001`) |
| `fields`, `table` | exact projection |
| `rename` | exact source/destination pairs |
| `sort` | bounded exact keys, with `sort 0` selecting all scoped rows |
| `dedup` | global first-N retention by exact key tuple |
| `head`, `tail` | bounded row selection; `tail` publishes reversed selection order |
| `stats` | bounded transforming aggregation and grouping |
| `eventstats` | bounded row-preserving aggregate attachment |
| `streamstats` | bounded ordered running aggregates |
| `top`, `rare` | bounded frequency summaries |
| `bin`/`bucket` | numeric and time discretization |
| `timechart`, `chart` | bounded chart aggregation and split series |
| `regex` | bounded RE2 row filtering (`SPL-REGEX-001`) |
| `reverse` | reverse the complete established relation order (`SPL-REVERSE-001`) |
| `accum` | running numeric sum (`SPL-ACCUM-001`) |
| `strcat` | bounded explicit-field concatenation (`SPL-STRCAT-001`) |
| `addinfo` | admitted time-range/search-start metadata (`SPL-ADDINFO-001`) |
| `fillnull` | explicit fields with a String replacement (`SPL-FILLNULL-001`) |
| `addtotals` | row-only total over explicit fields (`SPL-ADDTOTALS-001`) |
| `delta` | difference from 1 through 10,000 established rows earlier (`SPL-DELTA-001`) |
| `makemv` | literal-delimiter String splitting (`SPL-MAKEMV-001`) |
| `mvexpand` | bounded ordered native-list expansion (`SPL-MVEXPAND-001`) |
| `nomv` | newline-separated display without changing the typed list (`SPL-NOMV-001`) |
| `lookup` | immutable exact CSV enrichment (`SPL-LOOKUP-SYNTAX-001`) |

Command fields are exact and case-sensitive. The `__os_` namespace is private;
it cannot be authored, discovered, exported, or returned. Private order,
presence, provenance, and resource-ledger columns are removed before the public
schema is validated.

Order-sensitive commands consume an established total order. Sort, projection,
rename, aggregation, expansion, and limit operations either preserve or
deliberately replace private lineage. Hard failures in regex, multivalue,
lookup, arithmetic, or resource validation publish no schema or partial rows.

### `spath` multivalue extraction

An `spath` step may select no array, one fixed zero-based array index, or every
member with `{}`. Wildcards are valid at any step, including
`groups{}.users{}.name`; at most four total fixed or wildcard array selectors
may appear in one path. Multiple wildcards flatten depth-first in source array
order. Missing keys and non-array wildcard branches are skipped, while
explicit null leaves are retained. A matched empty terminal array produces a
present empty multivalue. Terminal objects, terminal nested arrays, and other
unsupported members fail the row atomically instead of publishing a partial
list.

Wildcard extraction preserves each supported JSON scalar's native type and
uses the same exact JSON-number classification as scalar `spath`. The existing
1 MiB input and 16,384-token guards remain, and one extraction may publish at
most 1,000 members. If no wildcard branch matches, an existing multivalue
output is preserved. An existing scalar or container output is an atomic
unsupported-type failure; a previously absent output remains absent.

### `nomv`

`nomv field` marks one exact public field for newline-separated flat display.
It is row-preserving and does not replace the authoritative typed multivalue:
downstream SPL, grouping, paging, APIs, and exports continue to use the list.
Mixed native members are displayed with the canonical text rules above.

For a runtime-typed field, `nomv` validates and normalizes an actual supported
list into sealed multivalue transport. Missing and null remain distinct;
present non-list scalars, objects, and nested containers fail atomically. The
presentation mark survives projection and follows rename, is cleared when the
field is overwritten, and can be reapplied by a later `nomv`.

### Statistical surface

`stats` accepts argument-free `count`; field and eval forms of current
field-taking functions; optional aliases; up to 16 `BY` fields; and bounded
`partitions`, `allnum`, `delim`, and `dedup_splitvals` options. Supported
families include:

- counts: `count`/`c`, `dc`/`distinct_count`, `estdc`, and `estdc_error`;
- numeric/distribution: `sum`, `sumsq`, `avg`/`mean`, `min`, `max`, `range`,
  `stdev`, `stdevp`, `var`, `varp`, `pN`/`percN`, `exactpercN`,
  `upperpercN`, and `median`;
- multivalue: `values` and `list`;
- order/time: `earliest`, `latest`, `first`, `last`, `earliest_time`,
  `latest_time`, and `rate`;
- frequency: `mode`; and
- `sparkline(...)` over the supported count, distinct-count, numeric, extrema,
  and variance families.

`count(field)` counts immediate non-null occurrences. `dc` and `values` use
canonical scalar spellings; `values` sorts exact bytes and `list` preserves
the first 100 eligible occurrences in current order. `dc` is exact to 100,000
distinct spellings per group. `values` publishes at most 10,000 entries and
512 KiB per group; `list` publishes 100 entries and 512 KiB per group; their
complete result is capped at 100,000 entries and 8 MiB. `BY` over multivalue
fields forms a staged Cartesian product capped at 10,000 combinations per
source event; `dedup_splitvals=true` removes repeated members before expansion.

Approximate percentile and estimated-distinct implementations are explicit
Open Splunk choices and are not claimed to reproduce an undocumented Splunk
algorithm. Typed mixed-value extrema use a deterministic numeric-first,
byte-lexical total order. Sparkline output remains a typed multivalue whose
first member is `##__SPARKLINE__##`; inline chart rendering is presentation
only.

## Lookup

The accepted syntax is:

```text
lookup definition key_column AS event_field
    [key_column AS event_field ...]
    (OUTPUT|OUTPUTNEW)
    output_column [AS event_field]
    [output_column [AS event_field] ...]
```

There are one through four key mappings and one through sixteen output
mappings. Names are exact and unquoted. Duplicate columns or destinations,
wildcards, calculated arguments, private names, and ambiguous open-schema
`fields` destinations are rejected. Keys are exact, case-sensitive tuples of
present scalar Strings; missing, null, other scalar types, multivalue, and
containers do not match. Published assets reject duplicate composite keys, so
lookup never changes row count.

`OUTPUT` replaces listed destinations on a match. `OUTPUTNEW` writes only when
the destination is missing or null; empty String, zero, false, time, and
multivalue are already present. No match preserves the complete input row.

A query admits at most sixteen explicit plus automatic lookup stages and 64
key components per event. Resolved assets and external tables are bounded to
6,400,000 cells in aggregate, with independent asset-byte, argument-byte,
generated-SQL, memory, and result limits. Automatic lookups run after
calculated fields and before the base predicate; they are deterministic and
cannot depend on another lookup.

## Diagnostics, security, and unsupported surface

Diagnostics use half-open UTF-8 byte ranges over authored source and stable
codes such as `SPL_QUERY_TOO_COMPLEX`, `SPL_EXPECTED_FIELD`,
`SPL_RESERVED_FIELD`, command-specific `SPL_UNSUPPORTED_*_SYNTAX`, and bounded
execution/unsupported-value categories. Generated SQL, arguments, raw backend
errors, event values, and private definitions are never public diagnostic
text.

Subsearches, joins, generating commands, external calls, mutation commands,
unbounded row generation, `inputlookup`, `outputlookup`, mutable/external URL
or scripted lookups, fuzzy/CIDR/temporal lookup matching, multivalue lookup
keys, automatic lookup chaining, and arbitrary Splunk configuration execution
are unsupported. A similarly named Splunk feature is not accepted unless it is
described here and covered by the executable corpus.
