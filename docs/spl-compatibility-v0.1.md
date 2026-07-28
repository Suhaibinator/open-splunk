# Open Splunk SPL compatibility contract v0.1

**Status:** executable implementation contract
**Compatibility version:** `0.1`
**Last updated:** July 27, 2026

Open Splunk accepts only the syntax and behavior described here. Unsupported
commands or forms fail with a source-located diagnostic; the compiler never
executes a supported prefix as a partial search.

All searches are additionally constrained by the server-resolved tenant,
authorized index set, half-open event-time range, index-time cutoff, and
storage visibility snapshot. SPL cannot widen those boundaries.

To bound parser, compiler, and ClickHouse AST work, one search may contain at
most 16 KiB of UTF-8 source, 1,024 syntax tokens, and 64 pipeline commands.
Scalar expressions may nest 32 levels, with at most 32 eval/where predicates.
`eval` and `rename` accept at most 64 assignments; `stats` accepts at most 16
measures and 16 `BY` fields; `dedup` accepts at most 16 key fields. A `rex`
pattern and its normalized form are each limited to 4 KiB, its estimated
post-repeat RE2 program to 4,096 work units, and its captures to 16 total
groups, including unnamed groups; one query may produce at most 64 named `rex`
outputs. Across all `rex` stages, the total bytes in all capture groups for one
row may not exceed 4 MiB; exceeding that runtime ceiling fails the query with
an execution-limit error.
Exceeding a general structural limit returns the source-located
`SPL_QUERY_TOO_COMPLEX` diagnostic before planning or execution. Unsupported
`dedup` arity is reported as `SPL_UNSUPPORTED_DEDUP_SYNTAX`. Dynamic field
paths align with ingestion's ceiling: 17 dotted segments and 256 unescaped
UTF-8 bytes per segment. Generated ClickHouse SQL is additionally capped at
256 KiB. Its longest generated `SELECT`/`UNION` dependency path is capped at
96 relational levels; independent sibling branches do not add to that path.
Every compiled `if` scalar node has a separate 64 KiB generated-SQL ceiling,
checked incrementally at each nested conditional before its fragments are
concatenated. This bounds repeated Dynamic-comparison expansion before the
whole-query ceiling is reached.
Every `coalesce` call accepts at most 32 values and has the same separate
64 KiB generated-SQL ceiling, checked incrementally while its variadic
arguments are compiled.
Every `case` call accepts one through 16 condition/value pairs and has the
same separate 64 KiB generated-SQL ceiling, checked incrementally while its
alternating predicates and values are compiled.
Every `lower` or `upper` call accepts exactly one value and has the same
separate 64 KiB generated-SQL ceiling, checked after its scalar or
multivalue lowering is built.
Every `len` or `length` call accepts exactly one value and has the same
separate 64 KiB generated-SQL ceiling, checked after its UTF-8 code-point
lowering is built.
Every `substr` call accepts one String value, one literal integer start, and
an optional literal integer length. It has the same separate 64 KiB
generated-SQL ceiling, checked after its SQLite-compatible UTF-8 interval
lowering is built.
Every `tostring` call accepts exactly one value and has the same separate
64 KiB generated-SQL ceiling, checked after its typed conversion lowering is
built. Formatted modes are a separate unsupported surface.
Every `round` call accepts one numeric value and an optional literal precision
from 0 through 18. It has the same separate 64 KiB generated-SQL ceiling,
checked after its typed numeric lowering is built.
Every `ceil`, `ceiling`, or `floor` call accepts exactly one numeric value and
has the same separate 64 KiB generated-SQL ceiling, checked after its typed
numeric lowering is built.
Every `mvcount` call accepts exactly one value and has the same separate
64 KiB generated-SQL ceiling, checked after its typed cardinality lowering is
built. Validation of a tagged scalar payload is independently capped at the
1 MiB hard ingestion ceiling.
Every `match` call accepts one value and one quoted literal regular expression.
Both the original and normalized RE2 text are capped at 4 KiB, the estimated
post-repeat program is capped at 4,096 work units, and one query may use at
most 16,384 total `match` work units. Calculated match inputs have a
conservative 4 MiB byte ceiling, and each call has the same separate 64 KiB
generated-SQL ceiling.
Every `like` call accepts one value and one quoted literal wildcard pattern.
Both the original and normalized pattern are capped at 4 KiB, one pattern is
capped at 4,096 wildcard/literal work units, and one query may use at most
16,384 total `like` work units. Calculated like inputs have a conservative
4 MiB per-call byte ceiling, and the conservative input bounds across all
`like` occurrences may total at most 16 MiB of wildcard scanning per row.
Each call has the same separate 64 KiB generated-SQL ceiling.
Every `now()` call accepts no arguments and returns the immutable search-start
Unix second as a fixed signed integer. It is bound from the server-resolved
search snapshot rather than evaluated from the ClickHouse wall clock.
Every `strftime` call accepts one canonical time or numeric Unix-seconds value
and one quoted literal format. The authored format is capped at 4 KiB, 4,096
literal-code-point/directive work units, and 16 KiB of conservative output.
Across one query, all calls may total at most 16,384 format work units and
64 KiB of conservative output per row. Each call has the same separate 64 KiB
generated-SQL ceiling.
Every `strptime` call accepts one String date value and one quoted literal
format. The authored format is independently capped at 4 KiB and 4,096
literal-code-point/directive work units. One input may contain at most 4 KiB
of UTF-8, and across one query all calls may total at most 16,384 format work
units and 64 KiB of date parsing per row. Each call has the same separate
64 KiB generated-SQL ceiling.
Exceeding any internal expansion budget returns the same diagnostic. The
executor also pins ClickHouse's independently measured `max_subquery_depth`
to 100 and applies a 1 MiB `max_query_size` ceiling after bound arguments are
expanded, in addition to its time, memory, scan, group, and result budgets.

One `dc` measure retains at most 100,000 exact distinct scalar spellings in
one group. The 100,001st distinct value fails the search atomically with an
execution-limit error; it is never truncated or approximated. ClickHouse's
per-query memory limit remains an independent bound on retained string bytes,
the sum of all groups, and multiple distinct-count measures. A whole-result
validation barrier runs before later pipeline commands, so `head`, filtering,
or projection cannot hide an overflowing group.

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
case-insensitive. Mathematically integral `decimal/v1` values inside signed
`Int256`, including fractional/exponent spellings and exact Decimal results
produced by `bin`, use the exact integer comparison path. Other extended
decimal values compare through finite `Float64`, so distinct values beyond
`Float64` precision can compare as equal in compatibility version 0.1.

### `where`

```spl
| where p95_ms > 500
| where status=500 OR duration_ms>500 AND level="ERROR"
| where isnull(optional) OR isnotnull(status)
| where match(message, "(?i)error|warn")
| where like(path, "/api/%")
| where _time<=now()
| where strftime(_time, "%Y")="2026"
| where strptime(received_at, "%F %T")>=0
```

`where` uses a separate eval-expression grammar: quoted strings are literals,
bare names are fields, comparisons are case-sensitive, implicit `AND` is not
accepted, and precedence is parentheses, `NOT`, `AND`, then `OR`. The current
slice supports Boolean combinations of scalar comparisons and direct
`isnull(value)` / `isnotnull(value)` predicates. Each informational function
accepts exactly one scalar expression, and can also be compared explicitly
with a Boolean literal. Scalar operands may be fields, typed literals, or the
supported `tonumber`, `replace`, bounded `if`, bounded `coalesce`, bounded
`case`, `lower`, `upper`, `len`, `length`, bounded `substr`, and bounded
default `tostring`, bounded `round`, `ceil`/`ceiling`, `floor`, and `mvcount`
calls, plus bounded `match(value, "regex")` and
`like(value, "pattern")`, zero-argument `now()`, and bounded
`strftime(time, "format")` and `strptime(value, "format")` described below;
arithmetic, field quoting, `XOR`, and other eval functions are not yet
accepted. Missing, null, container, or failed numeric operands do not pass
ordinary comparisons.

`isnull` is true when its input field is missing or its scalar result is null;
`isnotnull` is its exact complement. Empty strings, numeric zero, and false are
present, non-null values. A projected-away field is missing, and a failed
`tonumber` result is null. An empty fixed multivalue result, such as an empty
`stats values(...)` array, follows the existing logical-absence contract.
Open Splunk's typed Dynamic boundary treats an exact array as present even
when it is empty or contains only null members, and treats a flattened object
parent as present when descendant metadata exists. These predicates do not
walk or expand container members.

Dynamic values compare through their runtime scalar types: integer pairs retain
full 64-bit precision, numeric pairs compare numerically, and string pairs
compare lexically. Canonical `_time` and `_indextime` use Unix epoch seconds in
numeric comparisons. Mathematically integral extended decimals inside signed
`Int256` use the exact integer path; other extended decimals have the same
`Float64` precision caveat as base-search comparisons.

## Pipeline commands

### `eval`

```spl
| eval duration_ms=tonumber(replace(duration, "ms$", ""))
| eval label=if(isnull(optional), "missing", "present")
| eval score=if(status>=500, 1, 0)
| eval selected=coalesce(null, source, "unknown")
| eval class=case(status>=500, "server", status>=400, "client", 1=1, "other")
| eval normalized=lower(username), display=upper(normalized)
| eval characters=len(message)
| eval route=substr(path, 1, 32)
| eval rendered=tostring(status), flag=tostring(isnull(optional))
| eval latency_ms=round(duration_ms, 2)
| eval upper_bound=ceil(ratio), lower_bound=floor(ratio)
| eval recipient_count=mvcount(recipients)
| eval class=if(match(message, "(?i)error|warn"), "problem", "ok")
| eval route_class=if(like(path, "/api/%"), "api", "other")
| eval search_started=now()
| eval rendered_time=strftime(_time, "%Y-%m-%dT%H:%M:%S.%Q%:z")
| eval received_epoch=strptime(received_at, "%F %T.%6N")
```

Assignments are evaluated from left to right, and later assignments may use an
earlier output. Existing fields can be replaced without producing duplicate
columns. Literal assignments retain their `Int64`, `UInt64`, `Float64`, `Bool`,
or `String` output type. The current scalar-expression surface is deliberately
narrow:

- `replace(value, "regex", "replacement")` substitutes every match and
  requires literal regex/replacement arguments;
- `tonumber(value)` returns a nullable `Float64`; invalid, missing, null,
  non-string dynamic, multivalue, object, `NaN`, and infinite inputs become
  null;
- `if(predicate, true_value, false_value)` selects exactly one fixed scalar
  result. It requires exactly three arguments;
- `coalesce(value, ...)` selects the first non-null supported fixed value from
  one through 32 arguments;
- `case(predicate, value, ...)` selects the value paired with the first true
  predicate from one through 16 condition/value pairs;
- `lower(value)` and `upper(value)` map one String or multivalue String using
  Unicode-aware case conversion;
- `len(value)` and its `length(value)` alias count UTF-8 code points in one
  scalar String;
- `substr(value, start[, length])` selects a UTF-8 code-point interval using
  SQLite-compatible indexes and literal integer bounds;
- `tostring(value)` returns one fixed String from a scalar String, number, or
  Boolean. Formatted modes are not supported.
- `round(value[, precision])` rounds one numeric value with an optional literal
  precision from 0 through 18. The default precision is zero.
- `ceil(value)` and its `ceiling(value)` alias round one numeric value upward;
  `floor(value)` rounds one numeric value downward.
- `mvcount(value)` returns the number of immediate non-null multivalue members,
  one for a non-null scalar, or null when there are no values.
- `match(value, "regex")` returns a Boolean substring-match result for
  predicate and conditional use with a bounded literal RE2 pattern.
- `like(value, "pattern")` returns a Boolean whole-string wildcard-match result
  for predicate and conditional use with a bounded literal pattern.
- `now()` returns the fixed search-start Unix second as an `Int64`.
- `strftime(time, "format")` returns one fixed nullable String using the
  search's effective IANA timezone and a bounded literal format.
- `strptime(value, "format")` returns nullable `Float64` Unix seconds from one
  String date value using a bounded literal full-date format.

The `if` predicate uses exactly the `where` grammar described above:
case-sensitive scalar comparisons, direct `isnull` / `isnotnull` predicates,
parentheses, and explicit `NOT`, `AND`, and `OR` with `where` precedence.
An `if` whose result branches are syntactically known Boolean values—Boolean
literals, null predicates, or recursively recognized conditionals—is also
accepted as a direct predicate. The parser does not infer the runtime type of
an arbitrary field, so a fixed `Bool` field used as an `if` result branch does
not qualify for that direct-predicate form.
Implicit `AND`, base-search text predicates, and other eval functions or
operators remain unsupported. Every comparison or direct scalar predicate in
an `if` condition consumes the same query-wide budget of 32 eval/where
predicates. Conditional calls and their branch expressions share the existing
32-level scalar-expression nesting ceiling.

Only a condition that evaluates to Boolean true selects `true_value`. Boolean
false or null selects `false_value`; in particular, a comparison involving a
missing or null operand follows the false branch. Assignments still run from
left to right, so a later conditional in the same `eval` may inspect an earlier
assignment.

Branch types are resolved before execution. The supported fixed result types
are `String`, `Bool`, `UInt8`, `Int64`, `UInt64`, and `Float64`. Two non-null
branches must have the same type, including the exact numeric type; Open Splunk
does not ask ClickHouse to infer a `Variant` or a wider common numeric type. A
statically null branch adopts the other supported fixed type and makes the
result nullable. This also applies to a projected-away or declared-missing
fixed-schema input whose value is known to be null. When both branches are
statically null, the output is a nullable `String`. An `eval` destination is
present even when the selected value is explicit null.

Dynamic fields, fixed multivalue arrays, canonical time values, mixed result
kinds, and different numeric types are rejected with
`SPL_UNSUPPORTED_IF_BRANCH_TYPE`. Non-null String branches must also carry
identical text-eligibility provenance. A null branch adopts the other branch's
provenance, and `_raw` may be selected against itself, but mixing
binary-sensitive `_raw` with an ordinary String is rejected rather than
allowing a downstream text function to parse bytes as UTF-8.

Search-mode SPL1 does not directly assign the Boolean function result of
`isnull` or `isnotnull` with `eval`. The predicate position of `if` consumes
those results, so `eval label=if(isnull(value), "missing", "present")` is
supported. A Boolean-valued conditional is also supported when its result is
consumed as a predicate, such as
`where if(test=1, isnull(left), isnotnull(right))`. The Boolean-function result
does not become assignable merely because it passes through a result branch:
`eval flag=if(test=1, isnull(left), isnotnull(right))` remains rejected, as
does feeding that result to `tonumber` or `replace`. A conditional made from
plain Boolean literals, optionally with a null branch, is an ordinary fixed
`Bool` scalar and may be assigned.

The lowering is one scalar ClickHouse
`if(ifNull(predicate, 0), true_value, false_value)` per conditional; it does
not expand multivalue members or multiply input rows. A surrounding `where`
still applies its ordinary row filter. Production execution pins
`short_circuit_function_evaluation=enable`, so an unselected supported branch
is not evaluated for that row. ClickHouse still performs static type analysis
and may constant-fold constant subexpressions before row execution; the
short-circuit guarantee is a per-row runtime contract for the supported
expression surface, not a general lazy-language contract for future throwing
functions. Branch arguments remain bound data, and the compiler enforces the
64 KiB conditional-SQL ceiling at every node before constructing a larger
nested expression.

These fixed-type and text-provenance restrictions are deliberate version 0.1
compatibility boundaries. Splunk supports a broader eval-expression surface,
while its public documentation does not fully pin mixed-return coercion,
container behavior, or all evaluation-order edges. Open Splunk rejects those
cases instead of inheriting ClickHouse common-supertype or `Variant` behavior;
they require a live Splunk differential oracle before expansion.

`coalesce` examines its arguments from left to right and returns the first
supported fixed value that is not null. One argument is an identity operation.
An explicit null, a failed nullable conversion, or a statically missing
fixed-schema value is skipped. Empty String, numeric zero, and Boolean false
are values and stop selection. When every argument is null, the result is a
present nullable `String` null; as with every `eval` assignment, the
destination exists even when its value is null.

The supported non-null result types are the same stable fixed scalars as the
current `if` tier: `String`, `Bool`, `UInt8`, `Int64`, `UInt64`, and `Float64`.
Every non-null argument must have the same exact type, including the numeric
type. Statically null arguments adopt that type. Non-null String arguments
must also carry identical text-eligibility provenance, so
`coalesce(_raw, _raw)` and `coalesce(null, _raw)` are supported while
`coalesce(_raw, "fallback")` is rejected.

Dynamic event values, fixed multivalue arrays, canonical time values, mixed
result kinds, and different numeric types fail with
`SPL_UNSUPPORTED_COALESCE_VALUE_TYPE`. In particular, the compiler does not
ask ClickHouse to infer a common `Variant`, widen a numeric type, or
reconstruct a flattened object parent. This means the common Splunk form
`coalesce(field_a, field_b)` requires those inputs to have a fixed pipeline
type in compatibility version 0.1; arbitrary open-event Dynamic inputs are a
future typed-union slice.

A coalesce whose non-null arguments are syntactically known Boolean values may
be consumed directly by `where`, an `if` condition, or
`count(eval(...))`. Plain Boolean literals may still be assigned. A Boolean
`isnull`/`isnotnull` result does not become assignable or consumable by
`tonumber`/`replace` merely because it passes through coalesce, preserving the
search-mode Boolean boundary described above.

The compiler emits one scalar ClickHouse `coalesce(...)`, after normalizing
every statically null argument to the selected fixed type. Bindings remain in
source occurrence order, no argument text is interpolated into SQL, and the
operation neither expands multivalue members nor multiplies rows. The
left-to-right result rule does not promise lazy evaluation of future
side-effecting or throwing function arguments; the supported scalar surface is
pure, and ClickHouse may analyze or constant-fold expressions independently.
The per-call 64 KiB SQL ceiling is enforced before the final expression is
concatenated.

`case` accepts alternating predicate/value arguments:

```spl
| eval class=case(status>=500, "server", status>=400, "client")
| eval class=case(status>=500, "server", status>=400, "client", 1=1, "other")
| where case(isnull(optional), false, isnotnull(status), true)
```

Predicates are evaluated from left to right, and the value paired with the
first Boolean true predicate is selected. Boolean false or null continues to
the next pair. If no predicate is true, the result is a present null; there is
no separate default argument. A final always-true comparison such as `1=1`
provides an explicit default in the current grammar. Splunk commonly shows
`true()` for that purpose, but Open Splunk does not yet support `true()` as an
eval function and rejects it rather than silently changing the grammar.

Every predicate uses the exact current `where` grammar and precedence:
case-sensitive scalar comparisons, direct `isnull` / `isnotnull`, parentheses,
and explicit `NOT`, `AND`, and `OR`. Predicate leaves share the query-wide
32-leaf eval/where ceiling with `where`, `if`, other `case` calls, and
`count(eval(...))`. The complete call and all nested predicate/value
expressions share the 32-level scalar nesting ceiling. Zero arguments, an odd
number of arguments, a missing condition or value, and a seventeenth pair are
rejected before planning or execution.

Result typing uses the same conservative fixed-scalar contract as `if` and
`coalesce`: non-null values must have one identical type from `String`, `Bool`,
`UInt8`, `Int64`, `UInt64`, and `Float64`, including exact numeric type.
Statically null values and the implicit no-match default adopt that type. If
every value is statically null, the result is nullable `String`. A selected
empty String, numeric zero, or Boolean false remains a value rather than being
treated as null. Bindings inside a live statically null-producing expression
remain in source order; type normalization does not invent argument-elision
semantics.

Dynamic event values, fixed multivalue arrays, canonical time values, mixed
result kinds, differing numeric types, and incompatible String provenance fail
with `SPL_UNSUPPORTED_CASE_VALUE_TYPE`. The compiler never delegates those
cases to ClickHouse common-type or `Variant` inference. Matching `_raw`
provenance is accepted, while mixing binary-sensitive `_raw` with an ordinary
String is rejected.

A case whose non-null values are all syntactically known Boolean values can be
consumed directly by `where`, an `if` or `case` condition, or
`count(eval(...))`; its implicit no-match null is false at a predicate
boundary. Plain Boolean-literal values remain assignable. A Boolean
`isnull`/`isnotnull` result still cannot escape through a case value into a
direct `eval`, `tonumber`, or `replace` consumer.

The compiler emits one parameterized ClickHouse
`multiIf(ifNull(predicate, 0), value, ..., typed_null_default)`. Predicate and
value bindings retain alternating source order. Calculated Dynamic inputs are
materialized before predicate evaluation when required, and the scalar path
does not expand multivalue members or multiply rows. Production execution pins
`short_circuit_function_evaluation=enable`; ClickHouse may still perform
static type analysis and constant folding. The per-call 64 KiB SQL ceiling is
enforced incrementally before the final expression is concatenated, in
addition to the whole-query ceiling.

`lower` and `upper` each accept exactly one argument:

```spl
| eval normalized=lower(username)
| eval shouted=upper(normalized)
| where lower(source)="api"
```

Function names are case-insensitive. A bare field named `lower` or `upper`
remains an ordinary field; function parsing applies only when the name is
followed by parentheses. The argument must be a String-producing expression
or a multivalue String. A fixed numeric, Boolean, or canonical time argument
fails with `SPL_UNSUPPORTED_TEXT_CASE_VALUE_TYPE`; Open Splunk does not
silently apply the separate `tostring` conversion.

A fixed `String` produces a fixed `String`. A Dynamic runtime `String`
produces a Dynamic `String`. Missing, explicit null, and unsupported Dynamic
runtime values produce null. Unsupported runtime values include numbers,
Booleans, objects, and other containers rather than being converted through
ClickHouse's generic `toString`.

Multivalue input is mapped member by member while preserving member order and
cardinality. Fixed `Array(String)`, Dynamic `Array(String)`, and homogeneous
all-String `Array(Dynamic)` are supported; the latter is normalized to
`Array(String)`. A heterogeneous or null-containing Dynamic array produces
null. An invalid fixed String array becomes the canonical empty, logically
absent multivalue. No form uses `ARRAY JOIN`, expands an event into multiple
rows, or changes event cardinality.

The functions require valid UTF-8. Ingested typed Strings already satisfy that
boundary. Canonical `_raw` is eligible only when its stored encoding is UTF-8;
binary-declared raw bytes produce null even if those bytes happen to be ASCII.
`replace(_raw, ...)` preserves this provenance. The String normalization used
by `stats values(_raw)` and `stats list(_raw)` removes binary-declared inputs,
and fixed multivalue conversion independently validates every retained member
before applying a UTF-8 function.

Open Splunk lowers to ClickHouse `lowerUTF8` and `upperUTF8` on the pinned
ClickHouse `26.3.17.4` execution target. This is Unicode-aware case mapping,
not locale-aware collation, normalization, or full case folding; in
particular, callers must not infer Turkish-locale behavior. ClickHouse also
documents limitations when a code point's upper- or lowercase representation
changes encoded byte length. Compatibility expansion for locale-sensitive or
normalization-sensitive searches requires a separate differential contract.

The result of `lower` or `upper` still participates in `where`'s ordinary
case-sensitive String comparison. Thus `lower(value)="text"` and
`lower(value)="TEXT"` are distinct predicates. Dynamic text results use a
text-only comparison path that binds each operand once and does not generate
irrelevant numeric, decimal, or Boolean branches. Nested Dynamic and fixed
multivalue calls also bind each child expression once, so SQL growth is linear
until the per-call 64 KiB and whole-query 256 KiB ceilings apply.

`len` and `length` are equivalent names for one scalar String operation:

```spl
| eval characters=len(message)
| eval characters=length(message)
| where len(source)>3
```

The function returns the number of UTF-8 code points, not encoded bytes. An
empty String returns `0`. Fixed String input returns `UInt64`; Dynamic runtime
String input returns nullable `UInt64`. Missing, explicit null, and Dynamic
runtime numbers, Booleans, arrays, objects, or other containers return null.
A fixed numeric, Boolean, or canonical time argument fails with
`SPL_UNSUPPORTED_TEXT_LENGTH_VALUE_TYPE`.

Unlike `lower` and `upper`, Splunk does not support `len` on a multivalue
field. Open Splunk rejects a fixed `Array(String)` with
`SPL_UNSUPPORTED_MULTIVALUE_USAGE`; a Dynamic runtime array returns null.
Callers that need a member count require the separate, currently unsupported
`mvcount` contract.

The same valid-UTF-8 and `_raw` provenance boundary applies as for
`lower`/`upper`: binary-declared raw bytes return null, while typed Strings and
UTF-8-declared raw input are eligible. ClickHouse lowering uses `lengthUTF8`.
For Dynamic input, nullable `dynamicElement(value, 'String')` both selects the
only supported runtime type and returns null for every other type, so the
source expression appears once without a per-row singleton array or generic
Dynamic numeric branches. Nested text input is compiled linearly under the
per-call 64 KiB and whole-query 256 KiB SQL ceilings.

`substr` accepts a scalar String, a literal integer start, and an optional
literal integer length:

```spl
| eval prefix=substr(message, 1, 32)
| eval suffix=substr(source, -4)
| eval preceding=substr(source, 4, -2)
| where substr(path, 1, 4)="/api"
```

Function names are case-insensitive, and a bare field named `substr` remains
an ordinary field. Compatibility version 0.1 deliberately requires literal
integer start and length arguments. A field, nested expression, Boolean, or
floating-point index fails at its own source range with
`SPL_UNSUPPORTED_SUBSTRING_INDEX`; it is never truncated or coerced. Signed
64-bit integers and the complete non-negative unsigned 64-bit literal range
are accepted. Out-of-range syntax fails during logical planning with
`SPL_NUMBER_OUT_OF_RANGE`.

Indexes exactly follow the SQLite semantics referenced by Splunk. The
leftmost code point is position `1`; a negative start counts from the right.
Start `0` denotes the virtual position immediately before the first code
point, so `substr("abcdef", 0, 3)` is `"ab"`. Omitting length returns through
the end. A positive length selects that many positions beginning at start,
zero returns an empty String, and a negative length selects the absolute
number of positions immediately preceding start. The resulting half-open
interval is clipped to the real String. Consequently, overlong negative
starts can still select data when their positive length reaches the String,
while overlong positive starts normally return empty. Indexes count UTF-8
code points, not bytes.

Fixed String input produces a fixed String. Dynamic runtime String input
produces a nullable fixed String. Missing, explicit null, and Dynamic runtime
numbers, Booleans, arrays, objects, or other containers return null. Fixed
numeric, Boolean, or canonical time input fails with
`SPL_UNSUPPORTED_SUBSTRING_VALUE_TYPE`. Splunk does not support `substr` on a
multivalue field: fixed `Array(String)` fails with
`SPL_UNSUPPORTED_MULTIVALUE_USAGE`, while a Dynamic runtime array returns
null.

The same valid-UTF-8 and `_raw` provenance boundary applies as for the other
text functions. Binary-declared `_raw` returns null even when its bytes happen
to be ASCII, and `replace(_raw, ...)` retains that boundary. No lowering uses
`ARRAY JOIN` or changes event cardinality.

Open Splunk lowers statically equivalent literal intervals directly to
ClickHouse `substringUTF8`. Common positive starts, omitted lengths, zero
lengths, start `0`, and negative lengths with non-negative starts therefore
avoid a separate `lengthUTF8` scan and higher-order runtime work. A negative
start combined with an explicit non-zero length needs the row's code-point
count to preserve SQLite clipping. The same fallback is used when an unsigned
offset or length exceeds ClickHouse's empirically safe signed native argument
range; the pinned server otherwise reinterprets values such as `MaxUint64` as
negative. The bounded fallback binds the source and indexes once, computes in
`Int128`, and converts only already-clipped offsets to ClickHouse's accepted
integer types. This makes `MinInt64` and `MaxUint64` arguments overflow-safe.
Nested calls grow linearly under the per-call 64 KiB and whole-query 256 KiB
SQL ceilings.

`tostring` accepts exactly one value:

```spl
| eval status_text=tostring(status)
| eval flag=tostring(isnull(optional))
| where tostring(code)="500"
```

Function names are case-insensitive, and a bare field named `tostring`
remains an ordinary field. The default, unformatted conversion is the only
supported form in compatibility version 0.1. Splunk documents optional
`binary`, `hex`, `commas`, and `duration` numeric formats; any second argument
fails at the call with `SPL_UNSUPPORTED_TOSTRING_FORMAT`, regardless of its
value. Zero arguments or more than two arguments fail arity validation.

A fixed String is returned unchanged. Fixed `Int64`, `UInt64`, and `Float64`
values use their exact ClickHouse textual spelling. A Boolean becomes exactly
`"True"` or `"False"`, including a supported `isnull` or `isnotnull` result
that cannot otherwise escape directly into an `eval` assignment. Missing,
explicit-null, and statically null input returns null rather than a textual
sentinel. Canonical `_time` and `_indextime` are deliberately rejected with
`SPL_UNSUPPORTED_TOSTRING_VALUE_TYPE` until their timezone and precision
contract is pinned.

Dynamic runtime String, integer, floating-point, and Boolean values follow the
same conversion. The repository's exact `decimal/v1` extension preserves its
validated payload spelling, including trailing fractional zeroes, so it never
loses precision through `Float64`. Decimal envelopes must contain exactly the
type and value keys, have a payload of at most 4 KiB, and match the canonical
Decimal grammar. Malformed or oversized envelopes return null without
exposing their payload. Dynamic null, arrays, objects, and other tagged
containers also return null. Fixed `Array(String)` fails with
`SPL_UNSUPPORTED_MULTIVALUE_USAGE`; no form maps members, applies
`ARRAY JOIN`, or changes row cardinality.

String identity retains text-eligibility provenance. Thus
`tostring(_raw)` preserves the raw bytes even when they are declared binary,
while a later `lower`, `upper`, `len`, or `substr` still returns null instead
of treating those bytes as UTF-8. Dynamic text-only producers such as
`lower(field)` use direct nullable String extraction rather than repeating
the broader type dispatch. General Dynamic conversion binds its input once;
fixed nullable Boolean conversion uses one scalar `transform` call. Nested
calls grow linearly under the per-call 64 KiB and whole-query 256 KiB SQL
ceilings.

Splunk's public documentation specifies String/number/Boolean conversion and
capitalized Boolean output, but it does not fully pin null spelling, extended
Decimal input, canonical time precision, or every default numeric edge.
Those cases therefore remain the explicit conservative boundaries above
until a live Splunk differential oracle is available.

`round` accepts one numeric value and an optional precision:

```spl
| eval nearest=round(duration_ms)
| eval hundredths=round(duration_ms, 2)
| where round(score, 1)>=2.5
```

Function names are case-insensitive, and a bare field named `round` remains an
ordinary field. Precision defaults to zero and, in compatibility version 0.1,
must be a literal integer from `0` through `18`. Negative precision is rejected
because Splunk documents it as unsupported. A field, Boolean, null,
floating-point value, nested expression, or integer above `18` in the precision
position fails at that argument with `SPL_UNSUPPORTED_ROUND_PRECISION`; no
value is truncated or coerced. The upper bound also stays inside the pinned
ClickHouse target's accepted Float64 precision range.

Fixed `Int64` and `UInt64` input is an exact no-op for every supported
non-negative precision and retains its type, including `MaxUint64`. Fixed
`Float64` input returns `Float64`. Missing, explicit-null, and statically null
input returns nullable `Float64` null. Fixed String, Boolean, canonical time,
or other nonnumeric input fails with `SPL_UNSUPPORTED_ROUND_VALUE_TYPE`.
A fixed multivalue fails with `SPL_UNSUPPORTED_MULTIVALUE_USAGE`.

Dynamic physical signed and unsigned integer variants are returned exactly
with their original runtime type. A validated `decimal/v1` payload whose
mathematical value is integral and fits signed `Int256` becomes that exact
`Int256`; adjacent values such as `9007199254740992` and `9007199254740993`
therefore remain distinct. Other validated Decimal payloads, physical
floating-point variants, and physical Decimal variants convert to finite
`Float64` before rounding and return Dynamic `Float64`. A Decimal envelope
must contain exactly the type and value keys, have at most 4 KiB of payload,
and match the canonical Decimal grammar. Malformed, oversized, or non-finite
values return null without exposing their payload. Dynamic Strings, Booleans,
null, arrays, objects, and other containers also return null rather than being
coerced.

Float64 rounding uses the pinned ClickHouse `round` implementation: decimal
precision with binary-double input and halfway-to-even behavior. Representative
results are `round(3.5)=4`, `round(2.5)=2`, `round(-2.5)=-2`,
`round(2.555,2)=2.56`, `round(15.275,2)=15.28`, and
`round(17.275,2)=17.27`. The latter pair deliberately records binary
representation effects rather than promising decimal arithmetic. Splunk's
public documentation specifies the signature, non-negative precision, and
examples such as `3.5` and `2.555`, but does not fully specify every halfway
or binary-representation edge; these results remain the explicit version 0.1
boundary until a live differential oracle is available.

Dynamic lowering binds the source once. An explicit precision is a second
lambda input, so nested calls preserve inner-to-outer placeholder order.
After the first Dynamic `round`, a numeric-only domain marker lets nested calls
and predicates omit String, Boolean, and tagged-envelope dispatch. Atomic
field/literal comparisons stay scalar; compound comparisons bind each operand
once to prevent repeated evaluation and superlinear SQL. No form applies
`ARRAY JOIN`, expands a multivalue, or changes row cardinality. Nested calls
grow linearly under the per-call 64 KiB and whole-query 256 KiB SQL ceilings.

`ceil`/`ceiling` and `floor` each accept exactly one numeric value:

```spl
| eval next_integer=ceil(duration_ms)
| eval next_integer=ceiling(duration_ms)
| eval prior_integer=floor(duration_ms)
| where floor(score)>=2
```

Function names are case-insensitive. `ceil` and `ceiling` are exact aliases;
bare fields named `ceil`, `ceiling`, or `floor` remain ordinary fields.
Zero or multiple arguments fail with `SPL_INVALID_EVAL_ARITY`. A Boolean
null-predicate result is not implicitly numeric and fails with
`SPL_UNSUPPORTED_EVAL_EXPRESSION`.

Fixed `Int64` and `UInt64` input is an exact no-op and retains its type,
including `MaxUint64`. Fixed `Float64` input returns `Float64`. Missing,
explicit-null, projected-away, and statically null input returns nullable
`Float64` null. Fixed String, Boolean, canonical time, or other nonnumeric
input fails with `SPL_UNSUPPORTED_CEIL_VALUE_TYPE` or
`SPL_UNSUPPORTED_FLOOR_VALUE_TYPE`. A fixed multivalue fails with
`SPL_UNSUPPORTED_MULTIVALUE_USAGE`.

Dynamic physical signed and unsigned integer variants retain their exact value
and runtime type. A validated integral `decimal/v1` payload inside signed
`Int256` becomes exact `Int256`, so adjacent integers above `2^53` remain
distinct. Other validated Decimal payloads, physical floating-point variants,
and physical Decimal variants convert to finite `Float64` before applying the
function and return Dynamic `Float64`. The same 4 KiB Decimal-envelope grammar
and exact-key validation as `round` applies. Malformed or non-finite Decimals,
Strings, Booleans, null, arrays, objects, and other containers return null.

Float64 behavior is pinned to ClickHouse `ceil` and `floor`:
`ceil(1.2)=2`, `ceil(-1.2)=-1`, `floor(1.2)=1`, and
`floor(-1.2)=-2`. `ceil(-0.2)` preserves the negative-zero sign bit. Each
Dynamic source is bound once, numeric-only results let predicates skip
text/container dispatch, and atomic comparisons remain scalar. Because every
successful result is already integral, any outer `ceil` or `floor` is an
identity and is removed during compilation, including across eval projection
and rename stages. No form applies `ARRAY JOIN`, expands multivalue members,
or changes row cardinality.

`mvcount` accepts exactly one value:

```spl
| eval recipient_count=mvcount(recipients)
| where mvcount(errors)>1
| eval present_count=mvcount(isnotnull(status))
```

Function names are case-insensitive, and a bare field named `mvcount` remains
an ordinary field. Zero or multiple arguments fail with
`SPL_INVALID_EVAL_ARITY`. Unlike text and numeric functions, `mvcount` accepts
a Boolean null-predicate result because Boolean false is still one scalar
value.

A non-null fixed String, number, Boolean, or canonical time has count one.
Missing, explicit-null, projected-away, and statically null input returns
nullable `UInt64` null. A fixed `Array(String)` returns its member count, while
an empty fixed array returns null. The result is always an integral
`UInt64` when present.

For a runtime `Array(Dynamic)`, Open Splunk counts immediate members whose
runtime type is not `None`. Empty and all-null arrays therefore return null.
Immediate nested arrays and objects are each counted atomically when non-null;
members are never walked or expanded. Other physical Dynamic array variants
use their immediate physical length and return null when empty. A Dynamic
String, finite or non-finite physical number, or Boolean has count one.
Validated `bytes/v1`, `timestamp/v1`, `duration/v1`, and `decimal/v1`
envelopes also have count one. Each envelope must contain exactly the type and
value keys and pass its tag-specific payload grammar. Malformed envelopes,
unknown maps, ordinary objects, flattened object parents, Dynamic null, and
missing fields return null without exposing stored payloads.

This scalar and no-value behavior follows Splunk's documented `mvcount`
contract. Splunk's public documentation does not settle typed null members or
nested typed containers; immediate non-null member counting and atomic nested
containers are the explicit Open Splunk v0.1 typed-data boundary.

Dynamic sources are bound once. A metadata-authoritative missing-field guard
runs before the binding, numeric-only calculated inputs skip a redundant
lambda allocation, and tagged-payload regular-expression work is limited to
1 MiB. Repeated `mvcount(mvcount(...))` calls collapse to one cardinality
operation because the first result is already one or null, including across
eval projection and rename stages. No form uses `ARRAY JOIN` or changes row
cardinality. The per-call 64 KiB and whole-query 256 KiB generated-SQL ceilings
remain authoritative.

`match` accepts one value and one quoted literal regular expression:

```spl
| where match(message, "(?i)error|warn")
| where NOT match(path, "^/health(?:/|$)")
| eval class=if(match(message, "timeout"), "slow", "other")
| eval rendered=tostring(match(source, "^/api"))
```

Function names are case-insensitive, and a bare field named `match` remains an
ordinary field. Any arity other than two fails with
`SPL_INVALID_EVAL_ARITY`. The pattern cannot be a field or calculated value.
Both its authored UTF-8 bytes and normalized RE2 text are limited to 4 KiB.
Before any counted repetition is expanded, the compact syntax tree is
conservatively limited to 4,096 estimated RE2 program work units. A query may
use at most 16,384 total work units across all `match` occurrences. Invalid
UTF-8, embedded NUL, PCRE constructs unavailable in RE2 such as lookaround and
pattern backreferences, or any byte/program limit violation fails before
execution. Resource overflow uses `SPL_QUERY_TOO_COMPLEX`; unsupported regex
syntax uses `SPL_UNSUPPORTED_REGEX`.

Matching is case-sensitive by default and succeeds when the expression finds
any substring. Anchors request a full or boundary match. Empty and zero-width
patterns are accepted because they do not have replacement's global
zero-width ambiguity. Inline RE2 flags such as `(?i)` are accepted.
Open Splunk explicitly disables ClickHouse's default dot-all mode so `.` does
not match a newline, matching ordinary Splunk PCRE behavior; a caller can opt
back in with `(?s)`. Non-multiline `$` preserves PCRE's match at strict end or
immediately before one final newline; `\z` remains strict end, and `(?m)$`
retains line-end behavior.

A fixed String is matched directly. Fixed numeric, Boolean, and canonical-time
scalars use their supported text spelling before matching. A Dynamic runtime
String is matched; Dynamic numbers, Booleans, arrays, objects, tagged values,
null, and missing fields produce nullable Boolean null. Fixed
`Array(String)` fails with `SPL_UNSUPPORTED_MULTIVALUE_USAGE`. Binary `_raw`
and other values carrying failed text-eligibility provenance also produce
null. In predicate position, null behaves as false through the ordinary
`where`/conditional predicate contract.

`match` is a direct predicate and may be compared explicitly with a Boolean,
used under `NOT`/`AND`/`OR`, or consumed by `if`, `case`, `mvcount`, and
default `tostring`. Compatibility version 0.1 retains the established
search-mode boundary that a Boolean-returning function cannot be assigned
directly by `eval`; use a conditional or `tostring` when a stored field is
needed.

The compiler validates the literal again at its trust boundary, binds the
normalized pattern as a query argument, and references the input once. No form
expands rows. Each call has a 64 KiB generated-SQL ceiling in addition to the
256 KiB whole-query ceiling. Stored inputs fit within the 1 MiB ingestion
ceiling. String-size metadata is propagated through supported scalar
composition; UTF-8 case conversion allows a conservative 4x expansion, while
always-consuming `replace` uses a saturating input/replacement bound. A
calculated value that may exceed 4 MiB is rejected before regex execution.
Splunk executes PCRE while Open Splunk executes the explicitly validated RE2
subset through ClickHouse; dialect differences remain a documented
compatibility boundary.

`like` accepts one value and one quoted literal wildcard pattern:

```spl
| where like(path, "/api/%")
| where NOT like(filename, "%.tmp")
| eval class=if(like(message, "%ERROR%"), "problem", "ok")
| eval rendered=tostring(like(status, "5__"))
```

Function names are case-insensitive, and a bare field named `like` remains an
ordinary field. Any arity other than two fails with
`SPL_INVALID_EVAL_ARITY`. The pattern cannot be a field or calculated value.
Both its authored and normalized UTF-8 text are limited to 4 KiB. One pattern
is limited to 4,096 wildcard/literal work units, and one query is limited to
16,384 total work units across all `like` occurrences. Invalid UTF-8, an
embedded NUL, or an unpaired terminal backslash escape fails with
`SPL_UNSUPPORTED_LIKE_PATTERN`; a byte or work limit fails with
`SPL_QUERY_TOO_COMPLEX`. Independently, the conservative input byte bounds
across all occurrences may total at most 16 MiB of wildcard scanning per row,
so many individually cheap short patterns cannot multiply a large input scan
without limit.

Matching is case-sensitive and covers the complete input String. `%` matches
zero or more Unicode code points, including newlines; `_` matches exactly one
Unicode code point. An empty pattern therefore matches only an empty input,
while `%` matches every non-null String. No Unicode normalization or case
folding is implicit. In the decoded SPL string, backslash escapes a literal
`%`, `_`, or backslash. Before any other character, backslash is itself a
literal character. These escape, Unicode, and newline edges are pinned against
ClickHouse 26.3.17.4. Splunk's public documentation specifies whole-string
matching and the `%`/`_` wildcards but does not fully specify those edges, so
they are an explicit Open Splunk v0.1 boundary pending a live differential
oracle.

A fixed String is matched directly. Fixed numeric, Boolean, and canonical-time
scalars use their supported text spelling before matching. A Dynamic runtime
String is matched; Dynamic numbers, Booleans, arrays, objects, tagged values,
null, and missing fields produce nullable Boolean null. Fixed
`Array(String)` fails with `SPL_UNSUPPORTED_MULTIVALUE_USAGE`. Binary `_raw`
and other values carrying failed text-eligibility provenance also produce
null. In predicate position, null behaves as false through the ordinary
`where`/conditional predicate contract.

`like` is a direct predicate and may be compared explicitly with a Boolean,
used under `NOT`/`AND`/`OR`, or consumed by `if`, `case`, `mvcount`, and
default `tostring`. Compatibility version 0.1 retains the established
search-mode boundary that a Boolean-returning function cannot be assigned
directly by `eval`; use a conditional or `tostring` when a stored field is
needed.

The parser, planner, and compiler validate the literal independently.
Normalization collapses adjacent unescaped `%` wildcards without expanding
the pattern. The compiler binds that normalized pattern as a query argument,
references the input once, and lowers directly to ClickHouse `like()` rather
than generating or executing a compiler-side regular expression. ClickHouse
optimizes common `%literal%` patterns to substring search. No form expands
rows. Each call has a 64 KiB generated-SQL ceiling in addition to the 256 KiB
whole-query ceiling. Stored inputs fit within the 1 MiB ingestion ceiling;
calculated input that may exceed 4 MiB is rejected before wildcard execution.
That size metadata remains conservative when `rex` or `spath` can retain a
prior destination on a miss, and through `stats BY`, `min`, `max`, `earliest`,
and `latest` when those commands preserve an input value.

`now()` accepts no arguments:

```spl
| eval search_started=now()
| where _time<=now()
| eval rendered=tostring(now())
| eval state=if(now()>0, "started", "invalid")
```

Function names are case-insensitive, and a bare field named `now` remains an
ordinary field. Any argument fails with `SPL_INVALID_EVAL_ARITY`. The result is
a present, non-null fixed `Int64` containing the whole Unix second at which the
ad-hoc search was admitted. Every occurrence in one search returns the same
value. Subsecond precision is deliberately truncated rather than rounded.

The anchor is the explicit immutable server-resolved search-admission timestamp
captured beside, but independently from, the search's index-time and storage
visibility cutoffs. Repeated execution for field analysis or export preserves
that timestamp in the completed execution snapshot and therefore reproduces the
original value; a delayed ClickHouse query cannot move it forward. The compiler
binds the signed integer as an ordinary query argument and never emits
ClickHouse `now()` or `now64()`. Projection, aggregation, and later eval stages
retain the same shared compile context.

`now()` is a numeric scalar and can be assigned directly, compared in `where`,
or consumed by the supported conditional, rounding, and conversion functions.
It has no field-dependent Dynamic, null, multivalue, text-provenance, or
cardinality behavior. General scalar depth, node, token, argument, generated
SQL, and whole-query ceilings remain authoritative for repeated composition.

Open Splunk v0.1 has no scheduled-search execution surface, so Splunk's
scheduled-time variant is not claimed. The per-event wall-clock `time()`
function is also unsupported rather than being approximated with `now()`.
`relative_time` remains a separate planned slice.

`strftime` accepts one time value and one quoted literal format:

```spl
| eval rendered=strftime(_time, "%Y-%m-%dT%H:%M:%S.%Q%:z")
| eval admitted=strftime(now(), "%F %T")
| eval epoch=strftime(0, "%F %T.%9N %s %z")
| where strftime(_time, "%Y-%m")="2026-07"
```

Function names are case-insensitive, and a bare field named `strftime`
remains an ordinary field. Any arity other than two fails with
`SPL_INVALID_EVAL_ARITY`. The format must be a quoted String literal; a field,
calculated value, malformed directive, invalid UTF-8, or embedded NUL fails
before execution. A Boolean-producing first argument is rejected as an
unsupported scalar consumer.

Canonical `_time` and `_indextime` values preserve `DateTime64(9)` precision.
Fixed numeric input is interpreted as Unix seconds; a fractional value is
floored to nanoseconds, so `-0.5` is
`1969-12-31 23:59:59.500000000`, and `%s` returns `-1`. The immutable numeric
result of `now()` composes directly. Statically null input returns null.
Fixed String, Boolean, and multivalue inputs are rejected. For a Dynamic
field, only a finite runtime number inside the supported `DateTime64(9)` range
is formatted; String, Boolean, array, object, tagged, null, missing, non-finite,
and out-of-range values return null. Numeric conversion does not parse a
Dynamic String.

Formatting uses the effective IANA timezone retained with the search's
time-range intent. An omitted timezone means UTC. `Local`, host-specific
POSIX/leap-second zone names, malformed names, and unknown IANA names fail at
job admission or the planner trust boundary. The effective name is preserved
in completed execution snapshots and in field-summary/cache fingerprints, so
field analysis, timelines, exports, and cursor replay cannot silently switch
zones. Daylight-saving offsets follow that named zone at the formatted
instant. `%s` remains the instant's timezone-independent Unix second.

The supported locale-stable directive subset is:

| Group | Variables |
| --- | --- |
| literal | `%%` |
| year and ISO week | `%Y`, `%y`, `%G`, `%g`, `%V` |
| month and day | `%m`, `%b`, `%B`, `%d`, `%e`, `%j` |
| weekday | `%w`, `%a`, `%A` |
| clock | `%H`, `%I`, `%M`, `%S`, `%p`, `%T` |
| composed date | `%F` |
| epoch and zone offset | `%s`, `%z`, `%:z` |
| fractional second | `%Q`, `%3Q`, `%6Q`, `%9Q`, `%N`, `%3N`, `%6N`, `%9N`, `%f` |

`%G`/`%g` are the ISO week-based year rather than the calendar year; for
example, 2021-01-01 formats as ISO year 2020, week 53. `%w` numbers Sunday as
zero. `%Q` defaults to three fractional digits, `%N` defaults to nine, and
`%f` emits six. Explicit fractional widths are limited to 3, 6, or 9 digits
and are zero-padded/truncated from the nanosecond timestamp without rounding.
`%z` emits `+hhmm` and `%:z` emits `+hh:mm`. Month/weekday names and AM/PM use
the pinned English spellings; no process locale is consulted.

Locale-dependent variables and unimplemented portability edges are rejected
rather than delegated to ClickHouse or server configuration. This includes
`%c`, `%+`, `%x`, `%X`, `%Z`, `%U`, `%k`, `%C`, `%Ez`, `%::z`, `%:::z`,
unknown variables, a dangling `%`, and unsupported precisions such as `%2Q`,
`%4N`, `%0Q`, or `%12Q`.

One authored format may contain at most 4 KiB of UTF-8, 4,096 literal Unicode
code points plus directives, and 16 KiB of conservative output. One query may
total at most 16,384 work units and 64 KiB of conservative `strftime` output
per row across all occurrences. Resource overflow uses
`SPL_QUERY_TOO_COMPLEX`; unsupported format syntax uses
`SPL_UNSUPPORTED_TIME_FORMAT`. Each scalar lowering also has a 64 KiB
generated-SQL ceiling beneath the 256 KiB whole-query ceiling.

The parser, planner, and compiler independently revalidate the format. The
compiler binds the format fragments and timezone as query arguments, evaluates
the input once, localizes the timestamp once, and does not expand rows. The
pinned ClickHouse 26.3.17.4 integration corpus covers every supported
directive, UTC and daylight-saving offsets, the ISO year boundary, pre-epoch
flooring, nanoseconds, literal Unicode/apostrophes/percent, fixed and Dynamic
types, predicates, projection, aggregation, later eval, and snapshot replay.
Open Splunk uses custom lowering where ClickHouse's native percent/Joda tokens
do not match this contract, including `%s`, `%g`, `%e`, `%w`, and zone offsets.

`strptime` accepts one String date value and one quoted literal format:

```spl
| eval epoch=strptime(received_at, "%F %T.%6N")
| eval midnight=strptime("1971-01-01", "%F")
| eval offset_epoch=strptime("2026-07-27 19:20:21-0730", "%F %T%z")
| where strptime(received_at, "%F %T")>=0
```

Function names are case-insensitive, and a bare field named `strptime`
remains an ordinary field. Any arity other than two fails with
`SPL_INVALID_EVAL_ARITY`. The format must be a quoted String literal; a field,
calculated value, malformed directive, invalid UTF-8, or embedded NUL fails
before execution. A Boolean-producing first argument is rejected as an
unsupported scalar consumer.

The parsing subset requires exactly one complete numeric calendar date. It
accepts:

| Group | Variables |
| --- | --- |
| literal | `%%` |
| complete date | `%Y`, `%m`, `%d`, `%F` |
| clock | `%H`, `%I`, `%M`, `%S`, `%p`, `%T` |
| numeric zone offset | `%z` |
| fractional second | `%Q`, `%3Q`, `%6Q`, `%3N`, `%6N`, `%f` |

`%Y`, `%m`, and `%d` must all occur once, either separately or through `%F`.
A date alone is valid. Clock fields must form a hierarchy: minute requires an
hour, second requires minute, fractional second requires second, and `%p`
requires the 12-hour `%I` form. `%T` supplies `%H:%M:%S`. Duplicate or
ambiguous fields are rejected. Month or weekday names, locale-dependent forms,
two-digit years, day-of-year and ISO-week dates, epoch-seconds input, timezone
names, colon offsets, nine-digit fractions, unknown variables, and a dangling
`%` are unsupported rather than delegated to backend defaults.

Numeric month, day, hour, minute, and second fields accept one or two digits;
the year requires four. `%z` accepts the compact `+hhmm` or `-hhmm` spelling.
`%p` accepts case-insensitive `AM` or `PM`.
`%Q` defaults to milliseconds. Explicit `%Q`/`%N` widths are three or six
digits, while `%f` is the six-digit microsecond alias. A fractional field
accepts one through its declared width, with the value interpreted at that
unit scale. When a format ends in a literal dot followed by `%Q`, `%3Q`,
`%6Q`, `%3N`, or `%6N`, the complete `.subseconds` suffix is optional, matching
Splunk's enhanced `strptime` rule. `%f` remains an exact required field.

A fixed String is parsed directly. A Dynamic runtime String is parsed;
Dynamic numbers, Booleans, lists, objects, tagged values, null, and missing
fields return null. Statically null input returns a typed numeric null. Fixed
numeric, Boolean, canonical-time, and multivalue values are rejected rather
than stringified or expanded. Invalid calendar values, mismatched/trailing
text, unsupported dates, and overlong runtime input return null without
throwing.

The authored civil date must be from 1971-01-01 through 2299-12-31 inclusive.
The lower bound follows Splunk's documented `strptime` date rule; the upper
bound is the portable calendar limit of the pinned ClickHouse `DateTime64(6)`
parser; the resulting offset-adjusted instant must also remain representable.
The authored year/month/day are checked before timezone conversion,
so `1971-01-01 00:00:00+1400` is supported even though its resulting instant
is in 1970, while `1970-12-31 23:30:00-1200` remains unsupported.

An input without `%z` is interpreted in the search's effective IANA timezone;
an explicit numeric offset takes precedence. An omitted search timezone means
UTC, and the same admission, snapshot, replay, and invalid-zone rules as
`strftime` apply. Pinned ClickHouse behavior normalizes a daylight-saving gap
forward and chooses the earlier occurrence of an ambiguous fall-back fold.
Those choices are part of compatibility version 0.1 pending a live Splunk
differential oracle.

The result is a nullable `Float64` Unix timestamp in seconds. Parsing retains
microseconds before conversion, and ordinary dates expose six fractional
digits. As with the rest of the v0.1 numeric model, the published value is a
binary `Float64`, not an exact decimal: sufficiently late in the supported
range, adjacent microseconds can map to the same numeric value. Callers that
need exact textual microseconds should retain the source String.

One authored format may contain at most 4 KiB of UTF-8 and 4,096 literal
Unicode code points plus directives. One runtime input may contain at most
4 KiB; a longer value returns null. One query may total at most 16,384 format
work units and 64 KiB of date parsing per row across all occurrences.
Resource overflow uses `SPL_QUERY_TOO_COMPLEX`; unsupported format syntax uses
`SPL_UNSUPPORTED_TIME_FORMAT`. Each scalar lowering also has a 64 KiB
generated-SQL ceiling beneath the 256 KiB whole-query ceiling.

The parser, planner, and compiler independently revalidate the format. The
compiler binds the input-shape regular expression, Joda parser pattern, and
timezone as query arguments. It references the input once, extracts the
authored date once, and executes exactly one parser per value. A format with
the optional terminal fractional suffix binds both the primary and fallback
patterns, but the extracted suffix capture selects one before parsing. No form
expands rows. The pinned ClickHouse 26.3.17.4 integration
corpus covers fixed and Dynamic types, valid and invalid dates, trailing and
overlong input, optional fractions, millisecond and microsecond values,
12-hour time, compact offsets, the civil-date boundaries, UTC and IANA zones,
daylight-saving gaps/folds, predicates, projection, aggregation, and later
eval.

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

### `spath`

```spl
| spath path=request.context.trace_id
| spath input=payload output=status path=response.status
| spath input=payload output=first_sku path=items{0}.sku
| spath output=server_name server.name
```

Version 0.1 supports row-preserving extraction of one typed scalar from one
explicit, constant JSON path:

```text
spath [input=<exact field>] [output=<exact field>]
      (path=<datapath> | <datapath>)
```

Option names are case-insensitive, may appear in any order, and may each occur
once. Field and path values may be quoted or unquoted. The input defaults to
`_raw`; the output defaults to the decoded path spelling. A path is always
required. Omitting it is rejected rather than silently entering Splunk's
runtime-schema-generating auto-extract mode.

A supported path contains one through 17 case-sensitive object steps separated
by dots. Each step has a nonempty UTF-8 key of at most 256 bytes and may end in
one fixed array selector. At most four steps may have selectors:

```text
step  := key [ "{" index "}" ]
path  := step ("." step)*
index := "0" | [1-9][0-9]*
```

The complete decoded path is limited to 4 KiB. Dots delimit steps; literal-dot
escapes, backslashes, braces inside keys, control characters, `*`, the `{}`
array wildcard, XML `{@attribute}` selectors, negative indexes, root-array
selectors, and repeated selectors such as `a{0}{1}` are unsupported. Fixed
array indexes are zero-based, matching Splunk; the compiler translates them to
ClickHouse's one-based extraction arguments. The maximum accepted index is
`2^31-2`; the pinned server wraps larger integer path arguments rather than
reliably treating them as out of range.

The default output spelling is validated independently as an Open Splunk field
name. In particular, adding an index suffix to a 256-byte key can exceed the
256-byte field-segment ceiling even though the JSON path itself is valid; use a
shorter explicit `output` in that case.

The input must be a present, non-null, valid-UTF-8 String containing one
well-formed JSON document. Missing, null, non-string, binary, malformed, or
projected-away inputs produce no extraction for that row. A missing member, an
out-of-range array index, or a path step applied to the wrong JSON container
also produces no extraction. The latter is an explicit conservative Open
Splunk behavior; Splunk's documentation says an inappropriate array selector
is erroneous but does not define row-versus-job failure precisely.

On a successful extraction, JSON String, Boolean, null, `Int64`, and `UInt64`
leaves retain those types. Explicit JSON null is present but null, so it remains
distinct from a missing path. Fractional or exponent-form numbers produce a
sanitized unsupported-value error in this first slice rather than being
silently rounded to `Float64`; exact Decimal parity with ingestion is reserved
for a later slice. A terminal array or object likewise produces that error
instead of being stringified or exposed as a partially supported container.
JSON documents containing numbers outside ClickHouse's bounded
`Int64`/`UInt64` parser domain may be treated as malformed by the pinned server,
even when the selected member is unrelated. This is documented as an
unverified compatibility boundary, not Splunk parity.

A successful extraction replaces an existing destination, including replacing
it with explicit null. When extraction does not match, an existing destination
retains its exact value, semantic type, and sparse presence; a destination that
did not exist remains missing. Reading and writing the same field reads the
pre-command input value. The command preserves row count, event identity, and
established ordering. Its typed output participates in downstream `search`,
`where`, `sort`, `stats`, `dedup`, `bin`, projections, field discovery, field
summaries, and timelines under their existing value contracts.

One source String is limited to 1 MiB per row. An oversized calculated input
produces a sanitized execution-limit error rather than exposing payload bytes.
`rex` captures and `spath` destinations share a limit of 64 calculated
extraction outputs per query. Each stage binds every user path component as a
ClickHouse argument; no path text is interpolated into SQL. It performs one
terminal type inspection, one raw-value extraction, one typed-leaf decode, and
one additional array-container inspection per fixed selector. The
four-selector ceiling therefore bounds a stage to at most seven JSON parser
invocations. Across all `spath` stages, a query may use at most 32 of these
evaluation work units per row; the planner and compiler independently enforce
that cumulative ceiling.

The input-size and unsupported-value errors are part of evaluating a live
destination. A destination is live when it is returned, consumed downstream,
used by field analysis, or needed as the prior-value fallback of a later
extraction. If an exact later projection or unconditional overwrite makes the
destination provably unobservable, the pinned ClickHouse optimizer may prune
the extraction and no error is required. This is a value-realization boundary,
not a promise of compiler-level dead-code elimination.

On an open event schema, `fields` is the reserved whole-payload convenience
column and cannot be the input or output; a prior closed transforming schema
may declare an ordinary field with that spelling. After any successful-capable
`spath` stage, the immutable convenience payload is omitted because it cannot
represent sparse calculated overwrites. An output named `index` changes only
pipeline data and cannot widen the resolved physical index scope. An output
named `_time` replaces canonical-time provenance and makes the pipeline
ineligible for `timechart` and completed-job timeline analysis.

The following remain explicit future slices: auto-extract and its configurable
5,000-character discovery cutoff, `{}` and multivalue output, XML, container
output, dynamic paths supplied by another field, and the `spath()` eval
function. Duplicate JSON member selection follows the pinned ClickHouse parser's
first-member behavior; Splunk's public documentation does not define that edge,
so it must remain pinned by integration coverage.

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
| stats count(productId) AS products BY host
| stats c(productId) AS products BY host
| stats count(eval(status>=500)) AS errors BY service
| stats dc(user) AS unique_users BY service
| stats distinct_count(device) AS devices
| stats values(user) AS users
| stats list(user) AS ordered_users
| stats min(duration_ms) AS fastest max(duration_ms) AS slowest BY path
| stats earliest(status) AS first_status latest(status) AS last_status BY service
| stats p50(duration_ms) AS median_ms p90(duration_ms) p95(duration_ms) p99(duration_ms) BY path
| stats perc42(duration_ms) AS answer
| stats sum(bytes) AS total_bytes avg(duration_ms) AS mean_ms BY path
```

Argument-free `count`, `count(field)`/`c(field)`,
`count(eval(predicate)) AS output`,
`dc(field)`/`distinct_count(field)`,
`pN(field)`/`percN(field)` for integer `N` from 1 through 99,
`values(field)`, `list(field)`, `sum(field)`, `avg(field)`,
`min(field)`, `max(field)`, `earliest(field)`, and `latest(field)` are
supported, including multiple space- or comma-separated measures and `AS`
aliases. Function names are
case-insensitive. Both
distinct-count spellings use the canonical default output `dc(field)`;
`count(field)` and `c(field)` both use the explicit Open Splunk canonical
default output `count(field)`. Other default names use canonical lowercase
spelling such as
`values(user)`, `list(user)`, `min(duration_ms)`, `earliest(status)`, or
`sum(bytes)`. Percentiles use the Splunk-compatible canonical default name
`percN(field)` regardless of whether the query spells the function `pN` or
`percN`; for example, `p95(duration_ms)` publishes `perc95(duration_ms)`.
The command is transforming: output contains only the `BY` fields followed by
measures in source order. Argument-free `count` includes every input row in a
retained group.

`count(eval(predicate))` is the bounded conditional-count form. Version 0.1
requires the exact shape
`count(eval(<where predicate>)) AS <exact unquoted output field>`; both
function names are case-insensitive. The predicate uses the same
case-sensitive eval grammar and precedence as `where` and `if`: scalar
comparisons, direct `isnull`/`isnotnull`, bounded Boolean-valued `if`,
parentheses, and explicit `NOT`, `AND`, and `OR`. Its atomic predicates share
the query-wide ceiling of 32, and the measure shares the ordinary 16-measure
ceiling.

Each input row contributes one only when the predicate is Boolean true. False
or null contributes zero. An ordinary comparison involving a missing or null
operand is null and therefore contributes zero; `isnull(missing)` is true,
while `isnotnull(missing)` is false. The condition is local to that measure:
it never becomes an aggregate `WHERE`, so sibling measures and `BY` grouping
observe every scoped input row. A retained group with no true result publishes
zero. Global empty input publishes one non-null `UInt64` zero, while grouped
empty input publishes no groups.

Repeated conditional measures share one per-row contribution only when their
compiled predicate and bound arguments are exactly identical. Contributions
are non-null `UInt64`, aggregate through `UInt128`, and publish `UInt64` under
the production row ceiling. Ordinary predicates do not expand rows. A
predicate over a calculated field that requires the existing ClickHouse
optimizer fence uses one materialized CTE with singleton bindings shared by
all conditional measures; the singleton operation preserves cardinality.
Projected-away fields stay missing rather than being recovered from hidden
event data.

The explicit `AS` is required because Splunk's conditional-count guidance
requires an alias and does not define a stable default expression-derived
field name. `c(eval(...))`, an omitted alias, `match`, `like`, `XOR`,
arbitrary scalar truthiness, wildcard count, quoted names, and other eval
functions remain explicit version 0.1 failures. Splunk supports a broader eval
expression surface; Open Splunk does not silently reinterpret those forms.

`count(field)` counts immediate, non-null field occurrences without
stringifying values or expanding event rows:

- a missing field, explicit null, empty multivalue, or null multivalue member
  contributes zero;
- every other scalar contributes one, including an empty String, zero,
  `false`, Bytes, timestamp, duration, and decimal values;
- every immediate non-null member of a top-level multivalue contributes one,
  including duplicates; and
- a top-level object contributes one. A non-empty object stored as flattened
  leaves contributes once for its parent, not once per descendant. A nested
  list or object member likewise contributes once and is not traversed.

The scalar and ordinary multivalue behavior follows Splunk's documented
occurrence-count behavior. Null members and typed containers are not settled
by the available Splunk documentation; counting immediate non-null containers
atomically is the explicit Open Splunk v0.1 typed-data choice pending a live
differential oracle.

`count(field)` returns a non-null `UInt64`. A global aggregation over no rows or
with no eligible value emits one row containing zero. A retained group whose
field is always missing, null, or empty also contains zero; grouped
aggregation over no rows emits no groups. Projected-away fields remain absent
and contribute zero rather than being recovered from hidden event columns.
Repeated `count(field)` measures over the same input share one per-row
cardinality calculation. The compiler sums that contribution in `UInt128`;
the production 250-million-row read ceiling and 1 MiB hard event ceiling make
the published total strictly representable as `UInt64`.

The current downstream field grammar cannot reference a default aggregate name
that contains parentheses. Use `AS` when a `count(field)`, `dc`, `values`,
`list`, `min`, `max`, `earliest`, `latest`, `sum`, `avg`, or percentile result
will be consumed by a later command.

The field-occurrence form accepts exactly one unquoted, exact field inside
`count(...)` or `c(...)`; the conditional form above is the only supported
expression argument. Bare `c`, `count()`, `c()`, wildcard fields,
`c(eval(...))`, quoted fields, and other predicate/expression forms remain
explicitly unsupported rather than being approximated. Public SPL
documentation establishes `c(field)` as an abbreviation but does not pin its
generated result-field spelling; canonicalizing it to `count(field)` is an
explicit Open Splunk v0.1 choice. Bare row count remains the documented
`count` form.

`earliest(field)` and `latest(field)` select by event chronology, not by field
value and not by current pipeline order. `earliest` chooses the eligible value
with the least original canonical `_time`; `latest` chooses the greatest.
An upstream `sort` alone therefore cannot change the winner. Filters,
`head`, `tail`, and `dedup` can change it only by changing which events survive
to the aggregate.

Chronological aggregates require source event rows with the visible,
unmodified canonical `_time`. Removing or replacing `_time`, renaming it away
or back, binning it in place, or placing a transforming command before the
aggregate is rejected explicitly. `table` must retain `_time`. Copying a time
bucket to another field with `bin _time ... AS bucket_time` preserves the
original `_time` and remains eligible.

Equal times use an ascending immutable total key: original nanosecond `_time`,
event ID, visibility sequence, source identity `(index, collector, batch
sequence, batch ID)`, and one-based multivalue member ordinal. `earliest`
selects the minimum key and `latest` the maximum. Thus equal-time events can
produce different earliest and latest winners, and within one multivalue
event the first eligible member wins `earliest` while the last wins `latest`.
Splunk's public documentation distinguishes chronological `earliest`/`latest`
from processing-order `first`/`last`, but does not pin equal-time or
multivalue ties; this deterministic key is an explicit Open Splunk v0.1
boundary.

Missing fields, explicit nulls, empty multivalues, and null members do not
participate. An empty String is eligible. Immediate scalar members of a
top-level multivalue participate independently without expanding event rows;
duplicates remain distinct occurrences. Generic objects, flattened object
parents, nested arrays, and nested objects fail the live aggregate atomically.
Rows outside authorization/time/visibility scope, rows removed upstream, and
rows omitted for an incomplete `BY` tuple cannot trigger that measure error.
Validation is forced before any downstream projection, filter, sort, or row
limit can hide a poisoned retained row.

The selected value uses its stored canonical scalar spelling and is returned
as nullable `Mixed`: valid UTF-8 is String, invalid UTF-8 is Bytes, and no
winner is null. Numerics, Booleans, timestamps, durations, and decimals are
not compared as values; their canonical spellings are carried from the
chronologically selected occurrence. A global aggregate over no rows emits
one null result row; a retained group with no eligible candidate contains
null; grouped aggregation over no rows emits no groups. Projected-away inputs
remain absent.

Repeated identical chronological measures share one normalization and one
winner state per input/direction. `earliest` and `latest` require separate
constant-size states and share one immutable row key. A Dynamic input retains
only its first eligible value, last eligible value, eligible-member count, and
unsupported-container bit per event; bounded selector/count/validation passes
over a multivalue never retain a normalized member array or repeat the row key
for each member. The lowering then uses scalar conditional `argMin`/`argMax`
and one constant-size validation state per Dynamic input. A materialized
whole-result check is joined before publication and retained in a final
zero-row validation branch, so even a downstream always-false filter cannot
erase validation. The lowering uses no pipeline sort, window, `groupArray`,
Array aggregate combinator, `arrayFold`, or row-expanding `ARRAY JOIN`.
`first`, `last`, `earliest_time`, and `latest_time` remain unsupported.

`min` and `max` follow Splunk's documented numeric-if-possible rule for
ordinary numbers and text. Open Splunk v0.1 makes the mixed-type and symbol
edges deterministic: finite numeric candidates sort before lexical candidates;
numeric candidates compare numerically and lexical candidates compare by raw
bytes. Consequently, `min` selects a numeric value whenever one is present,
while `max` selects a lexical value whenever one is present. This is a total,
locale-independent order. Public Splunk documentation does not define a
standard symbol order and warns that some symbols may precede numbers, so the
placement of punctuation is an explicit Open Splunk v0.1 boundary pending a
live differential oracle.

For runtime String and Dynamic values, a candidate is numeric only when it is
valid UTF-8, no longer than 4 KiB, matches this complete decimal grammar, and
converts to a finite `Float64`:

```text
[+-]?(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+)(?:[eE][+-]?[0-9]+)?
```

Whitespace, `NaN`, infinity spellings, overflowing exponents, invalid UTF-8,
and longer values remain lexical. Numeric zero is normalized to positive zero,
and equivalent spellings such as `01`, `1.0`, and `1e0` publish the same
canonical `Double(1)` result. The runtime comparison key is currently
`Float64`; distinct very-wide integers or exact decimals that collapse to the
same `Float64` are an explicit Open Splunk v0.1 precision boundary pending an
exact decimal comparison key and live Splunk oracle. Statically typed numeric,
Boolean, and timestamp columns take an exact native ClickHouse path instead,
preserving their physical width and timestamp precision; non-finite fixed
floats do not participate.

Missing values, explicit null, an empty multivalue, and null multivalue members
do not participate. An empty String is a lexical candidate. Every immediate
member of a top-level multivalue participates independently, without expanding
event rows. Generic objects, flattened object parents, nested arrays, and
nested objects fail the live aggregate atomically with the same sanitized
unsupported-value error as `dc` and `values`. Rows outside the authorized
tenant/index/time/visibility scope, rows removed by an upstream filter, and
rows omitted for an incomplete `BY` tuple cannot trigger that error.

A runtime String, Dynamic, or multivalue result is nullable `Mixed`: numeric
winners are `Double`, lexical winners are String or Bytes, and no winner is
null. A statically typed scalar result is nullable and retains its numeric,
Boolean, or timestamp type. A global aggregation over no rows emits one null
result row; a retained group with no eligible candidate also contains null;
grouped aggregation over no rows emits no groups. Projected-away inputs remain
absent. Repeated extrema over one input share one bounded row-local
normalization, and `min` plus `max` use separate constant-size aggregate
states. A statically known scalar String uses one nullable input alias, one
numeric-classification alias, and one non-null ordering tuple; ClickHouse
aggregates that tuple with conditional scalar `min`/`max` and constructs the
nullable `Mixed` winner only after aggregation. This avoids row-local
singleton arrays and per-row Dynamic boxing. Dynamic and fixed multivalue
inputs retain the guarded Array-combinator path. Scalar extrema and
`dc`/`values` over the same input share the nullable scalar materialization.
Neither path uses `ARRAY JOIN`, row expansion, sorting, or unbounded list
aggregation.

`min` and `max` accept exactly one unquoted, exact field. Expression/eval
arguments, wildcards, quoted fields, empty argument lists, and multiple
arguments are rejected explicitly.

`dc` and `values` process the stored canonical scalar spelling
case-sensitively, as string-oriented Splunk aggregates:

- missing and explicit-null inputs contribute nothing; an empty String is one
  distinct value;
- every scalar element of a top-level multivalue input contributes
  independently, null elements are ignored, and duplicates within or across
  events collapse;
- integer `1` and String `"1"` converge, while stored Strings `"1.0"` and
  `"01"` remain distinct;
- Bool `true` and String `"true"` converge; and
- Bytes, timestamps, durations, and decimals use their deterministic stored
  lexical encodings.

Generic objects, nested arrays, and object or nested-container multivalue
elements have no supported scalar spelling and fail the live aggregate
atomically with a sanitized unsupported-value error. A row removed by an
upstream filter or by an incomplete `BY` tuple cannot contribute to the
measure and does not trigger that measure error. Unsupported `BY` keys retain
their stricter whole-input validation described below. A successful `rex` or
`spath` scalar overwrite replaces an older container for this purpose; a
failed extraction that preserves the older container still fails.

Collector JSON decoding does not preserve every original numeric token
spelling. For example, an exactly representable JSON `1.0` is stored as
Float64 and has canonical lexical spelling `"1"` at search time. `dc` is exact
over the stored typed value and its canonical spelling, not over source bytes
that ingestion no longer retains. This is an explicit Open Splunk boundary
relative to Splunk's string-field model.

`dc` returns a non-null `UInt64`. Global aggregation over no rows or with no
eligible values emits one row containing zero. A retained group with no
eligible measure value also contains zero.

`values` returns one non-null typed multivalue cell containing the distinct
canonical strings in raw-byte lexicographic order. It does not apply numeric,
locale, Unicode-normalization, or case-insensitive ordering. A global or
retained empty group publishes `[]`; that physical empty list is logically
absent to downstream SPL presence tests. Grouped aggregation over no rows emits
no groups. A projected-away input stays absent and contributes zero to `dc` or
an empty list to `values`, rather than being recovered from hidden event
columns. Invalid UTF-8 fixed String data is retained as a Bytes child at the
typed result boundary instead of being replaced or exposed as malformed text.

`list` returns one non-null typed multivalue cell containing the first 100
eligible canonical strings in current pipeline order. Unlike `values`, it
preserves duplicates. A top-level multivalue contributes each immediate
non-null member in its stored member order before the next event contributes.
Missing fields, explicit nulls, and null members contribute nothing; an empty
String is retained. Scalar conversion and unsupported object or
nested-container handling are identical to `dc` and `values`. In particular, an unsupported
value fails the whole aggregate even when it occurs after the visible first
100 values, because truncation is not permission to skip validation.

Event pipelines begin in `_time DESC, event_id DESC` order. The immutable
visibility sequence closes cross-commit ties, followed by the immutable
`(index, collector, batch sequence, batch ID)` source identity. That final
private key also makes migrated rows deterministic when their pre-migration
visibility sequence is zero. An upstream `sort`, `head`, `tail`, or `dedup`
establishes the order seen by `list`; use `sort 0` when all matching rows must
participate. Ordering is implemented with a per-group ordinal and a bounded
merge-stable aggregate, never ClickHouse's indeterminately ordered `groupArray`
and never row-expanding `ARRAY JOIN`.

A global or retained empty group publishes `[]`; that physical empty list is
logically absent to downstream SPL presence tests. Grouped aggregation over no
rows emits no groups, and a projected-away input stays absent. Invalid UTF-8
fixed String data is retained as a Bytes child at the typed result boundary.
The first 100 selected values in one `list` cell may contain at most 512 KiB
of raw lexical payload. Crossing that byte ceiling fails atomically; values
after occurrence 100 are truncated and do not count toward it. The retained
aggregate state is bounded before grouping to at most 100 tuples and 512 KiB
of exact String payload per unique group/input.

Equivalent `dc` and `values` measures over the same input share one exact
canonical set. A values-bearing set retains at most 10,000 strings; a dc-only
set retains at most 100,000. Every `values` cell and the combined public
`values` and `list` cells in one row are limited to 10,000 elements and 512
KiB of raw lexical payload. Duplicate output aliases count independently,
while repeated `list` aliases over the same input share one ordered physical
state. Before any downstream filter, projection, sort, or row limit, the
complete transforming result is also limited to 100,000 elements and 8 MiB
across all `values` and `list` outputs. Crossing any ceiling fails the search atomically with a sanitized
resource-limit error; results are never truncated or approximated. The
ClickHouse query-memory ceiling independently bounds the exact aggregate state
before these post-aggregate publication checks run.

`fields`, `table`, `rename`, `head`, `tail`, and a direct `eval` field copy
preserve the fixed multivalue type. A later `dc` or `values` flattens its
members; a later `list` flattens its members in order; `min`/`max` compare its
immediate members; and `sum`/`avg` parse and
flatten finite numeric members. Base-search equality and wildcard tests match
when any valid-UTF-8 member matches;
inequality matches only a nonempty list with no equal member, and `field=*`
matches only a nonempty list. Invalid-UTF-8 Bytes members do not match textual
equality or wildcard literals. Ordered base-search comparison, `where`, scalar
`eval` functions, `sort`, `dedup`, `stats ... BY`, `rex`, `spath`,
`bin`, `top`, `rare`, and chart axes reject a known fixed multivalue input
explicitly until their SPL multivalue behavior is pinned; the compiler never
silently stringifies the array.

On an open event schema, `fields` is the reserved convenience payload and
cannot be a `stats` input or `BY` field. A prior transforming command or exact
`table` may close the schema and declare an ordinary field named `fields`.

The numeric aggregates accept finite integers, floats, numeric strings, tagged
decimals, and canonical timestamps converted to Unix epoch seconds. Missing,
null, empty-string, Boolean, bytes, object, nonnumeric, `NaN`, and infinite
inputs are ignored. For `sum`, `avg`, and percentiles, each finite numeric
scalar in a top-level multivalue array contributes independently, including
duplicates; nonnumeric members and nested containers are ignored without
expanding event rows. This applies both to runtime Dynamic event arrays and to
known fixed multivalue results from commands such as `values`.

`sum`, `avg`, and percentiles return nullable `Float64`. A global aggregation
over no rows still emits one row; an aggregate with no eligible numeric
contribution is null, including `sum` rather than zero. Splunk's primary
documentation specifies that nonnumeric values are ignored but does not define
the empty `sum` result, so null is the explicit v0.1 compatibility choice. A
grouped aggregation over no rows emits no groups. Finite inputs are filtered
before aggregation, but a computed IEEE `NaN` or positive/negative infinity
caused by Float64 arithmetic is preserved rather than changed to null.
Projected-away inputs stay absent and cannot be recovered from hidden event
columns.

Percentiles use ClickHouse
`quantilesGKOrNullArray(100, levels...)(numeric_members)`, a bounded
approximately 1%-rank-error aggregate. All unique levels over one exact input
share one normalized numeric array and one physical GK state; synonymous or
repeated requests such as `p50(field)` and `perc50(field)` reuse the same
component. Accuracy is fixed at 100, while the 16-measure, 10,001-group,
250-million-source-row, four-thread, and 1 GiB query-memory ceilings remain
authoritative resource bounds. Splunk uses exact behavior for smaller distinct
sets and a different proprietary approximation for larger inputs, whereas
Open Splunk currently uses GK at every cardinality. Exact values can therefore
differ while the documented rank/error intent is preserved. A global empty or
all-ineligible percentile and a retained all-ineligible group publish null.
Splunk's available documentation does not settle typed null/container members;
the immediate-member rules above are the explicit Open Splunk v0.1 boundary
pending a live oracle.

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

### `bin` / `bucket`

```spl
| bin _time span=5m
| bin _time span=5 AS bucket_time
| bin severity span=10
| bucket span=10 latency AS latency_band
| stats count | bin count span=100
| bin _time span=5m | stats count BY _time
```

`bucket` is an exact alias for `bin`. The streaming slice accepts one exact,
unquoted field, one explicit positive span, and optional `AS <exact output>`.
The field and `span` option may appear in either order; `AS` is final. The
command replaces its source by default. With `AS`, the source is retained and
the destination is added or overwritten. An event without the source field is
not binned at all: an existing destination keeps its prior value, semantic
type, and sparse presence, exactly as `rex` does on no match. Row cardinality,
event identity, and established order do not change, and downstream filters,
projections, sorts, aggregations, field catalogs, and field summaries consume
the bucketed value.

A unitless span is a base-10 integer from 1 through `9007199254740991`
(`2^53-1`). For a non-time field it is an absolute numeric width. A field whose
pipeline type is already a fixed integer or `Float64` qualifies—promoted
numeric columns such as `severity`, numeric `eval` outputs, and numeric `stats`
outputs—as does a runtime-typed event field, whose per-row semantic type
decides the bucket. A fixed string, Boolean, or other non-numeric pipeline type
is known before execution and is still rejected with
`SPL_UNSUPPORTED_BIN_FIELD_TYPE`; use the supported `eval`/`tonumber` or
`stats` surface to establish a fixed numeric field first.

Integer bucketing uses exact widened intermediate arithmetic and the
mathematical definition `floor(value / span) * span`, so `-11` with span 10
becomes `-20`. The original signed or unsigned physical type is retained.
A boundary whose bucket start cannot be represented by that type—for example,
`MinInt64` with a non-dividing span—fails the search with a sanitized
unsupported-value error rather than wrapping or widening. `Float64` values use
finite double-precision floor semantics, retain `Float64`, normalize negative
zero, and fail the search if the input or result is not finite. Explicit null
remains null.

A runtime-typed event field is classified per row from its stored semantic type
and its runtime scalar type. Numeric text becomes the number it spells, so the
bucket participates in later numeric filters and still groups with its numeric
twin. The exact lexical path is limited to 4,096 input bytes and to a signed
`Int256` bucket boundary. Integral text becomes `Int64` or `UInt64` when the
boundary fits, otherwise it becomes semantic Decimal backed by `Int256`.
Fractional or exponent text is also bucketed exactly; a boundary in
`[-2^53, 2^53]` is published as an exactly representable `Float64`, while a
wider boundary uses that Decimal/`Int256` representation. Leading zeros and an
explicit `+` belong to the spelling rather than to the value, so a zero-padded
fixed-width numeric field—an account or order identifier, a mainframe-style
log—buckets exactly as its unpadded twin does, in the significand and in the
exponent alike.

A valid stored `decimal/v1` value follows the same exact lexical arithmetic and
is always published as semantic Decimal backed by `Int256`; a later `bin` can
bucket that calculated value again without changing its semantic type. The
result is exact only when its signed `Int256` boundary is representable. An
oversized, malformed, or out-of-range value that is declared Decimal fails with
the sanitized unsupported-value error instead of passing through or wrapping.
Containers and multivalue values are the same explicit runtime error.

Every other String keeps its exact text—non-numeric text, surrounding
whitespace, `NaN`/`inf` spellings, an overflowing exponent, invalid UTF-8, text
above the 4,096-byte exact-parser ceiling, and any spelling whose exact bucket
start is outside signed `Int256`—so one anomalous text value never fails an
otherwise successful search. Boolean, timestamp, duration, and bytes values
also keep their value: `bin` discretizes numbers and leaves other scalars
alone. Rows written before the current aligned field metadata existed carry no
readable semantic type; their values are never interpreted heuristically and
pass through unbinned instead of failing the search.

The sanitized unsupported-value error is a property of one row's value, never
of the pipeline that reads it. A bucket that is supported stays supported under
every downstream consumer, so `sort <destination>`, `search <destination> >=
<literal>`, and the equivalent `where <destination> >= <literal>` all agree with
one another and with the same query without the `bin` stage.

For exact canonical `_time`, a unitless span means seconds. Explicit `s`, `m`,
or `h` spans are also accepted from one second up to, but not including, 24
hours. A time-unit span on another field is rejected. Time alignment uses
mathematical floor division on UTC Unix-epoch nanoseconds, including for
timestamps before 1970 and values one nanosecond below a boundary. The
bucketed output remains a `DateTime64(9)` timestamp.

A bin result remains an Events relation unless an earlier transforming command
already made it Statistics. Replacing `_time` makes that field synthetic, so a
later time `bin`, `timechart`, or timeline request is rejected with a
source-located canonical-time diagnostic. `bin _time ... AS bucket_time`
retains the original canonical `_time`, so later timechart and timeline
analysis remain valid. Conversely, writing any numeric bin `AS _time` replaces
the canonical clock and invalidates those consumers.

ClickHouse must materialize each public bin boundary as `DateTime64(9)`.
Consequently a search beginning at the supported `1900-01-01T00:00:00Z` lower
bound with an epoch-aligned span such as `7h` is rejected when its first
possible bin would be `1899-12-31T19:00:00Z`; the value is never clamped or
wrapped. This differs deliberately from `timechart`, whose private integer
ordinal transport can represent a partial bucket before the timestamp storage
minimum.

On an open event schema, `fields` is the reserved whole-payload convenience
column and cannot be the source or destination; a closed `table` or
transforming schema may declare an ordinary numeric column with that spelling.
When an `AS` output could shadow a member of the immutable dynamic payload, the
public convenience object is omitted while unrelated dynamic fields remain
available to downstream SPL.

Omitted `span`, automatic `bins`/`minspan`, `start`, `end`, `aligntime`,
logarithmic spans, calendar/subsecond units, wildcards, quoted fields, and
multiple fields are not yet supported. Spans of one day or more are also
rejected because Splunk aligns them to midnight in the user's timezone,
including daylight-saving transitions, while the current logical plan does
not carry that alignment context. Each unsupported form fails explicitly
rather than falling back to an approximate or data-dependent bin.

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

### `chart`

```spl
| chart count OVER path BY status_class
| chart count BY path, status_class
```

`chart` is a bounded runtime-wide two-field pivot, not a `stats` alias. The
initial slice accepts exactly one argument-free `count`, exactly one row-split
field (Splunk's `<row-split>`, the first output column), and exactly one
column-split field (the `<column-split>` whose values become column names). The
two spellings above are the only accepted ones and compile to identical plans
and SQL; in the `BY` form the fields may be separated by a comma, whitespace, or
both, and a trailing comma is rejected. `OVER`, `BY`, and `count` are
case-insensitive. A field literally named `over` is charted by either spelling,
but in the `BY` form it must be comma-separated (`chart count BY over, level`),
because `chart count BY level over` is indistinguishable from the rejected
`BY`-before-`OVER` form.

`chart` must be the final pipeline command (`SPL_UNSUPPORTED_CHART_PIPELINE`,
located at the following command) because every public column except the first
is named from a runtime field value, so no downstream stage can be resolved
during planning. Completed-job field analysis and the timeline reject the
resulting relation as transforming, exactly as they reject `timechart`.

Unlike `timechart`, `chart` does not require event rows and does not require the
canonical `_time`, so `... | stats count BY path, level | chart count OVER path
BY level` is legal. `count` counts rows of the input relation, never a sum of an
upstream `count` column.

#### Row axis

The first output column is exactly the group column `stats count BY <row field>`
produces for the same field: the same public name, value kind, value rendering,
and eligibility. Fixed `String`, numeric, `Bool`, and timestamp fields keep
their own scalar kind; runtime-typed fields converge on the same lexical scalar
text `stats BY` uses, so the integer `500` and the string `"500"` are one row
labeled `500`. Rows exist only for present, non-null row values — `usenull` and
`nullstr` are column-axis options only — and an empty string is a present value
that produces a row. Lists, objects, and flattened non-empty object parents in
the row field fail the whole search, the same boundary as `stats BY`. There is
no length ceiling on an individual row value, which is data rather than a column
name; the buffered result as a whole is bounded below.

Parity is exact rather than approximate, including its two special cases.
`_raw` is published as the `Mixed`, nullable column `stats count BY _raw`
produces, so a search over events ingested with `RAW_ENCODING_BINARY` charts
their non-UTF-8 bytes rather than failing. A row field that is statically null,
such as `| eval n=null | chart count OVER n BY level`, is the `String` group
column `stats count BY n` publishes: it declares its one column and names no
rows, because no present, non-null value exists.

Rows are ordered ascending using the pipeline's automatic numeric-aware
ordering, exactly the order `| sort 0 +<row field>` produces on that column.
Splunk documents no chart row order; this is Open Splunk's own established
dynamic ordering.

#### Column axis

This version supports string column values plus missing/explicit-null. A field
that is statically null, such as `| eval n=null | chart count OVER path BY n`,
is inside that domain and publishes the one `NULL` series `usenull=true`
describes, exactly as a column field an upstream projection removed does.
Numeric, Boolean, timestamp, extended, list, and object column values fail the
whole command atomically before any schema or row is published — Splunk
documents `bins`/`span`/`start`/`end` discretization for the row split only and
documents nothing about rendering numeric split values as column names, so no
approximation is attempted. A compile-time-known non-string column field type is
rejected earlier with `SPL_UNSUPPORTED_CHART_FIELD_TYPE`. Numeric values are
therefore legal row labels and fatal column labels, and this version offers no
remedy that makes one chartable: `bin` discretizes numbers into numeric bucket
starts and so only converts a working string column axis into a fatal numeric
one, while `tonumber` produces numbers and `rex` does not match a non-string
source. A numeric column field must be a string before it reaches `chart`.

A column value is classified on its own presence, independent of whether the
same input row carries an eligible row value. An unsupported column value on an
event that omits the row field entirely fails the whole command exactly as it
would on any other event, so the outcome never depends on which field happens to
name the row axis. Column values that appear only on row-ineligible events name
no public column, because no published row could count them.

Behavior equals Splunk's documented defaults `limit=top 10`, `useother=true`,
`usenull=true`, `otherstr=OTHER`, and `nullstr=NULL`. The ten ordinary values
with the highest total count across the whole chart are retained — the limit is
global, not per row — with UTF-8 lexical ascending order of the raw label
breaking ties. `NULL` exists whenever at least one input row has a missing or
explicit-null column value and never consumes a top-ten slot; `OTHER` exists
whenever at least one ordinary value was excluded and carries the per-row sum of
every excluded value. Column values beginning with `_` receive Splunk's `VALUE`
prefix (`_audit` becomes `VALUE_audit`). Public columns are the row column
first, then ordinary columns in UTF-8 lexical ascending order of the published
name, then `NULL`, then `OTHER`, for at most 13 columns. Cells are non-null
unsigned counts and an absent (row, column) pair is exactly `0`.

A label fails the whole command atomically when it is empty, invalid UTF-8,
longer than 256 bytes, exactly `NULL` or `OTHER`, when two distinct labels
converge after `VALUE` normalization, or when it equals the row column's name.
Every member of that rule, convergence included, is evaluated over the labels
the input carries rather than over the columns the pivot publishes: two
converging labels fail the search whether they lose the top-ten cutoff, fold
into `OTHER`, or appear only on row-ineligible events.

Because `NULL` and `OTHER` are always available, the column bound can never drop
an input row: for every published row, the sum of its cells equals exactly the
count `stats count BY <row field>` reports, and the published row set and order
equal `stats count BY <row field> | sort 0 +<row field>`.

#### Discretization

`chart` never discretizes anything itself. `bin`/`bucket` is the only
discretizer and its contract is reused unchanged, so
`... | bin severity span=10 | chart count OVER severity BY level` charts bucket
starts, and `... | bin _time span=5m AS bucket_time | chart count OVER
bucket_time BY level` charts `DateTime64(9)` bucket starts. **Only observed
buckets appear**: unlike `timechart`, `chart` does not fill empty buckets and
does not extend the row axis to the search time range. An in-place
`bin _time span=5m | chart count OVER _time BY level` is legal.

Discretization is a row-axis facility only. A binned field is numeric or a
timestamp, so naming one as the column split fails: a fixed numeric or time
field is rejected with `SPL_UNSUPPORTED_CHART_FIELD_TYPE` before execution, and
a runtime-typed one fails the whole command atomically on its bucket values.

#### Bounds and explicit rejections

Results are bounded to 10,000 distinct row values and 12 runtime series (ten
ordinary, `NULL`, and `OTHER`). Exceeding the row ceiling fails atomically with
an execution-limit error and no partial result — never truncation and never an
`OTHER` row, because Splunk documents no `OTHER` row. This non-truncating
resource policy intentionally differs from Splunk's installation-configurable
`maxresultrows` ceiling. With default executor settings the intermediate group
budget grows to at most 130,000 states, exactly as for `timechart`; an
explicitly configured lower group cap remains authoritative.

The column axis is collapsed to the published domain before the row-keyed
aggregation, so that aggregation holds at most one state per (row value, public
series) pair and a wide column axis never consumes the row budget: 200 distinct
row values across 1,000 distinct column values publishes 200 rows and 13 columns
and stays inside the budget. The preceding one-dimensional aggregate that
chooses the domain holds one state per distinct raw column value, so a column
field with more than roughly 130,000 distinct raw values still fails atomically
with an execution-limit error and no partial result. Reducing that raw
cardinality means re-shaping the column field into a coarser string — `replace`
is the string-in, string-out surface for it — because `bin` would replace the
labels with numeric bucket starts, which the column axis rejects.

The whole pivot is buffered before any schema or row is published, because the
public column names are runtime values. The buffered result therefore carries
its own total-byte ceiling in addition to the row ceiling: a chart whose
retained row values exceed it fails atomically with an execution-limit error and
no partial result, rather than materializing an unbounded result in front of the
search job's incremental byte limits. Individual row values remain unbounded;
only their total is capped.

The following fail explicitly rather than being approximated:

- any aggregate other than argument-free `count` — `sum`, `avg`, percentiles,
  `count(field)`, `dc`, `values`, multiple aggregates, `agg=<term>`, sparkline
  aggregates, and parenthesized eval-expression aggregates — and `AS <name>` on
  the aggregate, all with `SPL_UNSUPPORTED_CHART_AGGREGATE`. `AS` is rejected
  because no output column of the supported pivot could carry the alias;
- every option other than `agg`, with `SPL_UNSUPPORTED_CHART_OPTION`, including
  spellings equal to a documented default: `limit`, `useother`, `usenull`,
  `otherstr`, `nullstr`, `cont`, `sep`, `format`, `bins`, `span`, `start`,
  `end`, `aligntime`, and `dedup_splitvals`; `agg=` names the aggregate rather
  than a rendering option and keeps `SPL_UNSUPPORTED_CHART_AGGREGATE` in every
  position;
- the trailing `WHERE` series filter (`... BY x WHERE count > 100`,
  `... in top 5`), with `SPL_UNSUPPORTED_CHART_SYNTAX`;
- the single-split forms `chart count OVER f` and `chart count BY f`, with
  `SPL_UNSUPPORTED_CHART_SYNTAX` and a suggestion to use `stats count BY f`;
- more than two split fields, a duplicated field on both axes, `BY` before
  `OVER`, wildcard or quoted field names, and any trailing token, all with
  `SPL_UNSUPPORTED_CHART_SYNTAX`; a missing field token after `OVER`/`BY` is
  `SPL_EXPECTED_FIELD`;
- a row or column field named `NULL` or `OTHER`, and the reserved `fields`
  convenience column on an open event schema, with
  `SPL_UNSUPPORTED_CHART_FIELD_TYPE`.

## Completed-job field analysis

Field discovery re-executes an immutable completed-job snapshot with the same
tenant, authorized indexes, half-open event-time range, index-time cutoff, and
visibility cutoff as the original search. It analyzes the final event relation,
so `search`, `where`, `eval`, `rex`, `bin`, `rename`, `fields`, `table`, `sort`,
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

All ten of the product plan's initial searches are one shared versioned
manifest. The ordinary suite sends every manifest query through parsing,
planning, and compilation. A required CI integration test additionally sends
one deterministic, sanitized 20-event NDJSON fixture through the collector
decoder, production ClickHouse store, parser, planner, compiler, query
executor, search-job manager, and owner/tenant-scoped signed-cursor paging
against the pinned ClickHouse release.

The fixture is generated from one typed profile, has a pinned SHA-256 digest,
uses only synthetic identifiers and documentation IP addresses, and is scanned
for secret/PII keys, credential-like values, email-like values, workstation
paths, production URLs, duplicate keys, and non-documentation IPv4/IPv6
addresses. Scanner failures never echo rejected key text. The executed
contract asserts every public column's name, value kind, nullability, and
multivalue flag; every ordered typed cell and row ordinal; stable schemas and
totals across three-row pages; and a complete terminal page with no cursor.
Wrong-owner and wrong-tenant page access returns not found, and a modified
signed cursor is rejected. It covers:

- trace-ID event investigation;
- errors/warnings with descending time;
- quoted raw error fragments;
- severity counts;
- frequent errors by logger and message;
- event volume by severity through `timechart`;
- server errors by route through `timechart`;
- HTTP response counts by route and status as the product-plan
  `stats count BY path, status` search, with additional pivot coverage through
  `chart count OVER path BY status_class`;
- slow routes through `eval`, `p95`, and `where`; and
- top messages.

Ordinary event rows expose a sparse `fields` object, not a part-wide union of
every dynamic ClickHouse JSON path. The compiler transports per-row presence
metadata in one bounded private column whenever it exposes the raw public
`fields` payload. The executor validates and consumes that column but never
publishes it. A path named as present with no physical JSON value is explicit
null; an unnamed part-level null path is missing and is omitted. The corpus
contains both states for `optional_note` and pins them in ordered results.
Canonical shared path parsing preserves literal dots, backslashes, and percent
escapes without collisions.

Each durable event may carry at most 1 MiB of aggregate normalized field-name
metadata in addition to the existing count, depth, segment, and event-size
bounds. This limit is enforced after mandatory redaction before
acknowledgement with allocation-free shared encoder accounting, defensively by
storage, and again by result conversion.

The slow-route fixture deliberately gives every assessment request the same
`800ms` duration, so the current bounded approximate `p95` has an exact
expected result without claiming a live Splunk differential oracle. The
product-plan query and this v0.1 corpus use the literal message
`Request metrics` and millisecond durations; current GradeThis deployments
emit `Request summary statistics` and may use microsecond/second duration
spellings. Those differences are covered by a separate `current-source-v1`
migration profile rather than an undocumented rewrite of the v0.1 manifest.
Its sanitized 20-event default is scanner-validated and SHA-256 pinned.
`TestBackendVertical` rebases the same typed semantics to a recent UTC window,
runs the shipped GradeThis collector configuration through file discovery,
pre-WAL sanitization, gRPC ingestion, ClickHouse, and public protobuf search
paging, then proves a three-layer trace, severity counts, 5xx requests,
route/status counts, `rex` counts for `µs`/`ms`/`s`, and top messages. Trusted
host/source/sourcetype/service/environment metadata remains outside the raw
application JSON, and no OpenTelemetry log collector is started.

The `chart` pivot additionally has pinned ClickHouse coverage for
`bin severity span=10 | chart count OVER severity BY level`, for
`chart count OVER path BY level` with both `NULL` and `OTHER` present, and for
the differential that every published row's cells sum to the count
`stats count BY path` reports.

## Explicitly unsupported surface

The following planned commands are not implemented in this version:

```text
eventstats, streamstats
```

The supported `stats` functions are argument-free `count`, exact-field
`count(field)`, conditional `count(eval(predicate)) AS output`,
`dc(field)`/`distinct_count(field)`, `values(field)`,
`list(field)`, `min(field)`, `max(field)`, integer-suffix
`pN(field)`/`percN(field)` for `N` from 1 through 99, `sum(field)`,
`avg(field)`, `earliest(field)`, and `latest(field)`. Other functions are
unsupported.
Unsupported percentile examples include suffixes 0 or 100, decimal suffixes,
the SPL2-style `perc(field, N)` form, `upperperc`, and `exactperc`. Percentile
eval expressions and wildcard inputs are also outside the current exact-field
subset. The broader `count` forms listed in the stats section are unsupported
too.

This contract will be versioned as support expands. A live Splunk differential
oracle is not currently available, so ambiguous null, multivalue, formatting,
and type edges remain conservative and must gain oracle-backed differential
coverage before they are declared compatible.

Reference behavior is compared against Splunk's official [`search`](https://help.splunk.com/en/splunk-enterprise/search/spl-search-reference/10.2/search-commands/search),
[`sort`](https://help.splunk.com/en/splunk-enterprise/search/spl-search-reference/10.2/search-commands/sort),
[`dedup`](https://help.splunk.com/en/splunk-enterprise/search/spl-search-reference/10.2/search-commands/dedup),
[`stats`](https://help.splunk.com/en/splunk-enterprise/spl-search-reference/10.0/search-commands/stats),
[`stats` with eval expressions](https://help.splunk.com/en/splunk-enterprise/search/search-manual/10.2/calculate-statistics/use-stats-with-eval-expressions-and-functions),
[`stats` multivalue aggregation](https://help.splunk.com/en/splunk-cloud-platform/search/spl2-search-reference/stats-command/stats-command-overview-syntax-and-usage),
[`min` and `max` aggregate functions](https://help.splunk.com/en/splunk-enterprise/search/spl2-search-reference/statistical-and-charting-functions/aggregate-functions),
[`earliest` and `latest` time functions](https://help.splunk.com/en/splunk-enterprise/search/spl2-search-reference/statistical-and-charting-functions/time-functions),
[`first` and `last` event-order functions](https://help.splunk.com/en/splunk-enterprise/search/spl-search-reference/10.2/statistical-and-charting-functions/event-order-functions),
[`where`](https://help.splunk.com/en/splunk-enterprise/spl-search-reference/10.2/search-commands/where),
[`if`, `coalesce`, `case`, `match`, `like`, and conditional functions](https://help.splunk.com/en/splunk-enterprise/search/spl-search-reference/10.4/evaluation-functions/comparison-and-conditional-functions),
[`now` and date/time functions](https://help.splunk.com/en/splunk-enterprise/search/spl-search-reference/10.4/evaluation-functions/date-and-time-functions),
[`strftime` date/time variables](https://help.splunk.com/en/splunk-enterprise/search/spl-search-reference/10.4/time-format-variables-and-modifiers/date-and-time-format-variables),
[`strptime` enhanced timestamp parsing](https://help.splunk.com/en/splunk-enterprise/get-started/get-data-in/10.2/configure-timestamps/configure-timestamp-recognition),
[`lower`, `upper`, and text functions](https://help.splunk.com/en/splunk-enterprise/spl-search-reference/10.0/evaluation-functions/text-functions),
[SQLite core `substr` semantics](https://www.sqlite.org/lang_corefunc.html),
[`tostring` and conversion functions](https://help.splunk.com/en/splunk-enterprise/spl-search-reference/10.2/evaluation-functions/conversion-functions),
[`round` and mathematical functions](https://help.splunk.com/en/splunk-enterprise/search/spl-search-reference/10.2/evaluation-functions/mathematical-functions),
[`mvcount` and multivalue eval functions](https://help.splunk.com/en?resourceId=SCS_SearchReference_MultivalueEvalFunctions),
[`predicate expressions`](https://help.splunk.com/en/splunk-enterprise/search/search-manual/10.2/expressions-and-predicates/predicate-expressions),
[`isnull` and `isnotnull` informational functions](https://help.splunk.com/en/splunk-enterprise/spl-search-reference/10.0/evaluation-functions/informational-functions),
[`rex`](https://help.splunk.com/en/splunk-cloud-platform/spl-search-reference/10.2.2510/search-commands/rex),
[`spath`](https://help.splunk.com/en/splunk-cloud-platform/spl-search-reference/10.0.2503/search-commands/spath),
[`replace`](https://help.splunk.com/en/splunk-cloud-platform/spl-search-reference/10.4.2604/evaluation-functions/text-functions),
[`tonumber`](https://help.splunk.com/en/splunk-enterprise/search/spl-search-reference/9.0/evaluation-functions/conversion-functions),
[`rename`](https://help.splunk.com/en/splunk-enterprise/search/spl-search-reference/10.2/search-commands/rename),
[`percentile functions`](https://help.splunk.com/en/splunk-enterprise/search/spl-search-reference/9.4/statistical-and-charting-functions/aggregate-functions),
[`top`](https://help.splunk.com/en/splunk-enterprise/spl-search-reference/9.0/search-commands/top),
[`rare`](https://help.splunk.com/en/splunk-enterprise/spl-search-reference/9.4/search-commands/rare),
[`bin`](https://help.splunk.com/en/splunk-enterprise/search/spl-search-reference/10.4/search-commands/bin),
[`bucket`](https://help.splunk.com/en/splunk-enterprise/search/spl-search-reference/10.4/search-commands/bucket),
[`timechart`](https://help.splunk.com/en/splunk-enterprise/spl-search-reference/10.4/search-commands/timechart),
[`chart`](https://help.splunk.com/en/splunk-enterprise/search/spl-search-reference/10.4/search-commands/chart),
[`time modifiers`](https://help.splunk.com/en/splunk-enterprise/search/search-manual/10.4/specify-time-ranges/specify-time-modifiers-in-your-search),
ClickHouse's [`if` and conditional functions](https://clickhouse.com/docs/reference/functions/regular-functions/conditional-functions),
ClickHouse's [`lowerUTF8`, `upperUTF8`, `lengthUTF8`, and String functions](https://clickhouse.com/docs/sql-reference/functions/string-functions),
ClickHouse's [`toString` and type-conversion functions](https://clickhouse.com/docs/sql-reference/functions/type-conversion-functions),
ClickHouse's [`round` and rounding functions](https://clickhouse.com/docs/reference/functions/regular-functions/rounding-functions),
ClickHouse's [`match`, `like`, and string-search functions](https://clickhouse.com/docs/sql-reference/functions/string-search-functions),
ClickHouse's [`extractGroups`](https://clickhouse.com/docs/sql-reference/functions/string-search-functions),
and the [RE2 syntax reference](https://github.com/google/re2/wiki/Syntax)
documentation.
