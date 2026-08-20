# Open Splunk SPL compatibility contract v0.2

**Status:** historical executable delta; incorporated into cumulative runtime
identity `0.4`

**Compatibility version:** `0.2`

**Last updated:** August 12, 2026

**Rule inventory:** `internal/spl/testdata/compatibility-v0.2.json`

Version 0.2 incorporates every unchanged rule in
[`spl-compatibility-v0.1.md`](spl-compatibility-v0.1.md). This document is the
normative delta for authored searches, including the explicitly identified
command transition below. It does not widen the Tier-1
knowledge-object compiler, whose calculated-field profile remains v0.1.
Operators upgrading stored searches should use the
[`v0.2 migration and read-only audit guide`](spl-compatibility-v0.2-migration.md).
The accepted v0.2 runtime is retained in repository history. Current release
qualification follows [`releasing.md`](releasing.md).

Accepted source is parsed, planned, and compiled as one complete query. An
error never publishes or executes a supported prefix. Tenant, index, event-time,
index-time, visibility, and knowledge-snapshot authority are immutable inputs
to compilation and cannot be changed by an expression.

The rule IDs in this document and in the machine-readable inventory are an
exact set. Repository tests fail when either side adds or removes an ID without
updating the other.

## Compatibility identity and profiles

### `SPL-V02-PROFILE-001` — closed expression profiles

Production-authored searches use the closed `SPLExpressionV02` expression
profile. Standalone Tier-1 calculated fields use `SPLExpressionV01` and continue
to reject v0.2 arithmetic, grouping, membership, and quoted fields. Callers do
not select individual syntax flags.

Every admitted job stores its authored source and the current authored-search
compatibility identity. History rerun reparses source with the current profile;
ASTs and generated SQL are never persisted. Active v0.1 knowledge snapshots
retain their compiler version and digest.

### `SPL-V02-ACTIVATION-001` — atomic advertisement

The accepted v0.2 runtime is application `0.1.0`, retained by immutable release
tag and repository history. All parser, planner, compiler, executor, job,
export, inspection, history, saved-search, field-analysis, browser, audit,
integration, and release surfaces report the same identity. The former
two-revision evidence workflow has been retired; current publication uses the
exact-tag and successful-CI gate in [`releasing.md`](releasing.md).

## Grammar

### `SPL-V02-GRAMMAR-001` — authored eval grammar

```text
where-expression       := or-expression
or-expression          := and-expression (OR and-expression)*
and-expression         := not-expression (AND not-expression)*
not-expression         := NOT not-expression | predicate-primary
predicate-primary      := "(" where-expression ")"
                        | scalar-expression comparison-op scalar-expression
                        | scalar-expression membership-op membership-list
                        | boolean-scalar

comparison-op          := "=" | "==" | "!=" | "<" | "<=" | ">" | ">="
membership-op          := IN | NOT IN
membership-list        := "(" scalar-expression
                            ("," scalar-expression){0,31} ")"

scalar-expression      := concatenation-expression
concatenation-expression
                       := additive-expression
                          ("." additive-expression){0,31}
additive-expression    := multiplicative-expression
                          (("+" | "-") multiplicative-expression)*
multiplicative-expression
                       := unary-expression
                          (("*" | "/" | "%") unary-expression)*
unary-expression       := ("+" | "-") unary-expression
                        | scalar-primary
scalar-primary         := typed-literal
                        | field-reference
                        | supported-function-call
                        | supported-if-call
                        | supported-case-call
                        | "(" scalar-expression ")"

field-reference        := exact-unquoted-field | single-quoted-field
```

The grammar is shared by `eval`, `where`, `if`, `case`, `count(eval(...))`, and
all existing eval-predicate consumers. Base search keeps its separate v0.1
grammar and does not accept membership or arithmetic.

### `SPL-V02-PRECEDENCE-001` — precedence and associativity

Precedence from tightest to loosest is: grouping/function calls; right-
associative unary `+` and `-`; left-associative `*`, `/`, `%`;
left-associative binary `+`, `-`; flattened period concatenation; comparison
or membership; `NOT`; `AND`; `OR`. `=` and `==` are equality synonyms in eval
predicates.

The pinned examples are:

```text
1 + 2 * 3          = 7
(1 + 2) * 3        = 9
20 / 5 / 2         = 2
- - 2              = 2
"value=" . 1 + 2  = "value=3"
1 + 2 . "ms"      = "3ms"
```

### `SPL-V02-GROUPING-001` — scalar/Boolean disambiguation

Parentheses may group either a scalar or a Boolean expression. Parse category
is determined from syntax and the enclosing grammar, never from event schema.
A parenthesized scalar does not become truthy. A Boolean expression cannot be
assigned directly by `eval` or passed to a non-Boolean scalar consumer.

## Lexing and quoted fields

### `SPL-V02-LEXER-001` — operators and punctuation transition

In authored scalar-expression positions, `+`, `-`, `*`, `/`, and `%` are
operators even without whitespace. `==` is one comparison token. Numeric signs
and exponent signs remain part of numeric-literal recognition where the grammar
requires them. Base-search and command-specific exact-field tokenization retain
their v0.1 behavior.

An old scalar field containing an operator character must be single quoted.
An unquoted `identifier-identifier` spelling is necessarily parsed as
subtraction; the parser cannot infer whether an older saved search intended a
single field. The read-only compatibility audit identifies those stored-source
candidates and migration guidance shows the quoted spelling. Source stored in
saved searches or history is never rewritten.

### `SPL-V02-QUOTED-FIELD-001` — exact field references

Single quotes denote an exact scalar field reference and may also quote an
`eval` destination. The command-specific exact-name slots for `stats`
aggregate inputs (including `sparkline` inputs), `stats BY`, and `table` use
the same decoder and validation:

```spl
| eval normalized='request-bytes' / 1024
| where 'HTTP Status' IN (500, 502, 503)
| eval 'error-rate'=(errors / requests) * 100
| stats avg('request-bytes') AS "Mean Bytes" BY 'HTTP Status'
| table 'HTTP Status', 'Mean Bytes'
```

The decoder accepts UTF-8 plus only `\\` and `\'`. It rejects an empty name,
invalid UTF-8, controls, leading/trailing Unicode whitespace, wildcards,
compiler-private names, invalid dynamic paths, and names outside the existing
field-path byte/segment limits. A quoted dotted name retains the existing SPL
path interpretation; quoting is not a literal-dot escape.

Quote decoding happens before the existing field-path decoder. Consequently a
literal backslash in a path crosses both escape layers: the authored quoted
payload uses four backslashes so quote decoding yields the two-backslash path
escape consumed by the field-path decoder.

Quoted fields remain excluded from `eventstats` and `streamstats` aggregate or
`BY` slots, and from `fields`, `sort`, `rename`, `rex`, `spath`, `bin`, `top`,
`rare`, `chart`, and `timechart`. A single-quoted `stats` input or group is
always one exact field, never a wildcard. In scalar expressions, double quotes
remain String literals and backticks are invalid. The `stats AS "literal
output"` command grammar is a separate exact-output syntax; a later scalar or
`table` reference uses the single-quoted spelling. The complete bounded
`stats` name surface is recorded in
[`stats-command-parity.md`](stats-command-parity.md). Unterminated single and
double quotes have distinct stable diagnostics and point at the opening quote.

## Statistical grouping

### `SPL-V02-STATS-BY-MULTIVALUE-001` — bounded multivalue grouping

Version 0.2 supersedes the v0.1 scalar-only `stats BY` boundary. A resolved BY
input may be a fixed scalar, a fixed `Array(String)`, or a raw Dynamic scalar
or top-level multivalue whose non-null members are individually supported
scalar values. Each admitted member contributes one group occurrence. Member
duplicates are preserved by default; `dedup_splitvals=true` removes duplicates
within each BY field before grouping. Multiple multivalue BY fields form their
Cartesian product in authored field order. A fixed `Array(String)` member
retains its raw bytes at the typed result boundary: valid UTF-8 publishes as a
String cell and invalid UTF-8 publishes byte-for-byte as Bytes, never as a
malformed String. Textual comparisons and text functions consider only the
valid-UTF-8 members.

Missing, whole-cell null, null members, and empty arrays contribute no group.
Objects, nested arrays or objects, and any other unsupported member fail the
complete job through the sanitized unsupported-value boundary before a public
group is published. Validation is scoped to the complete admitted input and
cannot be hidden by another missing BY key or by downstream `search`, `where`,
projection, limiting, or transforming commands. Every query that can execute
multivalue grouping is published atomically, whether its input is a fixed
`Array(String)` or Dynamic: the executor consumes and closes the complete
bounded backend stream before invoking its result sink, and the production job
manager keeps every sink write private until one commit. Dynamic grouping is
also atomic because an unsupported runtime shape cannot be ruled out earlier.
Preview and paging remain unavailable on failure. For these atomic paths,
executor or job result row, byte, page, and retained-memory ceilings are hard
failures rather than successful truncation; statically scalar-only grouping
retains the ordinary result-retention policy. The executor's private atomic
buffer has a fixed 128 MiB ceiling, independent of the backend query byte
limit. Manager staging remains bounded by its configured row, page-byte, and
global retained-memory ceilings, so the handoff peak is bounded by that 128 MiB
private buffer plus the manager reservation; the backend applies its own
configured execution limits independently.

Expansion is capped at 10,000 Cartesian combinations per source event after
null filtering and optional per-field deduplication. The overflow sentinel is
validated before the first `ARRAY JOIN`; the implementation never truncates or
publishes a supported prefix. This rule normatively adopts the bounded
`by-multivalue` surface recorded in
[`stats-command-parity.md`](stats-command-parity.md); that inventory remains
descriptive for every surface not explicitly adopted by a compatibility rule.

## Arithmetic

### `SPL-V02-ARITHMETIC-TYPE-001` — operators and result type

Unary `+`/`-` and binary `+`, `-`, `*`, `/`, `%` are numeric. Every result is a
present nullable `Float64`, including integer-literal arithmetic. `+` never
concatenates Strings; period is the only concatenation operator.

Eligible fixed inputs are integers, Float64, numeric literals, valid
`decimal/v1`, and outputs already proven numeric by supported conversion,
rounding, time-conversion, numeric aggregate, `bin`, or arithmetic operations.
Open-schema Dynamic inputs additionally admit integer/float scalars and bounded
numeric-looking Strings. Canonical `_time` and `_indextime`, Bool, binary,
objects, lists, and fixed multivalue values are not implicit numbers.

A fixed String is not arithmetic input even when its text looks numeric. This
includes quoted literals such as `"12"` and expressions statically proven to
return String. Runtime String conversion is limited to an otherwise open
Dynamic value; callers with a fixed String use `tonumber` explicitly. This
keeps planning deterministic and prevents a fixed type from changing behavior
because the same value was stored in a Dynamic event field.

Statically known unsupported types produce
`SPL_UNSUPPORTED_ARITHMETIC_VALUE_TYPE` at the operand range. An open Dynamic
unsupported ordinary value produces null for that row.

### `SPL-V02-ARITHMETIC-NULL-001` — null and conversion

An arithmetic result is null if any required operand is missing, explicit
null, an empty or nonnumeric String, Bool, binary, ordinary object, list,
unsupported tag, or over-budget numeric spelling. A failed result is an
explicit present null at an `eval` destination.

Dynamic numeric String input is limited to 4 KiB and uses the repository's
whole-string decimal grammar. Leading sign, fraction, and exponent forms are
accepted. Whitespace, textual `NaN`/`Inf`, incomplete exponents, invalid UTF-8,
or a value outside the bounded grammar yields null. Conversion to Float64 may
lose low-order precision beyond `2^53`.

A recognized semantic tag with a malformed, oversized, or inconsistent
payload fails the job atomically through the sanitized unsupported-value
boundary. Diagnostics, logs, and audit records never include payload text.

### `SPL-V02-ARITHMETIC-EXCEPTION-001` — exceptional results

Division and remainder by positive or negative zero return null and never a
backend exception. Addition, subtraction, multiplication, unary negation, and
remainder normalize a published negative zero to positive zero. Finite
overflow may publish infinity and other IEEE invalid operations may publish
NaN. Computed non-finite values use the existing Double transport.

Remainder uses truncated-division sign behavior. ClickHouse backend defaults
are not the public contract; lowering uses guarded or `OrNull` operations.

For finite operands, remainder has the dividend's sign: `-5%2=-1`,
`5%-2=1`, and `-5%-2=-1`. A finite dividend modulo either infinity equals the
dividend before the required negative-zero publication normalization. An
infinite dividend modulo a finite nonzero divisor, or any NaN operand, produces
NaN. Division is the one operation that does not normalize a non-divisor
negative-zero result, so `0/-2` publishes negative zero.

Arithmetic-result comparisons use an explicit IEEE predicate: equality with
NaN is false, including NaN against NaN; inequality with NaN is true when both
operands are present; and every ordered comparison involving NaN is false.
Membership reuses that equality rule, so NaN never matches a candidate.

### `SPL-V02-ARITHMETIC-EVALUATION-001` — bounded evaluation

Operands compile in authored left-to-right order and occur at most once in the
physical action graph per row. `if` and `case` retain short-circuit semantics;
an unselected branch cannot fail a row. Arithmetic is row-preserving and does
not change event identity, ordering, cardinality, relation kind, or time
lineage unless `_time` itself is assigned.

Version 0.2 does not add general conditional numeric type unification.
Existing `if`, `case`, and `coalesce` branch compatibility remains in force.
Because arithmetic returns Float64, a branch paired with arithmetic uses a
Float64 alternative such as `0.0`; `if(condition, a+b, 0)` remains a static
branch-type error rather than silently widening the integer literal.

## Membership

### `SPL-V02-MEMBERSHIP-001` — accepted forms and equality

The case-insensitive function and infix forms are accepted in eval-predicate
positions:

```spl
| where in(status, 400, 401, 403)
| where status IN (400, 401, 403)
| where status NOT IN (200, 201, 204)
| eval class=if(environment IN ("production", "staging"), "managed", "other")
| stats count(eval(status IN (500, 502, 503))) AS server_errors
```

One through 32 scalar candidates are required. Candidates are expressions and
are evaluated once in source order. Membership reuses existing eval equality:
String matching is case-sensitive, numeric matching follows the fixed/Dynamic
comparison contract, candidates are never wildcard patterns, and backend
`IN` common-type coercion is not used. The left value is materialized once.

Base-search `field IN (...)`, wildcard membership, subsearch membership, and
multivalue membership remain unsupported.

All candidates are validated/evaluated for runtime unsupported-value failure,
even after an earlier comparison is true. This preserves whole-job atomic
malformed-tag detection and is distinct from the Boolean result rule below.
The membership Boolean itself is not a scalar and cannot be explicitly
compared to `true` or `false` in v0.2; use it directly in a Boolean consumer.

### `SPL-V02-MEMBERSHIP-NULL-001` — three-valued result

Membership is true if any candidate comparison is true. Otherwise it is null
if the input or any comparison is null; otherwise it is false. `NOT IN` is
exact three-valued negation. `where` and conditional counts retain only true;
`if`/`case` use their existing null-predicate behavior.

Membership is Boolean and cannot be assigned directly by `eval`. It may be
consumed by `where`, `if`, `case`, `count(eval(...))`, or Boolean composition.
It cannot be compared to a Boolean literal.

## Budgets, validation, and security

### `SPL-V02-LIMITS-001` — structural limits

All v0.1 limits remain. Version 0.2 additionally enforces:

| Resource | Limit |
| --- | ---: |
| arithmetic operator occurrences per query | 256 |
| unary operator chain | 32 |
| membership candidates per occurrence | 32 |
| membership candidates per query | 256 |
| Dynamic numeric String | 4 KiB |
| one arithmetic or membership compiled node | 64 KiB SQL |
| whole generated query | 256 KiB SQL |

Scalar nesting remains 32, eval/where predicate leaves 32, syntax tokens 1,024,
pipeline commands 64, and the dependency path 96. A membership counts as one
predicate leaf; its candidates and nested arithmetic are charged separately.
The 32-level scalar limit measures authored grouping/function recursion, not
the iterative left spine of an otherwise legal 256-operator chain; unary
recursion has its separate 32 limit.
Parser, planner, and compiler derive and validate their own trusted counters.
Cycles, typed nils, invalid enums/ranges, oversized slices, and forged trees
fail closed with no SQL publication.

### `SPL-V02-SECURITY-001` — authority and SQL safety

All values remain bound arguments and all field identifiers are emitted only
through compiler-owned quoting. No expression can author a ClickHouse function,
identifier, type, setting, SQL fragment, external call, dictionary access,
file access, mutation, second event scan, or row expansion. Arithmetic and
membership add no row-expanding `ARRAY JOIN`. An existing singleton
materialization fence or v0.1 knowledge prelude may retain its already-proven
cardinality-preserving join; acceptance compares the action graph and row count
to the same v0.1 pipeline without the v0.2 expression.
Quoted names pass the same canonical/private-name checks as unquoted names.
Cancellation, deadlines, memory, scan, group, result, query-size, and backend
depth limits remain independent and are not raised by expression budgets.

## Product composition

### `SPL-V02-SURFACES-001` — one supported surface

Validation, creation, paging, inspection, saved-search execution, history,
rerun, export, field discovery, field summary, and browser execution all parse
and compile the same authored v0.2 source. Arithmetic fields are fixed nullable
Double downstream inputs for filtering, projection, sorting, deduplication,
aggregation, `eventstats`, `streamstats`, `bin`, `chart`, and `timechart` under
those commands' existing rules.

The browser scans single quotes without treating inner pipes, commas, or
operators as structure; highlights operators only outside quotes; offers
membership completion in predicate contexts; quotes special-character field
completions; preserves UTF-8/UTF-16 diagnostic offsets; and treats server
diagnostics as authoritative.

### `SPL-V02-AUDIT-001` — read-only punctuation audit

The compatibility audit accepts an explicit control-database path and scans
repository fixtures and saved-search source for newly reserved operator
characters in scalar positions. It reports object IDs and source locations,
never SPL text or field values, and performs no mutation. Invocation,
interpretation, and migration steps are documented in the
[`v0.2 migration and read-only audit guide`](spl-compatibility-v0.2-migration.md).

## Diagnostics

### `SPL-V02-DIAGNOSTICS-001` — stable failures

The smallest responsible authored range is reported. The minimum stable
families are:

| Failure | Code |
| --- | --- |
| unterminated single-quoted field | `SPL_UNTERMINATED_FIELD_QUOTE` |
| unsupported arithmetic syntax | `SPL_UNSUPPORTED_ARITHMETIC_SYNTAX` |
| unsupported fixed arithmetic type | `SPL_UNSUPPORTED_ARITHMETIC_VALUE_TYPE` |
| malformed membership | `SPL_UNSUPPORTED_MEMBERSHIP_SYNTAX` |
| unsupported fixed membership type | `SPL_UNSUPPORTED_MEMBERSHIP_VALUE_TYPE` |
| direct Boolean assignment | `SPL_UNSUPPORTED_EVAL_EXPRESSION` |
| structural/SQL overflow | `SPL_QUERY_TOO_COMPLEX` |
| malformed runtime semantic scalar | sanitized unsupported-value job failure |

Suggestions are static, bounded, and payload-free. They may recommend period
concatenation, a quoted field spelling, explicit time conversion, or consuming
membership inside a Boolean-capable function.

## Source provenance and deliberate deviations

The behavior above was frozen from the following primary documentation as
retrieved August 11, 2026:

- Splunk Enterprise 10.2 `eval`, last updated January 9, 2026, documents the
  arithmetic operator family, period concatenation, and `=`/`==` synonym:
  <https://help.splunk.com/en/splunk-enterprise/spl-search-reference/10.2/search-commands/eval>
- Splunk Enterprise 10.0 expression types, last updated July 18, 2025, shows
  single-quoted field names and unspaced arithmetic:
  <https://help.splunk.com/en/splunk-enterprise/search/search-manual/10.0/expressions-and-predicates/types-of-expressions>
- Splunk Enterprise 10.4 comparison functions, last updated May 14, 2026,
  documents function/infix `in` and the Boolean-assignment restriction:
  <https://help.splunk.com/en/splunk-enterprise/spl-search-reference/10.4/evaluation-functions/comparison-and-conditional-functions>
- ClickHouse arithmetic functions document `divideOrNull` and `moduloOrNull`:
  <https://clickhouse.com/docs/reference/functions/regular-functions/arithmetic-functions>

Open Splunk deliberately does not implement Splunk's String `+` overload,
general Boolean assignment, XOR, infix LIKE, exact/decimal arithmetic,
canonical-time arithmetic, multivalue arithmetic, or base-search membership in
v0.2. Null membership, malformed-tag behavior, non-finite publication, and
single-evaluation guarantees are explicit Open Splunk rules until a licensed
Splunk differential oracle records otherwise.
