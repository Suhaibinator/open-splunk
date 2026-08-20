# Open Splunk SPL compatibility contract v0.3

**Status:** historical executable delta; incorporated into cumulative runtime
identity `0.4`

**Compatibility version:** `0.3`

**Last updated:** August 12, 2026

**Rule inventory:** `internal/spl/testdata/compatibility-v0.3.json`

Version 0.3 incorporates every unchanged authored-search rule from
[`spl-compatibility-v0.1.md`](spl-compatibility-v0.1.md) and
[`spl-compatibility-v0.2.md`](spl-compatibility-v0.2.md). This document is the
normative delta for ten bounded single-relation commands. It does not widen the
Tier-1 calculated-field knowledge profile, which remains v0.1.

The command grammar is closed. A spelling or option not accepted below is not
an approximate compatibility feature: it is rejected before execution with a
source-located diagnostic. Parameters are bound separately from generated SQL.
Compiler-private fields never enter the public schema, paging cursor, export,
inspection literal inventory, or error text.

## Compatibility identity and shared boundaries

### `SPL-V03-PROFILE-001` — closed authored and knowledge profiles

Public authored searches use this complete command surface when the running
binary advertises `0.3` or a later cumulative identity. Runtime `0.4`
incorporates it unchanged. Tier-1 calculated fields remain on
`SPLExpressionV01`; an authored v0.3 suffix may consume their published outputs
without enabling v0.3 syntax inside the knowledge object.

Every admitted search snapshot records the exact authored compiler version.
Rebuild-based export, inspection, and search analysis fail closed when that
version differs from the running authored compiler. A retained sealed compiled
authority may be replayed only through its existing versioned execution path.

### `SPL-V03-ACTIVATION-001` — atomic advertisement

The cumulative v0.4 activation qualifies this complete ten-command surface
together with lookup support. The contract, inventory, migration guide, pinned
ClickHouse verticals, product verticals, and release readback bind to the exact
tagged revision under [`releasing.md`](releasing.md). The v0.3 names in this
historical regression contract identify when the behavior was introduced; the
current runtime exposes only product profile `0.4`.

Validation has no public search-job identifier. It may prove `addinfo` shape
with an explicit null SID placeholder, but only admitted execution may publish
the reserved immutable job ID.

### `SPL-V03-FIELDS-001` — exact command fields and private namespace

Every v0.3 command field is one exact unquoted field. Wildcards, single-quoted
command fields, calculated command arguments, empty names, invalid paths, and
the case-insensitive `__os_` compiler namespace are rejected. `makemv` and
`mvexpand` additionally reject names beginning with `_`; their first slice does
not rewrite canonical/internal fields.

The reserved event convenience payload named `fields` is ambiguous while an
event schema remains open. Commands reject it as an input or destination until
an exact upstream projection makes that public spelling unambiguous. A forged
AST or logical plan is revalidated at every trust boundary.

## Commands

### `SPL-V03-REGEX-001` — bounded row filtering

Accepted forms are:

```text
regex "pattern"
regex field="pattern"
regex field!="pattern"
```

The omitted field is `_raw`. The pattern is one quoted String literal compiled
with the shared RE2-compatible `match` profile. Matching is unanchored unless
the authored pattern anchors itself. The positive form keeps only present
values that match. The negative form also keeps a row when the field is missing
or explicit null.

`regex`, scalar `match`, and `rex` consume one compiler-authoritative shared
regex program budget, including retained knowledge preludes. Invalid RE2
syntax reports `SPL_UNSUPPORTED_REGEX`; source- or compiler-determined work
overflow reports `SPL_QUERY_TOO_COMPLEX` at the pattern.

### `SPL-V03-REVERSE-001` — complete established-order reversal

`reverse` accepts no arguments. It reverses the complete established relation
order, including private deterministic tie lineage, and changes no public
field, value, event identity, or row count. It works after `sort`, `head`,
`tail`, `dedup`, global or grouped aggregation, and a prior `reverse`.

Reversal is not a new `_time` sort. A later order-sensitive command observes
the reversed relation, and a later explicit sort establishes its own public
keys while retaining deterministic private identity ties.

### `SPL-V03-ACCUM-001` — shared running numeric sum

Accepted forms are `accum field` and `accum field AS output`. The default
replaces `field`. The explicit form writes `output`. Both are exact unquoted
fields.

The result is exactly the supported global, current-row-inclusive
`streamstats sum(field)` behavior in established pipeline order. It therefore
inherits that command's numeric eligibility, nullable Float64 result,
malformed semantic-value handling, 10,000-row ordered-input ceiling, and
atomic failure behavior. It does not introduce command-local coercion.

### `SPL-V03-STRCAT-001` — bounded shared concatenation

Accepted syntax is an optional leading `allrequired=true|false`, followed by
two through 32 source operands and one exact destination. Each source is an
exact unquoted field or quoted String literal. The option defaults to false and
must precede every operand.

With `allrequired=false`, missing, null, or a runtime-ineligible open Dynamic
source contributes an empty String. Fixed String and number sources use the
existing period-concatenation lexical conversion. A statically known Bool,
time, or multivalue source is rejected before execution; v0.3 does not weaken
the shared v0.1 type profile. With `allrequired=true`, the destination is
written only when every source converts to a present String; otherwise an
existing destination value is preserved and a previously absent destination
remains null. Output and aggregate query-wide operand byte/work ceilings are
the shared v0.1 concatenation ceilings.

### `SPL-V03-ADDINFO-001` — immutable admitted-search metadata

`addinfo` accepts no arguments and overwrites these four public fields on every
input row:

- `info_min_time`: resolved earliest boundary as Unix seconds;
- `info_max_time`: resolved latest boundary as Unix seconds;
- `info_search_time`: immutable admitted search-start time as Unix seconds;
- `info_sid`: the canonical reserved public search-job ID.

The three time values are non-null `Float64` Unix timestamps on admitted
execution. Subsecond precision is preserved when the admitted boundary or
search-start authority contains it; “seconds” does not require truncation to
an integer boundary. Validation may use an explicit null SID solely to prove
shape without fabricating an executable job identity.

Values come only from the sealed server snapshot. Authored shadow fields are
overwritten. ClickHouse wall-clock time and authored literals are never
authorities. ID reservation occurs before knowledge admission and is released
if admission fails before publication; snapshot rebuilds preserve the same ID.

### `SPL-V03-FILLNULL-001` — explicit String fill projection

Accepted forms are `fillnull field...` and
`fillnull value="string" field...`, with one through 64 distinct exact fields.
The default fill String is `"0"`; `value=` accepts exactly one quoted String
and must precede the fields.

For each listed field, missing or explicit null becomes the fill String.
Present empty String, zero, false, time, and present multivalue values are
unchanged. A present flattened object/container parent is preserved rather
than rewritten. Mixed input/fill domains publish through the existing Dynamic
representation. Cardinality and order are unchanged.

### `SPL-V03-ADDTOTALS-001` — row-only explicit numeric total

Accepted syntax has zero or more leading options followed by one through 64
distinct exact inputs. Options may occur once each and must precede the fields:

```text
row=true
col=false
fieldname=exact_output
```

The default output is `Total`. No other row/column/label or implicit-field form
is accepted. Fixed numeric and eligible open Dynamic numeric values contribute
only when finite. Missing, null, fixed String, nonnumeric Dynamic String, Bool,
time, multivalue, container, NaN, and infinity contribute zero. A malformed
recognized semantic tag fails atomically. An all-ineligible row yields numeric
zero. The published column has nullable Float64 schema even though this slice's
all-ineligible rule produces a non-null zero value. Input order and cardinality
are unchanged.

This explicit zero rule is an Open Splunk v0.3 compatibility decision for the
bounded row-only slice; it must not be inferred as support for Splunk column
totals or implicit numeric-field discovery.

### `SPL-V03-DELTA-001` — bounded ordered subtraction

Accepted forms are `delta field`, an optional `AS output`, and an optional
`p=integer`; `AS` and `p=` may appear in either order and at most once. `p`
defaults to 1 and is in the inclusive range 1 through 10,000. The default
destination is the literal public name `delta(field)`.

For each row, output is current value minus the value `p` established rows
earlier. A row without that predecessor, a missing/null operand, or an ordinary
ineligible open Dynamic value produces a present null Float64. Static
unsupported domains are rejected. Numeric normalization and exceptional
results are exactly v0.2 subtraction. The complete ordered input is limited to
10,000 rows; overflow and malformed semantic values fail atomically before
publication.

### `SPL-V03-MAKEMV-001` — optional typed String multivalue construction

Accepted syntax is optional leading `delim="nonempty"` and
`allowempty=true|false`, in either order and at most once, followed by exactly
one non-internal exact field. The default delimiter is one space,
`allowempty=false`, and a decoded delimiter is at most 1,024 UTF-8 bytes.

A present scalar String is split by the literal delimiter. With
`allowempty=false`, empty members are removed; with true they are retained.
Missing and explicit-null input remain public null, distinct from a present
empty multivalue `[]`. Present non-String, already-multivalue, invalid UTF-8,
or container input fails atomically. Output is a nullable typed List of String,
not a scalarized JSON String or Mixed value.

Hard ceilings are 1,000 members and 1 MiB member bytes per row, 100,000 members
and 8 MiB member bytes across the command result, and 64 MiB for the complete
retained public relation at that stage. A later filter or `head` cannot hide an
earlier breach.

### `SPL-V03-MVEXPAND-001` — controlled ordered row expansion

Accepted syntax is `mvexpand field` with optional final
`limit=0..1000`. Omission and authored zero select the hard maximum; a positive
limit selects at most that many members per input row and never bypasses any
hard ceiling. At most two `mvexpand` stages are admitted.

A String multivalue emits one copied row per selected member in member order;
an explicit null member emits one row with null. A present empty list emits no
rows. Missing or whole-cell null emits one row with null. A scalar String,
Number, Bool, or time value is preserved as one row. A Dynamic list is accepted
only when every member is String or null; numeric/Boolean members, nested lists,
objects, and other containers fail atomically. This deliberately remains a
nullable String-multivalue consumer, not a heterogeneous-list feature.

The hard ceilings are 1,000 source members per row, 10,000 emitted rows per
stage, 15,000 cumulative emitted rows across admitted stages, and 64 MiB of
canonical retained public-row bytes per stage. Counters survive projection,
rename, filter, calculated fields, and aggregation between two expansions.
Member ordinals extend both current order and deterministic tie lineage.

For both multivalue commands, a canonical retained public row is the UTF-8
output of ClickHouse `toJSONString` over a named tuple in public field order.
The sealed query enables named tuple elements, retains null tuple members,
serializes named tuples and maps as JSON objects, leaves `/` unescaped, and
disables JSON quoting of 64-bit integers/floats, decimals, and denormals. These
settings are compiler authority rather than ambient server configuration.
Before serialization, the nullable-List presence sidecar is reconstructed:
presence zero is JSON `null`, while presence one is the array (including
present `[]`). The pure-Go resource ledger is an independent limit model; the
pinned ClickHouse fixtures are authoritative for these exact runtime bytes.

## Shared execution contracts

### `SPL-V03-MULTIVALUE-TYPE-001` — nullable List transport

The compiler carries a sealed optional-multivalue descriptor mapping each
public List output to one private UInt8 presence sidecar. The executor validates
descriptor ordinals, schema names, native `Array(String)` values, and presence
bits before publishing anything. Presence zero requires an empty physical
array and publishes public null; presence one publishes the array, including a
present empty `[]`. Invalid headers, types, bits, descriptor collisions, or
payloads fail atomically and are never exposed as private columns.

Field catalog analysis reports the output as semantic List. Exact-value field
summary remains scalar-only and returns sanitized unsupported-value for a List
rather than scalarizing it. Export preserves public JSON array/null types.

### `SPL-V03-ORDER-001` — private durable relation lineage

`reverse`, `accum`, `delta`, and `mvexpand` consume the complete established
relation order. The compiler materializes authored order expressions and
private event identity ties before an order-sensitive stage. Projection,
extend, rename, aggregate, stream aggregate, sort, dedup, limit, and repeated
order-sensitive commands preserve or deliberately replace that lineage.

Private order columns use the reserved namespace, cannot collide with authored
fields, and are removed before executor schema validation, public paging,
export, inspection, and field analysis. Public cursors retain insertion order;
they do not serialize private lineage.

### `SPL-V03-LIMITS-001` — source and runtime resource accounting

All inherited source, token, command-count, expression, relational-depth (96),
generated-SQL, execution, result-row, and result-byte limits still apply.
Version 0.3 additionally enforces exact field-list, regex, concatenation,
arithmetic, lag, delimiter, member, expanded-row, and retained-byte counters at
the earliest trusted boundary. Compiler traversal recharges parser-derived
evidence so forged plans and retained knowledge composition cannot evade it.

Source-determined excess returns `SPL_QUERY_TOO_COMPLEX` at the token or
command that crosses the limit. Data-determined hard limits use stable private
backend markers mapped to the public execution-limit category with surrounding
backend SQL, arguments, values, and exception text redacted.

### `SPL-V03-ATOMIC-001` — no observable prefix on hard failure

Any lowering that can raise a deterministic malformed-value or v0.3 resource
marker seals `RequiresAtomicResult`. Query execution buffers and validates the
complete backend stream. The search-job sink then preflights schema, row,
capacity, and byte ceilings and commits all rows under one lock. Running
preview cannot observe a staged prefix, and an atomic job never converts sink
overflow into successful truncation.

On cancellation, marker failure, malformed optional-list transport, or sink
failure, public schema, retained rows, row count, and result bytes remain zero.
Ordinary non-atomic searches retain their established streaming/truncation
contract.

### `SPL-V03-SURFACES-001` — one immutable product vertical

Validation, admitted jobs, knowledge-prelude composition, saved-search launch,
history rerun, paging, export, inspection, field catalog, timeline, suggestions,
and browser syntax all parse or rebuild the same authored v0.3 source. A saved
search executes the persisted definition identified by its trusted object ID,
not caller-supplied SPL. A history rerun reconstructs the retained definition.

Timeline admits event-preserving v0.3 stages but rejects any command that
replaces `_time`; `mvexpand` is timeline-ineligible because it duplicates
events. Inspection reports exact reads/writes while redacting literal values
and private columns. Compatibility-version mismatch fails before reparse or
recompile.

### Compatibility decisions without a licensed Splunk differential receipt

The following ambiguous behaviors are deliberate Open Splunk v0.3 decisions,
not claims that an internal reference model is an independent Splunk oracle.
They are normative until a later, separately accepted compatibility revision
supersedes them:

- `SPL-V03-STRCAT-001`: missing, null, and runtime-ineligible open Dynamic
  values contribute empty when `allrequired=false`; `allrequired=true`
  preserves the prior destination when any source is unavailable.
- `SPL-V03-ADDTOTALS-001`: every ineligible input contributes zero and an
  all-ineligible row publishes non-null numeric zero in a nullable Float64
  column.
- `SPL-V03-DELTA-001`: the first `p` rows, and rows with a missing/null or
  ordinary ineligible operand, publish a present null Float64.
- `SPL-V03-MAKEMV-001`: missing and whole-cell null remain null; present
  non-String and already-multivalue input fail atomically; splitting a present
  empty String with `allowempty=false` produces present `[]`.
- `SPL-V03-MVEXPAND-001`: missing and whole-cell null each emit one null row;
  scalar String/Number/Bool/time emits one preserved row; present `[]` emits
  none; explicit null members emit null rows; unsupported containers or member
  domains fail atomically.

The pure-Go reference model and pinned ClickHouse/product fixtures verify that
the implementation agrees with these declared decisions. They do not replace
the external-oracle provenance that would be required to relabel one as exact
Splunk parity.

### `SPL-V03-DIAGNOSTICS-001` — stable fail-closed errors

Unsupported command forms use command-specific `SPL_UNSUPPORTED_*_SYNTAX`
diagnostics; missing exact fields use `SPL_EXPECTED_FIELD`; private fields use
`SPL_RESERVED_FIELD`; unsupported static numeric/multivalue domains use stable
unsupported-value/type diagnostics; and deterministic excess uses
`SPL_QUERY_TOO_COMPLEX`. Ranges are half-open UTF-8 byte ranges over the
authored source and identify the smallest authoritative token when available.

Runtime ClickHouse code 395 messages are classified only by exact stable
markers. Unsupported multivalue values map to the public unsupported-value
category; member, byte, stage, query, and retained limits map to the execution-
limit category. Public errors and logs never include SPL literals, field
values, bound arguments, generated SQL, or raw backend exception text.

## Deliberately unsupported v0.3 forms

Version 0.3 does not add lookups, subsearches, joins, generating commands,
external calls, mutation, implicit schema-wide field selection, wildcard
command fields, quoted command fields, regex tokenizers, heterogeneous
multivalue construction, column totals, summary rows, warning-based expansion
truncation, or unbounded row generation. See
[`spl-compatibility-v0.3-migration.md`](spl-compatibility-v0.3-migration.md)
for accepted replacements and rollout guidance.
