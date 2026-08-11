# Open Splunk SPL expansion plan: compatibility v0.2 expression foundation

**Status:** completed and acceptance-verified on August 11, 2026
**Date:** August 10, 2026
**Selected compatibility target:** bounded SPL1 arithmetic, scalar grouping,
quoted scalar field references, and eval-language membership
**Current prerequisite:** SPL compatibility `0.1` and Tier-1 knowledge objects
are implemented and acceptance-verified
**Estimated effort:** 11–17 engineer-weeks, excluding access to and operation of
a licensed Splunk differential environment

> **Decision:** the next SPL expansion is the expression foundation described
> in this document. It does not begin lookups, subsearches, joins, or another
> isolated aggregate. Those features depend on a more complete scalar and
> predicate language and remain later programs.

## Executive summary

Open Splunk already has the right search architecture: a bounded lexer and
source-located parser, a semantic planner, a typed logical pipeline, a
parameterized ClickHouse compiler, immutable search-job authority, completed-job
analysis, inspection, history, export, suggestions, and an executable
compatibility contract. The current language is useful for operational log
search, but its scalar grammar is deliberately narrow. It accepts scalar
primaries and SPL1 period concatenation, while arithmetic, scalar grouping,
quoted field references, and membership remain unsupported.

That missing expression foundation is now the highest-leverage bounded SPL
unit. It unlocks ordinary searches such as:

```spl
index=api
| eval latency_seconds=duration_ms / 1000
| where latency_seconds >= 0.5
| stats avg(latency_seconds) AS mean_seconds BY route
```

```spl
index=api
| where status IN (400, 401, 403, 404, 500, 502, 503)
| eval weighted_cost=(request_bytes + response_bytes) * retry_count
| table _time route status weighted_cost
```

```spl
index=api
| eval normalized=- delta, ratio=(used / capacity) * 100
| where in(environment, "production", "staging") AND ratio > 90
```

The release adds one coherent expression grammar across authored `eval`,
`where`, `if`, `case`, `count(eval(...))`, and every existing predicate consumer.
It adds:

- single-quoted exact field references in scalar expressions and `eval`
  destinations;
- parenthesized scalar expressions;
- unary `+` and `-`;
- binary `*`, `/`, `%`, `+`, and `-` with explicit precedence;
- `==` as the eval-language equality synonym;
- bounded `in(value, candidate, ...)` and
  `value [NOT] IN (candidate, ...)` predicates; and
- one typed arithmetic lowering that is shared by authored SPL and every
  downstream consumer.

The release deliberately does **not** add string concatenation through `+`,
general Boolean assignment, `XOR`, infix `LIKE`, exact/decimal arithmetic,
multivalue arithmetic, base-search `field IN (...)`, wildcard membership,
lookups, subsearches, or multiple input relations. Unsupported forms continue
to fail before planning or execution; a supported prefix is never run as a
partial search.

This plan treats compatibility as an end-to-end product contract. Parser-only
support is not completion. Every accepted form must survive planning,
knowledge-prelude composition, ClickHouse compilation, search execution,
validation, inspection, saved-search and history rerun, export, completed-job
field analysis, browser editing, resource enforcement, cancellation, and
adversarial testing before the server advertises compatibility `0.2`.

## Relationship to the existing plans and contracts

This document is subordinate to, and does not replace:

- [`product-architecture-plan.md`](product-architecture-plan.md), which owns
  the product shape and parser/planner/compiler separation;
- [`spl-compatibility-v0.1.md`](spl-compatibility-v0.1.md), which remains the
  normative contract for the currently implemented language;
- [`knowledge-objects-plan.md`](knowledge-objects-plan.md), which owns exact
  CSV lookups, event types, tags, macros, workflow actions, data models, and
  acceleration;
- [`knowledge-compatibility-v0.1.md`](knowledge-compatibility-v0.1.md), which
  continues to govern active Tier-1 field knowledge; and
- [`backend-checkpoint.md`](backend-checkpoint.md), which is implementation
  history and handoff evidence rather than the future SPL roadmap.

The implementation must add a normative
`docs/spl-compatibility-v0.2.md` before the first v0.2 syntax is enabled in the
runtime. That contract incorporates every unchanged v0.1 rule by reference and
fully specifies the changed expression grammar, type behavior, null behavior,
resource limits, diagnostics, and deliberate deviations. The v0.1 document is
not rewritten to make historical searches appear to have supported v0.2.

The machine-readable compatibility corpus for this release lives at
`internal/spl/testdata/compatibility-v0.2.json`. The human contract and the
fixture must carry the same rule IDs. A test fails if either inventory has a
rule that the other does not.

## Why this is the next chunk

### User value

Arithmetic and membership occur throughout practical operational SPL:

- convert milliseconds, microseconds, bytes, and percentages;
- calculate rates, ratios, deltas, and weighted scores;
- normalize signed measurements;
- filter one field against a short status, environment, service, or outcome
  set;
- use calculated values in `stats`, `eventstats`, `streamstats`, `chart`, and
  `timechart`; and
- express concise conditional aggregates without repeating long OR chains.

These capabilities improve the commands already implemented instead of adding
another isolated terminal command.

### Architectural leverage

The current parser has separate scalar and Boolean ASTs, but scalar parsing has
only one binary precedence level: period concatenation over scalar primaries.
Adding a disciplined expression parser now provides the foundation later work
needs:

- multivalue functions need grouped and nested scalar arguments;
- lookup keys need ordinary calculated expressions;
- subsearch predicates need a stable outer expression grammar;
- broader `stats(eval(...))` forms need numeric expressions;
- `foreach` and `map` need source-located expression expansion; and
- macro expansion must target a grammar whose precedence is already fixed.

### Bounded risk

This chunk remains a single-relation, row-preserving change. It adds no storage
schema, network protocol, external asset, child search, row fan-out, join,
mutation, or new search scheduler. Every expression can lower into one
parameterized ClickHouse projection or predicate over the current pipeline
relation.

## Goals

The v0.2 expression program must:

1. implement a documented SPL1 expression precedence without weakening base
   search precedence;
2. preserve exact source ranges and deterministic diagnostics for every new
   node and operator;
3. give arithmetic one explicit numeric, null, non-finite, and unsupported-type
   contract rather than inheriting ClickHouse coercion;
4. give membership the same equality semantics as existing `where`
   comparisons;
5. evaluate each authored operand once per row even when Dynamic type
   normalization is required;
6. preserve immutable tenant, index, time, index-time, visibility, and
   knowledge-snapshot scope;
7. maintain linear parser, planner, compiler, and generated-SQL work under
   explicit structural budgets;
8. expose the same supported surface through validation, suggestions,
   inspection, history, export, field analysis, and the browser editor;
9. keep existing v0.1 searches valid except for the explicit pre-1.0 unquoted
   operator-character boundary described below; and
10. advertise compatibility `0.2` only after the complete production vertical
    passes.

## Non-goals

The following remain outside this release:

- SPL2 syntax or a claim of SPL2 compatibility;
- string concatenation through `+`; SPL1 period concatenation remains the only
  supported string-concatenation operator;
- exponentiation, bitwise operators, shifts, integer division, or an `exact()`
  arithmetic mode;
- implicit arithmetic over canonical timestamps or durations;
- arithmetic over fixed or Dynamic multivalue values;
- vectorized member-wise arithmetic;
- Boolean results as direct `eval` assignment values;
- general scalar truthiness;
- `XOR`, infix `LIKE`, `BETWEEN`, `IS NULL`, or a ternary operator;
- base-search `field IN (...)`, including its wildcard and search-command
  comparison behavior;
- `IN` against subsearch results;
- calculated wildcard membership or CIDR membership;
- `mvappend`, `mvindex`, `mvfilter`, `mvmap`, `makemv`, `mvexpand`, or other
  multivalue expansion;
- quoted field names in aggregate arguments, `BY`, `fields`, `table`, `sort`,
  `rename`, `rex`, `spath`, `bin`, `top`, `rare`, `chart`, or `timechart`;
- broadening existing aggregate arity or command options;
- lookups, `inputlookup`, `outputlookup`, event types, tags, or macros;
- subsearches, `join`, `append`, `appendpipe`, `union`, `multisearch`, `map`, or
  multiple logical roots;
- `transaction`, data models, acceleration, or `tstats`; and
- scripts, external calls, or arbitrary custom commands.

Each excluded form must retain a source-located diagnostic. Parser acceptance
must never be used as a placeholder for a missing planner or compiler.

## Compatibility target and release identity

### SPL dialect

The release targets the SPL1 eval-expression behavior documented for Splunk
Enterprise 10.2–10.4, constrained by the explicit Open Splunk rules in this
plan and the v0.2 compatibility contract. SPL2 spellings are not accepted
merely because an SPL2 reference describes similar behavior.

Where public Splunk documentation is ambiguous, the implementation must use
one of:

1. a result from a legally available licensed Splunk differential oracle;
2. an explicit Open Splunk v0.2 rule with a permanent regression fixture; or
3. an unsupported diagnostic.

ClickHouse behavior alone is never a compatibility oracle.

### Version advertisement

During development the server retains its development compatibility identity.
The production bootstrap, job journal, inspection, history, and export metadata
change to `0.2` in one final activation change only after all acceptance gates
pass. Partial branches must not advertise `0.2`.

Every admitted job captures the compatibility version with its source SPL. A
retained history rerun uses the current parser and records the new job's current
version; it never reuses an old AST or generated SQL. If a v0.1 source becomes
invalid under the explicit operator-character transition, rerun returns the
new source-located diagnostic without creating a partially planned job.

### Pre-1.0 punctuation transition

The current development lexer permits several arithmetic characters inside an
unquoted scalar field token. SPL requires special-character field references
to be quoted. Supporting ordinary no-whitespace expressions such as
`bytes/1024` therefore requires reserving `+`, `-`, `*`, `/`, and `%` as
expression operators.

Version 0.2 makes that pre-1.0 grammar correction:

- a scalar field containing one of those characters must be single-quoted;
- the parser reports an exact diagnostic suggesting the quoted spelling when
  the old tokenization can be identified safely;
- no stored SPL is automatically rewritten;
- saved searches and history keep their authored source unchanged;
- opening an affected saved search shows the v0.2 diagnostic before run; and
- the release notes call out the transition with examples.

This transition is limited to scalar-expression positions. Existing base-search
field-path and value tokenization changes only where the new v0.2 base-search
contract explicitly says so; this release does not add base-search arithmetic.

Before activation, a read-only compatibility audit must scan repository-owned
fixtures and a supplied control database for saved searches whose scalar
expressions contain newly reserved characters. The audit reports object IDs and
source locations but never logs SPL text. Active Tier-1 calculated fields are
not reinterpreted because their compatibility boundary remains v0.1 in this
release.

## Normative expression grammar

The final v0.2 contract must publish an executable grammar equivalent to the
following. This is descriptive notation, not permission to generate a parser
without the semantic checks in later sections.

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

The parser may use a Pratt parser, precedence climbing, or recursive descent,
but the resulting source ranges, diagnostics, limits, and AST must be identical.
Parser strategy is not observable compatibility.

### Parenthesis disambiguation

Parentheses may contain a scalar or Boolean expression:

```spl
| eval kib=(bytes + overhead) / 1024
| where (bytes / 1024) > 10
| where (status=500 OR status=503) AND attempts > 1
```

The implementation must not guess from a token immediately following the
closing parenthesis or use data-dependent field resolution. The parser should
parse a bounded internal eval-expression node that records whether the result
is scalar or Boolean, then convert to the existing public scalar or predicate
AST after the enclosing grammar establishes the required category. A scalar
cannot become truthy merely because it is parenthesized, and a Boolean
expression cannot escape into a direct `eval` assignment.

### Precedence and associativity

Version 0.2 pins:

1. parentheses and function calls;
2. unary `+` and `-`, right-associative;
3. `*`, `/`, and `%`, left-associative;
4. binary `+` and `-`, left-associative;
5. SPL1 period concatenation, left-to-right over its flattened operand list;
6. comparison and membership predicates;
7. `NOT`;
8. `AND`; and
9. `OR`.

This preserves existing `where` Boolean precedence. Base-search precedence
remains its separate parentheses, `NOT`, `OR`, then `AND` contract.

Examples that must be pinned:

```text
1 + 2 * 3            => 7
(1 + 2) * 3          => 9
20 / 5 / 2           => 2
- - 2                => 2
"value=" . 1 + 2    => "value=3"
1 + 2 . "ms"        => "3ms"
```

The final two examples assert that arithmetic binds more tightly than period
concatenation. `+` never concatenates Strings in v0.2; users must use `.`.

## Quoted scalar field references

### Accepted form

One single-quoted token denotes an exact scalar field reference:

```spl
| eval normalized='request-bytes' / 1024
| where 'HTTP Status' IN (500, 502, 503)
| eval 'error-rate'=(errors / requests) * 100
```

Double quotes continue to denote String literals. Backticks are not accepted.

The initial quoted-field decoder accepts ordinary UTF-8 plus the two escapes
`\\` and `\'`. A backslash before any other character is rejected rather than
silently normalized. Empty names, invalid UTF-8, controls, leading or trailing
Unicode whitespace, private compiler names, wildcards, and names exceeding the
existing normalized field-path limits fail before planning.

Quoting changes only tokenization. It does not bypass:

- canonical-field rules;
- dynamic-path parsing, segment limits, or percent-escape semantics;
- projection and result-shape rules;
- reserved `fields` behavior;
- index authorization;
- canonical `_time` lineage; or
- destination collision and private-column protections.

A quoted dotted spelling still uses the existing SPL path interpretation.
Quoting is not a second literal-dot escape system.

### Limited command surface

Version 0.2 accepts quoted fields only where the shared eval-expression parser
or an `eval` assignment destination consumes them. Aggregate inputs, `BY`
fields, extraction options, projection commands, and other command-specific
field grammars remain exact-unquoted. This prevents an expression change from
silently widening every command grammar.

## Arithmetic compatibility contract

### Operators and result type

The supported arithmetic operators are:

| Operator | Meaning | Result |
| --- | --- | --- |
| unary `+` | numeric identity | nullable `Float64` |
| unary `-` | numeric negation | nullable `Float64` |
| binary `+` | numeric addition | nullable `Float64` |
| binary `-` | numeric subtraction | nullable `Float64` |
| `*` | numeric multiplication | nullable `Float64` |
| `/` | floating-point division | nullable `Float64` |
| `%` | floating-point remainder using truncated division | nullable `Float64` |

Every arithmetic result is a fixed nullable `Float64`, even when both operands
are integer literals. This follows the documented SPL calculation model and
prevents ClickHouse integer width, signedness, Decimal scale, or common-type
selection from becoming public behavior. Version 0.2 makes no exact-arithmetic
claim beyond IEEE-754 double behavior.

The period operator remains separately typed and produces String. Binary `+`
with two String operands is rejected with a suggestion to use `.`.

### Eligible inputs

Arithmetic accepts:

- fixed signed and unsigned integers;
- fixed finite or non-finite `Float64` values;
- numeric literals admitted by the existing literal grammar;
- runtime Dynamic integer and floating-point scalars;
- bounded numeric-looking Dynamic Strings;
- a valid bounded `decimal/v1` scalar that can be converted to `Float64`; and
- outputs already proven numeric by `tonumber`, `round`, `ceil`, `floor`,
  `mvcount`, `relative_time`, `strptime`, numeric aggregates, `bin`, or an
  inner arithmetic expression.

Arithmetic does not accept canonical `_time` or `_indextime` as an implicit
number in v0.2. A caller must use a supported conversion-producing function.
This prevents accidental timestamp-unit assumptions.

### Missing, null, and incompatible values

The per-row result is null when any required operand is:

- missing or projected away;
- explicit null;
- an empty String;
- a nonnumeric String;
- a Boolean;
- bytes or binary-ineligible `_raw`;
- an ordinary object;
- a list or other multivalue value;
- an unsupported tagged scalar; or
- a numeric spelling that exceeds the existing bounded conversion grammar.

A statically known fixed multivalue, object, Boolean, binary, or canonical-time
operand fails planning with a source-located unsupported-value-type diagnostic.
An open-schema Dynamic value uses the per-row null behavior above because its
runtime type is not known during planning.

A value claiming a recognized semantic tag with a malformed, oversized, or
internally inconsistent payload is not ordinary nonnumeric input. It fails the
job atomically through the existing sanitized unsupported-value boundary. No
payload text appears in a diagnostic, audit event, or log.

### Numeric String conversion

Dynamic numeric Strings reuse the repository's bounded numeric grammar and
one-evaluation helpers. An input spelling is capped at 4 KiB. Leading sign,
fraction, and exponent spellings follow the existing normalized-number rules.
Whitespace, `NaN`/`Inf` text, incomplete exponent syntax, invalid UTF-8, or a
value that cannot convert to a double produces null.

Valid integers beyond exact IEEE-754 representation may lose low-order
precision when converted to `Float64`. This is intentional v0.2 behavior and
must be visible in the compatibility corpus around `2^53`. The separate exact
comparison and aggregate ordering contract is not reused to imply exact
arithmetic.

### Zero, non-finite, and exceptional results

- addition, subtraction, multiplication, unary negation, and remainder
  normalize negative zero to positive zero at publication;
- division by positive or negative zero returns null;
- remainder by positive or negative zero returns null;
- finite arithmetic overflow may produce positive or negative infinity;
- invalid IEEE operations other than the explicit zero-divisor cases may
  produce `NaN`;
- computed `NaN` and infinities are transported as `Double` values;
- ordinary comparisons with `NaN` follow the pinned v0.2 comparison corpus;
  no backend default is accepted without a fixture; and
- `tonumber` continues to reject non-finite input according to its v0.1
  contract.

ClickHouse's `divide` returns infinity on zero and its ordinary `modulo` may
throw. The compiler must use guarded or `OrNull` forms and cannot expose those
backend defaults as SPL behavior.

### Evaluation order and short circuiting

Arithmetic operands are pure, but source occurrence and failure behavior are
still observable. Operands compile in authored left-to-right order and every
operand is evaluated at most once per row. A long left-associative chain must
not duplicate a Dynamic normalization subtree.

Existing `if` and `case` short-circuit behavior remains authoritative. An
arithmetic error or malformed tagged value in an unselected supported branch
must not fail that row after ClickHouse's pinned short-circuit setting is
applied. Static type analysis and constant folding remain bounded separately;
the supported language does not gain side effects.

## Membership compatibility contract

### Accepted forms

Version 0.2 accepts the function and infix spellings in every existing
eval-predicate position:

```spl
| where in(status, 400, 401, 403, 404)
| where status IN (400, 401, 403, 404)
| where status NOT IN (200, 201, 204)
| eval class=if(environment IN ("production", "staging"), "managed", "other")
| stats count(eval(status IN (500, 502, 503))) AS server_errors
```

Function and keyword names are case-insensitive. A bare field named `in`
remains an ordinary field. The list contains one through 32 candidate scalar
expressions. Empty lists, a thirty-third candidate, a trailing comma, and
missing parentheses fail before planning.

This release does not accept membership in base search:

```spl
index=main status IN (4*, 5*)
```

That form has search-command wildcard and case behavior and requires its own
later contract.

### Equality and types

Membership is a bounded, single-evaluation disjunction of existing eval-language
equality comparisons:

```text
value IN (a, b, c)  ==  (value=a) OR (value=b) OR (value=c)
```

This is a semantic definition, not permission to duplicate `value` three times
in generated SQL. The compiler binds or materializes the left value once.

Consequently:

- String membership is case-sensitive;
- numeric equality uses the existing eval comparison contract;
- fixed Float comparisons retain native `Float64` behavior;
- mixed Dynamic numeric values use the existing bounded exact comparison key;
- no candidate is treated as a wildcard;
- Boolean-function results remain restricted to Boolean consumers;
- a fixed multivalue input is unsupported; and
- open-schema lists and objects produce a null comparison for that row.

Candidates may be any scalar expression supported by v0.2, including exact
field references, literals, functions, grouping, and arithmetic. Every
candidate is evaluated at most once and in source order. The total candidate
budget below prevents a field list from multiplying compiler work.

### Three-valued result

Membership uses three-valued Boolean semantics:

1. if any candidate comparison is true, the result is true;
2. otherwise, if the input or any candidate comparison is null, the result is
   null; and
3. otherwise, the result is false.

`NOT IN` is the exact Boolean negation of that result: true becomes false,
false becomes true, and null remains null. A `where` or conditional-count
consumer admits only Boolean true. `if` and `case` follow their existing null
predicate behavior.

The result cannot be assigned directly by `eval`, matching the existing
search-mode Boolean-result restriction. It can be consumed by `if`, `case`,
`where`, `count(eval(...))`, or the existing supported Boolean composition.
It cannot be explicitly compared with a Boolean literal in v0.2.

## Result shape and downstream composition

Arithmetic is row-preserving. It does not change event identity, row order,
cardinality, relation kind, or canonical time lineage. An `eval` assignment
creates or replaces one fixed nullable `Double` field. A null result is an
explicit present null, following the existing `eval` destination contract.

Downstream commands consume arithmetic output through their existing rules:

- `search`, `where`, `sort`, `dedup`, `fields`, and `table` see the calculated
  field normally;
- `stats`, `eventstats`, `streamstats`, `chart`, and `timechart` treat the field
  as fixed numeric input;
- `bin` can bucket it numerically;
- field discovery reports its fixed type and separate null/presence counts;
- field summary treats computed `NaN` and infinities according to its existing
  Double-value contract;
- export preserves the typed protobuf value before CSV/JSONL rendering; and
- assigning `_time` still replaces canonical clock lineage and makes later
  timeline or `timechart` consumers ineligible.

Membership is a predicate and does not add a result field unless consumed by a
supported scalar conditional whose output is assignable.

## Target implementation architecture

### Lexer

Extend `internal/spl/lexer.go` with explicit tokens for:

- single-quoted field references;
- `+`, `-`, `*`, `/`, and `%`;
- `==`; and
- any punctuation required to distinguish membership lists without changing
  the existing comma/parenthesis tokens.

`IN` and `NOT` remain case-insensitive word tokens interpreted by parser
context. They are not globally reserved field names.

The lexer must remain allocation-bounded and source-position exact for UTF-8.
It must preserve current double-quoted escape behavior and dynamic dotted-path
scanning. Unterminated single and double quotes get distinct stable diagnostic
codes and exact opening-quote ranges.

The lexer change is security-sensitive because operator characters previously
belonged to larger words. Add a differential lexer corpus that walks every
ASCII byte and representative Unicode whitespace around every new token.

### Parser

Replace the scalar-primary-plus-concatenation path with one bounded precedence
parser. The implementation must:

- share one grammar across `eval`, `where`, `if`, `case`, and conditional
  aggregates;
- retain base-search parsing as a separate precedence implementation;
- carry explicit stop tokens for commas, assignment separators, and closing
  parentheses;
- reject scalar/Boolean category confusion before AST publication;
- count arithmetic operators, membership candidates, predicate leaves, nodes,
  tokens, and depth during parsing;
- produce no partial query on error; and
- preserve exact source ranges for operators, operands, groups, membership
  lists, and quoted fields.

Do not implement data-dependent parse resolution. The same source must produce
the same AST regardless of event schema, tenant, index, or knowledge snapshot.

### SPL AST

Add explicit source-layer nodes and closed enums rather than representing
operators as synthetic function calls:

```go
type ScalarUnaryExpr struct {
    Op      ScalarUnaryOp
    Operand ScalarExpr
    Range   Range
}

type ScalarBinaryExpr struct {
    Op    ScalarBinaryOp
    Left  ScalarExpr
    Right ScalarExpr
    Range Range
}

type WhereMembershipExpr struct {
    Value      ScalarExpr
    Candidates []ScalarExpr
    Negated    bool
    Range      Range
}
```

The exact names may change, but unary, binary, and membership kinds must be
closed enums with invalid sentinels. Every visitor must reject unknown enum
values, nil interfaces, typed nils, cycles, excess depth, excess nodes, invalid
ranges, empty candidate lists, and forged candidate counts.

Quoted and unquoted fields both become `ScalarFieldExpr` after validated
decoding. Quoting is syntax provenance, not a runtime field type.

### Logical plan

Add matching `plan.ScalarUnaryExpression`, `plan.ScalarBinaryExpression`, and
membership predicate nodes. Builder conversion must:

- clone caller-owned slices;
- resolve every field through the existing exact field resolver;
- preserve read-position analysis in source order;
- reject private compiler fields and forged source ASTs;
- preserve fixed/Dynamic type evidence without claiming a runtime type it did
  not prove; and
- include every operator and candidate in complexity evidence.

Update all exhaustive analysis, result-shape, knowledge-composition, inspection,
and defensive-validation walks. No default switch arm may silently ignore a new
node.

### ClickHouse compiler

Create one arithmetic compiler subsystem rather than five unrelated string
templates. It should return the existing `compiledScalar` shape plus explicit
numeric-domain evidence.

The subsystem must:

1. validate the node and operator before recursion;
2. compile child operands in source order;
3. normalize each child once to nullable `Float64` under the v0.2 input rules;
4. use `divideOrNull` and `moduloOrNull`, or equivalent guarded expressions,
   for zero-divisor behavior;
5. normalize publication negative zero where the contract requires it;
6. retain computed `NaN` and infinities;
7. bind literals and user text as arguments, never SQL fragments;
8. carry the fixed nullable-Double result type and conservative output bound;
9. enforce per-node and aggregate SQL/work ceilings before concatenation; and
10. prove that every authored operand occurs once in the physical action graph.

Membership compilation must reuse eval-comparison semantics but must not emit a
ClickHouse `IN` over an inferred common type. Backend `IN` coercion, null, and
Dynamic behavior are not the public contract. Materialize the input once,
compile each bounded comparison, and combine the results with explicit
three-valued logic.

Chains may use a compiler-private projection to prevent repeated Dynamic
normalization. Private names remain unaddressable by SPL and are removed before
public schema publication. Added projection levels count against the existing
96-level dependency path.

### Executor and search jobs

No new executor mode should be required. The existing typed row transport can
publish nullable Double arithmetic results and Boolean predicates remain inside
ClickHouse filtering or conditional contribution.

Nevertheless, execution tests must prove:

- computed non-finite Doubles survive ordinary result paging;
- null arithmetic fields retain present-null metadata;
- no membership Boolean leaks as an assignable public value;
- malformed semantic tags map to the sanitized unsupported-value failure;
- divide/remainder by zero do not become backend exceptions;
- cancellation and memory/time limits remain effective; and
- no schema or row is published before a whole-result validation barrier when
  a later row can fail atomically.

The job manager's validation, failure classification, inspection, history,
rerun, export, and field-analysis paths must all parse and compile the same
v0.2 source.

### Knowledge-object boundary

Tier-1 knowledge compatibility remains `0.1` during this release. Existing
calculated-field definitions continue to use the v0.1 standalone scalar
profile, so enabling authored v0.2 arithmetic cannot silently change an ACTIVE
knowledge snapshot or its digest.

Implement a closed internal expression-profile enum:

- authored search profile `SPLExpressionV02`; and
- knowledge calculated-field profile `SPLExpressionV01`.

Do not expose arbitrary feature flags to callers. The parser entry point chooses
one closed profile, and every profile has a complete positive and negative
fixture inventory.

Adopting arithmetic in reusable calculated fields is a later knowledge
compatibility release. It must version the knowledge compiler, snapshot digest,
retained execution, publication validation, and compatibility fixture together.

### Browser editor and suggestions

The browser must not lag the backend grammar. Update the shared completion and
editor surfaces to:

- scan single-quoted field references without treating inner pipes, commas, or
  operators as structure;
- distinguish unterminated single and double quotes;
- highlight arithmetic and membership operators only outside quoted values;
- offer `IN (...)` and `in(...)` completions in predicate contexts;
- suggest quoting a special-character field returned by server field
  completion;
- retain source offsets in UTF-16/UTF-8 conversions used by diagnostics;
- avoid local acceptance of backend-rejected Boolean assignment; and
- render server diagnostics as authoritative when local structure scanning is
  insufficient.

The Go completion catalog remains the shared command/function authority loaded
by TypeScript. Add operator metadata to that catalog only if both runtimes
consume and validate it; do not create a second handwritten operator list.

## Resource limits

Version 0.2 retains every v0.1 ceiling and adds:

| Resource | Limit | Failure |
| --- | ---: | --- |
| arithmetic operator occurrences per query | 256 | `SPL_QUERY_TOO_COMPLEX` |
| unary operator chain | 32, also bounded by scalar depth | `SPL_QUERY_TOO_COMPLEX` |
| membership candidates per occurrence | 32 | `SPL_QUERY_TOO_COMPLEX` |
| membership candidates across one query | 256 | `SPL_QUERY_TOO_COMPLEX` |
| quoted decoded field name | existing field-path byte/segment limits | existing field diagnostic |
| Dynamic numeric String input | 4 KiB per value | null result |
| one arithmetic or membership compiled node | 64 KiB generated SQL | `SPL_QUERY_TOO_COMPLEX` |
| whole generated query | existing 256 KiB | `SPL_QUERY_TOO_COMPLEX` |
| scalar nesting | existing 32 levels | `SPL_QUERY_TOO_COMPLEX` |
| eval/where predicate leaves | existing 32 | `SPL_QUERY_TOO_COMPLEX` |
| syntax tokens | existing 1,024 | `SPL_QUERY_TOO_COMPLEX` |
| pipeline commands | existing 64 | `SPL_QUERY_TOO_COMPLEX` |

Nested occurrences are charged independently. A membership predicate consumes
one predicate leaf regardless of candidate count, while every candidate is
charged to the membership-candidate budget. Arithmetic inside a candidate also
consumes its ordinary operator budget.

Every compiler counter must be derived from the trusted AST/plan rather than a
caller-supplied total. Parser-produced counters may be retained as sealed
evidence, but the planner and compiler independently validate forged trees.

ClickHouse's existing time, memory, scan, group, result-row, query-size,
subquery-depth, and cancellation limits remain independent. A new expression
budget never raises a backend budget.

## Security and authorization invariants

The implementation must preserve:

- parameterized SQL for every literal, field-derived value, and membership
  candidate;
- exact field-identifier quoting through compiler-owned identifier functions;
- no user-authored ClickHouse function, identifier, type, setting, or SQL
  fragment;
- immutable tenant and authorized-index scope independent of calculated
  `index` values;
- immutable search time, index-time cutoff, and visibility snapshot;
- immutable pinned knowledge snapshot;
- no arithmetic influence over resource settings or limits;
- no logs, diagnostics, or audits containing field values or semantic-tag
  payloads;
- bounded parse and compile work before ClickHouse access;
- no row expansion, `ARRAY JOIN`, external call, file read, dictionary lookup,
  or mutation; and
- whole-query rejection before any supported-prefix execution.

Quoted field references must pass the same private-name and canonical-root
checks as unquoted fields. Quoting must never make compiler-private columns or
unauthorized physical selectors addressable.

## Diagnostics

The compatibility contract must define exact stable codes and source ranges.
The implementation may reuse an existing code where its category is already
correct, but the minimum taxonomy is:

| Category | Expected code family |
| --- | --- |
| unexpected or unterminated operator/quote syntax | `SPL_*SYNTAX` / existing lexer code |
| unsupported arithmetic form | `SPL_UNSUPPORTED_ARITHMETIC_SYNTAX` |
| statically unsupported arithmetic input | `SPL_UNSUPPORTED_ARITHMETIC_VALUE_TYPE` |
| malformed membership form | `SPL_UNSUPPORTED_MEMBERSHIP_SYNTAX` |
| statically unsupported membership input | `SPL_UNSUPPORTED_MEMBERSHIP_VALUE_TYPE` |
| Boolean assignment escape | existing `SPL_UNSUPPORTED_EVAL_EXPRESSION` |
| invalid/forbidden field | existing field diagnostic |
| structural or SQL expansion overflow | `SPL_QUERY_TOO_COMPLEX` |
| malformed runtime semantic scalar | sanitized unsupported-value job failure |

Diagnostics identify the smallest responsible authored range: operator for an
unsupported operator form, operand for a type failure, list for candidate
overflow, and opening quote for an unterminated quoted field.

Suggestions are static, bounded, and payload-free. Examples include:

- `use the SPL1 period operator for String concatenation`;
- `quote the field as 'request-bytes'`;
- `use relative_time or strptime before arithmetic on time values`; and
- `use in(value, ...) inside if or where; Boolean results cannot be assigned`.

## API, protobuf, persistence, and upgrade impact

### Protobuf

No new public result type is required. Arithmetic publishes the existing Double
value and membership remains a predicate. Existing diagnostics, suggestions,
inspection, history, export, and bootstrap version fields should be sufficient.

If implementation discovers that a wire change is necessary, it must be an
append-only protobuf change with:

- regenerated Go and TypeScript bindings;
- protobuf lint and breaking checks;
- unknown-field forward-compatibility tests;
- bounded custom decoder/encoder updates where applicable; and
- no reuse of retired tags or names.

### SQLite and ClickHouse

No ClickHouse event-schema migration is planned. No SQLite schema migration is
planned for the language feature itself. Searches and saved objects continue to
store authored SPL rather than ASTs or generated SQL.

The optional read-only compatibility audit accepts an explicit control-database
path and performs no mutation. If a persistent audit receipt is later desired,
that is a separate control-plane design and migration.

### Rollback

Rolling back to a v0.1 binary causes v0.2 saved-search text to fail with the
older unsupported-expression diagnostic. It does not corrupt stored data
because no v0.2 AST or SQL is persisted. Running jobs use the process that
admitted them and are drained under the existing shutdown contract before a
binary rollback.

Active knowledge definitions remain v0.1 and therefore require no rollback
translation in this release.

## Testing strategy

### Normative compatibility corpus

Create a machine-readable v0.2 corpus with at least the following rule groups:

| Group | Required cases |
| --- | --- |
| precedence | every adjacent precedence pair, associativity, nested groups |
| lexing | spaced/unspaced operators, numeric signs/exponents, dots, percent escapes, Unicode whitespace |
| quoted fields | ordinary, keyword-shaped, dotted, escaped quote/backslash, invalid/control/oversized/private |
| unary arithmetic | positive, negative, nested, missing/null, type failures |
| binary arithmetic | every operator, mixed numeric types, numeric Strings, `2^53` edges |
| exceptional arithmetic | zero divisors, negative zero, overflow, `NaN`, infinities, malformed tags |
| membership | function/infix/NOT, one/32/33 candidates, fields and expressions |
| membership nulls | null input, null candidate, earlier match before null, no match plus null |
| membership types | String case, integer/Float/Decimal, mixed Dynamic, containers, multivalue |
| Boolean boundaries | where, if, case, count(eval), explicit comparison, rejected direct assignment |
| pipeline composition | before/after projection, rename, extraction, aggregate, sort, bin, timeline |
| authorization | calculated `index`, `_time`, projected scope fields, knowledge prelude |
| limits | depth, nodes, operators, candidates, tokens, SQL, Dynamic input bytes |
| diagnostics | exact code, range, line/column, suggestions, no source echo |

Each case names its expected relation kind, output schema, ordered typed values,
presence/null state, or exact diagnostic. Success fixtures do not use only
synthetic ASTs; representative strings must pass through the public parser.

### Parser tests

Add table, property, and fuzz tests for:

- complete grammar and precedence;
- scalar-versus-Boolean parenthesis disambiguation;
- every operator next to every delimiter and quote;
- source positions across ASCII, multibyte UTF-8, CRLF, and multiple lines;
- v0.1 positive corpus preservation except the explicit operator-character
  transition cases;
- no supported-prefix AST on any error;
- stable complexity rejection before recursive overflow;
- single-quoted decoding and invalid escapes; and
- parser determinism independent of schema.

Fuzzers must assert termination, bounded allocation relative to the source
ceiling, valid source ranges, and parse/pretty-canonicalize/reparse equivalence
for the supported subset.

### Planner tests

For every new node, test:

- AST-to-plan conversion;
- exact field dependencies and first-read positions;
- left-to-right assignment visibility;
- fixed and Dynamic type evidence;
- projection and canonical-lineage changes;
- nil, typed nil, forged enums, invalid ranges, cycles, and oversized slices;
- knowledge-prelude composition without enabling v0.2 knowledge expressions;
  and
- result-shape and inspection evidence.

### Compiler tests

Compiler tests must pin:

- parameter order and exact placeholder count;
- one physical source occurrence per operand;
- no ClickHouse implicit `IN` coercion;
- guarded zero-divisor lowering;
- fixed nullable-Double result types;
- Dynamic numeric classification and bounded text parsing;
- negative zero and non-finite behavior;
- left-associative evaluation shape;
- per-node and whole-query SQL ceilings;
- no `ARRAY JOIN`, second event scan, or unbounded array construction;
- private materialization-column removal;
- relational-depth accounting; and
- forged-plan rejection before SQL publication.

### Independent reference evaluator

Implement a small test-only Go evaluator for the exact v0.2 arithmetic and
membership contract. It must not share compiler SQL fragments or ClickHouse
helpers. Use it as the oracle for randomized finite arithmetic, null
propagation, zero divisors, membership three-valued logic, and candidate order.

Floating-point comparisons account for `NaN`, infinity, sign of zero where
relevant, and an explicit tolerance only where the contract permits it. Exact
integer-to-double boundary cases are fixed fixtures, not tolerance comparisons.

### Pinned ClickHouse integration

Run the isolated compiler corpus and production Store/Executor path against the
digest-pinned ClickHouse release. It must cover:

- fixed and Dynamic event fields;
- every stored scalar type;
- sparse missing versus explicit null;
- mixed rows where only some values are numeric;
- malformed semantic-tag rows inserted through the trusted test seam;
- zero, negative zero, overflow, `NaN`, and infinities;
- membership with mixed types and null candidates;
- downstream stats, eventstats, streamstats, bin, chart, and timechart;
- result paging and export;
- cancellation and query limits; and
- `EXPLAIN actions=1` evidence for single evaluation and no row expansion.

### Search-manager and API tests

Test public validation and job routes for:

- valid and invalid v0.2 source;
- normalized source retention;
- synchronous invalid/unsupported classification;
- asynchronous runtime unsupported-value classification;
- owner/tenant isolation;
- history record and rerun;
- saved-search execution;
- inspection and redacted plan evidence;
- CSV/JSONL export;
- field catalog and summary; and
- capability version advertisement only after complete composition.

### Knowledge regression tests

Prove that:

- v0.1 ACTIVE calculated fields retain their exact snapshot digest and runtime
  behavior;
- arithmetic is rejected by the v0.1 standalone knowledge parser;
- authored v0.2 arithmetic can consume outputs from a v0.1 knowledge prelude;
- preview, validation, retained execution, history, and export keep their
  knowledge authority; and
- no v0.2 syntax changes publication or dependency resolution without a future
  knowledge-version change.

### Browser tests

Test:

- single-quote structure scanning and fixes;
- operator highlighting outside both quote kinds;
- completion around arithmetic and membership;
- special-character field insertion with quoting;
- Unicode diagnostic offsets;
- server-diagnostic rendering;
- open/save/rerun preservation of source; and
- real browser execution from editor through typed results and visualization.

### Differential compatibility

If a licensed Splunk oracle is available, record version/build/timezone and run
at least:

- integer, floating, numeric-String, missing, null, Boolean, and multivalue
  arithmetic;
- division and remainder by zero;
- negative remainder;
- overflow and non-finite publication;
- unary spacing and precedence;
- String `+` behavior, retained as unsupported in Open Splunk v0.2;
- membership String case, mixed types, null candidates, and field candidates;
- `NOT IN` null behavior; and
- quoted field parsing.

Oracle absence does not block explicit Open Splunk behavior, but every
unverified edge must be labeled as such in the compatibility contract and
release evidence.

## Performance and efficiency gates

The release must demonstrate:

1. parse, analysis, and compile time scale linearly with operator and candidate
   counts up to their ceilings;
2. generated SQL grows linearly and remains below every configured ceiling;
3. a fixed numeric arithmetic chain performs one input relation scan;
4. Dynamic operands are classified once per occurrence rather than once per
   consumer branch;
5. membership materializes its left input once and performs at most the bounded
   candidate comparisons;
6. no query creates an `ARRAY JOIN` or multiplies event rows;
7. cancellation remains prompt under the maximum expression shape; and
8. compiler caches cannot grow beyond one query lifecycle.

Add benchmarks for:

- parsing 1, 32, 128, and 256 arithmetic operators;
- compiling fixed and Dynamic arithmetic chains;
- compiling membership lists of 1, 8, 16, and 32 candidates;
- executing fixed and Dynamic arithmetic over a production-shaped fixture; and
- executing membership over low- and high-cardinality fields.

The first benchmark run establishes a checked-in diagnostic baseline. Release
acceptance requires no superlinear regression and no material unexplained
regression against equivalent hand-authored parameterized ClickHouse SQL.
Hardware-dependent throughput numbers are recorded with machine, container,
dataset, warmup, run count, and variance; they are not generalized into a
portable product claim.

## Delivery plan

The phases are ordered by dependency. Each implementation phase ends with a
reviewable, test-complete commit or small commit series. A later phase must not
paper over a missing earlier contract.

### Phase EX-0 — Contract, corpus, and ambiguity closure

**Estimated effort:** 1–2 engineer-weeks
**Outcome:** the expression behavior is frozen before parser code changes.

- write `spl-compatibility-v0.2.md`;
- create the machine-readable rule inventory and initial positive/negative
  corpus;
- pin precedence, result types, nulls, non-finite values, zero divisors,
  remainder sign, membership nulls, and quoting;
- run available Splunk differential cases or mark exact unverified decisions;
- inventory v0.1 sources affected by reserved operator characters;
- design the closed v0.1/v0.2 expression profiles; and
- add failing compatibility tests that establish the intended public surface.

**Exit gate:** no unresolved semantic question can change the AST shape or
public result type. Remaining oracle questions must be explicit unsupported
boundaries or documented Open Splunk rules.

### Phase EX-1 — Lexer, grouped grammar, and typed IR

**Estimated effort:** 2–3 engineer-weeks
**Outcome:** source parses into a bounded, source-located, defensively
validated expression tree.

- add quote/operator tokens and exact diagnostics;
- implement the unified parenthesized scalar/Boolean precedence parser;
- add unary, binary, and membership AST nodes and enums;
- add quoted scalar fields and quoted `eval` destinations;
- add `==`;
- implement closed expression profiles;
- update analysis, builders, visitors, cycles, node/depth accounting, and field
  dependencies;
- update suggestions and the shared completion metadata; and
- complete parser/planner property and fuzz gates.

**Exit gate:** parser and planner tests pass; compiler rejects every new node as
unsupported rather than accidentally accepting it. No production compatibility
version changes.

### Phase EX-2 — Typed arithmetic lowering

**Estimated effort:** 3–4 engineer-weeks
**Outcome:** unary and binary arithmetic execute through the pinned ClickHouse
compiler with the complete type and resource contract.

- implement shared numeric normalization;
- implement all seven unary/binary operation forms;
- guard division and remainder;
- preserve non-finite results and present-null metadata;
- implement one-evaluation materialization and SQL ceilings;
- connect result-shape, field discovery, inspection, and export;
- add the independent Go evaluator and randomized comparison tests; and
- pass pinned ClickHouse arithmetic and downstream-composition integration.

**Exit gate:** every arithmetic compatibility fixture passes through parser,
planner, compiler, executor, and manager; `EXPLAIN` proves one scan and no row
expansion.

### Phase EX-3 — Eval-language membership

**Estimated effort:** 2–3 engineer-weeks
**Outcome:** function, infix, and negated membership work in every existing
predicate consumer.

- implement function and infix syntax over one AST contract;
- implement three-valued equality disjunction;
- materialize the left value once;
- enforce per-list and per-query candidate ceilings;
- integrate Boolean traits and assignment rejection;
- connect conditional aggregates and all predicate consumers; and
- pass unit, randomized reference, and pinned ClickHouse membership matrices.

**Exit gate:** every membership fixture passes, backend `IN` coercion is absent,
and null/NOT behavior is atomic and deterministic.

### Phase EX-4 — Product surfaces and compatibility audit

**Estimated effort:** 1–2 engineer-weeks
**Outcome:** the browser and every retained-search surface agree with the
backend.

- update editor scanning, highlighting, completion, and diagnostic fixes;
- add the read-only affected-source compatibility audit;
- execute saved search, history rerun, inspection, export, and field analysis;
- prove v0.1 knowledge snapshots remain unchanged;
- update examples and user documentation; and
- run the production browser vertical against pinned ClickHouse.

**Exit gate:** no backend-supported syntax is invisible or locally rejected in
the editor, and no UI-advertised syntax fails the backend contract.

### Phase EX-5 — Adversarial hardening and activation

**Estimated effort:** 2–3 engineer-weeks
**Outcome:** compatibility `0.2` is safe to advertise.

- run independent correctness, security, reuse, and efficiency reviews;
- resolve every concrete finding;
- run full unit, race, fuzz, lint, build, protobuf, integration, browser, and
  release gates;
- record benchmark and pinned `EXPLAIN` evidence;
- update the compatibility version in bootstrap/job/history/export surfaces in
  one final change;
- update README and checkpoint documentation; and
- publish a redacted acceptance report with exact revision and tool versions.

**Exit gate:** all acceptance criteria below pass on the exact activation
revision. A failure keeps the server on its previous compatibility identity.

## Parallelism and sequencing

After EX-0 freezes behavior:

- parser/IR work and the test-only reference evaluator can proceed in parallel;
- browser structure-scanner work can begin after quote/operator token behavior
  is fixed;
- arithmetic and membership compiler work share numeric/comparison helpers and
  should not diverge in separate designs;
- public API/job integration waits until compiler result types are stable; and
- activation and version advertisement remain strictly last.

Two engineers can overlap parser/reference work and compiler/browser work, but
the critical path still runs through EX-0, the common typed IR, compiler
completion, and final vertical acceptance. The 11–17 engineer-week estimate
does not imply 11–17 calendar weeks with parallel staffing.

## Acceptance criteria

Compatibility v0.2 is complete only when all of the following are true:

### Language and semantics

- [x] The normative v0.2 contract and machine-readable inventory agree.
- [x] Quoted scalar fields and quoted eval destinations obey exact field bounds.
- [x] Parenthesized scalar and Boolean expressions cannot be confused.
- [x] Unary and binary arithmetic obey the pinned precedence and type matrix.
- [x] Division/remainder by zero, negative zero, overflow, `NaN`, and infinity
      match the contract.
- [x] Function, infix, and `NOT IN` membership obey the same equality and null
      semantics.
- [x] Direct Boolean assignment, string `+`, multivalue arithmetic, base-search
      `IN`, and every other excluded form fail explicitly.

### Safety and resources

- [x] Tenant/index/time/visibility/knowledge authority cannot be widened.
- [x] Every value remains parameterized and every field identifier is
      compiler-quoted.
- [x] Parser, planner, and compiler independently reject forged and oversized
      structures.
- [x] Operator, candidate, SQL, depth, token, and backend budgets are enforced.
- [x] No arithmetic or membership path expands rows, emits `ARRAY JOIN`, scans
      events twice, or performs external work.
- [x] Malformed semantic tags fail atomically without payload disclosure.
- [x] Cancellation, deadlines, and memory limits remain effective.

### Product completeness

- [x] Validation, job creation, paging, inspection, history, rerun, saved
      searches, export, field discovery, and field summary accept the same
      surface.
- [x] Browser highlighting, completion, quoting, and diagnostics match the
      backend.
- [x] Authored v0.2 expressions compose with v0.1 knowledge preludes without
      changing knowledge snapshot identity.
- [x] The production collector-to-browser vertical executes representative
      arithmetic and membership searches against pinned ClickHouse.
- [x] Compatibility `0.2` is advertised only by a completely composed runtime.

### Quality evidence

- [x] Focused, repository-wide, race, fuzz, vet, lint, build, protobuf, and
      frontend gates pass.
- [x] Pinned ClickHouse integration and `EXPLAIN` evidence pass.
- [x] Benchmarks show linear bounded work and no unexplained material regression.
- [x] Independent correctness, security, reuse, and efficiency reviews have no
      unresolved concrete findings.
- [x] Release evidence names the exact Git revision, ClickHouse digest, Go/Node
      toolchain, and test commands.

## Required validation commands

The exact implementation may add focused packages or tests, but final evidence
must include at least:

```sh
go test ./... -count=1 -timeout=10m
go test -race ./internal/spl ./internal/plan ./internal/clickhouse \
  ./internal/queryexec ./internal/searchjobs ./internal/knowledgeprogram \
  ./internal/knowledgesnapshot ./cmd/open-splunk-server -count=1 -timeout=20m
go mod tidy -diff
go vet ./...
go build ./...
golangci-lint run ./...

npm run typecheck
npm run lint
npm run test:frontend
npm run build

make proto
make proto-lint
BUF_CACHE_DIR="$PWD/.cache/buf" \
  node scripts/check-buf-breaking.mjs --against-ref main
go test . -count=1

OPEN_SPLUNK_CLICKHOUSE_INTEGRATION=1 \
OPEN_SPLUNK_CLICKHOUSE_TEST_IMAGE='<digest-pinned selected image>' \
  go test ./internal/clickhouse ./internal/queryexec \
    -run 'Arithmetic|Membership|ExpressionV02' \
    -count=1 -timeout=15m -v

OPEN_SPLUNK_BACKEND_INTEGRATION=1 \
OPEN_SPLUNK_CLICKHOUSE_TEST_IMAGE='<digest-pinned selected image>' \
  go test ./integration -run '^TestBackendVertical$' \
    -count=1 -timeout=15m -v

git diff --check
```

Run every affected fuzz target for a time-boxed campaign during development and
the repository's documented longer release campaign before activation. Record
seeds for every discovered regression.

## Rollout and activation

1. Merge EX-0 through EX-4 without changing public compatibility advertisement.
2. Exercise the complete surface in a non-production build against a copied,
   sanitized control database and production-shaped ClickHouse fixture.
3. Run the read-only punctuation compatibility audit.
4. Complete adversarial review, fuzz campaigns, and load/compile benchmarks.
5. Run the full release validation on one exact clean revision.
6. Change the public compatibility identity to `0.2` and rerun all gates.
7. Publish the compatibility contract, migration note, examples, and exact
   acceptance evidence together.
8. If any complete-family route, editor surface, or runtime dependency is
   missing, suppress `0.2` advertisement and retain the previous identity.

There is no partial feature flag intended for ordinary users. Internal tests may
select an expression profile directly, but production composition exposes one
complete compatibility version.

## Risks and mitigations

| Risk | Consequence | Mitigation |
| --- | --- | --- |
| arithmetic punctuation changes old tokenization | an old field spelling parses differently | pre-1.0 transition contract, quoted fields, read-only audit, exact diagnostics |
| scalar and Boolean parentheses diverge | valid expressions reject or truthiness leaks | unified parser-local expression category and exhaustive precedence corpus |
| ClickHouse chooses a wider/different numeric type | results drift by input type | normalize every arithmetic operand to nullable `Float64` before the operator |
| division or modulo throws | partial or backend-shaped failure | guarded/OrNull lowering and pinned zero/overflow tests |
| Dynamic normalization is duplicated | SQL and per-row work grow exponentially | one-evaluation materialization, occurrence tests, 64/256 KiB SQL ceilings |
| backend `IN` coercion differs | mixed types or nulls match incorrectly | explicit bounded equality disjunction; never delegate semantics to backend `IN` |
| membership list becomes a compile bomb | excessive SQL and comparisons | 32 per occurrence, 256 per query, trusted counters, linear benchmarks |
| non-finite values break transport/UI | schema or rendering failures | existing Double transport, executor and browser non-finite fixtures |
| knowledge calculated fields silently widen | running jobs change after upgrade | keep knowledge expression profile and compiler compatibility at v0.1 |
| browser scanner disagrees with backend quotes | wrong pipe splits or misleading fixes | shared corpus, backend-authoritative diagnostics, real browser vertical |
| no licensed Splunk oracle | ambiguous edge is overstated as compatible | explicit Open Splunk rules or unsupported behavior, labeled evidence |
| implementation grows the monolithic compiler | maintainability degrades | dedicated arithmetic/membership helpers and typed results, independent simplify review |

## Decisions made by this plan

- The next chunk is expression compatibility, not lookups or subsearches.
- The target is bounded SPL1, not SPL2.
- Arithmetic results are nullable `Float64`.
- String `+` is unsupported; period concatenation remains authoritative.
- Canonical time arithmetic and multivalue arithmetic are unsupported.
- Membership reuses eval equality and explicit three-valued logic.
- Membership candidates are bounded scalar expressions, not wildcard patterns.
- Base-search `IN` remains a separate contract.
- `==` is added as an eval comparison synonym.
- Quoted fields are initially limited to scalar expressions and eval
  destinations.
- Operator characters become reserved in scalar expressions under an explicit
  pre-1.0 transition.
- Authored search expressions advance to v0.2 while knowledge calculated fields
  remain on their v0.1 compatibility profile.
- No AST or generated SQL is persisted.
- Compatibility advertisement changes only after complete vertical acceptance.

## Questions intentionally deferred

These questions do not block v0.2 because their forms remain unsupported:

1. Should a later exact arithmetic tier use `decimal/v1`, ClickHouse
   `Decimal256`, or a Go-side exact evaluator?
2. Should String `+` follow SPL1's documented overload or remain a permanent
   deviation in favor of `.`?
3. When should quoted fields expand into every command-specific field grammar?
4. Should a future base-search `IN` support wildcard candidates exactly like
   Splunk, and how should it interact with text-index pruning?
5. Should membership over multivalue input use any-member semantics, and how
   should null members affect `NOT IN`?
6. Which v0.2 expression features should enter the next knowledge compiler
   compatibility version?
7. Is a licensed Splunk differential service available for permanent CI, or
   only for manually recorded release campaigns?

## Roadmap after v0.2

This sequence is directional; each item requires its own approved plan and
compatibility contract.

1. **Core multivalue expressions and controlled row expansion** — selected
   `mvappend`, `mvindex`, `mvjoin`, `mvdedup`, `makemv`, and bounded `mvexpand`,
   with explicit cardinality and memory ceilings.
2. **Exact CSV lookup tier** — execute KO-2 from
   `knowledge-objects-plan.md`, including immutable assets, explicit `lookup`,
   and automatic exact-key enrichment.
3. **Base-search membership and expression breadth** — wildcard `IN`, quoted
   fields across command grammars, `XOR`, infix `LIKE`, selected conversions,
   text/math/date functions, and broader aggregate expressions.
4. **Multiple-relation foundation** — subsearch authority, child-job budgets,
   `append`, `appendpipe`, and bounded `union` before any general join.
5. **Bounded join and correlation** — exact-key `join`, then a separately
   specified bounded `transaction` alternative or implementation.
6. **Knowledge classification and reuse** — KO-3 event types, tags, macros, and
   workflow actions.
7. **Data models and acceleration** — a separate 12–24+ engineer-week design
   program for `datamodel`, pivot, acceleration lifecycle, and `tstats`.

Arbitrary custom commands, scripts, distributed search, and unchanged execution
of arbitrary Splunk apps remain outside this roadmap unless the product scope
is explicitly expanded.

## Source notes

Primary behavior references for the v0.2 contract:

- [Splunk Enterprise `eval` command and operators](https://help.splunk.com/en/splunk-enterprise/spl-search-reference/10.2/search-commands/eval)
- [Splunk Enterprise expression types](https://help.splunk.com/en/splunk-enterprise/search/search-manual/10.0/expressions-and-predicates/types-of-expressions)
- [Splunk Enterprise comparison and conditional functions](https://help.splunk.com/en/splunk-enterprise/spl-search-reference/10.4/evaluation-functions/comparison-and-conditional-functions)
- [Splunk Enterprise `where`](https://help.splunk.com/en/splunk-enterprise/spl-search-reference/9.3/search-commands/where)
- [ClickHouse arithmetic functions](https://clickhouse.com/docs/reference/functions/regular-functions/arithmetic-functions)

The final compatibility document must pin exact source versions and record any
newer documentation consulted during implementation. Documentation examples are
inputs to compatibility design, not substitutes for executable evidence.
