# Open Splunk SPL compatibility v0.3 plan

**Status:** proposed implementation plan; not an executable compatibility
contract

**Target authored-search identity:** `0.3`

**Prerequisite:** authored-search compatibility `0.2` is activated and its
acceptance provenance is closed

**Knowledge-expression identity:** unchanged from `0.1` unless a separate
knowledge compatibility plan expands it

**Prepared:** August 11, 2026

> **Decision:** version 0.3 targets ten bounded commands that reuse the current
> single-relation parser, logical pipeline, ClickHouse compiler, immutable
> search snapshot, ordering, expression, regex, and multivalue infrastructure.
> It does not introduce lookups, subsearches, joins, arbitrary row generation,
> external calls, or mutation.

This document proposes implementation scope. It does not make any syntax
executable and must not be used as the advertised compatibility contract.
Before the first v0.3 syntax is enabled, the implementation must add a
normative `spl-compatibility-v0.3.md`, a matching machine-readable rule
inventory, and an acceptance report.

## Executive summary

Version 0.2 establishes arithmetic, scalar grouping, quoted scalar field
references, and eval-language membership. Version 0.3 should turn that
foundation into broader command coverage without crossing into multiple input
relations or externally managed data.

The proposed commands, in implementation order, are:

1. `regex`
2. `reverse`
3. `accum`
4. `strcat`
5. `addinfo`
6. `fillnull`
7. `addtotals`
8. `delta`
9. `makemv`
10. `mvexpand`

The first five are narrow aliases or projections over behavior Open Splunk
already owns. The next three broaden null, numeric, and ordered-window
operations. The final two establish controlled creation and expansion of
multivalue fields. Together they improve practical SPL portability while
preserving the current bounded, single-root execution model.

Representative target searches are:

```spl
index=api
| regex message="(?i)timeout|connection refused"
| reverse
```

```spl
index=api
| accum bytes AS running_bytes
| delta running_bytes AS bytes_since_previous
```

```spl
index=api
| fillnull value="unknown" host route
| strcat host ":" route endpoint
```

```spl
index=api
| makemv delim="," tags
| mvexpand tags limit=100
| stats count BY tags
```

## Relationship to existing compatibility documents

This plan is subordinate to:

- [`spl-compatibility-v0.1.md`](spl-compatibility-v0.1.md), which remains the
  baseline command and expression contract;
- [`spl-compatibility-v0.2.md`](spl-compatibility-v0.2.md), which owns the
  authored expression foundation used by several commands in this plan;
- [`spl-roadmap.md`](spl-roadmap.md), which identifies controlled multivalue
  expansion as the next architectural program; and
- [`knowledge-objects-plan.md`](knowledge-objects-plan.md), which continues to
  own exact CSV lookups and later knowledge-object expansion.

Version 0.3 incorporates all unchanged v0.1 and v0.2 authored-search behavior.
Tier-1 calculated fields remain on their closed v0.1 expression profile unless
another document explicitly versions that boundary.

## Goals

Version 0.3 should:

1. add ten useful SPL command names without adding a second input relation;
2. reuse existing semantic implementations instead of creating command-local
   definitions of regex, concatenation, numeric conversion, null, or order;
3. retain source-located diagnostics and reject unsupported options before
   planning or execution;
4. preserve stable pipeline order through ordering-sensitive commands;
5. add explicit cardinality and retained-byte ceilings before row expansion;
6. fail resource-limit breaches atomically rather than publish truncated
   results;
7. preserve immutable tenant, index, time, index-time, visibility, knowledge,
   and search-start authority;
8. expose the same syntax through validation, suggestions, inspection,
   history, saved searches, export, field analysis, and the browser editor; and
9. qualify `0.3` on one complete clean, reachable runtime revision and
   authorize public advertisement only after a later documentation-only
   evidence revision binds terminal-success receipts to that exact revision.

## Non-goals

Version 0.3 does not include:

- `lookup`, `inputlookup`, `outputlookup`, or automatic lookup execution;
- subsearches, `append`, `appendcols`, `appendpipe`, `union`, or `multisearch`;
- `join`, `selfjoin`, `transaction`, or other correlation commands;
- `makeresults`, `gentimes`, `metadata`, or another generating root;
- `collect`, deletion, summary-index writes, email, scripts, or external calls;
- `tstats`, data models, acceleration, or metric-index commands;
- schema-wide wildcard command expansion unless explicitly listed below;
- arbitrary PCRE behavior outside the existing bounded RE2-compatible
  profile;
- unbounded multivalue construction or row expansion; or
- a claim of full syntax coverage for any command whose initial bounded slice
  is specified below.

## Proposed command surface

| Order | Command | Initial v0.3 slice | Primary reuse | Relative effort |
| ---: | --- | --- | --- | --- |
| 1 | `regex` | quoted literal pattern, optional exact field, `=` or `!=` | `match`, `rex`, regex budgets, filter | small |
| 2 | `reverse` | argument-free | stable pipeline order, `tail`/`sort` ordinals | small |
| 3 | `accum` | one exact numeric field, optional `AS` | `streamstats sum(field)` | small |
| 4 | `strcat` | bounded fields/literals and one destination | period concatenation, `tostring` | small |
| 5 | `addinfo` | four standard immutable search fields | admitted job snapshot | small |
| 6 | `fillnull` | explicit exact fields and one literal fill value | missing/null projection | small-medium |
| 7 | `addtotals` | row totals over explicit exact fields | v0.2 numeric conversion/arithmetic | small-medium |
| 8 | `delta` | one exact field, optional `AS`, bounded `p` | ordered window plus subtraction | medium |
| 9 | `makemv` | literal delimiter and bounded `allowempty` | typed multivalue transport | medium |
| 10 | `mvexpand` | one exact field and optional bounded `limit` | controlled `ArrayJoin`-like operator | medium-large |

Relative effort includes parser, AST, planner, compiler, executor, product
surfaces, compatibility fixtures, and acceptance evidence. It is not a
calendar estimate.

## Command targets

### `regex`

The initial surface should accept:

```text
regex "pattern"
regex field="pattern"
regex field!="pattern"
```

An omitted field means `_raw`. Patterns are quoted literals and use the same
normalization, RE2 subset, byte ceiling, work-unit estimator, and aggregate
query budget as `match` and `rex`. The command is a filter and does not create
captures.

Positive matching keeps rows for which the field contains an unanchored
match. Negative matching must receive an explicit missing/null contract; the
target Splunk behavior includes rows whose field is absent. The implementation
must lower that rule deliberately rather than inherit ordinary `search !=`
semantics.

Deferred forms include dynamic patterns, field wildcards, `NOT` syntax, PCRE
features outside the shared profile, and command-local regex limits.

### `reverse`

The initial surface is the argument-free command:

```text
reverse
```

It reverses the complete established input order and changes no fields or
rows. It must operate on the logical pipeline ordinal, including after
`sort`, `head`, `tail`, `dedup`, aggregation, and a previous `reverse`. It must
not infer order from `_time` or another public field.

The compiler may invert a materialized ordinal or establish a reverse ordinal
at the stage boundary, but later order-sensitive commands must observe the
result. Existing admitted-row and result ceilings remain authoritative; the
command does not create an unbounded materialization exemption.

### `accum`

The initial surface should accept:

```text
accum field
accum field AS output
```

It computes a running numeric sum in pipeline order. Without `AS`, the source
field is replaced. The implementation should share the exact value-eligibility,
null, missing, nonnumeric, order, and non-finite behavior of the equivalent
supported `streamstats sum(field)` form.

Only one exact unquoted field and one exact unquoted destination are accepted.
Wildcards, quoted command fields, multiple inputs, and command-specific numeric
coercion are deferred.

### `strcat`

The initial surface should accept two through 32 source operands followed by
one exact destination:

```text
strcat host ":" port endpoint
strcat allrequired=true service "/" route route_key
```

A source operand is an exact unquoted field or a quoted String literal. The
command reuses the v0.1 bounded period-concatenation conversion and output-byte
rules. It must not introduce a second String conversion contract.

`allrequired=true` writes the destination only when every source field is
present and non-null. `allrequired=false` uses the documented Splunk missing
field behavior, which must be pinned by an oracle fixture before the normative
contract is finalized. The option defaults to Splunk's documented default.

Wildcard fields, calculated source expressions, quoted field names, and more
than 32 source operands are deferred.

### `addinfo`

The initial surface is:

```text
addinfo
```

It adds these fields to every row:

- `info_min_time`: the resolved earliest search boundary in Unix seconds;
- `info_max_time`: the resolved latest search boundary in Unix seconds;
- `info_search_time`: the immutable captured search-start Unix second; and
- `info_sid`: the admitted public search-job identifier.

The values come from the immutable server-resolved search snapshot and job
authority. They are not evaluated from ClickHouse wall-clock time or accepted
from authored SPL. The normative contract must define overwrite behavior when
an input row already contains one of these names.

No options are accepted.

### `fillnull`

The initial surface should require at least one explicit exact field:

```text
fillnull host route
fillnull value="unknown" host route
```

The default fill value and authored `value=` literal type must be pinned to
Splunk behavior. Each listed missing or explicit-null field is replaced with
the fill value; present empty Strings, numeric zero, false, and present
multivalue values are not null.

The first slice accepts at most 64 exact unquoted fields. It deliberately
defers the no-field-list form because applying a value to every schema field
requires dataset-wide schema discovery and has different streaming behavior.
It also defers field wildcards, calculated fill values, and container
rewriting.

### `addtotals`

The initial surface should compute row totals over an explicit field list:

```text
addtotals bytes_in bytes_out
addtotals fieldname=total_bytes bytes_in bytes_out
```

It accepts one through 64 exact unquoted fields. `row=true` is the only
supported row option and `col=false` is the only supported column option. The
default destination is `Total`; `fieldname=` may select another exact field.

The implementation reuses v0.2 numeric eligibility and guarded arithmetic.
The normative contract must pin how missing, null, nonnumeric, Boolean,
multivalue, non-finite, and all-ineligible rows contribute. Output is one
nullable numeric field and the input cardinality and order are unchanged.

Deferred forms include implicit all-numeric-fields selection, wildcard field
names, `row=false`, `col=true`, `labelfield`, `label`, and a summary result row.

### `delta`

The initial surface should accept:

```text
delta field
delta field AS output
delta field AS output p=3
```

`p` is a literal positive integer bounded by the existing maximum supported
row window. The default is one previous row. The result is current minus prior
in established pipeline order. Rows without the requested predecessor have a
missing or null result according to the oracle-pinned Splunk behavior.

The default destination spelling is `delta(field)`. The implementation reuses
v0.2 subtraction, including numeric eligibility, zero handling, non-finite
transport, and sanitized malformed-value failure. It must not create a looser
numeric conversion path.

Only one exact unquoted source and destination are accepted. Multiple fields,
dynamic `p`, negative or zero `p`, and quoted command fields are deferred.

### `makemv`

The initial surface should accept delimiter splitting of one field:

```text
makemv tags
makemv delim="," tags
makemv delim=":" allowempty=true product_info
```

The delimiter is a nonempty quoted literal with a bounded UTF-8 byte length;
the default is one space. `allowempty` defaults to false. Input must be a
present scalar String; missing, null, non-String, and already-multivalue input
behavior must be pinned before activation.

Each result is a typed multivalue String field. The parser and executor enforce
a maximum member count, aggregate member bytes, and retained result bytes.
Exceeding a hard ceiling fails the query atomically and never returns a partial
array.

The initial slice defers regex `tokenizer=`, `setsv=true`, internal fields,
multiple fields, quoted command fields, and implicit coercion of containers.

### `mvexpand`

The initial surface should accept:

```text
mvexpand tags
mvexpand tags limit=100
```

For a multivalue input, the command emits one row per selected member while
copying every other field. Member order becomes row order. `limit` is a
literal nonnegative integer; zero or omission means no user-requested
truncation but never bypasses Open Splunk's hard expansion ceilings.

The implementation must define and enforce, before publication:

- a maximum members expanded from one input row;
- a maximum output rows produced by one command;
- a query-wide maximum across multiple `mvexpand` stages;
- retained-byte and generated-SQL ceilings;
- cancellation during expansion; and
- exact behavior for missing, null, scalar, empty, and null-containing arrays.

Hard ceilings fail the query atomically. Open Splunk does not reproduce
Splunk's memory-limit behavior by silently truncating a result chunk and
emitting a warning.

Only one exact unquoted field reference is accepted. The referenced field may
have been produced by an earlier supported command (for example, `eval`), but
the command does not accept an inline calculated expression. Internal fields,
wildcard field arguments, calculated command arguments, and unbounded
compatibility claims are deferred.

## Shared semantics and implementation constraints

### Exact command fields

All command field arguments in this plan initially use the existing exact
unquoted command-field grammar. Version 0.2 single quotes remain limited to
scalar expressions and `eval` destinations. Extending quoted fields across
command grammars is a separate compatibility change and should not be hidden
inside v0.3 command work.

### One semantic implementation per operation

- `regex` shares pattern validation and matching with `match`/`rex`.
- `accum` shares running-sum behavior with `streamstats`.
- `strcat` shares conversion and concatenation with period expressions.
- `addinfo` reads the immutable search snapshot.
- `fillnull` shares the existing missing/null model.
- `addtotals` and `delta` share v0.2 numeric conversion and arithmetic.
- `makemv` and `mvexpand` share one typed multivalue representation.

Command parsers may provide command-specific syntax and diagnostics, but must
not fork these semantic rules.

### Ordering

`reverse`, `accum`, `delta`, and `mvexpand` are order-sensitive. Every logical
relation reaching one of these commands must carry a deterministic private
ordinal. The ordinal is not a public field, cannot collide with authored
names, and must survive paging without becoming part of cursor-visible schema.

### Resource accounting

Every command counts against the existing source, token, pipeline-stage,
relational-depth, generated-SQL, execution-time, memory, result-row, and result-
byte limits. The final contract must add command-specific trusted counters for
regex work, concatenation operands, projection assignments, lag distance,
multivalue members, and expanded rows where existing counters are insufficient.

Limits are checked in the parser or planner when source alone determines the
cost and again at runtime for data-dependent cardinality. Later `head`,
filtering, or projection cannot hide an earlier atomic resource failure.

## Delivery sequence

### Phase 1 — low-risk aliases and projections

Implement `regex`, `reverse`, `accum`, `strcat`, and `addinfo`. This phase
should prove that command aliases reuse existing semantic helpers and that
stable ordinals survive all current pipeline stages.

### Phase 2 — null, numeric, and lag operations

Implement explicit-field `fillnull`, row-only explicit-field `addtotals`, and
bounded `delta`. This phase depends on the activated v0.2 arithmetic profile
and must close the remaining Splunk-oracle questions for missing, null,
nonnumeric, and default-output behavior.

### Phase 3 — controlled multivalue expansion

Implement delimiter-only `makemv` and bounded `mvexpand` together. Shipping
`makemv` without a consumer would provide limited user value; shipping
`mvexpand` without construction and resource fixtures would leave the row-
expansion boundary under-specified.

The server continues to advertise `0.2` throughout all three development
phases. Activation then uses two immutable revisions so that neither a Git
object nor its post-commit CI result must be embedded in itself:

1. Runtime/release revision `R` atomically changes the runtime identity to
   `0.3` and the machine-readable evidence phase to
   `qualification-candidate`. `R` is committed, clean, reachable, fully tested,
   and used to build and read back retained, access-bounded CI qualification
   artifacts. Candidate artifacts must not enter a stable tag, package, or
   deployment channel.
2. After CI for `R` is terminal-success, direct-child documentation-only
   revision `E` records `R` and its exact receipts and changes the evidence
   phase to `accepted`. A strict verifier proves that `E` changes no runtime,
   normative semantic artifact, verifier, or schema and that all recorded
   identities and hashes bind to `R`. Stable publication is allowed only from
   accepted, reachable `E`.

Later CI descendants retain full Git history and verify the exact accepted `E`
as an ancestor, rather than treating the descendant as a new evidence commit.
That ancestor replay emits the immutable E/manifest binding and cannot request
publication; stable publication remains restricted to the release-tagged `E`
checkout itself.

The phase is publication evidence, not a second runtime compatibility value:
the exact `R` artifact reports `0.3` both before and after `E`. The release
pipeline, rather than the already-built artifact, enforces the transition from
quarantined candidate to publicly distributable release. This is the only
exception to the ordinary `0.2` development identity, and it exists solely to
make exact-artifact qualification satisfiable without a false acceptance
claim.

Application and SPL compatibility versions are separate authorities. The
accepted v0.2 prerequisite is application `0.1.0` at release tag `v0.1.0`;
the v0.3 runtime is application `0.2.0` at release tag `v0.2.0`. CI must bind
that exact mapping before building either release or OCI artifacts and reject
every other valid SemVer. Candidate quarantine is expressed only by the
activation phase, never by changing the qualified application version.

## Compatibility and acceptance deliverables

Before activation, v0.3 requires:

1. `docs/spl-compatibility-v0.3.md` with stable rule IDs and normative syntax,
   type, null, order, limit, and diagnostic behavior;
2. `internal/spl/testdata/compatibility-v0.3.json` with exact rule-ID parity;
3. parser, planner, compiler, executor, and reference-model fixtures for every
   supported and deliberately unsupported form;
4. pinned ClickHouse integration tests for order, null, numeric, regex, and
   multivalue behavior;
5. adversarial tests for expansion bombs, oversized delimiters, regex work,
   lag bounds, private-field collisions, cancellation, and SQL depth;
6. saved-search, history rerun, export, inspection, field-analysis, and
   knowledge-prelude composition coverage;
7. browser syntax, suggestions, examples, and source-range coverage;
8. a sanitized end-to-end corpus that exercises all ten commands through
   public result paging;
9. a migration note listing intentionally unsupported options and any source
   transition; and
10. a `vX.Y.Z` tag on a `main` commit whose CI run concluded success, which is
    the whole release gate; see [`docs/releasing.md`](releasing.md).

## Deferred follow-ons

Once `delta` has established a reusable bounded lag operator, `autoregress`
is the strongest immediate follow-on:

```spl
| autoregress count AS previous_count p=1
```

A first slice could accept one field and one lag before adding lag ranges.
Other natural follow-ons after the multivalue foundation are `nomv`, selected
`mvappend`/`mvindex`/`mvjoin`/`mvdedup` functions, and bounded `mvcombine`.

Exact CSV `lookup` remains the next materially different architectural tier.
It should follow its knowledge-object plan rather than being folded into this
single-relation command release.

## Source notes

Primary Splunk behavior references for the final compatibility contract:

- [`regex`](https://help.splunk.com/en/splunk-enterprise/spl-search-reference/10.2/search-commands/regex)
- [`reverse`](https://help.splunk.com/en/splunk-enterprise/spl-search-reference/10.2/search-commands/reverse)
- [`accum`](https://help.splunk.com/en/splunk-enterprise/spl-search-reference/10.2/search-commands/accum)
- [`strcat`](https://help.splunk.com/splunk-enterprise/spl-search-reference/10.0/search-commands/strcat)
- [`addinfo`](https://help.splunk.com/en/splunk-enterprise/spl-search-reference/10.2/search-commands/addinfo)
- [`fillnull`](https://help.splunk.com/en/splunk-enterprise/spl-search-reference/10.4/search-commands/fillnull)
- [`addtotals`](https://help.splunk.com/en/splunk-enterprise/search/spl-search-reference/10.2/search-commands/addtotals)
- [`delta`](https://help.splunk.com/en/splunk-enterprise/spl-search-reference/10.2/search-commands/delta)
- [`makemv`](https://help.splunk.com/en/splunk-enterprise/spl-search-reference/10.2/search-commands/makemv)
- [`mvexpand`](https://help.splunk.com/en/splunk-enterprise/spl-search-reference/10.2/search-commands/mvexpand)

Documentation is an input to compatibility design, not an executable oracle.
Ambiguous null, missing, multivalue, order, default-value, and type behavior
must be pinned by a licensed Splunk differential result or retained as an
explicit Open Splunk deviation before activation.
