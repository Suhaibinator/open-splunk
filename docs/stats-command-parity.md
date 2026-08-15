# SPL1 `stats` command parity inventory

This file and [`testdata/spl-stats-parity.json`](../testdata/spl-stats-parity.json)
are the authoritative inventory for `stats` parity work. They describe the
current end-to-end parser, planner, compiler, and runtime surface as of
2026-08-11; they do not widen the compatibility contract by themselves. The
bounded multivalue-BY rows below are separately adopted by
`SPL-V02-STATS-BY-MULTIVALUE-001`; other rows still require their own normative
compatibility rule before they can supersede an older contract boundary.

The target is exactly **SPL1 Splunk Enterprise 10.0**, with only the
repository-pinned **10.2 eval-in-stats** page used to define embedded eval
expressions. SPL2 syntax is out of scope.

## Primary sources

| ID | Official Splunk source |
|---|---|
| `stats-10.0` | [SPL1 `stats` command, Enterprise 10.0](https://help.splunk.com/en/splunk-enterprise/spl-search-reference/10.0/search-commands/stats) |
| `functions-10.0` | [Statistical and charting functions, Enterprise 10.0](https://help.splunk.com/en/splunk-enterprise/search/spl-search-reference/10.0/statistical-and-charting-functions/statistical-and-charting-functions) |
| `aggregate-10.0` | [Aggregate functions, Enterprise 10.0](https://help.splunk.com/en/splunk-enterprise/search/spl-search-reference/10.0/statistical-and-charting-functions/aggregate-functions) |
| `multivalue-10.0` | [Multivalue stats and chart functions, Enterprise 10.0](https://help.splunk.com/en/splunk-enterprise/spl-search-reference/10.0/statistical-and-charting-functions/multivalue-stats-and-chart-functions) |
| `event-order-10.0` | [Event order functions, Enterprise 10.0](https://help.splunk.com/en/splunk-enterprise/spl-search-reference/10.0/statistical-and-charting-functions/event-order-functions) |
| `time-10.0` | [Time functions, Enterprise 10.0](https://help.splunk.com/en/splunk-enterprise/spl-search-reference/10.0/statistical-and-charting-functions/time-functions) |
| `time-bins-10.0` | [Search using time bins and spans, Enterprise 10.0](https://help.splunk.com/en/splunk-enterprise/search/search-manual/10.0/specify-time-ranges/search-using-time-bins-and-spans) |
| `limits-10.0` | [`limits.conf`, Enterprise 10.0.0](https://help.splunk.com/en/splunk-enterprise/administer/admin-manual/10.0/configuration-file-reference/10.0.0-configuration-file-reference/limits.conf) |
| `eval-stats-10.2` | [Use stats with eval expressions and functions, Enterprise 10.2](https://help.splunk.com/en/splunk-enterprise/search/search-manual/10.2/calculate-statistics/use-stats-with-eval-expressions-and-functions) |

Status values are deliberately narrow:

- `implemented`: the documented syntax has an end-to-end implementation in
  the current bounded Open Splunk slice. A linked oracle item can still govern
  undocumented edge behavior.
- `currently_missing`: the documented syntax is rejected or has no complete
  runtime implementation.
- `oracle_required`: official sources are silent or contradictory. Do not
  choose behavior by inference.

Evidence is tracked independently from parity status:

- **syntax/IR tested** means parser and logical-plan tests preserve the
  documented spelling, source metadata, and effective option value;
- **lowering tested** means compiler tests prove the intended ClickHouse shape;
- **pinned runtime tested** means the generated query ran against the
  digest-pinned ClickHouse 26.7.3.19 image over stored events.
- **UI rendering tested** means the generic statistics table recognizes the
  sparkline marker transport or effective multivalue delimiter metadata and
  applies the corresponding presentation without replacing the typed cell.

Runtime evidence proves the stated Open Splunk behavior. It does not resolve a
linked oracle item or make a provisional algorithm semantically identical to
Splunk.

## Command grammar and options

The documented complete grammar is:

```text
| stats
  [partitions=<num>]
  [allnum=<bool>]
  [delim=<string>]
  ( <stats-agg-term>... | <sparkline-agg-term>... )
  [BY <field-list>]
  [dedup_splitvals=<boolean>]

<stats-agg-term> := <stats-func>(<evaled-field> | <wc-field>) [AS <wc-field>]
```

| Surface | Documented default | Status | Current boundary |
|---|---:|---|---|
| Multiple aggregate terms; comma or whitespace separators | n/a | `implemented` | At most 16 measures. |
| `partitions=<num>` before aggregates | `0`, resolving to `default_partitions=1` | `implemented` | The parser preserves an authored unsigned 64-bit value; the plan resolves `0` to `1` and clamps values above `100` to `100`. Each stats stage contributes `min(partitions,4)` to a sealed whole-query `max_threads` hint, multiple stages take the minimum, and the executor never raises a stricter configured cap. This is a bounded ClickHouse execution approximation, not Splunk stage-local reduce topology; order/approximation remains `O-partitions-order`. |
| `allnum=<bool>` before aggregates | `false` | `implemented` | `true` suppresses each affected numeric result only in a BY group containing a present nonnumeric value; missing/null do not participate. `false` retains eligible numeric values. Participation edges remain `O-measure-values`. |
| `delim=<string>` before aggregates | one space | `implemented` | The effective delimiter is attached only to `list`/`values` output ordinals as sealed presentation metadata. It survives exact rename/table/project operations, clears on overwrite or a non-MV transform, and is reassigned by a downstream stats stage. Go/protobuf/TypeScript transport preserves empty versus absent metadata; the generic statistics table joins string members for display only. Typed MV cells and CSV/JSON exports remain unchanged under `O-delim`. |
| One exact `BY <field-list>` | no `BY` | `implemented` | At most 16 exact fields, either ordinary unquoted names or single-quoted SPL field names. Each resolved input may be a fixed scalar, a fixed string multivalue, or a raw Dynamic scalar/multivalue. |
| Wildcard in `BY` is rejected | unsupported by Splunk | `implemented` | Open Splunk does not expand wildcard grouping fields. |
| Quoted BY fields | governed by SPL field syntax | `implemented` | Single-quoted exact fields are decoded as literal names and share the scalar/MV grouping path. Wildcards and reserved/private names still fail closed. |
| Multivalue BY fields | supported by the `dedup_splitvals` description | `implemented` | Raw Dynamic scalar or scalar-member MV values and fixed `Array(String)` values are expanded. Multiple MV fields form a staged Cartesian product, capped at 10,000 combinations per source event after null filtering and optional split-value deduplication; overflow throws before the first `ARRAY JOIN`. Missing/null/empty inputs emit no group; a nested container rejects the complete operation atomically. Fixed `values()` output can feed a downstream stats `BY`. Unpinned value and order edges remain `O-by-values`. |
| `dedup_splitvals=<boolean>` after `BY` | `false` | `implemented` | `false` preserves duplicate members, including their aggregate multiplicity. `true` deduplicates each BY field before staged Cartesian expansion; scalar fields are unchanged. |
| No `BY`: one global aggregate row; with `BY`: one row per tuple | n/a | `implemented` | Empty-input and null-key details are oracle items. |

Current option evidence:

| Option | Syntax/IR | ClickHouse lowering | Pinned runtime |
|---|---|---|---|
| `allnum` | tested | tested per BY group for numerical functions | tested for `avg`/`range`, including mixed, invalid, empty, and singleton groups |
| `partitions` | tested, including `0`, `100`, `101`, and `MaxUint64`; over-limit values clamp to `100` in the plan | sealed whole-query `max_threads` hint; stage minima and the independent four-thread ceiling are tested | not independently pinned; executor settings are unit-tested and Splunk stage-local topology is not claimed |
| `delim` | tested, including bare and empty quoted strings | ordinal presentation metadata is sealed, cloned, retained-byte charged, type checked, and transported through search jobs and optional protobuf presence | not independently pinned; underlying `list`/`values` execution is pinned separately and focused Go transport/UI tests prove no value coercion |
| `dedup_splitvals` | tested in terminal position | tested for duplicate-preserving and per-field `arrayDistinct` paths, including guarded staged Cartesian expansion | tested over raw Dynamic scalar/MV fields for default and `true` behavior, including duplicate-driven overflow rescued by deduplication |

The pinned multivalue fixtures also cover the exact 10,000-combination ceiling,
10,100-combination and 16-field adversarial overflow, empty final dimensions,
atomic failure behind a downstream `head`, missing-field exclusion, multiple MV
BY fields, and atomic rejection of nested containers. Fixed `Array(String)` input
from `values()` feeding a downstream stats `BY` is compiler-tested; it uses the
same staged expansion path but is not a separate pinned-runtime fixture.

## Aggregate functions and aliases

| Family | Documented SPL1 spelling and aliases | Status | Current boundary or oracle |
|---|---|---|---|
| Average | `avg(field)` | `implemented` | Numeric exact-field input, including single-quoted names, or `avg(eval(bounded scalar))`; `AS` is optional. Boolean eval results are accepted and contribute no numeric value. Value edges remain `O-measure-values`. |
| Mean | `mean(field)`; documented equal to `avg` | `implemented` | Same bounded input surface as `avg`. The stats command table omits it, but the 10.0 aggregate page and stats examples explicitly support it. |
| Count | `count`, `count(field)`, `c(field)` | `implemented` | `c` requires a field. Default alias canonicalizes to `count(field)`. |
| Conditional count | `count(eval(expression))` | `implemented` | Current slice accepts a Boolean where-predicate subset. `AS` is optional; without it, the output preserves the exact authored invocation under `O-alias-schema`. |
| Exact distinct | `distinct_count(field)`, `dc(field)` | `implemented` | Exact string set; default alias canonicalizes to `dc(field)`. |
| Estimated distinct | `estdc(field)` | `implemented` | Bounded `uniqCombined64(17)` string lowering; small and 10,000-value runtime cases pass. Equivalence to Splunk's configured digest and exact-below-threshold rule remains `O-estdc`. |
| Estimated distinct error | `estdc_error(field)` | `implemented` | Local result is `0` below 1,000 estimated values and the HLL17 relative-error constant otherwise; this is provisional under `O-estdc`. |
| Exact percentile | `exactpercN(field)` | `implemented` | Exact nearest-rank sort over eligible `Float64` values; linear aggregate state and suffix/definition contradictions remain `O-percentile`. |
| Approximate percentile | `percN(field)`, `pN(field)` | `implemented` | Integer `N=1..99`; exact-field or eval scalar input. Default alias is `percN(field)` for field input; eval defaults preserve the authored invocation under `O-alias-schema`. Current GK behavior is recorded under `O-percentile`. |
| Upper percentile | `upperpercN(field)` | `implemented` | Provisional bounded GK two-level lowering; it is not claimed equivalent to Splunk's upper digest and remains `O-percentile`. |
| Maximum | `max(field)` | `implemented` | Exact-field input; mixed ordering requires `O-min-max-order`. |
| Minimum | `min(field)` | `implemented` | Exact-field input; mixed ordering requires `O-min-max-order`. |
| Median | `median(field)` | `implemented` | Bounded GK p50 lowering is approximate/provisional; even-cardinality and algorithm behavior remain `O-percentile`. |
| Mode | `mode(field)` | `implemented` | Exact frequency counts with state linear in distinct lexical values; local ties choose the bytewise-lowest value under `O-mode-tie`. |
| Range | `range(field)` | `implemented` | Numeric exact-field or eval scalar input; documented as numeric `max-min`. Value edges remain `O-measure-values`. |
| Sample standard deviation | `stdev(field)` | `implemented` | Numeric exact-field or eval scalar input; value edges remain `O-measure-values`. |
| Population standard deviation | `stdevp(field)` | `implemented` | Numeric exact-field or eval scalar input; value edges remain `O-measure-values`. |
| Sum | `sum(field)` | `implemented` | Numeric exact-field or eval scalar input. All-ineligible output is an Open Splunk choice covered by `O-measure-values`. |
| Sum of squares | `sumsq(field)` | `implemented` | Numeric exact-field or eval scalar input; value edges remain `O-measure-values`. |
| Sample variance | `var(field)` | `implemented` | Numeric exact-field or eval scalar input; value edges remain `O-measure-values`. |
| Population variance | `varp(field)` | `implemented` | Numeric exact-field or eval scalar input; value edges remain `O-measure-values`. |
| First seen | `first(field)` | `implemented` | Follows current pipeline processing order using a deterministic row ordinal; otherwise-tied rows remain `O-event-order-tie`. |
| Last seen | `last(field)` | `implemented` | Follows current pipeline processing order using a deterministic row ordinal; otherwise-tied rows remain `O-event-order-tie`. |
| Ordered multivalue | `list(field)` | `implemented` | First 100 eligible values in pipeline order; fixed publication ceilings remain in force. |
| Distinct multivalue | `values(field)` | `implemented` | Exact lexical values with fixed publication ceilings. |
| Chronologically earliest value | `earliest(field)` | `implemented` | Requires canonical time; equal-time/member ties require `O-time`. |
| Earliest occurrence time | `earliest_time(field)` | `implemented` | Returns nullable Unix seconds for the earliest eligible occurrence and requires canonical `_time`; tie/value edges remain `O-time`. |
| Chronologically latest value | `latest(field)` | `implemented` | Requires canonical time; equal-time/member ties require `O-time`. |
| Latest occurrence time | `latest_time(field)` | `implemented` | Returns nullable Unix seconds for the latest eligible occurrence and requires canonical `_time`; tie/value edges remain `O-time`. |
| Counter rate | `rate(field)` | `implemented` | Uses chronological eligible endpoints and returns null for fewer than two points or zero duration. Counter resets deliberately use the provisional no-reset formula pending `O-rate-reset`. |

The only documented aliases are `c` for `count`, `dc` for
`distinct_count`, and `pN` for `percN`. `mean` is a separately documented
equivalent function, not an alias spelling. `std` is not part of the pinned
grammar.

Phase 2 evidence is green at the implementation layer while the listed oracle
questions remain open:

| Functions | Syntax/IR | Lowering | Pinned runtime | Unresolved semantics |
|---|---|---|---|---|
| `exactpercN` | tested for field and eval; `AS` optional | exact nearest-rank | field, empty, singleton, stored Dynamic | suffix/rank definition in `O-percentile`; eval runtime not separately pinned |
| `upperpercN`, `median` | tested for field and eval; `AS` optional | bounded GK, explicitly provisional | field, empty, singleton, stored Dynamic | digest, threshold, and even-cardinality behavior in `O-percentile`; eval runtime not separately pinned |
| `estdc`, `estdc_error` | tested for field and eval; `AS` optional | `uniqCombined64(17)` plus provisional error ratio | field, empty, singleton, 10,000 values, stored Dynamic | Splunk digest/exact-threshold/error behavior in `O-estdc`; eval runtime not separately pinned |
| `mode` | tested for field and eval; `AS` optional | exact `sumMap` frequency state | field, empty, singleton, ties, stored Dynamic | tie choice in `O-mode-tie`; eval runtime not separately pinned |
| `first`, `last` | tested for field and eval; `AS` optional | pipeline row ordinal plus `argMin`/`argMax` | field and eval; processing order deliberately differs from chronology | otherwise-tied processing order in `O-event-order-tie` |
| `earliest_time`, `latest_time` | tested for field and eval; `AS` optional | immutable chronological key | field and eval; empty, ineligible, singleton, equal-time | equal-time/missing-time behavior in `O-time` |
| `rate` | tested for field and eval; `AS` optional | chronological endpoint formula | field and eval; empty, ineligible, singleton, equal-time, reset | reset behavior in `O-rate-reset` |

## Eval, wildcard, alias, BY, and sparkline surface

| ID | Surface | Status | Current boundary |
|---|---|---|---|
| `eval-count-explicit` | `count(eval(Boolean predicate)) AS output` | `implemented` | Uses the bounded where-predicate grammar; at most 32 predicate leaves query-wide. |
| `eval-count-default-alias` | Conditional count without `AS` | `implemented` | The output name preserves the exact authored aggregate invocation. The normative default-name schema remains `O-alias-schema`. |
| `eval-numeric-explicit` | `numeric-function(eval(bounded scalar)) AS output` | `implemented` | Bounded scalar grammar for `pN`/`percN`, `exactpercN`, `upperpercN`, `median`, `sum`, `avg`/`mean`, `range`, `sumsq`, `stdev`, `stdevp`, `var`, `varp`, and `rate`. Boolean results are accepted and are ineligible for numeric contribution. |
| `eval-numeric-default-alias` | Implemented numeric eval subset without `AS` | `implemented` | The output name preserves the exact authored aggregate invocation, including case and spacing; this deterministic local schema remains governed by `O-alias-schema`. |
| `eval-general` | Supported non-count field-taking stats functions over `eval(<expression>)` | `implemented` | Every currently supported non-count field-taking family accepts a bounded scalar, including Boolean results, with optional `AS`; numeric families treat Boolean results as ineligible. Runtime evidence is representative by family; distribution eval variants are compiler-tested but not separately pinned at runtime. `count(eval(...))` remains the separate Boolean predicate subset. |
| `wildcard-input` | Aggregate input `<wc-field>` such as `avg(*lay)` | `implemented` | Closed schemas expand case-sensitively in upstream `OutputFields` order. Raw/open schemas use the sealed inventory described by `wildcard-open-schema`, ordered by aggregate ordinal and exact field bytes. Zero matches, unsupported names, more than 16 expanded measures, duplicate sources, and output collisions fail closed. |
| `wildcard-implicit` | Deprecated bare non-count function, for example `avg` as `avg(*)` | `implemented` | Uses the same closed-schema or sealed raw/open expansion as an implicit `*`. Bare `count` remains row count and is not expanded. |
| `input-quoted-exact` | Exact single-quoted aggregate input such as `avg('Product Name')` | `implemented` | Decodes one literal SPL field name for ordinary and sparkline aggregates. Wildcard, empty, reserved, private, and invalidly escaped names fail closed. |
| `alias-exact` | `AS exact_output` | `implemented` | Duplicate output names are rejected. |
| `alias-quoted` | Quoted alias such as `AS "total bytes"` | `implemented` | Double-quoted literal output names are decoded and preserved through downstream schema. Empty, control-containing, reserved, and private outputs fail closed; single-quoted aliases are not this syntax. |
| `alias-wildcard` | Wildcard `AS <wc-field>` mapping | `implemented` | Requires equal input/output star counts. Captures are deterministic left-to-right with the final literal suffix-anchored, then substituted into an exact unquoted output; invalid names and collisions fail closed. The normative capture/default-name policy remains `O-wildcard` and `O-alias-schema`. |
| `wildcard-open-schema` | Aggregate wildcard over a raw/open event schema | `implemented` | A sealed two-phase execution first compiles the exact canonical prefix and immutable read scope into a name-only inventory over visible canonical, Dynamic, calculated, and knowledge-generated fields. Matching is anchored and case-sensitive; rows are ordered by aggregate ordinal then exact field bytes. At most 16 expanded measures are accepted, while a 17th inventory pair is an overflow sentinel and never truncates silently. The final plan is rebuilt only from the opaque expansion, and legacy, knowledge, and snapshot replay paths bind that expansion into execution authority. Metadata poison, zero matches, invalid/private names, read-scope drift, changed knowledge, and forged prefix/result rows fail atomically. Splunk's normative ordering and matching remain `O-wildcard`. |
| `alias-same-source-twice-rejection` | Same source aggregate renamed to two outputs | `implemented` | Canonical field-source equivalences (`c`/`count`, `dc`/`distinct_count`, `pN`/`percN`, `avg`/`mean`, quoted/unquoted exact fields, and equivalent sparklines at one span) are rejected. Different functions, different inputs, and different sparkline spans remain valid. Eval-source equivalence remains `O-alias-schema` and is not conflated. |
| `by-exact-tuple` | One or more exact grouping fields | `implemented` | Up to 16 exact unquoted or single-quoted fields, each scalar or multivalue at runtime. |
| `by-multivalue` | Multivalue grouping and split-value deduplication | `implemented` | Raw Dynamic scalar/MV and fixed string-array inputs; duplicate-preserving by default, optional per-field deduplication, and staged Cartesian products capped at 10,000 combinations per source event before expansion. Missing inputs are excluded and nested containers fail atomically. See `O-by-values` for unpinned member and order edges. |
| `sparkline-term` | `sparkline(...) [AS field]` aggregate term | `implemented` | Stats-only AST/plan/lowering over the unmodified canonical `_time`; ordinary measures and sparklines share one event scan. Output uses the typed `Array(String)` MV transport with `##__SPARKLINE__##` first. Publication edges remain `O-sparkline-publication`. |
| `sparkline-count` | Scoped and unscoped `count` sparkline | `implemented` | `sparkline(count)`/`sparkline(count())` count rows; `sparkline(count(field))` and `sparkline(c(field))` count eligible values. Missing bins publish zero. |
| `sparkline-functions` | `c`, `count`, `dc`, `mean`, `avg`, `stdev`, `stdevp`, `var`, `varp`, `sum`, `sumsq`, `min`, `max`, `range` | `implemented` | Stored-event runtime covers every semantic lowering plus the `mean` alias. Exact `dc` retains the 100,000-distinct-values cap per BY group and bucket. Min/max ordering remains `O-min-max-order`; non-count missing bins remain `O-sparkline-publication`. |
| `sparkline-span` | Optional explicit span and automatic span selection | `implemented` | Explicit `us`, `ms`, `cs`, `ds`, `s`, `m`, `h`, `d`, and `mon` spans are supported. Automatic selection uses the official `1s,5s,10s,30s,1m,5m,10m,30m,1h,1d,1mon` order and the first grid with at most 100 numeric bins. Day/month bins use search-local civil boundaries; multi-day/month anchors remain `O-sparkline-grid`. Oversized explicit grids fail closed. |
| `sparkline-statistics-rendering` | Marker-driven chart display in generic statistics results | `implemented` | Only a typed MV whose first element is `##__SPARKLINE__##` becomes an inline SVG. The chart has an accessible value label, breaks segments at missing buckets, and labels an all-missing series. This is presentation-only: the typed `Array(String)` remains the authoritative result and export value. |
| `sparkline-wildcard-alias` | Wildcard input and output alias | `implemented` | Uses the same closed-schema or sealed raw/open inventory, capture substitution, post-expansion 16-measure limit, and collision checks as ordinary stats. Raw/open discovery shares the ordinary stats authority path; its sparkline execution is compiler-tested but not separately pinned at runtime. The normative mapping remains `O-wildcard`. |

Sparkline evidence is parser/plan/lowering tested and pinned-runtime tested over
stored events. Coverage includes mixed ordinary and sparkline measures, global
and multivalue-BY results, per-field deduplication, missing bins, empty global
input, downstream `mvcount`/`where`/`table`, automatic and explicit spans, the
special marker, and point/byte overflow guards. Quoted exact inputs and quoted
aliases use the same compiler path; their current evidence is syntax/plan and
lowering tested. The generic statistics UI has focused marker/segment tests and
renders the transport as an accessible inline chart without replacing or
coercing the typed value used by result APIs and exports.

Wildcard evidence covers parser provenance, deterministic glob
capture/substitution helpers, logical expansion, post-expansion resource and
collision validation, ordinary and sparkline compiler lowering, and sealed
raw/open execution. The inventory compiler rebuilds and compiles the canonical
prefix under the retained scope, validates all Dynamic metadata before
filtering, transports only bounded ordered field names, and keeps the 17th pair
as an overflow sentinel. Manager tests cover legacy and knowledge-generated
fields, scope/read admission, shared runtime budgeting, snapshot retention, and
changed-knowledge replay rejection. Digest-pinned ClickHouse tests cover
anchored case-sensitive multistar matching with literal regex metacharacters,
exact bytewise order, wildcard alias captures, overflow, and nonmatching
metadata poison.

Delimiter evidence covers the compiler's ordinal contract, exact
rename/table/project preservation, overwrite and downstream-stats reset,
execution-seal tampering, executor type binding, search-job validation,
clone/retained-byte accounting, optional protobuf presence, browser schema
validation, typed statistics adaptation, and empty-versus-absent UI display.
No export adapter was changed; exports continue to consume the typed arrays.

## Remaining parity work

No documented `stats` syntax in this inventory remains `currently_missing`.
The oracle-required rows below continue to delimit behavior that the official
sources do not specify precisely enough to claim normative Splunk parity.

`partitions` has an end-to-end bounded execution effect, but it is a
query-global ClickHouse `max_threads` approximation rather than Splunk's
stage-local reduce topology. `delim` has an end-to-end display-only effect,
while typed MV publication and exports deliberately remain unchanged. Their
unresolved equivalence questions stay in `O-partitions-order` and `O-delim`.

The OracleRequired table below is a separate set of semantic unknowns, not an
implementation backlog that may be resolved by inference. In particular,
percentile/estimated-distinct algorithms, mode and order ties, counter resets,
BY value edges, default-name schema, partition ordering, and sparkline
publication/grid edges remain open even where the bounded implementation is
green.

## Documented defaults that affect parity

These are Splunk defaults, not permission to relax Open Splunk limits:

| Splunk setting | 10.0 default | Inventory consequence |
|---|---:|---|
| `default_partitions` | `1` | Authored `partitions=0` resolves to this value and caps the complete ClickHouse query at one thread when a stats stage is present. This is not stage-local partition topology. |
| `partitions_limit` | `100` | The parser retains any unsigned 64-bit authored value; the plan clamps the effective value to this maximum. |
| `approx_dc_threshold` | `1000` | Local `estdc_error` uses the threshold, but local `estdc` uses a different ClickHouse state; see `O-estdc`. |
| `dc_digest_bits` | `10` (1 KiB) | Local `uniqCombined64(17)` is not asserted equivalent; see `O-estdc`. |
| `check_for_invalid_time` | `false` | Splunk can return likely incorrect/random results when time is absent; Open Splunk currently requires canonical time. Do not emulate randomness without an explicit decision. |
| `list_maxsize` | `100` | Current `list` element limit matches. |
| `sparkline_maxsize` | inherits `list_maxsize` (`100`) | Open Splunk emits at most 100 numeric bins plus the marker. Whether the marker consumes this maximum remains `O-sparkline-publication`. |
| `maxvalues` / `maxvaluesize` | `0` / `0` (unlimited) | Open Splunk intentionally retains stricter publication ceilings below. |
| stats `maxresultrows` | `50,000` | Splunk documents an in-memory/spill threshold, not a stats output truncation rule. |
| `natural_sort_output` | `true` | Applies when output reaches `maxresultrows`; general output order remains `O-output-order`. |
| `perc_method` | `nearest-rank` | Conflicts with interpolated examples; see `O-percentile`. |
| `perc_digest_type` | `tdigest` | Conflicts with the aggregate page's radix-tree narrative; see `O-percentile`. |

## Oracle-required behavior

| ID | Why no behavior may be inferred |
|---|---|
| `O-percentile` | The pinned pages disagree on integer `1..99` versus floating `0..100` suffixes, nearest-rank versus interpolated examples, and radix-tree versus `tdigest`. Open Splunk uses exact nearest-rank for `exactperc`, ordinary GK for `perc`, bounded GK for `median`, and a provisional two-level GK result for `upperperc`; none of the provisional variants is labeled Splunk-equivalent. |
| `O-estdc` | The official threshold/digest settings do not establish equivalence to ClickHouse `uniqCombined64(17)` or the local HLL17 error estimate. Exact-below-1,000 and error behavior require a differential oracle. |
| `O-min-max-order` | Numeric coercion is documented, but mixed values and symbols do not have a complete portable ordering contract. Open Splunk has a deterministic typed mixed-order contract. |
| `O-measure-values` | Missing, null, empty string, input MV members, containers, all-ineligible output, and `allnum` participation are not comprehensively specified for every function. |
| `O-by-values` | The sources do not completely pin missing/null/empty keys, null or mixed-type MV-member representation, member ordering, resulting BY-row ordering, or a per-event expansion ceiling. Open Splunk excludes missing/null/empty inputs, lexicalizes supported scalar members, preserves duplicates unless `dedup_splitvals=true`, forms a Cartesian product across MV fields, caps that product at 10,000 combinations per source event before expansion, and rejects nested containers atomically. |
| `O-output-order` | No general BY-row order is promised below the conditional natural-sort threshold. |
| `O-empty-input` | Global row existence and each aggregate's empty result are not comprehensively specified. |
| `O-time` | Equal-time/member ties and missing or noncanonical time are incomplete or configuration-dependent. |
| `O-event-order-tie` | `first`/`last` are defined by processing order, but otherwise-tied processing rows have no portable normative tie-breaker. |
| `O-rate-reset` | The pinned description refers to counter resets/largest values without enough detail to reproduce reset accumulation. Open Splunk currently uses the no-reset endpoint formula and does not claim reset parity. |
| `O-mode-tie` | The official mode page does not define tie-breaking. |
| `O-wildcard` | Expansion order, zero matches, wildcard alias substitution, default names, and collisions are not normative. Open Splunk matches case-sensitively with deterministic left-to-right captures and a suffix-anchored final literal. Closed schemas retain upstream order; raw/open inventory is ordered by aggregate ordinal then exact field bytes across visible canonical, Dynamic, calculated, and knowledge-generated names. Zero matches, invalid/private names, limits, duplicates, collisions, metadata poison, or authority drift fail closed. Splunk's exact raw/open ordering and matching still require an oracle. |
| `O-delim` | Sources call `list`/`values` results multivalue entries while also exposing a delimiter; logical-versus-display behavior is unclear. Open Splunk retains typed MV arrays and exports, and carries the effective delimiter only as optional flat-table presentation metadata. |
| `O-alias-schema` | Default names for eval/wildcard terms, quoting, case, and collision behavior are not comprehensively specified. |
| `O-partitions-order` | The docs describe parallel reduction but do not define its effect on order-sensitive or approximate functions. Open Splunk uses the smallest `min(partitions,4)` across stats stages as a sealed whole-query thread cap; it does not claim Splunk reduce topology or partition-dependent ordering. |
| `O-sparkline-publication` | The marker is documented and `sparkline_maxsize` limits emitted values, but the sources do not say whether the marker consumes one slot or how non-count missing/null buckets are encoded. Open Splunk emits up to 100 numeric/lexical bucket strings plus the marker, uses `0` for missing count/DC bins, and uses an empty string for missing numeric/min/max bins. |
| `O-sparkline-grid` | Local day/month boundaries are documented, but the anchor for explicit spans of multiple civil days or months is not. Open Splunk uses deterministic proleptic-Gregorian civil day/month indices in the search timezone. |

## Open Splunk resource ceilings to preserve

Parity work must remain inside these bounds unless a separate, reviewed change
explicitly changes resource policy. Limit overflow fails the complete operation
unless the row says the documented behavior is truncation.

| Scope | Current ceiling |
|---|---:|
| SPL source | 16 KiB UTF-8 |
| Syntax tokens | 1,024 |
| Pipeline commands | 64 |
| Stats measures / BY fields | 16 / 16 |
| Scalar nesting | 32 levels |
| `coalesce` arguments | 32 per call |
| `case` branches | 16 per call |
| Unary operator chain | 32 operators |
| Concatenation operands | 32 per expression |
| Eval/where predicate leaves | 32 query-wide |
| Arithmetic operators | 256 query-wide |
| Concatenation operand occurrences | 256 query-wide |
| Membership candidates | 32 per list; 256 query-wide |
| Compiled SQL | 256 KiB |
| Executor query size | 1 MiB |
| Execution time / threads | 120 seconds / 4 |
| Query memory | 1 GiB |
| Rows / bytes read | 250,000,000 / 64 GiB |
| Result rows / result bytes | 10,001 / 128 MiB |
| Flat MV delimiter metadata | 16 KiB UTF-8 per column; invalid or mismatched metadata fails closed |
| Ordinary GROUP BY rows | 10,001; overflow throws |
| Exact `dc` values | 100,000 per group; overflow throws |
| Sparkline bins | 100 bucket values per cell; a separate leading marker yields 101 physical MV elements under `O-sparkline-publication`; oversized explicit grids fail at compile time and a 101-record runtime sentinel throws |
| Sparkline payload | 8 MiB per published cell including the marker; overflow throws; the executor's 128 MiB result cap remains independent |
| Sparkline exact `dc` | 100,000 distinct values per BY group and time bucket; overflow throws |
| `exactperc` state | Linear in eligible values; bounded by the query read and 1 GiB memory ceilings, with no dedicated per-group value cap |
| `mode` state | Linear in distinct eligible lexical values; bounded by the query memory ceiling, with no dedicated per-group distinct cap |
| Multivalue stats `BY` expansion | 10,000 Cartesian combinations per source event after null filtering and optional `dedup_splitvals`; overflow throws before the first `ARRAY JOIN` |
| `values` publication | 10,000 values and 512 KiB per group; 100,000 values and 8 MiB per result |
| `list` publication | first 100 values per group; 512 KiB per group; 100,000 values and 8 MiB per result |

When implementation status changes, update this file and the JSON inventory in
the same change. An item becomes `implemented` only after parser-through-lowering
coverage exists; behavior that depends on ClickHouse execution additionally
requires pinned-runtime evidence. Oracle items remain oracle items until pinned
differential evidence or a documented Open Splunk deviation resolves them.
