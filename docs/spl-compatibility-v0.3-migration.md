# SPL compatibility v0.3 migration guide

**Compatibility target:** authored searches `0.3`

**Knowledge-expression target:** unchanged `0.1`

**Prepared:** August 12, 2026

Version 0.3 adds ten bounded single-relation commands: `regex`, `reverse`,
`accum`, `strcat`, `addinfo`, `fillnull`, `addtotals`, `delta`, `makemv`, and
`mvexpand`. The exact behavior is defined by
[`spl-compatibility-v0.3.md`](spl-compatibility-v0.3.md). This guide describes
source review and rollout; it does not widen the contract.

## Before upgrading from v0.2

1. Retain a recoverable v0.2 deployment before upgrading.
2. Validate every retained saved search with the candidate v0.3 backend.
3. Review exact command fields: v0.3 command positions do not accept single
   quotes or wildcards, even though v0.2 scalar expressions may.
4. Review pipelines that use a public field named `fields`. Insert an exact
   upstream `table`/`fields` projection when that field is intentional.
5. Run representative multivalue searches against production-shaped data and
   confirm the hard member, row, and retained-byte limits below.
6. Retain the exact source revision and release tag used for the upgrade.

Saved-search launch uses the persisted object definition associated with its
trusted ID. Client SPL accompanying that launch is not an execution authority.
History rerun reconstructs the retained definition and records the compiler
identity of the rerun. Old jobs are never silently recompiled with a different
authored compatibility version.

## Command transitions

| Intent | v0.3 source | Review notes |
| --- | --- | --- |
| Filter `_raw` with RE2 | `regex "timeout"` | Pattern must be a quoted literal. |
| Filter one field | `regex message!="debug"` | Negative form keeps missing/null fields. |
| Reverse current order | `reverse` | No arguments; it is not shorthand for sorting `_time`. |
| Running total | `accum bytes AS running_bytes` | Same numeric/null/row limits as supported `streamstats sum(bytes)`. |
| Build a String | `strcat host ":" route endpoint` | Last word is the destination; there is no `AS`. |
| Require all concatenation inputs | `strcat allrequired=true host ":" route endpoint` | Option must be first. |
| Add search metadata | `addinfo` | Existing `info_*` fields are overwritten from the immutable snapshot. |
| Fill explicit nulls | `fillnull value="unknown" host route` | A field list is mandatory and the fill is a quoted String. |
| Compute one row total | `addtotals fieldname=total in out` | Only `row=true` and `col=false`; all-ineligible rows yield 0. |
| Ordered difference | `delta total AS change p=2` | `p` is 1..10,000; `AS` and `p=` may be written in either order. |
| Split a String list | `makemv delim="," allowempty=true tags` | Output is nullable `List<String>` with hard atomic limits. |
| Expand a String list | `mvexpand tags limit=100` | At most two stages; member order becomes row order. |

## Deliberately unsupported spellings

| Command | Deferred or rejected in v0.3 | Supported alternative |
| --- | --- | --- |
| `regex` | dynamic pattern, `NOT` command syntax, wildcard/quoted field | Use one quoted literal and `=` or `!=`. |
| `reverse` | count or field arguments | Establish order with `sort`, then use argument-free `reverse`. |
| `accum` | multiple inputs, wildcard/quoted input, command-local coercion | One exact field; convert in a prior supported `eval` when appropriate. |
| `strcat` | calculated source expressions, wildcard fields, more than 32 sources, `AS` | Precompute fields with `eval`; the final word is the destination. |
| `addinfo` | options or authored metadata values | Use argument-free `addinfo`; values are server-owned. |
| `fillnull` | no-field form, wildcard fields, non-String/dynamic fill, container rewrite | List one through 64 exact fields. |
| `addtotals` | implicit numeric fields, wildcard fields, `row=false`, `col=true`, labels, summary row | List one through 64 exact inputs and keep row-only options. |
| `delta` | multiple fields, dynamic/zero/negative lag, quoted fields | Use one exact field and literal `p=1..10000`. |
| `makemv` | `tokenizer=`, `setsv=true`, empty/dynamic delimiter, multiple/internal fields, existing lists | Use a nonempty quoted literal delimiter on one public scalar String. |
| `mvexpand` | wildcard/internal field or inline calculated expression, more than two stages, heterogeneous/container members, warning-based truncation | Expand one exact field (including one produced by an earlier supported command) with an optional `limit=0..1000`. |

## Null, type, and value changes to review

- `fillnull` distinguishes missing/null from empty String, zero, false, and
  present empty list. It does not replace present flattened containers.
- `addtotals` publishes nullable Float64 schema, ignores non-finite and
  ineligible inputs, and yields numeric zero when every input is ineligible.
- `delta` publishes null until its requested predecessor exists and reuses the
  v0.2 subtraction contract.
- `makemv` distinguishes whole-cell null from present empty `[]`. Export emits
  JSON null or array, not a String containing JSON.
- `mvexpand` preserves scalar String/Number/Bool/time as one row. Dynamic lists
  may contain only String and explicit-null members; general heterogeneous
  lists remain unsupported.

## Multivalue hard limits

`makemv` fails the entire job before publication when any of these is exceeded:

- 1,000 members or 1 MiB member payload in one row;
- 100,000 members or 8 MiB member payload across the stage result; or
- 64 MiB for the complete retained public relation at the stage.

`mvexpand` admits at most two stages and enforces:

- 1,000 source members per input row;
- 10,000 emitted rows per stage;
- 15,000 cumulative emitted rows across stages; and
- 64 MiB of retained public-row bytes per stage.

An authored `limit` chooses a smaller published member prefix but does not
waive the 1,000-member source-value validity ceiling. A later `head`, filter,
or projection cannot hide a preceding breach. These failures are errors, not
warning-plus-truncation compatibility.

## Product behavior

- Suggestions advertise only grammar-valid command/options and stop at literal
  value or terminal positions.
- Timeline accepts event-preserving v0.3 commands unless they replace `_time`;
  `mvexpand` is timeline-ineligible.
- Field catalog reports `makemv` output as List. Exact-value field summary is
  scalar-only and rejects the List without scalarizing it.
- Inspection shows public reads/writes and never includes literals or private
  ordering/presence fields.
- Public paging and export preserve stable row/member order and typed null/list
  values.

## Rollback

Running jobs execute under the binary and immutable authority that admitted
them. Rolling back to a v0.2 binary does not execute a persisted v0.3 AST or
SQL because neither is stored. A saved search containing a v0.3 command will
fail validation under v0.2. Rebuild-based export, inspection, and analysis of a
v0.3 job fail closed on compiler-version mismatch rather than reinterpret the
source. Restore a v0.3 binary to re-open those versioned derived products.
