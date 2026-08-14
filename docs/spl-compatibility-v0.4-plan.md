# Open Splunk SPL compatibility v0.4 plan

**Status:** implementation plan; not an activation claim

**Target authored-search identity:** `0.4`

**Target knowledge identity:** `0.2`

**Prerequisite:** SPL compatibility `0.3` is accepted and distributable

## Decision

Version 0.4 is the bounded exact-lookup release. Its only required new
pipeline command is `lookup`; the release also adds immutable CSV lookup
assets and selector-driven automatic lookup enrichment. It does not introduce
a second input relation, row fan-out, mutable lookup writes, or caller-selected
physical storage.

The initial explicit syntax is:

```spl
| lookup service_catalog service_id AS service_key
    OUTPUTNEW owner tier team
```

```spl
| lookup service_catalog region AS event_region code AS status_code
    OUTPUT description AS status_description
```

`OUTPUT` replaces listed destinations. `OUTPUTNEW` writes only destinations
that are missing or null. One through four ordered key mappings and one through
sixteen ordered output mappings are accepted. Every name is an exact unquoted
field-name token; wildcards, calculated arguments, dynamic lookup names, and
implicit output discovery are rejected.

## Semantics

- A lookup resolves by visible logical name through immutable server authority.
- Matching is case-sensitive over exact scalar String keys. Authors must use a
  prior `eval tostring(...)` when conversion is intended.
- A missing, null, non-String, container, or multivalue key produces no match.
- No match preserves the input row and its established order.
- Published lookup versions contain unique composite keys, so one input row
  can match at most one lookup row.
- Explicit and automatic lookups share one lowering and the same limits.
- Automatic lookups run after calculated-field knowledge and before authored
  base-search filtering. The first slice rejects automatic lookup-to-lookup
  chaining by freezing every automatic key against the same input relation.
- A running job and every retained derived product remain bound to the exact
  lookup object/version and asset digest captured at admission.

## Resource limits

The first release admits at most 8 MiB of UTF-8 CSV data, 100,000 data rows,
64 columns, 64 KiB per decoded cell, and 1 MiB per decoded row. A search may
apply at most sixteen automatic and explicit lookup stages in total, evaluate
at most 64 key components per event, resolve at most 6,400,000 asset cells,
and materialize at most 6,400,000 external-table cells, including one private
match marker per retained asset row. Lookup resolution, retained row bytes,
and emitted argument bytes are independently bounded; a later filter,
projection, or `head` cannot hide an earlier breach. All breaches fail
atomically.

Per-tenant management ceilings are 64 concurrent stages, 2,048 physical asset
identities, 8,192 immutable physical versions, 2 GiB of staged plus published
canonical bytes, 2,048 logical lookup identities, and 8,192 immutable logical
definition versions. Create, replace, and enable mutations share a 4,096-row
normal ceiling; the other 4,096 rows are reserved for disable/delete so all
2,048 retained identities can complete both terminal transitions. Deleted
logical identities remain counted in the lifetime identity ceiling.

## Delivery sequence

1. close the v0.3 qualification/evidence transition;
2. add strict CSV normalization, immutable asset versions, staging,
   publication, cleanup, and content digests;
3. add explicit `lookup` parsing, planning, analysis, suggestions, and
   non-executable validation;
4. add server-owned resolution and executable-authority sealing;
5. add bounded ClickHouse lowering and automatic lookup injection;
6. add management upload/mapping/preview UI and API routes;
7. bind lookup versions through inspection, history, export, and field
   analysis; and
8. complete adversarial, load, cancellation, recovery, browser, and release
   acceptance gates before advertising either new compatibility identity.

## Deferred

`inputlookup`, `outputlookup`, wildcard/CIDR/temporal matching, external and
scripted lookups, mutable assets, duplicate-match selection, automatic lookup
chaining, subsearch membership, `append`, `join`, and
`transaction` remain unsupported.
`autoregress` is a stretch follow-on and is not on the v0.4 critical path.
