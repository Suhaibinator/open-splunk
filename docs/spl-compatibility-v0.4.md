# Open Splunk SPL compatibility v0.4

**Status:** normative executable contract; active

**Authored-search compatibility:** `0.4`

**Product release:** `0.4.0`

**Rule inventory:** `internal/spl/testdata/compatibility-v0.4.json`

This contract incorporates unchanged v0.1 through v0.3 authored-search
behavior and the unchanged Tier-1 knowledge behavior. It adds exact immutable
CSV lookup enrichment. Parser acceptance alone is not compatibility: a lookup
must resolve to a visible, versioned server authority and compile to a bounded,
row-preserving operation before a job can be admitted.

## Compatibility identity

### `SPL-V04-PROFILE-001` — Cumulative authored profile

Production-authored searches use the cumulative `0.4` profile. It includes
every unchanged v0.1 through v0.3 authored-search rule and the lookup rules in
this contract; callers cannot select individual feature versions. Every
admitted job, bootstrap response, history record, and export surface reports
the same authored compiler identity. Inspection and analysis retain and
validate that identity and fail closed if a rebuild would cross it.

Tier-1 calculated fields remain on their smaller closed reusable-expression
grammar; activating authored compatibility `0.4` does not enable the broader
authored syntax inside stored calculated fields. Knowledge snapshots retain
internal compiler and immutable lookup evidence for exact reconstruction, not
a second public compatibility profile.

The production bootstrap reports authored SPL `0.4` independently from two
runtime capabilities. `SERVER_FEATURE_KNOWLEDGE_FIELD_OBJECTS` advertises the
complete Tier-1 field-knowledge family. The additive
`SERVER_FEATURE_LOOKUP_MANAGEMENT` advertises the complete lookup management,
admission, snapshot, execution, retained-product, and browser family and is
emitted only alongside the field-knowledge capability. A partial server
composition omits the capability for the incomplete family instead of
overstating what it can execute through every retained product.

## `lookup`

### `SPL-V04-LOOKUP-SYNTAX-001` — Exact lookup syntax and semantics

Accepted syntax is:

```text
lookup definition key_column AS event_field
    [key_column AS event_field ...]
    (OUTPUT|OUTPUTNEW)
    output_column [AS event_field]
    [output_column [AS event_field] ...]
```

The definition name, lookup columns, and event fields are exact unquoted names.
There are one through four key mappings and one through sixteen output
mappings. Every key mapping requires `AS`; output `AS` defaults the event
field to the preceding lookup column. The command requires exactly one
`OUTPUT` or `OUTPUTNEW` marker. Duplicate lookup columns or destinations are
rejected. The private case-insensitive `__os_`
namespace, wildcards, quoted command fields, calculated arguments, and the
ambiguous open-schema event payload named `fields` are rejected.

Management accepts a logical definition only when its stable spelling (`AS`
included for every key and output mapping) fits the authored-search ceiling of
16,384 UTF-8 bytes. `OUTPUT` and `OUTPUTNEW` therefore cannot be key columns or
key destinations because the grammar would consume them as the required mode
marker. They remain representable as output columns or destinations after that
marker.

`OUTPUT` replaces every listed destination on a match, including with an empty
String. `OUTPUTNEW` replaces a destination only when its current value is
missing or null. A present empty String, zero, false, time, or multivalue is not
new. On no match, every destination and the complete input row are preserved.

## Keys and matching

Lookup keys are ordered tuples of one through four present scalar String
values. Matching is case-sensitive over exact UTF-8 bytes. Empty String is a
valid key member. Missing, null, Number, Bool, time, bytes, multivalue, or
container input produces no match; v0.4 performs no implicit conversion.

Published versions reject duplicate exact composite keys. The canonical key is
an unambiguous length-prefixed byte sequence. Implementations may index its
SHA-256 digest, but exact canonical bytes remain the final match authority.
Consequently a lookup never fans one event into multiple rows.

## Asset contract

One published lookup version contains a bounded RFC 4180-style UTF-8 CSV
document. An optional leading UTF-8 BOM is removed during normalization. The
header contains 1 through 64 nonempty, trimmed, control- and format-free names
of at most 255 UTF-8 bytes that are unique by exact bytes. Every data row has
exactly the header width. Cells are valid UTF-8 and contain no NUL byte. An
empty CSV cell is a present empty String; v0.4 has no CSV null sentinel.
A blank physical data line is therefore one empty cell and is valid only for a
one-column asset; it is never silently discarded.

The hard limits are:

- 8 MiB encoded input and canonical output;
- 100,000 data rows;
- 64 columns;
- 64 KiB decoded UTF-8 bytes per cell; and
- 1 MiB decoded UTF-8 bytes per row.

Normalization produces deterministic canonical CSV bytes, a source digest,
and a canonical-content digest. Upload is staged privately. Publication
verifies its expected stage/version, schema, key uniqueness, row count, and
digests before atomically changing the visible logical-name winner. Failed and
abandoned stages never resolve for search.

Per tenant, storage admits at most 64 concurrent staged assets, 2,048 physical
asset identities, 8,192 immutable physical versions, and 2 GiB total staged
plus published canonical bytes. The logical catalog independently retains at
most 2,048 lookup identities and 8,192 immutable definition versions. Ordinary
create, replace, and enable mutations stop at 4,096 logical versions. The
remaining 4,096-version terminal reserve is available only to disable and
delete mutations, enough for both terminal transitions of every retained
identity. Deleted identities remain counted in the lifetime identity bound.

## Resolution and execution authority

Authored SPL cannot select a tenant, asset ID, version, digest, table, database,
dictionary, or path. Search admission resolves the logical definition under
the authenticated tenant, principal, app, sharing, and lifecycle rules. The
resolved authority contains the exact immutable asset version and mappings and
is detached from mutable catalog memory before compilation.

An unresolved or validation-only lookup may be analyzed for diagnostics but
cannot produce an executable compiled query. Compiler execution cloning,
history, inspection, field analysis, export, and preview all revalidate the
same authority commitment.

### `SPL-V04-LOOKUP-BOUNDS-001` — Bounded resolution and execution

The operation preserves public row count, event identity, and established
order. A query may contain at most sixteen explicit plus automatic lookup
stages and evaluate at most 64 exact key components per event. Resolved assets
may contain at most 6,400,000 cells in aggregate, and compilation admits at
most 6,400,000 external-table cells in aggregate, including one private match
marker for each retained asset row. Lookup input rows, retained asset/argument
bytes, generated SQL, and result bytes have independent hard ceilings. Limit
failures are atomic and cannot be hidden by a downstream command.

## Automatic lookups

An automatic definition uses the same asset, mapping, match, overwrite, and
resource contract. It is selected only from trusted event metadata and runs
after Tier-1 calculated fields but before the authored base-search predicate.
Definitions are ordered deterministically by normalized name and stable ID.
The v0.4 snapshot rejects lookup-to-lookup dependencies, so all automatic
lookups consume the same pre-lookup relation.

Every definition belongs to an active app. An active definition blocks that
app from being archived; definitions from an archived app never resolve, even
when globally shared. Retained lookup identities also prevent their app from
being permanently deleted.

## Deliberately unsupported

The release does not support `inputlookup`, `outputlookup`, mutable or external
assets, URL/scripted lookup providers, wildcard, CIDR, temporal, fuzzy or
case-insensitive matching, duplicate-match selection, implicit fields,
automatic lookup chaining, multivalue keys, subsearch lookup inputs, or any
lookup-driven row generation.
