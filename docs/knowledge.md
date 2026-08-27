# Knowledge and lookups

Open Splunk currently supports search-time field knowledge and immutable CSV
lookups. This is a major-version-zero release/source contract, not a
cross-release compatibility promise. Anything not described here is unsupported
and must fail before publication or execution; the server never applies a valid
prefix of an invalid snapshot.

`SERVER_FEATURE_KNOWLEDGE_FIELD_OBJECTS` advertises the complete extraction,
alias, calculated-field, catalog, snapshot, execution, retained-product, and
browser family. `SERVER_FEATURE_KNOWLEDGE_QUARANTINE` independently advertises
the recovery-keyed quarantine preparation and execution routes and is emitted
only alongside field knowledge. `SERVER_FEATURE_LOOKUP_MANAGEMENT` advertises
the complete lookup family and is likewise emitted only alongside field
knowledge. Capabilities are runtime readiness signals, not authorization or
caller-selectable profiles.

## Security and identity

Knowledge is resolved after authentication, app authorization, effective-index
authorization, and time resolution. A request supplies SPL and app intent; it
cannot supply trusted object bodies, versions, catalog revisions, asset paths,
or snapshot digests. Knowledge may filter or enrich already-authorized rows but
cannot grant an app, index, event, field, object, or lookup.

Management and preview routes are administrator-only and derive tenant, owner,
and manageable-app scope from the authenticated principal. A forbidden object
is indistinguishable from an absent one. Private definitions and lookup bytes
are not copied into audit, history, inspection, errors, or browser summaries.

Names and selectors are valid UTF-8, bounded, control-free, and stored without
Unicode normalization. Stable ASCII whitespace is trimmed where the contract
calls for normalized text. Names, app/owner IDs, fields, selector values,
lookup columns, and lookup keys are bytewise case-sensitive.

Object types have separate namespaces. Active-name uniqueness is:

| Sharing | Identity |
| --- | --- |
| private | tenant, app, owner, type, name |
| app | tenant, app, type, name |
| global | tenant, type, name |

For a principal searching app A, resolution considers owned private objects in
A, then app-shared objects in A, then tenant-global objects. The first object
with the same `(type, name)` wins as a whole; definitions are never merged.
Draft, disabled, deleted, unauthorized, and archived-app definitions do not
execute. Index-impossible selectors are pruned before shadowing so an
irrelevant private object cannot hide an applicable shared object.

## Selectors and stages

A selector may constrain `index`, `host`, `source`, and `sourcetype`.
Dimensions are ANDed and patterns within a dimension are ORed. Empty dimensions
are unrestricted. Patterns are anchored, case-sensitive Unicode-scalar globs:
`*` matches zero or more scalars, `?` matches one, and `\` quotes `*`, `?`, or
`\`. Missing/null metadata does not match a constrained dimension.

The normalized limits are 16 patterns per dimension, 64 total, 255 UTF-8 bytes
per pattern, 8 KiB per selector, and 1,024 aggregate wildcard work units per
snapshot. Runtime matching has independent per-value, per-event, and per-query
input/work ceilings. It never scans once per alternative or truncates an
input; exceeding a budget fails the query atomically.

The relation is enriched in this order:

1. server-authorized stored events;
2. matching regular-expression and JSON extractions, in parallel;
3. matching field aliases, in parallel;
4. matching calculated fields, in parallel;
5. matching automatic lookups, in deterministic order;
6. authored base-search predicate; and
7. authored pipeline commands.

Objects in one stage observe the completed previous stage, never another
object in their own stage. Same-stage chaining and cycles are rejected.
Definitions that may write the same destination in the same stage must have
provably disjoint selectors. Authored `eval` is separate and retains
left-to-right assignment behavior.

Knowledge cannot write canonical event/security/storage roots or the private
`__os_` namespace. This includes `event_id`, `index`, `_time`, `_indextime`,
`host`, `source`, `sourcetype`, `service`, `severity`, `level`, `message`,
`_raw`, tracing IDs, collector/batch identity, and the public `fields`
container. Canonical values may be read only where the object type allows it.

Missing, present null, and present non-null are distinct. With
`overwrite=false`, a destination is written only when missing; present null is
preserved. With `overwrite=true`, a produced value replaces missing, null, or
non-null. A selector miss, missing source, extraction miss, or missing
expression result never erases an existing destination.

## Field knowledge

### Regular-expression extraction

Extraction reads `_raw` only and uses bounded Go/ClickHouse-compatible RE2.
Named captures define outputs; repeated/fan-out capture behavior and
multivalue input are unsupported. Pattern bytes, normalized program work,
capture count, output count, and per-row captured bytes are bounded. A normal
no-match leaves destinations unchanged; malformed stored authority or a hard
resource breach fails the complete operation.

### JSON extraction

JSON extraction reads `_raw` and follows an exact bounded path into one valid
JSON document. It emits one supported typed scalar rather than flattening an
arbitrary object or array. Invalid JSON, nesting/value limits, unsupported
container results, and ambiguous paths follow the definition's declared
failure semantics without exposing source content.

### Field aliases

An alias copies one prior-stage exact field to one destination without changing
the source. Missing input produces no value. Overwrite policy is applied to the
destination and aliases cannot chain within the alias stage.

### Calculated fields

Calculated fields use the closed `KnowledgeExpressionProfile`. It is a bounded
reusable-expression grammar rather than the complete authored SPL surface.
It supports the documented typed scalar comparisons and functions needed for
safe enrichment, but not pipeline commands, arbitrary authored syntax, or
same-stage dependencies. A missing expression result does not materialize a
null unless the expression itself produces present null.

## Lookup assets and definitions

A lookup definition names one immutable normalized CSV asset version plus one
through four key mappings and one through sixteen output mappings. Search
resolution uses the visible active logical definition and exact asset version;
SPL cannot select a tenant, physical identity, digest, path, database, or
table.

CSV is bounded RFC 4180-style UTF-8. An optional leading BOM is removed. The
header has 1 through 64 unique, nonempty bounded names. Each row has exactly
that width. Cells contain valid UTF-8 without NUL; empty cell is present empty
String and there is no null sentinel. Limits are 8 MiB encoded/canonical bytes,
100,000 rows, 64 columns, 64 KiB per decoded cell, and 1 MiB decoded bytes per
row.

Normalization produces deterministic canonical CSV, source digest, and
canonical-content digest. Upload is staged privately. Publication verifies
the expected stage/version, schema, row count, key uniqueness, and digests
before atomically changing the visible winner. Failed or abandoned stages never
resolve.

Matching uses a one-to-four-part tuple of present scalar Strings with exact,
case-sensitive UTF-8 bytes. Empty String is valid. Missing, null, numeric,
Boolean, time, bytes, multivalue, and container inputs do not match. Published
versions reject duplicate exact composite keys, so enrichment is row
preserving. `OUTPUT` overwrites on match; `OUTPUTNEW` writes only to missing or
null destinations.

Automatic lookups use the same matching, overwrite, asset, and resource rules.
They are selected from trusted event metadata, sorted by normalized name and
stable ID, and cannot chain. An active lookup definition prevents its app from
being archived; retained lookup identities prevent permanent app deletion.

Per tenant, lookup storage is bounded to 64 concurrent stages, 2,048 physical
asset identities, 8,192 immutable asset versions, and 2 GiB of staged plus
published canonical data. The logical catalog is independently bounded to
2,048 identities and 8,192 versions. Ordinary writes stop before the terminal
disable/delete reserve. Deleted identities continue to count toward the
lifetime identity bound.

## Publication and lifecycle

Objects receive opaque integer revisions. Every body, scope, or lifecycle
mutation creates the next immutable revision and advances the tenant catalog
revision. Those counters are optimistic-concurrency and snapshot mechanics,
not product or API versions. Mutations require `expected_version`; idempotency, committed
receipt, object version, body digest, dependency graph, projection, audit, and
catalog-revision changes are one SQLite transaction.

Definitions move among `DRAFT`, `ACTIVE`, `DISABLED`, quarantined, and deleted
states through explicit validated operations. Publication validates identity,
definition shape, selectors, output collisions, dependencies, compiler
support, resource budgets, and active-name uniqueness before visibility.
Opaque future definitions can be retained safely but cannot be activated by a
server that cannot validate them.

Corrupt active authority fails admission. Protective quarantine is a terminal,
audited fail-closed transition that removes corrupt authority from resolution
without rewriting its forensic bytes. The general mutation journal and the
bounded rejected-attempt/recovery journals never contain definition content.

Catalog list and graph operations are bounded, revision-consistent, and
keyset-paged. Signed cursors bind caller scope, normalized filters, ordering,
page choices, and the first page's catalog revision/state. A changed or restored
catalog branch invalidates continuation rather than mixing snapshots.

## Immutable search snapshots

Admission resolves one detached snapshot containing exact object versions,
lookup asset versions, normalized selectors, dependency/order evidence,
compiler identity, resource charges, and a deterministic digest. Objects and
edges have canonical binary ordering; protobuf unknown fields, malformed
digests, inconsistent identities, and unsupported format version fail closed.

Snapshot formats carry a private implementation discriminator. It rejects
unrecognized state but does not promise interchange across source revisions.
One admitted job retains its snapshot and compiled authority for execution,
inspection, history, field analysis, and export. Catalog changes cannot alter
it. A saved-search launch or history rerun is a new admission that reauthorizes
current app/index intent and resolves the current catalog; it never treats a
historical summary as execution authority.

Browser and audit projections expose only bounded redacted summaries and
provenance ordinals. Definition bodies, selectors, asset bytes, object
identities not currently authorized for disclosure, generated SQL, event
values, and private compiler columns are not exposed.

The physical catalog, snapshots, resolver concurrency/cache, definition bytes,
dependency graph, selector work, preview, and runtime enrichment all have hard
tenant, app, owner, query, and response ceilings. A limit never causes silent
object omission or partial enrichment.

The executable knowledge fixture is
`internal/knowledge/testdata/compatibility.json`. Field knowledge and lookup
runtime coverage must reach the production resolver, planner, compiler,
ClickHouse executor, retained lifecycle, inspection/history/export, and browser
boundaries.

## Deliberate exclusions

The current baseline does not implement event types, tags, macros, workflow
actions, automatic key/value extraction, index-time extraction, multivalue
extraction, arbitrary transforms, scripts/external calls, lookup chaining,
mutable or external lookup providers, data models, or acceleration. Cross-app
role grants and full Splunk configuration-layer precedence are also not
claimed. See the [roadmap](roadmap.md).
