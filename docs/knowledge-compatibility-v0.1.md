# Open Splunk knowledge compatibility contract v0.1

**Status:** normative implementation contract; executable runtime corpus pending
**Compatibility version:** `0.1`
**Last updated:** August 9, 2026

This contract defines the first Open Splunk search-time knowledge surface. It
is intentionally narrower than Splunk Enterprise knowledge behavior. Anything
not described here is unsupported and must fail before publication or search
execution; the server never applies a supported prefix of an invalid knowledge
snapshot.

The eventual advertised `0.1` runtime surface contains regular-expression and
JSON field extractions, field aliases, and calculated fields. Catalog APIs and
snapshot metadata may ship before enrichment, but the server must not advertise
`SERVER_FEATURE_KNOWLEDGE_FIELD_OBJECTS` until the complete CRUD, resolution,
snapshot, planner, executor, inspection, lifecycle, and browser family is
configured and tested.

**Current readiness status:** production registers the nine administrator-only
create/get/list/dependencies/dependents/validate/update/set-state/delete routes
as one complete management unit and composes their Store, concrete ready Writer,
app authority, and attempt journal. The two graph routes and Validate are
registered but remain capability-unadvertised. Get/List and the two graph reads
remain the only knowledge paths in the browser administrator-bearer allowlist;
Validate and every mutation are excluded. Validate is also absent from the
backend's generic outer administrator-route map because the inner knowledge-
attempt boundary owns its authentication, administrator authorization, and
`ActionValidate` journaling before body decode.
The dormant hidden detail view consumes both graph routes for an exact object
version, but production bootstrap still omits navigation and the dynamic chunk,
so it issues no knowledge request. Production also constructs and retains a
concrete Resolver, but intentionally does not attach it to production
`searchjobs.Manager`. Bootstrap does not advertise the feature, so the
read-only Knowledge Manager stays out of
navigation despite its app/object-type/lifecycle-state filters and name-
ascending, updated-time-descending, created-time-descending, and object-type-
ascending sort readiness. Recognized definitions can be created
ACTIVE, updated while ACTIVE, or enabled from DRAFT/DISABLED through the
transactional, compiler-proven Writer path; opaque future definitions cannot be
updated or enabled as ACTIVE. A dedicated bounded raw decoder, handler, and
exact sealed-response encoder now expose the Writer's rollback-only candidate-
validation service at the Validate route. Preview now has only an internal
request-only codec and structural forced-ACTIVE envelope validator. It still has
no response codec, handler, catalog/search service, retained-execution
acquisition or caller-authorization integration, route or manifest/bearer
entry, capability, navigation/UI request, Resolver attachment, or execution
path. The nonempty compiler, snapshot-finalization, and execution gates remain
closed, so no knowledge object affects search results.

## Security boundary

When knowledge admission is enabled, the server resolves knowledge after
authenticating the principal, authorizing the selected app, and deriving the
effective index scope. A request supplies authored SPL and an app intent; it
cannot supply resolved object bodies, versions, catalog revisions, physical
storage identities, or a trusted snapshot digest.

Knowledge never grants access to an app, index, event, field, or object. A
selector can only remove rows from the set to which an object applies. It
cannot add an index to the server-authorized search scope. Unauthorized private
objects, and authorized draft, disabled, or deleted objects, are omitted without
disclosing private object existence. A visible active object that is corrupt or
unsupported fails admission with a payload-free corruption category; it is
never silently omitted or partially applied.

Version `0.1` targets the existing trusted single-user deployment. The nine
registered knowledge management routes—get, list, dependencies, dependents,
validate, create, update, change state, and delete—are administrator-only and
are bound to the authenticated tenant and owner. Browser transport attaches its
memory-only administrator bearer only to the four read paths: get, list,
dependencies, and dependents. Validate, create, update, change-state, and delete
remain outside that allowlist. The reserved Preview route must preserve the same
boundary when implemented; its current internal request codec and envelope
validator install no route or browser authority. The local administrator may
publish app-shared and tenant-global objects. Supplied tenant
or owner identity is never authority. A forbidden or cross-tenant object
selector returns the same not-found response as an absent object.

The production router registers all nine object-management handlers only
when their complete dependency unit and constructor-ready concrete Writer are
present; partial configuration registers none of them. This route availability
is independent of the absent Tier-1 bootstrap capability. Each registered path
authenticates before decoding, requires the administrator role, derives scope
from the detached principal and trusted app catalog, and uses the fail-closed
attempt-journal boundary described below.

The schema is ACL-ready, but role-grant enforcement and cross-app export grants
are not claimed until multi-user RBAC is implemented. Within one tenant, a
global object is visible in every readable app. This is an explicit Open
Splunk deviation, not an assertion about Splunk configuration-layer
permissions. Preview's request identifies future owner-scoped retained
execution authority, not an immutable event-snapshot identity or access grant.
A future service must reacquire that authority under the authenticated caller
and preserve its app, index, time, visibility, row, byte, duration, and
concurrency boundaries; draft content must never supply or widen them.

## Text, identity, and case

Object names, descriptions, app IDs, owner IDs, field names, and selector
patterns must be valid UTF-8. Normalization trims only the stable ASCII set TAB
`U+0009`, LF `U+000A`, VT `U+000B`, FF `U+000C`, CR `U+000D`, and SPACE
`U+0020` from both ends. Any remaining C0 control `U+0000..U+001F` or C1
control `U+007F..U+009F` is rejected. Names must remain between 1 and 255 UTF-8
bytes and are otherwise stored exactly. Unicode is not normalized and no
version-dependent Unicode category table participates. Descriptions are
trimmed by the same ASCII rule, may be absent, and are limited to 16 KiB.

Names, app IDs, owner IDs, field names, selector values, and lookup keys are
case-sensitive binary UTF-8 identities. `Status`, `status`, and canonically
equivalent but byte-distinct Unicode spellings are different. SQLite columns
and indexes enforcing identity use binary collation; locale and database row
order never affect resolution.

Object types have separate namespaces. The active-name uniqueness keys are:

| Sharing scope | Unique identity |
| --- | --- |
| private | tenant, app, owner, type, name |
| app | tenant, app, type, name |
| global | tenant, type, name |

A sharing change is a checked identity transition and fails on collision. Old
versions retain their original identity even after the current version changes
scope, is disabled, or is deleted.

Administrator catalog list filters are normalized with the same stable ASCII
trim and control-character rules as stored text. `text_filter` is a
case-sensitive binary UTF-8 substring of the current normalized name or
description. `selector_text_filter` is a case-sensitive binary UTF-8 substring
of at least one individual current normalized selector pattern; it never
matches synthetic delimiters or a concatenation of patterns. Both filters must
be nonempty after trimming and are limited to 255 UTF-8 bytes. All visibility,
state, type, scope, text, and selector predicates are applied to one catalog
revision before keyset ordering and `LIMIT`; filtering a page after retrieval
is invalid.

These predicates use a current-version-only derived projection rather than
decoding definition blobs during traversal. Its definition-derived portion
stores only the normalized description (with absent and normalized-empty both
represented as zero bytes) and the four ordered selector pattern lists; copied
registry fields bind filtering to the exact current identity and version. The
complete projection is verified byte-for-byte against the registry and decoded
definition before a response is returned. A quarantined current row has an
empty description and no selector rows because suspect definition bytes are
never decoded. Each projection also stores and verifies the selector's exact
canonical byte charge, which cannot exceed 8 KiB. Tenant projection accounting
is the exact sum of stored description and selector-value bytes and has a 256
MiB hard ceiling; publication fails atomically before exceeding it.

## Visibility, shadowing, and deterministic order

For a search in app `A` by principal `P`, candidates are:

1. active private objects owned by `P` in `A`;
2. active app-shared objects in `A`; and
3. active tenant-global objects.

For equal `(object_type, name)`, the first candidate in that list wins as a
whole object. Definitions are never merged. Shadowed objects are recorded in a
bounded inspection warning but are not executable. Draft, disabled, and
deleted objects never participate.

Executable objects are ordered first by stage, then by binary normalized name,
then by stable object ID. This order defines snapshot and inspection output.
It does not create same-stage data dependencies. Publication fails when two
objects that may be visible together can write the same destination in the
same stage and their selectors are not provably disjoint, except when one is
the shadowed object with the same `(object_type, name)`. A conservative overlap
check may reject ambiguous wildcard pairs; it must never admit a possible
collision.

## Selectors

A selector has four optional dimensions: `index`, `host`, `source`, and
`sourcetype`. Dimensions are ANDed. Patterns within a dimension are ORed. An
empty dimension is unrestricted. A nonempty dimension does not match a
missing or null canonical metadata field.

Patterns are case-sensitive anchored globs over Unicode scalar values. `*`
matches zero or more scalar values, `?` matches exactly one, and `\` quotes the
next `*`, `?`, or `\`. A trailing `\`, invalid UTF-8, and every other escape
are invalid. Matching is against the complete field value; there is no
substring mode or locale folding. Canonical index, host, source, and sourcetype
values come from the trusted admitted event relation, never similarly named
user fields.

Resolution may discard an object only when its `index` patterns cannot match
any authorized effective index. Host, source, and sourcetype are still checked
per row. This index-impossible pruning occurs before whole-object precedence,
so an irrelevant private candidate cannot shadow an applicable app or global
candidate. Cross-index searches therefore can apply different objects to
different rows.

Selector limits are 16 patterns per dimension, 64 patterns total, 255 UTF-8
bytes per pattern after validation, 8 KiB per normalized selector, and 1,024
aggregate wildcard work units per snapshot. One literal scalar costs one unit,
`?` costs two, and `*` costs four.

## Search-time stages

The `0.1` relation is built in this order:

1. the server-authorized stored event relation;
2. all matching regular-expression and JSON extractions, in parallel;
3. all matching field aliases, in parallel;
4. all matching calculated fields, in parallel;
5. the authored base-search predicate; and
6. authored pipeline commands.

Every object in one stage observes the completed previous-stage relation, not
another object in its own stage. An extraction cannot read an extraction
output, an alias cannot read an alias output, and a calculated field cannot
read a calculated-field output. Same-stage cycles and chains are rejected at
publication. Dependencies may only point from a later stage to an earlier
stage. The authored `eval` command retains its existing left-to-right
assignment semantics and is distinct from the parallel calculated-field
stage.

In `0.1`, extraction input is `_raw` only. Definitions naming another input
field are rejected. Supporting other exact scalar String/Bytes fields requires
a later compatibility version.

All field references use the existing exact SPL field resolver. Any output
whose root is an ASCII-case-insensitive reserved event root, storage/security
column, public `fields` container, or compiler-private `__os_` name is rejected
at publication. In particular, field knowledge cannot write `event_id`,
`index`, `_time`, `_indextime`, `host`, `source`, `sourcetype`, `service`,
`severity`, `level`, `message`, `_raw`, `trace_id`, `span_id`, `collector_id`,
or `batch_id`. Those canonical fields may be read where the object type permits
it. This prevents pre-base-filter enrichment from replacing authorization,
selector, timeline, raw-search, or provenance inputs. The reserved-root list is
the versioned `internal/eventfields` contract and upgrade tests pin it.

## Presence and overwrite semantics

Field state distinguishes missing, present null, and present non-null. Empty
String, empty Bytes, empty array, and empty object are present non-null values.
The initial field objects never coerce missing to null.

When `overwrite=false`, an object writes only if its destination is missing;
a present null destination is preserved. When `overwrite=true`, a produced
value replaces a missing, null, or non-null destination. A false selector,
missing source, failed extraction match, or expression result that is missing
does not erase an existing destination under either policy.

Multivalue extraction input is not supported in `0.1`; encountering one leaves
the destination unchanged and increments a bounded per-object diagnostic
counter. Definitions cannot request fan-out, repeated captures, or multivalue
construction. Aliases are different: they copy an existing array/object value
as one typed value without iterating or coercing its members.

## Regular-expression extraction

Regular expressions use the repository's bounded RE2-compatible dialect and
the same parser, normalization, compile-time work estimator, and runtime
capture ceilings as authored `rex`. PCRE-only constructs, backreferences,
lookaround, executable replacements, unnamed outputs, duplicate named outputs,
and repeated-capture output are unsupported.

`_raw` String values are matched as UTF-8. Every Bytes value, including Bytes
whose payload happens to be valid UTF-8, produces no outputs. Missing, null,
container, and multivalue `_raw` also produce no outputs. No match produces no
outputs. On a successful match every declared named output is a present String;
an empty capture and a named optional group that did not participate both
produce the empty String. These rules exactly match authored `rex` `0.1` and
allow the existing typed extraction path to be reused.

One definition is limited to a 4 KiB source pattern, a 4 KiB normalized
pattern, 4,096 estimated RE2 work units, 16 total capture groups, and 16 named
outputs. Knowledge and authored `rex` share the query ceilings: 64 regex
programs, 262,144 exact aggregate RE2 work units, and 4 MiB of captured bytes
per event. Regex named outputs and JSON extraction outputs share one separate
64-output ceiling. Admission rejects a snapshot that cannot fit those
aggregate ceilings.

## JSON extraction

JSON extraction uses the exact JSON-path grammar and typed-value behavior
documented for authored `spath` in the SPL `0.1` contract. The path is compiled
at publication. It is limited to 17 segments, 256 UTF-8 bytes per unescaped
segment, 4 KiB encoded path bytes, and one named scalar destination.

Missing, null, every Bytes value, malformed JSON, no-match, and a container
selected where a scalar is required produce no output. JSON String,
number, boolean, and explicit null results retain the same typed representation
as authored `spath`; a selected explicit JSON null is a produced present-null
value. Runtime parse failures are bounded diagnostics, not query-wide syntax
failures. Knowledge and authored JSON paths share 32 aggregate evaluation work
units and the 64-output extraction ceiling.

## Field aliases

An alias copies one exact field to one different exact destination and never
removes or renames the source. Source and destination names are limited to 255
UTF-8 bytes and 17 dotted segments. Wildcard field names are unsupported.

A missing source produces nothing. A present null source produces a present
null destination subject to overwrite policy. String, Bytes, boolean, integer,
unsigned integer, floating point, decimal, timestamp, duration, array, and
object values are copied without string coercion or mutation. Source equal to
destination is invalid.

Alias cycles, alias-to-alias chains, and multiple possibly overlapping writers
for one destination are rejected before publication.

Every successful alias write is runtime-accounted after selector evaluation,
source-presence validation, overwrite arbitration, and disjoint-writer winner
selection. A missing source, false selector, preserve-existing write blocked by
any present destination (including present null or a container), and a losing
writer charge zero. Copying one source to two destinations charges twice. For
the one winning value tuple `W`, ClickHouse widens every operand to `UInt128`
and computes:

```text
payload = byteSize(W.value)
        + byteSize(W.relative_names)
        + byteSize(W.relative_types)
        + 1
work    = payload + length(W.relative_names) + 1
```

The final `1` values account for the metadata-version byte and the copy
operation. Per-destination charges are summed once per event, then independently
saturated at 4 MiB plus one payload byte and 1 GiB plus one work unit. The 4 MiB
per-event payload ceiling and 1 GiB cumulative per-query work ceiling admit
equality and fail atomically when exceeded; no alias output is partially
published. Compiler-private charge columns are removed before authored SPL or
result decoding. This accounting does not make nonempty execution available by
itself: the pinned ClickHouse acceptance matrix must prove row-local `byteSize`
for every admitted Dynamic/container representation and lazy non-evaluation of
false, blocked, and losing branches before the finalization gate opens.

## Calculated fields

A calculated field has one exact non-reserved destination and one expression accepted by
the current authored `eval` expression subset. The same lexer, parser, type
rules, function allowlist, source locations, nesting limit, and generated-SQL
limits apply. Search commands, subsearches, macros, raw ClickHouse SQL, external
calls, nondeterministic functions, environment access, and mutation are not
expressions.

Publication parses and plans the expression against the fields available after
the alias stage. Unknown dynamic event fields are permitted where authored
`eval` permits them, but a declared dependency on a later or same-stage field
is rejected. Every calculated field in a snapshot evaluates against the same
alias-stage input, so `b=lower(a)` and `c=upper(b)` cannot be used to chain `c`
through a calculated `b`. After both expressions pass syntax validation,
publication reports the same-stage dependency error for `c`.

Missing, null, numeric, String, Bytes, container, comparison, and conditional
semantics are identical to authored `eval`. A missing result does not erase an
existing destination. A present null result is written subject to overwrite
policy.

One expression is limited to 16 KiB, 32 nested scalar levels, and the existing
64 KiB per-conditional/per-variadic generated-SQL ceilings. One snapshot is
limited to 32 calculated fields, 32,768 exact scalar and Boolean AST node
occurrences, and all knowledge expressions share the authored limit of 32
aggregate eval/where predicate leaves. Nodes and predicate leaves are counted
before optimization, including repeated occurrences. Final authored-plus-
knowledge parameterized SQL is limited to 256 KiB. If combined authored and
knowledge work exceeds a limit, search admission fails before execution with a
source-attributed complexity diagnostic.

## Publication and dependency validity

Definitions have `draft`, `active`, `disabled`, and administrator-recovery
`quarantined` publication states. Delete creates a tombstoned current registry
state; it does not physically erase version bytes. Quarantined objects are
never executable or directly re-enabled. Create commits immutable version one.
Body update, metadata update, scope change, enable, disable, and delete require
the expected current version and commit one new immutable version. A failed
mutation creates no version, catalog revision, or success audit event.

Before an active version becomes visible, the server validates and canonicalizes
all recognized protobuf fields, rejects unknown enum values and unknown future
definition bodies, compiles regex/JSON/expression bodies, checks field and object
dependencies, checks conservative destination and selector overlap, and charges
publication and snapshot budgets. Invalid definitions never become partially
active.

A recognized draft is an editable, non-executable definition, so its body is
required to be structurally normalized and canonically encoded but need not yet
compile or pass executable dependency semantics. Disabled body edits have the
same authoring behavior, and deletion preserves the final inactive bytes.
Administrative `Get` and `List` therefore continue to validate identity,
digest, lifecycle, dependency seals and row order, target identities and
existence, and bounded acyclic graph shape for these inactive versions, but do
not apply expression parsing or executable target-state, sharing-scope, stage,
or field-identity rules. Validation and every transition to `active` rerun all
of those semantic checks. Inactive definitions never enter resolution,
snapshots, execution, or provenance.

Every retained normalized dependency graph is limited to 256 nodes, 1,024
edges, and depth 16; direct and indirect cycles and missing targets are invalid
in every state. For active versions, disabled targets, later-stage dependencies,
and same-stage data dependencies are also invalid. Deleting or
disabling an active dependency fails unless a bounded explicit transaction also
disables all active dependents. `0.1` does not expose cascading mutation through
the browser.

Every dependency target must have the same tenant and be executable everywhere
the source is executable. A private source may depend on private objects of the
same owner and app, app-shared objects of the same app, or global objects. An
app-shared source may depend only on app-shared objects of the same app or
global objects. A global source may depend only on global objects. No source
may depend on a private object owned by another principal, an object from
another app outside that matrix, or any cross-tenant identity. Forbidden and
absent targets produce the same publication error category.

A declared dependency must also prove that the source selector implies the
target selector independently in all four dimensions. An unrestricted target
dimension accepts every source. A universal `*` target accepts every
constrained source, but not an unrestricted source because constrained
dimensions reject missing and null metadata. Every literal source pattern must
match at least one target pattern, while a wildcard source pattern is proven
only by an identical canonical target wildcard. More ambitious wildcard-
language containment fails closed in `0.1`.

The direct `FIELD_INPUT` graph is derived rather than caller-selected. For
every winning later-stage source input that intersects an earlier-stage winner
output and whose selectors are not provably disjoint, resolution derives one
object edge. The source-to-target sharing matrix and selector implication must
both hold. The immutable stored edge set and detached snapshot edge set must
equal this derivation exactly: missing, extra, or stale-version authorities
fail closed. Resolver input order is not authority; the detached snapshot list
is sorted into its canonical dependency order before encoding.

Every successful mutation, including a draft or disabled-object mutation,
advances the tenant catalog revision in the same immediate SQLite transaction
as registry/version/dependency/ACL rows and the bounded audit record. A true
selected-app revision is optional in `0.1`; when unavailable its optional
snapshot field remains absent and cache invalidation uses the mandatory tenant
revision and state commitment. The tenant-wide app-catalog revision is not
misrepresented as a selected-app revision.

The same transaction rotates a 32-byte catalog-state commitment stored beside
the numeric tenant revision. An exact backup and restore preserves the
commitment, while a mutation on either side of a restored fork produces a new
commitment even when both histories reach the same numeric revision. Readers,
pagination cursors, snapshot caches, and retained-job admission compare the
revision and commitment together; the numeric revision alone is never a
snapshot identity.

Completed-search analysis caches additionally verify the private Manager seal
before lookup and bind the exact application, an explicit knowledge-presence
bit, the finalized snapshot digest, and the compiler execution digest. A valid
legacy snapshot and an enabled snapshot with an empty program are distinct.
Finalization reduces the immutable snapshot, program, retained compiler
counters, and deterministic encoding to fixed-size retention facts; repeated
cache and cursor checks validate those signed facts without cloning the
variable-size snapshot payload. Invalid, unsigned, stripped, or independently
rotated snapshot/compiler pairs fail before cached analysis can be reused.

Every immutable object version has one immutable lifecycle record. Draft and
active versions carry no transition marker. A disabled version carries the
exact time of the disable transition; later body, metadata, or scope mutations
that retain the disabled state retain that original marker. Enable clears it,
and a later disable establishes a new marker. Quarantine and delete markers
equal their terminal transition time. Version one is timestamped exactly at
registry creation, retained version timestamps are nondecreasing, and no
historical timestamp may exceed the current registry update time.

Every mutation carries a 16–128 byte `client_request_id` using only bytes
`0x21..0x7E`. The
server stores `(tenant, actor, route, client_request_id, canonical request
digest, committed outcome)` in the mutation transaction. An exact retry returns
the original outcome; reuse with different bytes is a conflict. This reconciles
a commit followed by a lost response for create as well as optimistic updates
and delete. Normal idempotency records are limited to 16,384 per tenant, with
the protective reserve defined below. They are retained for at least seven days
and the maximum configured client retry window, and removed oldest-first only
after that fence. Capacity exhaustion rejects a new mutation before catalog
work rather than accepting an unreplayable commit.

The request digest is computed from one detached, unknown-free protobuf clone;
route execution never reads the caller-owned message again. The clone retains
optional presence, clears only `client_request_id`, and is marshaled
deterministically before semantic normalization. SHA-256 input is the raw ASCII
domain `open-splunk/knowledge-mutation-request/v1\x00`, followed by unsigned
64-bit big-endian length frames for the exact route, trusted owner identity,
and deterministic request bytes. Tenant, actor kind and actor ID remain exact
receipt-key dimensions rather than duplicated digest input. A post-call caller
mutation therefore cannot change either the committed authority or its replay
key.

Retention uses one SQLite transaction-time microsecond as an independent clock
authority. Its anchor is the later of that database time and the immutable
mutation occurrence, and a sub-microsecond configured duration is rounded up;
the stored fence is never shorter than the configured retry window. Reclaim
selects only the exact oldest expired prefix required for a new receipt (at
most 4,097 rows), width-preflights and canonically validates every receipt and
its immutable commit authority, then deletes that same prefix in the mutation
transaction. Corruption, cancellation, or a fence disagreement rolls the
entire attempted reclaim and mutation back.

Replay reauthorizes the current registry scalar before opening the retained
immutable outcome. If current policy no longer authorizes that identity, the
retry has the same not-found response as any other current-policy denial and
does not re-execute the mutation. If the current identity is quarantined, the
committed side effect likewise remains a no-op replay, but the response is
withheld with one fixed redacted-outcome error; no historical definition,
digest, or definition-derived projection is opened. These are the only
exceptions to returning the original response, and preserve current-policy
nondisclosure and permanent quarantine redaction over historical idempotency.

If SQLite reports an ambiguous commit result, the server opens one fresh,
bounded reconciliation transaction and performs that same authorization and
strict replay validation. A matching durable receipt returns the response
reconstructed from the durable receipt, version, lifecycle, audit, and snapshot
authorities. It never returns the staged pre-commit response, whose timestamp
or catalog token may differ from a concurrent exact retry that actually won.
Definitive absence or an indeterminate/corrupt reconciliation returns generic
unavailable and never re-executes under a new request identity.

The version-1 persisted outcome is a deterministic, unknown-free protobuf no
larger than 1 KiB. It duplicates the exact route, mutation kind, immutable
object ID and version, definition digest, committed catalog revision and
32-byte state token, immutable occurrence time, database retention anchor,
retain-until fence, plus exactly one successful-audit or recovery-audit
sequence. Ordinary mutations require a 32-byte definition digest and a success
audit; protective quarantine requires an empty digest and a recovery audit.
Every committed revision/token pair also has a separate immutable commit row,
retained independently of the mutable receipt. It binds the exact actor kind
and ID, route, `client_request_id`, request digest, revision/token pair,
mutation kind, object/version, occurrence and retention times, and audit
sequence. A composite receipt foreign key pins those request authorities to
that commit row, while separate foreign keys pin the immutable version and
audit. The duplicated envelope, receipt scalars, immutable commit row,
immutable version and lifecycle, and audit authority must all agree before
replay hydration or receipt reclamation. Noncanonical encoding, duplicate
known fields, nested or top-level unknown fields, wrong audit choice, or any
same-width disagreement fails closed as catalog corruption.

## Candidate validation and preview contract

The protobuf definitions freeze validation and preview behavior. The concrete
catalog Writer implements rollback-only validation and the production router
registers Validate as the ninth all-or-none management route; Preview remains
unregistered and bootstrap advertises neither capability. The pure normalizer
provides one internal bridge:
`Normalize` remains fail-fast and `IssueFromError` may extract one detached,
definition-relative candidate issue with code
`KNOWLEDGE_DEFINITION_INVALID`, `KNOWLEDGE_DEFINITION_UNKNOWN_FIELD`, or
`KNOWLEDGE_DEFINITION_RESOURCE_LIMIT`. An empty path identifies the definition
message. Existing error text and `errors.Is` roots are preserved, lower causes
are not newly exposed, and infrastructure, invariant, canonical-storage, and
other non-candidate failures have no issue. The registered handler never maps
the raw seam directly; Writer result construction consumes only its closed typed
projection.

The semantic compiler has a separate internal-only seam. Fail-fast
`knowledgeprogram.Compile` may bind one detached issue to the exact input index
responsible for an intrinsic regex syntax/resource/capture-shape, JSON-path
syntax/unsupported/resource, calculated-expression `SPL_*`, or direct-Boolean-
result failure. A validation adapter must independently prove that index is the
submitted candidate, should prefer singleton Compile at index zero, and must
never project a winner-cohort issue. `Prepare` and object/definition authority,
aggregate resource, cohort/collision/selector, and dependency failures remain
opaque to this typed compiler-issue seam. Error text and sentinel behavior stay
compatible. The registered handler likewise never maps the raw seam directly;
the result layer consumes only its closed typed projection.

The pure `internal/knowledgevalidation` package now constructs bounded results
from those typed seams without owning a catalog, database, transaction,
transition, authorization, route, or HTTP policy. Inactive construction only
normalizes into an opaque detached result and performs no publication compile
or dependency derivation. Active preparation normalizes and singleton-compiles
into exactly one opaque terminal invalid result or one opaque candidate with no
catalog authority. It maps only recognized typed definition/compiler issues
whose exact code, field path, candidate body, range, and suggestion shape agree.
Untyped or malformed local failures become opaque invariants. Authority, cohort,
aggregate, dependency, and transition failures receive no typed issue mapping
through inactive/active preparation; the sole result-layer exception is the
caller-selected, target-free `BuildDependencyUnavailable` generic diagnostic
below.

For a valid ACTIVE result, a full transition supplies an evaluation-local
candidate identity and the complete already-authorized direct target
projection. The package bounds, detaches, binary-sorts, and rejects malformed,
duplicate, non-`FIELD_INPUT`, over-version, and self dependencies, but cannot
prove completeness or visibility. Its sole transition-adjacent exception,
`BuildDependencyUnavailable`, accepts no target identity and emits the fixed
`KNOWLEDGE_DEPENDENCY_UNAVAILABLE` diagnostic.

A Validate request requires definition message presence and exactly one intent.
`INACTIVE_STORAGE` proves only bounded canonical inactive persistence and does
not compile publication semantics. `ACTIVE_PUBLICATION` evaluates the candidate
as a proposed ACTIVE version in one fixed knowledge/app/index catalog
transaction. The response revision is only the exact knowledge-ledger component
of that advisory evaluation; zero proves that ledger is empty. It is not
complete reusable transaction authority, a reservation, mutation proof, or
promise that later validation will agree. `UNSPECIFIED` and unknown numeric
intent values are envelope errors. Create
mode requires absent object ID, expected version, and update-mask message.
Update mode requires a present nonempty ID, a present expected version from 1
through MaxInt64 (`9223372036854775807`), and a present nonempty canonical mask
relative to `KnowledgeObjectDefinition`; validation applies it to that exact
current version. A missing definition message is an envelope error, while a
present definition with a missing or unknown body is candidate invalidity.

Validate uses a dedicated raw-wire codec rather than the generic read-all-plus-
`proto.Unmarshal` path. Its candidate walker/builders are now shared with the
unregistered request-only Preview codec through envelope-specific field
layouts; the extraction preserves Validate behavior. Both enforce the ordinary
mutation raw-body ceiling of 4 MiB plus 64 KiB (`4259840` bytes) by reading at
most one byte beyond it solely as an overflow witness, then perform a bounded
two-pass projection with
protobuf-compatible duplicate-message merge, last-scalar, optional-presence, and
`oneof` merge/reset semantics. Correct-wire object-ID presence, including
empty, selects update/mask projection; absence selects the complete create
definition. The decoder retains at most 9 update-mask paths, 17 entries in each
selected selector dimension, and 17 selected regex outputs. Every recognized
string occurrence is still UTF-8-validated even when unselected, overwritten,
or cleared. Malformed wire or unknown-group
nesting beyond 32 levels is rejected. Bounded oracles cover one million
mask paths, selected and unselected selector entries, job-ID scalars, regex
outputs, and alternating body choices without materializing those repetitions.

Both candidate codecs preserve unknown fields only where they carry validation
authority. Request-envelope unknowns, including wrong-wire envelope fields, and
field-mask unknowns remain present so structural envelope validation rejects
them. A create retains the complete candidate's unknowns; an update retains
unknowns nested in mask-selected values, so Validate—and a later Preview
service—treat those authored meanings as candidate invalidity. Candidate
top-level unknowns on update
and unknowns nested solely in unselected fields are discarded because they are
outside that exact field mask. Both request types bypass generic unknown
clearing; this split does not weaken mutation semantics.

Internal `Writer.Validate` takes a `ValidationScope` split between read and
write authority. Both scopes must identify the same authenticated tenant and
owner, but their app sets are independent: write authority controls the
requested root and candidate app, while read authority independently controls
dependency disclosure. A process-local one-slot gate is shared by every Writer
using the same control database and fails fast rather than queueing. Before that
gate, only the bounded request envelope is inspected. Once admitted, create
selects the complete definition and update makes a shallow mask-selected view;
unselected update fields are omitted from the request byte charge and clone.

The selected selector dimensions and regex output list are each capped at 16
entries. The service checks those list lengths in the normalizer's established
fail-fast order before walking exact request bytes. For overflow, it substitutes
a newly allocated 17-entry witness containing no caller scalar and only the
applicable body kind. Normalization therefore emits the same typed resource
issue without traversing or cloning an attacker-sized repetition. A nonoverflow
selected view must fit the normal mutation-request byte ceiling before it is
detached. Every result, including the witness path, still passes the separate
8 MiB deterministic response seal.

Both intents run inside one fresh `BEGIN IMMEDIATE` transaction that is always
rolled back before response sealing. An update first authorizes the requested
root, then checks its exact expected version, lifecycle, stored integrity, and
whether its current definition is opaque; only after those out-of-band checks
does it apply and inspect the candidate. Local candidate invalidity precedes
candidate-app/index inventory work once the root is established. Errors may
carry only an already-authorized root or candidate-app context, never a
dependency target, and default to definitive rejection.

`INACTIVE_STORAGE` performs structural normalization, authorizes the app of a
valid applied definition, and bookends the knowledge revision. It does not
compile publication semantics, read the ACTIVE transition inventory, or derive
dependencies. `ACTIVE_PUBLICATION` singleton-prepares the candidate and then
evaluates the complete bounded knowledge/app/index transition. Before
classifying any post-candidate conflict, it compiles all affected
candidate-absent baseline cohorts; persisted baseline faults therefore remain
catalog errors instead of candidate diagnostics. Conflict decisions expose no
identity and deterministically retain stronger topology failures. Only
cohort-local target absence may become the generic target-free dependency
diagnostic. Derived targets are first integrity-checked against their exact
current ACTIVE registry/version and then filtered through read authority;
missing and forbidden produce the same generic in-band outcome, while all other
transition, target-integrity, catalog, resource, and infrastructure failures
remain out-of-band.

For ACTIVE create, the service selects the first deterministic
`knowledge-validation-candidate-%04x` identity absent from the complete tenant
inventory after matching the identity ledger to the physical row count. The
identity is neither returned nor reserved, and the mutation ID generator is not
called. Both intents return the exact knowledge-ledger revision observed in the
transaction; revision zero additionally requires the physical knowledge object
ledger to be empty, and every revision path is bookended. Rollback failure
invalidates the request. Validation performs no DML, commit, mutation audit,
idempotency operation, publication hook, clock read, or mutation-ID allocation.

The evaluation-local create identity is alpha-invariant: semantic validity,
diagnostics, resource charges, and the target-only dependency projection must
be unchanged under every other fresh candidate-ID choice. A later Create
generates its own ID and revalidates the then-current catalog, app, and index
facts, so intervening changes may alter the outcome.

Only candidate-authored invalidity may return an in-band `valid=false`; the
registered HTTP adapter maps that sealed result to HTTP 200. Such a result
retains at least one field violation or ERROR diagnostic even after
truncation and omits normalized definition, digest, dependencies, and resource
estimates. A valid result carries the normalized definition, exact 32-byte
digest of its deterministic encoding, and a complete candidate-only resource
report; it has no field violations or ERROR diagnostics and no field-violation
truncation. Request, authentication, requested-object authorization,
catalog-integrity, hidden-inventory, and service failures remain out-of-band;
the handler maps only the closed, definitive validation error taxonomy to
uniform non-2xx outcomes. The sole admitted join is the exact
`control.ErrCapacityExceeded` plus `knowledgevalidation.ErrResponseTooLarge`
pair; impossible or every other joined authority collapses to the generic
unavailable response rather than candidate diagnostics.
`object_type` is unspecified only
when an invalid candidate's body cannot be identified; otherwise it is the
exact applied body type.

`valid` is advisory definition validity under the selected intent, never
mutation acceptability, reservation, or a promise. An applied masked update
identical to the current definition may be valid. `INACTIVE_STORAGE` against a
currently ACTIVE object proves only hypothetical non-ACTIVE storage validity,
never ACTIVE Update admissibility. Every later Writer independently revalidates
then-current authorization, version, lifecycle, capacity, app, index, and
publication authority.

For `INACTIVE_STORAGE`, resources contain only exact selector-pattern and
normalized-definition-byte charges; dependencies and every dependency or
compile-derived counter are zero. For `ACTIVE_PUBLICATION`, resources are the
intrinsic charges from a canonical singleton program whose only object is the
normalized candidate and whose dependency list is empty: generated
operators/fields, regex programs/work, scalar expressions/nodes, extraction
outputs, JSON evaluation work, and scalar predicates. They are neither cohort
totals nor marginal post-fusion deltas. `extraction_outputs = 12`,
`json_evaluation_work_units = 13`, and `scalar_predicates = 14` complete the
append-only resource shape; waived draft SQL-size tag/name 11 remains reserved.
The complete set of at most 1,024 unique authorized direct candidate
`FIELD_INPUT` dependencies is returned in binary target-ID/version/role order.
Nodes count distinct exact returned targets excluding the candidate, and edges
equal the returned list size; those counts come from the full ACTIVE transition,
not the singleton compile. Missing and unauthorized targets are
indistinguishable and produce only the static
`KNOWLEDGE_DEPENDENCY_UNAVAILABLE` diagnostic with no target identity.

Field violations and located diagnostics are separately limited to 256 exact-
deduplicated values and longest deterministic prefixes of 256 KiB and 768 KiB
aggregate UTF-8 text. A path is at most 1 KiB, a code 128 bytes, and a message
4 KiB; a diagnostic has at most 32 unique binary-sorted suggestions of at most
1 KiB. Aggregate charges sum the UTF-8 byte lengths of path, code, message, and
all diagnostic suggestions without separators or wire framing. Truncation
flags state whether each list is complete. Violations sort by binary
path/code/message. Diagnostics sort ERROR before WARNING before INFO, then by
path, absent range before present range, offsets, code/message, canonical
coordinates, and suggestion sequence. Unknown or unspecified severity is
invalid service output. A present range requires nonnil start and end, is
half-open inside the exact candidate field scalar, lands on UTF-8 code-point
boundaries, and has uniquely derived one-based coordinates: LF advances the
line and resets column to one; every other Unicode scalar, including CR,
advances the column. Any future issue field must extend validation,
deduplication, and the total comparison key before emission.

Issue text may contain only fixed templates and exact source text already in
the applied candidate scalar. It never exposes another catalog object, app,
owner, name, ID, version, digest, definition, index inventory beyond candidate
text, cohort/global counts, generated SQL, or hidden authority. The response
boundary recursively rejects unknown result fields and all unknown nested
fields before deterministic serialization; the complete Validate response is
at most 8 MiB.

Ranged typed diagnostics are rebound to the exact submitted source, not trusted
from canonical offsets alone. The package re-normalizes a detached definition;
JSON-path text must be byte-identical, while calculated-expression ranges are
rebased through the exact ASCII trim, including canonical EOF after submitted
trailing trim. A private sidecar binds field path and submitted scalar.
Projection and every seal rederive UTF-8 boundaries and one-based coordinates
from it, so missing, relabelled, mid-rune, out-of-scalar, or forged-coordinate
provenance fails closed.

`SealValidateResponse` revalidates the opaque result kind, canonical definition
and digest, inactive resource zeros or a fresh active singleton compile,
transition-supplied dependencies and their exact resource counts, issue
ordering and private range provenance, recursive unknown-field absence, and a
revision through MaxInt64. It retains the exact deterministic encoding only at
or below 8 MiB, and all protobuf/byte projections detach. This is an internal
result boundary, not a database read, catalog proof, route, or HTTP service.
The Writer adapter supplies the database, transition, and authorization proofs
and calls the seal only after successful rollback. The registered handler
requires that exact ready concrete Writer, detaches request authority, derives
independently cloned read/write scopes, and holds response-serialization
capacity through a custom encoder. That encoder revalidates the seal under the
live request context and writes its exact deterministic bytes without a fresh
mutable protobuf marshal; the route remains unadvertised.

Preview has no independent intent. Its internal request-only codec shares the
bounded candidate decoder. Its canonical request fields are
`retained_search_job_id = 1`, `definition = 2`, optional
`knowledge_object_id = 3`, optional `expected_version = 4`,
`update_mask = 5`, and optional uint32 `maximum_rows = 6`. The retained job ID
names future owner-scoped retained execution authority which a service must
reacquire under the authenticated caller; it is not an immutable-event-snapshot
identity and grants no access by itself.

The codec validates every recognized string occurrence as UTF-8, including
overwritten, unselected, or cleared values, and retains the last UTF-8 job ID
through the 256-byte ceiling or a detached 257-byte over-limit witness. The
structural validator requires a nonempty job ID unchanged by whitespace
trimming and free of Unicode control code points, rejects outer unknown
authority including wrong-wire envelope fields plus mask unknowns, and passes a
synchronous nonescaping candidate view through the exact Validate create/update
envelope with `ACTIVE_PUBLICATION` forced by the server. It never mutates or
normalizes the decoded request and performs no retained-job lookup or
authorization. The full create candidate and update mask-selected nested
unknowns remain candidate authority; update candidate top-level and unselected
nested unknowns are discarded.

`maximum_rows` has full optional uint32 wire authority: absence, explicit zero,
and every value through `4294967295` remain distinct and unchanged. The request
boundary assigns no default, bound, or execution meaning. Generated Go and
TypeScript contract oracles independently preserve create tags `[1, 2]` and
all six present-empty update tags; the Go structural oracle and TypeScript wire
oracle additionally preserve the maximum uint32 value. This contract hardening
changes no field number, type, or presence encoding.

Preview remains unregistered and unadvertised. It has no response codec,
handler, catalog/search service, caller-authorization integration, acquisition
of retained execution, route, TypeScript manifest or browser-bearer entry,
capability, UI/navigation request, Resolver attachment, or execution path. A
future service must reacquire the owner-scoped retained execution, perform
`ACTIVE_PUBLICATION` evaluation in one fixed knowledge/app/index transaction,
apply the resulting candidate program to that retained server-authorized
execution, and freeze the row-limit default/bound/execution policy plus paired
before/after schema-row, truncation, response-byte, deadline, and concurrency
semantics. Its revision remains advisory knowledge-ledger correlation metadata,
not mutation acceptability, a reservation, or reusable publication proof;
later Writer operations revalidate live authority. The nonempty compiler,
snapshot-finalization, and digest-pinned ClickHouse acceptance gates remain
closed. Preview accepts no raw events, physical scope, asset path, or SQL.

The validation wire redesign has an intentional historical protobuf
FILE-compatibility waiver. Draft result tags 6 and 7 and resource tag/name 11
(`estimated_generated_sql_bytes`) were retired before Validate was registered
and were never served by either the Validate or Preview route. They remain
reserved against reuse. Old and new peers may drop those never-served values,
but this is not a schema-nonbreaking change. Validate's later registration does
not retroactively alter that classification; Preview remains unregistered.

## Protobuf forward compatibility and corruption

Mutation routes reject unknown fields anywhere in a submitted definition and
reject unknown future `oneof` bodies. Exact field-mask updates are applied to
the current stored message on the server; omitted fields are not replacement
authority. A state-only disable or delete may preserve an unreadable future
body byte-for-byte, but an older server cannot enable, publish, or body-edit it.
Selector and extraction-output cardinality is checked in constant time before
either recursive unknown-field walker, protobuf sizing, cloning, or canonical
marshaling so repeated empty submessages cannot amplify the request byte limit.
Validate additionally applies the authority-sensitive unknown split above: its
envelope and mask unknowns are out-of-band request failures, selected nested
candidate unknowns are in-band invalidity, and unselected update unknowns are
ignored. Preview's dormant request boundary retains the same wire split, but
with no service or route it classifies only structural envelope authority;
candidate invalidity remains a future service result.

`KnowledgeObjectDefinition` field numbers 13 through 31 are permanently
allocated exclusively to future length-delimited `body` oneof alternatives.
Future top-level metadata fields use 32 or greater and cannot use protobuf's
compiler-reserved 19000 through 19999 range. Every future ordinary metadata
field must be declared in ascending field-number order before the `body` oneof.
That declaration rule aligns generators that emit ordinary fields before
oneofs with generators that emit source declaration order. The canonical
unknown-field suffix therefore contains future metadata fields first, with
nondecreasing field numbers, followed by exactly one length-delimited future
body field in 13 through 31 as its final field.

An older reader accepts that suffix on a draft, disabled, or deleted immutable
version. Repeated occurrences of one future metadata field number are preserved
because an older reader cannot know whether that future field is singular,
repeated, or a compatibility-preserving sequence; their wire tags and values
must still be minimal. It rejects unknown fields nested inside known metadata,
non-minimal wire encodings, descending metadata numbers, metadata after the
future body, multiple future bodies, and every unknown body on an active
version.

An older reader cannot derive an object type from an opaque body. For one of
these inactive versions only, it may return the sealed registry/version
`object_type` as safe administrative display metadata after those scalar
authorities agree with each other. This is the sole exception to deriving the
indexed object type from the body. The value is never executable authority,
never participates in publication or dependency validation, and is never used
to construct provenance. Stored raw bytes and their digest remain authoritative;
the fallback never attempts to interpret or execute the opaque payload.

An administrator integrity scan can issue a single-use, ten-minute HMAC token
binding tenant, catalog revision, the stable root object ID, the SHA-256 digest
of its complete raw current registry row, and an ordered digest of all current
active direct and transitive dependents discovered through trusted inverse
dependency rows. The closure is limited to the tenant's 4,096 active-object
ceiling. The quarantine mutation accepts only that token and a fresh client
request ID, opens an immediate transaction, re-reads and re-digests the exact
root, dependency edges, and dependent registry rows, and fails on any change.
It does not decode suspect definitions.

On success the transaction appends a small bodyless forensic version and marks
the root `quarantined`; every active dependent receives one state-only forensic
version and also becomes terminally `quarantined` with a fixed
`dependency_recovery` reason. Cascade members cannot be edited or re-enabled;
after repair an administrator publishes a newly validated object identity and
may reuse the inactive name under the ordinary collision rules. The transaction
retains every suspect byte and row for recovery analysis, advances the catalog
revision once, consumes the token, and commits one fixed recovery audit record
for every state transition.

If corrupt inverse edges leave another active invalid object, the post-commit
integrity scan reports it and a separate token quarantines that object; no
active invalid object is silently executable. Because every recovery transition
is terminal, one lifetime reserved record per physical identity is sufficient.
This bounded cascade can isolate malformed protobuf, digest or identity
disagreement, impossible state, missing version/dependency, and unknown-body
corruption without manual SQLite surgery. It cannot repair, enable, export, or
reveal a suspect body.

Visibility filtering by trusted tenant/app/owner/scope columns and ACL rows
occurs before definition decoding. Hidden rows are never decoded on a search
path and therefore cannot become an existence oracle or denial of service. A
candidate visible to the principal is then fully decoded and integrity-checked;
visible corruption fails admission, while hidden corruption is reported only
through administrator health and integrity tooling. Caller-facing reads bound
and validate the authorized visible identity set, not the tenant-wide physical
identity count or mutation ledger. Tenant-wide physical-count and ledger
mismatches therefore fail mutation and administrator-health checks without
changing another owner's Get/List result. Corruption of the shared tenant
catalog revision itself remains tenant-wide because no coherent read snapshot
can be established without that authority.

Stored definition bytes are deterministic protobuf encodings with a SHA-256
digest. Indexed identity fields must exactly equal the decoded normalized
definition. A digest mismatch, malformed protobuf, unknown active body,
identity disagreement, impossible state, missing version, or invalid dependency
is corruption. Resolution fails closed and produces a payload-free admission
diagnostic; request handling never repairs the row in place.

`Get` without a version reads the current registry version. An explicit
historical version is returned only after the caller is authorized from the
current registry identity; absence and current-policy denial have the same
not-found response. Draft, disabled, deleted, and their historical versions
remain inspectable by an authorized administrator. A currently quarantined
identity returns only its safe current scalar projection for every `Get`
request and never returns a current or historical definition, digest, or
definition-derived projection. This quarantine redaction is permanent even if
the caller once authored the object.

`List` defaults to 50 objects and accepts at most 256. Its opaque signed cursor
is limited to 4 KiB and binds the normalized tenant, owner, readable-app set,
filters, sort order, page bound, and first-page catalog revision plus catalog-
state commitment. A change to either value invalidates continuation rather
than mixing snapshots, including divergent histories that reuse a revision
number after restore. One response
detaches at most 4 MiB of canonical definition bytes and validates at most
65,536 dependency edges; it stops at the preceding object and emits a cursor
when either ceiling would be crossed. One maximum-sized object always fits.

The HTTP boundary canonicalizes and detaches the List request before invoking a
configurable catalog. It validates every returned object against the complete
normalized filter and authorization scope, requires unique IDs and canonical
ordering with object-ID tie breaks, and checks bounded page/token/total/revision
continuation shape, the reported exact-total 8,192-identity ceiling, and
per-definition, aggregate-definition, and response byte budgets. A global
object is visible regardless of its provenance app; app membership remains
mandatory for app-shared and private objects. An impossible
dependency-conflict error from the catalog fails with one generic unavailable
response rather than leaking a partial page.

Description and selector filtering trusts projected membership only after the
complete authorized, scalar-filtered candidate set has been decoded and
checked against canonical definitions. That sweep admits at most 8,192
authorized identities, 8 MiB each of definition, projection, and selector-value
bytes, 65,536 selector patterns, and 65,536 dependency edges. Exceeding a sweep
ceiling fails closed with a capacity error; it does not return a partially
validated page or total. More than 8,192 physically visible identities is
catalog corruption rather than a request-capacity error. Scalar-only filters
hydrate only returned objects.
Authorization is applied before either path, so hidden rows and their bodies
cannot consume these caller-visible integrity budgets or become corruption
oracles.

Known response fields remain decodable across version skew. Unknown bytes may
be preserved by the Go runtime but are not promised to browser clients. The
stored canonical bytes, rather than a browser round trip, are the authority for
snapshot digesting and history.

Store `ListDependencies` and `ListDependents`, exposed as `dependencies` and
`dependents`, inspect only direct persisted object-to-object `FIELD_INPUT`
edges. Their wire projection contains exact source and target object IDs and
versions plus role; it deliberately omits definition digests
and snapshot-global stage, longest-path depth, and canonical ordinal. An
omitted request version selects the current root version. An explicit
historical root version never grants authority: current registry identity and
current policy authorize every current or historical graph read. A missing,
hidden, or currently quarantined root returns the uniform not-found-or-
forbidden response and exposes no current or historical graph.

Outgoing inspection lists the selected source version's exact direct edges.
Inverse inspection lists edges to the selected target version only when the
source edge belongs to that source object's current registry version; DRAFT,
ACTIVE, DISABLED, and DELETED current sources are eligible, while quarantined
or otherwise hidden sources are not. In either direction a hidden or
quarantined opposite endpoint is omitted before ordering, keyset pagination,
and total counting. There is no redacted placeholder, ordinal gap, or hidden
count. Pages contain at most 256 disclosed edges. `total_size` is an exact
disclosed-edge count only when `include_total_size` requests it and is otherwise
absent; outgoing totals are bounded by 1,024 edges and inverse totals by the
8,192 current-object identity ceiling. The protobuf HTTP response is capped at
128 KiB and uses the management boundary's fail-fast serialization capacity.

Each graph continuation is an authenticated opaque token no larger than 4 KiB.
It binds the route direction, tenant, owner, readable-app set, root object,
requested-version presence and value, resolved root version, page size, total-
count choice, last disclosed edge key, and first-page catalog revision plus
state commitment. A change to either catalog identity invalidates the token.
Outgoing order is binary target ID, target version, then role; inverse order is
binary source ID, source version, then role. Storage ordinal never enters the
cursor or response because gaps could disclose omitted edges.

Inverse reads are authorization-leading: the implementation first bounds the
currently authorized registry source set, then probes each exact current source
version with `knowledge_object_dependencies_source_target_idx` and
`target_kind = 'object'`. It must not drive the query from the retained target
index `knowledge_object_dependencies_target_idx`, because historical or hidden
source rows are not disclosure authority. The v0.1 management inverse therefore
recognizes object targets only. A
constraint-bypassed non-object target row is outside this bounded inverse
projection and is detected when the authoritative source graph is read or
revalidated. Supporting another target kind requires a new bounded current-
inverse authority/index contract before that kind may appear in `dependents`.

The dormant Knowledge Manager detail binds both relationship requests to the
exact ID and version returned by Get. Dependencies and dependents maintain
independent continuations, exact visible totals, failure/retry state, and
catalog-revision labels. A displayed edge contains only the currently visible
opposite endpoint's object ID and version plus the fixed `Field input` role
label; it does not recover an omitted endpoint or expose definition metadata.
This consumer is readiness evidence only: while the capability is absent, the
navigation entry and dynamic chunk are absent and production bootstrap causes
no knowledge request.

## Immutable snapshots and lifecycle

The KO-0H manager path is optional. A nil resolver, including a typed nil, and
an app-less request preserve the legacy asynchronous path. For a configured
nonempty-app request, the browser boundary first proves live app and index
authority. The manager then captures storage visibility and synchronously
parses, plans, resolves, compiler-seals, and finalizes the request before any
job ID, history or audit admission, publication, or execution can exist.

That knowledge-enabled admission detaches a canonical snapshot containing
tenant, principal, app, effective authorized indexes, tenant revision and its
exact 32-byte catalog-state commitment, optional app revision, compiler
compatibility version, exact ordered object IDs/versions/types/names/scopes,
selectors, definition bytes and digests, dependency order, shadow warnings,
and aggregate budget charges. The SHA-256 snapshot digest covers a versioned,
length-prefixed canonical encoding of every field, including that commitment;
map iteration, timestamps, database row order, and mutable ORM records are
excluded.

All revision, current-registry, exact-version, dependency, and ACL reads for
one snapshot occur through one SQLite read transaction. The transaction reads
the tenant revision first to establish its WAL snapshot, performs all bounded
queries through that handle, re-reads the same revision in the same snapshot,
and detaches all bytes before commit. A revision mismatch, missing row, busy
snapshot, cancellation, or deadline aborts the attempt; it never produces a
mixed snapshot. Resolution has three bounded attempts within a 250 ms total
deadline and otherwise fails unavailable. A concurrent mutation may yield the
complete old catalog or complete new catalog, never a mixture.

The digest input is a `KnowledgeSnapshot` protobuf with no maps or floating
point fields. Unknown fields are rejected and cleared before digesting; repeated
indexes, objects, dependencies, and warnings use the orders specified by this
contract; absent optional fields remain distinct from present empty values.
The digest is SHA-256 over the ASCII prefix
`open-splunk-knowledge-snapshot-v0.1\x00`, followed by the unsigned 64-bit
big-endian length of deterministic protobuf bytes, followed by those bytes.
Generated field numbers and this framing are part of compatibility `0.1` and
are pinned by cross-language golden tests.

The canonical byte charge and digest are self-excluding in two explicit steps.
First clear `snapshot_sha256`, set `canonical_snapshot_bytes` to zero, and
deterministically marshal `B0`; `len(B0)` must not exceed 4 MiB and becomes the
canonical byte charge. Then set that charge, leave `snapshot_sha256` absent,
and deterministically marshal `B1`. The framed digest above hashes `B1`; only
afterward is the 32-byte result stored in `snapshot_sha256`. Neither the final
digest field nor the encoded nonzero charge recursively contributes to the
charge.

Shadow ordinals are assigned by winner resolution ordinal, then losing
precedence nearest-first (private, app, global), then loser object ID. Each
shadow produces exactly one warning with only its shadow ordinal present.
Every executable winner counts as one dependency node even when isolated.
Node depth is the longest outgoing dependency path in edges, so a leaf or
isolated node has depth zero. Direct edges sort by source depth, explicit source
stage rank, source object ID and version, target kind, target object ID and
version, then dependency role; their zero-based canonical ordinals follow that
order.

One snapshot may contain at most 256 executable objects, 512 exact knowledge
output occurrences, 256 knowledge-origin logical-operator occurrences, 4 MiB
canonical bytes, and the stricter type-specific aggregate limits above.
`generated_operators` counts the canonical pre-optimization knowledge prelude,
so one fused parallel operator may represent multiple objects. `regex_programs`
and `regex_work_units` combine knowledge extraction and authored `rex`
occurrences; calculated-field `match()` work remains a separate shared budget.
`regex_capture_bytes` is zero with no such program and otherwise exactly the
4 MiB per-row capture guard. `scalar_expressions` and
`scalar_expression_nodes` are exact knowledge-only occurrences. Finally,
`generated_sql_bytes` is `len(compiled.SQL)` for the sealed parameterized SQL;
rendered bind values, executor wrappers, settings, and the independent 1 MiB
executor query-size defense are excluded. Resolution or sealed compilation
exceeding a ceiling fails admission before a job is created.

The canonical Tier-1 field program is present even when it contains zero
objects. Its zero value means absent/legacy; a prepared empty value means
an admitted empty field program and contributes a stable nonzero private
commitment. For a nonempty program, individual regex and JSON operators retain
the canonical resolution order, followed by at most one fused alias stage and
at most one fused calculated stage. The program re-normalizes every definition,
requires exact named regex captures, applies authored calculated-assignment
result restrictions, enforces aggregate charges, rejects possible same-stage
write collisions or chains unless selectors are provably disjoint, and requires
the submitted `FIELD_INPUT` list to equal the independently derived closure,
including canonical order and longest-path source depth.

Publication uses that same backend-neutral semantic compiler without accepting
caller-supplied dependency authority. Given one complete canonical winner set,
it derives version- and definition-digest-pinned `FIELD_INPUT` edges, source
depths, and canonical ordinals before producing the immutable program. Snapshot
preparation remains the independent persisted-authority oracle: its submitted
dependency list must be byte-for-byte equal to the compiler-derived result.
A publication candidate is first evaluated against an explicitly complete
post-publication winner cohort. Its exact candidate key binds owner, identity,
version, and definition digest. The derived authority distinguishes a successful
empty edge set from missing derivation and retains each target's identity,
owner, version, digest, role, and stage. Every other immutable winner's
persisted outgoing rows must already be present, in per-source database order,
and exactly equal the newly derived target set; program-global canonical
ordinals and depths are never mistaken for database row ordinals. This pure
cohort check alone does not establish visibility completeness from a count, one
principal's `Resolution`, or a winners-plus-shadows mixture. The concrete
Writer therefore consumes it only through the transaction-bound authority
below, which proves complete tenant object, app, and durable-index inventories
and persists the resulting derived edges atomically.

Each validated cohort authority is transition-specific even when the candidate
is absent from its winners. It binds the exact candidate owner, identity,
version, definition digest, an explicit winner/non-winner mode, and the
canonical program commitment. Winner mode requires exactly one candidate;
non-winner mode requires none and independently revalidates every retained
winner's persisted edges. Multi-index scopes use a paired before/after
component-wise OR closure with exact minimum witness counts. The closure admits
at most 1,024 physical applicability atoms and 1,024 retained signatures and
stops after 65,536 charged atom/join probes, independently of those structural
ceilings. Singleton-index checks are not publication authority because a
combined scope can produce a winner or dependency absent from each singleton.

One ACTIVE transition is validated from an exact tenant-wide current-ACTIVE
inventory, the exact active-app inventory, and every live ACTIVE or ARCHIVED
index name that may become searchable. The validator derives the post inventory
from explicit before/after endpoints; it does not trust caller-built winner
cohorts. It evaluates tagged generic future-app, generic-principal, and exact
private-principal classes across pre/post visibility, applies unique-highest
precedence to every reachable multi-index signature, and independently compiles
each distinct changed post cohort. An ACTIVE candidate must win at least one
durable-index witness, and its version/digest/owner/stage/role-pinned dependency
authority must be identical in every cohort where it wins. Disable/delete may
have no surviving witness, but every newly unshadowed winner must retain its
exact sealed outgoing rows. Post-tenant resources, per-class hydration,
wildcard matching, membership, cohort revisits, graph work, and repeated
selector and semantic compiler charges are bounded separately. The issued body-free
persistence authority matches tenant, exact before/after scalar endpoints,
retained ordered rows, and the derived-or-retained database projection. Opaque
future bodies are not admitted by this semantic validator. Their separate
state-only emergency removal path reopens and exact-matches the stored body and
retained dependency authority; it does not permit ACTIVE update or enable.

The transactional prerequisite now reads the exact current-ACTIVE object,
tenant app, and physical-index inventories only from the Writer's existing
`BEGIN IMMEDIATE` transaction. It reconciles the knowledge revision/token and
ACTIVE ledger, app revision, and index revision/physical count before bounded
definition, selector, or dependency hydration. A separate preflight validates
each rich derived target against its exact current ACTIVE registry/version and
returns a detached database projection. An opaque authority binds those facts,
the exact transaction, and the transition proof. Immediately before its first
write or persistence hook, `publishMutation` rechecks that authority and uses
only its detached projection for the version count, dependency rows, and seal.
Recognized definitions use this path for ACTIVE create, ACTIVE update, enable,
disable, and delete; opaque ACTIVE update and enable remain closed.

Enabling a retained draft or disabled definition rederives its complete edge
set against that transaction's post-publication winners; it never copies the
predecessor's edges as semantic authority. Disable and delete remain
state-only versions and retain their predecessor's exact ordered edge set.
Advancing the immutable version of an ACTIVE target is allowed only when no
current ACTIVE dependent still pins another target version. A bounded atomic
cascade may disable those dependents with exact state-only edges, advance the
target, and then re-enable them with newly derived edges before commit.
Historical and inactive source edges do not block that transition. A schema
upgrade fails atomically if a current ACTIVE edge already targets an absent,
inactive, or noncurrent target version. This target-transition guard is not
source-publication authority by itself: the Writer validates every new ACTIVE
source edge against the complete post-publication cohort before its registry
row can advance.

The versioned program commitment length-frames the exact typed operator order,
object origin and definition digest, per-output definition location, selector
canonical bytes and runtime assessment, overwrite behavior, executable body,
static charges, and canonical dependency authorities. Logical-plan consumers
must prove the explicit contiguous prefix still equals that retained program;
equal counters alone are not authority. KO-1B itself did not lower these
operators. KO-1C now constructs their bounded ClickHouse stages, runtime
accounting barriers, sealed result sidecars, and specialized derived execution
authority, but the final nonempty compiler seal and snapshot finalization gates
remain closed until the digest-pinned runtime compatibility matrix passes.

Generic lifecycle records distinguish absence from emptiness. An absent
`KnowledgeSnapshotSummary` means knowledge resolution was disabled or the
request followed the legacy/app-less path. A present reference with
`object_count == 0` is the enabled, canonically empty result. One summary is
limited to 32 KiB and contains exactly the canonical first
`min(object_count, 64)` object summaries; `objects_truncated` is true exactly
when `object_count` exceeds 64. The reference always retains the exact total.

Every detached completed-execution snapshot is manager-minted, including the
legacy form. Its private seal commits the explicit knowledge-enabled bit, the
complete execution tuple, the compiled query and snapshot authority when
enabled, and the exact result generation, schema, row count, and truncation
state. A result lease is valid for that snapshot only when its private
manager-owned attestation matches the seal; constructed, cross-manager, or
mismatched pins fail closed.

The physical catalog is limited to 8,192 object identities, 65,536 immutable
versions, 512 MiB of unique definition-body bytes, and 20,480 idempotency
records per tenant. Normal creates and edits stop at 61,440 versions and 16,384
idempotency records, reserving 4,096 rows of each for one protective terminal
quarantine per maximally full active catalog. Definition bodies are
content-addressed immutable blobs; a state-only version references the prior
blob and does not duplicate its bytes, while a bodyless quarantine needs none.
The reserved rows cannot be consumed by create, edit, scope change, enable, or
ordinary delete. Once quarantined, an object no longer blocks search admission;
later deletion may wait for compatible reclamation.

No tenant may have more than 4,096 active objects, one app more than 1,024
active objects, one owner more than 512 active private objects, one type more
than 2,048 active tenant objects, or one app/type pair more than 512. Counters
are transactionally maintained and checked before writes. Identities,
tombstones, retained versions, and unique bodies continue to count until a
later compatible garbage collector proves them safe to remove.

Resolution decodes at most 8 MiB of candidate definition bytes before the
stricter 4 MiB snapshot limit, permits at most 32 concurrent resolver calls per
server, and fails fast when the permit pool is full. The optional resolver cache
is bounded to 128 entries and 256 MiB of detached decoded data; eviction is LRU
and never changes correctness. Catalog capacity and cache use are exposed as
administrator health metrics with an 80% warning threshold.

One object selector evaluates constrained dimensions in the fixed order
`index`, `host`, `source`, `sourcetype` and stops at the first nonmatch. An
unrestricted dimension is skipped. A reached missing or null value is a
nonmatch and costs no input or wildcard work. A reached present string is valid
only when it is valid UTF-8 and at most 1 MiB; its UTF-8 byte length `B` is
charged once for that object/dimension application. Exact literals use one set;
wildcard alternatives use at most one bounded anchored matcher. Sequentially
rescanning the value once per pattern is forbidden.

Runtime wildcard work uses a deterministic compiler assessment rather than an
actual matcher-transition count. For each normalized wildcard pattern `p`, let
`n` be its normalized token count and define:

```text
initial(p)  = 1 + (1 when the first token is `*`, otherwise 0)
per_byte(p) = 3*n + 1
final(p)    = n + 1
```

For the wildcard patterns in one reached dimension, the assessed transition
bound for a present value of `B` valid UTF-8 bytes is:

```text
bound(B) = sum(initial(p)) + B*sum(per_byte(p)) + sum(final(p))
```

Using bytes rather than Unicode scalar count is deliberately conservative and
is directly computable in generated SQL. An exact-literal hit charges only
`B`, even when wildcard alternatives also exist. An exact miss in a
literal-only dimension also charges only `B`. Only an exact miss with at least
one wildcard alternative precharges the complete `bound(B)` before matching;
that dimension contributes `B + 8*bound(B)` query units regardless of the
wildcard result or the matcher's actual work.

The 4 MiB per-event ceiling is the sum of input-byte charges across all reached
object/dimension applications for that event. The 1 GiB per-query ceiling is
the cumulative input-plus-assessed-wildcard charge across all processed events.
Exactly-equal ceilings are allowed; exceeding one fails the query with a
source-attributed resource-limit error. Input is never truncated and an object
is never silently skipped. The accounting shares the search execution deadline
and memory ceilings.

This assessed bound replaces actual Go matcher-transition charging before
nonempty knowledge execution is enabled in KO-1. Logical matching, the
publication-time literal/`?`/`*` weights of one/two/four, and the canonical
snapshot charge remain unchanged. Actual NFA state layout and transition count
are implementation details. The ClickHouse lowering must use ordinary bounded
SQL predicates plus compiler-derived constants; it must not require a UDF or
encode an NFA in SQL. Byte/coefficient multiplication, the factor of eight, and
event/query aggregation widen to `UInt128` before comparison. Match and charge
expressions for a later dimension remain inside the preceding dimension's
short-circuit branch; eager independent aliases that inspect unreachable values
are forbidden.

Resolver index pruning applies the same logical index matcher to at most 256
authorized canonical index names, but those bounded admission probes are not
processed events and do not consume the later query's execution charge. Each
index probe receives an independent hard matcher budget under the resolver's
shared deadline. A conservative cumulative assessment over the authorization
inventory must therefore never turn a valid catalog into corruption.

The lifecycle binding is implemented for enabled-empty snapshots. Updating the
catalog cannot change that retained authority, but KO-0H does not permit a
nonempty snapshot to finalize or execute. KO-1 must supply and seal the
knowledge prelude before this contract can extend to executable objects.

History durably stores the exact bounded summary admitted with the queued job
and requires the same summary at terminal publication. The search-attempt audit
row committed with that admission stores only the compact reference, including
the exact object count, and never stores the inventory or definition bodies.
Current browser job, history, export, and inspection projections preserve the
reference and canonical ordinal/type/stage inventory but redact every retained
object identity. A future current-policy authorizer may disclose an identity
only after checking the current registry scope, state, ownership, and ACL;
historical scope remains provenance and never grants disclosure.

Inspection additionally associates each generated logical stage and output
occurrence with that redacted inventory. Operator provenance is ordered by
ascending canonical ordinal; output provenance is ordered by output field and
then ordinal, so selector-disjoint objects targeting one destination remain
distinct. Generated stages omit the authored source-range message instead of
inventing a coordinate. The projection never carries object IDs, names,
versions, owners, apps, sharing, definition digests or locations, selectors,
patterns, paths, expressions, or overwrite policy.

Inspection opens the retained compiled authority for an enabled snapshot,
performs a second detached sealed metadata read after `EXPLAIN`, and consumes no
result-lease capacity. Export atomically acquires the exact execution authority
and matching result-generation pin, retains that pin for the export lifetime,
and uses the retained compiled authority for an enabled snapshot. Saved-search
execution and History **Run Again** reauthorize current app/index intent and,
when the optional resolver is configured, perform a fresh current-catalog
resolution. A historical summary is provenance only and is never accepted as
execution authority.

Version `0.1` performs no physical garbage collection of knowledge definition
versions. Backup and restore include registry rows, exact version bytes,
dependencies, ACL rows, catalog revision, snapshot leases, and digests in one
SQLite recovery generation. Cleanup and cross-engine asset retention arrive
with lookup support and KO-4 hardening. Retaining data on ambiguous cleanup is
always safer than substituting a newer version.

## Provenance, diagnostics, and audit

Every generated logical operator and field records origin kind, object ID,
object version, object type, object name, and definition location. Provenance
survives safe rewrites and nested ClickHouse subqueries. Field discovery may
return only the bounded kind (`extracted`, `alias`, or `calculated`) plus object
identity after the response-time current-policy authorization check above.

Publication errors identify the responsible object and source location without
echoing private definitions in logs or audit. Search diagnostics for an object
may include name/type/version only after the response-time authorization check;
otherwise they use the redacted ordinal. Other object existence is not
disclosed. Runtime row-content failures never echo `_raw` or derived values.

Committed actions are `create`, `update`, `scope_change`, `enable`, `disable`,
`quarantine`, and `delete`. The rejected privileged-attempt taxonomy also
includes the catalog reads `get`, `list`, `dependencies`, and `dependents`, and
the read-only operations `validate` and `preview`. Results are `success` or
`rejected`. Rejection reasons are the closed set
`not_administrator`, `not_found_or_forbidden`, `version_conflict`,
`idempotency_conflict`, `invalid_definition`, `forbidden_dependency`,
`resource_limit`, and `service_unavailable`. No free-form reason is stored.

Records contain only actor, tenant, app, object ID when already authorized,
type, version, scope, result, fixed reason, and microsecond occurrence time.
Each string is bounded by its identity limit and a serialized record is limited
to 4 KiB. They never contain regexes, JSON paths, expressions, event values,
generated SQL, snapshots, request bodies, or attacker-supplied error text. The
success record, version, idempotency outcome, and catalog-revision increment are
atomic. An authenticated privileged attempt that is rejected is written to a
separate bounded attempt journal before responding; if that journal is
unavailable the route fails closed with the same generic unavailable response.
Unauthenticated traffic remains in the server access-security log because no
trusted tenant or actor exists to bind a knowledge audit record.

For the nine management operations, the route deadline encloses authentication,
authorization, body handling, catalog work, and response validation.
Authentication accepts a valid browser user or administrator and installs a
detached principal and audit actor while removing the bearer credential; only
then does the knowledge boundary require the administrator role plus exact
handler tenant/owner agreement. A non-administrator or principal mismatch
attempts its journal append before any body byte is read and exposes that
rejection only when the append succeeds. Tenant, owner, and the complete
bounded manageable-app set come only from that principal and the trusted app
catalog.

Validate enters that boundary as `ActionValidate` before decode. Every
authenticated definitive envelope, authorization, resource, catalog, or
serialization rejection therefore follows the same single synchronous append
rule, while a successful sealed HTTP 200 response writes no rejected-attempt
row. The route is intentionally not duplicated in the generic outer
administrator map; that omission does not relax its inner administrator check.

A pre-decode rejection on Update or SetState uses the conservative `update`
action. After complete request validation, a mask selecting `app_id` or
`sharing_scope` refines Update to `scope_change`, while SetState refines to
`enable` or `disable`. Rejection context is accepted only when its app lies in
the derived scope and, except for an idempotency key reused across targets, its
object/app identity binds the submitted operation. Supported absent or hidden
Get outcomes retain the uniform `not_found_or_forbidden` response. Impossible
or unbound configured-dependency errors collapse to `service_unavailable` with
no context; a post-authorization corruption or infrastructure failure may
retain only correctly bound scalar context.

Catalog errors carry one of three dispositions. A definitive pre-commit
rejection triggers one synchronous journal append attempt and is exposed only
after that append succeeds. A proven exact receipt whose mutation is already
committed suppresses a second rejected row even if response reconstruction or
delivery fails. An infrastructure failure before receipt absence/digest
relationship is proven is indeterminate and likewise suppresses a false
rejection. A proven different digest is the definitive
`idempotency_conflict`. The synchronous journal tail ignores client
cancellation but has its own five-second deadline and concurrency gate; gate
exhaustion or append failure returns only the fixed unavailable response.

Mutation requests are validated and snapshotted before a configurable Writer
can mutate caller memory. Each success is rebound to its applicable submitted
authorities—normalized definition or mask, target ID, expected version,
lifecycle marker, and derived tenant/owner/app scope—while its catalog revision
and 32-byte state token are relationship- and shape-checked. Definitions are
shape-preflighted before reflection, sizing, clone, or marshal. New ACTIVE
publication remains disabled. The concrete receipt-first catalog Writer may
nevertheless replay a previously committed ACTIVE outcome after downgrade when
that outcome remains recognized and canonical; no other Writer implementation
can opt into that exception.

The rejected-attempt journal retains at most 100,000 rows per tenant and
atomically evicts the oldest row before appending the next one. Its sequence is
monotonic and never reused; exhaustion of the signed 64-bit sequence space
fails closed rather than wrapping. An attempt row is immutable, limited to the
same 4 KiB scalar-only envelope, and includes an object ID only after that
identity was already authorized. Journal failure is never converted into the
underlying authorization, validation, conflict, or resource-limit response.

The existing general mutation-audit journal retains its established 100,000
row tenant ceiling. Protective quarantine cascades instead use a
dedicated append-only `knowledge_recovery_audit` reserve with exactly 8,192
rows per tenant and a unique `(tenant, object_id)` key. Ordinary actions cannot
consume it. The reserve matches the lifetime 8,192-identity tenant ceiling, so
an initial quarantined generation and later replacement identities each retain
one guaranteed terminal recovery slot. This remains true when general audit,
normal version, and normal idempotency capacity are exhausted. Recovery records
follow the same 4 KiB redacted schema, are merged into administrator audit
reads, are retained for the life of the quarantined forensic identity, and are
committed in the same SQLite transaction as every cascade row. Exhausting this
reserve means every physical identity the tenant can ever allocate already
received its terminal protective transition; the uniqueness constraint
prevents unrelated consumption.

## Explicit deviations and deferred behavior

Open Splunk `0.1` deliberately uses private → app → tenant-global shadowing
rather than claiming full Splunk configuration-layer precedence. It includes
`index` selectors. Extractions are `_raw`-only. Same-stage extraction, alias,
and calculated objects are parallel and cannot chain. Names and matches are
binary case-sensitive. Knowledge cannot overwrite canonical or reserved event
roots. Physical version garbage collection is disabled.

Lookups, event types, tags, macros, workflow actions, arbitrary transforms,
automatic key/value extraction, index-time extraction, multivalue extraction,
ordered calculated groups, app import, data models, acceleration, scripts,
external calls, and custom commands are unsupported by this compatibility
version. Licensed Splunk differential results are recorded when an oracle is
available; absence of an oracle must be stated in checkpoint evidence and must
not be presented as verified equivalence.

## Compatibility fixtures

The normative starter corpus is
`internal/knowledge/testdata/compatibility-v0.1.json`. Each starter case names a
rule, stage, and expected result or publication error. Runtime implementations
extend those records with concrete input relations and visible catalogs rather
than replacing them, and must execute relevant cases through normalization,
resolution, planning, and ClickHouse integration. The fixture structural test
prevents silent deletion, renaming, duplication, or version skew before those
implementations land.

The starter corpus is not evidence that runtime semantics are implemented. Its
strict structural test is only a contract inventory. Before field knowledge is
advertised, every case must contain executable inputs and exact typed outputs
or diagnostics and must run through the relevant production normalization,
resolution, planner, compiler, and pinned ClickHouse paths.

The repository also contains an opt-in public compiler/executor matrix for the
KO-1C field subset. A successful default or non-Docker run proves only fixture
construction and private-limit classification; it skips the container path.
Only a completed run against the required digest-pinned ClickHouse image and
exact certified server version counts as runtime evidence. A cancellation,
compiler-gate stop, skipped test, or table-free SQL probe must be recorded as
pending evidence rather than compatibility acceptance.
