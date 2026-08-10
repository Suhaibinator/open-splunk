# Open Splunk v1 API contracts

This directory is the source of truth shared by the Go server, Go collector, and TypeScript browser application. The package name is `open_splunk.v1`; generated Go code uses package `opensplunkv1` and generated TypeScript follows the source directory hierarchy.

## Contract boundaries

- `value.proto`, `common.proto`, `event.proto`, and `result.proto` define shared wire primitives, exact dynamic values, canonical collected events, dynamic result schemas, field summaries, timelines, and visualization settings.
- `collector.proto` is the only gRPC service. It defines the collector registration and at-least-once batch-delivery stream.
- `search.proto` and `search_api.proto` separate reusable search intent and job
  state from SRouter request/response messages. `search.proto`, `history.proto`,
  `export.proto`, and `search_inspection_api.proto` carry the optional bounded
  `KnowledgeSnapshotSummary`; `search_inspection_api.proto` remains the
  administrator-only completed-job logical/physical plan inspection contract.
- `search_ws.proto` defines binary WebSocket commands and sequenced progress events. It is not a results paging API.
- `saved_search*`, `history*`, and `export*` remain separate because they have different lifecycle, persistence, and security semantics.
- `index*`, `app*`, and `collector_admin*` define control-plane entities plus SRouter operations.
- `audit*` defines the fixed, administrator-only immutable mutation-audit
  projection and bounded list operation. `search_attempt_audit*` defines the
  separately bounded, payload-free search-admission projection and list
  operation and carries only the optional compact `KnowledgeSnapshotRef`.
- `knowledge.proto` defines the common registry projection, authorized selectors,
  Tier-1 typed definitions, versioned dependencies, provenance, and immutable
  search snapshot. `knowledge_api.proto` reserves the protobuf CRUD,
  validation, dependency, and bounded preview messages. The nine
  create/get/list/dependencies/dependents/validate/update/set-state/delete routes
  are registered as one complete administrator-only management unit. Validate
  uses a dedicated bounded decoder and the rollback-only catalog service;
  Preview has only an internal bounded request codec and structural envelope
  validator. Quarantine and Preview remain unregistered route contracts, and
  none of this advertises the Tier-1 capability.
- `system_api.proto` gives the static frontend one bootstrap call for server capabilities and initial app/index choices.

Persistent database rows and ClickHouse table definitions are deliberately not protobuf contracts. Converters at the service boundary keep storage migrations from becoming accidental wire changes.

## Browser routes

Every route below is `POST`, relative to `/api/v1`, and uses `application/x-protobuf` for successful request and response bodies. Non-2xx errors use the standard SRouter/go-common transport error shape. Authentication can be added by SRouter middleware without changing these messages.

Binary protobuf version skew normally follows the ignore-unknown contract.
Unless an authority-sensitive knowledge operation below defines a stricter
rule, a syntactically valid request may contain fields that this server version
does not recognize, including within populated known submessages. Those fields
count toward the route's raw body limit and are discarded at the HTTP transport
boundary before known-field validation or persistence. They cannot change
authentication, authorization, target selection, confirmation, optimistic
versions, update masks, quotas, stored objects, or responses.

Recognized fields keep their complete validation contract. In particular, an
unsupported numeric value in a known enum is not an unknown field and remains
invalid where the operation does not define it. A future oneof arm is absent
from an older server's known view, so an operation that requires that oneof
still rejects the request. New fields whose omission would change a
security-sensitive operation require explicit capability negotiation or a
new versioned route. Malformed or truncated protobuf remains a transport
error, and duplicate known fields retain the protobuf runtime's existing
decode semantics.

Clients may rely on known fields remaining decodable, not on unknown bytes
being preserved or echoed. The Go runtime retains unknown response bytes by
default, while the generated TypeScript decoders skip them.

Knowledge-definition mutation payloads are the deliberate exception to the
generic browser request rule above. Their raw protobuf must be inspected for
unknown fields recursively, including nested messages and unknown future
`oneof` bodies, before any generic decoder can discard those bytes. The route
rejects such a definition as invalid instead of silently accepting a meaning
an older server cannot validate. Read-only response evolution retains the
ordinary known-field decodability rule. Selector-pattern and extraction-output
cardinality is preflighted before either reflection walker, `proto.Size`, clone,
or deterministic marshal so repeated empty messages cannot amplify the raw
wire ceiling into an unbounded traversal allocation.

Validate is a second, read-only exception with authority-sensitive rather than
uniform unknown handling. Its dedicated codec enforces the mutation raw-body
ceiling by reading at most one byte beyond it solely as an overflow witness and
performs a bounded two-pass wire projection instead of the
generic `proto.Unmarshal` allocation path. It preserves protobuf duplicate-
message, last-scalar, and `oneof` merge/reset semantics; validates every known
string as UTF-8 even when unselected or cleared; caps retained mask paths,
selected selector entries, and selected extraction outputs at each semantic
maximum plus one; and rejects malformed wire or unknown-group depth above 32.
Million-entry mask, selected/unselected repetition, and alternating-body oracles
pin bounded retention and allocation behavior.

The candidate wire walker/builders are shared with an internal request-only
Preview codec through envelope-specific field layouts; this extraction does not
change Validate behavior. Correct-wire object-ID presence, including empty,
selects update/mask projection while absence selects the complete create
definition. Duplicate definitions and masks merge, scalars are last-wins,
optional empty/zero presence is preserved, and nested `oneof` merge/reset
behavior matches protobuf decoding. The Preview codec accepts at most 4 MiB
plus 64 KiB (`4259840` bytes), retains at most 9 update-mask paths, 17 entries
in each selected selector dimension, and 17 selected regex outputs, and rejects
malformed wire or unknown-group nesting deeper than 32. It validates every
recognized string occurrence as UTF-8, including values later overwritten,
unselected, or cleared. It retains the last UTF-8 job-ID value through the
256-byte ceiling or a detached 257-byte over-limit witness.

For both candidate request codecs, outer-request unknowns—including wrong-wire
envelope fields—and field-mask unknowns are retained so structural envelope
validation rejects them. Create retains all candidate unknowns, and update
retains unknowns inside mask-selected nested values, so Validate—and a future
Preview service—treat candidate-authored meaning as invalidity. Candidate
top-level unknowns on update and unknowns solely inside unselected values are
discarded because they are outside the exact mask authority. Both types bypass
generic unknown clearing. Validate's successful responses additionally bypass a new
`proto.Marshal`: its transport revalidates and writes the service seal's exact
deterministic bytes, whose complete response bound is 8 MiB.

### Knowledge-object contracts

The `SERVER_FEATURE_KNOWLEDGE_FIELD_OBJECTS` enum value is reserved for the
complete Tier-1 field-knowledge family. A server must not include it in
`GetSystemBootstrapResponse.features` until CRUD, validation, authorized
resolution, immutable snapshot admission, field enrichment, inspection, and
the browser workflow are all configured. Merely generating the messages does
not create a route or advertise a capability.

Knowledge definitions deliberately repeat their indexed app, name, sharing,
and type identity in the registry projection. The catalog storage boundary
requires exact agreement after decoding and reports corruption rather than
repairing one representation during a request. Definition and snapshot SHA-256
fields are raw 32-byte digests and must be length-checked.

`KnowledgeObjectDefinition` reserves field numbers 13 through 31 for future
length-delimited `body` oneof alternatives and numbers 32 or greater for future
ordinary metadata, excluding protobuf's compiler-reserved range. Future
ordinary metadata must be declared in ascending field-number order before the
`body` oneof. Its canonical forward-compatible unknown suffix encodes those
metadata fields first in nondecreasing order, including same-number repeats,
then exactly one body field as the final field. This declaration and wire order
is a cross-language deterministic-encoding and digest contract.

`KnowledgeSnapshot` contains no protobuf maps, floating-point fields, or
timestamps. Unknown fields at any depth are rejected and cleared before
digesting. The digest input includes normalized binary-sorted effective
authorized indexes, the bounded shadow and closed-code warning inventory, and
aggregate query-budget charges. Wall-clock admission time belongs to job
lifecycle metadata outside the digest message.
`KnowledgeSnapshotBudgetCharges.selector_wildcard_work_units` is the static
publication charge over canonical literal/`?`/`*` tokens with weights one, two,
and four. It is not a runtime matcher-transition counter. Runtime execution uses
the deterministic conservative per-dimension assessment defined by the
knowledge compatibility contract, so Go and generated ClickHouse SQL do not
need to expose or reproduce one engine's internal NFA transitions.
`canonical_snapshot_bytes` is computed first as the length of deterministic
`KnowledgeSnapshot` protobuf bytes with both `snapshot_sha256` and
`canonical_snapshot_bytes` cleared. The charge is then populated, the digest
form is deterministically serialized with only `snapshot_sha256` cleared, and
the compatibility framing is hashed. This two-step definition is
non-self-referential and is pinned by golden tests.
Snapshot objects are encoded in ascending `resolution_ordinal`; each stage has
its own contiguous `stage_ordinal`. Shadows sort by winner resolution ordinal,
losing precedence nearest-first (private, app, global), then loser object ID;
their `shadow_ordinal` is the zero-based position in that order. Every
executable object counts as a dependency node even when isolated. Dependency
depth is the longest outgoing path in edges, with leaves and isolated objects
at zero. Edges sort by source depth, explicit source stage rank, source
ID/version, target kind, target ID/version, then role, and carry their unique
contiguous `canonical_ordinal`. Lookup asset references use a contiguous
`asset_ordinal`.
These ordering rules are part of canonical snapshot hashing, not permission to
trust a client-authored snapshot. Knowledge-enabled search admission creates
the snapshot after authentication, app and index authorization, and server-side
catalog resolution. Legacy, resolver-disabled, and app-less admission leaves it
absent.

`KnowledgeSnapshotRef` has explicit presence semantics: absence means disabled
or legacy resolution, while a present reference with `object_count` zero means
enabled-empty resolution. `KnowledgeSnapshotSummary` is limited to 32 KiB and
contains exactly the canonical first `min(ref.object_count, 64)` object
summaries; `objects_truncated` is true exactly when `ref.object_count` exceeds
64. The reference always carries the exact total. Retained internal summaries
may carry authorized object identity, but current browser search-job, history,
export, and inspection projections replace every such disclosure with
`redacted = true`, preserving only ordinal, type, and stage.

These wire fields are provenance, not client-authored execution authority. The
search manager privately seals every completed execution snapshot, legacy or
enabled, together with its explicit enabled bit and exact result generation,
schema, row count, and truncation state. Enabled seals also commit the compiled
query and snapshot authority. Inspection and export accept only
manager-validated authority; export additionally requires the matching
manager-owned result pin.

`SearchHistoryEntry` durably preserves the exact admitted summary from queued
admission through the first terminal publication. Production commits the
pending history row and its `SearchAttemptAuditEvent` in one SQLite transaction;
the audit event retains only the summary's compact reference. Terminal history
publication must reproduce the same summary and cannot add, remove, or replace
that admission authority.

Executable object order is stage, normalized binary name, and stable object ID.
There is no client-authored stage order. `QUARANTINED` is a terminal
administrator-recovery state and ordinary state mutation must reject it. A
separate prepare operation performs the privileged integrity scan and returns
only a single-use, ten-minute HMAC token plus bounded metadata. The quarantine
request accepts that token and a required fresh `client_request_id`; the token
binds the raw corrupt root and its bounded ordered active-dependent closure so
recovery does not decode attacker-controlled or future definition bodies.
`DELETED` is a distinct retained terminal tombstone created only by the delete
route; it is never executable and is not accepted by create or ordinary state
mutation. The current tombstoned registry identity remains the authorization
authority, so an authorized administrator can inspect its current projection
and retained historical versions under current policy. Delete does not
physically erase immutable definition or version data.
Every mutation request uses a required 16–128 byte printable-ASCII
`client_request_id`; omission is invalid rather than non-idempotent execution.
Ordinary state mutation accepts only `ACTIVE` or `DISABLED`; `DRAFT` is an
initial create state, while quarantine and delete have dedicated routes. Field
masks on update, validate, and preview are relative to
`KnowledgeObjectDefinition`, never the containing request message.

Provenance uses a typed `oneof`: authored SPL carries only its source range, a
currently authorized object carries its stable identity and definition
location, and a currently unauthorized historical object carries only its
stage, bounded object kind, and response-local fixed redacted ordinal. The
redacted wire variant cannot retain an object ID, name, version, owner, app, or
definition location.

The route comments in `knowledge_api.proto` reserve the intended endpoint
names. The browser-route table below contains exactly the nine production
object-management routes currently registered by `NewHandler` when its complete
management dependency unit, including a constructor-ready concrete Writer, is
present. Registration is all-or-none and independent of bootstrap feature
advertisement. The quarantine and preview messages do not create routes.
Preview's internal request codec and structural validator likewise do not
register a route, install a response codec, or add the message to the route
manifest or browser bearer policy.
Validate is registered but deliberately absent from the browser administrator-
bearer allowlist and the backend's generic outer administrator-route map; its
inner knowledge-attempt boundary authenticates and authorizes the administrator
before its dedicated decoder runs. List and graph continuations use bounded
`PageRequest`/`PageResponse`
contracts. The implemented List cursor binds all normalized filters, ordering,
caller scope, page bound, and the
first-page catalog revision plus state commitment. Each implemented graph
cursor additionally binds its direction, requested-version presence and value,
resolved root identity, and total-count choice; a catalog revision or state-
commitment change invalidates continuation.

Two typed candidate-issue seams exist inside the Go implementation; the HTTP
handler never maps arbitrary normalizer/compiler errors directly. The Writer
consumes only their closed typed projection while constructing its sealed
result. Definition normalization remains fail-fast
and exposes only detached `KNOWLEDGE_DEFINITION_INVALID`,
`KNOWLEDGE_DEFINITION_UNKNOWN_FIELD`, or
`KNOWLEDGE_DEFINITION_RESOURCE_LIMIT` issues. Separately,
`knowledgeprogram.Compile` may bind one detached semantic issue to the exact
input index responsible for an intrinsic regex syntax/resource/capture-shape,
JSON-path syntax/unsupported/resource, calculated-expression `SPL_*`, or
direct-Boolean-result failure. An adapter must independently prove that index
is the submitted candidate, should compile a singleton candidate at index zero,
and must never project an issue from a winner cohort. `Prepare` and authority,
aggregate, cohort/collision/selector, and dependency failures stay opaque to
this typed compiler-issue seam; legacy error text and sentinel behavior remain
unchanged.

`internal/knowledgevalidation` now implements a pure result-construction layer,
not a service. It owns no catalog, database, transaction, transition,
authorization, routing, or HTTP policy. Inactive construction only normalizes
and returns an opaque detached `Result`; active preparation normalizes and
singleton-compiles into either an opaque terminal invalid result or an opaque
candidate carrying no catalog authority. It maps only the typed definition and
compiler issues above, checks their exact code/path/body/range/suggestion
shape, and converts untyped or malformed local failures into opaque invariant
errors. Authority, cohort, aggregate, dependency, and transition failures
receive no typed issue mapping through inactive/active preparation; the sole
result-layer exception is the caller-selected, target-free
`BuildDependencyUnavailable` generic diagnostic below.

A full ACTIVE-transition caller must supply the complete already-authorized
direct target projection plus an evaluation-local candidate identity. The layer
validates and canonicalizes the target-only shape, bound, `FIELD_INPUT` role,
versions, duplicates, and self-reference and seals exact dependency charges; it
cannot prove catalog completeness or visibility. Its sole transition-adjacent
exception, `BuildDependencyUnavailable`, accepts no target identity and emits
the generic `KNOWLEDGE_DEPENDENCY_UNAVAILABLE` diagnostic.

The Validate request requires a present definition and exactly one
nonzero `KnowledgeValidationIntent`. `INACTIVE_STORAGE` proves only bounded
canonical persistence in an inactive state; `ACTIVE_PUBLICATION` evaluates the
candidate as a proposed ACTIVE version in one fixed knowledge/app/index catalog
transaction. The returned revision identifies only the advisory knowledge-
ledger component; zero proves that ledger is empty. It is not complete
transaction authority, a reservation, mutation proof, or promise that later
validation will agree. Unspecified and unknown intent values are request-
envelope errors. Create mode
is selected only by an absent object ID and requires both expected version and
the update-mask message to be absent. Update mode requires a present nonempty
ID, a present expected version in the inclusive range 1 through MaxInt64
(`9223372036854775807`), and a present nonempty canonical field mask relative
to `KnowledgeObjectDefinition`; it applies that mask to the exact current
version. A missing top-level definition is an envelope error, while a present
definition with no recognized body is candidate-authored invalidity.

The HTTP adapter first consumes only the bounded request projected by the
Validate-specific codec. It requires the exact ready concrete catalog Writer,
holds response-serialization capacity across service and encoding work, detaches
request binding authority, and derives independent cloned read/write scopes from
the authenticated principal and trusted app catalog. Only the closed definitive
validation error taxonomy may cross as a specific non-2xx result. Its sole
admitted join is the exact `control.ErrCapacityExceeded` plus
`knowledgevalidation.ErrResponseTooLarge` pair; impossible or every other
joined authority collapses to the generic unavailable response.

The concrete `knowledgecatalog.Writer.Validate` adapter accepts a
`ValidationScope` whose `ReadScope` and `WriteScope` must have the same
authenticated tenant and owner but may carry independent app sets. Write scope
authorizes the requested root and applied candidate app; read scope separately
filters which derived dependency targets may be returned. The adapter uses a
shared per-control-database, one-slot fail-fast admission gate. Before admission
it validates only the bounded envelope. Under the gate, create selects the
whole definition, while update builds a shallow view of only mask-selected
top-level fields; unselected payload is neither byte-charged nor cloned.

Each selected selector dimension and a selected regex output list have a
16-entry ceiling. Cardinality is checked in definition-normalization order
before an exact byte-size traversal. An overflow is replaced with a detached,
newly allocated 17-entry witness retaining only the applicable body kind, so
normalization yields its standard typed resource issue without cloning or
walking the caller's repeated list. Otherwise the selected request view must
fit the ordinary mutation-request byte bound before it is cloned. This witness
does not waive the independently sealed 8 MiB response limit.

Every admitted validation runs in one fresh `BEGIN IMMEDIATE` transaction and
always rolls it back before response sealing. For update, requested-root
authorization precedes expected-version, lifecycle, current-record integrity,
and opaque-current rejection, which in turn precede applied-candidate issue
construction. Candidate-local invalidity precedes app/index inventory after
that root is established. Only an authorized root or candidate app may appear
in service error context; dependency targets never do. Every service error
defaults to definitive rejection.

`INACTIVE_STORAGE` normalizes only, authorizes the app of a valid applied
definition, and bookends the knowledge revision; it performs no publication
compile, ACTIVE inventory read, or dependency derivation.
`ACTIVE_PUBLICATION` performs singleton candidate preparation and then the
complete transactional ACTIVE transition. Every affected candidate-absent
baseline cohort is compiled before a post-candidate conflict can be classified,
preventing stored baseline invalidity from becoming a candidate diagnostic.
Opaque conflict decisions have deterministic stronger-conflict precedence.
Only cohort-local target absence is eligible for the generic target-free
dependency diagnostic. Rich targets are integrity-checked against the exact
current ACTIVE registry/version before `ReadScope` projection; a missing or
unauthorized target produces that same generic result, while all other
transition, integrity, catalog, resource, and infrastructure failures remain
out-of-band errors.

ACTIVE create chooses the first deterministic
`knowledge-validation-candidate-%04x` value absent from the complete bounded
tenant inventory after reconciling the identity ledger and physical row count.
It calls no mutation ID generator and reserves or returns no identity. The
response carries the same transaction's exact knowledge-ledger revision. Zero
is permitted only with a physically empty knowledge object ledger, and all
revision paths are bookended. Rollback failure invalidates the request. The
adapter performs no DML, commit, audit, idempotency operation, publication hook,
clock read, or mutation-ID allocation.

The evaluation-local create identity is alpha-invariant: validity, diagnostics,
candidate resources, and the target-only dependency projection must be
unchanged under every other fresh candidate-ID choice. A later Create generates
its own ID and revalidates the then-current catalog, app, and index facts;
intervening facts may therefore change its outcome.

Only candidate-authored invalidity is returned in-band as `valid=false`; the
registered HTTP adapter maps that sealed result to HTTP 200. It retains at
least one field violation or ERROR diagnostic and
omits normalized definition, digest, dependencies, and resources. A valid
result requires the normalized definition, its exact 32-byte deterministic
protobuf digest, complete resources, no field violations or ERROR diagnostics,
and a false field-violation truncation flag. Request, authentication,
authorization-to-the-requested-object, catalog-integrity, hidden-inventory, and
service failures remain out-of-band; the handler maps only its closed service-
error authority to uniform non-2xx outcomes. Result and response unknown
fields are rejected recursively before issue canonicalization and deterministic
serialization; the complete response is capped at 8 MiB.
`object_type` is unspecified only when an invalid candidate's body cannot be
identified; every other result carries the exact applied body type.

`valid` is advisory definition validity under the selected intent, not mutation
acceptability, a reservation, or a promise. A masked no-op update may be valid;
`INACTIVE_STORAGE` against a current ACTIVE object proves only hypothetical
non-ACTIVE storage validity and never ACTIVE Update admissibility. A later
Writer independently revalidates then-current authorization, version,
lifecycle, capacity, app, index, and publication authority.

Every resource charge describes only the applied candidate. An inactive result
reports exact selector-pattern and normalized-definition-byte charges, with an
empty dependency list and zero dependency and compile-derived charges. An
active result derives its intrinsic compile fields from a canonical singleton
program whose only object is the normalized candidate and whose dependency list
is empty. These exact fields are generated operators/fields, regex
programs/work, scalar expressions/nodes, extraction outputs, JSON evaluation
work, and scalar predicates; they are neither affected-cohort totals nor
marginal post-fusion deltas. The last three are append-only fields 12, 13, and
14; waived draft SQL-size field/name 11 remains reserved. The complete set of
at most 1,024 unique, authorized direct `FIELD_INPUT` dependencies is ordered
by binary target ID, version, and role. Dependency nodes are the distinct exact
returned targets excluding the candidate, and dependency edges equal the
returned list size; both derive from the full ACTIVE transition rather than the
singleton program. A missing or unauthorized target produces only the
static `KNOWLEDGE_DEPENDENCY_UNAVAILABLE` candidate diagnostic; it exposes no
target identity, and missing and forbidden are indistinguishable.

Field violations and located diagnostics each admit at most 256 exact-deduped
entries. Their longest deterministic prefixes have aggregate UTF-8 text budgets
of 256 KiB and 768 KiB respectively, with explicit truncation flags. Each path
is at most 1 KiB, code 128 bytes, and message 4 KiB; diagnostics permit at most
32 unique binary-sorted 1 KiB suggestions. Aggregate charges sum the UTF-8
lengths of path, code, message, and—for diagnostics—every suggestion, without
separators or wire framing. Field violations sort by binary path/code/message.
Diagnostics use the total order ERROR, WARNING, INFO, then path, absent range
before present range, offsets, code/message, derived coordinates, and suggestion
sequence; unspecified or unknown severity is invalid service output. A present
source range requires nonnil start and end positions, is half-open within the
exact field scalar, lands on UTF-8 code-point boundaries, and carries uniquely
derived one-based coordinates where LF starts a new line and every other
Unicode scalar, including CR, advances the column. Any future issue field must
extend entry validation, deduplication, and this total comparison key before it
may be emitted. All issue text is limited to stable static
templates plus exact source text already in the applied candidate scalar; it
cannot expose another object, app, owner, name, ID, version, digest,
definition, non-candidate index inventory, cohort/global count, generated SQL,
or hidden authority.

For ranged typed diagnostics, the internal result layer re-normalizes a
detached submitted definition before rebasing canonical scalar offsets. A JSON
path must remain byte-identical; a calculated expression is mapped through its
exact ASCII trim, including canonical EOF after submitted trailing trim. Each
range retains a private field-path/source-scalar provenance sidecar. Projection
and sealing rederive code-point boundaries and the one-based LF/Unicode-scalar
coordinates from that sidecar, rejecting missing, relabelled, or altered
provenance rather than sanitizing it.

The opaque `SealValidateResponse` boundary revalidates result kind, normalized
definition/digest, inactive zeros or a fresh active singleton compilation,
transition-supplied dependencies and exact resource formulas, issue ordering
and private range provenance, recursive unknown-field absence, and a revision
at most MaxInt64. It retains and returns detached copies of the exact
deterministic encoding only when the complete response is at most 8 MiB. No
database read, catalog proof, route registration, or HTTP mapping occurs there.
The Writer adapter supplies those catalog, transaction, transition, and
authorization proofs and calls this seal only after successful rollback. The
registered handler and custom encoder consume the seal without reopening its
mutable protobuf authority. Validate remains absent from the browser bearer
allowlist and capability advertisement.

Preview's internal request-only codec accepts the retained-search-job scalar
plus the same create/update candidate fields. The canonical field authority is
`retained_search_job_id = 1`, `definition = 2`, optional
`knowledge_object_id = 3`, optional `expected_version = 4`,
`update_mask = 5`, and optional uint32 `maximum_rows = 6`; Preview accepts no
independent intent. The retained job ID names future owner-scoped retained
execution authority which a service must reacquire under the authenticated
caller. It is not an immutable-event-snapshot identity and does not itself
grant access.

The structural validator requires a nonempty valid-UTF-8 job ID of at most 256
bytes, unchanged by whitespace trimming and containing no Unicode control code
point. It rejects outer unknown authority, including wrong-wire envelope
fields, and mask unknowns, then synchronously passes the candidate through the
exact Validate create/update envelope with the server forcing
`ACTIVE_PUBLICATION`. It performs no retained-job lookup or authorization and
never mutates or normalizes the decoded request. Create full-candidate unknowns
and update mask-selected nested unknowns remain candidate authority for future
service evaluation; update candidate top-level and unselected nested unknowns
are outside the mask authority and are discarded.

`maximum_rows` preserves full optional uint32 wire authority: absence,
explicit zero, and every value through `4294967295` remain distinct and
unchanged. This request boundary assigns no default, bound, or execution
meaning. Generated Go and TypeScript contract oracles independently preserve
the create tags `[1, 2]` and all six present-empty update tags; the Go
structural oracle and TypeScript wire oracle additionally preserve the maximum
uint32 value. The frozen request comments and oracle coverage are wire-neutral:
no field number, type, or presence encoding changed.

Preview remains unregistered and unadvertised. There is no Preview response
codec, handler, catalog/search service, retained-execution acquisition or
caller-authorization integration, route, TypeScript route entry, browser bearer
attachment, capability, UI/navigation request, Resolver attachment, or search
execution. A future service must reacquire the owner-scoped retained execution,
evaluate definition validity in one fixed knowledge/app/index transaction,
apply the validated candidate program to that retained server-authorized
execution, and freeze row-limit, paired before/after schema-row, truncation,
response-byte, deadline, and concurrency semantics while retaining the
advisory-only revision contract. The production nonempty compiler,
snapshot-finalization, and digest-pinned ClickHouse acceptance gates remain
closed. The compiler-only and dual-tag snapshot lifecycle test bridges open no
Preview service or wire authority. The runtime tag alone remains insufficient
for `Authority.Finalize`; the dual-tag test finalizer grants no route or
production authority. Preview must never accept raw events, physical table
names, index authority, asset paths, or SQL. Validate remains registered but
unadvertised.

This validation redesign intentionally uses a historical FILE-compatibility
waiver. The earlier draft result fields 6 (`diagnostics`) and 7
(`dependencies`) and resource field/name 11
(`estimated_generated_sql_bytes`) were retired before Validate was registered
and were never served by either the Validate or Preview route; all tags and the
resource name remain reserved against reinterpretation. Peers may drop those
never-served draft unknown values, but the change must not be described as
schema non-breaking. Validate's later registration does not retroactively
change that historical classification; Preview remains unregistered.

The dependency routes expose only direct persisted object-to-object edges and
never snapshot-global stage, depth, ordinal, or definition-digest authority.
`dependencies` resolves one exact source version, including an authorized
historical version, but authorizes it solely through the source's current
registry identity. `dependents` resolves one exact target version but admits
edges only from source versions which are those objects' current registry
versions; every nonquarantined source lifecycle state is eligible. Both routes
authorize the root from its current registry identity, return the uniform not-
found-or-forbidden result for a missing, hidden, or currently quarantined root,
and omit a hidden or quarantined opposite endpoint before pagination and total
counting. Exact totals are returned only when `include_total_size` requests
them; no hidden count or redacted edge placeholder is emitted.

The registered management boundary authenticates before reading a protobuf
body and derives tenant, owner, and complete manageable-app scope only from the
detached browser principal plus the trusted app catalog. An authenticated
definitive rejection triggers exactly one synchronous append attempt to the
separate bounded attempt journal. Its underlying response is exposed only after
that append succeeds; append or journal-gate failure instead returns the fixed
unavailable response. A pre-decode Update or SetState rejection uses the
conservative `update` action; only a fully validated mask or state may refine
it to `scope_change`, `enable`, or `disable`. A known-committed mutation failure
or an indeterminate outcome never creates a second rejected-attempt row.
Validate enters this same boundary as `ActionValidate` before decode and is not
duplicated in the generic outer administrator map. Its authenticated definitive
rejections follow the same single synchronous append-attempt rule, while an
HTTP 200 sealed result appends no rejected-attempt row.
Recognized definitions may be created ACTIVE, updated while ACTIVE, or enabled
from DRAFT/DISABLED only after the concrete Writer proves the complete
transactional catalog/app/index authority and compiler-derived dependency
closure. Opaque future definitions cannot be updated or enabled as ACTIVE. An
exact retained ACTIVE result remains replayable after downgrade only when the
retained outcome is still recognized and canonical.

Production composes the management Store, concrete ready Writer, attempt
journal, app authority, and a concrete Resolver. It retains that Resolver for
readiness but intentionally does not attach it to `searchjobs.Manager`, and it
does not advertise the capability. The dependency, dependent, and Validate
routes are represented in the central TypeScript route manifest. Only the graph
routes join Get/List in the browser administrator-bearer allowlist; Validate and
every knowledge mutation remain excluded. Preview is absent from the manifest,
bearer allowlist, handler route set, and capability response; its internal
request boundaries do not acquire a retained job or read a catalog. The hidden
read-only Knowledge Manager is still omitted from navigation, is not dynamically
loaded, and therefore issues no production-bootstrap knowledge API request. Its
dormant
surface is app/object-type/lifecycle-state filter-ready with name-ascending, updated-time-
descending, created-time-descending, and object-type-ascending sorts plus exact
continuation reuse. Its detail view now requests the selected exact version's
dependencies and dependents independently, pages and labels each direction at
its own catalog revision, and displays only each visible opposite endpoint's
object ID, version, and `FIELD_INPUT` role.
Production nonempty compiler, snapshot-finalization, and execution gates remain
closed, so no shipping knowledge execution is claimed. The acceptance bridges
are deliberately outside the protobuf/runtime capability. Let A mean
`open_splunk_knowledge_runtime_acceptance` and B mean
`open_splunk_knowledge_snapshot_acceptance`. A additionally requires
`testing.Testing()` before the public Compiler can seal. B is conjoined with A
and an independent `testing.Testing()` check at finalization. Thus default mode
keeps both gates closed, A alone seals but cannot finalize, B alone cannot seal,
and A+B lets the A-enabled test-only Compiler cross finalization only inside
`go test`.

One shared plan matrix reaches the exact seal with zero typed results by
default, while the A-tagged test binary seals thirteen public compiler/derived
surfaces only for non-Docker construction evidence: nine shared `Compile`
cases—ordinary, selector controls, chart, timechart, stats, stacked chronology,
pruned consumer, runtime-empty consumer, and alias overflow—plus timeline,
field catalog, field summary, and field suggestions. The ordinary proof pins 19
public output fields, 13 detached container descriptors, 39 private sidecar
columns, exact admitted-program evidence, and detached clone invalidation;
ordinary and timeline use executable `where isnotnull(regex_value)` rather than
the former parser-invalid wildcard-shaped predicate, without claiming authored
wildcard-predicate coverage.

The A+B direct test sends a real public Compiler seal through
`Authority.Finalize` and pins exact scope/count/budget fields plus summary,
digest, encoding, prelude commitment, retained compiler budgets, detachment,
tamper, scope, and equal-charge program-substitution rejection. A second A+B
test uses the real Writer, Resolver, and Manager to retain distinct ACTIVE v1
and v2 snapshot/prelude/compiler authority across fake dispatch and owner-
scoped access. This is lifecycle/identity staging only: it executes no
ClickHouse row and adds no production Resolver attachment, route, capability,
browser request, protobuf behavior, or runtime acceptance.

At `9f8c8ace0da51b837ebccc0eca1e61db8e9c2dcf` (exactly 135
post-`c5440b9` commits), all four full `internal/knowledgesnapshot` modes and
the four named Manager entrypoint modes pass. The direct snapshot and Manager
lifecycle tests pass together under `-race`; default and dual-tag vet pass; and
isolated tracked-only ClickHouse default/A full plus A-tagged race runs pass.
The full server package is not claimed green: the same two pre-existing field-
catalog/field-summary HTTP tests and two blocked-worker search-analysis tests
fail identically in every mode. `gofmt` and `git diff --check` pass. The
protected pre-existing untracked ClickHouse probe
was excluded and left untouched without opening or hashing it.

The three added `Compile` cases prove relationships rather than one full SQL
golden: one physical scan, placeholder/argument equality with the exact ordered
authored suffix, and exact program/scope evidence for each; paired and
increasing CTE stages, materialization/terminal-setting authority, exact
source/direction/measure/aggregate/output binding, immutable chronology versus
authored sort, and live validation for stacked chronology; and `event_id`-only
valid empty container authority plus retained guards/validation for pruned and
runtime-empty consumers. Only the runtime-empty predicate precedes its
still-live validation union. These are compile-only additions, not Docker
executor rows. Default and either single-tag mode remain closed. A supported
dual-tag server `go build` remains closed; a separate disposable tracked-source
probe in an ordinary binary observed only the private snapshot helper returning
`false` and
did not dynamically call public `Authority.Finalize`, whose compiler path would
stop first at its own test-process guard. This is not a claim of adversarial
linker resistance. Docker acceptance was **NOT RUN** and is still
paused/canceled.

Collector display-name and enabled-state mutations return a
`CollectorAdministrationSnapshot`, not a full operational `CollectorRecord`.
The snapshot is the exact durable result of the optimistic update and contains
only fleet-owned fields. This keeps a committed security-critical disable
reportable even when unrelated runtime telemetry is unavailable or corrupt;
clients may refresh `/collectors/get` separately for a current operational
projection.

| Path | Request | Response |
| --- | --- | --- |
| `/system/bootstrap` | `GetSystemBootstrapRequest` | `GetSystemBootstrapResponse` |
| `/knowledge/objects/create` | `CreateKnowledgeObjectRequest` | `CreateKnowledgeObjectResponse` |
| `/knowledge/objects/get` | `GetKnowledgeObjectRequest` | `GetKnowledgeObjectResponse` |
| `/knowledge/objects/list` | `ListKnowledgeObjectsRequest` | `ListKnowledgeObjectsResponse` |
| `/knowledge/objects/dependencies` | `ListKnowledgeObjectDependenciesRequest` | `ListKnowledgeObjectDependenciesResponse` |
| `/knowledge/objects/dependents` | `ListKnowledgeObjectDependentsRequest` | `ListKnowledgeObjectDependentsResponse` |
| `/knowledge/objects/validate` | `ValidateKnowledgeObjectRequest` | `ValidateKnowledgeObjectResponse` |
| `/knowledge/objects/update` | `UpdateKnowledgeObjectRequest` | `UpdateKnowledgeObjectResponse` |
| `/knowledge/objects/set-state` | `SetKnowledgeObjectStateRequest` | `SetKnowledgeObjectStateResponse` |
| `/knowledge/objects/delete` | `DeleteKnowledgeObjectRequest` | `DeleteKnowledgeObjectResponse` |
| `/search/jobs/create` | `CreateSearchJobRequest` | `CreateSearchJobResponse` |
| `/search/jobs/get` | `GetSearchJobRequest` | `GetSearchJobResponse` |
| `/search/jobs/list` | `ListSearchJobsRequest` | `ListSearchJobsResponse` |
| `/search/jobs/results` | `GetSearchResultsRequest` | `GetSearchResultsResponse` |
| `/search/jobs/fields/list` | `ListSearchFieldsRequest` | `ListSearchFieldsResponse` |
| `/search/jobs/field-summary` | `GetSearchFieldSummaryRequest` | `GetSearchFieldSummaryResponse` |
| `/search/jobs/timeline` | `GetSearchTimelineRequest` | `GetSearchTimelineResponse` |
| `/search/jobs/cancel` | `CancelSearchJobRequest` | `CancelSearchJobResponse` |
| `/search/jobs/inspect` | `InspectSearchJobRequest` | `InspectSearchJobResponse` |
| `/search/validate` | `ValidateSearchRequest` | `ValidateSearchResponse` |
| `/search/suggestions` | `GetSearchSuggestionsRequest` | `GetSearchSuggestionsResponse` |
| `/search/history/get` | `GetSearchHistoryEntryRequest` | `GetSearchHistoryEntryResponse` |
| `/search/history/list` | `ListSearchHistoryRequest` | `ListSearchHistoryResponse` |
| `/search/history/delete` | `DeleteSearchHistoryEntryRequest` | `DeleteSearchHistoryEntryResponse` |
| `/search/history/clear` | `ClearSearchHistoryRequest` | `ClearSearchHistoryResponse` |
| `/search/exports/create` | `CreateExportJobRequest` | `CreateExportJobResponse` |
| `/search/exports/get` | `GetExportJobRequest` | `GetExportJobResponse` |
| `/search/exports/list` | `ListExportJobsRequest` | `ListExportJobsResponse` |
| `/search/exports/cancel` | `CancelExportJobRequest` | `CancelExportJobResponse` |
| `/saved-searches/create` | `CreateSavedSearchRequest` | `CreateSavedSearchResponse` |
| `/saved-searches/get` | `GetSavedSearchRequest` | `GetSavedSearchResponse` |
| `/saved-searches/list` | `ListSavedSearchesRequest` | `ListSavedSearchesResponse` |
| `/saved-searches/update` | `UpdateSavedSearchRequest` | `UpdateSavedSearchResponse` |
| `/saved-searches/duplicate` | `DuplicateSavedSearchRequest` | `DuplicateSavedSearchResponse` |
| `/saved-searches/delete` | `DeleteSavedSearchRequest` | `DeleteSavedSearchResponse` |
| `/indexes/create` | `CreateIndexRequest` | `CreateIndexResponse` |
| `/indexes/get` | `GetIndexRequest` | `GetIndexResponse` |
| `/indexes/list` | `ListIndexesRequest` | `ListIndexesResponse` |
| `/indexes/update` | `UpdateIndexRequest` | `UpdateIndexResponse` |
| `/indexes/state/set` | `SetIndexStateRequest` | `SetIndexStateResponse` |
| `/indexes/delete` | `DeleteIndexRequest` | `DeleteIndexResponse` |
| `/indexes/stats/get` | `GetIndexStatsRequest` | `GetIndexStatsResponse` |
| `/indexes/fields/list` | `ListIndexFieldsRequest` | `ListIndexFieldsResponse` |
| `/apps/create` | `CreateAppRequest` | `CreateAppResponse` |
| `/apps/get` | `GetAppRequest` | `GetAppResponse` |
| `/apps/list` | `ListAppsRequest` | `ListAppsResponse` |
| `/apps/update` | `UpdateAppRequest` | `UpdateAppResponse` |
| `/apps/state/set` | `SetAppStateRequest` | `SetAppStateResponse` |
| `/apps/delete` | `DeleteAppRequest` | `DeleteAppResponse` |
| `/collectors/list` | `ListCollectorsRequest` | `ListCollectorsResponse` |
| `/collectors/get` | `GetCollectorRequest` | `GetCollectorResponse` |
| `/collectors/update` | `UpdateCollectorRequest` | `UpdateCollectorResponse` |
| `/collectors/state/set` | `SetCollectorEnabledRequest` | `SetCollectorEnabledResponse` |
| `/audit/events/list` | `ListAuditEventsRequest` | `ListAuditEventsResponse` |
| `/audit/search-attempts/list` | `ListSearchAttemptAuditEventsRequest` | `ListSearchAttemptAuditEventsResponse` |
| `/ingestion-tokens/create` | `CreateIngestionTokenRequest` | `CreateIngestionTokenResponse` |
| `/ingestion-tokens/get` | `GetIngestionTokenRequest` | `GetIngestionTokenResponse` |
| `/ingestion-tokens/list` | `ListIngestionTokensRequest` | `ListIngestionTokensResponse` |
| `/ingestion-tokens/update` | `UpdateIngestionTokenRequest` | `UpdateIngestionTokenResponse` |
| `/ingestion-tokens/revoke` | `RevokeIngestionTokenRequest` | `RevokeIngestionTokenResponse` |

### Audit events

`POST /api/v1/audit/events/list` is administrator-only. Tenant and owner scope
come from the authenticated browser principal and cannot be supplied on the
wire. The first contract contains successful ingestion-token, index, app, and
saved-search mutations. Actor kind, actor role, action, and target kind are
fixed enums; the projection contains no arbitrary payload, saved-search
definition, or credential material. Administrative token, index, and app
mutations require a system or browser-administrator actor. Saved-search
mutations also accept an authenticated browser-user actor; the current trusted
single-user saved-search routes record the system actor when no browser
principal is present. App deletion reports the final archived app version
rather than a fictitious post-delete generation.

Pages are ordered by descending tenant-local sequence and capped at 200 rows.
The opaque HMAC-authenticated cursor binds tenant, exact normalized filters,
page size, total-size choice, sequence boundary, and an immutable first-page
journal high-water identity. Later appends cannot enter the traversal, and a
cursor from a database state newer than a restored snapshot fails instead of
silently traversing the wrong prefix. See
[Audit events v0.1](audit-events-v0.1.md) for storage, atomicity, capacity,
redaction, and frontend requirements.

### Search-attempt audit

`POST /api/v1/audit/search-attempts/list` is administrator-only. Tenant scope
comes from the authenticated browser principal and cannot be supplied on the
wire. Its immutable projection contains tenant-local sequence, occurrence
time, fixed actor identity, owner ID, search-job ID, and an optional compact
`KnowledgeSnapshotRef`. That reference contains only digest, catalog/compiler
identity, and exact object count; no object inventory or definition is stored.
The projection never exposes SPL, index or app scope, generated SQL, results,
warnings, failures, headers, credentials, or arbitrary metadata.

Pages are ordered by descending sequence, capped at 200 rows and 2 MiB, and
support exact actor-ID and owner-ID filters. The purpose-separated authenticated
cursor binds tenant, filters, page size, total-size choice, retained-range
floor, and first-page high-water identity. Appends below the rolling ceiling
cannot enter a continuation; an eviction invalidates the continuation instead
of returning an incomplete snapshot. See
[Search-attempt audit v0.1](search-attempt-audit-v0.1.md) for atomic admission,
retention, corruption, privacy, and frontend requirements.

### Index deletion

`POST /api/v1/indexes/delete` is administrator-only. Every request supplies the
original archived optimistic version and a `confirmation_name` equal to the
canonical stored index name. `KEEP_DATA` requires the index still to be
archived at that version, completes synchronously, returns only `index_id`,
hides the catalog entry behind a permanent tombstone, and retains its
ClickHouse events.

`DELETE_DATA` is asynchronous. A fresh admission requires archived version
`N`; `N = MaxInt64` is rejected because the durable transition must create
deleting generation `N+1`. Tenant scope never comes from the protobuf request:
the server binds the operation to its trusted authenticated/configured tenant.
On success, HTTP 200 returns `index_id` and a nonempty
`deletion_operation_id` after the operation is durable. A nonblocking
postcommit wake asks the deletion coordinator to reconcile promptly, while its
periodic scan provides restart recovery and is the correctness backstop.

Exact retries using the original archived version are idempotent only while
the durable operation remains outstanding. Sequential and concurrent retries,
including after process restart and through the equivalent ID or canonical
name selector, return the same operation ID. A stale version, wrong state, or
different confirmation is rejected without starting another operation.
Terminal completion consumes the outstanding operation and tombstones the
catalog entry; after that point the same request returns `404 Not Found`.
Clients must not treat this route as an indefinite completion-status or
terminal-response replay API.

### Index statistics

`POST /api/v1/indexes/stats/get` is administrator-only and accepts the same ID
or canonical-name selector as the other index administration routes. The
server first resolves a current, non-tombstoned catalog record, then captures
the largest committed storage-visibility sequence and one UTC,
millisecond-aligned `measured_at` instant. Tenant, index ID, and canonical
index name are trusted server inputs; none can be supplied or replaced by the
browser request.

`event_count`, `earliest_event_time`, and `latest_event_time` are exact for
rows in that logical scope satisfying all three measurement boundaries:
`expires_at > measured_at`, `index_time <= measured_at`, and
`visibility_seq <= visibility_cutoff`. An empty index has count and storage
bytes zero and omits both event-time bounds. A nonempty index always returns
both bounds.

Because all logical indexes share one MergeTree, attributing compressed part
bytes exactly to one tenant/index key would require an additional expensive
scan. `storage_bytes` is therefore the overflow-checked ceiling of
`active_table_bytes * event_count / active_table_rows`. It is a proportional
estimate based on active `system.parts` metadata, and `estimates` is always
true even though the logical count and time bounds are exact. Inconsistent or
racing physical counters fail closed instead of returning a misleading
estimate.

The native ClickHouse reader issues exactly one parameterized aggregate for an
empty result and one additional active-parts aggregate for a nonempty result.
Only one native statistics operation may use the shared runtime connection at
a time; concurrent saturation fails fast rather than queueing or occupying the
sessions needed by ingestion and search. The complete operation owns a
ten-second deadline, and every query is read-only with explicit execution,
memory, read, result, thread, query-size, and subquery-depth limits. The
GORM/SQLite control plane performs only catalog selector resolution; the event
and part-statistics reads remain native ClickHouse operations.

`ListIndexesRequest.include_stats` enriches only the already-filtered,
metadata-sorted catalog page, whose endpoint maximum is 64 indexes. The
GORM/SQLite control plane remains responsible for catalog filtering, ordering,
cursor validation, and pagination. The catalog has a hard ceiling of 1,024
physical index identities, including permanently retained terminal rows. One
trigger-maintained singleton holds its physical count and global revision.
Index creation checks capacity and inserts under one immediate GORM
transaction; concurrent callers cannot both consume the last slot, tombstones
do not release capacity, and physical identity deletion/replacement is
forbidden.

The signed continuation contains the catalog revision and the final
`(name|created_at|updated_at, index_id)` key. GORM applies state and literal
text filters, the matching keyset predicate, deterministic ordering, and
`LIMIT page_size + 1` in SQLite under one read-only transaction. Exact totals
are counted only when requested. No offset or full-record catalog scan is
used. Text filtering is ASCII-case-insensitive and non-ASCII-case-sensitive
across name, display name, and description; `%` and `_` remain literal.
Create, definition update, state transition, deletion admission, and terminal
tombstone insertion all invalidate an outstanding continuation.

After that page is fixed, the server captures one committed visibility cutoff
followed by one UTC millisecond measurement instant and submits the page's
trusted tenant/index scopes to one native grouped ClickHouse query. Missing
groups become explicit empty statistics. If any group is nonempty, one
additional `system.parts` aggregate provides a common proportional-storage
sample for the whole page. The operation is therefore zero queries for an
empty page, one for an all-empty page, and two otherwise; it never issues one
query per index.

Every enriched item on one page shares the same measurement instant and
visibility cutoff. A later continuation page is measured independently because
the protobuf response carries measurement time per item rather than a
list-wide immutable statistics snapshot. The signed catalog cursor binds
`include_stats`, so plain and enriched continuations cannot be replayed across
modes. Native batching shares the single-index reader's ten-second deadline
and fail-fast one-slot gate, limits scopes/groups/results to 64, and uses
explicit read, memory, output, thread, and expanded-query-size bounds. The
bounded GORM page is selected before the administrative response permit is
acquired, so a SQLite busy wait cannot occupy response capacity. The permit
then covers defensive page validation and materialization and is released
before native work begins.

`INDEX_SORT_BY_EVENT_COUNT` and `INDEX_SORT_BY_STORAGE_BYTES` remain rejected.
Correct global statistics ordering would have to measure every filtered
catalog candidate and preserve that ordering across continuation pages despite
ingestion, retention, TTL removal, and changing part metadata. It therefore
requires an immutable bounded native-statistics snapshot in addition to the
now-enforced metadata catalog ceiling, not the page-local enrichment contract.

### Index field catalog

`POST /api/v1/indexes/fields/list` is administrator-only. On every request the
server resolves the supplied stable ID or canonical name through the
GORM/SQLite index catalog before doing any native work. Resolution admits every
current, non-tombstoned catalog record: `ACTIVE`, `ARCHIVED`, and outstanding
`DELETING` records remain inspectable regardless of whether their search access
is enabled or disabled. A completed deletion is hidden by its tombstone and
returns `404 Not Found`. The resolved ID, canonical name, and current version,
not browser-provided physical scope, bind the analysis and its cursor.

`time_range`, `time_range.earliest`, and `time_range.latest` are all required.
Bounds may still use the absolute or relative SPL time syntax represented by
`TimeRangeSpec`. The initial request resolves them once to an absolute
half-open interval `[earliest, latest)`, then captures one
`SnapshotAnalysisScope`: the largest committed visibility sequence is captured
before one clock anchor supplies both the search-start and index-time cutoffs.
The server builds an empty-AST raw-event scan with its trusted tenant and the
single resolved canonical index; callers cannot supply SPL or widen that
scope. The one native ClickHouse catalog query applies tenant/index equality,
the resolved event-time interval, `index_time <= snapshot_anchor`,
`expires_at > snapshot_anchor`, and
`visibility_seq <= visibility_cutoff`.

The query buffers and validates the complete bytewise-name-sorted catalog
before returning anything. Each `FieldProfile` has exact presence,
explicit-null, and missing-event counts plus the complete sorted set of
observed durable value types for this snapshot. Known canonical fields remain
represented with zero counts for an empty index. `selected` and `interesting`
use the same deterministic field-profile rules as completed-search catalogs.
This endpoint deliberately leaves `distinct_count` absent and
`distinct_count_is_approximate` false; it neither computes nor estimates
cardinality.

One complete catalog is limited to 10,000 profiles. ClickHouse reads one extra
ordered profile as an overflow sentinel, and the server rejects the whole
analysis instead of returning a truncated catalog. Unsupported legacy or
inconsistent field metadata likewise fails the complete request. Filtering is
an optional case-sensitive UTF-8 substring match over the validated catalog.
Pages default to 100 fields and may request at most 1,000; changing page size
between continuations is allowed. `total_size` is the exact filtered count when
`include_total_size` requests it and is otherwise omitted.

Continuation tokens are authenticated, service-instance-scoped opaque values.
They bind the caller tenant and owner, resolved index ID/name/version, original
time intent, immutable analysis snapshot, name filter, cache generation, and
paging positions. An equivalent ID/name selector may continue the same record,
but an index version change, different filter or time intent, another caller,
server restart, cache expiry, or cache eviction makes the token invalid. All
pages therefore read the exact complete in-memory result created for the first
page even if ingestion, retention, TTL removal, or relative wall-clock time
changes later.

The complete catalog is stored in the bounded field-analysis LRU and
computations for the same exact snapshot key coalesce, so a cache miss performs
one ClickHouse query and continuations perform none. The default cache admits
at most 128 entries and 64 MiB for five minutes. A shared fail-fast
field-analysis gate defaults to four concurrent native computations. The
catalog query is hard-capped at fifteen seconds, 128 MiB memory, five million
source rows, 1 GiB source bytes, two threads, and a 32 MiB result; the protobuf
response has a separate 32 MiB ceiling.

The persistence boundary is deliberate: GORM is used only to resolve the
control-plane index record, while compilation and event reads use the native
ClickHouse driver only. Existing ClickHouse migration `0003` already provides
the field metadata needed by the catalog, and the runtime event-table `SELECT`
grant already covers the scan, so this endpoint adds neither a control-plane
migration nor a ClickHouse migration or grant.

### Search validation

`POST /api/v1/search/validate` accepts the same bounded `SearchDefinition`
intent used to create an ad-hoc search. The server requires nonblank,
NUL-free SPL, an explicit resolvable time range, and at least one normalized
active, search-enabled index which the caller may search. Search presentation
fields are not validation inputs. The server resolves relative times once and
passes the exact detached tenant, authorized index set, requested index set,
and half-open absolute time range through the same parse, plan, authorization,
and ClickHouse compilation path used by search-job creation.

A successfully compiled search returns HTTP 200 with `valid = true`, trimmed
`normalized_spl`, sorted unique effective `referenced_indexes`, sorted unique
logical read `referenced_fields`, and a non-unspecified
`predicted_result_kind`. Referenced fields describe inputs read by the logical
pipeline, not merely final result columns.

An SPL parse, planning, compiler, or in-query index-scope rejection also
returns HTTP 200, but with `valid = false` and one or more error
`diagnostics`. An invalid response contains no normalized SPL, referenced
indexes, referenced fields, or predicted result kind, so a rejected partial
plan cannot be mistaken for accepted analysis. Diagnostic ranges are exact
half-open ranges into the original UTF-8 SPL: byte offsets are zero-based,
while lines and Unicode-scalar columns are one-based. The shared diagnostic
projection keeps validation, retained-job/history, and WebSocket coordinates
identical.

Malformed protobuf or search definitions, invalid or unauthorized requested
index scopes, unresolvable time ranges, oversized requests, deadlines,
capacity exhaustion, unavailable dependencies, and internal failures are
transport failures rather than SPL diagnostics. They use the standard
non-2xx SRouter/go-common error response and do not expose compiler SQL,
arguments, catalog details, or other internal error text.

Validation is synchronous, cancellation-aware, and independently bounded by
the search manager's validation concurrency gate; a full gate fails fast
instead of queuing. Existing request-byte, SPL-token, scope-count,
expression-depth, logical-analysis node/depth, and compiler-work limits still
apply. Validation creates no ID or search job, changes no queue or retained
metadata, writes no history or journal record, takes no storage visibility
snapshot, and executes no ClickHouse query. Compilation may construct
generated SQL transiently to prove backend support, but neither SQL nor bound
arguments are retained, exposed, or submitted to ClickHouse.

### Administrator search inspection

`POST /api/v1/search/jobs/inspect` is an administrator-only diagnostic route
for one retained, completed search job. Its request contains only
`search_job_id`; tenant and owner scope come exclusively from the authenticated
browser principal and cannot be selected on the wire.

The response contains a bounded logical projection (ordered stages, safe field
names, authored-stage source ranges, and final output shape), a bounded physical
projection (allowlisted ClickHouse node types, read columns, index names and
keys, and initial/selected part and granule counts), the generated SQL, raw
structured `EXPLAIN PLAN` text, the diagnostic ClickHouse query ID, and the
optional bounded knowledge summary.
`OPEN`, `STATIC`, and `DYNAMIC` output kinds distinguish an unknown schema, a
complete ordered schema, and a fixed prefix with a maximum number of dynamic
fields.

Generated knowledge stages omit `source_range`. Their additive
`operator_provenance` entries are canonical-ordinal ordered redacted
ordinal/type/stage values, and `output_provenance` associates each output
occurrence with one such ordinal in output-field/ordinal order. Distinct
selector-disjoint objects may therefore target the same output without
collapsing provenance. These fields never disclose object identity or
definition content.

Generated SQL and raw EXPLAIN text are administrator-sensitive and are not
returned by ordinary search-job reads. Compiler arguments are never separate
response fields and the generated SQL remains parameterized, but ClickHouse
may render any query-bound tenant, index, or predicate value into the raw
EXPLAIN text. Treat the entire inspection response as privileged diagnostic
data. Execution rows, owner IDs, and mutable planner state are never part of
this contract. The server validates and bounds every projected collection and
string before serialization.

For an enabled snapshot, inspection uses the retained manager-sealed compiled
authority. It performs a second detached execution-snapshot read after
`EXPLAIN` and requires exact sealed equality; this metadata-only path consumes
no result lease. The browser projection redacts every object identity in the
returned summary.

Export admission atomically obtains the completed execution authority and its
exact manager-owned result-generation pin. The pin is held for the full
re-execution and export lifetime. An enabled export uses the retained compiled
query and snapshot summary; it cannot substitute a current catalog snapshot or
a constructed, cross-manager, or mismatched result lease. Export responses use
the same redacted bounded summary projection.

Export-job listing is owner-and-tenant scoped and ordered by immutable
`created_at DESC, export_job_id DESC`. Pages default to and are capped at 15
jobs. The opaque continuation token is authenticated and manager-instance
scoped; it binds the caller, canonical state and exact search-job filters,
fixed ordering, keyset boundary, and the first request's admission high-water
mark. Jobs admitted later cannot enter that traversal, even if they sort after
its current boundary. A removed boundary job does not prevent continuation.

Lifecycle state remains live between calls, and a due job is expired before
state filtering. Consequently, a job may disappear from or enter a state
filter during one traversal. An exact total is the count of currently retained
matching jobs at or below that traversal's high-water for the individual
request that computes it; it is not a frozen snapshot total. Page size and
whether to compute that total may change between continuation calls, while
changing a state or search-job filter invalidates the token. List responses are
detached bounded metadata snapshots and never contain or issue download
capabilities.

The export download route is a raw `GET` file response rather than protobuf. Its short-lived path and bearer capability are returned only in `ExportDownloadGrant`; the token is sent in the `Authorization` header and never placed in a query string.

The `client_request_id` fields reserve the wire contract for future durable retry handling. The current server does not support them for search jobs, exports, saved searches, indexes, ingestion tokens, or apps: supplying the field, including an explicitly empty value, fails request validation. Create requests without the field are not deduplicated, and the server does not currently advertise an idempotency-retention window. When support is added, a key will be scoped to the authenticated caller and operation, and reuse for a different canonical request will be a conflict.

Ingestion-token metadata is a bounded operational catalog, not permanent audit
history. The hard catalog ceiling is 1,024 physical token records and 16,384
total token-to-index scope memberships. Create returns HTTP `429` only when
normal retention compaction and reclaiming revoked tombstones cannot make
enough record and scope room; the response contains no token material. Expired
credentials remain visible and consume capacity until an administrator
explicitly revokes them. Ordinary revocation always preserves the
just-revoked current tombstone and retains the newest prior tombstones, ordered
by revocation time and token ID, within the configured bound. A later create
may reclaim even the last revoked tombstone for capacity. A physically pruned
token returns `404` from get, is absent from list totals and filters, and
invalidates a list cursor whose snapshot included it. Its former plaintext
credential remains indistinguishable from every other unauthorized credential
at the collector boundary.

`IngestionRateLimits` is shared by `IndexDefinition` and
`IngestionTokenDefinition`; returned `Index` and `IngestionToken` records expose
the same stored values. Its optional `max_events_per_second` and
`max_uncompressed_bytes_per_second` fields use zero or absence to mean
unlimited. Index and token update masks accept the whole
`ingestion_rate_limits` message or either leaf, with the usual optional-field
clear semantics. The hard ceilings, accepted-event charging, durable virtual
schedule, exact-retry precedence, and collector backpressure behavior are
normative in [Ingestion rate limits v0.1](ingestion-rate-limits-v0.1.md).

A legacy or app-less search creation creates a job record even for parse or
planning failures; that job transitions to `FAILED` and appears in history. A
configured nonempty-app knowledge admission instead parses, plans, resolves,
compiles, and finalizes before an ID exists, so any failure creates no job or
history/audit row. An ordinary or saved-search create supplies `definition`. A
`SEARCH_JOB_ORIGIN_HISTORY_RERUN` create instead supplies only
`source.history_search_id`: `definition` is forbidden, and the server
reconstructs trusted reusable intent from the caller's owner-scoped retained
history row, reauthorizes its current app/index scope, resolves relative time
again, and records the immediate source history ID on the fresh job. The
retained knowledge summary is provenance only. When knowledge admission is
configured, the fresh job resolves the current catalog and receives a new
snapshot; it never reuses the historical snapshot or compiled authority. If
current-clock resolution makes a retained mixed relative/absolute range
non-executable, the server returns `409 Conflict` without admitting a job.
Search and export cancellation are idempotent, and an already-terminal job is
returned unchanged.

Result cursors are scoped to one immutable search snapshot and one column selection. A page token must not be reused with another job or changed request parameters. Rows contain exactly one cell per schema column; a nonexistent field uses `MISSING_VALUE_MISSING`, while an explicitly present null uses `NULL_VALUE_NULL`.

## Collector stream

The native service is `open_splunk.v1.CollectorIngestService/Collect`, a bidirectional gRPC stream.

1. The collector opens the stream using TLS by default and sends `authorization: Bearer <token>` in gRPC metadata.
2. Its first frame has connection sequence `1` and a `CollectorHello`. Credentials never appear in protobuf payloads.
3. The server authenticates the token, negotiates protocol limits, and returns `CollectorReady`. Authentication/protocol failures that invalidate the stream use canonical gRPC status codes.
4. The collector sends durable `EventBatch` records and periodic `CollectorHeartbeat` frames while respecting negotiated in-flight and byte/event limits.
5. `BatchAck` and `BatchReject` are terminal dispositions. An ack may contain permanent per-event rejections; accepted, duplicate, and rejected counts must sum to the original event count. Rejected events go to the collector dead-letter output while the durable batch advances. `RetryBatch` is non-terminal and requires replay of the unchanged batch. `Throttle` applies to future sending without acknowledging anything. A quota denial sends a `RATE_LIMITED` retry for the current batch followed by a separately sequenced `TOKEN_QUOTA` or `INDEX_QUOTA` throttle for later sends; the throttle's zero limit overrides preserve the negotiated batch limits.
6. On disconnect, the collector reconnects, sends its last contiguous acknowledged sequence, and replays every unacknowledged batch with the same batch ID, sequence, event IDs, and event-ID digest.

The control plane retains at most 256 durable collector identities per tenant,
independent of the 16-collector process-liveness ceiling. At capacity, an
authenticated Hello for a previously unseen bound `collector_id` fails with
gRPC `RESOURCE_EXHAUSTED` and records neither token use nor partial fleet
state. Existing enabled identities may reconnect at capacity; disabled
identities continue to fail as disabled rather than being misreported as a
capacity failure.

An acknowledgment means the server reached `ACK_DURABILITY_CLICKHOUSE_COMMITTED`. It does not promise global exactly-once delivery. Stable event and batch IDs make retries idempotent for the most recent 10,000 committed server visibility sequences, matching the exclusively managed ClickHouse events table's 10,000-block deduplication window. The server pauses newer sends behind an ambiguous insert until its durable outbox is reconciled, so that insert cannot age out of the window. Replaying a terminal batch older than this explicit horizon is at-least-once and may create duplicates.

## Search WebSocket

`GET /api/v1/search/ws` upgrades to a WebSocket. Every application message is one binary protobuf frame: browsers send `SearchWebSocketCommand` and decode `SearchWebSocketEvent`.

Sequences are per search/export target, not per TCP connection. A reconnecting client subscribes with its last processed sequence. The server replays newer retained events or sends `ResynchronizationRequired`; the client then calls the authoritative SRouter `get`/results endpoints. Preview rows are bounded and disposable, and may be reset or dropped without affecting durable result pages.

## Evolution rules

- Never renumber a field or reuse a deleted field/enum number; reserve both old numbers and names.
- Every enum starts with `UNSPECIFIED = 0`; handlers reject unspecified values where the operation requires a choice.
- Opaque IDs, cursors, download grants, and idempotency keys are strings. Clients must not parse their contents.
- Mutable SQLite entities expose `version`; updates and deletes require `expected_version` and fail on stale writes.
- `TimeRangeSpec` preserves relative expressions, while `ResolvedTimeRange` records the half-open absolute interval used by one execution.
- `TypedValue` preserves signed/unsigned integers, decimals, bytes, timestamps, lists, objects, explicit null, and an explicit missing-field marker. Do not replace it with `google.protobuf.Struct`, which represents every number as a double.
- Job results always include `ResultSchema`; each row's cells correspond positionally to its columns.
- Plaintext ingestion tokens and export download grants are one-time/short-lived secrets and must never be logged.

## Generation

From the repository root:

```sh
make proto-tools  # once per development environment
make proto-lint
make proto
```

Generated Go protobuf and gRPC files are written to `gen/go/open_splunk/v1`. Generated `ts-proto` messages and index files are written to `gen/ts`. Generated files are full-rebuild artifacts and must not be edited manually.

TypeScript `oneof` fields are generated as discriminated unions (`oneof=unions-value`). Frontend code must switch on `$case`; it cannot construct multiple payload variants simultaneously.
