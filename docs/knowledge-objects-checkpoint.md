# Knowledge objects implementation checkpoint

**Goal status:** active

**Current milestone:** KO-1 search-time runtime acceptance (closed gates)

**Last completed slice:** shared bounded candidate request decoding, an
unregistered request-only Preview codec, and a structural Preview envelope
validator plus wire-neutral protobuf contract hardening which pins its canonical
six-field authority, exact bounds, forced-ACTIVE view, and full optional uint32
row-scalar presence without assigning row policy; Preview service, response
codec, handler, route, browser/runtime, and every nonempty execution gate remain
closed

**Evidence date:** August 9, 2026

## Durable checkpoint

- Published KO-0 foundation: inclusive `00c88c1` through `b7ac77b`
- KO-0D local implementation and checkpoint range: `c5440b9..e9af86d`
- KO-0E local implementation range: `e9af86d..a8000e7`
- KO-0F local implementation range: `e78d60b..d6e767d`
- KO-0G local implementation range: `6005b24..4aaacc7`
- KO-0G implementation terminal revision:
  `4aaacc74a2724be28cb0b960646014d8a678f21a`
- KO-0H local implementation range: `441fd4d..bcf095b`
- KO-0H implementation terminal revision:
  `bcf095bb2db19a1b8d9b1810bfd1eaed32bf11ba`
- KO-1A selector-contract revision:
  `03a0b3e991424ae41716a8cb36749cb3ad8aff5b`
- KO-1B immutable-prelude revision:
  `3278018fd5bd989b630e3c722f658177a6192c42`
- KO-1C closed-gate retained-prelude revision:
  `1a30afc9cbbac466e698bf19199ebdcc8927ff4d`
- Internal validation-service terminal revision:
  `7d54c01172f74b097d05d09df4652c28c94a29c0`
- Validation-route terminal revision:
  `2aa51384ead0be6482a1e9ea3ce85fbeed1777f9`
- Preview request-boundary terminal revision:
  `74df953f6f317b651f7bde97c0370fc37822a756`
- Branch: `codex/knowledge-objects-runtime`
- Publication state before this document: 129 intentional post-`c5440b9` KO
  commits through `74df953` are durable locally. This update is prepared as a
  separate documentation change. `origin/main` remains `c5440b9`; the local
  remote-tracking feature branch ends at `7503246`, so all later work remains
  local. No further push was attempted without explicit destination approval.
- Scope: lifecycle- and commitment-aware protobuf contracts, canonical known
  and inactive-future definitions, migrations 0029–0034 state/commit/tenant
  authorities, a bounded read-only `Get`/`List` catalog, and an atomic Writer
  for DRAFT or recognized ACTIVE create, DRAFT/DISABLED or recognized ACTIVE
  definition update, DRAFT/DISABLED-to-ACTIVE enable, DRAFT/ACTIVE disable, and
  DRAFT/ACTIVE/DISABLED delete with exact replay,
  optimistic versions, immutable bodies/versions/dependencies/projections,
  successful audit, revision/state-token rotation, bounded health/reclamation,
  and crash-safe recovery. KO-0F adds six administrator-only protobuf handler
  implementations, bounded codecs, trusted app-scope derivation, detached
  request/response authority, and synchronous rejected-attempt journaling with
  exact known-committed/indeterminate suppression. KO-0G adds exact
  revision-zero authority for app-provisioned tenants with an empty knowledge
  catalog, a bounded one-read-transaction active resolver, and an opaque
  detached snapshot authority with canonical object, shadow, dependency,
  static-charge, and cross-language wire/digest contracts. KO-0H adds migration
  0032, bounded snapshot reference/summary contracts, optional synchronous
  nonempty-app admission, compiler- and manager-private execution seals,
  durable history and attempt-audit provenance, retained inspection/export
  authority, and a hidden read-only Knowledge Manager shell with bounded
  browser transport. KO-1A freezes one detached exact-literal/combined-RE2
  selector program and deterministic conservative cross-engine runtime charge,
  while keeping catalog index-pruning probes separate from event-execution
  accounting. KO-1B adds the cycle-neutral immutable Tier-1 field program,
  explicit plan-native extraction/JSON/alias/calculated operators, private
  object and output provenance, exact dependency/collision revalidation,
  aggregate semantic ceilings, and prefix-integrity validation for analysis.
  KO-1C now stages the exact selector and Tier-1 field operators in ClickHouse,
  composes one frozen prelude before authored SPL, carries container metadata
  through conditional and authored identity stages, reconstructs hidden
  container result sidecars, enforces atomic selector/capture/alias-copy
  runtime guards, preserves an exact authored/knowledge compiler-evidence
  split, and re-injects the manager-sealed retained program for postflight
  inspection and completed-search analysis.
- ACTIVE publication preparation now includes the backend-neutral dependency
  compiler, the complete-winner-cohort adapter, candidate-present and
  candidate-absent post-transition validation, paired multi-index OR closure,
  the pure tenant transition authority, and migration 0033. The transition
  derives pre/post winner cohorts across tagged generic-app, generic-principal,
  private-principal, and multi-index scopes; it binds exact endpoints and one
  cross-cohort candidate graph under independent hydration, matcher,
  selector-revisit, semantic-work, and other revisit ceilings plus fixed-digest
  transcripts bounded by the membership ceiling. Its body-free persistence
  matcher binds tenant, scalar endpoints, retained rows, and the database
  projection.
  Index closure retains exact minimum witnesses under separate atom, state,
  and 65,536-probe work ceilings. The
  migration permits enable to persist a newly derived graph, preserves exact
  disable/delete graph identity, rejects stale ACTIVE pins during upgrade, and
  blocks target version advancement while a current ACTIVE dependent retains
  an old pin. A future atomic cascade disables dependents state-only, advances
  the target, and re-enables them with derived edges. Migration 0034 and the
  same-transaction global index validator close the future-index prerequisite.
  Recognized definitions now use the complete authority for ACTIVE create,
  ACTIVE update, enable, disable, and delete; opaque ACTIVE update and enable
  remain closed.
- The transaction boundary now requires the existing Writer
  `BEGIN IMMEDIATE` snapshot, proves exact knowledge/app/index catalog facts,
  preflights all aggregate object resources before hydration, and validates rich
  derived targets against their current ACTIVE registry/version before
  returning a detached database projection. An opaque authority binds those
  facts and the exact `*sql.Tx` to the pure transition proof. Before its first
  persistence write or hook, `publishMutation` reconstructs the live before
  endpoint, rechecks all revision domains, rejects caller-supplied ACTIVE
  dependencies, and uses only a fresh private projection for the version
  count, dependency rows, and seal. A zero authority cannot publish an ACTIVE
  endpoint. Recognized ACTIVE disable/delete now mint the proof from the exact
  live object and retained graph, while the sole zero-proof emergency removal
  path re-decodes and exact-matches a genuinely opaque stored future body and
  its dependencies before any write or hook. New index-name creation now
  revalidates every affected ACTIVE tenant in the index transaction before its
  first write.
- Runtime feature state: the capability remains hard-disabled and unadvertised.
  Production registers exactly the nine knowledge-management routes as one
  complete administrator-only unit and composes their Store, concrete ready
  Writer, app authority, and attempt journal. The management runtime also
  constructs and retains a concrete Resolver, but intentionally does not attach
  it to production `searchjobs.Manager`. The compiler and snapshot finalizer
  retain independent nonempty gates, so ClickHouse knowledge execution remains
  unavailable. The dependency/dependent and Validate routes are registered but
  capability-unadvertised and represented in the central route manifest. The
  graph routes join Get/List as the only knowledge paths in the browser
  administrator-bearer allowlist; Validate and every mutation remain excluded.
  Validate is absent from the backend's generic outer `administratorRoutes` map
  because the inner knowledge-attempt boundary owns its administrator
  authentication and `ActionValidate` journaling before decode. The dormant
  Preview request codec and structural forced-ACTIVE envelope validator are
  internal only; neither is installed in this route unit, manifest, or bearer
  policy, and no Preview response codec or service authority exists. The dormant
  read-only Knowledge Manager detail consumes both graph routes for the selected
  exact object version, independently pages and labels each direction's catalog
  revision,
  and displays only visible opposite ID/version/`FIELD_INPUT` rows. Capability-
  gated navigation and dynamic loading remain absent, so production bootstrap
  still makes no knowledge request.
- Candidate normalization now has a detached, fail-fast typed `Issue` seam for
  invalid definitions, recursive unknown fields, and definition resource
  limits. It preserves legacy error text and `errors.Is` roots, exposes at most
  one definition-relative issue, and deliberately excludes infrastructure,
  invariant, canonical-storage, and other non-candidate failures. No HTTP
  boundary maps the raw seam directly; Writer result construction consumes only
  its closed typed projection.
- Semantic `Compile` now has a separate detached, fail-fast issue seam for an
  exact candidate input index's intrinsic regex, JSON-path, calculated `SPL_*`,
  and Boolean-result failures. An adapter must prove the submitted candidate
  and use singleton/index-zero attribution; `Prepare`, winner-cohort,
  authority, aggregate, collision/selector, and dependency failures remain
  opaque to this typed compiler-issue seam. Legacy error/sentinel behavior is
  preserved; the registered handler never maps the raw seam directly, and the
  result layer consumes only its closed typed projection.
- The Validate wire contract pins two explicit intents, `INACTIVE_STORAGE` and
  `ACTIVE_PUBLICATION`, while Preview accepts no intent and is forced active. The
  combined contracts pin presence-sensitive create/update envelopes,
  update versions through MaxInt64, candidate-only in-band invalidity, exact
  successful normalized/digest/resource shape, inactive structural-only versus
  active singleton intrinsic charges (including extraction outputs, JSON work,
  and scalar predicates), full-transition authorized candidate dependencies,
  deterministic bounded located diagnostics and violations, UTF-8 source
  coordinates, privacy, truncation, recursive unknown-output rejection, and an
  8 MiB response maximum. ACTIVE create uses a fresh deterministic
  non-persisted evaluation ID, reserves no identity, and requires validity,
  diagnostics, resources, and target dependencies to be invariant under every
  fresh-ID rename; a later Create allocates and revalidates its own ID. A valid
  result is advisory definition validity, not mutation acceptability: no-op
  updates may be valid and inactive validation of an ACTIVE object is only
  hypothetical. The fixed transaction also reads app/index authority, while
  `tenant_catalog_revision` names only its knowledge-ledger component (zero
  proves an empty ledger) and is no reservation, mutation proof, or reusable
  authority. Preview always uses `ACTIVE_PUBLICATION`. Validate is registered
  but unadvertised; Preview remains unregistered and unadvertised. Draft result
  tags 6/7 and resource tag/name 11 were retired before Validate registration,
  were never served by either route, and remain reserved under an intentional
  historical FILE compatibility waiver; the change is not claimed
  schema-nonbreaking.
- `internal/knowledgevalidation` now implements that result shape as a pure,
  context-aware layer with no database, catalog reader, transition engine,
  authorization, router, or HTTP policy. Inactive construction normalizes only;
  active preparation singleton-compiles into one opaque terminal invalid result
  or one opaque candidate. Only exact typed normalization/compiler issues map
  in-band through preparation; the sole transition/dependency exception is the
  caller-selected, target-free `BuildDependencyUnavailable` generic diagnostic.
  Ranged diagnostics are rebound to the detached submitted scalar and retain
  private field-path/source provenance. A full transition must supply
  the complete already-authorized target projection; the layer validates its
  shape but cannot prove completeness or visibility. Result projection and the
  8 MiB deterministic response seal revalidate kind, digest, exact intrinsic
  and dependency resources, issue/range provenance, recursive unknown-field
  absence, and MaxInt64 revision bounds. That pure result layer itself owns no
  route and does not change the capability, browser, resolver, or nonempty
  execution gates.
- The concrete catalog Writer exposes the internal `Validate` service now
  consumed by the registered HTTP adapter. `ValidationScope` deliberately
  splits `ReadScope`, which
  controls dependency disclosure, from `WriteScope`, which controls candidate
  object/app admission; both must carry the same authenticated tenant and
  owner, while their app sets remain independent. A shared per-control-database
  one-slot gate fails fast before the rollback-only transaction. Once admitted,
  create treats the full definition as selected and update constructs only a
  shallow view of mask-selected top-level fields. Selected selector/output
  over-cardinality is detected without traversing or cloning the repeated
  payload and is represented by a bounded `maximum+1` witness so normalizer
  ordering still produces the typed in-band resource issue. Otherwise only the
  bounded selected request view is sized and detached; unselected update data
  cannot consume byte or clone authority.
- Every admitted evaluation uses one fixed `BEGIN IMMEDIATE` knowledge/app/index
  snapshot and always rolls it back before sealing. Update root authorization,
  expected version, lifecycle, current storage integrity, and rejection of an
  opaque current definition precede candidate issue construction. After a root
  or create app is authorized, only its context may be attached to an error;
  dependency targets never become authorization context, and every error
  defaults to definitive rejection. `INACTIVE_STORAGE`
  performs structural normalization, authorizes a valid applied app, and
  bookends the knowledge revision without publication compilation or inventory.
  `ACTIVE_PUBLICATION` singleton-compiles and then uses the complete ACTIVE
  transition inventory. Its validation mode compiles every affected
  candidate-absent baseline before classifying candidate conflicts, so stored
  baseline failures remain out-of-band. Only a cohort-local missing target or a
  target hidden by `ReadScope` becomes the target-free generic dependency
  diagnostic; other conflicts, target-integrity failures, and infrastructure
  faults stay out-of-band service errors.
- ACTIVE create chooses the first deterministic
  `knowledge-validation-candidate-%04x` identity absent from the complete
  tenant inventory after reconciling the identity ledger and physical row
  count. It never calls the mutation ID generator and neither reserves nor
  returns that identity. Both intents return the exact same-transaction
  knowledge revision; revision zero additionally proves the physical object
  ledger empty, and all revision paths use matching bookends. A rollback error
  invalidates the evaluation, and only after successful rollback does the
  existing deterministic 8 MiB response seal run. Validation executes no DML,
  audit append, idempotency path, publication hook, clock, mutation ID
  generator, or commit. The dedicated handler and codec now consume this service
  through the ninth all-or-none route, but no browser path or bootstrap
  capability changed.
- Validate and the unregistered request-only Preview codec now share an
  extracted layout-parameterized candidate raw-wire decoder. Both enforce the
  4 MiB-plus-64-KiB (`4259840`-byte) request ceiling, read at most one byte
  beyond it solely as an overflow witness, and use bounded two-pass projection
  instead of generic protobuf materialization. Correct-wire object-ID presence
  selects update projection; duplicate messages and masks merge, scalars are
  last-wins, and empty/zero optional presence is retained. The decoder keeps at
  most 9 mask paths, 17 entries in each selected selector dimension, and 17
  selected regex outputs, validates every recognized string occurrence as
  UTF-8 even when unselected, overwritten, or cleared, and rejects malformed
  wire or unknown-group depth above 32. Outer unknowns, including wrong-wire
  envelope fields, and mask unknowns remain envelope errors; the full create
  candidate and update mask-selected nested unknowns remain candidate authority;
  update candidate top-level and unselected nested unknowns are discarded.
  Million-entry mask, selected/unselected repetition, job-ID, extraction-output,
  and alternating-body tests prove bounded retention and allocation behavior.
- Preview's canonical request authority is `retained_search_job_id = 1`,
  `definition = 2`, optional `knowledge_object_id = 3`, optional
  `expected_version = 4`, `update_mask = 5`, and optional uint32
  `maximum_rows = 6`, with no independent intent. The ID names future
  owner-scoped retained execution authority that a service must reacquire under
  the authenticated caller; it is not an immutable-event-snapshot identity and
  grants no access by itself. The codec retains the last UTF-8 ID through the
  256-byte ceiling or a detached 257-byte over-limit witness while validating
  every occurrence. The validator requires a nonempty valid-UTF-8 ID unchanged
  by whitespace trimming and free of Unicode control code points, rejects nil
  and outer/mask unknown authority, and passes the exact Validate create/update view
  with `ACTIVE_PUBLICATION` forced by the server. It performs no retained-job
  lookup or authorization and never mutates or normalizes the decoded request.
  `maximum_rows` preserves absence, explicit zero, and every exact uint32 value
  through `4294967295`; this boundary assigns no default, bound, or execution
  meaning. Go/TypeScript contract oracles pin tags and presence, the Go
  structural and TypeScript wire oracles preserve MaxUint32, and no field
  number, type, or presence encoding changed. No Preview response codec,
  handler, catalog/search service, retained-execution acquisition, caller-auth
  integration, route, manifest/bearer entry, capability, browser surface,
  Resolver attachment, or execution path exists.
- The HTTP adapter requires the exact ready concrete Writer, acquires response-
  serialization capacity before retained request authority, detaches binding
  fields, independently clones read/write scope, and admits only the closed
  definitive service-error taxonomy, including only the exact
  `control.ErrCapacityExceeded` plus `knowledgevalidation.ErrResponseTooLarge`
  join and no other joined authority. The response codec revalidates the seal
  under the live request context and writes its exact deterministic bytes at or
  below 8 MiB without a fresh mutable protobuf marshal. The route enters the
  authenticated attempt boundary as `ActionValidate`; every definitive
  rejection follows the single synchronous append rule, while HTTP 200 in-band
  results append no rejected-attempt row.

## KO-0 durable commits

The following intentional commits are present on `main` and `origin/main`:

| Commit | Subject | Scope |
| --- | --- | --- |
| `8a25098` | `feat(knowledge): reserve KO-0 protobuf contracts` | Typed Tier-1 definitions, CRUD/validation/dependency/preview messages, immutable snapshot/provenance wire contracts, generated Go/TypeScript, and a hard-disabled capability guard |
| `c23889b` | `feat(knowledge): add bounded selector primitives` | Stable text/destination normalization, 17-segment search-field parsing, canonical selectors, combined NFA matching, cumulative resource accounting, cancellation, race tests, and four fuzz targets |
| `99f42d5` | `feat(knowledge): add immutable catalog schema` | Forward-only SQLite migration 0024, content-addressed bodies, exact registry/version agreement, dependency seals, capacity reserves/counters, idempotency, quarantine recovery audit, app/dependent guards, rollback and migration tests |
| `b42b046` | `docs(knowledge): checkpoint KO-0B foundations` | Durable KO-0B review, test, and delivery evidence |
| `1539136` | `docs(knowledge): pin catalog runtime semantics` | Binary substring and selector-filter semantics, exact projection/selector accounting, historical authorization and quarantine non-disclosure, and exact rejected-attempt retention/privacy contracts |
| `f9eb14a` | `feat(knowledge): add bounded list projections` | Migration 0025, exact sealed current-version projections, canonical selector framing, 256 MiB tenant accounting, pre-LIMIT filtering, lifecycle/rollback guards, and adversarial migration tests |
| `fc34902` | `feat(knowledge): extend successful mutation audit` | Migration 0026, closed knowledge mutation taxonomy, optional all-or-none knowledge metadata, legacy cursor compatibility, server projection, and migration/corruption/race tests |
| `b7ac77b` | `feat(knowledge): journal rejected privileged attempts` | Migration 0027 and a fail-closed, payload-free, privacy-shaped, 100,000-row rolling journal with monotonic sequences, owned transactions, startup integrity checks, concurrency tests, and recursive-trigger-safe eviction |

The published post-KO-0C prerequisites used as KO-0D's baseline are:

| Commit | Subject | Scope |
| --- | --- | --- |
| `83bfb56` | `feat(knowledge): represent deleted catalog tombstones` | Deleted-state immutable catalog representation and compatibility groundwork |
| `076d4b8` | `feat(knowledge): canonicalize stored definitions` | Canonical stored known-definition bodies and exact compatibility validation |
| `c5440b9` | `feat(knowledge): audit rejected catalog reads` | Migration 0028 and rejected catalog-read audit foundations; exact KO-0D base on `main` and `origin/main` |

KO-0D adds the following dependency-ordered local commits:

| Commit | Subject | Scope |
| --- | --- | --- |
| `7fd1270` | `feat(knowledge): harden catalog definition contracts` | Lifecycle and snapshot-token protobuf additions, inactive future-body preservation, canonical/detached definition decoding, exact forward-compatibility tests, and the bounded standalone calculated-expression parser |
| `dbee1cf` | `feat(control): add catalog state authority migration` | Migration 0029 revision commitments, lifecycle authorities, list order keys, transition and upgrade guards, linear disabled-history backfill, exact registry/version agreement, rollback, backup, and adversarial migration tests |
| `be0c965` | `feat(knowledge): add bounded catalog reader` | Authorization-first read-only `Get`/`List`, current-policy historical access, quarantine redaction, revision-bound keyset cursors, bounded hydration, lifecycle chronology, dependency graph semantics, and detached return values |
| `af7e9b3` | `test(knowledge): add catalog property and fuzz suites` | Unit, reference-model, property, boundary, and fuzz coverage for requests, cursors, definitions, dependency semantics, pagination budgets, and version/revision behavior |
| `d258a0b` | `test(knowledge): add adversarial catalog integration matrix` | SQLite integration coverage for authorization, non-disclosure, forward compatibility, lifecycle/history, revision commitments, coherent WAL snapshots, backup, keyset traversal, bounded work, corruption, cancellation, and hostile physical rows |
| `c8b9757` | `ci(knowledge): run dedicated fuzz shards` | Four fail-fast-independent knowledge-object fuzz shards covering all 15 declared targets, exact inventory enforcement, failure artifacts, and an explicit 20-minute package timeout for the race/coverage gate |

The KO-0D checkpoint itself is durable as `e9af86d` (`docs(knowledge):
checkpoint KO-0D catalog reader`). KO-0E then adds these dependency-ordered
local commits:

| Commit | Subject | Scope |
| --- | --- | --- |
| `f9df615` | `proto: bind knowledge mutation snapshot outcomes` | Additive CRUD response revision/state-token pairs plus compact internal mutation outcome authority and exact cross-language wire contracts |
| `a70edd8` | `proto: bind knowledge mutation retention authority` | Canonical outcome occurrence, retention-anchor, and retain-until authorities with deterministic Go/TypeScript encoding |
| `457245b` | `feat(control): add writer commit authority migration` | Migration 0030 version-semantic guards, bounded active-dependent plan, immutable commit authority, exact receipt composite binding, audit/recovery linkage, retention and capacity invariants, legacy-row refusal, and migration/backup/long-history tests |
| `d2704dd` | `feat(knowledge): add atomic catalog writer` | Trusted-scope Writer for draft Create, inactive Update, disable SetState, and Delete; detached request authority, exact idempotency replay, bounded publication/health/reclamation, successful audit, commit reconciliation, forward-compatible state-only preservation, and transaction batching |
| `9195313` | `test(knowledge): harden writer publication and recovery` | Public black-box, concurrency, state-machine, capacity, migration, query-plan, corruption, forward-compatibility, real SIGKILL/reopen, ambiguous-commit, panic rollback, batching, and fuzz coverage |
| `a8000e7` | `ci(knowledge): fuzz and budget writer verification` | Four exact Writer fuzz targets and a measured 30-minute package timeout for the enlarged race/coverage suite; no test or assertion was removed to meet the budget |

The KO-0E checkpoint is durable as `e78d60b` (`docs(knowledge): checkpoint
KO-0E catalog writer`). KO-0F then adds these dependency-ordered local commits:

| Commit | Subject | Scope |
| --- | --- | --- |
| `049f749` | `feat(knowledge): expose route-safe catalog outcomes` | Detached authorized rejection context, definitive/known-committed/indeterminate error disposition, exact receipt-digest replay classification, public non-mutating mutation/read/List preflights, canonical List normalization, response conversion, and update-result binding |
| `51b7ff4` | `feat(server): stage knowledge management boundary` | Six unregistered administrator handlers, protobuf codecs, authentication/authorization split, trusted app-scope derivation, bounded response authority, synchronous rejected-attempt journaling, stable error redaction, request detachment, and recognized canonical ACTIVE replay preservation for the concrete catalog Writer |
| `d6e767d` | `test(server): harden knowledge management boundary` | Public-404/capability-negative contracts, real SQLite Writer/Store/audit integration, all-six HTTP matrices, attempt-journal failures, authorization/error nondisclosure, request/response detachment, List coherence, definition authority, resource ceilings, and focused race coverage |

The KO-0F checkpoint is durable as `6005b24` (`docs(knowledge): checkpoint
KO-0F route boundary`). KO-0G then adds these dependency-ordered local commits:

| Commit | Subject | Scope |
| --- | --- | --- |
| `37e6bfc` | `fix(proto): align knowledge extraction wire order` | Go/TypeScript deterministic field-extraction wire agreement, descriptor ordering guard, and a shared cross-language extraction fixture |
| `3a10a3d` | `feat(control): provision empty knowledge catalog authority` | Migration 0031 backfills and atomically provisions revision-zero tenant/head/token/projection authority from the app catalog, with immutable monotonic app-catalog guards, backup preservation, corruption refusal, and rollback tests |
| `301b697` | `fix(knowledge): order future metadata before opaque bodies` | Forward-compatible definition metadata/body declaration order, canonical inactive-future decoding, generated Go/TypeScript agreement, and adversarial/fuzz fixtures |
| `d4ca9b6` | `feat(knowledge): prepare immutable search snapshots` | Opaque unfinalized snapshot authority, exact winner/shadow/dependency derivation, selector implication/disjointness, parallel-stage collision checks, static semantic charges, canonical framing, and a shared Go/TypeScript wire/digest golden |
| `7503246` | `feat(knowledge): resolve active catalog snapshots` | One-read-transaction authorization-leading resolver, index pruning and private/app/global precedence, exact winner closure, bounded hydration/retries/admission, old-or-new WAL authority, EXPLAIN/query-count evidence, and detached snapshot handoff |
| `0144be2` | `test(server): preserve empty knowledge authority` | Real HTTP audit-failure rollback oracle proving the provisioned revision-zero authority and token remain exact while every Writer mutation table rolls back |
| `4aaacc7` | `test(knowledge): make async oracles causal` | Causal real-journal finalization and paused single-transaction resolver overlap oracles that remain deterministic under the full race suite without weakening production deadlines |

KO-0H then adds these dependency-ordered local commits:

| Commit | Subject | Scope |
| --- | --- | --- |
| `20a729d` | `feat(search): persist bounded knowledge provenance` | Additive reference/summary protobufs and shared Go/TypeScript wire fixture, migration 0032 compact attempt-audit authority, detached summary validation, and exact pending-to-terminal history retention |
| `a5ad901` | `feat(search): seal knowledge-aware admission` | Optional pre-ID parse/plan/resolve/compile/finalize path, compiler-private whole-query evidence, manager-private execution/result authority, bounded metadata admission, and worker reuse of the prepared execution |
| `de64633` | `feat(search): retain sealed knowledge execution` | Redacted job/list/history/audit projections, retained inspection authority, atomic export result pinning, exact app/scope/result binding, and fail-closed configurable-dependency validation |
| `bcf095b` | `feat(ui): add hidden knowledge manager shell` | Capability-gated lazy read-only list/detail UI, bounded Get/List adapter and response streaming, app filtering, continuation coherence, accessibility states, and exact absent-capability no-load/no-request behavior |

KO-1A then adds the first field-execution prerequisite:

| Commit | Subject | Scope |
| --- | --- | --- |
| `03a0b3e` | `feat(knowledge): freeze cross-engine selector charging` | Detached exact-literal plus single anchored dot-all RE2 programs; compiler-derived transition upper bounds shared by Go and future ClickHouse lowering; fixed-order/missing/null/UTF-8 charging; and independent bounded catalog-pruning probes that cannot misclassify valid high-work scopes as corruption |

KO-1B then adds the backend-neutral Tier-1 field program:

| Commit | Subject | Scope |
| --- | --- | --- |
| `3278018` | `feat(knowledge): add immutable prelude program` | Cycle-neutral immutable typed program; explicit extraction, JSON, fused alias, and fused calculated plan operators; exact dependency and parallel-stage revalidation; aggregate charges; private operation/output provenance and commitment; and analysis-time contiguous-prefix integrity |

Later local commits anchoring the reconciled current state include:

| Commit | Subject | Scope |
| --- | --- | --- |
| `7c6890a` | `feat(knowledge): publish active objects` | Transaction- and compiler-proven recognized ACTIVE create/update/enable plus retained disable/delete authority; opaque ACTIVE update/enable remain closed |
| `5961233` | `feat(control): expose bounded app identities` | Bounded trusted app authority for concrete management composition |
| `5c16a50` | `feat(knowledge): seal management writer readiness` | Exact concrete Writer construction-readiness contract for all-or-none route registration |
| `0bdb516` | `feat(server): register knowledge management routes` | Six production administrator-only management routes registered only as one complete dependency unit, independently of capability advertisement |
| `a3b4cbf` | `feat(server): compose knowledge management runtime` | Shared-control-database Store, Writer, app authority, and attempt-journal production composition |
| `d846dc2` | `feat(knowledge): stage runtime resolver authority` | Concrete Resolver retained by the management runtime without attachment to production search admission; nonempty requests still fail before side effects |
| `597dfb6` | `fix(web): align knowledge route manifest` | Six management routes represented in the central TypeScript route manifest with bounded Get/List responses |
| `0ccd2f5` | `feat(web): filter hidden knowledge manager` | Dormant read-only app/object-type/lifecycle-state filters, four stable sorts, exact continuation reuse, and synchronous query-reset behavior |
| `b8cd2c4` | `feat(proto): define management dependency edges` | Direct exact-version management edge identity without snapshot-global depth/ordinal/digest authority, plus dependency/dependent response roots and a shared Go/TypeScript wire golden |
| `a4e1a0f` | `feat(knowledge): inspect dependency relationships` | Same-WAL direct outgoing and authorization-leading current-source inverse reads, omission-before-page/count privacy, optional exact totals, signed state-bound cursors, and bounded hydration/query plans |
| `7540804` | `feat(knowledge): expose dependency graph reads` | Two bounded graph codecs/handlers, all-or-none eight-route production registration, trusted-scope response validation, rejected-attempt actions, a 128 KiB response ceiling, and unadvertised route-manifest entries |
| `1fd8c12` | `feat(web): inspect knowledge relationships` | Dormant exact-version dependency/dependent consumers, independently validated pagination/revision state, visible opposite-ID/version/role-only presentation, and a read-only four-route knowledge bearer allowlist |
| `8dfa3bc` | `docs(knowledge): checkpoint graph browser inspection` | Reconciled exact-version graph Store/API/browser readiness while preserving absent navigation/chunk/capability and closed nonempty execution gates |
| `0636d9d` | `refactor(knowledge): report definition issues` | Detached fail-fast candidate normalization issue extraction with definition-relative paths and three stable codes, preserving legacy text/`errors.Is` taxonomy while excluding non-candidate failures; no HTTP mapping |
| `0c31de0` | `feat(proto): define knowledge validation contract` | Future unregistered Validate/Preview envelope, intent, result, resource, dependency, located-diagnostic, privacy, deterministic-bound, and 8 MiB response contracts; Preview is always ACTIVE publication and reserved draft fields use an explicit pre-route FILE waiver |
| `392577a` | `fix(proto): bind advisory create validation` | Deterministic transaction-fresh non-persisted ACTIVE-create evaluation identity, fresh-ID alpha-invariance, no ID reservation/authorization, and mandatory later Create allocation plus live authority revalidation |
| `97651f9` | `refactor(knowledge): report semantic issues` | Detached index-bound fail-fast Compile issues for intrinsic regex, JSON-path, calculated SPL, and Boolean-result failures while Prepare, authority, aggregate, cohort, and dependency failures remain opaque to that typed semantic seam; no HTTP mapping |
| `5057a67` | `fix(proto): complete validation resource authority` | Append-only extraction-output/JSON-work/scalar-predicate resource fields, singleton intrinsic compile charges versus full-transition dependency counts, advisory valid/no-op/hypothetical-inactive semantics, and knowledge-revision-only correlation metadata |
| `593ab31` | `docs(knowledge): checkpoint validation contracts` | Reconciled typed issue seams, advisory create/revision validity, complete intrinsic resource fields, intentional pre-route FILE waiver, and still-closed Validate/Preview/runtime gates |
| `350e6d7` | `feat(knowledge): build validation results` | Pure opaque inactive/active result construction, typed-only issue projection, submitted-scalar range provenance, transition-supplied authorized dependencies, exact resource revalidation, and deterministic 8 MiB response sealing without a database or route |
| `af95866` | `docs(knowledge): checkpoint validation results` | Reconciled pure result construction, sealing, privacy, wire bounds, and still-closed service/route/runtime gates |
| `f0748f5` | `feat(knowledge): validate active publication candidates` | Complete ACTIVE candidate validation mode with candidate-absent baseline prepass, opaque conflict decisions, deterministic conflict precedence, target-absence privacy, shared index-admission closure, and fresh-ID alpha-invariance |
| `7d54c01` | `feat(knowledge): validate object candidates` | Internal rollback-only `Writer.Validate`, split read/write authority, one-slot database gate, bounded selected-view/witness admission, inactive revision proof, complete ACTIVE transition and target authorization, deterministic unreserved create identity, strict rollback, and sealed side-effect-free responses without a route |
| `df7044b` | `docs(knowledge): checkpoint validation service` | Reconciled the rollback-only Writer service, selected-view amplification bounds, revision and privacy proofs, verification evidence, and then-still-closed transport/route/runtime gates |
| `1e06e86` | `feat(knowledge): bound validation transport` | Dedicated raw-wire request decoder with bounded projection, semantic unknown split, million-entry allocation oracles, and exact sealed deterministic responses capped at 8 MiB |
| `ceb3b85` | `feat(knowledge): adapt validation service` | Exact-ready-Writer HTTP adapter, detached request binding and cloned scope authority, serialization-permit transfer, closed error/disposition sanitization, and `ActionValidate`-safe rejection context |
| `2aa5138` | `feat(knowledge): expose validation route` | Ninth all-or-none administrator management route, inner attempt-boundary authentication and journaling, central route-manifest/contract registration, real rollback-only HTTP oracles, and preserved browser/capability/runtime closure |
| `eec63ee` | `docs(knowledge): checkpoint validation route` | Reconciled the registered ninth route, bounded transport/handler proofs, exact compatibility history, preserved browser/runtime closure, and 123-commit terminal state |
| `d2a57cd` | `docs(proto): clarify validation compatibility waiver` | Historical FILE-waiver comments now state that the draft fields were retired before Validate registration and were never served by either Validate or Preview |
| `2db17c3` | `feat(knowledge): bound preview requests` | Behavior-preserving extraction of the shared bounded candidate wire decoder plus an unregistered request-only Preview codec with exact projection, UTF-8, unknown-authority, raw-cap, detachment, and allocation proofs |
| `ca9c2aa` | `feat(knowledge): validate preview envelopes` | Structural Preview request validation with a canonical retained-job identity, exact Validate create/update envelope reuse, server-forced ACTIVE publication, preserved optional uint32 `maximum_rows` without assigning policy, and no response codec, handler, service, or route |
| `da56ce4` | `docs(knowledge): checkpoint preview requests` | Reconciled the shared candidate decoder, dormant Preview codec and structural validator, bounded evidence, route/runtime closure, and 127-commit terminal state |
| `74df953` | `fix(proto): bind preview request authority` | Wire-neutral Preview request contract hardening for owner-scoped retained-execution reacquisition, literal parser bounds, forced ACTIVE validation without lookup/authorization, exact optional uint32 row presence, generated Go/TypeScript comments, and cross-runtime oracle coverage |

The separate pre-existing dependency commit `fdcc17e` is also present in the
published `main` history. Unrelated commits between KO checkpoints are excluded
from the ranges above; no history rewrite or alternate publication branch was
used.

## Contract decisions frozen

- Binary case-sensitive identities with a stable ASCII-only trim/control rule
- Private → current-app → tenant-global shadow precedence
- Administrator-only, tenant-bound v0.1 management APIs
- Server-authoritative index scope and per-row trusted metadata selectors
- One exact-literal set and at most one anchored dot-all RE2 wildcard matcher per
  reached selector dimension. Runtime work uses the canonical token-derived
  `initial + UTF8_bytes*per_byte + final` upper bound rather than an engine's
  observed NFA transitions; catalog index pruning uses independent bounded
  probes rather than the later event-execution budget
- Parallel extraction, alias, and calculated stages before the authored base
  predicate, with no same-stage chaining
- `_raw`-only extraction and existing authored `rex`/`spath` typed semantics
- Copy aliases that preserve their source and explicit missing/null/overwrite
  behavior
- Reserved/canonical event roots cannot be knowledge destinations
- Scope-monotonic, same-tenant dependencies and bounded graph validation
- Append-only versions, exact mutation idempotency, catalog revisions, and
  detached immutable snapshots
- Detached canonical mutation requests: recursive unknown-field rejection,
  exact field-mask authority, domain-separated request digests, and no later
  reads from caller-owned protobuf memory
- Compact replay outcomes that bind immutable version, successful/recovery
  audit, catalog revision/state token, occurrence time, retention anchor, and
  expiry without duplicating definition bodies
- Current and latest-immutable owner/app authorization before receipt, history,
  or body hydration; immutable quarantine permanently overrides mutable
  registry rollback or state rewrite
- One immediate Writer transaction for blob, version, lifecycle, dependencies,
  projection/selectors, registry, successful audit, revision/token, immutable
  commit authority, and idempotency receipt, with durable replay reconciliation
  after an ambiguous commit result
- `Create` with ACTIVE, `Update` of an ACTIVE object, and `SetState` to ACTIVE
  remain unavailable until the KO-1 compiler supplies exact executable
  validation and dependency derivation; opaque future bodies may receive
  metadata-only updates while inactive and may be disabled/deleted through
  state-only preservation without reinterpretation, including from ACTIVE
- One-transaction resolver authority: exact first/final catalog revision and
  state-token agreement, ACTIVE defining-app validation, authorization before
  body decode, index pruning before private/app/global precedence, exact
  winning dependency closure, a shared fail-fast admission gate, and bounded
  SQLite retries
- Enabled empty catalogs use a durably provisioned revision-zero random state
  token; resolver absence remains distinct from a canonical empty authority
- Opaque unfinalized snapshot authority with binary stage/name/ID ordering,
  canonical shadows and dependency depth/ordinals, exact FIELD_INPUT derivation,
  parallel-stage collision/chaining rejection, static semantic charges, and
  self-excluding B0/B1 byte-charge plus domain-framed digest rules; only KO-0H
  sealed compiler evidence may finalize a nonzero execution snapshot
- Snapshot presence is authoritative: absent means disabled/legacy or app-less;
  a present zero-object reference is enabled-empty. References are capped at
  512 bytes and summaries at 32 KiB, retain at most the first 64 canonical
  objects out of 256, and reject unknown fields or incoherent ordinal, type,
  stage, truncation, or disclosure shapes before cloning
- Configured nonempty-app admission authorizes the live app and indexes, then
  parses, plans, resolves, compiler-seals, and finalizes before allocating a
  job ID or writing history/audit. Nil and typed-nil resolvers plus app-less
  requests preserve the legacy asynchronous path
- KO-0H finalization is intentionally enabled-empty only. A nonempty resolved
  authority fails before admission until KO-1 supplies exact knowledge
  operators, generated fields, and combined authored/knowledge charges
- A compiler-private seal covers SQL, typed arguments, output/shape contracts,
  read scope, relational evidence, authored semantic work, generated SQL
  charges, and snapshot evidence. A manager-private seal binds every completed
  execution snapshot, including legacy, to its complete immutable execution
  tuple and exact result generation; only the matching manager-owned result pin
  can open that generation
- Search jobs and history retain the bounded summary, attempt audit retains only
  the compact reference, inspection uses two sealed metadata reads without a
  result pin, and export atomically retains the matching execution/result pin
  for its complete lifetime. Browser projections redact every object identity
  pending a current-policy provenance authorizer
- The hidden Knowledge Manager is read-only and lazy. Without the trusted
  capability it is absent from navigation, its chunk is not imported, and it
  issues no knowledge request. Its adapter bounds bootstrap apps, pages,
  continuations, totals, success responses, and error bodies before exposing
  detached data. Its dormant controls support app, object-type, and lifecycle-
  state filters plus name-ascending, updated-time-descending, created-time-
  descending, and object-type-ascending sorting; every continuation reuses the
  exact query tuple. Its exact-version detail consumers independently page
  dependencies and dependents, label each direction's own catalog revision,
  and expose only the visible opposite object ID/version and fixed field-input
  role. Browser bearer attachment is limited to Get/List and those two graph
  reads; all knowledge mutation paths remain excluded
- Current-policy response redaction for retained provenance and inspection
- Binary UTF-8 substring and individual-selector-pattern filters applied before
  keyset `LIMIT` at one catalog revision
- Exact sealed current-version projection verification, canonical selector
  framing, and a 256 MiB per-tenant projection byte ceiling
- Historical Get authorized from the current identity, with permanent
  definition redaction while the current identity is quarantined
- Direct exact-version dependency inspection authorized only from the current
  root registry identity; outgoing reads may select historical source versions,
  while inverse reads admit only current source versions across every
  nonquarantined lifecycle state. Hidden or quarantined opposite endpoints are
  omitted before page/count, and exact totals are optional
- Authorization-leading inverse traversal over bounded current registry
  identities and exact `knowledge_object_dependencies_source_target_idx`
  probes. The v0.1 inverse recognizes only `target_kind = 'object'`; another
  target kind requires a new bounded current-inverse authority/index contract
- Atomic successful-mutation audit metadata and a separate fail-closed rejected
  privileged-attempt journal that retains no unauthorized object metadata
- Authentication-before-decode for all nine management operations, followed by
  tenant/owner-bound administrator authorization and trusted complete app-scope
  derivation; every authenticated definitive rejection attempts exactly one
  synchronous journal append, exposes the underlying rejection only after that
  append succeeds, and otherwise returns the fixed unavailable response, while
  known-committed and indeterminate outcomes suppress a false rejected-attempt
  row. Validate enters this boundary as `ActionValidate`, remains outside the
  generic outer administrator map, and writes no rejected-attempt row for a
  successful sealed HTTP 200 result
- Request-authority snapshots and configurable-dependency response validation:
  canonical definitions, lifecycle markers, filters, ordering, pagination,
  revision/token shape and continuation metadata, aggregate byte/node ceilings,
  and authorized error context are validated and bound before bytes or metadata
  cross the HTTP boundary
- A conservative `update` action for pre-decode Update/SetState rejection,
  refined only after complete request validation to `scope_change`, `enable`,
  or `disable`; journal tails have an independent concurrency gate and bounded
  deadline outside client cancellation
- Recognized definitions may now be created ACTIVE, updated while ACTIVE, or
  enabled from DRAFT/DISABLED through the complete transactional and compiler-
  derived publication authority. Opaque ACTIVE update and enable remain closed.
  The concrete catalog Writer may replay a previously committed,
  still-recognized canonical ACTIVE Create/Update/SetState receipt after
  downgrade without being mislabeled as a new rejection
- Catalog, resolver, cache, selector, expression, snapshot, and audit ceilings
- Terminal corruption quarantine with bounded dependent closure, protected
  version/idempotency capacity, and one recovery-audit slot for every lifetime
  physical identity
- Fail-fast candidate definition issues are detached and definition-relative,
  use only `KNOWLEDGE_DEFINITION_INVALID`,
  `KNOWLEDGE_DEFINITION_UNKNOWN_FIELD`, and
  `KNOWLEDGE_DEFINITION_RESOURCE_LIMIT`, preserve legacy text and `errors.Is`
  roots, and exclude infrastructure/invariant/canonical-storage failures. This
  remains an internal seam consumed only through Writer result construction;
  the registered handler never maps its raw errors directly
- Candidate semantic issues are detached and bound to one exact `Compile`
  input index for intrinsic regex, JSON-path, calculated `SPL_*`, and direct-
  Boolean-result failures. Validation must prove the submitted candidate and
  use singleton/index-zero attribution; `Prepare` plus authority, aggregate,
  winner-cohort/collision/selector, and dependency failures remain opaque to
  this typed compiler-issue seam. It also preserves legacy error/sentinel
  behavior; the registered handler never maps its raw errors directly
- Internal catalog validation requires exactly one inactive-storage or active-
  publication intent and a presence-sensitive create/update envelope. Update
  versions are
  1 through MaxInt64; only candidate invalidity is an in-band `valid=false`
  result. Successful output binds normalized bytes, a 32-byte digest, complete
  candidate-only singleton intrinsic resources, full-transition authorized
  direct dependencies, deterministic bounded source-located diagnostics,
  privacy, explicit truncation, recursive unknown-field rejection, and an
  8 MiB response maximum. Intrinsic resources explicitly include extraction
  outputs, JSON evaluation work, and scalar predicates. ACTIVE create uses a
  deterministic transaction-fresh non-persisted evaluation ID, returns/reserves
  no identity, and requires validity, diagnostics, resources, and target-only
  dependencies to be invariant under fresh-ID alpha-renaming; a later Create
  allocates and revalidates its own identity and live facts. `valid` is advisory
  definition validity rather than mutation acceptance: a no-op masked update
  may be valid and inactive validation of a current ACTIVE object is only
  hypothetical. One fixed transaction observes knowledge/app/index facts, but
  the response revision identifies only its advisory knowledge-ledger
  component and is no reservation or reusable proof. Preview always uses
  active-publication semantics. Validate is registered but unadvertised;
  Preview remains unregistered and unadvertised. Reserved draft fields carry an
  intentional historical FILE compatibility waiver: they were retired before
  Validate registration and never served by either route, and the change is
  not claimed schema-nonbreaking
- Candidate request decoding is one shared, layout-parameterized bounded
  raw-wire authority for Validate and the request-only Preview codec. Correct-wire
  object-ID presence controls full-create versus mask-selected-update
  projection; protobuf duplicate merge, last-scalar, optional-presence, and
  oneof reset semantics are preserved. The exact request bounds are `4259840`
  raw bytes, a 256-byte job ID, 9 retained mask paths, 17 entries per selected
  selector dimension, 17 selected regex outputs, and unknown-group depth 32.
  Every recognized string occurrence is UTF-8-checked regardless of selection,
  overwrite, or clearing; unknown authority follows the exact outer/mask,
  create, and mask-selected versus unselected update split
- Preview request structure is valid only with the canonical six-field wire
  authority and exact Validate create/update envelope under server-forced
  `ACTIVE_PUBLICATION`. The canonical retained-execution ID is not an access
  grant, and this structural boundary performs no lookup or authorization.
  Optional uint32 `maximum_rows` preserves absence, explicit zero, and every
  value through `4294967295` while assigning no default, bound, or execution
  meaning. These internal request boundaries add no Preview response codec,
  retained-execution reacquisition/application or caller-authorization service,
  handler, route, browser/capability surface, or runtime gate
- Pure result construction owns no catalog/database/transition/authorization or
  HTTP policy. Inactive normalization and active singleton preparation return
  opaque detached states; only exact typed definition/compiler issues map to
  closed in-band output, while untyped or malformed local failures are opaque
  invariants. External authority/cohort/dependency/transition failures receive
  no typed mapping through preparation; the sole result-layer exception is the
  caller-selected, target-free `BuildDependencyUnavailable` generic diagnostic.
  Ranged semantic issues are proven against and rebased to the exact submitted
  scalar with private provenance retained for every later projection/seal. A
  full transition supplies the already-authorized direct target list; the layer
  validates shape/order/bounds and rejects duplicate/self edges but cannot prove
  completeness or visibility. Result projection re-normalizes, re-digests, and
  for ACTIVE re-compiles singleton charges; the final Validate seal recursively
  rejects unknown fields, verifies exact dependency resources and range
  coordinates, limits the revision to MaxInt64, and retains at most 8 MiB of
  exact deterministic bytes. This internal boundary registers no route
- `Writer.Validate` is the catalog-owning adapter over that pure boundary. It
  requires matching tenant/owner identities but deliberately independent read
  and write app sets, admits at most one evaluation per control database, and
  snapshots only the create-all or update-mask-selected request view. Selected
  repeated-field overflow uses a bounded witness so the candidate receives the
  normalizer's typed resource issue without an unbounded size walk or clone.
  Both intents run in an always-rolled-back immediate transaction. Inactive
  validation performs no publication transition; active validation compiles
  candidate-absent baselines before opaque candidate conflict classification,
  authorizes the root/app before exposing only a generic hidden-or-missing
  target diagnostic, and never discloses target context. Fresh create IDs are
  deterministic, transaction-local, ledger-reconciled, and unreserved.
  Revision-zero responses prove physical catalog emptiness and all revisions
  are bookended. Rollback precedes sealing and any rollback error fails the
  request. No DML, mutation ID, clock, hook, audit, idempotency, commit, HTTP
  transport, browser transport, or feature advertisement is part of the Writer
  boundary; the registered adapter surrounds it without changing those service
  guarantees

The compatibility inventory contains 55 hash-pinned cases. It is explicitly a
strict contract inventory, not yet runtime semantic evidence. Before the field
feature is advertised, every relevant case must acquire concrete catalog/event
inputs and exact typed outputs or diagnostics and execute through production
normalization, resolution, planning, compilation, and ClickHouse paths.

## Independent adversarial review

Three independent reviewers covered security/authorization, semantic
compatibility, and systems/concurrency/resource behavior. All credible findings
were resolved and each reviewer returned a clean final verdict.

Resolved security findings included dependency-scope laundering, corruption as
an existence oracle, missing CRUD/preview privilege rules, retained-inspection
authorization ambiguity, and an underdefined audit taxonomy.

Resolved semantic findings included reserved/canonical output hazards,
inconsistent Bytes and optional-capture behavior versus authored `rex`,
ambiguous JSON Bytes behavior, an unsupported arithmetic example, and a fixture
claim stronger than its implementation.

Resolved systems findings included mixed-revision snapshots, incomplete digest
canonicalization, unbounded catalog/cache/resolver work, selector input rescans,
Unicode-version-dependent normalization, lost-response create retries, hard-cap
recovery deadlocks, corrupt-object quarantine, transitive dependent recovery,
general-audit exhaustion, repeated cascade transitions, and replacement-
generation recovery capacity.

KO-0B received additional independent protobuf, selector, and migration review.
Resolved protobuf findings covered calculated-field overwrite authority,
create-only draft state, accidental capability advertisement, definition-
relative masks, contradictory provenance shapes, current-policy redaction,
non-self-referential snapshot byte charging, and raw unknown-field rejection.

Resolved selector findings covered the 16-versus-17 segment storage/search
boundary, uncharged epsilon work, cancellation during a worst-case matcher,
mixed exact/wildcard canonical ordering, duplicate normalization, and multi-
pattern reference fuzzing.

Resolved migration findings covered incomplete or later-mutated dependency
sets, activation under archived apps, disabling/deleting targets with active
dependents, detached version success records, `INSERT OR REPLACE` bypasses,
NULL-safe registry/version digest agreement, bodyless terminal quarantine, and
failure rollback. A deferred immutable dependency seal now proves the exact
ordinal set before a current registry generation can commit.

KO-0C received three further independent security full-file review shards plus
independent correctness, concurrency, compatibility, performance, and
simplification review. All 39 source-like files in the exact
`b42b046..b7ac77b` diff received unique completion receipts. No plausible
security candidate survived discovery.

Resolved projection findings included a missing reverse current-version
projection invariant, undercharged description and selector bytes, missing
8 KiB canonical framing enforcement, selector ordering/duplicate gaps,
recursive-trigger behavior, and insufficient proof that correlated filters
deduplicate before keyset `LIMIT`.

Resolved successful-audit findings included missing app/type/scope metadata,
insufficient preflight width bounds, cursor digests that did not bind the new
metadata, migration tests that accidentally included later schema, and list
lookahead allocation/ownership issues.

Resolved rejected-attempt findings included disclosure-prone metadata shapes,
an inexact configurable retention ceiling, corrupt-boundary append/eviction,
redundant persisted authorization state, repeated startup scans and
allocations, sequence exhaustion, and recursive-trigger/`OR REPLACE`
tampering. The final journal records no request payloads, error strings,
derived authorization booleans, or unauthorized object metadata.

The formal Codex Security bundle used scan ID
`6e16ec57-6a8b-4bf3-a5d6-11502a0b229b`, exact binary-diff snapshot
`codex-security-snapshot/v1:sha256:3d8a3fbd70b8f42512d21123a68c71b8a2b6acdb62500b7c0897f23b69b656d9`,
39/39 source-like receipts, complete coverage, and zero reportable findings.
The desktop finalizer first rejected the unsealed manifest because its range
launcher omitted `scan.target.snapshotDigest`. The same evidence bundle was
repaired with the digest of the exact full-index binary Git diff and sealed by
the plugin's local contract validator. Final SHA-256 values are
`93c3b8fdc1f17ac64e5a0c6f048caefb10d4ca27b3812fe932f64ad8c957da5d`
for `scan-manifest.json`,
`51122d213e5c9ddf109c063835b744374051847f5ef036a6d65916908b731cb4`
for `findings.json`,
`d914dcd1313e509ef34a45466b42655ec8b7d12ecece5b15b019c1f73ebf13dc`
for `coverage.json`, and
`0614b235166c28c42fc31cf31cb8ae04a62db36c21894af30738e8146fd31010`
for the deterministic `report.md` projection. The workbench row remains
terminally failed from the launcher omission; the locally sealed bundle is the
canonical review evidence.

KO-0D used dedicated, independent test lanes throughout: one owned unit,
property, fuzz, and compatibility reference models; one owned store/API,
SQLite, concurrency, migration, crash/recovery, and integration matrices; and
rotating specialists reviewed protobufs, authorization/privacy, lifecycle,
migration/recovery, compatibility, performance, and simplification. Production
implementers were not the sole reviewers of their corresponding tests.

The pre-remediation frozen KO-0D review candidate had binary full-diff SHA-256
`f5cc1ac98fa7d4d4ce4d50d2b09c6435ab99f782cac95c5d4aea2be8686b159f`.
Security review returned clean. Correctness and concurrency review identified a
missing hostile-width case for dependency target identities and a stale query-
driver comment. The added test then exposed a failure-path cleanup weakness in
its own corruption harness. The correctness remediation rereview was clean at
SHA-256 `dd02aad8451ccc4a58efb32bb145448c4e765f22c9f0eb0df0f23a1346fd4e76`.
After failure-safe cleanup was added, the focused test passed five times
normally and under race, and the concurrency/resource cleanup rereview returned
clean on the final KO-0D binary full diff. Its SHA-256 is
`53c28d93c5adfcdcbd448c32e64f8633469930aee923b6a4de9f87308d6717f4`,
derived exactly by
`git diff --binary c5440b9..c8b9757 | shasum -a 256`.

Earlier frozen reviews in the same slice found and resolved revision-ledger
coupling, dependency graph agreement, lifecycle chronology, quadratic upgrade
backfill, unsafe private SQLite test instrumentation, projection materialization
before byte checks, selector/dependency pre-LIMIT work amplification,
non-deterministic WAL overlap tests, semantic-budget cursor coverage, and rogue
projection starvation. The final implementation uses public test hooks only,
streams guarded projection payloads, starts projection authorization from
bounded authorized registry branches, and proves read transactions are wholly
old or wholly new across a concurrent WAL commit.

The formal security-diff workflow was not claimed for KO-0D: its attempted
snapshot became stale while concurrent agents were still editing, and that
workflow disallowed substituting a new scan in the same pass. The independent
frozen-hash security rereview above is the applicable KO-0D security evidence.

KO-0E used independent implementation, migration, protobuf, test-architecture,
capacity, crash-recovery, state-machine, compatibility, security, correctness,
concurrency/resource, and simplification lanes. Review findings were treated as
release blockers until their production invariant and a non-tautological
regression passed. Resolved issues included authorization after receipt decode,
caller-request mutation after digest preparation, mutable-only quarantine
checks, incomplete historical owner binding, replay/commit authority rebinding,
retention truncation and stale clocks, ambiguous commit responses, panic-held
transactions, unbounded counter/blob health scans, reclaim-prefix skipping and
TOCTOU, premature or corrupt receipt reclamation, active-dependent historical
scan amplification, state-only active opaque handling, outcome/body size
duplication, audit actor attribution, migration semantic smuggling, and
absolute-capacity reclamation liveness.

The formal KO-0E Codex Security diff scan used scan ID
`0bc1c966-0d29-4e94-86d8-6c5ca0c66f7e` and exact worktree snapshot
`codex-security-snapshot/v1:sha256:aae89148c169ae365fa17fc6e8c5a6fed4edbf80dcdc00c0fb3eb648650e5867`.
All 55 deterministic source-like paths had exactly one full-file completion
receipt; there were no duplicate, missing, deferred, or reportable rows. The
sealed report returned zero findings. Its SHA-256 values are
`046aa3f35930322b4d27ecc54ae7f17c29eb35e831ce3d85f0e69ffb28de1d5f`
for `scan-manifest.json`,
`135804db61599f337ecd952278a64ce14ea96b1431b0fd17905f0f6156c2740a`
for `findings.json`,
`687b7f499482c857855791f6c39500830118ebdac501e77602a9b42d0cbef308`
for `coverage.json`, and
`00b610045842194614f5d5cacfa9925eb33740b2bd6dd43c611c276fddd21e8d`
for the generated `report.md`. The workbench reported complete usage coverage:
568,018 total tokens, 21,759,481 input tokens, and 21,250,048 cached input
tokens.

The formal snapshot's tracked binary-diff SHA-256 was
`b648c05244854466640a7feab8f3af7396e916f7f893760107c84e5fc3b1effd`;
its sorted untracked content manifest was
`b8e481a7671e315442e294b9c9504157766c2c32606a5468861b5c98c86ef2ad`.
The only post-scan semantic change was the measured CI timeout adjustment from
20 to 30 minutes. An independent rereview proved that reversing exactly that
line reproduced the formal tracked hash, the untracked hash remained unchanged,
and the change altered only the timeout argument; it changed no test target,
permission, action, runner, or secret.
The full KO-0E implementation binary diff `e9af86d..a8000e7` has SHA-256
`2c8ecb49c201f89ef9fe011d964af01cc4364709ffa5ed7e367ef5eb8d9ac368`.
The narrower post-snapshot-contract Writer diff `f9df615..a8000e7`, which
contains the formal scan's implementation payload plus the separately reviewed
timeout-only change, has SHA-256
`cdb3f9afb82e64e55f2444be85dac2d4beff0f3e967d654f57a4dab449c66e4c`.

KO-0F used independent handler-architecture, catalog-context, authentication,
attempt-boundary, real-persistence, HTTP-matrix, read-contract, authorization,
resource, correctness, and simplification lanes. Resolved findings included
cross-tenant principals, authentication outside the route deadline,
request-pointer mutation, Unicode identity drift, unbound rejection metadata,
false rejection of committed or indeterminate mutations, journal-tail
concurrency escape, ACTIVE publication bypass and downgrade replay loss, List
request/response/filter/pagination mismatch, impossible dependency errors,
canonical definition and opaque-body gaps, and repeated-message allocation
amplification before sanitization or response validation. The final frozen
adversarial pass reported no remaining concrete finding. The exact
three-commit KO-0F binary diff `e78d60b..d6e767d` has SHA-256
`c075ea32554144a036de277953746fa9282c9d3d7291d739cd63eea90ac335e3`.

KO-0G used independent snapshot-wire, resolver architecture, search-lifecycle
seam, tenant-authority migration, compiler-charge, SQL-plan, retry/pool,
cross-language, correctness, resource, and adversarial lanes. Resolved findings
included Go/TypeScript oneof ordering drift, metadata-after-body forward-wire
ambiguity, absent revision-zero state authority, mutable/rekeyable app revision
rows, incomplete FIELD_INPUT derivation, selector implication for missing/null,
same-stage output collisions and chaining, repeated-pattern allocation, visible
loser corruption, global defining-app validity, duplicated hydration, per-Store
instead of per-database admission, unsealed compiler-charge finalization, and
wall-clock test oracles unrelated to the production deadline. Final frozen
snapshot and resolver reviews reported no remaining concrete blocker. The exact
seven-commit KO-0G binary diff `6005b24..4aaacc7` has SHA-256
`d0819a58c8cbe7da302733521dd98830242e68aeea68ec862e0e8753ead6283f`.

KO-0H used independent wire, migration, security/principal, search-lifecycle,
compiler-seal, audit/history, inspection, export, UI, consumer-adversarial, and
simplification lanes. Findings were held open until the shared tree and an
exact regression were green. Resolved issues included untrusted app intent,
post-admission resolution, forged or downgraded execution snapshots, compiled
query mutation, cross-manager result pins, inspection/export authority drift,
typed-nil dependencies, metadata undercharging, late sink callbacks, summary
omission and identity disclosure across lifecycle states, UI continuation
contradictions, unmount cancellation, response-body amplification, and
malformed bootstrap-app amplification.

Three final simplify lanes inspected a frozen 9,473-line tracked diff with
SHA-256 `42dec703866849092b05f603ef87426367fecf51d6dc7d0763dc5cb56de930fa`
plus all 29 then-untracked paths. That hash predates the remediations and is not
the final range hash. The applied cleanup centralized retained-authority
opening, reused canonical identity validation, replaced a positional
six-result lease API with `ValidatedResultMetadata`, eliminated ordinary-lease
execution HMAC work, reused the sealing nonce, and acquired export send turns
before full-row clones. Consumer/UI rereviews returned clean. A final
race-shuffle run exposed only a test-fixture dependence on the resolver's
production 250 ms deadline; the real migrated test resolver now retries only
its own deadline under a 30-second test watchdog, and the exact captured
searchjobs package seed passed twice without changing the production bound.

The exact four-commit KO-0H binary diff `441fd4d..bcf095b` has SHA-256
`0b5f16843bddd62682b9a5b2847709c801ce3b2f5654b8e5936cd054063833c2`.

## Verification evidence

All commands ran from `/Users/suhaib/code/open-splunk`.

| Gate | Result | Evidence |
| --- | --- | --- |
| Strict fixture JSON check | pass | `jq -e '.compatibility_version == "0.1" and (.cases | length == 55)' internal/knowledge/testdata/compatibility-v0.1.json` |
| Fixture digest | pass | SHA-256 `958eb18284f45a895951e5a1539537dda19f78c0ad380acb8847312b6ebe7fd4` |
| Targeted race test | pass | `GOCACHE=/tmp/open-splunk-ko-go-cache go test -race ./internal/knowledge -count=1` |
| Full Go unit suite | pass | `GOCACHE=/tmp/open-splunk-ko-go-cache go test ./... -count=1` at checkpoint commit; local socket/filesystem permission was required for existing listener and ACL tests |
| Patch hygiene | pass | `git diff --cached --check` before commit |
| Remote durability | pass | `origin/main` resolved to checkpoint commit after push |

KO-0B evidence:

| Gate | Result | Evidence |
| --- | --- | --- |
| Protobuf format/lint/generation | pass | `make proto` |
| Protobuf backward compatibility | pass | `buf breaking --against '.git#branch=main'` |
| Protobuf/server contract tests | pass | targeted root and `internal/server` knowledge/capability tests |
| Generator crash/concurrency safety | pass | `node --test scripts/compile-protos.test.mjs`, 6/6 |
| TypeScript contracts | pass | `npm run typecheck` |
| Selector unit/race | pass | `go test -race ./internal/eventfields ./internal/knowledge -count=1` |
| Selector fuzz | pass | four 2-second targets; final runs executed 504,686, 555,086, 377,154, and 601,243 cases |
| Migration fresh/upgrade/invariants | pass | seven `TestKnowledgeCatalog*` tests |
| Control-plane migration race gate | pass | KO migration, startup, backup, and ledger subset; final local run 7.989s |
| Full control package | pass | `go test ./internal/control -count=1`, 6.809s |
| Patch hygiene | pass | `git diff --check` and staged checks before every commit |
| KO-0B remote durability | pass | KO-0B commits are present on `origin/main` |

KO-0C evidence:

| Gate | Result | Evidence |
| --- | --- | --- |
| Patch and formatting hygiene | pass | `git diff --check`; generated Go formatting diff was empty |
| Protobuf generation and lint | pass | `make proto` |
| Protobuf backward compatibility | pass | `buf breaking --against '.git#branch=main'` against the pre-change revision |
| TypeScript contracts | pass | `npm run typecheck` |
| Generator safety | pass | `node --test scripts/compile-protos.test.mjs`, 6/6 |
| Focused normal packages | pass | control 11.992s; audit 7.145s; auth 8.624s; server 9.331s; searchaudit 2.193s; knowledgeattemptaudit 4.878s; testsupport 1.062s |
| Fresh focused confirmation | pass | control 12.458s; audit 7.480s; knowledgeattemptaudit 4.838s; server 11.037s; auth 9.899s; searchaudit 3.763s |
| Full Go suite | pass | `go test ./... -count=1`, including self-contained ClickHouse package tests |
| Combined race suite | pass | control 93.489s; audit 117.742s; auth 86.709s; server 64.344s; searchaudit 19.478s; knowledgeattemptaudit 84.702s; testsupport 1.885s |
| Per-commit isolation | pass | detached worktrees verified `f9eb14a`, `fc34902`, and `b7ac77b` with their relevant package suites |
| Simplification review | pass | three independent reviews of snapshot SHA-256 `0b016d90d035d204903d8263e525e75a3d8c2240512142444ef19bdb64311e0c`; all credible findings resolved |
| Attempt-journal performance | pass | normal 4.626s → 3.358s; race 126.474s → 73.650s, 41.8% faster |
| Formal security diff scan | pass | exact range `b42b046..b7ac77b`; 39/39 receipts; complete coverage; zero reportable findings; canonical bundle sealed locally after the launcher digest omission |
| KO-0C remote durability | pass | `origin/main` contains terminal KO-0C commit `b7ac77b1a9fc28b282e72e7153f7b26739a35629` |

KO-0D evidence:

| Gate | Result | Evidence |
| --- | --- | --- |
| Patch and generation hygiene | pass | `git diff --check`; `make proto`; regenerated Go/TypeScript outputs left the frozen diff hash unchanged |
| Protobuf backward compatibility | pass | `BUF_CACHE_DIR=.cache/buf npx --no-install buf breaking --against '.git#branch=main'` |
| Generator crash/concurrency safety | pass | `node --test scripts/compile-protos.test.mjs`, 6/6 |
| Full catalog package | pass | final `go test ./internal/knowledgecatalog -count=1`, 17.619s |
| Exact backend CI-equivalent gate | pass | `GOCACHE=/private/tmp/open-splunk-ko-go-race-cache go test -race -shuffle=on -covermode=atomic -coverprofile=/private/tmp/open-splunk-ko-coverage.out -timeout=20m ./...`; catalog 710.912s at 80.5% statement coverage, control 516.379s at 79.1%, and every package passed |
| Targeted final corruption-harness repetition | pass | `TestIntegrationDependencyPhysicalPreflightBoundsRowsBeforeGroupedValidation` with `GOCACHE=/private/tmp/open-splunk-ko-go-cache go test ./internal/knowledgecatalog -run '^TestIntegrationDependencyPhysicalPreflightBoundsRowsBeforeGroupedValidation$' -count=5` (0.997s) and the same command with the race cache and `-race` (15.484s); grouped validation remained unentered |
| Migration upgrade/recovery | pass | fresh/upgrade/rollback/marker/tuple/over-cap/backup matrices; 61,440-version linear backfill had no correlated plan, and the full control race family passed during review |
| Go static analysis | pass | `go vet ./internal/knowledgecatalog ./internal/control ./internal/knowledgedefinition ./internal/spl` |
| Live fuzz confirmation | pass | captured 5-second runs using `go test ./internal/knowledgecatalog -run '^$' -fuzz '<exact target>' -fuzztime=5s -parallel=2 -count=1`: `FuzzCalculatedDependencyInputFields` executed 53,343 cases with 9 new interesting inputs; `FuzzDecodeCursorRejectsMalformedOrReboundInput` executed 68,775 with 13 new interesting inputs |
| Dedicated CI fuzz inventory | pass | four shards enumerate and execute all 15 exact fuzz targets in `internal/knowledge`, `internal/knowledgedefinition`, `internal/knowledgecatalog`, and `internal/spl`; additions, removals, and renames inside those inventoried packages fail instead of silently running seeds only |
| Frontend lint/types/tests/build | pass | `npm run lint`; `npm run typecheck`; `npm run test:frontend` (66 infrastructure + 159 UI tests); `npm run build` |
| Frozen adversarial rereview | pass | security clean on pre-remediation `f5cc1a…`; correctness remediation clean at `dd02aa…`; final concurrency/resource cleanup clean on the binary full diff SHA-256 `53c28d93c5adfcdcbd448c32e64f8633469930aee923b6a4de9f87308d6717f4`; all credible findings resolved |
| Local durability | pass | six intentional commits, inclusive `7fd1270` through `c8b9757`, on `codex/knowledge-objects-runtime`; clean worktree before this checkpoint edit |
| Remote durability | pending | push to the exact GitHub `origin` was policy-blocked pending explicit destination approval; no remote commit is claimed |

KO-0E evidence:

| Gate | Result | Evidence |
| --- | --- | --- |
| Protobuf format/lint/generation | pass | `make proto` twice; generated tree SHA-256 stayed `47803324a947184a8e21afdb2a6256905d20ce2e144eceeed526694e11ccedc3` under the sorted tracked-file recipe below; no generation-lock residue |
| Protobuf backward compatibility | pass | `BUF_CACHE_DIR=$PWD/.cache/buf npx --no-install buf breaking --against '.git#branch=main'` |
| Go/TypeScript wire contracts | pass | root Go contracts passed; generated Go and TypeScript outcome encoders independently pin revision/token, retention authorities, and the success/recovery audit oneof |
| Frontend/static contracts | pass | `npm run typecheck`; `npm run lint`; `npm run test:frontend` (66 infrastructure + 161 TypeScript tests, 227/227); `npm run build`; `node --test scripts/compile-protos.test.mjs` (6/6) |
| Full non-race backend | pass | `GOCACHE=/private/tmp/open-splunk-ko-go-cache go test ./... -count=1`; about 70.04s wall, control 57.754s, knowledgecatalog 65.978s |
| Exact race/coverage gate | pass | Exact retained-log invocation below; 24m01.735s wall, 71 package passes, zero failures/race diagnostics; control 472.460s at 79.1%, knowledgecatalog 1429.801s at 77.9% |
| Race shuffle evidence | pass | control seed `1786151276428055000`; knowledgecatalog seed `1786151281042435000`; all 68 emitted package seeds retained in `/private/tmp/open-splunk-ko-writer-final-race.jsonl` (three no-test-binary packages emitted no shuffle seed) |
| Timeout calibration | pass | the preceding 20-minute run and same-seed isolated catalog rerun both timed out solely from cumulative suite time; the isolated run completed 122/166 top-level tests with 1197.12s summed elapsed and no race report, so CI moved to 30 minutes without removing tests |
| Writer transaction fault matrix | pass | 17 Create precommit points (the prepared hook plus 16 transactional boundaries), Create after-commit, and Update/Disable/Delete before/after commit; all 24 real subprocess SIGKILL/reopen scenarios passed a fresh `-count=10` (240 executions) and the retained full race gate, with exact rollback/replay/integrity/FK and zero unexpected object-ID allocation assertions |
| Deterministic state machine | pass | 20 seeds × 19 public API steps (380 modeled steps, 100 unique commits), exact history/current/ledger/token/receipt-audit/FK/integrity and caller-detachment assertions after every step; normal repeat and race gates passed |
| Capacity and reclaim matrix | pass | exact normal 16,384 and absolute 20,480 receipt boundaries; deterministic oldest 4,097 reclaim; retention 7d/365d and nanosecond rounding; corruption width/semantics/provenance, cancellation rollback, and exact production EXPLAIN plans; focused race 118.875s |
| Publication batching | pass | the production publication staging path wrote 1,024 dependency rows as 16×64 batches and 64 selector rows as one batch before the graph-node guard rejected and rolled back the ceiling fixture; zero-row behavior, hook order, no nested SAVEPOINT, and later-batch full rollback were also pinned; focused race 21.485s |
| Live Writer fuzz | pass | 5-second independent runs: update mask 279,069 executions; mask detachment 424,449; prepared Create digest 230,438; strict outcome decode 374,239; each target asserts a known-valid success seed before fuzzing |
| Static analysis and hygiene | pass | `go vet ./internal/knowledgecatalog ./internal/control`; the tracked-Go formatting command below produced no paths; `git diff --check` and every staged diff check clean |
| Independent frozen review | pass | final security verdict clean on the formal hashes; correctness, compatibility, concurrency/resource, test-oracle, and simplification reviews resolved every credible finding; the timeout-only delta received a separate clean review |
| Formal security diff scan | pass | scan `0bc1c966-0d29-4e94-86d8-6c5ca0c66f7e`; 55/55 unique receipts, complete coverage, zero candidates/findings, sealed canonical JSON and generated report |
| Local durability | pass | six KO-0E implementation commits, inclusive `f9df615` through `a8000e7`, plus the prior KO-0D checkpoint are on `codex/knowledge-objects-runtime`; implementation worktree was clean before this checkpoint edit |
| Remote durability | pending | `origin/main` remains `c5440b9`; the exact GitHub push is still blocked pending explicit destination approval, so no remote KO-0D/KO-0E durability is claimed |

KO-0F evidence:

| Gate | Result | Evidence |
| --- | --- | --- |
| Historical KO-0F public-disablement contract | pass for that slice | At the KO-0F checkpoint, `TestConfiguredKnowledgeManagementRemainsPubliclyUnregisteredAndUnadvertised` configured all four hidden dependencies and proved the then-production Create path remained 404 with no Writer, app-catalog, or attempt-journal call while bootstrap omitted `SERVER_FEATURE_KNOWLEDGE_FIELD_OBJECTS`; later management-runtime work intentionally replaced only the route-absence part of this historical boundary |
| Historical all-six HTTP matrix | pass for that slice | The KO-0F test-only SRouter exercised Get/List/Create/Update/SetState/Delete success, pre-decode authentication/administrator rejection, exact fallback/refined actions, bounded codecs, stable error bodies, scope derivation, cancellation, and journal failure without adding a then-production route |
| Real persistence boundary | pass | five focused HTTP integration tests use the migrated SQLite Store, Writer, successful audit, and rejected-attempt journal through create/update/disable/delete/replay, stale versions, hidden outcomes, success-audit rollback, and attempt-journal failure |
| Full catalog package | pass | `GOCACHE=/private/tmp/open-splunk-ko-go-cache go test ./internal/knowledgecatalog -count=1`, 45.283s on the final implementation tree |
| Full server package | pass | `GOCACHE=/private/tmp/open-splunk-ko-go-cache go test ./internal/server -count=1`, 16.911s outside the sandbox for existing localhost listener tests |
| Focused race | pass | catalog error/preflight/result contracts 3.648s; server Knowledge/ACTIVE-replay matrix 12.287s |
| Full non-race backend | pass | `GOCACHE=/private/tmp/open-splunk-ko-go-cache go test ./... -count=1`; catalog 69.157s, control 60.172s, server 20.821s, and every package passed including integration and ClickHouse |
| Static analysis and hygiene | pass | `go vet ./internal/knowledgecatalog`; `go vet ./internal/server`; all package Go files were gofmt-clean; `git diff --check` and every staged diff check passed |
| Independent review | pass | final security/correctness/resource adversarial review returned clean; the simplify pass removed duplicate full definition normalization and a throwaway maximum-size request clone/marshal before commit |
| Local durability | pass | `049f749`, `51b7ff4`, and `d6e767d` are separate catalog, production-server, and test-matrix commits on `codex/knowledge-objects-runtime` |
| Remote durability | pending | no push was attempted without explicit destination approval; no new remote durability is claimed |

KO-0G evidence:

| Gate | Result | Evidence |
| --- | --- | --- |
| Protobuf generation and compatibility | pass | `make proto` twice with stable output; `BUF_CACHE_DIR=$PWD/.cache/buf npx --no-install buf breaking --against '.git#branch=main'`, where `main` resolved to `c5440b96248c68a9b58d10ebaf08eaef5345b61a`; generated-tree SHA-256 `8552de46908863d8abe1864bfb486e6f5e0e261dc9613078529cbc0ac9ddddce` |
| Go/TypeScript canonical wire | pass | shared extraction fixture plus current `testdata/knowledge-snapshot-wire.json` SHA-256 `ea3c774af5dcf6f3c684e4c0769fc01cb224f71725deb325d7fae5e3be69db0c`; Go and ts-proto independently reproduce B0 length 1272, B1 length 1275, final length 1309, and digest `6d7a0758742fbec7123dfc45afffd6dbfa8784d3ecf86527a1a8e2294f5a1231`. KO-1C evidence staging revised only the fixture's provisional synthetic generated-operator count to the exact sealed program charge; protobuf schema, format version, and wire lengths remain unchanged |
| Frontend/generator contracts | pass | `npm run typecheck`; `npm run lint`; `npm run test:frontend` (66 infrastructure + 163 frontend tests); `npm run build`; `node --test scripts/compile-protos.test.mjs` (6/6) |
| Tenant authority migration | pass | fresh/backfill/preserve/idempotence, exact backup token, future first-app provisioning, second-app monotonicity, transition/delete/REPLACE/UPSERT guards, corrupt-prestate rollback, and default-recursive-trigger behavior; full control normal and race gates passed |
| Snapshot authority | pass | focused normal/race/vet; exact winner, shadow, dependency, selector, parallel-stage, byte/node/work, unknown/nil, detachment, B0/B1 framing, and shared cross-language golden matrices passed |
| Resolver semantics and query plans | pass | authorization-leading one-transaction resolution, old-or-new WAL barrier, visible-loser/global-app corruption, index pruning/precedence, exact dependency closure, 513-object two-chunk hydration, and production EXPLAIN plans for all three authorization branches passed normal and race gates |
| Retry, deadline, and admission bounds | pass | actual SQLite BUSY plus primary/extended BUSY/LOCKED classification, exact three attempts with 2/4 ms cancellable backoff, nonbusy single attempt, 250 ms sole-connection exhaustion, permit recovery, and one shared 32-permit gate across Stores over the same `*control.DB` handle |
| Full non-race backend | pass | `GOCACHE=/private/tmp/open-splunk-ko-go-cache go test ./... -count=1`; knowledgecatalog 70.095s, control 60.878s, server 21.012s, every package passed |
| Exact race/coverage gate | pass | exact retained-log invocation below; 26m24.739s wall, 72 package passes, 69 emitted shuffle seeds, zero failures and zero race diagnostics; knowledgecatalog 1569.799s at 79.7%, control 502.109s at 79.2%, server 192.179s at 85.1% |
| Causal async oracles | pass | the real server journal finalization test passed under full race load in 33.52s using a causal finalize signal with a 30-second watchdog rather than a two-second polling deadline; the paused resolver read-transaction core passed independently of the public 250 ms budget while public pre/post `Resolve` assertions stayed intact; both passed 10 normal and 5 concurrent race+coverage repetitions before the final gate |
| Static analysis and hygiene | pass | final `go vet ./...`; tracked Go formatting produced no paths; `git diff --check`; generated output and worktree stayed clean through the final gate |
| Independent frozen review | pass | snapshot, resolver, migration, wire, charge, query, retry, security/correctness, and resource reviewers resolved every credible finding and returned clean frozen verdicts |
| Local durability | pass | seven KO-0G commits, inclusive `37e6bfc` through `4aaacc7`, are on `codex/knowledge-objects-runtime`; implementation worktree was clean before this checkpoint edit |
| Remote durability | pending | `origin/main` remains `c5440b9` and `origin/codex/knowledge-objects-runtime` ends at `7503246`; no push of the terminal test-oracle commits or this checkpoint was attempted without explicit destination approval |

KO-0H evidence:

| Gate | Result | Evidence |
| --- | --- | --- |
| Protobuf generation and compatibility | pass | `make proto` ran twice with stable output; the exact binary `proto`/`gen` diff SHA-256 is `f4f6578f9c99d100d391a689031d5e3bbdee00ab8e29ff96bc0eb63abe6b498b`; Buf breaking passed against `main` at `c5440b96248c68a9b58d10ebaf08eaef5345b61a` |
| Go/TypeScript summary wire | pass | shared `testdata/knowledge-snapshot-summary-wire.json` SHA-256 `d60bb896c8fba542e36daf02efa3074b2df15b6d341e60fdf6f749e454bc5318`; Go and TypeScript pin absent, enabled-empty, authorized, and redacted presence plus deterministic bytes above JavaScript's safe-integer range |
| Attempt-audit migration | pass | migration 0032 SHA-256 `5253463a8f669a7287849ab8516db0be62b84d25689e1da993c8fecc872d683a`; legacy upgrade, maximum valid tuple, partial/invalid rejection, immutability, backup, and foreign-key matrices passed |
| Admission and execution authority | pass | focused matrices cover live-app-before-index authorization, synchronous side-effect-free failure, nil/typed-nil and app-less legacy parity, no worker reparse/replan/reresolve, empty-only finalization, compiler-seal tampering, manager-signed result generations, and cross-manager/mismatched pin rejection |
| Lifecycle consumers | pass | search job/list, history, compact attempt audit, inspection, and export passed detachment, exact admission identity, corrupt tuple, retained compiler, postflight mutation, expiry, and lease-lifetime result-pinning tests |
| Hidden browser shell | pass | `npm run typecheck`; `npm run lint`; `npm run test:frontend` (66 infrastructure plus 183 frontend tests, 249 total); final `npm run build` generated all 11 static pages. Absent-capability tests prove unchanged navigation, no chunk import, and no knowledge request |
| Full non-race backend | pass | `go test ./... -count=1` passed on the final production implementation; after the race-only fixture stabilization, `go test ./internal/searchjobs -count=1` passed in 1.760s |
| Affected race suites | pass | full race packages for clickhouse, queryexec, searchjobs, searchinspection, export, searchhistory, searchaudit, and the focused server projection/inspection/export families passed; the captured searchjobs shuffle seed `1786190703558618000` passed twice after the test-only deadline repair |
| All-package race/coverage | canceled | early attempts exposed restricted-sandbox listener/ACL failures and the subsequently repaired searchjobs race-fixture deadline. The affected-package gates passed separately after that repair; an unrestricted all-package attempt then hit host-memory kills in unchanged `internal/auth` and `internal/audit` without a failed assertion or race report. The user canceled another long rerun, so no all-package race pass, package count, coverage, or wall time is claimed |
| Static analysis and hygiene | pass | final `go vet ./...`; changed and untracked Go files were gofmt-clean; `git diff --check` and every staged diff check passed |
| Independent review | pass | wire, principal/security, core, consumer, UI, and three simplify lanes resolved every concrete finding selected for KO-0H; final consumer and UI rereviews were clean |
| Local durability | pass | `20a729d`, `a5ad901`, `de64633`, and `bcf095b` are separate provenance, sealed-admission, retained-consumer, and hidden-UI commits on `codex/knowledge-objects-runtime` |
| Remote durability | pending | `origin/main` remains `c5440b9` and the remote feature branch remains `7503246`; no push was attempted without explicit destination approval |

KO-1A evidence:

| Gate | Result | Evidence |
| --- | --- | --- |
| Logical and charge contract | pass | exact literal hit, literal-only miss, wildcard hit/miss, valid multibyte UTF-8 byte charging, universal-`*` missing/null behavior, all four fixed dimension positions, exact ceiling/exhaustion, and cancellation oracles pass; the conservative `initial`, `per_byte`, and `final` coefficients provably bound every combined-NFA state/closure inspection |
| Compiler-facing matcher | pass | detached programs expose sorted exact literals and one deterministic anchored case-sensitive dot-all RE2 wildcard alternation; regex metacharacter, escape, Unicode-scalar, newline, and combined-alternative results match the independent closed-glob reference corpus |
| Resolver compatibility | pass | the exact 1,024-unit selector over 256 distinct 255-byte authorized indexes has a hypothetical cumulative execution charge above 1 GiB but remains a valid bounded pruning nonmatch; each admission probe has an independent hard budget under the shared resolver context instead of producing false `ErrCorrupt` |
| Focused and downstream tests | pass | final selector normal 0.343s and race 1.676s; exact maximum-scope pruning race 1.818s; full knowledgecatalog normal 59.335s; knowledgesnapshot normal 0.469s |
| Static analysis and hygiene | pass | `go vet ./internal/knowledge ./internal/knowledgesnapshot ./internal/knowledgecatalog`; gofmt; `git diff --check` |
| Independent review | pass | separate correctness, security/resource, and compatibility/test-oracle reviewers proved the assessment, resolved the event/query wording and maximum-scope pruning findings, and returned clean final verdicts |
| Runtime activation at KO-1A | unchanged in that slice | At the KO-1A checkpoint, production configured no search resolver or management route, advertised no capability, and rejected nonempty snapshot finalization; KO-1A changed no shipping search path |
| Local durability | pass | `03a0b3e` is a separate selector-contract commit on `codex/knowledge-objects-runtime` |
| Remote durability | pending | no push was attempted without explicit destination approval |

KO-1B evidence:

| Gate | Result | Evidence |
| --- | --- | --- |
| Typed program authority | pass | valid-empty versus absent authority, detached typed operations, canonical regex/JSON interleaving, named-capture and calculated-result restrictions, output-level provenance, semantic charge N/N+1, commitment sensitivity, and caller-mutation isolation pass |
| Dependency and parallel semantics | pass | exact missing/extra/depth dependency closure, endpoint digest/order, sharing and selector implication, overlapping same-stage output and chain rejection, and provably disjoint writer acceptance pass |
| Plan integrity | pass | prelude placement immediately after detached `Scan`, exact drop/reorder/substitute/duplicate/marker-removal rejection, authored predicate separation, and valid empty/nonempty field-analysis and timeline eligibility pass |
| Focused verification | pass | `go test ./internal/spl ./internal/knowledgeprogram ./internal/plan -count=1` passed in 1.2s; `go test -race ./internal/knowledgeprogram ./internal/plan -count=1` passed in 3.0s; focused vet passed in 1.2s; `git diff --check` was clean |
| Independent review | pass | separate semantic-mapping and program/plan adversarial reviews found commitment reconstruction, dependency completeness, same-stage semantics, capture shape, aggregate bounds, ordering, detachment, eligibility, Boolean-result, and provenance gaps; every concrete finding selected for this slice was fixed and covered before the focused rerun |
| Runtime activation at KO-1B | unchanged in that slice | At the KO-1B checkpoint, the program was not yet consumed by ClickHouse, sealed into compiler evidence, rebuilt by retained consumers, or accepted by nonempty finalization; production composition, routes, capability, and UI remained disabled |
| Local durability | pass | `3278018` is a separate immutable-prelude implementation commit on `codex/knowledge-objects-runtime` |
| Remote durability | pending | no push was attempted without explicit destination approval |

KO-1C closed-gate evidence:

| Gate | Result | Evidence |
| --- | --- | --- |
| Physical field prelude | pass, execution closed | one anchored selector matcher per constrained dimension; exact regex/JSON extraction tuples; canonical interleaved extraction publication; frozen-input alias and calculated stages; disjoint repeated-destination arbitration; deterministic argument order; and one central relation composer before authored operators are implemented and unit-tested |
| Runtime resource graph | pass, pinned acceptance pending | selector input/query, cumulative regex capture, and alias-copy event/query accounting use UInt128, strict ceilings, stable precedence markers, and one deferred materialized validation branch that compiler/unit tests structurally force ahead of authored filtering; pinned engine proof remains pending, and the exact alias-copy formula and saturation authority live in `internal/knowledge` |
| Container fidelity | pass, runtime acceptance pending | stored-path authority, relative name/type/version sidecars, exact-leaf precedence, conditional-writer preservation, compiler-sealed hidden result descriptors, exact driver header checks, explicit-null/escaped-path reconstruction, and future metadata-version rejection are implemented; the nonempty gate remains closed until the digest-pinned engine matrix proves native Dynamic sizing and branch laziness |
| Compiler/snapshot evidence | pass, execution closed | the physical lowering proof derives exact object/charge totals; the semantic program supplies the independently compared commitment; authored regex/extraction/JSON/predicate charges remain separate; `knowledgesnapshot` checked-adds shared ceilings and emits exact knowledge-only versus combined wire charges. The shared deterministic fixture remains schema/version/length stable with SHA-256 `ea3c774af5dcf6f3c684e4c0769fc01cb224f71725deb325d7fae5e3be69db0c` and snapshot digest `6d7a0758742fbec7123dfc45afffd6dbfa8784d3ecf86527a1a8e2294f5a1231` |
| Retained plan reconstruction | pass for sealed legacy/enabled-empty; nonempty staged | `ExecutionSnapshot.OpenRetainedKnowledgePrelude` accepts only a valid Manager signature, rejects half-pairs, in-place stripping, and fresh public-field reconstruction, preserves legacy absence versus enabled-empty presence, and returns a detached program. `searchsnapshot.BuildExecutionPlan` injects that exact program for postflight inspection, field catalog/summary, and timeline; knowledge export continues to use the exact retained compiled execution instead of recompiling |
| Focused verification | pass | affected-package normal gate: `go test ./internal/searchjobs ./internal/searchsnapshot ./internal/searchinspection ./internal/searchanalysis ./internal/export -count=1 -timeout=30s` (all five packages passed in 2.5s or less each); focused race across retained authority, downgrade, inspection, analysis concurrency, and export tamper tests passed in 7.7s; focused vet and `git diff --check` passed |
| Independent review | pass | independent reviewers found the unsigned reconstructed-downgrade and unsigned search-analysis fixture gaps; both were fixed with universal Manager-seal validation and Manager-minted test snapshots, then two final reviewers returned clean verdicts |
| Digest-pinned ClickHouse acceptance | pending | the opt-in fixture covers Dynamic `byteSize`, alias-copy exact/+1, losing-branch laziness, all five guard markers, and deferred hidden-failure atomicity. A driver query-parameter false positive in its synthetic JSON literal was corrected in `98b7a15`, but the subsequent Docker run was canceled; no green pinned runtime result or broader authored-suffix/finalizer matrix is claimed |
| ACTIVE dependency schema prerequisite at this checkpoint | pass, publication then closed | Migration 0033 SHA-256 `171c5b390d1033a48405ab5953131c312a2fed5ba09a22bd7b8a58f62cba9f7f`; enable could seal a rederived dependency set, disable/delete retained exact predecessor identity, stale ACTIVE pins rejected upgrade atomically, and advancement was blocked only while a current ACTIVE dependent retained another target pin. The schema supported a bounded disable/advance/re-enable cascade; Writer create/active-update/enable gates were still closed at this checkpoint |
| Publication transition authority at this checkpoint | pass for recognized removal; activation then closed | Strict and candidate-absent cohort validation bound exact candidate identity, winner mode, canonical program commitment, rich derived edges, and every retained winner's persisted edge authority. Paired before/after index atoms produced exact minimum-witness OR closure under 1,024-atom, 1,024-state, 256-index, and independent 65,536-probe ceilings. The pure transition derived symbolic visibility and one candidate graph across every win. Its same-transaction reader proved bounded object/app/index inventories and exact revision facts; an opaque transaction-bound wrapper revalidated rich current ACTIVE targets and made `publishMutation` use only its detached projection for version, rows, and seal before any persistence write or hook. Recognized ACTIVE disable/delete minted and consumed that proof with exact replay and dependency retention. The separate zero-proof emergency path proved a genuinely opaque live body and exact retained authority. Future index-name validation and the create/active-update/enable gates were still pending at this checkpoint |
| Runtime activation at this checkpoint | unchanged in that slice | `compiled_query_execution.go` and `knowledgesnapshot.Authority.Finalize` retained separate nonempty hard gates; production still supplied no resolver, routes, capability, navigation, or supported ACTIVE publication at this checkpoint |
| Local durability | pass | forty-nine intentional KO-1C commits from `5088427` through `70b9ef6` inclusive are separate and locally durable on `codex/knowledge-objects-runtime`; this checkpoint update is a separate documentation commit |
| Remote durability | pending | `origin/main` remains `c5440b9` and `origin/codex/knowledge-objects-runtime` remains `7503246`; no push was attempted without explicit destination approval |

Current candidate-validation and dormant Preview request-boundary evidence:

| Gate | Result | Evidence |
| --- | --- | --- |
| Candidate normalization issue seam | pass | focused `internal/knowledgedefinition` normal, race, and vet gates plus its `knowledgeprogram` consumer passed; tests pin fail-fast field paths/codes, recursive unknown fields, every structural preflight, exact canonical-size boundary, detachment, wrapping, legacy text, and `errors.Is` parity |
| Candidate semantic issue seam | pass | focused `internal/knowledgeprogram` tests pin index-bound detachment/wrapping, regex syntax/resource/capture issues, UTF-8 JSON ranges, calculated `SPL_*` diagnostics, Boolean-result attribution, legacy error/sentinel parity, singleton guidance, and opaque Prepare/authority/aggregate/cohort failures |
| Validation/Preview protobuf generation and wire | pass with declared compatibility waiver | `make proto` produced the generated Go/TypeScript request comments without changing Preview field numbers, types, or presence encoding; descriptor/source tests pin canonical fields 1–6. Go and TypeScript contract oracles preserve create tags `[1, 2]` and all six present-empty update tags, while the Go structural and TypeScript wire oracles preserve MaxUint32. Root Go contracts, TypeScript typecheck/lint, and the frontend compatibility suite passed. The historical FILE waiver remains exact: draft result fields 6/7 and resource field 11 were retired before Validate registration, never served by either route, and remain reserved |
| Pure validation result construction | pass | focused `internal/knowledgevalidation` tests pin typed-only closed issue mapping, opaque inactive/active states, submitted-scalar range rebasing and private provenance, transition-supplied dependency canonicalization, exact inactive/active resources, recursive unknown-field rejection, detachment, exact deterministic wire-size comparison at an injected test bound, and the production 8 MiB cap |
| ACTIVE candidate transition | pass | focused normal and race coverage plus the three-second alpha-invariance fuzz gate pin complete candidate-absent baseline validation before conflict attribution, deterministic stronger-conflict precedence, opaque non-target conflicts, generic target absence, detached candidate dependencies, index-admission closure reuse, and response invariance across fresh evaluation-ID renames |
| Rollback-only Writer validation | pass | focused `ValidateKnowledgeObjectRequest`/`WriterValidate`/`ValidationBoundary`/`FinishValidation` normal and race tests, the full `internal/knowledgecatalog` package, `internal/knowledgevalidation`, and `go vet` pass. Tests pin independent read/write app authority, shared fail-fast admission, selected-view/witness amplification bounds, root/version/lifecycle/opaque precedence, inactive versus active inventory behavior, deterministic fresh identities, authorization-safe target projection, revision-zero physical emptiness, strict rollback-before-seal, cancellation, response taxonomy, detachment, and absence of mutation collaborators or DML |
| Bounded validation transport | pass | focused normal/race tests and bounded differential fuzzing pin protobuf merge/oneof equivalence, exact raw N/N+1 body limits, body close and input detachment, UTF-8 and 32-level group policy, authority-sensitive unknown retention, exact sealed bytes, context/permit failure handling, and bounded retention/allocation for one million mask paths, selected/unselected selector patterns, regex outputs, and body alternations |
| Shared candidate request decoder | pass, Validate behavior preserved | the candidate walker/builders are extracted behind envelope-specific field layouts. Existing Validate normal/race/fuzz coverage and Preview differential tests pin correct-wire object-ID mode selection, duplicate definition/mask merge, last-scalar and optional-presence semantics, selected/unselected projection, UTF-8 checks after caps or overwrites, the create/update unknown-authority split, malformed wire, and group depth 32/33 |
| Bounded Preview request transport | pass, unregistered | focused tests and bounded differential fuzzing pin the literal `4259840`-byte raw N/N+1 limit, body close, input detachment, retained-job scalar with a 256-byte exact value or detached 257-byte over-limit witness, at most 9 mask paths, 17 entries per selected selector dimension, and 17 selected regex outputs, depth 32/33 behavior, every-recognized-string UTF-8 validation, exact unknown-authority split, million-entry cardinalities, alternating oneofs, and bounded outer-unknown copies. A real handler fixture proves the route count stays nine and `/api/v1/knowledge/objects/preview` returns 404 without authentication, body read, or attempt journal activity |
| Preview structural envelope | pass, no service | focused tests prove nil-request rejection, outer/mask unknown rejection including wrong-wire envelope fields, a nonempty valid-UTF-8 job ID of at most 256 bytes unchanged by whitespace trimming and free of Unicode control code points, exact Validate create/update parity under server-forced `ACTIVE_PUBLICATION`, candidate-unknown authority preservation, no retained-job lookup or authorization, and no caller-protobuf mutation or normalization. `maximum_rows` preserves absent, zero, one, and MaxUint32 as full optional uint32 wire authority while assigning no default, bound, or execution meaning |
| Validation HTTP adapter | pass | focused normal/race tests pin exact-ready-Writer enforcement, serialization admission before retained authority, detached request binding, cloned read/write scope, valid HTTP 200 seals, closed error/disposition classification, create/update authorization context, cancellation, response-too-large handling, permit transfer/release, and request mutation isolation |
| Registered validation route | pass, exposure closed | real HTTP tests pin ninth-route all-or-none configuration, authentication and administrator rejection before body read, `ActionValidate` journaling/fail-closed journal failures, shared Writer-gate 429 behavior, million-entry selected versus unselected outcomes, outer/mask/selected-nested/unselected unknown semantics, exact sealed responses, and no side effects. The exact protobuf route fixture now contains 60 routes; TypeScript declares Validate with an 8 MiB response cap while explicitly keeping it outside browser bearer attachment, and the backend generic outer administrator map remains unchanged |
| Validation result bounds and privacy | service and route pass | descriptor/comment, Go/TypeScript wire, catalog-service, codec, handler, and HTTP tests pin presence-sensitive create/update mode, MaxInt64, explicit intent, no create-ID reservation, fresh-ID alpha-invariance with later Create revalidation, advisory valid/no-op/hypothetical-inactive semantics, knowledge-ledger-only revision correlation, singleton intrinsic charges including fields 12/13/14, full-transition candidate dependencies, exact count/text/8 MiB ceilings, error-first deterministic diagnostics, Unicode source coordinates, recursive unknown-output rejection, and nondisclosure rules |
| Runtime activation | Validate route plus dormant Preview request boundary only | Validate is the ninth registered administrator route and is capability-unadvertised. Preview has only an internal request codec and envelope validator: it has no response codec, handler, service, retained-execution acquisition or caller-auth integration, route, manifest/bearer entry, capability, browser UI/navigation, Resolver attachment, or execution path. Service work remains blocked on owner-scoped retained-execution reacquisition, fixed-catalog ACTIVE evaluation and program application, row-limit default/bound/execution policy, paired schema-row/truncation and response resource semantics, plus the closed nonempty compiler, snapshot-finalization, and digest-pinned ClickHouse gates. Validate remains outside the browser bearer allowlist and generic outer administrator map |
| Docker-backed acceptance | not run; prior cancellation preserved | these commits refactor bounded request decoding, add dormant Preview request validation, and harden its wire-neutral request contract without ClickHouse execution. The previously canceled digest-pinned Docker matrix remains canceled; no Docker command was invoked and no ClickHouse runtime claim is made |
| Local durability | pass | validation work through route checkpoint `eec63ee`, historical waiver clarification `d2a57cd`, bounded Preview transport `2db17c3`, structural envelope validation `ca9c2aa`, documentation checkpoint `da56ce4`, and request-authority hardening `74df953` is separately durable on `codex/knowledge-objects-runtime`; `git rev-list --count c5440b9..74df953` is exactly 129 and the terminal revision is `74df953f6f317b651f7bde97c0370fc37822a756` |
| Remote durability | pending | `origin/main` remains `c5440b9` and the remote feature branch remains `7503246`; no push was attempted without explicit destination approval |

The exact KO-0E final retained-log race command was:

```sh
zsh -o pipefail -c 'GOCACHE=/private/tmp/open-splunk-ko-go-race-cache go test -json -race -shuffle=on -covermode=atomic -coverprofile=/private/tmp/open-splunk-ko-writer-final-coverage.out -timeout=30m ./... 2>&1 | tee /private/tmp/open-splunk-ko-writer-final-race.jsonl'
```

It ran from `/Users/suhaib/code/open-splunk` outside the sandbox so existing
localhost/filesystem-policy tests could execute. `pipefail` preserved the Go
test exit status through `tee`.

The exact KO-0G decisive retained-log race command was:

```sh
zsh -o pipefail -c 'GOCACHE=/private/tmp/open-splunk-ko-go-race-cache go test -json -race -shuffle=on -covermode=atomic -coverprofile=/private/tmp/open-splunk-ko0g-final-green-coverage.out -timeout=30m ./... 2>&1 | tee /private/tmp/open-splunk-ko0g-final-green-race.jsonl'
```

It ran from the same working directory and with the same required local
execution permission. The retained control, catalog, and server shuffle seeds
are `1786177325841385000`, `1786177332043603000`, and
`1786177355421150000`, respectively.

The generated-tree hash and tracked-Go formatting checks used these exact
recipes:

```sh
git ls-files -z -- gen | LC_ALL=C sort -z | xargs -0 shasum -a 256 | shasum -a 256
git ls-files -z -- '*.go' | xargs -0 gofmt -l
```

For KO-0C, the first sandboxed full-suite attempt failed only where existing
tests bind loopback sockets or exercise host ACL behavior. The identical suite passed with
the required local execution permission. KO-0C changed SQLite migrations and
therefore exercised fresh, upgrade, rollback, corruption, recovery-adjacent,
recursive-trigger, concurrency, and capacity behavior. It did not add browser
or ClickHouse runtime behavior, so Docker-backed ClickHouse and browser
verticals were not applicable. No licensed Splunk oracle was available or
used; this checkpoint makes no differential-equivalence claim.

The following KO-0D through KO-0H paragraphs preserve the evidence boundary at
each historical checkpoint. Their then-current route, resolver, and publication
state is superseded by the current-state summary at the top of this document.

At the KO-0D checkpoint, that slice added no server route, browser surface,
resolver, search admission, or ClickHouse knowledge execution. Browser and
Docker-backed knowledge verticals were therefore future hard gates rather than
skipped acceptance evidence. Existing frontend tests and ClickHouse package
tests passed, the feature stayed hard-disabled, and no licensed Splunk oracle
was available.

At the KO-0E checkpoint, that slice added an internal Writer library but
registered no production route, advertised no capability, rendered no
Knowledge Manager UI, constructed no search snapshot, and executed no
ClickHouse knowledge operator. Browser and Docker-backed knowledge verticals
were therefore inapplicable to that slice, not silently waived.

At the KO-0F checkpoint, that slice implemented and integration-tested the six
management handlers, codecs, authentication split, and rejected-attempt
boundary but deliberately kept their route registrations in test code only.
The production router, exact API inventory, capability response, and browser
navigation were unchanged; real public browser and ClickHouse knowledge
verticals were therefore still future hard gates rather than claimed evidence.

At the KO-0G checkpoint, that slice constructed and validated the internal
active resolver and opaque snapshot authority but did not attach either to
`searchjobs.Manager`, persist snapshot metadata in history/inspection/export,
compile a knowledge prelude, register a management route, or advertise the
capability. Browser and Docker-backed knowledge-execution verticals were
therefore future hard gates; the shared Go/TypeScript snapshot fixture was
wire/digest evidence, not a claim that the browser could request or execute
knowledge.

At the KO-0H checkpoint, that slice implemented the optional manager seam,
enabled-empty sealing, lifecycle provenance, retained inspection/export
authority, and hidden browser shell, while production composition supplied no
resolver, registered no management route, and advertised no capability. The
shell's component/static tests were readiness evidence, not a live browser
vertical. Because nonempty finalization was rejected and there was no knowledge
prelude, Docker-backed ClickHouse knowledge execution and licensed-Splunk
differential tests remained KO-1 hard gates rather than silently skipped
acceptance evidence.

## Next dependency-ordered work

KO-0 foundations and the hidden KO-0H lifecycle/browser readiness vertical are
complete. KO-1A/KO-1B freeze the selector and backend-neutral program, and the
current closed-gate KO-1C slice lowers, accounts, seals, retains, reconstructs,
and inspects that program without making it executable. Recognized ACTIVE
publication and the nine management routes are now implemented independently of
search-time capability exposure. The dormant Preview request codec and
structural forced-ACTIVE envelope validator do not alter that order: without a
retained-execution reacquisition/application and caller-authorization service,
frozen row/response policy, response codec, handler, route, or open execution
gates they authorize no preview behavior. The next
dependency-ordered slices are:

1. complete the digest-pinned ClickHouse acceptance matrix as bounded named
   gates covering ordinary filter/project/eval/rex/spath/sort/limit/stats,
   eventstats/streamstats and stacked chronological barriers, chart/timechart,
   every event-analysis finalizer, empty/pruned consumers, exact argument
   order, resource boundaries, container decoding, and hidden-failure
   atomicity;
2. only after those runtime gates pass, remove the compiler and snapshot
   nonempty gates together and attach the retained concrete Resolver to
   production search admission, then prove one hidden seeded-ACTIVE lifecycle
   across search, history rerun, inspection, and export with exact retained
   compiler/program equality; and
3. only after the complete hidden Tier-1 browser/ClickHouse vertical passes,
   advertise the capability and enable Knowledge Manager navigation and
   mutation workflows. The nine administrator routes remain registered; the
   four catalog/graph read routes are browser bearer-allowlisted, while Validate
   and all mutation routes remain excluded, and the dormant read-only UI remains
   hidden until that exposure decision. Preview service, response codec, and
   route work remain deferred until owner-scoped retained execution can be
   reacquired under the authenticated caller; forced-ACTIVE validation can
   yield and apply the exact candidate program; row-limit, paired schema/row,
   truncation, response-byte, deadline, and concurrency policy is frozen; and
   the hidden nonempty compiler, snapshot-finalization, and digest-pinned
   ClickHouse gates pass without reopening any execution or disclosure gate.
