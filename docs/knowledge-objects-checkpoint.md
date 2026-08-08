# Knowledge objects implementation checkpoint

**Goal status:** active
**Current milestone:** KO-0 catalog runtime prerequisites
**Last completed slice:** KO-0E atomic inactive catalog Writer foundation
**Evidence date:** August 7, 2026

## Durable checkpoint

- Published KO-0 foundation: inclusive `00c88c1` through `b7ac77b`
- KO-0D local implementation and checkpoint range: `c5440b9..e9af86d`
- KO-0E local implementation range: `e9af86d..a8000e7`
- KO-0E implementation terminal revision:
  `a8000e7ebd0b118ad313c3315cc16e18ae62f44f`
- Branch: `codex/knowledge-objects-runtime`
- Publication state before this document: thirteen intentional post-`c5440b9`
  KO commits through `a8000e7` are durable locally. This checkpoint is kept as
  a separate documentation commit. An
  attempted push to `git@github.com:Suhaibinator/open-splunk.git` was blocked
  by the execution environment's unverified-destination egress policy. No
  bypass was attempted; explicit approval for that exact destination is still
  required before this branch can be published.
- Scope: lifecycle- and commitment-aware protobuf contracts, canonical known
  and inactive-future definitions, migrations 0029–0030 state/commit
  authorities, a bounded read-only `Get`/`List` catalog, and an atomic Writer
  for draft create, draft/disabled definition update, draft/active disable, and
  draft/active/disabled delete with exact replay,
  optimistic versions, immutable bodies/versions/dependencies/projections,
  successful audit, revision/state-token rotation, bounded health/reclamation,
  and crash-safe recovery.
- Runtime feature state: the capability remains hard-disabled and unadvertised;
  knowledge-management routes are not implemented or registered.
  Rejected-attempt integration, resolver/snapshot construction, search
  admission, UI, ACTIVE publication, and ClickHouse knowledge execution are not
  implemented.

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

The separate pre-existing dependency commit `fdcc17e` is also present in the
published `main` history. Unrelated commits between KO checkpoints are excluded
from the ranges above; no history rewrite or alternate publication branch was
used.

## Contract decisions frozen

- Binary case-sensitive identities with a stable ASCII-only trim/control rule
- Private → current-app → tenant-global shadow precedence
- Administrator-only, tenant-bound v0.1 management APIs
- Server-authoritative index scope and per-row trusted metadata selectors
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
- One-transaction coherent resolver reads and deterministic snapshot digest
  framing
- Current-policy response redaction for retained provenance and inspection
- Binary UTF-8 substring and individual-selector-pattern filters applied before
  keyset `LIMIT` at one catalog revision
- Exact sealed current-version projection verification, canonical selector
  framing, and a 256 MiB per-tenant projection byte ceiling
- Historical Get authorized from the current identity, with permanent
  definition redaction while the current identity is quarantined
- Atomic successful-mutation audit metadata and a separate fail-closed rejected
  privileged-attempt journal that retains no unauthorized object metadata
- Catalog, resolver, cache, selector, expression, snapshot, and audit ceilings
- Terminal corruption quarantine with bounded dependent closure, protected
  version/idempotency capacity, and one recovery-audit slot for every lifetime
  physical identity

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

The exact final retained-log race command was:

```sh
zsh -o pipefail -c 'GOCACHE=/private/tmp/open-splunk-ko-go-race-cache go test -json -race -shuffle=on -covermode=atomic -coverprofile=/private/tmp/open-splunk-ko-writer-final-coverage.out -timeout=30m ./... 2>&1 | tee /private/tmp/open-splunk-ko-writer-final-race.jsonl'
```

It ran from `/Users/suhaib/code/open-splunk` outside the sandbox so existing
localhost/filesystem-policy tests could execute. `pipefail` preserved the Go
test exit status through `tee`.

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

KO-0D likewise adds no server route, browser surface, resolver, search
admission, or ClickHouse knowledge execution. Browser and Docker-backed
knowledge verticals therefore remain future hard gates rather than skipped
acceptance evidence. Existing frontend tests and ClickHouse package tests pass,
the feature stays hard-disabled, and no licensed Splunk oracle was available.

KO-0E adds an internal Writer library but still registers no production route,
advertises no capability, renders no Knowledge Manager UI, constructs no search
snapshot, and executes no ClickHouse knowledge operator. Browser and
Docker-backed knowledge verticals therefore remain inapplicable to this slice,
not silently waived. The next route slice must add authenticated rejected-attempt
journaling before any management API can become reachable.

## Next dependency-ordered work

The migration, audit schema/primitives, definition, read-only catalog, and
internal pre-compiler Writer hard gates are complete. Route-level rejected
audit, resolver/snapshot construction, and active compilation remain required.
The next slice continues KO-0 in dependency order:

1. implement administrator-only protobuf HTTP handlers/codecs for `Get`,
   `List`, `Create`, `Update`, `SetState`, and `Delete` behind disabled route
   registration; split authentication from administrator
   authorization so every authenticated rejection receives an explicit actor,
   derive tenant/owner/app only from the validated principal and trusted app
   catalog, carry detached authorized rejection context from the Writer, and
   return a generic unavailable response whenever the separate rejected-attempt journal
   cannot commit;
2. add route-level matrices for unauthenticated access, non-administrator
   attempts, hidden/missing/corrupt objects, stale versions, idempotency
   conflicts, capacity failures, malformed bodies, rejected-read and
   rejected-mutation journal exhaustion/failure, successful-audit rollback,
   and stable protobuf HTTP error mappings while keeping route registration and
   capability advertisement disabled;
3. implement the one-transaction resolver and detached immutable snapshot
   builder, including precedence, shadow inventory, authorized-index pinning,
   canonical cross-language digest framing, cache bounds, and old-or-new
   admission races;
4. add the hidden Knowledge Manager list/detail shell plus negative API,
   capability-advertisement, route-registration, and navigation tests; keep the
   feature unadvertised and navigation absent while the runtime is incomplete;
   and
5. integrate snapshot persistence into search lifecycle/provenance before
   beginning KO-1 field execution; then supply the executable publication
   compiler and dependency derivation required to replace
   `ErrActivePublicationUnavailable`. Keep ACTIVE publication and the feature
   flag disabled until the complete Tier-1 vertical and its browser and
   ClickHouse acceptance tests pass.
