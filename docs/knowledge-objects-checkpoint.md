# Knowledge objects implementation checkpoint

**Goal status:** active
**Current milestone:** KO-0 catalog runtime prerequisites
**Last completed slice:** KO-0C bounded projections and audit foundations
**Evidence date:** August 7, 2026

## Durable checkpoint

- KO implementation range: `00c88c1..b7ac77b`
- KO-0C terminal revision:
  `b7ac77b1a9fc28b282e72e7153f7b26739a35629`
- Current repository and published revision:
  `8d8e5cd068c906680dc67055c964ba6adfb13a63`
- Branch and remote: `main`; local `main` and `origin/main` are identical
- Scope: the proposed end-to-end plan, normative Tier 1 compatibility
  contract, protobuf contracts, bounded selector primitives, immutable catalog
  schema, bounded current-version list projections, successful-mutation audit
  taxonomy, and rejected privileged-attempt journal
- Runtime feature state: unimplemented and unadvertised

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

The separate pre-existing dependency commit `fdcc17e` is also present in the
published `main` history. No history rewrite or alternate publication branch
was used.

The subsequent `8d8e5cd` (`publish backend and collector images`) commit is
unrelated to knowledge objects and is excluded from KO-0C scope. Any current
`scripts/build-oci.test.mjs` worktree change also belongs to that separate
image-publication work and is neither staged nor modified by this checkpoint.

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

The first sandboxed full-suite attempt failed only where existing tests bind
loopback sockets or exercise host ACL behavior. The identical suite passed with
the required local execution permission. KO-0C changed SQLite migrations and
therefore exercised fresh, upgrade, rollback, corruption, recovery-adjacent,
recursive-trigger, concurrency, and capacity behavior. It did not add browser
or ClickHouse runtime behavior, so Docker-backed ClickHouse and browser
verticals were not applicable. No licensed Splunk oracle was available or
used; this checkpoint makes no differential-equivalence claim.

## Next dependency-ordered work

The migration and audit hard gates are complete. The next slice implements the
administrator-only catalog and validation runtime in dependency order:

1. add store-level Create/Get/List/Update/Delete with exact idempotency,
   optimistic version matching, current-policy historical authorization,
   quarantine non-disclosure, immutable body/version publication, and bounded
   pre-LIMIT filters;
2. integrate successful mutations and every authenticated rejected privileged
   attempt into their respective caller-owned transactions;
3. add protobuf HTTP routes that derive tenant, owner, and administrator state
   only from the validated browser principal and never from request fields;
4. exercise fresh/upgrade/crash/corruption/concurrency/race/resource and
   adversarial authorization tests before any capability is advertised; and
5. keep ACTIVE publication and the feature flag disabled until the complete
   Tier-1 parser/compiler/resolver family is present and proven.
