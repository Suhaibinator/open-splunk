# Knowledge objects implementation checkpoint

**Goal status:** active
**Current milestone:** KO-0 catalog runtime prerequisites
**Last completed slice:** KO-0B protobuf, selector, and SQLite foundations
**Evidence date:** August 6, 2026

## Durable checkpoint

- Base commit: `8880ef086eaa7671f5d0c821de552c955c3af1d3`
- Checkpoint commit: `00c88c19cbdab71d1fbe0c71c5c3552a7f794f38`
- Branch and remote: `main`; KO-0A is pushed to `origin/main`
- Commit subject: `docs(knowledge): define v0.1 compatibility contract`
- Scope: the proposed end-to-end plan, normative Tier 1 compatibility
  contract, version constant, strict starter compatibility inventory, and its
  structural test
- Runtime feature state: unimplemented and unadvertised

## KO-0B local durable commits

The following intentional commits are present on local `main`:

| Commit | Subject | Scope |
| --- | --- | --- |
| `8a25098` | `feat(knowledge): reserve KO-0 protobuf contracts` | Typed Tier-1 definitions, CRUD/validation/dependency/preview messages, immutable snapshot/provenance wire contracts, generated Go/TypeScript, and a hard-disabled capability guard |
| `c23889b` | `feat(knowledge): add bounded selector primitives` | Stable text/destination normalization, 17-segment search-field parsing, canonical selectors, combined NFA matching, cumulative resource accounting, cancellation, race tests, and four fuzz targets |
| `99f42d5` | `feat(knowledge): add immutable catalog schema` | Forward-only SQLite migration 0024, content-addressed bodies, exact registry/version agreement, dependency seals, capacity reserves/counters, idempotency, quarantine recovery audit, app/dependent guards, rollback and migration tests |

These commits are not yet on `origin/main`. Push was attempted after `8a25098`
and rejected by the execution approval boundary because local `main` also
contains the separate pre-existing commit `fdcc17e` (`chore: bump dependencies
in go.mod and go.sum`). Pushing `main` necessarily publishes that commit too;
explicit user approval after disclosure is required. No history rewrite,
alternate branch, or partial-push workaround was attempted.

The checkpoint intentionally did not stage or modify the pre-existing unified
image-publication worktree changes in:

- `.github/workflows/publish-collector-image.yml`
- `.github/workflows/publish-images.yml`
- `README.md`
- `deploy/README.md`
- `docs/collector-deployment.md`
- `scripts/build-oci.test.mjs`

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
| KO-0B remote durability | blocked | requires approval to include pre-existing local `fdcc17e` in the `main` push |

The first sandboxed full-suite attempt failed only where existing tests bind
loopback sockets or exercise host ACL behavior. The identical suite passed with
the required local execution permission. No Docker, ClickHouse, browser,
migration, recovery, or frontend runtime behavior changed in this contract-only
slice, so those gates were not applicable. No licensed Splunk oracle was
available or used; this checkpoint makes no differential-equivalence claim.

## Next dependency-ordered work

Before CRUD runtime work, additive migration 0025 is a hard gate:

1. add a bounded current-version description/selector search projection so
   list filters run before keyset `LIMIT` without decoding up to the 512 MiB
   definition-body tenant budget;
2. transactionally extend the existing 100,000-row general audit journal and
   protobuf/Go closed taxonomy for ordinary successful knowledge mutations;
3. add a separate bounded, fail-closed authenticated rejected-attempt journal;
4. pin binary-substring filter semantics, the deleted/historical Get policy,
   and exact projection-to-definition integrity checks; and
5. only then implement the administrator-only catalog/validation runtime. The
   full Tier-1 compiler remains mandatory for ACTIVE publication, and routes
   and the feature flag remain absent until the complete family exists.
