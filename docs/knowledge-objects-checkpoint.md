# Knowledge objects implementation checkpoint

**Goal status:** active
**Current milestone:** KO-0 contracts and catalog foundation
**Last completed slice:** KO-0A compatibility contract
**Evidence date:** August 6, 2026

## Durable checkpoint

- Base commit: `8880ef086eaa7671f5d0c821de552c955c3af1d3`
- Checkpoint commit: `00c88c19cbdab71d1fbe0c71c5c3552a7f794f38`
- Branch and remote: `main`, pushed to `origin/main`
- Commit subject: `docs(knowledge): define v0.1 compatibility contract`
- Scope: the proposed end-to-end plan, normative Tier 1 compatibility
  contract, version constant, strict starter compatibility inventory, and its
  structural test
- Runtime feature state: unimplemented and unadvertised

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

The first sandboxed full-suite attempt failed only where existing tests bind
loopback sockets or exercise host ACL behavior. The identical suite passed with
the required local execution permission. No Docker, ClickHouse, browser,
migration, recovery, or frontend runtime behavior changed in this contract-only
slice, so those gates were not applicable. No licensed Splunk oracle was
available or used; this checkpoint makes no differential-equivalence claim.

## Next dependency-ordered work

KO-0B is the next unblocked slice:

1. add common knowledge, selector, dependency, provenance, snapshot, and CRUD
   protobuf contracts without advertising the runtime feature;
2. generate Go and TypeScript bindings and update exact route/forward-
   compatibility fixtures;
3. add the first forward-only SQLite catalog migration with immutable body,
   registry, version, dependency, ACL-ready, revision, idempotency, recovery-
   audit, and capacity-counter tables; and
4. begin typed normalization and corruption tests against those contracts.

Protobuf and SQLite work can proceed in parallel after this frozen identity and
precedence contract. Snapshot/job integration remains blocked on both.
