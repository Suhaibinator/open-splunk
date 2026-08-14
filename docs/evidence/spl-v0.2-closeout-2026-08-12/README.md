# SPL compatibility v0.2 reachable-target closeout

**Evidence state:** incomplete; acceptance remains open

This attempt is immutable negative/checkpoint evidence. Its schema and
verifier deliberately require `incomplete`/`pending`; it must not be edited
into an accepted bundle. A successful v0.2 runtime revision requires a new
accepted sibling bundle that records the qualified runtime/release revision
and terminal-success receipts.

This sibling bundle supersedes the pending-target decision in
[`../spl-v0.2/`](../spl-v0.2/) without modifying that historical bundle. The
historical evidence is bound to runtime target
`dffde13c84d9a2ef0567e89dd527ec4776f5ca42` and GitHub Actions run
[`31540820337`](https://github.com/Suhaibinator/open-splunk/actions/runs/31540820337).
That older run completed at `2026-08-11T22:35:13Z` with conclusion
`cancelled`; it therefore cannot supply terminal-success acceptance
provenance.

Its documented placeholder-tolerant verifier also does not pass in the current
carrier: the contract/corpus have moved, and the committed frontend and release
receipts are three bytes shorter than their manifest declarations. These are
additional reasons to supersede the bundle instead of finalizing its pending
fields in place.

This closeout attempt binds the reachable `origin/main` target
`230774476dfd96c5e11ef87f7372b81986689353` and tree
`0da294a410c813656960ddc40242480d7b076525`. Remote readback on August 12,
2026 returned that exact revision for `refs/heads/main`.

A distinct, later target-bound GitHub Actions
[`CI` run `31571852036`](https://github.com/Suhaibinator/open-splunk/actions/runs/31571852036)
checked out that exact `origin/main` revision. It was created and started at
`2026-08-12T06:55:15Z`, and GitHub recorded its terminal update at
`2026-08-12T07:46:59Z`. The run completed with conclusion `failure`, not
success. Its
[`Backend vertical` job](https://github.com/Suhaibinator/open-splunk/actions/runs/31571852036/job/94035340634)
ran from `2026-08-12T06:57:34Z` through `2026-08-12T07:07:30Z` and failed in
the `Test backend vertical and deletion lifecycle` step. The logs record one
ClickHouse corpus failure and two queryexec failures. Although the separate
`Release OCI contract` job succeeded, `Production binaries` and
`Linux/macOS release asset consistency` were both skipped. This later run
proves target reachability, but it supplies negative—not terminal-success—CI
and release provenance.

## Semantic reconciliation

The stats-parity merge intentionally added exact single-quoted `stats`
aggregate and `sparkline` inputs, `stats BY` fields, literal double-quoted
`stats AS` outputs, and downstream single-quoted `table` references. Parser,
planner, compiler, suggestion, and product tests pin that behavior. The v0.2
contract and migration guide still described those slots as disabled, while
the committed corpus already expected quoted stats aggregate and BY parsing to
succeed.

The carrier changes reconcile the documents and corpus with the implemented
quoted-field behavior. They also make bounded scalar-member multivalue
`stats BY` an explicit v0.2 transition and add the atomic publication bit and
executor/manager evidence required for its runtime guards. That second part is
a production semantic hardening. Because neither the corrected artifacts nor
the production guard are part of the reachable target commit, final acceptance
must bind a new reachable committed revision containing both.

## Checks completed

The exact target was exercised from a clean isolated checkout. The following
checks passed and left `git status --porcelain=v1` empty:

- `go test ./... -count=1 -timeout=15m`
- focused `-race` tests for `internal/spl`, `internal/plan`,
  `internal/clickhouse`, `internal/queryexec`, `internal/searchjobs`,
  `internal/knowledgeprogram`, and `internal/knowledgesnapshot`
- the pinned v0.2 ClickHouse compiler integration test
- Node `v24.18.0` / npm `11.16.0` clean install, typecheck, lint, frontend
  tests, and production UI build
- v0.2 corpus/parity plus quoted-stats parser, planner, and compiler tests
- focused compatibility-audit package and command tests

The working-tree carrier also passed the v0.2 corpus schema/rule-parity and
parser-expectation tests after the documentation/corpus reconciliation.

These local results are regression evidence, not a release decision. The
machine-readable status, identities, hashes, and remaining gates are in
[`manifest.json`](manifest.json).

## Remaining acceptance gates

Acceptance remains open until one new committed reachable revision containing
the reconciled contract/corpus passes, on that exact clean revision:

1. the complete command sheet in
   [`../../spl-compatibility-v0.2-acceptance.md`](../../spl-compatibility-v0.2-acceptance.md),
   including any target-bound gates not preserved as receipts here;
2. a fresh deterministic read-only audit and reviewed disposition ledger;
3. the pinned release build with exact Node/npm/Go versions and artifact
   source-identity readback;
4. terminal-success CI with checkout/tree and release-artifact readback,
   superseding both the older cancelled run and the later reachable failed
   run; and
5. final clean-tree proof and evidence checksum index.

No pending value in this bundle is a claim of success.

Validate the internal identities and deliberate incomplete state with:

```sh
node docs/evidence/spl-v0.2-closeout-2026-08-12/verify.mjs
```
