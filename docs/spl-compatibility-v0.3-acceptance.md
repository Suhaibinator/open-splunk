# SPL compatibility v0.3 acceptance report

**Acceptance phase:** `implementation-checkpoint`

**Status:** implementation checkpoint; activation provenance pending

**Target authored-search identity:** `0.3`

**Knowledge-expression identity:** `0.1`

**v0.2 prerequisite:** pending

**Activation decision:** pending; distribution blocked

**Prepared:** August 12, 2026

This report records reproducible implementation evidence for the complete
v0.3 command surface. It is intentionally not an activation claim. During the
implementation checkpoint the stable public runtime remains `0.2`. A
`qualification-candidate` revision `R` embeds `0.3` only for quarantined
qualification after the v0.2 prerequisite has closed; neither state authorizes
stable publication. Only the later verified, documentation-only evidence
revision `E` can authorize distribution.
An explicitly labeled synthetic local `E` exercises this protocol only and is
not remote or publication authority.

Normative artifacts:

- [`v0.3 compatibility contract`](spl-compatibility-v0.3.md)
- [`v0.3 machine-readable inventory`](../internal/spl/testdata/compatibility-v0.3.json)
- [`v0.3 migration guide`](spl-compatibility-v0.3-migration.md)
- [`v0.3 implementation plan`](spl-compatibility-v0.3-plan.md)
- [`v0.3 activation evidence protocol`](evidence/spl-v0.3/README.md)
- [`v0.3 activation manifest`](evidence/spl-v0.3/manifest.json)

## Candidate identity

| Field | Value |
| --- | --- |
| Reachable base revision | `230774476dfd96c5e11ef87f7372b81986689353` (`origin/main` read back August 12, 2026) |
| Candidate v0.3 revision | derived from the containing clean checkout in `qualification-candidate` phase; remote readback is recorded only by `E` |
| Candidate tree | derived from that containing commit; recorded explicitly only by `E` |
| Required public identity | `0.3` in qualification-candidate `R`; stable advertisement and distribution only after every acceptance gate passes |
| Required application identity | `0.2.0`; exact release tag `v0.2.0` at `E` |
| Required knowledge identity | `0.1` |
| Go | `go1.26.5 darwin/arm64` |
| Node.js required by lockfile | `v24.18.0` |
| npm | `11.16.0` |
| Docker client/server | `29.7.2` / `29.7.2` |
| ClickHouse | `26.3.17.4` |
| Pinned ClickHouse image | `clickhouse/clickhouse-server:26.3.17.4@sha256:85c434814ac8905e5648027ce926f74ab067edd6aadbccb6c0c165cd3571ea49` |

Acceptance uses two immutable revisions because a Git commit cannot contain
its own final object identity or the terminal CI result produced after that
identity exists:

1. The runtime/release revision `R` changes the authored identity to `0.3` in
   the same commit that changes this report and the activation manifest to
   `qualification-candidate`. That phase is deliberately **not accepted** and
   remains stable-publication-blocked. The exact `R` object is derived from the
   committed checkout; it is not guessed or embedded in itself. Tests, CI,
   remote readback, and quarantined release artifacts then qualify that exact
   object and its exact `0.3` identity.
2. The direct-child evidence revision `E` is documentation-only. It records
   `R`, its tree and artifact hashes, terminal-success CI, remote readback,
   release identity readback, exact test intervals, and checksummed sanitized
   receipts. Only `E` changes the phase, status, and activation decision to
   `accepted`.

The `0.3` value in an `R` binary is therefore a candidate artifact identity,
not permission to publish or deploy that artifact through a stable release
channel. Retained CI outputs are quarantined qualification evidence. Public
distribution is authorized only after the strict accepted-phase publication
verifier passes for a reachable, non-synthetic remote `E`. `E` must have `R`
as its only parent, and its changed paths
are limited to this report, the activation manifest, and checksummed receipt
files. A runtime,
normative contract, corpus, migration, verifier, or schema change in `E`
invalidates the qualification and requires a new `R`. CI for `R` must reach a
terminal result before `E` is pushed because this repository cancels an older
in-progress run when a newer revision is published.

The machine-enforced transitions are:

| Phase | Runtime identity | v0.2 prerequisite | Decision | Distribution |
| --- | --- | --- | --- | --- |
| `implementation-checkpoint` | `0.2` | pending | pending | blocked |
| `qualification-candidate` (`R`) | `0.3` | accepted | pending | blocked |
| `accepted` (`E`) | `0.3` inherited unchanged from `R` | accepted | accepted | authorized only for verified non-synthetic remote evidence; synthetic/local remains blocked |

There is no accepted `0.2` state, no distributable candidate state, and no
accepted state whose parent was not a valid candidate. Run the verifier with
the expected phase; it rejects a dirty `R`/`E`, phase disagreement between the
report and manifest, missing provenance, and non-documentation changes in `E`:

```sh
node scripts/verify-spl-v03-acceptance.mjs --phase implementation-checkpoint --allow-dirty
node scripts/verify-spl-v03-acceptance.mjs --phase qualification-candidate
node scripts/verify-spl-v03-acceptance.mjs --phase accepted
node scripts/verify-spl-v03-acceptance.mjs \
  --phase accepted --evidence-revision <E> --print-evidence-binding
```

The third command is intentionally valid only at `E`; release publication must
run from that exact tagged checkout. The fourth command is the full-history CI
form for later descendants. It requires `E` to be an ancestor, replays the
accepted manifest/report/receipts and their candidate `R`, and returns the
machine binding `<E> <manifest-sha256>` without granting publication authority
to the descendant.

## Implementation evidence

These receipts were produced during the implementation run. They prove the
tested working-tree behavior but do not replace the final clean-revision rerun.

| Surface | Command or test identity | Result |
| --- | --- | --- |
| Complete Go suite | `go test ./... -count=1 -timeout=15m` | passed at an implementation checkpoint; a fresh post-fix rerun is required for `R` |
| Focused race suite | `go test -race ./internal/spl ./internal/plan ./internal/clickhouse ./internal/queryexec ./internal/searchjobs ./internal/server ./internal/searchsnapshot ./internal/searchinspection ./internal/searchanalysis ./internal/export -count=1 -timeout=15m` | passed at an implementation checkpoint; a fresh post-fix rerun is required for `R` |
| Parser, ranges, private fields, options, suggestions, shapes | `go test ./internal/spl -run 'TestV03' -count=1` excluding the deliberate activation gates | passed |
| Executable compatibility corpus | `TestV03CorpusFixturesAreStrictBoundAndExecutable`, hostile schema/binding tests, and `TestV03ReleaseArtifactsExistAndHaveExactRuleParity` | all 53 cases passed through 39 uniquely bound fixtures: 22 independent typed row-model programs and 17 exact production-evidence fixtures; the release gate derives and compares exact ordered schemas, binds exact authored pipelines, and correlates every evidence claim to its registered test assertions |
| Planner, read sets, field-analysis policy, timeline | `go test ./internal/plan -run 'TestV03|TestValidateTimeline' -count=1` | passed |
| Compiler and forged-plan defenses | `go test ./internal/clickhouse -run 'TestV03' -count=1` | passed |
| Runtime marker classification/redaction and optional List transport | `go test ./internal/queryexec -run 'TestV03' -count=1` | passed |
| Search-job validation, atomic publication, paging | `go test ./internal/searchjobs -run 'TestV03' -count=1` | passed |
| Saved-search and history API authority | `go test ./internal/server -run 'TestV03Saved|TestV03History' -count=1` | passed |
| Snapshot rebuild/version immutability | `go test ./internal/searchsnapshot -run 'TestV03|CompilerVersion' -count=1` | passed |
| Export, inspection, field catalog/summary, timeline analysis | focused `internal/export`, `internal/searchinspection`, and `internal/searchanalysis` suites | passed |
| Browser catalog/pipeline scanning | `lib/search/spl-syntax.test.ts` in frontend suite | passed during implementation |
| Exact frontend toolchain | Node `v24.18.0`, npm `11.16.0`: `npm ci`, critical production audit, typecheck, lint, frontend tests, and production build | frontend runner passed after wiring the CI structural and release-identity gates: 117 script tests and 304 frontend tests; exact Node 24 full-sheet rerun still required for `R` |
| Pinned live ClickHouse all-ten vertical | `OPEN_SPLUNK_CLICKHOUSE_INTEGRATION=1 go test ./internal/clickhouse -run '^TestV03AdversarialAgainstClickHouse$' -count=1 -timeout=15m -v` | earlier checkpoint passed in 96.62 s; a fresh post-fix rerun is required for `R`; exact image digest shown above |
| Pinned fillnull materialization compositions | `OPEN_SPLUNK_CLICKHOUSE_INTEGRATION=1 go test ./internal/clickhouse -run '^(TestV03FillNullDynamicThenMVExpandAgainstClickHouse|TestV03FillNullAfterPrivatePhysicalProducersAgainstClickHouse)$' -count=1 -timeout=15m -p=1 -v` | post-fix checkpoint passed both exact verticals against the pinned image: stored Dynamic through `mvexpand`/pivot barriers and private physical Dynamic/String/Time/Number/Bool producers through `fillnull`; a fresh exact-`R` rerun remains required |
| Pinned public paging/export vertical | `OPEN_SPLUNK_CLICKHOUSE_INTEGRATION=1 go test ./internal/export -run '^TestV03PinnedClickHousePublishesNullableListsThroughPagingAndExport$' -count=1 -timeout=15m -v` | passed in 6.95 s; real ClickHouse → executor → manager paging → JSONL export, including nullable multivalue, expanded nullable-String, and semantic-Bytes transport |
| Pinned retained-byte canonicalization | `OPEN_SPLUNK_CLICKHOUSE_INTEGRATION=1 go test ./internal/clickhouse -run '^TestV03PinnedClickHouseRetainedTupleHasCanonicalBytes$' -count=1 -timeout=15m -v` | passed in 5.94 s; exact public field order/names, optional `null` versus `[]`, Unicode/slash, UInt64, and Decimal bytes |
| Pinned semantic String/Bytes manager lineage | `OPEN_SPLUNK_CLICKHOUSE_INTEGRATION=1 go test ./internal/queryexec -run '^(TestSemanticBytesLineageManagerAgainstClickHouse|TestSemanticBytesModeManagerAgainstClickHouse|TestSparklineFeedsStatsByThroughManagerAgainstClickHouse)$' -count=1 -timeout=15m -p=1 -v` | passed in 18.17 s after the final sidecar fixes; real ClickHouse -> executor -> manager coverage for copy/rename/tostring/concat/if/case/coalesce/fillnull/strcat, nullable transport, mode identity/ties/text fencing/re-grouping, and sparkline fixed-String Bytes through downstream `stats BY` |
| Independent cumulative expansion marker | pinned live exact-boundary fixtures | exactly 15,000 cumulative rows passed; 15,001 selected the query-wide marker while each stage remained at or below 10,000 |
| Release identity readback plumbing | server `-verify-embedded-release` implementation and unit tests | now emits and tests exact `spl_compatibility_version` alongside application/source/UI identities; no current local `build/` artifact is acceptance evidence, and `R` still requires a fresh release build/readback |
| Candidate CI enforcement | `.github/workflows/ci.yml` `spl-compatibility-clickhouse` job and `scripts/spl-v03-ci.test.mjs` | terminal production-build gate now requires all-ten, fillnull materialization/private-producer compositions, public paging/export, canonical retained-byte, and semantic String/Bytes manager verticals against the pinned image |
| Activation protocol simulation | `scripts/spl-v03-acceptance.test.mjs` synthetic clean Git lineage | a strict accepted v0.2 prerequisite, clean v0.3 candidate `R`, direct-child documentation-only `E`, exact jobs/receipts/release identities, and candidate/accepted verifier invocations all passed without creating repository refs |

The live dataset covers regex missing/null/Unicode, reverse after head/tail/
dedup/aggregation and equal public ties, accum/delta numeric values, strcat
missing policy and byte ceilings, fillnull types/containers, addtotals finite
eligibility, addinfo immutable overwrite, Unicode/empty/null `makemv`, scalar/
list/null `mvexpand`, semantic-Bytes preservation and rejection boundaries,
every multivalue marker family, hard boundaries, repeated expansion,
cancellation, and an all-ten pipeline.

## Adversarial review and SDET

| Workstream | Run identity | Disposition |
| --- | --- | --- |
| Correctness/authority/resource audit | `/root/adversarial_review` and bounded child audits | Findings were source-located; ordering, SID authority, regex aggregation, fail-closed switches, optional-list and semantic-Bytes transport, cumulative expansion, version rebuild, saved-search authority, and timeline/analysis gaps were fixed with regressions. The latest checkpoint audit found no remaining confirmed v0.3 defect before the subsequent v0.2 stats-BY hardening; clean-revision release audit remains pending. |
| Dedicated parser/compiler/product SDET | `/root/adversarial_sdet` and child test-gap audit | Added weird syntax/range/Unicode/private-field cases, forged AST/plan tests, exact runtime markers, atomic prefix checks, product paging/API/export tests, and pinned synthetic ClickHouse data. Final clean-revision suite pending. |
| Runtime live-data matrix | `TestV03AdversarialAgainstClickHouse` | Complete post-fix live pass recorded, including fixed-schema `mvexpand`, canonical retained-byte settings/null reconstruction, exact cumulative expansion boundaries, malformed-value barriers, and downstream non-bypass cases. |

## Required final closeout

The exact candidate revision must run from a clean materialization:

```sh
test -z "$(git status --porcelain)"
git rev-parse HEAD
git rev-parse HEAD^{tree}
git ls-remote origin HEAD refs/heads/<candidate-branch>
shasum -a 256 \
  docs/spl-compatibility-v0.3.md \
  internal/spl/testdata/compatibility-v0.3.json

go test ./... -count=1 -timeout=15m
go test -race \
  ./internal/spl ./internal/plan ./internal/clickhouse \
  ./internal/queryexec ./internal/searchjobs ./internal/server \
  ./internal/searchsnapshot ./internal/searchinspection \
  ./internal/searchanalysis ./internal/export \
  -count=1 -timeout=30m
go mod tidy -diff
go vet ./...
go build ./...
golangci-lint run ./...

npm ci
npm audit --omit=dev --audit-level=critical
npm run typecheck
npm run lint
npm run test:frontend
npm run build

export OPEN_SPLUNK_CLICKHOUSE_TEST_IMAGE='clickhouse/clickhouse-server:26.3.17.4@sha256:85c434814ac8905e5648027ce926f74ab067edd6aadbccb6c0c165cd3571ea49'

require_exact_test() {
  package="$1"
  name="$2"
  test "$(go test "$package" -list "^${name}$" | awk -v n="$name" '$0 == n { c++ } END { print c + 0 }')" -eq 1
}
while read -r package name; do
  require_exact_test "$package" "$name"
done <<'EOF'
./internal/clickhouse TestStatsByDeferredValidationAdversarialAgainstClickHouse
./internal/clickhouse TestStatsMultivalueByAgainstClickHouse
./internal/clickhouse TestStatsMultivalueByExpansionLimitAgainstClickHouse
./internal/clickhouse TestStatsByUnsupportedCannotHideBehindMissingKeyAgainstClickHouse
./internal/queryexec TestStatsByFixedMultivalueStringOrBytesAgainstClickHouse
./internal/queryexec TestSemanticBytesV02ManagerAgainstClickHouse
./internal/clickhouse TestV03AdversarialAgainstClickHouse
./internal/clickhouse TestV03FillNullDynamicThenMVExpandAgainstClickHouse
./internal/clickhouse TestV03FillNullAfterPrivatePhysicalProducersAgainstClickHouse
./internal/export TestV03PinnedClickHousePublishesNullableListsThroughPagingAndExport
./internal/clickhouse TestV03PinnedClickHouseRetainedTupleHasCanonicalBytes
./internal/queryexec TestSemanticBytesLineageManagerAgainstClickHouse
./internal/queryexec TestSemanticBytesModeManagerAgainstClickHouse
./internal/queryexec TestSparklineFeedsStatsByThroughManagerAgainstClickHouse
./internal/queryexec TestV03AllTenPreserveUntouchedSemanticBytesThroughManagerAgainstClickHouse
EOF

OPEN_SPLUNK_CLICKHOUSE_INTEGRATION=1 \
go test ./internal/clickhouse \
  -run '^(TestStatsByDeferredValidationAdversarialAgainstClickHouse|TestStatsMultivalueByAgainstClickHouse|TestStatsMultivalueByExpansionLimitAgainstClickHouse|TestStatsByUnsupportedCannotHideBehindMissingKeyAgainstClickHouse)$' \
  -count=1 -timeout=15m -p=1 -v

OPEN_SPLUNK_CLICKHOUSE_INTEGRATION=1 \
go test ./internal/queryexec \
  -run '^(TestStatsByFixedMultivalueStringOrBytesAgainstClickHouse|TestSemanticBytesV02ManagerAgainstClickHouse)$' \
  -count=1 -timeout=15m -p=1 -v

OPEN_SPLUNK_CLICKHOUSE_INTEGRATION=1 \
go test ./internal/clickhouse \
  -run '^TestV03AdversarialAgainstClickHouse$' \
  -count=1 -timeout=15m -p=1 -v

OPEN_SPLUNK_CLICKHOUSE_INTEGRATION=1 \
go test ./internal/clickhouse \
  -run '^(TestV03FillNullDynamicThenMVExpandAgainstClickHouse|TestV03FillNullAfterPrivatePhysicalProducersAgainstClickHouse)$' \
  -count=1 -timeout=15m -p=1 -v

OPEN_SPLUNK_CLICKHOUSE_INTEGRATION=1 \
go test ./internal/export \
  -run '^TestV03PinnedClickHousePublishesNullableListsThroughPagingAndExport$' \
  -count=1 -timeout=15m -p=1 -v

OPEN_SPLUNK_CLICKHOUSE_INTEGRATION=1 \
go test ./internal/clickhouse \
  -run '^TestV03PinnedClickHouseRetainedTupleHasCanonicalBytes$' \
  -count=1 -timeout=15m -p=1 -v

OPEN_SPLUNK_CLICKHOUSE_INTEGRATION=1 \
go test ./internal/queryexec \
  -run '^(TestSemanticBytesLineageManagerAgainstClickHouse|TestSemanticBytesModeManagerAgainstClickHouse|TestSparklineFeedsStatsByThroughManagerAgainstClickHouse|TestV03AllTenPreserveUntouchedSemanticBytesThroughManagerAgainstClickHouse)$' \
  -count=1 -timeout=15m -p=1 -v

git diff --check
test -z "$(git status --porcelain)"
node scripts/verify-spl-v03-acceptance.mjs --phase qualification-candidate
OPEN_SPLUNK_APPLICATION_VERSION=0.2.0 \
OPEN_SPLUNK_EXPECTED_SPL_COMPATIBILITY_VERSION=0.3 \
OPEN_SPLUNK_SOURCE_REVISION="$(git rev-parse HEAD)" \
make release
test "$(sed -n '1p' build/release-verification.txt)" = \
  'application_version=0.2.0'
test "$(sed -n '2p' build/release-verification.txt)" = \
  "source_revision=$(git rev-parse HEAD)"
test "$(sed -n '3p' build/release-verification.txt)" = \
  'spl_compatibility_version=0.3'
```

The final report must replace every pending candidate field with the committed
revision/tree, clean-tree proof, contract/corpus SHA-256 values, exact test
intervals, release source identity, and remote readback. Logs must not contain
event values, SPL secrets, bound arguments, or generated SQL from failures.

## Open activation gates

| Gate | Current state |
| --- | --- |
| v0.2 acceptance prerequisite | checkpoint carrier: pending; materialized qualification candidate: verifier-bound accepted prerequisite, synthetic local fixture or real remote authority as explicitly labeled |
| Candidate committed/reachable revision | `R` is a local clean commit when materialized; remote reachability and readback remain an `E` gate |
| Clean candidate full Go/race/frontend/release sheet | qualification results are post-`R` evidence recorded only by `E` |
| Public compatibility identity `0.3` | embedded only in quarantined `R`; stable activation remains pending `E` |

## Decision

The implementation is not accepted for activation merely by this report. The
test and review evidence is sufficient to continue closeout, not to advertise
`0.3`. Phase `implementation-checkpoint` keeps identity `0.2`; phase
`qualification-candidate` gives exact `R` the quarantined identity `0.3` while
stable publication remains blocked. Only a verified non-synthetic remote `E`
authorizes stable release.
