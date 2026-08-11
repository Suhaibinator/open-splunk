# SPL compatibility v0.2 acceptance report

**Status:** accepted

**Target authored-search identity:** `0.2`

**Knowledge-expression identity:** `0.1`

**Prepared:** August 11, 2026

The authored-search runtime now advertises `0.2`. The final command sheet ran
against one clean, committed materialization of the exact activated source
tree. That isolated validation commit deliberately avoided staging or
overwriting pre-existing changes in the shared development worktree. This
report was populated after the run; its evidence-only edits therefore postdate
the named activation snapshot without changing runtime, contract, or corpus
bytes.

Normative and operator documents:

- [`SPL compatibility v0.2 contract`](spl-compatibility-v0.2.md)
- [`v0.2 migration and read-only audit guide`](spl-compatibility-v0.2-migration.md)
- [`v0.2 execution benchmark baseline`](spl-compatibility-v0.2-benchmark.md)
- [`SPL expansion roadmap`](spl-roadmap.md)

## Release invariants

- The public authored-search compatibility identity is exactly `0.2`, not a
  prefixed product label.
- Tier-1 calculated fields remain on the closed `SPLExpressionV01` profile.
- One admitted job retains its authored source and immutable compiler version;
  history rerun reparses source and records the rerunning binary's version.
- Activation happens once, after every backend, retained-search, audit,
  browser, and release gate passes on one exact clean revision.
- A failed or incomplete gate keeps the prior development identity.

## Exact evidence identity

| Field | Value |
| --- | --- |
| Activation Git revision | `31d483c53c69a51b7481925af213e85d1dddbcd0` |
| Activation tree state | clean isolated materialization; empty `git status --porcelain` before and after every mutating generator/release gate |
| Activation UTC interval | `2026-08-11T10:14:58Z`–`2026-08-11T10:38:05Z` |
| Runner/CPU/OS | Apple M4 Max, 16 logical CPUs, Darwin 25.6.0 arm64 |
| Go | `go1.26.5` |
| Node.js | `v24.18.0` |
| npm | `11.16.0` |
| Playwright | `1.62.1` |
| ClickHouse | `26.3.17.4` |
| ClickHouse image | `clickhouse/clickhouse-server:26.3.17.4@sha256:85c434814ac8905e5648027ce926f74ab067edd6aadbccb6c0c165cd3571ea49` |
| Contract SHA-256 | `2f1e7c9f2fb3902908bf328a135112b7acf20024796ce39179c486737c6eca7e` |
| Corpus SHA-256 | `613ee59ce81252f733399ffa7d69487d37648b22052cbf6118fced51c39512b3` |
| Redacted audit report SHA-256 | `60fdaedfee2068d4ddd874299f5112895fd84f08df33c9d1703bebd67fdab2d9` (three byte-identical runs) |
| Release artifact identity/readback | application `0.1.0`, source `31d483c53c69a51b7481925af213e85d1dddbcd0`, UI SHA-256 `def273f2b32850d74c751395b055403da956de78d1aadbe7c4abbd74de2c4815`; server binary SHA-256 `d97b97e9bfd143693a883111d3691bd82aa3c93c3186c1bb14e7c169e9647d47` |
| Complete evidence bundle | local acceptance artifact `spl-v02-acceptance-evidence.tar.gz`, SHA-256 `a859364cea7e4f7c73dfc5add97fed0df76e16f04091156b42e4cb96b278ad88` |

## Development evidence already recorded

The following results were obtained while implementing v0.2. They are retained
as additional regression evidence; every required surface was repeated on the
final activation revision below.

The working tree was based on
`97401955e3739f50ac7bee7c1ea1fee9e60fb661` and contained uncommitted changes,
so that hash is not the activation revision.

| Surface | Command/evidence | Development result |
| --- | --- | --- |
| Contract/corpus, parser, reference evaluator | `go test ./internal/spl -count=1` | passed; exact rule parity and deterministic/randomized arithmetic/membership fixtures included |
| Planner/compiler | `go test ./internal/plan ./internal/clickhouse -count=1` | passed |
| Pinned compiler/EXPLAIN gate | `OPEN_SPLUNK_CLICKHOUSE_INTEGRATION=1 go test ./internal/clickhouse -run '^TestExpressionV02AgainstClickHouse$' -count=1 -v` | passed in 7.508 s against the exact image above; asserts one event scan, no generated `ARRAY JOIN`, and no physical `ArrayJoin` action |
| Pinned executor/manager gate | `OPEN_SPLUNK_CLICKHOUSE_INTEGRATION=1 go test ./internal/queryexec -run '^TestExpressionV02ExecutorManagerAgainstClickHouse$' -count=1 -v` | passed in 6.47 s against the exact image above |
| Execution benchmark | fixed 65,536-row, seven-iteration command in the benchmark document | passed in 8.820 s; exact checksums/counts verified |
| Protobuf generation | `make proto` | passed during development |
| TypeScript static check | `node_modules/.bin/tsc --noEmit` | passed during development |

The benchmark host was Apple M4 Max, Darwin arm64, 16 logical CPUs, Go
`1.26.5`, and the exact ClickHouse image above. The pre-activation baselines
were:

| Workload | Rows | Median | CV | Verified result |
| --- | ---: | ---: | ---: | ---: |
| fixed arithmetic | 65,536 | 6.486 ms | 36.29% | 262144 |
| Dynamic arithmetic | 65,536 | 44.940 ms | 10.84% | 49047392 |
| low-cardinality membership | 65,536 | 9.945 ms | 32.74% | 16384 |
| high-cardinality membership | 65,536 | 26.460 ms | 23.13% | 16 |

These are machine-specific diagnostics, not portable throughput claims. The
benchmark document records the paired parameterized controls, generated-SQL
sizes, allocations, dataset shape, warmup, and interpretation.

The final three-run median-of-medians was 6.208 ms fixed arithmetic, 45.61 ms
Dynamic arithmetic, 9.549 ms low-cardinality membership, and 28.27 ms
high-cardinality membership. Changes versus the baseline were -4.3%, +1.5%,
-4.0%, and +6.8%, respectively, all within the recorded baseline variance and
with exact result checks unchanged. Parser, compiler, and audit microbenchmarks
also retained linear time, SQL-size, and allocation growth through 256
operators, 32 candidates, and 100,000 audit operators.

## Compatibility-audit release record

Run the audit as described in the migration guide against a quiesced,
sanitized control-database copy and the exact activation checkout. The tool
reports identities and locations but never authored SPL or field values.

| Field | Value |
| --- | --- |
| Sanitized control-database source/digest | deterministic four-object synthetic saved-search fixture generated by `TestGenerateCompatibilityAuditAcceptanceFixture`; SHA-256 `a3217421b6858599ba495302f29d9b0ba7936e2afe1b6ff3d972a13784247ed0` |
| Repository revision/root | `31d483c53c69a51b7481925af213e85d1dddbcd0`; isolated clean acceptance root |
| Exact audit command | `./build/open-splunk-server audit-spl-v0.2 -control-db /tmp/open-splunk-v02-acceptance.TsdTsf/sanitized-control.db -repository "$PWD"` |
| Scanned objects | 186 (4 control-database saved searches plus 182 repository sources) |
| Findings by kind | 198 `ambiguous_unspaced_scalar_operator` candidates |
| Finding dispositions | control DB: one synthetic legacy-field positive control and one intended subtraction; repository: 25 audit/migration controls, 46 host-language formatting artifacts in 17 files, 124 intentional v0.2 conformance/vertical/benchmark cases in 15 files, and one pre-existing intended comparison; zero unresolved production sources |
| Second-run report digest | `60fdaedfee2068d4ddd874299f5112895fd84f08df33c9d1703bebd67fdab2d9`; runs 1, 2, and 3 byte-identical |
| Input database digest before/after | `a3217421b6858599ba495302f29d9b0ba7936e2afe1b6ff3d972a13784247ed0` / same |

A finding is a review candidate, not proof of an incompatible field. Any
unresolved finding blocks activation. A zero-finding report applies only to the
explicit sources and revision named above.

## Independent review record

| Review | Reviewer/run identity | Findings | Resolution |
| --- | --- | --- | --- |
| correctness/adversarial | `/root/adversarial_spec_review`, `/root/final_adversarial_review`, parser adversarial child | lexer/context, grouping, quote/UTF-8 ranges, exponent boundaries, RPN fold quoting, all predicate consumers, and max-shape cancellation gaps | every concrete finding fixed with a permanent regression; final ClickHouse and queryexec verticals passed |
| security/authority/redaction | `/root/product_surface_sdet` plus independent authority-retention shard | no validated actionable finding after admission/seal/journal/history/export/browser/audit review | five no-issue authority receipts; focused Go and all 355 frontend/script tests passed |
| reuse/maintainability | three frozen-diff `simplify` reviewers plus primary review | duplicate validators/scanners/comparison binding, obsolete audit walkers, and split frontend classifiers | centralized shared helpers, removed dead code, and full lint/build gates passed |
| efficiency/resource bounds | `/root/final_adversarial_review`, execution benchmark SDET | atomic nested-value undercharge risk, O(n²) audit/editor scans, eager membership bindings, max-shape resource evidence | exact recursive retained sizing, block charging, linear scanners/batched offsets/lazy aliases, benchmark and cancellation/limit gates passed |
| dedicated SDET/integration | inventory, expression vertical, product-surface, benchmark, and predicate/cancellation SDET agents | corpus/reference, typed real data, sparse nulls, paging, audit dispositions, retained knowledge, browser, and release matrix | all dedicated unit/race/real-data/browser/release gates passed; zero unresolved finding |

Record concrete resolved findings as test names or commit references. “Reviewed”
without a reproducible run identity and disposition is not acceptance evidence.

## Final activation command sheet

All commands in this section ran from the same clean activation revision after
the public identity changed to `0.2`. Complete logs are in the evidence bundle;
the table records each SHA-256 log digest.

### Toolchain and source identity

```sh
test "$(go version | awk '{print $3}')" = "go1.26.5"
test "$(node --version)" = "v24.18.0"
test "$(npm --version)" = "11.16.0"
test -z "$(git status --porcelain)"
git rev-parse HEAD
shasum -a 256 \
  docs/spl-compatibility-v0.2.md \
  internal/spl/testdata/compatibility-v0.2.json
```

| Command group | Exit/status | Log digest |
| --- | --- | --- |
| Toolchain and source identity | pass | `77dfe48eae6ccd0d16e5a21be213cb0d7b91fb69257e8b91f704f8ba65c9e7ae` |

### Backend and generated contracts

```sh
go test ./... -count=1 -timeout=10m
go test -race \
  ./internal/spl ./internal/plan ./internal/clickhouse \
  ./internal/queryexec ./internal/searchjobs ./internal/knowledgeprogram \
  ./internal/knowledgesnapshot ./cmd/open-splunk-server \
  -count=1 -timeout=20m
go mod tidy -diff
go vet ./...
go build ./...
golangci-lint run ./...

make proto
make proto-lint
BUF_CACHE_DIR="$PWD/.cache/buf" \
  node scripts/check-buf-breaking.mjs --against-ref main
go test . -count=1
```

| Command group | Exit/status | Log digest |
| --- | --- | --- |
| Repository-wide Go tests | pass | `a55fdb010686733adb27233b96f75037c9f5ae651109f6064dbf3de0c3938ede` |
| Focused race tests | pass | `f1492fc2b9165051b22f4abf986114f74ca2bfc51a478eafa7d255dd77ed9923` |
| Tidy/vet/build/golangci-lint | pass; `0 issues` | `e92606b0bf483111dff0a120c315ea165821348f31365020e2468a0059095c47` |
| Protobuf generation/lint/breaking | pass; exactly five pinned knowledge migration notices | `4360d45a7a91429619b319551d2cdb687be33c0197837a7613a99239fba647b9` |
| Embedded-root package | pass | `38e5fa59b6ed1da72fe32d475b75c48085df4728e121a8808870f129e3e6b982` |

### Frontend and browser

```sh
npm ci
npm run typecheck
npm run lint
npm run test:frontend
npm run build
npx --no-install playwright install chromium
```

| Command group | Exit/status | Log digest |
| --- | --- | --- |
| npm clean install and toolchain readback | pass; 44 packages, zero vulnerabilities | `53052113dd4707f4771f03306e0f70ffa761213c67851964072d2a006529c959` |
| Typecheck/lint/frontend tests/build | pass; 70 script tests, 285 frontend tests, production build | `7be290c84e7ac451b845b27337cf0c8a24ac727a30acbfb98c33f8e0ec2df89e` |
| Pinned Playwright browser install/readback | pass; `1.62.1` | `47de3041a9b82754419b455f44018d3278b4e3cd6f1dc4385a871ceb13bca325` |

### Pinned ClickHouse and complete vertical

```sh
export OPEN_SPLUNK_CLICKHOUSE_TEST_IMAGE='clickhouse/clickhouse-server:26.3.17.4@sha256:85c434814ac8905e5648027ce926f74ab067edd6aadbccb6c0c165cd3571ea49'

OPEN_SPLUNK_CLICKHOUSE_INTEGRATION=1 \
go test ./internal/clickhouse \
  -run '^TestExpressionV02AgainstClickHouse$' \
  -count=1 -timeout=15m -v

OPEN_SPLUNK_CLICKHOUSE_INTEGRATION=1 \
go test ./internal/queryexec \
  -run '^TestExpressionV02ExecutorManagerAgainstClickHouse$' \
  -count=1 -timeout=15m -v

OPEN_SPLUNK_CLICKHOUSE_BENCHMARK=1 \
OPEN_SPLUNK_EXPRESSION_V02_BENCH_ROWS=65536 \
go test ./internal/queryexec -run '^$' \
  -bench '^BenchmarkExpressionV02Execution$' \
  -benchtime=7x -count=3 -benchmem -v

OPEN_SPLUNK_BACKEND_INTEGRATION=1 \
go test ./integration -run '^TestBackendVertical$' \
  -count=1 -timeout=15m -v
```

| Command group | Exit/status | Log digest |
| --- | --- | --- |
| Compiler, real ClickHouse, and pinned EXPLAIN | pass in 9.72 s; every predicate consumer, 256-op fixed/Dynamic, one scan/no `ArrayJoin` | `4a0aeca94a01f292159acbb30bca55748844e658f746c356378105e6c6bfd009` |
| Executor/manager real-data vertical | pass in 7.83 s; native cancel 47.75 µs, 32-candidate limit atomic | `09cd0b912f44941f41d37ab1033451507f3e5a8e543117a366578e84f88bf0bf` |
| Fixed execution benchmark and comparison | pass, three runs with exact result checks and no material regression | `3d35f4924d140f2d4d57fadb71502654555830d5c87cf96d582a41d81d399f3d` |
| Collector/server/browser backend vertical | pass in 29.69 s with runtime identity `0.2` | `39e74f7c7872d7f32ed6abb3d8ca9f5c8dba24e58c2fce4e67e9caa2610d8576` |
| Retained v0.1 knowledge → authored v0.2 arithmetic | pass against pinned ClickHouse | `5f51500f30b75e2b31e1a2ce48b63c9e1666dda48bb52a052f6f8e7c85c96362` |

### Fuzz, audit, and release

Record the exact time budget and seeds for every affected fuzz target. The
minimum campaign includes scalar lexing/parsing, quoted fields, arithmetic
trees, membership lists, planner/compiler forged structures, and the browser
structure scanner where its harness supports fuzz/property input.

```sh
make build-server

./build/open-splunk-server audit-spl-v0.2 \
  -control-db /absolute/path/to/sanitized-control.db \
  -repository "$PWD"

git diff --check
test -z "$(git status --porcelain)"
OPEN_SPLUNK_SOURCE_REVISION="$(git rev-parse HEAD)" make release
```

| Command group | Exit/status | Log digest |
| --- | --- | --- |
| Recorded fuzz campaigns | pass; three independent 60 s campaigns, 2,169,260 + 1,515,077 + 4,090,302 executions | `1077a140184e9c0dcd54b21f33acd42d9a5239af906abafdcfe0cd88394474c3`, `b62505e94ccad60ccdd01d9130233b6a1f787eb4bd98e5da1bc6f13637a335ae`, `a70a55c10548bf5ef181b70deb1760f8521c35f2576b25f498bcdfa7a25504a8` |
| Parser/compiler/audit benchmarks | pass; linear bounded scaling | `eb1f0969713214d0cd147cfdc5e1631a8cdd250aba1c37e73fccf4b4870f587d` |
| Redacted read-only audit and dispositions | pass; three byte-identical reports, zero unresolved source | report `60fdaedfee2068d4ddd874299f5112895fd84f08df33c9d1703bebd67fdab2d9`, metadata log `5fd9ec3e45c2c07da80b8eb19840f18f61da8a7036305bae2ce3bdadac0bfad1` |
| Diff/clean-tree proof | pass before and after generation, audit, and release | manifest `9755ec9cad4e4bc6b5d555958939314bdd32c0680f5392bb66ae1de2bfc191bb` |
| Clean-snapshot release build and identity readback | pass; committed snapshot identity matched all embedded artifacts | `38dda9e479571bb38f027d5e18db63d2e5335907865617d47ffe948b526bdc17` |

## Final acceptance decision

| Decision field | Value |
| --- | --- |
| All roadmap language/semantic criteria pass | yes |
| All authority/resource criteria pass | yes |
| All product/browser/retained-search surfaces pass | yes |
| All quality, review, audit, and release gates pass | yes |
| Unresolved findings | zero |
| Accepted activation revision | `31d483c53c69a51b7481925af213e85d1dddbcd0` |
| Decision and approver/run identity | accepted by the Codex `/root` implementation acceptance run ending `2026-08-11T10:38:05Z`; this records engineering acceptance, not a separate deployment approval |

Every required command passed on the named clean revision, the audit has no
unresolved candidate, and runtime, export, job, and browser surfaces all read
back compatibility identity `0.2`.
