# Build and validation automation

Use the root Make/npm workflows rather than invoking implementation scripts
directly. The wrappers pin tool inputs, environment, ordering, and output
checks; files in this directory are internal automation unless this page says
otherwise.

## Developer workflows

| Command | Purpose |
| --- | --- |
| `make proto-tools` | Install pinned frontend and protobuf tools. |
| `make dev-clickhouse` / `make dev-down` | Start or stop reusable development ClickHouse. |
| `make run` | Build the embedded backend-mode UI/server and run the development server. |
| `make proto` | Lint and regenerate all Go and TypeScript protobuf output. |
| `make build` | Build the UI-backed server and native collector. |
| `make build-loggen` | Build the separate test/benchmark log generator. |
| `make docs-check` | Validate the owned Markdown set, links, anchors, and documentation naming rules. |
| `make lint` | Run frontend/TypeScript/CSS lint. |
| `make test` | Run docs, lint, Go, frontend, Playwright contract, and typecheck gates. |
| `make release` | Reproduce versionless archives from one clean committed source revision. |
| `make oci` | Reproduce local server and collector images from one clean committed source revision. |

`make clean` runs `go clean`, removes repository build/UI/test outputs
(`build/`, `.next/`, `out/`, `test-results/`, and `coverage.out`) and local tool
caches (`.cache/` and `node_modules/.cache/`), then restores the tracked
`out/.gitkeep` export placeholder. It preserves dependencies, generated
protobufs, and development data.

`npm run test:frontend` is a hardcoded runner. A new `*.test.ts(x)` or
`scripts/*.test.mjs` file does not run until it is added to
`scripts/test-frontend.mjs`. Playwright contracts require the pinned Chromium
runtime, installed once with:

```sh
npx --no-install playwright install chromium
```

## Script ownership

| Script | Role |
| --- | --- |
| `compile-protos.sh` | Full deterministic protobuf generation behind `make proto`. |
| `build-ui.mjs` / `build-ui-output.mjs` | Static Next.js export and embedded-asset manifest validation. |
| `run-development.sh` | Sanitized native development-server launch. |
| `check-docs.mjs` | Owned-document, link, anchor, and wording validation. |
| `test-frontend.mjs` | Explicit frontend/unit/style test dispatcher. |
| `style-inventory.mjs` | Shared CSS parser/inventory used by styling tests. |
| `palette-gallery.mjs` | Screenshot gallery of every palette in light and dark from the demo export; see [Theming](../docs/theming.md#guardrails-what-holds-this-in-place). |
| `materialize-git-snapshot.mjs` | Clean committed-tree materialization for reproducible artifacts. |
| `build-release.sh` | Cross-platform archive construction and identity verification. |
| `build-oci.sh` | Local OCI build/identity/architecture verification. |
| `publish-release.sh` | CI-only GHCR and GitHub Release publication. |
| `run-go-race-shard.sh` | CI race-test sharding with explicit package coverage. |
| `verify-elf-architecture.mjs` | Linux ELF target verification. |
| `strict-json.mjs` | Duplicate-key-rejecting JSON support for build checks. |

The `*.test.mjs` files exercise their corresponding automation without
publishing artifacts. JSON styling ledgers are described in
[Theming](../docs/theming.md#guardrails-what-holds-this-in-place).

## Reproducible artifacts

Both local artifact paths require a clean committed tree and the exact full
lowercase HEAD hash:

```sh
OPEN_SPLUNK_SOURCE_REVISION="$(git rev-parse HEAD)" make release
OPEN_SPLUNK_SOURCE_REVISION="$(git rev-parse HEAD)" make oci
```

The OCI path defaults to local images tagged with that revision on
`linux/amd64`. `OPEN_SPLUNK_SERVER_IMAGE`,
`OPEN_SPLUNK_COLLECTOR_IMAGE`, and `OPEN_SPLUNK_OCI_PLATFORM=linux/arm64`
select explicit local outputs. These commands never push. Official publication
is CI-only; see [Releasing](../docs/releasing.md).

## Opt-in ClickHouse benchmarks

The guarded scalar-String stats-extrema lowering benchmark uses a 2,000,000-row
default corpus:

```sh
OPEN_SPLUNK_CLICKHOUSE_BENCHMARK=1 \
  go test ./internal/clickhouse -run '^$' \
  -bench '^BenchmarkStatsExtremaLowering$' -benchtime=7x -count=1 -v
```

Set `OPEN_SPLUNK_STATS_EXTREMA_BENCH_ROWS` to change its corpus size.

The production-shaped authored-expression benchmark compares the complete
parser/planner/compiler/executor path with parameterized control SQL over a
100,000-row default corpus:

```sh
OPEN_SPLUNK_CLICKHOUSE_BENCHMARK=1 \
  go test ./internal/queryexec -run '^$' \
  -bench '^BenchmarkAuthoredExpressionExecution$' \
  -benchtime=7x -count=3 -benchmem -v
```

Set `OPEN_SPLUNK_AUTHORED_EXPRESSION_BENCH_ROWS` within its documented test
range to change that corpus. Both benchmarks use the repository-pinned
ClickHouse digest unless `OPEN_SPLUNK_CLICKHOUSE_TEST_IMAGE` explicitly selects
another canonical digest. Results are evidence, not release timing gates.
