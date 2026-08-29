# Build scripts

Protobuf generation, reproducible artifact builds, migration checks, and
packaging automation belong here. Artifact automation must build the Next.js
static export before compiling `open-splunk-server`.

`compile-protos.sh` implements `make proto`. Invoke the Make target so the public developer workflow remains stable.

`build-visual-exports.mjs` and `serve-static.mjs` support `npm run test:visual`.
The first builds the static export in both the demo and backend data modes,
moves each build into `.cache/visual`, and resets `out/` to the state Git
tracks, because `out/` is the release payload `webui.go` embeds and a test
target must not leave a manifest-less demo build in it. The second serves those
directories over loopback with no dependencies, so the visual-regression
baselines need neither the Go server nor ClickHouse. See
[the integration guide](../integration/README.md#visual-regression-baselines).

`check-docs.mjs` implements `make docs-check`. It validates local Markdown
targets and heading anchors across the owned documentation set and rejects
retired pre-release version tokens, versioned protobuf/API/HEC identifiers,
release-era SPL rule IDs, versioned document names, and public version-floor
language. Negative-support fixtures outside the owned Markdown files remain
available to protocol tests without weakening the documentation contract.

The scalar-String `stats min`/`max` ClickHouse microbenchmark is an opt-in Go
benchmark because it starts a disposable container. It compares the production
guarded-Array and scalar-tuple SQL helpers over the same generated corpus and
reports client time plus server duration, average/peak memory, and rows read
from `system.query_log`:

```sh
OPEN_SPLUNK_CLICKHOUSE_BENCHMARK=1 \
  go test ./internal/clickhouse -run '^$' \
  -bench '^BenchmarkStatsExtremaLowering$' -benchtime=7x -count=1 -v
```

The default corpus has 2,000,000 rows and uses the repository-pinned
ClickHouse image. Set `OPEN_SPLUNK_STATS_EXTREMA_BENCH_ROWS` for a deliberate
corpus-size change or `OPEN_SPLUNK_CLICKHOUSE_TEST_IMAGE` for an explicit image
comparison. This is deliberately a helper-level lowering microbenchmark;
compiler alias/state reuse and downstream key cleanup are covered separately
by compiler assertions and pinned `EXPLAIN` tests. Benchmark output is
evidence, not a timing gate.

`css-inventory.mjs` parses the stylesheets and the code that names their
classes. It is a library rather than a test so `css-invariants.test.mjs` can
assert on stylesheet structure without reading a stylesheet itself, which is
one of the invariants it enforces. `css-dynamic-classes.json` records the
global classes that only ever exist at runtime.

`safety-net.test.mjs` checks that the Phase 0 safety net cannot stop running
unnoticed: every unit test file is named in this directory's hardcoded runner
list, and every screenshot a visual spec pins has a committed baseline.

`visual-determinism.mjs` implements `npm run test:visual:determinism`. It
builds the exports once, serves them itself, and runs the visual suite twice
over that single build, comparing the two passes with no pixel budget. See
[the integration guide](../integration/README.md#screenshot-determinism).
