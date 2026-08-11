# Build scripts

Protobuf generation, release builds, migration checks, and packaging automation belong here. Release automation must build the Next.js static export before compiling `open-splunk-server`.

`compile-protos.sh` implements `make proto`. Invoke the Make target so the public developer workflow remains stable.

`check-buf-breaking.mjs` runs the repository-pinned Buf `FILE` compatibility
policy. Five exact pre-activation migrations in `knowledge_api.proto` are
waived only when the comparison branch resolves to
`c5440b96248c68a9b58d10ebaf08eaef5345b61a`; another base commit or any other
diagnostic still fails. After synchronizing the local base branch, run:

```sh
node scripts/check-buf-breaking.mjs --against-ref main
```

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
