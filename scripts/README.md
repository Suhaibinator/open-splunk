# Build scripts

Protobuf generation, release builds, migration checks, and packaging automation belong here. Release automation must build the Next.js static export before compiling `open-splunk-server`.

`compile-protos.sh` implements `make proto`. Invoke the Make target so the public developer workflow remains stable.

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
