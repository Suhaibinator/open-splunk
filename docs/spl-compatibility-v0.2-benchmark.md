# SPL compatibility v0.2 execution benchmark baseline

**Date:** August 11, 2026

**Status:** checked-in diagnostic baseline; not a portable throughput claim

This baseline covers the execution cases required by the SPL v0.2 roadmap:
fixed and Dynamic arithmetic over a production-shaped event table, plus
membership over low- and high-cardinality fields. The benchmark lives in
`internal/queryexec/expression_v02_execution_benchmark_test.go`.

## Method

- Machine: Apple M4 Max, 16 logical CPUs, Darwin arm64.
- Go: `go1.26.6`.
- ClickHouse server: `26.3.17.4`.
- Exact image:
  `clickhouse/clickhouse-server:26.3.17.4@sha256:85c434814ac8905e5648027ce926f74ab067edd6aadbccb6c0c165cd3571ea49`.
- Dataset: 65,536 canonical typed log events admitted through the production
  `clickhouse.Store` in 66 bounded batches and compacted to one part before
  timing. Every event has the full fixed event envelope, raw payload, one
  Dynamic `Int64` metric, an eight-value `host`, a 32-value `service`, and a
  unique `event_id`.
- Read path: authored SPL is parsed, planned against an immutable visibility
  cutoff, compiled, sealed, admitted, and executed by the production
  resource-bounded `queryexec.Executor`.
- Controls: parameterized hand-authored ClickHouse SQL scans the same rows with
  the same tenant, index, source, event-time, index-time, visibility, and
  retention predicates. Controls are equivalent for this fixed corpus, not
  alternate compatibility implementations. In particular, the Dynamic
  control can directly extract `Int64`, and the membership control can use
  ClickHouse `IN`; production lowering must validate every supported Dynamic
  numeric representation and uses explicit comparisons to preserve Open
  Splunk equality semantics.
- Query settings: one ClickHouse thread, query cache disabled for controls,
  and the production executor's ordinary memory/read/result ceilings.
- Sampling: one verified warmup followed by seven timed and result-verified
  iterations per case. Setup, ingestion, compaction, compilation, and warmup
  are excluded from the timed interval. `cv_pct` is the population coefficient
  of variation across those seven client-observed durations.

Exact command:

```sh
OPEN_SPLUNK_CLICKHOUSE_BENCHMARK=1 \
OPEN_SPLUNK_EXPRESSION_V02_BENCH_ROWS=65536 \
GOCACHE=/tmp/open-splunk-root-gocache \
go test ./internal/queryexec -run '^$' \
  -bench '^BenchmarkExpressionV02Execution$' \
  -benchtime=7x -count=1 -benchmem -v
```

## Initial diagnostic sample

| Workload | Path | Generated SQL bytes | Mean ms/op | Median ms/op | CV | Rows/s at median |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| fixed arithmetic | production executor | 1,923 | 7.634 | 6.486 | 36.29% | 10,104,224 |
| fixed arithmetic | hand SQL driver | — | 5.573 | 4.160 | 53.42% | 15,755,266 |
| Dynamic arithmetic | production executor | 8,347 | 46.909 | 44.940 | 10.84% | 1,458,400 |
| Dynamic arithmetic | hand SQL driver | — | 7.221 | 5.575 | 49.19% | 11,754,721 |
| low-cardinality membership | production executor | 2,464 | 11.564 | 9.945 | 32.74% | 6,589,706 |
| low-cardinality membership | hand SQL driver | — | 5.648 | 5.617 | 1.64% | 11,668,045 |
| high-cardinality membership | production executor | 8,005 | 29.258 | 26.460 | 23.13% | 2,476,811 |
| high-cardinality membership | hand SQL driver | — | 5.436 | 5.445 | 1.36% | 12,036,273 |

Every timed iteration verified the aggregate result before it was accepted:

- fixed arithmetic checksum: `262144`;
- Dynamic arithmetic checksum: `49047392`;
- low-cardinality membership count: `16384`; and
- high-cardinality membership count: `16`.

The production/control gap is expected to be largest for Dynamic arithmetic:
the control is specialized to this corpus's one physical type, while the
production query enforces the complete nullable numeric and semantic-tag
contract and retains atomic-result failure behavior. Membership similarly
uses explicit bounded equality comparisons instead of ClickHouse's optimized
`IN`, as required by the compatibility contract. The high-cardinality case has
eight times as many candidates as the low-cardinality case but took 2.66 times
the production median in this sample; this is consistent with bounded linear
candidate work and does not indicate superlinear growth. These observations
explain the diagnostic deltas but do not waive future regression review.

## Reproduction and comparison policy

Use a fixed `-benchtime`, retain the exact image digest, and record a fresh
machine/dataset/run-count/variance block when updating this baseline. Compare
like-for-like median and allocation metrics, inspect a material change in
generated SQL size, and rerun enough samples to distinguish background noise
from a regression. Do not compare throughput across different machines as a
product performance claim.
