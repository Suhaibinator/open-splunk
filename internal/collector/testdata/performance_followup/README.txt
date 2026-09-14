Collector performance follow-up, 2026-09-08

Baseline: origin/main at 6475068938f5699f9e93870d8ab5c7f85ff50aa0,
the merged native-format implementation from PR #107.
Final production source: a64e0e7abf4585ed520afa0bf53098dd3a6f86a3.
Branch: codex/collector-parser-performance.

This is an allocation/performance change with the existing parsing, numeric,
timestamp, metadata, redaction, raw-byte ownership, stable-ID and delivery
contracts preserved. There is no new redaction default, runtime dependency,
unsafe conversion, object pool, shared mutable scratch state, or schema change.

Final results: 20 paired samples per case

Fixture                  Baseline    Final       Time change   Allocations/op
NDJSON canonical         1.625 us    1.520 us        -6.49%         55 -> 46
NDJSON nested            3.100 us    2.963 us        -4.44%        129 -> 119
NDJSON high-field      252.1 us    232.0 us          -7.99%       6369 -> 6354
Docker                   2.073 us    1.902 us        -8.25%         72 -> 61
NGINX combined           1.961 us    1.506 us       -23.21%         64 -> 46
Apache common            1.698 us    1.388 us       -18.23%         56 -> 41
Apache combined          1.963 us    1.515 us       -22.85%         64 -> 46
logfmt                   2.463 us    1.778 us       -27.82%         79 -> 61
Log4j2 pattern           1.175 us    0.950 us       -19.18%         36 -> 25
Logback pattern          1.158 us    0.942 us       -18.61%         36 -> 25

All rows above have p < 0.001 in benchstat's final comparison. Across all 25
main fixtures, geometric means changed by -17.23% time/op, -20.25% bytes/op and
-24.99% allocations/op. No allocation count or byte count increased. Large
plain logfmt was 32.77% faster; numeric-looking nonnumbers were 54.13% faster.

No statistically significant slowdown was detected in this cohort. Large
escaped logfmt measured 214.4 us -> 215.0 us (p=0.718), with allocations 33 -> 23;
large raw and 1 MiB Java timings also had no significant change. The paired
samples establish improvements in the named fixtures, not a guarantee that
all production inputs or machines are equivalent. All 20 pairs were accepted
without observed competing builds/tests. Full values: paired-final/.

The supplemental 23-case cohort (paired-malformed/) found no Java compilation
change and reduced malformed-event allocations. Its 200ms timings showed a
0.74% large-NGINX slowdown (p=0.009) and a noisy 15.40% large-logfmt slowdown
(p=0.023, both revisions varying between approximately 195 and 234 us).
These were investigated, not discarded. Astra's disassembly review found no
additional hot-loop instructions or work for either malformed fixture.

A separate 20-pair comparison with one-second samples for those same two cases
(paired-malformed-long/) measured NGINX 385.6 -> 383.0 us (-0.67%, p=0.046) and
logfmt 224.4 -> 220.5 us (no significant difference, p=0.529). Thus the earlier
slowdowns did not reproduce with longer measurements. Large malformed-logfmt
timing still varies by about 6-7%; no throughput improvement or tight timing
equivalence is claimed for that fixture. Its allocation count reproducibly
falls from 21 to 13, and NGINX from 25 to 16. Both original and longer runs are
retained, with all 20 pairs accepted without detected competing builds/tests.

Changes measured

- Avoid interface-induced temporary allocations while hashing the unchanged
  length-prefixed event-ID preimage; use stack digest/hex buffers.
- Reuse per-event dynamic field storage when there are no static fields.
  Static-field cloning, collision precedence and output budgets are unchanged.
- Avoid constructing the NDJSON fallback timestamp twice.
- Copy an access request into one owned immutable string, shared by its message
  and string projections. Binary values retain independent byte ownership.
- Reuse the already-decoded Docker stream string; format the access timestamp
  canonical check into a stack buffer.
- Use the existing exact JSON-number grammar scanner for numeric logfmt tokens.
  Exact numeric conversion itself is unchanged. Plain quoted strings avoid JSON
  unmarshalling; escaped values still use JSON and strict surrogate validation.
- Reuse caller-provided Java capture storage for common small patterns. Large
  patterns retain their bounded fallback allocation, with no backtracking.

Method

Machine: Apple M4 Max, darwin/arm64, Go 1.27.1. Exact versions in tool-versions.txt.
The baseline canonical/nested/wide NDJSON measurements were recorded before
production edits (diagnostics/before-initial.txt). Those three-sample results,
and the extended three-sample exploration, are diagnostic only.

For the final comparisons, compile baseline and candidate from isolated git
archives, each with precisely the same collector benchmark registry:
  decoder_benchmark_test.go
  native_benchmark_test.go
  performance_benchmark_test.go
Copy the final versions of those three files into the baseline archive. Only
other top-level internal/collector/*_test.go files are removed from these
temporary benchmark archives; no correctness tests are removed from the repo.
The full repository tests run separately with all regression tests present.
Construction/compilation is outside decode timing loops, and every Decode
produces independently owned output. Java compilation has its own benchmark.

Build each archive with GOWORK=off and:
  go test -c ./internal/collector -o /absolute/path/to/binary.test

Run the retained driver with absolute binary/output paths:
  python3 run_paired.py /path/before.test /path/after.test /path/paired-final
  PERF_BENCH_PATTERN='^(BenchmarkNativeJavaCompile|BenchmarkNativeMalformed)$' \
    PERF_EXPECTED_CASES=23 python3 run_paired.py \
    /path/before.test /path/after.test /path/paired-malformed
  PERF_BENCH_PATTERN='^BenchmarkNativeMalformed$/(logfmt|nginx-combined)$/262144$' \
    PERF_EXPECTED_CASES=2 PERF_BENCH_TIME=1s python3 run_paired.py \
    /path/before.test /path/after.test /path/paired-malformed-long

The driver uses GOMAXPROCS=1 and macOS taskpolicy -a -l 0 -t 0. Each sample uses
-test.run=^$, -test.benchmem, -test.benchtime=200ms and -test.count=1. It collects
20 complete before/after pairs, alternating which revision runs first. The main
cohort has 25 cases; the separate cohort has 21 malformed/almost-matching cases
at 1 KiB/16 KiB/256 KiB and two Java compilation cases. All scheduled tests,
builds, fuzzing and review agents finished before measurements began.

The driver checks for other Go/frontend builds, tests and lint processes every
250ms, waits for a quiet machine before a pair, and excludes the whole pair if
it observes overlapping work. Exclusions are based solely on process activity,
never timings. Original excluded samples, if any, remain under excluded/ and
the workload observations remain in workloads.jsonl. This reduces known local
interference; it does not promise perfect OS scheduling or universal hardware
results. No competing process is killed.

Compare the raw samples with the pinned x/perf benchstat version:
  benchstat paired-final/before.txt paired-final/after.txt
  benchstat paired-malformed/before.txt paired-malformed/after.txt
  benchstat paired-malformed-long/before.txt paired-malformed-long/after.txt

All benchmarks use ReportAllocs; decode cases also report bytes processed.
Mixed-input benchmarks decode TWO events per operation. Pattern benchmark size
labels are a minimum target: a 32- or 1,024-capture header can exceed 32 bytes;
SetBytes uses the actual framed length. The large five-capture case is exactly
1 MiB. A geomean across fixtures is a descriptive benchmark summary, not an
estimate of a particular production workload's throughput.

Investigation retained

intermediate/ contains the first full 20-pair comparison against e6d04627.
It showed strong common-path gains, but a 0.80% slowdown on the large escaped
logfmt fixture (p=0.038). This was not accepted as a clean performance result.
The redundant raw-control scan was moved out of the escaped path, preserving
historical error precedence. Two further owned-string/stack-buffer changes
removed Docker/access allocations. All affected validation and all three
adversarial reviews were repeated on the final production revision.

profiles/ retains CPU and allocation-object profile summaries used to guide
the first optimization round. They compare baseline with e6d04627; they are
not profiles of the final tiny follow-up. Absolute profile totals are not
comparable per operation because the faster revision executes more iterations.
Use the paired allocation measurements for quantitative changes. The remaining
large allocations are mainly required typed protobuf output, JSON token
materialization, owned event bytes, and exact-number arithmetic. Speculative
bulk arenas were not introduced because they can retain discarded fields and
change memory-lifetime tradeoffs.

verification.txt records executed commands and results; reviews.txt records
the independent Astra/Sol reviews. The corresponding detailed outputs and
regression tests are retained. Frontend gates were executed even though these
changes are entirely in Go and test evidence.
