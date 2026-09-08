Issue #104 benchmark evidence
============================

Baseline: a8092b5111b98c3d18fcacb6e1a6b49918f1e030
Candidate: d874217530a8f8187919f5e65232f999c48900f6
Host: Apple M4 Max, darwin/arm64; Go go1.27.1.
benchstat: golang.org/x/perf v0.0.0-20260825160852-19be9d8e6c70.

The baseline is a separate source snapshot with only the identical
decoder_benchmark_test.go added. Each snapshot was built with go test -c.
Construction is outside the measured loop. No implementation/test agents,
builds, fuzzers, integration workloads, or linters ran during sampling.
Ordinary desktop/system background activity was not modified.

NDJSON methodology:
  GOMAXPROCS=1 <snapshot-test-binary> -test.run='^$' 
    -test.bench='^BenchmarkNDJSONDecoder$' -test.benchmem 
    -test.benchtime=1s -test.count=1

Run 20 pairs, alternating baseline/candidate and candidate/baseline order.
before.txt and after.txt retain every sample; comparison.txt is benchstat's
comparison at its default significance threshold. All three cases retain
identical B/op and allocs/op across every sample. No case has a statistically
significant throughput regression. These are measurements on this host,
not a universal throughput or latency guarantee; confidence ranges are kept
in the report rather than omitted.

Native methodology:
  taskpolicy -a -l 0 -t 0 /usr/bin/env GOMAXPROCS=1 <candidate-test-binary> -test.run='^$' 
    -test.bench='^BenchmarkNative(Decoder|MixedInputs)$' -test.benchmem 
    -test.benchtime=2s -test.count=10

native.txt retains all samples, and native-summary.txt summarizes them.
MixedInputs reports a PAIR of events per operation: two NDJSON events or one
NDJSON plus one Java event. It measures independent configured decoder paths;
it does not claim to isolate NDJSON latency under external CPU contention.
The compiled-backend mixed-load gate separately ingested and verified 60,000
NDJSON/Java records over 30 seconds, with exact projections and checkpoints.

Reproduce the comparisons from this directory with:
  benchstat before.txt after.txt
  benchstat native.txt

Additional checked-in benchmarks cover compilation and malformed near-matches
at increasing sizes. Construction is never included in decoder timings.

The initial native timing run had large variability and is retained under
preliminary/. It was not used to claim stable parser throughput. The rerun
uses macOS taskpolicy application mode, latency tier 0 and throughput tier 0
on the benchmark process only. One aborted partial rerun overlapped installing
the profile reader; that partial output is also retained in preliminary/ and
excluded from the reported sample set. The completed rerun had no competing
agent, build, install, lint, integration, or fuzz workload.

hostile.txt measures Java construction separately and every malformed native
parser at 1 KiB, 16 KiB, and 256 KiB, with the same taskpolicy invocation and
-test.bench='^BenchmarkNative(JavaCompile|Malformed)$'
-test.benchtime=200ms -test.count=20. This shorter duration is for scaling and
allocation analysis, not comparison with the former unsupported formats.

Profiles cover the logfmt and log4j2-pattern decoding fixtures for 2 seconds
each, with GOMAXPROCS=1 and -test.cpuprofile / -test.memprofile. The text CPU
and allocation summaries use github.com/google/pprof
v0.0.0-20260906184651-6331bc6350fe (the supplied Go distribution lacks go tool
pprof). Allocation results identify output construction, captures and exact
numeric conversion as principal costs; no shared Java parser state appears
on the NDJSON path. The CPU profile includes runtime/system wait samples and
is diagnostic evidence, not a throughput measurement.

pattern.txt separately measures the compiled pattern package using the same
scheduled command, 200ms duration and 20 samples, with
-test.bench='^BenchmarkPattern(Compile|Capture|AlmostMatching)$'. Its hostile
whitespace-delimiter cases extend through the full 1 MiB event ceiling.

For the final comparison against updated main after the wave 1 configuration
fix, use final/README.txt and final/comparison.txt. The earlier candidate and
all its timing history remain intact above.
