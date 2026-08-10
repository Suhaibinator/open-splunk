# HEC load and live-transport gates

This runbook separates the fast, deterministic HEC transport gate from the
long-running durable product soak. Passing the transport gate is necessary,
but it is not evidence that the complete performance-and-soak section of
`hec-compatibility-plan.md` has passed.

## Always-on live transport gate

Run:

```sh
go test ./internal/hechttp -run '^TestHandlerLive' -count=1 -v
go test -race ./internal/hechttp -run '^TestHandlerLive' -count=1
```

The tests use generated `httptest` TLS identities and synthetic credentials;
they do not read or print deployment secrets. Unlike tests that assign
`Request.Proto` directly, these tests cross a real loopback TCP/TLS boundary.
They prove:

- TLS ALPN negotiates HTTP/2 and two simultaneous streams reuse one
  connection;
- neither a second token nor a second stream for the same token bypasses the
  process-wide or per-token concurrency gate;
- an incomplete HTTP/1.1 chunked body occupies only its admitted slot;
- a backpressure-rejected `Expect: 100-continue` request receives the bounded
  busy response without its body being read;
- JSON, raw, gzip, exact-number/field-heavy, 1,000-event, ACK-enabled, and
  maximum-width ACK-query shapes share one mixed workload;
- successful and busy responses both occur under saturation;
- staging concurrency never exceeds 8 process-wide or 4 per token; and
- peak heap growth, retained post-GC heap growth, peak goroutines, and
  post-load goroutines remain within explicit test envelopes.

The short test uses 48 clients and 384 requests. Its throughput is
observational, not a product performance threshold, because its stager is a
bounded in-memory test double.

## Opt-in transport soak

The reusable transport workload runs for 24 hours by default. Unlike the
always-on saturation test, the soak launches one synchronized 48-client burst
every 480 milliseconds: a declared 100 requests/second. The in-memory stager
holds admitted requests for 50 milliseconds, so every burst must contain both
successful requests and explicit code-9 backpressure without continuously
saturating several CPU cores.

Run this only with explicit operator approval on a dedicated or otherwise
quiescent host connected to AC power. Prevent system sleep for the complete
run, and do not overlap builds, benchmarks, upgrades, or other resource-heavy
work. Release evidence must come from a clean, committed, immutable revision;
record `git rev-parse HEAD` and require `git status --porcelain` to produce no
output before starting. Changes made after the test binary starts are not part
of its evidence.

Run:

```sh
OPEN_SPLUNK_HEC_SOAK=1 \
go test ./internal/hechttp \
  -run '^TestHandlerLiveTransportSoak$' \
  -count=1 -timeout=25h -v
```

For harness validation only, override the duration:

```sh
OPEN_SPLUNK_HEC_SOAK=1 \
OPEN_SPLUNK_HEC_SOAK_DURATION=2m \
go test ./internal/hechttp \
  -run '^TestHandlerLiveTransportSoak$' \
  -count=1 -timeout=5m -v
```

Durations from one second through seven days are accepted. Only a run of at
least 24 hours satisfies the transport-soak duration in the compatibility
plan. The 24-hour plan requires at least 180,000 mixed bursts and 8,640,000
classified requests. Every burst must exercise both success and backpressure;
successful, busy, and accepted-event counts must each remain at least the burst
count. The harness rejects a run that finishes early, falls more than five
seconds behind schedule, has a forward liveness gap or backward wall-clock
discontinuity greater than five seconds, or has no completed activity within
960 milliseconds of the end. A sleeping or suspended machine therefore cannot
produce passing evidence by merely advancing wall time.

With `-v`, the harness emits a bounded aggregate progress line once per hour.
Capture those lines and the final target rate, burst/mixed-burst counts,
minimum and actual request counts, success, busy, event, elapsed-time,
schedule-lag, liveness-gap, tail-gap, peak-heap-growth, and
peak-goroutine-growth fields. An absent hourly line is an operator signal to
inspect the process; it does not relax the automated five-second liveness gate.
The offered rate remains transport-longevity coverage, not a product
performance claim, because the stager is an in-memory test double.

## Durable product evidence still required

The live transport harness intentionally does not substitute an in-memory
stager for claims about SQLite or ClickHouse. The opt-in durable harness runs
the shipped server and collector against the digest-pinned ClickHouse image:

```sh
OPEN_SPLUNK_HEC_LOAD=1 \
go test ./integration -run '^TestBackendHECDurableLoad$' \
  -count=1 -timeout=15m -v
```

Its default `mixed` 30-second workload offers a combined 1,000 HEC
events/second: 50 one-event requests/second plus 950 events/second in full
1,000-event requests. The gate requires at least 80 percent of each declared
shape budget and 90 percent of the combined budget to complete before the
ClickHouse outage. Acceptance is classified by response completion time, not
the intended schedule time, and either scheduler falling more than two seconds
behind fails the run. The summary reports accepted requests/second separately
from accepted events/second, so batching cannot hide transaction capacity.

The harness also runs native collector traffic and repeated administrator
token-state writes, pauses and unpauses ClickHouse, observes the bounded
durable backlog, waits for reconciliation and ACK truth, and verifies exact
combined row cardinality. The server runs with bounded runtime tracing so the
result records sampled Go heap, goroutine, and operating-system thread
high-water marks without enabling a public diagnostic endpoint.

The rate and outage can be changed for a short harness validation:

```sh
OPEN_SPLUNK_HEC_LOAD=1 \
OPEN_SPLUNK_HEC_LOAD_DURATION=8s \
OPEN_SPLUNK_HEC_LOAD_EVENTS_PER_SECOND=1000 \
OPEN_SPLUNK_HEC_LOAD_OUTAGE_AFTER=2s \
OPEN_SPLUNK_HEC_LOAD_OUTAGE_DURATION=2s \
go test ./integration -run '^TestBackendHECDurableLoad$' \
  -count=1 -timeout=15m -v
```

The durable harness enforces these ceilings:

- at most 64 pending HEC outbox requests and 256 MiB of pending HEC payload;
- at least 80 percent of each configured pre-outage shape budget and 90 percent
  of the combined pre-outage budget accepted by response completion time;
- no more than two seconds of request-scheduler lag;
- at most 512 MiB in sampled short-run Go GC heap (or the enforced long-run Go
  memory limit), 512 sampled goroutines, 768 MiB long-run resident memory, and
  128 operating-system threads; and
- complete post-outage queue/ACK drain with no duplicate ClickHouse event IDs.

The durable load harness restarts neither the server nor its SQLite recovery
unit. `TestBackendHECVertical` is the separate shipped-process proof that an
indexed acknowledgment remains true across a server restart.

Its tokens and event canaries are generated or synthetic and are checked
against server and collector logs before the test passes. Runtime summaries
contain only counts and durations.

### Request-rate and batch-rate profiles

The mixed release gate does not claim that the server sustains 500 independent
SQLite-backed HEC transactions/second. Use the observational `small-only`
profile as a rate matrix to preserve that distinct capacity evidence:

```sh
for rate in 50 75 100 250 500; do
  OPEN_SPLUNK_HEC_LOAD=1 \
  OPEN_SPLUNK_HEC_LOAD_PROFILE=small-only \
  OPEN_SPLUNK_HEC_LOAD_EVENTS_PER_SECOND="$rate" \
  go test ./integration -run '^TestBackendHECDurableLoad$' \
    -count=1 -timeout=15m -v || exit
done
```

This profile requires valid pacing and at least one accepted request, then
reports accepted request and event rates plus bounded capacity responses. It
is a saturation measurement, not the mixed release-rate gate. In the initial
equal-split diagnostic, 500 offered one-event requests/second produced about
60 accepted requests/second over the full 30 seconds. Its old `922` "warm"
counter used schedule time rather than response completion time, so it is
retained only as invalidated diagnostic context, not warm-throughput evidence.
Run the matrix with the corrected harness rather than treating 500
requests/second as a product SLA.

Use `batch-only` to prove that full-width batching alone carries the 1,000 EPS
target without being masked by the small-request path:

```sh
OPEN_SPLUNK_HEC_LOAD=1 \
OPEN_SPLUNK_HEC_LOAD_PROFILE=batch-only \
OPEN_SPLUNK_HEC_LOAD_EVENTS_PER_SECOND=1000 \
go test ./integration -run '^TestBackendHECDurableLoad$' \
  -count=1 -timeout=15m -v
```

### Shipped slow-compressed client gate

The real-listener deadline and resource proof is documented in the
[shipped HEC slow-client gate](hec-slow-client-gate.md). It holds all 16
per-token slots with authenticated gzip/chunked TLS clients through the
production 30-second read deadline, then proves bounded cleanup and a healthy
post-timeout ingest/ACK:

```sh
OPEN_SPLUNK_HEC_SLOW_CLIENT=1 \
go test ./integration \
  -run '^TestBackendHECSlowCompressedReadDeadline$' \
  -count=1 -timeout=15m -v
```

### Executable 24-hour shipped-process soak

The `soak` profile is the elapsed-time lifecycle gate. It deliberately does
not repeat the short 1,000 EPS throughput target for 24 hours:

The same release controls as the transport soak apply: obtain explicit
operator approval, use a quiescent AC-powered host with sleep prevented, and
run from a clean, committed, immutable revision. On one host, run the
transport and durable soaks sequentially so their resource measurements do
not contaminate one another; concurrent runs are evidence only when they use
separate isolated hosts built from the same revision.

```sh
OPEN_SPLUNK_HEC_LOAD=1 \
OPEN_SPLUNK_HEC_LOAD_PROFILE=soak \
OPEN_SPLUNK_HEC_LOAD_DURATION=24h \
go test ./integration -run '^TestBackendHECDurableLoad$' \
  -count=1 -timeout=26h -v
```

When no event-rate override is supplied, a long `soak` run uses 10 HEC
events/second: one single-event request/second plus 9 events/second in full
1,000-event requests. Over 24 hours it plans 87,178 HEC request/ACK rows and
about 864,400 HEC events. The plan validator rejects more than 96,000 retained
rows, leaving margin below the fixed 100,000 rows per token. It also scales
native ingestion to one event/second, administrator mutations to one/minute,
operational polling and resident-memory/thread sampling to once/30 seconds,
and detailed scheduler traces to once/30 minutes. `GOMEMLIMIT=512MiB` remains
active; the run rejects resident memory over 768 MiB, more than 512 live
goroutines, or more than 128 threads.

The fixture index retention is the greater of 24 hours or the requested load
duration plus one hour. A 24-hour soak therefore uses 25-hour retention, which
keeps its earliest rows outside the ClickHouse TTL horizon through the final
drain and exact-cardinality query.

The long profile requires at least 90 percent sustained acceptance for each
shape across the complete run and an accepted completion near the end, in
addition to the pre-outage gates. It still pauses ClickHouse, proves bounded
backlog and recovery, checks exact ClickHouse event-ID uniqueness, and checks
final ACK truth. The command is executable release machinery; it is not
evidence of a completed 24-hour run until its full output and timestamps have
been captured.

Before declaring the complete HEC load gate passed, release evidence must
include both durable harness output and:

- the shipped-process HTTPS HEC-to-SPL vertical:

  ```sh
  OPEN_SPLUNK_BACKEND_INTEGRATION=1 \
  go test ./integration -run '^TestBackendHECVertical$' \
    -count=1 -timeout=15m -v
  ```

- a passing [shipped slow-compressed client gate](hec-slow-client-gate.md);
  and
- a completed run of the 24-hour shipped-process `soak` profile above. The
  30-second 1,000 EPS gate proves throughput but does not replace this
  elapsed-time gate.

Record machine sizing, release revision, exact command and environment, start
and finish timestamps, request/event counts, queue high-water marks, recovery
time, heap/goroutine high-water marks, and whether any retry or invariant
failure occurred. Never record plaintext tokens, authorization headers,
channels, request bodies, or event fields.
