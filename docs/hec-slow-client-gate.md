# Shipped HEC slow-client resource gate

`TestBackendHECSlowCompressedReadDeadline` is the opt-in release gate for
slow, compressed HEC clients against the shipped server. It owns a pinned
ClickHouse container, builds the real `open-splunk-server` binary, serves HEC
on its TLS listener, and waits through the fixed production 30-second HTTP
read timeout. Budget at least two minutes for image startup, builds, and the
deliberate timeout window.

Run the gate from the repository root:

```sh
OPEN_SPLUNK_HEC_SLOW_CLIENT=1 \
  go test ./integration \
    -run '^TestBackendHECSlowCompressedReadDeadline$' \
    -count=1 -timeout=15m -v
```

The default digest-pinned ClickHouse image is used unless
`OPEN_SPLUNK_CLICKHOUSE_TEST_IMAGE` selects another digest-pinned image.

The gate warms the normal durable HEC-to-ClickHouse path, then opens sixteen
authenticated gzip and chunked HTTP/1.1 requests over independent TLS
connections. Each request sends a complete gzip member containing a bounded,
incomplete JSON envelope and withholds the terminal HTTP chunk. A seventeenth
request must receive HEC code 9, proving the production per-token admission
envelope is held before the test waits for the listener deadline.

Acceptance requires:

- every held connection completes 25–40 seconds after it opened, around the
  shipped 30-second read timeout, with either the bounded HEC invalid-data
  response or a deadline connection close;
- a normal HEC ingest and acknowledgment succeeds afterward, and the
  administrator operational snapshot shows no pending outbox or ACK rows;
- complete scheduler samples bound the server to 256 goroutines and 128
  operating-system threads, with post-timeout goroutines settling within 32
  of the warmed baseline;
- Go GC observations keep peak heap at or below 256 MiB and post-timeout live
  heap within 32 MiB of the warmed baseline; and
- captured server logs contain no administrator token, HEC token or prefix,
  ClickHouse password, channel, or payload canary.

The test emits only aggregate counts, durations, and runtime high-water marks.
Do not add authorization headers, channels, bodies, event fields, or plaintext
tokens to the runbook evidence.
