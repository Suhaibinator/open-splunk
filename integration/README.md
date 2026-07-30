# Backend vertical integration

`backend_vertical_test.go` exercises the real deployable path:

1. build the static UI in backend mode;
2. start pinned, ephemeral ClickHouse on a random loopback port;
3. build and start `open-splunk-server` from an empty working directory with
   temporary SQLite/key files and no executable runtime on `PATH`;
4. provision an index and one-time ingestion token over protobuf HTTP;
5. build and start `open-splunk-collector` against an empty `app.log`, append
   and durably acknowledge one primer event, then hard-kill the collector;
6. inspect the valid checkpoint/WAL crash boundary, restart the collector with
   the same state, append two generated events, and require their durable
   checkpoint and WAL acknowledgment high-water to settle;
7. hard-kill the server, append and fsync a final sentinel while it is down,
   wait for and explicitly sync the real WAL segment append, then hard-kill the
   collector;
8. reopen the WAL and require the exact pending sentinel origin, restart both
   processes with their original durable state, and prove four distinct stable
   event IDs with no loss. One physical sentinel replay is allowed by the
   at-least-once contract, and search uses `dedup event_id` to return the four
   logical events;
9. require the final exact line/byte checkpoint and a drained collector WAL
   with a positive acknowledged sequence;
10. create an SPL job through protobuf
   HTTP, and subscribe to its binary protobuf WebSocket stream;
11. require an explicit subscription acknowledgment followed by monotonically
   sequenced search state/progress events and a completed terminal event, then
   fetch the authoritative typed/redacted results over two opaque cursor pages;
12. launch Chromium against the UI embedded in that compiled server, run an SPL
   search, observe its same-origin protobuf HTTP and binary WebSocket traffic,
   and verify the final non-preview event rows contain the ingested fixture;
13. create and poll a JSON Lines export, redeem its one-time bearer grant over
   the raw download route, validate artifact headers/content, and reject grant
   replay;
14. provision a separate `gradethis` index and index-scoped token, run the
   shipped `configs/examples/collector.yaml` through `collector validate`, and
   start that exact configuration against an empty synthetic GradeThis log;
15. append and fsync the scanner-validated 20-event current GradeThis profile,
   then require exact distinct ClickHouse rows, trusted
   host/source/sourcetype/service/environment metadata, an EOF checkpoint, a
   drained acknowledged WAL, and no token leakage. The profile is rebased to a
   recent UTC window so maximum-event-age and logical-retention policy remain
   active; its default form remains byte-pinned in unit tests;
16. execute six current-source SPL investigations through protobuf HTTP with
   exact typed cursor-paged results: trace flow, severity counts, failed
   requests, path/status counts, mixed Go duration units (`µs`, `ms`, `s`), and
   top messages. This path starts no OpenTelemetry collector;
17. insert a deterministic 10,001-row fixture into a separately provisioned
   index, prove the interactive snapshot reports its 10,000-row truncation,
   and prove bounded export re-execution downloads all 10,001 unique rows.

The WebSocket subscription uses `after_sequence = 0`, so the test accepts the
current terminal snapshot when the small search finishes before the upgrade.
WebSocket frames carry only bounded notifications; full result rows are always
retrieved from the authoritative protobuf HTTP results endpoint.

Install the pinned Chromium build once, then run the test explicitly because
Docker, browser automation, and binary builds are intentionally excluded from
the default unit-test loop:

```sh
npm ci
npx --no-install playwright install chromium
OPEN_SPLUNK_BACKEND_INTEGRATION=1 go test ./integration -run '^TestBackendVertical$' -count=1 -v
```

Set `OPEN_SPLUNK_BROWSER_EXECUTABLE` to use a specific Chromium-family browser
instead of Playwright's pinned download.

The default image is
`clickhouse/clickhouse-server:26.3.17.4@sha256:85c434814ac8905e5648027ce926f74ab067edd6aadbccb6c0c165cd3571ea49`.
Set `OPEN_SPLUNK_CLICKHOUSE_TEST_IMAGE` to exercise another digest-pinned image
deliberately.

## Physical index deletion API lifecycle

`backend_index_data_deletion_test.go` exercises administrator `DELETE_DATA`
admission through the compiled server and its real SQLite/ClickHouse
dependencies. It rejects malformed, unauthorized, missing, stale, active, and
misconfirmed requests without durable or physical effects; then issues 16
concurrent exact admissions and requires one stable operation and one
correlated ClickHouse mutation.

The test uses `SYSTEM STOP MERGES` to hold that mutation deterministically,
hard-kills the server, reopens SQLite to prove the pending operation and
mutation attempt are durable, restarts the server, and retries through the
normalized name selector. After merges resume, only the trusted tenant/index
rows disappear: same-name rows for a foreign tenant and neighboring-index rows
for the trusted tenant survive. A terminal hard stop followed by repeated
SQLite reopen verifies the immutable completion audit, consumed work records,
retained deleting generation, tombstone, and permanent name reservation.
Stable operation-ID replay is intentionally bounded to the outstanding
operation: once terminal completion hides the catalog entry, the exact same
HTTP request returns `404 Not Found`.

The ClickHouse image must include an exact `@sha256:` digest:

```sh
OPEN_SPLUNK_BACKEND_INTEGRATION=1 \
  go test ./integration -run '^TestBackendIndexDataDeletionLifecycle$' -count=1 -v
```

## Fixed-result browser rendering

`browser_rendering_test.go` isolates browser rendering from ClickHouse and
query latency. It runs the compiled backend UI and production protobuf HTTP
handler against a deterministic executor that returns exactly 1,000 statistics
rows at the browser's 64-column boundary. The test verifies the decoded
response, exact total and page contract, bounded virtualized rows and cells at
the first and last rows, fixed table width, ARIA row positions, sorting, table
density changes, and browser/API safety.
The browser rejects a result page that exceeds its requested row count or the
explicit 64-column interactive-table limit before adapting or rendering it.

Run it with the same pinned Chromium installation:

```sh
OPEN_SPLUNK_BACKEND_INTEGRATION=1 go test ./integration -run '^TestBrowserFixedResultRendering$' -count=1 -v
```

The test waits for semantic completion, a mutation-free turn, and two animation
frames before recording rendering measurements. Durations, long tasks, layout
work, heap use, and DOM counts are observational metrics, not machine-dependent
latency acceptance thresholds. Exact payload semantics and the maximum number
of materialized rows remain correctness gates. Metrics and top/bottom
screenshots are written beneath
`test-results/browser-fixed-result-rendering/visual`.
