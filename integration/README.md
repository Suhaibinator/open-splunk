# Backend vertical integration

## Development workflow smoke test

The opt-in development workflow gate generates a Git-independent development
environment on random loopback ports, starts one pinned plaintext ClickHouse
container but no Open Splunk server, builds
the current tree as `development`, reaches readiness with the host-native
server, and proves its private control state survives a graceful restart:

```sh
OPEN_SPLUNK_DEVELOPMENT_INTEGRATION=1 \
  go test ./integration -run '^TestDevelopmentWorkflow$' -count=1 -timeout=10m -v
```

It uses a unique Compose project and deletes only its own temporary volumes and
host files during cleanup.

`backend_vertical_test.go` exercises the real deployable path:

1. build the static UI in backend mode;
2. start pinned, ephemeral ClickHouse on a random loopback port;
3. build and start `open-splunk-server` from an empty working directory with
   temporary SQLite/key files and no executable runtime on `PATH`;
4. provision an index, app-scoped immutable lookup, saved lookup search, and
   one-time ingestion token over protobuf HTTP;
5. build and start `open-splunk-collector` against an empty `app.log`, append
   and durably acknowledge one primer event, then hard-kill the collector;
6. inspect the valid checkpoint/WAL crash boundary, restart the collector with
   the same state, append two generated events, and require their durable
   checkpoint and WAL acknowledgment high-water to settle;
7. hard-kill the server, append and fsync a final sentinel while it is down,
   wait for and explicitly sync the real WAL segment append, then hard-kill the
   collector;
8. reopen the WAL and require the exact pending sentinel origin, restart both
   processes with their original durable state, reload the exact app, lookup,
   and saved-search versions, and prove four distinct stable event IDs with no
   loss. One physical sentinel replay is allowed by the at-least-once contract,
   and search uses `dedup event_id` to return the four logical events;
9. require the final exact line/byte checkpoint and a drained collector WAL
   with a positive acknowledged sequence;
10. execute the persisted lookup search through protobuf HTTP with saved-search
    provenance, and subscribe to its binary protobuf WebSocket stream;
11. require an explicit subscription acknowledgment followed by monotonically
   sequenced search state/progress events and a completed terminal event, then
   fetch the authoritative typed/redacted lookup-enriched results over two
   opaque cursor pages, and verify the pinned lookup identity through
   administrator inspection;
12. launch Chromium against the UI embedded in that compiled server, run an SPL
   lookup search, observe its same-origin protobuf HTTP and binary WebSocket
   traffic, and verify the expanded final event rows contain the typed lookup
   enrichment;
13. create and poll a JSON Lines export, redeem its one-time bearer grant over
   the raw download route, validate lookup-enriched artifact headers/content,
   and reject grant replay;
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
`clickhouse/clickhouse-server:26.7.5.10-alpine@sha256:0a45b864c73322d4360dea1973ee9b77f29c51af1242ad2d47409908071fa56e`.
Set `OPEN_SPLUNK_CLICKHOUSE_TEST_IMAGE` to exercise another digest-pinned image
deliberately.

## Sustained native ingestion and search load

`backend_load_test.go` builds the browser/server, collector, and log generator,
then admits a deterministic 30,000-event source at 1,000 events per second. It
proves an acknowledged warm boundary, a durable WAL backlog while the server is
down, collector crash/restart with that backlog, recovery while source
generation continues, and concurrent bounded searches over monotonically
advancing visibility snapshots. Final checks require exact source, raw-row,
cardinality, checkpoint, WAL-drain, dead-letter, and secret-redaction results.
Throughput, outage/recovery, storage, and search-latency measurements are logged
as observational evidence rather than portable timing promises.

Run this resource-intensive gate explicitly:

```sh
OPEN_SPLUNK_BACKEND_LOAD=1 \
  go test ./integration -run '^TestBackendSustainedLoad$' \
    -count=1 -timeout=12m -v
```

It starts the repository-pinned ClickHouse image unless
`OPEN_SPLUNK_CLICKHOUSE_TEST_IMAGE` deliberately selects another digest-pinned
image.

## Browser stream recovery and cancellation

Five deterministic shipped-browser gates exercise expired retained sequences,
live sequence gaps, REST-terminal and REST-first recovery, and authoritative
search cancellation without reconnect. They run the real compiled browser
against a bounded test server and write failure diagnostics beneath the
corresponding `test-results/browser-*` directory.

After installing the pinned Chromium build described above, run the complete
family with:

```sh
OPEN_SPLUNK_BACKEND_INTEGRATION=1 \
  go test ./integration \
    -run '^TestBrowser(SearchCancellation|Sequence(ExpiredRecovery|Gap(REST(FirstProgress|Terminal))?Recovery))$' \
    -count=1 -timeout=20m -v
```

## HEC ingestion-to-SPL vertical

`backend_hec_test.go` builds the shipped server, enables the default-off HEC
surface, provisions an administrator-created HEC token, and sends concatenated
JSON and raw events over TLS. It pauses ClickHouse to prove ACKs remain pending,
then verifies exact typed/provenance projection, public SPL base search,
grouped `stats`, and `timechart`. A hard server restart proves retained ACK
state; an independent retry proves documented at-least-once behavior without
assuming ACK IDs are sequential. The test also checks logs and audit output for
credential leakage.

```sh
OPEN_SPLUNK_BACKEND_INTEGRATION=1 \
  go test ./integration -run '^TestBackendHECVertical$' -count=1 -timeout=8m -v
```

## ClickHouse migration integration

The opt-in migration gate starts the pinned ClickHouse image, connects over
its plaintext native listener with one account, applies the same embedded
migrations used by normal server startup twice, and verifies the migration
ledger remains idempotent:

```sh
OPEN_SPLUNK_CLICKHOUSE_INTEGRATION=1 \
  go test ./migrations/clickhouse -run '^TestMigrationsAgainstClickHouse$' \
    -count=1 -timeout=8m -v
```

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

## Global stylesheet computed-style contracts

`visual/css-contracts.spec.ts` pins the behaviour of `app/globals.css` that
unit tests used to pin by matching the stylesheet's raw text. Text matching
tied the suite to formatting — newline placement, single-line media-query
bodies, declaration order — so any tokenising or reformatting pass broke it
without changing a rendered pixel. The spec instead loads the stylesheet into
Chromium against fixture markup that mirrors the production DOM and reads
resolved values through `getComputedStyle` at the 1280, 980, 760, and 480 pixel
breakpoints.

It needs no server, no container, and no backend fixtures, so it runs in the
default loop once the pinned Chromium build is installed:

```sh
npx --no-install playwright install chromium
npm run test:contracts
```

Failure artifacts are written beneath `test-results/css-contracts`.
