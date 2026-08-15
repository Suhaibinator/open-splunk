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
`clickhouse/clickhouse-server:26.7.3.19@sha256:f90a77560f72b10802106ee49e9870e41668cbc496e280c3911f6e3b216657f3`.
Set `OPEN_SPLUNK_CLICKHOUSE_TEST_IMAGE` to exercise another digest-pinned image
deliberately.

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

## Release OCI and full-stack Compose

`release_oci_integration_test.go` builds the production `server` and
`collector` images for the host Linux architecture through the production
`make oci` launcher. That launcher requires a clean worktree and materializes
the exact committed `HEAD` snapshot before either Docker target is built. The
test verifies both images' OCI identity and fixed non-root process contract,
then starts the canonical five-service Compose graph: the networkless
ClickHouse recovery-volume bootstrap, ClickHouse, the exact-image one-shot
ClickHouse migrator, the network-disabled administrator bootstrap, and the
long-running server. It does not add a collector to the default stack.

The test requires verified HTTPS health and embedded build identity, exact
container hardening, no published ClickHouse port, no ClickHouse or
administrator secret in the server environment/arguments, no one-shot
migration/backup/restore credential or recovery mount in the long-running
server, an isolated successful migrator with only its CA/secret mounts,
read-only runtime/deletion credential files, exact recovery-volume ownership,
rejection of the passwordless base ClickHouse user, and a successful
administrator API mutation. It reapplies the exact embedded release migrations
to prove the current ledger is idempotent, stops ClickHouse to prove readiness
becomes unavailable while HTTP liveness remains available, and requires
sustained readiness after ClickHouse restarts. It then rotates all six
ClickHouse credentials, atomically replaces all five service-principal
credential files, and force-recreates the recovery bootstrap, ClickHouse,
migrator, administrator bootstrap, and server. The recreated application must
be healthy with the same state volume and expose both pre- and post-rotation
SQLite records through the live protobuf API. The
separate ClickHouse principal integration mutates and restores both physical
table definitions and an unexpected third table while the ledger remains
complete; every mutation must fail the release schema validator. The Compose
integration rejects all previous principal credentials on the persistent
volume. Cleanup removes every test-owned container, network, volume, and image.

The same release test proves paired deployment disaster recovery rather than a
control-plane-only snapshot. It creates a bounded index, ingestion credential,
and ClickHouse event, stops the server, and uses the production recovery profile
to create and independently verify the coordinated recovery set. The retained
backup, marker-reconcile, verify, restore, and restore-retry one-offs are
inspected after exit for
their exact UID, read-only root filesystem, dropped capabilities,
no-new-privileges, PID limit, process arguments, environment, mounts, tmpfs,
network, disabled inherited healthcheck, and port contract before explicit
removal. After the backup boundary, the test commits both SQLite and ClickHouse
mutations, then runs the real restore helper while the original server remains
healthy and requires the retained shared singleton lock to reject it before a
ClickHouse connection or mutation. With the original deployment stopped, the
drill also copies one test-owned orphan archive, deletes only that name
through the production UID-`101` operator-attested one-shot, inspects the same
effective confinement boundary, retries the already-absent deletion, and proves
the published archive remains. Before post-backup mutations, the packaged drill
also seeds a test-owned stale source marker, stops and restarts only ClickHouse,
proves a mismatched confirmed identity cannot mutate it, clears the exact
identity through the production `deployment-marker-reconcile` container, and
retries the already-absent case. Those retained one-offs are inspected for the
exact image command, singleton-lock and credential mounts, network, secret
isolation, and hardening. It then rebinds fresh ClickHouse data/log,
server-state, and server-export volumes while retaining the exact two recovery
volumes and the deployment singleton-lock volume through the committed
`docker-compose.recovery-target.yaml` contract that must remain applied for the
restored deployment's lifetime. The test starts only fresh
ClickHouse, requires the canonical database to be absent, restores directly
into `open_splunk`, restores the same set again to prove exact receipt-gated
retry, and applies the production restore overlay so the retained
ClickHouse recovery volume is the same named volume but read-only in the fresh
ClickHouse container. The restore principal attests that exact disk/path/mode,
the command re-verifies the archive bytes after native restore, and the
archive-embedded recovery marker must match before receipt publication. The
receipt is then revalidated before exact marker consumption and proven absence;
there is no staging database, rename, or promotion boundary. The drill also
proves live and restored marker absence. Focused recovery tests
separately prove complete recovery-database namespace rejection and
outer-manifest binding of the control child.
The test validates the result with the embedded migrator,
and starts the server directly without recreating administrator bootstrap. The
restored deployment must recover the pre-backup token, indexes, and event, exclude both
post-backup mutations, and accept new authenticated ingestion, search, and
control-plane writes. The drill exercises both the real descriptor-bound SQLite
sidecar lock and the deployment-wide retained singleton lock, and rejects an
unsafe control target before ClickHouse mutation.
Cleanup inventories both the exact Compose-project and integration-project
labels, then removes the original and restored volumes as well as every
test-owned container, network, and image.

```sh
OPEN_SPLUNK_APPLICATION_VERSION=0.1.0 \
OPEN_SPLUNK_EXPECTED_SPL_COMPATIBILITY_VERSION=0.2 \
OPEN_SPLUNK_OCI_INTEGRATION=1 \
OPEN_SPLUNK_CLICKHOUSE_TEST_IMAGE=clickhouse/clickhouse-server:26.7.3.19@sha256:f90a77560f72b10802106ee49e9870e41668cbc496e280c3911f6e3b216657f3 \
  go test ./integration -run '^TestReleaseOCIComposeContract$' \
    -count=1 -timeout=25m -v
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
