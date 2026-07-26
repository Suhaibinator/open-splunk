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
14. insert a deterministic 10,001-row fixture into a separately provisioned
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

The default image is `clickhouse/clickhouse-server:26.3.17.4`. Set
`OPEN_SPLUNK_CLICKHOUSE_TEST_IMAGE` to exercise another image deliberately.
