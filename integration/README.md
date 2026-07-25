# Backend vertical integration

`backend_vertical_test.go` exercises the real deployable path:

1. build the static UI in backend mode;
2. start pinned, ephemeral ClickHouse on a random loopback port;
3. build and start `open-splunk-server` from an empty working directory with
   temporary SQLite/key files and no executable runtime on `PATH`;
4. provision an index and one-time ingestion token over protobuf HTTP;
5. build and start `open-splunk-collector` against an empty `app.log`, append
   and durably acknowledge one primer event, then append and fsync the remaining
   generated fixture;
6. require the collector's durable checkpoint to reach both synced fixture
   boundaries and its WAL to drain with a positive acknowledged sequence;
7. create an SPL job through protobuf
   HTTP, and subscribe to its binary protobuf WebSocket stream;
8. require an explicit subscription acknowledgment followed by monotonically
   sequenced search state/progress events and a completed terminal event, then
   fetch the authoritative typed/redacted results over two opaque cursor pages;
9. launch Chromium against the UI embedded in that compiled server, run an SPL
   search, observe its same-origin protobuf HTTP and binary WebSocket traffic,
   and verify the final non-preview event rows contain the ingested fixture;
10. create and poll a JSON Lines export, redeem its one-time bearer grant over
   the raw download route, validate artifact headers/content, and reject grant
   replay;
11. insert a deterministic 10,001-row fixture into a separately provisioned
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
