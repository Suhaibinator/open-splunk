# Backend vertical integration

`backend_vertical_test.go` exercises the real deployable path:

1. start pinned, ephemeral ClickHouse on a random loopback port;
2. build and start `open-splunk-server` with temporary SQLite and key files;
3. provision an index and one-time ingestion token over protobuf HTTP;
4. build and start `open-splunk-collector` against a deterministic `app.log`;
5. wait for acknowledged gRPC ingestion, create an SPL job through protobuf
   HTTP, and subscribe to its binary protobuf WebSocket stream;
6. require an explicit subscription acknowledgment followed by monotonically
   sequenced search state/progress events and a completed terminal event, then
   fetch the authoritative job and full typed/redacted results over HTTP;
7. create and poll a JSON Lines export, redeem its one-time bearer grant over
   the raw download route, validate artifact headers/content, and reject grant
   replay;
8. insert a deterministic 10,001-row fixture into a separately provisioned
   index, prove the interactive snapshot reports its 10,000-row truncation,
   and prove bounded export re-execution downloads all 10,001 unique rows.

The WebSocket subscription uses `after_sequence = 0`, so the test accepts the
current terminal snapshot when the small search finishes before the upgrade.
WebSocket frames carry only bounded notifications; full result rows are always
retrieved from the authoritative protobuf HTTP results endpoint.

Run it explicitly because Docker and binary builds are intentionally excluded
from the default unit-test loop:

```sh
OPEN_SPLUNK_BACKEND_INTEGRATION=1 go test ./integration -run '^TestBackendVertical$' -count=1 -v
```

The default image is `clickhouse/clickhouse-server:26.3.17.4`. Set
`OPEN_SPLUNK_CLICKHOUSE_TEST_IMAGE` to exercise another image deliberately.
