# Backend vertical integration

`backend_vertical_test.go` exercises the real deployable path:

1. start pinned, ephemeral ClickHouse on a random loopback port;
2. build and start `open-splunk-server` with temporary SQLite and key files;
3. provision an index and one-time ingestion token over protobuf HTTP;
4. build and start `open-splunk-collector` against a deterministic `app.log`;
5. wait for acknowledged gRPC ingestion, run SPL through protobuf HTTP, and
   verify typed numeric values plus mandatory field/raw redaction.

Run it explicitly because Docker and binary builds are intentionally excluded
from the default unit-test loop:

```sh
OPEN_SPLUNK_BACKEND_INTEGRATION=1 go test ./integration -run '^TestBackendVertical$' -count=1 -v
```

The default image is `clickhouse/clickhouse-server:26.3.17.4`. Set
`OPEN_SPLUNK_CLICKHOUSE_TEST_IMAGE` to exercise another image deliberately.
