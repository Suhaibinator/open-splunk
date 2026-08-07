# Deploy the Open Splunk collector container

The collector is a separate, non-root image intended to run on each
log-producing host. It tails host files, sanitizes events before persistence,
queues them in a local durable WAL, and sends them to the Open Splunk collector
gRPC listener. It does not delete, truncate, rotate, or compact source logs.

This guide deploys one collector with Docker. The same mount, identity, and
secret contracts apply to another container orchestrator.

## 1. Choose and pull an image

Release images are published as:

```text
ghcr.io/suhaibinator/open-splunk-collector:X.Y.Z
```

Pushing the corresponding `vX.Y.Z` Git release tag publishes the image without
the leading `v`.

Use an exact semantic-version tag in production. A release version containing
SemVer build metadata replaces `+` with `_` in the OCI tag. Each release tag is
a multi-platform manifest containing `linux/amd64` and `linux/arm64`; an
ordinary `docker pull` selects the host architecture automatically. Do not set
`--platform` unless deliberately running a non-native image through emulation.

```sh
export OPEN_SPLUNK_COLLECTOR_VERSION=0.1.0
export OPEN_SPLUNK_COLLECTOR_IMAGE="ghcr.io/suhaibinator/open-splunk-collector:${OPEN_SPLUNK_COLLECTOR_VERSION}"
docker pull "$OPEN_SPLUNK_COLLECTOR_IMAGE"
docker image inspect --format '{{.Os}}/{{.Architecture}}' "$OPEN_SPLUNK_COLLECTOR_IMAGE"
```

Record the pulled digest with the deployment inventory if the local release
policy requires digest pinning.

## 2. Make the server reachable

The server must expose its collector gRPC listener with TLS to the collector
host. The checked-in production-shaped Compose deployment publishes that
listener only at `127.0.0.1:4317`. A collector on another host cannot reach it.
Before deploying a remote collector, change the Compose `server` port
publication to a deliberately selected routable interface, restrict port 4317
with the host firewall, and confirm the route from the collector host. Do not
publish ClickHouse.

For example, replace only the collector port publication in
`deploy/docker-compose.yaml`:

```yaml
# Local-only default:
- "127.0.0.1:${OPEN_SPLUNK_SERVER_GRPC_PORT:-4317}:4317"

# Example private server-host address; choose the real interface deliberately:
- "10.0.0.5:${OPEN_SPLUNK_SERVER_GRPC_PORT:-4317}:4317"
```

Permit TCP 4317 only from intended collector addresses. The listener inside
the server container already uses `0.0.0.0:4317`; the host publication is the
remote-reachability boundary.

`deploy/generate-env.sh` issues the server certificate for
`open-splunk-server` (also `localhost` and loopback). The example configuration
therefore sets TLS `server_name` to `open-splunk-server` even when
`server.address` uses the deployment host's DNS name or IP. Copy the generated
CA file named by `OPEN_SPLUNK_SERVER_TLS_CA_FILE` to the collector host through
a trusted configuration channel. Do not copy the server certificate private
key.

For a collector container on the same Linux host as the unmodified Compose
stack, host networking plus `127.0.0.1:4317` is possible, but a remote-host
deployment should use an explicit routable address instead. Plaintext remote
gRPC exposes the bearer token and is rejected by default; keep TLS enabled.

## 3. Prepare configuration, state, CA, and log access

The image runs as numeric UID/GID `65532:65532`. Use a dedicated host state
directory owned by that UID. It must be a real directory, not a symlink, below
a trusted non-group/world-writable parent. Do not use a filesystem root, the
container working directory, or one state directory for multiple collectors.

The following paths are used by the commands in this guide:

```sh
sudo install -d -o root -g root -m 0755 /etc/open-splunk-collector
sudo install -d -o 65532 -g 65532 -m 0700 /etc/open-splunk-collector/private
sudo install -d -o 65532 -g 65532 -m 0700 /var/lib/open-splunk-collector/state

sudo install -o root -g root -m 0444 \
  ./configs/examples/collector-container.yaml \
  /etc/open-splunk-collector/collector.yaml
sudo install -o root -g root -m 0444 \
  ./deploy/.env.tls/ca.crt \
  /etc/open-splunk-collector/ca.crt
```

The CA source above is the default location created by `deploy/generate-env.sh`
when this repository is the server host. On a remote collector host, replace it
with the securely delivered CA path.

Create `/etc/open-splunk-collector/collector.env` with non-secret deployment
values. Docker's `--env-file` format does not perform shell expansion, so use
literal values:

```dotenv
OPEN_SPLUNK_SERVER_GRPC_ADDRESS=splunk.example.internal:4317
OPEN_SPLUNK_SERVER_TLS_SERVER_NAME=open-splunk-server
OPEN_SPLUNK_COLLECTOR_TOKEN_FILE=/run/open-splunk/secrets/collector.token
OPEN_SPLUNK_SERVER_TLS_CA_FILE=/run/open-splunk/tls/ca.crt
OPEN_SPLUNK_COLLECTOR_STATE_DIRECTORY=/var/lib/open-splunk-collector/state
OPEN_SPLUNK_LOG_GLOB=/var/log/source/*.log
OPEN_SPLUNK_INDEX=application
OPEN_SPLUNK_LOG_SOURCE=application
OPEN_SPLUNK_LOG_SOURCETYPE=json
OPEN_SPLUNK_LOG_HOST=app-01.example.internal
OPEN_SPLUNK_SERVICE=application
OPEN_SPLUNK_ENVIRONMENT=production
```

Install that file mode `0444` or `0644`; it must not contain either bearer
token. Edit the input `format`, `start_at`, metadata, and processors in
`collector.yaml` before bootstrap. The example expects newline-delimited JSON.
Choose `start_at: beginning` only when an initial backfill is intended.

```sh
sudo chown root:root /etc/open-splunk-collector/collector.env
sudo chmod 0444 /etc/open-splunk-collector/collector.env
```

The collector UID must be able to traverse the mounted source directory and
read current and newly rotated log files. Prefer granting a dedicated log group
read access and supply its numeric GID with `--group-add` below; alternatively,
use a narrowly scoped host ACL for UID 65532. Do not make logs world-writable or
change the state directory away from UID 65532. Set the deployment's log path
and group:

```sh
export OPEN_SPLUNK_HOST_LOG_DIRECTORY=/var/log/application
export OPEN_SPLUNK_HOST_LOG_GID=1234
```

Keep host log rotation and retention enabled. Size `state.max_queue_bytes` and
the state filesystem for the longest expected server outage, and retain source
log rotations long enough for a backpressured collector to catch up.

## 4. Initialize the stable collector identity

Run `identity` against the final state mount before requesting a token. The
token file may still be absent. The command does not scan inputs, open the WAL,
or contact the server; it creates or reuses `state/collector_id` and prints the
ID only.

```sh
export OPEN_SPLUNK_COLLECTOR_ID="$(
  docker run --rm \
    --user 65532:65532 \
    --read-only \
    --cap-drop ALL \
    --security-opt no-new-privileges:true \
    --env-file /etc/open-splunk-collector/collector.env \
    --mount type=bind,src=/etc/open-splunk-collector/collector.yaml,dst=/etc/open-splunk/collector.yaml,readonly \
    --mount type=bind,src=/var/lib/open-splunk-collector/state,dst=/var/lib/open-splunk-collector/state \
    "$OPEN_SPLUNK_COLLECTOR_IMAGE" \
    identity -config /etc/open-splunk/collector.yaml
)"
printf '%s\n' "$OPEN_SPLUNK_COLLECTOR_ID"
```

Treat the complete state directory as the identity. Never regenerate, edit, or
copy `collector_id` independently of its WAL, checkpoints, and dead-letter
state. The collector fails closed when prior runtime state exists without a
valid identity.

## 5. Create the index and collector-bound token

The configured index must already exist, be active, and have ingestion access
enabled. Using an authenticated administrator API client:

1. Create the index with `POST /api/v1/indexes/create` if it does not exist.
2. Create a token with `POST /api/v1/ingestion-tokens/create` whose
   `definition.constraints.allowed_index_names` includes every configured
   input index and whose `definition.constraints.bound_collector_id` exactly
   equals the ID printed above.
3. Capture `CreateIngestionTokenResponse.plaintext_token` at creation time. It
   is returned once; later get/list responses do not contain it.

If the token also sets optional `allowed_host_regexes` or
`allowed_source_regexes`, they must match the complete configured `host` and
`source` values respectively.

These HTTP endpoints accept `application/x-protobuf` and require
`Authorization: Bearer <administrator-token>`. The current browser does not
provide the administrator-token workflow, and this repository does not ship an
administrator CLI, so use an API client generated from
`proto/open_splunk/v1/index_api.proto` and
`proto/open_splunk/v1/collector_admin_api.proto`. Do not give the collector the
administrator token, ClickHouse credentials, or server key material.

Deliver only the one-time collector token to the host secret path. Avoid shell
history and command arguments; the following assumes a secret manager has
materialized a temporary owner-readable file:

```sh
sudo install -o 65532 -g 65532 -m 0600 \
  /path/from/secret-manager/collector.token \
  /etc/open-splunk-collector/private/collector.token
```

## 6. Validate and run

`validate` parses the configuration and reports input glob match counts. It
does not contact the server or prove that the token and CA can be used; the
normal `run` command performs those checks.

```sh
docker run --rm \
  --user 65532:65532 \
  --read-only \
  --cap-drop ALL \
  --security-opt no-new-privileges:true \
  --group-add "$OPEN_SPLUNK_HOST_LOG_GID" \
  --env-file /etc/open-splunk-collector/collector.env \
  --mount type=bind,src=/etc/open-splunk-collector/collector.yaml,dst=/etc/open-splunk/collector.yaml,readonly \
  --mount type=bind,src=/etc/open-splunk-collector/private/collector.token,dst=/run/open-splunk/secrets/collector.token,readonly \
  --mount type=bind,src=/etc/open-splunk-collector/ca.crt,dst=/run/open-splunk/tls/ca.crt,readonly \
  --mount type=bind,src=/var/lib/open-splunk-collector/state,dst=/var/lib/open-splunk-collector/state \
  --mount type=bind,src="$OPEN_SPLUNK_HOST_LOG_DIRECTORY",dst=/var/log/source,readonly \
  "$OPEN_SPLUNK_COLLECTOR_IMAGE" \
  validate -config /etc/open-splunk/collector.yaml
```

If validation reports the intended files, start the long-running container:

```sh
docker run --detach \
  --name open-splunk-collector \
  --restart unless-stopped \
  --stop-timeout 30 \
  --user 65532:65532 \
  --read-only \
  --cap-drop ALL \
  --security-opt no-new-privileges:true \
  --pids-limit 256 \
  --group-add "$OPEN_SPLUNK_HOST_LOG_GID" \
  --env-file /etc/open-splunk-collector/collector.env \
  --mount type=bind,src=/etc/open-splunk-collector/collector.yaml,dst=/etc/open-splunk/collector.yaml,readonly \
  --mount type=bind,src=/etc/open-splunk-collector/private/collector.token,dst=/run/open-splunk/secrets/collector.token,readonly \
  --mount type=bind,src=/etc/open-splunk-collector/ca.crt,dst=/run/open-splunk/tls/ca.crt,readonly \
  --mount type=bind,src=/var/lib/open-splunk-collector/state,dst=/var/lib/open-splunk-collector/state \
  --mount type=bind,src="$OPEN_SPLUNK_HOST_LOG_DIRECTORY",dst=/var/log/source,readonly \
  "$OPEN_SPLUNK_COLLECTOR_IMAGE"
```

For the same-host Linux/loopback case described earlier, add `--network host`
to all commands that need server connectivity and set the server address to
`127.0.0.1:4317`. `identity` and `validate` do not contact the server.

Follow startup and connection status with:

```sh
docker logs --follow open-splunk-collector
docker inspect --format '{{.State.Status}} restarts={{.RestartCount}}' open-splunk-collector
```

A healthy connection logs `collector stream ready`. Also verify the collector
through the administrator `POST /api/v1/collectors/get` or `/collectors/list`
API and confirm that the expected input and authorized index appear. The image
does not expose an HTTP health endpoint, so monitor the container restart count,
server-side collector heartbeat/queue telemetry, state-disk utilization, and
source-log retention headroom.

## Operations

### Restarts and upgrades

SIGTERM stops input reads, flushes a partial batch to the WAL, and gives queued
data a bounded drain window. The 30-second Docker stop timeout allows that
graceful path; do not routinely use `docker kill`. Any unacknowledged batches
remain on disk and replay after restart.

To upgrade, pull the new exact version, stop and remove only the container, and
recreate it with the same configuration, token, CA, source, and complete state
directory mounts. Never remove or replace the host state directory. Run the new
image's `validate` first. Roll back the container image the same way while
retaining state; if a release reports an incompatible state migration, follow
that release's instructions rather than manually editing files.

Back up or migrate collector state only while the container is stopped. Copy
the entire state directory as one filesystem-consistent unit and restore it
with UID/GID `65532:65532` and owner-only permissions. A state snapshot and
source-log retention must be coordinated if exact continuity after host loss is
required.

### Token rotation

Create a replacement token authorized for the same indexes and bound to the
same output of the `identity` command. Stop the collector, atomically install
the replacement host token as UID 65532 mode `0600`, then remove and recreate
the container so Docker establishes a fresh read-only file bind mount. Confirm
`collector stream ready` and server-side heartbeat with the new credential
before revoking the old token. The collector reads the token when it connects;
replacing a file does not change an already authenticated stream.

Never rotate identity as part of credential rotation. Losing the token does not
require losing or recreating collector state.

### WAL, checkpoints, and disk pressure

The collector owns these state paths:

```text
state/collector_id          stable security identity
state/.collector.lock       single-process state-directory lock
state/wal/                  durable unacknowledged batches
state/checkpoints/          per-file terminal read positions
state/dead-letter.jsonl     append-only permanently rejected events
```

Acknowledged sealed WAL segments are reclaimed automatically. There is no
supported manual WAL compaction, acknowledgment, or replay-edit command. Do not
delete files under `wal/` or `checkpoints/` to free space: that can discard
unacknowledged data, break source-coordinate continuity, or cause duplicate
ingestion. When the configured WAL limit is reached, the collector stops
reading source files until delivery frees space; it does not delete source
logs. Restore server reachability or increase capacity and source-log retention
rather than clearing the queue.

WAL recovery can quarantine a corrupt tail and all later segments as
`*.corrupt` so a missing source range cannot be skipped. Preserve the complete
state directory and collector logs before intervention. There is currently no
supported repair/import command; investigate the storage failure and recover
from a coordinated state/source backup instead of renaming quarantined files
back into service.

### Dead letters

`dead-letter.jsonl` contains full event payloads that were permanently rejected
by the server or could not fit as one WAL record. It is mode `0600`, sensitive,
append-only, and is not automatically compacted. Alert on a nonzero/growing
file, inspect rejection codes without publishing payloads, correct the index,
token, event-size, or schema cause, and use an explicitly reviewed external
process if those events should be transformed and resubmitted.

To archive the file for retention, stop the collector first, move it to an
owner-only protected archive, and restart the collector; it creates a new
owner-only file. Never edit it while the collector is running, and never treat
removing dead letters as successful ingestion. Keep `collector_id`, WAL, and
checkpoints untouched.
