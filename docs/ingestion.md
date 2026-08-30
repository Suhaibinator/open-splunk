# Native ingestion and collector operations

The Open Splunk collector is a separate non-root process deployed beside log
files. It tails files, normalizes and sanitizes events, persists batches in a
local WAL, and sends them over the `open_splunk` gRPC stream. It never
deletes, truncates, rotates, or compacts source logs.

The server admits native and HEC events through one transport-neutral policy,
quota, visibility, outbox, redaction, and ClickHouse path. This document covers
the native collector; its complete CLI, YAML, environment-template, default,
processor, and TLS surface is documented in
[Collector configuration](collector-configuration.md). HEC is documented
separately in [HEC](hec.md).

## Token and collector authority

A native ingestion token has immutable native purpose, at least one explicit
allowed index, and exactly one bound collector ID. HEC-purpose tokens cannot
open the collector stream, and native tokens cannot authenticate HEC. Token
create returns the plaintext once; later reads return only safe metadata.

At stream admission and every protected heartbeat or batch boundary, the
server refreshes token state, collector enabled state, allowed indexes, index
state, constraints, and rate policy. A request may use only its captured
snapshot. A committed batch is not reinterpreted after policy changes; the next
fresh boundary observes them.

The server retains at most 256 durable collector identities per tenant and 16
live collectors per process. A previously unseen identity at catalog capacity
fails without recording token use or partial fleet state. Existing enabled
collectors may reconnect; disabled collectors continue to fail as disabled.

### Browser recovery for one-time token creation

The Administration page stores a non-secret recovery guard before sending a
token-create request. The guard contains the requested definition, baseline
token identities, server-clock timing, and an attempt identity; it never
contains the plaintext token. This lets the browser reconcile a timeout,
connection loss, reload, or tab closure without silently creating a second
live credential.

An unresolved guard pauses only **Generate token**. It does not block links,
browser navigation, authentication, or other administration work. A persistent
banner appears in the Ingestion Tokens section with **Resolve token creation**;
restoring a guard does not change the current section or open a dialog. Only a
plaintext token currently visible in memory prevents navigation, because
leaving would permanently discard that one-time secret.

One tab owns recovery through an exact API-base Web Lock. That tab polls a
complete, stable, exact-total, name-filtered token snapshot immediately and
with 1, 2, 4, 8, then at most 10 second delays. Polling pauses while its
document is hidden or offline and resumes immediately. Other tabs report lock
contention directly and can use **Try again** after the owner closes. When the
owner safely resolves and removes the exact guard, matching tabs unlock
without a reload.

For an ambiguous request with no matching token, the browser waits until the
server-clock deadline calculated as two request timeouts plus clock
uncertainty—about 60 seconds with the current 30-second request timeout. It
then requires two complete zero-result snapshots at least two seconds apart
before concluding that no token was created. A matching token cancels that
completion; an attributable live token whose plaintext was lost must be
revoked. This bounded policy has a small residual risk if a reverse proxy
delivers the original create request more than 60 seconds late.

An exact Open Splunk `408` response with `administrative request was canceled`
or `429` with `ingestion token capacity is exhausted` is a definite no-create
outcome. A proxy-generated 408/429, browser timeout, connection failure,
malformed response, or incomplete/unstable listing remains ambiguous. If a
check requires authentication, use the recovery dialog's sign-in route; the
durable guard remains in place while the recovery lock is released.

If the saved guard is unreadable, the owning tab records its first observation
against the authoritative server clock and performs complete unfiltered token
snapshots. With no trustworthy attribution data, every nonterminal token is
treated as potentially related and must become revoked or expired. The same
quiescence period and two zero-nonterminal snapshots are required before the
damaged record can be removed. Never delete or edit a token recovery guard in
browser developer tools: an unmatched removal remains fail-closed and can
leave the browser unable to prove the create outcome safely.

## Host and source constraints

A token may carry independent `allowed_host_regexes` and
`allowed_source_regexes` lists. Empty means unrestricted. A nonempty dimension
has at most 16 unique RE2 patterns, each 1 through 512 UTF-8 bytes, with at most
4,096 bytes of source text per dimension. Each compiled pattern is limited to
4,096 program instructions and a dimension to 16,384.

Patterns are exact configuration data: no trimming, case folding, or Unicode
normalization occurs. They are deduplicated and stored in bytewise lexical
order. Each is anchored to the complete canonical value, conceptually
`\A(?:pattern)\z`. Alternatives within one dimension are ORed; host and source
dimensions are ANDed.

Index/event validation precedes host and source matching. Host wins a dual
failure. Constraint failures are permanent per-event rejections, reveal no
value or pattern, do not consume quota, and never reach ClickHouse. Other valid
events in the same batch may still commit.

## Rate limits

Every token and logical index may independently set
`max_events_per_second` and `max_uncompressed_bytes_per_second`. Zero or
absence means unlimited. Values may not exceed 1,000,000 events/second or 1 TiB
per second.

The administration console states the byte rate, and the index policy's
`max_event_bytes`, as a byte size in the same notation the collector's
configuration file uses: a bare byte count, `B`, decimal `KB`/`MB`/`GB`/`TB`/`PB`,
or binary `KiB`/`MiB`/`GiB`/`TiB`/`PiB` (see **YAML parsing and scalar syntax**
in docs/collector-configuration.md). Because `MB` and `MiB` are different
numbers, each field prints the exact byte count it read underneath itself
whenever the text is not already that count's shortest spelling. The wire format
is unchanged: every one of these fields is a byte count on the API.

Only fresh events that pass index authorization, event validation, and
host/source constraints are charged. Byte charge is the server-computed
protobuf encoding size before normalization and any explicitly configured
redaction; client totals are never accounting authority. Token charge includes
all admitted events, while each index receives its subset. A mixed-index batch
commits all applicable schedules or none.

Each enabled dimension stores a durable next-admission time. Admission advances
it by `ceil(charge / rate seconds)`. This virtual schedule permits one complete
batch burst and carries debt forward. The greatest blocking delay is returned,
with token winning an exact tie then lexical index name. Public delay is capped
at one hour. Changing one rate resets that dimension on the first fresh
boundary that observes the policy; unchanged dimensions retain debt.

A denial sends `RetryBatch(RATE_LIMITED)` for the current WAL batch and a
separate `Throttle(TOKEN_QUOTA|INDEX_QUOTA)` for later sends. Neither
acknowledges the batch. The collector retains retry timing across reconnect and
anchors server-provided intervals to local receipt time rather than comparing
unsynchronized wall clocks.

Exact durable batch lookup precedes mutable policy and quota. Terminal results
replay exactly; pending ClickHouse work resumes; a fresh batch charges in the
same serializable SQLite transaction that establishes batch identity,
visibility, and outbox work. Concurrent duplicates, ambiguous inserts,
restart, and stream takeover therefore cannot double-charge inside the retained
replay horizon.

## Container deployment

Successful `v0.MINOR.PATCH` publications produce a public,
multi-architecture collector image for Linux AMD64 and ARM64 at
[`ghcr.io/suhaibinator/open-splunk-collector`](https://github.com/Suhaibinator/open-splunk/pkgs/container/open-splunk-collector).
Use the numeric release version without the leading `v`, and use the same
version for the server and collector:

```sh
export COLLECTOR_IMAGE=ghcr.io/suhaibinator/open-splunk-collector:0.MINOR.PATCH
docker pull "$COLLECTOR_IMAGE"
```

Replace `0.MINOR.PATCH` with the selected release. The published `latest` tag
is convenient for evaluation but is mutable; persistent deployments should use
an exact version. See [build and publication status](releasing.md) for the
release contract.

To test unreleased source instead, build both local images from the same clean,
committed revision. `make oci` gives each image that full revision as its
default tag:

```sh
revision="$(git rev-parse HEAD)"
OPEN_SPLUNK_SOURCE_REVISION="$revision" make oci
export COLLECTOR_IMAGE="open-splunk-collector:$revision"
```

Do not mix a published collector with a different server version or mix local
server and collector images from different revisions.

The image runs as UID/GID `65532:65532`. Use a dedicated owner-only state
directory below a trusted parent, a read-only config, a read-only CA, a
read-only token file, and read-only log mounts. The server gRPC listener uses
TLS by default. The default Compose service does not enable a collector
listener; remote collectors require an explicitly configured listener, private
bind address, and firewall rule. Never publish ClickHouse or copy the server
private key.

The example container config uses the following template variables. These
names are referenced by that YAML file; they are not an implicit environment
registry in the collector executable. See
[environment substitution](collector-configuration.md#environment-substitution)
for the complete behavior and field mapping.

```dotenv
OPEN_SPLUNK_COLLECTOR_SERVER_ADDRESS=splunk.example.internal:4317
OPEN_SPLUNK_COLLECTOR_SERVER_TLS_SERVER_NAME=open-splunk-server
OPEN_SPLUNK_COLLECTOR_TOKEN_FILE=/run/open-splunk/secrets/collector.token
OPEN_SPLUNK_COLLECTOR_SERVER_TLS_CA_CERTIFICATE_FILE=/run/open-splunk/tls/ca.crt
OPEN_SPLUNK_COLLECTOR_STATE_DIRECTORY=/var/lib/open-splunk-collector/state
OPEN_SPLUNK_COLLECTOR_INPUT_GLOB=/var/log/source/*.log
OPEN_SPLUNK_COLLECTOR_INPUT_INDEX=application
OPEN_SPLUNK_COLLECTOR_INPUT_SOURCE=application
OPEN_SPLUNK_COLLECTOR_INPUT_SOURCETYPE=json
OPEN_SPLUNK_COLLECTOR_INPUT_HOST=app-01.example.internal
OPEN_SPLUNK_COLLECTOR_INPUT_SERVICE=application
OPEN_SPLUNK_COLLECTOR_INPUT_ENVIRONMENT=production
```

Initialize stable identity against the final state mount before creating the
token:

```sh
docker run --rm \
  --user 65532:65532 --read-only --cap-drop ALL \
  --security-opt no-new-privileges:true \
  --env-file /etc/open-splunk-collector/collector.env \
  --mount type=bind,src=/etc/open-splunk-collector/collector.yaml,dst=/etc/open-splunk/collector.yaml,readonly \
  --mount type=bind,src=/var/lib/open-splunk-collector/state,dst=/var/lib/open-splunk-collector/state \
  "$COLLECTOR_IMAGE" \
  identity -config /etc/open-splunk/collector.yaml
```

Create/activate the index through `/api/indexes/create`, then create a native
token through `/api/ingestion-tokens/create` with that exact
collector ID and the configured index set. Install only the one-time token on
the collector host as UID 65532 mode `0600`; never provide an administrator
token or ClickHouse/server credentials.

Run `validate` with the same environment, user, configuration, state, and source
mounts to prove YAML parsing, local configuration constraints, and current glob
matches. It does not read the token or CA file, connect to the server, perform a
TLS handshake, or authenticate. Then run the image with the same complete state
mount, CA, token, config, and source mounts. A healthy connection logs
`collector stream ready`; also monitor `/api/collectors/get` or `/list`,
heartbeat age, restart count, state-disk utilization, and source-log retention
headroom.

Collector state belongs to the source revision that created it. Cross-revision
reuse is not a compatibility promise. Unrecognized state fails closed; do not
edit its identity, WAL, or checkpoints. Provision a fresh state directory and
retain the old directory for forensic recovery when development formats change.

## File and WAL behavior

Prefer rename/recreate log rotation, keep rotated files readable, and include
their names in globs until terminal checkpoints catch up. Copy-truncate is
detected, but bytes removed before reaching the WAL cannot be recovered after a
crash and therefore do not have a strict source-level at-least-once guarantee.

The collector state contract is:

```text
collector_id          stable security identity
.collector.lock       single-process lock
wal/                  durable unacknowledged batches
checkpoints/          per-file terminal positions
dead-letter.jsonl     permanently rejected events
dead-letter.jsonl.N   bounded rotated dead-letter backups
```

Treat the directory as one unit. Never regenerate/copy `collector_id`
separately, edit/delete WAL or checkpoints, or run multiple collectors against
one state directory. A full WAL stops new file reads until delivery frees
space; it does not discard source logs. Corrupt WAL tails and later segments
may be quarantined as `*.corrupt` so a missing range cannot be skipped. There
is no supported manual compaction, acknowledgment, or repair/import command.

Dead letters contain sensitive full events and remain owner-only. Alert on
growth, fix the token/index/schema/size cause, and use an explicitly reviewed
external replay process if resubmission is appropriate. Removing a dead letter
does not mean it was ingested.

## Restart, backup, and token rotation

SIGTERM stops reads, seals partial work, and gives the WAL a bounded drain
window. Use a stop timeout of at least 30 seconds. Unacknowledged batches replay
after restart.

Back up or move state only while the collector is stopped. Copy the complete
directory consistently and preserve UID/GID and owner-only modes. Collector
state and source-log retention must be coordinated for continuity after host
loss.

To rotate credentials, create a new token bound to the unchanged collector ID,
stop the process, atomically replace the token file, restart, prove a ready
stream and heartbeat, then revoke the old token. Never rotate identity merely
to rotate a credential.
