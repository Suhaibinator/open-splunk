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

## Choosing a source format

Set `inputs[].format` to match the producer: `ndjson`, `raw`,
`docker-json-file`, `nginx-combined`, `apache-common`, `apache-combined`,
`logfmt`, `log4j2-pattern`, or `logback-pattern`. The collector selects one
parser per input at startup; it does not guess formats or retry malformed
records as raw. See [Input formats](collector-configuration.md#input-formats)
for projections, producer references, and complete parser examples.

Docker records retain their JSON envelope as raw while exposing the original
message, nanosecond timestamp, and stream field. Access presets expose typed
status and byte counts alongside the request and client/header fields.
Malformed HTTP request text inside a valid access envelope remains available
for investigation. logfmt supports canonical-key mappings and exact numeric
conversion. Java inputs require an emitted-text delimiter pattern; configure
multiline framing separately for exceptions. Offset-free timestamps require an
explicit timezone, and ambiguous or nonexistent IANA local times are rejected.

Run `validate` before restarting with parser changes. It validates patterns,
field mappings, layouts, and timezone names without opening source logs or
contacting the server. Keep the input ID and state directory to preserve the
cursor, and set `sourcetype` explicitly when migrating from `raw` if existing
queries depend on its old value. Parser changes apply to newly read records;
pending WAL batches retain their existing parsed events. They do not trigger a
historical replay.

The decoder preserves original framed bytes in raw, and event IDs bind those
bytes and source position. With explicit redaction, native parsers track
sensitive source fields, active source-derived rename aliases, and embedded
assignments into parsed fields. They conservatively replace the entire raw and
message when lexical scrubbing cannot safely remove those values; configured
field replacements and trusted static metadata remain intact. See the
[processor reference](collector-configuration.md#processors-reference) for
replacement precedence and canonical-field behavior. Event IDs remain based on
the original input, while decode/framing recovery artifacts retain sensitive
original bytes outside this sanitizer.

Configure redaction before collecting sensitive data, and monitor decode
failures when enabling a strict parser. Custom access layouts, Java
conversion-pattern interpretation, syslog, CRI reassembly, and journald are not
supported by these formats.

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
token-create request. The guard contains the exact requested definition, a UUID
`client_request_id`, server-clock timing, and a browser ownership identity; it
never contains plaintext. After a timeout, connection loss, reload, or tab
closure, the browser resubmits the same definition and key to obtain the exact
server receipt. Names, creation times, and similar metadata do not identify the
outcome of a keyed request.

An unresolved guard pauses only **Generate token**. It does not block links,
browser navigation, authentication, or other administration work. A persistent
banner appears in the Ingestion Tokens section with **Resolve token creation**;
restoring a guard does not change the current section or open a dialog. Only a
plaintext token currently visible in memory prevents navigation, because
leaving would permanently discard that one-time secret.

One tab owns recovery through an exact API-base Web Lock. It waits for the
authoritative server clock before checking the seven-day retry fence, and keeps
the same lock and request identity during asynchronous retries. Checks pause
while the document is hidden or offline. Other tabs report lock contention and
can use **Try again** after the owner closes. Safely resolving and removing the
exact guard unlocks matching tabs without a reload.

Only the first successful issue returns plaintext. A receipt replay returns the
same token ID and current metadata, which may have changed or been revoked,
with no secret. When an active or disabled token is identified but its secret
was lost, the dialog requires explicit revocation before a replacement is
created under a new key. A confirmed revoked or expired token is safe to clear.
A changed definition under the same key conflicts instead of creating another
token. Authentication failures preserve the durable guard while the user signs
in again.

Legacy records without a request key, records past the seven-day fence, and
unreadable guards use a conservative fallback without resubmitting the old
create. The owning tab reviews complete, stable, exact-total, unfiltered token
snapshots. Every nonterminal token must become revoked or expired; matching a
name or timestamp never authorizes recovery. Before clearing the record, the
browser waits two request timeouts plus clock uncertainty from its first
server-clock observation, then requires two complete zero-nonterminal snapshots
at least two seconds apart. This fallback cannot identify a historical token
exactly and can require manual review of unrelated live tokens. The browser
retains ownership throughout that review. Editing or deleting the guard outside
the recovery flow cannot establish a safe create outcome.

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

Fresh terminal whole-batch rejections use a separate durable token budget:
at most 10 receipts per second and 256 KiB of encoded receipt metadata per
second, tightened by either nonzero token rate when lower. This budget permits
one complete receipt burst and retains debt across reconnect, restart, and
receipt pruning. It does not debit accepted-event or index quotas. A denial
returns `RetryBatch(RATE_LIMITED)` and `Throttle(TOKEN_QUOTA)` without storing a
terminal receipt; retry the unchanged batch. Existing durable outcomes replay
before this budget is checked, including `REPACK_REQUIRED` receipts.

Each tenant/source has a durable pending budget of 10,000 batches, 128 MiB of
outbox payload, and 128 MiB of response metadata, within shared ceilings of
20,000 batches and 256 MiB for each byte dimension. Native sources use the
authenticated bound collector ID, so replacing its credential does not reset
the budget. HEC uses the stable token record ID. Client-selected batch IDs,
channels, hosts, sources, and indexes do not create new budgets. These limits
apply even when token and index rate quotas are unlimited.

All accepted work remains charged while pending, including released leases,
write-group members, ambiguous sends, and work recovered after restart. Existing
replay proceeds at capacity; commit or safe abandonment frees capacity. An
upgrade preserves old reservations without assigning an inferred owner: their
unattributed usage counts against every new admission until it drains. A large
old backlog can therefore temporarily pause fresh ingestion after upgrade.
The budgets prevent one source from filling the shared queue; multiple
independently provisioned sources can still fill it. Existing ambiguous-send
barriers and ordered recovery continue to protect visibility consistency.

The logical collector batch remains the unit of identity, quota, response, and
acknowledgment, but it is not normally the physical ClickHouse insert. The
server durably coalesces ordered pending batches toward 10,000 rows or 16 MiB,
with a 200 ms maximum linger and hard limits of 50,000 rows, 64 MiB decoded
event data, and 10,000 member batches. Sparse traffic may therefore produce a
small insert when its durable linger deadline expires. A native request waits
for its own durable terminal result; cancellation or a lost response leaves
the staged batch available to exact retry and background recovery. The full
failure and resource contract is in [Insert coalescing](insert-coalescing.md).

Native streams pipeline up to 32 batch commit waits by default, additionally
bounded by 32 MiB of encoded pending batches per stream. Request authority and
sequence admission remain ordered; completion can be out of order and carries
only that batch's exact disposition. This lets one collector contribute
multiple logical batches to a coalesced insert without weakening commit
durability or reinterpreting previously admitted authority.

Peers may negotiate `LOSSLESS_REPACKING` in Hello/Ready. A `repack_batch` request
contains the **unchanged original** EventBatch, even if it exceeds the current
deployment's negotiated limits (immutable protocol hard bounds still apply).
Exact durable lookup runs first. Only when the original has no accepted side
effects may the server durably fence it with `REPACK_REQUIRED`. That rejection
authorizes replacing it with new child identities; a timeout or Ready resume
hint does not. The original's local checkpoint barrier remains until all child
outcomes have been handled. A committed original replays its acknowledgment
instead, preventing duplicates when an earlier acknowledgment was lost.

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
dead-letter.jsonl     permanently rejected durable events
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

Decode and framing failures occur before WAL append and are synchronously
fsynced to the sensitive dead-letter journal with bounded raw bytes and exact
source coordinates before a later valid record can advance the checkpoint. A
failed recovery write leaves the cursor unchanged; decode recovery stops the
run and framing recovery keeps the input on the same retryable range. See
[Decode, framing, and recovery](collector-configuration.md#decode-framing-and-recovery).

A trailing malformed record is durable in the recovery journal but does not
independently advance a terminal checkpoint. It can therefore be reread after
restart until a later acknowledged event covers its source position; the same
applies to an all-malformed file. Monitor the failure counters and repair the
producer/configuration rather than assuming repeated recovery artifacts mean
successful ingestion.

## Restart, backup, and token rotation

SIGTERM stops reads, seals partial work, and gives the WAL a bounded drain
window. Use a stop timeout of at least 30 seconds. Unacknowledged batches replay
after restart.

Server shutdown separately stops new admission, force-seals accepted
ungrouped batches, and drains write groups within its bounded shutdown context
before closing SQLite or ClickHouse. Any work that does not finish remains in
SQLite with its sealed membership and outbox, so restart resumes ambiguous
groups first, then ready groups, then ungrouped batches. An expired sparse
linger deadline is durable and fires immediately after restart rather than
starting a new delay.

Back up or move state only while the collector is stopped. Copy the complete
directory consistently and preserve UID/GID and owner-only modes. Collector
state and source-log retention must be coordinated for continuity after host
loss.

To rotate credentials, create a new token bound to the unchanged collector ID,
stop the process, atomically replace the token file, restart, prove a ready
stream and heartbeat, then revoke the old token. Never rotate identity merely
to rotate a credential.
