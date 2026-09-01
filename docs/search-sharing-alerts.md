# Search sharing, schedules, and alerts

Open Splunk keeps reusable search intent separate from retained execution
artifacts. This matches the three link types exposed by the Search workspace:

- A saved-search link opens the latest persisted definition and has no TTL.
- A query link carries SPL and time-range intent and creates a new job when run.
- A job link opens one exact retained result. It never silently reruns a search.

Manual jobs have a ten-minute sliding lifetime. Only a completed, unexpired job
with its retained result artifact can be shared. Pending, failed, canceled,
interrupted, missing, and expired jobs are not shareable. Sharing changes the
job's visibility to Everyone and its sliding lifetime to seven days. Reads of
the job or its results refresh the deadline; listings and maintenance do not.
An expired link returns an explicit expired state and can be rerun only by an
operator action. Search history remains separate metadata with its existing
30-day default retention and never extends a result artifact.

## Scheduled searches

A saved search may have a strict five-field cron schedule and an IANA
timezone. Each claimed occurrence snapshots the definition, schedule period,
and resolved `dispatch.ttl`. Explicit integer TTLs are seconds. `Np` means N
times the interval from the claimed occurrence to its next cron occurrence in
the schedule timezone, so daylight-saving transitions are included. The
default is `2p`.

Claims and next-run advancement are persisted together. Only one execution for
a saved search may be active. Scheduled reports use the default Splunk policy
of skipping missed or overlapping periods instead of backfilling them.
Running an unscheduled saved search creates a one-off report occurrence; its
default `2p` lifetime resolves from the scheduler's five-minute one-off period.

Schedule forms use `POST /api/schedules/validate` before mutation. The server
parses cron and timezone with the execution scheduler, resolves `Np` against
the next claimed period, and returns stable field-coded violations. This keeps
weekday bounds, daylight-saving behavior, and the ten-year retention ceiling
identical for browser validation, scheduled reports, and webhook alerts.

Retained-result projections are state-first. Pending jobs carry a result status
but may omit expiry until an artifact is published; available and expired jobs
carry a valid deadline. Missing or corrupt jobs include a deadline only when it
is known. A newer failed or skipped occurrence remains the latest run outcome
without hiding an older result-bearing run.

## Webhook alerts

Alerts are separate persisted objects with their own version, schedule,
condition, encrypted destination, signing-secret generation, enabled state,
and run history. A new alert starts disabled and returns a generated 32-byte
HMAC key exactly once, encoded as unpadded base64url. Decode that value before
using it as the HMAC key; the displayed string bytes are not the key. A
configured public base URL is required before the alert can be enabled.

Alert creation accepts a client request ID. Retrying the same byte-equivalent
definition with that ID returns the existing alert without revealing the
one-time secret again; reusing the ID for a different definition is rejected.
If the initial secret response is lost, the operator must rotate the secret.

For each due occurrence the scheduler persists a claim and snapshots the alert
version, search definition, schedule period, and signing-secret generation.
One run per alert may be active. Overlaps are recorded as skipped. Missed
occurrences after downtime are coalesced into one immediate run with a missed
count. Updating or disabling an alert affects future claims; deleting an alert
with an active run is rejected.

Successful exact results compare the row count using `>`, `<`, `=`, or `!=`.
A truncated result is only a lower bound. It may prove `>` and may prove `!=`
when the lower bound is already above the threshold; comparisons that cannot be
proved are indeterminate and do not deliver. Failed, canceled, expired, or
interrupted searches never deliver.

A triggered alert extends its job lifetime to the longer of `dispatch.ttl` and
the webhook TTL, whose default is `10p`, before delivery. Delivery is one
best-effort HTTPS POST with a ten-second timeout, no proxy, no redirects, no
retry, and a bounded response read. Any 2xx response succeeds.

### Webhook receiver contract

The request body is JSON schema version 1. `event_type` is
`alert.triggered` for a scheduled delivery or `alert.test` for a test. The body
also includes alert, alert-run, and search-job IDs; alert name and application;
scheduled, started, finished, and delivery times; missed-occurrence count;
operator, threshold, exactness-aware result count, result schema, sample rows,
search/sample truncation flags, and the retained-results URL. Sample output is
five rows by default, at most ten, and may be zero. The complete body is bounded
to 64 KiB by dropping trailing sample rows and setting `sample_truncated`.

Verify the request against the exact received body bytes:

1. Read `Open-Splunk-Alert-Timestamp` as Unix seconds and reject stale values
   according to the receiver's replay policy.
2. Decode the issued secret with unpadded base64url.
3. Compute HMAC-SHA256 over the ASCII timestamp, one period, and the exact body
   bytes: `timestamp + "." + body`.
4. Hex-encode the digest and compare it in constant time with
   `Open-Splunk-Alert-Signature`, whose value is `v1=<lowercase-hex>`.

`Open-Splunk-Alert-Id` and `Open-Splunk-Alert-Delivery-Id` identify the alert
and unique delivery. Do not parse and reserialize the body before signature
verification.

Destinations reject credentials, fragments, plaintext HTTP, redirects, DNS
rebinding, and unapproved private, loopback, or link-local addresses. Public
HTTPS is allowed after resolution and the validated address is dialed while TLS
continues to verify the original hostname. Exact private destinations require
an administrator allowlist. Secret rotation creates a new generation and
reveals the new secret once; a run holding an older generation keeps its job
and history but skips delivery.

## Component boundaries

Retention parsing and condition evaluation are pure packages. Persistence,
scheduling, search admission, and webhook delivery expose narrow interfaces and
receive clocks, resolvers, dialers, HTTP clients, and secret generators as
dependencies. HTTP and protobuf mapping stay at the transport boundary. The UI
uses typed API adapters and feature components rather than duplicating wire
interpretation inside screens.

Durable jobs, scheduled reports, and alerts emit bounded operational counters,
identity-free log records, and a tenant-scoped immutable SQLite audit journal.
The journal assigns a monotonic sequence and canonical occurrence time,
persists across restart and coordinated control-plane recovery, rejects
external updates and deletes, and atomically rolls the oldest event after
retaining 100,000 without changing the feature operation that was already
committed. Audit persistence failures emit only a
fixed safe category. The closed taxonomy includes admission, sharing,
retention, claims, lifecycle, evaluation, delivery, recovery, capacity
rejection, and cleanup; it cannot carry SPL, object IDs, endpoints, hostnames,
or secrets.
