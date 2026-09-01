# Roadmap

This list contains only unfinished product work; completed phases, estimates,
checkpoints, and migration history are not retained here.

## Stable v1 contract

- Define protobuf/HTTP/gRPC, SPL, storage, cursor, backup, collector-state, and
  retained-job compatibility promises before declaring any client or data
  upgrade path supported.
- Specify migration, deprecation, support-window, rollback, and
  security-response policy and add executable conformance gates for every
  declared promise.
- Define signing and attestation policy for the existing release artifacts.
- Publish stable release notes and operator upgrade guidance only after those
  decisions are implemented and tested.

## Knowledge and reusable content

- Event types, tags, macros, and workflow actions with bounded compilation,
  lifecycle, provenance, and audit contracts.
- Additional extraction inputs and multivalue behavior where semantics can
  be specified without weakening resource limits.
- Cross-app import/export and role-aware sharing after the authorization model
  supports them.
- Safe version/body garbage collection that preserves retained search,
  recovery, and forensic authority.

## Data models and acceleration

- Typed data-model definitions, datasets, and dependency management.
- Bounded summary construction, invalidation, retention, and recovery.
- Acceleration planning that cannot bypass tenant/index/time/visibility or
  knowledge-snapshot authority.

## Broader SPL coverage

- Additional commands and eval functions selected by user value and available
  behavioral evidence.
- Subsearch/join-style composition only with explicit authorization, row,
  memory, fanout, and cancellation semantics.
- Continued differential testing for ambiguous percentile, estimated-distinct,
  mixed-value, ordering, chart, time, and wildcard behavior.
- More optimizer work that keeps exact predicates authoritative and preserves
  diagnostic/provenance boundaries.

## HEC interoperability

- Additional well-defined producer interoperability where it does not require a
  general `props.conf`/`transforms.conf` engine.
- Operational export/archival for retained request and ACK state.
- Larger-scale performance characterization and published sizing guidance;
  current gates are acceptance envelopes, not general SLAs.
- Distributed acknowledgment/reconciliation only after storage coordination is
  designed explicitly.

## Authorization and governance

- Multi-user RBAC, role grants, cross-app permissions, and current-policy
  disclosure checks throughout the browser and retained-product surfaces.
- Authentication activity, export activity, terminal search outcomes, and
  external audit archival. Search admission attempts already have a durable
  bounded journal.
- Secret rotation and workload identity integrations beyond local bearer-token
  administration.
- Add client-generated ingestion-token IDs as the exact create fence. The
  browser will persist a validated UUID in its recovery guard, supply that UUID
  on create and retry, fetch an existing ID for exact definition comparison,
  and revoke it when its one-time plaintext was lost. This replaces fuzzy
  metadata/timing recovery without requiring `client_request_id` to replay a
  one-time secret response; any future idempotency contract must define that
  outcome separately.

## Scale and availability

- Multi-node control-plane and ingestion coordination.
- Horizontally scalable search admission/execution with immutable snapshot and
  cursor semantics.
- Automated backup scheduling, retention, restore verification, and disaster-
  recovery exercises.
- Capacity planning, telemetry, and operators for catalogs, queues, journals,
  ClickHouse, and collector fleets.
