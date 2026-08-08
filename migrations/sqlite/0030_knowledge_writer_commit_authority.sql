-- Exact writer-side commit authorities for ordinary and recovery knowledge
-- mutations.  Catalog CRUD was not registered before this migration, so an
-- older idempotency row has no response-format, actor-kind, state-token, or
-- audit linkage that a new writer can safely replay.  Refuse that ambiguous
-- upgrade instead of guessing, then install the first replayable format.

-- Migration 0029 made lifecycle transitions exact, but older schemas still
-- admitted version rows whose mutation label disagreed with their immutable
-- metadata transition.  Refuse such retained history before installing the
-- equivalent forward trigger.  The guard table makes the complete migration
-- roll back through the migration runner's outer BEGIN IMMEDIATE transaction.
CREATE TABLE knowledge_writer_0030_version_upgrade_guard (
    invalid INTEGER NOT NULL CHECK (invalid = 0)
) STRICT;

INSERT INTO knowledge_writer_0030_version_upgrade_guard (invalid)
SELECT 1
WHERE EXISTS (
    SELECT 1
    FROM knowledge_object_versions AS version
    WHERE version.object_version > 1
      AND NOT EXISTS (
          SELECT 1
          FROM knowledge_object_versions AS previous
          WHERE previous.tenant_id = version.tenant_id
            AND previous.knowledge_object_id = version.knowledge_object_id
            AND previous.object_version = version.object_version - 1
            AND version.created_at_unix_micro >= previous.created_at_unix_micro
            AND version.owner_id = previous.owner_id
            AND version.object_type = previous.object_type
            AND (
                (
                    version.mutation_kind = 'update'
                    AND version.state = previous.state
                    AND version.state IN ('draft', 'active', 'disabled')
                    AND version.app_id = previous.app_id
                    AND version.sharing_scope = previous.sharing_scope
                    AND version.definition_digest_key <> previous.definition_digest_key
                )
                OR (
                    version.mutation_kind = 'scope_change'
                    AND version.state = previous.state
                    AND version.state IN ('draft', 'active', 'disabled')
                    AND (
                        version.app_id <> previous.app_id
                        OR version.sharing_scope <> previous.sharing_scope
                    )
                    AND version.definition_digest_key <> previous.definition_digest_key
                )
                OR (
                    version.mutation_kind = 'enable'
                    AND version.state = 'active'
                    AND previous.state IN ('draft', 'disabled')
                    AND version.app_id = previous.app_id
                    AND version.name = previous.name
                    AND version.sharing_scope = previous.sharing_scope
                    AND version.definition_digest_key = previous.definition_digest_key
                    AND version.dependency_count = previous.dependency_count
                )
                OR (
                    version.mutation_kind = 'disable'
                    AND version.state = 'disabled'
                    AND previous.state IN ('draft', 'active')
                    AND version.app_id = previous.app_id
                    AND version.name = previous.name
                    AND version.sharing_scope = previous.sharing_scope
                    AND version.definition_digest_key = previous.definition_digest_key
                    AND version.dependency_count = previous.dependency_count
                )
                OR (
                    version.mutation_kind = 'delete'
                    AND version.state = 'deleted'
                    AND previous.state IN ('draft', 'active', 'disabled')
                    AND version.app_id = previous.app_id
                    AND version.name = previous.name
                    AND version.sharing_scope = previous.sharing_scope
                    AND version.definition_digest_key = previous.definition_digest_key
                    AND version.dependency_count = previous.dependency_count
                )
                OR (
                    version.mutation_kind = 'quarantine'
                    AND version.state = 'quarantined'
                    AND previous.state IN ('draft', 'active', 'disabled')
                    AND version.app_id = previous.app_id
                    AND version.name = previous.name
                    AND version.sharing_scope = previous.sharing_scope
                    AND version.definition_digest IS NULL
                )
            )
      )
);

-- State-only mutations preserve the exact dependency graph authority, not
-- merely its cardinality.  Earlier schemas admitted a same-count edge swap,
-- which would make an immutable enable, disable, or delete version disagree
-- with the definition that it is required to carry unchanged.
INSERT INTO knowledge_writer_0030_version_upgrade_guard (invalid)
SELECT 1
WHERE EXISTS (
    SELECT 1
    FROM knowledge_object_versions AS version
    JOIN knowledge_object_versions AS previous
      ON previous.tenant_id = version.tenant_id
     AND previous.knowledge_object_id = version.knowledge_object_id
     AND previous.object_version = version.object_version - 1
    WHERE version.object_version > 1
      AND version.mutation_kind IN ('enable', 'disable', 'delete')
      AND (
          version.dependency_count <> previous.dependency_count
          OR EXISTS (
              SELECT 1
              FROM knowledge_object_dependencies AS dependency
              WHERE dependency.tenant_id = version.tenant_id
                AND dependency.source_object_id = version.knowledge_object_id
                AND dependency.source_object_version = version.object_version
                AND NOT EXISTS (
                    SELECT 1
                    FROM knowledge_object_dependencies AS prior_dependency
                    WHERE prior_dependency.tenant_id = previous.tenant_id
                      AND prior_dependency.source_object_id = previous.knowledge_object_id
                      AND prior_dependency.source_object_version = previous.object_version
                      AND prior_dependency.ordinal = dependency.ordinal
                      AND prior_dependency.target_kind = dependency.target_kind
                      AND prior_dependency.target_object_id = dependency.target_object_id
                      AND prior_dependency.target_object_version = dependency.target_object_version
                      AND prior_dependency.dependency_role = dependency.dependency_role
                )
          )
          OR EXISTS (
              SELECT 1
              FROM knowledge_object_dependencies AS prior_dependency
              WHERE prior_dependency.tenant_id = previous.tenant_id
                AND prior_dependency.source_object_id = previous.knowledge_object_id
                AND prior_dependency.source_object_version = previous.object_version
                AND NOT EXISTS (
                    SELECT 1
                    FROM knowledge_object_dependencies AS dependency
                    WHERE dependency.tenant_id = version.tenant_id
                      AND dependency.source_object_id = version.knowledge_object_id
                      AND dependency.source_object_version = version.object_version
                      AND dependency.ordinal = prior_dependency.ordinal
                      AND dependency.target_kind = prior_dependency.target_kind
                      AND dependency.target_object_id = prior_dependency.target_object_id
                      AND dependency.target_object_version = prior_dependency.target_object_version
                      AND dependency.dependency_role = prior_dependency.dependency_role
                )
          )
      )
);

DROP TABLE knowledge_writer_0030_version_upgrade_guard;

CREATE TRIGGER knowledge_object_version_writer_semantics_are_exact
BEFORE INSERT ON knowledge_object_versions
WHEN NEW.object_version > 1
 AND EXISTS (
     SELECT 1
     FROM knowledge_object_versions AS previous
     WHERE previous.tenant_id = NEW.tenant_id
       AND previous.knowledge_object_id = NEW.knowledge_object_id
       AND previous.object_version = NEW.object_version - 1
       AND NEW.created_at_unix_micro >= previous.created_at_unix_micro
       AND (
           (
               NEW.mutation_kind IN ('update', 'scope_change')
               AND NEW.state = previous.state
               AND NEW.state IN ('draft', 'active', 'disabled')
           )
           OR (
               NEW.mutation_kind = 'enable'
               AND NEW.state = 'active'
               AND previous.state IN ('draft', 'disabled')
           )
           OR (
               NEW.mutation_kind = 'disable'
               AND NEW.state = 'disabled'
               AND previous.state IN ('draft', 'active')
           )
           OR (
               NEW.mutation_kind = 'quarantine'
               AND NEW.state = 'quarantined'
               AND previous.state IN ('draft', 'active', 'disabled')
           )
           OR (
               NEW.mutation_kind = 'delete'
               AND NEW.state = 'deleted'
               AND previous.state IN ('draft', 'active', 'disabled')
           )
       )
 )
 AND NOT EXISTS (
     SELECT 1
     FROM knowledge_object_versions AS previous
     WHERE previous.tenant_id = NEW.tenant_id
       AND previous.knowledge_object_id = NEW.knowledge_object_id
       AND previous.object_version = NEW.object_version - 1
       AND NEW.created_at_unix_micro >= previous.created_at_unix_micro
       AND NEW.owner_id = previous.owner_id
       AND NEW.object_type = previous.object_type
       AND (
           (
               NEW.mutation_kind = 'update'
               AND NEW.state = previous.state
               AND NEW.state IN ('draft', 'active', 'disabled')
               AND NEW.app_id = previous.app_id
               AND NEW.sharing_scope = previous.sharing_scope
               AND NEW.definition_digest_key <> previous.definition_digest_key
           )
           OR (
               NEW.mutation_kind = 'scope_change'
               AND NEW.state = previous.state
               AND NEW.state IN ('draft', 'active', 'disabled')
               AND (
                   NEW.app_id <> previous.app_id
                   OR NEW.sharing_scope <> previous.sharing_scope
               )
               AND NEW.definition_digest_key <> previous.definition_digest_key
           )
           OR (
               NEW.mutation_kind = 'enable'
               AND NEW.state = 'active'
               AND previous.state IN ('draft', 'disabled')
               AND NEW.app_id = previous.app_id
               AND NEW.name = previous.name
               AND NEW.sharing_scope = previous.sharing_scope
               AND NEW.definition_digest_key = previous.definition_digest_key
               AND NEW.dependency_count = previous.dependency_count
           )
           OR (
               NEW.mutation_kind = 'disable'
               AND NEW.state = 'disabled'
               AND previous.state IN ('draft', 'active')
               AND NEW.app_id = previous.app_id
               AND NEW.name = previous.name
               AND NEW.sharing_scope = previous.sharing_scope
               AND NEW.definition_digest_key = previous.definition_digest_key
               AND NEW.dependency_count = previous.dependency_count
           )
           OR (
               NEW.mutation_kind = 'delete'
               AND NEW.state = 'deleted'
               AND previous.state IN ('draft', 'active', 'disabled')
               AND NEW.app_id = previous.app_id
               AND NEW.name = previous.name
               AND NEW.sharing_scope = previous.sharing_scope
               AND NEW.definition_digest_key = previous.definition_digest_key
               AND NEW.dependency_count = previous.dependency_count
           )
           OR (
               NEW.mutation_kind = 'quarantine'
               AND NEW.state = 'quarantined'
               AND previous.state IN ('draft', 'active', 'disabled')
               AND NEW.app_id = previous.app_id
               AND NEW.name = previous.name
               AND NEW.sharing_scope = previous.sharing_scope
               AND NEW.definition_digest IS NULL
           )
       )
 )
BEGIN
    SELECT RAISE(ABORT, 'knowledge object version writer semantics are invalid');
END;

-- Dependency rows are staged after the immutable version and before its seal.
-- Enforce state-only graph identity at the seal boundary, when both complete
-- ordered sets are available but the new version is not yet publishable.
CREATE TRIGGER knowledge_object_state_only_dependencies_are_exact
BEFORE INSERT ON knowledge_object_dependency_seals
WHEN NEW.object_version > 1
 AND EXISTS (
     SELECT 1
     FROM knowledge_object_versions AS version
     WHERE version.tenant_id = NEW.tenant_id
       AND version.knowledge_object_id = NEW.knowledge_object_id
       AND version.object_version = NEW.object_version
       AND version.mutation_kind IN ('enable', 'disable', 'delete')
 )
 AND EXISTS (
     SELECT 1
     FROM knowledge_object_versions AS version
     JOIN knowledge_object_versions AS previous
       ON previous.tenant_id = version.tenant_id
      AND previous.knowledge_object_id = version.knowledge_object_id
      AND previous.object_version = version.object_version - 1
     WHERE version.tenant_id = NEW.tenant_id
       AND version.knowledge_object_id = NEW.knowledge_object_id
       AND version.object_version = NEW.object_version
       AND (
           version.dependency_count <> previous.dependency_count
           OR EXISTS (
               SELECT 1
               FROM knowledge_object_dependencies AS dependency
               WHERE dependency.tenant_id = version.tenant_id
                 AND dependency.source_object_id = version.knowledge_object_id
                 AND dependency.source_object_version = version.object_version
                 AND NOT EXISTS (
                     SELECT 1
                     FROM knowledge_object_dependencies AS prior_dependency
                     WHERE prior_dependency.tenant_id = previous.tenant_id
                       AND prior_dependency.source_object_id = previous.knowledge_object_id
                       AND prior_dependency.source_object_version = previous.object_version
                       AND prior_dependency.ordinal = dependency.ordinal
                       AND prior_dependency.target_kind = dependency.target_kind
                       AND prior_dependency.target_object_id = dependency.target_object_id
                       AND prior_dependency.target_object_version = dependency.target_object_version
                       AND prior_dependency.dependency_role = dependency.dependency_role
                 )
           )
           OR EXISTS (
               SELECT 1
               FROM knowledge_object_dependencies AS prior_dependency
               WHERE prior_dependency.tenant_id = previous.tenant_id
                 AND prior_dependency.source_object_id = previous.knowledge_object_id
                 AND prior_dependency.source_object_version = previous.object_version
                 AND NOT EXISTS (
                     SELECT 1
                     FROM knowledge_object_dependencies AS dependency
                     WHERE dependency.tenant_id = version.tenant_id
                       AND dependency.source_object_id = version.knowledge_object_id
                       AND dependency.source_object_version = version.object_version
                       AND dependency.ordinal = prior_dependency.ordinal
                       AND dependency.target_kind = prior_dependency.target_kind
                       AND dependency.target_object_id = prior_dependency.target_object_id
                       AND dependency.target_object_version = prior_dependency.target_object_version
                       AND dependency.dependency_role = prior_dependency.dependency_role
                 )
           )
       )
 )
BEGIN
    SELECT RAISE(ABORT, 'knowledge object state-only dependencies are invalid');
END;

-- Migration 0024's terminal-state guard drove from the retained-history
-- inverse target index. A popular long-lived target could therefore make one
-- disable/delete proportional to all historical references. Drive instead
-- from the schema-capped current ACTIVE registry set (at most 4,096 rows), and
-- probe only each source's exact current-version dependency prefix (at most
-- 1,024 rows, then narrow by target identity). The named covering index makes
-- that access path a schema contract instead of a planner-cost preference;
-- CROSS JOIN likewise makes the outer current-registry order structural.
CREATE INDEX knowledge_object_dependencies_source_target_idx
    ON knowledge_object_dependencies (
        tenant_id, source_object_id, source_object_version,
        target_kind, target_object_id
    );

DROP TRIGGER knowledge_active_dependency_target_transition_is_blocked;

CREATE TRIGGER knowledge_active_dependency_target_transition_is_blocked
BEFORE UPDATE OF state ON knowledge_objects
WHEN OLD.state = 'active'
 AND NEW.state IN ('disabled', 'quarantined', 'deleted')
 AND EXISTS (
    SELECT 1
    FROM knowledge_objects AS dependent INDEXED BY knowledge_objects_resolution_idx
    CROSS JOIN knowledge_object_dependencies AS dependency
        INDEXED BY knowledge_object_dependencies_source_target_idx
    WHERE dependent.tenant_id = OLD.tenant_id
      AND dependent.state = 'active'
      AND dependency.tenant_id = dependent.tenant_id
      AND dependency.source_object_id = dependent.knowledge_object_id
      AND dependency.source_object_version = dependent.current_version
      AND dependency.target_kind = 'object'
      AND dependency.target_object_id = OLD.knowledge_object_id
    LIMIT 1
)
BEGIN
    SELECT RAISE(ABORT, 'active knowledge dependency has active dependents');
END;

-- Retain one immutable authority for every committed catalog branch advance.
-- The current revision head rotates in place, so replay receipts cannot use it
-- to prove an older revision/token pair after subsequent mutations. This
-- compact bridge binds that pair to the immutable version and its exact audit
-- authority without duplicating definition bodies or public responses.
CREATE TABLE knowledge_mutation_commit_authorities (
    tenant_id TEXT NOT NULL COLLATE BINARY,
    actor_kind TEXT NOT NULL COLLATE BINARY CHECK (
        actor_kind IN ('browser', 'system')
    ),
    actor_id TEXT NOT NULL COLLATE BINARY,
    route TEXT NOT NULL COLLATE BINARY,
    client_request_id TEXT NOT NULL COLLATE BINARY,
    request_digest BLOB NOT NULL CHECK (length(request_digest) = 32),
    catalog_revision INTEGER NOT NULL CHECK (
        catalog_revision BETWEEN 1 AND 9223372036854775806
    ),
    catalog_state_token BLOB NOT NULL CHECK (
        length(catalog_state_token) = 32
    ),
    mutation_kind TEXT NOT NULL COLLATE BINARY CHECK (
        mutation_kind IN (
            'create', 'update', 'scope_change', 'enable', 'disable',
            'quarantine', 'delete'
        )
    ),
    knowledge_object_id TEXT NOT NULL COLLATE BINARY,
    object_version INTEGER NOT NULL CHECK (object_version >= 1),
    occurred_at_unix_micro INTEGER NOT NULL CHECK (
        occurred_at_unix_micro BETWEEN 1 AND 253402300799999999
    ),
    retention_anchor_unix_micro INTEGER NOT NULL CHECK (
        retention_anchor_unix_micro BETWEEN 1 AND 253402300799999999
    ),
    retain_until_unix_micro INTEGER NOT NULL CHECK (
        retain_until_unix_micro BETWEEN 1 AND 253402300799999999
    ),
    successful_audit_sequence INTEGER CHECK (
        successful_audit_sequence IS NULL
        OR successful_audit_sequence BETWEEN 1 AND 100000
    ),
    recovery_audit_sequence INTEGER CHECK (
        recovery_audit_sequence IS NULL
        OR recovery_audit_sequence BETWEEN 1 AND 8192
    ),
    PRIMARY KEY (tenant_id, catalog_revision),
    UNIQUE (tenant_id, catalog_state_token),
    UNIQUE (tenant_id, catalog_revision, catalog_state_token),
    UNIQUE (
        tenant_id, catalog_revision, catalog_state_token,
        actor_kind, actor_id, route, client_request_id, request_digest
    ),
    CONSTRAINT knowledge_mutation_commit_actor_id_bounded CHECK (
        length(CAST(actor_id AS BLOB)) BETWEEN 1 AND 255
        AND instr(CAST(actor_id AS BLOB), X'00') = 0
        AND actor_id = trim(actor_id)
        AND actor_id NOT GLOB (
            '*[' || char(1) || '-' || char(31)
            || char(127) || '-' || char(159) || ']*'
        )
    ),
    CONSTRAINT knowledge_mutation_commit_request_id_bounded CHECK (
        length(CAST(client_request_id AS BLOB)) BETWEEN 16 AND 128
        AND client_request_id NOT GLOB '*[^!-~]*'
    ),
    CONSTRAINT knowledge_mutation_commit_route_matches_kind CHECK (
        (route = 'objects.create' AND mutation_kind = 'create')
        OR (
            route = 'objects.update'
            AND mutation_kind IN ('update', 'scope_change')
        )
        OR (
            route = 'objects.set_state'
            AND mutation_kind IN ('enable', 'disable')
        )
        OR (route = 'objects.delete' AND mutation_kind = 'delete')
        OR (route = 'objects.quarantine' AND mutation_kind = 'quarantine')
    ),
    CONSTRAINT knowledge_mutation_commit_audit_shape_is_exact CHECK (
        (
            mutation_kind = 'quarantine'
            AND successful_audit_sequence IS NULL
            AND recovery_audit_sequence IS NOT NULL
        )
        OR (
            mutation_kind <> 'quarantine'
            AND successful_audit_sequence IS NOT NULL
            AND recovery_audit_sequence IS NULL
        )
    ),
    CONSTRAINT knowledge_mutation_commit_retention_is_bounded CHECK (
        retention_anchor_unix_micro >= occurred_at_unix_micro
        AND retain_until_unix_micro - retention_anchor_unix_micro
            BETWEEN 604800000000 AND 31536000000000
    ),
    FOREIGN KEY (tenant_id) REFERENCES knowledge_catalog_tenants (tenant_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
    FOREIGN KEY (tenant_id, knowledge_object_id, object_version)
        REFERENCES knowledge_object_versions (
            tenant_id, knowledge_object_id, object_version
        ) ON UPDATE RESTRICT ON DELETE RESTRICT,
    FOREIGN KEY (tenant_id, successful_audit_sequence)
        REFERENCES audit_events (tenant_id, sequence)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
    FOREIGN KEY (tenant_id, recovery_audit_sequence)
        REFERENCES knowledge_recovery_audit (tenant_id, sequence)
        ON UPDATE RESTRICT ON DELETE RESTRICT
) STRICT, WITHOUT ROWID;

-- Reject INSERT OR REPLACE before it can turn a collision into an immutable
-- authority deletion when recursive triggers are disabled.
CREATE TRIGGER knowledge_mutation_commit_authority_collision_is_forbidden
BEFORE INSERT ON knowledge_mutation_commit_authorities
WHEN EXISTS (
    SELECT 1
    FROM knowledge_mutation_commit_authorities
    WHERE tenant_id = NEW.tenant_id
      AND (
          catalog_revision = NEW.catalog_revision
          OR catalog_state_token = NEW.catalog_state_token
      )
)
BEGIN
    SELECT RAISE(ABORT, 'knowledge mutation commit authority already exists');
END;

-- A bridge can be created only for the exact current head, current immutable
-- version/lifecycle tuple, and matching success or recovery audit. The receipt
-- inserted later in the same transaction references this immutable row.
CREATE TRIGGER knowledge_mutation_commit_authority_is_exact
BEFORE INSERT ON knowledge_mutation_commit_authorities
WHEN NOT EXISTS (
    SELECT 1
    FROM knowledge_catalog_tenants AS tenant
    JOIN knowledge_catalog_revision_heads AS head
      ON head.tenant_id = tenant.tenant_id
     AND head.catalog_revision = tenant.catalog_revision
    JOIN knowledge_object_versions AS version
      ON version.tenant_id = tenant.tenant_id
     AND version.knowledge_object_id = NEW.knowledge_object_id
     AND version.object_version = NEW.object_version
    JOIN knowledge_object_version_lifecycle AS lifecycle
      ON lifecycle.tenant_id = version.tenant_id
     AND lifecycle.knowledge_object_id = version.knowledge_object_id
     AND lifecycle.object_version = version.object_version
     AND lifecycle.state = version.state
    JOIN knowledge_objects AS current
      ON current.tenant_id = version.tenant_id
     AND current.knowledge_object_id = version.knowledge_object_id
     AND current.current_version = version.object_version
     AND current.app_id = version.app_id
     AND current.owner_id = version.owner_id
     AND current.object_type = version.object_type
     AND current.name = version.name
     AND current.sharing_scope = version.sharing_scope
     AND current.state = version.state
     AND current.definition_digest_key = version.definition_digest_key
     AND current.updated_at_unix_micro = version.created_at_unix_micro
     AND current.disabled_at_unix_micro IS lifecycle.disabled_at_unix_micro
     AND current.quarantined_at_unix_micro IS lifecycle.quarantined_at_unix_micro
     AND current.deleted_at_unix_micro IS lifecycle.deleted_at_unix_micro
     AND current.quarantine_reason IS lifecycle.quarantine_reason
    WHERE tenant.tenant_id = NEW.tenant_id
      AND tenant.catalog_revision = NEW.catalog_revision
      AND head.state_token = NEW.catalog_state_token
      AND length(head.state_token) = 32
      AND version.mutation_kind = NEW.mutation_kind
      AND version.created_at_unix_micro = NEW.occurred_at_unix_micro
      AND (
          (
              NEW.mutation_kind <> 'quarantine'
              AND NEW.recovery_audit_sequence IS NULL
              AND EXISTS (
                  SELECT 1
                  FROM audit_events AS event
                  WHERE event.tenant_id = version.tenant_id
                    AND event.sequence = NEW.successful_audit_sequence
                    AND event.actor_kind = NEW.actor_kind
                    AND event.actor_id = NEW.actor_id
                    AND event.occurred_at_unix_micro = version.created_at_unix_micro
                    AND event.target_kind = 'knowledge_object'
                    AND event.target_id = version.knowledge_object_id
                    AND event.target_version = version.object_version
                    AND event.app_id = version.app_id
                    AND event.object_type = version.object_type
                    AND event.sharing_scope = version.sharing_scope
                    AND event.action = CASE NEW.mutation_kind
                        WHEN 'create' THEN 'knowledge.object.create'
                        WHEN 'update' THEN 'knowledge.object.update'
                        WHEN 'scope_change' THEN 'knowledge.object.scope_change'
                        WHEN 'enable' THEN 'knowledge.object.enable'
                        WHEN 'disable' THEN 'knowledge.object.disable'
                        WHEN 'delete' THEN 'knowledge.object.delete'
                    END
              )
          )
          OR (
              NEW.mutation_kind = 'quarantine'
              AND NEW.successful_audit_sequence IS NULL
              AND EXISTS (
                  SELECT 1
                  FROM knowledge_recovery_audit AS recovery
                  WHERE recovery.tenant_id = version.tenant_id
                    AND recovery.sequence = NEW.recovery_audit_sequence
                    AND recovery.actor_kind = NEW.actor_kind
                    AND recovery.actor_id = NEW.actor_id
                    AND recovery.occurred_at_unix_micro = version.created_at_unix_micro
                    AND recovery.knowledge_object_id = version.knowledge_object_id
                    AND recovery.object_version = version.object_version
                    AND recovery.app_id = version.app_id
                    AND recovery.object_type = version.object_type
                    AND recovery.sharing_scope = version.sharing_scope
                    AND recovery.recovery_reason = version.quarantine_reason
              )
          )
      )
)
BEGIN
    SELECT RAISE(ABORT, 'knowledge mutation commit authority is invalid');
END;

CREATE TRIGGER knowledge_mutation_commit_authority_update_is_forbidden
BEFORE UPDATE ON knowledge_mutation_commit_authorities
BEGIN
    SELECT RAISE(ABORT, 'knowledge mutation commit authority is immutable');
END;

CREATE TRIGGER knowledge_mutation_commit_authority_delete_is_forbidden
BEFORE DELETE ON knowledge_mutation_commit_authorities
BEGIN
    SELECT RAISE(ABORT, 'knowledge mutation commit authority is retained');
END;

-- No management writer shipped against the earlier replay table.  Any row, or
-- any nonzero tenant ledger left after manual tampering, is therefore
-- ambiguous.  Refuse the upgrade before dropping the old triggers or table.
CREATE TABLE knowledge_writer_0030_idempotency_upgrade_guard (
    invalid INTEGER NOT NULL CHECK (invalid = 0)
) STRICT;

INSERT INTO knowledge_writer_0030_idempotency_upgrade_guard (invalid)
SELECT 1
WHERE EXISTS (SELECT 1 FROM knowledge_mutation_idempotency LIMIT 1)
   OR EXISTS (
       SELECT 1
       FROM knowledge_catalog_tenants
       WHERE idempotency_count <> 0
   );

DROP TABLE knowledge_writer_0030_idempotency_upgrade_guard;

DROP TRIGGER knowledge_mutation_idempotency_capacity_is_available;
DROP TRIGGER knowledge_mutation_idempotency_identity_collision_is_forbidden;
DROP TRIGGER knowledge_mutation_idempotency_matches_version;
DROP TRIGGER knowledge_quarantine_idempotency_matches_current_registry;
DROP TRIGGER knowledge_mutation_idempotency_after_insert;
DROP TRIGGER knowledge_mutation_idempotency_update_is_forbidden;
DROP TRIGGER knowledge_mutation_idempotency_delete_before_retention_is_forbidden;
DROP TRIGGER knowledge_mutation_idempotency_after_delete;
DROP INDEX knowledge_mutation_idempotency_retention_idx;

ALTER TABLE knowledge_mutation_idempotency
    RENAME TO knowledge_mutation_idempotency_before_commit_authority;

CREATE TABLE knowledge_mutation_idempotency (
    tenant_id TEXT NOT NULL COLLATE BINARY,
    actor_kind TEXT NOT NULL COLLATE BINARY CHECK (
        actor_kind IN ('system', 'browser')
    ),
    actor_id TEXT NOT NULL COLLATE BINARY,
    route TEXT NOT NULL COLLATE BINARY,
    client_request_id TEXT NOT NULL COLLATE BINARY,
    mutation_kind TEXT NOT NULL COLLATE BINARY CHECK (
        mutation_kind IN (
            'create', 'update', 'scope_change', 'enable', 'disable',
            'quarantine', 'delete'
        )
    ),
    request_digest_format_version INTEGER NOT NULL CHECK (
        request_digest_format_version = 1
    ),
    request_digest BLOB NOT NULL CHECK (length(request_digest) = 32),
    outcome_format_version INTEGER NOT NULL CHECK (
        outcome_format_version = 1
    ),
    outcome_proto BLOB NOT NULL CHECK (
        length(outcome_proto) BETWEEN 1 AND 1024
    ),
    committed_catalog_revision INTEGER NOT NULL CHECK (
        committed_catalog_revision BETWEEN 1 AND 9223372036854775806
    ),
    committed_catalog_state_token BLOB NOT NULL CHECK (
        length(committed_catalog_state_token) = 32
    ),
    knowledge_object_id TEXT NOT NULL COLLATE BINARY,
    object_version INTEGER NOT NULL CHECK (object_version >= 1),
    successful_audit_sequence INTEGER CHECK (
        successful_audit_sequence IS NULL
        OR successful_audit_sequence BETWEEN 1 AND 100000
    ),
    recovery_audit_sequence INTEGER CHECK (
        recovery_audit_sequence IS NULL
        OR recovery_audit_sequence BETWEEN 1 AND 8192
    ),
    created_at_unix_micro INTEGER NOT NULL CHECK (
        created_at_unix_micro BETWEEN 1 AND 253402300799999999
    ),
    retention_anchor_unix_micro INTEGER NOT NULL CHECK (
        retention_anchor_unix_micro BETWEEN 1 AND 253402300799999999
    ),
    retain_until_unix_micro INTEGER NOT NULL CHECK (
        retain_until_unix_micro BETWEEN 1 AND 253402300799999999
    ),
    PRIMARY KEY (
        tenant_id, actor_kind, actor_id, route, client_request_id
    ),
    CONSTRAINT knowledge_mutation_idempotency_actor_id_bounded CHECK (
        length(CAST(actor_id AS BLOB)) BETWEEN 1 AND 255
        AND instr(CAST(actor_id AS BLOB), X'00') = 0
        AND actor_id = trim(actor_id)
        AND actor_id NOT GLOB (
            '*[' || char(1) || '-' || char(31)
            || char(127) || '-' || char(159) || ']*'
        )
    ),
    CONSTRAINT knowledge_mutation_idempotency_route_matches_kind CHECK (
        (route = 'objects.create' AND mutation_kind = 'create')
        OR (
            route = 'objects.update'
            AND mutation_kind IN ('update', 'scope_change')
        )
        OR (
            route = 'objects.set_state'
            AND mutation_kind IN ('enable', 'disable')
        )
        OR (route = 'objects.delete' AND mutation_kind = 'delete')
        OR (route = 'objects.quarantine' AND mutation_kind = 'quarantine')
    ),
    CONSTRAINT knowledge_mutation_idempotency_request_id_bounded CHECK (
        length(CAST(client_request_id AS BLOB)) BETWEEN 16 AND 128
        AND client_request_id NOT GLOB '*[^!-~]*'
    ),
    CONSTRAINT knowledge_mutation_idempotency_retention_is_bounded CHECK (
        retention_anchor_unix_micro >= created_at_unix_micro
        AND retain_until_unix_micro - retention_anchor_unix_micro
            BETWEEN 604800000000 AND 31536000000000
    ),
    CONSTRAINT knowledge_mutation_idempotency_audit_shape_is_exact CHECK (
        (
            mutation_kind = 'quarantine'
            AND successful_audit_sequence IS NULL
            AND recovery_audit_sequence IS NOT NULL
        )
        OR (
            mutation_kind <> 'quarantine'
            AND successful_audit_sequence IS NOT NULL
            AND recovery_audit_sequence IS NULL
        )
    ),
    FOREIGN KEY (tenant_id) REFERENCES knowledge_catalog_tenants (tenant_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
    FOREIGN KEY (tenant_id, knowledge_object_id, object_version)
        REFERENCES knowledge_object_versions (
            tenant_id, knowledge_object_id, object_version
        ) ON UPDATE RESTRICT ON DELETE RESTRICT,
    FOREIGN KEY (tenant_id, successful_audit_sequence)
        REFERENCES audit_events (tenant_id, sequence)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
    FOREIGN KEY (tenant_id, recovery_audit_sequence)
        REFERENCES knowledge_recovery_audit (tenant_id, sequence)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
    FOREIGN KEY (
        tenant_id, committed_catalog_revision, committed_catalog_state_token,
        actor_kind, actor_id, route, client_request_id, request_digest
    ) REFERENCES knowledge_mutation_commit_authorities (
        tenant_id, catalog_revision, catalog_state_token,
        actor_kind, actor_id, route, client_request_id, request_digest
    )
        ON UPDATE RESTRICT ON DELETE RESTRICT
) STRICT, WITHOUT ROWID;

DROP TABLE knowledge_mutation_idempotency_before_commit_authority;

CREATE INDEX knowledge_mutation_idempotency_retention_idx
    ON knowledge_mutation_idempotency (
        tenant_id, retain_until_unix_micro, created_at_unix_micro,
        actor_kind, actor_id, route, client_request_id,
        retention_anchor_unix_micro
    );

-- Idempotency has a 16,384-row normal ceiling and a 4,096-row protective
-- reserve available only to terminal quarantine mutations.
CREATE TRIGGER knowledge_mutation_idempotency_capacity_is_available
BEFORE INSERT ON knowledge_mutation_idempotency
WHEN NOT EXISTS (
    SELECT 1
    FROM knowledge_catalog_tenants
    WHERE tenant_id = NEW.tenant_id
      AND (
          (NEW.mutation_kind = 'quarantine' AND idempotency_count < 20480)
          OR (NEW.mutation_kind <> 'quarantine' AND idempotency_count < 16384)
      )
)
BEGIN
    SELECT RAISE(ABORT, 'knowledge mutation idempotency capacity exhausted');
END;

-- Reject OR REPLACE before it can bypass immutable DELETE behavior when
-- recursive_triggers is disabled.
CREATE TRIGGER knowledge_mutation_idempotency_identity_collision_is_forbidden
BEFORE INSERT ON knowledge_mutation_idempotency
WHEN EXISTS (
    SELECT 1
    FROM knowledge_mutation_idempotency
    WHERE tenant_id = NEW.tenant_id
      AND actor_kind = NEW.actor_kind
      AND actor_id = NEW.actor_id
      AND route = NEW.route
      AND client_request_id = NEW.client_request_id
)
BEGIN
    SELECT RAISE(ABORT, 'knowledge mutation idempotency identity already exists');
END;

-- The receipt is inserted after publication and the one revision increment.
-- Bind it to the exact current immutable version, lifecycle projection, numeric
-- revision, and restore-fork-safe state token observed in this transaction.
CREATE TRIGGER knowledge_mutation_idempotency_matches_commit_authority
BEFORE INSERT ON knowledge_mutation_idempotency
WHEN NOT EXISTS (
    SELECT 1
    FROM knowledge_mutation_commit_authorities AS committed
    JOIN knowledge_object_versions AS version
      ON version.tenant_id = committed.tenant_id
     AND version.knowledge_object_id = NEW.knowledge_object_id
     AND version.object_version = NEW.object_version
    JOIN knowledge_object_version_lifecycle AS lifecycle
      ON lifecycle.tenant_id = version.tenant_id
     AND lifecycle.knowledge_object_id = version.knowledge_object_id
     AND lifecycle.object_version = version.object_version
     AND lifecycle.state = version.state
    JOIN knowledge_objects AS current
      ON current.tenant_id = version.tenant_id
     AND current.knowledge_object_id = version.knowledge_object_id
     AND current.current_version = version.object_version
     AND current.app_id = version.app_id
     AND current.owner_id = version.owner_id
     AND current.object_type = version.object_type
     AND current.name = version.name
     AND current.sharing_scope = version.sharing_scope
     AND current.state = version.state
     AND current.definition_digest_key = version.definition_digest_key
     AND current.updated_at_unix_micro = version.created_at_unix_micro
     AND current.disabled_at_unix_micro IS lifecycle.disabled_at_unix_micro
     AND current.quarantined_at_unix_micro IS lifecycle.quarantined_at_unix_micro
     AND current.deleted_at_unix_micro IS lifecycle.deleted_at_unix_micro
     AND current.quarantine_reason IS lifecycle.quarantine_reason
    WHERE committed.tenant_id = NEW.tenant_id
      AND committed.catalog_revision = NEW.committed_catalog_revision
      AND committed.catalog_state_token = NEW.committed_catalog_state_token
      AND committed.actor_kind = NEW.actor_kind
      AND committed.actor_id = NEW.actor_id
      AND committed.route = NEW.route
      AND committed.client_request_id = NEW.client_request_id
      AND committed.request_digest = NEW.request_digest
      AND committed.mutation_kind = NEW.mutation_kind
      AND committed.knowledge_object_id = NEW.knowledge_object_id
      AND committed.object_version = NEW.object_version
      AND committed.occurred_at_unix_micro = NEW.created_at_unix_micro
      AND committed.retention_anchor_unix_micro = NEW.retention_anchor_unix_micro
      AND committed.retain_until_unix_micro = NEW.retain_until_unix_micro
      AND committed.successful_audit_sequence IS NEW.successful_audit_sequence
      AND committed.recovery_audit_sequence IS NEW.recovery_audit_sequence
      AND version.mutation_kind = NEW.mutation_kind
      AND version.created_at_unix_micro = NEW.created_at_unix_micro
)
BEGIN
    SELECT RAISE(ABORT, 'knowledge mutation idempotency commit authority is invalid');
END;

-- Ordinary mutations point at their exact immutable general-audit event.
-- Quarantine uses the separately reserved recovery journal so it remains
-- possible after ordinary audit capacity is exhausted.
CREATE TRIGGER knowledge_mutation_idempotency_matches_audit_authority
BEFORE INSERT ON knowledge_mutation_idempotency
WHEN (
    NEW.mutation_kind <> 'quarantine'
    AND NOT EXISTS (
        SELECT 1
        FROM knowledge_object_versions AS version
        JOIN audit_events AS event
          ON event.tenant_id = version.tenant_id
         AND event.sequence = NEW.successful_audit_sequence
         AND event.occurred_at_unix_micro = version.created_at_unix_micro
         AND event.actor_kind = NEW.actor_kind
         AND event.actor_id = NEW.actor_id
         AND event.target_kind = 'knowledge_object'
         AND event.target_id = version.knowledge_object_id
         AND event.target_version = version.object_version
         AND event.app_id = version.app_id
         AND event.object_type = version.object_type
         AND event.sharing_scope = version.sharing_scope
         AND event.action = CASE NEW.mutation_kind
             WHEN 'create' THEN 'knowledge.object.create'
             WHEN 'update' THEN 'knowledge.object.update'
             WHEN 'scope_change' THEN 'knowledge.object.scope_change'
             WHEN 'enable' THEN 'knowledge.object.enable'
             WHEN 'disable' THEN 'knowledge.object.disable'
             WHEN 'delete' THEN 'knowledge.object.delete'
         END
        WHERE version.tenant_id = NEW.tenant_id
          AND version.knowledge_object_id = NEW.knowledge_object_id
          AND version.object_version = NEW.object_version
          AND version.mutation_kind = NEW.mutation_kind
    )
)
 OR (
    NEW.mutation_kind = 'quarantine'
    AND NOT EXISTS (
        SELECT 1
        FROM knowledge_object_versions AS version
        JOIN knowledge_recovery_audit AS recovery
          ON recovery.tenant_id = version.tenant_id
         AND recovery.sequence = NEW.recovery_audit_sequence
         AND recovery.occurred_at_unix_micro = version.created_at_unix_micro
         AND recovery.actor_kind = NEW.actor_kind
         AND recovery.actor_id = NEW.actor_id
         AND recovery.knowledge_object_id = version.knowledge_object_id
         AND recovery.object_version = version.object_version
         AND recovery.app_id = version.app_id
         AND recovery.object_type = version.object_type
         AND recovery.sharing_scope = version.sharing_scope
         AND recovery.recovery_reason = version.quarantine_reason
        WHERE version.tenant_id = NEW.tenant_id
          AND version.knowledge_object_id = NEW.knowledge_object_id
          AND version.object_version = NEW.object_version
          AND version.mutation_kind = 'quarantine'
    )
)
BEGIN
    SELECT RAISE(ABORT, 'knowledge mutation idempotency audit authority is invalid');
END;

CREATE TRIGGER knowledge_mutation_idempotency_after_insert
AFTER INSERT ON knowledge_mutation_idempotency
BEGIN
    UPDATE knowledge_catalog_tenants
    SET idempotency_count = idempotency_count + 1
    WHERE tenant_id = NEW.tenant_id;
END;

CREATE TRIGGER knowledge_mutation_idempotency_update_is_forbidden
BEFORE UPDATE ON knowledge_mutation_idempotency
BEGIN
    SELECT RAISE(ABORT, 'knowledge mutation idempotency outcome is immutable');
END;

CREATE TRIGGER knowledge_mutation_idempotency_delete_before_retention_is_forbidden
BEFORE DELETE ON knowledge_mutation_idempotency
WHEN CAST(unixepoch('subsec') * 1000000 AS INTEGER) < OLD.retain_until_unix_micro
BEGIN
    SELECT RAISE(ABORT, 'knowledge mutation idempotency retention fence is active');
END;

CREATE TRIGGER knowledge_mutation_idempotency_after_delete
AFTER DELETE ON knowledge_mutation_idempotency
BEGIN
    UPDATE knowledge_catalog_tenants
    SET idempotency_count = idempotency_count - 1
    WHERE tenant_id = OLD.tenant_id;
END;
