-- ACTIVE publication re-resolves the candidate against the current winner
-- cohort. Enabling a retained definition can therefore derive a different
-- ordered dependency set than its draft/disabled predecessor. Disable and
-- delete remain state-only mutations and must retain their predecessor's
-- exact dependency authority.

-- Migration 0030's source-driven index stops at target identity. The upgrade
-- preflight and forward version guard also compare the immutable target
-- version, so extend the same named contract before either bounded scan. The
-- migration transaction restores the prior index automatically if any later
-- preflight fails.
DROP INDEX knowledge_object_dependencies_source_target_idx;

CREATE INDEX knowledge_object_dependencies_source_target_idx
    ON knowledge_object_dependencies (
        tenant_id, source_object_id, source_object_version,
        target_kind, target_object_id, target_object_version
    );

-- Before installing the forward target-version guard, reject a catalog that
-- already has a current ACTIVE source whose current immutable edge does not
-- pin the exact current ACTIVE target. The migration runner's outer
-- BEGIN IMMEDIATE transaction makes this preflight and the trigger replacement
-- one atomic schema transition.
CREATE TABLE knowledge_active_publication_0033_upgrade_guard (
    invalid INTEGER NOT NULL CHECK (invalid = 0)
) STRICT;

INSERT INTO knowledge_active_publication_0033_upgrade_guard (invalid)
SELECT 1
WHERE EXISTS (
    SELECT 1
    FROM knowledge_objects AS dependent
        INDEXED BY knowledge_objects_resolution_idx
    CROSS JOIN knowledge_object_dependencies AS dependency
        INDEXED BY knowledge_object_dependencies_source_target_idx
    LEFT JOIN knowledge_objects AS target
      ON target.tenant_id = dependency.tenant_id
     AND target.knowledge_object_id = dependency.target_object_id
    WHERE dependent.state = 'active'
      AND dependency.tenant_id = dependent.tenant_id
      AND dependency.source_object_id = dependent.knowledge_object_id
      AND dependency.source_object_version = dependent.current_version
      AND dependency.target_kind = 'object'
      AND (
          target.knowledge_object_id IS NULL
          OR target.state <> 'active'
          OR target.current_version <> dependency.target_object_version
      )
    LIMIT 1
);

DROP TABLE knowledge_active_publication_0033_upgrade_guard;

DROP TRIGGER knowledge_object_version_writer_semantics_are_exact;

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

DROP TRIGGER knowledge_object_state_only_dependencies_are_exact;

CREATE TRIGGER knowledge_object_state_only_dependencies_are_exact
BEFORE INSERT ON knowledge_object_dependency_seals
WHEN NEW.object_version > 1
 AND EXISTS (
     SELECT 1
     FROM knowledge_object_versions AS version
     WHERE version.tenant_id = NEW.tenant_id
       AND version.knowledge_object_id = NEW.knowledge_object_id
       AND version.object_version = NEW.object_version
       AND version.mutation_kind IN ('disable', 'delete')
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

-- An ACTIVE target can advance only when no current ACTIVE dependent still
-- pins another immutable target version. A future atomic cascade can satisfy
-- this by publishing exact state-only disabled dependent versions, advancing
-- the target, and then enabling those dependents with newly derived edges.
-- Drive from the tenant's
-- schema-capped ACTIVE registry set (at most 4,096 rows) and probe each exact
-- current source prefix (at most 1,024 rows) through the named covering index.
-- Historical and non-ACTIVE sources never participate in this guard.
-- This is a transition backstop, not publication authority: the still-closed
-- Writer ACTIVE gates must validate every newly published source edge against
-- the complete post-publication cohort before updating its registry row.
CREATE TRIGGER knowledge_active_dependency_target_version_advance_is_blocked
BEFORE UPDATE OF current_version ON knowledge_objects
WHEN OLD.state = 'active'
 AND NEW.state = 'active'
 AND NEW.current_version <> OLD.current_version
 AND EXISTS (
    SELECT 1
    FROM knowledge_objects AS dependent
        INDEXED BY knowledge_objects_resolution_idx
    CROSS JOIN knowledge_object_dependencies AS dependency
        INDEXED BY knowledge_object_dependencies_source_target_idx
    WHERE dependent.tenant_id = OLD.tenant_id
      AND dependent.state = 'active'
      AND dependency.tenant_id = dependent.tenant_id
      AND dependency.source_object_id = dependent.knowledge_object_id
      AND dependency.source_object_version = dependent.current_version
      AND dependency.target_kind = 'object'
      AND dependency.target_object_id = OLD.knowledge_object_id
      AND dependency.target_object_version <> NEW.current_version
    LIMIT 1
 )
BEGIN
    SELECT RAISE(ABORT, 'active knowledge dependency pins a prior target version');
END;
