-- Two tenants that deliberately reuse every tenant-scoped name: index "main",
-- app slug "search", saved search "errors" owned by "alice" in the legacy
-- namespace "search", token name "ingest". Only globally unique identities
-- (index_id, app_id, ingestion_token_id, saved_search_id, token_digest)
-- differ. Each tenant also holds an archived index "old" that index-deletion
-- scenarios start from.

INSERT INTO tenants (tenant_id, created_at_unix_micro, updated_at_unix_micro)
VALUES ('tenant-a', 1, 1), ('tenant-b', 1, 1);

INSERT INTO indexes (
    index_id, tenant_id, version, name, display_name, description,
    retention_nanoseconds, ingestion_enabled, search_enabled,
    default_sourcetype, state, created_at_unix_micro, updated_at_unix_micro
) VALUES
    ('idx_a_main', 'tenant-a', 1, 'main', 'Main', '', 0, 1, 1, '', 'active', 1000, 1000),
    ('idx_b_main', 'tenant-b', 1, 'main', 'Main', '', 0, 1, 1, '', 'active', 1000, 1000),
    ('idx_a_old', 'tenant-a', 1, 'old', 'Old', '', 0, 0, 0, '', 'archived', 1000, 1000),
    ('idx_b_old', 'tenant-b', 1, 'old', 'Old', '', 0, 0, 0, '', 'archived', 1000, 1000);

INSERT INTO app_workspaces (
    app_id, tenant_id, version, slug, display_name, default_time_range_present,
    state, created_at_unix_micro, updated_at_unix_micro, archived_at_unix_micro
) VALUES
    ('app_AAAAAAAAAAAAAAAAAAAAAA', 'tenant-a', 1, 'search', 'Search', 0, 'active', 1000, 1000, NULL),
    ('app_BBBBBBBBBBBBBBBBBBBBBA', 'tenant-b', 1, 'search', 'Search', 0, 'active', 1000, 1000, NULL);

INSERT INTO app_default_indexes (tenant_id, app_id, index_id) VALUES
    ('tenant-a', 'app_AAAAAAAAAAAAAAAAAAAAAA', 'idx_a_main'),
    ('tenant-b', 'app_BBBBBBBBBBBBBBBBBBBBBA', 'idx_b_main');

INSERT INTO ingestion_tokens (
    ingestion_token_id, tenant_id, version, name, token_prefix, token_digest,
    state, created_at_unix_micro, updated_at_unix_micro, revoked_at_unix_micro,
    bound_collector_id, purpose
) VALUES
    ('tok_a', 'tenant-a', 1, 'ingest', 'osk_aaaa0000',
        decode(repeat('aa', 32), 'hex'), 'active', 1000, 1000, NULL, NULL, 'hec'),
    ('tok_b', 'tenant-b', 1, 'ingest', 'osk_bbbb0000',
        decode(repeat('bb', 32), 'hex'), 'active', 1000, 1000, NULL, NULL, 'hec');

INSERT INTO ingestion_token_indexes (ingestion_token_id, index_id) VALUES
    ('tok_a', 'idx_a_main'),
    ('tok_b', 'idx_b_main');

INSERT INTO saved_searches (
    saved_search_id, tenant_id, version, name, app_id, owner_id, sharing_scope,
    definition_proto, created_at_unix_micro, updated_at_unix_micro
) VALUES
    ('ss_a', 'tenant-a', 1, 'errors', 'search', 'alice', 1, '\x00'::bytea, 1, 1),
    ('ss_b', 'tenant-b', 1, 'errors', 'search', 'alice', 1, '\x00'::bytea, 1, 1);
