-- open-splunk PostgreSQL control plane, part 1: tenants and the core ingest
-- catalog (indexes, ingest visibility/quota bookkeeping, ingestion tokens,
-- HEC state, server key state). Ported from the final SQLite schema
-- (migrations/sqlite/0001..0012); see migrations/postgres/README.md for the
-- translation rules. Triggers are ported separately.
--
-- Objects are created unqualified: the migration runner sets
-- search_path = open_splunk before applying these files.

-- octet_length() byte bounds and the regex character classes below assume
-- UTF-8 text, matching SQLite's UTF-8 storage.
DO $$
BEGIN
    IF current_setting('server_encoding') <> 'UTF8' THEN
        RAISE EXCEPTION
            'open_splunk control plane requires a UTF8 database, found %',
            current_setting('server_encoding');
    END IF;
END
$$;

CREATE SCHEMA IF NOT EXISTS open_splunk;

-- One row per open-splunk tenant. A go-common organization maps to exactly
-- one tenant; tenant_id is the stored ClickHouse tenant key (the bootstrap
-- organization claims the existing key 'default'), never derived from the
-- organization ID. organization_id gains its FK to go-common's organizations
-- table in a later phase. Tenants are disabled, never deleted.
CREATE TABLE tenants (
    tenant_id text COLLATE "C" PRIMARY KEY NOT NULL,
    organization_id bigint UNIQUE
        CONSTRAINT tenants_organization_id_positive
        CHECK (organization_id IS NULL OR organization_id >= 1),
    state text COLLATE "C" NOT NULL DEFAULT 'active'
        CONSTRAINT tenants_state_supported
        CHECK (state IN ('active', 'disabled')),
    adopted_clickhouse boolean NOT NULL DEFAULT false,
    created_at_unix_micro bigint NOT NULL
        CONSTRAINT tenants_created_at_bounded
        CHECK (created_at_unix_micro BETWEEN 1 AND 253402300799999999),
    updated_at_unix_micro bigint NOT NULL
        CONSTRAINT tenants_updated_at_bounded
        CHECK (
            updated_at_unix_micro BETWEEN
                created_at_unix_micro AND 253402300799999999
        ),
    CONSTRAINT tenants_tenant_id_bounded CHECK (
        octet_length(tenant_id) BETWEEN 1 AND 255
        AND tenant_id = trim(tenant_id)
        AND tenant_id !~ '[\u0001-\u001f\u007f-\u009f]'
    )
);

CREATE TABLE index_catalog_state (
    singleton_id bigint PRIMARY KEY NOT NULL
        CHECK (singleton_id = 1),
    revision bigint NOT NULL
        CHECK (revision BETWEEN 1 AND 9223372036854775807),
    physical_count bigint NOT NULL
        CHECK (physical_count BETWEEN 0 AND 1024)
);

INSERT INTO index_catalog_state (singleton_id, revision, physical_count)
VALUES (1, 1, 0);

CREATE TABLE indexes (
    index_id text COLLATE "C" PRIMARY KEY NOT NULL,
    -- Added for multi-tenancy (no SQLite counterpart).
    tenant_id text COLLATE "C" NOT NULL,
    version bigint NOT NULL CHECK (version >= 1),
    name text COLLATE "C" NOT NULL,
    display_name text COLLATE "C" NOT NULL,
    description text COLLATE "C" NOT NULL DEFAULT '',
    retention_nanoseconds bigint NOT NULL DEFAULT 0 CHECK (retention_nanoseconds >= 0),
    ingestion_enabled bigint NOT NULL CHECK (ingestion_enabled IN (0, 1)),
    search_enabled bigint NOT NULL CHECK (search_enabled IN (0, 1)),
    default_sourcetype text COLLATE "C" NOT NULL DEFAULT '',
    max_event_bytes bigint NOT NULL DEFAULT 0 CHECK (max_event_bytes >= 0),
    max_field_count bigint NOT NULL DEFAULT 0 CHECK (max_field_count >= 0),
    max_nesting_depth bigint NOT NULL DEFAULT 0 CHECK (max_nesting_depth >= 0),
    maximum_future_skew_nanoseconds bigint NOT NULL DEFAULT 0 CHECK (maximum_future_skew_nanoseconds >= 0),
    maximum_event_age_nanoseconds bigint NOT NULL DEFAULT 0 CHECK (maximum_event_age_nanoseconds >= 0),
    state text COLLATE "C" NOT NULL CHECK (state IN ('active', 'archived', 'deleting')),
    created_at_unix_micro bigint NOT NULL,
    updated_at_unix_micro bigint NOT NULL,
    max_ingest_events_per_second bigint NOT NULL DEFAULT 0
        CONSTRAINT indexes_max_ingest_events_per_second_bounded
        CHECK (max_ingest_events_per_second BETWEEN 0 AND 1000000),
    max_ingest_uncompressed_bytes_per_second bigint NOT NULL DEFAULT 0
        CONSTRAINT indexes_max_ingest_uncompressed_bytes_per_second_bounded
        CHECK (
            max_ingest_uncompressed_bytes_per_second
            BETWEEN 0 AND 1099511627776
        ),
    -- SQLite: name UNIQUE (global). Index names are unique per tenant.
    CONSTRAINT indexes_tenant_name_key UNIQUE (tenant_id, name),
    -- Target of the tenant-scoped foreign keys below, so a row that names an
    -- index_id must also carry that index's tenant (no SQLite counterpart).
    CONSTRAINT indexes_tenant_index_id_key UNIQUE (tenant_id, index_id),
    CHECK (char_length(name) BETWEEN 1 AND 255),
    CHECK (name = lower(name)),
    CHECK (name !~ '[^a-z0-9_-]'),
    CHECK (substr(name, 1, 1) ~ '^[a-z0-9]$'),
    CHECK (strpos(name, 'kvstore') = 0),
    CHECK (updated_at_unix_micro >= created_at_unix_micro),
    CONSTRAINT indexes_tenant_fk FOREIGN KEY (tenant_id)
        REFERENCES tenants (tenant_id) ON UPDATE RESTRICT ON DELETE RESTRICT
);

CREATE TABLE index_data_deletion_completions (
    deletion_operation_id text COLLATE "C" PRIMARY KEY NOT NULL,
    correlation_id text COLLATE "C" NOT NULL UNIQUE,
    index_id text COLLATE "C" NOT NULL UNIQUE,
    index_name text COLLATE "C" NOT NULL,
    archived_index_version bigint NOT NULL,
    deleting_index_version bigint NOT NULL,
    tenant_id text COLLATE "C" NOT NULL,
    clickhouse_database text COLLATE "C" NOT NULL,
    clickhouse_table text COLLATE "C" NOT NULL,
    clickhouse_table_uuid text COLLATE "C" NOT NULL,
    protocol_version bigint NOT NULL,
    operation_created_at_unix_micro bigint NOT NULL,
    attempt_created_at_unix_micro bigint NOT NULL,
    completed_at_unix_micro bigint NOT NULL,
    CONSTRAINT index_data_deletion_completions_operation_id_canonical
        CHECK (
            octet_length(deletion_operation_id) BETWEEN 1 AND 128
            AND substr(deletion_operation_id, 1, 1) ~ '^[A-Za-z0-9]$'
            AND deletion_operation_id !~ '[^A-Za-z0-9._:-]'
        ),
    CONSTRAINT index_data_deletion_completions_correlation_id_canonical
        CHECK (
            octet_length(correlation_id) BETWEEN 1 AND 128
            AND substr(correlation_id, 1, 1) ~ '^[A-Za-z0-9]$'
            AND correlation_id !~ '[^A-Za-z0-9._:-]'
        ),
    CONSTRAINT index_data_deletion_completions_index_name_canonical
        CHECK (
            octet_length(index_name) BETWEEN 1 AND 255
            AND index_name = lower(index_name)
            AND index_name !~ '[^a-z0-9_-]'
            AND substr(index_name, 1, 1) ~ '^[a-z0-9]$'
            AND strpos(index_name, 'kvstore') = 0
        ),
    CONSTRAINT index_data_deletion_completions_versions_supported
        CHECK (
            archived_index_version >= 1
            AND archived_index_version < 9223372036854775807
            AND deleting_index_version = archived_index_version + 1
        ),
    CONSTRAINT index_data_deletion_completions_tenant_id_bounded
        CHECK (
            octet_length(tenant_id) BETWEEN 1 AND 255
        ),
    CONSTRAINT index_data_deletion_completions_database_canonical
        CHECK (
            octet_length(clickhouse_database) BETWEEN 1 AND 255
            AND substr(clickhouse_database, 1, 1) ~ '^[A-Za-z_]$'
            AND clickhouse_database !~ '[^A-Za-z0-9_]'
        ),
    CONSTRAINT index_data_deletion_completions_table_canonical
        CHECK (
            octet_length(clickhouse_table) BETWEEN 1 AND 255
            AND substr(clickhouse_table, 1, 1) ~ '^[A-Za-z_]$'
            AND clickhouse_table !~ '[^A-Za-z0-9_]'
        ),
    CONSTRAINT index_data_deletion_completions_table_uuid_canonical
        CHECK (
            octet_length(clickhouse_table_uuid) = 36
            AND clickhouse_table_uuid = lower(clickhouse_table_uuid)
            AND substr(clickhouse_table_uuid, 9, 1) = '-'
            AND substr(clickhouse_table_uuid, 14, 1) = '-'
            AND substr(clickhouse_table_uuid, 19, 1) = '-'
            AND substr(clickhouse_table_uuid, 24, 1) = '-'
            AND char_length(replace(clickhouse_table_uuid, '-', '')) = 32
            AND replace(clickhouse_table_uuid, '-', '')
                !~ '[^0-9a-f]'
            AND clickhouse_table_uuid <>
                '00000000-0000-0000-0000-000000000000'
        ),
    CONSTRAINT index_data_deletion_completions_protocol_supported
        CHECK (protocol_version = 1),
    CONSTRAINT index_data_deletion_completions_timestamps_supported
        CHECK (
            operation_created_at_unix_micro
                BETWEEN 1 AND 253402300799999999
            AND attempt_created_at_unix_micro
                BETWEEN operation_created_at_unix_micro
                    AND 253402300799999999
            AND completed_at_unix_micro
                BETWEEN attempt_created_at_unix_micro
                    AND 253402300799999999
        ),
    -- SQLite: FOREIGN KEY (index_id). Scoped by tenant so a completion can
    -- only name an index of its own tenant; the SQLite constraint name is kept.
    CONSTRAINT index_data_deletion_completions_index_id_fkey
        FOREIGN KEY (tenant_id, index_id)
        REFERENCES indexes (tenant_id, index_id)
        ON DELETE RESTRICT,
    CONSTRAINT index_data_deletion_completions_tenant_fk FOREIGN KEY (tenant_id)
        REFERENCES tenants (tenant_id) ON UPDATE RESTRICT ON DELETE RESTRICT
);

CREATE TABLE index_deletion_operations (
    deletion_operation_id text COLLATE "C" PRIMARY KEY NOT NULL,
    index_id text COLLATE "C" NOT NULL UNIQUE,
    index_name text COLLATE "C" NOT NULL,
    tenant_id text COLLATE "C" NOT NULL,
    archived_index_version bigint NOT NULL
        CHECK (
            archived_index_version >= 1
            AND archived_index_version < 9223372036854775807
        ),
    created_at_unix_micro bigint NOT NULL
        CHECK (created_at_unix_micro > 0),
    CHECK (
        octet_length(deletion_operation_id) BETWEEN 1 AND 128
        AND substr(deletion_operation_id, 1, 1) ~ '^[A-Za-z0-9]$'
        AND deletion_operation_id !~ '[^A-Za-z0-9._:-]'
    ),
    CONSTRAINT index_deletion_operations_tenant_id_bounded
        CHECK (
            octet_length(tenant_id) BETWEEN 1 AND 255
            AND tenant_id = trim(tenant_id)
            AND tenant_id !~ '[\u0001-\u001f\u007f-\u009f]'
        ),
    -- Target of index_deletion_mutation_attempts' tenant-scoped key.
    CONSTRAINT index_deletion_operations_tenant_operation_key
        UNIQUE (tenant_id, deletion_operation_id),
    -- SQLite: FOREIGN KEY (index_id). Scoped by tenant: an operation (and the
    -- trigger that moves its index to 'deleting') can only target an index of
    -- its own tenant.
    CONSTRAINT index_deletion_operations_index_id_fkey
        FOREIGN KEY (tenant_id, index_id)
        REFERENCES indexes (tenant_id, index_id)
        ON DELETE RESTRICT,
    CONSTRAINT index_deletion_operations_tenant_fk FOREIGN KEY (tenant_id)
        REFERENCES tenants (tenant_id) ON UPDATE RESTRICT ON DELETE RESTRICT
);

CREATE TABLE index_deletion_mutation_attempts (
    deletion_operation_id text COLLATE "C" PRIMARY KEY NOT NULL,
    correlation_id text COLLATE "C" NOT NULL UNIQUE,
    tenant_id text COLLATE "C" NOT NULL,
    clickhouse_database text COLLATE "C" NOT NULL,
    clickhouse_table text COLLATE "C" NOT NULL,
    clickhouse_table_uuid text COLLATE "C" NOT NULL,
    protocol_version bigint NOT NULL,
    created_at_unix_micro bigint NOT NULL,
    CONSTRAINT index_deletion_mutation_attempts_correlation_id_canonical
        CHECK (
            octet_length(correlation_id) BETWEEN 1 AND 128
            AND substr(correlation_id, 1, 1) ~ '^[A-Za-z0-9]$'
            AND correlation_id !~ '[^A-Za-z0-9._:-]'
        ),
    CONSTRAINT index_deletion_mutation_attempts_tenant_id_bounded
        CHECK (
            octet_length(tenant_id) BETWEEN 1 AND 255
        ),
    CONSTRAINT index_deletion_mutation_attempts_database_canonical
        CHECK (
            octet_length(clickhouse_database) BETWEEN 1 AND 255
            AND substr(clickhouse_database, 1, 1) ~ '^[A-Za-z_]$'
            AND clickhouse_database !~ '[^A-Za-z0-9_]'
        ),
    CONSTRAINT index_deletion_mutation_attempts_table_canonical
        CHECK (
            octet_length(clickhouse_table) BETWEEN 1 AND 255
            AND substr(clickhouse_table, 1, 1) ~ '^[A-Za-z_]$'
            AND clickhouse_table !~ '[^A-Za-z0-9_]'
        ),
    CONSTRAINT index_deletion_mutation_attempts_table_uuid_canonical
        CHECK (
            octet_length(clickhouse_table_uuid) = 36
            AND clickhouse_table_uuid = lower(clickhouse_table_uuid)
            AND substr(clickhouse_table_uuid, 9, 1) = '-'
            AND substr(clickhouse_table_uuid, 14, 1) = '-'
            AND substr(clickhouse_table_uuid, 19, 1) = '-'
            AND substr(clickhouse_table_uuid, 24, 1) = '-'
            AND char_length(replace(clickhouse_table_uuid, '-', '')) = 32
            AND replace(clickhouse_table_uuid, '-', '')
                !~ '[^0-9a-f]'
            AND clickhouse_table_uuid <>
                '00000000-0000-0000-0000-000000000000'
        ),
    CONSTRAINT index_deletion_mutation_attempts_protocol_supported
        CHECK (protocol_version = 1),
    CONSTRAINT index_deletion_mutation_attempts_created_at_positive
        CHECK (
            created_at_unix_micro BETWEEN 1 AND 253402300799999999
        ),
    -- SQLite: FOREIGN KEY (deletion_operation_id). Scoped by tenant.
    CONSTRAINT index_deletion_mutation_attempts_deletion_operation_id_fkey
        FOREIGN KEY (tenant_id, deletion_operation_id)
        REFERENCES index_deletion_operations (tenant_id, deletion_operation_id)
        ON DELETE CASCADE,
    CONSTRAINT index_deletion_mutation_attempts_tenant_fk FOREIGN KEY (tenant_id)
        REFERENCES tenants (tenant_id) ON UPDATE RESTRICT ON DELETE RESTRICT
);

CREATE TABLE index_deletion_tombstones (
    index_id text COLLATE "C" PRIMARY KEY NOT NULL,
    name text COLLATE "C" NOT NULL,
    deleted_version bigint NOT NULL CHECK (deleted_version >= 1),
    deleted_at_unix_micro bigint NOT NULL CHECK (deleted_at_unix_micro > 0),
    FOREIGN KEY (index_id)
        REFERENCES indexes (index_id)
        ON DELETE RESTRICT
);

CREATE TABLE ingest_batch_identities (
    batch_key text COLLATE "C" PRIMARY KEY NOT NULL,
    sequence_key text COLLATE "C" NOT NULL UNIQUE,
    payload_sha256 bytea NOT NULL CHECK (octet_length(payload_sha256) = 32),
    first_visibility_seq bigint NOT NULL CHECK (first_visibility_seq >= 1),
    created_at_unix_micro bigint NOT NULL,
    CHECK (char_length(batch_key) BETWEEN 1 AND 512),
    CHECK (char_length(sequence_key) BETWEEN 1 AND 512)
);

CREATE TABLE ingest_quota_admissions (
    batch_key text COLLATE "C" PRIMARY KEY NOT NULL,
    admitted_at_unix_micro bigint NOT NULL,
    event_count bigint NOT NULL,
    uncompressed_bytes bigint NOT NULL,
    CONSTRAINT ingest_quota_admissions_admitted_at_bounded CHECK (
        admitted_at_unix_micro BETWEEN 1 AND 253402300799999999
    ),
    CONSTRAINT ingest_quota_admissions_event_count_bounded CHECK (
        event_count BETWEEN 1 AND 1000
    ),
    CONSTRAINT ingest_quota_admissions_uncompressed_bytes_bounded CHECK (
        uncompressed_bytes BETWEEN 1 AND 8388608
    ),
    FOREIGN KEY (batch_key) REFERENCES ingest_batch_identities (batch_key)
        ON UPDATE RESTRICT ON DELETE CASCADE
);

CREATE TABLE ingest_visibility_legacy_tombstones (
    batch_key text COLLATE "C" PRIMARY KEY NOT NULL,
    legacy_visibility_seq bigint NOT NULL UNIQUE
        CHECK (legacy_visibility_seq >= 1),
    created_at_unix_micro bigint NOT NULL,
    CHECK (char_length(batch_key) BETWEEN 1 AND 512)
);

CREATE TABLE ingest_visibility_reservations (
    sequence bigint PRIMARY KEY NOT NULL CHECK (sequence >= 1),
    batch_key text COLLATE "C" NOT NULL,
    state text COLLATE "C" NOT NULL CHECK (state IN ('reserved', 'committed', 'rejected', 'abandoned')),
    phase text COLLATE "C" NOT NULL CHECK (phase IN ('unsent', 'ambiguous', 'final')),
    attempt_id text COLLATE "C" NOT NULL DEFAULT '',
    index_time_unix_milli bigint NOT NULL,
    metadata bytea NOT NULL CHECK (octet_length(metadata) <= 1048576),
    outbox bytea NOT NULL CHECK (octet_length(outbox) <= 16777216),
    created_at_unix_micro bigint NOT NULL,
    committed_at_unix_micro bigint,
    outbox_sha256 bytea NOT NULL DEFAULT '\x'::bytea
        CHECK (octet_length(outbox_sha256) IN (0, 32)),
    stored_row_count bigint NOT NULL DEFAULT 0
        CHECK (stored_row_count BETWEEN 0 AND 1000),
    decoded_event_bytes bigint NOT NULL DEFAULT 0
        CHECK (decoded_event_bytes BETWEEN 0 AND 8388608),
    principal_sha256 bytea NOT NULL DEFAULT '\x'::bytea
        CHECK (octet_length(principal_sha256) IN (0, 32)),
    CHECK (char_length(batch_key) BETWEEN 1 AND 512),
    CHECK (char_length(attempt_id) <= 128),
    FOREIGN KEY (batch_key) REFERENCES ingest_batch_identities (batch_key)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
    CHECK (
        (state = 'reserved'
            AND phase IN ('unsent', 'ambiguous')
            AND octet_length(outbox) BETWEEN 1 AND 16777216
            AND committed_at_unix_micro IS NULL)
        OR (state = 'committed'
            AND phase = 'final'
            AND attempt_id = ''
            AND octet_length(outbox) = 0
            AND committed_at_unix_micro IS NOT NULL)
        -- The column name predates rejected dispositions; for every final
        -- active outcome it stores that disposition's terminal timestamp.
        OR (state = 'rejected'
            AND phase = 'final'
            AND attempt_id = ''
            AND octet_length(outbox) = 0
            AND committed_at_unix_micro IS NOT NULL)
        OR (state = 'abandoned'
            AND phase = 'final'
            AND attempt_id = ''
            AND octet_length(outbox) = 0
            AND committed_at_unix_micro IS NULL)
    )
);

CREATE TABLE ingest_visibility_state (
    singleton bigint PRIMARY KEY NOT NULL CHECK (singleton = 1),
    last_assigned bigint NOT NULL CHECK (last_assigned >= 0),
    committed_through bigint NOT NULL CHECK (committed_through >= 0),
    CHECK (committed_through <= last_assigned)
);

INSERT INTO ingest_visibility_state (singleton, last_assigned, committed_through)
VALUES (1, 0, 0);

CREATE TABLE ingest_write_groups (
    write_group_id text COLLATE "C" PRIMARY KEY NOT NULL,
    state text COLLATE "C" NOT NULL CHECK (state IN ('ready', 'ambiguous', 'committed')),
    attempt_id text COLLATE "C" NOT NULL DEFAULT '',
    member_count bigint NOT NULL CHECK (member_count BETWEEN 1 AND 10000),
    row_count bigint NOT NULL CHECK (row_count BETWEEN 1 AND 50000),
    decoded_bytes bigint NOT NULL CHECK (decoded_bytes BETWEEN 1 AND 67108864),
    membership_sha256 bytea NOT NULL CHECK (octet_length(membership_sha256) = 32),
    first_sequence bigint NOT NULL CHECK (first_sequence >= 1),
    last_sequence bigint NOT NULL CHECK (last_sequence >= first_sequence),
    created_at_unix_micro bigint NOT NULL,
    sending_at_unix_micro bigint,
    committed_at_unix_micro bigint,
    CHECK (char_length(write_group_id) BETWEEN 1 AND 64),
    CHECK (char_length(attempt_id) <= 128),
    CHECK (created_at_unix_micro BETWEEN 1 AND 253402300799999999),
    CHECK (
        sending_at_unix_micro IS NULL
        OR sending_at_unix_micro BETWEEN created_at_unix_micro AND 253402300799999999
    ),
    CHECK (
        committed_at_unix_micro IS NULL
        OR committed_at_unix_micro BETWEEN 1 AND 253402300799999999
    ),
    CHECK (
        (state = 'ready'
            AND sending_at_unix_micro IS NULL
            AND committed_at_unix_micro IS NULL)
        OR (state = 'ambiguous'
            AND sending_at_unix_micro IS NOT NULL
            AND committed_at_unix_micro IS NULL)
        OR (state = 'committed'
            AND attempt_id = ''
            AND sending_at_unix_micro IS NOT NULL
            AND committed_at_unix_micro IS NOT NULL
            AND committed_at_unix_micro >= sending_at_unix_micro)
    )
);

CREATE TABLE ingest_write_group_members (
    write_group_id text COLLATE "C" NOT NULL,
    ordinal bigint NOT NULL CHECK (ordinal BETWEEN 0 AND 9999),
    visibility_sequence bigint NOT NULL UNIQUE CHECK (visibility_sequence >= 1),
    row_count bigint NOT NULL CHECK (row_count BETWEEN 1 AND 1000),
    decoded_bytes bigint NOT NULL CHECK (decoded_bytes BETWEEN 1 AND 8388608),
    outbox_sha256 bytea NOT NULL CHECK (octet_length(outbox_sha256) = 32),
    PRIMARY KEY (write_group_id, ordinal),
    FOREIGN KEY (write_group_id) REFERENCES ingest_write_groups (write_group_id)
        ON UPDATE RESTRICT ON DELETE CASCADE,
    FOREIGN KEY (visibility_sequence) REFERENCES ingest_visibility_reservations (sequence)
        ON UPDATE RESTRICT ON DELETE RESTRICT
);

CREATE TABLE ingestion_tokens (
    ingestion_token_id text COLLATE "C" PRIMARY KEY NOT NULL,
    -- Added for multi-tenancy (no SQLite counterpart).
    tenant_id text COLLATE "C" NOT NULL,
    version bigint NOT NULL CHECK (version >= 1),
    name text COLLATE "C" NOT NULL,
    description text COLLATE "C" NOT NULL DEFAULT '',
    token_prefix text COLLATE "C" NOT NULL,
    token_digest bytea NOT NULL UNIQUE CHECK (octet_length(token_digest) = 32),
    state text COLLATE "C" NOT NULL CHECK (state IN ('active', 'disabled', 'revoked')),
    created_at_unix_micro bigint NOT NULL,
    updated_at_unix_micro bigint NOT NULL,
    expires_at_unix_micro bigint,
    revoked_at_unix_micro bigint,
    last_used_at_unix_micro bigint
        CONSTRAINT ingestion_tokens_last_use_not_before_create
        CHECK (
            last_used_at_unix_micro IS NULL
            OR last_used_at_unix_micro >= created_at_unix_micro
        ),
    bound_collector_id text COLLATE "C"
        CONSTRAINT ingestion_tokens_bound_collector_id_canonical
        CHECK (
            bound_collector_id IS NULL
            OR (
                char_length(bound_collector_id) BETWEEN 1 AND 128
                AND substr(bound_collector_id, 1, 1) ~ '^[A-Za-z0-9]$'
                AND bound_collector_id !~ '[^A-Za-z0-9._:-]'
            )
        ),
    max_ingest_events_per_second bigint NOT NULL DEFAULT 0
        CONSTRAINT ingestion_tokens_max_ingest_events_per_second_bounded
        CHECK (max_ingest_events_per_second BETWEEN 0 AND 1000000),
    max_ingest_uncompressed_bytes_per_second bigint NOT NULL DEFAULT 0
        CONSTRAINT ingestion_tokens_max_ingest_uncompressed_bytes_per_sec_bounded
        CHECK (
            max_ingest_uncompressed_bytes_per_second
            BETWEEN 0 AND 1099511627776
        ),
    purpose text COLLATE "C" NOT NULL DEFAULT 'native_collector'
        CONSTRAINT ingestion_tokens_purpose_supported
        CHECK (purpose IN ('native_collector', 'hec')),
    CHECK (char_length(name) BETWEEN 1 AND 255),
    CHECK (char_length(token_prefix) BETWEEN 8 AND 32),
    CHECK (updated_at_unix_micro >= created_at_unix_micro),
    CHECK (expires_at_unix_micro IS NULL OR expires_at_unix_micro > created_at_unix_micro),
    CHECK (
        (state = 'revoked' AND revoked_at_unix_micro IS NOT NULL)
        OR
        (state IN ('active', 'disabled') AND revoked_at_unix_micro IS NULL)
    ),
    -- Target of the tenant-scoped foreign keys of the HEC and ingest bucket
    -- tables (no SQLite counterpart).
    CONSTRAINT ingestion_tokens_tenant_token_key
        UNIQUE (tenant_id, ingestion_token_id),
    CONSTRAINT ingestion_tokens_tenant_fk FOREIGN KEY (tenant_id)
        REFERENCES tenants (tenant_id) ON UPDATE RESTRICT ON DELETE RESTRICT
);

CREATE TABLE hec_channels (
    tenant_id text COLLATE "C" NOT NULL,
    ingestion_token_id text COLLATE "C" NOT NULL,
    channel_id text COLLATE "C" NOT NULL,
    next_acknowledgment_id bigint NOT NULL DEFAULT 1
        CHECK (next_acknowledgment_id >= 1),
    created_at_unix_micro bigint NOT NULL,
    last_used_at_unix_micro bigint NOT NULL,
    PRIMARY KEY (tenant_id, ingestion_token_id, channel_id),
    CHECK (octet_length(tenant_id) BETWEEN 1 AND 255),
    CHECK (octet_length(channel_id) BETWEEN 1 AND 128),
    CHECK (channel_id = trim(channel_id)),
    CHECK (last_used_at_unix_micro >= created_at_unix_micro),
    -- SQLite: FOREIGN KEY (ingestion_token_id). Scoped by tenant so the row's
    -- tenant is its token's tenant; the SQLite constraint name is kept.
    CONSTRAINT hec_channels_ingestion_token_id_fkey
        FOREIGN KEY (tenant_id, ingestion_token_id)
        REFERENCES ingestion_tokens (tenant_id, ingestion_token_id)
        ON UPDATE RESTRICT ON DELETE CASCADE,
    CONSTRAINT hec_channels_tenant_fk FOREIGN KEY (tenant_id)
        REFERENCES tenants (tenant_id) ON UPDATE RESTRICT ON DELETE RESTRICT
);

CREATE TABLE hec_source_sequences (
    tenant_id text COLLATE "C" NOT NULL,
    ingestion_token_id text COLLATE "C" NOT NULL,
    next_request_sequence bigint NOT NULL DEFAULT 1
        CHECK (next_request_sequence >= 1),
    updated_at_unix_micro bigint NOT NULL,
    PRIMARY KEY (tenant_id, ingestion_token_id),
    CHECK (octet_length(tenant_id) BETWEEN 1 AND 255),
    -- SQLite: FOREIGN KEY (ingestion_token_id). Scoped by tenant so the row's
    -- tenant is its token's tenant; the SQLite constraint name is kept.
    CONSTRAINT hec_source_sequences_ingestion_token_id_fkey
        FOREIGN KEY (tenant_id, ingestion_token_id)
        REFERENCES ingestion_tokens (tenant_id, ingestion_token_id)
        ON UPDATE RESTRICT ON DELETE CASCADE,
    CONSTRAINT hec_source_sequences_tenant_fk FOREIGN KEY (tenant_id)
        REFERENCES tenants (tenant_id) ON UPDATE RESTRICT ON DELETE RESTRICT
);

CREATE TABLE hec_requests (
    tenant_id text COLLATE "C" NOT NULL,
    ingestion_token_id text COLLATE "C" NOT NULL,
    request_sequence bigint NOT NULL CHECK (request_sequence >= 1),
    request_id text COLLATE "C" NOT NULL,
    visibility_sequence bigint UNIQUE,
    state text COLLATE "C" NOT NULL DEFAULT 'pending'
        CHECK (state IN ('pending', 'indexed', 'terminal_failure')),
    created_at_unix_micro bigint NOT NULL,
    terminal_at_unix_micro bigint,
    PRIMARY KEY (tenant_id, ingestion_token_id, request_sequence),
    UNIQUE (tenant_id, ingestion_token_id, request_id),
    FOREIGN KEY (tenant_id, ingestion_token_id)
        REFERENCES hec_source_sequences (tenant_id, ingestion_token_id)
            ON UPDATE RESTRICT
            ON DELETE CASCADE,
    CHECK (octet_length(request_id) BETWEEN 1 AND 128),
    CHECK (
        (state = 'pending'
            AND visibility_sequence IS NOT NULL
            AND terminal_at_unix_micro IS NULL)
        OR
        (state IN ('indexed', 'terminal_failure')
            AND terminal_at_unix_micro IS NOT NULL)
    ),
    FOREIGN KEY (visibility_sequence)
        REFERENCES ingest_visibility_reservations (sequence)
        ON UPDATE RESTRICT ON DELETE SET NULL,
    CONSTRAINT hec_requests_tenant_fk FOREIGN KEY (tenant_id)
        REFERENCES tenants (tenant_id) ON UPDATE RESTRICT ON DELETE RESTRICT
);

CREATE TABLE hec_acknowledgments (
    tenant_id text COLLATE "C" NOT NULL,
    ingestion_token_id text COLLATE "C" NOT NULL,
    channel_id text COLLATE "C" NOT NULL,
    acknowledgment_id bigint NOT NULL
        CHECK (acknowledgment_id BETWEEN 1 AND 9007199254740991),
    request_sequence bigint NOT NULL CHECK (request_sequence >= 1),
    created_at_unix_micro bigint NOT NULL,
    PRIMARY KEY (
        tenant_id,
        ingestion_token_id,
        channel_id,
        acknowledgment_id
    ),
    UNIQUE (tenant_id, ingestion_token_id, request_sequence),
    FOREIGN KEY (tenant_id, ingestion_token_id, channel_id)
        REFERENCES hec_channels (tenant_id, ingestion_token_id, channel_id)
            ON UPDATE RESTRICT
            ON DELETE CASCADE,
    FOREIGN KEY (tenant_id, ingestion_token_id, request_sequence)
        REFERENCES hec_requests (
            tenant_id,
            ingestion_token_id,
            request_sequence
        )
            ON UPDATE RESTRICT
            ON DELETE CASCADE,
    CONSTRAINT hec_acknowledgments_tenant_fk FOREIGN KEY (tenant_id)
        REFERENCES tenants (tenant_id) ON UPDATE RESTRICT ON DELETE RESTRICT
);

CREATE TABLE ingest_quota_buckets (
    tenant_id text COLLATE "C" NOT NULL,
    scope_kind text COLLATE "C" NOT NULL,
    scope_id text COLLATE "C" NOT NULL,
    max_ingest_events_per_second bigint NOT NULL,
    max_ingest_uncompressed_bytes_per_second bigint NOT NULL,
    next_event_admission_unix_nano bigint NOT NULL,
    next_byte_admission_unix_nano bigint NOT NULL,
    updated_at_unix_micro bigint NOT NULL,
    -- Token scopes duplicate their scope ID here solely to obtain a real
    -- cascading FK. Index scopes leave it NULL; index identities are already
    -- bounded and retained by the catalog's deletion-tombstone contract.
    token_owner_id text COLLATE "C",
    PRIMARY KEY (tenant_id, scope_kind, scope_id),
    CONSTRAINT ingest_quota_buckets_tenant_id_bounded CHECK (
        octet_length(tenant_id) BETWEEN 1 AND 255
    ),
    CONSTRAINT ingest_quota_buckets_scope_kind_supported CHECK (
        scope_kind IN ('token', 'index')
    ),
    CONSTRAINT ingest_quota_buckets_scope_id_bounded CHECK (
        octet_length(scope_id) BETWEEN 1 AND 255
    ),
    CONSTRAINT ingest_quota_buckets_token_owner_consistent CHECK (
        (
            scope_kind = 'token'
            AND token_owner_id IS NOT NULL
            AND token_owner_id = scope_id
        )
        OR (
            scope_kind = 'index'
            AND token_owner_id IS NULL
        )
    ),
    CONSTRAINT ingest_quota_buckets_event_schedule_valid CHECK (
        (
            max_ingest_events_per_second = 0
            AND next_event_admission_unix_nano = 0
        )
        OR (
            max_ingest_events_per_second BETWEEN 1 AND 1000000
            AND next_event_admission_unix_nano
                BETWEEN 1 AND 9223372036854775807
        )
    ),
    CONSTRAINT ingest_quota_buckets_byte_schedule_valid CHECK (
        (
            max_ingest_uncompressed_bytes_per_second = 0
            AND next_byte_admission_unix_nano = 0
        )
        OR (
            max_ingest_uncompressed_bytes_per_second
                BETWEEN 1 AND 1099511627776
            AND next_byte_admission_unix_nano
                BETWEEN 1 AND 9223372036854775807
        )
    ),
    CONSTRAINT ingest_quota_buckets_updated_at_bounded CHECK (
        updated_at_unix_micro BETWEEN 1 AND 253402300799999999
    ),
    -- SQLite: FOREIGN KEY (token_owner_id). Scoped by tenant so a token
    -- bucket lives in its token's tenant; index buckets (NULL owner) are not
    -- checked, as before. The SQLite constraint name is kept.
    CONSTRAINT ingest_quota_buckets_token_owner_id_fkey
        FOREIGN KEY (tenant_id, token_owner_id)
        REFERENCES ingestion_tokens (tenant_id, ingestion_token_id)
        ON UPDATE RESTRICT ON DELETE CASCADE,
    CONSTRAINT ingest_quota_buckets_tenant_fk FOREIGN KEY (tenant_id)
        REFERENCES tenants (tenant_id) ON UPDATE RESTRICT ON DELETE RESTRICT
);

CREATE TABLE ingest_rejection_buckets (
    tenant_id text COLLATE "C" NOT NULL
        CHECK (octet_length(tenant_id) BETWEEN 1 AND 255),
    token_id text COLLATE "C" NOT NULL
        CHECK (octet_length(token_id) BETWEEN 1 AND 255),
    max_rejections_per_second bigint NOT NULL
        CHECK (max_rejections_per_second BETWEEN 1 AND 10),
    max_metadata_bytes_per_second bigint NOT NULL
        CHECK (max_metadata_bytes_per_second BETWEEN 1 AND 262144),
    next_rejection_unix_nano bigint NOT NULL CHECK (next_rejection_unix_nano > 0),
    next_metadata_unix_nano bigint NOT NULL CHECK (next_metadata_unix_nano > 0),
    updated_at_unix_micro bigint NOT NULL
        CHECK (updated_at_unix_micro BETWEEN 1 AND 253402300799999999),
    PRIMARY KEY (tenant_id, token_id),
    -- SQLite: FOREIGN KEY (token_id). Scoped by tenant; the SQLite
    -- constraint name is kept.
    CONSTRAINT ingest_rejection_buckets_token_id_fkey
        FOREIGN KEY (tenant_id, token_id)
        REFERENCES ingestion_tokens (tenant_id, ingestion_token_id)
        ON UPDATE RESTRICT ON DELETE CASCADE,
    CONSTRAINT ingest_rejection_buckets_tenant_fk FOREIGN KEY (tenant_id)
        REFERENCES tenants (tenant_id) ON UPDATE RESTRICT ON DELETE RESTRICT
);

CREATE TABLE ingestion_token_constraints (
    ingestion_token_id text COLLATE "C" NOT NULL,
    constraint_kind text COLLATE "C" NOT NULL
        CHECK (constraint_kind IN ('host', 'source')),
    ordinal bigint NOT NULL CHECK (ordinal BETWEEN 0 AND 15),
    pattern text COLLATE "C" NOT NULL,
    PRIMARY KEY (ingestion_token_id, constraint_kind, ordinal),
    CHECK (octet_length(pattern) BETWEEN 1 AND 512),
    FOREIGN KEY (ingestion_token_id)
        REFERENCES ingestion_tokens (ingestion_token_id)
        ON UPDATE RESTRICT ON DELETE CASCADE
);

CREATE TABLE ingestion_token_indexes (
    ingestion_token_id text COLLATE "C" NOT NULL,
    index_id text COLLATE "C" NOT NULL,
    PRIMARY KEY (ingestion_token_id, index_id),
    FOREIGN KEY (ingestion_token_id)
        REFERENCES ingestion_tokens (ingestion_token_id)
        ON DELETE CASCADE,
    FOREIGN KEY (index_id)
        REFERENCES indexes (index_id)
        ON DELETE RESTRICT
);

CREATE TABLE ingestion_token_hec_profiles (
    ingestion_token_id text COLLATE "C" PRIMARY KEY NOT NULL,
    default_index_id text COLLATE "C",
    default_host text COLLATE "C",
    default_source text COLLATE "C",
    default_sourcetype text COLLATE "C",
    indexer_acknowledgment bigint NOT NULL
        CHECK (indexer_acknowledgment IN (0, 1)),
    CONSTRAINT ingestion_token_hec_profiles_default_index_membership
        FOREIGN KEY (ingestion_token_id, default_index_id)
        REFERENCES ingestion_token_indexes (ingestion_token_id, index_id)
            ON UPDATE RESTRICT
            ON DELETE RESTRICT,
    CONSTRAINT ingestion_token_hec_profiles_default_host_bounded CHECK (
        default_host IS NULL
        OR (
            octet_length(default_host) BETWEEN 1 AND 255
            AND default_host = trim(default_host)
            AND default_host !~ '[\u0001-\u001f\u007f-\u009f]'
        )
    ),
    CONSTRAINT ingestion_token_hec_profiles_default_source_bounded CHECK (
        default_source IS NULL
        OR (
            octet_length(default_source) BETWEEN 1 AND 255
            AND default_source = trim(default_source)
            AND default_source !~ '[\u0001-\u001f\u007f-\u009f]'
        )
    ),
    CONSTRAINT ingestion_token_hec_profiles_default_sourcetype_bounded CHECK (
        default_sourcetype IS NULL
        OR (
            octet_length(default_sourcetype) BETWEEN 1 AND 255
            AND default_sourcetype = trim(default_sourcetype)
            AND default_sourcetype !~ '[\u0001-\u001f\u007f-\u009f]'
        )
    ),
    FOREIGN KEY (ingestion_token_id)
        REFERENCES ingestion_tokens (ingestion_token_id)
        ON UPDATE RESTRICT ON DELETE CASCADE
);

CREATE TABLE server_key_state (
    key_name text COLLATE "C" PRIMARY KEY NOT NULL,
    fingerprint bytea NOT NULL CHECK (octet_length(fingerprint) = 32),
    created_at_unix_micro bigint NOT NULL,
    CHECK (key_name = 'server-master-v1')
);

CREATE INDEX hec_acknowledgments_bounded_lookup_idx
    ON hec_acknowledgments (
        tenant_id,
        ingestion_token_id,
        channel_id,
        acknowledgment_id,
        request_sequence
    );

CREATE INDEX hec_channels_token_activity_idx
    ON hec_channels (
        tenant_id,
        ingestion_token_id,
        last_used_at_unix_micro,
        channel_id
    );

CREATE INDEX hec_requests_terminal_retention_idx
    ON hec_requests (
        state,
        terminal_at_unix_micro,
        tenant_id,
        ingestion_token_id,
        request_sequence
    )
    WHERE state IN ('indexed', 'terminal_failure');

CREATE INDEX index_deletion_operations_created_id_idx
    ON index_deletion_operations (
        created_at_unix_micro,
        deletion_operation_id
    );

CREATE INDEX indexes_created_id_idx
    ON indexes (created_at_unix_micro, index_id);

CREATE INDEX indexes_name_id_idx
    ON indexes (name, index_id);

CREATE INDEX indexes_state_name_idx ON indexes (state, name);

CREATE INDEX indexes_updated_id_idx
    ON indexes (updated_at_unix_micro, index_id);

CREATE INDEX ingest_quota_buckets_token_owner_idx
    ON ingest_quota_buckets (token_owner_id)
    WHERE token_owner_id IS NOT NULL;

CREATE INDEX ingest_rejection_buckets_token_idx ON ingest_rejection_buckets (token_id);

CREATE INDEX ingest_visibility_principal_pending_idx
    ON ingest_visibility_reservations (principal_sha256)
    WHERE state = 'reserved';

CREATE UNIQUE INDEX ingest_visibility_reservations_active_batch_idx
    ON ingest_visibility_reservations (batch_key)
    WHERE state IN ('reserved', 'committed', 'rejected');

CREATE UNIQUE INDEX ingest_visibility_reservations_attempt_idx
    ON ingest_visibility_reservations (attempt_id)
    WHERE attempt_id <> '';

CREATE INDEX ingest_visibility_reservations_batch_sequence_idx
    ON ingest_visibility_reservations (batch_key, sequence DESC);

CREATE INDEX ingest_visibility_reservations_group_formation_idx
    ON ingest_visibility_reservations (state, phase, attempt_id, sequence);

CREATE INDEX ingest_visibility_reservations_state_sequence_idx
    ON ingest_visibility_reservations (state, sequence);

CREATE INDEX ingest_write_group_members_sequence_idx
    ON ingest_write_group_members (visibility_sequence);

CREATE UNIQUE INDEX ingest_write_groups_attempt_idx
    ON ingest_write_groups (attempt_id)
    WHERE attempt_id <> '';

CREATE INDEX ingest_write_groups_state_sequence_idx
    ON ingest_write_groups (state, first_sequence);

CREATE INDEX ingestion_token_indexes_index_idx
    ON ingestion_token_indexes (index_id, ingestion_token_id);

CREATE INDEX ingestion_tokens_revoked_retention_idx
    ON ingestion_tokens (
        revoked_at_unix_micro DESC,
        ingestion_token_id DESC
    )
    WHERE state = 'revoked';
