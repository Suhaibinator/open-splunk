-- Fresh-state-only SQLite baseline for the current Open Splunk schema.
-- Databases created from earlier migration histories are intentionally unsupported.

CREATE TABLE indexes (
    index_id TEXT PRIMARY KEY NOT NULL,
    version INTEGER NOT NULL CHECK (version >= 1),
    name TEXT NOT NULL UNIQUE COLLATE BINARY,
    display_name TEXT NOT NULL,
    description TEXT NOT NULL DEFAULT '',
    retention_nanoseconds INTEGER NOT NULL DEFAULT 0 CHECK (retention_nanoseconds >= 0),
    ingestion_enabled INTEGER NOT NULL CHECK (ingestion_enabled IN (0, 1)),
    search_enabled INTEGER NOT NULL CHECK (search_enabled IN (0, 1)),
    default_sourcetype TEXT NOT NULL DEFAULT '',
    max_event_bytes INTEGER NOT NULL DEFAULT 0 CHECK (max_event_bytes >= 0),
    max_field_count INTEGER NOT NULL DEFAULT 0 CHECK (max_field_count >= 0),
    max_nesting_depth INTEGER NOT NULL DEFAULT 0 CHECK (max_nesting_depth >= 0),
    maximum_future_skew_nanoseconds INTEGER NOT NULL DEFAULT 0 CHECK (maximum_future_skew_nanoseconds >= 0),
    maximum_event_age_nanoseconds INTEGER NOT NULL DEFAULT 0 CHECK (maximum_event_age_nanoseconds >= 0),
    state TEXT NOT NULL CHECK (state IN ('active', 'archived', 'deleting')),
    created_at_unix_micro INTEGER NOT NULL,
    updated_at_unix_micro INTEGER NOT NULL, max_ingest_events_per_second INTEGER NOT NULL DEFAULT 0
    CONSTRAINT indexes_max_ingest_events_per_second_bounded
    CHECK (max_ingest_events_per_second BETWEEN 0 AND 1000000), max_ingest_uncompressed_bytes_per_second INTEGER NOT NULL DEFAULT 0
    CONSTRAINT indexes_max_ingest_uncompressed_bytes_per_second_bounded
    CHECK (
        max_ingest_uncompressed_bytes_per_second
        BETWEEN 0 AND 1099511627776
    ),
    CHECK (length(name) BETWEEN 1 AND 255),
    CHECK (name = lower(name)),
    CHECK (name NOT GLOB '*[^a-z0-9_-]*'),
    CHECK (substr(name, 1, 1) GLOB '[a-z0-9]'),
    CHECK (instr(name, 'kvstore') = 0),
    CHECK (updated_at_unix_micro >= created_at_unix_micro)
) STRICT;
CREATE TABLE ingestion_tokens (
    ingestion_token_id TEXT PRIMARY KEY NOT NULL,
    version INTEGER NOT NULL CHECK (version >= 1),
    name TEXT NOT NULL,
    description TEXT NOT NULL DEFAULT '',
    token_prefix TEXT NOT NULL,
    token_digest BLOB NOT NULL UNIQUE CHECK (length(token_digest) = 32),
    state TEXT NOT NULL CHECK (state IN ('active', 'disabled', 'revoked')),
    created_at_unix_micro INTEGER NOT NULL,
    updated_at_unix_micro INTEGER NOT NULL,
    expires_at_unix_micro INTEGER,
    revoked_at_unix_micro INTEGER, last_used_at_unix_micro INTEGER
    CONSTRAINT ingestion_tokens_last_use_not_before_create
    CHECK (
        last_used_at_unix_micro IS NULL
        OR last_used_at_unix_micro >= created_at_unix_micro
    ), bound_collector_id TEXT
    CONSTRAINT ingestion_tokens_bound_collector_id_canonical
    CHECK (
        bound_collector_id IS NULL
        OR (
            length(bound_collector_id) BETWEEN 1 AND 128
            AND instr(bound_collector_id, char(0)) = 0
            AND substr(bound_collector_id, 1, 1) GLOB '[A-Za-z0-9]'
            AND bound_collector_id NOT GLOB '*[^A-Za-z0-9._:-]*'
        )
    ), max_ingest_events_per_second INTEGER NOT NULL DEFAULT 0
    CONSTRAINT ingestion_tokens_max_ingest_events_per_second_bounded
    CHECK (max_ingest_events_per_second BETWEEN 0 AND 1000000), max_ingest_uncompressed_bytes_per_second INTEGER NOT NULL DEFAULT 0
    CONSTRAINT ingestion_tokens_max_ingest_uncompressed_bytes_per_second_bounded
    CHECK (
        max_ingest_uncompressed_bytes_per_second
        BETWEEN 0 AND 1099511627776
    ), purpose TEXT NOT NULL DEFAULT 'native_collector' COLLATE BINARY
    CONSTRAINT ingestion_tokens_purpose_supported
    CHECK (purpose IN ('native_collector', 'hec')),
    CHECK (length(name) BETWEEN 1 AND 255),
    CHECK (length(token_prefix) BETWEEN 8 AND 32),
    CHECK (updated_at_unix_micro >= created_at_unix_micro),
    CHECK (expires_at_unix_micro IS NULL OR expires_at_unix_micro > created_at_unix_micro),
    CHECK (
        (state = 'revoked' AND revoked_at_unix_micro IS NOT NULL)
        OR
        (state IN ('active', 'disabled') AND revoked_at_unix_micro IS NULL)
    )
) STRICT;
CREATE TABLE ingestion_token_indexes (
    ingestion_token_id TEXT NOT NULL
        REFERENCES ingestion_tokens (ingestion_token_id) ON DELETE CASCADE,
    index_id TEXT NOT NULL
        REFERENCES indexes (index_id) ON DELETE RESTRICT,
    PRIMARY KEY (ingestion_token_id, index_id)
) STRICT, WITHOUT ROWID;
CREATE TABLE ingestion_token_constraints (
    ingestion_token_id TEXT NOT NULL
        REFERENCES ingestion_tokens (ingestion_token_id)
            ON UPDATE RESTRICT
            ON DELETE CASCADE,
    constraint_kind TEXT NOT NULL COLLATE BINARY
        CHECK (constraint_kind IN ('host', 'source')),
    ordinal INTEGER NOT NULL CHECK (ordinal BETWEEN 0 AND 15),
    pattern TEXT NOT NULL COLLATE BINARY,
    PRIMARY KEY (ingestion_token_id, constraint_kind, ordinal),
    CHECK (length(CAST(pattern AS BLOB)) BETWEEN 1 AND 512),
    CHECK (instr(CAST(pattern AS BLOB), X'00') = 0)
) STRICT, WITHOUT ROWID;
CREATE TABLE ingest_visibility_state (
    singleton INTEGER PRIMARY KEY NOT NULL CHECK (singleton = 1),
    last_assigned INTEGER NOT NULL CHECK (last_assigned >= 0),
    committed_through INTEGER NOT NULL CHECK (committed_through >= 0),
    CHECK (committed_through <= last_assigned)
) STRICT;
INSERT INTO ingest_visibility_state VALUES(1,0,0);
CREATE TABLE ingest_visibility_legacy_tombstones (
    batch_key TEXT PRIMARY KEY NOT NULL COLLATE BINARY,
    legacy_visibility_seq INTEGER NOT NULL UNIQUE
        CHECK (legacy_visibility_seq >= 1),
    created_at_unix_micro INTEGER NOT NULL,
    CHECK (length(batch_key) BETWEEN 1 AND 512)
) STRICT;
CREATE TABLE ingest_batch_identities (
    batch_key TEXT PRIMARY KEY NOT NULL COLLATE BINARY,
    sequence_key TEXT NOT NULL UNIQUE COLLATE BINARY,
    payload_sha256 BLOB NOT NULL CHECK (length(payload_sha256) = 32),
    first_visibility_seq INTEGER NOT NULL CHECK (first_visibility_seq >= 1),
    created_at_unix_micro INTEGER NOT NULL,
    CHECK (length(batch_key) BETWEEN 1 AND 512),
    CHECK (length(sequence_key) BETWEEN 1 AND 512)
) STRICT;
CREATE TABLE ingest_visibility_reservations (
    sequence INTEGER PRIMARY KEY NOT NULL CHECK (sequence >= 1),
    batch_key TEXT NOT NULL COLLATE BINARY,
    state TEXT NOT NULL CHECK (state IN ('reserved', 'committed', 'rejected', 'abandoned')),
    phase TEXT NOT NULL CHECK (phase IN ('unsent', 'ambiguous', 'final')),
    attempt_id TEXT NOT NULL DEFAULT '' COLLATE BINARY,
    index_time_unix_milli INTEGER NOT NULL,
    metadata BLOB NOT NULL CHECK (length(metadata) <= 1048576),
    outbox BLOB NOT NULL CHECK (length(outbox) <= 16777216),
    outbox_sha256 BLOB NOT NULL CHECK (length(outbox_sha256) IN (0, 32)),
    stored_row_count INTEGER NOT NULL CHECK (stored_row_count BETWEEN 0 AND 1000),
    decoded_event_bytes INTEGER NOT NULL CHECK (decoded_event_bytes BETWEEN 0 AND 8388608),
    created_at_unix_micro INTEGER NOT NULL,
    committed_at_unix_micro INTEGER,
    CHECK (length(batch_key) BETWEEN 1 AND 512),
    CHECK (length(attempt_id) <= 128),
    FOREIGN KEY (batch_key) REFERENCES ingest_batch_identities (batch_key)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
    CHECK (
        (state = 'reserved'
            AND phase IN ('unsent', 'ambiguous')
            AND length(outbox) BETWEEN 1 AND 16777216
            AND length(outbox_sha256) = 32
            AND stored_row_count BETWEEN 1 AND 1000
            AND decoded_event_bytes BETWEEN 1 AND 8388608
            AND committed_at_unix_micro IS NULL)
        OR (state = 'committed'
            AND phase = 'final'
            AND attempt_id = ''
            AND length(outbox) = 0
            AND length(outbox_sha256) = 32
            AND stored_row_count BETWEEN 1 AND 1000
            AND decoded_event_bytes BETWEEN 1 AND 8388608
            AND committed_at_unix_micro IS NOT NULL)
        -- The column name predates rejected dispositions; for every final
        -- active outcome it stores that disposition's terminal timestamp.
        OR (state = 'rejected'
            AND phase = 'final'
            AND attempt_id = ''
            AND length(outbox) = 0
            AND length(outbox_sha256) = 0
            AND stored_row_count = 0
            AND decoded_event_bytes = 0
            AND committed_at_unix_micro IS NOT NULL)
        OR (state = 'abandoned'
            AND phase = 'final'
            AND attempt_id = ''
            AND length(outbox) = 0
            AND length(outbox_sha256) IN (0, 32)
            AND committed_at_unix_micro IS NULL)
    )
) STRICT;
CREATE TABLE ingest_write_groups (
    write_group_id TEXT PRIMARY KEY NOT NULL COLLATE BINARY,
    state TEXT NOT NULL CHECK (state IN ('ready', 'ambiguous', 'committed')),
    attempt_id TEXT NOT NULL DEFAULT '' COLLATE BINARY,
    member_count INTEGER NOT NULL CHECK (member_count BETWEEN 1 AND 10000),
    row_count INTEGER NOT NULL CHECK (row_count BETWEEN 1 AND 50000),
    decoded_bytes INTEGER NOT NULL CHECK (decoded_bytes BETWEEN 1 AND 67108864),
    membership_sha256 BLOB NOT NULL CHECK (length(membership_sha256) = 32),
    first_sequence INTEGER NOT NULL CHECK (first_sequence >= 1),
    last_sequence INTEGER NOT NULL CHECK (last_sequence >= first_sequence),
    created_at_unix_micro INTEGER NOT NULL,
    sending_at_unix_micro INTEGER,
    committed_at_unix_micro INTEGER,
    CHECK (length(write_group_id) BETWEEN 1 AND 64),
    CHECK (length(attempt_id) <= 128),
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
) STRICT;
CREATE TABLE ingest_write_group_members (
    write_group_id TEXT NOT NULL COLLATE BINARY,
    ordinal INTEGER NOT NULL CHECK (ordinal BETWEEN 0 AND 9999),
    visibility_sequence INTEGER NOT NULL UNIQUE CHECK (visibility_sequence >= 1),
    row_count INTEGER NOT NULL CHECK (row_count BETWEEN 1 AND 1000),
    decoded_bytes INTEGER NOT NULL CHECK (decoded_bytes BETWEEN 1 AND 8388608),
    outbox_sha256 BLOB NOT NULL CHECK (length(outbox_sha256) = 32),
    PRIMARY KEY (write_group_id, ordinal),
    FOREIGN KEY (write_group_id) REFERENCES ingest_write_groups (write_group_id)
        ON UPDATE RESTRICT ON DELETE CASCADE,
    FOREIGN KEY (visibility_sequence) REFERENCES ingest_visibility_reservations (sequence)
        ON UPDATE RESTRICT ON DELETE RESTRICT
) STRICT, WITHOUT ROWID;
CREATE TABLE saved_searches (
    saved_search_id TEXT PRIMARY KEY NOT NULL COLLATE BINARY,
    version INTEGER NOT NULL CHECK (version >= 1),
    name TEXT NOT NULL COLLATE BINARY,
    app_id TEXT NOT NULL COLLATE BINARY,
    owner_id TEXT NOT NULL COLLATE BINARY,
    sharing_scope INTEGER NOT NULL CHECK (sharing_scope BETWEEN 1 AND 3),
    definition_proto BLOB NOT NULL CHECK (length(definition_proto) BETWEEN 1 AND 262144),
    created_at_unix_micro INTEGER NOT NULL,
    updated_at_unix_micro INTEGER NOT NULL,
    CHECK (length(saved_search_id) BETWEEN 1 AND 128),
    CHECK (length(name) BETWEEN 1 AND 255),
    CHECK (length(app_id) <= 255),
    CHECK (length(owner_id) <= 255),
    CHECK (updated_at_unix_micro >= created_at_unix_micro),
    UNIQUE (owner_id, app_id, name)
) STRICT;
CREATE TABLE server_key_state (
    key_name TEXT PRIMARY KEY NOT NULL COLLATE BINARY,
    fingerprint BLOB NOT NULL CHECK (length(fingerprint) = 32),
    created_at_unix_micro INTEGER NOT NULL,
    CHECK (key_name = 'server-master-v1')
) STRICT;
CREATE TABLE search_history (
    search_job_id TEXT PRIMARY KEY NOT NULL COLLATE BINARY,
    tenant_id TEXT NOT NULL COLLATE BINARY,
    owner_id TEXT NOT NULL COLLATE BINARY,
    app_id TEXT NOT NULL COLLATE BINARY,
    saved_search_id TEXT NOT NULL COLLATE BINARY,
    final_state INTEGER NOT NULL CHECK (final_state BETWEEN 6 AND 9),
    search_text TEXT NOT NULL COLLATE BINARY,
    created_at_unix_micro INTEGER NOT NULL,
    finished_at_unix_micro INTEGER NOT NULL,
    duration_nanoseconds INTEGER NOT NULL CHECK (duration_nanoseconds >= 0),
    matched_events INTEGER NOT NULL CHECK (matched_events >= 0),
    entry_proto BLOB NOT NULL CHECK (length(entry_proto) BETWEEN 1 AND 524288),
    entry_sha256 BLOB NOT NULL CHECK (length(entry_sha256) = 32),
    CHECK (length(search_job_id) BETWEEN 1 AND 256),
    CHECK (length(tenant_id) BETWEEN 1 AND 1024),
    CHECK (length(owner_id) BETWEEN 1 AND 255),
    CHECK (length(app_id) <= 255),
    CHECK (length(saved_search_id) <= 128),
    CHECK (length(search_text) BETWEEN 1 AND 65536),
    CHECK (finished_at_unix_micro >= created_at_unix_micro)
) STRICT;
CREATE TABLE search_history_pending (
    search_job_id TEXT PRIMARY KEY NOT NULL COLLATE BINARY,
    tenant_id TEXT NOT NULL COLLATE BINARY,
    owner_id TEXT NOT NULL COLLATE BINARY,
    state INTEGER NOT NULL CHECK (state BETWEEN 1 AND 5),
    created_at_unix_micro INTEGER NOT NULL,
    entry_proto BLOB NOT NULL CHECK (length(entry_proto) BETWEEN 1 AND 524288),
    entry_sha256 BLOB NOT NULL CHECK (length(entry_sha256) = 32),
    CHECK (length(search_job_id) BETWEEN 1 AND 256),
    CHECK (length(tenant_id) BETWEEN 1 AND 1024),
    CHECK (length(owner_id) BETWEEN 1 AND 255)
) STRICT;
CREATE TABLE app_workspaces (
    app_id TEXT PRIMARY KEY NOT NULL COLLATE BINARY,
    tenant_id TEXT NOT NULL COLLATE BINARY,
    version INTEGER NOT NULL CHECK (version >= 1),
    slug TEXT NOT NULL COLLATE BINARY,
    display_name TEXT NOT NULL COLLATE BINARY,
    description TEXT NOT NULL DEFAULT '',
    default_time_range_present INTEGER NOT NULL
        CHECK (default_time_range_present IN (0, 1)),
    default_earliest TEXT,
    default_latest TEXT,
    default_timezone TEXT,
    state TEXT NOT NULL CHECK (state IN ('active', 'archived')),
    created_at_unix_micro INTEGER NOT NULL CHECK (created_at_unix_micro > 0),
    updated_at_unix_micro INTEGER NOT NULL CHECK (updated_at_unix_micro > 0),
    archived_at_unix_micro INTEGER,
    CHECK (length(app_id) = 26),
    CHECK (substr(app_id, 1, 4) = 'app_'),
    CHECK (substr(app_id, 5) NOT GLOB '*[^A-Za-z0-9_-]*'),
    CHECK (substr(app_id, 26, 1) GLOB '[AQgw]'),
    CHECK (length(tenant_id) BETWEEN 1 AND 255),
    CHECK (length(slug) BETWEEN 1 AND 128),
    CHECK (slug = lower(slug)),
    CHECK (slug NOT GLOB '*[^a-z0-9_-]*'),
    CHECK (substr(slug, 1, 1) GLOB '[a-z0-9]'),
    CHECK (length(display_name) BETWEEN 1 AND 255),
    CHECK (length(description) <= 16384),
    CHECK (default_earliest IS NULL OR length(default_earliest) BETWEEN 1 AND 1024),
    CHECK (default_latest IS NULL OR length(default_latest) BETWEEN 1 AND 1024),
    CHECK (default_timezone IS NULL OR length(default_timezone) BETWEEN 1 AND 255),
    CHECK (
        default_time_range_present = 1
        OR (
            default_earliest IS NULL
            AND default_latest IS NULL
            AND default_timezone IS NULL
        )
    ),
    CHECK (updated_at_unix_micro >= created_at_unix_micro),
    CHECK (
        (state = 'active' AND archived_at_unix_micro IS NULL)
        OR
        (
            state = 'archived'
            AND archived_at_unix_micro IS NOT NULL
            AND archived_at_unix_micro BETWEEN created_at_unix_micro AND updated_at_unix_micro
        )
    ),
    UNIQUE (tenant_id, slug),
    UNIQUE (tenant_id, app_id)
) STRICT;
CREATE TABLE app_catalog_revisions (
    tenant_id TEXT PRIMARY KEY NOT NULL COLLATE BINARY,
    revision INTEGER NOT NULL CHECK (revision >= 1),
    CHECK (length(tenant_id) BETWEEN 1 AND 255)
) STRICT, WITHOUT ROWID;
CREATE TABLE app_default_indexes (
    tenant_id TEXT NOT NULL COLLATE BINARY,
    app_id TEXT NOT NULL COLLATE BINARY,
    index_id TEXT NOT NULL COLLATE BINARY,
    PRIMARY KEY (tenant_id, app_id, index_id),
    FOREIGN KEY (tenant_id, app_id)
        REFERENCES app_workspaces (tenant_id, app_id) ON DELETE CASCADE,
    FOREIGN KEY (index_id)
        REFERENCES indexes (index_id) ON DELETE RESTRICT
) STRICT, WITHOUT ROWID;
CREATE TABLE collector_fleet (
    tenant_id TEXT NOT NULL COLLATE BINARY,
    collector_id TEXT NOT NULL COLLATE BINARY,
    admin_version INTEGER NOT NULL
        CONSTRAINT collector_fleet_admin_version_positive
        CHECK (admin_version >= 1),
    display_name TEXT COLLATE BINARY,
    -- Persisted display names are non-empty, so the empty string is an
    -- unambiguous, non-null key that preserves SQLite's NULL-first ascending
    -- and NULL-last descending ordering while keeping keyset seeks indexable.
    display_name_sort_key TEXT NOT NULL COLLATE BINARY
        GENERATED ALWAYS AS (coalesce(display_name, '')) STORED,
    administrative_state TEXT NOT NULL COLLATE BINARY
        CONSTRAINT collector_fleet_administrative_state_valid
        CHECK (administrative_state IN ('enabled', 'disabled')),
    first_seen_at_unix_micro INTEGER NOT NULL
        CONSTRAINT collector_fleet_first_seen_positive
        CHECK (first_seen_at_unix_micro BETWEEN 1 AND 253402300799999999),
    updated_at_unix_micro INTEGER NOT NULL
        CONSTRAINT collector_fleet_updated_at_valid
        CHECK (
            updated_at_unix_micro BETWEEN
                first_seen_at_unix_micro AND 253402300799999999
        ),
    CONSTRAINT collector_fleet_tenant_id_bounded
        CHECK (
            length(CAST(tenant_id AS BLOB)) BETWEEN 1 AND 255
            AND instr(tenant_id, char(0)) = 0
        ),
    CONSTRAINT collector_fleet_collector_id_canonical
        CHECK (
            length(collector_id) BETWEEN 1 AND 128
            AND instr(collector_id, char(0)) = 0
            AND substr(collector_id, 1, 1) GLOB '[A-Za-z0-9]'
            AND collector_id NOT GLOB '*[^A-Za-z0-9._:-]*'
        ),
    CONSTRAINT collector_fleet_display_name_bounded
        CHECK (
            display_name IS NULL
            OR (
                length(CAST(display_name AS BLOB)) BETWEEN 1 AND 255
                AND instr(display_name, char(0)) = 0
            )
        ),
    PRIMARY KEY (tenant_id, collector_id)
) STRICT, WITHOUT ROWID;
CREATE TABLE collector_catalog_revisions (
    tenant_id TEXT PRIMARY KEY NOT NULL COLLATE BINARY,
    revision INTEGER NOT NULL
        CONSTRAINT collector_catalog_revisions_revision_positive
        CHECK (revision >= 1),
    fleet_count INTEGER NOT NULL
        CONSTRAINT collector_catalog_revisions_fleet_count_bounded
        CHECK (fleet_count BETWEEN 0 AND 256),
    runtime_count INTEGER NOT NULL
        CONSTRAINT collector_catalog_revisions_runtime_count_bounded
        CHECK (runtime_count BETWEEN 0 AND 256),
    CONSTRAINT collector_catalog_revisions_tenant_id_bounded
        CHECK (
            length(CAST(tenant_id AS BLOB)) BETWEEN 1 AND 255
            AND instr(tenant_id, char(0)) = 0
        )
) STRICT, WITHOUT ROWID;
CREATE TABLE collector_runtime (
    tenant_id TEXT NOT NULL COLLATE BINARY,
    collector_id TEXT NOT NULL COLLATE BINARY,
    telemetry_revision INTEGER NOT NULL
        CONSTRAINT collector_runtime_telemetry_revision_positive
        CHECK (telemetry_revision >= 1),
    lease_generation INTEGER NOT NULL
        CONSTRAINT collector_runtime_lease_generation_positive
        CHECK (lease_generation >= 1),
    boot_epoch TEXT COLLATE BINARY,
    stream_id TEXT COLLATE BINARY,
    active_instance_id TEXT COLLATE BINARY,
    source_revision TEXT NOT NULL COLLATE BINARY,
    hostname TEXT NOT NULL COLLATE BINARY,
    operating_system TEXT NOT NULL COLLATE BINARY,
    architecture TEXT NOT NULL COLLATE BINARY,
    started_at_unix_micro INTEGER NOT NULL
        CONSTRAINT collector_runtime_started_at_positive
        CHECK (started_at_unix_micro BETWEEN 1 AND 253402300799999999),
    connected_at_unix_micro INTEGER NOT NULL
        CONSTRAINT collector_runtime_connected_at_positive
        CHECK (connected_at_unix_micro BETWEEN 1 AND 253402300799999999),
    last_seen_at_unix_micro INTEGER NOT NULL
        CONSTRAINT collector_runtime_last_seen_valid
        CHECK (
            last_seen_at_unix_micro BETWEEN
                connected_at_unix_micro AND 253402300799999999
        ),
    disconnected_at_unix_micro INTEGER,
    observation_sequence INTEGER NOT NULL DEFAULT 0
        CONSTRAINT collector_runtime_observation_sequence_nonnegative
        CHECK (observation_sequence >= 0),
    observed_at_unix_micro INTEGER,
    last_acked_at_hello_sequence INTEGER,
    queued_events INTEGER NOT NULL DEFAULT 0
        CONSTRAINT collector_runtime_queued_events_nonnegative
        CHECK (queued_events >= 0),
    queued_bytes INTEGER NOT NULL DEFAULT 0
        CONSTRAINT collector_runtime_queued_bytes_nonnegative
        CHECK (queued_bytes >= 0),
    oldest_event_age_nanoseconds INTEGER,
    sent_events_total INTEGER NOT NULL DEFAULT 0
        CONSTRAINT collector_runtime_sent_events_nonnegative
        CHECK (sent_events_total >= 0),
    acknowledged_events_total INTEGER NOT NULL DEFAULT 0
        CONSTRAINT collector_runtime_acknowledged_events_nonnegative
        CHECK (acknowledged_events_total >= 0),
    retried_batches_total INTEGER NOT NULL DEFAULT 0
        CONSTRAINT collector_runtime_retried_batches_nonnegative
        CHECK (retried_batches_total >= 0),
    rejected_events_total INTEGER NOT NULL DEFAULT 0
        CONSTRAINT collector_runtime_rejected_events_nonnegative
        CHECK (rejected_events_total >= 0),
    dropped_events_total INTEGER NOT NULL DEFAULT 0
        CONSTRAINT collector_runtime_dropped_events_nonnegative
        CHECK (dropped_events_total >= 0),
    last_sent_batch_sequence INTEGER,
    last_acknowledged_batch_sequence INTEGER,
    process_resident_memory_bytes INTEGER NOT NULL DEFAULT 0
        CONSTRAINT collector_runtime_memory_nonnegative
        CHECK (process_resident_memory_bytes >= 0),
    process_cpu_percent REAL NOT NULL DEFAULT 0
        CONSTRAINT collector_runtime_cpu_valid
        CHECK (
            process_cpu_percent = process_cpu_percent
            AND process_cpu_percent BETWEEN 0 AND 1000000
        ),
    CONSTRAINT collector_runtime_boot_epoch_canonical
        CHECK (
            boot_epoch IS NULL
            OR (
                length(boot_epoch) BETWEEN 1 AND 128
                AND instr(boot_epoch, char(0)) = 0
                AND substr(boot_epoch, 1, 1) GLOB '[A-Za-z0-9]'
                AND boot_epoch NOT GLOB '*[^A-Za-z0-9._:-]*'
            )
        ),
    CONSTRAINT collector_runtime_stream_id_canonical
        CHECK (
            stream_id IS NULL
            OR (
                length(stream_id) BETWEEN 1 AND 128
                AND instr(stream_id, char(0)) = 0
                AND substr(stream_id, 1, 1) GLOB '[A-Za-z0-9]'
                AND stream_id NOT GLOB '*[^A-Za-z0-9._:-]*'
            )
        ),
    CONSTRAINT collector_runtime_instance_id_canonical
        CHECK (
            active_instance_id IS NULL
            OR (
                length(active_instance_id) BETWEEN 1 AND 128
                AND instr(active_instance_id, char(0)) = 0
                AND substr(active_instance_id, 1, 1) GLOB '[A-Za-z0-9]'
                AND active_instance_id NOT GLOB '*[^A-Za-z0-9._:-]*'
            )
        ),
    CONSTRAINT collector_runtime_source_revision_valid
        CHECK (
            source_revision = 'development'
            OR (
                length(CAST(source_revision AS BLOB)) IN (40, 64)
                AND source_revision NOT GLOB '*[^0-9a-f]*'
            )
        ),
    CONSTRAINT collector_runtime_metadata_bounded
        CHECK (
            length(CAST(hostname AS BLOB)) <= 255
            AND instr(hostname, char(0)) = 0
            AND length(CAST(operating_system AS BLOB)) <= 128
            AND instr(operating_system, char(0)) = 0
            AND length(CAST(architecture AS BLOB)) <= 128
            AND instr(architecture, char(0)) = 0
        ),
    CONSTRAINT collector_runtime_disconnect_valid
        CHECK (
            disconnected_at_unix_micro IS NULL
            OR disconnected_at_unix_micro BETWEEN
                last_seen_at_unix_micro AND 253402300799999999
        ),
    CONSTRAINT collector_runtime_observation_snapshot_valid
        CHECK (
            (
                observation_sequence = 0
                AND observed_at_unix_micro IS NULL
            )
            OR
            (
                observation_sequence > 0
                AND observed_at_unix_micro IS NOT NULL
                AND observed_at_unix_micro
                    BETWEEN 1 AND 253402300799999999
            )
        ),
    CONSTRAINT collector_runtime_hello_sequence_valid
        CHECK (
            last_acked_at_hello_sequence IS NULL
            OR last_acked_at_hello_sequence >= 0
        ),
    CONSTRAINT collector_runtime_oldest_age_valid
        CHECK (
            oldest_event_age_nanoseconds IS NULL
            OR oldest_event_age_nanoseconds >= 0
        ),
    CONSTRAINT collector_runtime_last_sent_sequence_valid
        CHECK (
            last_sent_batch_sequence IS NULL
            OR last_sent_batch_sequence >= 0
        ),
    CONSTRAINT collector_runtime_last_acknowledged_sequence_valid
        CHECK (
            last_acknowledged_batch_sequence IS NULL
            OR last_acknowledged_batch_sequence >= 0
        ),
    CONSTRAINT collector_runtime_active_lease_consistent
        CHECK (
            (
                boot_epoch IS NOT NULL
                AND stream_id IS NOT NULL
                AND active_instance_id IS NOT NULL
                AND disconnected_at_unix_micro IS NULL
            )
            OR
            (
                boot_epoch IS NULL
                AND stream_id IS NULL
                AND active_instance_id IS NULL
                AND disconnected_at_unix_micro IS NOT NULL
            )
        ),
    PRIMARY KEY (tenant_id, collector_id),
    FOREIGN KEY (tenant_id, collector_id)
        REFERENCES collector_fleet (tenant_id, collector_id) ON DELETE CASCADE
) STRICT, WITHOUT ROWID;
CREATE TABLE collector_capabilities (
    tenant_id TEXT NOT NULL COLLATE BINARY,
    collector_id TEXT NOT NULL COLLATE BINARY,
    capability INTEGER NOT NULL
        CONSTRAINT collector_capabilities_value_valid
        CHECK (capability BETWEEN 1 AND 2147483647),
    PRIMARY KEY (tenant_id, collector_id, capability),
    FOREIGN KEY (tenant_id, collector_id)
        REFERENCES collector_fleet (tenant_id, collector_id) ON DELETE CASCADE
) STRICT, WITHOUT ROWID;
CREATE TABLE collector_authorized_indexes (
    tenant_id TEXT NOT NULL COLLATE BINARY,
    collector_id TEXT NOT NULL COLLATE BINARY,
    index_name TEXT NOT NULL COLLATE BINARY,
    CONSTRAINT collector_authorized_indexes_name_canonical
        CHECK (
            length(index_name) BETWEEN 1 AND 255
            AND index_name = lower(index_name)
            AND index_name NOT GLOB '*[^a-z0-9_-]*'
            AND substr(index_name, 1, 1) GLOB '[a-z0-9]'
            AND instr(index_name, 'kvstore') = 0
        ),
    PRIMARY KEY (tenant_id, collector_id, index_name),
    FOREIGN KEY (tenant_id, collector_id)
        REFERENCES collector_fleet (tenant_id, collector_id) ON DELETE CASCADE
) STRICT, WITHOUT ROWID;
CREATE TABLE collector_inputs (
    tenant_id TEXT NOT NULL COLLATE BINARY,
    collector_id TEXT NOT NULL COLLATE BINARY,
    input_id TEXT NOT NULL COLLATE BINARY,
    input_type INTEGER NOT NULL
        CONSTRAINT collector_inputs_type_valid
        CHECK (input_type BETWEEN 1 AND 2147483647),
    index_name TEXT NOT NULL COLLATE BINARY,
    source TEXT COLLATE BINARY,
    sourcetype TEXT COLLATE BINARY,
    CONSTRAINT collector_inputs_id_canonical
        CHECK (
            length(input_id) BETWEEN 1 AND 128
            AND instr(input_id, char(0)) = 0
            AND substr(input_id, 1, 1) GLOB '[A-Za-z0-9]'
            AND input_id NOT GLOB '*[^A-Za-z0-9._:-]*'
        ),
    CONSTRAINT collector_inputs_index_name_canonical
        CHECK (
            length(index_name) BETWEEN 1 AND 255
            AND index_name = lower(index_name)
            AND index_name NOT GLOB '*[^a-z0-9_-]*'
            AND substr(index_name, 1, 1) GLOB '[a-z0-9]'
            AND instr(index_name, 'kvstore') = 0
        ),
    CONSTRAINT collector_inputs_source_bounded
        CHECK (
            source IS NULL
            OR (
                length(CAST(source AS BLOB)) BETWEEN 1 AND 4096
                AND instr(source, char(0)) = 0
            )
        ),
    CONSTRAINT collector_inputs_sourcetype_bounded
        CHECK (
            sourcetype IS NULL
            OR (
                length(CAST(sourcetype AS BLOB)) BETWEEN 1 AND 255
                AND instr(sourcetype, char(0)) = 0
            )
        ),
    PRIMARY KEY (tenant_id, collector_id, input_id),
    FOREIGN KEY (tenant_id, collector_id)
        REFERENCES collector_fleet (tenant_id, collector_id) ON DELETE CASCADE,
    FOREIGN KEY (tenant_id, collector_id, index_name)
        REFERENCES collector_authorized_indexes (tenant_id, collector_id, index_name)
        ON DELETE CASCADE
) STRICT, WITHOUT ROWID;
CREATE TABLE collector_input_health (
    tenant_id TEXT NOT NULL COLLATE BINARY,
    collector_id TEXT NOT NULL COLLATE BINARY,
    input_id TEXT NOT NULL COLLATE BINARY,
    state INTEGER NOT NULL
        CONSTRAINT collector_input_health_state_valid
        CHECK (state BETWEEN 1 AND 2147483647),
    status_message TEXT NOT NULL COLLATE BINARY,
    discovered_sources INTEGER NOT NULL
        CONSTRAINT collector_input_health_discovered_nonnegative
        CHECK (discovered_sources >= 0),
    active_sources INTEGER NOT NULL
        CONSTRAINT collector_input_health_active_valid
        CHECK (active_sources BETWEEN 0 AND discovered_sources),
    events_read_total INTEGER NOT NULL
        CONSTRAINT collector_input_health_events_nonnegative
        CHECK (events_read_total >= 0),
    bytes_read_total INTEGER NOT NULL
        CONSTRAINT collector_input_health_bytes_nonnegative
        CHECK (bytes_read_total >= 0),
    last_event_at_unix_micro INTEGER,
    last_error_at_unix_micro INTEGER,
    CONSTRAINT collector_input_health_message_bounded
        CHECK (
            length(CAST(status_message AS BLOB)) <= 8192
            AND instr(status_message, char(0)) = 0
        ),
    CONSTRAINT collector_input_health_last_event_valid
        CHECK (
            last_event_at_unix_micro IS NULL
            OR last_event_at_unix_micro
                BETWEEN 1 AND 253402300799999999
        ),
    CONSTRAINT collector_input_health_last_error_valid
        CHECK (
            last_error_at_unix_micro IS NULL
            OR last_error_at_unix_micro
                BETWEEN 1 AND 253402300799999999
        ),
    PRIMARY KEY (tenant_id, collector_id, input_id),
    FOREIGN KEY (tenant_id, collector_id, input_id)
        REFERENCES collector_inputs (tenant_id, collector_id, input_id)
        ON DELETE CASCADE
) STRICT, WITHOUT ROWID;
CREATE TABLE search_history_owner_counts (
    tenant_id TEXT NOT NULL COLLATE BINARY,
    owner_id TEXT NOT NULL COLLATE BINARY,
    terminal_count INTEGER NOT NULL CHECK (terminal_count > 0),
    PRIMARY KEY (tenant_id, owner_id),
    CHECK (length(tenant_id) BETWEEN 1 AND 1024),
    CHECK (length(owner_id) BETWEEN 1 AND 255)
) STRICT;
CREATE TABLE index_deletion_tombstones (
    index_id TEXT PRIMARY KEY NOT NULL
        REFERENCES indexes (index_id) ON DELETE RESTRICT,
    name TEXT NOT NULL COLLATE BINARY,
    deleted_version INTEGER NOT NULL CHECK (deleted_version >= 1),
    deleted_at_unix_micro INTEGER NOT NULL CHECK (deleted_at_unix_micro > 0)
) STRICT;
CREATE TABLE index_deletion_operations (
    deletion_operation_id TEXT PRIMARY KEY NOT NULL,
    index_id TEXT NOT NULL UNIQUE
        REFERENCES indexes (index_id) ON DELETE RESTRICT,
    index_name TEXT NOT NULL COLLATE BINARY,
    tenant_id TEXT NOT NULL COLLATE BINARY,
    archived_index_version INTEGER NOT NULL
        CHECK (
            archived_index_version >= 1
            AND archived_index_version < 9223372036854775807
        ),
    created_at_unix_micro INTEGER NOT NULL
        CHECK (created_at_unix_micro > 0),
    CHECK (
        length(CAST(deletion_operation_id AS BLOB)) BETWEEN 1 AND 128
        AND instr(
            CAST(deletion_operation_id AS BLOB),
            X'00'
        ) = 0
        AND substr(deletion_operation_id, 1, 1) GLOB '[A-Za-z0-9]'
        AND deletion_operation_id NOT GLOB '*[^A-Za-z0-9._:-]*'
    ),
    CONSTRAINT index_deletion_operations_tenant_id_bounded
        CHECK (
            length(CAST(tenant_id AS BLOB)) BETWEEN 1 AND 255
            AND instr(CAST(tenant_id AS BLOB), X'00') = 0
            AND tenant_id = trim(tenant_id)
            AND tenant_id NOT GLOB (
                '*['
                || char(1)
                || '-'
                || char(31)
                || char(127)
                || '-'
                || char(159)
                || ']*'
            )
        )
) STRICT;
CREATE TABLE index_deletion_mutation_attempts (
    deletion_operation_id TEXT PRIMARY KEY NOT NULL
        REFERENCES index_deletion_operations (
            deletion_operation_id
        ) ON DELETE CASCADE,
    correlation_id TEXT NOT NULL UNIQUE COLLATE BINARY,
    tenant_id TEXT NOT NULL COLLATE BINARY,
    clickhouse_database TEXT NOT NULL COLLATE BINARY,
    clickhouse_table TEXT NOT NULL COLLATE BINARY,
    clickhouse_table_uuid TEXT NOT NULL COLLATE BINARY,
    protocol_version INTEGER NOT NULL,
    created_at_unix_micro INTEGER NOT NULL,
    CONSTRAINT index_deletion_mutation_attempts_correlation_id_canonical
        CHECK (
            length(CAST(correlation_id AS BLOB)) BETWEEN 1 AND 128
            AND instr(CAST(correlation_id AS BLOB), X'00') = 0
            AND substr(correlation_id, 1, 1) GLOB '[A-Za-z0-9]'
            AND correlation_id NOT GLOB '*[^A-Za-z0-9._:-]*'
        ),
    CONSTRAINT index_deletion_mutation_attempts_tenant_id_bounded
        CHECK (
            length(CAST(tenant_id AS BLOB)) BETWEEN 1 AND 255
            AND instr(CAST(tenant_id AS BLOB), X'00') = 0
        ),
    CONSTRAINT index_deletion_mutation_attempts_database_canonical
        CHECK (
            length(CAST(clickhouse_database AS BLOB)) BETWEEN 1 AND 255
            AND instr(CAST(clickhouse_database AS BLOB), X'00') = 0
            AND substr(clickhouse_database, 1, 1) GLOB '[A-Za-z_]'
            AND clickhouse_database NOT GLOB '*[^A-Za-z0-9_]*'
        ),
    CONSTRAINT index_deletion_mutation_attempts_table_canonical
        CHECK (
            length(CAST(clickhouse_table AS BLOB)) BETWEEN 1 AND 255
            AND instr(CAST(clickhouse_table AS BLOB), X'00') = 0
            AND substr(clickhouse_table, 1, 1) GLOB '[A-Za-z_]'
            AND clickhouse_table NOT GLOB '*[^A-Za-z0-9_]*'
        ),
    CONSTRAINT index_deletion_mutation_attempts_table_uuid_canonical
        CHECK (
            length(CAST(clickhouse_table_uuid AS BLOB)) = 36
            AND instr(CAST(clickhouse_table_uuid AS BLOB), X'00') = 0
            AND clickhouse_table_uuid = lower(clickhouse_table_uuid)
            AND substr(clickhouse_table_uuid, 9, 1) = '-'
            AND substr(clickhouse_table_uuid, 14, 1) = '-'
            AND substr(clickhouse_table_uuid, 19, 1) = '-'
            AND substr(clickhouse_table_uuid, 24, 1) = '-'
            AND length(replace(clickhouse_table_uuid, '-', '')) = 32
            AND replace(clickhouse_table_uuid, '-', '')
                NOT GLOB '*[^0-9a-f]*'
            AND clickhouse_table_uuid <>
                '00000000-0000-0000-0000-000000000000'
        ),
    CONSTRAINT index_deletion_mutation_attempts_protocol_supported
        CHECK (protocol_version = 1),
    CONSTRAINT index_deletion_mutation_attempts_created_at_positive
        CHECK (
            created_at_unix_micro BETWEEN 1 AND 253402300799999999
        )
) STRICT;
CREATE TABLE index_data_deletion_completions (
    deletion_operation_id TEXT PRIMARY KEY NOT NULL,
    correlation_id TEXT NOT NULL UNIQUE COLLATE BINARY,
    index_id TEXT NOT NULL UNIQUE
        REFERENCES indexes (index_id) ON DELETE RESTRICT,
    index_name TEXT NOT NULL COLLATE BINARY,
    archived_index_version INTEGER NOT NULL,
    deleting_index_version INTEGER NOT NULL,
    tenant_id TEXT NOT NULL COLLATE BINARY,
    clickhouse_database TEXT NOT NULL COLLATE BINARY,
    clickhouse_table TEXT NOT NULL COLLATE BINARY,
    clickhouse_table_uuid TEXT NOT NULL COLLATE BINARY,
    protocol_version INTEGER NOT NULL,
    operation_created_at_unix_micro INTEGER NOT NULL,
    attempt_created_at_unix_micro INTEGER NOT NULL,
    completed_at_unix_micro INTEGER NOT NULL,
    CONSTRAINT index_data_deletion_completions_operation_id_canonical
        CHECK (
            length(CAST(deletion_operation_id AS BLOB)) BETWEEN 1 AND 128
            AND instr(CAST(deletion_operation_id AS BLOB), X'00') = 0
            AND substr(deletion_operation_id, 1, 1) GLOB '[A-Za-z0-9]'
            AND deletion_operation_id NOT GLOB '*[^A-Za-z0-9._:-]*'
        ),
    CONSTRAINT index_data_deletion_completions_correlation_id_canonical
        CHECK (
            length(CAST(correlation_id AS BLOB)) BETWEEN 1 AND 128
            AND instr(CAST(correlation_id AS BLOB), X'00') = 0
            AND substr(correlation_id, 1, 1) GLOB '[A-Za-z0-9]'
            AND correlation_id NOT GLOB '*[^A-Za-z0-9._:-]*'
        ),
    CONSTRAINT index_data_deletion_completions_index_name_canonical
        CHECK (
            length(CAST(index_name AS BLOB)) BETWEEN 1 AND 255
            AND instr(CAST(index_name AS BLOB), X'00') = 0
            AND index_name = lower(index_name)
            AND index_name NOT GLOB '*[^a-z0-9_-]*'
            AND substr(index_name, 1, 1) GLOB '[a-z0-9]'
            AND instr(index_name, 'kvstore') = 0
        ),
    CONSTRAINT index_data_deletion_completions_versions_supported
        CHECK (
            archived_index_version >= 1
            AND archived_index_version < 9223372036854775807
            AND deleting_index_version = archived_index_version + 1
        ),
    CONSTRAINT index_data_deletion_completions_tenant_id_bounded
        CHECK (
            length(CAST(tenant_id AS BLOB)) BETWEEN 1 AND 255
            AND instr(CAST(tenant_id AS BLOB), X'00') = 0
        ),
    CONSTRAINT index_data_deletion_completions_database_canonical
        CHECK (
            length(CAST(clickhouse_database AS BLOB)) BETWEEN 1 AND 255
            AND instr(CAST(clickhouse_database AS BLOB), X'00') = 0
            AND substr(clickhouse_database, 1, 1) GLOB '[A-Za-z_]'
            AND clickhouse_database NOT GLOB '*[^A-Za-z0-9_]*'
        ),
    CONSTRAINT index_data_deletion_completions_table_canonical
        CHECK (
            length(CAST(clickhouse_table AS BLOB)) BETWEEN 1 AND 255
            AND instr(CAST(clickhouse_table AS BLOB), X'00') = 0
            AND substr(clickhouse_table, 1, 1) GLOB '[A-Za-z_]'
            AND clickhouse_table NOT GLOB '*[^A-Za-z0-9_]*'
        ),
    CONSTRAINT index_data_deletion_completions_table_uuid_canonical
        CHECK (
            length(CAST(clickhouse_table_uuid AS BLOB)) = 36
            AND instr(CAST(clickhouse_table_uuid AS BLOB), X'00') = 0
            AND clickhouse_table_uuid = lower(clickhouse_table_uuid)
            AND substr(clickhouse_table_uuid, 9, 1) = '-'
            AND substr(clickhouse_table_uuid, 14, 1) = '-'
            AND substr(clickhouse_table_uuid, 19, 1) = '-'
            AND substr(clickhouse_table_uuid, 24, 1) = '-'
            AND length(replace(clickhouse_table_uuid, '-', '')) = 32
            AND replace(clickhouse_table_uuid, '-', '')
                NOT GLOB '*[^0-9a-f]*'
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
        )
) STRICT;
CREATE TABLE index_catalog_state (
    singleton_id INTEGER PRIMARY KEY NOT NULL
        CHECK (singleton_id = 1),
    revision INTEGER NOT NULL
        CHECK (revision BETWEEN 1 AND 9223372036854775807),
    physical_count INTEGER NOT NULL
        CHECK (physical_count BETWEEN 0 AND 1024)
) STRICT, WITHOUT ROWID;
INSERT INTO index_catalog_state VALUES(1,1,0);
CREATE TABLE ingest_quota_buckets (
    tenant_id TEXT NOT NULL COLLATE BINARY,
    scope_kind TEXT NOT NULL COLLATE BINARY,
    scope_id TEXT NOT NULL COLLATE BINARY,
    max_ingest_events_per_second INTEGER NOT NULL,
    max_ingest_uncompressed_bytes_per_second INTEGER NOT NULL,
    next_event_admission_unix_nano INTEGER NOT NULL,
    next_byte_admission_unix_nano INTEGER NOT NULL,
    updated_at_unix_micro INTEGER NOT NULL,
    -- Token scopes duplicate their scope ID here solely to obtain a real
    -- cascading FK. Index scopes leave it NULL; index identities are already
    -- bounded and retained by the catalog's deletion-tombstone contract.
    token_owner_id TEXT COLLATE BINARY,
    PRIMARY KEY (tenant_id, scope_kind, scope_id),
    CONSTRAINT ingest_quota_buckets_tenant_id_bounded CHECK (
        length(CAST(tenant_id AS BLOB)) BETWEEN 1 AND 255
        AND instr(CAST(tenant_id AS BLOB), X'00') = 0
    ),
    CONSTRAINT ingest_quota_buckets_scope_kind_supported CHECK (
        scope_kind IN ('token', 'index')
    ),
    CONSTRAINT ingest_quota_buckets_scope_id_bounded CHECK (
        length(CAST(scope_id AS BLOB)) BETWEEN 1 AND 255
        AND instr(CAST(scope_id AS BLOB), X'00') = 0
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
    FOREIGN KEY (token_owner_id) REFERENCES ingestion_tokens (ingestion_token_id)
        ON UPDATE RESTRICT ON DELETE CASCADE
) STRICT, WITHOUT ROWID;
CREATE TABLE ingest_quota_admissions (
    batch_key TEXT PRIMARY KEY NOT NULL COLLATE BINARY,
    admitted_at_unix_micro INTEGER NOT NULL,
    event_count INTEGER NOT NULL,
    uncompressed_bytes INTEGER NOT NULL,
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
) STRICT, WITHOUT ROWID;
CREATE TABLE audit_tenant_state (
    tenant_id TEXT PRIMARY KEY NOT NULL COLLATE BINARY,
    next_sequence INTEGER NOT NULL
        CHECK (next_sequence BETWEEN 1 AND 100001),
    event_count INTEGER NOT NULL
        CHECK (event_count BETWEEN 0 AND 100000),
    CONSTRAINT audit_tenant_state_sequence_matches_count CHECK (
        next_sequence = event_count + 1
    ),
    CONSTRAINT audit_tenant_state_tenant_id_bounded CHECK (
        length(CAST(tenant_id AS BLOB)) BETWEEN 1 AND 255
        AND instr(CAST(tenant_id AS BLOB), X'00') = 0
        AND tenant_id = trim(tenant_id)
        AND tenant_id NOT GLOB (
            '*[' || char(1) || '-' || char(31)
            || char(127) || '-' || char(159) || ']*'
        )
    )
) STRICT, WITHOUT ROWID;
CREATE TABLE search_attempt_audit_tenant_state (
    tenant_id TEXT PRIMARY KEY NOT NULL COLLATE BINARY,
    first_sequence INTEGER NOT NULL CHECK (
        first_sequence BETWEEN 1 AND 9223372036854775807
    ),
    next_sequence INTEGER NOT NULL CHECK (
        next_sequence BETWEEN 1 AND 9223372036854775807
    ),
    retained_count INTEGER NOT NULL CHECK (
        retained_count BETWEEN 0 AND 100001
    ),
    maximum_retained_attempts INTEGER NOT NULL CHECK (
        maximum_retained_attempts BETWEEN 1 AND 100000
    ),
    CONSTRAINT search_attempt_audit_state_dense CHECK (
        next_sequence >= first_sequence
        AND next_sequence - first_sequence = retained_count
    ),
    -- maximum + 1 exists only transiently inside the append trigger, immediately
    -- before that trigger removes the oldest row.
    CONSTRAINT search_attempt_audit_state_bounded CHECK (
        retained_count <= maximum_retained_attempts + 1
    ),
    CONSTRAINT search_attempt_audit_state_tenant_id_bounded CHECK (
        length(CAST(tenant_id AS BLOB)) BETWEEN 1 AND 255
        AND instr(CAST(tenant_id AS BLOB), X'00') = 0
        AND tenant_id = trim(tenant_id)
        AND tenant_id NOT GLOB (
            '*[' || char(1) || '-' || char(31)
            || char(127) || '-' || char(159) || ']*'
        )
    )
) STRICT, WITHOUT ROWID;
CREATE TABLE search_attempt_audit_events (
    tenant_id TEXT NOT NULL COLLATE BINARY,
    sequence INTEGER NOT NULL CHECK (
        sequence BETWEEN 1 AND 9223372036854775806
    ),
    occurred_at_unix_micro INTEGER NOT NULL CHECK (
        occurred_at_unix_micro BETWEEN 1 AND 253402300799999999
    ),
    actor_kind TEXT NOT NULL COLLATE BINARY CHECK (
        actor_kind IN ('system', 'browser')
    ),
    actor_id TEXT NOT NULL COLLATE BINARY,
    actor_role TEXT NOT NULL COLLATE BINARY CHECK (
        actor_role IN ('system', 'user', 'administrator')
    ),
    owner_id TEXT NOT NULL COLLATE BINARY,
    search_job_id TEXT NOT NULL COLLATE BINARY, knowledge_snapshot_sha256 BLOB CHECK (
    knowledge_snapshot_sha256 IS NULL
    OR (
        typeof(knowledge_snapshot_sha256) = 'blob'
        AND length(knowledge_snapshot_sha256) = 32
    )
), knowledge_snapshot_tenant_catalog_revision INTEGER CHECK (
    knowledge_snapshot_tenant_catalog_revision IS NULL
    OR knowledge_snapshot_tenant_catalog_revision
        BETWEEN 0 AND 9223372036854775806
), knowledge_snapshot_tenant_catalog_state_token BLOB CHECK (
    knowledge_snapshot_tenant_catalog_state_token IS NULL
    OR (
        typeof(knowledge_snapshot_tenant_catalog_state_token) = 'blob'
        AND length(knowledge_snapshot_tenant_catalog_state_token) = 32
    )
), knowledge_snapshot_object_count INTEGER CHECK (
    knowledge_snapshot_object_count IS NULL
    OR knowledge_snapshot_object_count BETWEEN 0 AND 256
), knowledge_snapshot_lookup_asset_count INTEGER CHECK (
    knowledge_snapshot_lookup_asset_count IS NULL
    OR knowledge_snapshot_lookup_asset_count BETWEEN 0 AND 16
),
    PRIMARY KEY (tenant_id, sequence),
    -- Search history publishes only a newly admitted job. This uniqueness is
    -- intentionally scoped to the retained rolling window: once the oldest
    -- audit row is pruned, its journal-only identity is no longer retained.
    UNIQUE (tenant_id, search_job_id),
    CONSTRAINT search_attempt_audit_actor_id_bounded CHECK (
        length(CAST(actor_id AS BLOB)) BETWEEN 1 AND 255
        AND instr(CAST(actor_id AS BLOB), X'00') = 0
        AND actor_id = trim(actor_id)
        AND actor_id NOT GLOB (
            '*[' || char(1) || '-' || char(31)
            || char(127) || '-' || char(159) || ']*'
        )
    ),
    CONSTRAINT search_attempt_audit_actor_shape_supported CHECK (
        (actor_kind = 'system' AND actor_role = 'system')
        OR (
            actor_kind = 'browser'
            AND actor_role IN ('user', 'administrator')
        )
    ),
    CONSTRAINT search_attempt_audit_owner_id_bounded CHECK (
        length(CAST(owner_id AS BLOB)) BETWEEN 1 AND 255
        AND instr(CAST(owner_id AS BLOB), X'00') = 0
        AND owner_id = trim(owner_id)
        AND owner_id NOT GLOB (
            '*[' || char(1) || '-' || char(31)
            || char(127) || '-' || char(159) || ']*'
        )
    ),
    CONSTRAINT search_attempt_audit_job_id_bounded CHECK (
        length(CAST(search_job_id AS BLOB)) BETWEEN 1 AND 256
        AND instr(CAST(search_job_id AS BLOB), X'00') = 0
        AND search_job_id = trim(search_job_id)
        AND search_job_id NOT GLOB (
            '*[' || char(1) || '-' || char(31)
            || char(127) || '-' || char(159) || ']*'
        )
    ),
    FOREIGN KEY (tenant_id)
        REFERENCES search_attempt_audit_tenant_state (tenant_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT
) STRICT, WITHOUT ROWID;
CREATE TABLE knowledge_catalog_tenants (
    tenant_id TEXT PRIMARY KEY NOT NULL COLLATE BINARY,
    catalog_revision INTEGER NOT NULL DEFAULT 0 CHECK (
        catalog_revision BETWEEN 0 AND 9223372036854775806
    ),
    identity_count INTEGER NOT NULL DEFAULT 0 CHECK (
        identity_count BETWEEN 0 AND 8192
    ),
    version_count INTEGER NOT NULL DEFAULT 0 CHECK (
        version_count BETWEEN 0 AND 65536
    ),
    definition_body_bytes INTEGER NOT NULL DEFAULT 0 CHECK (
        definition_body_bytes BETWEEN 0 AND 536870912
    ),
    idempotency_count INTEGER NOT NULL DEFAULT 0 CHECK (
        idempotency_count BETWEEN 0 AND 20480
    ),
    active_object_count INTEGER NOT NULL DEFAULT 0 CHECK (
        active_object_count BETWEEN 0 AND 4096
    ),
    recovery_audit_count INTEGER NOT NULL DEFAULT 0 CHECK (
        recovery_audit_count BETWEEN 0 AND 8192
    ),
    CONSTRAINT knowledge_catalog_tenant_id_bounded CHECK (
        length(CAST(tenant_id AS BLOB)) BETWEEN 1 AND 255
        AND instr(CAST(tenant_id AS BLOB), X'00') = 0
        AND tenant_id = trim(tenant_id)
        AND tenant_id NOT GLOB (
            '*[' || char(1) || '-' || char(31)
            || char(127) || '-' || char(159) || ']*'
        )
    )
) STRICT, WITHOUT ROWID;
CREATE TABLE knowledge_definition_blobs (
    tenant_id TEXT NOT NULL COLLATE BINARY,
    definition_digest BLOB NOT NULL CHECK (length(definition_digest) = 32),
    definition_proto BLOB NOT NULL,
    definition_bytes INTEGER NOT NULL CHECK (
        definition_bytes BETWEEN 1 AND 4194304
        AND definition_bytes = length(definition_proto)
    ),
    created_at_unix_micro INTEGER NOT NULL CHECK (
        created_at_unix_micro BETWEEN 1 AND 253402300799999999
    ),
    PRIMARY KEY (tenant_id, definition_digest),
    FOREIGN KEY (tenant_id) REFERENCES knowledge_catalog_tenants (tenant_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT
) STRICT, WITHOUT ROWID;
CREATE TABLE knowledge_objects (
    tenant_id TEXT NOT NULL COLLATE BINARY,
    knowledge_object_id TEXT NOT NULL COLLATE BINARY,
    current_version INTEGER NOT NULL CHECK (
        current_version BETWEEN 1 AND 9223372036854775807
    ),
    app_id TEXT NOT NULL COLLATE BINARY,
    owner_id TEXT NOT NULL COLLATE BINARY,
    object_type TEXT NOT NULL COLLATE BINARY CHECK (
        object_type IN ('field_extraction', 'field_alias', 'calculated_field')
    ),
    name TEXT NOT NULL COLLATE BINARY,
    sharing_scope TEXT NOT NULL COLLATE BINARY CHECK (
        sharing_scope IN ('private', 'app', 'global')
    ),
    state TEXT NOT NULL COLLATE BINARY CHECK (
        state IN ('draft', 'active', 'disabled', 'quarantined', 'deleted')
    ),
    definition_digest BLOB CHECK (
        definition_digest IS NULL OR length(definition_digest) = 32
    ),
    definition_digest_key BLOB GENERATED ALWAYS AS (
        coalesce(definition_digest, X'')
    ) STORED,
    created_at_unix_micro INTEGER NOT NULL CHECK (
        created_at_unix_micro BETWEEN 1 AND 253402300799999999
    ),
    updated_at_unix_micro INTEGER NOT NULL CHECK (
        updated_at_unix_micro BETWEEN 1 AND 253402300799999999
    ),
    disabled_at_unix_micro INTEGER,
    quarantined_at_unix_micro INTEGER,
    deleted_at_unix_micro INTEGER,
    quarantine_reason TEXT COLLATE BINARY CHECK (
        quarantine_reason IS NULL
        OR quarantine_reason IN ('root_corruption', 'dependency_recovery')
    ),
    PRIMARY KEY (tenant_id, knowledge_object_id),
    UNIQUE (tenant_id, knowledge_object_id, current_version),
    CONSTRAINT knowledge_objects_id_bounded CHECK (
        length(CAST(knowledge_object_id AS BLOB)) BETWEEN 1 AND 128
        AND instr(CAST(knowledge_object_id AS BLOB), X'00') = 0
        AND knowledge_object_id = trim(knowledge_object_id)
        AND knowledge_object_id NOT GLOB (
            '*[' || char(1) || '-' || char(31)
            || char(127) || '-' || char(159) || ']*'
        )
    ),
    CONSTRAINT knowledge_objects_owner_id_bounded CHECK (
        length(CAST(owner_id AS BLOB)) BETWEEN 1 AND 255
        AND instr(CAST(owner_id AS BLOB), X'00') = 0
        AND owner_id = trim(owner_id)
        AND owner_id NOT GLOB (
            '*[' || char(1) || '-' || char(31)
            || char(127) || '-' || char(159) || ']*'
        )
    ),
    CONSTRAINT knowledge_objects_name_bounded CHECK (
        length(CAST(name AS BLOB)) BETWEEN 1 AND 255
        AND instr(CAST(name AS BLOB), X'00') = 0
        AND name = trim(name)
        AND name NOT GLOB (
            '*[' || char(1) || '-' || char(31)
            || char(127) || '-' || char(159) || ']*'
        )
    ),
    CONSTRAINT knowledge_objects_time_ordered CHECK (
        updated_at_unix_micro >= created_at_unix_micro
        AND (
            disabled_at_unix_micro IS NULL
            OR disabled_at_unix_micro BETWEEN created_at_unix_micro AND updated_at_unix_micro
        )
        AND (
            quarantined_at_unix_micro IS NULL
            OR quarantined_at_unix_micro BETWEEN created_at_unix_micro AND updated_at_unix_micro
        )
        AND (
            deleted_at_unix_micro IS NULL
            OR deleted_at_unix_micro BETWEEN created_at_unix_micro AND updated_at_unix_micro
        )
    ),
    CONSTRAINT knowledge_objects_state_shape_supported CHECK (
        (
            state IN ('draft', 'active')
            AND definition_digest IS NOT NULL
            AND disabled_at_unix_micro IS NULL
            AND quarantined_at_unix_micro IS NULL
            AND deleted_at_unix_micro IS NULL
            AND quarantine_reason IS NULL
        )
        OR (
            state = 'disabled'
            AND definition_digest IS NOT NULL
            AND disabled_at_unix_micro IS NOT NULL
            AND quarantined_at_unix_micro IS NULL
            AND deleted_at_unix_micro IS NULL
            AND quarantine_reason IS NULL
        )
        OR (
            state = 'quarantined'
            AND definition_digest IS NULL
            AND disabled_at_unix_micro IS NULL
            AND quarantined_at_unix_micro IS NOT NULL
            AND deleted_at_unix_micro IS NULL
            AND quarantine_reason IS NOT NULL
        )
        OR (
            state = 'deleted'
            AND definition_digest IS NOT NULL
            AND disabled_at_unix_micro IS NULL
            AND quarantined_at_unix_micro IS NULL
            AND deleted_at_unix_micro IS NOT NULL
            AND quarantine_reason IS NULL
        )
    ),
    FOREIGN KEY (tenant_id) REFERENCES knowledge_catalog_tenants (tenant_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
    FOREIGN KEY (tenant_id, app_id) REFERENCES app_workspaces (tenant_id, app_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
    FOREIGN KEY (tenant_id, definition_digest)
        REFERENCES knowledge_definition_blobs (tenant_id, definition_digest)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
    FOREIGN KEY (tenant_id, knowledge_object_id, current_version)
        REFERENCES knowledge_object_versions (
            tenant_id, knowledge_object_id, object_version
        ) ON UPDATE RESTRICT ON DELETE RESTRICT
        DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (
        tenant_id, knowledge_object_id, current_version,
        app_id, owner_id, object_type, name, sharing_scope, state,
        definition_digest_key
    ) REFERENCES knowledge_object_versions (
        tenant_id, knowledge_object_id, object_version,
        app_id, owner_id, object_type, name, sharing_scope, state,
        definition_digest_key
    ) ON UPDATE RESTRICT ON DELETE RESTRICT
        DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (tenant_id, knowledge_object_id, current_version)
        REFERENCES knowledge_object_dependency_seals (
            tenant_id, knowledge_object_id, object_version
        ) ON UPDATE RESTRICT ON DELETE RESTRICT
        DEFERRABLE INITIALLY DEFERRED
) STRICT, WITHOUT ROWID;
CREATE TABLE knowledge_object_versions (
    tenant_id TEXT NOT NULL COLLATE BINARY,
    knowledge_object_id TEXT NOT NULL COLLATE BINARY,
    object_version INTEGER NOT NULL CHECK (
        object_version BETWEEN 1 AND 9223372036854775807
    ),
    app_id TEXT NOT NULL COLLATE BINARY,
    owner_id TEXT NOT NULL COLLATE BINARY,
    object_type TEXT NOT NULL COLLATE BINARY CHECK (
        object_type IN ('field_extraction', 'field_alias', 'calculated_field')
    ),
    name TEXT NOT NULL COLLATE BINARY,
    sharing_scope TEXT NOT NULL COLLATE BINARY CHECK (
        sharing_scope IN ('private', 'app', 'global')
    ),
    state TEXT NOT NULL COLLATE BINARY CHECK (
        state IN ('draft', 'active', 'disabled', 'quarantined', 'deleted')
    ),
    definition_digest BLOB CHECK (
        definition_digest IS NULL OR length(definition_digest) = 32
    ),
    definition_digest_key BLOB GENERATED ALWAYS AS (
        coalesce(definition_digest, X'')
    ) STORED,
    quarantine_object_id TEXT COLLATE BINARY GENERATED ALWAYS AS (
        CASE WHEN state = 'quarantined' THEN knowledge_object_id END
    ) STORED,
    quarantine_object_version INTEGER GENERATED ALWAYS AS (
        CASE WHEN state = 'quarantined' THEN object_version END
    ) STORED,
    dependency_count INTEGER NOT NULL CHECK (
        dependency_count BETWEEN 0 AND 1024
    ),
    mutation_kind TEXT NOT NULL COLLATE BINARY CHECK (
        mutation_kind IN (
            'create', 'update', 'scope_change', 'enable', 'disable',
            'quarantine', 'delete'
        )
    ),
    quarantine_reason TEXT COLLATE BINARY CHECK (
        quarantine_reason IS NULL
        OR quarantine_reason IN ('root_corruption', 'dependency_recovery')
    ),
    created_at_unix_micro INTEGER NOT NULL CHECK (
        created_at_unix_micro BETWEEN 1 AND 253402300799999999
    ),
    PRIMARY KEY (tenant_id, knowledge_object_id, object_version),
    UNIQUE (
        tenant_id, knowledge_object_id, object_version,
        app_id, owner_id, object_type, name, sharing_scope, state,
        definition_digest_key
    ),
    UNIQUE (
        tenant_id, knowledge_object_id, object_version, dependency_count
    ),
    CONSTRAINT knowledge_object_versions_first_is_create CHECK (
        (object_version = 1 AND mutation_kind = 'create')
        OR (object_version > 1 AND mutation_kind <> 'create')
    ),
    CONSTRAINT knowledge_object_versions_state_shape_supported CHECK (
        (state = 'quarantined') = (mutation_kind = 'quarantine')
        AND (state = 'deleted') = (mutation_kind = 'delete')
        AND (state = 'quarantined') = (quarantine_reason IS NOT NULL)
        AND (state = 'quarantined') = (definition_digest IS NULL)
        AND (mutation_kind <> 'enable' OR state = 'active')
        AND (mutation_kind <> 'disable' OR state = 'disabled')
        AND (mutation_kind <> 'create' OR state IN ('draft', 'active'))
        AND (
            mutation_kind NOT IN ('create', 'update', 'scope_change')
            OR state IN ('draft', 'active', 'disabled')
        )
    ),
    CONSTRAINT knowledge_object_versions_name_bounded CHECK (
        length(CAST(name AS BLOB)) BETWEEN 1 AND 255
        AND instr(CAST(name AS BLOB), X'00') = 0
        AND name = trim(name)
        AND name NOT GLOB (
            '*[' || char(1) || '-' || char(31)
            || char(127) || '-' || char(159) || ']*'
        )
    ),
    CONSTRAINT knowledge_object_versions_owner_id_bounded CHECK (
        length(CAST(owner_id AS BLOB)) BETWEEN 1 AND 255
        AND instr(CAST(owner_id AS BLOB), X'00') = 0
        AND owner_id = trim(owner_id)
        AND owner_id NOT GLOB (
            '*[' || char(1) || '-' || char(31)
            || char(127) || '-' || char(159) || ']*'
        )
    ),
    FOREIGN KEY (tenant_id, knowledge_object_id)
        REFERENCES knowledge_objects (tenant_id, knowledge_object_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT
        DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (tenant_id, app_id) REFERENCES app_workspaces (tenant_id, app_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
    FOREIGN KEY (tenant_id, definition_digest)
        REFERENCES knowledge_definition_blobs (tenant_id, definition_digest)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
    FOREIGN KEY (
        tenant_id, quarantine_object_id, quarantine_object_version
    ) REFERENCES knowledge_objects (
        tenant_id, knowledge_object_id, current_version
    ) ON UPDATE RESTRICT ON DELETE RESTRICT
        DEFERRABLE INITIALLY DEFERRED
) STRICT, WITHOUT ROWID;
CREATE TABLE knowledge_object_dependencies (
    tenant_id TEXT NOT NULL COLLATE BINARY,
    source_object_id TEXT NOT NULL COLLATE BINARY,
    source_object_version INTEGER NOT NULL,
    ordinal INTEGER NOT NULL CHECK (ordinal BETWEEN 0 AND 1023),
    target_kind TEXT NOT NULL COLLATE BINARY CHECK (target_kind = 'object'),
    target_object_id TEXT NOT NULL COLLATE BINARY,
    target_object_version INTEGER NOT NULL,
    dependency_role TEXT NOT NULL COLLATE BINARY CHECK (
        dependency_role = 'field_input'
    ),
    PRIMARY KEY (
        tenant_id, source_object_id, source_object_version, ordinal
    ),
    UNIQUE (
        tenant_id, source_object_id, source_object_version,
        target_kind, target_object_id, target_object_version, dependency_role
    ),
    CHECK (
        source_object_id <> target_object_id
        OR source_object_version <> target_object_version
    ),
    FOREIGN KEY (tenant_id, source_object_id, source_object_version)
        REFERENCES knowledge_object_versions (
            tenant_id, knowledge_object_id, object_version
        ) ON UPDATE RESTRICT ON DELETE RESTRICT,
    FOREIGN KEY (tenant_id, target_object_id, target_object_version)
        REFERENCES knowledge_object_versions (
            tenant_id, knowledge_object_id, object_version
        ) ON UPDATE RESTRICT ON DELETE RESTRICT
) STRICT, WITHOUT ROWID;
CREATE TABLE knowledge_object_dependency_seals (
    tenant_id TEXT NOT NULL COLLATE BINARY,
    knowledge_object_id TEXT NOT NULL COLLATE BINARY,
    object_version INTEGER NOT NULL CHECK (object_version >= 1),
    dependency_count INTEGER NOT NULL CHECK (
        dependency_count BETWEEN 0 AND 1024
    ),
    PRIMARY KEY (tenant_id, knowledge_object_id, object_version),
    FOREIGN KEY (
        tenant_id, knowledge_object_id, object_version, dependency_count
    ) REFERENCES knowledge_object_versions (
        tenant_id, knowledge_object_id, object_version, dependency_count
    ) ON UPDATE RESTRICT ON DELETE RESTRICT
        DEFERRABLE INITIALLY DEFERRED
) STRICT, WITHOUT ROWID;
CREATE TABLE knowledge_object_acl (
    tenant_id TEXT NOT NULL COLLATE BINARY,
    knowledge_object_id TEXT NOT NULL COLLATE BINARY,
    role_id TEXT NOT NULL COLLATE BINARY,
    can_read INTEGER NOT NULL CHECK (can_read IN (0, 1)),
    can_write INTEGER NOT NULL CHECK (can_write IN (0, 1)),
    PRIMARY KEY (tenant_id, knowledge_object_id, role_id),
    CHECK (can_read = 1 OR can_write = 1),
    CHECK (can_write = 0 OR can_read = 1),
    CHECK (
        length(CAST(role_id AS BLOB)) BETWEEN 1 AND 255
        AND instr(CAST(role_id AS BLOB), X'00') = 0
        AND role_id = trim(role_id)
        AND role_id NOT GLOB (
            '*[' || char(1) || '-' || char(31)
            || char(127) || '-' || char(159) || ']*'
        )
    ),
    FOREIGN KEY (tenant_id, knowledge_object_id)
        REFERENCES knowledge_objects (tenant_id, knowledge_object_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT
) STRICT, WITHOUT ROWID;
CREATE TABLE knowledge_app_active_counters (
    tenant_id TEXT NOT NULL COLLATE BINARY,
    app_id TEXT NOT NULL COLLATE BINARY,
    active_object_count INTEGER NOT NULL CHECK (
        active_object_count BETWEEN 0 AND 1024
    ),
    PRIMARY KEY (tenant_id, app_id),
    FOREIGN KEY (tenant_id) REFERENCES knowledge_catalog_tenants (tenant_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
    FOREIGN KEY (tenant_id, app_id) REFERENCES app_workspaces (tenant_id, app_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT
) STRICT, WITHOUT ROWID;
CREATE TABLE knowledge_owner_active_counters (
    tenant_id TEXT NOT NULL COLLATE BINARY,
    owner_id TEXT NOT NULL COLLATE BINARY,
    active_private_object_count INTEGER NOT NULL CHECK (
        active_private_object_count BETWEEN 0 AND 512
    ),
    PRIMARY KEY (tenant_id, owner_id),
    FOREIGN KEY (tenant_id) REFERENCES knowledge_catalog_tenants (tenant_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT
) STRICT, WITHOUT ROWID;
CREATE TABLE knowledge_type_active_counters (
    tenant_id TEXT NOT NULL COLLATE BINARY,
    object_type TEXT NOT NULL COLLATE BINARY CHECK (
        object_type IN ('field_extraction', 'field_alias', 'calculated_field')
    ),
    active_object_count INTEGER NOT NULL CHECK (
        active_object_count BETWEEN 0 AND 2048
    ),
    PRIMARY KEY (tenant_id, object_type),
    FOREIGN KEY (tenant_id) REFERENCES knowledge_catalog_tenants (tenant_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT
) STRICT, WITHOUT ROWID;
CREATE TABLE knowledge_app_type_active_counters (
    tenant_id TEXT NOT NULL COLLATE BINARY,
    app_id TEXT NOT NULL COLLATE BINARY,
    object_type TEXT NOT NULL COLLATE BINARY CHECK (
        object_type IN ('field_extraction', 'field_alias', 'calculated_field')
    ),
    active_object_count INTEGER NOT NULL CHECK (
        active_object_count BETWEEN 0 AND 512
    ),
    PRIMARY KEY (tenant_id, app_id, object_type),
    FOREIGN KEY (tenant_id) REFERENCES knowledge_catalog_tenants (tenant_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
    FOREIGN KEY (tenant_id, app_id) REFERENCES app_workspaces (tenant_id, app_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT
) STRICT, WITHOUT ROWID;
CREATE TABLE knowledge_recovery_audit (
    tenant_id TEXT NOT NULL COLLATE BINARY,
    sequence INTEGER NOT NULL CHECK (sequence BETWEEN 1 AND 8192),
    knowledge_object_id TEXT NOT NULL COLLATE BINARY,
    object_version INTEGER NOT NULL CHECK (object_version >= 1),
    actor_kind TEXT NOT NULL COLLATE BINARY CHECK (
        actor_kind IN ('system', 'browser')
    ),
    actor_id TEXT NOT NULL COLLATE BINARY,
    actor_role TEXT NOT NULL COLLATE BINARY CHECK (
        actor_role IN ('system', 'administrator')
    ),
    app_id TEXT NOT NULL COLLATE BINARY,
    object_type TEXT NOT NULL COLLATE BINARY CHECK (
        object_type IN ('field_extraction', 'field_alias', 'calculated_field')
    ),
    sharing_scope TEXT NOT NULL COLLATE BINARY CHECK (
        sharing_scope IN ('private', 'app', 'global')
    ),
    recovery_reason TEXT NOT NULL COLLATE BINARY CHECK (
        recovery_reason IN ('root_corruption', 'dependency_recovery')
    ),
    occurred_at_unix_micro INTEGER NOT NULL CHECK (
        occurred_at_unix_micro BETWEEN 1 AND 253402300799999999
    ),
    PRIMARY KEY (tenant_id, sequence),
    UNIQUE (tenant_id, knowledge_object_id),
    CHECK (
        (actor_kind = 'system' AND actor_role = 'system')
        OR (actor_kind = 'browser' AND actor_role = 'administrator')
    ),
    CHECK (
        length(CAST(actor_id AS BLOB)) BETWEEN 1 AND 255
        AND instr(CAST(actor_id AS BLOB), X'00') = 0
        AND actor_id = trim(actor_id)
        AND actor_id NOT GLOB (
            '*[' || char(1) || '-' || char(31)
            || char(127) || '-' || char(159) || ']*'
        )
    ),
    FOREIGN KEY (tenant_id) REFERENCES knowledge_catalog_tenants (tenant_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
    FOREIGN KEY (tenant_id, knowledge_object_id, object_version)
        REFERENCES knowledge_object_versions (
            tenant_id, knowledge_object_id, object_version
        ) ON UPDATE RESTRICT ON DELETE RESTRICT
) STRICT, WITHOUT ROWID;
CREATE TABLE knowledge_projection_tenant_ledgers (
    tenant_id TEXT PRIMARY KEY NOT NULL COLLATE BINARY,
    projection_bytes INTEGER NOT NULL DEFAULT 0 CHECK (
        projection_bytes BETWEEN 0 AND 268435456
    ),
    FOREIGN KEY (tenant_id) REFERENCES knowledge_catalog_tenants (tenant_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT
) STRICT, WITHOUT ROWID;
CREATE TABLE knowledge_object_list_projections (
    tenant_id TEXT NOT NULL COLLATE BINARY,
    knowledge_object_id TEXT NOT NULL COLLATE BINARY,
    object_version INTEGER NOT NULL CHECK (object_version >= 1),
    app_id TEXT NOT NULL COLLATE BINARY,
    owner_id TEXT NOT NULL COLLATE BINARY,
    object_type TEXT NOT NULL COLLATE BINARY CHECK (
        object_type IN ('field_extraction', 'field_alias', 'calculated_field')
    ),
    name TEXT NOT NULL COLLATE BINARY,
    sharing_scope TEXT NOT NULL COLLATE BINARY CHECK (
        sharing_scope IN ('private', 'app', 'global')
    ),
    state TEXT NOT NULL COLLATE BINARY CHECK (
        state IN ('draft', 'active', 'disabled', 'quarantined', 'deleted')
    ),
    description_present INTEGER NOT NULL CHECK (
        description_present IN (0, 1)
    ),
    description TEXT NOT NULL COLLATE BINARY DEFAULT '',
    index_selector_count INTEGER NOT NULL CHECK (
        index_selector_count BETWEEN 0 AND 16
    ),
    host_selector_count INTEGER NOT NULL CHECK (
        host_selector_count BETWEEN 0 AND 16
    ),
    source_selector_count INTEGER NOT NULL CHECK (
        source_selector_count BETWEEN 0 AND 16
    ),
    sourcetype_selector_count INTEGER NOT NULL CHECK (
        sourcetype_selector_count BETWEEN 0 AND 16
    ),
    selector_value_bytes INTEGER NOT NULL CHECK (
        selector_value_bytes BETWEEN 0 AND 8192
    ),
    canonical_selector_bytes INTEGER NOT NULL CHECK (
        canonical_selector_bytes BETWEEN 0 AND 8192
        AND selector_value_bytes <= canonical_selector_bytes
    ),
    -- Normative accounted bytes are exactly description bytes plus selector
    -- value bytes. Registry identity columns are already charged by KO-0.
    projection_bytes INTEGER GENERATED ALWAYS AS (
        length(CAST(description AS BLOB))
        + selector_value_bytes
    ) STORED,
    PRIMARY KEY (tenant_id, knowledge_object_id, object_version),
    CONSTRAINT knowledge_list_projection_description_canonical CHECK (
        length(CAST(description AS BLOB)) <= 16384
        AND instr(CAST(description AS BLOB), X'00') = 0
        AND (
            (description_present = 0 AND description = '')
            OR (
                description_present = 1
                AND length(CAST(description AS BLOB)) >= 1
                AND description = trim(description)
                AND description NOT GLOB (
                    '*[' || char(1) || '-' || char(31)
                    || char(127) || '-' || char(159) || ']*'
                )
            )
        )
    ),
    CONSTRAINT knowledge_list_projection_selector_count_bounded CHECK (
        index_selector_count
        + host_selector_count
        + source_selector_count
        + sourcetype_selector_count <= 64
    ),
    -- The canonical selector encoding has 43 bytes of domain/dimension
    -- framing, four bytes of framing per pattern, and the exact UTF-8 value
    -- bytes. Quarantined definitions are never decoded and have no encoding.
    CONSTRAINT knowledge_list_projection_selector_charge_exact CHECK (
        (
            state = 'quarantined'
            AND canonical_selector_bytes = 0
        )
        OR (
            state <> 'quarantined'
            AND canonical_selector_bytes = 43
                + 4 * (
                    index_selector_count
                    + host_selector_count
                    + source_selector_count
                    + sourcetype_selector_count
                )
                + selector_value_bytes
        )
    ),
    CONSTRAINT knowledge_list_projection_quarantine_is_bodyless CHECK (
        state <> 'quarantined'
        OR (
            description_present = 0
            AND index_selector_count = 0
            AND host_selector_count = 0
            AND source_selector_count = 0
            AND sourcetype_selector_count = 0
            AND selector_value_bytes = 0
            AND canonical_selector_bytes = 0
        )
    ),
    FOREIGN KEY (tenant_id) REFERENCES knowledge_projection_tenant_ledgers (tenant_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
    FOREIGN KEY (
        tenant_id, knowledge_object_id, object_version,
        app_id, owner_id, object_type, name, sharing_scope, state
    ) REFERENCES knowledge_objects (
        tenant_id, knowledge_object_id, current_version,
        app_id, owner_id, object_type, name, sharing_scope, state
    ) ON UPDATE NO ACTION ON DELETE RESTRICT
        DEFERRABLE INITIALLY DEFERRED
) STRICT, WITHOUT ROWID;
CREATE TABLE knowledge_object_list_selector_patterns (
    tenant_id TEXT NOT NULL COLLATE BINARY,
    knowledge_object_id TEXT NOT NULL COLLATE BINARY,
    object_version INTEGER NOT NULL CHECK (object_version >= 1),
    dimension TEXT NOT NULL COLLATE BINARY CHECK (
        dimension IN ('index', 'host', 'source', 'sourcetype')
    ),
    ordinal INTEGER NOT NULL CHECK (ordinal BETWEEN 0 AND 15),
    match_kind TEXT NOT NULL COLLATE BINARY CHECK (
        match_kind IN ('exact', 'wildcard')
    ),
    value TEXT NOT NULL COLLATE BINARY,
    value_bytes INTEGER GENERATED ALWAYS AS (
        length(CAST(value AS BLOB))
    ) STORED,
    PRIMARY KEY (
        tenant_id, knowledge_object_id, object_version, dimension, ordinal
    ),
    UNIQUE (
        tenant_id, knowledge_object_id, object_version, dimension, value
    ),
    CHECK (value_bytes BETWEEN 1 AND 255),
    CHECK (instr(CAST(value AS BLOB), X'00') = 0),
    CHECK (
        value = trim(value)
        AND value NOT GLOB (
            '*[' || char(1) || '-' || char(31)
            || char(127) || '-' || char(159) || ']*'
        )
    ),
    FOREIGN KEY (tenant_id, knowledge_object_id, object_version)
        REFERENCES knowledge_object_list_projections (
            tenant_id, knowledge_object_id, object_version
        ) ON UPDATE RESTRICT ON DELETE RESTRICT
) STRICT, WITHOUT ROWID;
CREATE TABLE knowledge_object_list_projection_seals (
    tenant_id TEXT NOT NULL COLLATE BINARY,
    knowledge_object_id TEXT NOT NULL COLLATE BINARY,
    object_version INTEGER NOT NULL CHECK (object_version >= 1),
    projection_bytes INTEGER NOT NULL CHECK (
        projection_bytes BETWEEN 0 AND 268435456
    ),
    canonical_selector_bytes INTEGER NOT NULL CHECK (
        canonical_selector_bytes BETWEEN 0 AND 8192
    ),
    PRIMARY KEY (tenant_id, knowledge_object_id, object_version),
    FOREIGN KEY (tenant_id, knowledge_object_id, object_version)
        REFERENCES knowledge_object_list_projections (
            tenant_id, knowledge_object_id, object_version
        ) ON UPDATE RESTRICT ON DELETE RESTRICT
) STRICT, WITHOUT ROWID;
CREATE TABLE audit_events (
    tenant_id TEXT NOT NULL COLLATE BINARY,
    sequence INTEGER NOT NULL CHECK (sequence BETWEEN 1 AND 100000),
    occurred_at_unix_micro INTEGER NOT NULL CHECK (
        occurred_at_unix_micro BETWEEN 1 AND 253402300799999999
    ),
    actor_kind TEXT NOT NULL COLLATE BINARY CHECK (
        actor_kind IN ('system', 'browser')
    ),
    actor_id TEXT NOT NULL COLLATE BINARY,
    actor_role TEXT NOT NULL COLLATE BINARY CHECK (
        actor_role IN ('system', 'user', 'administrator')
    ),
    action TEXT NOT NULL COLLATE BINARY CHECK (
        action IN (
            'ingestion_token.create',
            'ingestion_token.update',
            'ingestion_token.revoke',
            'index.create',
            'index.update',
            'index.activate',
            'index.archive',
            'index.delete_keep_data',
            'index.delete_data',
            'app.create',
            'app.update',
            'app.activate',
            'app.archive',
            'app.delete',
            'saved_search.create',
            'saved_search.update',
            'saved_search.duplicate',
            'saved_search.delete',
            'knowledge.object.create',
            'knowledge.object.update',
            'knowledge.object.scope_change',
            'knowledge.object.enable',
            'knowledge.object.disable',
            'knowledge.object.delete'
        )
    ),
    target_kind TEXT NOT NULL COLLATE BINARY CHECK (
        target_kind IN (
            'ingestion_token', 'index', 'app', 'saved_search',
            'knowledge_object'
        )
    ),
    target_id TEXT NOT NULL COLLATE BINARY,
    target_version INTEGER NOT NULL CHECK (
        target_version BETWEEN 1 AND 9223372036854775807
    ),
    app_id TEXT COLLATE BINARY,
    object_type TEXT COLLATE BINARY CHECK (
        object_type IS NULL
        OR object_type IN (
            'field_extraction', 'field_alias', 'calculated_field'
        )
    ),
    sharing_scope TEXT COLLATE BINARY CHECK (
        sharing_scope IS NULL OR sharing_scope IN ('private', 'app', 'global')
    ),
    PRIMARY KEY (tenant_id, sequence),
    CONSTRAINT audit_events_actor_id_bounded CHECK (
        length(CAST(actor_id AS BLOB)) BETWEEN 1 AND 255
        AND instr(CAST(actor_id AS BLOB), X'00') = 0
        AND actor_id = trim(actor_id)
        AND actor_id NOT GLOB (
            '*[' || char(1) || '-' || char(31)
            || char(127) || '-' || char(159) || ']*'
        )
    ),
    CONSTRAINT audit_events_actor_shape_supported CHECK (
        (actor_kind = 'system' AND actor_role = 'system')
        OR (
            actor_kind = 'browser'
            AND actor_role = 'administrator'
        )
        OR (
            actor_kind = 'browser'
            AND actor_role = 'user'
            AND action IN (
                'saved_search.create',
                'saved_search.update',
                'saved_search.duplicate',
                'saved_search.delete'
            )
        )
    ),
    CONSTRAINT audit_events_action_version_supported CHECK (
        (
            action IN (
                'ingestion_token.create',
                'index.create',
                'app.create',
                'saved_search.create',
                'saved_search.duplicate',
                'knowledge.object.create'
            )
            AND target_version = 1
        )
        OR (
            action IN (
                'ingestion_token.update',
                'ingestion_token.revoke',
                'index.update',
                'index.activate',
                'index.archive',
                'index.delete_keep_data',
                'app.update',
                'app.activate',
                'app.archive',
                'app.delete',
                'saved_search.update',
                'knowledge.object.update',
                'knowledge.object.scope_change',
                'knowledge.object.enable',
                'knowledge.object.disable',
                'knowledge.object.delete'
            )
            AND target_version >= 2
        )
        OR (action = 'index.delete_data' AND target_version >= 3)
        OR (action = 'saved_search.delete' AND target_version >= 1)
    ),
    CONSTRAINT audit_events_action_target_supported CHECK (
        (
            action IN (
                'ingestion_token.create',
                'ingestion_token.update',
                'ingestion_token.revoke'
            )
            AND target_kind = 'ingestion_token'
        )
        OR (
            action IN (
                'index.create',
                'index.update',
                'index.activate',
                'index.archive',
                'index.delete_keep_data',
                'index.delete_data'
            )
            AND target_kind = 'index'
        )
        OR (
            action IN (
                'app.create',
                'app.update',
                'app.activate',
                'app.archive',
                'app.delete'
            )
            AND target_kind = 'app'
        )
        OR (
            action IN (
                'saved_search.create',
                'saved_search.update',
                'saved_search.duplicate',
                'saved_search.delete'
            )
            AND target_kind = 'saved_search'
        )
        OR (
            action IN (
                'knowledge.object.create',
                'knowledge.object.update',
                'knowledge.object.scope_change',
                'knowledge.object.enable',
                'knowledge.object.disable',
                'knowledge.object.delete'
            )
            AND target_kind = 'knowledge_object'
        )
    ),
    CONSTRAINT audit_events_target_id_bounded CHECK (
        length(CAST(target_id AS BLOB)) BETWEEN 1 AND 128
        AND instr(CAST(target_id AS BLOB), X'00') = 0
        AND target_id = trim(target_id)
        AND target_id NOT GLOB (
            '*[' || char(1) || '-' || char(31)
            || char(127) || '-' || char(159) || ']*'
        )
    ),
    CONSTRAINT audit_events_knowledge_metadata_shape_supported CHECK (
        (
            target_kind = 'knowledge_object'
            AND app_id IS NOT NULL
            AND object_type IS NOT NULL
            AND sharing_scope IS NOT NULL
        )
        OR (
            target_kind <> 'knowledge_object'
            AND app_id IS NULL
            AND object_type IS NULL
            AND sharing_scope IS NULL
        )
    ),
    CONSTRAINT audit_events_knowledge_app_id_bounded CHECK (
        app_id IS NULL
        OR (
            length(CAST(app_id AS BLOB)) BETWEEN 1 AND 128
            AND instr(CAST(app_id AS BLOB), X'00') = 0
            AND app_id = trim(app_id)
            AND app_id NOT GLOB (
                '*[' || char(1) || '-' || char(31)
                || char(127) || '-' || char(159) || ']*'
            )
        )
    ),
    FOREIGN KEY (tenant_id) REFERENCES audit_tenant_state (tenant_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT
) STRICT, WITHOUT ROWID;
CREATE TABLE knowledge_attempt_audit_tenant_state (
    tenant_id TEXT PRIMARY KEY NOT NULL COLLATE BINARY,
    first_sequence INTEGER NOT NULL CHECK (
        first_sequence BETWEEN 1 AND 9223372036854775807
    ),
    next_sequence INTEGER NOT NULL CHECK (
        next_sequence BETWEEN 1 AND 9223372036854775807
    ),
    retained_count INTEGER NOT NULL CHECK (
        retained_count BETWEEN 0 AND 100001
    ),
    CONSTRAINT knowledge_attempt_audit_state_dense CHECK (
        next_sequence >= first_sequence
        AND next_sequence - first_sequence = retained_count
    ),
    CONSTRAINT knowledge_attempt_audit_state_tenant_id_bounded CHECK (
        length(CAST(tenant_id AS BLOB)) BETWEEN 1 AND 255
        AND instr(CAST(tenant_id AS BLOB), X'00') = 0
        AND tenant_id = trim(tenant_id)
        AND tenant_id NOT GLOB (
            '*[' || char(1) || '-' || char(31)
            || char(127) || '-' || char(159) || ']*'
        )
    )
) STRICT, WITHOUT ROWID;
CREATE TABLE knowledge_attempt_audit_events (
    tenant_id TEXT NOT NULL COLLATE BINARY,
    sequence INTEGER NOT NULL CHECK (
        sequence BETWEEN 1 AND 9223372036854775806
    ),
    occurred_at_unix_micro INTEGER NOT NULL CHECK (
        occurred_at_unix_micro BETWEEN 1 AND 253402300799999999
    ),
    actor_kind TEXT NOT NULL COLLATE BINARY CHECK (actor_kind = 'browser'),
    actor_id TEXT NOT NULL COLLATE BINARY,
    actor_role TEXT NOT NULL COLLATE BINARY CHECK (
        actor_role IN ('user', 'administrator')
    ),
    action TEXT NOT NULL COLLATE BINARY CHECK (
        action IN (
            'create', 'get', 'list', 'update', 'scope_change',
            'enable', 'disable', 'quarantine', 'delete', 'validate',
            'dependencies', 'dependents', 'preview'
        )
    ),
    result TEXT NOT NULL COLLATE BINARY CHECK (result = 'rejected'),
    reason TEXT NOT NULL COLLATE BINARY CHECK (
        reason IN (
            'not_administrator', 'not_found_or_forbidden',
            'version_conflict', 'idempotency_conflict',
            'invalid_definition', 'forbidden_dependency',
            'resource_limit', 'service_unavailable'
        )
    ),
    app_id TEXT COLLATE BINARY,
    knowledge_object_id TEXT COLLATE BINARY,
    object_type TEXT COLLATE BINARY CHECK (
        object_type IS NULL OR object_type IN (
            'field_extraction', 'field_alias', 'calculated_field'
        )
    ),
    object_version INTEGER CHECK (
        object_version IS NULL OR object_version >= 1
    ),
    sharing_scope TEXT COLLATE BINARY CHECK (
        sharing_scope IS NULL OR sharing_scope IN ('private', 'app', 'global')
    ),
    PRIMARY KEY (tenant_id, sequence),
    CONSTRAINT knowledge_attempt_audit_actor_id_bounded CHECK (
        length(CAST(actor_id AS BLOB)) BETWEEN 1 AND 255
        AND instr(CAST(actor_id AS BLOB), X'00') = 0
        AND actor_id = trim(actor_id)
        AND actor_id NOT GLOB (
            '*[' || char(1) || '-' || char(31)
            || char(127) || '-' || char(159) || ']*'
        )
    ),
    CONSTRAINT knowledge_attempt_audit_actor_reason_shape CHECK (
        (
            actor_role = 'user'
            AND reason = 'not_administrator'
            AND app_id IS NULL
            AND knowledge_object_id IS NULL
            AND object_type IS NULL
            AND object_version IS NULL
            AND sharing_scope IS NULL
        )
        OR (
            actor_role = 'administrator'
            AND reason <> 'not_administrator'
        )
    ),
    CONSTRAINT knowledge_attempt_audit_app_shape CHECK (
        app_id IS NULL
        OR (
            length(CAST(app_id AS BLOB)) BETWEEN 1 AND 128
            AND instr(CAST(app_id AS BLOB), X'00') = 0
            AND app_id = trim(app_id)
            AND app_id NOT GLOB (
                '*[' || char(1) || '-' || char(31)
                || char(127) || '-' || char(159) || ']*'
            )
        )
    ),
    CONSTRAINT knowledge_attempt_audit_object_shape CHECK (
        (
            knowledge_object_id IS NULL
            AND object_type IS NULL
            AND object_version IS NULL
            AND sharing_scope IS NULL
        )
        OR (
            app_id IS NOT NULL
            AND reason NOT IN (
                'not_administrator', 'not_found_or_forbidden'
            )
            AND knowledge_object_id IS NOT NULL
            AND object_type IS NOT NULL
            AND object_version IS NOT NULL
            AND sharing_scope IS NOT NULL
            AND length(CAST(knowledge_object_id AS BLOB)) BETWEEN 1 AND 128
            AND instr(CAST(knowledge_object_id AS BLOB), X'00') = 0
            AND knowledge_object_id = trim(knowledge_object_id)
            AND knowledge_object_id NOT GLOB (
                '*[' || char(1) || '-' || char(31)
                || char(127) || '-' || char(159) || ']*'
            )
        )
    ),
    CONSTRAINT knowledge_attempt_audit_action_object_shape CHECK (
        action NOT IN ('create', 'list') OR knowledge_object_id IS NULL
    ),
    CONSTRAINT knowledge_attempt_audit_version_conflict_shape CHECK (
        reason <> 'version_conflict' OR knowledge_object_id IS NOT NULL
    ),
    CONSTRAINT knowledge_attempt_audit_serialized_bound CHECK (
        length(CAST(tenant_id AS BLOB))
        + length(CAST(actor_kind AS BLOB))
        + length(CAST(actor_id AS BLOB))
        + length(CAST(actor_role AS BLOB))
        + length(CAST(action AS BLOB))
        + length(CAST(result AS BLOB))
        + length(CAST(reason AS BLOB))
        + coalesce(length(CAST(app_id AS BLOB)), 0)
        + coalesce(length(CAST(knowledge_object_id AS BLOB)), 0)
        + coalesce(length(CAST(object_type AS BLOB)), 0)
        + coalesce(length(CAST(sharing_scope AS BLOB)), 0)
        <= 4096
    ),
    FOREIGN KEY (tenant_id)
        REFERENCES knowledge_attempt_audit_tenant_state (tenant_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT
) STRICT, WITHOUT ROWID;
CREATE TABLE knowledge_catalog_revision_heads (
    tenant_id TEXT PRIMARY KEY NOT NULL COLLATE BINARY,
    catalog_revision INTEGER NOT NULL CHECK (
        catalog_revision BETWEEN 0 AND 9223372036854775806
    ),
    state_token BLOB NOT NULL CHECK (length(state_token) = 32),
    FOREIGN KEY (tenant_id) REFERENCES knowledge_catalog_tenants (tenant_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT
) STRICT, WITHOUT ROWID;
CREATE TABLE knowledge_object_version_lifecycle (
    tenant_id TEXT NOT NULL COLLATE BINARY,
    knowledge_object_id TEXT NOT NULL COLLATE BINARY,
    object_version INTEGER NOT NULL CHECK (object_version >= 1),
    state TEXT NOT NULL COLLATE BINARY CHECK (
        state IN ('draft', 'active', 'disabled', 'quarantined', 'deleted')
    ),
    disabled_at_unix_micro INTEGER CHECK (
        disabled_at_unix_micro IS NULL
        OR disabled_at_unix_micro BETWEEN 1 AND 253402300799999999
    ),
    quarantined_at_unix_micro INTEGER CHECK (
        quarantined_at_unix_micro IS NULL
        OR quarantined_at_unix_micro BETWEEN 1 AND 253402300799999999
    ),
    deleted_at_unix_micro INTEGER CHECK (
        deleted_at_unix_micro IS NULL
        OR deleted_at_unix_micro BETWEEN 1 AND 253402300799999999
    ),
    quarantine_reason TEXT COLLATE BINARY CHECK (
        quarantine_reason IS NULL
        OR quarantine_reason IN ('root_corruption', 'dependency_recovery')
    ),
    PRIMARY KEY (tenant_id, knowledge_object_id, object_version),
    CONSTRAINT knowledge_object_version_lifecycle_shape_is_exact CHECK (
        (
            state IN ('draft', 'active')
            AND disabled_at_unix_micro IS NULL
            AND quarantined_at_unix_micro IS NULL
            AND deleted_at_unix_micro IS NULL
            AND quarantine_reason IS NULL
        )
        OR (
            state = 'disabled'
            AND disabled_at_unix_micro IS NOT NULL
            AND quarantined_at_unix_micro IS NULL
            AND deleted_at_unix_micro IS NULL
            AND quarantine_reason IS NULL
        )
        OR (
            state = 'quarantined'
            AND disabled_at_unix_micro IS NULL
            AND quarantined_at_unix_micro IS NOT NULL
            AND deleted_at_unix_micro IS NULL
            AND quarantine_reason IS NOT NULL
        )
        OR (
            state = 'deleted'
            AND disabled_at_unix_micro IS NULL
            AND quarantined_at_unix_micro IS NULL
            AND deleted_at_unix_micro IS NOT NULL
            AND quarantine_reason IS NULL
        )
    ),
    FOREIGN KEY (tenant_id, knowledge_object_id, object_version)
        REFERENCES knowledge_object_versions (
            tenant_id, knowledge_object_id, object_version
        ) ON UPDATE RESTRICT ON DELETE RESTRICT
) STRICT, WITHOUT ROWID;
CREATE TABLE knowledge_object_list_order_keys (
    tenant_id TEXT NOT NULL COLLATE BINARY,
    knowledge_object_id TEXT NOT NULL COLLATE BINARY,
    object_version INTEGER NOT NULL CHECK (object_version >= 1),
    created_at_unix_micro INTEGER NOT NULL CHECK (
        created_at_unix_micro BETWEEN 1 AND 253402300799999999
    ),
    updated_at_unix_micro INTEGER NOT NULL CHECK (
        updated_at_unix_micro BETWEEN created_at_unix_micro AND 253402300799999999
    ),
    PRIMARY KEY (tenant_id, knowledge_object_id, object_version),
    FOREIGN KEY (tenant_id, knowledge_object_id, object_version)
        REFERENCES knowledge_object_list_projections (
            tenant_id, knowledge_object_id, object_version
        ) ON UPDATE RESTRICT ON DELETE CASCADE
) STRICT, WITHOUT ROWID;
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
CREATE TABLE ingestion_token_hec_profiles (
    ingestion_token_id TEXT PRIMARY KEY NOT NULL COLLATE BINARY
        REFERENCES ingestion_tokens (ingestion_token_id)
            ON UPDATE RESTRICT
            ON DELETE CASCADE,
    default_index_id TEXT COLLATE BINARY,
    default_host TEXT COLLATE BINARY,
    default_source TEXT COLLATE BINARY,
    default_sourcetype TEXT COLLATE BINARY,
    indexer_acknowledgment INTEGER NOT NULL
        CHECK (indexer_acknowledgment IN (0, 1)),
    CONSTRAINT ingestion_token_hec_profiles_default_index_membership
        FOREIGN KEY (ingestion_token_id, default_index_id)
        REFERENCES ingestion_token_indexes (ingestion_token_id, index_id)
            ON UPDATE RESTRICT
            ON DELETE RESTRICT,
    CONSTRAINT ingestion_token_hec_profiles_default_host_bounded CHECK (
        default_host IS NULL
        OR (
            length(CAST(default_host AS BLOB)) BETWEEN 1 AND 255
            AND instr(CAST(default_host AS BLOB), X'00') = 0
            AND default_host = trim(default_host)
            AND default_host NOT GLOB (
                '*[' || char(1) || '-' || char(31)
                || char(127) || '-' || char(159) || ']*'
            )
        )
    ),
    CONSTRAINT ingestion_token_hec_profiles_default_source_bounded CHECK (
        default_source IS NULL
        OR (
            length(CAST(default_source AS BLOB)) BETWEEN 1 AND 255
            AND instr(CAST(default_source AS BLOB), X'00') = 0
            AND default_source = trim(default_source)
            AND default_source NOT GLOB (
                '*[' || char(1) || '-' || char(31)
                || char(127) || '-' || char(159) || ']*'
            )
        )
    ),
    CONSTRAINT ingestion_token_hec_profiles_default_sourcetype_bounded CHECK (
        default_sourcetype IS NULL
        OR (
            length(CAST(default_sourcetype AS BLOB)) BETWEEN 1 AND 255
            AND instr(CAST(default_sourcetype AS BLOB), X'00') = 0
            AND default_sourcetype = trim(default_sourcetype)
            AND default_sourcetype NOT GLOB (
                '*[' || char(1) || '-' || char(31)
                || char(127) || '-' || char(159) || ']*'
            )
        )
    )
) STRICT, WITHOUT ROWID;
CREATE TABLE hec_source_sequences (
    tenant_id TEXT NOT NULL COLLATE BINARY,
    ingestion_token_id TEXT NOT NULL COLLATE BINARY
        REFERENCES ingestion_tokens (ingestion_token_id)
            ON UPDATE RESTRICT
            ON DELETE CASCADE,
    next_request_sequence INTEGER NOT NULL DEFAULT 1
        CHECK (next_request_sequence >= 1),
    updated_at_unix_micro INTEGER NOT NULL,
    PRIMARY KEY (tenant_id, ingestion_token_id),
    CHECK (length(CAST(tenant_id AS BLOB)) BETWEEN 1 AND 255),
    CHECK (instr(CAST(tenant_id AS BLOB), X'00') = 0)
) STRICT, WITHOUT ROWID;
CREATE TABLE hec_requests (
    tenant_id TEXT NOT NULL COLLATE BINARY,
    ingestion_token_id TEXT NOT NULL COLLATE BINARY,
    request_sequence INTEGER NOT NULL CHECK (request_sequence >= 1),
    request_id TEXT NOT NULL COLLATE BINARY,
    visibility_sequence INTEGER UNIQUE
        REFERENCES ingest_visibility_reservations (sequence)
            ON UPDATE RESTRICT
            ON DELETE SET NULL,
    state TEXT NOT NULL DEFAULT 'pending' COLLATE BINARY
        CHECK (state IN ('pending', 'indexed', 'terminal_failure')),
    created_at_unix_micro INTEGER NOT NULL,
    terminal_at_unix_micro INTEGER,
    PRIMARY KEY (tenant_id, ingestion_token_id, request_sequence),
    UNIQUE (tenant_id, ingestion_token_id, request_id),
    FOREIGN KEY (tenant_id, ingestion_token_id)
        REFERENCES hec_source_sequences (tenant_id, ingestion_token_id)
            ON UPDATE RESTRICT
            ON DELETE CASCADE,
    CHECK (length(CAST(request_id AS BLOB)) BETWEEN 1 AND 128),
    CHECK (instr(CAST(request_id AS BLOB), X'00') = 0),
    CHECK (
        (state = 'pending'
            AND visibility_sequence IS NOT NULL
            AND terminal_at_unix_micro IS NULL)
        OR
        (state IN ('indexed', 'terminal_failure')
            AND terminal_at_unix_micro IS NOT NULL)
    )
) STRICT, WITHOUT ROWID;
CREATE TABLE hec_channels (
    tenant_id TEXT NOT NULL COLLATE BINARY,
    ingestion_token_id TEXT NOT NULL COLLATE BINARY
        REFERENCES ingestion_tokens (ingestion_token_id)
            ON UPDATE RESTRICT
            ON DELETE CASCADE,
    channel_id TEXT NOT NULL COLLATE BINARY,
    next_acknowledgment_id INTEGER NOT NULL DEFAULT 1
        CHECK (next_acknowledgment_id >= 1),
    created_at_unix_micro INTEGER NOT NULL,
    last_used_at_unix_micro INTEGER NOT NULL,
    PRIMARY KEY (tenant_id, ingestion_token_id, channel_id),
    CHECK (length(CAST(tenant_id AS BLOB)) BETWEEN 1 AND 255),
    CHECK (instr(CAST(tenant_id AS BLOB), X'00') = 0),
    CHECK (length(CAST(channel_id AS BLOB)) BETWEEN 1 AND 128),
    CHECK (instr(CAST(channel_id AS BLOB), X'00') = 0),
    CHECK (channel_id = trim(channel_id)),
    CHECK (last_used_at_unix_micro >= created_at_unix_micro)
) STRICT, WITHOUT ROWID;
CREATE TABLE hec_acknowledgments (
    tenant_id TEXT NOT NULL COLLATE BINARY,
    ingestion_token_id TEXT NOT NULL COLLATE BINARY,
    channel_id TEXT NOT NULL COLLATE BINARY,
    acknowledgment_id INTEGER NOT NULL
        CHECK (acknowledgment_id BETWEEN 1 AND 9007199254740991),
    request_sequence INTEGER NOT NULL CHECK (request_sequence >= 1),
    created_at_unix_micro INTEGER NOT NULL,
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
            ON DELETE CASCADE
) STRICT, WITHOUT ROWID;
CREATE TABLE knowledge_lookup_asset_tenant_ledgers (
    tenant_id TEXT PRIMARY KEY NOT NULL COLLATE BINARY,
    staged_asset_count INTEGER NOT NULL DEFAULT 0 CHECK (
        staged_asset_count BETWEEN 0 AND 64
    ),
    asset_identity_count INTEGER NOT NULL DEFAULT 0 CHECK (
        asset_identity_count BETWEEN 0 AND 2048
    ),
    published_version_count INTEGER NOT NULL DEFAULT 0 CHECK (
        published_version_count BETWEEN 0 AND 8192
    ),
    stored_content_bytes INTEGER NOT NULL DEFAULT 0 CHECK (
        stored_content_bytes BETWEEN 0 AND 2147483648
    ),
    FOREIGN KEY (tenant_id) REFERENCES knowledge_catalog_tenants (tenant_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT
) STRICT, WITHOUT ROWID;
CREATE TABLE knowledge_lookup_asset_stages (
    tenant_id TEXT NOT NULL COLLATE BINARY,
    stage_id TEXT NOT NULL COLLATE BINARY,
    owner_id TEXT NOT NULL COLLATE BINARY,
    source_sha256 BLOB NOT NULL CHECK (length(source_sha256) = 32),
    content_sha256 BLOB NOT NULL CHECK (length(content_sha256) = 32),
    source_bytes INTEGER NOT NULL CHECK (
        source_bytes BETWEEN 1 AND 8388608
    ),
    canonical_csv BLOB NOT NULL,
    canonical_bytes INTEGER NOT NULL CHECK (
        canonical_bytes BETWEEN 1 AND 8388608
        AND canonical_bytes = length(canonical_csv)
    ),
    row_count INTEGER NOT NULL CHECK (row_count BETWEEN 0 AND 100000),
    column_count INTEGER NOT NULL CHECK (column_count BETWEEN 1 AND 64),
    created_at_unix_micro INTEGER NOT NULL CHECK (
        created_at_unix_micro BETWEEN 1 AND 253402300799999999
    ),
    expires_at_unix_micro INTEGER NOT NULL CHECK (
        expires_at_unix_micro BETWEEN 2 AND 253402300799999999
        AND expires_at_unix_micro > created_at_unix_micro
    ),
    PRIMARY KEY (tenant_id, stage_id),
    CONSTRAINT knowledge_lookup_asset_stage_id_bounded CHECK (
        length(CAST(stage_id AS BLOB)) BETWEEN 1 AND 128
        AND instr(CAST(stage_id AS BLOB), X'00') = 0
        AND stage_id = trim(stage_id)
        AND stage_id NOT GLOB (
            '*[' || char(1) || '-' || char(31)
            || char(127) || '-' || char(159) || ']*'
        )
    ),
    CONSTRAINT knowledge_lookup_asset_stage_owner_bounded CHECK (
        length(CAST(owner_id AS BLOB)) BETWEEN 1 AND 255
        AND instr(CAST(owner_id AS BLOB), X'00') = 0
        AND owner_id = trim(owner_id)
        AND owner_id NOT GLOB (
            '*[' || char(1) || '-' || char(31)
            || char(127) || '-' || char(159) || ']*'
        )
    ),
    FOREIGN KEY (tenant_id)
        REFERENCES knowledge_lookup_asset_tenant_ledgers (tenant_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT
) STRICT, WITHOUT ROWID;
CREATE TABLE knowledge_lookup_assets (
    tenant_id TEXT NOT NULL COLLATE BINARY,
    lookup_asset_id TEXT NOT NULL COLLATE BINARY,
    current_version INTEGER NOT NULL CHECK (
        current_version BETWEEN 1 AND 9223372036854775807
    ),
    created_at_unix_micro INTEGER NOT NULL CHECK (
        created_at_unix_micro BETWEEN 1 AND 253402300799999999
    ),
    updated_at_unix_micro INTEGER NOT NULL CHECK (
        updated_at_unix_micro BETWEEN created_at_unix_micro AND 253402300799999999
    ),
    PRIMARY KEY (tenant_id, lookup_asset_id),
    UNIQUE (tenant_id, lookup_asset_id, current_version),
    CONSTRAINT knowledge_lookup_asset_id_bounded CHECK (
        length(CAST(lookup_asset_id AS BLOB)) BETWEEN 1 AND 128
        AND instr(CAST(lookup_asset_id AS BLOB), X'00') = 0
        AND lookup_asset_id = trim(lookup_asset_id)
        AND lookup_asset_id NOT GLOB (
            '*[' || char(1) || '-' || char(31)
            || char(127) || '-' || char(159) || ']*'
        )
    ),
    FOREIGN KEY (tenant_id)
        REFERENCES knowledge_lookup_asset_tenant_ledgers (tenant_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
    FOREIGN KEY (tenant_id, lookup_asset_id, current_version)
        REFERENCES knowledge_lookup_asset_versions (
            tenant_id, lookup_asset_id, asset_version
        ) ON UPDATE RESTRICT ON DELETE RESTRICT
        DEFERRABLE INITIALLY DEFERRED
) STRICT, WITHOUT ROWID;
CREATE TABLE knowledge_lookup_asset_versions (
    tenant_id TEXT NOT NULL COLLATE BINARY,
    lookup_asset_id TEXT NOT NULL COLLATE BINARY,
    asset_version INTEGER NOT NULL CHECK (
        asset_version BETWEEN 1 AND 9223372036854775807
    ),
    source_sha256 BLOB NOT NULL CHECK (length(source_sha256) = 32),
    content_sha256 BLOB NOT NULL CHECK (length(content_sha256) = 32),
    source_bytes INTEGER NOT NULL CHECK (
        source_bytes BETWEEN 1 AND 8388608
    ),
    canonical_csv BLOB NOT NULL,
    canonical_bytes INTEGER NOT NULL CHECK (
        canonical_bytes BETWEEN 1 AND 8388608
        AND canonical_bytes = length(canonical_csv)
    ),
    row_count INTEGER NOT NULL CHECK (row_count BETWEEN 0 AND 100000),
    column_count INTEGER NOT NULL CHECK (column_count BETWEEN 1 AND 64),
    created_at_unix_micro INTEGER NOT NULL CHECK (
        created_at_unix_micro BETWEEN 1 AND 253402300799999999
    ),
    PRIMARY KEY (tenant_id, lookup_asset_id, asset_version),
    UNIQUE (
        tenant_id, lookup_asset_id, asset_version,
        content_sha256, canonical_bytes
    ),
    FOREIGN KEY (tenant_id, lookup_asset_id)
        REFERENCES knowledge_lookup_assets (tenant_id, lookup_asset_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT
        DEFERRABLE INITIALLY DEFERRED
) STRICT, WITHOUT ROWID;
CREATE TABLE knowledge_lookup_definitions (
    tenant_id TEXT NOT NULL,
    lookup_id TEXT NOT NULL,
    owner_id TEXT NOT NULL,
    app_id TEXT NOT NULL,
    name TEXT NOT NULL,
    sharing_scope INTEGER NOT NULL CHECK (sharing_scope IN (1, 2, 3)),
    automatic INTEGER NOT NULL CHECK (automatic IN (0, 1)),
    current_version INTEGER NOT NULL CHECK (
        current_version BETWEEN 1 AND 9223372036854775807
    ),
    state TEXT NOT NULL CHECK (state IN ('ACTIVE', 'DISABLED', 'DELETED')),
    created_at_unix_micro INTEGER NOT NULL CHECK (
        created_at_unix_micro BETWEEN 1 AND 253402300799999999
    ),
    updated_at_unix_micro INTEGER NOT NULL CHECK (
        updated_at_unix_micro BETWEEN created_at_unix_micro AND 253402300799999999
    ),
    disabled_at_unix_micro INTEGER CHECK (
        disabled_at_unix_micro IS NULL OR disabled_at_unix_micro
            BETWEEN created_at_unix_micro AND updated_at_unix_micro
    ),
    deleted_at_unix_micro INTEGER CHECK (
        deleted_at_unix_micro IS NULL OR deleted_at_unix_micro
            BETWEEN created_at_unix_micro AND updated_at_unix_micro
    ),
    PRIMARY KEY (tenant_id, lookup_id),
    UNIQUE (tenant_id, lookup_id, current_version),
    CHECK (length(tenant_id) BETWEEN 1 AND 255),
    CHECK (length(lookup_id) BETWEEN 1 AND 128),
    CHECK (length(owner_id) BETWEEN 1 AND 255),
    CHECK (length(app_id) BETWEEN 1 AND 128),
    CHECK (length(name) BETWEEN 1 AND 255),
    CHECK (
        (state = 'ACTIVE' AND disabled_at_unix_micro IS NULL AND deleted_at_unix_micro IS NULL) OR
        (state = 'DISABLED' AND disabled_at_unix_micro IS NOT NULL AND deleted_at_unix_micro IS NULL) OR
        (
            state = 'DELETED'
            AND disabled_at_unix_micro IS NOT NULL
            AND deleted_at_unix_micro IS NOT NULL
            AND deleted_at_unix_micro >= disabled_at_unix_micro
        )
    ),
    FOREIGN KEY (tenant_id, lookup_id, current_version)
        REFERENCES knowledge_lookup_definition_versions (
            tenant_id, lookup_id, definition_version
        ) ON UPDATE RESTRICT ON DELETE RESTRICT
        DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (tenant_id, app_id)
        REFERENCES app_workspaces (tenant_id, app_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT
) STRICT;
CREATE TABLE knowledge_lookup_definition_versions (
    tenant_id TEXT NOT NULL,
    lookup_id TEXT NOT NULL,
    definition_version INTEGER NOT NULL CHECK (
        definition_version BETWEEN 1 AND 9223372036854775807
    ),
    lookup_asset_id TEXT NOT NULL,
    asset_version INTEGER NOT NULL CHECK (asset_version >= 1),
    asset_size_bytes INTEGER NOT NULL CHECK (asset_size_bytes BETWEEN 1 AND 8388608),
    asset_content_sha256 BLOB NOT NULL CHECK (length(asset_content_sha256) = 32),
    definition_proto BLOB NOT NULL CHECK (length(definition_proto) BETWEEN 1 AND 65536),
    columns_blob BLOB NOT NULL CHECK (length(columns_blob) BETWEEN 9 AND 16580),
    mutation_kind TEXT NOT NULL CHECK (
        mutation_kind IN ('CREATE', 'REPLACE', 'ENABLE', 'DISABLE', 'DELETE')
    ),
    state TEXT NOT NULL CHECK (state IN ('ACTIVE', 'DISABLED', 'DELETED')),
    disabled_at_unix_micro INTEGER CHECK (
        disabled_at_unix_micro IS NULL OR disabled_at_unix_micro
            BETWEEN 1 AND created_at_unix_micro
    ),
    deleted_at_unix_micro INTEGER CHECK (
        deleted_at_unix_micro IS NULL OR deleted_at_unix_micro
            BETWEEN 1 AND created_at_unix_micro
    ),
    created_at_unix_micro INTEGER NOT NULL CHECK (
        created_at_unix_micro BETWEEN 1 AND 253402300799999999
    ),
    PRIMARY KEY (tenant_id, lookup_id, definition_version),
    FOREIGN KEY (tenant_id, lookup_id)
        REFERENCES knowledge_lookup_definitions (tenant_id, lookup_id)
        ON DELETE RESTRICT,
    FOREIGN KEY (
        tenant_id, lookup_asset_id, asset_version,
        asset_content_sha256, asset_size_bytes
    ) REFERENCES knowledge_lookup_asset_versions (
        tenant_id, lookup_asset_id, asset_version,
        content_sha256, canonical_bytes
    ) ON UPDATE RESTRICT ON DELETE RESTRICT,
    CHECK (
        (
            mutation_kind = 'CREATE'
            AND state = 'ACTIVE'
            AND disabled_at_unix_micro IS NULL
            AND deleted_at_unix_micro IS NULL
        )
        OR (
            mutation_kind = 'REPLACE'
            AND (
                (
                    state = 'ACTIVE'
                    AND disabled_at_unix_micro IS NULL
                    AND deleted_at_unix_micro IS NULL
                )
                OR (
                    state = 'DISABLED'
                    AND disabled_at_unix_micro IS NOT NULL
                    AND deleted_at_unix_micro IS NULL
                )
            )
        )
        OR (
            mutation_kind = 'ENABLE'
            AND state = 'ACTIVE'
            AND disabled_at_unix_micro IS NULL
            AND deleted_at_unix_micro IS NULL
        )
        OR (
            mutation_kind = 'DISABLE'
            AND state = 'DISABLED'
            AND disabled_at_unix_micro = created_at_unix_micro
            AND deleted_at_unix_micro IS NULL
        )
        OR (
            mutation_kind = 'DELETE'
            AND state = 'DELETED'
            AND disabled_at_unix_micro IS NOT NULL
            AND deleted_at_unix_micro = created_at_unix_micro
            AND deleted_at_unix_micro >= disabled_at_unix_micro
        )
    )
) STRICT;
CREATE TRIGGER indexes_name_is_immutable
BEFORE UPDATE OF name ON indexes
WHEN NEW.name <> OLD.name
BEGIN
    SELECT RAISE(ABORT, 'index name is immutable');
END;
CREATE TRIGGER ingestion_token_digest_is_immutable
BEFORE UPDATE OF token_digest ON ingestion_tokens
WHEN NEW.token_digest <> OLD.token_digest
BEGIN
    SELECT RAISE(ABORT, 'ingestion token digest is immutable');
END;
CREATE TRIGGER revoked_ingestion_token_is_irreversible
BEFORE UPDATE OF state ON ingestion_tokens
WHEN OLD.state = 'revoked' AND NEW.state <> 'revoked'
BEGIN
    SELECT RAISE(ABORT, 'revoked ingestion token cannot be reactivated');
END;
CREATE TRIGGER indexes_retention_is_millisecond_aligned_on_insert
BEFORE INSERT ON indexes
WHEN NEW.retention_nanoseconds % 1000000 <> 0
BEGIN
    SELECT RAISE(ABORT, 'index retention must use whole milliseconds');
END;
CREATE TRIGGER indexes_retention_is_millisecond_aligned_on_update
BEFORE UPDATE OF retention_nanoseconds ON indexes
WHEN NEW.retention_nanoseconds % 1000000 <> 0
BEGIN
    SELECT RAISE(ABORT, 'index retention must use whole milliseconds');
END;
CREATE TRIGGER app_workspaces_identity_is_immutable
BEFORE UPDATE OF app_id, tenant_id, slug ON app_workspaces
WHEN
    NEW.app_id <> OLD.app_id
    OR NEW.tenant_id <> OLD.tenant_id
    OR NEW.slug <> OLD.slug
BEGIN
    SELECT RAISE(ABORT, 'app workspace identity is immutable');
END;
CREATE TRIGGER active_app_workspace_cannot_be_deleted
BEFORE DELETE ON app_workspaces
WHEN OLD.state <> 'archived'
BEGIN
    SELECT RAISE(ABORT, 'app workspace must be archived before deletion');
END;
CREATE TRIGGER app_workspace_id_cannot_adopt_legacy_saved_search_namespace
BEFORE INSERT ON app_workspaces
WHEN EXISTS (
    SELECT 1 FROM saved_searches WHERE app_id = NEW.app_id
)
BEGIN
    SELECT RAISE(ABORT, 'app ID is already a legacy saved-search namespace');
END;
CREATE TRIGGER app_default_indexes_require_searchable_index_insert
BEFORE INSERT ON app_default_indexes
WHEN NOT EXISTS (
    SELECT 1
    FROM indexes
    WHERE index_id = NEW.index_id
      AND state = 'active'
      AND search_enabled = 1
)
BEGIN
    SELECT RAISE(ABORT, 'app default index is not searchable');
END;
CREATE TRIGGER app_default_indexes_require_searchable_index_update
BEFORE UPDATE OF tenant_id, app_id, index_id ON app_default_indexes
WHEN NOT EXISTS (
    SELECT 1
    FROM indexes
    WHERE index_id = NEW.index_id
      AND state = 'active'
      AND search_enabled = 1
)
BEGIN
    SELECT RAISE(ABORT, 'app default index is not searchable');
END;
CREATE TRIGGER active_app_default_indexes_remain_searchable
BEFORE UPDATE OF state, search_enabled ON indexes
WHEN
    (NEW.state <> 'active' OR NEW.search_enabled <> 1)
    AND EXISTS (
        SELECT 1
        FROM app_default_indexes AS app_index
        JOIN app_workspaces AS app
          ON app.tenant_id = app_index.tenant_id
         AND app.app_id = app_index.app_id
        WHERE app_index.index_id = OLD.index_id
          AND app.state = 'active'
    )
BEGIN
    SELECT RAISE(ABORT, 'active app requires searchable index');
END;
CREATE TRIGGER reactivated_app_requires_searchable_indexes
BEFORE UPDATE OF state ON app_workspaces
WHEN
    NEW.state = 'active'
    AND EXISTS (
        SELECT 1
        FROM app_default_indexes AS app_index
        LEFT JOIN indexes AS search_index
          ON search_index.index_id = app_index.index_id
        WHERE app_index.tenant_id = OLD.tenant_id
          AND app_index.app_id = OLD.app_id
          AND (
              search_index.index_id IS NULL
              OR search_index.state <> 'active'
              OR search_index.search_enabled <> 1
          )
    )
BEGIN
    SELECT RAISE(ABORT, 'reactivated app requires searchable indexes');
END;
CREATE TRIGGER canonical_saved_search_app_exists_insert
BEFORE INSERT ON saved_searches
WHEN
    length(NEW.app_id) = 26
    AND substr(NEW.app_id, 1, 4) = 'app_'
    AND substr(NEW.app_id, 5) NOT GLOB '*[^A-Za-z0-9_-]*'
    AND substr(NEW.app_id, 26, 1) GLOB '[AQgw]'
    AND NOT EXISTS (
        SELECT 1 FROM app_workspaces WHERE app_id = NEW.app_id
    )
    AND NOT EXISTS (
        SELECT 1 FROM saved_searches WHERE app_id = NEW.app_id
    )
BEGIN
    SELECT RAISE(ABORT, 'canonical saved-search app does not exist');
END;
CREATE TRIGGER canonical_saved_search_app_exists_update
BEFORE UPDATE OF app_id ON saved_searches
WHEN
    length(NEW.app_id) = 26
    AND substr(NEW.app_id, 1, 4) = 'app_'
    AND substr(NEW.app_id, 5) NOT GLOB '*[^A-Za-z0-9_-]*'
    AND substr(NEW.app_id, 26, 1) GLOB '[AQgw]'
    AND NOT EXISTS (
        SELECT 1 FROM app_workspaces WHERE app_id = NEW.app_id
    )
    AND NOT EXISTS (
        SELECT 1 FROM saved_searches WHERE app_id = NEW.app_id
    )
BEGIN
    SELECT RAISE(ABORT, 'canonical saved-search app does not exist');
END;
CREATE TRIGGER referenced_app_workspace_cannot_be_deleted
BEFORE DELETE ON app_workspaces
WHEN EXISTS (
    SELECT 1 FROM saved_searches WHERE app_id = OLD.app_id
)
BEGIN
    SELECT RAISE(ABORT, 'app workspace is referenced by saved searches');
END;
CREATE TRIGGER ingestion_token_collector_binding_is_immutable
BEFORE UPDATE OF bound_collector_id ON ingestion_tokens
WHEN OLD.bound_collector_id IS NOT NULL
     AND (
         NEW.bound_collector_id IS NULL
         OR NEW.bound_collector_id <> OLD.bound_collector_id
     )
BEGIN
    SELECT RAISE(ABORT, 'ingestion token collector binding is immutable');
END;
CREATE TRIGGER collector_catalog_revision_marker_is_undeletable
BEFORE DELETE ON collector_catalog_revisions
BEGIN
    SELECT RAISE(
        ABORT,
        'collector catalog fleet/runtime revision marker cannot be deleted'
    );
END;
CREATE TRIGGER collector_fleet_identity_is_immutable
BEFORE UPDATE OF tenant_id, collector_id ON collector_fleet
WHEN
    NEW.tenant_id <> OLD.tenant_id
    OR NEW.collector_id <> OLD.collector_id
BEGIN
    SELECT RAISE(ABORT, 'collector fleet identity is immutable');
END;
CREATE TRIGGER collector_fleet_admin_version_is_monotonic
BEFORE UPDATE OF admin_version ON collector_fleet
WHEN NEW.admin_version < OLD.admin_version
BEGIN
    SELECT RAISE(ABORT, 'collector administrator version cannot move backward');
END;
CREATE TRIGGER collector_catalog_revision_after_fleet_insert
AFTER INSERT ON collector_fleet
BEGIN
    INSERT INTO collector_catalog_revisions (
        tenant_id,
        revision,
        fleet_count,
        runtime_count
    )
    VALUES (NEW.tenant_id, 1, 1, 0)
    ON CONFLICT (tenant_id) DO UPDATE SET
        revision = CASE
            WHEN collector_catalog_revisions.revision
                    = 9223372036854775807
                THEN RAISE(ABORT, 'collector catalog revision exhausted')
            ELSE collector_catalog_revisions.revision + 1
        END,
        fleet_count = CASE
            WHEN collector_catalog_revisions.fleet_count = 256
                THEN RAISE(ABORT, 'collector fleet capacity exhausted')
            ELSE collector_catalog_revisions.fleet_count + 1
        END;
END;
CREATE TRIGGER collector_catalog_revision_after_fleet_update
AFTER UPDATE ON collector_fleet
BEGIN
    UPDATE collector_catalog_revisions
    SET revision = CASE
        WHEN revision = 9223372036854775807
            THEN RAISE(ABORT, 'collector catalog revision exhausted')
        ELSE revision + 1
    END
    WHERE tenant_id = NEW.tenant_id;
    SELECT CASE
        WHEN changes() <> 1
            THEN RAISE(ABORT, 'collector catalog fleet/runtime revision is missing')
    END;
END;
CREATE TRIGGER collector_catalog_revision_after_fleet_delete
AFTER DELETE ON collector_fleet
BEGIN
    UPDATE collector_catalog_revisions
    SET
        revision = CASE
            WHEN revision = 9223372036854775807
                THEN RAISE(ABORT, 'collector catalog revision exhausted')
            ELSE revision + 1
        END,
        fleet_count = CASE
            WHEN fleet_count = 0
                THEN RAISE(ABORT, 'collector catalog fleet count underflow')
            ELSE fleet_count - 1
        END
    WHERE tenant_id = OLD.tenant_id;
    SELECT CASE
        WHEN changes() <> 1
            THEN RAISE(ABORT, 'collector catalog fleet/runtime revision is missing')
    END;
END;
CREATE TRIGGER collector_catalog_revision_before_fleet_insert
BEFORE INSERT ON collector_fleet
WHEN
    NOT EXISTS (
        SELECT 1
        FROM collector_catalog_revisions
        WHERE tenant_id = NEW.tenant_id
    )
    AND (
        EXISTS (
            SELECT 1
            FROM collector_fleet
            WHERE tenant_id = NEW.tenant_id
        )
        OR EXISTS (
            SELECT 1
            FROM collector_runtime
            WHERE tenant_id = NEW.tenant_id
        )
    )
BEGIN
    SELECT RAISE(
        ABORT,
        'collector catalog fleet/runtime revision is missing'
    );
END;
CREATE TRIGGER collector_runtime_identity_is_immutable
BEFORE UPDATE OF tenant_id, collector_id ON collector_runtime
WHEN
    NEW.tenant_id <> OLD.tenant_id
    OR NEW.collector_id <> OLD.collector_id
BEGIN
    SELECT RAISE(ABORT, 'collector runtime identity is immutable');
END;
CREATE TRIGGER collector_runtime_revisions_are_monotonic
BEFORE UPDATE OF telemetry_revision, lease_generation ON collector_runtime
WHEN
    NEW.telemetry_revision < OLD.telemetry_revision
    OR NEW.lease_generation < OLD.lease_generation
BEGIN
    SELECT RAISE(ABORT, 'collector runtime revisions cannot move backward');
END;
CREATE TRIGGER collector_catalog_revision_after_runtime_insert
AFTER INSERT ON collector_runtime
BEGIN
    UPDATE collector_catalog_revisions
    SET
        revision = CASE
            WHEN revision = 9223372036854775807
                THEN RAISE(ABORT, 'collector catalog revision exhausted')
            ELSE revision + 1
        END,
        runtime_count = CASE
            WHEN runtime_count = 256
                THEN RAISE(ABORT, 'collector runtime capacity exhausted')
            ELSE runtime_count + 1
        END
    WHERE tenant_id = NEW.tenant_id;
    SELECT CASE
        WHEN changes() <> 1
            THEN RAISE(ABORT, 'collector catalog fleet/runtime revision is missing')
    END;
END;
CREATE TRIGGER collector_catalog_revision_after_runtime_update
AFTER UPDATE ON collector_runtime
BEGIN
    UPDATE collector_catalog_revisions
    SET revision = CASE
        WHEN revision = 9223372036854775807
            THEN RAISE(ABORT, 'collector catalog revision exhausted')
        ELSE revision + 1
    END
    WHERE tenant_id = NEW.tenant_id;
    SELECT CASE
        WHEN changes() <> 1
            THEN RAISE(ABORT, 'collector catalog fleet/runtime revision is missing')
    END;
END;
CREATE TRIGGER collector_catalog_revision_after_runtime_delete
AFTER DELETE ON collector_runtime
BEGIN
    UPDATE collector_catalog_revisions
    SET
        revision = CASE
            WHEN revision = 9223372036854775807
                THEN RAISE(ABORT, 'collector catalog revision exhausted')
            ELSE revision + 1
        END,
        runtime_count = CASE
            WHEN runtime_count = 0
                THEN RAISE(ABORT, 'collector catalog runtime count underflow')
            ELSE runtime_count - 1
        END
    WHERE tenant_id = OLD.tenant_id;
    SELECT CASE
        WHEN changes() <> 1
            THEN RAISE(ABORT, 'collector catalog fleet/runtime revision is missing')
    END;
END;
CREATE TRIGGER search_history_owner_count_after_insert
AFTER INSERT ON search_history
BEGIN
    INSERT INTO search_history_owner_counts (
        tenant_id,
        owner_id,
        terminal_count
    ) VALUES (
        NEW.tenant_id,
        NEW.owner_id,
        1
    )
    ON CONFLICT (tenant_id, owner_id) DO UPDATE
    SET terminal_count = terminal_count + 1;
END;
CREATE TRIGGER search_history_owner_count_after_delete
AFTER DELETE ON search_history
BEGIN
    SELECT CASE
        WHEN NOT EXISTS (
            SELECT 1
            FROM search_history_owner_counts
            WHERE tenant_id = OLD.tenant_id
              AND owner_id = OLD.owner_id
        )
        THEN RAISE(ABORT, 'search-history owner count is missing')
    END;

    DELETE FROM search_history_owner_counts
    WHERE tenant_id = OLD.tenant_id
      AND owner_id = OLD.owner_id
      AND terminal_count = 1;

    UPDATE search_history_owner_counts
    SET terminal_count = terminal_count - 1
    WHERE tenant_id = OLD.tenant_id
      AND owner_id = OLD.owner_id;
END;
CREATE TRIGGER tombstoned_index_update_is_forbidden
BEFORE UPDATE ON indexes
WHEN EXISTS (
    SELECT 1
    FROM index_deletion_tombstones
    WHERE index_deletion_tombstones.index_id = OLD.index_id
)
BEGIN
    SELECT RAISE(ABORT, 'tombstoned index is immutable');
END;
CREATE TRIGGER tombstoned_index_delete_is_forbidden
BEFORE DELETE ON indexes
WHEN EXISTS (
    SELECT 1
    FROM index_deletion_tombstones
    WHERE index_deletion_tombstones.index_id = OLD.index_id
)
BEGIN
    SELECT RAISE(ABORT, 'tombstoned index cannot be deleted');
END;
CREATE TRIGGER index_deletion_tombstone_update_is_forbidden
BEFORE UPDATE ON index_deletion_tombstones
BEGIN
    SELECT RAISE(ABORT, 'index deletion tombstone is immutable');
END;
CREATE TRIGGER index_deletion_tombstone_delete_is_forbidden
BEFORE DELETE ON index_deletion_tombstones
BEGIN
    SELECT RAISE(ABORT, 'index deletion tombstone cannot be deleted');
END;
CREATE TRIGGER deleting_index_insert_is_forbidden
BEFORE INSERT ON indexes
WHEN NEW.state = 'deleting'
BEGIN
    SELECT RAISE(
        ABORT,
        'deleting index creation requires a deletion operation'
    );
END;
CREATE TRIGGER index_deletion_operation_insert_is_valid
BEFORE INSERT ON index_deletion_operations
WHEN NOT EXISTS (
    SELECT 1
    FROM indexes
    WHERE indexes.index_id = NEW.index_id
      AND indexes.name = NEW.index_name
      AND indexes.version = NEW.archived_index_version
      AND indexes.state = 'archived'
      AND indexes.updated_at_unix_micro <= NEW.created_at_unix_micro
      AND NOT EXISTS (
          SELECT 1
          FROM index_deletion_tombstones
          WHERE index_deletion_tombstones.index_id = indexes.index_id
      )
)
BEGIN
    SELECT RAISE(
        ABORT,
        'index deletion operation must match an archived index generation'
    );
END;
CREATE TRIGGER index_deletion_operation_identity_collision_is_forbidden
BEFORE INSERT ON index_deletion_operations
WHEN EXISTS (
    SELECT 1
    FROM index_deletion_operations
    WHERE deletion_operation_id = NEW.deletion_operation_id
       OR index_id = NEW.index_id
)
BEGIN
    SELECT RAISE(
        ABORT,
        'index deletion operation identity is already in use'
    );
END;
CREATE TRIGGER index_deleting_transition_requires_operation
BEFORE UPDATE OF state ON indexes
WHEN OLD.state <> 'deleting'
 AND NEW.state = 'deleting'
 AND NOT EXISTS (
     SELECT 1
     FROM index_deletion_operations
     WHERE index_deletion_operations.index_id = OLD.index_id
       AND index_deletion_operations.index_name = OLD.name
       AND index_deletion_operations.archived_index_version = OLD.version
       AND NEW.version = OLD.version + 1
       AND NEW.updated_at_unix_micro =
           index_deletion_operations.created_at_unix_micro
 )
BEGIN
    SELECT RAISE(
        ABORT,
        'deleting index transition requires a matching deletion operation'
    );
END;
CREATE TRIGGER index_deletion_operation_insert_starts_deletion
AFTER INSERT ON index_deletion_operations
BEGIN
    UPDATE indexes
    SET state = 'deleting',
        version = version + 1,
        updated_at_unix_micro = NEW.created_at_unix_micro
    WHERE index_id = NEW.index_id
      AND name = NEW.index_name
      AND version = NEW.archived_index_version
      AND state = 'archived';

    SELECT CASE
        WHEN changes() <> 1
        THEN RAISE(ABORT, 'index deletion operation transition failed')
    END;
END;
CREATE TRIGGER deleting_index_with_operation_update_is_forbidden
BEFORE UPDATE ON indexes
WHEN OLD.state = 'deleting'
 AND EXISTS (
     SELECT 1
     FROM index_deletion_operations
     WHERE index_deletion_operations.index_id = OLD.index_id
 )
BEGIN
    SELECT RAISE(ABORT, 'deleting index is coordinator-owned');
END;
CREATE TRIGGER deleting_index_with_operation_delete_is_forbidden
BEFORE DELETE ON indexes
WHEN EXISTS (
    SELECT 1
    FROM index_deletion_operations
    WHERE index_deletion_operations.index_id = OLD.index_id
)
BEGIN
    SELECT RAISE(ABORT, 'deleting index cannot be deleted');
END;
CREATE TRIGGER index_deletion_operation_update_is_forbidden
BEFORE UPDATE ON index_deletion_operations
BEGIN
    SELECT RAISE(ABORT, 'index deletion operation is immutable');
END;
CREATE TRIGGER index_deletion_mutation_attempt_identity_collision_is_forbidden
BEFORE INSERT ON index_deletion_mutation_attempts
WHEN EXISTS (
    SELECT 1
    FROM index_deletion_mutation_attempts
    WHERE deletion_operation_id = NEW.deletion_operation_id
       OR correlation_id = NEW.correlation_id
)
BEGIN
    SELECT RAISE(
        ABORT,
        'index deletion mutation attempt identity is already in use'
    );
END;
CREATE TRIGGER index_deletion_mutation_attempt_insert_is_valid
BEFORE INSERT ON index_deletion_mutation_attempts
WHEN NOT EXISTS (
    SELECT 1
    FROM index_deletion_operations AS deletion_operation
    JOIN indexes AS target_index
      ON target_index.index_id = deletion_operation.index_id
     AND target_index.name = deletion_operation.index_name
     AND target_index.version =
         deletion_operation.archived_index_version + 1
     AND target_index.state = 'deleting'
     AND target_index.updated_at_unix_micro =
         deletion_operation.created_at_unix_micro
    WHERE deletion_operation.deletion_operation_id =
          NEW.deletion_operation_id
      AND deletion_operation.tenant_id = NEW.tenant_id
      AND NEW.created_at_unix_micro >=
          deletion_operation.created_at_unix_micro
      AND NOT EXISTS (
          SELECT 1
          FROM index_deletion_tombstones
          WHERE index_deletion_tombstones.index_id =
                deletion_operation.index_id
      )
)
BEGIN
    SELECT RAISE(
        ABORT,
        'index deletion mutation attempt requires a deleting operation'
    );
END;
CREATE TRIGGER index_deletion_mutation_attempt_update_is_forbidden
BEFORE UPDATE ON index_deletion_mutation_attempts
BEGIN
    SELECT RAISE(ABORT, 'index deletion mutation attempt is immutable');
END;
CREATE TRIGGER index_data_deletion_completion_identity_collision_is_forbidden
BEFORE INSERT ON index_data_deletion_completions
WHEN EXISTS (
    SELECT 1
    FROM index_data_deletion_completions
    WHERE deletion_operation_id = NEW.deletion_operation_id
       OR correlation_id = NEW.correlation_id
       OR index_id = NEW.index_id
)
BEGIN
    SELECT RAISE(
        ABORT,
        'index data deletion completion identity is already in use'
    );
END;
CREATE TRIGGER index_data_deletion_completion_insert_is_valid
BEFORE INSERT ON index_data_deletion_completions
WHEN NOT EXISTS (
    SELECT 1
    FROM index_deletion_operations AS deletion_operation
    JOIN index_deletion_mutation_attempts AS mutation_attempt
      ON mutation_attempt.deletion_operation_id =
         deletion_operation.deletion_operation_id
    JOIN indexes AS target_index
      ON target_index.index_id = deletion_operation.index_id
     AND target_index.name = deletion_operation.index_name
     AND target_index.version =
         deletion_operation.archived_index_version + 1
     AND target_index.state = 'deleting'
     AND target_index.updated_at_unix_micro =
         deletion_operation.created_at_unix_micro
    WHERE deletion_operation.deletion_operation_id =
          NEW.deletion_operation_id
      AND deletion_operation.index_id = NEW.index_id
      AND deletion_operation.index_name = NEW.index_name
      AND deletion_operation.tenant_id = NEW.tenant_id
      AND deletion_operation.archived_index_version =
          NEW.archived_index_version
      AND deletion_operation.created_at_unix_micro =
          NEW.operation_created_at_unix_micro
      AND NEW.deleting_index_version =
          deletion_operation.archived_index_version + 1
      AND mutation_attempt.correlation_id = NEW.correlation_id
      AND mutation_attempt.tenant_id = NEW.tenant_id
      AND mutation_attempt.clickhouse_database =
          NEW.clickhouse_database
      AND mutation_attempt.clickhouse_table = NEW.clickhouse_table
      AND mutation_attempt.clickhouse_table_uuid =
          NEW.clickhouse_table_uuid
      AND mutation_attempt.protocol_version = NEW.protocol_version
      AND mutation_attempt.created_at_unix_micro =
          NEW.attempt_created_at_unix_micro
      AND NEW.completed_at_unix_micro >=
          mutation_attempt.created_at_unix_micro
      AND NOT EXISTS (
          SELECT 1
          FROM index_deletion_tombstones
          WHERE index_deletion_tombstones.index_id = NEW.index_id
      )
)
BEGIN
    SELECT RAISE(
        ABORT,
        'index data deletion completion must match outstanding work'
    );
END;
CREATE TRIGGER index_deletion_tombstone_identity_collision_is_forbidden
BEFORE INSERT ON index_deletion_tombstones
WHEN EXISTS (
    SELECT 1
    FROM index_deletion_tombstones
    WHERE index_id = NEW.index_id
)
BEGIN
    SELECT RAISE(
        ABORT,
        'index deletion tombstone identity is already in use'
    );
END;
CREATE TRIGGER index_deletion_tombstone_insert_is_valid
BEFORE INSERT ON index_deletion_tombstones
WHEN NOT EXISTS (
    SELECT 1
    FROM indexes AS target_index
    WHERE target_index.index_id = NEW.index_id
      AND target_index.name = NEW.name
      AND target_index.version = NEW.deleted_version
      AND (
          (
              target_index.state = 'archived'
              AND NOT EXISTS (
                  SELECT 1
                  FROM index_deletion_operations
                  WHERE index_deletion_operations.index_id =
                        target_index.index_id
              )
          )
          OR
          (
              target_index.state = 'deleting'
              AND EXISTS (
                  SELECT 1
                  FROM index_data_deletion_completions AS completion
                  WHERE completion.index_id = NEW.index_id
                    AND completion.index_name = NEW.name
                    AND completion.deleting_index_version =
                        NEW.deleted_version
                    AND completion.completed_at_unix_micro =
                        NEW.deleted_at_unix_micro
              )
          )
      )
)
BEGIN
    SELECT RAISE(
        ABORT,
        'index deletion tombstone must match a terminal index'
    );
END;
CREATE TRIGGER index_deletion_mutation_attempt_delete_is_forbidden
BEFORE DELETE ON index_deletion_mutation_attempts
WHEN EXISTS (
    SELECT 1
    FROM index_deletion_operations
    WHERE deletion_operation_id = OLD.deletion_operation_id
)
 AND NOT EXISTS (
     SELECT 1
     FROM index_data_deletion_completions
     WHERE deletion_operation_id = OLD.deletion_operation_id
       AND correlation_id = OLD.correlation_id
       AND tenant_id = OLD.tenant_id
       AND clickhouse_database = OLD.clickhouse_database
       AND clickhouse_table = OLD.clickhouse_table
       AND clickhouse_table_uuid = OLD.clickhouse_table_uuid
       AND protocol_version = OLD.protocol_version
       AND attempt_created_at_unix_micro =
           OLD.created_at_unix_micro
 )
BEGIN
    SELECT RAISE(
        ABORT,
        'index deletion mutation attempt cannot be deleted'
    );
END;
CREATE TRIGGER index_deletion_operation_delete_is_forbidden
BEFORE DELETE ON index_deletion_operations
WHEN NOT EXISTS (
    SELECT 1
    FROM index_data_deletion_completions AS completion
    JOIN index_deletion_tombstones AS tombstone
      ON tombstone.index_id = completion.index_id
     AND tombstone.name = completion.index_name
     AND tombstone.deleted_version =
         completion.deleting_index_version
     AND tombstone.deleted_at_unix_micro =
         completion.completed_at_unix_micro
    JOIN indexes AS target_index
      ON target_index.index_id = completion.index_id
     AND target_index.name = completion.index_name
     AND target_index.version = completion.deleting_index_version
     AND target_index.state = 'deleting'
     AND target_index.updated_at_unix_micro =
         completion.operation_created_at_unix_micro
    WHERE completion.deletion_operation_id =
          OLD.deletion_operation_id
      AND completion.index_id = OLD.index_id
      AND completion.index_name = OLD.index_name
      AND completion.tenant_id = OLD.tenant_id
      AND completion.archived_index_version =
          OLD.archived_index_version
      AND completion.operation_created_at_unix_micro =
          OLD.created_at_unix_micro
)
BEGIN
    SELECT RAISE(
        ABORT,
        'index deletion operation cannot be deleted'
    );
END;
CREATE TRIGGER index_data_deletion_completion_finishes_operation
AFTER INSERT ON index_data_deletion_completions
BEGIN
    INSERT INTO index_deletion_tombstones (
        index_id,
        name,
        deleted_version,
        deleted_at_unix_micro
    ) VALUES (
        NEW.index_id,
        NEW.index_name,
        NEW.deleting_index_version,
        NEW.completed_at_unix_micro
    );

    SELECT CASE
        WHEN changes() <> 1
        THEN RAISE(
            ABORT,
            'index data deletion completion tombstone failed'
        )
    END;

    DELETE FROM index_deletion_operations
    WHERE deletion_operation_id = NEW.deletion_operation_id
      AND index_id = NEW.index_id
      AND index_name = NEW.index_name
      AND tenant_id = NEW.tenant_id
      AND archived_index_version = NEW.archived_index_version
      AND created_at_unix_micro =
          NEW.operation_created_at_unix_micro;

    SELECT CASE
        WHEN changes() <> 1
        THEN RAISE(
            ABORT,
            'index data deletion completion operation cleanup failed'
        )
    END;

    SELECT CASE
        WHEN EXISTS (
            SELECT 1
            FROM index_deletion_mutation_attempts
            WHERE deletion_operation_id = NEW.deletion_operation_id
        )
        THEN RAISE(
            ABORT,
            'index data deletion completion attempt cleanup failed'
        )
    END;
END;
CREATE TRIGGER index_data_deletion_completion_update_is_forbidden
BEFORE UPDATE ON index_data_deletion_completions
BEGIN
    SELECT RAISE(ABORT, 'index data deletion completion is immutable');
END;
CREATE TRIGGER index_data_deletion_completion_delete_is_forbidden
BEFORE DELETE ON index_data_deletion_completions
BEGIN
    SELECT RAISE(
        ABORT,
        'index data deletion completion cannot be deleted'
    );
END;
CREATE TRIGGER index_deletion_operation_completed_identity_is_forbidden
BEFORE INSERT ON index_deletion_operations
WHEN EXISTS (
    SELECT 1
    FROM index_data_deletion_completions
    WHERE deletion_operation_id = NEW.deletion_operation_id
       OR index_id = NEW.index_id
)
BEGIN
    SELECT RAISE(
        ABORT,
        'completed index deletion operation identity is already in use'
    );
END;
CREATE TRIGGER index_deletion_mutation_completed_identity_is_forbidden
BEFORE INSERT ON index_deletion_mutation_attempts
WHEN EXISTS (
    SELECT 1
    FROM index_data_deletion_completions
    WHERE deletion_operation_id = NEW.deletion_operation_id
       OR correlation_id = NEW.correlation_id
)
BEGIN
    SELECT RAISE(
        ABORT,
        'completed index deletion mutation identity is already in use'
    );
END;
CREATE TRIGGER index_catalog_state_identity_is_immutable
BEFORE UPDATE OF singleton_id ON index_catalog_state
WHEN NEW.singleton_id <> OLD.singleton_id
BEGIN
    SELECT RAISE(ABORT, 'index catalog state identity is immutable');
END;
CREATE TRIGGER index_catalog_state_collision_is_forbidden
BEFORE INSERT ON index_catalog_state
WHEN EXISTS (
    SELECT 1
    FROM index_catalog_state
    WHERE singleton_id = NEW.singleton_id
)
BEGIN
    SELECT RAISE(ABORT, 'index catalog state already exists');
END;
CREATE TRIGGER index_catalog_state_transition_is_valid
BEFORE UPDATE OF revision, physical_count ON index_catalog_state
WHEN NOT (
    NEW.revision = OLD.revision + 1
    AND (
        NEW.physical_count = OLD.physical_count
        OR NEW.physical_count = OLD.physical_count + 1
    )
)
BEGIN
    SELECT RAISE(ABORT, 'index catalog state transition is invalid');
END;
CREATE TRIGGER index_catalog_state_delete_is_forbidden
BEFORE DELETE ON index_catalog_state
BEGIN
    SELECT RAISE(ABORT, 'index catalog state cannot be deleted');
END;
CREATE TRIGGER index_catalog_record_insert_is_bounded
BEFORE INSERT ON indexes
WHEN NOT (
    length(CAST(NEW.index_id AS BLOB)) BETWEEN 1 AND 128
    AND instr(CAST(NEW.index_id AS BLOB), X'00') = 0
    AND NEW.index_id = trim(NEW.index_id)
    AND NEW.index_id NOT GLOB (
        '*[' || char(1) || '-' || char(31)
        || char(127) || '-' || char(159) || ']*'
    )
    AND length(CAST(NEW.display_name AS BLOB)) BETWEEN 1 AND 255
    AND instr(CAST(NEW.display_name AS BLOB), X'00') = 0
    AND NEW.display_name = trim(NEW.display_name)
    AND NEW.display_name NOT GLOB (
        '*[' || char(1) || '-' || char(31)
        || char(127) || '-' || char(159) || ']*'
    )
    AND length(CAST(NEW.description AS BLOB)) BETWEEN 0 AND 8192
    AND instr(CAST(NEW.description AS BLOB), X'00') = 0
    AND NEW.description NOT GLOB (
        '*[' || char(1) || '-' || char(8)
        || char(11) || '-' || char(12)
        || char(14) || '-' || char(31)
        || char(127) || '-' || char(159) || ']*'
    )
    AND length(CAST(NEW.default_sourcetype AS BLOB)) BETWEEN 0 AND 255
    AND instr(CAST(NEW.default_sourcetype AS BLOB), X'00') = 0
    AND NEW.default_sourcetype = trim(NEW.default_sourcetype)
    AND NEW.default_sourcetype NOT GLOB (
        '*[' || char(1) || '-' || char(31)
        || char(127) || '-' || char(159) || ']*'
    )
    AND NEW.created_at_unix_micro BETWEEN 1 AND 253402300799999999
    AND NEW.updated_at_unix_micro BETWEEN
        NEW.created_at_unix_micro AND 253402300799999999
)
BEGIN
    SELECT RAISE(ABORT, 'index catalog record is invalid or unbounded');
END;
CREATE TRIGGER index_catalog_record_update_is_bounded
BEFORE UPDATE OF
    index_id,
    display_name,
    description,
    default_sourcetype,
    created_at_unix_micro,
    updated_at_unix_micro
ON indexes
WHEN NOT (
    length(CAST(NEW.index_id AS BLOB)) BETWEEN 1 AND 128
    AND instr(CAST(NEW.index_id AS BLOB), X'00') = 0
    AND NEW.index_id = trim(NEW.index_id)
    AND NEW.index_id NOT GLOB (
        '*[' || char(1) || '-' || char(31)
        || char(127) || '-' || char(159) || ']*'
    )
    AND length(CAST(NEW.display_name AS BLOB)) BETWEEN 1 AND 255
    AND instr(CAST(NEW.display_name AS BLOB), X'00') = 0
    AND NEW.display_name = trim(NEW.display_name)
    AND NEW.display_name NOT GLOB (
        '*[' || char(1) || '-' || char(31)
        || char(127) || '-' || char(159) || ']*'
    )
    AND length(CAST(NEW.description AS BLOB)) BETWEEN 0 AND 8192
    AND instr(CAST(NEW.description AS BLOB), X'00') = 0
    AND NEW.description NOT GLOB (
        '*[' || char(1) || '-' || char(8)
        || char(11) || '-' || char(12)
        || char(14) || '-' || char(31)
        || char(127) || '-' || char(159) || ']*'
    )
    AND length(CAST(NEW.default_sourcetype AS BLOB)) BETWEEN 0 AND 255
    AND instr(CAST(NEW.default_sourcetype AS BLOB), X'00') = 0
    AND NEW.default_sourcetype = trim(NEW.default_sourcetype)
    AND NEW.default_sourcetype NOT GLOB (
        '*[' || char(1) || '-' || char(31)
        || char(127) || '-' || char(159) || ']*'
    )
    AND NEW.created_at_unix_micro BETWEEN 1 AND 253402300799999999
    AND NEW.updated_at_unix_micro BETWEEN
        NEW.created_at_unix_micro AND 253402300799999999
)
BEGIN
    SELECT RAISE(ABORT, 'index catalog record is invalid or unbounded');
END;
CREATE TRIGGER indexes_id_is_immutable
BEFORE UPDATE OF index_id ON indexes
WHEN NEW.index_id <> OLD.index_id
BEGIN
    SELECT RAISE(ABORT, 'index ID is immutable');
END;
CREATE TRIGGER index_catalog_before_index_insert
BEFORE INSERT ON indexes
WHEN NOT EXISTS (
    SELECT 1
    FROM index_catalog_state
    WHERE singleton_id = 1
      AND revision BETWEEN 1 AND 9223372036854775806
      AND physical_count BETWEEN 0 AND 1023
)
BEGIN
    SELECT RAISE(
        ABORT,
        'index catalog state is invalid or capacity is exhausted'
    );
END;
CREATE TRIGGER index_catalog_identity_collision_is_forbidden
BEFORE INSERT ON indexes
WHEN EXISTS (
    SELECT 1
    FROM indexes
    WHERE index_id = NEW.index_id
       OR name = NEW.name
)
BEGIN
    SELECT RAISE(ABORT, 'index catalog identity is already in use');
END;
CREATE TRIGGER index_catalog_after_index_insert
AFTER INSERT ON indexes
BEGIN
    UPDATE index_catalog_state
    SET revision = revision + 1,
        physical_count = physical_count + 1
    WHERE singleton_id = 1;

    SELECT CASE
        WHEN changes() <> 1
        THEN RAISE(ABORT, 'index catalog insert accounting failed')
    END;
END;
CREATE TRIGGER index_catalog_before_index_update
BEFORE UPDATE ON indexes
WHEN NOT EXISTS (
    SELECT 1
    FROM index_catalog_state
    WHERE singleton_id = 1
      AND revision BETWEEN 1 AND 9223372036854775806
      AND physical_count BETWEEN 1 AND 1024
)
BEGIN
    SELECT RAISE(ABORT, 'index catalog state is invalid');
END;
CREATE TRIGGER index_catalog_after_index_update
AFTER UPDATE ON indexes
BEGIN
    UPDATE index_catalog_state
    SET revision = revision + 1
    WHERE singleton_id = 1;

    SELECT CASE
        WHEN changes() <> 1
        THEN RAISE(ABORT, 'index catalog update accounting failed')
    END;
END;
CREATE TRIGGER index_catalog_index_delete_is_forbidden
BEFORE DELETE ON indexes
BEGIN
    SELECT RAISE(ABORT, 'index catalog identity cannot be deleted');
END;
CREATE TRIGGER index_catalog_before_tombstone_insert
BEFORE INSERT ON index_deletion_tombstones
WHEN NOT EXISTS (
    SELECT 1
    FROM index_catalog_state
    WHERE singleton_id = 1
      AND revision BETWEEN 1 AND 9223372036854775806
      AND physical_count BETWEEN 1 AND 1024
)
BEGIN
    SELECT RAISE(ABORT, 'index catalog state is invalid');
END;
CREATE TRIGGER index_catalog_after_tombstone_insert
AFTER INSERT ON index_deletion_tombstones
BEGIN
    UPDATE index_catalog_state
    SET revision = revision + 1
    WHERE singleton_id = 1;

    SELECT CASE
        WHEN changes() <> 1
        THEN RAISE(ABORT, 'index catalog tombstone accounting failed')
    END;
END;
CREATE TRIGGER audit_tenant_state_identity_collision_is_forbidden
BEFORE INSERT ON audit_tenant_state
WHEN EXISTS (
    SELECT 1
    FROM audit_tenant_state
    WHERE tenant_id = NEW.tenant_id
)
BEGIN
    SELECT RAISE(ABORT, 'audit tenant state already exists');
END;
CREATE TRIGGER audit_tenant_state_initial_shape_is_valid
BEFORE INSERT ON audit_tenant_state
WHEN NEW.event_count <> 0 OR NEW.next_sequence <> 1
BEGIN
    SELECT RAISE(ABORT, 'audit tenant state must begin empty');
END;
CREATE TRIGGER audit_tenant_state_delete_is_forbidden
BEFORE DELETE ON audit_tenant_state
BEGIN
    SELECT RAISE(ABORT, 'audit tenant state cannot be deleted');
END;
CREATE TRIGGER search_attempt_audit_state_identity_collision_is_forbidden
BEFORE INSERT ON search_attempt_audit_tenant_state
WHEN EXISTS (
    SELECT 1
    FROM search_attempt_audit_tenant_state
    WHERE tenant_id = NEW.tenant_id
)
BEGIN
    SELECT RAISE(ABORT, 'search-attempt audit tenant state already exists');
END;
CREATE TRIGGER search_attempt_audit_state_initial_shape_is_valid
BEFORE INSERT ON search_attempt_audit_tenant_state
WHEN NEW.first_sequence <> 1
  OR NEW.next_sequence <> 1
  OR NEW.retained_count <> 0
BEGIN
    SELECT RAISE(ABORT, 'search-attempt audit tenant state must begin empty');
END;
CREATE TRIGGER search_attempt_audit_state_transition_is_valid
BEFORE UPDATE ON search_attempt_audit_tenant_state
WHEN NOT (
    NEW.tenant_id = OLD.tenant_id
    AND NEW.maximum_retained_attempts = OLD.maximum_retained_attempts
    AND (
        (
            OLD.retained_count BETWEEN 0 AND OLD.maximum_retained_attempts
            AND OLD.next_sequence BETWEEN 1 AND 9223372036854775806
            AND NEW.first_sequence = OLD.first_sequence
            AND NEW.next_sequence = OLD.next_sequence + 1
            AND NEW.retained_count = OLD.retained_count + 1
            AND EXISTS (
                SELECT 1
                FROM search_attempt_audit_events
                WHERE tenant_id = NEW.tenant_id
                  AND sequence = OLD.next_sequence
            )
        )
        OR (
            OLD.retained_count = OLD.maximum_retained_attempts + 1
            AND NEW.first_sequence = OLD.first_sequence + 1
            AND NEW.next_sequence = OLD.next_sequence
            AND NEW.retained_count = OLD.retained_count - 1
            AND NOT EXISTS (
                SELECT 1
                FROM search_attempt_audit_events
                WHERE tenant_id = NEW.tenant_id
                  AND sequence = OLD.first_sequence
            )
        )
    )
)
BEGIN
    SELECT RAISE(ABORT, 'search-attempt audit tenant state transition is invalid');
END;
CREATE TRIGGER search_attempt_audit_state_delete_is_forbidden
BEFORE DELETE ON search_attempt_audit_tenant_state
BEGIN
    SELECT RAISE(ABORT, 'search-attempt audit tenant state cannot be deleted');
END;
CREATE TRIGGER search_attempt_audit_event_identity_collision_is_forbidden
BEFORE INSERT ON search_attempt_audit_events
WHEN EXISTS (
    SELECT 1
    FROM search_attempt_audit_events
    WHERE tenant_id = NEW.tenant_id
      AND sequence = NEW.sequence
)
BEGIN
    SELECT RAISE(ABORT, 'search-attempt audit event identity already exists');
END;
CREATE TRIGGER search_attempt_audit_event_job_identity_collision_is_forbidden
BEFORE INSERT ON search_attempt_audit_events
WHEN EXISTS (
    SELECT 1
    FROM search_attempt_audit_events
    WHERE tenant_id = NEW.tenant_id
      AND search_job_id = NEW.search_job_id
)
BEGIN
    SELECT RAISE(
        ABORT,
        'search-attempt audit retained job identity already exists'
    );
END;
CREATE TRIGGER search_attempt_audit_event_insert_requires_current_state
BEFORE INSERT ON search_attempt_audit_events
WHEN NOT EXISTS (
    SELECT 1
    FROM search_attempt_audit_tenant_state
    WHERE tenant_id = NEW.tenant_id
      AND retained_count BETWEEN 0 AND maximum_retained_attempts
      AND next_sequence = NEW.sequence
      AND next_sequence BETWEEN 1 AND 9223372036854775806
)
BEGIN
    SELECT RAISE(
        ABORT,
        'search-attempt audit tenant state is invalid or sequence is exhausted'
    );
END;
CREATE TRIGGER search_attempt_audit_event_advances_and_prunes
AFTER INSERT ON search_attempt_audit_events
BEGIN
    UPDATE search_attempt_audit_tenant_state
    SET next_sequence = next_sequence + 1,
        retained_count = retained_count + 1
    WHERE tenant_id = NEW.tenant_id
      AND next_sequence = NEW.sequence
      AND retained_count BETWEEN 0 AND maximum_retained_attempts;

    SELECT CASE
        WHEN changes() <> 1
        THEN RAISE(ABORT, 'search-attempt audit event accounting failed')
    END;

    DELETE FROM search_attempt_audit_events
    WHERE tenant_id = NEW.tenant_id
      AND sequence = (
          SELECT first_sequence
          FROM search_attempt_audit_tenant_state
          WHERE tenant_id = NEW.tenant_id
            AND retained_count = maximum_retained_attempts + 1
      );
END;
CREATE TRIGGER search_attempt_audit_event_delete_requires_rolling_prune
BEFORE DELETE ON search_attempt_audit_events
WHEN NOT EXISTS (
    SELECT 1
    FROM search_attempt_audit_tenant_state
    WHERE tenant_id = OLD.tenant_id
      AND first_sequence = OLD.sequence
      AND retained_count = maximum_retained_attempts + 1
)
BEGIN
    SELECT RAISE(ABORT, 'search-attempt audit event deletion is not a rolling prune');
END;
CREATE TRIGGER search_attempt_audit_event_prune_advances_state
AFTER DELETE ON search_attempt_audit_events
BEGIN
    UPDATE search_attempt_audit_tenant_state
    SET first_sequence = first_sequence + 1,
        retained_count = retained_count - 1
    WHERE tenant_id = OLD.tenant_id
      AND first_sequence = OLD.sequence
      AND retained_count = maximum_retained_attempts + 1;

    SELECT CASE
        WHEN changes() <> 1
        THEN RAISE(ABORT, 'search-attempt audit rolling prune accounting failed')
    END;
END;
CREATE TRIGGER knowledge_catalog_tenant_identity_collision_is_forbidden
BEFORE INSERT ON knowledge_catalog_tenants
WHEN EXISTS (
    SELECT 1 FROM knowledge_catalog_tenants WHERE tenant_id = NEW.tenant_id
)
BEGIN
    SELECT RAISE(ABORT, 'knowledge catalog tenant already exists');
END;
CREATE TRIGGER knowledge_catalog_tenant_initial_shape_is_valid
BEFORE INSERT ON knowledge_catalog_tenants
WHEN NEW.catalog_revision <> 0
  OR NEW.identity_count <> 0
  OR NEW.version_count <> 0
  OR NEW.definition_body_bytes <> 0
  OR NEW.idempotency_count <> 0
  OR NEW.active_object_count <> 0
  OR NEW.recovery_audit_count <> 0
BEGIN
    SELECT RAISE(ABORT, 'knowledge catalog tenant must begin empty');
END;
CREATE TRIGGER knowledge_catalog_revision_transition_is_valid
BEFORE UPDATE OF catalog_revision ON knowledge_catalog_tenants
WHEN NEW.catalog_revision <> OLD.catalog_revision
 AND NEW.catalog_revision <> OLD.catalog_revision + 1
BEGIN
    SELECT RAISE(ABORT, 'knowledge catalog revision must advance by one');
END;
CREATE TRIGGER knowledge_catalog_tenant_identity_is_immutable
BEFORE UPDATE OF tenant_id ON knowledge_catalog_tenants
WHEN NEW.tenant_id <> OLD.tenant_id
BEGIN
    SELECT RAISE(ABORT, 'knowledge catalog tenant identity is immutable');
END;
CREATE TRIGGER knowledge_catalog_tenant_delete_is_forbidden
BEFORE DELETE ON knowledge_catalog_tenants
BEGIN
    SELECT RAISE(ABORT, 'knowledge catalog tenant cannot be deleted');
END;
CREATE TRIGGER knowledge_definition_blob_capacity_is_available
BEFORE INSERT ON knowledge_definition_blobs
WHEN NOT EXISTS (
    SELECT 1
    FROM knowledge_catalog_tenants
    WHERE tenant_id = NEW.tenant_id
      AND definition_body_bytes <= 536870912 - NEW.definition_bytes
)
BEGIN
    SELECT RAISE(ABORT, 'knowledge definition body capacity exhausted');
END;
CREATE TRIGGER knowledge_definition_blob_identity_collision_is_forbidden
BEFORE INSERT ON knowledge_definition_blobs
WHEN EXISTS (
    SELECT 1 FROM knowledge_definition_blobs
    WHERE tenant_id = NEW.tenant_id
      AND definition_digest = NEW.definition_digest
)
BEGIN
    SELECT RAISE(ABORT, 'knowledge definition blob already exists');
END;
CREATE TRIGGER knowledge_definition_blob_after_insert
AFTER INSERT ON knowledge_definition_blobs
BEGIN
    UPDATE knowledge_catalog_tenants
    SET definition_body_bytes = definition_body_bytes + NEW.definition_bytes
    WHERE tenant_id = NEW.tenant_id;
END;
CREATE TRIGGER knowledge_definition_blob_update_is_forbidden
BEFORE UPDATE ON knowledge_definition_blobs
BEGIN
    SELECT RAISE(ABORT, 'knowledge definition blob is immutable');
END;
CREATE TRIGGER knowledge_definition_blob_delete_is_forbidden
BEFORE DELETE ON knowledge_definition_blobs
BEGIN
    SELECT RAISE(ABORT, 'knowledge definition blob cannot be deleted');
END;
CREATE TRIGGER knowledge_object_identity_capacity_is_available
BEFORE INSERT ON knowledge_objects
WHEN NOT EXISTS (
    SELECT 1 FROM knowledge_catalog_tenants
    WHERE tenant_id = NEW.tenant_id AND identity_count < 8192
)
BEGIN
    SELECT RAISE(ABORT, 'knowledge object identity capacity exhausted');
END;
CREATE TRIGGER knowledge_object_active_app_is_required_insert
BEFORE INSERT ON knowledge_objects
WHEN NEW.state = 'active' AND NOT EXISTS (
    SELECT 1
    FROM app_workspaces
    WHERE tenant_id = NEW.tenant_id
      AND app_id = NEW.app_id
      AND state = 'active'
)
BEGIN
    SELECT RAISE(ABORT, 'active knowledge object requires active app');
END;
CREATE TRIGGER knowledge_object_active_app_is_required_update
BEFORE UPDATE ON knowledge_objects
WHEN NEW.state = 'active' AND NOT EXISTS (
    SELECT 1
    FROM app_workspaces
    WHERE tenant_id = NEW.tenant_id
      AND app_id = NEW.app_id
      AND state = 'active'
)
BEGIN
    SELECT RAISE(ABORT, 'active knowledge object requires active app');
END;
CREATE TRIGGER knowledge_object_identity_collision_is_forbidden
BEFORE INSERT ON knowledge_objects
WHEN EXISTS (
    SELECT 1 FROM knowledge_objects
    WHERE tenant_id = NEW.tenant_id
      AND knowledge_object_id = NEW.knowledge_object_id
)
BEGIN
    SELECT RAISE(ABORT, 'knowledge object identity already exists');
END;
CREATE TRIGGER knowledge_object_active_name_collision_is_forbidden
BEFORE INSERT ON knowledge_objects
WHEN NEW.state = 'active' AND EXISTS (
    SELECT 1
    FROM knowledge_objects AS existing
    WHERE existing.tenant_id = NEW.tenant_id
      AND existing.state = 'active'
      AND existing.object_type = NEW.object_type
      AND existing.name = NEW.name
      AND (
          (
              NEW.sharing_scope = 'private'
              AND existing.sharing_scope = 'private'
              AND existing.app_id = NEW.app_id
              AND existing.owner_id = NEW.owner_id
          )
          OR (
              NEW.sharing_scope = 'app'
              AND existing.sharing_scope = 'app'
              AND existing.app_id = NEW.app_id
          )
          OR (
              NEW.sharing_scope = 'global'
              AND existing.sharing_scope = 'global'
          )
      )
)
BEGIN
    SELECT RAISE(ABORT, 'knowledge active name already exists');
END;
CREATE TRIGGER knowledge_object_active_name_update_collision_is_forbidden
BEFORE UPDATE ON knowledge_objects
WHEN NEW.state = 'active' AND EXISTS (
    SELECT 1
    FROM knowledge_objects AS existing
    WHERE existing.tenant_id = NEW.tenant_id
      AND existing.knowledge_object_id <> OLD.knowledge_object_id
      AND existing.state = 'active'
      AND existing.object_type = NEW.object_type
      AND existing.name = NEW.name
      AND (
          (
              NEW.sharing_scope = 'private'
              AND existing.sharing_scope = 'private'
              AND existing.app_id = NEW.app_id
              AND existing.owner_id = NEW.owner_id
          )
          OR (
              NEW.sharing_scope = 'app'
              AND existing.sharing_scope = 'app'
              AND existing.app_id = NEW.app_id
          )
          OR (
              NEW.sharing_scope = 'global'
              AND existing.sharing_scope = 'global'
          )
      )
)
BEGIN
    SELECT RAISE(ABORT, 'knowledge active name already exists');
END;
CREATE TRIGGER knowledge_object_after_insert_count_identity
AFTER INSERT ON knowledge_objects
BEGIN
    UPDATE knowledge_catalog_tenants
    SET identity_count = identity_count + 1
    WHERE tenant_id = NEW.tenant_id;
END;
CREATE TRIGGER knowledge_object_registry_transition_is_valid
BEFORE UPDATE ON knowledge_objects
WHEN NOT (
    NEW.tenant_id = OLD.tenant_id
    AND NEW.knowledge_object_id = OLD.knowledge_object_id
    AND NEW.created_at_unix_micro = OLD.created_at_unix_micro
    AND NEW.current_version = OLD.current_version + 1
    AND NEW.updated_at_unix_micro >= OLD.updated_at_unix_micro
    AND OLD.state NOT IN ('quarantined', 'deleted')
)
BEGIN
    SELECT RAISE(ABORT, 'knowledge object registry transition is invalid');
END;
CREATE TRIGGER knowledge_object_delete_is_forbidden
BEFORE DELETE ON knowledge_objects
BEGIN
    SELECT RAISE(ABORT, 'knowledge object registry row cannot be deleted');
END;
CREATE TRIGGER knowledge_object_version_capacity_is_available
BEFORE INSERT ON knowledge_object_versions
WHEN NOT EXISTS (
    SELECT 1
    FROM knowledge_catalog_tenants
    WHERE tenant_id = NEW.tenant_id
      AND (
          (NEW.mutation_kind = 'quarantine' AND version_count < 65536)
          OR (NEW.mutation_kind <> 'quarantine' AND version_count < 61440)
      )
)
BEGIN
    SELECT RAISE(ABORT, 'knowledge object version capacity exhausted');
END;
CREATE TRIGGER knowledge_object_version_identity_collision_is_forbidden
BEFORE INSERT ON knowledge_object_versions
WHEN EXISTS (
    SELECT 1 FROM knowledge_object_versions
    WHERE tenant_id = NEW.tenant_id
      AND knowledge_object_id = NEW.knowledge_object_id
      AND object_version = NEW.object_version
)
BEGIN
    SELECT RAISE(ABORT, 'knowledge object version already exists');
END;
CREATE TRIGGER knowledge_object_version_is_contiguous
BEFORE INSERT ON knowledge_object_versions
WHEN NEW.object_version > 1 AND NOT EXISTS (
    SELECT 1
    FROM knowledge_object_versions
    WHERE tenant_id = NEW.tenant_id
      AND knowledge_object_id = NEW.knowledge_object_id
      AND object_version = NEW.object_version - 1
)
BEGIN
    SELECT RAISE(ABORT, 'knowledge object version must be contiguous');
END;
CREATE TRIGGER knowledge_object_version_after_insert
AFTER INSERT ON knowledge_object_versions
BEGIN
    UPDATE knowledge_catalog_tenants
    SET version_count = version_count + 1
    WHERE tenant_id = NEW.tenant_id;
END;
CREATE TRIGGER knowledge_object_version_update_is_forbidden
BEFORE UPDATE ON knowledge_object_versions
BEGIN
    SELECT RAISE(ABORT, 'knowledge object version is immutable');
END;
CREATE TRIGGER knowledge_object_version_delete_is_forbidden
BEFORE DELETE ON knowledge_object_versions
BEGIN
    SELECT RAISE(ABORT, 'knowledge object version cannot be deleted');
END;
CREATE TRIGGER knowledge_dependency_ordinal_is_declared
BEFORE INSERT ON knowledge_object_dependencies
WHEN NOT EXISTS (
    SELECT 1
    FROM knowledge_object_versions
    WHERE tenant_id = NEW.tenant_id
      AND knowledge_object_id = NEW.source_object_id
      AND object_version = NEW.source_object_version
      AND NEW.ordinal < dependency_count
)
BEGIN
    SELECT RAISE(ABORT, 'knowledge dependency ordinal exceeds declared count');
END;
CREATE TRIGGER knowledge_dependency_sealed_version_is_immutable
BEFORE INSERT ON knowledge_object_dependencies
WHEN EXISTS (
    SELECT 1
    FROM knowledge_object_dependency_seals
    WHERE tenant_id = NEW.tenant_id
      AND knowledge_object_id = NEW.source_object_id
      AND object_version = NEW.source_object_version
)
BEGIN
    SELECT RAISE(ABORT, 'knowledge dependency set is sealed');
END;
CREATE TRIGGER knowledge_dependency_identity_collision_is_forbidden
BEFORE INSERT ON knowledge_object_dependencies
WHEN EXISTS (
    SELECT 1
    FROM knowledge_object_dependencies
    WHERE tenant_id = NEW.tenant_id
      AND source_object_id = NEW.source_object_id
      AND source_object_version = NEW.source_object_version
      AND (
          ordinal = NEW.ordinal
          OR (
              target_kind = NEW.target_kind
              AND target_object_id = NEW.target_object_id
              AND target_object_version = NEW.target_object_version
              AND dependency_role = NEW.dependency_role
          )
      )
)
BEGIN
    SELECT RAISE(ABORT, 'knowledge dependency already exists');
END;
CREATE TRIGGER knowledge_object_acl_identity_collision_is_forbidden
BEFORE INSERT ON knowledge_object_acl
WHEN EXISTS (
    SELECT 1 FROM knowledge_object_acl
    WHERE tenant_id = NEW.tenant_id
      AND knowledge_object_id = NEW.knowledge_object_id
      AND role_id = NEW.role_id
)
BEGIN
    SELECT RAISE(ABORT, 'knowledge object ACL already exists');
END;
CREATE TRIGGER knowledge_app_active_counter_identity_collision_is_forbidden
BEFORE INSERT ON knowledge_app_active_counters
WHEN EXISTS (
    SELECT 1 FROM knowledge_app_active_counters
    WHERE tenant_id = NEW.tenant_id AND app_id = NEW.app_id
)
BEGIN
    SELECT RAISE(ABORT, 'knowledge app active counter already exists');
END;
CREATE TRIGGER knowledge_app_active_counter_identity_is_immutable
BEFORE UPDATE OF tenant_id, app_id ON knowledge_app_active_counters
WHEN NEW.tenant_id <> OLD.tenant_id OR NEW.app_id <> OLD.app_id
BEGIN
    SELECT RAISE(ABORT, 'knowledge app active counter identity is immutable');
END;
CREATE TRIGGER knowledge_app_active_counter_delete_is_forbidden
BEFORE DELETE ON knowledge_app_active_counters
BEGIN
    SELECT RAISE(ABORT, 'knowledge app active counter cannot be deleted');
END;
CREATE TRIGGER knowledge_owner_active_counter_identity_collision_is_forbidden
BEFORE INSERT ON knowledge_owner_active_counters
WHEN EXISTS (
    SELECT 1 FROM knowledge_owner_active_counters
    WHERE tenant_id = NEW.tenant_id AND owner_id = NEW.owner_id
)
BEGIN
    SELECT RAISE(ABORT, 'knowledge owner active counter already exists');
END;
CREATE TRIGGER knowledge_owner_active_counter_identity_is_immutable
BEFORE UPDATE OF tenant_id, owner_id ON knowledge_owner_active_counters
WHEN NEW.tenant_id <> OLD.tenant_id OR NEW.owner_id <> OLD.owner_id
BEGIN
    SELECT RAISE(ABORT, 'knowledge owner active counter identity is immutable');
END;
CREATE TRIGGER knowledge_owner_active_counter_delete_is_forbidden
BEFORE DELETE ON knowledge_owner_active_counters
BEGIN
    SELECT RAISE(ABORT, 'knowledge owner active counter cannot be deleted');
END;
CREATE TRIGGER knowledge_type_active_counter_identity_collision_is_forbidden
BEFORE INSERT ON knowledge_type_active_counters
WHEN EXISTS (
    SELECT 1 FROM knowledge_type_active_counters
    WHERE tenant_id = NEW.tenant_id AND object_type = NEW.object_type
)
BEGIN
    SELECT RAISE(ABORT, 'knowledge type active counter already exists');
END;
CREATE TRIGGER knowledge_type_active_counter_identity_is_immutable
BEFORE UPDATE OF tenant_id, object_type ON knowledge_type_active_counters
WHEN NEW.tenant_id <> OLD.tenant_id OR NEW.object_type <> OLD.object_type
BEGIN
    SELECT RAISE(ABORT, 'knowledge type active counter identity is immutable');
END;
CREATE TRIGGER knowledge_type_active_counter_delete_is_forbidden
BEFORE DELETE ON knowledge_type_active_counters
BEGIN
    SELECT RAISE(ABORT, 'knowledge type active counter cannot be deleted');
END;
CREATE TRIGGER knowledge_app_type_active_counter_identity_collision_is_forbidden
BEFORE INSERT ON knowledge_app_type_active_counters
WHEN EXISTS (
    SELECT 1 FROM knowledge_app_type_active_counters
    WHERE tenant_id = NEW.tenant_id AND app_id = NEW.app_id
      AND object_type = NEW.object_type
)
BEGIN
    SELECT RAISE(ABORT, 'knowledge app/type active counter already exists');
END;
CREATE TRIGGER knowledge_app_type_active_counter_identity_is_immutable
BEFORE UPDATE OF tenant_id, app_id, object_type ON knowledge_app_type_active_counters
WHEN NEW.tenant_id <> OLD.tenant_id
  OR NEW.app_id <> OLD.app_id
  OR NEW.object_type <> OLD.object_type
BEGIN
    SELECT RAISE(ABORT, 'knowledge app/type active counter identity is immutable');
END;
CREATE TRIGGER knowledge_app_type_active_counter_delete_is_forbidden
BEFORE DELETE ON knowledge_app_type_active_counters
BEGIN
    SELECT RAISE(ABORT, 'knowledge app/type active counter cannot be deleted');
END;
CREATE TRIGGER knowledge_dependency_update_is_forbidden
BEFORE UPDATE ON knowledge_object_dependencies
BEGIN
    SELECT RAISE(ABORT, 'knowledge object dependency is immutable');
END;
CREATE TRIGGER knowledge_dependency_delete_is_forbidden
BEFORE DELETE ON knowledge_object_dependencies
BEGIN
    SELECT RAISE(ABORT, 'knowledge object dependency cannot be deleted');
END;
CREATE TRIGGER knowledge_dependency_seal_identity_collision_is_forbidden
BEFORE INSERT ON knowledge_object_dependency_seals
WHEN EXISTS (
    SELECT 1
    FROM knowledge_object_dependency_seals
    WHERE tenant_id = NEW.tenant_id
      AND knowledge_object_id = NEW.knowledge_object_id
      AND object_version = NEW.object_version
)
BEGIN
    SELECT RAISE(ABORT, 'knowledge dependency seal already exists');
END;
CREATE TRIGGER knowledge_dependency_seal_is_complete
BEFORE INSERT ON knowledge_object_dependency_seals
WHEN NEW.dependency_count <> (
    SELECT count(*)
    FROM knowledge_object_dependencies
    WHERE tenant_id = NEW.tenant_id
      AND source_object_id = NEW.knowledge_object_id
      AND source_object_version = NEW.object_version
)
BEGIN
    SELECT RAISE(ABORT, 'knowledge dependency set is incomplete');
END;
CREATE TRIGGER knowledge_dependency_seal_update_is_forbidden
BEFORE UPDATE ON knowledge_object_dependency_seals
BEGIN
    SELECT RAISE(ABORT, 'knowledge dependency seal is immutable');
END;
CREATE TRIGGER knowledge_dependency_seal_delete_is_forbidden
BEFORE DELETE ON knowledge_object_dependency_seals
BEGIN
    SELECT RAISE(ABORT, 'knowledge dependency seal cannot be deleted');
END;
CREATE TRIGGER knowledge_object_active_capacity_insert
BEFORE INSERT ON knowledge_objects
WHEN NEW.state = 'active' AND (
    (SELECT active_object_count FROM knowledge_catalog_tenants
        WHERE tenant_id = NEW.tenant_id) >= 4096
    OR COALESCE((
        SELECT active_object_count FROM knowledge_app_active_counters
        WHERE tenant_id = NEW.tenant_id AND app_id = NEW.app_id
    ), 0) >= 1024
    OR COALESCE((
        SELECT active_object_count FROM knowledge_type_active_counters
        WHERE tenant_id = NEW.tenant_id AND object_type = NEW.object_type
    ), 0) >= 2048
    OR COALESCE((
        SELECT active_object_count FROM knowledge_app_type_active_counters
        WHERE tenant_id = NEW.tenant_id AND app_id = NEW.app_id
          AND object_type = NEW.object_type
    ), 0) >= 512
    OR (
        NEW.sharing_scope = 'private'
        AND COALESCE((
            SELECT active_private_object_count FROM knowledge_owner_active_counters
            WHERE tenant_id = NEW.tenant_id AND owner_id = NEW.owner_id
        ), 0) >= 512
    )
)
BEGIN
    SELECT RAISE(ABORT, 'knowledge active object capacity exhausted');
END;
CREATE TRIGGER knowledge_object_active_capacity_update
BEFORE UPDATE ON knowledge_objects
WHEN NEW.state = 'active' AND (
    (OLD.state <> 'active' AND (
        SELECT active_object_count FROM knowledge_catalog_tenants
        WHERE tenant_id = NEW.tenant_id
    ) >= 4096)
    OR ((OLD.state <> 'active' OR OLD.app_id <> NEW.app_id) AND COALESCE((
        SELECT active_object_count FROM knowledge_app_active_counters
        WHERE tenant_id = NEW.tenant_id AND app_id = NEW.app_id
    ), 0) >= 1024)
    OR ((OLD.state <> 'active' OR OLD.object_type <> NEW.object_type) AND COALESCE((
        SELECT active_object_count FROM knowledge_type_active_counters
        WHERE tenant_id = NEW.tenant_id AND object_type = NEW.object_type
    ), 0) >= 2048)
    OR ((OLD.state <> 'active' OR OLD.app_id <> NEW.app_id
         OR OLD.object_type <> NEW.object_type) AND COALESCE((
        SELECT active_object_count FROM knowledge_app_type_active_counters
        WHERE tenant_id = NEW.tenant_id AND app_id = NEW.app_id
          AND object_type = NEW.object_type
    ), 0) >= 512)
    OR (
        NEW.sharing_scope = 'private'
        AND (
            OLD.state <> 'active'
            OR OLD.sharing_scope <> 'private'
            OR OLD.owner_id <> NEW.owner_id
        )
        AND COALESCE((
            SELECT active_private_object_count FROM knowledge_owner_active_counters
            WHERE tenant_id = NEW.tenant_id AND owner_id = NEW.owner_id
        ), 0) >= 512
    )
)
BEGIN
    SELECT RAISE(ABORT, 'knowledge active object capacity exhausted');
END;
CREATE TRIGGER knowledge_object_active_counters_after_insert
AFTER INSERT ON knowledge_objects
WHEN NEW.state = 'active'
BEGIN
    UPDATE knowledge_catalog_tenants
    SET active_object_count = active_object_count + 1
    WHERE tenant_id = NEW.tenant_id;

    UPDATE knowledge_app_active_counters
    SET active_object_count = active_object_count + 1
    WHERE tenant_id = NEW.tenant_id AND app_id = NEW.app_id;
    INSERT INTO knowledge_app_active_counters (
        tenant_id, app_id, active_object_count
    ) SELECT NEW.tenant_id, NEW.app_id, 1
      WHERE NOT EXISTS (
          SELECT 1 FROM knowledge_app_active_counters
          WHERE tenant_id = NEW.tenant_id AND app_id = NEW.app_id
      );

    UPDATE knowledge_type_active_counters
    SET active_object_count = active_object_count + 1
    WHERE tenant_id = NEW.tenant_id AND object_type = NEW.object_type;
    INSERT INTO knowledge_type_active_counters (
        tenant_id, object_type, active_object_count
    ) SELECT NEW.tenant_id, NEW.object_type, 1
      WHERE NOT EXISTS (
          SELECT 1 FROM knowledge_type_active_counters
          WHERE tenant_id = NEW.tenant_id AND object_type = NEW.object_type
      );

    UPDATE knowledge_app_type_active_counters
    SET active_object_count = active_object_count + 1
    WHERE tenant_id = NEW.tenant_id AND app_id = NEW.app_id
      AND object_type = NEW.object_type;
    INSERT INTO knowledge_app_type_active_counters (
        tenant_id, app_id, object_type, active_object_count
    ) SELECT NEW.tenant_id, NEW.app_id, NEW.object_type, 1
      WHERE NOT EXISTS (
          SELECT 1 FROM knowledge_app_type_active_counters
          WHERE tenant_id = NEW.tenant_id AND app_id = NEW.app_id
            AND object_type = NEW.object_type
      );

    UPDATE knowledge_owner_active_counters
    SET active_private_object_count = active_private_object_count + 1
    WHERE NEW.sharing_scope = 'private'
      AND tenant_id = NEW.tenant_id AND owner_id = NEW.owner_id;
    INSERT INTO knowledge_owner_active_counters (
        tenant_id, owner_id, active_private_object_count
    ) SELECT NEW.tenant_id, NEW.owner_id, 1
      WHERE NEW.sharing_scope = 'private'
        AND NOT EXISTS (
            SELECT 1 FROM knowledge_owner_active_counters
            WHERE tenant_id = NEW.tenant_id AND owner_id = NEW.owner_id
        );
END;
CREATE TRIGGER knowledge_object_active_counters_before_update
BEFORE UPDATE ON knowledge_objects
WHEN OLD.state = 'active'
BEGIN
    UPDATE knowledge_catalog_tenants
    SET active_object_count = active_object_count - 1
    WHERE tenant_id = OLD.tenant_id;

    UPDATE knowledge_app_active_counters
    SET active_object_count = active_object_count - 1
    WHERE tenant_id = OLD.tenant_id AND app_id = OLD.app_id;

    UPDATE knowledge_type_active_counters
    SET active_object_count = active_object_count - 1
    WHERE tenant_id = OLD.tenant_id AND object_type = OLD.object_type;

    UPDATE knowledge_app_type_active_counters
    SET active_object_count = active_object_count - 1
    WHERE tenant_id = OLD.tenant_id AND app_id = OLD.app_id
      AND object_type = OLD.object_type;

    UPDATE knowledge_owner_active_counters
    SET active_private_object_count = active_private_object_count - 1
    WHERE OLD.sharing_scope = 'private'
      AND tenant_id = OLD.tenant_id AND owner_id = OLD.owner_id;
END;
CREATE TRIGGER knowledge_object_active_counters_after_update
AFTER UPDATE ON knowledge_objects
WHEN NEW.state = 'active'
BEGIN
    UPDATE knowledge_catalog_tenants
    SET active_object_count = active_object_count + 1
    WHERE tenant_id = NEW.tenant_id;

    UPDATE knowledge_app_active_counters
    SET active_object_count = active_object_count + 1
    WHERE tenant_id = NEW.tenant_id AND app_id = NEW.app_id;
    INSERT INTO knowledge_app_active_counters (
        tenant_id, app_id, active_object_count
    ) SELECT NEW.tenant_id, NEW.app_id, 1
      WHERE NOT EXISTS (
          SELECT 1 FROM knowledge_app_active_counters
          WHERE tenant_id = NEW.tenant_id AND app_id = NEW.app_id
      );

    UPDATE knowledge_type_active_counters
    SET active_object_count = active_object_count + 1
    WHERE tenant_id = NEW.tenant_id AND object_type = NEW.object_type;
    INSERT INTO knowledge_type_active_counters (
        tenant_id, object_type, active_object_count
    ) SELECT NEW.tenant_id, NEW.object_type, 1
      WHERE NOT EXISTS (
          SELECT 1 FROM knowledge_type_active_counters
          WHERE tenant_id = NEW.tenant_id AND object_type = NEW.object_type
      );

    UPDATE knowledge_app_type_active_counters
    SET active_object_count = active_object_count + 1
    WHERE tenant_id = NEW.tenant_id AND app_id = NEW.app_id
      AND object_type = NEW.object_type;
    INSERT INTO knowledge_app_type_active_counters (
        tenant_id, app_id, object_type, active_object_count
    ) SELECT NEW.tenant_id, NEW.app_id, NEW.object_type, 1
      WHERE NOT EXISTS (
          SELECT 1 FROM knowledge_app_type_active_counters
          WHERE tenant_id = NEW.tenant_id AND app_id = NEW.app_id
            AND object_type = NEW.object_type
      );

    UPDATE knowledge_owner_active_counters
    SET active_private_object_count = active_private_object_count + 1
    WHERE NEW.sharing_scope = 'private'
      AND tenant_id = NEW.tenant_id AND owner_id = NEW.owner_id;
    INSERT INTO knowledge_owner_active_counters (
        tenant_id, owner_id, active_private_object_count
    ) SELECT NEW.tenant_id, NEW.owner_id, 1
      WHERE NEW.sharing_scope = 'private'
        AND NOT EXISTS (
            SELECT 1 FROM knowledge_owner_active_counters
            WHERE tenant_id = NEW.tenant_id AND owner_id = NEW.owner_id
        );
END;
CREATE TRIGGER knowledge_recovery_audit_capacity_is_available
BEFORE INSERT ON knowledge_recovery_audit
WHEN NOT EXISTS (
    SELECT 1
    FROM knowledge_catalog_tenants
    WHERE tenant_id = NEW.tenant_id
      AND recovery_audit_count < 8192
      AND NEW.sequence = recovery_audit_count + 1
)
BEGIN
    SELECT RAISE(ABORT, 'knowledge recovery audit capacity or sequence invalid');
END;
CREATE TRIGGER knowledge_recovery_audit_identity_collision_is_forbidden
BEFORE INSERT ON knowledge_recovery_audit
WHEN EXISTS (
    SELECT 1
    FROM knowledge_recovery_audit
    WHERE tenant_id = NEW.tenant_id
      AND (
          sequence = NEW.sequence
          OR knowledge_object_id = NEW.knowledge_object_id
      )
)
BEGIN
    SELECT RAISE(ABORT, 'knowledge recovery audit identity already exists');
END;
CREATE TRIGGER knowledge_recovery_audit_matches_terminal_version
BEFORE INSERT ON knowledge_recovery_audit
WHEN NOT EXISTS (
    SELECT 1
    FROM knowledge_object_versions
    WHERE tenant_id = NEW.tenant_id
      AND knowledge_object_id = NEW.knowledge_object_id
      AND object_version = NEW.object_version
      AND state = 'quarantined'
      AND app_id = NEW.app_id
      AND object_type = NEW.object_type
      AND sharing_scope = NEW.sharing_scope
      AND quarantine_reason = NEW.recovery_reason
      AND EXISTS (
          SELECT 1
          FROM knowledge_objects
          WHERE tenant_id = NEW.tenant_id
            AND knowledge_object_id = NEW.knowledge_object_id
            AND current_version = NEW.object_version
            AND state = 'quarantined'
            AND app_id = NEW.app_id
            AND object_type = NEW.object_type
            AND sharing_scope = NEW.sharing_scope
            AND quarantine_reason = NEW.recovery_reason
      )
)
BEGIN
    SELECT RAISE(ABORT, 'knowledge recovery audit does not match terminal version');
END;
CREATE TRIGGER knowledge_recovery_audit_after_insert
AFTER INSERT ON knowledge_recovery_audit
BEGIN
    UPDATE knowledge_catalog_tenants
    SET recovery_audit_count = recovery_audit_count + 1
    WHERE tenant_id = NEW.tenant_id;
END;
CREATE TRIGGER knowledge_recovery_audit_update_is_forbidden
BEFORE UPDATE ON knowledge_recovery_audit
BEGIN
    SELECT RAISE(ABORT, 'knowledge recovery audit is immutable');
END;
CREATE TRIGGER knowledge_recovery_audit_delete_is_forbidden
BEFORE DELETE ON knowledge_recovery_audit
BEGIN
    SELECT RAISE(ABORT, 'knowledge recovery audit cannot be deleted');
END;
CREATE TRIGGER knowledge_referenced_app_workspace_cannot_be_deleted
BEFORE DELETE ON app_workspaces
WHEN EXISTS (
    SELECT 1
    FROM knowledge_objects
    WHERE tenant_id = OLD.tenant_id AND app_id = OLD.app_id
)
BEGIN
    SELECT RAISE(ABORT, 'app workspace is referenced by knowledge objects');
END;
CREATE TRIGGER knowledge_active_app_workspace_cannot_be_archived
BEFORE UPDATE OF state ON app_workspaces
WHEN NEW.state = 'archived'
 AND EXISTS (
    SELECT 1
    FROM knowledge_objects
    WHERE tenant_id = OLD.tenant_id
      AND app_id = OLD.app_id
      AND state = 'active'
)
BEGIN
    SELECT RAISE(ABORT, 'app workspace has active knowledge objects');
END;
CREATE TRIGGER knowledge_projection_ledger_identity_collision_is_forbidden
BEFORE INSERT ON knowledge_projection_tenant_ledgers
WHEN EXISTS (
    SELECT 1 FROM knowledge_projection_tenant_ledgers
    WHERE tenant_id = NEW.tenant_id
)
BEGIN
    SELECT RAISE(ABORT, 'knowledge projection tenant ledger already exists');
END;
CREATE TRIGGER knowledge_projection_ledger_initial_shape_is_valid
BEFORE INSERT ON knowledge_projection_tenant_ledgers
WHEN NEW.projection_bytes <> 0
 OR EXISTS (
    SELECT 1 FROM knowledge_object_list_projections
    WHERE tenant_id = NEW.tenant_id
)
BEGIN
    SELECT RAISE(ABORT, 'knowledge projection tenant ledger must begin empty');
END;
CREATE TRIGGER knowledge_projection_ledger_transition_is_exact
BEFORE UPDATE ON knowledge_projection_tenant_ledgers
WHEN NEW.tenant_id <> OLD.tenant_id
 OR NEW.projection_bytes <> COALESCE((
    SELECT sum(projection_bytes)
    FROM knowledge_object_list_projections
    WHERE tenant_id = OLD.tenant_id
), 0)
BEGIN
    SELECT RAISE(ABORT, 'knowledge projection byte ledger transition is invalid');
END;
CREATE TRIGGER knowledge_projection_ledger_delete_is_forbidden
BEFORE DELETE ON knowledge_projection_tenant_ledgers
BEGIN
    SELECT RAISE(ABORT, 'knowledge projection tenant ledger cannot be deleted');
END;
CREATE TRIGGER knowledge_list_projection_capacity_is_available
BEFORE INSERT ON knowledge_object_list_projections
WHEN NOT EXISTS (
    SELECT 1
    FROM knowledge_projection_tenant_ledgers
    WHERE tenant_id = NEW.tenant_id
      AND projection_bytes <= 268435456 - NEW.projection_bytes
)
BEGIN
    SELECT RAISE(ABORT, 'knowledge list projection byte capacity exhausted');
END;
CREATE TRIGGER knowledge_list_projection_identity_collision_is_forbidden
BEFORE INSERT ON knowledge_object_list_projections
WHEN EXISTS (
    SELECT 1
    FROM knowledge_object_list_projections
    WHERE tenant_id = NEW.tenant_id
      AND knowledge_object_id = NEW.knowledge_object_id
      AND object_version = NEW.object_version
)
BEGIN
    SELECT RAISE(ABORT, 'knowledge list projection identity already exists');
END;
CREATE TRIGGER knowledge_list_projection_after_insert
AFTER INSERT ON knowledge_object_list_projections
BEGIN
    UPDATE knowledge_projection_tenant_ledgers
    SET projection_bytes = projection_bytes + NEW.projection_bytes
    WHERE tenant_id = NEW.tenant_id;
END;
CREATE TRIGGER knowledge_list_projection_update_is_forbidden
BEFORE UPDATE ON knowledge_object_list_projections
BEGIN
    SELECT RAISE(ABORT, 'knowledge list projection is immutable');
END;
CREATE TRIGGER knowledge_list_projection_delete_requires_unsealed_empty_row
BEFORE DELETE ON knowledge_object_list_projections
WHEN EXISTS (
    SELECT 1 FROM knowledge_object_list_projection_seals
    WHERE tenant_id = OLD.tenant_id
      AND knowledge_object_id = OLD.knowledge_object_id
      AND object_version = OLD.object_version
)
 OR EXISTS (
    SELECT 1 FROM knowledge_object_list_selector_patterns
    WHERE tenant_id = OLD.tenant_id
      AND knowledge_object_id = OLD.knowledge_object_id
      AND object_version = OLD.object_version
)
BEGIN
    SELECT RAISE(ABORT, 'knowledge list projection must be unsealed and empty before deletion');
END;
CREATE TRIGGER knowledge_list_projection_after_delete
AFTER DELETE ON knowledge_object_list_projections
BEGIN
    UPDATE knowledge_projection_tenant_ledgers
    SET projection_bytes = projection_bytes - OLD.projection_bytes
    WHERE tenant_id = OLD.tenant_id;
END;
CREATE TRIGGER knowledge_list_selector_identity_collision_is_forbidden
BEFORE INSERT ON knowledge_object_list_selector_patterns
WHEN EXISTS (
    SELECT 1
    FROM knowledge_object_list_selector_patterns
    WHERE tenant_id = NEW.tenant_id
      AND knowledge_object_id = NEW.knowledge_object_id
      AND object_version = NEW.object_version
      AND dimension = NEW.dimension
      AND (ordinal = NEW.ordinal OR value = NEW.value)
)
BEGIN
    SELECT RAISE(ABORT, 'knowledge list selector identity already exists');
END;
CREATE TRIGGER knowledge_list_selector_ordinal_is_declared
BEFORE INSERT ON knowledge_object_list_selector_patterns
WHEN NOT EXISTS (
    SELECT 1
    FROM knowledge_object_list_projections
    WHERE tenant_id = NEW.tenant_id
      AND knowledge_object_id = NEW.knowledge_object_id
      AND object_version = NEW.object_version
      AND NEW.ordinal < CASE NEW.dimension
          WHEN 'index' THEN index_selector_count
          WHEN 'host' THEN host_selector_count
          WHEN 'source' THEN source_selector_count
          WHEN 'sourcetype' THEN sourcetype_selector_count
      END
)
BEGIN
    SELECT RAISE(ABORT, 'knowledge list selector ordinal exceeds declared count');
END;
CREATE TRIGGER knowledge_list_selector_sealed_projection_is_immutable_insert
BEFORE INSERT ON knowledge_object_list_selector_patterns
WHEN EXISTS (
    SELECT 1 FROM knowledge_object_list_projection_seals
    WHERE tenant_id = NEW.tenant_id
      AND knowledge_object_id = NEW.knowledge_object_id
      AND object_version = NEW.object_version
)
BEGIN
    SELECT RAISE(ABORT, 'knowledge list selector set is sealed');
END;
CREATE TRIGGER knowledge_list_selector_update_is_forbidden
BEFORE UPDATE ON knowledge_object_list_selector_patterns
BEGIN
    SELECT RAISE(ABORT, 'knowledge list selector is immutable');
END;
CREATE TRIGGER knowledge_list_selector_sealed_projection_is_immutable_delete
BEFORE DELETE ON knowledge_object_list_selector_patterns
WHEN EXISTS (
    SELECT 1 FROM knowledge_object_list_projection_seals
    WHERE tenant_id = OLD.tenant_id
      AND knowledge_object_id = OLD.knowledge_object_id
      AND object_version = OLD.object_version
)
BEGIN
    SELECT RAISE(ABORT, 'knowledge list selector set is sealed');
END;
CREATE TRIGGER knowledge_list_projection_seal_identity_collision_is_forbidden
BEFORE INSERT ON knowledge_object_list_projection_seals
WHEN EXISTS (
    SELECT 1
    FROM knowledge_object_list_projection_seals
    WHERE tenant_id = NEW.tenant_id
      AND knowledge_object_id = NEW.knowledge_object_id
      AND object_version = NEW.object_version
)
BEGIN
    SELECT RAISE(ABORT, 'knowledge list projection seal already exists');
END;
CREATE TRIGGER knowledge_list_projection_seal_is_complete
BEFORE INSERT ON knowledge_object_list_projection_seals
WHEN NOT EXISTS (
    SELECT 1
    FROM knowledge_object_list_projections AS projection
    WHERE projection.tenant_id = NEW.tenant_id
      AND projection.knowledge_object_id = NEW.knowledge_object_id
      AND projection.object_version = NEW.object_version
      AND projection.projection_bytes = NEW.projection_bytes
      AND projection.canonical_selector_bytes = NEW.canonical_selector_bytes
      AND projection.canonical_selector_bytes = CASE
          WHEN projection.state = 'quarantined' THEN 0
          ELSE 43
              + 4 * (
                  projection.index_selector_count
                  + projection.host_selector_count
                  + projection.source_selector_count
                  + projection.sourcetype_selector_count
              )
              + projection.selector_value_bytes
      END
      AND EXISTS (
          SELECT 1
          FROM (
              SELECT
                  COALESCE(SUM(CASE dimension WHEN 'index' THEN 1 ELSE 0 END), 0)
                      AS index_count,
                  COALESCE(SUM(CASE dimension WHEN 'host' THEN 1 ELSE 0 END), 0)
                      AS host_count,
                  COALESCE(SUM(CASE dimension WHEN 'source' THEN 1 ELSE 0 END), 0)
                      AS source_count,
                  COALESCE(SUM(CASE dimension WHEN 'sourcetype' THEN 1 ELSE 0 END), 0)
                      AS sourcetype_count,
                  COALESCE(SUM(value_bytes), 0) AS value_bytes
              FROM knowledge_object_list_selector_patterns
              WHERE tenant_id = NEW.tenant_id
                AND knowledge_object_id = NEW.knowledge_object_id
                AND object_version = NEW.object_version
          ) AS selector_aggregate
          WHERE selector_aggregate.index_count = projection.index_selector_count
            AND selector_aggregate.host_count = projection.host_selector_count
            AND selector_aggregate.source_count = projection.source_selector_count
            AND selector_aggregate.sourcetype_count = projection.sourcetype_selector_count
            AND selector_aggregate.value_bytes = projection.selector_value_bytes
      )
      AND NOT EXISTS (
          SELECT 1
          FROM knowledge_object_list_selector_patterns AS current_pattern
          JOIN knowledge_object_list_selector_patterns AS previous_pattern
            ON previous_pattern.tenant_id = current_pattern.tenant_id
           AND previous_pattern.knowledge_object_id = current_pattern.knowledge_object_id
           AND previous_pattern.object_version = current_pattern.object_version
           AND previous_pattern.dimension = current_pattern.dimension
           AND previous_pattern.ordinal = current_pattern.ordinal - 1
          WHERE current_pattern.tenant_id = NEW.tenant_id
            AND current_pattern.knowledge_object_id = NEW.knowledge_object_id
            AND current_pattern.object_version = NEW.object_version
            AND CAST(previous_pattern.value AS BLOB)
                >= CAST(current_pattern.value AS BLOB)
      )
)
BEGIN
    SELECT RAISE(ABORT, 'knowledge list projection is incomplete');
END;
CREATE TRIGGER knowledge_list_projection_seal_update_is_forbidden
BEFORE UPDATE ON knowledge_object_list_projection_seals
BEGIN
    SELECT RAISE(ABORT, 'knowledge list projection seal is immutable');
END;
CREATE TRIGGER knowledge_list_projection_current_seal_delete_is_forbidden
BEFORE DELETE ON knowledge_object_list_projection_seals
WHEN EXISTS (
    SELECT 1
    FROM knowledge_objects
    WHERE tenant_id = OLD.tenant_id
      AND knowledge_object_id = OLD.knowledge_object_id
      AND current_version = OLD.object_version
)
BEGIN
    SELECT RAISE(ABORT, 'current knowledge list projection seal cannot be deleted');
END;
CREATE TRIGGER knowledge_object_insert_requires_sealed_list_projection
BEFORE INSERT ON knowledge_objects
WHEN NOT EXISTS (
    SELECT 1
    FROM knowledge_object_list_projections AS projection
    JOIN knowledge_object_list_projection_seals AS seal
      ON seal.tenant_id = projection.tenant_id
     AND seal.knowledge_object_id = projection.knowledge_object_id
     AND seal.object_version = projection.object_version
     AND seal.projection_bytes = projection.projection_bytes
    WHERE projection.tenant_id = NEW.tenant_id
      AND projection.knowledge_object_id = NEW.knowledge_object_id
      AND projection.object_version = NEW.current_version
      AND projection.app_id = NEW.app_id
      AND projection.owner_id = NEW.owner_id
      AND projection.object_type = NEW.object_type
      AND projection.name = NEW.name
      AND projection.sharing_scope = NEW.sharing_scope
      AND projection.state = NEW.state
)
BEGIN
    SELECT RAISE(ABORT, 'knowledge object requires exact sealed list projection');
END;
CREATE TRIGGER knowledge_object_update_requires_sealed_list_projection
BEFORE UPDATE ON knowledge_objects
WHEN NOT EXISTS (
    SELECT 1
    FROM knowledge_object_list_projections AS projection
    JOIN knowledge_object_list_projection_seals AS seal
      ON seal.tenant_id = projection.tenant_id
     AND seal.knowledge_object_id = projection.knowledge_object_id
     AND seal.object_version = projection.object_version
     AND seal.projection_bytes = projection.projection_bytes
    WHERE projection.tenant_id = NEW.tenant_id
      AND projection.knowledge_object_id = NEW.knowledge_object_id
      AND projection.object_version = NEW.current_version
      AND projection.app_id = NEW.app_id
      AND projection.owner_id = NEW.owner_id
      AND projection.object_type = NEW.object_type
      AND projection.name = NEW.name
      AND projection.sharing_scope = NEW.sharing_scope
      AND projection.state = NEW.state
)
BEGIN
    SELECT RAISE(ABORT, 'knowledge object requires exact sealed list projection');
END;
CREATE TRIGGER knowledge_object_update_removes_old_list_projection
AFTER UPDATE ON knowledge_objects
WHEN NEW.current_version <> OLD.current_version
BEGIN
    DELETE FROM knowledge_object_list_projection_seals
    WHERE tenant_id = OLD.tenant_id
      AND knowledge_object_id = OLD.knowledge_object_id
      AND object_version = OLD.current_version;

    DELETE FROM knowledge_object_list_selector_patterns
    WHERE tenant_id = OLD.tenant_id
      AND knowledge_object_id = OLD.knowledge_object_id
      AND object_version = OLD.current_version;

    DELETE FROM knowledge_object_list_projections
    WHERE tenant_id = OLD.tenant_id
      AND knowledge_object_id = OLD.knowledge_object_id
      AND object_version = OLD.current_version;
END;
CREATE TRIGGER audit_tenant_state_transition_is_valid
BEFORE UPDATE ON audit_tenant_state
WHEN NOT (
    NEW.tenant_id = OLD.tenant_id
    AND OLD.event_count BETWEEN 0 AND 99999
    AND OLD.next_sequence BETWEEN 1 AND 100000
    AND NEW.event_count = OLD.event_count + 1
    AND NEW.next_sequence = OLD.next_sequence + 1
    AND EXISTS (
        SELECT 1
        FROM audit_events
        WHERE tenant_id = NEW.tenant_id
          AND sequence = NEW.event_count
    )
)
BEGIN
    SELECT RAISE(ABORT, 'audit tenant state transition is invalid');
END;
CREATE TRIGGER audit_event_identity_collision_is_forbidden
BEFORE INSERT ON audit_events
WHEN EXISTS (
    SELECT 1
    FROM audit_events
    WHERE tenant_id = NEW.tenant_id
      AND sequence = NEW.sequence
)
BEGIN
    SELECT RAISE(ABORT, 'audit event identity already exists');
END;
CREATE TRIGGER audit_event_insert_requires_current_tenant_state
BEFORE INSERT ON audit_events
WHEN NOT EXISTS (
    SELECT 1
    FROM audit_tenant_state
    WHERE tenant_id = NEW.tenant_id
      AND event_count BETWEEN 0 AND 99999
      AND next_sequence = NEW.sequence
)
BEGIN
    SELECT RAISE(
        ABORT,
        'audit tenant state is invalid or capacity is exhausted'
    );
END;
CREATE TRIGGER audit_event_advances_tenant_state
AFTER INSERT ON audit_events
BEGIN
    UPDATE audit_tenant_state
    SET next_sequence = next_sequence + 1,
        event_count = event_count + 1
    WHERE tenant_id = NEW.tenant_id
      AND next_sequence = NEW.sequence
      AND event_count = NEW.sequence - 1;

    SELECT CASE
        WHEN changes() <> 1
        THEN RAISE(ABORT, 'audit event accounting failed')
    END;
END;
CREATE TRIGGER audit_event_update_is_forbidden
BEFORE UPDATE ON audit_events
BEGIN
    SELECT RAISE(ABORT, 'audit events cannot be updated');
END;
CREATE TRIGGER audit_event_delete_is_forbidden
BEFORE DELETE ON audit_events
BEGIN
    SELECT RAISE(ABORT, 'audit events cannot be deleted');
END;
CREATE TRIGGER knowledge_attempt_audit_state_identity_collision_is_forbidden
BEFORE INSERT ON knowledge_attempt_audit_tenant_state
WHEN EXISTS (
    SELECT 1
    FROM knowledge_attempt_audit_tenant_state
    WHERE tenant_id = NEW.tenant_id
)
BEGIN
    SELECT RAISE(ABORT, 'knowledge-attempt audit tenant state already exists');
END;
CREATE TRIGGER knowledge_attempt_audit_state_initial_shape_is_valid
BEFORE INSERT ON knowledge_attempt_audit_tenant_state
WHEN NEW.first_sequence <> 1
  OR NEW.next_sequence <> 1
  OR NEW.retained_count <> 0
BEGIN
    SELECT RAISE(ABORT, 'knowledge-attempt audit tenant state must begin empty');
END;
CREATE TRIGGER knowledge_attempt_audit_state_delete_is_forbidden
BEFORE DELETE ON knowledge_attempt_audit_tenant_state
BEGIN
    SELECT RAISE(ABORT, 'knowledge-attempt audit tenant state cannot be deleted');
END;
CREATE TRIGGER knowledge_attempt_audit_state_transition_is_valid
BEFORE UPDATE ON knowledge_attempt_audit_tenant_state
WHEN NOT (
    NEW.tenant_id = OLD.tenant_id
    AND (
        (
            OLD.retained_count BETWEEN 0 AND 100000
            AND OLD.next_sequence BETWEEN 1 AND 9223372036854775806
            AND NEW.first_sequence = OLD.first_sequence
            AND NEW.next_sequence = OLD.next_sequence + 1
            AND NEW.retained_count = OLD.retained_count + 1
            AND EXISTS (
                SELECT 1
                FROM knowledge_attempt_audit_events
                WHERE tenant_id = NEW.tenant_id
                  AND sequence = OLD.next_sequence
            )
        )
        OR (
            OLD.retained_count = 100001
            AND NEW.first_sequence = OLD.first_sequence + 1
            AND NEW.next_sequence = OLD.next_sequence
            AND NEW.retained_count = OLD.retained_count - 1
            AND NOT EXISTS (
                SELECT 1
                FROM knowledge_attempt_audit_events
                WHERE tenant_id = NEW.tenant_id
                  AND sequence = OLD.first_sequence
            )
        )
    )
)
BEGIN
    SELECT RAISE(ABORT, 'knowledge-attempt audit tenant state transition is invalid');
END;
CREATE TRIGGER knowledge_attempt_audit_event_identity_collision_is_forbidden
BEFORE INSERT ON knowledge_attempt_audit_events
WHEN EXISTS (
    SELECT 1
    FROM knowledge_attempt_audit_events
    WHERE tenant_id = NEW.tenant_id
      AND sequence = NEW.sequence
)
BEGIN
    SELECT RAISE(ABORT, 'knowledge-attempt audit event identity already exists');
END;
CREATE TRIGGER knowledge_attempt_audit_event_insert_requires_current_state
BEFORE INSERT ON knowledge_attempt_audit_events
WHEN NOT EXISTS (
    SELECT 1
    FROM knowledge_attempt_audit_tenant_state
    WHERE tenant_id = NEW.tenant_id
      AND retained_count BETWEEN 0 AND 100000
      AND next_sequence = NEW.sequence
      AND next_sequence BETWEEN 1 AND 9223372036854775806
)
BEGIN
    SELECT RAISE(
        ABORT,
        'knowledge-attempt audit tenant state is invalid or sequence is exhausted'
    );
END;
CREATE TRIGGER knowledge_attempt_audit_event_advances_and_prunes
AFTER INSERT ON knowledge_attempt_audit_events
BEGIN
    UPDATE knowledge_attempt_audit_tenant_state
    SET next_sequence = next_sequence + 1,
        retained_count = retained_count + 1
    WHERE tenant_id = NEW.tenant_id
      AND next_sequence = NEW.sequence
      AND retained_count BETWEEN 0 AND 100000;

    SELECT CASE
        WHEN changes() <> 1
        THEN RAISE(ABORT, 'knowledge-attempt audit event accounting failed')
    END;

    DELETE FROM knowledge_attempt_audit_events
    WHERE tenant_id = NEW.tenant_id
      AND sequence = (
          SELECT first_sequence
          FROM knowledge_attempt_audit_tenant_state
          WHERE tenant_id = NEW.tenant_id
            AND retained_count = 100001
      );

    UPDATE knowledge_attempt_audit_tenant_state
    SET first_sequence = first_sequence + 1,
        retained_count = retained_count - 1
    WHERE tenant_id = NEW.tenant_id
      AND retained_count = 100001;

    SELECT CASE
        WHEN NOT EXISTS (
            SELECT 1
            FROM knowledge_attempt_audit_tenant_state
            WHERE tenant_id = NEW.tenant_id
              AND next_sequence = NEW.sequence + 1
              AND retained_count BETWEEN 1 AND 100000
              AND next_sequence - first_sequence = retained_count
        )
        THEN RAISE(ABORT, 'knowledge-attempt audit prune postcondition failed')
    END;
END;
CREATE TRIGGER knowledge_attempt_audit_event_update_is_forbidden
BEFORE UPDATE ON knowledge_attempt_audit_events
BEGIN
    SELECT RAISE(ABORT, 'knowledge-attempt audit events cannot be updated');
END;
CREATE TRIGGER knowledge_attempt_audit_event_delete_requires_rolling_prune
BEFORE DELETE ON knowledge_attempt_audit_events
WHEN NOT EXISTS (
    SELECT 1
    FROM knowledge_attempt_audit_tenant_state
    WHERE tenant_id = OLD.tenant_id
      AND first_sequence = OLD.sequence
      AND retained_count = 100001
)
BEGIN
    SELECT RAISE(ABORT, 'knowledge-attempt audit event deletion is not a rolling prune');
END;
CREATE TRIGGER knowledge_attempt_audit_event_prune_advances_state
AFTER DELETE ON knowledge_attempt_audit_events
BEGIN
    UPDATE knowledge_attempt_audit_tenant_state
    SET first_sequence = first_sequence + 1,
        retained_count = retained_count - 1
    WHERE tenant_id = OLD.tenant_id
      AND first_sequence = OLD.sequence
      AND retained_count = 100001;

    SELECT CASE
        WHEN changes() <> 1
        THEN RAISE(ABORT, 'knowledge-attempt audit rolling prune accounting failed')
    END;
END;
CREATE TRIGGER knowledge_catalog_revision_head_identity_collision_is_forbidden
BEFORE INSERT ON knowledge_catalog_revision_heads
WHEN EXISTS (
    SELECT 1 FROM knowledge_catalog_revision_heads
    WHERE tenant_id = NEW.tenant_id
)
BEGIN
    SELECT RAISE(ABORT, 'knowledge catalog revision head already exists');
END;
CREATE TRIGGER knowledge_catalog_revision_head_insert_agrees_with_tenant
BEFORE INSERT ON knowledge_catalog_revision_heads
WHEN NOT EXISTS (
    SELECT 1 FROM knowledge_catalog_tenants
    WHERE tenant_id = NEW.tenant_id
      AND catalog_revision = NEW.catalog_revision
)
BEGIN
    SELECT RAISE(ABORT, 'knowledge catalog revision head disagrees with tenant');
END;
CREATE TRIGGER knowledge_catalog_revision_head_transition_is_exact
BEFORE UPDATE ON knowledge_catalog_revision_heads
WHEN NEW.tenant_id <> OLD.tenant_id
  OR NEW.catalog_revision <> OLD.catalog_revision + 1
  OR length(NEW.state_token) <> 32
  OR NEW.state_token = OLD.state_token
  OR NOT EXISTS (
      SELECT 1 FROM knowledge_catalog_tenants
      WHERE tenant_id = OLD.tenant_id
        AND catalog_revision = NEW.catalog_revision
  )
BEGIN
    SELECT RAISE(ABORT, 'knowledge catalog revision head transition is invalid');
END;
CREATE TRIGGER knowledge_catalog_revision_head_delete_is_forbidden
BEFORE DELETE ON knowledge_catalog_revision_heads
BEGIN
    SELECT RAISE(ABORT, 'knowledge catalog revision head cannot be deleted');
END;
CREATE TRIGGER knowledge_catalog_tenant_creates_revision_head
AFTER INSERT ON knowledge_catalog_tenants
BEGIN
    INSERT INTO knowledge_catalog_revision_heads (
        tenant_id, catalog_revision, state_token
    ) VALUES (NEW.tenant_id, NEW.catalog_revision, randomblob(32));
END;
CREATE TRIGGER knowledge_catalog_revision_requires_exact_head
BEFORE UPDATE OF catalog_revision ON knowledge_catalog_tenants
WHEN NEW.catalog_revision <> OLD.catalog_revision
 AND NOT EXISTS (
     SELECT 1 FROM knowledge_catalog_revision_heads
     WHERE tenant_id = OLD.tenant_id
       AND catalog_revision = OLD.catalog_revision
       AND length(state_token) = 32
 )
BEGIN
    SELECT RAISE(ABORT, 'knowledge catalog revision head is missing');
END;
CREATE TRIGGER knowledge_catalog_revision_rotates_state_token
AFTER UPDATE OF catalog_revision ON knowledge_catalog_tenants
WHEN NEW.catalog_revision <> OLD.catalog_revision
BEGIN
    UPDATE knowledge_catalog_revision_heads
    SET catalog_revision = NEW.catalog_revision,
        state_token = randomblob(32)
    WHERE tenant_id = OLD.tenant_id
      AND catalog_revision = OLD.catalog_revision;

    SELECT CASE WHEN NOT EXISTS (
        SELECT 1 FROM knowledge_catalog_revision_heads
        WHERE tenant_id = NEW.tenant_id
          AND catalog_revision = NEW.catalog_revision
          AND length(state_token) = 32
    ) THEN RAISE(ABORT, 'knowledge catalog revision head rotation failed') END;
END;
CREATE TRIGGER knowledge_object_version_lifecycle_identity_collision_is_forbidden
BEFORE INSERT ON knowledge_object_version_lifecycle
WHEN EXISTS (
    SELECT 1 FROM knowledge_object_version_lifecycle
    WHERE tenant_id = NEW.tenant_id
      AND knowledge_object_id = NEW.knowledge_object_id
      AND object_version = NEW.object_version
)
BEGIN
    SELECT RAISE(ABORT, 'knowledge object version lifecycle already exists');
END;
CREATE TRIGGER knowledge_object_version_lifecycle_agrees_with_version
BEFORE INSERT ON knowledge_object_version_lifecycle
WHEN NOT EXISTS (
    SELECT 1
    FROM knowledge_object_versions AS version
    WHERE version.tenant_id = NEW.tenant_id
      AND version.knowledge_object_id = NEW.knowledge_object_id
      AND version.object_version = NEW.object_version
      AND NEW.state = version.state
      AND (
          (
              version.state IN ('draft', 'active')
              AND NEW.disabled_at_unix_micro IS NULL
              AND NEW.quarantined_at_unix_micro IS NULL
              AND NEW.deleted_at_unix_micro IS NULL
              AND NEW.quarantine_reason IS NULL
          )
          OR (
              version.state = 'disabled'
              AND NEW.disabled_at_unix_micro IS NOT NULL
              AND NEW.disabled_at_unix_micro <= version.created_at_unix_micro
              AND NEW.disabled_at_unix_micro = (
                  SELECT disabled_version.created_at_unix_micro
                  FROM knowledge_object_versions AS disabled_version
                  WHERE disabled_version.tenant_id = version.tenant_id
                    AND disabled_version.knowledge_object_id = version.knowledge_object_id
                    AND disabled_version.object_version <= version.object_version
                    AND disabled_version.mutation_kind = 'disable'
                  ORDER BY disabled_version.object_version DESC
                  LIMIT 1
              )
              AND NEW.quarantined_at_unix_micro IS NULL
              AND NEW.deleted_at_unix_micro IS NULL
              AND NEW.quarantine_reason IS NULL
          )
          OR (
              version.state = 'quarantined'
              AND NEW.disabled_at_unix_micro IS NULL
              AND NEW.quarantined_at_unix_micro = version.created_at_unix_micro
              AND NEW.deleted_at_unix_micro IS NULL
              AND NEW.quarantine_reason = version.quarantine_reason
          )
          OR (
              version.state = 'deleted'
              AND NEW.disabled_at_unix_micro IS NULL
              AND NEW.quarantined_at_unix_micro IS NULL
              AND NEW.deleted_at_unix_micro = version.created_at_unix_micro
              AND NEW.quarantine_reason IS NULL
          )
      )
)
BEGIN
    SELECT RAISE(ABORT, 'knowledge object version lifecycle disagrees with version');
END;
CREATE TRIGGER knowledge_object_version_lifecycle_update_is_forbidden
BEFORE UPDATE ON knowledge_object_version_lifecycle
BEGIN
    SELECT RAISE(ABORT, 'knowledge object version lifecycle is immutable');
END;
CREATE TRIGGER knowledge_object_version_lifecycle_delete_is_forbidden
BEFORE DELETE ON knowledge_object_version_lifecycle
BEGIN
    SELECT RAISE(ABORT, 'knowledge object version lifecycle cannot be deleted');
END;
CREATE TRIGGER knowledge_object_version_transition_is_exact
BEFORE INSERT ON knowledge_object_versions
WHEN NEW.object_version > 65536
  OR (
      NEW.object_version = 1
      AND NOT (
          NEW.mutation_kind = 'create'
          AND NEW.state IN ('draft', 'active')
      )
  )
  OR (
      NEW.object_version > 1
      AND NOT EXISTS (
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
  )
BEGIN
    SELECT RAISE(ABORT, 'knowledge object version transition is invalid');
END;
CREATE TRIGGER knowledge_object_version_creates_lifecycle
AFTER INSERT ON knowledge_object_versions
BEGIN
    INSERT INTO knowledge_object_version_lifecycle (
        tenant_id, knowledge_object_id, object_version,
        state,
        disabled_at_unix_micro, quarantined_at_unix_micro,
        deleted_at_unix_micro, quarantine_reason
    ) VALUES (
        NEW.tenant_id,
        NEW.knowledge_object_id,
        NEW.object_version,
        NEW.state,
        CASE
            WHEN NEW.state = 'disabled' AND NEW.mutation_kind = 'disable'
                THEN NEW.created_at_unix_micro
            WHEN NEW.state = 'disabled' THEN (
                SELECT disabled_at_unix_micro
                FROM knowledge_object_version_lifecycle
                WHERE tenant_id = NEW.tenant_id
                  AND knowledge_object_id = NEW.knowledge_object_id
                  AND object_version = NEW.object_version - 1
            )
        END,
        CASE WHEN NEW.state = 'quarantined'
             THEN NEW.created_at_unix_micro END,
        CASE WHEN NEW.state = 'deleted'
             THEN NEW.created_at_unix_micro END,
        CASE WHEN NEW.state = 'quarantined'
             THEN NEW.quarantine_reason END
    );
END;
CREATE TRIGGER knowledge_list_order_key_identity_collision_is_forbidden
BEFORE INSERT ON knowledge_object_list_order_keys
WHEN EXISTS (
    SELECT 1 FROM knowledge_object_list_order_keys
    WHERE tenant_id = NEW.tenant_id
      AND knowledge_object_id = NEW.knowledge_object_id
      AND object_version = NEW.object_version
)
BEGIN
    SELECT RAISE(ABORT, 'knowledge list order key already exists');
END;
CREATE TRIGGER knowledge_list_order_key_agrees_with_authorities
BEFORE INSERT ON knowledge_object_list_order_keys
WHEN NOT EXISTS (
    SELECT 1
    FROM knowledge_object_list_projections AS projection
    JOIN knowledge_object_versions AS version
      ON version.tenant_id = projection.tenant_id
     AND version.knowledge_object_id = projection.knowledge_object_id
     AND version.object_version = projection.object_version
    JOIN knowledge_object_versions AS creation_version
      ON creation_version.tenant_id = projection.tenant_id
     AND creation_version.knowledge_object_id = projection.knowledge_object_id
     AND creation_version.object_version = 1
    WHERE projection.tenant_id = NEW.tenant_id
      AND projection.knowledge_object_id = NEW.knowledge_object_id
      AND projection.object_version = NEW.object_version
      AND NEW.updated_at_unix_micro = version.created_at_unix_micro
      AND NEW.created_at_unix_micro = creation_version.created_at_unix_micro
)
BEGIN
    SELECT RAISE(ABORT, 'knowledge list order key disagrees with authorities');
END;
CREATE TRIGGER knowledge_list_order_key_update_is_forbidden
BEFORE UPDATE ON knowledge_object_list_order_keys
BEGIN
    SELECT RAISE(ABORT, 'knowledge list order key is immutable');
END;
CREATE TRIGGER knowledge_list_order_key_sealed_delete_is_forbidden
BEFORE DELETE ON knowledge_object_list_order_keys
WHEN EXISTS (
    SELECT 1 FROM knowledge_object_list_projection_seals
    WHERE tenant_id = OLD.tenant_id
      AND knowledge_object_id = OLD.knowledge_object_id
      AND object_version = OLD.object_version
)
BEGIN
    SELECT RAISE(ABORT, 'sealed knowledge list order key cannot be deleted');
END;
CREATE TRIGGER knowledge_list_projection_creates_order_key
AFTER INSERT ON knowledge_object_list_projections
WHEN EXISTS (
    SELECT 1 FROM knowledge_object_versions
    WHERE tenant_id = NEW.tenant_id
      AND knowledge_object_id = NEW.knowledge_object_id
      AND object_version = NEW.object_version
)
BEGIN
    INSERT INTO knowledge_object_list_order_keys (
        tenant_id, knowledge_object_id, object_version,
        created_at_unix_micro, updated_at_unix_micro
    )
    SELECT NEW.tenant_id,
           NEW.knowledge_object_id,
           NEW.object_version,
           creation_version.created_at_unix_micro,
           version.created_at_unix_micro
    FROM knowledge_object_versions AS version
    JOIN knowledge_object_versions AS creation_version
      ON creation_version.tenant_id = version.tenant_id
     AND creation_version.knowledge_object_id = version.knowledge_object_id
     AND creation_version.object_version = 1
    WHERE version.tenant_id = NEW.tenant_id
      AND version.knowledge_object_id = NEW.knowledge_object_id
      AND version.object_version = NEW.object_version;
END;
CREATE TRIGGER knowledge_object_version_creates_staged_order_key
AFTER INSERT ON knowledge_object_versions
WHEN EXISTS (
    SELECT 1 FROM knowledge_object_list_projections
    WHERE tenant_id = NEW.tenant_id
      AND knowledge_object_id = NEW.knowledge_object_id
      AND object_version = NEW.object_version
)
 AND NOT EXISTS (
    SELECT 1 FROM knowledge_object_list_order_keys
    WHERE tenant_id = NEW.tenant_id
      AND knowledge_object_id = NEW.knowledge_object_id
      AND object_version = NEW.object_version
)
BEGIN
    INSERT INTO knowledge_object_list_order_keys (
        tenant_id, knowledge_object_id, object_version,
        created_at_unix_micro, updated_at_unix_micro
    )
    SELECT NEW.tenant_id,
           NEW.knowledge_object_id,
           NEW.object_version,
           creation_version.created_at_unix_micro,
           NEW.created_at_unix_micro
    FROM knowledge_object_versions AS creation_version
    WHERE creation_version.tenant_id = NEW.tenant_id
      AND creation_version.knowledge_object_id = NEW.knowledge_object_id
      AND creation_version.object_version = 1;
END;
CREATE TRIGGER knowledge_list_projection_seal_requires_order_key
BEFORE INSERT ON knowledge_object_list_projection_seals
WHEN NOT EXISTS (
    SELECT 1 FROM knowledge_object_list_order_keys
    WHERE tenant_id = NEW.tenant_id
      AND knowledge_object_id = NEW.knowledge_object_id
      AND object_version = NEW.object_version
)
BEGIN
    SELECT RAISE(ABORT, 'knowledge list projection lacks exact order key');
END;
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
CREATE TRIGGER app_catalog_revision_identity_collision_is_forbidden
BEFORE INSERT ON app_catalog_revisions
WHEN EXISTS (
    SELECT 1
    FROM app_catalog_revisions AS authority
    WHERE authority.tenant_id = NEW.tenant_id
)
BEGIN
    SELECT RAISE(ABORT, 'app catalog revision authority already exists');
END;
CREATE TRIGGER app_catalog_revision_initial_shape_is_exact
BEFORE INSERT ON app_catalog_revisions
WHEN NEW.revision <> 1
  OR NOT EXISTS (
      SELECT 1
      FROM app_workspaces AS app
      WHERE app.tenant_id = NEW.tenant_id
  )
BEGIN
    SELECT RAISE(ABORT, 'app catalog revision authority must begin with its first app');
END;
CREATE TRIGGER app_catalog_revision_tenant_is_immutable
BEFORE UPDATE OF tenant_id ON app_catalog_revisions
WHEN NEW.tenant_id <> OLD.tenant_id
BEGIN
    SELECT RAISE(ABORT, 'app catalog revision tenant is immutable');
END;
CREATE TRIGGER app_catalog_revision_transition_is_exact
BEFORE UPDATE OF revision ON app_catalog_revisions
WHEN OLD.revision < 1
  OR NEW.revision <> OLD.revision + 1
BEGIN
    SELECT RAISE(ABORT, 'app catalog revision must advance by exactly one');
END;
CREATE TRIGGER app_catalog_revision_delete_is_forbidden
BEFORE DELETE ON app_catalog_revisions
BEGIN
    SELECT RAISE(ABORT, 'app catalog revision authority cannot be deleted');
END;
CREATE TRIGGER app_catalog_revision_after_insert
AFTER INSERT ON app_workspaces
BEGIN
    UPDATE app_catalog_revisions
    SET revision = revision + 1
    WHERE tenant_id = NEW.tenant_id;

    INSERT INTO app_catalog_revisions (tenant_id, revision)
    SELECT NEW.tenant_id, 1
    WHERE NOT EXISTS (
        SELECT 1
        FROM app_catalog_revisions AS authority
        WHERE authority.tenant_id = NEW.tenant_id
    )
      AND (
          SELECT count(*)
          FROM app_workspaces AS app
          WHERE app.tenant_id = NEW.tenant_id
      ) = 1;

    SELECT CASE WHEN NOT EXISTS (
        SELECT 1
        FROM app_catalog_revisions AS authority
        WHERE authority.tenant_id = NEW.tenant_id
    ) THEN RAISE(ABORT, 'app catalog revision authority is missing') END;
END;
CREATE TRIGGER app_catalog_revision_after_update
AFTER UPDATE ON app_workspaces
BEGIN
    SELECT CASE WHEN NOT EXISTS (
        SELECT 1
        FROM app_catalog_revisions AS authority
        WHERE authority.tenant_id = NEW.tenant_id
    ) THEN RAISE(ABORT, 'app catalog revision authority is missing') END;

    UPDATE app_catalog_revisions
    SET revision = revision + 1
    WHERE tenant_id = NEW.tenant_id;
END;
CREATE TRIGGER app_catalog_revision_after_delete
AFTER DELETE ON app_workspaces
BEGIN
    SELECT CASE WHEN NOT EXISTS (
        SELECT 1
        FROM app_catalog_revisions AS authority
        WHERE authority.tenant_id = OLD.tenant_id
    ) THEN RAISE(ABORT, 'app catalog revision authority is missing') END;

    UPDATE app_catalog_revisions
    SET revision = revision + 1
    WHERE tenant_id = OLD.tenant_id;
END;
CREATE TRIGGER app_catalog_revision_provisions_knowledge_catalog_after_insert
AFTER INSERT ON app_catalog_revisions
BEGIN
    -- Refuse to conceal a partial authority that could have been persisted by
    -- a connection with foreign-key enforcement disabled.  A valid existing
    -- tenant/head may still receive its safely reconstructible empty ledger.
    SELECT CASE WHEN EXISTS (
        SELECT 1
        FROM knowledge_catalog_tenants AS tenant
        WHERE tenant.tenant_id = NEW.tenant_id
          AND NOT EXISTS (
              SELECT 1
              FROM knowledge_catalog_revision_heads AS head
              WHERE head.tenant_id = tenant.tenant_id
                AND head.catalog_revision = tenant.catalog_revision
                AND length(head.state_token) = 32
          )
    )
    OR EXISTS (
        SELECT 1
        FROM knowledge_catalog_revision_heads AS head
        WHERE head.tenant_id = NEW.tenant_id
          AND NOT EXISTS (
              SELECT 1
              FROM knowledge_catalog_tenants AS tenant
              WHERE tenant.tenant_id = head.tenant_id
                AND tenant.catalog_revision = head.catalog_revision
          )
    )
    OR EXISTS (
        SELECT 1
        FROM knowledge_projection_tenant_ledgers AS ledger
        WHERE ledger.tenant_id = NEW.tenant_id
          AND (
              NOT EXISTS (
                  SELECT 1
                  FROM knowledge_catalog_tenants AS tenant
                  WHERE tenant.tenant_id = ledger.tenant_id
              )
              OR ledger.projection_bytes <> COALESCE((
                  SELECT sum(projection.projection_bytes)
                  FROM knowledge_object_list_projections AS projection
                  WHERE projection.tenant_id = ledger.tenant_id
              ), 0)
          )
    )
    OR EXISTS (
        SELECT 1
        FROM knowledge_object_list_projections AS projection
        WHERE projection.tenant_id = NEW.tenant_id
          AND NOT EXISTS (
              SELECT 1
              FROM knowledge_projection_tenant_ledgers AS ledger
              WHERE ledger.tenant_id = projection.tenant_id
          )
    ) THEN RAISE(
        ABORT,
        'app catalog tenant knowledge prestate is incomplete or corrupt'
    ) END;

    INSERT INTO knowledge_catalog_tenants (tenant_id)
    SELECT NEW.tenant_id
    WHERE NOT EXISTS (
        SELECT 1
        FROM knowledge_catalog_tenants AS tenant
        WHERE tenant.tenant_id = NEW.tenant_id
    );

    INSERT INTO knowledge_projection_tenant_ledgers (tenant_id)
    SELECT NEW.tenant_id
    WHERE NOT EXISTS (
        SELECT 1
        FROM knowledge_projection_tenant_ledgers AS ledger
        WHERE ledger.tenant_id = NEW.tenant_id
    );

    SELECT CASE WHEN NOT EXISTS (
        SELECT 1
        FROM knowledge_catalog_tenants AS tenant
        JOIN knowledge_catalog_revision_heads AS head
          ON head.tenant_id = tenant.tenant_id
         AND head.catalog_revision = tenant.catalog_revision
        JOIN knowledge_projection_tenant_ledgers AS ledger
          ON ledger.tenant_id = tenant.tenant_id
        WHERE tenant.tenant_id = NEW.tenant_id
          AND length(head.state_token) = 32
          AND ledger.projection_bytes = COALESCE((
              SELECT sum(projection.projection_bytes)
              FROM knowledge_object_list_projections AS projection
              WHERE projection.tenant_id = tenant.tenant_id
          ), 0)
    ) THEN RAISE(
        ABORT,
        'app catalog tenant knowledge authority is incomplete or corrupt'
    ) END;
END
;
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
CREATE TRIGGER ingestion_token_collector_binding_is_required
BEFORE INSERT ON ingestion_tokens
WHEN NEW.purpose = 'native_collector'
     AND NEW.bound_collector_id IS NULL
BEGIN
    SELECT RAISE(ABORT, 'ingestion token collector binding is required');
END;
CREATE TRIGGER ingestion_token_hec_collector_binding_is_forbidden
BEFORE INSERT ON ingestion_tokens
WHEN NEW.purpose = 'hec'
     AND NEW.bound_collector_id IS NOT NULL
BEGIN
    SELECT RAISE(ABORT, 'HEC ingestion token cannot have a collector binding');
END;
CREATE TRIGGER ingestion_token_hec_collector_binding_update_is_forbidden
BEFORE UPDATE OF bound_collector_id ON ingestion_tokens
WHEN NEW.purpose = 'hec'
     AND NEW.bound_collector_id IS NOT NULL
BEGIN
    SELECT RAISE(ABORT, 'HEC ingestion token cannot have a collector binding');
END;
CREATE TRIGGER ingestion_token_purpose_is_immutable
BEFORE UPDATE OF purpose ON ingestion_tokens
WHEN NEW.purpose <> OLD.purpose
BEGIN
    SELECT RAISE(ABORT, 'ingestion token purpose is immutable');
END;
CREATE TRIGGER ingestion_token_hec_profile_requires_hec_purpose
BEFORE INSERT ON ingestion_token_hec_profiles
WHEN NOT EXISTS (
    SELECT 1
    FROM ingestion_tokens AS token
    WHERE token.ingestion_token_id = NEW.ingestion_token_id
      AND token.purpose = 'hec'
      AND token.bound_collector_id IS NULL
)
BEGIN
    SELECT RAISE(ABORT, 'HEC token profile requires an unbound HEC token');
END;
CREATE TRIGGER ingestion_token_hec_profile_owner_is_immutable
BEFORE UPDATE OF ingestion_token_id ON ingestion_token_hec_profiles
WHEN NEW.ingestion_token_id <> OLD.ingestion_token_id
BEGIN
    SELECT RAISE(ABORT, 'HEC token profile owner is immutable');
END;
CREATE TRIGGER ingestion_token_hec_acknowledgment_is_immutable
BEFORE UPDATE OF indexer_acknowledgment ON ingestion_token_hec_profiles
WHEN NEW.indexer_acknowledgment <> OLD.indexer_acknowledgment
BEGIN
    SELECT RAISE(ABORT, 'HEC token acknowledgment mode is immutable');
END;
CREATE TRIGGER hec_request_visibility_committed
AFTER UPDATE OF state ON ingest_visibility_reservations
WHEN OLD.state = 'reserved' AND NEW.state = 'committed'
BEGIN
    UPDATE hec_requests
    SET state = 'indexed',
        terminal_at_unix_micro = NEW.committed_at_unix_micro
    WHERE visibility_sequence = NEW.sequence
      AND state = 'pending';
END;
CREATE TRIGGER hec_request_visibility_failed
AFTER UPDATE OF state ON ingest_visibility_reservations
WHEN OLD.state = 'reserved' AND NEW.state IN ('rejected', 'abandoned')
BEGIN
    UPDATE hec_requests
    SET state = 'terminal_failure',
        terminal_at_unix_micro = COALESCE(
            NEW.committed_at_unix_micro,
            CAST(unixepoch('subsec') * 1000000 AS INTEGER)
        )
    WHERE visibility_sequence = NEW.sequence
      AND state = 'pending';
END;
CREATE TRIGGER ingest_write_group_state_transition_is_valid
BEFORE UPDATE OF state ON ingest_write_groups
WHEN NOT (
    (OLD.state = 'ready' AND NEW.state = 'ambiguous')
    OR (OLD.state = 'ambiguous' AND NEW.state = 'committed')
)
BEGIN
    SELECT RAISE(ABORT, 'invalid ingest write group state transition');
END;
CREATE TRIGGER ingest_write_group_identity_is_immutable
BEFORE UPDATE OF write_group_id, member_count, row_count, decoded_bytes,
    membership_sha256, first_sequence, last_sequence, created_at_unix_micro
    ON ingest_write_groups
BEGIN
    SELECT RAISE(ABORT, 'ingest write group identity is immutable');
END;
CREATE TRIGGER ingest_write_group_seal_is_valid
BEFORE UPDATE OF state ON ingest_write_groups
WHEN OLD.state = 'ready' AND NEW.state = 'ambiguous' AND (
    OLD.member_count <> (
        SELECT count(*)
        FROM ingest_write_group_members
        WHERE write_group_id = OLD.write_group_id
    )
    OR OLD.row_count <> (
        SELECT COALESCE(sum(row_count), 0)
        FROM ingest_write_group_members
        WHERE write_group_id = OLD.write_group_id
    )
    OR OLD.decoded_bytes <> (
        SELECT COALESCE(sum(decoded_bytes), 0)
        FROM ingest_write_group_members
        WHERE write_group_id = OLD.write_group_id
    )
    OR OLD.first_sequence <> (
        SELECT min(visibility_sequence)
        FROM ingest_write_group_members
        WHERE write_group_id = OLD.write_group_id
    )
    OR OLD.last_sequence <> (
        SELECT max(visibility_sequence)
        FROM ingest_write_group_members
        WHERE write_group_id = OLD.write_group_id
    )
)
BEGIN
    SELECT RAISE(ABORT, 'ingest write group seal does not match its members');
END;
CREATE TRIGGER ingest_write_group_member_insert_is_valid
BEFORE INSERT ON ingest_write_group_members
WHEN NOT EXISTS (
    SELECT 1
    FROM ingest_write_groups AS write_group
    JOIN ingest_visibility_reservations AS reservation
      ON reservation.sequence = NEW.visibility_sequence
    WHERE write_group.write_group_id = NEW.write_group_id
      AND write_group.state = 'ready'
      AND reservation.state = 'reserved'
      AND reservation.phase = 'unsent'
      AND reservation.attempt_id = ''
      AND reservation.stored_row_count = NEW.row_count
      AND reservation.decoded_event_bytes = NEW.decoded_bytes
      AND reservation.outbox_sha256 = NEW.outbox_sha256
)
BEGIN
    SELECT RAISE(ABORT, 'invalid ingest write group member');
END;
CREATE TRIGGER ingest_write_group_member_insert_is_contiguous
BEFORE INSERT ON ingest_write_group_members
WHEN NEW.ordinal <> (
        SELECT count(*)
        FROM ingest_write_group_members
        WHERE write_group_id = NEW.write_group_id
    )
    OR NEW.ordinal >= (
        SELECT member_count
        FROM ingest_write_groups
        WHERE write_group_id = NEW.write_group_id
    )
BEGIN
    SELECT RAISE(ABORT, 'ingest write group ordinals must be contiguous');
END;
CREATE TRIGGER ingest_write_group_member_insert_is_ordered
BEFORE INSERT ON ingest_write_group_members
WHEN NEW.ordinal > 0 AND NEW.visibility_sequence <= (
    SELECT max(visibility_sequence)
    FROM ingest_write_group_members
    WHERE write_group_id = NEW.write_group_id
)
BEGIN
    SELECT RAISE(ABORT, 'ingest write group member sequences must be ordered');
END;
CREATE TRIGGER ingest_write_group_member_is_immutable
BEFORE UPDATE ON ingest_write_group_members
BEGIN
    SELECT RAISE(ABORT, 'ingest write group membership is immutable');
END;
CREATE TRIGGER ingest_write_group_active_member_delete_is_forbidden
BEFORE DELETE ON ingest_write_group_members
WHEN (
    SELECT state
    FROM ingest_write_groups
    WHERE write_group_id = OLD.write_group_id
) <> 'committed'
BEGIN
    SELECT RAISE(ABORT, 'active ingest write group membership cannot be deleted');
END;
CREATE TRIGGER knowledge_catalog_tenant_provisions_lookup_asset_ledger
AFTER INSERT ON knowledge_catalog_tenants
BEGIN
    INSERT INTO knowledge_lookup_asset_tenant_ledgers (tenant_id)
    VALUES (NEW.tenant_id);
END;
CREATE TRIGGER knowledge_lookup_asset_ledger_identity_collision_is_forbidden
BEFORE INSERT ON knowledge_lookup_asset_tenant_ledgers
WHEN EXISTS (
    SELECT 1 FROM knowledge_lookup_asset_tenant_ledgers
    WHERE tenant_id = NEW.tenant_id
)
BEGIN
    SELECT RAISE(ABORT, 'lookup asset tenant ledger already exists');
END;
CREATE TRIGGER knowledge_lookup_asset_ledger_initial_shape_is_exact
BEFORE INSERT ON knowledge_lookup_asset_tenant_ledgers
WHEN NEW.staged_asset_count <> 0
  OR NEW.asset_identity_count <> 0
  OR NEW.published_version_count <> 0
  OR NEW.stored_content_bytes <> 0
  OR EXISTS (
      SELECT 1 FROM knowledge_lookup_asset_stages
      WHERE tenant_id = NEW.tenant_id
  )
  OR EXISTS (
      SELECT 1 FROM knowledge_lookup_assets
      WHERE tenant_id = NEW.tenant_id
  )
  OR EXISTS (
      SELECT 1 FROM knowledge_lookup_asset_versions
      WHERE tenant_id = NEW.tenant_id
  )
BEGIN
    SELECT RAISE(ABORT, 'lookup asset tenant ledger must begin empty');
END;
CREATE TRIGGER knowledge_lookup_asset_ledger_transition_is_exact
BEFORE UPDATE ON knowledge_lookup_asset_tenant_ledgers
WHEN NEW.tenant_id <> OLD.tenant_id
  OR NEW.staged_asset_count <> (
      SELECT count(*) FROM knowledge_lookup_asset_stages
      WHERE tenant_id = OLD.tenant_id
  )
  OR NEW.asset_identity_count <> (
      SELECT count(*) FROM knowledge_lookup_assets
      WHERE tenant_id = OLD.tenant_id
  )
  OR NEW.published_version_count <> (
      SELECT count(*) FROM knowledge_lookup_asset_versions
      WHERE tenant_id = OLD.tenant_id
  )
  OR NEW.stored_content_bytes <> (
      COALESCE((
          SELECT sum(canonical_bytes) FROM knowledge_lookup_asset_stages
          WHERE tenant_id = OLD.tenant_id
      ), 0)
      + COALESCE((
          SELECT sum(canonical_bytes) FROM knowledge_lookup_asset_versions
          WHERE tenant_id = OLD.tenant_id
      ), 0)
  )
BEGIN
    SELECT RAISE(ABORT, 'lookup asset tenant ledger transition is invalid');
END;
CREATE TRIGGER knowledge_lookup_asset_ledger_delete_is_forbidden
BEFORE DELETE ON knowledge_lookup_asset_tenant_ledgers
BEGIN
    SELECT RAISE(ABORT, 'lookup asset tenant ledger cannot be deleted');
END;
CREATE TRIGGER knowledge_lookup_asset_stage_capacity_is_available
BEFORE INSERT ON knowledge_lookup_asset_stages
WHEN EXISTS (
    SELECT 1
    FROM knowledge_lookup_asset_tenant_ledgers AS ledger
    WHERE ledger.tenant_id = NEW.tenant_id
)
AND NOT EXISTS (
    SELECT 1
    FROM knowledge_lookup_asset_tenant_ledgers AS ledger
    WHERE ledger.tenant_id = NEW.tenant_id
      AND ledger.staged_asset_count < 64
      AND ledger.stored_content_bytes <= 2147483648 - NEW.canonical_bytes
)
BEGIN
    SELECT RAISE(ABORT, 'lookup asset staging capacity exhausted');
END;
CREATE TRIGGER knowledge_lookup_asset_stage_requires_tenant_ledger
BEFORE INSERT ON knowledge_lookup_asset_stages
WHEN NOT EXISTS (
    SELECT 1 FROM knowledge_lookup_asset_tenant_ledgers
    WHERE tenant_id = NEW.tenant_id
)
BEGIN
    SELECT RAISE(ABORT, 'lookup asset tenant ledger is missing');
END;
CREATE TRIGGER knowledge_lookup_asset_stage_updates_are_forbidden
BEFORE UPDATE ON knowledge_lookup_asset_stages
BEGIN
    SELECT RAISE(ABORT, 'lookup asset stages are immutable');
END;
CREATE TRIGGER knowledge_lookup_asset_stage_accounts_after_insert
AFTER INSERT ON knowledge_lookup_asset_stages
BEGIN
    UPDATE knowledge_lookup_asset_tenant_ledgers
    SET staged_asset_count = staged_asset_count + 1,
        stored_content_bytes = stored_content_bytes + NEW.canonical_bytes
    WHERE tenant_id = NEW.tenant_id;
END;
CREATE TRIGGER knowledge_lookup_asset_stage_accounts_after_delete
AFTER DELETE ON knowledge_lookup_asset_stages
BEGIN
    UPDATE knowledge_lookup_asset_tenant_ledgers
    SET staged_asset_count = staged_asset_count - 1,
        stored_content_bytes = stored_content_bytes - OLD.canonical_bytes
    WHERE tenant_id = OLD.tenant_id;
END;
CREATE TRIGGER knowledge_lookup_asset_identity_capacity_is_available
BEFORE INSERT ON knowledge_lookup_assets
WHEN EXISTS (
    SELECT 1
    FROM knowledge_lookup_asset_tenant_ledgers AS ledger
    WHERE ledger.tenant_id = NEW.tenant_id
)
AND (
    NEW.current_version <> 1
    OR NOT EXISTS (
      SELECT 1
      FROM knowledge_lookup_asset_tenant_ledgers AS ledger
      WHERE ledger.tenant_id = NEW.tenant_id
        AND ledger.asset_identity_count < 2048
    )
)
BEGIN
    SELECT RAISE(ABORT, 'lookup asset identity capacity is unavailable or first version is invalid');
END;
CREATE TRIGGER knowledge_lookup_asset_identity_requires_tenant_ledger
BEFORE INSERT ON knowledge_lookup_assets
WHEN NOT EXISTS (
    SELECT 1 FROM knowledge_lookup_asset_tenant_ledgers
    WHERE tenant_id = NEW.tenant_id
)
BEGIN
    SELECT RAISE(ABORT, 'lookup asset tenant ledger is missing');
END;
CREATE TRIGGER knowledge_lookup_asset_identity_accounts_after_insert
AFTER INSERT ON knowledge_lookup_assets
BEGIN
    UPDATE knowledge_lookup_asset_tenant_ledgers
    SET asset_identity_count = asset_identity_count + 1
    WHERE tenant_id = NEW.tenant_id;
END;
CREATE TRIGGER knowledge_lookup_asset_identity_transition_is_exact
BEFORE UPDATE ON knowledge_lookup_assets
WHEN NEW.tenant_id <> OLD.tenant_id
  OR NEW.lookup_asset_id <> OLD.lookup_asset_id
  OR NEW.created_at_unix_micro <> OLD.created_at_unix_micro
  OR NEW.current_version <> OLD.current_version + 1
  OR NEW.updated_at_unix_micro < OLD.updated_at_unix_micro
BEGIN
    SELECT RAISE(ABORT, 'lookup asset current version transition is invalid');
END;
CREATE TRIGGER knowledge_lookup_asset_identity_delete_is_forbidden
BEFORE DELETE ON knowledge_lookup_assets
BEGIN
    SELECT RAISE(ABORT, 'lookup asset identities cannot be deleted');
END;
CREATE TRIGGER knowledge_lookup_asset_version_capacity_is_available
BEFORE INSERT ON knowledge_lookup_asset_versions
WHEN EXISTS (
    SELECT 1
    FROM knowledge_lookup_asset_tenant_ledgers AS ledger
    WHERE ledger.tenant_id = NEW.tenant_id
)
AND NOT EXISTS (
    SELECT 1
    FROM knowledge_lookup_asset_tenant_ledgers AS ledger
    WHERE ledger.tenant_id = NEW.tenant_id
      AND ledger.published_version_count < 8192
      AND ledger.stored_content_bytes <= 2147483648 - NEW.canonical_bytes
)
BEGIN
    SELECT RAISE(ABORT, 'lookup asset version capacity exhausted');
END;
CREATE TRIGGER knowledge_lookup_asset_version_requires_tenant_ledger
BEFORE INSERT ON knowledge_lookup_asset_versions
WHEN NOT EXISTS (
    SELECT 1 FROM knowledge_lookup_asset_tenant_ledgers
    WHERE tenant_id = NEW.tenant_id
)
BEGIN
    SELECT RAISE(ABORT, 'lookup asset tenant ledger is missing');
END;
CREATE TRIGGER knowledge_lookup_asset_version_sequence_is_exact
BEFORE INSERT ON knowledge_lookup_asset_versions
WHEN NOT (
    (
        NEW.asset_version = 1
        AND EXISTS (
            SELECT 1 FROM knowledge_lookup_assets AS asset
            WHERE asset.tenant_id = NEW.tenant_id
              AND asset.lookup_asset_id = NEW.lookup_asset_id
              AND asset.current_version = 1
              AND asset.created_at_unix_micro = NEW.created_at_unix_micro
              AND asset.updated_at_unix_micro = NEW.created_at_unix_micro
        )
        AND NOT EXISTS (
            SELECT 1 FROM knowledge_lookup_asset_versions AS prior
            WHERE prior.tenant_id = NEW.tenant_id
              AND prior.lookup_asset_id = NEW.lookup_asset_id
        )
    )
    OR (
        NEW.asset_version > 1
        AND EXISTS (
            SELECT 1
            FROM knowledge_lookup_assets AS asset
            JOIN knowledge_lookup_asset_versions AS prior
              ON prior.tenant_id = asset.tenant_id
             AND prior.lookup_asset_id = asset.lookup_asset_id
             AND prior.asset_version = NEW.asset_version - 1
            WHERE asset.tenant_id = NEW.tenant_id
              AND asset.lookup_asset_id = NEW.lookup_asset_id
              AND asset.current_version = NEW.asset_version
              AND asset.updated_at_unix_micro = NEW.created_at_unix_micro
              AND prior.created_at_unix_micro <= NEW.created_at_unix_micro
        )
    )
)
BEGIN
    SELECT RAISE(ABORT, 'lookup asset version sequence is invalid');
END;
CREATE TRIGGER knowledge_lookup_asset_version_accounts_after_insert
AFTER INSERT ON knowledge_lookup_asset_versions
BEGIN
    UPDATE knowledge_lookup_asset_tenant_ledgers
    SET published_version_count = published_version_count + 1,
        stored_content_bytes = stored_content_bytes + NEW.canonical_bytes
    WHERE tenant_id = NEW.tenant_id;
END;
CREATE TRIGGER knowledge_lookup_asset_version_updates_are_forbidden
BEFORE UPDATE ON knowledge_lookup_asset_versions
BEGIN
    SELECT RAISE(ABORT, 'published lookup asset versions are immutable');
END;
CREATE TRIGGER knowledge_lookup_asset_version_deletes_are_forbidden
BEFORE DELETE ON knowledge_lookup_asset_versions
BEGIN
    SELECT RAISE(ABORT, 'published lookup asset versions cannot be deleted');
END;
CREATE TRIGGER knowledge_lookup_definition_versions_no_update
BEFORE UPDATE ON knowledge_lookup_definition_versions
BEGIN
    SELECT RAISE(ABORT, 'lookup definition versions are immutable');
END;
CREATE TRIGGER knowledge_lookup_definition_tenant_capacity_is_available
BEFORE INSERT ON knowledge_lookup_definitions
WHEN (
    SELECT count(*)
    FROM knowledge_lookup_definitions
    WHERE tenant_id = NEW.tenant_id
) >= 2048
BEGIN
    SELECT RAISE(ABORT, 'lookup definition tenant capacity is exhausted');
END;
CREATE TRIGGER knowledge_lookup_definition_version_capacity_is_available
BEFORE INSERT ON knowledge_lookup_definition_versions
WHEN (
    SELECT count(*)
    FROM knowledge_lookup_definition_versions
    WHERE tenant_id = NEW.tenant_id
) >= 8192
BEGIN
    SELECT RAISE(ABORT, 'lookup definition version capacity is exhausted');
END;
CREATE TRIGGER knowledge_lookup_definition_ordinary_version_capacity_is_available
BEFORE INSERT ON knowledge_lookup_definition_versions
WHEN NEW.mutation_kind IN ('CREATE', 'REPLACE', 'ENABLE')
 AND (
    SELECT count(*)
    FROM knowledge_lookup_definition_versions
    WHERE tenant_id = NEW.tenant_id
 ) >= 4096
BEGIN
    SELECT RAISE(ABORT, 'lookup definition ordinary version capacity is exhausted');
END;
CREATE TRIGGER knowledge_lookup_definition_initial_shape_is_exact
BEFORE INSERT ON knowledge_lookup_definitions
WHEN NEW.current_version != 1
  OR NEW.state != 'ACTIVE'
  OR NEW.updated_at_unix_micro != NEW.created_at_unix_micro
BEGIN
    SELECT RAISE(ABORT, 'lookup definition initial shape is invalid');
END;
CREATE TRIGGER knowledge_lookup_definition_transition_is_valid
BEFORE UPDATE ON knowledge_lookup_definitions
WHEN NOT (
    NEW.tenant_id = OLD.tenant_id
    AND NEW.lookup_id = OLD.lookup_id
    AND NEW.owner_id = OLD.owner_id
    AND NEW.created_at_unix_micro = OLD.created_at_unix_micro
    AND NEW.current_version = OLD.current_version + 1
    AND NEW.updated_at_unix_micro > OLD.updated_at_unix_micro
    AND (
        (
            OLD.state = 'ACTIVE'
            AND NEW.state = 'ACTIVE'
        )
        OR (
            OLD.state = 'DISABLED'
            AND NEW.state = 'DISABLED'
            AND NEW.disabled_at_unix_micro IS OLD.disabled_at_unix_micro
        )
        OR (
            NEW.app_id = OLD.app_id
            AND NEW.name = OLD.name
            AND NEW.sharing_scope = OLD.sharing_scope
            AND NEW.automatic = OLD.automatic
            AND (
                (
                    OLD.state = 'ACTIVE'
                    AND NEW.state = 'DISABLED'
                    AND NEW.disabled_at_unix_micro = NEW.updated_at_unix_micro
                    AND NEW.deleted_at_unix_micro IS NULL
                )
                OR (
                    OLD.state = 'DISABLED'
                    AND NEW.state = 'ACTIVE'
                    AND NEW.disabled_at_unix_micro IS NULL
                    AND NEW.deleted_at_unix_micro IS NULL
                )
                OR (
                    OLD.state = 'DISABLED'
                    AND NEW.state = 'DELETED'
                    AND NEW.disabled_at_unix_micro IS OLD.disabled_at_unix_micro
                    AND NEW.deleted_at_unix_micro = NEW.updated_at_unix_micro
                )
            )
        )
    )
)
BEGIN
    SELECT RAISE(ABORT, 'lookup definition registry transition is invalid');
END;
CREATE TRIGGER knowledge_lookup_definition_version_matches_current_registry
BEFORE INSERT ON knowledge_lookup_definition_versions
WHEN NOT (
    EXISTS (
        SELECT 1
        FROM knowledge_lookup_definitions
        WHERE tenant_id = NEW.tenant_id
          AND lookup_id = NEW.lookup_id
          AND current_version = NEW.definition_version
          AND state = NEW.state
          AND disabled_at_unix_micro IS NEW.disabled_at_unix_micro
          AND deleted_at_unix_micro IS NEW.deleted_at_unix_micro
          AND updated_at_unix_micro = NEW.created_at_unix_micro
    )
    AND (
        (
            NEW.definition_version = 1
            AND NEW.mutation_kind = 'CREATE'
        )
        OR (
            NEW.definition_version > 1
            AND EXISTS (
                SELECT 1
                FROM knowledge_lookup_definition_versions AS previous
                WHERE previous.tenant_id = NEW.tenant_id
                  AND previous.lookup_id = NEW.lookup_id
                  AND previous.definition_version = NEW.definition_version - 1
                  AND (
                    (
                        NEW.mutation_kind = 'REPLACE'
                        AND previous.state = NEW.state
                        AND previous.disabled_at_unix_micro IS NEW.disabled_at_unix_micro
                        AND previous.deleted_at_unix_micro IS NEW.deleted_at_unix_micro
                        AND (
                            NEW.lookup_asset_id != previous.lookup_asset_id
                            OR NEW.asset_version != previous.asset_version
                            OR NEW.asset_size_bytes != previous.asset_size_bytes
                            OR NEW.asset_content_sha256 != previous.asset_content_sha256
                            OR NEW.definition_proto != previous.definition_proto
                            OR NEW.columns_blob != previous.columns_blob
                        )
                    )
                    OR (
                        NEW.mutation_kind = 'ENABLE'
                        AND previous.state = 'DISABLED'
                        AND NEW.state = 'ACTIVE'
                        AND NEW.lookup_asset_id = previous.lookup_asset_id
                        AND NEW.asset_version = previous.asset_version
                        AND NEW.asset_size_bytes = previous.asset_size_bytes
                        AND NEW.asset_content_sha256 = previous.asset_content_sha256
                        AND NEW.definition_proto = previous.definition_proto
                        AND NEW.columns_blob = previous.columns_blob
                    )
                    OR (
                        NEW.mutation_kind = 'DISABLE'
                        AND previous.state = 'ACTIVE'
                        AND NEW.state = 'DISABLED'
                        AND NEW.lookup_asset_id = previous.lookup_asset_id
                        AND NEW.asset_version = previous.asset_version
                        AND NEW.asset_size_bytes = previous.asset_size_bytes
                        AND NEW.asset_content_sha256 = previous.asset_content_sha256
                        AND NEW.definition_proto = previous.definition_proto
                        AND NEW.columns_blob = previous.columns_blob
                    )
                    OR (
                        NEW.mutation_kind = 'DELETE'
                        AND previous.state = 'DISABLED'
                        AND NEW.state = 'DELETED'
                        AND NEW.disabled_at_unix_micro IS previous.disabled_at_unix_micro
                        AND NEW.lookup_asset_id = previous.lookup_asset_id
                        AND NEW.asset_version = previous.asset_version
                        AND NEW.asset_size_bytes = previous.asset_size_bytes
                        AND NEW.asset_content_sha256 = previous.asset_content_sha256
                        AND NEW.definition_proto = previous.definition_proto
                        AND NEW.columns_blob = previous.columns_blob
                    )
                  )
            )
        )
    )
)
BEGIN
    SELECT RAISE(ABORT, 'lookup definition version is not the current registry authority');
END;
CREATE TRIGGER knowledge_lookup_definition_versions_no_delete
BEFORE DELETE ON knowledge_lookup_definition_versions
BEGIN
    SELECT RAISE(ABORT, 'lookup definition versions are retained');
END;
CREATE TRIGGER knowledge_lookup_active_app_workspace_cannot_be_archived
BEFORE UPDATE OF state ON app_workspaces
WHEN NEW.state = 'archived'
 AND EXISTS (
    SELECT 1
    FROM knowledge_lookup_definitions
    WHERE tenant_id = OLD.tenant_id
      AND app_id = OLD.app_id
      AND state = 'ACTIVE'
)
BEGIN
    SELECT RAISE(ABORT, 'app workspace has active lookup definitions');
END;
CREATE TRIGGER knowledge_lookup_referenced_app_workspace_cannot_be_deleted
BEFORE DELETE ON app_workspaces
WHEN EXISTS (
    SELECT 1
    FROM knowledge_lookup_definitions
    WHERE tenant_id = OLD.tenant_id
      AND app_id = OLD.app_id
)
BEGIN
    SELECT RAISE(ABORT, 'app workspace is referenced by lookup definitions');
END;
CREATE TRIGGER knowledge_lookup_active_definition_requires_active_app_insert
BEFORE INSERT ON knowledge_lookup_definitions
WHEN NOT EXISTS (
    SELECT 1
    FROM app_workspaces
    WHERE tenant_id = NEW.tenant_id
      AND app_id = NEW.app_id
      AND state = 'active'
)
BEGIN
    SELECT RAISE(ABORT, 'lookup definition requires an active app workspace');
END;
CREATE TRIGGER knowledge_lookup_definition_update_requires_active_app
BEFORE UPDATE ON knowledge_lookup_definitions
WHEN NOT EXISTS (
    SELECT 1
    FROM app_workspaces
    WHERE tenant_id = NEW.tenant_id
      AND app_id = NEW.app_id
      AND state = 'active'
)
 AND NOT (
    OLD.state = 'DISABLED'
    AND NEW.state = 'DELETED'
    AND NEW.tenant_id = OLD.tenant_id
    AND NEW.lookup_id = OLD.lookup_id
    AND NEW.owner_id = OLD.owner_id
    AND NEW.app_id = OLD.app_id
    AND NEW.name = OLD.name
    AND NEW.sharing_scope = OLD.sharing_scope
    AND NEW.automatic = OLD.automatic
    AND NEW.created_at_unix_micro = OLD.created_at_unix_micro
 )
BEGIN
    SELECT RAISE(ABORT, 'lookup definition update requires an active app workspace');
END;
CREATE TRIGGER search_attempt_audit_event_knowledge_snapshot_is_complete
BEFORE INSERT ON search_attempt_audit_events
WHEN NOT (
    (
        NEW.knowledge_snapshot_sha256 IS NULL
        AND NEW.knowledge_snapshot_tenant_catalog_revision IS NULL
        AND NEW.knowledge_snapshot_tenant_catalog_state_token IS NULL
        AND NEW.knowledge_snapshot_object_count IS NULL
        AND NEW.knowledge_snapshot_lookup_asset_count IS NULL
    )
    OR (
        NEW.knowledge_snapshot_sha256 IS NOT NULL
        AND NEW.knowledge_snapshot_tenant_catalog_revision IS NOT NULL
        AND NEW.knowledge_snapshot_tenant_catalog_state_token IS NOT NULL
        AND NEW.knowledge_snapshot_object_count IS NOT NULL
        AND NEW.knowledge_snapshot_lookup_asset_count IS NOT NULL
    )
)
BEGIN
    SELECT RAISE(
        ABORT,
        'search-attempt audit knowledge snapshot must be absent or exact'
    );
END;
CREATE TRIGGER search_attempt_audit_event_update_is_forbidden
BEFORE UPDATE ON search_attempt_audit_events
BEGIN
    SELECT RAISE(ABORT, 'search-attempt audit events cannot be updated');
END;
CREATE INDEX indexes_state_name_idx ON indexes (state, name);
CREATE INDEX ingestion_token_indexes_index_idx
    ON ingestion_token_indexes (index_id, ingestion_token_id);
CREATE INDEX ingest_visibility_reservations_state_sequence_idx
    ON ingest_visibility_reservations (state, sequence);
CREATE INDEX ingest_visibility_reservations_batch_sequence_idx
    ON ingest_visibility_reservations (batch_key, sequence DESC);
CREATE UNIQUE INDEX ingest_visibility_reservations_active_batch_idx
    ON ingest_visibility_reservations (batch_key)
    WHERE state IN ('reserved', 'committed', 'rejected');
CREATE UNIQUE INDEX ingest_visibility_reservations_attempt_idx
    ON ingest_visibility_reservations (attempt_id)
    WHERE attempt_id <> '';
CREATE INDEX ingest_visibility_reservations_group_formation_idx
    ON ingest_visibility_reservations (state, phase, attempt_id, sequence);
CREATE INDEX ingest_write_groups_state_sequence_idx
    ON ingest_write_groups (state, first_sequence);
CREATE UNIQUE INDEX ingest_write_groups_attempt_idx
    ON ingest_write_groups (attempt_id)
    WHERE attempt_id <> '';
CREATE INDEX ingest_write_group_members_sequence_idx
    ON ingest_write_group_members (visibility_sequence);
CREATE INDEX saved_searches_app_name_id_idx
    ON saved_searches (app_id, name, saved_search_id);
CREATE INDEX saved_searches_updated_idx
    ON saved_searches (updated_at_unix_micro DESC, saved_search_id);
CREATE INDEX saved_searches_owner_name_id_idx
    ON saved_searches (owner_id, name, saved_search_id);
CREATE INDEX saved_searches_owner_created_id_idx
    ON saved_searches (owner_id, created_at_unix_micro, saved_search_id);
CREATE INDEX saved_searches_owner_updated_id_idx
    ON saved_searches (owner_id, updated_at_unix_micro, saved_search_id);
CREATE INDEX search_history_owner_created_idx
    ON search_history (tenant_id, owner_id, created_at_unix_micro DESC, search_job_id DESC);
CREATE INDEX search_history_owner_finished_idx
    ON search_history (tenant_id, owner_id, finished_at_unix_micro DESC, search_job_id DESC);
CREATE INDEX search_history_owner_duration_idx
    ON search_history (tenant_id, owner_id, duration_nanoseconds DESC, search_job_id DESC);
CREATE INDEX search_history_owner_matched_idx
    ON search_history (tenant_id, owner_id, matched_events DESC, search_job_id DESC);
CREATE INDEX search_history_owner_app_created_idx
    ON search_history (tenant_id, owner_id, app_id, created_at_unix_micro DESC, search_job_id DESC);
CREATE INDEX search_history_owner_saved_created_idx
    ON search_history (tenant_id, owner_id, saved_search_id, created_at_unix_micro DESC, search_job_id DESC);
CREATE INDEX search_history_pending_owner_created_idx
    ON search_history_pending (tenant_id, owner_id, created_at_unix_micro, search_job_id);
CREATE INDEX app_workspaces_tenant_display_id_idx
    ON app_workspaces (tenant_id, display_name, app_id);
CREATE INDEX app_workspaces_tenant_created_id_idx
    ON app_workspaces (tenant_id, created_at_unix_micro, app_id);
CREATE INDEX app_workspaces_tenant_updated_id_idx
    ON app_workspaces (tenant_id, updated_at_unix_micro, app_id);
CREATE INDEX app_default_indexes_index_app_idx
    ON app_default_indexes (index_id, tenant_id, app_id);
CREATE INDEX collector_fleet_tenant_state_id_idx
    ON collector_fleet (tenant_id, administrative_state, collector_id);
CREATE INDEX collector_fleet_tenant_display_id_idx
    ON collector_fleet (tenant_id, display_name_sort_key, collector_id);
CREATE INDEX collector_runtime_tenant_hostname_id_idx
    ON collector_runtime (tenant_id, hostname, collector_id);
CREATE INDEX collector_runtime_tenant_last_seen_id_idx
    ON collector_runtime (tenant_id, last_seen_at_unix_micro, collector_id);
CREATE INDEX collector_runtime_tenant_queued_bytes_id_idx
    ON collector_runtime (tenant_id, queued_bytes, collector_id);
CREATE INDEX collector_authorized_indexes_tenant_index_name_collector_idx
    ON collector_authorized_indexes (tenant_id, index_name, collector_id);
CREATE INDEX ingestion_tokens_revoked_retention_idx
    ON ingestion_tokens (
        revoked_at_unix_micro DESC,
        ingestion_token_id DESC
    )
    WHERE state = 'revoked';
CREATE INDEX search_history_created_idx
    ON search_history (created_at_unix_micro, search_job_id);
CREATE INDEX index_deletion_operations_created_id_idx
    ON index_deletion_operations (
        created_at_unix_micro,
        deletion_operation_id
    );
CREATE INDEX indexes_name_id_idx
    ON indexes (name, index_id);
CREATE INDEX indexes_created_id_idx
    ON indexes (created_at_unix_micro, index_id);
CREATE INDEX indexes_updated_id_idx
    ON indexes (updated_at_unix_micro, index_id);
CREATE INDEX ingest_quota_buckets_token_owner_idx
    ON ingest_quota_buckets (token_owner_id)
    WHERE token_owner_id IS NOT NULL;
CREATE INDEX search_attempt_audit_tenant_actor_sequence_idx
    ON search_attempt_audit_events (tenant_id, actor_id, sequence DESC);
CREATE INDEX search_attempt_audit_tenant_owner_sequence_idx
    ON search_attempt_audit_events (tenant_id, owner_id, sequence DESC);
CREATE INDEX knowledge_object_dependencies_target_idx
    ON knowledge_object_dependencies (
        tenant_id, target_kind, target_object_id, target_object_version,
        source_object_id, source_object_version
    );
CREATE INDEX knowledge_object_acl_role_object_idx
    ON knowledge_object_acl (tenant_id, role_id, knowledge_object_id);
CREATE INDEX knowledge_recovery_audit_occurred_idx
    ON knowledge_recovery_audit (tenant_id, occurred_at_unix_micro DESC, sequence DESC);
CREATE UNIQUE INDEX knowledge_objects_active_private_name_idx
    ON knowledge_objects (
        tenant_id, app_id, owner_id, object_type, name
    ) WHERE state = 'active' AND sharing_scope = 'private';
CREATE UNIQUE INDEX knowledge_objects_active_app_name_idx
    ON knowledge_objects (
        tenant_id, app_id, object_type, name
    ) WHERE state = 'active' AND sharing_scope = 'app';
CREATE UNIQUE INDEX knowledge_objects_active_global_name_idx
    ON knowledge_objects (
        tenant_id, object_type, name
    ) WHERE state = 'active' AND sharing_scope = 'global';
CREATE INDEX knowledge_objects_resolution_idx
    ON knowledge_objects (
        tenant_id, state, sharing_scope, app_id, owner_id,
        object_type, name, knowledge_object_id
    );
CREATE INDEX knowledge_objects_list_updated_idx
    ON knowledge_objects (
        tenant_id, updated_at_unix_micro DESC, knowledge_object_id
    );
CREATE UNIQUE INDEX knowledge_objects_current_projection_identity_idx
    ON knowledge_objects (
        tenant_id, knowledge_object_id, current_version,
        app_id, owner_id, object_type, name, sharing_scope, state
    );
CREATE INDEX knowledge_list_projection_filter_idx
    ON knowledge_object_list_projections (
        tenant_id, state, object_type, sharing_scope, app_id, owner_id,
        name COLLATE BINARY, knowledge_object_id, object_version
    );
CREATE INDEX knowledge_list_projection_name_idx
    ON knowledge_object_list_projections (
        tenant_id, name COLLATE BINARY, knowledge_object_id, object_version
    );
CREATE INDEX knowledge_list_projection_description_idx
    ON knowledge_object_list_projections (
        tenant_id, description COLLATE BINARY, knowledge_object_id, object_version
    ) WHERE description_present = 1;
CREATE INDEX knowledge_list_selector_value_idx
    ON knowledge_object_list_selector_patterns (
        tenant_id, dimension, match_kind, value COLLATE BINARY,
        knowledge_object_id, object_version, ordinal
    );
CREATE INDEX audit_events_tenant_action_sequence_idx
    ON audit_events (tenant_id, action, sequence DESC);
CREATE INDEX audit_events_tenant_actor_sequence_idx
    ON audit_events (tenant_id, actor_id, sequence DESC);
CREATE INDEX audit_events_tenant_target_sequence_idx
    ON audit_events (tenant_id, target_kind, sequence DESC);
CREATE INDEX knowledge_attempt_audit_tenant_actor_sequence_idx
    ON knowledge_attempt_audit_events (tenant_id, actor_id, sequence DESC);
CREATE INDEX knowledge_attempt_audit_tenant_reason_sequence_idx
    ON knowledge_attempt_audit_events (tenant_id, reason, sequence DESC);
CREATE INDEX knowledge_list_order_created_idx
    ON knowledge_object_list_order_keys (
        tenant_id, created_at_unix_micro,
        knowledge_object_id, object_version
    );
CREATE INDEX knowledge_list_order_updated_idx
    ON knowledge_object_list_order_keys (
        tenant_id, updated_at_unix_micro,
        knowledge_object_id, object_version
    );
CREATE INDEX knowledge_list_projection_object_type_order_idx
    ON knowledge_object_list_projections (
        tenant_id, object_type, name COLLATE BINARY,
        knowledge_object_id, object_version
    );
CREATE INDEX knowledge_objects_authorized_global_idx
    ON knowledge_objects (tenant_id, knowledge_object_id)
    WHERE sharing_scope = 'global';
CREATE INDEX knowledge_objects_authorized_app_idx
    ON knowledge_objects (tenant_id, app_id, knowledge_object_id)
    WHERE sharing_scope = 'app';
CREATE INDEX knowledge_objects_authorized_private_idx
    ON knowledge_objects (
        tenant_id, owner_id, app_id, knowledge_object_id
    ) WHERE sharing_scope = 'private';
CREATE INDEX knowledge_list_projection_authorized_global_idx
    ON knowledge_object_list_projections (
        tenant_id, knowledge_object_id, object_version
    ) WHERE sharing_scope = 'global';
CREATE INDEX knowledge_list_projection_authorized_app_idx
    ON knowledge_object_list_projections (
        tenant_id, app_id, knowledge_object_id, object_version
    ) WHERE sharing_scope = 'app';
CREATE INDEX knowledge_list_projection_authorized_private_idx
    ON knowledge_object_list_projections (
        tenant_id, owner_id, app_id, knowledge_object_id, object_version
    ) WHERE sharing_scope = 'private';
CREATE INDEX knowledge_mutation_idempotency_retention_idx
    ON knowledge_mutation_idempotency (
        tenant_id, retain_until_unix_micro, created_at_unix_micro,
        actor_kind, actor_id, route, client_request_id,
        retention_anchor_unix_micro
    );
CREATE INDEX knowledge_object_dependencies_source_target_idx
    ON knowledge_object_dependencies (
        tenant_id, source_object_id, source_object_version,
        target_kind, target_object_id, target_object_version
    );
CREATE INDEX knowledge_catalog_tenants_nonempty_active_idx
    ON knowledge_catalog_tenants (
        tenant_id, catalog_revision, active_object_count
    ) WHERE active_object_count > 0;
CREATE INDEX knowledge_objects_active_tenant_idx
    ON knowledge_objects (
        tenant_id, knowledge_object_id, current_version
    ) WHERE state = 'active';
CREATE INDEX hec_requests_terminal_retention_idx
    ON hec_requests (
        state,
        terminal_at_unix_micro,
        tenant_id,
        ingestion_token_id,
        request_sequence
    )
    WHERE state IN ('indexed', 'terminal_failure');
CREATE INDEX hec_channels_token_activity_idx
    ON hec_channels (
        tenant_id,
        ingestion_token_id,
        last_used_at_unix_micro,
        channel_id
    );
CREATE INDEX hec_acknowledgments_bounded_lookup_idx
    ON hec_acknowledgments (
        tenant_id,
        ingestion_token_id,
        channel_id,
        acknowledgment_id,
        request_sequence
    );
CREATE INDEX knowledge_lookup_asset_stage_expiry_idx
    ON knowledge_lookup_asset_stages (
        expires_at_unix_micro, tenant_id, stage_id
    );
CREATE INDEX knowledge_lookup_asset_version_digest_idx
    ON knowledge_lookup_asset_versions (
        tenant_id, content_sha256, lookup_asset_id, asset_version
    );
CREATE INDEX knowledge_lookup_definitions_resolution
    ON knowledge_lookup_definitions (
        tenant_id, state, name, sharing_scope, app_id, owner_id, automatic,
        lookup_id
    );
CREATE UNIQUE INDEX knowledge_lookup_definitions_private_name
    ON knowledge_lookup_definitions (tenant_id, owner_id, app_id, name)
    WHERE sharing_scope = 1 AND state != 'DELETED';
CREATE UNIQUE INDEX knowledge_lookup_definitions_app_name
    ON knowledge_lookup_definitions (tenant_id, app_id, name)
    WHERE sharing_scope = 2 AND state != 'DELETED';
CREATE UNIQUE INDEX knowledge_lookup_definitions_global_name
    ON knowledge_lookup_definitions (tenant_id, name)
    WHERE sharing_scope = 3 AND state != 'DELETED';
CREATE INDEX knowledge_lookup_definition_versions_asset
    ON knowledge_lookup_definition_versions (
        tenant_id, lookup_asset_id, asset_version, lookup_id, definition_version
    );

CREATE TABLE dashboards (
    dashboard_id TEXT PRIMARY KEY NOT NULL COLLATE BINARY,
    version INTEGER NOT NULL CHECK (version >= 1),
    name TEXT NOT NULL COLLATE BINARY,
    app_id TEXT NOT NULL COLLATE BINARY,
    tenant_id TEXT NOT NULL COLLATE BINARY,
    owner_id TEXT NOT NULL COLLATE BINARY,
    sharing_scope INTEGER NOT NULL CHECK (sharing_scope BETWEEN 1 AND 3),
    definition_proto BLOB NOT NULL CHECK (length(definition_proto) BETWEEN 1 AND 98304),
    created_at_unix_micro INTEGER NOT NULL,
    updated_at_unix_micro INTEGER NOT NULL,
    CHECK (length(dashboard_id) BETWEEN 1 AND 128),
    CHECK (length(name) BETWEEN 1 AND 255),
    CHECK (length(app_id) BETWEEN 1 AND 255),
    CHECK (length(tenant_id) BETWEEN 1 AND 255),
    CHECK (length(owner_id) BETWEEN 1 AND 255),
    CHECK (updated_at_unix_micro >= created_at_unix_micro),
    UNIQUE (tenant_id, owner_id, app_id, name)
) STRICT;

CREATE INDEX dashboards_owner_updated
    ON dashboards (tenant_id, owner_id, updated_at_unix_micro DESC, dashboard_id DESC);

CREATE TRIGGER dashboards_owner_capacity_insert
BEFORE INSERT ON dashboards
FOR EACH ROW
WHEN (
    SELECT count(*)
    FROM dashboards
    WHERE tenant_id = NEW.tenant_id AND owner_id = NEW.owner_id
) >= 64
BEGIN
    SELECT RAISE(ABORT, 'dashboard owner capacity exhausted');
END;

CREATE TRIGGER app_workspace_delete_restrict_dashboards
BEFORE DELETE ON app_workspaces
FOR EACH ROW
WHEN EXISTS (
    SELECT 1
    FROM dashboards
    WHERE tenant_id = OLD.tenant_id AND app_id = OLD.app_id
)
BEGIN
    SELECT RAISE(ABORT, 'app is referenced by a dashboard');
END;

CREATE TRIGGER canonical_dashboard_app_exists_insert
BEFORE INSERT ON dashboards
WHEN
    length(NEW.app_id) = 26
    AND substr(NEW.app_id, 1, 4) = 'app_'
    AND substr(NEW.app_id, 5) NOT GLOB '*[^A-Za-z0-9_-]*'
    AND substr(NEW.app_id, 26, 1) GLOB '[AQgw]'
    AND NOT EXISTS (
        SELECT 1
        FROM app_workspaces
        WHERE tenant_id = NEW.tenant_id AND app_id = NEW.app_id
    )
BEGIN
    SELECT RAISE(ABORT, 'canonical dashboard app does not exist');
END;

CREATE TRIGGER canonical_dashboard_app_exists_update
BEFORE UPDATE OF tenant_id, app_id ON dashboards
WHEN
    length(NEW.app_id) = 26
    AND substr(NEW.app_id, 1, 4) = 'app_'
    AND substr(NEW.app_id, 5) NOT GLOB '*[^A-Za-z0-9_-]*'
    AND substr(NEW.app_id, 26, 1) GLOB '[AQgw]'
    AND NOT EXISTS (
        SELECT 1
        FROM app_workspaces
        WHERE tenant_id = NEW.tenant_id AND app_id = NEW.app_id
    )
BEGIN
    SELECT RAISE(ABORT, 'canonical dashboard app does not exist');
END;
