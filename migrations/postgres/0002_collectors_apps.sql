-- open-splunk PostgreSQL control plane, part 2: Splunk-style app workspaces
-- and the native collector fleet. See migrations/postgres/README.md.

CREATE TABLE app_catalog_revisions (
    tenant_id text COLLATE "C" PRIMARY KEY NOT NULL,
    revision bigint NOT NULL CHECK (revision >= 1),
    CHECK (char_length(tenant_id) BETWEEN 1 AND 255),
    CONSTRAINT app_catalog_revisions_tenant_fk FOREIGN KEY (tenant_id)
        REFERENCES tenants (tenant_id) ON UPDATE RESTRICT ON DELETE RESTRICT
);

CREATE TABLE app_workspaces (
    app_id text COLLATE "C" PRIMARY KEY NOT NULL,
    tenant_id text COLLATE "C" NOT NULL,
    version bigint NOT NULL CHECK (version >= 1),
    slug text COLLATE "C" NOT NULL,
    display_name text COLLATE "C" NOT NULL,
    description text COLLATE "C" NOT NULL DEFAULT '',
    default_time_range_present bigint NOT NULL
        CHECK (default_time_range_present IN (0, 1)),
    default_earliest text COLLATE "C",
    default_latest text COLLATE "C",
    default_timezone text COLLATE "C",
    state text COLLATE "C" NOT NULL CHECK (state IN ('active', 'archived')),
    created_at_unix_micro bigint NOT NULL CHECK (created_at_unix_micro > 0),
    updated_at_unix_micro bigint NOT NULL CHECK (updated_at_unix_micro > 0),
    archived_at_unix_micro bigint,
    CHECK (char_length(app_id) = 26),
    CHECK (substr(app_id, 1, 4) = 'app_'),
    CHECK (substr(app_id, 5) !~ '[^A-Za-z0-9_-]'),
    CHECK (substr(app_id, 26, 1) ~ '^[AQgw]$'),
    CHECK (char_length(tenant_id) BETWEEN 1 AND 255),
    CHECK (char_length(slug) BETWEEN 1 AND 128),
    CHECK (slug = lower(slug)),
    CHECK (slug !~ '[^a-z0-9_-]'),
    CHECK (substr(slug, 1, 1) ~ '^[a-z0-9]$'),
    CHECK (char_length(display_name) BETWEEN 1 AND 255),
    CHECK (char_length(description) <= 16384),
    CHECK (default_earliest IS NULL OR char_length(default_earliest) BETWEEN 1 AND 1024),
    CHECK (default_latest IS NULL OR char_length(default_latest) BETWEEN 1 AND 1024),
    CHECK (default_timezone IS NULL OR char_length(default_timezone) BETWEEN 1 AND 255),
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
    UNIQUE (tenant_id, app_id),
    CONSTRAINT app_workspaces_tenant_fk FOREIGN KEY (tenant_id)
        REFERENCES tenants (tenant_id) ON UPDATE RESTRICT ON DELETE RESTRICT
);

CREATE TABLE app_default_indexes (
    tenant_id text COLLATE "C" NOT NULL,
    app_id text COLLATE "C" NOT NULL,
    index_id text COLLATE "C" NOT NULL,
    PRIMARY KEY (tenant_id, app_id, index_id),
    FOREIGN KEY (tenant_id, app_id)
        REFERENCES app_workspaces (tenant_id, app_id) ON DELETE CASCADE,
    FOREIGN KEY (index_id)
        REFERENCES indexes (index_id) ON DELETE RESTRICT,
    CONSTRAINT app_default_indexes_tenant_fk FOREIGN KEY (tenant_id)
        REFERENCES tenants (tenant_id) ON UPDATE RESTRICT ON DELETE RESTRICT
);

CREATE TABLE collector_catalog_revisions (
    tenant_id text COLLATE "C" PRIMARY KEY NOT NULL,
    revision bigint NOT NULL
        CONSTRAINT collector_catalog_revisions_revision_positive
        CHECK (revision >= 1),
    fleet_count bigint NOT NULL
        CONSTRAINT collector_catalog_revisions_fleet_count_bounded
        CHECK (fleet_count BETWEEN 0 AND 256),
    runtime_count bigint NOT NULL
        CONSTRAINT collector_catalog_revisions_runtime_count_bounded
        CHECK (runtime_count BETWEEN 0 AND 256),
    CONSTRAINT collector_catalog_revisions_tenant_id_bounded
        CHECK (
            octet_length(tenant_id) BETWEEN 1 AND 255
        ),
    CONSTRAINT collector_catalog_revisions_tenant_fk FOREIGN KEY (tenant_id)
        REFERENCES tenants (tenant_id) ON UPDATE RESTRICT ON DELETE RESTRICT
);

CREATE TABLE collector_fleet (
    tenant_id text COLLATE "C" NOT NULL,
    collector_id text COLLATE "C" NOT NULL,
    admin_version bigint NOT NULL
        CONSTRAINT collector_fleet_admin_version_positive
        CHECK (admin_version >= 1),
    display_name text COLLATE "C",
    -- Persisted display names are non-empty, so the empty string is an
    -- unambiguous, non-null key that preserves SQLite's NULL-first ascending
    -- and NULL-last descending ordering while keeping keyset seeks indexable.
    display_name_sort_key text COLLATE "C" NOT NULL
        GENERATED ALWAYS AS (coalesce(display_name, '')) STORED,
    administrative_state text COLLATE "C" NOT NULL
        CONSTRAINT collector_fleet_administrative_state_valid
        CHECK (administrative_state IN ('enabled', 'disabled')),
    first_seen_at_unix_micro bigint NOT NULL
        CONSTRAINT collector_fleet_first_seen_positive
        CHECK (first_seen_at_unix_micro BETWEEN 1 AND 253402300799999999),
    updated_at_unix_micro bigint NOT NULL
        CONSTRAINT collector_fleet_updated_at_valid
        CHECK (
            updated_at_unix_micro BETWEEN
                first_seen_at_unix_micro AND 253402300799999999
        ),
    CONSTRAINT collector_fleet_tenant_id_bounded
        CHECK (
            octet_length(tenant_id) BETWEEN 1 AND 255
        ),
    CONSTRAINT collector_fleet_collector_id_canonical
        CHECK (
            char_length(collector_id) BETWEEN 1 AND 128
            AND substr(collector_id, 1, 1) ~ '^[A-Za-z0-9]$'
            AND collector_id !~ '[^A-Za-z0-9._:-]'
        ),
    CONSTRAINT collector_fleet_display_name_bounded
        CHECK (
            display_name IS NULL
            OR (
                octet_length(display_name) BETWEEN 1 AND 255
            )
        ),
    PRIMARY KEY (tenant_id, collector_id),
    CONSTRAINT collector_fleet_tenant_fk FOREIGN KEY (tenant_id)
        REFERENCES tenants (tenant_id) ON UPDATE RESTRICT ON DELETE RESTRICT
);

CREATE TABLE collector_authorized_indexes (
    tenant_id text COLLATE "C" NOT NULL,
    collector_id text COLLATE "C" NOT NULL,
    index_name text COLLATE "C" NOT NULL,
    CONSTRAINT collector_authorized_indexes_name_canonical
        CHECK (
            char_length(index_name) BETWEEN 1 AND 255
            AND index_name = lower(index_name)
            AND index_name !~ '[^a-z0-9_-]'
            AND substr(index_name, 1, 1) ~ '^[a-z0-9]$'
            AND strpos(index_name, 'kvstore') = 0
        ),
    PRIMARY KEY (tenant_id, collector_id, index_name),
    FOREIGN KEY (tenant_id, collector_id)
        REFERENCES collector_fleet (tenant_id, collector_id) ON DELETE CASCADE,
    CONSTRAINT collector_authorized_indexes_tenant_fk FOREIGN KEY (tenant_id)
        REFERENCES tenants (tenant_id) ON UPDATE RESTRICT ON DELETE RESTRICT
);

CREATE TABLE collector_capabilities (
    tenant_id text COLLATE "C" NOT NULL,
    collector_id text COLLATE "C" NOT NULL,
    capability bigint NOT NULL
        CONSTRAINT collector_capabilities_value_valid
        CHECK (capability BETWEEN 1 AND 2147483647),
    PRIMARY KEY (tenant_id, collector_id, capability),
    FOREIGN KEY (tenant_id, collector_id)
        REFERENCES collector_fleet (tenant_id, collector_id) ON DELETE CASCADE,
    CONSTRAINT collector_capabilities_tenant_fk FOREIGN KEY (tenant_id)
        REFERENCES tenants (tenant_id) ON UPDATE RESTRICT ON DELETE RESTRICT
);

CREATE TABLE collector_inputs (
    tenant_id text COLLATE "C" NOT NULL,
    collector_id text COLLATE "C" NOT NULL,
    input_id text COLLATE "C" NOT NULL,
    input_type bigint NOT NULL
        CONSTRAINT collector_inputs_type_valid
        CHECK (input_type BETWEEN 1 AND 2147483647),
    index_name text COLLATE "C" NOT NULL,
    source text COLLATE "C",
    sourcetype text COLLATE "C",
    CONSTRAINT collector_inputs_id_canonical
        CHECK (
            char_length(input_id) BETWEEN 1 AND 128
            AND substr(input_id, 1, 1) ~ '^[A-Za-z0-9]$'
            AND input_id !~ '[^A-Za-z0-9._:-]'
        ),
    CONSTRAINT collector_inputs_index_name_canonical
        CHECK (
            char_length(index_name) BETWEEN 1 AND 255
            AND index_name = lower(index_name)
            AND index_name !~ '[^a-z0-9_-]'
            AND substr(index_name, 1, 1) ~ '^[a-z0-9]$'
            AND strpos(index_name, 'kvstore') = 0
        ),
    CONSTRAINT collector_inputs_source_bounded
        CHECK (
            source IS NULL
            OR (
                octet_length(source) BETWEEN 1 AND 4096
            )
        ),
    CONSTRAINT collector_inputs_sourcetype_bounded
        CHECK (
            sourcetype IS NULL
            OR (
                octet_length(sourcetype) BETWEEN 1 AND 255
            )
        ),
    PRIMARY KEY (tenant_id, collector_id, input_id),
    FOREIGN KEY (tenant_id, collector_id)
        REFERENCES collector_fleet (tenant_id, collector_id) ON DELETE CASCADE,
    FOREIGN KEY (tenant_id, collector_id, index_name)
        REFERENCES collector_authorized_indexes (tenant_id, collector_id, index_name)
        ON DELETE CASCADE,
    CONSTRAINT collector_inputs_tenant_fk FOREIGN KEY (tenant_id)
        REFERENCES tenants (tenant_id) ON UPDATE RESTRICT ON DELETE RESTRICT
);

CREATE TABLE collector_input_health (
    tenant_id text COLLATE "C" NOT NULL,
    collector_id text COLLATE "C" NOT NULL,
    input_id text COLLATE "C" NOT NULL,
    state bigint NOT NULL
        CONSTRAINT collector_input_health_state_valid
        CHECK (state BETWEEN 1 AND 2147483647),
    status_message text COLLATE "C" NOT NULL,
    discovered_sources bigint NOT NULL
        CONSTRAINT collector_input_health_discovered_nonnegative
        CHECK (discovered_sources >= 0),
    active_sources bigint NOT NULL
        CONSTRAINT collector_input_health_active_valid
        CHECK (active_sources BETWEEN 0 AND discovered_sources),
    events_read_total bigint NOT NULL
        CONSTRAINT collector_input_health_events_nonnegative
        CHECK (events_read_total >= 0),
    bytes_read_total bigint NOT NULL
        CONSTRAINT collector_input_health_bytes_nonnegative
        CHECK (bytes_read_total >= 0),
    last_event_at_unix_micro bigint,
    last_error_at_unix_micro bigint,
    CONSTRAINT collector_input_health_message_bounded
        CHECK (
            octet_length(status_message) <= 8192
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
        ON DELETE CASCADE,
    CONSTRAINT collector_input_health_tenant_fk FOREIGN KEY (tenant_id)
        REFERENCES tenants (tenant_id) ON UPDATE RESTRICT ON DELETE RESTRICT
);

CREATE TABLE collector_runtime (
    tenant_id text COLLATE "C" NOT NULL,
    collector_id text COLLATE "C" NOT NULL,
    telemetry_revision bigint NOT NULL
        CONSTRAINT collector_runtime_telemetry_revision_positive
        CHECK (telemetry_revision >= 1),
    lease_generation bigint NOT NULL
        CONSTRAINT collector_runtime_lease_generation_positive
        CHECK (lease_generation >= 1),
    boot_epoch text COLLATE "C",
    stream_id text COLLATE "C",
    active_instance_id text COLLATE "C",
    source_revision text COLLATE "C" NOT NULL,
    hostname text COLLATE "C" NOT NULL,
    operating_system text COLLATE "C" NOT NULL,
    architecture text COLLATE "C" NOT NULL,
    started_at_unix_micro bigint NOT NULL
        CONSTRAINT collector_runtime_started_at_positive
        CHECK (started_at_unix_micro BETWEEN 1 AND 253402300799999999),
    connected_at_unix_micro bigint NOT NULL
        CONSTRAINT collector_runtime_connected_at_positive
        CHECK (connected_at_unix_micro BETWEEN 1 AND 253402300799999999),
    last_seen_at_unix_micro bigint NOT NULL
        CONSTRAINT collector_runtime_last_seen_valid
        CHECK (
            last_seen_at_unix_micro BETWEEN
                connected_at_unix_micro AND 253402300799999999
        ),
    disconnected_at_unix_micro bigint,
    observation_sequence bigint NOT NULL DEFAULT 0
        CONSTRAINT collector_runtime_observation_sequence_nonnegative
        CHECK (observation_sequence >= 0),
    observed_at_unix_micro bigint,
    last_acked_at_hello_sequence bigint,
    queued_events bigint NOT NULL DEFAULT 0
        CONSTRAINT collector_runtime_queued_events_nonnegative
        CHECK (queued_events >= 0),
    queued_bytes bigint NOT NULL DEFAULT 0
        CONSTRAINT collector_runtime_queued_bytes_nonnegative
        CHECK (queued_bytes >= 0),
    oldest_event_age_nanoseconds bigint,
    sent_events_total bigint NOT NULL DEFAULT 0
        CONSTRAINT collector_runtime_sent_events_nonnegative
        CHECK (sent_events_total >= 0),
    acknowledged_events_total bigint NOT NULL DEFAULT 0
        CONSTRAINT collector_runtime_acknowledged_events_nonnegative
        CHECK (acknowledged_events_total >= 0),
    retried_batches_total bigint NOT NULL DEFAULT 0
        CONSTRAINT collector_runtime_retried_batches_nonnegative
        CHECK (retried_batches_total >= 0),
    rejected_events_total bigint NOT NULL DEFAULT 0
        CONSTRAINT collector_runtime_rejected_events_nonnegative
        CHECK (rejected_events_total >= 0),
    dropped_events_total bigint NOT NULL DEFAULT 0
        CONSTRAINT collector_runtime_dropped_events_nonnegative
        CHECK (dropped_events_total >= 0),
    last_sent_batch_sequence bigint,
    last_acknowledged_batch_sequence bigint,
    process_resident_memory_bytes bigint NOT NULL DEFAULT 0
        CONSTRAINT collector_runtime_memory_nonnegative
        CHECK (process_resident_memory_bytes >= 0),
    process_cpu_percent double precision NOT NULL DEFAULT 0
        CONSTRAINT collector_runtime_cpu_valid
        CHECK (
            process_cpu_percent <> 'NaN'::double precision
            AND process_cpu_percent BETWEEN 0 AND 1000000
        ),
    CONSTRAINT collector_runtime_boot_epoch_canonical
        CHECK (
            boot_epoch IS NULL
            OR (
                char_length(boot_epoch) BETWEEN 1 AND 128
                AND substr(boot_epoch, 1, 1) ~ '^[A-Za-z0-9]$'
                AND boot_epoch !~ '[^A-Za-z0-9._:-]'
            )
        ),
    CONSTRAINT collector_runtime_stream_id_canonical
        CHECK (
            stream_id IS NULL
            OR (
                char_length(stream_id) BETWEEN 1 AND 128
                AND substr(stream_id, 1, 1) ~ '^[A-Za-z0-9]$'
                AND stream_id !~ '[^A-Za-z0-9._:-]'
            )
        ),
    CONSTRAINT collector_runtime_instance_id_canonical
        CHECK (
            active_instance_id IS NULL
            OR (
                char_length(active_instance_id) BETWEEN 1 AND 128
                AND substr(active_instance_id, 1, 1) ~ '^[A-Za-z0-9]$'
                AND active_instance_id !~ '[^A-Za-z0-9._:-]'
            )
        ),
    CONSTRAINT collector_runtime_source_revision_valid
        CHECK (
            source_revision = 'development'
            OR (
                octet_length(source_revision) IN (40, 64)
                AND source_revision !~ '[^0-9a-f]'
            )
        ),
    CONSTRAINT collector_runtime_metadata_bounded
        CHECK (
            octet_length(hostname) <= 255
            AND octet_length(operating_system) <= 128
            AND octet_length(architecture) <= 128
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
        REFERENCES collector_fleet (tenant_id, collector_id) ON DELETE CASCADE,
    CONSTRAINT collector_runtime_tenant_fk FOREIGN KEY (tenant_id)
        REFERENCES tenants (tenant_id) ON UPDATE RESTRICT ON DELETE RESTRICT
);

CREATE INDEX app_default_indexes_index_app_idx
    ON app_default_indexes (index_id, tenant_id, app_id);

CREATE INDEX app_workspaces_tenant_created_id_idx
    ON app_workspaces (tenant_id, created_at_unix_micro, app_id);

CREATE INDEX app_workspaces_tenant_display_id_idx
    ON app_workspaces (tenant_id, display_name, app_id);

CREATE INDEX app_workspaces_tenant_updated_id_idx
    ON app_workspaces (tenant_id, updated_at_unix_micro, app_id);

CREATE INDEX collector_authorized_indexes_tenant_index_name_collector_idx
    ON collector_authorized_indexes (tenant_id, index_name, collector_id);

CREATE INDEX collector_fleet_tenant_display_id_idx
    ON collector_fleet (tenant_id, display_name_sort_key, collector_id);

CREATE INDEX collector_fleet_tenant_state_id_idx
    ON collector_fleet (tenant_id, administrative_state, collector_id);

CREATE INDEX collector_runtime_tenant_hostname_id_idx
    ON collector_runtime (tenant_id, hostname, collector_id);

CREATE INDEX collector_runtime_tenant_last_seen_id_idx
    ON collector_runtime (tenant_id, last_seen_at_unix_micro, collector_id);

CREATE INDEX collector_runtime_tenant_queued_bytes_id_idx
    ON collector_runtime (tenant_id, queued_bytes, collector_id);
