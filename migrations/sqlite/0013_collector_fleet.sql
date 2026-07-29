-- Persisted collector fleet state is normalized so administrator mutations,
-- lease fencing, and high-frequency telemetry have independent revisions.

CREATE TABLE collector_fleet (
    tenant_id TEXT NOT NULL COLLATE BINARY,
    collector_id TEXT NOT NULL COLLATE BINARY,
    admin_version INTEGER NOT NULL
        CONSTRAINT collector_fleet_admin_version_positive
        CHECK (admin_version >= 1),
    display_name TEXT COLLATE BINARY,
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

CREATE INDEX collector_fleet_tenant_state_id_idx
    ON collector_fleet (tenant_id, administrative_state, collector_id);

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
    protocol_major INTEGER NOT NULL
        CONSTRAINT collector_runtime_protocol_major_valid
        CHECK (protocol_major BETWEEN 0 AND 4294967295),
    protocol_minor INTEGER NOT NULL
        CONSTRAINT collector_runtime_protocol_minor_valid
        CHECK (protocol_minor BETWEEN 0 AND 4294967295),
    collector_version TEXT NOT NULL COLLATE BINARY,
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
    CONSTRAINT collector_runtime_metadata_bounded
        CHECK (
            length(CAST(collector_version AS BLOB)) <= 128
            AND instr(collector_version, char(0)) = 0
            AND length(CAST(hostname AS BLOB)) <= 255
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
