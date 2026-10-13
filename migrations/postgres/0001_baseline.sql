-- Native PostgreSQL control-plane schema. PostgreSQL types, constraints and
-- statement locks preserve the control contracts; ClickHouse event data is separate.


CREATE FUNCTION os_control_write_lock() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN PERFORM pg_advisory_xact_lock(hashtextextended(current_database() || ':' || current_schema(),5716000640707417153)); RETURN NULL; END $$;

CREATE FUNCTION os_constraint_failure(message TEXT) RETURNS BIGINT LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = message; END $$;

CREATE TABLE indexes (
    index_id TEXT PRIMARY KEY NOT NULL COLLATE "C",
    version BIGINT NOT NULL CHECK ( version >= 1 ),
    name TEXT NOT NULL UNIQUE COLLATE "C",
    display_name TEXT NOT NULL,
    description TEXT NOT NULL DEFAULT '',
    retention_nanoseconds BIGINT NOT NULL DEFAULT 0 CHECK ( retention_nanoseconds >= 0 ),
    ingestion_enabled BIGINT NOT NULL CHECK ( ingestion_enabled IN (0,1) ),
    search_enabled BIGINT NOT NULL CHECK ( search_enabled IN (0,1) ),
    default_sourcetype TEXT NOT NULL DEFAULT '',
    max_event_bytes BIGINT NOT NULL DEFAULT 0 CHECK ( max_event_bytes >= 0 ),
    max_field_count BIGINT NOT NULL DEFAULT 0 CHECK ( max_field_count >= 0 ),
    max_nesting_depth BIGINT NOT NULL DEFAULT 0 CHECK ( max_nesting_depth >= 0 ),
    maximum_future_skew_nanoseconds BIGINT NOT NULL DEFAULT 0 CHECK ( maximum_future_skew_nanoseconds >= 0 ),
    maximum_event_age_nanoseconds BIGINT NOT NULL DEFAULT 0 CHECK ( maximum_event_age_nanoseconds >= 0 ),
    state TEXT NOT NULL CHECK ( state IN ( 'active' , 'archived' , 'deleting' ) ),
    created_at_unix_micro BIGINT NOT NULL,
    updated_at_unix_micro BIGINT NOT NULL,
    max_ingest_events_per_second BIGINT NOT NULL DEFAULT 0 CONSTRAINT indexes_max_ingest_events_per_second_bounded CHECK ( max_ingest_events_per_second BETWEEN 0 AND 1000000 ),
    max_ingest_uncompressed_bytes_per_second BIGINT NOT NULL DEFAULT 0 CONSTRAINT indexes_max_ingest_uncompressed_bytes_per_second_bounded CHECK ( max_ingest_uncompressed_bytes_per_second BETWEEN 0 AND 1099511627776 ),
    CHECK ( length ( name ) BETWEEN 1 AND 255 ),
    CHECK ( name = lower ( name ) ),
    CHECK ( name !~ '^.*[^a-z0-9_-].*$' ),
    CHECK ( substr ( name , 1 , 1 ) ~ '^[a-z0-9]$' ),
    CHECK ( position ( 'kvstore' in name ) = 0 ),
    CHECK ( updated_at_unix_micro >= created_at_unix_micro )
);

CREATE TABLE ingestion_tokens (
    ingestion_token_id TEXT PRIMARY KEY NOT NULL COLLATE "C",
    version BIGINT NOT NULL CHECK ( version >= 1 ),
    name TEXT NOT NULL,
    description TEXT NOT NULL DEFAULT '',
    token_prefix TEXT NOT NULL,
    token_digest BYTEA NOT NULL UNIQUE CHECK ( length ( token_digest ) = 32 ),
    state TEXT NOT NULL CHECK ( state IN ( 'active' , 'disabled' , 'revoked' ) ),
    created_at_unix_micro BIGINT NOT NULL,
    updated_at_unix_micro BIGINT NOT NULL,
    expires_at_unix_micro BIGINT,
    revoked_at_unix_micro BIGINT,
    last_used_at_unix_micro BIGINT CONSTRAINT ingestion_tokens_last_use_not_before_create CHECK ( last_used_at_unix_micro IS NULL OR last_used_at_unix_micro >= created_at_unix_micro ),
    bound_collector_id TEXT CONSTRAINT ingestion_tokens_bound_collector_id_canonical CHECK ( bound_collector_id IS NULL OR ( length ( bound_collector_id ) BETWEEN 1 AND 128 AND TRUE AND substr ( bound_collector_id , 1 , 1 ) ~ '^[A-Za-z0-9]$' AND bound_collector_id !~ '^.*[^A-Za-z0-9._:-].*$' ) ),
    max_ingest_events_per_second BIGINT NOT NULL DEFAULT 0 CONSTRAINT ingestion_tokens_max_ingest_events_per_second_bounded CHECK ( max_ingest_events_per_second BETWEEN 0 AND 1000000 ),
    max_ingest_uncompressed_bytes_per_second BIGINT NOT NULL DEFAULT 0 CONSTRAINT ingestion_tokens_max_ingest_uncompressed_bytes_per_second_bounded CHECK ( max_ingest_uncompressed_bytes_per_second BETWEEN 0 AND 1099511627776 ),
    purpose TEXT NOT NULL DEFAULT 'native_collector' COLLATE "C" CONSTRAINT ingestion_tokens_purpose_supported CHECK ( purpose IN ( 'native_collector' , 'hec' ) ),
    CHECK ( length ( name ) BETWEEN 1 AND 255 ),
    CHECK ( length ( token_prefix ) BETWEEN 8 AND 32 ),
    CHECK ( updated_at_unix_micro >= created_at_unix_micro ),
    CHECK ( expires_at_unix_micro IS NULL OR expires_at_unix_micro > created_at_unix_micro ),
    CHECK ( ( state = 'revoked' AND revoked_at_unix_micro IS NOT NULL ) OR ( state IN ( 'active' , 'disabled' ) AND revoked_at_unix_micro IS NULL ) )
);

CREATE TABLE ingestion_token_indexes (
    ingestion_token_id TEXT NOT NULL REFERENCES ingestion_tokens ( ingestion_token_id ) ON DELETE CASCADE,
    index_id TEXT NOT NULL REFERENCES indexes ( index_id ) ON DELETE RESTRICT,
    PRIMARY KEY ( ingestion_token_id , index_id )
);

CREATE TABLE ingestion_token_constraints (
    ingestion_token_id TEXT NOT NULL REFERENCES ingestion_tokens ( ingestion_token_id ) ON UPDATE RESTRICT ON DELETE CASCADE,
    constraint_kind TEXT NOT NULL COLLATE "C" CHECK ( constraint_kind IN ( 'host' , 'source' ) ),
    ordinal BIGINT NOT NULL CHECK ( ordinal BETWEEN 0 AND 15 ),
    pattern TEXT NOT NULL COLLATE "C",
    PRIMARY KEY ( ingestion_token_id , constraint_kind , ordinal ),
    CHECK ( octet_length ( pattern ) BETWEEN 1 AND 512 ),
    CHECK ( position ( decode ( '00' , 'hex' ) IN convert_to ( pattern , 'UTF8' ) ) = 0 )
);

CREATE TABLE ingest_visibility_state (
    singleton BIGINT PRIMARY KEY NOT NULL CHECK ( singleton = 1 ),
    last_assigned BIGINT NOT NULL CHECK ( last_assigned >= 0 ),
    committed_through BIGINT NOT NULL CHECK ( committed_through >= 0 ),
    CHECK ( committed_through <= last_assigned )
);

CREATE TABLE ingest_visibility_legacy_tombstones (
    batch_key TEXT PRIMARY KEY NOT NULL COLLATE "C",
    legacy_visibility_seq BIGINT NOT NULL UNIQUE CHECK ( legacy_visibility_seq >= 1 ),
    created_at_unix_micro BIGINT NOT NULL,
    CHECK ( length ( batch_key ) BETWEEN 1 AND 512 )
);

CREATE TABLE ingest_batch_identities (
    batch_key TEXT PRIMARY KEY NOT NULL COLLATE "C",
    sequence_key TEXT NOT NULL UNIQUE COLLATE "C",
    payload_sha256 BYTEA NOT NULL CHECK ( length ( payload_sha256 ) = 32 ),
    first_visibility_seq BIGINT NOT NULL CHECK ( first_visibility_seq >= 1 ),
    created_at_unix_micro BIGINT NOT NULL,
    CHECK ( length ( batch_key ) BETWEEN 1 AND 512 ),
    CHECK ( length ( sequence_key ) BETWEEN 1 AND 512 )
);

CREATE TABLE ingest_visibility_reservations (
    sequence BIGINT PRIMARY KEY NOT NULL CHECK ( sequence >= 1 ),
    batch_key TEXT NOT NULL COLLATE "C",
    state TEXT NOT NULL CHECK ( state IN ( 'reserved' , 'committed' , 'rejected' , 'abandoned' ) ),
    phase TEXT NOT NULL CHECK ( phase IN ( 'unsent' , 'ambiguous' , 'final' ) ),
    attempt_id TEXT NOT NULL DEFAULT '' COLLATE "C",
    index_time_unix_milli BIGINT NOT NULL,
    metadata BYTEA NOT NULL CHECK ( length ( metadata ) <= 1048576 ),
    outbox BYTEA NOT NULL CHECK ( length ( outbox ) <= 16777216 ),
    created_at_unix_micro BIGINT NOT NULL,
    committed_at_unix_micro BIGINT,
    outbox_sha256 BYTEA NOT NULL DEFAULT decode ( '' , 'hex' ) CHECK ( length ( outbox_sha256 ) IN ( 0 , 32 ) ),
    stored_row_count BIGINT NOT NULL DEFAULT 0 CHECK ( stored_row_count BETWEEN 0 AND 1000 ),
    decoded_event_bytes BIGINT NOT NULL DEFAULT 0 CHECK ( decoded_event_bytes BETWEEN 0 AND 8388608 ),
    principal_sha256 BYTEA NOT NULL DEFAULT decode ( '' , 'hex' ) CHECK ( length ( principal_sha256 ) IN ( 0 , 32 ) ),
    CHECK ( length ( batch_key ) BETWEEN 1 AND 512 ),
    CHECK ( length ( attempt_id ) <= 128 ),
    CHECK ( ( state = 'reserved' AND phase IN ( 'unsent' , 'ambiguous' ) AND length ( outbox ) BETWEEN 1 AND 16777216 AND committed_at_unix_micro IS NULL ) OR ( state = 'committed' AND phase = 'final' AND attempt_id = '' AND length ( outbox ) = 0 AND committed_at_unix_micro IS NOT NULL ) OR ( state = 'rejected' AND phase = 'final' AND attempt_id = '' AND length ( outbox ) = 0 AND committed_at_unix_micro IS NOT NULL ) OR ( state = 'abandoned' AND phase = 'final' AND attempt_id = '' AND length ( outbox ) = 0 AND committed_at_unix_micro IS NULL ) )
);

CREATE TABLE saved_searches (
    organization_id BIGINT NOT NULL CHECK (organization_id > 0),
    saved_search_id TEXT PRIMARY KEY NOT NULL COLLATE "C",
    version BIGINT NOT NULL CHECK ( version >= 1 ),
    name TEXT NOT NULL COLLATE "C",
    app_id TEXT NOT NULL COLLATE "C",
    owner_id TEXT NOT NULL COLLATE "C",
    sharing_scope BIGINT NOT NULL CHECK ( sharing_scope BETWEEN 1 AND 3 ),
    definition_proto BYTEA NOT NULL CHECK ( length ( definition_proto ) BETWEEN 1 AND 262144 ),
    created_at_unix_micro BIGINT NOT NULL,
    updated_at_unix_micro BIGINT NOT NULL,
    CHECK ( length ( saved_search_id ) BETWEEN 1 AND 128 ),
    CHECK ( length ( name ) BETWEEN 1 AND 255 ),
    CHECK ( length ( app_id ) <= 255 ),
    CHECK ( length ( owner_id ) <= 255 ),
    CHECK ( updated_at_unix_micro >= created_at_unix_micro ),
    UNIQUE ( organization_id , owner_id , app_id , name )
);

CREATE TABLE server_key_state (
    key_name TEXT PRIMARY KEY NOT NULL COLLATE "C",
    fingerprint BYTEA NOT NULL CHECK ( length ( fingerprint ) = 32 ),
    created_at_unix_micro BIGINT NOT NULL,
    CHECK ( key_name = 'server-master-v1' )
);

CREATE TABLE search_history (
    search_job_id TEXT PRIMARY KEY NOT NULL COLLATE "C",
    tenant_id TEXT NOT NULL COLLATE "C",
    owner_id TEXT NOT NULL COLLATE "C",
    app_id TEXT NOT NULL COLLATE "C",
    saved_search_id TEXT NOT NULL COLLATE "C",
    final_state BIGINT NOT NULL CHECK ( final_state BETWEEN 6 AND 9 ),
    search_text TEXT NOT NULL COLLATE "C",
    created_at_unix_micro BIGINT NOT NULL,
    finished_at_unix_micro BIGINT NOT NULL,
    duration_nanoseconds BIGINT NOT NULL CHECK ( duration_nanoseconds >= 0 ),
    matched_events BIGINT NOT NULL CHECK ( matched_events >= 0 ),
    entry_proto BYTEA NOT NULL CHECK ( length ( entry_proto ) BETWEEN 1 AND 524288 ),
    entry_sha256 BYTEA NOT NULL CHECK ( length ( entry_sha256 ) = 32 ),
    CHECK ( length ( search_job_id ) BETWEEN 1 AND 256 ),
    CHECK ( length ( tenant_id ) BETWEEN 1 AND 1024 ),
    CHECK ( length ( owner_id ) BETWEEN 1 AND 255 ),
    CHECK ( length ( app_id ) <= 255 ),
    CHECK ( length ( saved_search_id ) <= 128 ),
    CHECK ( length ( search_text ) BETWEEN 1 AND 65536 ),
    CHECK ( finished_at_unix_micro >= created_at_unix_micro )
);

CREATE TABLE search_history_pending (
    search_job_id TEXT PRIMARY KEY NOT NULL COLLATE "C",
    tenant_id TEXT NOT NULL COLLATE "C",
    owner_id TEXT NOT NULL COLLATE "C",
    state BIGINT NOT NULL CHECK ( state BETWEEN 1 AND 5 ),
    created_at_unix_micro BIGINT NOT NULL,
    entry_proto BYTEA NOT NULL CHECK ( length ( entry_proto ) BETWEEN 1 AND 524288 ),
    entry_sha256 BYTEA NOT NULL CHECK ( length ( entry_sha256 ) = 32 ),
    CHECK ( length ( search_job_id ) BETWEEN 1 AND 256 ),
    CHECK ( length ( tenant_id ) BETWEEN 1 AND 1024 ),
    CHECK ( length ( owner_id ) BETWEEN 1 AND 255 )
);

CREATE TABLE app_workspaces (
    organization_id BIGINT NOT NULL CHECK(organization_id>0),
    app_id TEXT PRIMARY KEY NOT NULL COLLATE "C",
    tenant_id TEXT NOT NULL COLLATE "C",
    version BIGINT NOT NULL CHECK ( version >= 1 ),
    slug TEXT NOT NULL COLLATE "C",
    display_name TEXT NOT NULL COLLATE "C",
    description TEXT NOT NULL DEFAULT '',
    default_time_range_present BIGINT NOT NULL CHECK ( default_time_range_present IN (0,1) ),
    default_earliest TEXT,
    default_latest TEXT,
    default_timezone TEXT,
    state TEXT NOT NULL CHECK ( state IN ( 'active' , 'archived' ) ),
    created_at_unix_micro BIGINT NOT NULL CHECK ( created_at_unix_micro > 0 ),
    updated_at_unix_micro BIGINT NOT NULL CHECK ( updated_at_unix_micro > 0 ),
    archived_at_unix_micro BIGINT,
    CHECK ( length ( app_id ) = 26 ),
    CHECK ( substr ( app_id , 1 , 4 ) = 'app_' ),
    CHECK ( substr ( app_id , 5 ) !~ '^.*[^A-Za-z0-9_-].*$' ),
    CHECK ( substr ( app_id , 26 , 1 ) ~ '^[AQgw]$' ),
    CHECK ( length ( tenant_id ) BETWEEN 1 AND 255 ),
    CHECK ( length ( slug ) BETWEEN 1 AND 128 ),
    CHECK ( slug = lower ( slug ) ),
    CHECK ( slug !~ '^.*[^a-z0-9_-].*$' ),
    CHECK ( substr ( slug , 1 , 1 ) ~ '^[a-z0-9]$' ),
    CHECK ( length ( display_name ) BETWEEN 1 AND 255 ),
    CHECK ( length ( description ) <= 16384 ),
    CHECK ( default_earliest IS NULL OR length ( default_earliest ) BETWEEN 1 AND 1024 ),
    CHECK ( default_latest IS NULL OR length ( default_latest ) BETWEEN 1 AND 1024 ),
    CHECK ( default_timezone IS NULL OR length ( default_timezone ) BETWEEN 1 AND 255 ),
    CHECK ( default_time_range_present = 1 OR ( default_earliest IS NULL AND default_latest IS NULL AND default_timezone IS NULL ) ),
    CHECK ( updated_at_unix_micro >= created_at_unix_micro ),
    CHECK ( ( state = 'active' AND archived_at_unix_micro IS NULL ) OR ( state = 'archived' AND archived_at_unix_micro IS NOT NULL AND archived_at_unix_micro BETWEEN created_at_unix_micro AND updated_at_unix_micro ) ),
    UNIQUE ( tenant_id , organization_id , slug ),
    UNIQUE ( tenant_id , app_id )
);

CREATE TABLE app_catalog_revisions (
    tenant_id TEXT PRIMARY KEY NOT NULL COLLATE "C",
    revision BIGINT NOT NULL CHECK ( revision >= 1 ),
    CHECK ( length ( tenant_id ) BETWEEN 1 AND 255 )
);

CREATE TABLE app_default_indexes (
    tenant_id TEXT NOT NULL COLLATE "C",
    app_id TEXT NOT NULL COLLATE "C",
    index_id TEXT NOT NULL COLLATE "C",
    PRIMARY KEY ( tenant_id , app_id , index_id )
);

CREATE TABLE collector_fleet (
    tenant_id TEXT NOT NULL COLLATE "C",
    collector_id TEXT NOT NULL COLLATE "C",
    admin_version BIGINT NOT NULL CONSTRAINT collector_fleet_admin_version_positive CHECK ( admin_version >= 1 ),
    display_name TEXT COLLATE "C",
    display_name_sort_key TEXT NOT NULL COLLATE "C" GENERATED ALWAYS AS ( coalesce ( display_name , '' ) ) STORED,
    administrative_state TEXT NOT NULL COLLATE "C" CONSTRAINT collector_fleet_administrative_state_valid CHECK ( administrative_state IN ( 'enabled' , 'disabled' ) ),
    first_seen_at_unix_micro BIGINT NOT NULL CONSTRAINT collector_fleet_first_seen_positive CHECK ( first_seen_at_unix_micro BETWEEN 1 AND 253402300799999999 ),
    updated_at_unix_micro BIGINT NOT NULL CONSTRAINT collector_fleet_updated_at_valid CHECK ( updated_at_unix_micro BETWEEN first_seen_at_unix_micro AND 253402300799999999 ),
    CONSTRAINT collector_fleet_tenant_id_bounded CHECK ( octet_length ( tenant_id ) BETWEEN 1 AND 255 AND TRUE ),
    CONSTRAINT collector_fleet_collector_id_canonical CHECK ( length ( collector_id ) BETWEEN 1 AND 128 AND TRUE AND substr ( collector_id , 1 , 1 ) ~ '^[A-Za-z0-9]$' AND collector_id !~ '^.*[^A-Za-z0-9._:-].*$' ),
    CONSTRAINT collector_fleet_display_name_bounded CHECK ( display_name IS NULL OR ( octet_length ( display_name ) BETWEEN 1 AND 255 AND TRUE ) ),
    PRIMARY KEY ( tenant_id , collector_id )
);

CREATE TABLE collector_catalog_revisions (
    tenant_id TEXT PRIMARY KEY NOT NULL COLLATE "C",
    revision BIGINT NOT NULL CONSTRAINT collector_catalog_revisions_revision_positive CHECK ( revision >= 1 ),
    fleet_count BIGINT NOT NULL CONSTRAINT collector_catalog_revisions_fleet_count_bounded CHECK ( fleet_count BETWEEN 0 AND 256 ),
    runtime_count BIGINT NOT NULL CONSTRAINT collector_catalog_revisions_runtime_count_bounded CHECK ( runtime_count BETWEEN 0 AND 256 ),
    CONSTRAINT collector_catalog_revisions_tenant_id_bounded CHECK ( octet_length ( tenant_id ) BETWEEN 1 AND 255 AND TRUE )
);

CREATE TABLE collector_runtime (
    tenant_id TEXT NOT NULL COLLATE "C",
    collector_id TEXT NOT NULL COLLATE "C",
    telemetry_revision BIGINT NOT NULL CONSTRAINT collector_runtime_telemetry_revision_positive CHECK ( telemetry_revision >= 1 ),
    lease_generation BIGINT NOT NULL CONSTRAINT collector_runtime_lease_generation_positive CHECK ( lease_generation >= 1 ),
    boot_epoch TEXT COLLATE "C",
    stream_id TEXT COLLATE "C",
    active_instance_id TEXT COLLATE "C",
    source_revision TEXT NOT NULL COLLATE "C",
    hostname TEXT NOT NULL COLLATE "C",
    operating_system TEXT NOT NULL COLLATE "C",
    architecture TEXT NOT NULL COLLATE "C",
    started_at_unix_micro BIGINT NOT NULL CONSTRAINT collector_runtime_started_at_positive CHECK ( started_at_unix_micro BETWEEN 1 AND 253402300799999999 ),
    connected_at_unix_micro BIGINT NOT NULL CONSTRAINT collector_runtime_connected_at_positive CHECK ( connected_at_unix_micro BETWEEN 1 AND 253402300799999999 ),
    last_seen_at_unix_micro BIGINT NOT NULL CONSTRAINT collector_runtime_last_seen_valid CHECK ( last_seen_at_unix_micro BETWEEN connected_at_unix_micro AND 253402300799999999 ),
    disconnected_at_unix_micro BIGINT,
    observation_sequence BIGINT NOT NULL DEFAULT 0 CONSTRAINT collector_runtime_observation_sequence_nonnegative CHECK ( observation_sequence >= 0 ),
    observed_at_unix_micro BIGINT,
    last_acked_at_hello_sequence BIGINT,
    queued_events BIGINT NOT NULL DEFAULT 0 CONSTRAINT collector_runtime_queued_events_nonnegative CHECK ( queued_events >= 0 ),
    queued_bytes BIGINT NOT NULL DEFAULT 0 CONSTRAINT collector_runtime_queued_bytes_nonnegative CHECK ( queued_bytes >= 0 ),
    oldest_event_age_nanoseconds BIGINT,
    sent_events_total BIGINT NOT NULL DEFAULT 0 CONSTRAINT collector_runtime_sent_events_nonnegative CHECK ( sent_events_total >= 0 ),
    acknowledged_events_total BIGINT NOT NULL DEFAULT 0 CONSTRAINT collector_runtime_acknowledged_events_nonnegative CHECK ( acknowledged_events_total >= 0 ),
    retried_batches_total BIGINT NOT NULL DEFAULT 0 CONSTRAINT collector_runtime_retried_batches_nonnegative CHECK ( retried_batches_total >= 0 ),
    rejected_events_total BIGINT NOT NULL DEFAULT 0 CONSTRAINT collector_runtime_rejected_events_nonnegative CHECK ( rejected_events_total >= 0 ),
    dropped_events_total BIGINT NOT NULL DEFAULT 0 CONSTRAINT collector_runtime_dropped_events_nonnegative CHECK ( dropped_events_total >= 0 ),
    last_sent_batch_sequence BIGINT,
    last_acknowledged_batch_sequence BIGINT,
    process_resident_memory_bytes BIGINT NOT NULL DEFAULT 0 CONSTRAINT collector_runtime_memory_nonnegative CHECK ( process_resident_memory_bytes >= 0 ),
    process_cpu_percent DOUBLE PRECISION NOT NULL DEFAULT 0 CONSTRAINT collector_runtime_cpu_valid CHECK ( process_cpu_percent = process_cpu_percent AND process_cpu_percent BETWEEN 0 AND 1000000 ),
    CONSTRAINT collector_runtime_boot_epoch_canonical CHECK ( boot_epoch IS NULL OR ( length ( boot_epoch ) BETWEEN 1 AND 128 AND TRUE AND substr ( boot_epoch , 1 , 1 ) ~ '^[A-Za-z0-9]$' AND boot_epoch !~ '^.*[^A-Za-z0-9._:-].*$' ) ),
    CONSTRAINT collector_runtime_stream_id_canonical CHECK ( stream_id IS NULL OR ( length ( stream_id ) BETWEEN 1 AND 128 AND TRUE AND substr ( stream_id , 1 , 1 ) ~ '^[A-Za-z0-9]$' AND stream_id !~ '^.*[^A-Za-z0-9._:-].*$' ) ),
    CONSTRAINT collector_runtime_instance_id_canonical CHECK ( active_instance_id IS NULL OR ( length ( active_instance_id ) BETWEEN 1 AND 128 AND TRUE AND substr ( active_instance_id , 1 , 1 ) ~ '^[A-Za-z0-9]$' AND active_instance_id !~ '^.*[^A-Za-z0-9._:-].*$' ) ),
    CONSTRAINT collector_runtime_source_revision_valid CHECK ( source_revision = 'development' OR ( octet_length ( source_revision ) IN ( 40 , 64 ) AND source_revision !~ '^.*[^0-9a-f].*$' ) ),
    CONSTRAINT collector_runtime_metadata_bounded CHECK ( octet_length ( hostname ) <= 255 AND TRUE AND octet_length ( operating_system ) <= 128 AND TRUE AND octet_length ( architecture ) <= 128 AND TRUE ),
    CONSTRAINT collector_runtime_disconnect_valid CHECK ( disconnected_at_unix_micro IS NULL OR disconnected_at_unix_micro BETWEEN last_seen_at_unix_micro AND 253402300799999999 ),
    CONSTRAINT collector_runtime_observation_snapshot_valid CHECK ( ( observation_sequence = 0 AND observed_at_unix_micro IS NULL ) OR ( observation_sequence > 0 AND observed_at_unix_micro IS NOT NULL AND observed_at_unix_micro BETWEEN 1 AND 253402300799999999 ) ),
    CONSTRAINT collector_runtime_hello_sequence_valid CHECK ( last_acked_at_hello_sequence IS NULL OR last_acked_at_hello_sequence >= 0 ),
    CONSTRAINT collector_runtime_oldest_age_valid CHECK ( oldest_event_age_nanoseconds IS NULL OR oldest_event_age_nanoseconds >= 0 ),
    CONSTRAINT collector_runtime_last_sent_sequence_valid CHECK ( last_sent_batch_sequence IS NULL OR last_sent_batch_sequence >= 0 ),
    CONSTRAINT collector_runtime_last_acknowledged_sequence_valid CHECK ( last_acknowledged_batch_sequence IS NULL OR last_acknowledged_batch_sequence >= 0 ),
    CONSTRAINT collector_runtime_active_lease_consistent CHECK ( ( boot_epoch IS NOT NULL AND stream_id IS NOT NULL AND active_instance_id IS NOT NULL AND disconnected_at_unix_micro IS NULL ) OR ( boot_epoch IS NULL AND stream_id IS NULL AND active_instance_id IS NULL AND disconnected_at_unix_micro IS NOT NULL ) ),
    PRIMARY KEY ( tenant_id , collector_id )
);

CREATE TABLE collector_capabilities (
    tenant_id TEXT NOT NULL COLLATE "C",
    collector_id TEXT NOT NULL COLLATE "C",
    capability BIGINT NOT NULL CONSTRAINT collector_capabilities_value_valid CHECK ( capability BETWEEN 1 AND 2147483647 ),
    PRIMARY KEY ( tenant_id , collector_id , capability )
);

CREATE TABLE collector_authorized_indexes (
    tenant_id TEXT NOT NULL COLLATE "C",
    collector_id TEXT NOT NULL COLLATE "C",
    index_name TEXT NOT NULL COLLATE "C",
    CONSTRAINT collector_authorized_indexes_name_canonical CHECK ( length ( index_name ) BETWEEN 1 AND 255 AND index_name = lower ( index_name ) AND index_name !~ '^.*[^a-z0-9_-].*$' AND substr ( index_name , 1 , 1 ) ~ '^[a-z0-9]$' AND position ( 'kvstore' in index_name ) = 0 ),
    PRIMARY KEY ( tenant_id , collector_id , index_name )
);

CREATE TABLE collector_inputs (
    tenant_id TEXT NOT NULL COLLATE "C",
    collector_id TEXT NOT NULL COLLATE "C",
    input_id TEXT NOT NULL COLLATE "C",
    input_type BIGINT NOT NULL CONSTRAINT collector_inputs_type_valid CHECK ( input_type BETWEEN 1 AND 2147483647 ),
    index_name TEXT NOT NULL COLLATE "C",
    source TEXT COLLATE "C",
    sourcetype TEXT COLLATE "C",
    CONSTRAINT collector_inputs_id_canonical CHECK ( length ( input_id ) BETWEEN 1 AND 128 AND TRUE AND substr ( input_id , 1 , 1 ) ~ '^[A-Za-z0-9]$' AND input_id !~ '^.*[^A-Za-z0-9._:-].*$' ),
    CONSTRAINT collector_inputs_index_name_canonical CHECK ( length ( index_name ) BETWEEN 1 AND 255 AND index_name = lower ( index_name ) AND index_name !~ '^.*[^a-z0-9_-].*$' AND substr ( index_name , 1 , 1 ) ~ '^[a-z0-9]$' AND position ( 'kvstore' in index_name ) = 0 ),
    CONSTRAINT collector_inputs_source_bounded CHECK ( source IS NULL OR ( octet_length ( source ) BETWEEN 1 AND 4096 AND TRUE ) ),
    CONSTRAINT collector_inputs_sourcetype_bounded CHECK ( sourcetype IS NULL OR ( octet_length ( sourcetype ) BETWEEN 1 AND 255 AND TRUE ) ),
    PRIMARY KEY ( tenant_id , collector_id , input_id )
);

CREATE TABLE collector_input_health (
    tenant_id TEXT NOT NULL COLLATE "C",
    collector_id TEXT NOT NULL COLLATE "C",
    input_id TEXT NOT NULL COLLATE "C",
    state BIGINT NOT NULL CONSTRAINT collector_input_health_state_valid CHECK ( state BETWEEN 1 AND 2147483647 ),
    status_message TEXT NOT NULL COLLATE "C",
    discovered_sources BIGINT NOT NULL CONSTRAINT collector_input_health_discovered_nonnegative CHECK ( discovered_sources >= 0 ),
    active_sources BIGINT NOT NULL CONSTRAINT collector_input_health_active_valid CHECK ( active_sources BETWEEN 0 AND discovered_sources ),
    events_read_total BIGINT NOT NULL CONSTRAINT collector_input_health_events_nonnegative CHECK ( events_read_total >= 0 ),
    bytes_read_total BIGINT NOT NULL CONSTRAINT collector_input_health_bytes_nonnegative CHECK ( bytes_read_total >= 0 ),
    last_event_at_unix_micro BIGINT,
    last_error_at_unix_micro BIGINT,
    CONSTRAINT collector_input_health_message_bounded CHECK ( octet_length ( status_message ) <= 8192 AND TRUE ),
    CONSTRAINT collector_input_health_last_event_valid CHECK ( last_event_at_unix_micro IS NULL OR last_event_at_unix_micro BETWEEN 1 AND 253402300799999999 ),
    CONSTRAINT collector_input_health_last_error_valid CHECK ( last_error_at_unix_micro IS NULL OR last_error_at_unix_micro BETWEEN 1 AND 253402300799999999 ),
    PRIMARY KEY ( tenant_id , collector_id , input_id )
);

CREATE TABLE search_history_owner_counts (
    tenant_id TEXT NOT NULL COLLATE "C",
    owner_id TEXT NOT NULL COLLATE "C",
    terminal_count BIGINT NOT NULL CHECK ( terminal_count > 0 ),
    PRIMARY KEY ( tenant_id , owner_id ),
    CHECK ( length ( tenant_id ) BETWEEN 1 AND 1024 ),
    CHECK ( length ( owner_id ) BETWEEN 1 AND 255 )
);

CREATE TABLE index_deletion_tombstones (
    index_id TEXT PRIMARY KEY NOT NULL REFERENCES indexes ( index_id ) ON DELETE RESTRICT,
    name TEXT NOT NULL COLLATE "C",
    deleted_version BIGINT NOT NULL CHECK ( deleted_version >= 1 ),
    deleted_at_unix_micro BIGINT NOT NULL CHECK ( deleted_at_unix_micro > 0 )
);

CREATE TABLE index_deletion_operations (
    deletion_operation_id TEXT PRIMARY KEY NOT NULL,
    index_id TEXT NOT NULL UNIQUE REFERENCES indexes ( index_id ) ON DELETE RESTRICT,
    index_name TEXT NOT NULL COLLATE "C",
    tenant_id TEXT NOT NULL COLLATE "C",
    archived_index_version BIGINT NOT NULL CHECK ( archived_index_version >= 1 AND archived_index_version < 9223372036854775807 ),
    created_at_unix_micro BIGINT NOT NULL CHECK ( created_at_unix_micro > 0 ),
    CHECK ( octet_length ( deletion_operation_id ) BETWEEN 1 AND 128 AND position ( decode ( '00' , 'hex' ) IN convert_to ( deletion_operation_id , 'UTF8' ) ) = 0 AND substr ( deletion_operation_id , 1 , 1 ) ~ '^[A-Za-z0-9]$' AND deletion_operation_id !~ '^.*[^A-Za-z0-9._:-].*$' ),
    CONSTRAINT index_deletion_operations_tenant_id_bounded CHECK ( octet_length ( tenant_id ) BETWEEN 1 AND 255 AND position ( decode ( '00' , 'hex' ) IN convert_to ( tenant_id , 'UTF8' ) ) = 0 AND tenant_id = trim ( tenant_id ) AND tenant_id !~ '^.*[--].*$' )
);

CREATE TABLE index_deletion_mutation_attempts (
    deletion_operation_id TEXT PRIMARY KEY NOT NULL REFERENCES index_deletion_operations ( deletion_operation_id ) ON DELETE CASCADE,
    correlation_id TEXT NOT NULL UNIQUE COLLATE "C",
    tenant_id TEXT NOT NULL COLLATE "C",
    clickhouse_database TEXT NOT NULL COLLATE "C",
    clickhouse_table TEXT NOT NULL COLLATE "C",
    clickhouse_table_uuid TEXT NOT NULL COLLATE "C",
    protocol_version BIGINT NOT NULL,
    created_at_unix_micro BIGINT NOT NULL,
    CONSTRAINT index_deletion_mutation_attempts_correlation_id_canonical CHECK ( octet_length ( correlation_id ) BETWEEN 1 AND 128 AND position ( decode ( '00' , 'hex' ) IN convert_to ( correlation_id , 'UTF8' ) ) = 0 AND substr ( correlation_id , 1 , 1 ) ~ '^[A-Za-z0-9]$' AND correlation_id !~ '^.*[^A-Za-z0-9._:-].*$' ),
    CONSTRAINT index_deletion_mutation_attempts_tenant_id_bounded CHECK ( octet_length ( tenant_id ) BETWEEN 1 AND 255 AND position ( decode ( '00' , 'hex' ) IN convert_to ( tenant_id , 'UTF8' ) ) = 0 ),
    CONSTRAINT index_deletion_mutation_attempts_database_canonical CHECK ( octet_length ( clickhouse_database ) BETWEEN 1 AND 255 AND position ( decode ( '00' , 'hex' ) IN convert_to ( clickhouse_database , 'UTF8' ) ) = 0 AND substr ( clickhouse_database , 1 , 1 ) ~ '^[A-Za-z_]$' AND clickhouse_database !~ '^.*[^A-Za-z0-9_].*$' ),
    CONSTRAINT index_deletion_mutation_attempts_table_canonical CHECK ( octet_length ( clickhouse_table ) BETWEEN 1 AND 255 AND position ( decode ( '00' , 'hex' ) IN convert_to ( clickhouse_table , 'UTF8' ) ) = 0 AND substr ( clickhouse_table , 1 , 1 ) ~ '^[A-Za-z_]$' AND clickhouse_table !~ '^.*[^A-Za-z0-9_].*$' ),
    CONSTRAINT index_deletion_mutation_attempts_table_uuid_canonical CHECK ( octet_length ( clickhouse_table_uuid ) = 36 AND position ( decode ( '00' , 'hex' ) IN convert_to ( clickhouse_table_uuid , 'UTF8' ) ) = 0 AND clickhouse_table_uuid = lower ( clickhouse_table_uuid ) AND substr ( clickhouse_table_uuid , 9 , 1 ) = '-' AND substr ( clickhouse_table_uuid , 14 , 1 ) = '-' AND substr ( clickhouse_table_uuid , 19 , 1 ) = '-' AND substr ( clickhouse_table_uuid , 24 , 1 ) = '-' AND length ( replace ( clickhouse_table_uuid , '-' , '' ) ) = 32 AND replace ( clickhouse_table_uuid , '-' , '' ) !~ '^.*[^0-9a-f].*$' AND clickhouse_table_uuid <> '00000000-0000-0000-0000-000000000000' ),
    CONSTRAINT index_deletion_mutation_attempts_protocol_supported CHECK ( protocol_version = 1 ),
    CONSTRAINT index_deletion_mutation_attempts_created_at_positive CHECK ( created_at_unix_micro BETWEEN 1 AND 253402300799999999 )
);

CREATE TABLE index_data_deletion_completions (
    deletion_operation_id TEXT PRIMARY KEY NOT NULL,
    correlation_id TEXT NOT NULL UNIQUE COLLATE "C",
    index_id TEXT NOT NULL UNIQUE REFERENCES indexes ( index_id ) ON DELETE RESTRICT,
    index_name TEXT NOT NULL COLLATE "C",
    archived_index_version BIGINT NOT NULL,
    deleting_index_version BIGINT NOT NULL,
    tenant_id TEXT NOT NULL COLLATE "C",
    clickhouse_database TEXT NOT NULL COLLATE "C",
    clickhouse_table TEXT NOT NULL COLLATE "C",
    clickhouse_table_uuid TEXT NOT NULL COLLATE "C",
    protocol_version BIGINT NOT NULL,
    operation_created_at_unix_micro BIGINT NOT NULL,
    attempt_created_at_unix_micro BIGINT NOT NULL,
    completed_at_unix_micro BIGINT NOT NULL,
    CONSTRAINT index_data_deletion_completions_operation_id_canonical CHECK ( octet_length ( deletion_operation_id ) BETWEEN 1 AND 128 AND position ( decode ( '00' , 'hex' ) IN convert_to ( deletion_operation_id , 'UTF8' ) ) = 0 AND substr ( deletion_operation_id , 1 , 1 ) ~ '^[A-Za-z0-9]$' AND deletion_operation_id !~ '^.*[^A-Za-z0-9._:-].*$' ),
    CONSTRAINT index_data_deletion_completions_correlation_id_canonical CHECK ( octet_length ( correlation_id ) BETWEEN 1 AND 128 AND position ( decode ( '00' , 'hex' ) IN convert_to ( correlation_id , 'UTF8' ) ) = 0 AND substr ( correlation_id , 1 , 1 ) ~ '^[A-Za-z0-9]$' AND correlation_id !~ '^.*[^A-Za-z0-9._:-].*$' ),
    CONSTRAINT index_data_deletion_completions_index_name_canonical CHECK ( octet_length ( index_name ) BETWEEN 1 AND 255 AND position ( decode ( '00' , 'hex' ) IN convert_to ( index_name , 'UTF8' ) ) = 0 AND index_name = lower ( index_name ) AND index_name !~ '^.*[^a-z0-9_-].*$' AND substr ( index_name , 1 , 1 ) ~ '^[a-z0-9]$' AND position ( 'kvstore' in index_name ) = 0 ),
    CONSTRAINT index_data_deletion_completions_versions_supported CHECK ( archived_index_version >= 1 AND archived_index_version < 9223372036854775807 AND deleting_index_version = archived_index_version + 1 ),
    CONSTRAINT index_data_deletion_completions_tenant_id_bounded CHECK ( octet_length ( tenant_id ) BETWEEN 1 AND 255 AND position ( decode ( '00' , 'hex' ) IN convert_to ( tenant_id , 'UTF8' ) ) = 0 ),
    CONSTRAINT index_data_deletion_completions_database_canonical CHECK ( octet_length ( clickhouse_database ) BETWEEN 1 AND 255 AND position ( decode ( '00' , 'hex' ) IN convert_to ( clickhouse_database , 'UTF8' ) ) = 0 AND substr ( clickhouse_database , 1 , 1 ) ~ '^[A-Za-z_]$' AND clickhouse_database !~ '^.*[^A-Za-z0-9_].*$' ),
    CONSTRAINT index_data_deletion_completions_table_canonical CHECK ( octet_length ( clickhouse_table ) BETWEEN 1 AND 255 AND position ( decode ( '00' , 'hex' ) IN convert_to ( clickhouse_table , 'UTF8' ) ) = 0 AND substr ( clickhouse_table , 1 , 1 ) ~ '^[A-Za-z_]$' AND clickhouse_table !~ '^.*[^A-Za-z0-9_].*$' ),
    CONSTRAINT index_data_deletion_completions_table_uuid_canonical CHECK ( octet_length ( clickhouse_table_uuid ) = 36 AND position ( decode ( '00' , 'hex' ) IN convert_to ( clickhouse_table_uuid , 'UTF8' ) ) = 0 AND clickhouse_table_uuid = lower ( clickhouse_table_uuid ) AND substr ( clickhouse_table_uuid , 9 , 1 ) = '-' AND substr ( clickhouse_table_uuid , 14 , 1 ) = '-' AND substr ( clickhouse_table_uuid , 19 , 1 ) = '-' AND substr ( clickhouse_table_uuid , 24 , 1 ) = '-' AND length ( replace ( clickhouse_table_uuid , '-' , '' ) ) = 32 AND replace ( clickhouse_table_uuid , '-' , '' ) !~ '^.*[^0-9a-f].*$' AND clickhouse_table_uuid <> '00000000-0000-0000-0000-000000000000' ),
    CONSTRAINT index_data_deletion_completions_protocol_supported CHECK ( protocol_version = 1 ),
    CONSTRAINT index_data_deletion_completions_timestamps_supported CHECK ( operation_created_at_unix_micro BETWEEN 1 AND 253402300799999999 AND attempt_created_at_unix_micro BETWEEN operation_created_at_unix_micro AND 253402300799999999 AND completed_at_unix_micro BETWEEN attempt_created_at_unix_micro AND 253402300799999999 )
);

CREATE TABLE index_catalog_state (
    singleton_id BIGINT PRIMARY KEY NOT NULL CHECK ( singleton_id = 1 ),
    revision BIGINT NOT NULL CHECK ( revision BETWEEN 1 AND 9223372036854775807 ),
    physical_count BIGINT NOT NULL CHECK ( physical_count BETWEEN 0 AND 1024 )
);

CREATE TABLE ingest_quota_buckets (
    tenant_id TEXT NOT NULL COLLATE "C",
    scope_kind TEXT NOT NULL COLLATE "C",
    scope_id TEXT NOT NULL COLLATE "C",
    max_ingest_events_per_second BIGINT NOT NULL,
    max_ingest_uncompressed_bytes_per_second BIGINT NOT NULL,
    next_event_admission_unix_nano BIGINT NOT NULL,
    next_byte_admission_unix_nano BIGINT NOT NULL,
    updated_at_unix_micro BIGINT NOT NULL,
    token_owner_id TEXT COLLATE "C",
    PRIMARY KEY ( tenant_id , scope_kind , scope_id ),
    CONSTRAINT ingest_quota_buckets_tenant_id_bounded CHECK ( octet_length ( tenant_id ) BETWEEN 1 AND 255 AND position ( decode ( '00' , 'hex' ) IN convert_to ( tenant_id , 'UTF8' ) ) = 0 ),
    CONSTRAINT ingest_quota_buckets_scope_kind_supported CHECK ( scope_kind IN ( 'token' , 'index' ) ),
    CONSTRAINT ingest_quota_buckets_scope_id_bounded CHECK ( octet_length ( scope_id ) BETWEEN 1 AND 255 AND position ( decode ( '00' , 'hex' ) IN convert_to ( scope_id , 'UTF8' ) ) = 0 ),
    CONSTRAINT ingest_quota_buckets_token_owner_consistent CHECK ( ( scope_kind = 'token' AND token_owner_id IS NOT NULL AND token_owner_id = scope_id ) OR ( scope_kind = 'index' AND token_owner_id IS NULL ) ),
    CONSTRAINT ingest_quota_buckets_event_schedule_valid CHECK ( ( max_ingest_events_per_second = 0 AND next_event_admission_unix_nano = 0 ) OR ( max_ingest_events_per_second BETWEEN 1 AND 1000000 AND next_event_admission_unix_nano BETWEEN 1 AND 9223372036854775807 ) ),
    CONSTRAINT ingest_quota_buckets_byte_schedule_valid CHECK ( ( max_ingest_uncompressed_bytes_per_second = 0 AND next_byte_admission_unix_nano = 0 ) OR ( max_ingest_uncompressed_bytes_per_second BETWEEN 1 AND 1099511627776 AND next_byte_admission_unix_nano BETWEEN 1 AND 9223372036854775807 ) ),
    CONSTRAINT ingest_quota_buckets_updated_at_bounded CHECK ( updated_at_unix_micro BETWEEN 1 AND 253402300799999999 )
);

CREATE TABLE ingest_quota_admissions (
    batch_key TEXT PRIMARY KEY NOT NULL COLLATE "C",
    admitted_at_unix_micro BIGINT NOT NULL,
    event_count BIGINT NOT NULL,
    uncompressed_bytes BIGINT NOT NULL,
    CONSTRAINT ingest_quota_admissions_admitted_at_bounded CHECK ( admitted_at_unix_micro BETWEEN 1 AND 253402300799999999 ),
    CONSTRAINT ingest_quota_admissions_event_count_bounded CHECK ( event_count BETWEEN 1 AND 1000 ),
    CONSTRAINT ingest_quota_admissions_uncompressed_bytes_bounded CHECK ( uncompressed_bytes BETWEEN 1 AND 8388608 )
);

CREATE TABLE audit_tenant_state (
    tenant_id TEXT PRIMARY KEY NOT NULL COLLATE "C",
    next_sequence BIGINT NOT NULL CHECK ( next_sequence BETWEEN 1 AND 100001 ),
    event_count BIGINT NOT NULL CHECK ( event_count BETWEEN 0 AND 100000 ),
    CONSTRAINT audit_tenant_state_sequence_matches_count CHECK ( next_sequence = event_count + 1 ),
    CONSTRAINT audit_tenant_state_tenant_id_bounded CHECK ( octet_length ( tenant_id ) BETWEEN 1 AND 255 AND position ( decode ( '00' , 'hex' ) IN convert_to ( tenant_id , 'UTF8' ) ) = 0 AND tenant_id = trim ( tenant_id ) AND tenant_id !~ '^.*[--].*$' )
);

CREATE TABLE search_attempt_audit_tenant_state (
    tenant_id TEXT PRIMARY KEY NOT NULL COLLATE "C",
    first_sequence BIGINT NOT NULL CHECK ( first_sequence BETWEEN 1 AND 9223372036854775807 ),
    next_sequence BIGINT NOT NULL CHECK ( next_sequence BETWEEN 1 AND 9223372036854775807 ),
    retained_count BIGINT NOT NULL CHECK ( retained_count BETWEEN 0 AND 100001 ),
    maximum_retained_attempts BIGINT NOT NULL CHECK ( maximum_retained_attempts BETWEEN 1 AND 100000 ),
    CONSTRAINT search_attempt_audit_state_dense CHECK ( next_sequence >= first_sequence AND next_sequence - first_sequence = retained_count ),
    CONSTRAINT search_attempt_audit_state_bounded CHECK ( retained_count <= maximum_retained_attempts + 1 ),
    CONSTRAINT search_attempt_audit_state_tenant_id_bounded CHECK ( octet_length ( tenant_id ) BETWEEN 1 AND 255 AND position ( decode ( '00' , 'hex' ) IN convert_to ( tenant_id , 'UTF8' ) ) = 0 AND tenant_id = trim ( tenant_id ) AND tenant_id !~ '^.*[--].*$' )
);

CREATE TABLE search_attempt_audit_events (
    tenant_id TEXT NOT NULL COLLATE "C",
    sequence BIGINT NOT NULL CHECK ( sequence BETWEEN 1 AND 9223372036854775806 ),
    occurred_at_unix_micro BIGINT NOT NULL CHECK ( occurred_at_unix_micro BETWEEN 1 AND 253402300799999999 ),
    actor_kind TEXT NOT NULL COLLATE "C" CHECK ( actor_kind IN ( 'system' , 'browser' ) ),
    actor_id TEXT NOT NULL COLLATE "C",
    actor_role TEXT NOT NULL COLLATE "C" CHECK ( actor_role IN ( 'system' , 'user' , 'administrator' ) ),
    owner_id TEXT NOT NULL COLLATE "C",
    search_job_id TEXT NOT NULL COLLATE "C",
    knowledge_snapshot_sha256 BYTEA CHECK ( knowledge_snapshot_sha256 IS NULL OR ( TRUE AND length ( knowledge_snapshot_sha256 ) = 32 ) ),
    knowledge_snapshot_tenant_catalog_revision BIGINT CHECK ( knowledge_snapshot_tenant_catalog_revision IS NULL OR knowledge_snapshot_tenant_catalog_revision BETWEEN 0 AND 9223372036854775806 ),
    knowledge_snapshot_tenant_catalog_state_token BYTEA CHECK ( knowledge_snapshot_tenant_catalog_state_token IS NULL OR ( TRUE AND length ( knowledge_snapshot_tenant_catalog_state_token ) = 32 ) ),
    knowledge_snapshot_object_count BIGINT CHECK ( knowledge_snapshot_object_count IS NULL OR knowledge_snapshot_object_count BETWEEN 0 AND 256 ),
    knowledge_snapshot_lookup_asset_count BIGINT CHECK ( knowledge_snapshot_lookup_asset_count IS NULL OR knowledge_snapshot_lookup_asset_count BETWEEN 0 AND 16 ),
    PRIMARY KEY ( tenant_id , sequence ),
    UNIQUE ( tenant_id , search_job_id ),
    CONSTRAINT search_attempt_audit_actor_id_bounded CHECK ( octet_length ( actor_id ) BETWEEN 1 AND 255 AND position ( decode ( '00' , 'hex' ) IN convert_to ( actor_id , 'UTF8' ) ) = 0 AND actor_id = trim ( actor_id ) AND actor_id !~ '^.*[--].*$' ),
    CONSTRAINT search_attempt_audit_actor_shape_supported CHECK ( ( actor_kind = 'system' AND actor_role = 'system' ) OR ( actor_kind = 'browser' AND actor_role IN ( 'user' , 'administrator' ) ) ),
    CONSTRAINT search_attempt_audit_owner_id_bounded CHECK ( octet_length ( owner_id ) BETWEEN 1 AND 255 AND position ( decode ( '00' , 'hex' ) IN convert_to ( owner_id , 'UTF8' ) ) = 0 AND owner_id = trim ( owner_id ) AND owner_id !~ '^.*[--].*$' ),
    CONSTRAINT search_attempt_audit_job_id_bounded CHECK ( octet_length ( search_job_id ) BETWEEN 1 AND 256 AND position ( decode ( '00' , 'hex' ) IN convert_to ( search_job_id , 'UTF8' ) ) = 0 AND search_job_id = trim ( search_job_id ) AND search_job_id !~ '^.*[--].*$' )
);

CREATE TABLE knowledge_catalog_tenants (
    tenant_id TEXT PRIMARY KEY NOT NULL COLLATE "C",
    catalog_revision BIGINT NOT NULL DEFAULT 0 CHECK ( catalog_revision BETWEEN 0 AND 9223372036854775806 ),
    identity_count BIGINT NOT NULL DEFAULT 0 CHECK ( identity_count BETWEEN 0 AND 8192 ),
    version_count BIGINT NOT NULL DEFAULT 0 CHECK ( version_count BETWEEN 0 AND 65536 ),
    definition_body_bytes BIGINT NOT NULL DEFAULT 0 CHECK ( definition_body_bytes BETWEEN 0 AND 536870912 ),
    idempotency_count BIGINT NOT NULL DEFAULT 0 CHECK ( idempotency_count BETWEEN 0 AND 20480 ),
    active_object_count BIGINT NOT NULL DEFAULT 0 CHECK ( active_object_count BETWEEN 0 AND 4096 ),
    recovery_audit_count BIGINT NOT NULL DEFAULT 0 CHECK ( recovery_audit_count BETWEEN 0 AND 8192 ),
    CONSTRAINT knowledge_catalog_tenant_id_bounded CHECK ( octet_length ( tenant_id ) BETWEEN 1 AND 255 AND position ( decode ( '00' , 'hex' ) IN convert_to ( tenant_id , 'UTF8' ) ) = 0 AND tenant_id = trim ( tenant_id ) AND tenant_id !~ '^.*[--].*$' )
);

CREATE TABLE knowledge_definition_blobs (
    tenant_id TEXT NOT NULL COLLATE "C",
    definition_digest BYTEA NOT NULL CHECK ( length ( definition_digest ) = 32 ),
    definition_proto BYTEA NOT NULL,
    definition_bytes BIGINT NOT NULL CHECK ( definition_bytes BETWEEN 1 AND 4194304 AND definition_bytes = length ( definition_proto ) ),
    created_at_unix_micro BIGINT NOT NULL CHECK ( created_at_unix_micro BETWEEN 1 AND 253402300799999999 ),
    PRIMARY KEY ( tenant_id , definition_digest )
);

CREATE TABLE knowledge_objects (
    organization_id BIGINT NOT NULL CHECK(organization_id>0),
    tenant_id TEXT NOT NULL COLLATE "C",
    knowledge_object_id TEXT NOT NULL COLLATE "C",
    current_version BIGINT NOT NULL CHECK ( current_version BETWEEN 1 AND 9223372036854775807 ),
    app_id TEXT NOT NULL COLLATE "C",
    owner_id TEXT NOT NULL COLLATE "C",
    object_type TEXT NOT NULL COLLATE "C" CHECK ( object_type IN ( 'field_extraction' , 'field_alias' , 'calculated_field' ) ),
    name TEXT NOT NULL COLLATE "C",
    sharing_scope TEXT NOT NULL COLLATE "C" CHECK ( sharing_scope IN ( 'private' , 'app' , 'global' ) ),
    state TEXT NOT NULL COLLATE "C" CHECK ( state IN ( 'draft' , 'active' , 'disabled' , 'quarantined' , 'deleted' ) ),
    definition_digest BYTEA CHECK ( definition_digest IS NULL OR length ( definition_digest ) = 32 ),
    definition_digest_key BYTEA GENERATED ALWAYS AS ( coalesce ( definition_digest , decode ( '' , 'hex' ) ) ) STORED,
    created_at_unix_micro BIGINT NOT NULL CHECK ( created_at_unix_micro BETWEEN 1 AND 253402300799999999 ),
    updated_at_unix_micro BIGINT NOT NULL CHECK ( updated_at_unix_micro BETWEEN 1 AND 253402300799999999 ),
    disabled_at_unix_micro BIGINT,
    quarantined_at_unix_micro BIGINT,
    deleted_at_unix_micro BIGINT,
    quarantine_reason TEXT COLLATE "C" CHECK ( quarantine_reason IS NULL OR quarantine_reason IN ( 'root_corruption' , 'dependency_recovery' ) ),
    PRIMARY KEY ( tenant_id , knowledge_object_id ),
    UNIQUE ( tenant_id , knowledge_object_id , current_version ),
    CONSTRAINT knowledge_objects_id_bounded CHECK ( octet_length ( knowledge_object_id ) BETWEEN 1 AND 128 AND position ( decode ( '00' , 'hex' ) IN convert_to ( knowledge_object_id , 'UTF8' ) ) = 0 AND knowledge_object_id = trim ( knowledge_object_id ) AND knowledge_object_id !~ '^.*[--].*$' ),
    CONSTRAINT knowledge_objects_owner_id_bounded CHECK ( octet_length ( owner_id ) BETWEEN 1 AND 255 AND position ( decode ( '00' , 'hex' ) IN convert_to ( owner_id , 'UTF8' ) ) = 0 AND owner_id = trim ( owner_id ) AND owner_id !~ '^.*[--].*$' ),
    CONSTRAINT knowledge_objects_name_bounded CHECK ( octet_length ( name ) BETWEEN 1 AND 255 AND position ( decode ( '00' , 'hex' ) IN convert_to ( name , 'UTF8' ) ) = 0 AND name = trim ( name ) AND name !~ '^.*[--].*$' ),
    CONSTRAINT knowledge_objects_time_ordered CHECK ( updated_at_unix_micro >= created_at_unix_micro AND ( disabled_at_unix_micro IS NULL OR disabled_at_unix_micro BETWEEN created_at_unix_micro AND updated_at_unix_micro ) AND ( quarantined_at_unix_micro IS NULL OR quarantined_at_unix_micro BETWEEN created_at_unix_micro AND updated_at_unix_micro ) AND ( deleted_at_unix_micro IS NULL OR deleted_at_unix_micro BETWEEN created_at_unix_micro AND updated_at_unix_micro ) ),
    CONSTRAINT knowledge_objects_state_shape_supported CHECK ( ( state IN ( 'draft' , 'active' ) AND definition_digest IS NOT NULL AND disabled_at_unix_micro IS NULL AND quarantined_at_unix_micro IS NULL AND deleted_at_unix_micro IS NULL AND quarantine_reason IS NULL ) OR ( state = 'disabled' AND definition_digest IS NOT NULL AND disabled_at_unix_micro IS NOT NULL AND quarantined_at_unix_micro IS NULL AND deleted_at_unix_micro IS NULL AND quarantine_reason IS NULL ) OR ( state = 'quarantined' AND definition_digest IS NULL AND disabled_at_unix_micro IS NULL AND quarantined_at_unix_micro IS NOT NULL AND deleted_at_unix_micro IS NULL AND quarantine_reason IS NOT NULL ) OR ( state = 'deleted' AND definition_digest IS NOT NULL AND disabled_at_unix_micro IS NULL AND quarantined_at_unix_micro IS NULL AND deleted_at_unix_micro IS NOT NULL AND quarantine_reason IS NULL ) )
);

CREATE TABLE knowledge_object_versions (
    tenant_id TEXT NOT NULL COLLATE "C",
    knowledge_object_id TEXT NOT NULL COLLATE "C",
    object_version BIGINT NOT NULL CHECK ( object_version BETWEEN 1 AND 9223372036854775807 ),
    app_id TEXT NOT NULL COLLATE "C",
    owner_id TEXT NOT NULL COLLATE "C",
    object_type TEXT NOT NULL COLLATE "C" CHECK ( object_type IN ( 'field_extraction' , 'field_alias' , 'calculated_field' ) ),
    name TEXT NOT NULL COLLATE "C",
    sharing_scope TEXT NOT NULL COLLATE "C" CHECK ( sharing_scope IN ( 'private' , 'app' , 'global' ) ),
    state TEXT NOT NULL COLLATE "C" CHECK ( state IN ( 'draft' , 'active' , 'disabled' , 'quarantined' , 'deleted' ) ),
    definition_digest BYTEA CHECK ( definition_digest IS NULL OR length ( definition_digest ) = 32 ),
    definition_digest_key BYTEA GENERATED ALWAYS AS ( coalesce ( definition_digest , decode ( '' , 'hex' ) ) ) STORED,
    quarantine_object_id TEXT COLLATE "C" GENERATED ALWAYS AS ( CASE WHEN state = 'quarantined' THEN knowledge_object_id END ) STORED,
    quarantine_object_version BIGINT GENERATED ALWAYS AS ( CASE WHEN state = 'quarantined' THEN object_version END ) STORED,
    dependency_count BIGINT NOT NULL CHECK ( dependency_count BETWEEN 0 AND 1024 ),
    mutation_kind TEXT NOT NULL COLLATE "C" CHECK ( mutation_kind IN ( 'create' , 'update' , 'scope_change' , 'enable' , 'disable' , 'quarantine' , 'delete' ) ),
    quarantine_reason TEXT COLLATE "C" CHECK ( quarantine_reason IS NULL OR quarantine_reason IN ( 'root_corruption' , 'dependency_recovery' ) ),
    created_at_unix_micro BIGINT NOT NULL CHECK ( created_at_unix_micro BETWEEN 1 AND 253402300799999999 ),
    PRIMARY KEY ( tenant_id , knowledge_object_id , object_version ),
    UNIQUE ( tenant_id , knowledge_object_id , object_version , app_id , owner_id , object_type , name , sharing_scope , state , definition_digest_key ),
    UNIQUE ( tenant_id , knowledge_object_id , object_version , dependency_count ),
    CONSTRAINT knowledge_object_versions_first_is_create CHECK ( ( object_version = 1 AND mutation_kind = 'create' ) OR ( object_version > 1 AND mutation_kind <> 'create' ) ),
    CONSTRAINT knowledge_object_versions_state_shape_supported CHECK ( ( state = 'quarantined' ) = ( mutation_kind = 'quarantine' ) AND ( state = 'deleted' ) = ( mutation_kind = 'delete' ) AND ( state = 'quarantined' ) = ( quarantine_reason IS NOT NULL ) AND ( state = 'quarantined' ) = ( definition_digest IS NULL ) AND ( mutation_kind <> 'enable' OR state = 'active' ) AND ( mutation_kind <> 'disable' OR state = 'disabled' ) AND ( mutation_kind <> 'create' OR state IN ( 'draft' , 'active' ) ) AND ( mutation_kind NOT IN ( 'create' , 'update' , 'scope_change' ) OR state IN ( 'draft' , 'active' , 'disabled' ) ) ),
    CONSTRAINT knowledge_object_versions_name_bounded CHECK ( octet_length ( name ) BETWEEN 1 AND 255 AND position ( decode ( '00' , 'hex' ) IN convert_to ( name , 'UTF8' ) ) = 0 AND name = trim ( name ) AND name !~ '^.*[--].*$' ),
    CONSTRAINT knowledge_object_versions_owner_id_bounded CHECK ( octet_length ( owner_id ) BETWEEN 1 AND 255 AND position ( decode ( '00' , 'hex' ) IN convert_to ( owner_id , 'UTF8' ) ) = 0 AND owner_id = trim ( owner_id ) AND owner_id !~ '^.*[--].*$' )
);

CREATE TABLE knowledge_object_dependencies (
    tenant_id TEXT NOT NULL COLLATE "C",
    source_object_id TEXT NOT NULL COLLATE "C",
    source_object_version BIGINT NOT NULL,
    ordinal BIGINT NOT NULL CHECK ( ordinal BETWEEN 0 AND 1023 ),
    target_kind TEXT NOT NULL COLLATE "C" CHECK ( target_kind = 'object' ),
    target_object_id TEXT NOT NULL COLLATE "C",
    target_object_version BIGINT NOT NULL,
    dependency_role TEXT NOT NULL COLLATE "C" CHECK ( dependency_role = 'field_input' ),
    PRIMARY KEY ( tenant_id , source_object_id , source_object_version , ordinal ),
    UNIQUE ( tenant_id , source_object_id , source_object_version , target_kind , target_object_id , target_object_version , dependency_role ),
    CHECK ( source_object_id <> target_object_id OR source_object_version <> target_object_version )
);

CREATE TABLE knowledge_object_dependency_seals (
    tenant_id TEXT NOT NULL COLLATE "C",
    knowledge_object_id TEXT NOT NULL COLLATE "C",
    object_version BIGINT NOT NULL CHECK ( object_version >= 1 ),
    dependency_count BIGINT NOT NULL CHECK ( dependency_count BETWEEN 0 AND 1024 ),
    PRIMARY KEY ( tenant_id , knowledge_object_id , object_version )
);

CREATE TABLE knowledge_object_acl (
    tenant_id TEXT NOT NULL COLLATE "C",
    knowledge_object_id TEXT NOT NULL COLLATE "C",
    role_id TEXT NOT NULL COLLATE "C",
    can_read BIGINT NOT NULL CHECK ( can_read IN (0,1) ),
    can_write BIGINT NOT NULL CHECK ( can_write IN (0,1) ),
    PRIMARY KEY ( tenant_id , knowledge_object_id , role_id ),
    CHECK ( can_read = 1 OR can_write = 1 ),
    CHECK ( can_write = 0 OR can_read = 1 ),
    CHECK ( octet_length ( role_id ) BETWEEN 1 AND 255 AND position ( decode ( '00' , 'hex' ) IN convert_to ( role_id , 'UTF8' ) ) = 0 AND role_id = trim ( role_id ) AND role_id !~ '^.*[--].*$' )
);

CREATE TABLE knowledge_app_active_counters (
    tenant_id TEXT NOT NULL COLLATE "C",
    app_id TEXT NOT NULL COLLATE "C",
    active_object_count BIGINT NOT NULL CHECK ( active_object_count BETWEEN 0 AND 1024 ),
    PRIMARY KEY ( tenant_id , app_id )
);

CREATE TABLE knowledge_owner_active_counters (
    tenant_id TEXT NOT NULL COLLATE "C",
    owner_id TEXT NOT NULL COLLATE "C",
    active_private_object_count BIGINT NOT NULL CHECK ( active_private_object_count BETWEEN 0 AND 512 ),
    PRIMARY KEY ( tenant_id , owner_id )
);

CREATE TABLE knowledge_type_active_counters (
    tenant_id TEXT NOT NULL COLLATE "C",
    object_type TEXT NOT NULL COLLATE "C" CHECK ( object_type IN ( 'field_extraction' , 'field_alias' , 'calculated_field' ) ),
    active_object_count BIGINT NOT NULL CHECK ( active_object_count BETWEEN 0 AND 2048 ),
    PRIMARY KEY ( tenant_id , object_type )
);

CREATE TABLE knowledge_app_type_active_counters (
    tenant_id TEXT NOT NULL COLLATE "C",
    app_id TEXT NOT NULL COLLATE "C",
    object_type TEXT NOT NULL COLLATE "C" CHECK ( object_type IN ( 'field_extraction' , 'field_alias' , 'calculated_field' ) ),
    active_object_count BIGINT NOT NULL CHECK ( active_object_count BETWEEN 0 AND 512 ),
    PRIMARY KEY ( tenant_id , app_id , object_type )
);

CREATE TABLE knowledge_recovery_audit (
    tenant_id TEXT NOT NULL COLLATE "C",
    sequence BIGINT NOT NULL CHECK ( sequence BETWEEN 1 AND 8192 ),
    knowledge_object_id TEXT NOT NULL COLLATE "C",
    object_version BIGINT NOT NULL CHECK ( object_version >= 1 ),
    actor_kind TEXT NOT NULL COLLATE "C" CHECK ( actor_kind IN ( 'system' , 'browser' ) ),
    actor_id TEXT NOT NULL COLLATE "C",
    actor_role TEXT NOT NULL COLLATE "C" CHECK ( actor_role IN ( 'system' , 'administrator' ) ),
    app_id TEXT NOT NULL COLLATE "C",
    object_type TEXT NOT NULL COLLATE "C" CHECK ( object_type IN ( 'field_extraction' , 'field_alias' , 'calculated_field' ) ),
    sharing_scope TEXT NOT NULL COLLATE "C" CHECK ( sharing_scope IN ( 'private' , 'app' , 'global' ) ),
    recovery_reason TEXT NOT NULL COLLATE "C" CHECK ( recovery_reason IN ( 'root_corruption' , 'dependency_recovery' ) ),
    occurred_at_unix_micro BIGINT NOT NULL CHECK ( occurred_at_unix_micro BETWEEN 1 AND 253402300799999999 ),
    PRIMARY KEY ( tenant_id , sequence ),
    UNIQUE ( tenant_id , knowledge_object_id ),
    CHECK ( ( actor_kind = 'system' AND actor_role = 'system' ) OR ( actor_kind = 'browser' AND actor_role = 'administrator' ) ),
    CHECK ( octet_length ( actor_id ) BETWEEN 1 AND 255 AND position ( decode ( '00' , 'hex' ) IN convert_to ( actor_id , 'UTF8' ) ) = 0 AND actor_id = trim ( actor_id ) AND actor_id !~ '^.*[--].*$' )
);

CREATE TABLE knowledge_projection_tenant_ledgers (
    tenant_id TEXT PRIMARY KEY NOT NULL COLLATE "C",
    projection_bytes BIGINT NOT NULL DEFAULT 0 CHECK ( projection_bytes BETWEEN 0 AND 268435456 )
);

CREATE TABLE knowledge_object_list_projections (
    tenant_id TEXT NOT NULL COLLATE "C",
    knowledge_object_id TEXT NOT NULL COLLATE "C",
    object_version BIGINT NOT NULL CHECK ( object_version >= 1 ),
    app_id TEXT NOT NULL COLLATE "C",
    owner_id TEXT NOT NULL COLLATE "C",
    object_type TEXT NOT NULL COLLATE "C" CHECK ( object_type IN ( 'field_extraction' , 'field_alias' , 'calculated_field' ) ),
    name TEXT NOT NULL COLLATE "C",
    sharing_scope TEXT NOT NULL COLLATE "C" CHECK ( sharing_scope IN ( 'private' , 'app' , 'global' ) ),
    state TEXT NOT NULL COLLATE "C" CHECK ( state IN ( 'draft' , 'active' , 'disabled' , 'quarantined' , 'deleted' ) ),
    description_present BIGINT NOT NULL CHECK ( description_present IN ( 0 , 1 ) ),
    description TEXT NOT NULL COLLATE "C" DEFAULT '',
    index_selector_count BIGINT NOT NULL CHECK ( index_selector_count BETWEEN 0 AND 16 ),
    host_selector_count BIGINT NOT NULL CHECK ( host_selector_count BETWEEN 0 AND 16 ),
    source_selector_count BIGINT NOT NULL CHECK ( source_selector_count BETWEEN 0 AND 16 ),
    sourcetype_selector_count BIGINT NOT NULL CHECK ( sourcetype_selector_count BETWEEN 0 AND 16 ),
    selector_value_bytes BIGINT NOT NULL CHECK ( selector_value_bytes BETWEEN 0 AND 8192 ),
    canonical_selector_bytes BIGINT NOT NULL CHECK ( canonical_selector_bytes BETWEEN 0 AND 8192 AND selector_value_bytes <= canonical_selector_bytes ),
    projection_bytes BIGINT GENERATED ALWAYS AS ( octet_length ( description ) + selector_value_bytes ) STORED,
    PRIMARY KEY ( tenant_id , knowledge_object_id , object_version ),
    CONSTRAINT knowledge_list_projection_description_canonical CHECK ( octet_length ( description ) <= 16384 AND position ( decode ( '00' , 'hex' ) IN convert_to ( description , 'UTF8' ) ) = 0 AND ( ( description_present = 0 AND description = '' ) OR ( description_present = 1 AND octet_length ( description ) >= 1 AND description = trim ( description ) AND description !~ '^.*[--].*$' ) ) ),
    CONSTRAINT knowledge_list_projection_selector_count_bounded CHECK ( index_selector_count + host_selector_count + source_selector_count + sourcetype_selector_count <= 64 ),
    CONSTRAINT knowledge_list_projection_selector_charge_exact CHECK ( ( state = 'quarantined' AND canonical_selector_bytes = 0 ) OR ( state <> 'quarantined' AND canonical_selector_bytes = 43 + 4 * ( index_selector_count + host_selector_count + source_selector_count + sourcetype_selector_count ) + selector_value_bytes ) ),
    CONSTRAINT knowledge_list_projection_quarantine_is_bodyless CHECK ( state <> 'quarantined' OR ( description_present = 0 AND index_selector_count = 0 AND host_selector_count = 0 AND source_selector_count = 0 AND sourcetype_selector_count = 0 AND selector_value_bytes = 0 AND canonical_selector_bytes = 0 ) )
);

CREATE TABLE knowledge_object_list_selector_patterns (
    tenant_id TEXT NOT NULL COLLATE "C",
    knowledge_object_id TEXT NOT NULL COLLATE "C",
    object_version BIGINT NOT NULL CHECK ( object_version >= 1 ),
    dimension TEXT NOT NULL COLLATE "C" CHECK ( dimension IN ( 'index' , 'host' , 'source' , 'sourcetype' ) ),
    ordinal BIGINT NOT NULL CHECK ( ordinal BETWEEN 0 AND 15 ),
    match_kind TEXT NOT NULL COLLATE "C" CHECK ( match_kind IN ( 'exact' , 'wildcard' ) ),
    value TEXT NOT NULL COLLATE "C",
    value_bytes BIGINT GENERATED ALWAYS AS ( octet_length ( value ) ) STORED,
    PRIMARY KEY ( tenant_id , knowledge_object_id , object_version , dimension , ordinal ),
    UNIQUE ( tenant_id , knowledge_object_id , object_version , dimension , value ),
    CHECK ( value_bytes BETWEEN 1 AND 255 ),
    CHECK ( position ( decode ( '00' , 'hex' ) IN convert_to ( value , 'UTF8' ) ) = 0 ),
    CHECK ( value = trim ( value ) AND value !~ '^.*[--].*$' )
);

CREATE TABLE knowledge_object_list_projection_seals (
    tenant_id TEXT NOT NULL COLLATE "C",
    knowledge_object_id TEXT NOT NULL COLLATE "C",
    object_version BIGINT NOT NULL CHECK ( object_version >= 1 ),
    projection_bytes BIGINT NOT NULL CHECK ( projection_bytes BETWEEN 0 AND 268435456 ),
    canonical_selector_bytes BIGINT NOT NULL CHECK ( canonical_selector_bytes BETWEEN 0 AND 8192 ),
    PRIMARY KEY ( tenant_id , knowledge_object_id , object_version )
);

CREATE TABLE knowledge_attempt_audit_tenant_state (
    tenant_id TEXT PRIMARY KEY NOT NULL COLLATE "C",
    first_sequence BIGINT NOT NULL CHECK ( first_sequence BETWEEN 1 AND 9223372036854775807 ),
    next_sequence BIGINT NOT NULL CHECK ( next_sequence BETWEEN 1 AND 9223372036854775807 ),
    retained_count BIGINT NOT NULL CHECK ( retained_count BETWEEN 0 AND 100001 ),
    CONSTRAINT knowledge_attempt_audit_state_dense CHECK ( next_sequence >= first_sequence AND next_sequence - first_sequence = retained_count ),
    CONSTRAINT knowledge_attempt_audit_state_tenant_id_bounded CHECK ( octet_length ( tenant_id ) BETWEEN 1 AND 255 AND position ( decode ( '00' , 'hex' ) IN convert_to ( tenant_id , 'UTF8' ) ) = 0 AND tenant_id = trim ( tenant_id ) AND tenant_id !~ '^.*[--].*$' )
);

CREATE TABLE knowledge_attempt_audit_events (
    tenant_id TEXT NOT NULL COLLATE "C",
    sequence BIGINT NOT NULL CHECK ( sequence BETWEEN 1 AND 9223372036854775806 ),
    occurred_at_unix_micro BIGINT NOT NULL CHECK ( occurred_at_unix_micro BETWEEN 1 AND 253402300799999999 ),
    actor_kind TEXT NOT NULL COLLATE "C" CHECK ( actor_kind = 'browser' ),
    actor_id TEXT NOT NULL COLLATE "C",
    actor_role TEXT NOT NULL COLLATE "C" CHECK ( actor_role IN ( 'user' , 'administrator' ) ),
    action TEXT NOT NULL COLLATE "C" CHECK ( action IN ( 'create' , 'get' , 'list' , 'update' , 'scope_change' , 'enable' , 'disable' , 'quarantine' , 'delete' , 'validate' , 'dependencies' , 'dependents' , 'preview' ) ),
    result TEXT NOT NULL COLLATE "C" CHECK ( result = 'rejected' ),
    reason TEXT NOT NULL COLLATE "C" CHECK ( reason IN ( 'not_administrator' , 'not_found_or_forbidden' , 'version_conflict' , 'idempotency_conflict' , 'invalid_definition' , 'forbidden_dependency' , 'resource_limit' , 'service_unavailable' ) ),
    app_id TEXT COLLATE "C",
    knowledge_object_id TEXT COLLATE "C",
    object_type TEXT COLLATE "C" CHECK ( object_type IS NULL OR object_type IN ( 'field_extraction' , 'field_alias' , 'calculated_field' ) ),
    object_version BIGINT CHECK ( object_version IS NULL OR object_version >= 1 ),
    sharing_scope TEXT COLLATE "C" CHECK ( sharing_scope IS NULL OR sharing_scope IN ( 'private' , 'app' , 'global' ) ),
    PRIMARY KEY ( tenant_id , sequence ),
    CONSTRAINT knowledge_attempt_audit_actor_id_bounded CHECK ( octet_length ( actor_id ) BETWEEN 1 AND 255 AND position ( decode ( '00' , 'hex' ) IN convert_to ( actor_id , 'UTF8' ) ) = 0 AND actor_id = trim ( actor_id ) AND actor_id !~ '^.*[--].*$' ),
    CONSTRAINT knowledge_attempt_audit_actor_reason_shape CHECK ( ( actor_role = 'user' AND reason = 'not_administrator' AND app_id IS NULL AND knowledge_object_id IS NULL AND object_type IS NULL AND object_version IS NULL AND sharing_scope IS NULL ) OR ( actor_role = 'administrator' AND reason <> 'not_administrator' ) ),
    CONSTRAINT knowledge_attempt_audit_app_shape CHECK ( app_id IS NULL OR ( octet_length ( app_id ) BETWEEN 1 AND 128 AND position ( decode ( '00' , 'hex' ) IN convert_to ( app_id , 'UTF8' ) ) = 0 AND app_id = trim ( app_id ) AND app_id !~ '^.*[--].*$' ) ),
    CONSTRAINT knowledge_attempt_audit_object_shape CHECK ( ( knowledge_object_id IS NULL AND object_type IS NULL AND object_version IS NULL AND sharing_scope IS NULL ) OR ( app_id IS NOT NULL AND reason NOT IN ( 'not_administrator' , 'not_found_or_forbidden' ) AND knowledge_object_id IS NOT NULL AND object_type IS NOT NULL AND object_version IS NOT NULL AND sharing_scope IS NOT NULL AND octet_length ( knowledge_object_id ) BETWEEN 1 AND 128 AND position ( decode ( '00' , 'hex' ) IN convert_to ( knowledge_object_id , 'UTF8' ) ) = 0 AND knowledge_object_id = trim ( knowledge_object_id ) AND knowledge_object_id !~ '^.*[--].*$' ) ),
    CONSTRAINT knowledge_attempt_audit_action_object_shape CHECK ( action NOT IN ( 'create' , 'list' ) OR knowledge_object_id IS NULL ),
    CONSTRAINT knowledge_attempt_audit_version_conflict_shape CHECK ( reason <> 'version_conflict' OR knowledge_object_id IS NOT NULL ),
    CONSTRAINT knowledge_attempt_audit_serialized_bound CHECK ( octet_length ( tenant_id ) + octet_length ( actor_kind ) + octet_length ( actor_id ) + octet_length ( actor_role ) + octet_length ( action ) + octet_length ( result ) + octet_length ( reason ) + coalesce ( octet_length ( app_id ) , 0 ) + coalesce ( octet_length ( knowledge_object_id ) , 0 ) + coalesce ( octet_length ( object_type ) , 0 ) + coalesce ( octet_length ( sharing_scope ) , 0 ) <= 4096 )
);

CREATE TABLE knowledge_catalog_revision_heads (
    tenant_id TEXT PRIMARY KEY NOT NULL COLLATE "C",
    catalog_revision BIGINT NOT NULL CHECK ( catalog_revision BETWEEN 0 AND 9223372036854775806 ),
    state_token BYTEA NOT NULL CHECK ( length ( state_token ) = 32 )
);

CREATE TABLE knowledge_object_version_lifecycle (
    tenant_id TEXT NOT NULL COLLATE "C",
    knowledge_object_id TEXT NOT NULL COLLATE "C",
    object_version BIGINT NOT NULL CHECK ( object_version >= 1 ),
    state TEXT NOT NULL COLLATE "C" CHECK ( state IN ( 'draft' , 'active' , 'disabled' , 'quarantined' , 'deleted' ) ),
    disabled_at_unix_micro BIGINT CHECK ( disabled_at_unix_micro IS NULL OR disabled_at_unix_micro BETWEEN 1 AND 253402300799999999 ),
    quarantined_at_unix_micro BIGINT CHECK ( quarantined_at_unix_micro IS NULL OR quarantined_at_unix_micro BETWEEN 1 AND 253402300799999999 ),
    deleted_at_unix_micro BIGINT CHECK ( deleted_at_unix_micro IS NULL OR deleted_at_unix_micro BETWEEN 1 AND 253402300799999999 ),
    quarantine_reason TEXT COLLATE "C" CHECK ( quarantine_reason IS NULL OR quarantine_reason IN ( 'root_corruption' , 'dependency_recovery' ) ),
    PRIMARY KEY ( tenant_id , knowledge_object_id , object_version ),
    CONSTRAINT knowledge_object_version_lifecycle_shape_is_exact CHECK ( ( state IN ( 'draft' , 'active' ) AND disabled_at_unix_micro IS NULL AND quarantined_at_unix_micro IS NULL AND deleted_at_unix_micro IS NULL AND quarantine_reason IS NULL ) OR ( state = 'disabled' AND disabled_at_unix_micro IS NOT NULL AND quarantined_at_unix_micro IS NULL AND deleted_at_unix_micro IS NULL AND quarantine_reason IS NULL ) OR ( state = 'quarantined' AND disabled_at_unix_micro IS NULL AND quarantined_at_unix_micro IS NOT NULL AND deleted_at_unix_micro IS NULL AND quarantine_reason IS NOT NULL ) OR ( state = 'deleted' AND disabled_at_unix_micro IS NULL AND quarantined_at_unix_micro IS NULL AND deleted_at_unix_micro IS NOT NULL AND quarantine_reason IS NULL ) )
);

CREATE TABLE knowledge_object_list_order_keys (
    tenant_id TEXT NOT NULL COLLATE "C",
    knowledge_object_id TEXT NOT NULL COLLATE "C",
    object_version BIGINT NOT NULL CHECK ( object_version >= 1 ),
    created_at_unix_micro BIGINT NOT NULL CHECK ( created_at_unix_micro BETWEEN 1 AND 253402300799999999 ),
    updated_at_unix_micro BIGINT NOT NULL CHECK ( updated_at_unix_micro BETWEEN created_at_unix_micro AND 253402300799999999 ),
    PRIMARY KEY ( tenant_id , knowledge_object_id , object_version )
);

CREATE TABLE knowledge_mutation_commit_authorities (
    tenant_id TEXT NOT NULL COLLATE "C",
    actor_kind TEXT NOT NULL COLLATE "C" CHECK ( actor_kind IN ( 'browser' , 'system' ) ),
    actor_id TEXT NOT NULL COLLATE "C",
    route TEXT NOT NULL COLLATE "C",
    client_request_id TEXT NOT NULL COLLATE "C",
    request_digest BYTEA NOT NULL CHECK ( length ( request_digest ) = 32 ),
    catalog_revision BIGINT NOT NULL CHECK ( catalog_revision BETWEEN 1 AND 9223372036854775806 ),
    catalog_state_token BYTEA NOT NULL CHECK ( length ( catalog_state_token ) = 32 ),
    mutation_kind TEXT NOT NULL COLLATE "C" CHECK ( mutation_kind IN ( 'create' , 'update' , 'scope_change' , 'enable' , 'disable' , 'quarantine' , 'delete' ) ),
    knowledge_object_id TEXT NOT NULL COLLATE "C",
    object_version BIGINT NOT NULL CHECK ( object_version >= 1 ),
    occurred_at_unix_micro BIGINT NOT NULL CHECK ( occurred_at_unix_micro BETWEEN 1 AND 253402300799999999 ),
    retention_anchor_unix_micro BIGINT NOT NULL CHECK ( retention_anchor_unix_micro BETWEEN 1 AND 253402300799999999 ),
    retain_until_unix_micro BIGINT NOT NULL CHECK ( retain_until_unix_micro BETWEEN 1 AND 253402300799999999 ),
    successful_audit_sequence BIGINT CHECK ( successful_audit_sequence IS NULL OR successful_audit_sequence BETWEEN 1 AND 100000 ),
    recovery_audit_sequence BIGINT CHECK ( recovery_audit_sequence IS NULL OR recovery_audit_sequence BETWEEN 1 AND 8192 ),
    PRIMARY KEY ( tenant_id , catalog_revision ),
    UNIQUE ( tenant_id , catalog_state_token ),
    UNIQUE ( tenant_id , catalog_revision , catalog_state_token ),
    UNIQUE ( tenant_id , catalog_revision , catalog_state_token , actor_kind , actor_id , route , client_request_id , request_digest ),
    CONSTRAINT knowledge_mutation_commit_actor_id_bounded CHECK ( octet_length ( actor_id ) BETWEEN 1 AND 255 AND position ( decode ( '00' , 'hex' ) IN convert_to ( actor_id , 'UTF8' ) ) = 0 AND actor_id = trim ( actor_id ) AND actor_id !~ '^.*[--].*$' ),
    CONSTRAINT knowledge_mutation_commit_request_id_bounded CHECK ( octet_length ( client_request_id ) BETWEEN 16 AND 128 AND client_request_id !~ '^.*[^!-~].*$' ),
    CONSTRAINT knowledge_mutation_commit_route_matches_kind CHECK ( ( route = 'objects.create' AND mutation_kind = 'create' ) OR ( route = 'objects.update' AND mutation_kind IN ( 'update' , 'scope_change' ) ) OR ( route = 'objects.set_state' AND mutation_kind IN ( 'enable' , 'disable' ) ) OR ( route = 'objects.delete' AND mutation_kind = 'delete' ) OR ( route = 'objects.quarantine' AND mutation_kind = 'quarantine' ) ),
    CONSTRAINT knowledge_mutation_commit_audit_shape_is_exact CHECK ( ( mutation_kind = 'quarantine' AND successful_audit_sequence IS NULL AND recovery_audit_sequence IS NOT NULL ) OR ( mutation_kind <> 'quarantine' AND successful_audit_sequence IS NOT NULL AND recovery_audit_sequence IS NULL ) ),
    CONSTRAINT knowledge_mutation_commit_retention_is_bounded CHECK ( retention_anchor_unix_micro >= occurred_at_unix_micro AND retain_until_unix_micro - retention_anchor_unix_micro BETWEEN 604800000000 AND 31536000000000 )
);

CREATE TABLE knowledge_mutation_idempotency (
    tenant_id TEXT NOT NULL COLLATE "C",
    actor_kind TEXT NOT NULL COLLATE "C" CHECK ( actor_kind IN ( 'system' , 'browser' ) ),
    actor_id TEXT NOT NULL COLLATE "C",
    route TEXT NOT NULL COLLATE "C",
    client_request_id TEXT NOT NULL COLLATE "C",
    mutation_kind TEXT NOT NULL COLLATE "C" CHECK ( mutation_kind IN ( 'create' , 'update' , 'scope_change' , 'enable' , 'disable' , 'quarantine' , 'delete' ) ),
    request_digest_format_version BIGINT NOT NULL CHECK ( request_digest_format_version = 1 ),
    request_digest BYTEA NOT NULL CHECK ( length ( request_digest ) = 32 ),
    outcome_format_version BIGINT NOT NULL CHECK ( outcome_format_version = 1 ),
    outcome_proto BYTEA NOT NULL CHECK ( length ( outcome_proto ) BETWEEN 1 AND 1024 ),
    committed_catalog_revision BIGINT NOT NULL CHECK ( committed_catalog_revision BETWEEN 1 AND 9223372036854775806 ),
    committed_catalog_state_token BYTEA NOT NULL CHECK ( length ( committed_catalog_state_token ) = 32 ),
    knowledge_object_id TEXT NOT NULL COLLATE "C",
    object_version BIGINT NOT NULL CHECK ( object_version >= 1 ),
    successful_audit_sequence BIGINT CHECK ( successful_audit_sequence IS NULL OR successful_audit_sequence BETWEEN 1 AND 100000 ),
    recovery_audit_sequence BIGINT CHECK ( recovery_audit_sequence IS NULL OR recovery_audit_sequence BETWEEN 1 AND 8192 ),
    created_at_unix_micro BIGINT NOT NULL CHECK ( created_at_unix_micro BETWEEN 1 AND 253402300799999999 ),
    retention_anchor_unix_micro BIGINT NOT NULL CHECK ( retention_anchor_unix_micro BETWEEN 1 AND 253402300799999999 ),
    retain_until_unix_micro BIGINT NOT NULL CHECK ( retain_until_unix_micro BETWEEN 1 AND 253402300799999999 ),
    PRIMARY KEY ( tenant_id , actor_kind , actor_id , route , client_request_id ),
    CONSTRAINT knowledge_mutation_idempotency_actor_id_bounded CHECK ( octet_length ( actor_id ) BETWEEN 1 AND 255 AND position ( decode ( '00' , 'hex' ) IN convert_to ( actor_id , 'UTF8' ) ) = 0 AND actor_id = trim ( actor_id ) AND actor_id !~ '^.*[--].*$' ),
    CONSTRAINT knowledge_mutation_idempotency_route_matches_kind CHECK ( ( route = 'objects.create' AND mutation_kind = 'create' ) OR ( route = 'objects.update' AND mutation_kind IN ( 'update' , 'scope_change' ) ) OR ( route = 'objects.set_state' AND mutation_kind IN ( 'enable' , 'disable' ) ) OR ( route = 'objects.delete' AND mutation_kind = 'delete' ) OR ( route = 'objects.quarantine' AND mutation_kind = 'quarantine' ) ),
    CONSTRAINT knowledge_mutation_idempotency_request_id_bounded CHECK ( octet_length ( client_request_id ) BETWEEN 16 AND 128 AND client_request_id !~ '^.*[^!-~].*$' ),
    CONSTRAINT knowledge_mutation_idempotency_retention_is_bounded CHECK ( retention_anchor_unix_micro >= created_at_unix_micro AND retain_until_unix_micro - retention_anchor_unix_micro BETWEEN 604800000000 AND 31536000000000 ),
    CONSTRAINT knowledge_mutation_idempotency_audit_shape_is_exact CHECK ( ( mutation_kind = 'quarantine' AND successful_audit_sequence IS NULL AND recovery_audit_sequence IS NOT NULL ) OR ( mutation_kind <> 'quarantine' AND successful_audit_sequence IS NOT NULL AND recovery_audit_sequence IS NULL ) )
);

CREATE TABLE ingestion_token_hec_profiles (
    ingestion_token_id TEXT PRIMARY KEY NOT NULL COLLATE "C" REFERENCES ingestion_tokens ( ingestion_token_id ) ON UPDATE RESTRICT ON DELETE CASCADE,
    default_index_id TEXT COLLATE "C",
    default_host TEXT COLLATE "C",
    default_source TEXT COLLATE "C",
    default_sourcetype TEXT COLLATE "C",
    indexer_acknowledgment BOOLEAN NOT NULL CHECK ( indexer_acknowledgment IN (FALSE, TRUE) ),
    CONSTRAINT ingestion_token_hec_profiles_default_index_membership FOREIGN KEY ( ingestion_token_id , default_index_id ) REFERENCES ingestion_token_indexes ( ingestion_token_id , index_id ) ON UPDATE RESTRICT ON DELETE RESTRICT,
    CONSTRAINT ingestion_token_hec_profiles_default_host_bounded CHECK ( default_host IS NULL OR ( octet_length ( default_host ) BETWEEN 1 AND 255 AND position ( decode ( '00' , 'hex' ) IN convert_to ( default_host , 'UTF8' ) ) = 0 AND default_host = trim ( default_host ) AND default_host !~ '^.*[--].*$' ) ),
    CONSTRAINT ingestion_token_hec_profiles_default_source_bounded CHECK ( default_source IS NULL OR ( octet_length ( default_source ) BETWEEN 1 AND 255 AND position ( decode ( '00' , 'hex' ) IN convert_to ( default_source , 'UTF8' ) ) = 0 AND default_source = trim ( default_source ) AND default_source !~ '^.*[--].*$' ) ),
    CONSTRAINT ingestion_token_hec_profiles_default_sourcetype_bounded CHECK ( default_sourcetype IS NULL OR ( octet_length ( default_sourcetype ) BETWEEN 1 AND 255 AND position ( decode ( '00' , 'hex' ) IN convert_to ( default_sourcetype , 'UTF8' ) ) = 0 AND default_sourcetype = trim ( default_sourcetype ) AND default_sourcetype !~ '^.*[--].*$' ) )
);

CREATE TABLE hec_source_sequences (
    tenant_id TEXT NOT NULL COLLATE "C",
    ingestion_token_id TEXT NOT NULL COLLATE "C" REFERENCES ingestion_tokens ( ingestion_token_id ) ON UPDATE RESTRICT ON DELETE CASCADE,
    next_request_sequence BIGINT NOT NULL DEFAULT 1 CHECK ( next_request_sequence >= 1 ),
    updated_at_unix_micro BIGINT NOT NULL,
    PRIMARY KEY ( tenant_id , ingestion_token_id ),
    CHECK ( octet_length ( tenant_id ) BETWEEN 1 AND 255 ),
    CHECK ( position ( decode ( '00' , 'hex' ) IN convert_to ( tenant_id , 'UTF8' ) ) = 0 )
);

CREATE TABLE hec_requests (
    tenant_id TEXT NOT NULL COLLATE "C",
    ingestion_token_id TEXT NOT NULL COLLATE "C",
    request_sequence BIGINT NOT NULL CHECK ( request_sequence >= 1 ),
    request_id TEXT NOT NULL COLLATE "C",
    visibility_sequence BIGINT UNIQUE REFERENCES ingest_visibility_reservations ( sequence ) ON UPDATE RESTRICT ON DELETE SET NULL,
    state TEXT NOT NULL DEFAULT 'pending' COLLATE "C" CHECK ( state IN ( 'pending' , 'indexed' , 'terminal_failure' ) ),
    created_at_unix_micro BIGINT NOT NULL,
    terminal_at_unix_micro BIGINT,
    PRIMARY KEY ( tenant_id , ingestion_token_id , request_sequence ),
    UNIQUE ( tenant_id , ingestion_token_id , request_id ),
    CHECK ( octet_length ( request_id ) BETWEEN 1 AND 128 ),
    CHECK ( position ( decode ( '00' , 'hex' ) IN convert_to ( request_id , 'UTF8' ) ) = 0 ),
    CHECK ( ( state = 'pending' AND visibility_sequence IS NOT NULL AND terminal_at_unix_micro IS NULL ) OR ( state IN ( 'indexed' , 'terminal_failure' ) AND terminal_at_unix_micro IS NOT NULL ) )
);

CREATE TABLE hec_channels (
    tenant_id TEXT NOT NULL COLLATE "C",
    ingestion_token_id TEXT NOT NULL COLLATE "C" REFERENCES ingestion_tokens ( ingestion_token_id ) ON UPDATE RESTRICT ON DELETE CASCADE,
    channel_id TEXT NOT NULL COLLATE "C",
    next_acknowledgment_id BIGINT NOT NULL DEFAULT 1 CHECK ( next_acknowledgment_id >= 1 ),
    created_at_unix_micro BIGINT NOT NULL,
    last_used_at_unix_micro BIGINT NOT NULL,
    PRIMARY KEY ( tenant_id , ingestion_token_id , channel_id ),
    CHECK ( octet_length ( tenant_id ) BETWEEN 1 AND 255 ),
    CHECK ( position ( decode ( '00' , 'hex' ) IN convert_to ( tenant_id , 'UTF8' ) ) = 0 ),
    CHECK ( octet_length ( channel_id ) BETWEEN 1 AND 128 ),
    CHECK ( position ( decode ( '00' , 'hex' ) IN convert_to ( channel_id , 'UTF8' ) ) = 0 ),
    CHECK ( channel_id = trim ( channel_id ) ),
    CHECK ( last_used_at_unix_micro >= created_at_unix_micro )
);

CREATE TABLE hec_acknowledgments (
    tenant_id TEXT NOT NULL COLLATE "C",
    ingestion_token_id TEXT NOT NULL COLLATE "C",
    channel_id TEXT NOT NULL COLLATE "C",
    acknowledgment_id BIGINT NOT NULL CHECK ( acknowledgment_id BETWEEN 1 AND 9007199254740991 ),
    request_sequence BIGINT NOT NULL CHECK ( request_sequence >= 1 ),
    created_at_unix_micro BIGINT NOT NULL,
    PRIMARY KEY ( tenant_id , ingestion_token_id , channel_id , acknowledgment_id ),
    UNIQUE ( tenant_id , ingestion_token_id , request_sequence )
);

CREATE TABLE knowledge_lookup_asset_tenant_ledgers (
    tenant_id TEXT PRIMARY KEY NOT NULL COLLATE "C",
    staged_asset_count BIGINT NOT NULL DEFAULT 0 CHECK ( staged_asset_count BETWEEN 0 AND 64 ),
    asset_identity_count BIGINT NOT NULL DEFAULT 0 CHECK ( asset_identity_count BETWEEN 0 AND 2048 ),
    published_version_count BIGINT NOT NULL DEFAULT 0 CHECK ( published_version_count BETWEEN 0 AND 8192 ),
    stored_content_bytes BIGINT NOT NULL DEFAULT 0 CHECK ( stored_content_bytes BETWEEN 0 AND 2147483648 )
);

CREATE TABLE knowledge_lookup_asset_stages (
    tenant_id TEXT NOT NULL COLLATE "C",
    stage_id TEXT NOT NULL COLLATE "C",
    owner_id TEXT NOT NULL COLLATE "C",
    source_sha256 BYTEA NOT NULL CHECK ( length ( source_sha256 ) = 32 ),
    content_sha256 BYTEA NOT NULL CHECK ( length ( content_sha256 ) = 32 ),
    source_bytes BIGINT NOT NULL CHECK ( source_bytes BETWEEN 1 AND 8388608 ),
    canonical_csv BYTEA NOT NULL,
    canonical_bytes BIGINT NOT NULL CHECK ( canonical_bytes BETWEEN 1 AND 8388608 AND canonical_bytes = length ( canonical_csv ) ),
    row_count BIGINT NOT NULL CHECK ( row_count BETWEEN 0 AND 100000 ),
    column_count BIGINT NOT NULL CHECK ( column_count BETWEEN 1 AND 64 ),
    created_at_unix_micro BIGINT NOT NULL CHECK ( created_at_unix_micro BETWEEN 1 AND 253402300799999999 ),
    expires_at_unix_micro BIGINT NOT NULL CHECK ( expires_at_unix_micro BETWEEN 2 AND 253402300799999999 AND expires_at_unix_micro > created_at_unix_micro ),
    PRIMARY KEY ( tenant_id , stage_id ),
    CONSTRAINT knowledge_lookup_asset_stage_id_bounded CHECK ( octet_length ( stage_id ) BETWEEN 1 AND 128 AND position ( decode ( '00' , 'hex' ) IN convert_to ( stage_id , 'UTF8' ) ) = 0 AND stage_id = trim ( stage_id ) AND stage_id !~ '^.*[--].*$' ),
    CONSTRAINT knowledge_lookup_asset_stage_owner_bounded CHECK ( octet_length ( owner_id ) BETWEEN 1 AND 255 AND position ( decode ( '00' , 'hex' ) IN convert_to ( owner_id , 'UTF8' ) ) = 0 AND owner_id = trim ( owner_id ) AND owner_id !~ '^.*[--].*$' )
);

CREATE TABLE knowledge_lookup_assets (
    tenant_id TEXT NOT NULL COLLATE "C",
    lookup_asset_id TEXT NOT NULL COLLATE "C",
    current_version BIGINT NOT NULL CHECK ( current_version BETWEEN 1 AND 9223372036854775807 ),
    created_at_unix_micro BIGINT NOT NULL CHECK ( created_at_unix_micro BETWEEN 1 AND 253402300799999999 ),
    updated_at_unix_micro BIGINT NOT NULL CHECK ( updated_at_unix_micro BETWEEN created_at_unix_micro AND 253402300799999999 ),
    PRIMARY KEY ( tenant_id , lookup_asset_id ),
    UNIQUE ( tenant_id , lookup_asset_id , current_version ),
    CONSTRAINT knowledge_lookup_asset_id_bounded CHECK ( octet_length ( lookup_asset_id ) BETWEEN 1 AND 128 AND position ( decode ( '00' , 'hex' ) IN convert_to ( lookup_asset_id , 'UTF8' ) ) = 0 AND lookup_asset_id = trim ( lookup_asset_id ) AND lookup_asset_id !~ '^.*[--].*$' )
);

CREATE TABLE knowledge_lookup_asset_versions (
    tenant_id TEXT NOT NULL COLLATE "C",
    lookup_asset_id TEXT NOT NULL COLLATE "C",
    asset_version BIGINT NOT NULL CHECK ( asset_version BETWEEN 1 AND 9223372036854775807 ),
    source_sha256 BYTEA NOT NULL CHECK ( length ( source_sha256 ) = 32 ),
    content_sha256 BYTEA NOT NULL CHECK ( length ( content_sha256 ) = 32 ),
    source_bytes BIGINT NOT NULL CHECK ( source_bytes BETWEEN 1 AND 8388608 ),
    canonical_csv BYTEA NOT NULL,
    canonical_bytes BIGINT NOT NULL CHECK ( canonical_bytes BETWEEN 1 AND 8388608 AND canonical_bytes = length ( canonical_csv ) ),
    row_count BIGINT NOT NULL CHECK ( row_count BETWEEN 0 AND 100000 ),
    column_count BIGINT NOT NULL CHECK ( column_count BETWEEN 1 AND 64 ),
    created_at_unix_micro BIGINT NOT NULL CHECK ( created_at_unix_micro BETWEEN 1 AND 253402300799999999 ),
    PRIMARY KEY ( tenant_id , lookup_asset_id , asset_version ),
    UNIQUE ( tenant_id , lookup_asset_id , asset_version , content_sha256 , canonical_bytes )
);

CREATE TABLE knowledge_lookup_definitions (
    organization_id BIGINT NOT NULL CHECK(organization_id>0),
    tenant_id TEXT NOT NULL,
    lookup_id TEXT NOT NULL,
    owner_id TEXT NOT NULL,
    app_id TEXT NOT NULL,
    name TEXT NOT NULL,
    sharing_scope BIGINT NOT NULL CHECK ( sharing_scope IN ( 1 , 2 , 3 ) ),
    automatic BOOLEAN NOT NULL CHECK ( automatic IN (FALSE, TRUE) ),
    current_version BIGINT NOT NULL CHECK ( current_version BETWEEN 1 AND 9223372036854775807 ),
    state TEXT NOT NULL CHECK ( state IN ( 'ACTIVE' , 'DISABLED' , 'DELETED' ) ),
    created_at_unix_micro BIGINT NOT NULL CHECK ( created_at_unix_micro BETWEEN 1 AND 253402300799999999 ),
    updated_at_unix_micro BIGINT NOT NULL CHECK ( updated_at_unix_micro BETWEEN created_at_unix_micro AND 253402300799999999 ),
    disabled_at_unix_micro BIGINT CHECK ( disabled_at_unix_micro IS NULL OR disabled_at_unix_micro BETWEEN created_at_unix_micro AND updated_at_unix_micro ),
    deleted_at_unix_micro BIGINT CHECK ( deleted_at_unix_micro IS NULL OR deleted_at_unix_micro BETWEEN created_at_unix_micro AND updated_at_unix_micro ),
    PRIMARY KEY ( tenant_id , lookup_id ),
    UNIQUE ( tenant_id , lookup_id , current_version ),
    CHECK ( length ( tenant_id ) BETWEEN 1 AND 255 ),
    CHECK ( length ( lookup_id ) BETWEEN 1 AND 128 ),
    CHECK ( length ( owner_id ) BETWEEN 1 AND 255 ),
    CHECK ( length ( app_id ) BETWEEN 1 AND 128 ),
    CHECK ( length ( name ) BETWEEN 1 AND 255 ),
    CHECK ( ( state = 'ACTIVE' AND disabled_at_unix_micro IS NULL AND deleted_at_unix_micro IS NULL ) OR ( state = 'DISABLED' AND disabled_at_unix_micro IS NOT NULL AND deleted_at_unix_micro IS NULL ) OR ( state = 'DELETED' AND disabled_at_unix_micro IS NOT NULL AND deleted_at_unix_micro IS NOT NULL AND deleted_at_unix_micro >= disabled_at_unix_micro ) ),
    description TEXT NOT NULL DEFAULT ''
);

CREATE TABLE knowledge_lookup_definition_versions (
    tenant_id TEXT NOT NULL,
    lookup_id TEXT NOT NULL,
    definition_version BIGINT NOT NULL CHECK ( definition_version BETWEEN 1 AND 9223372036854775807 ),
    lookup_asset_id TEXT NOT NULL,
    asset_version BIGINT NOT NULL CHECK ( asset_version >= 1 ),
    asset_size_bytes BIGINT NOT NULL CHECK ( asset_size_bytes BETWEEN 1 AND 8388608 ),
    asset_content_sha256 BYTEA NOT NULL CHECK ( length ( asset_content_sha256 ) = 32 ),
    definition_proto BYTEA NOT NULL CHECK ( length ( definition_proto ) BETWEEN 1 AND 65536 ),
    columns_blob BYTEA NOT NULL CHECK ( length ( columns_blob ) BETWEEN 9 AND 16580 ),
    mutation_kind TEXT NOT NULL CHECK ( mutation_kind IN ( 'CREATE' , 'REPLACE' , 'ENABLE' , 'DISABLE' , 'DELETE' ) ),
    state TEXT NOT NULL CHECK ( state IN ( 'ACTIVE' , 'DISABLED' , 'DELETED' ) ),
    disabled_at_unix_micro BIGINT CHECK ( disabled_at_unix_micro IS NULL OR disabled_at_unix_micro BETWEEN 1 AND created_at_unix_micro ),
    deleted_at_unix_micro BIGINT CHECK ( deleted_at_unix_micro IS NULL OR deleted_at_unix_micro BETWEEN 1 AND created_at_unix_micro ),
    created_at_unix_micro BIGINT NOT NULL CHECK ( created_at_unix_micro BETWEEN 1 AND 253402300799999999 ),
    PRIMARY KEY ( tenant_id , lookup_id , definition_version ),
    CHECK ( ( mutation_kind = 'CREATE' AND state = 'ACTIVE' AND disabled_at_unix_micro IS NULL AND deleted_at_unix_micro IS NULL ) OR ( mutation_kind = 'REPLACE' AND ( ( state = 'ACTIVE' AND disabled_at_unix_micro IS NULL AND deleted_at_unix_micro IS NULL ) OR ( state = 'DISABLED' AND disabled_at_unix_micro IS NOT NULL AND deleted_at_unix_micro IS NULL ) ) ) OR ( mutation_kind = 'ENABLE' AND state = 'ACTIVE' AND disabled_at_unix_micro IS NULL AND deleted_at_unix_micro IS NULL ) OR ( mutation_kind = 'DISABLE' AND state = 'DISABLED' AND disabled_at_unix_micro = created_at_unix_micro AND deleted_at_unix_micro IS NULL ) OR ( mutation_kind = 'DELETE' AND state = 'DELETED' AND disabled_at_unix_micro IS NOT NULL AND deleted_at_unix_micro = created_at_unix_micro AND deleted_at_unix_micro >= disabled_at_unix_micro ) )
);

CREATE TABLE dashboards (
    organization_id BIGINT NOT NULL CHECK (organization_id > 0),
    dashboard_id TEXT PRIMARY KEY NOT NULL COLLATE "C",
    version BIGINT NOT NULL CHECK ( version >= 1 ),
    name TEXT NOT NULL COLLATE "C",
    app_id TEXT NOT NULL COLLATE "C",
    tenant_id TEXT NOT NULL COLLATE "C",
    owner_id TEXT NOT NULL COLLATE "C",
    sharing_scope BIGINT NOT NULL CHECK ( sharing_scope BETWEEN 1 AND 3 ),
    definition_proto BYTEA NOT NULL CHECK ( length ( definition_proto ) BETWEEN 1 AND 98304 ),
    created_at_unix_micro BIGINT NOT NULL,
    updated_at_unix_micro BIGINT NOT NULL,
    CHECK ( length ( dashboard_id ) BETWEEN 1 AND 128 ),
    CHECK ( length ( name ) BETWEEN 1 AND 255 ),
    CHECK ( length ( app_id ) BETWEEN 1 AND 255 ),
    CHECK ( length ( tenant_id ) BETWEEN 1 AND 255 ),
    CHECK ( length ( owner_id ) BETWEEN 1 AND 255 ),
    CHECK ( updated_at_unix_micro >= created_at_unix_micro ),
    UNIQUE ( organization_id , tenant_id , owner_id , app_id , name )
);

CREATE TABLE server_search_settings (
    singleton_id BIGINT PRIMARY KEY NOT NULL CHECK ( singleton_id = 1 ),
    version BIGINT NOT NULL CHECK ( version BETWEEN 1 AND 9223372036854775807 ),
    maximum_runtime_nanoseconds BIGINT NOT NULL CHECK ( maximum_runtime_nanoseconds BETWEEN 10000000000 AND 86400000000000 ),
    maximum_memory_bytes BIGINT NOT NULL CHECK ( maximum_memory_bytes BETWEEN 67108864 AND 68719476736 ),
    maximum_rows_to_read BIGINT NOT NULL CHECK ( maximum_rows_to_read BETWEEN 1 AND 10000000000 ),
    maximum_bytes_to_read BIGINT NOT NULL CHECK ( maximum_bytes_to_read BETWEEN 1048576 AND 17592186044416 ),
    maximum_grouped_rows BIGINT NOT NULL CHECK ( maximum_grouped_rows BETWEEN 1 AND 10000000 ),
    maximum_threads BIGINT NOT NULL CHECK ( maximum_threads BETWEEN 1 AND 64 ),
    maximum_result_rows BIGINT NOT NULL CHECK ( maximum_result_rows BETWEEN 1 AND 10000000 ),
    maximum_result_bytes BIGINT NOT NULL CHECK ( maximum_result_bytes BETWEEN 1048576 AND 4294967296 ),
    maximum_total_result_bytes BIGINT NOT NULL CHECK ( maximum_total_result_bytes BETWEEN maximum_result_bytes AND 68719476736 ),
    maximum_concurrent_searches BIGINT NOT NULL CHECK ( maximum_concurrent_searches BETWEEN 1 AND 256 ),
    result_retention_nanoseconds BIGINT NOT NULL CHECK ( result_retention_nanoseconds BETWEEN 60000000000 AND 2592000000000000 ),
    updated_at_unix_micro BIGINT NOT NULL CHECK ( updated_at_unix_micro BETWEEN 1 AND 253402300799999999 )
);

CREATE FUNCTION os_empty_capture_payload(payload BYTEA) RETURNS BOOLEAN LANGUAGE plpgsql IMMUTABLE AS $$
DECLARE value JSONB;
BEGIN
 value:=convert_from(payload,'UTF8')::jsonb;
 RETURN COALESCE(value->'job'->'CaptureComplete'='true'::jsonb AND value->'job'->'RowCount'='0'::jsonb,FALSE);
EXCEPTION WHEN invalid_text_representation OR character_not_in_repertoire OR invalid_parameter_value THEN RETURN FALSE;
END;
$$;

CREATE TABLE durable_search_jobs (
 organization_id BIGINT NOT NULL CHECK(organization_id>0),
    id TEXT PRIMARY KEY NOT NULL COLLATE "C",
    tenant_id TEXT NOT NULL COLLATE "C",
    owner_id TEXT NOT NULL COLLATE "C",
    state BIGINT NOT NULL CHECK ( state BETWEEN 1 AND 9 ),
    visibility BIGINT NOT NULL DEFAULT 1 CHECK ( visibility IN ( 1 , 2 ) ),
    retention_class BIGINT NOT NULL CHECK ( retention_class BETWEEN 1 AND 5 ),
    lifetime_ns BIGINT NOT NULL CHECK ( lifetime_ns BETWEEN 1 AND 315360000000000000 ),
    job_payload BYTEA NOT NULL CHECK ( length ( job_payload ) BETWEEN 1 AND 2097152 ),
    artifact_name TEXT COLLATE "C",
    artifact_sha256 BYTEA,
    artifact_size_bytes BIGINT NOT NULL DEFAULT 0 CHECK ( artifact_size_bytes >= 0 ),
    created_at_us BIGINT NOT NULL CHECK ( created_at_us > 0 ),
    started_at_us BIGINT,
    finished_at_us BIGINT,
    last_accessed_at_us BIGINT,
    expires_at_us BIGINT,
    tombstoned_at_us BIGINT,
    version BIGINT NOT NULL CHECK ( version >= 1 ),
    CHECK ( octet_length ( id ) BETWEEN 1 AND 256 ),
    CHECK ( octet_length ( tenant_id ) BETWEEN 1 AND 1024 ),
    CHECK ( octet_length ( owner_id ) BETWEEN 1 AND 255 ),
    CHECK ( ( artifact_name IS NULL AND artifact_sha256 IS NULL AND artifact_size_bytes = 0 ) OR ( octet_length ( artifact_name ) BETWEEN 1 AND 320 AND artifact_name NOT LIKE '%/%' AND artifact_name NOT LIKE '%\\%' AND length ( artifact_sha256 ) = 32 AND (artifact_size_bytes > 0 OR (artifact_size_bytes=0 AND os_empty_capture_payload(job_payload))) ) ),
    CHECK ( started_at_us IS NULL OR started_at_us >= created_at_us ),
    CHECK ( finished_at_us IS NULL OR finished_at_us >= created_at_us ),
    CHECK ( last_accessed_at_us IS NULL OR last_accessed_at_us >= created_at_us ),
    CHECK ( expires_at_us IS NULL OR expires_at_us >= created_at_us ),
    CHECK ( tombstoned_at_us IS NULL OR expires_at_us IS NOT NULL )
);

CREATE TABLE saved_search_schedules (
    saved_search_id TEXT PRIMARY KEY NOT NULL COLLATE "C" REFERENCES saved_searches ( saved_search_id ) ON UPDATE RESTRICT ON DELETE CASCADE,
    owner_id TEXT NOT NULL COLLATE "C",
    tenant_id TEXT NOT NULL COLLATE "C",
    config_version BIGINT NOT NULL CHECK ( config_version >= 1 ),
    runtime_version BIGINT NOT NULL CHECK ( runtime_version >= 1 ),
    cron_expression TEXT NOT NULL COLLATE "C",
    timezone TEXT NOT NULL COLLATE "C",
    dispatch_ttl TEXT NOT NULL COLLATE "C",
    enabled BOOLEAN NOT NULL CHECK ( enabled IN ( FALSE , TRUE ) ),
    next_run_at_unix_micro BIGINT,
    created_at_unix_micro BIGINT NOT NULL CHECK ( created_at_unix_micro > 0 ),
    updated_at_unix_micro BIGINT NOT NULL CHECK ( updated_at_unix_micro >= created_at_unix_micro ),
    CHECK ( octet_length ( owner_id ) BETWEEN 1 AND 255 ),
    CHECK ( octet_length ( tenant_id ) BETWEEN 1 AND 255 ),
    CHECK ( octet_length ( cron_expression ) BETWEEN 9 AND 255 ),
    CHECK ( octet_length ( timezone ) BETWEEN 1 AND 255 ),
    CHECK ( octet_length ( dispatch_ttl ) BETWEEN 0 AND 32 ),
    CHECK ( next_run_at_unix_micro IS NULL OR next_run_at_unix_micro > 0 )
);

CREATE TABLE saved_search_schedule_runs (
    run_id TEXT PRIMARY KEY NOT NULL COLLATE "C",
    saved_search_id TEXT NOT NULL COLLATE "C" REFERENCES saved_searches ( saved_search_id ) ON UPDATE RESTRICT ON DELETE CASCADE,
    owner_id TEXT NOT NULL COLLATE "C",
    tenant_id TEXT NOT NULL COLLATE "C",
    definition_version BIGINT NOT NULL CHECK ( definition_version >= 1 ),
    definition_proto BYTEA NOT NULL CHECK ( length ( definition_proto ) BETWEEN 1 AND 262144 ),
    cron_expression TEXT NOT NULL COLLATE "C",
    timezone TEXT NOT NULL COLLATE "C",
    dispatch_ttl TEXT NOT NULL COLLATE "C",
    schedule_period_microseconds BIGINT NOT NULL CHECK ( schedule_period_microseconds > 0 ),
    retention_lifetime_microseconds BIGINT NOT NULL CHECK ( retention_lifetime_microseconds > 0 ),
    scheduled_at_unix_micro BIGINT NOT NULL CHECK ( scheduled_at_unix_micro > 0 ),
    claimed_at_unix_micro BIGINT NOT NULL CHECK ( claimed_at_unix_micro >= scheduled_at_unix_micro ),
    skipped_occurrence_count BIGINT NOT NULL DEFAULT 0 CHECK ( skipped_occurrence_count BETWEEN 0 AND 4294967295 ),
    outcome TEXT NOT NULL COLLATE "C" CHECK ( outcome IN ( 'claimed' , 'submitted' , 'succeeded' , 'failed' , 'canceled' , 'expired' , 'interrupted' , 'skipped_overlap' ) ),
    search_job_id TEXT COLLATE "C",
    failure_category TEXT COLLATE "C",
    finished_at_unix_micro BIGINT,
    CHECK ( octet_length ( run_id ) BETWEEN 1 AND 128 ),
    CHECK ( octet_length ( owner_id ) BETWEEN 1 AND 255 ),
    CHECK ( octet_length ( tenant_id ) BETWEEN 1 AND 255 ),
    CHECK ( search_job_id IS NULL OR octet_length ( search_job_id ) BETWEEN 1 AND 256 ),
    CHECK ( failure_category IS NULL OR octet_length ( failure_category ) BETWEEN 1 AND 64 ),
    CHECK ( finished_at_unix_micro IS NULL OR finished_at_unix_micro >= claimed_at_unix_micro )
);

CREATE TABLE alerts (
    alert_id TEXT PRIMARY KEY NOT NULL COLLATE "C",
    version BIGINT NOT NULL CHECK ( version >= 1 ),
    tenant_id TEXT NOT NULL COLLATE "C",
    owner_id TEXT NOT NULL COLLATE "C",
    client_request_id TEXT COLLATE "C",
    create_request_sha256 BYTEA,
    app_id TEXT NOT NULL COLLATE "C",
    name TEXT NOT NULL COLLATE "C",
    enabled BIGINT NOT NULL DEFAULT 0 CHECK ( enabled IN ( 0 , 1 ) ),
    definition_proto BYTEA NOT NULL CHECK ( length ( definition_proto ) BETWEEN 1 AND 524288 ),
    endpoint_ciphertext BYTEA NOT NULL CHECK ( length ( endpoint_ciphertext ) BETWEEN 17 AND 16384 ),
    endpoint_nonce BYTEA NOT NULL CHECK ( length ( endpoint_nonce ) = 12 ),
    endpoint_generation BIGINT NOT NULL DEFAULT 1 CHECK ( endpoint_generation >= 1 ),
    webhook_hostname TEXT NOT NULL COLLATE "C",
    secret_generation BIGINT NOT NULL DEFAULT 1 CHECK ( secret_generation >= 1 ),
    secret_ciphertext BYTEA NOT NULL CHECK ( length ( secret_ciphertext ) = 48 ),
    secret_nonce BYTEA NOT NULL CHECK ( length ( secret_nonce ) = 12 ),
    secret_rotated_at_unix_micro BIGINT NOT NULL CHECK ( secret_rotated_at_unix_micro > 0 ),
    next_run_at_unix_micro BIGINT,
    last_claimed_at_unix_micro BIGINT,
    last_outcome BIGINT CHECK ( last_outcome IS NULL OR last_outcome BETWEEN 1 AND 12 ),
    last_outcome_scheduled_at_unix_micro BIGINT,
    last_evaluated_at_unix_micro BIGINT,
    last_delivered_at_unix_micro BIGINT,
    created_at_unix_micro BIGINT NOT NULL CHECK ( created_at_unix_micro > 0 ),
    updated_at_unix_micro BIGINT NOT NULL CHECK ( updated_at_unix_micro >= created_at_unix_micro ),
    CHECK ( octet_length ( alert_id ) BETWEEN 1 AND 128 ),
    CHECK ( octet_length ( tenant_id ) BETWEEN 1 AND 1024 ),
    CHECK ( octet_length ( owner_id ) BETWEEN 1 AND 255 ),
    CHECK ( ( client_request_id IS NULL AND create_request_sha256 IS NULL ) OR ( octet_length ( client_request_id ) BETWEEN 16 AND 128 AND TRUE AND length ( create_request_sha256 ) = 32 ) ),
    CHECK ( octet_length ( app_id ) BETWEEN 1 AND 255 ),
    CHECK ( octet_length ( name ) BETWEEN 1 AND 255 ),
    CHECK ( octet_length ( webhook_hostname ) BETWEEN 1 AND 253 ),
    CHECK ( next_run_at_unix_micro IS NULL OR next_run_at_unix_micro > 0 ),
    CHECK ( last_claimed_at_unix_micro IS NULL OR last_claimed_at_unix_micro > 0 ),
    CHECK ( ( last_outcome IS NULL AND last_outcome_scheduled_at_unix_micro IS NULL ) OR ( last_outcome IS NOT NULL AND last_outcome_scheduled_at_unix_micro > 0 ) ),
    CHECK ( last_evaluated_at_unix_micro IS NULL OR last_evaluated_at_unix_micro > 0 ),
    CHECK ( last_delivered_at_unix_micro IS NULL OR last_delivered_at_unix_micro > 0 ),
    UNIQUE ( tenant_id , owner_id , app_id , name ),
    UNIQUE ( tenant_id , owner_id , client_request_id )
);

CREATE TABLE alert_runs (
    alert_run_id TEXT PRIMARY KEY NOT NULL COLLATE "C",
    alert_id TEXT NOT NULL COLLATE "C" REFERENCES alerts ( alert_id ) ON UPDATE RESTRICT ON DELETE CASCADE,
    alert_version BIGINT NOT NULL CHECK ( alert_version >= 1 ),
    secret_generation BIGINT NOT NULL CHECK ( secret_generation >= 1 ),
    scheduled_at_unix_micro BIGINT NOT NULL CHECK ( scheduled_at_unix_micro > 0 ),
    started_at_unix_micro BIGINT,
    finished_at_unix_micro BIGINT,
    outcome BIGINT NOT NULL CHECK ( outcome BETWEEN 1 AND 12 ),
    missed_occurrence_count BIGINT NOT NULL DEFAULT 0 CHECK ( missed_occurrence_count BETWEEN 0 AND 4294967295 ),
    search_job_id TEXT COLLATE "C",
    search_job_expires_at_unix_micro BIGINT,
    delivery_id TEXT COLLATE "C",
    delivery_authorized_at_unix_micro BIGINT,
    delivery_attempted_at_unix_micro BIGINT,
    delivery_status_code BIGINT CHECK ( delivery_status_code IS NULL OR delivery_status_code BETWEEN 100 AND 599 ),
    evaluation BIGINT NOT NULL DEFAULT 0 CHECK ( evaluation BETWEEN 0 AND 3 ),
    result_count BIGINT CHECK ( result_count IS NULL OR result_count >= 0 ),
    result_count_exact BIGINT CHECK ( result_count_exact IS NULL OR result_count_exact IN ( 0 , 1 ) ),
    failure_category TEXT COLLATE "C",
    snapshot_proto BYTEA NOT NULL CHECK ( length ( snapshot_proto ) BETWEEN 1 AND 524288 ),
    CHECK ( octet_length ( alert_run_id ) BETWEEN 1 AND 128 ),
    CHECK ( started_at_unix_micro IS NULL OR started_at_unix_micro >= scheduled_at_unix_micro ),
    CHECK ( finished_at_unix_micro IS NULL OR finished_at_unix_micro >= scheduled_at_unix_micro ),
    CHECK ( search_job_id IS NULL OR octet_length ( search_job_id ) BETWEEN 1 AND 256 ),
    CHECK ( delivery_id IS NULL OR octet_length ( delivery_id ) BETWEEN 1 AND 128 ),
    CHECK ( delivery_authorized_at_unix_micro IS NULL OR delivery_authorized_at_unix_micro > 0 ),
    CHECK ( delivery_attempted_at_unix_micro IS NULL OR delivery_attempted_at_unix_micro > 0 ),
    CHECK ( failure_category IS NULL OR octet_length ( failure_category ) BETWEEN 1 AND 128 )
);

CREATE TABLE feature_operation_audit_tenant_state (
    tenant_id TEXT PRIMARY KEY NOT NULL COLLATE "C",
    first_sequence BIGINT NOT NULL CHECK ( first_sequence BETWEEN 1 AND 9223372036854775807 ),
    next_sequence BIGINT NOT NULL CHECK ( next_sequence BETWEEN 1 AND 9223372036854775807 ),
    retained_count BIGINT NOT NULL CHECK ( retained_count BETWEEN 0 AND 100001 ),
    last_occurred_at_unix_micro BIGINT CHECK ( last_occurred_at_unix_micro IS NULL OR last_occurred_at_unix_micro BETWEEN 1 AND 253402300799999999 ),
    CONSTRAINT feature_operation_audit_state_dense CHECK ( next_sequence - first_sequence = retained_count ),
    CONSTRAINT feature_operation_audit_state_timestamp_shape CHECK ( ( retained_count = 0 AND last_occurred_at_unix_micro IS NULL ) OR ( retained_count > 0 AND last_occurred_at_unix_micro IS NOT NULL ) ),
    CONSTRAINT feature_operation_audit_state_tenant_id_bounded CHECK ( octet_length ( tenant_id ) BETWEEN 1 AND 255 AND position ( decode ( '00' , 'hex' ) IN convert_to ( tenant_id , 'UTF8' ) ) = 0 AND tenant_id = trim ( tenant_id ) AND tenant_id !~ '^.*[--].*$' )
);

CREATE TABLE feature_operation_audit_events (
    tenant_id TEXT NOT NULL COLLATE "C",
    sequence BIGINT NOT NULL CHECK ( sequence BETWEEN 1 AND 9223372036854775806 ),
    occurred_at_unix_micro BIGINT NOT NULL CHECK ( occurred_at_unix_micro BETWEEN 1 AND 253402300799999999 ),
    feature BIGINT NOT NULL CHECK ( feature BETWEEN 1 AND 3 ),
    operation BIGINT NOT NULL CHECK ( operation BETWEEN 1 AND 18 ),
    outcome BIGINT NOT NULL CHECK ( outcome BETWEEN 1 AND 14 ),
    items BIGINT NOT NULL CHECK ( items BETWEEN 0 AND 9223372036854775807 ),
    bytes BIGINT NOT NULL CHECK ( bytes BETWEEN 0 AND 9223372036854775807 ),
    PRIMARY KEY ( tenant_id , sequence )
);

CREATE TABLE ingest_write_groups (
    write_group_id TEXT PRIMARY KEY NOT NULL COLLATE "C",
    state TEXT NOT NULL CHECK ( state IN ( 'ready' , 'ambiguous' , 'committed' ) ),
    attempt_id TEXT NOT NULL DEFAULT '' COLLATE "C",
    member_count BIGINT NOT NULL CHECK ( member_count BETWEEN 1 AND 10000 ),
    row_count BIGINT NOT NULL CHECK ( row_count BETWEEN 1 AND 50000 ),
    decoded_bytes BIGINT NOT NULL CHECK ( decoded_bytes BETWEEN 1 AND 67108864 ),
    membership_sha256 BYTEA NOT NULL CHECK ( length ( membership_sha256 ) = 32 ),
    first_sequence BIGINT NOT NULL CHECK ( first_sequence >= 1 ),
    last_sequence BIGINT NOT NULL CHECK ( last_sequence >= first_sequence ),
    created_at_unix_micro BIGINT NOT NULL,
    sending_at_unix_micro BIGINT,
    committed_at_unix_micro BIGINT,
    CHECK ( length ( write_group_id ) BETWEEN 1 AND 64 ),
    CHECK ( length ( attempt_id ) <= 128 ),
    CHECK ( created_at_unix_micro BETWEEN 1 AND 253402300799999999 ),
    CHECK ( sending_at_unix_micro IS NULL OR sending_at_unix_micro BETWEEN created_at_unix_micro AND 253402300799999999 ),
    CHECK ( committed_at_unix_micro IS NULL OR committed_at_unix_micro BETWEEN 1 AND 253402300799999999 ),
    CHECK ( ( state = 'ready' AND sending_at_unix_micro IS NULL AND committed_at_unix_micro IS NULL ) OR ( state = 'ambiguous' AND sending_at_unix_micro IS NOT NULL AND committed_at_unix_micro IS NULL ) OR ( state = 'committed' AND attempt_id = '' AND sending_at_unix_micro IS NOT NULL AND committed_at_unix_micro IS NOT NULL AND committed_at_unix_micro >= sending_at_unix_micro ) )
);

CREATE TABLE ingest_write_group_members (
    write_group_id TEXT NOT NULL COLLATE "C",
    ordinal BIGINT NOT NULL CHECK ( ordinal BETWEEN 0 AND 9999 ),
    visibility_sequence BIGINT NOT NULL UNIQUE CHECK ( visibility_sequence >= 1 ),
    row_count BIGINT NOT NULL CHECK ( row_count BETWEEN 1 AND 1000 ),
    decoded_bytes BIGINT NOT NULL CHECK ( decoded_bytes BETWEEN 1 AND 8388608 ),
    outbox_sha256 BYTEA NOT NULL CHECK ( length ( outbox_sha256 ) = 32 ),
    PRIMARY KEY ( write_group_id , ordinal )
);

CREATE TABLE server_appearance_settings (
    singleton_id BIGINT PRIMARY KEY NOT NULL CHECK ( singleton_id = 1 ),
    version BIGINT NOT NULL CHECK ( version BETWEEN 1 AND 9223372036854775807 ),
    palette TEXT NOT NULL COLLATE "C" CHECK ( palette IN ( 'classic' , 'ocean' , 'ember' , 'graphite' , 'glass' , 'terminal' ) ),
    updated_at_unix_micro BIGINT NOT NULL CHECK ( updated_at_unix_micro BETWEEN 1 AND 253402300799999999 )
);

CREATE TABLE ingest_rejection_buckets (
    tenant_id TEXT NOT NULL COLLATE "C" CHECK ( octet_length ( tenant_id ) BETWEEN 1 AND 255 AND position ( decode ( '00' , 'hex' ) IN convert_to ( tenant_id , 'UTF8' ) ) = 0 ),
    token_id TEXT NOT NULL COLLATE "C" CHECK ( octet_length ( token_id ) BETWEEN 1 AND 255 AND position ( decode ( '00' , 'hex' ) IN convert_to ( token_id , 'UTF8' ) ) = 0 ),
    max_rejections_per_second BIGINT NOT NULL CHECK ( max_rejections_per_second BETWEEN 1 AND 10 ),
    max_metadata_bytes_per_second BIGINT NOT NULL CHECK ( max_metadata_bytes_per_second BETWEEN 1 AND 262144 ),
    next_rejection_unix_nano BIGINT NOT NULL CHECK ( next_rejection_unix_nano > 0 ),
    next_metadata_unix_nano BIGINT NOT NULL CHECK ( next_metadata_unix_nano > 0 ),
    updated_at_unix_micro BIGINT NOT NULL CHECK ( updated_at_unix_micro BETWEEN 1 AND 253402300799999999 ),
    PRIMARY KEY ( tenant_id , token_id )
);

CREATE TABLE audit_events (
    tenant_id TEXT NOT NULL COLLATE "C",
    sequence BIGINT NOT NULL CHECK ( sequence BETWEEN 1 AND 100000 ),
    occurred_at_unix_micro BIGINT NOT NULL CHECK ( occurred_at_unix_micro BETWEEN 1 AND 253402300799999999 ),
    actor_kind TEXT NOT NULL COLLATE "C" CHECK ( actor_kind IN ( 'system' , 'browser' ) ),
    actor_id TEXT NOT NULL COLLATE "C",
    actor_role TEXT NOT NULL COLLATE "C" CHECK ( actor_role IN ( 'system' , 'user' , 'administrator' ) ),
    action TEXT NOT NULL COLLATE "C" CHECK ( action IN ( 'ingestion_token.create' , 'ingestion_token.update' , 'ingestion_token.revoke' , 'index.create' , 'index.update' , 'index.activate' , 'index.archive' , 'index.delete_keep_data' , 'index.delete_data' , 'app.create' , 'app.update' , 'app.activate' , 'app.archive' , 'app.delete' , 'saved_search.create' , 'saved_search.update' , 'saved_search.duplicate' , 'saved_search.delete' , 'knowledge.object.create' , 'knowledge.object.update' , 'knowledge.object.scope_change' , 'knowledge.object.enable' , 'knowledge.object.disable' , 'knowledge.object.delete' , 'server_settings.update' , 'lookup.create' , 'lookup.replace' , 'lookup.enable' , 'lookup.disable' , 'lookup.delete' , 'export.create' ) ),
    target_kind TEXT NOT NULL COLLATE "C" CHECK ( target_kind IN ( 'ingestion_token' , 'index' , 'app' , 'saved_search' , 'knowledge_object' , 'server_settings' , 'lookup' , 'export_job' ) ),
    target_id TEXT NOT NULL COLLATE "C",
    target_version BIGINT NOT NULL CHECK ( target_version BETWEEN 1 AND 9223372036854775807 ),
    app_id TEXT COLLATE "C",
    object_type TEXT COLLATE "C" CHECK ( object_type IS NULL OR object_type IN ( 'field_extraction' , 'field_alias' , 'calculated_field' ) ),
    sharing_scope TEXT COLLATE "C" CHECK ( sharing_scope IS NULL OR sharing_scope IN ( 'private' , 'app' , 'global' ) ),
    PRIMARY KEY ( tenant_id , sequence ),
    CONSTRAINT audit_events_actor_id_bounded CHECK ( octet_length ( actor_id ) BETWEEN 1 AND 255 AND position ( decode ( '00' , 'hex' ) IN convert_to ( actor_id , 'UTF8' ) ) = 0 AND actor_id = trim ( actor_id ) AND actor_id !~ '^.*[--].*$' ),
    CONSTRAINT audit_events_actor_shape_supported CHECK ( ( actor_kind = 'system' AND actor_role = 'system' ) OR ( actor_kind = 'browser' AND actor_role = 'administrator' ) OR ( actor_kind = 'browser' AND actor_role = 'user' AND action IN ( 'saved_search.create' , 'saved_search.update' , 'saved_search.duplicate' , 'saved_search.delete' , 'export.create' , 'index.update' , 'index.activate' , 'index.archive' , 'index.delete_keep_data' , 'index.delete_data' ) ) ),
    CONSTRAINT audit_events_action_version_supported CHECK ( ( action IN ( 'ingestion_token.create' , 'index.create' , 'app.create' , 'saved_search.create' , 'saved_search.duplicate' , 'knowledge.object.create' , 'lookup.create' , 'export.create' ) AND target_version = 1 ) OR ( action IN ( 'ingestion_token.update' , 'ingestion_token.revoke' , 'index.update' , 'index.activate' , 'index.archive' , 'index.delete_keep_data' , 'app.update' , 'app.activate' , 'app.archive' , 'app.delete' , 'saved_search.update' , 'knowledge.object.update' , 'knowledge.object.scope_change' , 'knowledge.object.enable' , 'knowledge.object.disable' , 'knowledge.object.delete' , 'lookup.replace' , 'lookup.enable' , 'lookup.disable' , 'lookup.delete' ) AND target_version >= 2 ) OR ( action = 'index.delete_data' AND target_version >= 3 ) OR ( action = 'saved_search.delete' AND target_version >= 1 ) OR ( action = 'server_settings.update' AND target_version >= 1 ) ),
    CONSTRAINT audit_events_action_target_supported CHECK ( ( action IN ( 'ingestion_token.create' , 'ingestion_token.update' , 'ingestion_token.revoke' ) AND target_kind = 'ingestion_token' ) OR ( action IN ( 'index.create' , 'index.update' , 'index.activate' , 'index.archive' , 'index.delete_keep_data' , 'index.delete_data' ) AND target_kind = 'index' ) OR ( action IN ( 'app.create' , 'app.update' , 'app.activate' , 'app.archive' , 'app.delete' ) AND target_kind = 'app' ) OR ( action IN ( 'saved_search.create' , 'saved_search.update' , 'saved_search.duplicate' , 'saved_search.delete' ) AND target_kind = 'saved_search' ) OR ( action IN ( 'knowledge.object.create' , 'knowledge.object.update' , 'knowledge.object.scope_change' , 'knowledge.object.enable' , 'knowledge.object.disable' , 'knowledge.object.delete' ) AND target_kind = 'knowledge_object' ) OR ( action = 'server_settings.update' AND target_kind = 'server_settings' ) OR ( action IN ( 'lookup.create' , 'lookup.replace' , 'lookup.enable' , 'lookup.disable' , 'lookup.delete' ) AND target_kind = 'lookup' ) OR ( action = 'export.create' AND target_kind = 'export_job' ) ),
    CONSTRAINT audit_events_target_id_bounded CHECK ( octet_length ( target_id ) BETWEEN 1 AND 128 AND position ( decode ( '00' , 'hex' ) IN convert_to ( target_id , 'UTF8' ) ) = 0 AND target_id = trim ( target_id ) AND target_id !~ '^.*[--].*$' ),
    CONSTRAINT audit_events_knowledge_metadata_shape_supported CHECK ( ( target_kind = 'knowledge_object' AND app_id IS NOT NULL AND object_type IS NOT NULL AND sharing_scope IS NOT NULL ) OR ( target_kind <> 'knowledge_object' AND app_id IS NULL AND object_type IS NULL AND sharing_scope IS NULL ) ),
    CONSTRAINT audit_events_knowledge_app_id_bounded CHECK ( app_id IS NULL OR ( octet_length ( app_id ) BETWEEN 1 AND 128 AND position ( decode ( '00' , 'hex' ) IN convert_to ( app_id , 'UTF8' ) ) = 0 AND app_id = trim ( app_id ) AND app_id !~ '^.*[--].*$' ) )
);

CREATE TABLE api_mutation_receipt_tenant_state (
    tenant_id TEXT PRIMARY KEY NOT NULL COLLATE "C",
    receipt_count BIGINT NOT NULL DEFAULT 0 CHECK ( receipt_count BETWEEN 0 AND 100000 ),
    encoded_bytes BIGINT NOT NULL DEFAULT 0 CHECK ( encoded_bytes BETWEEN 0 AND 67108864 ),
    high_water_unix_micro BIGINT NOT NULL CHECK ( high_water_unix_micro BETWEEN 1 AND 253402300799999999 ),
    CONSTRAINT api_mutation_receipt_state_tenant_id_bounded CHECK ( octet_length ( tenant_id ) BETWEEN 1 AND 255 AND position ( decode ( '00' , 'hex' ) IN convert_to ( tenant_id , 'UTF8' ) ) = 0 AND tenant_id = trim ( tenant_id ) AND tenant_id !~ '^.*[--].*$' )
);

CREATE TABLE api_mutation_receipts (
    tenant_id TEXT NOT NULL COLLATE "C",
    actor_kind TEXT NOT NULL COLLATE "C" CHECK ( actor_kind IN ( 'system' , 'browser' , 'public' ) ),
    actor_id TEXT NOT NULL COLLATE "C",
    route TEXT NOT NULL COLLATE "C" CHECK ( route IN ( '/api/apps/create' , '/api/indexes/create' , '/api/ingestion-tokens/create' , '/api/search/jobs/create' , '/api/search/exports/create' , '/api/saved-searches/create' , '/api/saved-searches/duplicate' , '/api/knowledge/lookups/create' ) ),
    client_request_id TEXT NOT NULL COLLATE "C",
    canonical_version BIGINT NOT NULL CHECK ( canonical_version BETWEEN 1 AND 65535 ),
    request_sha256 BYTEA NOT NULL CHECK ( length ( request_sha256 ) = 32 ),
    target_kind TEXT NOT NULL COLLATE "C" CHECK ( target_kind IN ( 'app' , 'index' , 'ingestion_token' , 'search_job' , 'export_job' , 'saved_search' , 'lookup' ) ),
    target_id TEXT NOT NULL COLLATE "C",
    target_version BIGINT NOT NULL CHECK ( target_version BETWEEN 1 AND 9223372036854775807 ),
    audit_sequence BIGINT CHECK ( audit_sequence IS NULL OR audit_sequence BETWEEN 1 AND 100000 ),
    created_at_unix_micro BIGINT NOT NULL CHECK ( created_at_unix_micro BETWEEN 1 AND 253402300799999999 ),
    retain_until_unix_micro BIGINT NOT NULL CHECK ( retain_until_unix_micro BETWEEN 1 AND 253402300799999999 AND retain_until_unix_micro >= created_at_unix_micro ),
    target_terminal_at_unix_micro BIGINT CHECK ( target_terminal_at_unix_micro IS NULL OR target_terminal_at_unix_micro BETWEEN created_at_unix_micro AND 253402300799999999 ),
    encoded_bytes BIGINT NOT NULL CHECK ( encoded_bytes BETWEEN 1 AND 67108864 ),
    PRIMARY KEY ( tenant_id , actor_kind , actor_id , route , client_request_id ),
    CONSTRAINT api_mutation_receipt_public_actor_route CHECK ( actor_kind <> 'public' OR route IN ( '/api/search/jobs/create' , '/api/search/exports/create' , '/api/saved-searches/create' , '/api/saved-searches/duplicate' ) ),
    CONSTRAINT api_mutation_receipt_actor_id_bounded CHECK ( octet_length ( actor_id ) BETWEEN 1 AND 255 AND position ( decode ( '00' , 'hex' ) IN convert_to ( actor_id , 'UTF8' ) ) = 0 AND actor_id = trim ( actor_id ) AND actor_id !~ '^.*[--].*$' ),
    CONSTRAINT api_mutation_receipt_request_id_bounded CHECK ( octet_length ( client_request_id ) BETWEEN 16 AND 128 AND client_request_id !~ '^.*[^ -~].*$' ),
    CONSTRAINT api_mutation_receipt_target_id_bounded CHECK ( octet_length ( target_id ) BETWEEN 1 AND 256 AND position ( decode ( '00' , 'hex' ) IN convert_to ( target_id , 'UTF8' ) ) = 0 AND target_id = trim ( target_id ) AND target_id !~ '^.*[--].*$' ),
    CONSTRAINT api_mutation_receipt_route_target_matches CHECK ( ( route = '/api/apps/create' AND target_kind = 'app' ) OR ( route = '/api/indexes/create' AND target_kind = 'index' ) OR ( route = '/api/ingestion-tokens/create' AND target_kind = 'ingestion_token' ) OR ( route = '/api/search/jobs/create' AND target_kind = 'search_job' ) OR ( route = '/api/search/exports/create' AND target_kind = 'export_job' ) OR ( route IN ( '/api/saved-searches/create' , '/api/saved-searches/duplicate' ) AND target_kind = 'saved_search' ) OR ( route = '/api/knowledge/lookups/create' AND target_kind = 'lookup' ) ),
    CONSTRAINT api_mutation_receipt_encoded_size_exact CHECK ( encoded_bytes = 96 + octet_length ( tenant_id ) + octet_length ( actor_kind ) + octet_length ( actor_id ) + octet_length ( route ) + octet_length ( client_request_id ) + length ( request_sha256 ) + octet_length ( target_kind ) + octet_length ( target_id ) )
);

CREATE FUNCTION os_export_private_identity_is_valid(encoded TEXT, export_id TEXT, digest BYTEA, payload BYTEA) RETURNS BOOLEAN LANGUAGE plpgsql IMMUTABLE AS $$
DECLARE identity JSONB; job JSONB; object_key TEXT; etag TEXT; version TEXT; size_value NUMERIC;
BEGIN
 job := convert_from(payload,'UTF8')::jsonb;
 IF jsonb_typeof(job)<>'object' OR COALESCE(job->>'State','') NOT IN ('1','2','3','4','5','6') THEN RETURN FALSE; END IF;
 IF encoded='' THEN RETURN digest IS NULL AND COALESCE(job->>'State','')<>'3'; END IF;
 IF octet_length(encoded)>4096 THEN RETURN FALSE; END IF;
 identity := encoded::jsonb;
 IF jsonb_typeof(identity)<>'object' OR (SELECT count(*) FROM jsonb_object_keys(identity))<>4 OR NOT identity ?& ARRAY['Key','ETag','Version','Bytes'] THEN RETURN FALSE; END IF;
 IF jsonb_typeof(identity->'Key')<>'string' OR jsonb_typeof(identity->'ETag')<>'string' OR jsonb_typeof(identity->'Version')<>'string' OR jsonb_typeof(identity->'Bytes')<>'number' THEN RETURN FALSE; END IF;
 object_key:=identity->>'Key';etag:=identity->>'ETag';version:=identity->>'Version';
 IF position('..' IN object_key)>0 OR octet_length(object_key)>1024 OR left(object_key,length('open-splunk/exports/'||export_id||'/'))<>'open-splunk/exports/'||export_id||'/' OR object_key!~'^open-splunk/exports/[A-Za-z0-9._:-]+/[0-9a-f]{32}$' THEN RETURN FALSE; END IF;
 IF octet_length(etag)>1024 OR octet_length(version)>1024 OR version='null' OR (identity->>'Bytes')!~'^(0|[1-9][0-9]*)$' THEN RETURN FALSE; END IF;
 size_value:=(identity->>'Bytes')::numeric;
 IF size_value>4294967296 OR (digest IS NOT NULL AND octet_length(digest)<>32) THEN RETURN FALSE; END IF;
 IF version='!conflict' THEN RETURN etag='' AND size_value=0 AND digest IS NULL AND COALESCE(job->>'State','') IN ('1','2','4','5','6'); END IF;
 IF etag='' OR version='' THEN
   RETURN etag='' AND version='' AND size_value=0 AND digest IS NULL AND COALESCE(job->>'State','') IN ('1','2','4','5','6');
 END IF;
 IF job->>'State'='3' THEN
   RETURN COALESCE(digest IS NOT NULL AND octet_length(digest)=32 AND jsonb_typeof(job->'Artifact')='object' AND (job->'Artifact'->>'SizeBytes') ~ '^(0|[1-9][0-9]*)$' AND (job->'Artifact'->>'SizeBytes')::numeric=size_value,FALSE);
 END IF;
 RETURN TRUE;
EXCEPTION WHEN invalid_text_representation OR character_not_in_repertoire OR numeric_value_out_of_range OR invalid_parameter_value THEN RETURN FALSE;
END;
$$;

CREATE TABLE durable_export_jobs (
 organization_id BIGINT NOT NULL CHECK(organization_id>0),
    tenant_id TEXT NOT NULL COLLATE "C",
    owner_id TEXT NOT NULL COLLATE "C",
    export_id TEXT NOT NULL COLLATE "C",
    version BIGINT NOT NULL CHECK ( version BETWEEN 1 AND 9223372036854775807 ),
    job_json BYTEA NOT NULL CHECK ( length ( job_json ) BETWEEN 2 AND 2097152 ),
    created_at_us BIGINT NOT NULL CHECK ( created_at_us BETWEEN 1 AND 253402300799999999 ),
    updated_at_us BIGINT NOT NULL CHECK ( updated_at_us BETWEEN created_at_us AND 253402300799999999 ),
    expires_at_us BIGINT CHECK ( expires_at_us IS NULL OR expires_at_us BETWEEN created_at_us AND 253402300799999999 ),
    metadata_bytes BIGINT NOT NULL CHECK ( metadata_bytes BETWEEN 1 AND 4294967296 ),
    artifact_name TEXT NOT NULL COLLATE "C" DEFAULT '',
    artifact_sha256 BYTEA,
    PRIMARY KEY ( tenant_id , export_id ),
    UNIQUE ( export_id ),
    CONSTRAINT durable_export_job_tenant_id_bounded CHECK ( octet_length ( tenant_id ) BETWEEN 1 AND 1024 AND position ( decode ( '00' , 'hex' ) IN convert_to ( tenant_id , 'UTF8' ) ) = 0 ),
    CONSTRAINT durable_export_job_owner_id_bounded CHECK ( octet_length ( owner_id ) BETWEEN 1 AND 1024 AND position ( decode ( '00' , 'hex' ) IN convert_to ( owner_id , 'UTF8' ) ) = 0 ),
    CONSTRAINT durable_export_job_id_bounded CHECK ( octet_length ( export_id ) BETWEEN 1 AND 128 AND position ( decode ( '00' , 'hex' ) IN convert_to ( export_id , 'UTF8' ) ) = 0 ),
    CONSTRAINT durable_export_job_artifact_shape_is_exact CHECK (os_export_private_identity_is_valid(artifact_name,export_id,artifact_sha256,job_json))
);

CREATE INDEX indexes_state_name_idx ON indexes ( state , name );

CREATE INDEX ingestion_token_indexes_index_idx ON ingestion_token_indexes ( index_id , ingestion_token_id );

CREATE INDEX ingest_visibility_reservations_state_sequence_idx ON ingest_visibility_reservations ( state , sequence );

CREATE INDEX ingest_visibility_reservations_batch_sequence_idx ON ingest_visibility_reservations ( batch_key , sequence DESC );

CREATE UNIQUE INDEX ingest_visibility_reservations_active_batch_idx ON ingest_visibility_reservations ( batch_key ) WHERE state IN ( 'reserved' , 'committed' , 'rejected' );

CREATE UNIQUE INDEX ingest_visibility_reservations_attempt_idx ON ingest_visibility_reservations ( attempt_id ) WHERE attempt_id <> '';

CREATE INDEX saved_searches_app_name_id_idx ON saved_searches ( organization_id , app_id , name , saved_search_id );

CREATE INDEX saved_searches_updated_idx ON saved_searches ( organization_id , updated_at_unix_micro DESC , saved_search_id );

CREATE INDEX saved_searches_owner_name_id_idx ON saved_searches ( organization_id , owner_id , name , saved_search_id );

CREATE INDEX saved_searches_owner_created_id_idx ON saved_searches ( organization_id , owner_id , created_at_unix_micro , saved_search_id );

CREATE INDEX saved_searches_owner_updated_id_idx ON saved_searches ( organization_id , owner_id , updated_at_unix_micro , saved_search_id );

CREATE INDEX search_history_owner_created_idx ON search_history ( tenant_id , owner_id , created_at_unix_micro DESC , search_job_id DESC );

CREATE INDEX search_history_owner_finished_idx ON search_history ( tenant_id , owner_id , finished_at_unix_micro DESC , search_job_id DESC );

CREATE INDEX search_history_owner_duration_idx ON search_history ( tenant_id , owner_id , duration_nanoseconds DESC , search_job_id DESC );

CREATE INDEX search_history_owner_matched_idx ON search_history ( tenant_id , owner_id , matched_events DESC , search_job_id DESC );

CREATE INDEX search_history_owner_app_created_idx ON search_history ( tenant_id , owner_id , app_id , created_at_unix_micro DESC , search_job_id DESC );

CREATE INDEX search_history_owner_saved_created_idx ON search_history ( tenant_id , owner_id , saved_search_id , created_at_unix_micro DESC , search_job_id DESC );

CREATE INDEX search_history_pending_owner_created_idx ON search_history_pending ( tenant_id , owner_id , created_at_unix_micro , search_job_id );

CREATE INDEX app_workspaces_tenant_display_id_idx ON app_workspaces ( organization_id , tenant_id , display_name , app_id );

CREATE INDEX app_workspaces_tenant_created_id_idx ON app_workspaces ( organization_id , tenant_id , created_at_unix_micro , app_id );

CREATE INDEX app_workspaces_tenant_updated_id_idx ON app_workspaces ( organization_id , tenant_id , updated_at_unix_micro , app_id );

CREATE INDEX app_default_indexes_index_app_idx ON app_default_indexes ( index_id , tenant_id , app_id );

CREATE INDEX collector_fleet_tenant_state_id_idx ON collector_fleet ( tenant_id , administrative_state , collector_id );

CREATE INDEX collector_fleet_tenant_display_id_idx ON collector_fleet ( tenant_id , display_name_sort_key , collector_id );

CREATE INDEX collector_runtime_tenant_hostname_id_idx ON collector_runtime ( tenant_id , hostname , collector_id );

CREATE INDEX collector_runtime_tenant_last_seen_id_idx ON collector_runtime ( tenant_id , last_seen_at_unix_micro , collector_id );

CREATE INDEX collector_runtime_tenant_queued_bytes_id_idx ON collector_runtime ( tenant_id , queued_bytes , collector_id );

CREATE INDEX collector_authorized_indexes_tenant_index_name_collector_idx ON collector_authorized_indexes ( tenant_id , index_name , collector_id );

CREATE INDEX ingestion_tokens_revoked_retention_idx ON ingestion_tokens ( revoked_at_unix_micro DESC , ingestion_token_id DESC ) WHERE state = 'revoked';

CREATE INDEX search_history_created_idx ON search_history ( created_at_unix_micro , search_job_id );

CREATE INDEX index_deletion_operations_created_id_idx ON index_deletion_operations ( created_at_unix_micro , deletion_operation_id );

CREATE INDEX indexes_name_id_idx ON indexes ( name , index_id );

CREATE INDEX indexes_created_id_idx ON indexes ( created_at_unix_micro , index_id );

CREATE INDEX indexes_updated_id_idx ON indexes ( updated_at_unix_micro , index_id );

CREATE INDEX ingest_quota_buckets_token_owner_idx ON ingest_quota_buckets ( token_owner_id ) WHERE token_owner_id IS NOT NULL;

CREATE INDEX search_attempt_audit_tenant_actor_sequence_idx ON search_attempt_audit_events ( tenant_id , actor_id , sequence DESC );

CREATE INDEX search_attempt_audit_tenant_owner_sequence_idx ON search_attempt_audit_events ( tenant_id , owner_id , sequence DESC );

CREATE INDEX knowledge_object_dependencies_target_idx ON knowledge_object_dependencies ( tenant_id , target_kind , target_object_id , target_object_version , source_object_id , source_object_version );

CREATE INDEX knowledge_object_acl_role_object_idx ON knowledge_object_acl ( tenant_id , role_id , knowledge_object_id );

CREATE INDEX knowledge_recovery_audit_occurred_idx ON knowledge_recovery_audit ( tenant_id , occurred_at_unix_micro DESC , sequence DESC );

CREATE UNIQUE INDEX knowledge_objects_active_private_name_idx ON knowledge_objects ( tenant_id , app_id , owner_id , object_type , name ) WHERE state = 'active' AND sharing_scope = 'private';

CREATE UNIQUE INDEX knowledge_objects_active_app_name_idx ON knowledge_objects ( tenant_id , app_id , object_type , name ) WHERE state = 'active' AND sharing_scope = 'app';

CREATE UNIQUE INDEX knowledge_objects_active_global_name_idx ON knowledge_objects ( tenant_id , organization_id , object_type , name ) WHERE state = 'active' AND sharing_scope = 'global';

CREATE INDEX knowledge_objects_resolution_idx ON knowledge_objects ( organization_id , tenant_id , state , sharing_scope , app_id , owner_id , object_type , name , knowledge_object_id );

CREATE INDEX knowledge_objects_list_updated_idx ON knowledge_objects ( organization_id , tenant_id , updated_at_unix_micro DESC , knowledge_object_id );

CREATE UNIQUE INDEX knowledge_objects_current_projection_identity_idx ON knowledge_objects ( tenant_id , knowledge_object_id , current_version , app_id , owner_id , object_type , name , sharing_scope , state );

CREATE INDEX knowledge_list_projection_filter_idx ON knowledge_object_list_projections ( tenant_id , state , object_type , sharing_scope , app_id , owner_id , name COLLATE "C" , knowledge_object_id , object_version );

CREATE INDEX knowledge_list_projection_name_idx ON knowledge_object_list_projections ( tenant_id , name COLLATE "C" , knowledge_object_id , object_version );

CREATE INDEX knowledge_list_projection_description_idx ON knowledge_object_list_projections ( tenant_id , description COLLATE "C" , knowledge_object_id , object_version ) WHERE description_present = 1;

CREATE INDEX knowledge_list_selector_value_idx ON knowledge_object_list_selector_patterns ( tenant_id , dimension , match_kind , value COLLATE "C" , knowledge_object_id , object_version , ordinal );

CREATE INDEX knowledge_attempt_audit_tenant_actor_sequence_idx ON knowledge_attempt_audit_events ( tenant_id , actor_id , sequence DESC );

CREATE INDEX knowledge_attempt_audit_tenant_reason_sequence_idx ON knowledge_attempt_audit_events ( tenant_id , reason , sequence DESC );

CREATE INDEX knowledge_list_order_created_idx ON knowledge_object_list_order_keys ( tenant_id , created_at_unix_micro , knowledge_object_id , object_version );

CREATE INDEX knowledge_list_order_updated_idx ON knowledge_object_list_order_keys ( tenant_id , updated_at_unix_micro , knowledge_object_id , object_version );

CREATE INDEX knowledge_list_projection_object_type_order_idx ON knowledge_object_list_projections ( tenant_id , object_type , name COLLATE "C" , knowledge_object_id , object_version );

CREATE INDEX knowledge_objects_authorized_global_idx ON knowledge_objects ( organization_id , tenant_id , knowledge_object_id ) WHERE sharing_scope = 'global';

CREATE INDEX knowledge_objects_authorized_app_idx ON knowledge_objects ( organization_id , tenant_id , app_id , knowledge_object_id ) WHERE sharing_scope = 'app';

CREATE INDEX knowledge_objects_authorized_private_idx ON knowledge_objects ( organization_id , tenant_id , owner_id , app_id , knowledge_object_id ) WHERE sharing_scope = 'private';

CREATE INDEX knowledge_list_projection_authorized_global_idx ON knowledge_object_list_projections ( tenant_id , knowledge_object_id , object_version ) WHERE sharing_scope = 'global';

CREATE INDEX knowledge_list_projection_authorized_app_idx ON knowledge_object_list_projections ( tenant_id , app_id , knowledge_object_id , object_version ) WHERE sharing_scope = 'app';

CREATE INDEX knowledge_list_projection_authorized_private_idx ON knowledge_object_list_projections ( tenant_id , owner_id , app_id , knowledge_object_id , object_version ) WHERE sharing_scope = 'private';

CREATE INDEX knowledge_mutation_idempotency_retention_idx ON knowledge_mutation_idempotency ( tenant_id , retain_until_unix_micro , created_at_unix_micro , actor_kind , actor_id , route , client_request_id , retention_anchor_unix_micro );

CREATE INDEX knowledge_object_dependencies_source_target_idx ON knowledge_object_dependencies ( tenant_id , source_object_id , source_object_version , target_kind , target_object_id , target_object_version );

CREATE INDEX knowledge_catalog_tenants_nonempty_active_idx ON knowledge_catalog_tenants ( tenant_id , catalog_revision , active_object_count ) WHERE active_object_count > 0;

CREATE INDEX knowledge_objects_active_tenant_idx ON knowledge_objects ( tenant_id , knowledge_object_id , current_version ) WHERE state = 'active';

CREATE INDEX hec_requests_terminal_retention_idx ON hec_requests ( state , terminal_at_unix_micro , tenant_id , ingestion_token_id , request_sequence ) WHERE state IN ( 'indexed' , 'terminal_failure' );

CREATE INDEX hec_channels_token_activity_idx ON hec_channels ( tenant_id , ingestion_token_id , last_used_at_unix_micro , channel_id );

CREATE INDEX hec_acknowledgments_bounded_lookup_idx ON hec_acknowledgments ( tenant_id , ingestion_token_id , channel_id , acknowledgment_id , request_sequence );

CREATE INDEX knowledge_lookup_asset_stage_expiry_idx ON knowledge_lookup_asset_stages ( expires_at_unix_micro , tenant_id , stage_id );

CREATE INDEX knowledge_lookup_asset_version_digest_idx ON knowledge_lookup_asset_versions ( tenant_id , content_sha256 , lookup_asset_id , asset_version );

CREATE INDEX knowledge_lookup_definitions_resolution ON knowledge_lookup_definitions ( tenant_id , state , name , sharing_scope , app_id , owner_id , automatic , lookup_id );

CREATE UNIQUE INDEX knowledge_lookup_definitions_private_name ON knowledge_lookup_definitions ( tenant_id , owner_id , app_id , name ) WHERE sharing_scope = 1 AND state != 'DELETED';

CREATE UNIQUE INDEX knowledge_lookup_definitions_app_name ON knowledge_lookup_definitions ( tenant_id , app_id , name ) WHERE sharing_scope = 2 AND state != 'DELETED';

CREATE UNIQUE INDEX knowledge_lookup_definitions_global_name ON knowledge_lookup_definitions ( tenant_id , organization_id , name ) WHERE sharing_scope = 3 AND state != 'DELETED';

CREATE INDEX knowledge_lookup_definition_versions_asset ON knowledge_lookup_definition_versions ( tenant_id , lookup_asset_id , asset_version , lookup_id , definition_version );

CREATE INDEX dashboards_owner_updated ON dashboards ( organization_id , tenant_id , owner_id , updated_at_unix_micro DESC , dashboard_id DESC );

CREATE INDEX durable_search_jobs_owner_created ON durable_search_jobs ( tenant_id , owner_id , created_at_us DESC , id DESC );

CREATE INDEX durable_search_jobs_expiry ON durable_search_jobs ( expires_at_us ) WHERE tombstoned_at_us IS NULL AND expires_at_us IS NOT NULL;

CREATE INDEX durable_search_jobs_tombstone ON durable_search_jobs ( tombstoned_at_us , id ) WHERE state = 8 AND tombstoned_at_us IS NOT NULL;

CREATE INDEX saved_search_schedules_due ON saved_search_schedules ( next_run_at_unix_micro , saved_search_id ) WHERE enabled = TRUE AND next_run_at_unix_micro IS NOT NULL;

CREATE UNIQUE INDEX saved_search_schedule_runs_one_active ON saved_search_schedule_runs ( saved_search_id ) WHERE outcome IN ( 'claimed' , 'submitted' );

CREATE INDEX saved_search_schedule_runs_history ON saved_search_schedule_runs ( saved_search_id , claimed_at_unix_micro DESC , run_id DESC );

CREATE INDEX alerts_owner_name ON alerts ( tenant_id , owner_id , app_id , name , alert_id );

CREATE INDEX alerts_due ON alerts ( next_run_at_unix_micro , alert_id ) WHERE enabled = 1 AND next_run_at_unix_micro IS NOT NULL;

CREATE INDEX alert_runs_history ON alert_runs ( alert_id , scheduled_at_unix_micro DESC , alert_run_id DESC );

CREATE UNIQUE INDEX alert_runs_one_active ON alert_runs ( alert_id ) WHERE outcome = 1;

CREATE INDEX feature_operation_audit_events_occurred ON feature_operation_audit_events ( tenant_id , occurred_at_unix_micro DESC , sequence DESC );

CREATE INDEX ingest_visibility_reservations_group_formation_idx ON ingest_visibility_reservations ( state , phase , attempt_id , sequence );

CREATE INDEX ingest_write_groups_state_sequence_idx ON ingest_write_groups ( state , first_sequence );

CREATE UNIQUE INDEX ingest_write_groups_attempt_idx ON ingest_write_groups ( attempt_id ) WHERE attempt_id <> '';

CREATE INDEX ingest_write_group_members_sequence_idx ON ingest_write_group_members ( visibility_sequence );

CREATE INDEX ingest_visibility_principal_pending_idx ON ingest_visibility_reservations ( principal_sha256 ) WHERE state = 'reserved';

CREATE INDEX ingest_rejection_buckets_token_idx ON ingest_rejection_buckets ( token_id );

CREATE INDEX audit_events_tenant_action_sequence_idx ON audit_events ( tenant_id , action , sequence DESC );

CREATE INDEX audit_events_tenant_actor_sequence_idx ON audit_events ( tenant_id , actor_id , sequence DESC );

CREATE INDEX audit_events_tenant_target_sequence_idx ON audit_events ( tenant_id , target_kind , sequence DESC );

CREATE INDEX api_mutation_receipts_reclaim_idx ON api_mutation_receipts ( tenant_id , retain_until_unix_micro , created_at_unix_micro , actor_kind , actor_id , route , client_request_id );

CREATE INDEX durable_export_jobs_owner_created_idx ON durable_export_jobs ( tenant_id , owner_id , created_at_us DESC , export_id DESC );

CREATE INDEX durable_export_jobs_expiry_idx ON durable_export_jobs ( expires_at_us , tenant_id , export_id ) WHERE expires_at_us IS NOT NULL;

ALTER TABLE ingest_visibility_reservations ADD FOREIGN KEY ( batch_key ) REFERENCES ingest_batch_identities ( batch_key ) ON UPDATE RESTRICT ON DELETE RESTRICT;

ALTER TABLE app_default_indexes ADD FOREIGN KEY ( tenant_id , app_id ) REFERENCES app_workspaces ( tenant_id , app_id ) ON DELETE CASCADE;

ALTER TABLE app_default_indexes ADD FOREIGN KEY ( index_id ) REFERENCES indexes ( index_id ) ON DELETE RESTRICT;

ALTER TABLE collector_runtime ADD FOREIGN KEY ( tenant_id , collector_id ) REFERENCES collector_fleet ( tenant_id , collector_id ) ON DELETE CASCADE;

ALTER TABLE collector_capabilities ADD FOREIGN KEY ( tenant_id , collector_id ) REFERENCES collector_fleet ( tenant_id , collector_id ) ON DELETE CASCADE;

ALTER TABLE collector_authorized_indexes ADD FOREIGN KEY ( tenant_id , collector_id ) REFERENCES collector_fleet ( tenant_id , collector_id ) ON DELETE CASCADE;

ALTER TABLE collector_inputs ADD FOREIGN KEY ( tenant_id , collector_id ) REFERENCES collector_fleet ( tenant_id , collector_id ) ON DELETE CASCADE;

ALTER TABLE collector_inputs ADD FOREIGN KEY ( tenant_id , collector_id , index_name ) REFERENCES collector_authorized_indexes ( tenant_id , collector_id , index_name ) ON DELETE CASCADE;

ALTER TABLE collector_input_health ADD FOREIGN KEY ( tenant_id , collector_id , input_id ) REFERENCES collector_inputs ( tenant_id , collector_id , input_id ) ON DELETE CASCADE;

ALTER TABLE ingest_quota_buckets ADD FOREIGN KEY ( token_owner_id ) REFERENCES ingestion_tokens ( ingestion_token_id ) ON UPDATE RESTRICT ON DELETE CASCADE;

ALTER TABLE ingest_quota_admissions ADD FOREIGN KEY ( batch_key ) REFERENCES ingest_batch_identities ( batch_key ) ON UPDATE RESTRICT ON DELETE CASCADE;

ALTER TABLE search_attempt_audit_events ADD FOREIGN KEY ( tenant_id ) REFERENCES search_attempt_audit_tenant_state ( tenant_id ) ON UPDATE RESTRICT ON DELETE RESTRICT;

ALTER TABLE knowledge_definition_blobs ADD FOREIGN KEY ( tenant_id ) REFERENCES knowledge_catalog_tenants ( tenant_id ) ON UPDATE RESTRICT ON DELETE RESTRICT;

ALTER TABLE knowledge_objects ADD FOREIGN KEY ( tenant_id ) REFERENCES knowledge_catalog_tenants ( tenant_id ) ON UPDATE RESTRICT ON DELETE RESTRICT;

ALTER TABLE knowledge_objects ADD FOREIGN KEY ( tenant_id , app_id ) REFERENCES app_workspaces ( tenant_id , app_id ) ON UPDATE RESTRICT ON DELETE RESTRICT;

ALTER TABLE knowledge_objects ADD FOREIGN KEY ( tenant_id , definition_digest ) REFERENCES knowledge_definition_blobs ( tenant_id , definition_digest ) ON UPDATE RESTRICT ON DELETE RESTRICT;

ALTER TABLE knowledge_objects ADD FOREIGN KEY ( tenant_id , knowledge_object_id , current_version ) REFERENCES knowledge_object_versions ( tenant_id , knowledge_object_id , object_version ) ON UPDATE RESTRICT ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED;

ALTER TABLE knowledge_objects ADD FOREIGN KEY ( tenant_id , knowledge_object_id , current_version , app_id , owner_id , object_type , name , sharing_scope , state , definition_digest_key ) REFERENCES knowledge_object_versions ( tenant_id , knowledge_object_id , object_version , app_id , owner_id , object_type , name , sharing_scope , state , definition_digest_key ) ON UPDATE RESTRICT ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED;

ALTER TABLE knowledge_objects ADD FOREIGN KEY ( tenant_id , knowledge_object_id , current_version ) REFERENCES knowledge_object_dependency_seals ( tenant_id , knowledge_object_id , object_version ) ON UPDATE RESTRICT ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED;

ALTER TABLE knowledge_object_versions ADD FOREIGN KEY ( tenant_id , knowledge_object_id ) REFERENCES knowledge_objects ( tenant_id , knowledge_object_id ) ON UPDATE RESTRICT ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED;

ALTER TABLE knowledge_object_versions ADD FOREIGN KEY ( tenant_id , app_id ) REFERENCES app_workspaces ( tenant_id , app_id ) ON UPDATE RESTRICT ON DELETE RESTRICT;

ALTER TABLE knowledge_object_versions ADD FOREIGN KEY ( tenant_id , definition_digest ) REFERENCES knowledge_definition_blobs ( tenant_id , definition_digest ) ON UPDATE RESTRICT ON DELETE RESTRICT;

ALTER TABLE knowledge_object_versions ADD FOREIGN KEY ( tenant_id , quarantine_object_id , quarantine_object_version ) REFERENCES knowledge_objects ( tenant_id , knowledge_object_id , current_version ) ON UPDATE RESTRICT ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED;

ALTER TABLE knowledge_object_dependencies ADD FOREIGN KEY ( tenant_id , source_object_id , source_object_version ) REFERENCES knowledge_object_versions ( tenant_id , knowledge_object_id , object_version ) ON UPDATE RESTRICT ON DELETE RESTRICT;

ALTER TABLE knowledge_object_dependencies ADD FOREIGN KEY ( tenant_id , target_object_id , target_object_version ) REFERENCES knowledge_object_versions ( tenant_id , knowledge_object_id , object_version ) ON UPDATE RESTRICT ON DELETE RESTRICT;

ALTER TABLE knowledge_object_dependency_seals ADD FOREIGN KEY ( tenant_id , knowledge_object_id , object_version , dependency_count ) REFERENCES knowledge_object_versions ( tenant_id , knowledge_object_id , object_version , dependency_count ) ON UPDATE RESTRICT ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED;

ALTER TABLE knowledge_object_acl ADD FOREIGN KEY ( tenant_id , knowledge_object_id ) REFERENCES knowledge_objects ( tenant_id , knowledge_object_id ) ON UPDATE RESTRICT ON DELETE RESTRICT;

ALTER TABLE knowledge_app_active_counters ADD FOREIGN KEY ( tenant_id ) REFERENCES knowledge_catalog_tenants ( tenant_id ) ON UPDATE RESTRICT ON DELETE RESTRICT;

ALTER TABLE knowledge_app_active_counters ADD FOREIGN KEY ( tenant_id , app_id ) REFERENCES app_workspaces ( tenant_id , app_id ) ON UPDATE RESTRICT ON DELETE RESTRICT;

ALTER TABLE knowledge_owner_active_counters ADD FOREIGN KEY ( tenant_id ) REFERENCES knowledge_catalog_tenants ( tenant_id ) ON UPDATE RESTRICT ON DELETE RESTRICT;

ALTER TABLE knowledge_type_active_counters ADD FOREIGN KEY ( tenant_id ) REFERENCES knowledge_catalog_tenants ( tenant_id ) ON UPDATE RESTRICT ON DELETE RESTRICT;

ALTER TABLE knowledge_app_type_active_counters ADD FOREIGN KEY ( tenant_id ) REFERENCES knowledge_catalog_tenants ( tenant_id ) ON UPDATE RESTRICT ON DELETE RESTRICT;

ALTER TABLE knowledge_app_type_active_counters ADD FOREIGN KEY ( tenant_id , app_id ) REFERENCES app_workspaces ( tenant_id , app_id ) ON UPDATE RESTRICT ON DELETE RESTRICT;

ALTER TABLE knowledge_recovery_audit ADD FOREIGN KEY ( tenant_id ) REFERENCES knowledge_catalog_tenants ( tenant_id ) ON UPDATE RESTRICT ON DELETE RESTRICT;

ALTER TABLE knowledge_recovery_audit ADD FOREIGN KEY ( tenant_id , knowledge_object_id , object_version ) REFERENCES knowledge_object_versions ( tenant_id , knowledge_object_id , object_version ) ON UPDATE RESTRICT ON DELETE RESTRICT;

ALTER TABLE knowledge_projection_tenant_ledgers ADD FOREIGN KEY ( tenant_id ) REFERENCES knowledge_catalog_tenants ( tenant_id ) ON UPDATE RESTRICT ON DELETE RESTRICT;

ALTER TABLE knowledge_object_list_projections ADD FOREIGN KEY ( tenant_id ) REFERENCES knowledge_projection_tenant_ledgers ( tenant_id ) ON UPDATE RESTRICT ON DELETE RESTRICT;

ALTER TABLE knowledge_object_list_projections ADD FOREIGN KEY ( tenant_id , knowledge_object_id , object_version , app_id , owner_id , object_type , name , sharing_scope , state ) REFERENCES knowledge_objects ( tenant_id , knowledge_object_id , current_version , app_id , owner_id , object_type , name , sharing_scope , state ) ON UPDATE NO ACTION ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED;

ALTER TABLE knowledge_object_list_selector_patterns ADD FOREIGN KEY ( tenant_id , knowledge_object_id , object_version ) REFERENCES knowledge_object_list_projections ( tenant_id , knowledge_object_id , object_version ) ON UPDATE RESTRICT ON DELETE RESTRICT;

ALTER TABLE knowledge_object_list_projection_seals ADD FOREIGN KEY ( tenant_id , knowledge_object_id , object_version ) REFERENCES knowledge_object_list_projections ( tenant_id , knowledge_object_id , object_version ) ON UPDATE RESTRICT ON DELETE RESTRICT;

ALTER TABLE knowledge_attempt_audit_events ADD FOREIGN KEY ( tenant_id ) REFERENCES knowledge_attempt_audit_tenant_state ( tenant_id ) ON UPDATE RESTRICT ON DELETE RESTRICT;

ALTER TABLE knowledge_catalog_revision_heads ADD FOREIGN KEY ( tenant_id ) REFERENCES knowledge_catalog_tenants ( tenant_id ) ON UPDATE RESTRICT ON DELETE RESTRICT;

ALTER TABLE knowledge_object_version_lifecycle ADD FOREIGN KEY ( tenant_id , knowledge_object_id , object_version ) REFERENCES knowledge_object_versions ( tenant_id , knowledge_object_id , object_version ) ON UPDATE RESTRICT ON DELETE RESTRICT;

ALTER TABLE knowledge_object_list_order_keys ADD FOREIGN KEY ( tenant_id , knowledge_object_id , object_version ) REFERENCES knowledge_object_list_projections ( tenant_id , knowledge_object_id , object_version ) ON UPDATE RESTRICT ON DELETE CASCADE;

ALTER TABLE knowledge_mutation_commit_authorities ADD FOREIGN KEY ( tenant_id ) REFERENCES knowledge_catalog_tenants ( tenant_id ) ON UPDATE RESTRICT ON DELETE RESTRICT;

ALTER TABLE knowledge_mutation_commit_authorities ADD FOREIGN KEY ( tenant_id , knowledge_object_id , object_version ) REFERENCES knowledge_object_versions ( tenant_id , knowledge_object_id , object_version ) ON UPDATE RESTRICT ON DELETE RESTRICT;

ALTER TABLE knowledge_mutation_commit_authorities ADD FOREIGN KEY ( tenant_id , successful_audit_sequence ) REFERENCES audit_events ( tenant_id , sequence ) ON UPDATE RESTRICT ON DELETE RESTRICT;

ALTER TABLE knowledge_mutation_commit_authorities ADD FOREIGN KEY ( tenant_id , recovery_audit_sequence ) REFERENCES knowledge_recovery_audit ( tenant_id , sequence ) ON UPDATE RESTRICT ON DELETE RESTRICT;

ALTER TABLE knowledge_mutation_idempotency ADD FOREIGN KEY ( tenant_id ) REFERENCES knowledge_catalog_tenants ( tenant_id ) ON UPDATE RESTRICT ON DELETE RESTRICT;

ALTER TABLE knowledge_mutation_idempotency ADD FOREIGN KEY ( tenant_id , knowledge_object_id , object_version ) REFERENCES knowledge_object_versions ( tenant_id , knowledge_object_id , object_version ) ON UPDATE RESTRICT ON DELETE RESTRICT;

ALTER TABLE knowledge_mutation_idempotency ADD FOREIGN KEY ( tenant_id , successful_audit_sequence ) REFERENCES audit_events ( tenant_id , sequence ) ON UPDATE RESTRICT ON DELETE RESTRICT;

ALTER TABLE knowledge_mutation_idempotency ADD FOREIGN KEY ( tenant_id , recovery_audit_sequence ) REFERENCES knowledge_recovery_audit ( tenant_id , sequence ) ON UPDATE RESTRICT ON DELETE RESTRICT;

ALTER TABLE knowledge_mutation_idempotency ADD FOREIGN KEY ( tenant_id , committed_catalog_revision , committed_catalog_state_token , actor_kind , actor_id , route , client_request_id , request_digest ) REFERENCES knowledge_mutation_commit_authorities ( tenant_id , catalog_revision , catalog_state_token , actor_kind , actor_id , route , client_request_id , request_digest ) ON UPDATE RESTRICT ON DELETE RESTRICT;

ALTER TABLE hec_requests ADD FOREIGN KEY ( tenant_id , ingestion_token_id ) REFERENCES hec_source_sequences ( tenant_id , ingestion_token_id ) ON UPDATE RESTRICT ON DELETE CASCADE;

ALTER TABLE hec_acknowledgments ADD FOREIGN KEY ( tenant_id , ingestion_token_id , channel_id ) REFERENCES hec_channels ( tenant_id , ingestion_token_id , channel_id ) ON UPDATE RESTRICT ON DELETE CASCADE;

ALTER TABLE hec_acknowledgments ADD FOREIGN KEY ( tenant_id , ingestion_token_id , request_sequence ) REFERENCES hec_requests ( tenant_id , ingestion_token_id , request_sequence ) ON UPDATE RESTRICT ON DELETE CASCADE;

ALTER TABLE knowledge_lookup_asset_tenant_ledgers ADD FOREIGN KEY ( tenant_id ) REFERENCES knowledge_catalog_tenants ( tenant_id ) ON UPDATE RESTRICT ON DELETE RESTRICT;

ALTER TABLE knowledge_lookup_asset_stages ADD FOREIGN KEY ( tenant_id ) REFERENCES knowledge_lookup_asset_tenant_ledgers ( tenant_id ) ON UPDATE RESTRICT ON DELETE RESTRICT;

ALTER TABLE knowledge_lookup_assets ADD FOREIGN KEY ( tenant_id ) REFERENCES knowledge_lookup_asset_tenant_ledgers ( tenant_id ) ON UPDATE RESTRICT ON DELETE RESTRICT;

ALTER TABLE knowledge_lookup_assets ADD FOREIGN KEY ( tenant_id , lookup_asset_id , current_version ) REFERENCES knowledge_lookup_asset_versions ( tenant_id , lookup_asset_id , asset_version ) ON UPDATE RESTRICT ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED;

ALTER TABLE knowledge_lookup_asset_versions ADD FOREIGN KEY ( tenant_id , lookup_asset_id ) REFERENCES knowledge_lookup_assets ( tenant_id , lookup_asset_id ) ON UPDATE RESTRICT ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED;

ALTER TABLE knowledge_lookup_definitions ADD FOREIGN KEY ( tenant_id , lookup_id , current_version ) REFERENCES knowledge_lookup_definition_versions ( tenant_id , lookup_id , definition_version ) ON UPDATE RESTRICT ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED;

ALTER TABLE knowledge_lookup_definitions ADD FOREIGN KEY ( tenant_id , app_id ) REFERENCES app_workspaces ( tenant_id , app_id ) ON UPDATE RESTRICT ON DELETE RESTRICT;

ALTER TABLE knowledge_lookup_definition_versions ADD FOREIGN KEY ( tenant_id , lookup_id ) REFERENCES knowledge_lookup_definitions ( tenant_id , lookup_id ) ON DELETE RESTRICT;

ALTER TABLE knowledge_lookup_definition_versions ADD FOREIGN KEY ( tenant_id , lookup_asset_id , asset_version , asset_content_sha256 , asset_size_bytes ) REFERENCES knowledge_lookup_asset_versions ( tenant_id , lookup_asset_id , asset_version , content_sha256 , canonical_bytes ) ON UPDATE RESTRICT ON DELETE RESTRICT;

ALTER TABLE feature_operation_audit_events ADD FOREIGN KEY ( tenant_id ) REFERENCES "feature_operation_audit_tenant_state" ( tenant_id ) ON UPDATE RESTRICT ON DELETE RESTRICT;

ALTER TABLE ingest_write_group_members ADD FOREIGN KEY ( write_group_id ) REFERENCES ingest_write_groups ( write_group_id ) ON UPDATE RESTRICT ON DELETE CASCADE;

ALTER TABLE ingest_write_group_members ADD FOREIGN KEY ( visibility_sequence ) REFERENCES ingest_visibility_reservations ( sequence ) ON UPDATE RESTRICT ON DELETE RESTRICT;

ALTER TABLE ingest_rejection_buckets ADD FOREIGN KEY ( token_id ) REFERENCES ingestion_tokens ( ingestion_token_id ) ON UPDATE RESTRICT ON DELETE CASCADE;

ALTER TABLE audit_events ADD FOREIGN KEY ( tenant_id ) REFERENCES audit_tenant_state ( tenant_id ) ON UPDATE RESTRICT ON DELETE RESTRICT;

ALTER TABLE api_mutation_receipts ADD FOREIGN KEY ( tenant_id ) REFERENCES api_mutation_receipt_tenant_state ( tenant_id ) ON UPDATE RESTRICT ON DELETE RESTRICT;

CREATE FUNCTION os_guard_0ff4a87b61ccec9515a4() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    IF NEW . name <> OLD . name THEN RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'index name is immutable'; END IF;
    RETURN NEW;
END
$$;
CREATE TRIGGER indexes_name_is_immutable BEFORE UPDATE OF name ON indexes FOR EACH ROW EXECUTE FUNCTION os_guard_0ff4a87b61ccec9515a4();

CREATE FUNCTION os_guard_235e9e9527a2dd1d15a5() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    IF NEW . token_digest <> OLD . token_digest THEN RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'ingestion token digest is immutable'; END IF;
    RETURN NEW;
END
$$;
CREATE TRIGGER ingestion_token_digest_is_immutable BEFORE UPDATE OF token_digest ON ingestion_tokens FOR EACH ROW EXECUTE FUNCTION os_guard_235e9e9527a2dd1d15a5();

CREATE FUNCTION os_guard_934b8d35c9eca5d074a0() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    IF OLD . state = 'revoked' AND NEW . state <> 'revoked' THEN RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'revoked ingestion token cannot be reactivated'; END IF;
    RETURN NEW;
END
$$;
CREATE TRIGGER revoked_ingestion_token_is_irreversible BEFORE UPDATE OF state ON ingestion_tokens FOR EACH ROW EXECUTE FUNCTION os_guard_934b8d35c9eca5d074a0();

CREATE FUNCTION os_guard_9256daca0163cd79012f() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    IF NEW . retention_nanoseconds % 1000000 <> 0 THEN RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'index retention must use whole milliseconds'; END IF;
    RETURN NEW;
END
$$;
CREATE TRIGGER indexes_retention_is_millisecond_aligned_on_insert BEFORE INSERT ON indexes FOR EACH ROW EXECUTE FUNCTION os_guard_9256daca0163cd79012f();

CREATE FUNCTION os_guard_c8def93558ea82f2daa5() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    IF NEW . retention_nanoseconds % 1000000 <> 0 THEN RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'index retention must use whole milliseconds'; END IF;
    RETURN NEW;
END
$$;
CREATE TRIGGER indexes_retention_is_millisecond_aligned_on_update BEFORE UPDATE OF retention_nanoseconds ON indexes FOR EACH ROW EXECUTE FUNCTION os_guard_c8def93558ea82f2daa5();

CREATE FUNCTION os_guard_9ccd8d9931cf76077eff() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    IF NEW . app_id <> OLD . app_id OR NEW . tenant_id <> OLD . tenant_id OR NEW . slug <> OLD . slug THEN RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'app workspace identity is immutable'; END IF;
    RETURN NEW;
END
$$;
CREATE TRIGGER app_workspaces_identity_is_immutable BEFORE UPDATE OF app_id, tenant_id, slug ON app_workspaces FOR EACH ROW EXECUTE FUNCTION os_guard_9ccd8d9931cf76077eff();

CREATE FUNCTION os_guard_29ada5bc0decd7c22152() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    IF OLD . state <> 'archived' THEN RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'app workspace must be archived before deletion'; END IF;
    RETURN OLD;
END
$$;
CREATE TRIGGER active_app_workspace_cannot_be_deleted BEFORE DELETE ON app_workspaces FOR EACH ROW EXECUTE FUNCTION os_guard_29ada5bc0decd7c22152();

CREATE FUNCTION os_guard_1b32714677913c5b0559() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    IF EXISTS ( SELECT 1 FROM saved_searches WHERE app_id = NEW . app_id ) THEN RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'app ID is already a legacy saved-search namespace'; END IF;
    RETURN NEW;
END
$$;
CREATE TRIGGER app_workspace_id_cannot_adopt_legacy_saved_search_namespace BEFORE INSERT ON app_workspaces FOR EACH ROW EXECUTE FUNCTION os_guard_1b32714677913c5b0559();

CREATE FUNCTION os_guard_aa69a471f138bd7fa6c5() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    IF NOT EXISTS ( SELECT 1 FROM indexes WHERE index_id = NEW . index_id AND state = 'active' AND search_enabled = 1 ) THEN RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'app default index is not searchable'; END IF;
    RETURN NEW;
END
$$;
CREATE TRIGGER app_default_indexes_require_searchable_index_insert BEFORE INSERT ON app_default_indexes FOR EACH ROW EXECUTE FUNCTION os_guard_aa69a471f138bd7fa6c5();

CREATE FUNCTION os_guard_db3c8d0c1a96dcf88401() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    IF NOT EXISTS ( SELECT 1 FROM indexes WHERE index_id = NEW . index_id AND state = 'active' AND search_enabled = 1 ) THEN RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'app default index is not searchable'; END IF;
    RETURN NEW;
END
$$;
CREATE TRIGGER app_default_indexes_require_searchable_index_update BEFORE UPDATE OF tenant_id, app_id, index_id ON app_default_indexes FOR EACH ROW EXECUTE FUNCTION os_guard_db3c8d0c1a96dcf88401();

CREATE FUNCTION os_guard_9b7ebed6ae0dec35375c() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    IF ( NEW . state <> 'active' OR NEW . search_enabled <> 1 ) AND EXISTS ( SELECT 1 FROM app_default_indexes AS app_index JOIN app_workspaces AS app ON app . tenant_id = app_index . tenant_id AND app . app_id = app_index . app_id WHERE app_index . index_id = OLD . index_id AND app . state = 'active' ) THEN RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'active app requires searchable index'; END IF;
    RETURN NEW;
END
$$;
CREATE TRIGGER active_app_default_indexes_remain_searchable BEFORE UPDATE OF state, search_enabled ON indexes FOR EACH ROW EXECUTE FUNCTION os_guard_9b7ebed6ae0dec35375c();

CREATE FUNCTION os_guard_d4583a0fbe558dc3a693() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    IF NEW . state = 'active' AND EXISTS ( SELECT 1 FROM app_default_indexes AS app_index LEFT JOIN indexes AS search_index ON search_index . index_id = app_index . index_id WHERE app_index . tenant_id = OLD . tenant_id AND app_index . app_id = OLD . app_id AND ( search_index . index_id IS NULL OR search_index . state <> 'active' OR search_index . search_enabled <> 1 ) ) THEN RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'reactivated app requires searchable indexes'; END IF;
    RETURN NEW;
END
$$;
CREATE TRIGGER reactivated_app_requires_searchable_indexes BEFORE UPDATE OF state ON app_workspaces FOR EACH ROW EXECUTE FUNCTION os_guard_d4583a0fbe558dc3a693();

CREATE FUNCTION os_guard_910d52128956cde5f146() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    IF length ( NEW . app_id ) = 26 AND substr ( NEW . app_id , 1 , 4 ) = 'app_' AND substr ( NEW . app_id , 5 ) !~ '^.*[^A-Za-z0-9_-].*$' AND substr ( NEW . app_id , 26 , 1 ) ~ '^[AQgw]$' AND NOT EXISTS ( SELECT 1 FROM app_workspaces WHERE app_id = NEW . app_id ) AND NOT EXISTS ( SELECT 1 FROM saved_searches WHERE app_id = NEW . app_id ) THEN RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'canonical saved-search app does not exist'; END IF;
    RETURN NEW;
END
$$;
CREATE TRIGGER canonical_saved_search_app_exists_insert BEFORE INSERT ON saved_searches FOR EACH ROW EXECUTE FUNCTION os_guard_910d52128956cde5f146();

CREATE FUNCTION os_guard_681dcb266a97ddb8fb54() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    IF length ( NEW . app_id ) = 26 AND substr ( NEW . app_id , 1 , 4 ) = 'app_' AND substr ( NEW . app_id , 5 ) !~ '^.*[^A-Za-z0-9_-].*$' AND substr ( NEW . app_id , 26 , 1 ) ~ '^[AQgw]$' AND NOT EXISTS ( SELECT 1 FROM app_workspaces WHERE app_id = NEW . app_id ) AND NOT EXISTS ( SELECT 1 FROM saved_searches WHERE app_id = NEW . app_id ) THEN RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'canonical saved-search app does not exist'; END IF;
    RETURN NEW;
END
$$;
CREATE TRIGGER canonical_saved_search_app_exists_update BEFORE UPDATE OF app_id ON saved_searches FOR EACH ROW EXECUTE FUNCTION os_guard_681dcb266a97ddb8fb54();

CREATE FUNCTION os_guard_9e876f151fb2f35b1d36() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    IF EXISTS ( SELECT 1 FROM saved_searches WHERE app_id = OLD . app_id ) THEN RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'app workspace is referenced by saved searches'; END IF;
    RETURN OLD;
END
$$;
CREATE TRIGGER referenced_app_workspace_cannot_be_deleted BEFORE DELETE ON app_workspaces FOR EACH ROW EXECUTE FUNCTION os_guard_9e876f151fb2f35b1d36();

CREATE FUNCTION os_guard_dacd9bbadd5e48adeef2() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    IF OLD . bound_collector_id IS NOT NULL AND ( NEW . bound_collector_id IS NULL OR NEW . bound_collector_id <> OLD . bound_collector_id ) THEN RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'ingestion token collector binding is immutable'; END IF;
    RETURN NEW;
END
$$;
CREATE TRIGGER ingestion_token_collector_binding_is_immutable BEFORE UPDATE OF bound_collector_id ON ingestion_tokens FOR EACH ROW EXECUTE FUNCTION os_guard_dacd9bbadd5e48adeef2();

CREATE FUNCTION os_guard_1b8d6f362469b89a1fd0() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'collector catalog fleet/runtime revision marker cannot be deleted';
    RETURN OLD;
END
$$;
CREATE TRIGGER collector_catalog_revision_marker_is_undeletable BEFORE DELETE ON collector_catalog_revisions FOR EACH ROW EXECUTE FUNCTION os_guard_1b8d6f362469b89a1fd0();

CREATE FUNCTION os_guard_b40286269860861f422b() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    IF NEW . tenant_id <> OLD . tenant_id OR NEW . collector_id <> OLD . collector_id THEN RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'collector fleet identity is immutable'; END IF;
    RETURN NEW;
END
$$;
CREATE TRIGGER collector_fleet_identity_is_immutable BEFORE UPDATE OF tenant_id, collector_id ON collector_fleet FOR EACH ROW EXECUTE FUNCTION os_guard_b40286269860861f422b();

CREATE FUNCTION os_guard_b6415c8220def938e813() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    IF NEW . admin_version < OLD . admin_version THEN RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'collector administrator version cannot move backward'; END IF;
    RETURN NEW;
END
$$;
CREATE TRIGGER collector_fleet_admin_version_is_monotonic BEFORE UPDATE OF admin_version ON collector_fleet FOR EACH ROW EXECUTE FUNCTION os_guard_b6415c8220def938e813();

CREATE FUNCTION os_guard_8df62d1eca9ff5b38582() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    INSERT INTO collector_catalog_revisions ( tenant_id , revision , fleet_count , runtime_count ) VALUES ( NEW . tenant_id , 1 , 1 , 0 ) ON CONFLICT ( tenant_id ) DO UPDATE SET revision = CASE WHEN collector_catalog_revisions . revision = 9223372036854775807 THEN os_constraint_failure('collector catalog revision exhausted') ELSE collector_catalog_revisions . revision + 1 END , fleet_count = CASE WHEN collector_catalog_revisions . fleet_count = 256 THEN os_constraint_failure('collector fleet capacity exhausted') ELSE collector_catalog_revisions . fleet_count + 1 END ;
    RETURN NEW;
END
$$;
CREATE TRIGGER collector_catalog_revision_after_fleet_insert AFTER INSERT ON collector_fleet FOR EACH ROW EXECUTE FUNCTION os_guard_8df62d1eca9ff5b38582();

CREATE FUNCTION os_guard_d1b438ea51e8d3376003() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    UPDATE collector_catalog_revisions SET revision = CASE WHEN revision = 9223372036854775807 THEN os_constraint_failure('collector catalog revision exhausted') ELSE revision + 1 END WHERE tenant_id = NEW . tenant_id ; GET DIAGNOSTICS affected_rows = ROW_COUNT; IF affected_rows <> 1 THEN RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'collector catalog fleet/runtime revision is missing'; END IF;
    RETURN NEW;
END
$$;
CREATE TRIGGER collector_catalog_revision_after_fleet_update AFTER UPDATE ON collector_fleet FOR EACH ROW EXECUTE FUNCTION os_guard_d1b438ea51e8d3376003();

CREATE FUNCTION os_guard_ac54dfd8bb40b1e80a24() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    UPDATE collector_catalog_revisions SET revision = CASE WHEN revision = 9223372036854775807 THEN os_constraint_failure('collector catalog revision exhausted') ELSE revision + 1 END , fleet_count = CASE WHEN fleet_count = 0 THEN os_constraint_failure('collector catalog fleet count underflow') ELSE fleet_count - 1 END WHERE tenant_id = OLD . tenant_id ; GET DIAGNOSTICS affected_rows = ROW_COUNT; IF affected_rows <> 1 THEN RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'collector catalog fleet/runtime revision is missing'; END IF;
    RETURN OLD;
END
$$;
CREATE TRIGGER collector_catalog_revision_after_fleet_delete AFTER DELETE ON collector_fleet FOR EACH ROW EXECUTE FUNCTION os_guard_ac54dfd8bb40b1e80a24();

CREATE FUNCTION os_guard_a00ae9c0849eca98d32b() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    IF NOT EXISTS ( SELECT 1 FROM collector_catalog_revisions WHERE tenant_id = NEW . tenant_id ) AND ( EXISTS ( SELECT 1 FROM collector_fleet WHERE tenant_id = NEW . tenant_id ) OR EXISTS ( SELECT 1 FROM collector_runtime WHERE tenant_id = NEW . tenant_id ) ) THEN RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'collector catalog fleet/runtime revision is missing'; END IF;
    RETURN NEW;
END
$$;
CREATE TRIGGER collector_catalog_revision_before_fleet_insert BEFORE INSERT ON collector_fleet FOR EACH ROW EXECUTE FUNCTION os_guard_a00ae9c0849eca98d32b();

CREATE FUNCTION os_guard_b338d4d3573eb8ea4bc6() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    IF NEW . tenant_id <> OLD . tenant_id OR NEW . collector_id <> OLD . collector_id THEN RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'collector runtime identity is immutable'; END IF;
    RETURN NEW;
END
$$;
CREATE TRIGGER collector_runtime_identity_is_immutable BEFORE UPDATE OF tenant_id, collector_id ON collector_runtime FOR EACH ROW EXECUTE FUNCTION os_guard_b338d4d3573eb8ea4bc6();

CREATE FUNCTION os_guard_fb9a596599a8208b7593() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    IF NEW . telemetry_revision < OLD . telemetry_revision OR NEW . lease_generation < OLD . lease_generation THEN RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'collector runtime revisions cannot move backward'; END IF;
    RETURN NEW;
END
$$;
CREATE TRIGGER collector_runtime_revisions_are_monotonic BEFORE UPDATE OF telemetry_revision, lease_generation ON collector_runtime FOR EACH ROW EXECUTE FUNCTION os_guard_fb9a596599a8208b7593();

CREATE FUNCTION os_guard_f9f2c4063ec2a51a4961() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    UPDATE collector_catalog_revisions SET revision = CASE WHEN revision = 9223372036854775807 THEN os_constraint_failure('collector catalog revision exhausted') ELSE revision + 1 END , runtime_count = CASE WHEN runtime_count = 256 THEN os_constraint_failure('collector runtime capacity exhausted') ELSE runtime_count + 1 END WHERE tenant_id = NEW . tenant_id ; GET DIAGNOSTICS affected_rows = ROW_COUNT; IF affected_rows <> 1 THEN RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'collector catalog fleet/runtime revision is missing'; END IF;
    RETURN NEW;
END
$$;
CREATE TRIGGER collector_catalog_revision_after_runtime_insert AFTER INSERT ON collector_runtime FOR EACH ROW EXECUTE FUNCTION os_guard_f9f2c4063ec2a51a4961();

CREATE FUNCTION os_guard_1ce3205de53a29777345() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    UPDATE collector_catalog_revisions SET revision = CASE WHEN revision = 9223372036854775807 THEN os_constraint_failure('collector catalog revision exhausted') ELSE revision + 1 END WHERE tenant_id = NEW . tenant_id ; GET DIAGNOSTICS affected_rows = ROW_COUNT; IF affected_rows <> 1 THEN RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'collector catalog fleet/runtime revision is missing'; END IF;
    RETURN NEW;
END
$$;
CREATE TRIGGER collector_catalog_revision_after_runtime_update AFTER UPDATE ON collector_runtime FOR EACH ROW EXECUTE FUNCTION os_guard_1ce3205de53a29777345();

CREATE FUNCTION os_guard_143cd61e4b45a2530d55() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    UPDATE collector_catalog_revisions SET revision = CASE WHEN revision = 9223372036854775807 THEN os_constraint_failure('collector catalog revision exhausted') ELSE revision + 1 END , runtime_count = CASE WHEN runtime_count = 0 THEN os_constraint_failure('collector catalog runtime count underflow') ELSE runtime_count - 1 END WHERE tenant_id = OLD . tenant_id ; GET DIAGNOSTICS affected_rows = ROW_COUNT; IF affected_rows <> 1 THEN RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'collector catalog fleet/runtime revision is missing'; END IF;
    RETURN OLD;
END
$$;
CREATE TRIGGER collector_catalog_revision_after_runtime_delete AFTER DELETE ON collector_runtime FOR EACH ROW EXECUTE FUNCTION os_guard_143cd61e4b45a2530d55();

CREATE FUNCTION os_guard_7106f18af37e8352b1f6() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    INSERT INTO search_history_owner_counts ( tenant_id , owner_id , terminal_count ) VALUES ( NEW . tenant_id , NEW . owner_id , 1 ) ON CONFLICT ( tenant_id , owner_id ) DO UPDATE SET terminal_count = search_history_owner_counts.terminal_count + 1 ;
    RETURN NEW;
END
$$;
CREATE TRIGGER search_history_owner_count_after_insert AFTER INSERT ON search_history FOR EACH ROW EXECUTE FUNCTION os_guard_7106f18af37e8352b1f6();

CREATE FUNCTION os_guard_583a8b9e86ab15d4316a() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    IF NOT EXISTS ( SELECT 1 FROM search_history_owner_counts WHERE tenant_id = OLD . tenant_id AND owner_id = OLD . owner_id ) THEN RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'search-history owner count is missing'; END IF; DELETE FROM search_history_owner_counts WHERE tenant_id = OLD . tenant_id AND owner_id = OLD . owner_id AND terminal_count = 1 ; UPDATE search_history_owner_counts SET terminal_count = terminal_count - 1 WHERE tenant_id = OLD . tenant_id AND owner_id = OLD . owner_id ;
    RETURN OLD;
END
$$;
CREATE TRIGGER search_history_owner_count_after_delete AFTER DELETE ON search_history FOR EACH ROW EXECUTE FUNCTION os_guard_583a8b9e86ab15d4316a();

CREATE FUNCTION os_guard_63fa15dcb2fd2fb05b60() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    IF EXISTS ( SELECT 1 FROM index_deletion_tombstones WHERE index_deletion_tombstones . index_id = OLD . index_id ) THEN RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'tombstoned index is immutable'; END IF;
    RETURN NEW;
END
$$;
CREATE TRIGGER tombstoned_index_update_is_forbidden BEFORE UPDATE ON indexes FOR EACH ROW EXECUTE FUNCTION os_guard_63fa15dcb2fd2fb05b60();

CREATE FUNCTION os_guard_695aa0517d9cb34f61dd() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    IF EXISTS ( SELECT 1 FROM index_deletion_tombstones WHERE index_deletion_tombstones . index_id = OLD . index_id ) THEN RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'tombstoned index cannot be deleted'; END IF;
    RETURN OLD;
END
$$;
CREATE TRIGGER tombstoned_index_delete_is_forbidden BEFORE DELETE ON indexes FOR EACH ROW EXECUTE FUNCTION os_guard_695aa0517d9cb34f61dd();

CREATE FUNCTION os_guard_3d4b4a35a30db1afc9b2() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'index deletion tombstone is immutable';
    RETURN NEW;
END
$$;
CREATE TRIGGER index_deletion_tombstone_update_is_forbidden BEFORE UPDATE ON index_deletion_tombstones FOR EACH ROW EXECUTE FUNCTION os_guard_3d4b4a35a30db1afc9b2();

CREATE FUNCTION os_guard_d12a1f52a5022bfd21ba() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'index deletion tombstone cannot be deleted';
    RETURN OLD;
END
$$;
CREATE TRIGGER index_deletion_tombstone_delete_is_forbidden BEFORE DELETE ON index_deletion_tombstones FOR EACH ROW EXECUTE FUNCTION os_guard_d12a1f52a5022bfd21ba();

CREATE FUNCTION os_guard_708f4cf86330ff9615cd() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    IF NEW . state = 'deleting' THEN RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'deleting index creation requires a deletion operation'; END IF;
    RETURN NEW;
END
$$;
CREATE TRIGGER deleting_index_insert_is_forbidden BEFORE INSERT ON indexes FOR EACH ROW EXECUTE FUNCTION os_guard_708f4cf86330ff9615cd();

CREATE FUNCTION os_guard_13dfeb3da754c74956e8() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    IF NOT EXISTS ( SELECT 1 FROM indexes WHERE indexes . index_id = NEW . index_id AND indexes . name = NEW . index_name AND indexes . version = NEW . archived_index_version AND indexes . state = 'archived' AND indexes . updated_at_unix_micro <= NEW . created_at_unix_micro AND NOT EXISTS ( SELECT 1 FROM index_deletion_tombstones WHERE index_deletion_tombstones . index_id = indexes . index_id ) ) THEN RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'index deletion operation must match an archived index generation'; END IF;
    RETURN NEW;
END
$$;
CREATE TRIGGER index_deletion_operation_insert_is_valid BEFORE INSERT ON index_deletion_operations FOR EACH ROW EXECUTE FUNCTION os_guard_13dfeb3da754c74956e8();

CREATE FUNCTION os_guard_6b3bdc893e820410da75() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    IF EXISTS ( SELECT 1 FROM index_deletion_operations WHERE deletion_operation_id = NEW . deletion_operation_id OR index_id = NEW . index_id ) THEN RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'index deletion operation identity is already in use'; END IF;
    RETURN NEW;
END
$$;
CREATE TRIGGER index_deletion_operation_identity_collision_is_forbidden BEFORE INSERT ON index_deletion_operations FOR EACH ROW EXECUTE FUNCTION os_guard_6b3bdc893e820410da75();

CREATE FUNCTION os_guard_8d459c68bda046094441() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    IF OLD . state <> 'deleting' AND NEW . state = 'deleting' AND NOT EXISTS ( SELECT 1 FROM index_deletion_operations WHERE index_deletion_operations . index_id = OLD . index_id AND index_deletion_operations . index_name = OLD . name AND index_deletion_operations . archived_index_version = OLD . version AND NEW . version = OLD . version + 1 AND NEW . updated_at_unix_micro = index_deletion_operations . created_at_unix_micro ) THEN RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'deleting index transition requires a matching deletion operation'; END IF;
    RETURN NEW;
END
$$;
CREATE TRIGGER index_deleting_transition_requires_operation BEFORE UPDATE OF state ON indexes FOR EACH ROW EXECUTE FUNCTION os_guard_8d459c68bda046094441();

CREATE FUNCTION os_guard_ad325cc9c91a1dbfe759() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    UPDATE indexes SET state = 'deleting' , version = version + 1 , updated_at_unix_micro = NEW . created_at_unix_micro WHERE index_id = NEW . index_id AND name = NEW . index_name AND version = NEW . archived_index_version AND state = 'archived' ; GET DIAGNOSTICS affected_rows = ROW_COUNT; IF affected_rows <> 1 THEN RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'index deletion operation transition failed'; END IF;
    RETURN NEW;
END
$$;
CREATE TRIGGER index_deletion_operation_insert_starts_deletion AFTER INSERT ON index_deletion_operations FOR EACH ROW EXECUTE FUNCTION os_guard_ad325cc9c91a1dbfe759();

CREATE FUNCTION os_guard_824f32bf376a3f7328e8() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    IF OLD . state = 'deleting' AND EXISTS ( SELECT 1 FROM index_deletion_operations WHERE index_deletion_operations . index_id = OLD . index_id ) THEN RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'deleting index is coordinator-owned'; END IF;
    RETURN NEW;
END
$$;
CREATE TRIGGER deleting_index_with_operation_update_is_forbidden BEFORE UPDATE ON indexes FOR EACH ROW EXECUTE FUNCTION os_guard_824f32bf376a3f7328e8();

CREATE FUNCTION os_guard_9b8ed7b78755eae913fe() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    IF EXISTS ( SELECT 1 FROM index_deletion_operations WHERE index_deletion_operations . index_id = OLD . index_id ) THEN RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'deleting index cannot be deleted'; END IF;
    RETURN OLD;
END
$$;
CREATE TRIGGER deleting_index_with_operation_delete_is_forbidden BEFORE DELETE ON indexes FOR EACH ROW EXECUTE FUNCTION os_guard_9b8ed7b78755eae913fe();

CREATE FUNCTION os_guard_70b5f95411e034bbc08e() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'index deletion operation is immutable';
    RETURN NEW;
END
$$;
CREATE TRIGGER index_deletion_operation_update_is_forbidden BEFORE UPDATE ON index_deletion_operations FOR EACH ROW EXECUTE FUNCTION os_guard_70b5f95411e034bbc08e();

CREATE FUNCTION os_guard_df0f52a9d854eee7ce64() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    IF EXISTS ( SELECT 1 FROM index_deletion_mutation_attempts WHERE deletion_operation_id = NEW . deletion_operation_id OR correlation_id = NEW . correlation_id ) THEN RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'index deletion mutation attempt identity is already in use'; END IF;
    RETURN NEW;
END
$$;
CREATE TRIGGER index_deletion_mutation_attempt_identity_collision_is_forbidden BEFORE INSERT ON index_deletion_mutation_attempts FOR EACH ROW EXECUTE FUNCTION os_guard_df0f52a9d854eee7ce64();

CREATE FUNCTION os_guard_986942956913feca4b20() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    IF NOT EXISTS ( SELECT 1 FROM index_deletion_operations AS deletion_operation JOIN indexes AS target_index ON target_index . index_id = deletion_operation . index_id AND target_index . name = deletion_operation . index_name AND target_index . version = deletion_operation . archived_index_version + 1 AND target_index . state = 'deleting' AND target_index . updated_at_unix_micro = deletion_operation . created_at_unix_micro WHERE deletion_operation . deletion_operation_id = NEW . deletion_operation_id AND deletion_operation . tenant_id = NEW . tenant_id AND NEW . created_at_unix_micro >= deletion_operation . created_at_unix_micro AND NOT EXISTS ( SELECT 1 FROM index_deletion_tombstones WHERE index_deletion_tombstones . index_id = deletion_operation . index_id ) ) THEN RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'index deletion mutation attempt requires a deleting operation'; END IF;
    RETURN NEW;
END
$$;
CREATE TRIGGER index_deletion_mutation_attempt_insert_is_valid BEFORE INSERT ON index_deletion_mutation_attempts FOR EACH ROW EXECUTE FUNCTION os_guard_986942956913feca4b20();

CREATE FUNCTION os_guard_28188f9f65ed69445d1b() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'index deletion mutation attempt is immutable';
    RETURN NEW;
END
$$;
CREATE TRIGGER index_deletion_mutation_attempt_update_is_forbidden BEFORE UPDATE ON index_deletion_mutation_attempts FOR EACH ROW EXECUTE FUNCTION os_guard_28188f9f65ed69445d1b();

CREATE FUNCTION os_guard_584b175efd4ea29ba2d5() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    IF EXISTS ( SELECT 1 FROM index_data_deletion_completions WHERE deletion_operation_id = NEW . deletion_operation_id OR correlation_id = NEW . correlation_id OR index_id = NEW . index_id ) THEN RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'index data deletion completion identity is already in use'; END IF;
    RETURN NEW;
END
$$;
CREATE TRIGGER index_data_deletion_completion_identity_collision_is_forbidden BEFORE INSERT ON index_data_deletion_completions FOR EACH ROW EXECUTE FUNCTION os_guard_584b175efd4ea29ba2d5();

CREATE FUNCTION os_guard_0cc02dacd3362bec9788() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    IF NOT EXISTS ( SELECT 1 FROM index_deletion_operations AS deletion_operation JOIN index_deletion_mutation_attempts AS mutation_attempt ON mutation_attempt . deletion_operation_id = deletion_operation . deletion_operation_id JOIN indexes AS target_index ON target_index . index_id = deletion_operation . index_id AND target_index . name = deletion_operation . index_name AND target_index . version = deletion_operation . archived_index_version + 1 AND target_index . state = 'deleting' AND target_index . updated_at_unix_micro = deletion_operation . created_at_unix_micro WHERE deletion_operation . deletion_operation_id = NEW . deletion_operation_id AND deletion_operation . index_id = NEW . index_id AND deletion_operation . index_name = NEW . index_name AND deletion_operation . tenant_id = NEW . tenant_id AND deletion_operation . archived_index_version = NEW . archived_index_version AND deletion_operation . created_at_unix_micro = NEW . operation_created_at_unix_micro AND NEW . deleting_index_version = deletion_operation . archived_index_version + 1 AND mutation_attempt . correlation_id = NEW . correlation_id AND mutation_attempt . tenant_id = NEW . tenant_id AND mutation_attempt . clickhouse_database = NEW . clickhouse_database AND mutation_attempt . clickhouse_table = NEW . clickhouse_table AND mutation_attempt . clickhouse_table_uuid = NEW . clickhouse_table_uuid AND mutation_attempt . protocol_version = NEW . protocol_version AND mutation_attempt . created_at_unix_micro = NEW . attempt_created_at_unix_micro AND NEW . completed_at_unix_micro >= mutation_attempt . created_at_unix_micro AND NOT EXISTS ( SELECT 1 FROM index_deletion_tombstones WHERE index_deletion_tombstones . index_id = NEW . index_id ) ) THEN RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'index data deletion completion must match outstanding work'; END IF;
    RETURN NEW;
END
$$;
CREATE TRIGGER index_data_deletion_completion_insert_is_valid BEFORE INSERT ON index_data_deletion_completions FOR EACH ROW EXECUTE FUNCTION os_guard_0cc02dacd3362bec9788();

CREATE FUNCTION os_guard_d93a230cc1e77ea3b237() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    IF EXISTS ( SELECT 1 FROM index_deletion_tombstones WHERE index_id = NEW . index_id ) THEN RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'index deletion tombstone identity is already in use'; END IF;
    RETURN NEW;
END
$$;
CREATE TRIGGER index_deletion_tombstone_identity_collision_is_forbidden BEFORE INSERT ON index_deletion_tombstones FOR EACH ROW EXECUTE FUNCTION os_guard_d93a230cc1e77ea3b237();

CREATE FUNCTION os_guard_5e5c8a7719250e9ab224() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    IF NOT EXISTS ( SELECT 1 FROM indexes AS target_index WHERE target_index . index_id = NEW . index_id AND target_index . name = NEW . name AND target_index . version = NEW . deleted_version AND ( ( target_index . state = 'archived' AND NOT EXISTS ( SELECT 1 FROM index_deletion_operations WHERE index_deletion_operations . index_id = target_index . index_id ) ) OR ( target_index . state = 'deleting' AND EXISTS ( SELECT 1 FROM index_data_deletion_completions AS completion WHERE completion . index_id = NEW . index_id AND completion . index_name = NEW . name AND completion . deleting_index_version = NEW . deleted_version AND completion . completed_at_unix_micro = NEW . deleted_at_unix_micro ) ) ) ) THEN RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'index deletion tombstone must match a terminal index'; END IF;
    RETURN NEW;
END
$$;
CREATE TRIGGER index_deletion_tombstone_insert_is_valid BEFORE INSERT ON index_deletion_tombstones FOR EACH ROW EXECUTE FUNCTION os_guard_5e5c8a7719250e9ab224();

CREATE FUNCTION os_guard_ddf4e3ef4ec942954d18() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    IF EXISTS ( SELECT 1 FROM index_deletion_operations WHERE deletion_operation_id = OLD . deletion_operation_id ) AND NOT EXISTS ( SELECT 1 FROM index_data_deletion_completions WHERE deletion_operation_id = OLD . deletion_operation_id AND correlation_id = OLD . correlation_id AND tenant_id = OLD . tenant_id AND clickhouse_database = OLD . clickhouse_database AND clickhouse_table = OLD . clickhouse_table AND clickhouse_table_uuid = OLD . clickhouse_table_uuid AND protocol_version = OLD . protocol_version AND attempt_created_at_unix_micro = OLD . created_at_unix_micro ) THEN RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'index deletion mutation attempt cannot be deleted'; END IF;
    RETURN OLD;
END
$$;
CREATE TRIGGER index_deletion_mutation_attempt_delete_is_forbidden BEFORE DELETE ON index_deletion_mutation_attempts FOR EACH ROW EXECUTE FUNCTION os_guard_ddf4e3ef4ec942954d18();

CREATE FUNCTION os_guard_37cd00d18a5e08124afd() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    IF NOT EXISTS ( SELECT 1 FROM index_data_deletion_completions AS completion JOIN index_deletion_tombstones AS tombstone ON tombstone . index_id = completion . index_id AND tombstone . name = completion . index_name AND tombstone . deleted_version = completion . deleting_index_version AND tombstone . deleted_at_unix_micro = completion . completed_at_unix_micro JOIN indexes AS target_index ON target_index . index_id = completion . index_id AND target_index . name = completion . index_name AND target_index . version = completion . deleting_index_version AND target_index . state = 'deleting' AND target_index . updated_at_unix_micro = completion . operation_created_at_unix_micro WHERE completion . deletion_operation_id = OLD . deletion_operation_id AND completion . index_id = OLD . index_id AND completion . index_name = OLD . index_name AND completion . tenant_id = OLD . tenant_id AND completion . archived_index_version = OLD . archived_index_version AND completion . operation_created_at_unix_micro = OLD . created_at_unix_micro ) THEN RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'index deletion operation cannot be deleted'; END IF;
    RETURN OLD;
END
$$;
CREATE TRIGGER index_deletion_operation_delete_is_forbidden BEFORE DELETE ON index_deletion_operations FOR EACH ROW EXECUTE FUNCTION os_guard_37cd00d18a5e08124afd();

CREATE FUNCTION os_guard_a470a47cd930dc166103() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    INSERT INTO index_deletion_tombstones ( index_id , name , deleted_version , deleted_at_unix_micro ) VALUES ( NEW . index_id , NEW . index_name , NEW . deleting_index_version , NEW . completed_at_unix_micro ) ; GET DIAGNOSTICS affected_rows = ROW_COUNT; IF affected_rows <> 1 THEN RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'index data deletion completion tombstone failed'; END IF; DELETE FROM index_deletion_operations WHERE deletion_operation_id = NEW . deletion_operation_id AND index_id = NEW . index_id AND index_name = NEW . index_name AND tenant_id = NEW . tenant_id AND archived_index_version = NEW . archived_index_version AND created_at_unix_micro = NEW . operation_created_at_unix_micro ; GET DIAGNOSTICS affected_rows = ROW_COUNT; IF affected_rows <> 1 THEN RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'index data deletion completion operation cleanup failed'; END IF; IF EXISTS ( SELECT 1 FROM index_deletion_mutation_attempts WHERE deletion_operation_id = NEW . deletion_operation_id ) THEN RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'index data deletion completion attempt cleanup failed'; END IF;
    RETURN NEW;
END
$$;
CREATE TRIGGER index_data_deletion_completion_finishes_operation AFTER INSERT ON index_data_deletion_completions FOR EACH ROW EXECUTE FUNCTION os_guard_a470a47cd930dc166103();

CREATE FUNCTION os_guard_72c84b515f43e4eb65c4() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'index data deletion completion is immutable';
    RETURN NEW;
END
$$;
CREATE TRIGGER index_data_deletion_completion_update_is_forbidden BEFORE UPDATE ON index_data_deletion_completions FOR EACH ROW EXECUTE FUNCTION os_guard_72c84b515f43e4eb65c4();

CREATE FUNCTION os_guard_0041c787a43c49683186() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'index data deletion completion cannot be deleted';
    RETURN OLD;
END
$$;
CREATE TRIGGER index_data_deletion_completion_delete_is_forbidden BEFORE DELETE ON index_data_deletion_completions FOR EACH ROW EXECUTE FUNCTION os_guard_0041c787a43c49683186();

CREATE FUNCTION os_guard_8ce25721df695eb0eeba() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    IF EXISTS ( SELECT 1 FROM index_data_deletion_completions WHERE deletion_operation_id = NEW . deletion_operation_id OR index_id = NEW . index_id ) THEN RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'completed index deletion operation identity is already in use'; END IF;
    RETURN NEW;
END
$$;
CREATE TRIGGER index_deletion_operation_completed_identity_is_forbidden BEFORE INSERT ON index_deletion_operations FOR EACH ROW EXECUTE FUNCTION os_guard_8ce25721df695eb0eeba();

CREATE FUNCTION os_guard_2c242b071eb7e772a2cd() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    IF EXISTS ( SELECT 1 FROM index_data_deletion_completions WHERE deletion_operation_id = NEW . deletion_operation_id OR correlation_id = NEW . correlation_id ) THEN RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'completed index deletion mutation identity is already in use'; END IF;
    RETURN NEW;
END
$$;
CREATE TRIGGER index_deletion_mutation_completed_identity_is_forbidden BEFORE INSERT ON index_deletion_mutation_attempts FOR EACH ROW EXECUTE FUNCTION os_guard_2c242b071eb7e772a2cd();

CREATE FUNCTION os_guard_cf059db83e6a1469966d() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    IF NEW . singleton_id <> OLD . singleton_id THEN RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'index catalog state identity is immutable'; END IF;
    RETURN NEW;
END
$$;
CREATE TRIGGER index_catalog_state_identity_is_immutable BEFORE UPDATE OF singleton_id ON index_catalog_state FOR EACH ROW EXECUTE FUNCTION os_guard_cf059db83e6a1469966d();

CREATE FUNCTION os_guard_a5689925e6af0e361f7c() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    IF EXISTS ( SELECT 1 FROM index_catalog_state WHERE singleton_id = NEW . singleton_id ) THEN RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'index catalog state already exists'; END IF;
    RETURN NEW;
END
$$;
CREATE TRIGGER index_catalog_state_collision_is_forbidden BEFORE INSERT ON index_catalog_state FOR EACH ROW EXECUTE FUNCTION os_guard_a5689925e6af0e361f7c();

CREATE FUNCTION os_guard_479ba7e0ba01289c14d0() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    IF NOT ( NEW . revision = OLD . revision + 1 AND ( NEW . physical_count = OLD . physical_count OR NEW . physical_count = OLD . physical_count + 1 ) ) THEN RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'index catalog state transition is invalid'; END IF;
    RETURN NEW;
END
$$;
CREATE TRIGGER index_catalog_state_transition_is_valid BEFORE UPDATE OF revision, physical_count ON index_catalog_state FOR EACH ROW EXECUTE FUNCTION os_guard_479ba7e0ba01289c14d0();

CREATE FUNCTION os_guard_c7c1296249f166b6fcb5() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'index catalog state cannot be deleted';
    RETURN OLD;
END
$$;
CREATE TRIGGER index_catalog_state_delete_is_forbidden BEFORE DELETE ON index_catalog_state FOR EACH ROW EXECUTE FUNCTION os_guard_c7c1296249f166b6fcb5();

CREATE FUNCTION os_guard_68c301e1e2c9e3350d96() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    IF NOT ( octet_length ( NEW . index_id ) BETWEEN 1 AND 128 AND position ( decode ( '00' , 'hex' ) IN convert_to ( NEW . index_id , 'UTF8' ) ) = 0 AND NEW . index_id = trim ( NEW . index_id ) AND NEW . index_id !~ '^.*[--].*$' AND octet_length ( NEW . display_name ) BETWEEN 1 AND 255 AND position ( decode ( '00' , 'hex' ) IN convert_to ( NEW . display_name , 'UTF8' ) ) = 0 AND NEW . display_name = trim ( NEW . display_name ) AND NEW . display_name !~ '^.*[--].*$' AND octet_length ( NEW . description ) BETWEEN 0 AND 8192 AND position ( decode ( '00' , 'hex' ) IN convert_to ( NEW . description , 'UTF8' ) ) = 0 AND NEW . description !~ '^.*[-
-
--].*$' AND octet_length ( NEW . default_sourcetype ) BETWEEN 0 AND 255 AND position ( decode ( '00' , 'hex' ) IN convert_to ( NEW . default_sourcetype , 'UTF8' ) ) = 0 AND NEW . default_sourcetype = trim ( NEW . default_sourcetype ) AND NEW . default_sourcetype !~ '^.*[--].*$' AND NEW . created_at_unix_micro BETWEEN 1 AND 253402300799999999 AND NEW . updated_at_unix_micro BETWEEN NEW . created_at_unix_micro AND 253402300799999999 ) THEN RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'index catalog record is invalid or unbounded'; END IF;
    RETURN NEW;
END
$$;
CREATE TRIGGER index_catalog_record_insert_is_bounded BEFORE INSERT ON indexes FOR EACH ROW EXECUTE FUNCTION os_guard_68c301e1e2c9e3350d96();

CREATE FUNCTION os_guard_c1345b449d29bb643be1() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    IF NOT ( octet_length ( NEW . index_id ) BETWEEN 1 AND 128 AND position ( decode ( '00' , 'hex' ) IN convert_to ( NEW . index_id , 'UTF8' ) ) = 0 AND NEW . index_id = trim ( NEW . index_id ) AND NEW . index_id !~ '^.*[--].*$' AND octet_length ( NEW . display_name ) BETWEEN 1 AND 255 AND position ( decode ( '00' , 'hex' ) IN convert_to ( NEW . display_name , 'UTF8' ) ) = 0 AND NEW . display_name = trim ( NEW . display_name ) AND NEW . display_name !~ '^.*[--].*$' AND octet_length ( NEW . description ) BETWEEN 0 AND 8192 AND position ( decode ( '00' , 'hex' ) IN convert_to ( NEW . description , 'UTF8' ) ) = 0 AND NEW . description !~ '^.*[-
-
--].*$' AND octet_length ( NEW . default_sourcetype ) BETWEEN 0 AND 255 AND position ( decode ( '00' , 'hex' ) IN convert_to ( NEW . default_sourcetype , 'UTF8' ) ) = 0 AND NEW . default_sourcetype = trim ( NEW . default_sourcetype ) AND NEW . default_sourcetype !~ '^.*[--].*$' AND NEW . created_at_unix_micro BETWEEN 1 AND 253402300799999999 AND NEW . updated_at_unix_micro BETWEEN NEW . created_at_unix_micro AND 253402300799999999 ) THEN RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'index catalog record is invalid or unbounded'; END IF;
    RETURN NEW;
END
$$;
CREATE TRIGGER index_catalog_record_update_is_bounded BEFORE UPDATE OF
    index_id,
    display_name,
    description,
    default_sourcetype,
    created_at_unix_micro,
    updated_at_unix_micro ON indexes FOR EACH ROW EXECUTE FUNCTION os_guard_c1345b449d29bb643be1();

CREATE FUNCTION os_guard_895103aca564f12797ac() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    IF NEW . index_id <> OLD . index_id THEN RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'index ID is immutable'; END IF;
    RETURN NEW;
END
$$;
CREATE TRIGGER indexes_id_is_immutable BEFORE UPDATE OF index_id ON indexes FOR EACH ROW EXECUTE FUNCTION os_guard_895103aca564f12797ac();

CREATE FUNCTION os_guard_b355b2151da3669346c5() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    IF NOT EXISTS ( SELECT 1 FROM index_catalog_state WHERE singleton_id = 1 AND revision BETWEEN 1 AND 9223372036854775806 AND physical_count BETWEEN 0 AND 1023 ) THEN RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'index catalog state is invalid or capacity is exhausted'; END IF;
    RETURN NEW;
END
$$;
CREATE TRIGGER index_catalog_before_index_insert BEFORE INSERT ON indexes FOR EACH ROW EXECUTE FUNCTION os_guard_b355b2151da3669346c5();

CREATE FUNCTION os_guard_daae48cf426386416bfa() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    IF EXISTS ( SELECT 1 FROM indexes WHERE index_id = NEW . index_id OR name = NEW . name ) THEN RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'index catalog identity is already in use'; END IF;
    RETURN NEW;
END
$$;
CREATE TRIGGER index_catalog_identity_collision_is_forbidden BEFORE INSERT ON indexes FOR EACH ROW EXECUTE FUNCTION os_guard_daae48cf426386416bfa();

CREATE FUNCTION os_guard_eeb8fb3ef9f36b4274fc() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    UPDATE index_catalog_state SET revision = revision + 1 , physical_count = physical_count + 1 WHERE singleton_id = 1 ; GET DIAGNOSTICS affected_rows = ROW_COUNT; IF affected_rows <> 1 THEN RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'index catalog insert accounting failed'; END IF;
    RETURN NEW;
END
$$;
CREATE TRIGGER index_catalog_after_index_insert AFTER INSERT ON indexes FOR EACH ROW EXECUTE FUNCTION os_guard_eeb8fb3ef9f36b4274fc();

CREATE FUNCTION os_guard_dc77cf72e236d23a70ed() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    IF NOT EXISTS ( SELECT 1 FROM index_catalog_state WHERE singleton_id = 1 AND revision BETWEEN 1 AND 9223372036854775806 AND physical_count BETWEEN 1 AND 1024 ) THEN RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'index catalog state is invalid'; END IF;
    RETURN NEW;
END
$$;
CREATE TRIGGER index_catalog_before_index_update BEFORE UPDATE ON indexes FOR EACH ROW EXECUTE FUNCTION os_guard_dc77cf72e236d23a70ed();

CREATE FUNCTION os_guard_6bc869968e1cdbd184ce() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    UPDATE index_catalog_state SET revision = revision + 1 WHERE singleton_id = 1 ; GET DIAGNOSTICS affected_rows = ROW_COUNT; IF affected_rows <> 1 THEN RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'index catalog update accounting failed'; END IF;
    RETURN NEW;
END
$$;
CREATE TRIGGER index_catalog_after_index_update AFTER UPDATE ON indexes FOR EACH ROW EXECUTE FUNCTION os_guard_6bc869968e1cdbd184ce();

CREATE FUNCTION os_guard_55a270d2343e409b6288() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'index catalog identity cannot be deleted';
    RETURN OLD;
END
$$;
CREATE TRIGGER index_catalog_index_delete_is_forbidden BEFORE DELETE ON indexes FOR EACH ROW EXECUTE FUNCTION os_guard_55a270d2343e409b6288();

CREATE FUNCTION os_guard_8f1c6f6bb0861a0d4bc1() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    IF NOT EXISTS ( SELECT 1 FROM index_catalog_state WHERE singleton_id = 1 AND revision BETWEEN 1 AND 9223372036854775806 AND physical_count BETWEEN 1 AND 1024 ) THEN RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'index catalog state is invalid'; END IF;
    RETURN NEW;
END
$$;
CREATE TRIGGER index_catalog_before_tombstone_insert BEFORE INSERT ON index_deletion_tombstones FOR EACH ROW EXECUTE FUNCTION os_guard_8f1c6f6bb0861a0d4bc1();

CREATE FUNCTION os_guard_9e3b6baed7a8cd275741() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    UPDATE index_catalog_state SET revision = revision + 1 WHERE singleton_id = 1 ; GET DIAGNOSTICS affected_rows = ROW_COUNT; IF affected_rows <> 1 THEN RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'index catalog tombstone accounting failed'; END IF;
    RETURN NEW;
END
$$;
CREATE TRIGGER index_catalog_after_tombstone_insert AFTER INSERT ON index_deletion_tombstones FOR EACH ROW EXECUTE FUNCTION os_guard_9e3b6baed7a8cd275741();

CREATE FUNCTION os_guard_ee7e2eea2ff1315189a5() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    IF EXISTS ( SELECT 1 FROM audit_tenant_state WHERE tenant_id = NEW . tenant_id ) THEN RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'audit tenant state already exists'; END IF;
    RETURN NEW;
END
$$;
CREATE TRIGGER audit_tenant_state_identity_collision_is_forbidden BEFORE INSERT ON audit_tenant_state FOR EACH ROW EXECUTE FUNCTION os_guard_ee7e2eea2ff1315189a5();

CREATE FUNCTION os_guard_8ead8c1ce0f9df51ccfc() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    IF NEW . event_count <> 0 OR NEW . next_sequence <> 1 THEN RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'audit tenant state must begin empty'; END IF;
    RETURN NEW;
END
$$;
CREATE TRIGGER audit_tenant_state_initial_shape_is_valid BEFORE INSERT ON audit_tenant_state FOR EACH ROW EXECUTE FUNCTION os_guard_8ead8c1ce0f9df51ccfc();

CREATE FUNCTION os_guard_376c91ab3fe7621b85cf() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'audit tenant state cannot be deleted';
    RETURN OLD;
END
$$;
CREATE TRIGGER audit_tenant_state_delete_is_forbidden BEFORE DELETE ON audit_tenant_state FOR EACH ROW EXECUTE FUNCTION os_guard_376c91ab3fe7621b85cf();

CREATE FUNCTION os_guard_bab4559047bfa99f091d() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    IF EXISTS ( SELECT 1 FROM search_attempt_audit_tenant_state WHERE tenant_id = NEW . tenant_id ) THEN RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'search-attempt audit tenant state already exists'; END IF;
    RETURN NEW;
END
$$;
CREATE TRIGGER search_attempt_audit_state_identity_collision_is_forbidden BEFORE INSERT ON search_attempt_audit_tenant_state FOR EACH ROW EXECUTE FUNCTION os_guard_bab4559047bfa99f091d();

CREATE FUNCTION os_guard_fa8113728f2f852a39be() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    IF NEW . first_sequence <> 1 OR NEW . next_sequence <> 1 OR NEW . retained_count <> 0 THEN RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'search-attempt audit tenant state must begin empty'; END IF;
    RETURN NEW;
END
$$;
CREATE TRIGGER search_attempt_audit_state_initial_shape_is_valid BEFORE INSERT ON search_attempt_audit_tenant_state FOR EACH ROW EXECUTE FUNCTION os_guard_fa8113728f2f852a39be();

CREATE FUNCTION os_guard_3832d073c147d8d32882() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    IF NOT ( NEW . tenant_id = OLD . tenant_id AND NEW . maximum_retained_attempts = OLD . maximum_retained_attempts AND ( ( OLD . retained_count BETWEEN 0 AND OLD . maximum_retained_attempts AND OLD . next_sequence BETWEEN 1 AND 9223372036854775806 AND NEW . first_sequence = OLD . first_sequence AND NEW . next_sequence = OLD . next_sequence + 1 AND NEW . retained_count = OLD . retained_count + 1 AND EXISTS ( SELECT 1 FROM search_attempt_audit_events WHERE tenant_id = NEW . tenant_id AND sequence = OLD . next_sequence ) ) OR ( OLD . retained_count = OLD . maximum_retained_attempts + 1 AND NEW . first_sequence = OLD . first_sequence + 1 AND NEW . next_sequence = OLD . next_sequence AND NEW . retained_count = OLD . retained_count - 1 AND NOT EXISTS ( SELECT 1 FROM search_attempt_audit_events WHERE tenant_id = NEW . tenant_id AND sequence = OLD . first_sequence ) ) ) ) THEN RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'search-attempt audit tenant state transition is invalid'; END IF;
    RETURN NEW;
END
$$;
CREATE TRIGGER search_attempt_audit_state_transition_is_valid BEFORE UPDATE ON search_attempt_audit_tenant_state FOR EACH ROW EXECUTE FUNCTION os_guard_3832d073c147d8d32882();

CREATE FUNCTION os_guard_1819beca35043f22d812() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'search-attempt audit tenant state cannot be deleted';
    RETURN OLD;
END
$$;
CREATE TRIGGER search_attempt_audit_state_delete_is_forbidden BEFORE DELETE ON search_attempt_audit_tenant_state FOR EACH ROW EXECUTE FUNCTION os_guard_1819beca35043f22d812();



CREATE FUNCTION os_guard_c384846bb5ce50f92a1d() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    IF NOT EXISTS ( SELECT 1 FROM search_attempt_audit_tenant_state WHERE tenant_id = NEW . tenant_id AND retained_count BETWEEN 0 AND maximum_retained_attempts AND next_sequence = NEW . sequence AND next_sequence BETWEEN 1 AND 9223372036854775806 ) THEN RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'search-attempt audit tenant state is invalid or sequence is exhausted'; END IF;
    RETURN NEW;
END
$$;
CREATE TRIGGER search_attempt_audit_event_insert_requires_current_state BEFORE INSERT ON search_attempt_audit_events FOR EACH ROW EXECUTE FUNCTION os_guard_c384846bb5ce50f92a1d();

-- Exact retained-sequence probes must remain index-backed as this table grows.
CREATE FUNCTION os_guard_2d57ac1f154f3bc5b7f7() RETURNS trigger LANGUAGE plpgsql SET enable_seqscan=off AS $$
DECLARE affected_rows BIGINT;
BEGIN
    UPDATE search_attempt_audit_tenant_state SET next_sequence = next_sequence + 1 , retained_count = retained_count + 1 WHERE tenant_id = NEW . tenant_id AND next_sequence = NEW . sequence AND retained_count BETWEEN 0 AND maximum_retained_attempts ; GET DIAGNOSTICS affected_rows = ROW_COUNT; IF affected_rows <> 1 THEN RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'search-attempt audit event accounting failed'; END IF; DELETE FROM search_attempt_audit_events WHERE tenant_id = NEW . tenant_id AND sequence = ( SELECT first_sequence FROM search_attempt_audit_tenant_state WHERE tenant_id = NEW . tenant_id AND retained_count = maximum_retained_attempts + 1 ) ;
    RETURN NEW;
END
$$;
CREATE TRIGGER search_attempt_audit_event_advances_and_prunes AFTER INSERT ON search_attempt_audit_events FOR EACH ROW EXECUTE FUNCTION os_guard_2d57ac1f154f3bc5b7f7();

CREATE FUNCTION os_guard_a13b56cf124a99298a2d() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    IF NOT EXISTS ( SELECT 1 FROM search_attempt_audit_tenant_state WHERE tenant_id = OLD . tenant_id AND first_sequence = OLD . sequence AND retained_count = maximum_retained_attempts + 1 ) THEN RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'search-attempt audit event deletion is not a rolling prune'; END IF;
    RETURN OLD;
END
$$;
CREATE TRIGGER search_attempt_audit_event_delete_requires_rolling_prune BEFORE DELETE ON search_attempt_audit_events FOR EACH ROW EXECUTE FUNCTION os_guard_a13b56cf124a99298a2d();

CREATE FUNCTION os_guard_aaa474cf591f452e8e57() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    UPDATE search_attempt_audit_tenant_state SET first_sequence = first_sequence + 1 , retained_count = retained_count - 1 WHERE tenant_id = OLD . tenant_id AND first_sequence = OLD . sequence AND retained_count = maximum_retained_attempts + 1 ; GET DIAGNOSTICS affected_rows = ROW_COUNT; IF affected_rows <> 1 THEN RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'search-attempt audit rolling prune accounting failed'; END IF;
    RETURN OLD;
END
$$;
CREATE TRIGGER search_attempt_audit_event_prune_advances_state AFTER DELETE ON search_attempt_audit_events FOR EACH ROW EXECUTE FUNCTION os_guard_aaa474cf591f452e8e57();

CREATE FUNCTION os_guard_eb9e62bdad4c8b61856f() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    IF EXISTS ( SELECT 1 FROM knowledge_catalog_tenants WHERE tenant_id = NEW . tenant_id ) THEN RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'knowledge catalog tenant already exists'; END IF;
    RETURN NEW;
END
$$;
CREATE TRIGGER knowledge_catalog_tenant_identity_collision_is_forbidden BEFORE INSERT ON knowledge_catalog_tenants FOR EACH ROW EXECUTE FUNCTION os_guard_eb9e62bdad4c8b61856f();

CREATE FUNCTION os_guard_a077db9c9e0b8aad420b() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    IF NEW . catalog_revision <> 0 OR NEW . identity_count <> 0 OR NEW . version_count <> 0 OR NEW . definition_body_bytes <> 0 OR NEW . idempotency_count <> 0 OR NEW . active_object_count <> 0 OR NEW . recovery_audit_count <> 0 THEN RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'knowledge catalog tenant must begin empty'; END IF;
    RETURN NEW;
END
$$;
CREATE TRIGGER knowledge_catalog_tenant_initial_shape_is_valid BEFORE INSERT ON knowledge_catalog_tenants FOR EACH ROW EXECUTE FUNCTION os_guard_a077db9c9e0b8aad420b();

CREATE FUNCTION os_guard_cc02885f89b1047f7fc7() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    IF NEW . catalog_revision <> OLD . catalog_revision AND NEW . catalog_revision <> OLD . catalog_revision + 1 THEN RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'knowledge catalog revision must advance by one'; END IF;
    RETURN NEW;
END
$$;
CREATE TRIGGER knowledge_catalog_revision_transition_is_valid BEFORE UPDATE OF catalog_revision ON knowledge_catalog_tenants FOR EACH ROW EXECUTE FUNCTION os_guard_cc02885f89b1047f7fc7();

CREATE FUNCTION os_guard_e5260564e5fc9e87f7f4() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    IF NEW . tenant_id <> OLD . tenant_id THEN RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'knowledge catalog tenant identity is immutable'; END IF;
    RETURN NEW;
END
$$;
CREATE TRIGGER knowledge_catalog_tenant_identity_is_immutable BEFORE UPDATE OF tenant_id ON knowledge_catalog_tenants FOR EACH ROW EXECUTE FUNCTION os_guard_e5260564e5fc9e87f7f4();

CREATE FUNCTION os_guard_d46a074bda305d357b19() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'knowledge catalog tenant cannot be deleted';
    RETURN OLD;
END
$$;
CREATE TRIGGER knowledge_catalog_tenant_delete_is_forbidden BEFORE DELETE ON knowledge_catalog_tenants FOR EACH ROW EXECUTE FUNCTION os_guard_d46a074bda305d357b19();

CREATE FUNCTION os_guard_481d48a13f41ad14746c() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    IF NOT EXISTS ( SELECT 1 FROM knowledge_catalog_tenants WHERE tenant_id = NEW . tenant_id AND definition_body_bytes <= 536870912 - NEW . definition_bytes ) THEN RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'knowledge definition body capacity exhausted'; END IF;
    RETURN NEW;
END
$$;
CREATE TRIGGER knowledge_definition_blob_capacity_is_available BEFORE INSERT ON knowledge_definition_blobs FOR EACH ROW EXECUTE FUNCTION os_guard_481d48a13f41ad14746c();

CREATE FUNCTION os_guard_39a0db499616bd6573a4() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    IF EXISTS ( SELECT 1 FROM knowledge_definition_blobs WHERE tenant_id = NEW . tenant_id AND definition_digest = NEW . definition_digest ) THEN RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'knowledge definition BYTEA already exists'; END IF;
    RETURN NEW;
END
$$;
CREATE TRIGGER knowledge_definition_blob_identity_collision_is_forbidden BEFORE INSERT ON knowledge_definition_blobs FOR EACH ROW EXECUTE FUNCTION os_guard_39a0db499616bd6573a4();

CREATE FUNCTION os_guard_6d139f87534db967891d() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    UPDATE knowledge_catalog_tenants SET definition_body_bytes = definition_body_bytes + NEW . definition_bytes WHERE tenant_id = NEW . tenant_id ;
    RETURN NEW;
END
$$;
CREATE TRIGGER knowledge_definition_blob_after_insert AFTER INSERT ON knowledge_definition_blobs FOR EACH ROW EXECUTE FUNCTION os_guard_6d139f87534db967891d();

CREATE FUNCTION os_guard_9665d07fa63933b5c5f7() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'knowledge definition BYTEA is immutable';
    RETURN NEW;
END
$$;
CREATE TRIGGER knowledge_definition_blob_update_is_forbidden BEFORE UPDATE ON knowledge_definition_blobs FOR EACH ROW EXECUTE FUNCTION os_guard_9665d07fa63933b5c5f7();

CREATE FUNCTION os_guard_d01b1a0a60755d0766b4() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'knowledge definition BYTEA cannot be deleted';
    RETURN OLD;
END
$$;
CREATE TRIGGER knowledge_definition_blob_delete_is_forbidden BEFORE DELETE ON knowledge_definition_blobs FOR EACH ROW EXECUTE FUNCTION os_guard_d01b1a0a60755d0766b4();

CREATE FUNCTION os_guard_65ca3ccb32d35c0eccd1() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    IF NOT EXISTS ( SELECT 1 FROM knowledge_catalog_tenants WHERE tenant_id = NEW . tenant_id AND identity_count < 8192 ) THEN RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'knowledge object identity capacity exhausted'; END IF;
    RETURN NEW;
END
$$;
CREATE TRIGGER knowledge_object_identity_capacity_is_available BEFORE INSERT ON knowledge_objects FOR EACH ROW EXECUTE FUNCTION os_guard_65ca3ccb32d35c0eccd1();

CREATE FUNCTION os_guard_1c6239fafae76056f49c() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    IF NEW . state = 'active' AND NOT EXISTS ( SELECT 1 FROM app_workspaces WHERE tenant_id = NEW . tenant_id AND app_id = NEW . app_id AND state = 'active' ) THEN RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'active knowledge object requires active app'; END IF;
    RETURN NEW;
END
$$;
CREATE TRIGGER knowledge_object_active_app_is_required_insert BEFORE INSERT ON knowledge_objects FOR EACH ROW EXECUTE FUNCTION os_guard_1c6239fafae76056f49c();

CREATE FUNCTION os_guard_4121d5a42edf4fac47c4() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    IF NEW . state = 'active' AND NOT EXISTS ( SELECT 1 FROM app_workspaces WHERE tenant_id = NEW . tenant_id AND app_id = NEW . app_id AND state = 'active' ) THEN RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'active knowledge object requires active app'; END IF;
    RETURN NEW;
END
$$;
CREATE TRIGGER knowledge_object_active_app_is_required_update BEFORE UPDATE ON knowledge_objects FOR EACH ROW EXECUTE FUNCTION os_guard_4121d5a42edf4fac47c4();

CREATE FUNCTION os_guard_ef3153c022fd34a09b15() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    IF EXISTS ( SELECT 1 FROM knowledge_objects WHERE tenant_id = NEW . tenant_id AND knowledge_object_id = NEW . knowledge_object_id ) THEN RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'knowledge object identity already exists'; END IF;
    RETURN NEW;
END
$$;
CREATE TRIGGER knowledge_object_identity_collision_is_forbidden BEFORE INSERT ON knowledge_objects FOR EACH ROW EXECUTE FUNCTION os_guard_ef3153c022fd34a09b15();

CREATE FUNCTION os_guard_34212c266b222fe12af9() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    IF NEW . state = 'active' AND EXISTS ( SELECT 1 FROM knowledge_objects AS existing WHERE existing . tenant_id = NEW . tenant_id AND existing.organization_id=NEW.organization_id AND existing . state = 'active' AND existing . object_type = NEW . object_type AND existing . name = NEW . name AND ( ( NEW . sharing_scope = 'private' AND existing . sharing_scope = 'private' AND existing . app_id = NEW . app_id AND existing . owner_id = NEW . owner_id ) OR ( NEW . sharing_scope = 'app' AND existing . sharing_scope = 'app' AND existing . app_id = NEW . app_id ) OR ( NEW . sharing_scope = 'global' AND existing . sharing_scope = 'global' ) ) ) THEN RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'knowledge active name already exists'; END IF;
    RETURN NEW;
END
$$;
CREATE TRIGGER knowledge_object_active_name_collision_is_forbidden BEFORE INSERT ON knowledge_objects FOR EACH ROW EXECUTE FUNCTION os_guard_34212c266b222fe12af9();

CREATE FUNCTION os_guard_fe7f95b0a95136c3b42e() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    IF NEW . state = 'active' AND EXISTS ( SELECT 1 FROM knowledge_objects AS existing WHERE existing . tenant_id = NEW . tenant_id AND existing.organization_id=NEW.organization_id AND existing . knowledge_object_id <> OLD . knowledge_object_id AND existing . state = 'active' AND existing . object_type = NEW . object_type AND existing . name = NEW . name AND ( ( NEW . sharing_scope = 'private' AND existing . sharing_scope = 'private' AND existing . app_id = NEW . app_id AND existing . owner_id = NEW . owner_id ) OR ( NEW . sharing_scope = 'app' AND existing . sharing_scope = 'app' AND existing . app_id = NEW . app_id ) OR ( NEW . sharing_scope = 'global' AND existing . sharing_scope = 'global' ) ) ) THEN RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'knowledge active name already exists'; END IF;
    RETURN NEW;
END
$$;
CREATE TRIGGER knowledge_object_active_name_update_collision_is_forbidden BEFORE UPDATE ON knowledge_objects FOR EACH ROW EXECUTE FUNCTION os_guard_fe7f95b0a95136c3b42e();

CREATE FUNCTION os_guard_57dc365cf52c927b5cfa() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    UPDATE knowledge_catalog_tenants SET identity_count = identity_count + 1 WHERE tenant_id = NEW . tenant_id ;
    RETURN NEW;
END
$$;
CREATE TRIGGER knowledge_object_after_insert_count_identity AFTER INSERT ON knowledge_objects FOR EACH ROW EXECUTE FUNCTION os_guard_57dc365cf52c927b5cfa();

CREATE FUNCTION os_guard_61eb588c4c6380ed4acd() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    IF NOT ( NEW . tenant_id = OLD . tenant_id AND NEW . knowledge_object_id = OLD . knowledge_object_id AND NEW . created_at_unix_micro = OLD . created_at_unix_micro AND NEW . current_version = OLD . current_version + 1 AND NEW . updated_at_unix_micro >= OLD . updated_at_unix_micro AND OLD . state NOT IN ( 'quarantined' , 'deleted' ) ) THEN RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'knowledge object registry transition is invalid'; END IF;
    RETURN NEW;
END
$$;
CREATE TRIGGER knowledge_object_registry_transition_is_valid BEFORE UPDATE ON knowledge_objects FOR EACH ROW EXECUTE FUNCTION os_guard_61eb588c4c6380ed4acd();

CREATE FUNCTION os_guard_c9660b9d395aa77e9722() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'knowledge object registry row cannot be deleted';
    RETURN OLD;
END
$$;
CREATE TRIGGER knowledge_object_delete_is_forbidden BEFORE DELETE ON knowledge_objects FOR EACH ROW EXECUTE FUNCTION os_guard_c9660b9d395aa77e9722();

CREATE FUNCTION os_guard_3e0d69a3ede2e10b3d51() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    IF NOT EXISTS ( SELECT 1 FROM knowledge_catalog_tenants WHERE tenant_id = NEW . tenant_id AND ( ( NEW . mutation_kind = 'quarantine' AND version_count < 65536 ) OR ( NEW . mutation_kind <> 'quarantine' AND version_count < 61440 ) ) ) THEN RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'knowledge object version capacity exhausted'; END IF;
    RETURN NEW;
END
$$;
CREATE TRIGGER knowledge_object_version_capacity_is_available BEFORE INSERT ON knowledge_object_versions FOR EACH ROW EXECUTE FUNCTION os_guard_3e0d69a3ede2e10b3d51();

CREATE FUNCTION os_guard_967b534d6a06d78d84cd() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    IF EXISTS ( SELECT 1 FROM knowledge_object_versions WHERE tenant_id = NEW . tenant_id AND knowledge_object_id = NEW . knowledge_object_id AND object_version = NEW . object_version ) THEN RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'knowledge object version already exists'; END IF;
    RETURN NEW;
END
$$;
CREATE TRIGGER knowledge_object_version_identity_collision_is_forbidden BEFORE INSERT ON knowledge_object_versions FOR EACH ROW EXECUTE FUNCTION os_guard_967b534d6a06d78d84cd();

CREATE FUNCTION os_guard_58057a8886e3add10238() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    IF NEW . object_version > 1 AND NOT EXISTS ( SELECT 1 FROM knowledge_object_versions WHERE tenant_id = NEW . tenant_id AND knowledge_object_id = NEW . knowledge_object_id AND object_version = NEW . object_version - 1 ) THEN RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'knowledge object version must be contiguous'; END IF;
    RETURN NEW;
END
$$;
CREATE TRIGGER knowledge_object_version_is_contiguous BEFORE INSERT ON knowledge_object_versions FOR EACH ROW EXECUTE FUNCTION os_guard_58057a8886e3add10238();

CREATE FUNCTION os_guard_05892236042c739f5534() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    UPDATE knowledge_catalog_tenants SET version_count = version_count + 1 WHERE tenant_id = NEW . tenant_id ;
    RETURN NEW;
END
$$;
CREATE TRIGGER knowledge_object_version_after_insert AFTER INSERT ON knowledge_object_versions FOR EACH ROW EXECUTE FUNCTION os_guard_05892236042c739f5534();

CREATE FUNCTION os_guard_4bea1376aa8a26fde573() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'knowledge object version is immutable';
    RETURN NEW;
END
$$;
CREATE TRIGGER knowledge_object_version_update_is_forbidden BEFORE UPDATE ON knowledge_object_versions FOR EACH ROW EXECUTE FUNCTION os_guard_4bea1376aa8a26fde573();

CREATE FUNCTION os_guard_16c7ae086466189ba2c4() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'knowledge object version cannot be deleted';
    RETURN OLD;
END
$$;
CREATE TRIGGER knowledge_object_version_delete_is_forbidden BEFORE DELETE ON knowledge_object_versions FOR EACH ROW EXECUTE FUNCTION os_guard_16c7ae086466189ba2c4();

CREATE FUNCTION os_guard_75c918bf0c34a5d58658() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    IF NOT EXISTS ( SELECT 1 FROM knowledge_object_versions WHERE tenant_id = NEW . tenant_id AND knowledge_object_id = NEW . source_object_id AND object_version = NEW . source_object_version AND NEW . ordinal < dependency_count ) THEN RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'knowledge dependency ordinal exceeds declared count'; END IF;
    RETURN NEW;
END
$$;
CREATE TRIGGER knowledge_dependency_ordinal_is_declared BEFORE INSERT ON knowledge_object_dependencies FOR EACH ROW EXECUTE FUNCTION os_guard_75c918bf0c34a5d58658();

CREATE FUNCTION os_guard_6ca57bed13e53ec4c23e() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    IF EXISTS ( SELECT 1 FROM knowledge_object_dependency_seals WHERE tenant_id = NEW . tenant_id AND knowledge_object_id = NEW . source_object_id AND object_version = NEW . source_object_version ) THEN RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'knowledge dependency set is sealed'; END IF;
    RETURN NEW;
END
$$;
CREATE TRIGGER knowledge_dependency_sealed_version_is_immutable BEFORE INSERT ON knowledge_object_dependencies FOR EACH ROW EXECUTE FUNCTION os_guard_6ca57bed13e53ec4c23e();

CREATE FUNCTION os_guard_19b807db56f44f45283b() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    IF EXISTS ( SELECT 1 FROM knowledge_object_dependencies WHERE tenant_id = NEW . tenant_id AND source_object_id = NEW . source_object_id AND source_object_version = NEW . source_object_version AND ( ordinal = NEW . ordinal OR ( target_kind = NEW . target_kind AND target_object_id = NEW . target_object_id AND target_object_version = NEW . target_object_version AND dependency_role = NEW . dependency_role ) ) ) THEN RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'knowledge dependency already exists'; END IF;
    RETURN NEW;
END
$$;
CREATE TRIGGER knowledge_dependency_identity_collision_is_forbidden BEFORE INSERT ON knowledge_object_dependencies FOR EACH ROW EXECUTE FUNCTION os_guard_19b807db56f44f45283b();

CREATE FUNCTION os_guard_46d0dfcc4bbc3e6a0939() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    IF EXISTS ( SELECT 1 FROM knowledge_object_acl WHERE tenant_id = NEW . tenant_id AND knowledge_object_id = NEW . knowledge_object_id AND role_id = NEW . role_id ) THEN RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'knowledge object ACL already exists'; END IF;
    RETURN NEW;
END
$$;
CREATE TRIGGER knowledge_object_acl_identity_collision_is_forbidden BEFORE INSERT ON knowledge_object_acl FOR EACH ROW EXECUTE FUNCTION os_guard_46d0dfcc4bbc3e6a0939();

CREATE FUNCTION os_guard_7d8001dc97157e23a893() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    IF EXISTS ( SELECT 1 FROM knowledge_app_active_counters WHERE tenant_id = NEW . tenant_id AND app_id = NEW . app_id ) THEN RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'knowledge app active counter already exists'; END IF;
    RETURN NEW;
END
$$;
CREATE TRIGGER knowledge_app_active_counter_identity_collision_is_forbidden BEFORE INSERT ON knowledge_app_active_counters FOR EACH ROW EXECUTE FUNCTION os_guard_7d8001dc97157e23a893();

CREATE FUNCTION os_guard_26fa3c703b3a86352aa4() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    IF NEW . tenant_id <> OLD . tenant_id OR NEW . app_id <> OLD . app_id THEN RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'knowledge app active counter identity is immutable'; END IF;
    RETURN NEW;
END
$$;
CREATE TRIGGER knowledge_app_active_counter_identity_is_immutable BEFORE UPDATE OF tenant_id, app_id ON knowledge_app_active_counters FOR EACH ROW EXECUTE FUNCTION os_guard_26fa3c703b3a86352aa4();

CREATE FUNCTION os_guard_f1f1bf65d48ff8545e74() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'knowledge app active counter cannot be deleted';
    RETURN OLD;
END
$$;
CREATE TRIGGER knowledge_app_active_counter_delete_is_forbidden BEFORE DELETE ON knowledge_app_active_counters FOR EACH ROW EXECUTE FUNCTION os_guard_f1f1bf65d48ff8545e74();

CREATE FUNCTION os_guard_cd7acbba64b82e8ac274() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    IF EXISTS ( SELECT 1 FROM knowledge_owner_active_counters WHERE tenant_id = NEW . tenant_id AND owner_id = NEW . owner_id ) THEN RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'knowledge owner active counter already exists'; END IF;
    RETURN NEW;
END
$$;
CREATE TRIGGER knowledge_owner_active_counter_identity_collision_is_forbidden BEFORE INSERT ON knowledge_owner_active_counters FOR EACH ROW EXECUTE FUNCTION os_guard_cd7acbba64b82e8ac274();

CREATE FUNCTION os_guard_a7810e963598f5bbb39d() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    IF NEW . tenant_id <> OLD . tenant_id OR NEW . owner_id <> OLD . owner_id THEN RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'knowledge owner active counter identity is immutable'; END IF;
    RETURN NEW;
END
$$;
CREATE TRIGGER knowledge_owner_active_counter_identity_is_immutable BEFORE UPDATE OF tenant_id, owner_id ON knowledge_owner_active_counters FOR EACH ROW EXECUTE FUNCTION os_guard_a7810e963598f5bbb39d();

CREATE FUNCTION os_guard_8a9dba58ff2267de7486() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'knowledge owner active counter cannot be deleted';
    RETURN OLD;
END
$$;
CREATE TRIGGER knowledge_owner_active_counter_delete_is_forbidden BEFORE DELETE ON knowledge_owner_active_counters FOR EACH ROW EXECUTE FUNCTION os_guard_8a9dba58ff2267de7486();

CREATE FUNCTION os_guard_2b997b33fcfcaa0bdbd2() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    IF EXISTS ( SELECT 1 FROM knowledge_type_active_counters WHERE tenant_id = NEW . tenant_id AND object_type = NEW . object_type ) THEN RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'knowledge type active counter already exists'; END IF;
    RETURN NEW;
END
$$;
CREATE TRIGGER knowledge_type_active_counter_identity_collision_is_forbidden BEFORE INSERT ON knowledge_type_active_counters FOR EACH ROW EXECUTE FUNCTION os_guard_2b997b33fcfcaa0bdbd2();

CREATE FUNCTION os_guard_b1dcbb23b289fd7b5756() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    IF NEW . tenant_id <> OLD . tenant_id OR NEW . object_type <> OLD . object_type THEN RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'knowledge type active counter identity is immutable'; END IF;
    RETURN NEW;
END
$$;
CREATE TRIGGER knowledge_type_active_counter_identity_is_immutable BEFORE UPDATE OF tenant_id, object_type ON knowledge_type_active_counters FOR EACH ROW EXECUTE FUNCTION os_guard_b1dcbb23b289fd7b5756();

CREATE FUNCTION os_guard_595d3b2495e1589f67a8() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'knowledge type active counter cannot be deleted';
    RETURN OLD;
END
$$;
CREATE TRIGGER knowledge_type_active_counter_delete_is_forbidden BEFORE DELETE ON knowledge_type_active_counters FOR EACH ROW EXECUTE FUNCTION os_guard_595d3b2495e1589f67a8();

CREATE FUNCTION os_guard_724890f51fb127eea2a1() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    IF EXISTS ( SELECT 1 FROM knowledge_app_type_active_counters WHERE tenant_id = NEW . tenant_id AND app_id = NEW . app_id AND object_type = NEW . object_type ) THEN RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'knowledge app/type active counter already exists'; END IF;
    RETURN NEW;
END
$$;
CREATE TRIGGER knowledge_app_type_active_counter_identity_724890f51fb127eea2a1 BEFORE INSERT ON knowledge_app_type_active_counters FOR EACH ROW EXECUTE FUNCTION os_guard_724890f51fb127eea2a1();

CREATE FUNCTION os_guard_fb162f7705f3ca06faa9() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    IF NEW . tenant_id <> OLD . tenant_id OR NEW . app_id <> OLD . app_id OR NEW . object_type <> OLD . object_type THEN RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'knowledge app/type active counter identity is immutable'; END IF;
    RETURN NEW;
END
$$;
CREATE TRIGGER knowledge_app_type_active_counter_identity_is_immutable BEFORE UPDATE OF tenant_id, app_id, object_type ON knowledge_app_type_active_counters FOR EACH ROW EXECUTE FUNCTION os_guard_fb162f7705f3ca06faa9();

CREATE FUNCTION os_guard_3f5ddc10914f3b061d9d() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'knowledge app/type active counter cannot be deleted';
    RETURN OLD;
END
$$;
CREATE TRIGGER knowledge_app_type_active_counter_delete_is_forbidden BEFORE DELETE ON knowledge_app_type_active_counters FOR EACH ROW EXECUTE FUNCTION os_guard_3f5ddc10914f3b061d9d();

CREATE FUNCTION os_guard_6f80672f92ff9cadcced() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'knowledge object dependency is immutable';
    RETURN NEW;
END
$$;
CREATE TRIGGER knowledge_dependency_update_is_forbidden BEFORE UPDATE ON knowledge_object_dependencies FOR EACH ROW EXECUTE FUNCTION os_guard_6f80672f92ff9cadcced();

CREATE FUNCTION os_guard_75870006849bf7303208() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'knowledge object dependency cannot be deleted';
    RETURN OLD;
END
$$;
CREATE TRIGGER knowledge_dependency_delete_is_forbidden BEFORE DELETE ON knowledge_object_dependencies FOR EACH ROW EXECUTE FUNCTION os_guard_75870006849bf7303208();

CREATE FUNCTION os_guard_7445b86433429aef3e64() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    IF EXISTS ( SELECT 1 FROM knowledge_object_dependency_seals WHERE tenant_id = NEW . tenant_id AND knowledge_object_id = NEW . knowledge_object_id AND object_version = NEW . object_version ) THEN RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'knowledge dependency seal already exists'; END IF;
    RETURN NEW;
END
$$;
CREATE TRIGGER knowledge_dependency_seal_identity_collision_is_forbidden BEFORE INSERT ON knowledge_object_dependency_seals FOR EACH ROW EXECUTE FUNCTION os_guard_7445b86433429aef3e64();

CREATE FUNCTION os_guard_c328ea5f689712e79068() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    IF NEW . dependency_count <> ( SELECT count ( * ) FROM knowledge_object_dependencies WHERE tenant_id = NEW . tenant_id AND source_object_id = NEW . knowledge_object_id AND source_object_version = NEW . object_version ) THEN RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'knowledge dependency set is incomplete'; END IF;
    RETURN NEW;
END
$$;
CREATE TRIGGER knowledge_dependency_seal_is_complete BEFORE INSERT ON knowledge_object_dependency_seals FOR EACH ROW EXECUTE FUNCTION os_guard_c328ea5f689712e79068();

CREATE FUNCTION os_guard_5aa69d7195c2d9f24f85() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'knowledge dependency seal is immutable';
    RETURN NEW;
END
$$;
CREATE TRIGGER knowledge_dependency_seal_update_is_forbidden BEFORE UPDATE ON knowledge_object_dependency_seals FOR EACH ROW EXECUTE FUNCTION os_guard_5aa69d7195c2d9f24f85();

CREATE FUNCTION os_guard_310f84981f8dd62bb24f() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'knowledge dependency seal cannot be deleted';
    RETURN OLD;
END
$$;
CREATE TRIGGER knowledge_dependency_seal_delete_is_forbidden BEFORE DELETE ON knowledge_object_dependency_seals FOR EACH ROW EXECUTE FUNCTION os_guard_310f84981f8dd62bb24f();

CREATE FUNCTION os_guard_c1311a3155a40ee77f69() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    IF NEW . state = 'active' AND ( ( SELECT active_object_count FROM knowledge_catalog_tenants WHERE tenant_id = NEW . tenant_id ) >= 4096 OR COALESCE ( ( SELECT active_object_count FROM knowledge_app_active_counters WHERE tenant_id = NEW . tenant_id AND app_id = NEW . app_id ) , 0 ) >= 1024 OR COALESCE ( ( SELECT active_object_count FROM knowledge_type_active_counters WHERE tenant_id = NEW . tenant_id AND object_type = NEW . object_type ) , 0 ) >= 2048 OR COALESCE ( ( SELECT active_object_count FROM knowledge_app_type_active_counters WHERE tenant_id = NEW . tenant_id AND app_id = NEW . app_id AND object_type = NEW . object_type ) , 0 ) >= 512 OR ( NEW . sharing_scope = 'private' AND COALESCE ( ( SELECT active_private_object_count FROM knowledge_owner_active_counters WHERE tenant_id = NEW . tenant_id AND owner_id = NEW . owner_id ) , 0 ) >= 512 ) ) THEN RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'knowledge active object capacity exhausted'; END IF;
    RETURN NEW;
END
$$;
CREATE TRIGGER knowledge_object_active_capacity_insert BEFORE INSERT ON knowledge_objects FOR EACH ROW EXECUTE FUNCTION os_guard_c1311a3155a40ee77f69();

CREATE FUNCTION os_guard_693d51cb5fd6279467a8() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    IF NEW . state = 'active' AND ( ( OLD . state <> 'active' AND ( SELECT active_object_count FROM knowledge_catalog_tenants WHERE tenant_id = NEW . tenant_id ) >= 4096 ) OR ( ( OLD . state <> 'active' OR OLD . app_id <> NEW . app_id ) AND COALESCE ( ( SELECT active_object_count FROM knowledge_app_active_counters WHERE tenant_id = NEW . tenant_id AND app_id = NEW . app_id ) , 0 ) >= 1024 ) OR ( ( OLD . state <> 'active' OR OLD . object_type <> NEW . object_type ) AND COALESCE ( ( SELECT active_object_count FROM knowledge_type_active_counters WHERE tenant_id = NEW . tenant_id AND object_type = NEW . object_type ) , 0 ) >= 2048 ) OR ( ( OLD . state <> 'active' OR OLD . app_id <> NEW . app_id OR OLD . object_type <> NEW . object_type ) AND COALESCE ( ( SELECT active_object_count FROM knowledge_app_type_active_counters WHERE tenant_id = NEW . tenant_id AND app_id = NEW . app_id AND object_type = NEW . object_type ) , 0 ) >= 512 ) OR ( NEW . sharing_scope = 'private' AND ( OLD . state <> 'active' OR OLD . sharing_scope <> 'private' OR OLD . owner_id <> NEW . owner_id ) AND COALESCE ( ( SELECT active_private_object_count FROM knowledge_owner_active_counters WHERE tenant_id = NEW . tenant_id AND owner_id = NEW . owner_id ) , 0 ) >= 512 ) ) THEN RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'knowledge active object capacity exhausted'; END IF;
    RETURN NEW;
END
$$;
CREATE TRIGGER knowledge_object_active_capacity_update BEFORE UPDATE ON knowledge_objects FOR EACH ROW EXECUTE FUNCTION os_guard_693d51cb5fd6279467a8();

CREATE FUNCTION os_guard_55b8a382b3d38b6f8a71() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    IF NEW . state = 'active' THEN UPDATE knowledge_catalog_tenants SET active_object_count = active_object_count + 1 WHERE tenant_id = NEW . tenant_id ; UPDATE knowledge_app_active_counters SET active_object_count = active_object_count + 1 WHERE tenant_id = NEW . tenant_id AND app_id = NEW . app_id ; INSERT INTO knowledge_app_active_counters ( tenant_id , app_id , active_object_count ) SELECT NEW . tenant_id , NEW . app_id , 1 WHERE NOT EXISTS ( SELECT 1 FROM knowledge_app_active_counters WHERE tenant_id = NEW . tenant_id AND app_id = NEW . app_id ) ; UPDATE knowledge_type_active_counters SET active_object_count = active_object_count + 1 WHERE tenant_id = NEW . tenant_id AND object_type = NEW . object_type ; INSERT INTO knowledge_type_active_counters ( tenant_id , object_type , active_object_count ) SELECT NEW . tenant_id , NEW . object_type , 1 WHERE NOT EXISTS ( SELECT 1 FROM knowledge_type_active_counters WHERE tenant_id = NEW . tenant_id AND object_type = NEW . object_type ) ; UPDATE knowledge_app_type_active_counters SET active_object_count = active_object_count + 1 WHERE tenant_id = NEW . tenant_id AND app_id = NEW . app_id AND object_type = NEW . object_type ; INSERT INTO knowledge_app_type_active_counters ( tenant_id , app_id , object_type , active_object_count ) SELECT NEW . tenant_id , NEW . app_id , NEW . object_type , 1 WHERE NOT EXISTS ( SELECT 1 FROM knowledge_app_type_active_counters WHERE tenant_id = NEW . tenant_id AND app_id = NEW . app_id AND object_type = NEW . object_type ) ; UPDATE knowledge_owner_active_counters SET active_private_object_count = active_private_object_count + 1 WHERE NEW . sharing_scope = 'private' AND tenant_id = NEW . tenant_id AND owner_id = NEW . owner_id ; INSERT INTO knowledge_owner_active_counters ( tenant_id , owner_id , active_private_object_count ) SELECT NEW . tenant_id , NEW . owner_id , 1 WHERE NEW . sharing_scope = 'private' AND NOT EXISTS ( SELECT 1 FROM knowledge_owner_active_counters WHERE tenant_id = NEW . tenant_id AND owner_id = NEW . owner_id ) ; END IF;
    RETURN NEW;
END
$$;
CREATE TRIGGER knowledge_object_active_counters_after_insert AFTER INSERT ON knowledge_objects FOR EACH ROW EXECUTE FUNCTION os_guard_55b8a382b3d38b6f8a71();

CREATE FUNCTION os_guard_3cf06bd9e52b9eacc481() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    IF OLD . state = 'active' THEN UPDATE knowledge_catalog_tenants SET active_object_count = active_object_count - 1 WHERE tenant_id = OLD . tenant_id ; UPDATE knowledge_app_active_counters SET active_object_count = active_object_count - 1 WHERE tenant_id = OLD . tenant_id AND app_id = OLD . app_id ; UPDATE knowledge_type_active_counters SET active_object_count = active_object_count - 1 WHERE tenant_id = OLD . tenant_id AND object_type = OLD . object_type ; UPDATE knowledge_app_type_active_counters SET active_object_count = active_object_count - 1 WHERE tenant_id = OLD . tenant_id AND app_id = OLD . app_id AND object_type = OLD . object_type ; UPDATE knowledge_owner_active_counters SET active_private_object_count = active_private_object_count - 1 WHERE OLD . sharing_scope = 'private' AND tenant_id = OLD . tenant_id AND owner_id = OLD . owner_id ; END IF;
    RETURN NEW;
END
$$;
CREATE TRIGGER knowledge_object_active_counters_before_update BEFORE UPDATE ON knowledge_objects FOR EACH ROW EXECUTE FUNCTION os_guard_3cf06bd9e52b9eacc481();

CREATE FUNCTION os_guard_b50fe1a5e6c569edddee() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    IF NEW . state = 'active' THEN UPDATE knowledge_catalog_tenants SET active_object_count = active_object_count + 1 WHERE tenant_id = NEW . tenant_id ; UPDATE knowledge_app_active_counters SET active_object_count = active_object_count + 1 WHERE tenant_id = NEW . tenant_id AND app_id = NEW . app_id ; INSERT INTO knowledge_app_active_counters ( tenant_id , app_id , active_object_count ) SELECT NEW . tenant_id , NEW . app_id , 1 WHERE NOT EXISTS ( SELECT 1 FROM knowledge_app_active_counters WHERE tenant_id = NEW . tenant_id AND app_id = NEW . app_id ) ; UPDATE knowledge_type_active_counters SET active_object_count = active_object_count + 1 WHERE tenant_id = NEW . tenant_id AND object_type = NEW . object_type ; INSERT INTO knowledge_type_active_counters ( tenant_id , object_type , active_object_count ) SELECT NEW . tenant_id , NEW . object_type , 1 WHERE NOT EXISTS ( SELECT 1 FROM knowledge_type_active_counters WHERE tenant_id = NEW . tenant_id AND object_type = NEW . object_type ) ; UPDATE knowledge_app_type_active_counters SET active_object_count = active_object_count + 1 WHERE tenant_id = NEW . tenant_id AND app_id = NEW . app_id AND object_type = NEW . object_type ; INSERT INTO knowledge_app_type_active_counters ( tenant_id , app_id , object_type , active_object_count ) SELECT NEW . tenant_id , NEW . app_id , NEW . object_type , 1 WHERE NOT EXISTS ( SELECT 1 FROM knowledge_app_type_active_counters WHERE tenant_id = NEW . tenant_id AND app_id = NEW . app_id AND object_type = NEW . object_type ) ; UPDATE knowledge_owner_active_counters SET active_private_object_count = active_private_object_count + 1 WHERE NEW . sharing_scope = 'private' AND tenant_id = NEW . tenant_id AND owner_id = NEW . owner_id ; INSERT INTO knowledge_owner_active_counters ( tenant_id , owner_id , active_private_object_count ) SELECT NEW . tenant_id , NEW . owner_id , 1 WHERE NEW . sharing_scope = 'private' AND NOT EXISTS ( SELECT 1 FROM knowledge_owner_active_counters WHERE tenant_id = NEW . tenant_id AND owner_id = NEW . owner_id ) ; END IF;
    RETURN NEW;
END
$$;
CREATE TRIGGER knowledge_object_active_counters_after_update AFTER UPDATE ON knowledge_objects FOR EACH ROW EXECUTE FUNCTION os_guard_b50fe1a5e6c569edddee();

CREATE FUNCTION os_guard_743e2ab6f0acaeb24fcd() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    IF NOT EXISTS ( SELECT 1 FROM knowledge_catalog_tenants WHERE tenant_id = NEW . tenant_id AND recovery_audit_count < 8192 AND NEW . sequence = recovery_audit_count + 1 ) THEN RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'knowledge recovery audit capacity or sequence invalid'; END IF;
    RETURN NEW;
END
$$;
CREATE TRIGGER knowledge_recovery_audit_capacity_is_available BEFORE INSERT ON knowledge_recovery_audit FOR EACH ROW EXECUTE FUNCTION os_guard_743e2ab6f0acaeb24fcd();

CREATE FUNCTION os_guard_9fbea005d73c55c77912() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    IF EXISTS ( SELECT 1 FROM knowledge_recovery_audit WHERE tenant_id = NEW . tenant_id AND ( sequence = NEW . sequence OR knowledge_object_id = NEW . knowledge_object_id ) ) THEN RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'knowledge recovery audit identity already exists'; END IF;
    RETURN NEW;
END
$$;
CREATE TRIGGER knowledge_recovery_audit_identity_collision_is_forbidden BEFORE INSERT ON knowledge_recovery_audit FOR EACH ROW EXECUTE FUNCTION os_guard_9fbea005d73c55c77912();

CREATE FUNCTION os_guard_cbf65a4b555366ca22c2() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    IF NOT EXISTS ( SELECT 1 FROM knowledge_object_versions WHERE tenant_id = NEW . tenant_id AND knowledge_object_id = NEW . knowledge_object_id AND object_version = NEW . object_version AND state = 'quarantined' AND app_id = NEW . app_id AND object_type = NEW . object_type AND sharing_scope = NEW . sharing_scope AND quarantine_reason = NEW . recovery_reason AND EXISTS ( SELECT 1 FROM knowledge_objects WHERE tenant_id = NEW . tenant_id AND knowledge_object_id = NEW . knowledge_object_id AND current_version = NEW . object_version AND state = 'quarantined' AND app_id = NEW . app_id AND object_type = NEW . object_type AND sharing_scope = NEW . sharing_scope AND quarantine_reason = NEW . recovery_reason ) ) THEN RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'knowledge recovery audit does not match terminal version'; END IF;
    RETURN NEW;
END
$$;
CREATE TRIGGER knowledge_recovery_audit_matches_terminal_version BEFORE INSERT ON knowledge_recovery_audit FOR EACH ROW EXECUTE FUNCTION os_guard_cbf65a4b555366ca22c2();

CREATE FUNCTION os_guard_bf3f240d9b22d740049e() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    UPDATE knowledge_catalog_tenants SET recovery_audit_count = recovery_audit_count + 1 WHERE tenant_id = NEW . tenant_id ;
    RETURN NEW;
END
$$;
CREATE TRIGGER knowledge_recovery_audit_after_insert AFTER INSERT ON knowledge_recovery_audit FOR EACH ROW EXECUTE FUNCTION os_guard_bf3f240d9b22d740049e();

CREATE FUNCTION os_guard_aa800829a9ba4710e457() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'knowledge recovery audit is immutable';
    RETURN NEW;
END
$$;
CREATE TRIGGER knowledge_recovery_audit_update_is_forbidden BEFORE UPDATE ON knowledge_recovery_audit FOR EACH ROW EXECUTE FUNCTION os_guard_aa800829a9ba4710e457();

CREATE FUNCTION os_guard_1f60c8057e441aac02b2() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'knowledge recovery audit cannot be deleted';
    RETURN OLD;
END
$$;
CREATE TRIGGER knowledge_recovery_audit_delete_is_forbidden BEFORE DELETE ON knowledge_recovery_audit FOR EACH ROW EXECUTE FUNCTION os_guard_1f60c8057e441aac02b2();

CREATE FUNCTION os_guard_21e9682f212c59db4c18() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    IF EXISTS ( SELECT 1 FROM knowledge_objects WHERE tenant_id = OLD . tenant_id AND app_id = OLD . app_id ) THEN RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'app workspace is referenced by knowledge objects'; END IF;
    RETURN OLD;
END
$$;
CREATE TRIGGER knowledge_referenced_app_workspace_cannot_be_deleted BEFORE DELETE ON app_workspaces FOR EACH ROW EXECUTE FUNCTION os_guard_21e9682f212c59db4c18();

CREATE FUNCTION os_guard_3242f159f75c9e4e1909() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    IF NEW . state = 'archived' AND EXISTS ( SELECT 1 FROM knowledge_objects WHERE tenant_id = OLD . tenant_id AND app_id = OLD . app_id AND state = 'active' ) THEN RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'app workspace has active knowledge objects'; END IF;
    RETURN NEW;
END
$$;
CREATE TRIGGER knowledge_active_app_workspace_cannot_be_archived BEFORE UPDATE OF state ON app_workspaces FOR EACH ROW EXECUTE FUNCTION os_guard_3242f159f75c9e4e1909();

CREATE FUNCTION os_guard_ac72347d9f1ba99bf2f4() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    IF EXISTS ( SELECT 1 FROM knowledge_projection_tenant_ledgers WHERE tenant_id = NEW . tenant_id ) THEN RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'knowledge projection tenant ledger already exists'; END IF;
    RETURN NEW;
END
$$;
CREATE TRIGGER knowledge_projection_ledger_identity_collision_is_forbidden BEFORE INSERT ON knowledge_projection_tenant_ledgers FOR EACH ROW EXECUTE FUNCTION os_guard_ac72347d9f1ba99bf2f4();

CREATE FUNCTION os_guard_324b864a49c16794d195() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    IF NEW . projection_bytes <> 0 OR EXISTS ( SELECT 1 FROM knowledge_object_list_projections WHERE tenant_id = NEW . tenant_id ) THEN RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'knowledge projection tenant ledger must begin empty'; END IF;
    RETURN NEW;
END
$$;
CREATE TRIGGER knowledge_projection_ledger_initial_shape_is_valid BEFORE INSERT ON knowledge_projection_tenant_ledgers FOR EACH ROW EXECUTE FUNCTION os_guard_324b864a49c16794d195();

CREATE FUNCTION os_guard_c859a15a9934a7d36013() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    IF NEW . tenant_id <> OLD . tenant_id OR NEW . projection_bytes <> COALESCE ( ( SELECT sum ( projection_bytes ) FROM knowledge_object_list_projections WHERE tenant_id = OLD . tenant_id ) , 0 ) THEN RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'knowledge projection byte ledger transition is invalid'; END IF;
    RETURN NEW;
END
$$;
CREATE TRIGGER knowledge_projection_ledger_transition_is_exact BEFORE UPDATE ON knowledge_projection_tenant_ledgers FOR EACH ROW EXECUTE FUNCTION os_guard_c859a15a9934a7d36013();

CREATE FUNCTION os_guard_bc91a43cd01202ce0612() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'knowledge projection tenant ledger cannot be deleted';
    RETURN OLD;
END
$$;
CREATE TRIGGER knowledge_projection_ledger_delete_is_forbidden BEFORE DELETE ON knowledge_projection_tenant_ledgers FOR EACH ROW EXECUTE FUNCTION os_guard_bc91a43cd01202ce0612();

CREATE FUNCTION os_guard_ade1efd63c001585c14b() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    IF NOT EXISTS ( SELECT 1 FROM knowledge_projection_tenant_ledgers WHERE tenant_id = NEW . tenant_id AND projection_bytes <= 268435456 - (octet_length(NEW.description) + NEW.selector_value_bytes) ) THEN RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'knowledge list projection byte capacity exhausted'; END IF;
    RETURN NEW;
END
$$;
CREATE TRIGGER knowledge_list_projection_capacity_is_available BEFORE INSERT ON knowledge_object_list_projections FOR EACH ROW EXECUTE FUNCTION os_guard_ade1efd63c001585c14b();

CREATE FUNCTION os_guard_0c18937dad1392326cd1() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    IF EXISTS ( SELECT 1 FROM knowledge_object_list_projections WHERE tenant_id = NEW . tenant_id AND knowledge_object_id = NEW . knowledge_object_id AND object_version = NEW . object_version ) THEN RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'knowledge list projection identity already exists'; END IF;
    RETURN NEW;
END
$$;
CREATE TRIGGER knowledge_list_projection_identity_collision_is_forbidden BEFORE INSERT ON knowledge_object_list_projections FOR EACH ROW EXECUTE FUNCTION os_guard_0c18937dad1392326cd1();

CREATE FUNCTION os_guard_5f74968ca966cbe7656a() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    UPDATE knowledge_projection_tenant_ledgers SET projection_bytes = projection_bytes + NEW . projection_bytes WHERE tenant_id = NEW . tenant_id ;
    RETURN NEW;
END
$$;
CREATE TRIGGER knowledge_list_projection_after_insert AFTER INSERT ON knowledge_object_list_projections FOR EACH ROW EXECUTE FUNCTION os_guard_5f74968ca966cbe7656a();

CREATE FUNCTION os_guard_3977ce1a1e0f764dd64c() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'knowledge list projection is immutable';
    RETURN NEW;
END
$$;
CREATE TRIGGER knowledge_list_projection_update_is_forbidden BEFORE UPDATE ON knowledge_object_list_projections FOR EACH ROW EXECUTE FUNCTION os_guard_3977ce1a1e0f764dd64c();

CREATE FUNCTION os_guard_9bf909df65668b40d958() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    IF EXISTS ( SELECT 1 FROM knowledge_object_list_projection_seals WHERE tenant_id = OLD . tenant_id AND knowledge_object_id = OLD . knowledge_object_id AND object_version = OLD . object_version ) OR EXISTS ( SELECT 1 FROM knowledge_object_list_selector_patterns WHERE tenant_id = OLD . tenant_id AND knowledge_object_id = OLD . knowledge_object_id AND object_version = OLD . object_version ) THEN RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'knowledge list projection must be unsealed and empty before deletion'; END IF;
    RETURN OLD;
END
$$;
CREATE TRIGGER knowledge_list_projection_delete_requires_unsealed_empty_row BEFORE DELETE ON knowledge_object_list_projections FOR EACH ROW EXECUTE FUNCTION os_guard_9bf909df65668b40d958();

CREATE FUNCTION os_guard_e517ef6cd976fa6d63c1() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    UPDATE knowledge_projection_tenant_ledgers SET projection_bytes = projection_bytes - OLD . projection_bytes WHERE tenant_id = OLD . tenant_id ;
    RETURN OLD;
END
$$;
CREATE TRIGGER knowledge_list_projection_after_delete AFTER DELETE ON knowledge_object_list_projections FOR EACH ROW EXECUTE FUNCTION os_guard_e517ef6cd976fa6d63c1();

CREATE FUNCTION os_guard_d12e0967d52313b6bfee() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    IF EXISTS ( SELECT 1 FROM knowledge_object_list_selector_patterns WHERE tenant_id = NEW . tenant_id AND knowledge_object_id = NEW . knowledge_object_id AND object_version = NEW . object_version AND dimension = NEW . dimension AND ( ordinal = NEW . ordinal OR value = NEW . value ) ) THEN RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'knowledge list selector identity already exists'; END IF;
    RETURN NEW;
END
$$;
CREATE TRIGGER knowledge_list_selector_identity_collision_is_forbidden BEFORE INSERT ON knowledge_object_list_selector_patterns FOR EACH ROW EXECUTE FUNCTION os_guard_d12e0967d52313b6bfee();

CREATE FUNCTION os_guard_cadb5e4afeb1fe2cc049() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    IF NOT EXISTS ( SELECT 1 FROM knowledge_object_list_projections WHERE tenant_id = NEW . tenant_id AND knowledge_object_id = NEW . knowledge_object_id AND object_version = NEW . object_version AND NEW . ordinal < CASE NEW . dimension WHEN 'index' THEN index_selector_count WHEN 'host' THEN host_selector_count WHEN 'source' THEN source_selector_count WHEN 'sourcetype' THEN sourcetype_selector_count END ) THEN RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'knowledge list selector ordinal exceeds declared count'; END IF;
    RETURN NEW;
END
$$;
CREATE TRIGGER knowledge_list_selector_ordinal_is_declared BEFORE INSERT ON knowledge_object_list_selector_patterns FOR EACH ROW EXECUTE FUNCTION os_guard_cadb5e4afeb1fe2cc049();

CREATE FUNCTION os_guard_c3c4aacbc44d746a79f0() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    IF EXISTS ( SELECT 1 FROM knowledge_object_list_projection_seals WHERE tenant_id = NEW . tenant_id AND knowledge_object_id = NEW . knowledge_object_id AND object_version = NEW . object_version ) THEN RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'knowledge list selector set is sealed'; END IF;
    RETURN NEW;
END
$$;
CREATE TRIGGER knowledge_list_selector_sealed_projection_is_immutable_insert BEFORE INSERT ON knowledge_object_list_selector_patterns FOR EACH ROW EXECUTE FUNCTION os_guard_c3c4aacbc44d746a79f0();

CREATE FUNCTION os_guard_e661e40e45ce2516fd8b() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'knowledge list selector is immutable';
    RETURN NEW;
END
$$;
CREATE TRIGGER knowledge_list_selector_update_is_forbidden BEFORE UPDATE ON knowledge_object_list_selector_patterns FOR EACH ROW EXECUTE FUNCTION os_guard_e661e40e45ce2516fd8b();

CREATE FUNCTION os_guard_cad05924a888cb36a5f5() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    IF EXISTS ( SELECT 1 FROM knowledge_object_list_projection_seals WHERE tenant_id = OLD . tenant_id AND knowledge_object_id = OLD . knowledge_object_id AND object_version = OLD . object_version ) THEN RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'knowledge list selector set is sealed'; END IF;
    RETURN OLD;
END
$$;
CREATE TRIGGER knowledge_list_selector_sealed_projection_is_immutable_delete BEFORE DELETE ON knowledge_object_list_selector_patterns FOR EACH ROW EXECUTE FUNCTION os_guard_cad05924a888cb36a5f5();

CREATE FUNCTION os_guard_c9df99da32d820df3a50() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    IF EXISTS ( SELECT 1 FROM knowledge_object_list_projection_seals WHERE tenant_id = NEW . tenant_id AND knowledge_object_id = NEW . knowledge_object_id AND object_version = NEW . object_version ) THEN RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'knowledge list projection seal already exists'; END IF;
    RETURN NEW;
END
$$;
CREATE TRIGGER knowledge_list_projection_seal_identity_collision_is_forbidden BEFORE INSERT ON knowledge_object_list_projection_seals FOR EACH ROW EXECUTE FUNCTION os_guard_c9df99da32d820df3a50();

CREATE FUNCTION os_guard_2b40a156e4a0ff9a7149() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    IF NOT EXISTS ( SELECT 1 FROM knowledge_object_list_projections AS projection WHERE projection . tenant_id = NEW . tenant_id AND projection . knowledge_object_id = NEW . knowledge_object_id AND projection . object_version = NEW . object_version AND projection . projection_bytes = NEW . projection_bytes AND projection . canonical_selector_bytes = NEW . canonical_selector_bytes AND projection . canonical_selector_bytes = CASE WHEN projection . state = 'quarantined' THEN 0 ELSE 43 + 4 * ( projection . index_selector_count + projection . host_selector_count + projection . source_selector_count + projection . sourcetype_selector_count ) + projection . selector_value_bytes END AND EXISTS ( SELECT 1 FROM ( SELECT COALESCE ( SUM ( CASE dimension WHEN 'index' THEN 1 ELSE 0 END ) , 0 ) AS index_count , COALESCE ( SUM ( CASE dimension WHEN 'host' THEN 1 ELSE 0 END ) , 0 ) AS host_count , COALESCE ( SUM ( CASE dimension WHEN 'source' THEN 1 ELSE 0 END ) , 0 ) AS source_count , COALESCE ( SUM ( CASE dimension WHEN 'sourcetype' THEN 1 ELSE 0 END ) , 0 ) AS sourcetype_count , COALESCE ( SUM ( value_bytes ) , 0 ) AS value_bytes FROM knowledge_object_list_selector_patterns WHERE tenant_id = NEW . tenant_id AND knowledge_object_id = NEW . knowledge_object_id AND object_version = NEW . object_version ) AS selector_aggregate WHERE selector_aggregate . index_count = projection . index_selector_count AND selector_aggregate . host_count = projection . host_selector_count AND selector_aggregate . source_count = projection . source_selector_count AND selector_aggregate . sourcetype_count = projection . sourcetype_selector_count AND selector_aggregate . value_bytes = projection . selector_value_bytes ) AND NOT EXISTS ( SELECT 1 FROM knowledge_object_list_selector_patterns AS current_pattern JOIN knowledge_object_list_selector_patterns AS previous_pattern ON previous_pattern . tenant_id = current_pattern . tenant_id AND previous_pattern . knowledge_object_id = current_pattern . knowledge_object_id AND previous_pattern . object_version = current_pattern . object_version AND previous_pattern . dimension = current_pattern . dimension AND previous_pattern . ordinal = current_pattern . ordinal - 1 WHERE current_pattern . tenant_id = NEW . tenant_id AND current_pattern . knowledge_object_id = NEW . knowledge_object_id AND current_pattern . object_version = NEW . object_version AND convert_to ( previous_pattern . value , 'UTF8' ) >= convert_to ( current_pattern . value , 'UTF8' ) ) ) THEN RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'knowledge list projection is incomplete'; END IF;
    RETURN NEW;
END
$$;
CREATE TRIGGER knowledge_list_projection_seal_is_complete BEFORE INSERT ON knowledge_object_list_projection_seals FOR EACH ROW EXECUTE FUNCTION os_guard_2b40a156e4a0ff9a7149();

CREATE FUNCTION os_guard_0482ae613cb031fbd942() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'knowledge list projection seal is immutable';
    RETURN NEW;
END
$$;
CREATE TRIGGER knowledge_list_projection_seal_update_is_forbidden BEFORE UPDATE ON knowledge_object_list_projection_seals FOR EACH ROW EXECUTE FUNCTION os_guard_0482ae613cb031fbd942();

CREATE FUNCTION os_guard_613beb79079c367b9390() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    IF EXISTS ( SELECT 1 FROM knowledge_objects WHERE tenant_id = OLD . tenant_id AND knowledge_object_id = OLD . knowledge_object_id AND current_version = OLD . object_version ) THEN RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'current knowledge list projection seal cannot be deleted'; END IF;
    RETURN OLD;
END
$$;
CREATE TRIGGER knowledge_list_projection_current_seal_delete_is_forbidden BEFORE DELETE ON knowledge_object_list_projection_seals FOR EACH ROW EXECUTE FUNCTION os_guard_613beb79079c367b9390();

CREATE FUNCTION os_guard_d5ae2f604e61a6b698e0() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    IF NOT EXISTS ( SELECT 1 FROM knowledge_object_list_projections AS projection JOIN knowledge_object_list_projection_seals AS seal ON seal . tenant_id = projection . tenant_id AND seal . knowledge_object_id = projection . knowledge_object_id AND seal . object_version = projection . object_version AND seal . projection_bytes = projection . projection_bytes WHERE projection . tenant_id = NEW . tenant_id AND projection . knowledge_object_id = NEW . knowledge_object_id AND projection . object_version = NEW . current_version AND projection . app_id = NEW . app_id AND projection . owner_id = NEW . owner_id AND projection . object_type = NEW . object_type AND projection . name = NEW . name AND projection . sharing_scope = NEW . sharing_scope AND projection . state = NEW . state ) THEN RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'knowledge object requires exact sealed list projection'; END IF;
    RETURN NEW;
END
$$;
CREATE TRIGGER knowledge_object_insert_requires_sealed_list_projection BEFORE INSERT ON knowledge_objects FOR EACH ROW EXECUTE FUNCTION os_guard_d5ae2f604e61a6b698e0();

CREATE FUNCTION os_guard_2817f4f35cbf8e85ff94() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    IF NOT EXISTS ( SELECT 1 FROM knowledge_object_list_projections AS projection JOIN knowledge_object_list_projection_seals AS seal ON seal . tenant_id = projection . tenant_id AND seal . knowledge_object_id = projection . knowledge_object_id AND seal . object_version = projection . object_version AND seal . projection_bytes = projection . projection_bytes WHERE projection . tenant_id = NEW . tenant_id AND projection . knowledge_object_id = NEW . knowledge_object_id AND projection . object_version = NEW . current_version AND projection . app_id = NEW . app_id AND projection . owner_id = NEW . owner_id AND projection . object_type = NEW . object_type AND projection . name = NEW . name AND projection . sharing_scope = NEW . sharing_scope AND projection . state = NEW . state ) THEN RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'knowledge object requires exact sealed list projection'; END IF;
    RETURN NEW;
END
$$;
CREATE TRIGGER knowledge_object_update_requires_sealed_list_projection BEFORE UPDATE ON knowledge_objects FOR EACH ROW EXECUTE FUNCTION os_guard_2817f4f35cbf8e85ff94();

CREATE FUNCTION os_guard_984773e9ddca17970b79() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    IF NEW . current_version <> OLD . current_version THEN DELETE FROM knowledge_object_list_projection_seals WHERE tenant_id = OLD . tenant_id AND knowledge_object_id = OLD . knowledge_object_id AND object_version = OLD . current_version ; DELETE FROM knowledge_object_list_selector_patterns WHERE tenant_id = OLD . tenant_id AND knowledge_object_id = OLD . knowledge_object_id AND object_version = OLD . current_version ; DELETE FROM knowledge_object_list_projections WHERE tenant_id = OLD . tenant_id AND knowledge_object_id = OLD . knowledge_object_id AND object_version = OLD . current_version ; END IF;
    RETURN NEW;
END
$$;
CREATE TRIGGER knowledge_object_update_removes_old_list_projection AFTER UPDATE ON knowledge_objects FOR EACH ROW EXECUTE FUNCTION os_guard_984773e9ddca17970b79();

CREATE FUNCTION os_guard_99e718077444f99250dc() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    IF EXISTS ( SELECT 1 FROM knowledge_attempt_audit_tenant_state WHERE tenant_id = NEW . tenant_id ) THEN RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'knowledge-attempt audit tenant state already exists'; END IF;
    RETURN NEW;
END
$$;
CREATE TRIGGER knowledge_attempt_audit_state_identity_collision_is_forbidden BEFORE INSERT ON knowledge_attempt_audit_tenant_state FOR EACH ROW EXECUTE FUNCTION os_guard_99e718077444f99250dc();

CREATE FUNCTION os_guard_dd5eecf81aeb7fdc642f() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    IF NEW . first_sequence <> 1 OR NEW . next_sequence <> 1 OR NEW . retained_count <> 0 THEN RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'knowledge-attempt audit tenant state must begin empty'; END IF;
    RETURN NEW;
END
$$;
CREATE TRIGGER knowledge_attempt_audit_state_initial_shape_is_valid BEFORE INSERT ON knowledge_attempt_audit_tenant_state FOR EACH ROW EXECUTE FUNCTION os_guard_dd5eecf81aeb7fdc642f();

CREATE FUNCTION os_guard_471cf5d25ed344f3d492() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'knowledge-attempt audit tenant state cannot be deleted';
    RETURN OLD;
END
$$;
CREATE TRIGGER knowledge_attempt_audit_state_delete_is_forbidden BEFORE DELETE ON knowledge_attempt_audit_tenant_state FOR EACH ROW EXECUTE FUNCTION os_guard_471cf5d25ed344f3d492();

CREATE FUNCTION os_guard_cebcd3747f7aa3040b73() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    IF NOT ( NEW . tenant_id = OLD . tenant_id AND ( ( OLD . retained_count BETWEEN 0 AND 100000 AND OLD . next_sequence BETWEEN 1 AND 9223372036854775806 AND NEW . first_sequence = OLD . first_sequence AND NEW . next_sequence = OLD . next_sequence + 1 AND NEW . retained_count = OLD . retained_count + 1 AND EXISTS ( SELECT 1 FROM knowledge_attempt_audit_events WHERE tenant_id = NEW . tenant_id AND sequence = OLD . next_sequence ) ) OR ( OLD . retained_count = 100001 AND NEW . first_sequence = OLD . first_sequence + 1 AND NEW . next_sequence = OLD . next_sequence AND NEW . retained_count = OLD . retained_count - 1 AND NOT EXISTS ( SELECT 1 FROM knowledge_attempt_audit_events WHERE tenant_id = NEW . tenant_id AND sequence = OLD . first_sequence ) ) ) ) THEN RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'knowledge-attempt audit tenant state transition is invalid'; END IF;
    RETURN NEW;
END
$$;
CREATE TRIGGER knowledge_attempt_audit_state_transition_is_valid BEFORE UPDATE ON knowledge_attempt_audit_tenant_state FOR EACH ROW EXECUTE FUNCTION os_guard_cebcd3747f7aa3040b73();


CREATE FUNCTION os_guard_e2e6e89aafc015609f35() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    IF NOT EXISTS ( SELECT 1 FROM knowledge_attempt_audit_tenant_state WHERE tenant_id = NEW . tenant_id AND retained_count BETWEEN 0 AND 100000 AND next_sequence = NEW . sequence AND next_sequence BETWEEN 1 AND 9223372036854775806 ) THEN RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'knowledge-attempt audit tenant state is invalid or sequence is exhausted'; END IF;
    RETURN NEW;
END
$$;
CREATE TRIGGER knowledge_attempt_audit_event_insert_requires_current_state BEFORE INSERT ON knowledge_attempt_audit_events FOR EACH ROW EXECUTE FUNCTION os_guard_e2e6e89aafc015609f35();

-- Exact retained-sequence probes must remain index-backed as this table grows.
CREATE FUNCTION os_guard_9e8abce4f70e1c0a33cf() RETURNS trigger LANGUAGE plpgsql SET enable_seqscan=off AS $$
DECLARE affected_rows BIGINT;
BEGIN
    UPDATE knowledge_attempt_audit_tenant_state SET next_sequence = next_sequence + 1 , retained_count = retained_count + 1 WHERE tenant_id = NEW . tenant_id AND next_sequence = NEW . sequence AND retained_count BETWEEN 0 AND 100000 ; GET DIAGNOSTICS affected_rows = ROW_COUNT; IF affected_rows <> 1 THEN RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'knowledge-attempt audit event accounting failed'; END IF; DELETE FROM knowledge_attempt_audit_events WHERE tenant_id = NEW . tenant_id AND sequence = ( SELECT first_sequence FROM knowledge_attempt_audit_tenant_state WHERE tenant_id = NEW . tenant_id AND retained_count = 100001 ) ; UPDATE knowledge_attempt_audit_tenant_state SET first_sequence = first_sequence + 1 , retained_count = retained_count - 1 WHERE tenant_id = NEW . tenant_id AND retained_count = 100001 ; IF NOT EXISTS ( SELECT 1 FROM knowledge_attempt_audit_tenant_state WHERE tenant_id = NEW . tenant_id AND next_sequence = NEW . sequence + 1 AND retained_count BETWEEN 1 AND 100000 AND next_sequence - first_sequence = retained_count ) THEN RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'knowledge-attempt audit prune postcondition failed'; END IF;
    RETURN NEW;
END
$$;
CREATE TRIGGER knowledge_attempt_audit_event_advances_and_prunes AFTER INSERT ON knowledge_attempt_audit_events FOR EACH ROW EXECUTE FUNCTION os_guard_9e8abce4f70e1c0a33cf();

CREATE FUNCTION os_guard_0f8bea069650bb9e1bca() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'knowledge-attempt audit events cannot be updated';
    RETURN NEW;
END
$$;
CREATE TRIGGER knowledge_attempt_audit_event_update_is_forbidden BEFORE UPDATE ON knowledge_attempt_audit_events FOR EACH ROW EXECUTE FUNCTION os_guard_0f8bea069650bb9e1bca();

CREATE FUNCTION os_guard_25f1db19f56e2ee50422() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    IF NOT EXISTS ( SELECT 1 FROM knowledge_attempt_audit_tenant_state WHERE tenant_id = OLD . tenant_id AND first_sequence = OLD . sequence AND retained_count = 100001 ) THEN RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'knowledge-attempt audit event deletion is not a rolling prune'; END IF;
    RETURN OLD;
END
$$;
CREATE TRIGGER knowledge_attempt_audit_event_delete_requires_rolling_prune BEFORE DELETE ON knowledge_attempt_audit_events FOR EACH ROW EXECUTE FUNCTION os_guard_25f1db19f56e2ee50422();

CREATE FUNCTION os_guard_4d73c25ccdbd34f1ab01() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    UPDATE knowledge_attempt_audit_tenant_state SET first_sequence = first_sequence + 1 , retained_count = retained_count - 1 WHERE tenant_id = OLD . tenant_id AND first_sequence = OLD . sequence AND retained_count = 100001 ; GET DIAGNOSTICS affected_rows = ROW_COUNT; IF affected_rows <> 1 THEN RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'knowledge-attempt audit rolling prune accounting failed'; END IF;
    RETURN OLD;
END
$$;
CREATE TRIGGER knowledge_attempt_audit_event_prune_advances_state AFTER DELETE ON knowledge_attempt_audit_events FOR EACH ROW EXECUTE FUNCTION os_guard_4d73c25ccdbd34f1ab01();

CREATE FUNCTION os_guard_878ca21922cfd6bfad22() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    IF EXISTS ( SELECT 1 FROM knowledge_catalog_revision_heads WHERE tenant_id = NEW . tenant_id ) THEN RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'knowledge catalog revision head already exists'; END IF;
    RETURN NEW;
END
$$;
CREATE TRIGGER knowledge_catalog_revision_head_identity_collision_is_forbidden BEFORE INSERT ON knowledge_catalog_revision_heads FOR EACH ROW EXECUTE FUNCTION os_guard_878ca21922cfd6bfad22();

CREATE FUNCTION os_guard_846e6f6a78b25c4a2536() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    IF NOT EXISTS ( SELECT 1 FROM knowledge_catalog_tenants WHERE tenant_id = NEW . tenant_id AND catalog_revision = NEW . catalog_revision ) THEN RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'knowledge catalog revision head disagrees with tenant'; END IF;
    RETURN NEW;
END
$$;
CREATE TRIGGER knowledge_catalog_revision_head_insert_agrees_with_tenant BEFORE INSERT ON knowledge_catalog_revision_heads FOR EACH ROW EXECUTE FUNCTION os_guard_846e6f6a78b25c4a2536();

CREATE FUNCTION os_guard_696a28146d6a6cdc4b0a() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    IF NEW . tenant_id <> OLD . tenant_id OR NEW . catalog_revision <> OLD . catalog_revision + 1 OR length ( NEW . state_token ) <> 32 OR NEW . state_token = OLD . state_token OR NOT EXISTS ( SELECT 1 FROM knowledge_catalog_tenants WHERE tenant_id = OLD . tenant_id AND catalog_revision = NEW . catalog_revision ) THEN RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'knowledge catalog revision head transition is invalid'; END IF;
    RETURN NEW;
END
$$;
CREATE TRIGGER knowledge_catalog_revision_head_transition_is_exact BEFORE UPDATE ON knowledge_catalog_revision_heads FOR EACH ROW EXECUTE FUNCTION os_guard_696a28146d6a6cdc4b0a();

CREATE FUNCTION os_guard_72da492ef535fe2a1de2() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'knowledge catalog revision head cannot be deleted';
    RETURN OLD;
END
$$;
CREATE TRIGGER knowledge_catalog_revision_head_delete_is_forbidden BEFORE DELETE ON knowledge_catalog_revision_heads FOR EACH ROW EXECUTE FUNCTION os_guard_72da492ef535fe2a1de2();

CREATE FUNCTION os_guard_22d15a2929c849f0859e() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    INSERT INTO knowledge_catalog_revision_heads ( tenant_id , catalog_revision , state_token ) VALUES ( NEW . tenant_id , NEW . catalog_revision , decode(replace(gen_random_uuid()::text,'-','') || replace(gen_random_uuid()::text,'-',''),'hex') ) ;
    RETURN NEW;
END
$$;
CREATE TRIGGER knowledge_catalog_tenant_creates_revision_head AFTER INSERT ON knowledge_catalog_tenants FOR EACH ROW EXECUTE FUNCTION os_guard_22d15a2929c849f0859e();

CREATE FUNCTION os_guard_4587685c95a2cd247f31() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    IF NEW . catalog_revision <> OLD . catalog_revision AND NOT EXISTS ( SELECT 1 FROM knowledge_catalog_revision_heads WHERE tenant_id = OLD . tenant_id AND catalog_revision = OLD . catalog_revision AND length ( state_token ) = 32 ) THEN RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'knowledge catalog revision head is missing'; END IF;
    RETURN NEW;
END
$$;
CREATE TRIGGER knowledge_catalog_revision_requires_exact_head BEFORE UPDATE OF catalog_revision ON knowledge_catalog_tenants FOR EACH ROW EXECUTE FUNCTION os_guard_4587685c95a2cd247f31();

CREATE FUNCTION os_guard_1f9c2795288321141e45() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    IF NEW . catalog_revision <> OLD . catalog_revision THEN UPDATE knowledge_catalog_revision_heads SET catalog_revision = NEW . catalog_revision , state_token = decode(replace(gen_random_uuid()::text,'-','') || replace(gen_random_uuid()::text,'-',''),'hex') WHERE tenant_id = OLD . tenant_id AND catalog_revision = OLD . catalog_revision ; IF NOT EXISTS ( SELECT 1 FROM knowledge_catalog_revision_heads WHERE tenant_id = NEW . tenant_id AND catalog_revision = NEW . catalog_revision AND length ( state_token ) = 32 ) THEN RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'knowledge catalog revision head rotation failed'; END IF; END IF;
    RETURN NEW;
END
$$;
CREATE TRIGGER knowledge_catalog_revision_rotates_state_token AFTER UPDATE OF catalog_revision ON knowledge_catalog_tenants FOR EACH ROW EXECUTE FUNCTION os_guard_1f9c2795288321141e45();

CREATE FUNCTION os_guard_8d49c513e7b2a1dac928() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    IF EXISTS ( SELECT 1 FROM knowledge_object_version_lifecycle WHERE tenant_id = NEW . tenant_id AND knowledge_object_id = NEW . knowledge_object_id AND object_version = NEW . object_version ) THEN RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'knowledge object version lifecycle already exists'; END IF;
    RETURN NEW;
END
$$;
CREATE TRIGGER knowledge_object_version_lifecycle_identit_8d49c513e7b2a1dac928 BEFORE INSERT ON knowledge_object_version_lifecycle FOR EACH ROW EXECUTE FUNCTION os_guard_8d49c513e7b2a1dac928();

CREATE FUNCTION os_guard_3d65cc56ebf143125aa4() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    IF NOT EXISTS ( SELECT 1 FROM knowledge_object_versions AS version WHERE version . tenant_id = NEW . tenant_id AND version . knowledge_object_id = NEW . knowledge_object_id AND version . object_version = NEW . object_version AND NEW . state = version . state AND ( ( version . state IN ( 'draft' , 'active' ) AND NEW . disabled_at_unix_micro IS NULL AND NEW . quarantined_at_unix_micro IS NULL AND NEW . deleted_at_unix_micro IS NULL AND NEW . quarantine_reason IS NULL ) OR ( version . state = 'disabled' AND NEW . disabled_at_unix_micro IS NOT NULL AND NEW . disabled_at_unix_micro <= version . created_at_unix_micro AND NEW . disabled_at_unix_micro = ( SELECT disabled_version . created_at_unix_micro FROM knowledge_object_versions AS disabled_version WHERE disabled_version . tenant_id = version . tenant_id AND disabled_version . knowledge_object_id = version . knowledge_object_id AND disabled_version . object_version <= version . object_version AND disabled_version . mutation_kind = 'disable' ORDER BY disabled_version . object_version DESC LIMIT 1 ) AND NEW . quarantined_at_unix_micro IS NULL AND NEW . deleted_at_unix_micro IS NULL AND NEW . quarantine_reason IS NULL ) OR ( version . state = 'quarantined' AND NEW . disabled_at_unix_micro IS NULL AND NEW . quarantined_at_unix_micro = version . created_at_unix_micro AND NEW . deleted_at_unix_micro IS NULL AND NEW . quarantine_reason = version . quarantine_reason ) OR ( version . state = 'deleted' AND NEW . disabled_at_unix_micro IS NULL AND NEW . quarantined_at_unix_micro IS NULL AND NEW . deleted_at_unix_micro = version . created_at_unix_micro AND NEW . quarantine_reason IS NULL ) ) ) THEN RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'knowledge object version lifecycle disagrees with version'; END IF;
    RETURN NEW;
END
$$;
CREATE TRIGGER knowledge_object_version_lifecycle_agrees_with_version BEFORE INSERT ON knowledge_object_version_lifecycle FOR EACH ROW EXECUTE FUNCTION os_guard_3d65cc56ebf143125aa4();

CREATE FUNCTION os_guard_f45e5eab1a5d21f033fd() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'knowledge object version lifecycle is immutable';
    RETURN NEW;
END
$$;
CREATE TRIGGER knowledge_object_version_lifecycle_update_is_forbidden BEFORE UPDATE ON knowledge_object_version_lifecycle FOR EACH ROW EXECUTE FUNCTION os_guard_f45e5eab1a5d21f033fd();

CREATE FUNCTION os_guard_33945fc1efcf18db49a0() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'knowledge object version lifecycle cannot be deleted';
    RETURN OLD;
END
$$;
CREATE TRIGGER knowledge_object_version_lifecycle_delete_is_forbidden BEFORE DELETE ON knowledge_object_version_lifecycle FOR EACH ROW EXECUTE FUNCTION os_guard_33945fc1efcf18db49a0();

CREATE FUNCTION os_guard_77fe0f5432f3e248a969() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    IF NEW . object_version > 65536 OR ( NEW . object_version = 1 AND NOT ( NEW . mutation_kind = 'create' AND NEW . state IN ( 'draft' , 'active' ) ) ) OR ( NEW . object_version > 1 AND NOT EXISTS ( SELECT 1 FROM knowledge_object_versions AS previous WHERE previous . tenant_id = NEW . tenant_id AND previous . knowledge_object_id = NEW . knowledge_object_id AND previous . object_version = NEW . object_version - 1 AND NEW . created_at_unix_micro >= previous . created_at_unix_micro AND ( ( NEW . mutation_kind IN ( 'update' , 'scope_change' ) AND NEW . state = previous . state AND NEW . state IN ( 'draft' , 'active' , 'disabled' ) ) OR ( NEW . mutation_kind = 'enable' AND NEW . state = 'active' AND previous . state IN ( 'draft' , 'disabled' ) ) OR ( NEW . mutation_kind = 'disable' AND NEW . state = 'disabled' AND previous . state IN ( 'draft' , 'active' ) ) OR ( NEW . mutation_kind = 'quarantine' AND NEW . state = 'quarantined' AND previous . state IN ( 'draft' , 'active' , 'disabled' ) ) OR ( NEW . mutation_kind = 'delete' AND NEW . state = 'deleted' AND previous . state IN ( 'draft' , 'active' , 'disabled' ) ) ) ) ) THEN RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'knowledge object version transition is invalid'; END IF;
    RETURN NEW;
END
$$;
CREATE TRIGGER knowledge_object_version_transition_is_exact BEFORE INSERT ON knowledge_object_versions FOR EACH ROW EXECUTE FUNCTION os_guard_77fe0f5432f3e248a969();

CREATE FUNCTION os_guard_6f0d12de60e28568c1d9() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    INSERT INTO knowledge_object_version_lifecycle ( tenant_id , knowledge_object_id , object_version , state , disabled_at_unix_micro , quarantined_at_unix_micro , deleted_at_unix_micro , quarantine_reason ) VALUES ( NEW . tenant_id , NEW . knowledge_object_id , NEW . object_version , NEW . state , CASE WHEN NEW . state = 'disabled' AND NEW . mutation_kind = 'disable' THEN NEW . created_at_unix_micro WHEN NEW . state = 'disabled' THEN ( SELECT disabled_at_unix_micro FROM knowledge_object_version_lifecycle WHERE tenant_id = NEW . tenant_id AND knowledge_object_id = NEW . knowledge_object_id AND object_version = NEW . object_version - 1 ) END , CASE WHEN NEW . state = 'quarantined' THEN NEW . created_at_unix_micro END , CASE WHEN NEW . state = 'deleted' THEN NEW . created_at_unix_micro END , CASE WHEN NEW . state = 'quarantined' THEN NEW . quarantine_reason END ) ;
    RETURN NEW;
END
$$;
CREATE TRIGGER knowledge_object_version_creates_lifecycle AFTER INSERT ON knowledge_object_versions FOR EACH ROW EXECUTE FUNCTION os_guard_6f0d12de60e28568c1d9();

CREATE FUNCTION os_guard_e1a481c2faa73cdc40fd() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    IF EXISTS ( SELECT 1 FROM knowledge_object_list_order_keys WHERE tenant_id = NEW . tenant_id AND knowledge_object_id = NEW . knowledge_object_id AND object_version = NEW . object_version ) THEN RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'knowledge list order key already exists'; END IF;
    RETURN NEW;
END
$$;
CREATE TRIGGER knowledge_list_order_key_identity_collision_is_forbidden BEFORE INSERT ON knowledge_object_list_order_keys FOR EACH ROW EXECUTE FUNCTION os_guard_e1a481c2faa73cdc40fd();

CREATE FUNCTION os_guard_4ad2b46f5b827e8d3241() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    IF NOT EXISTS ( SELECT 1 FROM knowledge_object_list_projections AS projection JOIN knowledge_object_versions AS version ON version . tenant_id = projection . tenant_id AND version . knowledge_object_id = projection . knowledge_object_id AND version . object_version = projection . object_version JOIN knowledge_object_versions AS creation_version ON creation_version . tenant_id = projection . tenant_id AND creation_version . knowledge_object_id = projection . knowledge_object_id AND creation_version . object_version = 1 WHERE projection . tenant_id = NEW . tenant_id AND projection . knowledge_object_id = NEW . knowledge_object_id AND projection . object_version = NEW . object_version AND NEW . updated_at_unix_micro = version . created_at_unix_micro AND NEW . created_at_unix_micro = creation_version . created_at_unix_micro ) THEN RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'knowledge list order key disagrees with authorities'; END IF;
    RETURN NEW;
END
$$;
CREATE TRIGGER knowledge_list_order_key_agrees_with_authorities BEFORE INSERT ON knowledge_object_list_order_keys FOR EACH ROW EXECUTE FUNCTION os_guard_4ad2b46f5b827e8d3241();

CREATE FUNCTION os_guard_c34a3fa147e3a05847aa() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'knowledge list order key is immutable';
    RETURN NEW;
END
$$;
CREATE TRIGGER knowledge_list_order_key_update_is_forbidden BEFORE UPDATE ON knowledge_object_list_order_keys FOR EACH ROW EXECUTE FUNCTION os_guard_c34a3fa147e3a05847aa();

CREATE FUNCTION os_guard_3049a0ec68d835605169() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    IF EXISTS ( SELECT 1 FROM knowledge_object_list_projection_seals WHERE tenant_id = OLD . tenant_id AND knowledge_object_id = OLD . knowledge_object_id AND object_version = OLD . object_version ) THEN RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'sealed knowledge list order key cannot be deleted'; END IF;
    RETURN OLD;
END
$$;
CREATE TRIGGER knowledge_list_order_key_sealed_delete_is_forbidden BEFORE DELETE ON knowledge_object_list_order_keys FOR EACH ROW EXECUTE FUNCTION os_guard_3049a0ec68d835605169();

CREATE FUNCTION os_guard_55e5069a71b9c18f1b4a() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    IF EXISTS ( SELECT 1 FROM knowledge_object_versions WHERE tenant_id = NEW . tenant_id AND knowledge_object_id = NEW . knowledge_object_id AND object_version = NEW . object_version ) THEN INSERT INTO knowledge_object_list_order_keys ( tenant_id , knowledge_object_id , object_version , created_at_unix_micro , updated_at_unix_micro ) SELECT NEW . tenant_id , NEW . knowledge_object_id , NEW . object_version , creation_version . created_at_unix_micro , version . created_at_unix_micro FROM knowledge_object_versions AS version JOIN knowledge_object_versions AS creation_version ON creation_version . tenant_id = version . tenant_id AND creation_version . knowledge_object_id = version . knowledge_object_id AND creation_version . object_version = 1 WHERE version . tenant_id = NEW . tenant_id AND version . knowledge_object_id = NEW . knowledge_object_id AND version . object_version = NEW . object_version ; END IF;
    RETURN NEW;
END
$$;
CREATE TRIGGER knowledge_list_projection_creates_order_key AFTER INSERT ON knowledge_object_list_projections FOR EACH ROW EXECUTE FUNCTION os_guard_55e5069a71b9c18f1b4a();

CREATE FUNCTION os_guard_6800e70fdfb4804a6d1e() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    IF EXISTS ( SELECT 1 FROM knowledge_object_list_projections WHERE tenant_id = NEW . tenant_id AND knowledge_object_id = NEW . knowledge_object_id AND object_version = NEW . object_version ) AND NOT EXISTS ( SELECT 1 FROM knowledge_object_list_order_keys WHERE tenant_id = NEW . tenant_id AND knowledge_object_id = NEW . knowledge_object_id AND object_version = NEW . object_version ) THEN INSERT INTO knowledge_object_list_order_keys ( tenant_id , knowledge_object_id , object_version , created_at_unix_micro , updated_at_unix_micro ) SELECT NEW . tenant_id , NEW . knowledge_object_id , NEW . object_version , creation_version . created_at_unix_micro , NEW . created_at_unix_micro FROM knowledge_object_versions AS creation_version WHERE creation_version . tenant_id = NEW . tenant_id AND creation_version . knowledge_object_id = NEW . knowledge_object_id AND creation_version . object_version = 1 ; END IF;
    RETURN NEW;
END
$$;
CREATE TRIGGER knowledge_object_version_creates_staged_order_key AFTER INSERT ON knowledge_object_versions FOR EACH ROW EXECUTE FUNCTION os_guard_6800e70fdfb4804a6d1e();

CREATE FUNCTION os_guard_7b9a24c545f541038573() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    IF NOT EXISTS ( SELECT 1 FROM knowledge_object_list_order_keys WHERE tenant_id = NEW . tenant_id AND knowledge_object_id = NEW . knowledge_object_id AND object_version = NEW . object_version ) THEN RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'knowledge list projection lacks exact order key'; END IF;
    RETURN NEW;
END
$$;
CREATE TRIGGER knowledge_list_projection_seal_requires_order_key BEFORE INSERT ON knowledge_object_list_projection_seals FOR EACH ROW EXECUTE FUNCTION os_guard_7b9a24c545f541038573();

CREATE FUNCTION os_guard_eb1f8b509a21dee28564() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    IF OLD . state = 'active' AND NEW . state IN ( 'disabled' , 'quarantined' , 'deleted' ) AND EXISTS ( SELECT 1 FROM knowledge_objects AS dependent JOIN LATERAL (SELECT 1 FROM knowledge_object_dependencies AS dependency WHERE dependency.tenant_id=dependent.tenant_id AND dependency.source_object_id=dependent.knowledge_object_id AND dependency.source_object_version=dependent.current_version AND dependency.target_kind='object' AND dependency.target_object_id=OLD.knowledge_object_id LIMIT 1) AS dependency_hit ON TRUE WHERE dependent.tenant_id=OLD.tenant_id AND dependent.state='active' LIMIT 1 ) THEN RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'active knowledge dependency has active dependents'; END IF;
    RETURN NEW;
END
$$;
CREATE TRIGGER knowledge_active_dependency_target_transition_is_blocked BEFORE UPDATE OF state ON knowledge_objects FOR EACH ROW EXECUTE FUNCTION os_guard_eb1f8b509a21dee28564();

CREATE FUNCTION os_guard_0b443941f0b3aae58b95() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    IF EXISTS ( SELECT 1 FROM knowledge_mutation_commit_authorities WHERE tenant_id = NEW . tenant_id AND ( catalog_revision = NEW . catalog_revision OR catalog_state_token = NEW . catalog_state_token ) ) THEN RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'knowledge mutation commit authority already exists'; END IF;
    RETURN NEW;
END
$$;
CREATE TRIGGER knowledge_mutation_commit_authority_collision_is_forbidden BEFORE INSERT ON knowledge_mutation_commit_authorities FOR EACH ROW EXECUTE FUNCTION os_guard_0b443941f0b3aae58b95();

CREATE FUNCTION os_guard_5f5f124258f285631541() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'knowledge mutation commit authority is immutable';
    RETURN NEW;
END
$$;
CREATE TRIGGER knowledge_mutation_commit_authority_update_is_forbidden BEFORE UPDATE ON knowledge_mutation_commit_authorities FOR EACH ROW EXECUTE FUNCTION os_guard_5f5f124258f285631541();

CREATE FUNCTION os_guard_915b814ebab9dad3998b() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'knowledge mutation commit authority is retained';
    RETURN OLD;
END
$$;
CREATE TRIGGER knowledge_mutation_commit_authority_delete_is_forbidden BEFORE DELETE ON knowledge_mutation_commit_authorities FOR EACH ROW EXECUTE FUNCTION os_guard_915b814ebab9dad3998b();

CREATE FUNCTION os_guard_8e18d5cb645801127556() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    IF NOT EXISTS ( SELECT 1 FROM knowledge_catalog_tenants WHERE tenant_id = NEW . tenant_id AND ( ( NEW . mutation_kind = 'quarantine' AND idempotency_count < 20480 ) OR ( NEW . mutation_kind <> 'quarantine' AND idempotency_count < 16384 ) ) ) THEN RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'knowledge mutation idempotency capacity exhausted'; END IF;
    RETURN NEW;
END
$$;
CREATE TRIGGER knowledge_mutation_idempotency_capacity_is_available BEFORE INSERT ON knowledge_mutation_idempotency FOR EACH ROW EXECUTE FUNCTION os_guard_8e18d5cb645801127556();

CREATE FUNCTION os_guard_937e69ef7c270b106d33() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    IF EXISTS ( SELECT 1 FROM knowledge_mutation_idempotency WHERE tenant_id = NEW . tenant_id AND actor_kind = NEW . actor_kind AND actor_id = NEW . actor_id AND route = NEW . route AND client_request_id = NEW . client_request_id ) THEN RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'knowledge mutation idempotency identity already exists'; END IF;
    RETURN NEW;
END
$$;
CREATE TRIGGER knowledge_mutation_idempotency_identity_collision_is_forbidden BEFORE INSERT ON knowledge_mutation_idempotency FOR EACH ROW EXECUTE FUNCTION os_guard_937e69ef7c270b106d33();

CREATE FUNCTION os_guard_07a3bfbd00402bd5bfab() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    IF NOT EXISTS ( SELECT 1 FROM knowledge_mutation_commit_authorities AS committed JOIN knowledge_object_versions AS version ON version . tenant_id = committed . tenant_id AND version . knowledge_object_id = NEW . knowledge_object_id AND version . object_version = NEW . object_version JOIN knowledge_object_version_lifecycle AS lifecycle ON lifecycle . tenant_id = version . tenant_id AND lifecycle . knowledge_object_id = version . knowledge_object_id AND lifecycle . object_version = version . object_version AND lifecycle . state = version . state JOIN knowledge_objects AS current ON current . tenant_id = version . tenant_id AND current . knowledge_object_id = version . knowledge_object_id AND current . current_version = version . object_version AND current . app_id = version . app_id AND current . owner_id = version . owner_id AND current . object_type = version . object_type AND current . name = version . name AND current . sharing_scope = version . sharing_scope AND current . state = version . state AND current . definition_digest_key = version . definition_digest_key AND current . updated_at_unix_micro = version . created_at_unix_micro AND current . disabled_at_unix_micro IS NOT DISTINCT FROM lifecycle . disabled_at_unix_micro AND current . quarantined_at_unix_micro IS NOT DISTINCT FROM lifecycle . quarantined_at_unix_micro AND current . deleted_at_unix_micro IS NOT DISTINCT FROM lifecycle . deleted_at_unix_micro AND current . quarantine_reason IS NOT DISTINCT FROM lifecycle . quarantine_reason WHERE committed . tenant_id = NEW . tenant_id AND committed . catalog_revision = NEW . committed_catalog_revision AND committed . catalog_state_token = NEW . committed_catalog_state_token AND committed . actor_kind = NEW . actor_kind AND committed . actor_id = NEW . actor_id AND committed . route = NEW . route AND committed . client_request_id = NEW . client_request_id AND committed . request_digest = NEW . request_digest AND committed . mutation_kind = NEW . mutation_kind AND committed . knowledge_object_id = NEW . knowledge_object_id AND committed . object_version = NEW . object_version AND committed . occurred_at_unix_micro = NEW . created_at_unix_micro AND committed . retention_anchor_unix_micro = NEW . retention_anchor_unix_micro AND committed . retain_until_unix_micro = NEW . retain_until_unix_micro AND committed . successful_audit_sequence IS NOT DISTINCT FROM NEW . successful_audit_sequence AND committed . recovery_audit_sequence IS NOT DISTINCT FROM NEW . recovery_audit_sequence AND version . mutation_kind = NEW . mutation_kind AND version . created_at_unix_micro = NEW . created_at_unix_micro ) THEN RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'knowledge mutation idempotency commit authority is invalid'; END IF;
    RETURN NEW;
END
$$;
CREATE TRIGGER knowledge_mutation_idempotency_matches_commit_authority BEFORE INSERT ON knowledge_mutation_idempotency FOR EACH ROW EXECUTE FUNCTION os_guard_07a3bfbd00402bd5bfab();

CREATE FUNCTION os_guard_8950d5c4eb7ed645dd97() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    UPDATE knowledge_catalog_tenants SET idempotency_count = idempotency_count + 1 WHERE tenant_id = NEW . tenant_id ;
    RETURN NEW;
END
$$;
CREATE TRIGGER knowledge_mutation_idempotency_after_insert AFTER INSERT ON knowledge_mutation_idempotency FOR EACH ROW EXECUTE FUNCTION os_guard_8950d5c4eb7ed645dd97();

CREATE FUNCTION os_guard_6684d233b69d115ad69c() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'knowledge mutation idempotency outcome is immutable';
    RETURN NEW;
END
$$;
CREATE TRIGGER knowledge_mutation_idempotency_update_is_forbidden BEFORE UPDATE ON knowledge_mutation_idempotency FOR EACH ROW EXECUTE FUNCTION os_guard_6684d233b69d115ad69c();

CREATE FUNCTION os_guard_0861d2fb9b1c30d33d40() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    IF CAST ( EXTRACT ( EPOCH FROM clock_timestamp() ) * 1000000 AS BIGINT ) < OLD . retain_until_unix_micro THEN RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'knowledge mutation idempotency retention fence is active'; END IF;
    RETURN OLD;
END
$$;
CREATE TRIGGER knowledge_mutation_idempotency_delete_befo_0861d2fb9b1c30d33d40 BEFORE DELETE ON knowledge_mutation_idempotency FOR EACH ROW EXECUTE FUNCTION os_guard_0861d2fb9b1c30d33d40();

CREATE FUNCTION os_guard_1c28a82bc672d4d02b1f() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    UPDATE knowledge_catalog_tenants SET idempotency_count = idempotency_count - 1 WHERE tenant_id = OLD . tenant_id ;
    RETURN OLD;
END
$$;
CREATE TRIGGER knowledge_mutation_idempotency_after_delete AFTER DELETE ON knowledge_mutation_idempotency FOR EACH ROW EXECUTE FUNCTION os_guard_1c28a82bc672d4d02b1f();

CREATE FUNCTION os_guard_ea75a3300b55d9b0d98c() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    IF EXISTS ( SELECT 1 FROM app_catalog_revisions AS authority WHERE authority . tenant_id = NEW . tenant_id ) THEN RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'app catalog revision authority already exists'; END IF;
    RETURN NEW;
END
$$;
CREATE TRIGGER app_catalog_revision_identity_collision_is_forbidden BEFORE INSERT ON app_catalog_revisions FOR EACH ROW EXECUTE FUNCTION os_guard_ea75a3300b55d9b0d98c();

CREATE FUNCTION os_guard_fff8faab46fbaddb731b() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    IF NEW . revision <> 1 OR NOT EXISTS ( SELECT 1 FROM app_workspaces AS app WHERE app . tenant_id = NEW . tenant_id ) THEN RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'app catalog revision authority must begin with its first app'; END IF;
    RETURN NEW;
END
$$;
CREATE TRIGGER app_catalog_revision_initial_shape_is_exact BEFORE INSERT ON app_catalog_revisions FOR EACH ROW EXECUTE FUNCTION os_guard_fff8faab46fbaddb731b();

CREATE FUNCTION os_guard_614a8ab4950c988008e5() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    IF NEW . tenant_id <> OLD . tenant_id THEN RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'app catalog revision tenant is immutable'; END IF;
    RETURN NEW;
END
$$;
CREATE TRIGGER app_catalog_revision_tenant_is_immutable BEFORE UPDATE OF tenant_id ON app_catalog_revisions FOR EACH ROW EXECUTE FUNCTION os_guard_614a8ab4950c988008e5();

CREATE FUNCTION os_guard_e79f9aa9b638260a87c4() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    IF OLD . revision < 1 OR NEW . revision <> OLD . revision + 1 THEN RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'app catalog revision must advance by exactly one'; END IF;
    RETURN NEW;
END
$$;
CREATE TRIGGER app_catalog_revision_transition_is_exact BEFORE UPDATE OF revision ON app_catalog_revisions FOR EACH ROW EXECUTE FUNCTION os_guard_e79f9aa9b638260a87c4();

CREATE FUNCTION os_guard_bca47d7d2fd474c07ab8() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'app catalog revision authority cannot be deleted';
    RETURN OLD;
END
$$;
CREATE TRIGGER app_catalog_revision_delete_is_forbidden BEFORE DELETE ON app_catalog_revisions FOR EACH ROW EXECUTE FUNCTION os_guard_bca47d7d2fd474c07ab8();

CREATE FUNCTION os_guard_41079def89e69acb90c5() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    UPDATE app_catalog_revisions SET revision = revision + 1 WHERE tenant_id = NEW . tenant_id ; INSERT INTO app_catalog_revisions ( tenant_id , revision ) SELECT NEW . tenant_id , 1 WHERE NOT EXISTS ( SELECT 1 FROM app_catalog_revisions AS authority WHERE authority . tenant_id = NEW . tenant_id ) AND ( SELECT count ( * ) FROM app_workspaces AS app WHERE app . tenant_id = NEW . tenant_id ) = 1 ; IF NOT EXISTS ( SELECT 1 FROM app_catalog_revisions AS authority WHERE authority . tenant_id = NEW . tenant_id ) THEN RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'app catalog revision authority is missing'; END IF;
    RETURN NEW;
END
$$;
CREATE TRIGGER app_catalog_revision_after_insert AFTER INSERT ON app_workspaces FOR EACH ROW EXECUTE FUNCTION os_guard_41079def89e69acb90c5();

CREATE FUNCTION os_guard_0d7e9f973e8e3e361126() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    IF NOT EXISTS ( SELECT 1 FROM app_catalog_revisions AS authority WHERE authority . tenant_id = NEW . tenant_id ) THEN RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'app catalog revision authority is missing'; END IF; UPDATE app_catalog_revisions SET revision = revision + 1 WHERE tenant_id = NEW . tenant_id ;
    RETURN NEW;
END
$$;
CREATE TRIGGER app_catalog_revision_after_update AFTER UPDATE ON app_workspaces FOR EACH ROW EXECUTE FUNCTION os_guard_0d7e9f973e8e3e361126();

CREATE FUNCTION os_guard_2894514558b7396a3c9c() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    IF NOT EXISTS ( SELECT 1 FROM app_catalog_revisions AS authority WHERE authority . tenant_id = OLD . tenant_id ) THEN RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'app catalog revision authority is missing'; END IF; UPDATE app_catalog_revisions SET revision = revision + 1 WHERE tenant_id = OLD . tenant_id ;
    RETURN OLD;
END
$$;
CREATE TRIGGER app_catalog_revision_after_delete AFTER DELETE ON app_workspaces FOR EACH ROW EXECUTE FUNCTION os_guard_2894514558b7396a3c9c();

CREATE FUNCTION os_guard_5bfc0bb56c6b40ad51c8() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    IF EXISTS ( SELECT 1 FROM knowledge_catalog_tenants AS tenant WHERE tenant . tenant_id = NEW . tenant_id AND NOT EXISTS ( SELECT 1 FROM knowledge_catalog_revision_heads AS head WHERE head . tenant_id = tenant . tenant_id AND head . catalog_revision = tenant . catalog_revision AND length ( head . state_token ) = 32 ) ) OR EXISTS ( SELECT 1 FROM knowledge_catalog_revision_heads AS head WHERE head . tenant_id = NEW . tenant_id AND NOT EXISTS ( SELECT 1 FROM knowledge_catalog_tenants AS tenant WHERE tenant . tenant_id = head . tenant_id AND tenant . catalog_revision = head . catalog_revision ) ) OR EXISTS ( SELECT 1 FROM knowledge_projection_tenant_ledgers AS ledger WHERE ledger . tenant_id = NEW . tenant_id AND ( NOT EXISTS ( SELECT 1 FROM knowledge_catalog_tenants AS tenant WHERE tenant . tenant_id = ledger . tenant_id ) OR ledger . projection_bytes <> COALESCE ( ( SELECT sum ( projection . projection_bytes ) FROM knowledge_object_list_projections AS projection WHERE projection . tenant_id = ledger . tenant_id ) , 0 ) ) ) OR EXISTS ( SELECT 1 FROM knowledge_object_list_projections AS projection WHERE projection . tenant_id = NEW . tenant_id AND NOT EXISTS ( SELECT 1 FROM knowledge_projection_tenant_ledgers AS ledger WHERE ledger . tenant_id = projection . tenant_id ) ) THEN RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'app catalog tenant knowledge prestate is incomplete or corrupt'; END IF; INSERT INTO knowledge_catalog_tenants ( tenant_id ) SELECT NEW . tenant_id WHERE NOT EXISTS ( SELECT 1 FROM knowledge_catalog_tenants AS tenant WHERE tenant . tenant_id = NEW . tenant_id ) ; INSERT INTO knowledge_projection_tenant_ledgers ( tenant_id ) SELECT NEW . tenant_id WHERE NOT EXISTS ( SELECT 1 FROM knowledge_projection_tenant_ledgers AS ledger WHERE ledger . tenant_id = NEW . tenant_id ) ; IF NOT EXISTS ( SELECT 1 FROM knowledge_catalog_tenants AS tenant JOIN knowledge_catalog_revision_heads AS head ON head . tenant_id = tenant . tenant_id AND head . catalog_revision = tenant . catalog_revision JOIN knowledge_projection_tenant_ledgers AS ledger ON ledger . tenant_id = tenant . tenant_id WHERE tenant . tenant_id = NEW . tenant_id AND length ( head . state_token ) = 32 AND ledger . projection_bytes = COALESCE ( ( SELECT sum ( projection . projection_bytes ) FROM knowledge_object_list_projections AS projection WHERE projection . tenant_id = tenant . tenant_id ) , 0 ) ) THEN RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'app catalog tenant knowledge authority is incomplete or corrupt'; END IF;
    RETURN NEW;
END
$$;
CREATE TRIGGER app_catalog_revision_provisions_knowledge_catalog_after_insert AFTER INSERT ON app_catalog_revisions FOR EACH ROW EXECUTE FUNCTION os_guard_5bfc0bb56c6b40ad51c8();

CREATE FUNCTION os_guard_7ab5033d6e1dc7d025ae() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    IF NEW . object_version > 1 AND EXISTS ( SELECT 1 FROM knowledge_object_versions AS previous WHERE previous . tenant_id = NEW . tenant_id AND previous . knowledge_object_id = NEW . knowledge_object_id AND previous . object_version = NEW . object_version - 1 AND NEW . created_at_unix_micro >= previous . created_at_unix_micro AND ( ( NEW . mutation_kind IN ( 'update' , 'scope_change' ) AND NEW . state = previous . state AND NEW . state IN ( 'draft' , 'active' , 'disabled' ) ) OR ( NEW . mutation_kind = 'enable' AND NEW . state = 'active' AND previous . state IN ( 'draft' , 'disabled' ) ) OR ( NEW . mutation_kind = 'disable' AND NEW . state = 'disabled' AND previous . state IN ( 'draft' , 'active' ) ) OR ( NEW . mutation_kind = 'quarantine' AND NEW . state = 'quarantined' AND previous . state IN ( 'draft' , 'active' , 'disabled' ) ) OR ( NEW . mutation_kind = 'delete' AND NEW . state = 'deleted' AND previous . state IN ( 'draft' , 'active' , 'disabled' ) ) ) ) AND NOT EXISTS ( SELECT 1 FROM knowledge_object_versions AS previous WHERE previous . tenant_id = NEW . tenant_id AND previous . knowledge_object_id = NEW . knowledge_object_id AND previous . object_version = NEW . object_version - 1 AND NEW . created_at_unix_micro >= previous . created_at_unix_micro AND NEW . owner_id = previous . owner_id AND NEW . object_type = previous . object_type AND ( ( NEW . mutation_kind = 'update' AND NEW . state = previous . state AND NEW . state IN ( 'draft' , 'active' , 'disabled' ) AND NEW . app_id = previous . app_id AND NEW . sharing_scope = previous . sharing_scope AND coalesce(NEW.definition_digest,decode('','hex')) <> previous . definition_digest_key ) OR ( NEW . mutation_kind = 'scope_change' AND NEW . state = previous . state AND NEW . state IN ( 'draft' , 'active' , 'disabled' ) AND ( NEW . app_id <> previous . app_id OR NEW . sharing_scope <> previous . sharing_scope ) AND coalesce(NEW.definition_digest,decode('','hex')) <> previous . definition_digest_key ) OR ( NEW . mutation_kind = 'enable' AND NEW . state = 'active' AND previous . state IN ( 'draft' , 'disabled' ) AND NEW . app_id = previous . app_id AND NEW . name = previous . name AND NEW . sharing_scope = previous . sharing_scope AND coalesce(NEW.definition_digest,decode('','hex')) = previous . definition_digest_key ) OR ( NEW . mutation_kind = 'disable' AND NEW . state = 'disabled' AND previous . state IN ( 'draft' , 'active' ) AND NEW . app_id = previous . app_id AND NEW . name = previous . name AND NEW . sharing_scope = previous . sharing_scope AND coalesce(NEW.definition_digest,decode('','hex')) = previous . definition_digest_key AND NEW . dependency_count = previous . dependency_count ) OR ( NEW . mutation_kind = 'delete' AND NEW . state = 'deleted' AND previous . state IN ( 'draft' , 'active' , 'disabled' ) AND NEW . app_id = previous . app_id AND NEW . name = previous . name AND NEW . sharing_scope = previous . sharing_scope AND coalesce(NEW.definition_digest,decode('','hex')) = previous . definition_digest_key AND NEW . dependency_count = previous . dependency_count ) OR ( NEW . mutation_kind = 'quarantine' AND NEW . state = 'quarantined' AND previous . state IN ( 'draft' , 'active' , 'disabled' ) AND NEW . app_id = previous . app_id AND NEW . name = previous . name AND NEW . sharing_scope = previous . sharing_scope AND NEW . definition_digest IS NULL ) ) ) THEN RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'knowledge object version writer semantics are invalid'; END IF;
    RETURN NEW;
END
$$;
CREATE TRIGGER knowledge_object_version_writer_semantics_are_exact BEFORE INSERT ON knowledge_object_versions FOR EACH ROW EXECUTE FUNCTION os_guard_7ab5033d6e1dc7d025ae();

CREATE FUNCTION os_guard_6726710a535ef99cfcd4() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    IF NEW . object_version > 1 AND EXISTS ( SELECT 1 FROM knowledge_object_versions AS version WHERE version . tenant_id = NEW . tenant_id AND version . knowledge_object_id = NEW . knowledge_object_id AND version . object_version = NEW . object_version AND version . mutation_kind IN ( 'disable' , 'delete' ) ) AND EXISTS ( SELECT 1 FROM knowledge_object_versions AS version JOIN knowledge_object_versions AS previous ON previous . tenant_id = version . tenant_id AND previous . knowledge_object_id = version . knowledge_object_id AND previous . object_version = version . object_version - 1 WHERE version . tenant_id = NEW . tenant_id AND version . knowledge_object_id = NEW . knowledge_object_id AND version . object_version = NEW . object_version AND ( version . dependency_count <> previous . dependency_count OR EXISTS ( SELECT 1 FROM knowledge_object_dependencies AS dependency WHERE dependency . tenant_id = version . tenant_id AND dependency . source_object_id = version . knowledge_object_id AND dependency . source_object_version = version . object_version AND NOT EXISTS ( SELECT 1 FROM knowledge_object_dependencies AS prior_dependency WHERE prior_dependency . tenant_id = previous . tenant_id AND prior_dependency . source_object_id = previous . knowledge_object_id AND prior_dependency . source_object_version = previous . object_version AND prior_dependency . ordinal = dependency . ordinal AND prior_dependency . target_kind = dependency . target_kind AND prior_dependency . target_object_id = dependency . target_object_id AND prior_dependency . target_object_version = dependency . target_object_version AND prior_dependency . dependency_role = dependency . dependency_role ) ) OR EXISTS ( SELECT 1 FROM knowledge_object_dependencies AS prior_dependency WHERE prior_dependency . tenant_id = previous . tenant_id AND prior_dependency . source_object_id = previous . knowledge_object_id AND prior_dependency . source_object_version = previous . object_version AND NOT EXISTS ( SELECT 1 FROM knowledge_object_dependencies AS dependency WHERE dependency . tenant_id = version . tenant_id AND dependency . source_object_id = version . knowledge_object_id AND dependency . source_object_version = version . object_version AND dependency . ordinal = prior_dependency . ordinal AND dependency . target_kind = prior_dependency . target_kind AND dependency . target_object_id = prior_dependency . target_object_id AND dependency . target_object_version = prior_dependency . target_object_version AND dependency . dependency_role = prior_dependency . dependency_role ) ) ) ) THEN RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'knowledge object state-only dependencies are invalid'; END IF;
    RETURN NEW;
END
$$;
CREATE TRIGGER knowledge_object_state_only_dependencies_are_exact BEFORE INSERT ON knowledge_object_dependency_seals FOR EACH ROW EXECUTE FUNCTION os_guard_6726710a535ef99cfcd4();

CREATE FUNCTION os_guard_c104797c384a4c4f1252() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    IF OLD . state = 'active' AND NEW . state = 'active' AND NEW . current_version <> OLD . current_version AND EXISTS ( SELECT 1 FROM knowledge_objects AS dependent JOIN LATERAL (SELECT 1 FROM knowledge_object_dependencies AS dependency WHERE dependency.tenant_id=dependent.tenant_id AND dependency.source_object_id=dependent.knowledge_object_id AND dependency.source_object_version=dependent.current_version AND dependency.target_kind='object' AND dependency.target_object_id=OLD.knowledge_object_id AND dependency.target_object_version<>NEW.current_version LIMIT 1) AS dependency_hit ON TRUE WHERE dependent.tenant_id=OLD.tenant_id AND dependent.state='active' LIMIT 1 ) THEN RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'active knowledge dependency pins a prior target version'; END IF;
    RETURN NEW;
END
$$;
CREATE TRIGGER knowledge_active_dependency_target_version_advance_is_blocked BEFORE UPDATE OF current_version ON knowledge_objects FOR EACH ROW EXECUTE FUNCTION os_guard_c104797c384a4c4f1252();

CREATE FUNCTION os_guard_b51e97bc55162ec967c4() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    IF NEW . purpose = 'native_collector' AND NEW . bound_collector_id IS NULL THEN RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'ingestion token collector binding is required'; END IF;
    RETURN NEW;
END
$$;
CREATE TRIGGER ingestion_token_collector_binding_is_required BEFORE INSERT ON ingestion_tokens FOR EACH ROW EXECUTE FUNCTION os_guard_b51e97bc55162ec967c4();

CREATE FUNCTION os_guard_4b4a1f73a5775abda591() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    IF NEW . purpose = 'hec' AND NEW . bound_collector_id IS NOT NULL THEN RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'HEC ingestion token cannot have a collector binding'; END IF;
    RETURN NEW;
END
$$;
CREATE TRIGGER ingestion_token_hec_collector_binding_is_forbidden BEFORE INSERT ON ingestion_tokens FOR EACH ROW EXECUTE FUNCTION os_guard_4b4a1f73a5775abda591();

CREATE FUNCTION os_guard_ae953fe78d8d78040d13() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    IF NEW . purpose = 'hec' AND NEW . bound_collector_id IS NOT NULL THEN RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'HEC ingestion token cannot have a collector binding'; END IF;
    RETURN NEW;
END
$$;
CREATE TRIGGER ingestion_token_hec_collector_binding_update_is_forbidden BEFORE UPDATE OF bound_collector_id ON ingestion_tokens FOR EACH ROW EXECUTE FUNCTION os_guard_ae953fe78d8d78040d13();

CREATE FUNCTION os_guard_c30148976b6279086045() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    IF NEW . purpose <> OLD . purpose THEN RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'ingestion token purpose is immutable'; END IF;
    RETURN NEW;
END
$$;
CREATE TRIGGER ingestion_token_purpose_is_immutable BEFORE UPDATE OF purpose ON ingestion_tokens FOR EACH ROW EXECUTE FUNCTION os_guard_c30148976b6279086045();

CREATE FUNCTION os_guard_96f459441b838097794e() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    IF NOT EXISTS ( SELECT 1 FROM ingestion_tokens AS token WHERE token . ingestion_token_id = NEW . ingestion_token_id AND token . purpose = 'hec' AND token . bound_collector_id IS NULL ) THEN RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'HEC token profile requires an unbound HEC token'; END IF;
    RETURN NEW;
END
$$;
CREATE TRIGGER ingestion_token_hec_profile_requires_hec_purpose BEFORE INSERT ON ingestion_token_hec_profiles FOR EACH ROW EXECUTE FUNCTION os_guard_96f459441b838097794e();

CREATE FUNCTION os_guard_2b7d741e46eb6d5f7cce() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    IF NEW . ingestion_token_id <> OLD . ingestion_token_id THEN RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'HEC token profile owner is immutable'; END IF;
    RETURN NEW;
END
$$;
CREATE TRIGGER ingestion_token_hec_profile_owner_is_immutable BEFORE UPDATE OF ingestion_token_id ON ingestion_token_hec_profiles FOR EACH ROW EXECUTE FUNCTION os_guard_2b7d741e46eb6d5f7cce();

CREATE FUNCTION os_guard_68bf07c8a6e32eead758() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    IF NEW . indexer_acknowledgment <> OLD . indexer_acknowledgment THEN RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'HEC token acknowledgment mode is immutable'; END IF;
    RETURN NEW;
END
$$;
CREATE TRIGGER ingestion_token_hec_acknowledgment_is_immutable BEFORE UPDATE OF indexer_acknowledgment ON ingestion_token_hec_profiles FOR EACH ROW EXECUTE FUNCTION os_guard_68bf07c8a6e32eead758();

CREATE FUNCTION os_guard_3a31f3a59757208fecf2() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    IF OLD . state = 'reserved' AND NEW . state = 'committed' THEN UPDATE hec_requests SET state = 'indexed' , terminal_at_unix_micro = NEW . committed_at_unix_micro WHERE visibility_sequence = NEW . sequence AND state = 'pending' ; END IF;
    RETURN NEW;
END
$$;
CREATE TRIGGER hec_request_visibility_committed AFTER UPDATE OF state ON ingest_visibility_reservations FOR EACH ROW EXECUTE FUNCTION os_guard_3a31f3a59757208fecf2();

CREATE FUNCTION os_guard_a8d53e57e2a0cd593020() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    IF OLD . state = 'reserved' AND NEW . state IN ( 'rejected' , 'abandoned' ) THEN UPDATE hec_requests SET state = 'terminal_failure' , terminal_at_unix_micro = COALESCE ( NEW . committed_at_unix_micro , CAST ( EXTRACT ( EPOCH FROM clock_timestamp() ) * 1000000 AS BIGINT ) ) WHERE visibility_sequence = NEW . sequence AND state = 'pending' ; END IF;
    RETURN NEW;
END
$$;
CREATE TRIGGER hec_request_visibility_failed AFTER UPDATE OF state ON ingest_visibility_reservations FOR EACH ROW EXECUTE FUNCTION os_guard_a8d53e57e2a0cd593020();

CREATE FUNCTION os_guard_9996b6a9502c2dc8a5e6() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    INSERT INTO knowledge_lookup_asset_tenant_ledgers ( tenant_id ) VALUES ( NEW . tenant_id ) ;
    RETURN NEW;
END
$$;
CREATE TRIGGER knowledge_catalog_tenant_provisions_lookup_asset_ledger AFTER INSERT ON knowledge_catalog_tenants FOR EACH ROW EXECUTE FUNCTION os_guard_9996b6a9502c2dc8a5e6();

CREATE FUNCTION os_guard_3acca6c4919fa9bfdeff() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    IF EXISTS ( SELECT 1 FROM knowledge_lookup_asset_tenant_ledgers WHERE tenant_id = NEW . tenant_id ) THEN RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'lookup asset tenant ledger already exists'; END IF;
    RETURN NEW;
END
$$;
CREATE TRIGGER knowledge_lookup_asset_ledger_identity_collision_is_forbidden BEFORE INSERT ON knowledge_lookup_asset_tenant_ledgers FOR EACH ROW EXECUTE FUNCTION os_guard_3acca6c4919fa9bfdeff();

CREATE FUNCTION os_guard_cef8a5298980786b6d59() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    IF NEW . staged_asset_count <> 0 OR NEW . asset_identity_count <> 0 OR NEW . published_version_count <> 0 OR NEW . stored_content_bytes <> 0 OR EXISTS ( SELECT 1 FROM knowledge_lookup_asset_stages WHERE tenant_id = NEW . tenant_id ) OR EXISTS ( SELECT 1 FROM knowledge_lookup_assets WHERE tenant_id = NEW . tenant_id ) OR EXISTS ( SELECT 1 FROM knowledge_lookup_asset_versions WHERE tenant_id = NEW . tenant_id ) THEN RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'lookup asset tenant ledger must begin empty'; END IF;
    RETURN NEW;
END
$$;
CREATE TRIGGER knowledge_lookup_asset_ledger_initial_shape_is_exact BEFORE INSERT ON knowledge_lookup_asset_tenant_ledgers FOR EACH ROW EXECUTE FUNCTION os_guard_cef8a5298980786b6d59();

CREATE FUNCTION os_guard_b91934fc93ae18ff35d3() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    IF NEW . tenant_id <> OLD . tenant_id OR NEW . staged_asset_count <> ( SELECT count ( * ) FROM knowledge_lookup_asset_stages WHERE tenant_id = OLD . tenant_id ) OR NEW . asset_identity_count <> ( SELECT count ( * ) FROM knowledge_lookup_assets WHERE tenant_id = OLD . tenant_id ) OR NEW . published_version_count <> ( SELECT count ( * ) FROM knowledge_lookup_asset_versions WHERE tenant_id = OLD . tenant_id ) OR NEW . stored_content_bytes <> ( COALESCE ( ( SELECT sum ( canonical_bytes ) FROM knowledge_lookup_asset_stages WHERE tenant_id = OLD . tenant_id ) , 0 ) + COALESCE ( ( SELECT sum ( canonical_bytes ) FROM knowledge_lookup_asset_versions WHERE tenant_id = OLD . tenant_id ) , 0 ) ) THEN RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'lookup asset tenant ledger transition is invalid'; END IF;
    RETURN NEW;
END
$$;
CREATE TRIGGER knowledge_lookup_asset_ledger_transition_is_exact BEFORE UPDATE ON knowledge_lookup_asset_tenant_ledgers FOR EACH ROW EXECUTE FUNCTION os_guard_b91934fc93ae18ff35d3();

CREATE FUNCTION os_guard_06fc6a6221375ed2f3d6() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'lookup asset tenant ledger cannot be deleted';
    RETURN OLD;
END
$$;
CREATE TRIGGER knowledge_lookup_asset_ledger_delete_is_forbidden BEFORE DELETE ON knowledge_lookup_asset_tenant_ledgers FOR EACH ROW EXECUTE FUNCTION os_guard_06fc6a6221375ed2f3d6();

CREATE FUNCTION os_guard_81fcba03ed1d664913b3() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    IF EXISTS ( SELECT 1 FROM knowledge_lookup_asset_tenant_ledgers AS ledger WHERE ledger . tenant_id = NEW . tenant_id ) AND NOT EXISTS ( SELECT 1 FROM knowledge_lookup_asset_tenant_ledgers AS ledger WHERE ledger . tenant_id = NEW . tenant_id AND ledger . staged_asset_count < 64 AND ledger . stored_content_bytes <= 2147483648 - NEW . canonical_bytes ) THEN RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'lookup asset staging capacity exhausted'; END IF;
    RETURN NEW;
END
$$;
CREATE TRIGGER knowledge_lookup_asset_stage_capacity_is_available BEFORE INSERT ON knowledge_lookup_asset_stages FOR EACH ROW EXECUTE FUNCTION os_guard_81fcba03ed1d664913b3();

CREATE FUNCTION os_guard_c63184413b55442c2d36() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    IF NOT EXISTS ( SELECT 1 FROM knowledge_lookup_asset_tenant_ledgers WHERE tenant_id = NEW . tenant_id ) THEN RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'lookup asset tenant ledger is missing'; END IF;
    RETURN NEW;
END
$$;
CREATE TRIGGER knowledge_lookup_asset_stage_requires_tenant_ledger BEFORE INSERT ON knowledge_lookup_asset_stages FOR EACH ROW EXECUTE FUNCTION os_guard_c63184413b55442c2d36();

CREATE FUNCTION os_guard_821abf1ef79d3c7d75cc() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'lookup asset stages are immutable';
    RETURN NEW;
END
$$;
CREATE TRIGGER knowledge_lookup_asset_stage_updates_are_forbidden BEFORE UPDATE ON knowledge_lookup_asset_stages FOR EACH ROW EXECUTE FUNCTION os_guard_821abf1ef79d3c7d75cc();

CREATE FUNCTION os_guard_788a437a07fb5035946b() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
 UPDATE knowledge_lookup_asset_tenant_ledgers AS ledger
 SET staged_asset_count=ledger.staged_asset_count+delta.count, stored_content_bytes=ledger.stored_content_bytes+delta.bytes
 FROM (SELECT tenant_id,count(*)::bigint AS count,sum(canonical_bytes)::bigint AS bytes FROM new_asset_rows GROUP BY tenant_id) AS delta
 WHERE ledger.tenant_id=delta.tenant_id;
 GET DIAGNOSTICS affected_rows=ROW_COUNT;
 IF affected_rows<>(SELECT count(DISTINCT tenant_id) FROM new_asset_rows) THEN
  RAISE EXCEPTION 'lookup asset statement accounting failed' USING ERRCODE='23514';
 END IF;
 RETURN NULL;
END;
$$;
CREATE TRIGGER knowledge_lookup_asset_stage_accounts_after_insert AFTER INSERT ON knowledge_lookup_asset_stages
 REFERENCING NEW TABLE AS new_asset_rows FOR EACH STATEMENT EXECUTE FUNCTION os_guard_788a437a07fb5035946b();

CREATE FUNCTION os_guard_a13d8e6a25ee1d80b7ce() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
 UPDATE knowledge_lookup_asset_tenant_ledgers AS ledger
 SET staged_asset_count=ledger.staged_asset_count-delta.count, stored_content_bytes=ledger.stored_content_bytes-delta.bytes
 FROM (SELECT tenant_id,count(*)::bigint AS count,sum(canonical_bytes)::bigint AS bytes FROM old_asset_rows GROUP BY tenant_id) AS delta
 WHERE ledger.tenant_id=delta.tenant_id;
 GET DIAGNOSTICS affected_rows=ROW_COUNT;
 IF affected_rows<>(SELECT count(DISTINCT tenant_id) FROM old_asset_rows) THEN
  RAISE EXCEPTION 'lookup asset statement accounting failed' USING ERRCODE='23514';
 END IF;
 RETURN NULL;
END;
$$;
CREATE TRIGGER knowledge_lookup_asset_stage_accounts_after_delete AFTER DELETE ON knowledge_lookup_asset_stages
 REFERENCING OLD TABLE AS old_asset_rows FOR EACH STATEMENT EXECUTE FUNCTION os_guard_a13d8e6a25ee1d80b7ce();

CREATE FUNCTION os_guard_d0164840f9bb43f35c93() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    IF EXISTS ( SELECT 1 FROM knowledge_lookup_asset_tenant_ledgers AS ledger WHERE ledger . tenant_id = NEW . tenant_id ) AND ( NEW . current_version <> 1 OR NOT EXISTS ( SELECT 1 FROM knowledge_lookup_asset_tenant_ledgers AS ledger WHERE ledger . tenant_id = NEW . tenant_id AND ledger . asset_identity_count < 2048 ) ) THEN RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'lookup asset identity capacity is unavailable or first version is invalid'; END IF;
    RETURN NEW;
END
$$;
CREATE TRIGGER knowledge_lookup_asset_identity_capacity_is_available BEFORE INSERT ON knowledge_lookup_assets FOR EACH ROW EXECUTE FUNCTION os_guard_d0164840f9bb43f35c93();

CREATE FUNCTION os_guard_a7e3d7f272b8eea9ff2f() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    IF NOT EXISTS ( SELECT 1 FROM knowledge_lookup_asset_tenant_ledgers WHERE tenant_id = NEW . tenant_id ) THEN RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'lookup asset tenant ledger is missing'; END IF;
    RETURN NEW;
END
$$;
CREATE TRIGGER knowledge_lookup_asset_identity_requires_tenant_ledger BEFORE INSERT ON knowledge_lookup_assets FOR EACH ROW EXECUTE FUNCTION os_guard_a7e3d7f272b8eea9ff2f();

CREATE FUNCTION os_guard_2a487a620dc694e440c5() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
 UPDATE knowledge_lookup_asset_tenant_ledgers AS ledger
 SET asset_identity_count=ledger.asset_identity_count+delta.count
 FROM (SELECT tenant_id,count(*)::bigint AS count FROM new_asset_rows GROUP BY tenant_id) AS delta
 WHERE ledger.tenant_id=delta.tenant_id;
 GET DIAGNOSTICS affected_rows=ROW_COUNT;
 IF affected_rows<>(SELECT count(DISTINCT tenant_id) FROM new_asset_rows) THEN
  RAISE EXCEPTION 'lookup asset statement accounting failed' USING ERRCODE='23514';
 END IF;
 RETURN NULL;
END;
$$;
CREATE TRIGGER knowledge_lookup_asset_identity_accounts_after_insert AFTER INSERT ON knowledge_lookup_assets
 REFERENCING NEW TABLE AS new_asset_rows FOR EACH STATEMENT EXECUTE FUNCTION os_guard_2a487a620dc694e440c5();

CREATE FUNCTION os_guard_419229c40d2c13f1e56d() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    IF NEW . tenant_id <> OLD . tenant_id OR NEW . lookup_asset_id <> OLD . lookup_asset_id OR NEW . created_at_unix_micro <> OLD . created_at_unix_micro OR NEW . current_version <> OLD . current_version + 1 OR NEW . updated_at_unix_micro < OLD . updated_at_unix_micro THEN RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'lookup asset current version transition is invalid'; END IF;
    RETURN NEW;
END
$$;
CREATE TRIGGER knowledge_lookup_asset_identity_transition_is_exact BEFORE UPDATE ON knowledge_lookup_assets FOR EACH ROW EXECUTE FUNCTION os_guard_419229c40d2c13f1e56d();

CREATE FUNCTION os_guard_6b435482159981a6e5ed() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'lookup asset identities cannot be deleted';
    RETURN OLD;
END
$$;
CREATE TRIGGER knowledge_lookup_asset_identity_delete_is_forbidden BEFORE DELETE ON knowledge_lookup_assets FOR EACH ROW EXECUTE FUNCTION os_guard_6b435482159981a6e5ed();

CREATE FUNCTION os_guard_e797f104a41cc1533f5b() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    IF EXISTS ( SELECT 1 FROM knowledge_lookup_asset_tenant_ledgers AS ledger WHERE ledger . tenant_id = NEW . tenant_id ) AND NOT EXISTS ( SELECT 1 FROM knowledge_lookup_asset_tenant_ledgers AS ledger WHERE ledger . tenant_id = NEW . tenant_id AND ledger . published_version_count < 8192 AND ledger . stored_content_bytes <= 2147483648 - NEW . canonical_bytes ) THEN RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'lookup asset version capacity exhausted'; END IF;
    RETURN NEW;
END
$$;
CREATE TRIGGER knowledge_lookup_asset_version_capacity_is_available BEFORE INSERT ON knowledge_lookup_asset_versions FOR EACH ROW EXECUTE FUNCTION os_guard_e797f104a41cc1533f5b();

CREATE FUNCTION os_guard_57399ad61f2b39c7d62a() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    IF NOT EXISTS ( SELECT 1 FROM knowledge_lookup_asset_tenant_ledgers WHERE tenant_id = NEW . tenant_id ) THEN RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'lookup asset tenant ledger is missing'; END IF;
    RETURN NEW;
END
$$;
CREATE TRIGGER knowledge_lookup_asset_version_requires_tenant_ledger BEFORE INSERT ON knowledge_lookup_asset_versions FOR EACH ROW EXECUTE FUNCTION os_guard_57399ad61f2b39c7d62a();

CREATE FUNCTION os_guard_5efd0bca56db433156f3() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    IF NOT ( ( NEW . asset_version = 1 AND EXISTS ( SELECT 1 FROM knowledge_lookup_assets AS asset WHERE asset . tenant_id = NEW . tenant_id AND asset . lookup_asset_id = NEW . lookup_asset_id AND asset . current_version = 1 AND asset . created_at_unix_micro = NEW . created_at_unix_micro AND asset . updated_at_unix_micro = NEW . created_at_unix_micro ) AND NOT EXISTS ( SELECT 1 FROM knowledge_lookup_asset_versions AS prior WHERE prior . tenant_id = NEW . tenant_id AND prior . lookup_asset_id = NEW . lookup_asset_id ) ) OR ( NEW . asset_version > 1 AND EXISTS ( SELECT 1 FROM knowledge_lookup_assets AS asset JOIN knowledge_lookup_asset_versions AS prior ON prior . tenant_id = asset . tenant_id AND prior . lookup_asset_id = asset . lookup_asset_id AND prior . asset_version = NEW . asset_version - 1 WHERE asset . tenant_id = NEW . tenant_id AND asset . lookup_asset_id = NEW . lookup_asset_id AND asset . current_version = NEW . asset_version AND asset . updated_at_unix_micro = NEW . created_at_unix_micro AND prior . created_at_unix_micro <= NEW . created_at_unix_micro ) ) ) THEN RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'lookup asset version sequence is invalid'; END IF;
    RETURN NEW;
END
$$;
CREATE TRIGGER knowledge_lookup_asset_version_sequence_is_exact BEFORE INSERT ON knowledge_lookup_asset_versions FOR EACH ROW EXECUTE FUNCTION os_guard_5efd0bca56db433156f3();

CREATE FUNCTION os_guard_671d4b78f2ef2d60501e() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
 UPDATE knowledge_lookup_asset_tenant_ledgers AS ledger
 SET published_version_count=ledger.published_version_count+delta.count, stored_content_bytes=ledger.stored_content_bytes+delta.bytes
 FROM (SELECT tenant_id,count(*)::bigint AS count,sum(canonical_bytes)::bigint AS bytes FROM new_asset_rows GROUP BY tenant_id) AS delta
 WHERE ledger.tenant_id=delta.tenant_id;
 GET DIAGNOSTICS affected_rows=ROW_COUNT;
 IF affected_rows<>(SELECT count(DISTINCT tenant_id) FROM new_asset_rows) THEN
  RAISE EXCEPTION 'lookup asset statement accounting failed' USING ERRCODE='23514';
 END IF;
 RETURN NULL;
END;
$$;
CREATE TRIGGER knowledge_lookup_asset_version_accounts_after_insert AFTER INSERT ON knowledge_lookup_asset_versions
 REFERENCING NEW TABLE AS new_asset_rows FOR EACH STATEMENT EXECUTE FUNCTION os_guard_671d4b78f2ef2d60501e();

CREATE FUNCTION os_guard_fd8c4c6f3ddbdd9d98f0() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'published lookup asset versions are immutable';
    RETURN NEW;
END
$$;
CREATE TRIGGER knowledge_lookup_asset_version_updates_are_forbidden BEFORE UPDATE ON knowledge_lookup_asset_versions FOR EACH ROW EXECUTE FUNCTION os_guard_fd8c4c6f3ddbdd9d98f0();

CREATE FUNCTION os_guard_30d07884c3070fa1f6e9() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'published lookup asset versions cannot be deleted';
    RETURN OLD;
END
$$;
CREATE TRIGGER knowledge_lookup_asset_version_deletes_are_forbidden BEFORE DELETE ON knowledge_lookup_asset_versions FOR EACH ROW EXECUTE FUNCTION os_guard_30d07884c3070fa1f6e9();

CREATE FUNCTION os_guard_a76b88041763227b9996() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'lookup definition versions are immutable';
    RETURN NEW;
END
$$;
CREATE TRIGGER knowledge_lookup_definition_versions_no_update BEFORE UPDATE ON knowledge_lookup_definition_versions FOR EACH ROW EXECUTE FUNCTION os_guard_a76b88041763227b9996();

CREATE FUNCTION os_guard_856cfcfab6c267e18261() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    IF ( SELECT count ( * ) FROM knowledge_lookup_definitions WHERE tenant_id = NEW . tenant_id ) >= 2048 THEN RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'lookup definition tenant capacity is exhausted'; END IF;
    RETURN NEW;
END
$$;
CREATE TRIGGER knowledge_lookup_definition_tenant_capacity_is_available BEFORE INSERT ON knowledge_lookup_definitions FOR EACH ROW EXECUTE FUNCTION os_guard_856cfcfab6c267e18261();

CREATE FUNCTION os_guard_0d44e5e9ffb81292f604() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    IF ( SELECT count ( * ) FROM knowledge_lookup_definition_versions WHERE tenant_id = NEW . tenant_id ) >= 8192 THEN RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'lookup definition version capacity is exhausted'; END IF;
    RETURN NEW;
END
$$;
CREATE TRIGGER knowledge_lookup_definition_version_capacity_is_available BEFORE INSERT ON knowledge_lookup_definition_versions FOR EACH ROW EXECUTE FUNCTION os_guard_0d44e5e9ffb81292f604();

CREATE FUNCTION os_guard_ee76921155c1ed76d98e() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    IF NEW . mutation_kind IN ( 'CREATE' , 'REPLACE' , 'ENABLE' ) AND ( SELECT count ( * ) FROM knowledge_lookup_definition_versions WHERE tenant_id = NEW . tenant_id ) >= 4096 THEN RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'lookup definition ordinary version capacity is exhausted'; END IF;
    RETURN NEW;
END
$$;
CREATE TRIGGER knowledge_lookup_definition_ordinary_versi_ee76921155c1ed76d98e BEFORE INSERT ON knowledge_lookup_definition_versions FOR EACH ROW EXECUTE FUNCTION os_guard_ee76921155c1ed76d98e();

CREATE FUNCTION os_guard_cf286205788c39ffe06d() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    IF NEW . current_version != 1 OR NEW . state != 'ACTIVE' OR NEW . updated_at_unix_micro != NEW . created_at_unix_micro THEN RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'lookup definition initial shape is invalid'; END IF;
    RETURN NEW;
END
$$;
CREATE TRIGGER knowledge_lookup_definition_initial_shape_is_exact BEFORE INSERT ON knowledge_lookup_definitions FOR EACH ROW EXECUTE FUNCTION os_guard_cf286205788c39ffe06d();

CREATE FUNCTION os_guard_b294e8d0fdf4176131ae() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    IF NOT ( NEW . tenant_id = OLD . tenant_id AND NEW . lookup_id = OLD . lookup_id AND NEW . owner_id = OLD . owner_id AND NEW . created_at_unix_micro = OLD . created_at_unix_micro AND NEW . current_version = OLD . current_version + 1 AND NEW . updated_at_unix_micro > OLD . updated_at_unix_micro AND ( ( OLD . state = 'ACTIVE' AND NEW . state = 'ACTIVE' ) OR ( OLD . state = 'DISABLED' AND NEW . state = 'DISABLED' AND NEW . disabled_at_unix_micro IS NOT DISTINCT FROM OLD . disabled_at_unix_micro ) OR ( NEW . app_id = OLD . app_id AND NEW . name = OLD . name AND NEW . sharing_scope = OLD . sharing_scope AND NEW . automatic = OLD . automatic AND ( ( OLD . state = 'ACTIVE' AND NEW . state = 'DISABLED' AND NEW . disabled_at_unix_micro = NEW . updated_at_unix_micro AND NEW . deleted_at_unix_micro IS NULL ) OR ( OLD . state = 'DISABLED' AND NEW . state = 'ACTIVE' AND NEW . disabled_at_unix_micro IS NULL AND NEW . deleted_at_unix_micro IS NULL ) OR ( OLD . state = 'DISABLED' AND NEW . state = 'DELETED' AND NEW . disabled_at_unix_micro IS NOT DISTINCT FROM OLD . disabled_at_unix_micro AND NEW . deleted_at_unix_micro = NEW . updated_at_unix_micro ) ) ) ) ) THEN RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'lookup definition registry transition is invalid'; END IF;
    RETURN NEW;
END
$$;
CREATE TRIGGER knowledge_lookup_definition_transition_is_valid BEFORE UPDATE ON knowledge_lookup_definitions FOR EACH ROW EXECUTE FUNCTION os_guard_b294e8d0fdf4176131ae();

CREATE FUNCTION os_guard_4879a3184d28a779bb61() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    IF NOT ( EXISTS ( SELECT 1 FROM knowledge_lookup_definitions WHERE tenant_id = NEW . tenant_id AND lookup_id = NEW . lookup_id AND current_version = NEW . definition_version AND state = NEW . state AND disabled_at_unix_micro IS NOT DISTINCT FROM NEW . disabled_at_unix_micro AND deleted_at_unix_micro IS NOT DISTINCT FROM NEW . deleted_at_unix_micro AND updated_at_unix_micro = NEW . created_at_unix_micro ) AND ( ( NEW . definition_version = 1 AND NEW . mutation_kind = 'CREATE' ) OR ( NEW . definition_version > 1 AND EXISTS ( SELECT 1 FROM knowledge_lookup_definition_versions AS previous WHERE previous . tenant_id = NEW . tenant_id AND previous . lookup_id = NEW . lookup_id AND previous . definition_version = NEW . definition_version - 1 AND ( ( NEW . mutation_kind = 'REPLACE' AND previous . state = NEW . state AND previous . disabled_at_unix_micro IS NOT DISTINCT FROM NEW . disabled_at_unix_micro AND previous . deleted_at_unix_micro IS NOT DISTINCT FROM NEW . deleted_at_unix_micro AND ( NEW . lookup_asset_id != previous . lookup_asset_id OR NEW . asset_version != previous . asset_version OR NEW . asset_size_bytes != previous . asset_size_bytes OR NEW . asset_content_sha256 != previous . asset_content_sha256 OR NEW . definition_proto != previous . definition_proto OR NEW . columns_blob != previous . columns_blob ) ) OR ( NEW . mutation_kind = 'ENABLE' AND previous . state = 'DISABLED' AND NEW . state = 'ACTIVE' AND NEW . lookup_asset_id = previous . lookup_asset_id AND NEW . asset_version = previous . asset_version AND NEW . asset_size_bytes = previous . asset_size_bytes AND NEW . asset_content_sha256 = previous . asset_content_sha256 AND NEW . definition_proto = previous . definition_proto AND NEW . columns_blob = previous . columns_blob ) OR ( NEW . mutation_kind = 'DISABLE' AND previous . state = 'ACTIVE' AND NEW . state = 'DISABLED' AND NEW . lookup_asset_id = previous . lookup_asset_id AND NEW . asset_version = previous . asset_version AND NEW . asset_size_bytes = previous . asset_size_bytes AND NEW . asset_content_sha256 = previous . asset_content_sha256 AND NEW . definition_proto = previous . definition_proto AND NEW . columns_blob = previous . columns_blob ) OR ( NEW . mutation_kind = 'DELETE' AND previous . state = 'DISABLED' AND NEW . state = 'DELETED' AND NEW . disabled_at_unix_micro IS NOT DISTINCT FROM previous . disabled_at_unix_micro AND NEW . lookup_asset_id = previous . lookup_asset_id AND NEW . asset_version = previous . asset_version AND NEW . asset_size_bytes = previous . asset_size_bytes AND NEW . asset_content_sha256 = previous . asset_content_sha256 AND NEW . definition_proto = previous . definition_proto AND NEW . columns_blob = previous . columns_blob ) ) ) ) ) ) THEN RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'lookup definition version is not the current registry authority'; END IF;
    RETURN NEW;
END
$$;
CREATE TRIGGER knowledge_lookup_definition_version_matches_current_registry BEFORE INSERT ON knowledge_lookup_definition_versions FOR EACH ROW EXECUTE FUNCTION os_guard_4879a3184d28a779bb61();

CREATE FUNCTION os_guard_adf519927704aa49dc20() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'lookup definition versions are retained';
    RETURN OLD;
END
$$;
CREATE TRIGGER knowledge_lookup_definition_versions_no_delete BEFORE DELETE ON knowledge_lookup_definition_versions FOR EACH ROW EXECUTE FUNCTION os_guard_adf519927704aa49dc20();

CREATE FUNCTION os_guard_388a03c509d20d40aece() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    IF NEW . state = 'archived' AND EXISTS ( SELECT 1 FROM knowledge_lookup_definitions WHERE tenant_id = OLD . tenant_id AND app_id = OLD . app_id AND state = 'ACTIVE' ) THEN RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'app workspace has active lookup definitions'; END IF;
    RETURN NEW;
END
$$;
CREATE TRIGGER knowledge_lookup_active_app_workspace_cannot_be_archived BEFORE UPDATE OF state ON app_workspaces FOR EACH ROW EXECUTE FUNCTION os_guard_388a03c509d20d40aece();

CREATE FUNCTION os_guard_c8e2f063cf0650358009() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    IF EXISTS ( SELECT 1 FROM knowledge_lookup_definitions WHERE tenant_id = OLD . tenant_id AND app_id = OLD . app_id ) THEN RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'app workspace is referenced by lookup definitions'; END IF;
    RETURN OLD;
END
$$;
CREATE TRIGGER knowledge_lookup_referenced_app_workspace_cannot_be_deleted BEFORE DELETE ON app_workspaces FOR EACH ROW EXECUTE FUNCTION os_guard_c8e2f063cf0650358009();

CREATE FUNCTION os_guard_911b830e24130296d979() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    IF NOT EXISTS ( SELECT 1 FROM app_workspaces WHERE tenant_id = NEW . tenant_id AND app_id = NEW . app_id AND state = 'active' ) THEN RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'lookup definition requires an active app workspace'; END IF;
    RETURN NEW;
END
$$;
CREATE TRIGGER knowledge_lookup_active_definition_requires_active_app_insert BEFORE INSERT ON knowledge_lookup_definitions FOR EACH ROW EXECUTE FUNCTION os_guard_911b830e24130296d979();

CREATE FUNCTION os_guard_4c6693f2e1d7767e4653() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    IF NOT EXISTS ( SELECT 1 FROM app_workspaces WHERE tenant_id = NEW . tenant_id AND app_id = NEW . app_id AND state = 'active' ) AND NOT ( OLD . state = 'DISABLED' AND NEW . state = 'DELETED' AND NEW . tenant_id = OLD . tenant_id AND NEW . lookup_id = OLD . lookup_id AND NEW . owner_id = OLD . owner_id AND NEW . app_id = OLD . app_id AND NEW . name = OLD . name AND NEW . sharing_scope = OLD . sharing_scope AND NEW . automatic = OLD . automatic AND NEW . created_at_unix_micro = OLD . created_at_unix_micro ) THEN RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'lookup definition update requires an active app workspace'; END IF;
    RETURN NEW;
END
$$;
CREATE TRIGGER knowledge_lookup_definition_update_requires_active_app BEFORE UPDATE ON knowledge_lookup_definitions FOR EACH ROW EXECUTE FUNCTION os_guard_4c6693f2e1d7767e4653();

CREATE FUNCTION os_guard_0bc717c12acb555550d2() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    IF NOT ( ( NEW . knowledge_snapshot_sha256 IS NULL AND NEW . knowledge_snapshot_tenant_catalog_revision IS NULL AND NEW . knowledge_snapshot_tenant_catalog_state_token IS NULL AND NEW . knowledge_snapshot_object_count IS NULL AND NEW . knowledge_snapshot_lookup_asset_count IS NULL ) OR ( NEW . knowledge_snapshot_sha256 IS NOT NULL AND NEW . knowledge_snapshot_tenant_catalog_revision IS NOT NULL AND NEW . knowledge_snapshot_tenant_catalog_state_token IS NOT NULL AND NEW . knowledge_snapshot_object_count IS NOT NULL AND NEW . knowledge_snapshot_lookup_asset_count IS NOT NULL ) ) THEN RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'search-attempt audit knowledge snapshot must be absent or exact'; END IF;
    RETURN NEW;
END
$$;
CREATE TRIGGER search_attempt_audit_event_knowledge_snapshot_is_complete BEFORE INSERT ON search_attempt_audit_events FOR EACH ROW EXECUTE FUNCTION os_guard_0bc717c12acb555550d2();

CREATE FUNCTION os_guard_43c7d430a699461affa4() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'search-attempt audit events cannot be updated';
    RETURN NEW;
END
$$;
CREATE TRIGGER search_attempt_audit_event_update_is_forbidden BEFORE UPDATE ON search_attempt_audit_events FOR EACH ROW EXECUTE FUNCTION os_guard_43c7d430a699461affa4();

CREATE FUNCTION os_guard_e3b9fa5d642583445e00() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    IF ( SELECT count ( * ) FROM dashboards WHERE organization_id=NEW.organization_id AND tenant_id = NEW . tenant_id AND owner_id = NEW . owner_id ) >= 64 THEN RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'dashboard owner capacity exhausted'; END IF;
    RETURN NEW;
END
$$;
CREATE TRIGGER dashboards_owner_capacity_insert BEFORE INSERT ON dashboards FOR EACH ROW EXECUTE FUNCTION os_guard_e3b9fa5d642583445e00();

CREATE FUNCTION os_guard_b3cb47b413d87bdc37f3() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    IF EXISTS ( SELECT 1 FROM dashboards WHERE tenant_id = OLD . tenant_id AND app_id = OLD . app_id ) THEN RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'app is referenced by a dashboard'; END IF;
    RETURN OLD;
END
$$;
CREATE TRIGGER app_workspace_delete_restrict_dashboards BEFORE DELETE ON app_workspaces FOR EACH ROW EXECUTE FUNCTION os_guard_b3cb47b413d87bdc37f3();

CREATE FUNCTION os_guard_e1f1e5ced9ab5a013190() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    IF length ( NEW . app_id ) = 26 AND substr ( NEW . app_id , 1 , 4 ) = 'app_' AND substr ( NEW . app_id , 5 ) !~ '^.*[^A-Za-z0-9_-].*$' AND substr ( NEW . app_id , 26 , 1 ) ~ '^[AQgw]$' AND NOT EXISTS ( SELECT 1 FROM app_workspaces WHERE tenant_id = NEW . tenant_id AND app_id = NEW . app_id ) THEN RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'canonical dashboard app does not exist'; END IF;
    RETURN NEW;
END
$$;
CREATE TRIGGER canonical_dashboard_app_exists_insert BEFORE INSERT ON dashboards FOR EACH ROW EXECUTE FUNCTION os_guard_e1f1e5ced9ab5a013190();

CREATE FUNCTION os_guard_8254350ee953f79ee273() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    IF length ( NEW . app_id ) = 26 AND substr ( NEW . app_id , 1 , 4 ) = 'app_' AND substr ( NEW . app_id , 5 ) !~ '^.*[^A-Za-z0-9_-].*$' AND substr ( NEW . app_id , 26 , 1 ) ~ '^[AQgw]$' AND NOT EXISTS ( SELECT 1 FROM app_workspaces WHERE tenant_id = NEW . tenant_id AND app_id = NEW . app_id ) THEN RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'canonical dashboard app does not exist'; END IF;
    RETURN NEW;
END
$$;
CREATE TRIGGER canonical_dashboard_app_exists_update BEFORE UPDATE OF tenant_id, app_id ON dashboards FOR EACH ROW EXECUTE FUNCTION os_guard_8254350ee953f79ee273();

CREATE FUNCTION os_guard_8e32e17013df9edce237() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    IF NEW . first_sequence <> 1 OR NEW . next_sequence <> 1 OR NEW . retained_count <> 0 OR NEW . last_occurred_at_unix_micro IS NOT NULL THEN RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'feature-operation audit tenant state must begin empty'; END IF;
    RETURN NEW;
END
$$;
CREATE TRIGGER feature_operation_audit_state_initial_shape_is_valid BEFORE INSERT ON feature_operation_audit_tenant_state FOR EACH ROW EXECUTE FUNCTION os_guard_8e32e17013df9edce237();

CREATE FUNCTION os_guard_c1a47a2a1ca47d635cb4() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    IF NOT ( NEW . tenant_id = OLD . tenant_id AND ( ( OLD . retained_count BETWEEN 0 AND 100000 AND NEW . first_sequence = OLD . first_sequence AND NEW . next_sequence = OLD . next_sequence + 1 AND NEW . retained_count = OLD . retained_count + 1 AND NEW . last_occurred_at_unix_micro = ( SELECT occurred_at_unix_micro FROM feature_operation_audit_events WHERE tenant_id = OLD . tenant_id AND sequence = OLD . next_sequence ) ) OR ( OLD . retained_count = 100001 AND NEW . first_sequence = OLD . first_sequence + 1 AND NEW . next_sequence = OLD . next_sequence AND NEW . retained_count = 100000 AND NEW . last_occurred_at_unix_micro = OLD . last_occurred_at_unix_micro ) ) ) THEN RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'feature-operation audit tenant state transition is invalid'; END IF;
    RETURN NEW;
END
$$;
CREATE TRIGGER feature_operation_audit_state_transition_is_valid BEFORE UPDATE ON feature_operation_audit_tenant_state FOR EACH ROW EXECUTE FUNCTION os_guard_c1a47a2a1ca47d635cb4();

CREATE FUNCTION os_guard_76ec3cf62340a6fda067() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'feature-operation audit tenant state cannot be deleted';
    RETURN OLD;
END
$$;
CREATE TRIGGER feature_operation_audit_state_delete_is_forbidden BEFORE DELETE ON feature_operation_audit_tenant_state FOR EACH ROW EXECUTE FUNCTION os_guard_76ec3cf62340a6fda067();

CREATE FUNCTION os_guard_126924c28fa0f5548159() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    IF NOT EXISTS ( SELECT 1 FROM feature_operation_audit_tenant_state WHERE tenant_id = NEW . tenant_id AND retained_count BETWEEN 0 AND 100000 AND next_sequence = NEW . sequence AND next_sequence < 9223372036854775807 AND ( last_occurred_at_unix_micro IS NULL OR NEW . occurred_at_unix_micro >= last_occurred_at_unix_micro ) ) THEN RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'feature-operation audit tenant state is invalid or sequence is exhausted'; END IF;
    RETURN NEW;
END
$$;
CREATE TRIGGER feature_operation_audit_event_insert_requires_current_state BEFORE INSERT ON feature_operation_audit_events FOR EACH ROW EXECUTE FUNCTION os_guard_126924c28fa0f5548159();

-- Exact rolling-retention probes must use the tenant/sequence primary key.
CREATE FUNCTION os_guard_81a759e9efd899c3a68a() RETURNS trigger LANGUAGE plpgsql SET enable_seqscan=off AS $$
DECLARE affected_rows BIGINT;
BEGIN
    UPDATE feature_operation_audit_tenant_state SET next_sequence = next_sequence + 1 , retained_count = retained_count + 1 , last_occurred_at_unix_micro = NEW . occurred_at_unix_micro WHERE tenant_id = NEW . tenant_id AND next_sequence = NEW . sequence AND retained_count BETWEEN 0 AND 100000 ; GET DIAGNOSTICS affected_rows = ROW_COUNT; IF affected_rows <> 1 THEN RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'feature-operation audit event accounting failed'; END IF; DELETE FROM feature_operation_audit_events WHERE tenant_id = NEW . tenant_id AND sequence = ( SELECT first_sequence FROM feature_operation_audit_tenant_state WHERE tenant_id = NEW . tenant_id AND retained_count = 100001 ) ;
    RETURN NEW;
END
$$;
CREATE TRIGGER feature_operation_audit_event_advances_and_prunes_tenant_state AFTER INSERT ON feature_operation_audit_events FOR EACH ROW EXECUTE FUNCTION os_guard_81a759e9efd899c3a68a();

CREATE FUNCTION os_guard_7ac1cdf60f274c3284c3() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'feature-operation audit events cannot be updated';
    RETURN NEW;
END
$$;
CREATE TRIGGER feature_operation_audit_event_update_is_forbidden BEFORE UPDATE ON feature_operation_audit_events FOR EACH ROW EXECUTE FUNCTION os_guard_7ac1cdf60f274c3284c3();

CREATE FUNCTION os_guard_74345ebebacaf2e0efde() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    IF NOT EXISTS ( SELECT 1 FROM feature_operation_audit_tenant_state WHERE tenant_id = OLD . tenant_id AND retained_count = 100001 AND first_sequence = OLD . sequence ) THEN RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'feature-operation audit events can only be deleted by rolling prune'; END IF;
    RETURN OLD;
END
$$;
CREATE TRIGGER feature_operation_audit_event_delete_requires_rolling_prune BEFORE DELETE ON feature_operation_audit_events FOR EACH ROW EXECUTE FUNCTION os_guard_74345ebebacaf2e0efde();

CREATE FUNCTION os_guard_e2e045e0d7f8fe32fae4() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    UPDATE feature_operation_audit_tenant_state SET first_sequence = first_sequence + 1 , retained_count = retained_count - 1 WHERE tenant_id = OLD . tenant_id AND first_sequence = OLD . sequence AND retained_count = 100001 ; GET DIAGNOSTICS affected_rows = ROW_COUNT; IF affected_rows <> 1 THEN RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'feature-operation audit rolling prune failed'; END IF;
    RETURN OLD;
END
$$;
CREATE TRIGGER feature_operation_audit_event_prune_advances_tenant_state AFTER DELETE ON feature_operation_audit_events FOR EACH ROW EXECUTE FUNCTION os_guard_e2e045e0d7f8fe32fae4();

CREATE FUNCTION os_guard_bdada8bc5dcc68207a8c() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    IF NOT ( ( NEW . state IN ( 'reserved' , 'committed' ) AND length ( NEW . outbox_sha256 ) = 32 AND NEW . stored_row_count BETWEEN 1 AND 1000 AND NEW . decoded_event_bytes BETWEEN 1 AND 8388608 ) OR ( NEW . state = 'rejected' AND length ( NEW . outbox_sha256 ) = 0 AND NEW . stored_row_count = 0 AND NEW . decoded_event_bytes = 0 ) OR ( NEW . state = 'abandoned' AND ( ( length ( NEW . outbox_sha256 ) = 32 AND NEW . stored_row_count BETWEEN 1 AND 1000 AND NEW . decoded_event_bytes BETWEEN 1 AND 8388608 ) OR ( length ( NEW . outbox_sha256 ) = 0 AND NEW . stored_row_count = 0 AND NEW . decoded_event_bytes = 0 ) ) ) ) THEN RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'invalid ingest visibility reservation accounting'; END IF;
    RETURN NEW;
END
$$;
CREATE TRIGGER ingest_visibility_reservation_accounting_insert_is_valid BEFORE INSERT ON ingest_visibility_reservations FOR EACH ROW EXECUTE FUNCTION os_guard_bdada8bc5dcc68207a8c();

CREATE FUNCTION os_guard_5d2d94a416eb6d383f6a() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    IF NOT ( ( NEW . state IN ( 'reserved' , 'committed' ) AND length ( NEW . outbox_sha256 ) = 32 AND NEW . stored_row_count BETWEEN 1 AND 1000 AND NEW . decoded_event_bytes BETWEEN 1 AND 8388608 ) OR ( NEW . state = 'rejected' AND length ( NEW . outbox_sha256 ) = 0 AND NEW . stored_row_count = 0 AND NEW . decoded_event_bytes = 0 ) OR ( NEW . state = 'abandoned' AND ( ( length ( NEW . outbox_sha256 ) = 32 AND NEW . stored_row_count BETWEEN 1 AND 1000 AND NEW . decoded_event_bytes BETWEEN 1 AND 8388608 ) OR ( length ( NEW . outbox_sha256 ) = 0 AND NEW . stored_row_count = 0 AND NEW . decoded_event_bytes = 0 ) ) ) OR ( length ( OLD . outbox_sha256 ) = 0 AND OLD . stored_row_count = 0 AND OLD . decoded_event_bytes = 0 AND length ( NEW . outbox_sha256 ) = 0 AND NEW . stored_row_count = 0 AND NEW . decoded_event_bytes = 0 ) ) THEN RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'invalid ingest visibility reservation accounting'; END IF;
    RETURN NEW;
END
$$;
CREATE TRIGGER ingest_visibility_reservation_accounting_update_is_valid BEFORE UPDATE OF state, outbox_sha256, stored_row_count, decoded_event_bytes ON ingest_visibility_reservations FOR EACH ROW EXECUTE FUNCTION os_guard_5d2d94a416eb6d383f6a();

CREATE FUNCTION os_guard_be40adc98a7090f64380() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    IF NOT ( ( OLD . state = 'ready' AND NEW . state = 'ambiguous' ) OR ( OLD . state = 'ambiguous' AND NEW . state = 'committed' ) ) THEN RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'invalid ingest write group state transition'; END IF;
    RETURN NEW;
END
$$;
CREATE TRIGGER ingest_write_group_state_transition_is_valid BEFORE UPDATE OF state ON ingest_write_groups FOR EACH ROW EXECUTE FUNCTION os_guard_be40adc98a7090f64380();

CREATE FUNCTION os_guard_e8be0f65241f4985b2d0() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'ingest write group identity is immutable';
    RETURN NEW;
END
$$;
CREATE TRIGGER ingest_write_group_identity_is_immutable BEFORE UPDATE OF write_group_id, member_count, row_count, decoded_bytes,
    membership_sha256, first_sequence, last_sequence, created_at_unix_micro ON ingest_write_groups FOR EACH ROW EXECUTE FUNCTION os_guard_e8be0f65241f4985b2d0();

CREATE FUNCTION os_guard_6f4c08b868142871f733() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    IF OLD . state = 'ready' AND NEW . state = 'ambiguous' AND ( OLD . member_count <> ( SELECT count ( * ) FROM ingest_write_group_members WHERE write_group_id = OLD . write_group_id ) OR OLD . row_count <> ( SELECT COALESCE ( sum ( row_count ) , 0 ) FROM ingest_write_group_members WHERE write_group_id = OLD . write_group_id ) OR OLD . decoded_bytes <> ( SELECT COALESCE ( sum ( decoded_bytes ) , 0 ) FROM ingest_write_group_members WHERE write_group_id = OLD . write_group_id ) OR OLD . first_sequence <> ( SELECT min ( visibility_sequence ) FROM ingest_write_group_members WHERE write_group_id = OLD . write_group_id ) OR OLD . last_sequence <> ( SELECT max ( visibility_sequence ) FROM ingest_write_group_members WHERE write_group_id = OLD . write_group_id ) ) THEN RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'ingest write group seal does not match its members'; END IF;
    RETURN NEW;
END
$$;
CREATE TRIGGER ingest_write_group_seal_is_valid BEFORE UPDATE OF state ON ingest_write_groups FOR EACH ROW EXECUTE FUNCTION os_guard_6f4c08b868142871f733();

CREATE FUNCTION os_guard_5682e237ae9f0e2cb2a8() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    IF NOT EXISTS ( SELECT 1 FROM ingest_write_groups AS write_group JOIN ingest_visibility_reservations AS reservation ON reservation . sequence = NEW . visibility_sequence WHERE write_group . write_group_id = NEW . write_group_id AND write_group . state = 'ready' AND reservation . state = 'reserved' AND reservation . phase = 'unsent' AND reservation . attempt_id = '' AND reservation . stored_row_count = NEW . row_count AND reservation . decoded_event_bytes = NEW . decoded_bytes AND reservation . outbox_sha256 = NEW . outbox_sha256 ) THEN RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'invalid ingest write group member'; END IF;
    RETURN NEW;
END
$$;
CREATE TRIGGER ingest_write_group_member_insert_is_valid BEFORE INSERT ON ingest_write_group_members FOR EACH ROW EXECUTE FUNCTION os_guard_5682e237ae9f0e2cb2a8();

CREATE FUNCTION os_guard_e3dba0c2cad99ef8fa3f() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    IF NEW . ordinal <> ( SELECT count ( * ) FROM ingest_write_group_members WHERE write_group_id = NEW . write_group_id ) OR NEW . ordinal >= ( SELECT member_count FROM ingest_write_groups WHERE write_group_id = NEW . write_group_id ) THEN RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'ingest write group ordinals must be contiguous'; END IF;
    RETURN NEW;
END
$$;
CREATE TRIGGER ingest_write_group_member_insert_is_contiguous BEFORE INSERT ON ingest_write_group_members FOR EACH ROW EXECUTE FUNCTION os_guard_e3dba0c2cad99ef8fa3f();

CREATE FUNCTION os_guard_4bda1b9dad2b32fa0def() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    IF NEW . ordinal > 0 AND NEW . visibility_sequence <= ( SELECT max ( visibility_sequence ) FROM ingest_write_group_members WHERE write_group_id = NEW . write_group_id ) THEN RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'ingest write group member sequences must be ordered'; END IF;
    RETURN NEW;
END
$$;
CREATE TRIGGER ingest_write_group_member_insert_is_ordered BEFORE INSERT ON ingest_write_group_members FOR EACH ROW EXECUTE FUNCTION os_guard_4bda1b9dad2b32fa0def();

CREATE FUNCTION os_guard_9b141b157cc139b1f5b9() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'ingest write group membership is immutable';
    RETURN NEW;
END
$$;
CREATE TRIGGER ingest_write_group_member_is_immutable BEFORE UPDATE ON ingest_write_group_members FOR EACH ROW EXECUTE FUNCTION os_guard_9b141b157cc139b1f5b9();

CREATE FUNCTION os_guard_9fbd6289cfd0cb8f86e9() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    IF ( SELECT state FROM ingest_write_groups WHERE write_group_id = OLD . write_group_id ) <> 'committed' THEN RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'active ingest write group membership cannot be deleted'; END IF;
    RETURN OLD;
END
$$;
CREATE TRIGGER ingest_write_group_active_member_delete_is_forbidden BEFORE DELETE ON ingest_write_group_members FOR EACH ROW EXECUTE FUNCTION os_guard_9fbd6289cfd0cb8f86e9();

CREATE FUNCTION os_guard_111baf81100a1f95a3fa() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    IF NEW . principal_sha256 <> OLD . principal_sha256 THEN RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'ingest visibility principal is immutable'; END IF;
    RETURN NEW;
END
$$;
CREATE TRIGGER ingest_visibility_principal_is_immutable BEFORE UPDATE OF principal_sha256 ON ingest_visibility_reservations FOR EACH ROW EXECUTE FUNCTION os_guard_111baf81100a1f95a3fa();

-- The native (tenant_id, sequence) primary key enforces audit identity uniqueness.
CREATE FUNCTION os_guard_0c5f3eae0446b29022fe() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM audit_tenant_state WHERE tenant_id = NEW.tenant_id AND event_count BETWEEN 0 AND 99999 AND next_sequence = event_count + 1) THEN
        RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'audit tenant state is invalid or capacity is exhausted';
    END IF;
    RETURN NEW;
END
$$;
CREATE TRIGGER audit_event_insert_requires_current_tenant_state BEFORE INSERT ON audit_events FOR EACH ROW EXECUTE FUNCTION os_guard_0c5f3eae0446b29022fe();

CREATE FUNCTION os_guard_61e891494679beb13192() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE changed RECORD; affected_rows BIGINT;
BEGIN
    FOR changed IN SELECT tenant_id, count(*) AS events, min(sequence) AS first_sequence, max(sequence) AS last_sequence FROM inserted_audit_events GROUP BY tenant_id LOOP
        UPDATE audit_tenant_state SET next_sequence = next_sequence + changed.events, event_count = event_count + changed.events
        WHERE tenant_id = changed.tenant_id AND next_sequence = changed.first_sequence AND event_count = changed.first_sequence - 1
          AND changed.last_sequence = changed.first_sequence + changed.events - 1;
        GET DIAGNOSTICS affected_rows = ROW_COUNT;
        IF affected_rows <> 1 THEN RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'audit event accounting failed'; END IF;
    END LOOP;
    RETURN NULL;
END
$$;
CREATE TRIGGER audit_event_advances_tenant_state AFTER INSERT ON audit_events REFERENCING NEW TABLE AS inserted_audit_events FOR EACH STATEMENT EXECUTE FUNCTION os_guard_61e891494679beb13192();

CREATE FUNCTION os_guard_15d0722c944f72bd487a() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'audit events cannot be updated';
    RETURN NEW;
END
$$;
CREATE TRIGGER audit_event_update_is_forbidden BEFORE UPDATE ON audit_events FOR EACH ROW EXECUTE FUNCTION os_guard_15d0722c944f72bd487a();

CREATE FUNCTION os_guard_90ac7543683b1fa1cfbc() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'audit events cannot be deleted';
    RETURN OLD;
END
$$;
CREATE TRIGGER audit_event_delete_is_forbidden BEFORE DELETE ON audit_events FOR EACH ROW EXECUTE FUNCTION os_guard_90ac7543683b1fa1cfbc();

CREATE FUNCTION os_guard_e072e1494ed59d1c901e() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF NOT (NEW.tenant_id = OLD.tenant_id AND OLD.event_count BETWEEN 0 AND 99999 AND OLD.next_sequence = OLD.event_count + 1
       AND NEW.event_count > OLD.event_count AND NEW.next_sequence = NEW.event_count + 1
       AND NEW.event_count = (SELECT max(sequence) FROM audit_events WHERE tenant_id = NEW.tenant_id)) THEN
        RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'audit tenant state transition is invalid';
    END IF;
    RETURN NEW;
END
$$;
CREATE TRIGGER audit_tenant_state_transition_is_valid BEFORE UPDATE ON audit_tenant_state FOR EACH ROW EXECUTE FUNCTION os_guard_e072e1494ed59d1c901e();

CREATE FUNCTION os_guard_f52ba8535d3ac0b6ad52() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    IF NOT EXISTS ( SELECT 1 FROM knowledge_catalog_tenants AS tenant JOIN knowledge_catalog_revision_heads AS head ON head . tenant_id = tenant . tenant_id AND head . catalog_revision = tenant . catalog_revision JOIN knowledge_object_versions AS version ON version . tenant_id = tenant . tenant_id AND version . knowledge_object_id = NEW . knowledge_object_id AND version . object_version = NEW . object_version JOIN knowledge_object_version_lifecycle AS lifecycle ON lifecycle . tenant_id = version . tenant_id AND lifecycle . knowledge_object_id = version . knowledge_object_id AND lifecycle . object_version = version . object_version AND lifecycle . state = version . state JOIN knowledge_objects AS current ON current . tenant_id = version . tenant_id AND current . knowledge_object_id = version . knowledge_object_id AND current . current_version = version . object_version AND current . app_id = version . app_id AND current . owner_id = version . owner_id AND current . object_type = version . object_type AND current . name = version . name AND current . sharing_scope = version . sharing_scope AND current . state = version . state AND current . definition_digest_key = version . definition_digest_key AND current . updated_at_unix_micro = version . created_at_unix_micro AND current . disabled_at_unix_micro IS NOT DISTINCT FROM lifecycle . disabled_at_unix_micro AND current . quarantined_at_unix_micro IS NOT DISTINCT FROM lifecycle . quarantined_at_unix_micro AND current . deleted_at_unix_micro IS NOT DISTINCT FROM lifecycle . deleted_at_unix_micro AND current . quarantine_reason IS NOT DISTINCT FROM lifecycle . quarantine_reason WHERE tenant . tenant_id = NEW . tenant_id AND tenant . catalog_revision = NEW . catalog_revision AND head . state_token = NEW . catalog_state_token AND length ( head . state_token ) = 32 AND version . mutation_kind = NEW . mutation_kind AND version . created_at_unix_micro = NEW . occurred_at_unix_micro AND ( ( NEW . mutation_kind <> 'quarantine' AND NEW . recovery_audit_sequence IS NULL AND EXISTS ( SELECT 1 FROM audit_events AS event WHERE event . tenant_id = version . tenant_id AND event . sequence = NEW . successful_audit_sequence AND event . actor_kind = NEW . actor_kind AND event . actor_id = NEW . actor_id AND event . occurred_at_unix_micro = version . created_at_unix_micro AND event . target_kind = 'knowledge_object' AND event . target_id = version . knowledge_object_id AND event . target_version = version . object_version AND event . app_id = version . app_id AND event . object_type = version . object_type AND event . sharing_scope = version . sharing_scope AND event . action = CASE NEW . mutation_kind WHEN 'create' THEN 'knowledge.object.create' WHEN 'update' THEN 'knowledge.object.update' WHEN 'scope_change' THEN 'knowledge.object.scope_change' WHEN 'enable' THEN 'knowledge.object.enable' WHEN 'disable' THEN 'knowledge.object.disable' WHEN 'delete' THEN 'knowledge.object.delete' END ) ) OR ( NEW . mutation_kind = 'quarantine' AND NEW . successful_audit_sequence IS NULL AND EXISTS ( SELECT 1 FROM knowledge_recovery_audit AS recovery WHERE recovery . tenant_id = version . tenant_id AND recovery . sequence = NEW . recovery_audit_sequence AND recovery . actor_kind = NEW . actor_kind AND recovery . actor_id = NEW . actor_id AND recovery . occurred_at_unix_micro = version . created_at_unix_micro AND recovery . knowledge_object_id = version . knowledge_object_id AND recovery . object_version = version . object_version AND recovery . app_id = version . app_id AND recovery . object_type = version . object_type AND recovery . sharing_scope = version . sharing_scope AND recovery . recovery_reason = version . quarantine_reason ) ) ) ) THEN RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'knowledge mutation commit authority is invalid'; END IF;
    RETURN NEW;
END
$$;
CREATE TRIGGER knowledge_mutation_commit_authority_is_exact BEFORE INSERT ON knowledge_mutation_commit_authorities FOR EACH ROW EXECUTE FUNCTION os_guard_f52ba8535d3ac0b6ad52();

CREATE FUNCTION os_guard_9fd90e46627353e10373() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    IF ( NEW . mutation_kind <> 'quarantine' AND NOT EXISTS ( SELECT 1 FROM knowledge_object_versions AS version JOIN audit_events AS event ON event . tenant_id = version . tenant_id AND event . sequence = NEW . successful_audit_sequence AND event . occurred_at_unix_micro = version . created_at_unix_micro AND event . actor_kind = NEW . actor_kind AND event . actor_id = NEW . actor_id AND event . target_kind = 'knowledge_object' AND event . target_id = version . knowledge_object_id AND event . target_version = version . object_version AND event . app_id = version . app_id AND event . object_type = version . object_type AND event . sharing_scope = version . sharing_scope AND event . action = CASE NEW . mutation_kind WHEN 'create' THEN 'knowledge.object.create' WHEN 'update' THEN 'knowledge.object.update' WHEN 'scope_change' THEN 'knowledge.object.scope_change' WHEN 'enable' THEN 'knowledge.object.enable' WHEN 'disable' THEN 'knowledge.object.disable' WHEN 'delete' THEN 'knowledge.object.delete' END WHERE version . tenant_id = NEW . tenant_id AND version . knowledge_object_id = NEW . knowledge_object_id AND version . object_version = NEW . object_version AND version . mutation_kind = NEW . mutation_kind ) ) OR ( NEW . mutation_kind = 'quarantine' AND NOT EXISTS ( SELECT 1 FROM knowledge_object_versions AS version JOIN knowledge_recovery_audit AS recovery ON recovery . tenant_id = version . tenant_id AND recovery . sequence = NEW . recovery_audit_sequence AND recovery . occurred_at_unix_micro = version . created_at_unix_micro AND recovery . actor_kind = NEW . actor_kind AND recovery . actor_id = NEW . actor_id AND recovery . knowledge_object_id = version . knowledge_object_id AND recovery . object_version = version . object_version AND recovery . app_id = version . app_id AND recovery . object_type = version . object_type AND recovery . sharing_scope = version . sharing_scope AND recovery . recovery_reason = version . quarantine_reason WHERE version . tenant_id = NEW . tenant_id AND version . knowledge_object_id = NEW . knowledge_object_id AND version . object_version = NEW . object_version AND version . mutation_kind = 'quarantine' ) ) THEN RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'knowledge mutation idempotency audit authority is invalid'; END IF;
    RETURN NEW;
END
$$;
CREATE TRIGGER knowledge_mutation_idempotency_matches_audit_authority BEFORE INSERT ON knowledge_mutation_idempotency FOR EACH ROW EXECUTE FUNCTION os_guard_9fd90e46627353e10373();

CREATE FUNCTION os_guard_8afce501a4f94ff3a539() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    IF NEW . receipt_count <> 0 OR NEW . encoded_bytes <> 0 THEN RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'API mutation receipt tenant state must begin empty'; END IF;
    RETURN NEW;
END
$$;
CREATE TRIGGER api_mutation_receipt_tenant_state_initial_shape_is_valid BEFORE INSERT ON api_mutation_receipt_tenant_state FOR EACH ROW EXECUTE FUNCTION os_guard_8afce501a4f94ff3a539();

CREATE FUNCTION os_guard_8c7becedba8186e856bf() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    IF NOT ( NEW . tenant_id = OLD . tenant_id AND NEW . high_water_unix_micro >= OLD . high_water_unix_micro AND NEW . receipt_count = ( SELECT count ( * ) FROM api_mutation_receipts WHERE tenant_id = OLD . tenant_id ) AND NEW . encoded_bytes = COALESCE ( ( SELECT sum ( encoded_bytes ) FROM api_mutation_receipts WHERE tenant_id = OLD . tenant_id ) , 0 ) ) THEN RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'API mutation receipt tenant state transition is invalid'; END IF;
    RETURN NEW;
END
$$;
CREATE TRIGGER api_mutation_receipt_tenant_state_transition_is_valid BEFORE UPDATE ON api_mutation_receipt_tenant_state FOR EACH ROW EXECUTE FUNCTION os_guard_8c7becedba8186e856bf();

CREATE FUNCTION os_guard_15e1f00024187051815a() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'API mutation receipt tenant state cannot be deleted';
    RETURN OLD;
END
$$;
CREATE TRIGGER api_mutation_receipt_tenant_state_delete_is_forbidden BEFORE DELETE ON api_mutation_receipt_tenant_state FOR EACH ROW EXECUTE FUNCTION os_guard_15e1f00024187051815a();

CREATE FUNCTION os_guard_c25988e923f2bd2973c3() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    IF NOT EXISTS ( SELECT 1 FROM api_mutation_receipt_tenant_state WHERE tenant_id = NEW . tenant_id AND receipt_count BETWEEN 0 AND 99999 AND encoded_bytes <= 67108864 - NEW . encoded_bytes AND high_water_unix_micro >= NEW . created_at_unix_micro ) THEN RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'API mutation receipt capacity is exhausted'; END IF;
    RETURN NEW;
END
$$;
CREATE TRIGGER api_mutation_receipt_capacity_is_available BEFORE INSERT ON api_mutation_receipts FOR EACH ROW EXECUTE FUNCTION os_guard_c25988e923f2bd2973c3();

CREATE FUNCTION os_guard_06e7b70c2a336cc00f5c() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE changed RECORD; affected_rows BIGINT;
BEGIN
    FOR changed IN SELECT tenant_id, count(*) AS receipts, sum(encoded_bytes) AS bytes FROM inserted_receipts GROUP BY tenant_id LOOP
        UPDATE api_mutation_receipt_tenant_state SET receipt_count = receipt_count + changed.receipts, encoded_bytes = encoded_bytes + changed.bytes WHERE tenant_id = changed.tenant_id;
        GET DIAGNOSTICS affected_rows = ROW_COUNT;
        IF affected_rows <> 1 THEN RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'API mutation receipt accounting failed'; END IF;
    END LOOP;
    RETURN NULL;
END
$$;
CREATE TRIGGER api_mutation_receipt_after_insert AFTER INSERT ON api_mutation_receipts REFERENCING NEW TABLE AS inserted_receipts FOR EACH STATEMENT EXECUTE FUNCTION os_guard_06e7b70c2a336cc00f5c();

CREATE FUNCTION os_guard_a3e682c0893f7df5743e() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    IF NOT ( NEW . tenant_id = OLD . tenant_id AND NEW . actor_kind = OLD . actor_kind AND NEW . actor_id = OLD . actor_id AND NEW . route = OLD . route AND NEW . client_request_id = OLD . client_request_id AND NEW . canonical_version = OLD . canonical_version AND NEW . request_sha256 = OLD . request_sha256 AND NEW . target_kind = OLD . target_kind AND NEW . target_id = OLD . target_id AND NEW . target_version = OLD . target_version AND NEW . audit_sequence IS NOT DISTINCT FROM OLD . audit_sequence AND NEW . created_at_unix_micro = OLD . created_at_unix_micro AND NEW . encoded_bytes = OLD . encoded_bytes AND NEW . retain_until_unix_micro >= OLD . retain_until_unix_micro AND ( NEW . target_terminal_at_unix_micro IS NOT DISTINCT FROM OLD . target_terminal_at_unix_micro OR ( OLD . target_terminal_at_unix_micro IS NULL AND NEW . target_terminal_at_unix_micro IS NOT NULL AND NEW . retain_until_unix_micro >= NEW . target_terminal_at_unix_micro ) ) ) THEN RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'API mutation receipt identity and outcome are immutable'; END IF;
    RETURN NEW;
END
$$;
CREATE TRIGGER api_mutation_receipt_update_is_bounded BEFORE UPDATE ON api_mutation_receipts FOR EACH ROW EXECUTE FUNCTION os_guard_a3e682c0893f7df5743e();

CREATE FUNCTION os_guard_0edbf0b4e102efe60d29() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_rows BIGINT;
BEGIN
    IF OLD . target_terminal_at_unix_micro IS NULL OR NOT EXISTS ( SELECT 1 FROM api_mutation_receipt_tenant_state WHERE tenant_id = OLD . tenant_id AND high_water_unix_micro >= OLD . retain_until_unix_micro ) THEN RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'API mutation receipt retention fence is active'; END IF;
    RETURN OLD;
END
$$;
CREATE TRIGGER api_mutation_receipt_delete_requires_expired_terminal_target BEFORE DELETE ON api_mutation_receipts FOR EACH ROW EXECUTE FUNCTION os_guard_0edbf0b4e102efe60d29();

CREATE FUNCTION os_guard_b1844d438cb37bba7bd5() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE changed RECORD; affected_rows BIGINT;
BEGIN
    FOR changed IN SELECT tenant_id, count(*) AS receipts, sum(encoded_bytes) AS bytes FROM deleted_receipts GROUP BY tenant_id LOOP
        UPDATE api_mutation_receipt_tenant_state SET receipt_count = receipt_count - changed.receipts, encoded_bytes = encoded_bytes - changed.bytes WHERE tenant_id = changed.tenant_id;
        GET DIAGNOSTICS affected_rows = ROW_COUNT;
        IF affected_rows <> 1 THEN RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'API mutation receipt accounting failed'; END IF;
    END LOOP;
    RETURN NULL;
END
$$;
CREATE TRIGGER api_mutation_receipt_after_delete AFTER DELETE ON api_mutation_receipts REFERENCING OLD TABLE AS deleted_receipts FOR EACH STATEMENT EXECUTE FUNCTION os_guard_b1844d438cb37bba7bd5();

CREATE TRIGGER a000_control_write_lock BEFORE INSERT OR UPDATE OR DELETE ON indexes FOR EACH STATEMENT EXECUTE FUNCTION os_control_write_lock();

CREATE TRIGGER a000_control_write_lock BEFORE INSERT OR UPDATE OR DELETE ON ingestion_tokens FOR EACH STATEMENT EXECUTE FUNCTION os_control_write_lock();

CREATE TRIGGER a000_control_write_lock BEFORE INSERT OR UPDATE OR DELETE ON ingestion_token_indexes FOR EACH STATEMENT EXECUTE FUNCTION os_control_write_lock();

CREATE TRIGGER a000_control_write_lock BEFORE INSERT OR UPDATE OR DELETE ON ingestion_token_constraints FOR EACH STATEMENT EXECUTE FUNCTION os_control_write_lock();

CREATE TRIGGER a000_control_write_lock BEFORE INSERT OR UPDATE OR DELETE ON ingest_visibility_state FOR EACH STATEMENT EXECUTE FUNCTION os_control_write_lock();

CREATE TRIGGER a000_control_write_lock BEFORE INSERT OR UPDATE OR DELETE ON ingest_visibility_legacy_tombstones FOR EACH STATEMENT EXECUTE FUNCTION os_control_write_lock();

CREATE TRIGGER a000_control_write_lock BEFORE INSERT OR UPDATE OR DELETE ON ingest_batch_identities FOR EACH STATEMENT EXECUTE FUNCTION os_control_write_lock();

CREATE TRIGGER a000_control_write_lock BEFORE INSERT OR UPDATE OR DELETE ON ingest_visibility_reservations FOR EACH STATEMENT EXECUTE FUNCTION os_control_write_lock();

CREATE TRIGGER a000_control_write_lock BEFORE INSERT OR UPDATE OR DELETE ON saved_searches FOR EACH STATEMENT EXECUTE FUNCTION os_control_write_lock();

CREATE TRIGGER a000_control_write_lock BEFORE INSERT OR UPDATE OR DELETE ON server_key_state FOR EACH STATEMENT EXECUTE FUNCTION os_control_write_lock();

CREATE TRIGGER a000_control_write_lock BEFORE INSERT OR UPDATE OR DELETE ON search_history FOR EACH STATEMENT EXECUTE FUNCTION os_control_write_lock();

CREATE TRIGGER a000_control_write_lock BEFORE INSERT OR UPDATE OR DELETE ON search_history_pending FOR EACH STATEMENT EXECUTE FUNCTION os_control_write_lock();

CREATE TRIGGER a000_control_write_lock BEFORE INSERT OR UPDATE OR DELETE ON app_workspaces FOR EACH STATEMENT EXECUTE FUNCTION os_control_write_lock();

CREATE TRIGGER a000_control_write_lock BEFORE INSERT OR UPDATE OR DELETE ON app_catalog_revisions FOR EACH STATEMENT EXECUTE FUNCTION os_control_write_lock();

CREATE TRIGGER a000_control_write_lock BEFORE INSERT OR UPDATE OR DELETE ON app_default_indexes FOR EACH STATEMENT EXECUTE FUNCTION os_control_write_lock();

CREATE TRIGGER a000_control_write_lock BEFORE INSERT OR UPDATE OR DELETE ON collector_fleet FOR EACH STATEMENT EXECUTE FUNCTION os_control_write_lock();

CREATE TRIGGER a000_control_write_lock BEFORE INSERT OR UPDATE OR DELETE ON collector_catalog_revisions FOR EACH STATEMENT EXECUTE FUNCTION os_control_write_lock();

CREATE TRIGGER a000_control_write_lock BEFORE INSERT OR UPDATE OR DELETE ON collector_runtime FOR EACH STATEMENT EXECUTE FUNCTION os_control_write_lock();

CREATE TRIGGER a000_control_write_lock BEFORE INSERT OR UPDATE OR DELETE ON collector_capabilities FOR EACH STATEMENT EXECUTE FUNCTION os_control_write_lock();

CREATE TRIGGER a000_control_write_lock BEFORE INSERT OR UPDATE OR DELETE ON collector_authorized_indexes FOR EACH STATEMENT EXECUTE FUNCTION os_control_write_lock();

CREATE TRIGGER a000_control_write_lock BEFORE INSERT OR UPDATE OR DELETE ON collector_inputs FOR EACH STATEMENT EXECUTE FUNCTION os_control_write_lock();

CREATE TRIGGER a000_control_write_lock BEFORE INSERT OR UPDATE OR DELETE ON collector_input_health FOR EACH STATEMENT EXECUTE FUNCTION os_control_write_lock();

CREATE TRIGGER a000_control_write_lock BEFORE INSERT OR UPDATE OR DELETE ON search_history_owner_counts FOR EACH STATEMENT EXECUTE FUNCTION os_control_write_lock();

CREATE TRIGGER a000_control_write_lock BEFORE INSERT OR UPDATE OR DELETE ON index_deletion_tombstones FOR EACH STATEMENT EXECUTE FUNCTION os_control_write_lock();

CREATE TRIGGER a000_control_write_lock BEFORE INSERT OR UPDATE OR DELETE ON index_deletion_operations FOR EACH STATEMENT EXECUTE FUNCTION os_control_write_lock();

CREATE TRIGGER a000_control_write_lock BEFORE INSERT OR UPDATE OR DELETE ON index_deletion_mutation_attempts FOR EACH STATEMENT EXECUTE FUNCTION os_control_write_lock();

CREATE TRIGGER a000_control_write_lock BEFORE INSERT OR UPDATE OR DELETE ON index_data_deletion_completions FOR EACH STATEMENT EXECUTE FUNCTION os_control_write_lock();

CREATE TRIGGER a000_control_write_lock BEFORE INSERT OR UPDATE OR DELETE ON index_catalog_state FOR EACH STATEMENT EXECUTE FUNCTION os_control_write_lock();

CREATE TRIGGER a000_control_write_lock BEFORE INSERT OR UPDATE OR DELETE ON ingest_quota_buckets FOR EACH STATEMENT EXECUTE FUNCTION os_control_write_lock();

CREATE TRIGGER a000_control_write_lock BEFORE INSERT OR UPDATE OR DELETE ON ingest_quota_admissions FOR EACH STATEMENT EXECUTE FUNCTION os_control_write_lock();

CREATE TRIGGER a000_control_write_lock BEFORE INSERT OR UPDATE OR DELETE ON audit_tenant_state FOR EACH STATEMENT EXECUTE FUNCTION os_control_write_lock();

CREATE TRIGGER a000_control_write_lock BEFORE INSERT OR UPDATE OR DELETE ON search_attempt_audit_tenant_state FOR EACH STATEMENT EXECUTE FUNCTION os_control_write_lock();

CREATE TRIGGER a000_control_write_lock BEFORE INSERT OR UPDATE OR DELETE ON search_attempt_audit_events FOR EACH STATEMENT EXECUTE FUNCTION os_control_write_lock();

CREATE TRIGGER a000_control_write_lock BEFORE INSERT OR UPDATE OR DELETE ON knowledge_catalog_tenants FOR EACH STATEMENT EXECUTE FUNCTION os_control_write_lock();

CREATE TRIGGER a000_control_write_lock BEFORE INSERT OR UPDATE OR DELETE ON knowledge_definition_blobs FOR EACH STATEMENT EXECUTE FUNCTION os_control_write_lock();

CREATE TRIGGER a000_control_write_lock BEFORE INSERT OR UPDATE OR DELETE ON knowledge_objects FOR EACH STATEMENT EXECUTE FUNCTION os_control_write_lock();

CREATE TRIGGER a000_control_write_lock BEFORE INSERT OR UPDATE OR DELETE ON knowledge_object_versions FOR EACH STATEMENT EXECUTE FUNCTION os_control_write_lock();

CREATE TRIGGER a000_control_write_lock BEFORE INSERT OR UPDATE OR DELETE ON knowledge_object_dependencies FOR EACH STATEMENT EXECUTE FUNCTION os_control_write_lock();

CREATE TRIGGER a000_control_write_lock BEFORE INSERT OR UPDATE OR DELETE ON knowledge_object_dependency_seals FOR EACH STATEMENT EXECUTE FUNCTION os_control_write_lock();

CREATE TRIGGER a000_control_write_lock BEFORE INSERT OR UPDATE OR DELETE ON knowledge_object_acl FOR EACH STATEMENT EXECUTE FUNCTION os_control_write_lock();

CREATE TRIGGER a000_control_write_lock BEFORE INSERT OR UPDATE OR DELETE ON knowledge_app_active_counters FOR EACH STATEMENT EXECUTE FUNCTION os_control_write_lock();

CREATE TRIGGER a000_control_write_lock BEFORE INSERT OR UPDATE OR DELETE ON knowledge_owner_active_counters FOR EACH STATEMENT EXECUTE FUNCTION os_control_write_lock();

CREATE TRIGGER a000_control_write_lock BEFORE INSERT OR UPDATE OR DELETE ON knowledge_type_active_counters FOR EACH STATEMENT EXECUTE FUNCTION os_control_write_lock();

CREATE TRIGGER a000_control_write_lock BEFORE INSERT OR UPDATE OR DELETE ON knowledge_app_type_active_counters FOR EACH STATEMENT EXECUTE FUNCTION os_control_write_lock();

CREATE TRIGGER a000_control_write_lock BEFORE INSERT OR UPDATE OR DELETE ON knowledge_recovery_audit FOR EACH STATEMENT EXECUTE FUNCTION os_control_write_lock();

CREATE TRIGGER a000_control_write_lock BEFORE INSERT OR UPDATE OR DELETE ON knowledge_projection_tenant_ledgers FOR EACH STATEMENT EXECUTE FUNCTION os_control_write_lock();

CREATE TRIGGER a000_control_write_lock BEFORE INSERT OR UPDATE OR DELETE ON knowledge_object_list_projections FOR EACH STATEMENT EXECUTE FUNCTION os_control_write_lock();

CREATE TRIGGER a000_control_write_lock BEFORE INSERT OR UPDATE OR DELETE ON knowledge_object_list_selector_patterns FOR EACH STATEMENT EXECUTE FUNCTION os_control_write_lock();

CREATE TRIGGER a000_control_write_lock BEFORE INSERT OR UPDATE OR DELETE ON knowledge_object_list_projection_seals FOR EACH STATEMENT EXECUTE FUNCTION os_control_write_lock();

CREATE TRIGGER a000_control_write_lock BEFORE INSERT OR UPDATE OR DELETE ON knowledge_attempt_audit_tenant_state FOR EACH STATEMENT EXECUTE FUNCTION os_control_write_lock();

CREATE TRIGGER a000_control_write_lock BEFORE INSERT OR UPDATE OR DELETE ON knowledge_attempt_audit_events FOR EACH STATEMENT EXECUTE FUNCTION os_control_write_lock();

CREATE TRIGGER a000_control_write_lock BEFORE INSERT OR UPDATE OR DELETE ON knowledge_catalog_revision_heads FOR EACH STATEMENT EXECUTE FUNCTION os_control_write_lock();

CREATE TRIGGER a000_control_write_lock BEFORE INSERT OR UPDATE OR DELETE ON knowledge_object_version_lifecycle FOR EACH STATEMENT EXECUTE FUNCTION os_control_write_lock();

CREATE TRIGGER a000_control_write_lock BEFORE INSERT OR UPDATE OR DELETE ON knowledge_object_list_order_keys FOR EACH STATEMENT EXECUTE FUNCTION os_control_write_lock();

CREATE TRIGGER a000_control_write_lock BEFORE INSERT OR UPDATE OR DELETE ON knowledge_mutation_commit_authorities FOR EACH STATEMENT EXECUTE FUNCTION os_control_write_lock();

CREATE TRIGGER a000_control_write_lock BEFORE INSERT OR UPDATE OR DELETE ON knowledge_mutation_idempotency FOR EACH STATEMENT EXECUTE FUNCTION os_control_write_lock();

CREATE TRIGGER a000_control_write_lock BEFORE INSERT OR UPDATE OR DELETE ON ingestion_token_hec_profiles FOR EACH STATEMENT EXECUTE FUNCTION os_control_write_lock();

CREATE TRIGGER a000_control_write_lock BEFORE INSERT OR UPDATE OR DELETE ON hec_source_sequences FOR EACH STATEMENT EXECUTE FUNCTION os_control_write_lock();

CREATE TRIGGER a000_control_write_lock BEFORE INSERT OR UPDATE OR DELETE ON hec_requests FOR EACH STATEMENT EXECUTE FUNCTION os_control_write_lock();

CREATE TRIGGER a000_control_write_lock BEFORE INSERT OR UPDATE OR DELETE ON hec_channels FOR EACH STATEMENT EXECUTE FUNCTION os_control_write_lock();

CREATE TRIGGER a000_control_write_lock BEFORE INSERT OR UPDATE OR DELETE ON hec_acknowledgments FOR EACH STATEMENT EXECUTE FUNCTION os_control_write_lock();

CREATE TRIGGER a000_control_write_lock BEFORE INSERT OR UPDATE OR DELETE ON knowledge_lookup_asset_tenant_ledgers FOR EACH STATEMENT EXECUTE FUNCTION os_control_write_lock();

CREATE TRIGGER a000_control_write_lock BEFORE INSERT OR UPDATE OR DELETE ON knowledge_lookup_asset_stages FOR EACH STATEMENT EXECUTE FUNCTION os_control_write_lock();

CREATE TRIGGER a000_control_write_lock BEFORE INSERT OR UPDATE OR DELETE ON knowledge_lookup_assets FOR EACH STATEMENT EXECUTE FUNCTION os_control_write_lock();

CREATE TRIGGER a000_control_write_lock BEFORE INSERT OR UPDATE OR DELETE ON knowledge_lookup_asset_versions FOR EACH STATEMENT EXECUTE FUNCTION os_control_write_lock();

CREATE TRIGGER a000_control_write_lock BEFORE INSERT OR UPDATE OR DELETE ON knowledge_lookup_definitions FOR EACH STATEMENT EXECUTE FUNCTION os_control_write_lock();

CREATE TRIGGER a000_control_write_lock BEFORE INSERT OR UPDATE OR DELETE ON knowledge_lookup_definition_versions FOR EACH STATEMENT EXECUTE FUNCTION os_control_write_lock();

CREATE TRIGGER a000_control_write_lock BEFORE INSERT OR UPDATE OR DELETE ON dashboards FOR EACH STATEMENT EXECUTE FUNCTION os_control_write_lock();

CREATE TRIGGER a000_control_write_lock BEFORE INSERT OR UPDATE OR DELETE ON server_search_settings FOR EACH STATEMENT EXECUTE FUNCTION os_control_write_lock();

CREATE TRIGGER a000_control_write_lock BEFORE INSERT OR UPDATE OR DELETE ON durable_search_jobs FOR EACH STATEMENT EXECUTE FUNCTION os_control_write_lock();

CREATE TRIGGER a000_control_write_lock BEFORE INSERT OR UPDATE OR DELETE ON saved_search_schedules FOR EACH STATEMENT EXECUTE FUNCTION os_control_write_lock();

CREATE TRIGGER a000_control_write_lock BEFORE INSERT OR UPDATE OR DELETE ON saved_search_schedule_runs FOR EACH STATEMENT EXECUTE FUNCTION os_control_write_lock();

CREATE TRIGGER a000_control_write_lock BEFORE INSERT OR UPDATE OR DELETE ON alerts FOR EACH STATEMENT EXECUTE FUNCTION os_control_write_lock();

CREATE TRIGGER a000_control_write_lock BEFORE INSERT OR UPDATE OR DELETE ON alert_runs FOR EACH STATEMENT EXECUTE FUNCTION os_control_write_lock();

CREATE TRIGGER a000_control_write_lock BEFORE INSERT OR UPDATE OR DELETE ON feature_operation_audit_tenant_state FOR EACH STATEMENT EXECUTE FUNCTION os_control_write_lock();

CREATE TRIGGER a000_control_write_lock BEFORE INSERT OR UPDATE OR DELETE ON feature_operation_audit_events FOR EACH STATEMENT EXECUTE FUNCTION os_control_write_lock();

CREATE TRIGGER a000_control_write_lock BEFORE INSERT OR UPDATE OR DELETE ON ingest_write_groups FOR EACH STATEMENT EXECUTE FUNCTION os_control_write_lock();

CREATE TRIGGER a000_control_write_lock BEFORE INSERT OR UPDATE OR DELETE ON ingest_write_group_members FOR EACH STATEMENT EXECUTE FUNCTION os_control_write_lock();

CREATE TRIGGER a000_control_write_lock BEFORE INSERT OR UPDATE OR DELETE ON server_appearance_settings FOR EACH STATEMENT EXECUTE FUNCTION os_control_write_lock();

CREATE TRIGGER a000_control_write_lock BEFORE INSERT OR UPDATE OR DELETE ON ingest_rejection_buckets FOR EACH STATEMENT EXECUTE FUNCTION os_control_write_lock();

CREATE TRIGGER a000_control_write_lock BEFORE INSERT OR UPDATE OR DELETE ON audit_events FOR EACH STATEMENT EXECUTE FUNCTION os_control_write_lock();

CREATE TRIGGER a000_control_write_lock BEFORE INSERT OR UPDATE OR DELETE ON api_mutation_receipt_tenant_state FOR EACH STATEMENT EXECUTE FUNCTION os_control_write_lock();

CREATE TRIGGER a000_control_write_lock BEFORE INSERT OR UPDATE OR DELETE ON api_mutation_receipts FOR EACH STATEMENT EXECUTE FUNCTION os_control_write_lock();

CREATE TRIGGER a000_control_write_lock BEFORE INSERT OR UPDATE OR DELETE ON durable_export_jobs FOR EACH STATEMENT EXECUTE FUNCTION os_control_write_lock();

INSERT INTO ingest_visibility_state (singleton, last_assigned, committed_through) VALUES (1, 0, 0);

INSERT INTO index_catalog_state (singleton_id, revision, physical_count) VALUES (1, 1, 0);

CREATE FUNCTION os_app_organization_immutable() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
 IF NEW.organization_id <> OLD.organization_id THEN RAISE EXCEPTION USING ERRCODE='23514',MESSAGE='app organization is immutable'; END IF;
 RETURN NEW; END $$;
CREATE TRIGGER app_organization_is_immutable BEFORE UPDATE OF organization_id ON app_workspaces FOR EACH ROW EXECUTE FUNCTION os_app_organization_immutable();

CREATE FUNCTION os_knowledge_objects_organization() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
 IF TG_OP='UPDATE' AND NEW.organization_id<>OLD.organization_id THEN RAISE EXCEPTION USING ERRCODE='23514',MESSAGE='resource organization is immutable'; END IF;
 IF NOT EXISTS(SELECT 1 FROM app_workspaces app WHERE app.app_id=NEW.app_id AND app.tenant_id=NEW.tenant_id AND app.organization_id=NEW.organization_id) THEN RAISE EXCEPTION USING ERRCODE='23514',MESSAGE='resource must belong to its app organization'; END IF;
 RETURN NEW; END $$;
CREATE TRIGGER knowledge_objects_organization_is_valid BEFORE INSERT OR UPDATE ON knowledge_objects FOR EACH ROW EXECUTE FUNCTION os_knowledge_objects_organization();

CREATE FUNCTION os_knowledge_lookup_definitions_organization() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
 IF TG_OP='UPDATE' AND NEW.organization_id<>OLD.organization_id THEN RAISE EXCEPTION USING ERRCODE='23514',MESSAGE='resource organization is immutable'; END IF;
 IF NOT EXISTS(SELECT 1 FROM app_workspaces app WHERE app.app_id=NEW.app_id AND app.tenant_id=NEW.tenant_id AND app.organization_id=NEW.organization_id) THEN RAISE EXCEPTION USING ERRCODE='23514',MESSAGE='resource must belong to its app organization'; END IF;
 RETURN NEW; END $$;
CREATE TRIGGER knowledge_lookup_definitions_organization_is_valid BEFORE INSERT OR UPDATE ON knowledge_lookup_definitions FOR EACH ROW EXECUTE FUNCTION os_knowledge_lookup_definitions_organization();

-- Recovery publication is empty during ordinary operation and records the
-- immutable generation binding when a stopped-server restore is published.
CREATE TABLE control_recovery_publication (
 singleton SMALLINT PRIMARY KEY CHECK (singleton = 1),
 recovery_set_id TEXT NOT NULL CHECK (recovery_set_id ~ '^[0-9a-f]{32}$'),
 manifest_sha256 TEXT NOT NULL CHECK (manifest_sha256 ~ '^[0-9a-f]{64}$'),
 phase TEXT NOT NULL CHECK (phase IN ('database','objects','complete')),
 created_at_unix_micro BIGINT NOT NULL CHECK (created_at_unix_micro > 0)
);
CREATE FUNCTION os_recovery_publication_guard() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF TG_OP = 'DELETE' OR (TG_OP = 'UPDATE' AND (
   NEW.singleton IS DISTINCT FROM OLD.singleton OR
   NEW.recovery_set_id IS DISTINCT FROM OLD.recovery_set_id OR
   NEW.manifest_sha256 IS DISTINCT FROM OLD.manifest_sha256 OR
   NEW.created_at_unix_micro IS DISTINCT FROM OLD.created_at_unix_micro OR
   (OLD.phase='complete' AND NEW.phase<>'complete') OR
   (OLD.phase='objects' AND NEW.phase='database'))) THEN
   RAISE EXCEPTION USING ERRCODE='23514', MESSAGE='recovery publication identity and phase are immutable';
 END IF;
 RETURN NEW;
END;
$$;
CREATE TRIGGER control_recovery_publication_guard BEFORE UPDATE OR DELETE ON control_recovery_publication FOR EACH ROW EXECUTE FUNCTION os_recovery_publication_guard();
CREATE TRIGGER os_write_guard_recovery_publication BEFORE INSERT OR UPDATE OR DELETE ON control_recovery_publication FOR EACH STATEMENT EXECUTE FUNCTION os_control_write_lock();

CREATE FUNCTION os_retained_job_organization_immutable() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.organization_id IS DISTINCT FROM OLD.organization_id THEN RAISE EXCEPTION 'retained job organization is immutable' USING ERRCODE='23514'; END IF; RETURN NEW; END; $$;
CREATE TRIGGER durable_search_job_organization_immutable BEFORE UPDATE ON durable_search_jobs FOR EACH ROW EXECUTE FUNCTION os_retained_job_organization_immutable();
CREATE TRIGGER durable_export_job_organization_immutable BEFORE UPDATE ON durable_export_jobs FOR EACH ROW EXECUTE FUNCTION os_retained_job_organization_immutable();
CREATE INDEX durable_search_jobs_organization_owner ON durable_search_jobs(tenant_id,organization_id,owner_id,created_at_us,id);
CREATE INDEX durable_export_jobs_organization_owner ON durable_export_jobs(tenant_id,organization_id,owner_id,created_at_us,export_id);

CREATE FUNCTION os_retained_search_authority_immutable() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE previous JSONB; current JSONB;
BEGIN
 previous:=convert_from(OLD.job_payload,'UTF8')::jsonb->'job';
 current:=convert_from(NEW.job_payload,'UTF8')::jsonb->'job';
 IF NEW.id IS DISTINCT FROM OLD.id OR NEW.tenant_id IS DISTINCT FROM OLD.tenant_id OR NEW.owner_id IS DISTINCT FROM OLD.owner_id OR NEW.created_at_us IS DISTINCT FROM OLD.created_at_us
 OR COALESCE(NULLIF(current->'SitePrivilegeUsed','null'::jsonb),'false'::jsonb) IS DISTINCT FROM COALESCE(NULLIF(previous->'SitePrivilegeUsed','null'::jsonb),'false'::jsonb)
 OR COALESCE(NULLIF(current->'KnowledgeObjectIDs','null'::jsonb),'[]'::jsonb) IS DISTINCT FROM COALESCE(NULLIF(previous->'KnowledgeObjectIDs','null'::jsonb),'[]'::jsonb) THEN
  RAISE EXCEPTION 'retained search identity and source authority are immutable' USING ERRCODE='23514';
 END IF;
 RETURN NEW;
END;
$$;
CREATE TRIGGER durable_search_job_source_authority_immutable BEFORE UPDATE ON durable_search_jobs FOR EACH ROW EXECUTE FUNCTION os_retained_search_authority_immutable();
