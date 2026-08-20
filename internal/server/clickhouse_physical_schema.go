package server

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"strings"

	"github.com/Suhaibinator/open-splunk/internal/recoverycontract"
)

var (
	// ErrClickHousePhysicalSchemaDrift means a managed ClickHouse table is
	// missing or its complete physical definition differs from the schema
	// produced by the embedded migrations.
	ErrClickHousePhysicalSchemaDrift = errors.New(
		"server: ClickHouse physical schema differs from embedded migrations",
	)
)

const (
	clickHouseManagedTableCount         = 4
	clickHouseManagedTableSentinel      = clickHouseManagedTableCount + 1
	clickHousePhysicalSchemaReadLimit   = 8 << 20
	clickHousePhysicalSchemaMemoryLimit = 64 << 20
	clickHousePhysicalSchemaResultLimit = 8 << 20
	// The four definitions below are byte-exact copies of what
	// system.tables.create_table_query renders on the pinned server version
	// (clickHousePrivilegeContractVersion). They are therefore version-coupled:
	// when the pin moves, re-read the live definitions and update these
	// constants rather than relaxing the comparison. ClickHouse 26.7 stopped
	// unwrapping single-column sort keys, so the three tables sorted on one
	// column render `ORDER BY (version)` / `ORDER BY (slot)` where 26.3
	// rendered `ORDER BY version` / `ORDER BY slot`; the events table's
	// multi-column key was unaffected.
	clickHouseMigrationLedgerPhysicalSchemaDefinition = "CREATE TABLE open_splunk.schema_migrations (" +
		"`version` UInt32, " +
		"`name` LowCardinality(String), " +
		"`applied_at` DateTime64(3, 'UTC') CODEC(Delta(8), ZSTD(1))" +
		") ENGINE = MergeTree ORDER BY (version) " +
		"SETTINGS index_granularity = 8192"
	clickHouseEventsPhysicalSchemaDefinition = "CREATE TABLE open_splunk.events (" +
		"`event_id` String CODEC(ZSTD(1)), " +
		"`tenant_id` LowCardinality(String) CODEC(ZSTD(1)), " +
		"`index_name` LowCardinality(String) CODEC(ZSTD(1)), " +
		"`event_time` DateTime64(9, 'UTC') CODEC(Delta(8), ZSTD(1)), " +
		"`index_time` DateTime64(3, 'UTC') CODEC(Delta(8), ZSTD(1)), " +
		"`collected_at` Nullable(DateTime64(9, 'UTC')) CODEC(ZSTD(1)), " +
		"`event_time_source` UInt8 CODEC(T64, ZSTD(1)), " +
		"`host` String CODEC(ZSTD(1)), " +
		"`source` String CODEC(ZSTD(1)), " +
		"`sourcetype` LowCardinality(String) CODEC(ZSTD(1)), " +
		"`service` LowCardinality(Nullable(String)) CODEC(ZSTD(1)), " +
		"`severity` UInt8 CODEC(T64, ZSTD(1)), " +
		"`level` LowCardinality(Nullable(String)) CODEC(ZSTD(1)), " +
		"`body` Nullable(String) CODEC(ZSTD(1)), " +
		"`raw` String CODEC(ZSTD(1)), " +
		"`raw_encoding` UInt8 CODEC(T64, ZSTD(1)), " +
		"`trace_id` Nullable(String) CODEC(ZSTD(1)), " +
		"`span_id` Nullable(String) CODEC(ZSTD(1)), " +
		"`fields` JSON(max_dynamic_types = 16, max_dynamic_paths = 256), " +
		"`field_names` Array(String) CODEC(ZSTD(1)), " +
		"`field_types` Array(UInt8) CODEC(ZSTD(1)), " +
		"`field_metadata_version` UInt8 CODEC(T64, ZSTD(1)), " +
		"`collector_id` String CODEC(ZSTD(1)), " +
		"`ingest_source_kind` UInt8 CODEC(T64, ZSTD(1)), " +
		"`ingest_source_id` String CODEC(ZSTD(1)), " +
		"`batch_id` String CODEC(ZSTD(1)), " +
		"`batch_sequence` UInt64 CODEC(Delta(8), ZSTD(1)), " +
		"`visibility_seq` UInt64 CODEC(Delta(8), ZSTD(1)), " +
		"`expires_at` DateTime64(3, 'UTC') CODEC(Delta(8), ZSTD(1)), " +
		"INDEX idx_event_id event_id TYPE bloom_filter(0.001) GRANULARITY 1, " +
		"INDEX idx_trace_id ifNull(trace_id, '') TYPE bloom_filter(0.001) GRANULARITY 1, " +
		"INDEX idx_span_id ifNull(span_id, '') TYPE bloom_filter(0.001) GRANULARITY 1, " +
		"INDEX idx_field_names field_names TYPE text(tokenizer = 'array') GRANULARITY 100000000, " +
		"INDEX idx_raw_text arrayMap(token -> lower(token), extractAll(translateUTF8(raw, 'ſK', 'sk'), '[A-Za-z0-9_]+')) TYPE text(tokenizer = 'array') GRANULARITY 100000000, " +
		"INDEX idx_visibility_seq visibility_seq TYPE minmax GRANULARITY 1, " +
		"CONSTRAINT visibility_seq_is_positive CHECK visibility_seq > 0, " +
		"CONSTRAINT field_metadata_version_is_supported CHECK field_metadata_version = 1, " +
		"CONSTRAINT field_metadata_is_aligned CHECK " +
		"(length(field_names) = length(field_types)) AND " +
		"arrayAll(code -> ((code >= 1) AND (code <= 12)), field_types), " +
		"CONSTRAINT ingest_source_kind_is_supported CHECK ingest_source_kind IN (1, 2), " +
		"CONSTRAINT ingest_source_shape_is_valid CHECK " +
		"((ingest_source_kind = 1) AND notEmpty(collector_id) AND (ingest_source_id = collector_id)) OR " +
		"((ingest_source_kind = 2) AND empty(collector_id) AND notEmpty(ingest_source_id)))" +
		" ENGINE = MergeTree PARTITION BY toYYYYMM(event_time) " +
		"PRIMARY KEY (tenant_id, index_name, toStartOfHour(event_time), event_time) " +
		"ORDER BY (tenant_id, index_name, toStartOfHour(event_time), event_time, event_id) " +
		"TTL expires_at SETTINGS index_granularity = 8192, " +
		"non_replicated_deduplication_window = 10000, " +
		"write_marks_for_substreams_in_compact_parts = 1"
	clickHouseRecoverySetsPhysicalSchemaDefinition = "CREATE TABLE open_splunk.recovery_sets (" +
		"`slot` UInt8, " +
		"`recovery_set_id` FixedString(32), " +
		"`deployment_manifest_sha256` FixedString(64), " +
		"`database_uuid` UUID, " +
		"`schema_migrations_table_uuid` UUID, " +
		"`events_table_uuid` UUID, " +
		"`recovery_sets_table_uuid` UUID, " +
		"`recovery_archive_markers_table_uuid` UUID, " +
		"`restored_at` DateTime64(3, 'UTC'), " +
		"CONSTRAINT slot_is_singleton CHECK slot = 1" +
		") ENGINE = MergeTree ORDER BY (slot) " +
		"SETTINGS index_granularity = 8192"
	clickHouseRecoveryArchiveMarkersPhysicalSchemaDefinition = "CREATE TABLE open_splunk.recovery_archive_markers (" +
		"`slot` UInt8, " +
		"`recovery_set_id` FixedString(32), " +
		"`backup_operation_uuid` UUID, " +
		"CONSTRAINT slot_is_singleton CHECK slot = 1" +
		") ENGINE = MergeTree ORDER BY (slot) " +
		"SETTINGS index_granularity = 8192"
)

// system.tables.create_table_query is ClickHouse's canonical serialization of
// the complete table definition. On the version-pinned server it covers
// columns, types, defaults, codecs, indexes, constraints, engine,
// partition/sort/primary keys, TTL, and table settings. The read and result
// settings cap adversarial metadata before validating the exact managed
// four-table contract.
var clickHousePhysicalSchemaQuery = fmt.Sprintf(`
		SELECT
			count(),
			countIf(name = 'schema_migrations'),
			anyIf(create_table_query, name = 'schema_migrations'),
			countIf(name = 'events'),
			anyIf(create_table_query, name = 'events'),
			countIf(name = 'recovery_sets'),
			anyIf(create_table_query, name = 'recovery_sets'),
			countIf(name = 'recovery_archive_markers'),
			anyIf(create_table_query, name = 'recovery_archive_markers'),
			toString(anyIf(uuid, name = 'schema_migrations')),
			toString(anyIf(uuid, name = 'events')),
			toString(anyIf(uuid, name = 'recovery_sets')),
			toString(anyIf(uuid, name = 'recovery_archive_markers'))
		FROM
		(
			SELECT name, create_table_query, uuid
			FROM system.tables
			WHERE database = ?
			LIMIT %d
		)
		SETTINGS
			max_rows_to_read = %d,
			max_bytes_to_read = %d,
			read_overflow_mode = 'throw',
			max_memory_usage = %d,
			max_result_rows = 1,
			max_result_bytes = %d,
			result_overflow_mode = 'throw'`,
	clickHouseManagedTableSentinel,
	clickHouseManagedTableSentinel,
	clickHousePhysicalSchemaReadLimit,
	clickHousePhysicalSchemaMemoryLimit,
	clickHousePhysicalSchemaResultLimit,
)

// ValidateClickHousePhysicalSchema verifies the exact definitions of all
// tables generated by the embedded migrations. It deliberately
// reads only system.tables so the one-shot migrator does not need broader
// metadata grants.
func ValidateClickHousePhysicalSchema(
	ctx context.Context,
	connection ClickHouseVersionConnection,
) error {
	return ValidateClickHousePhysicalSchemaForDatabase(
		ctx,
		connection,
		recoverycontract.CanonicalDatabase,
	)
}

// ValidateClickHousePhysicalSchemaForDatabase verifies the exact managed
// table set in a database admitted by the recovery contract's closed grammar.
// The deployment restore coordinator separately narrows fresh restoration to
// the absent canonical database; the metadata predicate itself remains
// parameterized.
func ValidateClickHousePhysicalSchemaForDatabase(
	ctx context.Context,
	connection ClickHouseVersionConnection,
	databaseName string,
) error {
	return validateClickHousePhysicalSchemaForDatabase(
		ctx,
		connection,
		databaseName,
		nil,
	)
}

type clickHousePhysicalSchemaInspection struct {
	SchemaMigrationsTableUUID       string
	EventsTableUUID                 string
	RecoverySetsTableUUID           string
	RecoveryArchiveMarkersTableUUID string
}

func inspectClickHousePhysicalSchemaForDatabase(
	ctx context.Context,
	connection ClickHouseVersionConnection,
	databaseName string,
) (clickHousePhysicalSchemaInspection, error) {
	var inspection clickHousePhysicalSchemaInspection
	if err := validateClickHousePhysicalSchemaForDatabase(
		ctx,
		connection,
		databaseName,
		&inspection,
	); err != nil {
		return clickHousePhysicalSchemaInspection{}, err
	}
	return inspection, nil
}

func validateClickHousePhysicalSchemaForDatabase(
	ctx context.Context,
	connection ClickHouseVersionConnection,
	databaseName string,
	inspection *clickHousePhysicalSchemaInspection,
) error {
	if ctx == nil || connection == nil {
		return errors.New(
			"validate ClickHouse physical schema: context and connection are required",
		)
	}
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("validate ClickHouse physical schema: %w", err)
	}
	if err := validateClickHouseRecoveryDatabaseName(databaseName); err != nil {
		return fmt.Errorf("validate ClickHouse physical schema: %w", err)
	}

	var tableCount uint64
	var migrationLedgerCount uint64
	var migrationLedgerDefinition string
	var eventsCount uint64
	var eventsDefinition string
	var recoverySetsCount uint64
	var recoverySetsDefinition string
	var recoveryArchiveMarkersCount uint64
	var recoveryArchiveMarkersDefinition string
	var schemaMigrationsUUID string
	var eventsUUID string
	var recoverySetsUUID string
	var recoveryArchiveMarkersUUID string
	if err := connection.QueryRow(
		ctx,
		clickHousePhysicalSchemaQuery,
		databaseName,
	).Scan(
		&tableCount,
		&migrationLedgerCount,
		&migrationLedgerDefinition,
		&eventsCount,
		&eventsDefinition,
		&recoverySetsCount,
		&recoverySetsDefinition,
		&recoveryArchiveMarkersCount,
		&recoveryArchiveMarkersDefinition,
		&schemaMigrationsUUID,
		&eventsUUID,
		&recoverySetsUUID,
		&recoveryArchiveMarkersUUID,
	); err != nil {
		return fmt.Errorf("read ClickHouse physical schema: %w", err)
	}
	if tableCount != clickHouseManagedTableCount || migrationLedgerCount != 1 || eventsCount != 1 ||
		recoverySetsCount != 1 || recoveryArchiveMarkersCount != 1 {
		return fmt.Errorf(
			"%w: %s table set has total=%d schema_migrations=%d events=%d recovery_sets=%d recovery_archive_markers=%d, want total=%d schema_migrations=1 events=1 recovery_sets=1 recovery_archive_markers=1",
			ErrClickHousePhysicalSchemaDrift,
			databaseName,
			tableCount,
			migrationLedgerCount,
			eventsCount,
			recoverySetsCount,
			recoveryArchiveMarkersCount,
			clickHouseManagedTableCount,
		)
	}
	if err := validateClickHousePhysicalTableDefinition(
		databaseName,
		"schema_migrations",
		migrationLedgerDefinition,
		clickHousePhysicalSchemaDefinitionForDatabase(
			clickHouseMigrationLedgerPhysicalSchemaDefinition,
			databaseName,
		),
	); err != nil {
		return err
	}
	if err := validateClickHousePhysicalTableDefinition(
		databaseName,
		"events",
		eventsDefinition,
		clickHousePhysicalSchemaDefinitionForDatabase(
			clickHouseEventsPhysicalSchemaDefinition,
			databaseName,
		),
	); err != nil {
		return err
	}
	if err := validateClickHousePhysicalTableDefinition(
		databaseName,
		"recovery_sets",
		recoverySetsDefinition,
		clickHousePhysicalSchemaDefinitionForDatabase(
			clickHouseRecoverySetsPhysicalSchemaDefinition,
			databaseName,
		),
	); err != nil {
		return err
	}
	if err := validateClickHousePhysicalTableDefinition(
		databaseName,
		"recovery_archive_markers",
		recoveryArchiveMarkersDefinition,
		clickHousePhysicalSchemaDefinitionForDatabase(
			clickHouseRecoveryArchiveMarkersPhysicalSchemaDefinition,
			databaseName,
		),
	); err != nil {
		return err
	}
	if err := validateClickHouseRecoveryTableUUIDIdentity(
		schemaMigrationsUUID,
		eventsUUID,
		recoverySetsUUID,
		recoveryArchiveMarkersUUID,
	); err != nil {
		return fmt.Errorf("validate ClickHouse physical-schema table UUIDs: %w", err)
	}
	if inspection != nil {
		*inspection = clickHousePhysicalSchemaInspection{
			SchemaMigrationsTableUUID:       schemaMigrationsUUID,
			EventsTableUUID:                 eventsUUID,
			RecoverySetsTableUUID:           recoverySetsUUID,
			RecoveryArchiveMarkersTableUUID: recoveryArchiveMarkersUUID,
		}
	}
	return nil
}

func validateClickHousePhysicalTableDefinition(
	databaseName string,
	tableName string,
	actual string,
	expected string,
) error {
	if actual == expected {
		return nil
	}

	digest := fmt.Sprintf("%x", sha256.Sum256([]byte(actual)))
	wantDigest := fmt.Sprintf("%x", sha256.Sum256([]byte(expected)))
	return fmt.Errorf(
		"%w: %s.%s definition SHA-256 is %s, want %s",
		ErrClickHousePhysicalSchemaDrift,
		databaseName,
		tableName,
		digest,
		wantDigest,
	)
}

func clickHousePhysicalSchemaDefinitionForDatabase(
	definition string,
	databaseName string,
) string {
	if databaseName == recoverycontract.CanonicalDatabase {
		return definition
	}
	return strings.Replace(
		definition,
		"CREATE TABLE open_splunk.",
		"CREATE TABLE "+databaseName+".",
		1,
	)
}
