package auth

import "database/sql"

// collectorTokenRecord is the explicit GORM representation of
// ingestion_tokens.
//
// Versioned SQL migrations remain the sole schema authority. In particular,
// SQLite STRICT mode and the digest-immutability, irreversible-revocation, and
// collector-binding insert/update triggers cannot be represented completely
// by GORM tags. Never use AutoMigrate for this model.
type collectorTokenRecord struct {
	IngestionTokenID                    string              `gorm:"column:ingestion_token_id;type:text;primaryKey;not null;index:ingestion_tokens_revoked_retention_idx,priority:2,sort:desc"`
	Version                             int64               `gorm:"column:version;type:integer;not null;check:ingestion_tokens_version_positive,version >= 1"`
	Name                                string              `gorm:"column:name;type:text;not null;check:ingestion_tokens_name_length,length(name) BETWEEN 1 AND 255"`
	Description                         string              `gorm:"column:description;type:text;not null"`
	TokenPrefix                         string              `gorm:"column:token_prefix;type:text;not null;check:ingestion_tokens_prefix_length,length(token_prefix) BETWEEN 8 AND 32"`
	TokenDigest                         []byte              `gorm:"column:token_digest;type:blob;not null;unique;check:ingestion_tokens_digest_length,length(token_digest) = 32"`
	State                               CollectorTokenState `gorm:"column:state;type:text;not null;check:ingestion_tokens_state,state IN ('active', 'disabled', 'revoked')"`
	CreatedAtUnixMicro                  int64               `gorm:"column:created_at_unix_micro;type:integer;not null"`
	UpdatedAtUnixMicro                  int64               `gorm:"column:updated_at_unix_micro;type:integer;not null;check:ingestion_tokens_update_not_before_create,updated_at_unix_micro >= created_at_unix_micro"`
	ExpiresAtUnixMicro                  *int64              `gorm:"column:expires_at_unix_micro;type:integer;check:ingestion_tokens_expiration_after_create,expires_at_unix_micro IS NULL OR expires_at_unix_micro > created_at_unix_micro"`
	RevokedAtUnixMicro                  *int64              `gorm:"column:revoked_at_unix_micro;type:integer;check:ingestion_tokens_revocation_consistency,(state = 'revoked' AND revoked_at_unix_micro IS NOT NULL) OR (state IN ('active', 'disabled') AND revoked_at_unix_micro IS NULL);index:ingestion_tokens_revoked_retention_idx,priority:1,sort:desc,where:state = 'revoked'"`
	LastUsedAtUnixMicro                 *int64              `gorm:"column:last_used_at_unix_micro;type:integer;check:ingestion_tokens_last_use_not_before_create,last_used_at_unix_micro IS NULL OR last_used_at_unix_micro >= created_at_unix_micro"`
	BoundCollectorID                    *string             `gorm:"column:bound_collector_id;type:text;check:ingestion_tokens_bound_collector_id_canonical,bound_collector_id IS NULL OR (length(bound_collector_id) BETWEEN 1 AND 128 AND instr(bound_collector_id, char(0)) = 0 AND substr(bound_collector_id, 1, 1) GLOB '[A-Za-z0-9]' AND bound_collector_id NOT GLOB '*[^A-Za-z0-9._:-]*')"`
	MaxIngestEventsPerSecond            int64               `gorm:"column:max_ingest_events_per_second;type:integer;not null;check:ingestion_tokens_max_ingest_events_per_second_bounded,max_ingest_events_per_second BETWEEN 0 AND 1000000"`
	MaxIngestUncompressedBytesPerSecond int64               `gorm:"column:max_ingest_uncompressed_bytes_per_second;type:integer;not null;check:ingestion_tokens_max_ingest_uncompressed_bytes_per_second_bounded,max_ingest_uncompressed_bytes_per_second BETWEEN 0 AND 1099511627776"`
}

func (collectorTokenRecord) TableName() string {
	return "ingestion_tokens"
}

// collectorTokenIndexRecord is the explicit GORM representation of
// ingestion_token_indexes. The SQL migration remains authoritative for its
// composite foreign keys, STRICT mode, WITHOUT ROWID storage, and delete
// actions.
type collectorTokenIndexRecord struct {
	IngestionTokenID string `gorm:"column:ingestion_token_id;type:text;primaryKey;not null;index:ingestion_token_indexes_index_idx,priority:2"`
	IndexID          string `gorm:"column:index_id;type:text;primaryKey;not null;index:ingestion_token_indexes_index_idx,priority:1"`
}

func (collectorTokenIndexRecord) TableName() string {
	return "ingestion_token_indexes"
}

// collectorTokenMetadataRow is a read-only parent projection. Scope children
// are hydrated separately under a bounded query so corrupt fanout cannot make
// one aggregate string consume unbounded memory.
type collectorTokenMetadataRow struct {
	IngestionTokenID                    string              `gorm:"column:ingestion_token_id"`
	Version                             int64               `gorm:"column:version"`
	Name                                string              `gorm:"column:name"`
	Description                         string              `gorm:"column:description"`
	TokenPrefix                         string              `gorm:"column:token_prefix"`
	State                               CollectorTokenState `gorm:"column:state"`
	CreatedAtUnixMicro                  int64               `gorm:"column:created_at_unix_micro"`
	UpdatedAtUnixMicro                  int64               `gorm:"column:updated_at_unix_micro"`
	ExpiresAtUnixMicro                  *int64              `gorm:"column:expires_at_unix_micro"`
	RevokedAtUnixMicro                  *int64              `gorm:"column:revoked_at_unix_micro"`
	LastUsedAtUnixMicro                 *int64              `gorm:"column:last_used_at_unix_micro"`
	BoundCollectorID                    *string             `gorm:"column:bound_collector_id"`
	MaxIngestEventsPerSecond            int64               `gorm:"column:max_ingest_events_per_second"`
	MaxIngestUncompressedBytesPerSecond int64               `gorm:"column:max_ingest_uncompressed_bytes_per_second"`
	AllowedIndexNames                   []string            `gorm:"-"`
}

// collectorTokenProjectionWidths is a constant-size preflight projection.
// Persisted text is loaded only after every selected value has safe byte
// bounds.
type collectorTokenProjectionWidths struct {
	IngestionTokenIDBytes int64  `gorm:"column:ingestion_token_id_bytes"`
	NameBytes             int64  `gorm:"column:name_bytes"`
	DescriptionBytes      int64  `gorm:"column:description_bytes"`
	TokenPrefixBytes      int64  `gorm:"column:token_prefix_bytes"`
	BoundCollectorIDBytes *int64 `gorm:"column:bound_collector_id_bytes"`
}

type collectorTokenMetadataScopeWidths struct {
	IngestionTokenIDBytes int64  `gorm:"column:ingestion_token_id_bytes"`
	IndexNameBytes        *int64 `gorm:"column:index_name_bytes"`
}

type collectorTokenMetadataScopeRow struct {
	IngestionTokenID string         `gorm:"column:ingestion_token_id"`
	IndexName        sql.NullString `gorm:"column:index_name"`
}

type collectorTokenScopeDistributionRow struct {
	StateKind   int64 `gorm:"column:state_kind"`
	ScopeCount  int64 `gorm:"column:scope_count"`
	TargetCount int64 `gorm:"column:target_count"`
}

type collectorTokenAuthenticationRow struct {
	IngestionTokenID                    string `gorm:"column:ingestion_token_id"`
	Name                                string `gorm:"column:name"`
	BoundCollectorID                    string `gorm:"column:bound_collector_id"`
	MaxIngestEventsPerSecond            int64  `gorm:"column:max_ingest_events_per_second"`
	MaxIngestUncompressedBytesPerSecond int64  `gorm:"column:max_ingest_uncompressed_bytes_per_second"`
}

type collectorTokenAuthenticationScopeRow struct {
	TargetPresent                       int64  `gorm:"column:target_present"`
	Name                                string `gorm:"column:name"`
	Version                             int64  `gorm:"column:version"`
	RetentionNanoseconds                int64  `gorm:"column:retention_nanoseconds"`
	DefaultSourcetype                   string `gorm:"column:default_sourcetype"`
	MaxEventBytes                       int64  `gorm:"column:max_event_bytes"`
	MaxFieldCount                       int64  `gorm:"column:max_field_count"`
	MaxNestingDepth                     int64  `gorm:"column:max_nesting_depth"`
	MaximumFutureSkewNanoseconds        int64  `gorm:"column:maximum_future_skew_nanoseconds"`
	MaximumEventAgeNanoseconds          int64  `gorm:"column:maximum_event_age_nanoseconds"`
	MaxIngestEventsPerSecond            int64  `gorm:"column:max_ingest_events_per_second"`
	MaxIngestUncompressedBytesPerSecond int64  `gorm:"column:max_ingest_uncompressed_bytes_per_second"`
	State                               string `gorm:"column:state"`
	IngestionEnabled                    int64  `gorm:"column:ingestion_enabled"`
}

type collectorTokenScopeTarget struct {
	IndexID string `gorm:"column:index_id"`
	Name    string `gorm:"column:name"`
}
