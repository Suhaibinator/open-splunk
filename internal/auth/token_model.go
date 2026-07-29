package auth

// collectorTokenRecord is the explicit GORM representation of
// ingestion_tokens.
//
// Versioned SQL migrations remain the sole schema authority. In particular,
// SQLite STRICT mode and the digest-immutability, irreversible-revocation, and
// collector-binding insert/update triggers cannot be represented completely
// by GORM tags. Never use AutoMigrate for this model.
type collectorTokenRecord struct {
	IngestionTokenID    string              `gorm:"column:ingestion_token_id;type:text;primaryKey;not null"`
	Version             int64               `gorm:"column:version;type:integer;not null;check:ingestion_tokens_version_positive,version >= 1"`
	Name                string              `gorm:"column:name;type:text;not null;check:ingestion_tokens_name_length,length(name) BETWEEN 1 AND 255"`
	Description         string              `gorm:"column:description;type:text;not null"`
	TokenPrefix         string              `gorm:"column:token_prefix;type:text;not null;check:ingestion_tokens_prefix_length,length(token_prefix) BETWEEN 8 AND 32"`
	TokenDigest         []byte              `gorm:"column:token_digest;type:blob;not null;unique;check:ingestion_tokens_digest_length,length(token_digest) = 32"`
	State               CollectorTokenState `gorm:"column:state;type:text;not null;check:ingestion_tokens_state,state IN ('active', 'disabled', 'revoked')"`
	CreatedAtUnixMicro  int64               `gorm:"column:created_at_unix_micro;type:integer;not null"`
	UpdatedAtUnixMicro  int64               `gorm:"column:updated_at_unix_micro;type:integer;not null;check:ingestion_tokens_update_not_before_create,updated_at_unix_micro >= created_at_unix_micro"`
	ExpiresAtUnixMicro  *int64              `gorm:"column:expires_at_unix_micro;type:integer;check:ingestion_tokens_expiration_after_create,expires_at_unix_micro IS NULL OR expires_at_unix_micro > created_at_unix_micro"`
	RevokedAtUnixMicro  *int64              `gorm:"column:revoked_at_unix_micro;type:integer;check:ingestion_tokens_revocation_consistency,(state = 'revoked' AND revoked_at_unix_micro IS NOT NULL) OR (state IN ('active', 'disabled') AND revoked_at_unix_micro IS NULL)"`
	LastUsedAtUnixMicro *int64              `gorm:"column:last_used_at_unix_micro;type:integer;check:ingestion_tokens_last_use_not_before_create,last_used_at_unix_micro IS NULL OR last_used_at_unix_micro >= created_at_unix_micro"`
	BoundCollectorID    *string             `gorm:"column:bound_collector_id;type:text;check:ingestion_tokens_bound_collector_id_canonical,bound_collector_id IS NULL OR (length(bound_collector_id) BETWEEN 1 AND 128 AND instr(bound_collector_id, char(0)) = 0 AND substr(bound_collector_id, 1, 1) GLOB '[A-Za-z0-9]' AND bound_collector_id NOT GLOB '*[^A-Za-z0-9._:-]*')"`
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

// collectorTokenMetadataRow is a read-only aggregate projection. It is kept
// separate from collectorTokenRecord so the persisted model cannot
// accidentally acquire a synthetic group-concatenation column.
type collectorTokenMetadataRow struct {
	IngestionTokenID    string              `gorm:"column:ingestion_token_id"`
	Version             int64               `gorm:"column:version"`
	Name                string              `gorm:"column:name"`
	Description         string              `gorm:"column:description"`
	TokenPrefix         string              `gorm:"column:token_prefix"`
	State               CollectorTokenState `gorm:"column:state"`
	CreatedAtUnixMicro  int64               `gorm:"column:created_at_unix_micro"`
	UpdatedAtUnixMicro  int64               `gorm:"column:updated_at_unix_micro"`
	ExpiresAtUnixMicro  *int64              `gorm:"column:expires_at_unix_micro"`
	RevokedAtUnixMicro  *int64              `gorm:"column:revoked_at_unix_micro"`
	LastUsedAtUnixMicro *int64              `gorm:"column:last_used_at_unix_micro"`
	BoundCollectorID    *string             `gorm:"column:bound_collector_id"`
	AllowedIndexNames   string              `gorm:"column:allowed_index_names"`
}

type collectorTokenAuthenticationRow struct {
	IngestionTokenID  string `gorm:"column:ingestion_token_id"`
	Name              string `gorm:"column:name"`
	BoundCollectorID  string `gorm:"column:bound_collector_id"`
	AllowedIndexNames string `gorm:"column:allowed_index_names"`
}

type collectorTokenScopeTarget struct {
	IndexID string `gorm:"column:index_id"`
	Name    string `gorm:"column:name"`
}
