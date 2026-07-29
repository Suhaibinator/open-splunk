package auth

// serverKeyStateRecord is the explicit GORM representation of
// server_key_state.
//
// Versioned SQL migrations remain the sole schema authority. In particular,
// SQLite STRICT mode is not represented by GORM tags. Never use AutoMigrate
// for this model.
type serverKeyStateRecord struct {
	KeyName            string `gorm:"column:key_name;type:text;primaryKey;not null;check:server_key_state_name_fixed,key_name = 'server-master-v1'"`
	Fingerprint        []byte `gorm:"column:fingerprint;type:blob;not null;check:server_key_state_fingerprint_sha256,length(fingerprint) = 32"`
	CreatedAtUnixMicro int64  `gorm:"column:created_at_unix_micro;type:integer;not null"`
}

func (serverKeyStateRecord) TableName() string {
	return "server_key_state"
}
