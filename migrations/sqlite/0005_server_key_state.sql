-- Bind durable HMAC-derived control-plane records to the external server
-- master key. The key itself never enters SQLite; only a one-way fingerprint
-- is retained so loss or accidental replacement fails closed at startup.

CREATE TABLE server_key_state (
    key_name TEXT PRIMARY KEY NOT NULL COLLATE BINARY,
    fingerprint BLOB NOT NULL CHECK (length(fingerprint) = 32),
    created_at_unix_micro INTEGER NOT NULL,
    CHECK (key_name = 'server-master-v1')
) STRICT;
