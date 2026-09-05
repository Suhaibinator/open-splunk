CREATE TABLE server_appearance_settings (
    singleton_id INTEGER PRIMARY KEY NOT NULL CHECK (singleton_id = 1),
    version INTEGER NOT NULL CHECK (version BETWEEN 1 AND 9223372036854775807),
    palette TEXT NOT NULL COLLATE BINARY CHECK (
        palette IN ('classic', 'ocean', 'ember', 'graphite', 'glass', 'terminal')
    ),
    updated_at_unix_micro INTEGER NOT NULL CHECK (
        updated_at_unix_micro BETWEEN 1 AND 253402300799999999
    )
) STRICT, WITHOUT ROWID;
