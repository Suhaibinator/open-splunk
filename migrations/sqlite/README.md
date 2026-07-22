# SQLite migrations

Migration files are named `NNNN_description.sql`, embedded into the server, and
applied in a transaction in strictly contiguous version order. Applied names
and SHA-256 checksums are recorded in `schema_migrations`; changing a released
migration is rejected as schema drift. Add a new migration instead.
