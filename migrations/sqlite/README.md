# SQLite migrations

Migration files are named `NNNN_description.sql`, embedded into the server, and
applied in a transaction in strictly contiguous version order. Applied names
and SHA-256 checksums are recorded in `schema_migrations`; changing a released
migration is rejected as schema drift. Add a new migration instead.

Index retention is stored in nanoseconds for Go duration compatibility but is
required to be a whole millisecond, matching ClickHouse `expires_at` precision.
Migration 0009 floors older unaligned values to their effective native storage
duration; a positive value below one millisecond becomes the smallest positive
representable policy instead of the zero/default sentinel. It also installs
insert/update guards for the durable invariant.
