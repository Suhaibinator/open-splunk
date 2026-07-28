ALTER TABLE ingestion_tokens
ADD COLUMN last_used_at_unix_micro INTEGER
    CONSTRAINT ingestion_tokens_last_use_not_before_create
    CHECK (
        last_used_at_unix_micro IS NULL
        OR last_used_at_unix_micro >= created_at_unix_micro
    );
