-- Revoked ingestion-token tombstones are retained in newest-first order.
-- Keep pruning on the bounded revoked subset and avoid sorting or scanning
-- active and disabled credentials.

CREATE INDEX ingestion_tokens_revoked_retention_idx
    ON ingestion_tokens (
        revoked_at_unix_micro DESC,
        ingestion_token_id DESC
    )
    WHERE state = 'revoked';
