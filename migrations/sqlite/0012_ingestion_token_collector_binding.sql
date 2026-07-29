ALTER TABLE ingestion_tokens
ADD COLUMN bound_collector_id TEXT
    CONSTRAINT ingestion_tokens_bound_collector_id_canonical
    CHECK (
        bound_collector_id IS NULL
        OR (
            length(bound_collector_id) BETWEEN 1 AND 128
            AND instr(bound_collector_id, char(0)) = 0
            AND substr(bound_collector_id, 1, 1) GLOB '[A-Za-z0-9]'
            AND bound_collector_id NOT GLOB '*[^A-Za-z0-9._:-]*'
        )
    );

CREATE TRIGGER ingestion_token_collector_binding_is_required
BEFORE INSERT ON ingestion_tokens
WHEN NEW.bound_collector_id IS NULL
BEGIN
    SELECT RAISE(ABORT, 'ingestion token collector binding is required');
END;

CREATE TRIGGER ingestion_token_collector_binding_is_immutable
BEFORE UPDATE OF bound_collector_id ON ingestion_tokens
WHEN OLD.bound_collector_id IS NOT NULL
     AND (
         NEW.bound_collector_id IS NULL
         OR NEW.bound_collector_id <> OLD.bound_collector_id
     )
BEGIN
    SELECT RAISE(ABORT, 'ingestion token collector binding is immutable');
END;
