-- Bounded immutable CSV lookup-asset storage. These tables are deliberately
-- physical and unexposed: logical lookup definitions, visibility selectors,
-- mappings, and overwrite policy live in a later sibling knowledge layer and
-- bind one exact never-deleted (tenant, asset, version, digest) reference.

CREATE TABLE knowledge_lookup_asset_tenant_ledgers (
    tenant_id TEXT PRIMARY KEY NOT NULL COLLATE BINARY,
    staged_asset_count INTEGER NOT NULL DEFAULT 0 CHECK (
        staged_asset_count BETWEEN 0 AND 64
    ),
    asset_identity_count INTEGER NOT NULL DEFAULT 0 CHECK (
        asset_identity_count BETWEEN 0 AND 2048
    ),
    published_version_count INTEGER NOT NULL DEFAULT 0 CHECK (
        published_version_count BETWEEN 0 AND 8192
    ),
    stored_content_bytes INTEGER NOT NULL DEFAULT 0 CHECK (
        stored_content_bytes BETWEEN 0 AND 2147483648
    ),
    FOREIGN KEY (tenant_id) REFERENCES knowledge_catalog_tenants (tenant_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT
) STRICT, WITHOUT ROWID;

INSERT INTO knowledge_lookup_asset_tenant_ledgers (tenant_id)
SELECT tenant_id FROM knowledge_catalog_tenants ORDER BY tenant_id;

CREATE TABLE knowledge_lookup_asset_stages (
    tenant_id TEXT NOT NULL COLLATE BINARY,
    stage_id TEXT NOT NULL COLLATE BINARY,
    owner_id TEXT NOT NULL COLLATE BINARY,
    source_sha256 BLOB NOT NULL CHECK (length(source_sha256) = 32),
    content_sha256 BLOB NOT NULL CHECK (length(content_sha256) = 32),
    source_bytes INTEGER NOT NULL CHECK (
        source_bytes BETWEEN 1 AND 8388608
    ),
    canonical_csv BLOB NOT NULL,
    canonical_bytes INTEGER NOT NULL CHECK (
        canonical_bytes BETWEEN 1 AND 8388608
        AND canonical_bytes = length(canonical_csv)
    ),
    row_count INTEGER NOT NULL CHECK (row_count BETWEEN 0 AND 100000),
    column_count INTEGER NOT NULL CHECK (column_count BETWEEN 1 AND 64),
    created_at_unix_micro INTEGER NOT NULL CHECK (
        created_at_unix_micro BETWEEN 1 AND 253402300799999999
    ),
    expires_at_unix_micro INTEGER NOT NULL CHECK (
        expires_at_unix_micro BETWEEN 2 AND 253402300799999999
        AND expires_at_unix_micro > created_at_unix_micro
    ),
    PRIMARY KEY (tenant_id, stage_id),
    CONSTRAINT knowledge_lookup_asset_stage_id_bounded CHECK (
        length(CAST(stage_id AS BLOB)) BETWEEN 1 AND 128
        AND instr(CAST(stage_id AS BLOB), X'00') = 0
        AND stage_id = trim(stage_id)
        AND stage_id NOT GLOB (
            '*[' || char(1) || '-' || char(31)
            || char(127) || '-' || char(159) || ']*'
        )
    ),
    CONSTRAINT knowledge_lookup_asset_stage_owner_bounded CHECK (
        length(CAST(owner_id AS BLOB)) BETWEEN 1 AND 255
        AND instr(CAST(owner_id AS BLOB), X'00') = 0
        AND owner_id = trim(owner_id)
        AND owner_id NOT GLOB (
            '*[' || char(1) || '-' || char(31)
            || char(127) || '-' || char(159) || ']*'
        )
    ),
    FOREIGN KEY (tenant_id)
        REFERENCES knowledge_lookup_asset_tenant_ledgers (tenant_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT
) STRICT, WITHOUT ROWID;

CREATE INDEX knowledge_lookup_asset_stage_expiry_idx
    ON knowledge_lookup_asset_stages (
        expires_at_unix_micro, tenant_id, stage_id
    );

-- The registry and immutable versions form a deferred cycle. New identities
-- begin at version one. Replacements first advance current_version by exactly
-- one and then insert that exact version in the same transaction; omitting
-- either half fails the deferred foreign key at commit.
CREATE TABLE knowledge_lookup_assets (
    tenant_id TEXT NOT NULL COLLATE BINARY,
    lookup_asset_id TEXT NOT NULL COLLATE BINARY,
    current_version INTEGER NOT NULL CHECK (
        current_version BETWEEN 1 AND 9223372036854775807
    ),
    created_at_unix_micro INTEGER NOT NULL CHECK (
        created_at_unix_micro BETWEEN 1 AND 253402300799999999
    ),
    updated_at_unix_micro INTEGER NOT NULL CHECK (
        updated_at_unix_micro BETWEEN created_at_unix_micro AND 253402300799999999
    ),
    PRIMARY KEY (tenant_id, lookup_asset_id),
    UNIQUE (tenant_id, lookup_asset_id, current_version),
    CONSTRAINT knowledge_lookup_asset_id_bounded CHECK (
        length(CAST(lookup_asset_id AS BLOB)) BETWEEN 1 AND 128
        AND instr(CAST(lookup_asset_id AS BLOB), X'00') = 0
        AND lookup_asset_id = trim(lookup_asset_id)
        AND lookup_asset_id NOT GLOB (
            '*[' || char(1) || '-' || char(31)
            || char(127) || '-' || char(159) || ']*'
        )
    ),
    FOREIGN KEY (tenant_id)
        REFERENCES knowledge_lookup_asset_tenant_ledgers (tenant_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
    FOREIGN KEY (tenant_id, lookup_asset_id, current_version)
        REFERENCES knowledge_lookup_asset_versions (
            tenant_id, lookup_asset_id, asset_version
        ) ON UPDATE RESTRICT ON DELETE RESTRICT
        DEFERRABLE INITIALLY DEFERRED
) STRICT, WITHOUT ROWID;

CREATE TABLE knowledge_lookup_asset_versions (
    tenant_id TEXT NOT NULL COLLATE BINARY,
    lookup_asset_id TEXT NOT NULL COLLATE BINARY,
    asset_version INTEGER NOT NULL CHECK (
        asset_version BETWEEN 1 AND 9223372036854775807
    ),
    source_sha256 BLOB NOT NULL CHECK (length(source_sha256) = 32),
    content_sha256 BLOB NOT NULL CHECK (length(content_sha256) = 32),
    source_bytes INTEGER NOT NULL CHECK (
        source_bytes BETWEEN 1 AND 8388608
    ),
    canonical_csv BLOB NOT NULL,
    canonical_bytes INTEGER NOT NULL CHECK (
        canonical_bytes BETWEEN 1 AND 8388608
        AND canonical_bytes = length(canonical_csv)
    ),
    row_count INTEGER NOT NULL CHECK (row_count BETWEEN 0 AND 100000),
    column_count INTEGER NOT NULL CHECK (column_count BETWEEN 1 AND 64),
    created_at_unix_micro INTEGER NOT NULL CHECK (
        created_at_unix_micro BETWEEN 1 AND 253402300799999999
    ),
    PRIMARY KEY (tenant_id, lookup_asset_id, asset_version),
    UNIQUE (
        tenant_id, lookup_asset_id, asset_version,
        content_sha256, canonical_bytes
    ),
    FOREIGN KEY (tenant_id, lookup_asset_id)
        REFERENCES knowledge_lookup_assets (tenant_id, lookup_asset_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT
        DEFERRABLE INITIALLY DEFERRED
) STRICT, WITHOUT ROWID;

CREATE INDEX knowledge_lookup_asset_version_digest_idx
    ON knowledge_lookup_asset_versions (
        tenant_id, content_sha256, lookup_asset_id, asset_version
    );

CREATE TRIGGER knowledge_catalog_tenant_provisions_lookup_asset_ledger
AFTER INSERT ON knowledge_catalog_tenants
BEGIN
    INSERT INTO knowledge_lookup_asset_tenant_ledgers (tenant_id)
    VALUES (NEW.tenant_id);
END;

CREATE TRIGGER knowledge_lookup_asset_ledger_identity_collision_is_forbidden
BEFORE INSERT ON knowledge_lookup_asset_tenant_ledgers
WHEN EXISTS (
    SELECT 1 FROM knowledge_lookup_asset_tenant_ledgers
    WHERE tenant_id = NEW.tenant_id
)
BEGIN
    SELECT RAISE(ABORT, 'lookup asset tenant ledger already exists');
END;

CREATE TRIGGER knowledge_lookup_asset_ledger_initial_shape_is_exact
BEFORE INSERT ON knowledge_lookup_asset_tenant_ledgers
WHEN NEW.staged_asset_count <> 0
  OR NEW.asset_identity_count <> 0
  OR NEW.published_version_count <> 0
  OR NEW.stored_content_bytes <> 0
  OR EXISTS (
      SELECT 1 FROM knowledge_lookup_asset_stages
      WHERE tenant_id = NEW.tenant_id
  )
  OR EXISTS (
      SELECT 1 FROM knowledge_lookup_assets
      WHERE tenant_id = NEW.tenant_id
  )
  OR EXISTS (
      SELECT 1 FROM knowledge_lookup_asset_versions
      WHERE tenant_id = NEW.tenant_id
  )
BEGIN
    SELECT RAISE(ABORT, 'lookup asset tenant ledger must begin empty');
END;

CREATE TRIGGER knowledge_lookup_asset_ledger_transition_is_exact
BEFORE UPDATE ON knowledge_lookup_asset_tenant_ledgers
WHEN NEW.tenant_id <> OLD.tenant_id
  OR NEW.staged_asset_count <> (
      SELECT count(*) FROM knowledge_lookup_asset_stages
      WHERE tenant_id = OLD.tenant_id
  )
  OR NEW.asset_identity_count <> (
      SELECT count(*) FROM knowledge_lookup_assets
      WHERE tenant_id = OLD.tenant_id
  )
  OR NEW.published_version_count <> (
      SELECT count(*) FROM knowledge_lookup_asset_versions
      WHERE tenant_id = OLD.tenant_id
  )
  OR NEW.stored_content_bytes <> (
      COALESCE((
          SELECT sum(canonical_bytes) FROM knowledge_lookup_asset_stages
          WHERE tenant_id = OLD.tenant_id
      ), 0)
      + COALESCE((
          SELECT sum(canonical_bytes) FROM knowledge_lookup_asset_versions
          WHERE tenant_id = OLD.tenant_id
      ), 0)
  )
BEGIN
    SELECT RAISE(ABORT, 'lookup asset tenant ledger transition is invalid');
END;

CREATE TRIGGER knowledge_lookup_asset_ledger_delete_is_forbidden
BEFORE DELETE ON knowledge_lookup_asset_tenant_ledgers
BEGIN
    SELECT RAISE(ABORT, 'lookup asset tenant ledger cannot be deleted');
END;

CREATE TRIGGER knowledge_lookup_asset_stage_capacity_is_available
BEFORE INSERT ON knowledge_lookup_asset_stages
WHEN EXISTS (
    SELECT 1
    FROM knowledge_lookup_asset_tenant_ledgers AS ledger
    WHERE ledger.tenant_id = NEW.tenant_id
)
AND NOT EXISTS (
    SELECT 1
    FROM knowledge_lookup_asset_tenant_ledgers AS ledger
    WHERE ledger.tenant_id = NEW.tenant_id
      AND ledger.staged_asset_count < 64
      AND ledger.stored_content_bytes <= 2147483648 - NEW.canonical_bytes
)
BEGIN
    SELECT RAISE(ABORT, 'lookup asset staging capacity exhausted');
END;

CREATE TRIGGER knowledge_lookup_asset_stage_requires_tenant_ledger
BEFORE INSERT ON knowledge_lookup_asset_stages
WHEN NOT EXISTS (
    SELECT 1 FROM knowledge_lookup_asset_tenant_ledgers
    WHERE tenant_id = NEW.tenant_id
)
BEGIN
    SELECT RAISE(ABORT, 'lookup asset tenant ledger is missing');
END;

CREATE TRIGGER knowledge_lookup_asset_stage_updates_are_forbidden
BEFORE UPDATE ON knowledge_lookup_asset_stages
BEGIN
    SELECT RAISE(ABORT, 'lookup asset stages are immutable');
END;

CREATE TRIGGER knowledge_lookup_asset_stage_accounts_after_insert
AFTER INSERT ON knowledge_lookup_asset_stages
BEGIN
    UPDATE knowledge_lookup_asset_tenant_ledgers
    SET staged_asset_count = staged_asset_count + 1,
        stored_content_bytes = stored_content_bytes + NEW.canonical_bytes
    WHERE tenant_id = NEW.tenant_id;
END;

CREATE TRIGGER knowledge_lookup_asset_stage_accounts_after_delete
AFTER DELETE ON knowledge_lookup_asset_stages
BEGIN
    UPDATE knowledge_lookup_asset_tenant_ledgers
    SET staged_asset_count = staged_asset_count - 1,
        stored_content_bytes = stored_content_bytes - OLD.canonical_bytes
    WHERE tenant_id = OLD.tenant_id;
END;

CREATE TRIGGER knowledge_lookup_asset_identity_capacity_is_available
BEFORE INSERT ON knowledge_lookup_assets
WHEN EXISTS (
    SELECT 1
    FROM knowledge_lookup_asset_tenant_ledgers AS ledger
    WHERE ledger.tenant_id = NEW.tenant_id
)
AND (
    NEW.current_version <> 1
    OR NOT EXISTS (
      SELECT 1
      FROM knowledge_lookup_asset_tenant_ledgers AS ledger
      WHERE ledger.tenant_id = NEW.tenant_id
        AND ledger.asset_identity_count < 2048
    )
)
BEGIN
    SELECT RAISE(ABORT, 'lookup asset identity capacity is unavailable or first version is invalid');
END;

CREATE TRIGGER knowledge_lookup_asset_identity_requires_tenant_ledger
BEFORE INSERT ON knowledge_lookup_assets
WHEN NOT EXISTS (
    SELECT 1 FROM knowledge_lookup_asset_tenant_ledgers
    WHERE tenant_id = NEW.tenant_id
)
BEGIN
    SELECT RAISE(ABORT, 'lookup asset tenant ledger is missing');
END;

CREATE TRIGGER knowledge_lookup_asset_identity_accounts_after_insert
AFTER INSERT ON knowledge_lookup_assets
BEGIN
    UPDATE knowledge_lookup_asset_tenant_ledgers
    SET asset_identity_count = asset_identity_count + 1
    WHERE tenant_id = NEW.tenant_id;
END;

CREATE TRIGGER knowledge_lookup_asset_identity_transition_is_exact
BEFORE UPDATE ON knowledge_lookup_assets
WHEN NEW.tenant_id <> OLD.tenant_id
  OR NEW.lookup_asset_id <> OLD.lookup_asset_id
  OR NEW.created_at_unix_micro <> OLD.created_at_unix_micro
  OR NEW.current_version <> OLD.current_version + 1
  OR NEW.updated_at_unix_micro < OLD.updated_at_unix_micro
BEGIN
    SELECT RAISE(ABORT, 'lookup asset current version transition is invalid');
END;

CREATE TRIGGER knowledge_lookup_asset_identity_delete_is_forbidden
BEFORE DELETE ON knowledge_lookup_assets
BEGIN
    SELECT RAISE(ABORT, 'lookup asset identities cannot be deleted');
END;

CREATE TRIGGER knowledge_lookup_asset_version_capacity_is_available
BEFORE INSERT ON knowledge_lookup_asset_versions
WHEN EXISTS (
    SELECT 1
    FROM knowledge_lookup_asset_tenant_ledgers AS ledger
    WHERE ledger.tenant_id = NEW.tenant_id
)
AND NOT EXISTS (
    SELECT 1
    FROM knowledge_lookup_asset_tenant_ledgers AS ledger
    WHERE ledger.tenant_id = NEW.tenant_id
      AND ledger.published_version_count < 8192
      AND ledger.stored_content_bytes <= 2147483648 - NEW.canonical_bytes
)
BEGIN
    SELECT RAISE(ABORT, 'lookup asset version capacity exhausted');
END;

CREATE TRIGGER knowledge_lookup_asset_version_requires_tenant_ledger
BEFORE INSERT ON knowledge_lookup_asset_versions
WHEN NOT EXISTS (
    SELECT 1 FROM knowledge_lookup_asset_tenant_ledgers
    WHERE tenant_id = NEW.tenant_id
)
BEGIN
    SELECT RAISE(ABORT, 'lookup asset tenant ledger is missing');
END;

CREATE TRIGGER knowledge_lookup_asset_version_sequence_is_exact
BEFORE INSERT ON knowledge_lookup_asset_versions
WHEN NOT (
    (
        NEW.asset_version = 1
        AND EXISTS (
            SELECT 1 FROM knowledge_lookup_assets AS asset
            WHERE asset.tenant_id = NEW.tenant_id
              AND asset.lookup_asset_id = NEW.lookup_asset_id
              AND asset.current_version = 1
              AND asset.created_at_unix_micro = NEW.created_at_unix_micro
              AND asset.updated_at_unix_micro = NEW.created_at_unix_micro
        )
        AND NOT EXISTS (
            SELECT 1 FROM knowledge_lookup_asset_versions AS prior
            WHERE prior.tenant_id = NEW.tenant_id
              AND prior.lookup_asset_id = NEW.lookup_asset_id
        )
    )
    OR (
        NEW.asset_version > 1
        AND EXISTS (
            SELECT 1
            FROM knowledge_lookup_assets AS asset
            JOIN knowledge_lookup_asset_versions AS prior
              ON prior.tenant_id = asset.tenant_id
             AND prior.lookup_asset_id = asset.lookup_asset_id
             AND prior.asset_version = NEW.asset_version - 1
            WHERE asset.tenant_id = NEW.tenant_id
              AND asset.lookup_asset_id = NEW.lookup_asset_id
              AND asset.current_version = NEW.asset_version
              AND asset.updated_at_unix_micro = NEW.created_at_unix_micro
              AND prior.created_at_unix_micro <= NEW.created_at_unix_micro
        )
    )
)
BEGIN
    SELECT RAISE(ABORT, 'lookup asset version sequence is invalid');
END;

CREATE TRIGGER knowledge_lookup_asset_version_accounts_after_insert
AFTER INSERT ON knowledge_lookup_asset_versions
BEGIN
    UPDATE knowledge_lookup_asset_tenant_ledgers
    SET published_version_count = published_version_count + 1,
        stored_content_bytes = stored_content_bytes + NEW.canonical_bytes
    WHERE tenant_id = NEW.tenant_id;
END;

CREATE TRIGGER knowledge_lookup_asset_version_updates_are_forbidden
BEFORE UPDATE ON knowledge_lookup_asset_versions
BEGIN
    SELECT RAISE(ABORT, 'published lookup asset versions are immutable');
END;

CREATE TRIGGER knowledge_lookup_asset_version_deletes_are_forbidden
BEFORE DELETE ON knowledge_lookup_asset_versions
BEGIN
    SELECT RAISE(ABORT, 'published lookup asset versions cannot be deleted');
END;
