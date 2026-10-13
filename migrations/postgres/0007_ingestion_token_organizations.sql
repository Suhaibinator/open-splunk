-- Machine credentials are capabilities owned by one organization. The issuer
-- is immutable audit provenance; their continued account lifetime is not a
-- condition for an already-issued organization capability.
ALTER TABLE ingestion_tokens ADD COLUMN organization_id BIGINT NOT NULL CHECK(organization_id>0);
ALTER TABLE ingestion_tokens ADD COLUMN issuer_user_id BIGINT NOT NULL CHECK(issuer_user_id>0);
CREATE FUNCTION os_ingestion_token_ownership_immutable() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
 IF NEW.organization_id IS DISTINCT FROM OLD.organization_id OR NEW.issuer_user_id IS DISTINCT FROM OLD.issuer_user_id THEN RAISE EXCEPTION 'ingestion token organization and issuer are immutable' USING ERRCODE='23514'; END IF;
 RETURN NEW; END; $$;
CREATE TRIGGER ingestion_token_ownership_immutable BEFORE UPDATE ON ingestion_tokens FOR EACH ROW EXECUTE FUNCTION os_ingestion_token_ownership_immutable();
CREATE INDEX ingestion_tokens_organization ON ingestion_tokens(organization_id,ingestion_token_id);
