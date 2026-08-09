-- Future index-name creation can change selector applicability for every
-- tenant with an ACTIVE knowledge object. Keep both cross-tenant admission
-- drivers sparse and covering so empty tenant ledgers and inactive retained
-- registry rows cannot amplify the fixed publication work bounds.

CREATE INDEX knowledge_catalog_tenants_nonempty_active_idx
    ON knowledge_catalog_tenants (
        tenant_id, catalog_revision, active_object_count
    ) WHERE active_object_count > 0;

CREATE INDEX knowledge_objects_active_tenant_idx
    ON knowledge_objects (
        tenant_id, knowledge_object_id, current_version
    ) WHERE state = 'active';
