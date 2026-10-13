-- Search captures and derived exports share physical retention budgets.
CREATE TABLE search_retained_export_reservations (
 export_job_id TEXT PRIMARY KEY COLLATE "C",
 organization_id BIGINT NOT NULL CHECK (organization_id > 0),
 reserved_bytes BIGINT NOT NULL CHECK (reserved_bytes >= 0)
);
CREATE INDEX search_retained_export_reservations_org ON search_retained_export_reservations(organization_id);
CREATE TRIGGER os_write_guard_export_reservations BEFORE INSERT OR UPDATE OR DELETE ON search_retained_export_reservations FOR EACH STATEMENT EXECUTE FUNCTION os_control_write_lock();
