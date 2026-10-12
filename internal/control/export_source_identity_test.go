package control

import "testing"

func TestNativeExportSourceIdentityCannotBeRebound(t *testing.T) {
	database := openTestDB(t)
	ctx := t.Context()
	payload := []byte(`{"State":1,"SearchJobID":"source"}`)
	if _, err := database.SQLDB().ExecContext(ctx, `INSERT INTO durable_export_jobs(organization_id,tenant_id,owner_id,export_id,version,job_json,created_at_us,updated_at_us,metadata_bytes,source_job_id) VALUES(1,'physical','42','export',1,$1,1,1,4096,'source')`, payload); err != nil {
		t.Fatal(err)
	}
	for _, mutation := range []struct {
		query   string
		message string
	}{
		{`UPDATE durable_export_jobs SET source_job_id='different',job_json=convert_to('{"State":1,"SearchJobID":"different"}','UTF8') WHERE export_id='export'`, "export source identity is immutable"},
		{`UPDATE durable_export_jobs SET job_json=convert_to('{"State":1,"SearchJobID":"different"}','UTF8') WHERE export_id='export'`, "durable_export_job_source_matches_payload"},
		{`UPDATE durable_export_jobs SET job_json=convert_to('{"State":1}','UTF8') WHERE export_id='export'`, "durable_export_job_source_matches_payload"},
	} {
		_, err := database.SQLDB().ExecContext(ctx, mutation.query)
		requireNativeControlGuard(t, err, mutation.message)
	}
	if _, err := database.SQLDB().ExecContext(ctx, `UPDATE durable_export_jobs SET version=version+1,updated_at_us=2 WHERE export_id='export'`); err != nil {
		t.Fatalf("ordinary export progress: %v", err)
	}
	var source string
	if err := database.SQLDB().QueryRowContext(ctx, `SELECT source_job_id FROM durable_export_jobs WHERE export_id='export'`).Scan(&source); err != nil || source != "source" {
		t.Fatalf("retained source changed: %q: %v", source, err)
	}
}
