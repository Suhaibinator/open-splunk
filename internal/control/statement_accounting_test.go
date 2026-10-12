package control

import "testing"

func TestPostgreSQLBulkAccountingPreservesExactState(t *testing.T) {
	db := openTestDB(t)
	ctx := t.Context()
	for _, query := range []string{
		`INSERT INTO audit_tenant_state(tenant_id,next_sequence,event_count) VALUES ('bulk',1,0)`,
		`INSERT INTO audit_events(tenant_id,sequence,occurred_at_unix_micro,actor_kind,actor_id,actor_role,action,target_kind,target_id,target_version)
		SELECT 'bulk',n,1,'system','system','system','index.create','index','index-'||n,1 FROM generate_series(1,3) AS n ORDER BY n`,
		`INSERT INTO api_mutation_receipt_tenant_state(tenant_id,high_water_unix_micro) VALUES ('bulk',3)`,
		`INSERT INTO api_mutation_receipts(tenant_id,actor_kind,actor_id,route,client_request_id,canonical_version,request_sha256,target_kind,target_id,target_version,created_at_unix_micro,retain_until_unix_micro,target_terminal_at_unix_micro,encoded_bytes)
		SELECT 'bulk','system','system','/api/indexes/create',lpad(n::text,16,'0'),1,decode(repeat('00',32),'hex'),'index','index-'||n,1,1,2,1,
		96+octet_length('bulk')+octet_length('system')+octet_length('system')+octet_length('/api/indexes/create')+16+32+octet_length('index')+octet_length('index-'||n)
		FROM generate_series(1,3) AS n`,
	} {
		if _, err := db.SQLDB().ExecContext(ctx, query); err != nil {
			t.Fatal(err)
		}
	}
	var sequence, events, receipts, bytes, actualBytes int64
	if err := db.SQLDB().QueryRowContext(ctx, `SELECT next_sequence,event_count FROM audit_tenant_state WHERE tenant_id='bulk'`).Scan(&sequence, &events); err != nil || sequence != 4 || events != 3 {
		t.Fatalf("bulk audit accounting: sequence=%d events=%d error=%v", sequence, events, err)
	}
	if _, err := db.SQLDB().ExecContext(ctx, `INSERT INTO audit_events(tenant_id,sequence,occurred_at_unix_micro,actor_kind,actor_id,actor_role,action,target_kind,target_id,target_version)
		SELECT 'bulk',n,1,'system','system','system','index.create','index','gap-'||n,1 FROM (VALUES(4),(6)) AS gaps(n)`); err == nil {
		t.Fatal("noncontiguous audit batch succeeded")
	}
	if err := db.SQLDB().QueryRowContext(ctx, `SELECT count(*) FROM audit_events WHERE tenant_id='bulk'`).Scan(&events); err != nil || events != 3 {
		t.Fatalf("invalid batch was not rolled back: events=%d error=%v", events, err)
	}
	if err := db.SQLDB().QueryRowContext(ctx, `SELECT receipt_count,encoded_bytes,(SELECT sum(encoded_bytes) FROM api_mutation_receipts WHERE tenant_id='bulk') FROM api_mutation_receipt_tenant_state WHERE tenant_id='bulk'`).Scan(&receipts, &bytes, &actualBytes); err != nil || receipts != 3 || bytes != actualBytes {
		t.Fatalf("bulk receipt accounting: receipts=%d bytes=%d actual=%d error=%v", receipts, bytes, actualBytes, err)
	}
	if _, err := db.SQLDB().ExecContext(ctx, `DELETE FROM api_mutation_receipts WHERE tenant_id='bulk'`); err != nil {
		t.Fatal(err)
	}
	if err := db.SQLDB().QueryRowContext(ctx, `SELECT receipt_count,encoded_bytes FROM api_mutation_receipt_tenant_state WHERE tenant_id='bulk'`).Scan(&receipts, &bytes); err != nil || receipts != 0 || bytes != 0 {
		t.Fatalf("bulk receipt cleanup: receipts=%d bytes=%d error=%v", receipts, bytes, err)
	}
	if _, err := db.SQLDB().ExecContext(ctx, `UPDATE audit_tenant_state SET next_sequence=6,event_count=5 WHERE tenant_id='bulk'`); err == nil {
		t.Fatal("forged audit accounting succeeded")
	}
}
