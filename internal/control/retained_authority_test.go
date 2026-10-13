package control

import "testing"

func TestNativeRetainedSearchAuthorityCannotBeDowngraded(t *testing.T) {
	database := openTestDB(t)
	ctx := t.Context()
	payload := []byte(`{"job":{"SitePrivilegeUsed":true,"KnowledgeObjectIDs":["private-object"]}}`)
	if _, err := database.SQLDB().ExecContext(ctx, `INSERT INTO durable_search_jobs(id,tenant_id,organization_id,owner_id,state,retention_class,lifetime_ns,job_payload,created_at_us,version) VALUES('authority','physical',1,'42',1,1,1,$1,1,1)`, payload); err != nil {
		t.Fatal(err)
	}
	for _, replacement := range []string{
		`{"job":{"SitePrivilegeUsed":false,"KnowledgeObjectIDs":["private-object"]}}`,
		`{"job":{"SitePrivilegeUsed":true,"KnowledgeObjectIDs":["different-object"]}}`,
		`{"job":{"SitePrivilegeUsed":true,"KnowledgeObjectIDs":null}}`,
	} {
		if _, err := database.SQLDB().ExecContext(ctx, `UPDATE durable_search_jobs SET job_payload=$1 WHERE id='authority'`, []byte(replacement)); err == nil {
			t.Fatal("retained source authority changed")
		}
	}
	if _, err := database.SQLDB().ExecContext(ctx, `UPDATE durable_search_jobs SET state=2,version=version+1 WHERE id='authority'`); err != nil {
		t.Fatalf("ordinary lifecycle update: %v", err)
	}
}
