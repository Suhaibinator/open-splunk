package auth

import (
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/control"
)

func TestCollectorTokenOrganizationFencesCurrentAndDurableAdmission(t *testing.T) {
	db := openControlDB(t)
	main, err := db.CreateIndex(t.Context(), activeIndex("org-main"))
	if err != nil {
		t.Fatal(err)
	}
	foreign, err := db.CreateIndex(t.Context(), activeIndex("org-foreign"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.SQLDB().ExecContext(t.Context(), `CREATE TABLE os_index_organizations(index_id TEXT PRIMARY KEY, organization_id BIGINT NOT NULL)`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.SQLDB().ExecContext(t.Context(), `INSERT INTO os_index_organizations VALUES($1,1),($2,2)`, main.ID, foreign.ID); err != nil {
		t.Fatal(err)
	}
	store, err := NewStoreWithOptions(db, []byte("0123456789abcdef0123456789abcdef"), StoreOptions{RequireOrganizationOwnership: true})
	if err != nil {
		t.Fatal(err)
	}
	request := CreateCollectorTokenRequest{OrganizationID: 1, IssuerUserID: 42, Name: "owned token", AllowedIndexNames: []string{"org-main"}, BoundCollectorID: testCollectorID}
	invalid := request
	invalid.OrganizationID = 0
	if _, err := store.CreateCollectorToken(t.Context(), invalid); !errors.Is(err, control.ErrInvalidArgument) {
		t.Fatalf("missing organization = %v", err)
	}
	invalid = request
	invalid.AllowedIndexNames = []string{"org-foreign", "org-main"}
	if _, err := store.CreateCollectorToken(t.Context(), invalid); !errors.Is(err, control.ErrInvalidArgument) {
		t.Fatalf("cross-organization mint = %v", err)
	}
	issued, err := store.CreateCollectorToken(t.Context(), request)
	if err != nil {
		t.Fatal(err)
	}
	metadata, err := store.GetCollectorToken(t.Context(), issued.Token.ID)
	if err != nil || metadata.OrganizationID != 1 || metadata.IssuerUserID != 42 {
		t.Fatalf("immutable metadata = %+v, %v", metadata, err)
	}
	if _, err := db.SQLDB().ExecContext(t.Context(), `UPDATE ingestion_tokens SET organization_id=2 WHERE ingestion_token_id=$1`, issued.Token.ID); err == nil {
		t.Fatal("database allowed token organization change")
	}
	if _, err := db.SQLDB().ExecContext(t.Context(), `UPDATE ingestion_tokens SET issuer_user_id=43 WHERE ingestion_token_id=$1`, issued.Token.ID); err == nil {
		t.Fatal("database allowed issuer change")
	}
	if _, err := store.UpdateCollectorToken(t.Context(), issued.Token.ID, 1, UpdateCollectorTokenRequest{OrganizationID: 2}); !errors.Is(err, control.ErrInvalidArgument) {
		t.Fatalf("service allowed organization change: %v", err)
	}
	authentication, err := store.Authenticate(t.Context(), issued.Secret.Plaintext())
	if err != nil || authentication.OrganizationID != 1 || !authentication.OrganizationOwnershipRequired {
		t.Fatalf("native current authentication = %+v, %v", authentication, err)
	}
	hecRequest := request
	hecRequest.Name, hecRequest.Purpose, hecRequest.BoundCollectorID = "owned HEC", IngestionTokenPurposeHEC, ""
	hec, err := store.CreateCollectorToken(t.Context(), hecRequest)
	if err != nil {
		t.Fatal(err)
	}
	hecAuthentication, err := store.AuthenticateHEC(t.Context(), hec.Secret.Plaintext())
	if err != nil || !hecAuthentication.OrganizationOwnershipRequired {
		t.Fatalf("HEC current authentication: %v", err)
	}
	authority := HECAdmissionAuthority{OrganizationID: 1, RequireOrganizationOwnership: true, TokenID: hec.Token.ID, TokenVersion: hec.Token.Version, Indexes: []HECIndexAuthoritySnapshot{{Name: main.Definition.Name, Version: main.Version}}}
	tx, err := db.SQLDB().BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := RevalidateHECAdmissionInTransaction(t.Context(), tx, authority, time.Now()); err != nil {
		_ = tx.Rollback()
		t.Fatal(err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	// Index ownership can change independently of its generation. The immutable
	// credential organization must fence both fresh authentication and staging.
	if _, err := db.SQLDB().ExecContext(t.Context(), `UPDATE os_index_organizations SET organization_id=2 WHERE index_id=$1`, main.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Authenticate(t.Context(), issued.Secret.Plaintext()); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("moved-index collector authentication = %v", err)
	}
	if _, err := store.Authorize(t.Context(), issued.Secret.Plaintext(), main.Definition.Name); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("moved-index direct bearer authorization = %v", err)
	}
	if _, err := store.AuthenticateHEC(t.Context(), hec.Secret.Plaintext()); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("moved-index HEC authentication = %v", err)
	}
	tx, err = db.SQLDB().BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := tx.Rollback(); err != nil && !errors.Is(err, sql.ErrTxDone) {
			t.Error(err)
		}
	}()
	if err := RevalidateHECAdmissionInTransaction(t.Context(), tx, authority, time.Now()); !errors.Is(err, ErrStaleHECAdmission) {
		t.Fatalf("moved-index durable admission = %v", err)
	}
}
