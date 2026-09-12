package requestidempotency_test

import (
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/requestidempotency"
	"gorm.io/gorm"
)

func TestPublicReceiptsIsolateConfiguredScopeAndRejectAdministrativeRoutes(t *testing.T) {
	db, err := control.Open(t.Context(), t.TempDir()+"/control.db")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	now := time.Now().UTC()
	publicRoutes := []struct{ route, kind string }{
		{requestidempotency.RouteCreateSearchJob, requestidempotency.TargetSearchJob},
		{requestidempotency.RouteCreateExportJob, requestidempotency.TargetExportJob},
		{requestidempotency.RouteCreateSavedSearch, requestidempotency.TargetSavedSearch},
		{requestidempotency.RouteDuplicateSavedSearch, requestidempotency.TargetSavedSearch},
	}
	canonical := &opensplunk.CreateSearchJobRequest{}
	for _, route := range publicRoutes {
		for index, scope := range []struct{ tenant, owner, actor string }{{"tenant-a", "owner-a", requestidempotency.ActorKindPublic}, {"tenant-a", "owner-b", requestidempotency.ActorKindPublic}, {"tenant-b", "owner-a", requestidempotency.ActorKindPublic}, {"tenant-a", "owner-a", "browser"}} {
			intent, err := requestidempotency.NewIntent(scope.tenant, scope.actor, scope.owner, route.route, "public-request-key-01", canonical)
			if err != nil {
				t.Fatal(err)
			}
			if _, found, err := requestidempotency.Read(t.Context(), db.GORMDB(), intent); err != nil || found {
				t.Fatalf("scope collision %v: %t %v", scope, found, err)
			}
			target := requestidempotency.Target{Kind: route.kind, ID: fmt.Sprintf("target-%d", index), Version: 1}
			if err := db.GORMDB().Transaction(func(tx *gorm.DB) error {
				_, err := requestidempotency.AppendInTransaction(t.Context(), tx, intent, target, nil, now)
				return err
			}); err != nil {
				t.Fatal(err)
			}
			if replay, found, err := requestidempotency.Read(t.Context(), db.GORMDB(), intent); err != nil || !found || replay.Target != target {
				t.Fatalf("public replay %v %t %v", replay, found, err)
			}
			changed := intent
			changed.RequestSHA256[0] ^= 0xff
			if _, _, err := requestidempotency.Read(t.Context(), db.GORMDB(), changed); !errors.Is(err, requestidempotency.ErrConflict) {
				t.Fatalf("changed public intent: %v", err)
			}
		}
	}
	for _, route := range []struct{ route, kind string }{
		{requestidempotency.RouteCreateApp, requestidempotency.TargetApp},
		{requestidempotency.RouteCreateIndex, requestidempotency.TargetIndex},
		{requestidempotency.RouteCreateIngestionToken, requestidempotency.TargetIngestionToken},
		{requestidempotency.RouteCreateLookup, requestidempotency.TargetLookup},
	} {
		if _, err := requestidempotency.NewIntent("tenant-a", requestidempotency.ActorKindPublic, "owner-a", route.route, "rejected-public-key", canonical); !errors.Is(err, requestidempotency.ErrInvalid) {
			t.Fatalf("public administrative intent accepted: %s %v", route.route, err)
		}
		err := db.GORMDB().Exec(`INSERT INTO api_mutation_receipts
   (tenant_id,actor_kind,actor_id,route,client_request_id,canonical_version,request_sha256,target_kind,target_id,target_version,audit_sequence,created_at_unix_micro,retain_until_unix_micro,target_terminal_at_unix_micro,encoded_bytes)
   SELECT tenant_id,actor_kind,actor_id,?,'rejected-public-key',canonical_version,request_sha256,?,target_id,target_version,audit_sequence,created_at_unix_micro,retain_until_unix_micro,target_terminal_at_unix_micro,
   encoded_bytes+length(?)-length(route)+length(?)-length(target_kind)+length('rejected-public-key')-length(client_request_id)
   FROM api_mutation_receipts WHERE tenant_id='tenant-a' AND actor_id='owner-a' AND actor_kind='public' AND route=?`, route.route, route.kind, route.route, route.kind, requestidempotency.RouteCreateSavedSearch).Error
		if err == nil || !strings.Contains(err.Error(), "api_mutation_receipt_public_actor_route") {
			t.Fatalf("SQLite public route fence: %s %v", route.route, err)
		}
	}
}
