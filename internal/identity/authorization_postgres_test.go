package identity

import (
	"context"
	"errors"
	"testing"

	"github.com/Suhaibinator/SRouter/pkg/scontext"
	pm "github.com/Suhaibinator/go-common/pkg/common_persistence/common_persistence_models"
	sm "github.com/Suhaibinator/go-common/pkg/common_service/common_service_models"
	pb "github.com/Suhaibinator/go-common/pkg/proto_models"
	"github.com/Suhaibinator/open-splunk/internal/authorization"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

func runtimeAuthorizationUsesNativeMembershipAndFreshType(t *testing.T, runtime *Runtime, pool *gorm.DB) {
	// The same PostgreSQL schema contains control metadata and native GoCommon
	// tables, as in startup; all membership mutations below use GoCommon service.
	source, ok := pool.Dialector.(*postgres.Dialector)
	if !ok {
		t.Fatal("native postgres dialector required")
	}
	metadata, err := control.Open(t.Context(), source.DSN)
	if err != nil {
		t.Fatal(err)
	}
	defer metadata.Close()
	db, err := pool.DB()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(t.Context(), authorization.Schema); err != nil {
		t.Fatal(err)
	}
	service, err := authorization.New(db, runtime, "physical-tenant")
	if err != nil {
		t.Fatal(err)
	}
	index, err := metadata.CreateIndex(t.Context(), control.IndexDefinition{Name: "native-acl", IngestionEnabled: true, SearchEnabled: true})
	if err != nil {
		t.Fatal(err)
	}
	var siteUser, ownerUser, targetUser pm.User
	for name, dest := range map[string]*pm.User{"orguser0": &siteUser, "orguser1": &ownerUser, "orguser3": &targetUser} {
		if err := pool.Where("username=?", name).Take(dest).Error; err != nil {
			t.Fatal(err)
		}
	}
	var parent pm.Organization
	if err := pool.Where("slug=?", "parent").Take(&parent).Error; err != nil {
		t.Fatal(err)
	}
	site := authorization.Subject{UserID: siteUser.ID, UserType: int32(siteUser.UserType)}
	owner := authorization.Subject{UserID: ownerUser.ID, UserType: int32(ownerUser.UserType)}
	target := authorization.Subject{UserID: targetUser.ID, UserType: int32(targetUser.UserType)}
	staleRegular := site
	staleRegular.UserType = int32(pb.UserType_USER_TYPE_REGULAR_TIER_0)
	revision, err := service.AssignIndexExpected(t.Context(), staleRegular, index.ID, parent.ID, 0)
	if err != nil || revision != 1 {
		t.Fatalf("native assignment revision%d %v", revision, err)
	}
	rule := authorization.Rule{IndexID: index.ID, SubjectKind: "user", SubjectID: target.UserID, Capability: authorization.Search, Effect: "allow"}
	if _, err := service.IndexVisibility(t.Context(), target, parent.ID); !errors.Is(err, authorization.ErrForbidden) {
		t.Fatalf("visibility admitted native nonmember: %v", err)
	}
	if visible, err := service.IndexVisibility(t.Context(), staleRegular, parent.ID); err != nil || !visible.Subject.IsSiteAdmin() || visible.Fingerprint == "" {
		t.Fatalf("visibility did not reload promoted native site user: %+v, %v", visible, err)
	}
	if _, err := service.ChangeRuleExpected(t.Context(), owner, parent.ID, rule, revision, false); !errors.Is(err, authorization.ErrForbidden) {
		t.Fatalf("nonmember grant allowed: %v", err)
	}
	ownerCtx := scontext.WithUser[sm.UserIdType](t.Context(), &sm.User{ID: ownerUser.ID, UserType: ownerUser.UserType})
	ownerCtx = scontext.WithUserID[sm.UserIdType, sm.User](ownerCtx, ownerUser.ID)
	handler := runtime.service.GetOrganizationHandler()
	if _, err := identityNativeTransaction(runtime, ownerCtx, func(ctx context.Context) (*sm.UserOrganizationResponse, error) {
		return handler.AddMember(ctx, target.UserID, parent.ID, pb.OrganizationRole_ORGANIZATION_ROLE_MEMBER_TIER_0)
	}); err != nil {
		t.Fatal(err)
	}
	revision, err = service.ChangeRuleExpected(t.Context(), owner, parent.ID, rule, revision, false)
	if err != nil || revision != 2 {
		t.Fatalf("CAS revision%d %v", revision, err)
	}
	if _, err := service.ChangeRuleExpected(t.Context(), owner, parent.ID, rule, 1, true); !errors.Is(err, authorization.ErrConflict) {
		t.Fatalf("stale CAS accepted: %v", err)
	}
	if err := service.AuthorizeIndex(t.Context(), target, parent.ID, index.ID, authorization.Search); err != nil {
		t.Fatal(err)
	}
	visible, err := service.IndexVisibility(t.Context(), target, parent.ID)
	if err != nil || len(visible.Indexes) != 1 || visible.Indexes[0].Name != index.Definition.Name || !visible.Indexes[0].SearchAllowed {
		t.Fatalf("native member visibility: %+v, %v", visible, err)
	}
	denial := rule
	denial.Effect = "deny"
	revision, err = service.ChangeRuleExpected(t.Context(), owner, parent.ID, denial, revision, false)
	if err != nil {
		t.Fatal(err)
	}
	denied, err := service.IndexVisibility(t.Context(), target, parent.ID)
	if err != nil || len(denied.Indexes) != 1 || denied.Indexes[0].SearchAllowed || denied.Fingerprint == visible.Fingerprint {
		t.Fatalf("native search denial was absent from visibility snapshot: %+v, %v", denied, err)
	}
	revision, err = service.ChangeRuleExpected(t.Context(), owner, parent.ID, rule, revision, false)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := identityNativeTransaction(runtime, ownerCtx, func(ctx context.Context) (struct{}, error) {
		return struct{}{}, handler.RemoveMember(ctx, target.UserID, parent.ID)
	}); err != nil {
		t.Fatal(err)
	}
	if err := service.AuthorizeIndex(t.Context(), target, parent.ID, index.ID, authorization.Search); !errors.Is(err, authorization.ErrForbidden) {
		t.Fatalf("retained grant bypassed revoked native membership: %v", err)
	}
	var records int
	var recordedType int32
	if err := db.QueryRowContext(t.Context(), "SELECT count(*),max(actor_user_type) FROM os_index_policy_audit WHERE index_id=$1", index.ID).Scan(&records, &recordedType); err != nil || records != 4 || recordedType != 44 {
		t.Fatalf("atomic assignment/rule audit %d type%d %v", records, recordedType, err)
	}
	runtimeNativeLookupAdmission(t, runtime, metadata, service, site, owner, target, parent.ID)
	// A stale site Subject cannot assign after its native core account was demoted.
	if err := pool.Exec("UPDATE gocommon_users SET user_type=? WHERE id=?", pb.UserType_USER_TYPE_REGULAR_TIER_0, site.UserID).Error; err != nil {
		t.Fatal(err)
	}
	if _, err := service.AssignIndexExpected(t.Context(), site, index.ID, parent.ID, revision); !errors.Is(err, authorization.ErrForbidden) {
		t.Fatalf("stale site authority survived demotion: %v", err)
	}
}
