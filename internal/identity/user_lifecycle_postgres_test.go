package identity

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/Suhaibinator/SRouter/pkg/scontext"
	pm "github.com/Suhaibinator/go-common/pkg/common_persistence/common_persistence_models"
	sm "github.com/Suhaibinator/go-common/pkg/common_service/common_service_models"
	gerrors "github.com/Suhaibinator/go-common/pkg/common_service/errors"
	"github.com/Suhaibinator/go-common/pkg/common_service/organization"
	"github.com/Suhaibinator/go-common/pkg/common_service/token"
	gcconfig "github.com/Suhaibinator/go-common/pkg/config"
	pb "github.com/Suhaibinator/go-common/pkg/proto_models"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

func runtimeUserDeletionPreservesNativeOwnership(t *testing.T, runtime *Runtime, pool *gorm.DB, cfg *gcconfig.Config) {
	users := []pm.User{
		{Username: "deletionsite", Email: "deletionsite@example.com", UserType: pb.UserType_USER_TYPE_ADMIN_TIER_10},
		{Username: "deletionowner", Email: "deletionowner@example.com", UserType: pb.UserType_USER_TYPE_REGULAR_TIER_0},
		{Username: "deletionmember", Email: "deletionmember@example.com", UserType: pb.UserType_USER_TYPE_REGULAR_TIER_0},
		{Username: "deletionsoleowner", Email: "deletionsoleowner@example.com", UserType: pb.UserType_USER_TYPE_REGULAR_TIER_0},
	}
	for i := range users {
		if err := pool.Create(&users[i]).Error; err != nil {
			t.Fatal(err)
		}
		if err := pool.Create(&pm.OAuthLogin{UserID: users[i].ID, ProviderSlug: "google", Subject: new(users[i].Username), Email: users[i].Email}).Error; err != nil {
			t.Fatal(err)
		}
	}
	// User deletion must retain GoCommon's always-provisioned workspace cleanup,
	// including rollback when the user remains the last Owner of a team.
	for _, user := range users[2:] {
		if _, err := identityNativeTransaction(runtime, t.Context(), func(ctx context.Context) (*sm.Organization, error) {
			return runtime.service.GetPersonalWorkspaceService().EnsureForUser(ctx, &sm.User{ID: user.ID, Username: user.Username, Email: user.Email, UserType: user.UserType})
		}); err != nil {
			t.Fatal(err)
		}
	}
	siteCtx := scontext.WithUser[sm.UserIdType](t.Context(), &sm.User{ID: users[0].ID, UserType: users[0].UserType})
	siteCtx = scontext.WithUserID[sm.UserIdType, sm.User](siteCtx, users[0].ID)
	handler := runtime.service.GetOrganizationHandler()
	create := func(slug string, parent *int64, owner uint64) int64 {
		org, err := identityNativeTransaction(runtime, siteCtx, func(ctx context.Context) (*organization.OrganizationResponse, error) {
			return runtime.CreateOrganizationWithInitialMemberships(ctx, &sm.Organization{Name: slug, Slug: slug, ParentOrganizationID: parent}, []InitialMembership{{UserID: owner, Role: pb.OrganizationRole_ORGANIZATION_ROLE_OWNER_TIER_10}})
		})
		if err != nil {
			t.Fatal(err)
		}
		return org.ID
	}
	add := func(user uint64, org int64) {
		if _, err := identityNativeTransaction(runtime, siteCtx, func(ctx context.Context) (*sm.UserOrganizationResponse, error) {
			return handler.AddMember(ctx, user, org, pb.OrganizationRole_ORGANIZATION_ROLE_MEMBER_TIER_0)
		}); err != nil {
			t.Fatal(err)
		}
	}
	root := create("deletion-root", nil, users[1].ID)
	add(users[2].ID, root)
	add(users[3].ID, root)
	child := create("deletion-child", &root, users[1].ID)
	add(users[2].ID, child)
	add(users[3].ID, child)
	soleRoot := create("deletion-sole-owner", nil, users[3].ID)
	_ = create("deletion-sole-child", &soleRoot, users[3].ID)
	tokens, err := token.NewTokenService(runtime.Persistence(), token.TokenServiceConfig{SessionLifetime: func() time.Duration { return time.Hour }, SecurityLifetime: func() time.Duration { return time.Hour }, SessionTokenSecret: cfg.SessionTokenSecret.StringValue()}, zap.NewNop())
	if err != nil {
		t.Fatal(err)
	}
	mint := func(user uint64) *http.Request {
		session, err := tokens.GenerateSessionToken(t.Context(), user, token.SessionAuthMethod{Provider: pb.AuthProvider_AUTH_PROVIDER_OAUTH, OAuthProviderSlug: "google"})
		if err != nil {
			t.Fatal(err)
		}
		request := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/api/search", nil)
		request.AddCookie(&http.Cookie{Name: runtime.cookieName, Value: session, Secure: true, HttpOnly: true, SameSite: http.SameSiteLaxMode})
		if _, _, err := runtime.AuthenticateRequest(request.Clone(t.Context())); err != nil {
			t.Fatal(err)
		}
		return request
	}
	memberSession, soleSession := mint(users[2].ID), mint(users[3].ID)
	deleteUser := func(user uint64) error {
		_, err := identityNativeTransaction(runtime, siteCtx, func(ctx context.Context) (struct{}, error) {
			return struct{}{}, runtime.service.AdminDeleteUser(ctx, user, false, "account lifecycle acceptance")
		})
		return err
	}
	if err := deleteUser(users[3].ID); !errors.Is(err, gerrors.ErrCannotRemoveLastOwner) {
		t.Fatalf("sole Owner deletion error=%v", err)
	}
	var memberships int64
	if err := pool.Model(&pm.UserOrganization{}).Where("user_id=?", users[3].ID).Count(&memberships).Error; err != nil || memberships != 5 {
		t.Fatalf("failed delete did not roll back all roots: count%d %v", memberships, err)
	}
	if _, _, err := runtime.AuthenticateRequest(soleSession.Clone(t.Context())); err != nil {
		t.Fatalf("failed delete revoked existing Google session: %v", err)
	}
	var active pm.User
	if err := pool.Take(&active, users[3].ID).Error; err != nil || active.Username != users[3].Username {
		t.Fatalf("failed delete changed native user identity: %+v %v", active, err)
	}
	if err := deleteUser(users[2].ID); err != nil {
		t.Fatal(err)
	}
	if err := pool.Table(pm.TableNameUserOrganizations+" AS membership").Joins("JOIN "+pm.TableNameOrganizations+" AS organization ON organization.id=membership.organization_id").Where("membership.user_id=? AND organization.deleted_at IS NULL", users[2].ID).Count(&memberships).Error; err != nil || memberships != 0 {
		t.Fatalf("successful delete left hierarchy memberships: count%d %v", memberships, err)
	}
	var personalRoots int64
	if err := pool.Model(&pm.Organization{}).Where("personal_owner_user_id=?", users[2].ID).Count(&personalRoots).Error; err != nil || personalRoots != 0 {
		t.Fatal("user deletion orphaned a managed personal workspace", personalRoots, err)
	}
	if _, _, err := runtime.AuthenticateRequest(memberSession.Clone(t.Context())); err == nil {
		t.Fatal("deleted user retained its Google session")
	}
	active = pm.User{}
	if err := pool.Take(&active, users[2].ID).Error; !errors.Is(err, gorm.ErrRecordNotFound) {
		t.Fatalf("deleted native user remains active: %v", err)
	}
}
