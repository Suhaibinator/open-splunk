package identity

import (
	"context"
	"errors"
	"testing"

	sm "github.com/Suhaibinator/go-common/pkg/common_service/common_service_models"
	gerrors "github.com/Suhaibinator/go-common/pkg/common_service/errors"
	gcconfig "github.com/Suhaibinator/go-common/pkg/config"
	pb "github.com/Suhaibinator/go-common/pkg/proto_models"
)

func TestGoogleOnlyConfigurationDoesNotGrantOrganizationOrCredentials(t *testing.T) {
	cfg := gcconfig.GetDefaults()
	cfg.SessionTokenSecret = gcconfig.NewSecret("session-resource-preserved")
	cfg.MinioBucket = "private-results"
	Configure(cfg)
	view := gcconfig.NewRootView(cfg)
	if !view.EnableOAuth() || view.EnableEmailAuth() || view.SMSEnabled() || !view.EnableOrganizations() {
		t.Fatal("unexpected identity feature policy")
	}
	if view.PersonalWorkspaceOrganizationType() != PersonalWorkspaceType || PersonalWorkspaceType >= view.OrgMinTypeCreateChildOrganizations() || PersonalWorkspaceType == TeamOrganizationType {
		t.Fatal("managed workspace must have its own type without child creation")
	}
	if view.EnableOrganizationEmailAutoAssociation() || view.OrgAncestorMembershipAutoAdd() {
		t.Fatal("signup/membership must not auto grant scope")
	}
	if view.DefaultUserType() != pb.UserType_USER_TYPE_REGULAR_TIER_0 || view.AuthCacheTtl() <= 0 || view.AuthTokenCacheCapacity() == 0 || view.AuthUserCacheCapacity() == 0 {
		t.Fatal("unprivileged signup and native token/user caching required")
	}
	if cfg.MinioBucket != "private-results" || cfg.SessionTokenSecret.StringValue() != "session-resource-preserved" {
		t.Fatal("policy replaced deployment resource identity")
	}
	if view.SiteMinUserTypeUserTypeChange() != pb.UserType_USER_TYPE_ADMIN_TIER_10 || view.OrgMinUserTypeCreate() != pb.UserType_USER_TYPE_REGULAR_TIER_0 {
		t.Fatal("site administration must use the same highest user type")
	}
}

func TestSubjectUsesRecognizedPersistedTypeAndAccountState(t *testing.T) {
	for _, user := range []sm.User{{ID: 1, UserType: 999}, {ID: 1, UserType: 44, Banned: true}, {ID: 1, UserType: 44, Bot: true}, {ID: 0, UserType: 44}} {
		if _, err := subjectForUser(user); err == nil {
			t.Fatalf("accepted invalid account %#v", user)
		}
	}
	site, err := subjectForUser(sm.User{ID: 1, UserType: pb.UserType_USER_TYPE_ADMIN_TIER_10})
	if err != nil || !site.IsSiteAdmin() {
		t.Fatal("recognized highest admin type must be siteadmin")
	}
	ordinary, err := subjectForUser(sm.User{ID: 1, UserType: pb.UserType_USER_TYPE_ADMIN_TIER_9})
	if err != nil || ordinary.IsSiteAdmin() {
		t.Fatal("lower admin tiers cannot gain OpenSplunk site authority")
	}
}

func TestNativeOwnersManageAdminsAndAdminsManageViewers(t *testing.T) {
	for _, fixture := range []struct {
		actor, target pb.OrganizationRole
		allowed       bool
	}{
		{pb.OrganizationRole_ORGANIZATION_ROLE_ADMIN_TIER_0, pb.OrganizationRole_ORGANIZATION_ROLE_MEMBER_TIER_0, true},
		{pb.OrganizationRole_ORGANIZATION_ROLE_ADMIN_TIER_0, pb.OrganizationRole_ORGANIZATION_ROLE_ADMIN_TIER_0, false},
		{pb.OrganizationRole_ORGANIZATION_ROLE_ADMIN_TIER_0, pb.OrganizationRole_ORGANIZATION_ROLE_OWNER_TIER_10, false},
		{pb.OrganizationRole_ORGANIZATION_ROLE_OWNER_TIER_10, pb.OrganizationRole_ORGANIZATION_ROLE_ADMIN_TIER_0, true},
		{pb.OrganizationRole_ORGANIZATION_ROLE_MEMBER_TIER_0, pb.OrganizationRole_ORGANIZATION_ROLE_MEMBER_TIER_0, false},
	} {
		if actual := sm.CanManageRole(fixture.actor, fixture.target); actual != fixture.allowed {
			t.Fatalf("native role management %v→%v=%t", fixture.actor, fixture.target, actual)
		}
	}
}

func TestRootCreationRequiresExplicitOwnerAndSiteAdmin(t *testing.T) {
	r := &Runtime{}
	input := OrganizationCreationInput{ActorID: 1, ActorType: pb.UserType_USER_TYPE_ADMIN_TIER_10, Organization: &sm.Organization{}}
	if _, err := r.initialMemberships(context.Background(), input); err == nil {
		t.Fatal("accepted ownerless creation")
	}
	input.RequestedMemberships = []InitialMembership{{UserID: 2, Role: pb.OrganizationRole_ORGANIZATION_ROLE_OWNER_TIER_10}}
	members, err := r.initialMemberships(context.Background(), input)
	if err != nil || len(members) != 1 || members[0].UserID != 2 {
		t.Fatal("siteadmin must assign the explicitly requested owner")
	}
	input.ActorType = pb.UserType_USER_TYPE_REGULAR_TIER_0
	if _, err := r.initialMemberships(context.Background(), input); !errors.Is(err, gerrors.ErrInsufficientPermissions) {
		t.Fatal("ordinary account created root organization")
	}
}
