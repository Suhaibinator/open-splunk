package identity

import (
	"context"
	"errors"
	"testing"

	sm "github.com/Suhaibinator/go-common/pkg/common_service/common_service_models"
	gerrors "github.com/Suhaibinator/go-common/pkg/common_service/errors"
	"github.com/Suhaibinator/go-common/pkg/common_service/organization"
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
	if view.DefaultUserType() != pb.UserType_USER_TYPE_REGULAR_TIER_0 || view.AuthCacheTtl() != 0 {
		t.Fatal("unprivileged signup and fresh persisted user authority required")
	}
	if cfg.MinioBucket != "private-results" || cfg.SessionTokenSecret.StringValue() != "session-resource-preserved" {
		t.Fatal("policy replaced deployment resource identity")
	}
	if view.SiteMinUserTypeUserTypeChange() != pb.UserType_USER_TYPE_ADMIN_TIER_10 || view.OrgMinUserTypeCreate() != pb.UserType_USER_TYPE_ADMIN_TIER_10 {
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

func TestAdminCanManagePeersButNotOwnersOrOtherTiers(t *testing.T) {
	p := rolePolicy{}
	for _, test := range []struct {
		actor, target int32
		allowed       bool
	}{{23, 1, true}, {23, 23, true}, {23, 44, false}, {23, 24, false}, {1, 1, false}, {44, 44, true}, {44, 23, true}} {
		if actual := p.CanManageRole(pb.OrganizationRole(test.actor), pb.OrganizationRole(test.target)); actual != test.allowed {
			t.Fatalf("manage %d->%d=%t", test.actor, test.target, actual)
		}
		if actual := p.CanGrantRole(pb.OrganizationRole(test.actor), pb.OrganizationRole(test.target)); actual != test.allowed {
			t.Fatalf("grant %d->%d=%t", test.actor, test.target, actual)
		}
	}
}

func TestRootCreationRequiresExplicitOwnerAndSiteAdmin(t *testing.T) {
	r := &Runtime{}
	input := organization.OrganizationCreationInput{ActorID: 1, ActorType: pb.UserType_USER_TYPE_ADMIN_TIER_10, Organization: &sm.Organization{}}
	if _, err := r.initialMemberships(context.Background(), input); err == nil {
		t.Fatal("accepted ownerless creation")
	}
	input.RequestedMemberships = []organization.InitialMembership{{UserID: 2, Role: pb.OrganizationRole_ORGANIZATION_ROLE_OWNER_TIER_10}}
	members, err := r.initialMemberships(context.Background(), input)
	if err != nil || len(members) != 1 || members[0].UserID != 2 {
		t.Fatal("siteadmin must assign the explicitly requested owner")
	}
	input.ActorType = pb.UserType_USER_TYPE_REGULAR_TIER_0
	if _, err := r.initialMemberships(context.Background(), input); !errors.Is(err, gerrors.ErrInsufficientPermissions) {
		t.Fatal("ordinary account created root organization")
	}
}
