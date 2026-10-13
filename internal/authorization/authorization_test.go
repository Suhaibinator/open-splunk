package authorization

import (
	"context"
	"errors"
	"testing"
)

type policyStore struct {
	org   int64
	rules []Rule
	reads int
}

func (s *policyStore) Policy(context.Context, string) (Policy, error) {
	s.reads++
	if s.org == 0 {
		return Policy{}, ErrUnassigned
	}
	return Policy{OrganizationID: s.org, Revision: 1, Rules: s.rules}, nil
}
func (s *policyStore) OrganizationForIndex(context.Context, string) (int64, error) {
	s.reads++
	if s.org == 0 {
		return 0, ErrUnassigned
	}
	return s.org, nil
}
func (s *policyStore) Rules(context.Context, string) ([]Rule, error) { s.reads++; return s.rules, nil }
func (s *policyStore) SetOrganization(_ context.Context, _ string, _, org int64) error {
	s.org = org
	return nil
}
func (s *policyStore) SetRule(context.Context, Rule, int64) error    { return nil }
func (s *policyStore) DeleteRule(context.Context, Rule, int64) error { return nil }

type membershipStub struct {
	role   int32
	member bool
	err    error
	reads  int
}

func (s *membershipStub) EffectiveRole(context.Context, uint64, int64) (int32, bool, error) {
	s.reads++
	return s.role, s.member, s.err
}

func TestNonMemberCannotUseDirectAllow(t *testing.T) {
	store := &policyStore{org: 7, rules: []Rule{{SubjectKind: "user", SubjectID: 9, Capability: Search, Effect: "allow"}}}
	memberships := &membershipStub{}
	service, _ := NewService(store, memberships, "physical")
	err := service.AuthorizeIndex(context.Background(), Subject{UserID: 9, UserType: 1}, 7, "index", Search)
	if !errors.Is(err, ErrForbidden) || memberships.reads != 1 || store.reads != 1 {
		t.Fatalf("nonmember received authority: %v, reads=%d/%d", err, memberships.reads, store.reads)
	}
}
func TestDenyOverridesUserAndRoleAllows(t *testing.T) {
	for _, role := range []int32{ViewerRole, AdminRole, OwnerRole} {
		for _, deniedSubject := range []string{"user", "role"} {
			id := uint64(role)
			if deniedSubject == "user" {
				id = 9
			}
			rules := []Rule{{SubjectKind: "user", SubjectID: 9, Capability: Search, Effect: "allow"}, {SubjectKind: "role", SubjectID: uint64(role), Capability: Search, Effect: "allow"}, {SubjectKind: deniedSubject, SubjectID: id, Capability: Search, Effect: "deny"}}
			if err := Evaluate(Subject{UserID: 9, UserType: 1}, role, Search, rules); !errors.Is(err, ErrForbidden) {
				t.Fatalf("deny failed for role %d subject %s: %v", role, deniedSubject, err)
			}
		}
	}
}
func TestSiteAdministratorBypassesAllResourcePolicy(t *testing.T) {
	store := &policyStore{}
	memberships := &membershipStub{err: errors.New("membership database unavailable")}
	service, _ := NewService(store, memberships, "physical")
	if err := service.AuthorizeIndex(context.Background(), Subject{UserID: 9, UserType: SiteAdminUserType}, 0, "index", Manage); err != nil {
		t.Fatal(err)
	}
	if store.reads != 0 || memberships.reads != 0 {
		t.Fatal("site administrator depended on resource policy")
	}
}
func TestMembershipRevocationTakesEffectOnNextDecision(t *testing.T) {
	store := &policyStore{org: 7}
	memberships := &membershipStub{role: ViewerRole, member: true}
	service, _ := NewService(store, memberships, "physical")
	subject := Subject{UserID: 9, UserType: 1}
	if err := service.AuthorizeIndex(context.Background(), subject, 7, "index", Search); err != nil {
		t.Fatal(err)
	}
	memberships.member = false
	if err := service.AuthorizeIndex(context.Background(), subject, 7, "index", Search); !errors.Is(err, ErrForbidden) {
		t.Fatalf("revoked membership retained authority: %v", err)
	}
}
func TestCapabilitiesAndExactRoleOverrides(t *testing.T) {
	subject := Subject{UserID: 9, UserType: 1}
	if err := Evaluate(subject, ViewerRole, Ingest, nil); !errors.Is(err, ErrForbidden) {
		t.Fatal("viewer can ingest")
	}
	if err := Evaluate(subject, AdminRole, Ingest, []Rule{{SubjectKind: "role", SubjectID: uint64(ViewerRole), Capability: Ingest, Effect: "deny"}}); err != nil {
		t.Fatal("viewer deny matched admin")
	}
	if err := Evaluate(subject, ViewerRole, Ingest, []Rule{{SubjectKind: "user", SubjectID: 9, Capability: Ingest, Effect: "allow"}}); err != nil {
		t.Fatal("scoped ingest grant was ignored")
	}
	if err := Evaluate(Subject{UserID: 9, UserType: 45}, OwnerRole, Search, nil); !errors.Is(err, ErrUnauthorized) {
		t.Fatal("undefined user type granted authority")
	}
}

func TestIndexMetadataPermitsManagementDespiteSearchDeny(t *testing.T) {
	store := &policyStore{org: 7, rules: []Rule{{IndexID: "index", SubjectKind: "role", SubjectID: uint64(AdminRole), Capability: Search, Effect: "deny"}}}
	service, _ := NewService(store, &membershipStub{role: AdminRole, member: true}, "physical")
	subject := Subject{UserID: 9, UserType: 12}
	if err := service.AuthorizeIndex(context.Background(), subject, 7, "index", Search); !errors.Is(err, ErrForbidden) {
		t.Fatal("search deny ignored")
	}
	if err := service.AuthorizeIndexMetadata(context.Background(), subject, 7, "index"); err != nil {
		t.Fatalf("authorized management index undiscoverable: %v", err)
	}
	for _, capability := range []Capability{Ingest, Manage, Grants} {
		store.rules = append(store.rules, Rule{IndexID: "index", SubjectKind: "role", SubjectID: uint64(AdminRole), Capability: capability, Effect: "deny"})
	}
	if err := service.AuthorizeIndexMetadata(context.Background(), subject, 7, "index"); !errors.Is(err, ErrForbidden) {
		t.Fatal("fully denied index metadata exposed")
	}
}
