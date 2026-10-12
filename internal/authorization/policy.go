package authorization

import (
	"context"
	"errors"

	"fortio.org/safecast"
)

var ErrConflict = errors.New("authorization: policy revision conflicts with current state")

type Policy struct {
	OrganizationID int64  `json:"organization_id"`
	Revision       uint64 `json:"revision"`
	Rules          []Rule `json:"rules"`
}
type Decision struct {
	Capability Capability
	Allowed    bool
	Reason     string
}
type policyReader interface {
	Policy(context.Context, string) (Policy, error)
}
type policyMutationCheck func(context.Context, Policy) error
type revisionPolicyStore interface {
	ChangeRuleExpected(context.Context, Rule, int64, uint64, Subject, bool, policyMutationCheck) (uint64, error)
	AssignExpected(context.Context, string, int64, uint64, Subject, policyMutationCheck) (uint64, error)
}
type freshSubjectResolver interface {
	CurrentSubject(context.Context, uint64) (Subject, error)
}

func (s *Service) policy(ctx context.Context, indexID string) (Policy, error) {
	if reader, ok := s.store.(policyReader); ok {
		return reader.Policy(ctx, indexID)
	}
	org, err := s.store.OrganizationForIndex(ctx, indexID)
	if err != nil {
		return Policy{}, err
	}
	rules, err := s.store.Rules(ctx, indexID)
	return Policy{OrganizationID: org, Revision: 1, Rules: rules}, err
}
func (s *Service) currentSubject(ctx context.Context, subject Subject) (Subject, error) {
	if resolver, ok := s.memberships.(transactionAuthority); ok {
		return resolver.CurrentSubjectInTransaction(ctx, subject.UserID)
	}
	if resolver, ok := s.memberships.(freshSubjectResolver); ok {
		return resolver.CurrentSubject(ctx, subject.UserID)
	}
	return subject, nil
}
func (s *Service) authorizePolicy(ctx context.Context, subject Subject, org int64, policy Policy, capability Capability) error {
	if !subject.Valid() {
		return ErrUnauthorized
	}
	if subject.IsSiteAdmin() {
		return nil
	}
	if policy.OrganizationID != org || org <= 0 {
		return ErrForbidden
	}
	role, err := s.OrganizationRole(ctx, subject, org)
	if err != nil {
		return err
	}
	return Evaluate(subject, role, capability, policy.Rules)
}
func (s *Service) GetPolicy(ctx context.Context, subject Subject, org int64, indexID string) (Policy, []Decision, error) {
	policy, err := s.policy(ctx, indexID)
	if errors.Is(err, ErrUnassigned) && subject.IsSiteAdmin() {
		policy = Policy{Rules: []Rule{}}
		err = nil
	}
	if err != nil {
		return Policy{}, nil, err
	}
	if err := s.authorizePolicy(ctx, subject, org, policy, Grants); err != nil {
		return Policy{}, nil, err
	}
	role := int32(0)
	if !subject.IsSiteAdmin() {
		role, err = s.OrganizationRole(ctx, subject, org)
		if err != nil {
			return Policy{}, nil, err
		}
	}
	decisions := make([]Decision, 0, 4)
	for _, capability := range []Capability{Search, Ingest, Manage, Grants} {
		decisions = append(decisions, Explain(subject, role, capability, policy.Rules))
	}
	return policy, decisions, nil
}
func Explain(subject Subject, role int32, capability Capability, rules []Rule) Decision {
	decision := Decision{Capability: capability}
	if subject.IsSiteAdmin() {
		decision.Allowed = true
		decision.Reason = "site_admin"
		return decision
	}
	if !subject.Valid() || !ValidRole(role) {
		decision.Reason = "membership_required"
		return decision
	}
	explicitAllow := false
	for _, rule := range rules {
		if rule.Capability == capability && ((rule.SubjectKind == "user" && rule.SubjectID == subject.UserID) || (rule.SubjectKind == "role" && rule.SubjectID == safecast.MustConv[uint64](role))) {
			if rule.Effect == "deny" {
				decision.Reason = "explicit_deny"
				return decision
			}
			explicitAllow = explicitAllow || rule.Effect == "allow"
		}
	}
	if explicitAllow {
		decision.Allowed = true
		decision.Reason = "explicit_allow"
		return decision
	}
	decision.Allowed = role >= AdminRole || capability == Search
	decision.Reason = "role_below_threshold"
	if decision.Allowed {
		decision.Reason = "role_default"
	}
	return decision
}
func (s *Service) ChangeRuleExpected(ctx context.Context, subject Subject, org int64, rule Rule, expected uint64, remove bool) (uint64, error) {
	if !validRule(rule) || expected == 0 {
		return 0, ErrInvalidPolicy
	}
	store, ok := s.store.(revisionPolicyStore)
	if !ok {
		return 0, ErrInvalidPolicy
	}
	return store.ChangeRuleExpected(ctx, rule, org, expected, subject, remove, func(lockedCtx context.Context, policy Policy) error {
		fresh, err := s.currentSubject(lockedCtx, subject)
		if err != nil {
			return err
		}
		if err = s.authorizePolicy(lockedCtx, fresh, org, policy, Grants); err != nil {
			return err
		}
		if rule.SubjectKind == "user" && !remove {
			_, err = s.OrganizationRole(lockedCtx, Subject{UserID: rule.SubjectID, UserType: 1}, policy.OrganizationID)
		}
		return err
	})
}
func (s *Service) AssignIndexExpected(ctx context.Context, subject Subject, indexID string, org int64, expected uint64) (uint64, error) {
	if indexID == "" || org <= 0 {
		return 0, ErrInvalidPolicy
	}
	store, ok := s.store.(revisionPolicyStore)
	if !ok {
		return 0, ErrInvalidPolicy
	}
	return store.AssignExpected(ctx, indexID, org, expected, subject, func(lockedCtx context.Context, policy Policy) error {
		fresh, err := s.currentSubject(lockedCtx, subject)
		if err != nil {
			return err
		}
		if err = s.AuthorizeOrganization(lockedCtx, fresh, org, OwnerRole); err != nil {
			return err
		}
		if policy.OrganizationID == 0 {
			if !fresh.IsSiteAdmin() {
				return ErrForbidden
			}
			return nil
		}
		if err = s.AuthorizeOrganization(lockedCtx, fresh, policy.OrganizationID, OwnerRole); err != nil {
			return err
		}
		return s.authorizePolicy(lockedCtx, fresh, policy.OrganizationID, policy, Grants)
	})
}

// AuthorizeIndexMetadata permits metadata needed for any currently authorized
// index capability. Search catalogs must separately require Search.
func (s *Service) AuthorizeIndexMetadata(ctx context.Context, subject Subject, org int64, indexID string) error {
	if !subject.Valid() {
		return ErrUnauthorized
	}
	if subject.IsSiteAdmin() {
		return nil
	}
	policy, err := s.policy(ctx, indexID)
	if err != nil {
		return err
	}
	if policy.OrganizationID != org || org <= 0 {
		return ErrForbidden
	}
	role, err := s.OrganizationRole(ctx, subject, org)
	if err != nil {
		return err
	}
	for _, capability := range []Capability{Search, Ingest, Manage, Grants} {
		if Evaluate(subject, role, capability, policy.Rules) == nil {
			return nil
		}
	}
	return ErrForbidden
}
